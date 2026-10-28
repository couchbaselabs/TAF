import contextlib
import json
import threading
import time

from mc_bin_client import MemcachedClient, MemcachedError
from membase.api.rest_client import RestConnection
from shell_util.remote_connection import RemoteMachineShellConnection

from pytests.basetestcase import ClusterSetup


class ExternalAuthQueue(ClusterSetup):
    """Coverage for MB-74514.

    MB-70685 capped in-flight external (LDAP) auth requests in memcached's
    ExternalAuthManagerThread at 100 (``maxPendingRequests``).  When that cap
    was reached and more external-auth requests were still queued,
    ``ExternalAuthManagerThread::run()`` stopped sleeping on its condition
    variable and busy-spun while holding the manager mutex.  ``responseReceived()``
    (run on the front-end worker threads) needs that mutex to retire a completed
    request and free a slot, so the spin deadlocked: memcached's front end
    stopped serving every connection while the process stayed up.

    This test forces that state at the black-box level:

    1. Configure ``/settings/ldap`` with a template userDNMapping so an external
       auth is a single LDAP bind, pointed at a *blackholed* host (a reachable
       IP whose port 389 is silently dropped via iptables) with a long
       ``requestTimeout``.  Every external-auth request then stays in-flight for
       seconds instead of draining, so > 100 can pile up at once.  No real LDAP
       server or users are needed - an unknown external username still enters the
       queue.
    2. Fire ``num_auth_requests`` (> 100) concurrent SASL PLAIN auths for
       external users straight at the memcached data port.  100 go in-flight and
       the rest queue, opening the bug window.
    3. Probe memcached for responsiveness from independent connections.  On a
       fixed build (Totoro >= 8.5.0-1320) the front end stays responsive and the
       test passes; on a pre-fix build the front end is frozen and the probe
       times out, so the test fails and reports MB-74514 reproduced.
    """

    def setUp(self):
        super().setUp()
        self.rest = RestConnection(self.cluster.master)
        self.master_ip = self.cluster.master.ip

        # > 100 so the 100-in-flight cap is exceeded with requests still queued.
        self.num_auth_requests = self.input.param("num_auth_requests", 150)
        # Seconds to let every burst thread send its SASL request before probing.
        self.settle_time = self.input.param("settle_time", 20)
        # Socket timeout (s) for the responsiveness probe. A frozen front end
        # never answers, so this bounds how long we wait before calling it hung.
        self.probe_timeout = self.input.param("probe_timeout", 20)
        # ns_server LDAP bind timeout (ms). Kept well above the probe window so
        # in-flight requests do not drain while we are measuring.
        self.ldap_request_timeout = self.input.param("ldap_request_timeout", 120000)

        self.admin_user = self.cluster.master.rest_username
        self.admin_pass = self.cluster.master.rest_password

        self._blackhole_nodes = []
        self._ldap_configured = False
        self._burst_clients = []
        self._burst_threads = []

    def tearDown(self):
        # Recover BEFORE the base tearDown runs its cluster health checks: on a
        # pre-fix build memcached is wedged and will not self-heal, so every node
        # that served external auth needs couchbase-server restarted.
        recovered = False
        try:
            self._clear_blackhole()
            self._clear_ldap_settings()
            recovered = self._restart_and_wait()
        except Exception as err:
            self.log.error(f"tearDown recovery error: {err}")
        finally:
            for client in self._burst_clients:
                with contextlib.suppress(Exception):
                    client.close()
        if not recovered:
            self.log.error(
                "Cluster was not confirmed healthy after recovery; base tearDown may hang on a wedged memcached."
            )
        super().tearDown()

    # ------------------------------------------------------------------
    # LDAP / blackhole helpers
    # ------------------------------------------------------------------
    def _configure_blackholed_ldap(self):
        """Point memcached's external auth at a host that never answers.

        A template userDNMapping makes each external auth a direct LDAP bind
        (no search), so the only network op is the bind.  The bind target is the
        master IP with tcp/389 dropped by iptables, so the bind hangs until
        ``requestTimeout``.  Every external auth request therefore lingers
        in-flight, letting > 100 accumulate.
        """
        user_dn_mapping = json.dumps({"template": "uid=%u,ou=Users,dc=couchbase,dc=com"})
        ldap_settings = {
            "authenticationEnabled": "true",
            "authorizationEnabled": "false",
            "hosts": self.master_ip,
            "port": 389,
            "encryption": "None",
            "userDNMapping": user_dn_mapping,
            "requestTimeout": self.ldap_request_timeout,
            "maxParallelConnections": 100,
            "bindDN": "",
            "bindPass": "",
        }
        status, content, _ = self.rest.configure_ldap_settings(ldap_settings)
        if not status:
            self.fail(f"Failed to configure /settings/ldap: {content}")
        self._ldap_configured = True
        self.log.info(
            "LDAP external auth enabled, pointed at blackholed "
            f"{self.master_ip}:389 (requestTimeout={self.ldap_request_timeout}ms)"
        )

    def _blackhole_ldap_port(self):
        """Silently drop ns_server's outbound LDAP SYN on every node."""
        rule = f"OUTPUT -p tcp -d {self.master_ip} --dport 389 -j DROP"
        for server in self.cluster.servers:
            shell = RemoteMachineShellConnection(server)
            try:
                shell.execute_command("iptables -A " + rule)
                self._blackhole_nodes.append(server)
            finally:
                shell.disconnect()
        self.log.info(f"Installed iptables blackhole for {self.master_ip}:389 on {len(self._blackhole_nodes)} node(s)")

    def _clear_blackhole(self):
        rule = f"OUTPUT -p tcp -d {self.master_ip} --dport 389 -j DROP"
        for server in self._blackhole_nodes:
            shell = RemoteMachineShellConnection(server)
            try:
                # -D may need repeating if the rule was somehow added twice.
                for _ in range(3):
                    out, _err = shell.execute_command("iptables -D " + rule)
                    if any("No chain/target/match" in line or "does a matching rule exist" in line for line in out):
                        break
            finally:
                shell.disconnect()
        self._blackhole_nodes = []

    def _clear_ldap_settings(self):
        if not self._ldap_configured:
            return
        try:
            self.rest.configure_ldap_settings({"authenticationEnabled": "false", "authorizationEnabled": "false"})
        except Exception as err:
            self.log.warning(f"Failed to disable LDAP in cleanup: {err}")
        self._ldap_configured = False

    def _restart_and_wait(self):
        for server in self.cluster.servers:
            shell = RemoteMachineShellConnection(server)
            try:
                shell.restart_couchbase()
            finally:
                shell.disconnect()
        # Wait for the data service to accept connections again.
        deadline = time.time() + 180
        while time.time() < deadline:
            try:
                probe = MemcachedClient(self.master_ip, 11210, timeout=10)
                try:
                    probe.sasl_auth_plain(self.admin_user, self.admin_pass)
                    probe.noop()
                    return True
                finally:
                    probe.close()
            except Exception:
                time.sleep(5)
        return False

    # ------------------------------------------------------------------
    # Burst + probe helpers
    # ------------------------------------------------------------------
    def _external_auth_once(self, index):
        """One external-user SASL PLAIN auth; keep the socket open so the
        in-flight request is not freed when the thread returns."""
        user = f"mb74514_ext_user_{index}"
        try:
            client = MemcachedClient(self.master_ip, 11210, timeout=self.ldap_request_timeout // 1000 + 30)
            self._burst_clients.append(client)
            client.sasl_auth_plain(user, "password")
        except (TimeoutError, OSError, MemcachedError, Exception) as err:
            # Auth failing / timing out is expected; the request still queued.
            self.log.debug(f"ext auth {index} returned: {err}")

    def _fire_auth_burst(self):
        for i in range(self.num_auth_requests):
            t = threading.Thread(target=self._external_auth_once, args=(i,))
            t.daemon = True
            t.start()
            self._burst_threads.append(t)
        self.log.info(f"Launched {self.num_auth_requests} concurrent external-auth requests")

    def _memcached_is_responsive(self, authed=True):
        """Open a fresh connection and run a trivial op within probe_timeout.
        Returns True if the op completes, False if the front end never answers."""
        client = None
        try:
            client = MemcachedClient(self.master_ip, 11210, timeout=self.probe_timeout)
            if authed:
                client.sasl_auth_plain(self.admin_user, self.admin_pass)
            client.noop()
            return True
        except (TimeoutError, OSError) as err:
            self.log.warning(f"Responsiveness probe timed out/errored: {err}")
            return False
        except MemcachedError as err:
            # A protocol-level answer still proves the front end is serving.
            self.log.info(f"Probe got a memcached response: {err}")
            return True
        finally:
            if client is not None:
                with contextlib.suppress(Exception):
                    client.close()

    # ------------------------------------------------------------------
    # Test
    # ------------------------------------------------------------------
    def test_external_auth_request_queue_overflow(self):
        if self.num_auth_requests <= 100:
            self.fail(
                f"num_auth_requests must exceed 100 to exceed the maxPendingRequests cap; got {self.num_auth_requests}"
            )

        # Baseline: memcached must be responsive before we start.
        if not self._memcached_is_responsive():
            self.fail(
                "memcached front end was not responsive before the test "
                "started; cannot attribute a later hang to MB-74514."
            )
        self.log.info("Baseline: memcached front end responsive.")

        self._configure_blackholed_ldap()
        self._blackhole_ldap_port()

        # Let LDAP + firewall settle so external auths actually hang in-flight.
        time.sleep(10)

        self._fire_auth_burst()
        self.log.info(f"Waiting {self.settle_time}s for requests to queue/stall in-flight")
        time.sleep(self.settle_time)

        # Probe from independent connections. On a fixed build the front end
        # keeps serving; on a pre-fix build the ExternalAuthManagerThread is
        # spinning on the mutex and nothing is answered.
        responsive = False
        for attempt in range(1, 4):
            self.log.info(f"Responsiveness probe attempt {attempt}/3")
            if self._memcached_is_responsive():
                responsive = True
                break

        # Confirm the process itself is still up (ticket: "process stays up").
        process_up = True
        try:
            self.rest.get_nodes_self()
        except Exception as err:
            process_up = False
            self.log.error(f"ns_server REST also unreachable: {err}")

        if not responsive:
            rest_state = "still up" if process_up else "also down"
            self.fail(
                "MB-74514 reproduced: memcached front end stopped serving KV "
                f"connections while {self.num_auth_requests} external-auth "
                f"requests were queued (ns_server REST {rest_state}). The "
                "ExternalAuthManagerThread is spinning on its mutex. "
                "Expected on builds < 8.5.0-1320."
            )

        self.log.info(
            f"PASS: memcached front end stayed responsive with {self.num_auth_requests} "
            "external-auth requests queued; MB-74514 fix present."
        )
