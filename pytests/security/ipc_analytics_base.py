import json
import time

import requests
from BucketLib.bucket import Bucket
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from membase.api.rest_client import RestConnection
from shell_util.remote_connection import RemoteMachineShellConnection

from couchbase_utils.security_utils.ipc_analytics_utils import IPCAnalyticsUtils
from couchbase_utils.security_utils.ipc_utils import IPCUtils
from pytests.security.ipc_base import IPCBase


class IPCAnalyticsBase(IPCBase):
    """
    IPCBase aimed at the Analytics (cbas) service.

    Analytics resolves a client certificate by asking ns_server who it names,
    so with the setting on an internal certificate names nobody there either.
    What differs per endpoint is what the certificate bought with the setting
    OFF: the query servlet and the node/config endpoints refuse the internal
    identity regardless, while the cluster, link and active-request endpoints,
    and the ns_server /_p/cbas/ proxy, answered it as an administrator.
    Those are the ones that prove the setting is what decides.
    """

    # name -> (method, path, statement or None, answered with the setting off)
    ENDPOINTS = {
        "query": ("POST", IPCAnalyticsUtils.SERVICE_PATH, "SELECT 1;", False),
        "cluster": ("GET", "/analytics/cluster", None, True),
        "active requests": (
            "GET", "/analytics/admin/active_requests", None, True),
        "links": ("GET", "/analytics/link", None, True),
        "node diagnostics": (
            "GET", "/analytics/node/diagnostics", None, False),
        "service config": ("GET", "/analytics/config/service", None, False),
        "node config": ("GET", "/analytics/config/node", None, False),
        "ingestion status": (
            "GET", "/analytics/status/ingestion", None, False),
    }

    # What must never appear in a response to a refused caller. The internal
    # domain itself would tell an attacker their SAN was recognised.
    LEAK_MARKERS = (
        IPCBase.INTERNAL_CERT_DOMAIN, "BEGIN CERTIFICATE", "PRIVATE KEY",
    )

    def setUp(self):
        super().setUp()
        self.cbas_nodes = self.nodes_with_service("cbas")
        if not self.cbas_nodes:
            self.fail("No node runs the cbas service; run with a "
                      "services_init that includes cbas.")
        self.analytics_toggle_timeout = self.input.param(
            "analytics_toggle_timeout", 60)
        self._n2n_changed = False
        self._audit_restore = None
        # Remote cluster state a test changed: (server, trusted CA ids,
        # IPC key as stored before, users created there).
        self._remote = None
        # Analytics objects a test created, dropped in tearDown whether or
        # not it passed: a leftover dataset bound to a deleted bucket would
        # otherwise be silently reused by the next CREATE ... IF NOT EXISTS.
        self._datasets = []
        self._links = []
        self.wait_for_analytics(self.cbas_nodes)

    def tearDown(self):
        # clientCertAuth first, as in IPCBase: left mandatory it blocks the
        # REST the restores below need.
        try:
            self._disable_client_cert_auth()
        except Exception as exc:
            self.log.warning(f"clientCertAuth disable error: {exc}")
        try:
            self._drop_analytics_objects()
        except Exception as exc:
            self.log.warning(f"Analytics object cleanup error: {exc}")
        if self._audit_restore is not None:
            try:
                IPCAnalyticsUtils.set_audit(self.cluster.master,
                                            *self._audit_restore)
            except Exception as exc:
                self.log.warning(f"Audit settings restore error: {exc}")
        if self._remote is not None:
            self._restore_remote()
        if self._n2n_changed:
            try:
                self.set_n2n_encryption(self.cluster.servers, "off")
            except Exception as exc:
                self.log.warning(f"Node-to-node encryption restore error: "
                                 f"{exc}")
        super().tearDown()

    # -- Cluster fixtures ----------------------------------------------------

    def track_dataset(self, name):
        self._datasets.append(name)

    def track_link(self, name):
        self._links.append(name)

    def _drop_analytics_objects(self):
        """Best effort; plain 8095, so no clientCertAuth state blocks it.

        Each call is isolated and short: if cbas is down (e.g. a restart test
        failed mid-restart) a transport error on one object must not skip the
        rest, and a long per-object timeout would stall the whole teardown.
        """
        node = self.cbas_nodes[0]

        def best_effort(what, call):
            try:
                resp = call()
            except Exception as exc:
                self.log.warning(f"Could not {what}: {exc}")
                return
            if resp.status_code not in (200, 404):
                self.log.warning(f"Could not {what}: HTTP "
                                 f"{resp.status_code}: {resp.text[:200]}")

        for name in reversed(self._links):
            best_effort(f"disconnect link {name}", lambda name=name:
                        IPCAnalyticsUtils.admin_query(
                            node, f"DISCONNECT LINK {name};", timeout=30))
        for name in reversed(self._datasets):
            best_effort(f"drop dataset {name}", lambda name=name:
                        IPCAnalyticsUtils.admin_query(
                            node, f"DROP DATASET {name} IF EXISTS;",
                            timeout=30))
        for name in reversed(self._links):
            best_effort(f"drop link {name}", lambda name=name:
                        IPCAnalyticsUtils.link_request(
                            node, name, method="DELETE", timeout=30))

    def prepare_remote(self, remote, ca_cert):
        """Trust `ca_cert` on the remote cluster and turn on client cert
        auth there; tearDown undoes both and puts the remote setting back
        to its derived default."""
        before = {entry["id"] for entry in requests.get(
            f"http://{remote.ip}:8091/pools/default/trustedCAs",
            auth=self._auth_of(remote), timeout=30).json()}
        stored, _ = self.localhost_diag_eval(
            remote, f"ns_config:search({self.IPC_CONFIG_KEY}).")
        self._remote = {"server": remote, "cas": before,
                        "ipc_stored": stored.strip(), "users": []}
        # Loaded through the remote's own REST, and kept off the local
        # cleanup list: the same id on the local cluster is a different CA.
        tracked = list(self.crl_utils.trusted_ca_ids)
        self.crl_utils.trust_ca_on_cluster(RestConnection(remote), remote,
                                           ca_cert)
        self.crl_utils.trusted_ca_ids[:] = tracked
        resp = requests.post(
            f"http://{remote.ip}:8091/settings/clientCertAuth",
            auth=self._auth_of(remote),
            json={"state": "enable", "prefixes": self.DEFAULT_CCA_PREFIXES},
            timeout=30)
        resp.raise_for_status()

    def create_remote_user(self, remote, username, password, roles):
        """A local user on the remote cluster; tearDown deletes it."""
        IPCUtils.create_local_user(
            remote, username, password, roles).raise_for_status()
        self._remote["users"].append(username)

    def set_remote_ipc(self, remote, enabled, timeout=60):
        """Set the remote cluster's setting and wait until it reports it."""
        resp = requests.post(
            f"http://{remote.ip}:8091/internalSettings",
            auth=self._auth_of(remote),
            data={self.IPC_SETTING: "true" if enabled else "false"},
            timeout=30)
        resp.raise_for_status()
        end = time.time() + timeout
        while time.time() < end:
            current = requests.get(
                f"http://{remote.ip}:8091/internalSettings",
                auth=self._auth_of(remote), timeout=30).json()
            if current.get(self.IPC_SETTING) is enabled:
                self.log.info(f"Remote {remote.ip}: "
                              f"{self.IPC_SETTING}={enabled}")
                return
            time.sleep(2)
        self.fail(f"Remote {remote.ip} did not report "
                  f"{self.IPC_SETTING}={enabled} within {timeout}s")

    def localhost_diag_eval(self, server, code):
        """/diag/eval from the node itself: a cluster the framework did not
        set up accepts it from localhost only."""
        shell = RemoteMachineShellConnection(server)
        try:
            out, err = shell.execute_command(
                f"curl -s -u {server.rest_username}:{server.rest_password} "
                f"-X POST http://127.0.0.1:8091/diag/eval -d '{code}'")
        finally:
            shell.disconnect()
        return (out or [""])[0], err

    def remote_cluster_ca(self, remote):
        """PEM of the CA that signed the remote node's own certificate."""
        for entry in requests.get(
                f"http://{remote.ip}:8091/pools/default/trustedCAs",
                auth=self._auth_of(remote), timeout=30).json():
            if entry.get("type") == "generated":
                return entry["pem"]
        self.fail(f"No generated CA in {remote.ip}'s trustedCAs")

    def _restore_remote(self):
        remote, before = self._remote["server"], self._remote["cas"]
        auth = self._auth_of(remote)
        stored = self._remote["ipc_stored"]

        def restore_cca():
            requests.post(
                f"http://{remote.ip}:8091/settings/clientCertAuth",
                auth=auth, json={"state": "disable",
                                 "prefixes": self.DEFAULT_CCA_PREFIXES},
                timeout=30)

        def restore_ipc():
            if stored.startswith("{value,"):
                # Explicitly set before the test: put that value back.
                requests.post(
                    f"http://{remote.ip}:8091/internalSettings", auth=auth,
                    data={self.IPC_SETTING: stored[len("{value,"):-1]},
                    timeout=30)
            else:
                # Derived before the test: remove the key the test wrote.
                self.localhost_diag_eval(
                    remote, f"ns_config:delete({self.IPC_CONFIG_KEY}).")

        def delete_user(user):
            requests.delete(
                f"http://{remote.ip}:8091/settings/rbac/users/local/{user}",
                auth=auth, timeout=30)

        steps = [("clientCertAuth", restore_cca), ("IPC key", restore_ipc)]
        steps += [(f"user {user}", lambda user=user: delete_user(user))
                  for user in self._remote["users"]]
        for step, call in steps:
            try:
                call()
            except Exception as exc:
                self.log.warning(f"Remote {step} restore error: {exc}")
        try:
            for entry in requests.get(
                    f"http://{remote.ip}:8091/pools/default/trustedCAs",
                    auth=auth, timeout=30).json():
                if entry["id"] not in before:
                    requests.delete(
                        f"http://{remote.ip}:8091/pools/default/trustedCAs/"
                        f"{entry['id']}", auth=auth, timeout=30)
        except Exception as exc:
            self.log.warning(f"Remote trusted CA cleanup error: {exc}")

    def internode_tls_level(self):
        """clusterEncryptionLevel if every node has node-to-node
        encryption on, else 'off'."""
        resp = requests.get(
            f"http://{self.cluster.master.ip}:8091/pools/default",
            auth=self._auth_of(self.cluster.master), timeout=30)
        resp.raise_for_status()
        body = resp.json()
        if all(node.get("nodeEncryption") for node in body["nodes"]):
            return body.get("clusterEncryptionLevel")
        return "off"

    def disable_internode_tls(self):
        """Node-to-node encryption off -- the framework turns it on in
        setUp unless enforce_tls=False -- and wait for cbas to serve."""
        if self.internode_tls_level() != "off":
            self.set_n2n_encryption(self.cluster.servers, "off")
            self.wait_for_analytics(self.cbas_nodes)

    def cbas_only_node(self):
        """An Analytics node that runs no kv, so it can leave the cluster
        without taking the data with it."""
        kv_ips = {node.ip for node in self.nodes_with_service("kv")}
        for node in self.cbas_nodes:
            if node.ip not in kv_ips:
                return node
        self.fail(f"No Analytics node without kv among "
                  f"{[node.ip for node in self.cbas_nodes]}")

    def enable_internode_tls(self, level="all"):
        """Node-to-node encryption at `level`; tearDown turns it off. cbas
        restarts to pick the change up, so wait for it to serve again."""
        self._n2n_changed = True
        self.set_n2n_encryption(self.cluster.servers, level)
        self.wait_for_analytics(self.cbas_nodes)

    def enable_all_audit(self):
        """Audit on with nothing disabled; tearDown restores the original
        settings."""
        resp = IPCAnalyticsUtils.get_audit(self.cluster.master)
        resp.raise_for_status()
        current = resp.json()
        self._audit_restore = (
            current.get("auditdEnabled", False),
            [entry["id"] if isinstance(entry, dict) else entry
             for entry in current.get("disabled", [])])
        IPCAnalyticsUtils.set_audit(self.cluster.master, True,
                                    []).raise_for_status()

    def load_docs(self, bucket, items, prefix="ipcan", master=None):
        """Insert `items` JSON docs into `bucket` with cbworkloadgen on
        `master` (default this cluster's)."""
        master = master or self.cluster.master
        shell = RemoteMachineShellConnection(master)
        try:
            out, err = shell.execute_command(
                f"/opt/couchbase/bin/cbworkloadgen -n 127.0.0.1:8091 "
                f"-u {master.rest_username} -p {master.rest_password} "
                f"-b {bucket} -i {items} -r 1 -j -t 4 --prefix={prefix}")
        finally:
            shell.disconnect()
        self.log.info(f"Loaded {items} docs into {bucket} ({prefix}): "
                      f"{(out or [''])[-1:]} {err[-1:] if err else ''}")

    def analytics_admin_exec(self, statement):
        """Run a fixture statement as Administrator over plain 8095; any
        failure is fatal, since dirty fixtures make later verdicts lie."""
        resp = IPCAnalyticsUtils.admin_query(self.cbas_nodes[0],
                                             statement)
        if resp.status_code != 200:
            self.fail(f"Fixture statement failed: {statement} -- HTTP "
                      f"{resp.status_code}: {resp.text[:300]}")
        return resp.json()

    def dataset_count(self, node, dataset, cert=None):
        """COUNT(*) of `dataset` queried on `node` over 18095 with the
        certificate + Administrator, or None if the query failed."""
        resp = self.statement(node, f"SELECT VALUE COUNT(*) FROM {dataset};",
                              cert=cert, auth=self.admin_auth())
        if resp.status_code != 200:
            self.log.info(f"COUNT on {node.ip}: HTTP {resp.status_code}: "
                          f"{resp.text[:200]}")
            return None
        return resp.json()["results"][0]

    def wait_for_dataset_count(self, dataset, expected, cert=None,
                               nodes=None, timeout=300):
        """Every Analytics node reports `expected` rows in `dataset`."""
        pending = list(nodes or self.cbas_nodes)
        end = time.time() + timeout
        last = {}
        while pending and time.time() < end:
            for node in list(pending):
                last[node.ip] = self.dataset_count(node, dataset, cert=cert)
                if last[node.ip] == expected:
                    pending.remove(node)
            if pending:
                time.sleep(5)
        if pending:
            self.fail(f"{dataset} did not reach {expected} rows within "
                      f"{timeout}s on {[n.ip for n in pending]}; last {last}")

    def settled_kv_count(self, bucket, timeout=120):
        """KV itemCount once two reads 10s apart agree. This, not what the
        loader reported, is what Analytics must end up holding."""
        end = time.time() + timeout
        last = None
        while time.time() < end:
            count = IPCAnalyticsUtils.bucket_item_count(self.cluster.master,
                                                        bucket)
            if count == last:
                return count
            last = count
            time.sleep(10)
        self.fail(f"KV itemCount of {bucket} did not settle within "
                  f"{timeout}s; last {last}")

    def pin_master(self, node):
        """Make `node` the framework's master, so REST that must outlive
        a topology change never goes through the node being removed."""
        self.cluster.master = node
        self.rest = RestConnection(node)
        self.cluster_rest = ClusterRestAPI(node)

    def analytics_log_count(self, node, pattern,
                            log="analytics_info.log"):
        """Number of lines in one Analytics log on `node` matching the
        fixed string `pattern`."""
        shell = RemoteMachineShellConnection(node)
        try:
            out, _ = shell.execute_command(
                f"grep -cF '{pattern}' "
                f"/opt/couchbase/var/lib/couchbase/logs/{log} || true")
        finally:
            shell.disconnect()
        text = (out[0] if out else "0").strip()
        return int(text) if text.isdigit() else 0

    LOG_DIR = "/opt/couchbase/var/lib/couchbase/logs"

    def log_baselines(self, node, files):
        """{file: line count} for `files` (globs under the logs directory)
        on `node`, so a later scan only sees lines written after this."""
        shell = RemoteMachineShellConnection(node)
        try:
            out, _ = shell.execute_command(
                f"cd {self.LOG_DIR} && for f in {' '.join(files)}; do "
                f"[ -f \"$f\" ] && echo \"$f $(wc -l < \"$f\")\"; done; "
                f"true")
        finally:
            shell.disconnect()
        baselines = {}
        for line in out or []:
            name, _, count = line.strip().rpartition(" ")
            if name and count.isdigit():
                baselines[name] = int(count)
        return baselines

    def grep_node_logs(self, node, needle, baselines):
        """Lines (max 5 per file) containing the fixed string `needle`
        written to each file in `baselines` after its recorded line count.
        A file rotated since then is read from its start."""
        shell = RemoteMachineShellConnection(node)
        try:
            out, _ = shell.execute_command(
                f"cd {self.LOG_DIR} && " + " ".join(
                    f"[ -f '{name}' ] && {{ n={count}; "
                    f"[ $(wc -l < '{name}') -lt $n ] && n=0; "
                    f"tail -n +$((n+1)) '{name}' | grep -F -m 5 -- "
                    f"'{needle}' | sed 's|^|{name}: |'; }};"
                    for name, count in baselines.items()) + " true")
        finally:
            shell.disconnect()
        return [line for line in (out or []) if line.strip()]

    ANALYTICS_SELECT_AUDIT_ID = 36867

    # Hyracks inter-node channels (cluster, client, NC cluster, data, result,
    # messaging, replication). 9110/9111 are HTTP listeners and 9122 is
    # loopback only, so neither belongs here.
    INTERNODE_PORTS = (9112, 9113, 9115, 9116, 9117, 9118, 9120)

    def internode_ports(self, node):
        """INTERNODE_PORTS that `node` actually listens on outside
        loopback -- only the cluster controller has 9112/9113."""
        shell = RemoteMachineShellConnection(node)
        try:
            out, _ = shell.execute_command("ss -tlnH")
        finally:
            shell.disconnect()
        listening = set()
        for line in out or []:
            fields = line.split()
            if len(fields) < 4 or fields[3].startswith(("127.", "[::1]")):
                continue
            port = fields[3].rsplit(":", 1)[-1]
            if port.isdigit():
                listening.add(int(port))
        return [port for port in self.INTERNODE_PORTS if port in listening]

    def internode_outcomes(self, node, port, certs, admin_frame,
                           non_admin_frame, ipc_enabled):
        """Probe one inter-node port and return its failures.

        With the setting off only a silent connection is attempted: anything
        sent after the handshake is parsed as mux traffic, and a crafted
        frame there kills the process."""
        probe = IPCAnalyticsUtils.internode_probe
        where = f"{node.ip}:{port}"
        failures = []
        if not ipc_enabled:
            result = probe(node.ip, port, certs["forged"], wait=12)
            self.log.info(f"{where} forged silent (setting off): {result}")
            if result["outcome"] != "open":
                failures.append(f"{where}: with the setting off the forged "
                                f"certificate should be accepted without "
                                f"credentials (connection held), got "
                                f"{result}")
            return failures

        timeout = self.input.param("auth_handshake_timeout", 5)
        checks = (
            ("forged silent", certs["forged"], b"", 0,
             lambda r: r["outcome"] == "closed"
             and r["elapsed"] <= timeout + 5),
            ("forged + Administrator", certs["forged"], admin_frame, 4,
             lambda r: r["outcome"] == "reply" and r["status"] == 200),
            ("forged + analytics_admin", certs["forged"], non_admin_frame, 4,
             lambda r: r["outcome"] == "reply" and r["status"] == 401),
            ("user certificate", certs["user"], b"", 0,
             lambda r: r["outcome"] in ("closed", "tls_reject")
             and r["elapsed"] < 3),
            ("untrusted CA", certs["untrusted"], b"", 0,
             lambda r: r["outcome"] in ("closed", "tls_reject")
             and r["elapsed"] < 3),
            ("no certificate", None, b"", 0,
             lambda r: r["outcome"] in ("closed", "tls_reject")
             and r["elapsed"] < 3),
        )
        for name, cert, send, reply_len, ok in checks:
            result = probe(node.ip, port, cert, send=send, wait=timeout + 7,
                           reply_len=reply_len)
            self.log.info(f"{where} {name}: {result}")
            if not ok(result):
                failures.append(f"{where} {name}: unexpected {result}")
        return failures

    def wait_for_internode_enforcement(self, node, port, cert, enabled,
                                       timeout=60):
        """cbas caches certificate lookups; poll until a silent forged
        connection gets the new answer."""
        end = time.time() + timeout
        want = "closed" if enabled else "open"
        result = None
        while time.time() < end:
            result = IPCAnalyticsUtils.internode_probe(node.ip, port, cert,
                                                       wait=10)
            if result["outcome"] == want:
                return
            time.sleep(2)
        self.fail(f"{node.ip}:{port} did not switch to '{want}' for a silent "
                  f"forged connection within {timeout}s; last {result}")

    def audit_line_count(self, node):
        shell = RemoteMachineShellConnection(node)
        try:
            out, _ = shell.execute_command(
                f"wc -l < {self.AUDIT_LOG_PATH} 2>/dev/null || echo 0")
        finally:
            shell.disconnect()
        text = (out[0] if out else "0").strip()
        return int(text) if text.isdigit() else 0

    def audit_events_since(self, node, baseline, event_id):
        """Audit events with `event_id` written on `node` after line
        `baseline` of the current audit log."""
        shell = RemoteMachineShellConnection(node)
        try:
            out, _ = shell.execute_command(
                f"tail -n +{baseline + 1} {self.AUDIT_LOG_PATH} | "
                f"grep -F '\"id\":{event_id},' || true")
        finally:
            shell.disconnect()
        events = []
        for line in out or []:
            try:
                events.append(json.loads(line))
            except ValueError:
                continue
        return events

    def wait_for_audit_users(self, baselines, event_id, user, timeout=60):
        """Poll every Analytics node's audit log, written asynchronously,
        until an `event_id` event names `user`. Returns every user named by
        such events since `baselines`."""
        end = time.time() + timeout
        users = []
        while True:
            events = []
            for node in self.cbas_nodes:
                events += self.audit_events_since(node, baselines[node.ip],
                                                  event_id)
            users = [event.get("real_userid", {}).get("user")
                     for event in events]
            if user in users or time.time() >= end:
                return users
            time.sleep(3)

    def rebalance_out_in(self, node):
        """Rebalance `node` out, wait for it to reset, add it back with
        the cbas service and rebalance it in."""
        self.rebalance_cluster(ejected=[node])
        self.wait_until_uninitialized(node)
        self.rest.add_node(node.rest_username, node.rest_password, node.ip,
                           services=["cbas"])
        self.rebalance_cluster()

    def failover_and_recover(self, node):
        """Hard failover of `node`, full recovery, rebalance."""
        self.assertTrue(self.rest.fail_over(self.otp_of(node),
                                            graceful=False),
                        f"Hard failover of {node.ip} failed")
        self.assertTrue(self.rest.set_recovery_type(self.otp_of(node), "full"),
                        f"Setting full recovery for {node.ip} failed")
        self.rebalance_cluster()

    def kill_cbas(self, node):
        """SIGKILL the cbas process; the babysitter restarts it. -x
        matches the process name only, so the SSH session running the
        command cannot match itself."""
        shell = RemoteMachineShellConnection(node)
        try:
            shell.execute_command("pkill -9 -x cbas")
        finally:
            shell.disconnect()

    def create_analytics_bucket(self, name, ram_quota=256):
        self.bucket_util.create_default_bucket(
            self.cluster, bucket_name=name, ram_quota=ram_quota, replica=0,
            storage=Bucket.StorageBackend.couchstore)

    def cc_node_ip(self):
        """IP of the Analytics cluster controller, from /analytics/cluster."""
        resp = IPCAnalyticsUtils.get_cluster(self.cbas_nodes[0])
        resp.raise_for_status()
        body = resp.json()
        for node in body.get("nodes", []):
            if node.get("nodeId") == body.get("ccNodeId"):
                return node["nodeName"].split(":")[0]
        self.fail(f"No Analytics node matches ccNodeId in {body}")

    def statement(self, node, statement, cert=None, auth=None,
                  proxied=False):
        """Run one statement on `node` (direct 18095, or the 18091 proxy).
        Returns the Response."""
        return self._retry_on_timeout(
            IPCAnalyticsUtils.query, node.ip, statement, cert=cert,
            auth=auth, proxied=proxied)

    def check_statement(self, node, statement, cert, auth, expected,
                        proxied, what, cc_ip):
        """Run one statement and return [] or a one-item failure list."""
        resp = self.statement(node, statement, cert=cert, auth=auth,
                              proxied=proxied)
        role = "" if cc_ip is None else (
            "CC, " if node.ip == cc_ip else "non-CC, ")
        where = f"{node.ip} ({role}{'proxy' if proxied else 'direct'})"
        self.log.info(f"{where} {what}: HTTP {resp.status_code}")
        if resp.status_code == expected:
            return []
        return [f"{where} {what}: expected {expected}, got "
                f"{resp.status_code}: {resp.text[:200]}"]

    def nodes_with_service(self, service, cluster=None):
        """Server objects of `cluster` (default this one) running
        `service`, read from /pools/default. The framework's per-service
        lists are not refreshed for every cluster of a multi-cluster run."""
        cluster = cluster or self.cluster
        resp = requests.get(
            f"http://{cluster.master.ip}:8091/pools/default",
            auth=self._auth_of(cluster.master), timeout=30)
        resp.raise_for_status()
        ips = {node["hostname"].rsplit(":", 1)[0]
               for node in resp.json()["nodes"]
               if service in node.get("services", [])}
        return [server for server in cluster.servers if server.ip in ips]

    def admin_auth(self):
        return (self.cluster.master.rest_username,
                self.cluster.master.rest_password)

    def wait_for_analytics(self, nodes, timeout=300):
        """Rebalance finishing does not mean cbas is serving yet."""
        pending = list(nodes)
        end = time.time() + timeout
        while pending and time.time() < end:
            for node in list(pending):
                try:
                    resp = IPCAnalyticsUtils.admin_query(node, "SELECT 1;")
                    if resp.status_code == 200:
                        pending.remove(node)
                except requests.exceptions.RequestException as exc:
                    self.log.debug(f"Analytics on {node.ip} not up: {exc}")
            if pending:
                self.sleep(5, "Waiting for the Analytics service")
        if pending:
            self.fail(f"Analytics did not serve within {timeout}s on "
                      f"{[node.ip for node in pending]}")

    def analytics_call(self, node, endpoint, cert=None, auth=None,
                       proxied=False):
        """One call to a named ENDPOINTS entry. Returns the Response."""
        method, path, statement, _ = self.ENDPOINTS[endpoint]
        if proxied:
            return IPCAnalyticsUtils.query(node.ip, statement, cert=cert,
                                           auth=auth, proxied=True)
        data = {"statement": statement} if statement else None
        return IPCAnalyticsUtils.request(node.ip, path, cert=cert, auth=auth,
                                         method=method, data=data)

    def analytics_outcomes(self, cert, nodes=None, auth=None):
        """{(node ip, endpoint or 'proxy'): status} over every Analytics node
        and every ENDPOINTS entry, plus the /_p/cbas/ proxy on each node.

        A TLS-layer rejection is recorded as 'tls_reject': the certificate is
        meant to stay valid for the connection and only stop naming anyone.
        Bodies are kept for the leak check under the same key.
        """
        outcomes, bodies = {}, {}
        for node in nodes or self.cbas_nodes:
            calls = [(name, False) for name in self.ENDPOINTS]
            calls.append(("query", True))
            for name, proxied in calls:
                key = (node.ip, "proxy" if proxied else name)
                try:
                    resp = self._retry_on_timeout(
                        self.analytics_call, node, name, cert=cert,
                        auth=auth, proxied=proxied)
                    outcomes[key], bodies[key] = resp.status_code, resp.text
                except requests.exceptions.SSLError as exc:
                    outcomes[key], bodies[key] = self.TLS_REJECT, str(exc)
        return outcomes, bodies

    def expected_cert_only(self, ipc_enabled):
        """Expected status per endpoint for the internal certificate alone."""
        expected = {name: 200 if (answered and not ipc_enabled) else 401
                    for name, (_, _, _, answered) in self.ENDPOINTS.items()}
        expected["proxy"] = 401 if ipc_enabled else 200
        return expected

    def assert_analytics_cert_only(self, cert, ipc_enabled, label,
                                   nodes=None):
        """Every endpoint on every Analytics node gives the expected answer to
        the certificate alone, and no refusal leaks anything. All mismatches
        are collected so one run shows the whole picture."""
        expected = self.expected_cert_only(ipc_enabled)
        outcomes, bodies = self.analytics_outcomes(cert, nodes=nodes)
        failures = []
        for (ip, name), status in sorted(outcomes.items()):
            if status != expected[name]:
                failures.append(f"{ip} {name}: expected {expected[name]}, "
                                f"got {status}: {bodies[(ip, name)][:150]}")
            if status != 200:
                leaked = [m for m in self.LEAK_MARKERS
                          if m in bodies[(ip, name)]]
                if leaked:
                    failures.append(f"{ip} {name}: refusal leaks {leaked}")
        self.log.info(f"{label}: cert-only outcomes "
                      f"({self.IPC_SETTING}={ipc_enabled}): {outcomes}")
        if failures:
            self.fail(f"{label}: the certificate alone was not answered as "
                      f"expected with {self.IPC_SETTING}={ipc_enabled}:\n  "
                      + "\n  ".join(failures))

    def assert_analytics_admin_control(self, cert, nodes=None):
        """Positive control: certificate + Administrator reaches every
        endpoint on every node, so a refusal later can only be about the
        certificate, never a wrong path or a down node."""
        outcomes, bodies = self.analytics_outcomes(cert, nodes=nodes,
                                                   auth=self.admin_auth())
        failures = [f"{ip} {name}: {status}: {bodies[(ip, name)][:150]}"
                    for (ip, name), status in sorted(outcomes.items())
                    if status != 200]
        if failures:
            self.fail("Positive control failed -- certificate + "
                      "Administrator should reach every Analytics endpoint:"
                      "\n  " + "\n  ".join(failures))

    def wait_for_analytics_cert_only(self, cert, ipc_enabled, nodes=None):
        """Poll the cluster endpoint until every node answers the certificate
        alone the new way. cbas caches certificate lookups for a few seconds,
        so a runtime toggle is not visible at once."""
        expected = 401 if ipc_enabled else 200
        pending = list(nodes or self.cbas_nodes)
        end = time.time() + self.analytics_toggle_timeout
        last = {}
        while pending and time.time() < end:
            for node in list(pending):
                last[node.ip] = self.analytics_call(node, "cluster",
                                                    cert=cert).status_code
                if last[node.ip] == expected:
                    pending.remove(node)
            if pending:
                time.sleep(1)
        if pending:
            self.fail(f"Analytics did not start answering {expected} to the "
                      f"certificate alone within "
                      f"{self.analytics_toggle_timeout}s on "
                      f"{[node.ip for node in pending]}; last {last}")
