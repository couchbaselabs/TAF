"""
Community Edition Restrictions Test Module

Validates ns_server enforcement of Community Edition limitations:
- 5-node cluster limit
- EE-only service restrictions (analytics, eventing, backup)
- CE topology restrictions (no query-only nodes)

Ported from TAF master's pytests/ns_server/ce_restrictions.py.

master_jython lacks cb_server_rest_util (ClusterRestAPI /
ClusterInitializationProvision) and
couchbase_utils/rebalance_utils/rebalance_util.py. This port is rewritten
against the classic monolithic RestConnection class
(lib/membase/api/rest_client.py):

- ClusterRestAPI(master)         -> RestConnection(master)
- ClusterInitializationProvision().initialize_cluster(...) -> a direct
  POST to the server's /clusterInit REST endpoint via
  RestConnection._http_request(), which is the same endpoint the newer
  wrapper ultimately calls. This keeps the exact CE-enforcement behavior
  under test (services / sendStats validation at cluster-init time)
  while removing the framework dependency.
- self.rest.request(url, method, body) -> self.rest._http_request(
  url, method, body); both return a (status, content, header) 3-tuple,
  but _http_request expects an already url-encoded string body rather
  than a raw dict, so dict bodies are encoded with urllib.urlencode()
  before the call.
- RestConnection.get_nodes(inactive_added=True) replaces
  ClusterUtils.get_nodes(master, inactive_added=True) -- on this branch
  ClusterUtils.get_nodes() takes no inactive_added kwarg.
- RestConnection.add_node()/eject_node() raise on failure instead of
  returning a (status, content) tuple, so call sites that need to
  classify success/failure wrap the call in try/except.
- shell_util.remote_connection.RemoteMachineShellConnection ->
  remote.remote_util.RemoteMachineShellConnection
"""

import json
import urllib

from basetestcase import ClusterSetup
from cb_tools.cb_cli import CbCli
from cluster_utils.cluster_ready_functions import ClusterUtils
from custom_exceptions.exception import RebalanceFailedException
from membase.api.rest_client import RestConnection
from ns_server.ce_restrictions_util import CommunityEditionRestrictionsUtil
from remote.remote_util import RemoteMachineShellConnection


class CommunityEditionRestrictions(ClusterSetup):
    """Test class for Community Edition restriction enforcement."""

    def setUp(self):
        super(CommunityEditionRestrictions, self).setUp()
        self.rest = RestConnection(self.cluster.master)
        self.ce_util = CommunityEditionRestrictionsUtil(
            self.cluster, self.cluster_util, self.log, self.sleep)

        if self.cluster_util.is_enterprise_edition(self.cluster):
            self.fail("Test requires Community Edition cluster. "
                      "Install with edition=community parameter.")

        self.log.info("Cluster edition: %s" % self.cluster.edition)

    def tearDown(self):
        self.ce_util.cleanup_pending_nodes()
        super(CommunityEditionRestrictions, self).tearDown()

    def test_ce_5_node_limit_restart_persistence(self):
        """
        Validates:
        1. CE cluster cannot rebalance with >5 nodes
        2. Restriction persists across master restart
        3. Recovery path (scale-down) works
        """
        if len(self.cluster.servers) < 6:
            self.fail("Requires 6 servers (5 cluster + 1 test node)")

        self.ce_util.verify_ce_edition_via_diag_eval()

        # Verify cluster has 5 active nodes
        active_nodes = self.ce_util.get_active_node_count()
        self.assertEqual(active_nodes, self.ce_util.CE_NODE_LIMIT,
                         "Expected %d nodes, found %d"
                         % (self.ce_util.CE_NODE_LIMIT, active_nodes))

        node_to_add = self.cluster.servers[self.nodes_init]
        self.log.info("Adding 6th node: %s" % node_to_add.ip)
        self.ce_util.add_node_without_rebalance(node_to_add, services=["kv"])
        self.ce_util.verify_node_in_pending_state(node_to_add.ip)

        # Rebalance should fail
        self.log.info("Verifying rebalance fails with >5 nodes")
        self.ce_util.attempt_rebalance_expect_failure()

        # Restart master and verify persistence
        self.log.info("Restarting master to verify persistence")
        self.ce_util.restart_couchbase_server(self.cluster.master)
        self.ce_util.verify_node_in_pending_state(node_to_add.ip)

        self.log.info("Verifying restriction persists after restart")
        self.ce_util.attempt_rebalance_expect_failure()

        self.log.info("Removing extra node to restore compliance")
        self.ce_util.rebalance_out_node(node_to_add)

        final_count = self.ce_util.get_active_node_count()
        self.assertEqual(final_count, self.ce_util.CE_NODE_LIMIT,
                         "Expected %d nodes after recovery, found %d"
                         % (self.ce_util.CE_NODE_LIMIT, final_count))

        self.log.info("CE 5-node limit enforcement validated")

    def test_ce_reject_ee_only_services(self):
        """
        Validates CE rejects EE-only services via CLI:
        - analytics: EE only
        - eventing: EE only
        - backup: EE only
        - query-only: Invalid CE topology
        """
        test_node = self._get_available_node()

        # EE-only services with expected error messages
        ee_services = [
            ("analytics", "analytics service is only available on Enterprise Edition"),
            ("eventing", "eventing service is only available on Enterprise Edition"),
            ("backup", "backup service is only available on Enterprise Edition"),
            ("data,analytics", "analytics service is only available on Enterprise Edition"),
        ]

        for services, expected_error in ee_services:
            success, error_msg = self.ce_util.add_node_via_cli(
                test_node, services, expect_success=False)
            self.assertTrue(success, "Services '%s' should be rejected" % services)
            self.assertIn(expected_error, error_msg,
                          "Expected '%s' in: %s" % (expected_error, error_msg))
            self.log.info("Rejected '%s': %s" % (services, error_msg))
            self.sleep(8, "Wait after ejection")

        # Query-only topology restriction
        success, error_msg = self.ce_util.add_node_via_cli(
            test_node, "query", expect_success=False)
        self.assertTrue(success, "Query-only should be rejected")
        self.assertIn("Community Edition only supports", error_msg)
        self.log.info("Rejected 'query-only': %s" % error_msg)
        self.sleep(8, "Wait after ejection")

    def test_ce_allow_valid_service_combinations(self):
        """
        Validates CE allows valid service combinations:
        - data (kv)
        - data,query,index (kv,n1ql,index)
        - data,query,index,fts (kv,n1ql,index,fts)
        """
        test_node = self._get_available_node()
        services = "kv,index,n1ql,fts"

        self.log.info("Adding node %s with services: %s" % (test_node.ip, services))
        self.ce_util.add_node_via_rest_and_rebalance_in(test_node, services)
        self.log.info("Rebalance-in succeeded")

        # Cleanup
        self.ce_util.rebalance_out_node(test_node)
        self.log.info("Cleanup completed")

    # ------------------------------------------------------------------
    # CE valid-service constants and reset/init helpers (CBQE-8979)
    # ------------------------------------------------------------------

    _CE_VALID_SERVICES = frozenset(["kv", "index,kv,n1ql", "fts,index,kv,n1ql"])

    def _cluster_init_rest(self, services, memory_quota=256,
                           index_memory_quota=256, fts_memory_quota=256,
                           cluster_name=None, send_stats=None):
        """POST directly to /clusterInit -- the same single-shot REST
        endpoint the newer ClusterInitializationProvision wrapper calls
        on master. Returns (status, content)."""
        master = self.cluster.master
        params = {
            "hostname": master.ip,
            "port": "SAME",
            "username": master.rest_username,
            "password": master.rest_password,
            "services": services,
            "memoryQuota": memory_quota,
            "indexMemoryQuota": index_memory_quota,
            "ftsMemoryQuota": fts_memory_quota,
        }
        if cluster_name is not None:
            params["clusterName"] = cluster_name
        if send_stats is not None:
            params["sendStats"] = "true" if send_stats else "false"
        api = self.rest.baseUrl + "clusterInit"
        status, content, _ = self.rest._http_request(
            api, "POST", urllib.urlencode(params))
        return status, content

    def _reset_and_init_master(self, services):
        """Hard-reset master, wait for it to come back, then initialize
        with the given services string.  Returns (status, content)."""
        master = self.cluster.master
        self.rest.reset_node()
        self.cluster_util.wait_for_ns_servers_or_assert([master])
        return self._cluster_init_rest(services)

    def _get_otp_nodes(self, inactive_added=False):
        return self.rest.get_nodes(inactive_added=inactive_added)

    @staticmethod
    def _as_json(raw):
        """_http_request() never auto-decodes JSON, so raw is normally a
        (possibly bytes) response body string that still needs
        json.loads(). Raises (TypeError, ValueError) on bad input."""
        if isinstance(raw, (dict, list)):
            return raw
        if isinstance(raw, bytes):
            raw = raw.decode()
        return json.loads(raw)

    def _reb_and_wait(self, timeout=120):
        """Trigger rebalance with all known nodes and wait for completion.
        Returns (success, error_msg)."""
        nodes = self._get_otp_nodes(inactive_added=True)
        status, content = self.rest.rebalance(
            otpNodes=[n.id for n in nodes], ejectedNodes=[])
        if not status:
            return False, content.decode() if isinstance(content, bytes) \
                else str(content)
        try:
            reb_ok = self.rest.monitorRebalance()
        except RebalanceFailedException as e:
            return False, str(e)
        return (True, "") if reb_ok else (False, "rebalance did not complete")

    def _eject_rebalance_out(self, node_ip, timeout=120):
        """Rebalance out an active member by IP and wait for completion."""
        nodes = self._get_otp_nodes()
        spare_otp = next((n.id for n in nodes if n.ip == node_ip), None)
        if not spare_otp:
            return
        ClusterUtils.rebalance(self.cluster, wait_for_completion=True,
                               ejected_nodes=[spare_otp])

    def _get_available_node(self):
        """Get a node not currently in the cluster."""
        available = [s for s in self.cluster.servers
                     if s not in self.cluster.nodes_in_cluster]
        if not available:
            self.fail("No available nodes. Requires at least 2 nodes in node.ini")
        return available[0]

    def _cli_on(self, node):
        shell = RemoteMachineShellConnection(node)
        cb_cli = CbCli(shell, username=node.rest_username,
                       password=node.rest_password)
        return shell, cb_cli

    @staticmethod
    def _combined(output, error):
        return "\n".join((output or []) + (error or []))

    def _add_node_rest(self, node, services):
        """RestConnection.add_node() raises on failure instead of
        returning a (status, content) tuple. Wrap it so callers can use
        REST (status, msg) semantics like master's ClusterRestAPI did."""
        try:
            otp_node = self.rest.add_node(
                user=node.rest_username, password=node.rest_password,
                remoteIp=node.ip, services=services.split(","))
            return True, otp_node.id
        except Exception as exc:
            return False, str(exc)

    # ------------------------------------------------------------------
    # CLI enforcement tests (CBQE-8973)
    # ------------------------------------------------------------------

    def test_ce_phonehome_enforcement(self):
        """
        CE enforcement of sendStats/PhoneHome -- REST post-init, CLI init, and
        REST /clusterInit covered in one test pass.

        Steps:
        1. REST: POST /settings/stats sendStats=false rejected on running cluster
        2. Reset master; CLI: cluster-init --update-notifications 0 -> rejected
           or forced true
        3. If CLI rejected (node still uninitialized): REST /clusterInit
           sendStats=false -> rejected or forced true

        Covers CBQE-8972 + CBQE-8973 CLI cluster-init. Requires nodes_init=1.
        """
        # --- Part 1: REST post-init -- sendStats=false must be rejected ---
        api = self.rest.baseUrl + "settings/stats"
        status, content, _ = self.rest._http_request(api, "GET")
        self.assertTrue(status,
                        "GET /settings/stats failed: %s" % content)
        settings = self._as_json(content)
        current = settings.get("sendStats", False) \
            if isinstance(settings, dict) else True
        self.assertTrue(current,
                        "CE must default to sendStats=true. Got: %s" % settings)
        self.log.info("Confirmed sendStats=true baseline: %s" % settings)

        status, content, _ = self.rest._http_request(
            api, "POST", urllib.urlencode({"sendStats": "false"}))
        err_msg = content.decode() if isinstance(content, bytes) else str(content)
        self.assertFalse(
            status,
            "CE must reject POST /settings/stats sendStats=false. Got: %s"
            % err_msg)
        self.log.info("REST rejected sendStats=false post-init: %s" % err_msg)

        status, content, _ = self.rest._http_request(api, "GET")
        self.assertTrue(status)
        settings = self._as_json(content)
        self.assertTrue(
            settings.get("sendStats", False) if isinstance(settings, dict) else True,
            "sendStats must remain true after rejected update. Got: %s" % settings)

        # --- Part 2: reset master; CLI cluster-init --update-notifications 0 ---
        self.rest.reset_node()
        master = self.cluster.master
        self.cluster_util.wait_for_ns_servers_or_assert([master])

        shell, cb_cli = self._cli_on(master)
        output, error = cb_cli.cluster_init(
            data_ramsize=256,
            index_ramsize=None,
            fts_ramsize=None,
            services="kv",
            index_storage_mode=None,
            cluster_name="CE_PhoneHome_Test",
            cluster_username=master.rest_username,
            cluster_password=master.rest_password,
            cluster_port=None,
            update_notifications=0)
        shell.disconnect()

        combined = self._combined(output, error)
        cli_rejected = bool(error or "error" in combined.lower())

        if cli_rejected:
            self.log.info("CLI rejected --update-notifications 0: %s"
                          % combined[:300])
            # Node still uninitialized -- exercise REST /clusterInit path too
            status, content = self._cluster_init_rest(
                "kv", cluster_name="CE_PhoneHome_REST_Test", send_stats=False)
            if not status:
                err = content.decode() if isinstance(content, bytes) else str(content)
                self.log.info("REST /clusterInit also rejected sendStats=false: %s"
                              % err)
            else:
                rest_init = RestConnection(master)
                status, content, _ = rest_init._http_request(
                    rest_init.baseUrl + "settings/stats", "GET")
                s = self._as_json(content)
                actual = s.get("sendStats", False) if isinstance(s, dict) else False
                self.assertTrue(
                    actual,
                    "CE must force sendStats=true at /clusterInit. Got: %s" % s)
                self.log.info("REST /clusterInit forced sendStats=true: %s" % s)
        else:
            # CLI accepted and initialized -- verify sendStats was forced true
            self.log.info("CLI accepted; verifying CE forced sendStats=true")
            rest_init = RestConnection(master)
            status, content, _ = rest_init._http_request(
                rest_init.baseUrl + "settings/stats", "GET")
            self.assertTrue(status, "GET /settings/stats failed after CLI init")
            settings = self._as_json(content)
            actual = (settings.get("sendStats", False)
                      if isinstance(settings, dict) else False)
            self.assertTrue(
                actual,
                "CE must force sendStats=true with --update-notifications 0. "
                "Got: %s" % settings)
            self.log.info("CE forced sendStats=true during CLI cluster-init")

        self.log.info("CE PhoneHome enforcement validated")

    def test_ce_cli_server_add_6th_node_rejected(self):
        """
        couchbase-cli server-add adding a 6th node must be rejected on CE.

        CE may enforce this at server-add time (immediate error) or at
        rebalance time. Both paths are validated.
        Requires nodes_init=5 and one spare node in node.ini.
        """
        if len(self.cluster.servers) < 6:
            self.fail("Requires 6 servers (5 cluster + 1 spare) in node.ini")
        if len(self.cluster.nodes_in_cluster) != 5:
            self.fail("Requires nodes_init=5")

        spare = self.cluster.servers[5]
        shell, cb_cli = self._cli_on(self.cluster.master)

        try:
            output = cb_cli.add_node(spare, "kv")
            output_str = "\n".join(output) if isinstance(output, list) \
                else str(output)
            add_failed = False
        except Exception as exc:
            output_str = str(exc)
            add_failed = True
        finally:
            shell.disconnect()

        if add_failed or "error" in output_str.lower():
            lower = output_str.lower()
            self.assertTrue(
                "community" in lower or "enterprise" in lower
                or "5" in lower or "limit" in lower,
                "Expected CE-specific error. Got: %s" % output_str)
            self.log.info("CE rejected server-add of 6th node: %s"
                          % output_str[:300])
            return

        # Node added to pending -- verify CLI rebalance is blocked
        self.log.info("server-add accepted; verifying CLI rebalance blocked")
        shell, cb_cli = self._cli_on(self.cluster.master)
        reb_out, reb_err = cb_cli.rebalance()
        shell.disconnect()

        combined = self._combined(reb_out, reb_err)
        self.assertTrue(
            reb_err or "error" in combined.lower(),
            "CE must reject rebalance with 6 nodes. Got: %s" % combined)
        self.log.info("CE blocked rebalance with 6 nodes: %s" % combined[:300])

    def test_ce_cli_mds_rebalance_rejected(self):
        """
        Rebalance-in of a query/index-only (MDS) node must fail on CE.

        CE may reject at server-add time or at rebalance time.
        Requires nodes_init=1 and one spare node in node.ini.
        """
        spare = self._get_available_node()
        shell, cb_cli = self._cli_on(self.cluster.master)

        try:
            output = cb_cli.add_node(spare, "query")
            output_str = "\n".join(output) if isinstance(output, list) \
                else str(output)
            add_failed = False
        except Exception as exc:
            output_str = str(exc)
            add_failed = True
        finally:
            shell.disconnect()

        if add_failed or "error" in output_str.lower():
            lower = output_str.lower()
            self.assertTrue(
                "community" in lower or "enterprise" in lower
                or "only supports" in lower,
                "Expected CE-specific MDS error. Got: %s" % output_str)
            self.log.info("CE rejected MDS server-add: %s" % output_str[:300])
            return

        # Node in pending -- verify CLI rebalance is blocked
        self.log.info("MDS node in pending; verifying CLI rebalance rejected")
        shell, cb_cli = self._cli_on(self.cluster.master)
        reb_out, reb_err = cb_cli.rebalance()
        shell.disconnect()

        combined = self._combined(reb_out, reb_err)
        self.assertTrue(
            reb_err or "error" in combined.lower(),
            "CE must reject rebalance with MDS topology. Got: %s" % combined)
        self.log.info("CE blocked MDS rebalance: %s" % combined[:300])

    def test_ce_cli_node_init_ee_services_rejected(self):
        """
        couchbase-cli node-init --services <ee-service> must fail on CE.

        Tests cbas, eventing, backup -- all EE-only services.
        Requires one spare node in node.ini.
        Note: --services on node-init requires Couchbase Server 8.1+.
        """
        spare = self._get_available_node()
        ee_services = ["cbas", "eventing", "backup"]

        for svc in ee_services:
            shell, cb_cli = self._cli_on(spare)
            output, error = cb_cli.node_init(
                cluster_url="localhost:8091",
                username=spare.rest_username,
                password=spare.rest_password,
                services=svc)
            shell.disconnect()

            combined = self._combined(output, error)
            self.assertTrue(
                error or "error" in combined.lower(),
                "CE must reject node-init --services %s. Got: %s"
                % (svc, combined))
            self.log.info("CE rejected node-init --services %s: %s"
                          % (svc, combined[:200]))

    # ------------------------------------------------------------------
    # Service-combination enforcement at init and add-node (CBQE-8979)
    # Replaces testrunner check_set_services and
    # check_set_services_when_add_node entries.
    # ------------------------------------------------------------------

    def test_ce_service_combinations_at_init(self):
        """CE must accept only valid service combos at /clusterInit.

        Valid: kv | index,kv,n1ql | fts,index,kv,n1ql
        All other combos (invalid MDS topology or EE-only services) must
        be rejected.

        Replaces: check_set_services (18 testrunner entries).
        Requires nodes_init=1.
        """
        combos = [
            # Valid CE service sets
            "kv",
            "index,kv,n1ql",
            "fts,index,kv,n1ql",
            # Invalid MDS topologies
            "index,kv",
            "kv,n1ql",
            "index,n1ql",
            "fts,index,kv",
            "fts,index,n1ql",
            "fts,kv,n1ql",
            # EE-only services mixed in
            "kv,eventing",
            "kv,index,n1ql,eventing",
            "fts,index,kv,n1ql,eventing",
            "kv,index,eventing",
            "kv,n1ql,eventing",
            "kv,fts,eventing",
            "fts,kv,index,eventing",
            "fts,kv,n1ql,eventing",
            "analytics,index,kv,n1ql",
        ]
        for services in combos:
            self.log.info("Testing /clusterInit services=%s" % services)
            status, content = self._reset_and_init_master(services)
            should_succeed = services in self._CE_VALID_SERVICES
            err = content.decode() if isinstance(content, bytes) else str(content)
            if should_succeed:
                self.assertTrue(
                    status,
                    "CE must allow services=%s at init. Got: %s" % (services, err))
                self.log.info("CE allowed services=%s" % services)
            else:
                self.assertFalse(
                    status,
                    "CE must block services=%s at init. Got: %s" % (services, err))
                self.log.info("CE blocked services=%s: %s" % (services, err[:120]))
        # Leave master in a valid state for tearDown
        self._reset_and_init_master("kv")

    def test_ce_service_combinations_add_node(self):
        """CE must accept/reject node-add based on service combos on both nodes.

        Expected: accept only when both start and add services are CE-valid.
        CE-valid: kv | index,kv,n1ql | fts,index,kv,n1ql

        Replaces: check_set_services_when_add_node (~69 testrunner entries).
        Requires nodes_init=1 + 1 spare.
        """
        spare = self._get_available_node()

        pairs = [
            # --- start=kv ---
            ("kv", "eventing"),
            ("kv", "kv,eventing"),
            ("kv", "index,kv,eventing"),
            ("kv", "fts,index,kv,n1ql,eventing"),
            ("kv", "kv"),
            ("kv", "index"),
            ("kv", "n1ql"),
            ("kv", "fts"),
            ("kv", "index,kv"),
            ("kv", "index,n1ql"),
            ("kv", "kv,n1ql"),
            ("kv", "index,kv,n1ql"),
            ("kv", "fts,index,kv"),
            ("kv", "fts,index,n1ql"),
            ("kv", "fts,kv,n1ql"),
            ("kv", "fts,index,kv,n1ql"),
            # --- start=index,kv,n1ql ---
            ("index,kv,n1ql", "kv"),
            ("index,kv,n1ql", "index"),
            ("index,kv,n1ql", "n1ql"),
            ("index,kv,n1ql", "fts"),
            ("index,kv,n1ql", "index,kv"),
            ("index,kv,n1ql", "index,n1ql"),
            ("index,kv,n1ql", "index,fts"),
            ("index,kv,n1ql", "fts,index,kv,n1ql"),
            ("index,kv,n1ql", "fts,index,kv"),
            ("index,kv,n1ql", "fts,index,n1ql"),
            ("index,kv,n1ql", "fts,n1ql"),
            ("index,kv,n1ql", "fts,kv,n1ql"),
            ("index,kv,n1ql", "kv,n1ql"),
            ("index,kv,n1ql", "kv,fts"),
            ("index,kv,n1ql", "index,kv,n1ql"),
            ("index,kv,n1ql", "eventing"),
            ("index,kv,n1ql", "kv,eventing"),
            ("index,kv,n1ql", "index,kv,n1ql,eventing"),
            ("index,kv,n1ql", "fts,index,kv,n1ql,eventing"),
            ("index,kv,n1ql", "analytics"),
            # --- start=fts,index,kv,n1ql ---
            ("fts,index,kv,n1ql", "kv"),
            ("fts,index,kv,n1ql", "index"),
            ("fts,index,kv,n1ql", "n1ql"),
            ("fts,index,kv,n1ql", "fts"),
            ("fts,index,kv,n1ql", "index,kv"),
            ("fts,index,kv,n1ql", "index,n1ql"),
            ("fts,index,kv,n1ql", "index,fts"),
            ("fts,index,kv,n1ql", "fts,index,kv,n1ql"),
            ("fts,index,kv,n1ql", "fts,index,kv"),
            ("fts,index,kv,n1ql", "fts,index,n1ql"),
            ("fts,index,kv,n1ql", "fts,n1ql"),
            ("fts,index,kv,n1ql", "fts,kv,n1ql"),
            ("fts,index,kv,n1ql", "kv,n1ql"),
            ("fts,index,kv,n1ql", "kv,fts"),
            ("fts,index,kv,n1ql", "index,kv,n1ql"),
            ("fts,index,kv,n1ql", "eventing"),
            ("fts,index,kv,n1ql", "kv,eventing"),
            ("fts,index,kv,n1ql", "index,kv,n1ql,eventing"),
            ("fts,index,kv,n1ql", "fts,index,kv,n1ql,eventing"),
            # --- Invalid start services (CE rejects at init) ---
            ("index", "kv"),
            ("index", "fts"),
            ("n1ql", "index"),
            ("n1ql", "fts"),
            ("fts", "kv"),
            ("fts", "index"),
            ("fts", "n1ql"),
            ("index,kv", "n1ql"),
            ("index,kv", "fts,n1ql"),
            ("fts,index", "n1ql"),
            ("fts,index", "fts,n1ql"),
            ("index,kv", "kv"),
            ("index,kv", "fts,kv"),
            ("index,n1ql", "index,kv"),
            ("index,n1ql", "fts,index,kv"),
            ("kv,n1ql", "index,n1ql"),
            ("kv,n1ql", "fts,index,n1ql"),
        ]

        for start, add in pairs:
            expected_ok = (start in self._CE_VALID_SERVICES
                           and add in self._CE_VALID_SERVICES)
            self.log.info("add-node test: start=%s add=%s expected=%s"
                          % (start, add, "ok" if expected_ok else "fail"))

            # Reset master and init with start_services
            init_ok, init_raw = self._reset_and_init_master(start)
            init_msg = (init_raw.decode() if isinstance(init_raw, bytes)
                        else str(init_raw))
            if not init_ok:
                self.assertFalse(
                    expected_ok,
                    "Init with start=%s failed unexpectedly. Got: %s"
                    % (start, init_msg))
                self.log.info("CE blocked init start=%s: %s" % (start, init_msg[:120]))
                continue

            # Try to add spare with add_services
            add_ok, add_msg = self._add_node_rest(spare, add)
            if not add_ok:
                self.assertFalse(
                    expected_ok,
                    "add_node start=%s add=%s failed unexpectedly. Got: %s"
                    % (start, add, add_msg))
                self.log.info("CE blocked add_node add=%s: %s" % (add, add_msg[:120]))
                continue

            # Spare is in pending -- trigger rebalance
            reb_ok, reb_msg = self._reb_and_wait()
            if expected_ok:
                self.assertTrue(
                    reb_ok,
                    "CE must allow rebalance start=%s add=%s. Got: %s"
                    % (start, add, reb_msg))
                self.log.info("CE allowed start=%s add=%s" % (start, add))
                # Rebalance out spare before next iteration
                self._eject_rebalance_out(spare.ip)
            else:
                self.assertFalse(
                    reb_ok,
                    "CE must block rebalance start=%s add=%s. Got: %s"
                    % (start, add, reb_msg))
                self.log.info("CE blocked rebalance start=%s add=%s: %s"
                              % (start, add, reb_msg[:120]))
                # Eject spare from pending state
                pend_nodes = self._get_otp_nodes(inactive_added=True)
                spare_otp = next(
                    (n.id for n in pend_nodes if n.ip == spare.ip), None)
                if spare_otp:
                    self.rest.eject_node(
                        user=self.cluster.master.rest_username,
                        password=self.cluster.master.rest_password,
                        otpNode=spare_otp)
                    self.sleep(3, "wait for spare to stabilize after eject")

        # Leave master in a valid state for tearDown
        self._reset_and_init_master("kv")

    # ------------------------------------------------------------------
    # N1QL EE-only feature rejection via query service (CBQE-8979)
    # testrunner: check_infer, check_flex_index, check_index_partitioning,
    #             check_query_window_functions, check_query_cost_based_optimizer,
    #             check_query_monitoring
    # ------------------------------------------------------------------

    def test_n1ql_ee_features_blocked(self):
        """CE must reject EE-only N1QL features via the query service.

        Reinitializes master with fts,index,kv,n1ql services so the
        query service is running, then exercises each EE-only feature:
        - INFER
        - Index partitioning (PARTITION BY HASH)
        - Window functions (CUME_DIST)
        - Cost-based optimizer (UPDATE STATISTICS)
        - Query profiling (admin/settings profile=phases)

        These must return status=fatal from the query service. The
        profiling request must fail with an EE-only error message.

        Flex index (USE INDEX ... USING FTS) is checked separately:
        USE INDEX is a hint, not a required semantic like the features
        above, so an unhonored hint is expected to fall back to another
        access path rather than fail the query. CE is validated instead
        via EXPLAIN -- the FTS hint must appear in hints_not_followed
        and the resulting plan must not use an FTS-backed scan.

        Replaces: check_infer, check_flex_index, check_index_partitioning,
                  check_query_window_functions,
                  check_query_cost_based_optimizer, check_query_monitoring.
        Requires nodes_init=1.
        """
        # Reinit with all CE-valid services so the query service runs
        ok, raw = self._reset_and_init_master("fts,index,kv,n1ql")
        msg = raw.decode() if isinstance(raw, bytes) else str(raw)
        self.assertTrue(ok,
                        "Failed to init master with fts,index,kv,n1ql: %s"
                        % msg)
        self.sleep(20, "wait for query/index/fts services to start")

        # Create default bucket for queries
        self.rest._http_request(
            self.rest.baseUrl + "pools/default/buckets", "POST",
            urllib.urlencode({"name": "default", "ramQuotaMB": 256,
                              "bucketType": "couchbase", "replicaNumber": 0}))
        self.sleep(8, "wait for default bucket to be ready")

        q_svc_url = self.rest.queryUrl + "query/service"

        # Create a real FTS index plus a matching document so the
        # flex-index query below has an actual FTS-backed plan and real
        # data to return. This makes the failure signal unambiguous: if
        # CE fails to block this, the query returns the real matching
        # row rather than an empty result that could be dismissed as
        # "the hint was never seriously attempted".
        fts_index_name = "ce_flex_test_idx"
        fts_index_body = json.dumps({
            "type": "fulltext-index",
            "name": fts_index_name,
            "sourceType": "couchbase",
            "sourceName": "default",
            "params": {
                "mapping": {
                    "default_mapping": {"enabled": True, "dynamic": True},
                    "default_analyzer": "standard"
                }
            }
        })
        fts_headers = self.rest.get_headers_for_content_type_json()
        self.rest._http_request(
            self.rest.ftsUrl + "api/index/" + fts_index_name, "PUT",
            fts_index_body, headers=fts_headers)
        self.sleep(10, "wait for FTS index to build")
        self.rest._http_request(
            q_svc_url, "POST",
            urllib.urlencode({
                "statement":
                    'INSERT INTO `default` (KEY, VALUE) '
                    'VALUES ("ce_flex_test_doc", {"f2": 100});'}))

        # Flex index (USE INDEX ... USING FTS) is a hint: if CE can't
        # honor it, the query is expected to fall back to another scan
        # rather than fail outright, so it doesn't belong in the
        # status=fatal loop below. Verify via EXPLAIN instead that CE
        # rejects the FTS-backed hint and does not actually use it.
        flex_index_stmt = (
            "EXPLAIN SELECT META(d).id FROM `default` AS d "
            "USE INDEX (USING FTS) WHERE d.f2 = 100;")
        body = urllib.urlencode({"statement": flex_index_stmt})
        _, content, _ = self.rest._http_request(q_svc_url, "POST", body)
        content_str = (content.decode() if isinstance(content, bytes)
                       else str(content))
        try:
            plan_result = (self._as_json(content).get("results") or [{}])[0]
        except (TypeError, ValueError, AttributeError, IndexError):
            plan_result = {}
        hints_not_followed = str(
            plan_result.get("optimizer_hints", {})
            .get("hints_not_followed", ""))
        plan_str = json.dumps(plan_result.get("plan", {}))
        self.assertIn(
            "INDEX_FTS", hints_not_followed,
            "CE must reject the FTS-backed flex-index hint (expected "
            "INDEX_FTS in hints_not_followed). Got: %s" % content_str[:300])
        self.assertNotIn(
            "fts", plan_str.lower(),
            "CE must not actually use an FTS-backed access path for the "
            "flex-index hint. Plan: %s" % plan_str[:300])
        self.log.info("CE correctly rejected FTS flex-index hint and "
                      "fell back to a non-FTS scan: %s" % hints_not_followed)

        # Each statement must return status=fatal on CE
        ee_stmts = [
            ("INFER",
             "infer `default` ;"),
            ("index partitioning PARTITION BY HASH",
             "CREATE INDEX idx_part ON `default`(id) "
             "PARTITION BY HASH(META().id)"),
            ("window function CUME_DIST",
             "SELECT d.id, CUME_DIST() OVER "
             "(PARTITION BY d.type ORDER BY d.id NULLS LAST) "
             "AS rank FROM `default` AS d LIMIT 7;"),
            ("UPDATE STATISTICS cost-based optimizer",
             "UPDATE STATISTICS FOR `default` INDEX ALL;"),
        ]

        for feature, stmt in ee_stmts:
            body = urllib.urlencode({"statement": stmt})
            _, content, _ = self.rest._http_request(q_svc_url, "POST", body)
            content_str = (content.decode() if isinstance(content, bytes)
                           else str(content))
            try:
                q_status = self._as_json(content).get("status", "")
            except (TypeError, ValueError, AttributeError):
                q_status = ""
            self.assertEqual(
                q_status, "fatal",
                "CE must block N1QL %s (expected status=fatal). "
                "Got status=%s body=%s"
                % (feature, q_status, content_str[:300]))
            self.log.info("CE blocked N1QL %s: status=fatal" % feature)

        self.rest._http_request(
            self.rest.ftsUrl + "api/index/" + fts_index_name, "DELETE")

        # Query profiling (admin/settings) -- must fail with EE-only message
        admin_url = self.rest.queryUrl + "admin/settings"
        prof_body = json.dumps({"profile": "phases"})
        status, content, _ = self.rest._http_request(
            admin_url, "POST", prof_body,
            headers=self.rest.get_headers_for_content_type_json())
        content_str = (content.decode() if isinstance(content, bytes)
                       else str(content))
        self.assertFalse(
            status,
            "CE must block query profiling. Request succeeded with: %s"
            % content_str[:300])
        self.assertIn(
            "EE only", content_str,
            "Expected 'EE only' in profiling error. Got: %s"
            % content_str[:200])
        self.log.info("CE blocked query profiling (admin/settings): %s"
                      % content_str[:120])

        # Restore to kv-only for tearDown
        self._reset_and_init_master("kv")
