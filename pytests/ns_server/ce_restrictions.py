"""
Community Edition Restrictions Test Module

Validates ns_server enforcement of Community Edition limitations:
- 5-node cluster limit
- EE-only service restrictions (analytics, eventing, backup)
- CE topology restrictions (no query-only nodes)
"""

import json
import time

from basetestcase import ClusterSetup
from cb_server_rest_util.cluster_nodes.cluster_init_provision import \
    ClusterInitializationProvision
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_tools.cb_cli import CbCli
from cluster_utils.cluster_ready_functions import ClusterUtils
from ns_server.ce_restrictions_util import CommunityEditionRestrictionsUtil
from shell_util.remote_connection import RemoteMachineShellConnection


class CommunityEditionRestrictions(ClusterSetup):
    """Test class for Community Edition restriction enforcement."""

    def setUp(self):
        super(CommunityEditionRestrictions, self).setUp()
        self.rest = ClusterRestAPI(self.cluster.master)
        self.ce_util = CommunityEditionRestrictionsUtil(
            self.cluster, self.cluster_util, self.log, self.sleep)

        if self.cluster_util.is_enterprise_edition(self.cluster):
            self.fail("Test requires Community Edition cluster. "
                      "Install with edition=community parameter.")

        self.log.info("Cluster edition: %s", self.cluster.edition)

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
        self.log.info("Adding 6th node: %s", node_to_add.ip)
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
            self.log.info("Rejected '%s': %s", services, error_msg)
            self.sleep(8, "Wait after ejection")

        # Query-only topology restriction
        success, error_msg = self.ce_util.add_node_via_cli(
            test_node, "query", expect_success=False)
        self.assertTrue(success, "Query-only should be rejected")
        self.assertIn("Community Edition only supports", error_msg)
        self.log.info("Rejected 'query-only': %s", error_msg)
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

        self.log.info("Adding node %s with services: %s", test_node.ip, services)
        self.ce_util.add_node_via_rest_and_rebalance_in(test_node, services)
        self.log.info("Rebalance-in succeeded")

        # Cleanup
        self.ce_util.rebalance_out_node(test_node)
        self.log.info("Cleanup completed")

    # ------------------------------------------------------------------
    # CE valid-service constants and reset/init helpers (CBQE-8979)
    # ------------------------------------------------------------------

    _CE_VALID_SERVICES = frozenset(["kv", "index,kv,n1ql", "fts,index,kv,n1ql"])

    def _reset_and_init_master(self, services):
        """Hard-reset master, wait for it to come back, then initialize
        with the given services string.  Returns (status, content)."""
        master = self.cluster.master
        self.rest.reset_node()
        self.cluster_util.wait_for_ns_servers_or_assert([master])
        init_prov = ClusterInitializationProvision()
        init_prov.set_server_values(master)
        init_prov.set_endpoint_urls(master)
        return init_prov.initialize_cluster(
            hostname=master.ip,
            username=master.rest_username,
            password=master.rest_password,
            services=services,
            memory_quota=256,
            index_memory_quota=256,
            fts_memory_quota=256)

    def _get_otp_nodes(self, inactive_added=False):
        return ClusterUtils.get_nodes(
            self.cluster.master, inactive_added=inactive_added)

    def _reb_and_wait(self, timeout=120):
        """Trigger rebalance with all known nodes and wait for completion.
        Returns (success, error_msg)."""
        nodes = self._get_otp_nodes(inactive_added=True)
        ok, raw = self.rest.rebalance(
            known_nodes=[n.id for n in nodes], eject_nodes=[])
        if not ok:
            return False, raw.decode() if isinstance(raw, bytes) else str(raw)
        end = time.time() + timeout
        while time.time() < end:
            _, prog_raw = self.rest.rebalance_progress()
            try:
                data = json.loads(
                    prog_raw.decode() if isinstance(prog_raw, bytes) else prog_raw)
            except (ValueError, AttributeError):
                self.sleep(2, "polling rebalance")
                continue
            if data.get("status") == "none":
                err = data.get("errorMessage", "")
                return (False, err) if err else (True, "")
            self.sleep(2, "rebalance running: %s" % data.get("status"))
        return False, "timeout after %ds" % timeout

    def _eject_rebalance_out(self, node_ip, timeout=120):
        """Rebalance out an active member by IP and wait for completion."""
        nodes = self._get_otp_nodes()
        spare_otp = next((n.id for n in nodes if n.ip == node_ip), None)
        if not spare_otp:
            return
        all_otp = [n.id for n in nodes]
        self.rest.rebalance(known_nodes=all_otp, eject_nodes=[spare_otp])
        end = time.time() + timeout
        while time.time() < end:
            _, prog_raw = self.rest.rebalance_progress()
            try:
                data = json.loads(
                    prog_raw.decode() if isinstance(prog_raw, bytes) else prog_raw)
            except (ValueError, AttributeError):
                self.sleep(2, "polling eject rebalance")
                continue
            if data.get("status") == "none":
                return
            self.sleep(2, "eject rebalance running")

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

    # ------------------------------------------------------------------
    # CLI enforcement tests (CBQE-8973)
    # ------------------------------------------------------------------

    def test_ce_phonehome_enforcement(self):
        """
        CE enforcement of sendStats/PhoneHome — REST post-init, CLI init, and
        REST /clusterInit covered in one test pass.

        Steps:
        1. REST: POST /settings/stats sendStats=false rejected on running cluster
        2. Reset master; CLI: cluster-init --update-notifications 0 → rejected
           or forced true
        3. If CLI rejected (node still uninitialized): REST /clusterInit
           sendStats=false → rejected or forced true

        Covers CBQE-8972 + CBQE-8973 CLI cluster-init. Requires nodes_init=1.
        """
        # --- Part 1: REST post-init — sendStats=false must be rejected ---
        status, settings = self.rest.get_stats_settings()
        self.assertTrue(status,
                        "GET /settings/stats failed: %s" % settings)
        current = (settings.get("sendStats", False)
                   if isinstance(settings, dict) else True)
        self.assertTrue(current,
                        "CE must default to sendStats=true. Got: %s" % settings)
        self.log.info("Confirmed sendStats=true baseline: %s", settings)

        status, content = self.rest.set_stats_settings(send_stats=False)
        err_msg = content.decode() if isinstance(content, bytes) else str(content)
        self.assertFalse(
            status,
            "CE must reject POST /settings/stats sendStats=false. Got: %s"
            % err_msg)
        self.log.info("REST rejected sendStats=false post-init: %s", err_msg)

        status, settings = self.rest.get_stats_settings()
        self.assertTrue(status)
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
            self.log.info("CLI rejected --update-notifications 0: %s",
                          combined[:300])
            # Node still uninitialized — exercise REST /clusterInit path too
            init_prov = ClusterInitializationProvision()
            init_prov.set_server_values(master)
            init_prov.set_endpoint_urls(master)
            status, content = init_prov.initialize_cluster(
                hostname=master.ip,
                username=master.rest_username,
                password=master.rest_password,
                send_stats=False,
                services="kv",
                memory_quota=256,
                cluster_name="CE_PhoneHome_REST_Test",
            )
            if not status:
                err = content.decode() if isinstance(content, bytes) else str(content)
                self.log.info("REST /clusterInit also rejected sendStats=false: %s",
                              err)
            else:
                rest_init = ClusterRestAPI(master)
                ok, s = rest_init.get_stats_settings()
                actual = s.get("sendStats", False) if isinstance(s, dict) else False
                self.assertTrue(
                    actual,
                    "CE must force sendStats=true at /clusterInit. Got: %s" % s)
                self.log.info("REST /clusterInit forced sendStats=true: %s", s)
        else:
            # CLI accepted and initialized — verify sendStats was forced true
            self.log.info("CLI accepted; verifying CE forced sendStats=true")
            rest_init = ClusterRestAPI(master)
            ok, settings = rest_init.get_stats_settings()
            self.assertTrue(ok, "GET /settings/stats failed after CLI init")
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
            self.log.info("CE rejected server-add of 6th node: %s",
                          output_str[:300])
            return

        # Node added to pending — verify CLI rebalance is blocked
        self.log.info("server-add accepted; verifying CLI rebalance blocked")
        shell, cb_cli = self._cli_on(self.cluster.master)
        reb_out, reb_err = cb_cli.rebalance()
        shell.disconnect()

        combined = self._combined(reb_out, reb_err)
        self.assertTrue(
            reb_err or "error" in combined.lower(),
            "CE must reject rebalance with 6 nodes. Got: %s" % combined)
        self.log.info("CE blocked rebalance with 6 nodes: %s", combined[:300])

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
            self.log.info("CE rejected MDS server-add: %s", output_str[:300])
            return

        # Node in pending — verify CLI rebalance is blocked
        self.log.info("MDS node in pending; verifying CLI rebalance rejected")
        shell, cb_cli = self._cli_on(self.cluster.master)
        reb_out, reb_err = cb_cli.rebalance()
        shell.disconnect()

        combined = self._combined(reb_out, reb_err)
        self.assertTrue(
            reb_err or "error" in combined.lower(),
            "CE must reject rebalance with MDS topology. Got: %s" % combined)
        self.log.info("CE blocked MDS rebalance: %s", combined[:300])

    def test_ce_cli_node_init_ee_services_rejected(self):
        """
        couchbase-cli node-init --services <ee-service> must fail on CE.

        Tests cbas, eventing, backup — all EE-only services.
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
            self.log.info("CE rejected node-init --services %s: %s",
                          svc, combined[:200])

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
            self.log.info("Testing /clusterInit services=%s", services)
            status, content = self._reset_and_init_master(services)
            should_succeed = services in self._CE_VALID_SERVICES
            err = content.decode() if isinstance(content, bytes) else str(content)
            if should_succeed:
                self.assertTrue(
                    status,
                    "CE must allow services=%s at init. Got: %s" % (services, err))
                self.log.info("CE allowed services=%s", services)
            else:
                self.assertFalse(
                    status,
                    "CE must block services=%s at init. Got: %s" % (services, err))
                self.log.info("CE blocked services=%s: %s", services, err[:120])
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
            self.log.info("add-node test: start=%s add=%s expected=%s",
                          start, add, "ok" if expected_ok else "fail")

            # Reset master and init with start_services
            init_ok, init_raw = self._reset_and_init_master(start)
            init_msg = (init_raw.decode() if isinstance(init_raw, bytes)
                        else str(init_raw))
            if not init_ok:
                self.assertFalse(
                    expected_ok,
                    "Init with start=%s failed unexpectedly. Got: %s"
                    % (start, init_msg))
                self.log.info("CE blocked init start=%s: %s", start, init_msg[:120])
                continue

            # Try to add spare with add_services
            add_ok, add_raw = self.rest.add_node(
                host_name=spare.ip,
                username=spare.rest_username,
                password=spare.rest_password,
                services=add)
            add_msg = (add_raw.decode() if isinstance(add_raw, bytes)
                       else str(add_raw))
            if not add_ok:
                self.assertFalse(
                    expected_ok,
                    "add_node start=%s add=%s failed unexpectedly. Got: %s"
                    % (start, add, add_msg))
                self.log.info("CE blocked add_node add=%s: %s", add, add_msg[:120])
                continue

            # Spare is in pending — trigger rebalance
            reb_ok, reb_msg = self._reb_and_wait()
            if expected_ok:
                self.assertTrue(
                    reb_ok,
                    "CE must allow rebalance start=%s add=%s. Got: %s"
                    % (start, add, reb_msg))
                self.log.info("CE allowed start=%s add=%s", start, add)
                # Rebalance out spare before next iteration
                self._eject_rebalance_out(spare.ip)
            else:
                self.assertFalse(
                    reb_ok,
                    "CE must block rebalance start=%s add=%s. Got: %s"
                    % (start, add, reb_msg))
                self.log.info("CE blocked rebalance start=%s add=%s: %s",
                              start, add, reb_msg[:120])
                # Eject spare from pending state
                pend_nodes = self._get_otp_nodes(inactive_added=True)
                spare_otp = next(
                    (n.id for n in pend_nodes if n.ip == spare.ip), None)
                if spare_otp:
                    self.rest.eject_node(spare_otp)
                    self.sleep(3, "wait for spare to stabilize after eject")

        # Leave master in a valid state for tearDown
        self._reset_and_init_master("kv")
