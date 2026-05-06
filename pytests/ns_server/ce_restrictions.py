"""
Community Edition Restrictions Test Module

Validates ns_server enforcement of Community Edition limitations:
- 5-node cluster limit
- EE-only service restrictions (analytics, eventing, backup)
- CE topology restrictions (no query-only nodes)
"""

from basetestcase import ClusterSetup
from ns_server.ce_restrictions_util import CommunityEditionRestrictionsUtil


class CommunityEditionRestrictions(ClusterSetup):
    """Test class for Community Edition restriction enforcement."""

    def setUp(self):
        super(CommunityEditionRestrictions, self).setUp()
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

    def _get_available_node(self):
        """Get a node not currently in the cluster."""
        available = [s for s in self.cluster.servers
                     if s not in self.cluster.nodes_in_cluster]
        if not available:
            self.fail("No available nodes. Requires at least 2 nodes in node.ini")
        return available[0]
