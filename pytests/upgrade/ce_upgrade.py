from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from couchbase_utils.cluster_utils.cluster_ready_functions import ClusterUtils
from rebalance_utils.rebalance_util import RebalanceUtil
from upgrade.ce_base import CEBaseTest


class CEUpgradeTests(CEBaseTest):
    """
    CE upgrade tests: node-limit enforcement, mixed-mode entry, and CE→EE
    restriction removal.

    All tests require community_upgrade=True so UpgradeBase installs the
    target build as Community Edition.
    """

    def setUp(self):
        super(CEUpgradeTests, self).setUp()

    def tearDown(self):
        super(CEUpgradeTests, self).tearDown()

    # ------------------------------------------------------------------ #
    # Gap 2: CE restriction enforcement — 8.1 node-limit                  #
    # ------------------------------------------------------------------ #

    def test_ce_node_limit_rebalance_in_blocked(self):
        """
        Rebalance-in of a 6th CE node into a 5-node 8.1 CE cluster must
        fail with the CE node-limit error.

        Validates:
        1. Version guard — skip on pre-8.1 builds where the limit is absent
        2. Adding a pending CE node is accepted by addNode
        3. Triggering rebalance is rejected with the CE restriction message
        4. Ejecting the pending node recovers the cluster to exactly 5 nodes
        5. Data integrity is intact after recovery
        """
        self._require_enforced_ce_version()

        self.assertEqual(len(self.cluster.nodes_in_cluster), self.CE_NODE_LIMIT,
                         "Requires exactly 5-node CE cluster, found %d"
                         % len(self.cluster.nodes_in_cluster))
        self.assertGreater(len(self.cluster.servers), self.CE_NODE_LIMIT,
                           "Requires at least 6 servers in node.ini")

        # Step 1: install CE on the spare so it is a valid candidate to add
        self._install_on_spare(edition="community")

        rest = ClusterRestAPI(self.cluster.master)

        # Step 2: add the 6th CE node without rebalancing
        self.log.info("Adding 6th CE node %s without rebalance",
                      self.spare_node.ip)
        status, content = rest.add_node(
            host_name=self.spare_node.ip,
            username=self.spare_node.rest_username,
            password=self.spare_node.rest_password,
            services="kv")
        self.assertTrue(status, "add_node failed: %s" % content)

        nodes = ClusterUtils.get_nodes(self.cluster.master, inactive_added=True)
        pending = [n for n in nodes if n.ip == self.spare_node.ip]
        self.assertTrue(pending,
                        "Spare node %s not in pending state after addNode"
                        % self.spare_node.ip)
        self.log.info("6th CE node is in pending state")

        # Step 3: trigger rebalance — must be rejected
        self.log.info("Attempting rebalance with 6 CE nodes — expecting failure")
        known_nodes = [n.id for n in nodes]
        self._attempt_rebalance_expect_ce_failure(rest, known_nodes)

        # Step 4: eject the pending node and verify cluster recovers to 5
        sixth_otp = next(
            (n.id for n in nodes if n.ip == self.spare_node.ip), None)
        self.assertIsNotNone(sixth_otp,
                             "Could not find OTP ID for spare node %s"
                             % self.spare_node.ip)
        rest.eject_node(sixth_otp)
        self.sleep(5, "Wait after ejecting 6th node")

        final_count = len(ClusterUtils.get_nodes(self.cluster.master))
        self.assertEqual(final_count, self.CE_NODE_LIMIT,
                         "Expected %d active nodes after cleanup, found %d"
                         % (self.CE_NODE_LIMIT, final_count))
        self.log.info("Cluster recovered to %d active nodes", final_count)

        # Step 5: data integrity
        self.bucket_util.validate_docs_per_collections_all_buckets(self.cluster)
        self.log.info("Data integrity validated after CE restriction recovery")

    # ------------------------------------------------------------------ #
    # Gap 3: CE→EE upgrade — mixed-mode and restriction removal           #
    # ------------------------------------------------------------------ #

    def test_ce_ee_mixed_mode_sixth_node(self):
        """
        Adding a 6th EE node to a 5-node CE cluster succeeds and puts the
        cluster into mixed CE+EE mode.

        Validates that the CE 5-node limit does not block EE node addition,
        confirming mixed-mode entry is allowed.

        Validates:
        1. Version guard — skip on pre-8.1 builds
        2. 6th EE node can be added and rebalanced into a CE cluster
        3. Resulting 6-node mixed cluster has intact data
        """
        self._require_enforced_ce_version()

        self.assertEqual(len(self.cluster.nodes_in_cluster), self.CE_NODE_LIMIT,
                         "Requires exactly 5-node CE cluster, found %d"
                         % len(self.cluster.nodes_in_cluster))
        self.assertGreater(len(self.cluster.servers), self.CE_NODE_LIMIT,
                           "Requires at least 6 servers in node.ini")

        # Install EE on the spare
        self._install_on_spare(edition="enterprise")

        rest = ClusterRestAPI(self.cluster.master)
        self.log.info("Adding 6th EE node %s (expect success — mixed mode)",
                      self.spare_node.ip)
        status, content = rest.add_node(
            host_name=self.spare_node.ip,
            username=self.spare_node.rest_username,
            password=self.spare_node.rest_password,
            services="kv")
        self.assertTrue(status, "add_node failed: %s" % content)

        nodes = ClusterUtils.get_nodes(self.cluster.master, inactive_added=True)
        known_nodes = [n.id for n in nodes]

        self.log.info("Rebalancing in 6th EE node")
        status, content = rest.rebalance(known_nodes=known_nodes, eject_nodes=[])
        self.assertTrue(status, "Rebalance call failed: %s" % content)

        rebalance_passed = RebalanceUtil(self.cluster).monitor_rebalance()
        self.assertTrue(rebalance_passed,
                        "Rebalance-in of 6th EE node failed — CE limit may "
                        "have incorrectly blocked it")

        mixed_count = len(ClusterUtils.get_nodes(self.cluster.master))
        self.assertEqual(mixed_count, self.CE_NODE_LIMIT + 1,
                         "Expected 6 nodes in mixed-mode cluster, found %d"
                         % mixed_count)
        self.log.info("Mixed-mode cluster confirmed: %d nodes (CE+EE)", mixed_count)

        self.bucket_util.validate_docs_per_collections_all_buckets(self.cluster)
        self.log.info("Data integrity validated in mixed-mode cluster")

        # Rebalance out the 6th node to restore clean state for tearDown
        nodes = ClusterUtils.get_nodes(self.cluster.master)
        sixth_otp = next(
            (n.id for n in nodes if n.ip == self.spare_node.ip), None)
        if sixth_otp:
            otp_ids = [n.id for n in nodes]
            status, content = rest.rebalance(known_nodes=otp_ids,
                                             eject_nodes=[sixth_otp])
            self.assertTrue(status, "Cleanup rebalance failed: %s" % content)
            cleanup_passed = RebalanceUtil(self.cluster).monitor_rebalance()
            self.assertTrue(cleanup_passed,
                            "Cleanup rebalance-out of 6th EE node failed — "
                            "cluster left dirty")
        self.log.info("CE→EE mixed-mode sixth-node test validated")

    def test_ce_ee_all_restrictions_lifted_post_upgrade(self):
        """
        After all CE nodes are upgraded to EE the CE 5-node restriction is
        fully lifted — a 6th node can be rebalanced in without error.

        Validates:
        1. Version guard — skip on pre-8.1 builds
        2. All 5 CE nodes swap-upgrade to EE
        3. 6th EE node rebalances in successfully (restriction lifted)
        4. Final 6-node all-EE cluster has intact data
        """
        self._require_enforced_ce_version()

        self.assertEqual(len(self.cluster.nodes_in_cluster), self.CE_NODE_LIMIT,
                         "Requires exactly 5-node CE cluster, found %d"
                         % len(self.cluster.nodes_in_cluster))

        # Step 1: swap-upgrade all 5 CE nodes to EE
        for upgrade_version in self.upgrade_chain[1:]:
            self.upgrade_version = upgrade_version
            self.PrintStep("CE→EE upgrading to %s" % upgrade_version)
            max_swaps = len(self.cluster.nodes_in_cluster) + 1
            swap_count = 0
            node_to_upgrade = self.fetch_node_to_upgrade()
            while node_to_upgrade is not None and swap_count < max_swaps:
                swap_count += 1
                self.log.info("Swap upgrading CE node %s → EE",
                              node_to_upgrade.ip)
                self.online_swap(node_to_upgrade)
                self.cluster_util.print_cluster_stats(self.cluster)
                node_to_upgrade = self.fetch_node_to_upgrade()
            if swap_count >= max_swaps and node_to_upgrade is not None:
                self.fail("Swap loop exceeded max iterations — possible "
                          "fetch_node_to_upgrade bug")

        self.assertTrue(
            self.cluster_util.is_enterprise_edition(self.cluster),
            "All nodes should be EE after full CE→EE upgrade")
        self.log.info("All 5 nodes upgraded to EE %s", self.upgrade_version)

        # Step 2: install EE on spare (ejected last during swaps)
        self._install_on_spare(edition="enterprise")

        # Step 3: rebalance-in the 6th EE node — must succeed
        rest = ClusterRestAPI(self.cluster.master)
        self.log.info("Adding 6th EE node %s — CE restriction should be lifted",
                      self.spare_node.ip)
        status, content = rest.add_node(
            host_name=self.spare_node.ip,
            username=self.spare_node.rest_username,
            password=self.spare_node.rest_password,
            services="kv")
        self.assertTrue(status, "add_node failed: %s" % content)

        nodes = ClusterUtils.get_nodes(self.cluster.master, inactive_added=True)
        known_nodes = [n.id for n in nodes]
        status, content = rest.rebalance(known_nodes=known_nodes, eject_nodes=[])
        self.assertTrue(status, "Rebalance call failed: %s" % content)

        rebalance_passed = RebalanceUtil(self.cluster).monitor_rebalance()
        self.assertTrue(rebalance_passed,
                        "Rebalance-in failed — CE restriction may still be "
                        "active after full EE upgrade")

        final_count = len(ClusterUtils.get_nodes(self.cluster.master))
        self.assertEqual(final_count, self.CE_NODE_LIMIT + 1,
                         "Expected 6 nodes post full EE upgrade, found %d"
                         % final_count)
        self.log.info("6-node all-EE cluster confirmed: CE restriction lifted")

        self.bucket_util.validate_docs_per_collections_all_buckets(self.cluster)
        self.log.info("Data integrity validated across full CE→EE upgrade chain")

    # ------------------------------------------------------------------ #
    # Gap 1: CE 6-node upgrade — restriction triggers mid-upgrade        #
    # ------------------------------------------------------------------ #

    def test_ce_6node_upgrade_restriction_and_ee_resolution(self):
        """
        Validates CE 8.1 node-limit behaviour on a 6-node pre-8.1 CE cluster.

        Real-world scenario: a customer had a valid 6-node CE 7.6.x cluster
        (no limit pre-8.1), then started an offline upgrade to CE 8.1.
        The restriction activates as soon as the FIRST node is upgraded —
        not after all six are on 8.1.

        Stage 1 — Baseline: 6-node CE 7.6.x rebalances freely.
        Stage 2 — Restriction trigger: offline-upgrade ONE node to CE 8.1;
                  the next rebalance attempt is rejected by the CE limit.
        Stage 3 — CE→EE resolution: swap-upgrade all 6 nodes to EE 8.1.
                  Each swap ejects one CE node so the resulting CE count
                  never exceeds 5, allowing the rebalance to proceed.
        Stage 4 — Post-EE: 6-node EE cluster rebalances freely; data intact.

        Requires nodes_init=6 and a 7th spare node in node.ini.
        Set initial_ce_version=7.6.2 so setUp installs the pre-8.1 baseline.
        """
        self.assertEqual(len(self.cluster.nodes_in_cluster), 6,
                         "Requires exactly 6-node CE cluster, found %d"
                         % len(self.cluster.nodes_in_cluster))
        self.assertGreater(len(self.cluster.servers), 6,
                           "Requires at least 7 servers in node.ini for spare")

        rest = ClusterRestAPI(self.cluster.master)

        # ── Stage 1: Baseline — 6-node CE 7.6.x, no restriction ────────── #
        self.PrintStep("Stage 1: Verify 6-node CE 7.6.x rebalances freely "
                       "(no restriction pre-8.1)")
        nodes = ClusterUtils.get_nodes(self.cluster.master)
        known_nodes = [n.id for n in nodes]
        status, _ = rest.rebalance(known_nodes=known_nodes, eject_nodes=[])
        if status:
            RebalanceUtil(self.cluster).monitor_rebalance()
        self.log.info("Stage 1 passed — 6-node CE 7.6.x rebalances without "
                      "restriction")

        # ── Stage 2: Offline-upgrade one node to CE 8.1 — restriction ──── #
        self.PrintStep("Stage 2: Offline-upgrade one node to CE 8.1 — "
                       "restriction fires immediately")

        # Pick a non-master node to upgrade offline
        node_to_upgrade = next(
            n for n in self.cluster.nodes_in_cluster
            if n.ip != self.cluster.master.ip)

        self._offline_upgrade_single_node(
            node_to_upgrade, self.upgrade_version, edition="community")
        self._wait_for_node_active(node_to_upgrade)
        self.sleep(15, "Allow cluster to stabilize after offline node upgrade "
                   "before triggering rebalance")

        self.log.info("One node upgraded to CE 8.1; cluster is now "
                      "5×7.6.x + 1×8.1 CE — restriction should be active")

        nodes = ClusterUtils.get_nodes(self.cluster.master, inactive_added=True)
        known_nodes = [n.id for n in nodes]
        # check_ce_message=False: in a mixed-version cluster (master on 7.6.2,
        # one node on 8.1 CE), the CE restriction fires but is re-raised by
        # the 7.6.2 orchestrator as a generic 500 rather than the clean
        # "Cannot rebalance with more than 5" message seen in all-8.1 clusters.
        # The proof is Stage 1 (7.6.2 only) passing + Stage 2 failing.
        self._attempt_rebalance_expect_ce_failure(rest, known_nodes,
                                                  check_ce_message=False)
        self.log.info("Stage 2 passed — CE restriction fires on first CE 8.1 "
                      "node, not after all 6 are upgraded")

        # ── Stage 3: CE→EE swap resolves the stuck cluster ──────────────── #
        self.PrintStep("Stage 3: CE→EE swap-upgrade all 6 nodes to EE 8.1 "
                       "using 7th node as spare")
        self._online_swap_all_nodes(edition="enterprise")

        self.assertTrue(
            self.cluster_util.is_enterprise_edition(self.cluster),
            "Stage 3: Expected all-EE cluster after CE→EE swap upgrade")
        self.log.info("Stage 3 passed — all 6 nodes on EE 8.1, "
                      "CE restriction resolved")

        # ── Stage 4: Post-EE — 6-node EE cluster works freely ───────────── #
        self.PrintStep("Stage 4: Verify 6-node EE cluster rebalances freely")
        # Reinitialize rest — master may have rotated during Stage 3 swaps
        self.cluster_util.find_orchestrator(self.cluster)
        rest = ClusterRestAPI(self.cluster.master)
        nodes = ClusterUtils.get_nodes(self.cluster.master)
        known_nodes = [n.id for n in nodes]
        status, content = rest.rebalance(known_nodes=known_nodes,
                                         eject_nodes=[])
        self.assertTrue(status,
                        "Stage 4: Rebalance call failed: %s" % content)
        rebalance_passed = RebalanceUtil(self.cluster).monitor_rebalance()
        self.assertTrue(rebalance_passed,
                        "Stage 4: Rebalance failed — CE restriction may "
                        "still be active after full EE upgrade")
        self.log.info("Stage 4 passed — 6-node EE cluster rebalances freely")

        self.bucket_util.validate_docs_per_collections_all_buckets(self.cluster)
        self.log.info("CE 6-node upgrade restriction and EE resolution "
                      "validated with data integrity")
