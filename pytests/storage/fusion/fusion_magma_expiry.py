import os
import random
import subprocess
import threading
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_expiry_compaction import MagmaExpiryTests


class FusionMagmaExpiry(MagmaExpiryTests, FusionBase):
    def setUp(self):
        super(FusionMagmaExpiry, self).setUp()

        self.log.info("FusionMagmaExpiry setup started")

    def tearDown(self):
        super(FusionMagmaExpiry, self).tearDown()

    def _set_expected_after_full_expiry(self):
        """The base expiry tests expire every doc they load (expiry_perc
        defaults to 100) and check for 0 items themselves, but leave
        self.num_items at the loaded count (or wherever loadgen_docs'
        per-collection adjustments landed), so the post-rebalance and
        tearDown count checks waited for docs that no longer exist."""
        if int(self.input.param("expiry_perc", 100)) != 100:
            self.fail("Fusion expiry wrappers expect expiry_perc=100; "
                      "work out the surviving doc count before using "
                      "another value")
        self.num_items = 0
        self.deleted_upto = 0

    def test_fusion_simple_expiry(self):

        self.log.info("Test fusion simple expiry started")

        self.test_expiry()
        self._set_expected_after_full_expiry()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_expiry_with_updates(self):

        self.log.info("Test fusion expiry with updates started")

        self.test_expiry_no_wait_update()
        self._set_expected_after_full_expiry()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_expiry_disk_full(self):

        self.log.info("Test fusion expiry during disk full scenario started")

        self.test_expiry_disk_full()
        self._set_expected_after_full_expiry()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_expiry_with_read_workload(self):

        self.log.info("Test fusion expiry with read workload started")

        # MagmaExpiryTests.test_expiry_heavy_reads is an empty stub, so this
        # test used to load nothing and then wait twice for num_items docs
        # that were never written. Fail straight away until the magma test
        # exists, with the expected count at 0 so tearDown doesn't wait too.
        self.num_items = 0
        self.fail("MagmaExpiryTests.test_expiry_heavy_reads is not "
                  "implemented (the body is 'pass'), so there is no expiry "
                  "+ read workload to run before the Fusion rebalance")

    def test_fusion_drop_collection_expired_items(self):

        self.log.info("Test fusion drop collection which contains expired items started")

        self.test_drop_collection_expired_items()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_drop_collection_during_tombstone_creation(self):

        self.log.info("Test fusion drop collection during tombstone creation started")

        self.test_drop_collection_during_tombstone_creation()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_failover_expired_items(self):

        self.log.info("Test fusion failover a node which has expired items started")

        self.test_failover_expired_items_in_vB()
        self._set_expected_after_full_expiry()

        # The base test fails over and rebalances out its last node, but
        # cluster.nodes_in_cluster still listed it, so the Fusion rebalance
        # script was given a node no longer in the cluster and
        # /controller/rebalance returned 400. Rebuild the list from the
        # cluster's own view of its active nodes.
        active_ips = {node.ip for node in
                      self.cluster_util.get_nodes(self.cluster.master)}
        self.cluster.nodes_in_cluster = [server for server in
                                         self.cluster.servers
                                         if server.ip in active_ips]
        self.log.info(f"Nodes in cluster after the failover: "
                      f"{[n.ip for n in self.cluster.nodes_in_cluster]}")

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()
