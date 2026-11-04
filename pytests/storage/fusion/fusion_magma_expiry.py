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

    def test_fusion_simple_expiry(self):

        self.log.info("Test fusion simple expiry started")

        self.test_expiry()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_expiry_with_updates(self):

        self.log.info("Test fusion expiry with updates started")

        self.test_expiry_no_wait_update()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_expiry_disk_full(self):

        self.log.info("Test fusion expiry during disk full scenario started")

        self.test_expiry_disk_full()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_expiry_with_read_workload(self):

        self.log.info("Test fusion expiry with read workload started")

        self.test_expiry_heavy_reads()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()

    def _set_expected_after_collection_drop(self):
        """The drop-collection base tests load expiry docs into every other
        collection, then drop (and pop) exactly those collections. Their
        loadgen_docs calls adjust self.num_items once per collection, leaving
        it far off (negative). What remains is only the initial load, if any,
        in the surviving collections."""
        if self.init_loading:
            self.num_items = (self.init_items_per_collection
                              * self.get_loaded_collection_count())
        else:
            self.num_items = 0
        self.deleted_upto = 0

    def test_fusion_drop_collection_expired_items(self):

        self.log.info("Test fusion drop collection which contains expired items started")

        self.test_drop_collection_expired_items()
        self._set_expected_after_collection_drop()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_drop_collection_during_tombstone_creation(self):

        self.log.info("Test fusion drop collection during tombstone creation started")

        self.test_drop_collection_during_tombstone_creation()
        self._set_expected_after_collection_drop()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_failover_expired_items(self):

        self.log.info("Test fusion failover a node which has expired items started")

        self.test_failover_expired_items_in_vB()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()
