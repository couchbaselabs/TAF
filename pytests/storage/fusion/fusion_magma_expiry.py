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

        split_path = self.local_test_path.split("/")
        self.fusion_output_dir = "/" + os.path.join("/".join(split_path[1:4]), "fusion_output")
        self.log.info(f"Fusion output dir = {self.fusion_output_dir}")
        subprocess.run(f"mkdir -p {self.fusion_output_dir}", shell=True, executable="/bin/bash")

        # Override Fusion default settings
        for bucket in self.cluster.buckets:
            self.change_fusion_settings(bucket, upload_interval=self.fusion_upload_interval,
                                        checkpoint_interval=self.fusion_log_checkpoint_interval,
                                        logstore_frag_threshold=self.logstore_frag_threshold)
        # Set Migration Rate Limit
        ClusterRestAPI(self.cluster.master). \
            manage_global_memcached_setting(fusion_migration_rate_limit=self.fusion_migration_rate_limit)

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

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()
