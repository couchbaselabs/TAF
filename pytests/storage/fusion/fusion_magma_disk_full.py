import os
import random
import subprocess
import threading
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_disk_full import MagmaDiskFull


class FusionMagmaDiskFull(MagmaDiskFull, FusionBase):
    def setUp(self):
        super(FusionMagmaDiskFull, self).setUp()

        self.log.info("FusionMagmaDiskFull setup started")

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
        super(FusionMagmaDiskFull, self).tearDown()

    def test_fusion_simple_disk_full(self):

        self.log.info("Test fusion simple disk full started")

        self.test_simple_disk_full()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_crash_recovery_disk_full(self):

        self.log.info("Test fusion crash recovery on disk full started")

        self.test_crash_recovery_disk_full()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_rollback_after_disk_full(self):

        self.log.info("Test fusion rollback after disk full started")

        self.test_rollback_after_disk_full()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_disk_full_reduce_replica(self):

        self.log.info("Test fusion disk full reduce replica started")

        self.test_disk_full_reduce_replica()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_disk_full_on_increasing_replica(self):

        self.log.info("Test fusion disk full on increasing replica started")

        self.test_disk_full_on_increasing_replica()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_deletes_disk_full(self):

        self.log.info("Test fusion deletes disk full started")

        self.test_deletes_disk_full()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_delete_bucket_disk_full(self):

        self.log.info("Test fusion delete bucket disk full started")

        self.test_delete_bucket_disk_full()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_bucket_flush_disk_full(self):

        self.log.info("Test fusion bucket flush disk full started")

        self.test_bucket_flush_disk_full()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_magma_dump_disk_full(self):

        self.log.info("Test fusion magma dump disk full started")

        self.test_magma_dump_disk_full()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_disk_full_add_nodes(self):

        self.log.info("Test fusion disk full add nodes started")

        self.test_disk_full_add_nodes()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_disk_full_with_large_docs(self):

        self.log.info("Test fusion disk full with large docs started")

        self.test_disk_full_with_large_docs()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()