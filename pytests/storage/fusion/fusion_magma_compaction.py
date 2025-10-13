import os
import subprocess
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_compaction import MagmaCompactionTests


class FusionMagmaCompactionTests(MagmaCompactionTests, FusionBase):
    def setUp(self):
        super(FusionMagmaCompactionTests, self).setUp()

        self.log.info("FusionMagmaCrashTests setUp Started")

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
        super(FusionMagmaCompactionTests, self).tearDown()

    def test_fusion_test_crash_during_compaction(self):

        self.log.info("Test fusion crash during compaction started")

        self.test_crash_during_compaction()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()


    def test_fusion_test_rollback_during_compaction(self):

        self.log.info("Test fusion rollback during compaction started")

        self.test_rollback_during_compaction()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()