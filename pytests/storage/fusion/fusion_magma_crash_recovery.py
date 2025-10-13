import os
import random
import subprocess
import threading
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_crash_recovery import MagmaCrashTests


class FusionMagmaCrashRecovery(MagmaCrashTests, FusionBase):
    def setUp(self):
        super(FusionMagmaCrashRecovery, self).setUp()

        self.log.info("FusionCrashRecovery setup started")

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
        super(FusionMagmaCrashRecovery, self).tearDown()

    def test_fusion_crash_during_dedupe(self):

        self.log.info("Test fusion crash during dedupe started")

        self.test_crash_during_dedupe()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()


    def test_fusion_crash_during_get_ops(self):

        self.log.info("Test fusion crash during get ops started")

        self.test_crash_during_get_ops()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()


    def test_fusion_wal_replay(self):

        self.log.info("Test fusion wal replay started")

        self.test_wal_replay()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()



    def test_fusion_crash_recovery_large_docs(self):

        self.log.info("Test fusion crash recovery with large documents started")

        self.test_crash_recovery_large_docs()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_crash_during_ops_new(self):

        self.log.info("Test fusion crash during ops on new cluster started")

        self.test_crash_during_ops_new()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()


    def test_fusion_crash_during_recovery_new(self):

        self.log.info("Test fusion crash during recovery on new cluster started")

        self.test_crash_during_recovery_new()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()
