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
