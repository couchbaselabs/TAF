import os
import subprocess
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_compaction import MagmaCompactionTests


class FusionMagmaCompactionTests(MagmaCompactionTests, FusionBase):
    def setUp(self):
        super(FusionMagmaCompactionTests, self).setUp()

        self.log.info("FusionMagmaCrashTests setUp Started")

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