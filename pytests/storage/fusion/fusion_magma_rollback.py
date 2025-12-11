import os
import random
import subprocess
import threading
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_rollback import MagmaRollbackTests


class FusionMagmaRollback(MagmaRollbackTests, FusionBase):
    def setUp(self):
        super(FusionMagmaRollback, self).setUp()

        self.log.info("FusionMagmaRollback setup started")

    def tearDown(self):
        super(FusionMagmaRollback, self).tearDown()

    def test_fusion_magma_rollback_basic(self):

        if not self.fusion_test and self.enable_fusion_during_rollback:
            self.configure_fusion()
            self.sleep(10, "Sleep after configuring fusion")

        self.test_magma_rollback_basic()
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_magma_rollback_with_CDC(self):

        if not self.fusion_test and self.enable_fusion_during_rollback:
            self.configure_fusion()

        self.test_magma_rollback_with_CDC()
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_magma_rollback_to_same_snapshot(self):

        if not self.fusion_test and self.enable_fusion_during_rollback:
            self.configure_fusion()

        self.test_magma_rollback_to_same_snapshot()
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_magma_rollback_on_all_nodes(self):

        if not self.fusion_test and self.enable_fusion_during_rollback:
            self.configure_fusion()

        self.test_magma_rollback_on_all_nodes()
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_magma_rollback_to_new_snapshot(self):

        if not self.fusion_test and self.enable_fusion_during_rollback:
            self.configure_fusion()

        self.test_magma_rollback_to_new_snapshot()
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_crash_during_rollback(self):

        if not self.fusion_test and self.enable_fusion_during_rollback:
            self.configure_fusion()

        self.test_crash_during_rollback()
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_rebalance_during_rollback(self):

        if not self.fusion_test and self.enable_fusion_during_rollback:
            self.configure_fusion()

        self.test_rebalance_during_rollback()
        self.execute_fusion_workflow_after_magma_test()