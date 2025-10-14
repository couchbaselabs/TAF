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

        self.log.info("Test fusion magma rollback basic started")

        self.test_magma_rollback_basic()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_magma_rollback_with_CDC(self):

        self.log.info("Test fusion magma rollback with CDC started")

        self.test_magma_rollback_with_CDC()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_magma_rollback_to_same_snapshot(self):

        self.log.info("Test fusion magma rollback to same snapshot started")

        self.test_magma_rollback_to_same_snapshot()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_magma_rollback_on_all_nodes(self):

        self.log.info("Test fusion magma rollback on all nodes started")

        self.test_magma_rollback_on_all_nodes()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_magma_rollback_to_new_snapshot(self):

        self.log.info("Test fusion magma rollback to new snapshot started")

        self.test_magma_rollback_to_new_snapshot()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_crash_during_rollback(self):

        self.log.info("Test fusion crash during rollback started")

        self.test_crash_during_rollback()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_rebalance_during_rollback(self):

        self.log.info("Test fusion rebalance during rollback started")

        self.test_rebalance_during_rollback()

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()