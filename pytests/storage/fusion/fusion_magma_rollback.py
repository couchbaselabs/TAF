import os
import random
import subprocess
import threading
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from memcached.helper.data_helper import MemcachedClientHelper
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_rollback import MagmaRollbackTests


class FusionMagmaRollback(MagmaRollbackTests, FusionBase):
    def setUp(self):
        super(FusionMagmaRollback, self).setUp()

        self.log.info("FusionMagmaRollback setup started")

    def tearDown(self):
        self._restart_persistence_on_all_nodes()
        super(FusionMagmaRollback, self).tearDown()

    def _restart_persistence_on_all_nodes(self):
        """The base rollback tests stop persistence on a node and only
        restart it by killing memcached. When a stat wait fails in between,
        persistence stays stopped, the disk-write queue never drains, and
        the tearDown health check fails on
        'persistence:num_items_for_cursor' as a side effect. Starting
        persistence again is harmless where it is already running."""
        for node in self.cluster.nodes_in_cluster:
            for bucket in self.cluster.buckets:
                client = None
                try:
                    client = MemcachedClientHelper.direct_client(node, bucket)
                    client.start_persistence()
                except Exception as e:
                    # Already running, or the node is down: nothing to undo
                    self.log.debug(f"start_persistence on {node.ip} "
                                   f"{bucket.name}: {e}")
                finally:
                    if client:
                        client.close()

    def test_fusion_magma_rollback_basic(self):

        if not self.fusion_enable and self.enable_fusion_during_rollback:
            self.configure_fusion()
            self.sleep(10, "Sleep after configuring fusion")

        self.test_magma_rollback_basic()
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_magma_rollback_with_CDC(self):

        if not self.fusion_enable and self.enable_fusion_during_rollback:
            self.configure_fusion()

        self.test_magma_rollback_with_CDC()
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_magma_rollback_to_same_snapshot(self):

        if not self.fusion_enable and self.enable_fusion_during_rollback:
            self.configure_fusion()

        self.test_magma_rollback_to_same_snapshot()
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_magma_rollback_on_all_nodes(self):

        if not self.fusion_enable and self.enable_fusion_during_rollback:
            self.configure_fusion()

        self.test_magma_rollback_on_all_nodes()
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_magma_rollback_to_new_snapshot(self):

        if not self.fusion_enable and self.enable_fusion_during_rollback:
            self.configure_fusion()

        self.test_magma_rollback_to_new_snapshot()
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_crash_during_rollback(self):

        if not self.fusion_enable and self.enable_fusion_during_rollback:
            self.configure_fusion()

        self.test_crash_during_rollback()
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_rebalance_during_rollback(self):

        if not self.fusion_enable and self.enable_fusion_during_rollback:
            self.configure_fusion()

        self.test_rebalance_during_rollback()
        self.execute_fusion_workflow_after_magma_test()