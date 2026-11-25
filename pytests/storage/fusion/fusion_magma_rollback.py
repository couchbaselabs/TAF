import os
import random
import subprocess
import threading
from cb_constants.CBServer import CbServer
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from memcached.helper.data_helper import MemcachedClientHelper
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_rollback import MagmaRollbackTests


class FusionMagmaRollback(MagmaRollbackTests, FusionBase):
    """Runs the magma rollback tests, then the Fusion workflow on top.

    The magma base tests are owned by the magma suite and are not modified
    here. Where their behaviour under sirius_java_sdk leaves a bucket count
    the Fusion checks cannot predict, the helpers they call are overridden
    below. Each override is behind a flag that only the wrapper needing it
    sets, so every other test using this class runs the base code unchanged.
    """

    # Class-level defaults: the base setUp already calls the overridden
    # helpers (initial load) before setUp below could set instance values
    _split_update_iterations = False
    _pin_targeted_create_itr = False
    _count_replica_loads = False
    _replica_load_passes = 0

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

    # ------------------------------------------------------------------
    # Overrides of helpers the magma base tests call
    # ------------------------------------------------------------------
    def loadgen_docs(self, *args, **kwargs):
        """With _split_update_iterations set, run an update load with
        iterations=N as N sequential single-iteration loads.

        For iterations != 1 the Sirius loader switches the key type to
        CircularKey (a RandomKey), while the initial load used SimpleKey. Those
        "updates" then create new docs in a different key namespace instead
        of updating the existing ones, and the bucket count grows.
        """
        iterations = kwargs.get("iterations", 1)
        if not (self._split_update_iterations
                and iterations != 1
                and kwargs.get("doc_ops") == "update"
                and self.load_docs_using == "sirius_java_sdk"):
            return super(FusionMagmaRollback, self).loadgen_docs(
                *args, **kwargs)

        kwargs["iterations"] = 1
        for _ in range(iterations - 1):
            super(FusionMagmaRollback, self).loadgen_docs(
                *args, **dict(kwargs, _sync=True))
        # Last pass keeps the caller's _sync, so an async caller still gets
        # its tasks back to wait on
        return super(FusionMagmaRollback, self).loadgen_docs(*args, **kwargs)

    def generate_docs(self, *args, **kwargs):
        """With _pin_targeted_create_itr set, make a targeted Sirius create
        generator report itr == start.

        After the load during rollback, test_crash_during_rollback and
        test_rebalance_during_rollback set start_items = gen_create.itr.
        SiriusJavaDocGen.itr is only an iteration cursor and stays 0 for
        range-based loads, so start_items dropped to 0 and every later step 6
        created the empty range [0, 0). gen_create.start is the start_items
        the test passed to compute_docs(), so this keeps it unchanged. The
        Sirius loader reads start/end, never itr, so the load is unaffected.
        """
        ret = super(FusionMagmaRollback, self).generate_docs(*args, **kwargs)
        gen_create = getattr(self, "gen_create", None)
        if self._pin_targeted_create_itr \
                and kwargs.get("target_vbucket") is not None \
                and gen_create is not None \
                and hasattr(gen_create, "start"):
            gen_create.itr = gen_create.start
        return ret

    def genrate_docs_basic(self, start, end, target_vbucket=None,
                           *args, **kwargs):
        """With _count_replica_loads set, count loads aimed at vbuckets other
        than self.target_vbucket (the master's vbuckets).

        In test_magma_rollback_on_all_nodes, each pass of the slave-load loop
        makes exactly one such load and adds rollback_items to the test's
        final item count, so this count gives that final count.
        """
        if self._count_replica_loads and target_vbucket is not None \
                and target_vbucket is not self.target_vbucket:
            self._replica_load_passes += 1
        return super(FusionMagmaRollback, self).genrate_docs_basic(
            start, end, target_vbucket, *args, **kwargs)

    # ------------------------------------------------------------------
    # Expected-count helpers
    # ------------------------------------------------------------------
    def _default_scope_collections(self):
        return list(self.buckets[0].scopes[CbServer.default_scope]
                    .collections.keys())

    def _set_expected_items(self, expected):
        """Point the post-fusion checks at the count the base test leaves in
        the bucket. self.num_items is not usable: loadgen_docs adjusts it for
        memory-only batches that are later rolled back."""
        self.num_items = expected
        self.deleted_upto = 0
        self.log.info(f"Expected items after rollback test: {self.num_items}")

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------
    def test_fusion_magma_rollback_basic(self):

        if not self.fusion_enable and self.enable_fusion_during_rollback:
            self.configure_fusion()
            self.sleep(10, "Sleep after configuring fusion")

        initial_items = self.num_items
        self.test_magma_rollback_basic()

        # Every memory-only batch the base test loads is rolled back (the base
        # test itself validates against the initial count), but loadgen_docs
        # still added each batch to self.num_items.
        self._set_expected_items(initial_items)
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_magma_rollback_with_CDC(self):

        if not self.fusion_enable and self.enable_fusion_during_rollback:
            self.configure_fusion()

        self._split_update_iterations = True
        try:
            self.test_magma_rollback_with_CDC()
        finally:
            self._split_update_iterations = False

        # The base test only runs updates, so the bucket still holds the
        # initial load. The sirius_java_sdk initial load writes
        # init_items_per_collection docs into every collection but does not
        # scale self.num_items by the collection count, while verify_stats
        # compares the bucket-wide item count.
        self._set_expected_items(self.init_items_per_collection
                                 * self.get_loaded_collection_count())
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_magma_rollback_to_same_snapshot(self):

        if not self.fusion_enable and self.enable_fusion_during_rollback:
            self.configure_fusion()

        # Every memory-only batch is rolled back and the base test verifies
        # the bucket against its starting count each iteration
        initial_items = self.num_items
        self.test_magma_rollback_to_same_snapshot()
        self._set_expected_items(initial_items)
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_magma_rollback_on_all_nodes(self):

        if not self.fusion_enable and self.enable_fusion_during_rollback:
            self.configure_fusion()

        initial_items = self.num_items
        mem_only_items = self.input.param("rollback_items", 10000)
        self._replica_load_passes = 0
        self._count_replica_loads = True
        try:
            self.test_magma_rollback_on_all_nodes()
        finally:
            self._count_replica_loads = False

        # Master-node batches are rolled back. Each slave-load pass survives
        # and adds rollback_items (base test: items += mem_item_count)
        self._set_expected_items(
            initial_items + mem_only_items * self._replica_load_passes)
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_magma_rollback_to_new_snapshot(self):

        if not self.fusion_enable and self.enable_fusion_during_rollback:
            self.configure_fusion()

        items = self.num_items
        divisor = self.input.param("divisor", 5)
        self.test_magma_rollback_to_new_snapshot()

        # Memory-only batches are rolled back. Step 6 adds items // divisor
        # in every iteration except the last
        for _ in range(1, self.num_rollbacks):
            items = items + items // divisor
        self._set_expected_items(items)
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_crash_during_rollback(self):

        if not self.fusion_enable and self.enable_fusion_during_rollback:
            self.configure_fusion()

        start_items = self.num_items
        divisor = self.input.param("divisor", 30)
        collections = self._default_scope_collections()
        self._pin_targeted_create_itr = True
        try:
            self.test_crash_during_rollback()
        finally:
            self._pin_targeted_create_itr = False

        # Step 6 grows every collection by start_items // divisor per
        # iteration; memory-only batches are rolled back
        for _ in range(self.num_rollbacks):
            start_items = start_items + start_items // divisor
        self._set_expected_items(start_items * len(collections))
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_rebalance_during_rollback(self):

        if not self.fusion_enable and self.enable_fusion_during_rollback:
            self.configure_fusion()

        start_items = self.num_items
        collections = self._default_scope_collections()
        # Step 1 of the base test loads gen_create from create_start /
        # create_end without setting them. With init_loading=False (as in the
        # Fusion confs) setUp leaves them at 0:0, so nothing was loaded.
        self.create_start = 0
        self.create_end = self.num_items
        self._pin_targeted_create_itr = True
        try:
            self.test_rebalance_during_rollback()
        finally:
            self._pin_targeted_create_itr = False

        # Step 6 grows every collection by start_items // 5 per iteration
        for _ in range(self.num_rollbacks):
            start_items = start_items + start_items // 5
        self._set_expected_items(start_items * len(collections))
        self.execute_fusion_workflow_after_magma_test()
