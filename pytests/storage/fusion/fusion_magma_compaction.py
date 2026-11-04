import os
import subprocess
from cb_constants import CbServer
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_compaction import MagmaCompactionTests


class FusionMagmaCompactionTests(MagmaCompactionTests, FusionBase):
    def setUp(self):
        super(FusionMagmaCompactionTests, self).setUp()

        self.log.info("FusionMagmaCompactionTests setUp Started")

    def tearDown(self):
        super(FusionMagmaCompactionTests, self).tearDown()

    def test_fusion_test_crash_during_compaction(self):

        if not self.fusion_enable and self.enable_fusion_during_compaction:
            self.configure_fusion()

        self.test_crash_during_compaction()
        self._settle_after_crash_during_compaction()
        self.execute_fusion_workflow_after_magma_test()

    def _settle_after_crash_during_compaction(self):
        """Bring the bucket to the exact end state the base test intended and
        point num_items/deleted_upto at it.

        The base test loads every collection in self.collections with
        track_failures=False and FAIL_FAST while memcached is being killed or
        restarted, so writes that were not yet persisted are lost and never
        retried. Its loadgen_docs calls also adjust self.num_items once per
        collection, so with many collections (e.g. doc_ops=expiry with 50
        collections) self.num_items ends up far off, even negative.
        compute_docs_ranges() tracks the intended per-collection count in
        num_items_per_collection, which is what we use here.
        """
        live_ranges = list()
        if "update" in self.doc_ops:
            live_ranges.append((self.update_start, self.update_end))
        if "create" in self.doc_ops:
            live_ranges.append((self.create_start, self.create_end))

        # Re-drive the live ranges now that no crash is running. Upserts are
        # idempotent and only fill in docs lost during the crash.
        for start, end in live_ranges:
            for collection in self.collections:
                self.generate_docs(doc_ops="update",
                                   update_start=start, update_end=end)
                self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  scope=CbServer.default_scope,
                                  collection=collection,
                                  _sync=True, doc_ops="update")

        if "expiry" in self.doc_ops:
            # Expired docs still count in curr_items until purged
            self.bucket_util._expiry_pager(self.cluster, val=10)
            self.sleep(self.maxttl + 60,
                       "Wait for the expiry pager to purge expired docs")

        self.num_items = self.num_items_per_collection * len(self.collections)
        # Reads cover [deleted_upto, deleted_upto + per-collection count).
        # With creates, [create_start, create_end) is the largest contiguous
        # live range; otherwise live docs start at key 0.
        self.deleted_upto = self.create_start \
            if "create" in self.doc_ops else 0
        self.log.info(f"Expected items after crash during compaction: "
                      f"{self.num_items} ({self.num_items_per_collection} x "
                      f"{len(self.collections)} collections), "
                      f"reads start at {self.deleted_upto}")


    def test_fusion_test_rollback_during_compaction(self):

        if not self.fusion_enable and self.enable_fusion_during_compaction:
            self.configure_fusion()

        initial_items = self.num_items
        self.test_rollback_during_compaction()

        # The base test only keeps the docs it creates after the rollback
        # (100K per iteration, keys starting at the original num_items); the
        # memory-only mutations are rolled back. Without init_loading there
        # are no docs below that key range, so the post-fusion item count
        # and doc validation must be anchored to what is really in the bucket.
        rollback_created = 100000 * self.num_rollbacks
        if self.init_loading:
            self.num_items = initial_items + rollback_created
        else:
            self.deleted_upto = initial_items
            self.num_items = rollback_created
        self.execute_fusion_workflow_after_magma_test()