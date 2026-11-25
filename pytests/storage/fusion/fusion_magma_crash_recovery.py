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

    def java_doc_loader(self, *args, **kwargs):
        """While _honour_skip_default is set, replace the base test's
        hard-coded skip_default=True with skip_load_to_default_collection.

        test_crash_during_ops_new / test_crash_during_recovery_new (owned by
        the magma suite) always pass skip_default=True. The Fusion confs do
        not set num_collections, so _default is the only collection and those
        loads write nothing. Overriding here keeps the fix out of the magma
        files.
        """
        if getattr(self, "_honour_skip_default", False) \
                and kwargs.get("skip_default") is True:
            kwargs["skip_default"] = self.skip_load_to_default_collection
        return super(FusionMagmaCrashRecovery, self).java_doc_loader(
            *args, **kwargs)

    def _run_honouring_skip_default(self, base_test):
        self._honour_skip_default = True
        try:
            base_test()
        finally:
            self._honour_skip_default = False

    def _settle_after_crash_ops(self):
        """Re-apply the end state test_crash_during_ops_new /
        test_crash_during_recovery_new intended, then point num_items and
        deleted_upto at it.

        Both base tests create [0, N) per loaded collection, then run the
        conf'd doc_ops over the compute_docs_ranges() ranges while memcached
        is being killed, with track_failures=False and FAIL_FAST. Writes lost
        to a kill are never retried, so the bucket ends up short by a random
        amount. They also never update self.num_items.

        Live keys per collection afterwards:
          - [0, N), plus [create_start, create_end) when doc_ops has create
          - minus [delete_start, delete_end) when doc_ops has delete
        "expiry" is a no-op here: Sirius runs it as an insert with TTL, which
        fails on keys that already exist, so those docs stay live.
        """
        ops = self.doc_ops.split(":")
        n = self.init_items_per_collection
        live_end = max(n, self.create_end)
        del_start, del_end = (self.delete_start, self.delete_end) \
            if "delete" in ops else (0, 0)

        live = list()
        if del_start > 0:
            live.append((0, del_start))
        if live_end > max(del_end, 0):
            live.append((max(del_end, del_start), live_end))

        # No crash is running now. Inserting a key that exists and deleting
        # one that is gone are both no-ops, so these only repair lost writes.
        for start, end in live:
            self.perform_workload(start, end, "create")
        if del_end > del_start:
            self.perform_workload(del_start, del_end, "delete")

        per_collection = sum(end - start for start, end in live)
        self.num_items = per_collection * self.get_loaded_collection_count()
        # Read-back covers [deleted_upto, deleted_upto + per_collection)
        self.deleted_upto = live[0][0] if len(live) == 1 else 0
        if len(live) > 1:
            self.log.warning(f"Live key ranges {live} are not contiguous, "
                             f"doc read-back from 0 will hit deleted keys")
        self.log.info(f"Expected items after crash test: {self.num_items} "
                      f"({per_collection} per collection, live {live})")

    def test_fusion_crash_during_dedupe(self):

        self.log.info("Test fusion crash during dedupe started")

        self.test_crash_during_dedupe()

        self.sleep(120, "Wait before executing fusion workflow after magma test")

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()


    def test_fusion_crash_during_get_ops(self):

        self.log.info("Test fusion crash during get ops started")

        self.test_crash_during_get_ops()

        self.sleep(120, "Wait before executing fusion workflow after magma test")

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()


    def test_fusion_wal_replay(self):

        self.log.info("Test fusion wal replay started")

        self.test_wal_replay()

        self.sleep(120, "Wait before executing fusion workflow after magma test")

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()



    def test_fusion_crash_recovery_large_docs(self):

        self.log.info("Test fusion crash recovery with large documents started")

        self.test_crash_recovery_large_docs()
        # The base test loads only with cbc-pillowfight and never touches
        # num_items, so the Fusion checks compared against the 100000
        # default. Expect what it loads: the large docs plus the 20000 1MB
        # docs of the rate-limited pillowfight thread.
        self.num_items = sum(int(self.input.param(param, default))
                             for param, default in (
                                 ("num_2mb_docs", 1000),
                                 ("num_4mb_docs", 1000),
                                 ("num_8mb_docs", 1000),
                                 ("num_16mb_docs", 500),
                                 ("num_20mb_docs", 500))) + 20000
        self.deleted_upto = 0
        # pillowfight keys, not sirius ones
        self.skip_doc_read_back = True

        self.sleep(120, "Wait before executing fusion workflow after magma test")

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()

    def test_fusion_crash_during_ops_new(self):

        self.log.info("Test fusion crash during ops on new cluster started")

        self._run_honouring_skip_default(self.test_crash_during_ops_new)
        self._settle_after_crash_ops()

        self.sleep(120, "Wait before executing fusion workflow after magma test")

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()


    def test_fusion_crash_during_recovery_new(self):

        self.log.info("Test fusion crash during recovery on new cluster started")

        self._run_honouring_skip_default(self.test_crash_during_recovery_new)
        self._settle_after_crash_ops()

        self.sleep(120, "Wait before executing fusion workflow after magma test")

        # Execute the complete fusion workflow after magma test
        self.execute_fusion_workflow_after_magma_test()
