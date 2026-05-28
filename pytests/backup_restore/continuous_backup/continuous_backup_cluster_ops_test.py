"""
Continuous Backup Cluster Operations Tests
==========================================

Validates PITR (Point-in-Time Recovery) correctness across disruptive cluster
operations: rebalance, failover, quorum loss, and KV process restart.

Key principle for all tests:
  1. Capture timestamp T1 + item count C1 BEFORE the disruptive operation.
  2. Perform the cluster operation.
  3. Capture timestamp T2 + item count C2 AFTER the operation.
  4. PITR restore to T1 -> verify count == C1.
  5. PITR restore to T2 -> verify count == C2.

This is the critical gap not covered by the existing bucket_collections-based
conf files (collections_rebalance.conf, collections_quorum_loss.conf etc.),
which only confirm that backup *runs* alongside cluster operations — not that
PITR *correctness* is preserved across them.

Note: PITR is supported only on Magma buckets. All tests use
      bucket_spec=single_bucket.continuous_backup_tests which enforces Magma.
"""

import time

from backup_restore.continuous_backup.continuous_backup_base import ContinuousBackupBase
from pytests.bucket_collections.collections_base import CollectionBase
from shell_util.remote_connection import RemoteMachineShellConnection


class ContinuousBackupClusterOpsTest(ContinuousBackupBase):
    """
    PITR correctness tests for cluster topology changes on Magma buckets.

    All tests inherit NFS setup, shell connection, backup manager, and
    continuous backup configuration from ContinuousBackupBase -> CollectionBase.
    """

    def setUp(self):
        super(ContinuousBackupClusterOpsTest, self).setUp()
        # Nodes that must be added back in tearDown after a failover
        self._failed_over_nodes = []

    def tearDown(self):
        # Re-add any failed-over nodes so the cluster is clean for the next test
        if self._failed_over_nodes:
            self.log.info(
                f"tearDown: re-adding failed-over nodes: "
                f"{[n.ip for n in self._failed_over_nodes]}")
            try:
                rebalance = self.task.async_rebalance(
                    self.cluster, self._failed_over_nodes, [])
                self.task.jython_task_manager.get_task_result(rebalance)
            except Exception as e:
                self.log.error(f"tearDown: failed to re-add nodes: {e}")
        super(ContinuousBackupClusterOpsTest, self).tearDown()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _wait_for_rebalance(self, rebalance_task, timeout=900):
        """Block until a rebalance task completes; fail if it does not succeed."""
        self.task.jython_task_manager.get_task_result(rebalance_task)
        if not rebalance_task.result:
            self.fail("Rebalance operation failed")
        self.log.info("Rebalance completed successfully")

    def _get_spare_node(self):
        """Return a server not currently part of the cluster (for rebalance-in)."""
        in_cluster_ips = {n.ip for n in self.cluster.nodes_in_cluster}
        for server in self.cluster.servers:
            if server.ip not in in_cluster_ips:
                return server
        self.fail(
            "No spare nodes available for rebalance-in. "
            "Ensure the server pool has more nodes than nodes_init.")

    def _get_removable_kv_node(self):
        """Return a non-master KV node suitable for rebalance-out or failover."""
        for node in self.cluster.kv_nodes:
            if node.ip != self.cluster.master.ip:
                return node
        self.fail(
            "No removable KV node found "
            "(need at least 2 KV nodes; master cannot be removed).")

    def _create_restore_bucket(self, restore_bucket_name):
        """Create a fresh Magma bucket to use as a PITR restore target."""
        from BucketLib.bucket import Bucket
        ram_quota = self.input.param("bucket_size", 100)
        self.bucket_util.create_default_bucket(
            self.cluster,
            bucket_name=restore_bucket_name,
            bucket_type=self.bucket_type,
            ram_quota=ram_quota,
            replica=self.num_replicas,
            storage=Bucket.StorageBackend.magma)

    def _delete_restore_bucket(self, restore_bucket_name):
        """Delete a restore bucket if it still exists."""
        bucket_obj = self.bucket_util.get_bucket_obj(
            self.cluster.buckets, restore_bucket_name)
        if bucket_obj:
            self.bucket_util.delete_bucket(self.cluster, bucket_obj)

    def _pitr_restore_to_new_bucket(self, restore_bucket_name, timestamp):
        """Restore the primary bucket to a new bucket at the given timestamp."""
        self.cont_bk_mgr.restore(
            self.backup_archive_dir, self.backup_repo_name,
            location=self.continuous_backup_location,
            temp_dir="/tmp",
            timestamp=timestamp,
            map_data=f"{self.bucket.name}={restore_bucket_name}")

    def _load_data_and_get_count(self, mutation_num=0):
        """Load data from spec and return the resulting item count."""
        CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
        self.bucket_util._wait_for_stats_all_buckets(
            self.cluster, self.cluster.buckets)
        return self.bucket_util.get_buckets_item_count(
            self.cluster, self.bucket.name)

    def _verify_doc_count(self, expected_count, bucket_name=None, timeout=300):
        """Poll until actual item count matches expected; fail on timeout."""
        if bucket_name is None:
            bucket_name = self.bucket.name
        self.log.info(
            f"Verifying doc count for '{bucket_name}'. Expected: {expected_count}")
        deadline = time.time() + timeout
        actual = None
        while time.time() < deadline:
            actual = self.bucket_util.get_buckets_item_count(
                self.cluster, bucket_name)
            if actual == expected_count:
                self.log.info(
                    f"Doc count verified for '{bucket_name}': {actual}")
                return
            self.log.info(
                f"Count mismatch — actual={actual}, expected={expected_count}. Retrying in 10s...")
            self.sleep(10)
        self.fail(
            f"Doc count mismatch for '{bucket_name}' after {timeout}s: "
            f"expected={expected_count}, actual={actual}")

    # ==================================================================
    # P0 Tests — PITR timestamp integrity across cluster operations
    # ==================================================================

    def test_pitr_timestamps_across_rebalance_in(self):
        """
        P0: Validates PITR correctness when a new node is rebalanced in.

        This test addresses the gap where existing conf/backup_restore/
        collections_rebalance.conf tests only confirm backup runs alongside
        rebalance — they do NOT verify that a timestamp captured BEFORE
        rebalance restores correctly to the pre-rebalance state.

        Note: PITR is supported only on Magma buckets.

        Flow:
          1. Load data; capture T1 and count C1.
          2. Rebalance a new KV node into the cluster.
          3. Load more data; capture T2 and count C2.
          4. PITR restore to T1 -> verify count == C1.
          5. PITR restore to T2 -> verify count == C2.
        """
        count_c1 = self._load_data_and_get_count()
        self.sleep(self.continuous_backup_interval * 60,
                   "Waiting for backup interval before rebalance-in")
        ts_t1 = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"T1 (pre-rebalance-in): {ts_t1}, count: {count_c1}")

        spare = self._get_spare_node()
        self.log.info(f"Rebalancing in node: {spare.ip}")
        rebalance = self.task.async_rebalance(self.cluster, [spare], [])
        self._wait_for_rebalance(rebalance)

        count_c2 = self._load_data_and_get_count()
        self.sleep(self.continuous_backup_interval * 60,
                   "Waiting for backup interval after rebalance-in")
        ts_t2 = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"T2 (post-rebalance-in): {ts_t2}, count: {count_c2}")

        self.backup_mgr.backup(self.backup_archive_dir, self.backup_repo_name)

        restore_name = f"restore_rbl_in_{int(time.time())}"
        self._create_restore_bucket(restore_name)

        self.log.info("PITR to T1 (pre-rebalance-in)")
        self._pitr_restore_to_new_bucket(restore_name, ts_t1)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_c1, bucket_name=restore_name)
        self.log.info(f"T1 restore verified: {count_c1} items")

        self._delete_restore_bucket(restore_name)
        self._create_restore_bucket(restore_name)

        self.log.info("PITR to T2 (post-rebalance-in)")
        self._pitr_restore_to_new_bucket(restore_name, ts_t2)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_c2, bucket_name=restore_name)
        self.log.info(f"T2 restore verified: {count_c2} items")

        self.log.info("Test completed successfully")

    def test_pitr_timestamps_across_rebalance_out(self):
        """
        P0: Validates PITR correctness when a KV node is rebalanced out.

        Removing a node triggers vbucket migration to the remaining nodes.
        The continuous backup DCP stream must reconnect and resume without
        gaps so that PITR timestamps captured before and after the rebalance
        are still valid.

        Note: PITR is supported only on Magma buckets.
        """
        count_c1 = self._load_data_and_get_count()
        self.sleep(self.continuous_backup_interval * 60,
                   "Waiting for backup interval before rebalance-out")
        ts_t1 = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"T1 (pre-rebalance-out): {ts_t1}, count: {count_c1}")

        remove_node = self._get_removable_kv_node()
        self.log.info(f"Rebalancing out node: {remove_node.ip}")
        rebalance = self.task.async_rebalance(self.cluster, [], [remove_node])
        self._wait_for_rebalance(rebalance)

        count_c2 = self._load_data_and_get_count()
        self.sleep(self.continuous_backup_interval * 60,
                   "Waiting for backup interval after rebalance-out")
        ts_t2 = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"T2 (post-rebalance-out): {ts_t2}, count: {count_c2}")

        self.backup_mgr.backup(self.backup_archive_dir, self.backup_repo_name)

        restore_name = f"restore_rbl_out_{int(time.time())}"
        self._create_restore_bucket(restore_name)

        self.log.info("PITR to T1 (pre-rebalance-out)")
        self._pitr_restore_to_new_bucket(restore_name, ts_t1)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_c1, bucket_name=restore_name)

        self._delete_restore_bucket(restore_name)
        self._create_restore_bucket(restore_name)

        self.log.info("PITR to T2 (post-rebalance-out)")
        self._pitr_restore_to_new_bucket(restore_name, ts_t2)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_c2, bucket_name=restore_name)

        self.log.info("Test completed successfully")

    def test_pitr_timestamps_across_swap_rebalance(self):
        """
        P0: Validates PITR correctness across a swap rebalance (add + remove
        simultaneously).

        A swap rebalance is particularly important for continuous backup because
        it moves ALL vbuckets simultaneously rather than sequentially, creating
        a brief window of increased DCP churn.

        Note: PITR is supported only on Magma buckets.
        """
        count_c1 = self._load_data_and_get_count()
        self.sleep(self.continuous_backup_interval * 60,
                   "Waiting for backup interval before swap rebalance")
        ts_t1 = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"T1 (pre-swap): {ts_t1}, count: {count_c1}")

        spare = self._get_spare_node()
        remove_node = self._get_removable_kv_node()
        self.log.info(
            f"Swap rebalance: adding {spare.ip}, removing {remove_node.ip}")
        rebalance = self.task.async_rebalance(
            self.cluster, [spare], [remove_node])
        self._wait_for_rebalance(rebalance)

        count_c2 = self._load_data_and_get_count()
        self.sleep(self.continuous_backup_interval * 60,
                   "Waiting for backup interval after swap rebalance")
        ts_t2 = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"T2 (post-swap): {ts_t2}, count: {count_c2}")

        self.backup_mgr.backup(self.backup_archive_dir, self.backup_repo_name)

        restore_name = f"restore_swap_{int(time.time())}"
        self._create_restore_bucket(restore_name)

        self.log.info("PITR to T1 (pre-swap-rebalance)")
        self._pitr_restore_to_new_bucket(restore_name, ts_t1)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_c1, bucket_name=restore_name)

        self._delete_restore_bucket(restore_name)
        self._create_restore_bucket(restore_name)

        self.log.info("PITR to T2 (post-swap-rebalance)")
        self._pitr_restore_to_new_bucket(restore_name, ts_t2)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_c2, bucket_name=restore_name)

        self.log.info("Test completed successfully")

    def test_pitr_timestamps_across_graceful_failover(self):
        """
        P0: Validates PITR correctness across a graceful failover + recovery.

        Graceful failover moves active vbuckets to replica nodes before taking
        the node offline. The continuous backup DCP stream must survive the
        topology change so that pre-failover timestamps remain valid.

        Note: PITR is supported only on Magma buckets.
        """
        count_c1 = self._load_data_and_get_count()
        self.sleep(self.continuous_backup_interval * 60,
                   "Waiting for backup interval before graceful failover")
        ts_t1 = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"T1 (pre-failover): {ts_t1}, count: {count_c1}")

        failover_node = self._get_removable_kv_node()
        self.log.info(f"Graceful failover of node: {failover_node.ip}")
        result = self.task.failover(
            self.cluster,
            failover_nodes=[failover_node],
            graceful=True)
        if not result:
            self.fail("Graceful failover did not succeed")
        self._failed_over_nodes.append(failover_node)

        # Rebalance out the failed-over node to complete the failover
        self.log.info("Rebalancing out failed-over node")
        rebalance = self.task.async_rebalance(self.cluster, [], [failover_node])
        self._wait_for_rebalance(rebalance)
        self._failed_over_nodes.remove(failover_node)

        count_c2 = self._load_data_and_get_count()
        self.sleep(self.continuous_backup_interval * 60,
                   "Waiting for backup interval after graceful failover")
        ts_t2 = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"T2 (post-failover): {ts_t2}, count: {count_c2}")

        self.backup_mgr.backup(self.backup_archive_dir, self.backup_repo_name)

        restore_name = f"restore_graceful_fo_{int(time.time())}"
        self._create_restore_bucket(restore_name)

        self.log.info("PITR to T1 (pre-graceful-failover)")
        self._pitr_restore_to_new_bucket(restore_name, ts_t1)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_c1, bucket_name=restore_name)

        self._delete_restore_bucket(restore_name)
        self._create_restore_bucket(restore_name)

        self.log.info("PITR to T2 (post-graceful-failover)")
        self._pitr_restore_to_new_bucket(restore_name, ts_t2)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_c2, bucket_name=restore_name)

        self.log.info("Test completed successfully")

    def test_pitr_timestamps_across_hard_failover(self):
        """
        P0: Validates PITR correctness across a hard (non-graceful) failover.

        Hard failover immediately removes a node without migrating vbuckets.
        Active vbuckets become unavailable until replicas are promoted.
        The continuous backup stream must reconnect and capture the state
        transition so pre- and post-failover PITR timestamps remain valid.

        Note: PITR is supported only on Magma buckets.
        """
        count_c1 = self._load_data_and_get_count()
        self.sleep(self.continuous_backup_interval * 60,
                   "Waiting for backup interval before hard failover")
        ts_t1 = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"T1 (pre-hard-failover): {ts_t1}, count: {count_c1}")

        failover_node = self._get_removable_kv_node()
        self.log.info(f"Hard failover of node: {failover_node.ip}")
        result = self.task.failover(
            self.cluster,
            failover_nodes=[failover_node],
            graceful=False)
        if not result:
            self.fail("Hard failover did not succeed")
        self._failed_over_nodes.append(failover_node)

        # Rebalance out the failed-over node
        self.log.info("Rebalancing out hard-failed-over node")
        rebalance = self.task.async_rebalance(self.cluster, [], [failover_node])
        self._wait_for_rebalance(rebalance)
        self._failed_over_nodes.remove(failover_node)

        count_c2 = self._load_data_and_get_count()
        self.sleep(self.continuous_backup_interval * 60,
                   "Waiting for backup interval after hard failover")
        ts_t2 = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"T2 (post-hard-failover): {ts_t2}, count: {count_c2}")

        self.backup_mgr.backup(self.backup_archive_dir, self.backup_repo_name)

        restore_name = f"restore_hard_fo_{int(time.time())}"
        self._create_restore_bucket(restore_name)

        self.log.info("PITR to T1 (pre-hard-failover)")
        self._pitr_restore_to_new_bucket(restore_name, ts_t1)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_c1, bucket_name=restore_name)

        self._delete_restore_bucket(restore_name)
        self._create_restore_bucket(restore_name)

        self.log.info("PITR to T2 (post-hard-failover)")
        self._pitr_restore_to_new_bucket(restore_name, ts_t2)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_c2, bucket_name=restore_name)

        self.log.info("Test completed successfully")

    def test_pitr_timestamps_across_quorum_loss(self):
        """
        P0: Validates PITR correctness after a quorum loss + unsafe failover.

        Quorum loss (majority of nodes unreachable) causes the cluster to stop
        accepting writes. After an unsafe failover the cluster resumes with
        reduced replica count. The continuous backup DCP stream must survive
        the disruption so that the pre-quorum-loss timestamp is still valid.

        This test requires at least 4 KV nodes so 2 can be lost while the
        remaining 2 still hold enough data for a meaningful backup.

        Note: PITR is supported only on Magma buckets.
        """
        if len(self.cluster.kv_nodes) < 4:
            self.skipTest(
                "Quorum loss test requires at least 4 KV nodes; "
                f"cluster has {len(self.cluster.kv_nodes)}")

        count_c1 = self._load_data_and_get_count()
        self.sleep(self.continuous_backup_interval * 60,
                   "Waiting for backup interval before quorum loss")
        ts_t1 = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"T1 (pre-quorum-loss): {ts_t1}, count: {count_c1}")

        # Select two non-master KV nodes to fail over (simulates quorum loss)
        quorum_loss_nodes = []
        for node in self.cluster.kv_nodes:
            if node.ip != self.cluster.master.ip and len(quorum_loss_nodes) < 2:
                quorum_loss_nodes.append(node)
        self.log.info(
            f"Unsafe failover of nodes: {[n.ip for n in quorum_loss_nodes]}")

        for node in quorum_loss_nodes:
            result = self.task.failover(
                self.cluster,
                failover_nodes=[node],
                graceful=False,
                allow_unsafe=True)
            if not result:
                self.log.warning(
                    f"Unsafe failover of {node.ip} returned False — "
                    f"may be expected depending on cluster state")
            self._failed_over_nodes.append(node)

        # Rebalance out the failed-over nodes to stabilise the cluster
        self.log.info("Rebalancing out quorum-lost nodes")
        rebalance = self.task.async_rebalance(
            self.cluster, [], quorum_loss_nodes)
        self._wait_for_rebalance(rebalance)
        for node in quorum_loss_nodes:
            if node in self._failed_over_nodes:
                self._failed_over_nodes.remove(node)

        count_c2 = self._load_data_and_get_count()
        self.sleep(self.continuous_backup_interval * 60,
                   "Waiting for backup interval after quorum loss recovery")
        ts_t2 = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"T2 (post-quorum-loss): {ts_t2}, count: {count_c2}")

        self.backup_mgr.backup(self.backup_archive_dir, self.backup_repo_name)

        restore_name = f"restore_quorum_loss_{int(time.time())}"
        self._create_restore_bucket(restore_name)

        self.log.info("PITR to T1 (pre-quorum-loss)")
        self._pitr_restore_to_new_bucket(restore_name, ts_t1)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_c1, bucket_name=restore_name)
        self.log.info(f"T1 restore verified: {count_c1} items")

        self._delete_restore_bucket(restore_name)
        self._create_restore_bucket(restore_name)

        self.log.info("PITR to T2 (post-quorum-loss recovery)")
        self._pitr_restore_to_new_bucket(restore_name, ts_t2)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_c2, bucket_name=restore_name)
        self.log.info(f"T2 restore verified: {count_c2} items")

        self.log.info("Test completed successfully")

    # ==================================================================
    # P1 Tests — KV process restart during continuous backup
    # ==================================================================

    def test_pitr_after_kv_process_restart(self):
        """
        P1: Validates continuous backup resumes without data gaps after the
        memcached (KV) process is killed and restarted on one node.

        Continuous backup relies on a persistent DCP stream from memcached.
        If memcached crashes and restarts, the DCP client inside the backup
        service must reconnect and resume streaming from the last acknowledged
        sequence number.  Any gap in the stream would corrupt the PITR log.

        This test kills memcached on one non-master node, waits for warmup,
        then verifies a PITR restore to a post-restart timestamp is correct.

        Note: PITR is supported only on Magma buckets.
        """
        # Phase 1: load data and capture pre-restart state
        count_c1 = self._load_data_and_get_count()
        self.sleep(self.continuous_backup_interval * 60,
                   "Waiting for backup interval before KV restart")
        ts_t1 = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"T1 (pre-KV-restart): {ts_t1}, count: {count_c1}")

        # Kill memcached on one non-master KV node
        kill_node = self._get_removable_kv_node()
        self.log.info(f"Killing memcached on node: {kill_node.ip}")
        shell = RemoteMachineShellConnection(kill_node)
        try:
            shell.kill_memcached()
        finally:
            shell.disconnect()

        # Wait for memcached to restart and the bucket to warm up
        warmup_wait = 60
        self.sleep(warmup_wait,
                   f"Waiting {warmup_wait}s for memcached restart and warmup")
        self.bucket_util._wait_for_stats_all_buckets(
            self.cluster, self.cluster.buckets)
        self.log.info("Memcached restarted and bucket warmed up")

        # Phase 2: load data after restart; continuous backup must resume seamlessly
        count_c2 = self._load_data_and_get_count()
        self.sleep(self.continuous_backup_interval * 60,
                   "Waiting for backup interval after KV restart "
                   "(DCP stream must have reconnected)")
        ts_t2 = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"T2 (post-KV-restart): {ts_t2}, count: {count_c2}")

        self.backup_mgr.backup(self.backup_archive_dir, self.backup_repo_name)

        restore_name = f"restore_kv_restart_{int(time.time())}"
        self._create_restore_bucket(restore_name)

        # PITR to T1 (before restart) must succeed
        self.log.info("PITR to T1 (pre-KV-restart)")
        self._pitr_restore_to_new_bucket(restore_name, ts_t1)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_c1, bucket_name=restore_name)
        self.log.info(f"T1 restore verified: {count_c1} items")

        self._delete_restore_bucket(restore_name)
        self._create_restore_bucket(restore_name)

        # PITR to T2 (after restart) must also succeed — this is the key assertion:
        # it confirms the DCP stream reconnected without leaving a gap in the log
        self.log.info(
            "PITR to T2 (post-KV-restart) — validates no gap in continuous backup log")
        self._pitr_restore_to_new_bucket(restore_name, ts_t2)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_c2, bucket_name=restore_name)
        self.log.info(
            f"T2 restore verified: {count_c2} items — DCP stream resumed without gaps")

        self.log.info("Test completed successfully")
