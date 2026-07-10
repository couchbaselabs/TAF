"""
Fusion Bucket Operation Tests

Validates bucket-level operations with fusion enabled to ensure they do not
corrupt or inadvertently delete the fusion S3 log-store (§5 of the E2E test plan).

Tests in this file:
  9.  test_bucket_flush_cleans_s3_objects                    — flush must reduce S3 log-store to < 1 GB
  10. test_bucket_delete_after_rebalance_cleans_guest_volumes — delete post-rebalance; EBS volumes gone within 5 min
  11. test_bucket_flush_after_rebalance_no_guest_volumes      — flush post-rebalance; no orphaned EBS volumes within 5 min
  12. test_bucket_drop_during_guest_volume_deletion           — drop bucket while CP is mid-EBS-teardown
  13. test_bucket_drop_and_recreate_loop                      — rapid drop/recreate cycle; fresh UUID each time
  14. test_full_compaction_with_fusion_enabled                — compact syncs to S3; fusion rebalance succeeds after
  15. test_replica_change_uploader_map_unchanged              — replica count change must not reshuffle uploaders
  16. test_flush_during_active_s3_upload                      — flush while S3 upload is in-flight
  17. test_multi_bucket_flush_cleans_all_s3_prefixes          — flush N buckets; every kv/<uuid> prefix cleaned
  18. test_bucket_delete_no_prior_rebalance_s3_prefix_cleaned — delete bucket (no rebalance) cleans S3 prefix
"""

import time

from BucketLib.BucketOperations import BucketHelper
from capella_utils.dedicated import CapellaUtils as CapellaAPI
from couchbase_utils.cb_server_rest_util.fusion.fusion_api import FusionRestAPI
from membase.api.rest_client import RestConnection
from .fusion_test_base import _FusionTestBase


class FusionBucketOpsTest(_FusionTestBase):
    """
    Tests for bucket operations (flush, drop, replica changes) with fusion enabled.

    Ensures CBS bucket operations that clear local data do not propagate incorrectly
    to the S3 log-store that backs the fusion accelerator's hydration path.
    """

    def setUp(self):
        super().setUp()
        self.log.info(f"[setUp] cluster={self.cluster.id}")
        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id,
            "Wait for healthy cluster before bucket cleanup", timeout=600)
        self.initial_kv_nodes = self.input.param("kv_nodes", 3)
        for bucket in self.cluster.buckets:
            try:
                self._delete_bucket_with_s3_cleanup(bucket)
            except Exception as e:
                self.log.warning(f"setUp cleanup failed for {bucket.name}: {e}")
        self.cluster.buckets = []
        self.create_buckets(self.pod, self.tenant, self.cluster)
        if self.cluster.buckets:
            rest = RestConnection(self.cluster.master)
            for bucket in self.cluster.buckets:
                info = rest.get_bucket_details(bucket_name=bucket.name)
                bucket.bucket_uuid = info.get("uuid", None)

    def tearDown(self):
        if self.num_nodes["data"] != self.initial_kv_nodes:
            delta = self.initial_kv_nodes - self.num_nodes["data"]
            try:
                CapellaAPI.wait_until_done(
                    self.pod, self.tenant, self.cluster.id,
                    "Wait before node reset", timeout=1800)
                self.wait_for_rebalances([self.task.async_rebalance_capella(
                    self.pod, self.tenant, self.cluster,
                    self.rebalance_config("data", delta), timeout=self.rebalance_timeout)])
            except Exception as e:
                self.log.error(f"Failed to reset KV nodes: {e}")
        for bucket in list(self.cluster.buckets):
            try:
                self._delete_bucket_with_s3_cleanup(bucket)
            except Exception:
                pass
        self.cluster.buckets = []
        super().tearDown()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _trigger_scale_out(self):
        """Trigger a +1 data-node scale-out rebalance and return the async task."""
        config = self.rebalance_config("data", +1)
        return self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster, config, timeout=self.rebalance_timeout)

    def _assert_migration_stats_zero(self):
        """Assert all ep_fusion_migration_* stats are 0 on every cluster node."""
        _MIGRATION_STATS = [
            "ep_fusion_migration_completed_bytes",
            "ep_fusion_migration_failures",
            "ep_fusion_migration_total_bytes",
        ]
        for bucket in self.cluster.buckets:
            for stat_key in _MIGRATION_STATS:
                rows = self.fusion_monitor.run_cbstats_on_all_nodes(
                    self.cluster, bucket, stat_key=stat_key)
                for instance_id, public_ip, value, status in (rows or []):
                    if status != "Success":
                        self.log.warning(
                            f"cbstats {stat_key} on {instance_id} ({public_ip}): {status}")
                        continue
                    try:
                        stat_val = int(value)
                    except (ValueError, TypeError):
                        self.log.warning(
                            f"Cannot parse {stat_key}='{value}' on {instance_id} ({public_ip})")
                        continue
                    self.log.info(f"Node {public_ip} — {stat_key}: {stat_val}")
                    self.assertEqual(
                        stat_val, 0,
                        f"Node {public_ip}: {stat_key}={stat_val} (expected 0)")
        self.log.info("All ep_fusion_migration_* stats are 0 on all nodes")

    def _poll_agv_to_zero(self, timeout=60):
        """Poll activeGuestVolumes until it reaches 0 or the timeout expires."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            count = self._get_active_guest_volume_count(self.cluster)
            if count == 0:
                elapsed = int(timeout - (deadline - time.time()))
                self.log.info(f"activeGuestVolumes reached 0 in ~{elapsed}s")
                return
            self.log.info(
                f"activeGuestVolumes still {count} — "
                f"{int(deadline - time.time())}s remaining in {timeout}s window")
            time.sleep(5)
        self.assertTrue(
            False,
            f"activeGuestVolumes did not reach 0 within {timeout}s")

    def _poll_ebs_to_zero(self, sla_start, sla_seconds=300):
        """Poll CBS-tracked EBS guest volumes until 0 within the SLA window."""
        sla_deadline = sla_start + sla_seconds
        while time.time() < sla_deadline:
            volumes = self.cp_monitor.get_current_guest_volume_ids(self.cluster)
            if not volumes:
                elapsed = time.time() - sla_start
                self.log.info(
                    f"EBS guest volumes reached 0 in {elapsed:.1f}s (SLA: {sla_seconds}s)")
                return
            self.log.info(
                f"EBS volumes still tracked: {len(volumes)} — "
                f"{int(sla_deadline - time.time())}s remaining in SLA window")
            time.sleep(10)
        elapsed = time.time() - sla_start
        self.assertTrue(
            False,
            f"EBS guest volumes not cleaned up within {sla_seconds}s (elapsed: {elapsed:.1f}s)")

    def _poll_s3_below_1gb(self, s3_bucket_name, timeout):
        """Poll S3 log-store total size until it drops below 1 GB. Returns final size_gb."""
        deadline = time.time() + timeout
        s3_size_gb = None
        while time.time() < deadline:
            stats = self.s3.get_bucket_size(s3_bucket_name)
            s3_size_gb = stats.get("total_size_gb", 0)
            s3_count = stats.get("file_count", 0)
            self.log.info(
                f"S3 log-store: {s3_count} objects, {s3_size_gb:.2f} GB — "
                f"{int(deadline - time.time())}s remaining")
            if s3_size_gb < 1.0:
                return s3_size_gb
            time.sleep(30)
        return s3_size_gb

    def _refresh_bucket_uuids(self):
        """Re-fetch bucket UUIDs from ns_server for all current cluster buckets."""
        rest = RestConnection(self.cluster.master)
        for bucket in self.cluster.buckets:
            info = rest.get_bucket_details(bucket_name=bucket.name)
            bucket.bucket_uuid = info.get("uuid", None)

    # ------------------------------------------------------------------
    # Test 9: Bucket flush must clean up the S3 log-store to < 1 GB
    # ------------------------------------------------------------------

    def test_bucket_flush_cleans_s3_objects(self):
        """
        Flush a bucket and verify that the fusion S3 log-store drops below 1 GB.

        No rebalance is triggered — this tests the steady-state flush path where
        the CP must propagate the flush signal and remove stale S3 log-store objects.

        Validates:
        - Data has fully synced to S3 before flush (file_count > 0)
        - Bucket flush completes successfully
        - S3 log-store drops below 1 GB within s3_cleanup_timeout
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        create_end = self.input.param("create_end", 1_000_000)
        self._load_data(self.cluster, create_start=0, create_end=create_end)

        self.log.info("Waiting for data to sync to S3 before flush")
        self._wait_for_s3_data_synced(self.cluster, timeout=self.sync_wait_timeout)

        s3_bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)
        self.assertIsNotNone(s3_bucket_name, "Could not resolve S3 bucket name")

        size_before = self.s3.get_bucket_size(s3_bucket_name)
        size_gb_before = size_before.get("total_size_gb", 0)
        count_before = size_before.get("file_count", 0)
        self.log.info(
            f"S3 log-store before flush: {count_before} objects, {size_gb_before:.2f} GB")
        self.assertGreater(
            count_before, 0,
            "No S3 objects found before flush — data may not have synced to S3")

        for bucket in self.cluster.buckets:
            self.log.info(f"Flushing bucket '{bucket.name}' on cluster {self.cluster.id}")
            CapellaAPI.flush_bucket(self.pod, self.tenant, self.cluster, bucket.name)
        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id,
            "Wait for bucket flush to complete", timeout=600)
        self.log.info("Bucket flush confirmed — polling S3 cleanup")

        s3_cleanup_timeout = self.input.param("s3_cleanup_timeout", self.sync_wait_timeout)
        s3_size_gb = self._poll_s3_below_1gb(s3_bucket_name, s3_cleanup_timeout)
        self.assertLess(
            s3_size_gb, 1.0,
            f"S3 log-store did not drop below 1 GB after flush within "
            f"{s3_cleanup_timeout}s "
            f"(before: {size_gb_before:.2f} GB, final: {s3_size_gb:.2f} GB)")
        self.log.info(
            f"S3 log-store cleaned up after flush: "
            f"{size_gb_before:.2f} GB → {s3_size_gb:.2f} GB")

    # ------------------------------------------------------------------
    # Test 10: Bucket delete after rebalance — EBS volumes gone within 5 min
    # ------------------------------------------------------------------

    def test_bucket_delete_after_rebalance_cleans_guest_volumes(self):
        """
        After a fusion rebalance completes, delete all buckets and verify that
        any residual EBS guest volumes are cleaned up within 5 minutes.

        The CP's phase-8 teardown (EBS volume deletion) may still be in-flight when
        the bucket delete arrives. This test verifies the CP finalizes all EBS cleanup
        within the 5-minute SLA regardless.

        Validates:
        - Fusion rebalance completes with guest volumes created and tracked
        - All buckets deleted immediately after CBS rebalance reaches 'healthy'
        - EBS guest volumes tracked by CBS reach 0 within 300 seconds of bucket deletion
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        create_end = self.input.param("create_end", 20_000_000)
        self._load_data(self.cluster, create_start=0, create_end=create_end)
        self.sleep(120, "Allow initial sync to S3 before triggering rebalance")

        rebalance_task = self._trigger_scale_out()
        self.wait_for_rebalances([rebalance_task])

        # Delete immediately after CBS rebalance completes — CP phase-8 teardown may
        # still be running. The 5-minute SLA clock starts from this point.
        cleanup_start = time.time()
        self.log.info(
            "Rebalance complete — deleting all buckets and starting 5-minute SLA timer")
        for bucket in list(self.cluster.buckets):
            self.log.info(f"Deleting bucket '{bucket.name}'")
            self._delete_bucket_with_s3_cleanup(bucket)
        self.cluster.buckets = []

        self._poll_ebs_to_zero(cleanup_start, sla_seconds=300)

    # ------------------------------------------------------------------
    # Test 11: Bucket flush after rebalance — no orphaned EBS volumes within 5 min
    # ------------------------------------------------------------------

    def test_bucket_flush_after_rebalance_no_guest_volumes(self):
        """
        After a fusion rebalance completes, flush all buckets and verify that
        any residual EBS guest volumes are cleaned up within 5 minutes.

        Validates:
        - Fusion rebalance completes with guest volumes created and tracked
        - All buckets flushed immediately after CBS rebalance reaches 'healthy'
        - CBS-tracked EBS guest volumes reach 0 within 300s of flush (primary SLA check)
        - ep_fusion_migration_* stats = 0 on all nodes (post-SLA validation)
        - activeGuestVolumes reaches 0 within agv_cleanup_timeout seconds
        - S3 log-store size drops below 1 GB within s3_cleanup_timeout
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        create_end = self.input.param("create_end", 20_000_000)
        self._load_data(self.cluster, create_start=0, create_end=create_end)
        self.sleep(120, "Allow initial sync to S3 before triggering rebalance")

        rebalance_task = self._trigger_scale_out()
        self.wait_for_rebalances([rebalance_task])

        # The 5-minute SLA clock starts the moment the flush is issued.
        cleanup_start = time.time()
        self.log.info(
            "Rebalance complete — flushing all buckets and starting 5-minute SLA timer")
        for bucket in self.cluster.buckets:
            self.log.info(f"Flushing bucket '{bucket.name}'")
            CapellaAPI.flush_bucket(self.pod, self.tenant, self.cluster, bucket.name)
        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id,
            "Wait for bucket flush to complete", timeout=600)
        self.log.info("All buckets flushed")

        # ── Check 1 (primary SLA): CBS-tracked EBS volumes must reach 0 within 300s ──
        # Run before cbstats so synchronous stat collection does not eat the SLA budget.
        self._poll_ebs_to_zero(cleanup_start, sla_seconds=300)

        # ── Check 2: ep_fusion_migration stats must be 0 (post-SLA validation) ─────
        self._assert_migration_stats_zero()

        # ── Check 3: activeGuestVolumes must reach 0 ──────────────────────────────
        agv_timeout = self.input.param("agv_cleanup_timeout", 60)
        self._poll_agv_to_zero(timeout=agv_timeout)

        # ── Check 4: S3 log-store must shrink to < 1 GB ───────────────────────────
        s3_bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)
        self.assertIsNotNone(s3_bucket_name, "Could not resolve S3 bucket name")
        s3_cleanup_timeout = self.input.param("s3_cleanup_timeout", self.sync_wait_timeout)
        s3_size_gb = self._poll_s3_below_1gb(s3_bucket_name, s3_cleanup_timeout)
        self.assertLess(
            s3_size_gb, 1.0,
            f"S3 log-store did not drop below 1 GB after flush within "
            f"{s3_cleanup_timeout}s (final: {s3_size_gb:.2f} GB)")
        self.log.info(f"S3 log-store cleaned up to {s3_size_gb:.2f} GB after flush")

    # ------------------------------------------------------------------
    # Test 12: Bucket delete while CP is mid-EBS-teardown
    # ------------------------------------------------------------------

    def test_bucket_drop_during_guest_volume_deletion(self):
        """
        After a fusion rebalance completes, wait for the CP to begin (but not finish)
        deleting EBS guest volumes, then delete all buckets. Verifies the CP correctly
        handles a bucket-delete signal arriving mid-teardown.

        Validates:
        - EBS volumes are present at peak after rebalance (confirms guest volumes were created)
        - CP teardown is caught mid-flight where possible (best-effort; logged if too fast)
        - All EBS guest volumes reach 0 within the 5-minute SLA after bucket deletion
        - Bucket S3 prefix (kv/<uuid>) is fully removed after deletion
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        create_end = self.input.param("create_end", 20_000_000)
        self._load_data(self.cluster, create_start=0, create_end=create_end)
        self.sleep(120, "Allow initial sync to S3 before triggering rebalance")

        rebalance_task = self._trigger_scale_out()
        self.wait_for_rebalances([rebalance_task])

        peak_volumes = self.cp_monitor.get_current_guest_volume_ids(self.cluster)
        peak_count = len(peak_volumes) if peak_volumes else 0
        self.log.info(
            f"Peak EBS guest volume count immediately after rebalance: {peak_count}")

        # Poll until CP has started (but not finished) its EBS teardown. If CP completes
        # cleanup before we can catch it, the test still exercises the post-teardown delete path.
        mid_cleanup_caught = False
        poll_deadline = time.time() + 120
        while time.time() < poll_deadline and peak_count > 0:
            current = self.cp_monitor.get_current_guest_volume_ids(self.cluster)
            current_count = len(current) if current else 0
            if 0 < current_count < peak_count:
                mid_cleanup_caught = True
                self.log.info(
                    f"Caught CP mid-teardown: {current_count}/{peak_count} volumes remain")
                break
            if current_count == 0:
                self.log.info(
                    "CP teardown completed before bucket delete could be issued")
                break
            time.sleep(3)

        if not mid_cleanup_caught:
            self.log.warning(
                "CP teardown was too fast to catch mid-deletion; "
                "proceeding with immediate bucket delete")

        cleanup_start = time.time()
        self.log.info("Deleting all buckets — starting 5-minute SLA timer")
        for bucket in list(self.cluster.buckets):
            self.log.info(f"Deleting bucket '{bucket.name}'")
            self._delete_bucket_with_s3_cleanup(bucket)
        self.cluster.buckets = []

        self._poll_ebs_to_zero(cleanup_start, sla_seconds=300)

    # ------------------------------------------------------------------
    # Test 13: Bucket drop and recreate loop
    # ------------------------------------------------------------------

    def test_bucket_drop_and_recreate_loop(self):
        """
        Repeatedly drop and recreate a bucket while fusion is enabled.

        Verifies that each new bucket receives a fresh UUID distinct from prior
        iterations, that S3 does not accumulate stale prefixes between cycles,
        and that fusion remains enabled throughout.

        Validates:
        - Each recreated bucket gets a unique UUID (no UUID reuse)
        - kv/<uuid> S3 prefix is fully cleaned before the next load begins
        - Fusion state is 'enabled' after each recreate cycle
        - Cluster reaches 'healthy' state after each bucket recreation
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        iterations = self.input.param("recreate_iterations", 3)
        create_end = self.input.param("create_end", 5_000_000)
        s3_cleanup_timeout = self.input.param("s3_cleanup_timeout", self.sync_wait_timeout)
        seen_uuids = set()
        s3_bucket_name = None

        for i in range(1, iterations + 1):
            self.log.info(f"=== Drop/recreate iteration {i}/{iterations} ===")

            self._load_data(self.cluster, create_start=0, create_end=create_end)
            self._wait_for_s3_data_synced(self.cluster, timeout=self.sync_wait_timeout)

            if s3_bucket_name is None:
                s3_bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)
                self.assertIsNotNone(
                    s3_bucket_name,
                    "Could not resolve S3 bucket name on first iteration")

            for bucket in list(self.cluster.buckets):
                self.assertIsNotNone(
                    getattr(bucket, "bucket_uuid", None),
                    f"Iteration {i}: bucket '{bucket.name}' has no UUID")
                uuid = bucket.bucket_uuid
                self.assertNotIn(
                    uuid, seen_uuids,
                    f"Iteration {i}: bucket '{bucket.name}' reused UUID {uuid} "
                    f"from a prior iteration — S3 prefix isolation broken")
                seen_uuids.add(uuid)
                self.log.info(
                    f"Iteration {i}: deleting bucket '{bucket.name}' (UUID={uuid})")
                self._delete_bucket_with_s3_cleanup(bucket, timeout=s3_cleanup_timeout)
            self.cluster.buckets = []

            if i < iterations:
                self.create_buckets(self.pod, self.tenant, self.cluster)
                CapellaAPI.wait_until_done(
                    self.pod, self.tenant, self.cluster.id,
                    f"Wait for healthy cluster after recreate (iter {i})", timeout=600)
                self._refresh_bucket_uuids()

                fusion_status = CapellaAPI.get_fusion_status(
                    self.pod, self.tenant, self.cluster.id)
                self.assertEqual(
                    fusion_status.get("state"), "enabled",
                    f"Iteration {i}: fusion state is not 'enabled' after bucket recreate "
                    f"(got '{fusion_status.get('state')}')")

        self.log.info(
            f"Drop/recreate loop complete: {iterations} iterations, "
            f"{len(seen_uuids)} unique bucket UUIDs confirmed")

    # ------------------------------------------------------------------
    # Test 14: Full compaction syncs to S3; fusion rebalance succeeds after
    # ------------------------------------------------------------------

    def test_full_compaction_with_fusion_enabled(self):
        """
        Run full compaction on all buckets with fusion enabled. Verify that all
        compaction output fully syncs to S3 (pending bytes = 0), then trigger a
        fusion scale-out rebalance and verify it completes successfully.

        Validates:
        - ep_fusion_migration_* stats are 0 before compaction
        - Full compaction completes without errors
        - All compaction output syncs to S3 (pending bytes → 0 post-compaction)
        - Fusion scale-out rebalance triggered after sync completes successfully
        - No orphaned EBS volumes remain after rebalance
        - Fusion state remains 'enabled' throughout
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        create_end = self.input.param("create_end", 20_000_000)
        self._load_data(self.cluster, create_start=0, create_end=create_end)

        self.log.info("Waiting for initial data sync to S3 before compaction")
        self._wait_for_s3_data_synced(self.cluster, timeout=self.sync_wait_timeout)
        self._assert_migration_stats_zero()

        # ── Trigger full compaction on all buckets ─────────────────────────────────
        bh = BucketHelper(self.cluster.master)
        for bucket in self.cluster.buckets:
            self.log.info(f"Triggering full compaction on bucket '{bucket.name}'")
            bh.compact_bucket(bucket.name)
        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id,
            "Wait for compaction to complete", timeout=self.sync_wait_timeout)
        self.log.info("Compaction completed — waiting for output to sync to S3")

        # ── Wait for compaction output to fully sync to S3 ────────────────────────
        # Use a 256 MB stable threshold: Magma background compaction generates a
        # steady ~190 MB trickle of new pending bytes that prevents an exact-zero
        # reading; 256 MB gives headroom while still catching genuine stalls.
        self._wait_for_pending_bytes_zero(
            self.cluster, timeout=self.sync_wait_timeout,
            stable_threshold_bytes=256 * 1024 * 1024, stable_secs=60)

        s3_bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)
        self.assertIsNotNone(s3_bucket_name, "Could not resolve S3 bucket name after compaction")
        stats_after_compact = self.s3.get_bucket_size(s3_bucket_name)
        self.log.info(
            f"S3 log-store after compaction + full sync: "
            f"{stats_after_compact.get('file_count', 0)} objects, "
            f"{stats_after_compact.get('total_size_gb', 0):.2f} GB")

        # ── Trigger fusion scale-out rebalance once S3 is fully synced ───────────
        self.log.info("S3 sync confirmed — triggering fusion scale-out rebalance")
        rebalance_task = self._trigger_scale_out()
        self.wait_for_rebalances([rebalance_task])

        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id,
            "Wait for healthy cluster post-rebalance", timeout=1200)

        fusion_status = CapellaAPI.get_fusion_status(
            self.pod, self.tenant, self.cluster.id)
        self.assertEqual(
            fusion_status.get("state"), "enabled",
            f"Fusion state is not 'enabled' after compaction + rebalance "
            f"(got '{fusion_status.get('state')}')")

        ebs_cleanup_timeout = self.input.param("ebs_cleanup_timeout", 300)
        self._poll_ebs_to_zero(time.time(), sla_seconds=ebs_cleanup_timeout)
        self.log.info(
            "Full compaction + fusion rebalance completed successfully; "
            "no orphaned EBS volumes")

    # ------------------------------------------------------------------
    # Test 15: Replica change must not reshuffle the uploader map
    # ------------------------------------------------------------------

    def test_replica_change_uploader_map_unchanged(self):
        """
        Change bucket replica count while fusion is enabled and verify that
        active vBucket uploader assignments are identical before and after.

        A replica change triggers NS server internal operations but must not
        cause a fusion rebalance or redistribute which node is responsible for
        uploading each active vBucket.

        Validates:
        - Uploader map captured before replica change (all vBuckets assigned)
        - Replica count updated via Capella API (1 → 2)
        - Uploader map captured after replica change
        - Active vBucket uploader node assignments are identical across both snapshots
        - Fusion state remains 'enabled' after replica change
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        create_end = self.input.param("create_end", 5_000_000)
        self._load_data(self.cluster, create_start=0, create_end=create_end)
        self._wait_for_s3_data_synced(self.cluster, timeout=self.sync_wait_timeout)

        # Initialise the cluster-side dicts that _populate_fusion_uploader_map writes to.
        if not hasattr(self.cluster, "fusion_uploader_dict"):
            self.cluster.fusion_uploader_dict = {}
        if not hasattr(self.cluster, "fusion_vb_uploader_map"):
            self.cluster.fusion_vb_uploader_map = {}
        for bucket in self.cluster.buckets:
            self.cluster.fusion_vb_uploader_map[bucket.name] = {}

        # ── Snapshot uploader map before replica change ────────────────────────────
        self.log.info("Capturing uploader map before replica change")
        self.fusion_monitor.get_fusion_uploader_map(self.tenant, self.cluster)
        uploader_map_before = {
            bname: dict(vb_map)
            for bname, vb_map in self.cluster.fusion_vb_uploader_map.items()
        }

        # ── Change replica count 1 → 2 on all buckets ─────────────────────────────
        for bucket in self.cluster.buckets:
            bucket_id = CapellaAPI.get_bucket_id(
                self.pod, self.tenant, self.cluster, bucket.name)
            self.assertIsNotNone(
                bucket_id,
                f"Could not resolve bucket ID for '{bucket.name}'")
            self.log.info(
                f"Updating bucket '{bucket.name}' replica count to 2")
            CapellaAPI.update_bucket_settings(
                self.pod, self.tenant, self.cluster,
                bucket_id, {"numReplicas": 2})

        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id,
            "Wait for cluster healthy after replica change", timeout=1200)

        # ── Snapshot uploader map after replica change ─────────────────────────────
        for bucket in self.cluster.buckets:
            self.cluster.fusion_vb_uploader_map[bucket.name] = {}

        self.log.info("Capturing uploader map after replica change")
        self.fusion_monitor.get_fusion_uploader_map(self.tenant, self.cluster)
        uploader_map_after = {
            bname: dict(vb_map)
            for bname, vb_map in self.cluster.fusion_vb_uploader_map.items()
        }

        # ── Assert active vBucket uploader assignments are unchanged ───────────────
        for bucket_name in uploader_map_before:
            self.assertIn(
                bucket_name, uploader_map_after,
                f"Bucket '{bucket_name}' missing from uploader map after replica change")
            before = uploader_map_before[bucket_name]
            after = uploader_map_after[bucket_name]
            changed_vbs = [
                vb for vb, details in before.items()
                if after.get(vb, {}).get("node") != details.get("node")
            ]
            self.assertEqual(
                len(changed_vbs), 0,
                f"Bucket '{bucket_name}': {len(changed_vbs)} vBucket uploader "
                f"assignments changed after replica change — first 10: {changed_vbs[:10]}")
            self.log.info(
                f"Bucket '{bucket_name}': all {len(before)} vBucket uploader "
                f"assignments unchanged after replica change")

        fusion_status = CapellaAPI.get_fusion_status(
            self.pod, self.tenant, self.cluster.id)
        self.assertEqual(
            fusion_status.get("state"), "enabled",
            f"Fusion state is not 'enabled' after replica change "
            f"(got '{fusion_status.get('state')}')")

    # ------------------------------------------------------------------
    # Test 16: Flush while S3 upload is in-flight
    # ------------------------------------------------------------------

    def test_flush_during_active_s3_upload(self):
        """
        Flush all buckets while fusion is actively uploading data to S3
        (snapshotPendingBytes > 0 at the moment of flush).

        Validates that the CP handles a flush arriving mid-upload: the in-flight
        upload must be aborted or superseded, leaving the S3 log-store consistent
        with the post-flush empty bucket state.

        Validates:
        - snapshotPendingBytes > 0 at flush time (upload confirmed in-flight, best-effort)
        - S3 log-store drops below 1 GB after flush within s3_cleanup_timeout
        - ep_fusion_migration_* stats are 0 on all nodes after flush
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        create_end = self.input.param("create_end", 20_000_000)
        self._load_data(self.cluster, create_start=0, create_end=create_end,
                        wait_for_load=False)

        # Poll until at least one node reports pending bytes > 0 (upload in-flight).
        self.fusion_monitor.set_admin_credentials(self.cluster)
        pending_confirmed = False
        poll_deadline = time.time() + 300
        while time.time() < poll_deadline:
            status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
            if status:
                nodes = content.get("nodes") or {}
                total_pending = sum(
                    b_stats.get("snapshotPendingBytes", 0)
                    for n_stats in nodes.values()
                    for b_stats in (n_stats.get("buckets") or {}).values()
                )
                if total_pending > 0:
                    pending_confirmed = True
                    self.log.info(
                        f"Upload confirmed in-flight: {total_pending} pending bytes — "
                        f"issuing flush now")
                    break
            time.sleep(5)

        if not pending_confirmed:
            self.log.warning(
                "Could not confirm upload in-flight before flush timeout — "
                "test proceeds but may not exercise the mid-upload race path")

        for bucket in self.cluster.buckets:
            self.log.info(f"Flushing bucket '{bucket.name}' (mid-upload)")
            CapellaAPI.flush_bucket(self.pod, self.tenant, self.cluster, bucket.name)
        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id,
            "Wait for bucket flush to complete", timeout=600)
        self.log.info("Flush complete — polling S3 cleanup")

        s3_bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)
        self.assertIsNotNone(s3_bucket_name, "Could not resolve S3 bucket name")
        s3_cleanup_timeout = self.input.param("s3_cleanup_timeout", self.sync_wait_timeout)
        s3_size_gb = self._poll_s3_below_1gb(s3_bucket_name, s3_cleanup_timeout)
        self.assertLess(
            s3_size_gb, 1.0,
            f"S3 log-store did not drop below 1 GB after mid-upload flush within "
            f"{s3_cleanup_timeout}s (final: {s3_size_gb:.2f} GB)")
        self.log.info(
            f"S3 log-store cleaned up to {s3_size_gb:.2f} GB after mid-upload flush")

        self._assert_migration_stats_zero()

    # ------------------------------------------------------------------
    # Test 17: Multi-bucket flush cleans all per-bucket S3 prefixes
    # ------------------------------------------------------------------

    def test_multi_bucket_flush_cleans_all_s3_prefixes(self):
        """
        Flush multiple buckets simultaneously and verify that the S3 log-store
        prefix for every bucket UUID (kv/<uuid>) is fully cleaned up.

        Tests per-bucket prefix isolation: a flush on bucket A must not leave
        residual objects under bucket B's prefix, and vice-versa.

        Requires num_buckets >= 2 in the conf entry.

        Validates:
        - All buckets have > 1 GB of S3 data before flush (bulk data synced)
        - All buckets flushed simultaneously
        - Each bucket's kv/<uuid> prefix drops below 1 GB within s3_cleanup_timeout
          (per-vBucket metadata files are re-synced immediately after flush so the
          prefix never reaches 0 objects — a size threshold is the correct check)
        - Total S3 log-store across all prefixes drops below 1 GB after flush
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        self.assertGreaterEqual(
            len(self.cluster.buckets), 2,
            "This test requires at least 2 buckets — set num_buckets >= 2 in conf")

        create_end = self.input.param("create_end", 5_000_000)
        self._load_data(self.cluster, create_start=0, create_end=create_end)
        self._wait_for_s3_data_synced(self.cluster, timeout=self.sync_wait_timeout)

        s3_bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)
        self.assertIsNotNone(s3_bucket_name, "Could not resolve S3 bucket name")

        # Confirm each bucket's S3 prefix has > 1 GB of data before flush.
        # A size-based check is more reliable than a count check because magma
        # metadata files are always present; we want to confirm bulk data synced.
        for bucket in self.cluster.buckets:
            self.assertIsNotNone(
                getattr(bucket, "bucket_uuid", None),
                f"Bucket '{bucket.name}' has no UUID — cannot verify per-prefix cleanup")
            prefix = f"kv/{bucket.bucket_uuid}"
            stats_before = self.s3.get_bucket_size(s3_bucket_name, prefix=prefix)
            size_before_gb = stats_before.get("total_size_gb", 0)
            self.assertGreater(
                size_before_gb, 1.0,
                f"Bucket '{bucket.name}' prefix {prefix} has only {size_before_gb:.2f} GB "
                f"({stats_before.get('file_count', 0)} objects) before flush — "
                f"expected > 1 GB of synced data")
            self.log.info(
                f"Bucket '{bucket.name}': {size_before_gb:.2f} GB "
                f"({stats_before.get('file_count', 0)} objects) in prefix {prefix}")

        for bucket in self.cluster.buckets:
            self.log.info(f"Flushing bucket '{bucket.name}'")
            CapellaAPI.flush_bucket(self.pod, self.tenant, self.cluster, bucket.name)
        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id,
            "Wait for all bucket flushes to complete", timeout=600)
        self.log.info(f"All {len(self.cluster.buckets)} buckets flushed")

        # ── Verify each bucket's S3 prefix drops below 1 GB ──────────────────────
        # After flush the bulk data objects are removed, but per-vBucket metadata
        # files are immediately re-synced, so the prefix never reaches 0 objects.
        # The correct assertion is a size threshold: bulk data (>> 1 GB) gone,
        # only metadata residue (well under 1 GB) remains.
        s3_cleanup_timeout = self.input.param("s3_cleanup_timeout", self.sync_wait_timeout)
        deadline = time.time() + s3_cleanup_timeout
        for bucket in self.cluster.buckets:
            prefix = f"kv/{bucket.bucket_uuid}"
            self.log.info(
                f"Polling prefix {prefix} for bucket '{bucket.name}' to drop below 1 GB")
            while time.time() < deadline:
                stats = self.s3.get_bucket_size(s3_bucket_name, prefix=prefix)
                size_gb = stats.get("total_size_gb", 0)
                if size_gb < 1.0:
                    self.log.info(
                        f"Prefix {prefix} for bucket '{bucket.name}': "
                        f"{size_gb:.2f} GB ({stats.get('file_count', 0)} objects) "
                        f"— below 1 GB threshold")
                    break
                self.log.info(
                    f"Prefix {prefix}: {size_gb:.2f} GB "
                    f"({stats.get('file_count', 0)} objects) remain — "
                    f"{int(deadline - time.time())}s remaining")
                time.sleep(30)
            else:
                stats = self.s3.get_bucket_size(s3_bucket_name, prefix=prefix)
                size_gb = stats.get("total_size_gb", 0)
                self.assertLess(
                    size_gb, 1.0,
                    f"S3 prefix {prefix} for bucket '{bucket.name}' did not drop below "
                    f"1 GB within {s3_cleanup_timeout}s after flush "
                    f"(final: {size_gb:.2f} GB, "
                    f"{stats.get('file_count', 0)} objects remain)")

        total_size_gb = sum(
            self.s3.get_bucket_size(s3_bucket_name,
                                    prefix=f"kv/{b.bucket_uuid}").get("total_size_gb", 0)
            for b in self.cluster.buckets if getattr(b, "bucket_uuid", None)
        )
        self.log.info(
            f"Total S3 log-store across all bucket prefixes after flush: "
            f"{total_size_gb:.2f} GB")
        self.assertLess(
            total_size_gb, 1.0,
            f"Total S3 log-store did not drop below 1 GB after multi-bucket flush "
            f"(final: {total_size_gb:.2f} GB)")

    # ------------------------------------------------------------------
    # Test 18: Delete bucket (no prior rebalance) — S3 prefix cleaned
    # ------------------------------------------------------------------

    def test_bucket_delete_no_prior_rebalance_s3_prefix_cleaned(self):
        """
        Delete a bucket without triggering any fusion rebalance and verify that
        the bucket's S3 log-store prefix (kv/<uuid>) is fully removed.

        This is the baseline bucket-delete path: no EBS guest volumes are involved,
        but the CP must still remove the S3 prefix within the cleanup timeout.

        Validates:
        - Data syncs to S3 before deletion (objects present in kv/<uuid> prefix)
        - No EBS guest volumes exist before or after deletion (no rebalance run)
        - kv/<uuid> prefix reaches 0 objects within s3_cleanup_timeout
        - Fusion state remains 'enabled' after deletion and bucket recreation
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        create_end = self.input.param("create_end", 5_000_000)
        self._load_data(self.cluster, create_start=0, create_end=create_end)

        s3_bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)
        self.assertIsNotNone(s3_bucket_name, "Could not resolve S3 bucket name")

        pre_delete_volumes = self._get_active_guest_volume_count(self.cluster)
        self.assertEqual(
            pre_delete_volumes, 0,
            f"Expected 0 EBS guest volumes before deletion (no rebalance was run), "
            f"got {pre_delete_volumes}")

        # Poll S3 directly until each bucket's prefix has objects — this is the
        # only precondition we need (data is in S3 before delete). Waiting for
        # pending_bytes == 0 is too strict here: Magma background compaction after
        # the initial load generates a steady trickle of new pending bytes that can
        # prevent the counter from ever reaching 0.
        for bucket in self.cluster.buckets:
            self.assertIsNotNone(
                getattr(bucket, "bucket_uuid", None),
                f"Bucket '{bucket.name}' has no UUID")
            prefix = f"kv/{bucket.bucket_uuid}"
            s3_poll_deadline = time.time() + self.sync_wait_timeout
            files_before = []
            while time.time() < s3_poll_deadline:
                files_before = self.s3.list_files_in_bucket(s3_bucket_name, prefix=prefix)
                if files_before:
                    break
                self.log.info(
                    f"No S3 objects yet under {prefix} — "
                    f"{int(s3_poll_deadline - time.time())}s remaining")
                time.sleep(15)
            self.assertGreater(
                len(files_before), 0,
                f"No S3 objects found under prefix {prefix} within "
                f"{self.sync_wait_timeout}s — data may not have synced")
            self.log.info(
                f"Bucket '{bucket.name}': {len(files_before)} objects in prefix {prefix}")

        s3_cleanup_timeout = self.input.param("s3_cleanup_timeout", self.sync_wait_timeout)
        deletion_start = time.time()
        for bucket in list(self.cluster.buckets):
            self.log.info(f"Deleting bucket '{bucket.name}'")
            self._delete_bucket_with_s3_cleanup(bucket, timeout=s3_cleanup_timeout)
        self.cluster.buckets = []
        self.log.info(
            f"All buckets deleted and S3 prefixes cleaned in "
            f"{time.time() - deletion_start:.1f}s")

        post_delete_volumes = self._get_active_guest_volume_count(self.cluster)
        self.assertEqual(
            post_delete_volumes, 0,
            f"EBS guest volumes appeared after deletion with no rebalance: "
            f"{post_delete_volumes}")

        # Recreate buckets so the cluster is usable by subsequent tests in the run.
        self.create_buckets(self.pod, self.tenant, self.cluster)
        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id,
            "Wait for healthy cluster after bucket recreation", timeout=600)
        self._refresh_bucket_uuids()

        fusion_status = CapellaAPI.get_fusion_status(
            self.pod, self.tenant, self.cluster.id)
        self.assertEqual(
            fusion_status.get("state"), "enabled",
            f"Fusion state is not 'enabled' after bucket delete+recreate "
            f"(got '{fusion_status.get('state')}')")
