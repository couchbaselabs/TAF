"""
Fusion Bucket Operation Tests

Validates bucket-level operations with fusion enabled to ensure they do not
corrupt or inadvertently delete the fusion S3 log-store (§5 of the E2E test plan).

Tests in this file:
  9.  test_bucket_flush_cleans_s3_objects                    — flush must reduce S3 log-store to < 1 GB
  10. test_bucket_delete_after_rebalance_cleans_guest_volumes — delete post-rebalance; EBS volumes gone within 5 min
  11. test_bucket_flush_after_rebalance_no_guest_volumes      — flush post-rebalance; no orphaned EBS volumes within 5 min
"""

import time

from capella_utils.dedicated import CapellaUtils as CapellaAPI
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
            except Exception:
                pass
        self.cluster.buckets = []
        self.create_buckets(self.pod, self.tenant, self.cluster)
        if self.cluster.buckets:
            bucket = self.cluster.buckets[0]
            rest = RestConnection(self.cluster.master)
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
                    self.rebalance_config("data", delta), timeout=self.index_timeout)])
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
            self.pod, self.tenant, self.cluster, config, timeout=self.index_timeout)

    # ------------------------------------------------------------------
    # Test 9: Bucket flush must clean up the S3 log-store to < 1 GB
    # ------------------------------------------------------------------

    def test_bucket_flush_cleans_s3_objects(self):
        """
        Flush a bucket and verify that the fusion S3 log-store is cleaned up
        to less than 1 GB.

        After a bucket flush all CBS key-value data is cleared. The CP should
        propagate this by removing the now-stale S3 log-store objects so that
        the log-store does not accumulate orphaned data from the flushed bucket.

        Validates:
        - Fusion is enabled and data has fully synced to S3 (file_count > 0)
        - S3 log-store size and object count are recorded before flush
        - Bucket flush completes successfully (Capella API accepts request)
        - S3 log-store size drops below 1 GB within sync_wait_timeout
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        create_end = self.input.param("create_end", 1_000_000)
        self._load_data(self.cluster, create_start=0, create_end=create_end)

        sync_timeout = self.input.param("sync_wait_timeout", 600)
        self.log.info("Waiting for data to sync to S3 before flush")
        self._wait_for_pending_bytes_zero(self.cluster, timeout=sync_timeout)

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
            self.log.info(
                f"Flushing bucket '{bucket.name}' on cluster {self.cluster.id}")
            CapellaAPI.flush_bucket(self.pod, self.tenant, self.cluster, bucket.name)
        self.log.info("Bucket flush complete")

        # ── Check 1: ep_fusion_migration stats must be 0 on every node ──────────
        # After a flush there is no data to migrate from EBS guest volumes to local
        # CBS storage. All three counters must be zero.
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
                            f"cbstats {stat_key} on {instance_id} "
                            f"({public_ip}): {status}")
                        continue
                    try:
                        stat_val = int(value)
                    except (ValueError, TypeError):
                        self.log.warning(
                            f"Cannot parse {stat_key}='{value}' on "
                            f"{instance_id} ({public_ip})")
                        continue
                    self.log.info(f"Node {public_ip} — {stat_key}: {stat_val}")
                    self.assertEqual(
                        stat_val, 0,
                        f"Node {public_ip}: {stat_key}={stat_val} after flush "
                        f"(expected 0)")
        self.log.info(
            "All ep_fusion_migration_* stats are 0 on all nodes after flush")

        # ── Check 2: activeGuestVolumes must reach 0 within 60 seconds ──────────
        self.log.info(
            "Polling for activeGuestVolumes to reach 0 within 60s after flush")
        agv_deadline = time.time() + 60
        agv_clean = False
        while time.time() < agv_deadline:
            count = self._get_active_guest_volume_count(self.cluster)
            if count == 0:
                agv_clean = True
                elapsed = int(60 - (agv_deadline - time.time()))
                self.log.info(
                    f"activeGuestVolumes reached 0 in ~{elapsed}s after flush")
                break
            self.log.info(
                f"activeGuestVolumes still {count} — "
                f"{int(agv_deadline - time.time())}s remaining in 60s window")
            time.sleep(5)
        self.assertTrue(
            agv_clean,
            "activeGuestVolumes did not reach 0 within 60 seconds of bucket flush")

        # ── Check 3: S3 log-store must shrink to < 1 GB ──────────────────────────
        self.log.info("Waiting for CP to clean up S3 log-store to < 1 GB")

        cleanup_timeout = self.input.param("sync_wait_timeout", 600)
        deadline = time.time() + cleanup_timeout
        s3_size_gb = size_gb_before
        while time.time() < deadline:
            stats = self.s3.get_bucket_size(s3_bucket_name)
            s3_size_gb = stats.get("total_size_gb", 0)
            s3_count = stats.get("file_count", 0)
            self.log.info(
                f"S3 log-store: {s3_count} objects, {s3_size_gb:.2f} GB — "
                f"{int(deadline - time.time())}s remaining")
            if s3_size_gb < 1.0:
                break
            time.sleep(30)

        self.assertLess(
            s3_size_gb, 1.0,
            f"S3 log-store did not drop below 1 GB after flush within {cleanup_timeout}s "
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
        - No orphaned EBS volumes remain after bucket deletion
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

        sla_deadline = cleanup_start + 300
        volumes_clean = False
        while time.time() < sla_deadline:
            volumes = self.cp_monitor.get_current_guest_volume_ids(self.cluster)
            if not volumes:
                volumes_clean = True
                elapsed = time.time() - cleanup_start
                self.log.info(
                    f"EBS guest volumes reached 0 in {elapsed:.1f}s after bucket "
                    f"deletion (SLA: 300s)")
                break
            self.log.info(
                f"EBS volumes still tracked by CBS: {len(volumes)} — "
                f"{int(sla_deadline - time.time())}s remaining in SLA window")
            time.sleep(10)

        elapsed = time.time() - cleanup_start
        self.assertTrue(
            volumes_clean,
            f"EBS guest volumes were not cleaned up within 300s of bucket deletion "
            f"(elapsed: {elapsed:.1f}s)")

    # ------------------------------------------------------------------
    # Test 11: Bucket flush after rebalance — no orphaned EBS volumes within 5 min
    # ------------------------------------------------------------------

    def test_bucket_flush_after_rebalance_no_guest_volumes(self):
        """
        After a fusion rebalance completes, flush all buckets and verify that
        any residual EBS guest volumes are cleaned up within 5 minutes.

        A bucket flush clears CBS key-value data but must not disrupt the CP's
        EBS guest volume lifecycle. This test verifies the CP finalizes cleanup
        within the 5-minute SLA even when a flush is issued right after the rebalance.

        Validates:
        - Fusion rebalance completes with guest volumes created and tracked
        - All buckets flushed immediately after CBS rebalance reaches 'healthy'
        - ep_fusion_migration_completed_bytes / failures / total_bytes = 0 on all nodes
        - activeGuestVolumes reaches 0 within 60 seconds of flush
        - CBS-tracked EBS guest volumes reach 0 within 300 seconds of flush
        - S3 log-store size drops below 1 GB within sync_wait_timeout
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        create_end = self.input.param("create_end", 20_000_000)
        self._load_data(self.cluster, create_start=0, create_end=create_end)
        self.sleep(120, "Allow initial sync to S3 before triggering rebalance")

        rebalance_task = self._trigger_scale_out()
        self.wait_for_rebalances([rebalance_task])

        # Flush immediately after CBS rebalance completes — CP phase-8 teardown may
        # still be running. The 5-minute SLA clock starts from this point.
        cleanup_start = time.time()
        self.log.info(
            "Rebalance complete — flushing all buckets and starting 5-minute SLA timer")
        for bucket in self.cluster.buckets:
            self.log.info(f"Flushing bucket '{bucket.name}'")
            CapellaAPI.flush_bucket(self.pod, self.tenant, self.cluster, bucket.name)
        self.log.info("All buckets flushed")

        # ── Check 1: ep_fusion_migration stats must be 0 on every node ──────────
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
                            f"cbstats {stat_key} on {instance_id} "
                            f"({public_ip}): {status}")
                        continue
                    try:
                        stat_val = int(value)
                    except (ValueError, TypeError):
                        self.log.warning(
                            f"Cannot parse {stat_key}='{value}' on "
                            f"{instance_id} ({public_ip})")
                        continue
                    self.log.info(f"Node {public_ip} — {stat_key}: {stat_val}")
                    self.assertEqual(
                        stat_val, 0,
                        f"Node {public_ip}: {stat_key}={stat_val} after flush "
                        f"(expected 0)")
        self.log.info(
            "All ep_fusion_migration_* stats are 0 on all nodes after flush")

        # ── Check 2: activeGuestVolumes must reach 0 within 60 seconds ──────────
        self.log.info(
            "Polling for activeGuestVolumes to reach 0 within 60s after flush")
        agv_deadline = time.time() + 60
        agv_clean = False
        while time.time() < agv_deadline:
            count = self._get_active_guest_volume_count(self.cluster)
            if count == 0:
                agv_clean = True
                elapsed = int(60 - (agv_deadline - time.time()))
                self.log.info(
                    f"activeGuestVolumes reached 0 in ~{elapsed}s after flush")
                break
            self.log.info(
                f"activeGuestVolumes still {count} — "
                f"{int(agv_deadline - time.time())}s remaining in 60s window")
            time.sleep(5)
        self.assertTrue(
            agv_clean,
            "activeGuestVolumes did not reach 0 within 60 seconds of bucket flush")

        # ── Check 3: CBS-tracked guest volumes must reach 0 within 5-minute SLA ─
        sla_deadline = cleanup_start + 300
        volumes_clean = False
        while time.time() < sla_deadline:
            volumes = self.cp_monitor.get_current_guest_volume_ids(self.cluster)
            if not volumes:
                volumes_clean = True
                elapsed = time.time() - cleanup_start
                self.log.info(
                    f"EBS guest volumes reached 0 in {elapsed:.1f}s after bucket "
                    f"flush (SLA: 300s)")
                break
            self.log.info(
                f"EBS volumes still tracked by CBS: {len(volumes)} — "
                f"{int(sla_deadline - time.time())}s remaining in SLA window")
            time.sleep(10)

        elapsed = time.time() - cleanup_start
        self.assertTrue(
            volumes_clean,
            f"EBS guest volumes were not cleaned up within 300s of bucket flush "
            f"(elapsed: {elapsed:.1f}s)")

        # ── Check 4: S3 log-store must shrink to < 1 GB ──────────────────────────
        s3_bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)
        self.assertIsNotNone(s3_bucket_name, "Could not resolve S3 bucket name")
        self.log.info("Waiting for CP to clean up S3 log-store to < 1 GB")
        s3_cleanup_timeout = self.input.param("sync_wait_timeout", 3600)
        s3_deadline = time.time() + s3_cleanup_timeout
        s3_size_gb = None
        while time.time() < s3_deadline:
            stats = self.s3.get_bucket_size(s3_bucket_name)
            s3_size_gb = stats.get("total_size_gb", 0)
            s3_count = stats.get("file_count", 0)
            self.log.info(
                f"S3 log-store: {s3_count} objects, {s3_size_gb:.2f} GB — "
                f"{int(s3_deadline - time.time())}s remaining")
            if s3_size_gb < 1.0:
                break
            time.sleep(30)
        self.assertLess(
            s3_size_gb, 1.0,
            f"S3 log-store did not drop below 1 GB after flush within "
            f"{s3_cleanup_timeout}s (final: {s3_size_gb:.2f} GB)")
        self.log.info(f"S3 log-store cleaned up to {s3_size_gb:.2f} GB after flush")
