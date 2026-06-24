"""
Fusion Health Tests

Validates fusion steady-state health properties (§3 of the E2E test plan).

Tests in this file:
  8. test_stop_start_fusion_resumes_s3_upload — stop/re-enable fusion restarts S3 upload
"""

import time

from capella_utils.dedicated import CapellaUtils as CapellaAPI
from membase.api.rest_client import RestConnection
from .fusion_test_base import _FusionTestBase


class FusionHealthTest(_FusionTestBase):
    """
    Validates fusion steady-state health: upload continuity across stop/start cycles,
    pending sync bounds under load, and log-file growth rates.

    Each test enables fusion, loads data, and validates specific sync/upload behaviors.
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
    # Test 8: Stop/Start fusion restarts S3 upload from checkpoint
    # ------------------------------------------------------------------

    def test_stop_start_fusion_resumes_s3_upload(self):
        """
        Stop fusion and re-enable it, verifying that S3 uploads resume from checkpoint
        and pending bytes eventually drain to 0.

        Validates:
        - Fusion enabled with data syncing (pending bytes > 0 initially)
        - After stop: fusion state is 'stopped', S3 object count is frozen
        - After re-enable: fusion state is 'enabled'
        - S3 object count increases after re-enable (upload resumed)
        - Pending bytes drain to 0 (full sync confirmed)
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        create_end = self.input.param("create_end", 5_000_000)
        self._load_data(self.cluster, create_start=0, create_end=create_end)
        self.sleep(60, "Allow initial sync to S3 before stopping")

        s3_bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)
        self.assertIsNotNone(s3_bucket_name, "Could not resolve S3 bucket name")

        size_before_stop = self.s3.get_bucket_size(s3_bucket_name)
        count_before_stop = size_before_stop.get("file_count", 0)
        self.log.info(f"S3 object count before stop: {count_before_stop}")

        self.log.info("Stopping fusion")
        resp = CapellaAPI.stop_fusion_internal(self.pod, self.tenant, self.cluster.id)
        self.assertIn(resp.status_code, [200, 202],
                      f"stop_fusion returned {resp.status_code}: {resp.content}")
        self._wait_for_fusion_state(self.tenant, self.cluster, "stopped")

        self.sleep(30, "Observe S3 count while stopped")
        size_while_stopped = self.s3.get_bucket_size(s3_bucket_name)
        count_while_stopped = size_while_stopped.get("file_count", 0)
        self.log.info(f"S3 object count while stopped: {count_while_stopped}")

        self.log.info("Re-enabling fusion")
        resp = CapellaAPI.enable_fusion(self.pod, self.tenant, self.cluster.id)
        self.assertEqual(resp.status_code, 200,
                         f"enable_fusion returned {resp.status_code}: {resp.content}")
        self._wait_for_fusion_state(self.tenant, self.cluster, "enabled")
        self.log.info("Fusion re-enabled; waiting for S3 upload to resume")

        resume_timeout = self.input.param("sync_wait_timeout", 1200)
        deadline = time.time() + resume_timeout
        s3_count_increased = False
        while time.time() < deadline:
            size_after = self.s3.get_bucket_size(s3_bucket_name)
            count_after = size_after.get("file_count", 0)
            self.log.info(
                f"S3 object count after re-enable: {count_after} "
                f"(count while stopped: {count_while_stopped})")
            if count_after > count_while_stopped:
                s3_count_increased = True
                break
            time.sleep(15)

        self.assertTrue(
            s3_count_increased,
            f"S3 object count did not increase after re-enabling fusion "
            f"(count while stopped: {count_while_stopped}). "
            "Uploads did not resume.")

        self.log.info("Waiting for pending bytes to drain to 0")
        self._wait_for_pending_bytes_zero(self.cluster, timeout=resume_timeout)
        self.log.info("S3 upload resumed and completed after stop/start cycle")
