"""
Fusion Cluster Destroy Tests

Validates that destroying a Capella cluster during an active fusion rebalance
correctly cleans up all AWS resources the CP created for the fusion path:
  - EBS guest volumes (fusion accelerator tag)
  - Fusion accelerator EC2 instances
  - Auto Scaling Groups
  - Cluster CBS/KV node EC2 instances
  - Fusion S3 log-store bucket
  - IAM instance profile for accelerator nodes

Each test in this class deploys its own dedicated cluster (no shared cluster).
setUp calls BaseTestCase.setUp directly — capella["clusters"] is never set, so
ProvisionedBaseTestCase creates a fresh cluster per test and destroys any
leftover in tearDown. If a test destroys the cluster mid-rebalance,
CapellaAPI.destroy_cluster removes it from tenant.clusters and tearDown's
destroy loop finds an empty list (no double-destroy).

Tests in this file (§11 of the E2E test plan):
  1. test_destroy_after_prepare_rebalance         — destroy before accelerators appear (phases 2-3)
  2. test_destroy_during_s3_download              — destroy during S3→EBS hydration (phase 5)
  3. test_destroy_during_file_extent_migration    — destroy when EBS volumes are attached to KV nodes (phase 6)
  4. test_destroy_in_scale_failed_state           — delete S3 bucket mid-phase-5 to force scaleFailed, then destroy
  5. test_destroy_during_accelerator_provisioning — destroy while accelerators exist but no guest volume yet (phase 4)
  6. test_destroy_during_cbs_rebalance            — destroy once volumes are on KV nodes and accelerators are gone (phase 7)
  7. test_destroy_with_active_backup              — destroy with a completed EBS snapshot backup present
  8. test_destroy_rejected_while_restore_source    — destroy must be rejected while cluster is an active restore source
  9. test_destroy_while_turning_off               — destroy must be rejected/not-silently-succeed while turned off
"""

import threading
import time
import uuid

from aGoodDoctor.workloads import Hotel
from aGoodDoctor.hostedOnOff import DoctorHostedOnOff
from capella_utils.dedicated import CapellaUtils as CapellaAPI
from cluster_utils.cluster_ready_functions import CBCluster
from pytests.basetestcase import BaseTestCase
from aGoodDoctor.hostedOPD import hostedOPD
from bucket_utils.bucket_ready_functions import JavaDocLoaderUtils

from .fusion_aws_util import FusionAWSUtil, resolve_fusion_aws_credentials
from .fusion_monitor_util import FusionMonitorUtil
from .fusion_cp_resource_monitor import FusionCPResourceMonitor
from .awslib.s3_lib import S3Lib
from .fusion_enable_disable_test import _FusionTestBase


class FusionClusterDestroyTest(_FusionTestBase):
    """
    Tests for cluster destruction at various stages of a fusion rebalance.

    Inherited from _FusionTestBase for helper methods only (setUp/tearDown are
    fully overridden). Each test deploys an independent cluster so that the
    destroy-under-test does not affect other tests in the suite.
    """

    _FAILED_STATES = frozenset([
        "deployment_failed", "deploymentFailed", "redeploymentFailed",
        "rebalance_failed", "rebalanceFailed", "scaleFailed",
    ])

    def setUp(self):
        # Bypass _FusionTestBase's shared-cluster bookkeeping entirely.
        # capella["clusters"] is intentionally never set, so
        # ProvisionedBaseTestCase creates a fresh cluster for every test
        # and destroys any leftover cluster in tearDown.
        BaseTestCase.setUp(self)
        hostedOPD.__init__(self)

        self.aws_region = self.input.param("region", "us-east-1")
        self.aws_access_key, self.aws_secret_key, self.aws_session_token, self.aws_iam = \
            resolve_fusion_aws_credentials(self.input, region=self.aws_region)
        # Assumed-role credentials expire mid-test — a single auto-refreshing
        # boto3.Session (built once here) keeps every AWS client below working
        # for the life of the run instead of failing with ExpiredToken.
        self.aws_boto3_session = self.aws_iam.get_boto3_session(region=self.aws_region) \
            if self.aws_iam else None

        self.fusion_aws_util = FusionAWSUtil(
            self.aws_access_key, self.aws_secret_key,
            session_token=self.aws_session_token, region=self.aws_region,
            boto3_session=self.aws_boto3_session)
        self.fusion_monitor = FusionMonitorUtil(self.log, self.fusion_aws_util,
                                                num_vbuckets=self.input.param("numVBuckets", 128))
        self.cp_monitor = FusionCPResourceMonitor(self.log, self.fusion_aws_util)
        self.s3 = S3Lib(self.aws_access_key, self.aws_secret_key,
                         session_token=self.aws_session_token, region=self.aws_region,
                         boto3_session=self.aws_boto3_session)

        self.sync_wait_timeout = self.input.param("sync_wait_timeout", 1200)
        self.fusion_threshold_gib = self.input.param("fusion_threshold_gib", 10)
        self.num_buckets = self.input.param("num_buckets", 1)
        self.rebalance_timeout = self.input.param("rebalance_timeout", 7200)
        # How long to poll AWS for full post-destroy resource cleanup before
        # asserting failure. Kept configurable rather than a hardcoded 600s:
        # the CP's destroy teardown has no fixed SLA (aws/destroyer.go retries
        # until its own job context deadline, not a constant), and recent
        # teardown-ordering/retry changes on the CP side make a single fixed
        # number risky to bake in permanently.
        self.post_destroy_cleanup_timeout = self.input.param(
            "post_destroy_cleanup_timeout", 600)
        # How long to wait for a destroy attempt that is expected to be
        # rejected (restore-source / turning-off negative paths) before
        # concluding it did not complete.
        self.destroy_reject_wait_timeout = self.input.param(
            "destroy_reject_wait_timeout", 300)
        self.load_defn = [Hotel]

        JavaDocLoaderUtils(self.bucket_util, self.cluster_util)
        self.stop_run_event = threading.Event()

        self.tenant = self.tenants[0]
        self.cluster = self.tenant.clusters[0]
        for bucket in self.cluster.buckets:
            if not hasattr(bucket, "loadDefn") or bucket.loadDefn is None:
                bucket.loadDefn = self.load_defn[0]

        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self.create_buckets(self.pod, self.tenant, self.cluster)

    def tearDown(self):
        if hasattr(self, "stop_run_event"):
            self.stop_run_event.set()
        self.stop_run = True
        # ProvisionedBaseTestCase.tearDown destroys any cluster remaining in
        # tenant.clusters (since capella["clusters"] is not set). If the test
        # already called CapellaAPI.destroy_cluster, tenant.clusters is empty
        # and tearDown's loop is a no-op.
        BaseTestCase.tearDown(self)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _accelerator_filter(self):
        return self._accelerator_filter_for(self.cluster.id)

    def _accelerator_filter_for(self, cluster_id):
        return self.fusion_aws_util._cluster_filter(cluster_id)

    def _load_above_threshold(self):
        create_end = self.input.param("create_end", 20_000_000)
        self._load_data(self.cluster, create_start=0, create_end=create_end)
        self.sleep(120, "Allow initial sync to S3 before triggering rebalance")

    def _trigger_scale_out(self):
        config = self.rebalance_config("data", +1)
        return self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster, config, timeout=self.rebalance_timeout)

    def _s3_bucket_still_exists(self, bucket_name):
        try:
            self.s3.s3_client.head_bucket(Bucket=bucket_name)
            return True
        except Exception:
            return False

    def _capture_pre_destroy_resources(self, cluster):
        """Capture AWS resource identifiers before cluster destroy for post-destroy verification.

        Must be called while the cluster (and ideally accelerators) are still running.
        """
        cluster_id = cluster.id
        s3_bucket_name = self._get_s3_bucket_name_from_uri(cluster)

        iam_profile_name = None
        acc_instances = self.fusion_aws_util.list_accelerator_instances(
            self._accelerator_filter_for(cluster_id), log="CaptureIAM")
        if acc_instances:
            iam_profile_name = self.fusion_aws_util.ec2.get_instance_iam_profile_name(
                acc_instances[0]["InstanceId"])
            self.log.info(f"IAM instance profile captured for post-destroy check: {iam_profile_name}")

        return {
            "cluster_id": cluster_id,
            "s3_bucket_name": s3_bucket_name,
            "iam_profile_name": iam_profile_name,
        }

    def _destroy_cluster_async(self, cluster):
        """Start CapellaAPI.destroy_cluster in a background thread.

        Returns (thread, result_dict). result_dict["failed"] is set True if
        destroy raises, with the message in result_dict["error"].
        """
        result = {"failed": False, "error": None}

        def _do_destroy():
            try:
                CapellaAPI.destroy_cluster(self.pod, self.tenant, cluster)
            except Exception as exc:
                result["failed"] = True
                result["error"] = str(exc)

        thread = threading.Thread(
            target=_do_destroy, name=f"destroy-{cluster.id}", daemon=True)
        thread.start()
        return thread, result

    def _assert_all_cluster_resources_cleaned(self, resources, timeout=None):
        """Assert every AWS resource the CP created for this cluster is gone.

        Thin Layer 3 wrapper: all polling/monitoring logic lives in
        FusionCPResourceMonitor.monitor_full_cluster_teardown() (Layer 2),
        which returns a list of failure strings; this method only asserts
        on that list, per the "utilities return data, tests assert"
        convention (see agents/fusion.md / architecture.md).

        Intended to run concurrently with _destroy_cluster_async so that
        resource cleanup is observed while the destroy is in progress.
        """
        if timeout is None:
            timeout = self.post_destroy_cleanup_timeout
        cluster_id = resources["cluster_id"]
        failures = self.cp_monitor.monitor_full_cluster_teardown(
            cluster_id, resources, timeout=timeout)
        self.assertEqual(
            len(failures), 0,
            f"Resources not cleaned up after cluster {cluster_id} destroy:\n"
            + "\n".join(f"  - {f}" for f in failures))

    # ------------------------------------------------------------------
    # Test 1: Destroy before accelerators are deployed (phases 2-3)
    # ------------------------------------------------------------------

    def test_destroy_after_prepare_rebalance(self):
        """
        Trigger a fusion scale-out rebalance and destroy the cluster immediately after
        the CBS rebalance starts (cluster leaves 'healthy'), before any accelerator
        instances are launched (phases 2-3 — prepareRebalance / log file leasing).

        Validates that destroying mid-prepareRebalance leaves no orphaned AWS resources:
        - No EBS guest volumes
        - No accelerator EC2 instances or ASGs
        - No cluster CBS/KV nodes remaining in non-terminated state
        - Fusion S3 bucket deleted by the CP
        - IAM instance profile (if created before phase 4) deleted by the CP
        """
        self._load_above_threshold()

        # Capture S3 bucket name before triggering rebalance
        s3_bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)
        resources = {
            "cluster_id": self.cluster.id,
            "s3_bucket_name": s3_bucket_name,
            "iam_profile_name": None,  # No accelerators exist in phases 2-3
        }

        rebalance_task = self._trigger_scale_out()

        # Wait for rebalance to start (cluster leaves 'healthy'), then destroy
        # before accelerator instances are launched. This targets phases 2-3 of the
        # 8-phase lifecycle, when fusion log files are leased in prepareRebalance.
        prepare_deadline = time.time() + 120
        while time.time() < prepare_deadline:
            state = CapellaAPI.get_cluster_state(
                self.pod, self.tenant, self.cluster.id)
            if state != "healthy":
                self.log.info(
                    f"Rebalance started (state={state}) — triggering destroy "
                    f"in prepareRebalance window (no accelerators yet)")
                break
            time.sleep(5)

        # Warn if accelerators already appeared (destroy targets a later phase)
        acc_now = self.fusion_aws_util.list_accelerator_instances(
            self._accelerator_filter(), log="PreDestroyPhaseCheck")
        if acc_now:
            self.log.warning(
                f"{len(acc_now)} accelerator(s) already present — destroy will "
                f"target a later phase than intended (phase 4+)")

        self.log.info(f"Destroying cluster {self.cluster.id} mid-prepareRebalance")
        destroy_thread, destroy_result = self._destroy_cluster_async(self.cluster)
        self._assert_all_cluster_resources_cleaned(resources, timeout=self.post_destroy_cleanup_timeout)

        destroy_thread.join(timeout=1800)
        self.assertFalse(
            destroy_thread.is_alive(),
            "CapellaAPI.destroy_cluster did not complete within 1800s")
        self.assertFalse(
            destroy_result["failed"],
            f"Cluster destroy returned an error: {destroy_result['error']}")
        self.log.info(
            "Cluster destroyed mid-prepareRebalance — all AWS resources verified clean")

    # ------------------------------------------------------------------
    # Test 2: Destroy during S3→EBS download (phase 5)
    # ------------------------------------------------------------------

    def test_destroy_during_s3_download(self):
        """
        Destroy the cluster during phase 5 of the fusion rebalance, when accelerator
        instances are actively downloading S3 log files to EBS guest volumes.

        Phase 5 is detected by waiting for EBS guest volumes to appear via
        cp_monitor.get_current_guest_volume_ids().

        Validates full AWS resource cleanup:
        - EBS guest volumes (may still be attached to accelerators at destroy time)
        - Accelerator EC2 instances and ASGs
        - Cluster CBS/KV nodes
        - Fusion S3 bucket
        - IAM instance profile
        """
        self._load_above_threshold()
        rebalance_task = self._trigger_scale_out()
        self.sleep(30, "Wait for rebalance to start")

        # Wait for EBS guest volumes to appear — this confirms phase 5 has started
        volumes_deadline = time.time() + 1800
        volumes_seen = False
        while time.time() < volumes_deadline:
            if rebalance_task.state in self._FAILED_STATES:
                self.fail(
                    f"Rebalance failed before EBS volumes appeared: {rebalance_task.state}")
            volumes = self.cp_monitor.get_current_guest_volume_ids(self.cluster)
            if volumes:
                volumes_seen = True
                self.log.info(
                    f"Phase 5 confirmed ({len(volumes)} EBS guest volume(s)) — "
                    f"triggering cluster destroy during S3 download")
                break
            if rebalance_task.state == "healthy":
                break
            time.sleep(10)

        self.assertTrue(
            volumes_seen,
            "EBS guest volumes did not appear before the rebalance completed — "
            "cannot target phase 5; check fusion threshold and data size")

        resources = self._capture_pre_destroy_resources(self.cluster)
        self.log.info(f"Destroying cluster {self.cluster.id} during S3 download (phase 5)")
        destroy_thread, destroy_result = self._destroy_cluster_async(self.cluster)
        self._assert_all_cluster_resources_cleaned(resources, timeout=self.post_destroy_cleanup_timeout)

        destroy_thread.join(timeout=1800)
        self.assertFalse(
            destroy_thread.is_alive(),
            "CapellaAPI.destroy_cluster did not complete within 1800s")
        self.assertFalse(
            destroy_result["failed"],
            f"Cluster destroy returned an error: {destroy_result['error']}")
        self.log.info(
            "Cluster destroyed during S3 download — all AWS resources verified clean")

    # ------------------------------------------------------------------
    # Test 3: Destroy during file extent migration (phase 6)
    # ------------------------------------------------------------------

    def test_destroy_during_file_extent_migration(self):
        """
        Destroy the cluster during phase 6 of the fusion rebalance, when EBS guest
        volumes have been detached from accelerator instances and re-attached to KV
        nodes for file extent migration (CBS reads file extents directly from EBS).

        Phase 6 is detected by finding at least one EBS guest volume in 'in-use'
        state whose attachment is to an instance NOT tagged as 'fusion-accelerator'
        (i.e., a CBS/KV node).

        Validates full AWS resource cleanup after destroy during this phase.
        """
        self._load_above_threshold()
        rebalance_task = self._trigger_scale_out()
        self.sleep(30, "Wait for rebalance to start")

        acc_filter = self._accelerator_filter()
        phase6_deadline = time.time() + 1800
        migration_seen = False

        while time.time() < phase6_deadline:
            if rebalance_task.state in self._FAILED_STATES:
                self.fail(
                    f"Rebalance failed before file extent migration: {rebalance_task.state}")

            # Phase 6: EBS volumes are 'in-use' and attached to a non-accelerator instance.
            # The couchbase-cloud-fusion-guest-volume=true tag is required here (not just
            # couchbase-cloud-function=fusion-accelerator) to scope to genuine guest volumes
            # only -- the latter alone also matches an accelerator instance's own root/boot
            # volume (see FusionCPResourceMonitor.get_current_guest_volume_ids docstring).
            raw_volumes = self.fusion_aws_util.ec2.list_volumes_by_cluster_id(filters={
                "couchbase-cloud-cluster-id": self.cluster.id,
                "couchbase-cloud-function": "fusion-accelerator",
                "couchbase-cloud-fusion-guest-volume": "true",
            })
            acc_ids = {
                inst["InstanceId"]
                for inst in self.fusion_aws_util.list_accelerator_instances(
                    acc_filter, log="Phase6Detection")
            }
            kv_attached = [
                v for v in raw_volumes
                if v.get("State") == "in-use"
                and any(
                    att.get("InstanceId") not in acc_ids
                    for att in v.get("Attachments", [])
                )
            ]
            if kv_attached:
                migration_seen = True
                self.log.info(
                    f"Phase 6 confirmed — {len(kv_attached)} EBS volume(s) attached "
                    f"to KV nodes. Triggering cluster destroy.")
                break

            if rebalance_task.state == "healthy":
                break
            time.sleep(10)

        self.assertTrue(
            migration_seen,
            "No EBS volumes found attached to KV nodes (phase 6) before rebalance "
            "completed — the phase 6 window may be too narrow; consider increasing "
            "data size or adding a brief sleep after phase 5 detection")

        resources = self._capture_pre_destroy_resources(self.cluster)
        self.log.info(
            f"Destroying cluster {self.cluster.id} during file extent migration (phase 6)")
        destroy_thread, destroy_result = self._destroy_cluster_async(self.cluster)
        self._assert_all_cluster_resources_cleaned(resources, timeout=self.post_destroy_cleanup_timeout)

        destroy_thread.join(timeout=1800)
        self.assertFalse(
            destroy_thread.is_alive(),
            "CapellaAPI.destroy_cluster did not complete within 1800s")
        self.assertFalse(
            destroy_result["failed"],
            f"Cluster destroy returned an error: {destroy_result['error']}")
        self.log.info(
            "Cluster destroyed during file extent migration — all AWS resources verified clean")

    # ------------------------------------------------------------------
    # Test 4: Destroy in scaleFailed state (S3 bucket deleted mid-phase-5)
    # ------------------------------------------------------------------

    def test_destroy_in_scale_failed_state(self):
        """
        Force the cluster into a scaleFailed state by deleting the fusion S3 log-store
        bucket while accelerators are actively downloading from it (phase 5). The CP
        detects the bucket deletion, aborts the rebalance, and transitions the cluster
        to scaleFailed. EBS guest volumes remain attached to accelerators at this point.

        Destroying the cluster in scaleFailed state verifies that the CP correctly
        cleans up all AWS resources even when the rebalance never completed:
        - EBS guest volumes (attached to accelerators at failure time)
        - Accelerator EC2 instances and ASGs
        - Cluster CBS/KV nodes
        - IAM instance profile
          (S3 bucket was deleted by the test — no bucket check after destroy)
        """
        self._load_above_threshold()

        # Capture S3 bucket name before triggering rebalance
        s3_bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)
        self.assertIsNotNone(
            s3_bucket_name,
            "Fusion S3 URI not available — ensure fusion is enabled and a bucket exists")

        rebalance_task = self._trigger_scale_out()
        self.sleep(30, "Wait for rebalance to start")

        # Wait for EBS guest volumes to confirm we are in phase 5 (S3 download active)
        # before deleting the bucket so the accelerators are mid-download when it vanishes.
        phase5_deadline = time.time() + 1800
        volumes_seen = False
        while time.time() < phase5_deadline:
            if rebalance_task.state in self._FAILED_STATES:
                break  # May have failed early — proceed to delete S3 and wait
            volumes = self.cp_monitor.get_current_guest_volume_ids(self.cluster)
            if volumes:
                volumes_seen = True
                self.log.info(
                    f"Phase 5 confirmed ({len(volumes)} EBS guest volume(s)) — "
                    f"deleting S3 bucket {s3_bucket_name} to force scaleFailed")
                break
            time.sleep(10)

        if not volumes_seen:
            self.log.warning(
                "EBS volumes did not appear before timeout — deleting S3 bucket "
                "anyway to force scaleFailed")

        # Delete the S3 bucket (all objects first, then the bucket itself)
        if self._s3_bucket_still_exists(s3_bucket_name):
            deleted = self.s3.delete_bucket(s3_bucket_name, force=True)
            self.assertTrue(deleted,
                            f"Failed to delete S3 bucket {s3_bucket_name}")
            self.log.info(
                f"S3 bucket {s3_bucket_name} deleted — waiting for cluster "
                f"to enter scaleFailed")
        else:
            self.log.warning(
                f"S3 bucket {s3_bucket_name} already gone before explicit deletion")

        # Wait for the CP to detect S3 unavailability and report a failed state
        failed_deadline = time.time() + 1800
        reached_failed = False
        while time.time() < failed_deadline:
            state = CapellaAPI.get_cluster_state(
                self.pod, self.tenant, self.cluster.id)
            if state in self._FAILED_STATES:
                reached_failed = True
                self.log.info(f"Cluster entered failed state: {state}")
                break
            time.sleep(15)

        self.assertTrue(
            reached_failed,
            "Cluster did not enter a failed state within 1800s after S3 bucket "
            "deletion — the CP may have not detected the missing bucket in time")

        # Log what AWS resources exist in the failed state (informational).
        # Guest-volume count delegates to cp_monitor.get_current_guest_volume_ids
        # (correctly tagged couchbase-cloud-fusion-guest-volume=true) rather than
        # hand-rolling the same 2-tag filter that over-counts accelerator root/boot
        # volumes; the broader "any cluster-tagged EBS volume" count is logged too
        # since destroy is expected to clean up more than just guest volumes.
        acc_in_failed = self.fusion_aws_util.list_accelerator_instances(
            self._accelerator_filter(), log="FailedStateResources")
        guest_vols_in_failed = self.cp_monitor.get_current_guest_volume_ids(self.cluster)
        all_ebs_in_failed = self.fusion_aws_util.ec2.list_volumes_by_cluster_id(filters={
            "couchbase-cloud-cluster-id": self.cluster.id,
        })
        asgs_in_failed = self.fusion_aws_util.list_cluster_fusion_asg(self.cluster.id)
        self.log.info(
            f"Resources present in scaleFailed state: "
            f"{len(acc_in_failed)} accelerator instances, "
            f"{len(guest_vols_in_failed)} fusion guest volumes "
            f"({len(all_ebs_in_failed)} cluster EBS volumes total), "
            f"{len(asgs_in_failed)} ASGs")

        # Capture IAM profile before destroy (accelerators still running in failed state)
        iam_profile_name = None
        if acc_in_failed:
            iam_profile_name = self.fusion_aws_util.ec2.get_instance_iam_profile_name(
                acc_in_failed[0]["InstanceId"])

        # S3 bucket was deleted by the test — skip S3 check in post-destroy verification
        resources = {
            "cluster_id": self.cluster.id,
            "s3_bucket_name": None,
            "iam_profile_name": iam_profile_name,
        }

        self.log.info(
            f"Destroying cluster {self.cluster.id} in scaleFailed state")
        destroy_thread, destroy_result = self._destroy_cluster_async(self.cluster)
        self._assert_all_cluster_resources_cleaned(resources, timeout=self.post_destroy_cleanup_timeout)

        destroy_thread.join(timeout=1800)
        self.assertFalse(
            destroy_thread.is_alive(),
            "CapellaAPI.destroy_cluster did not complete within 1800s")
        self.assertFalse(
            destroy_result["failed"],
            f"Cluster destroy returned an error: {destroy_result['error']}")
        self.log.info(
            "Cluster destroyed from scaleFailed state — all AWS resources verified clean")

    # ------------------------------------------------------------------
    # Test 5: Destroy during accelerator provisioning (phase 4)
    # ------------------------------------------------------------------

    def test_destroy_during_accelerator_provisioning(self):
        """
        Destroy the cluster during phase 4 of the fusion rebalance — fusion
        accelerator EC2 instances/ASGs have launched but no EBS guest volume has
        appeared yet (S3→EBS hydration / phase 5 has not started).

        Targeted deterministically by polling for "accelerator instances present
        AND zero guest volumes", rather than test_destroy_after_prepare_rebalance's
        best-effort timing, which only warns if it happens to land here.

        Validates full AWS resource cleanup, same as the other phase-targeted
        destroy tests.
        """
        self._load_above_threshold()
        rebalance_task = self._trigger_scale_out()

        acc_filter = self._accelerator_filter()
        phase4_deadline = time.time() + 1800
        phase4_seen = False

        while time.time() < phase4_deadline:
            if rebalance_task.state in self._FAILED_STATES:
                self.fail(
                    f"Rebalance failed before phase 4 window: {rebalance_task.state}")

            acc_instances = self.fusion_aws_util.list_accelerator_instances(
                acc_filter, log="Phase4Detection")
            guest_volumes = self.cp_monitor.get_current_guest_volume_ids(self.cluster)
            if acc_instances and not guest_volumes:
                phase4_seen = True
                self.log.info(
                    f"Phase 4 confirmed — {len(acc_instances)} accelerator instance(s) "
                    f"present, 0 guest volumes — triggering cluster destroy")
                break

            if rebalance_task.state == "healthy":
                break
            time.sleep(5)

        self.assertTrue(
            phase4_seen,
            "Accelerator instances with zero guest volumes (phase 4) were not "
            "observed before the rebalance progressed past it — the phase 4 "
            "window may be too narrow; consider polling more frequently or "
            "increasing data size to slow accelerator deployment")

        resources = self._capture_pre_destroy_resources(self.cluster)
        self.log.info(
            f"Destroying cluster {self.cluster.id} during accelerator provisioning (phase 4)")
        destroy_thread, destroy_result = self._destroy_cluster_async(self.cluster)
        self._assert_all_cluster_resources_cleaned(resources, timeout=self.post_destroy_cleanup_timeout)

        destroy_thread.join(timeout=1800)
        self.assertFalse(
            destroy_thread.is_alive(),
            "CapellaAPI.destroy_cluster did not complete within 1800s")
        self.assertFalse(
            destroy_result["failed"],
            f"Cluster destroy returned an error: {destroy_result['error']}")
        self.log.info(
            "Cluster destroyed during accelerator provisioning — all AWS resources verified clean")

    # ------------------------------------------------------------------
    # Test 6: Destroy during CBS rebalance, accelerators already gone (phase 7)
    # ------------------------------------------------------------------

    def test_destroy_during_cbs_rebalance(self):
        """
        Destroy the cluster during phase 7 of the fusion rebalance — EBS guest
        volumes have already been transferred to and are in use by KV/CBS nodes,
        and the fusion accelerator instances have been torn down naturally by the
        CP (not via cluster destroy). This is a materially different resource
        state than test_destroy_during_file_extent_migration (phase 6), where
        accelerators are still present alongside the KV-attached volumes.

        Phase 7 is detected by finding at least one EBS guest volume 'in-use' and
        attached to a non-accelerator (KV) instance, AND zero remaining fusion
        accelerator instances (they already finished their own teardown).

        Because no accelerators are expected to be alive at destroy time here,
        the captured IAM profile name will be None (nothing to assert on for
        that specific check) — consistent with test_destroy_after_prepare_rebalance's
        phase 2-3 case, just for the opposite reason (torn down vs. not yet created).
        """
        self._load_above_threshold()
        rebalance_task = self._trigger_scale_out()
        self.sleep(30, "Wait for rebalance to start")

        acc_filter = self._accelerator_filter()
        phase7_deadline = time.time() + 1800
        phase7_seen = False

        while time.time() < phase7_deadline:
            if rebalance_task.state in self._FAILED_STATES:
                self.fail(f"Rebalance failed before phase 7: {rebalance_task.state}")

            acc_instances = self.fusion_aws_util.list_accelerator_instances(
                acc_filter, log="Phase7Detection")
            acc_ids = {inst["InstanceId"] for inst in acc_instances}
            raw_volumes = self.fusion_aws_util.ec2.list_volumes_by_cluster_id(filters={
                "couchbase-cloud-cluster-id": self.cluster.id,
                "couchbase-cloud-function": "fusion-accelerator",
                "couchbase-cloud-fusion-guest-volume": "true",
            })
            kv_attached = [
                v for v in raw_volumes
                if v.get("State") == "in-use"
                and any(
                    att.get("InstanceId") not in acc_ids
                    for att in v.get("Attachments", [])
                )
            ]

            if kv_attached and not acc_instances:
                phase7_seen = True
                self.log.info(
                    f"Phase 7 confirmed — {len(kv_attached)} guest volume(s) on KV "
                    f"nodes, 0 accelerator instances remaining. Triggering destroy.")
                break

            if rebalance_task.state == "healthy":
                break
            time.sleep(10)

        self.assertTrue(
            phase7_seen,
            "Did not observe KV-attached guest volumes with zero accelerator "
            "instances (phase 7) before the rebalance completed — the phase 7 "
            "window may be too narrow, or accelerators are torn down at a "
            "different point relative to volume migration than assumed here; "
            "consider adding a brief sleep after phase 6 detection")

        resources = self._capture_pre_destroy_resources(self.cluster)
        self.log.info(
            f"Destroying cluster {self.cluster.id} during CBS rebalance (phase 7)")
        destroy_thread, destroy_result = self._destroy_cluster_async(self.cluster)
        self._assert_all_cluster_resources_cleaned(resources, timeout=self.post_destroy_cleanup_timeout)

        destroy_thread.join(timeout=1800)
        self.assertFalse(
            destroy_thread.is_alive(),
            "CapellaAPI.destroy_cluster did not complete within 1800s")
        self.assertFalse(
            destroy_result["failed"],
            f"Cluster destroy returned an error: {destroy_result['error']}")
        self.log.info(
            "Cluster destroyed during CBS rebalance — all AWS resources verified clean")

    # ------------------------------------------------------------------
    # Test 7: Destroy with an active EBS snapshot backup present
    # ------------------------------------------------------------------

    def test_destroy_with_active_backup(self):
        """
        Verify that EBS snapshot(s) created by a cloud snapshot backup are
        cleaned up when the cluster is destroyed (couchbase-cloud:
        destroy_backups.go processDestroy destroys all snapshots for a backup
        uniformly on cluster destroy, unless a RetainSnapshotBackups-equivalent
        flag is set).

        GAP / NOT IMPLEMENTED: TAF has no way to pass a RetainSnapshotBackups
        flag on destroy — CapellaUtils.destroy_cluster ultimately calls
        CapellaAPI.delete_cluster_internal (lib/capellaAPI, a read-only git
        submodule per AGENTS.md) which issues a bare DELETE with no body/query
        parameters. Only the default (snapshots cleaned up) path is exercised
        here; the retain-on-destroy variant would require adding a parameter to
        the submodule's delete_cluster_internal(), which is out of scope.
        """
        self._load_above_threshold()

        self.log.info(f"Creating EBS snapshot backup on cluster {self.cluster.id}")
        result = CapellaAPI.create_cloud_snapshot_backup(
            self.pod, self.tenant, self.tenant.projects[0], self.cluster.id)
        self.assertIsNotNone(result, "create_cloud_snapshot_backup returned None")
        backup_id = result.get("id")
        self.assertIsNotNone(
            backup_id, f"No 'id' in create_cloud_snapshot_backup response: {result}")

        ok = CapellaAPI.wait_for_cloud_snapshot_backup_to_complete(
            self.pod, self.tenant, self.tenant.projects[0], self.cluster.id,
            backup_id, timeout=4 * 3600)
        self.assertTrue(ok, f"Snapshot backup {backup_id} did not complete")

        snapshot_filter = [
            {"Name": "tag:couchbase-cloud-guestvolume", "Values": ["true"]},
            {"Name": "tag:couchbase-cloud-backup-id", "Values": [backup_id]},
            {"Name": "tag:couchbase-cloud-cluster-id", "Values": [self.cluster.id]},
        ]
        pre_destroy_snapshots = self.fusion_aws_util.ec2.list_snapshots_by_tags(snapshot_filter)
        self.assertTrue(
            pre_destroy_snapshots,
            f"No EBS snapshots found for completed backup {backup_id} on "
            f"cluster {self.cluster.id} — cannot verify destroy cleanup")
        self.log.info(
            f"{len(pre_destroy_snapshots)} EBS snapshot(s) present for backup "
            f"{backup_id} before destroy")

        resources = self._capture_pre_destroy_resources(self.cluster)
        self.log.info(
            f"Destroying cluster {self.cluster.id} with an active backup ({backup_id}) present")
        destroy_thread, destroy_result = self._destroy_cluster_async(self.cluster)
        self._assert_all_cluster_resources_cleaned(resources, timeout=self.post_destroy_cleanup_timeout)

        destroy_thread.join(timeout=1800)
        self.assertFalse(
            destroy_thread.is_alive(),
            "CapellaAPI.destroy_cluster did not complete within 1800s")
        self.assertFalse(
            destroy_result["failed"],
            f"Cluster destroy returned an error: {destroy_result['error']}")

        # Default cleanup path (no RetainSnapshotBackups-equivalent available
        # from TAF — see docstring above): the backup's EBS snapshots should be
        # gone after destroy.
        deadline = time.time() + self.post_destroy_cleanup_timeout
        remaining = pre_destroy_snapshots
        while time.time() < deadline:
            remaining = self.fusion_aws_util.ec2.list_snapshots_by_tags(snapshot_filter)
            if not remaining:
                break
            time.sleep(15)
        self.assertEqual(
            len(remaining), 0,
            f"{len(remaining)} EBS snapshot(s) for backup {backup_id} still "
            f"exist after cluster {self.cluster.id} destroy: "
            f"{[s.get('SnapshotId') for s in remaining]}")
        self.log.info(
            f"All EBS snapshots for backup {backup_id} confirmed deleted after destroy")

    # ------------------------------------------------------------------
    # Test 8: Destroy rejected while cluster is an active restore source
    # ------------------------------------------------------------------

    def test_destroy_rejected_while_restore_source(self):
        """
        Verify Capella rejects destroying a cluster while its EBS snapshot
        backups are the source of an in-flight restore/clone (couchbase-cloud:
        internal/clusters/service/manager.go isSourceClusterForRestore →
        goof.ErrClusterIsTheSourceForRestore, HTTP 409) rather than proceeding
        with fusion teardown mid-restore.

        Sequence: take a snapshot backup of self.cluster, clone it into a
        brand-new secondary cluster (CapellaAPI.clone_cloud_snapshot_backup —
        same v4 clone-restore mechanism fusion_backup_restore_volume.py uses to
        bootstrap its secondary cluster), then — while that clone/restore job is
        still queued/processing — attempt CapellaAPI.destroy_cluster(self.cluster).
        destroy_cluster raises immediately on any non-202 response from
        delete_cluster_internal, so a synchronous rejection surfaces as an
        exception here.

        This is a heavyweight test: it provisions an entire second Capella
        cluster via clone (to establish the "active restore source" precondition)
        purely to exercise the rejection path, then tears the clone down again.

        GAP / NOT FULLY IMPLEMENTED: TAF has no way to pass a
        RetainSnapshotBackups-equivalent flag — delete_cluster_internal
        (lib/capellaAPI, a read-only submodule) takes no body/query parameters —
        so only the default (no-retain) rejection path is exercised. It is also
        not independently confirmed against a live control plane whether the
        rejection remains in effect for the entire lifetime of the clone/restore
        relationship or only while the restore job is actively queued/processing;
        this test targets the latter (narrower, more clearly "active") window.
        """
        self._load_above_threshold()

        project_id = self.tenant.projects[0]
        backup_id = None
        v2_key_id = v4_key_id = None
        bearer_token = None
        new_cluster_id = None
        restore_id = None

        try:
            self.log.info(f"Creating EBS snapshot backup on cluster {self.cluster.id}")
            result = CapellaAPI.create_cloud_snapshot_backup(
                self.pod, self.tenant, project_id, self.cluster.id)
            self.assertIsNotNone(result, "create_cloud_snapshot_backup returned None")
            backup_id = result.get("id")
            self.assertIsNotNone(
                backup_id, f"No 'id' in create_cloud_snapshot_backup response: {result}")

            ok = CapellaAPI.wait_for_cloud_snapshot_backup_to_complete(
                self.pod, self.tenant, project_id, self.cluster.id, backup_id, timeout=4 * 3600)
            self.assertTrue(ok, f"Snapshot backup {backup_id} did not complete")

            v2_key_id, v4_key_id, bearer_token = CapellaAPI.create_v4_api_key(
                self.pod, self.tenant, name_prefix="fusion-destroy-restore-source")
            self.assertIsNotNone(bearer_token, "Failed to mint v4 API key for clone call")

            cluster_name = f"fusion_destroy_src_{self.cluster.id[:8]}_{uuid.uuid4().hex[:6]}"
            resp = CapellaAPI.clone_cloud_snapshot_backup(
                self.pod, self.tenant, project_id, backup_id,
                name=cluster_name, region=self.aws_region, bearer_token=bearer_token)
            self.assertIsNotNone(
                resp, f"clone_cloud_snapshot_backup returned None for backup {backup_id}")
            restore_id = resp.get("restoreId")
            new_cluster_id = resp.get("clusterId")
            self.assertIsNotNone(restore_id, f"No 'restoreId' in clone response: {resp}")
            self.assertIsNotNone(new_cluster_id, f"No 'clusterId' in clone response: {resp}")
            self.log.info(
                f"Clone triggered from {self.cluster.id}'s backup {backup_id} — "
                f"restoreId={restore_id}, new cluster provisioning as {new_cluster_id}")

            # Poll until the restore job is confirmed in-flight (queued/processing)
            # before attempting the destroy — this is the window in which the CP
            # should consider self.cluster an active restore source.
            in_flight_deadline = time.time() + 600
            restore_state = None
            while time.time() < in_flight_deadline:
                restores = CapellaAPI.list_cloud_snapshot_restores(
                    self.pod, self.tenant, project_id, new_cluster_id)
                restore_info = next(
                    (r.get("data") for r in restores
                     if r.get("data", {}).get("id") == restore_id),
                    None)
                restore_state = restore_info.get("status") if restore_info else None
                if restore_state in ("queued", "processing"):
                    break
                if restore_state == "complete":
                    self.log.warning(
                        "Clone restore completed before destroy could be attempted — "
                        "the restore-source rejection window may already be closed")
                    break
                time.sleep(10)

            self.log.info(
                f"Attempting to destroy {self.cluster.id} while it is the source "
                f"for restore {restore_id} (state={restore_state})")
            rejected = False
            try:
                CapellaAPI.destroy_cluster(self.pod, self.tenant, self.cluster)
            except Exception as e:
                rejected = True
                self.log.info(
                    f"Destroy correctly rejected while cluster is a restore source: {e}")

            self.assertTrue(
                rejected,
                f"Cluster {self.cluster.id} destroy did not raise while it was an "
                f"active restore source (expected 409 ErrClusterIsTheSourceForRestore)")
            self.assertIn(
                self.cluster, self.tenant.clusters,
                "Cluster was removed from tenant.clusters despite destroy being rejected")

            state = CapellaAPI.get_cluster_state(self.pod, self.tenant, self.cluster.id)
            self.log.info(
                f"Cluster {self.cluster.id} still present after rejected destroy, state={state}")

        finally:
            # Let the clone/restore job finish (success or failure) before tearing
            # down the secondary — destroying it mid-restore would just hit the
            # same rejection this test is designed to prove, on a cluster we
            # don't otherwise care about preserving.
            if new_cluster_id and restore_id:
                try:
                    CapellaAPI.wait_for_cloud_snapshot_restore_to_complete(
                        self.pod, self.tenant, project_id, new_cluster_id, restore_id,
                        timeout=3600)
                except Exception as e:
                    self.log.warning(f"Error waiting for clone restore to finish: {e}")
            if new_cluster_id:
                secondary = CBCluster(
                    username=self.rest_username, password=self.rest_password, servers=[None])
                secondary.id = new_cluster_id
                self.tenant.clusters.append(secondary)
                try:
                    CapellaAPI.destroy_cluster(self.pod, self.tenant, secondary)
                    self.log.info(f"Cloned secondary cluster {new_cluster_id} destroyed")
                except Exception as e:
                    self.log.error(
                        f"Failed to destroy cloned secondary cluster {new_cluster_id}: {e}")
            if backup_id:
                try:
                    CapellaAPI.delete_cloud_snapshot_backup(
                        self.pod, self.tenant, project_id, self.cluster.id, backup_id)
                except Exception as e:
                    self.log.warning(f"Failed to delete backup {backup_id}: {e}")
            if v2_key_id or v4_key_id:
                try:
                    CapellaAPI.delete_v4_api_key(
                        self.pod, self.tenant, v2_key_id, v4_key_id, bearer_token)
                except Exception as e:
                    self.log.error(f"Failed to clean up v4 API key(s): {e}")

    # ------------------------------------------------------------------
    # Test 9: Destroy attempted while cluster is turned off
    # ------------------------------------------------------------------

    def test_destroy_while_turning_off(self):
        """
        Destroy attempted while the cluster is turned off should be rejected by
        fusion teardown rather than silently succeeding (couchbase-cloud:
        fusion/accelerator/accelerator.go assertForceTearDown returns
        ErrTearDownWhileTurningOff when the cluster is in a TurningOff/TurnedOff
        state), or should surface as a clear failure rather than an unbounded hang.

        NOT INDEPENDENTLY CONFIRMED: whether this manifests as a synchronous
        non-202 rejection on the initial DELETE call, or as an async destroy-job
        failure that never reaches the "Not Found." terminal state (relying on
        dedicated.py's destroy_cluster timeout fix — see the review — to
        eventually raise instead of hot-looping), was not verified against a
        live control plane. This test tolerates either manifestation by
        asserting only that the destroy attempt does not succeed within
        destroy_reject_wait_timeout and that the cluster is not silently removed
        from tenant.clusters.
        """
        self._load_above_threshold()

        dr_on_off = DoctorHostedOnOff(self.pod, self.tenant, self.cluster)
        self.log.info(f"Turning cluster {self.cluster.id} off before destroy attempt")
        turned_off = dr_on_off.turn_off_cluster(timeout=1200)
        self.assertTrue(
            turned_off, f"Cluster {self.cluster.id} did not reach 'turned_off' state")

        self.log.info(f"Attempting to destroy cluster {self.cluster.id} while turned off")
        destroy_thread, destroy_result = self._destroy_cluster_async(self.cluster)
        destroy_thread.join(timeout=self.destroy_reject_wait_timeout)

        rejected = destroy_result["failed"] or destroy_thread.is_alive()
        self.assertTrue(
            rejected,
            f"Destroy of cluster {self.cluster.id} appeared to succeed while the "
            f"cluster was turned off — expected rejection (ErrTearDownWhileTurningOff) "
            f"or at least non-completion within {self.destroy_reject_wait_timeout}s")

        if destroy_thread.is_alive():
            self.log.warning(
                f"Destroy thread for {self.cluster.id} still running after "
                f"{self.destroy_reject_wait_timeout}s — treating as rejected/stuck "
                f"rather than waiting further; tearDown will attempt cleanup")
        else:
            self.assertIn(
                self.cluster, self.tenant.clusters,
                "Cluster was removed from tenant.clusters despite an expected destroy rejection")
            self.log.info(
                f"Destroy correctly rejected while turned off: {destroy_result['error']}")
            # Turn the cluster back on so tearDown's normal destroy path (which
            # assumes a live, reachable cluster) can clean it up normally.
            self.log.info(f"Turning cluster {self.cluster.id} back on for teardown")
            turned_on = dr_on_off.turn_on_cluster(timeout=1200)
            self.assertTrue(
                turned_on, f"Cluster {self.cluster.id} did not return to healthy after turn-on")
