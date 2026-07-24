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
  4. test_destroy_in_scale_failed_state           — corrupt a few S3 log-store files/folders before rebalance to force scaleFailed, then destroy
  5. test_destroy_during_accelerator_provisioning — destroy while accelerator instances are still 'pending' (phase 4)
  6. test_destroy_during_cbs_rebalance            — destroy once volumes are on KV nodes and accelerators are gone (phase 7)
  7. test_destroy_with_active_backup              — destroy with a completed EBS snapshot backup (regular + guest-volume) present
  8. test_destroy_rejected_while_restore_source    — destroy must be rejected while cluster is an active restore source
  9. test_destroy_while_turning_off               — destroy of a turned-off cluster is an allowed operation and must complete cleanly
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
from membase.api.rest_client import RestConnection

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
        self.fragmentation = int(self.input.param("fragmentation", 50))
        self.rebalance_timeout = self.input.param("rebalance_timeout", 7200)
        # How long to poll AWS for full post-destroy resource cleanup before
        # asserting failure. Kept configurable rather than a hardcoded 600s:
        # the CP's destroy teardown has no fixed SLA (aws/destroyer.go retries
        # until its own job context deadline, not a constant), and recent
        # teardown-ordering/retry changes on the CP side make a single fixed
        # number risky to bake in permanently.
        self.post_destroy_cleanup_timeout = self.input.param(
            "post_destroy_cleanup_timeout", 600)
        # Overall budget for tearDown's own force-destroy safety net (see
        # _ensure_cluster_destroyed) to get the cluster into a destroyable
        # state and destroyed, regardless of how/where the test itself failed.
        self.teardown_force_destroy_timeout = self.input.param(
            "teardown_force_destroy_timeout", 2400)
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
        try:
            self._ensure_cluster_destroyed()
        except Exception as e:
            # Must never propagate -- a leaked cluster is far worse than a
            # noisy log line, and BaseTestCase.tearDown below is still a
            # fallback destroy attempt if this raised before finishing.
            self.log.error(
                f"[tearDown] Unexpected error in _ensure_cluster_destroyed: {e}")
        # ProvisionedBaseTestCase.tearDown destroys any cluster remaining in
        # tenant.clusters (since capella["clusters"] is not set). By this
        # point _ensure_cluster_destroyed has already removed self.cluster
        # from tenant.clusters on success, so this loop is normally a no-op --
        # it only serves as a final fallback if the explicit destroy above
        # still failed.
        BaseTestCase.tearDown(self)

    def _ensure_cluster_destroyed(self):
        """
        Best-effort, retrying guarantee that self.cluster is destroyed before
        the rest of tearDown runs, no matter where or how the test failed
        (setUp, a phase-detection wait, a mid-test assertion, or an
        unhandled exception).

        Specifically handles the two states a destroy test can legitimately
        leave the cluster in in that make a plain destroy_cluster call fail:
          - turned_off / turning_off (test_destroy_while_turning_off failing
            before it turns the cluster back on) -- rejected by the CP with
            ErrTearDownWhileTurningOff, so turn it back on first.
          - still an active restore source (test_destroy_rejected_while_
            restore_source failing before its own restore-completion wait
            finishes) -- rejected with ErrClusterIsTheSourceForRestore (409),
            so just retry after a short wait for the restore job to settle.

        Bounded by self.teardown_force_destroy_timeout overall; never raises.
        """
        if not hasattr(self, "tenant") or not hasattr(self, "cluster"):
            # setUp failed before tenant/cluster were even assigned --
            # nothing to destroy yet (ProvisionedBaseTestCase.setUp itself
            # would not have created a cluster this far in).
            return

        deadline = time.time() + self.teardown_force_destroy_timeout

        # If the test kicked off _destroy_cluster_async and then raised
        # before joining it (e.g. a resource-cleanup assertion failed, or the
        # cluster was just genuinely still destroying when the test's own
        # join(timeout=...) gave up), that thread may still be in flight.
        # NEVER fire a second, concurrent destroy_cluster call while it's
        # still running: CapellaUtils.destroy_cluster does
        # tenant.clusters.remove(cluster) on success, and two concurrent
        # callers both reaching "Not Found." both try to remove the same
        # object from the same list -- the second one raises
        # ValueError("list.remove(x): x not in list"). So we wait it out
        # (bounded by the overall deadline) and only ever consider a fresh
        # attempt once it has actually finished one way or another.
        pending = getattr(self, "_pending_destroy", None)
        if pending is not None:
            thread, result = pending
            if thread.is_alive():
                self.log.warning(
                    f"[tearDown] A destroy_cluster call for {self.cluster.id} "
                    f"was still in flight when tearDown started — waiting "
                    f"for it to finish rather than risk a concurrent "
                    f"double-destroy race")
                thread.join(timeout=max(0, deadline - time.time()))
                if thread.is_alive():
                    self.log.critical(
                        f"[tearDown] In-flight destroy_cluster thread for "
                        f"{self.cluster.id} did not finish within "
                        f"{self.teardown_force_destroy_timeout}s — giving up "
                        f"here rather than risk a concurrent double-destroy "
                        f"race; check AWS/Capella for a leaked cluster.")
                    return
            if result.get("failed"):
                self.log.warning(
                    f"[tearDown] In-flight destroy_cluster for "
                    f"{self.cluster.id} had already failed: {result.get('error')}")

        if self.cluster not in self.tenant.clusters:
            # Already destroyed (by the test itself, or by the in-flight
            # thread we just joined above).
            return

        attempt = 0
        while True:
            attempt += 1
            try:
                state = CapellaAPI.get_cluster_state(
                    self.pod, self.tenant, self.cluster.id)
            except Exception as e:
                self.log.warning(
                    f"[tearDown] Could not fetch state for cluster "
                    f"{self.cluster.id} (attempt {attempt}): {e}")
                state = None

            if state in ("turning_off", "turned_off"):
                try:
                    self.log.warning(
                        f"[tearDown] Cluster {self.cluster.id} is {state} — "
                        f"turning it on before destroy (attempt {attempt})")
                    DoctorHostedOnOff(self.pod, self.tenant, self.cluster) \
                        .turn_on_cluster(timeout=1200)
                except Exception as e:
                    self.log.error(
                        f"[tearDown] Failed to turn on cluster "
                        f"{self.cluster.id} before destroy (attempt "
                        f"{attempt}): {e}")

            remaining = deadline - time.time()
            if remaining <= 0:
                break
            try:
                CapellaAPI.destroy_cluster(
                    self.pod, self.tenant, self.cluster,
                    timeout=max(60, int(remaining)))
                self.log.info(
                    f"[tearDown] Cluster {self.cluster.id} destroyed "
                    f"(attempt {attempt})")
                return
            except Exception as e:
                self.log.error(
                    f"[tearDown] Destroy attempt {attempt} failed for "
                    f"cluster {self.cluster.id}: {e}")
                if time.time() + 30 >= deadline:
                    break
                time.sleep(30)

        self.log.critical(
            f"[tearDown] Cluster {self.cluster.id} could NOT be confirmed "
            f"destroyed within {self.teardown_force_destroy_timeout}s across "
            f"{attempt} attempt(s) — falling back to BaseTestCase.tearDown's "
            f"destroy loop as a last resort; check AWS/Capella for a leaked "
            f"cluster if that also fails.")

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

        Also stashes (thread, result) on self._pending_destroy so that
        tearDown's _ensure_cluster_destroyed can join this thread first if
        the test fails/raises before joining it itself -- otherwise tearDown
        could fire a second, concurrent destroy_cluster call against the
        same cluster while this one is still in flight.
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
        self._pending_destroy = (thread, result)
        thread.start()
        return thread, result

    def _assert_all_cluster_resources_cleaned(self, resources, timeout=None,
                                              destroy_thread=None):
        """Assert every AWS resource the CP created for this cluster is gone.

        Thin Layer 3 wrapper: all polling/monitoring logic lives in
        FusionCPResourceMonitor.monitor_full_cluster_teardown() (Layer 2),
        which returns a list of failure strings; this method only asserts
        on that list, per the "utilities return data, tests assert"
        convention (see agents/fusion.md / architecture.md).

        Call this concurrently with the async destroy_cluster call (pass
        its thread as destroy_thread) so resource cleanup is still observed
        while destroy is in progress -- but the timeout budget used to
        decide pass/fail only starts counting once destroy_thread is
        confirmed done, not from when this method was called. Otherwise the
        cleanup-timeout budget is silently eaten by however long the
        destroy call itself takes (on Jenkins build 16458, destroy took
        ~13.4 minutes, longer than the then-600s cleanup timeout, so the
        point-in-time failure snapshot was taken while the cluster was
        still legitimately being destroyed).
        """
        if timeout is None:
            timeout = self.post_destroy_cleanup_timeout
        cluster_id = resources["cluster_id"]
        failures = self.cp_monitor.monitor_full_cluster_teardown(
            cluster_id, resources, timeout=timeout, destroy_thread=destroy_thread)
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
        self._assert_all_cluster_resources_cleaned(
            resources, timeout=self.post_destroy_cleanup_timeout,
            destroy_thread=destroy_thread)

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
        self._assert_all_cluster_resources_cleaned(
            resources, timeout=self.post_destroy_cleanup_timeout,
            destroy_thread=destroy_thread)

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
        self._assert_all_cluster_resources_cleaned(
            resources, timeout=self.post_destroy_cleanup_timeout,
            destroy_thread=destroy_thread)

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
        Force the cluster into a scaleFailed state by corrupting the fusion S3
        log-store *before* triggering the rebalance -- deleting a few
        vBucket/shard folders and a few individual files (FusionAWSUtil.
        corrupt_fusion_log_store) rather than the entire S3 bucket. The
        accelerator then fails to download the missing data during phase 5,
        and the rebalance fails.

        NOTE: deleting the *entire* S3 bucket mid-phase-5 (the previous
        approach) was observed to leave the CP hot-looping "Replacing Node
        (1/3)" at a fixed progress percentage for 35+ minutes without ever
        detecting the outage or transitioning to scaleFailed (Jenkins build
        16485) -- i.e. it doesn't reliably repro the failure this test targets.
        Corrupting a subset of objects before the rebalance starts is a
        smaller, more targeted blast radius that the accelerator is expected
        to hit deterministically as soon as it tries to download the missing
        shards.

        Destroying the cluster in scaleFailed state verifies that the CP correctly
        cleans up all AWS resources even when the rebalance never completed:
        - EBS guest volumes (attached to accelerators at failure time)
        - Accelerator EC2 instances and ASGs
        - Cluster CBS/KV nodes
        - IAM instance profile
        - Fusion S3 bucket (only individual objects were deleted by the test,
          the bucket itself still exists going into destroy)
        """
        self._load_above_threshold()

        # Capture S3 bucket name and resolve the bucket UUID (kv/<uuid>/ is the
        # log-store prefix for this Couchbase bucket) before corrupting anything.
        s3_bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)
        self.assertIsNotNone(
            s3_bucket_name,
            "Fusion S3 URI not available — ensure fusion is enabled and a bucket exists")

        bucket = self.cluster.buckets[0]
        if not getattr(bucket, "bucket_uuid", None):
            info = RestConnection(self.cluster.master).get_bucket_details(bucket_name=bucket.name)
            bucket.bucket_uuid = info.get("uuid")
        self.assertIsNotNone(
            bucket.bucket_uuid, f"Could not resolve UUID for bucket {bucket.name}")

        corrupted = self.fusion_aws_util.corrupt_fusion_log_store(
            s3_bucket_name, bucket.bucket_uuid, num_folders=3, num_files=5)
        self.assertTrue(
            corrupted["folders_deleted"] or corrupted["files_deleted"],
            f"Failed to corrupt any fusion log-store objects under kv/{bucket.bucket_uuid} "
            f"in {s3_bucket_name} before triggering the rebalance")
        self.log.info(
            f"Corrupted fusion log store before rebalance: "
            f"{len(corrupted['folders_deleted'])} folder(s), "
            f"{len(corrupted['files_deleted'])} individual file(s)")

        rebalance_task = self._trigger_scale_out()
        self.sleep(30, "Wait for rebalance to start")

        # Wait for the rebalance to fail once the accelerator hits the missing objects.
        failed_deadline = time.time() + 1800
        reached_failed = False
        while time.time() < failed_deadline:
            if rebalance_task.state in self._FAILED_STATES:
                reached_failed = True
                self.log.info(f"Rebalance entered failed state: {rebalance_task.state}")
                break
            time.sleep(15)

        self.assertTrue(
            reached_failed,
            f"Rebalance did not enter a failed state within 1800s after corrupting "
            f"the fusion log store — current state: {rebalance_task.state}")

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

        # The bucket itself was never deleted this time -- only some of its
        # objects were -- so the normal post-destroy S3 bucket check applies.
        resources = {
            "cluster_id": self.cluster.id,
            "s3_bucket_name": s3_bucket_name,
            "iam_profile_name": iam_profile_name,
        }

        self.log.info(
            f"Destroying cluster {self.cluster.id} in scaleFailed state")
        destroy_thread, destroy_result = self._destroy_cluster_async(self.cluster)
        self._assert_all_cluster_resources_cleaned(
            resources, timeout=self.post_destroy_cleanup_timeout,
            destroy_thread=destroy_thread)

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
        Destroy the cluster while fusion accelerator EC2 instances are still
        'pending' (initiating) — launched but not yet running.

        Originally this targeted "accelerator running AND zero guest volumes"
        via list_accelerator_instances(), but that method only ever returns
        instances that are already 'running' AND whose attached volume IOPS
        already matches FUSION_ACCELERATOR_IOPS -- i.e. by the time an
        instance is visible there at all, its guest volume already exists.
        On a live run (Jenkins build 16485) guest volumes and accelerator
        instances were observed appearing together in the very same AWS poll,
        confirming that window isn't reliably observable: guest-volume EBS
        creation can happen asynchronously in parallel with instance boot,
        not strictly after it.

        'pending' EC2 instance state, in contrast, is a distinct, real
        lifecycle stage that AWS itself reports before an instance transitions
        to 'running' -- polling raw EC2 state (bypassing
        list_accelerator_instances' running+IOPS filter) for at least one
        fusion-accelerator-tagged instance still 'pending' gives a
        deterministic, real window to target, independent of whether its
        guest volume has been created yet.

        Validates full AWS resource cleanup, same as the other phase-targeted
        destroy tests. IAM profile is captured directly from the pending
        instance (list_accelerator_instances/​_capture_pre_destroy_resources
        would see it as not-yet-running and miss it, weakening that check).
        """
        self._load_above_threshold()
        rebalance_task = self._trigger_scale_out()

        acc_tag_filter = self.fusion_aws_util._cluster_filter(
            self.cluster.id,
            [{'Name': 'tag:couchbase-cloud-function', 'Values': ['fusion-accelerator']}])
        phase4_deadline = time.time() + 1800
        pending_instances = []

        while time.time() < phase4_deadline:
            if rebalance_task.state in self._FAILED_STATES:
                self.fail(
                    f"Rebalance failed before phase 4 window: {rebalance_task.state}")

            raw_instances = self.fusion_aws_util.ec2.list_instances(filters=acc_tag_filter)
            pending_instances = [
                i for i in raw_instances if i.get("State", {}).get("Name") == "pending"]
            if pending_instances:
                self.log.info(
                    f"Phase 4 confirmed — {len(pending_instances)} accelerator "
                    f"instance(s) still 'pending' (initiating) — triggering "
                    f"cluster destroy")
                break

            if rebalance_task.state == "healthy":
                break
            # 'pending' is short-lived -- poll tightly rather than the 5-15s
            # interval used elsewhere for slower-moving phases.
            time.sleep(2)

        self.assertTrue(
            pending_instances,
            "No accelerator instance was observed in 'pending' (initiating) "
            "state before the rebalance progressed past it — the pending "
            "window may be too narrow at this poll rate; consider polling "
            "even more tightly or increasing data size to slow accelerator "
            "deployment")

        iam_profile_name = self.fusion_aws_util.ec2.get_instance_iam_profile_name(
            pending_instances[0]["InstanceId"])
        resources = {
            "cluster_id": self.cluster.id,
            "s3_bucket_name": self._get_s3_bucket_name_from_uri(self.cluster),
            "iam_profile_name": iam_profile_name,
        }

        self.log.info(
            f"Destroying cluster {self.cluster.id} during accelerator provisioning (phase 4)")
        destroy_thread, destroy_result = self._destroy_cluster_async(self.cluster)
        self._assert_all_cluster_resources_cleaned(
            resources, timeout=self.post_destroy_cleanup_timeout,
            destroy_thread=destroy_thread)

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
        self._assert_all_cluster_resources_cleaned(
            resources, timeout=self.post_destroy_cleanup_timeout,
            destroy_thread=destroy_thread)

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

        For complete coverage this test triggers a fusion scale-out rebalance
        first and waits for phase 6 (EBS guest volumes attached to KV nodes for
        file extent migration — same detection as test_destroy_during_file_extent_migration)
        before taking the backup. That guarantees the backup's
        GetEligibleNodes() (couchbase-cloud: recoverer.go) sees genuine fusion
        guest-volume nodes alongside the regular CBS/KV nodes, so
        snapshot_creator.go's Create() produces BOTH regular data-volume
        snapshots AND guest-volume snapshots (tagged
        couchbase-cloud-guestvolume=true by fusionTags()) for this backup —
        rather than only regular snapshots, which is all a backup taken with no
        rebalance in flight could ever produce. Phase 6 is deliberately used
        (not phases 2-5): couchbase-cloud's shouldBlockOnFusionRebalance
        (backup.go) rejects a backup with ErrFusionRebalanceDownloading while
        the fusion manifest is Pending/DownloadComplete; by phase 6 the
        manifest has moved to BackgroundMigration — the guest volumes are
        still attached and syncing (the "guest volumes present at the end of
        the rebalance" window) but backup is no longer blocked.
        """
        self._load_above_threshold()

        rebalance_task = self._trigger_scale_out()
        self.sleep(30, "Wait for rebalance to start")

        acc_filter = self._accelerator_filter()
        phase6_deadline = time.time() + 1800
        guest_volume_ids = []
        while time.time() < phase6_deadline:
            if rebalance_task.state in self._FAILED_STATES:
                self.fail(
                    f"Rebalance failed before file extent migration: {rebalance_task.state}")

            # Same phase 6 detection as test_destroy_during_file_extent_migration:
            # EBS guest volumes 'in-use' and attached to a non-accelerator (KV) node.
            raw_volumes = self.fusion_aws_util.ec2.list_volumes_by_cluster_id(filters={
                "couchbase-cloud-cluster-id": self.cluster.id,
                "couchbase-cloud-function": "fusion-accelerator",
                "couchbase-cloud-fusion-guest-volume": "true",
            })
            acc_ids = {
                inst["InstanceId"]
                for inst in self.fusion_aws_util.list_accelerator_instances(
                    acc_filter, log="BackupPhase6Detection")
            }
            kv_attached = [
                v for v in raw_volumes
                if v.get("State") == "in-use"
                and any(
                    att.get("InstanceId") not in acc_ids
                    for att in v.get("Attachments", []))
            ]
            if kv_attached:
                guest_volume_ids = [v["VolumeId"] for v in kv_attached]
                self.log.info(
                    f"Phase 6 confirmed — {len(guest_volume_ids)} guest volume(s) "
                    f"attached to KV nodes. Triggering backup while they are present.")
                break
            if rebalance_task.state == "healthy" and not raw_volumes:
                break
            time.sleep(10)

        self.assertTrue(
            guest_volume_ids,
            "No EBS guest volumes found attached to KV nodes before the rebalance "
            "completed — cannot verify fusion guest-volume snapshot coverage; "
            "consider increasing data size or reducing the fusion threshold")

        self.log.info(
            f"Creating EBS snapshot backup on cluster {self.cluster.id} while "
            f"{len(guest_volume_ids)} guest volume(s) are present")
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

        self.assertTrue(
            self.cp_monitor.verify_guest_volume_snapshots_for_backup(
                self.cluster, backup_id, num_snapshots=len(guest_volume_ids)),
            f"Guest-volume EBS snapshot count for backup {backup_id} did not match "
            f"the {len(guest_volume_ids)} guest volume(s) present at backup time")

        snapshot_filter = [
            {"Name": "tag:couchbase-cloud-backup-id", "Values": [backup_id]},
            {"Name": "tag:couchbase-cloud-cluster-id", "Values": [self.cluster.id]},
        ]
        pre_destroy_snapshots = self.fusion_aws_util.ec2.list_snapshots_by_tags(snapshot_filter)
        self.assertTrue(
            pre_destroy_snapshots,
            f"No EBS snapshots found for completed backup {backup_id} on "
            f"cluster {self.cluster.id} — cannot verify destroy cleanup")
        self.assertGreater(
            len(pre_destroy_snapshots), len(guest_volume_ids),
            f"Backup {backup_id} produced only {len(pre_destroy_snapshots)} snapshot(s), "
            f"no more than the {len(guest_volume_ids)} guest-volume snapshot(s) — "
            f"expected additional regular CBS/KV data-volume snapshots too")
        self.log.info(
            f"{len(pre_destroy_snapshots)} EBS snapshot(s) present for backup "
            f"{backup_id} before destroy ({len(guest_volume_ids)} of which are "
            f"guest-volume snapshots)")

        # Let the fusion rebalance finish on its own -- this test targets destroy
        # with an active backup on a stable cluster, not destroy mid-rebalance
        # (covered separately by test_destroy_during_file_extent_migration etc).
        self.task_manager.get_task_result(rebalance_task)
        self.assertTrue(
            rebalance_task.result,
            f"Rebalance did not complete successfully before destroy: "
            f"{rebalance_task.state}")

        resources = self._capture_pre_destroy_resources(self.cluster)
        self.log.info(
            f"Destroying cluster {self.cluster.id} with an active backup ({backup_id}) present")
        destroy_thread, destroy_result = self._destroy_cluster_async(self.cluster)
        self._assert_all_cluster_resources_cleaned(
            resources, timeout=self.post_destroy_cleanup_timeout,
            destroy_thread=destroy_thread)

        destroy_thread.join(timeout=1800)
        self.assertFalse(
            destroy_thread.is_alive(),
            "CapellaAPI.destroy_cluster did not complete within 1800s")
        self.assertFalse(
            destroy_result["failed"],
            f"Cluster destroy returned an error: {destroy_result['error']}")

        # Default cleanup path (no RetainSnapshotBackups-equivalent available
        # from TAF — see docstring above): ALL of the backup's EBS snapshots
        # (regular and guest-volume) should be gone after destroy.
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
    # Test 9: Destroy of a turned-off cluster
    # ------------------------------------------------------------------

    def test_destroy_while_turning_off(self):
        """
        Destroy a cluster while it is turned off and verify it completes cleanly
        with full AWS resource cleanup.

        Destroy transitions the cluster into a 'destroying' state, which the CP's
        fusion teardown path (couchbase-cloud: fusion/accelerator/accelerator.go
        assertForceTearDown) treats as isDestroying() — forcing the teardown
        regardless of the prior TurningOff/TurnedOff state — rather than hitting
        the ErrTearDownWhileTurningOff fatal error path (which only applies to
        teardown attempts against a cluster that is turning/turned off but NOT
        itself being destroyed, e.g. a periodic reconciliation attempt). Confirmed
        against a live control plane: destroy of a turned-off cluster is an
        allowed operation and succeeds.
        """
        self._load_above_threshold()

        s3_bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)
        resources = {
            "cluster_id": self.cluster.id,
            "s3_bucket_name": s3_bucket_name,
            "iam_profile_name": None,  # No rebalance triggered, no accelerators exist
        }

        dr_on_off = DoctorHostedOnOff(self.pod, self.tenant, self.cluster)
        self.log.info(f"Turning cluster {self.cluster.id} off before destroy attempt")
        turned_off = dr_on_off.turn_off_cluster(timeout=1200)
        self.assertTrue(
            turned_off, f"Cluster {self.cluster.id} did not reach 'turned_off' state")

        self.log.info(f"Destroying cluster {self.cluster.id} while turned off")
        destroy_thread, destroy_result = self._destroy_cluster_async(self.cluster)
        self._assert_all_cluster_resources_cleaned(
            resources, timeout=self.post_destroy_cleanup_timeout,
            destroy_thread=destroy_thread)

        destroy_thread.join(timeout=1800)
        self.assertFalse(
            destroy_thread.is_alive(),
            "CapellaAPI.destroy_cluster did not complete within 1800s")
        self.assertFalse(
            destroy_result["failed"],
            f"Cluster destroy returned an error: {destroy_result['error']}")
        self.log.info(
            "Cluster destroyed while turned off — all AWS resources verified clean")
