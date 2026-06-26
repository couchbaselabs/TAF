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
  1. test_destroy_after_prepare_rebalance       — destroy before accelerators appear (phases 2-3)
  2. test_destroy_during_s3_download            — destroy during S3→EBS hydration (phase 5)
  3. test_destroy_during_file_extent_migration  — destroy when EBS volumes are attached to KV nodes (phase 6)
  4. test_destroy_in_scale_failed_state         — delete S3 bucket mid-phase-5 to force scaleFailed, then destroy
"""

import threading
import time

from aGoodDoctor.workloads import Hotel
from capella_utils.dedicated import CapellaUtils as CapellaAPI
from pytests.basetestcase import BaseTestCase
from aGoodDoctor.hostedOPD import hostedOPD
from bucket_utils.bucket_ready_functions import JavaDocLoaderUtils

from .fusion_aws_util import FusionAWSUtil
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

        self.aws_access_key = self.input.param("aws_access_key", None)
        self.aws_secret_key = self.input.param("aws_secret_key", None)
        if not self.aws_access_key or not self.aws_secret_key:
            raise ValueError("aws_access_key and aws_secret_key are required parameters")
        self.aws_region = self.input.param("region", "us-east-1")

        self.fusion_aws_util = FusionAWSUtil(
            self.aws_access_key, self.aws_secret_key, region=self.aws_region)
        self.fusion_monitor = FusionMonitorUtil(self.log, self.fusion_aws_util,
                                                num_vbuckets=self.input.param("numVBuckets", 128))
        self.cp_monitor = FusionCPResourceMonitor(self.log, self.fusion_aws_util)
        self.s3 = S3Lib(self.aws_access_key, self.aws_secret_key, region=self.aws_region)

        self.sync_wait_timeout = self.input.param("sync_wait_timeout", 1200)
        self.fusion_threshold_gib = self.input.param("fusion_threshold_gib", 10)
        self.num_buckets = self.input.param("num_buckets", 1)
        self.rebalance_timeout = self.input.param("rebalance_timeout", 7200)
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
        return self.fusion_aws_util._cluster_filter(
            cluster_id,
            [{'Name': 'tag:couchbase-cloud-function', 'Values': ['fusion-accelerator']}],
        )

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

    def _assert_all_cluster_resources_cleaned(self, resources, timeout=600):
        """Poll until every AWS resource the CP created for this cluster is gone.

        Checked resources:
          - EBS guest volumes (fusion-accelerator tag)
          - Fusion accelerator EC2 instances
          - Fusion Auto Scaling Groups
          - All cluster EC2 nodes (CBS/KV) in non-terminated state
          - Fusion S3 log-store bucket (if captured)
          - IAM instance profile for accelerator nodes (if captured)

        Intended to run concurrently with _destroy_cluster_async so that
        resource cleanup is observed while the destroy is in progress.
        """
        cluster_id = resources["cluster_id"]
        s3_bucket_name = resources.get("s3_bucket_name")
        iam_profile_name = resources.get("iam_profile_name")
        acc_filter = self._accelerator_filter_for(cluster_id)
        all_nodes_filter = self.fusion_aws_util._cluster_filter(cluster_id)

        deadline = time.time() + timeout
        while time.time() < deadline:
            volumes = self.fusion_aws_util.ec2.list_volumes_by_cluster_id(filters={
                "couchbase-cloud-cluster-id": cluster_id,
                "couchbase-cloud-function": "fusion-accelerator",
            })
            asgs = self.fusion_aws_util.list_cluster_fusion_asg(cluster_id)
            acc_instances = self.fusion_aws_util.list_accelerator_instances(
                acc_filter, log="DestroyCleanup")
            all_nodes = self.fusion_aws_util.list_instances(
                all_nodes_filter, log="ClusterNodeCheck", suppress_log=True)
            running_nodes = [
                n for n in all_nodes
                if n.get("State", {}).get("Name") not in ("terminated", "shutting-down")
            ]
            if not volumes and not asgs and not acc_instances and not running_nodes:
                self.log.info(
                    f"All compute resources cleaned up for cluster {cluster_id}")
                break
            self.log.info(
                f"Cleanup in progress — {len(volumes)} EBS vols, {len(asgs)} ASGs, "
                f"{len(acc_instances)} acc instances, {len(running_nodes)} cluster nodes — "
                f"{int(deadline - time.time())}s remaining")
            time.sleep(15)

        # Final point-in-time assertions across all resource types
        failures = []

        final_volumes = self.fusion_aws_util.ec2.list_volumes_by_cluster_id(filters={
            "couchbase-cloud-cluster-id": cluster_id,
            "couchbase-cloud-function": "fusion-accelerator",
        })
        if final_volumes:
            failures.append(f"{len(final_volumes)} EBS guest volume(s) remain: "
                            f"{[v['VolumeId'] for v in final_volumes]}")

        final_asgs = self.fusion_aws_util.list_cluster_fusion_asg(cluster_id)
        if final_asgs:
            failures.append(f"{len(final_asgs)} ASG(s) remain: "
                            f"{[a['AutoScalingGroupName'] for a in final_asgs]}")

        final_acc = self.fusion_aws_util.list_accelerator_instances(
            acc_filter, log="FinalAccCheck")
        if final_acc:
            failures.append(f"{len(final_acc)} accelerator instance(s) remain: "
                            f"{[i['InstanceId'] for i in final_acc]}")

        all_final_nodes = self.fusion_aws_util.list_instances(
            all_nodes_filter, log="FinalNodeCheck", suppress_log=True)
        running_final = [
            n for n in all_final_nodes
            if n.get("State", {}).get("Name") not in ("terminated", "shutting-down")
        ]
        if running_final:
            failures.append(f"{len(running_final)} cluster node(s) still running: "
                            f"{[n['InstanceId'] for n in running_final]}")

        if s3_bucket_name:
            if self._s3_bucket_still_exists(s3_bucket_name):
                failures.append(f"S3 bucket {s3_bucket_name} still exists")
            else:
                self.log.info(f"S3 bucket {s3_bucket_name} confirmed deleted")

        if iam_profile_name:
            if self.fusion_aws_util.ec2.check_iam_instance_profile_exists(iam_profile_name):
                failures.append(f"IAM instance profile {iam_profile_name} still exists")
            else:
                self.log.info(f"IAM instance profile {iam_profile_name} confirmed deleted")

        self.assertEqual(
            len(failures), 0,
            f"Resources not cleaned up after cluster {cluster_id} destroy:\n"
            + "\n".join(f"  - {f}" for f in failures))
        self.log.info(
            f"All AWS resources verified clean for cluster {cluster_id}")

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
        self._assert_all_cluster_resources_cleaned(resources, timeout=600)

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
        self._assert_all_cluster_resources_cleaned(resources, timeout=600)

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

            # Phase 6: EBS volumes are 'in-use' and attached to a non-accelerator instance
            raw_volumes = self.fusion_aws_util.ec2.list_volumes_by_cluster_id(filters={
                "couchbase-cloud-cluster-id": self.cluster.id,
                "couchbase-cloud-function": "fusion-accelerator",
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
        self._assert_all_cluster_resources_cleaned(resources, timeout=600)

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

        # Log what AWS resources exist in the failed state (informational)
        acc_in_failed = self.fusion_aws_util.list_accelerator_instances(
            self._accelerator_filter(), log="FailedStateResources")
        ebs_in_failed = self.fusion_aws_util.ec2.list_volumes_by_cluster_id(filters={
            "couchbase-cloud-cluster-id": self.cluster.id,
            "couchbase-cloud-function": "fusion-accelerator",
        })
        asgs_in_failed = self.fusion_aws_util.list_cluster_fusion_asg(self.cluster.id)
        self.log.info(
            f"Resources present in scaleFailed state: "
            f"{len(acc_in_failed)} accelerator instances, "
            f"{len(ebs_in_failed)} EBS volumes, "
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
        self._assert_all_cluster_resources_cleaned(resources, timeout=600)

        destroy_thread.join(timeout=1800)
        self.assertFalse(
            destroy_thread.is_alive(),
            "CapellaAPI.destroy_cluster did not complete within 1800s")
        self.assertFalse(
            destroy_result["failed"],
            f"Cluster destroy returned an error: {destroy_result['error']}")
        self.log.info(
            "Cluster destroyed from scaleFailed state — all AWS resources verified clean")
