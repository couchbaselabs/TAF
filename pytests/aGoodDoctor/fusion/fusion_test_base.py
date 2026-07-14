"""
Fusion Test Base

Shared base class for all fusion cloud test suites.  Extracted here so every
fusion test file can import it directly without pulling in the full
FusionEnableDisableTests class hierarchy.
"""

import threading
import time

from TestInput import TestInputSingleton
from aGoodDoctor.workloads import Hotel
from capella_utils.dedicated import CapellaUtils as CapellaAPI
from couchbase_utils.cb_server_rest_util.fusion.fusion_api import FusionRestAPI
from pytests.basetestcase import BaseTestCase
from aGoodDoctor.hostedOPD import hostedOPD
from .fusion_aws_util import FusionAWSUtil, resolve_fusion_aws_credentials
from .fusion_monitor_util import FusionMonitorUtil
from .fusion_cp_resource_monitor import FusionCPResourceMonitor
from .awslib.s3_lib import S3Lib
from bucket_utils.bucket_ready_functions import JavaDocLoaderUtils
from BucketLib.BucketOperations import BucketHelper
from membase.api.rest_client import RestConnection


class _FusionTestBase(BaseTestCase, hostedOPD):
    """Shared base for all fusion lifecycle test classes.

    Not runnable directly — subclasses define test methods.

    Cluster lifecycle
    -----------------
    When no cluster IDs are supplied via the ini file, the first test to run
    deploys a fresh Capella cluster and stores its ID in
    TestInputSingleton.input.capella["clusters"]. Every subsequent setUp
    sees that key and reuses the same cluster instead of deploying a new one.
    After the last test in the run (case_number == no_of_test_identified),
    tearDown removes the key and calls CapellaUtils.destroy_cluster directly
    so the shared cluster is cleaned up exactly once.

    When cluster IDs *are* supplied in the ini, this logic is skipped and
    the cluster is never destroyed by this class (caller owns it).
    """

    DEFAULT_TIMEOUT = FusionMonitorUtil.DEFAULT_TIMEOUT

    # Tracks the cluster we deployed ourselves so tearDownClass can destroy it
    _shared_cluster_ids = None
    _last_pod = None
    _last_tenants = None
    _fusion_intermittent_states = ["enabling", "disabling", "stopping"]
    _fusion_final_states = ["enabled", "disabled", "stopped"]


    def setUp(self):
        if _FusionTestBase._shared_cluster_ids is None and \
                not TestInputSingleton.input.capella.get("clusters"):
            # First test and no pre-existing cluster — let the base class deploy one
            BaseTestCase.setUp(self)
            ids = ",".join(c.id for t in self.tenants for c in t.clusters)
            _FusionTestBase._shared_cluster_ids = ids
            TestInputSingleton.input.capella["clusters"] = ids
        else:
            # Subsequent tests, or pre-existing cluster — reuse via clusters= param
            BaseTestCase.setUp(self)

        # Keep references for tearDownClass (updated each setUp so they stay fresh)
        _FusionTestBase._last_pod = self.pod
        _FusionTestBase._last_tenants = self.tenants

        # Assign tenant/cluster immediately after BaseTestCase.setUp() so that
        # tearDown() can always access them even if subsequent setup steps fail.
        self.tenant = self.tenants[0]
        self.cluster = self.tenant.clusters[0]

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
        self.fusion_monitor = FusionMonitorUtil(
            self.log, self.fusion_aws_util,
            num_vbuckets=self.input.param("numVBuckets", 128))
        self.cp_monitor = FusionCPResourceMonitor(self.log, self.fusion_aws_util)
        self.s3 = S3Lib(self.aws_access_key, self.aws_secret_key,
                         session_token=self.aws_session_token, region=self.aws_region,
                         boto3_session=self.aws_boto3_session)

        self.sync_wait_timeout = self.input.param("sync_wait_timeout", 1200)
        self.fusion_threshold_gib = self.input.param("fusion_threshold_gib", 10)
        self.num_buckets = self.input.param("num_buckets", 1)
        self.fragmentation = int(self.input.param("fragmentation", 50))
        self.rebalance_timeout = self.input.param("rebalance_timeout", 1800)
        self.fusion_infra_timeout = self.input.param("fusion_infra_timeout", 1800)
        self.gv_launch_timeout = self.input.param("gv_launch_timeout", 1200)
        self.hydration_timeout = self.input.param("hydration_timeout", 1800)
        self.load_defn = [Hotel]

        JavaDocLoaderUtils(self.bucket_util, self.cluster_util)
        self.stop_run_event = threading.Event()

        for bucket in self.cluster.buckets:
            if not hasattr(bucket, "loadDefn") or bucket.loadDefn is None:
                bucket.loadDefn = self.load_defn[0]

    def tearDown(self):
        if hasattr(self, "stop_run_event"):
            self.stop_run_event.set()
        self.stop_run = True

        # If we own the shared cluster, check whether it is still usable.
        # A cluster that is not healthy (failed rebalance, turned-off, etc.)
        # cannot be handed to the next test.  Destroy it now and clear the
        # shared-cluster bookkeeping so the next setUp deploys a fresh one.
        if _FusionTestBase._shared_cluster_ids is not None:
            if not self._is_cluster_healthy():
                self.log.warning(
                    f"Cluster {self.cluster.id} is not healthy at tearDown — "
                    "destroying it so the next test starts with a fresh cluster")
                self._destroy_shared_cluster()
            elif (TestInputSingleton.input.test_params["case_number"] ==
                  TestInputSingleton.input.test_params["no_of_test_identified"]):
                # Last test in the suite — normal end-of-run destroy.
                self.log.info(
                    f"Last test complete — destroying shared cluster {self.cluster.id}")
                self._destroy_shared_cluster()

        BaseTestCase.tearDown(self)

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _is_cluster_healthy(self, timeout=120):
        """
        Return True if the cluster is in a healthy, usable state.

        Waits up to *timeout* seconds for any transitional state (scaling,
        rebalancing, turning on/off) to settle.  Returns False immediately
        for terminal non-healthy states.  Called from tearDown to decide
        whether to recycle the cluster.
        """
        _terminal_unhealthy = {
            "turnedOff", "scaleFailed", "rebalanceFailed",
            "deploymentFailed", "redeploymentFailed", "scalingFailed",
            "unhealthy",
        }
        if not hasattr(self, "tenant") or not hasattr(self, "cluster"):
            return False
        try:
            deadline = time.time() + timeout
            while time.time() < deadline:
                state = CapellaAPI.get_cluster_state(
                    self.pod, self.tenant, self.cluster.id)
                if state == "healthy":
                    return True
                if state in _terminal_unhealthy:
                    self.log.warning(
                        f"Cluster {self.cluster.id} is in non-healthy state: {state}")
                    return False
                self.log.info(
                    f"Cluster {self.cluster.id} in transitional state '{state}' — "
                    f"waiting for stable state ({int(deadline - time.time())}s left)")
                time.sleep(10)
            self.log.warning(
                f"Cluster {self.cluster.id} did not reach a stable state "
                f"within {timeout}s — treating as unusable")
            return False
        except Exception as e:
            self.log.error(
                f"Failed to check cluster state for {self.cluster.id}: {e}")
            return False

    def _destroy_shared_cluster(self):
        """
        Destroy the shared cluster and clear all shared-cluster bookkeeping.

        After this call the next setUp will hit the first-test branch and
        deploy a fresh cluster.
        """
        tenant = _FusionTestBase._last_tenants[0]
        cluster = tenant.clusters[0]
        try:
            CapellaAPI.destroy_cluster(
                _FusionTestBase._last_pod, tenant, cluster)
            self.log.info(f"Cluster {cluster.id} destroyed successfully")
        except Exception as e:
            self.log.error(f"Failed to destroy cluster {cluster.id}: {e}")
        TestInputSingleton.input.capella.pop("clusters", None)
        _FusionTestBase._shared_cluster_ids = None
        _FusionTestBase._last_pod = None
        _FusionTestBase._last_tenants = None

    def _delete_bucket_with_s3_cleanup(self, bucket, timeout=300):
        """Delete a Couchbase bucket and assert its S3 prefix (kv/<uuid>) is
        removed by the control plane within timeout seconds.
        """
        s3_bucket_name = self._get_s3_bucket_name_from_uri(self.cluster)
        if s3_bucket_name and not (hasattr(bucket, "bucket_uuid") and bucket.bucket_uuid is not None):
            try:
                rest = RestConnection(self.cluster.master)
                info = rest.get_bucket_details(bucket_name=bucket.name)
                bucket.bucket_uuid = info.get("uuid", None)
            except Exception as e:
                self.log.warning(
                    f"Could not fetch UUID for bucket {bucket.name}: {e}")

        # Ensure cluster is healthy before attempting delete — Capella rejects
        # bucket deletes when the cluster is still in a post-rebalance transition.
        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id,
            msg=f"Wait for healthy cluster before deleting bucket {bucket.name}",
            timeout=600)

        # Retry the delete up to 5 times with backoff — the API can transiently
        # return non-204 even after wait_until_done if the CP is still settling.
        for attempt in range(1, 6):
            try:
                CapellaAPI.delete_bucket(self.pod, self.tenant, self.cluster, bucket.name)
                break
            except Exception as e:
                if attempt == 5:
                    raise
                self.log.warning(
                    f"delete_bucket attempt {attempt}/5 failed for "
                    f"'{bucket.name}': {e} — retrying in {10 * attempt}s")
                time.sleep(10 * attempt)

        # Wait for ns_server to finish the deletion before returning.
        # Capella reports "healthy" before ns_server completes the removal, and
        # ns_server rejects new bucket creation while a deletion is still in flight:
        #   "Cannot create bucket during the bucket deletion"
        self.log.info(f"Waiting for ns_server to finish deleting bucket {bucket.name}")
        deletion_deadline = time.time() + timeout
        while time.time() < deletion_deadline:
            try:
                existing_names = [b["name"] for b in BucketHelper(self.cluster.master).get_buckets_json()]
            except Exception as e:
                self.log.warning(f"Exception while fetching buckets: {e}")
                existing_names = [bucket.name]
            if bucket.name not in existing_names:
                self.log.info(f"Bucket {bucket.name} confirmed gone from ns_server")
                break
            self.log.info(f"Bucket {bucket.name} still present in ns_server — waiting")
            time.sleep(5)
        else:
            self.log.warning(
                f"Bucket {bucket.name} did not disappear from ns_server within {timeout}s")

        if s3_bucket_name and bucket.bucket_uuid:
            prefix = f"kv/{bucket.bucket_uuid}"
            self.log.info(
                f"Waiting for S3 prefix {prefix} to be removed from {s3_bucket_name}")
            deadline = time.time() + timeout
            while time.time() < deadline:
                files = self.s3.list_files_in_bucket(s3_bucket_name, prefix=prefix)
                if not files:
                    self.log.info(
                        f"S3 prefix {prefix} removed for bucket {bucket.name}")
                    return
                self.log.info(
                    f"S3 prefix {prefix} still has {len(files)} object(s) — waiting")
                time.sleep(15)
            remaining = self.s3.list_files_in_bucket(s3_bucket_name, prefix=prefix)
            self.assertEqual(
                len(remaining), 0,
                f"S3 prefix {prefix} was not cleaned up within {timeout}s after "
                f"deleting bucket {bucket.name} — {len(remaining)} object(s) remain")

    def _load_data(self, cluster,
                   buckets=None,
                   doc_ops=["create"],
                   create_start=None,
                   create_end=None,
                   read_start=None,
                   read_end=None,
                   update_start=None,
                   update_end=None,
                   delete_start=None,
                   delete_end=None,
                   overRidePattern={"create": 100, "read": 0, "update": 0, "delete": 0, "expiry": 0},
                   wait_for_load=True):
        """Load initial dataset into all cluster buckets via JavaDocLoaderUtils.
        Volume is determined by bucket.loadDefn.get("num_items"), which is set
        from test parameters (e.g. -p num_items=1000000). Tests that need data
        above or below the fusion threshold should be configured accordingly.
        """
        if not cluster.buckets:
            tenant = next(
                (t for t in self.tenants for c in t.clusters if c.id == cluster.id), None
            )
            if tenant:
                self.log.info(f"No buckets found on cluster {cluster.id} — creating before load")
                self.create_buckets(self.pod, tenant, cluster)
        buckets = buckets or cluster.buckets
        for bucket in buckets:
            bucket_create_end = create_end if create_end is not None else bucket.loadDefn.get("num_items")
            JavaDocLoaderUtils.generate_docs(
                doc_ops=doc_ops,
                create_start=create_start if create_start is not None else 0,
                create_end=bucket_create_end,
                read_start=read_start,
                read_end=read_end,
                update_start=update_start,
                update_end=update_end,
                delete_start=delete_start,
                delete_end=delete_end,
                bucket=bucket
            )
        tasks = JavaDocLoaderUtils.perform_load(
            cluster=cluster,
            buckets=buckets,
            overRidePattern=overRidePattern,
            wait_for_load=wait_for_load,
            validate_data=False,
            wait_for_stats=False
        )

    def _enable_fusion_feature_flags(self, tenant, cluster_id=None):
        # Fusion feature flags are honored at the TENANT level, not the
        # cluster level (AV-136926, closed as designed) — set them on the
        # tenant. cluster_id is kept for caller compatibility and unused.
        CapellaAPI.create_tenant_feature_flag(
            self.pod, tenant, "fusion-rebalances", True)
        CapellaAPI.create_tenant_feature_flag(
            self.pod, tenant, "fusion-fallback-replace", True)

    def _ensure_fusion_state(self, tenant, cluster, target):
        """Idempotently transition fusion to target state ('enabled' or 'disabled').

        Waits for any in-progress transition to settle first, then transitions
        only if the current state differs from target.
        """
        # A disabling transition purges the whole S3 log store, which can take
        # well over 2 minutes — give in-flight transitions room to settle.
        deadline = time.time() + 600
        current = "unknown"
        while time.time() < deadline:
            status = CapellaAPI.get_fusion_status(self.pod, tenant, cluster.id)
            current = status.get("state", "disabled")
            if current in self._fusion_final_states:
                break
            self.log.info(
                f"Waiting for stable state on {cluster.id} (current: {current})")
            time.sleep(10)

        if current == target:
            self.log.info(f"Cluster {cluster.id} already in '{target}' state")
            return

        self.log.info(f"Transitioning cluster {cluster.id}: {current} → {target}")
        if target == "enabled":
            resp = CapellaAPI.enable_fusion(self.pod, tenant, cluster.id)
            self.assertEqual(resp.status_code, 200,
                             f"enable_fusion during _ensure_fusion_state "
                             f"returned {resp.status_code}: {resp.content}")
            self._wait_for_fusion_state(tenant, cluster, "enabled")
        elif target == "disabled":
            resp = CapellaAPI.disable_fusion(self.pod, tenant, cluster.id)
            self.assertEqual(resp.status_code, 200,
                             f"disable_fusion during _ensure_fusion_state "
                             f"returned {resp.status_code}: {resp.content}")
            self._wait_for_fusion_state(tenant, cluster, "disabled")

    def _get_s3_bucket_name_from_uri(self, cluster):
        """Parse the S3 bucket name from ep_magma_fusion_logstore_uri."""
        uri = self.fusion_monitor.get_fusion_s3_uri(cluster)
        if not uri:
            return None
        return uri.replace("s3://", "").split("?")[0]

    def _wait_for_s3_uri(self, cluster, timeout=120):
        """Poll until ep_magma_fusion_logstore_uri is set on the cluster.

        The stat is only populated after a fusion-enrolled bucket exists.
        Callers must ensure at least one bucket has been created before calling this.
        Returns the URI string, or raises AssertionError on timeout.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            uri = self.fusion_monitor.get_fusion_s3_uri(cluster)
            if uri:
                self.log.info(f"Fusion S3 URI available on cluster {cluster.id}: {uri}")
                return uri
            self.log.info(f"Waiting for S3 URI on cluster {cluster.id} (bucket must exist)…")
            time.sleep(10)
        raise AssertionError(
            f"Fusion S3 URI not set on cluster {cluster.id} within {timeout}s — "
            "ensure a fusion-enrolled bucket exists before checking the URI"
        )

    def _wait_for_s3_data_synced(self, cluster, timeout=600):
        """Wait until every bucket's kv/<uuid> S3 prefix has at least one object.

        Use this instead of _wait_for_pending_bytes_zero when the goal is simply
        to confirm that data has reached S3 before a flush/drop/replica-change.
        It avoids the Magma background-compaction false-negative where pending
        bytes never reach exactly 0 even after the initial load is fully uploaded.
        """
        s3_bucket_name = self._get_s3_bucket_name_from_uri(cluster)
        if not s3_bucket_name:
            self.log.warning(
                "Cannot verify S3 sync — S3 bucket name not resolved; skipping wait")
            return
        for bucket in cluster.buckets:
            uuid = getattr(bucket, "bucket_uuid", None)
            if not uuid:
                self.log.warning(
                    f"Bucket '{bucket.name}' has no UUID — skipping S3 sync check")
                continue
            prefix = f"kv/{uuid}"
            deadline = time.time() + timeout
            while time.time() < deadline:
                files = self.s3.list_files_in_bucket(s3_bucket_name, prefix=prefix)
                if files:
                    self.log.info(
                        f"Bucket '{bucket.name}': {len(files)} objects under "
                        f"{prefix} — S3 sync confirmed")
                    break
                self.log.info(
                    f"No S3 objects yet under {prefix} — "
                    f"{int(deadline - time.time())}s remaining")
                time.sleep(15)
            else:
                raise AssertionError(
                    f"No S3 objects found under prefix {prefix} within {timeout}s "
                    f"— data may not have synced")

    def _wait_for_pending_bytes_zero(self, cluster, timeout=600,
                                     stable_threshold_bytes=0, stable_secs=60):
        """Poll fusion pending bytes until they reach 0 or timeout.

        stable_threshold_bytes > 0: also succeed when pending bytes have stayed
        ≤ stable_threshold_bytes for stable_secs consecutive seconds.  Use this
        for post-compaction waits where Magma background I/O generates a steady
        trickle (~190 MB) that prevents an exact-zero reading.
        """
        self.fusion_monitor.set_admin_credentials(cluster)
        deadline = time.time() + timeout
        below_threshold_since = None
        while time.time() < deadline:
            status, content = FusionRestAPI(cluster.master).get_fusion_status()
            if not status:
                time.sleep(10)
                continue
            nodes = content.get("nodes") or {}
            total_pending = sum(
                bucket_stats.get("snapshotPendingBytes", 0)
                for node_stats in nodes.values()
                for bucket_stats in (node_stats.get("buckets") or {}).values()
            )
            self.log.info(f"Cluster {cluster.id} total pending bytes: {total_pending}")
            if total_pending == 0:
                return
            if stable_threshold_bytes > 0 and total_pending <= stable_threshold_bytes:
                if below_threshold_since is None:
                    below_threshold_since = time.time()
                elif time.time() - below_threshold_since >= stable_secs:
                    self.log.info(
                        f"Pending bytes stable at {total_pending} "
                        f"(≤ {stable_threshold_bytes} B threshold) "
                        f"for {stable_secs}s — treating as synced")
                    return
            else:
                below_threshold_since = None
            time.sleep(15)
        raise AssertionError(
            f"Pending bytes did not reach 0 within {timeout}s on cluster {cluster.id}")

    def _assert_s3_bucket_exists(self, bucket_name):
        try:
            self.s3.s3_client.head_bucket(Bucket=bucket_name)
            self.log.info(f"S3 bucket {bucket_name} exists as expected")
        except Exception:
            self.fail(f"Expected S3 bucket {bucket_name} to exist but it does not")

    def _assert_s3_bucket_deleted(self, bucket_name, timeout=120):
        """Poll until the S3 bucket no longer exists."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                self.s3.s3_client.head_bucket(Bucket=bucket_name)
                self.log.info(f"S3 bucket {bucket_name} still exists, waiting...")
                time.sleep(10)
            except Exception:
                self.log.info(f"S3 bucket {bucket_name} confirmed deleted")
                return
        self.fail(f"S3 bucket {bucket_name} was not deleted within {timeout}s")

    def _get_s3_object_count_for_buckets(self, s3_bucket_name, buckets=None):
        """Count S3 objects scoped to kv/<uuid> prefixes for the given CB buckets.

        Fetches each bucket's UUID on demand (cached on bucket.bucket_uuid) and
        sums the object counts under their individual S3 prefixes.  Pass this
        instead of a raw get_bucket_size() call whenever you want to isolate
        counts to the test's own buckets and avoid noise from other tests or
        previous runs sharing the same cluster-level S3 bucket.
        """
        if buckets is None:
            buckets = self.cluster.buckets
        total = 0
        for bucket in buckets:
            if not getattr(bucket, "bucket_uuid", None):
                try:
                    rest = RestConnection(self.cluster.master)
                    info = rest.get_bucket_details(bucket_name=bucket.name)
                    bucket.bucket_uuid = info.get("uuid", None)
                except Exception as e:
                    self.log.warning(
                        f"Could not fetch UUID for bucket {bucket.name}: {e}")
            if bucket.bucket_uuid:
                prefix = f"kv/{bucket.bucket_uuid}"
                count = self.s3.get_bucket_size(
                    s3_bucket_name, prefix=prefix).get("file_count", 0)
                self.log.debug(
                    f"S3 objects under {prefix} in {s3_bucket_name}: {count}")
                total += count
        return total

    def _assert_s3_bucket_empty(self, bucket_name, timeout=120, buckets=None):
        """Assert that S3 objects for the test's buckets have been deleted.

        When *buckets* is provided the check is scoped to their kv/<uuid>
        prefixes — which avoids false failures caused by orphaned objects from
        other tests sharing the same cluster-level S3 bucket.  When *buckets*
        is None the whole bucket is checked (legacy behaviour, kept for callers
        that have not been updated yet).
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            if buckets is not None:
                count = self._get_s3_object_count_for_buckets(bucket_name, buckets)
            else:
                count = self.s3.get_bucket_size(bucket_name).get("file_count", 0)
            self.log.info(f"S3 bucket {bucket_name} object count: {count}")
            if count == 0:
                return
            time.sleep(10)
        self.fail(f"S3 bucket {bucket_name} still has objects after {timeout}s")

    def _wait_for_fusion_state(self, tenant, cluster, state, timeout=None):
        """Wait for fusion to reach `state` then confirm the cluster is healthy."""
        if timeout is None:
            timeout = self.DEFAULT_TIMEOUT
        self.fusion_monitor.wait_for_fusion_status(cluster, state=state, timeout=timeout)
        deadline = time.time() + 600
        cluster_state = "unknown"
        while time.time() < deadline:
            cluster_state = CapellaAPI.get_cluster_state(self.pod, tenant, cluster.id)
            self.log.info(f"Cluster {cluster.id} state after fusion→{state}: {cluster_state}")
            if cluster_state.lower() == "healthy":
                return
            time.sleep(10)
        raise AssertionError(
            f"Cluster {cluster.id} did not reach healthy state within 600s "
            f"after fusion transitioned to '{state}' (last state: {cluster_state})")

    def _get_active_guest_volume_count(self, cluster):
        self.fusion_monitor.set_admin_credentials(cluster)
        status, content = FusionRestAPI(cluster.master).get_active_guest_volumes()
        self.assertTrue(status,
                        f"Failed to fetch active guest volumes: {content}")
        # Response is a dict keyed by node, each value a list of guest volumes
        if isinstance(content, dict):
            return sum(len(vols or []) for vols in content.values())
        return len(content) if isinstance(content, list) else 0
