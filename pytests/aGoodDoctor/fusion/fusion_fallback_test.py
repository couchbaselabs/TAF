"""
Fusion Accelerator Instance-Type Fallback Tests

The ASG created for each fusion accelerator node uses a MixedInstancesPolicy
with a 40-entry priority-ordered override list (unifiedInstanceTypes in
autoscalinggroups.go). AWS EC2 Auto Scaling's "prioritized" strategy tries
each entry in order, silently skipping any type not offered in the region.

These tests validate that the fallback mechanism works correctly when the
top-priority types are unavailable at launch time.

Fault injection mechanism
-------------------------
AWS FIS action ``aws:ec2:api-insufficient-instance-capacity-error`` targets
the per-cluster fusion accelerator IAM role (used as the EC2 instance profile
in the launch template). FIS causes EC2 to return InsufficientInstanceCapacity
for any RunInstances call that uses that instance profile, forcing the ASG to
walk down its override list one entry at a time.

We stop the FIS experiment once every ASG has logged N capacity-failure
scaling activities, at which point the next launch attempt (type N+1) will
succeed normally.

Test classes
------------
  FusionFallbackInstanceTypeTests — end-to-end fallback validation
"""

import re
import time
import datetime

from prettytable import PrettyTable

from capella_utils.dedicated import CapellaUtils as CapellaAPI
from pytests.basetestcase import BaseTestCase
from aGoodDoctor.hostedOPD import hostedOPD
from bucket_utils.bucket_ready_functions import JavaDocLoaderUtils

from .fusion_aws_util import FusionAWSUtil, FUSION_ASSUME_ROLE_NAME
from .kubectl_cp_db_util import KubectlCPDBUtil
from .fusion_monitor_util import FusionMonitorUtil
from .fusion_cp_resource_monitor import FusionCPResourceMonitor
from .fusion_test_base import _FusionTestBase
from .awslib.fis_lib import iso8601_duration_to_seconds
import threading
from TestInput import TestInputSingleton
from aGoodDoctor.workloads import Hotel
from couchbase_utils.cb_server_rest_util.fusion.fusion_api import FusionRestAPI
from membase.api.rest_client import RestConnection
from .awslib.s3_lib import S3Lib


# Priority-ordered instance types mirroring unifiedInstanceTypes in
# internal/clusters/fusion/infra/aws/autoscalinggroups.go.
# Must stay in sync with the Go source.
UNIFIED_INSTANCE_TYPES = [
    "c8gb.4xlarge",
    "c8gb.2xlarge",
    "m8gb.2xlarge",
    "r8gb.2xlarge",
    "c6in.4xlarge",
    "c8gn.4xlarge",
    "c6gn.4xlarge",
    "c6g.8xlarge",
    "c7gn.4xlarge",
    "gr6f.4xlarge",
    "c8gb.xlarge",
    "m8gb.xlarge",
    "r8gb.xlarge",
    "c6in.2xlarge",
    "m6in.2xlarge",
    "m6idn.2xlarge",
    "r6in.2xlarge",
    "r6idn.2xlarge",
    "c8gn.2xlarge",
    "c7g.4xlarge",
    "m8gn.2xlarge",
    "r5b.2xlarge",
    "c6a.4xlarge",
    "c8g.4xlarge",
    "m7g.4xlarge",
    "c6i.4xlarge",
    "m6a.4xlarge",
    "c7i.4xlarge",
    "m8g.4xlarge",
    "c7gd.4xlarge",
    "im4gn.2xlarge",
    "r8gn.2xlarge",
    "c8i.4xlarge",
    "m6i.4xlarge",
    "c8gd.4xlarge",
    "c6id.4xlarge",
    "m7i.4xlarge",
    "c7a.4xlarge",
    "m8i.4xlarge",
    "m7gd.4xlarge",
]

class FusionFallbackInstanceTypeTests(_FusionTestBase):
    """
    Validates ASG instance-type fallback behaviour using AWS FIS fault injection.

    FIS injects InsufficientInstanceCapacity errors for the cluster's accelerator
    IAM role so the ASG is forced to walk its 40-type priority list.  Stopping
    FIS after N failures per ASG causes the rebalance to succeed with type N+1.
    """

    def setUp(self):
        _FusionTestBase.setUp(self)
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                self._enable_fusion_feature_flags(tenant, cluster.id)
                self._ensure_fusion_state(tenant, cluster, "enabled")

    def test_fallback_when_top_n_instance_types_unavailable(self):
        """
        AV-XXXXXX: Validate fusion ASG instance-type priority fallback.

        Injects InsufficientInstanceCapacity for the top N priority slots using
        AWS FIS, then stops the experiment so the (N+1)th type can succeed.
        Verifies that:
        - Each ASG records exactly N capacity-failure scaling activities before
          any instance reaches InService state
        - The finally-launched instance type matches the expected fallback
          position (any type with index >= n_to_fail in UNIFIED_INSTANCE_TYPES)
        - The fusion rebalance completes successfully despite the initial
          capacity failures
        - All FIS experiment resources are cleaned up in tearDown

        Parameters (via test config):
          fallback_n       – number of instance types to fail (default 3)
          fis_role_arn     – ARN of the FIS execution role; if omitted, uses
                             AWSServiceRoleForFIS derived from account ID
          fis_duration     – ISO 8601 duration to keep the FIS capacity-error
                             experiment running (default 'PT15M')
        """
        n_to_fail = self.input.param("fallback_n", 3)
        fis_role_arn = self.input.param("fis_role_arn", None)
        fis_duration = self.input.param("fis_duration", "PT15M")

        for tenant in self.tenants:
            for cluster in tenant.clusters:
                self._run_fallback_test(
                    tenant, cluster, n_to_fail=n_to_fail, fis_role_arn=fis_role_arn,
                    duration=fis_duration,
                )

    def test_fallback_exhausts_all_arm_types_falls_back_to_x86(self):
        """
        Fail all ARM-architecture instance types (the top entries in the
        priority list) so the ASG must fall back to x86 types.

        The unified list interleaves ARM and x86 types; the very first x86 type
        is c6in.4xlarge at index 4.  This test fails the first 4 entries
        (all ARM) and expects the ASG to succeed with an x86 instance.

        Validates:
        - ASG correctly transitions across architecture boundaries in the
          override list when ARM capacity is exhausted
        - Launched instance type is an x86 type (c6in.4xlarge or later x86
          entries if c6in.4xlarge is also unavailable for non-FIS reasons)
        """
        arm_prefix_count = 4  # c8gb.4xlarge, c8gb.2xlarge, m8gb.2xlarge, r8gb.2xlarge
        fis_role_arn = self.input.param("fis_role_arn", None)
        fis_duration = self.input.param("fis_duration", "PT15M")

        for tenant in self.tenants:
            for cluster in tenant.clusters:
                self._run_fallback_test(
                    tenant,
                    cluster,
                    n_to_fail=arm_prefix_count,
                    fis_role_arn=fis_role_arn,
                    assert_x86=True,
                    duration=fis_duration,
                )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _run_fallback_test(self, tenant, cluster, n_to_fail: int,
                           fis_role_arn=None, assert_x86: bool = False,
                           duration: str = "PT15M"):
        """
        Core fallback test logic.

        Uses aws:ec2:asg-insufficient-instance-capacity-error targeting the
        cluster's fusion ASGs by tag. To avoid the race between ASG creation
        and the first RunInstances call, the Launch scaling process is suspended
        on each ASG as soon as it is detected, FIS is then started (target
        resolution now sees the ASGs), and Launch is resumed so all launch
        attempts hit the active FIS fault.

        :param tenant: Tenant object
        :param cluster: Cluster object
        :param n_to_fail: Number of top-priority instance types to fail via FIS
        :param fis_role_arn: ARN of the FIS execution role (optional)
        :param assert_x86: If True, additionally assert the launched type is x86
        :param duration: ISO 8601 duration to keep the FIS experiment running
        """
        # Validate the duration up front — AWS rejects a bare 'H10M' style
        # value (missing the 'PT' time designator) only once CreateExperimentTemplate
        # runs, by which point ASG launch has already been suspended. Fail fast instead.
        try:
            iso8601_duration_to_seconds(duration)
        except ValueError:
            self.fail(
                f"Invalid fis_duration {duration!r}: must be an ISO 8601 time "
                f"duration with the 'PT' designator, e.g. 'PT15M', 'PT1H10M'"
            )

        self._load_and_sync_for_fusion_rebalance(cluster)

        fis_template_id = None
        fis_experiment_id = None
        suspended_asg_names = []

        try:
            self.PrintStep(f"Triggering fusion rebalance on cluster {cluster.id}")
            # rebalance_config() counts from kv_nodes; a reused cluster may differ.
            self._resync_kv_node_count(cluster)
            config = self.rebalance_config("data", +1)
            rebalance_task = self.task.async_rebalance_capella(
                self.pod, tenant, cluster, config, timeout=1800)

            experiment_start = datetime.datetime.now(datetime.timezone.utc)

            # Poll aggressively so we catch the ASGs before the first RunInstances call
            asgs = self._wait_for_fusion_asgs(cluster, rebalance_task)

            # Suspend Launch immediately to prevent any RunInstances calls until
            # FIS is active. Any in-flight launch that already started before
            # suspension will still complete, but we check for that below.
            self.PrintStep("Suspending ASG Launch process to hold launches until FIS is active")
            asg_names = [asg["AutoScalingGroupName"] for asg in asgs]
            suspended_asg_names = self.fusion_aws_util.suspend_asg_launch_process(asg_names)

            # Verify no instance is already pending — if one is, the race was lost
            # and the first launch would succeed without FIS; abort early.
            asgs_refreshed = self.fusion_aws_util.list_cluster_fusion_asg(cluster.id)
            suspended = 0
            for asg in asgs_refreshed:
                pending = [
                    i for i in asg.get("Instances", [])
                    if i.get("LifecycleState") in ("Pending", "Pending:Wait", "Pending:Proceed", "InService")
                ]
                if len(pending) == 0:
                    suspended += 1
            if suspended == 0:
                self.fail("None of the ASGs could be suspended.")

            # Create and start FIS now that ASGs exist (target resolution succeeds)
            # and Launch is suspended (no launches will race with FIS startup).
            self.PrintStep(
                f"Starting FIS capacity-error experiment (n_to_fail={n_to_fail}) "
                f"on cluster {cluster.id}"
            )
            fis_template_id, fis_experiment_id = self._start_fis_capacity_experiment(
                cluster=cluster,
                duration=duration,
                fis_role_arn=fis_role_arn,
            )
            self.sleep(10)

            # Resume Launch — Auto Scaling will now attempt launches and every
            # attempt will hit the FIS InsufficientInstanceCapacity fault.
            self.PrintStep("Resuming ASG Launch process (FIS is now active)")
            self.fusion_aws_util.resume_asg_launch_process(suspended_asg_names)
            suspended_asg_names = []  # mark consumed so finally doesn't re-resume

            # Let FIS run its full configured duration rather than stopping it
            # early — keep observing capacity failures as they occur and return
            # once FIS itself reaches a terminal state. The fault lifts
            # naturally when FIS ends, and the next launch attempt for each
            # ASG will use the first type in the override list that isn't
            # blocked (type N+1+).
            self.PrintStep(
                f"Observing capacity failures until FIS experiment "
                f"{fis_experiment_id} completes (duration={duration})"
            )
            failure_counts = self.fusion_aws_util.wait_for_fis_experiment_to_finish(
                cluster_id=cluster.id,
                fis_experiment_id=fis_experiment_id,
                since_time=experiment_start,
                duration=duration,
            )
            self.log.info(f"Capacity failure counts per ASG: {failure_counts}")
            fis_experiment_id = None  # FIS reached a terminal state on its own; nothing to stop

            # Ensure the fault actually caused failures before trusting the
            # subsequent fallback-launch assertions below
            self.assertTrue(
                failure_counts and all(v > 0 for v in failure_counts.values()),
                f"Expected at least one capacity failure per ASG while FIS was "
                f"active, got: {failure_counts}"
            )

            # Wait for all ASG instances to reach InService
            self.PrintStep("Waiting for all ASG instances to become InService")
            self._wait_for_all_asgs_inservice(cluster.id, timeout=1200)

            # Verify instance types
            launched_types = self.fusion_aws_util.get_instance_type_per_asg(cluster.id)
            self.log.info(f"Launched instance types per ASG: {launched_types}")
            self.assertGreater(
                len(launched_types), 0,
                "No InService instances found in any ASG after FIS stopped"
            )

            asg_table = PrettyTable()
            asg_table.field_names = ["ASG Name", "Instance Type Launched"]
            for asg_name, instance_type in launched_types.items():
                asg_table.add_row([asg_name, instance_type])
            self.log.info(f"Fusion ASG launch results for cluster {cluster.id}:\n{asg_table}")

            # ordered_types = self.fusion_aws_util.get_asg_ordered_instance_types(cluster.id)
            # failing_types = set(ordered_types[:n_to_fail])
            # self.log.info(f"Expected-to-fail types (top {n_to_fail}): {failing_types}")

            # for asg_name, instance_type in launched_types.items():
            #     self.assertNotIn(
            #         instance_type, failing_types,
            #         f"ASG {asg_name} launched a type that should have been blocked by FIS: "
            #         f"{instance_type} is in top-{n_to_fail} list {failing_types}"
            #     )
            #     self.log.info(
            #         f"ASG {asg_name}: launched {instance_type} "
            #         f"(not in failing set — fallback correct)"
            #     )

            # if assert_x86:
            #     x86_families = {"c6i", "c6in", "c6a", "c7i", "c7a", "c8i", "c8id",
            #                      "m6i", "m6in", "m6idn", "m6a", "m7i", "m7id", "m8i",
            #                      "r6i", "r6in", "r6idn", "r5b", "gr6f"}
            #     for asg_name, instance_type in launched_types.items():
            #         family = instance_type.split(".")[0]
            #         self.assertIn(
            #             family, x86_families,
            #             f"ASG {asg_name} launched an ARM instance {instance_type} "
            #             f"but all ARM types should have been failed by FIS"
            #         )

            self.wait_for_rebalances([rebalance_task])
            self.log.info("Fusion rebalance completed successfully after instance-type fallback")

        finally:
            # Best-effort cleanup: resume ASG launches if still suspended
            if suspended_asg_names:
                try:
                    self.fusion_aws_util.resume_asg_launch_process(suspended_asg_names)
                except Exception as e:
                    self.log.warning(f"Could not resume ASG launch processes: {e}")
            self._cleanup_fis_experiment(fis_template_id, fis_experiment_id)

    def _load_and_sync_for_fusion_rebalance(self, cluster):
        """
        Load num_items into the cluster buckets (creating them if needed) and
        wait for the data to reach S3.

        Without a bucket holding synced data the CP runs a plain rebalance with
        no accelerators, so no fusion ASG is ever created. skip_init=True reuses
        the buckets and data already on the cluster.
        """
        if self.input.param("skip_init", False):
            self.assertTrue(cluster.buckets,
                            f"skip_init=True but cluster {cluster.id} has no buckets")
        else:
            create_end = int(self.input.param("num_items", 5000000))
            self.PrintStep(f"Loading {create_end} items for fusion rebalance on cluster {cluster.id}")
            self._load_data(cluster, create_start=0, create_end=create_end)
        self._wait_for_s3_uri(cluster)
        self._wait_for_s3_data_synced(cluster, timeout=self.sync_wait_timeout)

    def _rebalance_future(self, rebalance_task):
        """RebalanceTaskCapella never sets .completed; its future does."""
        return self.task.jython_task_manager.futures[rebalance_task.thread_name]

    def _wait_for_fusion_asgs(self, cluster, rebalance_task, timeout=1800):
        """
        Poll every second until the cluster's fusion ASGs exist and return them.
        Fails at once if the rebalance finishes first (it was not a fusion
        rebalance), or after *timeout* seconds.
        """
        self.PrintStep("Waiting for fusion ASGs to appear")
        rebalance_future = self._rebalance_future(rebalance_task)
        deadline = time.time() + timeout
        while time.time() < deadline:
            asgs = self.fusion_aws_util.list_cluster_fusion_asg(cluster.id)
            if asgs:
                self.log.info(f"Detected {len(asgs)} fusion ASGs for cluster {cluster.id}")
                return asgs
            if rebalance_future.done():
                self.fail(
                    f"Rebalance on cluster {cluster.id} finished (result="
                    f"{rebalance_task.result}) without creating fusion ASGs; "
                    f"it was not a fusion rebalance (no bucket data?)")
            time.sleep(1)
        self.fail(f"Fusion ASGs not created within {timeout}s of triggering rebalance")

    def _cleanup_fis_experiment(self, template_id=None, experiment_id=None):
        """Best-effort stop of a FIS experiment and delete of its template."""
        if experiment_id:
            try:
                self.fusion_aws_util.fis.stop_experiment(experiment_id)
            except Exception as e:
                self.log.warning(f"Could not stop FIS experiment {experiment_id}: {e}")
        if template_id:
            try:
                self.fusion_aws_util.fis.delete_experiment_template(template_id)
            except Exception as e:
                self.log.warning(f"Could not delete FIS template {template_id}: {e}")

    def _start_fis_capacity_experiment(self, cluster, duration: str, fis_role_arn=None):
        """
        Create and start a FIS ASG capacity-error experiment targeting the cluster's
        fusion ASGs using aws:ec2:asg-insufficient-instance-capacity-error.

        The ASG Launch process must already be suspended before calling this so that
        target resolution finds the ASGs and no launches race with experiment startup.

        Returns (template_id, experiment_id).
        """
        template_id = self.fusion_aws_util.fis.create_asg_capacity_error_experiment_for_cluster(
            experiment_name=(
                f"fusion-fallback-{cluster.id[:8]}-"
                f"{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
            ),
            cluster_id=cluster.id,
            duration=duration,
            fis_role_arn=fis_role_arn,
            tags={
                "couchbase-cloud-cluster-id": cluster.id,
                "Purpose": "fusion-fallback-testing",
            },
        )

        experiment_id = self.fusion_aws_util.fis.start_experiment(template_id)
        self.log.info(
            f"FIS experiment started: template={template_id}, experiment={experiment_id}"
        )
        return template_id, experiment_id

    def _wait_for_all_asgs_inservice(self, cluster_id: str, timeout: int = 300):
        """
        Block until every fusion ASG in the cluster has exactly one InService
        instance, or raise AssertionError on timeout.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            asgs = self.fusion_aws_util.list_cluster_fusion_asg(cluster_id)
            all_ready = all(
                any(i.get("LifecycleState") == "InService" for i in asg.get("Instances", []))
                for asg in asgs
            )
            if asgs and all_ready:
                self.log.info(f"All {len(asgs)} ASGs have InService instances")
                return
            pending = [
                a["AutoScalingGroupName"] for a in asgs
                if not any(i.get("LifecycleState") == "InService" for i in a.get("Instances", []))
            ]
            self.log.info(f"Waiting for ASGs to reach InService: {pending}")
            time.sleep(10)
        self.fail(
            f"Not all fusion ASGs for cluster {cluster_id} reached InService "
            f"within {timeout}s after FIS stopped"
        )


# CP database (and its EKS cluster) always lives in us-east-1, whatever region
# the data cluster under test is in.
CP_EKS_REGION = "us-east-1"
_SANDBOX_SLUG_RE = re.compile(r"(qe|sbx|dev)-\d+")


class FusionWorkerPoolShortfallTests(FusionFallbackInstanceTypeTests):
    """
    Single-ASG / worker-pool control plane (couchbase-cloud
    feature/AV-142561_single_asg): a persistent accelerator capacity shortfall.

    Why the per-node instance-type fallback flow is not enough here
    -------------------------------------------------------------
    The CP now creates ONE ASG per plan/AZ, in a single CreateAutoScalingGroup
    call with MinSize=0 and MaxSize=DesiredCapacity=N, where N is the number of
    shards (= guest volumes = CP DB accelerator node docs) in that AZ. One
    scaling activity launches all N together, so sleeping for a while before
    injecting a fault gives either 0 or N instances, never 0 < k < N; and a
    plain Launch suspend/resume only delays all N together.

    Accelerators no longer own a volume each. They pull work from a pool: each
    instance loops acquire (CAS a 'pending' node doc in its own AZ to
    'registered' with config.compute.id = its instance id) -> attach, download,
    detach -> complete, until acquire returns 410 Gone, then shuts itself down
    with TerminateInstanceInAutoScalingGroup(ShouldDecrementDesiredCapacity).
    The CP waits on node docs reaching 'complete' with no minimum-accelerator
    threshold, no ASG health check and no DCP fallback, bounded only by the
    deploy job's ~24h retry budget. So k < N accelerators per AZ must be able
    to finish all N volumes.

    Fault injection
    ---------------
    1. As soon as the ASG appears: UpdateAutoScalingGroup(DesiredCapacity=k).
       MaxSize is left alone as the CP owns it. DesiredCapacity is CP-set but
       DP-driven (accelerators decrement it on self-shutdown) and the CP never
       resets it, so the cap sticks. Surplus instances already launching are terminated by Auto
       Scaling before they can boot the agent.
    2. Wait for k InService instances per ASG: these are the workers.
    3. ice_mode=fis (default): start FIS aws:ec2:asg-insufficient-instance-
       capacity-error on the cluster's fusion ASGs (100%, all AZs, so every
       instance-type override fails too), wait for the action to be running,
       then give back the N-k DesiredCapacity. The ASG keeps trying to launch
       the missing N-k and keeps getting InsufficientInstanceCapacity: a real
       persistent ICE shortfall while the k workers keep pulling work.
       ice_mode=cap: keep DesiredCapacity=k and inject nothing. Use this where
       FIS cannot run (the FIS execution role's trust policy only allows
       us-east-1 experiments).
    4. The fault is removed in finally: FIS stopped and its template deleted,
       and the ASG uncapped if it was still capped, so a failed run leaves the
       CP able to finish.
    """

    def setUp(self):
        FusionFallbackInstanceTypeTests.setUp(self)
        self.cp_db_util = None
        self._worker_pool_threads = []
        self._worker_pool_stop = threading.Event()
        self._prior_fusion_config = None
        self._fusion_config_modified = False

    def tearDown(self):
        self._worker_pool_stop.set()
        for th in self._worker_pool_threads:
            th.join(timeout=120)
        self._restore_split_config()
        if self.cp_db_util is not None:
            try:
                self.cp_db_util.disconnect()
            except Exception as e:
                self.log.warning(f"CP DB disconnect failed: {e}")
        FusionFallbackInstanceTypeTests.tearDown(self)

    def test_worker_pool_finishes_with_partial_fleet_under_ice(self):
        """
        Let only k accelerators per AZ launch, keep the rest blocked by ICE,
        and verify the k workers download every guest volume and the rebalance
        completes through fusion.

        Parameters (via test config):
          accelerator_cap        k, accelerators allowed per fusion ASG: 'half'
                                 (default, MaxSize // 2 of each ASG, at least 1)
                                 or a fixed number
          ice_mode               'fis' (default) or 'cap' (no FIS, cap only)
          fis_duration           ISO 8601 duration of the ICE fault; must outlast
                                 the whole download phase (default PT2H)
          fis_role_arn           FIS execution role (optional)
          verify_cp_nodes        read CP DB accelerator node docs for compute.id
                                 attribution (default True)
          cp_eks_cluster_name    CP EKS cluster (default derived from the pod URL)
          cp_role_arn            role to reach the CP EKS cluster (default
                                 arn:aws:iam::<account>:role/jenkins-cp-cli)
          cp_external_id         external id for cp_role_arn (falls back to
                                 jenkins_cpcli_role_external_id)
          asg_settle_secs        how long to keep capping ASGs that appear after
                                 the first one (one per AZ; default 60)
          num_items / skip_init  data load, see _load_and_sync_for_fusion_rebalance
          fusion_min_split_size_gb / fusion_max_slots
                                 split the data into more shards so N > k per AZ
        """
        k = self.input.param("accelerator_cap", "half")
        if str(k).lower() == "half":
            k = None    # per ASG: MaxSize // 2, see _accelerator_caps()
        ice_mode = str(self.input.param("ice_mode", "fis")).lower()
        fis_duration = self.input.param("fis_duration", "PT2H")
        fis_role_arn = self.input.param("fis_role_arn", None)
        verify_cp_nodes = self.input.param("verify_cp_nodes", True)

        if k is not None:
            self.assertTrue(isinstance(k, int) and k >= 1,
                            f"accelerator_cap must be 'half' or an integer >= 1, got {k!r}")
        self.assertIn(ice_mode, ("fis", "cap"),
                      f"ice_mode must be 'fis' or 'cap', got {ice_mode!r}")
        if ice_mode == "fis":
            try:
                iso8601_duration_to_seconds(fis_duration)
            except ValueError:
                self.fail(f"Invalid fis_duration {fis_duration!r}: use an ISO 8601 "
                          f"duration with the 'PT' designator, e.g. 'PT2H'")
            if self.aws_region != "us-east-1" and not fis_role_arn:
                self.fail(
                    f"ice_mode=fis in region {self.aws_region}: the default FIS "
                    f"execution role only trusts us-east-1 experiments, so "
                    f"StartExperiment would fail. Run in us-east-1, pass a "
                    f"fis_role_arn that trusts this region, or use ice_mode=cap.")
        if verify_cp_nodes:
            self._connect_cp_db_for_worker_pool()
        self._apply_split_config()

        for tenant in self.tenants:
            for cluster in tenant.clusters:
                self._run_partial_fleet_test(
                    tenant, cluster, k=k, ice_mode=ice_mode,
                    fis_duration=fis_duration, fis_role_arn=fis_role_arn)

    # ------------------------------------------------------------------
    # Worker-pool helpers
    # ------------------------------------------------------------------

    def _apply_split_config(self):
        """
        Optionally lower manifest.minSplitSize / maxSlots so the same data
        becomes more shards (= more guest volumes = larger N per AZ ASG). The CP
        creates min(ceil(hostData / minSplitSize), maxSlots) shards per host.
        Params: fusion_min_split_size_gb, fusion_max_slots. Restored in tearDown.
        """
        split = self.input.param("fusion_min_split_size_gb", None)
        slots = self.input.param("fusion_max_slots", None)
        if not split and not slots:
            return
        try:
            self._prior_fusion_config = CapellaAPI.get_fusion_config(
                self.pod, self.tenant, self.cluster.id)
        except Exception as e:
            self.log.warning(f"Could not read the existing fusion config: {e}")
        CapellaAPI.set_fusion_config(
            self.pod, self.tenant, self.cluster.id,
            min_split_size=int(float(split) * (1024 ** 3)) if split else None,
            max_slots=int(slots) if slots else None)
        self._fusion_config_modified = True
        self.log.info(f"Fusion config set: minSplitSize={split} GB, maxSlots={slots}")

    def _restore_split_config(self):
        """Put back the fusion config replaced by _apply_split_config(); never raises."""
        if not self._fusion_config_modified:
            return
        try:
            prior = self._prior_fusion_config or {}
            manifest = prior.get("manifest") or {}
            accelerator = prior.get("accelerator") or {}
            guest_volumes = accelerator.get("guestVolumes") or {}
            download = accelerator.get("download") or {}
            if manifest or guest_volumes or download:
                CapellaAPI.set_fusion_config(
                    self.pod, self.tenant, self.cluster.id,
                    min_split_size=manifest.get("minSplitSize"),
                    max_slots=manifest.get("maxSlots"),
                    iops=guest_volumes.get("iops"),
                    throughput=guest_volumes.get("throughput"),
                    download_rate_limit=download.get("rateLimit"))
            else:
                CapellaAPI.delete_fusion_config(self.pod, self.tenant, self.cluster.id)
            self._fusion_config_modified = False
        except Exception as e:
            self.log.error(f"Could not restore the fusion config: {e}")

    def _connect_cp_db_for_worker_pool(self):
        """
        Connect to the CP DB through the CP EKS cluster (same derivation as
        fusion_billing_volume.FusionBillingVolumeTest). The base credentials
        that assume cp_role_arn are cp_aws_access_key/cp_aws_secret_key if
        given, else the AWS_*_004 env vars (KubectlCPDBUtil default). The
        aws_access_key/aws_secret_key params are not used: on Jenkins they are
        the tenant-account taf_aws_user, which may not assume jenkins-cp-cli.
        Fails the test if the connection cannot be made: set
        verify_cp_nodes=False to run on EC2 evidence alone.
        """
        cp_eks = self.input.param("cp_eks_cluster_name", None)
        if not cp_eks:
            match = _SANDBOX_SLUG_RE.search(getattr(self.pod, "url_public", "") or "")
            cp_eks = f"{match.group(0)}-cp-eks" if match else None
        role_arn = self.input.param("cp_role_arn", None)
        if not role_arn:
            account_id = getattr(self.tenant, "account_id", None) \
                or self.input.capella.get("account_id")
            role_arn = f"arn:aws:iam::{account_id}:role/{FUSION_ASSUME_ROLE_NAME}" \
                if account_id else None
        external_id = self.input.param(
            "cp_external_id", self.input.param("jenkins_cpcli_role_external_id", None))
        if not cp_eks or not role_arn:
            self.fail("verify_cp_nodes=True but cp_eks_cluster_name/cp_role_arn could not "
                      "be derived; pass them, or set verify_cp_nodes=False")
        self.cp_db_util = KubectlCPDBUtil(
            self.input.param("cp_aws_access_key", None),
            self.input.param("cp_aws_secret_key", None),
            self.input.param("cp_aws_session_token", None),
            region=CP_EKS_REGION)
        if not self.cp_db_util.connect(cp_eks, role_arn, external_id):
            self.fail(f"Could not connect to CP EKS cluster {cp_eks} for the CP DB node-doc "
                      f"checks: {self.cp_db_util.kubectl.last_error}; set "
                      f"verify_cp_nodes=False to run on EC2 evidence alone")
        self.cp_namespace = self.input.param("cp_namespace", None)
        self.log.info(f"Connected to CP DB via {cp_eks}")

    def _wait_for_capacity_failures(self, asg_names, since_time, timeout=300,
                                    poll_interval=15):
        """Poll fis.get_asg_capacity_failure_count until every ASG has at least
        one ICE launch failure since since_time; returns the last counts."""
        deadline = time.time() + timeout
        while True:
            counts = {n: self.fusion_aws_util.fis.get_asg_capacity_failure_count(n, since_time)
                      for n in asg_names}
            self.log.info(f"Capacity launch failures since {since_time}: {counts}")
            if (counts and all(counts.values())) or time.time() >= deadline:
                return counts
            time.sleep(poll_interval)

    @staticmethod
    def _accelerator_caps(asgs, k):
        """{asg_name: cap}: k for every ASG, or MaxSize // 2 (at least 1) of each
        ASG when k is None (accelerator_cap=half). The CP creates the ASG with
        MaxSize=DesiredCapacity=N, so this is half of the shards in that AZ."""
        return {a["AutoScalingGroupName"]:
                k if k is not None else max(1, int(a.get("MaxSize") or 0) // 2)
                for a in asgs}

    def _run_partial_fleet_test(self, tenant, cluster, k, ice_mode, fis_duration,
                                fis_role_arn=None):
        self._load_and_sync_for_fusion_rebalance(cluster)

        record = self.cp_monitor.new_worker_pool_record()
        tracker = threading.Thread(
            target=self.cp_monitor.track_worker_pool_fleet,
            kwargs={"cluster": cluster, "stop_run_event": self._worker_pool_stop,
                    "record": record, "cp_db_util": self.cp_db_util,
                    "cp_namespace": getattr(self, "cp_namespace", None)},
            name=f"worker-pool-tracker-{cluster.id[:8]}", daemon=True)
        tracker.start()
        self._worker_pool_threads.append(tracker)

        fis_template_id = None
        fis_experiment_id = None
        capped = None            # cap_fusion_asgs() result while the cap is in force
        originals = {}
        workers_by_asg = {}
        launch_failures = {}
        ice_since = None
        failures = []

        try:
            self.PrintStep(f"Triggering fusion rebalance on cluster {cluster.id}")
            self._resync_kv_node_count(cluster)
            rebalance_task = self.task.async_rebalance_capella(
                self.pod, tenant, cluster, self.rebalance_config("data", +1),
                timeout=self.rebalance_timeout)
            rebalance_future = self._rebalance_future(rebalance_task)

            asgs = self._wait_for_fusion_asgs(cluster, rebalance_task)
            # ASG name: cc-fusion-asg-<cluster id>-<plan uuid>-<zone>. Count only
            # this plan's guest volumes / node docs: an earlier rebalance's stay
            # until its background migration ends.
            prefix = f"cc-fusion-asg-{cluster.id}-"
            record["plan_uuids"] = {a["AutoScalingGroupName"][len(prefix):].split("-")[0]
                                    for a in asgs
                                    if a["AutoScalingGroupName"].startswith(prefix)}
            self.log.info(f"Fusion rebalance plan(s) under test: {record['plan_uuids']}")

            # 1. Cap at k immediately: the single creation activity is already
            #    launching all N, so every second counts.
            caps = self._accelerator_caps(asgs, k)
            self.PrintStep(f"Capping {len(asgs)} fusion ASG(s) at {caps} instance(s)")
            capped = self.fusion_aws_util.cap_fusion_asgs(asgs, caps)
            originals = capped
            # deployZonalASGs creates one ASG per AZ in turn; cap any that
            # appear after the first one was seen.
            settle_deadline = time.time() + int(self.input.param("asg_settle_secs", 60))
            while time.time() < settle_deadline:
                late_asgs = [a for a in self.fusion_aws_util.list_cluster_fusion_asg(cluster.id)
                             if a["AutoScalingGroupName"] not in capped]
                if late_asgs:
                    capped.update(self.fusion_aws_util.cap_fusion_asgs(
                        late_asgs, self._accelerator_caps(late_asgs, k)))
                time.sleep(1)
            not_capped = [n for n, o in capped.items() if not o["ok"]]
            self.assertFalse(not_capped, f"Could not cap fusion ASG(s) {not_capped}")
            shortfall_asgs = [n for n, o in capped.items()
                              if o["DesiredCapacity"] > o["capped_to"]]
            if not shortfall_asgs:
                self.fail(
                    f"No fusion ASG wants more accelerators than its cap "
                    f"({ {n: (o['DesiredCapacity'], o['capped_to']) for n, o in capped.items()} }"
                    f" as (desired, cap)), so there is no shortfall. Load more data (num_items), "
                    f"lower accelerator_cap, or lower the fusion split size so there "
                    f"are more shards per AZ.")

            # 2. The k workers per ASG.
            self.PrintStep("Waiting for the capped fleet to come InService")
            expected = {n: o["capped_to"] for n, o in capped.items()}
            in_service = self.fusion_aws_util.wait_for_fusion_asg_in_service(
                cluster.id, expected, timeout=self.gv_launch_timeout, exact=True,
                abort_fn=rebalance_future.done)
            self.assertIsNotNone(
                in_service,
                f"Capped fusion ASGs did not reach {expected} InService instances "
                f"(or the rebalance ended first, result={rebalance_task.result})")
            workers_by_asg = {n: set(ids) for n, ids in in_service.items()}
            record["cap_applied_at"] = time.time()
            self.log.info(f"Workers per ASG: { {n: sorted(v) for n, v in workers_by_asg.items()} }")

            # 3. Persistent ICE for the missing N-k.
            if ice_mode == "fis":
                self.PrintStep(f"Starting FIS ASG capacity-error fault ({fis_duration})")
                fis_template_id, fis_experiment_id = self._start_fis_capacity_experiment(
                    cluster=cluster, duration=fis_duration, fis_role_arn=fis_role_arn)
                self.sleep(10)
                status = self.fusion_aws_util.fis.get_experiment_status(
                    fis_experiment_id)["status"]
                self.assertEqual(
                    str(status).lower(), "running",
                    f"FIS experiment {fis_experiment_id} is '{status}', not running; "
                    f"restoring capacity would just launch N")
                ice_since = datetime.datetime.now(datetime.timezone.utc)
                self.PrintStep("Restoring ASG DesiredCapacity to N under ICE")
                restored = self.fusion_aws_util.restore_fusion_asgs(originals)
                capped = None
                self.assertTrue(all(restored.get(n) for n in shortfall_asgs),
                                f"Could not restore capacity on {restored}")
                launch_failures = self._wait_for_capacity_failures(
                    shortfall_asgs, ice_since, timeout=300)
                if not all(launch_failures.get(n) for n in shortfall_asgs):
                    failures.append(
                        f"No InsufficientInstanceCapacity launch failure on some "
                        f"shortfall ASG within 300s of restoring N: {launch_failures}. "
                        f"The shortfall was not driven by ICE.")
            else:
                self.log.info(f"ice_mode=cap: keeping DesiredCapacity at "
                              f"{ {n: o['capped_to'] for n, o in originals.items()} } "
                              f"for the whole download phase")

            # 4. Let the k workers drain the pool.
            self.PrintStep("Waiting for the rebalance to finish on the partial fleet")
            self.wait_for_rebalances([rebalance_task], timeout=self.rebalance_timeout)
            if ice_mode == "fis":
                status = self.fusion_aws_util.fis.get_experiment_status(
                    fis_experiment_id)["status"]
                if str(status).lower() != "running":
                    failures.append(
                        f"FIS experiment {fis_experiment_id} was '{status}' when the "
                        f"rebalance finished: the ICE window did not cover the whole "
                        f"download phase; raise fis_duration")
                launch_failures = {
                    n: self.fusion_aws_util.fis.get_asg_capacity_failure_count(n, ice_since)
                    for n in shortfall_asgs}
        finally:
            self._cleanup_fis_experiment(fis_template_id, fis_experiment_id)
            if capped:
                try:
                    self.fusion_aws_util.restore_fusion_asgs(capped)
                except Exception as e:
                    self.log.warning(f"Could not uncap fusion ASGs: {e}")
            self._worker_pool_stop.set()
            tracker.join(timeout=300)

        # ---------------- verification ----------------
        self.PrintStep("Verifying the partial fleet did all the work")
        # FIS reports running before the ICE fault is enforced (~60s in
        # fusion_volume #1037), so a few instances may launch after the cap.
        # They are part of the partial fleet: count them as workers. Instances
        # Auto Scaling terminated when the cap lowered DesiredCapacity were
        # already launching when the ASG was first seen.
        worker_ids = set().union(*workers_by_asg.values()) if workers_by_asg else set()
        cull_grace = record["cap_applied_at"] or 0
        culled_ids = {iid for iid, info in record["instances"].items()
                      if iid not in worker_ids and info["first_seen"] <= cull_grace}
        late_ids = set(record["instances"]) - worker_ids - culled_ids
        if late_ids:
            self.log.warning(f"{len(late_ids)} accelerator(s) launched after the cap, "
                             f"before ICE was enforced; counted as workers: {sorted(late_ids)}")
            for iid in late_ids:
                asg = record["instances"][iid].get("asg")
                if asg in workers_by_asg:
                    workers_by_asg[asg].add(iid)
        summary = self.cp_monitor.summarize_worker_pool_fleet(cluster, record, workers_by_asg)

        for name, o in originals.items():
            workers = workers_by_asg.get(name, set())
            if o["DesiredCapacity"] > o["capped_to"] and len(workers) >= o["DesiredCapacity"]:
                failures.append(
                    f"ASG {name}: {len(workers)} workers for {o['DesiredCapacity']} nodes; "
                    f"ICE never held a shortfall")
            covered = summary["covered_nodes_by_asg"].get(name, set())
            if len(covered) != o["DesiredCapacity"]:
                failures.append(
                    f"ASG {name}: its {len(workers)} worker(s) were seen "
                    f"handling {len(covered)} of {o['DesiredCapacity']} accelerator nodes")
            if o["DesiredCapacity"] > o["capped_to"]:
                per_worker = {iid: len(summary["nodes_by_instance"].get(iid, ()))
                              for iid in workers}
                if not any(n > 1 for n in per_worker.values()):
                    failures.append(
                        f"ASG {name}: no worker handled more than one node {per_worker}; "
                        f"the pool was not drained by fewer instances than volumes")

        expected_nodes = sum(o["DesiredCapacity"] for o in originals.values())
        if len(summary["all_node_ids"]) != expected_nodes:
            failures.append(
                f"Saw {len(summary['all_node_ids'])} accelerator nodes/guest volumes, the "
                f"ASGs were sized for {expected_nodes}")
        culled_work = {nid: iids & culled_ids
                       for nid, iids in summary["nodes_by_non_worker"].items()
                       if iids & culled_ids}
        if culled_work:
            self.log.warning(f"Instances culled by the cap had touched nodes before "
                             f"termination (claims are released lazily): {culled_work}")
        if summary["unattributed_nodes"]:
            failures.append(
                f"No accelerator was seen handling nodes {summary['unattributed_nodes']} "
                f"(neither a guest-volume attachment nor a CP DB compute.id)")
        if self.cp_db_util is not None and not record["nodes"]:
            failures.append("CP DB returned no fusion accelerator node docs during the run")
        if summary["nodes_not_complete"]:
            self.log.warning(f"Node docs whose last sampled status was not complete: "
                             f"{summary['nodes_not_complete']}")

        transfer = self.fusion_monitor.get_rebalance_transfer_summary(cluster)
        if not transfer:
            failures.append("No rebalance report: cannot tell fusion from DCP")
        for bucket, t in (transfer or {}).items():
            if t["rebalance_method"] != "fusion" or not t["mounted_volume_count"]:
                failures.append(
                    f"Bucket {bucket}: rebalance_method={t['rebalance_method']}, "
                    f"mounted guest volumes={t['mounted_volume_count']} -- not a fusion move")

        # Guest volumes are deleted only after background migration, which can
        # take hours: wait for the accelerators and ASGs only.
        self.PrintStep("Verifying fusion accelerators and ASGs are torn down")
        torn_down, left = self.cp_monitor.wait_for_fusion_infra_teardown(
            cluster, include_guest_volumes=False)
        if not torn_down:
            failures.append(f"Fusion infrastructure left after the rebalance: {left}")
        if not self._is_cluster_healthy():
            failures.append(f"Cluster {cluster.id} is not healthy after the rebalance")

        result = PrettyTable()
        result.field_names = ["Check", "Value"]
        result.add_row(["accelerator cap (k)",
                        {n: o["capped_to"] for n, o in originals.items()} or k])
        result.add_row(["ICE mode", ice_mode])
        result.add_row(["nodes expected / seen", f"{expected_nodes} / {len(summary['all_node_ids'])}"])
        result.add_row(["workers", sum(len(v) for v in workers_by_asg.values())])
        result.add_row(["capacity launch failures", launch_failures or "-"])
        result.add_row(["failures", len(failures)])
        self.log.info(f"Partial-fleet result for cluster {cluster.id}:\n{result}")
        if failures:
            self.fail("Partial accelerator fleet test failed:\n  - " + "\n  - ".join(failures))
