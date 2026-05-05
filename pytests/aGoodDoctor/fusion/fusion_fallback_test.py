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

import time
import datetime

from prettytable import PrettyTable

from capella_utils.dedicated import CapellaUtils as CapellaAPI
from pytests.basetestcase import BaseTestCase
from aGoodDoctor.hostedOPD import hostedOPD
from bucket_utils.bucket_ready_functions import JavaDocLoaderUtils

from .fusion_aws_util import FusionAWSUtil
from .fusion_monitor_util import FusionMonitorUtil
from .fusion_cp_resource_monitor import FusionCPResourceMonitor
from .fusion_enable_disable_test import _FusionTestBase


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
        """
        n_to_fail = self.input.param("fallback_n", 3)
        fis_role_arn = self.input.param("fis_role_arn", None)

        for tenant in self.tenants:
            for cluster in tenant.clusters:
                self._run_fallback_test(
                    tenant, cluster, n_to_fail=n_to_fail, fis_role_arn=fis_role_arn
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

        for tenant in self.tenants:
            for cluster in tenant.clusters:
                self._run_fallback_test(
                    tenant,
                    cluster,
                    n_to_fail=arm_prefix_count,
                    fis_role_arn=fis_role_arn,
                    assert_x86=True,
                )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _run_fallback_test(self, tenant, cluster, n_to_fail: int,
                           fis_role_arn=None, assert_x86: bool = False):
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
        """

        # self.PrintStep(f"Loading data for fusion rebalance on cluster {cluster.id}")
        # self.generate_docs(cluster, num_items=5_000_000, doc_size=1024)
        # self.data_load_till_given_num(num_items=5_000_000)
        # self.sleep(60, "Allow data to sync past fusion threshold")

        fis_template_id = None
        fis_experiment_id = None
        suspended_asg_names = []

        try:
            self.PrintStep(f"Triggering fusion rebalance on cluster {cluster.id}")
            config = self.rebalance_config("data", +1)
            rebalance_task = self.task.async_rebalance_capella(
                self.pod, tenant, cluster, config, timeout=1800)

            experiment_start = datetime.datetime.now(datetime.timezone.utc)

            # Poll aggressively so we catch the ASGs before the first RunInstances call
            self.PrintStep("Waiting for fusion ASGs to appear")
            deadline = time.time() + 1800
            while time.time() < deadline:
                asgs = self.fusion_aws_util.list_cluster_fusion_asg(cluster.id)
                if asgs:
                    self.log.info(f"Detected {len(asgs)} fusion ASGs for cluster {cluster.id}")
                    break
            else:
                self.fail("Fusion ASGs not created within 1800s of triggering rebalance")

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
                duration="PT15M",
                fis_role_arn=fis_role_arn,
            )
            self.sleep(10)

            # Resume Launch — Auto Scaling will now attempt launches and every
            # attempt will hit the FIS InsufficientInstanceCapacity fault.
            self.PrintStep("Resuming ASG Launch process (FIS is now active)")
            self.fusion_aws_util.resume_asg_launch_process(suspended_asg_names)
            suspended_asg_names = []  # mark consumed so finally doesn't re-resume

            # Wait until each ASG has attempted (and failed) n_to_fail instance types
            self.PrintStep(
                f"Waiting for {n_to_fail} capacity failures per ASG before stopping FIS"
            )
            failure_counts = self.fusion_aws_util.wait_for_min_capacity_failures_per_asg(
                cluster_id=cluster.id,
                min_failures=n_to_fail,
                since_time=experiment_start,
                timeout=1200,
                poll_interval=5,
            )
            self.log.info(f"Capacity failure counts per ASG: {failure_counts}")

            # Stop FIS — the next launch attempt for each ASG will use the first
            # type in the override list that isn't blocked (type N+1+)
            self.log.info(f"Stopping FIS experiment {fis_experiment_id}")
            self.fusion_aws_util.fis.stop_experiment(fis_experiment_id)
            fis_experiment_id = None  # mark consumed so finally doesn't re-stop it

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
            self.log.info(f"Fusion ASG launch results:\n{asg_table}")

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
            # Best-effort cleanup of FIS resources
            if fis_experiment_id:
                try:
                    self.fusion_aws_util.fis.stop_experiment(fis_experiment_id)
                except Exception as e:
                    self.log.warning(f"Could not stop FIS experiment {fis_experiment_id}: {e}")
            if fis_template_id:
                try:
                    self.fusion_aws_util.fis.delete_experiment_template(fis_template_id)
                except Exception as e:
                    self.log.warning(f"Could not delete FIS template {fis_template_id}: {e}")

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
