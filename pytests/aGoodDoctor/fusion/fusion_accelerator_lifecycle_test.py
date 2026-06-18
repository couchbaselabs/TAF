"""
Fusion Accelerator Lifecycle Tests

Validates the full lifecycle of fusion accelerator instances and EBS guest volumes
during Couchbase rebalances on AWS-backed Capella clusters.

Tests in this file (§2 and §7 of the E2E test plan):
  1. test_accelerator_creation_during_rebalance       — accelerators appear in phase 4
  2. test_accelerator_termination_after_rebalance     — accelerators and ASGs cleaned up post-rebalance
  3. test_ebs_guest_volume_full_lifecycle             — EBS volume lifecycle: created→hydrated→cleaned
  4. test_back_to_back_rebalances_no_orphaned_volumes — no orphaned volumes across consecutive rebalances
  5. test_fusion_state_stays_enabled_through_rebalance — fusion state stays 'enabled' throughout
  6. test_accelerator_instance_count_matches_data_size — instance count matches expected for data size
  7. test_no_public_ip_on_accelerator_nodes           — accelerator EC2 nodes have no public IP
  8. test_guest_volume_properties                     — storage class, IOPS, throughput, encryption, tags, AZ
  9. test_guest_volume_size_scales_with_data          — stub: volume size grows with data size
 10. test_asg_deleted_after_rebalance_within_5_mins   — ASGs reach 0 within 5 min of rebalance completion
 11. test_accelerator_node_termination_resilience     — terminate one accelerator mid-rebalance; verify relaunch and completion
"""

import threading
import time

from capella_utils.dedicated import CapellaUtils as CapellaAPI
from membase.api.rest_client import RestConnection
from .fusion_test_base import _FusionTestBase


class FusionAcceleratorLifecycleTest(_FusionTestBase):
    """
    Tests for fusion accelerator instance and EBS guest volume lifecycle.

    Each test calls _ensure_fusion_state() to reach its required starting state.
    setUp creates a fresh bucket; tearDown removes it and resets the node count.
    """

    _FAILED_STATES = frozenset([
        "deployment_failed", "deploymentFailed", "redeploymentFailed",
        "rebalance_failed", "rebalanceFailed", "scaleFailed",
    ])

    def setUp(self):
        super().setUp()
        self.log.info(f"[setUp] cluster={self.cluster.id}")
        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id,
            "Wait for healthy cluster state before bucket cleanup", timeout=600)
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
                    "Wait for healthy state before node reset", timeout=1800)
                self.wait_for_rebalances([self.task.async_rebalance_capella(
                    self.pod, self.tenant, self.cluster,
                    self.rebalance_config("data", delta), timeout=self.index_timeout)])
            except Exception as e:
                self.log.error(f"Failed to reset KV nodes to {self.initial_kv_nodes}: {e}")
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

    def _accelerator_filter(self):
        """Return EC2 filters scoped to fusion accelerators on this cluster."""
        return self.fusion_aws_util._cluster_filter(
            self.cluster.id,
            [{'Name': 'tag:couchbase-cloud-function', 'Values': ['fusion-accelerator']}],
        )

    def _load_above_threshold(self):
        """Load enough documents to push data above the fusion rebalance threshold."""
        create_end = self.input.param("create_end", 20_000_000)
        self._load_data(self.cluster, create_start=0, create_end=create_end)
        self.sleep(120, "Allow initial sync to S3 before triggering rebalance")

    def _trigger_scale_out(self):
        """Trigger a +1 data-node scale-out rebalance and return the async task."""
        config = self.rebalance_config("data", +1)
        return self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster, config, timeout=self.index_timeout)

    def _poll_until_accelerators_appear(self, rebalance_task, timeout=1800):
        """Poll until at least one accelerator instance appears or the rebalance ends.

        Returns the list of instances (may be empty if rebalance completed without
        ever launching accelerators — caller should assert on this).
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            if rebalance_task.state in self._FAILED_STATES:
                self.fail(f"Rebalance failed with state: {rebalance_task.state}")
            instances = self.fusion_aws_util.list_accelerator_instances(
                self._accelerator_filter(), log="Fusion Accelerator")
            if instances:
                return instances
            if rebalance_task.state == "healthy":
                break
            time.sleep(10)
        return []

    # ------------------------------------------------------------------
    # Test 1: Accelerators appear during rebalance (phase 4)
    # ------------------------------------------------------------------

    def test_accelerator_creation_during_rebalance(self):
        """
        Trigger a fusion-eligible rebalance and verify that accelerator EC2 instances
        are launched (8-phase lifecycle, phase 4 — Deploy Accelerators).

        Validates:
        - Fusion is enabled and data is above threshold
        - At least one accelerator instance appears during rebalance
        - Instances are tagged couchbase-cloud-function=fusion-accelerator
        - ASGs are created for the accelerator instances
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._load_above_threshold()

        rebalance_task = self._trigger_scale_out()
        self.sleep(30, "Wait for rebalance to start before checking accelerators")

        instances = self._poll_until_accelerators_appear(rebalance_task)
        self.assertGreater(
            len(instances), 0,
            "No accelerator instances found — fusion rebalance did not launch "
            "accelerators above threshold")
        self.log.info(f"Fusion accelerator instances created: {len(instances)}")

        asgs = self.fusion_aws_util.list_cluster_fusion_asg(self.cluster.id)
        self.assertGreater(len(asgs), 0,
                           "No ASGs found for fusion accelerators during rebalance")
        self.log.info(f"Fusion ASGs active during rebalance: {len(asgs)}")

        self.wait_for_rebalances([rebalance_task])

    # ------------------------------------------------------------------
    # Test 2: Accelerators and ASGs are cleaned up after rebalance (phase 8)
    # ------------------------------------------------------------------

    def test_accelerator_termination_after_rebalance(self):
        """
        After a fusion rebalance completes, verify that all accelerator EC2 instances
        are terminated and all ASGs are deleted (8-phase lifecycle, phase 8 — Teardown).

        Validates:
        - Rebalance completes successfully
        - Accelerator instances are terminated within timeout
        - EBS guest volumes reach count = 0 within EBS_CLEANUP_TIMEOUT
        - ASGs are deleted post-rebalance
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._load_above_threshold()

        rebalance_task = self._trigger_scale_out()
        self.sleep(30, "Wait for rebalance to start")

        self._poll_until_accelerators_appear(rebalance_task)
        self.wait_for_rebalances([rebalance_task])

        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id, timeout=600)

        terminated = self.cp_monitor.monitor_fusion_accelerator_nodes_killed_after_rebalance(
            self.cluster)
        self.assertTrue(terminated,
                        "Accelerator instances were not terminated after rebalance")

        cleaned = self.cp_monitor.monitor_ebs_cleanup(
            self.cluster, self.stop_run_event,
            timeout=self.cp_monitor.EBS_CLEANUP_TIMEOUT)
        self.assertTrue(cleaned,
                        "EBS guest volumes were not cleaned up after rebalance")

        self.cp_monitor.check_asg_cleanup_after_rebalance([self.cluster])
        self.log.info("All accelerator resources cleaned up after rebalance")

    # ------------------------------------------------------------------
    # Test 3: Full EBS guest volume lifecycle
    # ------------------------------------------------------------------

    def test_ebs_guest_volume_full_lifecycle(self):
        """
        Verify the complete EBS guest volume lifecycle during a fusion rebalance:
        created → attached to accelerators (hydration) → transferred to KV nodes → count=0.

        Validates:
        - 0 guest volumes exist before rebalance starts
        - Volumes appear and hydrate during rebalance
        - After rebalance, guest volume count returns to 0
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._load_above_threshold()

        pre_volumes = self.cp_monitor.get_current_guest_volume_ids(self.cluster)
        self.assertEqual(len(pre_volumes), 0,
                         f"Expected 0 guest volumes before rebalance, "
                         f"found {len(pre_volumes)}")

        fusion_rebalances = []
        rebalance_task = self._trigger_scale_out()
        self.sleep(30, "Wait for rebalance to start")

        hydration_completed = self.cp_monitor.monitor_fusion_guest_volumes(
            self.tenant, self.cluster, rebalance_task,
            self.fusion_monitor, fusion_rebalances,
            wait_for_hydration_complete=True,
            timeout=self.DEFAULT_TIMEOUT,
            find_master_func=self.find_master,
        )
        self.assertTrue(hydration_completed,
                        "EBS guest volume hydration did not complete successfully")

        self.wait_for_rebalances([rebalance_task])
        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id, timeout=600)

        cleaned = self.cp_monitor.monitor_ebs_cleanup(
            self.cluster, self.stop_run_event,
            timeout=self.cp_monitor.EBS_CLEANUP_TIMEOUT)
        self.assertTrue(cleaned,
                        "EBS guest volumes were not cleaned up after rebalance")

        post_volumes = self.cp_monitor.get_current_guest_volume_ids(self.cluster)
        self.assertEqual(
            len(post_volumes), 0,
            f"Expected 0 guest volumes after rebalance, "
            f"found {len(post_volumes)}: {post_volumes}")
        self.log.info("EBS guest volume full lifecycle validated successfully")

    # ------------------------------------------------------------------
    # Test 4: Back-to-back rebalances leave no orphaned volumes
    # ------------------------------------------------------------------

    def test_back_to_back_rebalances_no_orphaned_volumes(self):
        """
        Run two consecutive fusion-eligible scale-out rebalances and verify that
        no EBS guest volumes are orphaned between or after runs.

        Validates:
        - First rebalance completes with 0 guest volumes remaining
        - Second rebalance starts cleanly (0 pre-existing guest volumes)
        - Second rebalance completes with 0 guest volumes remaining
        - No ASGs from the first rebalance linger into the second
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._load_above_threshold()

        for run in range(1, 3):
            self.log.info(f"--- Starting rebalance run {run} of 2 ---")

            pre_run_volumes = self.cp_monitor.get_current_guest_volume_ids(self.cluster)
            self.assertEqual(
                len(pre_run_volumes), 0,
                f"Run {run}: expected 0 guest volumes before rebalance, "
                f"found {len(pre_run_volumes)}: {pre_run_volumes}")

            rebalance_task = self._trigger_scale_out()
            self.sleep(30, f"Wait for rebalance run {run} to start")

            self._poll_until_accelerators_appear(rebalance_task)
            self.wait_for_rebalances([rebalance_task])
            CapellaAPI.wait_until_done(
                self.pod, self.tenant, self.cluster.id, timeout=600)

            cleaned = self.cp_monitor.monitor_ebs_cleanup(
                self.cluster, self.stop_run_event,
                timeout=self.cp_monitor.EBS_CLEANUP_TIMEOUT)
            self.assertTrue(cleaned,
                            f"Orphaned EBS guest volumes remain after rebalance run {run}")

            post_run_volumes = self.cp_monitor.get_current_guest_volume_ids(self.cluster)
            self.assertEqual(
                len(post_run_volumes), 0,
                f"Run {run}: expected 0 guest volumes after rebalance, "
                f"found {len(post_run_volumes)}: {post_run_volumes}")
            self.log.info(f"Run {run}: guest volume cleanup confirmed — no orphaned volumes")

        self.log.info("Back-to-back rebalances validated — no orphaned volumes in either run")

    # ------------------------------------------------------------------
    # Test 5: Fusion state stays 'enabled' throughout rebalance
    # ------------------------------------------------------------------

    def test_fusion_state_stays_enabled_through_rebalance(self):
        """
        Continuously poll fusion state during a scale-out rebalance and assert
        it never leaves 'enabled'. After rebalance, assert cluster is 'healthy'.

        Validates:
        - Fusion state stays 'enabled' throughout the entire rebalance
        - Cluster returns to 'healthy' state after rebalance
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._load_above_threshold()

        rebalance_task = self._trigger_scale_out()

        state_violation = None
        poll_stop = threading.Event()

        def _poll_fusion_state():
            nonlocal state_violation
            while not poll_stop.is_set():
                status = CapellaAPI.get_fusion_status(
                    self.pod, self.tenant, self.cluster.id)
                current = status.get("state", "unknown")
                if current != "enabled":
                    state_violation = current
                    poll_stop.set()
                    return
                time.sleep(5)

        monitor_thread = threading.Thread(target=_poll_fusion_state, daemon=True)
        monitor_thread.start()

        try:
            self.wait_for_rebalances([rebalance_task])
        finally:
            poll_stop.set()
            monitor_thread.join(timeout=30)

        self.assertIsNone(
            state_violation,
            f"Fusion state left 'enabled' during rebalance — observed: '{state_violation}'")

        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id, timeout=600)

        final_status = CapellaAPI.get_fusion_status(self.pod, self.tenant, self.cluster.id)
        self.assertEqual(
            final_status.get("state"), "enabled",
            f"Fusion not 'enabled' after rebalance: {final_status}")
        self.log.info("Fusion state remained 'enabled' throughout rebalance")

    # ------------------------------------------------------------------
    # Test 6: Accelerator instance count matches data size
    # ------------------------------------------------------------------

    def test_accelerator_instance_count_matches_data_size(self):
        """
        Verify that the number of accelerator instances scales with data size:
        below the fusion threshold → 0 accelerators (DCP path);
        above threshold → at least expected_accelerator_count accelerators.

        Parameters:
        - small_create_end: document count for below-threshold load (default: 100)
        - expected_accelerator_count: minimum expected accelerators above threshold (default: 1)
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        small_end = self.input.param("small_create_end", 100)
        expected_count = self.input.param("expected_accelerator_count", 1)

        # Phase A: below-threshold load — expect 0 accelerators (DCP rebalance)
        self._load_data(self.cluster, create_start=0, create_end=small_end)
        small_task = self._trigger_scale_out()
        self.sleep(30, "Wait for small rebalance to start")

        deadline = time.time() + 600
        while time.time() < deadline:
            if small_task.state in (self._FAILED_STATES | {"healthy"}):
                break
            time.sleep(5)

        instances_small = self.fusion_aws_util.list_accelerator_instances(
            self._accelerator_filter(), log="BelowThreshold")
        self.assertEqual(
            len(instances_small), 0,
            f"Expected 0 accelerators for below-threshold data, "
            f"found {len(instances_small)}")
        self.wait_for_rebalances([small_task])
        self.log.info("Below-threshold rebalance used DCP — 0 accelerators confirmed")

        # Phase B: above-threshold load — expect >= expected_count accelerators
        self._load_above_threshold()
        large_task = self._trigger_scale_out()
        self.sleep(30, "Wait for large rebalance to start")

        instances_large = self._poll_until_accelerators_appear(large_task)
        self.assertGreaterEqual(
            len(instances_large), expected_count,
            f"Expected >= {expected_count} accelerators for above-threshold data, "
            f"found {len(instances_large)}")
        self.log.info(
            f"Above-threshold rebalance launched {len(instances_large)} accelerator(s) "
            f"(expected >= {expected_count})")
        self.wait_for_rebalances([large_task])

    # ------------------------------------------------------------------
    # Test 7: No public IP on accelerator nodes
    # ------------------------------------------------------------------

    def test_no_public_ip_on_accelerator_nodes(self):
        """
        Verify that accelerator EC2 instances do not have a public IP address.
        Accelerators must run in private subnets only — no public exposure.

        Validates:
        - Accelerator instances are launched during a fusion rebalance
        - None of the accelerator instances have a PublicIpAddress in AWS
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._load_above_threshold()

        rebalance_task = self._trigger_scale_out()
        self.sleep(30, "Wait for rebalance to start")

        deadline = time.time() + 1800
        accelerators_seen = False
        instances_with_public_ip = []

        while time.time() < deadline:
            if rebalance_task.state in self._FAILED_STATES:
                self.fail(f"Rebalance failed: {rebalance_task.state}")

            raw_instances = self.fusion_aws_util.list_instances(
                self._accelerator_filter(), log="PublicIPCheck")
            if raw_instances:
                accelerators_seen = True
                instances_with_public_ip = [
                    inst for inst in raw_instances
                    if inst.get("PublicIpAddress") not in (None, "", "N/A")
                ]
                # Check immediately; if any violation found, stop early
                if instances_with_public_ip:
                    break

            if rebalance_task.state == "healthy":
                break
            time.sleep(10)

        self.assertTrue(
            accelerators_seen,
            "No accelerator instances appeared during rebalance — "
            "cannot validate public IP absence")
        self.assertEqual(
            len(instances_with_public_ip), 0,
            f"Security violation: {len(instances_with_public_ip)} accelerator instance(s) "
            f"have public IPs: "
            f"{[i.get('InstanceId') for i in instances_with_public_ip]}")
        self.log.info("Confirmed: no accelerator instance has a public IP address")

        self.wait_for_rebalances([rebalance_task])

    # ------------------------------------------------------------------
    # Test 8: Guest volume storage properties, encryption, tags, and AZ
    # ------------------------------------------------------------------

    def test_guest_volume_properties(self):
        """
        During a fusion rebalance, inspect every EBS guest volume via the AWS API
        and assert that all required properties are correct:

        Storage class:
          - VolumeType = gp3
          - Iops       = expected_iops  (default 16 000)
          - Throughput = expected_throughput MB/s (default 2 000)

        Encryption:
          - Encrypted  = True
          - KmsKeyId   is a non-empty string

        Tags (required for CP tag-based lifecycle management):
          - couchbase-cloud-function   = fusion-accelerator
          - couchbase-cloud-cluster-id = <cluster.id>

        AZ colocation:
          - Each volume's AvailabilityZone must match one of the KV node AZs.
            EBS volumes can only attach to instances in the same AZ; a mismatch
            would cause a hard attach failure during phase 6 of the rebalance.
            KV node AZs are captured before the rebalance via list_instances so
            accelerator instances (added during phase 4) are not mixed in.
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        # Capture KV node AZs before the rebalance — at this point no accelerators
        # exist yet, so every cluster instance is a CBS/KV node.
        pre_rebalance_instances = self.fusion_aws_util.list_instances(
            self.fusion_aws_util._cluster_filter(self.cluster.id),
            log="KVNodesAZ")
        kv_azs = {
            inst.get("Placement", {}).get("AvailabilityZone")
            for inst in pre_rebalance_instances
        } - {None}
        self.assertGreater(len(kv_azs), 0,
                           "Could not determine KV node AZs before rebalance")
        self.log.info(f"KV node AZs (pre-rebalance): {kv_azs}")

        self._load_above_threshold()
        rebalance_task = self._trigger_scale_out()
        self.sleep(30, "Wait for rebalance to start before polling for guest volumes")

        expected_iops = self.input.param("expected_iops", 16000)
        expected_throughput = self.input.param("expected_throughput", 2000)

        # Poll until at least one guest volume appears in AWS
        volumes = []
        deadline = time.time() + 1800
        while time.time() < deadline:
            if rebalance_task.state in self._FAILED_STATES:
                self.fail(f"Rebalance failed before guest volumes appeared: "
                          f"{rebalance_task.state}")
            volumes = self.fusion_aws_util.ec2.list_volumes_by_cluster_id(filters={
                "couchbase-cloud-cluster-id": self.cluster.id,
                "couchbase-cloud-function": "fusion-accelerator",
            })
            if volumes:
                self.log.info(
                    f"Found {len(volumes)} guest volume(s) — inspecting properties")
                break
            if rebalance_task.state == "healthy":
                break
            time.sleep(10)

        self.assertGreater(
            len(volumes), 0,
            "No EBS guest volumes appeared during the fusion rebalance — "
            "cannot validate properties")

        failures = []
        for vol in volumes:
            vol_id = vol.get("VolumeId", "unknown")
            tag_dict = {t["Key"]: t["Value"] for t in vol.get("Tags", [])}

            checks = [
                # (description, actual, expected, assert_type)
                ("VolumeType",
                 vol.get("VolumeType"), "gp3", "eq"),
                ("Iops",
                 vol.get("Iops"), expected_iops, "eq"),
                ("Throughput",
                 vol.get("Throughput"), expected_throughput, "eq"),
                ("Encrypted",
                 vol.get("Encrypted"), True, "eq"),
                ("KmsKeyId non-empty",
                 bool(vol.get("KmsKeyId")), True, "eq"),
                ("tag:couchbase-cloud-function",
                 tag_dict.get("couchbase-cloud-function"), "fusion-accelerator", "eq"),
                ("tag:couchbase-cloud-cluster-id",
                 tag_dict.get("couchbase-cloud-cluster-id"), self.cluster.id, "eq"),
                ("AvailabilityZone in KV AZs",
                 vol.get("AvailabilityZone"), kv_azs, "in"),
            ]

            for desc, actual, expected, assert_type in checks:
                ok = (actual == expected) if assert_type == "eq" else (actual in expected)
                status = "OK" if ok else "FAIL"
                self.log.info(f"  [{status}] {vol_id} {desc}: {actual!r}")
                if not ok:
                    failures.append(
                        f"{vol_id} — {desc}: got {actual!r}, expected {expected!r}")

        self.assertEqual(
            len(failures), 0,
            "Guest volume property violations:\n" + "\n".join(failures))
        self.log.info(
            f"All {len(volumes)} guest volume(s) passed property validation")

        self.wait_for_rebalances([rebalance_task])

    # ------------------------------------------------------------------
    # Test 9: Guest volume size scales with data size  [STUB]
    # ------------------------------------------------------------------

    def test_guest_volume_size_scales_with_data(self):
        """
        Stub: Verify that EBS guest volume size scales with the amount of data.

        Plan (not yet implemented):
          Phase A — load a small dataset (small_create_end docs), trigger a
                    scale-out rebalance, record the Size (GB) of all guest volumes.
          Phase B — load a larger dataset (create_end docs), trigger a second
                    scale-out rebalance, record the guest volume sizes again.
          Assert  — max(size_B) > max(size_A): more data must produce larger volumes.

        Parameters needed:
          small_create_end   — doc count for the small load (default: 1_000_000)
          create_end         — doc count for the large load  (default: 20_000_000)
        """
        self.skipTest("Not yet implemented — guest volume size scaling validation pending")

    # ------------------------------------------------------------------
    # Test 10: ASGs deleted within 5 minutes of rebalance completion
    # ------------------------------------------------------------------

    def test_asg_deleted_after_rebalance_within_5_mins(self):
        """
        After a fusion rebalance completes (CBS reports 'healthy'), verify that all
        Auto Scaling Groups created for accelerator instances are deleted by the CP
        within 5 minutes.

        The CP's phase-8 teardown terminates accelerator EC2 instances and then
        deletes their ASGs. The 5-minute SLA clock starts the moment the CBS
        rebalance task transitions to 'healthy' — before wait_until_done — to
        measure the true end-to-end cleanup latency from CBS completion.

        Validates:
        - At least one ASG exists during the rebalance (fusion path was taken)
        - list_cluster_fusion_asg returns 0 ASGs within 300 seconds of rebalance
          completion
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._load_above_threshold()

        rebalance_task = self._trigger_scale_out()
        self.sleep(30, "Wait for rebalance to start before checking for ASGs")

        # Confirm at least one ASG was created (fusion path was taken)
        asgs_seen = self._poll_until_accelerators_appear(rebalance_task)
        self.assertGreater(
            len(asgs_seen), 0,
            "No accelerator instances appeared — fusion rebalance may not have "
            "triggered; cannot validate ASG deletion SLA")

        mid_asgs = self.fusion_aws_util.list_cluster_fusion_asg(self.cluster.id)
        self.assertGreater(
            len(mid_asgs), 0,
            "No ASGs found during rebalance despite accelerator instances being present")
        self.log.info(f"ASGs present during rebalance: {len(mid_asgs)}")

        # Wait for CBS to report rebalance complete, then start the 5-minute clock
        self.wait_for_rebalances([rebalance_task])
        sla_start = time.time()
        sla_deadline = sla_start + 300
        self.log.info(
            f"CBS rebalance complete — polling for ASG deletion within 300s")

        asgs_deleted = False
        while time.time() < sla_deadline:
            remaining_asgs = self.fusion_aws_util.list_cluster_fusion_asg(
                self.cluster.id)
            if not remaining_asgs:
                asgs_deleted = True
                elapsed = time.time() - sla_start
                self.log.info(
                    f"All ASGs deleted {elapsed:.1f}s after rebalance completion "
                    f"(SLA: 300s)")
                break
            self.log.info(
                f"ASGs still present: {len(remaining_asgs)} — "
                f"{int(sla_deadline - time.time())}s remaining in SLA window")
            time.sleep(15)

        elapsed = time.time() - sla_start
        self.assertTrue(
            asgs_deleted,
            f"Fusion accelerator ASGs were not deleted within 300s of rebalance "
            f"completion (elapsed: {elapsed:.1f}s, "
            f"remaining: {len(self.fusion_aws_util.list_cluster_fusion_asg(self.cluster.id))} ASGs)")

    # ------------------------------------------------------------------
    # Test 11: Accelerator node termination resilience
    # ------------------------------------------------------------------

    def test_accelerator_node_termination_resilience(self):
        """
        Terminate one accelerator EC2 instance while a fusion rebalance is in progress.
        The CP must detect the terminated instance via the ASG health check, relaunch a
        replacement, and the fusion rebalance must continue and complete successfully.

        Uses 200M items (create_end=200000000) so accelerators are deployed long enough
        that termination can be issued while phase 5 (S3 download / hydration) is still
        running.

        Validates:
        - Fusion rebalance takes the accelerator path (instances launched)
        - A specific accelerator instance is successfully terminated via the AWS API
        - A replacement accelerator instance (new InstanceId) is relaunched by the ASG,
          OR the rebalance completes without entering a failed state (both are valid
          recovery outcomes — the CP may finish the phase before ASG replaces the node)
        - The rebalance does NOT transition to any failed state throughout
        - The rebalance completes successfully (cluster reaches 'healthy')
        - EBS guest volumes and ASGs are cleaned up post-rebalance

        Parameters:
        - create_end       — doc count for above-threshold load (default via _load_above_threshold)
        - relaunch_timeout — seconds to wait for a replacement accelerator (default: 600)
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._load_above_threshold()

        rebalance_task = self._trigger_scale_out()
        self.sleep(30, "Wait for rebalance to start")

        # Wait for accelerator instances to appear (phase 4 — deploy accelerators)
        initial_instances = self._poll_until_accelerators_appear(rebalance_task)
        self.assertGreater(
            len(initial_instances), 0,
            "No accelerator instances appeared — fusion rebalance did not launch "
            "accelerators; cannot test termination resilience")

        initial_ids = {inst["InstanceId"] for inst in initial_instances}
        target_instance = initial_instances[0]
        target_id = target_instance["InstanceId"]
        self.log.info(
            f"Terminating accelerator instance {target_id} "
            f"({len(initial_instances)} total accelerators active)")

        terminated = self.fusion_aws_util.ec2.terminate_instance(target_id)
        self.assertTrue(terminated,
                        f"AWS API failed to terminate accelerator instance {target_id}")
        termination_time = time.time()
        self.log.info(
            f"Accelerator instance {target_id} terminated — "
            f"waiting for ASG to relaunch a replacement")

        # Poll until a replacement instance (new InstanceId) appears or the rebalance
        # completes without entering a failed state (both are valid recovery outcomes).
        relaunch_timeout = self.input.param("relaunch_timeout", 600)
        deadline = termination_time + relaunch_timeout
        replacement_seen = False

        while time.time() < deadline:
            if rebalance_task.state in self._FAILED_STATES:
                self.fail(
                    f"Rebalance entered failed state after accelerator termination: "
                    f"{rebalance_task.state}")

            current_instances = self.fusion_aws_util.list_accelerator_instances(
                self._accelerator_filter(), log="RelaunchCheck")
            current_ids = {inst["InstanceId"] for inst in current_instances}
            new_ids = current_ids - initial_ids
            if new_ids:
                elapsed = time.time() - termination_time
                replacement_seen = True
                self.log.info(
                    f"Replacement accelerator instance(s) launched after {elapsed:.1f}s: "
                    f"{new_ids}")
                break

            if rebalance_task.state == "healthy":
                # Rebalance completed before an explicit relaunch was observed —
                # the CP recovered and finished without needing a fresh accelerator.
                replacement_seen = True
                self.log.info(
                    "Rebalance completed before replacement accelerator was observed — "
                    "CP handled termination gracefully without relaunch")
                break

            self.log.info(
                f"Waiting for replacement accelerator — "
                f"current IDs: {current_ids} — "
                f"{int(deadline - time.time())}s remaining")
            time.sleep(10)

        self.assertTrue(
            replacement_seen,
            f"No replacement accelerator appeared within {relaunch_timeout}s after "
            f"terminating {target_id}; rebalance state: {rebalance_task.state}")

        # Assert the rebalance completes successfully despite the disruption
        self.wait_for_rebalances([rebalance_task])
        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id, timeout=600)

        # Verify all EBS guest volumes and ASGs are cleaned up post-rebalance
        cleaned = self.cp_monitor.monitor_ebs_cleanup(
            self.cluster, self.stop_run_event,
            timeout=self.cp_monitor.EBS_CLEANUP_TIMEOUT)
        self.assertTrue(cleaned,
                        "EBS guest volumes were not cleaned up after rebalance")

        self.cp_monitor.check_asg_cleanup_after_rebalance([self.cluster])
        self.log.info(
            "Fusion rebalance completed and all resources cleaned up successfully "
            "after accelerator instance termination")
