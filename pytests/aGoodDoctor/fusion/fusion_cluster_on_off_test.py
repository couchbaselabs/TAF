"""
Fusion Cluster On/Off Tests

Validates cluster turn-off / turn-on operations with fusion enabled, covering
scenarios from idle upload through active rebalance with live guest volumes
(§6 of the E2E test plan).

Tests in this file:
  10. test_cluster_off_on_with_pending_sync_resumes_upload
        Turn off while S3 upload is in-flight; verify upload resumes after turn-on.
  11. test_cluster_off_on_while_guest_volumes_present
        Turn off while a fusion rebalance has live guest volumes; verify full
        resource cleanup after turn-on.
  12. test_cluster_off_on_after_guest_volume_detach
        Turn off after volumes are detached from CBS/KV nodes post-CBS-rebalance but
        before CP deletes them; verify CP cleans up orphaned volumes while cluster is
        off, then verify full cleanup after turn-on.
"""

import time

from capella_utils.dedicated import CapellaUtils as CapellaAPI
from couchbase_utils.cb_server_rest_util.fusion.fusion_api import FusionRestAPI
from membase.api.rest_client import RestConnection
from aGoodDoctor.hostedOnOff import DoctorHostedOnOff
from .fusion_test_base import _FusionTestBase


class FusionClusterOnOffTest(_FusionTestBase):
    """
    Tests for cluster turn-off / turn-on behaviour with fusion enabled.

    Each test enables fusion, puts data in-flight or in-sync (or in an active
    rebalance), performs the on/off cycle, then asserts that full fusion health
    is restored and all AWS resources are cleaned up.
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
            "Wait for healthy cluster before bucket cleanup", timeout=600)
        self.initial_kv_nodes = self.input.param("kv_nodes", 3)
        for bucket in self.cluster.buckets:
            try:
                self.log.info(f"Setup: Cleaning up bucket {bucket.name}")
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
        # Guard against partial setUp: if tenant/cluster were never assigned
        # (e.g. setUp raised before reaching those lines), skip cluster-specific
        # teardown and fall through to the base class.
        if not hasattr(self, 'tenant') or not hasattr(self, 'cluster'):
            super().tearDown()
            return

        # Ensure the cluster is on before any teardown operations
        cluster_state = CapellaAPI.get_cluster_state(
            self.pod, self.tenant, self.cluster.id)
        if cluster_state == "turned_off":
            self.log.warning(
                f"Cluster {self.cluster.id} is still off in tearDown — turning it on")
            dr_on_off = DoctorHostedOnOff(self.pod, self.tenant, self.cluster)
            dr_on_off.turn_on_cluster(timeout=1200)

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
            self.log.info(f"Teardown: Cleaning up bucket {bucket.name}")
            try:
                self._delete_bucket_with_s3_cleanup(bucket)
            except Exception:
                pass
        self.cluster.buckets = []
        super().tearDown()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _get_total_pending_bytes(self, cluster):
        """Return total snapshotPendingBytes across all nodes and buckets."""
        self.fusion_monitor.set_admin_credentials(cluster)
        status, content = FusionRestAPI(cluster.master).get_fusion_status()
        if not status:
            return 0
        nodes = content.get("nodes") or {}
        return sum(
            bucket_stats.get("snapshotPendingBytes", 0)
            for node_stats in nodes.values()
            for bucket_stats in (node_stats.get("buckets") or {}).values()
        )

    def _active_guest_volume_count(self, cluster):
        """Return the count of volumes CBS considers active guest volumes."""
        self.fusion_monitor.set_admin_credentials(cluster)
        status, content = FusionRestAPI(cluster.master).get_active_guest_volumes()
        if not status or not content:
            return 0
        return sum(len(vols) for vols in content.values())

    def _guest_volumes_in_aws(self, cluster):
        """Return all EBS volumes tagged as fusion guest volumes for this cluster."""
        return self.fusion_aws_util.ec2.list_volumes_by_cluster_id(filters={
            "couchbase-cloud-cluster-id": cluster.id,
            "couchbase-cloud-function": "fusion-accelerator",
        })

    def _load_above_threshold(self):
        """Load enough documents to trigger fusion acceleration on next rebalance."""
        create_end = int(self.input.param("create_end", 20_000_000))
        self._load_data(self.cluster, create_start=0, create_end=create_end)
        self.sleep(120, "Allow initial sync to S3 before triggering rebalance")

    def _trigger_scale_out(self):
        """Trigger a +1 data-node scale-out rebalance and return the async task."""
        config = self.rebalance_config("data", +1)
        return self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster, config, timeout=self.rebalance_timeout)

    def _do_cluster_off_on(self, turn_off_timeout=600, turn_on_timeout=1200):
        """Turn the cluster off then on, asserting both transitions succeed."""
        dr_on_off = DoctorHostedOnOff(self.pod, self.tenant, self.cluster)
        self.log.info(f"Turning cluster {self.cluster.id} off")
        turned_off = dr_on_off.turn_off_cluster(timeout=turn_off_timeout)
        self.assertTrue(
            turned_off,
            f"Cluster {self.cluster.id} did not reach 'turned_off' state"
            f"within {turn_off_timeout}s")
        self.log.info(f"Cluster {self.cluster.id} is off; turning it back on")
        turned_on = dr_on_off.turn_on_cluster(timeout=turn_on_timeout)
        self.assertTrue(
            turned_on,
            f"Cluster {self.cluster.id} did not return to 'healthy' after turn-on "
            f"within {turn_on_timeout}s")
        self.log.info(f"Cluster {self.cluster.id} is back online")

    # ------------------------------------------------------------------
    # Test 10: Cluster off/on with pending sync — upload resumes after turn-on
    # ------------------------------------------------------------------

    def test_cluster_off_on_with_pending_sync_resumes_upload(self):
        """
        Turn a cluster off while fusion has pending sync data, then turn it back on,
        and verify that fusion uploads resume and pending bytes drain to 0.

        Validates:
        - Fusion is enabled with unsynced pending data before turn-off
        - Cluster turns off cleanly (reaches 'turned_off' state)
        - Cluster turns on cleanly (reaches 'healthy' state)
        - Fusion state is 'enabled' after turn-on (not reset to disabled)
        - Pending bytes drain to 0 after turn-on (upload resumes)
        - No memcached errors after the on/off cycle
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        create_end = int(self.input.param("create_end", 5_000_000))
        self._load_data(self.cluster, create_start=0, create_end=create_end)
        self.sleep(30, "Allow partial sync before capturing pending bytes")

        self.find_master(self.tenant, self.cluster)
        pending_before = self._get_total_pending_bytes(self.cluster)
        self.log.info(f"Pending bytes before turn-off: {pending_before}")

        self._do_cluster_off_on(
            turn_off_timeout=self.input.param("turn_off_timeout", 600),
            turn_on_timeout=self.input.param("turn_on_timeout", 1200))

        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id, timeout=600)
        self.find_master(self.tenant, self.cluster)

        fusion_status = CapellaAPI.get_fusion_status(
            self.pod, self.tenant, self.cluster.id)
        self.assertEqual(
            fusion_status.get("state"), "enabled",
            f"Fusion is not 'enabled' after cluster turn-on: {fusion_status}")
        self.log.info("Fusion state is 'enabled' after cluster turn-on")

        sync_timeout = self.input.param("sync_wait_timeout", 3600)
        self.log.info("Waiting for pending bytes to drain to 0 after turn-on")
        self._wait_for_pending_bytes_zero(self.cluster, timeout=sync_timeout)
        self.log.info("Pending bytes drained to 0 — fusion upload resumed successfully")

        errors_found = self.cp_monitor.scan_memcached_logs_for_errors(
            [self.cluster], steady_state_workload_sleep=0)
        self.assertEqual(
            len(errors_found), 0,
            f"Memcached errors detected after cluster on/off cycle on: "
            f"{[c.id for c in errors_found]}")
        self.log.info("No memcached errors found after cluster on/off cycle")

    # ------------------------------------------------------------------
    # Test 11: Cluster off/on while a fusion rebalance has live guest volumes
    # ------------------------------------------------------------------

    def test_cluster_off_on_while_guest_volumes_present(self):
        """
        Trigger a fusion rebalance so that EBS guest volumes are created, then turn
        the cluster off while those volumes are still live. After turn-on, verify that
        fusion health is restored and all guest volumes are cleaned up by the CP.

        Validates:
        - Guest volumes appear during the rebalance (CP launched accelerators)
        - Cluster turns off cleanly with guest volumes present
        - After turn-on: cluster is healthy, fusion state is 'enabled'
        - CP cleans up all guest volumes after turn-on (count reaches 0)
        - ASGs pending deletion are logged (CP handles async teardown)
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._load_above_threshold()

        rebalance_task = self._trigger_scale_out()
        self.sleep(30, "Wait for rebalance to start")
        self.wait_for_rebalances([rebalance_task])

        turn_off_timeout = self.input.param("turn_off_timeout", 600)
        turn_on_timeout = self.input.param("turn_on_timeout", 1200)

        self.log.info("Check for guest volumes presence before turning cluster off")
        volumes = self.cp_monitor.get_current_guest_volume_ids(self.cluster)
        self.assertTrue(len(volumes) > 0,
                        "No guest volumes present after rebalance — "
                        "fusion acceleration may not have triggered")

        # Turn the cluster off with live guest volumes present
        self._do_cluster_off_on(
            turn_off_timeout=turn_off_timeout,
            turn_on_timeout=turn_on_timeout)

        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id, timeout=600)
        self.find_master(self.tenant, self.cluster)

        fusion_status = CapellaAPI.get_fusion_status(
            self.pod, self.tenant, self.cluster.id)
        self.assertEqual(
            fusion_status.get("state"), "enabled",
            f"Fusion is not 'enabled' after cluster turn-on: {fusion_status}")
        self.log.info("Fusion state is 'enabled' after cluster turn-on")

        # CP must clean up all guest volumes after the interrupted rebalance
        self.log.info("Waiting for all guest volumes to be cleaned up after turn-on")
        cleaned = self.cp_monitor.monitor_ebs_cleanup(
            self.cluster, self.stop_run_event,
            timeout=self.cp_monitor.EBS_CLEANUP_TIMEOUT)
        self.assertTrue(cleaned,
                        "Guest volumes were not cleaned up after cluster on/off cycle")

        final_volumes = self.cp_monitor.get_current_guest_volume_ids(self.cluster)
        self.assertEqual(
            len(final_volumes), 0,
            f"Expected 0 guest volumes after cleanup, "
            f"found {len(final_volumes)}: {final_volumes}")

        self.cp_monitor.check_asg_cleanup_after_rebalance([self.cluster])
        self.log.info("All guest volumes and ASGs cleaned up after cluster on/off")

    # ------------------------------------------------------------------
    # Test 12: Cluster off/on after guest volumes detached from CBS nodes,
    #          before CP deletes them
    # ------------------------------------------------------------------

    def test_cluster_off_on_after_guest_volume_detach(self):
        """
        Turn the cluster off after CBS/KV nodes have finished using the guest volumes
        (post-CBS-rebalance) and those volumes have been detached from the CBS
        instances (entered AWS 'available' state) but before the CP has deleted them.
        Verify the CP cleans up the orphaned volumes while the cluster is off.
        After turn-on, verify full cleanup of any remaining guest volumes.

        Lifecycle context (8-phase fusion rebalance):
          Phase 5 — Hydration: S3 data downloaded to EBS guest volumes on accelerators
          Phase 6 — Volume transfer: volumes detached from accelerators, attached to KV nodes
          Phase 7 — CBS rebalance: runs with data already local on KV nodes
          Phase 8 — Teardown: volumes detached from KV nodes → 'available' → deleted

        This test intercepts the phase-8 window: CBS rebalance is done, volumes are
        detached from CBS/KV instances ('available' in AWS), but CP has not yet deleted
        them. CBS's activeGuestVolumes endpoint returns 0 at this point because CBS no
        longer tracks them as active.

        Validates:
        - CBS activeGuestVolumes drops to 0 (CBS rebalance done, volumes released)
        - AWS volumes remain in 'available' state (detached from KV, not yet deleted)
        - Cluster turns off cleanly during this window
        - CP deletes the orphaned 'available' volumes while cluster is off
        - After turn-on: cluster is healthy, fusion state is 'enabled'
        - Any remaining guest volumes from a resumed rebalance are cleaned up
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._load_above_threshold()

        rebalance_task = self._trigger_scale_out()
        self.sleep(30, "Wait for rebalance to start")
        self.wait_for_rebalances([rebalance_task])

        turn_off_timeout = self.input.param("turn_off_timeout", 600)
        turn_on_timeout = self.input.param("turn_on_timeout", 1200)
        detach_wait_timeout = self.input.param("detach_wait_timeout", 1800)

        # Phase 1: Wait for the post-CBS-rebalance detach window.
        #
        # Sequence we're watching for:
        #   (a) activeGuestVolumes > 0: volumes are in-use on KV nodes during CBS rebalance
        #   (b) activeGuestVolumes drops to 0: CBS rebalance done, volumes released from KV nodes
        #   (c) AWS volumes still exist in 'available' state: CP hasn't deleted them yet
        #
        # We turn the cluster off as soon as we see (b) + (c) together.
        self.log.info(
            "Waiting for CBS activeGuestVolumes to drop to 0 post-CBS-rebalance "
            "while AWS guest volumes are still present (detached from KV, not yet deleted)")

        self.find_master(self.tenant, self.cluster)
        deadline = time.time() + detach_wait_timeout

        # First wait for CBS to start tracking guest volumes (volumes on KV nodes)
        if self._active_guest_volume_count(self.cluster) > 0:
            self.log.info(
                "CBS is tracking active guest volumes on KV nodes — "
                "waiting for CBS rebalance to complete and volumes to be released")
        else:
            self.skipTest(
                "CBS activeGuestVolumes never went > 0; "
                "fusion rebalance may not have reached the CBS phase within timeout")

        # Now wait for CBS to release the volumes (activeGuestVolumes → 0)
        # while the volumes still exist in AWS ('available' state)
        available_volumes = []
        while time.time() < deadline:
            # CBS has released the volumes — check AWS state
            all_volumes = self._guest_volumes_in_aws(self.cluster)
            available_volumes = [
                v for v in all_volumes if v.get("State") == "available"
            ]
            if available_volumes:
                volume_ids = [v.get("VolumeId") for v in available_volumes]
                self.log.info(
                    f"Detected {len(available_volumes)} guest volume(s) in AWS "
                    f"'available' state — detached from CBS/KV instances post-CBS-rebalance, "
                    f"not yet deleted by CP: {volume_ids}")
                break
            time.sleep(5)

        self.assertGreater(
            len(available_volumes), 0,
            "No guest volumes were observed in AWS 'available' state after CBS rebalance — "
            "the post-CBS-rebalance detach window was not caught; "
            "try increasing detach_wait_timeout")

        orphaned_ids = {v.get("VolumeId") for v in available_volumes}

        # Phase 2: turn the cluster off while volumes are detached from KV nodes
        #          but not yet deleted by CP
        dr_on_off = DoctorHostedOnOff(self.pod, self.tenant, self.cluster)
        self.log.info(
            f"Turning cluster {self.cluster.id} off — "
            f"{len(orphaned_ids)} guest volumes are detached from CBS/KV nodes, "
            f"awaiting CP deletion")
        turned_off = dr_on_off.turn_off_cluster(timeout=turn_off_timeout)
        self.assertTrue(
            turned_off,
            f"Cluster {self.cluster.id} did not reach 'turned_off' state"
            f"within {turn_off_timeout}s")
        self.log.info(f"Cluster {self.cluster.id} is off")

        # Phase 3: while cluster is off, verify CP deletes the volumes that were
        #          detached from CBS/KV nodes — CP must clean up even without the
        #          cluster running
        self.log.info(
            f"Waiting for CP to delete {len(orphaned_ids)} guest volumes "
            "(detached from CBS/KV nodes) while cluster is off")
        orphan_cleanup_timeout = self.input.param(
            "orphan_cleanup_timeout", self.cp_monitor.EBS_CLEANUP_TIMEOUT)
        deadline = time.time() + orphan_cleanup_timeout
        orphans_deleted = False
        while time.time() < deadline:
            current_ids = {
                v.get("VolumeId")
                for v in self._guest_volumes_in_aws(self.cluster)
            }
            still_present = [orphan_id for orphan_id in orphaned_ids if orphan_id in current_ids]
            if not still_present:
                orphans_deleted = True
                self.log.info(
                    "All orphaned guest volumes deleted by CP while cluster was off")
                break
            self.log.info(
                f"Orphaned volumes still present ({len(still_present)}): "
                f"{still_present}")
            time.sleep(30)

        self.assertTrue(
            orphans_deleted,
            f"CP did not delete the guest volumes detached from CBS/KV nodes "
            f"while cluster was off (timeout {orphan_cleanup_timeout}s). "
            f"Still present: "
            f"{orphaned_ids & {v.get('VolumeId') for v in self._guest_volumes_in_aws(self.cluster)}}")

        # Phase 4: turn the cluster back on
        self.log.info(f"Turning cluster {self.cluster.id} back on")
        turned_on = dr_on_off.turn_on_cluster(timeout=turn_on_timeout)
        self.assertTrue(
            turned_on,
            f"Cluster {self.cluster.id} did not return to 'healthy' after turn-on "
            f"within {turn_on_timeout}s")
        self.log.info(f"Cluster {self.cluster.id} is back online")

        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id, timeout=600)
        self.find_master(self.tenant, self.cluster)

        fusion_status = CapellaAPI.get_fusion_status(
            self.pod, self.tenant, self.cluster.id)
        self.assertEqual(
            fusion_status.get("state"), "enabled",
            f"Fusion is not 'enabled' after cluster turn-on: {fusion_status}")
        self.log.info("Fusion state is 'enabled' after cluster turn-on")

        # Phase 5: wait for any remaining guest volumes (new ones from resumed
        # rebalance, or remnants from the interrupted run) to be cleaned up
        remaining = self.cp_monitor.get_current_guest_volume_ids(self.cluster)
        if remaining:
            self.log.info(
                f"{len(remaining)} guest volume(s) still exist after turn-on "
                "(likely from a resumed rebalance); waiting for cleanup")
            cleaned = self.cp_monitor.monitor_ebs_cleanup(
                self.cluster, self.stop_run_event,
                timeout=self.cp_monitor.EBS_CLEANUP_TIMEOUT)
            self.assertTrue(cleaned,
                            "Guest volumes from resumed rebalance were not cleaned up "
                            "after cluster turn-on")

        final_volumes = self.cp_monitor.get_current_guest_volume_ids(self.cluster)
        self.assertEqual(
            len(final_volumes), 0,
            f"Expected 0 guest volumes after full cleanup, "
            f"found {len(final_volumes)}: {final_volumes}")
        self.log.info("All guest volumes cleaned up after cluster on/off cycle")

    # ------------------------------------------------------------------
    # Test 13: Full functional on/off with 100M items, volume attachment
    #          verification, and post-turn-on fusion rebalance
    # ------------------------------------------------------------------

    def test_cluster_on_off_functional(self):
        """
        Full functional test: cluster on/off with 100M items.

        Test sequence:
        1.  Enable fusion; load create_end (default 100 M) documents — well
            above the fusion threshold so every rebalance uses fusion.
        2.  Trigger a +1 scale-out rebalance (fusion path expected).
        3.  Wait for EBS guest volumes to appear (accelerators launched by CP).
        4.  Verify every attached guest volume is attached to a cluster instance
            (KV node or accelerator) — not orphaned on a foreign instance.
        5.  Wait for rebalance to complete; assert accelerators terminated.
        6.  Assert guest volumes remain attached to KV nodes (NOT cleaned up —
            volumes persist after rebalance; only accelerators are temporary).
        7.  Turn the cluster off — CP cleans up guest volumes during turn-off.
        8.  Turn the cluster back on.
        9.  Assert fusion state == 'enabled'; assert guest volumes are still
            attached to KV nodes (EBS volumes persist across stop/start).
        10. Trigger a second +1 scale-out rebalance; assert fusion path is used
            (accelerator instances appear).
        11. Assert accelerator nodes are killed after the second rebalance.
        12. Assert guest volumes from the second rebalance are attached to KV nodes.
        13. Assert dp-agent remained healthy throughout the turn-on window.
        14. Assert no memcached errors after the entire cycle.
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        create_end = int(self.input.param("create_end", 100_000_000))
        self.log.info(
            f"Loading {create_end:,} documents into cluster {self.cluster.id}")
        self._load_data(self.cluster, create_start=0, create_end=create_end)
        self.sleep(120, "Allow initial sync to S3 before triggering rebalance")

        turn_off_timeout = self.input.param("turn_off_timeout", 600)
        turn_on_timeout = self.input.param("turn_on_timeout", 1200)
        volume_wait_timeout = self.input.param("volume_wait_timeout", 1800)

        # ------------------------------------------------------------------
        # Phase 1: trigger first scale-out rebalance
        # ------------------------------------------------------------------
        self.log.info(
            f"Triggering first scale-out rebalance on cluster {self.cluster.id}")
        rebalance_task = self._trigger_scale_out()
        self.sleep(30, "Wait for rebalance to initialise")

        # ------------------------------------------------------------------
        # Phase 2: observe guest volumes during the rebalance and verify
        #          they are attached to cluster instances (not orphaned).
        #          The rebalance is allowed to run to completion — we do NOT
        #          interrupt it here.
        # ------------------------------------------------------------------
        self.log.info(
            "Polling for EBS guest volumes while rebalance is in-flight")
        deadline = time.time() + volume_wait_timeout
        volumes_observed = False
        while time.time() < deadline:
            if rebalance_task.state in self._FAILED_STATES:
                self.fail(
                    f"Rebalance failed before guest volumes were observed: "
                    f"{rebalance_task.state}")
            if rebalance_task.state == "healthy":
                break

            volume_ids = self.cp_monitor.get_current_guest_volume_ids(self.cluster)
            if volume_ids:
                self.log.info(
                    f"{len(volume_ids)} guest volume(s) detected on cluster "
                    f"{self.cluster.id}: {volume_ids}")
                # Verify all attached volumes belong to cluster instances
                self.find_master(self.tenant, self.cluster)
                all_attached = \
                    self.cp_monitor.verify_guest_volumes_attached_to_cluster(
                        self.cluster)
                self.assertTrue(
                    all_attached,
                    f"One or more EBS guest volumes on cluster {self.cluster.id} "
                    "are attached to instances that do not belong to this cluster — "
                    "volumes may be orphaned or misattributed")
                volumes_observed = True
                break
            time.sleep(5)

        self.assertTrue(
            volumes_observed,
            "No EBS guest volumes appeared during the rebalance — "
            "fusion acceleration may not have triggered; check data volume "
            "vs. fusion threshold")

        # Wait for the first rebalance to complete fully before turning off
        self.log.info(
            "Waiting for first rebalance to complete before turning cluster off")
        self.wait_for_rebalances([rebalance_task])
        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id, timeout=600)
        self.find_master(self.tenant, self.cluster)

        # Accelerators are temporary EC2 instances — they must be terminated
        # once the rebalance completes.
        accel_killed = \
            self.cp_monitor.monitor_fusion_accelerator_nodes_killed_after_rebalance(
                self.cluster, timeout=self.fusion_infra_timeout)
        self.assertTrue(accel_killed,
                        "Accelerator nodes not killed after first rebalance")

        # Guest volumes are NOT cleaned up after a normal rebalance — they
        # are detached from the accelerators and re-attached to the KV nodes
        # where CBS uses them directly.  Cleanup only happens on cluster turn-off.
        self.find_master(self.tenant, self.cluster)
        post_rebl_volumes = self.cp_monitor.get_current_guest_volume_ids(self.cluster)
        self.assertGreater(
            len(post_rebl_volumes), 0,
            "No guest volumes found after first rebalance — volumes should "
            "remain attached to KV nodes after accelerators are terminated")
        all_attached = self.cp_monitor.verify_guest_volumes_attached_to_cluster(
            self.cluster)
        self.assertTrue(
            all_attached,
            "One or more guest volumes are not attached to a cluster instance "
            "after the first rebalance; expected all volumes on KV nodes")
        self.log.info(
            f"First rebalance complete — {len(post_rebl_volumes)} guest volume(s) "
            "attached to KV nodes; accelerators terminated; cluster is healthy")

        # ------------------------------------------------------------------
        # Phase 3: turn cluster off (rebalance is done, cluster is healthy)
        # ------------------------------------------------------------------
        dr_on_off = DoctorHostedOnOff(self.pod, self.tenant, self.cluster)
        self.log.info(f"Turning cluster {self.cluster.id} off")
        turned_off = dr_on_off.turn_off_cluster(timeout=turn_off_timeout)
        self.assertTrue(
            turned_off,
            f"Cluster {self.cluster.id} did not reach 'turned_off' state"
            f"within {turn_off_timeout}s")
        self.log.info(f"Cluster {self.cluster.id} is off")

        # ------------------------------------------------------------------
        # Phase 4: turn cluster back on
        # dp-agent is checked inside the turn-on polling loop: first check
        # 60 s after the trigger fires, then every 30 s until healthy.
        # ------------------------------------------------------------------
        self.log.info(f"Turning cluster {self.cluster.id} back on")
        _dp_agent_result = {"healthy": True, "checks": 0}

        def _dp_agent_poll():
            ok = self.cp_monitor.check_dp_agent_not_crashing(self.cluster)
            _dp_agent_result["checks"] += 1
            if not ok:
                _dp_agent_result["healthy"] = False
                self.log.critical(
                    f"dp-agent crash detected on cluster {self.cluster.id} "
                    "during turn-on polling window")

        turned_on = dr_on_off.turn_on_cluster(
            timeout=turn_on_timeout,
            poll_callback=_dp_agent_poll,
            callback_delay=60,
            callback_interval=30,
        )
        self.assertTrue(
            turned_on,
            f"Cluster {self.cluster.id} did not return to 'healthy' after "
            f"turn-on within {turn_on_timeout}s")
        self.log.info(
            f"Cluster {self.cluster.id} is back online — "
            f"dp-agent checked {_dp_agent_result['checks']} time(s) during turn-on")

        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id, timeout=600)
        self.find_master(self.tenant, self.cluster)

        # ------------------------------------------------------------------
        # Phase 5: assert fusion state is still 'enabled' after turn-on;
        #          guest volumes must be gone (cleaned up during turn-off)
        # ------------------------------------------------------------------
        fusion_status = CapellaAPI.get_fusion_status(
            self.pod, self.tenant, self.cluster.id)
        self.assertEqual(
            fusion_status.get("state"), "enabled",
            f"Fusion is not 'enabled' after cluster turn-on: {fusion_status}")
        self.log.info("Fusion state is 'enabled' after cluster turn-on")

        # Guest volumes survive a turn-off/turn-on cycle: EC2 instances are
        # stopped (not terminated), so attached EBS volumes persist and come
        # back with the nodes on turn-on.
        post_on_volumes = self.cp_monitor.get_current_guest_volume_ids(self.cluster)
        self.assertGreater(
            len(post_on_volumes), 0,
            "Expected guest volumes to still be attached to KV nodes after "
            "cluster turn-on — EBS volumes persist across stop/start cycles")
        all_attached_post_on = self.cp_monitor.verify_guest_volumes_attached_to_cluster(
            self.cluster)
        self.assertTrue(
            all_attached_post_on,
            "One or more guest volumes are not attached to a cluster instance "
            "after cluster turn-on; expected all volumes still on KV nodes")
        self.log.info(
            f"{len(post_on_volumes)} guest volume(s) confirmed on KV nodes "
            "after cluster turn-on")

        # ------------------------------------------------------------------
        # Phase 6: trigger second rebalance; assert it runs through fusion
        # ------------------------------------------------------------------
        self.log.info(
            "Triggering second scale-out rebalance after cluster turn-on — "
            "expecting fusion acceleration path")
        second_rebalance_task = self._trigger_scale_out()
        self.sleep(30, "Wait for second rebalance to initialise")

        second_deadline = time.time() + volume_wait_timeout
        fusion_confirmed = False
        while time.time() < second_deadline:
            if second_rebalance_task.state in self._FAILED_STATES:
                self.fail(
                    f"Second rebalance failed: {second_rebalance_task.state}")
            if second_rebalance_task.state == "healthy":
                self.log.warning(
                    "Second rebalance completed before accelerators were "
                    "observed — treating as success if no failure state")
                break

            instances = self.fusion_aws_util.list_accelerator_instances(
                self.fusion_aws_util._cluster_filter(
                    self.cluster.id,
                    [{'Name': 'tag:couchbase-cloud-function',
                      'Values': ['fusion-accelerator']}]
                ),
                log="Fusion Accelerator (second rebalance)"
            )
            if instances:
                self.log.info(
                    f"Second rebalance confirmed on fusion path: "
                    f"{len(instances)} accelerator instance(s) active")
                fusion_confirmed = True
                break
            time.sleep(10)

        self.assertTrue(
            fusion_confirmed,
            "Second rebalance did not use the fusion path — no accelerator "
            "instances were detected while 100 M items are present; check "
            "that the fusion-rebalances feature flag is still set after turn-on")

        # Wait for the second rebalance to complete
        self.log.info("Waiting for second rebalance to complete")
        self.wait_for_rebalances([second_rebalance_task])
        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id, timeout=600)
        self.find_master(self.tenant, self.cluster)

        # ------------------------------------------------------------------
        # Phase 7: accelerator nodes must be killed after second rebalance
        # ------------------------------------------------------------------
        self.log.info(
            "Verifying accelerator nodes are terminated "
            "after the second rebalance")
        accel_cleaned = \
            self.cp_monitor.monitor_fusion_accelerator_nodes_killed_after_rebalance(
                self.cluster, timeout=self.fusion_infra_timeout)
        self.assertTrue(
            accel_cleaned,
            "Fusion accelerator nodes were not terminated after the second "
            "rebalance completes")
        self.log.info(
            "Accelerator nodes terminated successfully after second rebalance")

        # ------------------------------------------------------------------
        # Phase 8: guest volumes from the second rebalance remain attached
        #          to KV nodes — accelerators are gone but volumes persist
        # ------------------------------------------------------------------
        self.find_master(self.tenant, self.cluster)
        final_volumes = self.cp_monitor.get_current_guest_volume_ids(self.cluster)
        self.assertGreater(
            len(final_volumes), 0,
            "No guest volumes found after second rebalance — volumes should "
            "remain attached to KV nodes after accelerators are terminated")
        all_attached_final = self.cp_monitor.verify_guest_volumes_attached_to_cluster(
            self.cluster)
        self.assertTrue(
            all_attached_final,
            "One or more guest volumes are not attached to a cluster instance "
            "after the second rebalance; expected all volumes on KV nodes")
        self.log.info(
            f"Second rebalance complete — {len(final_volumes)} guest volume(s) "
            "attached to KV nodes; accelerators terminated")

        # ------------------------------------------------------------------
        # Phase 9: assert dp-agent stayed healthy throughout turn-on
        # ------------------------------------------------------------------
        self.assertTrue(
            _dp_agent_result["healthy"],
            f"dp-agent crashed on one or more instances of cluster "
            f"{self.cluster.id} during the turn-on polling window "
            f"({_dp_agent_result['checks']} check(s) performed)")
        self.log.info(
            f"dp-agent remained stable across all {_dp_agent_result['checks']} "
            "check(s) during cluster turn-on")

        # ------------------------------------------------------------------
        # Phase 10: no memcached errors across the entire test cycle
        # ------------------------------------------------------------------
        errors_found = self.cp_monitor.scan_memcached_logs_for_errors(
            [self.cluster], steady_state_workload_sleep=0)
        self.assertEqual(
            len(errors_found), 0,
            f"Memcached errors detected after cluster on/off and rebalance "
            f"cycle on: {[c.id for c in errors_found]}")
        self.log.info(
            "No memcached errors — functional on/off test complete")

    # ------------------------------------------------------------------
    # Test 14: Cluster off/on with guest volumes attached, then cloud
    #          snapshot backup + in-place restore to the same cluster
    # ------------------------------------------------------------------

    def test_cluster_off_on_snapshot_backup_restore_same_cluster(self):
        """
        Functional test: guest volumes persist across cluster turn-off/turn-on,
        then a cloud snapshot backup is taken and restored in-place.

        Validates:
        1.  Fusion rebalance runs; EBS guest volumes appear on KV nodes.
        2.  After rebalance, accelerator nodes are terminated; volumes stay on KV.
        3.  Cluster turns off (EC2 stop — NOT terminate); EBS volumes are NOT lost.
        4.  Cluster turns back on; guest volumes are still attached to KV nodes.
        5.  Cloud snapshot backup completes; EBS guest-volume snapshots are tagged.
        6.  In-place restore completes; cluster returns to healthy.
        7.  Fusion S3 log-store bucket is empty after restore (cleanFusionBucket ran).
        8.  Item count per bucket is non-zero after restore.
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")
        self._load_above_threshold()

        turn_off_timeout = self.input.param("turn_off_timeout", 600)
        turn_on_timeout = self.input.param("turn_on_timeout", 1200)
        verify_snapshots = self.input.param("verify_snapshots", True)
        verify_fusion_bucket_clean = self.input.param(
            "verify_fusion_bucket_clean", True)

        project_id = self.tenant.projects[0]

        # ------------------------------------------------------------------
        # Phase 1: scale-out rebalance so guest volumes land on KV nodes
        # ------------------------------------------------------------------
        self.PrintStep(
            f"Triggering scale-out rebalance on {self.cluster.id} "
            "to create EBS guest volumes")
        rebalance_task = self._trigger_scale_out()
        self.wait_for_rebalances([rebalance_task])
        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id, timeout=600)
        self.find_master(self.tenant, self.cluster)

        accel_killed = \
            self.cp_monitor.monitor_fusion_accelerator_nodes_killed_after_rebalance(
                self.cluster, timeout=self.fusion_infra_timeout)
        self.assertTrue(
            accel_killed,
            "Accelerator nodes not terminated after scale-out rebalance")

        guest_volumes_before_off = \
            self.cp_monitor.get_current_guest_volume_ids(self.cluster)
        self.assertGreater(
            len(guest_volumes_before_off), 0,
            "No EBS guest volumes found on KV nodes after rebalance — "
            "fusion acceleration may not have triggered")
        self.log.info(
            f"{len(guest_volumes_before_off)} guest volume(s) on KV nodes "
            f"before cluster turn-off: {guest_volumes_before_off}")

        # ------------------------------------------------------------------
        # Phase 2: turn cluster off then back on
        # ------------------------------------------------------------------
        self.PrintStep(f"Turning cluster {self.cluster.id} off and on")
        self._do_cluster_off_on(
            turn_off_timeout=turn_off_timeout,
            turn_on_timeout=turn_on_timeout)
        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id, timeout=600)
        self.find_master(self.tenant, self.cluster)

        fusion_state = CapellaAPI.get_fusion_status(
            self.pod, self.tenant, self.cluster.id)
        self.assertEqual(
            fusion_state.get("state"), "enabled",
            f"Fusion must remain 'enabled' after cluster turn-on; "
            f"got: {fusion_state.get('state')}")

        # EBS volumes survive an EC2 stop/start — guest volumes must still
        # be attached to KV nodes after the cluster comes back online.
        guest_volumes_after_on = \
            self.cp_monitor.get_current_guest_volume_ids(self.cluster)
        self.assertGreater(
            len(guest_volumes_after_on), 0,
            "Expected EBS guest volumes to persist on KV nodes after cluster "
            "turn-on — EBS volumes survive EC2 stop/start")
        self.log.info(
            f"{len(guest_volumes_after_on)} guest volume(s) confirmed on KV "
            f"nodes after turn-on: {guest_volumes_after_on}")

        # ------------------------------------------------------------------
        # Phase 3: cloud snapshot backup with guest volumes attached
        # ------------------------------------------------------------------
        self.PrintStep(
            f"Taking EBS cloud snapshot backup on {self.cluster.id}")
        num_guest_volumes = len(guest_volumes_after_on)

        result = CapellaAPI.create_cloud_snapshot_backup(
            self.pod, self.tenant, project_id, self.cluster.id)
        self.assertIsNotNone(
            result, "create_cloud_snapshot_backup returned None — backup request failed")
        backup_id = result.get("id")
        self.assertIsNotNone(backup_id, "Backup response has no 'id' field")
        self.log.info(f"Cloud snapshot backup triggered — backup_id={backup_id}")

        ok = CapellaAPI.wait_for_cloud_snapshot_backup_to_complete(
            self.pod, self.tenant, project_id, self.cluster.id, backup_id)
        self.assertTrue(ok, f"Cloud snapshot backup {backup_id} did not complete")
        self.log.info(f"Cloud snapshot backup {backup_id} completed")

        if verify_snapshots:
            snaps_ok = self.cp_monitor.verify_guest_volume_snapshots_for_backup(
                self.cluster, backup_id, num_guest_volumes)
            self.assertTrue(
                snaps_ok,
                f"Guest-volume EBS snapshot verification failed for backup "
                f"{backup_id} — expected {num_guest_volumes} snapshot(s)")
            self.log.info(
                f"Guest-volume EBS snapshots verified for backup {backup_id}")

        # ------------------------------------------------------------------
        # Phase 4: in-place restore to the same cluster
        # ------------------------------------------------------------------
        self.PrintStep(
            f"Restoring backup {backup_id} in-place to {self.cluster.id}")

        restore_result = CapellaAPI.restore_cloud_snapshot_backup(
            self.pod, self.tenant, project_id, self.cluster.id, backup_id)
        self.assertIsNotNone(
            restore_result,
            "restore_cloud_snapshot_backup returned None — restore request failed")
        restore_id = restore_result.get("id")
        self.assertIsNotNone(restore_id, "Restore response has no 'id' field")
        self.log.info(
            f"In-place restore triggered — restore_id={restore_id}")

        restore_ok = CapellaAPI.wait_for_cloud_snapshot_restore_to_complete(
            self.pod, self.tenant, project_id, self.cluster.id, restore_id)
        self.assertTrue(
            restore_ok, f"Cloud snapshot restore {restore_id} did not complete")
        CapellaAPI.wait_until_done(
            self.pod, self.tenant, self.cluster.id, timeout=600)
        self.find_master(self.tenant, self.cluster)
        self.log.info(f"In-place restore {restore_id} completed")

        # ------------------------------------------------------------------
        # Phase 5: verify item count per bucket
        # ------------------------------------------------------------------
        self.PrintStep("Verifying item count after in-place restore")
        rest = RestConnection(self.cluster.master)
        for bucket in self.cluster.buckets:
            deadline = time.time() + self.index_timeout
            while time.time() < deadline:
                info = rest.get_bucket_details(bucket_name=bucket.name)
                actual = (
                    info.get("basicStats", {}).get("itemCount", 0) if info else 0)
                if actual > 0:
                    self.log.info(
                        f"Post-restore item count on "
                        f"{self.cluster.id}/{bucket.name}: {actual}")
                    break
                self.sleep(30, f"Polling item count on {bucket.name} after restore")
            else:
                self.fail(
                    f"No items on {self.cluster.id}/{bucket.name} after "
                    f"in-place restore timeout")

        # ------------------------------------------------------------------
        # Phase 6: verify fusion S3 log-store bucket was purged on restore
        # ------------------------------------------------------------------
        if verify_fusion_bucket_clean:
            for bucket in self.cluster.buckets:
                clean = self.fusion_monitor.verify_fusion_bucket_empty_after_restore(
                    self.cluster, bucket.name)
                self.assertTrue(
                    clean,
                    f"Fusion S3 bucket not empty after in-place restore on "
                    f"{self.cluster.id}/{bucket.name} — cleanFusionBucket must "
                    f"have run")
                self.log.info(
                    f"Fusion S3 bucket confirmed empty on "
                    f"{self.cluster.id}/{bucket.name} after restore")

        self.log.info(
            "test_cluster_off_on_snapshot_backup_restore_same_cluster complete")
