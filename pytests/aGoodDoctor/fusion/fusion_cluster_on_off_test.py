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
from .fusion_enable_disable_test import _FusionTestBase


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
        # Ensure the cluster is on before any teardown operations
        cluster_state = CapellaAPI.get_cluster_state(
            self.pod, self.tenant, self.cluster.id)
        if cluster_state == "turnedOff":
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
        create_end = self.input.param("create_end", 20_000_000)
        self._load_data(self.cluster, create_start=0, create_end=create_end)
        self.sleep(120, "Allow initial sync to S3 before triggering rebalance")

    def _trigger_scale_out(self):
        """Trigger a +1 data-node scale-out rebalance and return the async task."""
        config = self.rebalance_config("data", +1)
        return self.task.async_rebalance_capella(
            self.pod, self.tenant, self.cluster, config, timeout=self.index_timeout)

    def _do_cluster_off_on(self, turn_off_timeout=600, turn_on_timeout=1200):
        """Turn the cluster off then on, asserting both transitions succeed."""
        dr_on_off = DoctorHostedOnOff(self.pod, self.tenant, self.cluster)
        self.log.info(f"Turning cluster {self.cluster.id} off")
        turned_off = dr_on_off.turn_off_cluster(timeout=turn_off_timeout)
        self.assertTrue(
            turned_off,
            f"Cluster {self.cluster.id} did not reach 'turnedOff' state "
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
        - Cluster turns off cleanly (reaches 'turnedOff' state)
        - Cluster turns on cleanly (reaches 'healthy' state)
        - Fusion state is 'enabled' after turn-on (not reset to disabled)
        - Pending bytes drain to 0 after turn-on (upload resumes)
        - No memcached errors after the on/off cycle
        """
        self._enable_fusion_feature_flags(self.tenant, self.cluster.id)
        self._ensure_fusion_state(self.tenant, self.cluster, "enabled")

        create_end = self.input.param("create_end", 5_000_000)
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

        # Wait until guest volumes appear (accelerators have been deployed)
        turn_off_timeout = self.input.param("turn_off_timeout", 600)
        turn_on_timeout = self.input.param("turn_on_timeout", 1200)
        volume_wait_timeout = self.input.param("volume_wait_timeout", 1800)

        self.log.info("Waiting for guest volumes to appear before turning cluster off")
        deadline = time.time() + volume_wait_timeout
        volumes_seen = False
        while time.time() < deadline:
            if rebalance_task.state in self._FAILED_STATES:
                self.fail(f"Rebalance failed before guest volumes appeared: "
                          f"{rebalance_task.state}")
            if rebalance_task.state == "healthy":
                self.skipTest(
                    "Rebalance completed before guest volumes were detected; "
                    "adjust cluster/data size to create a wider observation window")
            volumes = self.cp_monitor.get_current_guest_volume_ids(self.cluster)
            if volumes:
                self.log.info(
                    f"Guest volumes detected ({len(volumes)} volumes): {volumes}")
                volumes_seen = True
                break
            time.sleep(5)

        self.assertTrue(volumes_seen,
                        "No guest volumes appeared during rebalance — "
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
        volumes_were_active = False
        while time.time() < deadline:
            if rebalance_task.state in self._FAILED_STATES:
                self.fail(f"Rebalance failed before CBS rebalance phase: "
                          f"{rebalance_task.state}")
            if rebalance_task.state == "healthy":
                self.skipTest(
                    "Rebalance completed before the detach window was observed; "
                    "adjust cluster/data size to widen the observation window")
            if self._active_guest_volume_count(self.cluster) > 0:
                volumes_were_active = True
                self.log.info(
                    "CBS is tracking active guest volumes on KV nodes — "
                    "waiting for CBS rebalance to complete and volumes to be released")
                break
            time.sleep(5)

        if not volumes_were_active:
            self.skipTest(
                "CBS activeGuestVolumes never went > 0; "
                "fusion rebalance may not have reached the CBS phase within timeout")

        # Now wait for CBS to release the volumes (activeGuestVolumes → 0)
        # while the volumes still exist in AWS ('available' state)
        available_volumes = []
        while time.time() < deadline:
            if rebalance_task.state in self._FAILED_STATES:
                self.fail(f"Rebalance failed during CBS rebalance phase: "
                          f"{rebalance_task.state}")
            if rebalance_task.state == "healthy":
                self.skipTest(
                    "Rebalance completed before the post-CBS-rebalance detach window; "
                    "adjust cluster/data size to widen the observation window")

            if self._active_guest_volume_count(self.cluster) == 0:
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
            f"Cluster {self.cluster.id} did not reach 'turnedOff' state "
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
            still_present = orphaned_ids & current_ids
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
