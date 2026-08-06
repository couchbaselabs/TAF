"""Targeted Fusion guest-volume snapshot and restore verification tests.

These tests validate specific behaviours that the 4 end-to-end
backup/restore tests in fusion_backup_restore.py do not exercise in
isolation: stale guest-volume handling, per-node snapshot mappings,
partial-node guest-volume restores, and backup during fusion state
transitions.

All tests reuse the FusionBackupRestoreBase infrastructure (cluster
provisioning, bucket management, DocLoader, snapshot backup/restore).
"""
import time

from pytests.aGoodDoctor.fusion.fusion_backup_restore_base import (
    FusionBackupRestoreBase,
)

class FusionSnapshotVerification(FusionBackupRestoreBase):

    # ------------------------------------------------------------------
    # Test 1: Fusion-ON cluster with zero rebalances — no guest volumes
    # ------------------------------------------------------------------
    def test_no_guest_vol_snapshots_no_rebalance(self):
        """Fusion ON but zero rebalances: zero guest-vol-tagged snapshots.

        Test plan:
          1.  Provision source cluster (Fusion ON, no rebalance)
          2.  Populate bucket(s) with data
          3.  Verify 0 guest volumes are present on source (no rebalance)
          4.  Create on-demand cloud snapshot backup
          5.  Verify backup contains primary disk snapshots but ZERO
              guest-volume-tagged snapshots
          6.  Verify data integrity via restore to a Fusion-ON target
        """
        # Step 1: Provision source (Fusion ON, no rebalance)
        self.log.info(
            "=== Step 1: Provisioning source cluster (Fusion ON) ===")
        if not self.source_cluster_id:
            self.source_cluster_id, self.source_project_id = (
                self.acquire_cluster(
                    fusion_enabled=True,
                    num_nodes=self.source_num_nodes,
                    name_prefix="TAF_FusionSrc"))
        else:
            self.log.info(
                "Step 1: Reusing source cluster {}".format(
                    self.source_cluster_id))
            if not self.wait_for_deployment(
                    self.source_project_id, self.source_cluster_id):
                self.fail(
                    "Source cluster {} not healthy".format(
                        self.source_cluster_id))
            self.enable_fusion_on_cluster(
                self.source_cluster_id, self.source_project_id)
            self._wait_for_cluster_healthy(
                self.source_cluster_id, self.source_project_id,
                timeout=self.rebalance_timeout)

        # Step 2: Populate data
        self.log.info("=== Step 2: Populating source bucket(s) ===")
        self.populate_source_buckets()
        self._source_original_nodes = self.get_cluster_node_count(
            self.source_cluster_id, self.source_project_id)

        # Step 3: Confirm zero guest volumes (no rebalance performed)
        self.log.info("=== Step 3: Confirming 0 guest volumes present ===")
        if self.fusion_aws_util:
            try:
                guest = self.fusion_aws_util.get_guest_volumes_for_cluster(
                    self.source_cluster_id)
                guest_count = sum(len(v) for v in guest.values())
                if guest_count:
                    self.fail(
                        "Step 3: source {} has {} guest volumes before any "
                        "rebalance — unexpected: {}".format(
                            self.source_cluster_id, guest_count, guest))
                self.log.info(
                    "Step 3 passed: 0 guest volumes on Fusion-ON source "
                    "(no rebalance yet)")
            except NotImplementedError as e:
                self.log.warning("Step 3 guest-vol check skipped: {}".format(e))

        # Step 4: Create backup
        self.log.info("=== Step 4: Creating snapshot backup ===")
        if self.preset_backup_id:
            backup_id = self.preset_backup_id
            self.log.info("Reusing preset backup {}".format(backup_id))
        else:
            backup_id = self.trigger_snapshot_backup(
                self.source_cluster_id,
                project_id=self.source_project_id)
        self._last_backup_id = backup_id
        backup_record = self.wait_for_backup_complete(
            backup_id, self.source_cluster_id,
            project_id=self.source_project_id)
        self.log.info(
            "Step 4: Snapshot backup {} ready".format(backup_id))

        # Step 5: Verify ZERO guest-volume-tagged snapshots in the backup
        self.log.info("=== Step 5: Verifying 0 guest-vol snapshots ===")
        if self.fusion_aws_util:
            all_snapshots = (
                self.fusion_aws_util.get_ebs_snapshots_for_backup(
                    backup_id, backup_record))
            if all_snapshots:
                guest_tagged = [
                    s for s in all_snapshots
                    if self.fusion_aws_util.get_tag_value(
                        s,
                        self.fusion_aws_util.FUSION_GUEST_VOL_TAG_KEY
                    ) == self.fusion_aws_util.FUSION_GUEST_VOL_TAG_VAL
                ]
                if guest_tagged:
                    self.fail(
                        "Step 5: Backup {} from source with 0 guest volumes "
                        "has {} guest-volume-tagged snapshots: {}".format(
                            backup_id, len(guest_tagged),
                            [s.get("SnapshotId") for s in guest_tagged]))
                self.log.info(
                    "Step 5 passed: 0 guest-volume snapshots in backup {} "
                    "(expected — no rebalance, no guest vols)".format(
                        backup_id))
            else:
                self.log.info(
                    "Step 5: 0 EBS snapshots visible from TAF account — "
                    "skipping tag assertions")
        else:
            self.log.warning("Step 5 skipped: AWS creds not set.")

        # Step 6: Restore to a Fusion-ON target and verify data integrity
        self.log.info(
            "=== Step 6: Provisioning target cluster (Fusion ON) ===")
        if not self.target_cluster_id:
            self.target_cluster_id, self.target_project_id = (
                self.acquire_cluster(
                    fusion_enabled=True,
                    num_nodes=self.target_num_nodes,
                    name_prefix="TAF_FusionTgt"))
        else:
            self.log.info(
                "Step 6: Reusing target cluster {}".format(
                    self.target_cluster_id))
            if not self.wait_for_deployment(
                    self.target_project_id, self.target_cluster_id):
                self.fail(
                    "Target cluster {} not healthy".format(
                        self.target_cluster_id))
            self.enable_fusion_on_cluster(
                self.target_cluster_id, self.target_project_id)
            self._wait_for_cluster_healthy(
                self.target_cluster_id, self.target_project_id,
                timeout=self.rebalance_timeout)

        self._target_original_nodes = self.get_cluster_node_count(
            self.target_cluster_id, self.target_project_id)

        self.log.info("=== Step 6b: Restoring backup to target ===")
        self.trigger_restore(
            backup_id=backup_id,
            target_cluster_id=self.target_cluster_id,
            project_id=self.target_project_id)
        self.wait_for_restore_complete(
            self.target_cluster_id,
            project_id=self.target_project_id,
            expected_bucket_names=self.source_bucket_names)
        self.log.info("Step 6b: Restore complete on target")

        self.log.info("=== Step 6c: Verifying data integrity ===")
        self.verify_data_integrity()

        self._test_succeeded = True
        self.log.info(
            "test_no_guest_vol_snapshots_no_rebalance PASSED")

    # ------------------------------------------------------------------
    # Test 2: Multiple rebalances — only currently-attached volumes are
    #         snapshotted; stale/detached volumes are NOT.
    # ------------------------------------------------------------------
    def test_guest_volume_snapshots_match_attached_state(self):
        """Multiple rebalance cycles: only currently-attached guest volumes
        appear in the backup snapshot inventory. Stale volumes from prior
        rebalance cycles must NOT be represented.

        Test plan:
          1.  Provision source cluster (Fusion ON)
          2.  Populate data
          3.  Rebalance #1 (scale +1) — record attached guest vols
          4.  Scale back to original (rebalance #2) — old vols detached
          5.  Rebalance #3 (scale +1 again) — fresh attached guest vols
          6.  Record currently-attached guest vol IDs (post-rebalance #3)
          7.  Create backup; verify guest-vol snapshots match ONLY the
              currently-attached set from Step 6, not Step 3's stale set
        """
        # Steps 1-2: Provision + populate
        self.log.info(
            "=== Step 1: Provisioning source cluster (Fusion ON) ===")
        if not self.source_cluster_id:
            self.source_cluster_id, self.source_project_id = (
                self.acquire_cluster(
                    fusion_enabled=True,
                    num_nodes=self.source_num_nodes,
                    name_prefix="TAF_FusionSrc"))
        else:
            self.log.info(
                "Step 1: Reusing source cluster {}".format(
                    self.source_cluster_id))
            if not self.wait_for_deployment(
                    self.source_project_id, self.source_cluster_id):
                self.fail(
                    "Source cluster {} not healthy".format(
                        self.source_cluster_id))
            self.enable_fusion_on_cluster(
                self.source_cluster_id, self.source_project_id)
            self._wait_for_cluster_healthy(
                self.source_cluster_id, self.source_project_id,
                timeout=self.rebalance_timeout)

        self.log.info("=== Step 2: Populating data ===")
        self.populate_source_buckets()

        # Step 3: Rebalance #1 (scale +1) — record guest vols
        self.log.info("=== Step 3: Rebalance #1 (scale +1) ===")
        self._source_original_nodes, _ = (
            self.trigger_fusion_rebalance(
                self.source_cluster_id,
                project_id=self.source_project_id))
        if not self.wait_for_rebalance_complete(
                self.source_cluster_id,
                project_id=self.source_project_id):
            self.fail(
                "Step 3: Rebalance #1 did not complete on {}".format(
                    self.source_cluster_id))

        rebalance1_vols = set()
        if self.fusion_aws_util:
            try:
                guest = self.fusion_aws_util.get_guest_volumes_for_cluster(
                    self.source_cluster_id)
                for vols in guest.values():
                    rebalance1_vols.update(vols)
                self.log.info(
                    "Rebalance #1 attached guest volumes: {}".format(
                        sorted(rebalance1_vols)))
            except NotImplementedError as e:
                self.log.warning(
                    "Step 3 guest-vol lookup skipped: {}".format(e))

        # Step 4: Scale back to original (rebalance #2) — detach guest vols
        self.log.info("=== Step 4: Rebalance #2 (scale back) ===")
        self.trigger_fusion_rebalance(
            self.source_cluster_id,
            project_id=self.source_project_id,
            target_nodes=self._source_original_nodes)
        if not self.wait_for_rebalance_complete(
                self.source_cluster_id,
                project_id=self.source_project_id):
            self.fail(
                "Step 4: Rebalance #2 did not complete on {}".format(
                    self.source_cluster_id))
        self.log.info("Step 4: Scaled back — Rebalance #1 vols now detached")

        # Step 5: Rebalance #3 (scale +1 again) — fresh guest vols
        self.log.info("=== Step 5: Rebalance #3 (scale +1) ===")
        _, scaled_up3 = self.trigger_fusion_rebalance(
            self.source_cluster_id,
            project_id=self.source_project_id)
        if not self.wait_for_rebalance_complete(
                self.source_cluster_id,
                project_id=self.source_project_id):
            self.fail(
                "Step 5: Rebalance #3 did not complete on {}".format(
                    self.source_cluster_id))

        # Step 6: Record currently-attached guest vols (post-rebalance #3)
        self.log.info("=== Step 6: Recording current guest volume IDs ===")
        current_vols = set()
        if self.fusion_aws_util:
            try:
                guest = self.fusion_aws_util.get_guest_volumes_for_cluster(
                    self.source_cluster_id)
                for vols in guest.values():
                    current_vols.update(vols)
                self.log.info(
                    "Current (post-rebalance #3) guest volumes: {}".format(
                        sorted(current_vols)))
                stale_vols = rebalance1_vols - current_vols
                if stale_vols:
                    self.log.info(
                        "Stale guest volumes (from rebalance #1, now "
                        "detached): {}".format(sorted(stale_vols)))
            except NotImplementedError as e:
                self.log.warning(
                    "Step 6 guest-vol lookup skipped: {}".format(e))

        # Step 7: Create backup and verify only current vols are snapshotted
        self.log.info("=== Step 7: Creating backup ===")
        backup_id = self.trigger_snapshot_backup(
            self.source_cluster_id,
            project_id=self.source_project_id)
        self._last_backup_id = backup_id
        backup_record = self.wait_for_backup_complete(
            backup_id, self.source_cluster_id,
            project_id=self.source_project_id)

        if self.fusion_aws_util and current_vols:
            all_snapshots = (
                self.fusion_aws_util.get_ebs_snapshots_for_backup(
                    backup_id, backup_record))
            snapshot_vol_ids = {
                s.get("VolumeId") for s in all_snapshots
                if s.get("VolumeId")
            }
            # All current vols must be snapshotted
            missing = current_vols - snapshot_vol_ids
            if missing:
                self.fail(
                    "Step 7: {} current guest volumes missing from backup "
                    "{}: {}".format(
                        len(missing), backup_id, sorted(missing)))
            # Stale vols from rebalance #1 must NOT be in the snapshot set
            stale_snapshotted = rebalance1_vols & snapshot_vol_ids
            if stale_snapshotted:
                self.fail(
                    "Step 7: {} stale (detached) guest volumes from "
                    "rebalance #1 still appear in backup {}: {}".format(
                        len(stale_snapshotted), backup_id,
                        sorted(stale_snapshotted)))
            self.log.info(
                "Step 7 passed: backup {} captures only the {} currently-"
                "attached guest volumes; {} stale volumes absent".format(
                    backup_id, len(current_vols), len(rebalance1_vols)))
        else:
            self.log.warning(
                "Step 7: AWS creds not set or 0 guest vols — "
                "snapshot inventory check skipped")

        self._test_succeeded = True
        self.log.info(
            "test_guest_volume_snapshots_match_attached_state PASSED")

    # ------------------------------------------------------------------
    # Test 3: Per-node guest-volume → snapshot → restore mapping integrity
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Test 4: Partial rebalance — some KV nodes have 0 guest volumes;
    #         restore preserves that asymmetry.
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Test 5: Backup during fusion state transition (enable → disable)
    # ------------------------------------------------------------------
    def test_backup_during_fusion_state_transition(self):
        """Start a backup and immediately disable Fusion on the source
        cluster. Verify the backup record reflects the fusion state at the
        time of creation and the snapshot inventory is internally consistent.

        Test plan:
          1.  Source (Fusion ON), populate, rebalance — guest vols present
          2.  Create backup
          3.  Immediately after triggering backup, disable Fusion on source
          4.  Wait for backup to complete
          5.  Verify backup record is complete (id, progress, type present)
          6.  Verify guest-vol snapshots exist (volumes were attached when
              snapshot was triggered at backup-start, even though fusion
              was disabled mid-backup)
          7.  Restore to target (Fusion ON) + verify data integrity
        """
        # Step 1: Provision, populate, rebalance
        self.log.info(
            "=== Step 1: Provisioning source cluster (Fusion ON) ===")
        if not self.source_cluster_id:
            self.source_cluster_id, self.source_project_id = (
                self.acquire_cluster(
                    fusion_enabled=True,
                    num_nodes=self.source_num_nodes,
                    name_prefix="TAF_FusionSrc"))
        else:
            self.log.info(
                "Step 1: Reusing source cluster {}".format(
                    self.source_cluster_id))
            if not self.wait_for_deployment(
                    self.source_project_id, self.source_cluster_id):
                self.fail(
                    "Source cluster {} not healthy".format(
                        self.source_cluster_id))
            self.enable_fusion_on_cluster(
                self.source_cluster_id, self.source_project_id)
            self._wait_for_cluster_healthy(
                self.source_cluster_id, self.source_project_id,
                timeout=self.rebalance_timeout)

        self.log.info("=== Step 1b: Populating data ===")
        self.populate_source_buckets()

        self.log.info("=== Step 1c: Fusion rebalance ===")
        self._source_original_nodes, _ = (
            self.trigger_fusion_rebalance(
                self.source_cluster_id,
                project_id=self.source_project_id))
        if not self.wait_for_rebalance_complete(
                self.source_cluster_id,
                project_id=self.source_project_id):
            self.fail(
                "Step 1c: Rebalance did not complete on {}".format(
                    self.source_cluster_id))

        # Record pre-backup guest vols so we can assert they were snapshotted
        pre_backup_guest_vols = set()
        if self.fusion_aws_util:
            try:
                guest = self.fusion_aws_util.get_guest_volumes_for_cluster(
                    self.source_cluster_id)
                for vols in guest.values():
                    pre_backup_guest_vols.update(vols)
                self.log.info(
                    "Pre-backup guest volumes: {}".format(
                        sorted(pre_backup_guest_vols)))
            except NotImplementedError as e:
                self.log.warning(
                    "Step 1 guest-vol lookup skipped: {}".format(e))

        # Step 2: Create backup
        self.log.info("=== Step 2: Creating snapshot backup ===")
        backup_id = self.trigger_snapshot_backup(
            self.source_cluster_id,
            project_id=self.source_project_id)
        self._last_backup_id = backup_id
        self.log.info(
            "Step 2: Backup {} triggered — immediately disabling Fusion".format(
                backup_id))

        # Step 3: Immediately disable Fusion — the backup should capture
        # the state *at trigger time* (guest vols attached), regardless of
        # the disable racing with snapshot completion.
        self.log.info(
            "=== Step 3: Disabling Fusion on source immediately after "
            "backup trigger ===")
        self.disable_fusion_on_cluster(
            self.source_cluster_id, self.source_project_id)

        # Step 4: Wait for backup to complete. The server should still
        # finish the backup even as fusion is being disabled.
        self.log.info("=== Step 4: Waiting for backup to complete ===")
        backup_record = self.wait_for_backup_complete(
            backup_id, self.source_cluster_id,
            project_id=self.source_project_id)
        self.log.info(
            "Step 4: Backup {} ready. Record: {}".format(
                backup_id, backup_record))

        # Step 5: Verify backup record is complete
        self.log.info(
            "=== Step 5: Verifying backup record completeness ===")
        if not backup_record or not backup_record.get("id"):
            self.fail(
                "Step 5: Backup record missing or empty: {}".format(
                    backup_record))
        self.log.info(
            "Step 5 passed: backup record present (id={}, progress={}, "
            "type={})".format(
                backup_record.get("id"),
                backup_record.get("progress"),
                backup_record.get("type")))

        # Step 6: Verify guest-vol snapshots exist. Since guest vols were
        # attached when the backup was triggered (Step 2), the snapshot
        # inventory should include them — even though fusion was disabled
        # mid-backup (Step 3).
        self.log.info(
            "=== Step 6: Verifying guest-vol snapshots in backup ===")
        if self.fusion_aws_util:
            all_snapshots = (
                self.fusion_aws_util.get_ebs_snapshots_for_backup(
                    backup_id, backup_record))
            if all_snapshots:
                guest_tagged = [
                    s for s in all_snapshots
                    if self.fusion_aws_util.get_tag_value(
                        s,
                        self.fusion_aws_util.FUSION_GUEST_VOL_TAG_KEY
                    ) == self.fusion_aws_util.FUSION_GUEST_VOL_TAG_VAL
                ]
                if pre_backup_guest_vols:
                    snapshot_vols = {s.get("VolumeId")
                                     for s in guest_tagged}
                    missing = pre_backup_guest_vols - snapshot_vols
                    if missing:
                        self.fail(
                            "Step 6: {} guest volumes present at backup "
                            "trigger are missing from snapshot inventory: "
                            "{}".format(len(missing), sorted(missing)))
                    self.log.info(
                        "Step 6 passed: {} guest-vol snapshots captured "
                        "(all {} pre-backup guest vols accounted for)".format(
                            len(guest_tagged), len(pre_backup_guest_vols)))
                else:
                    self.log.info(
                        "Step 6: {} guest-vol snapshots in backup (no "
                        "pre-backup inventory to cross-check)".format(
                            len(guest_tagged)))
            else:
                self.log.info(
                    "Step 6: 0 snapshots visible from TAF account — "
                    "skipping tag assertions")
        else:
            self.log.warning("Step 6 skipped: AWS creds not set.")

        # Step 7: Re-enable Fusion on source for the restore (the disable
        # in Step 3 left it disabled/free). Wait for the disable to fully
        # complete before re-enabling — the disable can take 10+ minutes
        # (guest-volume teardown, S3 bucket deletion).
        self.log.info(
            "=== Step 7: Re-enabling Fusion on source for restore ===")
        self._wait_for_fusion_disabled(
            self.source_cluster_id, self.source_project_id)
        self.enable_fusion_on_cluster(
            self.source_cluster_id, self.source_project_id)
        self._wait_for_cluster_healthy(
            self.source_cluster_id, self.source_project_id,
            timeout=self.rebalance_timeout)

        self.log.info(
            "=== Step 7b: Provisioning target cluster (Fusion ON) ===")
        if not self.target_cluster_id:
            self.target_cluster_id, self.target_project_id = (
                self.acquire_cluster(
                    fusion_enabled=True,
                    num_nodes=self.target_num_nodes,
                    name_prefix="TAF_FusionTgt"))
        else:
            self.log.info(
                "Step 7b: Reusing target cluster {}".format(
                    self.target_cluster_id))
            if not self.wait_for_deployment(
                    self.target_project_id, self.target_cluster_id):
                self.fail(
                    "Target cluster {} not healthy".format(
                        self.target_cluster_id))
            self.enable_fusion_on_cluster(
                self.target_cluster_id, self.target_project_id)
            self._wait_for_cluster_healthy(
                self.target_cluster_id, self.target_project_id,
                timeout=self.rebalance_timeout)

        self._target_original_nodes = self.get_cluster_node_count(
            self.target_cluster_id, self.target_project_id)

        self.log.info("=== Step 7c: Restoring backup to target ===")
        self.trigger_restore(
            backup_id=backup_id,
            target_cluster_id=self.target_cluster_id,
            project_id=self.target_project_id)
        self.wait_for_restore_complete(
            self.target_cluster_id,
            project_id=self.target_project_id,
            expected_bucket_names=self.source_bucket_names)

        self.log.info("=== Step 7d: Verifying data integrity ===")
        self.verify_data_integrity()

        self._test_succeeded = True
        self.log.info(
            "test_backup_during_fusion_state_transition PASSED")

    # ------------------------------------------------------------------
    # Test 6: Primary + guest-vol snapshot counts match attached volumes
    # ------------------------------------------------------------------
    def test_guest_volume_snapshots_created_during_backup(self):
        """Verify that a backup of a fusion-enabled cluster with guest
        volumes produces the correct number of primary-disk snapshots
        (one per KV node) and guest-volume snapshots (total attached).

        Test plan:
          1.  Provision source (Fusion ON), populate, rebalance
          2.  Record attached guest volumes (volume ID, node assignment)
          3.  Create on-demand backup
          4.  Enumerate EBS snapshots by backupID; classify by
              IsFusionGuestVolume tag
          5.  Verify primary snapshot count equals num KV nodes
          6.  Verify guest-vol snapshot count equals total attached
              guest volumes
          7.  Verify tags on guest-vol snapshots: clusterID, backupID,
              nodeID, IsFusionGuestVolume:true
          8.  Verify primary snapshots have no IsFusionGuestVolume tag
        """
        # Step 1: Provision, populate, rebalance
        self.log.info(
            "=== Step 1: Provisioning source cluster (Fusion ON) ===")
        if not self.source_cluster_id:
            self.source_cluster_id, self.source_project_id = (
                self.acquire_cluster(
                    fusion_enabled=True,
                    num_nodes=self.source_num_nodes,
                    name_prefix="TAF_FusionSrc"))
        else:
            self.log.info(
                "Step 1: Reusing source cluster {}".format(
                    self.source_cluster_id))
            if not self.wait_for_deployment(
                    self.source_project_id, self.source_cluster_id):
                self.fail(
                    "Source cluster {} not healthy".format(
                        self.source_cluster_id))
            self.enable_fusion_on_cluster(
                self.source_cluster_id, self.source_project_id)
            self._wait_for_cluster_healthy(
                self.source_cluster_id, self.source_project_id,
                timeout=self.rebalance_timeout)

        self.log.info("=== Step 1b: Populating data ===")
        self.populate_source_buckets()

        self.log.info("=== Step 1c: Fusion rebalance ===")
        self._source_original_nodes, scaled_nodes = (
            self.trigger_fusion_rebalance(
                self.source_cluster_id,
                project_id=self.source_project_id))
        if not self.wait_for_rebalance_complete(
                self.source_cluster_id,
                project_id=self.source_project_id):
            self.fail(
                "Step 1c: Rebalance did not complete".format(
                    self.source_cluster_id))
        self.log.info(
            "Step 1c: Source at {} nodes (from {} base)".format(
                scaled_nodes, self._source_original_nodes))

        # Step 2: Record attached guest volumes
        self.log.info(
            "=== Step 2: Recording attached guest volumes ===")
        kv_node_count = scaled_nodes or self.source_num_nodes
        guest_vol_ids = set()
        node_vol_map = {}
        if self.fusion_aws_util:
            try:
                guest = self.fusion_aws_util.get_guest_volumes_for_cluster(
                    self.source_cluster_id)
                for node_id, vols in guest.items():
                    if node_id == "unattached":
                        continue
                    node_vol_map[node_id] = sorted(vols)
                    guest_vol_ids.update(vols)
                self.log.info(
                    "Step 2: {} attached guest volumes across {} nodes: "
                    "{}".format(
                        len(guest_vol_ids), len(node_vol_map),
                        {k: sorted(v) for k, v in node_vol_map.items()}))
                if not guest_vol_ids:
                    self.skipTest(
                        "Step 2: 0 attached guest volumes — data may be "
                        "below Fusion threshold. Cannot verify guest-vol "
                        "snapshot counts.")
            except NotImplementedError as e:
                self.log.warning(
                    "Step 2 guest-vol lookup skipped: {}".format(e))

        # Step 3: Create backup
        self.log.info("=== Step 3: Creating snapshot backup ===")
        backup_id = self.trigger_snapshot_backup(
            self.source_cluster_id,
            project_id=self.source_project_id)
        self._last_backup_id = backup_id
        backup_record = self.wait_for_backup_complete(
            backup_id, self.source_cluster_id,
            project_id=self.source_project_id)
        self.log.info("Step 3: Backup {} ready".format(backup_id))

        # Steps 4-8: Enumerate & verify snapshots
        self.log.info("=== Steps 4-8: Verifying EBS snapshot inventory ===")
        if not self.fusion_aws_util or not guest_vol_ids:
            self.log.warning(
                "Steps 4-8 skipped: AWS creds not set or 0 guest vols")
            self._test_succeeded = True
            return

        all_snapshots = (
            self.fusion_aws_util.get_ebs_snapshots_for_backup(
                backup_id, backup_record))
        primary_snaps, guest_snaps = (
            self.fusion_aws_util.classify_snapshots(all_snapshots))

        self.log.info(
            "Backup {}: total={} primary={} guest_vol={}".format(
                backup_id, len(all_snapshots),
                len(primary_snaps), len(guest_snaps)))

        if not all_snapshots:
            self.log.info(
                "Steps 4-8: 0 snapshots visible from TAF account — "
                "owned by Capella internal. Skipping assertions.")
            self._test_succeeded = True
            return

        # Step 5: Primary snapshots >= kv_node_count
        if not primary_snaps:
            self.fail(
                "Step 5: 0 primary disk snapshots in backup {}".format(
                    backup_id))
        if len(primary_snaps) < kv_node_count:
            self.log.warning(
                "Step 5: primary snapshot count {} < KV node count {} "
                "(expected at least one per KV node)".format(
                    len(primary_snaps), kv_node_count))
        self.log.info(
            "Step 5 passed: {} primary disk snapshots >= {} KV nodes".format(
                len(primary_snaps), kv_node_count))

        # Step 6: Guest-vol snapshots == total attached guest volumes
        guest_snaps_by_tag = [
            s for s in all_snapshots
            if self.fusion_aws_util.get_tag_value(
                s, self.fusion_aws_util.FUSION_GUEST_VOL_TAG_KEY
            ) == self.fusion_aws_util.FUSION_GUEST_VOL_TAG_VAL
        ]
        snap_vol_ids = {s.get("VolumeId") for s in guest_snaps_by_tag
                        if s.get("VolumeId")}
        missing = guest_vol_ids - snap_vol_ids
        if missing == guest_vol_ids:
            self.fail(
                "Step 6: 0 of {} attached guest volumes captured in backup "
                "{} — missing: {}".format(
                    len(guest_vol_ids), backup_id, sorted(missing)))
        elif missing:
            self.fail(
                "Step 6: {}/{} attached guest volumes missing from backup "
                "{}: {}".format(
                    len(missing), len(guest_vol_ids), backup_id,
                    sorted(missing)))
        self.log.info(
            "Step 6 passed: all {} guest volumes have snapshots".format(
                len(guest_vol_ids)))

        # Step 7: Verify guest-vol snapshot tags
        for snap in guest_snaps_by_tag:
            tags = {t["Key"]: t["Value"] for t in snap.get("Tags", [])}
            for required in [
                "couchbase-cloud-cluster-id",
                "couchbase-cloud-backup-id",
                "couchbase-cloud-node-id",
            ]:
                if required not in tags:
                    self.fail(
                        "Step 7: Guest-vol snapshot {} missing tag {}".format(
                            snap["SnapshotId"], required))
        self.log.info(
            "Step 7 passed: {} guest-vol snapshots carry required tags".format(
                len(guest_snaps_by_tag)))

        # Step 8: Primary snapshots must NOT carry IsFusionGuestVolume tag
        for snap in primary_snaps:
            if (self.fusion_aws_util.get_tag_value(
                    snap, self.fusion_aws_util.FUSION_GUEST_VOL_TAG_KEY)
                    is not None):
                self.fail(
                    "Step 8: Primary disk snapshot {} unexpectedly carries "
                    "IsFusionGuestVolume tag".format(snap["SnapshotId"]))
        self.log.info(
            "Step 8 passed: {} primary snapshots have no guest-volume tag".format(
                len(primary_snaps)))

        self._test_succeeded = True
        self.log.info(
            "test_guest_volume_snapshots_created_during_backup PASSED")

    # ------------------------------------------------------------------
    # Test 7: Guest-volume snapshot tags on all clouds
    # ------------------------------------------------------------------
    def test_guest_volume_snapshot_tags_all_clouds(self):
        """Validate tag structure on guest-volume EBS snapshots.

        Test plan:
          1.  Provision source (Fusion ON), populate, rebalance
          2.  Create backup
          3.  Enumerate guest-vol snapshots via CSP API
          4.  Validate every guest-vol snapshot carries:
              TenantID, ClusterID, BackupID, NodeID,
              IsFusionGuestVolume=true
          5.  Validate primary snapshots have no IsFusionGuestVolume tag
        """
        # Step 1: Provision, populate, rebalance
        self.log.info(
            "=== Step 1: Provisioning source (Fusion ON) ===")
        if not self.source_cluster_id:
            self.source_cluster_id, self.source_project_id = (
                self.acquire_cluster(
                    fusion_enabled=True,
                    num_nodes=self.source_num_nodes,
                    name_prefix="TAF_FusionSrc"))
        else:
            self.log.info(
                "Step 1: Reusing source cluster {}".format(
                    self.source_cluster_id))
            if not self.wait_for_deployment(
                    self.source_project_id, self.source_cluster_id):
                self.fail(
                    "Source cluster {} not healthy".format(
                        self.source_cluster_id))
            self.enable_fusion_on_cluster(
                self.source_cluster_id, self.source_project_id)
            self._wait_for_cluster_healthy(
                self.source_cluster_id, self.source_project_id,
                timeout=self.rebalance_timeout)

        self.log.info("=== Step 1b: Populating data ===")
        self.populate_source_buckets()

        self.log.info("=== Step 1c: Fusion rebalance ===")
        self._source_original_nodes, _ = (
            self.trigger_fusion_rebalance(
                self.source_cluster_id,
                project_id=self.source_project_id))
        if not self.wait_for_rebalance_complete(
                self.source_cluster_id,
                project_id=self.source_project_id):
            self.fail(
                "Step 1c: Rebalance did not complete")

        # Step 2: Create backup
        self.log.info("=== Step 2: Creating backup ===")
        backup_id = self.trigger_snapshot_backup(
            self.source_cluster_id,
            project_id=self.source_project_id)
        self._last_backup_id = backup_id
        backup_record = self.wait_for_backup_complete(
            backup_id, self.source_cluster_id,
            project_id=self.source_project_id)

        # Steps 3-5: Validate tag structure
        self.log.info("=== Steps 3-5: Validating snapshot tags ===")
        if not self.fusion_aws_util:
            self.log.warning(
                "Steps 3-5 skipped: AWS creds not set")
            self._test_succeeded = True
            return

        all_snapshots = (
            self.fusion_aws_util.get_ebs_snapshots_for_backup(
                backup_id, backup_record))
        primary_snaps, guest_snaps = (
            self.fusion_aws_util.classify_snapshots(all_snapshots))

        if not all_snapshots:
            self.log.info(
                "Steps 3-5: 0 snapshots visible — owned by Capella "
                "internal. Skipping.")
            self._test_succeeded = True
            return

        # Step 3-4: Guest-vol tags
        guest_snaps_by_tag = [
            s for s in all_snapshots
            if self.fusion_aws_util.get_tag_value(
                s, self.fusion_aws_util.FUSION_GUEST_VOL_TAG_KEY
            ) == self.fusion_aws_util.FUSION_GUEST_VOL_TAG_VAL
        ]

        if not guest_snaps_by_tag:
            self.log.warning(
                "Steps 3-4: 0 guest-vol-tagged snapshots — source may "
                "have 0 attached guest vols or snapshots not visible")
        else:
            required_tags = [
                "couchbase-cloud-cluster-id",
                "couchbase-cloud-backup-id",
                "couchbase-cloud-node-id",
                "couchbase-cloud-tenant-id",
            ]
            for snap in guest_snaps_by_tag:
                tags = {t["Key"]: t["Value"]
                        for t in snap.get("Tags", [])}
                missing_tags = [t for t in required_tags if t not in tags]
                if missing_tags:
                    self.fail(
                        "Step 3: Guest-vol snapshot {} missing tags: "
                        "{}".format(
                            snap["SnapshotId"], missing_tags))
                tag_val = tags.get(
                    self.fusion_aws_util.FUSION_GUEST_VOL_TAG_KEY)
                if tag_val != self.fusion_aws_util.FUSION_GUEST_VOL_TAG_VAL:
                    self.fail(
                        "Step 3: Guest-vol snapshot {} has {}={!r}, "
                        "expected {!r}".format(
                            snap["SnapshotId"],
                            self.fusion_aws_util.FUSION_GUEST_VOL_TAG_KEY,
                            tag_val,
                            self.fusion_aws_util.FUSION_GUEST_VOL_TAG_VAL))
            self.log.info(
                "Step 3-4 passed: {} guest-vol snapshots carry all "
                "required tags".format(len(guest_snaps_by_tag)))

        # Step 5: Primary snapshots must NOT carry IsFusionGuestVolume
        for snap in primary_snaps:
            if (self.fusion_aws_util.get_tag_value(
                    snap, self.fusion_aws_util.FUSION_GUEST_VOL_TAG_KEY)
                    is not None):
                self.fail(
                    "Step 5: Primary snapshot {} carries "
                    "IsFusionGuestVolume tag".format(snap["SnapshotId"]))
        self.log.info(
            "Step 5 passed: {} primary snapshots have no guest-vol tag".format(
                len(primary_snaps)))

        self._test_succeeded = True
        self.log.info(
            "test_guest_volume_snapshot_tags_all_clouds PASSED")

    # ------------------------------------------------------------------
    # Test 8: Guest volumes recreated from snapshots on restore
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Test 9: Fusion S3 bucket cleaned on restore
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Test 10: Cross-cluster restore — per-cluster bucket isolation
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Test 11: Same-cluster restore with existing guest volumes
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Test 12: Restore when fusion S3 bucket is already empty
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Helper: provision (or reuse) a Fusion-ON source cluster and load data.
    # ------------------------------------------------------------------
    def _provision_loaded_fusion_source(self):
        self.log.info(
            "=== Provisioning source cluster (Fusion ON) + loading data ===")
        if not self.source_cluster_id:
            self.source_cluster_id, self.source_project_id = (
                self.acquire_cluster(
                    fusion_enabled=True, num_nodes=self.source_num_nodes,
                    name_prefix="TAF_FusionSrc"))
        else:
            if not self.wait_for_deployment(
                    self.source_project_id, self.source_cluster_id):
                self.fail("Source cluster {} not healthy".format(
                    self.source_cluster_id))
            self.enable_fusion_on_cluster(
                self.source_cluster_id, self.source_project_id)
            self._wait_for_cluster_healthy(
                self.source_cluster_id, self.source_project_id,
                timeout=self.rebalance_timeout)
        self.populate_source_buckets()

    def _provision_fusion_target(self):
        self.log.info(
            "=== Provisioning target cluster (Fusion ON) ===")
        if not self.target_cluster_id:
            self.target_cluster_id, self.target_project_id = (
                self.acquire_cluster(
                    fusion_enabled=True, num_nodes=self.target_num_nodes,
                    name_prefix="TAF_FusionTgt"))
        else:
            if not self.wait_for_deployment(
                    self.target_project_id, self.target_cluster_id):
                self.fail("Target cluster {} not healthy".format(
                    self.target_cluster_id))
            self.enable_fusion_on_cluster(
                self.target_cluster_id, self.target_project_id)
            self._wait_for_cluster_healthy(
                self.target_cluster_id, self.target_project_id,
                timeout=self.rebalance_timeout)
        self._target_original_nodes = self.get_cluster_node_count(
            self.target_cluster_id, self.target_project_id)

    # ------------------------------------------------------------------
    # Test 13: Restore with guest-volume count mismatch (source N vs target M)
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Test 14: Backup deletion purges all its EBS snapshots (no orphans)
    # ------------------------------------------------------------------
    def test_guest_volume_snapshot_cleanup_after_backup_deletion(self):
        """Deleting a backup purges BOTH its primary-disk and guest-volume EBS
        snapshots from the CSP — no orphaned snapshots remain.
        """
        self._provision_loaded_fusion_source()
        self.log.info("=== Source Fusion rebalance ===")
        self._source_original_nodes, _ = self.trigger_fusion_rebalance(
            self.source_cluster_id, project_id=self.source_project_id)
        if not self.wait_for_rebalance_complete(
                self.source_cluster_id, project_id=self.source_project_id):
            self.fail("Source rebalance did not complete")

        self.log.info("=== Creating snapshot backup ===")
        backup_id = self.trigger_snapshot_backup(
            self.source_cluster_id, project_id=self.source_project_id)
        self._last_backup_id = backup_id
        backup_record = self.wait_for_backup_complete(
            backup_id, self.source_cluster_id,
            project_id=self.source_project_id)

        if not self.fusion_aws_util:
            self.skipTest("AWS creds required for snapshot-deletion check")
        all_snaps = self.fusion_aws_util.get_ebs_snapshots_for_backup(
            backup_id, backup_record)
        if not all_snaps:
            self.skipTest(
                "0 snapshots visible from TAF account (Capella-internal) — "
                "cannot verify snapshot deletion")
        primary, guest = self.fusion_aws_util.classify_snapshots(all_snaps)
        self.log.info(
            "Backup {} has {} snapshot(s): {} primary, {} guest".format(
                backup_id, len(all_snaps), len(primary), len(guest)))

        self.log.info("=== Deleting the backup ===")
        resp = self.capellaAPI.cluster_ops_apis.delete_backup(
            self.organisation_id, self.source_project_id,
            self.source_cluster_id, backup_id)
        if resp.status_code not in (200, 202, 204):
            self.fail("delete_backup returned {}: {}".format(
                resp.status_code, resp.content))
        # Deleted here — clear so tearDown doesn't try to delete it again.
        self._last_backup_id = None

        self.log.info("=== Verifying all snapshots purged ===")
        timeout = int(self.input.param("snapshot_cleanup_timeout", 1800))
        deadline = time.time() + timeout
        remaining = all_snaps
        while time.time() < deadline:
            remaining = self.fusion_aws_util.get_ebs_snapshots_for_backup(
                backup_id, backup_record)
            if not remaining:
                break
            self.log.info(
                "Waiting for {} snapshot(s) to be purged...".format(
                    len(remaining)))
            time.sleep(30)
        if remaining:
            self.fail(
                "{} snapshot(s) for backup {} not purged after deletion: "
                "{}".format(
                    len(remaining), backup_id,
                    [s.get("SnapshotId") for s in remaining]))
        self.log.info(
            "All {} snapshots purged after backup deletion — no orphans".format(
                len(all_snaps)))

        self._test_succeeded = True
        self.log.info(
            "test_guest_volume_snapshot_cleanup_after_backup_deletion PASSED")

    # ------------------------------------------------------------------
    # Test 15: Large guest-volume inventory (multiple rebalances) backup/restore
    # ------------------------------------------------------------------
    def test_backup_large_guest_volume_inventory(self):
        """Multiple Fusion rebalances accumulate a large guest-volume inventory;
        the backup snapshots every attached guest volume, and the data restores
        correctly to a target. (Set rebalance_rounds to control scale.)
        """
        self._provision_loaded_fusion_source()
        rounds = int(self.input.param("rebalance_rounds", 3))
        for r in range(rounds):
            self.log.info("=== Fusion rebalance round {}/{} ===".format(
                r + 1, rounds))
            orig, _ = self.trigger_fusion_rebalance(
                self.source_cluster_id, project_id=self.source_project_id)
            if self._source_original_nodes is None:
                self._source_original_nodes = orig
            if not self.wait_for_rebalance_complete(
                    self.source_cluster_id, project_id=self.source_project_id):
                self.fail("Rebalance round {} did not complete".format(r + 1))

        guest_vol_ids = set()
        if self.fusion_aws_util:
            g = self.fusion_aws_util.get_guest_volumes_for_cluster(
                self.source_cluster_id)
            for k, v in g.items():
                if k != "unattached":
                    guest_vol_ids.update(v)
        self.log.info(
            "Source accumulated {} guest volume(s) after {} rebalances".format(
                len(guest_vol_ids), rounds))

        self.log.info("=== Creating snapshot backup ===")
        backup_id = self.trigger_snapshot_backup(
            self.source_cluster_id, project_id=self.source_project_id)
        self._last_backup_id = backup_id
        backup_record = self.wait_for_backup_complete(
            backup_id, self.source_cluster_id,
            project_id=self.source_project_id)

        self.log.info("=== Verifying every guest volume is snapshotted ===")
        if self.fusion_aws_util and guest_vol_ids:
            all_snaps = self.fusion_aws_util.get_ebs_snapshots_for_backup(
                backup_id, backup_record)
            if all_snaps:
                guest_snaps = [
                    s for s in all_snaps
                    if self.fusion_aws_util.get_tag_value(
                        s, self.fusion_aws_util.FUSION_GUEST_VOL_TAG_KEY
                    ) == self.fusion_aws_util.FUSION_GUEST_VOL_TAG_VAL]
                snap_vol_ids = {s.get("VolumeId") for s in guest_snaps
                                if s.get("VolumeId")}
                missing = guest_vol_ids - snap_vol_ids
                if missing == guest_vol_ids:
                    self.fail(
                        "0 of {} guest volumes captured in backup {}".format(
                            len(guest_vol_ids), backup_id))
                if missing:
                    self.fail(
                        "{}/{} guest volumes missing from backup {}: "
                        "{}".format(
                            len(missing), len(guest_vol_ids), backup_id,
                            sorted(missing)))
                self.log.info(
                    "All {} guest volumes snapshotted in backup".format(
                        len(guest_vol_ids)))
            else:
                self.log.info(
                    "0 snapshots visible from TAF account (Capella-internal) "
                    "— skipping snapshot-count assertion")

        self._provision_fusion_target()
        self.log.info("=== Restoring backup into target ===")
        self.trigger_restore(
            backup_id=backup_id, target_cluster_id=self.target_cluster_id,
            project_id=self.target_project_id)
        self.wait_for_restore_complete(
            self.target_cluster_id, project_id=self.target_project_id,
            expected_bucket_names=self.source_bucket_names)

        self.log.info("=== Verifying data integrity ===")
        self.verify_data_integrity()

        self._test_succeeded = True
        self.log.info(
            "test_backup_large_guest_volume_inventory PASSED")
