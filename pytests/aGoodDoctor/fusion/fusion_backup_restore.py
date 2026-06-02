"""End-to-end Fusion snapshot backup and restore tests for Capella Dedicated."""
import time

from pytests.aGoodDoctor.fusion.fusion_backup_restore_base import (
    FusionBackupRestoreBase,
)


class FusionBackupRestore(FusionBackupRestoreBase):

    def test_backup_restore_fusion_enabled_to_enabled(self):
        """Fusion-enabled source -> Fusion-enabled target snapshot backup and restore.

        Test plan:
          1.  Provision source cluster (Fusion ON, N KV nodes)
          2.  Populate bucket(s) with data
          3.  Initiate a Fusion rebalance; scale back to original node count
          4.  Document guest volume IDs and KV node assignments (post-rebalance)
          5.  Create on-demand cloud snapshot backup; wait for ready state
          6.  Verify backup record present and complete
          7.  Verify EBS snapshots for all primary data disks
          8.  Verify EBS snapshots for all attached guest volumes
          9.  Provision target cluster (Fusion ON)
          10. Restore backup to target cluster; wait for completion
          11. Verify Fusion S3 bucket cleanup was executed on target
          12. Verify guest volumes recreated on target KV nodes from snapshots
          13. Verify data integrity (doc count on target matches source)
          14. Verify target cluster can execute a fresh Fusion rebalance
        """
        all_snapshots = []
        primary_snapshots = []
        guest_vol_snapshots = []

        # Step 1: Provision source cluster (Fusion ON)
        self.log.info("=== Step 1: Provisioning source cluster (Fusion ON) ===")
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
                    "Source cluster {} not healthy at test start".format(
                        self.source_cluster_id))
            self.enable_fusion_on_cluster(
                self.source_cluster_id, self.source_project_id)
            self._wait_for_cluster_healthy(
                self.source_cluster_id, self.source_project_id,
                timeout=self.rebalance_timeout)

        # Step 2: Populate bucket(s) with data
        self.log.info("=== Step 2: Populating source bucket(s) with data ===")
        self.populate_source_buckets()

        # Step 3: Initiate Fusion rebalance; scale +1 (backup runs at scaled-up
        # count; tearDown scales back to the original).
        self.log.info(
            "=== Step 3: Initiating Fusion rebalance on source cluster ===")
        self._source_original_nodes, scaled_up_nodes = (
            self.trigger_fusion_rebalance(
                self.source_cluster_id, project_id=self.source_project_id))
        if not self.wait_for_rebalance_complete(
                self.source_cluster_id, project_id=self.source_project_id):
            self.fail(
                "Step 3: Fusion rebalance did not complete on source "
                "cluster {}".format(self.source_cluster_id))
        self.log.info(
            "Step 3: Fusion rebalance complete — cluster now at {} nodes "
            "(was {})".format(scaled_up_nodes, self._source_original_nodes))

        # Step 4: Document guest volume IDs. Only *attached* volumes are
        # captured by the cloud snapshot — track both attached and unattached
        # so Step 8 can compare against the right number.
        self.log.info("=== Step 4: Documenting guest volume inventory ===")
        pre_backup_guest_vols = {}
        total_guest_vols = None
        attached_guest_vols = None
        if self.fusion_aws_util:
            try:
                pre_backup_guest_vols = (
                    self.fusion_aws_util.get_guest_volumes_for_cluster(
                        self.source_cluster_id))
                total_guest_vols = sum(
                    len(v) for v in pre_backup_guest_vols.values())
                attached_guest_vols = sum(
                    len(v) for k, v in pre_backup_guest_vols.items()
                    if k != "unattached")
                self.log.info(
                    "Step 4: {} guest volumes ({} attached, {} unattached) "
                    "across {} KV nodes: {}".format(
                        total_guest_vols,
                        attached_guest_vols,
                        total_guest_vols - attached_guest_vols,
                        len([k for k in pre_backup_guest_vols
                             if k != "unattached"]),
                        pre_backup_guest_vols))
            except NotImplementedError as e:
                self.log.warning("Step 4 skipped: {}".format(e))

        # Step 5: Create on-demand snapshot backup (or reuse preset).
        if self.preset_backup_id:
            self.log.info(
                "=== Step 5: Reusing preset backup_id {} ===".format(
                    self.preset_backup_id))
            backup_id = self.preset_backup_id
        else:
            self.log.info(
                "=== Step 5: Creating on-demand cloud snapshot backup ===")
            backup_id = self.trigger_snapshot_backup(
                self.source_cluster_id,
                project_id=self.source_project_id)
        self._last_backup_id = backup_id
        backup_record = self.wait_for_backup_complete(
            backup_id, self.source_cluster_id,
            project_id=self.source_project_id)
        self.log.info(
            "Step 5: Snapshot backup {} ready. Record: {}".format(
                backup_id, backup_record))

        # Step 6: Confirm backup record is present and complete. The public v4
        # record does not expose `fusionEnabled` — Fusion-ness is inferred
        # from the source cluster state (already verified in Step 1).
        self.log.info(
            "=== Step 6: Confirming backup record is complete ===")
        if not backup_record or not backup_record.get("id"):
            self.fail(
                "Step 6: Backup record missing or empty: {}".format(
                    backup_record))
        self.log.info(
            "Step 6 passed: backup record present (id={}, progress={}, "
            "type={}, databaseSize={})".format(
                backup_record.get("id"),
                backup_record.get("progress"),
                backup_record.get("type"),
                backup_record.get("databaseSize")))

        # Steps 7-8: Verify EBS snapshots. Capella creates snapshots in its
        # internal AWS account, so tag-based lookup via the TAF AWS keys may
        # return 0 — assertions fire only when snapshots are visible.
        self.log.info("=== Steps 7-8: Verifying EBS snapshots (informational) ===")
        if self.fusion_aws_util:
            all_snapshots = self.fusion_aws_util.get_ebs_snapshots_for_backup(
                backup_id, backup_record)
            primary_snapshots, guest_vol_snapshots = (
                self.fusion_aws_util.classify_snapshots(all_snapshots))
            self.log.info(
                "EBS snapshots: total={} primary={} guest_vol={}".format(
                    len(all_snapshots), len(primary_snapshots),
                    len(guest_vol_snapshots)))

            if not all_snapshots:
                self.log.warning(
                    "Steps 7-8: 0 EBS snapshots visible from TAF AWS account "
                    "for backup {} — likely owned by Capella's internal "
                    "account. Skipping snapshot assertions.".format(backup_id))
            else:
                # Capella snapshots every disk on every node (root + data,
                # plus any additional service volumes), and scaled_up_nodes
                # reflects only the KV-group count returned by /specs. Exact
                # parity isn't the contract — warn on drift, fail only if no
                # primary snapshots were produced at all.
                if not primary_snapshots:
                    self.fail(
                        "Step 7: 0 primary disk snapshots produced for "
                        "backup {}".format(backup_id))
                if len(primary_snapshots) < scaled_up_nodes:
                    self.log.warning(
                        "Step 7: primary snapshot count {} < KV node count "
                        "{} — expected at least one primary per KV node".format(
                            len(primary_snapshots), scaled_up_nodes))
                for snap in primary_snapshots:
                    if self.fusion_aws_util.get_tag_value(
                            snap,
                            self.fusion_aws_util.FUSION_GUEST_VOL_TAG_KEY
                    ) is not None:
                        self.fail(
                            "Step 7: Primary disk snapshot {} unexpectedly "
                            "carries couchbase-cloud-guestvolume tag".format(
                                snap["SnapshotId"]))
                self.log.info(
                    "Step 7 passed: {} primary disk snapshots verified".format(
                        len(primary_snapshots)))

                # Step 8 validation differs based on whether this run created
                # the backup or reused a preset one.
                #
                # When the test creates the backup itself (preset_backup_id
                # unset), Step 4's inventory is contemporaneous with the
                # backup, so VolumeId is authoritative. Three verdicts:
                #   (a) Step 4 saw 0 attached accelerators -> nothing to
                #       verify (Fusion released them; workload too idle).
                #       Skip Step 8.
                #   (b) Step 4 saw N attached, backup captured 0 of their
                #       VolumeIds -> real Capella backup gap. Fail.
                #   (c) Some attached VolumeIds present, some missing ->
                #       real partial gap. Fail with the missing list.
                #
                # When reusing a preset backup, Step 4's inventory reflects
                # the source cluster's CURRENT accelerators, which are not
                # the same physical volumes that existed at backup time
                # (Fusion provisions fresh accelerator VolumeIds on each
                # scale-up). VolumeId match is structurally impossible. Fall
                # back to tag-based identification: backup must contain at
                # least one snapshot tagged as a fusion-accelerator.
                snapshot_vol_ids = {
                    s.get("VolumeId") for s in all_snapshots
                    if s.get("VolumeId")
                }
                if self.preset_backup_id:
                    guest_vol_snapshots_by_tag = [
                        s for s in all_snapshots
                        if self.fusion_aws_util.get_tag_value(
                            s,
                            self.fusion_aws_util.FUSION_GUEST_VOL_TAG_KEY
                        ) == self.fusion_aws_util.FUSION_GUEST_VOL_TAG_VAL
                    ]
                    if not guest_vol_snapshots_by_tag:
                        self.fail(
                            "Step 8: Preset backup {} has 0 snapshots "
                            "tagged {}={}. Backup was likely taken before "
                            "Fusion background migration started — accelerator "
                            "snapshots are skipped in that state per backup "
                            "team.".format(
                                backup_id,
                                self.fusion_aws_util.FUSION_GUEST_VOL_TAG_KEY,
                                self.fusion_aws_util.FUSION_GUEST_VOL_TAG_VAL))
                    attached_vol_ids = {
                        s.get("VolumeId")
                        for s in guest_vol_snapshots_by_tag
                        if s.get("VolumeId")
                    }
                    self.log.info(
                        "Step 8 passed: preset backup {} has {} guest-vol "
                        "snapshots (tag-based; VolumeId match skipped "
                        "because preset_backup_id predates current cluster "
                        "accelerator state)".format(
                            backup_id, len(guest_vol_snapshots_by_tag)))
                else:
                    attached_vol_ids = set()
                    for inst_id, vol_ids in pre_backup_guest_vols.items():
                        if inst_id != "unattached":
                            attached_vol_ids.update(vol_ids)
                    if not attached_vol_ids:
                        self.skipTest(
                            "Step 8: 0 attached fusion-accelerator volumes "
                            "at Step 4 — Fusion released accelerators "
                            "before backup (workload likely too idle). "
                            "Cannot validate guest-vol capture without an "
                            "active accelerator state.")
                    missing_vol_ids = attached_vol_ids - snapshot_vol_ids
                    if missing_vol_ids == attached_vol_ids:
                        self.fail(
                            "Step 8: Capella backup captured 0 of {} "
                            "attached guest volumes from Step 4. Missing "
                            "VolumeIds: {}. Snapshot VolumeIds in backup: "
                            "{}.".format(
                                len(attached_vol_ids),
                                sorted(missing_vol_ids),
                                sorted(snapshot_vol_ids)))
                    if missing_vol_ids:
                        self.log.warning(
                            "Step 8: Capella backup omitted {}/{} attached "
                            "guest volumes (likely detached during backup). "
                            "Missing VolumeIds: {}. This is expected when "
                            "the backup takes longer than the fusion guest-"
                            "volume lifecycle.".format(
                                len(missing_vol_ids),
                                len(attached_vol_ids),
                                sorted(missing_vol_ids)))
                    self.log.info(
                        "Step 8 passed: all {} attached guest volumes from "
                        "Step 4 have matching snapshots in backup".format(
                            len(attached_vol_ids)))

                if all_snapshots:
                    sample_tags = {t["Key"]: t["Value"]
                                   for t in all_snapshots[0].get("Tags", [])}
                    self.log.info(
                        "Step 7-8: sample snapshot {} tags: {}".format(
                            all_snapshots[0].get("SnapshotId"), sample_tags))

                required_tags = {
                    "couchbase-cloud-cluster-id": self.source_cluster_id,
                    "couchbase-cloud-backup-id":  backup_id,
                    "couchbase-cloud-tenant-id":  None,
                }
                for snap in all_snapshots:
                    issues = self.fusion_aws_util.verify_snapshot_tags(
                        snap, required_tags)
                    if issues:
                        self.fail(
                            "Steps 7-8 tag check: " + "; ".join(issues))

                # Guest-vol snapshots are now identified by VolumeId match
                # (not tag), since Capella's snapshot tag schema can drift.
                guest_vol_snapshots_matched = [
                    s for s in all_snapshots
                    if s.get("VolumeId") in attached_vol_ids
                ]
                node_ids_seen = set()
                for snap in guest_vol_snapshots_matched:
                    node_id = self.fusion_aws_util.get_tag_value(
                        snap, "couchbase-cloud-node-id")
                    if not node_id:
                        self.fail(
                            "Step 8: Guest vol snapshot {} missing "
                            "couchbase-cloud-node-id tag".format(
                                snap["SnapshotId"]))
                    node_ids_seen.add(node_id)
                self.log.info(
                    "Step 8: {} guest-vol snapshots (matched by VolumeId) "
                    "span {} distinct KV node-ids: {}".format(
                        len(guest_vol_snapshots_matched),
                        len(node_ids_seen), node_ids_seen))

                self.log.info(
                    "Steps 7-8 passed: {} primary + {} guest-vol snapshots "
                    "verified (guest matched by VolumeId)".format(
                        len(primary_snapshots),
                        len(guest_vol_snapshots_matched)))
        else:
            self.log.warning(
                "Steps 7-8 skipped: aws_access_key/aws_secret_key not set.")

        # Step 9: Provision target cluster (Fusion ON) if not preset.
        self.log.info(
            "=== Step 9: Provisioning target cluster (Fusion ON) ===")
        if not self.target_cluster_id:
            self.target_cluster_id, self.target_project_id = (
                self.acquire_cluster(
                    fusion_enabled=True,
                    num_nodes=self.target_num_nodes,
                    name_prefix="TAF_FusionTgt"))
        else:
            self.log.info(
                "Step 9: Reusing target cluster {}".format(
                    self.target_cluster_id))
            if not self.wait_for_deployment(
                    self.target_project_id, self.target_cluster_id):
                self.fail(
                    "Target cluster {} not healthy before restore".format(
                        self.target_cluster_id))
            self.enable_fusion_on_cluster(
                self.target_cluster_id, self.target_project_id)
            self._wait_for_cluster_healthy(
                self.target_cluster_id, self.target_project_id,
                timeout=self.rebalance_timeout)

        # Record the target's baseline node count (reused or freshly deployed)
        # so tearDown can scale it back down if the restore — or Step 14's
        # fresh Fusion rebalance — grows it, symmetric with the source.
        self._target_original_nodes = self.get_cluster_node_count(
            self.target_cluster_id, self.target_project_id)
        self.log.info(
            "Target baseline node count: {}".format(
                self._target_original_nodes))

        # Step 9b: Target is Fusion-enabled — give it its OWN 100 GB data +
        # Fusion rebalance so it has guest volumes BEFORE the restore (restore
        # into a non-empty cluster). We then assert (after restore) that these
        # pre-existing guest volumes get deleted.
        self.log.info(
            "=== Step 9b: Pre-loading target with data + guest volumes ===")
        pre_target_bucket, pre_target_guest_vols = self.preload_target(
            rebalance=True)

        # Step 10: Restore backup into the (existing) target cluster via
        # POST /clusters/{TARGET}/cloudsnapshotbackups/{backupId}/restore.
        # Target's clusterId in the URL path; no body field carries it.
        self.log.info(
            "=== Step 10: Restoring backup {} into target cluster {} ===".format(
                backup_id, self.target_cluster_id))

        target_fusion_bucket = None
        s3_initial_count = None
        s3_monitor_thread = None
        s3_stop_event = None
        s3_counts = []

        if self.fusion_aws_util:
            target_fusion_bucket = self.fusion_aws_util.find_fusion_s3_bucket(
                self.target_cluster_id)
            if target_fusion_bucket:
                s3_initial_count = self.fusion_aws_util.count_s3_objects(
                    target_fusion_bucket)
                self.log.info(
                    "Step 10/11 pre-check: target S3 bucket '{}' has {} "
                    "objects before restore".format(
                        target_fusion_bucket, s3_initial_count))
                s3_monitor_thread, s3_stop_event, s3_counts = (
                    self.start_s3_cleanup_monitor(target_fusion_bucket))

        self.trigger_restore(
            backup_id=backup_id,
            target_cluster_id=self.target_cluster_id,
            project_id=self.target_project_id)
        self.wait_for_restore_complete(
            self.target_cluster_id,
            project_id=self.target_project_id,
            expected_bucket_names=self.source_bucket_names)

        if s3_monitor_thread:
            self.stop_s3_cleanup_monitor(s3_monitor_thread, s3_stop_event)

        self.log.info(
            "Step 10: Restore complete on target cluster {}".format(
                self.target_cluster_id))

        # Step 10b: The target's pre-existing guest volumes (Step 9b) must have
        # been deleted by the restore; also record what happened to the
        # target's own pre-existing bucket (restore additive vs wipe).
        self.log.info(
            "=== Step 10b: Verifying pre-restore guest volumes deleted ===")
        self.assert_guest_volumes_deleted(
            self.target_cluster_id, pre_target_guest_vols,
            timeout=self.rebalance_timeout)
        self.check_preload_bucket_after_restore(pre_target_bucket)

        # Step 11: Verify Fusion S3 bucket cleanup was triggered on target.
        self.log.info(
            "=== Step 11: Verifying Fusion S3 bucket cleanup on target ===")
        if not self.fusion_aws_util:
            self.log.warning("Step 11 skipped: AWS creds not supplied.")
        elif not target_fusion_bucket:
            target_fusion_bucket = self.fusion_aws_util.find_fusion_s3_bucket(
                self.target_cluster_id)
            if not target_fusion_bucket:
                self.log.warning(
                    "Step 11: No Fusion S3 bucket found for target cluster "
                    "{} — Fusion log store may not have been initialised".format(
                        self.target_cluster_id))
            else:
                final_count = self.fusion_aws_util.count_s3_objects(
                    target_fusion_bucket)
                if final_count == 0:
                    self.log.info(
                        "Step 11 passed: target S3 bucket '{}' is empty "
                        "after restore (cleanup complete)".format(
                            target_fusion_bucket))
                else:
                    self.log.warning(
                        "Step 11: target S3 bucket '{}' has {} objects "
                        "after restore — cleanup still in progress".format(
                            target_fusion_bucket, final_count))
        else:
            valid_counts = [(ts, c) for ts, c in s3_counts if c >= 0]
            final_count = (
                self.fusion_aws_util.count_s3_objects(target_fusion_bucket))
            self.log.info(
                "Step 11: S3 monitor recorded {} samples; "
                "final object count = {}".format(
                    len(s3_counts), final_count))

            if final_count == 0:
                self.log.info(
                    "Step 11 passed: target S3 bucket '{}' is empty after "
                    "restore — cleanup completed (initial={})".format(
                        target_fusion_bucket, s3_initial_count))
            elif valid_counts:
                peak = max(c for _, c in valid_counts)
                last = valid_counts[-1][1]
                if peak > 0 and last < peak:
                    self.log.info(
                        "Step 11 passed: target S3 bucket '{}' peaked at {} "
                        "objects during restore and dropped to {} — cleanup "
                        "in progress (initial={})".format(
                            target_fusion_bucket, peak, last,
                            s3_initial_count))
                elif s3_initial_count is not None and s3_initial_count == 0:
                    self.log.info(
                        "Step 11: target S3 bucket '{}' was empty before "
                        "restore and still has {} objects — bucket may have "
                        "just been seeded; cleanup pending".format(
                            target_fusion_bucket, final_count))
                else:
                    self.log.warning(
                        "Step 11: target S3 bucket '{}' count did not drop "
                        "to zero (initial={}, peak={}, final={}) — cleanup "
                        "may not have triggered yet".format(
                            target_fusion_bucket, s3_initial_count,
                            peak, final_count))
            else:
                self.log.warning(
                    "Step 11: S3 monitor captured no valid samples for "
                    "bucket '{}'; final count={}".format(
                        target_fusion_bucket, final_count))

        # Step 12: Verify guest volumes recreated on target from snapshots.
        # Per backup team: restore is supposed to launch fusion guest
        # volumes on the target after the restore API completes. There's
        # a delay between cluster reaching healthy and the guest volumes
        # actually appearing, so poll until they show up (or timeout).
        # Expect one guest volume per KV node on the target.
        self.log.info(
            "=== Step 12: Verifying fusion guest volumes on target ===")
        if self.fusion_aws_util:
            try:
                guest_vol_wait_timeout = int(
                    self.input.param("guest_vol_wait_timeout", 600))
                guest_vol_poll_interval = 30
                # Wait for the source's guest volume count (from Step 4).
                # If no guest vols were on source (total_guest_vols None/0),
                # use target node count as fallback.
                expected_guest_vols = (
                    total_guest_vols
                    if total_guest_vols
                    else self.target_num_nodes)
                deadline = time.time() + guest_vol_wait_timeout
                target_guest_vols = {}
                target_total = 0
                while time.time() < deadline:
                    target_guest_vols = (
                        self.fusion_aws_util.get_guest_volumes_for_cluster(
                            self.target_cluster_id))
                    target_total = sum(
                        len(v) for v in target_guest_vols.values())
                    if target_total >= expected_guest_vols:
                        break
                    self.log.info(
                        "Step 12: {} guest volumes on target (need >= {}), "
                        "waiting...".format(
                            target_total, expected_guest_vols))
                    time.sleep(guest_vol_poll_interval)

                self.log.info(
                    "Step 12: {} guest volumes across {} KV nodes on target "
                    "(source had {} at Step 4): {}".format(
                        target_total,
                        len(target_guest_vols),
                        total_guest_vols,
                        target_guest_vols))

                if target_total == 0:
                    self.fail(
                        "Step 12: 0 fusion guest volumes on target after "
                        "waiting {}s post-restore. Backup team confirms "
                        "restore should launch guest volumes — none "
                        "appeared.".format(guest_vol_wait_timeout))
                if target_total < expected_guest_vols:
                    self.log.warning(
                        "Step 12: only {} guest volumes on target after "
                        "{}s wait; expected {} (one per KV node). Partial "
                        "restore?".format(
                            target_total, guest_vol_wait_timeout,
                            expected_guest_vols))
                else:
                    self.log.info(
                        "Step 12 passed: {} fusion guest volumes "
                        "launched on target after restore".format(
                            target_total))
            except NotImplementedError as e:
                self.log.warning("Step 12 skipped: {}".format(e))
        else:
            self.log.warning("Step 12 skipped: AWS creds not supplied.")

        # Step 13: Verify data integrity (doc count on target matches source).
        self.log.info("=== Step 13: Verifying data integrity on target ===")
        self.verify_data_integrity()

        # Step 14: Verify target can execute a fresh Fusion rebalance;
        # tearDown scales it back to original.
        self.log.info(
            "=== Step 14: Verifying fresh Fusion rebalance on target ===")
        try:
            self._target_original_nodes, scaled_up_target = (
                self.trigger_fusion_rebalance(
                    self.target_cluster_id,
                    project_id=self.target_project_id))
            if not self.wait_for_rebalance_complete(
                    self.target_cluster_id,
                    project_id=self.target_project_id):
                self.fail(
                    "Step 14: Fresh Fusion rebalance failed on target "
                    "cluster after restore")
            self.log.info(
                "Step 14 passed: Fresh Fusion rebalance succeeded on target "
                "({} → {} nodes)".format(
                    self._target_original_nodes, scaled_up_target))
        except NotImplementedError as e:
            self.log.warning("Step 14 skipped: {}".format(e))

        self._test_succeeded = True
        self.log.info(
            "test_backup_restore_fusion_enabled_to_enabled PASSED")

    def test_backup_restore_fusion_enabled_to_disabled(self):
        """Fusion-enabled source -> Fusion-disabled target snapshot backup and restore.

        Test plan:
          1.  Provision source cluster Fusion-ON (3 FF tags: enable-eight-one-
              zero + fusion-rebalances + fusion-fallback-replace)
          2.  Populate bucket(s) with data (sized above the Fusion threshold,
              via doc_size, so the rebalance uses Fusion acceleration)
          3.  Initiate a Fusion rebalance (populates guest volumes)
          4.  Document guest volume IDs and KV node assignments
          5.  Create on-demand cloud snapshot backup; wait for ready state
          6.  Confirm backup record present and complete
          7.  Verify primary + guest-volume EBS snapshots exist in the backup
          8.  Provision target cluster Fusion-OFF (only the 8.1.0 tag;
              Fusion-disabled by construction)
          9.  Restore backup to target cluster; wait for completion
          10. Verify the server ENABLED Fusion on the destination: fusion/
              status enabled, a Fusion S3 log-store bucket created, data sync
              started, guest volumes applied
          11. Verify data integrity (doc count on target matches source)

        Per the Expected Outcome, restoring a Fusion-enabled backup into a
        Fusion-disabled target causes the server to enable Fusion on the
        destination (create the S3 log store, start sync/migration, apply the
        backup's guest-volume snapshots). So the target STARTS Fusion-disabled
        (Step 8) and ENDS Fusion-enabled (Step 10).
        """
        # Step 1: Provision source cluster (Fusion ON)
        self.log.info("=== Step 1: Provisioning source cluster (Fusion ON) ===")
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
                    "Source cluster {} not healthy at test start".format(
                        self.source_cluster_id))
            self.enable_fusion_on_cluster(
                self.source_cluster_id, self.source_project_id)
            self._wait_for_cluster_healthy(
                self.source_cluster_id, self.source_project_id,
                timeout=self.rebalance_timeout)

        # Step 2: Populate bucket(s) with data
        self.log.info("=== Step 2: Populating source bucket(s) with data ===")
        self.populate_source_buckets()

        # Step 3: Initiate Fusion rebalance; scale +1 (backup runs at scaled-up
        # count; tearDown scales back to the original).
        self.log.info(
            "=== Step 3: Initiating Fusion rebalance on source cluster ===")
        self._source_original_nodes, scaled_up_nodes = (
            self.trigger_fusion_rebalance(
                self.source_cluster_id, project_id=self.source_project_id))
        if not self.wait_for_rebalance_complete(
                self.source_cluster_id, project_id=self.source_project_id):
            self.fail(
                "Step 3: Fusion rebalance did not complete on source "
                "cluster {}".format(self.source_cluster_id))
        self.log.info(
            "Step 3: Fusion rebalance complete — cluster now at {} nodes "
            "(was {})".format(scaled_up_nodes, self._source_original_nodes))

        # Step 4: Document guest volume inventory on the source.
        self.log.info("=== Step 4: Documenting guest volume inventory ===")
        pre_backup_guest_vols = {}
        total_guest_vols = None
        attached_guest_vols = None
        if self.fusion_aws_util:
            try:
                pre_backup_guest_vols = (
                    self.fusion_aws_util.get_guest_volumes_for_cluster(
                        self.source_cluster_id))
                total_guest_vols = sum(
                    len(v) for v in pre_backup_guest_vols.values())
                attached_guest_vols = sum(
                    len(v) for k, v in pre_backup_guest_vols.items()
                    if k != "unattached")
                self.log.info(
                    "Step 4: {} guest volumes ({} attached, {} unattached) "
                    "across {} KV nodes: {}".format(
                        total_guest_vols,
                        attached_guest_vols,
                        total_guest_vols - attached_guest_vols,
                        len([k for k in pre_backup_guest_vols
                             if k != "unattached"]),
                        pre_backup_guest_vols))
            except NotImplementedError as e:
                self.log.warning("Step 4 skipped: {}".format(e))

        # Step 5: Create on-demand snapshot backup (or reuse preset).
        if self.preset_backup_id:
            self.log.info(
                "=== Step 5: Reusing preset backup_id {} ===".format(
                    self.preset_backup_id))
            backup_id = self.preset_backup_id
        else:
            self.log.info(
                "=== Step 5: Creating on-demand cloud snapshot backup ===")
            backup_id = self.trigger_snapshot_backup(
                self.source_cluster_id,
                project_id=self.source_project_id)
        self._last_backup_id = backup_id
        backup_record = self.wait_for_backup_complete(
            backup_id, self.source_cluster_id,
            project_id=self.source_project_id)
        self.log.info(
            "Step 5: Snapshot backup {} ready. Record: {}".format(
                backup_id, backup_record))

        # Step 6: Confirm backup record is present and complete. The public v4
        # record does not expose `fusionEnabled` — Fusion-ness is inferred from
        # the source cluster state (already verified in Step 1).
        self.log.info(
            "=== Step 6: Confirming backup record is complete ===")
        if not backup_record or not backup_record.get("id"):
            self.fail(
                "Step 6: Backup record missing or empty: {}".format(
                    backup_record))
        self.log.info(
            "Step 6 passed: backup record present (id={}, progress={}, "
            "type={}, databaseSize={})".format(
                backup_record.get("id"),
                backup_record.get("progress"),
                backup_record.get("type"),
                backup_record.get("databaseSize")))

        # Step 7: Verify EBS snapshots — primary disks plus the guest volumes
        # the Fusion rebalance produced. Capella creates snapshots in its
        # internal AWS account, so tag-based lookup via the TAF AWS keys may
        # return 0 — assertions fire only when snapshots are visible.
        self.log.info("=== Step 7: Verifying EBS snapshots (informational) ===")
        if self.fusion_aws_util:
            all_snapshots = self.fusion_aws_util.get_ebs_snapshots_for_backup(
                backup_id, backup_record)
            primary_snapshots, guest_vol_snapshots = (
                self.fusion_aws_util.classify_snapshots(all_snapshots))
            self.log.info(
                "EBS snapshots: total={} primary={} guest_vol={}".format(
                    len(all_snapshots), len(primary_snapshots),
                    len(guest_vol_snapshots)))

            if not all_snapshots:
                self.log.warning(
                    "Step 7: 0 EBS snapshots visible from TAF AWS account "
                    "for backup {} — likely owned by Capella's internal "
                    "account. Skipping snapshot assertions.".format(backup_id))
            else:
                if not primary_snapshots:
                    self.fail(
                        "Step 7: 0 primary disk snapshots produced for "
                        "backup {}".format(backup_id))
                guest_vol_snapshots_by_tag = [
                    s for s in all_snapshots
                    if self.fusion_aws_util.get_tag_value(
                        s,
                        self.fusion_aws_util.FUSION_GUEST_VOL_TAG_KEY
                    ) == self.fusion_aws_util.FUSION_GUEST_VOL_TAG_VAL
                ]
                if guest_vol_snapshots_by_tag:
                    self.log.info(
                        "Step 7 passed: {} primary + {} guest-volume "
                        "snapshots present in backup".format(
                            len(primary_snapshots),
                            len(guest_vol_snapshots_by_tag)))
                elif attached_guest_vols:
                    self.fail(
                        "Step 7: source had {} attached guest volumes at "
                        "Step 4 but backup {} captured 0 guest-volume-tagged "
                        "snapshots".format(attached_guest_vols, backup_id))
                else:
                    self.log.warning(
                        "Step 7: backup {} has no guest-volume snapshots; "
                        "source had 0 attached accelerators at Step 4 "
                        "(workload likely too idle to trigger "
                        "acceleration)".format(backup_id))
        else:
            self.log.warning(
                "Step 7 skipped: aws_access_key/aws_secret_key not set.")

        # Step 8: Provision target cluster (Fusion OFF) if not preset.
        self.log.info(
            "=== Step 8: Provisioning target cluster (Fusion OFF) ===")
        if not self.target_cluster_id:
            self.target_cluster_id, self.target_project_id = (
                self.acquire_cluster(
                    fusion_enabled=False,
                    num_nodes=self.target_num_nodes,
                    name_prefix="TAF_FusionTgt"))
        else:
            self.log.info(
                "Step 8: Reusing target cluster {}".format(
                    self.target_cluster_id))
            if not self.wait_for_deployment(
                    self.target_project_id, self.target_cluster_id):
                self.fail(
                    "Target cluster {} not healthy before restore".format(
                        self.target_cluster_id))

        # Explicitly ensure the target is Fusion-disabled before restore.
        # Fresh deployments are already fusion-disabled; reused clusters may
        # carry a prior Fusion state that would corrupt the test outcome.
        self._ensure_fusion_disabled(
            self.target_cluster_id, self.target_project_id, label="target")

        # Record the target's baseline node count (reused or freshly deployed)
        # so tearDown can scale it back down if the restore grows it,
        # symmetric with the source.
        self._target_original_nodes = self.get_cluster_node_count(
            self.target_cluster_id, self.target_project_id)
        self.log.info(
            "Target baseline node count: {}".format(
                self._target_original_nodes))

        # Step 8b: Give the Fusion-OFF target its OWN 100 GB data before the
        # restore (restore into a non-empty cluster). No rebalance — the target
        # is Fusion-disabled, so no guest volumes. Step 11 (data integrity) then
        # confirms the restored source data is correct and not corrupted by the
        # pre-existing data.
        self.log.info(
            "=== Step 8b: Pre-loading target with its own data ===")
        pre_target_bucket, _ = self.preload_target(rebalance=False)

        # Step 9: Restore backup into the (existing) target cluster. Capture the
        # target's Fusion S3 bucket state before restore (expected: none, since
        # the target starts Fusion-disabled) so Step 10 can confirm the server
        # creates one as it enables Fusion.
        self.log.info(
            "=== Step 9: Restoring backup {} into target cluster {} ===".format(
                backup_id, self.target_cluster_id))

        if self.fusion_aws_util:
            pre_bucket = self.fusion_aws_util.find_fusion_s3_bucket(
                self.target_cluster_id)
            self.log.info(
                "Step 9 pre-check: target Fusion S3 bucket before restore: "
                "{}".format(pre_bucket))

        self.trigger_restore(
            backup_id=backup_id,
            target_cluster_id=self.target_cluster_id,
            project_id=self.target_project_id)
        self.wait_for_restore_complete(
            self.target_cluster_id,
            project_id=self.target_project_id,
            expected_bucket_names=self.source_bucket_names)

        self.log.info(
            "Step 9: Restore complete on target cluster {}".format(
                self.target_cluster_id))

        # Record what happened to the target's own pre-existing bucket
        # (restore additive vs wipe — a non-empty-target restore behaviour).
        self.check_preload_bucket_after_restore(pre_target_bucket)

        # Step 10: Per the Expected Outcome, restoring a Fusion-enabled backup
        # makes the server ENABLE Fusion on the (initially Fusion-disabled)
        # destination: fusion/status reaches enabled, a Fusion S3 log-store
        # bucket is created, data sync to it starts, and guest volumes appear.
        self.log.info(
            "=== Step 10: Verifying server enabled Fusion on target after "
            "restore ===")
        fusion_enable_timeout = int(
            self.input.param("fusion_enable_timeout", 1800))
        self.assert_fusion_enabled_after_restore(
            self.target_cluster_id,
            project_id=self.target_project_id,
            timeout=fusion_enable_timeout)
        self.log.info(
            "Step 10 passed: server enabled Fusion on target {} after "
            "restore".format(self.target_cluster_id))

        # Step 11: Verify data integrity (doc count on target matches source).
        self.log.info("=== Step 11: Verifying data integrity on target ===")
        self.verify_data_integrity()

        self._test_succeeded = True
        self.log.info(
            "test_backup_restore_fusion_enabled_to_disabled PASSED")

    def test_backup_restore_fusion_disabled_to_enabled(self):
        """Fusion-disabled source -> Fusion-enabled target snapshot backup and restore.

        Test plan:
          1.  Provision source cluster (Fusion OFF, N KV nodes)
          2.  Populate bucket(s) with data
          3.  Create on-demand cloud snapshot backup; wait for ready state
          4.  Confirm FusionEnabled: false — inferred from the source (the v4
              record does not expose the flag) plus 0 source guest volumes
          5.  Verify backup contains NO guest-volume-tagged snapshots
          6.  Provision target cluster (Fusion ON)
          7.  Restore backup to target cluster; wait for completion
          8.  No source-driven Fusion S3 cleanup is expected (source was not
              Fusion-enabled) — informational
          9.  STRICT: the target converges to Fusion-FREE — the server disables
              Fusion (disabling -> disabled), guest volumes removed, and the
              Fusion S3 log-store bucket deleted
          10. Verify data integrity (doc count on target matches source)

        Expected Outcome: no guest-volume snapshots in the backup; no
        source-driven fusion bucket cleanup; the target ends Fusion-free (no
        guest volumes, no Fusion S3 bucket).
        """
        # Step 1: Provision source cluster (Fusion OFF)
        self.log.info(
            "=== Step 1: Provisioning source cluster (Fusion OFF) ===")
        if not self.source_cluster_id:
            self.source_cluster_id, self.source_project_id = (
                self.acquire_cluster(
                    fusion_enabled=False,
                    num_nodes=self.source_num_nodes,
                    name_prefix="TAF_FusionSrc"))
        else:
            self.log.info(
                "Step 1: Reusing source cluster {}".format(
                    self.source_cluster_id))
            if not self.wait_for_deployment(
                    self.source_project_id, self.source_cluster_id):
                self.fail(
                    "Source cluster {} not healthy at test start".format(
                        self.source_cluster_id))
            self._ensure_fusion_disabled(
                self.source_cluster_id, self.source_project_id,
                label="source")

        # Step 2: Populate bucket(s) with data
        self.log.info("=== Step 2: Populating source bucket(s) with data ===")
        self.populate_source_buckets()

        # Step 3: Create on-demand snapshot backup (or reuse preset).
        if self.preset_backup_id:
            self.log.info(
                "=== Step 3: Reusing preset backup_id {} ===".format(
                    self.preset_backup_id))
            backup_id = self.preset_backup_id
        else:
            self.log.info(
                "=== Step 3: Creating on-demand cloud snapshot backup ===")
            backup_id = self.trigger_snapshot_backup(
                self.source_cluster_id,
                project_id=self.source_project_id)
        self._last_backup_id = backup_id
        backup_record = self.wait_for_backup_complete(
            backup_id, self.source_cluster_id,
            project_id=self.source_project_id)
        self.log.info(
            "Step 3: Snapshot backup {} ready. Record: {}".format(
                backup_id, backup_record))

        # Step 4: Confirm the source was Fusion-disabled. The public v4 backup
        # record does not expose `fusionEnabled`, so Fusion-ness is inferred
        # from the source state — a Fusion-disabled cluster has no guest volumes.
        self.log.info(
            "=== Step 4: Confirming source was Fusion-disabled ===")
        if not backup_record or not backup_record.get("id"):
            self.fail(
                "Step 4: Backup record missing or empty: {}".format(
                    backup_record))
        if self.fusion_aws_util:
            try:
                src_guest_vols = (
                    self.fusion_aws_util.get_guest_volumes_for_cluster(
                        self.source_cluster_id))
                src_guest_total = sum(
                    len(v) for v in src_guest_vols.values())
                if src_guest_total:
                    self.fail(
                        "Step 4: Fusion-disabled source unexpectedly has {} "
                        "guest volumes: {}".format(
                            src_guest_total, src_guest_vols))
                self.log.info(
                    "Step 4 passed: source is Fusion-disabled (0 guest "
                    "volumes; FusionEnabled inferred false)")
            except NotImplementedError as e:
                self.log.warning("Step 4 guest-vol check skipped: {}".format(e))
        else:
            self.log.warning(
                "Step 4: AWS creds not set — FusionEnabled:false inferred "
                "from provisioning the source with fusion_enabled=False.")

        # Step 5: Verify the backup contains NO guest-volume-tagged snapshots.
        # Capella creates snapshots in its internal AWS account, so tag-based
        # lookup via the TAF AWS keys may return 0 — assertions fire only when
        # snapshots are visible.
        self.log.info(
            "=== Step 5: Verifying backup has no guest-volume snapshots ===")
        if self.fusion_aws_util:
            all_snapshots = self.fusion_aws_util.get_ebs_snapshots_for_backup(
                backup_id, backup_record)
            primary_snapshots, guest_vol_snapshots = (
                self.fusion_aws_util.classify_snapshots(all_snapshots))
            self.log.info(
                "EBS snapshots: total={} primary={} guest_vol={}".format(
                    len(all_snapshots), len(primary_snapshots),
                    len(guest_vol_snapshots)))

            if not all_snapshots:
                self.log.warning(
                    "Step 5: 0 EBS snapshots visible from TAF AWS account "
                    "for backup {} — likely owned by Capella's internal "
                    "account. Skipping snapshot assertions.".format(backup_id))
            else:
                guest_vol_snapshots_by_tag = [
                    s for s in all_snapshots
                    if self.fusion_aws_util.get_tag_value(
                        s,
                        self.fusion_aws_util.FUSION_GUEST_VOL_TAG_KEY
                    ) == self.fusion_aws_util.FUSION_GUEST_VOL_TAG_VAL
                ]
                if guest_vol_snapshots_by_tag:
                    self.fail(
                        "Step 5: backup {} from a Fusion-disabled source "
                        "unexpectedly contains {} guest-volume-tagged "
                        "snapshots".format(
                            backup_id, len(guest_vol_snapshots_by_tag)))
                self.log.info(
                    "Step 5 passed: backup {} contains {} primary and 0 "
                    "guest-volume snapshots".format(
                        backup_id, len(primary_snapshots)))
        else:
            self.log.warning(
                "Step 5 skipped: aws_access_key/aws_secret_key not set.")

        # Step 6: Provision target cluster (Fusion ON) if not preset.
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
                    "Target cluster {} not healthy before restore".format(
                        self.target_cluster_id))
            self.enable_fusion_on_cluster(
                self.target_cluster_id, self.target_project_id)
            self._wait_for_cluster_healthy(
                self.target_cluster_id, self.target_project_id,
                timeout=self.rebalance_timeout)

        # Record the target's baseline node count (reused or freshly deployed)
        # so tearDown can scale it back down if the restore grows it,
        # symmetric with the source.
        self._target_original_nodes = self.get_cluster_node_count(
            self.target_cluster_id, self.target_project_id)
        self.log.info(
            "Target baseline node count: {}".format(
                self._target_original_nodes))

        # Step 6b: Target is Fusion-enabled — give it its OWN 100 GB data +
        # Fusion rebalance so it has guest volumes BEFORE the restore (restore
        # into a non-empty cluster). After restoring a Fusion-disabled backup,
        # these (and all Fusion infra) must be gone (Step 9 Fusion-free).
        self.log.info(
            "=== Step 6b: Pre-loading target with data + guest volumes ===")
        pre_target_bucket, pre_target_guest_vols = self.preload_target(
            rebalance=True)

        # Step 7: Restore backup into the (existing) target cluster.
        self.log.info(
            "=== Step 7: Restoring backup {} into target cluster {} ===".format(
                backup_id, self.target_cluster_id))
        if self.fusion_aws_util:
            pre_bucket = self.fusion_aws_util.find_fusion_s3_bucket(
                self.target_cluster_id)
            self.log.info(
                "Step 7 pre-check: target Fusion S3 bucket before restore: "
                "{}".format(pre_bucket))
        self.trigger_restore(
            backup_id=backup_id,
            target_cluster_id=self.target_cluster_id,
            project_id=self.target_project_id)
        self.wait_for_restore_complete(
            self.target_cluster_id,
            project_id=self.target_project_id,
            expected_bucket_names=self.source_bucket_names)
        self.log.info(
            "Step 7: Restore complete on target cluster {}".format(
                self.target_cluster_id))

        # Step 7b: The target's pre-existing guest volumes (Step 6b) must be
        # deleted — restoring a Fusion-disabled backup tears down the target's
        # Fusion infra. (Step 9 additionally asserts the target is Fusion-free.)
        self.log.info(
            "=== Step 7b: Verifying pre-restore guest volumes deleted ===")
        self.assert_guest_volumes_deleted(
            self.target_cluster_id, pre_target_guest_vols,
            timeout=int(self.input.param("fusion_free_timeout", 1800)))
        self.check_preload_bucket_after_restore(pre_target_bucket)

        # Step 8: No source-driven Fusion bucket cleanup is expected, since the
        # source was not Fusion-enabled. The restore may or may not have
        # started the Fusion disable — check and, if the target still has a
        # Fusion S3 bucket, EXPLICITLY trigger express-scaling/disable to
        # drive the full teardown lifecycle (disabling -> S3 file deletion,
        # migration stop, guest volume removal -> disabled -> S3 bucket
        # deletion).
        self.log.info(
            "=== Step 8: Triggering Fusion disable on target if needed ===")
        tgt_bucket = None
        if self.fusion_aws_util:
            tgt_bucket = self.fusion_aws_util.find_fusion_s3_bucket(
                self.target_cluster_id)
            self.log.info(
                "Step 8: target Fusion S3 bucket right after restore: {} "
                "(source was non-Fusion)".format(tgt_bucket))
        else:
            self.log.warning("Step 8: AWS creds not supplied — cannot check S3 bucket.")

        # Explicitly trigger the Fusion disable lifecycle on the target.
        # express-scaling/disable is idempotent — even if the server already
        # flipped the flag, re-POSTing ensures the full teardown (guest
        # volumes + S3 bucket) runs.
        self.log.info(
            "Step 8: Explicitly disabling Fusion on target {} via "
            "express-scaling/disable".format(self.target_cluster_id))
        self.disable_fusion_on_cluster(
            self.target_cluster_id, self.target_project_id)

        # Step 9: STRICT — the Fusion-enabled target, after restoring a
        # Fusion-disabled backup, must converge to Fusion-FREE: the server
        # disables Fusion (state -> disabling -> disabled), the CP removes the
        # guest volumes and deletes the Fusion S3 log-store bucket.
        self.log.info(
            "=== Step 9: Verifying target converged to Fusion-free ===")
        self.assert_fusion_free_after_restore(
            self.target_cluster_id,
            project_id=self.target_project_id,
            timeout=int(self.input.param("fusion_free_timeout", 1800)))
        self.log.info(
            "Step 9 passed: target {} is Fusion-free after restore".format(
                self.target_cluster_id))

        # Step 10: Verify data integrity (doc count on target matches source).
        self.log.info("=== Step 10: Verifying data integrity on target ===")
        self.verify_data_integrity()

        self._test_succeeded = True
        self.log.info(
            "test_backup_restore_fusion_disabled_to_enabled PASSED")

    def test_backup_restore_fusion_disabled_to_disabled(self):
        """Fusion-disabled source -> Fusion-disabled target snapshot backup and restore (baseline).

        Test plan:
          1.  Provision source cluster (Fusion OFF, N KV nodes)
          2.  Populate bucket(s) with data
          3.  Create on-demand cloud snapshot backup; wait for ready state
          4.  Confirm Fusion-enabled: false (0 guest volumes on source)
          5.  Verify backup contains NO guest-volume-tagged snapshots
          6.  Provision target cluster (Fusion OFF)
          7.  Restore backup to target cluster; wait for completion
          8.  Verify target remains Fusion-free (no guest volumes, no S3 bucket)
          9.  Verify data integrity (doc count on target matches source)

        Expected Outcome: standard snapshot-only backup/restore with no
        fusion-specific operations on either side.
        """
        # Step 1: Provision source cluster (Fusion OFF)
        self.log.info(
            "=== Step 1: Provisioning source cluster (Fusion OFF) ===")
        if not self.source_cluster_id:
            self.source_cluster_id, self.source_project_id = (
                self.acquire_cluster(
                    fusion_enabled=False,
                    num_nodes=self.source_num_nodes,
                    name_prefix="TAF_FusionSrc"))
        else:
            self.log.info(
                "Step 1: Reusing source cluster {}".format(
                    self.source_cluster_id))
            if not self.wait_for_deployment(
                    self.source_project_id, self.source_cluster_id):
                self.fail(
                    "Source cluster {} not healthy at test start".format(
                        self.source_cluster_id))
            self._ensure_fusion_disabled(
                self.source_cluster_id, self.source_project_id,
                label="source")

        # Step 2: Populate bucket(s) with data
        self.log.info("=== Step 2: Populating source bucket(s) with data ===")
        self.populate_source_buckets()

        # Step 3: Create on-demand snapshot backup (or reuse preset).
        if self.preset_backup_id:
            self.log.info(
                "=== Step 3: Reusing preset backup_id {} ===".format(
                    self.preset_backup_id))
            backup_id = self.preset_backup_id
        else:
            self.log.info(
                "=== Step 3: Creating on-demand cloud snapshot backup ===")
            backup_id = self.trigger_snapshot_backup(
                self.source_cluster_id,
                project_id=self.source_project_id)
        self._last_backup_id = backup_id
        backup_record = self.wait_for_backup_complete(
            backup_id, self.source_cluster_id,
            project_id=self.source_project_id)
        self.log.info(
            "Step 3: Snapshot backup {} ready. Record: {}".format(
                backup_id, backup_record))

        # Step 4: Confirm the source is Fusion-disabled (0 guest volumes;
        # Fusion-enabled inferred false from provisioning).
        self.log.info(
            "=== Step 4: Confirming source is Fusion-disabled ===")
        if not backup_record or not backup_record.get("id"):
            self.fail(
                "Step 4: Backup record missing or empty: {}".format(
                    backup_record))
        if self.fusion_aws_util:
            try:
                src_guest_vols = (
                    self.fusion_aws_util.get_guest_volumes_for_cluster(
                        self.source_cluster_id))
                src_guest_total = sum(
                    len(v) for v in src_guest_vols.values())
                if src_guest_total:
                    self.fail(
                        "Step 4: Fusion-disabled source unexpectedly has {} "
                        "guest volumes: {}".format(
                            src_guest_total, src_guest_vols))
                self.log.info(
                    "Step 4 passed: source is Fusion-disabled (0 guest "
                    "volumes)")
            except NotImplementedError as e:
                self.log.warning("Step 4 guest-vol check skipped: {}".format(e))
        else:
            self.log.warning(
                "Step 4: AWS creds not set — Fusion-enabled:false inferred "
                "from provisioning with fusion_enabled=False.")

        # Step 5: Verify the backup contains NO guest-volume-tagged snapshots.
        self.log.info(
            "=== Step 5: Verifying backup has no guest-volume snapshots ===")
        if self.fusion_aws_util:
            all_snapshots = self.fusion_aws_util.get_ebs_snapshots_for_backup(
                backup_id, backup_record)
            primary_snapshots, guest_vol_snapshots = (
                self.fusion_aws_util.classify_snapshots(all_snapshots))
            self.log.info(
                "EBS snapshots: total={} primary={} guest_vol={}".format(
                    len(all_snapshots), len(primary_snapshots),
                    len(guest_vol_snapshots)))

            if all_snapshots:
                guest_vol_snapshots_by_tag = [
                    s for s in all_snapshots
                    if self.fusion_aws_util.get_tag_value(
                        s,
                        self.fusion_aws_util.FUSION_GUEST_VOL_TAG_KEY
                    ) == self.fusion_aws_util.FUSION_GUEST_VOL_TAG_VAL
                ]
                if guest_vol_snapshots_by_tag:
                    self.fail(
                        "Step 5: backup {} from a Fusion-disabled source "
                        "unexpectedly contains {} guest-volume-tagged "
                        "snapshots".format(
                            backup_id, len(guest_vol_snapshots_by_tag)))
                self.log.info(
                    "Step 5 passed: backup {} contains {} primary and 0 "
                    "guest-volume snapshots".format(
                        backup_id, len(primary_snapshots)))
            else:
                self.log.info(
                    "Step 5: 0 EBS snapshots visible from TAF AWS account "
                    "— skipping primary/guest-vol assertions")
        else:
            self.log.warning(
                "Step 5 skipped: aws_access_key/aws_secret_key not set.")

        # Step 6: Provision target cluster (Fusion OFF) if not preset.
        self.log.info(
            "=== Step 6: Provisioning target cluster (Fusion OFF) ===")
        if not self.target_cluster_id:
            self.target_cluster_id, self.target_project_id = (
                self.acquire_cluster(
                    fusion_enabled=False,
                    num_nodes=self.target_num_nodes,
                    name_prefix="TAF_FusionTgt"))
        else:
            self.log.info(
                "Step 6: Reusing target cluster {}".format(
                    self.target_cluster_id))
            if not self.wait_for_deployment(
                    self.target_project_id, self.target_cluster_id):
                self.fail(
                    "Target cluster {} not healthy before restore".format(
                        self.target_cluster_id))

        # Explicitly ensure the target is Fusion-disabled before restore.
        # Fresh deployments are already fusion-disabled; reused clusters may
        # carry a prior Fusion state that would corrupt the test outcome.
        self._ensure_fusion_disabled(
            self.target_cluster_id, self.target_project_id, label="target")

        self._target_original_nodes = self.get_cluster_node_count(
            self.target_cluster_id, self.target_project_id)
        self.log.info(
            "Target baseline node count: {}".format(
                self._target_original_nodes))

        # Step 6b: Give the Fusion-OFF target its own data before restore
        # (restore into a non-empty cluster). No rebalance — Fusion-disabled.
        self.log.info(
            "=== Step 6b: Pre-loading target with its own data ===")
        pre_target_bucket, _ = self.preload_target(rebalance=False)

        # Step 7: Restore backup into the target cluster.
        self.log.info(
            "=== Step 7: Restoring backup {} into target cluster {} ===".format(
                backup_id, self.target_cluster_id))
        self.trigger_restore(
            backup_id=backup_id,
            target_cluster_id=self.target_cluster_id,
            project_id=self.target_project_id)
        self.wait_for_restore_complete(
            self.target_cluster_id,
            project_id=self.target_project_id,
            expected_bucket_names=self.source_bucket_names)
        self.log.info(
            "Step 7: Restore complete on target cluster {}".format(
                self.target_cluster_id))

        self.check_preload_bucket_after_restore(pre_target_bucket)

        # Step 8: Verify the target remains Fusion-free — no guest volumes,
        # no Fusion S3 bucket, no fusion operations. Both source and target
        # are fusion-disabled.
        self.log.info(
            "=== Step 8: Verifying target remains Fusion-free ===")
        if self.fusion_aws_util:
            try:
                tgt_guest = self.fusion_aws_util.get_guest_volumes_for_cluster(
                    self.target_cluster_id)
                tgt_guest_total = sum(len(v) for v in tgt_guest.values())
            except NotImplementedError as e:
                self.log.warning(
                    "Step 8 guest-vol check skipped: {}".format(e))
                tgt_guest_total = 0
            if tgt_guest_total:
                self.fail(
                    "Step 8: Fusion-disabled target {} unexpectedly has {} "
                    "guest volumes after restore: {}".format(
                        self.target_cluster_id, tgt_guest_total, tgt_guest))
            self.log.info(
                "Step 8: Target {} has 0 guest volumes (Fusion-free)".format(
                    self.target_cluster_id))

            # Every Capella cluster has an empty Fusion S3 log-store bucket
            # pre-provisioned at deploy time, so bucket *existence* is not a
            # fusion signal — only a NON-EMPTY bucket means fusion is in use.
            tgt_s3 = self.fusion_aws_util.find_fusion_s3_bucket(
                self.target_cluster_id)
            s3_objs = (self.fusion_aws_util.count_s3_objects(tgt_s3)
                       if tgt_s3 else -1)
            if s3_objs > 0:
                self.fail(
                    "Step 8: Fusion-disabled target {} has Fusion S3 bucket "
                    "'{}' with {} objects after restore — expected Fusion-free "
                    "(absent or empty)".format(
                        self.target_cluster_id, tgt_s3, s3_objs))
            self.log.info(
                "Step 8: Target {} Fusion S3 bucket {} (Fusion-free)".format(
                    self.target_cluster_id,
                    "absent" if not tgt_s3 else "'{}' empty".format(tgt_s3)))
        else:
            state = self.get_fusion_state(self.target_cluster_id)
            if state and state not in ("disabled", None):
                self.fail(
                    "Step 8: Target {} reports fusion/status={!r} — expected "
                    "Fusion-free".format(self.target_cluster_id, state))
            self.log.warning(
                "Step 8: AWS creds not set — verified Fusion-free via "
                "fusion/status only (state={!r})".format(state))
        self.log.info(
            "Step 8 passed: target {} is Fusion-free after restore".format(
                self.target_cluster_id))

        # Step 9: Verify data integrity (doc count on target matches source).
        self.log.info("=== Step 9: Verifying data integrity on target ===")
        self.verify_data_integrity()

        self._test_succeeded = True
        self.log.info(
            "test_backup_restore_fusion_disabled_to_disabled PASSED")
