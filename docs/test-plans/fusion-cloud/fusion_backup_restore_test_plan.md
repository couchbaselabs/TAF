# Fusion Backup/Restore Test Plan

## Overview

This test plan covers backup and restore operations for fusion-enabled clusters on Capella Dedicated.
The implementation snapshots both main data disks and fusion guest volumes (EBS volumes that are
downloaded onto accelerator nodes and then re-attached to KV nodes), tracks fusion state in the backup
record, restores guest volumes from their snapshots during restore, and cleans the fusion S3 bucket to
ensure a consistent post-restore state.

---

## Terminology

| Term | Definition |
|------|-----------|
| **Guest volumes** | EBS volumes created during the acceleration phase of a fusion rebalance. They are first attached to accelerator nodes where shard data is downloaded onto them from S3. The accelerator nodes are then destroyed and these same EBS volumes are re-attached to KV nodes in the cluster. At rest (post-rebalance) they live on KV nodes. These ARE backed up and restored. |
| **Accelerator nodes** | Temporary spot instances spun up during a fusion rebalance. EBS guest volumes are attached to them, shard data from S3 is downloaded onto those volumes, then the accelerator nodes are destroyed and the volumes are detached and handed off to KV nodes. Ephemeral — **no role in backup or restore**. |
| **Fusion S3 bucket** | Object storage holding shard data. Accelerators read from this bucket when downloading onto guest volumes. Cleaned during restore because the restored guest volumes already contain shard data from backup time. |

---

## Implementation Summary

### What IS Backed Up
| Component | Backup Method | Restore Behavior |
|-----------|--------------|------------------|
| Main data disks | EBS Snapshot (per KV node) | Volume recreated from snapshot |
| **Fusion guest volumes** | **EBS Snapshot (`IsFusionGuestVolume` tag)** | **Volume recreated from snapshot — guest volumes restored to their state at backup time** |
| Cluster metadata | Backup record | Restored via `RestoreConfig` |
| `FusionEnabled` field | Backup record | Triggers fusion bucket cleanup during restore |
| Bucket configs | Backup record | Restored |
| On-off schedules | Backup record | Restored |

### What is NOT Backed Up / Needs Explicit Cleanup
| Component | Why | Restore Behavior |
|-----------|-----|-----------------|
| Accelerator nodes | Ephemeral spot instances, deleted after every rebalance | Not relevant to backup/restore |
| Fusion S3 bucket (shard data) | Object storage, not EBS | **Cleaned (deleted)** during restore — restored guest volumes already contain shard data from backup time; stale S3 data would be inconsistent |

### Key Design
- **Main disk snapshots**: Recreate KV node data volumes on restore.
- **Guest volume snapshots** (`IsFusionGuestVolume: true`): Recreate the EBS volumes on KV nodes at restore time. At backup time these volumes reside on KV nodes (post-acceleration phase). Restored guest volumes contain the shard data that was on them when the backup was taken.
- **Fusion S3 bucket**: Cleaned on restore so that the next rebalance starts from a consistent state relative to the restored guest volume data. Fresh accelerator instances spun up by any subsequent rebalance will re-download from a clean bucket state.
- **Accelerators**: Not backed up. Not restored. Not referenced. They are gone by the time backup is taken in a steady-state cluster. Fresh temporary instances are spun up by any subsequent rebalance after restore.

### Key Code Paths
- **Backup Record**: `internal/backup/provisioned/cluster/backup/record.go`
  - `FusionEnabled bool` — set from `cluster.Config.EnableFusion`
- **Recoverer**: `internal/backup/provisioned/cluster/recoverer/recoverer.go`
  - `cleanFusionBucket()` — deletes fusion S3 bucket contents on restore
- **Snapshot Creator**: `internal/backup/provisioned/cluster/infra/aws/snapshot_creator.go`
  - `IsFusionGuestVolume` tag applied to guest volume EBS snapshots
- **Snapshot Struct**: `internal/backup/provisioned/cluster/infra/snapshot.go`
  - `IsFusionGuestVolume bool` field distinguishes guest volume snapshots from main disk snapshots
- **TC Client**: `internal/backup/provisioned/cluster/client/tc_client.go`
  - Multi-cloud object store client for fusion bucket cleanup (AWS/GCP/Azure)

> **Note**: Guest volume snapshot restore is planned/in-progress. Some test cases below
> validate behavior that may not yet be fully implemented in the control plane.

---

## Test Scenarios

### 1. Basic Backup/Restore Matrix

#### 1.1 Fusion-Enabled → Fusion-Enabled
**Test ID**: `test_backup_restore_fusion_enabled_to_enabled`

**Steps**:
1. Create a fusion-enabled cluster (multi-node)
2. Load data into buckets
3. Trigger a fusion rebalance — accelerator spot instances spin up, EBS guest volumes are created and attached to them, shard data is downloaded from S3 onto those volumes, accelerators are destroyed, and the guest volumes are re-attached to KV nodes
4. Record current state: all guest volume IDs and their KV node assignments (post-acceleration)
5. Create on-demand snapshot backup
6. Verify backup record has `FusionEnabled: true`
7. Verify EBS snapshots exist for **all main data disks** (one per KV node)
8. Verify EBS snapshots exist for **all attached guest volumes** (tagged `IsFusionGuestVolume: true`)
9. Create target cluster with fusion enabled
10. Restore backup to target cluster
11. Verify fusion S3 bucket cleanup was triggered on target cluster
12. Verify guest volumes are recreated on target cluster from their snapshots (correct mount points, devices, KV node assignments)
13. Verify data integrity on main disks
14. Verify target cluster can perform a new fusion rebalance after restore (fresh accelerators will spin up and re-sync)

**Expected Result**:
- Both main disk and guest volume snapshots are created and tagged correctly
- Restore recreates guest volumes from snapshots on the target KV nodes
- Fusion bucket is cleaned; guest volumes and bucket are in a consistent state for the next rebalance

---

#### 1.2 Fusion-Enabled → Fusion-Disabled
**Test ID**: `test_backup_restore_fusion_enabled_to_disabled`

**Steps**:
1. Create fusion-enabled source cluster; trigger fusion rebalance (guest volumes populated)
2. Create snapshot backup; verify guest volume snapshots exist
3. Create target cluster with fusion **disabled**
4. Restore backup to target cluster
5. Verify fusion bucket cleanup is triggered (based on source `FusionEnabled: true`)
6. Verify guest volume snapshots from the backup are **not** applied (target has no guest volume infrastructure)
7. Verify data integrity from main disk snapshots

**Expected Result**:
- Main disk restore succeeds
- Guest volume snapshots exist in the backup but are not applied to the target
- Fusion bucket cleaned despite target being fusion-disabled

---

#### 1.3 Fusion-Disabled → Fusion-Enabled
**Test ID**: `test_backup_restore_fusion_disabled_to_enabled`

**Steps**:
1. Create fusion-disabled source cluster; load data; create snapshot backup
2. Verify `FusionEnabled: false` in backup record
3. Verify **no** `IsFusionGuestVolume`-tagged snapshots (no guest volumes existed on source)
4. Create target cluster with fusion **enabled**
5. Restore backup to target cluster
6. Verify **no** fusion bucket cleanup is triggered (source was not fusion-enabled)
7. Verify target cluster can perform fusion rebalances independently after restore

**Expected Result**:
- No guest volume snapshots in backup
- No fusion bucket cleanup
- Target cluster is free to use fusion independently post-restore

---

#### 1.4 Fusion-Disabled → Fusion-Disabled (Baseline)
**Test ID**: `test_backup_restore_fusion_disabled_to_disabled`

**Steps**:
1. Create fusion-disabled cluster; load data; create backup; restore to another fusion-disabled cluster
2. Verify standard snapshot-only flow; no fusion-specific operations

**Expected Result**: Standard backup/restore; no guest volume snapshots; no fusion operations.

---

### 2. Guest Volume EBS Snapshot Verification During Backup

#### 2.1 Verify Guest Volume Snapshots Are Created
**Test ID**: `test_guest_volume_snapshots_created_during_backup`

**Steps**:
1. Create fusion-enabled cluster with N KV nodes
2. Trigger fusion rebalance; record all attached guest volumes (volume ID, KV node, mount point)
3. Trigger on-demand backup
4. Via CSP API, enumerate all EBS snapshots created for this backupID
5. Classify by tag: `IsFusionGuestVolume` = true vs absent
6. Verify count: main disk snapshots == number of KV nodes
7. Verify count: guest volume snapshots == total attached guest volumes across all KV nodes
8. Verify each guest volume snapshot is tagged: `clusterID`, `backupID`, `nodeID`, `IsFusionGuestVolume: true`
9. Verify each main disk snapshot has **no** `IsFusionGuestVolume` tag

**Expected Result**:
- Total snapshots = (# KV nodes × 1 main disk) + (total attached guest volumes)
- Every attached guest volume has a corresponding EBS snapshot with correct tags

---

#### 2.2 Snapshot Inventory Matches Currently Attached Volumes
**Test ID**: `test_guest_volume_snapshots_match_attached_state`

**Steps**:
1. Create fusion-enabled cluster; perform multiple rebalances (guest volumes attached and detached across cycles)
2. Record currently attached guest volumes immediately before triggering backup
3. Trigger backup
4. Verify guest volume snapshots correspond to **currently attached** volumes only
5. Verify no snapshots for previously-detached volumes from prior rebalance cycles

**Expected Result**:
- Snapshot inventory exactly matches attached volume state at backup time
- No orphaned snapshots for volumes detached in earlier rebalance cycles

---

#### 2.3 No Guest Volume Snapshots When None Attached
**Test ID**: `test_no_guest_volume_snapshots_when_none_attached`

**Steps**:
1. Create fusion-enabled cluster (no rebalance triggered yet — no guest volumes attached)
2. Trigger backup
3. Verify no `IsFusionGuestVolume`-tagged snapshots exist in CSP

**Expected Result**: Backup succeeds; `FusionEnabled: true` in record; zero guest volume snapshots.

---

#### 2.4 Maximum Guest Volume Slots — All Snapshotted
**Test ID**: `test_guest_volume_snapshots_max_slots`

**Steps**:
1. Create fusion-enabled cluster; load data to trigger maximum shard distribution
2. Fill guest volume slots to near-maximum (up to 22 per host)
3. Trigger backup; verify ALL guest volumes across all KV nodes are snapshotted
4. Verify backup state is `triggered` (not `invalid`); backup skew < 10 seconds

**Expected Result**:
- All (up to 22/host) guest volume snapshots created with `IsFusionGuestVolume` tag
- Backup is complete and valid despite large snapshot count

---

#### 2.5 Guest Volume Snapshot Tags — Cross-Cloud Validation
**Test ID**: `test_guest_volume_snapshot_tags_all_clouds`

Run for each CSP (AWS, GCP, Azure):

**Steps**:
1. Create fusion-enabled cluster; trigger rebalance; create backup
2. Enumerate snapshots via CSP API; validate tag structure for guest volume snapshots:
   - AWS: EC2 snapshot tags include `IsFusionGuestVolume`, `TenantID`, `ClusterID`, `BackupID`, `NodeID`
   - GCP: Disk snapshot labels include equivalent keys
   - Azure: Snapshot tags include equivalent keys

**Expected Result**: Consistent, correctly-structured tags on all three CSPs.

---

### 3. Guest Volume Snapshot Restore Verification

#### 3.1 Guest Volumes Recreated From Snapshots on Restore
**Test ID**: `test_restore_recreates_guest_volumes_from_snapshots`

**Critical**: Core validation that guest volume snapshots are applied during restore.

**Steps**:
1. Create fusion-enabled cluster; trigger rebalance; note all guest volume IDs and KV node assignments
2. Create backup; record all guest volume snapshot IDs from the backup
3. Restore backup to target cluster
4. Via CSP API, verify volumes were created from the guest volume snapshot IDs
5. Verify restored guest volumes are attached to the correct KV nodes (matching node assignments from backup)
6. Verify mount points and device names are consistent with backup state
7. Verify fusion bucket was cleaned as part of restore

**Expected Result**:
- Every `IsFusionGuestVolume`-tagged snapshot from the backup is used to create a volume on the target
- KV node-to-guest-volume assignment matches the backed-up state
- Guest volumes contain the shard data from backup time; fusion bucket is clean for next rebalance

---

#### 3.2 Guest Volume Snapshot-to-Node Mapping Integrity
**Test ID**: `test_guest_volume_snapshot_node_mapping`

**Steps**:
1. Create multi-node fusion cluster; run rebalance so each KV node has a different number of guest volumes
2. Create backup; record per-node guest volume snapshot inventory
3. Restore to target cluster
4. Verify each restored guest volume is attached to the corresponding KV node (by nodeID tag)
5. Verify no cross-node assignment — KV node A's guest volume snapshots are not applied to KV node B

**Expected Result**: Per-node guest volume snapshot mapping is preserved exactly during restore.

---

#### 3.3 Restore Handles KV Nodes With Zero Guest Volumes
**Test ID**: `test_restore_nodes_with_no_guest_volumes`

**Steps**:
1. Create multi-node fusion cluster; trigger partial rebalance (only some KV nodes get guest volumes)
2. Create backup; some nodes have guest volume snapshots, some do not
3. Restore to target cluster
4. Verify KV nodes with guest volume snapshots get volumes restored
5. Verify KV nodes without guest volume snapshots in the backup also have no guest volumes post-restore

**Expected Result**: Per-node restore is correct regardless of whether each KV node had guest volumes.

---

#### 3.4 Fusion S3 Bucket Cleaned on Restore
**Test ID**: `test_fusion_bucket_cleaned_on_restore`

**Context**: Guest volumes are restored to their state at backup time. The fusion S3 bucket may contain
newer shard data written by subsequent rebalances after the backup. This stale S3 data must be cleaned
so the next rebalance starts from a state consistent with the restored guest volumes.

**Steps**:
1. Create fusion-enabled cluster; run rebalance; populate fusion S3 bucket; create backup
2. Run additional rebalances post-backup (bucket now has newer shard data than what's in the snapshots)
3. Restore backup to same or new cluster
4. Via Datadog logs, verify `cleanFusionBucket` is called during restore
5. Verify fusion S3 bucket is empty post-restore
6. Verify a new fusion rebalance can be triggered successfully after restore (fresh accelerators will re-sync cleanly)

**Expected Result**:
- Fusion S3 bucket is empty after restore
- Stale post-backup shard data is not present when the next rebalance begins
- Fresh accelerator instances spawned by any subsequent rebalance operate from a clean state

---

#### 3.5 Fusion S3 Bucket Delete Races With Active Fusion Sync
**Test ID**: `test_restore_bucket_cleanup_races_with_active_fusion_sync`

**Critical Race Condition**: Restore triggers `cleanFusionBucket()` (deleting all S3 objects) while
accelerators are still actively downloading shard data **from** that bucket onto guest volumes. The S3
read call on the accelerator side will fail because objects are being deleted under it. This failure
**must not crash** the fusion process or the control plane service.

**Steps**:
1. Create fusion-enabled cluster; trigger a long-running fusion rebalance with heavy data load
2. Confirm accelerators are actively reading shard data from the fusion S3 bucket and writing it onto guest volumes (verify via S3 object read activity or Datadog metrics)
3. While accelerator download is in progress, trigger a restore on the same cluster
4. `cleanFusionBucket()` fires and deletes objects from the bucket while accelerators are still reading from it
5. Observe accelerator processes receiving S3 errors (object not found, key deleted mid-read, etc.)
6. Verify the failure is **logged** and handled gracefully — no panic, no control plane crash, no unhandled exception
7. Verify the restore completes successfully (bucket cleanup finishes despite concurrent writes)
8. Verify the fusion S3 bucket is empty after restore completes
9. Verify the cluster is stable post-restore: no crashed processes, no stuck jobs

**Expected Result**:
- `cleanFusionBucket()` completes regardless of concurrent accelerator writes
- S3 write failures on the accelerator sync path are caught and logged, not propagated as fatal errors
- No crash, panic, or unhandled error in the sync path when S3 returns errors during bucket cleanup
- Control plane service remains healthy throughout

**Validation Points**:
- Datadog logs: sync error logged at `warn` or `error` level, not causing process exit
- No `FATAL`, panic traces, or OOMKill events in control plane logs during or after restore
- Restore job status transitions to `complete`, not stuck in `processing`
- S3 bucket object count = 0 after restore

**Risk**: If the accelerator sync path does not handle S3 errors defensively (e.g., treats any S3 error
as fatal), this race will crash the sync process. S3 errors during sync must be retriable or ignorable
in the context of a concurrent restore.

---

### 4. Backup During Fusion States

#### 4.1 Backup During Fusion Rebalance In Progress
**Test ID**: `test_backup_during_fusion_rebalance_in_progress`

**Context**: During an active rebalance, guest volumes may still be attached to accelerator nodes
(download in progress) or may have already been detached from accelerators and re-attached to KV nodes.
The backup should handle both sub-states.

**Steps**:
1. Create fusion-enabled cluster; load significant data
2. Start fusion rebalance; trigger backup at different sub-phases:
   - Phase 1 (early): guest volumes attached to accelerator nodes, download in progress
   - Phase 2 (late): accelerators destroyed, guest volumes re-attached to KV nodes
3. Verify backup captures snapshots of guest volumes at their current attachment point
4. Verify backup state is `triggered` (not `invalid`); skew < 10 seconds
5. Restore backup to target cluster; verify guest volumes are recreated in the state captured

**Expected Result**:
- Backup correctly snapshots guest volumes regardless of which phase of rebalance they are in
- Guest volumes are restored to the exact mid-rebalance state captured by the snapshot

---

#### 4.2 Backup While Accelerators Are Actively Downloading Onto Guest Volumes
**Test ID**: `test_backup_during_accelerator_active_write`

**Context**: During Phase 1 of rebalance, guest volumes are attached to accelerator nodes and shard
data is being downloaded from S3 onto them. The accelerators have not yet been destroyed and the
volumes have not yet been handed to KV nodes.

**Steps**:
1. Create fusion-enabled cluster; start fusion rebalance
2. Wait for accelerators to be active — guest volumes are attached to accelerator nodes and download from S3 is in progress
3. Trigger backup while accelerators are actively writing downloaded data onto guest volumes
4. Verify guest volume snapshots are created (volumes attached to accelerator nodes and in-use)
5. Restore to target cluster
6. Verify guest volumes are recreated in the mid-download state captured by the snapshot
7. Verify a new fusion rebalance can be triggered post-restore (fresh accelerators will re-download from clean S3 bucket)

**Expected Result**:
- Guest volume snapshots capture the in-progress download state (volumes still on accelerator nodes)
- Restore recreates those volumes; the captured partial-download state is a valid starting point for the next rebalance

---

#### 4.3 Backup During Guest Volume Attach/Detach Race
**Test ID**: `test_backup_during_guest_volume_attach_detach_race`

**Steps**:
1. Create fusion-enabled cluster; complete a rebalance
2. Trigger backup **while** control plane is detaching guest volumes from KV nodes (post-rebalance cleanup)
3. Verify backup handles transient volume state gracefully
4. Check for orphaned snapshots or errors in Datadog logs
5. Verify backup succeeds or fails with a clear error (no silent partial snapshot)

**Expected Result**:
- Volumes being detached during snapshot trigger are handled consistently
- If a guest volume disappears mid-backup: backup marked `StateInvalid` with clear error
- No orphaned EBS snapshots left in CSP

---

#### 4.4 Backup During Fusion State Transition (Enable/Disable)
**Test ID**: `test_backup_during_fusion_state_transition`

**Steps**:
1. Cluster has fusion enabled with guest volumes attached to KV nodes
2. Start backup; during backup trigger fusion disable
3. Verify `FusionEnabled` in the backup record reflects the state at record creation time
4. Verify guest volume snapshots are created if volumes were attached when snapshot was triggered

**Expected Result**:
- `FusionEnabled` is set at backup record creation, not at snapshot completion
- Consistent relationship between the flag and the actual snapshot inventory

---

### 5. Restore Scenarios

#### 5.1 Restore Aborts Pending CP-Jobs for Guest Volume Deletion
**Test ID**: `test_restore_aborts_pending_guest_volume_deletion_jobs`

**Critical Race Condition**: After a fusion rebalance completes — accelerators have downloaded data
onto guest volumes, been destroyed, and those guest volumes have been re-attached to KV nodes — the
control plane queues jobs to delete the guest volumes from KV nodes. If a restore is triggered
**before those deletion jobs execute**, the restore must abort the queued jobs. If they are not aborted,
the deletion jobs will run after restore and delete guest volumes that were just recreated from snapshots,
leaving the KV nodes without their guest volume data.

**Steps**:
1. Create a fusion-enabled cluster; load significant data
2. Trigger a fusion rebalance; allow it to complete (accelerators deleted, guest volumes on KV nodes)
3. Confirm the control plane has queued cp-jobs to delete the guest volumes (post-rebalance cleanup phase)
4. **Before those deletion jobs execute**, trigger a restore
5. Verify the restore detects and **aborts** the queued guest volume deletion jobs
6. Verify the aborted jobs do not execute after restore completes
7. Verify restore proceeds to recreate guest volumes from their snapshots on the KV nodes
8. Verify no guest volumes are deleted post-restore by any lingering or re-queued deletion job
9. Verify the cluster is fully operational; a new fusion rebalance can be triggered

**Expected Result**:
- Restore explicitly cancels/aborts all pending cp-jobs for guest volume deletion before restoring
- Aborted jobs do not re-execute or re-queue after restore completes
- Guest volumes are present and consistent on KV nodes after restore

**Validation Points**:
- Job queue: queued guest volume deletion jobs transition to `aborted`/`cancelled`, not `complete`
- Datadog logs: abort of deletion jobs logged before restore proceeds to volume recreation
- Post-restore: guest volumes exist on all KV nodes as expected from snapshot restore
- Post-restore: no subsequent job deletes those restored guest volumes

**Risk**: If restore does not clear the pending deletion job queue, the deletion jobs will run after
volume recreation from snapshots and silently remove the restored guest volumes from KV nodes.

---

#### 5.2 Same-Cluster Restore With Existing Guest Volumes
**Test ID**: `test_restore_same_cluster_with_existing_guest_volumes`

**Steps**:
1. Create fusion-enabled cluster; run rebalance; create backup
2. Run more rebalances post-backup (KV nodes now have different guest volumes)
3. Restore backup to **same cluster**
4. Verify existing (post-backup) guest volumes are detached/deleted from KV nodes during restore
5. Verify guest volumes from the backup snapshots are recreated on the KV nodes
6. Verify fusion bucket is cleaned
7. Verify data matches backup point; cluster is fully operational

**Expected Result**:
- Restore replaces current guest volumes on KV nodes with snapshot-restored ones
- No leftover guest volumes from post-backup rebalances remain

---

#### 5.3 Cross-Cluster Restore — Correct Bucket and Volume Targeting
**Test ID**: `test_backup_restore_cross_cluster_fusion`

**Steps**:
1. Create source cluster A (fusion-enabled); run rebalance; create backup
2. Create target cluster B (fusion-enabled); run its own rebalance
3. Restore backup from A to B
4. Verify fusion S3 bucket cleanup targets **Cluster B's** bucket, not Cluster A's
5. Verify guest volume snapshots from Cluster A's backup are applied to KV nodes on Cluster B
6. Verify data integrity on Cluster B

**Expected Result**:
- Correct per-cluster bucket cleanup isolation
- Guest volume snapshots applied to the correct target cluster's KV nodes

---

#### 5.4 Cross-Region Restore
**Test ID**: `test_backup_restore_cross_region_fusion`

**Steps**:
1. Create fusion-enabled cluster in Region A; run rebalance; create backup
2. Verify cross-region copy includes copies of **both** main disk and guest volume snapshots in Region B
3. Create target cluster in Region B; restore from Region B backup copy
4. Verify guest volumes are recreated on KV nodes from Region B snapshot copies
5. Verify fusion bucket cleanup in Region B; data integrity confirmed

**Expected Result**:
- Cross-region copy correctly replicates both snapshot types
- Guest volume restore from cross-region copies works identically to same-region restore

---

#### 5.5 Guest Volume Snapshot Cleanup After Backup Deletion
**Test ID**: `test_guest_volume_snapshot_cleanup_after_backup_deletion`

**Steps**:
1. Create fusion-enabled cluster; run rebalance; create backup
2. Confirm both main disk and guest volume snapshots are created and tagged
3. Delete the backup (or let it expire per retention policy)
4. Via CSP API, verify **all** guest volume snapshots for that backupID are deleted
5. Verify main disk snapshots for that backupID are also deleted
6. Verify no orphaned guest volume snapshots remain in the CSP account

**Expected Result**:
- Backup deletion purges both snapshot types
- No orphaned guest volume snapshots remain in the CSP

---

### 6. Negative Scenarios

#### 6.1 Guest Volume Snapshot Failure During Backup
**Test ID**: `test_backup_guest_volume_snapshot_failure`

**Steps**:
1. Create fusion-enabled cluster; guest volumes attached to KV nodes via rebalance
2. Simulate CSP snapshot failure for one guest volume (e.g., volume in error state)
3. Trigger backup
4. Verify backup fails or is marked `StateInvalid` — partial guest volume snapshots must not silently produce an inconsistent backup

**Expected Result**:
- Backup fails with clear error if any guest volume cannot be snapshotted
- No partially-complete backup record that falsely implies full guest volume coverage

---

#### 6.2 Restore Fails If Guest Volume Snapshot Missing
**Test ID**: `test_restore_fails_if_guest_volume_snapshot_missing`

**Steps**:
1. Create fusion-enabled backup with guest volume snapshots
2. Manually delete one guest volume snapshot from the CSP
3. Attempt restore
4. Verify restore fails with a clear error identifying the missing snapshot
5. Verify no partial restore state

**Expected Result**: Restore fails clearly and safely when an expected guest volume snapshot is not found.

---

#### 6.3 Fusion Bucket Cleanup Failure During Restore
**Test ID**: `test_restore_fusion_bucket_cleanup_failure`

**Steps**:
1. Create backup with `FusionEnabled: true`
2. Simulate S3 access issue (permission denied)
3. Attempt restore
4. Verify restore fails with error "failed to clean fusion bucket"
5. Verify restore record is marked failed; no partial state

---

#### 6.4 Restore When Fusion Bucket Is Already Empty
**Test ID**: `test_restore_fusion_empty_bucket`

**Steps**:
1. Create backup with `FusionEnabled: true`; manually empty the fusion S3 bucket
2. Restore; verify `DeleteDirectory` returns `NotFoundError` which is handled gracefully
3. Verify restore completes; guest volumes restored from snapshots normally

**Expected Result**: Empty-bucket case is a no-op; restore completes successfully.

---

#### 6.5 Storage Lookup Failure for Fusion Bucket
**Test ID**: `test_restore_fusion_storage_lookup_failure`

**Steps**:
1. Create backup with `FusionEnabled: true`; corrupt the `FusionStorage` tracker record
2. Attempt restore
3. Verify error: "failed to get storage for fusion bucket cleanup"

---

#### 6.6 Object Client Creation Failure (Credential Issue)
**Test ID**: `test_restore_fusion_object_client_failure`

**Steps**:
1. Simulate AWS/GCP/Azure credential issue; attempt restore of fusion-enabled backup
2. Verify error: "failed to create tools-common object client"

---

### 7. Edge Cases

#### 7.1 Backup With Versioned Objects in Fusion Bucket
**Test ID**: `test_restore_fusion_versioned_bucket`

**Steps**:
1. Create fusion-enabled cluster; perform multiple rebalances (creates versioned S3 objects in fusion bucket)
2. Create backup; restore to target cluster
3. Verify ALL S3 object versions are deleted (`Versions: true` in `DeleteDirectory`)

**Expected Result**: All versions cleaned; no stale versioned shard data remains in the bucket.

---

#### 7.2 Multiple Rebalances — Large Guest Volume Inventory
**Test ID**: `test_backup_large_guest_volume_inventory`

**Steps**:
1. Create fusion-enabled multi-node cluster; perform many rebalances
2. Accumulate a large number of guest volumes across all KV nodes
3. Trigger backup; verify complete snapshot inventory
4. Restore to new cluster; verify all guest volumes recreated from snapshots on the correct KV nodes

---

#### 7.3 Restore With Guest Volume Count Mismatch
**Test ID**: `test_guest_volume_count_mismatch_backup_restore`

**Scenario**: Source cluster had N guest volumes at backup time; target cluster currently has M guest volumes (M ≠ N).

**Steps**:
1. Create source cluster; run rebalances; create backup (N guest volumes snapshotted)
2. Create target cluster; run a different number of rebalances (M guest volumes on KV nodes)
3. Restore backup to target cluster
4. Verify target cluster ends up with exactly N guest volumes (from backup snapshots), not M
5. Verify the pre-existing M guest volumes on target KV nodes are removed before the N restored volumes are attached

**Expected Result**:
- Restore is authoritative: KV node guest volume state matches backup, not pre-restore state

---

#### 7.4 Backup During Guest Volume Write — Verify Restore Consistency
**Test ID**: `test_backup_mid_write_restore_consistency`

**Scenario**: Backup taken while accelerators are downloading data from S3 onto guest volumes (which
are still attached to accelerator nodes at this point, not yet on KV nodes). Restored guest volumes
will be in a mid-download state. Verify this is a consistent restore point.

**Steps**:
1. Create fusion-enabled cluster; trigger rebalance; capture backup mid-write
2. Restore to target cluster
3. Verify KV nodes have guest volumes in the expected mid-write state
4. Trigger a new fusion rebalance on the target cluster
5. Verify fresh accelerators can start from the restored guest volume state and complete the rebalance

**Expected Result**:
- Mid-write guest volume state is a valid restore point
- A new rebalance triggered post-restore completes successfully

---

## Validation Checklists

### Post-Backup Validation
- [ ] Backup record exists with correct `FusionEnabled` value
- [ ] Snapshot count: `(# KV nodes)` main disk snapshots + `(total attached guest volumes)` guest volume snapshots
- [ ] Every main disk snapshot has **no** `IsFusionGuestVolume` tag
- [ ] Every guest volume snapshot is tagged `IsFusionGuestVolume: true`
- [ ] All snapshots tagged: `TenantID`, `ClusterID`, `BackupID`, `NodeID`
- [ ] Guest volume snapshot `NodeID` matches the KV node the volume was attached to
- [ ] Backup status is `triggered` (not `invalid`)
- [ ] Backup skew < 10 seconds

### Post-Restore Validation
- [ ] Restore record status is `complete`
- [ ] If `FusionEnabled: true`: fusion S3 bucket is empty post-restore
- [ ] Main disk snapshots used to recreate KV node data volumes
- [ ] Guest volume snapshots used to recreate guest volumes on the correct KV nodes
- [ ] Per-node guest volume assignment matches the backup (`NodeID` mapping preserved)
- [ ] No pre-existing (pre-restore) guest volumes remain on KV nodes
- [ ] No pending or lingering cp-jobs for guest volume deletion survive the restore
- [ ] Data integrity verified (item count, CRC check)
- [ ] A new fusion rebalance can be triggered after restore (fresh accelerators spawn and operate correctly)

### Snapshot Inventory Validation (CSP API)
- [ ] For each backup: total snapshots = `(# KV nodes)` + `(# guest volumes at backup time)`
- [ ] All guest volume snapshot volume IDs match attached guest volume IDs recorded at backup time
- [ ] After backup deletion: all guest volume AND main disk snapshots purged from CSP
- [ ] No orphaned snapshots in CSP after deletion or failed backup

### Datadog Log Validation
- [ ] `cleanFusionBucket` called when `FusionEnabled: true`
- [ ] `DeleteDirectory` logged with correct bucket name
- [ ] Guest volume snapshot creation logged per volume with `IsFusionGuestVolume` marker
- [ ] Guest volume restoration logged per volume during restore
- [ ] No `FATAL`, panic, or crash from S3 errors during concurrent sync/restore race
- [ ] Pending guest volume deletion jobs logged as aborted during restore

---

## Test Configuration Parameters

```ini
# Common parameters for fusion backup/restore tests
fusion_enabled=true
fusion_storage_type=aws  # or gcp, azure
backup_type=on-demand
restore_type=cross-cluster

# Data parameters
num_items=100000
doc_size=1024

# Fusion specific
fusion_rebalance_type=swap-rebalance
guest_volumes_per_host=5  # vary up to 22 for max-slot tests
```

---

## Test File Location

```
pytests/storage/fusion/fusion_backup_restore.py
```

## Dependencies

- Capella API for snapshot backup/restore operations
- CSP APIs (AWS EC2, GCP Compute, Azure Compute) for snapshot inventory and volume verification
- Datadog API for log validation
- AWS S3 / GCS / Azure Blob API for fusion bucket inspection
- Fusion cluster API for guest volume enumeration and KV node state verification
- CP job queue API for verifying deletion job abort during restore
