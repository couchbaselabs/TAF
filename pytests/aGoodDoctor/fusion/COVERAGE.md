# Fusion Test Coverage

**Authoritative test plan (GDrive):** [Fusion2 - E2E Capella Integration Test Plan](https://docs.google.com/document/d/1rVaNJ9ybrF0vDB-oLYL8GY5U-9ZFDLLJg_liixRmVCw)

This file maps each section of the GDrive plan to its TAF implementation. It is the only fusion test document maintained in this repo.

Status legend: ✅ Automated · 🔲 Planned (stub/file exists) · ⬜ Not Started

---

## §1 Enable / Disable / Stop Fusion

> The `aGoodDoctor/fusion/fusion_enable_disable_test.py` lifecycle suite
> (`FusionEnableDisableTests`) was removed. Its shared `_FusionTestBase` now
> lives in `fusion_fallback_test.py`. The on-prem storage lifecycle suite at
> `pytests/storage/fusion/fusion_enable_disable.py` is unaffected.

---

## §1a Fusion Backup / Restore (Capella Dedicated)

| TAF File | TAF Class | TAF Method | Status |
|---|---|---|---|
| `fusion_backup_restore.py` | `FusionBackupRestore` | `test_backup_restore_fusion_enabled_to_enabled` | ✅ |
| | | `test_backup_restore_fusion_enabled_to_disabled` | ✅ |
| | | `test_backup_restore_fusion_disabled_to_enabled` | ✅ |
| | | `test_backup_restore_fusion_disabled_to_disabled` | ✅ |

> **Restore behaviour note (observed):** a Fusion restore brings back KV
> **primary data only** — it does **not** re-apply the `IsFusionGuestVolume`
> snapshots to recreate guest volumes on the target. Data integrity is intact
> (verified by doc-count checks); guest volumes are regenerated on the **next
> Fusion rebalance**. Snapshot-verification tests that check post-restore guest
> volumes therefore trigger a rebalance first
> (`regenerate_guest_volumes_via_rebalance`) and assert guest volumes come back,
> rather than expecting the backup's exact count/per-node mapping. This differs
> from the design doc ("guest volumes recreated during restoration") — flagged
> for the backup/fusion team to confirm intended behaviour.

---

## §2 Fusion Rebalance at Scale

| TAF File | TAF Class | TAF Method | Status |
|---|---|---|---|
| `fusion_accelerator_lifecycle_test.py` | `FusionAcceleratorLifecycleTest` | `test_accelerator_deployment` (was `test_accelerator_creation_during_rebalance` + `test_accelerator_instance_type_validation`) | ✅ |
| | | `test_accelerator_termination_after_rebalance` | ✅ |
| | | `test_ebs_guest_volume_full_lifecycle` | ✅ |
| | | `test_back_to_back_rebalances_no_orphaned_volumes` | ✅ |
| | | fusion state stays 'enabled' — background watcher in `test_fusion_scaling_lifecycle` | ✅ |
| | | `test_accelerator_instance_count_matches_data_size` | ✅ |
| *(no file yet)* | — | EBS slot limits (>24 vol/node), compute quota limits | ⬜ |
| *(no file yet)* | — | Remove node with attached guest volumes | ⬜ |
| *(no file yet)* | — | Rebalance in low-availability regions | ⬜ |
| *(no file yet)* | — | Rebalance during each fusion transitional state | ⬜ |
| *(no file yet)* | — | Full fusion logs used/downloaded post-rebalance | ⬜ |
| *(no file yet)* | — | Hydration failure (unmount EBS, remount) | ⬜ |
| *(no file yet)* | — | 100% RR / 1% RR cache miss ratio validation | ⬜ |
| *(no file yet)* | — | Full compaction during hydration | ⬜ |

---

## §3 Fusion Health Checks

| TAF File | TAF Class | TAF Method | Status |
|---|---|---|---|
| `fusion_health_test.py` | `FusionHealthTest` | `test_stop_start_fusion_resumes_s3_upload` | ✅ |
| *(no file yet)* | — | Pending sync stays <100 GB/node under constant load | ⬜ |
| *(no file yet)* | — | Log file count ≤100/vB (slow-creates, 20 GB/vB) | ⬜ |
| *(no file yet)* | — | Migration progress visible post-rebalance | ⬜ |

---

## §4 NS Server Uploader Management at Scale

| TAF File | TAF Class | TAF Method | Status |
|---|---|---|---|
| *(no file yet)* | — | vB uploader change rules (existing vs new node) | ⬜ |
| *(no file yet)* | — | Pure-creates workload: no log deletion on compute change | ⬜ |
| *(no file yet)* | — | Uploader balancing: 3→27→3 node scaling at 10TB | ⬜ |
| *(no file yet)* | — | Add 1 / 2 / N-1 nodes, verify uploader rebalance | ⬜ |

---

## §5 Bucket Operations

| TAF File | TAF Class | TAF Method | Status |
|---|---|---|---|
| `fusion_bucket_ops_test.py` | `FusionBucketOpsTest` | `test_bucket_flush_cleans_s3_objects` | ✅ |
| | | `test_bucket_delete_after_rebalance_cleans_guest_volumes` | ✅ |
| | | `test_bucket_flush_after_rebalance_no_guest_volumes` | ✅ |
| | | `test_bucket_drop_during_guest_volume_deletion` | ✅ |
| | | `test_bucket_drop_and_recreate_loop` | ✅ |
| | | `test_full_compaction_with_fusion_enabled` | ✅ |
| | | `test_replica_change_uploader_map_unchanged` | ✅ |
| | | `test_flush_during_active_s3_upload` | ✅ |
| | | `test_multi_bucket_flush_cleans_all_s3_prefixes` | ✅ |
| | | `test_bucket_delete_no_prior_rebalance_s3_prefix_cleaned` | ✅ |

---

## §6 Cluster On/Off

| TAF File | TAF Class | TAF Method | Status |
|---|---|---|---|
| `fusion_cluster_on_off_test.py` | `FusionClusterOnOffTest` | `test_cluster_off_on_with_pending_sync_resumes_upload` | ✅ |
| | | `test_cluster_off_on_while_guest_volumes_present` | ✅ |
| | | `test_cluster_off_on_after_guest_volume_detach` | ✅ |
| | | `test_cluster_on_off_functional` | ✅ |
| | | `test_cluster_off_on_snapshot_backup_restore_same_cluster` | ✅ |
| | | `test_enable_fusion_then_immediately_turn_off_cluster` | ✅ |

---

## §7 Fusion Accelerator

| TAF File | TAF Class | TAF Method | Status |
|---|---|---|---|
| `fusion_accelerator_lifecycle_test.py` | `FusionAcceleratorLifecycleTest` | `test_no_public_ip_on_accelerator_nodes` | ✅ |
| | | `test_guest_volume_properties` | ✅ |
| | | `test_guest_volume_size_scales_with_data` | ✅ |
| | | `test_asg_deleted_after_rebalance_within_5_mins` | ✅ |
| `fusion_accelerator_chaos_test.py` | `FusionAcceleratorChaosTest` | `test_remove_node_with_attached_guest_volumes` | ✅ |
| | | `test_slot_exhaustion_triggers_fallback_replacement` | ✅ |
| | | `test_abort_rebalance_invalidates_manifest` | ✅ |
| | | `test_kill_memcached_during_extent_migration` | ✅ |
| | | `test_accelerator_node_termination_resilience` (moved from lifecycle) | ✅ |
| | | `test_accelerator_stopped_mid_download` | ✅ |
| | | `test_asg_cannot_launch_replacement` | ✅ |
| | | `test_accelerator_disk_full_during_download` | ✅ |
| | | `test_accelerator_volume_detached_during_download` | ✅ |
| | | `test_guest_volume_detached_during_transfer` | ✅ |
| `fusion_fallback_test.py` | `FusionFallbackInstanceTypeTests` | `test_fallback_when_top_n_instance_types_unavailable` | ✅ |
| | | `test_fallback_exhausts_all_arm_types_falls_back_to_x86` | ✅ |
| | | `test_no_public_ip_on_accelerator_nodes` | ✅ |
| *(no file yet)* | — | Log file variants: large count (slow mutations) | ⬜ |
| *(no file yet)* | — | Log file variants: large size (post log cleaning) | ⬜ |
| | | `test_accelerator_instance_count_matches_data_size` | ✅ |

---

## §8 AWS Fault Injection (FIS)

| TAF File | TAF Class | TAF Method | Status |
|---|---|---|---|
| `fusion_fallback_test.py` | `FusionFallbackInstanceTypeTests` | `test_fallback_when_top_n_instance_types_unavailable` | ✅ |
| | | `test_fallback_exhausts_all_arm_types_falls_back_to_x86` | ✅ |
| *(no file yet)* | — | S3 unavailability (`aws:network:disrupt-connectivity`) | ⬜ |
| *(no file yet)* | — | EBS Pause I/O (`aws:ebs:pause-volume-io`) | ⬜ |

---

## §9 CP Resiliency

| TAF File | TAF Class | TAF Method | Status |
|---|---|---|---|
| *(no file yet)* | — | Kill CP job during guest volume / accelerator creation | ⬜ |
| *(no file yet)* | — | Delete S3 bucket → CP disables fusion, DCP fallback | ⬜ |
| *(no file yet)* | — | Keep crashing CP job N times (retry resilience) | ⬜ |
| *(no file yet)* | — | Kill CP job mid accelerator-cli download | ⬜ |
| *(no file yet)* | — | Crash dp-accelerator during log file download | ⬜ |
| *(no file yet)* | — | Restart / terminate node during guest volume mounting | ⬜ |
| *(no file yet)* | — | Terminate node after all guest volumes attached | ⬜ |
| *(no file yet)* | — | Delete log files from guest volumes during download | ⬜ |
| *(no file yet)* | — | Delete log files from guest volumes after attachment | ⬜ |
| *(no file yet)* | — | Corrupt log files on guest volumes (junk bytes) | ⬜ |
| *(no file yet)* | — | Restart accelerator nodes mid-download | ⬜ |
| *(no file yet)* | — | Delete log file from S3 → accelerator-cli failure | ⬜ |

---

## §10 Crash Recovery — Server

| TAF File | TAF Class | TAF Method | Status |
|---|---|---|---|
| *(no file yet)* | — | Crash memcached during S3 upload | ⬜ |
| *(no file yet)* | — | Crash memcached during file extent migration | ⬜ |
| *(no file yet)* | — | Abort rebalance on backend after `/controller/rebalance` | ⬜ |

---

## §11 Destroy Cluster

| TAF File | TAF Class | TAF Method | Status |
|---|---|---|---|
| `fusion_cluster_destroy_test.py` | `FusionClusterDestroyTest` | `test_destroy_after_prepare_rebalance` | ✅ |
| | | `test_destroy_during_s3_download` | ✅ |
| | | `test_destroy_during_file_extent_migration` | ✅ |
| | | `test_destroy_in_scale_failed_state` | ✅ |
| | | `test_destroy_during_accelerator_provisioning` | ✅ |
| | | `test_destroy_during_cbs_rebalance` | ✅ |
| | | `test_destroy_with_active_backup` | ✅ |
| | | `test_destroy_rejected_while_restore_source` | ✅ |
| | | `test_destroy_while_turning_off` | ✅ |

---

## §12 Upload / Download / Migration Bandwidth

| TAF File | TAF Class | TAF Method | Status |
|---|---|---|---|
| *(no file yet)* | — | Change upload bandwidth via CP internal API | ⬜ |
| *(no file yet)* | — | Change download bandwidth via CP internal API | ⬜ |
| *(no file yet)* | — | Change migration rate during active migration | ⬜ |

---

## §13 Cluster Upgrades

| TAF File | TAF Class | TAF Method | Status |
|---|---|---|---|
| *(no file yet)* | — | 8.0 → 8.1 upgrade + enable fusion | ⬜ |
| *(no file yet)* | — | All-nodes-parallel upgrade, <30 min SLA | ⬜ |
| *(no file yet)* | — | 27-node cluster upgrade at scale (10TB+) | ⬜ |

---

## §14 XDCR

| TAF File | TAF Class | TAF Method | Status |
|---|---|---|---|
| *(no file yet)* | — | XDCR replication during file extent migration | ⬜ |
| *(no file yet)* | — | XDCR with migration rate = 0 | ⬜ |

---

## Volume / Scale Tests

| Target | TAF File | TAF Class | TAF Method | Status |
|---|---|---|---|---|
| Volume 1 (100TB) | *(no file yet)* | — | Full Volume 1 steps (3→12→27 nodes, backup, on/off) | ⬜ |
| Volume 1 full (30TB/node) | *(no file yet)* | — | Full Volume 1 steps (3→12→27 nodes, backup, on/off) | ⬜ |
| Volume 2 (30×2TB) | *(no file yet)* | — | 30 buckets × 2TB, <30 min SLA | ⬜ |

---

## Capella Feature Integration

Owned by other teams — see their linked plans.

| Feature | Owner | External Plan |
|---|---|---|
| Backup/Restore (AV-96371) | Aman Srivastava | [Fusion Backup/Restore Test Plan](https://docs.google.com/document/d/1u6TVBeznbEHbpEAN9f6KbnR4wu6kgvNzUuvIZBxDd9g) |
| Cluster Clone | Aman Srivastava | TBD |
| Management APIs (AV-94241) | Thuan Nguyen | TBD |
| Fleet Manager (AV-98228) | SRE | TBD |
| Pricing & Billing (AV-94188) | Ankit Pandey | [Fusion Billing Functional Test Plan](https://docs.google.com/document/d/1w83kj9clk-U_dosD9LlHzU__hcTj_eUMM2PTwEd9iWo) |
| UI/UX (AV-94185) | Nimiya Joseph | [Capella UI - Fusion 2 Test Plan](https://docs.google.com/document/d/1qb6fy7N6RcIRqJgCSdF1K0-0FaL-StfZYf_311DUpyQ) |
| Observability/Metrics | Nishant Tripathy | TBD |
| Guardrails (AV-94235) | Nishant Tripathy | TBD |
