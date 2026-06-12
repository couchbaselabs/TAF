# Fusion Test Coverage

**Authoritative test plan (GDrive):** [Fusion2 - E2E Capella Integration Test Plan](https://docs.google.com/document/d/1rVaNJ9ybrF0vDB-oLYL8GY5U-9ZFDLLJg_liixRmVCw)

This file maps each section of the GDrive plan to its TAF implementation. It is the only fusion test document maintained in this repo.

Status legend: ✅ Automated · 🔲 Planned (stub/file exists) · ⬜ Not Started

---

## §1 Enable / Disable / Stop Fusion

| TAF File | TAF Class | TAF Method | Status |
|---|---|---|---|
| `fusion_enable_disable_test.py` | `FusionEnableDisableTests` | `test_enable_fusion_on_existing_cluster` | ✅ |
| | | `test_enable_fusion_cancel_leaves_empty_s3_bucket` | ✅ |
| | | `test_stop_fusion_during_enable` | ✅ |
| | | `test_kill_memcached_during_enable_cp_retries` | ✅ |
| | | `test_enable_fusion_from_stopped_state` | ✅ |
| | | `test_ns_server_restart_during_enable` | ✅ |
| | | `test_small_cluster_uses_dcp_not_fusion` | ✅ |
| | | `test_rebalance_below_threshold_uses_dcp` | ✅ |
| | | `test_rebalance_above_threshold_uses_fusion` | ✅ |
| | | `test_enable_fusion_no_rebalance_use` | ✅ |
| | | `test_new_bucket_inherits_fusion_when_enabled` | ✅ |
| | | `test_fusion_status_api_fields` | ✅ |
| | | `test_stop_fusion_when_already_disabled` | ✅ |
| | | `test_stop_fusion_during_active_rebalance` | ✅ |
| | | `test_stop_vs_disable_s3_retention_difference` | ✅ |
| | | `test_stop_fusion_on_synced_data_falls_back_to_dcp_rebalance` | ✅ |
| | | `test_stop_then_reenable_resumes_uploads_from_checkpoint` | ✅ |
| | | `test_disable_fusion_when_synced_deletes_s3_objects` | ✅ |
| | | `test_disable_fusion_no_rebalance_stops_uploads` | ✅ |
| | | `test_disable_fusion_during_rebalance_waits_for_operations` | ✅ |
| | | `test_disable_fusion_during_prepare_rebalance_with_leased_logs` | ✅ |
| | | `test_reenable_fusion_creates_new_s3_bucket` | ✅ |
| | | `test_disable_fusion_from_stopped_state` | ✅ |
| | | `test_disable_fusion_during_enabling_state` | ✅ |
| | | `test_override_rebalances_skips_fusion_while_enabled` | ✅ |
| | | `test_override_rebalances_false_restores_fusion_rebalance` | ✅ |
| | | `test_override_has_no_effect_when_fusion_disabled` | ✅ |
| | | `test_invalid_state_transitions` | ✅ |

---

## §2 Fusion Rebalance at Scale

| TAF File | TAF Class | TAF Method | Status |
|---|---|---|---|
| `fusion_accelerator_lifecycle_test.py` | `FusionAcceleratorLifecycleTest` | `test_accelerator_creation_during_rebalance` | ✅ |
| | | `test_accelerator_termination_after_rebalance` | ✅ |
| | | `test_ebs_guest_volume_full_lifecycle` | ✅ |
| | | `test_back_to_back_rebalances_no_orphaned_volumes` | ✅ |
| | | `test_fusion_state_stays_enabled_through_rebalance` | ✅ |
| | | `test_accelerator_instance_count_matches_data_size` | ✅ |
| | | `test_accelerator_registration_completion` | ⬜ |
| | | `test_100m_document_scale_validation` | ⬜ |
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
| *(no file yet)* | — | Drop during CP guest volume deletion | ⬜ |
| *(no file yet)* | — | Drop during hydration (post-rebalance) | ⬜ |
| *(no file yet)* | — | Drop & recreate in a loop | ⬜ |
| *(no file yet)* | — | Full compaction | ⬜ |
| *(no file yet)* | — | Replica change: uploader map unchanged | ⬜ |

---

## §6 Cluster On/Off

| TAF File | TAF Class | TAF Method | Status |
|---|---|---|---|
| `fusion_cluster_on_off_test.py` | `FusionClusterOnOffTest` | `test_cluster_off_on_with_pending_sync_resumes_upload` | ✅ |
| | | `test_cluster_off_on_while_guest_volumes_present` | ✅ |
| | | `test_cluster_off_on_after_guest_volume_detach` | ✅ |
| *(no file yet)* | — | Turn off during active S3 upload | ⬜ |
| *(no file yet)* | — | Enable fusion then immediately turn off | ⬜ |
| *(no file yet)* | — | Turn off during file extent migration | ⬜ |

---

## §7 Fusion Accelerator

| TAF File | TAF Class | TAF Method | Status |
|---|---|---|---|
| `fusion_accelerator_lifecycle_test.py` | `FusionAcceleratorLifecycleTest` | `test_no_public_ip_on_accelerator_nodes` | ✅ |
| | | `test_guest_volume_properties` | ✅ |
| | | `test_guest_volume_size_scales_with_data` | 🔲 |
| | | `test_asg_deleted_after_rebalance_within_5_mins` | ✅ |
| | | `test_accelerator_node_termination_resilience` | ✅ |
| | | `test_accelerator_instance_type_validation` | ⬜ |
| `fusion_fallback_test.py` | `FusionFallbackInstanceTypeTests` | `test_fallback_when_top_n_instance_types_unavailable` | ✅ |
| | | `test_fallback_exhausts_all_arm_types_falls_back_to_x86` | ✅ |
| *(no file yet)* | — | Download speed ≥800 MB/s | ⬜ |
| *(no file yet)* | — | No public IP on accelerator nodes | ⬜ |
| *(no file yet)* | — | Log file variants: large count (slow mutations) | ⬜ |
| *(no file yet)* | — | Log file variants: large size (post log cleaning) | ⬜ |
| *(no file yet)* | — | Correct instance count based on data size | ⬜ |

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
| *(no file yet)* | — | Destroy after prepareRebalance (log files leased) | ⬜ |
| *(no file yet)* | — | Destroy during accelerator log file download | ⬜ |
| *(no file yet)* | — | Destroy during file extent migration | ⬜ |
| *(no file yet)* | — | Destroy in ScaleFailed/RebalanceFailed state with guest volumes attached | ⬜ |

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
| Volume 1 (100TB) | `fusion_accelerator_lifecycle_test.py` | `FusionAcceleratorLifecycleTest` | `test_100m_document_scale_validation` | ✅ (partial — 100M docs) |
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
