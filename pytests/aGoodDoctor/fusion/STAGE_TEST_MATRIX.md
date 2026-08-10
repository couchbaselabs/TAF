# Fusion Acceleration — Stage Model & Stage-Boundary Test Matrix

Working document for planning `fusion_accelerator_lifecycle_test.py` tests around the
acceleration lifecycle. Derived from the control-plane design doc
`couchbase-cloud/internal/clusters/fusion/ACCELERATION.md` and the current TAF cloud suite.

This is a planning/brainstorm doc — the authoritative status tracker is [COVERAGE.md](COVERAGE.md).
Test names below map 1:1 to COVERAGE.md rows.

---

## Canonical Stage Model

The acceleration lifecycle, corrected against ACCELERATION.md (phase numbers in brackets):

```
Pre-flight (no accelerator infra yet)
  P1  Decide fusion vs DCP          [Phase 1]  threshold + feature flags
  P2  CBS returns manifest          [Phase 2]  PlanUUID + per-host data ranges
  P3  Split manifest → shards       [Phase 3]  each shard ≥50GB, ≤22 shards/host

A   Deploy accelerators             [Phase 4]  per shard: EBS + launch templates (x86+ARM)
                                                + ASG(desired=1) → 1 EC2 launches
B   Download                        [Phase 5]  agent registers → attach+mount its EBS
                                                → download shard from S3 → report Complete
C   Transfer volume to cluster node [Phase 6]  detach EBS from accelerator (→ available)
                                                → scale down IOPS 16000 → 3000 (gp3 baseline)
                                                → attach to TARGET node at /dev/xvdb{c+slot}
                                                → dp-agent mounts at
                                                /{planUUID}/{shardNo}/guest{slot}.
                                                Accelerator compute may be destroyed now.
D   Inform CBS + rebalance          [Phase 7]  when ALL shards mounted on ALL nodes:
                                                inform CBS of guest volumes → call CBS
                                                rebalance with PlanUUID (fast, data local)
E   Background/extent migration     [Phase 8]  CBS copies guest-volume data → node's own
                                                managed storage (happens AFTER rebalance call)
F   Teardown                        [Phase 8]  unmount + detach + delete each guest EBS,
                                                delete ASG + launch templates, mark Complete
```

Manifest status lifecycle:
`Pending → DownloadComplete → BackgroundMigration → TearingDown → Complete`
(plus `Invalidated` if CBS loses the PlanUUID after a failed rebalance).

### Common misconceptions (corrected)

- **It is not one accelerator per node.** It is **one accelerator + one EBS volume per shard**;
  the shard count comes from manifest splitting (`totalDataForHost / 50GB`, capped at 22).
- **Volumes are detached *before* the accelerator instance is terminated**, not after — the
  EBS handoff (Stage C) precedes/decouples from compute teardown. A **two-phase teardown**
  can destroy compute early while the EBS volume persists for migration.
- **Not "the main EC2 hosting Couchbase," singular.** There are multiple KV nodes; each shard's
  volume attaches to *its* target node, and one node holds up to 22 guest volumes.
- **The actual CBS rebalance (Stage D) is the point of the whole flow** and is easy to omit
  from a mental model — acceleration replaces the *rebalance step*, not the deployment.
- **"Hydration" = download+attach+mount (Stages B–C).** The guest-EBS → managed-storage copy
  is **background/extent migration (Stage E)**, and it runs *after* the rebalance call.

---

## Rebalance Type Compatibility

Fusion acceleration applies to **any topology change** — scale-out, scale-in, and swap all
drive the same Stage A–F accelerator/guest-volume lifecycle. As of the `_trigger_rebalance()`
refactor, **every test in `fusion_accelerator_lifecycle_test.py` runs under all three types**,
selected by the `rebalance_type` param (default `in`). The whole suite can be run under a single
type, e.g. `-p rebalance_type=swap`.

| `rebalance_type` | Mechanism | Precondition |
|---|---|---|
| `in` (default) / `scale_out` / `rebalance_in` | `rebalance_config("data", +rebalance_delta)` — add nodes | none |
| `out` / `scale_in` / `rebalance_out` | `rebalance_config("data", -rebalance_delta)` — remove nodes | `kv_nodes` high enough to stay ≥ 3 after removal (guarded; fails clearly otherwise) |
| `swap` | change `compute["data"]` to a different type at the same count → Capella replaces nodes | needs a distinct same-family compute; pass `swap_compute=<type>` (fusion's c7g family is not in the provider list, so derivation returns none and the test fails clearly without it) |

Params: `rebalance_type` (default `in`), `rebalance_delta` (default `1`), `swap_compute`
(explicit target instance type for swaps).

Design notes:
- Multi-rebalance tests keep the topology valid across runs by using the **inverse** type on
  the second rebalance (`test_back_to_back_rebalances_no_orphaned_volumes`,
  `test_accelerator_instance_count_matches_data_size`): in↔out, and swap toggles compute back.
- `tearDown` restores **both** node count and compute type to their initial values (a swap
  changes compute without changing count), so the shared cluster is handed back unchanged.
- `_trigger_scale_out()` remains as a deprecated alias (`→ _trigger_rebalance("in")`).

---

## Positive Tests by Stage

Validate each stage succeeds and its stage-boundary properties are correct.
Status: ✅ implemented · ⬜ planned (see COVERAGE.md) · **gap** = not yet in COVERAGE.md

| Stage | Positive test | Method | Status |
|---|---|---|---|
| P1 | below threshold → 0 accelerators (DCP); above → N | `test_accelerator_instance_count_matches_data_size` | ✅ |
| A | all accelerators of one specific EC2 instance type (+ in ASG override list) | `test_accelerator_deployment` | ✅ |
| A | ASG DesiredCapacity==1, 1 instance/ASG, #ASG==#accelerators | `test_asg_desired_capacity_one_per_shard` | ✅ |
| A | private subnet + AZ colocation with KV nodes | `test_accelerator_network_placement` | ⬜ |
| A | IAM instance profile attached | `test_accelerator_iam_instance_profile` | ⬜ |
| A | complete tag set on instances | `test_accelerator_instance_tags_complete` | ⬜ |
| A | launch templates x86 + ARM present | `test_accelerator_launch_templates_x86_and_arm` | ⬜ |
| A/B | EBS gp3 / 16000 IOPS / encrypted / KMS (guest volumes) | `test_guest_volume_properties` | ✅ |
| A/B | accelerator EBS Size ≥ 50 GB floor + 16000 IOPS | `test_accelerator_ebs_volume_min_size` | ✅ |
| A | S3 VPC endpoint present in fusion VPC | `test_s3_vpc_endpoint_present` | ⬜ |
| B | accelerators appear during rebalance (phase 4) | `test_accelerator_deployment` | ✅ |
| B | no public IP on accelerators | `test_no_public_ip_on_accelerator_nodes` | ✅ |
| B | data actively downloading to accelerator EBS | *removed — `test_accelerator_ebs_download_progress` deleted; the download phase is no longer validated from TAF* | **gap** |
| B | dp-accelerator agent healthy / no errors during download | `test_dp_accelerator_agent_healthy_during_download` | **gap** |
| B | all accelerators reach download-complete before any guest volume attaches (Phase 5 precedes Phase 6) | `test_download_completes_before_transfer` | **gap** |
| B | accelerator count stable during download (no unexpected churn) | `test_accelerator_count_stable_during_download` | **gap** |
| C | Stage-C handoff: accelerators terminated + all guest volumes on KV nodes + IOPS scaled below 16000 + guest-volume API returns all + read workload succeeds | `test_guest_volume_transfer_to_cluster` | ✅ |
| C | volume attached→available→attached-to-KV-node at /dev/xvdb[c-x] | `test_guest_volume_attached_at_expected_device_path` | ⬜ |
| C | guest volumes actually attached to KV nodes | `test_guest_volumes_actually_attached_to_kv_nodes` | ✅ folded into `test_guest_volume_transfer_to_cluster` |
| C | IOPS scaled down below the 16000 download value after transfer to KV node (the CP normally lands on the 3000 gp3 baseline, but the exact value is not asserted) | `test_guest_volume_iops_scaled_down_after_transfer` | ✅ folded into `test_guest_volume_transfer_to_cluster` |
| C | full EBS guest volume lifecycle (created→hydrated→cleaned) | `test_ebs_guest_volume_full_lifecycle` | ✅ |
| C→F | compute torn down early while EBS persists | `test_two_phase_teardown_compute_before_ebs` | ⬜ |
| D | rebalance completes healthy; fusion stays enabled | `test_fusion_scaling_lifecycle` (background fusion-state watcher, whole lifecycle) | ✅ |
| D | guest volume provisioning scales with data (total capacity always; per-volume size when `fusion_max_slots` pins the shard count) | `test_guest_volume_size_scales_with_data` | ✅ |
| E | background migration copy monitored to completion: migration stats climb, guest-volume API drains N→0, main-volume du rises | `test_background_migration_progress` | ✅ |
| **E** | **data integrity: item count / checksum matches pre vs post migration** | `test_data_integrity_through_migration` | **gap** |
| F | teardown gate: EBS deleted + no orphaned 'available' volumes + ASGs deleted + accelerators terminated + no migration failures (all ep_fusion_migration_*==0) + cluster healthy & fusion enabled + S3 log store intact + read workload succeeds | `test_teardown_after_rebalance` | ✅ |
| F | accelerators + ASGs terminated after rebalance | `test_accelerator_termination_after_rebalance` | ✅ |
| F | ASGs deleted within 5 min SLA | `test_asg_deleted_after_rebalance_within_5_mins` | ✅ |
| F | back-to-back rebalances leave no orphaned volumes | `test_back_to_back_rebalances_no_orphaned_volumes` | ✅ |
| F | launch templates (x86 + ARM) deleted at teardown | `test_launch_templates_deleted_at_teardown` (needs `describe_launch_templates` helper) | ⬜ |
| A→F | **end-to-end**: every Stage A/B/C/E/F assertion above observed on a SINGLE rebalance, in stage order — also covers ordering/handoff defects the per-stage tests cannot see (e.g. volumes transfer but never drain; teardown "passes" only because migration never ran) | `test_fusion_scaling_lifecycle` | ✅ |

**Per-stage vs end-to-end.** The Stage A–F rows above are each validated by their own test
(one rebalance per stage — a failure isolates cleanly), and *also* by
`test_fusion_scaling_lifecycle`, which drives one rebalance and walks the stages in order.
Both share the same `_validate_*` stage validators in `fusion_accelerator_lifecycle_test.py`,
so there is exactly one copy of each assertion. Run the per-stage tests when triaging a known
stage; run the end-to-end test for cheap full-lifecycle coverage (7 rebalances → 1).

**Stage-E migration is monitorable via exposed stats.** The server publishes per-node/bucket
`ep_fusion_migration_*` cbstats (read over SSM via `run_cbstats_on_all_nodes`):
`ep_fusion_migration_completed_bytes` climbs toward `ep_fusion_migration_total_bytes`
(progress = completed/total), and `ep_fusion_migration_failures` should stay 0.
`test_background_migration_progress` watches these plus two corroborating signals —
the guest-volume API count draining N→0 (`get_active_guest_volumes` /
`_get_active_guest_volume_count`) and the main persistent-volume `df` usage rising
(`get_main_volume_disk_usage_percent`, the `/opt/couchbase/var/lib/couchbase` mount the
extents copy into). Remaining Stage-E gap: **data integrity** (`test_data_integrity_through_migration`)
— nothing yet asserts item count / checksum matches pre vs post migration.

**Download progress (Stage B) — how to measure and what to expect.** Two ways to observe
bytes landing on an accelerator EBS volume during download:
- *CloudWatch EBS metrics (available, not currently used):* polling `VolumeWriteBytes` per
  accelerator volume — non-zero and increasing ⇒ active download. No SSM / public IP / mount
  path needed. `awslib/cloudwatch_lib.py` (`CloudWatchLib.get_ebs_metric_sum`) still provides
  this, but no test calls it since `test_accelerator_ebs_download_progress` was deleted.
  Caveat that made it awkward: EBS CloudWatch metrics lag real time by a few minutes, so
  small/fast downloads finish before any datapoint publishes, and the check reads as a pass
  on noise unless the load is large enough to keep the download running for minutes.
- *SSM `df`/`du` on the accelerator:* the pattern exists in
  `get_main_volume_disk_usage_percent` (SSM `df` — but against KV nodes, not accelerators).
  Viable only if accelerator instances are SSM-managed (probe with `ec2_lib.is_instance_ssm_ready`
  and fail clearly if not) and using `df` on the device (the accelerator-side download mount
  path is not documented; `/{planUUID}/{shardNo}/guest{slot}` is the *cluster-node* mount).

Expected size is **not exposed to TAF**: the manifest `LogicalSize` / `StorageSize` /
per-shard `shardStorageSize` are CP-internal. Best proxy is the provisioned EBS `Size`
(`ceil(shardStorageSize/1GB) + 10%`, 50 GB floor) → per-shard expected data ≈ `Size / 1.1`.
`ep_fusion_log_store_data_size` (cbstat) / `get_fusion_log_store_data_size_on_s3` give the
*total* S3 log-store size per bucket, an upper bound (a rebalance downloads only the moved
vBuckets). So assert a **bounded** invariant (used bytes climb toward, never exceed, provisioned
Size; end > 0), not an exact match.

**Accelerator count (Stage A) is validated loosely, not by formula.** Per ACCELERATION.md the
count is `sum over hosts of min(ceil(hostData / 50GB), 22)` — one accelerator + one EBS + one
ASG per shard, each shard ≥ 50 GB (`minVolumeSize`), capped at `maxSlots = 22` per host.
`test_accelerator_instance_count_matches_data_size` only asserts `>= expected_accelerator_count`
(a param), and `test_asg_desired_capacity_one_per_shard` asserts `#ASG == #accelerators`. Exact
formula validation would need per-host `StorageSize` from the CBS manifest, which TAF cannot read
directly — so it is intentionally not asserted. The 50 GB floor itself is checked by
`test_accelerator_ebs_volume_min_size`.

**IOPS scale-down (Stage C) is validated by `test_guest_volume_transfer_to_cluster`.** Note
`test_guest_volume_properties` only checks the *download-time* value (16000) while volumes are
still on the accelerators; the scale-down to 3000 is checked separately after transfer. Key
implementation detail: `FUSION_ACCELERATOR_IOPS = 16000` is used as the *identifier* for
accelerator volumes (`list_accelerator_instances` at `fusion_aws_util.py:258`,
`monitor_ebs_cleanup` at `fusion_cp_resource_monitor.py:164`), so those helpers stop matching a
volume once it is scaled to 3000. The test therefore captures the volume IDs while at 16000 and
re-reads each via `get_ebs_volume_by_id`, polling until `Iops == 3000` (a *soft*, async CP step).

---

## Negative Tests by Stage Boundary

"Something goes wrong between stages X and Y." Injects a fault at the boundary and asserts
the CP recovers, fails cleanly, or the safety invariant holds.

| Boundary | Fault injected | Method | Status |
|---|---|---|---|
| A→B | accelerator instance terminated mid-download | `test_accelerator_node_termination_resilience` (chaos) | ✅ |
| A→B | accelerator instance STOPPED mid-download (stays an ASG member, fails health check) | `test_accelerator_stopped_mid_download` (chaos) | ✅ |
| A | ASG can't launch (instance type unavailable) → fallback | `test_fallback_*` (fusion_fallback_test.py) | ✅ |
| B | S3 object deleted / network disrupted mid-download | `test_s3_disrupt_during_download` (FIS network) | gap |
| B | download volume force-detached from its accelerator mid-download → re-attach or redeploy + retry | `test_accelerator_volume_detached_during_download` (chaos) | ✅ |
| B | dp-accelerator agent crashes mid-download | `test_dp_accelerator_crash_during_download` | ⬜ — needs SSM on the accelerator (`pkill` over SSM). Whether the accelerator AMI runs the SSM agent with a permitting instance profile is untested; nothing in the suite establishes it |
| B | S3 download throttled below the log-file lease TTL → leases expire mid-download → DCP fallback | `test_download_rate_limit_expires_lease_falls_back_to_dcp` (chaos) | ✅ — `accelerator.download.rateLimit` on the fusion support config; ~25 GiB at 2.5 MiB/s against a 1-hour lease |
| B→C | EBS attach to KV node stalls | `test_ebs_pause_io_during_hydration` (FIS) | **blocked** — `FISLib.create_ebs_mount_failure_experiment` and `simulate_volume_attach_failure` both raise NotImplementedError |
| C | guest-volume files deleted after attach, before rebalance | `test_delete_guest_volumes_during_migration` | ⬜ |
| C | guest-volume files corrupted (junk bytes) | `test_corrupt_guest_volume_files` | ⬜ |
| C | guest-volume permissions toggled | `test_toggle_guest_volume_permissions_during_migration` | ⬜ |
| C→D | host out of slots → fallback replacement | `test_slot_exhaustion_triggers_fallback_replacement` (fusion_accelerator_chaos_test.py) | ✅ |
| D | CBS rebalance aborted → manifest Invalidated → teardown | `test_abort_rebalance_invalidates_manifest` (chaos) | ✅ |
| D | ns_server loses PlanUUID (ErrFusionPlanNotFound) | `test_kill_memcached_during_rebalance` | gap |
| E | delete/corrupt guest volumes *during* migration | (port fusion_migration) | gap |
| E | kill memcached during extent migration | `test_kill_memcached_during_extent_migration` (chaos) | ✅ |
| E | node removed while its guest volumes are migrating | `test_remove_node_with_attached_guest_volumes` (chaos) | ✅ |
| E | cluster off/on during migration | fusion_cluster_on_off_test.py (partial) | ✅ partial |
| E | destroy cluster during migration → force teardown | `test_destroy_during_file_extent_migration` | ✅ |
| F | CP job killed during teardown → orphan detect/retry | `test_kill_cp_job_during_teardown` | **blocked** — no control-plane job API exposed to TAF |
| F | teardown before RebalanceInitiated → safety gate must REFUSE | `test_teardown_blocked_before_rebalance_initiated` | gap |

**Implemented chaos suite.** `fusion_accelerator_chaos_test.py` /
`conf/fusion/cloud/fusion_accelerator_chaos_test.conf` cover eight of these boundaries —
four at the CP/CBS layer (C→D slot exhaustion, D aborted rebalance, E node removal, E
memcached kill), three AWS-infrastructure faults (accelerator terminated, accelerator
stopped, download volume force-detached from its accelerator) and one config-only fault (B
download throttled past the log-file lease). It subclasses
`FusionAcceleratorLifecycleTest`, so every `_validate_*` helper, the migration freeze/resume
lever and the soft-fail `_stage` machinery are reused rather than reimplemented.

Every chaos test freezes background migration before injecting its fault. Guest volumes are
deleted per shard as migration completes, so without the freeze the resource the fault targets
may already be gone — the freeze makes the fault land on a known, complete set of resources,
and the test then resumes migration and asserts the cluster still reclaims everything. In the
lease-expiry test the freeze serves the mirror-image purpose: nothing should reach a KV node at
all, and the freeze is what makes "no guest volume was ever attached to one" — its verdict for
whether the DCP path was taken — an observation rather than a poll-timing race.

Rows marked **blocked** above are not scheduling decisions — the primitive does not exist.
Three need shell access to an accelerator (which this framework does not have) or an
unimplemented FIS action; one needs a control-plane job API that is not exposed.

**Notable negative-invariant test:** the teardown safety gate (`RebalanceInitiated` must be
true before teardown proceeds) prevents destroying guest volumes CBS hasn't used yet
[ACCELERATION.md §Error Handling — "Tear down safety gate"]. Verifying it holds under
adversarial timing is a high-value invariant test.

---

## Notes on translating on-prem (`pytests/storage/fusion`) tests

The on-prem suite manipulates guest storage over SSH to the NFS server. In cloud, the guest
volumes mount on KV nodes at `/{planUUID}/{shardNo}/guest{slot}`, reachable via
`ec2_lib.run_shell_command` (SSM). So the on-prem delete/corrupt/chmod migration tests port
directly — swap SSH-to-NFS for SSM-to-KV-node. NFS-specific knobs (`min_storage_size`,
`nfs_server_path`) do not translate; migration-rate control is available via
`fusion_monitor.set_memcached_global_setting(fusion_migration_rate_limit=...)`.
