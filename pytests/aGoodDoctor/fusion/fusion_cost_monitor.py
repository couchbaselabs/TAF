"""
Fusion Rebalance Cost Monitor -- AWS burn vs. CP-billed cost, per rebalance.

Estimates the real AWS cost incurred by a single fusion rebalance
(accelerator EC2 compute + guest EBS volume storage/IOPS) and compares it
against what CP actually billed the customer for that same rebalance
(billing.variable creditQuantity, verified separately by
FusionCPBillingMonitor).

This is cost VISIBILITY, not a correctness check: it never asserts and
never fails the test -- burn/bill ratio naturally varies by rebalance size
and accelerator instance mix (observed 20.8% on a 3->6 node rebalance vs.
42.2% on a 6->12 node rebalance in the same run), so a fixed threshold
would just be flaky. Read the logged table; act on it manually. Any
AWS/pricing lookup failure logs a warning and degrades gracefully rather
than raising -- a cost estimate is never allowed to fail the test.

Guest-volume cost model
------------------------
A fusion guest volume is created at GUEST_VOLUME_HYDRATION_IOPS (16000)
and GUEST_VOLUME_HYDRATION_THROUGHPUT_MIBPS (2000) for the active
hydration window, then CP steps both down once the accelerator's own
download/rebalance work is done for that volume -- this is independently
observable via EC2's DescribeVolumesModifications (real AWS data, not a
guess), which reports OriginalIops/TargetIops AND
OriginalThroughput/TargetThroughput. The volume itself is NOT deleted at
that point though -- it keeps accruing gp3 storage, extra-IOPS (above the
3000 free baseline), and extra-throughput (above the 125 MiB/s free
baseline) cost at whatever IOPS/throughput it was actually stepped down
to. Throughput alone is a real, comparable-magnitude cost to IOPS during
Phase 1 -- gp3's free 125 MiB/s baseline is dwarfed by the ~2000 MiB/s
hydration rate, unlike IOPS's 3000-free-of-16000 ratio which leaves
relatively less headroom being paid for.

Two things are NOT assumed fixed, both learned the hard way from real job
runs:

1. Not every volume in a rebalance downgrades to exactly 3000 IOPS / 125
   MiB/s (the gp3 free baselines) -- CP's chosen targets vary per volume.
   So the real OriginalIops/TargetIops/OriginalThroughput/TargetThroughput
   off each volume's own modification record is used for billing, never
   the GUEST_VOLUME_HYDRATION_*/GP3_FREE_* constants as assumed values --
   those constants are only fallbacks for volumes with no modification
   record yet (still mid-hydration).
2. CP's downgrade does not fire on a fixed, predictable delay after
   volume creation -- two rebalances with near-identical elapsed time
   from completion have been observed to land on opposite outcomes (one
   already downgraded, one not). So rather than reading volume state
   immediately after the rebalance's own billing checks finish (which
   can easily run before CP's async downgrade has fired at all, making
   the whole post-downgrade phase silently read as $0 for that
   rebalance), this POLLS each volume's live Iops every
   GUEST_VOLUME_CHECK_POLL_INTERVAL_SECS, exiting as soon as every volume
   has moved off the hydration rate -- or, failing that, once
   GUEST_VOLUME_CHECK_DELAY_SECS have passed since the youngest volume
   was created, whichever comes first. This gives CP's downgrade a real
   window to have happened without pretending to know exactly when, while
   not wasting time waiting out the full cap when it already happened
   sooner.

Given that, each volume falls into exactly one of two observed states as
of the checkpoint:
  - Still at hydration IOPS (no modification record yet): billed for its
    full elapsed lifetime at the hydration rate, measured to the
    checkpoint -- this IS the real cost so far, not a placeholder. Phase 2
    is correctly $0 -- it hasn't started yet.
  - Already modified (record found, real StartTime/OriginalIops/TargetIops):
    Phase 1 is real/AWS-verified -- hydration IOPS from creation to the
    real downgrade StartTime. Phase 2's DURATION is then modeled, not
    measured: each guest volume moves its own data onto the node's main
    volume at a dedicated GUEST_VOLUME_TRANSFER_RATE_MBPS, but a node
    drains its guest volumes ONE AT A TIME, not in parallel. CP downgrades
    a node's volumes in a batch (they typically share one StartTime), so
    when K volumes on a node become ready together, they queue: the 1st
    is done (and deleted) at T, the 2nd at 2T, ..., the Kth at K*T, where
    T is one volume's own transfer time. A random volume among them is
    therefore expected to wait+process for the AVERAGE of that arithmetic
    progression -- (T + 2T + ... + K*T)/K = T*(K+1)/2 -- NOT T*K/2 (which
    is what simply halving the K*T batch total would give; the two only
    converge for large K). At K=1 there is no queue at all, so the right
    answer is T*(1+1)/2 = T -- the volume's own full transfer time, no
    reduction -- not T/2. Phase 2 is billed at that volume's own real
    TargetIops (never assumed to be exactly 3000 -- see point 1 above) for
    that T*(K+1)/2 projected duration.

So Phase 1 is always real (AWS-verified once observed, or a real
elapsed-so-far measurement if not yet downgraded); Phase 2 is a real
starting point (the actual downgrade StartTime) but a modeled duration
(never measured to actual deletion, since AWS exposes no such signal
without CloudTrail + waiting it out). If a stronger signal for Phase 2's
true end ever exists (e.g. real DeleteVolume timestamps), replace the
GUEST_VOLUME_TRANSFER_RATE_MBPS calculation in _guest_volume_cost() with
that instead.

Actual disk usage vs. billed GiB
----------------------------------
Separately from cost, _actual_guest_volume_disk_usage_gib() runs `df`
(via SSM) on every node this rebalance's guest volumes are attached to and
sums real bytes used, then logs that against total_billed_gib -- the
strongest ground truth available for whether CP's billed GiB matches what
actually landed on disk. The df output is filtered to THIS rebalance's
mount paths (/<planUUID>/<shard>/guest) specifically, not just any path
containing "guest" -- a node can carry an earlier rebalance's still-
attached guest volumes well after this rebalance's own volumes land on
it, and a bare substring match would silently fold that unrelated data
into a total meant to be compared against this rebalance's own billed
GiB. (Earlier field observations of this being "a few percent to ~15%
ahead of billed" predate this scoping fix and may be partly explained by
it, not root-caused separately.) Requires the SSM agent online on a node
to check it; nodes without it are skipped and
called out via nodes_unreachable rather than silently treated as zero, so
a partial check is never mistaken for a complete one.
"""

import threading
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone

from prettytable import PrettyTable


class AcceleratorCostTracker:
    """
    Accumulates the observed lifetime of every fusion accelerator EC2
    instance seen across repeated polls of one rebalance's accelerator
    fleet.

    Fed by FusionCPResourceMonitor.monitor_cluster_accelerator_instances,
    which already polls list_accelerator_instances() every 5s until the
    fleet returns to 0 -- this tracker just keeps a running-max "how long
    has this instance existed" per instance ID as a side effect of polls
    that already happen, no extra AWS calls needed.

    AWS gives no post-hoc EC2 termination timestamp without CloudTrail, so
    this live-polling capture is the only cheap way to get a real
    (near-)actual instance lifetime instead of guessing one after the fact.
    One tracker instance is meant to cover exactly one rebalance -- create a
    fresh one per rebalance task.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._by_id = {}  # instance_id -> {"type": str, "seconds_alive": float}

    def observe(self, instances):
        """Called once per accelerator poll with the current live instance list
        (raw boto3 instance dicts, as returned by FusionAWSUtil.list_accelerator_instances)."""
        now = datetime.now(timezone.utc)
        with self._lock:
            for inst in instances:
                instance_id = inst.get("InstanceId")
                launch_time = inst.get("LaunchTime")
                if not instance_id or not launch_time:
                    continue
                seconds_alive = (now - launch_time).total_seconds()
                rec = self._by_id.setdefault(
                    instance_id,
                    {"type": inst.get("InstanceType", "unknown"), "seconds_alive": 0.0},
                )
                rec["type"] = inst.get("InstanceType", rec["type"])
                if seconds_alive > rec["seconds_alive"]:
                    rec["seconds_alive"] = seconds_alive

    def snapshot(self):
        """Return {instance_id: {"type": ..., "seconds_alive": ...}}, a plain copy."""
        with self._lock:
            return {k: dict(v) for k, v in self._by_id.items()}


class FusionCostMonitor:
    """Computes and logs AWS burn vs. CP-billed cost for one fusion rebalance."""

    # gp3 baseline IOPS/throughput included free with every volume
    # regardless of size.
    GP3_FREE_IOPS = 3000
    GP3_FREE_THROUGHPUT_MIBPS = 125
    # Fusion accelerator guest volumes run at this elevated IOPS/throughput
    # during active hydration -- see FusionAWSUtil.FUSION_ACCELERATOR_IOPS.
    # Only used as a fallback for volumes with no modification record yet
    # (real per-volume OriginalIops/OriginalThroughput is always preferred
    # once one exists -- see module docstring point 1).
    GUEST_VOLUME_HYDRATION_IOPS = 16000
    GUEST_VOLUME_HYDRATION_THROUGHPUT_MIBPS = 2000
    # This is only the MINIMUM CP provisions a guest volume at, not a fixed
    # size -- larger rebalances get larger guest volumes (observed live:
    # ~350 GiB volumes on a big rebalance). Storage cost always prefers
    # each volume's own real Size from AWS (see size_gib_by_id in
    # _guest_volume_cost()); this constant is only the fallback for a
    # volume AWS didn't report a Size for.
    GUEST_VOLUME_SIZE_GIB = 50

    HOURS_PER_MONTH = 730  # AWS's own convention for GB-month/IOPS-month billing
    GP3_STORAGE_USD_PER_GB_MONTH = 0.08
    GP3_IOPS_USD_PER_IOPS_MONTH = 0.005
    # $0.04 per provisioned MiB/s-month -- verified via the Pricing API
    # (SKU reports $40.96 per GiBps-month; 40.96/1024 = 0.04/MiBps-month).
    GP3_THROUGHPUT_USD_PER_MIBPS_MONTH = 0.04

    # Cap on how long to poll, from guest-volume creation, before reading
    # each volume's IOPS/throughput state for billing -- see module
    # docstring's "Guest-volume cost model" section for why this isn't a
    # flat wait. Polling exits early (before this cap) as soon as every
    # volume has moved off the hydration rate.
    GUEST_VOLUME_CHECK_DELAY_SECS = 180
    GUEST_VOLUME_CHECK_POLL_INTERVAL_SECS = 15

    # Modeled (not AWS-verified) Phase 2 duration: the rate at which a
    # single guest volume drains onto its node's main volume. A node
    # processes its guest volumes one at a time, not in parallel -- see
    # module docstring's arithmetic-progression halving.
    GUEST_VOLUME_TRANSFER_RATE_MBPS = 35

    def __init__(self, fusion_aws_util, log):
        """
        :param fusion_aws_util: FusionAWSUtil instance -- reuses its EC2
            boto3 session, so this shares credentials/region with the rest
            of the test and needs no separate AWS auth.
        :param log: logger to write the comparison table to.
        """
        self.aws = fusion_aws_util
        self.log = log
        self._price_cache = {}  # instance_type -> USD/hour (or None if unpriceable)

    # -------------------------------------------------------------------
    # Pricing
    # -------------------------------------------------------------------

    def _hourly_price(self, instance_type: str):
        """On-demand USD/hour for instance_type in us-east-1, cached per test run."""
        if instance_type not in self._price_cache:
            self._price_cache[instance_type] = self.aws.ec2.get_ec2_on_demand_hourly_price(instance_type)
        return self._price_cache[instance_type]

    # -------------------------------------------------------------------
    # Accelerator EC2 compute cost
    # -------------------------------------------------------------------

    def _accelerator_compute_cost(self, accelerator_observations: dict):
        """
        :param accelerator_observations: AcceleratorCostTracker.snapshot() output
        :return: (total_usd, by_type_count: Counter, by_type_cost: dict)
        """
        total = 0.0
        by_type_count = Counter()
        by_type_cost = defaultdict(float)
        for rec in accelerator_observations.values():
            instance_type = rec["type"]
            price = self._hourly_price(instance_type)
            by_type_count[instance_type] += 1
            if price is None:
                continue
            cost = price * (rec["seconds_alive"] / 3600.0)
            total += cost
            by_type_cost[instance_type] += cost
        return total, by_type_count, by_type_cost

    # -------------------------------------------------------------------
    # Guest EBS volume cost
    # -------------------------------------------------------------------

    def _gp3_storage_cost(self, gib: float, hours: float) -> float:
        return self.GP3_STORAGE_USD_PER_GB_MONTH * gib * (hours / self.HOURS_PER_MONTH)

    def _gp3_extra_iops_cost(self, extra_iops: float, hours: float) -> float:
        if extra_iops <= 0:
            return 0.0
        return self.GP3_IOPS_USD_PER_IOPS_MONTH * extra_iops * (hours / self.HOURS_PER_MONTH)

    def _gp3_extra_throughput_cost(self, extra_mibps: float, hours: float) -> float:
        if extra_mibps <= 0:
            return 0.0
        return self.GP3_THROUGHPUT_USD_PER_MIBPS_MONTH * extra_mibps * (hours / self.HOURS_PER_MONTH)

    def _guest_volume_cost(self, cluster_id: str, plan_uuid: str, total_billed_gib: float):
        """
        Cost for every guest volume this rebalance created -- see module
        docstring's "Guest-volume cost model" for the full reasoning. In
        short: wait until each volume is at least GUEST_VOLUME_CHECK_DELAY_SECS
        old, then read its actual IOPS modification state (if any).
        Phase 1 (hydration IOPS, up to the real downgrade StartTime if one
        is found) is real/AWS-verified. Phase 2 (whatever IOPS it was
        actually stepped down to) starts at that same real StartTime, but
        its DURATION is modeled -- a node drains its guest volumes one at
        a time at GUEST_VOLUME_TRANSFER_RATE_MBPS each, so a volume's
        expected Phase 2 duration is half of its node's total drain time
        (arithmetic-progression average across the node's queued volumes,
        not just its own individual transfer time -- see module
        docstring), not bounded by "now". Both phases use that volume's
        own real OriginalIops/TargetIops -- never an assumed fixed value.

        :param total_billed_gib: total GiB CP billed for this rebalance,
            spread evenly across every guest volume as the per-volume data
            size input to the Phase 2 transfer-time model (we don't have
            each volume's own real shard size here).
        :return: (total_usd, phase1_usd, phase2_usd, volume_count, node_by_volume)
            -- node_by_volume ({VolumeId: InstanceId}) is returned so the
            caller can reuse it for the actual-disk-usage check instead of
            re-querying AWS for the same volumes a second time.
        """
        # Scoped to just THIS rebalance's volumes via the fusion-rebalance
        # tag -- get_guest_volumes_for_cluster() alone is cluster-wide and
        # can include an earlier rebalance's still-attached volumes too.
        volumes = self.aws.ec2.list_volumes_by_cluster_id(filters={
            "couchbase-cloud-cluster-id": cluster_id,
            "couchbase-cloud-fusion-rebalance": plan_uuid,
            "couchbase-cloud-fusion-guest-volume": "true",
        })
        if not volumes:
            return 0.0, 0.0, 0.0, 0, {}

        create_time_by_id = {v["VolumeId"]: v.get("CreateTime") for v in volumes if v.get("VolumeId")}
        # Real per-volume provisioned size -- GUEST_VOLUME_SIZE_GIB (50) is
        # only the minimum CP provisions a guest volume at, not a fixed
        # size: larger rebalances get larger guest volumes (observed live,
        # e.g. ~350 GiB volumes on a big rebalance). Storage cost must use
        # each volume's own real Size, never the 50 GiB constant, or it
        # silently undercounts by whatever multiple the real volume is
        # provisioned above the minimum.
        size_gib_by_id = {v["VolumeId"]: v.get("Size", self.GUEST_VOLUME_SIZE_GIB) for v in volumes if v.get("VolumeId")}
        node_by_volume = {}
        for v in volumes:
            attachments = v.get("Attachments", [])
            if attachments and v.get("VolumeId"):
                node_by_volume[v["VolumeId"]] = attachments[0].get("InstanceId")

        # Poll each volume's LIVE Iops/Throughput -- capped at
        # GUEST_VOLUME_CHECK_DELAY_SECS from the youngest volume's
        # creation, exiting early the moment every volume has moved off
        # the hydration rate. CP's async downgrade doesn't fire on a fixed
        # delay (see module docstring), so a flat sleep either wastes time
        # waiting out the full cap when it already happened sooner, or
        # (with a shorter cap) risks reading stale state -- polling with
        # early exit gets the real state as soon as it's available without
        # guessing a fixed number.
        volume_ids = list(create_time_by_id.keys())
        current_iops_by_id, current_tput_by_id = {}, {}
        create_times = [t for t in create_time_by_id.values() if t]
        if create_times:
            youngest_create = max(create_times)
            while True:
                current_volumes = self.aws.ec2.list_volumes_by_cluster_id(filters={
                    "couchbase-cloud-cluster-id": cluster_id,
                    "couchbase-cloud-fusion-rebalance": plan_uuid,
                    "couchbase-cloud-fusion-guest-volume": "true",
                })
                for v in current_volumes:
                    vid = v.get("VolumeId")
                    if vid:
                        current_iops_by_id[vid] = v.get("Iops")
                        current_tput_by_id[vid] = v.get("Throughput")

                all_changed = volume_ids and all(
                    current_iops_by_id.get(vid) is not None
                    and current_iops_by_id[vid] != self.GUEST_VOLUME_HYDRATION_IOPS
                    for vid in volume_ids
                )
                elapsed = (datetime.now(timezone.utc) - youngest_create).total_seconds()
                if all_changed or elapsed >= self.GUEST_VOLUME_CHECK_DELAY_SECS:
                    break
                self.log.info(
                    f"Guest-volume IOPS not yet settled for cluster={cluster_id} "
                    f"planUUID={plan_uuid} ({elapsed:.0f}s/{self.GUEST_VOLUME_CHECK_DELAY_SECS}s) "
                    f"-- polling again in {self.GUEST_VOLUME_CHECK_POLL_INTERVAL_SECS}s"
                )
                time.sleep(self.GUEST_VOLUME_CHECK_POLL_INTERVAL_SECS)

        # Still needed for the real Phase 1/Phase 2 boundary (StartTime) --
        # only the TARGET rate itself now comes from the live poll above,
        # not from this record's TargetIops/TargetThroughput (see module
        # docstring point 1: swapped to the live-observed value).
        modifications = self.aws.ec2.describe_volumes_modifications(volume_ids)
        modification_by_id = {m["VolumeId"]: m for m in modifications if m.get("VolumeId")}

        # Per-volume data size for the Phase 2 transfer-time model below --
        # we don't have each volume's own real shard size here, so this
        # spreads total_billed_gib evenly across every volume in the batch
        # as the best available estimate.
        avg_gib_per_volume = (total_billed_gib / len(volume_ids)) if volume_ids and total_billed_gib else 0.0
        # A node drains its guest volumes ONE AT A TIME (not in parallel),
        # so volumes that become ready simultaneously (the common case --
        # CP downgrades a node's volumes in one batch) queue up: 1st done
        # at T, 2nd at 2T, ..., Kth at K*T, where T is a single volume's
        # own transfer time. See the halving note below.
        node_volume_counts = Counter(n for n in node_by_volume.values() if n)
        # Per-node breakdown, for the log table below -- summed Phase 2 $ by
        # component (storage / extra-IOPS / extra-throughput), plus the last
        # observed target IOPS/throughput for that node (volumes on the same
        # node are downgraded together in one CP batch, so these are
        # normally uniform across a node's volumes; last-write-wins here is
        # just to have *a* representative value to display).
        node_phase2_storage_usd = defaultdict(float)
        node_phase2_iops_usd = defaultdict(float)
        node_phase2_tput_usd = defaultdict(float)
        node_phase2_usd = defaultdict(float)
        node_target_iops = {}
        node_target_tput = {}

        phase1_usd = 0.0
        phase2_usd = 0.0
        now = datetime.now(timezone.utc)
        downgraded_count = 0
        still_hydrating_count = 0
        for vol_id, create_time in create_time_by_id.items():
            if create_time is None:
                continue
            volume_size_gib = size_gib_by_id.get(vol_id, self.GUEST_VOLUME_SIZE_GIB)
            mod = modification_by_id.get(vol_id)

            if mod is not None and mod.get("StartTime") is not None:
                downgraded_count += 1
                downgrade_time = mod["StartTime"]
                original_iops = mod.get("OriginalIops", self.GUEST_VOLUME_HYDRATION_IOPS)
                original_tput = mod.get("OriginalThroughput", self.GUEST_VOLUME_HYDRATION_THROUGHPUT_MIBPS)
                # Live-observed, not the modification record's Target* --
                # this is what the volume is ACTUALLY set to right now,
                # which is what it's actually being billed at (see module
                # docstring point 1).
                target_iops = current_iops_by_id.get(vol_id, mod.get("TargetIops", self.GP3_FREE_IOPS))
                target_tput = current_tput_by_id.get(vol_id, mod.get("TargetThroughput", self.GP3_FREE_THROUGHPUT_MIBPS))

                # Phase 1 -- real, AWS-verified: create_time to the real
                # downgrade StartTime, at this volume's own real original IOPS.
                phase1_hours = max(0.0, (downgrade_time - create_time).total_seconds() / 3600.0)

                # Phase 2 -- modeled, not measured: how long this volume is
                # expected to remain alive (and billed) post-downgrade.
                # Each volume moves its own data off its node's main volume
                # at GUEST_VOLUME_TRANSFER_RATE_MBPS, but the node processes
                # its guest volumes one at a time (not in parallel), so a
                # node with K volumes ready together (T = one volume's own
                # transfer time) finishes them at T, 2T, ..., K*T -- a random
                # volume among them is expected to wait+process for the
                # AVERAGE of that arithmetic progression, (T+2T+...+K*T)/K =
                # T*(K+1)/2 -- NOT T*K/2 (which is what halving K*T would
                # give). The two only converge for large K; at K=1 there's no
                # queue at all, so the right answer is T*(1+1)/2 = T (its own
                # full transfer time, no reduction), not T/2. This is a
                # projected TOTAL duration, not bounded by "now".
                node = node_by_volume.get(vol_id)
                node_volume_count = node_volume_counts.get(node, 1)
                single_volume_transfer_hours = (avg_gib_per_volume * 1024) / self.GUEST_VOLUME_TRANSFER_RATE_MBPS / 3600.0
                node_transfer_hours = node_volume_count * single_volume_transfer_hours
                phase2_hours = (node_volume_count + 1) / 2.0 * single_volume_transfer_hours

                phase1_usd += self._gp3_storage_cost(volume_size_gib, phase1_hours)
                phase1_usd += self._gp3_extra_iops_cost(original_iops - self.GP3_FREE_IOPS, phase1_hours)
                phase1_usd += self._gp3_extra_throughput_cost(
                    original_tput - self.GP3_FREE_THROUGHPUT_MIBPS, phase1_hours
                )

                this_storage_usd = self._gp3_storage_cost(volume_size_gib, phase2_hours)
                this_iops_usd = self._gp3_extra_iops_cost(target_iops - self.GP3_FREE_IOPS, phase2_hours)
                this_tput_usd = self._gp3_extra_throughput_cost(
                    target_tput - self.GP3_FREE_THROUGHPUT_MIBPS, phase2_hours
                )
                this_volume_phase2_usd = this_storage_usd + this_iops_usd + this_tput_usd
                phase2_usd += this_volume_phase2_usd
                if node:
                    node_phase2_storage_usd[node] += this_storage_usd
                    node_phase2_iops_usd[node] += this_iops_usd
                    node_phase2_tput_usd[node] += this_tput_usd
                    node_phase2_usd[node] += this_volume_phase2_usd
                    node_target_iops[node] = target_iops
                    node_target_tput[node] = target_tput
            else:
                # No modification record as of the checkpoint -- still at
                # hydration IOPS/throughput for its whole observed lifetime
                # so far. This is the real cost so far, not a placeholder;
                # Phase 2 is correctly $0 because it hasn't started yet.
                still_hydrating_count += 1
                phase1_hours = max(0.0, (now - create_time).total_seconds() / 3600.0)
                phase1_usd += self._gp3_storage_cost(volume_size_gib, phase1_hours)
                phase1_usd += self._gp3_extra_iops_cost(
                    self.GUEST_VOLUME_HYDRATION_IOPS - self.GP3_FREE_IOPS, phase1_hours
                )
                phase1_usd += self._gp3_extra_throughput_cost(
                    self.GUEST_VOLUME_HYDRATION_THROUGHPUT_MIBPS - self.GP3_FREE_THROUGHPUT_MIBPS, phase1_hours
                )

        self.log.info(
            f"Guest volume IOPS state at checkpoint for cluster={cluster_id} planUUID={plan_uuid}: "
            f"{downgraded_count} downgraded, {still_hydrating_count} still at hydration IOPS "
            f"({len(volume_ids)} total)"
        )

        if node_volume_counts:
            node_table = PrettyTable()
            node_table.field_names = [
                "Node ID", "GiB (vols)", "IOPS/Tput",
                "Transfer hrs (full->expected)", "$: store+iops+tput=total",
            ]
            for node_id, count in sorted(node_volume_counts.items(), key=lambda kv: -kv[1]):
                node_gib = count * avg_gib_per_volume
                single_volume_hours = (avg_gib_per_volume * 1024) / self.GUEST_VOLUME_TRANSFER_RATE_MBPS / 3600.0
                node_transfer_hours = count * single_volume_hours
                expected_hours = (count + 1) / 2.0 * single_volume_hours  # see _guest_volume_cost() for derivation
                node_table.add_row([
                    node_id,
                    f"{node_gib:.2f} ({count})",
                    f"{node_target_iops.get(node_id, '?')}/{node_target_tput.get(node_id, '?')}",
                    f"{node_transfer_hours:.4f} -> {expected_hours:.4f}",
                    f"{node_phase2_storage_usd.get(node_id, 0.0):.4f}+"
                    f"{node_phase2_iops_usd.get(node_id, 0.0):.4f}+"
                    f"{node_phase2_tput_usd.get(node_id, 0.0):.4f}="
                    f"{node_phase2_usd.get(node_id, 0.0):.4f}",
                ])
            self.log.info(
                f"Guest volume Phase 2 (post-downgrade) cost breakdown per node for "
                f"cluster={cluster_id} planUUID={plan_uuid} (GiB is avg_gib_per_volume x "
                f"guest-volume-count; IOPS/Tput is that node's live-observed target "
                f"values):\n{node_table}"
            )

        return phase1_usd + phase2_usd, phase1_usd, phase2_usd, len(volume_ids), node_by_volume

    # -------------------------------------------------------------------
    # Actual disk usage (ground truth, via df over SSM)
    # -------------------------------------------------------------------

    def _actual_guest_volume_disk_usage_gib(self, plan_uuid: str, node_by_volume: dict):
        """
        Real `df` usage (not modeled) across every node this rebalance's
        guest volumes are attached to -- the strongest ground truth
        available for "how much data is actually on disk" vs. what CP
        billed for. Requires the SSM agent to be online on each node;
        nodes it can't reach are skipped (not counted as zero) and
        reported separately so a partial check doesn't silently masquerade
        as a complete one.

        Filtered to THIS rebalance's mount paths specifically
        (/<planUUID>/<shard>/guest -- see fusion_monitor_util's mount
        convention) -- a bare "guest" substring match would also pick up
        an earlier rebalance's still-attached guest volumes on the same
        node (observed directly: a node can carry a prior rebalance's
        volumes well after this rebalance's own volumes land on it), which
        would silently inflate this number past what total_billed_gib
        (scoped to only this rebalance) is being compared against.

        :param plan_uuid: this rebalance's planUUID, to scope the df output
        :param node_by_volume: {VolumeId: InstanceId}, from _guest_volume_cost()
        :return: (total_actual_gib, nodes_checked, nodes_unreachable)
        """
        node_ids = sorted({n for n in node_by_volume.values() if n})
        if not node_ids:
            return 0.0, 0, 0

        df_cmd = f"df --output=target,used -B1 | grep '/{plan_uuid}/'"
        total_bytes = 0
        nodes_checked = 0
        nodes_unreachable = 0
        for node_id in node_ids:
            try:
                result = self.aws.ec2.run_shell_command(node_id, df_cmd, timeout=60)
            except Exception as e:
                self.log.warning(f"df check failed for node {node_id} (unreachable, skipping): {e}")
                nodes_unreachable += 1
                continue
            if not result.get("success"):
                self.log.warning(
                    f"df check skipped for node {node_id} (SSM not ready or command "
                    f"failed): {result.get('stderr')}"
                )
                nodes_unreachable += 1
                continue
            node_bytes = 0
            for line in result.get("stdout", "").splitlines():
                parts = line.split()
                if len(parts) != 2 or not parts[1].isdigit():
                    continue
                node_bytes += int(parts[1])
            total_bytes += node_bytes
            nodes_checked += 1

        return total_bytes / (1024 ** 3), nodes_checked, nodes_unreachable

    # -------------------------------------------------------------------
    # Public entry point
    # -------------------------------------------------------------------

    def estimate_and_log_rebalance_cost(
        self, cluster_id: str, plan_uuid: str,
        accelerator_observations: dict, total_billed_gib, billed_credit_quantity,
    ):
        """
        Log a burn-vs-bill comparison table for one rebalance. Never raises
        and never asserts (see module docstring) -- any AWS/pricing lookup
        failure just logs a warning and returns.

        :param cluster_id: Capella cluster ID
        :param plan_uuid: fusion rebalance planUUID
        :param accelerator_observations: AcceleratorCostTracker.snapshot()
            captured live during this rebalance (see monitor_cluster_status's
            cost_tracker param); pass {} if no tracker was wired up -- EC2
            compute cost is then reported as 0/not tracked
        :param total_billed_gib: total GiB CP billed for this rebalance
            (sum of PagerTask shardSizeInBytes -- caller already computes
            this as `expected_gib` for the variable-record check)
        :param billed_credit_quantity: creditQuantity CP actually billed
            (from FusionCPBillingMonitor.query_variable_records), or None
            if billing.variable verification is disabled/unavailable
        """
        try:
            compute_usd, by_type_count, by_type_cost = self._accelerator_compute_cost(
                accelerator_observations or {}
            )
            guest_vol_usd, phase1_usd, phase2_usd, volume_count, node_by_volume = self._guest_volume_cost(
                cluster_id, plan_uuid, total_billed_gib or 0.0
            )
            total_burn = compute_usd + guest_vol_usd

            # Ground-truth check: actual df usage on the guest volumes vs.
            # what CP billed for them. Isolated in its own try/except --
            # SSM reachability is flaky (see nodes_unreachable) and must
            # never take down the rest of this cost estimate.
            actual_gib, nodes_checked, nodes_unreachable = None, 0, 0
            try:
                actual_gib, nodes_checked, nodes_unreachable = self._actual_guest_volume_disk_usage_gib(
                    plan_uuid, node_by_volume
                )
            except Exception as e:
                self.log.warning(
                    f"Actual guest-volume disk usage check failed for cluster={cluster_id} "
                    f"planUUID={plan_uuid} (non-fatal, skipping): {e}"
                )

            type_table = PrettyTable()
            type_table.field_names = ["Instance Type", "Count", "EC2 Cost (USD)"]
            for instance_type, count in sorted(by_type_count.items(), key=lambda kv: -by_type_cost.get(kv[0], 0)):
                type_table.add_row([instance_type, count, f"{by_type_cost.get(instance_type, 0.0):.4f}"])

            summary = PrettyTable()
            summary.field_names = ["Metric", "Value"]
            summary.add_row(["Cluster ID", cluster_id])
            summary.add_row(["Plan UUID", plan_uuid])
            summary.add_row(["Accelerator EC2 compute (USD)", f"{compute_usd:.4f}"])
            summary.add_row(["Guest volumes -- 16000-IOPS phase (USD)", f"{phase1_usd:.4f}"])
            summary.add_row(["Guest volumes -- post-downgrade phase (USD)", f"{phase2_usd:.4f}"])
            summary.add_row(["Guest volume count", volume_count])
            if total_billed_gib is not None:
                summary.add_row(["Total GiB processed (billed)", f"{total_billed_gib:.4f}"])
            if actual_gib is not None and nodes_checked:
                summary.add_row(["Actual disk usage on guest volumes (GiB, via df)", f"{actual_gib:.4f}"])
                coverage = (
                    f"{nodes_checked} node(s) checked"
                    + (f", {nodes_unreachable} unreachable (excluded)" if nodes_unreachable else "")
                )
                summary.add_row(["  df coverage", coverage])
                if total_billed_gib:
                    delta_gib = actual_gib - total_billed_gib
                    summary.add_row(["  Actual vs. billed delta (GiB)", f"{delta_gib:+.4f}"])
                    summary.add_row(["  Actual vs. billed delta (%)", f"{delta_gib / total_billed_gib * 100:+.1f}%"])
                if nodes_unreachable:
                    self.log.warning(
                        f"Actual disk usage check for cluster={cluster_id} planUUID={plan_uuid} "
                        f"is PARTIAL -- {nodes_unreachable} node(s) had no SSM agent online and "
                        f"were excluded, so the real total may be higher than {actual_gib:.4f} GiB"
                    )
            elif total_billed_gib is not None:
                summary.add_row(
                    ["Actual disk usage on guest volumes (GiB, via df)", "no node reachable via SSM"]
                )
            summary.add_row(["Total AWS burn (USD)", f"{total_burn:.4f}"])
            if billed_credit_quantity is not None:
                summary.add_row(["Billed to customer (credits)", f"{billed_credit_quantity:.4f}"])
                summary.add_row(["Margin (billed - burn)", f"{billed_credit_quantity - total_burn:.4f}"])
                if billed_credit_quantity:
                    margin_pct = (billed_credit_quantity - total_burn) / billed_credit_quantity * 100
                    summary.add_row(["Margin %", f"{margin_pct:.1f}%"])
            else:
                summary.add_row(["Billed to customer (credits)", "not verified this run"])

            self.log.info(
                f"Fusion rebalance AWS cost estimate for cluster={cluster_id}, "
                f"planUUID={plan_uuid}:\nAccelerator instance types:\n{type_table}\n"
                f"Burn vs. bill:\n{summary}"
            )
        except Exception as e:
            self.log.warning(
                f"Fusion cost estimation failed for cluster={cluster_id} "
                f"planUUID={plan_uuid} (non-fatal, skipping): {e}"
            )
