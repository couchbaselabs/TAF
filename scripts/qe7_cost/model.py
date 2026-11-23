"""Instance ledger, daily cost and alerts for the qe-7 sandbox.

Costs are on-demand list prices:
* EC2: exact instance-hours from CloudTrail launch/terminate times, plus the
  live inventory at report time.
* EBS: each instance's 50 GB root volume; each Couchbase node's data disk at
  its real size/IOPS when seen live, else at what the test requested (Jenkins
  log), else 1 TB at 16,000 IOPS (TAF's default for >= 1 TB); each fusion
  accelerator's guest volume (16,000 IOPS for about 10 minutes, then baseline
  until it is deleted about 3 hours later).
* A fixed daily baseline for the sandbox itself (control plane, network):
  about $85/day, measured on idle days (7-8 Nov 2026).
Not covered: S3, data transfer, NAT, EBS snapshots.
"""

import collections
import datetime as dt
import json
import statistics

from . import events as ev
from .pricing import default_throughput, gp3_hourly

ENV_TAG = "qe-7"
BASELINE_PER_DAY = 85.0
ROOT_GB = 50
DEFAULT_DATA_DISK = dict(size=1024, iops=16000, throughput=1000)
# Guest volume: 50 GB at 16,000 IOPS / 2,000 MB/s for ~10 min, then baseline
# until deleted ~3 h later (traced in CloudTrail, Nov 2026)
GUEST_VOLUME_USD = (gp3_hourly(50, 16000, 2000) * 10 / 60
                    + gp3_hourly(50) * 3)
LEDGER_KEEP_DAYS = 7      # history.json keeps the daily totals
KEEP_TAGS = ("stack-env", "user-createdby", "couchbase-cloud-cluster-id",
             "couchbase-cloud-function", "couchbase-cloud-fusion-rebalance",
             "taf-docloader-fleet", "aws:autoscaling:groupName", "Name",
             "stack-service")


def iso(t):
    return t.isoformat() if t else None


def parse(t):
    return dt.datetime.fromisoformat(t) if t else None


def overlap_hours(start, end, w0, w1):
    s, e = max(start, w0), min(end, w1)
    return max(0.0, (e - s).total_seconds() / 3600.0)


def normalise_owner(owner):
    """'Subhrajit.Panigrahi+1' and 'subhrajit-panigrahi' are the same person."""
    owner = (owner or "").strip().lower().split("@")[0].split("+")[0]
    return owner.replace(".", "-").replace(" ", "-")


class Ledger(object):
    """{instance_id: {region, type, start, end, last_seen, tags, disk}}"""

    def __init__(self, path=None):
        self.path = path
        self.meta = {}
        self.items = {}
        if path:
            try:
                with open(path) as f:
                    raw = json.load(f)
                self.meta = raw.get("meta", {})
                self.items = raw.get("instances", {})
            except FileNotFoundError:
                pass

    def save(self, now):
        cutoff = now - dt.timedelta(days=LEDGER_KEEP_DAYS)
        self.items = {k: v for k, v in self.items.items()
                      if not v.get("end") or parse(v["end"]) >= cutoff}
        if self.path:
            with open(self.path, "w") as f:
                json.dump({"meta": self.meta, "instances": self.items}, f)

    def merge_events(self, launches, terms):
        added = 0
        for iid, rec in launches.items():
            if rec["tags"].get("stack-env") != ENV_TAG:
                continue
            item = self.items.setdefault(iid, {})
            if not item:
                added += 1
            item.update(region=rec["region"], type=rec["type"],
                        tags={k: v for k, v in rec["tags"].items()
                              if k in KEEP_TAGS})
            if not item.get("start") or parse(item["start"]) > rec["start"]:
                item["start"] = iso(rec["start"])
        for iid, when in terms.items():
            if iid in self.items and not self.items[iid].get("end"):
                self.items[iid]["end"] = iso(when)
                self.items[iid]["end_source"] = "terminate"
        return added

    def merge_live(self, live, now):
        """live: {iid: {region, type, start, tags, disk}} running right now."""
        for iid, rec in live.items():
            item = self.items.setdefault(iid, {})
            item.update(region=rec["region"], type=rec["type"],
                        tags={k: v for k, v in rec["tags"].items()
                              if k in KEEP_TAGS},
                        last_seen=iso(now))
            item.setdefault("start", iso(rec["start"]))
            item.pop("end", None)
            if rec.get("disk"):
                item["disk"] = rec["disk"]

    def close_missing(self, live, now, horizon=None):
        """Instances we think are running but AWS no longer lists ended with
        no TerminateInstances event (accelerators shut themselves down, and
        ASG force-deletes can be missed). End accelerators with the rest of
        their rebalance, else at the last time we saw them running.

        ``horizon``: events were only downloaded up to this time (a backfill
        of an old day). An instance with no terminate before it was still
        running then, so it ends there at the earliest."""
        reb_end, lifetimes = {}, []
        for item in self.items.values():
            if item.get("end") and item.get("end_source") == "terminate" and \
                    item["tags"].get("couchbase-cloud-function") == "fusion-accelerator":
                r = item["tags"].get("couchbase-cloud-fusion-rebalance")
                end = parse(item["end"])
                reb_end[r] = max(reb_end.get(r, end), end)
                lifetimes.append((end - parse(item["start"])).total_seconds())
        median = dt.timedelta(seconds=statistics.median(lifetimes)) \
            if lifetimes else dt.timedelta(minutes=5)
        closed = collections.Counter()
        for iid, item in self.items.items():
            if item.get("end") or iid in live:
                continue
            start = parse(item["start"])
            tags = item.get("tags", {})
            if tags.get("couchbase-cloud-function") == "fusion-accelerator":
                r = tags.get("couchbase-cloud-fusion-rebalance")
                end = reb_end.get(r) if reb_end.get(r, start) > start else \
                    start + median
                source = "rebalance-end"
            else:
                end = parse(item.get("last_seen")) or start
                source = "last-seen"
                if horizon and horizon > end:
                    end, source = horizon, "window-end"
            item["end"] = iso(min(end, now))
            item["end_source"] = source
            closed[source] += 1
        return closed


class Attributor(object):
    """Who and which run an instance belongs to."""

    def __init__(self, jmap):
        self.jmap = jmap

    def __call__(self, item):
        tags = item.get("tags", {})
        role = ev.role(tags)
        group = ev.group_key(tags)
        info = self.jmap.fleet(group) if role == "loader" else \
            self.jmap.cluster(group)
        # The tenant owner tag is authoritative for Capella resources; loader
        # fleets carry the owner of the launch template, so use the build's.
        if role == "loader":
            owner = info.get("owner") or tags.get("user-createdby")
        else:
            owner = tags.get("user-createdby") or info.get("owner")
        return dict(role=role, group=group, run=info.get("job") or group,
                    owner=normalise_owner(owner) or "untagged",
                    spec=info)


def data_disk_hourly(item, spec):
    disk = item.get("disk")
    if not disk:
        iops = spec.get("iops") or (16000 if (spec.get("disk_gb") or 1024) >= 1000
                                    else 3000)
        disk = dict(size=spec.get("disk_gb") or DEFAULT_DATA_DISK["size"],
                    iops=iops, throughput=default_throughput(iops))
    return gp3_hourly(disk["size"], disk.get("iops"), disk.get("throughput"))


def day_rows(ledger, prices, attribute, day_start, day_end, now):
    """Cost rows for one window: one row per instance that overlapped it.
    Instances still running count up to ``now``, so a report for the
    current day only covers the hours that have passed."""
    rows = []
    for iid, item in ledger.items.items():
        start = parse(item["start"])
        end = parse(item.get("end")) or now
        hours = overlap_hours(start, end, day_start, day_end)
        launched_today = day_start <= start < day_end
        a = attribute(item)
        if a["role"] == "controlplane":   # part of the daily baseline
            continue
        if hours <= 0 and not (launched_today and a["role"] == "accel"):
            continue
        ec2 = hours * prices.hourly(item["type"], item["region"])
        ebs = hours * gp3_hourly(ROOT_GB)
        if a["role"] == "node":
            ebs += hours * data_disk_hourly(item, a["spec"])
        if a["role"] == "accel" and launched_today:
            ebs += GUEST_VOLUME_USD
        rows.append(dict(instance=iid, region=item["region"], type=item["type"],
                         role=a["role"], group=a["group"], run=a["run"],
                         owner=a["owner"], hours=round(hours, 3),
                         ec2=round(ec2, 4), ebs=round(ebs, 4)))
    return rows


def summarise(rows, unattached, baseline=BASELINE_PER_DAY):
    by_owner = collections.Counter()
    by_run = collections.defaultdict(lambda: dict(ec2=0.0, ebs=0.0, owner=None,
                                                  roles=collections.Counter()))
    for r in rows:
        cost = r["ec2"] + r["ebs"]
        by_owner[r["owner"]] += cost
        run = by_run[r["run"]]
        run["ec2"] += r["ec2"]
        run["ebs"] += r["ebs"]
        run["owner"] = run["owner"] or r["owner"]
        run["roles"][r["role"]] += cost
    unattached_usd = sum(v["usd_per_day"] for v in unattached)
    total = sum(by_owner.values()) + unattached_usd + baseline
    return dict(
        total=round(total, 2), baseline=baseline,
        unattached_volumes=round(unattached_usd, 2),
        by_owner={k: round(v, 2) for k, v in by_owner.most_common()},
        runs=sorted(([dict(run=k, owner=v["owner"],
                           ec2=round(v["ec2"], 2), ebs=round(v["ebs"], 2),
                           total=round(v["ec2"] + v["ebs"], 2),
                           by_role={kk: round(vv, 2) for kk, vv in v["roles"].items()})
                      for k, v in by_run.items()]),
                    key=lambda r: -r["total"]))


def alerts(summary, ledger, prices, attribute, running_builds, now,
           spend_limit, idle_hours, quiet_hours, unattached):
    """Report-only findings; nothing is deleted."""
    out = []
    if summary["total"] > spend_limit:
        out.append(dict(kind="spend", severity="high",
                        text="Spend %.0f USD exceeds the %.0f USD/day limit"
                             % (summary["total"], spend_limit)))
    running_runs = {"%s #%d" % (b["job"], b["build"]) for b in running_builds}
    # Nodes and loaders only, decided per instance: accelerators carry their
    # cluster's id, and a long-lived one must neither hide its cluster (by
    # being the group's first item) nor keep it looking busy
    groups = collections.defaultdict(list)
    last_activity = {}
    for item in ledger.items.values():
        a = attribute(item)
        if a["role"] not in ("node", "loader"):
            continue
        key = (a["role"], a["group"])
        # Launches and terminations both count as activity (scale-in,
        # rebalance swaps)
        times = [parse(item["start"])] + \
            ([parse(item["end"])] if item.get("end") else [])
        last_activity[key] = max([last_activity.get(key, times[0])] + times)
        if not item.get("end"):
            groups[key].append((item, a))
    quiet_cut = now - dt.timedelta(hours=quiet_hours)
    for (_, group), members in groups.items():
        items = [i for i, _ in members]
        a = members[0][1]
        oldest = min(parse(i["start"]) for i in items)
        age_h = (now - oldest).total_seconds() / 3600
        limit_h = idle_hours if a["role"] == "node" else 6
        if age_h < limit_h or last_activity[(a["role"], group)] > quiet_cut \
                or a["run"] in running_runs:
            continue
        per_day = 24 * sum(prices.hourly(i["type"], i["region"])
                           + gp3_hourly(ROOT_GB)
                           + (data_disk_hourly(i, a["spec"]) if a["role"] == "node" else 0)
                           for i in items)
        what = "cluster" if a["role"] == "node" else "doc-loader fleet"
        out.append(dict(kind="idle_" + a["role"], severity="high",
                        group=group, owner=a["owner"], run=a["run"],
                        instances=len(items), usd_per_day=round(per_day, 2),
                        text="Idle %s %s (%s, %s): %d instances up %.0f h, no "
                             "running build, about %.0f USD/day"
                             % (what, group, a["owner"], a["run"], len(items),
                                age_h, per_day)))
    for vol in unattached:
        out.append(dict(kind="unattached_volume", severity="medium",
                        text="Unattached volume %s (%s, %d GB, %s IOPS): about "
                             "%.2f USD/day" % (vol["id"], vol["owner"] or "untagged",
                                               vol["size"], vol["iops"],
                                               vol["usd_per_day"]), **vol))
    return out
