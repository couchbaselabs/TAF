"""Daily qe-7 cost report.

Nightly (Jenkins, see Jenkinsfile):
    python3 -m scripts.qe7_cost.run_daily --state-dir state --out-dir out

By hand, for a past day, with your own profile for the base credentials:
    python3 -m scripts.qe7_cost.run_daily --base-profile cbc-main-jenkins \
        --date 2026-11-20

Backtest over events already downloaded (no AWS calls for events/inventory):
    python3 -m scripts.qe7_cost.run_daily --no-download --skip-live \
        --events-dir <dir> --from 2026-11-01 --to 2026-11-18 --now 2026-11-19T13:30:00+00:00

Writes to --out-dir: <day>.json, report.html, slack.txt, alerts.json.
Keeps in --state-dir between runs: ledger.json, jenkins_map.json,
prices.json, history.json.
"""

import argparse
import datetime as dt
import json
import os
import sys
import time

from . import events as ev
from . import model, report
from .jenkins import JenkinsMap
from .pricing import PriceBook

REGIONS = ("us-east-1", "us-east-2", "us-west-2")
UTC = dt.timezone.utc


def log(msg):
    print("[%s] %s" % (time.strftime("%H:%M:%S"), msg), flush=True)


def parse_args(argv):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--date", help="UTC day to report (default: yesterday)")
    p.add_argument("--from", dest="date_from", help="first day of a range")
    p.add_argument("--to", dest="date_to", help="last day of a range")
    p.add_argument("--now", help="pretend the run happens at this ISO time")
    p.add_argument("--regions", default=",".join(REGIONS))
    p.add_argument("--state-dir", default="qe7_cost_state")
    p.add_argument("--events-dir", default=None,
                   help="default: <state-dir>/events")
    p.add_argument("--out-dir", default="qe7_cost_out")
    p.add_argument("--lookback-days", type=int, default=1,
                   help="extra days of events before the report day, for "
                        "instances launched shortly before it; longer-lived "
                        "ones come from the ledger kept in --state-dir")
    p.add_argument("--base-profile", default=None,
                   help="AWS profile for the base credentials (default: env)")
    p.add_argument("--jenkins-url", default="http://172.23.121.80")
    p.add_argument("--spend-limit", type=float, default=500.0)
    p.add_argument("--idle-hours", type=float, default=24.0)
    p.add_argument("--quiet-hours", type=float, default=12.0)
    p.add_argument("--baseline", type=float, default=model.BASELINE_PER_DAY)
    p.add_argument("--report-url", default=None)
    p.add_argument("--no-download", action="store_true")
    p.add_argument("--skip-live", action="store_true")
    p.add_argument("--skip-jenkins", action="store_true")
    return p.parse_args(argv)


def days_to_report(args, now):
    if args.date_from:
        d0 = dt.date.fromisoformat(args.date_from)
        d1 = dt.date.fromisoformat(args.date_to or args.date_from)
    else:
        d0 = d1 = dt.date.fromisoformat(args.date) if args.date else \
            (now - dt.timedelta(days=1)).date()
    out = []
    while d0 <= d1:
        out.append(d0)
        d0 += dt.timedelta(days=1)
    return out


def main(argv=None):
    args = parse_args(argv if argv is not None else sys.argv[1:])
    now = dt.datetime.fromisoformat(args.now) if args.now else \
        dt.datetime.now(UTC)
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    regions = [r.strip() for r in args.regions.split(",") if r.strip()]
    days = days_to_report(args, now)
    state = args.state_dir
    events_dir = args.events_dir or os.path.join(state, "events")
    os.makedirs(state, exist_ok=True)
    os.makedirs(args.out_dir, exist_ok=True)
    window_start = dt.datetime.combine(days[0], dt.time(), UTC) - \
        dt.timedelta(days=args.lookback_days)
    # Events up to now, except for a backfill of an old day: stop one day
    # after it (enough to see that day's instances terminate) instead of
    # downloading everything since
    window_end = min(now, dt.datetime.combine(days[-1], dt.time(), UTC)
                     + dt.timedelta(days=2))

    qe = None
    if not (args.no_download and args.skip_live):
        from .aws import QeSession
        qe = QeSession(base_profile=args.base_profile)
        log("AWS identity: %s" % qe.identity())

    if not args.no_download:
        from . import cloudtrail
        log("CloudTrail %s -> %s, %s" % (window_start, window_end, regions))
        n = cloudtrail.download(qe, regions, window_start, window_end,
                                events_dir, log=log)
        log("CloudTrail: %d new events" % n)
        cloudtrail.prune(events_dir, args.lookback_days + 3, log=log)

    jmap = JenkinsMap(args.jenkins_url, os.path.join(state, "jenkins_map.json"),
                      log=log)
    running = []
    if not args.skip_jenkins:
        log("Jenkins: scanning qe-7 builds since %s" % window_start)
        running = jmap.scan(window_start)
        jmap.save()
        log("Jenkins: %d clusters, %d fleets mapped; %d builds running"
            % (len(jmap.data["clusters"]), len(jmap.data["fleets"]), len(running)))
    else:
        # The Jenkins scan ran separately (python3 -m scripts.qe7_cost.jenkins),
        # on an agent that can reach Jenkins
        try:
            with open(os.path.join(state, "running_builds.json")) as f:
                running = json.load(f)
            log("Jenkins: using the earlier scan (%d clusters, %d fleets, "
                "%d builds running)" % (len(jmap.data["clusters"]),
                                        len(jmap.data["fleets"]), len(running)))
        except FileNotFoundError:
            log("Jenkins: no scan results; idle alerts cannot tell running "
                "builds apart")

    ledger = model.Ledger(os.path.join(state, "ledger.json"))
    for region in regions:
        launches, terms, stats = ev.load(events_dir, region)
        added = ledger.merge_events(launches, terms)
        log("%s: %d launches, %d terminations in events, %d new qe-7 "
            "instances, %d fleet instances without a template"
            % (region, len(launches), len(terms), added,
               stats["fleet_instances_without_template"]))

    live, unattached = {}, []
    if not args.skip_live:
        from . import inventory
        live, unattached = inventory.snapshot(qe, regions)
        ledger.merge_live(live, now)
        log("Live: %d qe-7 instances running, %d unattached qe-7 volumes"
            % (len(live), len(unattached)))
    if args.skip_live:
        # Backtest: no inventory, so only accelerators (which never get a
        # terminate event when they shut themselves down) are closed here;
        # anything else without a terminate is treated as still running.
        still_open = {k for k, v in ledger.items.items() if not v.get("end")
                      and v.get("tags", {}).get("couchbase-cloud-function")
                      != "fusion-accelerator"}
        closed = ledger.close_missing(still_open, now)
    else:
        closed = ledger.close_missing(
            live, now, horizon=window_end if window_end < now else None)
    log("Closed without a terminate event: %s" % dict(closed))

    prices = PriceBook(qe, os.path.join(state, "prices.json"), log=log)
    attribute = model.Attributor(jmap)
    hist_path = os.path.join(state, "history.json")
    try:
        with open(hist_path) as f:
            history = json.load(f)
    except FileNotFoundError:
        history = {}

    for day in days:
        d0 = dt.datetime.combine(day, dt.time(), UTC)
        d1 = d0 + dt.timedelta(days=1)
        rows = model.day_rows(ledger, prices, attribute, d0, d1, now)
        is_latest = day == days[-1]
        summary = model.summarise(rows, unattached if is_latest else [],
                                  baseline=args.baseline)
        key = day.isoformat()
        history[key] = dict(total=summary["total"], by_owner=summary["by_owner"])
        with open(os.path.join(args.out_dir, key + ".json"), "w") as f:
            json.dump(dict(day=key, summary=summary, rows=rows), f)
        log("%s: %s %s" % (key, report.usd(summary["total"]),
                           ", ".join("%s %s" % (o, report.usd(c))
                                     for o, c in list(summary["by_owner"].items())[:5])))

    day = days[-1].isoformat()
    alert_list = model.alerts(summary, ledger, prices, attribute, running, now,
                              args.spend_limit, args.idle_hours,
                              args.quiet_hours, unattached) \
        if not args.skip_live else []
    mtd = report.month_to_date(history, day)
    stats = dict(unpriced=sorted(prices.unpriced), closed=dict(closed),
                 instances_in_ledger=len(ledger.items))
    with open(os.path.join(args.out_dir, "alerts.json"), "w") as f:
        json.dump(alert_list, f, indent=1)
    with open(os.path.join(args.out_dir, "slack.txt"), "w") as f:
        f.write(report.slack_text(day, summary, alert_list, mtd, args.report_url))
    with open(os.path.join(args.out_dir, "report.html"), "w") as f:
        f.write(report.html_report(day, summary, alert_list, mtd, stats))
    with open(hist_path, "w") as f:
        json.dump(history, f, indent=1, sort_keys=True)
    prices.save()
    ledger.save(now)
    if prices.unpriced:
        log("WARNING: no price for %s" % sorted(prices.unpriced))
    log("Done. %d alerts." % len(alert_list))
    return 0


if __name__ == "__main__":
    sys.exit(main())
