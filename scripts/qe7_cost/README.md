# qe7_cost: daily qe-7 sandbox cost report

Rebuilds what the qe-7 sandbox (AWS account 264138468394) cost each UTC day,
per person and per test run, and flags what is still costing money with
nothing running. It exists because QE has no Cost Explorer access: the
numbers come from CloudTrail and the live EC2 inventory, priced at on-demand
list prices.

Runs nightly as the Jenkins job `qe7-cost/qe7_daily_cost_report` (see
`Jenkinsfile`) and posts a summary to Slack.

## What it counts

| Item | Source |
|---|---|
| EC2 instance-hours | CloudTrail `RunInstances`, `CreateFleet`, `TerminateInstances` for instances tagged `stack-env=qe-7`, plus the live inventory |
| Node data disks | Real size/IOPS when seen live, else the size the test asked for (Jenkins log), else 1 TB at 16,000 IOPS |
| Root and fusion guest volumes | 50 GB root per instance; guest volume 16,000 IOPS for ~10 min, then baseline for ~3 h |
| Sandbox baseline | $85/day (control plane, network), measured on idle days |
| Unattached volumes | Live inventory at report time |

Not counted: S3, data transfer, NAT, EBS snapshots. Apptio's amortized
figures include Savings Plan discounts, so on quiet days this report sits a
little above Apptio. Instances are attributed by the Capella tenant-owner tag
and, through the cluster id or `taf-dl-*` fleet name, to the Jenkins build
whose console created them.

## Alerts (report only, nothing is deleted)

* the day cost more than `--spend-limit` (default 500 USD);
* a cluster's nodes have been up for `--idle-hours` (default 24), none of
  them launched or terminated in the last 12 h (`--quiet-hours`), and the
  build that created the cluster is not running. Fusion accelerators are
  left out of this check, so they neither hide nor wake up their cluster;
* a `taf-dl-*` doc-loader fleet has been up 6 h with no running build;
* qe-7 volumes unattached for more than 6 h. Control-plane volumes
  (`stack-service=cp`) are part of the baseline and are skipped.

Known limits of the idle check:

* a cluster is tied to the build whose console printed `Cluster created
  with cluster ID`. A later, still-running build that reuses the cluster
  without creating it is not known, so the cluster can be flagged while in
  use;
* a cluster created by hand (no Jenkins build) is flagged once it is older
  than `--idle-hours`, even if someone is still using it.

## Running it by hand

Base credentials from a profile (the `jenkins-cp-cli` IAM user), report a day:

    python3 -m scripts.qe7_cost.run_daily --base-profile cbc-main-jenkins \
        --date 2026-11-22 --state-dir /tmp/qe7/state --out-dir /tmp/qe7/out

Outputs in `--out-dir`: `<day>.json` (per-instance rows and summary),
`report.html`, `slack.txt`, `alerts.json`. State kept in `--state-dir`
between runs: `ledger.json`, `jenkins_map.json`, `prices.json`,
`history.json`.

Events are downloaded from `--lookback-days` (default 1) before the report
day up to now; instances that started earlier come from `ledger.json`. A
backfill of an old day (`--date` more than a day ago) only downloads up to the
day after it. Only complete CloudTrail slices that ended at least 20 minutes
ago are cached as done; the last, partial slice is fetched again next run.

A busy day in us-east-1 means tens of thousands of CloudTrail events, which
the LookupEvents rate limit (about 2 calls/s) turns into 20-60 minutes, so
a nightly run with no cached events takes up to about 1-2 h.

The role and external id default to qe-7's `jenkins-cp-cli` role; set
`QE7_COST_ROLE_ARN` and `QE7_COST_EXTERNAL_ID` to use another account.
