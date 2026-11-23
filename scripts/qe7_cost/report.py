"""Report outputs: JSON for later analysis, a self-contained HTML page for
the Jenkins build, and a short Slack message."""

import html
import json

OWNER_LABELS = {}   # optional pretty names: {"vibhav-sp": "Vibhav"}


def usd(v):
    return "${:,.0f}".format(v)


def owner_label(owner):
    if owner.startswith("qe-7-fusion-") and owner.endswith("-regression"):
        return "fusion regression #" + owner.split("-")[3]
    return OWNER_LABELS.get(owner, owner)


def month_to_date(history, day):
    month = day[:7]
    days = {d: v for d, v in history.items() if d.startswith(month) and d <= day}
    total = sum(v["total"] for v in days.values())
    by_owner = {}
    for v in days.values():
        for o, c in v["by_owner"].items():
            by_owner[o] = by_owner.get(o, 0) + c
    return dict(days=len(days), total=total, by_owner=by_owner)


def slack_text(day, summary, alert_list, mtd, report_url=None, top=5):
    lines = ["*qe-7 cost, %s (UTC): %s*" % (day, usd(summary["total"]))]
    owners = ", ".join("%s %s" % (owner_label(o), usd(c))
                       for o, c in list(summary["by_owner"].items())[:6] if c >= 1)
    if owners:
        lines.append("By owner: " + owners)
    runs = [r for r in summary["runs"] if r["total"] >= 1][:top]
    if runs:
        lines.append("Top runs: " + "; ".join(
            "%s (%s) %s" % (r["run"], owner_label(r["owner"]), usd(r["total"]))
            for r in runs))
    lines.append("Month to date (%d days): %s" % (mtd["days"], usd(mtd["total"])))
    if alert_list:
        lines.append("*Alerts:*")
        lines += ["• " + a["text"] for a in alert_list[:10]]
        if len(alert_list) > 10:
            lines.append("• … and %d more in the report" % (len(alert_list) - 10))
    else:
        lines.append("No alerts: nothing idle, no unattached volumes.")
    lines.append("_List prices from CloudTrail and live inventory; excludes S3, "
                 "data transfer, NAT and snapshots._")
    if report_url:
        lines.append("Report: " + report_url)
    return "\n".join(lines)


def _table(headers, rows, num_cols=()):
    out = ["<table><thead><tr>"]
    out += ["<th%s>%s</th>" % (' class="n"' if i in num_cols else "",
                               html.escape(h)) for i, h in enumerate(headers)]
    out.append("</tr></thead><tbody>")
    for row in rows:
        out.append("<tr>" + "".join(
            "<td%s>%s</td>" % (' class="n"' if i in num_cols else "",
                               html.escape(str(c))) for i, c in enumerate(row))
            + "</tr>")
    out.append("</tbody></table>")
    return "".join(out)


def html_report(day, summary, alert_list, mtd, stats):
    owners = _table(["Owner", "Cost"], [(owner_label(o), usd(c))
                                        for o, c in summary["by_owner"].items()],
                    num_cols=(1,))
    runs = _table(["Run", "Owner", "Nodes", "Accelerators", "Loaders", "of which disks", "Total"],
                  [(r["run"], owner_label(r["owner"]),
                    usd(r["by_role"].get("node", 0)), usd(r["by_role"].get("accel", 0)),
                    usd(r["by_role"].get("loader", 0)), usd(r["ebs"]), usd(r["total"]))
                   for r in summary["runs"] if r["total"] >= 1],
                  num_cols=(2, 3, 4, 5, 6))
    mtd_tbl = _table(["Owner", "Month to date"],
                     [(owner_label(o), usd(c)) for o, c in
                      sorted(mtd["by_owner"].items(), key=lambda x: -x[1])],
                     num_cols=(1,))
    alerts_html = "".join("<li>%s</li>" % html.escape(a["text"])
                          for a in alert_list) or "<li>None</li>"
    return """<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>qe-7 cost {day}</title><style>
body{{font:14px/1.5 -apple-system,Segoe UI,Helvetica,Arial,sans-serif;margin:24px;color:#1d2330;max-width:1100px}}
h1{{font-size:22px;margin:0 0 4px}} h2{{font-size:16px;margin:28px 0 8px}}
.muted{{color:#5b6475}} .big{{font-size:28px;font-weight:600}}
table{{border-collapse:collapse;width:100%}} th,td{{padding:6px 10px;border-bottom:1px solid #e3e6ec;text-align:left}}
th{{color:#5b6475;font-weight:600}} .n{{text-align:right;font-variant-numeric:tabular-nums}}
li{{margin:4px 0}}
</style></head><body>
<h1>qe-7 cost, {day} (UTC)</h1>
<div class="big">{total}</div>
<p class="muted">Includes {baseline} sandbox baseline and {unatt} of unattached volumes.
Month to date ({mtd_days} days): {mtd_total}.</p>
<h2>Alerts</h2><ul>{alerts}</ul>
<h2>By owner</h2>{owners}
<h2>By run</h2>{runs}
<h2>Month to date by owner</h2>{mtd_tbl}
<p class="muted">On-demand list prices. EC2 hours from CloudTrail launch/terminate
events and the live inventory; disks at real or requested size and IOPS.
Not included: S3, data transfer, NAT, EBS snapshots. Apptio's amortized
figures include Savings Plan discounts. {stats}</p>
</body></html>""".format(
        day=day, total=usd(summary["total"]), baseline=usd(summary["baseline"]),
        unatt=usd(summary["unattached_volumes"]), mtd_days=mtd["days"],
        mtd_total=usd(mtd["total"]), alerts=alerts_html, owners=owners,
        runs=runs, mtd_tbl=mtd_tbl, stats=html.escape(json.dumps(stats)))
