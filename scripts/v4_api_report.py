#!/usr/bin/env python3
"""
Summarise a V4 API run from testrunner's JUnit XML (logs/testrunner-*/
report-*.xml) and sort every failure into a category, so a red run says
at a glance whether the environment, the infrastructure, the test code or
the product/expectations broke.

Categories (first match wins, on the failure text):
  NOT_RUN           skipped in seconds because the run's shared setup had
                    already failed (root cause = the first failure)
  ENV_QUOTA         org-wide limits: API keys, projects, rate-limit quota
  ENV_PROVISIONING  cluster / instance / project / app service never ready
  INFRA_NETWORK     no HTTP response after retries (capella_http), resets
  INFRA_TIMEOUT     test stopped by testrunner's timeout
  TEST_CODE         a crash in test code (KeyError, TypeError, import ...)
  ASSERTION         the test ran and an API response didn't match what it
                    expected: a product bug or a stale expectation; the
                    test log has one "[validate]" line per case to decide
  OTHER             anything else

Usage: v4_api_report.py --logs logs [--since FILE] [--json out.json]
                        [--desc-prefix "<version> - <desc> - <os>"]
"""
import argparse
import glob
import json
import os
import re
import sys
import xml.etree.ElementTree as ET

CATEGORIES = [
    # Not executed: an earlier test lost the run's shared setup (keys,
    # WRAPPER project, shared cluster); the root cause is that test's
    # failure, listed under its own category.
    ("NOT_RUN", [r"NOT RUN - shared setup failed"]),
    ("ENV_QUOTA", [r"limit has been reached", r"Org API key limit",
                   r"quota", r"maximum number of projects"]),
    ("ENV_PROVISIONING", [r"[Cc]luster creation [Ff]ailed",
                          r"deploymentFailed", r"did not deploy",
                          r"didn't stabilize", r"[Ii]nstance creation [Ff]ailed",
                          r"[Pp]roject creation failed",
                          r"[Cc]ouldn't decipher result",
                          r"not healthy after", r"didn't deploy within",
                          r"[Dd]eployment [Ff]ailed",
                          r"[Ii]nstance deployment failed"]),
    ("INFRA_NETWORK", [r"could not log in", r"no response after",
                       r"CapellaRequestError", r"connection error",
                       r"Max retries exceeded", r"Read timed out",
                       r"Connection (reset|refused|aborted)",
                       r"BindException", r"CbcAPIError"]),
    ("INFRA_TIMEOUT", [r"Test was stopped due to timeout"]),
    ("TEST_CODE", [r"\b(KeyError|TypeError|NameError|IndexError|"
                   r"AttributeError|ImportError|SyntaxError|ValueError|"
                   r"UnboundLocalError|ZeroDivisionError)\b",
                   r"was not found:"]),
    ("ASSERTION", [r"AssertionError", r"tests? FAILED out of",
                   r"FAIL[:.]", r"[Ff]ailure"]),
]


def categorise(text):
    for name, patterns in CATEGORIES:
        for pattern in patterns:
            if re.search(pattern, text):
                return name
    return "OTHER"


def short_name(name):
    base = name.split(",")[0]
    return base.replace("Capella.RestAPIv4.", "")


def last_line(text):
    # testrunner stores str([traceback]) - a list repr with literal "\n".
    text = text.strip()
    if text.startswith("[") and "\\n" in text:
        text = text.strip("[]").strip("'\"").replace("\\n", "\n") \
            .replace("\\'", "'").replace('\\"', '"')
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    return lines[-1][:300] if lines else ""


def collect(logs_dir, since=None):
    since_ts = os.path.getmtime(since) if since and os.path.exists(since) \
        else None
    cases = {}
    for path in sorted(glob.glob(os.path.join(logs_dir, "testrunner-*",
                                              "report-*.xml"))):
        if since_ts is not None and os.path.getmtime(path) < since_ts:
            continue
        try:
            root = ET.parse(path).getroot()
        except ET.ParseError as e:
            sys.stderr.write("skipping unreadable {}: {}\n".format(path, e))
            continue
        for tc in root.iter("testcase"):
            name = tc.get("name", "")
            err = tc.find("error")
            if err is None:
                err = tc.find("failure")
            if tc.find("skipped") is not None:
                status, text = "skip", ""
            elif err is not None:
                status, text = "fail", (err.text or err.get("message") or "")
            else:
                status, text = "pass", ""
            # Reports are rewritten after each test; keep the latest entry.
            cases[name] = {"name": name, "status": status, "text": text,
                           "time": float(tc.get("time") or 0)}
    return list(cases.values())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--logs", default="logs")
    parser.add_argument("--since")
    parser.add_argument("--json")
    parser.add_argument("--desc-prefix", default="")
    args = parser.parse_args()

    cases = collect(args.logs, args.since)
    total = len([c for c in cases if c["status"] != "skip"])
    failed = [c for c in cases if c["status"] == "fail"]
    by_cat = {}
    for c in failed:
        c["reason"] = last_line(c["text"])
        # The exception line decides; the full traceback is only a fallback
        # (function names in it, e.g. ..._with_quota_retry, mislead).
        c["category"] = categorise(c["reason"])
        if c["category"] == "OTHER":
            c["category"] = categorise(c["text"])
        by_cat.setdefault(c["category"], []).append(c)

    print("=" * 78)
    print("V4 API RUN SUMMARY: {} passed / {} run, {} failed".format(
        total - len(failed), total, len(failed)))
    print("=" * 78)
    order = [name for name, _ in CATEGORIES] + ["OTHER"]
    for cat in order:
        items = by_cat.get(cat, [])
        if not items:
            continue
        print("\n{} ({})".format(cat, len(items)))
        for c in sorted(items, key=lambda x: x["name"]):
            print("  - {}\n      {}".format(short_name(c["name"]),
                                          c["reason"]))
    if not cases:
        print("No test results found under {}".format(args.logs))
    # Jenkins description line, same format as executor_script_cloud.sh.
    print("\nDesc1: {} ({}/{})".format(args.desc_prefix,
                                       total - len(failed), total))

    if args.json:
        with open(args.json, "w") as f:
            json.dump({"total": total, "failed": len(failed),
                       "by_category": {k: [short_name(c["name"])
                                           for c in v]
                                       for k, v in by_cat.items()},
                       "failures": [{"test": short_name(c["name"]),
                                     "category": c["category"],
                                     "reason": c["reason"]}
                                    for c in failed]},
                      f, indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
