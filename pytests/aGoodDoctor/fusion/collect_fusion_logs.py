#!/usr/bin/env python3
"""Manually collect fusion logs for a Capella cluster.

Standalone version of the collection FusionFailureLogsMixin runs when a
fusion cloud test fails, for when you need the logs outside a test run:

  * accelerator logs uploaded to s3://cbc-storage-<cluster>/logs/fusion/<plan_uuid>/
  * logs pulled over SSM from any fusion accelerator still running (same
    content as the S3 upload: journal, stats.json, manifest, disk stats)
  * the dp-agent journal from every Couchbase node, over SSM (the same
    `journalctl -u dp-agent` Fleet Manager's dp-agent-logs endpoint returns)

cbcollect_info is not collected here; it needs Capella credentials. Use the
Capella UI or the internal-support log-collection API for that.

AWS access, first match wins:
  1. --aws-access-key/--aws-secret-key [--session-token]: keys for the
     cluster's account, used as-is.
  2. --role-arn, --account-id or -i <ini> ([capella] account_id): the
     jenkins-cp-cli role (or --role-arn) is assumed, the same way the fusion
     tests do it (resolve_fusion_aws_credentials), with
     AWS_ACCESS_KEY_ID_004/AWS_SECRET_ACCESS_KEY_004 as the source credentials.
  3. None of the above: AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY
     [/AWS_SESSION_TOKEN] from the environment, used as-is.

Usage:
    python pytests/aGoodDoctor/fusion/collect_fusion_logs.py \\
        --cluster-id <cluster_id> --plan-uuid <plan_uuid> --account-id <aws_account_id>

    # several plans, account id from a test ini, custom output dir
    python pytests/aGoodDoctor/fusion/collect_fusion_logs.py -i fusion.ini \\
        --cluster-id <id> --plan-uuid <p1> --plan-uuid <p2> -o /tmp/fusion_logs

    # no --plan-uuid: plans with accelerator logs from the last --since-hours,
    # plus every running accelerator
    python pytests/aGoodDoctor/fusion/collect_fusion_logs.py --cluster-id <id> --account-id <acct>

    # keys for the cluster's account in the standard AWS env vars
    export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... [AWS_SESSION_TOKEN=...]
    python pytests/aGoodDoctor/fusion/collect_fusion_logs.py --cluster-id <id> --plan-uuid <plan>

Output: <out_dir>/<cluster_id>/{collection_summary.log, dp-agent/,
accelerator-s3/<plan_uuid>/, accelerator-live/<plan_uuid>/}
"""
import argparse
import configparser
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

# Make the TAF SDK importable when run standalone (mirrors testrunner.py).
_REPO_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", ".."))
for _p in ("py_constants", "couchbase_utils", "pytests", "lib", "."):
    _full = os.path.join(_REPO_ROOT, _p) if _p != "." else _REPO_ROOT
    if _full not in sys.path:
        sys.path.insert(0, _full)

from aGoodDoctor.fusion.fusion_aws_util import (  # noqa: E402
    FusionAWSUtil, resolve_fusion_aws_credentials)
from aGoodDoctor.fusion.fusion_log_collector import FusionLogCollector  # noqa: E402


class _CliInput:
    """Minimal stand-in for TestInputSingleton.input, as consumed by
    resolve_fusion_aws_credentials (.param() and .capella.get())."""

    def __init__(self, params, capella):
        self._params = params
        self.capella = capella

    def param(self, name, default=None):
        value = self._params.get(name)
        return default if value is None else value


def _read_ini_capella(path):
    parser = configparser.ConfigParser()
    if not parser.read(path):
        raise SystemExit(f"Could not read ini file {path}")
    return dict(parser["capella"]) if parser.has_section("capella") else {}


def _build_aws_util(args, region):
    if args.aws_access_key:
        return FusionAWSUtil(args.aws_access_key, args.aws_secret_key,
                             session_token=args.session_token, region=region)
    if not (args.role_arn or args.account_id or args.ini):
        access_key = os.environ.get("AWS_ACCESS_KEY_ID")
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        if not (access_key and secret_key):
            raise SystemExit(
                "No AWS credentials: pass --aws-access-key/--aws-secret-key, "
                "--account-id, -i <ini> or --role-arn, or set "
                "AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY")
        return FusionAWSUtil(access_key, secret_key,
                             session_token=os.environ.get("AWS_SESSION_TOKEN"),
                             region=region)
    capella = _read_ini_capella(args.ini) if args.ini else {}
    if args.account_id:
        capella["account_id"] = args.account_id
    params = {"aws_assume_role_arn": args.role_arn}
    if args.external_id:
        params["jenkins_cpcli_role_external_id"] = args.external_id
    access_key, secret_key, session_token, iam = resolve_fusion_aws_credentials(
        _CliInput(params, capella), region=region)
    boto3_session = iam.get_boto3_session(region=region) if iam else None
    return FusionAWSUtil(access_key, secret_key, session_token=session_token,
                         region=region, boto3_session=boto3_session)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Collect fusion accelerator and dp-agent logs for a Capella cluster.",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cluster-id", required=True, help="Capella cluster id")
    parser.add_argument("--plan-uuid", action="append", default=[],
                        help="Fusion rebalance plan UUID (repeatable). Limits the S3 "
                             "accelerator logs and the live accelerators to these plans.")
    parser.add_argument("--since-hours", type=float, default=6,
                        help="Without --plan-uuid: plans with accelerator logs uploaded "
                             "in the last N hours (default 6)")
    parser.add_argument("--region", help="AWS region of the cluster (default: detected "
                                         "from the cbc-storage-<cluster> bucket)")
    parser.add_argument("-o", "--out-dir", help="Output directory "
                                                "(default ./fusion_logs/<timestamp>)")

    creds = parser.add_argument_group(
        "AWS credentials (explicit keys, else role assumption, else "
        "AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY/AWS_SESSION_TOKEN env vars)")
    creds.add_argument("-i", "--ini", help="Test .ini; its [capella] account_id is used")
    creds.add_argument("--account-id", help="AWS account id to assume jenkins-cp-cli in")
    creds.add_argument("--role-arn", help="Role to assume instead of jenkins-cp-cli")
    creds.add_argument("--external-id", help="External id for the role assumption")
    creds.add_argument("--aws-access-key", help="Use these keys directly (no role assumption)")
    creds.add_argument("--aws-secret-key")
    creds.add_argument("--session-token", help="Session token for temporary keys (ASIA...)")

    what = parser.add_argument_group("What to collect")
    what.add_argument("--no-dp-agent", action="store_true", help="Skip node dp-agent logs")
    what.add_argument("--no-accelerator-s3", action="store_true",
                      help="Skip accelerator logs from S3")
    what.add_argument("--no-accelerator-live", action="store_true",
                      help="Skip collecting from running accelerators")
    what.add_argument("--days", type=int, default=1,
                      help="dp-agent journalctl --since window in days (default 1)")
    what.add_argument("--units", default="dp-agent",
                      help="':'-separated systemd units on the nodes (default dp-agent)")
    what.add_argument("--staging-bucket", default="default",
                      help="Bucket used to move logs off the instances (default "
                           "cbc-storage-<cluster>; 'none' = inline SSM only, slower "
                           "and truncated to the last 20 MB)")
    args = parser.parse_args(argv)
    if args.session_token and not args.aws_access_key:
        parser.error("--session-token needs --aws-access-key/--aws-secret-key")
    if bool(args.aws_access_key) != bool(args.aws_secret_key):
        parser.error("--aws-access-key and --aws-secret-key go together")
    return args


def main(argv=None):
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s | %(levelname)s | %(message)s")
    log = logging.getLogger("test")

    region = args.region
    if not region:
        probe = _build_aws_util(args, "us-east-1")
        bucket = f"cbc-storage-{args.cluster_id}"
        try:
            location = probe.s3.s3_client.get_bucket_location(Bucket=bucket)
        except Exception as e:
            raise SystemExit(
                f"Could not detect the region from s3://{bucket} ({e}). The "
                f"cluster may have been destroyed, or the credentials are for a "
                f"different account. Pass --region to skip detection.")
        region = location.get("LocationConstraint") or "us-east-1"
        log.info(f"Detected region {region} from {bucket}")
    aws_util = _build_aws_util(args, region)

    out_dir = args.out_dir or os.path.join(
        os.getcwd(), "fusion_logs", datetime.now().strftime("%Y%m%d-%H%M%S"))
    staging = None if args.staging_bucket.lower() in ("none", "false", "") \
        else args.staging_bucket
    since = datetime.now(timezone.utc) - timedelta(hours=args.since_hours)

    summary = FusionLogCollector(log).collect(
        [{"cluster_id": args.cluster_id, "capella_api": None, "aws_util": aws_util}],
        out_dir,
        reason="manual collection",
        cbcollect=False,
        dp_agent=not args.no_dp_agent,
        accelerator_s3=not args.no_accelerator_s3,
        accelerator_live=not args.no_accelerator_live,
        accelerator_plan_uuids=args.plan_uuid or None,
        dp_agent_days=args.days,
        dp_agent_units=[u for u in args.units.split(":") if u],
        s3_bucket=staging,
        since=since)
    print(f"\nLogs written to {os.path.join(out_dir, args.cluster_id)}")
    rows = summary.get(args.cluster_id, [])
    failed = [r for r in rows if str(r[2]).startswith("failed")]
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
