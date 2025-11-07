"""
CloudTrail setup to log only S3 object deletions from a source bucket
into a target bucket/prefix, equivalent to fusion_s3_delete_check.sh.

Usage (CLI):
    python -m pytests.aGoodDoctor.fusion.awslib.cloudtrail_delete_setup \
      --source-bucket fusion-logstore-data \
      --target-bucket fusion-accesslogs \
      --trail-name fusion-delete-tracking \
      --log-prefix cloudtrail-deletes \
      --region us-east-1

Note: CloudTrail delivery may take 5–15 minutes to appear in S3.
"""
from __future__ import annotations

import json
import logging
from typing import Optional, Dict, Any
import boto3
from botocore.exceptions import ClientError
from pytests.aGoodDoctor.fusion.awslib.ec2_lib import AWSBase

logger = logging.getLogger("cloudtrail_delete_setup")
logging.basicConfig(level=logging.INFO, format="%(message)s")

class CloudTrailSetup(AWSBase):
    def __init__(self, aws_access_key: Optional[str] = None, aws_secret_key: Optional[str] = None, session: Optional[boto3.Session] = None,
                 region=None, endpoint_url=None):

        super(CloudTrailSetup, self).__init__(aws_access_key, aws_secret_key, session, endpoint_url)
        self.region = region or 'us-east-1'
        self.sts = self.aws_session.client("sts", region_name="us-east-1")
        self.account_id = self.sts.get_caller_identity()["Account"]
        self.s3 = self.aws_session.client("s3", region_name=self.region)
        self.cloudtrail = self.aws_session.client("cloudtrail", region_name=self.region)

    def setup_cloudtrail_delete_obj_s3_logging(
            self,
            source_bucket: str,
            target_bucket: str,
            trail_name: str = "fusion-delete-tracking",
            log_prefix: str = "s3-object-deletes",
            region: Optional[str] = None) -> Dict[str, Any]:
        """
        Configure CloudTrail to log only S3 object deletions from `source_bucket`
        and write logs to `target_bucket/log_prefix`.

        Steps:
        1) Apply required S3 bucket policy on target bucket for CloudTrail write
        2) Create or reuse a CloudTrail trail
        3) Put advanced event selectors (delete-only for given source bucket)
        4) Start logging
        """

        logger.info("--- CloudTrail Delete Logging Setup ---")
        logger.info(f"Source Bucket: {source_bucket}")
        logger.info(f"Target Bucket: {target_bucket}")
        logger.info(f"Trail Name:    {trail_name}")
        logger.info(f"Account ID:    {self.account_id}")
        logger.info(f"Region:        {self.region}")
        logger.info("---------------------------------------")

        # 1) Apply bucket policy to allow CloudTrail to write
        logger.info("[1/4] Applying permission policy to Target Bucket...")
        policy_doc = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AWSCloudTrailAclCheck",
                    "Effect": "Allow",
                    "Principal": {"Service": "cloudtrail.amazonaws.com"},
                    "Action": "s3:GetBucketAcl",
                    "Resource": f"arn:aws:s3:::{target_bucket}"
                },
                {
                    "Sid": "AWSCloudTrailWrite",
                    "Effect": "Allow",
                    "Principal": {"Service": "cloudtrail.amazonaws.com"},
                    "Action": "s3:PutObject",
                    "Resource": f"arn:aws:s3:::{target_bucket}/{log_prefix}/AWSLogs/{self.account_id}/*",
                    "Condition": {
                        "StringEquals": {
                            "s3:x-amz-acl": "bucket-owner-full-control"
                        }
                    }
                }
            ],
        }
        try:
            self.s3.put_bucket_policy(Bucket=target_bucket, Policy=json.dumps(policy_doc))
            logger.info("      > Policy applied successfully.")
        except ClientError as e:
            logger.error(f"      > Error applying policy: {e}")
            raise

        # 2) Create or reuse trail
        logger.info("[2/4] Creating/Updating CloudTrail...")
        trail_exists = False
        try:
            resp = self.cloudtrail.describe_trails(trailNameList=[trail_name], includeShadowTrails=False)
            trail_exists = bool(resp.get("trailList"))
        except ClientError:
            trail_exists = False

        if trail_exists:
            logger.info(f"      > Trail '{trail_name}' already exists. Skipping creation.")
        else:
            try:
                self.cloudtrail.create_trail(
                    Name=trail_name,
                    S3BucketName=target_bucket,
                    S3KeyPrefix=log_prefix,
                    IncludeGlobalServiceEvents=True,
                )
                logger.info("      > Trail created.")
            except ClientError as e:
                logger.error(f"      > Error creating trail: {e}")
                raise

        # 3) Configure delete-only advanced event selectors
        logger.info("[3/4] Configuring filters to log ONLY object deletions...")
        advanced_event_selectors = [
            {
                "Name": "LogS3DeletesOnly",
                "FieldSelectors": [
                    {"Field": "eventCategory", "Equals": ["Data"]},
                    {"Field": "resources.type", "Equals": ["AWS::S3::Object"]},
                    {"Field": "resources.ARN", "StartsWith": [f"arn:aws:s3:::{source_bucket}/"]},
                    {"Field": "eventName", "Equals": ["DeleteObject", "DeleteObjects"]},
                ],
            }
        ]
        try:
            self.cloudtrail.put_event_selectors(
                TrailName=trail_name,
                AdvancedEventSelectors=advanced_event_selectors,
            )
        except ClientError as e:
            logger.error(f"      > Error setting event selectors: {e}")
            raise

        # 4) Start logging
        logger.info("[4/4] Starting the trail...")
        try:
            self.cloudtrail.start_logging(Name=trail_name)
        except ClientError as e:
            logger.error(f"      > Error starting logging: {e}")
            raise

        log_path = f"s3://{target_bucket}/{log_prefix}/AWSLogs/{self.account_id}/CloudTrail/{self.region}/"
        logger.info("---------------------------------------")
        logger.info("Done! Deletions will be logged to:")
        logger.info(log_path)
        logger.info("Note: It takes about 5–15 minutes for logs to appear.")

        return {
            "trail_name": trail_name,
            "target_bucket": target_bucket,
            "log_prefix": log_prefix,
            "account_id": self.account_id,
            "region": region,
            "log_path": log_path,
        }


    def teardown_cloudtrail_delete_logging(
            self,
            trail_name: str,
            target_bucket: Optional[str] = None,
            log_prefix: Optional[str] = None,
            *,
            remove_bucket_policy: bool = False) -> Dict[str, Any]:
        """
        Stop logging and delete the CloudTrail trail. Optionally remove the
        CloudTrail write ACL statements from the target bucket policy.

        Parameters
        - trail_name: Name of the CloudTrail trail to delete
        - target_bucket: Target S3 bucket (required if remove_bucket_policy=True)
        - log_prefix: Log prefix (required if remove_bucket_policy=True)
        - remove_bucket_policy: When True, attempts to remove the two statements
        added (AWSCloudTrailAclCheck, AWSCloudTrailWrite). If the policy becomes
        empty, deletes the bucket policy entirely.

        Returns: dict summary of actions performed.
        """

        info: Dict[str, Any] = {"trail_name": trail_name}

        # Stop logging (best-effort)
        try:
            self.cloudtrail.stop_logging(Name=trail_name)
            info["stopped_logging"] = True
        except ClientError as e:
            logger.debug(f"stop_logging error: {e}")
            info["stopped_logging"] = False

        # Delete trail
        try:
            self.cloudtrail.delete_trail(Name=trail_name)
            info["trail_deleted"] = True
        except ClientError as e:
            logger.error(f"delete_trail error: {e}")
            info["trail_deleted"] = False

        # Optionally tidy bucket policy
        if remove_bucket_policy:
            if not target_bucket or not log_prefix:
                raise ValueError("target_bucket and log_prefix are required when remove_bucket_policy=True")

            info["bucket"] = target_bucket
            info["log_prefix"] = log_prefix
            try:
                pol_resp = self.s3.get_bucket_policy(Bucket=target_bucket)
                policy_str = pol_resp.get("Policy", "{}")
                policy_doc = json.loads(policy_str)
            except ClientError as e:
                logger.debug(f"No existing bucket policy or error fetching policy: {e}")
                policy_doc = {}

            changed = False
            if policy_doc and "Statement" in policy_doc:
                statements = policy_doc.get("Statement", [])
                new_statements = []
                for st in statements:
                    sid = st.get("Sid")
                    # Match ACL check and write statements we added
                    if sid in ("AWSCloudTrailAclCheck", "AWSCloudTrailWrite"):
                        # For write, verify it targets our path to avoid removing user content
                        if sid == "AWSCloudTrailWrite":
                            res = st.get("Resource")
                            expected = f"arn:aws:s3:::{target_bucket}/{log_prefix}/AWSLogs/{self.account_id}/*"
                            if isinstance(res, list):
                                if expected not in res:
                                    new_statements.append(st)
                                    continue
                            elif isinstance(res, str):
                                if res != expected:
                                    new_statements.append(st)
                                    continue
                        # omit this statement (we remove it)
                        changed = True
                        continue
                    new_statements.append(st)

                if changed:
                    if new_statements:
                        policy_doc["Statement"] = new_statements
                        try:
                            self.s3.put_bucket_policy(Bucket=target_bucket, Policy=json.dumps(policy_doc))
                            info["bucket_policy_updated"] = True
                        except ClientError as e:
                            logger.error(f"Failed to update bucket policy: {e}")
                            info["bucket_policy_updated"] = False
                    else:
                        # No statements left; delete policy entirely
                        try:
                            self.s3.delete_bucket_policy(Bucket=target_bucket)
                            info["bucket_policy_deleted"] = True
                        except ClientError as e:
                            logger.error(f"Failed to delete bucket policy: {e}")
                            info["bucket_policy_deleted"] = False
                else:
                    info["bucket_policy_updated"] = False

        return info
