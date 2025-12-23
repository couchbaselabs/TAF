"""
Created on 30-Oct-2025

@author: himanshu.jain@couchbase.com
"""

from Columnar.columnar_base import ColumnarBaseTest
import json
from common_lib import sleep
import logging
import time
import boto3
from botocore.exceptions import ClientError
from global_vars import logger
from capellaAPI.capella.columnar.CapellaAPI import CapellaAPI as ColumnarAPI
from TestInput import TestInputSingleton
import uuid
from queue import Queue

# Disable debug logging for boto3 and botocore
logging.getLogger('boto3').setLevel(logging.ERROR)
logging.getLogger('botocore').setLevel(logging.ERROR)


class CrossAccountSetup:
    """
    Create IAM role with trust policy
    Create IAM policy for S3 access
    Attach policy to role
    """

    def __init__(self, columnar_api=None, tenant_id=None, project_id=None, instance_id=None, columnar_cluster_id=None, s3_bucket=None):
        self.log = logger.get("test")
        self.input = TestInputSingleton.input

        self.s3_bucket = s3_bucket
        self.aws_region = self.input.param("aws_region", "us-west-1")
        self.aws_access_key_id = self.input.param("aws_access_key", None)
        self.aws_secret_access_key = self.input.param("aws_secret_key", None)
        self.user_role_name = f"columnar-regression-cross-account-auth-{str(int(time.time()))}"
        self.external_id = str(uuid.uuid4())

        # Store columnar API and IDs for trustauth template retrieval
        self.columnar_api = columnar_api
        self.tenant_id = tenant_id
        self.project_id = project_id
        self.instance_id = instance_id
        self.columnar_cluster_id = columnar_cluster_id

        self.iam = boto3.client(
            "iam",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name="us-east-1"
        )
        self.sts = boto3.client(
            "sts",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name="us-east-1"
        )

        self.iam_permission_policy = self.get_iam_permission_policy()
        self.trust_policy = self.get_iam_trust_policy()

        self.iam_role_arn = None
        self.iam_policy_arn = None

    def get_trustauth_template(self, columnar_api, tenant_id, project_id, instance_id):
        response = columnar_api.get_trustauth_template(
            tenant_id, project_id, instance_id)
        if response.status_code == 200:
            return response.json()
        else:
            self.log.error(
                f"Failed to fetch trustauth template: {response.status_code} - {response.content}")
            raise Exception(
                f"Failed to fetch trustauth template: {response.status_code}")

    def get_iam_trust_policy(self):
        trustauth_template = self.get_trustauth_template(
            self.columnar_api, self.tenant_id, self.project_id, self.instance_id)
        policy = trustauth_template[0]
        policy['Statement'][0]['Condition']['StringEquals']['sts:ExternalId'] = self.external_id
        return policy

    def get_iam_permission_policy(self):
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "ListS3",
                    "Effect": "Allow",
                    "Action": "s3:ListBucket",
                    "Resource": [
                        f"arn:aws:s3:::{self.s3_bucket}",
                        f"arn:aws:s3:::{self.s3_bucket}/*"
                    ]
                },
                {
                    "Sid": "ReadS3",
                    "Effect": "Allow",
                    "Action": "s3:GetObject",
                    "Resource": [
                        f"arn:aws:s3:::{self.s3_bucket}/*"
                    ]
                },
                {
                    "Sid": "PutS3",
                    "Effect": "Allow",
                    "Action": "s3:PutObject",
                    "Resource": [
                        f"arn:aws:s3:::{self.s3_bucket}/*"
                    ]
                },
                {
                    "Sid": "DeleteS3",
                    "Effect": "Allow",
                    "Action": "s3:DeleteObject",
                    "Resource": [
                        f"arn:aws:s3:::{self.s3_bucket}/*"
                    ]
                }
            ]
        }
        return policy

    def create_iam_permission_policy(self):
        policy_name = self.user_role_name
        try:
            resp = self.iam.create_policy(
                PolicyName=policy_name,
                PolicyDocument=json.dumps(self.iam_permission_policy),
                Description="IAM permission policy for S3 cross account"
            )
            self.iam_policy_arn = resp['Policy']['Arn']
            self.log.info(f"Created IAM policy: {resp['Policy']['Arn']}")
        except ClientError as e:
            # Policy already exists, retrieve its ARN
            if e.response["Error"]["Code"] == "EntityAlreadyExists":
                self.log.info(
                    f"IAM policy already exists: {e.response['Error']['Message']}")
                # Get account ID to construct ARN
                account_id = self.sts.get_caller_identity()['Account']
                self.iam_policy_arn = f"arn:aws:iam::{account_id}:policy/{policy_name}"
                self.log.info(
                    f"Retrieved existing IAM policy ARN: {self.iam_policy_arn}")
            else:
                raise

    def create_iam_role(self):
        try:
            resp = self.iam.create_role(
                RoleName=self.user_role_name,
                AssumeRolePolicyDocument=json.dumps(self.trust_policy),
                Description="IAM role for S3 cross account"
            )
            self.iam_role_arn = resp['Role']['Arn']
            self.log.info(f"Created IAM role: {self.iam_role_arn}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityAlreadyExists":
                # Role already exists, retrieve its ARN
                self.log.info(
                    f"IAM role already exists: {e.response['Error']['Message']}")
                # Get the role to retrieve its ARN
                resp = self.iam.get_role(RoleName=self.user_role_name)
                self.iam_role_arn = resp['Role']['Arn']
                self.log.info(
                    f"Retrieved existing IAM role ARN: {self.iam_role_arn}")
            else:
                raise

    def attach_iam_policy_to_role(self):
        try:
            self.iam.attach_role_policy(
                RoleName=self.user_role_name,
                PolicyArn=self.iam_policy_arn
            )
            self.log.info(f"Attached IAM policy to role")
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityAlreadyExists":
                self.log.info(f"IAM policy already attached to role")
                pass
            else:
                raise

    def upgrade_dp_agent_hash(self, hash):
        # update dp-agent hash
        self.log.info(f"Updating dp-agent hash to {hash}")
        response = self.columnar_api.update_dp_agent(
            self.columnar_cluster_id, hash)
        if response.status_code != 200:
            self.log.error(
                f"Failed to update dp-agent hash: {response.status_code} - {response.content}")
            raise Exception(
                f"Failed to update dp-agent hash: {response.status_code}")

        sleep(180)

        # Get dp-agent hash
        response = self.columnar_api.get_dp_agent_hash(
            self.columnar_cluster_id)
        if response.status_code == 200:
            resp = response.json()
            if resp['desiredHashes']['dp-agent'] == hash:
                for node in resp['nodes']:
                    if node['agentHashes']['dp-agent'] != hash:
                        self.log.error(
                            f"dp-agent hash mismatch on node: {node['hostname']} - {node['agentHashes']['dp-agent']}")
                        raise Exception(
                            f"dp-agent hash mismatch on node: {node['hostname']} - {node['agentHashes']['dp-agent']}")
                self.log.info(f"dp-agent hash updated successfully")
        else:
            self.log.error(
                f"Failed to get dp-agent hash: {response.status_code} - {response.content}")
            raise Exception(
                f"Failed to get dp-agent hash: {response.status_code}")

    def create_cross_account(self):
        # upgrade dp-agent hash (temp) - remove after feature branch is merged
        # self.upgrade_dp_agent_hash("10346eef")

        self.create_iam_permission_policy()
        self.create_iam_role()
        self.attach_iam_policy_to_role()
        return self.iam_role_arn

    def destroy_cross_account(self):
        policy_name = self.user_role_name

        # Retrieve ARNs if not already set
        if self.iam_policy_arn is None:
            try:
                account_id = self.sts.get_caller_identity()['Account']
                self.iam_policy_arn = f"arn:aws:iam::{account_id}:policy/{policy_name}"
                self.log.info(
                    f"Retrieved IAM policy ARN for cleanup: {self.iam_policy_arn}")
            except Exception as e:
                self.log.warning(f"Could not retrieve policy ARN: {e}")
                # If we can't get the ARN, we can't clean up
                return

        if self.iam_role_arn is None:
            try:
                resp = self.iam.get_role(RoleName=self.user_role_name)
                self.iam_role_arn = resp['Role']['Arn']
                self.log.info(
                    f"Retrieved IAM role ARN for cleanup: {self.iam_role_arn}")
            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchEntity":
                    self.log.info(
                        "Role does not exist, continuing with policy cleanup")
                else:
                    self.log.warning(f"Could not retrieve role ARN: {e}")

        # Detach policy from role (if role exists)
        if self.iam_policy_arn:
            try:
                self.iam.detach_role_policy(
                    RoleName=self.user_role_name,
                    PolicyArn=self.iam_policy_arn
                )
                self.log.info("Detached IAM policy from role")
            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchEntity":
                    self.log.info(
                        "Policy not attached to role, skipping detach")
                else:
                    self.log.warning(f"Could not detach policy from role: {e}")

        # Delete policy
        if self.iam_policy_arn:
            try:
                self.iam.delete_policy(PolicyArn=self.iam_policy_arn)
                self.log.info("Deleted IAM policy")
            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchEntity":
                    self.log.info("Policy does not exist, skipping deletion")
                else:
                    self.log.warning(f"Could not delete policy: {e}")

        # Delete role
        try:
            self.iam.delete_role(RoleName=self.user_role_name)
            self.log.info("Deleted IAM role")
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchEntity":
                self.log.info("Role does not exist, skipping deletion")
            else:
                self.log.warning(f"Could not delete role: {e}")


class S3CrossAccountAuth(ColumnarBaseTest):
    def __init__(self, methodName: str = "runTest"):
        super().__init__(methodName)

    def setUp(self):
        super(S3CrossAccountAuth, self).setUp()

        self.columnar_cluster = self.tenant.columnar_instances[0]
        if not self.columnar_spec_name:
            self.columnar_spec_name = "full_template"

        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            external_collection_file_formats=["json"])

        self.columnar_api = ColumnarAPI(self.pod.url_public, self.tenant.api_secret_key, self.tenant.api_access_key,
                                        self.tenant.user, self.tenant.pwd, TOKEN_FOR_INTERNAL_SUPPORT=self.pod.TOKEN)
        self.cross_account_setup = CrossAccountSetup(self.columnar_api, self.tenant.id, self.tenant.project_id,
                                                     self.columnar_cluster.instance_id, self.columnar_cluster.cluster_id, self.s3_source_bucket)
        self.role_arn = self.cross_account_setup.create_cross_account()
        self.log.info(f"Created role ARN: {self.role_arn}")
        sleep(60)

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)

        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.columnar_cluster, self.columnar_spec):
            self.fail("Error while deleting cbas entities")

        # destroy cross account
        if self.cross_account_setup:
            self.cross_account_setup.destroy_cross_account()
            self.log.info(f"Cross account destroyed")

        super(S3CrossAccountAuth, self).tearDown()

        self.log_setup_status(self.__class__.__name__,
                              "Finished", stage="Teardown")

    """
    Create S3 link using cross account auth
    """

    def test_create_s3_link_using_cross_account_auth(self):
        self.log.info("Creating S3 link")
        self.columnar_spec["external_link"]["properties"] = [{
            "type": "s3",
            "region": self.aws_region,
            "instanceProfile": "true",
            "roleArn": self.role_arn,
            "externalId": self.cross_account_setup.external_id
        }]

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.columnar_cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(f"Failed to create CBAS infrastructure: {msg}")

        # Get the created datasets
        datasets = self.cbas_util.get_all_dataset_objs("external")
        if not datasets:
            self.fail("No external datasets were created")

        self.log.info("S3 link created successfully")

        # Run query to verify access
        self.log.info("Running verification query")
        dataset = datasets[0]
        query = f"SELECT COUNT(*) as doc_count FROM {dataset.full_name}"

        status, metrics, errors, results, _, _ = \
            self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, query, timeout=300, analytics_timeout=300)

        if status != "success":
            self.fail(f"Query execution failed: {errors}")

        doc_count = results[0]["doc_count"]
        self.log.info(
            f"Successfully queried {doc_count} documents from cross-account S3")

        # Verify we got some data
        if doc_count == 0:
            self.fail("No documents found in cross-account S3 bucket")

        self.log.info("Cross-account S3 access test completed successfully")

    """
    Run long query to test auto-refresh of assumed creds
    """

    def test_long_running_query(self):
        self.columnar_spec["external_link"]["properties"] = [{
            "type": "s3",
            "region": self.aws_region,
            "instanceProfile": "true",
            "roleArn": self.role_arn,
            "externalId": self.cross_account_setup.external_id
        }]
        self.columnar_spec["external_dataset"]["external_dataset_properties"][0]["path_on_external_container"] = "cross_account_auth"

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.columnar_cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)
        datasets = self.cbas_util.get_all_dataset_objs("external")

        # Read all the docs in the aws s3 bucket
        for dataset in datasets:
            # query limit is 25 mins, if duration is more than 25 mins, rerun duration - 25 until 0 min
            duration = self.input.param("duration", 60*60*1000)
            self.log.info(f"Total duration to run: {duration} ms")

            while duration > 0:
                duration_to_run = min(duration, 25*60*1000)
                duration -= duration_to_run
                self.log.info(
                    f"Running query for duration: {duration_to_run} ms ({duration_to_run/1000:.2f} seconds)")

                query = f"SELECT sleep(\"v\", {duration_to_run}) AS k from {dataset.full_name};"

                jobs = Queue()
                results = []
                # Calculate timeout: add buffer of 5 minutes to duration
                timeout_seconds = (duration_to_run // 1000) + 300
                jobs.put((
                    self.cbas_util.execute_statement_on_cbas_util,
                    {"cluster": self.columnar_cluster,
                        "statement": query,
                        "timeout": timeout_seconds,
                        "analytics_timeout": timeout_seconds}))
                self.cbas_util.run_jobs_in_parallel(
                    jobs, results, self.sdk_clients_per_user, async_run=False)

                # Check if query executed successfully
                if results and results[0][0] != "success":
                    self.fail("Query execution failed with error - {}".format(
                        results[0][2]))
                self.log.info("Results: {}".format(results))
                self.log.info(
                    f"Query completed. Remaining duration: {duration} ms")

        self.log.info("All long running queries completed successfully")
