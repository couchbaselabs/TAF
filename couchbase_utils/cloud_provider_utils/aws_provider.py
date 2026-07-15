import os
from urllib.parse import urlparse

import boto3

from couchbase_utils.cloud_provider_utils.cloud_provider_interface import \
    CloudProviderInterface
from couchbase_utils.security_utils.credential_store_utils import \
    CredentialStoreUtils


class AWSProvider(CloudProviderInterface):
    def __init__(self):
        self.aws_region = os.getenv("AWS_REGION", "us-east-1")
        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    def get_cbbackupmgr_flags(self, shell=None):
        return (
            "--obj-region {0} --obj-access-key-id {1} "
            "--obj-secret-access-key {2}"
        ).format(self.aws_region, self.aws_access_key_id,
                 self.aws_secret_access_key)

    def get_cbconbk_flags(self, shell=None):
        return self.get_cbbackupmgr_flags(shell)

    def cleanup_for_bkrs(self, s3_path):
        """
        Deletes the directory under the given S3 bucket so it no longer
        exists.

        :param s3_path: e.g. s3://bucket-name/some-dir
        """
        parsed = urlparse(s3_path)
        bucket_name = parsed.netloc
        prefix = "{0}/".format(parsed.path.strip("/"))

        s3_resource = boto3.resource(
            "s3",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.aws_region)
        s3_resource.Bucket(bucket_name).objects.filter(
            Prefix=prefix).delete()

    def create_credential_store(self, rest, cred_id, username=None,
                                 password=None, description=None,
                                 allowed_services=None, expires_at_ms=None):
        """
        Build an 'aws' Credential Store payload and create it via
        POST /settings/credentials/:id.

        Returns:
            tuple: (status_code, content)
        """
        cs_utils = CredentialStoreUtils()
        payload = cs_utils.build_aws_payload(
            self.aws_access_key_id, self.aws_secret_access_key,
            self.aws_region, description=description,
            allowed_services=allowed_services,
            expires_at_ms=expires_at_ms)
        return cs_utils.create_credential(
            rest, cred_id, payload, username=username, password=password)
