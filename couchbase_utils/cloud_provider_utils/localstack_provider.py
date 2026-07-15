import os
from urllib.parse import urlparse

import boto3

from couchbase_utils.cloud_provider_utils.cloud_provider_interface import \
    CloudProviderInterface
from couchbase_utils.security_utils.credential_store_utils import \
    CredentialStoreUtils


class LocalstackProvider(CloudProviderInterface):
    def __init__(self):
        self.localstack_region = os.getenv("LOCALSTACK_REGION", "us-east-1")
        self.localstack_access_key_id = os.getenv(
            "LOCALSTACK_ACCESS_KEY_ID")
        self.localstack_secret_access_key = os.getenv(
            "LOCALSTACK_SECRET_ACCESS_KEY")
        self.localstack_endpoint = os.getenv("LOCALSTACK_ENDPOINT")

    def get_cbbackupmgr_flags(self, shell=None):
        return (
            "--obj-region {0} --obj-access-key-id {1} "
            "--obj-secret-access-key {2} --obj-endpoint {3} "
            "--s3-force-path-style"
        ).format(self.localstack_region, self.localstack_access_key_id,
                 self.localstack_secret_access_key,
                 self.localstack_endpoint)

    def get_cbconbk_flags(self, shell=None):
        return self.get_cbbackupmgr_flags(shell)

    def cleanup_for_bkrs(self, s3_path):
        """
        Not a true "delete this one directory" cleanup - Localstack buckets
        are cheap/local, so instead: if the bucket does not exist yet, it is
        created (with the sub-dir placeholder if one was given); if the
        bucket already exists, the whole bucket is deleted so the next run
        starts from a clean slate.

        :param s3_path: e.g. s3://bucket-name/some-dir
        """
        parsed = urlparse(s3_path)
        bucket_name = parsed.netloc
        folder_path = parsed.path.strip("/")

        s3_resource = boto3.resource(
            "s3",
            aws_access_key_id=self.localstack_access_key_id,
            aws_secret_access_key=self.localstack_secret_access_key,
            region_name=self.localstack_region,
            endpoint_url=self.localstack_endpoint)

        existing_buckets = [b.name for b in s3_resource.buckets.all()]
        bucket = s3_resource.Bucket(bucket_name)
        if bucket_name not in existing_buckets:
            create_kwargs = {}
            if self.localstack_region and self.localstack_region != "us-east-1":
                create_kwargs["CreateBucketConfiguration"] = {
                    "LocationConstraint": self.localstack_region}
            bucket.create(**create_kwargs)
            if folder_path:
                bucket.put_object(Key="{0}/".format(folder_path))
        else:
            bucket.objects.all().delete()
            bucket.delete()

    def create_credential_store(self, rest, cred_id, username=None,
                                 password=None, description=None,
                                 allowed_services=None, expires_at_ms=None):
        """
        Build an 'aws' Credential Store payload (Localstack is
        S3-compatible, so it reuses the 'aws' credential type with its
        endpoint set) and create it via POST /settings/credentials/:id.

        Returns:
            tuple: (status_code, content)
        """
        cs_utils = CredentialStoreUtils()
        payload = cs_utils.build_aws_payload(
            self.localstack_access_key_id,
            self.localstack_secret_access_key,
            self.localstack_region,
            endpoint=self.localstack_endpoint,
            description=description, allowed_services=allowed_services,
            expires_at_ms=expires_at_ms)
        return cs_utils.create_credential(
            rest, cred_id, payload, username=username, password=password)
