import os
from urllib.parse import urlparse

import boto3

from couchbase_utils.cloud_provider_utils.cloud_provider_interface import \
    CloudOperationError, CloudProviderInterface
from couchbase_utils.security_utils.credential_store_utils import \
    CredentialStoreUtils


class AWSProvider(CloudProviderInterface):
    def __init__(self):
        self.aws_region = os.getenv("AWS_REGION", "us-east-1")
        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self._s3_client = None
        self.validate_credentials()

    def validate_credentials(self):
        if not self.aws_access_key_id or not self.aws_secret_access_key:
            raise CloudOperationError("Incomplete AWS credentials")

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

    def _client(self):
        if self._s3_client is None:
            self._s3_client = boto3.client(
                "s3",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.aws_region)
        return self._s3_client

    def list_objects(self, archive_uri, repo_name, relative_prefix=""):
        location = self._parse_location(archive_uri)
        prefix = self._object_path(location["prefix"], repo_name,
                                   relative_prefix)
        keys = []
        continuation_token = None
        client = self._client()
        while True:
            kwargs = {"Bucket": location["bucket"], "Prefix": prefix}
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token
            response = client.list_objects_v2(**kwargs)
            keys.extend(item["Key"] for item in response.get("Contents", []))
            if not response.get("IsTruncated"):
                return keys
            continuation_token = response.get("NextContinuationToken")

    def object_exists(self, archive_uri, repo_name, relative_path):
        location = self._parse_location(archive_uri)
        key = self._object_path(location["prefix"], repo_name, relative_path)
        try:
            self._client().head_object(Bucket=location["bucket"], Key=key)
            return True
        except Exception:
            return False

    def read_text(self, archive_uri, repo_name, relative_path):
        location = self._parse_location(archive_uri)
        key = self._object_path(location["prefix"], repo_name, relative_path)
        response = self._client().get_object(Bucket=location["bucket"], Key=key)
        return response["Body"].read().decode("utf-8")

    def get_retention_until(self, archive_uri, repo_name, relative_path):
        location = self._parse_location(archive_uri)
        key = self._object_path(location["prefix"], repo_name, relative_path)
        client = self._client()
        try:
            response = client.get_object_retention(
                Bucket=location["bucket"], Key=key)
            timestamp = self._to_timestamp(
                response.get("Retention", {}).get("RetainUntilDate"))
            if timestamp is not None:
                return timestamp
        except Exception:
            pass
        response = client.head_object(Bucket=location["bucket"], Key=key)
        return self._to_timestamp(response.get("ObjectLockRetainUntilDate"))

    def attempt_overwrite(self, archive_uri, repo_name, relative_path,
                          content="tampered"):
        location = self._parse_location(archive_uri)
        key = self._object_path(location["prefix"], repo_name, relative_path)
        try:
            self._client().put_object(Bucket=location["bucket"], Key=key,
                                      Body=content.encode("utf-8"))
        except Exception as error:
            return False, str(error)
        return True, "overwrite succeeded"

    def delete_object(self, archive_uri, repo_name, relative_path):
        location = self._parse_location(archive_uri)
        key = self._object_path(location["prefix"], repo_name, relative_path)
        try:
            self._client().delete_object(Bucket=location["bucket"], Key=key)
        except Exception as error:
            return False, str(error)
        return True, "delete succeeded"

    def list_object_versions(self, archive_uri, repo_name, relative_path):
        location = self._parse_location(archive_uri)
        key = self._object_path(location["prefix"], repo_name, relative_path)
        versions = []
        kwargs = {"Bucket": location["bucket"], "Prefix": key}
        client = self._client()
        while True:
            response = client.list_object_versions(**kwargs)
            for item in response.get("Versions", []):
                if item.get("Key") == key:
                    versions.append({
                        "version_id": item.get("VersionId"),
                        "is_latest": item.get("IsLatest", False),
                        "delete_marker": False,
                    })
            for item in response.get("DeleteMarkers", []):
                if item.get("Key") == key:
                    versions.append({
                        "version_id": item.get("VersionId"),
                        "is_latest": item.get("IsLatest", False),
                        "delete_marker": True,
                    })
            if not response.get("IsTruncated"):
                return versions
            kwargs["KeyMarker"] = response.get("NextKeyMarker")
            kwargs["VersionIdMarker"] = response.get("NextVersionIdMarker")
