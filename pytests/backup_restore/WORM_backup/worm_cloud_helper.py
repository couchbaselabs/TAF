import json
import os
import time
from datetime import datetime
from urllib.parse import urlparse


class CloudOperationError(Exception):
    pass


class WormCloudHelper(object):
    def __init__(self, archive_uri, storage_provider=None, test_obj=None):
        self.archive_uri = archive_uri
        self.storage_provider = self._normalise_provider(
            storage_provider or self._infer_provider(archive_uri))
        self.test_obj = test_obj
        self.location = self._parse_archive_uri(archive_uri)
        self.client = self._create_client()
        self._validate_credentials()

    @staticmethod
    def _normalise_provider(provider):
        if provider is None:
            return None
        provider = str(provider).lower()
        if provider in ["s3", "aws"]:
            return "aws"
        if provider in ["az", "azure", "azblob", "azureblob"]:
            return "azure"
        if provider in ["gcp", "gcs", "gs"]:
            return "gcs"
        return provider

    @staticmethod
    def _infer_provider(archive_uri):
        scheme = urlparse(archive_uri).scheme.lower()
        if scheme == "s3":
            return "aws"
        if scheme in ["az", "azure", "azblob", "azureblob"]:
            return "azure"
        if scheme in ["gcs", "gs"]:
            return "gcs"
        return None

    @staticmethod
    def _parse_archive_uri(archive_uri):
        parsed = urlparse(archive_uri)
        scheme = parsed.scheme.lower()
        path = parsed.path.lstrip("/")
        if scheme in ["s3", "gs", "gcs"]:
            return {"bucket": parsed.netloc, "prefix": path}
        if scheme in ["az", "azure", "azblob", "azureblob"]:
            return {"container": parsed.netloc, "prefix": path}
        if scheme in ["http", "https"] and ".blob.core.windows.net" in parsed.netloc:
            parts = path.split("/", 1)
            return {
                "container": parts[0],
                "prefix": parts[1] if len(parts) == 2 else ""
            }
        raise ValueError("Unsupported WORM archive URI: %s" % archive_uri)

    def _create_client(self):
        if self.storage_provider == "aws":
            from awsLib.S3 import S3
            return S3(
                self._value("aws_access_key", "AWS_ACCESS_KEY_ID"),
                self._value("aws_secret_key", "AWS_SECRET_ACCESS_KEY"),
                session_token=self._value("aws_session_token", "AWS_SESSION_TOKEN"),
                region=self._value("aws_region", "AWS_DEFAULT_REGION"),
                endpoint_url=self._value("aws_endpoint", "AWS_ENDPOINT_URL"))
        if self.storage_provider == "azure":
            from azureLib.Azure import Azure
            account_name = self._value("azure_account_name",
                                       "AZURE_BLOB_ACCESS_KEY_ID")
            account_key = self._value("azure_account_key",
                                      "AZURE_BLOB_SECRET_ACCESS_KEY")
            if account_name is None:
                account_name = self._value("aws_access_key", None)
            if account_key is None:
                account_key = self._value("aws_secret_key", None)
            if not account_name or not account_key:
                raise CloudOperationError(
                    "Missing Azure storage account credentials")
            return Azure(account_name=account_name, account_key=account_key)
        if self.storage_provider == "gcs":
            from gcs import GCS
            credentials = getattr(self.test_obj, "gcs_credentials_data", None)
            if credentials is None:
                credentials_file = os.getenv("gcp_access_file")
                if credentials_file:
                    with open(credentials_file, "r") as file_handle:
                        credentials = json.load(file_handle)
            if credentials is None:
                raise CloudOperationError(
                    "Missing GCS service-account credentials")
            return GCS(credentials)
        raise ValueError("Unsupported WORM storage provider: %s"
                         % self.storage_provider)

    def _validate_credentials(self):
        if self.storage_provider != "aws":
            return
        session = getattr(self.client, "aws_session", None)
        if session is None:
            raise CloudOperationError("AWS session is not available")
        try:
            credentials = session.get_credentials()
        except Exception as error:
            raise CloudOperationError(
                "Failed to resolve AWS credentials: %s" % error)
        if credentials is None:
            raise CloudOperationError("Unable to locate AWS credentials")
        frozen_credentials = credentials.get_frozen_credentials()
        if not frozen_credentials.access_key or not frozen_credentials.secret_key:
            raise CloudOperationError("Incomplete AWS credentials")

    def _value(self, attr_name, env_name):
        if self.test_obj is not None and attr_name:
            value = getattr(self.test_obj, attr_name, None)
            if value:
                return value
        if env_name:
            return os.getenv(env_name)
        return None

    def _object_path(self, repo_name, relative_path=""):
        parts = [self.location.get("prefix", ""), repo_name, relative_path]
        return "/".join(part.strip("/") for part in parts if part)

    def relative_path(self, repo_name, object_name):
        repo_prefix = self._object_path(repo_name)
        return object_name[len(repo_prefix):].strip("/")

    @staticmethod
    def _to_timestamp(value):
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, datetime):
            return value.timestamp()
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
            except ValueError:
                return None
        return None

    def list_objects(self, repo_name, relative_prefix=""):
        prefix = self._object_path(repo_name, relative_prefix)
        if self.storage_provider == "aws":
            return self._list_s3_objects(prefix)
        if self.storage_provider == "azure":
            container_client = self.client.blob_service_client.get_container_client(
                self.location["container"])
            return [blob.name for blob in container_client.list_blobs(
                name_starts_with=prefix)]
        bucket = self.client.client.bucket(self.location["bucket"])
        return [blob.name for blob in bucket.list_blobs(prefix=prefix)]

    def _list_s3_objects(self, prefix):
        keys = []
        continuation_token = None
        while True:
            kwargs = {"Bucket": self.location["bucket"], "Prefix": prefix}
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token
            response = self.client.s3_client.list_objects_v2(**kwargs)
            keys.extend(item["Key"] for item in response.get("Contents", []))
            if not response.get("IsTruncated"):
                return keys
            continuation_token = response.get("NextContinuationToken")

    def object_exists(self, repo_name, relative_path):
        key = self._object_path(repo_name, relative_path)
        if self.storage_provider == "aws":
            try:
                self.client.s3_client.head_object(
                    Bucket=self.location["bucket"], Key=key)
                return True
            except Exception:
                return False
        if self.storage_provider == "azure":
            return self._azure_blob_client(key).exists()
        bucket = self.client.client.bucket(self.location["bucket"])
        return bucket.blob(key).exists()

    def read_json(self, repo_name, relative_path):
        return json.loads(self.read_text(repo_name, relative_path))

    def read_text(self, repo_name, relative_path):
        key = self._object_path(repo_name, relative_path)
        if self.storage_provider == "aws":
            response = self.client.s3_client.get_object(
                Bucket=self.location["bucket"], Key=key)
            return response["Body"].read().decode("utf-8")
        if self.storage_provider == "azure":
            content = self._azure_blob_client(key).download_blob().readall()
            if isinstance(content, bytes):
                return content.decode("utf-8")
            return content
        bucket = self.client.client.bucket(self.location["bucket"])
        return bucket.blob(key).download_as_text()

    def find_backup_names(self, repo_name):
        repo_prefix = self._object_path(repo_name)
        names = set()
        for object_name in self.list_objects(repo_name):
            relative_name = object_name[len(repo_prefix):].strip("/")
            if relative_name and not relative_name.startswith("."):
                names.add(relative_name.split("/", 1)[0])
        return sorted(names)

    def find_latest_backup_name(self, repo_name):
        names = self.find_backup_names(repo_name)
        if names:
            return names[-1]
        return None

    def find_relative_paths(self, repo_name, suffix=None, contains=None):
        paths = []
        for object_name in self.list_objects(repo_name):
            relative_name = self.relative_path(repo_name, object_name)
            if suffix and not relative_name.endswith(suffix):
                continue
            if contains and contains not in relative_name:
                continue
            paths.append(relative_name)
        return paths

    def find_metadata_path(self, repo_name, names):
        for name in names:
            matches = self.find_relative_paths(repo_name, suffix=name)
            if matches:
                return matches[0]
        return None

    def find_first_data_object(self, repo_name):
        metadata_names = [".worm", ".statusflag", ".obj_versions", "plan.json"]
        for object_name in self.list_objects(repo_name):
            relative_name = self.relative_path(repo_name, object_name)
            if not relative_name:
                continue
            if any(relative_name.endswith(name) for name in metadata_names):
                continue
            if relative_name.endswith("/"):
                continue
            return relative_name
        return None

    def get_retention_until(self, repo_name, relative_path):
        key = self._object_path(repo_name, relative_path)
        if self.storage_provider == "aws":
            return self._get_s3_retention_until(key)
        if self.storage_provider == "azure":
            properties = self._azure_blob_client(key).get_blob_properties()
            policy = getattr(properties, "immutability_policy", None)
            expiry = getattr(policy, "expiry_time", None)
            if expiry is None:
                expiry = getattr(policy, "expires_on", None)
            return self._to_timestamp(expiry)
        bucket = self.client.client.bucket(self.location["bucket"])
        blob = bucket.blob(key)
        blob.reload()
        return self._to_timestamp(getattr(blob, "retention_expiration_time", None))

    def _get_s3_retention_until(self, key):
        try:
            response = self.client.s3_client.get_object_retention(
                Bucket=self.location["bucket"], Key=key)
            timestamp = self._to_timestamp(
                response.get("Retention", {}).get("RetainUntilDate"))
            if timestamp is not None:
                return timestamp
        except Exception:
            pass
        response = self.client.s3_client.head_object(
            Bucket=self.location["bucket"], Key=key)
        return self._to_timestamp(response.get("ObjectLockRetainUntilDate"))

    def wait_for_objects(self, repo_name, timeout=300, relative_prefix=""):
        end_time = time.time() + timeout
        while time.time() < end_time:
            if self.list_objects(repo_name, relative_prefix=relative_prefix):
                return True
            time.sleep(10)
        return False

    def attempt_overwrite(self, repo_name, relative_path, content="tampered"):
        key = self._object_path(repo_name, relative_path)
        if self.storage_provider == "aws":
            return self._attempt_s3_overwrite(key, content)
        if self.storage_provider == "azure":
            return self._attempt_azure_overwrite(key, content)
        return self._attempt_gcs_overwrite(key, content)

    def upload_text(self, repo_name, relative_path, content):
        key = self._object_path(repo_name, relative_path)
        if self.storage_provider == "aws":
            return self._attempt_s3_overwrite(key, content)
        if self.storage_provider == "azure":
            return self._attempt_azure_overwrite(key, content)
        return self._attempt_gcs_overwrite(key, content)

    def delete_object(self, repo_name, relative_path):
        key = self._object_path(repo_name, relative_path)
        try:
            if self.storage_provider == "aws":
                self.client.s3_client.delete_object(
                    Bucket=self.location["bucket"], Key=key)
            elif self.storage_provider == "azure":
                self._azure_blob_client(key).delete_blob()
            else:
                bucket = self.client.client.bucket(self.location["bucket"])
                bucket.blob(key).delete()
        except Exception as error:
            return False, str(error)
        return True, "delete succeeded"

    def list_object_versions(self, repo_name, relative_path):
        key = self._object_path(repo_name, relative_path)
        if self.storage_provider == "aws":
            return self._list_s3_object_versions(key)
        if self.storage_provider == "azure":
            return self._list_azure_object_versions(key)
        return self._list_gcs_object_versions(key)

    def _list_s3_object_versions(self, key):
        versions = []
        kwargs = {"Bucket": self.location["bucket"], "Prefix": key}
        while True:
            response = self.client.s3_client.list_object_versions(**kwargs)
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

    def _list_azure_object_versions(self, key):
        container_client = self.client.blob_service_client.get_container_client(
            self.location["container"])
        versions = []
        for blob in container_client.list_blobs(
                name_starts_with=key, include=["versions"]):
            if blob.name == key:
                versions.append({
                    "version_id": getattr(blob, "version_id", None),
                    "is_latest": getattr(blob, "is_current_version", False),
                    "delete_marker": False,
                })
        return versions

    def _list_gcs_object_versions(self, key):
        bucket = self.client.client.bucket(self.location["bucket"])
        return [{
            "version_id": getattr(blob, "generation", None),
            "is_latest": True,
            "delete_marker": False,
        } for blob in bucket.list_blobs(prefix=key, versions=True)
            if blob.name == key]

    def _attempt_s3_overwrite(self, key, content):
        try:
            self.client.s3_client.put_object(
                Bucket=self.location["bucket"],
                Key=key,
                Body=content.encode("utf-8"))
        except Exception as error:
            return False, str(error)
        return True, "overwrite succeeded"

    def _attempt_azure_overwrite(self, key, content):
        blob_client = self._azure_blob_client(key)
        try:
            blob_client.upload_blob(content.encode("utf-8"), overwrite=True)
        except Exception as error:
            return False, str(error)
        return True, "overwrite succeeded"

    def _attempt_gcs_overwrite(self, key, content):
        bucket = self.client.client.bucket(self.location["bucket"])
        blob = bucket.blob(key)
        try:
            blob.upload_from_string(content)
        except Exception as error:
            return False, str(error)
        return True, "overwrite succeeded"

    def _azure_blob_client(self, key):
        container_client = self.client.blob_service_client.get_container_client(
            self.location["container"])
        return container_client.get_blob_client(key)