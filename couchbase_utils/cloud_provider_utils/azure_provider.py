import os
from urllib.parse import urlparse

from azure.storage.blob import BlobServiceClient

from couchbase_utils.cloud_provider_utils.cloud_provider_interface import \
    CloudOperationError, CloudProviderInterface
from couchbase_utils.security_utils.credential_store_utils import \
    CredentialStoreUtils


class AzureProvider(CloudProviderInterface):
    def __init__(self):
        self.azure_storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
        self.azure_storage_key = os.getenv("AZURE_STORAGE_KEY")
        self.azure_region = os.getenv("AZURE_REGION", "westus")
        self.azure_endpoint = os.getenv("AZURE_ENDPOINT")
        self._blob_service_client = None
        self.validate_credentials()

    def validate_credentials(self):
        if not self.azure_storage_account or not self.azure_storage_key:
            raise CloudOperationError("Incomplete Azure credentials")

    def get_cbbackupmgr_flags(self, shell=None):
        return (
            "--obj-region {0} --obj-endpoint {1} --obj-access-key-id {2} "
            "--obj-secret-access-key {3}"
        ).format(self.azure_region, self.azure_endpoint,
                 self.azure_storage_account, self.azure_storage_key)

    def get_cbconbk_flags(self, shell=None):
        return self.get_cbbackupmgr_flags(shell)

    def cleanup_for_bkrs(self, azure_path):
        """
        Deletes the directory under the given Azure storage blob container
        so it no longer exists.

        :param azure_path: e.g. az://container-name/some-dir
        """
        parsed = urlparse(azure_path)
        container_name = parsed.netloc
        prefix = "{0}/".format(parsed.path.strip("/"))

        account_url = self.azure_endpoint or (
            "https://{0}.blob.core.windows.net".format(
                self.azure_storage_account))
        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=self.azure_storage_key)
        container_client = blob_service_client.get_container_client(
            container_name)
        for blob in container_client.list_blobs(name_starts_with=prefix):
            container_client.delete_blob(blob.name)

    def create_credential_store(self, rest, cred_id, username=None,
                                 password=None, description=None,
                                 allowed_services=None, expires_at_ms=None):
        """
        Build an 'azureShared' Credential Store payload and create it via
        POST /settings/credentials/:id.

        Returns:
            tuple: (status_code, content)
        """
        cs_utils = CredentialStoreUtils()
        payload = cs_utils.build_azure_payload(
            self.azure_storage_account, self.azure_storage_key,
            self.azure_endpoint, description=description,
            allowed_services=allowed_services,
            expires_at_ms=expires_at_ms)
        return cs_utils.create_credential(
            rest, cred_id, payload, username=username, password=password)

    @staticmethod
    def _parse_location(archive_uri):
        """
        Adds support for the https://<account>.blob.core.windows.net/
        <container>/<prefix> REST URL form on top of the generic
        scheme://netloc/path parsing (az://, azblob://, ...).
        """
        parsed = urlparse(archive_uri)
        if parsed.scheme.lower() in ("http", "https") and \
                ".blob.core.windows.net" in parsed.netloc:
            path = parsed.path.lstrip("/")
            parts = path.split("/", 1)
            return {"bucket": parts[0],
                   "prefix": parts[1] if len(parts) == 2 else ""}
        return CloudProviderInterface._parse_location(archive_uri)

    def _client(self):
        if self._blob_service_client is None:
            account_url = self.azure_endpoint or (
                "https://{0}.blob.core.windows.net".format(
                    self.azure_storage_account))
            self._blob_service_client = BlobServiceClient(
                account_url=account_url, credential=self.azure_storage_key)
        return self._blob_service_client

    def _blob_client(self, container, key):
        return self._client().get_container_client(container) \
            .get_blob_client(key)

    def list_objects(self, archive_uri, repo_name, relative_prefix=""):
        location = self._parse_location(archive_uri)
        prefix = self._object_path(location["prefix"], repo_name,
                                   relative_prefix)
        container_client = self._client().get_container_client(
            location["bucket"])
        return [blob.name for blob in
               container_client.list_blobs(name_starts_with=prefix)]

    def object_exists(self, archive_uri, repo_name, relative_path):
        location = self._parse_location(archive_uri)
        key = self._object_path(location["prefix"], repo_name, relative_path)
        return self._blob_client(location["bucket"], key).exists()

    def read_text(self, archive_uri, repo_name, relative_path):
        location = self._parse_location(archive_uri)
        key = self._object_path(location["prefix"], repo_name, relative_path)
        content = self._blob_client(location["bucket"], key) \
            .download_blob().readall()
        if isinstance(content, bytes):
            return content.decode("utf-8")
        return content

    def get_retention_until(self, archive_uri, repo_name, relative_path):
        location = self._parse_location(archive_uri)
        key = self._object_path(location["prefix"], repo_name, relative_path)
        properties = self._blob_client(location["bucket"], key) \
            .get_blob_properties()
        policy = getattr(properties, "immutability_policy", None)
        expiry = getattr(policy, "expiry_time", None)
        if expiry is None:
            expiry = getattr(policy, "expires_on", None)
        return self._to_timestamp(expiry)

    def attempt_overwrite(self, archive_uri, repo_name, relative_path,
                          content="tampered"):
        location = self._parse_location(archive_uri)
        key = self._object_path(location["prefix"], repo_name, relative_path)
        blob_client = self._blob_client(location["bucket"], key)
        try:
            blob_client.upload_blob(content.encode("utf-8"), overwrite=True)
        except Exception as error:
            return False, str(error)
        return True, "overwrite succeeded"

    def delete_object(self, archive_uri, repo_name, relative_path):
        location = self._parse_location(archive_uri)
        key = self._object_path(location["prefix"], repo_name, relative_path)
        try:
            self._blob_client(location["bucket"], key).delete_blob()
        except Exception as error:
            return False, str(error)
        return True, "delete succeeded"

    def list_object_versions(self, archive_uri, repo_name, relative_path):
        location = self._parse_location(archive_uri)
        key = self._object_path(location["prefix"], repo_name, relative_path)
        container_client = self._client().get_container_client(
            location["bucket"])
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
