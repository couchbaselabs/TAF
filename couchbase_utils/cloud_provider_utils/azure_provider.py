import logging
import os
import uuid
from urllib.parse import urlparse

from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.identity import ClientSecretCredential
from azure.keyvault.keys import KeyClient
from azure.storage.blob import BlobServiceClient

from couchbase_utils.cloud_provider_utils.cloud_provider_interface import \
    CloudOperationError, CloudProviderInterface
from couchbase_utils.security_utils.credential_store_utils import \
    CredentialStoreUtils


class AzureProvider(CloudProviderInterface):
    def __init__(self, log=None):
        self.log = log if log is not None else logging.getLogger("test")
        self.azure_storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
        self.azure_storage_key = os.getenv("AZURE_STORAGE_KEY")
        self.azure_region = os.getenv("AZURE_REGION", "westus")
        self.azure_endpoint = os.getenv("AZURE_ENDPOINT")
        self._blob_service_client = None
        self.validate_credentials()

    def validate_credentials(self):
        if not self.azure_storage_account or not self.azure_storage_key:
            raise CloudOperationError("Incomplete Azure credentials")

        self.azure_kv_url = os.getenv("AZURE_KEY_VAULT_URL")
        self.azure_tenant_id = os.getenv("AZURE_TENANT_ID")
        self.azure_client_id = os.getenv("AZURE_CLIENT_ID")
        self.azure_client_secret = os.getenv("AZURE_CLIENT_SECRET")

        self.km_key_url = None
        self._km_key_name = None
        self._km_created_by_us = False
        if self.azure_kv_url:
            self._kms_client = self._build_kms_client()
        else:
            self._kms_client = None

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

    def create_kms_credential_store(self, rest, cred_id, username=None,
                                    password=None, description=None,
                                    allowed_services=None,
                                    expires_at_ms=None):
        """
        Build an 'azureAd' Credential Store payload from the AD service principal env vars
        (AZURE_TENANT_ID/AZURE_CLIENT_ID/AZURE_CLIENT_SECRET) the same identity used to talk to Key Vault in `_build_kms_client()`.
        """
        if not (self.azure_tenant_id and self.azure_client_id
                and self.azure_client_secret):
            raise CloudOperationError(
                "AZURE_TENANT_ID, AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET "
                "must be set to upload an azureAd credential for KMS.")
        cs_utils = CredentialStoreUtils()
        payload = cs_utils.build_azure_ad_payload(
            self.azure_tenant_id, self.azure_client_id,
            self.azure_client_secret, description=description,
            allowed_services=allowed_services,
            expires_at_ms=expires_at_ms)
        return cs_utils.create_credential(
            rest, cred_id, payload, username=username, password=password)

    def _build_kms_client(self):
        credential = ClientSecretCredential(
            tenant_id=self.azure_tenant_id,
            client_id=self.azure_client_id,
            client_secret=self.azure_client_secret)
        return KeyClient(vault_url=self.azure_kv_url, credential=credential)

    def _vault_host(self):
        return urlparse(self.azure_kv_url).netloc

    def create_kms_key(self, alias=None):
        if self._kms_client is None:
            raise RuntimeError(
                "AZURE_KEY_VAULT_URL must be set for KMS operations.")
        if alias:
            self._kms_client.get_key(alias)
            self._km_key_name = alias
            self._km_created_by_us = False
        else:
            self._km_key_name = "contbk-taf-{0}".format(uuid.uuid4().hex[:12])
            self._kms_client.create_rsa_key(
                name=self._km_key_name, size=2048)
            self._km_created_by_us = True

        self.km_key_url = "azurekeyvault://{0}/keys/{1}".format(
            self._vault_host(), self._km_key_name)
        return self.km_key_url

    def delete_kms_key(self, key_url=None):
        if key_url is None:
            key_url = self.km_key_url
        if not key_url or not self._km_created_by_us:
            return
        key_name = self._km_key_name
        if self._kms_client is None:
            self.log.error(
                f"AzureProvider.delete_kms_key: Key Vault client not "
                f"constructed (AZURE_KEY_VAULT_URL unset) — key {key_name} "
                f"must be manually deleted from vault {self.azure_kv_url}.")
            return

        try:
            self._kms_client.begin_delete_key(key_name)
        except ResourceNotFoundError as e:
            self.log.info(
                f"Azure Key Vault begin_delete_key({key_name}): key not "
                f"found — safe to ignore on cleanup path. {e}")
        except HttpResponseError as e:
            status_code = getattr(e, "status_code", "?")
            self.log.error(
                f"Azure Key Vault begin_delete_key({key_name}) HTTP error "
                f"[status={status_code}]: {e}. Key must be manually "
                f"deleted from vault {self.azure_kv_url}.")
        except Exception as e:
            self.log.error(
                f"Azure Key Vault begin_delete_key({key_name}) raised "
                f"unexpected: {e}. Key must be manually deleted from "
                f"vault {self.azure_kv_url}.")

        self.km_key_url = None
        self._km_key_name = None
        self._km_created_by_us = False

    def get_km_flags(self, shell=None):
        if not self.km_key_url:
            raise RuntimeError(
                "AzureProvider.get_km_flags called before a key URL was set.")
        # cbbackupmgr/cbcontbk reuse --km-access-key-id / --km-secret-access-key
        # for the AD client id / client secret when the target KM is Azure Key
        # Vault, plus --km-tenant-id on top. There is no --km-client-id flag.
        return (
            "--km-tenant-id {0} --km-access-key-id {1} "
            "--km-secret-access-key {2} --km-key-url {3}"
        ).format(self.azure_tenant_id, self.azure_client_id,
                 self.azure_client_secret, self.km_key_url)

    def set_km_key(self, key_url):
        # Parse `azurekeyvault://<vault>.vault.azure.net/keys/<name>`
        # (optionally with a trailing `/<version>`) and extract just the key name:
        # the segment right after `/keys/`, stripping any version suffix.
        # The parsing is prefix-agnostic (works if the URL ever arrives with `https://` too), so it
        # doesn't need to know that cbbackupmgr's accepted scheme is `azurekeyvault://`.
        self.km_key_url = key_url
        self._km_key_name = None
        if key_url and "/keys/" in key_url:
            tail = key_url.rsplit("/keys/", 1)[-1]
            self._km_key_name = tail.split("/", 1)[0]
        self._km_created_by_us = False

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
