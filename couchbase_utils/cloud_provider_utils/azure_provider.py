import os
from urllib.parse import urlparse

from azure.storage.blob import BlobServiceClient

from couchbase_utils.cloud_provider_utils.cloud_provider_interface import \
    CloudProviderInterface
from couchbase_utils.security_utils.credential_store_utils import \
    CredentialStoreUtils


class AzureProvider(CloudProviderInterface):
    def __init__(self):
        self.azure_storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
        self.azure_storage_key = os.getenv("AZURE_STORAGE_KEY")
        self.azure_region = os.getenv("AZURE_REGION")
        self.azure_endpoint = os.getenv("AZURE_ENDPOINT")

    def get_cbbackupmgr_flags(self):
        return (
            "--obj-region {0} --obj-endpoint {1} --obj-access-key-id {2} "
            "--obj-secret-access-key {3}"
        ).format(self.azure_region, self.azure_endpoint,
                 self.azure_storage_account, self.azure_storage_key)

    def get_cbconbk_flags(self):
        return self.get_cbbackupmgr_flags()

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
