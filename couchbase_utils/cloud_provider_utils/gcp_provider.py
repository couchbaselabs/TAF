import json
import os
from urllib.parse import urlparse

from google.cloud import storage

from couchbase_utils.cloud_provider_utils.cloud_provider_interface import \
    CloudProviderInterface
from couchbase_utils.security_utils.credential_store_utils import \
    CredentialStoreUtils


class GCPProvider(CloudProviderInterface):
    # cbbackupmgr/cbcontbk read --obj-auth-file on the node they run on,
    # which is a remote cluster node reached via `shell`, not the test
    # controller where GOOGLE_APPLICATION_CREDENTIALS points. Stage the key
    # file at this fixed remote path instead of passing the local path through.
    REMOTE_AUTH_FILE_PATH = "/tmp/tmp_gcp_service_account.json"

    def __init__(self):
        self.gcp_service_account_key_file = os.getenv(
            "GOOGLE_APPLICATION_CREDENTIALS")
        self.gcp_region = os.getenv("GCP_REGION", "us")
        self._remote_auth_file_staged_hosts = set()

    def _stage_remote_auth_file(self, shell):
        """
        Copies the local GCP service account key file to a fixed path on
        `shell`'s node (once per node) and returns that remote path. Falls
        back to the local path if no shell is given.
        """
        if shell is None:
            return self.gcp_service_account_key_file

        host = shell.ip
        if host not in self._remote_auth_file_staged_hosts:
            shell.copy_file_local_to_remote(
                self.gcp_service_account_key_file,
                self.REMOTE_AUTH_FILE_PATH)
            self._remote_auth_file_staged_hosts.add(host)
        return self.REMOTE_AUTH_FILE_PATH

    def get_cbbackupmgr_flags(self, shell=None):
        auth_file = self._stage_remote_auth_file(shell)
        return "--obj-auth-file {0} --obj-region {1}".format(
            auth_file, self.gcp_region)

    def get_cbconbk_flags(self, shell=None):
        return self.get_cbbackupmgr_flags(shell)

    def cleanup_for_bkrs(self, gcs_path):
        """
        Deletes the directory under the given GCS bucket so it no longer
        exists.

        :param gcs_path: e.g. gs://bucket-name/some-dir
        """
        parsed = urlparse(gcs_path)
        bucket_name = parsed.netloc
        prefix = "{0}/".format(parsed.path.strip("/"))

        with open(self.gcp_service_account_key_file) as key_file:
            credentials_info = json.load(key_file)
        client = storage.Client.from_service_account_info(credentials_info)
        bucket = client.bucket(bucket_name)
        for blob in bucket.list_blobs(prefix=prefix):
            blob.delete()

    def create_credential_store(self, rest, cred_id, username=None,
                                 password=None, description=None,
                                 allowed_services=None, expires_at_ms=None):
        """
        Build a 'gcp' Credential Store payload and create it via
        POST /settings/credentials/:id.

        Returns:
            tuple: (status_code, content)
        """
        cs_utils = CredentialStoreUtils()
        with open(self.gcp_service_account_key_file) as key_file:
            json_credentials = key_file.read()
        payload = cs_utils.build_gcp_service_account_payload(
            json_credentials, self.gcp_region, description=description,
            allowed_services=allowed_services,
            expires_at_ms=expires_at_ms)
        return cs_utils.create_credential(
            rest, cred_id, payload, username=username, password=password)
