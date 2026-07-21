import json
import logging
import os
import uuid
from urllib.parse import urlparse

from google.api_core.exceptions import (
    FailedPrecondition,
    GoogleAPICallError,
    NotFound,
)
from google.cloud import storage
from google.cloud import kms

from couchbase_utils.cloud_provider_utils.cloud_provider_interface import \
    CloudOperationError, CloudProviderInterface
from couchbase_utils.security_utils.credential_store_utils import \
    CredentialStoreUtils


class GCPProvider(CloudProviderInterface):
    # cbbackupmgr/cbcontbk read --obj-auth-file on the node they run on,
    # which is a remote cluster node reached via `shell`, not the test
    # controller where GOOGLE_APPLICATION_CREDENTIALS points. Stage the key
    # file at this fixed remote path instead of passing the local path through.
    REMOTE_AUTH_FILE_PATH = "/tmp/tmp_gcp_service_account.json"

    def __init__(self, log=None):
        self.log = log if log is not None else logging.getLogger("test")
        self.gcp_service_account_key_file = os.getenv(
            "GOOGLE_APPLICATION_CREDENTIALS")
        self.gcp_region = os.getenv("GCP_REGION", "us")
        self._remote_auth_file_staged_hosts = set()
        self._gcs_client = None
        self.validate_credentials()

    def validate_credentials(self):
        if not self.gcp_service_account_key_file:
            raise CloudOperationError(
                "Missing GCS service-account credentials")

        self.gcp_kms_project = os.getenv("GCP_KMS_PROJECT")
        self.gcp_kms_location = os.getenv("GCP_KMS_LOCATION", self.gcp_region)
        self.gcp_kms_key_ring = os.getenv("GCP_KMS_KEY_RING", "contbk-taf")

        self.km_key_url = None
        self._km_key_name = None
        self._km_created_by_us = False

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

    def _client(self):
        if self._gcs_client is None:
            with open(self.gcp_service_account_key_file) as key_file:
                credentials_info = json.load(key_file)
            self._gcs_client = storage.Client.from_service_account_info(
                credentials_info)
        return self._gcs_client

    def list_objects(self, archive_uri, repo_name, relative_prefix=""):
        location = self._parse_location(archive_uri)
        prefix = self._object_path(location["prefix"], repo_name,
                                   relative_prefix)
        bucket = self._client().bucket(location["bucket"])
        return [blob.name for blob in bucket.list_blobs(prefix=prefix)]

    def object_exists(self, archive_uri, repo_name, relative_path):
        location = self._parse_location(archive_uri)
        key = self._object_path(location["prefix"], repo_name, relative_path)
        bucket = self._client().bucket(location["bucket"])
        return bucket.blob(key).exists()

    def read_text(self, archive_uri, repo_name, relative_path):
        location = self._parse_location(archive_uri)
        key = self._object_path(location["prefix"], repo_name, relative_path)
        bucket = self._client().bucket(location["bucket"])
        return bucket.blob(key).download_as_text()

    def get_retention_until(self, archive_uri, repo_name, relative_path):
        location = self._parse_location(archive_uri)
        key = self._object_path(location["prefix"], repo_name, relative_path)
        bucket = self._client().bucket(location["bucket"])
        blob = bucket.blob(key)
        blob.reload()
        return self._to_timestamp(
            getattr(blob, "retention_expiration_time", None))

    def attempt_overwrite(self, archive_uri, repo_name, relative_path,
                          content="tampered"):
        location = self._parse_location(archive_uri)
        key = self._object_path(location["prefix"], repo_name, relative_path)
        bucket = self._client().bucket(location["bucket"])
        blob = bucket.blob(key)
        try:
            blob.upload_from_string(content)
        except Exception as error:
            return False, str(error)
        return True, "overwrite succeeded"

    def delete_object(self, archive_uri, repo_name, relative_path):
        location = self._parse_location(archive_uri)
        key = self._object_path(location["prefix"], repo_name, relative_path)
        try:
            bucket = self._client().bucket(location["bucket"])
            bucket.blob(key).delete()
        except Exception as error:
            return False, str(error)
        return True, "delete succeeded"

    def list_object_versions(self, archive_uri, repo_name, relative_path):
        location = self._parse_location(archive_uri)
        key = self._object_path(location["prefix"], repo_name, relative_path)
        bucket = self._client().bucket(location["bucket"])
        return [{
            "version_id": getattr(blob, "generation", None),
            "is_latest": True,
            "delete_marker": False,
        } for blob in bucket.list_blobs(prefix=key, versions=True)
            if blob.name == key]

    def _kms_client(self):
        with open(self.gcp_service_account_key_file) as key_file:
            credentials_info = json.load(key_file)
        if not self.gcp_kms_project:
            self.gcp_kms_project = credentials_info.get("project_id")
        return kms.KeyManagementServiceClient.from_service_account_info(
            credentials_info)

    def _key_ring_path(self, client):
        return client.key_ring_path(
            self.gcp_kms_project, self.gcp_kms_location, self.gcp_kms_key_ring)

    def _crypto_key_url(self, key_id):
        return ("gcpkms://projects/{0}/locations/{1}/keyRings/{2}"
                "/cryptoKeys/{3}").format(
                    self.gcp_kms_project, self.gcp_kms_location,
                    self.gcp_kms_key_ring, key_id)

    def create_kms_key(self, alias=None):
        client = self._kms_client()
        if alias:
            self._km_key_name = alias
            self._km_created_by_us = False
        else:
            self._km_key_name = "contbk-taf-{0}".format(uuid.uuid4().hex[:12])
            try:
                client.get_key_ring(
                    request={"name": self._key_ring_path(client)})
            except Exception:
                parent = "projects/{0}/locations/{1}".format(
                    self.gcp_kms_project, self.gcp_kms_location)
                client.create_key_ring(
                    request={"parent": parent,
                             "key_ring_id": self.gcp_kms_key_ring,
                             "key_ring": {}})
            client.create_crypto_key(
                request={
                    "parent": self._key_ring_path(client),
                    "crypto_key_id": self._km_key_name,
                    "crypto_key": {
                        "purpose": kms.CryptoKey.CryptoKeyPurpose
                                    .ENCRYPT_DECRYPT}})
            self._km_created_by_us = True

        self.km_key_url = self._crypto_key_url(self._km_key_name)
        return self.km_key_url

    def delete_kms_key(self, key_url=None):
        # GCP KMS crypto keys cannot be truly deleted, only their key versions
        # can be scheduled for destruction. Reused keys are cheap; leaving the
        # key itself in place is the intended lifecycle.
        if key_url is None:
            key_url = self.km_key_url
        if not key_url or not self._km_created_by_us:
            return
        key_name = self._km_key_name
        try:
            client = self._kms_client()
        except Exception as e:
            self.log.error(
                "GCPProvider.delete_kms_key: failed to construct KMS client "
                "for key %s: %s. Key versions must be manually destroyed in "
                "the GCP console.", key_name, e)
            return

        key_path = client.crypto_key_path(
            self.gcp_kms_project, self.gcp_kms_location,
            self.gcp_kms_key_ring, key_name)

        try:
            versions = list(client.list_crypto_key_versions(
                request={"parent": key_path}))
        except NotFound as e:
            self.log.info(
                "GCP KMS list_crypto_key_versions(%s): key not found — "
                "safe to ignore on cleanup path. %s", key_name, e)
            versions = []
        except GoogleAPICallError as e:
            self.log.error(
                "GCP KMS list_crypto_key_versions(%s) API error: %s. Key "
                "versions must be manually destroyed in the GCP console.",
                key_name, e)
            versions = []
        except Exception as e:
            self.log.error(
                "GCP KMS list_crypto_key_versions(%s) raised unexpected: "
                "%s. Key versions must be manually destroyed in the GCP "
                "console.", key_name, e)
            versions = []

        # Per-version try/except so one un-destroyable version doesn't skip
        # the rest — versions already scheduled for destruction commonly
        # raise FailedPrecondition here and should not block the loop.
        for version in versions:
            if version.state != (kms.CryptoKeyVersion.CryptoKeyVersionState
                                   .ENABLED):
                continue
            try:
                client.destroy_crypto_key_version(
                    request={"name": version.name})
            except FailedPrecondition as e:
                self.log.info(
                    "GCP KMS destroy_crypto_key_version(%s): version not "
                    "in destroyable state — safe to ignore. %s",
                    version.name, e)
            except NotFound as e:
                self.log.info(
                    "GCP KMS destroy_crypto_key_version(%s): version not "
                    "found — safe to ignore. %s", version.name, e)
            except GoogleAPICallError as e:
                self.log.error(
                    "GCP KMS destroy_crypto_key_version(%s) API error: "
                    "%s. Version must be manually destroyed in the GCP "
                    "console.", version.name, e)
            except Exception as e:
                self.log.error(
                    "GCP KMS destroy_crypto_key_version(%s) raised "
                    "unexpected: %s.", version.name, e)

        self.km_key_url = None
        self._km_key_name = None
        self._km_created_by_us = False

    def get_km_flags(self, shell=None):
        if not self.km_key_url:
            raise RuntimeError(
                "GCPProvider.get_km_flags called before a key URL was set.")
        auth_file = self._stage_remote_auth_file(shell)
        return "--km-auth-file {0} --km-region {1} --km-key-url {2}".format(
            auth_file, self.gcp_kms_location, self.km_key_url)

    def set_km_key(self, key_url):
        self.km_key_url = key_url
        self._km_created_by_us = False
