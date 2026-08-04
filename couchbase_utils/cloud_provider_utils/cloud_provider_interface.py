import json
import time
from abc import ABCMeta, abstractmethod
from datetime import datetime
from urllib.parse import urlparse


class CloudOperationError(Exception):
    pass


class CloudProviderInterface(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def validate_credentials(self):
        """
        Verify that the provider's required credentials/config are present.

        Must be invoked from the provider's ``__init__`` and must raise
        ``CloudOperationError`` when any required credential is missing or
        incomplete, so misconfiguration fails fast at construction time.
        """
        pass

    @abstractmethod
    def get_cbbackupmgr_flags(self, shell=None):
        """
        :param shell: shell connection to the node cbbackupmgr will run on.
                     Providers whose flags reference a local file (e.g. GCP's
                     --obj-auth-file) need this to stage the file on that
                     node first, since cbbackupmgr runs there, not on the
                     test controller.
        """
        pass

    @abstractmethod
    def get_cbconbk_flags(self, shell=None):
        pass

    @abstractmethod
    def cleanup_for_bkrs(self, location):
        pass

    @abstractmethod
    def create_credential_store(self, rest, cred_id, username=None,
                                 password=None, description=None,
                                 allowed_services=None, expires_at_ms=None):
        pass

    def create_kms_credential_store(self, rest, cred_id, username=None,
                                    password=None, description=None,
                                    allowed_services=None,
                                    expires_at_ms=None):
        """
        Upload a Credential Store entry usable as `continuousBackupKmCredId`.

        Default: delegate to `create_credential_store` — for AWS and GCP the same IAM identity legitimately serves both
        object-store and KMS, so a single credential type suffices. Providers whose KMS uses a distinct credential type
        from their object-store (e.g. Azure: azureShared for blob, azureAd for Key Vault) must override.
        """
        return self.create_credential_store(
            rest, cred_id, username=username, password=password,
            description=description, allowed_services=allowed_services,
            expires_at_ms=expires_at_ms)

    @abstractmethod
    def create_kms_key(self, alias=None):
        """
        Create (or reuse) a KMS key for backup EaR tests.

        :param alias: If given, reuse an existing key with this alias/name
                      instead of creating a new one. Useful in CI where the
                      IAM identity may not have kms:CreateKey.
        :return: A cbbackupmgr/cbcontbk-compatible key URL string
                 (e.g., ``awskms://alias/<name>``). Provider also stores
                 the URL as state so ``get_km_flags()`` can find it.
        """
        pass

    @abstractmethod
    def delete_kms_key(self, key_url=None):
        """
        Schedule deletion of a KMS key previously created by this provider.
        No-op when the key was reused (not created by this run).

        :param key_url: If None, deletes the key URL currently held as state.
        """
        pass

    @abstractmethod
    def get_km_flags(self, shell=None):
        """
        Return the ``--km-*`` flag string for cbbackupmgr / cbcontbk. Both
        tools accept the same flag names for KMS credentials. Callers must
        have set a key URL first (via ``create_kms_key`` or ``set_km_key``).

        :param shell: shell connection to the node the CLI runs on (mirrors
                      ``get_cbbackupmgr_flags``); providers that reference a
                      local file need it to stage.
        """
        pass

    @abstractmethod
    def set_km_key(self, key_url):
        """
        Attach an already-existing key URL to this provider without creating
        a new key. Used at restore time in tests that split setUp/tearDown
        across processes, or to point at a pre-provisioned CI key.
        """
        pass

    # ---- WORM object-store primitives ----
    # Each provider implements these against its own SDK. Every method takes
    # `archive_uri` (e.g. s3://bucket/prefix, gs://bucket/prefix,
    # az://container/prefix) per call rather than storing it on `self`, since
    # a single provider instance is reused across archive locations - same
    # convention as cleanup_for_bkrs(location).

    @abstractmethod
    def list_objects(self, archive_uri, repo_name, relative_prefix=""):
        pass

    @abstractmethod
    def object_exists(self, archive_uri, repo_name, relative_path):
        pass

    @abstractmethod
    def read_text(self, archive_uri, repo_name, relative_path):
        pass

    @abstractmethod
    def get_retention_until(self, archive_uri, repo_name, relative_path):
        """Returns a unix timestamp, or None if the object has no retention."""
        pass

    @abstractmethod
    def attempt_overwrite(self, archive_uri, repo_name, relative_path,
                          content="tampered"):
        """Returns (succeeded: bool, message: str)."""
        pass

    @abstractmethod
    def delete_object(self, archive_uri, repo_name, relative_path):
        """Returns (succeeded: bool, message: str)."""
        pass

    @abstractmethod
    def list_object_versions(self, archive_uri, repo_name, relative_path):
        """Returns a list of {version_id, is_latest, delete_marker} dicts."""
        pass

    # ---- WORM helpers built only on the primitives above ----
    # Generic across every provider, so implemented once here rather than
    # duplicated per subclass.

    @staticmethod
    def _parse_location(archive_uri):
        parsed = urlparse(archive_uri)
        return {"bucket": parsed.netloc, "prefix": parsed.path.strip("/")}

    @staticmethod
    def _object_path(prefix, repo_name, relative_path=""):
        parts = [prefix, repo_name, relative_path]
        return "/".join(part.strip("/") for part in parts if part)

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
                return datetime.fromisoformat(
                    value.replace("Z", "+00:00")).timestamp()
            except ValueError:
                return None
        return None

    def relative_path(self, archive_uri, repo_name, object_name):
        location = self._parse_location(archive_uri)
        repo_prefix = self._object_path(location["prefix"], repo_name)
        return object_name[len(repo_prefix):].strip("/")

    def read_json(self, archive_uri, repo_name, relative_path):
        return json.loads(self.read_text(archive_uri, repo_name, relative_path))

    def upload_text(self, archive_uri, repo_name, relative_path, content):
        return self.attempt_overwrite(archive_uri, repo_name, relative_path,
                                      content)

    def find_backup_names(self, archive_uri, repo_name):
        location = self._parse_location(archive_uri)
        repo_prefix = self._object_path(location["prefix"], repo_name)
        names = set()
        for object_name in self.list_objects(archive_uri, repo_name):
            relative_name = object_name[len(repo_prefix):].strip("/")
            if relative_name and not relative_name.startswith("."):
                names.add(relative_name.split("/", 1)[0])
        return sorted(names)

    def find_latest_backup_name(self, archive_uri, repo_name):
        names = self.find_backup_names(archive_uri, repo_name)
        if names:
            return names[-1]
        return None

    def find_relative_paths(self, archive_uri, repo_name, suffix=None,
                            contains=None):
        paths = []
        for object_name in self.list_objects(archive_uri, repo_name):
            relative_name = self.relative_path(archive_uri, repo_name,
                                               object_name)
            if suffix and not relative_name.endswith(suffix):
                continue
            if contains and contains not in relative_name:
                continue
            paths.append(relative_name)
        return paths

    def find_metadata_path(self, archive_uri, repo_name, names):
        for name in names:
            matches = self.find_relative_paths(archive_uri, repo_name,
                                               suffix=name)
            if matches:
                return matches[0]
        return None

    def find_first_data_object(self, archive_uri, repo_name):
        metadata_names = [".worm", ".status_flag", ".obj_versions", "plan.json"]
        for object_name in self.list_objects(archive_uri, repo_name):
            relative_name = self.relative_path(archive_uri, repo_name,
                                               object_name)
            if not relative_name or relative_name.endswith("/"):
                continue
            if any(relative_name.endswith(name) for name in metadata_names):
                continue
            return relative_name
        return None

    def wait_for_objects(self, archive_uri, repo_name, timeout=300,
                         relative_prefix=""):
        end_time = time.time() + timeout
        while time.time() < end_time:
            if self.list_objects(archive_uri, repo_name,
                                 relative_prefix=relative_prefix):
                return True
            time.sleep(10)
        return False
