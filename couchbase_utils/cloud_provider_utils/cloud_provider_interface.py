from abc import ABCMeta, abstractmethod


class CloudProviderInterface(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def get_cbbackupmgr_flags(self):
        pass

    @abstractmethod
    def get_cbconbk_flags(self):
        pass

    @abstractmethod
    def cleanup_for_bkrs(self, location):
        pass

    @abstractmethod
    def create_credential_store(self, rest, cred_id, username=None,
                                 password=None, description=None,
                                 allowed_services=None, expires_at_ms=None):
        pass

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
