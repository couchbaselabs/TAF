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
