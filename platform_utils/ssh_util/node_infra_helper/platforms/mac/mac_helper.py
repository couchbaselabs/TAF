from node_infra_helper.remote_connection_helper import RemoteConnectionHelper
class MacHelper(RemoteConnectionHelper):
    def __init__(self, ipaddr, ssh_username, ssh_password) -> None:
        super().__init__(ipaddr, ssh_username, ssh_password)
        raise NotImplementedError("The helper has to be implemented")
    
    def __del__(self):
        super().__del__()