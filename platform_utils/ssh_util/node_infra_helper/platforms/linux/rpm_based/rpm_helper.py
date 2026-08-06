from node_infra_helper.platforms.linux.linux_helper import LinuxHelper
class RPMHelper(LinuxHelper):
    def __init__(self, ipaddr, ssh_username, ssh_password) -> None:
        super().__init__(ipaddr, ssh_username, ssh_password)

    def __del__(self):
        super().__del__()
    
    def initialize_node(self):
        raise NotImplementedError("The helper has to be implemented")