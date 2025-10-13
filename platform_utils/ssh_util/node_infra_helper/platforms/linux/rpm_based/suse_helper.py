from node_infra_helper.platforms.linux.rpm_based.rpm_helper import RPMHelper
class SUSEHelper(RPMHelper):
    def __init__(self, ipaddr, ssh_username, ssh_password) -> None:
        super().__init__(ipaddr, ssh_username, ssh_password)
        raise NotImplementedError("The helper has to be implemented")

    def __del__(self):
        super().__del__()
    