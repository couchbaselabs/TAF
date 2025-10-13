from install_util.test_input import TestInputServer
from shell_util.remote_connection import RemoteMachineShellConnection
from node_infra_helper.remote_connection_helper import RemoteConnectionHelper
from node_infra_helper.platforms.windows.windows_helper import WindowsHelper
from node_infra_helper.platforms.linux.linux_helper import LinuxHelper
from node_infra_helper.platforms.mac.mac_helper import MacHelper
from node_infra_helper.platforms.linux.debian_based.debian_helper import DebianHelper
from node_infra_helper.platforms.linux.rpm_based.rpm_helper import RPMHelper
from node_infra_helper.platforms.linux.rpm_based.suse_helper import SUSEHelper

class RemoteConnectionObjectFactory:
    @staticmethod
    def fetch_helper(ipaddr, ssh_username, ssh_password):
        target_object = None
        server = TestInputServer()
        server.ip = ipaddr
        server.ssh_username = ssh_username
        server.ssh_password = ssh_password

        shell = RemoteMachineShellConnection(server)
        os_info = RemoteMachineShellConnection.get_info_for_server(server)
        shell.disconnect()

        if os_info.type.lower() == "linux":
            if os_info.deliverable_type.lower() == "deb":
                target_object = DebianHelper(ipaddr, ssh_username, ssh_password)
            elif os_info.deliverable_type.lower() == "rpm":
                if "suse" not in os_info.distribution_version.lower():
                    target_object = RPMHelper(ipaddr, ssh_username, ssh_password)
                else:
                    target_object = SUSEHelper(ipaddr, ssh_username, ssh_password)
            else:
                target_object = LinuxHelper(ipaddr, ssh_username, ssh_password)
        elif os_info.type.lower() == "mac":
            target_object = MacHelper(ipaddr, ssh_username, ssh_password)
        elif os_info.type.lower() == "windows":
            target_object = WindowsHelper(ipaddr, ssh_username, ssh_password)
        else:
            target_object = RemoteConnectionHelper(ipaddr, ssh_username, ssh_password)
        return target_object
    
    @staticmethod
    def delete_helper(ipaddr):
        RemoteMachineShellConnection.delete_info_for_server(None, ipaddr)


