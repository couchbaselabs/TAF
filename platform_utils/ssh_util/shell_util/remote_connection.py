from shell_util.platforms.linux import Linux
from shell_util.platforms.unix import Unix
from shell_util.platforms.windows import Windows
from shell_util.shell_conn import ShellConnection


class SupportedPlatforms(object):
    MAC = "mac"
    WINDOWS = "windows"
    LINUX = "linux"


class RemoteMachineShellConnection(object):
    __info_dict = dict()

    @staticmethod
    def get_info_for_server(server):
        if server.ip in RemoteMachineShellConnection.__info_dict:
            return RemoteMachineShellConnection.__info_dict[server.ip]

    def __new__(cls, *args, **kwargs):
        server = args[0]
        if server.ip in RemoteMachineShellConnection.__info_dict:
            info = RemoteMachineShellConnection.__info_dict[server.ip]
        else:
            shell = ShellConnection(server)
            shell.ssh_connect_with_retries(server.ip, server.ssh_username,
                                           server.ssh_password, server.ssh_key)
            info = shell.extract_remote_info()
            shell.disconnect()
            RemoteMachineShellConnection.__info_dict[server.ip] = info

        platform = info.type.lower()
        if platform == SupportedPlatforms.LINUX:
            target_class = Linux
        elif platform == SupportedPlatforms.WINDOWS:
            target_class = Windows
        elif platform == SupportedPlatforms.MAC:
            target_class = Unix
        else:
            raise NotImplementedError("Unsupported platform")
        obj = super(RemoteMachineShellConnection, cls) \
            .__new__(target_class, *args, **kwargs)
        obj.__init__(server, info)
        obj.ssh_connect_with_retries(server.ip, server.ssh_username,
                                     server.ssh_password, server.ssh_key)
        return obj

    @staticmethod
    def delete_info_for_server(server, ipaddr=None):
        ipaddr = ipaddr or server.ip
        if ipaddr in RemoteMachineShellConnection.__info_dict:
            del RemoteMachineShellConnection.__info_dict[ipaddr]
        RemoteMachineShellConnection.__info_dict.pop(ipaddr, None)
