from install_util.constants.windows import WindowsConstants
from shell_util.remote_connection import RemoteMachineShellConnection


class Windows(WindowsConstants):
    def __init__(self, test_server):
        super(Windows, self).__init__()
        self.shell = RemoteMachineShellConnection(test_server)

    def uninstall(self):
        self.shell.stop_couchbase()
        cmd = self.cmds["uninstall"]
        self.shell.execute_command(cmd)
        return True

    def install(self, build_url):
        cmd = self.cmds["install"]
        f_name = build_url.split("/")[-1]
        cmd = cmd.replace("buildpath", "{}/{}"
                          .format(self.download_dir, f_name))
        self.shell.execute_command(cmd)

        output, err = self.shell.execute_command(cmd)
        if output[0] == '1':
            return True
        self.shell.log.critical("Output: {}, Error: {}".format(output, err))
        return False

    def post_install(self):
        cmds = self.cmds
        cmd = cmds["post_install"]
        retry_cmd = cmds["post_install_retry"]

        if cmd is None:
            return True

        output, err = self.shell.execute_command(cmd)
        if output[0] == '1':
            return True

        self.shell.log.critical("Output: {}, Error: {}".format(output, err))
        if retry_cmd is None:
            return False

        self.shell.log.critical("Retrying post_install steps")
        output, err = self.shell.execute_command(retry_cmd)
        if output[0] == '1':
            return True
        self.shell.log.critical("Output: {}, Error: {}".format(output, err))
        return False

    def init_cluster(self, node):
        return True
