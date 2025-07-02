import os

from TestInput import TestInputSingleton
from cb_constants.ClusterRun import ClusterRun
from platform_constants.os_constants import Linux, LinuxEnterpriseAnalytics, Mac, Windows


class CbCmdBase:
    def __init__(self, shell_conn, binary_name,
                 username="Administrator", password="password"):

        self.shellConn = shell_conn
        self.port = int(shell_conn.port)
        self.mc_port = shell_conn.server.memcached_port
        self.username = username
        self.password = password
        self.binaryName = binary_name
        if shell_conn.server.type == "analytics":
            self.cbstatCmd = "%s%s" % (LinuxEnterpriseAnalytics.COUCHBASE_BIN_PATH, self.binaryName)
        else:
            self.cbstatCmd = "%s%s" % (Linux.COUCHBASE_BIN_PATH, self.binaryName)

        if ClusterRun.is_enabled:
            # Cluster run case
            target_dir = "kv_engine"
            if binary_name == "couchbase-cli":
                target_dir = "couchbase-cli"
            self.cbstatCmd = os.path.join(
                TestInputSingleton.input.servers[0].cli_path,
                "build", target_dir, self.binaryName)
        elif self.shellConn.info.type.lower() == 'windows':
            # Windows case
            self.cbstatCmd = "%s%s.exe" % (Windows.COUCHBASE_BIN_PATH,
                                           self.binaryName)
        elif self.shellConn.info.type.lower() == 'mac':
            # MacOS case
            self.cbstatCmd = "%s%s" % (Mac.COUCHBASE_BIN_PATH,
                                       self.binaryName)
        elif self.shellConn.server.ssh_username != "root":
            # Linux non-root case
            self.cbstatCmd = "%s%s" % (Linux.NONROOT_CB_BIN_PATH,
                                       self.binaryName)

    def disconnect(self):
        """
        Function with the same name (but dummy) is defined
        within the file cbstats_memcached.py
        """
        self.shellConn.disconnect()

    def _execute_cmd(self, cmd):
        """
        Executed the given command in the target shell
        Arguments:
        :cmd - Command to execute

        Returns:
        :output - Output for the command execution
        :error  - Buffer containing warnings/errors from the execution
        """
        return self.shellConn.execute_command(cmd)
