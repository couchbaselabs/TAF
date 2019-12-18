from couchbase_helper import cb_constants
from testconstants import \
    LINUX_COUCHBASE_BIN_PATH, LINUX_NONROOT_CB_BIN_PATH, \
    WIN_COUCHBASE_BIN_PATH, MAC_COUCHBASE_BIN_PATH


class CbCmdBase:
    def __init__(self, shell_conn, binary_name,
                 port=cb_constants.memcached_port,
                 username="Administrator", password="password"):

        self.shellConn = shell_conn
        self.port = port
        self.username = username
        self.password = password

        self.binaryName = binary_name
        self.cbstatCmd = "%s%s" % (LINUX_COUCHBASE_BIN_PATH, self.binaryName)

        if self.shellConn.username != "root":
            self.cbstatCmd = "%s%s" % (LINUX_NONROOT_CB_BIN_PATH,
                                       self.binaryName)

        if self.shellConn.extract_remote_info().type.lower() == 'windows':
            self.cbstatCmd = "%s%s.exe" % (WIN_COUCHBASE_BIN_PATH,
                                           self.binaryName)
        elif self.shellConn.extract_remote_info().type.lower() == 'mac':
            self.cbstatCmd = "%s%s" % (MAC_COUCHBASE_BIN_PATH,
                                       self.binaryName)

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
