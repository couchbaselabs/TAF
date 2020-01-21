from Cb_constants import CbServer
from cb_tools.cb_tools_base import CbCmdBase


class CbCli(CbCmdBase):
    def __init__(self, shell_conn, username="Administrator",
                 password="password"):
        CbCmdBase.__init__(self, shell_conn, "couchbase-cli",
                           username=username, password=password)

    def enable_dp(self):
        """
        Method to enable developer-preview

        Raise:
        Exception(if any) during command execution
        """
        cmd = "echo 'y' | %s enable-developer-preview --enable " \
              "-c %s:%s -u %s -p %s" \
              % (self.cbstatCmd, self.shellConn.ip, self.port,
                 self.username, self.password)
        output, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception("\n".join(error))
        if "SUCCESS: Cluster is in developer preview mode" not in str(output):
            raise Exception("Expected output not seen: %s" % output)
