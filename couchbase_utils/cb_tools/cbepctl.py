from cb_tools.cb_tools_base import CbCmdBase


class Cbepctl(CbCmdBase):
    def __init__(self, shell_conn, port=11210, username="Administrator",
                 password="password"):
        CbCmdBase.__init__(self, shell_conn, "cbepctl", port=port,
                           username=username, password=password)

    def persistence(self, bucket_name, action):
        """
        To control the persistence on the target node.
        Uses command,
          cbepctl localhost:port -b 'bucket_name' 'action'

        Arguments:
        :action - Persistence command to execute. start/stop (Str)

        Returns:
        :None

        Raise:
        :Exception - Warnings/Errors (if any) during the command execution
        """
        cmd = "%s localhost:%s -u %s -p %s -b %s %s" \
              % (self.cbstatCmd, self.port, self.username, self.password,
                 bucket_name, action.lower())
        _, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise("\n".join(error))
