from cb_tools.cb_tools_base import CbCmdBase
from couchbase_helper import cb_constants


class Cbepctl(CbCmdBase):
    def __init__(self, shell_conn, port=cb_constants.memcached_port,
                 username="Administrator", password="password"):
        CbCmdBase.__init__(self, shell_conn, "cbepctl", port=port,
                           username=username, password=password)

    def set(self, bucket_name, set_type, key, value):
        """
        Generic method to set values using cbepctl set command.
        Uses command,
          cbepctl localhost:port -b 'bucket_name' set 'set_type' 'key' 'value'

        :param bucket_name: Target bucket_name
        :param set_type: Target to set. Example: flush_param
        :param key: Param to set for 'set_type'. Example: exp_pager_stime
        :param value: Value for the given 'key'
        :return:

        Raise:
        :Exception - Warnings/Errors (if any) during the command execution
        """
        cmd = "%s localhost:%s -u %s -p %s -b %s set %s %s %s" \
              % (self.cbstatCmd, self.port, self.username, self.password,
                 bucket_name, set_type, key, value)
        _, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception("\n".join(error))

    def persistence(self, bucket_name, action):
        """
        To control the persistence on the target node.
        Uses command,
          cbepctl localhost:port -b 'bucket_name' 'action'

        :param bucket_name: Target bucket_name
        :param action: Persistence command to execute. start/stop (Str)
        :return:

        Raise:
        :Exception - Warnings/Errors (if any) during the command execution
        """
        cmd = "%s localhost:%s -u %s -p %s -b %s %s" \
              % (self.cbstatCmd, self.port, self.username, self.password,
                 bucket_name, action.lower())
        _, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception("\n".join(error))
