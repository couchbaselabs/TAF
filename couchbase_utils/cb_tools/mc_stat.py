from cb_tools.cb_tools_base import CbCmdBase


class McStat(CbCmdBase):
    def __init__(self, shell_conn, username="Administrator",
                 password="password"):
        CbCmdBase.__init__(self, shell_conn, "mcstat",
                           username=username, password=password)

    def reset(self, bucket_name):
        """
        Resets mcstat for the specified bucket_name
        :param bucket_name: Bucket name to reset stat
        """
        cmd = "%s -h localhost:%s -u %s -P %s -b %s reset" \
              % (self.cbstatCmd, self.mc_port, self.username, self.password,
                 bucket_name)
        _, error = self._execute_cmd(cmd)
        if error:
            raise Exception("".join(error))

    def get_tenants_stat(self, bucket_name):
        cmd = "%s -h localhost:%s -u %s -P %s -b %s tenants" \
              % (self.cbstatCmd, self.mc_port, self.username, self.password,
                 bucket_name)
        output, error = self._execute_cmd(cmd)
        if error:
            raise Exception("".join(error))
        return output

    def get_user_stat(self, bucket_name, user):
        # 'tenants {\"domain\":\"local\",\"user\":\"%s\"}'
        cmd = "%s -h localhost:%s -u %s -P %s -b %s tenants" \
              % (self.cbstatCmd, self.mc_port, user.username, user.password,
                 bucket_name)
        output, error = self._execute_cmd(cmd)
        if error:
            raise Exception("{0}".format(error))
        return output
