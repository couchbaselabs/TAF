import json

from BucketLib.bucket import Bucket
from cb_tools.cb_tools_base import CbCmdBase
from memcached.helper.data_helper import MemcachedClientHelper


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

    def bucket_details(self, server, bucket_name):
        client = MemcachedClientHelper.direct_client(
            server, Bucket({"name": bucket_name}), 30,
            self.username, self.password)
        buckets = json.loads(client.stats("bucket_details")[
                                 "bucket details"])["buckets"]
        for bucket in buckets:
            if bucket["name"] == bucket_name:
                return bucket
        return None

class Mcthrottle(CbCmdBase):
    def __init__(self, shell_conn, username="Administrator",
                 password="password"):
        CbCmdBase.__init__(self, shell_conn, "mcthrottlectl",
                           username=username, password=password)

    def set_throttle_limit(self, bucket, throttle_value=5000):
        cmd = "%s --user %s --password  %s --throttle-limit %s %s" \
              % (self.cbstatCmd, self.username, self.password,
                 throttle_value, bucket.name)
        output, error = self._execute_cmd(cmd)
        if error:
            raise Exception("".join(error))
        return output
