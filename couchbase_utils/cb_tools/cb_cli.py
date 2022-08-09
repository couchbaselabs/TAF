import json

from BucketLib.bucket import Bucket
from cb_tools.cb_tools_base import CbCmdBase
from Cb_constants import CbServer


class CbCli(CbCmdBase):
    def __init__(self, shell_conn, username="Administrator",
                 password="password", no_ssl_verify=None):
        CbCmdBase.__init__(self, shell_conn, "couchbase-cli",
                           username=username, password=password)
        if no_ssl_verify is None:
            no_ssl_verify = CbServer.use_https
        self.cli_flags = ""
        if no_ssl_verify:
            self.cli_flags += " --no-ssl-verify"

    def __get_http_port(self):
        port = self.port
        if self.port == str(CbServer.ssl_port):
            port = str(CbServer.port)
        return port

    def create_bucket(self, bucket_dict, wait=False):
        """
        Cli bucket-create command support
        :param bucket_dict: Dict with key,values mapping to cb-cli options
        :param wait: If True, cli bucket-create will wait till bucket
                     creation completes
        :return:
        """
        cmd = "%s bucket-create -c %s:%s -u %s -p %s" \
              % (self.cbstatCmd, "localhost", self.port,
                 self.username, self.password)
        if wait:
            cmd += " --wait"
        for key, value in bucket_dict.items():
            option = None
            if key == Bucket.name:
                option = "--bucket"
            elif key == Bucket.bucketType:
                option = "--bucket-type"
            elif key == Bucket.durabilityMinLevel:
                option = "--durability-min-level"
            elif key == Bucket.storageBackend:
                option = "--storage-backend"
            elif key == Bucket.ramQuotaMB:
                option = "--bucket-ramsize"
            elif key == Bucket.replicaNumber:
                option = "--bucket-replica"
            elif key == Bucket.priority:
                option = "--bucket-priority"
            elif key == Bucket.evictionPolicy:
                option = "--bucket-eviction-policy"
            elif key == Bucket.maxTTL:
                option = "--max-ttl"
            elif key == Bucket.compressionMode:
                option = "--compression-mode"
            elif key == Bucket.flushEnabled:
                option = "--enable-flush"
            elif key == Bucket.replicaIndex:
                option = "--enable-index-replica"
            elif key == Bucket.conflictResolutionType:
                option = "--conflict-resolution"

            if option:
                cmd += " %s %s " % (option, value)

        output, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception(str(error))
        return output

    def delete_bucket(self, bucket_name):
        cmd = "%s bucket-delete -c %s:%s -u %s -p %s --bucket %s" \
              % (self.cbstatCmd, "localhost", self.port,
                 self.username, self.password, bucket_name)
        cmd += self.cli_flags
        output, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception(str(error))
        return output

    def enable_dp(self):
        """
        Method to enable developer-preview

        Raise:
        Exception(if any) during command execution
        """
        cmd = "echo 'y' | %s enable-developer-preview --enable " \
              "-c %s:%s -u %s -p %s" \
              % (self.cbstatCmd, "localhost", self.port,
                 self.username, self.password)
        cmd += self.cli_flags
        output, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception("\n".join(error))
        if "SUCCESS: Cluster is in developer preview mode" not in str(output):
            raise Exception("Expected output not seen: %s" % output)

    def enable_n2n_encryption(self):
        cmd = "%s node-to-node-encryption -c %s:%s -u %s -p %s --enable" \
             % (self.cbstatCmd, "localhost", self.__get_http_port(),
                self.username, self.password)
        cmd += self.cli_flags
        output, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception(str(error))
        return output

    def disable_n2n_encryption(self):
        cmd = "%s node-to-node-encryption -c %s:%s -u %s -p %s --disable" \
              % (self.cbstatCmd, "localhost", self.__get_http_port(),
                 self.username, self.password)
        cmd += self.cli_flags

        output, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception(str(error))
        return output

    def set_n2n_encryption_level(self, level="all"):
        cmd = "%s setting-security -c %s:%s -u %s -p %s --set --cluster-encryption-level %s" \
              % (self.cbstatCmd, "localhost", self.__get_http_port(),
                 self.username, self.password, level)
        cmd += self.cli_flags
        output, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception(str(error))
        return output

    def get_n2n_encryption_level(self):
        cmd = "%s setting-security -c %s:%s -u %s -p %s --get" \
              % (self.cbstatCmd, "localhost", self.__get_http_port(),
                 self.username, self.password)
        cmd += self.cli_flags
        output, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception(str(error))
        json_acceptable_string = output[0].replace("'", "\"")
        security_dict = json.loads(json_acceptable_string)
        if "clusterEncryptionLevel" in security_dict:
            return security_dict["clusterEncryptionLevel"]
        else:
            return None
