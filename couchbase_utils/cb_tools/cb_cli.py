from BucketLib.bucket import Bucket
from cb_tools.cb_tools_base import CbCmdBase


class CbCli(CbCmdBase):
    def __init__(self, shell_conn, username="Administrator",
                 password="password"):
        CbCmdBase.__init__(self, shell_conn, "couchbase-cli",
                           username=username, password=password)

    def create_bucket(self, bucket_dict, wait=False):
        """
        Cli bucket-create command support
        :param bucket_dict: Dict with key,values mapping to cb-cli options
        :param wait: If True, cli bucket-create will wait till bucket
                     creation completes
        :return:
        """
        cmd = "%s bucket-create -c %s:%s -u %s -p %s" \
              % (self.cbstatCmd, self.shellConn.ip, self.port,
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
              % (self.cbstatCmd, self.shellConn.ip, self.port,
                 self.username, self.password, bucket_name)
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
              % (self.cbstatCmd, self.shellConn.ip, self.port,
                 self.username, self.password)
        output, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception("\n".join(error))
        if "SUCCESS: Cluster is in developer preview mode" not in str(output):
            raise Exception("Expected output not seen: %s" % output)
