import json

from BucketLib.bucket import Bucket
from cb_constants.CBServer import CbServer
from cb_constants.ClusterRun import ClusterRun
from cb_tools.cb_tools_base import CbCmdBase


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
        if self.port == CbServer.ssl_port:
            return CbServer.port
        elif ClusterRun.is_enabled and self.port > ClusterRun.ssl_port:
            return self.port - 10000
        return self.port

    def cluster_init(self, data_ramsize, index_ramsize, fts_ramsize, services,
                     index_storage_mode, cluster_name,
                     cluster_username, cluster_password, cluster_port):
        cmd = "%s cluster-init -c localhost:%s -u %s -p %s" \
              % (self.cbstatCmd, self.__get_http_port(),
                 self.username, self.password)
        if cluster_username:
            cmd += " --cluster-username " + str(cluster_username)
        if cluster_password:
            cmd += " --cluster-password " + str(cluster_password)
        if data_ramsize:
            cmd += " --cluster-ramsize " + str(data_ramsize)
        if index_ramsize:
            cmd += " --cluster-index-ramsize " + str(index_ramsize)
        if fts_ramsize:
            cmd += " --cluster-fts-ramsize " + str(fts_ramsize)
        if cluster_name:
            cmd += " --cluster-name " + str(cluster_name)
        if index_storage_mode:
            cmd += " --index-storage-setting " + str(index_storage_mode)
        if cluster_port:
            cmd += " --cluster-port " + str(cluster_port)
        if services:
            cmd += " --services " + str(services)
        return self._execute_cmd(cmd)

    def cluster_settings(self, data_ramsize, index_ramsize, fts_ramsize,
                         cluster_name, cluster_username,
                         cluster_password, cluster_port):
        cmd = "%s setting-cluster -c localhost:%s -u %s -p %s" \
              % (self.cbstatCmd, self.__get_http_port(),
                 self.username, self.password)
        if cluster_username is not None:
            cmd += " --cluster-username " + str(cluster_username)
        if cluster_password is not None:
            cmd += " --cluster-password " + str(cluster_password)
        if data_ramsize:
            cmd += " --cluster-ramsize " + str(data_ramsize)
        if index_ramsize:
            cmd += " --cluster-index-ramsize " + str(index_ramsize)
        if fts_ramsize:
            cmd += " --cluster-fts-ramsize " + str(fts_ramsize)
        if cluster_name:
            if cluster_name == "empty":
                cluster_name = " "
            cmd += " --cluster-name " + str(cluster_name)
        if cluster_port:
            cmd += " --cluster-port " + str(cluster_port)
        return self._execute_cmd(cmd)

    def add_node(self, server, service):
        """
        Add nodes to the cluster with given service
        """
        server = "https://{}:{}".format(server.ip, CbServer.ssl_port)
        cluster_ip = "{}:{}".format(self.shellConn.ip, self.port)
        no_ssl_verify_flag = ""
        if CbServer.use_https:
            cluster_ip = "https://{}".format(cluster_ip)
            no_ssl_verify_flag = "--no-ssl-verify"
        cmd = "{0} server-add -c {1} -u {2} -p {3} --server-add {4} " \
              "--server-add-username {2} --server-add-password {3} " \
              "--services {5} {6}" \
              .format(self.cbstatCmd, cluster_ip,
                      self.username, self.password,
                      server, service,
                      no_ssl_verify_flag)
        output, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception(str(error))
        return output

    def create_bucket(self, bucket_dict, wait=False):
        """
        Cli bucket-create command support
        :param bucket_dict: Dict with key,values mapping to cb-cli options
        :param wait: If True, cli bucket-create will wait till bucket
                     creation completes
        :return:
        """
        host = "localhost"
        no_ssl_verify_str = ""
        if CbServer.use_https:
            host = "https://localhost"
            no_ssl_verify_str = " --no-ssl-verify"

        cmd = "%s bucket-create -c %s:%s -u %s -p %s %s" \
              % (self.cbstatCmd, host, self.port,
                 self.username, self.password, no_ssl_verify_str)
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
            elif key == Bucket.historyRetentionCollectionDefault:
                option = "--enable-history-retention-by-default"
            elif key == Bucket.historyRetentionBytes:
                option = "--history-retention-bytes"
            elif key == Bucket.historyRetentionSeconds:
                option = "--history-retention-seconds"
            elif key == Bucket.rank:
                option = "--rank"

            if option:
                cmd += " %s %s " % (option, value)

        output, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception(str(error))
        return output

    def delete_bucket(self, bucket_name):
        host = "localhost"
        if CbServer.use_https:
            host = "https://localhost"
        cmd = "%s bucket-delete -c %s:%s -u %s -p %s --bucket %s" \
              % (self.cbstatCmd, host, self.port,
                 self.username, self.password, bucket_name)
        cmd += self.cli_flags
        output, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception(str(error))
        return output

    def edit_bucket(self, bucket_name, **params):
        host = "localhost"
        no_ssl_verify_str = ""
        if CbServer.use_https:
            host = "https://localhost"
            no_ssl_verify_str = " --no-ssl-verify"
        cmd = "{0} bucket-edit -c {1}:{2} -u {3} -p {4} --bucket {5} {6}" \
              .format(self.cbstatCmd, host, self.port, self.username,
                      self.password, bucket_name, no_ssl_verify_str)

        for key, value in params.items():
            if key == Bucket.ramQuotaMB:
                cmd += " --bucket-ramsize " + str(value)
            elif key == Bucket.evictionPolicy:
                cmd += " --bucket-eviction-policy " + value
            elif key == Bucket.replicaNumber:
                cmd += " --bucket-replica " + str(value)
            elif key == Bucket.priority:
                cmd += " --bucket-priority " + value
            elif key == Bucket.flushEnabled:
                cmd += " --enable-flush " + str(value)
            elif key == Bucket.rank:
                cmd += " --rank " + str(value)
            elif key == Bucket.compressionMode:
                cmd += " --compression-mode " + str(value)

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

    def auto_failover(self, enable_auto_fo=1,
                      fo_timeout=None, max_failovers=None,
                      disk_fo=None, disk_fo_timeout=None,
                      can_abort_rebalance=None):
        cmd = "%s setting-autofailover -c localhost:%s -u %s -p %s" \
              % (self.cbstatCmd, self.__get_http_port(),
                 self.username, self.password)
        cmd += " --enable-auto-failover %s" % enable_auto_fo
        if fo_timeout:
            cmd += " --auto-failover-timeout %s" % fo_timeout
        if max_failovers:
            cmd += " --max-failovers %s" % max_failovers
        if disk_fo:
            cmd += " --enable-failover-on-data-disk-issues %s" % disk_fo
        if disk_fo_timeout:
            cmd += " --failover-data-disk-period %s" % disk_fo_timeout
        if can_abort_rebalance:
            cmd += " --can-abort-rebalance %s" % can_abort_rebalance
        output, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception("\n".join(error))
        return output

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
        cmd = "%s node-to-node-encryption -c %s:%s -u %s -p %s " \
              "--disable" \
              % (self.cbstatCmd, "localhost", self.__get_http_port(),
                 self.username, self.password)
        cmd += self.cli_flags

        output, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception(str(error))
        return output

    def set_n2n_encryption_level(self, level="all"):
        cmd = "%s setting-security -c %s:%s -u %s -p %s --set " \
              "--cluster-encryption-level %s" \
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
        if "WARNING" in output[0]:
            json_acceptable_string = output[1].replace("'", "\"")
        else:
            json_acceptable_string = output[0].replace("'", "\"")
        security_dict = json.loads(json_acceptable_string)
        if "clusterEncryptionLevel" in security_dict:
            return security_dict["clusterEncryptionLevel"]
        else:
            return None
