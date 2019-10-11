import os, shutil, ast, re, subprocess
from basetestcase import BaseTestCase
from cluster_utils.cluster_ready_functions import ClusterUtils, CBCluster
from bucket_utils.bucket_ready_functions import BucketUtils
from BucketLib.bucket import Bucket
from membase.api.rest_client import RestHelper, RestConnection, \
                                    Bucket as CBBucket
from remote.remote_util import RemoteMachineShellConnection
from testconstants import LINUX_COUCHBASE_BIN_PATH,\
                          COUCHBASE_DATA_PATH, WIN_COUCHBASE_DATA_PATH_RAW,\
                          WIN_COUCHBASE_BIN_PATH_RAW, WIN_COUCHBASE_BIN_PATH, WIN_TMP_PATH_RAW,\
                          MAC_COUCHBASE_BIN_PATH, LINUX_ROOT_PATH, WIN_ROOT_PATH,\
                          WIN_TMP_PATH, STANDARD_BUCKET_PORT, WIN_CYGWIN_BIN_PATH
from testconstants import INDEX_QUOTA, FTS_QUOTA, COUCHBASE_FROM_MAD_HATTER,\
                          CLUSTER_QUOTA_RATIO

class EnterpriseBKRSNewBaseTest(BaseTestCase):

    def setUp(self):
        super(EnterpriseBKRSNewBaseTest, self).setUp()
        self.clusters = self.get_clusters()
        self.master = self.servers[0]
        self.task = self.get_task()
        self.taskmgr = self.get_task_mgr()

        self.backupset = Backupset()
        self.cmd_ext = ""
        self.should_fail = self.input.param("should-fail", False)
        self.restore_should_fail = self.input.param("restore_should_fail", False)
        self.merge_should_fail = self.input.param("merge_should_fail", False)
        self.database_path = COUCHBASE_DATA_PATH

        cmd =  'curl -g {0}:8091/diag/eval -u {1}:{2} '.format(self.master.ip,
                                                              self.master.rest_username,
                                                              self.master.rest_password)
        cmd += '-d "path_config:component_path(bin)."'
        shell = RemoteMachineShellConnection(self.master)
        output, error = shell.enable_diag_eval_on_non_local_hosts()
        shell.disconnect()

        bin_path  = subprocess.check_output(cmd, shell=True)
        if not self.skip_init_check_cbserver:
            if "bin" not in bin_path:
                self.fail("Check if cb server install on %s" % self.master.ip)
            else:
                self.cli_command_location = bin_path.replace('"','') + "/"

        self.debug_logs = self.input.param("debug-logs", False)
        self.backupset.directory = self.input.param("dir", "/tmp/entbackup")
        self.backupset.user_env = self.input.param("user-env", False)
        self.backupset.passwd_env = self.input.param("passwd-env", False)
        self.backupset.log_archive_env = self.input.param("log-archive-env", False)
        self.backupset.log_redaction = self.input.param("log-redaction", False)
        self.backupset.redaction_salt = self.input.param("redaction-salt", None)
        self.backupset.no_log_output_flag = self.input.param("no-log-output-flag", False)
        self.backupset.ex_logs_path = self.input.param("ex-logs-path", None)
        self.backupset.overwrite_user_env = self.input.param("overwrite-user-env", False)
        self.backupset.overwrite_passwd_env = self.input.param("overwrite-passwd-env", False)
        self.backupset.disable_conf_res_restriction = self.input.param("disable-conf-res-restriction", None)
        self.backupset.force_updates = self.input.param("force-updates", False)
        self.backupset.resume = self.input.param("resume", False)
        self.backupset.purge = self.input.param("purge", False)
        self.backupset.start = self.input.param("start", 1)
        self.backupset.end = self.input.param("stop", 1)
        self.backupset.number_of_backups = self.input.param("number_of_backups", 1)
        self.replace_ttl_with = self.input.param("replace-ttl-with", None)
        self.backupset.backup_host = self.servers[-1]
        self.backupset.name = self.input.param("name", "backup")
        self.backupset.filter_keys = self.input.param("filter-keys", "")
        self.backupset.random_keys = self.input.param("random_keys", False)
        self.backupset.filter_values = self.input.param("filter-values", "")
        self.backupset.no_ssl_verify = self.input.param("no-ssl-verify", False)
        self.backupset.secure_conn = self.input.param("secure-conn", False)
        self.backupset.bk_no_cert = self.input.param("bk-no-cert", False)
        self.backupset.rt_no_cert = self.input.param("rt-no-cert", False)
        self.backupset.backup_list_name = self.input.param("list-names", None)
        self.backupset.backup_incr_backup = self.input.param("incr-backup", None)
        self.backupset.bucket_backup = self.input.param("bucket-backup", None)

        shell = RemoteMachineShellConnection(self.servers[0])
        info = shell.extract_remote_info().type.lower()
        self.root_path = LINUX_ROOT_PATH
        self.wget = "wget"
        self.os_name = "linux"
        self.tmp_path = "/tmp/"
        self.long_help_flag = "--help"
        self.short_help_flag = "-h"
        self.cygwin_bin_path = ""
        self.enable_firewal = False
        self.rfc3339_date = "date +%s --date='{0} seconds' | ".format(self.replace_ttl_with) + \
                                "xargs -I {} date --date='@{}' --rfc-3339=seconds | "\
                                "sed 's/ /T/'"
        self.seconds_with_ttl = "date +%s --date='{0} seconds'".format(self.replace_ttl_with)
        if info == 'linux':
            if self.nonroot:
                base_path = "/home/{0}".format(self.master.ssh_username)
                self.database_path = "{0}{1}".format(base_path, COUCHBASE_DATA_PATH)
                self.root_path = "/home/{0}/".format(self.master.ssh_username)
        elif info == 'windows':
            self.os_name = "windows"
            self.cmd_ext = ".exe"
            self.wget = "/cygdrive/c/automation/wget.exe"
            self.database_path = WIN_COUCHBASE_DATA_PATH_RAW
            self.root_path = WIN_ROOT_PATH
            self.tmp_path = WIN_TMP_PATH
            self.long_help_flag = "help"
            self.short_help_flag = "h"
            self.cygwin_bin_path = WIN_CYGWIN_BIN_PATH
            self.rfc3339_date = "date +%s --date='{0} seconds' | ".format(self.replace_ttl_with) + \
                            "{0}xargs -I {{}} date --date=\"@'{{}}'\" --rfc-3339=seconds | "\
                                                            .format(self.cygwin_bin_path) + \
                                                                               "sed 's/ /T/'"
            win_format = "C:/Program Files"
            cygwin_format = "/cygdrive/c/Program\ Files"
            if win_format in self.cli_command_location:
                self.cli_command_location = self.cli_command_location.replace(win_format,
                                                                              cygwin_format)
            self.backupset.directory = self.input.param("dir", WIN_TMP_PATH_RAW + "entbackup")
        elif info == 'mac':
            self.backupset.directory = self.input.param("dir", "/tmp/entbackup")
        else:
            raise Exception("OS not supported.")


        self.non_master_host = self.input.param("non-master", False)
        self.value_size = self.input.param("value_size", 512)
        self.no_progress_bar = self.input.param("no-progress-bar", True)
        self.multi_threads = self.input.param("multi_threads", False)
        self.threads_count = self.input.param("threads_count", 1)
        self.bucket_delete = self.input.param("bucket_delete", False)
        self.bucket_flush = self.input.param("bucket_flush", False)
        self.commit = self.input.param("commit", True)
        self.ops_type = self.input.param("ops_type", "create")
        self.num_threads = self.input.param("num_threads", 5)
        self.bk_with_ttl = self.input.param("bk-with-ttl", None)
        self.create_fts_index = self.input.param("create-fts-index", False)
        self.reset_restore_cluster = self.input.param("reset-restore-cluster", False)
        self.backupset.user_env_with_prompt = \
                        self.input.param("user-env-with-prompt", False)
        self.backupset.passwd_env_with_prompt = \
                        self.input.param("passwd-env-with-prompt", False)
        self.restore_compression_mode = self.input.param("restore-compression-mode", None)
        self.force_version_upgrade = self.input.param("force-version-upgrade", None)
        self.skip_buckets = self.input.param("skip_buckets", False)
        self.num_replicas = self.input.param("replicas", 2)
        self.restore_only = self.input.param("restore-only", False)

        if self.non_master_host:
            self.backupset.cluster_host = self.servers[1]
            self.backupset.cluster_host_username = self.servers[1].rest_username
            self.backupset.cluster_host_password = self.servers[1].rest_password
        else:
            self.backupset.cluster_host = self.servers[0]
            self.backupset.cluster_host_username = self.servers[0].rest_username
            self.backupset.cluster_host_password = self.servers[0].rest_password

        self.same_cluster = self.input.param("same-cluster", False)
        if self.same_cluster:
            self.backupset.restore_cluster_host = self.input.clusters[0][0]
            self.backupset.restore_cluster_host_username = self.input.clusters[0][0].rest_username
            self.backupset.restore_cluster_host_password = self.input.clusters[0][0].rest_password
        else:
            self.backupset.restore_cluster_host = self.input.clusters[1][0]
            self.backupset.restore_cluster_host_username = self.input.clusters[1][0].rest_username
            self.backupset.restore_cluster_host_password = self.input.clusters[1][0].rest_password
        """ new user to test RBAC """
        self.cluster_new_user = self.input.param("new_user", None)
        if self.cluster_new_user:
            self.backupset.cluster_host_username = self.cluster_new_user
            self.backupset.restore_cluster_host_username = self.cluster_new_user
        self.backups = []
        self.number_of_backups_taken = 0
        for cluster in self.clusters:
            cluster_util = ClusterUtils(cluster, self.taskmgr)
            cluster_util.add_all_nodes_then_rebalance(cluster.servers[1:])

    def tearDown(self):
        super(EnterpriseBKRSNewBaseTest, self).tearDown()

    def get_cb_cluster_by_name(self, name):
        """Return couchbase cluster object for given name.
        @return: CBCluster object
        """
        for cluster in self.clusters:
            if cluster.name == name:
                cluster.cluster_util = ClusterUtils(cluster, self.task_manager)
                cluster.bucket_util = BucketUtils(cluster, cluster.cluster_util,
                                      self.task)
                return cluster
        raise Exception("Couchbase Cluster with name: {0} not exist".format(name))

    def _create_index(self, server):
        query_check_index_exists = "SELECT COUNT(*) FROM system:indexes " \
                                   "WHERE name=`" + self.bucket_name + "_index`"
        if not self._execute_query(server, query_check_index_exists):
            self._execute_query(server, "CREATE PRIMARY INDEX `" + self.bucket_name + "_index` "
                                 + "ON `" + self.bucket_name + '`')

    def _execute_query(self, server, query):
        try:
            results = RestConnection(server).query_tool(query)
            if "COUNT" in query:
                return (int(results["results"][0]['$1']))
            elif results:
                return results
            else:
                return False
        except Exception as e:
            self.fail(
                "Errors encountered while executing query {0} on {1} : {2}"\
                .format(query, server, e.message))

    def get_keys_with_txn_commit(self, server, bucket_name):
        query =  'select raw a1.id from '
        query += '(select raw OBJECT_INNER_VALUES(meta().xattrs.attempts)[0].ins '
        query += 'from {0} '.format(bucket_name)
        query += 'where OBJECT_INNER_VALUES(meta().xattrs.attempts)[0].st = "COMPLETED") '
        query += 'as a  unnest a as a1;'
        results = self._execute_query(server, query)
        if results:
            return results
        else:
            self.fail("Fail to get keys with txn commit")

    def verify_txn_in_bkrs_bucket(self):
        self.log.info("Create index at bkrs cluster")
        self._create_index(self.bk_cluster.master)
        self._create_index(self.rs_cluster.master)
        self.sleep(7)

        query = 'SELECT count(*) FROM default where META(default).id like "bkrs%";'
        bk_docs = self.num_items
        doc_name = "bkrs"
        if self.commit:
            if self.ops_type == "delete":
                bk_docs -= self.num_items / 4
        else:
            bk_docs = 0
        bk_docs = int(self._execute_query(self.bk_cluster.master, query)["results"][0]['$1'])
        rs_docs = int(self._execute_query(self.rs_cluster.master, query)["results"][0]['$1'])
        if rs_docs != bk_docs:
            self.fail("restore docs does not match with backup doc")

    def backup_create(self, del_old_backup=True):
        args = "config --archive {0} --repo {1}".format(self.backupset.directory, self.backupset.name)
        if self.backupset.exclude_buckets:
            args += " --exclude-buckets \"{0}\"".format(",".join(self.backupset.exclude_buckets))
        if self.backupset.include_buckets:
            args += " --include-buckets \"{0}\"".format(",".join(self.backupset.include_buckets))
        if self.backupset.disable_bucket_config:
            args += " --disable-bucket-config"
        if self.backupset.disable_views:
            args += " --disable-views"
        if self.backupset.disable_gsi_indexes:
            args += " --disable-gsi-indexes"
        if self.backupset.disable_ft_indexes:
            args += " --disable-ft-indexes"
        if self.backupset.disable_data:
            args += " --disable-data"

        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, args)
        if del_old_backup:
            self.log.info("Remove any old dir before create new one")
            remote_client.execute_command("rm -rf %s" % self.backupset.directory)
        output, error = remote_client.execute_command(command)
        remote_client.log_command_output(output, error)
        remote_client.disconnect()
        return output, error

    def backup_cluster(self, threads_count=1):
        url_format = ""
        secure_port = ""
        if self.backupset.secure_conn:
            cacert = self.get_cluster_certificate_info(self.backupset.backup_host,
                                                       self.backupset.cluster_host)
            secure_port = "1"
            url_format = "s"

        user_input = "--username %s " % self.backupset.cluster_host_username
        password_input = "--password %s " % self.backupset.cluster_host_password
        if self.backupset.user_env and not self.backupset.overwrite_user_env:
            user_input = ""
        elif self.backupset.user_env_with_prompt:
            password_input = "-u "

        if self.backupset.passwd_env and not self.backupset.overwrite_passwd_env:
            password_input = ""
        elif self.backupset.passwd_env_with_prompt:
            password_input = "-p "

        """ Print out of cbbackupmgr from 6.5 is different with older version """
        self.cbbkmgr_version = "6.5"
        self.bk_printout = "Backup successfully completed".split(",")
        if RestHelper(RestConnection(self.backupset.backup_host)).is_ns_server_running():
            self.cbbkmgr_version = RestConnection(self.backupset.backup_host).get_nodes_version()

        if "4.6" <= self.cbbkmgr_version[:3]:
            self.cluster_flag = "--cluster"

        args = "backup --archive {0} --repo {1} {6} http{7}://{2}:{8}{3} "\
                   "{4} {5}".format(self.backupset.directory, self.backupset.name,
                   self.backupset.cluster_host.ip,
                   self.backupset.cluster_host.port,
                   user_input,
                   password_input,
                   self.cluster_flag, url_format,
                   secure_port)
        if self.backupset.no_ssl_verify:
            args += " --no-ssl-verify"
        if self.backupset.secure_conn:
            if not self.backupset.bk_no_cert:
                args += " --cacert %s" % cacert
        if self.backupset.resume:
            args += " --resume"
        if self.backupset.purge:
            args += " --purge"
        if self.no_progress_bar:
            args += " --no-progress-bar"
        if self.multi_threads:
            args += " --threads %s " % threads_count
        if self.backupset.backup_compressed:
            args += " --value-compression compressed"
        user_env = ""
        password_env = ""
        if self.backupset.user_env:
            self.log.info("set user env to Administrator")
            user_env = "export CB_USERNAME=Administrator; "
        if self.backupset.passwd_env:
            self.log.info("set password env to password")
            password_env = "export CB_PASSWORD=password; "
        if self.backupset.user_env_with_prompt:
            self.log.info("set username env to prompt")
            user_env = "unset CB_USERNAME; export CB_USERNAME;"
        if self.backupset.passwd_env_with_prompt:
            self.log.info("set password env to prompt")
            password_env = "unset CB_PASSWORD; export CB_PASSWORD;"
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "{3} {2} {0}/cbbackupmgr {1}".format(self.cli_command_location, args,
                                                   password_env, user_env)
        output, error = remote_client.execute_command(command)
        if self.debug_logs:
            remote_client.log_command_output(output, error)

        command = "ls -tr {0}/{1} | tail -1".format(self.backupset.directory, self.backupset.name)
        o, e = remote_client.execute_command(command)
        if o:
            self.backups.append(o[0])
        self.number_of_backups_taken += 1
        self.log.info("Finished taking backup  with args: {0}".format(args))
        remote_client.disconnect()
        return output, error

    def backup_reset_clusters(self, servers):
        servers.delete_all_buckets()
        BucketOperationHelper.delete_all_buckets_or_assert(servers, self)
        ClusterOperationHelper.cleanup_cluster(servers, master=servers[0])
        ClusterOperationHelper.wait_for_ns_servers_or_assert(servers, self)

    def cluster_to_restore(self):
        return self.get_nodes_in_cluster(self.backupset.restore_cluster_host)

    def get_nodes_in_cluster(self, master_node=None):
        rest = None
        if master_node == None:
            rest = RestConnection(self.master)
        else:
            rest = RestConnection(master_node)
        nodes = rest.node_statuses()
        server_set = []
        for node in nodes:
            for server in self.servers:
                if server.ip == node.ip:
                    server_set.append(server)
        return server_set

    def backup_restore(self):
        if self.restore_only:
            if self.create_fts_index:
                self.backups.append("2017-05-18T13_40_30.842368123-07_00")
            else:
                self.backups.append("2017-05-18T11_55_22.009680763-07_00")
        try:
            backup_start = self.backups[int(self.backupset.start) - 1]
        except IndexError:
            backup_start = "{0}{1}".format(self.backups[-1], self.backupset.start)
        try:
            backup_end = self.backups[int(self.backupset.end) - 1]
        except IndexError:
            backup_end = "{0}{1}".format(self.backups[-1], self.backupset.end)
        url_format = ""
        secure_port = ""
        if self.backupset.secure_conn:
            cacert = self.get_cluster_certificate_info(self.backupset.backup_host,
                                                       self.backupset.restore_cluster_host)
            url_format = "s"
            secure_port = "1"

        user_input = "--username {0} ".format(self.backupset.restore_cluster_host_username)
        password_input = "--password {0} ".format(self.backupset.restore_cluster_host_password)
        if self.backupset.user_env and not self.backupset.overwrite_user_env:
            user_input = ""
        elif self.backupset.user_env_with_prompt:
            user_input = "-u "
        if self.backupset.passwd_env and not self.backupset.overwrite_passwd_env:
            password_input = ""
        elif self.backupset.passwd_env_with_prompt:
            password_input = "-p "

        if "4.6" <= RestConnection(self.backupset.backup_host).get_nodes_version():
            self.cluster_flag = "--cluster"

        args = "restore --archive {0} --repo {1} {2} http{9}://{3}:{10}{4} "\
               "{5} {6} --start {7} --end {8}" \
                               .format(self.backupset.directory,
                                       self.backupset.name,
                                       self.cluster_flag,
                                       self.backupset.restore_cluster_host.ip,
                                       self.backupset.restore_cluster_host.port,
                                       user_input,
                                       password_input,
                                       backup_start.rstrip(), backup_end.rstrip(),
                                       url_format, secure_port)
        if self.backupset.no_ssl_verify:
            args += " --no-ssl-verify"
        if self.backupset.secure_conn:
            if not self.backupset.rt_no_cert:
                args += " --cacert %s" % cacert
        if self.backupset.exclude_buckets:
            args += " --exclude-buckets {0}".format(self.backupset.exclude_buckets)
        if self.backupset.include_buckets:
            args += " --include-buckets {0}".format(self.backupset.include_buckets)
        if self.backupset.disable_bucket_config:
            args += " --disable-bucket-config {0}".format(self.backupset.disable_bucket_config)
        if self.backupset.disable_views:
            args += " --disable-views "
        if self.backupset.disable_gsi_indexes:
            args += " --disable-gsi-indexes {0}".format(self.backupset.disable_gsi_indexes)
        if self.backupset.disable_ft_indexes:
            args += " --disable-ft-indexes {0}".format(self.backupset.disable_ft_indexes)
        if self.backupset.disable_data:
            args += " --disable-data {0}".format(self.backupset.disable_data)
        if self.backupset.disable_conf_res_restriction is not None:
            args += " --disable-conf-res-restriction {0}".format(
                self.backupset.disable_conf_res_restriction)
        filter_chars = {"star": "*", "dot": "."}
        if self.backupset.filter_keys:
            for key in filter_chars:
                if key in self.backupset.filter_keys:
                    self.backupset.filter_keys = self.backupset.filter_keys.replace(key,
                                                                      filter_chars[key])
            args += " --filter-keys '{0}'".format(self.backupset.filter_keys)
        if self.backupset.filter_values:
            for key in filter_chars:
                if key in self.backupset.filter_values:
                    self.backupset.filter_values = self.backupset.filter_values.replace(key,
                                                                          filter_chars[key])
            args += " --filter-values '{0}'".format(self.backupset.filter_values)
        if self.backupset.force_updates:
            args += " --force-updates"
        if self.no_progress_bar:
            args += " --no-progress-bar"
        bucket_compression_mode = self.compression_mode
        if self.restore_compression_mode is not None:
            bucket_compression_mode = self.restore_compression_mode
        user_env = ""
        password_env = ""
        if self.backupset.user_env:
            self.log.info("set user env to Administrator")
            user_env = "export CB_USERNAME=Administrator; "
        if self.backupset.passwd_env:
            self.log.info("set password env to password")
            password_env = "export CB_PASSWORD=password; "
        if self.backupset.user_env_with_prompt:
            self.log.info("set username env to prompt")
            user_env = "unset CB_USERNAME; export CB_USERNAME;"
        if self.backupset.passwd_env_with_prompt:
            self.log.info("set password env to prompt")
            password_env = "unset CB_PASSWORD; export CB_PASSWORD;"
        shell = RemoteMachineShellConnection(self.backupset.backup_host)

        command = "{3} {2} {0}/cbbackupmgr {1}".format(self.cli_command_location, args,
                                                   password_env, user_env)
        self.log.info("\nrestore command: {0}".format(command))
        output, error = shell.execute_command(command)
        shell.log_command_output(output, error)
        errors_check = ["Unable to process value for", "Error restoring cluster",
                        "Expected argument for option"]
        if "Error restoring cluster" in output[0] or "Unable to process value" in output[0] \
            or "Expected argument for option" in output[0]:
            if not self.should_fail:
                if not self.restore_should_fail:
                    self.fail("Failed to restore cluster")
            else:
                self.log.info("This test is for negative test")
        res = output
        res.extend(error)
        error_str = "Error restoring cluster: Transfer failed. "\
                    "Check the logs for more information."
        if error_str in res:
            bk_log_file_name = "backup.log"
            if "6.5" <= RestConnection(self.backupset.backup_host).get_nodes_version():
                bk_log_file_name = "backup-*.log"
            command = "cat " + self.backupset.directory + \
                      "/logs/{0} | grep '".format(bk_log_file_name) + \
                      error_str + "' -A 10 -B 100"
            output, error = shell.execute_command(command)
            shell.log_command_output(output, error)
        if 'Required Flags:' in res:
            if not self.should_fail:
                self.fail("Command line failed. Please check test params.")
        shell.disconnect()
        return output, error

    def get_cluster_certificate_info(self, server_host, server_cert):
        """
            This will get certificate info from cluster
        """
        cert_file_location = self.root_path + "cert.pem"
        if self.os_name == "windows":
            cert_file_location = WIN_TMP_PATH_RAW + "cert.pem"
        shell = RemoteMachineShellConnection(server_host)
        cmd = "%s/couchbase-cli ssl-manage -c %s:8091 -u Administrator -p password "\
              " --cluster-cert-info > %s" % (self.cli_command_location,
                                                     server_cert.ip,
                                                     cert_file_location)
        output, _ = shell.execute_command(cmd)
        if output and "Error" in output[0]:
            self.fail("Failed to get CA certificate from cluster.")
        shell.disconnect()
        return cert_file_location

    def backup_list(self):
        args = "list --archive {0}".format(self.backupset.directory)
        if self.backupset.backup_list_name:
            args += " --repo {0}".format(self.backupset.backup_list_name)
        if self.backupset.backup_incr_backup:
            args += " --backup {0}".format(self.backupset.backup_incr_backup)
        if self.backupset.bucket_backup:
            args += " --bucket {0}".format(self.backupset.bucket_backup)
        remote_client = RemoteMachineShellConnection(self.backupset.backup_host)
        command = "{0}/cbbackupmgr {1}".format(self.cli_command_location, args)
        output, error = remote_client.execute_command(command, debug=True)
        remote_client.log_command_output(output, error)
        remote_client.disconnect()
        if error:
            return False, error, "Getting backup list failed."
        else:
            return True, output, "Backup list obtained"

class Backupset:
    def __init__(self):
        self.backup_host = None
        self.directory = ''
        self.name = ''
        self.cluster_host = None
        self.cluster_host_username = ''
        self.cluster_host_password = ''
        self.cluster_new_user = None
        self.cluster_new_role = None
        self.restore_cluster_host = None
        self.restore_cluster_host_username = ''
        self.restore_cluster_host_password = ''
        self.threads = ''
        self.exclude_buckets = []
        self.include_buckets = []
        self.disable_bucket_config = False
        self.disable_views = False
        self.disable_gsi_indexes = False
        self.disable_ft_indexes = False
        self.disable_data = False
        self.disable_conf_res_restriction = False
        self.force_updates = False
        self.resume = False
        self.purge = False
        self.start = 1
        self.end = 1
        self.number_of_backups = 1
        self.number_of_backups_after_upgrade = 1
        self.filter_keys = ''
        self.filter_values = ''
        self.no_ssl_verify = False
        self.random_keys = False
        self.secure_conn = False
        self.bk_no_cert = False
        self.rt_no_cert = False
        self.backup_list_name = ''
        self.backup_incr_backup = ''
        self.bucket_backup = ''
        self.backup_to_compact = ''
        self.map_buckets = None
        self.delete_old_bucket = False
        self.backup_compressed = False
        self.user_env = False
        self.log_archive_env = False
        self.no_log_output_flag = False
        self.ex_logs_path = None
        self.log_redaction = None
        self.redaction_salt = None
        self.passwd_env = False
        self.overwrite_user_env = False
        self.overwrite_passwd_env = False
        self.user_env_with_prompt = False
        self.passwd_env_with_prompt = False
        self.deleted_buckets = []
        self.new_buckets = []
        self.flushed_buckets = []
        self.deleted_backups = []

