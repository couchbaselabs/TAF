import os
import re
import traceback

import Cb_constants
from SystemEventLogLib.Events import Event
from SystemEventLogLib.data_service_events import DataServiceEvents
from cb_basetest import CouchbaseBaseTest
from security_config import trust_all_certs

from datetime import datetime
from ruamel.yaml import YAML

from BucketLib.bucket import Bucket
from Cb_constants import ClusterRun, CbServer
from TestInput import TestInputSingleton
from membase.api.rest_client import RestConnection
from bucket_utils.bucket_ready_functions import BucketUtils
from cluster_utils.cluster_ready_functions import ClusterUtils, CBCluster
from remote.remote_util import RemoteMachineShellConnection

from constants.platform_constants import os_constants
from couchbase_utils.security_utils.x509_multiple_CA_util import x509main


class OnPremBaseTest(CouchbaseBaseTest):
    def setUp(self):
        super(OnPremBaseTest, self).setUp()

        # Framework specific parameters (Extension from cb_basetest)
        self.skip_cluster_reset = self.input.param("skip_cluster_reset", False)
        self.skip_setup_cleanup = self.input.param("skip_setup_cleanup", False)
        # End of framework parameters

        # Cluster level info settings
        self.log_info = self.input.param("log_info", None)
        self.log_location = self.input.param("log_location", None)
        self.stat_info = self.input.param("stat_info", None)
        self.port = self.input.param("port", None)
        self.port_info = self.input.param("port_info", None)
        self.servers = self.input.servers
        self.spare_nodes = list()
        self.num_servers = self.input.param("servers", len(self.servers))
        self.server_groups = self.input.param("server_groups",
                                              CbServer.default_server_group)
        self.vbuckets = self.input.param("vbuckets", None)
        self.gsi_type = self.input.param("gsi_type", 'plasma')
        # Memory quota settings
        # Max memory quota to utilize per node
        self.quota_percent = self.input.param("quota_percent", 100)
        # Services' RAM quota to set on cluster
        self.kv_mem_quota_percent = self.input.param("kv_quota_percent", None)
        self.index_mem_quota_percent = \
            self.input.param("index_quota_percent", None)
        self.fts_mem_quota_percent = \
            self.input.param("fts_quota_percent", None)
        self.cbas_mem_quota_percent = \
            self.input.param("cbas_quota_percent", None)
        self.eventing_mem_quota_percent = \
            self.input.param("eventing_quota_percent", None)
        # CBAS setting
        self.jre_path = self.input.param("jre_path", None)
        self.enable_dp = self.input.param("enable_dp", False)
        # End of cluster info parameters

        # Bucket specific params
        # Note: Over riding bucket_eviction_policy from CouchbaseBaseTest
        self.bucket_eviction_policy = \
            self.input.param("bucket_eviction_policy",
                             Bucket.EvictionPolicy.VALUE_ONLY)
        self.bucket_replica_index = self.input.param("bucket_replica_index",
                                                     1)
        if self.bucket_storage == Bucket.StorageBackend.magma:
            self.bucket_eviction_policy = Bucket.EvictionPolicy.FULL_EVICTION
        # End of bucket parameters

        self.services_in = self.input.param("services_in", None)
        self.forceEject = self.input.param("forceEject", False)
        self.wait_timeout = self.input.param("wait_timeout", 120)
        self.verify_unacked_bytes = \
            self.input.param("verify_unacked_bytes", False)
        self.disabled_consistent_view = \
            self.input.param("disabled_consistent_view", None)
        self.rebalanceIndexWaitingDisabled = \
            self.input.param("rebalanceIndexWaitingDisabled", None)
        self.rebalanceIndexPausingDisabled = \
            self.input.param("rebalanceIndexPausingDisabled", None)
        self.maxParallelIndexers = \
            self.input.param("maxParallelIndexers", None)
        self.maxParallelReplicaIndexers = \
            self.input.param("maxParallelReplicaIndexers", None)
        self.use_https = self.input.param("use_https", False)
        self.enforce_tls = self.input.param("enforce_tls", False)
        self.ipv4_only = self.input.param("ipv4_only", False)
        self.ipv6_only = self.input.param("ipv6_only", False)
        self.multiple_ca = self.input.param("multiple_ca", False)
        # This is user defined throttling limit used specifically in serverless config
        self.kv_throttling_limit = self.input.param("kv_throttling_limit", 200000)

        self.node_utils.cleanup_pcaps(self.servers)
        self.collect_pcaps = self.input.param("collect_pcaps", False)
        if self.collect_pcaps:
            self.node_utils.start_collect_pcaps(self.servers)

        '''
        Be careful while using this flag.
        This is only and only for stand-alone tests.
        During bugs reproductions, when a crash is seen
        stop_server_on_crash will stop the server
        so that we can collect data/logs/dumps at the right time
        '''
        self.stop_server_on_crash = self.input.param("stop_server_on_crash",
                                                     False)
        self.collect_data = self.input.param("collect_data", False)
        self.validate_system_event_logs = \
            self.input.param("validate_sys_event_logs", False)

        self.nonroot = False
        self.crash_warning = self.input.param("crash_warning", False)

        self.cluster_util = ClusterUtils(self.task_manager)
        self.bucket_util = BucketUtils(self.cluster_util, self.task)

        # Fetch the profile_type from the master node
        # Value will be default / serverless
        CbServer.cluster_profile = self.cluster_util.get_server_profile_type(
            self.servers[0])

        if self.use_https or CbServer.cluster_profile == "serverless":
            CbServer.use_https = True
            trust_all_certs()

        # Enable use_https and enforce_tls for 'serverless' cluster testing
        # And set default bucket/cluster setting values to tests
        if CbServer.cluster_profile == "serverless":
            self.use_https = True
            self.enforce_tls = True

            self.bucket_storage = Bucket.StorageBackend.magma
            self.num_replicas = Bucket.ReplicaNum.TWO
            self.server_groups = "test_zone_1:test_zone_2:test_zone_3"

        # Populate memcached_port in case of cluster_run
        cluster_run_base_port = ClusterRun.port
        if int(self.input.servers[0].port) == ClusterRun.port:
            for server in self.input.servers:
                server.port = cluster_run_base_port
                cluster_run_base_port += 1
                # If not defined in node.ini under 'memcached_port' section
                if server.memcached_port is CbServer.memcached_port:
                    server.memcached_port = \
                        ClusterRun.memcached_port \
                        + (2 * (int(server.port) - ClusterRun.port))

        self.log_setup_status(self.__class__.__name__, "started")
        cluster_name_format = "C%s"
        default_cluster_index = counter_index = 1
        num_vb = self.vbuckets or CbServer.total_vbuckets
        if len(self.input.clusters) > 1:
            # Multi cluster setup
            for _, nodes in self.input.clusters.iteritems():
                cluster_name = cluster_name_format % counter_index
                tem_cluster = CBCluster(name=cluster_name, servers=nodes,
                                        vbuckets=num_vb)
                self.cb_clusters[cluster_name] = tem_cluster
                counter_index += 1
        else:
            # Single cluster
            cluster_name = cluster_name_format % counter_index
            self.cb_clusters[cluster_name] = CBCluster(name=cluster_name,
                                                       servers=self.servers,
                                                       vbuckets=num_vb)

        # Initialize self.cluster with first available cluster as default
        self.cluster = self.cb_clusters[cluster_name_format
                                        % default_cluster_index]
        CbServer.enterprise_edition = \
            self.cluster_util.is_enterprise_edition(self.cluster)
        self.cluster.edition = "enterprise" \
            if CbServer.enterprise_edition else "community"

        if self.standard_buckets > 10:
            self.bucket_util.change_max_buckets(self.cluster.master,
                                                self.standard_buckets)

        for cluster_name, cluster in self.cb_clusters.items():
            # Append initial master node to the nodes_in_cluster list
            cluster.nodes_in_cluster.append(cluster.master)

            shell = RemoteMachineShellConnection(cluster.master)
            self.os_info = shell.extract_remote_info().type.lower()
            if self.os_info != 'windows':
                if cluster.master.ssh_username != "root":
                    self.nonroot = True
                    shell.disconnect()
                    break
            shell.disconnect()

        self.log_setup_status("OnPremBaseTest", "started")
        try:
            # Construct dict of mem. quota percent / mb per service
            mem_quota_percent = dict()
            # Construct dict of mem. quota percent per service
            if self.kv_mem_quota_percent:
                mem_quota_percent[CbServer.Services.KV] = \
                    self.kv_mem_quota_percent
            if self.index_mem_quota_percent:
                mem_quota_percent[CbServer.Services.INDEX] = \
                    self.index_mem_quota_percent
            if self.cbas_mem_quota_percent:
                mem_quota_percent[CbServer.Services.CBAS] = \
                    self.cbas_mem_quota_percent
            if self.fts_mem_quota_percent:
                mem_quota_percent[CbServer.Services.FTS] = \
                    self.fts_mem_quota_percent
            if self.eventing_mem_quota_percent:
                mem_quota_percent[CbServer.Services.EVENTING] = \
                    self.eventing_mem_quota_percent

            if not mem_quota_percent:
                mem_quota_percent = None

            if self.skip_setup_cleanup:
                # Update current server/service map and buckets for the cluster
                for _, cluster in self.cb_clusters.items():
                    self.cluster_util.update_cluster_nodes_service_list(
                        cluster)
                    cluster.buckets = self.bucket_util.get_all_buckets(cluster)
                    cluster.nodes_in_cluster = list(set(
                        cluster.kv_nodes + cluster.fts_nodes +
                        cluster.cbas_nodes + cluster.index_nodes +
                        cluster.query_nodes + cluster.eventing_nodes +
                        cluster.backup_nodes))
                return
            else:
                for cluster_name, cluster in self.cb_clusters.items():
                    self.log.info("Delete all buckets and rebalance out "
                                  "other nodes from '%s'" % cluster_name)
                    self.cluster_util.cluster_cleanup(cluster,
                                                      self.bucket_util)

            # avoid clean up if the previous test has been tear down
            if self.case_number == 1 or self.case_number > 1000:
                if self.case_number > 1000:
                    self.log.warn("TearDown for prev test failed. Will retry")
                    self.case_number -= 1000
                self.tearDownEverything(reset_cluster_env_vars=False)

            for cluster_name, cluster in self.cb_clusters.items():
                if not self.skip_cluster_reset:
                    self.initialize_cluster(
                        cluster_name, cluster, services=None,
                        services_mem_quota_percent=mem_quota_percent)

                # Update initial service map for the master node
                self.cluster_util.update_cluster_nodes_service_list(cluster)

                # Set this unconditionally
                RestConnection(cluster.master).set_internalSetting(
                    "magmaMinMemoryQuota", 256)

            # Enable dp_version since we need collections enabled
            if self.enable_dp:
                tasks = [self.node_utils.async_enable_dp(server)
                         for server in self.cluster.server]
                for task in tasks:
                    self.task_manager.get_task_result(task)

            # Enforce tls on nodes of all clusters
            if self.use_https and self.enforce_tls:
                for _, cluster in self.cb_clusters.items():
                    tasks = [self.node_utils.async_enable_tls(node)
                             for node in cluster.servers]
                    for task in tasks:
                        self.task_manager.get_task_result(task)
                    self.log.info("Validating if services obey tls only on servers {0}".
                                  format(cluster.servers))
                    status = self.cluster_util.check_if_services_obey_tls(
                        cluster.servers)
                    if not status:
                        self.fail("Services did not honor enforce tls")

            reload(Cb_constants)

            # Enforce IPv4 or IPv6 or both
            if self.ipv4_only or self.ipv6_only:
                for _, cluster in self.cb_clusters.items():
                    status, msg = self.cluster_util.enable_disable_ip_address_family_type(
                        cluster, True, self.ipv4_only, self.ipv6_only)
                    if not status:
                        self.fail(msg)

            self.standard = self.input.param("standard", "pkcs8")
            self.passphrase_type = self.input.param("passphrase_type", "script")
            self.encryption_type = self.input.param("encryption_type", "aes256")
            if self.multiple_ca:
                for _, cluster in self.cb_clusters.items():
                    cluster.x509 = x509main(
                        host=cluster.master, standard=self.standard,
                        encryption_type=self.encryption_type,
                        passphrase_type=self.passphrase_type)
                    self.generate_and_upload_cert(
                        cluster.servers, cluster.x509, upload_root_certs=True,
                        upload_node_certs=True, upload_client_certs=True)
                    payload = "name=cbadminbucket&roles=admin&password=password"
                    rest = RestConnection(cluster.master)
                    rest.add_set_builtin_user("cbadminbucket", payload)

            for cluster_name, cluster in self.cb_clusters.items():
                self.modify_cluster_settings(cluster)

            # Track test start time only if we need system log validation
            if self.validate_system_event_logs:
                self.system_events.set_test_start_time()
            self.log_setup_status("OnPremBaseTest", "finished")
            self.__log("started")
        except Exception as e:
            traceback.print_exc()
            self.task.shutdown(force=True)
            self.fail(e)
        finally:
            # Track test start time only if we need system log validation
            if self.validate_system_event_logs:
                self.system_events.set_test_start_time()

            self.log_setup_status("OnPremBaseTest", "finished")

    def initialize_cluster(self, cluster_name, cluster, services=None,
                           services_mem_quota_percent=None):
        self.log.info("Initializing cluster : {0}".format(cluster_name))
        self.node_utils.reset_cluster_nodes(self.cluster_util, cluster)
        if not services:
            master_services = self.cluster_util.get_services(
                cluster.servers[:1], self.services_init, start_node=0)
        else:
            master_services = self.cluster_util.get_services(
                cluster.servers[:1], services, start_node=0)
        if master_services is not None:
            master_services = master_services[0].split(",")

        self._initialize_nodes(
            self.task,
            cluster,
            self.disabled_consistent_view,
            self.rebalanceIndexWaitingDisabled,
            self.rebalanceIndexPausingDisabled,
            self.maxParallelIndexers,
            self.maxParallelReplicaIndexers,
            self.port,
            self.quota_percent,
            services=master_services,
            services_mem_quota_percent=services_mem_quota_percent)

        self.cluster_util.change_env_variables(cluster)
        self.cluster_util.change_checkpoint_params(cluster)
        self.log.info("Cluster %s initialized" % cluster_name)

    def modify_cluster_settings(self, cluster):
        if self.log_info:
            self.cluster_util.change_log_info(cluster,
                                              self.log_info)
        if self.log_location:
            self.cluster_util.change_log_location(cluster,
                                                  self.log_location)
        if self.stat_info:
            self.cluster_util.change_stat_info(cluster,
                                               self.stat_info)
        if self.port_info:
            self.cluster_util.change_port_info(cluster,
                                               self.port_info)
        if self.port:
            self.port = str(self.port)

    def start_fetch_pcaps(self):
        log_path = TestInputSingleton.input.param("logs_folder", "/tmp")
        is_test_failed = self.is_test_failed()
        self.node_utils.start_fetch_pcaps(self.servers, log_path, is_test_failed)

    def tearDown(self):
        # Perform system event log validation and get failures (if any)
        sys_event_validation_failure = None
        if self.validate_system_event_logs:
            sys_event_validation_failure = \
                self.system_events.validate(self.cluster.master)

        if self.ipv4_only or self.ipv6_only:
            for _, cluster in self.cb_clusters.items():
                self.cluster_util.enable_disable_ip_address_family_type(
                    cluster, False, self.ipv4_only, self.ipv6_only)

        # Disable n2n encryption on nodes of all clusters
        if self.use_https and self.enforce_tls:
            for _, cluster in self.cb_clusters.items():
                tasks = [self.node_utils.async_disable_tls(node)
                         for node in cluster.servers]
                for task in tasks:
                    self.task_manager.get_task_result(task)
        if self.multiple_ca:
            CbServer.use_https = False
            for _, cluster in self.cb_clusters.items():
                rest = RestConnection(cluster.master)
                rest.delete_builtin_user("cbadminbucket")
                x509 = x509main(host=cluster.master)
                x509.teardown_certs(servers=cluster.servers)
        if self.sdk_client_pool:
            self.sdk_client_pool.shutdown()
        if self.collect_pcaps:
            self.log.info("Starting Pcaps collection!!")
            self.start_fetch_pcaps()
        result = self.check_coredump_exist(self.servers, force_collect=True)
        if self.skip_teardown_cleanup:
            self.log.debug("Skipping tearDownEverything")
        else:
            self.tearDownEverything()
        if not self.crash_warning:
            self.assertFalse(result, msg="Cb_log file validation failed")
        if self.crash_warning and result:
            self.log.warn("CRASH | CRITICAL | WARN messages found in cb_logs")

        # Fail test in case of sys_event_logging failure
        if (not self.is_test_failed()) and sys_event_validation_failure:
            self.fail(sys_event_validation_failure)
        elif sys_event_validation_failure:
            self.log.critical("System event log validation failed: %s"
                              % sys_event_validation_failure)

        self.shutdown_task_manager()

    def tearDownEverything(self, reset_cluster_env_vars=True):
        for _, cluster in self.cb_clusters.items():
            try:
                test_failed = self.is_test_failed()
                if test_failed:
                    # Collect logs because we have not shut things down
                    if self.get_cbcollect_info:
                        self.fetch_cb_collect_logs()
                    get_trace = \
                        TestInputSingleton.input.param("get_trace", None)
                    if get_trace:
                        tasks = [
                            self.node_utils.async_get_trace(server, get_trace)
                            for server in cluster.servers]
                        for task in tasks:
                            self.task_manager.get_task_result(task)
                    else:
                        self.log.critical("Skipping get_trace !!")

                rest = RestConnection(cluster.master)
                alerts = rest.get_alerts()
                if alerts:
                    self.log.warn("Alerts found: {0}".format(alerts))
            except BaseException as e:
                # kill memcached
                traceback.print_exc()
                self.log.warning("Killing memcached due to {0}".format(e))
                self.cluster_util.kill_memcached(cluster)
                # Increase case_number to retry tearDown in setup for next test
                self.case_number += 1000
            finally:
                if reset_cluster_env_vars:
                    self.cluster_util.reset_env_variables(cluster)
        self.infra_log.info("========== tasks in thread pool ==========")
        self.task_manager.print_tasks_in_pool()
        self.infra_log.info("==========================================")

    def __log(self, status):
        try:
            msg = "{0}: {1} {2}" \
                .format(datetime.now(), self._testMethodName, status)
            RestConnection(self.servers[0]).log_client_error(msg)
        except Exception as e:
            self.log.warning("Exception during REST log_client_error: %s" % e)

    def _initialize_nodes(self, task, cluster, disabled_consistent_view=None,
                          rebalance_index_waiting_disabled=None,
                          rebalance_index_pausing_disabled=None,
                          max_parallel_indexers=None,
                          max_parallel_replica_indexers=None,
                          port=None, quota_percent=None, services=None,
                          services_mem_quota_percent=None):
        quota = 0
        init_tasks = list()
        ssh_sessions = dict()

        # Open ssh_connections for command execution
        for server in cluster.servers:
            ssh_sessions[server.ip] = RemoteMachineShellConnection(server)

        for server in cluster.servers:
            # Make sure that data_and index_path are writable by couchbase user
            if not server.index_path:
                server.index_path = server.data_path
            if not server.cbas_path:
                server.cbas_path = str([server.data_path])
            if not server.eventing_path:
                server.eventing_path = server.data_path
            for path in set([_f for _f in [server.data_path, server.index_path]
                             if _f]):
                for cmd in ("rm -rf {0}/*".format(path),
                            "chown -R couchbase:couchbase {0}".format(path)):
                    ssh_sessions[server.ip].execute_command(cmd)
                rest = RestConnection(server)
                rest.set_data_path(data_path=server.data_path,
                                   index_path=server.index_path,
                                   cbas_path=server.cbas_path)

            if cluster.master != server:
                continue

            init_port = port or server.port or '8091'
            assigned_services = services
            init_tasks.append(
                task.async_init_node(
                    server, disabled_consistent_view,
                    rebalance_index_waiting_disabled,
                    rebalance_index_pausing_disabled,
                    max_parallel_indexers,
                    max_parallel_replica_indexers, init_port,
                    quota_percent, services=assigned_services,
                    gsi_type=self.gsi_type,
                    services_mem_quota_percent=services_mem_quota_percent))
        for _task in init_tasks:
            node_quota = self.task_manager.get_task_result(_task)
            if node_quota < quota or quota == 0:
                quota = node_quota
        if quota < 100 and not len(set([server.ip
                                        for server in self.servers])) == 1:
            self.log.warn("RAM quota was defined less than 100 MB:")
            for server in cluster.servers:
                ram = ssh_sessions[server.ip].extract_remote_info().ram
                self.log.debug("Node: {0}: RAM: {1}".format(server.ip, ram))

        # Close all ssh_connections
        for server in cluster.servers:
            ssh_sessions[server.ip].disconnect()

        if self.jre_path:
            for server in cluster.servers:
                rest = RestConnection(server)
                rest.set_jre_path(self.jre_path)
        return quota

    def fetch_cb_collect_logs(self):
        log_path = TestInputSingleton.input.param("logs_folder", "/tmp")
        is_single_node_server = len(self.servers) == 1
        for _, cluster in self.cb_clusters.items():
            rest = RestConnection(cluster.master)
            nodes = rest.get_nodes(inactive=True)
            # Creating cluster_util object to handle multi_cluster scenario
            status = self.cluster_util.trigger_cb_collect_on_cluster(
                rest, nodes,
                is_single_node_server)

            if status is True:
                self.cluster_util.wait_for_cb_collect_to_complete(rest)
                self.cluster_util.copy_cb_collect_logs(rest, nodes, cluster,
                                                       log_path)
            else:
                self.log.error("API perform_cb_collect returned False")

    def check_coredump_exist(self, servers, force_collect=False):
        bin_cb = os_constants.Linux.COUCHBASE_BIN_PATH
        lib_cb = os_constants.Linux.COUCHBASE_LIB_PATH
        crash_dir_win = os_constants.Windows.COUCHBASE_CRASH_PATH_RAW
        result = False
        self.data_sets = dict()

        def find_index_of(str_list, sub_string):
            for i in range(len(str_list)):
                if sub_string in str_list[i]:
                    return i
            return -1

        def get_gdb(gdb_shell, dmp_path, dmp_name):
            dmp_file = dmp_path + dmp_name
            core_file = dmp_path + dmp_name.strip(".dmp") + ".core"
            gdb_shell.execute_command("rm -rf " + core_file)
            core_cmd = "/" + bin_cb + "minidump-2-core " + dmp_file + " > " + core_file
            print("running: %s" % core_cmd)
            gdb_shell.execute_command(core_cmd)
            gdb = "gdb --batch {} -c {} -ex \"bt full\" -ex quit"\
                .format(os.path.join(bin_cb, "memcached"), core_file)
            print("running: %s" % gdb)
            result = gdb_shell.execute_command(gdb)[0]
            t_index = find_index_of(result, "Core was generated by")
            result = result[t_index:]
            result = " ".join(result)
            return result

        def get_full_thread_dump(shell):
            cmd = 'gdb -p `(pidof memcached)` -ex "thread apply all bt" -ex detach -ex quit'
            print("running: %s" % cmd)
            thread_dump = shell.execute_command(cmd)[0]
            index = find_index_of(
                thread_dump, "Thread debugging using libthread_db enabled")
            result = " ".join(thread_dump[index:])
            return result

        def run_cbanalyze_core(shell, core_file):
            cbanalyze_core = os.path.join(bin_cb, "tools/cbanalyze-core")
            cmd = '%s %s' % (cbanalyze_core, core_file)
            print("running: %s" % cmd)
            shell.execute_command(cmd)[0]
            cbanalyze_log = core_file + ".log"
            if shell.file_exists(os.path.dirname(cbanalyze_log), os.path.basename(cbanalyze_log)):
                log_path = TestInputSingleton.input.param("logs_folder", "/tmp")
                shell.get_file(os.path.dirname(cbanalyze_log), os.path.basename(cbanalyze_log), log_path)

        def check_logs(grep_output_list):
            """
            Check the grep's last line for the latest timestamp.
            If this timestamp < start_timestamp of the test,
            then return False (as the grep's output is from previous tests)
            Note: This method works only if slave's time(timezone) matches
                  that of VM's. Else it won't be possible to compare timestamps
            """
            last_line = grep_output_list[-1]
            # eg: 2021-07-12T04:03:45
            timestamp_regex = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
            match_obj = timestamp_regex.search(last_line)
            if not match_obj:
                self.log.critical("%s does not match any timestamp" % last_line)
                return True
            timestamp = match_obj.group()
            timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")
            self.log.info("Comparing timestamps: Log's latest timestamp: %s, "
                          "Test's start timestamp is %s"
                          % (timestamp, self.start_timestamp))
            if timestamp > self.start_timestamp:
                return True
            else:
                return False

        for idx, server in enumerate(servers):
            shell = RemoteMachineShellConnection(server)
            shell.extract_remote_info()
            crash_dir = lib_cb + "crash/"
            if shell.info.type.lower() == "windows":
                crash_dir = crash_dir_win
            if int(server.port) in range(ClusterRun.port,
                                         ClusterRun.port + 10):
                crash_dir = os.path.join(
                    TestInputSingleton.input.servers[0].cli_path,
                    "ns_server", "data",
                    "n_%s" % str(idx), "crash")
            dmp_files = shell.execute_command("ls -lt " + crash_dir)[0]
            dmp_files = [f for f in dmp_files if ".core" not in f]
            dmp_files = [f for f in dmp_files if "total" not in f]
            dmp_files = [f.split()[-1] for f in dmp_files if ".core" not in f]
            dmp_files = [f.strip("\n") for f in dmp_files]
            if dmp_files:
                print("#"*30)
                print("%s: %d core dump seen" % (server.ip, len(dmp_files)))
                print("%s: Stack Trace of first crash - %s\n%s"
                      % (server.ip, dmp_files[-1],
                         get_gdb(shell, crash_dir, dmp_files[-1])))
                print("#"*30)
                result = get_full_thread_dump(shell)
                print(result)
#                 print("#"*30)
#                 run_cbanalyze_core(shell, crash_dir + dmp_files[-1].strip(".dmp") + ".core")
#                 print("#"*30)
                if self.stop_server_on_crash:
                    shell.stop_couchbase()
                result = True
            else:
                self.log.debug(server.ip + ": No crash files found")

            logs_dir = lib_cb + "logs/"
            if int(server.port) in range(ClusterRun.port,
                                         ClusterRun.port + 10):
                logs_dir = os.path.join(
                    TestInputSingleton.input.servers[0].cli_path,
                    "ns_server", "logs", "n_%s" % str(idx))

            # Perform log file searching based on the input yaml config
            yaml = YAML()
            with open("lib/couchbase_helper/error_log_config.yaml", "r") as fp:
                y_data = yaml.load(fp.read())

            for file_data in y_data["file_name_patterns"]:
                log_files = shell.execute_command(
                    "ls " + os.path.join(logs_dir, file_data['file']))[0]

                if len(log_files) == 0:
                    self.log.debug("%s: No '%s' files found"
                                   % (server.ip, file_data['file']))
                    continue

                if 'target_file_index' in file_data:
                    log_files = [
                        log_files[int(file_data['target_file_index'])]
                    ]

                for log_file in log_files:
                    log_file = log_file.strip("\n")
                    for grep_pattern in file_data['grep_for']:
                        grep_for_str = grep_pattern['string']
                        err_pattern = exclude_pattern = None
                        if 'error_patterns' in grep_pattern:
                            err_pattern = grep_pattern['error_patterns']
                        if 'exclude_patterns' in grep_pattern:
                            exclude_pattern = grep_pattern['exclude_patterns']

                        cmd_to_run = "grep -r '%s' %s" \
                                     % (grep_for_str, log_file)
                        if exclude_pattern is not None:
                            for pattern in exclude_pattern:
                                cmd_to_run += " | grep -v '%s'" % pattern

                        grep_output = shell.execute_command(cmd_to_run)[0]
                        if grep_output and check_logs(grep_output):
                            regex = r"(\bkvstore-\d+)"
                            grep_str = "".join(grep_output)
                            kvstores = list(set(re.findall(regex, grep_str)))
                            self.data_sets[server] = kvstores
                            grep_str = None
                        if err_pattern is not None:
                            for pattern in err_pattern:
                                index = find_index_of(grep_output, pattern)
                                grep_output = grep_output[:index]
                                if grep_output:
                                    self.log.info("unwanted messages in %s" %
                                                  log_file)
                                    if check_logs(grep_output):
                                        self.log.critical(
                                            "%s: Found '%s' logs - %s"
                                            % (server.ip, grep_for_str,
                                               "".join(grep_output)))
                                        result = True
                                        break
                        else:
                            if grep_output \
                                    and check_logs(grep_output):
                                self.log.info("unwanted messages in %s" %
                                              log_file)
                                self.log.critical("%s: Found '%s' logs - %s"
                                                  % (server.ip, grep_for_str,
                                                     grep_output))
                                result = True
                                break
                    if result is True:
                        if self.stop_server_on_crash:
                            shell.stop_couchbase()
                        break

            shell.disconnect()
        if result and force_collect and not self.stop_server_on_crash:
            self.fetch_cb_collect_logs()
            self.get_cbcollect_info = False
        if (self.is_test_failed() or result) and self.collect_data:
            self.copy_data_on_slave()

        return result

    def copy_data_on_slave(self, servers=None):
        log_path = TestInputSingleton.input.param("logs_folder", "/tmp")
        if servers is None:
            servers = self.cluster.nodes_in_cluster
            for node in servers:
                if "kv" not in node.services.lower():
                    servers.remove(node)
        if type(servers) is not list:
            servers = [servers]
        remote_path = RestConnection(servers[0]).get_data_path()
        file_path = os.path.join(remote_path, self.cluster.buckets[0].name)
        file_name = self.cluster.buckets[0].name + ".tar.gz"

        def get_tar(remotepath, filepath, filename, servers, todir="."):
            if type(servers) is not list:
                servers = [servers]
            for server in servers:
                shell = RemoteMachineShellConnection(server)
                _ = shell.execute_command("tar -zcvf %s.tar.gz %s" %
                                          (filepath, filepath))
                file_check = shell.file_exists(remotepath, filename)
                if not file_check:
                    self.log.error("Tar File {} doesn't exist".
                                   format(filename))
                tar_file_copied = shell.get_file(remotepath, filename, todir)
                if not tar_file_copied:
                    self.log.error("Failed to copy Tar file")

                _ = shell.execute_command("rm -rf %s.tar.gz" % filepath)

        copy_path_msg_format = "Copying data, Server :: %s, Path :: %s"
        '''
          Temporarily enabling data copy
          of all nodes irrespective of nodes in
          data_sets
        '''
        if False and self.data_sets and self.bucket_storage == "magma":
            self.log.critical("data_sets ==> {}".format(self.data_sets))
            wal_tar = "wal.tar.gz"
            config_json_tar = "config.json.tar.gz"
            for server, kvstores in self.data_sets.items():
                shell = RemoteMachineShellConnection(server)
                if not kvstores:
                    copy_to_path = os.path.join(log_path,
                                                server.ip.replace(".", "_"))
                    if not os.path.isdir(copy_to_path):
                        os.makedirs(copy_to_path, 0o777)
                    self.log.info(copy_path_msg_format % (server.ip,
                                                          copy_to_path))
                    get_tar(remote_path, file_path, file_name,
                            server, todir=copy_to_path)
                else:
                    num_vb = self.vbuckets or CbServer.total_vbuckets
                    for kvstore in kvstores:
                        if int(kvstore.split("-")[1]) >= num_vb:
                            continue
                        kvstore_path = shell.execute_command(
                            "find %s -type d -name '%s'" % (remote_path,
                                                            kvstore))[0][0]
                        magma_dir = kvstore_path.split(kvstore)[0]
                        wal_path = kvstore_path.split(kvstore)[0] + "wal"
                        config_json_path = kvstore_path.split(kvstore)[0]
                        + "config.json"
                        kvstore_path = kvstore_path.split(kvstore)[0] + kvstore
                        kvstore_tar = kvstore + ".tar.gz"
                        copy_to_path = os.path.join(log_path, kvstore)
                        if not os.path.isdir(copy_to_path):
                            os.makedirs(copy_to_path, 0o777)
                        self.log.info(copy_path_msg_format % (server.ip,
                                                              copy_to_path))
                        get_tar(magma_dir, kvstore_path, kvstore_tar,
                                server, todir=copy_to_path)
                        get_tar(magma_dir, wal_path, wal_tar,
                                server, todir=copy_to_path)
                        get_tar(magma_dir, config_json_path, config_json_tar,
                                server, todir=copy_to_path)
        else:
            for server in servers:
                copy_to_path = os.path.join(log_path,
                                            server.ip.replace(".", "_"))
                if not os.path.isdir(copy_to_path):
                    os.makedirs(copy_to_path, 0o777)
                self.log.info(copy_path_msg_format % (server.ip, copy_to_path))
                get_tar(remote_path, file_path, file_name,
                        server, todir=copy_to_path)

    def generate_and_upload_cert(
            self, servers, x509, generate_certs=True, delete_inbox_folder=True,
            upload_root_certs=True, upload_node_certs=True,
            delete_out_of_the_box_CAs=True, upload_client_certs=True):

        if generate_certs:
            x509.generate_multiple_x509_certs(servers=servers)

        if delete_inbox_folder:
            for server in servers:
                x509.delete_inbox_folder_on_server(server=server)

        if upload_root_certs:
            for server in servers:
                _ = x509.upload_root_certs(server)

        if upload_node_certs:
            x509.upload_node_certs(servers=servers)

        if delete_out_of_the_box_CAs:
            for node in servers:
                x509.delete_unused_out_of_the_box_CAs(server=node)

        if upload_client_certs:
            x509.upload_client_cert_settings(server=servers[0])


class ClusterSetup(OnPremBaseTest):
    def setUp(self):
        super(ClusterSetup, self).setUp()

        if self.skip_setup_cleanup:
            self.cluster_util.print_cluster_stats(self.cluster)
            return

        self.log_setup_status("ClusterSetup", "started", "setup")
        self.__initial_rebalance()
        # Add basic RBAC users
        self.bucket_util.add_rbac_user(self.cluster.master)

        # Print cluster stats
        self.cluster_util.print_cluster_stats(self.cluster)
        self.log_setup_status("ClusterSetup", "complete", "setup")

    def tearDown(self):
        super(ClusterSetup, self).tearDown()

    def __initial_rebalance(self):
        services = None
        if self.services_init:
            services = list()
            for service in self.services_init.split("-"):
                services.append(service.replace(":", ","))
        services = services[1:] \
            if services is not None and len(services) > 1 else None

        # Rebalance-in nodes_init servers
        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        if nodes_init:
            result = self.task.rebalance(self.cluster, nodes_init, [],
                                         services=services,
                                         add_nodes_server_groups=None)
            if result is False:
                # Need this block since cb-collect won't be collected
                # in BaseTest if failure happens during setup() stage
                if self.get_cbcollect_info:
                    self.fetch_cb_collect_logs()
                self.fail("Initial rebalance failed")

        if CbServer.cluster_profile == "serverless":
            # Workaround to hitting throttling on serverless config
            _, status = RestConnection(self.cluster.master).set_throttle_limit(limit=self.kv_throttling_limit)

        # Used to track spare nodes.
        # Test case can use this for further rebalance
        self.spare_nodes = self.servers[self.nodes_init:]

    def create_bucket(self, cluster, bucket_name="default"):
        create_bucket_params = {
            "cluster": cluster,
            "bucket_type": self.bucket_type,
            "ram_quota": self.bucket_size,
            "replica": self.num_replicas,
            "maxTTL": self.bucket_ttl,
            "compression_mode": self.compression_mode,
            "wait_for_warmup": True,
            "conflict_resolution": Bucket.ConflictResolution.SEQ_NO,
            "replica_index": self.bucket_replica_index,
            "storage": self.bucket_storage,
            "eviction_policy": self.bucket_eviction_policy,
            "flush_enabled": self.flush_enabled,
            "bucket_durability": self.bucket_durability_level,
            "purge_interval": self.bucket_purge_interval,
            "autoCompactionDefined": "false",
            "fragmentation_percentage": 50,
            "bucket_name": bucket_name,
            "width": self.bucket_width,
            "weight": self.bucket_weight
        }

        # This is needed because server will throw the error saying,
        # "Support for variable number of vbuckets is not enabled"
        if CbServer.cluster_profile == "serverless":
            create_bucket_params["vbuckets"] = self.vbuckets

        self.bucket_util.create_default_bucket(**create_bucket_params)

        # Add bucket create event in system event log
        if self.system_events.test_start_time is not None:
            bucket = self.bucket_util.get_all_buckets(self.cluster)[0]
            eviction_policy_val = "full_eviction"
            if self.bucket_eviction_policy \
                    == Bucket.EvictionPolicy.VALUE_ONLY:
                eviction_policy_val = "value_only"
            elif self.bucket_eviction_policy \
                    == Bucket.EvictionPolicy.NO_EVICTION:
                eviction_policy_val = "no_eviction"
            bucket_create_event = DataServiceEvents.bucket_create(
                self.cluster.master.ip, self.bucket_type,
                bucket.name, bucket.uuid,
                {'compression_mode': self.compression_mode,
                 'max_ttl': self.bucket_ttl,
                 'storage_mode': self.bucket_storage,
                 'conflict_resolution_type': Bucket.ConflictResolution.SEQ_NO,
                 'eviction_policy': eviction_policy_val,
                 'purge_interval': 'undefined',
                 'durability_min_level': self.bucket_durability_level,
                 'num_replicas': self.num_replicas})
            if self.bucket_type == Bucket.Type.EPHEMERAL:
                bucket_create_event[Event.Fields.EXTRA_ATTRS][
                    'bucket_props']['storage_mode'] = Bucket.Type.EPHEMERAL
                bucket_create_event[Event.Fields.EXTRA_ATTRS][
                    'bucket_props'].pop('purge_interval', None)
                bucket_create_event[Event.Fields.EXTRA_ATTRS][
                    'bucket_props']['eviction_policy'] = "no_eviction"
            self.system_events.add_event(bucket_create_event)
