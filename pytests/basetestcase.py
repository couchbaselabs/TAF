import os
import re
import traceback
import unittest

from datetime import datetime
from ruamel.yaml import YAML

from BucketLib.bucket import Bucket
from Cb_constants import ClusterRun, CbServer
from cb_tools.cb_cli import CbCli
from common_lib import sleep
from couchbase_helper.cluster import ServerTasks
from TestInput import TestInputSingleton
from global_vars import logger
from couchbase_helper.durability_helper import BucketDurability
from membase.api.rest_client import RestHelper, RestConnection
from bucket_utils.bucket_ready_functions import BucketUtils, DocLoaderUtils
from cluster_utils.cluster_ready_functions import ClusterUtils, CBCluster
from remote.remote_util import RemoteMachineShellConnection
from Jython_tasks.task_manager import TaskManager
from sdk_client3 import SDKClientPool
from test_summary import TestSummary


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        self.input = TestInputSingleton.input

        # Framework specific parameters
        self.log_level = self.input.param("log_level", "info").upper()
        self.infra_log_level = self.input.param("infra_log_level",
                                                "error").upper()
        self.skip_setup_cleanup = self.input.param("skip_setup_cleanup", False)
        self.tear_down_while_setup = self.input.param("tear_down_while_setup",
                                                      True)
        self.test_timeout = self.input.param("test_timeout", 3600)
        self.thread_to_use = self.input.param("threads_to_use", 30)
        self.case_number = self.input.param("case_number", 0)
        # End of framework parameters

        # Cluster level info settings
        self.log_info = self.input.param("log_info", None)
        self.log_location = self.input.param("log_location", None)
        self.stat_info = self.input.param("stat_info", None)
        self.port = self.input.param("port", None)
        self.port_info = self.input.param("port_info", None)
        self.servers = self.input.servers
        self.__cb_clusters = []
        self.num_servers = self.input.param("servers", len(self.servers))
        self.primary_index_created = False
        self.index_quota_percent = self.input.param("index_quota_percent",
                                                    None)
        self.gsi_type = self.input.param("gsi_type", 'plasma')
        # CBAS setting
        self.jre_path = self.input.param("jre_path", None)
        self.enable_dp = self.input.param("enable_dp", False)
        # End of cluster info parameters

        # Bucket specific params
        self.bucket_type = self.input.param("bucket_type",
                                            Bucket.Type.MEMBASE)
        self.bucket_size = self.input.param("bucket_size", None)
        self.bucket_conflict_resolution_type = \
            self.input.param("bucket_conflict_resolution",
                             Bucket.ConflictResolution.SEQ_NO)
        self.bucket_replica_index = self.input.param("bucket_replica_index",
                                                     1)
        self.bucket_eviction_policy = \
            self.input.param("bucket_eviction_policy",
                             Bucket.EvictionPolicy.VALUE_ONLY)
        self.flush_enabled = self.input.param("flushEnabled",
                                              Bucket.FlushBucket.DISABLED)
        self.bucket_time_sync = self.input.param("bucket_time_sync", False)
        self.standard_buckets = self.input.param("standard_buckets", 1)
        self.num_replicas = self.input.param("replicas",
                                             Bucket.ReplicaNum.ONE)
        self.active_resident_threshold = \
            int(self.input.param("active_resident_threshold", 100))
        self.compression_mode = \
            self.input.param("compression_mode",
                             Bucket.CompressionMode.PASSIVE)
        self.bucket_storage = \
            self.input.param("bucket_storage",
                             Bucket.StorageBackend.couchstore)
        if self.bucket_storage == Bucket.StorageBackend.magma:
            self.bucket_eviction_policy = Bucket.EvictionPolicy.FULL_EVICTION

        self.scope_name = self.input.param("scope", CbServer.default_scope)
        self.collection_name = self.input.param("collection",
                                                CbServer.default_collection)
        self.bucket_durability_level = self.input.param(
            "bucket_durability", Bucket.DurabilityLevel.NONE).upper()
        self.bucket_purge_interval = self.input.param("bucket_purge_interval",
                                                      1)
        self.bucket_durability_level = \
            BucketDurability[self.bucket_durability_level]
        # End of bucket parameters

        # Doc specific params
        self.key = self.input.param("key", "test_docs")
        self.key_size = self.input.param("key_size", 8)
        self.doc_size = self.input.param("doc_size", 256)
        self.sub_doc_size = self.input.param("sub_doc_size", 10)
        self.doc_type = self.input.param("doc_type", "json")
        self.num_items = self.input.param("num_items", 100000)
        self.target_vbucket = self.input.param("target_vbucket", None)
        self.maxttl = self.input.param("maxttl", 0)
        self.random_exp = self.input.param("random_exp", False)
        self.randomize_doc_size = self.input.param("randomize_doc_size", False)
        self.randomize_value = self.input.param("randomize_value", False)
        self.rev_write = self.input.param("rev_write", False)
        self.rev_read = self.input.param("rev_read", False)
        self.rev_update = self.input.param("rev_update", False)
        self.rev_del = self.input.param("rev_del", False)
        self.random_key = self.input.param("random_key", False)
        self.mix_key_size = self.input.param("mix_key_size", False)
        # End of doc specific parameters

        # Transactions parameters
        self.transaction_timeout = self.input.param("transaction_timeout", 100)
        self.transaction_commit = self.input.param("transaction_commit", True)
        self.update_count = self.input.param("update_count", 1)
        self.sync = self.input.param("sync", True)
        self.default_bucket = self.input.param("default_bucket", True)
        self.num_buckets = self.input.param("num_buckets", 0)
        self.atomicity = self.input.param("atomicity", False)
        self.defer = self.input.param("defer", False)
        # end of transaction parameters

        # Client specific params
        self.sdk_client_type = self.input.param("sdk_client_type", "java")
        self.replicate_to = self.input.param("replicate_to", 0)
        self.persist_to = self.input.param("persist_to", 0)
        self.sdk_retries = self.input.param("sdk_retries", 5)
        self.sdk_timeout = self.input.param("sdk_timeout", 5)
        self.durability_level = self.input.param("durability", "").upper()
        self.sdk_client_pool = self.input.param("sdk_client_pool", None)
        self.sdk_pool_capacity = self.input.param("sdk_pool_capacity", 1)
        # Client compression settings
        self.sdk_compression = self.input.param("sdk_compression", None)
        compression_min_ratio = self.input.param("min_ratio", None)
        compression_min_size = self.input.param("min_size", None)
        if type(self.sdk_compression) is bool:
            self.sdk_compression = {"enabled": self.sdk_compression}
            if compression_min_size:
                self.sdk_compression["minSize"] = compression_min_size
            if compression_min_ratio:
                self.sdk_compression["minRatio"] = compression_min_ratio

        # Doc Loader Params
        self.process_concurrency = self.input.param("process_concurrency", 20)
        self.batch_size = self.input.param("batch_size", 2000)
        self.ryow = self.input.param("ryow", False)
        self.check_persistence = self.input.param("check_persistence", False)
        # End of client specific parameters

        # initial number of items in the cluster
        self.services_init = self.input.param("services_init", None)
        self.nodes_init = self.input.param("nodes_init", 1)
        self.nodes_in = self.input.param("nodes_in", 1)
        self.nodes_out = self.input.param("nodes_out", 1)
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
        self.quota_percent = self.input.param("quota_percent", 90)
        self.skip_buckets_handle = self.input.param("skip_buckets_handle",
                                                    False)

        # SDKClientPool object for creating generic clients across tasks
        if self.sdk_client_pool is True:
            self.init_sdk_pool_object()

        # Initiate logging variables
        self.log = logger.get("test")
        self.infra_log = logger.get("infra")

        self.cleanup_pcaps()
        self.collect_pcaps = self.input.param("collect_pcaps", False)
        if self.collect_pcaps:
            self.start_collect_pcaps()

        # variable for log collection using cbCollect
        self.get_cbcollect_info = self.input.param("get-cbcollect-info", False)

        # Variable for initializing the current (start of test) timestamp
        self.start_timestamp = datetime.now()

        '''
        Be careful while using this flag.
        This is only and only for stand-alone tests.
        During bugs reproductions, when a crash is seen
        stop_server_on_crash will stop the server
        so that we can collect data/logs/dumps at the right time
        '''
        self.stop_server_on_crash = self.input.param("stop_server_on_crash",
                                                     False)

        # Configure loggers
        self.log.setLevel(self.log_level)
        self.infra_log.setLevel(self.infra_log_level)

        # Support lib objects for testcase execution
        self.task_manager = TaskManager(self.thread_to_use)
        self.task = ServerTasks(self.task_manager)
        # End of library object creation

        self.sleep = sleep

        self.cleanup = False
        self.nonroot = False
        self.test_failure = None
        self.crash_warning = self.input.param("crash_warning", False)
        self.summary = TestSummary(self.log)

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

        self.log_setup_status("BaseTestCase", "started")
        if len(self.input.clusters) > 1:
            # Multi cluster setup
            counter = 1
            for _, nodes in self.input.clusters.iteritems():
                self.__cb_clusters.append(CBCluster(name="C%s" % counter,
                                                    servers=nodes))
                counter += 1
        else:
            # Single cluster
            self.cluster = CBCluster(servers=self.servers)
            self.__cb_clusters.append(self.cluster)
            self.cluster_util = ClusterUtils(self.cluster, self.task_manager)

            self.bucket_util = BucketUtils(self.cluster, self.cluster_util,
                                           self.task)

        if self.standard_buckets > 10:
            self.bucket_util.change_max_buckets(self.standard_buckets)

        for cluster in self.__cb_clusters:
            shell = RemoteMachineShellConnection(cluster.master)
            self.os_info = shell.extract_remote_info().type.lower()
            if self.os_info != 'windows':
                if cluster.master.ssh_username != "root":
                    self.nonroot = True
                    shell.disconnect()
                    break
            shell.disconnect()

        """ some tests need to bypass checking cb server at set up
            to run installation """
        self.skip_init_check_cbserver = \
            self.input.param("skip_init_check_cbserver", False)

        try:
            if self.skip_setup_cleanup:
                self.bucket_util.buckets = self.bucket_util.get_all_buckets()
                return
            if not self.skip_init_check_cbserver:
                for cluster in self.__cb_clusters:
                    self.cb_version = None
                    rest = RestConnection(cluster.master)
                    if RestHelper(rest).is_ns_server_running():
                        """
                        Since every new couchbase version, there will be new
                        features that test code won't work on previous release.
                        So we need to get couchbase version to filter out
                        those tests.
                        """
                        self.cb_version = rest.get_nodes_version()
                    else:
                        self.log.debug("couchbase server does not run yet")
                    # Stopped supporting TAP protocol since 3.x
                    # and 3.x support also has stopped
                    self.protocol = "dcp"
            self.services_map = None

            self.log_setup_status("BaseTestCase", "started")
            for cluster in self.__cb_clusters:
                if not self.skip_buckets_handle \
                        and not self.skip_init_check_cbserver:
                    self.log.debug("Cleaning up cluster")
                    cluster_util = ClusterUtils(cluster, self.task_manager)
                    bucket_util = BucketUtils(cluster, cluster_util,
                                              self.task)
                    cluster_util.cluster_cleanup(bucket_util)

            # Avoid cluster operations in setup for new upgrade / upgradeXDCR
            if str(self.__class__).find('newupgradetests') != -1 or \
                    str(self.__class__).find('upgradeXDCR') != -1 or \
                    str(self.__class__).find('Upgrade_EpTests') != -1 or \
                    self.skip_buckets_handle:
                self.log.warning("Cluster operation in setup will be skipped")
                self.primary_index_created = True
                self.log_setup_status("BaseTestCase", "finished")
                return
            # avoid clean up if the previous test has been tear down
            if self.case_number == 1 or self.case_number > 1000:
                if self.case_number > 1000:
                    self.log.warn("TearDown for prev test failed. Will retry")
                    self.case_number -= 1000
                self.cleanup = True
                if not self.skip_init_check_cbserver:
                    self.tearDownEverything()
                    self.tear_down_while_setup = False
            if not self.skip_init_check_cbserver:
                for cluster in self.__cb_clusters:
                    self.log.info("Initializing cluster")
                    cluster_util = ClusterUtils(cluster, self.task_manager)
                    # self.cluster_util.reset_cluster()
                    master_services = cluster_util.get_services(
                        cluster.servers[:1], self.services_init, start_node=0)
                    if master_services is not None:
                        master_services = master_services[0].split(",")

                    self.quota = self._initialize_nodes(
                        self.task,
                        cluster,
                        self.disabled_consistent_view,
                        self.rebalanceIndexWaitingDisabled,
                        self.rebalanceIndexPausingDisabled,
                        self.maxParallelIndexers,
                        self.maxParallelReplicaIndexers,
                        self.port,
                        self.quota_percent,
                        services=master_services)

                    cluster_util.change_env_variables()
                    cluster_util.change_checkpoint_params()
                    self.log.info("{0} initialized".format(cluster))
            else:
                self.quota = ""

            # Enable dp_version since we need collections enabled
            if self.enable_dp:
                for server in self.cluster.servers:
                    shell_conn = RemoteMachineShellConnection(server)
                    cb_cli = CbCli(shell_conn)
                    cb_cli.enable_dp()
                    shell_conn.disconnect()

            for cluster in self.__cb_clusters:
                cluster_util = ClusterUtils(cluster, self.task_manager)
                if self.log_info:
                    cluster_util.change_log_info()
                if self.log_location:
                    cluster_util.change_log_location()
                if self.stat_info:
                    cluster_util.change_stat_info()
                if self.port_info:
                    cluster_util.change_port_info()
                if self.port:
                    self.port = str(self.port)

            self.log_setup_status("BaseTestCase", "finished")

            if not self.skip_init_check_cbserver:
                self.__log("started")
        except Exception, e:
            traceback.print_exc()
            self.task.shutdown(force=True)
            self.fail(e)

    def cleanup_pcaps(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            # Stop old instances of tcpdump if still running
            stop_tcp_cmd = "if [[ \"$(pgrep tcpdump)\" ]]; " \
                           "then kill -s TERM $(pgrep tcpdump); fi"
            _, _ = shell.execute_command(stop_tcp_cmd)
            shell.execute_command("rm -rf pcaps")
            shell.execute_command("rm -rf " + server.ip + "_pcaps.zip")
            shell.disconnect()

    def start_collect_pcaps(self):
        for server in self.servers:
            shell = RemoteMachineShellConnection(server)
            # Create path for storing pcaps
            create_path = "mkdir -p pcaps"
            o, e = shell.execute_command(create_path)
            shell.log_command_output(o, e)
            # Install tcpdump command if it doesn't exist
            o, e = shell.execute_command("yum install -y tcpdump")
            shell.log_command_output(o, e)
            # Install screen command if it doesn't exist
            o, e = shell.execute_command("yum install -y screen")
            shell.log_command_output(o, e)
            # Execute the tcpdump command
            tcp_cmd = "screen -dmS test bash -c \"tcpdump -C 500 " \
                      "-w pcaps/pack-dump-file.pcap  -i eth0 -s 0 tcp\""
            o, e = shell.execute_command(tcp_cmd)
            shell.log_command_output(o, e)
            shell.disconnect()

    def start_fetch_pcaps(self):
        log_path = TestInputSingleton.input.param("logs_folder", "/tmp")
        for server in self.servers:
            remote_client = RemoteMachineShellConnection(server)
            # stop tcdump
            stop_tcp_cmd = "if [[ \"$(pgrep tcpdump)\" ]]; " \
                           "then kill -s TERM $(pgrep tcpdump); fi"
            o, e = remote_client.execute_command(stop_tcp_cmd)
            remote_client.log_command_output(o, e)
            # install zip unzip
            o, e = remote_client.execute_command("yum install -y zip unzip")
            remote_client.log_command_output(o, e)
            # zip the pcaps folder
            zip_cmd = "zip -r " + server.ip + "_pcaps.zip pcaps"
            o, e = remote_client.execute_command(zip_cmd)
            remote_client.log_command_output(o, e)
            # transfer the zip file
            zip_file_copied = remote_client.get_file(
                "/root",
                os.path.basename(server.ip + "_pcaps.zip"),
                log_path)
            self.log.info(
                "%s node pcap zip copied on client : %s"
                % (server.ip, zip_file_copied))
            if zip_file_copied:
                # clean up everything
                remote_client.execute_command("rm -rf pcaps")
                remote_client.execute_command("rm -rf "
                                              + server.ip + "_pcaps.zip")
            remote_client.disconnect()

    def tearDown(self):
        self.task_manager.shutdown_task_manager()
        self.task.shutdown(force=True)
        self.task_manager.abort_all_tasks()
        if self.sdk_client_pool:
            self.sdk_client_pool.shutdown()
        if self.collect_pcaps:
            self.start_fetch_pcaps()
        result = self.check_coredump_exist(self.servers, force_collect=True)
        self.tearDownEverything()
        if not self.crash_warning:
            self.assertFalse(result, msg="Cb_log file validation failed")
        if self.crash_warning and result:
            self.log.warn("CRASH | CRITICAL | WARN messages found in cb_logs")

    def tearDownEverything(self):
        if self.skip_setup_cleanup:
            return
        for cluster in self.__cb_clusters:
            cluster_util = ClusterUtils(cluster, self.task_manager)
            bucket_util = BucketUtils(cluster, cluster_util,
                                      self.task)
            try:
                if self.skip_buckets_handle:
                    return
                test_failed = self.is_test_failed()
                if test_failed \
                        and TestInputSingleton.input.param("stop-on-failure",
                                                           False) \
                        or self.input.param("skip_cleanup", False):
                    self.log.warn("CLEANUP WAS SKIPPED")
                else:
                    if test_failed:
                        # Collect logs because we have not shut things down
                        if self.get_cbcollect_info:
                            self.fetch_cb_collect_logs()

                        get_trace = \
                            TestInputSingleton.input.param("get_trace", None)
                        if get_trace:
                            for server in cluster.servers:
                                shell = \
                                    RemoteMachineShellConnection(server)
                                output, _ = shell.execute_command(
                                    "ps -aef|grep %s" % get_trace)
                                output = shell.execute_command(
                                    "pstack %s"
                                    % output[0].split()[1].strip())
                                self.infra_log.debug(output[0])
                                shell.disconnect()
                        else:
                            self.log.critical("Skipping get_trace !!")

                    rest = RestConnection(cluster.master)
                    alerts = rest.get_alerts()
                    if alerts is not None and len(alerts) != 0:
                        self.infra_log.warn("Alerts found: {0}".format(alerts))
                    self.log.debug("Cleaning up cluster")
                    cluster_util.cluster_cleanup(bucket_util)
            except BaseException as e:
                # kill memcached
                traceback.print_exc()
                self.log.warning("Killing memcached due to {0}".format(e))
                cluster_util.kill_memcached()
                # Increase case_number to retry tearDown in setup for next test
                self.case_number += 1000
            finally:
                if not self.input.param("skip_cleanup", False):
                    cluster_util.reset_cluster(self.crash_warning)
                # stop all existing task manager threads
                if self.cleanup:
                    self.cleanup = False
                else:
                    cluster_util.reset_env_variables()
        self.infra_log.info("========== tasks in thread pool ==========")
        self.task_manager.print_tasks_in_pool()
        self.infra_log.info("==========================================")
        if not self.tear_down_while_setup:
            self.task_manager.shutdown_task_manager()
            self.task.shutdown(force=True)

    def is_test_failed(self):
        return (hasattr(self, '_resultForDoCleanups')
                and len(self._resultForDoCleanups.failures
                or self._resultForDoCleanups.errors)) \
               or (hasattr(self, '_exc_info')
                   and self._exc_info()[1] is not None)

    def handle_setup_exception(self, exception_obj):
        # Shutdown client pool in case of any error before failing
        if self.sdk_client_pool is not None:
            self.sdk_client_pool.shutdown()
        # print the tracback of the failure
        traceback.print_exc()
        # Throw the exception so that the test will fail at setUp
        raise exception_obj

    def __log(self, status):
        try:
            msg = "{0}: {1} {2}" \
                .format(datetime.now(), self._testMethodName, status)
            RestConnection(self.servers[0]).log_client_error(msg)
        except Exception as e:
            self.log.warning("Exception during REST log_client_error: %s" % e)

    def log_setup_status(self, class_name, status):
        self.log.info(
            "========= %s setup %s for test #%d %s ========="
            % (class_name, status, self.case_number, self._testMethodName))

    def _initialize_nodes(self, task, cluster, disabled_consistent_view=None,
                          rebalance_index_waiting_disabled=None,
                          rebalance_index_pausing_disabled=None,
                          max_parallel_indexers=None,
                          max_parallel_replica_indexers=None,
                          port=None, quota_percent=None, services=None):
        quota = 0
        init_tasks = []
        for server in cluster.servers:
            # Make sure that data_and index_path are writable by couchbase user
            for path in set([_f for _f in [server.data_path, server.index_path]
                             if _f]):
                shell = RemoteMachineShellConnection(server)
                for cmd in ("rm -rf {0}/*".format(path),
                            "chown -R couchbase:couchbase {0}".format(path)):
                    shell.execute_command(cmd)
                shell.disconnect()
                rest = RestConnection(server)
                rest.set_data_path(data_path=server.data_path,
                                   index_path=server.index_path,
                                   cbas_path = server.cbas_path)
            init_port = port or server.port or '8091'
            assigned_services = services
            if cluster.master != server:
                assigned_services = None
            init_tasks.append(
                task.async_init_node(
                    server, disabled_consistent_view,
                    rebalance_index_waiting_disabled,
                    rebalance_index_pausing_disabled,
                    max_parallel_indexers,
                    max_parallel_replica_indexers, init_port,
                    quota_percent, services=assigned_services,
                    index_quota_percent=self.index_quota_percent,
                    gsi_type=self.gsi_type))
        for _task in init_tasks:
            node_quota = self.task_manager.get_task_result(_task)
            if node_quota < quota or quota == 0:
                quota = node_quota
        if quota < 100 and not len(set([server.ip
                                        for server in self.servers])) == 1:
            self.log.warn("RAM quota was defined less than 100 MB:")
            for server in cluster.servers:
                remote_client = RemoteMachineShellConnection(server)
                ram = remote_client.extract_remote_info().ram
                self.log.debug("{0}: {1}".format(server.ip, ram))
                remote_client.disconnect()

        if self.jre_path:
            for server in cluster.servers:
                rest = RestConnection(server)
                rest.set_jre_path(self.jre_path)
        return quota

    def fetch_cb_collect_logs(self):
        log_path = TestInputSingleton.input.param("logs_folder", "/tmp")
        is_single_node_server = len(self.servers) == 1
        for cluster in self.__cb_clusters:
            rest = RestConnection(cluster.master)
            nodes = rest.get_nodes()
            # Creating cluster_util object to handle multi_cluster scenario
            cluster_util = ClusterUtils(cluster, self.task_manager)
            status = cluster_util.trigger_cb_collect_on_cluster(
                rest, nodes,
                is_single_node_server)

            if status is True:
                cluster_util.wait_for_cb_collect_to_complete(rest)
                cluster_util.copy_cb_collect_logs(rest, nodes, cluster,
                                                  log_path)
            else:
                self.log.error("API perform_cb_collect returned False")

    def log_failure(self, message):
        self.log.error(message)
        self.summary.set_status("FAILED")
        if self.test_failure is None:
            self.test_failure = message

    def validate_test_failure(self):
        if self.test_failure is not None:
            self.fail(self.test_failure)

    def get_clusters(self):
        return self.__cb_clusters

    def get_task(self):
        return self.task

    def get_task_mgr(self):
        return self.task_manager

    def init_sdk_pool_object(self):
        self.sdk_client_pool = SDKClientPool()
        DocLoaderUtils.sdk_client_pool = self.sdk_client_pool

    def check_coredump_exist(self, servers, force_collect=False):
        bin_cb = "/opt/couchbase/bin/"
        lib_cb = "/opt/couchbase/var/lib/couchbase/"
        # crash_dir = "/opt/couchbase/var/lib/couchbase/"
        crash_dir_win = "c://CrashDumps"
        result = False

        def find_index_of(str_list, sub_string):
            for i in range(len(str_list)):
                if sub_string in str_list[i]:
                    return i
            return -1

        def get_gdb(gdb_shell, dmp_path, dmp_name):
            dmp_file = dmp_path + dmp_name
            core_file = dmp_path + dmp_name.strip(".dmp") + ".core"
            gdb_shell.execute_command("rm -rf " + core_file)
            gdb_shell.execute_command("/" + bin_cb + "minidump-2-core "
                                      + dmp_file + " > " + core_file)
            gdb_out = gdb_shell.execute_command(
                "gdb --batch " + bin_cb + "memcached -c "
                + core_file + " -ex \"bt full\" -ex quit")[0]
            t_index = find_index_of(gdb_out, "Core was generated by")
            gdb_out = gdb_out[t_index:]
            gdb_out = " ".join(gdb_out)
            return gdb_out

        def get_full_thread_dump(gdb_shell):
            thread_dump = gdb_shell.execute_command('gdb -p `(pidof memcached)` -ex \
            "thread apply all bt" -ex detach -ex quit')[0]
            index = find_index_of(thread_dump, "Thread debugging using libthread_db enabled")
            print " ".join(thread_dump[index:])

        def check_if_new_messages(grep_output_list):
            """
            Check the grep's last line for the latest timestamp.
            If this timestamp < start_timestamp of the test,
            then return False (as the grep's output is from previous tests)
            Note: This method works only if slave's time(timezone) matches
                  that of VM's. Else it won't be possible to compare timestamps
            """
            last_line = grep_output_list[-1]
            if not re.match(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T", last_line):
                # To check if line doesn't begin with any yyyy-mm-ddT
                self.log.critical("%s does not match any timestamp" % last_line)
                return True
            timestamp = last_line.split()[0]
            timestamp = timestamp.split(".")[0]
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
                msg = "%s: %d core dump seen" % (server.ip, len(dmp_files))
                self.log.critical(msg)
                self.log.critical("%s: Stack Trace of first crash - %s\n%s"
                                  % (server.ip, dmp_files[-1],
                                     get_gdb(shell, crash_dir, dmp_files[-1])))
                get_full_thread_dump(shell)
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
                    self.log.warning("%s: No '%s' files found"
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
                        if err_pattern is not None:
                            for pattern in err_pattern:
                                index = find_index_of(grep_output, pattern)
                                grep_output = grep_output[:index]
                                if grep_output:
                                    self.log.info("unwanted messages in %s" %
                                                  log_file)
                                    if check_if_new_messages(grep_output):
                                        self.log.critical(
                                            "%s: Found '%s' logs - %s"
                                            % (server.ip, grep_for_str,
                                               "".join(grep_output)))
                                        result = True
                                        break
                        else:
                            if grep_output \
                                    and check_if_new_messages(grep_output):
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

        return result
