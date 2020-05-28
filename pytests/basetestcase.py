import datetime
import os
import traceback
import unittest

from BucketLib.bucket import Bucket
from Cb_constants import ClusterRun, CbServer
from cb_tools.cb_cli import CbCli
from common_lib import sleep
from couchbase_helper.cluster import ServerTasks
from TestInput import TestInputSingleton
from global_vars import logger
from membase.api.rest_client import RestHelper, RestConnection
from bucket_utils.bucket_ready_functions import BucketUtils, DocLoaderUtils
from cluster_utils.cluster_ready_functions import ClusterUtils, CBCluster
from remote.remote_util import RemoteMachineShellConnection
from Jython_tasks.task_manager import TaskManager
from sdk_client3 import SDKClientPool


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
        self.dgm_run = self.input.param("dgm_run", False)
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
        self.quota_percent = self.input.param("quota_percent", None)
        self.skip_buckets_handle = self.input.param("skip_buckets_handle",
                                                    False)

        # SDKClientPool object for creating generic clients across tasks
        if self.sdk_client_pool is True:
            self.init_sdk_pool_object()

        # Initiate logging variables
        self.log = logger.get("test")
        self.infra_log = logger.get("infra")

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

            # avoid any cluster operations in setup for new upgrade
            #  & upgradeXDCR tests
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

    def tearDown(self):
        self.task_manager.shutdown_task_manager()
        self.task.shutdown(force=True)
        self.task_manager.abort_all_tasks()
        if self.sdk_client_pool:
            self.sdk_client_pool.shutdown()
        server_with_crashes = self.check_coredump_exist(self.servers)
        self.tearDownEverything()
        if not self.crash_warning:
            self.assertEqual(len(server_with_crashes), 0,
                             msg="Test failed, Coredump found on servers {}"
                             .format(server_with_crashes))
        if self.crash_warning and len(server_with_crashes) > 0:
            self.log.warn("Coredump found on servers {}\
            ".format(server_with_crashes))

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
                test_failed = (hasattr(self, '_resultForDoCleanups') and
                               len(self._resultForDoCleanups.failures or
                                   self._resultForDoCleanups.errors)) or \
                              (hasattr(self, '_exc_info') and
                               self._exc_info()[1] is not None)

                if test_failed \
                        and TestInputSingleton.input.param("stop-on-failure",
                                                           False) \
                        or self.input.param("skip_cleanup", False):
                    self.log.warn("CLEANUP WAS SKIPPED")
                else:
                    if test_failed:
                        # Collect logs because we have not shut things down
                        if TestInputSingleton.input.param("get-cbcollect-info",
                                                          False):
                            self.fetch_cb_collect_logs()

                        get_trace = \
                            TestInputSingleton.input.param("get_trace", None)
                        if get_trace:
                            for server in cluster.servers:
                                try:
                                    shell = \
                                        RemoteMachineShellConnection(server)
                                    output, _ = shell.execute_command(
                                        "ps -aef|grep %s" % get_trace)
                                    output = shell.execute_command(
                                        "pstack %s"
                                        % output[0].split()[1].strip())
                                    self.infra_log.debug(output[0])
                                    shell.disconnect()
                                except:
                                    pass
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
                    cluster_util.reset_cluster(crash_warning=self.crash_warning)
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

    def __log(self, status):
        try:
            msg = "{0}: {1} {2}" \
                .format(datetime.datetime.now(), self._testMethodName, status)
            RestConnection(self.servers[0]).log_client_error(msg)
        except:
            pass

    def log_setup_status(self, class_name, status):
        self.log.info(
            "========= %s setup %s for test #%d %s ========="
            % (class_name, status, self.case_number, self._testMethodName))

    def _initialize_nodes(self, task, cluster, disabled_consistent_view=None,
                          rebalanceIndexWaitingDisabled=None,
                          rebalanceIndexPausingDisabled=None,
                          maxParallelIndexers=None,
                          maxParallelReplicaIndexers=None,
                          port=None, quota_percent=None, services=None):
        quota = 0
        init_tasks = []
        for server in cluster.servers:
            # Make sure that data_and index_path are writable by couchbase user
            for path in set([_f for _f in [server.data_path, server.index_path] if _f]):
                shell = RemoteMachineShellConnection(server)
                for cmd in ("rm -rf {0}/*".format(path),
                            "chown -R couchbase:couchbase {0}".format(path)):
                    shell.execute_command(cmd)
                shell.disconnect()
                rest = RestConnection(server)
                rest.set_data_path(data_path=server.data_path,
                                   index_path=server.index_path)
            init_port = port or server.port or '8091'
            assigned_services = services
            if cluster.master != server:
                assigned_services = None
            init_tasks.append(
                task.async_init_node(
                    server, disabled_consistent_view,
                    rebalanceIndexWaitingDisabled,
                    rebalanceIndexPausingDisabled,
                    maxParallelIndexers,
                    maxParallelReplicaIndexers, init_port,
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
        for node in self.servers:
            params = dict()
            if len(self.servers) != 1:
                params['nodes'] = 'ns_1@' + node.ip
            else:
                # In case of single node we have to pass ip as below
                params['nodes'] = 'ns_1@' + '127.0.0.1'

            self.log.info('Running cbcollect on node ' + node.ip)
            rest = RestConnection(node)
            status, _, _ = rest.perform_cb_collect(params)
            sleep(10, "Wait for CB collect to start", log_type="infra")
            self.log.info("%s - cbcollect status: %s" % (node.ip, status))

            if status is True:
                self.log.info("Polling active_tasks to check cbcollect status")
                cb_collect_response = dict()
                retry = 0
                while retry < 30:
                    cb_collect_response = rest.ns_server_tasks(
                        "clusterLogsCollection")
                    self.log.debug("{}: CBCollectInfo Iteration {} - {}"
                                   .format(node.ip,
                                           retry,
                                           cb_collect_response["status"]))
                    if cb_collect_response['status'] == 'completed':
                        self.log.debug(cb_collect_response)
                        break
                    else:
                        retry += 1
                        sleep(10, "CB collect still running", log_type="infra")

                self.log.info("Copying cbcollect ZIP file to Client")
                remote_client = RemoteMachineShellConnection(node)
                if 'perNode' in cb_collect_response:
                    cb_collect_path = \
                        cb_collect_response['perNode'][params['nodes']]['path']
                    zip_file_copied = remote_client.get_file(
                        os.path.dirname(cb_collect_path),
                        os.path.basename(cb_collect_path),
                        log_path)
                    self.log.info(
                        "%s node cb collect zip coped on client : %s"
                        % (node.ip, zip_file_copied))
                    if zip_file_copied:
                        remote_client.execute_command("rm -f %s"
                                                      % cb_collect_path)
                        remote_client.disconnect()
                else:
                    self.log.error(
                        "Failed to retrieve zip file path on node %s"
                        % node.ip)
            else:
                self.log.error("API perform_cb_collect returned False")

    def log_failure(self, message):
        self.log.error(message)
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

    def check_coredump_exist(self, servers):
        """
        Checks coredump on the given nodes/node
        return: List of servers with crashes or a empty list if no core exists
        Args: list of servers
        """

        self.log.info("Initializing core dump check on all the nodes")
        servers_with_crashes = list()
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            shell.extract_remote_info()
            core_path = None
            if shell.info.type.lower() == "linux":
                rest = RestConnection(server)
                core_path = str(rest.get_data_path()).split("data")[0] \
                    + "crash/"
                if not os.path.isdir(core_path):
                    core_path = "/opt/couchbase/var/lib/couchbase/crash/"

            elif shell.info.type.lower() == "windows":
                core_path = 'c://CrashDumps'
            o, e = shell.execute_command("ls -l %s | grep '.dmp' | wc -l"
                                         % core_path)
            output = o[0].split('\n')[0]
            if int(output) == 0:
                self.log.info("Node %s - no core exists" % server.ip)
                shell.disconnect()
            else:
                self.log.error("Node %s - Core dump seen: %s"
                               % (server.ip, output))
                if TestInputSingleton.input.param("get-cbcollect-info", True):
                    servers_with_crashes.append(server.ip)
                shell.disconnect()
        if servers_with_crashes:
            self.fetch_cb_collect_logs()
        return servers_with_crashes
