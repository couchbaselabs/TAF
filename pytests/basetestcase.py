import logger
import unittest
import datetime
import logging
import traceback
from couchbase_helper.cluster import ServerTasks
from TestInput import TestInputSingleton
from membase.api.rest_client import RestHelper, RestConnection
from membase.helper.cluster_helper import ClusterOperationHelper
from membase.helper.rebalance_helper import RebalanceHelper
# from couchbase_helper.data_analysis_helper import *
# from security.rbac_base import RbacBase
import testconstants
from scripts.collect_server_info import cbcollectRunner
from bucket_utils.bucket_ready_functions import bucket_utils
from cluster_utils.cluster_ready_functions import cluster_utils
from cluster_utils.cluster_ready_functions import CBCluster
from BucketLib.BucketOperations import BucketHelper
from remote.remote_util import RemoteMachineShellConnection
from Jython_tasks.task_manager import TaskManager
import time
import os

log = logging.getLogger()


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.tear_down_while_setup = True
        self.input = TestInputSingleton.input
        self.primary_index_created = False
        self.sdk_client_type = self.input.param("sdk_client_type", "java")
        if self.input.param("log_level", None):
            log.setLevel(level=0)
            for hd in log.handlers:
                if str(hd.__class__).find('FileHandler') != -1:
                    hd.setLevel(level=logging.DEBUG)
                else:
                    hd.setLevel(level=getattr(logging, self.input.param("log_level", None)))
        self.servers = self.input.servers
        self.buckets = []
        self.case_number = self.input.param("case_number", 0)
        self.thread_to_use = self.input.param("threads_to_use", 10)
        self.cluster = CBCluster(servers=self.input.servers)
        self.task_manager = TaskManager(self.thread_to_use)
        self.load_gen_task_manager = TaskManager()
        self.cluster_util = cluster_utils(self.cluster, self.task_manager)
        self.bucket_util = bucket_utils(self.cluster, self.task_manager, self.cluster_util)
        self.task = ServerTasks(self.task_manager, self.load_gen_task_manager)
        self.cleanup = False
        self.nonroot = False
        shell = RemoteMachineShellConnection(self.cluster.master)
        self.os_info = shell.extract_remote_info().type.lower()
        if self.os_info != 'windows':
            if self.cluster.master.ssh_username != "root":
                self.nonroot = True
        shell.disconnect()
        """ some tests need to bypass checking cb server at set up
            to run installation """
        self.skip_init_check_cbserver = self.input.param("skip_init_check_cbserver", False)

        try:
            # Framework specific params
            self.skip_setup_cleanup = self.input.param("skip_setup_cleanup", False)
            self.log_info = self.input.param("log_info", None)
            self.log_location = self.input.param("log_location", None)
            # kill hang test and jump to next one.
            self.test_timeout = self.input.param("test_timeout", 3600)

            # Bucket specific params
            self.bucket_type = self.input.param("bucket_type", "membase")
            self.bucket_size = self.input.param("bucket_size", None)
            self.standard_buckets = self.input.param("standard_buckets", None)
            self.vbuckets = self.input.param("vbuckets", 1024)
            self.num_replicas = self.input.param("replicas", 1)
            self.active_resident_threshold = int(self.input.param("active_resident_threshold", 100))
            self.compression_mode = self.input.param("compression_mode", 'passive')
            # end of bucket parameters spot (this is ongoing)

            # Doc specific params
            self.key_size = self.input.param("key_size", 0)
            self.doc_size = self.input.param("doc_size", 10)
            self.doc_type = self.input.param("doc_type", "json")
            self.num_items = self.input.param("num_items", 100000)
            self.target_vbucket = self.input.param("target_vbucket", None)
            self.maxttl = self.input.param("maxttl", 0)

            # Client specific params
            self.sdk_compression = self.input.param("sdk_compression", True)
            self.replicate_to = self.input.param("replicate_to", 0)
            self.persist_to = self.input.param("persist_to", 0)
            self.sdk_retries = self.input.param("sdk_retries", 5)
            self.sdk_timeout = self.input.param("sdk_timeout", 5)
            self.durability_level = self.input.param("durability_level", None)

            self.index_quota_percent = self.input.param("index_quota_percent", None)
            self.num_servers = self.input.param("servers", len(self.cluster.servers))

            # initial number of items in the cluster
            self.services_init = self.input.param("services_init", None)
            self.nodes_init = self.input.param("nodes_init", 1)
            self.nodes_in = self.input.param("nodes_in", 1)
            self.nodes_out = self.input.param("nodes_out", 1)
            self.services_in = self.input.param("services_in", None)
            self.forceEject = self.input.param("forceEject", False)
            self.value_size = self.input.param("value_size", 1)
            self.wait_timeout = self.input.param("wait_timeout", 60)
            self.dgm_run = self.input.param("dgm_run", False)
            self.verify_unacked_bytes = self.input.param("verify_unacked_bytes", False)
            self.force_kill_memcached = TestInputSingleton.input.param('force_kill_memcached', False)
            self.disabled_consistent_view = self.input.param("disabled_consistent_view", None)
            self.rebalanceIndexWaitingDisabled = self.input.param("rebalanceIndexWaitingDisabled", None)
            self.rebalanceIndexPausingDisabled = self.input.param("rebalanceIndexPausingDisabled", None)
            self.maxParallelIndexers = self.input.param("maxParallelIndexers", None)
            self.maxParallelReplicaIndexers = self.input.param("maxParallelReplicaIndexers", None)
            self.quota_percent = self.input.param("quota_percent", None)
            self.port = None
            self.stat_info = self.input.param("stat_info", None)
            self.port_info = self.input.param("port_info", None)
            if not hasattr(self, 'skip_buckets_handle'):
                self.skip_buckets_handle = self.input.param("skip_buckets_handle", False)
            self.gsi_type = self.input.param("gsi_type", 'plasma')
            # jre-path for cbas
            self.jre_path = self.input.param("jre_path", None)

            if self.skip_setup_cleanup:
                self.buckets = self.bucket_util.get_all_buckets()
                return
            if not self.skip_init_check_cbserver:
                self.cb_version = None
                if RestHelper(RestConnection(self.cluster.master)).is_ns_server_running():
                    """ since every new couchbase version, there will be new features
                        that test code will not work on previous release.  So we need
                        to get couchbase version to filter out those tests. """
                    self.cb_version = RestConnection(self.cluster.master).get_nodes_version()
                else:
                    log.info("couchbase server does not run yet")
                self.protocol = self.cluster_util.get_protocol_type()
            self.services_map = None

            log.info("==============  basetestcase setup was started for test #{0} {1}=============="
                     .format(self.case_number, self._testMethodName))
            if not self.skip_buckets_handle and not self.skip_init_check_cbserver:
                self.cluster_util.cluster_cleanup(self.bucket_util)

            # avoid any cluster operations in setup for new upgrade
            #  & upgradeXDCR tests
            if str(self.__class__).find('newupgradetests') != -1 or \
                    str(self.__class__).find('upgradeXDCR') != -1 or \
                    str(self.__class__).find('Upgrade_EpTests') != -1 or \
                    hasattr(self, 'skip_buckets_handle') and \
                    self.skip_buckets_handle:
                log.info("any cluster operation in setup will be skipped")
                self.primary_index_created = True
                log.info("==============  basetestcase setup was finished for test #{0} {1} =============="
                         .format(self.case_number, self._testMethodName))
                return
            # avoid clean up if the previous test has been tear down
            if self.case_number == 1 or self.case_number > 1000:
                if self.case_number > 1000:
                    log.warn("teardDown for previous test failed. will retry..")
                    self.case_number -= 1000
                self.cleanup = True
                if not self.skip_init_check_cbserver:
                    self.tearDownEverything()
                    self.tear_down_while_setup = False
                #self.task = ServerTasks(self.task_manager)
            if not self.skip_init_check_cbserver:
                log.info("initializing cluster")
                # self.cluster_util.reset_cluster()
                master_services = self.cluster_util.get_services(self.servers[:1],
                                                                 self.services_init,
                                                                 start_node=0)
                if master_services is not None:
                    master_services = master_services[0].split(",")

                self.quota = self._initialize_nodes(self.task, self.cluster.servers,
                                                    self.disabled_consistent_view,
                                                    self.rebalanceIndexWaitingDisabled,
                                                    self.rebalanceIndexPausingDisabled,
                                                    self.maxParallelIndexers,
                                                    self.maxParallelReplicaIndexers,
                                                    self.port,
                                                    self.quota_percent,
                                                    services=master_services)

                self.cluster_util.change_env_variables()
                self.cluster_util.change_checkpoint_params()
                log.info("done initializing cluster")
            else:
                self.quota = ""
            if self.input.param("log_info", None):
                self.cluster_util.change_log_info()
            if self.input.param("log_location", None):
                self.cluster_util.change_log_location()
            if self.input.param("stat_info", None):
                self.cluster_util.change_stat_info()
            if self.input.param("port_info", None):
                self.cluster_util.change_port_info()
            if self.input.param("port", None):
                self.port = str(self.input.param("port", None))

            log.info("==============  basetestcase setup was finished for test #{0} {1} =============="
                     .format(self.case_number, self._testMethodName))

            if not self.skip_init_check_cbserver:
                self._log_start()
                self.sleep(5)
        except Exception, e:
            traceback.print_exc()
            self.task.shutdown(force=True)
            self.fail(e)

    def tearDown(self):
        self.tearDownEverything()

    def tearDownEverything(self):
        if self.skip_setup_cleanup:
            return
        try:
            if hasattr(self, 'skip_buckets_handle') and self.skip_buckets_handle:
                return
            test_failed = (hasattr(self, '_resultForDoCleanups') and \
                           len(self._resultForDoCleanups.failures or \
                               self._resultForDoCleanups.errors)) or \
                          (hasattr(self, '_exc_info') and \
                           self._exc_info()[1] is not None)

            if test_failed and TestInputSingleton.input.param("stop-on-failure", False) \
                    or self.input.param("skip_cleanup", False):
                log.warn("CLEANUP WAS SKIPPED")
            else:

                if test_failed:
                    # collect logs here instead of in test runner because we have not shut things down
                    if TestInputSingleton.param("get-cbcollect-info", False):
                        for server in self.servers:
                            log.info("Collecting logs @ {0}".format(server.ip))
                            self.get_cbcollect_info(server)
                        # collected logs so turn it off so it is not done later
                        TestInputSingleton.input.test_params["get-cbcollect-info"] = False

                    if TestInputSingleton.input.param('get_trace', None):
                        for server in self.servers:
                            try:
                                shell = RemoteMachineShellConnection(server)
                                output, _ = shell.execute_command("ps -aef|grep %s" %
                                                                  TestInputSingleton.input.param('get_trace', None))
                                output = shell.execute_command("pstack %s" % output[0].split()[1].strip())
                                print output[0]
                            except:
                                pass

                log.info("==============  basetestcase cleanup was started for test #{0} {1} =============="
                         .format(self.case_number, self._testMethodName))
                rest = RestConnection(self.cluster.master)
                alerts = rest.get_alerts()
                if alerts is not None and len(alerts) != 0:
                    log.warn("Alerts were found: {0}".format(alerts))
                self.cluster_util.cluster_cleanup(self.bucket_util)
                log.info("==============  basetestcase cleanup was finished for test #{0} {1} =============="
                         .format(self.case_number, self._testMethodName))
        except BaseException:
            # kill memcached
            self.cluster_util.kill_memcached()
            # increase case_number to retry tearDown in setup for the next test
            self.case_number += 1000
        finally:
            if not self.input.param("skip_cleanup", False):
                self.cluster_util.reset_cluster()
            # stop all existing task manager threads
            if self.cleanup:
                self.cleanup = False
            else:
                self.cluster_util.reset_env_variables()
            log.info("==========================tasks in thread pool==================")
            self.task_manager.print_tasks_in_pool()
            log.info("====================================================")
            if not self.tear_down_while_setup:
                self.task_manager.shutdown_task_manager()
                self.load_gen_task_manager.shutdown_task_manager()
                self.task.shutdown(force=True)
            self._log_finish()

    def get_cbcollect_info(self, server):
        """Collect cbcollectinfo logs for all the servers in the cluster.
        """
        path = TestInputSingleton.input.param("logs_folder", "/tmp")
        print "grabbing cbcollect from {0}".format(server.ip)
        path = path or "."
        try:
            cbcollectRunner(server, path).run()
            TestInputSingleton.input.test_params[
                "get-cbcollect-info"] = False
        except Exception as e:
            log.error("IMPOSSIBLE TO GRAB CBCOLLECT FROM {0}: {1}".format(server.ip, e))

    def _log_start(self):
        try:
            msg = "{0} : {1} started ".format(datetime.datetime.now(), self._testMethodName)
            RestConnection(self.servers[0]).log_client_error(msg)
        except:
            pass

    def _log_finish(self):
        try:
            msg = "{0} : {1} finished ".format(datetime.datetime.now(), self._testMethodName)
            RestConnection(self.servers[0]).log_client_error(msg)
        except:
            pass

    def sleep(self, timeout=15, message=""):
        log.info("sleep for {0} secs. {1} ...".format(timeout, message))
        time.sleep(timeout)

    def _initialize_nodes(self, cluster, servers, disabled_consistent_view=None, rebalanceIndexWaitingDisabled=None,
                          rebalanceIndexPausingDisabled=None, maxParallelIndexers=None, maxParallelReplicaIndexers=None,
                          port=None, quota_percent=None, services=None):
        quota = 0
        init_tasks = []
        for server in servers:
            init_port = port or server.port or '8091'
            assigned_services = services
            if self.cluster.master != server:
                assigned_services = None
            init_tasks.append(cluster.async_init_node(server, disabled_consistent_view, rebalanceIndexWaitingDisabled,
                                                      rebalanceIndexPausingDisabled, maxParallelIndexers,
                                                      maxParallelReplicaIndexers, init_port,
                                                      quota_percent, services=assigned_services,
                                                      index_quota_percent=self.index_quota_percent,
                                                      gsi_type=self.gsi_type))
        for task in init_tasks:
            node_quota = task.get_result()
            if node_quota < quota or quota == 0:
                quota = node_quota
        if quota < 100 and len(set([server.ip for server in self.servers])) != 1:
            log.warn("RAM quota was defined less than 100 MB:")
            for server in servers:
                remote_client = RemoteMachineShellConnection(server)
                ram = remote_client.extract_remote_info().ram
                log.info("{0}: {1} MB".format(server.ip, ram))
                remote_client.disconnect()

        if self.jre_path:
            for server in servers:
                rest = RestConnection(server)
                rest.set_jre_path(self.jre_path)
        return quota

    def expire_pager(self, servers, val=10):
        for bucket in self.buckets:
            for server in servers:
                ClusterOperationHelper.flushctl_set(server, "exp_pager_stime",
                                                    val, bucket)
        self.sleep(val, "wait for expiry pager to run on all these nodes")
