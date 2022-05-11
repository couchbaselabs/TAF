"""
Created on Feb 16, 2022

@author: ritesh.agarwal
"""
from collections import OrderedDict
from datetime import datetime
import traceback
import unittest

from BucketLib.bucket import Bucket
from Cb_constants import CbServer
from Jython_tasks.task_manager import TaskManager
from SystemEventLogLib.Events import EventHelper
from TestInput import TestInputSingleton, TestInputServer
from bucket_utils.bucket_ready_functions import BucketUtils, DocLoaderUtils
from capella.internal_api import CapellaUtils as CapellaAPI
from capella.internal_api import Pod, Tenant
from cluster_utils.cluster_ready_functions import ClusterUtils, CBCluster
from common_lib import sleep
from couchbase_helper.cluster import ServerTasks
from couchbase_helper.durability_helper import BucketDurability
from global_vars import logger
import global_vars
from membase.api.rest_client import RestConnection
from node_utils.node_ready_functions import NodeUtils
from sdk_client3 import SDKClientPool
from security_config import trust_all_certs
from test_summary import TestSummary
from Jython_tasks.task import DeployCloud


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        self.input = TestInputSingleton.input

        # Framework specific parameters
        self.log_level = self.input.param("log_level", "info").upper()
        self.infra_log_level = self.input.param("infra_log_level",
                                                "error").upper()
        self.tear_down_while_setup = self.input.param("tear_down_while_setup",
                                                      True)
        self.test_timeout = self.input.param("test_timeout", 3600)
        self.thread_to_use = self.input.param("threads_to_use", 30)
        self.case_number = self.input.param("case_number", 0)
        for server in self.input.servers:
            server.hosted_on_cloud = True
        # End of framework parameters

        # Cluster level info settings
        self.servers = list()
        self.cb_clusters = OrderedDict()
        self.capella = self.input.capella
        self.num_clusters = self.input.param("num_clusters", 1)

        # Bucket specific params
        self.bucket_type = self.input.param("bucket_type",
                                            Bucket.Type.MEMBASE)
        self.bucket_ttl = self.input.param("bucket_ttl", 0)
        self.bucket_size = self.input.param("bucket_size", None)
        self.bucket_conflict_resolution_type = \
            self.input.param("bucket_conflict_resolution",
                             Bucket.ConflictResolution.SEQ_NO)
        self.bucket_eviction_policy = \
            self.input.param("bucket_eviction_policy",
                             Bucket.EvictionPolicy.FULL_EVICTION)
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
        self.transaction_durability_level = \
            self.input.param("transaction_durability", "")
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
        self.time_unit = self.input.param("time_unit", "seconds")
        self.durability_level = self.input.param("durability", "NONE").upper()
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
        self.dgm_batch = self.input.param("dgm_batch", 5000)
        self.ryow = self.input.param("ryow", False)
        self.check_persistence = self.input.param("check_persistence", False)
        self.ops_rate = self.input.param("ops_rate", 10000)
        self.delete_docs_at_end = self.input.param(
            "delete_doc_at_end", True)
        # End of client specific parameters

        # initial number of items in the cluster
        self.services_init = self.input.param("services_init", None)
        self.nodes_init = self.input.param("nodes_init", 1)
        self.wait_timeout = self.input.param("wait_timeout", 120)
        self.use_https = self.input.param("use_https", True)
        self.enforce_tls = self.input.param("enforce_tls", True)
        self.ipv4_only = self.input.param("ipv4_only", False)
        self.ipv6_only = self.input.param("ipv6_only", False)
        self.multiple_ca = self.input.param("multiple_ca", False)
        CbServer.use_https = True
        trust_all_certs()

        # initialise pod object
        pod_url = self.input.capella.get("pod")
        pod_url = "https://{}".format(pod_url)
        pod_url_basic = "https://cloud{}".format(pod_url)
        self.pod = Pod(pod_url, pod_url_basic)

        self.tenant = Tenant(self.input.capella.get("tenant_id"),
                             self.input.capella.get("capella_user"),
                             self.input.capella.get("capella_pwd"))

        # SDKClientPool object for creating generic clients across tasks
        if self.sdk_client_pool is True:
            self.init_sdk_pool_object()

        # Initiate logging variables
        self.log = logger.get("test")
        self.infra_log = logger.get("infra")
        global_vars.system_event_logs = EventHelper()
        self.system_events = global_vars.system_event_logs

        # Support lib objects for testcase execution
        self.task_manager = TaskManager(self.thread_to_use)
        self.task = ServerTasks(self.task_manager)
        self.node_utils = NodeUtils(self.task_manager)
        # End of library object creation

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
        self.collect_data = self.input.param("collect_data", False)
        self.validate_system_event_logs = \
            self.input.param("validate_sys_event_logs", False)

        # Configure loggers
        self.log.setLevel(self.log_level)
        self.infra_log.setLevel(self.infra_log_level)

        self.sleep = sleep

        self.cleanup = False
        self.nonroot = False
        self.test_failure = None
        self.crash_warning = self.input.param("crash_warning", False)
        self.summary = TestSummary(self.log)
        self.rest_username = \
            TestInputSingleton.input.membase_settings.rest_username
        self.rest_password = \
            TestInputSingleton.input.membase_settings.rest_password
        self.skip_teardown = self.input.param("skip_teardown",
                                              False)
        self.skip_setup = self.input.param("skip_setup",
                                           False)
        if self.skip_setup:
            return

        self.log_setup_status(self.__class__.__name__, "started")
        cluster_name_format = "C%s"
        default_cluster_index = counter_index = 1
        self.capella_cluster_config = {
            "region": self.input.param("region", "us-west-2"),
            "provider": self.input.param("provider", "aws"),
            "name": "aaTAF_",
            "cidr": None,
            "singleAZ": False,
            "specs": [],
            "plan": "Developer Pro",
            "projectId": None,
            "timezone": "PT",
            "description": ""
            }

        services = self.input.param("services", CbServer.Services.KV)
        for service_group in services.split("-"):
            service_group = service_group.split(":")
            min_nodes = 3 if CbServer.Services.KV in service_group else 2
            service_config = {
                "count": max(min_nodes, self.nodes_init),
                "services": service_group,
                "compute": self.input.param("compute", "m5.xlarge"),
                "disk": {
                    "type": self.input.param("type", "gp3"),
                    "sizeInGb": self.input.param("sizeInGb", 50),
                    "iops": self.input.param("iops", 3000)
                }
            }
            if self.capella_cluster_config["provider"] != "aws":
                service_config["disk"].pop("iops")
            self.capella_cluster_config["specs"].append(service_config)

        self.tenant.project_id = \
            TestInputSingleton.input.capella.get("project", None)
        if not self.tenant.project_id:
            CapellaAPI.create_project(self.pod, self.tenant, "a_taf_run")

        tasks = list()
        for _ in range(self.num_clusters):
            cluster_name = cluster_name_format % counter_index
            self.capella_cluster_config["name"] = "a_%s_%s_%sGB_%s" % (
                self.capella_cluster_config["provider"],
                self.input.param("compute", "m5.xlarge"),
                self.input.param("sizeInGb", 50),
                cluster_name)
            deploy_task = DeployCloud(self.pod,
                                      self.tenant,
                                      cluster_name, self.capella_cluster_config)
            self.task_manager.add_new_task(deploy_task)
            tasks.append(deploy_task)
            counter_index += 1
            # cluster_id, srv, servers = \
            #     CapellaAPI.create_cluster(self.pod, self.tenant,
            #                               self.capella_cluster_config)
            # CapellaAPI.create_db_user(self.pod, self.tenant, cluster_id,
            #                           self.rest_username,
            #                           self.rest_password)
        for task in tasks:
            cluster = self.task_manager.get_task_result(task)
            cluster_id, srv, servers = task.cluster_id, task.srv, task.servers
            CapellaAPI.create_db_user(self.pod, self.tenant, cluster_id,
                                      self.rest_username,
                                      self.rest_password)
            nodes = list()
            for server in servers:
                temp_server = TestInputServer()
                temp_server.ip = server.get("hostname")
                temp_server.hostname = server.get("hostname")
                temp_server.services = server.get("services")
                temp_server.port = "18091"
                temp_server.rest_username = self.rest_username
                temp_server.rest_password = self.rest_password
                temp_server.hosted_on_cloud = True
                temp_server.memcached_port = "11207"
                nodes.append(temp_server)
            cluster = CBCluster(username=self.rest_username,
                                password=self.rest_password,
                                servers=nodes)
            cluster.id = cluster_id
            cluster.srv = srv
            cluster.details = self.capella_cluster_config
            for temp_server in nodes:
                if "Data" in temp_server.services:
                    cluster.kv_nodes.append(temp_server)
                if "Query" in temp_server.services:
                    cluster.query_nodes.append(temp_server)
                if "Index" in temp_server.services:
                    cluster.index_nodes.append(temp_server)
                if "Eventing" in temp_server.services:
                    cluster.eventing_nodes.append(temp_server)
                if "Analytics" in temp_server.services:
                    cluster.cbas_nodes.append(temp_server)
                if "FTS" in temp_server.services:
                    cluster.fts_nodes.append(temp_server)
                cluster.nodes_in_cluster.append(temp_server)
            self.tenant.clusters.update({cluster.id: cluster})

            self.cb_clusters[cluster_name] = cluster
            self.cb_clusters[cluster_name].cloud_cluster = True

        # Initialize self.cluster with first available cluster as default
        self.cluster = self.cb_clusters[cluster_name_format
                                        % default_cluster_index]
        self.servers = self.cluster.servers
        self.cluster_util = ClusterUtils(self.task_manager)
        self.bucket_util = BucketUtils(self.cluster_util, self.task)
        for _, cluster in self.cb_clusters.items():
            self.cluster_util.print_cluster_stats(cluster)

        self.cluster.edition = "enterprise"
        self.sleep(10)

    def tearDown(self):
        self.task_manager.shutdown_task_manager()
        self.task.shutdown(force=True)
        self.task_manager.abort_all_tasks()
        if self.sdk_client_pool:
            self.sdk_client_pool.shutdown()

        if self.skip_teardown:
            return

        for name, cluster in self.cb_clusters.items():
            self.log.info("Destroying cluster: {}".format(name))
            CapellaAPI.destroy_cluster(self.pod, self.tenant, cluster)
        CapellaAPI.delete_project(self.pod, self.tenant)

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

    def log_setup_status(self, class_name, status, stage="setup"):
        self.log.info(
            "========= %s %s %s for test #%d %s ========="
            % (class_name, stage, status, self.case_number,
               self._testMethodName))

    def log_failure(self, message):
        self.log.error(message)
        self.summary.set_status("FAILED")
        if self.test_failure is None:
            self.test_failure = message

    def validate_test_failure(self):
        if self.test_failure is not None:
            self.fail(self.test_failure)

    def get_clusters(self):
        return [self.cb_clusters[name] for name in self.cb_clusters.keys()]

    def get_task(self):
        return self.task

    def get_task_mgr(self):
        return self.task_manager

    def init_sdk_pool_object(self):
        self.sdk_client_pool = SDKClientPool()
        DocLoaderUtils.sdk_client_pool = self.sdk_client_pool


class ClusterSetup(BaseTestCase):
    def setUp(self):
        super(ClusterSetup, self).setUp()

        self.log_setup_status("ClusterSetup", "started", "setup")

        # Print cluster stats
        self.cluster_util.print_cluster_stats(self.cluster)
        self.log_setup_status("ClusterSetup", "complete", "setup")

    def tearDown(self):
        super(ClusterSetup, self).tearDown()
