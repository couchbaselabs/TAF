import os
import re
import traceback
import unittest

import Cb_constants
import global_vars
from SystemEventLogLib.Events import EventHelper, Event
from SystemEventLogLib.data_service_events import DataServiceEvents
from security_config import trust_all_certs
from collections import OrderedDict

from datetime import datetime
from ruamel.yaml import YAML

from BucketLib.bucket import Bucket
from Cb_constants import ClusterRun, CbServer
from common_lib import sleep
from couchbase_helper.cluster import ServerTasks
from TestInput import TestInputSingleton
from global_vars import logger
from couchbase_helper.durability_helper import BucketDurability
from couchbase_utils.cb_tools.cb_cli import CbCli
from membase.api.rest_client import RestConnection
from bucket_utils.bucket_ready_functions import BucketUtils, DocLoaderUtils
from cluster_utils.cluster_ready_functions import ClusterUtils, CBCluster
from remote.remote_util import RemoteMachineShellConnection
from Jython_tasks.task_manager import TaskManager
from sdk_client3 import SDKClientPool
from test_summary import TestSummary
from couchbase_utils.security_utils.x509_multiple_CA_util import x509main
from node_utils.node_ready_functions import NodeUtils


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        self.input = TestInputSingleton.input

        # Framework specific parameters
        self.log_level = self.input.param("log_level", "info").upper()
        self.infra_log_level = self.input.param("infra_log_level",
                                                "error").upper()
        self.skip_setup_cleanup = self.input.param("skip_setup_cleanup", False)
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
        self.cb_clusters = OrderedDict()
        self.num_servers = self.input.param("servers", len(self.servers))
        self.vbuckets = self.input.param("vbuckets", CbServer.total_vbuckets)
        self.gsi_type = self.input.param("gsi_type", 'plasma')
        self.skip_cluster_reset = self.input.param("skip_cluster_reset", False)
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
        self.bucket_type = self.input.param("bucket_type",
                                            Bucket.Type.MEMBASE)
        self.bucket_ttl = self.input.param("bucket_ttl", 0)
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
        b_durability = "MAJORITY_AND_PERSIST_TO_ACTIVE" \
            if self.bucket_durability_level == "MAJORITY_AND_PERSIST_ACTIVE" \
            else self.bucket_durability_level
        self.bucket_durability_level = BucketDurability[b_durability]
        self.bucket_collection_history_retention_default = \
            self.input.param("default_history_retention_for_collections", None)
        self.bucket_dedup_retention_seconds = \
            self.input.param("bucket_history_retention_seconds", None)
        self.bucket_dedup_retention_bytes = \
            self.input.param("bucket_history_retention_bytes", None)
        self.magma_key_tree_data_block_size = \
        self.input.param("magma_key_tree_data_block_size", None)
        self.magma_seq_tree_data_block_size = \
        self.input.param("magma_seq_tree_data_block_size", None)
        self.oso_dcp_backfill = self.input.param("oso_dcp_backfill", None)
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
        self.load_collections_exponentially = \
            self.input.param("load_collections_exponentially", False)
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
        # End of client specific parameters

        # initial number of items in the cluster
        self.services_init = self.input.param("services_init", None)
        self.nodes_init = self.input.param("nodes_init", 1)
        self.nodes_in = self.input.param("nodes_in", 0)
        self.nodes_out = self.input.param("nodes_out", 0)
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
        self.skip_buckets_handle = self.input.param("skip_buckets_handle",
                                                    False)
        self.use_https = self.input.param("use_https", False)
        self.enforce_tls = self.input.param("enforce_tls", False)
        self.tls_level = self.input.param("tls_level", "strict")
        self.ipv4_only = self.input.param("ipv4_only", False)
        self.ipv6_only = self.input.param("ipv6_only", False)
        self.multiple_ca = self.input.param("multiple_ca", False)

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

        self.node_utils.cleanup_pcaps(self.servers)
        self.collect_pcaps = self.input.param("collect_pcaps", False)
        if self.collect_pcaps:
            self.node_utils.start_collect_pcaps(self.servers)

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

        # Force disable TLS to avoid initial connection issues
        tasks = [self.node_utils.async_disable_tls(server)
                 for server in self.servers]
        for task in tasks:
            self.task_manager.get_task_result(task)

        self.log_setup_status(self.__class__.__name__, "started")
        cluster_name_format = "C{}"
        default_cluster_index = counter_index = 1
        if len(self.input.clusters) > 1:
            # Multi cluster setup
            for _, nodes in self.input.clusters.iteritems():
                cluster_name = cluster_name_format.format(counter_index)
                tem_cluster = CBCluster(name=cluster_name, servers=nodes,
                                        vbuckets=self.vbuckets)
                self.cb_clusters[cluster_name] = tem_cluster
                counter_index += 1
        else:
            # Single cluster
            cluster_name = cluster_name_format.format(counter_index)
            self.cb_clusters[cluster_name] = CBCluster(name=cluster_name,
                                                       servers=self.servers,
                                                       vbuckets=self.vbuckets)

        if self.use_https:
            CbServer.use_https = True
            trust_all_certs()

        # Initialize self.cluster with first available cluster as default
        self.cluster = self.cb_clusters[cluster_name_format
                                        .format(default_cluster_index)]
        self.cluster_util = ClusterUtils(self.task_manager)
        self.bucket_util = BucketUtils(self.cluster_util, self.task)

        CbServer.enterprise_edition = \
            self.cluster_util.is_enterprise_edition(self.cluster)
        if CbServer.enterprise_edition:
            self.cluster.edition = "enterprise"
        else:
            self.cluster.edition = "community"

        if self.standard_buckets > 10:
            self.bucket_util.change_max_buckets(self.cluster.master,
                                                self.standard_buckets)

        for cluster_name, cluster in self.cb_clusters.items():
            shell = RemoteMachineShellConnection(cluster.master)
            self.os_info = shell.extract_remote_info().type.lower()
            if self.os_info != 'windows':
                if cluster.master.ssh_username != "root":
                    self.nonroot = True
                    shell.disconnect()
                    break
            shell.disconnect()

        try:
            if self.skip_setup_cleanup:
                self.cluster.buckets = self.bucket_util.get_all_buckets(
                    self.cluster)
                return

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

            self.log_setup_status("BaseTestCase", "started")
            for cluster_name, cluster in self.cb_clusters.items():
                if not self.skip_buckets_handle:
                    self.log.debug("Cleaning up cluster")
                    self.cluster_util.cluster_cleanup(cluster,
                                                      self.bucket_util)

            reload(Cb_constants)
            # Avoid cluster operations in setup for new upgrade / upgradeXDCR
            if str(self.__class__).find('newupgradetests') != -1 or \
                    str(self.__class__).find('upgradeXDCR') != -1 or \
                    str(self.__class__).find('Upgrade_EpTests') != -1 or \
                    self.skip_buckets_handle:
                self.log.warning("Cluster operation in setup will be skipped")

                # Track test start time only if we need system log validation
                if self.validate_system_event_logs:
                    self.system_events.set_test_start_time()

                self.log_setup_status("BaseTestCase", "finished")
                return
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
                else:
                    self.quota = ""
                # Set this unconditionally
                RestConnection(cluster.master).set_internalSetting(
                    "magmaMinMemoryQuota", 256)
            else:
                self.quota = ""

            # Enable dp_version since we need collections enabled
            if self.enable_dp:
                tasks = []
                for server in self.cluster.servers:
                    task = self.node_utils.async_enable_dp(server)
                    tasks.append(task)
                for task in tasks:
                    self.task_manager.get_task_result(task)

            # Enforce tls on nodes of all clusters
            if self.use_https and self.enforce_tls:
                for _, cluster in self.cb_clusters.items():
                    tasks = []
                    for node in cluster.servers:
                        task = self.node_utils.async_enable_tls(node, self.tls_level)
                        tasks.append(task)
                    for task in tasks:
                        self.task_manager.get_task_result(task)
                    self.sleep(20, "waiting after enabling TLS")
                    if self.tls_level == "strict":
                        self.log.info("Validating if services obey tls only on servers {0}".
                                      format(cluster.servers))
                        status = self.cluster_util.\
                            check_if_services_obey_tls(cluster.servers)
                        if not status:
                            self.fail("Services did not honor enforce tls")
                    for node in cluster.servers:
                        shell_conn = RemoteMachineShellConnection(node)
                        cb_cli = CbCli(shell_conn, no_ssl_verify=True)
                        level = cb_cli.get_n2n_encryption_level()
                        shell_conn.disconnect()
                        self.assertTrue(level == self.tls_level)

            if self.use_https:
                for server in self.input.servers:
                    self.set_ports_for_server(server, "ssl")

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
            self.log_setup_status("BaseTestCase", "finished")
            self.__log("started")
        except Exception as e:
            traceback.print_exc()
            self.task.shutdown(force=True)
            self.fail(e)

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

    def set_ports_for_server(self, server, port_type="non_ssl"):
        self.log.debug("Setting %s ports for server %s" % (port_type, server))
        if port_type == "non_ssl":
            server.port = CbServer.port
            server.memcached_port = CbServer.memcached_port
            server.fts_port = CbServer.fts_port
            server.index_port = CbServer.index_port
            server.n1ql_port = CbServer.n1ql_port
            server.cbas_port = CbServer.cbas_port
            server.eventing_port = CbServer.eventing_port
        elif port_type == "ssl":
            server.port = CbServer.ssl_port
            server.memcached_port = CbServer.memcached_port
            server.fts_port = CbServer.ssl_fts_port
            server.index_port = CbServer.ssl_index_port
            server.n1ql_port = CbServer.ssl_n1ql_port
            server.cbas_port = CbServer.ssl_cbas_port
            server.eventing_port = CbServer.ssl_eventing_port

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
        if self.use_https:
            if self.enforce_tls:
                for _, cluster in self.cb_clusters.items():
                    tasks = []
                    for node in cluster.servers:
                        task = self.node_utils.async_disable_tls(node)
                        tasks.append(task)
                    for task in tasks:
                        self.task_manager.get_task_result(task)
            for server in self.input.servers:
                self.set_ports_for_server(server, "non_ssl")
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

        self.task_manager.shutdown_task_manager()
        self.task.shutdown(force=True)
        self.task_manager.abort_all_tasks()

    def tearDownEverything(self, reset_cluster_env_vars=True):
        if self.skip_setup_cleanup:
            return
        for _, cluster in self.cb_clusters.items():
            try:
                if self.skip_buckets_handle:
                    return
                test_failed = self.is_test_failed()
                if test_failed:
                    # Collect logs because we have not shut things down
                    if self.get_cbcollect_info:
                        self.fetch_cb_collect_logs()
                    get_trace = \
                        TestInputSingleton.input.param("get_trace", None)
                    if get_trace:
                        tasks = []
                        for server in cluster.servers:
                            task = self.node_utils.async_get_trace(
                                server, get_trace)
                            tasks.append(task)
                        for task in tasks:
                            self.task_manager.get_task_result(task)
                    else:
                        self.log.critical("Skipping get_trace !!")

                if test_failed \
                        and TestInputSingleton.input.param("stop-on-failure",
                                                           False) \
                        or self.input.param("skip_cleanup", False):
                    self.log.warn("CLEANUP WAS SKIPPED")
                else:
                    rest = RestConnection(cluster.master)
                    alerts = rest.get_alerts()
                    if alerts is not None and len(alerts) != 0:
                        self.infra_log.warn("Alerts found: {0}".format(alerts))
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

    def _initialize_nodes(self, task, cluster, disabled_consistent_view=None,
                          rebalance_index_waiting_disabled=None,
                          rebalance_index_pausing_disabled=None,
                          max_parallel_indexers=None,
                          max_parallel_replica_indexers=None,
                          port=None, quota_percent=None, services=None,
                          services_mem_quota_percent=None):
        quota = 0
        init_tasks = []
        ssh_sessions = dict()

        # Open ssh_connections for command execution
        for server in cluster.servers:
            ssh_sessions[server.ip] = RemoteMachineShellConnection(server)

        for server in cluster.servers:
            # Make sure that data_and index_path are writable by couchbase user
            if not server.index_path:
                server.index_path = server.data_path
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

    def check_coredump_exist(self, servers, force_collect=False):
        bin_cb = "/opt/couchbase/bin/"
        lib_cb = "/opt/couchbase/var/lib/couchbase/"
        crash_dir_win = "c:/Program Files/Couchbase/Server/var/lib/couchbase/crash/"
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
                    pattern_to_grep = []
                    if 'grep_for' in file_data:
                        for grep_pattern_of_log_file in file_data['grep_for']:
                            pattern_to_grep.append(grep_pattern_of_log_file)
                    for common_grep_pattern in y_data["common_patterns"]['grep_for']:
                        pattern_to_grep.append(common_grep_pattern)
                    for grep_pattern in pattern_to_grep:
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
                    for kvstore in kvstores:
                        if int(kvstore.split("-")[1]) >= self.vbuckets:
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


class ClusterSetup(BaseTestCase):
    def setUp(self):
        super(ClusterSetup, self).setUp()

        if self.skip_setup_cleanup:
            return

        self.log_setup_status("ClusterSetup", "started", "setup")

        services = None
        if self.services_init:
            services = list()
            for service in self.services_init.split("-"):
                services.append(service.replace(":", ","))
        services = services[1:] \
            if services is not None and len(services) > 1 else None

        # Add master node to the nodes_in_cluster list
        self.cluster.nodes_in_cluster.extend([self.cluster.master])

        # Rebalance-in nodes_init servers
        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        if nodes_init:
            result = self.task.rebalance([self.cluster.master], nodes_init, [],
                                         services=services)
            if result is False:
                # Need this block since cb-collect won't be collected
                # in BaseTest if failure happens during setup() stage
                if self.get_cbcollect_info:
                    self.fetch_cb_collect_logs()
                self.fail("Initial rebalance failed")

            self.cluster.nodes_in_cluster.extend(nodes_init)

        # Add basic RBAC users
        self.bucket_util.add_rbac_user(self.cluster.master)

        # Print cluster stats
        self.cluster_util.print_cluster_stats(self.cluster)
        self.log_setup_status("ClusterSetup", "complete", "setup")

    def tearDown(self):
        super(ClusterSetup, self).tearDown()

    def create_bucket(self, cluster, bucket_name="default"):
        self.bucket_util.create_default_bucket(
            cluster,
            bucket_type=self.bucket_type,
            ram_quota=self.bucket_size,
            replica=self.num_replicas,
            maxTTL=self.bucket_ttl,
            compression_mode=self.compression_mode,
            wait_for_warmup=True,
            conflict_resolution=Bucket.ConflictResolution.SEQ_NO,
            replica_index=self.bucket_replica_index,
            storage=self.bucket_storage,
            eviction_policy=self.bucket_eviction_policy,
            flush_enabled=self.flush_enabled,
            bucket_durability=self.bucket_durability_level,
            purge_interval=self.bucket_purge_interval,
            autoCompactionDefined="false",
            fragmentation_percentage=50,
            bucket_name=bucket_name,
            history_retention_collection_default=self.bucket_collection_history_retention_default,
            history_retention_seconds=self.bucket_dedup_retention_seconds,
            history_retention_bytes=self.bucket_dedup_retention_bytes,
            magma_key_tree_data_block_size=self.magma_key_tree_data_block_size,
            magma_seq_tree_data_block_size=self.magma_seq_tree_data_block_size)

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
