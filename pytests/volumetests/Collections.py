import threading
import time
import random
import math

from FtsLib.FtsOperations import FtsHelper
from backup_service import BackupServiceTest
from com.couchbase.client.java import *
from com.couchbase.client.java.json import *
from com.couchbase.client.java.query import *

from Cb_constants import CbServer
from collections_helper.collections_spec_constants import MetaCrudParams
from couchbase_utils.cb_tools.cb_cli import CbCli
from membase.api.rest_client import RestConnection, RestHelper
from TestInput import TestInputSingleton
from BucketLib.BucketOperations import BucketHelper
from remote.remote_util import RemoteMachineShellConnection
from error_simulation.cb_error import CouchbaseError
from bucket_collections.collections_base import CollectionBase
from sdk_exceptions import SDKException
from StatsLib.StatsOperations import StatsHelper


class volume(CollectionBase):
    # will add the __init__ functions after the test has been stabilised
    def setUp(self):
        self.input = TestInputSingleton.input
        self.input.test_params.update({"default_bucket": False})
        self.backup_service_test = self.input.param("backup_service_test", False)
        if self.backup_service_test:
            self.backup_service = BackupServiceTest(self.input.servers)
        super(volume, self).setUp()
        self.bucket_util._expiry_pager(self.cluster, val=5)
        self.rest = RestConnection(self.servers[0])
        self.available_servers = self.cluster.servers[self.nodes_init:]
        self.exclude_nodes = [self.cluster.master]
        self.skip_check_logs = False
        self.iterations = self.input.param("iterations", 2)
        self.vbucket_check = self.input.param("vbucket_check", True)
        self.retry_get_process_num = self.input.param("retry_get_process_num", 400)
        self.data_load_spec = self.input.param("data_load_spec", "volume_test_load_for_volume_test")
        self.perform_quorum_failover = self.input.param("perform_quorum_failover", True)
        self.rebalance_moves_per_node = self.input.param("rebalance_moves_per_node", 4)
        self.cluster_util.set_rebalance_moves_per_nodes(rebalanceMovesPerNode=self.rebalance_moves_per_node)
        self.scrape_interval = self.input.param("scrape_interval", None)
        if self.scrape_interval:
            self.log.info("Changing scrape interval and scrape_timeout to {0}".format(self.scrape_interval))
            StatsHelper(self.cluster.master).change_scrape_timeout(self.scrape_interval)
            StatsHelper(self.cluster.master).change_scrape_interval(self.scrape_interval)
            # Change high cardinality services' scrape_interval
            value = "[{S, [{high_cardinality_enabled, true}, {high_cardinality_scrape_interval, %s}]} " \
                    "|| S <- [index, fts, kv, cbas, eventing]]" % self.scrape_interval
            StatsHelper(self.cluster.master).configure_stats_settings_from_diag_eval("services", value)

        self.enable_n2n_encryption = self.input.param("enable_n2n_encryption", False)
        if self.enable_n2n_encryption:
            shell_conn = RemoteMachineShellConnection(self.cluster.master)
            cb_cli = CbCli(shell_conn)
            cb_cli.enable_n2n_encryption()
            cb_cli.set_n2n_encryption_level(level="all")
            shell_conn.disconnect()

        self.doc_and_collection_ttl = self.input.param("doc_and_collection_ttl", False)  # For using doc_ttl + coll_ttl
        self.skip_validations = self.input.param("skip_validations", True)

        # Services to be added on rebalance-in nodes during the volume test
        self.services_for_rebalance_in = self.input.param("services_for_rebalance_in", None)

        self.index_and_query_setup()
        self.fts_setup()
        self.query_thread_flag = False
        self.query_thread = None
        self.ui_stats_thread_flag = False
        self.ui_stats_thread = None

        # Setup the backup service
        if self.backup_service_test:
            self.backup_service.setup()

    def index_and_query_setup(self):
        """
        Init index, query objects and create initial indexes
        """
        self.number_of_indexes = self.input.param("number_of_indexes", 0)
        self.n1ql_nodes = None
        self.flush_buckets_before_indexes_creation = False
        if self.number_of_indexes > 0:
            self.flush_buckets_before_indexes_creation = \
                self.input.param("flush_buckets_before_indexes_creation", True)
            if self.flush_buckets_before_indexes_creation:
                self.bucket_util.flush_all_buckets(
                    self.cluster, skip_resetting_num_items=True)
            self.kv_mem_quota = self.input.param("kv_mem_quota", 10000)
            self.index_mem_quota = self.input.param("index_mem_quota", 11000)
            self.set_memory_quota(services=["kv", "index"])

            self.n1ql_nodes = self.cluster_util.get_nodes_from_services_map(service_type="n1ql",
                                                                            get_all_nodes=True,
                                                                            servers=self.cluster.servers[
                                                                                    :self.nodes_init],
                                                                            master=self.cluster.master)
            self.n1ql_rest_connections = list()
            for n1ql_node in self.n1ql_nodes:
                self.n1ql_rest_connections.append(RestConnection(n1ql_node))
                self.exclude_nodes.append(n1ql_node)
            self.n1ql_turn_counter = 0  # To distribute the turn of using n1ql nodes for query. Start with first node

            indexes_to_build = self.create_indexes_and_initialize_queries()
            self.build_deferred_indexes(indexes_to_build)

    def fts_setup(self):
        """
        Create initial fts indexes
        """
        self.fts_indexes_to_create = self.input.param("fts_indexes_to_create", 0)
        self.fts_indexes_to_recreate = self.input.param("fts_indexes_to_recreate", 0)
        self.fts_mem_quota = self.input.param("fts_mem_quota", 22000)
        if self.fts_indexes_to_create > 0:
            self.fts_index_partitions = self.input.param("fts_index_partition", 6)
            self.set_memory_quota(services=["fts"])
            _ = self.create_fts_indexes(self.fts_indexes_to_create)

    def tearDown(self):
        # Do not call the base class's teardown, as we want to keep the cluster intact after the volume run
        if self.scrape_interval:
            self.log.info("Reverting prometheus settings back to default")
            StatsHelper(self.cluster.master).reset_stats_settings_from_diag_eval()
        self.close_all_threads()
        # Cleanup the backup service
        if self.backup_service_test:
            self.backup_service.clean()
        self.log.info("Printing bucket stats before teardown")
        self.bucket_util.print_bucket_stats(self.cluster)
        if self.collect_pcaps:
            self.start_fetch_pcaps()
        if not self.skip_check_logs:
            self.check_logs()

    def close_all_threads(self):
        if self.query_thread:
            # Join query thread
            self.query_thread_flag = False
            self.query_thread.join()
            self.query_thread = None
        if self.ui_stats_thread:
            # Join ui_stats thread
            self.ui_stats_thread_flag = False
            self.ui_stats_thread.join()
            self.ui_stats_thread = None

    def check_logs(self):
        self.log.info("Checking logs on {0}".format(self.servers))
        result = self.check_coredump_exist(self.servers, force_collect=True)
        if not self.crash_warning:
            self.skip_check_logs = True  # Setting this, as we don't have to check logs again in tearDown
            self.assertFalse(result, msg="Cb_log file validation failed")
        if self.crash_warning and result:
            self.log.warn("CRASH | CRITICAL | WARN messages found in cb_logs")

    def set_memory_quota(self, services=None):
        """
        Set memory quota of services before starting volume steps
        services: list of services for which mem_quota has to be updated
        """
        if services is None:
            return
        ram_quota_dict = dict()
        if "kv" in services:
            ram_quota_dict[CbServer.Settings.KV_MEM_QUOTA] = \
                int(self.kv_mem_quota)
        if "index" in services:
            ram_quota_dict[CbServer.Settings.INDEX_MEM_QUOTA] = \
                int(self.index_mem_quota)
        if "fts" in services:
            ram_quota_dict[CbServer.Settings.FTS_MEM_QUOTA] = \
                int(self.fts_mem_quota)
        self.rest.set_service_mem_quota(ram_quota_dict)

    def run_cbq_query(self, query):
        """
        To run cbq queries
        Note: Do not run this in parallel
        """
        result = self.n1ql_rest_connections[self.n1ql_turn_counter].query_tool(query, timeout=1300)
        self.n1ql_turn_counter = (self.n1ql_turn_counter + 1) % len(self.n1ql_nodes)
        return result

    def wait_for_indexes_to_go_online(self, gsi_index_names, timeout=300):
        """
        Wait for indexes to go online after building the deferred indexes
        """
        self.log.info("Waiting for indexes to go online")
        start_time = time.time()
        stop_time = start_time + timeout
        for gsi_index_name in gsi_index_names:
            while True:
                check_state_query = "SELECT state FROM system:indexes WHERE name='%s'" % gsi_index_name
                result = self.run_cbq_query(check_state_query)
                if result['results'][0]['state'] == "online":
                    break
                if time.time() > stop_time:
                    self.fail("Index availability timeout of index: {0}".format(gsi_index_name))

    def build_deferred_indexes(self, indexes_to_build):
        """
        Build secondary indexes that were deferred
        """
        self.log.info("Building indexes")
        for bucket, bucket_data in indexes_to_build.items():
            for scope, collection_data in bucket_data.items():
                for collection, gsi_index_names in collection_data.items():
                    build_query = "BUILD INDEX on `%s`.`%s`.`%s`(`%s`) " \
                                  "USING GSI" \
                                  % (bucket, scope, collection, ",".join(gsi_index_names))
                    result = self.run_cbq_query(build_query)
                    self.assertTrue(result['status'] == "success", "Build query %s failed." % build_query)
                    self.wait_for_indexes_to_go_online(gsi_index_names)

        query = "select state from system:indexes where state='deferred'"
        result = self.run_cbq_query(query)
        self.log.info("deferred indexes remaining: {0}".format(len(result['results'])))
        query = "select state from system:indexes where state='online'"
        result = self.run_cbq_query(query)
        self.log.info("online indexes count: {0}".format(len(result['results'])))
        self.sleep(60, "Wait after building indexes")

    def create_indexes_and_initialize_queries(self):
        """
        Create gsi indexes on collections - according to number_of_indexes, and
        Initialize select queries on collections that will be run later
        """
        self.log.info("Creating indexes with defer build")
        self.select_queries = list()
        indexes_to_build = dict()
        count = 0
        # ToDO create indexes on ephemeral buckets too using MOI storage
        couchbase_buckets = [bucket for bucket in self.cluster.buckets if bucket.bucketType == "couchbase"]
        for bucket in couchbase_buckets:
            indexes_to_build[bucket.name] = dict()
            for _, scope in bucket.scopes.items():
                indexes_to_build[bucket.name][scope.name] = dict()
                for _, collection in scope.collections.items():
                    gsi_index_name = "gsi-" + str(count)
                    create_index_query = "CREATE INDEX `%s` " \
                                         "ON `%s`.`%s`.`%s`(`age`)" \
                                         "WITH { 'defer_build': true, 'num_replica': 2 }" \
                                         % (gsi_index_name, bucket.name, scope.name, collection.name)
                    result = self.run_cbq_query(create_index_query)
                    # self.assertTrue(result['status'] == "success", "Defer build Query %s failed." % create_index_query)

                    if collection.name not in indexes_to_build[bucket.name][scope.name]:
                        indexes_to_build[bucket.name][scope.name][collection.name] = list()
                    indexes_to_build[bucket.name][scope.name][collection.name].append(gsi_index_name)

                    select_query = "select age from `%s`.`%s`.`%s` where age > 1 limit 1" % (
                        bucket.name, scope.name, collection.name)
                    self.select_queries.append(select_query)

                    count = count + 1
                    if count >= self.number_of_indexes:
                        return indexes_to_build
        return indexes_to_build

    def run_select_query(self):
        """
        Runs select queries in a loop in a separate thread until the thread is asked for to join
        """
        self.log.info("Starting select queries: {0}".format(self.query_thread_flag))
        while self.query_thread_flag:
            for select_query in self.select_queries:
                result = self.run_cbq_query(select_query)
                if result['status'] != "success":
                    self.log.warn("Query failed: {0}".format(select_query))
                # time.sleep(1)
        self.log.info("Stopping select queries")

    def run_ui_stats_queries(self):
        """
        Runs UI stats queries in a loop in a seperate thread unitl the thread is asked for to join
        """
        self.log.info("Starting to poll UI stats queries")
        while self.ui_stats_thread_flag:
            for bucket in self.cluster.buckets:
                _ = StatsHelper(self.cluster.master).post_range_api_metrics(bucket.name)
                self.sleep(10)

    @staticmethod
    def get_fts_param_template():
        fts_param_template = '{ \
                  "type": "fulltext-index", \
                  "name": "%s", \
                  "sourceType": "gocbcore", \
                  "sourceName": "%s", \
                  "planParams": { \
                    "maxPartitionsPerPIndex": 1024, \
                    "indexPartitions": %d \
                  }, \
                  "params": { \
                    "doc_config": { \
                      "docid_prefix_delim": "", \
                      "docid_regexp": "", \
                      "mode": "scope.collection.type_field", \
                      "type_field": "type" \
                    }, \
                    "mapping": { \
                      "analysis": {}, \
                      "default_analyzer": "standard", \
                      "default_datetime_parser": "dateTimeOptional", \
                      "default_field": "_all", \
                      "default_mapping": { \
                        "dynamic": true, \
                        "enabled": false \
                      }, \
                      "default_type": "_default", \
                      "docvalues_dynamic": false, \
                      "index_dynamic": true, \
                      "store_dynamic": false, \
                      "type_field": "_type", \
                      "types": { \
                        "%s.%s": { \
                          "dynamic": true, \
                          "enabled": true \
                        } \
                      } \
                    }, \
                    "store": { \
                      "indexType": "scorch", \
                      "segmentVersion": 15 \
                    } \
                  }, \
                  "sourceParams": {} \
                }'
        return fts_param_template

    def create_fts_indexes(self, count=100, base_name="fts"):
        """
        Creates count number of fts indexes on collections
        count should be less than number of collections
        """
        self.log.info("Creating {} fts indexes ".format(count))
        fts_helper = FtsHelper(self.cluster_util.get_nodes_from_services_map(
            service_type=CbServer.Services.FTS,
            get_all_nodes=False))
        couchbase_buckets = [bucket for bucket in self.cluster.buckets if bucket.bucketType == "couchbase"]
        created_count = 0
        fts_indexes = dict()
        for bucket in couchbase_buckets:
            fts_indexes[bucket.name] = dict()
            for _, scope in bucket.scopes.items():
                fts_indexes[bucket.name][scope.name] = dict()
                for _, collection in scope.collections.items():
                    fts_index_name = base_name + str(created_count)
                    fts_param_template = self.get_fts_param_template()
                    status, content = fts_helper.create_fts_index_from_json(
                        fts_index_name,
                        fts_param_template % (fts_index_name,
                                              bucket.name,
                                              self.fts_index_partitions,
                                              scope.name, collection.name))

                    if status is False:
                        self.fail("Failed to create fts index %s: %s"
                                  % (fts_index_name, content))
                    if collection not in fts_indexes[bucket.name][scope.name]:
                        fts_indexes[bucket.name][scope.name][collection.name] = list()
                    fts_indexes[bucket.name][scope.name][collection.name].append(fts_index_name)
                    created_count = created_count + 1
                    if created_count >= count:
                        return fts_indexes

    def drop_fts_indexes(self, fts_dict, count=10):
        """
        Drop count number of fts indexes using fts name
        from fts_dict
        """
        self.log.info("Dropping {0} fts indexes".format(count))
        fts_helper = FtsHelper(self.cluster_util.get_nodes_from_services_map(
            service_type=CbServer.Services.FTS,
            get_all_nodes=False))
        indexes_dropped = dict()
        dropped_count = 0
        for bucket, bucket_data in fts_dict.items():
            indexes_dropped[bucket] = dict()
            for scope, collection_data in bucket_data.items():
                indexes_dropped[bucket][scope] = dict()
                for collection, fts_index_names in collection_data.items():
                    for fts_index_name in fts_index_names:
                        status, content = fts_helper.delete_fts_index(fts_index_name)
                        if status is False:
                            self.fail("Failed to drop fts index %s: %s"
                                      % (fts_index_name, content))
                        if collection not in indexes_dropped[bucket][scope]:
                            indexes_dropped[bucket][scope][collection] = list()
                        indexes_dropped[bucket][scope][collection].append(fts_index_name)
                        dropped_count = dropped_count + 1
                        if dropped_count >= count:
                            return indexes_dropped

    def create_and_drop_fts_indexes(self, count=10):
        """
        Create and drop count number of fts indexes
        """
        fts_dict = self.create_fts_indexes(count=count, base_name="fts-recreate-")
        self.drop_fts_indexes(fts_dict)

    # Inducing and reverting failures wrt memcached/prometheus process
    def induce_and_revert_failure(self, action):
        target_node = self.servers[-1]  # select last node
        remote = RemoteMachineShellConnection(target_node)
        error_sim = CouchbaseError(self.log, remote)
        error_sim.create(action)
        self.sleep(20, "Wait before reverting the error condition")
        if action in [CouchbaseError.STOP_MEMCACHED, CouchbaseError.STOP_PROMETHEUS]:
            # Revert the simulated error condition explicitly. In kill memcached, prometheus
            # babysitter will bring back the process automatically
            error_sim.revert(action)
        remote.disconnect()

    def custom_induce_failure(self, nodes, failover_action="stop_server"):
        """
        Induce failure on nodes
        """
        for node in nodes:
            if failover_action == "stop_server":
                self.cluster_util.stop_server(node)
            elif failover_action == "firewall":
                self.cluster_util.start_firewall_on_node(node)
            elif failover_action == "stop_memcached":
                self.cluster_util.stop_memcached_on_node(node)
            elif failover_action == "kill_erlang":
                remote = RemoteMachineShellConnection(node)
                remote.kill_erlang()
                remote.disconnect()

    def custom_remove_failure(self, nodes, revert_failure="stop_server"):
        """
        Remove failure
        """
        for node in nodes:
            if revert_failure == "stop_server":
                self.cluster_util.start_server(node)
            elif revert_failure == "firewall":
                self.cluster_util.stop_firewall_on_node(node)
            elif revert_failure == "stop_memcached":
                self.cluster_util.start_memcached_on_node(node)
            elif revert_failure == "kill_erlang":
                self.cluster_util.stop_server(node)
                self.cluster_util.start_server(node)

    def wipe_config_on_removed_nodes(self, remove_nodes):
        """
        Stop servers on nodes that were failed over and removed, and wipe config dir
        """
        for node in remove_nodes:
            self.log.info("Wiping node config and restarting server on {0}".format(node))
            rest = RestConnection(node)
            data_path = rest.get_data_path()
            shell = RemoteMachineShellConnection(node)
            shell.stop_couchbase()
            self.sleep(10)
            shell.cleanup_data_config(data_path)
            shell.start_server()
            self.sleep(10)
            if not RestHelper(rest).is_ns_server_running():
                self.log.error("ns_server {0} is not running.".format(node.ip))
            shell.disconnect()

    def rebalance(self, nodes_in=0, nodes_out=0):
        servs_in = random.sample(self.available_servers, nodes_in)

        self.nodes_cluster = self.cluster.nodes_in_cluster[:]
        for node in self.exclude_nodes:
            self.nodes_cluster.remove(node)
        servs_out = random.sample(self.nodes_cluster, nodes_out)

        if nodes_in == nodes_out:
            self.vbucket_check = False

        services = None
        if self.services_for_rebalance_in and nodes_in > 0:
            services = list()
            services.append(self.services_for_rebalance_in.replace(":", ","))
            services = services * nodes_in

        rebalance_task = self.task.async_rebalance(
            self.cluster.servers[:self.nodes_init], servs_in, servs_out, check_vbucket_shuffling=self.vbucket_check,
            retry_get_process_num=self.retry_get_process_num, services=services)

        self.available_servers = [servs for servs in self.available_servers if servs not in servs_in]
        self.available_servers += servs_out

        self.cluster.nodes_in_cluster.extend(servs_in)
        self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) - set(servs_out))
        return rebalance_task

    def wait_for_rebalance_to_complete(self, task):
        self.task.jython_task_manager.get_task_result(task)
        self.assertTrue(task.result, "Rebalance Failed")

    def set_retry_exceptions(self, doc_loading_spec):
        """
        Exceptions for which mutations need to be retried during
        topology changes
        """
        retry_exceptions = list()
        retry_exceptions.append(SDKException.AmbiguousTimeoutException)
        retry_exceptions.append(SDKException.TimeoutException)
        retry_exceptions.append(SDKException.RequestCanceledException)
        retry_exceptions.append(SDKException.DocumentNotFoundException)
        if self.durability_level:
            retry_exceptions.append(SDKException.DurabilityAmbiguousException)
            retry_exceptions.append(SDKException.DurabilityImpossibleException)
        doc_loading_spec[MetaCrudParams.RETRY_EXCEPTIONS] = retry_exceptions

    def data_load_collection(self, async_load=True, skip_read_success_results=True):
        doc_loading_spec = \
            self.bucket_util.get_crud_template_from_package(self.data_load_spec)
        self.set_retry_exceptions(doc_loading_spec)
        doc_loading_spec[MetaCrudParams.DURABILITY_LEVEL] = self.durability_level
        doc_loading_spec[MetaCrudParams.SKIP_READ_SUCCESS_RESULTS] = skip_read_success_results
        task = self.bucket_util.run_scenario_from_spec(self.task,
                                                       self.cluster,
                                                       self.cluster.buckets,
                                                       doc_loading_spec,
                                                       mutation_num=0,
                                                       async_load=async_load)
        return task

    def reload_data_into_buckets(self):
        """
        Initial data load happens in collections_base. But this method loads
        data again when buckets have been flushed during volume test
        """
        doc_loading_spec = \
            self.bucket_util.get_crud_template_from_package(
                self.data_spec_name)
        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.cluster.buckets,
                doc_loading_spec,
                mutation_num=0,
                batch_size=self.batch_size)
        if doc_loading_task.result is False:
            self.fail("Initial reloading failed")
        ttl_buckets = [
            "multi_bucket.buckets_for_rebalance_tests_with_ttl",
            "multi_bucket.buckets_all_membase_for_rebalance_tests_with_ttl",
            "volume_templates.buckets_for_volume_tests_with_ttl"]

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets(self.cluster.buckets)
        if self.spec_name not in ttl_buckets:
            self.bucket_util.validate_docs_per_collections_all_buckets(
                self.cluster)

        # Prints bucket stats after doc_ops
        self.bucket_util.print_bucket_stats(self.cluster)

    def wait_for_async_data_load_to_complete(self, task):
        self.task.jython_task_manager.get_task_result(task)
        if not self.skip_validations:
            self.bucket_util.validate_doc_loading_results(task)
            if task.result is False:
                self.fail("Doc_loading failed")

    def data_validation_collection(self):
        retry_count = 0
        while retry_count < 10:
            try:
                self.bucket_util._wait_for_stats_all_buckets(
                    self.cluster.buckets)
            except:
                retry_count = retry_count + 1
                self.log.info("ep-queue hasn't drained yet. Retry count: {0}".format(retry_count))
            else:
                break
        if retry_count == 10:
            self.log.info("Attempting last retry for ep-queue to drain")
            self.bucket_util._wait_for_stats_all_buckets(self.cluster.buckets)
        if self.doc_and_collection_ttl:
            self.bucket_util._expiry_pager(self.cluster, val=5)
            self.sleep(400, "wait for doc/collection maxttl to finish")
            items = 0
            self.bucket_util._wait_for_stats_all_buckets(self.cluster.buckets)
            for bucket in self.cluster.buckets:
                items = items + self.bucket_helper_obj.get_active_key_count(bucket)
            if items != 0:
                self.fail("doc count!=0, TTL + rebalance failed")
        else:
            if not self.skip_validations:
                self.bucket_util.validate_docs_per_collections_all_buckets(
                    self.cluster)
            else:
                pass

    def wait_for_failover_or_assert(self, expected_failover_count, timeout=7200):
        # Timeout is kept large for graceful failover
        time_start = time.time()
        time_max_end = time_start + timeout
        actual_failover_count = 0
        while time.time() < time_max_end:
            actual_failover_count = self.get_failover_count()
            if actual_failover_count == expected_failover_count:
                break
            time.sleep(20)
        time_end = time.time()
        if actual_failover_count != expected_failover_count:
            self.log.info(self.rest.print_UI_logs())
        self.assertTrue(actual_failover_count == expected_failover_count,
                        "{0} nodes failed over, expected : {1}"
                        .format(actual_failover_count,
                                expected_failover_count))
        self.log.info("{0} nodes failed over as expected in {1} seconds"
                      .format(actual_failover_count, time_end - time_start))

    def get_failover_count(self):
        rest = RestConnection(self.cluster.master)
        cluster_status = rest.cluster_status()
        failover_count = 0
        # check for inactiveFailed
        for node in cluster_status['nodes']:
            if node['clusterMembership'] == "inactiveFailed":
                failover_count += 1
        return failover_count

    def test_volume_taf(self):
        self.loop = 0
        # self.cluster_utils.set_metadata_purge_interval()
        if self.number_of_indexes > 0:
            # start running select queries thread
            self.query_thread = threading.Thread(target=self.run_select_query)
            self.query_thread_flag = True
            self.query_thread.start()
            # Start running ui stats queries thread
            self.ui_stats_thread = threading.Thread(target=self.run_ui_stats_queries)
            self.ui_stats_thread_flag = True
            self.ui_stats_thread.start()
        self.log.info("Finished steps 1-4 successfully in setup")
        while self.loop < self.iterations:
            if self.loop > 0 or self.flush_buckets_before_indexes_creation:
                self.log.info("Reloading items to buckets")
                self.reload_data_into_buckets()
            #########################################################################################################################
            self.log.info("Step 5: Rebalance in with Loading of docs")
            rebalance_task = self.rebalance(nodes_in=1, nodes_out=0)
            task = self.data_load_collection()
            self.wait_for_rebalance_to_complete(rebalance_task)
            self.wait_for_async_data_load_to_complete(task)
            self.data_validation_collection()
            if self.fts_indexes_to_recreate > 0:
                self.create_and_drop_fts_indexes(count=self.fts_indexes_to_recreate)
            self.bucket_util.print_bucket_stats(self.cluster)
            self.check_logs()
            #########################################################################################################################
            self.log.info("Step 6: Rebalance Out with Loading of docs")
            rebalance_task = self.rebalance(nodes_in=0, nodes_out=1)
            task = self.data_load_collection()
            self.wait_for_rebalance_to_complete(rebalance_task)
            self.wait_for_async_data_load_to_complete(task)
            self.data_validation_collection()
            if self.fts_indexes_to_recreate > 0:
                self.create_and_drop_fts_indexes(count=self.fts_indexes_to_recreate)
            self.bucket_util.print_bucket_stats(self.cluster)
            self.check_logs()
            #######################################################################################################################
            self.log.info("Step 7: Rebalance In_Out with Loading of docs")
            rebalance_task = self.rebalance(nodes_in=2, nodes_out=1)
            task = self.data_load_collection()
            self.wait_for_rebalance_to_complete(rebalance_task)
            self.wait_for_async_data_load_to_complete(task)
            self.data_validation_collection()
            if self.fts_indexes_to_recreate > 0:
                self.create_and_drop_fts_indexes(count=self.fts_indexes_to_recreate)
            self.bucket_util.print_bucket_stats(self.cluster)
            self.check_logs()
            ########################################################################################################################
            self.log.info("Step 8: Swap with Loading of docs")
            rebalance_task = self.rebalance(nodes_in=1, nodes_out=1)
            task = self.data_load_collection()
            self.wait_for_rebalance_to_complete(rebalance_task)
            self.wait_for_async_data_load_to_complete(task)
            self.data_validation_collection()
            self.tasks = []
            if self.fts_indexes_to_recreate > 0:
                self.create_and_drop_fts_indexes(count=self.fts_indexes_to_recreate)
            self.bucket_util.print_bucket_stats(self.cluster)
            self.check_logs()
            ########################################################################################################################
            self.log.info("Step 9: Updating the bucket replica to 2 and rebalance-in")
            bucket_helper = BucketHelper(self.cluster.master)
            for i in range(len(self.cluster.buckets)):
                bucket_helper.change_bucket_props(
                    self.cluster.buckets[i], replicaNumber=2)
            rebalance_task = self.rebalance(nodes_in=1, nodes_out=0)
            task = self.data_load_collection()
            self.wait_for_rebalance_to_complete(rebalance_task)
            self.wait_for_async_data_load_to_complete(task)
            self.data_validation_collection()
            if self.fts_indexes_to_recreate > 0:
                self.create_and_drop_fts_indexes(count=self.fts_indexes_to_recreate)
            self.bucket_util.print_bucket_stats(self.cluster)
            self.check_logs()
            ########################################################################################################################
            self.log.info("Enabling autoreprovison before inducing failure to prevent data loss "
                          "for if there are ephemeral buckets")
            status = self.rest.update_autoreprovision_settings(True, maxNodes=1)
            if not status:
                self.fail("Failed to enable autoreprovison")
            step_count = 9
            for action in [CouchbaseError.STOP_MEMCACHED, CouchbaseError.STOP_PROMETHEUS]:
                step_count = step_count + 1
                self.log.info("Step {0}: {1}".format(step_count, action))
                # TODO Uncomment this after debugging CBQE-6721
                # self.log.info("Forcing durability level: MAJORITY")
                # self.durability_level = "MAJORITY"
                task = self.data_load_collection()
                self.induce_and_revert_failure(action)
                # Rebalance is required after error is reverted
                rebalance_task = self.task.async_rebalance(self.cluster.servers, [], [], retry_get_process_num=self.retry_get_process_num)
                self.wait_for_rebalance_to_complete(rebalance_task)
                self.wait_for_async_data_load_to_complete(task)
                self.data_validation_collection()
                if self.fts_indexes_to_recreate > 0:
                    self.create_and_drop_fts_indexes(count=self.fts_indexes_to_recreate)
                self.bucket_util.print_bucket_stats(self.cluster)
                self.check_logs()
            self.durability_level = ""
            ########################################################################################################################
            step_count = 11
            for failover in ["Graceful", "Hard"]:
                for action in ["RebalanceOut", "FullRecovery", "DeltaRecovery"]:
                    step_count = step_count + 1
                    self.log.info(
                        "Step {0}: {1} Failover a node and {2} that node with data load in parallel".format(step_count,
                                                                                                            failover,
                                                                                                            action))

                    self.std_vbucket_dist = self.input.param("std_vbucket_dist", None)
                    std = self.std_vbucket_dist or 1.0

                    kv_nodes = self.cluster_util.get_kv_nodes()
                    self.log.info("Collecting pre_failover_stats. KV nodes are {0}".format(kv_nodes))
                    prev_failover_stats = self.bucket_util.get_failovers_logs(kv_nodes,
                                                                              self.cluster.buckets)
                    prev_vbucket_stats = self.bucket_util.get_vbucket_seqnos(kv_nodes,
                                                                             self.cluster.buckets)
                    self.sleep(10)

                    disk_replica_dataset, disk_active_dataset = self.bucket_util.get_and_compare_active_replica_data_set_all(
                        kv_nodes, self.cluster.buckets, path=None)

                    # Pick node(s) for failover
                    failover_nodes = list()
                    kv_nodes = self.cluster_util.get_kv_nodes()
                    for node in kv_nodes:
                        if node.ip != self.cluster.master.ip:
                            failover_nodes.append(node)
                            break

                    reset_flag = False
                    if (not self.durability_level) and failover == "Hard":
                        # Force a durability level to prevent data loss during hard failover
                        # TODO Uncomment this after debugging CBQE-6721
                        # self.log.info("Forcing durability level: MAJORITY")
                        # self.durability_level = "MAJORITY"
                        reset_flag = True
                    task = self.data_load_collection()
                    if reset_flag:
                        self.durability_level = ""

                    # Failover the node(s)
                    if failover == "Graceful":
                        failover_result = self.task.failover(self.cluster.nodes_in_cluster,
                                                             failover_nodes=failover_nodes,
                                                             graceful=True, wait_for_pending=120,
                                                             all_at_once=True)
                    else:
                        failover_result = self.task.failover(self.cluster.nodes_in_cluster,
                                                             failover_nodes=failover_nodes,
                                                             graceful=False, wait_for_pending=120,
                                                             all_at_once=True)

                    self.assertTrue(failover_result, "Failover Failed")

                    # Perform the action
                    if action == "RebalanceOut":
                        rebalance_task = self.task.async_rebalance(self.cluster.nodes_in_cluster, [], failover_nodes,
                                                                   retry_get_process_num=self.retry_get_process_num)
                        self.wait_for_rebalance_to_complete(rebalance_task)
                        self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) -
                                                             set(failover_nodes))
                        for node in failover_nodes:
                            self.available_servers.append(node)
                        self.sleep(10)
                    else:
                        if action == "FullRecovery":
                            for failover_node in failover_nodes:
                                self.rest.set_recovery_type(otpNode='ns_1@' + failover_node.ip,
                                                            recoveryType="full")
                        elif action == "DeltaRecovery":
                            for failover_node in failover_nodes:
                                self.rest.set_recovery_type(otpNode='ns_1@' + failover_node.ip,
                                                            recoveryType="delta")

                        rebalance_task = self.task.async_rebalance(
                            self.cluster.nodes_in_cluster, [], [], retry_get_process_num=self.retry_get_process_num)
                        self.wait_for_rebalance_to_complete(rebalance_task)
                        self.sleep(10)

                    self.wait_for_async_data_load_to_complete(task)
                    self.data_validation_collection()

                    kv_nodes = self.cluster_util.get_kv_nodes()
                    self.log.info("Collecting post_failover_stats. KV nodes are {0}".format(kv_nodes))
                    self.bucket_util.compare_failovers_logs(prev_failover_stats, kv_nodes,
                                                            self.cluster.buckets)
                    self.sleep(10)

                    self.bucket_util.data_analysis_active_replica_all(
                        disk_active_dataset, disk_replica_dataset,
                        kv_nodes,
                        self.cluster.buckets, path=None)
                    self.bucket_util.vb_distribution_analysis(
                        self.cluster,
                        servers=kv_nodes, buckets=self.cluster.buckets,
                        num_replicas=2,
                        std=std, total_vbuckets=self.cluster_util.vbuckets)
                    self.sleep(10)
                    self.tasks = []
                    # Bring back the rebalance out node back to cluster for further steps
                    if action == "RebalanceOut":
                        self.sleep(120)
                        self.log.info("Rebalancing-in a node")
                        rebalance_task = self.rebalance(nodes_in=1, nodes_out=0)
                        # self.sleep(600)
                        self.wait_for_rebalance_to_complete(rebalance_task)
                    if self.fts_indexes_to_recreate > 0:
                        self.create_and_drop_fts_indexes(count=self.fts_indexes_to_recreate)
                    self.bucket_util.print_bucket_stats(self.cluster)
                    self.check_logs()
            ########################################################################################################################
            self.log.info("Step 18: Updating the bucket replica to 1")
            bucket_helper = BucketHelper(self.cluster.master)
            for i in range(len(self.cluster.buckets)):
                bucket_helper.change_bucket_props(
                    self.cluster.buckets[i], replicaNumber=1)
            rebalance_task = self.task.async_rebalance(self.cluster.servers, [], [], retry_get_process_num=self.retry_get_process_num)
            task = self.data_load_collection()
            self.wait_for_rebalance_to_complete(rebalance_task)
            self.wait_for_async_data_load_to_complete(task)
            self.data_validation_collection()
            self.tasks = []
            if self.fts_indexes_to_recreate > 0:
                self.create_and_drop_fts_indexes(count=self.fts_indexes_to_recreate)
            self.bucket_util.print_bucket_stats(self.cluster)
            self.check_logs()
            ########################################################################################################################
            self.cluster.nodes_in_cluster = self.cluster.servers
            step_count = 19
            removed_nodes = list()  # total list of all nodes that will be removed
            if self.perform_quorum_failover:
                self.log.info("Step {0}: Quorum failover nodes".format(step_count))
                # keep performing QF until one node is left
                while len(self.cluster.nodes_in_cluster) != 1:
                    majority_number = int(math.ceil(len(self.cluster.nodes_in_cluster) / 2.0))
                    self.nodes_cluster = self.cluster.nodes_in_cluster[:]
                    self.nodes_cluster.remove(self.cluster.master)
                    remove_nodes = random.sample(self.nodes_cluster, majority_number)
                    self.custom_induce_failure(remove_nodes, failover_action="stop_server")
                    self.sleep(10, "Wait after inducing failure")
                    self.log.info("Failing over nodes explicitly {0}".format(remove_nodes))
                    result = self.task.failover(self.cluster.nodes_in_cluster, failover_nodes=remove_nodes,
                                                graceful=False, wait_for_pending=120,
                                                allow_unsafe=True,
                                                all_at_once=True)
                    self.assertTrue(result, "Failover Failed")
                    self.custom_remove_failure(nodes=remove_nodes, revert_failure="stop_server")
                    self.sleep(15)
                    self.wipe_config_on_removed_nodes(remove_nodes)
                    for node in remove_nodes:
                        removed_nodes.append(node)
                    self.cluster.nodes_in_cluster = [node for node in self.cluster.nodes_in_cluster
                                                     if node not in remove_nodes]
                # add back all the nodes with kv service
                rebalance_task = self.task.async_rebalance(self.cluster.nodes_in_cluster, removed_nodes, [],
                                                           retry_get_process_num=self.retry_get_process_num)
                self.wait_for_rebalance_to_complete(rebalance_task)
                self.cluster.nodes_in_cluster = self.cluster.servers[:]
                step_count = step_count + 1
                self.bucket_util.print_bucket_stats(self.cluster)
                self.check_logs()
            #################################################################
            self.log.info("Step {0}: Flush bucket(s) and start the entire process again".format(step_count))
            self.loop += 1
            if self.loop < self.iterations:
                # Flush bucket(s)
                self.bucket_util.flush_all_buckets(
                    self.cluster, skip_resetting_num_items=True)
                self.sleep(10)
                if len(self.cluster.nodes_in_cluster) > self.nodes_init:
                    self.nodes_cluster = self.cluster.nodes_in_cluster[:]
                    self.nodes_cluster.remove(self.cluster.master)
                    servs_out = random.sample(self.nodes_cluster,
                                              int(len(self.cluster.nodes_in_cluster) - self.nodes_init))
                    rebalance_task = self.task.async_rebalance(
                        self.cluster.servers[:self.nodes_init], [], servs_out, retry_get_process_num=self.retry_get_process_num)
                    self.wait_for_rebalance_to_complete(rebalance_task)
                    self.available_servers += servs_out
                    self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) - set(servs_out))
            else:
                if self.number_of_indexes > 0:
                    self.close_all_threads()
                self.log.info("Volume Test Run Complete")
        ############################################################################################################################
