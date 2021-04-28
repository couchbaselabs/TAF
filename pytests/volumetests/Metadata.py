import random
import time

from com.couchbase.client.java import *
from com.couchbase.client.java.json import *
from com.couchbase.client.java.query import *

from Cb_constants import CbServer
from membase.api.rest_client import RestConnection, RestHelper
from TestInput import TestInputSingleton
from bucket_collections.collections_base import CollectionBase


class volume(CollectionBase):
    def setUp(self):
        self.input = TestInputSingleton.input
        self.input.test_params.update({"default_bucket": False})
        super(volume, self).setUp()
        self.bucket_util._expiry_pager(val=5)
        self.rest = RestConnection(self.servers[0])
        self.available_servers = list()
        self.available_servers = self.cluster.servers[self.nodes_init:]
        self.nodes_in_cluster = self.cluster.servers[:self.nodes_init]
        self.exclude_nodes = [self.cluster.master]
        self.skip_check_logs = False
        self.iterations = self.input.param("iterations", 1)
        self.retry_get_process_num = self.input.param("retry_get_process_num", 400)
        self.kv_mem_quota = self.input.param("kv_mem_quota", 2000)
        self.index_mem_quota = self.input.param("index_mem_quota", 22700)
        self.skip_index_creation_in_setup = self.input.param("skip_index_creation_in_setup", False)
        self.index_setup()

    def index_setup(self):
        # Initialize parameters for index querying
        self.n1ql_nodes = None
        self.number_of_indexes_per_coll = self.input.param("number_of_indexes_per_coll",
                                                           5)  # with 1 replicas = total 10 indexes per coll
        # Below is the total num of indexes to drop after each rebalance (and recreate them)
        self.num_indexes_to_drop = self.input.param("num_indexes_to_drop", 500)
        self.bucket_util.flush_all_buckets(self.cluster.master, skip_resetting_num_items=True)
        self.set_memory_quota_kv_index()
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
        if not self.skip_index_creation_in_setup:
            self.indexes_to_build = self.create_indexes()
            self.build_deferred_indexes(self.indexes_to_build)

    def tearDown(self):
        self.log.info("Printing bucket stats before teardown")
        self.bucket_util.print_bucket_stats()
        if self.collect_pcaps:
            self.start_fetch_pcaps()
        if not self.skip_check_logs:
            self.check_logs()

    def check_logs(self):
        self.log.info("Checking logs on {0}".format(self.servers))
        result = self.check_coredump_exist(self.servers, force_collect=True)
        if not self.crash_warning:
            self.skip_check_logs = True  # Setting this, as we don't have to check logs again in tearDown
            self.assertFalse(result, msg="Cb_log file validation failed")
        if self.crash_warning and result:
            self.log.warn("CRASH | CRITICAL | WARN messages found in cb_logs")

    def set_memory_quota_kv_index(self):
        """
        To set memory quota of KV and index services before creating indexes
        """
        self.rest.set_service_mem_quota(
            {CbServer.Settings.KV_MEM_QUOTA: int(self.kv_mem_quota),
             CbServer.Settings.INDEX_MEM_QUOTA: int(self.index_mem_quota)})

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
                    build_query = "BUILD INDEX on `%s`.`%s`.`%s`(%s) " \
                                  "USING GSI" \
                                  % (bucket, scope, collection, gsi_index_names)
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

    def create_indexes(self, gsi_base_name="gsi"):
        """
        Create gsi indexes on collections - according to number_of_indexes_per_coll
        """
        self.log.info("Creating indexes with defer build")
        indexes_to_build = dict()
        count = 0
        couchbase_buckets = [bucket for bucket in self.bucket_util.buckets if bucket.bucketType == "couchbase"]
        for bucket in couchbase_buckets:
            indexes_to_build[bucket.name] = dict()
            for _, scope in bucket.scopes.items():
                indexes_to_build[bucket.name][scope.name] = dict()
                for _, collection in scope.collections.items():
                    for _ in range(self.number_of_indexes_per_coll):
                        gsi_index_name = gsi_base_name + str(count)
                        create_index_query = "CREATE INDEX `%s` " \
                                             "ON `%s`.`%s`.`%s`(`age`) " \
                                             "WITH { 'defer_build': true, 'num_replica': 0 }" \
                                             % (gsi_index_name, bucket.name, scope.name, collection.name)
                        result = self.run_cbq_query(create_index_query)
                        # self.assertTrue(result['status'] == "success", "Defer build Query %s failed." % create_index_query)

                        if collection.name not in indexes_to_build[bucket.name][scope.name]:
                            indexes_to_build[bucket.name][scope.name][collection.name] = list()
                        indexes_to_build[bucket.name][scope.name][collection.name].append(gsi_index_name)
                        count = count + 1
        return indexes_to_build

    def recreate_dropped_indexes(self, indexes_dropped):
        """
        Recreate dropped indexes given indexes_dropped dict
        """
        self.log.info("Recreating dropped indexes")
        for bucket, bucket_data in indexes_dropped.items():
            for scope, collection_data in bucket_data.items():
                for collection, gsi_index_names in collection_data.items():
                    for gsi_index_name in gsi_index_names:
                        create_index_query = "CREATE INDEX `%s` " \
                                             "ON `%s`.`%s`.`%s`(`age`)" \
                                             "WITH { 'defer_build': true, 'num_replica': 0 }" \
                                             % (gsi_index_name, bucket, scope, collection)
                        result = self.run_cbq_query(create_index_query)
        self.build_deferred_indexes(indexes_dropped)

    def drop_indexes(self, num_indexes_to_drop=15):
        """
        Drop gsi indexes
        Returns dropped indexes dict
        """
        self.log.info("Dropping {0} indexes".format(num_indexes_to_drop))
        indexes_dropped = dict()
        count = 0
        for bucket, bucket_data in self.indexes_to_build.items():
            indexes_dropped[bucket] = dict()
            for scope, collection_data in bucket_data.items():
                indexes_dropped[bucket][scope] = dict()
                for collection, gsi_index_names in collection_data.items():
                    for gsi_index_name in gsi_index_names:
                        drop_index_query = "DROP INDEX `%s` ON " \
                                           "`%s`.`%s`.`%s`" \
                                           "USING GSI" \
                                           % (gsi_index_name, bucket, scope, collection)
                        result = self.run_cbq_query(drop_index_query)
                        if collection not in indexes_dropped[bucket][scope]:
                            indexes_dropped[bucket][scope][collection] = list()
                        indexes_dropped[bucket][scope][collection].append(gsi_index_name)
                        count = count + 1
                        if count >= num_indexes_to_drop:
                            return indexes_dropped

    def wait_for_rebalance_to_complete(self, task):
        self.task.jython_task_manager.get_task_result(task)
        self.assertTrue(task.result, "Rebalance Failed")

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
                self.bucket_util.buckets,
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
        self.bucket_util._wait_for_stats_all_buckets()
        if self.spec_name not in ttl_buckets:
            self.bucket_util.validate_docs_per_collections_all_buckets()

        # Prints bucket stats after doc_ops
        self.bucket_util.print_bucket_stats()

    def test_volume_taf(self):
        """
        Keep rebalancing-in until you exhaust available nodes
        After each rebalance-in drop some indexes and re-create them
        """
        self.log.info("Finished steps 1-4 successfully in setup")
        self.reload_data_into_buckets()
        step_count = 1
        while len(self.available_servers) != 0:
            add_node = self.available_servers[0]
            self.log.info("Step: {0} Adding node {1}".format(step_count, add_node.ip))
            operation = self.task.async_rebalance(self.nodes_in_cluster, [add_node], [],
                                                  services=["index"],
                                                  retry_get_process_num=self.retry_get_process_num)
            self.wait_for_rebalance_to_complete(operation)
            self.available_servers.remove(add_node)
            self.nodes_in_cluster.append(add_node)

            self.log.info("################# Dropping indexes ###################")
            indexes_dropped = self.drop_indexes(num_indexes_to_drop=self.num_indexes_to_drop)

            query = "select state from system:indexes where state='online'"
            result = self.run_cbq_query(query)
            self.log.info("online indexes count: {0}".format(len(result['results'])))

            self.log.info("################# Recreating indexes ###################")
            self.recreate_dropped_indexes(indexes_dropped)

            self.check_logs()
            step_count = step_count + 1

    def test_volume_rebalance_cycles(self):
        """
        1. Rebalance-in
        2. drop and recreate indexes
        3. Rebalance-out and repeat this cycle
        """
        self.reload_data_into_buckets()
        cycles = 1
        while cycles < 6:
            add_node = self.available_servers[0]
            self.log.info("Cycle: {0}".format(cycles))
            operation = self.task.async_rebalance(self.nodes_in_cluster, [add_node], [],
                                                  services=["index"],
                                                  retry_get_process_num=self.retry_get_process_num)
            self.wait_for_rebalance_to_complete(operation)
            self.available_servers.remove(add_node)
            self.nodes_in_cluster.append(add_node)

            self.log.info("################# Dropping indexes ###################")
            indexes_dropped = self.drop_indexes(num_indexes_to_drop=self.num_indexes_to_drop)

            query = "select state from system:indexes where state='online'"
            result = self.run_cbq_query(query)
            self.log.info("online indexes count: {0}".format(len(result['results'])))

            self.log.info("################# Recreating indexes ###################")
            self.recreate_dropped_indexes(indexes_dropped)

            remove_node = self.nodes_in_cluster[-1]
            operation = self.task.async_rebalance(self.nodes_in_cluster, [], [remove_node],
                                                  retry_get_process_num=self.retry_get_process_num)
            self.wait_for_rebalance_to_complete(operation)
            self.available_servers.append(remove_node)
            self.nodes_in_cluster.remove(remove_node)

            cycles = cycles + 1

    def test_volume_incremental_index_creation(self):
        """
        Perform following steps in a loop until 10000 indexes are created.

        Step 1. Add all the remaining indexer nodes in the cluster.
        Step 2. Remove some indexer nodes from the cluster - by keeping some indexer nodes.
        No of nodes remaining are proportional to number of indexes.
        Step 3. Create 2000 indexes
        Step 4. Build all newly crated 2000 indexes, 2 indexes at a time.
        Step 5. Wait for indexes to become active synchronously.
        """
        self.reload_data_into_buckets()
        self.available_index_nodes = self.available_servers  # dynamic variable to keep track of free index nodes
        self.total_available_index_nodes = self.available_servers  # nodes reserved for index nodes. Remains constant
        self.number_of_nodes_to_rebalance_out = [14, 10, 6, 2]

        for increment_count in range(5):
            # Rebalance-in all available free nodes
            services = ["index"]
            services = services * (len(self.available_index_nodes))
            operation = self.task.async_rebalance(self.nodes_in_cluster, self.available_index_nodes, [],
                                                  services=services,
                                                  retry_get_process_num=self.retry_get_process_num)
            self.wait_for_rebalance_to_complete(operation)
            for node in self.available_index_nodes:
                self.nodes_in_cluster.append(node)

            # Rebalance-out a few index nodes as appropriate. Not needed for last step
            # as in the last step we need all nodes to be there in cluster
            if increment_count != 4:
                remove_nodes = random.sample(self.total_available_index_nodes,
                                             self.number_of_nodes_to_rebalance_out[increment_count])
                operation = self.task.async_rebalance(self.nodes_in_cluster, [], remove_nodes,
                                                      retry_get_process_num=self.retry_get_process_num)
                self.wait_for_rebalance_to_complete(operation)
                self.available_index_nodes = remove_nodes
                for node in remove_nodes:
                    self.nodes_in_cluster.remove(node)

            self.number_of_indexes_per_coll = 2
            self.n1ql_turn_counter = 0  # To distribute the turn of using n1ql nodes for query. Start with first node
            gsi_base_name = "gsi-set-" + str(increment_count) + "-"
            self.indexes_to_build = self.create_indexes(gsi_base_name=gsi_base_name)
            self.build_deferred_indexes(self.indexes_to_build)
