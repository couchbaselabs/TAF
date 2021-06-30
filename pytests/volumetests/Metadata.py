import random
import re
import time

from com.couchbase.client.java import *
from com.couchbase.client.java.json import *
from com.couchbase.client.java.query import *

from Cb_constants import CbServer
from FtsLib.FtsOperations import FtsHelper
from membase.api.rest_client import RestConnection, RestHelper
from TestInput import TestInputSingleton
from bucket_collections.collections_base import CollectionBase

from platform_utils.remote.remote_util import RemoteMachineShellConnection


class volume(CollectionBase):
    def setUp(self):
        self.input = TestInputSingleton.input
        self.input.test_params.update({"default_bucket": False})
        super(volume, self).setUp()
        self.bucket_util._expiry_pager(self.cluster, val=5)
        self.rest = RestConnection(self.servers[0])
        self.available_servers = list()
        self.available_servers = self.cluster.servers[self.nodes_init:]
        self.nodes_in_cluster = self.cluster.servers[:self.nodes_init]
        self.exclude_nodes = [self.cluster.master]
        self.skip_check_logs = False
        self.iterations = self.input.param("iterations", 1)
        self.retry_get_process_num = self.input.param("retry_get_process_num", 400)
        self.kv_mem_quota = self.input.param("kv_mem_quota", 22000)
        self.index_mem_quota = self.input.param("index_mem_quota", 22700)
        self.skip_index_creation_in_setup = self.input.param("skip_index_creation_in_setup", False)
        self.tombstone_purge_age = self.input.param("tombstone_purge_age", 300)
        shell = RemoteMachineShellConnection(self.cluster.master)
        shell.enable_diag_eval_on_non_local_hosts()
        shell.disconnect()
        self.rest.update_tombstone_purge_age_for_removal(self.tombstone_purge_age)
        self.index_setup()
        self.fts_setup()

    def index_setup(self):
        # Initialize parameters for index querying
        self.n1ql_nodes = None
        self.number_of_indexes_per_coll = self.input.param("number_of_indexes_per_coll",
                                                           5)  # with 1 replicas = total 10 indexes per coll
        self.max_2i_count = self.input.param("max_2i_count", 10000)
        # Below is the total num of indexes to drop after each rebalance (and recreate them)
        self.num_indexes_to_drop = self.input.param("num_indexes_to_drop", 500)
        self.bucket_util.flush_all_buckets(self.cluster,
                                           skip_resetting_num_items=True)
        self.set_memory_quota(services=["kv", "index"])
        self.n1ql_nodes = self.cluster_util.get_nodes_from_services_map(
            cluster=self.cluster,
            service_type=CbServer.Services.N1QL,
            get_all_nodes=True,
            servers=self.cluster.servers[:self.nodes_init])
        self.n1ql_rest_connections = list()
        for n1ql_node in self.n1ql_nodes:
            self.n1ql_rest_connections.append(RestConnection(n1ql_node))
            self.exclude_nodes.append(n1ql_node)
        self.n1ql_turn_counter = 0  # To distribute the turn of using n1ql nodes for query. Start with first node
        if not self.skip_index_creation_in_setup:
            self.indexes_to_build = self.create_indexes()
            self.build_deferred_indexes(self.indexes_to_build)

    def fts_setup(self):
        """
        Create initial fts indexes
        """
        self.fts_indexes_to_create = self.input.param("fts_indexes_to_create", 0)
        self.fts_indexes_to_recreate = self.input.param("fts_indexes_to_recreate", 0)
        self.fts_mem_quota = self.input.param("fts_mem_quota", 22000)
        self.fts_nodes = self.cluster_util.get_nodes_from_services_map(
            cluster=self.cluster,
            service_type=CbServer.Services.FTS,
            get_all_nodes=True,
            servers=self.cluster.servers[:self.nodes_init])

        if self.fts_indexes_to_create > 0:
            self.fts_index_partitions = self.input.param("fts_index_partition", 6)
            self.set_memory_quota(services=["fts"])
            self.fts_dict = self.create_fts_indexes(self.fts_indexes_to_create)

    def tearDown(self):
        self.log.info("Printing bucket stats before teardown")
        self.bucket_util.print_bucket_stats(self.cluster)
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
                    #TODO: Change it back to "fail" after MB-45990 is fixed post-CC
                    self.log.critical("Index availability timeout of index: {0}".format(gsi_index_name))

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
        couchbase_buckets = [bucket for bucket in self.cluster.buckets if bucket.bucketType == "couchbase"]
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
                        if count >= self.max_2i_count:
                            return indexes_to_build
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
        self.log.debug("Creating {} fts indexes ".format(count))
        fts_helper = FtsHelper(self.fts_nodes[0])
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
        self.log.debug("Dropping {0} fts indexes".format(count))
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
        self.drop_fts_indexes(fts_dict, count=count)

    def create_and_drop_metakv_keys(self, metakv_cycles=50,
                                    sleep_time_between=3, batches=1000):
        """
        :param metakv_cycles: Number of create and drops cycles
        :param sleep_time_between: Sleep time between create and delete
        :param batches: number of creates before starting drops
        """
        self.log.info("Creating & dropping metakv keys, cycles %s, batches %s"
                      % (metakv_cycles, batches))
        for cycle in range(metakv_cycles):
            keys = list()
            for batch in range(batches):
                key = "random_key-cycle-" + str(cycle) + "-batch-" + str(batch)
                self.rest.create_metakv_key(key=key, value=key)
                keys.append(key)
            self.sleep(sleep_time_between)
            for key in keys:
                self.rest.delete_metakv_key(key=key)

    def wait_for_rebalance_to_complete(self, task):
        self.task.jython_task_manager.get_task_result(task)
        self.assertTrue(task.result, "Rebalance Failed")

    def get_latest_tombstones_purged_count(self, nodes=None):
        """
        grep debug log for the latest tombstones purged count
        Return dict with key = node_ip and value = ts purged count
        as string
        """
        ts_purged_count_dict = dict()
        if nodes is None:
            nodes = self.cluster_util.get_nodes_in_cluster(self.cluster)
        for node in nodes:
            shell = RemoteMachineShellConnection(node)
            command = "grep 'tombstone_agent:purge:' /opt/couchbase/var/lib/couchbase/logs/debug.log | tail -1"
            output, _ = shell.execute_command(command)
            try:
                if len(output) == 0:
                    self.log.info("Debug.log must have got rotated; trying to find the latest gz file")
                    command = "find /opt/couchbase/var/lib/couchbase/logs -name 'debug.log.*.gz' -print0 " \
                              "| xargs -r -0 ls -1 -t | tail -1"
                    output, _ = shell.execute_command(command)
                    log_file = output[0]
                    command = "zgrep 'tombstone_agent:purge:' %s | tail -1" % log_file
                    output, _ = shell.execute_command(command)
                self.log.info("On {0} {1}".format(node.ip, output))
                shell.disconnect()
                purged_count = re.findall("Purged [0-9]+", output[0])[0].split(" ")[1]
                ts_purged_count_dict[node.ip] = purged_count
            except Exception as e:
                print(e)
                ts_purged_count_dict[node.ip] = 0
        return ts_purged_count_dict

    def get_ns_config_deleted_keys_count(self, nodes=None):
        """
        get a dump of ns_config and grep for "_deleted" to get
        deleted keys count
        Return dict with key = node_ip and value = deleted key count
        as string
        """
        deleted_keys_count_dict = dict()
        if nodes is None:
            nodes = self.cluster_util.get_nodes_in_cluster(self.cluster)
        for node in nodes:
            shell = RemoteMachineShellConnection(node)
            shell.enable_diag_eval_on_non_local_hosts()
            command = "curl --silent -u %s:%s http://localhost:8091/diag/eval -d 'ns_config:get()' " \
                      "| grep '_deleted' | wc -l" % (self.rest.username, self.rest.password)
            output, _ = shell.execute_command(command)
            shell.disconnect()
            deleted_keys_count_dict[node.ip] = output[0].strip('\n')
        return deleted_keys_count_dict

    def validation_for_ts_purging(self, nodes=None):
        """
        1. Disable ts purger
        2. Create fts indexes (to create metakv, ns_config entries)
        3. Delete fts indexes
        4. Grep ns_config for '_deleted' to get total deleted keys count
        5. Sleep for "age" minutes
        6. enable ts purger and age  & wait for 1 min
        7. Grep for debug.log and check for latest tombstones purged count
        8. Validate step4 count matches step 7 count for all nodes
        """
        if nodes is None:
            nodes = self.cluster_util.get_nodes_in_cluster(self.cluster)

        self.rest.update_tombstone_purge_age_for_removal(self.tombstone_purge_age)
        self.rest.disable_tombstone_purger()

        # self.log.info("Creating and deleting FTS index for {0} times".
        #               format(self.fts_indexes_to_recreate))
        # for i in range(self.fts_indexes_to_recreate):
        #     fts_dict = self.create_fts_indexes(count=1, base_name="recreate-" + str(i) + str(i))
        #     self.drop_fts_indexes(fts_dict, count=1)
        # self.sleep(60, "Wait after dropping fts indexes")
        self.create_and_drop_metakv_keys(metakv_cycles=self.metakv_cycles)
        self.sleep(60, "Wait after dropping keys")

        deleted_keys_count_dict = self.get_ns_config_deleted_keys_count(nodes=nodes)

        self.sleep(self.tombstone_purge_age, "waiting for purging to finish")
        self.rest.enable_tombstone_purger()
        self.sleep(60, "Waiting for purger agent to wake up")

        ts_purged_count_dict = self.get_latest_tombstones_purged_count(nodes=nodes)
        for node in nodes:
            expected_ts_purge_count = deleted_keys_count_dict[node.ip]
            actual_ts_purge_count = ts_purged_count_dict[node.ip]
            if expected_ts_purge_count != actual_ts_purge_count:
                self.log.critical("On {0}: Expected ts purged count: {1} "
                                  "Actual ts purged count: {2}".
                                  format(node.ip, expected_ts_purge_count,
                                         actual_ts_purge_count))

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
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        if self.spec_name not in ttl_buckets:
            self.bucket_util.validate_docs_per_collections_all_buckets(
                self.cluster)

        # Prints bucket stats after doc_ops
        self.bucket_util.print_bucket_stats(self.cluster)

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

    def test_nsconfig_tombstones_purging_volume(self):
        self.reload_data_into_buckets()
        self.metakv_cycles = self.input.param("metakv_cycles", 500)
        self.cycles = self.input.param("cycles", 5)
        cycles = 0
        while cycles < self.cycles:
            self.log.info("Cycle: {0}".format(cycles))
            # KV rebalance
            add_nodes = random.sample(self.available_servers, 2)
            operation = self.task.async_rebalance(self.nodes_in_cluster, add_nodes, [],
                                                  services=["kv", "kv"],
                                                  retry_get_process_num=self.retry_get_process_num)
            # self.log.info("Creating and deleting FTS index for {0} times".
            #               format(self.fts_indexes_to_recreate))
            # for i in range(self.fts_indexes_to_recreate / 10):
            #     fts_dict = self.create_fts_indexes(count=10, base_name="recreate-reb-" + str(i))
            #     self.drop_fts_indexes(fts_dict, count=10)
            self.create_and_drop_metakv_keys(metakv_cycles=self.metakv_cycles)
            self.wait_for_rebalance_to_complete(operation)
            for node in add_nodes:
                self.available_servers.remove(node)
                self.nodes_in_cluster.append(node)
            self.validation_for_ts_purging()
            ############################################################################################
            # Index rebalance + drop and recreate indexes
            add_nodes = random.sample(self.available_servers, 2)
            operation = self.task.async_rebalance(self.nodes_in_cluster, add_nodes, [],
                                                  services=["index", "index"],
                                                  retry_get_process_num=self.retry_get_process_num)
            # self.log.info("Creating and deleting FTS index for {0} times".
            #               format(self.fts_indexes_to_recreate))
            # for i in range(self.fts_indexes_to_recreate / 10):
            #     fts_dict = self.create_fts_indexes(count=10, base_name="recreate-reb-" + str(i))
            #     self.drop_fts_indexes(fts_dict, count=10)
            self.create_and_drop_metakv_keys(metakv_cycles=self.metakv_cycles)
            self.wait_for_rebalance_to_complete(operation)
            for node in add_nodes:
                self.available_servers.remove(node)
                self.nodes_in_cluster.append(node)
            indexes_dropped = self.drop_indexes(num_indexes_to_drop=self.num_indexes_to_drop)
            self.recreate_dropped_indexes(indexes_dropped)
            self.validation_for_ts_purging()
            ############################################################################################
            # fts rebalance + create and drop fts indexes
            add_nodes = random.sample(self.available_servers, 2)
            operation = self.task.async_rebalance(self.nodes_in_cluster, add_nodes, [],
                                                  services=["fts", "fts"],
                                                  retry_get_process_num=self.retry_get_process_num)
            # self.log.info("Creating and deleting FTS index for {0} times".
            #               format(self.fts_indexes_to_recreate))
            # for i in range(self.fts_indexes_to_recreate / 10):
            #     fts_dict = self.create_fts_indexes(count=10, base_name="recreate-reb-" + str(i))
            #     self.drop_fts_indexes(fts_dict, count=10)
            self.create_and_drop_metakv_keys(metakv_cycles=self.metakv_cycles)
            self.wait_for_rebalance_to_complete(operation)
            for node in add_nodes:
                self.available_servers.remove(node)
                self.nodes_in_cluster.append(node)
            self.validation_for_ts_purging()
            ############################################################################################
            remove_nodes = list()
            kv_nodes = self.cluster_util.get_nodes_from_services_map(
                cluster=self.cluster,
                service_type=CbServer.Services.KV,
                get_all_nodes=True,
                servers=self.nodes_in_cluster)
            kv_nodes.remove(self.cluster.master)
            remove_nodes.extend(random.sample(kv_nodes, 2))
            gsi_nodes = self.cluster_util.get_nodes_from_services_map(
                cluster=self.cluster,
                service_type=CbServer.Services.INDEX,
                get_all_nodes=True,
                servers=self.nodes_in_cluster)
            remove_nodes.extend(random.sample(gsi_nodes, 2))
            fts_nodes = self.cluster_util.get_nodes_from_services_map(
                cluster=self.cluster,
                service_type=CbServer.Services.FTS,
                get_all_nodes=True,
                servers=self.nodes_in_cluster)
            remove_nodes.extend(random.sample(fts_nodes, 2))
            operation = self.task.async_rebalance(self.nodes_in_cluster, [], remove_nodes,
                                                  retry_get_process_num=self.retry_get_process_num)
            # for i in range(self.fts_indexes_to_recreate / 10):
            #     fts_dict = self.create_fts_indexes(count=10, base_name="recreate-reb-" + str(i))
            #     self.drop_fts_indexes(fts_dict, count=10)
            self.create_and_drop_metakv_keys(metakv_cycles=self.metakv_cycles)
            self.wait_for_rebalance_to_complete(operation)
            for node in remove_nodes:
                self.available_servers.append(node)
                self.nodes_in_cluster.remove(node)
            self.validation_for_ts_purging()
            ############################################################################################
            self.log.info("Killing erlang on {0}".format(self.cluster.master))
            shell = RemoteMachineShellConnection(self.cluster.master)
            shell.kill_erlang()
            shell.disconnect()
            self.sleep(120, "Wait after killing erlang")
            shell = RemoteMachineShellConnection(self.cluster.master)
            self.log.info("restarting server on {0}".format(self.cluster.master))
            shell.restart_couchbase()
            shell.disconnect()
            self.validation_for_ts_purging()
            ############################################################################################
            cycles = cycles + 1
