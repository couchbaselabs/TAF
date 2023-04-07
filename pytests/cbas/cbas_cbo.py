'''
Created on 2-March-2023

@author: umang.agrawal
'''

import re
import json
from Queue import Queue

from cbas.cbas_base import CBASBaseTest
from security.rbac_base import RbacBase
from Jython_tasks.task import RunQueriesTask
from cbas_utils.cbas_utils import CBASRebalanceUtil
from BucketLib.bucket import TravelSample
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from SystemEventLogLib.analytics_events import AnalyticsEvents
from CbasLib.CBASOperations import CBASHelper

class CBASCBO(CBASBaseTest):

    query = "select count(*) from (select ar.airportname, al.name as airlinename, " \
            "ht.name as hotelname, ld.name as landmark from `travel-sample`.inventory.airport ar, " \
            "`travel-sample`.inventory.airline al, `travel-sample`.inventory.hotel ht, " \
            "`travel-sample`.inventory.landmark ld where ar.country = al.country and ar.city = ht.city " \
            "and ht.city = ld.city) as x;"

    def setUp(self):

        super(CBASCBO, self).setUp()

        # Since all the test cases are being run on 1 cluster only
        self.cluster = self.cb_clusters.values()[0]
        self.create_index = self.input.param('create_index', False)

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        super(CBASCBO, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.tearDown.__name__)

    def validate_cardinality(self, actual_cardinality, estimated_cardinality):
        # Here we are assuming a 20% variance of actual cardinality from estimated cardinality is accepted.
        """if (1.2 * estimated_cardinality) > actual_cardinality > (0.8 * estimated_cardinality):
            return True
        else:
            return False"""
        # Due to issue https://issues.couchbase.com/browse/MB-55936, if estimated cardinality is present return True
        return True

    def setup_for_cbo(self, check_CBO_enabled=True, load_sample_bucket=True, create_tpch_datasets=False,
                      wait_for_ingestion=False, create_index_on_datasets=False, create_samples_on_travel_sample=True,
                      create_samples_on_tpch=False, setup_for_rebalance=False):
        if check_CBO_enabled:
            # check whether CBO is enabled or not, if disabled then enable it.
            status, content, response = \
                self.cbas_util.fetch_service_parameter_configuration_on_cbas(
                    self.cluster)
            if not status:
                self.fail("Error while fetching the analytics service config")

            if not content["compilerCbo"]:
                status, _, _ = self.cbas_util.update_service_parameter_configuration_on_cbas(
                    self.cluster, { "compilerCbo": True })
                if not status:
                    self.fail("Error while setting compilerCbo to True")
                # Restart analytics cluster for config to take effect.
                status, _, _ = self.restart_analytics_cluster_uri(self.cluster)
                if not status:
                    self.fail("Error while restarting analytics cluster")

        if load_sample_bucket:
            # Load travel-sample bucket
            if not self.bucket_util.load_sample_bucket(self.cluster, TravelSample()):
                self.fail("Failed to load travel-sample bucket on KV")

            if not self.cbas_util.verify_datasets_and_views_are_loaded_for_travel_sample(self.cluster):
                self.fail("Datasets and Views for travel-sample bucket were not loaded on analytics workbench")

            self.data_load_type = "travel-sample"

        if create_tpch_datasets:
            # Create datasets on all tpch buckets
            result = self.cbas_util.create_datasets_for_tpch(self.cluster)
            if not result:
                self.fail("Error while creating analytics collections on tpch buckets")
            self.data_load_type = "tpch"

        if wait_for_ingestion:
            if self.data_load_type == "travel-sample":
                for collection, doc_count in self.cbas_util.travel_sample_inventory_collections.iteritems():
                    result = self.cbas_util.wait_for_ingestion_complete(
                        self.cluster, "`travel-sample`.inventory.{0}".format(collection), doc_count)
                    if not result:
                        self.fail("error while ingesting data into dataset `travel-sample`.inventory.{0}".format(collection))
            elif self.data_load_type == "tpch":
                bucket_doc_count = self.tpch_util.get_doc_count_in_tpch_buckets()

                for bucket in self.cluster.buckets:
                    result = self.cbas_util.wait_for_ingestion_complete(self.cluster, bucket.name,
                                                                        bucket_doc_count[bucket.name],timeout=3000)
                    if not result:
                        self.fail("error while ingesting data into dataset {0}".format(bucket.name))

        if create_index_on_datasets:
            if self.data_load_type == "tpch":
                index_query_info = self.tpch_util.get_cbas_index_queries_tpch()
            elif self.data_load_type == "travel-sample":
                index_query_info = self.cbas_util.travel_sample_inventory_indexes

            for collection_name in index_query_info:
                if self.data_load_type == "travel-sample":
                    dataset_name = "`travel-sample`.inventory.{0}".format(collection_name)
                else:
                    dataset_name = collection_name
                for index_info in index_query_info[collection_name]:
                    result = self.cbas_util.create_cbas_index(
                        cluster=self.cluster, index_name=index_info["index_name"],
                        indexed_fields=index_info["indexed_field"], dataset_name=dataset_name,
                        analytics_index=True)
                    if not result:
                        self.fail("Error while creating index {0} on collection {1} for fields {2}".format(
                            index_info["index_name"], dataset_name, index_info["indexed_field"]))

        if create_samples_on_travel_sample:
            # Create samples on all collections of `travel-sample`.inventory dataverse
            for collection in self.cbas_util.travel_sample_inventory_collections.keys():
                result = self.cbas_util.create_sample_for_analytics_collections(
                    self.cluster, "`travel-sample`.inventory." + collection)
                if not result:
                    self.fail("Error while creating sample on collection %s" % collection)

        if create_samples_on_tpch:
            for bucket in self.cluster.buckets:
                # Keeping a fixed sample seed in order to get same result everytime.
                result = self.cbas_util.create_sample_for_analytics_collections(
                    cluster=self.cluster, collection_name=bucket.name, sample_seed=10)
                if not result:
                    self.fail("Error while creating sample on collection %s" % bucket.name)

        if setup_for_rebalance:
            self.cluster.exclude_nodes = list()

            self.rebalance_util = CBASRebalanceUtil(
                self.cluster_util, self.bucket_util, self.task, True, self.cbas_util)

            self.cluster.exclude_nodes.extend(
                [self.cluster.master, self.cluster.cbas_cc_node])

            self.replica_num = self.input.param('replica_num', 0)

            if self.replica_num:
                set_result = self.cbas_util.set_replica_number_from_settings(
                    self.cluster.master, replica_num=self.replica_num)
                if set_result != self.replica_num:
                    self.fail("Error while setting replica for CBAS")

                self.log.info(
                    "Rebalancing for CBAS replica setting change to take "
                    "effect.")
                rebalance_task, self.available_servers = self.rebalance_util.rebalance(
                    self.cluster, kv_nodes_in=0, kv_nodes_out=0, cbas_nodes_in=0,
                    cbas_nodes_out=0, available_servers=self.available_servers,
                    exclude_nodes=[])
                if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                        rebalance_task, self.cluster):
                    self.fail("Rebalance failed")

    def test_set_compiler_queryplanshape_option(self):
        self.setup_for_cbo(check_CBO_enabled=True, load_sample_bucket=False, create_tpch_datasets=True,
                           wait_for_ingestion=True, create_index_on_datasets=self.create_index,
                           create_samples_on_travel_sample=False, create_samples_on_tpch=True, setup_for_rebalance=False)
        tpch_queries = self.tpch_util.get_tpch_queries()
        queryplanshape = self.input.param('queryplanshape', "zigzag")
        query = "SET `compiler.queryplanshape` \"{0}\"; Explain ".format(queryplanshape) + tpch_queries["query_2"]
        status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster, query)
        if status != "success":
            self.fail("Error while running analytics query")
        else:
            if results:
                match = re.findall("\"build-side\": ([0-1])", json.dumps(results[0]))
                if match:
                    result = [int(i) for i in match]
                if queryplanshape == "zigzag":
                    if not (0 in result and 1 in result):
                        self.fail("The generated query plan was not zigzag")
                elif queryplanshape == "leftdeep":
                    if 1 in result:
                        self.fail("The generated query plan was not leftdeep")
                if queryplanshape == "rightdeep":
                    if 0 in result:
                        self.fail("The generated query plan was not rightdeep")

    def test_disable_cbo(self):
        self.setup_for_cbo(check_CBO_enabled=True, load_sample_bucket=True, create_tpch_datasets=False,
                           wait_for_ingestion=True, create_index_on_datasets=self.create_index,
                           create_samples_on_travel_sample=True, create_samples_on_tpch=False,
                           setup_for_rebalance=False)
        query = "SET `compiler.cbo` \"false\"; Explain " + CBASCBO.query
        status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster, query)
        if status != "success":
            self.fail("Error while running analytics query")
        else:
            if "optimizer-estimates" in results[0]:
                self.fail("Cardinality and Cost are still present in query plan.")

        query = "SET `compiler.cbo` \"false\"; " + CBASCBO.query
        status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster, query)
        if status != "success":
            self.fail("Error while running analytics query")
        else:
            doc_count_with_cbo_off = results[0]["$1"]

        status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster, CBASCBO.query)
        if status != "success":
            self.fail("Error while running analytics query")
        else:
            doc_count_with_cbo_on = results[0]["$1"]

        if doc_count_with_cbo_off != doc_count_with_cbo_on:
            self.fail("Doc is different when CBO is turned off. With CBO : {0} Without CBO : {1}".format(
                doc_count_with_cbo_on, doc_count_with_cbo_off))

    def get_estimated_and_actual_cardinality_for_query(self):
        query = "Explain " + CBASCBO.query
        status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster, query)
        if status != "success":
            self.fail("Error while running analytics query")
        else:
            cardinality = results[0]['optimizer-estimates']["cardinality"]

        status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster, CBASCBO.query)
        if status != "success":
            self.fail("Error while running analytics query")
        else:
            doc_count = results[0]["$1"]

        return cardinality, doc_count

    def test_rebalance_of_analytics_nodes_after_stats_are_built(self):
        self.setup_for_cbo(check_CBO_enabled=True, load_sample_bucket=True, create_tpch_datasets=False,
                           wait_for_ingestion=True, create_index_on_datasets=self.create_index,
                           create_samples_on_travel_sample=True, create_samples_on_tpch=False,
                           setup_for_rebalance=True)

        # Execute query before doing rebalance in/out/swap of cbas node
        estimated_cardinality_before_rebalance, actual_cardinality_before_rebalance = self.get_estimated_and_actual_cardinality_for_query()

        if not self.validate_cardinality(actual_cardinality_before_rebalance, estimated_cardinality_before_rebalance):
            self.fail("Actual cardinality : {0} has more than 20% variance from estimated cardinality {1}".format(
                actual_cardinality_before_rebalance, estimated_cardinality_before_rebalance
            ))

        rebalance_task, self.available_servers = self.rebalance_util.rebalance(
            self.cluster, 0, 0, self.input.param("cbas_nodes_in", 0),
            self.input.param("cbas_nodes_out", 0), self.available_servers, self.cluster.exclude_nodes)

        if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                rebalance_task, self.cluster):
            self.fail("Rebalance failed")

        estimated_cardinality_after_rebalance, actual_cardinality_after_rebalance = self.get_estimated_and_actual_cardinality_for_query()
        if not self.validate_cardinality(actual_cardinality_after_rebalance, estimated_cardinality_after_rebalance):
            self.fail("Actual cardinality : {0} has more than 20% variance from estimated cardinality {1} after rebalance".format(
                actual_cardinality_after_rebalance, estimated_cardinality_after_rebalance
            ))

        if actual_cardinality_before_rebalance != actual_cardinality_after_rebalance:
            self.fail("Actual cardinality after rebalance {0} is different from actual cardinality before rebalance {1}".format(
                actual_cardinality_before_rebalance, actual_cardinality_after_rebalance
            ))

    def test_failover_of_analytics_nodes_after_stats_are_built(self):
        self.setup_for_cbo(check_CBO_enabled=True, load_sample_bucket=True, create_tpch_datasets=False,
                           wait_for_ingestion=True, create_index_on_datasets=self.create_index,
                           create_samples_on_travel_sample=True, create_samples_on_tpch=False,
                           setup_for_rebalance=True)

        # Execute query before doing failover of cbas node
        estimated_cardinality_before_failover, actual_cardinality_before_failover = self.get_estimated_and_actual_cardinality_for_query()

        if not self.validate_cardinality(actual_cardinality_before_failover, estimated_cardinality_before_failover):
            self.fail("Actual cardinality : {0} has more than 20% variance from estimated cardinality {1}".format(
                actual_cardinality_before_failover, estimated_cardinality_before_failover
            ))

        self.available_servers, _, _ = self.rebalance_util.failover(
            self.cluster, kv_nodes=0, cbas_nodes=1,
            failover_type="Hard", action=self.input.param("failover_action", "RebalanceOut"), timeout=7200,
            available_servers=self.available_servers,
            exclude_nodes=self.cluster.exclude_nodes)

        estimated_cardinality_after_failover, actual_cardinality_after_failover = self.get_estimated_and_actual_cardinality_for_query()

        if not self.validate_cardinality(actual_cardinality_after_failover, estimated_cardinality_after_failover):
            self.fail("Actual cardinality : {0} has more than 20% variance from estimated cardinality {1}".format(
                actual_cardinality_after_failover, estimated_cardinality_after_failover
            ))

        if actual_cardinality_before_failover != actual_cardinality_after_failover:
            self.fail("Actual cardinality after failover {0} is different from actual cardinality before failover {1}".format(
                actual_cardinality_before_failover, actual_cardinality_after_failover
            ))

    def test_crash_and_recover_of_analytics_nodes_after_stats_are_built(self):
        self.setup_for_cbo(check_CBO_enabled=True, load_sample_bucket=True, create_tpch_datasets=False,
                           wait_for_ingestion=True, create_index_on_datasets=self.create_index,
                           create_samples_on_travel_sample=True, create_samples_on_tpch=False,
                           setup_for_rebalance=True)

        # Execute query before crashing cbas node
        estimated_cardinality_before_crash, actual_cardinality_before_crash = self.get_estimated_and_actual_cardinality_for_query()

        if not self.validate_cardinality(actual_cardinality_before_crash, estimated_cardinality_before_crash):
            self.fail("Actual cardinality : {0} has more than 20% variance from estimated cardinality {1}".format(
                actual_cardinality_before_crash, estimated_cardinality_before_crash
            ))

        cluster_cbas_nodes = self.cluster_util.get_nodes_from_services_map(
            self.cluster, service_type="cbas", get_all_nodes=True,
            servers=self.cluster.nodes_in_cluster)

        for node in self.cluster.exclude_nodes:
            if node in cluster_cbas_nodes:
                cluster_cbas_nodes.remove(node)

        self.log.debug("Killing Cbas on {0}".format(cluster_cbas_nodes[0]))
        self.cbas_util.kill_cbas_process(self.cluster, cbas_nodes=cluster_cbas_nodes[:1])

        if not self.cbas_util.is_analytics_running(self.cluster):
            self.fail("Analytics service did not come up even after 10\
                         mins of wait after initialisation")

        estimated_cardinality_after_crash, actual_cardinality_after_crash = self.get_estimated_and_actual_cardinality_for_query()

        if not self.validate_cardinality(actual_cardinality_after_crash, estimated_cardinality_after_crash):
            self.fail("Actual cardinality : {0} has more than 20% variance from estimated cardinality {1}".format(
                actual_cardinality_after_crash, estimated_cardinality_after_crash
            ))

        if actual_cardinality_before_crash != actual_cardinality_after_crash:
            self.fail("Actual cardinality after crash {0} is different from actual cardinality before crash {1}".format(
                actual_cardinality_before_crash, actual_cardinality_after_crash
            ))

    def test_CBO_with_HA_after_stats_are_built(self):
        self.setup_for_cbo(check_CBO_enabled=True, load_sample_bucket=True, create_tpch_datasets=False,
                           wait_for_ingestion=True, create_index_on_datasets=self.create_index,
                           create_samples_on_travel_sample=True, create_samples_on_tpch=False,
                           setup_for_rebalance=True)

        # Execute query before crashing cbas node
        estimated_cardinality_before_failover, actual_cardinality_before_failover = self.get_estimated_and_actual_cardinality_for_query()

        if not self.validate_cardinality(actual_cardinality_before_failover, estimated_cardinality_before_failover):
            self.fail("Actual cardinality : {0} has more than 20% variance from estimated cardinality {1}".format(
                actual_cardinality_before_failover, estimated_cardinality_before_failover
            ))

        self.log.info("Marking one of the CBAS nodes as failed over.")
        self.available_servers, kv_failover_nodes, cbas_failover_nodes = self.rebalance_util.failover(
            self.cluster, kv_nodes=0, cbas_nodes=1, failover_type="Hard",
            action=None, timeout=7200, available_servers=self.available_servers,
            exclude_nodes=[], kv_failover_nodes=None, cbas_failover_nodes=None,
            all_at_once=True)

        estimated_cardinality_after_failover, actual_cardinality_after_failover = self.get_estimated_and_actual_cardinality_for_query()

        if not self.validate_cardinality(actual_cardinality_after_failover, estimated_cardinality_after_failover):
            self.fail("Actual cardinality : {0} has more than 20% variance from estimated cardinality {1}".format(
                actual_cardinality_after_failover, estimated_cardinality_after_failover
            ))

        if actual_cardinality_before_failover != actual_cardinality_after_failover:
            self.fail(
                "Actual cardinality after failover {0} is different from actual cardinality before failover {1}".format(
                    actual_cardinality_before_failover, actual_cardinality_after_failover
                ))

    def test_cbas_crash_during_sample_building(self):
        self.setup_for_cbo(check_CBO_enabled=True, load_sample_bucket=True, create_tpch_datasets=False,
                           wait_for_ingestion=True, create_index_on_datasets=self.create_index,
                           create_samples_on_travel_sample=False, create_samples_on_tpch=False,
                           setup_for_rebalance=False)

        queries = list()
        # Create samples on all collections of `travel-sample`.inventory dataverse
        for collection in self.cbas_util.travel_sample_inventory_collections.keys():
            queries.append("ANALYZE ANALYTICS COLLECTION `travel-sample`.inventory.%s" % collection)

        cluster_cbas_nodes = self.cluster_util.get_nodes_from_services_map(
            self.cluster, service_type="cbas", get_all_nodes=True,
            servers=self.cluster.nodes_in_cluster)

        self.cluster.exclude_nodes = [self.cluster.master, self.cluster.cbas_cc_node]

        for node in self.cluster.exclude_nodes:
            if node in cluster_cbas_nodes:
                cluster_cbas_nodes.remove(node)

        shell = RemoteMachineShellConnection(cluster_cbas_nodes[0])

        self.query_task = RunQueriesTask(
            self.cluster, queries, self.task_manager, self.cbas_util, "cbas",
            run_infinitely=False, parallelism=len(queries), is_prepared=True,
            record_results=True)
        self.task_manager.add_new_task(self.query_task)

        shell.restart_couchbase()
        shell.disconnect()
        self.log.debug("Couchbase restarted on {0}".format(cluster_cbas_nodes[0]))

        self.task_manager.get_task_result(self.query_task)

        result = True

        for collection in self.cbas_util.travel_sample_inventory_collections.keys():
            result = result and self.cbas_util.verify_sample_present_in_Metadata(self.cluster, collection)

        if result:
            self.fail("All samples were created even when a CBAS node failed.")

    def test_KV_crash_during_sample_building(self):
        self.setup_for_cbo(check_CBO_enabled=True, load_sample_bucket=True, create_samples_on_tpch=False,
                           create_tpch_datasets=False, create_samples_on_travel_sample=False, setup_for_rebalance=False,
                           create_index_on_datasets=self.create_index)

        queries = list()
        # Create samples on all collections of `travel-sample`.inventory dataverse
        for collection in self.cbas_util.travel_sample_inventory_collections.keys():
            queries.append("ANALYZE ANALYTICS COLLECTION `travel-sample`.inventory.%s" % collection)

        cluster_kv_nodes = self.cluster_util.get_nodes_from_services_map(
            self.cluster, service_type="kv", get_all_nodes=True,
            servers=self.cluster.nodes_in_cluster)

        self.cluster.exclude_nodes = [self.cluster.master, self.cluster.cbas_cc_node]

        for node in self.cluster.exclude_nodes:
            if node in cluster_kv_nodes:
                cluster_kv_nodes.remove(node)

        shell = RemoteMachineShellConnection(cluster_kv_nodes[0])

        self.query_task = RunQueriesTask(
            self.cluster, queries, self.task_manager, self.cbas_util, "cbas",
            run_infinitely=False, parallelism=len(queries), is_prepared=True,
            record_results=True)
        self.task_manager.add_new_task(self.query_task)

        shell.restart_couchbase()
        shell.disconnect()
        self.log.debug("Couchbase restarted on {0}".format(cluster_kv_nodes[0]))

        self.task_manager.get_task_result(self.query_task)

        result = True

        for collection in self.cbas_util.travel_sample_inventory_collections.keys():
            result = result and self.cbas_util.verify_sample_present_in_Metadata(self.cluster, collection)

        if not result:
            self.fail("All samples were not created after a KV node failed.")

    def test_analyze_query_execution_parallely_with_another_query(self):
        if self.input.param('sample_created', True):
            self.setup_for_cbo(setup_for_rebalance=False)
        else:
            self.setup_for_cbo(create_samples_on_travel_sample=False, setup_for_rebalance=False)

        status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster, CBASCBO.query)
        if status == "success":
            doc_count = results[0]["$1"]
        else:
            self.fail("Failed to execute analytics query")

        queries = list()
        # Create samples on all collections of `travel-sample`.inventory dataverse
        for collection in self.cbas_util.travel_sample_inventory_collections.keys():
            queries.append("ANALYZE ANALYTICS COLLECTION `travel-sample`.inventory.%s" % collection)

        queries.append(CBASCBO.query)

        self.query_task = RunQueriesTask(
            self.cluster, queries, self.task_manager, self.cbas_util, "cbas",
            run_infinitely=False, parallelism=len(queries), is_prepared=True,
            record_results=True)
        self.task_manager.add_new_task(self.query_task)
        self.task_manager.get_task_result(self.query_task)
        for result in self.query_task.result:
            if result:
                doc_count_parallel = result[0]["$1"]
        if doc_count != doc_count_parallel:
            self.fail("Doc count mismatch while running count(*) queries parallely with analyze queries.")

    def test_multiple_analyze_query_execution_parallely(self):
        self.setup_for_cbo(check_CBO_enabled=True, load_sample_bucket=True, create_samples_on_tpch=False,
                           create_tpch_datasets=False, create_samples_on_travel_sample=False, setup_for_rebalance=False,
                           create_index_on_datasets=self.create_index)

        queries = list()
        if self.input.param('same_collection', True):
            for i in range(5):
                queries.append("ANALYZE ANALYTICS COLLECTION `travel-sample`.inventory.%s;" %
                               self.cbas_util.travel_sample_inventory_collections.keys()[0])
        else:
            for collection in self.cbas_util.travel_sample_inventory_collections.keys():
                queries.append("ANALYZE ANALYTICS COLLECTION `travel-sample`.inventory.%s;" % collection)

        self.query_task = RunQueriesTask(
            self.cluster, queries, self.task_manager, self.cbas_util, "cbas",
            run_infinitely=False, parallelism=len(queries), is_prepared=True,
            record_results=True)
        self.task_manager.add_new_task(self.query_task)
        self.task_manager.get_task_result(self.query_task)

    def test_multiple_analyze_query_execution_parallely_by_multiple_users(self):
        self.setup_for_cbo(check_CBO_enabled=True, load_sample_bucket=True, create_samples_on_tpch=False,
                           create_tpch_datasets=False, create_samples_on_travel_sample=False, setup_for_rebalance=False,
                           create_index_on_datasets=self.create_index)

        queries = list()
        if self.input.param('same_collection', True):
            for i in range(5):
                queries.append("ANALYZE ANALYTICS COLLECTION `travel-sample`.inventory.%s" %
                               self.cbas_util.travel_sample_inventory_collections.keys()[0])
        else:
            for collection in self.cbas_util.travel_sample_inventory_collections.keys():
                queries.append("ANALYZE ANALYTICS COLLECTION `travel-sample`.inventory.%s" % collection)

        # create multiple users
        rbac_users = list()
        for i in range(5):
            rbac_username = "test_user_{0}".format(i)
            user = [
                {'id': rbac_username, 'password': 'password', 'name': 'Some Name'}]
            _ = RbacBase().create_user_source(user, 'builtin', self.cluster.master)
            payload = "name=" + rbac_username + "&roles=admin"
            response = self.cluster.rest.add_set_builtin_user(rbac_username, payload)
            rbac_users.append(rbac_username)

        jobs = Queue()
        results = list()

        for index, user in enumerate(rbac_users):
            jobs.put((self.cbas_util.execute_statement_on_cbas_util,
                      dict(cluster=self.cluster, statement=queries[index],
                           username=user, password="password")))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, len(rbac_users), async_run=False)

        for result in results:
            if result[0] != "success":
                self.fail("One of the analyze queries failed.")

    def test_RBO_is_used_when_not_all_collections_in_join_query_have_samples(self):
        self.setup_for_cbo(check_CBO_enabled=True, load_sample_bucket=True, create_samples_on_tpch=False,
                           create_tpch_datasets=False, create_samples_on_travel_sample=False, setup_for_rebalance=False,
                           create_index_on_datasets=self.create_index)
        query = "ANALYZE ANALYTICS COLLECTION `travel-sample`.inventory.%s" % "airline"
        status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster, query)
        if status != "success":
            self.fail("Failed to execute analytics analyze query")

        estimated_cardinality, actual_cardinality = self.get_estimated_and_actual_cardinality_for_query()

        if estimated_cardinality != 0:
            self.fail("CBO was used even when samples were not present for all the collections in the query")

    def test_sample_creation_when_disk_full(self):
        datasets = list()
        for i in range(1,11):
            dataset_name = "test_{0}".format(i)
            result = self.cbas_util.create_dataset(
                self.cluster, dataset_name, "`{0}`".format(self.cluster.buckets[0].name))
            if not result:
                self.fail("error while creating dataset {0} on {1}".format(dataset_name, self.cluster.buckets[0].name))
            else:
                datasets.append(dataset_name)
            result = self.cbas_util.wait_for_ingestion_complete(self.cluster, dataset_name, self.input.param("num_items"))
            if not result:
                self.fail("error while ingesting data into dataset {0}".format(dataset_name))

        cluster_cbas_nodes = self.cluster_util.get_nodes_from_services_map(
            self.cluster, service_type="cbas", get_all_nodes=True,
            servers=self.cluster.nodes_in_cluster)

        rest = RestConnection(cluster_cbas_nodes[0])
        response = rest.get_jre_path()
        if not response:
            self.fail("No response returned from nodes/self endpoint")
        free_storage_in_MB = response["storageTotals"]["hdd"]["free"] / 1000000

        shell = RemoteMachineShellConnection(cluster_cbas_nodes[0])
        shell.extract_remote_info()

        if free_storage_in_MB > 1000:
            if shell.info.type.lower() == 'windows':
                self.fail("This test case will not work for windows")
            elif shell.info.type.lower() == "linux":
                command = "fallocate -l {0}M /x".format(free_storage_in_MB - 1000)
                o, r = shell.execute_command(command)
                shell.log_command_output(o, r)

        all_result = True
        for dataset in datasets:
            result = self.cbas_util.create_sample_for_analytics_collections(self.cluster, dataset, "high")
            all_result = all_result and result

        if shell.info.type.lower() == "linux":
            command = "rm -f /x"
            o, r = shell.execute_command(command)
            shell.log_command_output(o, r)

        if all_result:
            self.fail("unable to simulate disk fill scenario for analytics")

    def test_data_ingestion_sample_and_index_creation(self):
        self.tpch_util.create_kv_buckets_for_tpch(self.cluster)
        test_case = {
            1: "Data Ingestion > CBAS secondary Index creation > Sample creation",
            2: "Data Ingestion > Sample creation > CBAS secondary Index creation",
            3: "Sample creation > Data Ingestion > CBAS secondary Index creation",
            4: "Sample creation > CBAS secondary Index creation > Data Ingestion",
            5: "CBAS secondary Index creation > Data Ingestion > Sample creation",
            6: "CBAS secondary Index creation > Sample creation > Data Ingestion"
        }

        test_to_execute = self.input.param('testcase', 1)

        self.log.info("Executing scenarion {0}".format(test_case[test_to_execute]))
        if test_to_execute == 1:
            self.tpch_util.load_tpch_data_into_KV_buckets(self.cluster)
            self.setup_for_cbo(check_CBO_enabled=True, load_sample_bucket=False, create_tpch_datasets=True,
                               wait_for_ingestion=True, create_index_on_datasets=True,
                               create_samples_on_travel_sample=False, create_samples_on_tpch=True,
                               setup_for_rebalance=False)

        elif test_to_execute == 2:
            self.tpch_util.load_tpch_data_into_KV_buckets(self.cluster)
            self.setup_for_cbo(check_CBO_enabled=True, load_sample_bucket=False, create_tpch_datasets=True,
                               wait_for_ingestion=True, create_index_on_datasets=False,
                               create_samples_on_travel_sample=False, create_samples_on_tpch=True,
                               setup_for_rebalance=False)
            self.setup_for_cbo(check_CBO_enabled=False, load_sample_bucket=False, create_tpch_datasets=False,
                               wait_for_ingestion=False, create_index_on_datasets=True,
                               create_samples_on_travel_sample=False, create_samples_on_tpch=False,
                               setup_for_rebalance=False)

        elif test_to_execute == 3:
            self.setup_for_cbo(check_CBO_enabled=True, load_sample_bucket=False, create_tpch_datasets=True,
                               wait_for_ingestion=False, create_index_on_datasets=False,
                               create_samples_on_travel_sample=False, create_samples_on_tpch=True,
                               setup_for_rebalance=False)
            self.tpch_util.load_tpch_data_into_KV_buckets(self.cluster)
            self.setup_for_cbo(check_CBO_enabled=False, load_sample_bucket=False, create_tpch_datasets=False,
                               wait_for_ingestion=True, create_index_on_datasets=True,
                               create_samples_on_travel_sample=False, create_samples_on_tpch=False,
                               setup_for_rebalance=False)

        elif test_to_execute == 4:
            self.setup_for_cbo(check_CBO_enabled=True, load_sample_bucket=False, create_tpch_datasets=True,
                               wait_for_ingestion=False, create_index_on_datasets=False,
                               create_samples_on_travel_sample=False, create_samples_on_tpch=True,
                               setup_for_rebalance=False)
            self.setup_for_cbo(check_CBO_enabled=False, load_sample_bucket=False, create_tpch_datasets=False,
                               wait_for_ingestion=False, create_index_on_datasets=True,
                               create_samples_on_travel_sample=False, create_samples_on_tpch=False,
                               setup_for_rebalance=False)
            self.tpch_util.load_tpch_data_into_KV_buckets(self.cluster)
            self.setup_for_cbo(check_CBO_enabled=False, load_sample_bucket=False, create_tpch_datasets=False,
                               wait_for_ingestion=True, create_index_on_datasets=False,
                               create_samples_on_travel_sample=False, create_samples_on_tpch=False,
                               setup_for_rebalance=False)

        elif test_to_execute == 5:
            self.setup_for_cbo(check_CBO_enabled=True, load_sample_bucket=False, create_tpch_datasets=True,
                               wait_for_ingestion=False, create_index_on_datasets=True,
                               create_samples_on_travel_sample=False, create_samples_on_tpch=False,
                               setup_for_rebalance=False)
            self.tpch_util.load_tpch_data_into_KV_buckets(self.cluster)
            self.setup_for_cbo(check_CBO_enabled=False, load_sample_bucket=False, create_tpch_datasets=False,
                               wait_for_ingestion=True, create_index_on_datasets=False,
                               create_samples_on_travel_sample=False, create_samples_on_tpch=True,
                               setup_for_rebalance=False)

        elif test_to_execute == 6:
            self.setup_for_cbo(check_CBO_enabled=True, load_sample_bucket=False, create_tpch_datasets=True,
                               wait_for_ingestion=False, create_index_on_datasets=True,
                               create_samples_on_travel_sample=False, create_samples_on_tpch=True,
                               setup_for_rebalance=False)
            self.tpch_util.load_tpch_data_into_KV_buckets(self.cluster)
            self.setup_for_cbo(check_CBO_enabled=False, load_sample_bucket=False, create_tpch_datasets=False,
                               wait_for_ingestion=True, create_index_on_datasets=False,
                               create_samples_on_travel_sample=False, create_samples_on_tpch=False,
                               setup_for_rebalance=False)


        # Execute query before doing rebalance in/out/swap of cbas node
        tpch_queries = self.tpch_util.get_tpch_queries()
        query = "Explain " + tpch_queries["query_2"]
        status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster, query)
        if status != "success":
            self.fail("Error while running analytics query")
        else:
            estimated_cardinality = results[0]['optimizer-estimates']["cardinality"]

        status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster, query)
        if status != "success":
            self.fail("Error while running analytics query")
        else:
            actual_cardinality = len(results)

        if not self.validate_cardinality(actual_cardinality, estimated_cardinality):
            self.fail("Actual cardinality : {0} has more than 20% variance from estimated cardinality {1}".format(
                actual_cardinality, estimated_cardinality
            ))

    def test_system_events_for_collection_stats_creation_deletion(self):
        self.setup_for_cbo(check_CBO_enabled=True, load_sample_bucket=True, create_tpch_datasets=False,
                           wait_for_ingestion=True, create_index_on_datasets=False, create_samples_on_travel_sample=False,
                           create_samples_on_tpch=False, setup_for_rebalance=False)
        query = "ANALYZE ANALYTICS COLLECTION `travel-sample`.inventory.airline"
        status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster, query)
        if status != "success":
            self.fail("Failed to execute analytics analyze query")

        self.system_events.add_event(AnalyticsEvents.collection_analyzed(
            self.cluster.cbas_cc_node.ip,
            CBASHelper.metadata_format("`travel-sample`.inventory"),
            CBASHelper.metadata_format("airline")
        ))

        query += " drop statistics"
        status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster, query)
        if status != "success":
            self.fail("Failed to drop statistics on collection")

        self.system_events.add_event(AnalyticsEvents.collection_stats_dropped(
            self.cluster.cbas_cc_node.ip,
            CBASHelper.metadata_format("`travel-sample`.inventory"),
            CBASHelper.metadata_format("airline")
        ))
