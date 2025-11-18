import copy
import random
from random import choice
from time import time

from BucketLib.bucket import Bucket
from cb_server_rest_util.buckets.buckets_api import BucketRestApi
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_server_rest_util.server_groups.server_groups_api import ServerGroupsAPI
from rebalance_utils.rebalance_util import RebalanceUtil
from sdk_client3 import SDKClient, SDKClientPool
from cb_constants import CbServer, DocLoading
from Jython_tasks.task import ConcurrentFailoverTask
from bucket_collections.collections_base import CollectionBase
from collections_helper.collections_spec_constants import MetaCrudParams
from couchbase_helper.documentgenerator import doc_generator
from error_simulation.cb_error import CouchbaseError
from constants.sdk_constants.java_client import SDKConstants
from failover.AutoFailoverBaseTest import AutoFailoverBaseTest
from membase.api.rest_client import RestConnection
from shell_util.remote_connection import RemoteMachineShellConnection
from table_view import TableView
from scenario_plugins.ns_server_scenarios import NsServerFeaturePlugins


class ConcurrentFailoverTests(AutoFailoverBaseTest):
    def setUp(self):
        super(ConcurrentFailoverTests, self).setUp()

        self.log_setup_status(self.__class__.__name__, "started",
                              self.setUp.__name__)

        #######################################################################
        # List of params to be used for failover
        # self.timeout from AutoFailoverBaseTest
        # self.max_count from AutoFailoverBaseTest

        # To track the triggered failover events
        self.fo_events = 0

        # failover_order to be used for failover_order_tests
        # Format:
        #   kv:kv-kv:index_query
        #   * Iteration marked by '-'
        #   * Nodes marked by ':'
        #   * Service within a node denoted by '_' (underscore)
        # In the above case,
        # - Loop #0 :: 2 KV nodes will be failed
        # - Loop #1 :: 1 KV + 1 node running n1ql+index will be failed
        self.failover_order = \
            self.input.param("failover_order", "kv").split("-")
        self.failover_method = \
            self.input.param("failover_method", CouchbaseError.STOP_MEMCACHED)
        # Failover type determines the order of FO (Auto/Graceful/Hard).
        # Length of this should match the len(self.failover_order)
        # Example -> auto-graceful-auto
        # This expects first set of nodes from failover_order to undergo
        # AUTO FO followed by GRACEFUL FO of nodes through API and then
        # followed by AUTO FO of 3rd set of nodes as defined by failover_order
        self.failover_type = \
            self.input.param("failover_type",
                             CbServer.Failover.Type.AUTO).split("-")
        # End of params to be used for failover
        #######################################################################

        self.load_during_fo = self.input.param("load_during_fo", False)
        self.preserve_durability_during_auto_fo = self.input.param(
            "preserve_durability_during_auto_fo", "false")
        self.log.info("Updating Auto-failover settings")
        self.rest.update_auto_failover_settings(
            enabled="true", timeout=self.timeout, max_count=self.max_count,
            failover_preserve_durability=
            self.preserve_durability_during_auto_fo)
        # Find the bucket with least-replica to check the Auto-FO possibility
        self.find_minimum_bucket_replica()
        # Hold the dict of {node_obj_to_fail: failover_type, ...}
        self.nodes_to_fail = None
        # To display test execution status
        self.test_status_tbl = TableView(self.log.critical)
        self.auto_fo_settings_tbl = TableView(self.log.critical)
        self.test_status_tbl.set_headers(
            ["Node", "Services", "Node status", "Failover type"])
        self.auto_fo_settings_tbl.set_headers(
            ["Enabled", "Auto FO count", "Max Events configured",
             "Auto FO timeout", "Disk Auto FO", "Disk Auto FO timeout"])

        self.validate_failover_settings(True, self.timeout, 0, self.max_count)

        # Init sdk_client_pool if not initialized before
        if self.cluster.sdk_client_pool is None:
            self.cluster.sdk_client_pool = SDKClientPool()
            CollectionBase.create_sdk_clients(
                self.cluster, self.task_manager.number_of_threads,
                self.cluster.master, self.cluster.buckets,
                self.sdk_compression, self.load_docs_using)

        # Perform initial collection load
        self.__load_initial_collection_data()
        self.log_setup_status(self.__class__.__name__, "complete",
                              self.setUp.__name__)
        self.delete_primary_indexes()

    def delete_primary_indexes(self):
        try:
            if self.cluster.index_nodes and self.cluster.query_nodes:
                for bucket in self.cluster.buckets:
                    client = SDKClient([self.cluster.master], bucket)
                    client.run_query(
                        "DROP INDEX "
                        "default:`%s`.`_system`.`_query`.`#primary` if exists;"
                        % (bucket.name), timeout=300)
        except Exception as e:
            self.log.info("Exception %s occurred wile deleting primary "
                          "indexes" % e)
        self.sleep(8, "small sleep post primary index deletion")

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "started",
                              self.tearDown.__name__)
        # Select KV node as a cluster master to perform tearDown rebalance out
        self.cluster_util.update_cluster_nodes_service_list(self.cluster)
        self.cluster.master = self.cluster.kv_nodes[0]

        self.log.info("Resetting auto-failover settings to default")
        self.rest.update_auto_failover_settings(
            enabled="true", timeout=120, max_count=1)
        self.log_setup_status(self.__class__.__name__, "complete",
                              self.tearDown.__name__)

        super(ConcurrentFailoverTests, self).tearDown()

    def find_minimum_bucket_replica(self):
        self.min_bucket_replica = Bucket.ReplicaNum.THREE
        for bucket in self.cluster.buckets:
            if bucket.replicaNumber < self.min_bucket_replica:
                self.min_bucket_replica = bucket.replicaNumber

    def __get_collection_load_spec(self, doc_ttl=0):
        """
        Set doc_ttl for loading doc during failover operations
        """
        doc_key_size = 8
        if self.key_size is not None:
            doc_key_size = self.key_size
        d_level = SDKConstants.DurabilityLevel.NONE
        if self.num_replicas != Bucket.ReplicaNum.THREE:
            random.seed(round(time()*1000))
            # Since durability is not supported with replicas=3
            d_level = choice([
                SDKConstants.DurabilityLevel.NONE,
                SDKConstants.DurabilityLevel.MAJORITY,
                SDKConstants.DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE,
                SDKConstants.DurabilityLevel.PERSIST_TO_MAJORITY])
        return {
            # Scope/Collection ops params
            MetaCrudParams.COLLECTIONS_TO_DROP: 3,

            MetaCrudParams.SCOPES_TO_DROP: 1,
            MetaCrudParams.SCOPES_TO_ADD_PER_BUCKET: 3,
            MetaCrudParams.COLLECTIONS_TO_ADD_FOR_NEW_SCOPES: 5,

            MetaCrudParams.COLLECTIONS_TO_ADD_PER_BUCKET: 10,

            MetaCrudParams.BUCKET_CONSIDERED_FOR_OPS: "all",
            MetaCrudParams.SCOPES_CONSIDERED_FOR_OPS: "all",
            MetaCrudParams.COLLECTIONS_CONSIDERED_FOR_OPS: "all",

            # Doc loading params
            "doc_crud": {
                MetaCrudParams.DocCrud.COMMON_DOC_KEY: "test_collections",

                MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS: 5000,
                MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION: 20,
                MetaCrudParams.DocCrud.READ_PERCENTAGE_PER_COLLECTION: 10,
                MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION: 10,
                MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION: 10,
                MetaCrudParams.DocCrud.DOC_KEY_SIZE: doc_key_size
            },

            # Doc_loading task options
            MetaCrudParams.DOC_TTL: doc_ttl,
            MetaCrudParams.DURABILITY_LEVEL: d_level,
            MetaCrudParams.SKIP_READ_ON_ERROR: True,
            MetaCrudParams.SUPPRESS_ERROR_TABLE: False,
            # The below is to skip populating success dictionary for reads
            MetaCrudParams.SKIP_READ_SUCCESS_RESULTS: True,

            MetaCrudParams.RETRY_EXCEPTIONS: [],
            MetaCrudParams.IGNORE_EXCEPTIONS: [],
            MetaCrudParams.COLLECTIONS_CONSIDERED_FOR_CRUD: "all",
            MetaCrudParams.SCOPES_CONSIDERED_FOR_CRUD: "all",
            MetaCrudParams.BUCKETS_CONSIDERED_FOR_CRUD: "all"
        }

    @property
    def num_nodes_to_be_failover(self):
        def is_safe_to_fo(service):
            is_safe = False
            # Reference ticket
            # MB-51791
            if self.preserve_durability_during_auto_fo == "true" and \
                    self.min_bucket_replica >= 2:
                self.min_bucket_replica -= 1

            # Reference doc:
            # https://docs.couchbase.com/server/7.0/learn/clusters-and-availability/automatic-failover.html#failover-policy
            # Service / Data loss check
            if service == CbServer.Services.KV:
                if self.min_bucket_replica > 0 \
                        and node_count[CbServer.Services.KV] > 1:
                    is_safe = True
            # elif service == CbServer.Services.INDEX:
            #     if node_count[CbServer.Services.INDEX] > 1:
            #         is_safe = True
            else:
                # All other services require at least 2 nodes to FO
                if node_count[service] > 1:
                    is_safe = True
            return is_safe

        def decr_node_count(service):
            node_count[service] -= 1
            if service == CbServer.Services.KV:
                self.min_bucket_replica -= 1

        fo_nodes = set()
        num_unreachable_nodes = 0
        active_cluster_nodes = len(self.cluster_util.get_nodes(
            self.cluster.master))
        total_nodes = active_cluster_nodes + self.fo_events + self.nodes_in
        min_nodes_for_quorum = int(total_nodes/2) + 1
        max_allowed_unreachable_nodes = total_nodes - min_nodes_for_quorum

        # Quorum check before checking individual services
        for _, failure_type in self.nodes_to_fail.items():
            if failure_type in ["stop_couchbase", "network_split"]:
                num_unreachable_nodes += 1
        if num_unreachable_nodes > max_allowed_unreachable_nodes:
            return 0
        # End of quorum check

        node_count = dict()
        node_count[CbServer.Services.KV] = len(self.cluster.kv_nodes)
        node_count[CbServer.Services.INDEX] = len(self.cluster.index_nodes)
        node_count[CbServer.Services.N1QL] = len(self.cluster.query_nodes)
        node_count[CbServer.Services.EVENTING] = len(
            self.cluster.eventing_nodes)
        node_count[CbServer.Services.BACKUP] = len(self.cluster.backup_nodes)

        kv_nodes = dict()
        non_kv_nodes = dict()
        for node, failure_type in self.nodes_to_fail.items():
            if CbServer.Services.KV in node.services:
                kv_nodes[node] = failure_type
            else:
                non_kv_nodes[node] = failure_type

        kv_service = CbServer.Services.KV
        for node, failure_type in kv_nodes.items():
            if kv_service in node.services:
                # KV takes priority over other nodes in deciding the Auto-FO
                if self.max_count > (len(fo_nodes) + self.fo_events) \
                        and is_safe_to_fo(kv_service):
                    fo_nodes.add(node)
                    for service_type in node.services:
                        # Decrement the node count for the service
                        decr_node_count(service_type)
                else:
                    self.log.info("KV failover not possible")
                    # No nodes should be FO'ed if KV FO is not possible
                    fo_nodes = set()
                    # Break to make sure no other service failover
                    # will be expected
                    break
        else:
            nodes_not_failed = set()
            for node, failure_type in non_kv_nodes.items():
                # For other nodes, we need to check if the node running
                # other services are also safe to failover
                for service_type in node.services:
                    if self.max_count == (len(fo_nodes) + self.fo_events):
                        # Check to see whether the max_fo count is reached
                        self.log.info("Max auto-fo count already reached")
                        break
                    if not is_safe_to_fo(service_type):
                        self.log.warning("Service '%s' not safe to failover"
                                         % service_type)
                        for t_node in fo_nodes:
                            if service_type in t_node.services \
                                    and kv_service not in t_node.services:
                                nodes_not_failed.add(t_node)
                        break
                else:
                    fo_nodes.add(node)
                    for service_type in node.services:
                        # Decrement the node count for the service
                        decr_node_count(service_type)
            fo_nodes = fo_nodes.difference(nodes_not_failed)
        expected_num_nodes = len(fo_nodes)
        self.log.info("Expected nodes to be failed over: %d"
                      % expected_num_nodes)
        return expected_num_nodes

    def __get_server_obj(self, node):
        for server in self.cluster.servers:
            if server.ip == node.ip:
                return server

    def __update_server_obj(self):
        temp_data = self.nodes_to_fail
        self.nodes_to_fail = dict()
        for node_obj, fo_type in temp_data.items():
            self.nodes_to_fail[self.__get_server_obj(node_obj)] = fo_type

    def __load_initial_collection_data(self):
        load_spec = self.__get_collection_load_spec()
        load_spec[MetaCrudParams.SCOPES_TO_DROP] = 0
        load_spec[MetaCrudParams.COLLECTIONS_TO_DROP] = 0
        load_spec[MetaCrudParams.SCOPES_TO_ADD_PER_BUCKET] = 2
        load_spec[MetaCrudParams.COLLECTIONS_TO_ADD_FOR_NEW_SCOPES] = 5
        load_spec["doc_crud"][
            MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS] = 10000

    def __perform_doc_ops(self, durability=None, validate_num_items=True):
        load_spec = self.__get_collection_load_spec()
        if self.skip_collections_during_data_load is not None:
            load_spec['skip_dict'] = self.skip_collections_during_data_load
        if durability and self.num_replicas != Bucket.ReplicaNum.THREE:
            load_spec[MetaCrudParams.DURABILITY_LEVEL] = durability

        self.log.info("Performing doc_ops with durability level=%s"
                      % load_spec[MetaCrudParams.DURABILITY_LEVEL])
        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.cluster.buckets,
                load_spec,
                mutation_num=0,
                async_load=(not validate_num_items),
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency)

        if doc_loading_task.result is False:
            self.fail("Collection CRUDs failure")

        if validate_num_items:
            # Verify initial doc load count
            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets,
                                                         timeout=1200)
            self.bucket_util.validate_docs_per_collections_all_buckets(
                self.cluster)

    def get_nodes_to_fail(self, services_to_fail, dynamic_fo_method=False):
        nodes = dict()
        master_fo = False
        # Update the list of service-nodes mapping in the cluster object
        self.cluster_util.update_cluster_nodes_service_list(self.cluster)
        nodes_in_cluster = self.cluster_util.get_nodes(self.cluster.master)
        for services in services_to_fail:
            node_services = set(services.split("_"))
            # To handle serviceless node failover, force creating a set
            if node_services == {"None"}:
                node_services = set()
            for index, node in enumerate(nodes_in_cluster):
                if node_services == set(node.services):
                    fo_type = self.failover_method
                    if dynamic_fo_method:
                        fo_type = "stop_couchbase"
                        if CbServer.Services.KV in node_services:
                            fo_type = CouchbaseError.STOP_MEMCACHED
                    nodes[node] = fo_type
                    # Remove the node to be failed to avoid double insertion
                    nodes_in_cluster.pop(index)
                    if node.ip == self.cluster.master.ip:
                        master_fo = True
                    break
        if master_fo and len(nodes_in_cluster) > 0:
            # Assign new cluster.master to handle rest connections
            self.cluster.master = [
                server for server in self.cluster.nodes_in_cluster
                if nodes_in_cluster[0].ip == server.ip][0]
        return nodes

    def validate_failover_settings(self, enabled, timeout, count, max_count):
        _, settings = self.rest.get_auto_failover_settings()
        self.auto_fo_settings_tbl.rows = list()
        self.auto_fo_settings_tbl.rows.append([
            str(settings['enabled']), str(settings['count']),
            str(settings['maxCount']), str(settings['timeout']),
            str(settings['failoverOnDataDiskIssues']['enabled']),
            str(settings['failoverOnDataDiskIssues']['timePeriod'])])
        self.auto_fo_settings_tbl.display("Auto failover status:")

        err_msg = "Mismatch in '%s' field. " \
                  "Cluster FO data: " + str(settings)
        self.assertEqual(settings['enabled'], enabled, err_msg % "enabled")
        self.assertEqual(settings['timeout'], timeout, err_msg % "timeout")
        self.assertEqual(settings['count'], count, err_msg % "count")
        self.assertEqual(settings['maxCount'], max_count,
                         err_msg % "maxCount")

    def __display_failure_node_status(self, message):
        self.test_status_tbl.rows = list()
        cluster_nodes = self.cluster_util.get_nodes(self.cluster.master,
                                                    inactive_added=True,
                                                    inactive_failed=True)
        for node, fo_type in self.nodes_to_fail.items():
            node = [t_node for t_node in cluster_nodes
                    if t_node.ip == node.ip][0]
            self.test_status_tbl.add_row([node.ip, ",".join(node.services),
                                          node.clusterMembership, fo_type])
        self.test_status_tbl.display(message)

    def __update_unaffected_node(self):
        cluster_nodes = self.cluster_util.get_nodes(self.cluster.master)
        for cluster_node in cluster_nodes:
            for failure_node in self.nodes_to_fail:
                if cluster_node.ip == failure_node.ip:
                    break
            else:
                self.orchestrator = cluster_node
                self.rest = ClusterRestAPI(self.orchestrator)
                self.cluster.master = cluster_node
                self.log.info("Node for REST APIs: %s" % cluster_node.ip)
                break

    def test_max_events_range(self):
        """
        - Try setting max_events 1 to 100 (Valid)
        - Try setting 0 > max_events > 100 (Invalid - negative)
        - Current timeout_range (5-120seconds) should work"
        """

        self.log.info("Testing max_event counts")
        enable_failover = "true"
        timeout_val = 10
        max_plus_1 = CbServer.Failover.MAX_EVENTS + 1

        # Set max_events between (min, max)
        for num_events in range(CbServer.Failover.MIN_EVENTS, max_plus_1):
            status = self.rest.update_auto_failover_settings(
                enable_failover, timeout_val, max_count=num_events)
            self.assertTrue(status, "Failed to set max events=%s" % num_events)
            self.validate_failover_settings(
                enable_failover == "true", timeout_val, 0, num_events)

        for num_events in [0, max_plus_1]:
            self.log.info("Testing max_event_count=%s" % num_events)
            status, _ = self.rest.update_auto_failover_settings(
                enable_failover, timeout_val, max_count=max_plus_1)
            self.assertFalse(status, "Able to set max events=%s" % num_events)
            self.validate_failover_settings(
                enable_failover == "true", timeout_val, 0,
                CbServer.Failover.MAX_EVENTS)

    def __run_test(self):
        reb_util = RebalanceUtil(self.cluster)
        # Validate count before the start of failover procedure
        self.validate_failover_settings(True, self.timeout,
                                        self.fo_events, self.max_count)

        # Before failure - nodes' information
        self.__display_failure_node_status("Nodes to be failed")

        try:
            rest_nodes = self.cluster_util.get_nodes(self.cluster.master)
            if self.current_fo_strategy == CbServer.Failover.Type.AUTO:
                expected_fo_nodes = self.num_nodes_to_be_failover
                self.fo_events += expected_fo_nodes
                self.__update_server_obj()
                failover_task = ConcurrentFailoverTask(
                    task_manager=self.task_manager, master=self.orchestrator,
                    servers_to_fail=self.nodes_to_fail,
                    expected_fo_nodes=self.fo_events,
                    task_type="induce_failure")
                self.task_manager.add_new_task(failover_task)
                self.task_manager.get_task_result(failover_task)
                if failover_task.result is False:
                    self.fail("Failure during concurrent failover procedure")
            elif self.current_fo_strategy == CbServer.Failover.Type.GRACEFUL:
                fo_err_msg = "Failover Node failed: Failover cannot be done " \
                             "gracefully for a node " \
                             "without data service. Use hard failover"
                for node in self.nodes_to_fail:
                    node = [t_node for t_node in rest_nodes
                            if t_node.ip == node.ip][0]
                    try:
                        status, _ = self.rest.perform_graceful_failover(
                            node.id)
                    except Exception as e_msg:
                        if CbServer.Services.KV in node.services:
                            self.fail("Graceful failover failed for %s:%s"
                                      % (node, node.services))
                        if fo_err_msg not in str(e_msg):
                            self.fail("Unexpected message: %s" % e_msg)
                    else:
                        if status and \
                                CbServer.Services.KV not in node.services:
                            self.fail("Graceful FO happened without KV %s: %s"
                                      % (node, node.services))
                        if status is False:
                            self.fail("Graceful failover failed for %s: %s"
                                      % (node, node.services))
                        self.sleep(5, "Wait for failover to start")
                        reb_result = reb_util.monitor_rebalance()
                        self.assertTrue(reb_result, "Graceful failover failed")
            elif self.current_fo_strategy == CbServer.Failover.Type.FORCEFUL:
                for node in self.nodes_to_fail:
                    node = [t_node for t_node in rest_nodes
                            if t_node.ip == node.ip][0]
                    status, _ = self.rest.perform_hard_failover(node.id)
                    if status is False:
                        self.fail("Hard failover failed for %s" % node)
                    self.sleep(5, "Wait for failover to start")
                    reb_result = reb_util.monitor_rebalance()
                    self.assertTrue(reb_result, "Hard failover failed")

            new_replica = self.input.param('new_bucket_replica', None)
            if new_replica is not None:
                self.bucket_util.update_all_bucket_replicas(self.cluster,
                                                            new_replica)

            if self.input.param('test_non_durable_sync_writes', False):
                NsServerFeaturePlugins.test_durability_impossible_fallback(
                    self, self.orchestrator)
                self.log.info(
                    "Setting durable fallback for doc-ops to work always")
                bucket_param = {
                    Bucket.durabilityImpossibleFallback: "fallbackToActiveAck"}
                bucket_rest = BucketRestApi(self.cluster.master)
                for t_bucket in self.cluster.buckets:
                    bucket_rest.edit_bucket(t_bucket.name, bucket_param)
                self.sleep(12, "Wait for new settings to take effect")
        except Exception as e:
            self.log.error("Exception occurred: %s" % str(e))
            raise e
        finally:
            # Disable auto-fo after the expected time limit
            self.rest.update_auto_failover_settings(
                enabled="false", timeout=self.timeout, max_count=self.max_count,
                failover_preserve_durability=self.preserve_durability_during_auto_fo)

            if self.current_fo_strategy == CbServer.Failover.Type.AUTO:
                failover_task = ConcurrentFailoverTask(
                    task_manager=self.task_manager, master=self.orchestrator,
                    servers_to_fail=self.nodes_to_fail,
                    task_type="revert_failure")
                self.task_manager.add_new_task(failover_task)
                self.task_manager.get_task_result(failover_task)
                if failover_task.result is False:
                    self.fail("Failure during failover operation")

            # Enable back prev auto_fo settings
            self.sleep(60, "Wait before enabling back auto-fo")
            self.rest.update_auto_failover_settings(
                enabled="true", timeout=self.timeout, max_count=self.max_count,
                failover_preserve_durability=self.preserve_durability_during_auto_fo)

        # After failure - failed nodes' information
        self.__display_failure_node_status("Nodes status failure")

        self.bucket_util.print_bucket_stats(self.cluster)
        # Validate count at the end of failover procedure
        self.validate_failover_settings(True, self.timeout,
                                        self.fo_events, self.max_count)

    def test_concurrent_failover(self):
        """
        Common code to run failover tests
        """
        self.current_fo_strategy = None
        load_data_after_fo = self.input.param("post_failover_data_load", False)
        pause_rebalance_test = self.input.param("pause_rebalance_test", False)
        pre_fo_data_load = self.input.param("pre_fo_data_load", False)
        update_replica = self.input.param("update_replica", 0)
        update_replica_number_to = self.input.param(
            "update_replica_number_to", self.num_replicas)
        exception = None
        if pre_fo_data_load:
            self.__perform_doc_ops(durability=self.durability_level,
                                   validate_num_items=True)
        if update_replica > 0:
            buckets = random.sample(self.cluster.buckets, update_replica)
            for bucket in buckets:
                self.bucket_util.update_bucket_property(
                    self.cluster.master, bucket,
                    replica_number=update_replica_number_to)

            rebalance_task = self.task.async_rebalance(
                self.cluster, [], [], retry_get_process_num=3000)
            self.task_manager.get_task_result(rebalance_task)
            self.find_minimum_bucket_replica()
            self.__perform_doc_ops(durability=self.durability_level,
                                   validate_num_items=True)

        for index, services_to_fo in enumerate(self.failover_order):
            self.current_fo_strategy = self.failover_type[index]
            # servers_to_fail -> kv:index / kv:index_kv / index:n1ql
            services_to_fo = services_to_fo.split(":")
            # servers_to_fail -> [kv, index] / [kv, index_kv]
            self.nodes_to_fail = self.get_nodes_to_fail(services_to_fo)
            self.__update_unaffected_node()
            try:
                self.__run_test()
            except Exception as e:
                # Making sure to remove failed nodes before failing the test
                self.cluster_util.rebalance(self.cluster)
                self.fail("Exception occurred: %s" % str(e))

            # Perform collection crud + doc_ops before rebalance operation
            if load_data_after_fo:
                try:
                    self.__perform_doc_ops(durability="NONE",
                                           validate_num_items=False)
                except Exception as e:
                    exception = e
                    break

        self.sleep(20, "Wait for failed nodes to recover completely")
        if choice([True, False]):
            # Add back all nodes and rebalance
            self.log.info("Performing node add back operation")
            rest_nodes = self.cluster_util.get_nodes(self.cluster.master,
                                                     inactive_added=True,
                                                     inactive_failed=True)
            for node in rest_nodes:
                if node.clusterMembership == "inactiveFailed":
                    self.rest.re_add_node(node.id)
                    if CbServer.Services.KV in node.services:
                        self.rest.set_recovery_type(node.id, "delta")
            result = self.cluster_util.rebalance(self.cluster,
                                                 wait_for_completion=
                                                 (not pause_rebalance_test))
        else:
            # Eject nodes and rebalance
            self.log.info("Ejecting all failed nodes from the cluster")
            result = self.cluster_util.rebalance(self.cluster,
                                                 wait_for_completion=
                                                 (not pause_rebalance_test))

        reb_util = RebalanceUtil(self.cluster)
        if pause_rebalance_test:
            status, content = self.rest.stop_rebalance()
            self.assertTrue(status, f"Stop rebalance failed: {content}")
            for i in range(self.wait_timeout):
                if reb_util.get_rebalance_status() != 'running':
                    break
                self.log.warn("Rebalance not stopped after {0} sec"
                                   .format(i + 1))
                self.sleep(1)
            else:
                self.fail("Unable to stop rebalance")
            result = self.cluster_util.rebalance(self.cluster)

        if exception:
            self.fail(exception)
        self.assertTrue(result, "Final rebalance failed")

        # Validate count is reset back to 0 after rebalance operation
        self.validate_failover_settings(True, self.timeout, 0, self.max_count)

        # Perform collection crud + doc_ops
        if load_data_after_fo:
            durability_val = self.durability_level
            for bucket in self.cluster.buckets:
                # If we have bucket_replica=3, force use level=NONE
                if bucket.replicaNumber == Bucket.ReplicaNum.THREE:
                    durability_val = SDKConstants.DurabilityLevel.NONE
                    break
                # If we have ephemeral bucket, force use level=MAJORITY
                if bucket.bucketType == Bucket.Type.EPHEMERAL:
                    durability_val = SDKConstants.DurabilityLevel.MAJORITY
            self.__perform_doc_ops(durability=durability_val)

    def test_split_brain(self):
        """
        Test params:
        split_nodes - Accepts string of pattern 'a_b:c-b_a:d'
                      This creates a barriers like,
                      Node running services a_b & c to ignore anything from
                      nodes running services b_a & d and vice versa
        """
        self.failover_method = "network_split"
        def get_nodes_based_on_services(services):
            nodes = list()
            services = services.split(":")
            for t_service in services:
                t_service = t_service.split("_")
                t_service.sort()
                for c_node in cluster_nodes:
                    if c_node.services == t_service:
                        node_object = copy.deepcopy(self.__get_server_obj(c_node))
                        node_object.services = c_node.services
                        nodes.append(node_object)
                        # Remove nodes from cluster_nodes once picked
                        # to avoid picking same node again
                        cluster_nodes.remove(c_node)
                        break
            return nodes

        def create_split_between_nodes(dest_nodes, src_nodes):
            for ssh_node in dest_nodes:
                shell_conn = RemoteMachineShellConnection(ssh_node)
                for src_node in src_nodes:
                    shell_conn.execute_command(
                        "iptables -A INPUT -s %s -j DROP" % src_node.ip)
                    o,b = shell_conn.execute_command(
                        "nft add rule ip filter INPUT ip saddr %s counter "
                        "drop " % src_node.ip)
                shell_conn.disconnect()

        def recover_from_split(node_list):
            self.log.info("Flushing iptables rules from all nodes")
            for ssh_node in node_list:
                ssh_shell = RemoteMachineShellConnection(ssh_node)
                ssh_shell.execute_command("iptables -F")
                ssh_shell.execute_command("nft flush ruleset")
                ssh_shell.disconnect()
            self.sleep(5, "Wait for nodes to be reachable")

        def post_failover_procedure():
            RebalanceUtil(self.cluster.master).monitor_rebalance()
            self.validate_failover_settings(True, self.timeout,
                                            num_nodes_to_fo,
                                            self.max_count)
            recover_from_split(node_split_1 + node_split_2)
            self.log.info("Rebalance out failed nodes")
            rebalance_res = self.cluster_util.rebalance(self.cluster)
            self.assertTrue(rebalance_res, "Post failover rebalance failed")

            # Validate failover count reset post rebalance
            self.validate_failover_settings(True, self.timeout,
                                            0, self.max_count)

        fo_happens = self.input.param("fo_happens", True)
        nodes_to_split = self.input.param("split_nodes", None).split('-')

        if nodes_to_split is None:
            self.fail("Nothing to test. split_nodes is None")

        # Validate count before the start of failover procedure
        self.validate_failover_settings(True, self.timeout,
                                        self.fo_events, self.max_count)

        self.log.info("Fetching current cluster_nodes")
        self.cluster_util.find_orchestrator(self.cluster)
        # cluster_nodes holds servers which are not yet selected for nw split
        cluster_nodes = self.cluster_util.get_nodes(self.cluster.master)
        for node in cluster_nodes:
            node.services.sort()

        # Fetch actual nodes from given service list to create a split
        node_split_1 = get_nodes_based_on_services(nodes_to_split[0])
        node_split_2 = get_nodes_based_on_services(nodes_to_split[1])

        service_count = [dict(), dict()]
        for index, split_services in enumerate(nodes_to_split):
            for node_services in nodes_to_split[index].split(':'):
                for service in node_services.split("_"):
                    if service not in service_count[index]:
                        service_count[index][service] = 0
                    service_count[index][service] += 1

        if len(node_split_1) == len(node_split_2):
            num_nodes_to_fo = 0
        else:
            smaller_chunk_in_split = node_split_1 if len(node_split_1) < len(
                node_split_2) else node_split_2
            fo_nodes = dict()
            for node in smaller_chunk_in_split:
                fo_nodes[node] = self.failover_method
            self.nodes_to_fail = fo_nodes
            num_nodes_to_fo = self.num_nodes_to_be_failover

        self.rest = ClusterRestAPI(self.orchestrator)
        self.log.info("N/w split between -> [%s] || [%s]. Expect %s fo_events"
                      % ([n.ip for n in node_split_1],
                         [n.ip for n in node_split_2],
                         num_nodes_to_fo))
        try:
            create_split_between_nodes(node_split_1, node_split_2)
            create_split_between_nodes(node_split_2, node_split_1)

            self.sleep(self.timeout, "Wait for configured fo_timeout")
            self.sleep(15, "Extra sleep to avoid fail results")

            if fo_happens:
                self.log.info("Expecting failover to be triggered")
                post_failover_procedure()
            elif len([t_serv for t_serv in self.services_init.split("-")
                      if CbServer.Services.KV in t_serv]) > 2:
                self.log.info("Expecting no failover will be triggered")
                self.validate_failover_settings(True, self.timeout,
                                                0, self.max_count)
                if (self.nodes_init % 2) == 1:
                    # Pick new master based on split network
                    new_master = node_split_1[0]
                    if len(node_split_2) > len(node_split_1):
                        new_master = node_split_2[0]

                    self.sleep(10, "FO expected wrt node %s" % new_master.ip)
                    self.rest = ClusterRestAPI(new_master)
                    self.cluster.master = new_master

                    post_failover_procedure()
        finally:
            recover_from_split(node_split_1 + node_split_2)
            self.sleep(5, "Wait for n/w split to heal")
            rest_nodes = self.cluster_util.get_nodes(self.cluster.master,
                                                     inactive_added=True,
                                                     inactive_failed=True)
            for t_node in rest_nodes:
                if t_node.clusterMembership == "active":
                    for node in self.cluster.servers:
                        if node.ip == t_node.ip:
                            self.cluster.master = node
                            break
                    break
            reb_result = self.cluster_util.rebalance(self.cluster)
            self.assertTrue(reb_result, "Final rebalance failed")

    def test_concurrent_failover_timer_reset(self):
        """
        1. Trigger failure on destined nodes
        2. Wait for little less time than failover_timeout
        3. Bring back few nodes back online for few seconds
        4. Make sure no auto failover triggered till next failover timeout
        5. Validate auto failovers after new timeout
        """

        services_to_fo = self.failover_order[0].split(":")
        self.nodes_to_fail = self.get_nodes_to_fail(services_to_fo,
                                                    dynamic_fo_method=True)
        expected_fo_nodes = self.num_nodes_to_be_failover
        self.__update_server_obj()
        rand_node = choice(list(self.nodes_to_fail.keys()))
        self.__update_unaffected_node()
        self.__display_failure_node_status("Nodes to be failed")
        try:
            self.log.info("Starting auto-failover procedure")
            failover_task = ConcurrentFailoverTask(
                task_manager=self.task_manager, master=self.orchestrator,
                servers_to_fail=self.nodes_to_fail,
                expected_fo_nodes=expected_fo_nodes,
                task_type="induce_failure")
            self.task_manager.add_new_task(failover_task)
            self.sleep(int(self.timeout * 0.7),
                       "Wait before bringing back the failed nodes")

            self.log.info("Bringing back '%s' for some time" % rand_node.ip)
            new_timer = None
            shell = RemoteMachineShellConnection(rand_node)
            cb_err = CouchbaseError(self.log,
                                    shell,
                                    node=rand_node)
            if self.nodes_to_fail[rand_node] == CouchbaseError.STOP_MEMCACHED:
                cb_err.revert(CouchbaseError.STOP_MEMCACHED)
                self.sleep(10, "Wait before creating failure again")
                cb_err.create(CouchbaseError.STOP_MEMCACHED)
                new_timer = time()
            elif self.nodes_to_fail[rand_node] == "stop_couchbase":
                cb_err.revert(CouchbaseError.STOP_SERVER)
                self.sleep(10, "Wait before creating failure again")
                cb_err.create(CouchbaseError.STOP_SERVER)
                new_timer = time()
            shell.disconnect()

            # Validate the previous auto-failover task failed
            # due to the random_node coming back online
            self.task_manager.get_task_result(failover_task)
            self.assertFalse(failover_task.result,
                             "Nodes failed over though nodes became active")

            # Validate auto_failover_settings
            self.validate_failover_settings(True, self.timeout,
                                            0, self.max_count)

            # Make sure the new auto-failover timing is honoured
            new_timer = new_timer + self.timeout
            while int(time()) < new_timer:
                _, settings = self.rest.get_auto_failover_settings()
                if settings.count != 0:
                    self.fail("Nodes failed over before new failover time")

            self.sleep(10, "Wait for failover rebalance to trigger")
            RebalanceUtil(self.cluster).monitor_rebalance()

            # Validate auto_failover_settings after actual auto failover
            self.validate_failover_settings(True, self.timeout,
                                            expected_fo_nodes, self.max_count)
        finally:
            # Recover all nodes from induced failures
            failover_task = ConcurrentFailoverTask(
                task_manager=self.task_manager, master=self.orchestrator,
                servers_to_fail=self.nodes_to_fail,
                expected_fo_nodes=expected_fo_nodes,
                task_type="revert_failure")
            self.task_manager.add_new_task(failover_task)
            self.task_manager.get_task_result(failover_task)
            self.sleep(15, "wait post failure revert")
            self.log.info("Rebalance out the failed nodes")
            result = self.cluster_util.rebalance(self.cluster)
            self.assertTrue(result, "Final rebalance failed")

        # Perform collection crud + doc_ops after rebalance operation
        self.__perform_doc_ops()

    def test_failover_during_rebalance(self):
        """
        1. Start rebalance operation on the active cluster
        2. Introduce failures on target nodes to trigger auto-failover
        3. Validate rebalance succeeds after auto-fo trigger
        """
        def get_reb_out_nodes():
            nodes = list()
            nodes_with_services = dict()
            cluster_nodes = self.cluster_util.get_nodes(self.cluster.master)
            for node in cluster_nodes:
                node.services.sort()
                d_key = '_'.join(node.services)
                if d_key not in nodes_with_services:
                    nodes_with_services[d_key] = list()
                nodes_with_services[d_key].append(node)

            for services in out_nodes:
                services = services.split("_")
                services.sort()
                services = "_".join(services)
                rand_node = choice(nodes_with_services[services])
                nodes_with_services[services].remove(rand_node)
                nodes.append(rand_node)
            return nodes

        self.nodes_in = self.input.param("nodes_in", 0)
        pre_fo_data_load = self.input.param("pre_fo_data_load", False)
        if pre_fo_data_load:
            self.__perform_doc_ops(durability=self.durability_level,
                                   validate_num_items=True)

        add_nodes = list()
        remove_nodes = list()
        # Format - kv:kv_index -> 2 nodes with services [kv, kv:index]
        out_nodes = self.input.param("out_nodes", "kv").split(":")
        # Can take any of (in/out/swap)
        rebalance_type = self.input.param("rebalance_type", "in")
        # During hard failover + non-durability case, there is a chance that
        # active data in mem. can be lost before replication completes
        # resulting in less num_items than expected
        skip_post_rebalance_data_validation = self.input.param(
            "skip_post_rebalance_data_validation", False)
        perform_doc_ops_before_rebalance = self.input.param(
            "perform_doc_ops_before_rebalance", True)

        services_to_fo = self.failover_order[0].split(":")
        self.nodes_to_fail = self.get_nodes_to_fail(services_to_fo,
                                                    dynamic_fo_method=True)
        loader_task = None
        reader_task = None

        if rebalance_type == "in":
            add_nodes = self.cluster.servers[
                self.nodes_init:self.nodes_init+self.nodes_in]
            self.cluster.kv_nodes.extend(add_nodes)
        elif rebalance_type == "out":
            remove_nodes = get_reb_out_nodes()
        elif rebalance_type == "swap":
            remove_nodes = get_reb_out_nodes()
            add_nodes = self.cluster.servers[
                self.nodes_init:self.nodes_init+self.nodes_in]
            self.cluster.kv_nodes.extend(add_nodes)

        expected_fo_nodes = self.num_nodes_to_be_failover
        self.__update_server_obj()

        # Start doc_ops in background
        if self.load_during_fo:
            doc_gen = doc_generator("fo_docs", 0, 200000)
            loader_task = self.task.async_continuous_doc_ops(
                self.cluster, self.cluster.buckets[0], doc_gen,
                DocLoading.Bucket.DocOps.UPDATE, exp=5, process_concurrency=1)
            reader_task = self.task.async_continuous_doc_ops(
                self.cluster, self.cluster.buckets[0], doc_gen,
                DocLoading.Bucket.DocOps.READ, process_concurrency=1)

        self.__update_unaffected_node()
        self.__display_failure_node_status("Nodes to be failed")

        # Create Auto-failover task but won't start it
        failover_task = ConcurrentFailoverTask(
            task_manager=self.task_manager, master=self.orchestrator,
            servers_to_fail=self.nodes_to_fail,
            expected_fo_nodes=expected_fo_nodes,
            task_type="induce_failure")

        # Start rebalance operation
        self.log.info("Starting rebalance operation")
        rebalance_task = self.task.async_rebalance(
            self.cluster, to_add=add_nodes, to_remove=remove_nodes)

        self.sleep(max(10, 4*self.nodes_in),
                   "Wait for rebalance to start before failover")
        self.task_manager.add_new_task(failover_task)

        try:
            self.log.info("Wait for failover task to complete")
            self.task_manager.get_task_result(failover_task)

            failure_msg = "Auto-failover task failed"

            self.assertTrue(failover_task.result, failure_msg)

            # Validate auto_failover_settings after failover
            self.validate_failover_settings(True, self.timeout,
                                            expected_fo_nodes, self.max_count)

            # Stop background doc_ops
            if self.load_during_fo:
                for task in [loader_task, reader_task]:
                    task.end_task()
                    self.task_manager.get_task_result(task)

            # Perform collection crud + doc_ops before rebalance operation
            self.__update_unaffected_node()
            if perform_doc_ops_before_rebalance:
                self.__perform_doc_ops(durability="NONE",
                                       validate_num_items=False)
        finally:
            # Disable auto-fo after the expected time limit
            retry = 5
            for i in range(retry):
                try:
                    status = self.rest.update_auto_failover_settings(
                        enabled="false", timeout=self.timeout,
                        max_count=self.max_count,
                        failover_preserve_durability=self.preserve_durability_during_auto_fo)
                    self.assertTrue(status)
                    break
                except Exception as e:
                    if i >= retry - 1:
                        raise e
                    else:
                        self.sleep(1, "waiting 1 sec before afo setting "
                                      "update retry")

            # Recover all nodes from induced failures
            recovery_task = ConcurrentFailoverTask(
                task_manager=self.task_manager, master=self.orchestrator,
                servers_to_fail=self.nodes_to_fail,
                expected_fo_nodes=expected_fo_nodes,
                task_type="revert_failure")
            self.task_manager.add_new_task(recovery_task)
            self.task_manager.get_task_result(recovery_task)
            self.task_manager.stop_task(rebalance_task)

            # Enable back prev auto_fo settings
            self.sleep(5, "Wait before enabling back auto-fo")
            self.rest.update_auto_failover_settings(
                enabled="true", timeout=self.timeout,
                max_count=self.max_count,
                failover_preserve_durability=self.preserve_durability_during_auto_fo)

            # Rebalance the cluster to remove failed nodes
            result = self.cluster_util.rebalance(self.cluster)
            self.assertTrue(result, "Rebalance failed")

        # Validate auto_failover_settings after rebalance operation
        self.validate_failover_settings(True, self.timeout, 0,
                                        self.max_count)

        # Perform collection crud + doc_ops after rebalance operation
        self.__update_unaffected_node()
        validate_num_items = True
        if skip_post_rebalance_data_validation:
            validate_num_items = False
        self.__perform_doc_ops(validate_num_items=validate_num_items)

    def test_autofailover_preserve_durability(self):
        """
        with single v-bucket
        positive and negative afo test with combination picked from
        once with active/replica nodes and once without v-buckets
        """

        def add_node_to_failover(ip):
            for index, node in enumerate(nodes_in_cluster):
                if str(node.ip) in str(ip):
                    nodes_to_fo[node] = self.failover_method
        update_minimum_replica_number = self.input.\
            param("update_minimum_replica", None)
        update_afo_nodes_with_vbucket_number = \
            self.input.param("update_afo_nodes", None)
        nodes_in_cluster = self.cluster_util.get_nodes(self.cluster.master)
        nodes_to_fo = dict()
        iterator = 0
        # dividing all nodes in 2 node per zone
        server_group_rest = ServerGroupsAPI(self.cluster.master)
        while iterator < len(self.cluster.servers):
            iterator += 2
            group = "Group " + str(iterator)
            server_group_rest.create_server_group(group)
            nodes = [server for server in self.cluster.servers[
                                             iterator:iterator + 2]]
            self.cluster_util.shuffle_nodes_in_zones(nodes, "Group 1", group)

        cluster_details = self.cluster_util.get_cluster_stats()
        num_nodes_with_vbuckets_to_afo = 0
        if self.min_bucket_replica == 1:
            num_nodes_with_vbuckets_to_afo = 1
        elif self.min_bucket_replica >= 2:
            num_nodes_with_vbuckets_to_afo = self.min_bucket_replica - 1
        if update_afo_nodes_with_vbucket_number is not None:
            num_nodes_with_vbuckets_to_afo = update_afo_nodes_with_vbucket_number
        total_afo_nodes_without_vbuckets = len(
            self.failover_order[0].split(":")) - num_nodes_with_vbuckets_to_afo
        for node in cluster_details:
            if str(self.cluster.master.ip) in str(node):
                continue
            if cluster_details[node]["active_item_count"] > 0 or \
                    cluster_details[node]["replica_item_count"] > 0:
                if num_nodes_with_vbuckets_to_afo > 0:
                    add_node_to_failover(node)
                    num_nodes_with_vbuckets_to_afo -= 1
            elif total_afo_nodes_without_vbuckets > 0:
                add_node_to_failover(node)
                total_afo_nodes_without_vbuckets -= 1
        self.cluster_util.update_cluster_nodes_service_list(self.cluster)
        self.nodes_to_fail = nodes_to_fo
        self.current_fo_strategy = CbServer.Failover.Type.AUTO
        self.preserve_durability_during_auto_fo = False
        if update_minimum_replica_number is not None:
            self.min_bucket_replica = update_minimum_replica_number
        try:
            self.__run_test()
        except Exception as e:
            # Making sure to remove failed nodes before failing the test
            self.cluster_util.rebalance(self.cluster)
            self.fail("Exception occurred: %s" % str(e))
        self.cluster_util.rebalance(self.cluster)

    def test_MB_51219(self):
        """
        5 node cluster with all nodes running (kv, index, n1ql, fts) services
        Enable auto-failover with max_count=1, timeout=30
        Couchbase bucket with replicas=3
        Bring down two nodes A and B simultaneously
        Failover didn't happen as expected
        Bring up one of the node A
        """
        len_of_nodes_to_afo = len(self.failover_order[0].split(":"))
        nodes_to_fo = dict()
        nodes_in_cluster = self.cluster_util.get_nodes(self.cluster.master)
        for node in nodes_in_cluster:
            if len_of_nodes_to_afo <= 0:
                break
            if str(self.cluster.master.ip) == str(node.ip):
                continue
            nodes_to_fo[node] = self.failover_method
            len_of_nodes_to_afo -= 1
        self.cluster_util.update_cluster_nodes_service_list(self.cluster)
        self.nodes_to_fail = nodes_to_fo
        self.__update_server_obj()
        try:
            failover_task = ConcurrentFailoverTask(
                task_manager=self.task_manager, master=self.orchestrator,
                servers_to_fail=self.nodes_to_fail,
                expected_fo_nodes=self.fo_events,
                task_type="induce_failure")
            self.task_manager.add_new_task(failover_task)
            self.task_manager.get_task_result(failover_task)
            dictionary = dict(list(self.nodes_to_fail.items())[:1])
            failover_task = ConcurrentFailoverTask(
                task_manager=self.task_manager, master=self.orchestrator,
                servers_to_fail=dictionary,
                task_type="revert_failure")
            self.task_manager.add_new_task(failover_task)
            self.task_manager.get_task_result(failover_task)
            timeout = int(time()) + 15
            task_id_changed = False
            self.prev_rebalance_status_id = None
            while not task_id_changed and int(time()) < timeout:
                server_task = self.cluster_util.get_cluster_tasks(
                    task_type="rebalance", task_sub_type="failover")
                if server_task and server_task["statusId"] != \
                        self.prev_rebalance_status_id:
                    task_id_changed = True
                    self.prev_rebalance_status_id = server_task["statusId"]
                    self.log.debug("New failover status id: %s"
                                   % server_task["statusId"])
            self.assertTrue(task_id_changed,
                            "Fail-over did not happen as expected")
            self.bucket_util._wait_warmup_completed(self.cluster.buckets[0],
                                                    servers=[
                                                        self.cluster.master],
                                                    wait_time=30)
        finally:
            # reverting failure from all the nodes
            failover_task = ConcurrentFailoverTask(
                task_manager=self.task_manager, master=self.orchestrator,
                servers_to_fail=self.nodes_to_fail,
                task_type="revert_failure")
            self.task_manager.add_new_task(failover_task)
            self.task_manager.get_task_result(failover_task)
        result = self.cluster_util.rebalance(self.cluster)
        self.assertTrue(result, "Final re-balance failed")
