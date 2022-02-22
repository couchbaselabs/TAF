from random import choice
from time import time

from BucketLib.bucket import Bucket
from Cb_constants import CbServer, DocLoading
from Jython_tasks.task import ConcurrentFailoverTask
from bucket_collections.collections_base import CollectionBase
from collections_helper.collections_spec_constants import MetaCrudParams
from couchbase_helper.documentgenerator import doc_generator
from error_simulation.cb_error import CouchbaseError
from failover.AutoFailoverBaseTest import AutoFailoverBaseTest
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from table_view import TableView


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

        self.log.info("Updating Auto-failover settings")
        self.rest.update_autofailover_settings(
            enabled=True, timeout=self.timeout, maxCount=self.max_count,
            canAbortRebalance=self.can_abort_rebalance)

        # Find the bucket with least replica to check the Auto-FO possibility
        self.min_bucket_replica = Bucket.ReplicaNum.THREE
        for bucket in self.cluster.buckets:
            if bucket.replicaNumber < self.min_bucket_replica:
                self.min_bucket_replica = bucket.replicaNumber

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
        if self.sdk_client_pool is None:
            self.init_sdk_pool_object()
            CollectionBase.create_sdk_clients(
                self.task_manager.number_of_threads,
                self.cluster.master, self.cluster.buckets,
                self.sdk_client_pool, self.sdk_compression)

        # Perform initial collection load
        self.__load_initial_collection_data()

        self.log_setup_status(self.__class__.__name__, "complete",
                              self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "started",
                              self.tearDown.__name__)
        # Select KV node as a cluster master to perform tearDown rebalance out
        self.cluster_util.update_cluster_nodes_service_list(self.cluster)
        self.cluster.master = self.cluster.kv_nodes[0]

        self.log.info("Resetting auto-failover settings to default")
        self.rest.update_autofailover_settings(
            enabled=True, timeout=120, maxCount=1,
            canAbortRebalance=self.can_abort_rebalance)
        self.log_setup_status(self.__class__.__name__, "complete",
                              self.tearDown.__name__)

        super(ConcurrentFailoverTests, self).tearDown()

    def __get_collection_load_spec(self, doc_ttl=0):
        """
        Set doc_ttl for loading doc during failover operations
        """
        d_level = Bucket.DurabilityLevel.NONE
        if self.num_replicas != 3:
            # Since durability is not supported with replicas=3
            d_level = choice([
                Bucket.DurabilityLevel.NONE,
                Bucket.DurabilityLevel.MAJORITY,
                Bucket.DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE,
                Bucket.DurabilityLevel.PERSIST_TO_MAJORITY])
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
                MetaCrudParams.DocCrud.COMMON_DOC_KEY: "concurrent_fo_docs",

                MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS: 5000,
                MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION: 20,
                MetaCrudParams.DocCrud.READ_PERCENTAGE_PER_COLLECTION: 10,
                MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION: 10,
                MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION: 10,
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

            # Check to see whether the max_fo count is reached
            if self.max_count == (self.fo_events + len(fo_nodes)):
                return is_safe

            # Reference doc:
            # https://docs.couchbase.com/server/7.0/learn/clusters-and-availability/automatic-failover.html#failover-policy
            # Service / Data loss check
            if service == CbServer.Services.KV:
                if self.min_bucket_replica > 0 \
                        and node_count[CbServer.Services.KV] > 2:
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
        active_cluster_nodes = len(self.rest.get_nodes(inactive=False))
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
                if is_safe_to_fo(kv_service):
                    fo_nodes.add(node)
                    for service_type in node.services:
                        # Decrement the node count for the service
                        decr_node_count(service_type)
                else:
                    self.log.warning("KV failover not possible")
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
        if durability:
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
        # Update the list of service-nodes mapping in the cluster object
        self.cluster_util.update_cluster_nodes_service_list(self.cluster)
        nodes_in_cluster = self.rest.get_nodes()
        for services in services_to_fail:
            node_services = set(services.split("_"))
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
                    break
        return nodes

    def validate_failover_settings(self, enabled, timeout, count, max_count):
        settings = self.rest.get_autofailover_settings()
        self.auto_fo_settings_tbl.rows = list()
        self.auto_fo_settings_tbl.rows.append([
            str(settings.enabled), str(settings.count),
            str(settings.maxCount), str(settings.timeout),
            str(settings.failoverOnDataDiskIssuesEnabled),
            str(settings.failoverOnDataDiskIssuesTimeout)])
        self.auto_fo_settings_tbl.display("Auto failover status:")

        err_msg = "Mismatch in '%s' field. " \
                  "Cluster FO data: " + str(settings.__dict__)
        self.assertEqual(settings.enabled, enabled, err_msg % "enabled")
        self.assertEqual(settings.timeout, timeout, err_msg % "timeout")
        self.assertEqual(settings.count, count, err_msg % "count")
        self.assertEqual(settings.maxCount, max_count,
                         err_msg % "maxCount")

    def __display_failure_node_status(self, message):
        self.test_status_tbl.rows = list()
        cluster_nodes = self.rest.get_nodes(inactive=True)
        for node, fo_type in self.nodes_to_fail.items():
            node = [t_node for t_node in cluster_nodes
                    if t_node.ip == node.ip][0]
            self.test_status_tbl.add_row([node.ip, ",".join(node.services),
                                          node.clusterMembership, fo_type])
        self.test_status_tbl.display(message)

    def __update_unaffected_node(self):
        cluster_nodes = self.rest.get_nodes()
        for cluster_node in cluster_nodes:
            for failure_node in self.nodes_to_fail:
                if cluster_node.ip == failure_node.ip:
                    break
            else:
                self.orchestrator = cluster_node
                self.rest = RestConnection(self.orchestrator)
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
        enable_failover = True
        timeout_val = 10
        max_plus_1 = CbServer.Failover.MAX_EVENTS + 1

        # Set max_events between (min, max)
        for num_events in range(CbServer.Failover.MIN_EVENTS, max_plus_1):
            status = self.rest.update_autofailover_settings(
                enable_failover, timeout_val, maxCount=num_events)
            self.assertTrue(status, "Failed to set max events=%s" % num_events)
            self.validate_failover_settings(enable_failover, timeout_val,
                                            0, num_events)

        for num_events in [0, max_plus_1]:
            self.log.info("Testing max_event_count=%s" % num_events)
            status = self.rest.update_autofailover_settings(
                enable_failover, timeout_val, maxCount=max_plus_1)
            self.assertFalse(status, "Able to set max events=%s" % num_events)
            self.validate_failover_settings(enable_failover, timeout_val,
                                            0, CbServer.Failover.MAX_EVENTS)

    def __run_test(self):
        # Validate count before the start of failover procedure
        self.validate_failover_settings(True, self.timeout,
                                        self.fo_events, self.max_count)

        # Before failure - nodes' information
        self.__display_failure_node_status("Nodes to be failed")

        try:
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
                for node in self.nodes_to_fail:
                    status = self.rest.fail_over(node.otpNode, graceful=True)
                    if status is False:
                        self.fail("Graceful failover failed for %s" % node)
            elif self.current_fo_strategy == CbServer.Failover.Type.FORCEFUL:
                for node in self.nodes_to_fail:
                    status = self.rest.fail_over(node.otpNode, graceful=False)
                    if status is False:
                        self.fail("Hard failover failed for %s" % node)
        except Exception as e:
            self.log.error("Exception occurred: %s" % str(e))
        finally:
            # Disable auto-fo after the expected time limit
            self.rest.update_autofailover_settings(
                enabled=False, timeout=self.timeout, maxCount=self.max_count,
                canAbortRebalance=self.can_abort_rebalance)

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
            self.sleep(5, "Wait before enabling back auto-fo")
            self.rest.update_autofailover_settings(
                enabled=True, timeout=self.timeout, maxCount=self.max_count,
                canAbortRebalance=self.can_abort_rebalance)

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
        load_data_after_fo = self.input.param("post_failover_data_load", True)
        for index, services_to_fo in enumerate(self.failover_order):
            self.current_fo_strategy = self.failover_type[index]
            # servers_to_fail -> kv:index / kv:index_kv / index:n1ql
            services_to_fo = services_to_fo.split(":")
            # servers_to_fail -> [kv, index] / [kv, index_kv]
            self.nodes_to_fail = self.get_nodes_to_fail(services_to_fo)
            self.__update_unaffected_node()
            self.__run_test()

            # Perform collection crud + doc_ops before rebalance operation
            if load_data_after_fo:
                self.__perform_doc_ops(durability="NONE",
                                       validate_num_items=False)

        self.sleep(5, "Wait before rebalance out all failed nodes")
        result = self.cluster_util.rebalance(self.cluster)
        self.assertTrue(result, "Final rebalance failed")

        # Validate count is reset back to 0 after rebalance operation
        self.validate_failover_settings(True, self.timeout, 0, self.max_count)

        # Perform collection crud + doc_ops
        if load_data_after_fo:
            self.__perform_doc_ops()

    def test_split_brain(self):
        """
        Test params:
        split_nodes - Accepts string of pattern 'a_b:c-b_a:d'
                      This creates a barriers like,
                      Node running services a_b & c to ignore anything from
                      nodes running services b_a & d and vice versa
        """
        def get_nodes_based_on_services(services):
            nodes = list()
            services = services.split(":")
            for t_service in services:
                t_service = t_service.split("_")
                t_service.sort()
                for t_node in cluster_nodes:
                    if t_node.services == t_service:
                        nodes.append(self.__get_server_obj(t_node))
                        # Remove nodes from cluster_nodes once picked
                        # to avoid picking same node again
                        cluster_nodes.remove(t_node)
                        break
            return nodes

        def create_split_between_nodes(dest_nodes, src_nodes):
            for t_node in dest_nodes:
                shell_conn = RemoteMachineShellConnection(t_node)
                for src_node in src_nodes:
                    shell_conn.execute_command(
                        "iptables -A INPUT -s %s -j DROP" % src_node.ip)
                shell_conn.disconnect()

        def get_num_nodes_to_fo(num_nodes_affected,
                                service_count_affected_nodes,
                                service_count_unaffected_nodes):
            nodes_to_fo = num_nodes_affected
            for t_serv, count in service_count_affected_nodes.items():
                if t_serv not in service_count_unaffected_nodes \
                        or service_count_unaffected_nodes[t_serv] < 1:
                    nodes_to_fo -= service_count_affected_nodes[t_serv]
            return nodes_to_fo

        def recover_from_split(node_list):
            self.log.info("Flushing iptables rules from all nodes")
            for t_node in node_list:
                ssh_shell = RemoteMachineShellConnection(t_node)
                ssh_shell.execute_command("iptables -F")
                ssh_shell.disconnect()
            self.sleep(5, "Wait for nodes to be reachable")

        def post_failover_procedure():
            self.rest.monitorRebalance()
            self.validate_failover_settings(True, self.timeout,
                                            num_nodes_to_fo,
                                            self.max_count)
            recover_from_split(node_split_1 + node_split_2)
            self.log.info("Rebalance out failed nodes")
            reb_result = self.cluster_util.rebalance(self.cluster)
            self.assertTrue(reb_result, "Post failover rebalance failed")

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
        cluster_nodes = self.rest.get_nodes()
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

        if len(node_split_1) > len(node_split_2):
            num_nodes_to_fo = get_num_nodes_to_fo(len(node_split_2),
                                                  service_count[1],
                                                  service_count[0])
        else:
            num_nodes_to_fo = get_num_nodes_to_fo(len(node_split_1),
                                                  service_count[0],
                                                  service_count[1])

        self.log.info("Creating split between nodes %s || %s. "
                      "Expected %s failover events"
                      % (nodes_to_split[0], nodes_to_split[1],
                         num_nodes_to_fo))

        try:
            create_split_between_nodes(node_split_1, node_split_2)
            create_split_between_nodes(node_split_2, node_split_1)

            self.sleep(self.timeout, "Wait for configured fo_timeout")
            self.sleep(15, "Extra sleep to avoid fail results")

            if fo_happens:
                self.log.info("Expecting failover to be triggered")
                post_failover_procedure()
            else:
                self.log.info("Expecting no failover will be triggered")
                self.validate_failover_settings(True, self.timeout,
                                                0, self.max_count)
                if (self.nodes_init % 2) == 1:
                    # Pick new master based on split network
                    new_master = node_split_1[0]
                    if len(node_split_2) > len(node_split_1):
                        new_master = node_split_2[0]

                    self.sleep(10, "FO expected wrt node %s" % new_master.ip)
                    self.rest = RestConnection(new_master)
                    self.cluster.master = new_master

                    post_failover_procedure()
        finally:
            reb_result = self.cluster_util.rebalance(self.cluster)
            self.assertTrue(reb_result, "Final rebalance failed")
            recover_from_split(node_split_1 + node_split_2)

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
        rand_node = choice(self.nodes_to_fail.keys())
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
            cb_err = CouchbaseError(self.log, shell)
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
                settings = self.rest.get_autofailover_settings()
                if settings.count != 0:
                    self.fail("Nodes failed over before new failover time")

            self.sleep(10, "Wait for failover rebalance to trigger")
            self.rest.monitorRebalance()

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
            cluster_nodes = self.rest.get_nodes()
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

        add_nodes = list()
        remove_nodes = list()
        # Format - kv:kv_index -> 2 nodes with services [kv, kv:index]
        out_nodes = self.input.param("out_nodes", "kv").split(":")
        # Can take any of (in/out/swap)
        rebalance_type = self.input.param("rebalance_type", "in")
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
            self.cluster.nodes_in_cluster,
            to_add=add_nodes, to_remove=remove_nodes)

        self.sleep(max(10, 4*self.nodes_in),
                   "Wait for rebalance to start before failover")
        self.task_manager.add_new_task(failover_task)

        try:
            self.log.info("Wait for failover task to complete")
            self.task_manager.get_task_result(failover_task)

            self.assertTrue(failover_task.result, "Auto-failover task failed")
        finally:
            # Recover all nodes from induced failures
            recovery_task = ConcurrentFailoverTask(
                task_manager=self.task_manager, master=self.orchestrator,
                servers_to_fail=self.nodes_to_fail,
                expected_fo_nodes=expected_fo_nodes,
                task_type="revert_failure")
            self.task_manager.add_new_task(recovery_task)
            self.task_manager.get_task_result(recovery_task)
            self.task_manager.stop_task(rebalance_task)

        # Validate auto_failover_settings after failover
        self.validate_failover_settings(True, self.timeout,
                                        expected_fo_nodes, self.max_count)

        # Stop background doc_ops
        if self.load_during_fo:
            for task in [loader_task, reader_task]:
                task.end_task()
                self.task_manager.get_task_result(task)

        # Perform collection crud + doc_ops before rebalance operation
        self.__perform_doc_ops(durability="NONE", validate_num_items=False)

        # Rebalance the cluster to remove failed nodes
        result = self.cluster_util.rebalance(self.cluster)
        self.assertTrue(result, "Rebalance failed")

        # Validate auto_failover_settings after rebalance operation
        self.validate_failover_settings(True, self.timeout, 0, self.max_count)

        # Perform collection crud + doc_ops after rebalance operation
        self.__perform_doc_ops()
