from BucketLib.bucket import Bucket
from Cb_constants import CbServer
from Jython_tasks.task import ConcurrentFailoverTask
from error_simulation.cb_error import CouchbaseError
from failover.AutoFailoverBaseTest import AutoFailoverBaseTest


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

        self.log_setup_status(self.__class__.__name__, "complete",
                              self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "started",
                              self.tearDown.__name__)
        self.log.info("Resetting auto-failover settings to default")
        self.rest.update_autofailover_settings(
            enabled=True, timeout=120, maxCount=1,
            canAbortRebalance=self.can_abort_rebalance)
        self.log_setup_status(self.__class__.__name__, "complete",
                              self.tearDown.__name__)

        super(ConcurrentFailoverTests, self).tearDown()

    @property
    def num_nodes_to_be_failover(self):
        def is_safe_to_fo(service):
            # Service / Data loss check
            if service == CbServer.Services.KV \
                    and self.min_bucket_replica > 0 \
                    and node_count[CbServer.Services.KV] > 2:
                return True
            elif service == CbServer.Services.INDEX \
                    and node_count[CbServer.Services.INDEX] > 1:
                return True
            elif service == CbServer.Services.N1QL \
                    and node_count[CbServer.Services.N1QL] > 1:
                return True
            else:
                self.log.critical("Safety check missing for '%s'" % service)

            return False

        def decr_node_count(service):
            node_count[service] -= 1
            if service == CbServer.Services.KV:
                self.min_bucket_replica -= 1

        expected_num_nodes = 0
        num_unreachable_nodes = 0
        active_cluster_nodes = len(self.rest.get_nodes(inactive=False))
        total_nodes = active_cluster_nodes + self.fo_events
        min_nodes_for_quorum = int(total_nodes/2) + 1
        max_allowed_unreachable_nodes = total_nodes - min_nodes_for_quorum

        # Quorum check before checking individual services
        for _, failure_type in self.nodes_to_fail.items():
            if failure_type in ["stop_couchbase", "network_split"]:
                num_unreachable_nodes += 1
        if num_unreachable_nodes > max_allowed_unreachable_nodes:
            return expected_num_nodes
        # End of quorum check

        node_count = dict()
        node_count[CbServer.Services.KV] = len(self.cluster.kv_nodes)
        node_count[CbServer.Services.INDEX] = len(self.cluster.index_nodes)
        node_count[CbServer.Services.N1QL] = len(self.cluster.query_nodes)

        for node, failure_type in self.nodes_to_fail.items():
            node_fo_possible = False
            if CbServer.Services.KV in node.services:
                # KV takes priority over other nodes in deciding the Auto-FO
                if is_safe_to_fo(CbServer.Services.KV):
                    node_fo_possible = True
            else:
                # For other nodes, we need to check if the node running
                # other services are also safe to failover
                for service_type in node.services:
                    if not is_safe_to_fo(service_type):
                        break
                else:
                    node_fo_possible = True

            if node_fo_possible:
                expected_num_nodes += 1
                for service_type in node.services:
                    decr_node_count(service_type)

        return expected_num_nodes

    def get_nodes_to_fail(self, services_to_fail):
        nodes = dict()
        # Update the list of service-nodes mapping in the cluster object
        self.cluster_util.update_cluster_nodes_service_list(self.cluster)
        nodes_in_cluster = self.rest.get_nodes()
        for services in services_to_fail:
            node_services = set(services.split("_"))
            for index, node in enumerate(nodes_in_cluster):
                if node_services == set(node.services):
                    nodes[node] = self.failover_method
                    # Remove the node to be failed to avoid double insertion
                    nodes_in_cluster.pop(index)
                    break

        return nodes

    def validate_failover_settings(self, enabled, timeout, count, max_count):
        settings = self.rest.get_autofailover_settings()
        err_msg = "Mismatch in '%s' field. " \
                  "Cluster FO data: " + str(settings.__dict__)
        self.assertEqual(settings.enabled, enabled, err_msg % "enabled")
        self.assertEqual(settings.timeout, timeout, err_msg % "timeout")
        self.assertEqual(settings.count, count, err_msg % "count")
        self.assertEqual(settings.maxCount, max_count,
                         err_msg % "maxCount")

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
        try:
            if self.current_fo_strategy == CbServer.Failover.Type.AUTO:
                expected_fo_nodes = self.num_nodes_to_be_failover
                failover_task = ConcurrentFailoverTask(
                    task_manager=self.task_manager, master=self.orchestrator,
                    servers_to_fail=self.nodes_to_fail,
                    expected_fo_nodes=expected_fo_nodes,
                    task_type="induce_failure")
                self.task_manager.add_new_task(failover_task)
                self.task_manager.get_task_result(failover_task)
                self.fo_events += expected_fo_nodes
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
            if self.current_fo_strategy == CbServer.Failover.Type.AUTO:
                failover_task = ConcurrentFailoverTask(
                    task_manager=self.task_manager, master=self.orchestrator,
                    servers_to_fail=self.nodes_to_fail,
                    task_type="revert_failure")
                self.task_manager.add_new_task(failover_task)
                self.task_manager.get_task_result(failover_task)
                if failover_task.result is False:
                    self.fail("Failure during failover operation")

        # Validate count at the end of failover procedure
        self.validate_failover_settings(True, self.timeout,
                                        self.fo_events, self.max_count)

    def test_concurrent_failover(self):
        """
        Common code to run failover tests
        """
        self.current_fo_strategy = None
        for index, services_to_fo in enumerate(self.failover_order):
            self.current_fo_strategy = self.failover_type[index]
            # servers_to_fail -> kv:index / kv:index_kv / index:n1ql
            services_to_fo = services_to_fo.split(":")
            # servers_to_fail -> [kv, index] / [kv, index_kv]
            self.nodes_to_fail = self.get_nodes_to_fail(services_to_fo)
            self.log.info("Current target nodes: %s" % self.nodes_to_fail)
            self.__run_test()