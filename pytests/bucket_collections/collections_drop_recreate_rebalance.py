import threading
import time

from collections_helper.collections_spec_constants import MetaCrudParams
from bucket_collections.collections_base import CollectionBase
from membase.api.rest_client import RestConnection
from bucket_utils.bucket_ready_functions import BucketUtils


class CollectionsDropRecreateRebalance(CollectionBase):
    def setUp(self):
        super(CollectionsDropRecreateRebalance, self).setUp()
        self.known_nodes = self.cluster.servers[:self.nodes_init]
        self.nodes_failover = self.input.param("nodes_failover", 1)
        self.nodes_swap = self.input.param("nodes_swap", 0)
        self.data_load_flag = False  # When to start/stop drop/recreate
        self.recovery_type = self.input.param("recovery_type", "delta")
        self.data_loading_thread = None

    def tearDown(self):
        if self.data_loading_thread:
            # stop data loading before tearDown if its still running
            self.data_load_flag = False
            self.data_loading_thread.join()
            self.data_loading_thread = None
        super(CollectionsDropRecreateRebalance, self).tearDown()

    def pick_nodes_for_rebalance(self):
        if self.nodes_swap:
            self.nodes_in = self.nodes_out = self.nodes_swap
        self.add_nodes = self.cluster.servers[self.nodes_init:self.nodes_init + self.nodes_in]
        self.remove_nodes = self.cluster.servers[:self.nodes_out]
        self.cluster.master = self.master = self.cluster.servers[self.nodes_out]
        self.rest = RestConnection(self.cluster.master)

    def pick_nodes_for_failover(self, rebalance_operation):
        self.failover_nodes = self.cluster.servers[:self.nodes_failover]
        # Change the orchestrator, if there is rebalance-out of orchestrator after failover
        if "rebalance_out" in rebalance_operation:
            self.cluster.master = self.master = self.cluster.servers[self.nodes_failover]
        self.rest = RestConnection(self.cluster.master)

    def wait_for_failover_or_assert(self, expected_failover_count, timeout=180):
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

    def wait_for_rebalance_to_complete(self, task):
        self.task.jython_task_manager.get_task_result(task)
        self.assertTrue(task.result, "Rebalance Failed")

    def spec_for_drop_recreate(self):
        spec = {
            # Scope/Collection ops params
            MetaCrudParams.COLLECTIONS_TO_FLUSH: 0,
            MetaCrudParams.COLLECTIONS_TO_DROP: 250,

            MetaCrudParams.SCOPES_TO_DROP: 3,
            MetaCrudParams.SCOPES_TO_ADD_PER_BUCKET: 0,
            MetaCrudParams.COLLECTIONS_TO_ADD_FOR_NEW_SCOPES: 0,

            MetaCrudParams.COLLECTIONS_TO_ADD_PER_BUCKET: 0,

            # Only dropped scope/collection will be created.
            # While scope recreated all prev collection will also be created
            # In both the collection creation case, previous maxTTL value of
            # individual collection is considered
            MetaCrudParams.SCOPES_TO_RECREATE: 3,
            MetaCrudParams.COLLECTIONS_TO_RECREATE: 250,

            # Applies only for the above listed scope/collection operations
            MetaCrudParams.BUCKET_CONSIDERED_FOR_OPS: "all",
            MetaCrudParams.SCOPES_CONSIDERED_FOR_OPS: "all",
            MetaCrudParams.COLLECTIONS_CONSIDERED_FOR_OPS: "all",
        }
        return spec

    def data_load(self):
        while self.data_load_flag:
            doc_loading_spec = self.spec_for_drop_recreate()
            op_details = BucketUtils.perform_tasks_from_spec(self.cluster,
                                                             self.bucket_util.buckets,
                                                             doc_loading_spec)

    def load_collections_with_rebalance(self, rebalance_operation):
        self.pick_nodes_for_rebalance()

        self.data_load_flag = True
        self.data_loading_thread = threading.Thread(target=self.data_load)
        self.data_loading_thread.start()

        if rebalance_operation == "rebalance_in":
            operation = self.task.async_rebalance(self.known_nodes, self.add_nodes, [])
        elif rebalance_operation == "rebalance_out":
            operation = self.task.async_rebalance(self.known_nodes, [], self.remove_nodes)
        elif rebalance_operation == "swap_rebalance":
            for node in self.add_nodes:
                self.rest.add_node(self.cluster.master.rest_username, self.cluster.master.rest_password,
                                   node.ip, self.cluster.servers[self.nodes_init].port)
            operation = self.task.async_rebalance(self.known_nodes, [], self.remove_nodes,
                                                  check_vbucket_shuffling=False)
        elif rebalance_operation == "rebalance_in_out":
            for node in self.add_nodes:
                self.rest.add_node(self.cluster.master.rest_username, self.cluster.master.rest_password,
                                   node.ip, self.cluster.servers[self.nodes_init].port)
            operation = self.task.async_rebalance(self.known_nodes, [], self.remove_nodes,
                                                  check_vbucket_shuffling=False)

        self.wait_for_rebalance_to_complete(operation)
        self.data_load_flag = False
        self.data_loading_thread.join()
        self.data_loading_thread = None

    def load_collections_with_failover(self, rebalance_operation):
        self.pick_nodes_for_failover(rebalance_operation)

        self.data_load_flag = True
        self.data_loading_thread = threading.Thread(target=self.data_load)
        self.data_loading_thread.start()

        graceful = True if "graceful" in rebalance_operation else False
        failover_count = 0
        for failover_node in self.failover_nodes:
            failover_operation = self.task.failover(self.known_nodes, failover_nodes=[failover_node],
                                                    graceful=graceful, wait_for_pending=120)
            failover_count = failover_count + 1
            self.wait_for_failover_or_assert(failover_count)

        if "recovery" in rebalance_operation:
            for failover_node in self.failover_nodes:
                self.rest.set_recovery_type(otpNode='ns_1@' + failover_node.ip,
                                            recoveryType=self.recovery_type)
            operation = self.task.async_rebalance(self.known_nodes, [], [])
        else:
            operation = self.task.async_rebalance(self.known_nodes, [], self.failover_nodes)

        self.wait_for_rebalance_to_complete(operation)
        self.sleep(60, "Wait after rebalance completes before stopping data load")
        self.data_load_flag = False
        self.data_loading_thread.join()
        self.data_loading_thread = None

    def test_data_load_collections_with_rebalance_in(self):
        self.load_collections_with_rebalance(rebalance_operation="rebalance_in")

    def test_data_load_collections_with_rebalance_out(self):
        self.load_collections_with_rebalance(rebalance_operation="rebalance_out")

    def test_data_load_collections_with_swap_rebalance(self):
        self.load_collections_with_rebalance(rebalance_operation="swap_rebalance")

    def test_data_load_collections_with_rebalance_in_out(self):
        self.load_collections_with_rebalance(rebalance_operation="rebalance_in_out")

    def test_data_load_collections_with_graceful_failover_rebalance_out(self):
        self.load_collections_with_failover(rebalance_operation="graceful_failover_rebalance_out")

    def test_data_load_collections_with_hard_failover_rebalance_out(self):
        self.load_collections_with_failover(rebalance_operation="hard_failover_rebalance_out")

    def test_data_load_collections_with_graceful_failover_recovery(self):
        self.load_collections_with_failover(rebalance_operation="graceful_failover_recovery")

    def test_data_load_collections_with_hard_failover_recovery(self):
        self.load_collections_with_failover(rebalance_operation="hard_failover_recovery")
