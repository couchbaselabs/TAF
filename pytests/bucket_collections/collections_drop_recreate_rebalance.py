import threading
import time

from cb_constants import CbServer
from collections_helper.collections_spec_constants import MetaCrudParams
from bucket_collections.collections_base import CollectionBase
from membase.api.rest_client import RestConnection
from bucket_utils.bucket_ready_functions import BucketUtils
from couchbase_helper.tuq_helper import N1QLHelper
from shell_util.remote_connection import RemoteMachineShellConnection

from table_view import TableView


class CollectionsDropRecreateRebalance(CollectionBase):
    def setUp(self):
        super(CollectionsDropRecreateRebalance, self).setUp()
        self.known_nodes = self.cluster.servers[:self.nodes_init]
        self.nodes_failover = self.input.param("nodes_failover", 1)
        self.nodes_swap = self.input.param("nodes_swap", 0)
        self.recovery_type = self.input.param("recovery_type", "delta")
        self.rebalance_moves_per_node = self.input.param("rebalance_moves_per_node", 2)
        self.sleep_between_collections_crud = self.input.param("sleep_between_collections_crud", None)
        self.cluster_util.set_rebalance_moves_per_nodes(
            self.cluster.master,
            rebalanceMovesPerNode=self.rebalance_moves_per_node)
        self.change_ephemeral_purge_age_and_interval = self.input.param("change_ephemeral_purge_age_and_interval",
                                                                        True)
        if self.change_ephemeral_purge_age_and_interval:
            self.set_ephemeral_purge_age_and_interval()
        self.data_load_flag = False  # When to start/stop drop/recreate
        self.data_loading_thread = None
        self.data_load_exception = None # Object variable to assign data load thread's exception
        self.skip_validations = self.input.param("skip_validations", True)
        self.N1qltxn = self.input.param("N1ql_txn", False)
        if self.N1qltxn:
            self.n1ql_server = self.cluster_util.get_nodes_from_services_map(
                cluster=self.cluster,
                service_type=CbServer.Services.N1QL,
                get_all_nodes=True)
            self.n1ql_helper = N1QLHelper(server=self.n1ql_server,
                                          use_rest=True,
                                          buckets = self.cluster.buckets,
                                          log=self.log,
                                          scan_consistency='REQUEST_PLUS',
                                          num_collection=3,
                                          num_buckets=1,
                                          num_savepoints=1,
                                          override_savepoint=False,
                                          num_stmt=10,
                                          load_spec=self.data_spec_name)
            self.bucket_col = self.n1ql_helper.get_collections()
            self.stmts = self.n1ql_helper.get_stmt(self.bucket_col)
            self.stmts = self.n1ql_helper.create_full_stmts(self.stmts)

    def tearDown(self):
        self.cluster_util.set_rebalance_moves_per_nodes(
            self.cluster.master, rebalanceMovesPerNode=4)
        if self.data_loading_thread:
            # stop data loading before tearDown if its still running
            self.data_load_flag = False
            self.data_loading_thread.join()
            self.data_loading_thread = None
        if self.N1qltxn:
            super(CollectionBase, self).tearDown()
        else:
            super(CollectionsDropRecreateRebalance, self).tearDown()

    def set_ephemeral_purge_age_and_interval(self, ephemeral_metadata_purge_age=0,
                                             ephemeral_metadata_purge_interval=1):
        """
        Enables diag eval on master node and updates the above two parameters
        for all ephemeral buckets on the cluster
        """
        shell = RemoteMachineShellConnection(self.cluster.master)
        shell.enable_diag_eval_on_non_local_hosts()
        shell.disconnect()
        ephemeral_buckets = [bucket for bucket in self.cluster.buckets if bucket.bucketType == "ephemeral"]
        for ephemeral_bucket in ephemeral_buckets:
            rest = RestConnection(self.cluster.master)
            status, content = rest.set_ephemeral_purge_age_and_interval(bucket=ephemeral_bucket.name,
                                                                        ephemeral_metadata_purge_age=ephemeral_metadata_purge_age,
                                                                        ephemeral_metadata_purge_interval=ephemeral_metadata_purge_interval)
            if not status:
                raise Exception(content)

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

    def wait_for_failover_or_assert(self, expected_failover_count, timeout=300):
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
            self.log.info(self.cluster_util.print_UI_logs(self.cluster.master))
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
        if not task.result:
            self.task.jython_task_manager.abort_all_tasks()
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
        if self.bucket_dedup_retention_seconds \
                or self.bucket_dedup_retention_bytes:
            spec["doc_crud"] = {
                MetaCrudParams.DocCrud.CONT_UPDATE_PERCENT_PER_COLLECTION:
                    (1, 100)
            }

        return spec

    def print_spec_details(self, spec, cycles, elapsed_time):
        table = TableView(self.log.info)
        table.set_headers(["Operation", "Value"])
        table.add_row(["Collections dropped and recreated", str(spec[MetaCrudParams.COLLECTIONS_TO_RECREATE])])
        table.add_row(["Scopes dropped and recreated", str(spec[MetaCrudParams.SCOPES_TO_RECREATE])])
        table.add_row(["Cycles of data load", str(cycles)])
        table.add_row(["Time Elapsed in secs", str(elapsed_time)])
        table.display("Data load details")

    def data_load(self):
        cycles = 0
        start_time = time.time()
        while self.data_load_flag:
            doc_loading_spec = self.spec_for_drop_recreate()
            try:
                _ = BucketUtils.perform_tasks_from_spec(self.cluster,
                                                        self.cluster.buckets,
                                                        doc_loading_spec)
            except Exception as e:
                self.data_load_exception = e
                raise
            cycles = cycles + 1
            # TODO : This sleep is intentionally added. See MB-47533
            # TODO : Needs to be reverted when MB-47810 is fixed
            if self.sleep_between_collections_crud:
                time.sleep(self.sleep_between_collections_crud)
        end_time = time.time()
        elapsed_time = end_time - start_time
        self.print_spec_details(self.spec_for_drop_recreate(), cycles, elapsed_time)

    def load_collections_with_rebalance(self, rebalance_operation):
        self.pick_nodes_for_rebalance()

        cont_load_task = None
        if self.N1qltxn:
            self.N1ql_load_task = self.task.async_n1qlTxn_query( self.stmts,
                 n1ql_helper=self.n1ql_helper,
                 commit=True,
                 scan_consistency="REQUEST_PLUS")
        else:
            cont_load_task = \
                CollectionBase.start_history_retention_data_load(self)
            self.data_load_flag = True
            self.data_loading_thread = threading.Thread(target=self.data_load)
            self.data_loading_thread.start()

        if rebalance_operation == "rebalance_in":
            operation = self.task.async_rebalance(self.cluster, self.add_nodes,
                                                  [],
                                                  retry_get_process_num=self.retry_get_process_num*3)
        elif rebalance_operation == "rebalance_out":
            operation = self.task.async_rebalance(self.cluster, [],
                                                  self.remove_nodes,
                                                  retry_get_process_num=self.retry_get_process_num*3)
        elif rebalance_operation == "swap_rebalance":
            for node in self.add_nodes:
                self.rest.add_node(self.cluster.master.rest_username, self.cluster.master.rest_password,
                                   node.ip, self.cluster.servers[self.nodes_init].port)
            operation = self.task.async_rebalance(self.cluster, [], self.remove_nodes,
                                                  check_vbucket_shuffling=False,
                                                  retry_get_process_num=self.retry_get_process_num*3)
        elif rebalance_operation == "rebalance_in_out":
            for node in self.add_nodes:
                self.rest.add_node(self.cluster.master.rest_username, self.cluster.master.rest_password,
                                   node.ip, self.cluster.servers[self.nodes_init].port)
            operation = self.task.async_rebalance(self.cluster, [], self.remove_nodes,
                                                  check_vbucket_shuffling=False,
                                                  retry_get_process_num=self.retry_get_process_num*3)

        self.wait_for_rebalance_to_complete(operation)
        self.data_load_flag = False
        if not self.N1qltxn:
            self.data_loading_thread.join()
        CollectionBase.wait_for_cont_doc_load_to_complete(self, cont_load_task)
        self.data_loading_thread = None
        if self.data_load_exception:
            self.log.error("Caught exception from data load thread")
            self.fail(self.data_load_exception)

    def load_collections_with_failover(self, rebalance_operation):
        self.pick_nodes_for_failover(rebalance_operation)
        cont_load_task = None
        if self.N1qltxn:
            self.N1ql_load_task = self.task.async_n1qlTxn_query(self.stmts,
                 n1ql_helper=self.n1ql_helper,
                 commit=True,
                 scan_consistency="REQUEST_PLUS")
        else:
            cont_load_task = \
                CollectionBase.start_history_retention_data_load(self)
            self.data_load_flag = True
            self.data_loading_thread = threading.Thread(target=self.data_load)
            self.data_loading_thread.start()

        graceful = True if "graceful" in rebalance_operation else False
        failover_count = 0
        self.log.info("failing over nodes {0}".format(self.failover_nodes))
        for failover_node in self.failover_nodes:
            _ = self.task.failover(self.known_nodes, failover_nodes=[failover_node],
                                                    graceful=graceful, wait_for_pending=120)
            failover_count = failover_count + 1
            self.wait_for_failover_or_assert(failover_count)

        if "recovery" in rebalance_operation:
            for failover_node in self.failover_nodes:
                self.rest.set_recovery_type(otpNode='ns_1@' + failover_node.ip,
                                            recoveryType=self.recovery_type)
            operation = self.task.async_rebalance(self.cluster, [], [],
                                                  retry_get_process_num=self.retry_get_process_num*3)
        else:
            operation = self.task.async_rebalance(self.cluster, [], self.failover_nodes,
                                                  retry_get_process_num=self.retry_get_process_num*3)

        self.wait_for_rebalance_to_complete(operation)
        self.sleep(60, "Wait after rebalance completes before stopping data load")
        self.data_load_flag = False
        if not self.N1qltxn:
            self.data_loading_thread.join()
        self.data_loading_thread = None
        CollectionBase.wait_for_cont_doc_load_to_complete(self, cont_load_task)
        if self.data_load_exception:
            self.log.error("Caught exception from data load thread")
            self.fail(self.data_load_exception)

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
