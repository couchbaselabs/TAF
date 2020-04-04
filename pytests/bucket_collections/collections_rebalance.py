from couchbase_helper.documentgenerator import doc_generator
from bucket_collections.collections_base import CollectionBase
from membase.api.rest_client import RestConnection, RestHelper
from BucketLib.BucketOperations import BucketHelper


class CollectionsRebalance(CollectionBase):
    def setUp(self):
        super(CollectionsRebalance, self).setUp()
        self.load_gen = doc_generator(self.key, 0, self.num_items)
        self.bucket = self.bucket_util.buckets[0]
        self.rest = RestConnection(self.cluster.master)
        self.data_load_stage = self.input.param("data_load_stage", "before")
        self.data_load_type = self.input.param("data_load_type", "sync")
        self.nodes_swap = self.input.param("nodes_swap", 1)
        self.nodes_failover = self.input.param("nodes_failover", 1)
        self.step_count = self.input.param("step_count", -1)
        self.replicas_for_failover = self.input.param("replicas_for_failover", 3)
        self.recovery_type = self.input.param("recovery_type", "full")

    def tearDown(self):
        super(CollectionsRebalance, self).tearDown()

    def rebalance_operation(self, rebalance_operation, known_nodes=None, add_nodes=None, remove_nodes=None,
                            failover_nodes=None, wait_for_pending=120):
        self.log.info("Starting rebalance operation of type : {0}".format(rebalance_operation))
        step_count = self.step_count
        if rebalance_operation == "rebalance_out":
            if step_count == -1:
                # all at once
                operation = self.task.async_rebalance(known_nodes, [], remove_nodes)
            else:
                # list of lists each of length step_count
                remove_list = []
                for i in range(0, len(remove_nodes), step_count):
                    if i + step_count >= len(remove_nodes):
                        remove_list.append(remove_nodes[i:])
                    else:
                        remove_list.append(remove_nodes[i:i + step_count])
                iter_count = 0
                # start each intermediate rebalance and wait for it to finish before
                # starting new one
                for new_remove_nodes in remove_list:
                    operation = self.task.async_rebalance(known_nodes, [], new_remove_nodes)
                    known_nodes = [node for node in known_nodes if node not in new_remove_nodes]
                    iter_count = iter_count + 1
                    # if this is last intermediate rebalance, don't wait
                    if iter_count == len(remove_list):
                        continue
                    self.wait_for_rebalance_to_complete(operation)
        elif rebalance_operation == "rebalance_in":
            if step_count == -1:
                # all at once
                operation = self.task.async_rebalance(known_nodes, add_nodes, [])
            else:
                # list of lists each of length step_count
                add_list = []
                for i in range(0, len(add_nodes), step_count):
                    if i + step_count >= len(add_nodes):
                        add_list.append(add_nodes[i:])
                    else:
                        add_list.append(add_nodes[i:i + step_count])
                iter_count = 0
                # start each intermediate rebalance and wait for it to finish before
                # starting new one
                for new_add_nodes in add_list:
                    operation = self.task.async_rebalance(known_nodes, new_add_nodes, [])
                    known_nodes.append(new_add_nodes)
                    iter_count = iter_count + 1
                    # if this is last intermediate rebalance, don't wait
                    if iter_count == len(add_list):
                        continue
                    self.wait_for_rebalance_to_complete(operation)
        elif rebalance_operation == "swap_rebalance":
            if(step_count == -1):
                for node in add_nodes:
                    self.rest.add_node(self.cluster.master.rest_username, self.cluster.master.rest_password,
                                       node.ip, self.cluster.servers[self.nodes_init].port)
                operation = self.task.async_rebalance(self.cluster.servers[:self.nodes_init], [], remove_nodes)
            else:
                #list of lists each of length step_count
                add_list = []
                remove_list = []
                for i in range(0, len(add_nodes), step_count):
                    if i + step_count >= len(add_nodes):
                        add_list.append(add_nodes[i:])
                        remove_list.append(remove_nodes[i:])
                    else:
                        add_list.append(add_nodes[i:i + step_count])
                        remove_list.append(remove_nodes[i:i + step_count])
                iter_count = 0
                # start each intermediate rebalance and wait for it to finish before
                # starting new one
                for new_add_nodes, new_remove_nodes in zip(add_list, remove_list):
                    operation = self.task.async_rebalance(known_nodes, new_add_nodes, new_remove_nodes)
                    known_nodes = [node for node in known_nodes if node not in new_remove_nodes]
                    known_nodes.extend(new_add_nodes)
                    iter_count = iter_count + 1
                    # if this is last intermediate rebalance, don't wait
                    if iter_count == len(add_list):
                        continue
                    self.wait_for_rebalance_to_complete(operation)
        elif rebalance_operation == "rebalance_in_out":
            for node in add_nodes:
                self.rest.add_node(self.cluster.master.rest_username, self.cluster.master.rest_password,
                                   node.ip, self.cluster.servers[self.nodes_init].port)
            operation = self.task.async_rebalance(self.cluster.servers[:self.nodes_init], [], remove_nodes)
        elif rebalance_operation == "graceful_failover_rebalance_out":
            self.log.info("Updating all the bucket replicas to {0}" .format(self.replicas_for_failover))
            for i in range(len(self.bucket_util.buckets)):
                bucket_helper = BucketHelper(self.cluster.master)
                bucket_helper.change_bucket_props(
                    self.bucket_util.buckets[i], replicaNumber=self.replicas_for_failover)
            task = self.task.async_rebalance(known_nodes, [], [])
            self.task.jython_task_manager.get_task_result(task)
            self.log.info("Bucket stats before failover")
            self.bucket_util.print_bucket_stats()
            for failover_node in failover_nodes:
                failover_operation = self.task.async_failover(known_nodes, failover_nodes=[failover_node],
                                                     graceful=True, wait_for_pending=wait_for_pending)
                self.task.jython_task_manager.get_task_result(failover_operation)
            operation = self.task.async_rebalance(known_nodes, [], failover_nodes)
        elif rebalance_operation == "hard_failover_rebalance_out":
            self.log.info("Updating all the bucket replicas to {0}".format(self.replicas_for_failover))
            for i in range(len(self.bucket_util.buckets)):
                bucket_helper = BucketHelper(self.cluster.master)
                bucket_helper.change_bucket_props(
                    self.bucket_util.buckets[i], replicaNumber=self.replicas_for_failover)
            task = self.task.async_rebalance(known_nodes, [], [])
            self.task.jython_task_manager.get_task_result(task)
            self.log.info("Bucket stats before failover")
            for failover_node in failover_nodes:
                failover_operation = self.task.async_failover(known_nodes, failover_nodes=[failover_node],
                                                     graceful=False, wait_for_pending=wait_for_pending)
                self.task.jython_task_manager.get_task_result(failover_operation)
            operation = self.task.async_rebalance(known_nodes, [], failover_nodes)
        elif rebalance_operation == "graceful_failover_recovery":
            self.log.info("Updating all the bucket replicas to {0}" .format(self.replicas_for_failover))
            for i in range(len(self.bucket_util.buckets)):
                bucket_helper = BucketHelper(self.cluster.master)
                bucket_helper.change_bucket_props(
                    self.bucket_util.buckets[i], replicaNumber=self.replicas_for_failover)
            task = self.task.async_rebalance(known_nodes, [], [])
            self.task.jython_task_manager.get_task_result(task)
            self.log.info("Bucket stats before failover")
            self.bucket_util.print_bucket_stats()
            for failover_node in failover_nodes:
                failover_operation = self.task.async_failover(known_nodes, failover_nodes=[failover_node],
                                                     graceful=True, wait_for_pending=wait_for_pending)
                self.task.jython_task_manager.get_task_result(failover_operation)
            # Mark the failover nodes for recovery
            for failover_node in failover_nodes:
                self.rest.set_recovery_type(otpNode='ns_1@' + failover_node.ip, recoveryType=self.recovery_type)
            # Rebalance all the nodes
            operation = self.task.async_rebalance(known_nodes, [], [])
        elif rebalance_operation == "hard_failover_recovery":
            self.log.info("Updating all the bucket replicas to {0}" .format(self.replicas_for_failover))
            for i in range(len(self.bucket_util.buckets)):
                bucket_helper = BucketHelper(self.cluster.master)
                bucket_helper.change_bucket_props(
                    self.bucket_util.buckets[i], replicaNumber=self.replicas_for_failover)
            task = self.task.async_rebalance(known_nodes, [], [])
            self.task.jython_task_manager.get_task_result(task)
            self.log.info("Bucket stats before failover")
            self.bucket_util.print_bucket_stats()
            for failover_node in failover_nodes:
                failover_operation = self.task.async_failover(known_nodes, failover_nodes=[failover_node],
                                                     graceful=False, wait_for_pending=wait_for_pending)
                self.task.jython_task_manager.get_task_result(failover_operation)
            # Mark the failover nodes for recovery
            for failover_node in failover_nodes:
                self.rest.set_recovery_type(otpNode='ns_1@' + failover_node.ip, recoveryType=self.recovery_type)
            # Rebalance all the nodes
            operation = self.task.async_rebalance(known_nodes, [], [])
        else:
            self.fail("rebalance_operation is not defined")
        return operation

    def subsequent_data_load(self, async_load=False):
        doc_loading_spec = self.bucket_util.get_crud_template_from_package("volume_test_load")
        tasks = self.bucket_util.run_scenario_from_spec(self.task,
                                                self.cluster,
                                                self.bucket_util.buckets,
                                                doc_loading_spec,
                                                mutation_num=0,
                                                async_load=async_load)
        return tasks

    def async_data_load(self):
        tasks = self.subsequent_data_load(async_load=True)
        return tasks

    def sync_data_load(self):
        self.subsequent_data_load()

    def wait_for_async_data_load_to_complete(self, tasks):
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

    def wait_for_rebalance_to_complete(self, task, wait_step=120):
        self.task.jython_task_manager.get_task_result(task)
        reached = RestHelper(self.rest).rebalance_reached(wait_step=wait_step)
        self.assertTrue(reached, "Rebalance failed, stuck or did not complete")
        self.assertTrue(task.result, "Rebalance Failed")

    def data_validation_collection(self):
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_docs_per_collections_all_buckets()

    def load_collections_with_rebalance(self, rebalance_operation):
        tasks = None
        rebalance = None
        self.log.info("Doing collection data load {0} {1}".format(self.data_load_stage, rebalance_operation))
        if self.data_load_stage == "before":
            if self.data_load_type == "async":
                tasks = self.async_data_load()
            else:
                self.sync_data_load()
        if rebalance_operation =="rebalance_in":
            rebalance = self.rebalance_operation(rebalance_operation="rebalance_in",
                                                 known_nodes=self.cluster.servers[:self.nodes_init],
                                                 add_nodes=self.cluster.servers[
                                                           self.nodes_init:self.nodes_init + self.nodes_in],
                                                 )
        elif rebalance_operation =="rebalance_out":
            rebalance = self.rebalance_operation(rebalance_operation="rebalance_out",
                                                 known_nodes=self.cluster.servers[:self.nodes_init],
                                                 remove_nodes=self.cluster.servers[:self.nodes_init][-self.nodes_out:],
                                                 )
        elif rebalance_operation =="swap_rebalance":
            rebalance = self.rebalance_operation(rebalance_operation="swap_rebalance",
                                                 known_nodes=self.cluster.servers[:self.nodes_init],
                                                 add_nodes=self.cluster.servers[
                                                           self.nodes_init:self.nodes_init + self.nodes_swap],
                                                 remove_nodes=self.cluster.servers[:self.nodes_init][-self.nodes_swap:],
                                                 )
        elif rebalance_operation =="rebalance_in_out":
            rebalance = self.rebalance_operation(rebalance_operation="rebalance_in_out",
                                                 known_nodes=self.cluster.servers[:self.nodes_init],
                                                 add_nodes=self.cluster.servers[
                                                           self.nodes_init:self.nodes_init + self.nodes_in],
                                                 remove_nodes=self.cluster.servers[:self.nodes_init][-self.nodes_out:],
                                                 )
        elif rebalance_operation == "graceful_failover_rebalance_out":
            rebalance = self.rebalance_operation(rebalance_operation="graceful_failover_rebalance_out",
                                                 known_nodes=self.cluster.servers[:self.nodes_init],
                                                 failover_nodes=self.cluster.servers[:self.nodes_init]
                                                 [-self.nodes_failover:],
                                                 )
        elif rebalance_operation == "hard_failover_rebalance_out":
            rebalance = self.rebalance_operation(rebalance_operation="hard_failover_rebalance_out",
                                                 known_nodes=self.cluster.servers[:self.nodes_init],
                                                 failover_nodes=self.cluster.servers[:self.nodes_init]
                                                 [-self.nodes_failover:],
                                                 )
        elif rebalance_operation == "graceful_failover_recovery":
            rebalance = self.rebalance_operation(rebalance_operation="graceful_failover_recovery",
                                                 known_nodes=self.cluster.servers[:self.nodes_init],
                                                 failover_nodes=self.cluster.servers[:self.nodes_init]
                                                 [-self.nodes_failover:],
                                                 )
        elif rebalance_operation == "hard_failover_recovery":
            rebalance = self.rebalance_operation(rebalance_operation="hard_failover_recovery",
                                                 known_nodes=self.cluster.servers[:self.nodes_init],
                                                 failover_nodes=self.cluster.servers[:self.nodes_init]
                                                 [-self.nodes_failover:],
                                                 )
        if self.data_load_stage == "during":
            if self.data_load_type == "async":
                tasks = self.async_data_load()
            else:
                self.sync_data_load()
        if self.data_load_stage == "before":
            if self.data_load_type == "async":
                self.wait_for_async_data_load_to_complete(tasks)
            self.data_validation_collection()
        self.wait_for_rebalance_to_complete(rebalance)
        if self.data_load_stage == "during":
            if self.data_load_type == "async":
                self.wait_for_async_data_load_to_complete(tasks)
            self.data_validation_collection()
        if self.data_load_stage == "after":
            self.sync_data_load()
            self.data_validation_collection()

    def test_data_load_collections_with_rebalance_in(self):
        self.load_collections_with_rebalance(rebalance_operation="rebalance_in")

    def test_data_load_collections_with_rebalance_out(self):
        self.load_collections_with_rebalance(rebalance_operation="rebalance_out")

    def test_data_load_collections_with_swap_rebalance(self):
        self.load_collections_with_rebalance(rebalance_operation="swap_rebalance")

    def test_data_load_collections_with_rebalance_in_out(self):
        self.load_collections_with_rebalance(rebalance_operation="rebalance_in_out")

    def test_data_load_collections_with_graceful_failover_rebalance_out(self):
        self.load_collections_with_rebalance(rebalance_operation="graceful_failover_rebalance_out")

    def test_data_load_collections_with_hard_failover_rebalance_out(self):
        self.load_collections_with_rebalance(rebalance_operation="hard_failover_rebalance_out")

    def test_data_load_collections_with_graceful_failover_recovery(self):
        self.load_collections_with_rebalance(rebalance_operation="graceful_failover_recovery")

    def test_data_load_collections_with_hard_failover_recovery(self):
        self.load_collections_with_rebalance(rebalance_operation="hard_failover_recovery")
