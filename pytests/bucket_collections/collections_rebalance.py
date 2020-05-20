import time
from collections_helper.collections_spec_constants import MetaCrudParams
from couchbase_helper.documentgenerator import doc_generator
from bucket_collections.collections_base import CollectionBase
from membase.api.rest_client import RestConnection, RestHelper
from remote.remote_util import RemoteMachineShellConnection


class CollectionsRebalance(CollectionBase):
    def setUp(self):
        super(CollectionsRebalance, self).setUp()
        self.bucket_util._expiry_pager()
        self.load_gen = doc_generator(self.key, 0, self.num_items)
        self.bucket = self.bucket_util.buckets[0]
        self.rest = RestConnection(self.cluster.master)
        self.data_load_spec = self.input.param("data_load_spec", "volume_test_load")
        self.data_load_stage = self.input.param("data_load_stage", "before")
        self.data_load_type = self.input.param("data_load_type", "async")
        self.nodes_swap = self.input.param("nodes_swap", 1)
        self.nodes_failover = self.input.param("nodes_failover", 1)
        self.failover_ops = ["graceful_failover_rebalance_out", "hard_failover_rebalance_out",
                             "graceful_failover_recovery", "hard_failover_recovery"]
        self.step_count = self.input.param("step_count", -1)
        self.recovery_type = self.input.param("recovery_type", "full")
        self.compaction = self.input.param("compaction", False)
        self.warmup = self.input.param("warmup", False)
        self.update_replica = self.input.param("update_replica", False)  # for replica + rebalance tests
        self.num_replicas = self.input.param("num_replicas", 1)  # for replica + rebalance tests, forced hard failover
        self.forced_hard_failover = self.input.param("forced_hard_failover", False) # for forced hard failover tests
        self.change_ram_quota_cluster = self.input.param("change_ram_quota_cluster", False) # To change during rebalance
        self.skip_validations = self.input.param("skip_validations", True)
        if self.compaction:
            self.compaction_tasks = list()

    def tearDown(self):
        super(CollectionsRebalance, self).tearDown()

    def compact_all_buckets(self):
        self.sleep(10, "wait for rebalance to start")
        self.log.info("Starting compaction for each bucket")
        for bucket in self.bucket_util.buckets:
            self.compaction_tasks.append(self.task.async_compact_bucket(
                self.cluster.master, bucket))

    def warmup_node(self, node):
        self.log.info("Warmuping up node...")
        shell = RemoteMachineShellConnection(node)
        shell.stop_couchbase()
        self.sleep(30)
        shell.start_couchbase()
        shell.disconnect()
        self.log.info("Done warming up...")

    def set_ram_quota_cluster(self):
        self.sleep(45, "Wait for rebalance have some progress")
        self.log.info("Changing cluster RAM size")
        status = self.rest.init_cluster_memoryQuota(self.cluster.master.rest_username,
                                                    self.cluster.master.rest_password,
                                                    memoryQuota=2500)
        self.assertTrue(status, "RAM quota wasn't changed")

    def data_load_after_failover(self):
        self.log.info("Starting a sync data load after failover")
        self.subsequent_data_load()  # sync data load
        # Until we recover/rebalance-out, we can't call - self.bucket_util.validate_docs_per_collections_all_buckets()
        self.bucket_util._wait_for_stats_all_buckets()

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

    def forced_failover_operation(self, known_nodes=None, failover_nodes=None, wait_for_pending=120):
        self.log.info("Updating all the bucket replicas to {0}".format(self.num_replicas))
        self.bucket_util.update_all_bucket_replicas(self.num_replicas)
        failover_count = 0
        for failover_node in failover_nodes:
            failover_operation = self.task.async_failover(known_nodes, failover_nodes=[failover_node],
                                                          graceful=False, wait_for_pending=wait_for_pending)
            failover_count = failover_count + 1
            self.wait_for_failover_or_assert(failover_count)
        operation = self.task.async_rebalance(known_nodes, [], failover_nodes)
        self.data_load_after_failover()
        return operation

    def rebalance_operation(self, rebalance_operation, known_nodes=None, add_nodes=None, remove_nodes=None,
                            failover_nodes=None, wait_for_pending=120):
        self.log.info("Starting rebalance operation of type : {0}".format(rebalance_operation))
        step_count = self.step_count
        if rebalance_operation == "rebalance_out":
            if step_count == -1:
                if self.warmup:
                    node = known_nodes[-1]
                    self.warmup_node(node)
                    operation = self.task.async_rebalance(known_nodes, [], remove_nodes)
                    self.task.jython_task_manager.get_task_result(operation)
                    if not operation.result:
                        self.log.info("rebalance was failed as expected")
                        for bucket in self.bucket_util.buckets:
                            self.assertTrue(self.bucket_util._wait_warmup_completed(
                                            [node], bucket))
                        self.log.info("second attempt to rebalance")
                        operation = self.task.async_rebalance(known_nodes, [], remove_nodes)
                        self.wait_for_rebalance_to_complete(operation)
                    self.sleep(60)
                else:
                    if self.update_replica:
                        self.log.info("Updating all the bucket replicas to {0}".format(self.num_replicas))
                        self.bucket_util.update_all_bucket_replicas(self.num_replicas)
                        self.bucket_util.print_bucket_stats()
                    # all at once
                    operation = self.task.async_rebalance(known_nodes, [], remove_nodes)
                    if self.compaction:
                        self.compact_all_buckets()
                    if self.change_ram_quota_cluster:
                        self.set_ram_quota_cluster()
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
                if self.warmup:
                    node = known_nodes[-1]
                    self.warmup_node(node)
                    operation = self.task.async_rebalance(known_nodes, add_nodes, [])
                    self.task.jython_task_manager.get_task_result(operation)
                    if not operation.result:
                        self.log.info("rebalance was failed as expected")
                        for bucket in self.bucket_util.buckets:
                            self.assertTrue(self.bucket_util._wait_warmup_completed(
                                            [node], bucket))
                        self.log.info("second attempt to rebalance")
                        operation = self.task.async_rebalance(known_nodes, add_nodes, [])
                        self.wait_for_rebalance_to_complete(operation)
                    self.sleep(60)
                else:
                    if self.update_replica:
                        self.log.info("Updating all the bucket replicas to {0}".format(self.num_replicas))
                        self.bucket_util.update_all_bucket_replicas(self.num_replicas)
                        self.bucket_util.print_bucket_stats()
                    # all at once
                    operation = self.task.async_rebalance(known_nodes, add_nodes, [])
                    if self.compaction:
                        self.compact_all_buckets()
                    if self.change_ram_quota_cluster:
                        self.set_ram_quota_cluster()
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
            if (step_count == -1):
                if self.warmup:
                    for node in add_nodes:
                        self.rest.add_node(self.cluster.master.rest_username, self.cluster.master.rest_password,
                                           node.ip, self.cluster.servers[self.nodes_init].port)
                    node = known_nodes[-1]
                    self.warmup_node(node)
                    operation = self.task.async_rebalance(self.cluster.servers[:self.nodes_init], [], remove_nodes)
                    self.task.jython_task_manager.get_task_result(operation)
                    if not operation.result:
                        self.log.info("rebalance was failed as expected")
                        for bucket in self.bucket_util.buckets:
                            self.assertTrue(self.bucket_util._wait_warmup_completed(
                                [node], bucket))
                        self.log.info("second attempt to rebalance")
                        operation = self.task.async_rebalance(self.cluster.servers[:self.nodes_init], [], remove_nodes)
                        self.wait_for_rebalance_to_complete(operation)
                    self.sleep(60)
                else:
                    if self.update_replica:
                        self.log.info("Updating all the bucket replicas to {0}".format(self.num_replicas))
                        self.bucket_util.update_all_bucket_replicas(self.num_replicas)
                        self.bucket_util.print_bucket_stats()
                    for node in add_nodes:
                        self.rest.add_node(self.cluster.master.rest_username, self.cluster.master.rest_password,
                                           node.ip, self.cluster.servers[self.nodes_init].port)
                    operation = self.task.async_rebalance(self.cluster.servers[:self.nodes_init], [], remove_nodes)
                    if self.compaction:
                        self.compact_all_buckets()
                    if self.change_ram_quota_cluster:
                        self.set_ram_quota_cluster()
            else:
                # list of lists each of length step_count
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
            if self.warmup:
                for node in add_nodes:
                    self.rest.add_node(self.cluster.master.rest_username, self.cluster.master.rest_password,
                                       node.ip, self.cluster.servers[self.nodes_init].port)
                node = known_nodes[-1]
                self.warmup_node(node)
                operation = self.task.async_rebalance(self.cluster.servers[:self.nodes_init], [], remove_nodes)
                self.task.jython_task_manager.get_task_result(operation)
                if not operation.result:
                    self.log.info("rebalance was failed as expected")
                    for bucket in self.bucket_util.buckets:
                        self.assertTrue(self.bucket_util._wait_warmup_completed(
                            [node], bucket))
                    self.log.info("second attempt to rebalance")
                    operation = self.task.async_rebalance(self.cluster.servers[:self.nodes_init], [], remove_nodes)
                    self.wait_for_rebalance_to_complete(operation)
                self.sleep(60)
            else:
                if self.update_replica:
                    self.log.info("Updating all the bucket replicas to {0}".format(self.num_replicas))
                    self.bucket_util.update_all_bucket_replicas(self.num_replicas)
                    self.bucket_util.print_bucket_stats()
                for node in add_nodes:
                    self.rest.add_node(self.cluster.master.rest_username, self.cluster.master.rest_password,
                                       node.ip, self.cluster.servers[self.nodes_init].port)
                operation = self.task.async_rebalance(self.cluster.servers[:self.nodes_init], [], remove_nodes)
                if self.compaction:
                    self.compact_all_buckets()
                if self.change_ram_quota_cluster:
                    self.set_ram_quota_cluster()
        elif rebalance_operation == "graceful_failover_rebalance_out":
            if step_count == -1:
                failover_count = 0
                for failover_node in failover_nodes:
                    failover_operation = self.task.async_failover(known_nodes, failover_nodes=[failover_node],
                                                                  graceful=True, wait_for_pending=wait_for_pending)
                    failover_count = failover_count + 1
                    self.wait_for_failover_or_assert(failover_count)
                if self.compaction:
                    self.compact_all_buckets()
                self.data_load_after_failover()
                operation = self.task.async_rebalance(known_nodes, [], failover_nodes)
                if self.change_ram_quota_cluster:
                    self.set_ram_quota_cluster()
            else:
                # list of lists each of length step_count
                failover_list = []
                for i in range(0, len(failover_nodes), step_count):
                    if i + step_count >= len(failover_nodes):
                        failover_list.append(failover_nodes[i:])
                    else:
                        failover_list.append(failover_nodes[i:i + step_count])
                # For each set of step_count number of failover nodes we failover and rebalance them out
                iter_count = 0
                for new_failover_nodes in failover_list:
                    failover_count = 0
                    for failover_node in new_failover_nodes:
                        failover_operation = self.task.async_failover(known_nodes, failover_nodes=[failover_node],
                                                                      graceful=True, wait_for_pending=wait_for_pending)
                        failover_count = failover_count + 1
                        self.wait_for_failover_or_assert(failover_count)
                    self.data_load_after_failover()
                    operation = self.task.async_rebalance(known_nodes, [], new_failover_nodes)
                    iter_count = iter_count + 1
                    known_nodes = [node for node in known_nodes if node not in new_failover_nodes]
                    if iter_count == len(failover_list):
                        continue
                    self.wait_for_rebalance_to_complete(operation)
        elif rebalance_operation == "hard_failover_rebalance_out":
            if step_count == -1:
                failover_count = 0
                for failover_node in failover_nodes:
                    failover_operation = self.task.async_failover(known_nodes, failover_nodes=[failover_node],
                                                                  graceful=False, wait_for_pending=wait_for_pending)
                    failover_count = failover_count + 1
                    self.wait_for_failover_or_assert(failover_count)
                if self.compaction:
                    self.compact_all_buckets()
                self.data_load_after_failover()
                operation = self.task.async_rebalance(known_nodes, [], failover_nodes)
                if self.change_ram_quota_cluster:
                    self.set_ram_quota_cluster()
            else:
                # list of lists each of length step_count
                failover_list = []
                for i in range(0, len(failover_nodes), step_count):
                    if i + step_count >= len(failover_nodes):
                        failover_list.append(failover_nodes[i:])
                    else:
                        failover_list.append(failover_nodes[i:i + step_count])
                # For each set of step_count number of failover nodes we failover and rebalance them out
                iter_count = 0
                for new_failover_nodes in failover_list:
                    failover_count = 0
                    for failover_node in new_failover_nodes:
                        failover_operation = self.task.async_failover(known_nodes, failover_nodes=[failover_node],
                                                                      graceful=False, wait_for_pending=wait_for_pending)
                        failover_count = failover_count + 1
                        self.wait_for_failover_or_assert(failover_count)
                    self.data_load_after_failover()
                    operation = self.task.async_rebalance(known_nodes, [], new_failover_nodes)
                    iter_count = iter_count + 1
                    known_nodes = [node for node in known_nodes if node not in new_failover_nodes]
                    if iter_count == len(failover_list):
                        continue
                    self.wait_for_rebalance_to_complete(operation)
        elif rebalance_operation == "graceful_failover_recovery":
            if (step_count == -1):
                failover_count = 0
                for failover_node in failover_nodes:
                    failover_operation = self.task.async_failover(known_nodes, failover_nodes=[failover_node],
                                                                  graceful=True, wait_for_pending=wait_for_pending)
                    failover_count = failover_count + 1
                    self.wait_for_failover_or_assert(failover_count)
                self.data_load_after_failover()
                # Mark the failover nodes for recovery
                for failover_node in failover_nodes:
                    self.rest.set_recovery_type(otpNode='ns_1@' + failover_node.ip, recoveryType=self.recovery_type)
                if self.compaction:
                    self.compact_all_buckets()
                # Rebalance all the nodes
                operation = self.task.async_rebalance(known_nodes, [], [])
                if self.change_ram_quota_cluster:
                    self.set_ram_quota_cluster()
            else:
                # list of lists each of length step_count
                failover_list = []
                for i in range(0, len(failover_nodes), step_count):
                    if i + step_count >= len(failover_nodes):
                        failover_list.append(failover_nodes[i:])
                    else:
                        failover_list.append(failover_nodes[i:i + step_count])
                # For each set of step_count number of failover nodes we failover and recover
                iter_count = 0
                for new_failover_nodes in failover_list:
                    failover_count = 0
                    for failover_node in new_failover_nodes:
                        failover_operation = self.task.async_failover(known_nodes, failover_nodes=[failover_node],
                                                                      graceful=True, wait_for_pending=wait_for_pending)

                        failover_count = failover_count + 1
                        self.wait_for_failover_or_assert(failover_count)
                    self.data_load_after_failover()
                    # Mark the failover nodes for recovery
                    for failover_node in new_failover_nodes:
                        self.rest.set_recovery_type(otpNode='ns_1@' + failover_node.ip,
                                                    recoveryType=self.recovery_type)
                    operation = self.task.async_rebalance(known_nodes, [], [])
                    iter_count = iter_count + 1
                    if iter_count == len(failover_list):
                        continue
                    self.wait_for_rebalance_to_complete(operation)
        elif rebalance_operation == "hard_failover_recovery":
            if (step_count == -1):
                failover_count = 0
                for failover_node in failover_nodes:
                    failover_operation = self.task.async_failover(known_nodes, failover_nodes=[failover_node],
                                                                  graceful=False, wait_for_pending=wait_for_pending)
                    failover_count = failover_count + 1
                    self.wait_for_failover_or_assert(failover_count)
                self.data_load_after_failover()
                # Mark the failover nodes for recovery
                for failover_node in failover_nodes:
                    self.rest.set_recovery_type(otpNode='ns_1@' + failover_node.ip, recoveryType=self.recovery_type)
                if self.compaction:
                    self.compact_all_buckets()
                # Rebalance all the nodes
                operation = self.task.async_rebalance(known_nodes, [], [])
                if self.change_ram_quota_cluster:
                    self.set_ram_quota_cluster()
            else:
                # list of lists each of length step_count
                failover_list = []
                for i in range(0, len(failover_nodes), step_count):
                    if i + step_count >= len(failover_nodes):
                        failover_list.append(failover_nodes[i:])
                    else:
                        failover_list.append(failover_nodes[i:i + step_count])
                # For each set of step_count number of failover nodes we failover and recover
                iter_count = 0
                for new_failover_nodes in failover_list:
                    failover_count = 0
                    for failover_node in new_failover_nodes:
                        failover_operation = self.task.async_failover(known_nodes, failover_nodes=[failover_node],
                                                                      graceful=False, wait_for_pending=wait_for_pending)

                        failover_count = failover_count + 1
                        self.wait_for_failover_or_assert(failover_count)
                    self.data_load_after_failover()
                    # Mark the failover nodes for recovery
                    for failover_node in new_failover_nodes:
                        self.rest.set_recovery_type(otpNode='ns_1@' + failover_node.ip,
                                                    recoveryType=self.recovery_type)
                    operation = self.task.async_rebalance(known_nodes, [], [])
                    iter_count = iter_count + 1
                    if iter_count == len(failover_list):
                        continue
                    self.wait_for_rebalance_to_complete(operation)
        else:
            self.fail("rebalance_operation is not defined")
        return operation

    def subsequent_data_load(self, async_load=False):
        doc_loading_spec = self.bucket_util.get_crud_template_from_package(self.data_load_spec)
        if self.forced_hard_failover and self.spec_name == "multi_bucket.buckets_for_rebalance_tests_more_collections":
            # create collections, else if other bucket_spec - then just "create" ops
            doc_loading_spec[MetaCrudParams.COLLECTIONS_TO_ADD_PER_BUCKET] = 20
        tasks = self.bucket_util.run_scenario_from_spec(self.task,
                                                        self.cluster,
                                                        self.bucket_util.buckets,
                                                        doc_loading_spec,
                                                        mutation_num=0,
                                                        async_load=async_load,
                                                        batch_size=self.batch_size)
        return tasks

    def async_data_load(self):
        tasks = self.subsequent_data_load(async_load=True)
        return tasks

    def sync_data_load(self):
        self.subsequent_data_load()

    def wait_for_async_data_load_to_complete(self, task):
        self.task.jython_task_manager.get_task_result(task)
        if not self.skip_validations:
            self.bucket_util.validate_doc_loading_results(task)
            if task.result is False:
                self.fail("Doc_loading failed")

    def wait_for_compaction_to_complete(self):
        # Strictly, we should be doing this
        # But this is not working properly.
        # for task in self.compaction_tasks:
        #     self.task_manager.get_task_result(task)
        #     self.assertTrue(task.result, "Compaction failed for bucket: %s" %
        #                     task.bucket.name)
        pass

    def wait_for_rebalance_to_complete(self, task, wait_step=120):
        self.task.jython_task_manager.get_task_result(task)
        reached = RestHelper(self.rest).rebalance_reached(wait_step=wait_step)
        self.assertTrue(reached, "Rebalance failed, stuck or did not complete")
        self.assertTrue(task.result, "Rebalance Failed")
        if self.compaction:
            self.wait_for_compaction_to_complete()

    def data_validation_collection(self):
        if not self.skip_validations:
            if self.data_load_spec == "ttl_load":
                self.bucket_util._expiry_pager()
                self.sleep(300, "wait for maxttl to finish")
                items = 0
                self.bucket_util._wait_for_stats_all_buckets()
                for bucket in self.bucket_util.buckets:
                    items = items + self.bucket_helper_obj.get_active_key_count(bucket)
                if items != 0:
                    self.fail("TTL + rebalance failed")
            elif self.forced_hard_failover:
                pass
            else:
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
        if rebalance_operation == "rebalance_in":
            rebalance = self.rebalance_operation(rebalance_operation="rebalance_in",
                                                 known_nodes=self.cluster.servers[:self.nodes_init],
                                                 add_nodes=self.cluster.servers[
                                                           self.nodes_init:self.nodes_init + self.nodes_in],
                                                 )
        elif rebalance_operation == "rebalance_out":
            rebalance = self.rebalance_operation(rebalance_operation="rebalance_out",
                                                 known_nodes=self.cluster.servers[:self.nodes_init],
                                                 remove_nodes=self.cluster.servers[:self.nodes_init][-self.nodes_out:],
                                                 )
        elif rebalance_operation == "swap_rebalance":
            rebalance = self.rebalance_operation(rebalance_operation="swap_rebalance",
                                                 known_nodes=self.cluster.servers[:self.nodes_init],
                                                 add_nodes=self.cluster.servers[
                                                           self.nodes_init:self.nodes_init + self.nodes_swap],
                                                 remove_nodes=self.cluster.servers[:self.nodes_init][-self.nodes_swap:],
                                                 )
        elif rebalance_operation == "rebalance_in_out":
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
        elif rebalance_operation == "forced_hard_failover_rebalance_out":
            rebalance = self.forced_failover_operation(known_nodes=self.cluster.servers[:self.nodes_init],
                                                       failover_nodes=self.cluster.servers[:self.nodes_init]
                                                       [-self.nodes_failover:],
                                                       )

        if self.data_load_stage == "during":
            if self.data_load_type == "async":
                tasks = self.async_data_load()
            else:
                self.sync_data_load()
        if not self.warmup:
            self.wait_for_rebalance_to_complete(rebalance)
        if self.data_load_stage == "during" or self.data_load_stage == "before":
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

    def test_data_load_collections_with_forced_hard_failover_rebalance_out(self):
        self.load_collections_with_rebalance(rebalance_operation="forced_hard_failover_rebalance_out")
