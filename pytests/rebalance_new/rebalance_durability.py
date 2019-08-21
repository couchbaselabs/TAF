from math import floor

from BucketLib.BucketOperations import BucketHelper
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from membase.helper.cluster_helper import ClusterOperationHelper
from rebalance_base import RebalanceBaseTest
from remote.remote_util import RemoteMachineShellConnection
from couchbase_helper.durability_helper import DurableExceptions


class RebalanceDurability(RebalanceBaseTest):
    def setUp(self):
        super(RebalanceDurability, self).setUp()
        self.items = self.num_items
        self.start_from = self.num_items
        self.add_items = self.num_items
        self.delete_from = self.num_items/2
        self.delete_items = 5000

    def tearDown(self):
        super(RebalanceDurability, self).tearDown()

    def __load_docs_in_all_buckets(self):
        """
        Common function to perform Create/Update/Delete operations
        """
        tasks = list()
        self.gen_create = self.get_doc_generator(self.start_from, self.start_from + self.add_items)
        self.gen_delete = self.get_doc_generator(self.delete_from, self.delete_from + self.delete_items)

        retry_exceptions = [
            DurableExceptions.RequestTimeoutException,
            DurableExceptions.RequestCanceledException,
            DurableExceptions.DurabilityAmbiguousException,
            ]

        # CRUDs while rebalance is running in parallel
        tasks_info = self.loadgen_docs(retry_exceptions=retry_exceptions)

        if self.doc_ops is not None:
            if "create" in self.doc_ops:
                self.start_from += self.items
            if "delete" in self.doc_ops:
                self.delete_from += self.items
        return tasks_info

    def test_replica_update_with_durability_without_adding_removing_nodes(self):
        servs_in = [self.cluster.servers[i + self.nodes_init]
                    for i in range(self.nodes_in)]
        tasks_info = self.__load_docs_in_all_buckets()
        rebalance = self.task.async_rebalance(
            self.cluster.servers[:self.nodes_init], servs_in, [])
        self.task.jython_task_manager.get_task_result(rebalance)
        self.bucket_util.verify_doc_op_task_exceptions(
            tasks_info, self.cluster)
        self.bucket_util.log_doc_ops_task_failures(tasks_info)
        for task, task_info in tasks_info.items():
            self.assertFalse(
                task_info["ops_failed"],
                "Doc ops failed for task: {}".format(task.thread_name))
#             self.assertTrue(len(task.fail) == 0, "CRUD failed during initial rebalance")

        # Override docs_ops to perform CREATE/UPDATE during all rebalance
        self.doc_ops = ["create", "update"]

        self.sleep(10, "Wait for cluster to be ready after rebalance")
        for replicas in [1, 2]:
            self.log.info("Updating the bucket replicas to {0}"
                          .format(replicas))
            tasks_info = self.__load_docs_in_all_buckets()
            self.bucket_util.update_all_bucket_replicas(replicas=replicas)
            rebalance_result = self.task.rebalance(
                self.cluster.servers[:self.nodes_init], [], [])
            self.assertTrue(rebalance_result)
            self.bucket_util.verify_doc_op_task_exceptions(
                tasks_info, self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)
            for task, task_info in tasks_info.items():
                self.assertFalse(
                    task_info["ops_failed"],
                    "Doc ops failed for task: {}".format(task.thread_name))
#                 self.assertTrue(len(task.fail) == 0, "CRUD failed during initial rebalance")

        for replicas in [1, 0]:
            self.log.info("Updating the bucket replicas to {0}"
                          .format(replicas))
            tasks = self.__load_docs_in_all_buckets()
            self.bucket_util.update_all_bucket_replicas(replicas=replicas)
            rebalance_result = self.task.rebalance(
                self.cluster.servers[:self.nodes_init], [], [])
            self.assertTrue(rebalance_result)
            self.bucket_util.verify_doc_op_task_exceptions(
                tasks_info, self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)
            for task, task_info in tasks_info.items():
                self.assertFalse(
                    task_info["ops_failed"],
                    "Doc ops failed for task: {}".format(task.thread_name))
#                 self.assertTrue(len(task.fail) == 0, "CRUD failed during initial rebalance")

        # Verify doc load count to match the overall CRUDs
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

    def test_replica_update_with_durability_with_adding_removing_nodes(self):
        tasks_info = self.__load_docs_in_all_buckets()
        self.bucket_util.verify_doc_op_task_exceptions(
            tasks_info, self.cluster)
        self.bucket_util.log_doc_ops_task_failures(tasks_info)
        for task, task_info in tasks_info.items():
            self.assertFalse(
                task_info["ops_failed"],
                "Doc ops failed for task: {}".format(task.thread_name))
            self.assertTrue(len(task.fail) == 0, "CRUD failed during initial rebalance")

        # Override docs_ops to perform CREATE/UPDATE during all rebalance
        self.doc_ops = ["create", "update"]

        self.sleep(10, "Wait for cluster to be ready after rebalance")
        self.log.info("Increasing the replicas and rebalancing in the nodes")
        for replicas in [1, 2]:
            # Start document CRUDs
            tasks_info = self.__load_docs_in_all_buckets()
            self.log.info("Increasing the bucket replicas to {0}"
                          .format(replicas))
            self.bucket_util.update_all_bucket_replicas(replicas=replicas)
            rebalance_result = self.task.rebalance(self.cluster.nodes_in_cluster,
                                            [self.cluster.servers[replicas]],
                                            [])
            self.assertTrue(rebalance_result)
            self.cluster.nodes_in_cluster.extend([self.cluster.servers[replicas]])
            # Wait for all doc_load tasks to complete and validate
            self.bucket_util.verify_doc_op_task_exceptions(
                tasks_info, self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)
            for task, task_info in tasks_info.items():
                self.assertFalse(
                    task_info["ops_failed"],
                    "Doc ops failed for task: {}".format(task.thread_name))

        self.log.info("Decreasing the replicas and rebalancing out the nodes")
        for replicas in [1, 0]:
            self.log.info("Reducing the bucket replicas to {0}"
                          .format((replicas)))
            # Start document CRUDs
            tasks_info = self.__load_docs_in_all_buckets()
            self.bucket_util.update_all_bucket_replicas(replicas=replicas)
            rebalance_result = self.task.rebalance(
                self.cluster.servers[:self.nodes_init], [],
                [self.cluster.servers[replicas+1]])
            self.assertTrue(rebalance_result)
            # Wait for all doc_load tasks to complete and validate
            self.bucket_util.verify_doc_op_task_exceptions(
                tasks_info, self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)
            for task, task_info in tasks_info.items():
                self.assertFalse(
                    task_info["ops_failed"],
                    "Doc ops failed for task: {}".format(task.thread_name))

        # Verify doc load count to match the overall CRUDs
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

    def test_rebalance_out_durabilitybreaks_rebalance_in(self):
        self.assertTrue(self.num_replicas >= 1,
                        "Need at-least one replica to run this test")

        durability_will_fail = False
        def_bucket = self.bucket_util.buckets[0]

        nodes_in_cluster = self.nodes_init
        nodes_required_for_durability = int(floor((self.num_replicas+1)/2)+1)

        gen_update = doc_generator(self.key, 0, self.num_items)
        num_of_docs_to_insert = 1000

        # Rebalance all nodes expect master one-by-one
        for _ in range(self.nodes_init-1):
            rebalance_result = self.task.rebalance(
                self.cluster.servers[:nodes_in_cluster], [],
                [self.cluster.servers[nodes_in_cluster-1]])
            self.assertTrue(rebalance_result)
            nodes_in_cluster -= 1

            if nodes_in_cluster < nodes_required_for_durability and self.durability_level and self.durability_level.lower() != "none":
                durability_will_fail = True

            tasks = list()
            # Perform CRUD operations after rebalance_out to verify the
            # durability outcome
            gen_create = doc_generator(self.key, self.num_items,
                                       self.num_items+num_of_docs_to_insert)
            tasks.append(self.task.async_load_gen_docs(
                self.cluster, def_bucket, gen_create, "create", exp=0,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level, timeout_secs=self.sdk_timeout,
                batch_size=20, process_concurrency=4))

            tasks.append(self.task.async_load_gen_docs(
                self.cluster, def_bucket, gen_update, "update", 0,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level, timeout_secs=self.sdk_timeout,
                batch_size=20, process_concurrency=4))

            # Wait for all CRUD tasks to complete
            for task in tasks:
                self.task.jython_task_manager.get_task_result(task)

            if durability_will_fail:
                self.log.info("Durability broken with cluster {0}, replica {1}"
                              .format(self.cluster.servers[:nodes_in_cluster],
                                      self.num_replicas))
                for task in tasks:
                    if task.op_type == "create":
                        self.assertTrue(len(task.fail) == num_of_docs_to_insert,
                                        "Items crud succeeded although majority condition failed.")
                    elif task.op_type == "update":
                        self.assertTrue(len(task.fail) == (gen_update.end - gen_update.start),
                                        "Items crud succeeded although majority condition failed.")
                break
            else:
                # If durability works fine, re-calculate the self.num_items
                self.num_items += num_of_docs_to_insert
                # Reset the tasks list to reuse
                tasks = list()
                # Create tasks for doc verification
                tasks.append(self.task.async_validate_docs(
                    self.cluster, def_bucket, gen_update, "update", 0,
                    batch_size=10, process_concurrency=4))
                tasks.append(self.task.async_validate_docs(
                    self.cluster, def_bucket, gen_create, "create", 0,
                    batch_size=10, process_concurrency=4))

                # Wait for all verification tasks to complete
                for task in tasks:
                    self.task.jython_task_manager.get_task_result(task)

        # Rebalance-in single node back into the cluster
        rebalance_result = self.task.rebalance(
            self.cluster.servers[:nodes_in_cluster],
            self.cluster.servers[nodes_in_cluster:nodes_in_cluster+1], [])
        # Wait for rebalance-in task to complete
        self.assertTrue(rebalance_result)

        # Reset the tasks list
        tasks = list()
        # Perform CRUD operations after rebalance_in to verify the
        # durability is working as expected again
        gen_create = doc_generator(self.key, self.num_items,
                                   self.num_items+num_of_docs_to_insert)
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, def_bucket, gen_create, "create", exp=0,
            persist_to=self.persist_to, replicate_to=self.replicate_to,
            durability=self.durability_level, timeout_secs=self.sdk_timeout,
            batch_size=20))

        tasks.append(self.task.async_load_gen_docs(
            self.cluster, def_bucket, gen_update, "update", 0,
            persist_to=self.persist_to, replicate_to=self.replicate_to,
            durability=self.durability_level, timeout_secs=self.sdk_timeout,
            batch_size=20))

        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)
        # Wait for all CRUD tasks to complete
        for task in tasks:
            if task.op_type == "create":
                self.assertTrue(len(task.fail) == 0,
                                "Items crud succeeded although majority condition failed.")
            elif task.op_type == "update":
                self.assertTrue(len(task.fail) == 0,
                                "Items crud succeeded although majority condition failed.")
        # After rebalance-in durability should work fine
        # So recalculating the self.num_items to match gen_create loader
        self.num_items += num_of_docs_to_insert

        # Reset the tasks list
        tasks = list()
        # Create tasks for doc verification
        tasks.append(self.task.async_validate_docs(
            self.cluster, def_bucket, gen_update, "update", 0,
            batch_size=10, process_concurrency=4))
        tasks.append(self.task.async_validate_docs(
            self.cluster, def_bucket, gen_create, "create", 0,
            batch_size=10, process_concurrency=4))

        # Wait for all doc verification tasks to complete
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

        # Verify doc load count to match the overall CRUDs
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

    def test_multiple_scenarios(self):
        """
        Test multiple rebalance scenarios in single test with CRUDs in parallel

        1. Rebalance_out orchestrator node
        2. Rebalance_in nodes as given in nodes_in param
        3. Update replica and do rebalance
        4. Rebalance_out nodes as given in nodes_out param
        5. Do Plain CRUDs at the end of all this to verify the cluster status
        """
        # Local function to wait for all crud task to complete
        def wait_for_crud_task_and_verify_for_no_errors(tasks):
            for task in tasks:
                self.task.jython_task_manager.get_task_result(task)
                self.assertTrue(len(task.fail.keys()) == 0,
                                msg="Unexpected durability failures: {0}"
                                .format(task.fail))

        self.assertTrue(self.replica_to_update is not None)
        def_bucket = self.bucket_util.buckets[0]
        servers_in = [self.cluster.servers[self.nodes_init + i]
                      for i in range(self.nodes_in)]
        servers_out = [self.cluster.servers[self.nodes_init - i - 1]
                       for i in range(self.nodes_out)]

        # Start CRUD operations
        crud_tasks = self.__load_docs_in_all_buckets()

        # Rebalance_out the orchestrator node
        rebalance_result = self.task.rebalance(
            self.cluster.servers[:self.nodes_init],
            [], [self.cluster.servers[0]])
        self.assertTrue(rebalance_result, "Rebalance out orchestrator node failed")
        # Wait for all CRUD tasks to complete and verify no failures are seen
        wait_for_crud_task_and_verify_for_no_errors(crud_tasks)

        self.cluster.master = self.servers[1]
        self.cluster.nodes_in_cluster = self.servers[1:self.nodes_init]
        # Start CRUD operations
        crud_tasks = self.__load_docs_in_all_buckets()
        # Rebalance_in multiple cluster nodes
        self.add_remove_servers_and_rebalance(servers_in, [])
        wait_for_crud_task_and_verify_for_no_errors(crud_tasks)

        # Start CRUD operations
        crud_tasks = self.__load_docs_in_all_buckets()
        # Update bucket replica value
        bucket_helper = BucketHelper(self.cluster.servers[1])
        bucket_helper.change_bucket_props(def_bucket.name,
                                          replicaNumber=self.replica_to_update)
        def_bucket.replicaNumber = self.replica_to_update
        # Start and wait till rebalance is complete
        rebalance = self.task.async_rebalance(self.cluster.nodes_in_cluster, [], [])
        self.task.jython_task_manager.get_task_result(rebalance)
        wait_for_crud_task_and_verify_for_no_errors(crud_tasks)

        # Start CRUD operations
        crud_tasks = self.__load_docs_in_all_buckets()
        # Rebalance_out multiple cluster nodes
        self.add_remove_servers_and_rebalance([], servers_out)
        wait_for_crud_task_and_verify_for_no_errors(crud_tasks)

        # Start CRUD operations
        crud_tasks = self.__load_docs_in_all_buckets()
        wait_for_crud_task_and_verify_for_no_errors(crud_tasks)

        # Doc count verification
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

    def test_auto_retry_of_failed_rebalance_with_rebalance_test_conditions(self):
        sleep_time = self.input.param("sleep_time", 15)
        afterTimePeriod = self.input.param("afterTimePeriod", 40)
        rebalance_operation = self.input.param("rebalance_operation")
        self.change_retry_rebalance_settings(enabled=True, afterTimePeriod=afterTimePeriod,
                                             maxAttempts=1)
        self.rest.update_autofailover_settings(False, 120)
        test_failure_condition = self.input.param("test_failure_condition")
        # induce the failure before the rebalance starts
        self.induce_rebalance_test_condition(test_failure_condition)
        self.gen_update = self.get_doc_generator(0, self.num_items)
        self.doc_ops = "update"
        self.sleep(sleep_time)
        try:
            # start update of all keys
            task_update = self.loadgen_docs()
            rebalance = self.start_rebalance(rebalance_operation)
            self.task.jython_task_manager.get_task_result(rebalance)
            if rebalance.result:
                self.fail("Rebalance succeeded when it should have failed")
            # Ensure there are no failures
            self.bucket_util.verify_doc_op_task_exceptions(task_update, self.cluster)
            self.bucket_util.log_doc_ops_task_failures(task_update)
            # Delete the rebalance test condition so that we recover from the error
            self.delete_rebalance_test_condition(test_failure_condition)
            self.sleep(sleep_time)
            # start update of all keys
            task_update = self.loadgen_docs()
            self.check_retry_rebalance_succeeded()
            # Ensure there are no failures
            self.bucket_util.verify_doc_op_task_exceptions(task_update, self.cluster)
            self.bucket_util.log_doc_ops_task_failures(task_update)
        finally:
            self.delete_rebalance_test_condition(test_failure_condition)
        # Verify doc load count to match the overall CRUDs
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)