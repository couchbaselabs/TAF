from math import floor

from BucketLib.BucketOperations import BucketHelper
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from membase.helper.cluster_helper import ClusterOperationHelper
from rebalance_base import RebalanceBaseTest
from remote.remote_util import RemoteMachineShellConnection


class RebalanceDurability(RebalanceBaseTest):
    def setUp(self):
        super(RebalanceDurability, self).setUp()
        self.delete_doc_index = self.num_items/2
        self.items_deleted = 5000

    def tearDown(self):
        super(RebalanceDurability, self).tearDown()

    def __load_docs_in_all_buckets(self):
        """
        Common function to perform Create/Update/Delete operations
        """

        tasks = list()
        gen_create = self.get_doc_generator(self.num_items, self.num_items * 2)
        gen_delete = self.get_doc_generator(
            self.delete_doc_index, self.delete_doc_index+self.items_deleted)
        item_added = (self.num_items*2) - self.num_items

        for bucket in self.bucket_util.buckets:
            if self.doc_ops is not None:
                if "update" in self.doc_ops:
                    tasks.append(self.task.async_load_gen_docs(
                        self.cluster, bucket, self.gen_update, "update", 0,
                        batch_size=20, persist_to=self.persist_to,
                        replicate_to=self.replicate_to, pause_secs=5,
                        durability=self.durability_level,
                        timeout_secs=self.sdk_timeout,
                        retries=self.sdk_retries))
                if "create" in self.doc_ops:
                    tasks.append(self.task.async_load_gen_docs(
                        self.cluster, bucket, gen_create, "create", 0,
                        batch_size=20, persist_to=self.persist_to,
                        replicate_to=self.replicate_to, pause_secs=5,
                        durability=self.durability_level,
                        timeout_secs=self.sdk_timeout,
                        retries=self.sdk_retries))
                    # Update self.num_items as per the gen_create value
                    self.num_items += item_added
                if "delete" in self.doc_ops:
                    tasks.append(self.task.async_load_gen_docs(
                        self.cluster, bucket, gen_delete, "delete", 0,
                        batch_size=20, persist_to=self.persist_to,
                        replicate_to=self.replicate_to,
                        durability=self.durability_level,
                        pause_secs=5, timeout_secs=self.sdk_timeout,
                        retries=self.sdk_retries))
                    # Update self.num_items as per the gen_delete value
                    self.num_items -= self.items_deleted
                    self.delete_doc_index += self.items_deleted
        return tasks

    def __wait_for_load_gen_tasks_complete(self, tasks):
        """
        Function to wait for all doc loading tasks to complete
        and also verify no failures are seen during the operation.

        Returns:
        :result - Boolean value saying the CRUDs operation is succeeded or not
        """
        result = True
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)
            if task.failed_count != 0:
                result = False
                self.log.error("Failures seen during doc_ops: {0}"
                               .format(task.fail))
        return result

    def test_replica_update_with_durability_without_adding_removing_nodes(self):
        servs_in = [self.cluster.servers[i + self.nodes_init]
                    for i in range(self.nodes_in)]
        tasks = self.__load_docs_in_all_buckets()
        rebalance = self.task.async_rebalance(
            self.cluster.servers[:self.nodes_init], servs_in, [])
        self.task.jython_task_manager.get_task_result(rebalance)
        result = self.__wait_for_load_gen_tasks_complete(tasks)
        self.cluster.nodes_in_cluster.extend(servs_in)
        self.assertTrue(result, "CRUD failed during initial rebalance")

        # Override docs_ops to perform CREATE/UPDATE during all rebalance
        self.doc_ops = ["create", "update"]

        self.sleep(60, "Wait for cluster to be ready after rebalance")
        for replicas in range(2, 4):
            self.log.info("Updating the bucket replicas to {0}"
                          .format(replicas))
            tasks = self.__load_docs_in_all_buckets()
            self.bucket_util.update_all_bucket_replicas(replicas=replicas)
            rebalance = self.task.rebalance(
                self.cluster.servers[:self.nodes_init], [], [])
            self.task.jython_task_manager.get_task_result(rebalance)
            result = self.__wait_for_load_gen_tasks_complete(tasks)
            self.assertTrue(result, "CRUD failed during replica increment")

        for replicas in range(3, 0):
            self.log.info("Updating the bucket replicas to {0}"
                          .format(replicas))
            tasks = self.__load_docs_in_all_buckets()
            self.bucket_util.update_all_bucket_replicas(replicas=replicas)
            rebalance = self.task.rebalance(
                self.cluster.servers[:self.nodes_init], [], [])
            self.task.jython_task_manager.get_task_result(rebalance)
            result = self.__wait_for_load_gen_tasks_complete(tasks)
            self.assertTrue(result, "CRUD failed during replica decrement")

        # Verify doc load count to match the overall CRUDs
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

    def test_replica_update_with_durability_with_adding_removing_nodes(self):
        tasks = self.__load_docs_in_all_buckets()
        result = self.__wait_for_load_gen_tasks_complete(tasks)
        self.assertTrue(result, "CRUD failed during initial doc_load")

        # Override docs_ops to perform CREATE/UPDATE during all rebalance
        self.doc_ops = ["create", "update"]

        self.sleep(60, "Wait for cluster to be ready after rebalance")
        self.log.info("Increasing the replicas and rebalancing in the nodes")
        for replicas in range(2, 4):
            # Start document CRUDs
            tasks = self.__load_docs_in_all_buckets()
            self.log.info("Increasing the bucket replicas to {0}"
                          .format(replicas))
            self.bucket_util.update_all_bucket_replicas(replicas=replicas)
            rebalance = self.task.rebalance(self.cluster.nodes_in_cluster,
                                            [self.cluster.servers[replicas]],
                                            [])
            self.task.jython_task_manager.get_task_result(rebalance)
            self.cluster.nodes_in_cluster.extend([self.cluster.servers[replicas]])
            # Wait for all doc_load tasks to complete and validate
            result = self.__wait_for_load_gen_tasks_complete(tasks)
            self.assertTrue(result, "CRUD failed during initial doc_load")

        self.log.info("Decreasing the replicas and rebalancing out the nodes")
        for replicas in range(3, 0):
            self.log.info("Reducing the bucket replicas to {0}"
                          .format((replicas - 1)))
            # Start document CRUDs
            tasks = self.__load_docs_in_all_buckets()
            self.bucket_util.update_all_bucket_replicas(replicas=replicas - 1)
            rebalance = self.task.rebalance(
                self.cluster.servers[:self.nodes_init], [],
                [self.cluster.servers[replicas - 1]])
            self.task.jython_task_manager.get_task_result(rebalance)
            # Wait for all doc_load tasks to complete and validate
            result = self.__wait_for_load_gen_tasks_complete(tasks)
            self.assertTrue(result, "CRUD failed during initial doc_load")

        # Verify doc load count to match the overall CRUDs
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

    def test_rebalance_out_durabilitybreaks_rebalance_in(self):
        self.assertTrue(self.num_replicas >= 1,
                        "Need at-least one replica to run this test")

        durability_will_fail = False
        def_bucket = self.bucket_util.buckets[0]

        nodes_in_cluster = self.nodes_init
        nodes_required_for_durability = int(floor(self.num_replicas/2)+1)

        gen_update = doc_generator(self.key, 0, self.num_items)
        num_of_docs_to_insert = 1000

        # Connection to run cbstats command
        master_shell_conn = RemoteMachineShellConnection(self.cluster.master)
        master_node_cb_stat = Cbstats(master_shell_conn)

        # Dict to store vb_seq_no info
        vb_info = dict()
        vb_info["init"] = master_node_cb_stat.vbucket_seqno(def_bucket.name)

        # Rebalance all nodes expect master one-by-one
        for _ in range(self.nodes_init-1):
            rebalance_out_task = self.task.rebalance(
                self.cluster.servers[:nodes_in_cluster], [],
                [self.cluster.servers[nodes_in_cluster-1]])
            self.task.jython_task_manager.get_task_result(rebalance_out_task)
            nodes_in_cluster -= 1

            if nodes_in_cluster <= nodes_required_for_durability:
                durability_will_fail = True

            tasks = list()
            # Perform CRUD operations after rebalance_out to verify the
            # durability outcome
            gen_create = doc_generator(self.key, self.num_items,
                                       self.num_items+num_of_docs_to_insert)
            tasks.append(self.task.async_load_gen_docs(
                self.cluster, def_bucket, gen_create, "create", 0,
                batch_size=20, persist_to=self.persist_to,
                replicate_to=self.replicate_to,
                pause_secs=5, timeout_secs=self.sdk_timeout,
                retries=self.sdk_retries))
            tasks.append(self.task.async_load_gen_docs(
                self.cluster, def_bucket, gen_update, "update", 0,
                batch_size=20, persist_to=self.persist_to,
                replicate_to=self.replicate_to,
                pause_secs=5, timeout_secs=self.sdk_timeout,
                retries=self.sdk_retries))

            # Wait for all CRUD tasks to complete
            for task in tasks:
                self.task.jython_task_manager.get_task_result(task)

            # Fetch vb_seq_no after the CRUDs
            vb_info["afterCrud"] = \
                master_node_cb_stat.vbucket_seqno(def_bucket.name)

            if durability_will_fail:
                self.log.info("Durability broken with cluster {0}, replica {1}"
                              .format(self.cluster.servers[:nodes_in_cluster],
                                      self.num_replicas))
                # Verification of seq_no for each vbucket
                for vb_num in range(0, self.vbuckets):
                    self.assertTrue(
                        vb_info["init"][vb_num]["abs_high_seqno"]
                        == vb_info["afterCrud"][vb_num]["abs_high_seq_no"],
                        "Seq_no mismatch for vbucket {0}. {1}->{2}"
                        .format(vb_num,
                                vb_info["init"][vb_num]["abs_high_seqno"],
                                vb_info["afterCrud"][vb_num]["abs_high_seqno"]))
                # Break from loop that is running rebalance_out tasks
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

            # Fetch vb_seq_no after the CRUDs
            vb_info["afterCrud"] = \
                master_node_cb_stat.vbucket_seqno(def_bucket.name)

        # Disconnect the remote_shell connection
        master_shell_conn.disconnect()

        # Rebalance-in single node back into the cluster
        rebalance_in_task = self.task.rebalance(
            self.cluster.servers[:nodes_in_cluster],
            [self.cluster.servers[:nodes_in_cluster+1]], [])
        # Wait for rebalance-in task to complete
        self.task.jython_task_manager.get_task_result(rebalance_in_task)

        # Reset the tasks list
        tasks = list()
        # Perform CRUD operations after rebalance_in to verify the
        # durability is working as expected again
        gen_create = doc_generator(self.key, self.num_items,
                                   self.num_items+num_of_docs_to_insert)
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, def_bucket, gen_create, "create", 0,
            batch_size=20, persist_to=self.persist_to,
            replicate_to=self.replicate_to,
            pause_secs=5, timeout_secs=self.sdk_timeout,
            retries=self.sdk_retries))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, def_bucket, gen_update, "update", 0,
            batch_size=20, persist_to=self.persist_to,
            replicate_to=self.replicate_to,
            pause_secs=5, timeout_secs=self.sdk_timeout,
            retries=self.sdk_retries))

        # Wait for all CRUD tasks to complete
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

        # After rebalance-in durability should works fine
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
                self.task.jython_task_manager.get_all_result(task)
                self.assertTrue(len(task.fail.keys()) == 0,
                                msg="Unexpected durability failures: {0}"
                                .format(task.fail))

        self.assertTrue(self.replica_to_update is not None)
        def_bucket = self.bucket_util.buckets[0]
        servers_in = [self.cluster.servers[self.nodes_init + i]
                      for i in range(self.nodes_in)]
        servers_out = [self.cluster.servers[self.num_servers - i - 1]
                       for i in range(self.nodes_out)]

        # Start CRUD operations
        crud_tasks = self.__load_docs_in_all_buckets()

        # Rebalance_out the orchestrator node
        self.add_remove_servers_and_rebalance([], [self.cluster.servers[0]])

        # Wait for all CRUD tasks to complete and verify no failures are seen
        wait_for_crud_task_and_verify_for_no_errors(crud_tasks)

        # Start CRUD operations
        crud_tasks = self.__load_docs_in_all_buckets()
        # Rebalance_in multiple cluster nodes
        self.add_remove_servers_and_rebalance([servers_in], [])
        wait_for_crud_task_and_verify_for_no_errors(crud_tasks)

        # Start CRUD operations
        crud_tasks = self.__load_docs_in_all_buckets()
        # Update bucket replica value
        bucket_helper = BucketHelper(self.cluster.servers[1])
        bucket_helper.change_bucket_props(def_bucket.name,
                                          replicaNumber=self.replica_to_update)
        # Start and wait till rebalance is complete
        rebalance = self.task.async_rebalance(self.cluster.servers, [], [])
        self.task.jython_task_manager.get_task_result(rebalance)
        wait_for_crud_task_and_verify_for_no_errors(crud_tasks)

        # Start CRUD operations
        crud_tasks = self.__load_docs_in_all_buckets()
        # Rebalance_out multiple cluster nodes
        self.add_remove_servers_and_rebalance([], [servers_out])
        wait_for_crud_task_and_verify_for_no_errors(crud_tasks)

        # Start CRUD operations
        crud_tasks = self.__load_docs_in_all_buckets()
        wait_for_crud_task_and_verify_for_no_errors(crud_tasks)

        # Doc count verification
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)
