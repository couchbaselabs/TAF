from math import floor
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from rebalance_base import RebalanceBaseTest
from remote.remote_util import RemoteMachineShellConnection


class RebalanceDurability(RebalanceBaseTest):
    def setUp(self):
        super(RebalanceDurability, self).setUp()

    def tearDown(self):
        super(RebalanceDurability, self).tearDown()

    def test_replica_update_with_durability_without_adding_removing_nodes(self):
        gen_create = self.get_doc_generator(self.num_items, self.num_items * 2)
        gen_delete = self.get_doc_generator(self.num_items / 2, self.num_items)
        servs_in = [self.cluster.servers[i + self.nodes_init]
                    for i in range(self.nodes_in)]
        tasks = list()
        task = self.task.async_rebalance(
            self.cluster.servers[:self.nodes_init], servs_in, [])
        tasks.append(task)
        for bucket in self.bucket_util.buckets:
            if (self.doc_ops is not None):
                if ("update" in self.doc_ops):
                    tasks.append(self.task.async_load_gen_docs(
                        self.cluster, bucket, self.gen_update, "update", 0,
                        batch_size=20, persist_to=self.persist_to,
                        replicate_to=self.replicate_to, pause_secs=5,
                        timeout_secs=self.sdk_timeout,
                        retries=self.sdk_retries))
                if ("create" in self.doc_ops):
                    tasks.append(self.task.async_load_gen_docs(
                        self.cluster, bucket, gen_create, "create", 0,
                        batch_size=20, persist_to=self.persist_to,
                        replicate_to=self.replicate_to, pause_secs=5,
                        timeout_secs=self.sdk_timeout,
                        retries=self.sdk_retries))
                if ("delete" in self.doc_ops):
                    tasks.append(self.task.async_load_gen_docs(
                        self.cluster, bucket, gen_delete, "delete", 0,
                        batch_size=20, persist_to=self.persist_to,
                        replicate_to=self.replicate_to,
                        pause_secs=5, timeout_secs=self.sdk_timeout,
                        retries=self.sdk_retries))
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)
        self.cluster.nodes_in_cluster.extend(servs_in)

        self.sleep(60, "Wait for cluster to be ready after rebalance")
        for bucket in self.bucket_util.buckets:
            if (self.doc_ops is not None):
                if ("update" in self.doc_ops):
                    tasks.append(self.task.async_validate_docs(
                        self.cluster, bucket, self.gen_update, "update", 0,
                        batch_size=10))
                if ("create" in self.doc_ops):
                    tasks.append(
                        self.task.async_validate_docs(
                            self.cluster, bucket, gen_create, "create", 0,
                            batch_size=10, process_concurrency=8))
                if ("delete" in self.doc_ops):
                    tasks.append(
                        self.task.async_validate_docs(
                            self.cluster, bucket, gen_delete, "delete", 0,
                            batch_size=10))

        for replicas in range(2, 4):
            self.log.info("Updating the bucket replicas to {0}"
                          .format(replicas))
            self.bucket_util.update_all_bucket_replicas(replicas=replicas)
            rebalance = self.task.rebalance(
                self.cluster.servers[:self.nodes_init], [], [])
            self.task.jython_task_manager.get_task_result(rebalance)

        for replicas in range(3, 0):
            self.log.info("Updating the bucket replicas to {0}"
                          .format(replicas))
            self.bucket_util.update_all_bucket_replicas(replicas=replicas)
            rebalance = self.task.rebalance(
                self.cluster.servers[:self.nodes_init], [], [])
            self.task.jython_task_manager.get_task_result(rebalance)

        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)
        self.bucket_util.verify_stats_all_buckets(self.num_items * 2)

    def test_replica_update_with_durability_with_adding_removing_nodes(self):
        gen_create = self.get_doc_generator(self.num_items, self.num_items * 2)
        gen_delete = self.get_doc_generator(self.num_items / 2, self.num_items)
        tasks = list()
        for bucket in self.bucket_util.buckets:
            if (self.doc_ops is not None):
                if ("update" in self.doc_ops):
                    tasks.append(self.task.async_load_gen_docs(
                        self.cluster, bucket, self.gen_update, "update", 0,
                        batch_size=20, persist_to=self.persist_to,
                        replicate_to=self.replicate_to, pause_secs=5,
                        timeout_secs=self.sdk_timeout,
                        retries=self.sdk_retries))
                if ("create" in self.doc_ops):
                    tasks.append(self.task.async_load_gen_docs(
                        self.cluster, bucket, gen_create, "create", 0,
                        batch_size=20, persist_to=self.persist_to,
                        replicate_to=self.replicate_to,
                        pause_secs=5, timeout_secs=self.sdk_timeout,
                        retries=self.sdk_retries))
                if ("delete" in self.doc_ops):
                    tasks.append(self.task.async_load_gen_docs(
                        self.cluster, bucket, gen_delete, "delete", 0,
                        batch_size=20, persist_to=self.persist_to,
                        replicate_to=self.replicate_to,
                        pause_secs=5, timeout_secs=self.sdk_timeout,
                        retries=self.sdk_retries))
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

        self.sleep(60, "Wait for cluster to be ready after rebalance")
        for bucket in self.bucket_util.buckets:
            if (self.doc_ops is not None):
                if ("update" in self.doc_ops):
                    tasks.append(self.task.async_validate_docs(
                        self.cluster, bucket, self.gen_update, "update", 0,
                        batch_size=10))
                if ("create" in self.doc_ops):
                    tasks.append(
                        self.task.async_validate_docs(
                            self.cluster, bucket, gen_create, "create", 0,
                            batch_size=10, process_concurrency=8))
                if ("delete" in self.doc_ops):
                    tasks.append(
                        self.task.async_validate_docs(self.cluster, bucket,
                                                      gen_delete, "delete", 0,
                                                      batch_size=10))

        self.log.info("Increasing the replicas and rebalancing in the nodes")
        for replicas in range(2, 4):
            self.log.info("Increasing the bucket replicas to {0}"
                          .format(replicas))
            self.bucket_util.update_all_bucket_replicas(replicas=replicas)
            rebalance = self.task.rebalance(self.cluster.nodes_in_cluster,
                                            [self.cluster.servers[replicas]],
                                            [])
            self.task.jython_task_manager.get_task_result(rebalance)
            self.cluster.nodes_in_cluster.extend([self.cluster.servers[replicas]])

        self.log.info("Decreasing the replicas and rebalancing out the nodes")
        for replicas in range(3, 0):
            self.log.info("Reducing the bucket replicas to {0}"
                          .format((replicas - 1)))
            self.bucket_util.update_all_bucket_replicas(replicas=replicas - 1)
            rebalance = self.task.rebalance(
                self.cluster.servers[:self.nodes_init], [],
                [self.cluster.servers[replicas - 1]])
            self.task.jython_task_manager.get_task_result(rebalance)

        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)
        self.bucket_util.verify_stats_all_buckets(self.num_items * 2)

    def test_rebalance_in_out_with_durability_check(self):
        self.assertTrue(self.num_replicas >= 1,
                        "Need at-least one replica to run this test")

        durability_will_fail = False
        def_bucket = self.bucket_util.buckets[0]

        nodes_in_cluster = self.nodes_init
        nodes_required_for_durability = int(floor(self.num_replicas/2)+1)

        gen_update = doc_generator(self.key, 0, self.num_items)
        num_of_docs_to_insert = 1000

        vbucket_info_dict = dict()

        # Connection to run cbstats command
        master_shell_conn = RemoteMachineShellConnection(self.cluster.master)
        master_node_cb_stat = Cbstats(master_shell_conn)

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

            if durability_will_fail:
                self.log.info("Durability broken with cluster {0}, replica {1}"
                              .format(self.cluster.servers[:nodes_in_cluster],
                                      self.num_replicas))
                # Verification of seq_no for each vbucket
                for vb_num in range(0, self.vbuckets):
                    tem_seqno = master_node_cb_stat.vbucket_seqno(
                        def_bucket.name, vb_num, "abs_high_seqno")
                    self.assertTrue(tem_seqno == vbucket_info_dict[vb_num],
                                    "Seq_no mismatch for vbucket {0}. {1}->{2}"
                                    .format(vb_num, vbucket_info_dict[vb_num],
                                            tem_seqno))
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

            # Update each vbucket's seq_no for latest value for verification
            for vb_num in range(0, self.vbuckets):
                vbucket_info_dict[vb_num] = master_node_cb_stat.vbucket_seqno(
                    def_bucket.name, vb_num, "abs_high_seqno")

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
