from rebalance_base import RebalanceBaseTest


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
        tasks = []
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
        tasks = []
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
