from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import doc_generator


class MultiBucketTests(BaseTestCase):
    def setUp(self):
        super(MultiBucketTests, self).setUp()
        self.key = "test_multi_bucket_docs".rjust(self.key_size, '0')
        self.nodes_init = self.input.param("nodes_init", 1)
        self.nodes_in = self.input.param("nodes_in", 1)
        self.nodes_out = self.input.param("nodes_out", 1)
        self.doc_ops = self.input.param("doc_ops", "create")
        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.append(self.cluster.master)

        # Create  multiple buckets here
        buckets_created = self.bucket_util.create_multiple_buckets(
            self.cluster.master, self.num_replicas,
            bucket_count=self.standard_buckets, bucket_type=self.bucket_type)
        self.assertTrue(buckets_created, "Multi-bucket creation failed")
        self.bucket_util.add_rbac_user()

        self.log.info("Creating doc_generator..")
        self.load_gen = doc_generator(
            self.key, 0, self.num_items,
            doc_size=self.doc_size,
            doc_type=self.doc_type,
            target_vbucket=self.target_vbucket,
            vbuckets=self.vbuckets)
        self.log.info("doc_generator created")

        # Load all buckets with initial load of docs
        tasks = []
        for bucket in self.bucket_util.buckets:
            tasks.append(self.task.async_load_gen_docs(
                self.cluster, bucket, self.load_gen, "create", 0,
                batch_size=10,
                process_concurrency=2,
                replicate_to=self.replicate_to,
                persist_to=self.persist_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout))

        for task in tasks:
            self.task_manager.get_task_result(task)

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)
        self.log.info("====== MultiBucketTests base setup complete ======")

    def tearDown(self):
        super(MultiBucketTests, self).tearDown()

    def test_multi_bucket_cruds(self):
        tasks = list()
        bucket_1 = self.bucket_util.buckets[0]
        bucket_2 = self.bucket_util.buckets[1]
        bucket_3 = self.bucket_util.buckets[2]
        bucket_4 = self.bucket_util.buckets[3]

        self.log.info("Creating create doc_generator")
        # Generator for loading new docs
        gen_create = doc_generator(
            self.key, self.num_items, self.num_items * 2,
            doc_size=self.doc_size, doc_type="json",
            target_vbucket=self.target_vbucket, vbuckets=self.vbuckets)
        self.log.info("doc_generator created")

        # Load new docs in bucket-1
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, bucket_1, gen_create, "create", 0, batch_size=10,
            process_concurrency=1,
            replicate_to=self.replicate_to,
            persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout))

        # Update docs in bucket-2
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, bucket_2, self.load_gen, "update", 0, batch_size=10,
            process_concurrency=1,
            replicate_to=self.replicate_to,
            persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout))

        # Delete docs in bucket-3
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, bucket_3, self.load_gen, "delete", 0, batch_size=10,
            process_concurrency=1,
            replicate_to=self.replicate_to,
            persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout))

        # Read docs in bucket-4
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, bucket_4, self.load_gen, "read", 0, batch_size=10,
            process_concurrency=1,
            replicate_to=self.replicate_to,
            persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout))

        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

        # Verify doc counts after bucket ops
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_for_bucket(bucket_1, self.num_items*2)
        for bucket in [bucket_2, bucket_4]:
            self.bucket_util.verify_stats_for_bucket(bucket, self.num_items)
        self.bucket_util.verify_stats_for_bucket(bucket_3, 0)

    def test_multi_bucket_in_different_state(self):
        dgm_for_bucket1 = 80
        dgm_for_bucket2 = 50
        dgm_for_bucket3 = 20

        bucket_1 = self.bucket_util.buckets[0]
        bucket_2 = self.bucket_util.buckets[1]
        bucket_3 = self.bucket_util.buckets[2]
        bucket_4 = self.bucket_util.buckets[3]

        tasks = list()

        gen_update = doc_generator(self.key, 0, self.num_items)
        update_task = self.task.async_continuous_update_docs(
            self.cluster, bucket_4, gen_update, exp=0,
            batch_size=10,
            process_concurrency=2,
            replicate_to=self.replicate_to,
            persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout)

        bucket_1_num_items = self.task.load_bucket_into_dgm(
            self.cluster, bucket_1, self.key, self.num_items,
            dgm_for_bucket1,
            batch_size=10,
            process_concurrency=2,
            persist_to=self.persist_to,
            replicate_to=self.replicate_to,
            durability=self.durability_level,
            sdk_timeout=self.sdk_timeout)
        bucket_2_num_items = self.task.load_bucket_into_dgm(
            self.cluster, bucket_2, self.key, self.num_items,
            dgm_for_bucket2,
            batch_size=10,
            process_concurrency=2,
            persist_to=self.persist_to,
            replicate_to=self.replicate_to,
            durability=self.durability_level,
            sdk_timeout=self.sdk_timeout)
        bucket_3_num_items = self.task.load_bucket_into_dgm(
            self.cluster, bucket_3, self.key, self.num_items,
            dgm_for_bucket3,
            batch_size=10,
            process_concurrency=2,
            persist_to=self.persist_to,
            replicate_to=self.replicate_to,
            sdk_timeout=self.sdk_timeout)

        gen_1 = doc_generator(self.key, bucket_1_num_items,
                              bucket_1_num_items+self.num_items)
        gen_2 = doc_generator(self.key, bucket_2_num_items,
                              bucket_2_num_items+self.num_items)
        gen_3 = doc_generator(self.key, self.num_items, bucket_3_num_items)

        # Start ASYNC tasks to bucket ops on multi-buckets
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, bucket_1, gen_1, "create", self.maxttl,
            batch_size=10,
            process_concurrency=1,
            replicate_to=self.replicate_to,
            persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, bucket_2, gen_2, "create", self.maxttl,
            batch_size=10,
            process_concurrency=1,
            replicate_to=self.replicate_to,
            persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout))
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, bucket_3, gen_3, "delete", self.maxttl,
            batch_size=10,
            process_concurrency=1,
            replicate_to=self.replicate_to,
            persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout))

        # Wait for tasks to complete
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

        # Stop the continuous doc update task
        self.task_manager.stop_task(update_task)
        self.bucket_util.verify_stats_for_bucket(bucket_4, self.num_items)
