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
            self.cluster, self.num_replicas,
            storage=self.bucket_storage,
            eviction_policy=self.bucket_eviction_policy,
            bucket_count=self.standard_buckets, bucket_type=self.bucket_type)
        self.assertTrue(buckets_created, "Multi-bucket creation failed")
        self.bucket_util.add_rbac_user(self.cluster.master)

        self.log.info("Creating doc_generator..")
        self.load_gen = doc_generator(
            self.key, 0, self.num_items,
            doc_size=self.doc_size,
            doc_type=self.doc_type,
            target_vbucket=self.target_vbucket,
            vbuckets=self.cluster.vbuckets)
        self.log.info("doc_generator created")

        # Load all buckets with initial load of docs
        tasks = list()
        for index, bucket in enumerate(self.cluster.buckets):
            tasks.append(self.task.async_load_gen_docs(
                self.cluster, bucket, self.load_gen, "create", 0,
                batch_size=10,
                process_concurrency=2,
                replicate_to=self.replicate_to,
                persist_to=self.persist_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                task_identifier="bucket%d" % int(index+1)))

        for task in tasks:
            self.task_manager.get_task_result(task)

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)
        self.log.info("====== MultiBucketTests base setup complete ======")

    def tearDown(self):
        super(MultiBucketTests, self).tearDown()

    def test_multi_bucket_cruds(self):
        def update_dict(bucket_obj, op_type, doc_gen):
            doc_ops_dict[bucket_obj] = dict()
            doc_ops_dict[bucket_obj]["op_type"] = op_type
            doc_ops_dict[bucket_obj]["doc_gen"] = doc_gen

        tasks = list()
        doc_ops_dict = dict()
        bucket_1 = self.cluster.buckets[0]
        bucket_2 = self.cluster.buckets[1]
        bucket_3 = self.cluster.buckets[2]
        bucket_4 = self.cluster.buckets[3]

        self.log.info("Creating create doc_generator")
        # Generator for loading new docs
        gen_create = doc_generator(
            self.key, self.num_items, self.num_items * 2,
            doc_size=self.doc_size, doc_type="json",
            target_vbucket=self.target_vbucket, vbuckets=self.cluster.vbuckets)

        update_dict(bucket_1, "create", gen_create)
        update_dict(bucket_2, "update", self.load_gen)
        update_dict(bucket_3, "delete", self.load_gen)
        update_dict(bucket_4, "read", self.load_gen)

        self.log.info("Starting doc_ops on buckets")
        for bucket_obj, bucket_op_dict in doc_ops_dict.items():
            tasks.append(self.task.async_load_gen_docs(
                self.cluster, bucket_obj,
                bucket_op_dict["doc_gen"],
                bucket_op_dict["op_type"],
                0, batch_size=10,
                process_concurrency=1,
                replicate_to=self.replicate_to,
                persist_to=self.persist_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                task_identifier=bucket_obj.name))

        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

        # Verify doc counts after bucket ops
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.verify_stats_for_bucket(self.cluster, bucket_1,
                                                 self.num_items*2)
        for bucket in [bucket_2, bucket_4]:
            self.bucket_util.verify_stats_for_bucket(self.cluster, bucket,
                                                     self.num_items)
        self.bucket_util.verify_stats_for_bucket(self.cluster, bucket_3, 0)

    def test_multi_bucket_in_different_state(self):
        def update_dict(bucket_obj, op_type, doc_gen, dgm=100, num_items=0):
            doc_ops_dict[bucket_obj] = dict()
            doc_ops_dict[bucket_obj]["num_items"] = num_items
            doc_ops_dict[bucket_obj]["dgm"] = dgm
            doc_ops_dict[bucket_obj]["op_type"] = op_type
            doc_ops_dict[bucket_obj]["doc_gen"] = doc_gen

        doc_ops_dict = dict()
        bucket_1 = self.cluster.buckets[0]
        bucket_2 = self.cluster.buckets[1]
        bucket_3 = self.cluster.buckets[2]
        bucket_4 = self.cluster.buckets[3]

        update_dict(bucket_1, "create", None, 80, self.num_items)
        update_dict(bucket_2, "create", None, 50, self.num_items)
        update_dict(bucket_3, "create", None, 20, self.num_items)

        gen_update = doc_generator(self.key, 0, self.num_items)
        update_task = self.task.async_continuous_doc_ops(
            self.cluster, bucket_4, gen_update,
            op_type="update",
            batch_size=10,
            process_concurrency=2,
            replicate_to=self.replicate_to,
            persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout)

        for bucket_obj, bucket_op_dict in doc_ops_dict.items():
            bucket_op_dict["num_items"] = \
                self.task.load_bucket_into_dgm(
                    self.cluster, bucket_1, self.key,
                    bucket_op_dict["num_items"],
                    bucket_op_dict["dgm"],
                    batch_size=10,
                    process_concurrency=2,
                    persist_to=self.persist_to,
                    replicate_to=self.replicate_to,
                    durability=self.durability_level,
                    sdk_timeout=self.sdk_timeout)

        gen_1 = doc_generator(
            self.key,
            doc_ops_dict[bucket_1]["num_items"],
            doc_ops_dict[bucket_1]["num_items"] + self.num_items)
        gen_2 = doc_generator(
            self.key,
            doc_ops_dict[bucket_2]["num_items"],
            doc_ops_dict[bucket_2]["num_items"] + self.num_items)
        gen_3 = doc_generator(
            self.key,
            self.num_items,
            doc_ops_dict[bucket_3]["num_items"])

        tasks = list()
        update_dict(bucket_1, "create", gen_1)
        update_dict(bucket_2, "create", gen_2)
        update_dict(bucket_3, "delete", gen_3)

        # Start ASYNC tasks to bucket ops on multi-buckets
        for bucket_obj, bucket_op_dict in doc_ops_dict.items():
            tasks.append(self.task.async_load_gen_docs(
                self.cluster, bucket_obj,
                bucket_op_dict[bucket_obj]["doc_gen"],
                bucket_op_dict[bucket_obj]["op_type"],
                exp=self.maxttl,
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

        # Validate docs in known buckets 3, 4
        for bucket_obj in [bucket_3, bucket_4]:
            self.bucket_util.verify_stats_for_bucket(self.cluster, bucket_obj,
                                                     self.num_items)
