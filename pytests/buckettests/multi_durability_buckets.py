from basetestcase import BaseTestCase
from BucketLib.bucket import Bucket
from couchbase_helper.documentgenerator import doc_generator
from membase.api.rest_client import RestConnection


class MultiDurabilityTests(BaseTestCase):
    def setUp(self):
        super(MultiDurabilityTests, self).setUp()

        replica_list = self.input.param("replica_list", list())
        bucket_type_list = self.input.param("bucket_type_list", list())
        self.bucket_dict = dict()
        tasks = dict()

        # Create cluster
        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        self.task.rebalance(self.cluster, nodes_init, [])
        self.bucket_util.add_rbac_user(self.cluster.master)

        rest = RestConnection(self.cluster.master)
        info = rest.get_nodes_self()
        if info.memoryQuota < 450.0:
            self.fail("At least 450MB of memory required")
        else:
            available_ram = info.memoryQuota * (2.0 / 3.0)
            if available_ram / self.standard_buckets > 100:
                bucket_ram_quota = int(available_ram / self.standard_buckets)
            else:
                bucket_ram_quota = 100

        if type(replica_list) is str:
            replica_list = replica_list.split(";")
        if type(bucket_type_list) is str:
            bucket_type_list = bucket_type_list.split(";")

        for index in range(self.standard_buckets):
            self.bucket_dict[index] = dict()

            # If replica not provided, set replica value to '0'
            if len(replica_list)-1 < index:
                self.bucket_dict[index]["replica"] = 0
            else:
                self.bucket_dict[index]["replica"] = int(replica_list[index])

            # If bucket_type not provided, set replica value to 'MEMBASE'
            if len(bucket_type_list)-1 < index:
                self.bucket_dict[index]["type"] = Bucket.Type.MEMBASE
            else:
                self.bucket_dict[index]["type"] = bucket_type_list[index]

            # create bucket object for creation
            bucket = Bucket(
                {Bucket.name: "bucket_{0}".format(index),
                 Bucket.bucketType: self.bucket_dict[index]["type"],
                 Bucket.ramQuotaMB: bucket_ram_quota,
                 Bucket.replicaNumber: self.bucket_dict[index]["replica"],
                 Bucket.compressionMode: "off",
                 Bucket.maxTTL: 0})
            tasks[bucket] = self.bucket_util.async_create_bucket(self.cluster,
                                                                 bucket)

            # Append bucket object into the bucket_info dict
            self.bucket_dict[index]["object"] = bucket

        raise_exception = None
        for bucket, task in tasks.items():
            self.task_manager.get_task_result(task)
            if task.result:
                self.sleep(2)
                warmed_up = self.bucket_util._wait_warmup_completed(
                    self.cluster_util.get_kv_nodes(self.cluster), bucket,
                    wait_time=60)
                if not warmed_up:
                    task.result = False
                    raise_exception = "Bucket %s not warmed up" % bucket.name

            if task.result:
                self.cluster.buckets.append(bucket)
            self.task_manager.stop_task(task)

        if raise_exception:
            raise Exception("Create bucket failed: %s" % raise_exception)
        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)
        self.log.info("=== MultiDurabilityTests base setup done ===")

    def tearDown(self):
        super(MultiDurabilityTests, self).tearDown()

    def test_multi_bucket_with_different_durability_level(self):
        """
        Test to perform CRUDs on multiple buckets using various
        durability levels

        1. Multiple buckets will be created as part of setUp() part
        2. Perform all possible durability level operation on these buckets
           in parallel
        3. Validate all the CRUDs performed went through fine
        """

        # Function to perform CRUDs on all buckets in parallel and verify
        def generic_multi_load_cruds(bucket_info):
            tasks = list()
            for index in range(5):
                tasks.append(self.task.async_load_gen_docs(
                    self.cluster, bucket_info[index]["object"],
                    bucket_info[index]["loader"],
                    bucket_info[index]["op_type"], 0, batch_size=10,
                    process_concurrency=1,
                    replicate_to=bucket_info[index]["replicate_to"],
                    persist_to=bucket_info[index]["persist_to"],
                    durability=bucket_info[index]["durability"],
                    timeout_secs=bucket_info[index]["sdk_timeout"],
                    task_identifier=bucket_info[index]["object"].name,
                    print_ops_rate=False))

            # Wait for all tasks to complete
            for task in tasks:
                self.task.jython_task_manager.get_task_result(task)

            # Verify doc load count
            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets)
            for _, tem_bucket in bucket_info.items():
                self.bucket_util.verify_stats_for_bucket(
                    self.cluster,
                    tem_bucket["object"],
                    tem_bucket["num_items"],
                    timeout=30)

        # Function to update the existing bucket dictionary object
        # based on the values passed to it
        def dict_updater(index, bucket_dict, doc_gen, op_type,
                         persist_to=0, replicate_to=0, durability="",
                         sdk_timeout=0):
            bucket_dict[index]["loader"] = doc_gen
            bucket_dict[index]["op_type"] = op_type
            bucket_dict[index]["persist_to"] = persist_to
            bucket_dict[index]["replicate_to"] = replicate_to
            bucket_dict[index]["durability"] = durability
            bucket_dict[index]["sdk_timeout"] = sdk_timeout
            if "num_items" not in bucket_dict[index]:
                bucket_dict[index]["num_items"] = self.num_items
            if op_type == "create":
                bucket_dict[index]["num_items"] += self.num_items
            elif op_type == "delete":
                bucket_dict[index]["num_items"] -= int(self.num_items)/2

        # Load all buckets without any durability requirements
        gen_create = doc_generator(self.key, 0, self.num_items,
                                   key_size=self.key_size,
                                   doc_size=self.doc_size,
                                   doc_type=self.doc_type,
                                   vbuckets=self.cluster.vbuckets)
        doc_loading_tasks = list()
        for bucket in self.cluster.buckets:
            doc_loading_tasks.append(self.task.async_load_gen_docs(
                self.cluster, bucket, gen_create, "create", 0,
                batch_size=10, process_concurrency=1,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                task_identifier=bucket.name))
        for task in doc_loading_tasks:
            self.task.jython_task_manager.get_task_result(task)

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)

        half_of_num_items = int(self.num_items/2)

        # Create doc_generators for various CRUD operations
        gen_create = doc_generator(self.key, self.num_items, self.num_items*2,
                                   key_size=self.key_size,
                                   doc_size=self.doc_size,
                                   doc_type=self.doc_type,
                                   vbuckets=self.cluster.vbuckets)
        gen_update = doc_generator(self.key, half_of_num_items, self.num_items,
                                   key_size=self.key_size,
                                   doc_size=self.doc_size,
                                   doc_type=self.doc_type,
                                   vbuckets=self.cluster.vbuckets)
        gen_delete = doc_generator(self.key, 0, half_of_num_items,
                                   key_size=self.key_size,
                                   doc_size=self.doc_size,
                                   doc_type=self.doc_type,
                                   vbuckets=self.cluster.vbuckets)

        # replicate_to computation for 4th bucket object
        replica = self.bucket_dict[3]["replica"]
        replicate_to = 0 if replica == 0 else int(replica/2) + 1
        # First-set of ops on multi-buckets
        dict_updater(
            0, self.bucket_dict, gen_create, "create",
            durability=Bucket.DurabilityLevel.MAJORITY,
            sdk_timeout=30)
        dict_updater(
            1, self.bucket_dict, gen_update, "update",
            durability=Bucket.DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE,
            sdk_timeout=30)
        dict_updater(
            2, self.bucket_dict, gen_delete, "delete",
            durability=Bucket.DurabilityLevel.PERSIST_TO_MAJORITY,
            sdk_timeout=60)
        dict_updater(3, self.bucket_dict, gen_update, "update",
                     persist_to=1, replicate_to=replicate_to,
                     sdk_timeout=10)
        dict_updater(4, self.bucket_dict, gen_create, "create",
                     sdk_timeout=10)
        dict_updater(5, self.bucket_dict, gen_create, "update")

        # Call the generic doc_operation function
        generic_multi_load_cruds(self.bucket_dict)

        # replicate_to computation for 4th bucket object
        replica = self.bucket_dict[1]["replica"]
        replicate_to = 0 if replica == 0 else int(replica/2) + 1
        persist_to = 0 if replica == 0 else int(replica/2) + 1
        # Second-set of ops on multi-buckets
        dict_updater(0, self.bucket_dict, gen_update, "update",
                     persist_to=persist_to, replicate_to=replicate_to,
                     sdk_timeout=30)
        dict_updater(
            1, self.bucket_dict, gen_delete, "delete",
            durability=Bucket.DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE,
            sdk_timeout=30)
        dict_updater(
            2, self.bucket_dict, gen_create, "create",
            durability=Bucket.DurabilityLevel.PERSIST_TO_MAJORITY,
            sdk_timeout=60)
        dict_updater(
            3, self.bucket_dict, gen_delete, "delete",
            durability=Bucket.DurabilityLevel.MAJORITY,
            sdk_timeout=10)
        dict_updater(4, self.bucket_dict, gen_update, "update",
                     sdk_timeout=10)

        # Call the generic doc_operation function
        generic_multi_load_cruds(self.bucket_dict)
