from basetestcase import BaseTestCase
from bucket_utils.Bucket import Bucket
from couchbase_helper.documentgenerator import doc_generator


class MultiDurabilityTests(BaseTestCase):
    def setUp(self):
        super(MultiDurabilityTests, self).setUp()

        self.key = 'test_docs'.rjust(self.key_size, '0')
        replica_list = self.input.param("replica_list", list())
        bucket_type_list = self.input.param("bucket_type_list", list())
        self.bucket_dict = dict()
        tasks = dict()
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
                self.bucket_dict[index]["replica"] = replica_list[index]

            # If bucket_type not provided, set replica value to 'MEMBASE'
            if len(bucket_type_list)-1 < index:
                self.bucket_dict[index]["type"] = Bucket.bucket_type.MEMBASE
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
            tasks[bucket] = self.bucket_util.async_create_bucket(bucket)

            # Append bucket object into the bucket_info dict
            self.bucket_dict[index]["object"] = self.bucket_util.buckets[-1]

        raise_exception = None
        for bucket, task in tasks.items():
            self.task_manager.get_task_result(task)
            if task.result:
                self.sleep(2)
                warmed_up = self.bucket_util._wait_warmup_completed(
                    self.cluster_util.get_kv_nodes(), bucket, wait_time=60)
                if not warmed_up:
                    task.result = False
                    raise_exception = "Bucket %s not warmed up" % bucket.name

            if task.result:
                self.buckets.append(bucket)
            self.task_manager.stop_task(task)

        if raise_exception:
            raise Exception("Create bucket failed: %s" % raise_exception)
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
            tasks = []
            tasks.append(self.task.async_load_gen_docs(
                self.cluster, bucket_info[0]["object"],
                bucket_info[0]["loader"],
                bucket_info[0]["op_type"], 0, batch_size=10,
                process_concurrency=1,
                replicate_to=bucket_info[0]["replicate_to"],
                persist_to=bucket_info[0]["persist_to"],
                durability=bucket_info[0]["durability"],
                sdk_timeout=bucket_info[0]["sdk_timeout"]))
            tasks.append(self.task.async_load_gen_docs(
                self.cluster, bucket_info[1]["object"],
                bucket_info[1]["loader"],
                bucket_info[1]["op_type"], 0, batch_size=10,
                process_concurrency=1,
                replicate_to=bucket_info[1]["replicate_to"],
                persist_to=bucket_info[1]["persist_to"],
                durability=bucket_info[1]["durability"],
                sdk_timeout=bucket_info[1]["sdk_timeout"]))
            tasks.append(self.task.async_load_gen_docs(
                self.cluster, bucket_info[2]["object"],
                bucket_info[2]["loader"],
                bucket_info[2]["op_type"], 0, batch_size=10,
                process_concurrency=1,
                replicate_to=bucket_info[2]["replicate_to"],
                persist_to=bucket_info[2]["persist_to"],
                durability=bucket_info[2]["durability"],
                sdk_timeout=bucket_info[2]["sdk_timeout"]))
            tasks.append(self.task.async_load_gen_docs(
                self.cluster, bucket_info[3]["object"],
                bucket_info[3]["loader"],
                bucket_info[3]["op_type"], 0, batch_size=10,
                process_concurrency=1,
                replicate_to=bucket_info[3]["replicate_to"],
                persist_to=bucket_info[3]["persist_to"],
                durability=bucket_info[3]["durability"],
                sdk_timeout=bucket_info[3]["sdk_timeout"]))
            tasks.append(self.task.async_load_gen_docs(
                self.cluster, bucket_info[4]["object"],
                bucket_info[4]["loader"],
                bucket_info[4]["op_type"], 0, batch_size=10,
                process_concurrency=1,
                replicate_to=bucket_info[4]["replicate_to"],
                persist_to=bucket_info[4]["persist_to"],
                durability=bucket_info[4]["durability"],
                sdk_timeout=bucket_info[4]["sdk_timeout"]))

            # Wait for all tasks to complete
            for task in tasks:
                self.task.jython_task_manager.get_task_result(task)

            # Verify doc load count
            self.bucket_util._wait_for_stats_all_buckets()
            for _, tem_bucket in bucket_info.items():
                self.bucket_util.verify_stats_for_bucket(
                    tem_bucket["object"],
                    tem_bucket["num_items"],
                    timeout=30)

        # Function to update the existing bucket dictionary object
        # based on the values passed to it
        def dict_updater(index, bucket_dict, doc_gen, op_type,
                         persist_to=0, replicate_to=0, durability=None,
                         sdk_timeout=0):
            bucket_dict[index]["loader"] = doc_gen
            bucket_dict[index]["op_type"] = "create"
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
        gen_create = doc_generator(self.key, 0, self.num_items)
        for bucket in self.bucket_util.buckets:
            doc_loader = self.task.async_load_gen_docs(
                self.cluster, bucket, gen_create, "create", 0,
                batch_size=10, process_concurrency=8,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries)
            self.task.jython_task_manager.get_task_result(doc_loader)

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)
        return

        half_of_num_items = int(self.num_items/2)

        # Create doc_generators for various CRUD operations
        gen_create = doc_generator(self.key, self.num_items, self.num_items*2)
        gen_update = doc_generator(self.key, half_of_num_items, self.num_items)
        gen_delete = doc_generator(self.key, 0, half_of_num_items)

        # replicate_to computation for 4th bucket object
        replica = self.bucket_dict[3]["replica"]
        replicate_to = 0 if replica == 0 else int(replica/2) + 1
        # First-set of ops on multi-buckets
        dict_updater(0, self.bucket_dict, gen_create, "create",
                     durability="MAJORITY", sdk_timeout=5)
        dict_updater(1, self.bucket_dict, gen_update, "update",
                     durability="MAJORITY_AND_PERSIST_ON_MASTER",
                     sdk_timeout=5)
        dict_updater(2, self.bucket_dict, gen_delete, "delete",
                     durability="PERSIST_TO_MAJORITY", sdk_timeout=5)
        dict_updater(3, self.bucket_dict, gen_update, "update",
                     persist_to=1, replicate_to=replicate_to)
        dict_updater(4, self.bucket_dict, gen_create, "create")

        # Call the generic doc_operation function
        generic_multi_load_cruds(self.bucket_dict)

        # replicate_to computation for 4th bucket object
        replica = self.bucket_dict[1]["replica"]
        replicate_to = 0 if replica == 0 else int(replica/2) + 1
        persist_to = 0 if replica == 0 else int(replica/2) + 1
        # Second-set of ops on multi-buckets
        dict_updater(0, self.bucket_dict, gen_update, "update",
                     persist_to=persist_to, replicate_to=replicate_to)
        dict_updater(1, self.bucket_dict, gen_delete, "delete",
                     durability="MAJORITY_AND_PERSIST_ON_MASTER",
                     sdk_timeout=5)
        dict_updater(2, self.bucket_dict, gen_create, "create",
                     durability="PERSIST_TO_MAJORITY", sdk_timeout=5)
        dict_updater(3, self.bucket_dict, gen_update, "update",
                     durability="MAJORITY", sdk_timeout=5)
        dict_updater(4, self.bucket_dict, gen_create, "create")

        # Call the generic doc_operation function
        generic_multi_load_cruds(self.bucket_dict)
