import random
import string
import json

from Cb_constants import DocLoading
from sdk_client3 import SDKClient
from LMT_base import LMT
from couchbase_helper.documentgenerator import \
    doc_generator

class ServerlessThrottling(LMT):
    def setUp(self):
        super(ServerlessThrottling, self).setUp()
        self.bucket = self.cluster.buckets[0]
        self.sdk_compression = self.input.param("sdk_compression", False)
        compression_settings = {"enabled": self.sdk_compression}
        self.client = SDKClient([self.cluster.master], self.bucket,
                                compression_settings=compression_settings)

    def tearDown(self):
        self.client.close()
        super(ServerlessThrottling, self).tearDown()

    def data_load(self, gen_docs, op_type="create"):
        task_info = dict()
        task_info.update(self.bucket_util._async_load_all_buckets(
                self.cluster, gen_docs, op_type, exp=0,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                batch_size=10, process_concurrency=1))
        return task_info

    def get_size_of_doc(self, gen_doc):
        key, _ = next(gen_doc)
        result = self.client.crud(DocLoading.Bucket.DocOps.READ, key)
        self.key_size = len(result["key"])
        doc_size = len(result["value"])
        return self.key_size, doc_size

    def test_CRUD_throttling(self):
        self.bucket = self.bucket_util.get_all_buckets(self.cluster)[0]
        node = random.choice(self.bucket.servers)
        target_vbucket = self.get_active_vbuckets(node, self.bucket)
        throttle_limit = self.bucket_throttling_limit/len(self.bucket.servers)
        expected_wu = 0
        expected_num_throttled = 0
        write_units = self.bucket_util.calculate_units(self.doc_size, 0,
                                                       durability=self.durability_level)
        self.generate_data_for_vbuckets(target_vbucket)

        for op_type in ["create", "update", "replace"]:
            for key, value in self.key_value.iteritems():
                result = self.client.crud(op_type, key, value,
                                              durability=self.durability_level)
                if throttle_limit == 0 and result["status"] is False:
                    self.log.info("Load failed as expected for throttle limit")
                elif result["status"] is False:
                    self.log.critical("%s Loading failed: %s" % (key, result["error"]))
                throttle_limit, expected_num_throttled = self.calculate_expected_num_throttled(
                                                        node, self.bucket, throttle_limit,
                                                        write_units, expected_num_throttled)

            self.sleep(10)
            num_throttled, ru, wu = self.get_stat(self.bucket)
            expected_wu += (write_units * self.key_value.keys())
            self.compare_ru_wu_stat(ru, wu, 0, expected_wu)
            if num_throttled > 0:
                if num_throttled < (expected_num_throttled - 10):
                    self.fail("num_throlled value %s expected_num_throttled %s"
                                  %(num_throttled, expected_num_throttled))
            expected_num_throttled = num_throttled

    def test_subdoc_throttling(self):
        self.xattr = self.input.param("xattr", False)
        self.system_xattr = self.input.param("system_xattr", False)
        self.bucket = self.bucket_util.get_all_buckets(self.cluster)[0]
        node = random.choice(self.bucket.servers)
        target_vbucket = self.get_active_vbuckets(node, self.bucket)
        throttle_limit = self.bucket_throttling_limit/len(self.bucket.servers)


        sub_doc_key = "my-attr"
        if self.system_xattr:
            sub_doc_key = "my._attr"
        self.generate_data_for_vbuckets(target_vbucket)

        # create the documents
        for key, value in self.key_value.iteritems():
            result = self.client.crud(DocLoading.Bucket.DocOps.CREATE, key, value,
                                          durability=self.durability_level)
            if result["status"] is False:
                self.log.critical("%s Loading failed: %s" % (key, result["error"]))
                break
        expected_num_throttled, expected_ru, expected_wu = self.get_stat(self.bucket)

        self.total_size = self.doc_size + self.sub_doc_size + 10
        write_units = self.bucket_util.calculate_units(self.total_size, 0,
                                                       durability=self.durability_level)
        expected_ru = 0

        for sub_doc_op in ["subdoc_insert", "subdoc_upsert", "subdoc_replace"]:
            value = random.choice(string.ascii_letters) * self.sub_doc_size
            for key in self.key_value.keys():
                _, failed_items = self.client.crud(sub_doc_op, key,
                                                   [sub_doc_key, value],
                                                   durability=self.durability_level,
                                                   timeout=self.sdk_timeout,
                                                   time_unit="seconds",
                                                   create_path=self.xattr,
                                                   xattr=self.xattr)
                throttle_limit, expected_num_throttled = self.calculate_expected_num_throttled(
                                                            node, self.bucket, throttle_limit,
                                                            write_units, expected_num_throttled)

            self.sleep(5)
            num_throttled, ru, wu = self.get_stat(self.bucket)
            expected_wu += (write_units * len(self.key_value.keys()))
            self.compare_ru_wu_stat(ru, wu, expected_ru, expected_wu)

            if num_throttled > 0:
                if num_throttled < (expected_num_throttled - 10):
                    self.fail("num_throlled value %s expected_num_throttled %s"
                                  %(num_throttled, expected_num_throttled))
            expected_num_throttled = num_throttled

    def test_load_all_nodes(self):
        # set throttle limit to a very high value and load data
        self.buckets = self.bucket_util.get_all_buckets(self.cluster)
        self.expected_wu = 0
        expected_num_throttled = 0

        batch_size = 100
        for bucket in self.buckets:
            total_items = 0
            for node in bucket.servers:
                throttle_limit = self.bucket_throttling_limit/len(bucket.servers)
                target_vbucket = self.get_active_vbuckets(node, bucket)
                gen_docs = doc_generator("throttling", 0, self.num_items,
                                         doc_size=self.doc_size,
                                         mutation_type="create",
                                         randomize_value=True,
                                         target_vbucket=target_vbucket)

                load_task = self.task.async_load_gen_docs(
                    self.cluster, bucket, gen_docs,
                    DocLoading.Bucket.DocOps.CREATE, 0,
                    batch_size=batch_size, process_concurrency=8,
                    replicate_to=self.replicate_to, persist_to=self.persist_to,
                    durability=self.durability_level,
                    compression=self.sdk_compression,
                    timeout_secs=self.sdk_timeout,
                    sdk_client_pool=self.sdk_client_pool,
                    print_ops_rate=False)
                self.task_manager.get_task_result(load_task)
                self.key_size, doc_size = self.get_size_of_doc(gen_docs)
                self.sleep(30)
                write_units = self.bucket_util.calculate_units(self.key_size, doc_size,
                                    durability=self.durability_level) * batch_size
                throttle_limit, expected_num_throttled = self.calculate_expected_num_throttled(
                                                        node, self.bucket, throttle_limit,
                                                        write_units, expected_num_throttled)
                self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                             self.cluster.buckets)
                num_throttled, ru, wu = self.get_stat(bucket)
                items = self.bucket_util.get_total_items_bucket(bucket) - total_items
                self.expected_wu += self.bucket_util.calculate_units(self.key_size, doc_size,
                                        durability=self.durability_level) * items
                self.assertEqual(wu, self.expected_wu)
                if 0 < num_throttled < expected_num_throttled:
                    self.fail("num_throlled value %s expected_num_throttled %s"
                                  %(num_throttled, expected_num_throttled))
                expected_num_throttled = num_throttled
                total_items = items

    def test_throttling_steady_state(self):
        """
        Test Focus: Check WU and RU count after
                    different doc ops
        STEPS:
          -- Create n items
          -- Validate n items
          -- Check for WU and RU count
          -- Different doc ops(passed from test conf)
             and validate wu and ru count
        """
        self.diff_throttle_limit = self.input.param("diff_throttle_limit", False)
        self.buckets = self.bucket_util.get_all_buckets(self.cluster)
        if self.diff_throttle_limit:
            throttle_limit = [1000, 10000, -1, 100, 2000, 3000, 500, 3333, 5000]
        else:
            throttle_limit = [5000]
        for bucket in self.buckets:
            self.bucket_util.set_throttle_limit(bucket, random.choice(throttle_limit))
        gen_docs = doc_generator("throttling", 0, self.num_items,
                                 doc_size=self.doc_size,
                                 mutation_type="create",
                                 randomize_value=True)
        tasks = self.data_load(gen_docs)
        for load_task in tasks:
            self.task_manager.get_task_result(load_task)
        self.sleep(30)
        self.key_size, doc_size = self.get_size_of_doc(gen_docs)

        expected_wu = self.bucket_util.calculate_units(self.key_size, doc_size,
                                                       durability=self.durability_level) * self.num_items

        for bucket in self.cluster.buckets:
            num_throttled, self.ru, self.wu = self.bucket_util.get_stat_from_metrics(bucket)
            self.log.info("num_throttled %s, ru %s, wu %s" %(num_throttled, self.ru, self.wu))
            msg = "bucket = {}, expected_wu {} != prometheus wu {}".format(bucket.name, expected_wu, self.wu)
            self.assertEqual(self.wu, expected_wu, msg)

    def test_throttling_different_load(self):
        """
        create 10 buckets, get vbucket of each node and load to the node
        sleep for 5 seconds, load the second node
        sleep for 5 seconds , load the third node
        make sure load will have min 5000cu
        """
        self.buckets = self.bucket_util.get_all_buckets(self.cluster)
        self.loop = self.input.param("loop", 1)
        self.load_single_node = self.input.param("load_single_node", False)
        self.num_non_throttled_bucket = self.input.param("num_non_throttle_bucket", 1)
        self.expected_stats = self.bucket_util.get_initial_stats(self.cluster.buckets)
        if self.load_single_node:
            self.loop = len(self.buckets[0].servers)

        for bucket in self.buckets:
            if self.num_non_throttled_bucket > 1:
                self.bucket_util.set_throttle_limit(bucket, -1)
                self.num_non_throttled_bucket -=1
            else:
                self.bucket_util.set_throttle_limit(bucket, self.bucket_throttling_limit)

        for i in range(self.loop):
            tasks = []
            key = "throttling" + str(i)
            self.log.info("loop %s" %i)
            doc_size = 0
            for bucket in self.buckets:
                if i > 2:
                   server = random.choice(bucket.servers)
                else:
                    server = bucket.servers[i]
                target_vbuckets = self.get_active_vbuckets(server, bucket)
                self.gen_docs = doc_generator(key, 0, self.num_items,
                                         doc_size=self.doc_size,
                                         mutation_type="create",
                                         randomize_value=True,
                                         target_vbucket=target_vbuckets)

                load_task = self.task.async_load_gen_docs(
                    self.cluster, bucket, self.gen_docs,
                    DocLoading.Bucket.DocOps.CREATE, 0,
                    batch_size=self.batch_size, process_concurrency=8,
                    replicate_to=self.replicate_to, persist_to=self.persist_to,
                    durability=self.durability_level,
                    compression=self.sdk_compression,
                    timeout_secs=self.sdk_timeout,
                    sdk_client_pool=self.sdk_client_pool,
                    print_ops_rate=False)
                tasks.append(load_task)

            for load_task in tasks:
                self.task_manager.get_task_result(load_task)
            self.key_size, doc_size = self.get_size_of_doc(self.gen_docs)

            for bucket in self.cluster.buckets:
                actual_items_loaded = self.check_actual_items(self.num_items,
                                                       self.expected_stats[bucket.name]["total_items"],
                                                       bucket)
                num_throttled, self.ru, self.wu = self.bucket_util.get_stat_from_metrics(bucket)
                self.expected_stats[bucket.name]["wu"] += \
                    self.bucket_util.calculate_units(self.key_size, doc_size,
                    durability=self.durability_level) * actual_items_loaded
                self.assertEqual(self.wu, self.expected_stats[bucket.name]["wu"])
                self.expected_stats[bucket.name]["total_items"] += actual_items_loaded

    def test_throttling_read_write(self):
        """
        create 10 buckets, load to all of then
        perform read  on half of the buckets
        perform write on half of the buckets
        validate throrrle_limit as well the ru/wu incurred
        also the throttling stats
        """

        self.buckets = self.bucket_util.get_all_buckets(self.cluster)
        self.loop = self.input.param("loop", 1)
        self.load_single_node = self.input.param("load_single_node", False)
        self.expected_stats = self.bucket_util.get_initial_stats(self.cluster.buckets)

        if self.load_single_node:
            self.loop = len(self.buckets[0].servers)
        self.gen_docs = doc_generator("throttling", 0, self.num_items,
                                      doc_size=self.doc_size,
                                      mutation_type="create",
                                      randomize_value=True)
        task_info = self.data_load(self.gen_docs)
        for task in task_info:
            self.task_manager.get_task_result(task)
        self.key_size, doc_size = self.get_size_of_doc(self.gen_docs)

        for bucket in self.cluster.buckets:
            self.expected_stats[bucket.name]["total_items"] = \
                self.check_actual_items(self.num_items,
                                        self.expected_stats[bucket.name]["total_items"],
                                        bucket)
            self.expected_stats[bucket.name]["num_throttled"], \
                self.expected_stats[bucket.name]["ru"], self.wu = \
                self.bucket_util.get_stat_from_metrics(bucket)
            self.expected_stats[bucket.name]["wu"] = \
                self.bucket_util.calculate_units(self.key_size, doc_size) * \
                self.expected_stats[bucket.name]["total_items"]
            self.assertEqual(self.wu, self.expected_stats[bucket.name]["wu"])

        # get keys of the load and perform read task
        read_bucket = self.buckets[:self.num_buckets/2]
        start = self.num_items
        end = start + self.num_items
        for i in range(self.loop):
            self.log.info("loading docs %s" % i)
            gen_docs = doc_generator("throttling", start, end,
                             doc_size=self.doc_size,
                             mutation_type="create",
                             randomize_value=True)
            task_info = self.data_load(gen_docs)
            for bucket in read_bucket:
                load_task = self.task.async_load_gen_docs(
                    self.cluster, bucket, self.gen_docs,
                    DocLoading.Bucket.DocOps.READ, 0,
                    batch_size=self.batch_size, process_concurrency=1,
                    replicate_to=self.replicate_to, persist_to=self.persist_to,
                    durability=self.durability_level,
                    compression=self.sdk_compression,
                    timeout_secs=self.sdk_timeout,
                    sdk_client_pool=self.sdk_client_pool,
                    print_ops_rate=False)
                self.task_manager.get_task_result(load_task)
            for task in task_info:
                self.task_manager.get_task_result(task)

            for bucket in self.buckets:
                actual_items_loaded = self.check_actual_items(self.num_items,
                                                              self.expected_stats[bucket.name]["total_items"],
                                                              bucket)
                self.expected_stats[bucket.name]["wu"] += \
                    self.bucket_util.calculate_units(self.key_size, doc_size,
                                                     durability=self.durability_level) * actual_items_loaded
                num_throttled, self.ru, self.wu = self.bucket_util.get_stat_from_metrics(bucket)
                self.assertEqual(self.wu, self.expected_stats[bucket.name]["wu"])
                self.expected_stats[bucket.name]["total_items"] += actual_items_loaded
                if bucket in read_bucket:
                    self.expected_stats[bucket.name]["ru"] += \
                        self.bucket_util.calculate_units(self.key_size, doc_size,
                                                         read=True) * self.num_items
                    self.assertEqual(self.ru, self.expected_stats[bucket.name]["ru"])
            start = end
            end = start + self.num_items

    def test_read_throttling(self):
        """
        create lots of buckets
        get vbuckets of all the bucket in a node
        load to those vbuckets, perform heavy read and write to the same node
        """
        """
        create 10 buckets, get vbucket of each node and load to the node
        sleep for 5 seconds, load the second node
        sleep for 5 seconds , load the third node
        make sure load will have min 5000cu
        """
        self.buckets = self.bucket_util.get_all_buckets(self.cluster)
        self.loop = self.input.param("loop", 1)
        self.num_non_throttled_bucket = self.input.param("num_non_throttle_bucket", 1)
        self.expected_stats = self.bucket_util.get_initial_stats(self.cluster.buckets)

        for bucket in self.buckets:
            if self.num_non_throttled_bucket > 1:
                self.bucket_util.set_throttle_limit(bucket, -1)
                self.num_non_throttled_bucket -=1
            else:
                self.bucket_util.set_throttle_limit(bucket, self.bucket_throttling_limit)
        gen_docs = doc_generator("throttling", 0, self.num_items,
                                         doc_size=self.doc_size,
                                         mutation_type="create",
                                         randomize_value=True)

        task = self.data_load(gen_docs)
        for load_task in task:
            self.task_manager.get_task_result(load_task)
        self.key_size, doc_size = self.get_size_of_doc(gen_docs)

        for bucket in self.cluster.buckets:
            self.expected_stats[bucket.name]["total_items"] = \
                self.check_actual_items(self.num_items,
                                        self.expected_stats[bucket.name]["total_items"],
                                        bucket)
            self.expected_stats[bucket.name]["wu"] = \
                self.bucket_util.calculate_units(self.key_size, doc_size) * \
                self.expected_stats[bucket.name]["total_items"]
            num_throttled, self.expected_stats[bucket.name]["ru"], self.wu = \
                self.bucket_util.get_stat_from_metrics(bucket)
            self.assertEqual(self.wu, self.expected_stats[bucket.name]["wu"])

        # validate read throttling
        for i in range(self.loop):
            self.log.info("loop %s" % i)
            task = self.data_load(gen_docs, "read")
            for load_task in task:
                self.task_manager.get_task_result(load_task)

            for bucket in self.cluster.buckets:
                self.expected_stats[bucket.name]["ru"] += \
                    self.bucket_util.calculate_units(self.key_size, doc_size, read=True) * self.num_items
                num_throttled, self.ru, self.wu = self.bucket_util.get_stat_from_metrics(bucket)
                self.assertEqual(self.ru, self.expected_stats[bucket.name]["ru"])
                self.assertEqual(self.wu, self.expected_stats[bucket.name]["wu"])
