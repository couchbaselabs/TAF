import random
import string

from Cb_constants import DocLoading
from sdk_client3 import SDKClient
from LMT_base import LMT
from couchbase_helper.documentgenerator import \
    doc_generator

class ServerlessMetering(LMT):
    def setUp(self):
        super(ServerlessMetering, self).setUp()
        self.bucket = self.cluster.buckets[0]
        self.sdk_compression = self.input.param("sdk_compression", False)
        compression_settings = {"enabled": self.sdk_compression}
        self.client = SDKClient([self.cluster.master], self.bucket,
                                compression_settings=compression_settings)

    def tearDown(self):
        self.client.close()
        super(ServerlessMetering, self).tearDown()

    def test_CRUD_throttling(self):
        self.bucket = self.bucket_util.get_all_buckets(self.cluster)[0]
        node = random.choice(self.bucket.servers)
        target_vbucket = self.get_active_vbuckets(node, self.bucket)
        throttle_limit = self.bucket_throttling_limit/len(self.bucket.servers)
        expected_wu = 0
        expected_num_throttled = 0
        write_units = self.calculate_units(self.doc_size, 0)
        self.generate_data_for_vbuckets(target_vbucket)

        for op_type in ["create", "update", "replace"]:
            for key, value in self.key_value.iteritems():
                result = self.client.crud(op_type, key, value,
                                              durability=self.durability_level)
                if throttle_limit == 0 and result["status"] is False:
                    self.log.info("Load failed as expected for throttle limit")
                elif result["status"] is False:
                    self.log.critical("%s Loading failed: %s" % (key, result["error"]))
                self.sleep(1)
                throttle_limit, expected_num_throttled = self.calculate_expected_num_throttled(
                                                        node, self.bucket, throttle_limit,
                                                        write_units, expected_num_throttled)

            self.sleep(10)
            num_throttled, ru, wu = self.get_stat(self.bucket)
            expected_wu += (write_units * self.num_items)
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
        expected_ru = self.calculate_units(self.doc_size, 0, read=True) * self.num_items
        write_units = self.calculate_units(self.total_size, 0)

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
                self.assertFalse(failed_items, "Subdoc Xattr operation failed")
                self.sleep(1)
                throttle_limit, expected_num_throttled = self.calculate_expected_num_throttled(
                                                        node, self.bucket, throttle_limit,
                                                        write_units, expected_num_throttled)

            self.sleep(5)
            num_throttled, ru, wu = self.get_stat(self.bucket)
            expected_wu += write_units * self.num_items
            self.compare_ru_wu_stat(ru, wu, expected_ru, expected_wu)
            expected_ru += (self.calculate_units(self.total_size, 0,
                                                 read=True) * self.num_items)
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

        batch_size = 500
        for bucket in self.buckets:
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
                write_units = self.calculate_units(self.doc_size, 0) * batch_size
                throttle_limit, expected_num_throttled = self.calculate_expected_num_throttled(
                                                        node, self.bucket, throttle_limit,
                                                        write_units, expected_num_throttled)
                self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                             self.cluster.buckets)

                self.sleep(30)
                num_throttled, ru, wu = self.get_stat(bucket)
                self.expected_wu += self.calculate_units(self.doc_size, 0) * self.num_items
                self.assertEqual(wu, self.expected_wu)
                if 0 < num_throttled < expected_num_throttled:
                    self.fail("num_throlled value %s expected_num_throttled %s"
                                  %(num_throttled, expected_num_throttled))
                expected_num_throttled = num_throttled
