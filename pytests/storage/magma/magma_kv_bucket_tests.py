from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from storage.magma.magma_base import MagmaBaseTest


class MagmaKVTests(MagmaBaseTest):
    def setUp(self):
        super(MagmaKVTests, self).setUp()

    def tearDown(self):
        super(MagmaKVTests, self).tearDown()

    def test_create_max_buckets_with_min_ram_quota(self):

        self.bucket_util.print_bucket_stats(self.cluster)

        if len(self.cluster.buckets) == self.standard_buckets:
            self.log.info("{0} {1} buckets created successfully with RAM Quota {2} MB".format(
                                                self.standard_buckets, self.bucket_storage,
                                                self.bucket_ram_quota))

        total_buckets = self.cluster.buckets
        new_buckets_count = len(total_buckets) // 2

        self.log.info("Deleting half the buckets...")
        for i in range(len(total_buckets)//2):
            bucket_obj = total_buckets[i]
            self.bucket_util.delete_bucket(self.cluster, bucket_obj)
            self.log.info("Bucket {0} deleted".format(bucket_obj.name))

        self.sleep(30, "Wait for bucket deletion to get reflected")

        buckets_in_cluster = self.bucket_util.get_all_buckets(self.cluster)

        create_task = None
        self.log.info("Inserting docs into the existing buckets")
        self.create_start = self.num_items
        self.create_end = self.create_start + 100000
        self.generate_docs(doc_ops="create")
        create_task = self.data_load()

        self.log.info("Creating {0} buckets...".format(new_buckets_count))
        buckets_creation_task = self.bucket_util.create_multiple_buckets(
                                            self.cluster,
                                            self.num_replicas,
                                            bucket_count=new_buckets_count,
                                            bucket_type=self.bucket_type,
                                            storage=self.bucket_storage,
                                            eviction_policy=self.bucket_eviction_policy,
                                            bucket_name="new_bucket",
                                            ram_quota=self.bucket_ram_quota)

        self.assertTrue(buckets_creation_task, "Unable to create multiple buckets")
        self.wait_for_doc_load_completion(create_task)

        self.bucket_util.print_bucket_stats(self.cluster)

        self.sleep(30, "Wait for newly created bucktes to get reflected")
        buckets_in_cluster = self.bucket_util.get_all_buckets(self.cluster)

        if self.load_docs_using == "default_loader":
            self.log.info("Creating SDK clients for the new buckets")
            for bucket in buckets_in_cluster:
                if "new_bucket" in bucket.name:
                    self.cluster.sdk_client_pool.create_clients(
                        self.cluster, bucket,
                        req_clients=1,
                        compression_settings=self.sdk_compression)
        elif self.load_docs_using == "sirius_java_sdk":
            self.log.info("Creating SDK clients in Java side")
            for bucket in self.cluster.buckets:
                if "new_bucket" in bucket.name:
                    SiriusCouchbaseLoader.create_clients_in_pool(
                        self.cluster.master, self.cluster.master.rest_username,
                        self.cluster.master.rest_password,
                        bucket.name, req_clients=1)

        self.log.info("Loading data into all buckets...")
        self.key = "new_docs"
        self.create_start = 0
        self.create_end = 100000
        self.generate_docs(doc_ops="create")
        create_task = self.data_load()
        self.wait_for_doc_load_completion(create_task)

        self.bucket_util.print_bucket_stats(self.cluster)

        self.log.info("Deleting the newly created buckets")
        for bucket in buckets_in_cluster:
            if "new_bucket" in bucket.name:
                self.bucket_util.delete_bucket(self.cluster, bucket)
                self.log.info("Bucket {0} deleted".format(bucket.name))

        self.sleep(30, "Wait for bucket deletion to get reflected")
        self.bucket_util.print_bucket_stats(self.cluster)
        buckets_in_cluster = self.bucket_util.get_all_buckets(self.cluster)

        self.log.info("Creating {0} buckets...".format(new_buckets_count))
        buckets_creation_task = self.bucket_util.create_multiple_buckets(
                                            self.cluster,
                                            self.num_replicas,
                                            bucket_count=new_buckets_count,
                                            bucket_type=self.bucket_type,
                                            storage=self.bucket_storage,
                                            eviction_policy=self.bucket_eviction_policy,
                                            bucket_name="new_bucket",
                                            ram_quota=self.bucket_ram_quota)

        self.assertTrue(buckets_creation_task, "Unable to create multiple buckets")
        self.bucket_util.print_bucket_stats(self.cluster)
        buckets_in_cluster = self.bucket_util.get_all_buckets(self.cluster)

        if self.load_docs_using == "default_loader":
            self.log.info("Creating SDK clients for the new buckets")
            for bucket in buckets_in_cluster:
                if "new_bucket" in bucket.name:
                    self.cluster.sdk_client_pool.create_clients(
                        self.cluster, bucket,
                        req_clients=1,
                        compression_settings=self.sdk_compression)
        elif self.load_docs_using == "sirius_java_sdk":
            self.log.info("Creating SDK clients in Java side")
            for bucket in self.cluster.buckets:
                if "new_bucket" in bucket.name:
                    SiriusCouchbaseLoader.create_clients_in_pool(
                        self.cluster.master, self.cluster.master.rest_username,
                        self.cluster.master.rest_password,
                        bucket.name, req_clients=1)

        self.log.info("Loading data into all buckets...")
        self.key = "new_test_docs"
        self.create_start = 0
        self.create_end = 100000
        self.generate_docs(doc_ops="create")
        create_task = self.data_load()
        self.wait_for_doc_load_completion(create_task)
        self.sleep(20)

        self.bucket_util.print_bucket_stats(self.cluster)

        if len(buckets_in_cluster) == self.standard_buckets:
            self.log.info("Bucket count matches with the specified number of standard buckets")
        else:
            self.log_failure("Bucket count mismatch. Expected : {0}, Actual : {1}".format(
                                            self.standard_buckets, len(buckets_in_cluster)))
