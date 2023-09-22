import random
from guardrails_base import GuardrailsBase


class BucketGuardrails(GuardrailsBase):
    def setUp(self):
        super(BucketGuardrails, self).setUp()
        self.num_buckets = self.input.param("num_buckets", 1)
        self.bucket_name = self.input.param("bucket_name", None)
        self.bucket_ram_quota = self.input.param("bucket_ram_quota", None)
        self.fragmentation = int(self.input.param("fragmentation", 50))
        self.cpu_cores = self.input.param("cpu_cores", 8)
        self.cores_per_bucket = self.input.param("cores_per_bucket", 0.4)

        self.bucket_util.change_max_buckets(self.cluster.master, 50)
        self.cluster_util.print_cluster_stats(self.cluster)

    def tearDown(self):
        super(BucketGuardrails, self).tearDown()


    def test_max_bucket_guardrail(self):
        buckets_to_create = int(self.cpu_cores / self.cores_per_bucket)
        self.log.info("Max number of buckets allowed in the cluster according "
                      "to the guardrail = {}".format(buckets_to_create))
        buckets_to_create -= len(self.cluster.buckets)

        self.log.info("Creating buckets...")
        buckets_created = self.bucket_util.create_multiple_buckets(
                self.cluster, self.num_replicas,
                bucket_count=buckets_to_create,
                bucket_type=self.bucket_type,
                storage=self.bucket_storage,
                eviction_policy=self.bucket_eviction_policy,
                bucket_name=self.bucket_name,
                ram_quota=self.bucket_ram_quota,
                history_retention_collection_default=self.bucket_collection_history_retention_default,
                history_retention_seconds=self.bucket_dedup_retention_seconds,
                history_retention_bytes=self.bucket_dedup_retention_bytes)
        self.assertTrue(buckets_created, "Unable to create desired buckets")

        self.bucket_util.print_bucket_stats(self.cluster)
        self.log.info("{0} buckets created successfully".format(buckets_to_create))

        try:
            self.bucket_util.create_default_bucket(
                    self.cluster,
                    bucket_type=self.bucket_type,
                    ram_quota=self.bucket_ram_quota,
                    replica=self.num_replicas,
                    storage=self.bucket_storage,
                    eviction_policy=self.bucket_eviction_policy,
                    magma_key_tree_data_block_size=self.magma_key_tree_data_block_size,
                    magma_seq_tree_data_block_size=self.magma_seq_tree_data_block_size,
                    history_retention_collection_default=self.bucket_collection_history_retention_default,
                    history_retention_seconds=self.bucket_dedup_retention_seconds,
                    history_retention_bytes=self.bucket_dedup_retention_bytes)
        except:
            self.log.info("Bucket creation failed as expected")

    def test_create_delete_bucket_guardrail(self):
        buckets_to_create = int(self.cpu_cores / self.cores_per_bucket)
        self.log.info("Max number of buckets allowed in the cluster according "
                      "to the guardrail = {}".format(buckets_to_create))
        buckets_to_create -= len(self.cluster.buckets)

        self.log.info("Creating buckets...")
        buckets_created = self.bucket_util.create_multiple_buckets(
                self.cluster, self.num_replicas,
                bucket_count=buckets_to_create,
                bucket_type=self.bucket_type,
                storage=self.bucket_storage,
                eviction_policy=self.bucket_eviction_policy,
                bucket_name=self.bucket_name,
                ram_quota=self.bucket_ram_quota,
                history_retention_collection_default=self.bucket_collection_history_retention_default,
                history_retention_seconds=self.bucket_dedup_retention_seconds,
                history_retention_bytes=self.bucket_dedup_retention_bytes)
        self.assertTrue(buckets_created, "Unable to create desired buckets")
        self.sleep(10, "Wait for a few seconds after bucket creation")

        self.bucket_util.print_bucket_stats(self.cluster)
        self.log.info("{0} buckets created successfully".format(buckets_to_create))

        self.iterations = self.input.param("iterations", 5)
        count = 0

        while count < self.iterations:
            random_idx = random.randint(0,len(self.cluster.buckets))
            bucket_to_delete = self.cluster.buckets[random_idx]

            res = self.bucket_util.delete_bucket(self.cluster, bucket_to_delete)
            self.assertTrue(res, "Bucket deletion failed")
            self.log.info("Bucket {0} deleted".format(bucket_to_delete.name))

            try:
                bucketName = "default-" + str(count+1)
                self.bucket_util.create_default_bucket(
                        self.cluster, bucket_type=self.bucket_type,
                        ram_quota=self.bucket_ram_quota, replica=self.num_replicas,
                        storage=self.bucket_storage,
                        bucket_name=bucketName,
                        eviction_policy=self.bucket_eviction_policy,
                        magma_key_tree_data_block_size=self.magma_key_tree_data_block_size,
                        magma_seq_tree_data_block_size=self.magma_seq_tree_data_block_size,
                        history_retention_collection_default=self.bucket_collection_history_retention_default,
                        history_retention_seconds=self.bucket_dedup_retention_seconds,
                        history_retention_bytes=self.bucket_dedup_retention_bytes)
                self.log.info("Bucket {0} created successfully".format(bucketName))
            except:
                self.fail("Creation of new bucket failed")
            self.log.info("Iteration {0} done".format(count+1))
            count += 1

    def test_create_bucket_with_max_collections(self):

        max_collections = self.bucket_ram_quota
        if self.bucket_ram_quota > 1000:
            max_collections = 1000

        self.bucket = self.cluster.buckets[0]
        curr_collections = 0

        scope_prefix = "scope"
        for i in range(10):
            scope_name = scope_prefix + "-" + str(i+1)
            scope_dict = {"name": scope_name}
            self.log.info("Creating scope {}".format(scope_name))
            self.bucket_util.create_scope(self.cluster.master,
                                          self.bucket,
                                          scope_dict)
            self.sleep(5)

        scope_names = list(self.bucket.scopes.keys())
        scope_names.remove("_system")
        scope_coll_list = []

        coll_prefix = "collection"
        while curr_collections < max_collections:
            random_scope = random.choice(scope_names)
            coll_name = coll_prefix + str(curr_collections)
            scope_coll_list.append([random_scope, coll_name])
            self.log.info("Creating collection {0} in scope {1}".format(coll_name,
                                                                        random_scope))
            self.bucket_util.create_collection(self.cluster.master, self.bucket,
                                                random_scope, {"name": coll_name})
            curr_collections += 1

        try:
            self.log.info("Creating another collection...")
            coll_name = "new_collection"
            self.bucket_util.create_collection(self.cluster.master, self.bucket,
                                               random_scope, {"name": coll_name})
            self.fail("Creation of a collection was successful even after"
                          " the max collection count has exceeded")
        except:
            self.log.info("Creation of collection failed as expected")

        iterations = 5
        new_coll_prefix = "new_collection"
        self.log.info("Dropping and creating collections...")
        for i in range(iterations):
            random_scope_col = random.choice(scope_coll_list)
            self.log.info("Dropping collection {0} in scope {1}".format(random_scope_col[1],
                                                                        random_scope_col[0]))
            self.bucket_util.drop_collection(self.cluster.master, self.bucket,
                                             random_scope_col[0], random_scope_col[1])
            scope_coll_list.remove(random_scope_col)
            self.sleep(2)
            new_coll_name = new_coll_prefix + str(i+1)
            self.bucket_util.create_collection(self.cluster.master, self.bucket,
                                               random_scope_col[0], {"name": new_coll_name})
            self.log.info("Creating new collection {0} in scope {1}".format(new_coll_name,
                                                                        random_scope_col[0]))
            self.sleep(2)
