from couchbase_helper.documentgenerator import doc_generator
from bucket_collections.collections_base import CollectionBase
from Cb_constants import CbServer


class CollectionsTTL(CollectionBase):
    def setUp(self):
        super(CollectionsTTL, self).setUp()
        self.load_gen = doc_generator(self.key, 0, self.num_items)
        self.bucket = self.bucket_util.buckets[0]
        self.bucket_util._expiry_pager()
        self.remaining_docs = self.input.param("remaining_docs", 0)

    def test_collections_ttl(self):
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", exp=0,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope="scope1",
            collection="collection_1")
        self.bucket_util._expiry_pager()
        # Validate the bucket doc count is '0' after drop collection
        self.sleep(200, "waiting for maxTTL to complete")
        items = self.bucket_helper_obj.get_active_key_count("default")
        if items != self.remaining_docs:
            self.fail("collections_ttl is not working as expected. Num docs : {0}".format(items))

    def test_collections_ttl_with_doc_expiry_set(self):
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", exp=500,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope="scope1",
            collection="collection_1")
        self.bucket_util._expiry_pager()
        # Validate the bucket doc count is '0' after drop collection
        self.sleep(100, "waiting for maxTTL to complete")
        items = self.bucket_helper_obj.get_active_key_count("default")
        if items != self.num_items:
            self.fail("collections_ttl value was considered instead of the doc_ttl. Num docs : {0}".format(items))
        self.sleep(500, "waiting for maxTTL to complete")
        items = self.bucket_helper_obj.get_active_key_count("default")
        if items != 0:
            self.fail("collections_ttl value was considered instead of the doc_ttl. Num docs : {0}".format(items))

    def test_collections_ttl_greater_than_doc_expiry(self):
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", exp=2,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope="scope1",
            collection="collection_2")
        self.bucket_util._expiry_pager()
        # Validate the bucket doc count is '0' after drop collection
        self.sleep(15, "waiting for maxTTL to complete")
        items = self.bucket_helper_obj.get_active_key_count("default")
        if items != 0:
            self.fail("collections_ttl value was considered instead of the doc_ttl. Num docs : {0}".format(items))

    def test_collections_ttl_greater_than_scope_ttl(self):
        # TODO: API is not available to set scope ttl. run it here
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", exp=300,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope=CbServer.default_scope,
            collection=CbServer.default_collection)
        self.bucket.scopes[CbServer.default_scope] \
            .collections[CbServer.default_collection] \
            .num_items += self.num_items
        self.bucket_util._expiry_pager()
        # Validate the bucket doc count is '0' after drop collection
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_doc_count_as_per_collections(self.bucket)
        self.validate_test_failure()

    def test_collections_ttl_greater_than_bucket_ttl(self):
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", exp=self.maxttl,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope=CbServer.default_scope,
            collection=CbServer.default_collection)
        self.bucket.scopes[CbServer.default_scope] \
            .collections[CbServer.default_collection] \
            .num_items += self.num_items
        self.bucket_util._expiry_pager()
        # Validate the bucket doc count is '0' after drop collection
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_doc_count_as_per_collections(self.bucket)
        self.validate_test_failure()

    def test_collections_ttl_lesser_than_doc_expiry(self):
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", exp=self.maxttl,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope=CbServer.default_scope,
            collection=CbServer.default_collection)
        self.bucket.scopes[CbServer.default_scope] \
            .collections[CbServer.default_collection] \
            .num_items += self.num_items
        self.bucket_util._expiry_pager()
        # Validate the bucket doc count is '0' after drop collection
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_doc_count_as_per_collections(self.bucket)
        self.validate_test_failure()

    def test_collections_ttl_lesser_than_scope_ttl(self):
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", exp=self.maxttl,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope=CbServer.default_scope,
            collection=CbServer.default_collection)
        self.bucket.scopes[CbServer.default_scope] \
            .collections[CbServer.default_collection] \
            .num_items += self.num_items
        self.bucket_util._expiry_pager()
        # Validate the bucket doc count is '0' after drop collection
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_doc_count_as_per_collections(self.bucket)
        self.validate_test_failure()

    def test_collections_ttl_lesser_than_bucket_ttl(self):
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", exp=300,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope=CbServer.default_scope,
            collection=CbServer.default_collection)
        self.bucket.scopes[CbServer.default_scope] \
            .collections[CbServer.default_collection] \
            .num_items += self.num_items
        self.bucket_util._expiry_pager()
        # Validate the bucket doc count is '0' after drop collection
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_doc_count_as_per_collections(self.bucket)
        self.validate_test_failure()

    def test_collections_ttl_after_initially_setting_as_0(self):
        # TODO: Rest api to modify the maxTTL for collections is not yet in
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", exp=self.maxttl,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope=CbServer.default_scope,
            collection=CbServer.default_collection)
        self.bucket.scopes[CbServer.default_scope] \
            .collections[CbServer.default_collection] \
            .num_items += self.num_items
        self.bucket_util._expiry_pager()
        # Validate the bucket doc count is '0' after drop collection
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_doc_count_as_per_collections(self.bucket)
        self.validate_test_failure()

    def test_collections_ttl_with_non_0_and_then_setting_as_0(self):
        # TODO: Rest api to modify the maxTTL for collections is not yet in
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", exp=self.maxttl,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope=CbServer.default_scope,
            collection=CbServer.default_collection)
        self.bucket.scopes[CbServer.default_scope] \
            .collections[CbServer.default_collection] \
            .num_items += self.num_items
        self.bucket_util._expiry_pager()
        # Validate the bucket doc count is '0' after drop collection
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_doc_count_as_per_collections(self.bucket)
        self.validate_test_failure()
