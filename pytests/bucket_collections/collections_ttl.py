from couchbase_helper.documentgenerator import doc_generator
from bucket_collections.collections_base import CollectionBase
from Cb_constants import CbServer


class CollectionsTTL(CollectionBase):
    def setUp(self):
        super(CollectionsTTL, self).setUp()
        self.load_gen = doc_generator(self.key, 0, self.num_items)
        self.bucket = self.bucket_util.buckets[0]
        self.bucket_util._expiry_pager()

    def test_collections_ttl(self):
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", self.maxttl,
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

    def test_collections_ttl_with_doc_expiry_set(self):
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", 300,
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

    def test_collections_ttl_greater_than_doc_expiry(self):
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", 300,
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

    def test_collections_ttl_greater_than_scope_ttl(self):
        # TODO: API is not available to set scope ttl. run it here
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", 300,
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
            self.cluster, self.bucket, self.load_gen, "create", self.maxttl,
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
            self.cluster, self.bucket, self.load_gen, "create", self.maxttl,
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
            self.cluster, self.bucket, self.load_gen, "create", self.maxttl,
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
            self.cluster, self.bucket, self.load_gen, "create", 300,
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
            self.cluster, self.bucket, self.load_gen, "create", self.maxttl,
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
            self.cluster, self.bucket, self.load_gen, "create", self.maxttl,
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
