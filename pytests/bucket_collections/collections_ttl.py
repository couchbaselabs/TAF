from couchbase_helper.documentgenerator import doc_generator
from bucket_collections.collections_base import CollectionBase
from Cb_constants import CbServer
import copy


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

    def test_collections_ttl_with_update_docs_and_doc_expiry_set(self):
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", exp=0,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope="scope1",
            collection="collection_1")
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "update", exp=100,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope="scope1",
            collection="collection_1")
        self.bucket_util._expiry_pager()
        # Validate the bucket doc count is '0' after drop collection
        self.sleep(120, "waiting for doc_expiry to complete")
        items = self.bucket_helper_obj.get_active_key_count("default")
        if items != self.remaining_docs:
            self.fail("collections_ttl value was considered instead of the doc_ttl. Num docs : {0}".format(items))

    def test_collections_ttl_greater_than_doc_expiry(self):
        self.bucket_util._expiry_pager(val=1)
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", exp=1,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope="scope1",
            collection="collection_2")
        # Validate the bucket doc count is '0' after doc expiry timeout
        self.sleep(15, "waiting for maxTTL to complete")
        items = self.bucket_helper_obj.get_active_key_count("default")
        if items != 0:
            self.fail("collections_ttl value was considered instead of the doc_ttl. Num docs : {0}".format(items))

    def test_collections_ttl_greater_than_scope_ttl(self):
        # TODO: API is not available to set scope ttl. run it here
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", exp=self.maxttl,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope="scope1",
            collection="collection_2")
        self.bucket_util._expiry_pager()
        # Validate the bucket doc count is '0' after drop collection
        self.sleep(200, "waiting for maxTTL to complete")
        items = self.bucket_helper_obj.get_active_key_count("default")
        if items != self.remaining_docs:
            self.fail("collections_ttl is not working as expected. Num docs : {0}".format(items))

    def test_collections_ttl_greater_than_bucket_ttl(self):
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", exp=self.maxttl,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope="scope1",
            collection="collection_2")
        self.bucket_util._expiry_pager()
        # Validate the bucket doc count is '0' after drop collection
        self.sleep(200, "waiting for bucket maxTTL to complete")
        items = self.bucket_helper_obj.get_active_key_count("default")
        if items != self.num_items:
            self.fail("bucket_ttl value was considered instead of the collection_ttl. Num docs : {0}".format(items))
        self.sleep(400, "waiting for collection maxTTL to complete")
        items = self.bucket_helper_obj.get_active_key_count("default")
        if items != self.remaining_docs:
            self.fail("bucket_ttl value was considered instead of the collection_ttl. Num docs : {0}".format(items))

    def test_collections_ttl_lesser_than_doc_expiry(self):
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", exp=400,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope="scope1",
            collection="collection_2")
        self.bucket_util._expiry_pager()
        # Validate the bucket doc count is '0' after drop collection
        self.sleep(120, "waiting for collection maxTTL to complete")
        items = self.bucket_helper_obj.get_active_key_count("default")
        if items != 0:
            self.fail("collection_ttl value was not considered, instead of the doc_expiry was considered."
                      " Num docs : {0}".format(items))
            
    def test_docs_which_has_bucket_collection_ttl_and_doc_expiry_set(self):
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
        self.sleep(20, "waiting for doc expiry to complete")
        items = self.bucket_helper_obj.get_active_key_count("default")
        if items != self.remaining_docs:
            self.fail("collection_ttl/bucket_ttl value was considered instead of the doc_expiry. Num docs : {0}".format(items))

    def test_collections_ttl_lesser_than_scope_ttl(self):
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", exp=self.maxttl,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope="scope1",
            collection="collection_2")
        self.bucket_util._expiry_pager()
        # Validate the bucket doc count is '0' after drop collection
        self.sleep(200, "waiting for maxTTL to complete")
        items = self.bucket_helper_obj.get_active_key_count("default")
        if items != self.remaining_docs:
            self.fail("collections_ttl is not working as expected. Num docs : {0}".format(items))

    def test_collections_ttl_with_doc_update(self):
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", exp=0,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope="scope1",
            collection="collection_1")
        self.bucket_util._expiry_pager()
        self.sleep(60, "Sleep for sometime before we update")
        for i in xrange(1, 5):
            update_load_gen = copy.deepcopy(self.load_gen)
            self.task.load_gen_docs(
                self.cluster, self.bucket, update_load_gen, "update", exp=0,
                batch_size=10, process_concurrency=8,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                scope="scope1",
                collection="collection_1")
            self.sleep(60, "Sleep for sometime before we update")
            items = self.bucket_helper_obj.get_active_key_count("default")
            if items != self.num_items:
                self.fail("update of the docs had no effect on collections_ttl. Num docs : {0}".format(items))
        # Validate the bucket doc count is '0' after drop collection
        self.sleep(60, "waiting for collections_ttl to complete")
        items = self.bucket_helper_obj.get_active_key_count("default")
        if items != self.remaining_docs:
            self.fail("update of the docs had no effect on collections_ttl. Num docs : {0}".format(items))

    def test_collections_ttl_lesser_than_bucket_ttl(self):
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
        self.sleep(120, "waiting for collections_ttl to complete")
        items = self.bucket_helper_obj.get_active_key_count("default")
        if items != self.remaining_docs:
            self.fail("bucket_ttl had priority than collection_ttl when it was larger. Num docs : {0}".format(items))

    def test_collections_ttl_with_few_defined_and_few_inherited_from_bucket_ttl(self):
        self.bucket_util._expiry_pager()
        # Validate the bucket doc count is '0' after drop collection
        self.sleep(120, "waiting for collections_ttl to complete")
        items = self.bucket_helper_obj.get_active_key_count("default")
        if items != self.remaining_docs:
            self.fail("bucket_ttl had priority than collection_ttl when it was larger. Num docs : {0}".format(items))

    def test_collections_ttl_with_doc_update_sets_expiry_to_0(self):
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", exp=0,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope="scope1",
            collection="collection_1")
        self.bucket_util._expiry_pager()
        self.sleep(60, "Sleep for sometime before we update")
        for i in xrange(1, 5):
            update_load_gen = copy.deepcopy(self.load_gen)
            self.task.load_gen_docs(
                self.cluster, self.bucket, update_load_gen, "update", exp=200,
                batch_size=10, process_concurrency=8,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                scope="scope1",
                collection="collection_1")
            update_load_gen1 = copy.deepcopy(self.load_gen)
            self.task.load_gen_docs(
                self.cluster, self.bucket, update_load_gen1, "update", exp=0,
                batch_size=10, process_concurrency=8,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                scope="scope1",
                collection="collection_1")
            self.sleep(100, "Sleep for sometime before we update")
        # Validate the bucket doc count is '0' after drop collection
        self.sleep(100, "waiting for collections_ttl to complete")
        items = self.bucket_helper_obj.get_active_key_count("default")
        if items != self.remaining_docs:
            self.fail("update of the docs to had no effect on collections_ttl. Num docs : {0}".format(items))

    def test_collections_ttl_max_possible_values(self):
        self.bucket = self.bucket_util.buckets[0]
        self.bucket_util.create_collection(self.cluster.master,
                                      self.bucket,
                                      "scope1",
                                      {"name": "collection_3", "maxTTL" : 2147483647})
        try:
            self.bucket_util.create_collection(self.cluster.master,
                                               self.bucket,
                                               "scope1",
                                               {"name": "collection_4", "maxTTL": 2147483648})
        except Exception as e:
            self.log.info("collection creation failed as expected as maxTTL was >  2147483647")
        else:
            self.fail("collection creation did not fail even when maxTTL was >  2147483647")
        try:
            self.bucket_util.create_collection(self.cluster.master,
                                               self.bucket,
                                               "scope1",
                                               {"name": "collection_5", "maxTTL": -1})
        except Exception as e:
            self.log.info("collection creation failed as expected as maxTTL was < 0")
        else:
            self.fail("collection creation did not fail even when maxTTL was < 0")

    def test_collections_ttl_delete_recreate_collections(self):
        self.bucket = self.bucket_util.buckets[0]
        for scope in ["scope1", "scope2"]:
            for collection in ["collection_1","collection_2"]:
                self.bucket_util.drop_collection(self.cluster.master,
                                                   self.bucket,
                                                   scope, collection)

        for scope in ["scope1", "scope2"]:
            for collection in ["collection_1", "collection_2"]:
                self.bucket_util.create_collection(self.cluster.master,
                                                   self.bucket,
                                                   scope,
                                                   {"name": collection})

        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", exp=0,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope="scope1",
            collection="collection_1")
        self.sleep(150, "sleep until old collection_ttl")
        items = self.bucket_helper_obj.get_active_key_count("default")
        if items != self.num_items:
            self.fail("old collection_ttl value was inherited. Num docs : {0}".format(items))

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
