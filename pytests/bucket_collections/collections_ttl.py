from couchbase_helper.documentgenerator import doc_generator
from bucket_collections.collections_base import CollectionBase
from Cb_constants import CbServer
import copy


class CollectionsTTL(CollectionBase):
    def setUp(self):
        super(CollectionsTTL, self).setUp()
        self.load_gen = doc_generator(self.key, 0, self.num_items)
        self.bucket = self.cluster.buckets[0]
        self.bucket_util._expiry_pager(self.cluster)
        self.remaining_docs = self.input.param("remaining_docs", 0)
        self.update_max_ttl = self.input.param("update_max_ttl", False)
        self.max_allowed_ttl = 2147483647

    def test_collections_ttl(self):
        wait_time = 200
        if self.update_max_ttl:
            self.update_maxTTL_mutliple_times(self.bucket)
            wait_time += 25
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", exp=0,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope="scope1",
            collection="collection_1")

        self.bucket_util._expiry_pager(self.cluster)
        # Validate the bucket doc count is '0' after drop collection
        val_status, items = self.wait_time_validation_of_docs_ttl(wait_time, num_docs=self.remaining_docs)
        if not val_status:
            self.fail("collections_ttl is not working as expected. Num docs : {0}".format(items))

    def test_collections_ttl_with_doc_expiry_set(self):
        wait_time = 125
        if self.update_max_ttl:
            self.update_maxTTL_mutliple_times(self.bucket)
            wait_time += 25
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", exp=500,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope="scope1",
            collection="collection_1")

        self.bucket_util._expiry_pager(self.cluster)
        # Validate the bucket doc count is '0' after drop collection
        val_status, items = self.wait_time_validation_of_docs_ttl(wait_time, num_docs=self.remaining_docs)
        if not val_status:
            self.fail("doc_ttl value was considered instead of the collections_ttl. Num docs : {0}".format(items))

    def test_collections_ttl_with_update_docs_and_doc_expiry_set(self):
        if self.update_max_ttl:
            self.bucket_util.update_ttl_for_collections(self.cluster, self.bucket, ttl_value=86400)
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
        self.bucket_util._expiry_pager(self.cluster)
        # Validate the bucket doc count is '0' after drop collection
        val_status, items = self.wait_time_validation_of_docs_ttl(120, num_docs=self.remaining_docs)
        if not val_status:
            self.fail("collections_ttl value was considered instead of the doc_ttl. Num docs : {0}".format(items))

    def test_collections_ttl_greater_than_doc_expiry(self):
        self.bucket_util._expiry_pager(self.cluster, val=1)
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", exp=1,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope="scope1",
            collection="collection_2")
        # Validate the bucket doc count is '0' after doc expiry timeout
        val_status, items = self.wait_time_validation_of_docs_ttl(wait_time=15, num_docs=self.remaining_docs,
                                                                  time_interval=5)
        if not val_status:
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
        self.bucket_util._expiry_pager(self.cluster)
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
        self.bucket_util._expiry_pager(self.cluster)
        # Validate the bucket doc count is '0' after drop collection
        val_status, items = self.wait_time_validation_of_docs_ttl(200, num_docs=self.num_items)
        if not val_status:
            self.fail("bucket_ttl value was considered instead of the collection_ttl. Num docs : {0}".format(items))
        val_status, items = self.wait_time_validation_of_docs_ttl(400, num_docs=self.remaining_docs)
        if not val_status:
            self.fail("bucket_ttl value was considered instead of the collection_ttl. Num docs : {0}".format(items))

    def test_collections_ttl_lesser_than_doc_expiry(self):
        wait_time = 120
        if self.update_max_ttl:
            self.update_maxTTL_mutliple_times(self.bucket)
            wait_time += 25
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", exp=400,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope="scope1",
            collection="collection_2")

        self.bucket_util._expiry_pager(self.cluster)
        # Validate the bucket doc count is '0' after drop collection
        val_status, items = self.wait_time_validation_of_docs_ttl(wait_time, num_docs=0)
        if not val_status:
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
        self.bucket_util._expiry_pager(self.cluster)
        # Validate the bucket doc count is '0' after drop collection
        val_status, items = self.wait_time_validation_of_docs_ttl(20, num_docs=self.remaining_docs,
                                                                  time_interval=5)
        if not val_status:
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
        self.bucket_util._expiry_pager(self.cluster)
        # Validate the bucket doc count is '0' after drop collection
        self.sleep(200, "waiting for maxTTL to complete")
        items = self.bucket_helper_obj.get_active_key_count("default")
        if items != self.remaining_docs:
            self.fail("collections_ttl is not working as expected. Num docs : {0}".format(items))

    def test_collections_ttl_with_doc_update(self):
        final_wait_time = 120
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", exp=0,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope="scope1",
            collection="collection_1")
        self.bucket_util._expiry_pager(self.cluster)
        self.sleep(60, "Sleep for sometime before we update")
        for i in xrange(1, 5):
            if self.update_max_ttl:
                self.update_maxTTL_mutliple_times(self.bucket)
                final_wait_time += 25
            update_load_gen = copy.deepcopy(self.load_gen)
            self.task.load_gen_docs(
                self.cluster, self.bucket, update_load_gen, "update", exp=0,
                batch_size=10, process_concurrency=8,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                scope="scope1",
                collection="collection_1")
            val_status, items = self.wait_time_validation_of_docs_ttl(60, num_docs=self.num_items,
                                                                      time_interval=20)
            if not val_status:
                self.fail("update of the docs had no effect on collections_ttl. Num docs : {0}".format(items))
        # Validate the bucket doc count is '0' after drop collection
        val_status, items = self.wait_time_validation_of_docs_ttl(final_wait_time, num_docs=self.remaining_docs,
                                                                  time_interval=20)
        if not val_status:
            self.fail("update of the docs had no effect on collections_ttl. Num docs : {0}".format(items))

    def test_collections_ttl_lesser_than_bucket_ttl(self):
        wait_time = 120
        if self.update_max_ttl:
            self.update_maxTTL_mutliple_times(self.bucket)
            wait_time += 25
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", exp=0,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope="scope1",
            collection="collection_1")

        self.bucket_util._expiry_pager(self.cluster)
        # Validate the bucket doc count is '0' after drop collection
        val_status, items = self.wait_time_validation_of_docs_ttl(wait_time, num_docs=self.remaining_docs)
        if not val_status:
            self.fail("bucket_ttl had priority than collection_ttl when it was larger. Num docs : {0}".format(items))

    def test_collections_ttl_with_few_defined_and_few_inherited_from_bucket_ttl(self):
        self.bucket_util._expiry_pager(self.cluster)
        # Validate the bucket doc count is '0' after drop collection
        val_status, items = self.wait_time_validation_of_docs_ttl(120, num_docs=self.remaining_docs)
        if not val_status:
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

        if self.update_max_ttl:
            self.bucket_util.update_ttl_for_collections(self.cluster, self.bucket, ttl_value=86400)
        self.bucket_util._expiry_pager(self.cluster)
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
        val_status, items = self.wait_time_validation_of_docs_ttl(120, num_docs=self.remaining_docs)
        if not val_status:
            self.fail("update of the docs to had no effect on collections_ttl. Num docs : {0}".format(items))

    def test_collections_ttl_max_possible_values(self):
        self.bucket_util.create_collection(self.cluster.master,
                                      self.bucket,
                                      "scope1",
                                      {"name": "collection_3", "maxTTL" : self.max_allowed_ttl})
        try:
            self.bucket_util.create_collection(self.cluster.master,
                                               self.bucket,
                                               "scope1",
                                               {"name": "collection_4", "maxTTL": self.max_allowed_ttl+1})
        except Exception as e:
            self.log.info("collection creation failed as expected as maxTTL was > {0}".format(self.max_allowed_ttl))
        else:
            self.fail("collection creation did not fail even when maxTTL was > {0}".format(self.max_allowed_ttl))
        try:
            self.bucket_util.create_collection(self.cluster.master,
                                               self.bucket,
                                               "scope1",
                                               {"name": "collection_5", "maxTTL": -1})
        except Exception as e:
            self.log.info("collection creation failed as expected as maxTTL was < 0")
        else:
            self.fail("collection creation did not fail even when maxTTL was < 0")

        if self.update_max_ttl:
            self.bucket_util.update_ttl_for_collections(self.cluster, self.bucket, ttl_value=self.max_allowed_ttl)
            self.bucket_util.update_ttl_for_collections(self.cluster, self.bucket, ttl_value=self.max_allowed_ttl,
                                                        enable_ttl=True)
            for scope_name in self.bucket.scopes:
                if scope_name == "_system":
                    continue
                for coll_name in self.bucket.scopes[scope_name].collections:
                    col_ttl = self.bucket.scopes[scope_name].collections[coll_name].maxTTL
                    if col_ttl != self.max_allowed_ttl:
                        self.fail("collection maxTTL update to {0} failed".format(self.max_allowed_ttl))

            status = self.bucket_util.set_maxTTL_for_collection(self.cluster.master,
                                                                self.bucket,
                                                                CbServer.default_scope,
                                                                CbServer.default_collection,
                                                                maxttl=self.max_allowed_ttl+1)
            if status is False:
                self.log.info("Collection maxTTL update to {0} failed as expected".format(
                                                                            self.max_allowed_ttl+1))
            else:
                self.fail("Collection TTL update did not fail even when maxTTL was > {0}".format(
                                                                            self.max_allowed_ttl))

            status = self.bucket_util.set_maxTTL_for_collection(self.cluster.master,
                                                                self.bucket,
                                                                "scope1",
                                                                "collection_1",
                                                                maxttl=-1)
            if status is False:
                self.log.info("Collection maxTTL update to -1 failed as expected")
            else:
                self.fail("Collection TTL update did not fail even when maxTTL was < 0")

    def test_collections_ttl_delete_recreate_collections(self):
        self.bucket = self.cluster.buckets[0]
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

    def test_collections_ttl_after_update_docs_with_doc_ttl(self):
        if self.update_max_ttl:
            self.bucket_util.update_ttl_for_collections(self.cluster, self.bucket, ttl_value=86400)
            self.bucket_util.update_ttl_for_collections(self.cluster, self.bucket, ttl_value=86400, enable_ttl=True)
        self.bucket_util._expiry_pager(self.cluster)
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", exp=0,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope=CbServer.default_scope,
            collection=CbServer.default_collection)
        self.log.info("Updating the docs with doc ttl")
        update_load_gen1 = copy.deepcopy(self.load_gen)
        self.task.load_gen_docs(
            self.cluster, self.bucket, update_load_gen1, "update", exp=100,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope=CbServer.default_scope,
            collection=CbServer.default_collection)
        val_status, items = self.wait_time_validation_of_docs_ttl(120, num_docs=self.remaining_docs)
        if not val_status:
            self.fail("collection ttl value was considered instead of doc ttl value.")

    def test_collections_ttl_after_initially_setting_as_0(self):
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

        # Validate the bucket doc count is '0' after drop collection
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.validate_doc_count_as_per_collections(
            self.cluster, self.bucket)

        self.bucket_util.update_ttl_for_collections(self.cluster, self.bucket, ttl_value=100,
                                                    enable_ttl=True)

        self.bucket_util._expiry_pager(self.cluster, val=1)

        val_status, items = self.wait_time_validation_of_docs_ttl(120, num_docs=self.num_items)
        if not val_status:
            self.fail("New collection ttl value was inherited by the existing docs")

        # Update the existing docs
        update_load_gen = copy.deepcopy(self.load_gen)
        self.task.load_gen_docs(
            self.cluster, self.bucket, update_load_gen, "update", exp=0,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope=CbServer.default_scope,
            collection=CbServer.default_collection)

        val_status, items = self.wait_time_validation_of_docs_ttl(120, num_docs=0)
        if not val_status:
            self.fail("New collection ttl value was not inherited by the exisiting docs \
                                                after the update")

    def test_collections_ttl_with_non_0_and_then_setting_as_0(self):
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", exp=self.maxttl,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope="scope1",
            collection="collection_1")

        self.bucket_util.disable_ttl_for_collections(self.cluster, self.bucket)

        # Update the existing docs
        update_load_gen = copy.deepcopy(self.load_gen)
        self.task.load_gen_docs(
            self.cluster, self.bucket, update_load_gen, "update", exp=0,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope="scope1",
            collection="collection_1")

        self.bucket_util._expiry_pager(self.cluster)
        val_status, items = self.wait_time_validation_of_docs_ttl(120, num_docs=self.num_items)
        if not val_status:
            self.fail("New collection ttl value=0 was not inherited by the collection \
                      after the update")

    def test_ttl_update_for_collections_in_system_scope(self):
        for scope_name in self.bucket.scopes:
            if scope_name == "_system":
                for coll_name in self.bucket.scopes[scope_name].collections:
                    status = self.bucket_util.set_maxTTL_for_collection(self.cluster.master,
                                                                        self.bucket,
                                                                        scope_name,
                                                                        coll_name,
                                                                        maxttl=300)
                    if status is False:
                        self.log.info("Updating collection maxTTL value for "
                                      "{0} {1} failed as expected".format(scope_name, coll_name))
                    else:
                        self.fail("Updating collection maxTTL value did not fail "
                                  "for {0} {1}".format(scope_name, coll_name))

    def update_maxTTL_mutliple_times(self, bucket, inc_iter=10, dec_iter=5, ttl_delta=5):
        for scope in bucket.scopes:
            for col in bucket.scopes[scope].collections:
                if bucket.scopes[scope].collections[col].maxTTL > 0:
                    coll_name = col
                    scope_name = scope
                    break
        curr_ttl = bucket.scopes[scope_name].collections[coll_name].maxTTL
        self.log.info("Updating maxTTL value multiple times...")
        # Increasing the maxTTL value by ttl_delta in each iteration
        for i in range(inc_iter):
            curr_ttl += ttl_delta
            self.log.info("Updating collection maxTTL value to {0}".format(curr_ttl))
            self.bucket_util.update_ttl_for_collections(self.cluster, bucket,
                                                        ttl_value=curr_ttl)

        curr_ttl = bucket.scopes[scope_name].collections[coll_name].maxTTL
        # Decreasing the maxTTL value by ttl_delta in each iteration
        for i in range(dec_iter):
            curr_ttl -= ttl_delta
            self.log.info("Updating collection maxTTL value to {0}".format(curr_ttl))
            self.bucket_util.update_ttl_for_collections(self.cluster, bucket,
                                                        ttl_value=curr_ttl)

    def wait_time_validation_of_docs_ttl(self, wait_time, num_docs, time_interval=20):
        self.log.info("Validating expiry of docs dynamically")
        check_time = 0
        status = False

        while check_time <= wait_time:
            items_in_bucket = self.bucket_helper_obj.get_active_key_count("default")
            if items_in_bucket == num_docs:
                status = True
            check_time += time_interval
            self.sleep(time_interval)

        return status, items_in_bucket