import copy

from Cb_constants import CbServer, DocLoading
from bucket_collections.collections_base import CollectionBase
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection


class CollectionsTTL(CollectionBase):
    def setUp(self):
        super(CollectionsTTL, self).setUp()
        self.load_gen = doc_generator(self.key, 0, self.num_items)
        self.bucket = self.cluster.buckets[0]
        self.bucket_util._expiry_pager(self.cluster)
        self.remaining_docs = self.input.param("remaining_docs", 0)
        self.update_max_ttl = self.input.param("update_max_ttl", False)

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
        max_ttl_plus_1 = CbServer.max_ttl_seconds+1
        self.log.info("Testing TTL with upper range for collection")
        try:
            self.bucket_util.create_collection(
                self.cluster.master, self.bucket,
                "scope1", {"name": "collection_3",
                           "maxTTL": max_ttl_plus_1})
        except Exception:
            self.log.debug("Collection not created with TTL={}"
                           .format(max_ttl_plus_1))
        else:
            self.fail("Collection created with maxTTL={0}"
                      .format(max_ttl_plus_1))

        self.log.info("Testing TTL with negative range for collection")
        try:
            self.bucket_util.create_collection(
                self.cluster.master, self.bucket,
                "scope1", {"name": "collection_3", "maxTTL": -2})
        except Exception:
            self.log.info("Collection creation failed with maxTTL < -1")
        else:
            self.fail("Collection creation succeeded with maxTTL < -1")

        self.bucket_util.create_collection(
            self.cluster.master, self.bucket,
            "scope1", {"name": "collection_3",
                       "maxTTL": CbServer.max_ttl_seconds})

        if self.update_max_ttl:
            self.log.info("Updating collection ttl to max and validate")
            for _, scope in self.bucket.scopes.items():
                if scope.name == "_system":
                    continue
                for _, col in scope.collections.items():
                    self.bucket_util.set_maxTTL_for_collection(
                        self.cluster.master, self.bucket,
                        scope.name, col.name, maxttl=CbServer.max_ttl_seconds)

            cbstat = Cbstats(self.cluster.master).get_collections(self.bucket)
            for scope, collections in cbstat.items():
                if scope == CbServer.system_scope \
                        or not isinstance(collections, dict):
                    continue
                for col_name, col in collections.items():
                    self.assertEqual(col["maxTTL"], CbServer.max_ttl_seconds,
                                     "Collection maxTTL update to {} failed"
                                     .format(CbServer.max_ttl_seconds))

            status = self.bucket_util.set_maxTTL_for_collection(
                self.cluster.master, self.bucket, CbServer.default_scope,
                CbServer.default_collection, maxttl=max_ttl_plus_1)
            if status:
                self.fail("Collection TTL update did not fail with maxTTL={0}"
                          .format(CbServer.max_ttl_seconds))

            status = self.bucket_util.set_maxTTL_for_collection(
                self.cluster.master, self.bucket, "scope1", "collection_1",
                maxttl=-2)
            if status:
                self.fail("Collection TTL update didn't fail with maxTTL < -1")

    def test_doc_preserve_ttl(self):
        """
        References:
        MB-58693: Preserve TTL when modifying document using REST API
        MB-58664: TTL is not set when using Upsert(explicit TTL + preserveTTL)
                  to create a new document under full-eviction
        """
        bucket = self.cluster.buckets[0]
        self.log.info("Creating custom scope and collections")
        self.bucket_util.create_scope(self.cluster.master, bucket,
                                      {"name": "s1"})
        self.bucket_util.create_collection(
            self.cluster.master, bucket, "s1", {"name": "c1"})
        self.bucket_util.create_collection(
            self.cluster.master, bucket, "s1", {"name": "c2", "maxTTL": 0})
        self.bucket_util.create_collection(
            self.cluster.master, bucket, "s1", {"name": "c3", "maxTTL": 60})

        scope_col_list = [
            (CbServer.default_scope, CbServer.default_collection),
            ("s1", "c1"),
            ("s1", "c2"),
            ("s1", "c3")]

        rest = RestConnection(self.cluster.master)
        client = self.sdk_client_pool.get_client_for_bucket(bucket)

        preserve_ttl_key = "doc_preserve_expiry_with_ttl"
        for scope, col in scope_col_list:
            self.log.info("Loading docs in to {}:{}".format(scope, col))
            client.select_collection(scope, col)

            # MB-58664
            client.crud(DocLoading.Bucket.DocOps.UPDATE, preserve_ttl_key, {},
                        preserve_expiry=True, exp=60)
            result = client.read(preserve_ttl_key, populate_value=False,
                                 with_expiry=True)
            self.assertTrue(result["ttl_present"], "TTL not present")

            # MB-58693
            result = client.crud(DocLoading.Bucket.DocOps.CREATE, "key_1", {})
            self.assertTrue(result["status"], "Create 'key_1' failed")
            for key in ["key_2", "key_3"]:
                result = client.crud(DocLoading.Bucket.DocOps.CREATE, key, {},
                                     exp=40)
                self.assertTrue(result["status"],
                                "Create {} with ttl=40 failed".format(key))

        # MB-58693
        self.sleep(30, "Wait before upserting docs using REST")
        for scope, col in scope_col_list:
            self.log.info("Loading docs in to {}:{}".format(scope, col))
            client.select_collection(scope, col)
            # Retains default TTL values (ttl=0)
            self.assertTrue(rest.upsert_doc(bucket, "key_1", scope, col),
                            "Rest upsert failed")
            # Overrides TTL value for docs (TTL N -> 0)
            self.assertTrue(rest.upsert_doc(bucket, "key_2", scope, col),
                            "Rest upsert failed")
            # TTL is preserved wrt the doc
            self.assertTrue(rest.upsert_doc(bucket, "key_3", scope, col,
                                            preserve_ttl="true"),
                            "Rest upsert failed")

        self.sleep(20, "Wait for docs to go past initial ttl time")
        for scope, col in scope_col_list:
            self.log.info("Validating docs in {}:{}".format(scope, col))
            client.select_collection(scope, col)

            # Default TTL doc validation
            res = client.read("key_1", populate_value=False)
            self.assertTrue(res["status"], "Read 'key_1' failed: %s" % res)

            # TTL doc with preserveTTL set to false, so the TTL got reset
            res = client.read("key_2", populate_value=False)
            self.assertTrue(res["status"], "Read 'key_2' failed: %s" % res)

            # Doc with preserveTTL, so the doc expired with old TTL value
            res = client.read("key_3")
            self.assertFalse(res["status"], "Read 'key_3' succeeded: %s" % res)

        self.log.info("Releasing SDK client")
        client.select_collection(CbServer.default_scope,
                                 CbServer.default_collection)
        self.sdk_client_pool.release_client(client)

    def test_collections_inherit_bucket_ttl(self):
        """
        MB-57924
        """
        def load_docs_into_each_collection():
            for t_scope, t_col in scope_col_list:
                self.log.info("Loading doc into {}:{}".format(t_scope, t_col))
                client.select_collection(t_scope, t_col)
                r = client.upsert(key, {}, durability=self.durability_level)
                self.log.debug("Upsert %s:%s :: %s" % (t_scope, t_col, r["cas"]))
                self.assertTrue(r["status"], "Insert failed")

        def validate_expired_docs(cols_with_expired_doc):
            for t_scope, t_col in scope_col_list:
                client.select_collection(t_scope, t_col)
                col_name = "{}:{}".format(t_scope, t_col)
                r = client.read(key)
                self.log.debug("Validate %s:%s :: %s" % (t_scope, t_col, r["cas"]))
                if t_col in cols_with_expired_doc:
                    self.assertFalse(r["status"],
                                     "{}: Read succeeded".format(col_name))
                else:
                    self.assertTrue(r["status"],
                                    "{}: Read failed".format(col_name))
        bucket = self.cluster.buckets[0]
        self.log.info("Creating custom scope and collections")
        self.bucket_util.create_scope(self.cluster.master, bucket,
                                      {"name": "s1"})
        self.bucket_util.create_collection(
            self.cluster.master, bucket, "s1", {"name": "c1"})
        self.bucket_util.create_collection(
            self.cluster.master, bucket, "s1", {"name": "c2", "maxTTL": 0})
        self.bucket_util.create_collection(
            self.cluster.master, bucket, "s1", {"name": "c3", "maxTTL": 60})
        self.bucket_util.create_collection(
            self.cluster.master, bucket, "s1", {"name": "c4", "maxTTL": -1})
        scope_col_list = [
            (CbServer.default_scope, CbServer.default_collection),
            ("s1", "c1"),
            ("s1", "c2"),
            ("s1", "c3"),
            ("s1", "c4")]

        key = "test_doc"
        client = self.sdk_client_pool.get_client_for_bucket(bucket)

        self.log.info("Load and validate with bucket_ttl=0 and collection TTL")
        load_docs_into_each_collection()
        self.sleep(65, "Wait for doc to expire")
        self.log.info("Reading docs to validate doc status")
        validate_expired_docs(["c3"])

        self.log.info("Load and validate with bucket_ttl=10 + collection TTL")
        self.bucket_util.update_bucket_property(self.cluster.master,
                                                bucket, max_ttl=10)
        self.sleep(5, "Wait for ttl to get reflected")
        load_docs_into_each_collection()
        self.sleep(15, "Wait for doc to expire")
        self.log.info("Reading docs to validate doc status")
        validate_expired_docs(["_default", "c1", "c2"])
        self.sleep(50, "Wait for doc to expire on collection with TTL")
        validate_expired_docs(["_default", "c1", "c2", "c3"])

        self.log.info("Dropping custom created collections")
        for scope, col in scope_col_list[1:]:
            self.bucket_util.drop_collection(self.cluster.master, bucket,
                                             scope, col)

        self.log.info("Updating bucket ttl=20")
        self.bucket_util.update_bucket_property(self.cluster.master,
                                                bucket, max_ttl=20)
        self.sleep(5, "Wait for ttl to get reflected")

        self.log.info("Recreate dropped collections")
        self.bucket_util.create_collection(
            self.cluster.master, bucket, "s1", {"name": "c1"})
        self.bucket_util.create_collection(
            self.cluster.master, bucket, "s1", {"name": "c2", "maxTTL": 0})
        self.bucket_util.create_collection(
            self.cluster.master, bucket, "s1", {"name": "c3", "maxTTL": 60})
        self.bucket_util.create_collection(
            self.cluster.master, bucket, "s1", {"name": "c4", "maxTTL": -1})

        self.log.info("Validate doc expiry with bucket_ttl=20 + col TTL")
        load_docs_into_each_collection()
        self.sleep(15, "Wait before no expiry will be triggered and validate")
        validate_expired_docs(list())
        self.sleep(10, "Wait for bucket ttl to get trigger")
        validate_expired_docs(["_default", "c1", "c2"])
        self.sleep(40, "Wait for doc to expire on collection with TTL")
        validate_expired_docs(["_default", "c1", "c2", "c3"])

        self.log.info("Releasing SDK client")
        client.select_collection(CbServer.default_scope,
                                 CbServer.default_collection)
        self.sdk_client_pool.release_client(client)

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
            self.cluster, self.bucket, self.load_gen, "create",
            exp=self.maxttl, batch_size=10, process_concurrency=8,
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

        for _, scope in self.bucket.scopes.items():
            if scope.name == CbServer.system_scope:
                continue
            for _, col in scope.collections.items():
                self.bucket_util.set_maxTTL_for_collection(
                    self.cluster.master, self.bucket,
                    scope.name, col.name, maxttl=100)

        self.bucket_util._expiry_pager(self.cluster, val=1)
        self.sleep(5, "Wait for item_pager to run")

        exp_items = self.bucket.scopes[CbServer.default_scope] \
            .collections[CbServer.default_collection].num_items
        curr_items = self.bucket_helper_obj.get_active_key_count("default")
        self.assertEqual(int(curr_items), exp_items,
                         "Updated collection ttl inherited by existing docs")

        # Update the existing docs
        self.task.load_gen_docs(
            self.cluster, self.bucket, self.load_gen,
            DocLoading.Bucket.DocOps.UPDATE,
            batch_size=10, process_concurrency=8,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope=CbServer.default_scope,
            collection=CbServer.default_collection)

        self.bucket.scopes[CbServer.default_scope] \
            .collections[CbServer.default_collection] \
            .num_items -= self.num_items

        self.sleep(105, "Wait for docs to expire")
        self.bucket_util._expiry_pager(self.cluster, val=1)

        exp_items = self.bucket.scopes[CbServer.default_scope] \
            .collections[CbServer.default_collection].num_items
        val_status, items = self.wait_time_validation_of_docs_ttl(
            120, num_docs=exp_items)
        if not val_status:
            self.fail("New collection ttl value was not inherited by "
                      "the exisiting docs after the update")

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
        items_in_bucket = 0

        while check_time <= wait_time:
            items_in_bucket = self.bucket_helper_obj.get_active_key_count("default")
            if items_in_bucket == num_docs:
                status = True
                break
            check_time += time_interval
            self.sleep(time_interval)

        return status, items_in_bucket
