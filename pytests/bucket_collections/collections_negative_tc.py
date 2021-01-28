from bucket_collections.collections_base import CollectionBase
from bucket_utils.bucket_ready_functions import BucketUtils
from couchbase_helper.documentgenerator import doc_generator
from sdk_client3 import SDKClient
from Cb_constants import CbServer


class CollectionsNegativeTc(CollectionBase):
    def setUp(self):
        super(CollectionsNegativeTc, self).setUp()
        self.use_default_collection = \
            self.input.param("use_default_collection", False)
        self.bucket = self.bucket_util.buckets[0]
        self.invalid = ["_a", "%%", "a~", "a`", "a!", "a@", "a#", "a$", "a^",
                        "a&", "a*", "a(", "a)", "a=", "a+", "a{", "a}", "a|",
                        "a:", "a;", "a'", "a,", "a<", "a.", "a>", "a?", "a/",
                        "a" * (CbServer.max_collection_name_len+1)]

    def tearDown(self):
        super(CollectionsNegativeTc, self).tearDown()

    def test_create_collection_with_existing_name(self):
        BucketUtils.create_scope(self.cluster.master, self.bucket,
                                 {"name": "scope1"})
        BucketUtils.create_collection(self.cluster.master,
                                      self.bucket,
                                      "scope1",
                                      {"name": "collection1"})
        try:
            BucketUtils.create_collection(self.cluster.master,
                                          self.bucket,
                                          "scope1",
                                          {"name": "collection1"})
        except Exception as e:
            self.log.info("collection creation failed as expected as there was collection1 already")
        else:
            self.fail("collection creation did not fail even when given duplicate")

    def test_create_scope_with_existing_name(self):
        BucketUtils.create_scope(self.cluster.master, self.bucket,
                                 {"name": "scope1"})
        try:
            BucketUtils.create_scope(self.cluster.master, self.bucket,
                                     {"name": "scope1"})
        except Exception as e:
            self.log.info("Scope creation failed as expected as there was scope1 already")
        else:
            self.fail("Scope creation did not fail even when given duplicate")

    def test_delete_default_scope(self):
        try:
            BucketUtils.drop_scope(self.cluster.master, self.bucket, "_default")
        except Exception as e:
            self.log.info("Deafult Scope deletion failed as expected")
        else:
            self.fail("Default scope deletion did not fail")

    def test_delete_nonexistant_collection(self):
        try:
            BucketUtils.drop_collection(self.cluster.master, self.bucket, "sumedh")
        except Exception as e:
            self.log.info("Non existant collection deletion failed as expected")
        else:
            self.fail("deletion of non existing collection did not fail")

    def test_delete_nonexistant_scope(self):
        try:
            BucketUtils.drop_scope(self.cluster.master, self.bucket, "sumedh")
        except Exception as e:
            self.log.info("Non existant collection deletion failed as expected")
        else:
            self.fail("deletion of non existing scope did not fail")

    def test_illegal_collection_name(self):
        BucketUtils.create_scope(self.cluster.master, self.bucket,
                                 {"name": "scope1"})
        for name in self.invalid:
            try:
                BucketUtils.create_collection(self.cluster.master,
                                              self.bucket,
                                              "scope1",
                                              {"name": name})
            except Exception as e:
                self.log.info("Illegal collection name as expected")
            else:
                self.fail("Illegal collection name did not fail")

    def test_illegal_scope_name(self):
        for name in self.invalid:
            try:
                BucketUtils.create_scope(self.cluster.master, self.bucket,
                                         {"name": name})
            except Exception as e:
                self.log.info("Illegal scope name as expected")
            else:
                self.fail("Illegal scope name did not fail")

    def test_more_than_max_collections_single_scope(self):
        BucketUtils.create_scope(self.cluster.master, self.bucket,
                                 {"name": "scope1"})
        # create max collections under single scope
        collects_dict = BucketUtils.create_collections(self.cluster, self.bucket, self.MAX_COLLECTIONS, "scope1")
        actual_count = len(collects_dict)
        if actual_count != self.MAX_COLLECTIONS:
            self.fail("failed to create max number of collections")
        try:
            # create one more than the max allowed
            BucketUtils.create_collections(self.cluster, self.bucket, 1, "scope1")
        except Exception as e:
            self.log.info("Creating more than max collections failed as expected")
        else:
            self.fail("Creating more than max collections did not fail")

    def test_more_than_max_collections_multiple_scopes(self):
        # create max collections across 10 scopes
        BucketUtils.create_scopes(self.cluster, self.bucket, 10, collection_count=120)
        try:
            # create one more collection under a new scope
            BucketUtils.create_scopes(self.cluster, self.bucket, 1, collection_count=1)
        except Exception as e:
            self.log.info("Creating more than max collections failed as expected")
        else:
            self.fail("Creating more than max collections did not fail")

    def test_more_than_max_scopes(self):
        # create max scopes
        scopes_dict = BucketUtils.create_scopes(self.cluster, self.bucket, self.MAX_SCOPES)
        actual_count = len(scopes_dict)
        if actual_count != self.MAX_SCOPES:
            self.fail("failed to create max number of scopes")
        try:
            # create one more than the max allowed
            BucketUtils.create_scopes(self.cluster, self.bucket, 1)
        except Exception as e:
            self.log.info("Creating more than max scopes failed as expected")
        else:
            self.fail("Creating more than max scopes did not fail")

    def test_load_duplicate_key_within_same_collection(self):
        client = SDKClient([self.cluster.master], self.bucket,
                           scope=CbServer.default_scope,
                           collection=CbServer.default_collection,
                           compression_settings=self.sdk_compression)
        result = client.crud("create", "test_key-1", "TestValue")
        if result["status"] is True:
            self.log.info("CRUD succeeded first time")
        result = client.crud("create", "test_key-1", "TestValue")
        if result["status"] is True:
            self.fail("CRUD succeeded second time when it should have not")
        elif result["status"] is False:
            self.log.info("CRUD didn't succeed for duplicate key as expected")

    def test_max_key_size(self):
        if self.use_default_collection:
            self.key_size = 251
            self.collection_name = CbServer.default_collection
        else:
            self.key_size = 247
            self.collection_name = "collection-1"
            BucketUtils.create_collection(self.cluster.master,
                                          self.bucket,
                                          scope_name=CbServer.default_scope,
                                          collection_spec={"name": self.collection_name})
        gen_load = doc_generator("test-max-key-size", 0, 1,
                                 key_size=self.key_size,
                                 vbuckets=self.cluster_util.vbuckets)
        task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_load, "create", self.maxttl,
            batch_size=20,
            persist_to=self.persist_to,
            replicate_to=self.replicate_to,
            durability=self.durability_level,
            pause_secs=5, timeout_secs=self.sdk_timeout,
            retries=self.sdk_retries,
            collection=self.collection_name)
        self.task.jython_task_manager.get_task_result(task)
        if task.fail:
            self.log.info("inserting doc key greater than max key size failed as expected")
        else:
            self.fail("inserting doc key greater than max key size succeeded when it should have failed")
