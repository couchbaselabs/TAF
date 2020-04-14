from bucket_collections.collections_base import CollectionBase
from bucket_utils.bucket_ready_functions import BucketUtils
from sdk_client3 import SDKClient
from Cb_constants import CbServer


class CollectionsNegativeTc(CollectionBase):
    def setUp(self):
        super(CollectionsNegativeTc, self).setUp()
        self.bucket = self.bucket_util.buckets[0]
        self.invalid = ["_a", "%%", "a~", "a`", "a!", "a@", "a#", "a$", "a^", "a&", "a*", "a(", "a)", "a=", "a+", "a{",
                        "a}", "a|", "a:", "a;", "a'", "a,", "a<", "a.", "a>", "a?", "a/",
                        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]

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
        # Max_collections count, after considering default collection in setup
        max_collections = 1000
        BucketUtils.create_collections(self.cluster, self.bucket, max_collections - 1, "scope1")
        try:
            BucketUtils.create_collections(self.cluster, self.bucket, 500, "scope1")
        except Exception as e:
            self.log.info("Creating more than max collections failed as expected")
        else:
            self.fail("Creating more than max collections did not fail")

    def test_more_than_max_collections_multiple_scopes(self):
        try:
            BucketUtils.create_scopes(self.cluster, self.bucket, 10, collection_count=200)
        except Exception as e:
            self.log.info("Creating more than max collections failed as expected")
        else:
            self.fail("Creating more than max collections did not fail")

    def test_more_than_max_scopes(self):
        # Max_scopes count, after considering default scope in setup
        max_scopes = 1000
        BucketUtils.create_scopes(self.cluster, self.bucket, max_scopes - 1)
        try:
            BucketUtils.create_scopes(self.cluster, self.bucket, 500)
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
