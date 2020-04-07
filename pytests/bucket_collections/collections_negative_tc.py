from bucket_collections.collections_base import CollectionBase
from bucket_utils.bucket_ready_functions import BucketUtils


class CollectionsNegativeTc(CollectionBase):
    def setUp(self):
        super(CollectionsNegativeTc, self).setUp()
        self.bucket = self.bucket_util.buckets[0]

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
