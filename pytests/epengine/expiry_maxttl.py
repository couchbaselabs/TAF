from couchbase_helper.documentgenerator import doc_generator
from basetestcase import BaseTestCase
from BucketLib.BucketOperations import BucketHelper


class ExpiryMaxTTL(BaseTestCase):
    def setUp(self):
        super(ExpiryMaxTTL, self).setUp()
        self.key = 'test_ttl_docs'.rjust(self.key_size, '0')

        if self.target_vbucket and type(self.target_vbucket) is not list:
            self.target_vbucket = [self.target_vbucket]

        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend([self.cluster.master]+nodes_init)
        self.bucket_util.create_default_bucket(
            bucket_type=self.bucket_type, maxTTL=self.maxttl,
            replica=self.num_replicas, compression_mode=self.compression_mode)
        self.bucket_util.add_rbac_user()
        self.bucket_util.get_all_buckets()
        self.bucket_helper_obj = BucketHelper(self.cluster.master)
        self.log.info("==========Finished ExpiryMaxTTL base setup========")

    def _load_json(self, bucket, num_items, exp=0):
        self.log.info("Creating doc_generator..")
        doc_create = doc_generator(
            self.key, 0, num_items, doc_size=self.doc_size,
            doc_type="json", target_vbucket=self.target_vbucket,
            vbuckets=self.vbuckets)
        self.log.info("doc_generator created")
        task = self.task.async_load_gen_docs(
            self.cluster, bucket, doc_create, "create", exp,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
            compression=self.sdk_compression)
        self.task.jython_task_manager.get_task_result(task)
        self.bucket_util.verify_stats_all_buckets(self.num_items)
        self.bucket_util._wait_for_stats_all_buckets()
        return

    def test_maxttl_lesser_doc_expiry(self):
        """
         A simple test to create a bucket with maxTTL and
         check whether new creates with greater exp are
         deleted when maxTTL has lapsed
        :return:
        """
        for bucket in self.bucket_util.buckets:
            self._load_json(bucket, self.num_items, exp=self.maxttl+500)
        self.sleep(self.maxttl, "Waiting for docs to expire as per maxTTL")
        self.expire_pager(self.cluster.servers)
        self.sleep(20, "Waiting for item count to come down...")
        for bucket in self.bucket_util.buckets:
            items = self.bucket_helper_obj.get_active_key_count(bucket.name)
            self.log.info("Doc expiry {0}s, maxTTL {1}s, "
                          "after {2}s, item count {3}"
                          .format(self.maxttl+500, self.maxttl,
                                  self.maxttl, items))
            if items > 0:
                self.fail("Bucket maxTTL of {0} is not honored"
                          .format(self.maxttl))
            else:
                self.log.info("SUCCESS: Doc expiry={0}s, maxTTL {1}s, "
                              "after {2}s, item count {3}"
                              .format(int(self.maxttl)+500, self.maxttl,
                                      self.maxttl, items))

    def test_maxttl_greater_doc_expiry(self):
        """
        maxTTL is set to 200s in this test,
        Docs have lesser TTL.
        :return:
        """
        for bucket in self.buckets:
            self._load_json(bucket, self.num_items, exp=int(self.maxttl)-100)
        self.sleep(self.maxttl-100, "Waiting for docs to expire as per maxTTL")
        self.expire_pager(self.servers)
        self.sleep(20, "Waiting for item count to come down...")
        for bucket in self.buckets:
            items = self.bucket_helper_obj.get_active_key_count(bucket.name)
            self.log.info("Doc expiry={0}s, maxTTL={1}s, "
                          "after {2}s, item count={3}"
                          .format(int(self.maxttl) - 100, self.maxttl-100,
                                  self.maxttl-100, items))
            if items == 0:
                self.log.info("SUCCESS: Docs with lesser expiry deleted")
            else:
                self.fail("FAIL: Doc with lesser expiry still exists past ttl")

    def test_set_maxttl_on_existing_bucket(self):
        """
        1. Create a bucket with no max_ttl
        2. Upload 1000 docs with exp = 100s
        3. Set maxTTL on bucket as 60s
        4. After 60s, run expiry pager, get item count, must be 1000
        5. After 40s, run expiry pager again and get item count, must be 0
        6. Now load another set of docs with exp = 100s
        7. Run expiry pager after 60s and get item count, must be 0
        """
        for bucket in self.buckets:
            self._load_json(bucket, self.num_items, exp=100)
        self.bucket_util.update_all_bucket_maxTTL(maxttl=60)

        self.sleep(60, "waiting before running expiry pager...")
        self.expire_pager(self.servers)
        self.sleep(20, "waiting for item count to come down...")
        for bucket in self.buckets:
            items = self.bucket_helper_obj.get_active_key_count(bucket.name)
            self.log.info("Doc expiry set to = 100s, maxTTL = 60s "
                          "(set after doc creation), after 60s, item count={0}"
                          .format(items))
            if items != self.num_items:
                self.fail("FAIL: Items with larger expiry before "
                          "maxTTL updation deleted!")

        self.sleep(40, "Waiting before running expiry pager...")
        self.expire_pager(self.servers)
        self.sleep(20, "Waiting for item count to come down...")
        for bucket in self.buckets:
            items = self.bucket_helper_obj.get_active_key_count(bucket.name)
            self.log.info("Doc expiry set to = 100s, maxTTL = 60s"
                          "(set after doc creation), after 100s,"
                          " item count = {0}".format(items))
            if items != 0:
                self.fail("FAIL: Items with not greater expiry set before "
                          "maxTTL updation not deleted after elapsed TTL!")
        for bucket in self.buckets:
            self._load_json(bucket, self.num_items, exp=100)

        self.sleep(60, "waiting before running expiry pager...")
        self.expire_pager(self.servers)
        self.sleep(20, "waiting for item count to come down...")
        for bucket in self.buckets:
            items = self.bucket_helper_obj.get_active_key_count(bucket.name)
            self.log.info("Doc expiry set to = 100s, maxTTL = 60s, after 100s,"
                          " item count = {0}".format(items))
            if items != 0:
                self.fail("FAIL: Items with not greater expiry not "
                          "deleted after elapsed maxTTL!")

    def test_maxttl_possible_values(self):
        """
        Test
        1. min - 0
        2. max - 2147483647q
        3. default - 0
        4. negative values, date, string
        """
        # default
        default_bucket = self.bucket_util.buckets[0]
        self.bucket_util.get_bucket_from_cluster(default_bucket)
        default_maxttl = default_bucket.maxTTL
        if default_maxttl != 0:
            self.fail("FAIL: default maxTTL if left unset must be 0 but is {0}"
                      .format(default_maxttl))
        self.log.info("Verified: default maxTTL if left unset is {0}"
                      .format(default_maxttl))

        # max value
        try:
            self.bucket_util.update_all_bucket_maxTTL(maxttl=2147483648)
        except Exception as e:
            self.log.info("Expected exception : {0}".format(e))
            try:
                self.bucket_util.update_all_bucket_maxTTL(maxttl=2147483647)
            except Exception as e:
                self.fail("Unable to set maxTTL=2147483647 (max permitted)")
            else:
                self.log.info("Verified: Max value permitted is 2147483647")
        else:
            self.fail("Able to set maxTTL greater than 2147483647")

        # min value
        try:
            self.bucket_util.update_all_bucket_maxTTL(maxttl=0)
        except Exception as e:
            self.fail("Unable to set maxTTL=0, the min permitted value")
        else:
            self.log.info("Verified: Min value permitted is 0")

        # negative value
        try:
            self.bucket_util.update_all_bucket_maxTTL(maxttl=-60)
        except Exception as e:
            self.log.info("Verified: negative values denied, exception: {0}"
                          .format(e))
        else:
            self.fail("FAIL: Able to set a negative maxTTL")

        # date/string
        try:
            self.bucket_util.update_all_bucket_maxTTL(maxttl="12/23/2016")
        except Exception as e:
            self.log.info("Verified: string not permitted, exception : {0}"
                          .format(e))
        else:
            self.fail("FAIL: Able to set a date string maxTTL")

    def test_update_maxttl(self):
        """
        1. Create a bucket with ttl = 200s
        2. Upload 1000 docs with exp = 100s
        3. Update ttl = 40s
        4. After 40s, run expiry pager again and get item count, must be 1000
        5. After 60s, run expiry pager again and get item count, must be 0
        6. Now load another set of docs with exp = 100s
        7. Run expiry pager after 40s and get item count, must be 0
        """
        for bucket in self.buckets:
            self._load_json(bucket, self.num_items, exp=100)
        self.bucket_util.update_all_bucket_maxTTL(maxttl=40)

        self.sleep(40, "waiting before running expiry pager...")
        self.expire_pager(self.servers)
        self.sleep(20, "waiting for item count to come down...")
        for bucket in self.buckets:
            items = self.bucket_helper_obj.get_active_key_count(bucket.name)
            self.log.info("Doc expiry=100s, maxTTL during doc creation = 200s"
                          " updated maxttl=40s, after 40s item count = {0}"
                          .format(items))
            if items != self.num_items:
                self.fail("FAIL: Updated ttl affects docs with larger expiry "
                          "before updation!")

        self.sleep(60, "waiting before running expiry pager...")
        self.expire_pager(self.servers)
        self.sleep(20, "waiting for item count to come down...")
        for bucket in self.buckets:
            items = self.bucket_helper_obj.get_active_key_count(bucket.name)
            self.log.info("Doc expiry=100s, maxTTL during doc creation=200s"
                          " updated maxttl=40s, after 100s item count = {0}"
                          .format(items))
            if items != 0:
                self.fail("FAIL: Docs with 100s as expiry before "
                          "maxTTL updation still alive!")

    def test_maxttl_with_doc_updates(self):
        """
        1. Create a bucket with ttl = 60s
        2. Upload 1000 docs with exp = 40s
        3. After 20s, Update docs with exp = 60s
        4. After 40s, run expiry pager again and get item count, must be 1000
        5. After 20s, run expiry pager again and get item count, must be 0
        """
        for bucket in self.buckets:
            self._load_json(bucket, self.num_items, exp=40)

        self.sleep(20, "waiting to update docs with exp=60s...")

        for bucket in self.buckets:
            self._load_json(bucket, self.num_items, exp=60)

        self.sleep(40, "waiting before running expiry pager...")
        self.expire_pager(self.servers)
        for bucket in self.buckets:
            items = self.bucket_helper_obj.get_active_key_count(bucket.name)
            self.log.info("Items: {0}".format(items))
            if items != self.num_items:
                self.fail("FAIL: Docs with updated expiry deleted unexpectedly")

        self.sleep(20, "waiting before running expiry pager...")
        self.expire_pager(self.servers)
        self.sleep(20, "waiting for item count to come down...")
        for bucket in self.buckets:
            items = self.bucket_helper_obj.get_active_key_count(bucket.name)
            self.log.info("Items: {0}".format(items))
            if items != 0:
                self.fail("FAIL: Docs with updated expiry not deleted after "
                          "new exp has elapsed!")
