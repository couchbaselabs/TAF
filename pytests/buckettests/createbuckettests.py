import json
import urllib

from BucketLib.BucketOperations import BucketHelper
from Cb_constants import DocLoading
from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import doc_generator
from custom_exceptions.exception import BucketCreationException
from BucketLib.bucket import Bucket


class CreateBucketTests(BaseTestCase):
    def setUp(self):
        super(CreateBucketTests, self).setUp()

        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.append(self.cluster.master)
        self.cluster_util.print_cluster_stats()

    def tearDown(self):
        super(CreateBucketTests, self).tearDown()

    def test_two_replica(self):
        b_name = 'default'
        bucket_replica = 2
        bucket = Bucket({"name": b_name, "replicaNumber": bucket_replica})
        self.bucket_util.create_bucket(self.cluster, bucket)
        msg = 'create_bucket succeeded but bucket %s does not exist' % b_name
        self.assertTrue(
            self.bucket_util.wait_for_bucket_creation(self.cluster, bucket),
            msg)

    def test_valid_length(self):
        name_len = self.input.param('name_length', 100)
        name = 'a' * name_len
        bucket_replica = 1
        bucket = Bucket({"name": name, "replicaNumber": bucket_replica})
        msg = 'create_bucket succeeded but bucket %s does not exist' % name
        try:
            self.bucket_util.create_bucket(self.cluster, bucket)
            self.assertTrue(
                self.bucket_util.wait_for_bucket_creation(self.cluster,
                                                          bucket),
                msg)
        except BucketCreationException as ex:
            self.log.error(ex)
            self.fail('could not create bucket with valid length')

    def test_valid_bucket_name(self):
        """
        Create all types of bucket (CB/Eph/Memcached)
        """
        bucket_specs = [
            {"cb_bucket_with_underscore": {
                Bucket.bucketType: Bucket.Type.MEMBASE}},
            {"cb.bucket.with.dot": {
                Bucket.bucketType: Bucket.Type.MEMBASE}},
            {"eph_bucket_with_underscore": {
                Bucket.bucketType: Bucket.Type.EPHEMERAL}},
            {"eph.bucket.with.dot": {
                Bucket.bucketType: Bucket.Type.EPHEMERAL}},
        ]
        self.log.info("Creating required buckets")
        for bucket_dict in bucket_specs:
            name, spec = bucket_dict.keys()[0], bucket_dict.values()[0]
            self.bucket_util.create_default_bucket(
                bucket_name=name,
                bucket_type=spec[Bucket.bucketType],
                ram_quota=self.bucket_size, replica=self.num_replicas)

        tasks = list()
        load_gen = doc_generator(self.key, 0, self.num_items)
        self.log.info("Loading %s items to all buckets" % self.num_items)
        for bucket in self.bucket_util.buckets:
            task = self.task.async_load_gen_docs(
                self.cluster, bucket, load_gen,
                DocLoading.Bucket.DocOps.CREATE)
            tasks.append(task)

        for task in tasks:
            self.task_manager.get_task_result(task)

        # Validate doc_items count
        self.log.info("Validating the items on the buckets")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.bucket_util.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)

    def test_invalid_bucket_name(self):
        """
        Create buckets with invalid names
        """
        bucket_helper = BucketHelper(self.cluster.master)
        api = '{0}{1}'.format(bucket_helper.baseUrl, 'pools/default/buckets')
        invalid_names = {
            "_replicator.couch.1":
                "This name is reserved for the internal use.",
            ".delete": "Bucket name cannot start with dot.",
            "[bucket]": "Bucket name can only contain characters in range "
                        "A-Z, a-z, 0-9 as well as underscore, period, "
                        "dash & percent. Consult the documentation."
        }
        init_params = {
            Bucket.name: None,
            Bucket.ramQuotaMB: 256,
            Bucket.replicaNumber: self.num_replicas,
            Bucket.bucketType: self.bucket_type,
            Bucket.priority: Bucket.Priority.LOW,
            Bucket.flushEnabled: Bucket.FlushBucket.DISABLED,
            Bucket.evictionPolicy: self.bucket_eviction_policy,
            Bucket.storageBackend: self.bucket_storage,
            Bucket.conflictResolutionType: Bucket.ConflictResolution.SEQ_NO,
            Bucket.durabilityMinLevel: self.bucket_durability_level}
        for bucket_type in [Bucket.Type.MEMBASE, Bucket.Type.EPHEMERAL,
                            Bucket.Type.MEMCACHED]:
            init_params[Bucket.bucketType] = bucket_type
            for name, error in invalid_names.items():
                init_params[Bucket.name] = name
                params = urllib.urlencode(init_params)
                status, content, _ = bucket_helper._http_request(
                    api, params=params, method=bucket_helper.POST)
                self.assertFalse(status, "Bucket created with name=%s" % name)
                self.assertEqual(json.loads(content)["errors"]["name"], error,
                                 "Invalid error message")
