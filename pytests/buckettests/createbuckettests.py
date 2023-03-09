import json
import urllib

from BucketLib.BucketOperations import BucketHelper
from Cb_constants import DocLoading, CbServer
from basetestcase import ClusterSetup
from couchbase_helper.documentgenerator import doc_generator
from custom_exceptions.exception import BucketCreationException
from BucketLib.bucket import Bucket


class CreateBucketTests(ClusterSetup):
    def setUp(self):
        super(CreateBucketTests, self).setUp()

    def tearDown(self):
        super(CreateBucketTests, self).tearDown()

    def test_two_replica(self):
        name = 'default'
        replica_number = 2
        bucket = Bucket({"name": name, "replicaNumber": replica_number})
        self.bucket_util.create_bucket(self.cluster, bucket)
        msg = 'create_bucket succeeded but bucket %s does not exist' % name
        self.assertTrue(
            self.bucket_util.wait_for_bucket_creation(self.cluster, bucket),
            msg)

    def test_valid_length(self):
        name_len = self.input.param('name_length', 100)
        name = 'a' * name_len
        replica_number = 1
        bucket = Bucket({"name": name, "replicaNumber": replica_number})
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
                self.cluster, bucket_name=name,
                bucket_type=spec[Bucket.bucketType],
                ram_quota=self.bucket_size, replica=self.num_replicas)

        tasks = list()
        load_gen = doc_generator(self.key, 0, self.num_items)
        self.log.info("Loading %s items to all buckets" % self.num_items)
        for bucket in self.cluster.buckets:
            task = self.task.async_load_gen_docs(
                self.cluster, bucket, load_gen,
                DocLoading.Bucket.DocOps.CREATE)
            tasks.append(task)

        for task in tasks:
            self.task_manager.get_task_result(task)

        # Validate doc_items count
        self.log.info("Validating the items on the buckets")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
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

    def test_invalid_params(self):
        """
        Create with unsupported param and validate the error.
        - vbuckets
        """
        def create_bucket(width=None, weight=None, num_vb=None):
            init_params.pop(Bucket.width, None)
            init_params.pop(Bucket.weight, None)
            init_params.pop(Bucket.numVBuckets, None)
            if width is not None:
                init_params[Bucket.width] = width
            if weight is not None:
                init_params[Bucket.weight] = weight
            if num_vb is not None:
                init_params[Bucket.numVBuckets] = num_vb

            status, content, _ = bucket_helper._http_request(
                api, params=urllib.urlencode(init_params),
                method=bucket_helper.POST)
            self.assertFalse(status, "Bucket created successfully")
            self.log.critical("%s" % content)
            return json.loads(content)["errors"]

        bucket_helper = BucketHelper(self.cluster.master)
        api = '{0}{1}'.format(bucket_helper.baseUrl, 'pools/default/buckets')
        init_params = {
            Bucket.name: "default",
            Bucket.ramQuotaMB: 256,
            Bucket.replicaNumber: self.num_replicas,
            Bucket.bucketType: self.bucket_type,
            Bucket.priority: Bucket.Priority.LOW,
            Bucket.flushEnabled: Bucket.FlushBucket.DISABLED,
            Bucket.evictionPolicy: self.bucket_eviction_policy,
            Bucket.storageBackend: self.bucket_storage,
            Bucket.conflictResolutionType: Bucket.ConflictResolution.SEQ_NO,
            Bucket.durabilityMinLevel: self.bucket_durability_level}

        # error = create_bucket(width=1)
        # error = create_bucket(weight=1)
        # error = create_bucket(weight=0)

        error = create_bucket(num_vb=CbServer.total_vbuckets)
        self.assertEqual(
            error["numVbuckets"],
            "Support for variable number of vbuckets is not enabled",
            "Invalid error message")

    def test_create_collections_validate_history_stat(self):
        """
        1. Create a default bucket
        2. Create multiple collections in the bucket
        3. Validate history stats
        Ref - MB-55555
        """
        bucket_name = "default"
        num_collections = 5

        self.bucket_storage = Bucket.StorageBackend.couchstore

        self.create_bucket(self.cluster,bucket_name=bucket_name)
        self.log.info("Bucket with name : {0} "
                      "type : {1} "
                      "replicas : {2} "
                      "storage : couchstore "
                      "created"
                      .format(bucket_name,
                              self.bucket_type,
                              self.num_replicas))

        for i in range(num_collections):
            collection_name = "collection_{0}".format(i)
            self.bucket_util.create_collection(self.cluster.master,
                                               self.cluster.buckets[0],
                                               CbServer.default_scope,
                                               {"name":collection_name})
            self.log.info("Collection {0} created".format(collection_name))

        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        result = self.bucket_util.validate_history_retention_settings(self.cluster.master,
                                                                      self.cluster.buckets[0])
        self.assertTrue(result,
                        "History field in stats could not be validated")
        self.log.info("History field in stats validated successfully")
