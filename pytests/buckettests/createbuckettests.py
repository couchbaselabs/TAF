import copy

from BucketLib.BucketOperations import BucketHelper
from basetestcase import ClusterSetup
from cb_constants import DocLoading, CbServer
from cb_server_rest_util.buckets.buckets_api import BucketRestApi
from couchbase_helper.durability_helper import DurabilityHelper
from membase.api.rest_client import RestConnection
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
        def doc_op(op_type):
            tasks = list()
            self.log.info(f"Perform {op_type} for {self.num_items} items")
            for t_bucket in self.cluster.buckets:
                tasks.append(self.task.async_load_gen_docs(
                    self.cluster, t_bucket, load_gen, op_type,
                    load_using=self.load_docs_using,
                    durability=self.durability_level,
                    process_concurrency=2,
                    suppress_error_table=False,
                    print_ops_rate=False))

            for task in tasks:
                self.task_manager.get_task_result(task)

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
            name = list(bucket_dict.keys())[0]
            spec = list(bucket_dict.values())[0]
            self.bucket_util.create_default_bucket(
                self.cluster, bucket_name=name,
                bucket_type=spec[Bucket.bucketType],
                ram_quota=self.bucket_size, replica=self.num_replicas,
                durability_impossible_fallback=self.durability_impossible_fallback)

        rest = BucketRestApi(self.cluster.master)
        status, buckets_info = rest.get_bucket_info()
        self.assertTrue(status, "Get bucket info failed")
        for bucket in buckets_info:
            expected_val = self.durability_impossible_fallback
            if self.durability_impossible_fallback is None:
                expected_val = "disabled"
            self.assertEqual(bucket["durabilityImpossibleFallback"],
                             expected_val,
                             "Value mismatch")

        load_gen = doc_generator(self.key, 0, self.num_items)
        doc_op(DocLoading.Bucket.DocOps.CREATE)

        self.log.info(f"Validating the item={self.num_items} on the buckets")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)

        doc_op(DocLoading.Bucket.DocOps.UPDATE)
        doc_op(DocLoading.Bucket.DocOps.DELETE)

        verification_dict = dict()
        verification_dict["ops_create"] = self.num_items
        verification_dict["ops_update"] = self.num_items
        verification_dict["ops_delete"] = self.num_items
        verification_dict["rollback_item_count"] = 0
        verification_dict["sync_write_aborted_count"] = 0
        verification_dict["sync_write_committed_count"] = 0

        if self.durability_level not in ["", "NONE", None]:
            verification_dict[
                "sync_write_committed_count"] = self.num_items * 3

        self.log.info("Validating the items on the buckets")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)

        d_helper = DurabilityHelper(
            self.log, self.nodes_init,
            durability=self.durability_level)
        for bucket in self.cluster.buckets:
            failed = d_helper.verify_vbucket_details_stats(
                bucket, self.cluster_util.get_kv_nodes(self.cluster),
                vbuckets=bucket.numVBuckets,
                expected_val=verification_dict)
            if failed:
                self.fail("Cbstat vbucket-details verification failed for "
                          f"{bucket.name}")

        # Validate doc_items count
        self.log.info("Validating the items on the buckets")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.print_bucket_stats(self.cluster)
        self.bucket_util.verify_stats_all_buckets(self.cluster, 0)

    def test_minimum_replica_update_during_replica_update_rebalance(self):
        rest = RestConnection(self.cluster.master)
        minimum_replica = self.input.param("minimum_replica", 2)
        update_setting_during_regression = self.input.param(
            "update_setting_during_regression", True)
        self.num_replicas = 3
        self.create_bucket(self.cluster, bucket_name="3_replica")
        doc_create = doc_generator(self.key, 0, self.num_items,
                                   key_size=self.key_size,
                                   doc_size=self.doc_size,
                                   doc_type=self.doc_type)
        loading_tasks = []
        for bucket in self.cluster.buckets:
            task = self.task.async_load_gen_docs(
                self.cluster, bucket, doc_create, "create", 0,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                timeout_secs=self.sdk_timeout, batch_size=10,
                process_concurrency=8, load_using=self.load_docs_using)
            loading_tasks.append(task)
        for task in loading_tasks:
            self.task.jython_task_manager.get_task_result(task)
        for bucket in self.cluster.buckets:
            self.bucket_util.update_bucket_property(
                self.cluster.master, bucket, replica_number=0)
        if not update_setting_during_regression:
            status, content = rest. \
                set_minimum_bucket_replica_for_cluster(minimum_replica)
            self.assertTrue(status, "minimum replica setting not updated")
        rebalance = self.task.async_rebalance(self.cluster, [], [])
        if update_setting_during_regression:
            self.sleep(5, "waiting for rebalance to start")
            status, content = rest. \
                set_minimum_bucket_replica_for_cluster(minimum_replica)
            self.assertTrue(status, "minimum replica setting not updated")

        self.task.jython_task_manager.get_task_result(rebalance)
        self.assertTrue(rebalance.result, "Rebalance Failed")

    def test_sample_buckets_with_minimum_replica_setting(self):
        rest = RestConnection(self.cluster.master)
        bucket_helper = BucketHelper(self.cluster.master)
        status, content = rest. \
            set_minimum_bucket_replica_for_cluster(3)
        self.assertTrue(status, "minimum replica setting not updated")
        bucket_helper.load_sample("beer-sample")
        self.sleep(5, "waiting for previous sample bucket to gert deployed")
        status, content = rest. \
            set_minimum_bucket_replica_for_cluster(2)
        self.assertTrue(status, "minimum replica setting not updated")
        bucket_helper.load_sample("travel-sample")
        self.sleep(5, "waiting for previous sample bucket to gert deployed")
        status, content = rest. \
            set_minimum_bucket_replica_for_cluster(1)
        self.assertTrue(status, "minimum replica setting not updated")
        bucket_helper.load_sample("gamesim-sample")
        self.sleep(5, "waiting for previous sample bucket to gert deployed")
        self.assertTrue(bucket_helper.bucket_exists("beer-sample"))
        self.assertTrue(bucket_helper.bucket_exists("travel-sample"))
        self.assertTrue(bucket_helper.bucket_exists("gamesim-sample"))

    def test_recreate_bucket(self):
        bucket_helper = BucketHelper(self.cluster.master)
        minimum_replica = self.input.param("minimum_replica", 3)
        rest = RestConnection(self.cluster.master)
        self.num_replicas = 0
        self.create_bucket(self.cluster, bucket_name="0_replica")
        self.num_replicas = 1
        self.create_bucket(self.cluster, bucket_name="1_replica")
        self.num_replicas = 2
        self.create_bucket(self.cluster, bucket_name="2_replica")
        self.num_replicas = 3
        self.create_bucket(self.cluster, bucket_name="3_replica")
        loading_tasks = []
        doc_create = doc_generator(self.key, 0, self.num_items,
                                   key_size=self.key_size,
                                   doc_size=self.doc_size,
                                   doc_type=self.doc_type)
        for bucket in self.cluster.buckets:
            task = self.task.async_load_gen_docs(
                self.cluster, bucket, doc_create, "create", 0,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                timeout_secs=self.sdk_timeout,
                batch_size=10, process_concurrency=8,
                load_using=self.load_docs_using)
            loading_tasks.append(task)
        for task in loading_tasks:
            self.task.jython_task_manager.get_task_result(task)

        status, content = rest.\
            set_minimum_bucket_replica_for_cluster(minimum_replica)
        self.assertTrue(status, "minimum replica setting not updated")

        buckets = copy.copy(self.cluster.buckets)
        for bucket in buckets:
            bucket_recreate_fail = False
            bucket_helper.delete_bucket(bucket)
            try:
                self.num_replicas = bucket.replicaNumber
                self.create_bucket(self.cluster, bucket_name=bucket.name)
            except Exception:
                bucket_recreate_fail = True
            finally:
                if self.num_replicas < minimum_replica:
                    self.assertTrue(bucket_recreate_fail,
                                    "bucket creation expected to fail")
                else:
                    self.assertFalse(bucket_recreate_fail,
                                     "bucket creation was not expected to "
                                     "fail")

    def test_invalid_bucket_name(self):
        """
        Create buckets with invalid names
        """
        bucket_rest = BucketRestApi(self.cluster.master)
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
        for bucket_type in [Bucket.Type.MEMBASE, Bucket.Type.EPHEMERAL]:
            init_params[Bucket.bucketType] = bucket_type
            for name, error in invalid_names.items():
                init_params[Bucket.name] = name
                status, content = bucket_rest.create_bucket(init_params)
                self.assertFalse(status, "Bucket created with name=%s" % name)
                self.assertEqual(content.json()["errors"]["name"], error,
                                 "Invalid error message")

    def test_invalid_params(self):
        """
        Create with unsupported param and validate the error.
        - vbuckets
        - durabilityImpossibleFallback
        """
        def create_bucket(width=None, weight=None, num_vb=None,
                          durability_impossible_fallback=None):
            init_params.pop(Bucket.width, None)
            init_params.pop(Bucket.weight, None)
            init_params.pop(Bucket.numVBuckets, None)
            if width is not None:
                init_params[Bucket.width] = width
            if weight is not None:
                init_params[Bucket.weight] = weight
            if num_vb is not None:
                init_params[Bucket.numVBuckets] = num_vb

            if durability_impossible_fallback is not None:
                init_params[Bucket.durabilityImpossibleFallback] \
                    = durability_impossible_fallback
            status, content = bucket_helper.create_bucket(init_params)
            self.assertFalse(status, "Bucket created successfully")
            content = content.json()
            self.log.critical("%s" % content)
            if num_vb is not None:
                self.assertEqual(
                    content["errors"]["numVBuckets"],
                    "Number of vbuckets must be 128 or 1024 (magma) "
                    "or 1024 (couchstore)",
                    "Invalid error message")
            if durability_impossible_fallback is not None:
                self.assertEqual(
                    content["errors"]["durability_impossible_fallback"],
                    "Durability impossible fallback must be either "
                    "'disabled' or 'fallbackToActiveAck'",
                    "Mismatch in error message of d_impossible_fallback")

        bucket_helper = BucketRestApi(self.cluster.master)
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
        create_bucket(num_vb=int(CbServer.total_vbuckets)-1)

        # Valid durability_impossible_fallback = disabled / fallbackToActiveAck
        create_bucket(durability_impossible_fallback="NONE")
        create_bucket(durability_impossible_fallback="none")
        create_bucket(durability_impossible_fallback="Disabled")
        create_bucket(durability_impossible_fallback="fallbacktoactiveack")
        create_bucket(durability_impossible_fallback="None")

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
        self.log.info(f"Bucket with name : {bucket_name} "
                      f"type : {self.bucket_type} "
                      f"replicas : {self.num_replicas} "
                      f"storage : couchstore created")

        for i in range(num_collections):
            collection_name = f"collection_{i}"
            self.bucket_util.create_collection(self.cluster.master,
                                               self.cluster.buckets[0],
                                               CbServer.default_scope,
                                               {"name": collection_name})
            self.log.info("Collection {0} created".format(collection_name))

        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        result = self.bucket_util.validate_history_retention_settings(
            self.cluster.master, self.cluster.buckets[0])
        self.assertTrue(result,
                        "History field in stats could not be validated")
        self.log.info("History field in stats validated successfully")
