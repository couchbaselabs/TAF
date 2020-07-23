from Cb_constants.CBServer import CbServer
from com.couchbase.client.core.error import DocumentUnretrievableException
from com.couchbase.client.java.kv import GetAnyReplicaOptions
from couchbase_helper.documentgenerator import doc_generator
from magma_base import MagmaBaseTest
from sdk_client3 import SDKClient


class MagmaExpiryTests(MagmaBaseTest):
    def setUp(self):
        super(MagmaExpiryTests, self).setUp()
        self.gen_delete = None
        self.gen_create = None
        self.gen_update = None
        self.gen_expiry = None

    def tearDown(self):
        super(MagmaExpiryTests, self).tearDown()

    def test_expiry(self):
        result = True
        self.gen_create = doc_generator(
            self.key, 0, 10,
            doc_size=20,
            doc_type=self.doc_type,
            key_size=self.key_size)

        tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, self.gen_create, "create", 10,
                batch_size=10,
                process_concurrency=1,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level, pause_secs=5,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                )
        self.task.jython_task_manager.get_task_result(tasks_info.keys()[0])
        self.sleep(20)
        self.client = SDKClient([self.cluster.master],
                                self.bucket_util.buckets[0],
                                scope=CbServer.default_scope,
                                collection=CbServer.default_collection)
        for i in range(10):
            key = (self.key + "-" + str(i).zfill(self.key_size-len(self.key)))
            try:
                getReplicaResult = self.client.collection.getAnyReplica(
                    key, GetAnyReplicaOptions.getAnyReplicaOptions())
                if getReplicaResult:
                    result = False
                    try:
                        self.log.info("Able to retreive: %s" %
                                      {"key": key,
                                       "value": getReplicaResult.contentAsObject(),
                                       "cas": getReplicaResult.cas()})
                    except Exception as e:
                        print str(e)
            except DocumentUnretrievableException as e:
                pass
            if len(self.client.get_from_all_replicas(key)) > 0:
                result = False
        self.client.close()
        self.assertTrue(result, "SDK is able to retrieve expired documents")

    def test_expiry_update(self):
        count = 0
        self.gen_expiry = 100
        self.log.info("Test Iteration count == {}".format(count))
        self.generate_docs(doc_ops="expiry")
        _ = self.loadgen_docs(self.retry_exceptions,
                              self.ignore_exceptions,
                              _sync=True)
        self.sleep(self.maxttl, "Wait to docs to expire")

        self.log.info("Verifying doc counts after create doc_ops")
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        self.generate_docs(doc_ops="read")
        count += 1
