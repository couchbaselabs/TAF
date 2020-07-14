from Cb_constants.CBServer import CbServer
from com.couchbase.client.core.error import DocumentUnretrievableException
from com.couchbase.client.java.kv import GetAnyReplicaOptions
from couchbase_helper.documentgenerator import doc_generator
from magma_base import MagmaBaseTest
from sdk_client3 import SDKClient


class MagmaExpiryTests(MagmaBaseTest):
    def setUp(self):
        super(MagmaExpiryTests, self).setUp()
        self.change_swap_space(self.cluster.nodes_in_cluster)
        self.disk_usage = dict()

        self.create_start = 0
        self.create_end = self.num_items
        if self.rev_write:
            self.create_start = -int(self.num_items - 1)
            self.create_end = 1

        self.read_start = 0
        self.read_end = self.num_items
        if self.rev_read:
            self.read_start = -int(self.num_items - 1)
            self.read_end = 1

        self.delete_start = 0
        self.delete_end = self.num_items
        if self.rev_del:
            self.delete_start = -int(self.num_items - 1)
            self.delete_end = 1

        self.update_start = 0
        self.update_end = self.num_items
        if self.rev_update:
            self.update_start = -int(self.num_items - 1)
            self.update_end = 1

        self.generate_docs(doc_ops="create")

        self.init_loading = self.input.param("init_loading", True)
        if self.init_loading:
            self.result_task = self._load_all_buckets(
                self.cluster, self.gen_create,
                "create", 0,
                batch_size=self.batch_size,
                dgm_batch=self.dgm_batch)
            if self.active_resident_threshold != 100:
                for task in self.result_task.keys():
                    self.num_items = task.doc_index

            self.log.info("Verifying num_items counts after doc_ops")
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(self.num_items)

            if self.standard_buckets == 1 or self.standard_buckets == self.magma_buckets:
                for bucket in self.bucket_util.get_all_buckets():
                    disk_usage = self.get_disk_usage(
                        bucket, self.cluster.nodes_in_cluster)
                    self.disk_usage[bucket.name] = disk_usage[0]
                    self.log.info(
                        "For bucket {} disk usage after initial creation is {}MB\
                        ".format(bucket.name,
                                 self.disk_usage[bucket.name]))
            self.init_items = self.num_items
            self.end = self.num_items

        self.generate_docs(doc_ops="update:read:delete")

        self.cluster_util.print_cluster_stats()
        self.bucket_util.print_bucket_stats()

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
