import time

import Jython_tasks.task as jython_tasks

from basetestcase import BaseTestCase
from Cb_constants import DocLoading
from membase.api.exception import BucketCreationException
from membase.api.rest_client import RestConnection, RestHelper
from BucketLib.bucket import Bucket
from couchbase_helper.documentgenerator import doc_generator
from cb_tools.cbstats import Cbstats
from BucketLib.BucketOperations import BucketHelper

from remote.remote_util import RemoteMachineShellConnection
from sdk_exceptions import SDKException



class RebalanceInOutTests(BaseTestCase):
    def setUp(self):
        super(RebalanceInOutTests, self).setUp()

        self.nodes_init = self.input.param("nodes_init", 1)
        self.nodes_in = self.input.param("nodes_in", 1)
        self.nodes_out = self.input.param("nodes_out", 1)
        self.doc_ops = self.input.param("doc_ops", "create")
        #self.bucket_util.add_rbac_user()
        self.num_items = self.input.param("num_items", 100000)
        self.num_buckets = self.input.param("num_buckets", 1)
        self.rest = RestConnection(self.cluster.master)
        self.doc_and_collection_ttl = self.input.param("doc_and_collection_ttl", False)
        self.replica_to_update  = self.input.param("replica_to_update", 1)
        self.doc_size = self.input.param("doc_size", 256)
        self.key = self.input.param("key", "_ghali")


    def tearDown(self):
        super(RebalanceInOutTests, self).tearDown()

    def test_balance_out(self):

        bucket = self.create_requirebuckets()

        tasks_info = self.generate_load_docs(bucket)
        self.task.jython_task_manager.get_task_result(tasks_info)
        servs_in = [self.cluster.servers[i + self.nodes_init] for i in range(self.nodes_in)]
        rebalance_task = self.task.async_rebalance(
            self.cluster.servers[:self.nodes_init], servs_in, [])
        self.task.jython_task_manager.get_task_result(rebalance_task)
        self.assertTrue(rebalance_task.result, "Rebalance Failed")
        self.sleep(20)
        servs_out = [self.cluster.servers[len(self.cluster.nodes_in_cluster) - i - 1] for i in range(self.nodes_out)]
        rebalance_task = self.task.async_rebalance(
            self.cluster.servers[:self.nodes_init], [], servs_out)
        self.task_manager.get_task_result(rebalance_task)
        self.assertTrue(rebalance_task.result, "Rebalance Failed")
        self.sleep(20)

    def test_balance_in(self):
        bucket = self.create_requirebuckets()

        tasks_info = self.generate_load_docs(bucket)

        self.task.jython_task_manager.get_task_result(tasks_info)
        servs_in = [self.cluster.servers[i + self.nodes_init] for i in range(self.nodes_in)]
        rebalance_task = self.task.async_rebalance(
            self.cluster.servers[:self.nodes_init], servs_in, [])
        self.task.jython_task_manager.get_task_result(rebalance_task)
        self.assertTrue(rebalance_task.result, "Rebalance Failed")
        self.sleep(20)


        #Create bucket
    def create_requirebuckets(self):
        print("Get the available memory quota")
        self.info = self.rest.get_nodes_self()

        # threshold_memory_vagrant = 100
        kv_memory = self.info.memoryQuota - 100

        # Creating buckets for data loading purpose
        self.bucket_expiry = self.input.param("bucket_expiry", 0)
        ramQuota = self.input.param("ramQuota", kv_memory)
        buckets = self.input.param("bucket_names",
                                   "GleamBookUsers").split(';')
        self.bucket_type = self.bucket_type.split(';')
        self.compression_mode = self.compression_mode.split(';')
        self.bucket_eviction_policy = self.bucket_eviction_policy
        for i in range(self.num_buckets):
            bucket = Bucket(
                {Bucket.name: buckets[i],
                 Bucket.ramQuotaMB: ramQuota / self.num_buckets,
                 Bucket.maxTTL: self.bucket_expiry,
                 Bucket.replicaNumber: self.num_replicas,
                 Bucket.storageBackend: self.bucket_storage,
                 Bucket.evictionPolicy: self.bucket_eviction_policy,
                 Bucket.bucketType: self.bucket_type[i],
                 Bucket.flushEnabled: Bucket.FlushBucket.ENABLED,
                 Bucket.compressionMode: self.compression_mode[i]})
            self.bucket_util.create_bucket(bucket)
        return bucket

    #Generate documents and load into the bucket
    def generate_load_docs(self,bucket):
        load_gen = doc_generator(self.key, 0, self.num_items,
                                     doc_size=self.doc_size)
        task = self.task.async_load_gen_docs(
            self.cluster, bucket, load_gen,
            DocLoading.Bucket.DocOps.CREATE, self.maxttl,
            persist_to=self.persist_to,
            replicate_to=self.replicate_to,
            durability=self.durability_level,
            batch_size=100, process_concurrency=8,
            sdk_client_pool=self.sdk_client_pool)
        return task
