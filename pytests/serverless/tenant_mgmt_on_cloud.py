from time import time

from BucketLib.bucket import Bucket
from Cb_constants import CbServer
from capella_utils.serverless import CapellaUtils as ServerlessUtils
from cluster_utils.cluster_ready_functions import Nebula
from serverlessbasetestcase import OnCloudBaseTest

from com.couchbase.test.sdk import Server, SDKClient
from com.couchbase.test.sdk import SDKClient as NewSDKClient
from com.couchbase.test.loadgen import WorkLoadGenerate
from com.couchbase.test.docgen import \
    WorkLoadSettings, DocumentGenerator, DocRange
from couchbase.test.docgen import DRConstants

from java.util import HashMap


class TenantMgmtOnCloud(OnCloudBaseTest):
    def setUp(self):
        super(TenantMgmtOnCloud, self).setUp()
        self.db_name = "TAF-TenantMgmtOnCloud"

    def tearDown(self):
        super(TenantMgmtOnCloud, self).tearDown()

    def __get_serverless_bucket_obj(self, db_name, width, weight,
                                    num_vbs=None):
        self.log.debug("Creating server bucket_obj %s:%s:%s"
                       % (db_name, width, weight))
        bucket_obj = Bucket({
            Bucket.name: db_name,
            Bucket.bucketType: Bucket.Type.MEMBASE,
            Bucket.replicaNumber: Bucket.ReplicaNum.TWO,
            Bucket.storageBackend: Bucket.StorageBackend.magma,
            Bucket.evictionPolicy: Bucket.EvictionPolicy.FULL_EVICTION,
            Bucket.flushEnabled: Bucket.FlushBucket.DISABLED,
            Bucket.width: width,
            Bucket.weight: weight})
        if num_vbs:
            bucket_obj.numVBuckets = num_vbs

        return bucket_obj

    def __create_database(self):
        bucket = self.__get_serverless_bucket_obj(
            self.db_name, self.bucket_width, self.bucket_weight)
        task = self.bucket_util.async_create_database(self.cluster, bucket)
        self.task_manager.get_task_result(task)
        nebula = Nebula(task.srv, task.server)
        self.log.info("Populate Nebula object done!!")
        bucket.serverless.nebula_endpoint = nebula.endpoint
        bucket.serverless.dapi = \
            ServerlessUtils.get_database_DAPI(self.pod, self.tenant,
                                              bucket.name)
        self.bucket_util.update_bucket_nebula_servers(self.cluster, nebula,
                                                      bucket)
        self.cluster.buckets.append(bucket)

    def test_create_delete_database(self):
        """
        1. Loading initial buckets
        2. Start data loading to all buckets
        3. Create more buckets when data loading is running
        4. Delete the newly created database while intial load is still running
        :return:
        """
        self.db_name = "%s-testCreateDeleteDatabase" % self.db_name
        dynamic_buckets = self.input.param("other_buckets", 1)
        # Create Buckets
        self.log.info("Creating '%s' buckets for initial load"
                      % self.num_buckets)
        for _ in range(self.num_buckets):
            self.__create_database()

        # TODO: Data loading to the initial buckets

        new_buckets = list()
        for _ in range(dynamic_buckets):
            bucket = self.__get_serverless_bucket_obj(
                self.db_name, self.bucket_width, self.bucket_weight)
            task = self.bucket_util.async_create_database(self.cluster, bucket)
            self.task_manager.get_task_result(task)
            nebula = Nebula(task.srv, task.server)
            self.log.info("Populate Nebula object done!!")
            bucket.serverless.nebula_endpoint = nebula.endpoint
            bucket.serverless.dapi = \
                ServerlessUtils.get_database_DAPI(self.pod, self.tenant,
                                                  bucket.name)
            self.bucket_util.update_bucket_nebula_servers(self.cluster, nebula,
                                                          bucket)
            new_buckets.append(bucket)

        # TODO: Load docs to new buckets

        self.log.info("Removing '%s' buckets")
        for bucket in new_buckets:
            ServerlessUtils.delete_database(self.pod, self.tenant, bucket.name)
