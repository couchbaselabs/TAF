from basetestcase import BaseTestCase
from BucketLib.bucket import Bucket
from ServerlessLib.dapi.dapi import RestfulDAPI
from cluster_utils.cluster_ready_functions import Nebula
import json


class RestfulDAPITest(BaseTestCase):
    def setUp(self):
        BaseTestCase.setUp(self)
        self.num_buckets = self.input.param("num_buckets", 1)
        self.create_databases(self.num_buckets)

    def tearDown(self):
        BaseTestCase.tearDown(self)

    def create_databases(self, count=1):
        temp = list()
        for i in range(count):
            self.database_name = "dapi-{}".format(i)
            bucket = Bucket(
                    {Bucket.name: self.database_name,
                     Bucket.bucketType: Bucket.Type.MEMBASE,
                     Bucket.replicaNumber: 2,
                     Bucket.storageBackend: Bucket.StorageBackend.magma,
                     Bucket.evictionPolicy: Bucket.EvictionPolicy.FULL_EVICTION,
                     Bucket.flushEnabled: Bucket.FlushBucket.DISABLED,
                     Bucket.numVBuckets: 64,
                     Bucket.width: self.bucket_width or 1,
                     Bucket.weight: self.bucket_weight or 30
                     })
            task = self.bucket_util.async_create_database(self.cluster, bucket,
                                                          self.dataplane_id)
            temp.append((task, bucket))
            self.sleep(1)
        for task, bucket in temp:
            self.task_manager.get_task_result(task)
            self.assertTrue(task.result, "Database deployment failed: {}".
                            format(bucket.name))

        self.buckets = self.cluster.buckets

    def test_dapi_health(self):
        for bucket in self.buckets:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": bucket.serverless.dapi,
                                          "access_token": bucket.serverless.nebula_endpoint.rest_username,
                                          "access_secret": bucket.serverless.nebula_endpoint.rest_password})
            self.log.info("Checking DAPI health for DB: {}".format(bucket.name))
            self.log.info(bucket.serverless.dapi)
            response = self.rest_dapi.check_dapi_health()
            self.assertTrue(response.status_code == 200,
                            "DAPI is not healthy for database: {}".format(bucket.name))
            self.log.info(json.loads(response.content)["health"])
            self.assertTrue(json.loads(response.content)["health"].lower() == "ok",
                            "DAPI health is not OK")

    def test_dapi_insert(self):
        for bucket in self.buckets:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": bucket.serverless.dapi,
                                          "access_token": bucket.serverless.nebula_endpoint.rest_username,
                                          "access_secret": bucket.serverless.nebula_endpoint.rest_password})
            self.log.info("Checking DAPI health for DB: {}".format(bucket.name))
            self.log.info(bucket.serverless.dapi)
            response = self.rest_dapi.insert_doc("k", {"inserted": True}, "_default", "_default")
            self.assertTrue(response.status_code == 201,
                            "Insertion failed for database: {}".format(bucket.name))

    def test_dapi_get(self):
        for bucket in self.buckets:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": bucket.serverless.dapi,
                                          "access_token": bucket.serverless.nebula_endpoint.rest_username,
                                          "access_secret": bucket.serverless.nebula_endpoint.rest_password})
            self.log.info("Checking DAPI health for DB: {}".format(bucket.name))
            self.log.info(bucket.serverless.dapi)
            # Insert Doc
            response = self.rest_dapi.insert_doc("k", {"inserted": True}, "_default", "_default")
            self.assertTrue(response.status_code == 201,
                            "Insertion failed for database: {}".format(bucket.name))
            # Read Doc
            response = self.rest_dapi.get_doc("k", "_default", "_default")
            self.assertTrue(response.status_code == 200,
                            "Reading doc for database: {}".format(bucket.name))
            self.log.info(json.loads(response.content))
            val = json.loads(response.content).values()[0]
            self.assertTrue(val == {"inserted": True}, "Value mismatch")

    def test_dapi_upsert(self):
        for bucket in self.buckets:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": bucket.serverless.dapi,
                                          "access_token": bucket.serverless.nebula_endpoint.rest_username,
                                          "access_secret": bucket.serverless.nebula_endpoint.rest_password})
            self.log.info("Checking DAPI health for DB: {}".format(bucket.name))
            self.log.info(bucket.serverless.dapi)
            # Insert Doc
            response = self.rest_dapi.insert_doc("k", {"inserted": True}, "_default", "_default")
            self.assertTrue(response.status_code == 201,
                            "Insertion failed for database: {}".format(bucket.name))
            # Read Doc
            response = self.rest_dapi.get_doc("k", "_default", "_default")
            self.assertTrue(response.status_code == 200,
                            "Reading doc for database: {}".format(bucket.name))
            val = json.loads(response.content).values()[0]
            self.assertTrue(val == {"inserted": True}, "Value mismatch")
            # Upsert Doc
            response = self.rest_dapi.upsert_doc("k", {"updated": True}, "_default", "_default")
            self.assertTrue(response.status_code == 201,
                            "DAPI is not healthy for database: {}".format(bucket.name))
            # Read Doc
            response = self.rest_dapi.get_doc("k", "_default", "_default")
            self.assertTrue(response.status_code == 200,
                            "Reading doc for database: {}".format(bucket.name))
            val = json.loads(response.content).values()[0]
            self.assertTrue(val == {"updated": True}, "Value mismatch")

    def test_dapi_delete(self):
        for bucket in self.buckets:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": bucket.serverless.dapi,
                                          "access_token": bucket.serverless.nebula_endpoint.rest_username,
                                          "access_secret": bucket.serverless.nebula_endpoint.rest_password})
            self.log.info("Checking DAPI health for DB: {}".format(bucket.name))
            self.log.info(bucket.serverless.dapi)
            # Insert Doc
            response = self.rest_dapi.insert_doc("k", {"inserted": True}, "_default", "_default")
            self.assertTrue(response.status_code == 201,
                            "Insertion failed for database: {}".format(bucket.name))
            # Delete Doc
            response = self.rest_dapi.delete_doc("k", "_default", "_default")
            self.assertTrue(response.status_code == 200,
                            "Delete doc for database: {}".format(bucket.name))
            #Read Doc
            response = self.rest_dapi.get_doc("k", "_default", "_default")
            self.assertTrue(response.status_code == 400,
                            "Reading doc for database: {}".format(bucket.name))
            val = json.loads(response.content)["error"]["errorDetails"]["msg"]
            self.assertTrue(val == "document not found",
                            "Wrong error msg for deleted doc: {}".format(val))
