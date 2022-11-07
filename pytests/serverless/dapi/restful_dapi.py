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

    def test_get_scopes(self):
        for bucket in self.buckets:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": bucket.serverless.dapi,
                                          "access_token": bucket.serverless.nebula_endpoint.rest_username,
                                          "access_secret": bucket.serverless.nebula_endpoint.rest_password,
                                          "test": "scopes"})
            self.log.info("To get list of all scopes for DB: {}".format(bucket.name))
            self.log.info(bucket.serverless.dapi)

            number_of_scopes, scope_name , scope_suffix = 10, "scope", 0
            scope_name_list = ["_default", "_system"]
            for i in range(number_of_scopes):
                scope_suffix += 1
                scope_name = "scope" + str(scope_suffix)
                scope_name_list.append(scope_name)
                response = self.rest_dapi.create_scope({"scopeName": scope_name})
                self.log.info("response for creation of scope: {}".format(response.status_code))
                self.assertTrue(response.status_code == 200,
                                "Creation of scope failed for database {}".format(bucket.name))

            response = self.rest_dapi.get_scope_list()
            self.log.info("status code for getting list of scope: {}".format(response.status_code))
            self.log.info(json.loads(response.content))
            self.assertTrue(response.status_code == 200,
                            "Getting list of scopes failed for database {}".format(bucket.name))

            response_dict = json.loads(response.content)
            response_list = response_dict["scopes"]
            scope_list = []
            for scope in response_list:
                scope_list.append(scope["Name"])

            scope_list.sort()
            scope_name_list.sort()
            self.assertTrue(scope_list == scope_name_list,
                            "Wrong scopes received for database {}".format(bucket.name))

    def test_get_collections(self):
        for bucket in self.buckets:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": bucket.serverless.dapi,
                                          "access_token": bucket.serverless.nebula_endpoint.rest_username,
                                          "access_secret": bucket.serverless.nebula_endpoint.rest_password})

            self.log.info("To get list of all collections within a scope in database {}".format(bucket.name))
            self.log.info(bucket.serverless.dapi)

            scope = "testScope"
            response = self.rest_dapi.create_scope({"scopeName": scope})
            self.log.info(response.status_code)
            self.assertTrue(response.status_code == 200,
                            "Creation of scope failed for database {}".format(bucket.name))

            number_of_collection, collection_name, collection_suffix = 10, "collection", 0
            collection_name_list = []
            for i in range(number_of_collection):
                collection_suffix += 1
                collection_name = "collection" + str(collection_suffix)
                collection_name_list.append(collection_name)
                response = self.rest_dapi.create_collection(scope, {"name": collection_name})
                self.log.info("Response code for creation of collection: {}".format(response.status_code))
                self.assertTrue(response.status_code == 200,
                                "Creation of collection failed for database {}".format(bucket.name))

            response = self.rest_dapi.get_collection_list(scope)

            self.log.info(response.status_code)
            self.log.info(json.loads(response.content))

            self.assertTrue(response.status_code == 200,
                            "Getting list of collections failed for database {}".format(bucket.name))

            collection_list = json.loads(response.content)
            collection_list = collection_list["collections"]
            self.assertTrue(len(collection_list) == number_of_collection,
                            "Getting all collections failed for testscope for database {}".format(bucket.name))

            temp_collection_list = []
            for collection in collection_list:
                temp_collection_list.append(collection["Name"])

            temp_collection_list.sort()
            collection_name_list.sort()
            self.assertTrue(temp_collection_list == collection_name_list,
                            "Wrong collection/s received for database {}".format(bucket.name))

    def test_get_documents(self):
        for bucket in self.buckets:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": bucket.serverless.dapi,
                                          "access_token": bucket.serverless.nebula_endpoint.rest_username,
                                          "access_secret": bucket.serverless.nebula_endpoint.rest_password})

            self.log.info("Get list of all documents within a collection in database {}".format(bucket.name))
            self.log.info(bucket.serverless.dapi)

            length_of_list, key, content = 10, "key", "content"
            document_name_list = []
            key_value, content_value = 0, 0

            for i in range(length_of_list):
                key_value += 1
                content_value += 1
                key = "key" + str(key_value)
                content = "content" + str(content_value)
                document_name_list.append(key)
                response = self.rest_dapi.insert_doc(key, {"content": content}, "_default", "_default")
                if response.status_code == 201:
                    self.assertTrue(response.status_code == 201,
                                    "Insertion failed for database {}".format(bucket.name))

            response = self.rest_dapi.get_document_list("_default", "_default")
            self.assertTrue(response.status_code == 200,
                            "Getting list of documents failed for databsase {}".format(bucket.name))

            self.log.info(response.status_code)
            if response.status_code == 200:
                self.log.info(json.loads(response.content))

            response_dict = json.loads(response.content)
            document_list = response_dict["docs"]
            self.log.info(document_list)
            document_name_check_list = []
            for documents in document_list:
                document_name_check_list.append(documents['meta_id'])
            document_name_list.sort()
            document_name_check_list.sort()
            self.log.info(document_name_list)
            self.log.info(document_name_check_list)
            self.assertTrue(document_name_list == document_name_check_list,
                            "Wrong document received for database {}".format(bucket.name))

    def test_get_subdocument(self):
        for bucket in self.buckets:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": bucket.serverless.dapi,
                                          "access_token": bucket.serverless.nebula_endpoint.rest_username,
                                          "access_secret": bucket.serverless.nebula_endpoint.rest_password})

            self.log.info("Get list of all sub-documents within a document in database {}".format(bucket.name))
            self.log.info(bucket.serverless.dapi)

            response = self.rest_dapi.insert_doc("k", {"inserted": True}, "_default", "_default")
            self.log.info(response.status_code)

            self.assertTrue(response.status_code == 201,
                            "Insertion failed for database {}".format(bucket.name))

            response = self.rest_dapi.get_doc("k", "_default", "_default")
            self.log.info(response.status_code)

            self.assertTrue(response.status_code == 200,
                            "Getting document failed for database {}".format(bucket.name))

            response = self.rest_dapi.insert_subdoc("k",
                                                    [{"type": "insert", "path": "counter3", "value": 4},
                                                     {"type": "insert", "path": "counter1", "value": 10}],
                                                    "_default", "_default")
            self.log.info(response.status_code)

            self.assertTrue(response.status_code == 200,
                            "Sub doc insertion failed for database {}".format(bucket.name))

            response = self.rest_dapi.get_doc("k", "_default", "_default")
            self.log.info(response.content)
            response = self.rest_dapi.get_subdoc("k",
                                                 [{"type": "get", "path": "counter3"}],
                                                 "_default", "_default")
            self.log.info("printing status code for getting subdoc: {}".format(response.status_code))
            if response.status_code == 200:
                self.log.info("printing response content for getting subbdoc {}".format(response.content))

            self.assertTrue(response.status_code == 200,
                            "Getting subdocs failed for database {}".format(bucket.name))

    def test_insert_subdocument(self):
        for bucket in self.buckets:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": bucket.serverless.dapi,
                                          "access_token": bucket.serverless.nebula_endpoint.rest_username,
                                          "access_secret": bucket.serverless.nebula_endpoint.rest_password})

            self.log.info("Mutating subdocument inside a document in database {}".format(bucket.name))
            self.log.info(bucket.serverless.dapi)

            response = self.rest_dapi.insert_doc("k", {"inserted": True}, "_default", "_default")
            self.log.info(response.status_code)

            self.assertTrue(response.status_code == 201,
                            "Insertion failed for database {}".format(bucket.name))

            response = self.rest_dapi.get_doc("k", "_default", "_default")
            self.log.info(response.status_code)

            self.assertTrue(response.status_code == 200,
                            "Getting document failed for database {}".format(bucket.name))

            response = self.rest_dapi.insert_subdoc("k",
                                                    [{"type": "insert", "path": "counter3", "value": 4},
                                                     {"type": "insert", "path": "counter1", "value": 10}],
                                                    "_default", "_default")
            self.log.info(response.status_code)

            self.assertTrue(response.status_code == 200,
                            "Sub doc insertion failed for database {}".format(bucket.name))

            response = self.rest_dapi.get_subdoc("k", [{"type": "get", "path": "counter3"}], "_default", "_default")
            self.log.info("printing response status code for getting subdoc {}".format(response.status_code))
            self.assertTrue(response.status_code == 200, "Getting subdoc failed for database {}".format(bucket.name))
            self.log.info(response.content)
