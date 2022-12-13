from basetestcase import BaseTestCase
from BucketLib.bucket import Bucket
from ServerlessLib.dapi.dapi import RestfulDAPI
from couchbase_helper.documentgenerator import doc_generator
import json
import string
import random


class RestfulDAPITest(BaseTestCase):
    def setUp(self):
        BaseTestCase.setUp(self)
        self.num_buckets = self.input.param("num_buckets", 1)
        self.create_databases(self.num_buckets)
        self.value_size = self.input.param("value_size", 255)
        self.key_size = self.input.param("key_size", 8)
        self.number_of_docs = self.input.param("number_of_docs", 10)
        self.randomize_value = self.input.param("randomize_value", False)
        self.randomize_doc_size = self.input.param("randomize_doc_size", False)
        self.randomize = self.input.param("randomize", False)
        self.mixed_key = self.input.param("mixed_key", False)
        self.number_of_collections = self.input.param("number_of_collections", 10)
        self.number_of_scopes = self.input.param("number_of_scopes", 10)

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

            gen_obj = doc_generator("key", 0, self.number_of_docs,
                                    key_size=self.key_size, doc_size=self.value_size,
                                    randomize_value=self.randomize_value, randomize_doc_size=self.randomize_doc_size)

            # insertion with mixed key - combination of characters, digits and letter
            if self.mixed_key:
                key = ''.join(random.choice(string.ascii_uppercase + string.digits +
                                            string.punctuation + string.ascii_lowercase)
                              for length in range(self.key_size))
                doc = ''.join(random.choice(string.ascii_lowercase + string.ascii_uppercase + string.digits)
                              for length in range(self.value_size))

                response = self.rest_dapi.insert_doc(key, doc, "_default", "_default", 1200)
                self.assertTrue(response.status_code == 201,
                                "Document insertion failed with mixed key {} for database {}".format(key, bucket.name))
                # get doc
                response = self.rest_dapi.get_doc(key, "_default", "_default")
                self.assertTrue(response.status_code == 200,
                                "Get document failed with mixed key {} for database {}".format(key, bucket.name))

            document_list = []
            for i in range(self.number_of_docs):
                if gen_obj.has_next():
                    key, doc = gen_obj.next()
                    doc = doc.toMap()
                    doc = dict(doc)
                    document_list.append({"key": key, "doc": doc})
                    response = self.rest_dapi.insert_doc(key, doc, "_default", "_default", 1200)
                    self.log.info("Response code for inserting doc: {}".format(response.status_code))

                    self.assertTrue(response.status_code == 201,
                                    "Document insertion failed with doc {} "
                                    "and key {} for database {}".format(
                                        doc, key, bucket.name))

            for doc in document_list:
                key = doc["key"]
                response = self.rest_dapi.get_doc(key, "_default", "_default")
                self.assertTrue(response.status_code == 200,
                                "Get failed for document with key: {} in database {}".format(key, bucket.name))

            # validate duplicate doc insert
            for doc in document_list:
                key = doc["key"]
                document = doc["doc"]
                response = self.rest_dapi.insert_doc(key, document, "_default", "_default")
                self.log.info("Response code for inserting duplicate docs {}".format(response.status_code))
                self.assertTrue(response.status_code == 409,
                                "Duplicate entry Insertion for database {}".format(bucket.name))

    def test_dapi_get(self):
        for bucket in self.buckets:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": bucket.serverless.dapi,
                                          "access_token": bucket.serverless.nebula_endpoint.rest_username,
                                          "access_secret": bucket.serverless.nebula_endpoint.rest_password})
            self.log.info("Checking DAPI health for DB: {}".format(bucket.name))
            self.log.info(bucket.serverless.dapi)

            # insertion of document
            gen_obj = doc_generator("key", 0, self.number_of_docs, key_size=self.key_size,
                                    doc_size=self.value_size, randomize_value=self.randomize_value)
            document_list = []
            for i in range(self.number_of_docs):
                if gen_obj.has_next():
                    key, doc = gen_obj.next()
                    doc = doc.toMap()
                    doc = dict(doc)
                    key_document = {"key": key, "doc": doc}
                    document_list.append(key_document)
                    response = self.rest_dapi.insert_doc(key, doc, "_default", "_default")
                    self.log.info("Response code for inserting doc: {}".format(response.status_code))

                    self.assertTrue(response.status_code == 201,
                                    "Document insertion failed with doc size {} "
                                    "and key size {} for database {}".format(
                                        self.value_size, self.key_size, self.number_of_docs))
            # Reading of documents
            for doc in document_list:
                key = doc["key"]
                document = doc["doc"]
                response = self.rest_dapi.get_doc(key, "_default", "_default")
                self.assertTrue(response.status_code == 200,
                                "Get failed for with key {} for database {}".format(key, bucket.name))
                val = json.loads(response.content).values()[0]
                self.assertTrue(val == document, "Value mismatch")

            # GET for non existing document
            key = "monkey"
            response = self.rest_dapi.get_doc(key, "_default", "_default")
            self.log.info("Response code for getting non existing document {}".format(response.status_code))
            self.assertTrue(response.status_code == 404,
                            "Get success for non existing document for database {}".format(bucket.name))

    def test_dapi_upsert(self):
        for bucket in self.buckets:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": bucket.serverless.dapi,
                                          "access_token": bucket.serverless.nebula_endpoint.rest_username,
                                          "access_secret": bucket.serverless.nebula_endpoint.rest_password})
            self.log.info("Checking DAPI health for DB: {}".format(bucket.name))
            self.log.info(bucket.serverless.dapi)
            # Insert Doc
            gen_obj = doc_generator("key", 0, self.number_of_docs, key_size=self.key_size,
                                    doc_size=self.value_size, randomize_value=self.randomize_value)
            document_list = []
            for i in range(self.number_of_docs):
                if gen_obj.has_next():
                    key, doc = gen_obj.next()
                    doc = doc.toMap()
                    doc = dict(doc)
                    key_document = {"key": key, "doc": doc}
                    document_list.append(key_document)
                    response = self.rest_dapi.insert_doc(key, doc, "_default", "_default")
                    self.log.info("Response code for inserting doc: {}".format(response.status_code))

                    self.assertTrue(response.status_code == 201,
                                    "Document insertion failed with doc size {} "
                                    "and key size {} for database {}".format(
                                        self.value_size, self.key_size, bucket.name))
            # Read Doc
            for doc in document_list:
                key = doc["key"]
                document = doc["doc"]
                response = self.rest_dapi.get_doc(key, "_default", "_default")
                self.assertTrue(response.status_code == 200,
                                "Get failed for with key {} for database {}".format(key, bucket.name))
                val = json.loads(response.content).values()[0]
                self.assertTrue(val == document, "Value mismatch")
            # Upsert Doc
            self.randomize_value = (not self.randomize_value)
            gen_obj = doc_generator("key", 0, self.number_of_docs, key_size=self.key_size,
                                    doc_size=self.value_size, randomize_value=self.randomize_value)
            document_list = []
            for i in range(self.number_of_docs):
                if gen_obj.has_next():
                    key, doc = gen_obj.next()
                    doc = doc.toMap()
                    doc = dict(doc)
                    key_document = {"key": key, "doc": doc}
                    document_list.append(key_document)
                    response = self.rest_dapi.upsert_doc(key, doc, "_default", "_default")
                    self.log.info("Response code for Updating doc: {}".format(response.status_code))

                    self.assertTrue(response.status_code == 200,
                                    "Document updation failed with doc size {} "
                                    "and key size {} for database {}".format(
                                        self.value_size, self.key_size, self.number_of_docs))
            # Read Doc
            for doc in document_list:
                key = doc["key"]
                document = doc["doc"]
                response = self.rest_dapi.get_doc(key, "_default", "_default")
                self.assertTrue(response.status_code == 200,
                                "Get failed for with key {} for database {}".format(key, bucket.name))
                val = json.loads(response.content).values()[0]
                self.assertTrue(val == document, "Value mismatch")

            # upsert a non existing document
            gen_obj = doc_generator("monkey", 0, 1,
                                    key_size=self.key_size, doc_size=self.value_size,
                                    randomize_value=self.randomize_value)
            if gen_obj.has_next():
                key, doc = gen_obj.next()
                doc = doc.toMap()
                doc = dict(doc)
                # upsert for non existing document
                response = self.rest_dapi.upsert_doc(key, doc, "_default", "_default")
                self.log.info("Response code for updating a non existing document: {}".format(response.status_code))
                self.assertTrue(response.status_code == 200,
                                "Upsert failed for non existing document in database {}".format(bucket.name))
                response = self.rest_dapi.get_doc(key, "_default", "_default")
                self.assertTrue(response.status_code == 200,
                                "Get failed for database {}".format(bucket.name))
                # upsert with same key and value
                response = self.rest_dapi.upsert_doc(key, doc, "_default", "_default")
                self.log.info("Response code for updating doc with same key and value {}".format(response.status_code))
                self.assertTrue(response.status_code == 200,
                                "Updation failed with same key and value for database {}".format(bucket.name))
                # upsert with parameters - lock time
                response = self.rest_dapi.upsert_doc_with_lock_time(key, doc, "_default", "_default")
                self.log.info("Response code for updation of doc with lock time: {}".format(response.status_code))
                doc["temp"] = "temp value"
                response = self.rest_dapi.get_doc(key, "_default", "_default")
                self.log.info("Response code for get doc within lock time: {}".format(response.status_code))
                self.assertTrue(response.status_code == 200,
                                "Get doc failed within lock time for database {}".format(response.status_code))

                # Updation with mixed key - combination of characters, digits and letter
                if self.mixed_key:
                    key = ''.join(random.choice(string.ascii_uppercase + string.digits +
                                                string.punctuation + string.ascii_lowercase)
                                  for length in range(self.key_size))
                    doc = ''.join(random.choice(string.ascii_lowercase + string.ascii_uppercase + string.digits)
                                  for length in range(self.value_size))
                    response = self.rest_dapi.insert_doc(key, doc, "_default", "_default")
                    self.assertTrue(response.status_code == 201,
                                    "Document insertion failed with mixed key {} for database {}".format(key,
                                                                                                         bucket.name))
                    # get doc
                    response = self.rest_dapi.get_doc(key, "_default", "_default")
                    self.assertTrue(response.status_code == 200,
                                    "Get document failed with mixed key {} for database {}".format(key, bucket.name))
                    # update doc
                    doc = ''.join(random.choice(string.ascii_lowercase + string.digits)
                                  for length in range(self.value_size))
                    response = self.rest_dapi.upsert_doc(key, doc, "_default", "_default")
                    self.assertTrue(response.status_code == 200,
                                    "Document updation failed with mixed key {} for database {}".format(key, bucket.name))

    def test_dapi_delete(self):
        for bucket in self.buckets:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": bucket.serverless.dapi,
                                          "access_token": bucket.serverless.nebula_endpoint.rest_username,
                                          "access_secret": bucket.serverless.nebula_endpoint.rest_password})
            self.log.info("Checking DAPI health for DB: {}".format(bucket.name))
            self.log.info(bucket.serverless.dapi)

            # Insertion of documents
            gen_obj = doc_generator("key", 0, self.number_of_docs, key_size=self.key_size,
                                    doc_size=self.value_size, randomize_value=self.randomize_value)
            document_list = []
            for i in range(self.number_of_docs):
                if gen_obj.has_next():
                    key, doc = gen_obj.next()
                    doc = doc.toMap()
                    doc = dict(doc)
                    key_document = {"key": key, "doc": doc}
                    document_list.append(key_document)
                    response = self.rest_dapi.insert_doc(key, doc, "_default", "_default")
                    self.log.info("Response code for inserting doc: {}".format(response.status_code))

                    self.assertTrue(response.status_code == 201,
                                    "Document insertion failed with doc size {} "
                                    "and key size {} for database {}".format(
                                        self.value_size, self.key_size, self.number_of_docs))
            # Delete Doc
            for doc in document_list:
                key = doc["key"]
                response = self.rest_dapi.delete_doc(key, "_default", "_default")
                self.assertTrue(response.status_code == 200,
                                "Delete doc failed with key {} for database {}".format(key, bucket.name))

            # Read Documents
            response = self.rest_dapi.get_doc("k", "_default", "_default")
            for doc in document_list:
                key = doc["key"]
                response = self.rest_dapi.get_doc(key, "_default", "_default")
                self.assertTrue(response.status_code == 404,
                                "Reading doc with key {} for database: {}".format(key, bucket.name))
                val = json.loads(response.content)["error"]["message"]
                self.assertTrue(val == "The requested document was not found in the collection",
                                "Wrong error msg for deleted doc: {}".format(val))

    def test_get_scopes(self):
        for bucket in self.buckets:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": bucket.serverless.dapi,
                                          "access_token": bucket.serverless.nebula_endpoint.rest_username,
                                          "access_secret": bucket.serverless.nebula_endpoint.rest_password,
                                          "test": "scopes"})
            self.log.info("To get list of all scopes for DB: {}".format(bucket.name))
            self.log.info(bucket.serverless.dapi)

            scope_name , scope_suffix = "scope", 0
            scope_name_list = ["_default", "_system"]
            for i in range(self.number_of_scopes):
                scope_suffix += 1
                scope_name = "scope" + str(scope_suffix)
                scope_name_list.append(scope_name)
                response = self.rest_dapi.create_scope({"scopeName": scope_name})
                self.log.info("response for creation of scope: {}".format(response.status_code))
                self.assertTrue(response.status_code == 200,
                                "Creation of scope failed for database {}".format(bucket.name))

            response = self.rest_dapi.get_scope_list()
            self.log.info("status code for getting list of scope: {}".format(response.status_code))
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

            collection_name, collection_suffix = "collection", 0
            collection_name_list = []
            for i in range(self.number_of_collections):
                collection_suffix += 1
                collection_name = "collection" + str(collection_suffix)
                collection_name_list.append(collection_name)
                response = self.rest_dapi.create_collection(scope, {"name": collection_name})
                self.log.info("Response code for creation of collection: {}".format(response.status_code))
                self.assertTrue(response.status_code == 200,
                                "Creation of collection failed for database {}".format(bucket.name))

            response = self.rest_dapi.get_collection_list(scope)
            self.log.info("Response code for getting list of collection {}".format(response.status_code))

            self.assertTrue(response.status_code == 200,
                            "Getting list of collections failed for database {}".format(bucket.name))

            collection_list = json.loads(response.content)
            collection_list = collection_list["collections"]
            self.assertTrue(len(collection_list) == self.number_of_collections,
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
                self.assertTrue(response.status_code == 201,
                                "Insertion failed for database {}".format(bucket.name))

            response = self.rest_dapi.get_document_list("_default", "_default")
            self.assertTrue(response.status_code == 200,
                            "Getting list of documents failed for database {}".format(bucket.name))

            self.log.info("Response code for getting list of documents {}".format(response.status_code))

            response_dict = json.loads(response.content)
            document_list = response_dict["docs"]
            document_name_check_list = []
            for documents in document_list:
                document_name_check_list.append(documents['meta_id'])
            document_name_list.sort()
            document_name_check_list.sort()
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
            self.log.info("Response code for insertion of doc {}".format(response.status_code))

            self.assertTrue(response.status_code == 201,
                            "Insertion failed for database {}".format(bucket.name))

            response = self.rest_dapi.get_doc("k", "_default", "_default")
            self.log.info("Response code to get document {}".format(response.status_code))

            self.assertTrue(response.status_code == 200,
                            "Getting document failed for database {}".format(bucket.name))

            response = self.rest_dapi.insert_subdoc("k",
                                                    [{"type": "insert", "path": "counter3", "value": 4},
                                                     {"type": "insert", "path": "counter1", "value": 10}],
                                                    "_default", "_default")
            self.log.info("Response code for insertion of subdocument {}".format(response.status_code))

            self.assertTrue(response.status_code == 200,
                            "Sub doc insertion failed for database {}".format(bucket.name))

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
            self.log.info("Response code for insertion of document {}".format(response.status_code))

            self.assertTrue(response.status_code == 201,
                            "Insertion failed for database {}".format(bucket.name))

            response = self.rest_dapi.get_doc("k", "_default", "_default")
            self.log.info("Response code to get document {}".format(response.status_code))

            self.assertTrue(response.status_code == 200,
                            "Getting document failed for database {}".format(bucket.name))

            response = self.rest_dapi.insert_subdoc("k",
                                                    [{"type": "insert", "path": "counter3", "value": 4},
                                                     {"type": "insert", "path": "counter1", "value": 10}],
                                                    "_default", "_default")
            self.log.info("Response code for insertion of subdocument {}".format(response.status_code))

            self.assertTrue(response.status_code == 200,
                            "Sub doc insertion failed for database {}".format(bucket.name))

            response = self.rest_dapi.get_subdoc("k", [{"type": "get", "path": "counter3"}], "_default", "_default")
            self.log.info("printing response status code for getting subdoc {}".format(response.status_code))
            self.assertTrue(response.status_code == 200, "Getting subdoc failed for database {}".format(bucket.name))

    def test_create_scope(self):
        for bucket in self.buckets:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": bucket.serverless.dapi,
                                          "access_token": bucket.serverless.nebula_endpoint.rest_username,
                                          "access_secret": bucket.serverless.nebula_endpoint.rest_password})
            self.log.info("Creation of scope for database {}".format(bucket.name))

            scope_name, scope_suffix = "scope", 0
            scope_name_list = ["_default", "_system"]
            for i in range(self.number_of_scopes):
                scope_suffix += 1
                scope_name = "scope" + str(scope_suffix)
                scope_name_list.append(scope_name)
                response = self.rest_dapi.create_scope({"scopeName": scope_name})
                self.log.info("response for creation of scope: {}".format(response.status_code))
                self.assertTrue(response.status_code == 200,
                                "Creation of scope failed for database {}".format(bucket.name))

            response = self.rest_dapi.get_scope_list()
            self.log.info("status code for getting list of scope: {}".format(response.status_code))
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

    def test_create_collection(self):
        for bucket in self.buckets:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": bucket.serverless.dapi,
                                          "access_token": bucket.serverless.nebula_endpoint.rest_username,
                                          "access_secret": bucket.serverless.nebula_endpoint.rest_password})
            self.log.info("Creation of collection for database {}".format(bucket.name))

            scope = "testScope"
            response = self.rest_dapi.create_scope({"scopeName": scope})
            self.log.info("Response code for creation of scope {}".format(response.status_code))
            self.assertTrue(response.status_code == 200,
                            "Creation of scope failed for database {}".format(bucket.name))

            collection_name, collection_suffix = "collection", 0
            collection_name_list = []
            for i in range(self.number_of_collections):
                collection_suffix += 1
                collection_name = "collection" + str(collection_suffix)
                collection_name_list.append(collection_name)
                response = self.rest_dapi.create_collection(scope, {"name": collection_name})
                self.log.info("Response code for creation of collection: {}".format(response.status_code))
                self.assertTrue(response.status_code == 200,
                                "Creation of collection failed for database {}".format(bucket.name))

            response = self.rest_dapi.get_collection_list(scope)

            self.log.info("Response code for get list of collection {}".format(response.status_code))

            self.assertTrue(response.status_code == 200,
                            "Getting list of collections failed for database {}".format(bucket.name))

            collection_list = json.loads(response.content)
            collection_list = collection_list["collections"]

            temp_collection_list = []
            for collection in collection_list:
                temp_collection_list.append(collection["Name"])

            temp_collection_list.sort()
            collection_name_list.sort()
            self.assertTrue(temp_collection_list == collection_name_list,
                            "Wrong collection/s received for database {}".format(bucket.name))

    def test_delete_collection(self):
        for bucket in self.buckets:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": bucket.serverless.dapi,
                                          "access_token": bucket.serverless.nebula_endpoint.rest_username,
                                          "access_secret": bucket.serverless.nebula_endpoint.rest_password})
            self.log.info("Deletion of collection for database {}".format(bucket.name))

            collection_name = "testCollection"
            response = self.rest_dapi.create_collection("_default", {"name": collection_name})
            self.log.info("Response code for creation of collection {}".format(response.status_code))
            self.assertTrue(response.status_code == 200,
                            "Create collection for database {}".format(bucket.name))

            response = self.rest_dapi.delete_collection("_default", collection_name)
            self.log.info("Response code for deletion of collection {}".format(response.status_code))
            self.assertTrue(response.status_code == 200,
                            "Deletion of collection failed for database {}".format(bucket.name))

            response = self.rest_dapi.get_collection_list("_default")
            self.log.info("Response code to get list of collection {}".format(response.status_code))
            collection_list = json.loads(response.content)
            collection_list = collection_list["collections"]

            collection_name_list = []
            for collection in collection_list:
                collection_name_list.append(collection["Name"])

            for collection in collection_name_list:
                self.assertTrue(collection != collection_name,
                                "Getting delete collection: {} for database {}".format(collection_name, bucket.name))
            # negative test case
            response = self.rest_dapi.get_document_list("_default", collection_name)
            self.log.info("Response code to get list of documents {}".format(response.status_code))
            self.assertTrue(response.status_code == 404,
                            "Getting empty list for deleted collection for database {}".format(bucket.name))

    def test_delete_scope(self):
        for bucket in self.buckets:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": bucket.serverless.dapi,
                                          "access_token": bucket.serverless.nebula_endpoint.rest_username,
                                          "access_secret": bucket.serverless.nebula_endpoint.rest_password})
            self.log.info("Deletion of scope for database {}".format(bucket.name))

            scope_name = "testScope"
            response = self.rest_dapi.create_scope({"scopeName": scope_name})
            self.log.info("Response code for creation of scope {}".format(response.status_code))
            self.assertTrue(response.status_code == 200,
                            "Create scope {} failed for database {}".format(scope_name, bucket.name))

            response = self.rest_dapi.delete_scope(scope_name)
            self.log.info("Response code for deletion of scope {}".format(response.status_code))
            self.assertTrue(response.status_code == 200,
                            "Deletion of scope {} failed for database {}".format(scope_name, bucket.name))

            response = self.rest_dapi.get_scope_list()
            self.log.info("Response code to get list of scope {}".format(response.status_code))
            scope_list = json.loads(response.content)
            scope_list = scope_list["scopes"]
            scope_name_list = []
            for scope in scope_list:
                scope_name_list.append(scope["Name"])

            for scope in scope_name_list:
                self.assertTrue(scope != scope_name,
                                "Getting deleted scope: {} for database {}".format(scope_name, bucket.name))
            # negative test case
            response = self.rest_dapi.get_collection_list(scope_name)
            self.log.info("response code for getting collections for deleted scope {}".format(response.status_code))
            self.assertTrue(response.status_code == 404,
                            "Getting empty list for deleted scope for database {}".format(bucket.name))

    def test_execute_query(self):
        for bucket in self.buckets:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": bucket.serverless.dapi,
                                          "access_token": bucket.serverless.nebula_endpoint.rest_username,
                                          "access_secret": bucket.serverless.nebula_endpoint.rest_password})
            self.log.info("Execute query for database {}".format(bucket.name))

            gen_obj = doc_generator("key", 0, self.number_of_docs, key_size=self.key_size,
                                    doc_size=self.value_size, randomize_value=self.randomize_value)
            document_list = []
            for i in range(self.number_of_docs):
                if gen_obj.has_next():
                    key, doc = gen_obj.next()
                    doc = doc.toMap()
                    doc = dict(doc)
                    key_document = {"key": key, "doc": doc}
                    document_list.append(key_document)
                    response = self.rest_dapi.insert_doc(key, doc, "_default", "_default")
                    self.log.info("Response code for inserting doc: {}".format(response.status_code))

                    self.assertTrue(response.status_code == 201,
                                    "Document insertion failed with doc size {} "
                                    "and key size {} for database {}".format(
                                        self.value_size, self.key_size, self.number_of_docs))

            for doc in document_list:
                key = doc["key"]
                document = doc["doc"]
                response = self.rest_dapi.get_doc(key, "_default", "_default")
                get_doc = json.loads(response.content)
                get_doc = get_doc["doc"]
                self.assertTrue(response.status_code == 200,
                                "Reading doc failed with  {} for database: {}".format(key, bucket.name))
                self.assertTrue(document == get_doc,
                                "Wrong document received for database: {}".format(bucket.name))

            # Executing simple query
            query = {"query": "select * from `_default` limit 2;"}
            response = self.rest_dapi.execute_query(query, "_default")
            self.log.info("Response code for execution of query {}".format(response.status_code))
            self.assertTrue(response.status_code == 200,
                            "Query Execution failed for database {}".format(bucket.name))

            # Execute query with Named parameters
            query = {"query": "SELECT age, body FROM _default WHERE age=$age LIMIT 5",
                     "parameters": {"age": 10}}
            response = self.rest_dapi.execute_query(query, "_default")
            self.log.info("Response status code for execute query: {}".format(response.status_code))
            self.assertTrue(response.status_code == 200, "Query execution failed for database {}".format(bucket.name))
            # Invalid Query
            query = {"query": "SLECT city, body FROM _default WHERE age=$age LIMIT 5",
                     "parameters": {"age": 5}}
            response = self.rest_dapi.execute_query(query, "_default")
            self.log.info("Response code for execution of invalid query: {}".format(response.status_code))
            self.assertTrue(response.status_code == 400,
                            "Query Execution passed with invalid query for database: {}".format(bucket.name))
            val = json.loads(response.content)["error"]["message"]
            self.log.info(val)
