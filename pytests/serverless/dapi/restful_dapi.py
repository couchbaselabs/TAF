from basetestcase import BaseTestCase
from BucketLib.bucket import Bucket
from ServerlessLib.dapi.dapi import RestfulDAPI
from couchbase_helper.documentgenerator import doc_generator, BatchedDocumentGenerator
import json
import string
import random
import threading


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
        self.number_of_threads = self.input.param("number_of_threads", 1)
        self.error_message = self.input.param("error_msg", None)

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
            task = self.bucket_util.async_create_database(cluster=self.cluster,
                                                          bucket=bucket,
                                                          dataplane_id=self.dataplane_id)
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
                    response = self.rest_dapi.insert_doc(key, doc, "_default", "_default", 1200)
                    # negative test case - key or value is too large
                    if self.error_message is not None:
                        self.assertTrue(response.status_code == 409,
                                        "Negative test failed with unsupported "
                                        "keys for database: {}".format(bucket.name))
                        error_msg = json.loads(response.content)["error"]["message"]
                        self.assertTrue(error_msg == self.error_message,
                                        "Wrong error msg for unsupported key/value doc: {}".format(error_msg))
                    else:
                        document_list.append({"key": key, "doc": doc})
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

            # create collection query
            query = {"query": "CREATE COLLECTION country"}
            response = self.rest_dapi.execute_query(query, "_default")
            self.log.info("Response code for create collection query: {}".format(response.status_code))
            self.assertTrue(response.status_code == 200,
                            "Create collection query failed for database {}".format(bucket.name))
            response = self.rest_dapi.get_collection_list("_default")
            self.assertTrue(response.status_code == 200,
                            "Get list of collection failed for database {}".format(bucket.name))
            if "country" not in [collection["Name"] for collection in
                                    (json.loads(response.content)["collections"])]:
                self.log.critical("Collection does not get created with query")
            # Insert query
            query = {"query": 'INSERT INTO _default '
                              '(KEY, VALUE) VALUES ("key1", { "type" : "hotel", "name" : "new hotel" });'}
            response = self.rest_dapi.execute_query(query, "_default")
            self.log.info("Response code for execution of query {}".format(response.status_code))
            self.assertTrue(response.status_code == 200,
                            "Query Execution failed for database {}".format(bucket.name))
            response = self.rest_dapi.get_doc("key1", "_default", "_default",)
            self.assertTrue(response.status_code == 200,
                            "Get document failed for database {}".format(bucket.name))

            # select query
            query = {"query": "SELECT type, name FROM _default"}
            response = self.rest_dapi.execute_query(query, "_default")
            self.log.info("Response status code for execute query: {}".format(response.status_code))
            self.assertTrue(response.status_code == 200, "Query execution failed for database {}".format(bucket.name))
            # create primary index
            query = {"query": "CREATE PRIMARY INDEX idx_default_primary ON `_default` USING GSI"}
            response = self.rest_dapi.execute_query(query, "_default")
            self.log.info("Response code for creation of primary index: {}".format(response.status_code))
            # update query
            query = {"query": "UPDATE _default SET type = 'hostel' where name = 'new hostel'"}
            response = self.rest_dapi.execute_query(query, "_default")
            self.log.info("Response code for update query: {}".format(response.status_code))
            self.assertTrue(response.status_code == 200,
                            "Update query failed for database {}".format(bucket.name))
            # delete query
            query = {"query": "DELETE from _default"}
            response = self.rest_dapi.execute_query(query, "_default")
            self.log.info("Response code for delete doc query: {}".format(response.status_code))
            self.assertTrue(response.status_code == 200,
                            "Delete query failed for database {}".format(bucket.name))
            response = self.rest_dapi.get_doc("key1", "_default", "_default")
            self.log.info("Response code to get deleted doc: {}".format(response.status_code))
            self.assertTrue(response.status_code == 404,
                            "Getting deleted document for database {}".format(bucket.name))
            # drop collection query
            query = {"query": "DROP COLLECTION country"}
            response = self.rest_dapi.execute_query(query, "_default")
            self.log.info("Response code for drop of collection: {}".format(response.status_code))
            self.assertTrue(response.status_code == 200,
                            "Drop collection query failed for database {}".format(bucket.name))
            response = self.rest_dapi.get_collection_list("_default")
            self.assertTrue(response.status_code == 200,
                            "Get list of collection failed for database {}".format(bucket.name))
            if "country" in [collection["Name"] for collection in
                                    (json.loads(response.content)["collections"])]:
                self.log.critical("Drop collection query failed for database {}".format(bucket.name))
            # Invalid Query
            query = {"query": "SLECT city, body FROM _default WHERE age=$age LIMIT 5",
                     "parameters": {"age": 5}}
            response = self.rest_dapi.execute_query(query, "_default")
            self.log.info("Response code for execution of invalid query: {}".format(response.status_code))
            self.assertTrue(response.status_code == 400,
                            "Query Execution passed with invalid query for database: {}".format(bucket.name))
            val = json.loads(response.content)["error"]["message"]
            self.log.info(val)

    def test_get_bulk_doc(self):
        self.result = True

        def bulk_get_thread(start_prefix, end_prefix, bucket_name):

            # insertion of documents
            gen_obj = doc_generator("key", start_prefix, end_prefix,
                                    key_size=self.key_size,
                                    doc_size=self.value_size,
                                    randomize_value=self.randomize_value)
            batched_gen_obj = BatchedDocumentGenerator(gen_obj,
                                                       batch_size_int=10)

            while batched_gen_obj.has_next():
                kv_dapi = []
                kv = {}
                key_doc_list = batched_gen_obj.next_batch()
                for key_doc in key_doc_list:
                    key_doc = tuple(key_doc)
                    key = key_doc[0]
                    doc = dict(key_doc[1].toMap())
                    kv[key] = doc
                    kv_dapi.append({"id": key, "value": doc})

                # insert bulk document
                response = self.rest_dapi.insert_bulk_document("_default",
                                                               "_default",
                                                               kv_dapi)
                if response is None or response.status_code != 200:
                    self.result = False
                    self.log.critical(response)
                    self.log.critical("Bulk insert failed for {}: Response: {}".format(
                        bucket_name, response.status_code))
                    return
                # bulk keys get in response after insertion of bulk document
                keys = json.loads(response.content).get("docs", [])
                # compare both the list
                if [key["id"] for key in keys].sort() != kv.keys().sort():
                    self.log.critical("Ambiguous keys get inserted for database {}".format(bucket_name))
                    self.log.critical("Response: ".format(response))
                    self.result = False
                    return

                # Get bulk document
                response = self.rest_dapi.get_bulk_document("_default", "_default", tuple(kv.keys()))
                if response is None or response.status_code != 200:
                    self.result = False
                    self.log.critical("Response: ".format(response))
                    self.log.critical("Bulk get failed for {}: Response: ".format(
                        bucket_name, response.status_code))
                    return
                # list of key document pair get from response
                bulk_key_document = json.loads(response.content).get("docs", [])

                for key_doc in bulk_key_document:
                    if key_doc.get("doc") != kv.get(key_doc["id"]):
                        self.log.critical("{}: Value mismatch for key: "
                                          "{}. Actual {}, Expected {}".
                                          format(bucket_name, key,
                                                 key_doc.get("doc"),
                                                 kv.get(key_doc["id"])))
                        self.log.critical("Response: ".format(response))
                        self.result = False
                        return

        for bucket in self.buckets:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": bucket.serverless.dapi,
                                          "access_token": bucket.serverless.nebula_endpoint.rest_username,
                                          "access_secret": bucket.serverless.nebula_endpoint.rest_password})
            self.log.info("Insert bulk document for database {}".format(bucket.name))

            document_per_thread = self.number_of_docs // self.number_of_threads

            start, end, thread_list = 0, self.number_of_docs, []
            for number in range(self.number_of_threads):
                start_key, end_key = start, start + document_per_thread
                start = start + document_per_thread
                if end_key > end:
                    end_key = end
                thread = threading.Thread(target=bulk_get_thread,
                                          args=(start_key, end_key, bucket.name))
                thread_list.append(thread)

            for thread in thread_list:
                thread.start()

            for thread in thread_list:
                thread.join()

            self.assertTrue(self.result, "Check the test logs...")

    def test_delete_bulk_doc(self):
        self.result = True

        def bulk_delete_thread(start_prefix, end_prefix, bucket_name):

            # insertion of documents
            gen_obj = doc_generator("key", start_prefix, end_prefix,
                                    key_size=self.key_size,
                                    doc_size=self.value_size,
                                    randomize_value=self.randomize_value)
            batched_gen_obj = BatchedDocumentGenerator(gen_obj,
                                                       batch_size_int=10)

            while batched_gen_obj.has_next():
                kv_dapi = []
                kv = {}
                key_doc_list = batched_gen_obj.next_batch()
                for key_doc in key_doc_list:
                    key_doc = tuple(key_doc)
                    key = key_doc[0]
                    doc = dict(key_doc[1].toMap())
                    kv[key] = doc
                    kv_dapi.append({"id": key, "value": doc})

                # insert bulk document
                response = self.rest_dapi.insert_bulk_document("_default",
                                                               "_default",
                                                               kv_dapi)
                if response is None or response.status_code != 200:
                    self.result = False
                    self.log.critical(response)
                    self.log.critical("Bulk insert failed for {}: Response: {}".format(
                        bucket_name, response.status_code))
                    return
                # bulk keys get in response after insertion of bulk document
                keys = json.loads(response.content).get("docs", [])
                # compare both the list
                if [key["id"] for key in keys].sort() != kv.keys().sort():
                    self.log.critical("Ambiguous keys get inserted for database {}".format(bucket_name))
                    self.log.critical("Response: ".format(response))
                    self.result = False
                    return

                # Delete bulk document
                response = self.rest_dapi.delete_bulk_document("_default",
                                                               "_default",
                                                               tuple(kv.keys()))

                if response is None or response.status_code != 200:
                    self.result = False
                    self.log.critical(response)
                    self.log.critical("Bulk deletion failed for {}: Response: {}".format(
                        bucket_name, response.status_code))
                    return

                keys = json.loads(response.content).get("docs", [])

                if [key["id"] for key in keys].sort() != kv.keys().sort():
                    self.log.critical("Ambiguous keys get deleted for database {}".format(bucket_name))
                    self.log.critical("Response: ".format(response))
                    self.result = False
                    return

        for bucket in self.buckets:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": bucket.serverless.dapi,
                                          "access_token": bucket.serverless.nebula_endpoint.rest_username,
                                          "access_secret": bucket.serverless.nebula_endpoint.rest_password})
            self.log.info("Delete bulk document for database {}".format(bucket.name))

            document_per_thread = self.number_of_docs // self.number_of_threads

            start, end, thread_list = 0, self.number_of_docs, []

            for number in range(self.number_of_threads):
                start_key, end_key = start, start + document_per_thread
                start = start + document_per_thread
                if end_key > end:
                    end_key = end
                thread = threading.Thread(target=bulk_delete_thread,
                                          args=(start_key, end_key, bucket.name))
                thread_list.append(thread)

            for thread in thread_list:
                thread.start()

            for thread in thread_list:
                thread.join()

            self.assertTrue(self.result, "Check the test logs...")

    def test_update_bulk_doc(self):
        self.result = True

        def bulk_upsert_thread(start_prefix, end_prefix, bucket_name):

            # insertion of documents
            gen_obj = doc_generator("key", start_prefix, end_prefix,
                                    key_size=self.key_size,
                                    doc_size=self.value_size,
                                    randomize_value=self.randomize_value)
            batched_gen_obj = BatchedDocumentGenerator(gen_obj,
                                                       batch_size_int=10)

            while batched_gen_obj.has_next():
                kv_dapi = []
                kv = {}
                key_doc_list = batched_gen_obj.next_batch()
                for key_doc in key_doc_list:
                    key_doc = tuple(key_doc)
                    key = key_doc[0]
                    doc = dict(key_doc[1].toMap())
                    kv[key] = doc
                    kv_dapi.append({"id": key, "value": doc})

                # insert bulk document
                response = self.rest_dapi.insert_bulk_document("_default",
                                                               "_default",
                                                               kv_dapi)
                if response is None or response.status_code != 200:
                    self.result = False
                    self.log.critical(response)
                    self.log.critical("Bulk insert failed for {}: Response: {}".format(
                        bucket_name, response.status_code))
                    return
                # bulk keys get in response after insertion of bulk document
                keys = json.loads(response.content).get("docs", [])
                # compare both the list
                if [key["id"] for key in keys].sort() != kv.keys().sort():
                    self.log.critical("Ambiguous keys get inserted for database {}".format(bucket_name))
                    self.log.critical("Response: ".format(response))
                    self.result = False
                    return

                # updation of kv_dapi
                for key_value in kv_dapi:
                    key_value['value']['update'] = True

                # update bulk document
                response = self.rest_dapi.update_bulk_document("_default",
                                                               "_default",
                                                               kv_dapi)
                if response is None or response.status_code != 200:
                    self.result = False
                    self.log.critical(response)
                    self.log.critical("Bulk update failed for {}: Response: {}".format(
                        bucket_name, response.status_code))
                    return

                # bulk keys get in response after insertion of bulk document
                keys = json.loads(response.content).get("docs", [])
                # compare both the list
                if [key["id"] for key in keys].sort() != kv.keys().sort():
                    self.log.critical("Ambiguous keys get updated for database {}".format(bucket_name))
                    self.log.critical("Response: ".format(response))
                    self.result = False
                    return

        for bucket in self.buckets:
            self.rest_dapi = RestfulDAPI({"dapi_endpoint": bucket.serverless.dapi,
                                          "access_token": bucket.serverless.nebula_endpoint.rest_username,
                                          "access_secret": bucket.serverless.nebula_endpoint.rest_password})
            self.log.info("Insert bulk document for database {}".format(bucket.name))

            document_per_thread = self.number_of_docs // self.number_of_threads

            start, end, thread_list = 0, self.number_of_docs, []

            for number in range(self.number_of_threads):
                start_key, end_key = start, start + document_per_thread
                start = start + document_per_thread
                if end_key > end:
                    end_key = end
                thread = threading.Thread(target=bulk_upsert_thread,
                                          args=(start_key, end_key, bucket.name))
                thread_list.append(thread)

            for thread in thread_list:
                thread.start()

            for thread in thread_list:
                thread.join()

            self.assertTrue(self.result, "Check the test logs...")
