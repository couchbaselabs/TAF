"""
Created on 29-January-2024

@author: abhay.aggrawal@couchbase.com
"""
import json
import time
import random
import requests
from Queue import Queue
from couchbase_utils.capella_utils.dedicated import CapellaUtils
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI
from Columnar.columnar_base import ColumnarBaseTest
from BucketLib.bucket import Bucket


class StandaloneCollection(ColumnarBaseTest):
    def setUp(self):
        super(StandaloneCollection, self).setUp()
        self.cluster = self.tenant.columnar_instances[0]

        self.initial_doc_count = self.input.param("initial_doc_count", 100)
        self.doc_size = self.input.param("doc_size", 1024)

        if not self.columnar_spec_name:
            self.columnar_spec_name = "regressions.standalone_collection"
        self.columnar_spec = self.cbas_util.get_columnar_spec(self.columnar_spec_name)

        self.columnar_spec["database"]["no_of_databases"] = self.input.param("no_of_databases", 1)
        self.columnar_spec["dataverse"]["no_of_dataverses"] = self.input.param(
            "no_of_dataverses", 1)
        self.columnar_spec["standalone_dataset"][
            "num_of_standalone_coll"] = self.input.param(
            "num_of_standalone_coll", 0)
        self.columnar_spec["standalone_dataset"]["primary_key"] = [{"name": "string", "email": "string"}]
        self.capellaAPI = CapellaAPI(self.pod.url_public, '', '', self.tenant.user, self.tenant.pwd, '')

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def capella_provisioned_cluster_setup(self):

        self.remote_cluster = self.cb_clusters['C1']
        resp = (self.capellaAPI.create_control_plane_api_key(self.tenant.id, 'init api keys')).json()
        self.capellaAPI.cluster_ops_apis.SECRET = resp['secretKey']
        self.capellaAPI.cluster_ops_apis.ACCESS = resp['id']
        self.capellaAPI.cluster_ops_apis.bearer_token = resp['token']
        self.capellaAPI.org_ops_apis.SECRET = resp['secretKey']
        self.capellaAPI.org_ops_apis.ACCESS = resp['id']
        self.capellaAPI.org_ops_apis.bearer_token = resp['token']
        resp = self.capellaAPI.cluster_ops_apis.add_CIDR_to_allowed_CIDRs_list(self.tenant.id,
                                                                               self.tenant.project_id,
                                                                               self.remote_cluster.id, "0.0.0.0/0")
        if resp.status_code == 201 or resp.status_code == 422:
            self.log.info("Added allowed IP 0.0.0.0/0")
        else:
            self.fail("Failed to add allowed IP")
        remote_cluster_certificate_request = (
            self.capellaAPI.cluster_ops_apis.get_cluster_certificate(self.tenant.id, self.tenant.project_id,
                                                                     self.remote_cluster.id))
        if remote_cluster_certificate_request.status_code == 200:
            self.remote_cluster_certificate = (remote_cluster_certificate_request.json()["certificate"])
        else:
            self.fail("Failed to get cluster certificate")

        # creating bucket scope and collection to pump data
        bucket_name = "hotel"
        scope = None
        collection = None

        resp = self.capellaAPI.cluster_ops_apis.create_bucket(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.remote_cluster.id,
                                                              bucket_name, "couchbase", "couchstore", 1000, "seqno",
                                                              "majorityAndPersistActive", 1, True, 1000000)
        buckets = json.loads(CapellaUtils.get_all_buckets(self.pod, self.tenant, self.remote_cluster)
                             .content)["buckets"]["data"]
        for bucket in buckets:
            bucket = bucket["data"]
            bucket_obj = Bucket({
                Bucket.name: bucket["name"],
                Bucket.ramQuotaMB: bucket["memoryAllocationInMb"],
                Bucket.replicaNumber: bucket["replicas"],
                Bucket.conflictResolutionType:
                    bucket["bucketConflictResolution"],
                Bucket.flushEnabled: bucket["flush"],
                Bucket.durabilityMinLevel: bucket["durabilityLevel"],
                Bucket.maxTTL: bucket["timeToLive"],
            })
            bucket_obj.uuid = bucket["id"]
            bucket_obj.stats.itemCount = bucket["stats"]["itemCount"]
            bucket_obj.stats.memUsed = bucket["stats"]["memoryUsedInMib"]
            self.remote_cluster.buckets.append(bucket_obj)

        if resp.status_code == 201:
            self.bucket_id = resp.json()["id"]
            self.log.info("Bucket created successfully")
        else:
            self.fail("Error creating bucket in remote_cluster")

        if bucket_name and scope and collection:
            self.remote_collection = "{}.{}.{}".format(bucket_name, scope, collection)
        else:
            self.remote_collection = "{}.{}.{}".format(bucket_name, "_default", "_default")
        resp = self.capellaAPI.cluster_ops_apis.get_cluster_certificate(self.tenant.id,
                                                                        self.tenant.project_id,
                                                                        self.remote_cluster.id)
        if resp.status_code == 200:
            self.remote_cluster_certificate = (resp.json())["certificate"]
        else:
            self.fail("Failed to get cluster certificate")

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)

        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.cluster):
            self.fail("Error while deleting cbas entities")

        super(StandaloneCollection, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished", stage="Teardown")

    def start_source_ingestion(self, no_of_docs=1000000, doc_size=100000):
        remote_collections = []
        self.include_external_collections = dict()
        remote_collection = [self.remote_collection]
        self.include_external_collections["remote"] = remote_collection
        if "remote" in self.include_external_collections:
            remote_collections = set(self.include_external_collections["remote"])

        for collection in remote_collections:
            bucket = collection.split(".")[0]
            scope = collection.split(".")[1]
            collection = collection.split(".")[2]
            url = self.input.param("sirius_url", "127.0.0.1:4000")
            data = {
                "identifierToken": "hotel",
                "dbType": "couchbase",
                "username": self.remote_cluster.username,
                "password": self.remote_cluster.password,
                "connectionString": "couchbases://" + str(self.remote_cluster.srv),
                "extra": {
                    "bucket": bucket,
                    "scope": scope,
                    "collection": collection,
                },
                "operationConfig": {
                    "start": 0,
                    "end": no_of_docs,
                    "docSize": doc_size,
                    "template": "hotel"
                }
            }
            if url is not None:
                url = "http://" + url + "/bulk-create"
                response = requests.post(url, json=data)
                if response.status_code != 200:
                    self.log.error("Failed to start loader for remote collection")

    def wait_for_source_ingestion(self, no_of_docs=100000, timeout=1000000):
        start_time = time.time()
        remote_collection = [self.remote_collection]
        while time.time() < start_time + timeout:
            self.log.info("Waiting for data to be loaded in source databases")
            for collection in remote_collection:
                resp = self.capellaAPI.cluster_ops_apis.fetch_bucket_info(self.tenant.id,
                                                                          self.tenant.project_id,
                                                                          self.remote_cluster.id,
                                                                          self.bucket_id)
                if resp.status_code == 200 and (resp.json())["stats"]["itemCount"] == no_of_docs:
                    self.log.info("Doc loading complete for remote collection: {}".format(collection))
                    remote_collection.remove(collection)
            final_set = remote_collection
            if len(final_set) == 0:
                self.log.info("Doc loading is complete for all sources")
                return True
            time.sleep(30)
        self.log.error("Failed to wait for ingestion timeout {} sec reached".format(timeout))
        return False

    def test_create_drop_standalone_collection_duplicate_key(self):
        no_of_collection = self.input.param("num_of_standalone_coll", 1)
        key = self.input.param("key", None)
        expected_error=  self.input.param("error_message", None)
        for i in range(0, no_of_collection):
            dataset_name = self.cbas_util.generate_name()
            cmd = "Create Dataset {} Primary Key({})".format(dataset_name, key)
            status, metrics, errors, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster, cmd)
            if not self.cbas_util.validate_error_in_response(status, errors, expected_error):
                self.fail("Able to create collection with duplicate keys")
    def test_create_drop_standalone_collection(self):
        database_name = self.input.param("database", "Default")
        dataverse_name = self.input.param("dataverse", "Default")
        no_of_collection = self.input.param("num_of_standalone_coll", 1)
        key = json.loads(self.input.param("key", None)) if self.input.param("key", None) is not None else None
        primary_key = dict()
        validate_error = self.input.param("validate_error", False)
        error_message = str(self.input.param("error_message", None))
        if key is None:
            primary_key = None
        else:
            for key_value in key:
                primary_key[str(key_value)] = str(key[key_value])
        jobs = Queue()
        results = []
        for i in range(no_of_collection):
            dataset_obj = self.cbas_util.create_standalone_dataset_obj(self.cluster, database_name=database_name,
                                                                       dataverse_name=dataverse_name)
            jobs.put((self.cbas_util.create_standalone_collection,
                      {"cluster": self.cluster, "collection_name": dataset_obj[0].name,
                       "dataverse_name": dataset_obj[0].dataverse_name,
                       "database_name": dataset_obj[0].database_name, "primary_key": primary_key,
                       "validate_error_msg": validate_error, "expected_error": error_message}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )

        if not all(results):
            self.fail("Failed to create some collection with key {0}".format(str(key)))
        if validate_error:
            return

        datasets = self.cbas_util.get_all_dataset_objs()
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name,
                       "no_of_docs": self.initial_doc_count, "document_size": self.doc_size}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )

        if not all(results):
            self.fail("Failed to load data into standalone collection")

    def test_create_drop_standalone_collection_already_exist(self):
        no_of_collection = self.input.param("num_of_standalone_coll", 1)
        database_name = self.input.param("database", "Default")
        dataverse_name = self.input.param("dataverse", "Default")
        validate_error = self.input.param("validate_error", False)
        error_message = str(self.input.param("error_message", None))
        dataset_objs = []
        jobs = Queue()
        results = []
        for i in range(no_of_collection):
            dataset_obj = self.cbas_util.create_standalone_dataset_obj(self.cluster, database_name=database_name,
                                                                       dataverse_name=dataverse_name)
            dataset_objs.append(dataset_obj)
            jobs.put((self.cbas_util.create_standalone_collection,
                      {"cluster": self.cluster, "collection_name": dataset_obj[0].name,
                       "dataverse_name": dataset_obj[0].dataverse_name,
                       "database_name": dataset_obj[0].database_name}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )

        if not all(results):
            self.fail("Failed to create some collection")

        for i in range(len(dataset_objs)):
            error_message_to_verify = error_message.format(dataset_objs[i][0].name)
            jobs.put((self.cbas_util.create_standalone_collection,
                      {"cluster": self.cluster, "collection_name": dataset_objs[i][0].name,
                       "if_not_exists": False,
                       "dataverse_name": dataset_objs[i][0].dataverse_name,
                       "database_name": dataset_objs[i][0].database_name, "primary_key": None,
                       "validate_error_msg": validate_error, "expected_error": error_message_to_verify}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )

        if not all(results):
            self.fail("Failed to create some collection")

    def test_drop_non_existing_standalone_collection(self):
        database_name = self.input.param("database", "Default")
        dataverse_name = self.input.param("dataverse", "Default")
        validate_error = self.input.param("validate_error", False)
        error_message = str(self.input.param("error_message", None))
        jobs = Queue()
        results = []
        for i in range(10):
            dataset_obj = self.cbas_util.create_standalone_dataset_obj(self.cluster, database_name=database_name,
                                                                       dataverse_name=dataverse_name)
            error_message_to_verify = error_message.format(dataset_obj[0].name, dataset_obj[0].database_name,
                                                           dataset_obj[0].dataverse_name)
            jobs.put((self.cbas_util.drop_dataset,
                      {"cluster": self.cluster, "dataset_name": dataset_obj[0].full_name,
                       "validate_error_msg": validate_error, "expected_error": error_message_to_verify}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )

        if not all(results):
            self.fail("Some non-existing collections were deleted")

    def test_synonym_standalone_collection(self):
        self.columnar_spec["synonym"]["no_of_synonyms"] = self.input.param(
            "num_of_synonyms", 1)
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)
        jobs = Queue()
        results = []
        synonyms = self.cbas_util.get_all_synonym_objs()

        for synonym in synonyms:
            # load data to standalone collections
            jobs.put((self.cbas_util.crud_on_standalone_collection,
                      {"cluster": self.cluster, "collection_name": synonym.name,
                       "dataverse_name": synonym.dataverse_name, "database_name": synonym.database_name,
                       "target_num_docs": self.initial_doc_count, "doc_size": self.doc_size,
                       "where_clause_for_delete_op": "avg_rating > 0.2"}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )

        if not all(results):
            self.fail("Failed to load data into standalone collection using synonyms")

        for synonym in synonyms:
            jobs.put((self.cbas_util.get_num_items_in_cbas_dataset,
                      {"cluster": self.cluster, "dataset_name": synonym.full_name}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )

        if not all(results):
            self.fail("Failed to run query on standalone collection using synonyms")

    def test_insert_document_size(self):
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)
        jobs = Queue()
        results = []
        datasets = self.cbas_util.get_all_dataset_objs()
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name,
                       "no_of_docs": self.initial_doc_count, "document_size": self.doc_size}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )

        if not all(results):
            self.fail("Failed to insert some doc of size {}".format(self.doc_size))

    def test_create_collection_as(self):
        self.capella_provisioned_cluster_setup()
        self.start_source_ingestion(self.initial_doc_count, self.doc_size)
        self.wait_for_source_ingestion(self.initial_doc_count)
        self.columnar_spec["remote_link"]["no_of_remote_links"] = self.input.param(
            "no_of_remote_links", 1)

        remote_link_properties = list()
        remote_link_properties.append(
            {"type": "couchbase", "hostname": str(self.remote_cluster.srv),
             "username": self.remote_cluster.username,
             "password": self.remote_cluster.password,
             "encryption": "full",
             "certificate": self.remote_cluster_certificate})
        self.columnar_spec["remote_link"]["properties"] = remote_link_properties
        self.columnar_spec["remote_dataset"]["include_collections"] = [self.remote_collection]
        self.columnar_spec["remote_dataset"]["num_of_remote_datasets"] = self.input.param("num_of_remote_coll", 1)

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.columnar_spec, self.bucket_util, False, remote_clusters=[self.remote_cluster])
        if not result:
            self.fail(msg)
        remote_dataset = self.cbas_util.get_all_dataset_objs("remote")[0]
        subquery = ["select name as name, email as email, reviews from {}",
                    "select name as name, email as email, reviews from {} where avg_rating > 0.4"]
        datasets = self.cbas_util.create_standalone_dataset_obj(self.cluster,
                                                                no_of_objs=self.input.param("num_of_standalone_dataset",
                                                                                            1),
                                                                primary_key={"name": "string", "email": "string"})
        for dataset in datasets:
            subquery_execute = random.choice(subquery).format(remote_dataset.full_name)
            self.cbas_util.create_standalone_collection(self.cluster, dataset.name,
                                                        dataverse_name=dataset.dataverse_name,
                                                        database_name=dataset.database_name,
                                                        primary_key=dataset.primary_key,
                                                        subquery=subquery_execute)

            # verify the data
            status, metrics, errors, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster,
                                                                                               subquery_execute)
            doc_count_in_dataset = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, remote_dataset.full_name)
            if len(result) != doc_count_in_dataset:
                self.fail("Document count mismatch in {}".format(dataset.full_name))

        # verify the data after the link is disconnected
        links = self.cbas_util.get_all_link_objs("couchbase")[0]
        self.cbas_util.disconnect_link(self.cluster, link_name=links.full_name)
        for dataset in datasets:
            doc_count_in_dataset = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, dataset.full_name)
            if doc_count_in_dataset == 0:
                self.fail("No document found after link is disconnected")

    def test_insert_duplicate_doc(self):
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)
        docs_to_insert = []
        for i in range(self.initial_doc_count):
            docs_to_insert.append(self.cbas_util.generate_docs(self.doc_size))
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        jobs = Queue()
        results = []
        for dataset in datasets:
            jobs.put((self.cbas_util.insert_into_standalone_collection,
                      {
                          "cluster": self.cluster, "collection_name": dataset.name,
                          "document": docs_to_insert, "dataverse_name": dataset.dataverse_name,
                          "database_name": dataset.database_name
                      }))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )
        if not all(results):
            self.fail("Failed to insert doc in standalone collection")

        # Re-insert the doc using insert statement
        for dataset in datasets:
            jobs.put((self.cbas_util.insert_into_standalone_collection,
                      {
                          "cluster": self.cluster, "collection_name": dataset.name,
                          "document": docs_to_insert, "dataverse_name": dataset.dataverse_name,
                          "database_name": dataset.database_name, "validate_error_msg": True,
                          "expected_error": "Inserting duplicate keys into the primary storage",
                          "expected_error_code": 23072
                      }))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )
        if not all(results):
            self.fail("Failed to insert doc in standalone collection")

    def test_insert_with_missing_primary_key(self):
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)
        docs_to_insert = []
        for i in range(self.initial_doc_count):
            doc = self.cbas_util.generate_docs(self.doc_size)
            del doc[self.input.param("remove_field", "email")]
            docs_to_insert.append(doc)
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        jobs = Queue()
        results = []
        for dataset in datasets:
            jobs.put((self.cbas_util.insert_into_standalone_collection,
                      {
                          "cluster": self.cluster, "collection_name": dataset.name,
                          "document": docs_to_insert, "dataverse_name": dataset.dataverse_name,
                          "database_name": dataset.database_name, "validate_error_msg": True,
                          "expected_error": "Type mismatch: missing a required field {}: string".format(
                              self.input.param("remove_field", "email")),
                          "expected_error_code": 23071
                      }))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )
        if not all(results):
            self.fail("Failed to insert doc in standalone collection")

    def test_crud_during_scaling(self):
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)
        jobs = Queue()
        results = []
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        for dataset in datasets:
            jobs.put((self.cbas_util.crud_on_standalone_collection,
                      {"cluster": self.cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name,
                       "target_num_docs": self.initial_doc_count, "time_for_crud_in_mins": 5,
                       "doc_size": self.doc_size, "where_clause_for_delete_op": "avg_rating > 0.2"}))

        jobs.put((self.columnar_utils.scale_instance,
                  {"pod": self.pod, "tenant": self.tenant, "project_id": self.cluster.project_id, "instance": self.cluster,
                   "nodes": 4}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )

        self.columnar_utils.wait_for_instance_scaling_operation(
            self.pod, self.tenant, self.cluster.project_id, self.cluster)

        if not all(results):
            self.fail("Failed to insert doc in standalone collection")

        # validate number of docs in standalone collection
        for dataset in datasets:
            doc_count = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, dataset.full_name)
            results.append(doc_count == self.initial_doc_count)

        if not all(results):
            self.fail("Doc count mismatch after scaling")

    def test_insert_atomicity(self):
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)
        data_to_add = []
        for i in range(self.initial_doc_count):
            data_to_add.append(self.cbas_util.generate_docs(self.doc_size))

        jobs = Queue()
        results = []

        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        for dataset in datasets:
            jobs.put((self.cbas_util.insert_into_standalone_collection,
                      {"cluster": self.cluster, "collection_name": dataset.name,
                       "document": data_to_add, "dataverse_name": dataset.dataverse_name,
                       "database_name": dataset.database_name}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=True
        )

        active_requests = self.cbas_util.get_all_active_requests(self.cluster)
        for request in active_requests:
            if str(request['statement']).startswith("INSERT INTO"):
                context_id = str(request['clientContextID'])
                self.cbas_util.delete_request(self.cluster, context_id)

        for dataset in datasets:
            doc_count = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, dataset.full_name)
            if doc_count[0] != 0:
                self.fail("Some documents were inserted after statement is aborted")

    def test_delete_atomicity(self):
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        jobs = Queue()
        results = []

        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.cluster, "collection_name": dataset.name, "dataverse_name": dataset.dataverse_name,
                       "database_name": dataset.database_name, "no_of_docs": self.initial_doc_count,
                       "document_size": self.doc_size}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )

        for dataset in datasets:
            jobs.put((self.cbas_util.delete_from_standalone_collection,
                      {"cluster": self.cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=True
        )

        active_requests = self.cbas_util.get_all_active_requests(self.cluster)
        for request in active_requests:
            if str(request['statement']).startswith("DELETE FROM"):
                context_id = str(request['clientContextID'])
                self.cbas_util.delete_request(self.cluster, context_id)

        for dataset in datasets:
            doc_count = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, dataset.full_name)
            if doc_count[0] != self.initial_doc_count:
                self.fail("Some documents were deleted after statement is aborted")

    def test_upsert_atomicity(self):
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        data_to_add = []
        for i in range(self.initial_doc_count):
            data_to_add.append(self.cbas_util.generate_docs(self.doc_size))

        jobs = Queue()
        results = []

        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        for dataset in datasets:
            jobs.put((self.cbas_util.insert_into_standalone_collection,
                      {"cluster": self.cluster, "collection_name": dataset.name,
                       "document": data_to_add, "dataverse_name": dataset.dataverse_name,
                       "database_name": dataset.database_name}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )
        data_to_modify = data_to_add
        for data in data_to_modify:
            data['phone'] = random.randint(1, 10000)

        for dataset in datasets:
            jobs.put((self.cbas_util.upsert_into_standalone_collection,
                      {"cluster": self.cluster, "collection_name": dataset.name,
                       "new_item": data_to_modify, "dataverse_name": dataset.dataverse_name,
                       "database_name": dataset.database_name}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=True
        )
        time.sleep(2)
        active_request = self.cbas_util.get_all_active_requests(self.cluster)
        for request in active_request:
            if str(request['statement']).startswith("UPSERT INTO"):
                context_id = str(request['clientContextID'])
                self.cbas_util.delete_request(self.cluster, context_id, username=self.cluster.api_access_key,
                                              password=self.cluster.api_secret_key)

        for dataset in datasets:
            doc_count = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, dataset.full_name)
            if doc_count[0] != self.initial_doc_count:
                self.fail("Some documents are missing")
            else:
                statement = "Select * from {}".format(dataset.full_name)
                status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(
                    self.cluster, statement)
                if status == "success":
                    for dicts in data_to_add:
                        if dicts not in results:
                            self.fail("Few docs were updated")
