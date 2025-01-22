"""
Created on 29-January-2024

@author: abhay.aggrawal@couchbase.com
"""
import json
import string
import random
import time
from queue import Queue

from Columnar.columnar_base import ColumnarBaseTest
from Jython_tasks.sirius_task import CouchbaseUtil


class StandaloneCollection(ColumnarBaseTest):
    def setUp(self):
        super(StandaloneCollection, self).setUp()
        self.columnar_cluster = self.tenant.columnar_instances[0]
        self.remote_cluster = None
        if len(self.tenant.clusters) > 0:
            self.remote_cluster = self.tenant.clusters[0]
            self.couchbase_doc_loader = CouchbaseUtil(
                task_manager=self.task_manager,
                hostname=self.remote_cluster.master.ip,
                username=self.remote_cluster.master.rest_username,
                password=self.remote_cluster.master.rest_password,
            )

        self.initial_doc_count = self.input.param("initial_doc_count", 100)
        self.doc_size = self.input.param("doc_size", 1024)

        if not self.columnar_spec_name:
            self.columnar_spec_name = "full_template"
        self.columnar_spec = self.cbas_util.get_columnar_spec(self.columnar_spec_name)

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)

        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.columnar_cluster):
            self.fail("Error while deleting cbas entities")

        if hasattr(self, "remote_cluster") and self.remote_cluster:
            self.delete_all_buckets_from_capella_cluster(
                self.tenant, self.remote_cluster)

        super(StandaloneCollection, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished", stage="Teardown")

    def test_create_drop_standalone_collection_duplicate_key(self):
        no_of_collection = self.input.param("num_standalone_collections", 1)
        key = self.input.param("key", None)
        expected_error = self.input.param("error_message", None)
        for i in range(0, no_of_collection):
            dataset_name = self.cbas_util.generate_name()
            cmd = "Create Dataset {} Primary Key({})".format(dataset_name, key)
            status, metrics, errors, result, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, cmd)
            if not self.cbas_util.validate_error_and_warning_in_response(
                    status, errors, expected_error):
                self.fail("Able to create collection with duplicate keys")

    def test_create_drop_standalone_collection(self):
        database_name = self.input.param("database", "Default")
        dataverse_name = self.input.param("dataverse", "Default")
        no_of_collection = self.input.param("num_standalone_collections", 1)
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
            dataset_obj = self.cbas_util.create_standalone_dataset_obj(self.columnar_cluster, database_name=database_name,
                                                                       dataverse_name=dataverse_name)
            jobs.put((self.cbas_util.create_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": dataset_obj[0].name,
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
                      {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name,
                       "no_of_docs": self.initial_doc_count, "document_size": self.doc_size}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )

        if not all(results):
            self.fail("Failed to load data into standalone collection")

    def test_create_drop_standalone_collection_already_exist(self):
        no_of_collection = self.input.param("num_standalone_collections", 1)
        database_name = self.input.param("database", "Default")
        dataverse_name = self.input.param("dataverse", "Default")
        validate_error = self.input.param("validate_error", False)
        error_message = str(self.input.param("error_message", None))
        dataset_objs = []
        jobs = Queue()
        results = []
        for i in range(no_of_collection):
            dataset_obj = self.cbas_util.create_standalone_dataset_obj(self.columnar_cluster, database_name=database_name,
                                                                       dataverse_name=dataverse_name)
            dataset_objs.append(dataset_obj)
            jobs.put((self.cbas_util.create_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": dataset_obj[0].name,
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
                      {"cluster": self.columnar_cluster, "collection_name": dataset_objs[i][0].name,
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
            dataset_obj = self.cbas_util.create_standalone_dataset_obj(self.columnar_cluster, database_name=database_name,
                                                                       dataverse_name=dataverse_name)
            error_message_to_verify = error_message.format(dataset_obj[0].name, dataset_obj[0].database_name,
                                                           dataset_obj[0].dataverse_name)
            jobs.put((self.cbas_util.drop_dataset,
                      {"cluster": self.columnar_cluster, "dataset_name": dataset_obj[0].full_name,
                       "validate_error_msg": validate_error, "expected_error": error_message_to_verify}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )

        if not all(results):
            self.fail("Some non-existing collections were deleted")

    def test_synonym_standalone_collection(self):
        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=self.remote_cluster,
            external_collection_file_formats=["json"])

        if self.input.param("primary_key", None) is not None:
            self.columnar_spec["standalone_dataset"]["primary_key"] = json.loads(self.input.param("primary_key"))

        # create entities on columnar cluster based on spec file
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])

        if not result:
            self.fail(msg)
        jobs = Queue()
        results = []
        synonyms = self.cbas_util.get_all_synonym_objs()

        for synonym in synonyms:
            # load data to standalone collections
            jobs.put((
                self.cbas_util.crud_on_standalone_collection,
                {"cluster": self.columnar_cluster, "collection_name": synonym.name,
                 "dataverse_name": synonym.dataverse_name,
                 "database_name": synonym.database_name,
                 "target_num_docs": self.initial_doc_count,
                 "doc_size": self.doc_size,
                 "where_clause_for_delete_op": "avg_rating > 0.2",
                 "use_alias": True}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )

        if not all(results):
            self.fail("Failed to load data into standalone collection using synonyms")

        for synonym in synonyms:
            jobs.put((self.cbas_util.get_num_items_in_cbas_dataset,
                      {"cluster": self.columnar_cluster, "dataset_name": synonym.full_name}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )

        if not all(results):
            self.fail("Failed to run query on standalone collection using synonyms")

    def generate_large_document(self, doc_size_mb):
        size_bytes = doc_size_mb * 1024 * 1024
        large_doc = {}
        characters = string.ascii_letters + string.digits
        character_with_space = string.ascii_letters + string.digits + ' '

        # Generate an initial large random string
        random_string = ''.join(random.choices(character_with_space, k=3200))

        # Fill the document with random data
        for _ in range(size_bytes // 3200):  # To approximate the size
            field = ''.join(random.choices(characters, k=5))
            large_doc[field] = random_string

        # Adjust the size if necessary
        while True:
            large_doc_json = json.dumps(large_doc)
            current_size = len(large_doc_json.encode('utf-8'))

            if current_size > size_bytes:
                # Remove a random item if the document is too large
                large_doc.pop(next(iter(large_doc)))
            else:
                break

        # Ensure the document is exactly the requested size
        if current_size < size_bytes:
            # Add random characters to increase the size
            additional_size = size_bytes - current_size
            additional_data = ''.join(random.choices(character_with_space, k=additional_size))
            large_doc['extra_field'] = additional_data

        large_doc["name"] = ''.join(random.choices(character_with_space, k=10))
        large_doc["email"] = ''.join(random.choices(character_with_space, k=10))

        # Print the size for confirmation
        final_size = len(json.dumps(large_doc).encode('utf-8'))
        self.log.info(f"Size of document: {final_size} bytes")

        return large_doc

    def test_insert_document_size(self):
        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=self.remote_cluster,
            external_collection_file_formats=["json"])

        if self.input.param("primary_key", None) is not None:
            self.columnar_spec["standalone_dataset"]["primary_key"] = json.loads(self.input.param("primary_key"))

        # create entities on columnar cluster based on spec file
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
        if not result:
            self.fail(msg)
        jobs = Queue()
        results = []
        datasets = self.cbas_util.get_all_dataset_objs()
        self.log.info("Creating doc, this may take several minutes")
        doc_with_size = [self.generate_large_document(self.doc_size)]
        for dataset in datasets:
            if not self.cbas_util.insert_into_standalone_collection(self.columnar_cluster, dataset.name, document=doc_with_size,
                                                             database_name=dataset.database_name,
                                                             dataverse_name=dataset.dataverse_name):
                self.fail("Failed to insert some doc of size {}".format(self.doc_size))

    def test_create_collection_as(self):
        self.create_bucket_scopes_collections_in_capella_cluster(
            self.tenant, self.remote_cluster,
            self.input.param("num_buckets", 1))
        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=self.remote_cluster,
            external_collection_file_formats=["json"])

        if self.input.param("primary_key", None) is not None:
            self.columnar_spec["standalone_dataset"]["primary_key"] = json.loads(self.input.param("primary_key"))

        # create entities on columnar cluster based on spec file
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
        if not result:
            self.fail(msg)

        remote_bucket = self.remote_cluster.buckets[0]
        self.cbas_util.doc_operations_remote_collection_sirius(self.task_manager, "_default", remote_bucket.name,
                                                               "_default", "couchbases://" + self.remote_cluster.srv,
                                                               0, self.initial_doc_count, doc_size=self.doc_size,
                                                               username=self.remote_cluster.username,
                                                               password=self.remote_cluster.password,
                                                               template="hotel")

        self.cbas_util.wait_for_data_ingestion_in_the_collections(self.columnar_cluster)
        remote_dataset = self.cbas_util.get_all_dataset_objs("remote")[0]
        subquery = ["select name as name, email as email, reviews from {}",
                    "select name as name, email as email, reviews from {} where avg_rating > 0.4"]
        datasets = self.cbas_util.create_standalone_dataset_obj(self.columnar_cluster,
                                                                no_of_objs=self.input.param("num_of_standalone_dataset",
                                                                                            1),
                                                                primary_key={"name": "string", "email": "string"})
        for dataset in datasets:
            subquery_execute = random.choice(subquery).format(remote_dataset.full_name)
            self.cbas_util.create_standalone_collection(self.columnar_cluster, dataset.name,
                                                        dataverse_name=dataset.dataverse_name,
                                                        database_name=dataset.database_name,
                                                        primary_key=dataset.primary_key,
                                                        subquery=subquery_execute)

            # verify the data
            status, metrics, errors, result, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, subquery_execute)
            doc_count_in_dataset = self.cbas_util.get_num_items_in_cbas_dataset(self.columnar_cluster, remote_dataset.full_name)
            if len(result) != doc_count_in_dataset:
                self.fail("Document count mismatch in {}".format(dataset.full_name))

        # verify the data after the link is disconnected
        links = self.cbas_util.get_all_link_objs("couchbase")[0]
        self.cbas_util.disconnect_link(self.columnar_cluster, link_name=links.full_name)
        for dataset in datasets:
            doc_count_in_dataset = self.cbas_util.get_num_items_in_cbas_dataset(self.columnar_cluster, dataset.full_name)
            if doc_count_in_dataset == 0:
                self.fail("No document found after link is disconnected")

    def test_insert_duplicate_doc(self):
        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=self.remote_cluster,
            external_collection_file_formats=["json"])

        if self.input.param("primary_key", None) is not None:
            self.columnar_spec["standalone_dataset"]["primary_key"] = json.loads(self.input.param("primary_key"))

        # create entities on columnar cluster based on spec file
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
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
                          "cluster": self.columnar_cluster, "collection_name": dataset.name,
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
                          "cluster": self.columnar_cluster, "collection_name": dataset.name,
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
        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=self.remote_cluster,
            external_collection_file_formats=["json"])

        if self.input.param("primary_key", None) is not None:
            self.columnar_spec["standalone_dataset"]["primary_key"] = json.loads(self.input.param("primary_key"))

        # create entities on columnar cluster based on spec file
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
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
                          "cluster": self.columnar_cluster, "collection_name": dataset.name,
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
        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=self.remote_cluster,
            external_collection_file_formats=["json"])

        if self.input.param("primary_key", None) is not None:
            self.columnar_spec["standalone_dataset"]["primary_key"] = json.loads(self.input.param("primary_key"))

        # create entities on columnar cluster based on spec file
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
        if not result:
            self.fail(msg)
        jobs = Queue()
        results = []
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        for dataset in datasets:
            jobs.put((
                self.cbas_util.crud_on_standalone_collection,
                {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                 "dataverse_name": dataset.dataverse_name,
                 "database_name": dataset.database_name,
                 "target_num_docs": self.initial_doc_count,
                 "time_for_crud_in_mins": 5, "doc_size": self.doc_size,
                 "where_clause_for_delete_op": "avg_rating > 0.2",
                 "use_alias": True}))

        jobs.put((self.columnar_utils.scale_instance,
                  {"pod": self.pod, "tenant": self.tenant, "project_id": self.columnar_cluster.project_id,
                   "instance": self.columnar_cluster,
                   "nodes": 4}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, len(datasets) + 1, async_run=False
        )

        self.columnar_utils.wait_for_instance_scaling_operation(
            self.pod, self.tenant, self.columnar_cluster.project_id, self.columnar_cluster)

        if not all(results):
            self.fail("Failed to insert doc in standalone collection")

    def test_insert_atomicity(self):
        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=self.remote_cluster,
            external_collection_file_formats=["json"])

        if self.input.param("primary_key", None) is not None:
            self.columnar_spec["standalone_dataset"]["primary_key"] = json.loads(self.input.param("primary_key"))

        # create entities on columnar cluster based on spec file
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
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
                      {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                       "document": data_to_add, "dataverse_name": dataset.dataverse_name,
                       "database_name": dataset.database_name, "client_context_id": "insert_request"}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=True
        )
        get_requests = True
        retry_count = 1
        while get_requests and retry_count < 100:
            retry_count = retry_count + 1
            active_requests = self.cbas_util.get_all_active_requests(self.columnar_cluster)
            for request in active_requests:
                if str(request['statement']).startswith("INSERT INTO"):
                    get_requests = False
                    context_id = str(request['clientContextID'])
                    time.sleep(0.1)
                    if not self.cbas_util.delete_request(self.columnar_cluster, context_id):
                        self.fail("Failed to delete insert request")

        for dataset in datasets:
            doc_count = self.cbas_util.get_num_items_in_cbas_dataset(self.columnar_cluster, dataset.full_name)
            if doc_count != 0:
                self.fail("Some documents were inserted after statement is aborted")

    def test_delete_atomicity(self):
        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=self.remote_cluster,
            external_collection_file_formats=["json"])

        if self.input.param("primary_key", None) is not None:
            self.columnar_spec["standalone_dataset"]["primary_key"] = json.loads(self.input.param("primary_key"))

        # create entities on columnar cluster based on spec file
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
        if not result:
            self.fail(msg)

        jobs = Queue()
        results = []

        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name,
                       "database_name": dataset.database_name, "no_of_docs": self.initial_doc_count,
                       "document_size": self.doc_size}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )

        for dataset in datasets:
            jobs.put((self.cbas_util.delete_from_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name,
                       "client_context_id": "delete_Request"}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=True
        )
        get_requests = True
        retry_count = 1
        while get_requests and retry_count < 100:
            retry_count = retry_count + 1
            active_requests = self.cbas_util.get_all_active_requests(self.columnar_cluster)
            for request in active_requests:
                if str(request['statement']).startswith("DELETE FROM"):
                    get_requests = False
                    context_id = str(request['clientContextID'])
                    time.sleep(0.1)
                    if not self.cbas_util.delete_request(self.columnar_cluster, context_id):
                        self.fail("Failed to delete request")

        for dataset in datasets:
            doc_count = self.cbas_util.get_num_items_in_cbas_dataset(self.columnar_cluster, dataset.full_name)
            if doc_count != self.initial_doc_count:
                self.fail("Some documents were deleted after statement is aborted")

    def test_upsert_atomicity(self):
        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=self.remote_cluster,
            external_collection_file_formats=["json"])

        if self.input.param("primary_key", None) is not None:
            self.columnar_spec["standalone_dataset"]["primary_key"] = json.loads(self.input.param("primary_key"))

        # create entities on columnar cluster based on spec file
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
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
                      {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                       "document": data_to_add, "dataverse_name": dataset.dataverse_name,
                       "database_name": dataset.database_name}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )
        data_to_modify = [data.copy() for data in data_to_add]
        for data in data_to_modify:
            data['phone'] = random.randint(1, 10000)

        for dataset in datasets:
            jobs.put((self.cbas_util.upsert_into_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                       "new_item": data_to_modify, "dataverse_name": dataset.dataverse_name,
                       "database_name": dataset.database_name, "client_context_id": "upsert_request"}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=True
        )
        get_requests = True
        retry_count = 1
        while get_requests and retry_count < 20:
            retry_count = retry_count + 1
            active_request = self.cbas_util.get_all_active_requests(self.columnar_cluster)
            for request in active_request:
                if str(request['statement']).startswith("UPSERT INTO"):
                    get_requests = False
                    context_id = str(request['clientContextID'])
                    time.sleep(0.1)
                    if not self.cbas_util.delete_request(self.columnar_cluster, context_id):
                        self.fail("Failed to delete upsert request")

        for dataset in datasets:
            doc_count = self.cbas_util.get_num_items_in_cbas_dataset(self.columnar_cluster, dataset.full_name)
            if doc_count != self.initial_doc_count:
                self.fail("Some documents are missing")
            else:
                statement = "Select * from {}".format(dataset.full_name)
                status, metrics, errors, results, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                    self.columnar_cluster, statement)
                actual_result = [x[dataset.name] for x in results]
                if status == "success":
                    for dict_item in data_to_add:
                        if not any([item['address'] == dict_item["address"] for item in actual_result]):
                            self.fail("Some documents were not updated")
                else:
                    self.fail("Failed to get documents from the collection")
