import json
import random
import string
import time

from queue import Queue
from capellaAPI.capella.common.CapellaAPI import CommonCapellaAPI
from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI as CapellaAPIv2
from Columnar.columnar_base import ColumnarBaseTest
from CbasLib.CBASOperations import CBASHelper
from Jython_tasks.sirius_task import CouchbaseUtil
from Columnar.mini_volume_code_template import MiniVolume


def pairs(unique_pairs, datasets):
    for i in range(len(datasets)):
        for j in range(i + 1, len(datasets)):
            unique_pairs.append([datasets[i], datasets[j]])


class CopyToKv(ColumnarBaseTest):
    def __init__(self, methodName: str = "runTest"):
        super().__init__(methodName)
        self.columnar_utils = None
        self.capella = None
        self.columnarAPI = None
        self.pod = None
        self.tenant = None

    def setUp(self):
        super(CopyToKv, self).setUp()
        self.columnar_cluster = self.tenant.columnar_instances[0]
        if len(self.tenant.clusters) > 0:
            self.remote_cluster = self.tenant.clusters[0]
            self.couchbase_doc_loader = CouchbaseUtil(
                task_manager=self.task_manager,
                hostname=self.remote_cluster.master.ip,
                username=self.remote_cluster.master.rest_username,
                password=self.remote_cluster.master.rest_password,
            )
        self.no_of_docs = self.input.param("no_of_docs", 1000)

        if not self.columnar_spec_name:
            self.columnar_spec_name = "full_template"

        # create columnar entities to operate on
        self.base_setup()

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        """
        Delete all the analytics entities, columnar instance and remote cluster
        """
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.columnar_cluster, self.columnar_spec):
            self.fail("Error while deleting cbas entities")

        if hasattr(self, "remote_cluster"):
            self.delete_all_buckets_from_capella_cluster(self.tenant, self.remote_cluster)
        super(ColumnarBaseTest, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished", stage="Teardown")

    def base_setup(self):

        # populate spec file for the entities to be created
        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=self.remote_cluster,
            external_collection_file_formats=["json"])

        if self.input.param("primary_key", None) is not None:
            self.columnar_spec["standalone_dataset"]["primary_key"] = json.loads(self.input.param("primary_key"))
        else:
            self.columnar_spec["standalone_dataset"]["primary_key"] = [{"name": "string", "email": "string"}]

        # create entities on columnar cluster based on spec file
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
        if not result:
            self.fail(str(msg))

    def create_capella_bucket(self):
        # creating bucket for this test
        bucket_name = self.cbas_util.generate_name()
        storage_type = self.input.param("storage_type", "couchstore")
        ram_quota = 1024 if storage_type == "magma" else 100
        response = self.capellaAPI.cluster_ops_apis.create_bucket(self.tenant.id, self.tenant.project_id,
                                                                  self.remote_cluster.id, bucket_name, "couchbase",
                                                                  storage_type, ram_quota, "seqno",
                                                                  "majorityAndPersistActive", 1, True, 1000000)

        if response.status_code == 201:
            self.log.info("Bucket created successfully")
            return response.json()["id"], bucket_name
        else:
            self.fail("Error creating bucket in remote_cluster")

    def create_capella_scope(self, bucket_id, scope_name=None):
        if not scope_name:
            scope_name = self.cbas_util.generate_name()

        resp = self.capellaAPI.cluster_ops_apis.create_scope(self.tenant.id, self.tenant.project_id,
                                                             self.remote_cluster.id,
                                                             bucket_id, scope_name)

        if resp.status_code == 201:
            self.log.info("Created scope {} on bucket {}".format(scope_name, bucket_id))
            return scope_name
        else:
            self.fail("Failed to create capella provisioned scope")

    def create_capella_collection(self, bucket_id, scope_id, collection_name=None):
        if not collection_name:
            collection_name = self.cbas_util.generate_name()
        for i in range(0, 5):
            resp = self.capellaAPI.cluster_ops_apis.create_collection(
                self.tenant.id, self.tenant.project_id,
                clusterId=self.remote_cluster.id, bucketId=bucket_id,
                scopeName=scope_id, name=collection_name)

            if resp.status_code == 201:
                self.log.info("Create collection {} in scope {}".format(
                    collection_name, scope_id))
                return collection_name
            else:
                self.log.error(
                    "Failed to create collection {} in scope {}. "
                    "Retrying".format(collection_name, scope_id))
                time.sleep(10)
        self.log.error(
            "Failed to create collection {} in scope {} even after 5 "
            "retries".format(collection_name, scope_id))
        return None

    def copy_to_kv_all_collections(self, datasets, jobs, results=None, source_definition=None,
                                   link_name=None, primary_key=None, function=None,
                                   timeout=300, analytics_timeout=300, validate_error_msg=None,
                                   expected_error=None, expected_error_code=None, max_warnings=0,
                                   validate_warning_msg=None, async_run=False):

        self.provisioned_bucket_id, self.provisioned_bucket_name = self.create_capella_bucket()
        self.provisioned_scope_name = self.create_capella_scope(self.provisioned_bucket_id)
        provisioned_collections = []
        for dataset in datasets:
            remote_collection_name = self.create_capella_collection(self.provisioned_bucket_id,
                                                                    self.provisioned_scope_name)
            if not remote_collection_name:
                self.fail("Creating collection in remote KV bucket failed.")

            provisioned_collections.append(remote_collection_name)
            collection = "{}.{}.{}".format(self.provisioned_bucket_name, self.provisioned_scope_name,
                                           remote_collection_name)
            if source_definition:
                source_definition_query = source_definition.format(dataset.full_name)
            else:
                source_definition_query = None
            jobs.put((self.cbas_util.copy_to_kv,
                      {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                       "database_name": dataset.database_name, "source_definition": source_definition_query,
                       "dest_bucket": collection, "primary_key": primary_key, "function": function,
                       "dataverse_name": dataset.dataverse_name, "link_name": link_name,
                       "analytics_timeout": analytics_timeout, "timeout": timeout,
                       "validate_error_msg": validate_error_msg, "expected_error": expected_error,
                       "expected_error_code": expected_error_code, "max_warnings": max_warnings,
                       "validate_warning_msg": validate_warning_msg}))

        self.cbas_util.run_jobs_in_parallel(jobs, results, self.sdk_clients_per_user, async_run=async_run)
        if not async_run:
            if not all(results):
                self.fail("Copy to KV statement failed")
        return provisioned_collections

    def test_copyToKV_key_type(self):
        # get all datasets to initiate copy_to_kv from
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        # get remote link for remote cluster
        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        jobs = Queue()
        results = []

        # get the field value to be used as primary key
        key = self.input.param("copy_key", None)
        if ',' in key:
            key = key.split(',')

        # load docs to source collections.
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name,
                       "no_of_docs": self.no_of_docs, "document_size": self.doc_size}))
        self.cbas_util.run_jobs_in_parallel(jobs, results, self.sdk_clients_per_user)

        # populate expected error/warning fields to validate response
        expected_error_code = self.input.param("expected_error_code", None)
        expected_error_msg = self.input.param("expected_error_msg", None)
        validate_warning_msg = self.input.param("validate_warning_msg", False)
        validate_error_msg = self.input.param("validate_error_msg", False)

        # create copy to kv jobs from source columnar collection to destination remote cluster
        _ = self.copy_to_kv_all_collections(datasets, jobs=jobs, results=results,
                                            link_name=remote_link.full_name, primary_key=key,
                                            validate_error_msg=validate_error_msg,
                                            validate_warning_msg=validate_warning_msg,
                                            expected_error=expected_error_msg,
                                            expected_error_code=expected_error_code, max_warnings=25)

    def test_create_copyToKv_from_standalone_collection(self):
        # get all datasets to initiate copy_to_kv from
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        # get remote link for remote cluster
        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        jobs = Queue()
        results = []

        # disconnect link before initiating copy to kv
        if self.input.param("disconnect_link", False):
            self.log.info("Disconnecting Links")
            if not self.cbas_util.disconnect_link(self.columnar_cluster, remote_link.full_name):
                self.fail("Failed to disconnect link")

        # load data to source standalone collections
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name,
                       "no_of_docs": self.no_of_docs, "document_size": self.doc_size}))

        self.cbas_util.run_jobs_in_parallel(jobs, results, self.sdk_clients_per_user, async_run=False)

        provisioned_collections = self.copy_to_kv_all_collections(datasets, jobs, results,
                                                                  link_name=remote_link.full_name)

        # validate the copied data
        if self.input.param("disconnect_link", False):
            self.log.info("Connecting remote link: {0}".format(remote_link.full_name))
            if not self.cbas_util.connect_link(self.columnar_cluster, remote_link.full_name):
                self.fail("Failed to connect link")

        for i in range(len(datasets)):
            self.log.info("Creating remote collection on KV scope used for copy to kv")
            remote_dataset = \
                self.cbas_util.create_remote_dataset_obj(self.columnar_cluster, self.provisioned_bucket_name,
                                                         self.provisioned_scope_name,
                                                         provisioned_collections[i], remote_link,
                                                         capella_as_source=True)[0]
            if not self.cbas_util.create_remote_dataset(self.columnar_cluster, remote_dataset.name,
                                                        remote_dataset.full_kv_entity_name,
                                                        remote_dataset.link_name,
                                                        dataverse_name=remote_dataset.dataverse_name,
                                                        database_name=remote_dataset.database_name):
                self.log.error("Failed to create remote dataset on KV")
                results.append(False)
            # validate doc count at columnar and KV side
            columnar_count = self.cbas_util.get_num_items_in_cbas_dataset(self.columnar_cluster, datasets[i].full_name)
            if not self.cbas_util.wait_for_ingestion_complete(self.columnar_cluster, remote_dataset.full_name,
                                                              columnar_count):
                results.append(False)
            kv_count = self.cbas_util.get_num_items_in_cbas_dataset(self.columnar_cluster, remote_dataset.full_name)
            if columnar_count != kv_count:
                self.log.error("Doc count mismatch in KV and columnar {0}, {1}, expected: {2} got: {3}".
                               format(provisioned_collections[i], datasets[i].full_name,
                                      columnar_count, kv_count))
                results.append(columnar_count == kv_count)
            else:
                self.log.info("Doc count match in KV and columnar {0}, {1}, expected: {2}, got: {3}".format(
                    provisioned_collections[i], datasets[i].full_name,
                    columnar_count, kv_count))
        if not all(results):
            self.fail("Mismatch found in Copy To KV")

    def test_create_copyToKv_key_size(self):
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        jobs = Queue()
        results = []
        characters = string.ascii_letters + string.digits + string.punctuation
        valid_key_count = 0
        primary_key = "kv_key"
        for dataset in datasets:
            docs = []
            for i in range(self.no_of_docs):
                doc = self.cbas_util.generate_docs(document_size=1024)
                key_size = random.choice([245, 246])
                if key_size == 245:
                    valid_key_count += 1
                doc["kv_key"] = ''.join(random.choice(characters) for _ in range(key_size))
                docs.append(doc)
            if not self.cbas_util.insert_into_standalone_collection(self.columnar_cluster, dataset.name, docs,
                                                                    dataset.dataverse_name, dataset.database_name):
                self.fail("Failed to insert document")

        provisioned_collections = self.copy_to_kv_all_collections(datasets, jobs, results,
                                                                  link_name=remote_link.full_name,
                                                                  primary_key=primary_key,
                                                                  validate_error_msg=self.input.param("validate_error",
                                                                                                      False),
                                                                  expected_error=self.input.param("expected_error",
                                                                                                  None),
                                                                  expected_error_code=self.input.param(
                                                                      "expected_error_code", None),
                                                                  )

        for i in range(len(datasets)):
            remote_dataset = \
                self.cbas_util.create_remote_dataset_obj(self.columnar_cluster, self.provisioned_bucket_name,
                                                         self.provisioned_scope_name,
                                                         provisioned_collections[i], remote_link,
                                                         capella_as_source=True)[0]
            if not self.cbas_util.create_remote_dataset(self.columnar_cluster, remote_dataset.name,
                                                        remote_dataset.full_kv_entity_name,
                                                        remote_dataset.link_name,
                                                        dataverse_name=remote_dataset.dataverse_name,
                                                        database_name=remote_dataset.database_name):
                self.log.error("Failed to create remote dataset on KV")
                results.append(False)
                # validate doc count at columnar and KV side
                if not self.cbas_util.wait_for_ingestion_complete(self.columnar_cluster, remote_dataset.full_name,
                                                                  valid_key_count):
                    results.append(False)
                kv_count = self.cbas_util.get_num_items_in_cbas_dataset(self.columnar_cluster, remote_dataset.full_name)
                if valid_key_count != kv_count:
                    self.log.error("Doc count mismatch in KV and columnar {0}, {1}, expected: {2} got: {3}".
                                   format(provisioned_collections[i], datasets[i].full_name,
                                          valid_key_count, kv_count))
                    results.append(valid_key_count == kv_count)
                else:
                    self.log.info("Doc count match in KV and columnar {0}, {1}, expected: {2} got: {3}".
                                  format(provisioned_collections[i], datasets[i].full_name,
                                         valid_key_count, kv_count))
        if not all(results):
            self.fail("Mismatch found in Copy To KV")

    def test_create_copyToKv_duplicate_data_key(self):
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        jobs = Queue()
        results = []
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name,
                       "no_of_docs": self.no_of_docs, "document_size": self.doc_size}))

        self.cbas_util.run_jobs_in_parallel(jobs, results, self.sdk_clients_per_user, async_run=False)

        if self.input.param("use_key", False):
            primary_key = "email"
        else:
            primary_key = None

        provisioned_collections = self.copy_to_kv_all_collections(datasets, jobs, results,
                                                                  link_name=remote_link.full_name,
                                                                  primary_key=primary_key)

        for i in range(len(datasets)):
            collection = "{}.{}.{}".format(self.provisioned_bucket_name, self.provisioned_scope_name,
                                           provisioned_collections[i])
            dataset = datasets[i]
            jobs.put((self.cbas_util.copy_to_kv,
                      {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                       "database_name": dataset.database_name,
                       "dataverse_name": dataset.dataverse_name, "dest_bucket": collection,
                       "link_name": remote_link.full_name, "primary_key": primary_key}))

        self.cbas_util.run_jobs_in_parallel(jobs, results, self.sdk_clients_per_user)

        # validate the copied data
        for i in range(len(datasets)):
            remote_dataset = \
                self.cbas_util.create_remote_dataset_obj(self.columnar_cluster, self.provisioned_bucket_name,
                                                         self.provisioned_scope_name,
                                                         provisioned_collections[i], remote_link,
                                                         capella_as_source=True)[0]
            if not self.cbas_util.create_remote_dataset(self.columnar_cluster, remote_dataset.name,
                                                        remote_dataset.full_kv_entity_name,
                                                        remote_dataset.link_name,
                                                        dataverse_name=remote_dataset.dataverse_name,
                                                        database_name=remote_dataset.database_name):
                self.log.error("Failed to create remote dataset on KV")
                results.append(False)
            # validate doc count at columnar and KV side
            columnar_count = self.cbas_util.get_num_items_in_cbas_dataset(self.columnar_cluster, datasets[i].full_name)
            if not primary_key:
                columnar_count = 2 * columnar_count
            if not self.cbas_util.wait_for_ingestion_complete(self.columnar_cluster, remote_dataset.full_name,
                                                              columnar_count):
                results.append(False)
            kv_count = self.cbas_util.get_num_items_in_cbas_dataset(self.columnar_cluster, remote_dataset.full_name)
            if columnar_count != kv_count:
                self.log.error("Doc count mismatch in KV and columnar {0}, {1}, expected: {2} got: {3}".
                               format(provisioned_collections[i], datasets[i].full_name,
                                      columnar_count, kv_count))
                results.append(columnar_count == kv_count)
            else:
                self.log.info("Doc count match in KV and columnar {0}, {1}, expected: {2} got: {3}".
                              format(provisioned_collections[i], datasets[i].full_name,
                                     columnar_count, kv_count))
        if not all(results):
            self.fail("Mismatch found in Copy To KV")

    def test_create_copyToKv_from_external_collection(self):
        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        datasets = self.cbas_util.get_all_dataset_objs("external")
        jobs = Queue()
        results = []
        provisioned_collections = self.copy_to_kv_all_collections(datasets, jobs, results,
                                                                  source_definition="select * from {} limit 100000",
                                                                  link_name=remote_link.full_name, timeout=100000,
                                                                  analytics_timeout=100000)

        # validate the copied data
        for i in range(len(datasets)):
            remote_dataset = \
                self.cbas_util.create_remote_dataset_obj(self.columnar_cluster, self.provisioned_bucket_name,
                                                         self.provisioned_scope_name,
                                                         provisioned_collections[i], remote_link,
                                                         capella_as_source=True)[0]
            if not self.cbas_util.create_remote_dataset(self.columnar_cluster, remote_dataset.name,
                                                        remote_dataset.full_kv_entity_name,
                                                        remote_dataset.link_name,
                                                        dataverse_name=remote_dataset.dataverse_name,
                                                        database_name=remote_dataset.database_name):
                self.log.error("Failed to create remote dataset on KV")
                results.append(False)
            # validate doc count at columnar and KV side
            if not self.cbas_util.wait_for_ingestion_complete(self.columnar_cluster, remote_dataset.full_name, 100000):
                results.append(False)
            kv_count = self.cbas_util.get_num_items_in_cbas_dataset(self.columnar_cluster, remote_dataset.full_name)
            if 100000 != kv_count:
                self.log.error("Doc count mismatch in KV and columnar {0}, {1}, expected: {2} got: {3}".
                               format(provisioned_collections[i], datasets[i].full_name,
                                      100000, kv_count))
                results.append(100000 == kv_count)
            else:
                self.log.info("Doc count match in KV and columnar {0}, {1}, expected: {2} got: {3}".
                              format(provisioned_collections[i], datasets[i].full_name,
                                     100000, kv_count))
        if not all(results):
            self.fail("Mismatch found in Copy To KV")

    def test_negative_cases_invalid_name(self):
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        jobs = Queue()
        results = []
        self.provisioned_bucket_id, self.provisioned_bucket_name = self.create_capella_bucket()
        self.provisioned_scope_name = self.create_capella_scope(self.provisioned_bucket_id)
        expected_error = self.input.param("expected_error", None)
        for dataset in datasets:
            expected_error_code = None
            if self.input.param("invalid_dataset", False):
                dataset.name = self.cbas_util.generate_name()
                expected_error_code = expected_error.format(dataset.name)
            if self.input.param("invalid_kv_entity", False):
                collection_name = self.cbas_util.generate_name()
                collection = "{}.{}.{}".format(self.provisioned_bucket_name, self.provisioned_scope_name,
                                               collection_name)
                expected_error_code = expected_error.format(collection)
            else:
                collection_name = self.create_capella_collection(
                    self.provisioned_bucket_id, self.provisioned_scope_name)
                if not collection_name:
                    self.fail(
                        "Creating collection in remote KV bucket failed.")
                collection = "{}.{}.{}".format(self.provisioned_bucket_name, self.provisioned_scope_name,
                                               collection_name)
            if self.input.param("invalid_link", False):
                link_name = self.cbas_util.generate_name()
                expected_error_code = expected_error.format(link_name)
            else:
                link_name = remote_link.full_name

            jobs.put((self.cbas_util.copy_to_kv,
                      {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                       "database_name": dataset.database_name,
                       "dataverse_name": dataset.dataverse_name, "dest_bucket": collection,
                       "link_name": link_name, "validate_error_msg": self.input.param("validate_error", False),
                       "expected_error": expected_error_code,
                       "expected_error_code": self.input.param("expected_error_code", None)}))

        time.sleep(60)
        self.log.info("Running copy to kv statements")
        self.cbas_util.run_jobs_in_parallel(jobs, results, self.sdk_clients_per_user)
        if not all(results):
            self.fail("Negative test for invalid names failed")

    def test_drop_link_during_copy_to_kv(self):
        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        datasets = self.cbas_util.get_all_dataset_objs("external")
        jobs = Queue()
        results = []
        source_definition = "select * from {} limit 100000"
        provisioned_collections = self.copy_to_kv_all_collections(datasets, jobs, results,
                                                                  source_definition=source_definition,
                                                                  link_name=remote_link.full_name,
                                                                  analytics_timeout=1000000, timeout=100000,
                                                                  async_run=True)
        self.wait_for_query_in_columnar()
        time.sleep(20)
        if not self.cbas_util.drop_link(self.columnar_cluster, remote_link.full_name, timeout=720,
                                        analytics_timeout=720):
            self.fail("Failed to drop link while copying to KV")
        del (self.cbas_util.remote_links[remote_link.full_name])
        jobs.join()
        if not all(results):
            self.fail("Failed to execute copy to kv statement")

        # validate data in KV re-create remote link
        if not self.cbas_util.create_remote_link_from_spec(self.columnar_cluster, self.columnar_spec):
            self.fail("Failed to create remote link")

        all_remote_links = self.cbas_util.get_all_link_objs("couchbase")
        new_remote_link = None
        for i in all_remote_links:
            if i.full_name != remote_link.full_name:
                new_remote_link = i
                break
        self.cbas_util.connect_link(self.columnar_cluster, new_remote_link.full_name)
        for i in range(len(datasets)):
            remote_dataset = \
                self.cbas_util.create_remote_dataset_obj(self.columnar_cluster, self.provisioned_bucket_name,
                                                         self.provisioned_scope_name,
                                                         provisioned_collections[i], new_remote_link,
                                                         capella_as_source=True)[0]
            if not self.cbas_util.create_remote_dataset(self.columnar_cluster, remote_dataset.name,
                                                        remote_dataset.full_kv_entity_name,
                                                        remote_dataset.link_name,
                                                        dataverse_name=remote_dataset.dataverse_name,
                                                        database_name=remote_dataset.database_name):
                self.log.error("Failed to create remote dataset on KV")
                results.append(False)
            # validate doc count at columnar and KV side
            self.cbas_util.wait_for_data_ingestion_in_the_collections(self.columnar_cluster)
            columnar_count = 100000
            kv_count = self.cbas_util.get_num_items_in_cbas_dataset(self.columnar_cluster, remote_dataset.full_name)
            if columnar_count != kv_count:
                self.fail("Mismatch found in Copy To KV")
            else:
                self.log.info("Match found in Copy To KV")

    def test_remove_user_access_while_copy_to_kv(self):
        capella_api_v2 = CapellaAPIv2(self.pod.url_public, self.tenant.api_secret_key, self.tenant.api_access_key,
                                      self.tenant.user, self.tenant.pwd)
        new_db_user_id = capella_api_v2.create_db_user(self.tenant.id, self.tenant.project_id, self.remote_cluster.id,
                                                       "CopyToKV", "Couchbase@123").json()['id']
        self.remote_cluster.username = "CopyToKV"
        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        dataset = self.cbas_util.get_all_dataset_objs("external")[0]
        link_properties = remote_link.properties
        link_properties['username'] = "CopyToKV"
        link_properties['password'] = "Couchbase@123"
        self.cbas_util.alter_link_properties(self.columnar_cluster, link_properties)
        jobs = Queue()
        results = []
        source_definition = "select * from {} limit 100000"
        _ = self.copy_to_kv_all_collections([dataset], jobs, results,
                                            source_definition=source_definition,
                                            link_name=remote_link.full_name,
                                            analytics_timeout=1000000, timeout=100000,
                                            validate_error_msg=self.input.param("validate_error", False),
                                            expected_error=self.input.param("expected_error", ""),
                                            expected_error_code=self.input.param("expected_error_code", ""),
                                            async_run=True)
        self.wait_for_query_in_columnar()
        time.sleep(20)
        capella_api_v2.delete_db_user(self.tenant.id, self.tenant.project_id, self.remote_cluster.id, new_db_user_id)
        jobs.join()
        if not all(results):
            self.fail("Failed to execute copy to kv with failure")

    def delete_capella_bucket(self, bucket_id):
        for i in range(5):
            resp = self.capellaAPI.cluster_ops_apis.delete_bucket(self.tenant.id, self.tenant.project_id,
                                                                  self.remote_cluster.id, bucket_id)
            if resp.status_code == 204:
                self.log.info("Bucket deleted successfully")
                return True
            else:
                self.log.error("Failed to delete capella bucket")
                if i == 4:
                    return False

    def test_copy_to_kv_drop_remote_dataset(self):
        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        datasets = self.cbas_util.get_all_dataset_objs("external")
        jobs = Queue()
        results = []
        source_definition = "select * from {} limit 100000"
        _ = self.copy_to_kv_all_collections(datasets, jobs, results,
                                            source_definition=source_definition,
                                            link_name=remote_link.full_name,
                                            analytics_timeout=1000000, timeout=100000,
                                            validate_error_msg=self.input.param("validate_error",
                                                                                False),
                                            expected_error=self.input.param("expected_error", ""),
                                            expected_error_code=self.input.param(
                                                "expected_error_code", ""),
                                            async_run=True)
        self.wait_for_query_in_columnar()
        time.sleep(20)
        if not self.delete_capella_bucket(bucket_id=self.provisioned_bucket_id):
            self.fail("Failed to drop remote bucket while copying to KV")
        else:
            self.provisioned_bucket_id = None
        jobs.join()
        if not all(results):
            self.fail("Copy to kv statement failed")

    def test_copy_to_kv_drop_columnar_dataset(self):
        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        datasets = self.cbas_util.get_all_dataset_objs("external")
        jobs = Queue()
        results = []
        source_definition = "select * from {} limit 100000"
        _ = self.copy_to_kv_all_collections(datasets, jobs, results,
                                            source_definition=source_definition,
                                            link_name=remote_link.full_name,
                                            analytics_timeout=1000000, timeout=100000,
                                            validate_error_msg=self.input.param("validate_error",
                                                                                False),
                                            expected_error=self.input.param("expected_error", ""),
                                            expected_error_code=self.input.param(
                                                "expected_error_code", ""),
                                            async_run=True)
        self.wait_for_query_in_columnar()
        time.sleep(20)
        for dataset in datasets:
            if not self.cbas_util.drop_dataset(self.columnar_cluster, dataset.full_name, analytics_timeout=720,
                                               timeout=720):
                self.fail("Failed to drop columnar dataset while copying to KV")
        jobs.join()
        if not all(results):
            self.fail("Copy to kv statement failed")

    def wait_for_query_in_columnar(self):
        for i in range(10):
            requests = self.cbas_util.get_all_active_requests(self.columnar_cluster)
            found = False
            for req in requests:
                if req['state'] == "running" and "COPY" in req['statement']:
                    found = True
                    break
            if found:
                break
            time.sleep(5)

    def test_create_copyToKV_from_collection_aggregate_group_by(self):
        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        jobs = Queue()
        results = []
        for i in range(len(datasets)):
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": datasets[i].name,
                       "dataverse_name": datasets[i].dataverse_name, "database_name": datasets[i].database_name,
                       "no_of_docs": self.no_of_docs}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.log.error("Not all docs were inserted")
        results = []
        source_definition = "SELECT country, ARRAY_AGG(city) AS city FROM {0} GROUP BY country"
        provisioned_collections = self.copy_to_kv_all_collections(datasets, jobs, results,
                                                                  source_definition=source_definition,
                                                                  link_name=remote_link.full_name)
        for i in range(len(datasets)):
            remote_dataset = self.cbas_util.create_remote_dataset_obj(self.columnar_cluster,
                                                                      self.provisioned_bucket_name,
                                                                      self.provisioned_scope_name,
                                                                      provisioned_collections[i], remote_link,
                                                                      capella_as_source=True)[0]
            if not self.cbas_util.create_remote_dataset(self.columnar_cluster, remote_dataset.name,
                                                        remote_dataset.full_kv_entity_name,
                                                        remote_dataset.link_name,
                                                        dataverse_name=remote_dataset.dataverse_name,
                                                        database_name=remote_dataset.database_name):
                self.log.error("Failed to create remote dataset on KV")
                results.append(False)
                continue
            self.log.info("Sleeping 30 seconds waiting for data to be ingested")
            time.sleep(30)
            for j in range(5):
                statement = "select * from {0} limit 1".format(datasets[i].full_name)
                status, metrics, errors, result, _, _ \
                    = self.cbas_util.execute_statement_on_cbas_util(self.columnar_cluster,
                                                                    statement)
                country_name = ((result[0])[CBASHelper.unformat_name(datasets[i].name)]["country"]).replace("&amp;",
                                                                                                            "")
                query_statement = ("SELECT ARRAY_LENGTH(ARRAY_AGG(city)) as city "
                                   "FROM {0} where country = \"{1}\"").format(
                    datasets[i].full_name, str(country_name))
                remote_statement = "select * from {0} where country = \"{1}\"".format(
                    remote_dataset.full_name, str(country_name))

                status, metrics, errors, result, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                    self.columnar_cluster,
                    remote_statement)
                length_of_city_array_from_remote = len((result[0][remote_dataset.name])['city'])

                status, metrics, errors, result2, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                    self.columnar_cluster,
                    query_statement)
                length_of_city_array_from_dataset = result2[0]["city"]
                if length_of_city_array_from_dataset != length_of_city_array_from_remote:
                    self.log.error("Length of city aggregate failed")
                else:
                    self.log.info("Length of city aggregate passed")
                results.append(length_of_city_array_from_remote == length_of_city_array_from_dataset)

        if not all(results):
            self.fail("Copy to statement copied the wrong results")

    def generate_large_document(self, doc_size_mb):
        size_bytes = doc_size_mb * 1024 * 1024
        large_doc = {}
        characters = string.ascii_letters + string.digits
        character_with_space = string.ascii_letters + string.digits + ' '

        # Generate an initial large random string
        random_string = ''.join(random.choices(character_with_space, k=100000))

        # Fill the document with random data
        for _ in range(size_bytes // 100000):  # To approximate the size
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

    def test_create_copy_to_kv_doc_size_32_MB(self):
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        jobs = Queue()
        results = []
        document = self.generate_large_document(self.doc_size)
        for dataset in datasets:
            self.cbas_util.insert_into_standalone_collection(self.columnar_cluster, dataset.name, document=[document],
                                                             database_name=dataset.database_name,
                                                             dataverse_name=dataset.dataverse_name)
        for dataset in datasets:
            if self.cbas_util.get_num_items_in_cbas_dataset(self.columnar_cluster, dataset.full_name) == 0:
                self.fail("Failed to load data in standalone collection of size: {}".format(self.doc_size))

        _ = self.copy_to_kv_all_collections(datasets, jobs, results,
                                            link_name=remote_link.full_name,
                                            validate_warning_msg=True,
                                            expected_error="External sink error. The document value "
                                                           "is too large to be stored",
                                            expected_error_code=24230, max_warnings=25)

    def test_create_copyToKV_from_multiple_collection_query_drop_standalone_collection(self):
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        jobs = Queue()
        results = []
        self.provisioned_bucket_id, self.provisioned_bucket_name = self.create_capella_bucket()
        self.provisioned_scope_name = self.create_capella_scope(self.provisioned_bucket_id)
        provisioned_collections = []
        unique_pairs = []
        pairs(unique_pairs, datasets)
        for dataset in datasets:
            if dataset.data_source is None:
                jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                          {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                           "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name,
                           "no_of_docs": self.no_of_docs}))

        self.log.info("Running copy to kv statements")
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        results = []
        for i in range(len(unique_pairs)):
            statement = "select * from {0} as a, {1} as b where a.avg_rating > 0.4 and b.avg_rating > 0.4".format(
                (unique_pairs[i][0]).full_name, (unique_pairs[i][1]).full_name
            )
            collection_name = self.create_capella_collection(
                self.provisioned_bucket_id, self.provisioned_scope_name)
            if not collection_name:
                self.fail("Creating collection in remote KV bucket failed.")
            provisioned_collections.append(collection_name)
            collection = "{}.{}.{}".format(self.provisioned_bucket_name, self.provisioned_scope_name, collection_name)

            jobs.put((self.cbas_util.copy_to_kv,
                      {"cluster": self.columnar_cluster, "source_definition": statement, "dest_bucket": collection,
                       "link_name": remote_link.full_name, "function": "concat(c.a.name,c.b.name)",
                       "timeout": 100000, "analytics_timeout": 100000}))

        time.sleep(60)
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.log.error("Copy to KV statement failure")

        for i in range(len(unique_pairs)):
            remote_dataset = \
                self.cbas_util.create_remote_dataset_obj(self.columnar_cluster, self.provisioned_bucket_name,
                                                         self.provisioned_scope_name,
                                                         provisioned_collections[i], remote_link,
                                                         capella_as_source=True)[0]
            if not self.cbas_util.create_remote_dataset(self.columnar_cluster, remote_dataset.name,
                                                        remote_dataset.full_kv_entity_name,
                                                        remote_dataset.link_name,
                                                        dataverse_name=remote_dataset.dataverse_name,
                                                        database_name=remote_dataset.database_name):
                self.log.fail("Failed to create remote dataset on KV")

            dataset_statement = ("select count(*) from {0} as a, {1} as b"
                                 " where a.avg_rating > 0.4 and b.avg_rating > 0.4").format(
                (unique_pairs[i][0]).full_name, (unique_pairs[i][1]).full_name
            )
            status, metrics, errors, result, _, _ = self.cbas_util.execute_statement_on_cbas_util(self.columnar_cluster,
                                                                                                  dataset_statement)
            if not self.cbas_util.wait_for_ingestion_complete(self.columnar_cluster, remote_dataset.full_name,
                                                              result[0]['$1']):
                results.append("False")
            remote_statement = "select count(*) from {}".format(remote_dataset.full_name)
            status, metrics, errors, result1, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster,
                remote_statement)

            if result[0]['$1'] != result1[0]['$1']:
                self.log.error("Document count mismatch in remote dataset {0}".format(
                    remote_dataset.full_name
                ))
            else:
                self.log.info("Document count match in remote dataset {0}".format(
                    remote_dataset.full_name
                ))
            results.append(result[0]['$1'] == result1[0]['$1'])

            if not all(results):
                self.fail("The document count does not match in remote source and KV")

    def test_copy_to_kv_scaling(self):
        self.commonAPI = CommonCapellaAPI(self.pod.url_public, '', '', self.tenant.user, self.tenant.pwd,
                                          self.capella["override_token"])
        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        datasets = self.cbas_util.get_all_dataset_objs("external")
        jobs = Queue()
        results = []
        source_definition = "select * from {} limit 100000"
        _ = self.copy_to_kv_all_collections(datasets, jobs, results, source_definition,
                                            link_name=remote_link.full_name,
                                            analytics_timeout=100000, timeout=100000,
                                            async_run=True)
        self.log.info("Running copy to kv statements")
        self.columnar_utils.scale_instance(self.pod, self.tenant, self.tenant.project_id, self.columnar_cluster, 4)
        self.columnar_utils.wait_for_instance_scaling_operation(self.pod, self.tenant, self.tenant.project_id,
                                                                self.columnar_cluster, verify_with_backend_cluster=True,
                                                                expected_num_of_nodes=4)
        jobs.join()
        if not all(results):
            self.fail("Copy to kv statement failed")

    def validate_copy_to_kv_result(self, datasets, provisioned_collections, remote_link, validation_results):
        for i in range(len(datasets)):
            remote_dataset = \
                self.cbas_util.create_remote_dataset_obj(self.columnar_cluster, self.provisioned_bucket_name,
                                                         self.provisioned_scope_name,
                                                         (provisioned_collections[i].split('.'))[2],
                                                         remote_link,
                                                         capella_as_source=True)[0]
            if not self.cbas_util.create_remote_dataset(self.columnar_cluster, remote_dataset.name,
                                                        remote_dataset.full_kv_entity_name,
                                                        remote_dataset.link_name,
                                                        dataverse_name=remote_dataset.dataverse_name,
                                                        database_name=remote_dataset.database_name):
                self.log.error("Failed to create remote dataset on KV")
                validation_results.append(False)
            # validate doc count at columnar and KV side
            columnar_count = self.cbas_util.get_num_items_in_cbas_dataset(self.columnar_cluster, datasets[i].full_name)
            kv_count = self.cbas_util.get_num_items_in_cbas_dataset(self.columnar_cluster, remote_dataset.full_name)
            if columnar_count != kv_count:
                self.log.error("Doc count mismatch in KV and columnar {0}, {1}, expected: {2} got: {3}".
                               format(provisioned_collections[i], datasets[i].full_name,
                                      columnar_count, kv_count))
                validation_results.append(columnar_count != kv_count)
            self.cbas_util.drop_dataset(self.columnar_cluster, remote_dataset.full_name)
        if not all(validation_results):
            self.fail("Mismatch found in Copy To KV")

    def create_copy_to_kv_all_datasets(self, datasets, copy_to_kv_job, copy_to_kv_collections, remote_links):
        for dataset in datasets:
            if type(remote_links, list):
                remote_link = random.choice(remote_links)
            else:
                remote_link = remote_links
            collection_name = self.create_capella_collection(
                self.provisioned_bucket_id, self.provisioned_scope_name)
            if not collection_name:
                self.fail("Creating collection in remote KV bucket failed.")
            collection = "{}.{}.{}".format(CBASHelper.format_name(self.provisioned_bucket_name),
                                           CBASHelper.format_name(self.provisioned_scope_name),
                                           CBASHelper.format_name(collection_name))
            copy_to_kv_collections.append(collection)
            copy_to_kv_job.put((self.cbas_util.copy_to_kv,
                                {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                                 "database_name": dataset.database_name,
                                 "dataverse_name": dataset.dataverse_name, "dest_bucket": collection,
                                 "link_name": remote_link.full_name, "analytics_timeout": 1000000}))

    def mini_volume_copy_to_kv(self):
        self.remote_start = 0
        self.remote_end = 0

        self.copy_to_kv_job = Queue()
        self.mini_volume = MiniVolume(self, "http://127.0.0.1:4000")
        self.mini_volume.calculate_volume_per_source()
        # initiate copy to kv
        for i in range(1, 5):
            if i % 2 == 0:
                self.mini_volume.run_processes(i, 2 ** (i - 1), False)
            else:
                self.mini_volume.run_processes(i, 2 ** (i + 1), False)
            self.mini_volume.start_crud_on_data_sources(self.remote_start, self.remote_end)
            self.mini_volume.stop_process()
            self.mini_volume.stop_crud_on_data_sources()
            self.cbas_util.wait_for_ingestion_complete()
            datasets = self.cbas_util.get_all_dataset_objs()
            remote_link = self.cbas_util.get_all_link_objs("couchbase")
            self.provisioned_bucket_id, self.provisioned_bucket_name = self.create_capella_bucket()
            self.provisioned_scope_name = self.create_capella_scope(self.provisioned_bucket_id)
            copy_to_kv_collections = []
            copy_to_kv_results = []
            self.create_copy_to_kv_all_datasets(datasets, self.copy_to_kv_job, copy_to_kv_collections, remote_link)
            time.sleep(180)
            self.cbas_util.run_jobs_in_parallel(self.copy_to_kv_job, copy_to_kv_results, self.sdk_clients_per_user,
                                                async_run=False)
            if not all(copy_to_kv_results):
                self.fail("Copy to KV statement failed")

            validation_results = []
            self.validate_copy_to_kv_result(datasets, copy_to_kv_collections, remote_link, validation_results)
            self.mini_volume.stop_process(query_pass=True)
            self.delete_capella_bucket(self.provisioned_bucket_id)

        self.log.info("Copy to KV mini volume finish")
