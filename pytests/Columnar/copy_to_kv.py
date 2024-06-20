"""
Created on 4-March-2024

@author: abhay.aggrawal@couchbase.com
"""
import random
import string
import time
from queue import Queue

from CbasLib.CBASOperations import CBASHelper
from Columnar.columnar_base import ColumnarBaseTest
from capellaAPI.capella.columnar.CapellaAPI import CapellaAPI as ColumnarAPI
from capellaAPI.capella.common.CapellaAPI import CommonCapellaAPI
from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI as CapellaAPIv2
from mini_volume_code_template import MiniVolume


class CopyToKv(ColumnarBaseTest):
    def setUp(self):
        super(CopyToKv, self).setUp()
        self.cluster = self.tenant.columnar_instances[0]
        self.doc_loader_url = self.input.param("sirius_url", None)
        self.doc_loader_port = self.input.param("sirius_port", None)
        self.no_of_docs = self.input.param("no_of_docs", 1000)
        self.columnarAPI = ColumnarAPI(self.pod.url_public, '', '', self.tenant.user, self.tenant.pwd, '')

        # if none provided use 1 Kb doc size
        self.doc_size = self.input.param("doc_size", 1000)

        if not self.columnar_spec_name:
            self.columnar_spec_name = "regressions.copy_to_s3"

        self.columnar_spec = self.cbas_util.get_columnar_spec(
            self.columnar_spec_name)

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        """
        Delete all the analytics link and columnar instance
        """
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.cluster, self.columnar_spec):
            self.fail("Error while deleting cbas entities")

        if hasattr(self, 'provisioned_bucket_id'):
            self.delete_capella_bucket(self.provisioned_bucket_id)
        if hasattr(self, "remote_cluster") and hasattr(self.remote_cluster, "buckets"):
            self.delete_all_buckets_from_capella_cluster(self.tenant, self.cluster)
        super(ColumnarBaseTest, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished", stage="Teardown")

    def pairs(self, unique_pairs, datasets):
        for i in range(len(datasets)):
            for j in range(i + 1, len(datasets)):
                unique_pairs.append([datasets[i], datasets[j]])

    def base_infra_setup(self):
        self.columnar_spec["dataverse"]["no_of_dataverses"] = self.input.param(
            "no_of_scopes", 1)
        self.columnar_spec["remote_link"]["no_of_remote_links"] = self.input.param(
            "no_of_remote_links", 0)
        for key in self.cb_clusters:
            self.remote_cluster = self.cb_clusters[key]
            break
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
            remote_cluster_certificate = (remote_cluster_certificate_request.json()["certificate"])
        else:
            self.fail("Failed to get cluster certificate")

        # creating bucket scope and collections for remote collection
        no_of_remote_buckets = self.input.param("no_of_remote_bucket", 1)
        self.create_bucket_scopes_collections_in_capella_cluster(self.tenant, self.remote_cluster, no_of_remote_buckets)
        if self.input.param("no_of_remote_links", 1):
            remote_link_properties = list()
            remote_link_properties.append(
                {"type": "couchbase", "hostname": str(self.remote_cluster.srv),
                 "username": self.remote_cluster.username,
                 "password": self.remote_cluster.password,
                 "encryption": "full",
                 "certificate": remote_cluster_certificate})
            self.columnar_spec["remote_link"]["properties"] = remote_link_properties
            self.columnar_spec["remote_dataset"]["num_of_remote_datasets"] = self.input.param("num_of_remote_coll", 0)

        self.columnar_spec["external_link"][
            "no_of_external_links"] = self.input.param(
            "no_of_external_links", 0)
        self.columnar_spec["external_link"]["properties"] = [{
            "type": "s3",
            "region": self.aws_region,
            "accessKeyId": self.aws_access_key,
            "secretAccessKey": self.aws_secret_key,
            "serviceEndpoint": None
        }]

        self.columnar_spec["standalone_dataset"][
            "num_of_standalone_coll"] = self.input.param(
            "num_of_standalone_coll", 0)
        self.columnar_spec["standalone_dataset"]["primary_key"] = [{"name": "string", "email": "string"}]

        self.columnar_spec["external_dataset"]["num_of_external_datasets"] = self.input.param("num_of_external_coll", 0)
        if self.input.param("num_of_external_coll", 0):
            external_dataset_properties = [{
                "external_container_name": self.s3_source_bucket,
                "path_on_external_container": None,
                "file_format": self.input.param("file_format", "json"),
                "include": ["*.{0}".format(self.input.param("file_format", "json"))],
                "exclude": None,
                "region": self.aws_region,
                "object_construction_def": None,
                "redact_warning": None,
                "header": None,
                "null_string": None,
                "parse_json_string": 0,
                "convert_decimal_to_double": 0,
                "timezone": ""
            }]
            self.columnar_spec["external_dataset"][
                "external_dataset_properties"] = external_dataset_properties
        if not hasattr(self, "remote_cluster"):
            remote_cluster = None
        else:
            remote_cluster = [self.remote_cluster]
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.columnar_spec, self.bucket_util, False, remote_clusters=remote_cluster)
        if not result:
            self.fail(msg)

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
            self.fail("Failed to create capella provisined scope")

    def create_capella_collection(self, bucket_id, scope_id, collection_name=None):
        if not collection_name:
            collection_name = self.cbas_util.generate_name()
        resp = self.capellaAPI.cluster_ops_apis.create_collection(self.tenant.id, self.tenant.project_id,
                                                                  clusterId=self.remote_cluster.id, bucketId=bucket_id,
                                                                  scopeName=scope_id, name=collection_name)

        if resp.status_code == 201:
            self.log.info("Create collection {} in scope {}".format(collection_name, scope_id))
            return collection_name
        return False

    def delete_capella_bucket(self, bucket_id):
        resp = self.capellaAPI.cluster_ops_apis.delete_bucket(self.tenant.id, self.tenant.project_id,
                                                              self.remote_cluster.id, bucket_id)
        if resp.status_code == 204:
            self.log.info("Bucket deleted successfully")
        else:
            self.log.error("Failed to delete capella bucket")

    def test_create_copyToKv_from_standalone_collection(self):
        self.base_infra_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        jobs = Queue()
        results = []
        if self.input.param("disconnect_link", False):
            if not self.cbas_util.disconnect_link(self.cluster, remote_link.full_name):
                self.fail("Failed to disconnect link")

        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name,
                       "no_of_docs": self.no_of_docs, "document_size": self.doc_size}))

        self.cbas_util.run_jobs_in_parallel(jobs, results, self.sdk_clients_per_user, async_run=False)

        self.provisioned_bucket_id, self.provisioned_bucket_name = self.create_capella_bucket()
        self.provisioned_scope_name = self.create_capella_scope(self.provisioned_bucket_id)
        provisioned_collections = []
        for dataset in datasets:
            collection_name = self.create_capella_collection(self.provisioned_bucket_id,
                                                             self.provisioned_scope_name)
            provisioned_collections.append(collection_name)
            collection = "{}.{}.{}".format(self.provisioned_bucket_name, self.provisioned_scope_name,
                                           collection_name)
            jobs.put((self.cbas_util.copy_to_kv,
                      {"cluster": self.cluster, "collection_name": dataset.name, "database_name": dataset.database_name,
                       "dataverse_name": dataset.dataverse_name, "dest_bucket": collection,
                       "link_name": remote_link.full_name}))

        self.cbas_util.run_jobs_in_parallel(jobs, results, self.sdk_clients_per_user)
        if not all(results):
            self.fail("Copy to KV statement failed")

        # validate the copied data
        if self.input.param("disconnect_link", False):
            if not self.cbas_util.connect_link(self.cluster, remote_link.full_name):
                self.fail("Failed to connect link")

        for i in range(len(datasets)):
            remote_dataset = self.cbas_util.create_remote_dataset_obj(self.cluster, self.provisioned_bucket_name,
                                                                      self.provisioned_scope_name,
                                                                      provisioned_collections[i], remote_link,
                                                                      capella_as_source=True)[0]
            if not self.cbas_util.create_remote_dataset(self.cluster, remote_dataset.name,
                                                        remote_dataset.full_kv_entity_name,
                                                        remote_dataset.link_name,
                                                        dataverse_name=remote_dataset.dataverse_name,
                                                        database_name=remote_dataset.database_name):
                self.log.error("Failed to create remote dataset on KV")
                results.append(False)
            # validate doc count at columnar and KV side
            columnar_count = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, datasets[i].full_name)
            if not self.cbas_util.wait_for_ingestion_complete(self.cluster, remote_dataset.full_name, columnar_count):
                results.append(False)
            kv_count = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, remote_dataset.full_name)
            if columnar_count != kv_count:
                self.log.error("Doc count mismatch in KV and columnar {0}, {1}, expected: {2} got: {3}".
                               format(provisioned_collections[i], datasets[i].full_name,
                                      columnar_count, kv_count))
                results.append(columnar_count == kv_count)
        if not all(results):
            self.fail("Mismatch found in Copy To KV")

    def test_create_copyToKv_key_size(self):
        self.base_infra_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        jobs = Queue()
        results = []
        characters = string.ascii_letters + string.digits + string.punctuation
        valid_key_count = 0
        for dataset in datasets:
            docs = []
            for i in range(self.no_of_docs):
                doc = self.cbas_util.generate_docs(document_size=1024)
                key_size = random.choice([245, 246])
                if key_size == 245:
                    valid_key_count += 1
                doc["kv_key"] = ''.join(random.choice(characters) for _ in range(key_size))
                docs.append(doc)
            if not self.cbas_util.insert_into_standalone_collection(self.cluster, dataset.name, docs,
                                                                    dataset.dataverse_name, dataset.database_name):
                self.fail("Failed to insert document")
        self.provisioned_bucket_id, self.provisioned_bucket_name = self.create_capella_bucket()
        self.provisioned_scope_name = self.create_capella_scope(self.provisioned_bucket_id)
        provisioned_collections = []
        primary_key = "kv_key"
        for dataset in datasets:
            collection_name = self.create_capella_collection(self.provisioned_bucket_id,
                                                             self.provisioned_scope_name)
            provisioned_collections.append(collection_name)
            collection = "{}.{}.{}".format(self.provisioned_bucket_name, self.provisioned_scope_name,
                                           collection_name)

            jobs.put((self.cbas_util.copy_to_kv,
                      {"cluster": self.cluster, "collection_name": dataset.name, "database_name": dataset.database_name,
                       "dataverse_name": dataset.dataverse_name, "dest_bucket": collection,
                       "link_name": remote_link.full_name, "primary_key": primary_key,
                       "validate_error_msg": self.input.param("validate_error", False),
                       "expected_error": self.input.param("expected_error", None),
                       "expected_error_code": self.input.param("expected_error_code", None)}))

        self.cbas_util.run_jobs_in_parallel(jobs, results, self.sdk_clients_per_user)
        if not all(results):
            self.fail("Copy to kv statement failed")

        for i in range(len(datasets)):
            remote_dataset = self.cbas_util.create_remote_dataset_obj(self.cluster, self.provisioned_bucket_name,
                                                                      self.provisioned_scope_name,
                                                                      provisioned_collections[i], remote_link,
                                                                      capella_as_source=True)[0]
            if not self.cbas_util.create_remote_dataset(self.cluster, remote_dataset.name,
                                                        remote_dataset.full_kv_entity_name,
                                                        remote_dataset.link_name,
                                                        dataverse_name=remote_dataset.dataverse_name,
                                                        database_name=remote_dataset.database_name):
                self.log.error("Failed to create remote dataset on KV")
                results.append(False)
                # validate doc count at columnar and KV side
                if not self.cbas_util.wait_for_ingestion_complete(self.cluster, remote_dataset.full_name,
                                                                  valid_key_count):
                    results.append(False)
                kv_count = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, remote_dataset.full_name)
                if valid_key_count != kv_count:
                    self.log.error("Doc count mismatch in KV and columnar {0}, {1}, expected: {2} got: {3}".
                                   format(provisioned_collections[i], datasets[i].full_name,
                                          valid_key_count, kv_count))
                    results.append(valid_key_count == kv_count)
        if not all(results):
            self.fail("Mismatch found in Copy To KV")

    def test_create_copyToKv_duplicate_data_key(self):
        self.base_infra_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        jobs = Queue()
        results = []
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name,
                       "no_of_docs": self.no_of_docs, "document_size": self.doc_size}))

        self.cbas_util.run_jobs_in_parallel(jobs, results, self.sdk_clients_per_user, async_run=False)

        self.provisioned_bucket_id, self.provisioned_bucket_name = self.create_capella_bucket()
        self.provisioned_scope_name = self.create_capella_scope(self.provisioned_bucket_id)
        provisioned_collections = []
        if self.input.param("use_key", False):
            primary_key = "email"
        else:
            primary_key = None
        for dataset in datasets:
            collection_name = self.create_capella_collection(self.provisioned_bucket_id,
                                                             self.provisioned_scope_name)
            provisioned_collections.append(collection_name)
            collection = "{}.{}.{}".format(self.provisioned_bucket_name, self.provisioned_scope_name,
                                           collection_name)

            jobs.put((self.cbas_util.copy_to_kv,
                      {"cluster": self.cluster, "collection_name": dataset.name, "database_name": dataset.database_name,
                       "dataverse_name": dataset.dataverse_name, "dest_bucket": collection,
                       "link_name": remote_link.full_name, "primary_key": primary_key}))

        self.cbas_util.run_jobs_in_parallel(jobs, results, self.sdk_clients_per_user)
        if not all(results):
            self.fail("Copy to KV statement failed")

        for i in range(len(datasets)):
            collection = "{}.{}.{}".format(self.provisioned_bucket_name, self.provisioned_scope_name,
                                           provisioned_collections[i])
            dataset = datasets[i]
            jobs.put((self.cbas_util.copy_to_kv,
                      {"cluster": self.cluster, "collection_name": dataset.name, "database_name": dataset.database_name,
                       "dataverse_name": dataset.dataverse_name, "dest_bucket": collection,
                       "link_name": remote_link.full_name, "primary_key": primary_key}))

        self.cbas_util.run_jobs_in_parallel(jobs, results, self.sdk_clients_per_user)

        # validate the copied data
        for i in range(len(datasets)):
            remote_dataset = self.cbas_util.create_remote_dataset_obj(self.cluster, self.provisioned_bucket_name,
                                                                      self.provisioned_scope_name,
                                                                      provisioned_collections[i], remote_link,
                                                                      capella_as_source=True)[0]
            if not self.cbas_util.create_remote_dataset(self.cluster, remote_dataset.name,
                                                        remote_dataset.full_kv_entity_name,
                                                        remote_dataset.link_name,
                                                        dataverse_name=remote_dataset.dataverse_name,
                                                        database_name=remote_dataset.database_name):
                self.log.error("Failed to create remote dataset on KV")
                results.append(False)
            # validate doc count at columnar and KV side
            columnar_count = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, datasets[i].full_name)
            if not primary_key:
                columnar_count = 2 * columnar_count
            if not self.cbas_util.wait_for_ingestion_complete(self.cluster, remote_dataset.full_name, columnar_count):
                results.append(False)
            kv_count = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, remote_dataset.full_name)
            if columnar_count != kv_count:
                self.log.error("Doc count mismatch in KV and columnar {0}, {1}, expected: {2} got: {3}".
                               format(provisioned_collections[i], datasets[i].full_name,
                                      columnar_count, kv_count))
                results.append(columnar_count == kv_count)
        if not all(results):
            self.fail("Mismatch found in Copy To KV")

    def test_create_copyToKv_from_external_collection(self):
        self.base_infra_setup()
        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        datasets = self.cbas_util.get_all_dataset_objs("external")
        jobs = Queue()
        results = []
        self.provisioned_bucket_id, self.provisioned_bucket_name = self.create_capella_bucket()
        self.provisioned_scope_name = self.create_capella_scope(self.provisioned_bucket_id)
        provisioned_collections = []
        for dataset in datasets:
            collection_name = self.create_capella_collection(self.provisioned_bucket_id,
                                                             self.provisioned_scope_name)
            provisioned_collections.append(collection_name)
            collection = "{}.{}.{}".format(self.provisioned_bucket_name, self.provisioned_scope_name,
                                           collection_name)
            jobs.put((self.cbas_util.copy_to_kv,
                      {"cluster": self.cluster, "collection_name": dataset.name, "database_name": dataset.database_name,
                       "dataverse_name": dataset.dataverse_name, "dest_bucket": collection,
                       "link_name": remote_link.full_name, "analytics_timeout": 1000000, "timeout": 100000}))

        self.cbas_util.run_jobs_in_parallel(jobs, results, self.sdk_clients_per_user)
        if not all(results):
            self.fail("Copy to KV statement failed")

        # validate the copied data
        for i in range(len(datasets)):
            remote_dataset = self.cbas_util.create_remote_dataset_obj(self.cluster, self.provisioned_bucket_name,
                                                                      self.provisioned_scope_name,
                                                                      provisioned_collections[i], remote_link,
                                                                      capella_as_source=True)[0]
            if not self.cbas_util.create_remote_dataset(self.cluster, remote_dataset.name,
                                                        remote_dataset.full_kv_entity_name,
                                                        remote_dataset.link_name,
                                                        dataverse_name=remote_dataset.dataverse_name,
                                                        database_name=remote_dataset.database_name):
                self.log.error("Failed to create remote dataset on KV")
                results.append(False)
            # validate doc count at columnar and KV side
            columnar_count = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, datasets[i].full_name)
            if not self.cbas_util.wait_for_ingestion_complete(self.cluster, remote_dataset.full_name, columnar_count):
                results.append(False)
            kv_count = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, remote_dataset.full_name)
            if columnar_count != kv_count:
                self.log.error("Doc count mismatch in KV and columnar {0}, {1}, expected: {2} got: {3}".
                               format(provisioned_collections[i], datasets[i].full_name,
                                      columnar_count, kv_count))
                results.append(columnar_count == kv_count)
        if not all(results):
            self.fail("Mismatch found in Copy To KV")

    def test_negative_cases_invalid_name(self):
        self.base_infra_setup()
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
                collection_name = self.create_capella_collection(self.provisioned_bucket_id,
                                                                 self.provisioned_scope_name)
                collection = "{}.{}.{}".format(self.provisioned_bucket_name, self.provisioned_scope_name,
                                               collection_name)
            if self.input.param("invalid_link", False):
                link_name = self.cbas_util.generate_name()
                expected_error_code = expected_error.format(link_name)
            else:
                link_name = remote_link.full_name

            jobs.put((self.cbas_util.copy_to_kv,
                      {"cluster": self.cluster, "collection_name": dataset.name, "database_name": dataset.database_name,
                       "dataverse_name": dataset.dataverse_name, "dest_bucket": collection,
                       "link_name": link_name, "validate_error_msg": self.input.param("validate_error", False),
                       "expected_error": expected_error_code,
                       "expected_error_code": self.input.param("expected_error_code", None)}))

        self.cbas_util.run_jobs_in_parallel(jobs, results, self.sdk_clients_per_user)
        if not all(results):
            self.fail("Negative test for invalid names failed")

    def test_drop_link_during_copy_to_kv(self):
        self.base_infra_setup()
        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        datasets = self.cbas_util.get_all_dataset_objs("external")
        jobs = Queue()
        results = []
        self.provisioned_bucket_id, self.provisioned_bucket_name = self.create_capella_bucket()
        self.provisioned_scope_name = self.create_capella_scope(self.provisioned_bucket_id)
        provisioned_collections = []
        for dataset in datasets:
            collection_name = self.create_capella_collection(self.provisioned_bucket_id,
                                                             self.provisioned_scope_name)
            provisioned_collections.append(collection_name)
            collection = "{}.{}.{}".format(self.provisioned_bucket_name, self.provisioned_scope_name,
                                           collection_name)
            expected_error = self.input.param("expected_error")
            jobs.put((self.cbas_util.copy_to_kv,
                      {"cluster": self.cluster, "collection_name": dataset.name, "database_name": dataset.database_name,
                       "dataverse_name": dataset.dataverse_name, "dest_bucket": collection,
                       "link_name": remote_link.full_name,
                       "validate_error_msg": self.input.param("validate_error", False),
                       "expected_error": expected_error,
                       "expected_error_code": self.input.param("expected_error_code")}))

        self.cbas_util.run_jobs_in_parallel(jobs, results, self.sdk_clients_per_user, async_run=True)
        time.sleep(4)
        if not self.cbas_util.drop_link(self.cluster, remote_link.full_name):
            self.fail("Failed to drop link while copying to KV")
        jobs.join()

        # validate data in KV re-create remote link
        self.columnar_spec = {"remote_link": {"no_of_remote_links": 1,
                                              "properties": self.columnar_spec["remote_link"]["properties"]}}
        self.cbas_util.create_cbas_infra_from_spec(self.cluster, cbas_spec=self.columnar_spec,
                                                   bucket_util=self.bucket_util)
        all_remote_links = self.cbas_util.get_all_link_objs("couchbase")
        new_remote_link = None
        for i in all_remote_links:
            if i.full_name != remote_link.full_name:
                new_remote_link = i
                break
        for i in range(len(datasets)):
            remote_dataset = self.cbas_util.create_remote_dataset_obj(self.cluster, self.provisioned_bucket_name,
                                                                      self.provisioned_scope_name,
                                                                      provisioned_collections[i], new_remote_link,
                                                                      capella_as_source=True)[0]
            if not self.cbas_util.create_remote_dataset(self.cluster, remote_dataset.name,
                                                        remote_dataset.full_kv_entity_name,
                                                        remote_dataset.link_name,
                                                        dataverse_name=remote_dataset.dataverse_name,
                                                        database_name=remote_dataset.database_name):
                self.log.error("Failed to create remote dataset on KV")
                results.append(False)
            # validate doc count at columnar and KV side
            columnar_count = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, datasets[i].full_name)
            kv_count = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, remote_dataset.full_name)
            results.append(columnar_count != kv_count)
            if not all(results):
                self.fail("Mismatch found in Copy To KV")

    def test_remove_user_access_while_copy_to_kv(self):
        capella_api_v2 = CapellaAPIv2(self.pod.url_public, self.tenant.api_secret_key, self.tenant.api_access_key,
                                      self.tenant.user, self.tenant.pwd)
        self.remote_cluster = self.cb_clusters['C1']
        new_db_user_id = capella_api_v2.create_db_user(self.tenant.id, self.tenant.project_id, self.remote_cluster.id,
                                                       "CopyToKV", "Couchbase@123").json()['id']
        self.remote_cluster.username = "CopyToKV"
        self.base_infra_setup()
        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        dataset = self.cbas_util.get_all_dataset_objs("external")[0]
        link_properties = remote_link.properties
        link_properties['username'] = "CopyToKV"
        link_properties['password'] = "Couchbase@123"
        self.cbas_util.alter_link_properties(self.cluster, link_properties)
        jobs = Queue()
        results = []
        self.provisioned_bucket_id, self.provisioned_bucket_name = self.create_capella_bucket()
        self.provisioned_scope_name = self.create_capella_scope(self.provisioned_bucket_id)
        collection_name = self.create_capella_collection(self.provisioned_bucket_id,
                                                         self.provisioned_scope_name)
        collection = "{}.{}.{}".format(self.provisioned_bucket_name, self.provisioned_scope_name,
                                       collection_name)

        jobs.put((self.cbas_util.copy_to_kv,
                  {"cluster": self.cluster, "collection_name": dataset.name, "database_name": dataset.database_name,
                   "dataverse_name": dataset.dataverse_name, "dest_bucket": collection,
                   "link_name": remote_link.full_name,
                   "validate_error_msg": self.input.param("validate_error", False),
                   "expected_error": self.input.param("expected_error", ""),
                   "expected_error_code": self.input.param("expected_error_code", "")}))

        self.cbas_util.run_jobs_in_parallel(jobs, results, self.sdk_clients_per_user, async_run=True)
        time.sleep(10)
        capella_api_v2.delete_db_user(self.tenant.id, self.tenant.project_id, self.remote_cluster.id, new_db_user_id)
        jobs.join()

    def test_copy_to_kv_drop_remote_dataset(self):
        self.base_infra_setup()
        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        datasets = self.cbas_util.get_all_dataset_objs("external")
        jobs = Queue()
        results = []
        self.provisioned_bucket_id, self.provisioned_bucket_name = self.create_capella_bucket()
        self.provisioned_scope_name = self.create_capella_scope(self.provisioned_bucket_id)
        provisioned_collections = []
        for dataset in datasets:
            collection_name = self.create_capella_collection(self.provisioned_bucket_id,
                                                             self.provisioned_scope_name)
            provisioned_collections.append(collection_name)
            collection = "{}.{}.{}".format(self.provisioned_bucket_name, self.provisioned_scope_name,
                                           collection_name)
            expected_error = self.input.param("expected_error")
            jobs.put((self.cbas_util.copy_to_kv,
                      {"cluster": self.cluster, "collection_name": dataset.name, "database_name": dataset.database_name,
                       "dataverse_name": dataset.dataverse_name, "dest_bucket": collection,
                       "link_name": remote_link.full_name,
                       "validate_error_msg": self.input.param("validate_error", False),
                       "expected_error": expected_error,
                       "expected_error_code": self.input.param("expected_error_code")}))

        self.cbas_util.run_jobs_in_parallel(jobs, results, self.sdk_clients_per_user, async_run=True)
        time.sleep(4)
        if not self.delete_capella_bucket(bucket_id=self.provisioned_bucket_id):
            self.fail("Failed to drop remote bucket while copying to KV")
        else:
            self.provisioned_bucket_id = None
        jobs.join()

    def test_copy_to_kv_drop_columnar_dataset(self):
        self.base_infra_setup()
        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        datasets = self.cbas_util.get_all_dataset_objs("external")
        jobs = Queue()
        results = []
        self.provisioned_bucket_id, self.provisioned_bucket_name = self.create_capella_bucket()
        self.provisioned_scope_name = self.create_capella_scope(self.provisioned_bucket_id)
        provisioned_collections = []
        for dataset in datasets:
            collection_name = self.create_capella_collection(self.provisioned_bucket_id,
                                                             self.provisioned_scope_name)
            provisioned_collections.append(collection_name)
            collection = "{}.{}.{}".format(self.provisioned_bucket_name, self.provisioned_scope_name,
                                           collection_name)
            expected_error = self.input.param("expected_error")
            jobs.put((self.cbas_util.copy_to_kv,
                      {"cluster": self.cluster, "collection_name": dataset.name, "database_name": dataset.database_name,
                       "dataverse_name": dataset.dataverse_name, "dest_bucket": collection,
                       "link_name": remote_link.full_name,
                       "validate_error_msg": self.input.param("validate_error", False),
                       "expected_error": expected_error,
                       "expected_error_code": self.input.param("expected_error_code")}))

        self.cbas_util.run_jobs_in_parallel(jobs, results, self.sdk_clients_per_user, async_run=True)
        time.sleep(10)
        for dataset in datasets:
            if not self.cbas_util.drop_dataset(self.cluster, dataset.full_name):
                self.fail("Failed to drop columnar dataset while copying to KV")
        jobs.join()

    def test_create_copyToKV_from_collection_aggregate_group_by(self):
        self.base_infra_setup()
        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        no_of_docs = self.input.param("no_of_docs", 1000)
        jobs = Queue()
        results = []
        for i in range(len(datasets)):
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.cluster, "collection_name": datasets[i].name,
                       "dataverse_name": datasets[i].dataverse_name, "database_name": datasets[i].database_name,
                       "no_of_docs": no_of_docs}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.log.error("Not all docs were inserted")
        results = []
        self.provisioned_bucket_id, self.provisioned_bucket_name = self.create_capella_bucket()
        self.provisioned_scope_name = self.create_capella_scope(self.provisioned_bucket_id)
        provisioned_collections = []
        query = "SELECT country, ARRAY_AGG(city) AS city FROM {0} GROUP BY country"
        for dataset in datasets:
            collection_name = self.create_capella_collection(self.provisioned_bucket_id,
                                                             self.provisioned_scope_name)
            provisioned_collections.append(collection_name)
            collection = "{}.{}.{}".format(self.provisioned_bucket_name, self.provisioned_scope_name,
                                           collection_name)
            source_definition = query.format(dataset.full_name)
            jobs.put((self.cbas_util.copy_to_kv,
                      {"cluster": self.cluster, "source_definition": source_definition, "dest_bucket": collection,
                       "link_name": remote_link.full_name}))
        self.cbas_util.run_jobs_in_parallel(jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.fail("Copy to KV statement failed")

        for i in range(len(datasets)):
            remote_dataset = self.cbas_util.create_remote_dataset_obj(self.cluster, self.provisioned_bucket_name,
                                                                      self.provisioned_scope_name,
                                                                      provisioned_collections[i], remote_link,
                                                                      capella_as_source=True)[0]
            if not self.cbas_util.create_remote_dataset(self.cluster, remote_dataset.name,
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
                status, metrics, errors, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster,
                                                                                                   statement)
                country_name = ((result[0])[CBASHelper.unformat_name(datasets[i].name)]["country"]).replace("&amp;",
                                                                                                            "")
                query_statement = ("SELECT ARRAY_LENGTH(ARRAY_AGG(city)) as city "
                                   "FROM {0} where country = \"{1}\"").format(
                    datasets[i].full_name, str(country_name))
                remote_statement = "select * from {0} where country = \"{1}\"".format(
                    remote_dataset.full_name, str(country_name))

                status, metrics, errors, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster,
                                                                                                   remote_statement)
                length_of_city_array_from_remote = len((result[0][remote_dataset.name])['city'])

                status, metrics, errors, result2, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster,
                                                                                                    query_statement)
                length_of_city_array_from_dataset = result2[0]["city"]
                if length_of_city_array_from_dataset != length_of_city_array_from_remote:
                    self.log.error("Length of city aggregate failed")
                results.append(length_of_city_array_from_remote == length_of_city_array_from_dataset)

        if not all(results):
            self.fail("Copy to statement copied the wrong results")

    def test_create_copy_to_kv_doc_size_32_MB(self):
        # max doc size supported by KV is 20 MB
        self.base_infra_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        jobs = Queue()
        results = []
        self.provisioned_bucket_id, self.provisioned_bucket_name = self.create_capella_bucket()
        self.provisioned_scope_name = self.create_capella_scope(self.provisioned_bucket_id)
        provisioned_collections = []
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.cluster, "collection_name": dataset.name, "dataverse_name": dataset.dataverse_name,
                       "database_name": dataset.database_name, "no_of_docs": self.no_of_docs,
                       "document_size": self.doc_size}))
        self.cbas_util.run_jobs_in_parallel(jobs, results, self.sdk_clients_per_user, async_run=False)
        for dataset in datasets:
            if self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, dataset.full_name) == 0:
                self.fail("Failed to load data in standalone collection of size: {}".format(self.doc_size))

        for i in range(len(datasets)):
            collection_name = self.create_capella_collection(self.provisioned_bucket_id,
                                                             self.provisioned_scope_name)
            provisioned_collections.append(collection_name)
            collection = "{}.{}.{}".format(self.provisioned_bucket_name, self.provisioned_scope_name,
                                           collection_name)

            jobs.put((self.cbas_util.copy_to_kv,
                      {"cluster": self.cluster, "collection_name": datasets[i].name,
                       "database_name": datasets[i].database_name, "dataverse_name": datasets[i].dataverse_name,
                       "dest_bucket": collection, "link_name": remote_link.full_name,
                       "validate_error_msg": True}))

        self.cbas_util.run_jobs_in_parallel(jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.fail("Copy to KV statement failed")

    def test_create_copyToKV_from_multiple_collection_query_drop_standalone_collection(self):
        self.base_infra_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        jobs = Queue()
        results = []
        self.provisioned_bucket_id, self.provisioned_bucket_name = self.create_capella_bucket()
        self.provisioned_scope_name = self.create_capella_scope(self.provisioned_bucket_id)
        provisioned_collections = []
        unique_pairs = []
        self.pairs(unique_pairs, datasets)
        for dataset in datasets:
            if dataset.data_source is None:
                jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                          {"cluster": self.cluster, "collection_name": dataset.name,
                           "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name,
                           "no_of_docs": self.no_of_docs}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        results = []
        for i in range(len(unique_pairs)):
            statement = "select * from {0} as a, {1} as b where a.avg_rating > 0.4 and b.avg_rating > 0.4".format(
                (unique_pairs[i][0]).full_name, (unique_pairs[i][1]).full_name
            )
            collection_name = self.create_capella_collection(self.provisioned_bucket_id, self.provisioned_scope_name)
            provisioned_collections.append(collection_name)
            collection = "{}.{}.{}".format(self.provisioned_bucket_name, self.provisioned_scope_name, collection_name)

            jobs.put((self.cbas_util.copy_to_kv,
                      {"cluster": self.cluster, "source_definition": statement, "dest_bucket": collection,
                       "link_name": remote_link.full_name, "function": "concat(c.a.name,c.b.name)",
                       "timeout": 100000}))
        time.sleep(10)
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.log.error("Copy to KV statement failure")

        for i in range(len(unique_pairs)):
            remote_dataset = self.cbas_util.create_remote_dataset_obj(self.cluster, self.provisioned_bucket_name,
                                                                      self.provisioned_scope_name,
                                                                      provisioned_collections[i], remote_link,
                                                                      capella_as_source=True)[0]
            if not self.cbas_util.create_remote_dataset(self.cluster, remote_dataset.name,
                                                        remote_dataset.full_kv_entity_name,
                                                        remote_dataset.link_name,
                                                        dataverse_name=remote_dataset.dataverse_name,
                                                        database_name=remote_dataset.database_name):
                self.log.fail("Failed to create remote dataset on KV")

            dataset_statement = ("select count(*) from {0} as a, {1} as b"
                                 " where a.avg_rating > 0.4 and b.avg_rating > 0.4").format(
                (unique_pairs[i][0]).full_name, (unique_pairs[i][1]).full_name
            )
            status, metrics, errors, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster,
                                                                                               dataset_statement)
            if not self.cbas_util.wait_for_ingestion_complete(self.cluster, remote_dataset.full_name, result[0]['$1']):
                results.append("False")
            remote_statement = "select count(*) from {}".format(remote_dataset.full_name)
            status, metrics, errors, result1, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster,
                                                                                                remote_statement)

            if result[0]['$1'] != result1[0]['$1']:
                self.log.error("Document count mismatch in remote dataset {0}".format(
                    remote_dataset.full_name
                ))
            results.append(result[0]['$1'] == result1[0]['$1'])

            if not all(results):
                self.fail("The document count does not match in remote source and KV")

    def test_copy_to_kv_scaling(self):
        self.commonAPI = CommonCapellaAPI(self.pod.url_public, '', '', self.tenant.user, self.tenant.pwd,
                                          self.input.param("internal_support_token"))
        self.base_infra_setup()
        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        datasets = self.cbas_util.get_all_dataset_objs("external")
        jobs = Queue()
        results = []
        self.provisioned_bucket_id, self.provisioned_bucket_name = self.create_capella_bucket()
        self.provisioned_scope_name = self.create_capella_scope(self.provisioned_bucket_id)
        provisioned_collections = []
        start_time = time.time()
        for dataset in datasets:
            collection_name = self.create_capella_collection(self.provisioned_bucket_id,
                                                             self.provisioned_scope_name)
            provisioned_collections.append(collection_name)
            collection = "{}.{}.{}".format(self.provisioned_bucket_name, self.provisioned_scope_name,
                                           collection_name)
            jobs.put((self.cbas_util.copy_to_kv,
                      {"cluster": self.cluster, "collection_name": dataset.name, "database_name": dataset.database_name,
                       "dataverse_name": dataset.dataverse_name, "dest_bucket": collection,
                       "link_name": remote_link.full_name}))

        self.cbas_util.run_jobs_in_parallel(jobs, results, self.sdk_clients_per_user, async_run=True)
        status = None
        time.sleep(10)
        self.columnarAPI.update_columnar_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id,
                                                  self.cluster.name, '', 2)
        jobs.join()
        while status != "healthy" and start_time + 900 > time.time():
            resp = self.commonAPI.get_cluster_info_internal(self.cluster.cluster_id).json()
            status = resp["meta"]["status"]["state"]

        if status is None or status != "healthy":
            self.fail("Fail to scale cluster while copy to kv")

    def validate_copy_to_kv_result(self, datasets, provisioned_collections, remote_link, validation_results):
        for i in range(len(datasets)):
            remote_dataset = self.cbas_util.create_remote_dataset_obj(self.cluster, self.provisioned_bucket_name,
                                                                      self.provisioned_scope_name,
                                                                      (provisioned_collections[i].split('.'))[2],
                                                                      remote_link,
                                                                      capella_as_source=True)[0]
            if not self.cbas_util.create_remote_dataset(self.cluster, remote_dataset.name,
                                                        remote_dataset.full_kv_entity_name,
                                                        remote_dataset.link_name,
                                                        dataverse_name=remote_dataset.dataverse_name,
                                                        database_name=remote_dataset.database_name):
                self.log.error("Failed to create remote dataset on KV")
                validation_results.append(False)
            # validate doc count at columnar and KV side
            columnar_count = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, datasets[i].full_name)
            kv_count = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, remote_dataset.full_name)
            if columnar_count != kv_count:
                self.log.error("Doc count mismatch in KV and columnar {0}, {1}, expected: {2} got: {3}".
                               format(provisioned_collections[i], datasets[i].full_name,
                                      columnar_count, kv_count))
                validation_results.append(columnar_count != kv_count)
            self.cbas_util.drop_dataset(self.cluster, remote_dataset.full_name)
        if not all(validation_results):
            self.fail("Mismatch found in Copy To KV")

    def create_copy_to_kv_all_datasets(self, datasets, copy_to_kv_job, copy_to_kv_collections, remote_links):
        for dataset in datasets:
            if type(remote_links, list):
                remote_link = random.choice(remote_links)
            else:
                remote_link = remote_links
            collection_name = self.create_capella_collection(self.provisioned_bucket_id,
                                                             self.provisioned_scope_name)
            collection = "{}.{}.{}".format(CBASHelper.format_name(self.provisioned_bucket_name),
                                           CBASHelper.format_name(self.provisioned_scope_name),
                                           CBASHelper.format_name(collection_name))
            copy_to_kv_collections.append(collection)
            copy_to_kv_job.put((self.cbas_util.copy_to_kv,
                                {"cluster": self.cluster, "collection_name": dataset.name,
                                 "database_name": dataset.database_name,
                                 "dataverse_name": dataset.dataverse_name, "dest_bucket": collection,
                                 "link_name": remote_link.full_name, "analytics_timeout": 1000000}))

    def mini_volume_copy_to_kv(self):
        self.base_infra_setup()
        self.copy_to_kv_job = Queue()
        self.mini_volume = MiniVolume(self, "http://127.0.0.1:4000")
        self.mini_volume.calculate_volume_per_source()

        # initiate copy to kv
        for i in range(1, 5):
            if i % 2 == 0:
                self.mini_volume.run_processes(i, 2 ** (i - 1), False)
            else:
                self.mini_volume.run_processes(i, 2 ** (i + 1), False)
            datasets = self.cbas_util.get_all_dataset_objs()
            remote_link = self.cbas_util.get_all_link_objs("couchbase")
            self.provisioned_bucket_id, self.provisioned_bucket_name = self.create_capella_bucket()
            self.provisioned_scope_name = self.create_capella_scope(self.provisioned_bucket_id)
            copy_to_kv_collections = []
            copy_to_kv_results = []
            self.create_copy_to_kv_all_datasets(datasets, self.copy_to_kv_job, copy_to_kv_collections, remote_link)
            self.cbas_util.run_jobs_in_parallel(self.copy_to_kv_job, copy_to_kv_results, self.sdk_clients_per_user,
                                                async_run=False)
            if not all(copy_to_kv_results):
                self.fail("Copy to KV statement failed")

            validation_results = []
            self.validate_copy_to_kv_result(datasets, copy_to_kv_collections, remote_link, validation_results)
            self.mini_volume.stop_process(query_pass=True)
            self.delete_capella_bucket(self.provisioned_bucket_id)

        self.log.info("Copy to KV mini volume finish")
