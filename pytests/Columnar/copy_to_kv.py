"""
Created on 4-March-2024

@author: abhay.aggrawal@couchbase.com
"""
import json
import random
import threading
import time
from Queue import Queue

from CbasLib.CBASOperations import CBASHelper
from Columnar.columnar_base import ColumnarBaseTest
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI
from capellaAPI.capella.columnar.CapellaAPI import CapellaAPI as ColumnarAPI
from capellaAPI.capella.common.CapellaAPI import CommonCapellaAPI
from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI as CapellaAPIv2
from itertools import combinations, product
from sirius_client_framework.multiple_database_config import CouchbaseLoader
from sirius_client_framework.operation_config import WorkloadOperationConfig
from Jython_tasks.sirius_task import WorkLoadTask
from Jython_tasks.task_manager import TaskManager
from sirius_client_framework.sirius_constants import SiriusCodes


class CopyToKv(ColumnarBaseTest):
    def setUp(self):
        super(CopyToKv, self).setUp()
        self.cluster = self.tenant.columnar_instances[0]
        self.aws_access_key = self.input.param("aws_access_key")
        self.aws_secret_key = self.input.param("aws_secret_key")
        self.aws_session_token = self.input.param("aws_session_token", "")
        self.doc_loader_url = self.input.param("sirius_url", None)
        self.doc_loader_port = self.input.param("sirius_port", None)
        self.no_of_docs = self.input.param("no_of_docs", 1000)
        self.capellaAPI = CapellaAPI(self.pod.url_public, '', '', self.tenant.user, self.tenant.pwd, '')
        self.columnarAPI = ColumnarAPI(self.pod.url_public, '', '' , self.tenant.user, self.tenant.pwd, '')


        # if none provided use 1 Kb doc size
        self.doc_size = self.input.param("doc_size", 1000)

        if not self.columnar_spec_name:
            self.columnar_spec_name = "regressions.copy_to_s3"

        self.columnar_spec = self.cbas_util.get_columnar_spec(
            self.columnar_spec_name)

        self.aws_region = self.input.param("aws_region", "ap-south-1")

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
        super(ColumnarBaseTest, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished", stage="Teardown")

    def pairs(self, *lists):
        for t in combinations(lists, 2):
            for pair in product(*t):
                yield pair

    def base_infra_setup(self):
        self.columnar_spec["dataverse"]["no_of_dataverses"] = self.input.param(
            "no_of_scopes", 1)
        self.columnar_spec["remote_link"]["no_of_remote_links"] = self.input.param(
            "no_of_remote_links", 0)

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
            remote_cluster_certificate = (remote_cluster_certificate_request.json()["certificate"])
        else:
            self.fail("Failed to get cluster certificate")
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
                "external_container_name": self.input.param("s3_source_bucket", None),
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
        if not hasattr("self", "remote_cluster"):
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
        response = self.capellaAPI.cluster_ops_apis.create_bucket(self.tenant.id, self.tenant.project_id,
                                                                  self.remote_cluster.id, bucket_name, "couchbase",
                                                                  storage_type, 1000, "seqno",
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
            kv_count = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, remote_dataset.full_name)
            if columnar_count != kv_count:
                self.log.error("Doc count mismatch in KV and columnar {0}, {1}, expected: {2} got: {3}".
                               format(provisioned_collections[i], datasets[i].full_name,
                                      columnar_count, kv_count))
                results.append(columnar_count != kv_count)
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
            kv_count = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, remote_dataset.full_name)
            if not primary_key:
                columnar_count = 2 * columnar_count
            if columnar_count != kv_count:
                self.log.error("Doc count mismatch in KV and columnar {0}, {1}, expected: {2} got: {3}".
                               format(provisioned_collections[i], datasets[i].full_name,
                                      columnar_count, kv_count))
                results.append(columnar_count != kv_count)
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
            kv_count = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, remote_dataset.full_name)
            if columnar_count != kv_count:
                self.log.error("Doc count mismatch in KV and columnar {0}, {1}, expected: {2} got: {3}".
                               format(provisioned_collections[i], datasets[i].full_name,
                                      columnar_count, kv_count))
                results.append(columnar_count != kv_count)
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
            if self.input.param("invalid_dataset", False):
                dataset.name = self.cbas_util.generate_name()
                expected_error = expected_error.format(dataset.name)
            if self.input.param("invalid_kv_entity", False):
                collection_name = self.cbas_util.generate_name()
                collection = "{}.{}.{}".format(self.provisioned_bucket_name, self.provisioned_scope_name,
                                               collection_name)
                expected_error = expected_error.format(collection)
            else:
                collection_name = self.create_capella_collection(self.provisioned_bucket_id,
                                                                 self.provisioned_scope_name)
                collection = "{}.{}.{}".format(self.provisioned_bucket_name, self.provisioned_scope_name,
                                               collection_name)
            if self.input.param("invalid_link", False):
                link_name = self.cbas_util.generate_name()
                expected_error = expected_error.format("Default." + link_name)
            else:
                link_name = remote_link.full_name

            jobs.put((self.cbas_util.copy_to_kv,
                      {"cluster": self.cluster, "collection_name": dataset.name, "database_name": dataset.database_name,
                       "dataverse_name": dataset.dataverse_name, "dest_bucket": collection,
                       "link_name": link_name, "validate_error_msg": self.input.param("validate_error", False),
                       "expected_error": expected_error,
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
            if remote_link.database_name == "Default":
                expected_error = expected_error.format(remote_link.dataverse_name + '.' + remote_link.name)
            else:
                expected_error = expected_error.format(remote_link.full_name)
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
        self.columnar_spec = {"remote_link": {"properties": self.columnar_spec["remote_link"]["properties"]}}
        self.cbas_util.create_cbas_infra_from_spec(self.cluster, cbas_spec=self.columnar_spec)
        all_remote_links = self.cbas_util.get_all_link_objs("couchbase")
        new_remote_link = None
        for i in all_remote_links:
            if i.full_name != remote_link.full_name:
                new_remote_link = i
                break
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
            kv_count = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, remote_dataset.full_name)
            if columnar_count != kv_count:
                self.log.error("Doc count mismatch in KV and columnar {0}, {1}, expected: {2} got: {3}".
                               format(provisioned_collections[i], datasets[i].full_name,
                                      columnar_count, kv_count))
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
            if remote_link.database_name == "Default":
                expected_error = expected_error.format(remote_link.dataverse_name + '.' + remote_link.name)
            else:
                expected_error = expected_error.format(remote_link.full_name)
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
            if remote_link.database_name == "Default":
                expected_error = expected_error.format(remote_link.dataverse_name + '.' + remote_link.name)
            else:
                expected_error = expected_error.format(remote_link.full_name)
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
                      {"cluster": self.cluster, "source_definition":source_definition, "dest_bucket": collection,
                       "link_name": remote_link.full_name}))
        self.cbas_util.run_jobs_in_parallel(jobs, results, self.sdk_clients_per_user, async_run=False)

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
            for j in range(5):
                statement = "select * from {0} limit 1".format(datasets[i].full_name)
                status, metrics, errors, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster,
                                                                                                   statement)
                country_name = ((result[0])[CBASHelper.unformat_name(datasets[i].name)]["country"]).replace("&amp;",
                                                                                                            "")
                query_statement = "SELECT ARRAY_LENGTH(ARRAY_AGG(city)) as city FROM {0} where country = \"{1}\"".format(
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
            jobs.put((self.cbas_util.load_doc_to_standalone_collection_sirius,
                                  {"collection_name": dataset.name, "dataverse_name": dataset.dataverse_name,
                                   "database_name": dataset.database_name,
                                   "connection_string": "couchbases://" + self.cluster.srv,
                                   "start": 1, "end": self.no_of_docs, "doc_size": self.doc_size,
                                   "username": self.cluster.servers[0].rest_username,
                                   "password": self.cluster.servers[0].rest_password}))
        self.cbas_util.run_jobs_in_parallel(jobs, results, self.sdk_clients_per_user, async_run=False)

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
        for pair in self.pairs(datasets):
            unique_pairs.append(pair)
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
            collection = "{}.{}.{}".format(self.provisioned_bucket_id, self.provisioned_scope,
                                           self.create_capella_collection(self.provisioned_bucket_id,
                                                                          self.provisioned_scope))
            provisioned_collections.append(collection)
            jobs.put((self.cbas_util.copy_to_kv,
                      {"cluster": self.cluster, "source_definition": statement, "dest_bucket": collection,
                       "link_name": remote_link.full_name}))

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

            dataset_statement = "select count(*) from {0} as a, {1} as b where a.avg_rating > 0.4 and b.avg_rating > 0.4".format(
                (unique_pairs[i][0]).full_name, (unique_pairs[i][1]).full_name
            )
            status, metrics, errors, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster,
                                                                                               dataset_statement)

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

    def run_queries_on_datasets(self, query_jobs):
        self.cbas_util.get_all_dataset_objs()
        queries = ["SELECT name, AVG(review.rating.rating_value) AS avg_rating FROM {} AS d "
                   "UNNEST d.reviews AS review GROUP BY d.name;",
                   "SELECT name, COUNT(review) AS review_count FROM {} AS d "
                   "UNNEST d.reviews AS review GROUP BY d.name;",
                   "SELECT name, AVG(review.rating.checkin) AS avg_checkin_rating FROM mjObARKxQxO7tt5ZOn AS d "
                   "UNNEST d.reviews AS review WHERE review.rating.checkin IS NOT MISSING GROUP BY d.name;"
                   ]
        datasets = self.cbas_util.get_all_dataset_objs()
        while self.run_queries:
            if query_jobs.qsize() < 5:
                dataset = random.choice(datasets)
                query = random.choice(queries).format(dataset.full_name)
                query_jobs.put((self.cbas_util.execute_statement_on_cbas_util,
                                {"cluster": self.cluster, "statement": query, "analytics_timeout": 100000}))

    def load_doc_to_remote_collections(self, bucket, scope, collection, start, end):
        database_information = CouchbaseLoader(username= self.remote_cluster.username, password=self.remote_cluster.password,
                                               connection_string="couchbases://"  +self.remote_cluster.srv,
                                               bucket=bucket, scope=scope, collection=collection, sdk_batch_size=10)
        operation_config = WorkloadOperationConfig(start=start, end=end, template="hotel", doc_size=self.doc_size)
        task_insert = WorkLoadTask(task_manager=self.task, op_type=SiriusCodes.DocOps.CREATE,
                                   database_information=database_information, operation_config=operation_config)
        task_manager = TaskManager(1)
        task_manager.add_new_task(task_insert)
        task_manager.get_task_result(task_insert)
        return

    def load_doc_in_remote_and_standalone_collection(self, data_loading_job, start, end, remote_collection=None):
        standalone_dataset = self.cbas_util.get_all_dataset_objs("standalone")

        # start data load on standalone collections
        for dataset in standalone_dataset:
            data_loading_job.put((self.cbas_util.load_doc_to_standalone_collection_sirius,
                                  {"collection_name": dataset.name, "dataverse_name": dataset.dataverse_name,
                                   "database_name": dataset.database_name,
                                   "connection_string": "couchbases://" + self.cluster.srv,
                                   "start": start, "end": end, "doc_size": self.doc_size,
                                   "username": self.cluster.servers[0].rest_username,
                                   "password": self.cluster.servers[0].rest_password}))

        for collection in remote_collection:
            bucket, scope, collection = collection.split('.')
            data_loading_job.put((self.load_doc_to_remote_collections,
                                  {"bucket": bucket, "scope": scope, "collection": collection,
                                   "start": start, "end": end}))

    def validate_copy_to_kv_result(self, datasets, provisioned_collections, remote_link, validation_results):
        for i in range(len(datasets)):
            remote_dataset = self.cbas_util.create_remote_dataset_obj(self.cluster, self.provisioned_bucket_name,
                                                                      self.provisioned_scope_name,
                                                                      (provisioned_collections[i].split('.'))[2], remote_link,
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
        if not all(validation_results):
            self.run_queries = False
            self.fail_job("Mismatch found in Copy To KV", query_job=self.query_job)

    def create_copy_to_kv_all_datasets(self, datasets, copy_to_kv_job, copy_to_kv_collections, remote_link):
        for dataset in datasets:
            collection_name = self.create_capella_collection(self.provisioned_bucket_id,
                                                             self.provisioned_scope_name)
            collection = "{}.{}.{}".format(CBASHelper.format_name(self.provisioned_bucket_name),
                                           CBASHelper.format_name(self.provisioned_scope_name),
                                           CBASHelper.format_name(collection_name))
            copy_to_kv_collections.append(collection)
            copy_to_kv_job.put((self.cbas_util.copy_to_kv,
                      {"cluster": self.cluster, "collection_name": dataset.name, "database_name": dataset.database_name,
                       "dataverse_name": dataset.dataverse_name, "dest_bucket": collection,
                       "link_name": remote_link.full_name, "analytics_timeout": 1000000}))

    def scale_columnar_cluster(self, nodes):
        start_time = time.time()
        status = None
        resp = self.columnarAPI.update_columnar_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id,
                                                  self.cluster.name, '', nodes)
        if resp.status_code != 202:
            self.fail("Failed to scale cluster")
        while status != "healthy" and start_time + 900 > time.time():
            resp = self.commonAPI.get_cluster_info_internal(self.cluster.cluster_id).json()
            status = resp["meta"]["status"]["state"]
            if status == "healthy":
                return

        self.run_queries = False
        self.fail_job("Cluster state is {} after 15 minutes".format(status), query_job=self.query_job)

    def fail_job(self, message, **kwargs):
        for key, value in kwargs.items():
            value.join()
        self.fail(message)

    def mini_volume_copy_to_kv(self):
        self.sirius_base_url = "http://127.0.0.1:4000"
        self.base_infra_setup()
        self.commonAPI = CommonCapellaAPI(self.pod.url_public, '', '', self.tenant.user, self.tenant.pwd,
                                          self.input.param("internal_support_token"))

        source_collections = []
        self.provisioned_bucket_id, self.provisioned_bucket_name = self.create_capella_bucket()
        self.provisioned_scope_name = self.create_capella_scope(self.provisioned_bucket_id)
        for i in range(self.input.param("no_of_capella_collection")):
            collection_name = self.create_capella_collection(self.provisioned_bucket_id,
                                                             self.provisioned_scope_name)
            source_collections.append("{}.{}.{}".format(self.provisioned_bucket_name, self.provisioned_scope_name,
                                           collection_name))

        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        for dataset in source_collections:
            bucket, scope, collection_name = dataset.split('.')
            remote_dataset = self.cbas_util.create_remote_dataset_obj(self.cluster, bucket, scope, collection_name,
                                                                      remote_link, capella_as_source=True)[0]
            if not self.cbas_util.create_remote_dataset(self.cluster, remote_dataset.name,
                                                        remote_dataset.full_kv_entity_name,
                                                        remote_dataset.link_name,
                                                        dataverse_name=remote_dataset.dataverse_name,
                                                        database_name=remote_dataset.database_name):
                self.log.error("Failed to create remote dataset")

        self.query_job = Queue()
        self.data_loading_job = Queue()
        self.copy_to_kv_job = Queue()
        create_query_job = Queue()
        results = []
        self.load_doc_in_remote_and_standalone_collection(self.data_loading_job,  1, self.no_of_docs//3,
                                                          source_collections)
        self.cbas_util.run_jobs_in_parallel(self.data_loading_job, results, thread_count=5, async_run=True)

        # run query on datasets until doc loading is complete
        self.run_queries = True
        query_work_results = []
         # Create a new queue for the query job
        create_query_job.put((self.run_queries_on_datasets, {"query_jobs": self.query_job}))
        self.cbas_util.run_jobs_in_parallel(create_query_job, query_work_results, 1, async_run=True)
        while self.query_job.qsize() < 5:
            self.log.info("Waiting for query job to be created")
            time.sleep(10)
        self.cbas_util.run_jobs_in_parallel(self.query_job, query_work_results, 3, async_run=True)

        # wait for data loading to complete
        self.data_loading_job.join()

        # start copy to kv and validate the results
        datasets = self.cbas_util.get_all_dataset_objs()
        self.provisioned_bucket_id, self.provisioned_bucket_name = self.create_capella_bucket()
        self.provisioned_scope_name = self.create_capella_scope(self.provisioned_bucket_id)
        copy_to_kv_collections = copy_to_kv_results = []

        self.create_copy_to_kv_all_datasets(datasets, self.copy_to_kv_job, copy_to_kv_collections, remote_link)
        # run and wait for copy_to_kv_jobs_to_finish
        self.cbas_util.run_jobs_in_parallel(self.copy_to_kv_job, copy_to_kv_results, self.sdk_clients_per_user,
                                            async_run=False)
        if not all(copy_to_kv_results):
            self.fail_job("Copy to KV statement failed", query_job=self.query_job)

        # validate the result here
        validation_results = []
        self.validate_copy_to_kv_result(datasets, copy_to_kv_collections, remote_link, validation_results)

        # scale the cluster while queries are running starting with one node cluster
        for i in [4, 2, 16, 8]:
            self.scale_columnar_cluster(i)

        # load more data
        self.load_doc_in_remote_and_standalone_collection(self.data_loading_job, self.no_of_docs//3,
                                                          (self.no_of_docs // 3) * 2, source_collections)
        self.cbas_util.run_jobs_in_parallel(self.data_loading_job, results, thread_count=5)
        self.data_loading_job.join()

        # start copy to kv and validate the results
        datasets = self.cbas_util.get_all_dataset_objs()
        self.provisioned_bucket_id, self.provisioned_bucket_name = self.create_capella_bucket()
        self.provisioned_scope_name = self.create_capella_scope(self.provisioned_bucket_id)
        copy_to_kv_collections = copy_to_kv_results = []

        self.create_copy_to_kv_all_datasets(datasets, self.copy_to_kv_job, copy_to_kv_collections, remote_link)
        # run and wait for copy_to_kv_jobs_to_finish
        self.cbas_util.run_jobs_in_parallel(self.copy_to_kv_job, copy_to_kv_results, self.sdk_clients_per_user,
                                            async_run=False)

        if not all(copy_to_kv_results):
            self.run_queries = False
            self.fail_job("Copy to KV statement failed", query_job=self.query_job)

        # validate the result here
        validation_results = []
        self.validate_copy_to_kv_result(datasets, copy_to_kv_collections, remote_link, validation_results)

        # scale the cluster while queries are running
        for i in [4, 2, 16, 8]:
            self.scale_columnar_cluster(i)

        # load more data
        self.load_doc_in_remote_and_standalone_collection(self.data_loading_job, (self.no_of_docs // 3) * 2,
                                                          self.no_of_docs, source_collections)
        self.cbas_util.run_jobs_in_parallel(self.data_loading_job, results, thread_count=5)
        self.data_loading_job.join()

        # start copy to kv and validate the results
        datasets = self.cbas_util.get_all_dataset_objs()
        self.provisioned_bucket_id, self.provisioned_bucket_name = self.create_capella_bucket()
        self.provisioned_scope_name = self.create_capella_scope(self.provisioned_bucket_id)
        copy_to_kv_collections = copy_to_kv_results = []

        self.create_copy_to_kv_all_datasets(datasets, self.copy_to_kv_job, copy_to_kv_collections, remote_link)
        # run and wait for copy_to_kv_jobs_to_finish
        self.cbas_util.run_jobs_in_parallel(self.copy_to_kv_job, copy_to_kv_results, self.sdk_clients_per_user,
                                            async_run=False)
        if not all(copy_to_kv_results):
            self.run_queries = False
            self.fail_job("Copy to KV statement failed", query_job=self.query_job)

        # scale the cluster while queries are running
        for i in [4, 2, 16, 8]:
            self.scale_columnar_cluster(i)

        # validate the result here
        validation_results = []
        self.validate_copy_to_kv_result(datasets, copy_to_kv_collections, remote_link, validation_results)

        self.run_queries = False
        self.query_job.join()
        if not all(query_work_results):
            self.fail_job("Queries Failed", query_job=self.query_job)