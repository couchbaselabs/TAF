"""
Created on 4-March-2024

@author: abhay.aggrawal@couchbase.com
"""
import random
import time
from Queue import Queue
from Columnar.columnar_base import ColumnarBaseTest
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI


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
        # super(ColumnarBaseTest, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished", stage="Teardown")

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
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

    def create_capella_bucket(self):
        # creating bucket for this test
        bucket_name = self.cbas_util.generate_name()
        storage_type = self.input.param("storage_type", "couchstore")
        response = self.capellaAPI.cluster_ops_apis.create_bucket(self.tenant.id, self.tenant.project_id,
                                                                  self.remote_cluster.id, bucket_name, "couchbase",
                                                                  storage_type, 2000, "seqno",
                                                                  "majorityAndPersistActive", 0, True, 1000000)

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
            columnar_count = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, datasets[i].full_name)[0]
            kv_count = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, remote_dataset.full_name)[0]
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
            columnar_count = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, datasets[i].full_name)[0]
            kv_count = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, remote_dataset.full_name)[0]
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
            columnar_count = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, datasets[i].full_name)[0]
            kv_count = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, remote_dataset.full_name)[0]
            if columnar_count != kv_count:
                self.log.error("Doc count mismatch in KV and columnar {0}, {1}, expected: {2} got: {3}".
                               format(provisioned_collections[i], datasets[i].full_name,
                                      columnar_count, kv_count))
                results.append(columnar_count != kv_count)
            if not all(results):
                self.fail("Mismatch found in Copy To KV")

    def test_copy_to_kv_while_scaling(self):
        self.base_infra_setup()
        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]
        datasets = self.cbas_util.get_all_dataset_objs("external")
        jobs = Queue()
        results = []
        self.provisioned_bucket_id = self.create_capella_bucket()
        self.provisioned_scope = self.create_capella_scope(self.provisioned_bucket_id)
        provisioned_collections = []
        start_time = time.time()
        self.capellaAPI.update_specs(self.user.org_id, self.project.project_id, self.remote_cluster_id, self.specs)

        for dataset in datasets:
            collection = "{}.{}.{}".format(self.provisioned_bucket_id, self.provisioned_scope,
                                           self.create_capella_collection(self.provisioned_bucket_id,
                                                                          self.provisioned_scope))
            provisioned_collections.append(collection)
            jobs.put((self.cbas_util.copy_to_kv,
                      {"cluster": self.cluster, "collection_name": dataset.name, "database_name": dataset.database_name,
                       "dataverse_name": dataset.dataverse_name, "dest_bucket": collection,
                       "link_name": remote_link.full_name}))

        self.cbas_util.run_jobs_in_parallel(jobs, results, self.sdk_clients_per_user, async_run=True)
        jobs.join()
        status = None
        while status != "healthy" and start_time + 15 > time.time():
            status = self.capellaAPI.get_cluster_status(self.remote_cluster_id)

        if status is None or status != "healthy":
            self.fail("Fail to scale cluster while copy to kv")
