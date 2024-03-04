"""
Created on 4-March-2024

@author: abhay.aggrawal@couchbase.com
"""
import random
import time
from queue import Queue
from Columnar.columnar_base import ColumnarBaseTest
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI


class CopyToKv(ColumnarBaseTest):
    def setUp(self):
        super(CopyToKv, self).setUp()
        self.cluster = self.project.instances[0]
        self.aws_access_key = self.input.param("aws_access_key")
        self.aws_secret_key = self.input.param("aws_secret_key")
        self.aws_session_token = self.input.param("aws_session_token", "")
        self.doc_loader_url = self.input.param("sirius_url", None)
        self.doc_loader_port = self.input.param("sirius_port", None)
        self.no_of_docs = self.input.param("no_of_docs", 1000)
        self.capellaAPI = CapellaAPI(self.pod.url_public, '', '', self.user.email, self.user.password, '')

        # if none provided use 1 Kb doc size
        self.doc_size = self.input.param("doc_size", 1000)

        if not self.columnar_spec_name:
            self.columnar_spec_name = "regressions.copy_to_s3"

        self.columnar_spec = self.cbas_util.get_columnar_spec(
            self.columnar_spec_name)

        self.aws_region = "ap-south-1"
        self.aws_bucket_name = "columnar-sanity-test-data"

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp().__name__)

    def tearDown(self):
        """
        Delete all the analytics link and columnar instance
        """
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.cluster, self.columnar_spec):
            self.fail("Error while deleting cbas entities")

        super(ColumnarBaseTest, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished", stage="Teardown")

    def base_infra_setup(self):
        self.columnar_spec["dataverse"]["no_of_dataverses"] = self.input.param(
            "no_of_scopes", 1)

        self.columnar_spec["remote_link"]["no_of_remote_links"] = self.input.param(
            "no_of_remote_links", 0)

        if self.input.param("no_of_remote_links", 1):
            remote_link_properties = list()
            remote_link_properties.append(
                {"type": "couchbase", "hostname": self.remote_cluster_connection_string,
                 "username": self.remote_cluster_username,
                 "password": self.remote_cluster_password,
                 "encryption": "full",
                 "certificate": self.remote_cluster_certificate})
            self.columnar_spec["remote_link"]["properties"] = remote_link_properties
            self.include_external_collections = dict()
            remote_collection = [self.remote_collection]
            self.include_external_collections["remote"] = remote_collection
            self.columnar_spec["remote_dataset"]["include_collections"] = remote_collection
            self.columnar_spec["remote_dataset"]["num_of_remote_datasets"] = self.input.param("num_of_remote_coll", 1)

        self.columnar_spec["external_link"][
            "no_of_external_links"] = self.input.param(
            "no_of_external_links", 1)
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
        response = self.capellaAPI.cluster_ops_apis.create_cluster(self.user.org_id, self.project.project_id,
                                                                   self.remote_cluster_id, bucket_name, "couchbase",
                                                                   "couchstore", 2000, "seqno",
                                                                   "majorityAndPersistActive", 0, True, 1000000)

        if response.status_code == 201:
            self.log.info("Bucket created successfully")
            return response.json()["id"]
        else:
            self.fail("Error creating bucket in remote_cluster")

    def create_capella_scope(self, bucket_id, scope_name=None):
        if not scope_name:
            scope_name = self.cbas_util.generate_name()

        resp = self.capellaAPI.cluster_ops_apis.create_scope(self.user.org_id, self.project.project_id, self.remote_cluster_id,
                                                      bucket_id, scope_name)

        if resp.status_code == 201:
            self.log.info("Created scope {} on bucket {}".format(scope_name, bucket_id))
            return resp.json()['id']
        else:
            self.fail("Failed to create capella provisined scope")

    def create_capella_collection(self, bucket_id, scope_id, collection_name=None):
        if not collection_name:
            collection_name = self.cbas_util.generate_name()
        resp = self.capellaAPI.cluster_ops_apis.create_collection(self.user.org_id, self.project.project_id,
                                                                  clusterId=self.remote_cluster_id, bucketId=bucket_id,
                                                                  scopeName=scope_id, name=collection_name)

        if resp.status_code == 201:
            self.log.info("Create collection {} in scope {}".format(collection_name, scope_id))
            return resp.json()['id']


    def test_create_copyToKv_from_standalone_collection(self):
        self.base_infra_setup()
        datasets = self.cbas_util.list_all_dataset_objs("standalone")
        remote_link = self.cbas_util.list_all_link_objs("couchbase")[0]
        jobs = Queue()
        results = []
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name":dataset.database_name,
                       "no_of_docs": self.no_of_docs, "document_size": self.doc_size}))

        self.cbas_util.run_jobs_in_parallel(jobs, results, self.sdk_clients_per_user ,async_run=False)

        self.provisioned_bucket_id = self.create_capella_bucket()
        self.provisioned_scope = self.create_capella_scope(self.provisioned_bucket_id)
        provisioned_collections = []
        for dataset in datasets:
            collection = "{}.{}.{}".format(self.provisioned_bucket_id, self.provisioned_scope,
                                                             self.create_capella_collection(self.provisioned_bucket_id,
                                                                          self.provisioned_scope))
            provisioned_collections.append(collection)
            jobs.put((self.cbas_util.copy_to_kv,
                      {"cluster": self.cluster, "collection_name": dataset.name, "database_name": dataset.database_name,
                       "dataverse_name": dataset.dataverse_name, "dest_bucket": collection,
                       "link_name":remote_link.full_name}))

        self.cbas_util.run_jobs_in_parallel(jobs, results, self.sdk_clients_per_user)

        # validate the copied data
        for i in range(len(datasets)):
            remote_dataset = self.cbas_util.create_remote_dataset_obj(self.provisioned_bucket_id, self.scope_name,
                                                                      provisioned_collections[i], remote_link,
                                                                      capella=True)
            if not self.cbas_util.create_remote_dataset(self.cluster, remote_dataset.name, remote_dataset.full_kv_entity_name,
                                                        remote_dataset.link_name, dataverse_name=remote_dataset.dataverse_name,
                                                        database_name=remote_dataset.database_name):
                self.log.error("Failed to create remote dataset on KV")
                results.append(False)
            # validate doc count at columnar and KV side
            columnar_count = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, datasets[i].full_name)[0]
            kv_count = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, remote_dataset.full_name)[0]
            if columnar_count != kv_count:
                self.log.error("Doc count mismatch in KV and columnar {}, {}, expected: {0} got: {1}".
                               format(provisioned_collections[i], datasets[i].full_name,
                                      columnar_count, kv_count))
                results.append(columnar_count != kv_count)
            if not all(results):
                self.fail("Mismatch found in Copy To KV")

