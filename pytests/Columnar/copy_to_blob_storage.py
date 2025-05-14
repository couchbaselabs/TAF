"""
Created on 3-January-2024

@author: abhay.aggrawal@couchbase.com
"""
import math
import json
import os.path
import os
import random
import time
from queue import Queue

from capellaAPI.capella.columnar.CapellaAPI import CapellaAPI as ColumnarAPI
from CbasLib.CBASOperations import CBASHelper
from itertools import combinations, product
from gcs import GCS

from awsLib.s3_data_helper import perform_S3_operation
from Columnar.mini_volume_code_template import MiniVolume
from sirius_client_framework.sirius_constants import SiriusCodes
from couchbase_utils.kafka_util.confluent_utils import ConfluentUtils
from couchbase_utils.kafka_util.kafka_connect_util import KafkaConnectUtil
from Jython_tasks.sirius_task import MongoUtil, CouchbaseUtil
from TestInput import TestInputSingleton
runtype = TestInputSingleton.input.param("runtype", "default").lower()
if runtype == "columnar":
    from Columnar.columnar_base import ColumnarBaseTest
else:
    from Columnar.onprem.columnar_onprem_base import ColumnarOnPremBase as ColumnarBaseTest

def pairs(*lists):
    for t in combinations(lists, 2):
        for pair in product(*t):
            yield pair


gcs_certificate = os.getenv('gcp_access_file')

class CopyToBlobStorage(ColumnarBaseTest):
    def __init__(self, methodName: str = "runTest"):
        super().__init__(methodName)
        self.pod = None
        self.tenant = None
        self.no_of_docs = None

    def setUp(self):
        super(CopyToBlobStorage, self).setUp()
        self.remote_cluster = None
        if runtype == "columnar":
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
        self.sink_blob_bucket_name = None
        self.link_type = self.input.param("external_link_source", "s3")
        self.gcs_client = None
        if self.link_type == "gcs":
            with open(gcs_certificate, 'r') as file:
                # Load JSON data from file
                credentials = json.load(file)
                self.gcs_client = GCS(credentials)

        for i in range(5):
            try:
                if self.link_type == "s3":
                    self.sink_blob_bucket_name = "copy-to-blob-" + str(random.randint(1, 100000))
                    self.log.info("Creating S3 bucket for : {}".format(self.sink_blob_bucket_name))
                    self.sink_bucket_created = perform_S3_operation(
                        endpoint_url=self.aws_endpoint,
                        aws_access_key=self.aws_access_key,
                        aws_secret_key=self.aws_secret_key,
                        aws_session_token=self.aws_session_token,
                        create_bucket=True, bucket_name=self.sink_blob_bucket_name,
                        region=self.aws_region)
                    break
                elif self.link_type == "gcs":
                    self.sink_blob_bucket_name = "copy-to-gcs-" + str(random.randint(1, 100000))
                    self.log.info("Creating gcs bucket : {}".format(self.sink_blob_bucket_name))
                    if self.gcs_client.create_bucket(self.sink_blob_bucket_name):
                        break

            except Exception as e:
                self.log.error("Creating blob storage bucket - {0} failed".format(self.sink_blob_bucket_name))
                self.log.error(str(e))
            finally:
                if i == 4:
                    self.sink_blob_bucket_name = None
                    self.fail("Unable to create blob storage bucket even after 5 retries")

        if not self.columnar_spec_name:
            self.columnar_spec_name = "full_template"
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def base_setup(self):

        # populate spec file for the entities to be created
        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=self.remote_cluster,
            external_collection_file_formats=["json", "parquet", "csv"])

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

    def delete_blob_bucket(self):
        """
         Delete blob storage bucket created for copying files
        """
        for i in range(5):
            try:
                if self.link_type == "s3":
                    self.log.info("Emptying S3 bucket - {0}".format(self.sink_blob_bucket_name))
                    if perform_S3_operation(
                            aws_access_key=self.aws_access_key,
                            aws_secret_key=self.aws_secret_key,
                            aws_session_token=self.aws_session_token,
                            empty_bucket=True,
                            bucket_name=self.sink_blob_bucket_name,
                            region=self.aws_region,
                            endpoint_url=self.aws_endpoint):
                        break
                else:
                    if self.gcs_client.empty_gcs_bucket(self.sink_blob_bucket_name):
                        break

            except Exception as e:
                self.log.error("Unable to empty bucket - {0}".format(
                    self.sink_blob_bucket_name))
                self.log.error(str(e))
            finally:
                if i == 4:
                    self.fail("Unable to empty blob bucket even after 5 "
                              "retries")

        # Delete the created blob storage bucket
        self.log.info("Deleting blob bucket - {0}".format(
            self.sink_blob_bucket_name))

        for i in range(5):
            try:
                if self.link_type == "s3":
                    if perform_S3_operation(
                            aws_access_key=self.aws_access_key,
                            aws_secret_key=self.aws_secret_key,
                            aws_session_token=self.aws_session_token,
                            delete_bucket=True,
                            bucket_name=self.sink_blob_bucket_name,
                            region=self.aws_region,
                            endpoint_url=self.aws_endpoint):
                        break
                elif self.link_type == "gcs":
                    if self.gcs_client.delete_bucket(self.sink_blob_bucket_name):
                        break
            except Exception as e:
                self.log.error("Unable to delete blob bucket - {0}".format(
                    self.sink_blob_bucket_name))
                self.log.error(str(e))
            finally:
                if i == 4:
                    self.fail("Unable to delete blob bucket even after 5 "
                              "retries")

    def tearDown(self):
        """
        Delete all the analytics link and columnar instance
        """
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        if hasattr(self, "columnar_spec"):
            if not self.cbas_util.delete_cbas_infra_created_from_spec(
                    self.columnar_cluster, self.columnar_spec):
                self.fail("Error while deleting cbas entities")

        if hasattr(self, "remote_cluster") and self.remote_cluster:
            self.delete_all_buckets_from_capella_cluster(
                self.tenant, self.remote_cluster)

        if self.sink_blob_bucket_name:
                self.delete_blob_bucket()

        super(CopyToBlobStorage, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished", stage="Teardown")

    def create_external_dataset(self, dataset_obj, csv_type=None):
        if self.cbas_util.create_dataset_on_external_resource(
                self.columnar_cluster, dataset_obj.name,
                dataset_obj.dataset_properties[
                    "external_container_name"],
                dataset_obj.link_name, False,
                dataset_obj.dataverse_name,
                dataset_obj.database_name,
                dataset_obj.dataset_properties[
                    "object_construction_def"],
                dataset_obj.dataset_properties[
                    "path_on_external_container"],
                dataset_obj.dataset_properties[
                    "file_format"],
                dataset_obj.dataset_properties[
                    "redact_warning"],
                dataset_obj.dataset_properties[
                    "header"],
                dataset_obj.dataset_properties[
                    "null_string"],
                dataset_obj.dataset_properties[
                    "include"],
                dataset_obj.dataset_properties[
                    "exclude"],
                dataset_obj.dataset_properties[
                    "parse_json_string"],
                dataset_obj.dataset_properties[
                    "convert_decimal_to_double"],
                dataset_obj.dataset_properties[
                    "timezone"],
                False, None, None, None, None,
                timeout=300,
                analytics_timeout=300,
                csv_type=csv_type):
            return True
        return False

    def test_create_copyTo_from_standalone_dataset_query_drop_standalone_collection(self):
        # create columnar entities to operate on
        self.base_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        blob_storage_link = self.cbas_util.get_all_link_objs(self.link_type)[0]
        jobs = Queue()
        results = []
        self.log.info("Adding {} documents in standalone dataset. Default doc size is 1KB".format(self.no_of_docs))
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name,
                       "no_of_docs": self.no_of_docs}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Some documents were not inserted")

        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.columnar_cluster, "collection_name": datasets[i].name,
                       "dataverse_name": datasets[i].dataverse_name, "database_name": datasets[i].database_name,
                       "destination_bucket": self.sink_blob_bucket_name, "destination_link_name": blob_storage_link.full_name,
                       "path": path}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.fail("Failed to execute copy to statement")

        path_on_external_container = "{copy_dataset:string}"
        # create external dataset on the blob storage bucket
        dataset_obj = self.cbas_util.create_external_dataset_obj(
            self.columnar_cluster, link_type=self.link_type,
            external_container_names={
                self.sink_blob_bucket_name: self.aws_region},
            paths_on_external_container=[path_on_external_container],
            file_format="json")[0]

        if not self.create_external_dataset(dataset_obj):
            self.fail("Failed to create external dataset on destination blob storage bucket")

        # Verify the data in dataset and blob storage
        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            statement = "select count(*) from {0} where copy_dataset = \"{1}\"".format(dataset_obj.full_name, path)
            status, metrics, errors, result, _, _ = self.cbas_util.execute_statement_on_cbas_util(self.columnar_cluster,
                                                                                                  statement)
            doc_count_in_dataset = self.cbas_util.get_num_items_in_cbas_dataset(self.columnar_cluster,
                                                                                datasets[i].full_name)
            if result[0]['$1'] != doc_count_in_dataset:
                self.log.error("Document count mismatch in blob storage dataset {0} and dataset {1}".format(
                    dataset_obj.full_name, datasets[i].full_name
                ))
            results.append(result[0]['$1'] == doc_count_in_dataset)
        if not all(results):
            self.fail("The document count does not match in dataset and blob storage")

    def test_create_copyTo_from_remote_collection_query_drop_standalone_collection(self):
        self.create_bucket_scopes_collections_in_capella_cluster(
            self.tenant, self.remote_cluster,
            self.input.param("num_buckets", 1))
        # create columnar entities to operate on
        self.base_setup()
        remote_bucket = self.remote_cluster.buckets[0]
        self.cbas_util.doc_operations_remote_collection_sirius(self.task_manager, "_default", remote_bucket.name,
                                                               "_default", "couchbases://" + self.remote_cluster.srv,
                                                               0, self.no_of_docs, doc_size=self.doc_size,
                                                               username=self.remote_cluster.username,
                                                               password=self.remote_cluster.password,
                                                               template="hotel")
        datasets = self.cbas_util.get_all_dataset_objs("remote")
        self.cbas_util.wait_for_data_ingestion_in_the_collections(self.columnar_cluster)
        blob_storage_link = self.cbas_util.get_all_link_objs(self.link_type)[0]
        jobs = Queue()
        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.columnar_cluster, "collection_name": datasets[i].name,
                       "dataverse_name": datasets[i].dataverse_name, "database_name": datasets[i].database_name,
                       "destination_bucket": self.sink_blob_bucket_name, "destination_link_name": blob_storage_link.full_name,
                       "path": path}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.fail("Copy to blob storage statement failure")

        path_on_external_container = "{copy_dataset:string}"
        dataset_obj = self.cbas_util.create_external_dataset_obj(self.columnar_cluster, link_type=self.link_type,
                                                                 external_container_names={
                                                                     self.sink_blob_bucket_name: self.aws_region},
                                                                 paths_on_external_container=[
                                                                     path_on_external_container],
                                                                 file_format="json")[0]
        if not self.create_external_dataset(dataset_obj):
            self.fail("Failed to create external dataset on destination blob storage bucket")

        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            statement = "select count(*) from {0} where copy_dataset = \"{1}\"".format(dataset_obj.full_name, path)
            status, metrics, errors, result, _, _ = self.cbas_util.execute_statement_on_cbas_util(self.columnar_cluster,
                                                                                                  statement)
            doc_count_in_dataset = self.cbas_util.get_num_items_in_cbas_dataset(self.columnar_cluster,
                                                                                datasets[i].full_name)
            if result[0]['$1'] != doc_count_in_dataset:
                self.log.error("Document count mismatch in blob storage dataset {0} and remote dataset {1}".format(
                    dataset_obj.full_name, datasets[i].full_name
                ))
            results.append(result[0]['$1'] == doc_count_in_dataset)

        if not all(results):
            self.fail("The document count does not match in remote source and blob storage")

    def test_create_copyTo_from_external_collection_query_drop_standalone_collection(self):
        self.base_setup()
        external_datasets = self.cbas_util.get_all_dataset_objs("external")
        blob_storage_link = self.cbas_util.get_all_link_objs(self.link_type)[0]
        jobs = Queue()
        results = []
        for i in range(len(external_datasets)):
            path = "copy_dataset_" + str(i)
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.columnar_cluster, "collection_name": external_datasets[i].name,
                       "dataverse_name": external_datasets[i].dataverse_name,
                       "database_name": external_datasets[i].database_name,
                       "destination_bucket": self.sink_blob_bucket_name,
                       "destination_link_name": blob_storage_link.full_name, "path": path, "timeout": 3600,
                       "analytics_timeout": 3600}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.fail("Copy to blob storage statement failure")

        path_on_external_container = "{copy_dataset:string}"
        dataset_obj = self.cbas_util.create_external_dataset_obj(self.columnar_cluster, link_type=self.link_type,
                                                                 external_container_names={
                                                                     self.sink_blob_bucket_name: self.aws_region},
                                                                 paths_on_external_container=[
                                                                     path_on_external_container],
                                                                 file_format="json")[0]
        if not self.create_external_dataset(dataset_obj):
            self.fail("Failed to create external dataset on destination blob storage bucket")

        results = []
        for i in range(len(external_datasets)):
            path = "copy_dataset_" + str(i)
            statement = "select count(*) from {0} where copy_dataset = \"{1}\"".format(dataset_obj.full_name, path)
            status, metrics, errors, result, _, _ = self.cbas_util.execute_statement_on_cbas_util(self.columnar_cluster,
                                                                                                  statement)
            doc_count_in_dataset = self.cbas_util.get_num_items_in_cbas_dataset(self.columnar_cluster,
                                                                                external_datasets[i].full_name)
            if result[0]['$1'] != doc_count_in_dataset:
                self.log.error("Document count mismatch in blob storage dataset {0} and external dataset {1}".format(
                    dataset_obj.full_name, external_datasets[i].full_name
                ))
            results.append(result[0]['$1'] == doc_count_in_dataset)

        if not all(results):
            self.fail("The document count does not match in external source and blob storage")

    def test_create_copyTo_from_multiple_collection_query_drop_standalone_collection(self):

        self.create_bucket_scopes_collections_in_capella_cluster(
            self.tenant, self.remote_cluster,
            self.input.param("num_buckets", 1))
        # create columnar entities to operate on
        self.base_setup()
        remote_bucket = self.remote_cluster.buckets[0]
        self.cbas_util.doc_operations_remote_collection_sirius(self.task_manager, "_default", remote_bucket.name,
                                                               "_default", "couchbases://" + self.remote_cluster.srv,
                                                               0, self.no_of_docs, doc_size=self.doc_size,
                                                               username=self.remote_cluster.username,
                                                               password=self.remote_cluster.password,
                                                               template="hotel")
        remote_dataset = self.cbas_util.get_all_dataset_objs("remote")
        external_dataset = self.cbas_util.get_all_dataset_objs("external")
        standalone_dataset = self.cbas_util.get_all_dataset_objs("standalone")
        unique_pairs = []
        for pair in pairs(remote_dataset, external_dataset, standalone_dataset):
            unique_pairs.append(pair)
        blob_storage_link = self.cbas_util.get_all_link_objs(self.link_type)[0]
        jobs = Queue()
        results = []
        for dataset in standalone_dataset:
            if dataset.data_source is None:
                jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                          {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                           "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name,
                           "no_of_docs": self.doc_size}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        results = []
        for i in range(len(unique_pairs)):
            path = "copy_dataset_" + str(i)
            statement = ("select * from {0} as a, {1} as b where a.avg_rating > 0.4 "
                         "and b.avg_rating > 0.4 limit 1000").format(
                (unique_pairs[i][0]).full_name, (unique_pairs[i][1]).full_name
            )
            jobs.put((
                self.cbas_util.copy_to_s3,
                {"cluster": self.columnar_cluster, "source_definition_query": statement, "alias_identifier": "ally",
                 "destination_bucket": self.sink_blob_bucket_name,
                 "destination_link_name": blob_storage_link.full_name, "path": path, "timeout": 600, "analytics_timeout": 600}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.log.error("Copy to blob storage statement failure")

        path_on_external_container = "{copy_dataset:string}"
        dataset_obj = self.cbas_util.create_external_dataset_obj(self.columnar_cluster, link_type=self.link_type,
                                                                 external_container_names={
                                                                     self.sink_blob_bucket_name: self.aws_region},
                                                                 paths_on_external_container=[
                                                                     path_on_external_container],
                                                                 file_format="json")[0]
        if not self.create_external_dataset(dataset_obj):
            self.fail("Failed to create external dataset on destination blob storage bucket")

        for i in range(len(unique_pairs)):
            path = "copy_dataset_" + str(i)

            statement = "select count(*) from {0} where copy_dataset = \"{1}\"".format(dataset_obj.full_name, path)
            status, metrics, errors, result1, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, statement)

            if 1000 != result1[0]['$1']:
                self.log.error("Document count mismatch in blob storage dataset {0}".format(
                    dataset_obj.full_name
                ))
            results.append(1000 == result1[0]['$1'])

        if not all(results):
            self.fail("The document count does not match in remote source and blob storage")

    def test_create_copyTo_from_collection_with_different_key_type_query_drop_standalone_collection(self):
        # create columnar entities to operate on
        self.base_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        blob_storage_link = self.cbas_util.get_all_link_objs(self.link_type)[0]
        no_of_docs = self.input.param("no_of_docs", 1000)
        jobs = Queue()
        results = []
        for i in range(len(datasets)):
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": datasets[i].name,
                       "dataverse_name": datasets[i].dataverse_name, "database_name": datasets[i].database_name,
                       "no_of_docs": no_of_docs, "country_type": "mixed"}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.log.error("Not all docs were inserted")
        results = []

        items, copy_to_type, create_dataset_type = None, None, None

        if self.columnar_spec["file_format"] == "csv":
            items, copy_to_type, create_dataset_type = self.cbas_util.generate_type_for_copy_to_cmd_csv_format(
                collection_name=datasets[0].full_name,
                cluster=self.columnar_cluster,
            )
        copy_string = False
        if copy_to_type and "country: string" in copy_to_type:
            copy_string = True

        # initiate copy command
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.columnar_cluster, "collection_name": datasets[i].name,
                       "alias_identifier": "ally",
                       "dataverse_name": datasets[i].dataverse_name, "database_name": datasets[i].database_name,
                       "destination_bucket": self.sink_blob_bucket_name,
                       "destination_link_name": blob_storage_link.full_name, "path": path, "partition_alias": "country",
                       "partition_by": "ally.country", "file_format": self.columnar_spec["file_format"],
                       "copy_to_type": copy_to_type, "items": items}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.fail("Copy to blob storage statement failure")

        # create dataset based on dynamic prefixes
        path_on_external_container_int = "{copy_dataset:string}/{country:int}"
        dataset_obj_int = self.cbas_util.create_external_dataset_obj(self.columnar_cluster, link_type=self.link_type,
                                                                     external_container_names={
                                                                         self.sink_blob_bucket_name: self.aws_region},
                                                                     paths_on_external_container=[
                                                                         path_on_external_container_int],
                                                                     file_format=self.columnar_spec["file_format"])[0]
        if not self.create_external_dataset(dataset_obj_int, csv_type=create_dataset_type):
            self.fail("Failed to create external dataset on destination blob storage bucket")

        path_on_external_container_string = "{copy_dataset:string}/{country:string}"
        dataset_obj_string = self.cbas_util.create_external_dataset_obj(
            self.columnar_cluster, link_type=self.link_type,
            external_container_names={self.sink_blob_bucket_name: self.aws_region},
            paths_on_external_container=[path_on_external_container_string],
            file_format=self.columnar_spec["file_format"])[0]
        if not self.create_external_dataset(dataset_obj_string, csv_type=create_dataset_type):
            self.fail("Failed to create external dataset on destination blob storage bucket")

        # verification step
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            for j in range(5):
                statement = "select * from {0} limit 1".format(datasets[i].full_name)
                status, metrics, errors, result, _, _ \
                    = self.cbas_util.execute_statement_on_cbas_util(self.columnar_cluster, statement)
                country_name = (result[0])[CBASHelper.unformat_name(datasets[i].name)]["country"]

                if isinstance(country_name, int):
                    if copy_string:
                        continue
                    statement = "select count(*) from {0} where country = {1}".format(datasets[i].full_name,
                                                                                      country_name)
                    dynamic_statement = ("select count(*) from {0} where copy_dataset = \"{1}\" and "
                                         "country = {2}").format(dataset_obj_int.full_name, path, country_name)
                    status, metrics, errors, result, _, _ \
                        = self.cbas_util.execute_statement_on_cbas_util(self.columnar_cluster, dynamic_statement)
                    result_val = result[0]['$1']
                else:
                    if not copy_string:
                        continue
                    statement = "select count(*) from {0} where country = \"{1}\"".format(datasets[i].full_name,
                                                                                          str(country_name))
                    dynamic_statement = ("select count(*) from {0} where copy_dataset = \"{1}\" "
                                         "and country = \"{2}\"").format(
                        dataset_obj_string.full_name, path, str(country_name)
                    )
                    status, metrics, errors, result, _, _ \
                        = self.cbas_util.execute_statement_on_cbas_util(self.columnar_cluster, dynamic_statement)
                    result_val = result[0]['$1']
                status, metrics, errors, result, _, _ \
                    = self.cbas_util.execute_statement_on_cbas_util(self.columnar_cluster, statement)
                dataset_result = result[0]['$1']
                if dataset_result != result_val:
                    self.fail("Document Count Mismatch")

    def test_create_copyTo_from_collection_with_gzip_compression_drop_standalone_collection(self):
        self.base_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        blob_storage_link = self.cbas_util.get_all_link_objs(self.link_type)[0]
        no_of_docs = self.input.param("no_of_docs", 1000)
        jobs = Queue()
        results = []
        for dataset in datasets:
            if dataset.data_source is None:
                jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                          {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                           "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name,
                           "no_of_docs": no_of_docs}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.log.error("Some documents were not inserted")

        items, copy_to_type, create_dataset_type = None, None, None

        if self.columnar_spec["file_format"] == "csv":
            items, copy_to_type, create_dataset_type = self.cbas_util.generate_type_for_copy_to_cmd_csv_format(
                collection_name=datasets[0].full_name,
                cluster=self.columnar_cluster,
            )

        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.columnar_cluster, "collection_name": datasets[i].name,
                       "alias_identifier": "ally",
                       "dataverse_name": datasets[i].dataverse_name,
                       "database_name": datasets[i].database_name,
                       "destination_bucket": self.sink_blob_bucket_name,
                       "destination_link_name": blob_storage_link.full_name, "path": path, "timeout": 3600,
                       "analytics_timeout": 3600, "compression": "gzip",
                       "file_format": self.columnar_spec["file_format"],
                       "copy_to_type": copy_to_type, "items": items}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.fail("Copy to command failed for some datasets")

        path_on_external_container = "{copy_dataset:string}"
        dataset_obj = self.cbas_util.create_external_dataset_obj(self.columnar_cluster, link_type=self.link_type,
                                                                 external_container_names={
                                                                     self.sink_blob_bucket_name: self.aws_region},
                                                                 paths_on_external_container=[
                                                                     path_on_external_container],
                                                                 file_format=self.columnar_spec["file_format"])[0]
        if not self.create_external_dataset(dataset_obj, csv_type=create_dataset_type):
            self.fail("Failed to create external dataset on destination blob storage bucket")

        files = []
        if self.link_type == "s3":
            files = perform_S3_operation(aws_access_key=self.aws_access_key,
                                         aws_secret_key=self.aws_secret_key,
                                         region=self.aws_region,
                                         aws_session_token=self.aws_session_token,
                                         bucket_name=self.sink_blob_bucket_name,
                                         endpoint_url=self.aws_endpoint,
                                         get_bucket_objects=True)
        elif self.link_type == "gcs":
            files = self.gcs_client.list_objects_in_gcs_bucket(self.sink_blob_bucket_name)

        objects_in_blob_storage = [str(x) for x in files]

        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            for obj in objects_in_blob_storage:
                if not obj.endswith('.gzip'):
                    self.fail("Not all files are gzip")
            statement = "select count(*) from {0} where copy_dataset = \"{1}\"".format(dataset_obj.full_name, path)
            status, metrics, errors, result, _, _ \
                = self.cbas_util.execute_statement_on_cbas_util(self.columnar_cluster, statement)
            doc_count_in_dataset = self.cbas_util.get_num_items_in_cbas_dataset(self.columnar_cluster,
                                                                                datasets[i].full_name)
            if result[0]['$1'] != doc_count_in_dataset:
                self.log.error("Document count mismatch in blob storage dataset {0} and remote dataset {1}".format(
                    dataset_obj.full_name, datasets[i].full_name
                ))
            results.append(result[0]['$1'] == doc_count_in_dataset)

        if not all(results):
            self.fail("The document count does not match in remote source and blob storage")

    def test_create_copyTo_from_collection_with_max_object_compression_drop_standalone_collection(self):
        self.base_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        blob_storage_link = self.cbas_util.get_all_link_objs(self.link_type)[0]
        max_object_per_file = self.input.param("max_object_per_file", 100)
        no_of_docs = self.input.param("no_of_docs", 10000)
        jobs = Queue()
        results = []
        for dataset in datasets:
            if dataset.data_source is None:
                jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                          {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                           "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name,
                           "no_of_docs": no_of_docs}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.log.error("Some documents were not inserted")

        items, copy_to_type, create_dataset_type = None, None, None

        if self.columnar_spec["file_format"] == "csv":
            items, copy_to_type, create_dataset_type = self.cbas_util.generate_type_for_copy_to_cmd_csv_format(
                collection_name=datasets[0].full_name,
                cluster=self.columnar_cluster,
            )
        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.columnar_cluster, "collection_name": datasets[i].name,
                       "alias_identifier": "ally",
                       "dataverse_name": datasets[i].dataverse_name,
                       "database_name": datasets[i].database_name,
                       "destination_bucket": self.sink_blob_bucket_name,
                       "destination_link_name": blob_storage_link.full_name, "path": path, "timeout": 3600,
                       "analytics_timeout": 3600, "compression": "gzip", "max_object_per_file": max_object_per_file,
                       "file_format": self.columnar_spec["file_format"],
                       "copy_to_type": copy_to_type, "items": items}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.fail("Copy to command failed for some datasets")

        path_on_external_container = "{copy_dataset:string}"
        dataset_obj = self.cbas_util.create_external_dataset_obj(self.columnar_cluster, link_type=self.link_type,
                                                                 external_container_names={
                                                                     self.sink_blob_bucket_name: self.aws_region},
                                                                 paths_on_external_container=[
                                                                     path_on_external_container],
                                                                 file_format=self.columnar_spec["file_format"])[0]
        if not self.create_external_dataset(dataset_obj, csv_type=create_dataset_type):
            self.fail("Failed to create external dataset on destination blob storage bucket")

        files = []
        if self.link_type == "s3":
            files = perform_S3_operation(aws_access_key=self.aws_access_key,
                                     aws_secret_key=self.aws_secret_key,
                                     region=self.aws_region,
                                     aws_session_token=self.aws_session_token,
                                     bucket_name=self.sink_blob_bucket_name,
                                     endpoint_url=self.aws_endpoint,
                                     get_bucket_objects=True)
        elif self.link_type == "gcs":
            files = self.gcs_client.list_objects_in_gcs_bucket(self.sink_blob_bucket_name)

        objects_in_blob_storage = [str(x) for x in files]

        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            no_of_files_at_path = [x for x in objects_in_blob_storage if x.startswith(path)]
            doc_count_in_dataset = self.cbas_util.get_num_items_in_cbas_dataset(
                self.columnar_cluster, datasets[i].full_name)
            if len(no_of_files_at_path) < math.ceil(doc_count_in_dataset / max_object_per_file):
                self.fail("Number of files expected: {0}, actual: {1}".format(
                    math.ceil(doc_count_in_dataset / max_object_per_file),
                    len(no_of_files_at_path)))
            statement = "select count(*) from {0} where copy_dataset = \"{1}\"".format(dataset_obj.full_name, path)
            status, metrics, errors, result, _, _ \
                = self.cbas_util.execute_statement_on_cbas_util(self.columnar_cluster, statement)
            if result[0]['$1'] != doc_count_in_dataset:
                self.log.error("Document count mismatch in blob storage dataset {0} and remote dataset {1}".format(
                    dataset_obj.full_name, datasets[i].full_name
                ))
            results.append(result[0]['$1'] == doc_count_in_dataset)

        if not all(results):
            self.fail("The document count does not match in remote source and blob storage")

    def test_create_copyTo_from_collection_to_non_empty_blob_bucket(self):
        self.base_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        blob_storage_link = self.cbas_util.get_all_link_objs(self.link_type)[0]
        no_of_docs = self.input.param("no_of_docs", 10000)
        jobs = Queue()
        results = []
        self.log.info("Adding {} documents in standalone dataset. Default doc size is 1KB".format(no_of_docs))
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name,
                       "no_of_docs": no_of_docs}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Some documents were not inserted")

        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.columnar_cluster, "collection_name": datasets[i].name,
                       "dataverse_name": datasets[i].dataverse_name,
                       "database_name": datasets[i].database_name,
                       "destination_bucket": self.sink_blob_bucket_name,
                       "destination_link_name": blob_storage_link.full_name, "path": path}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Failed to execute copy to statement")

        path_on_external_container = "{copy_dataset:string}"
        # create external dataset on the blob storage bucket
        dataset_obj = self.cbas_util.create_external_dataset_obj(self.columnar_cluster, link_type=self.link_type,
                                                                 external_container_names={
                                                                     self.sink_blob_bucket_name: self.aws_region},
                                                                 paths_on_external_container=[
                                                                     path_on_external_container],
                                                                 file_format="json")[0]

        if not self.create_external_dataset(dataset_obj):
            self.fail("Failed to create external dataset on destination blob bucket")

        # Verify the data in dataset and blob storage
        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            statement = "select count(*) from {0} where copy_dataset = \"{1}\"".format(dataset_obj.full_name, path)
            status, metrics, errors, result, _, _ \
                = self.cbas_util.execute_statement_on_cbas_util(self.columnar_cluster, statement)
            doc_count_in_dataset = self.cbas_util.get_num_items_in_cbas_dataset(self.columnar_cluster,
                                                                                datasets[i].full_name)
            if result[0]['$1'] != doc_count_in_dataset:
                self.log.error("Document count mismatch in blob storage dataset {0} and dataset {1}".format(
                    dataset_obj.full_name, datasets[i].full_name
                ))
            results.append(result[0]['$1'] == doc_count_in_dataset)
        if not all(results):
            self.fail("The document count does not match in dataset and blob storage")

        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i) + "_2"
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.columnar_cluster, "collection_name": datasets[i].name,
                       "dataverse_name": datasets[i].dataverse_name,
                       "database_name": datasets[i].database_name,
                       "destination_bucket": self.sink_blob_bucket_name,
                       "destination_link_name": blob_storage_link.full_name, "path": path}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Failed to execute copy to statement")

        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i) + "_2"
            statement = "select count(*) from {0} where copy_dataset = \"{1}\"".format(dataset_obj.full_name, path)
            status, metrics, errors, result, _, _ \
                = self.cbas_util.execute_statement_on_cbas_util(self.columnar_cluster, statement)
            doc_count_in_dataset = self.cbas_util.get_num_items_in_cbas_dataset(self.columnar_cluster,
                                                                                datasets[i].full_name)
            if result[0]['$1'] != doc_count_in_dataset:
                self.log.error("Document count mismatch in blob storage dataset {0} and dataset {1}".format(
                    dataset_obj.full_name, datasets[i].full_name
                ))
            results.append(result[0]['$1'] == doc_count_in_dataset)
        if not all(results):
            self.fail("The document count does not match in dataset and blob storage")

    def test_create_copyTo_from_collection_to_non_existing_S3_bucket(self):
        expected_error = ("External sink error. software.amazon.awssdk.services.s3.model.NoSuchBucketException: "
                          "The specified bucket does not exist")
        if self.link_type != "s3":
            self.log.info("Test only valid for S3 links")
            expected_error = ("External sink error. com.google.cloud.storage.StorageException: The specified bucket "
                              "does not exist.")
        self.base_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        blob_storage_link = self.cbas_util.get_all_link_objs(self.link_type)[0]
        jobs = Queue()

        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.columnar_cluster, "collection_name": datasets[i].name,
                       "dataverse_name": datasets[i].dataverse_name,
                       "database_name": datasets[i].database_name,
                       "destination_bucket": self.cbas_util.generate_name(),
                       "destination_link_name": blob_storage_link.full_name, "path": path, "validate_error_msg": True,
                       "expected_error": expected_error,
                       "expected_error_code": 24230}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.fail("Failed to execute copy to statement with error")

    def test_create_copyTo_from_collection_to_non_existing_directory_blob_bucket(self):
        self.base_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        blob_storage_link = self.cbas_util.get_all_link_objs(self.link_type)[0]
        no_of_docs = self.input.param("no_of_docs", 100)
        jobs = Queue()
        results = []
        self.log.info("Adding {} documents in standalone dataset. Default doc size is 1KB".format(no_of_docs))
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name,
                       "database_name": dataset.database_name, "no_of_docs": no_of_docs}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Some documents were not inserted")

        results = []
        for i in range(len(datasets)):
            path = "main/copy_dataset_" + str(i)
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.columnar_cluster, "collection_name": datasets[i].name,
                       "dataverse_name": datasets[i].dataverse_name,
                       "database_name": datasets[i].database_name,
                       "destination_bucket": self.sink_blob_bucket_name,
                       "destination_link_name": blob_storage_link.full_name, "path": path}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.fail("Failed to execute copy to statement")

        path_on_external_container = "main/{copy_dataset:string}"
        # create external dataset on the blob bucket
        dataset_obj = self.cbas_util.create_external_dataset_obj(self.columnar_cluster, link_type=self.link_type,
                                                                 external_container_names={
                                                                     self.sink_blob_bucket_name: self.aws_region},
                                                                 paths_on_external_container=[
                                                                     path_on_external_container],
                                                                 file_format="json")[0]
        if not self.create_external_dataset(dataset_obj):
            self.fail("Failed to create external dataset on destination blob storage bucket")

        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            statement = "select count(*) from {0} where copy_dataset = \"{1}\"".format(dataset_obj.full_name, path)
            status, metrics, errors, result, _, _ \
                = self.cbas_util.execute_statement_on_cbas_util(self.columnar_cluster, statement)
            doc_count_in_dataset = self.cbas_util.get_num_items_in_cbas_dataset(self.columnar_cluster,
                                                                                datasets[i].full_name)
            if result[0]['$1'] != doc_count_in_dataset:
                self.log.error("Document count mismatch in blob storage dataset {0} and dataset {1}".format(
                    dataset_obj.full_name, datasets[i].full_name
                ))
            results.append(result[0]['$1'] == doc_count_in_dataset)
        if not all(results):
            self.fail("The document count does not match in dataset and blob storage")

    def test_create_copyTo_from_dynamic_prefix_collection_query_drop_standalone_collection(self):
        self.base_setup()
        path_on_external_container = "{country:string}"
        # create external dataset on the blob storage bucket
        no_of_dynamic_collection = self.input.param("no_of_dynamic_collection", 1)

        copy_to_type = (
            "name: string, email: string, characters_with_spaces: string, "
            "characters_without_spaces: string, document_size: bigint, address: string, "
            "free_parking: boolean, city: string, url: string, phone: bigint, "
            "price: double, avg_rating: double, free_breakfast: boolean, mutated: double, "
            "padding: string, country: string"
        ) if self.columnar_spec["file_format"] == "csv" else None

        create_dataset_type = (
            "name string, email string, characters_with_spaces string, "
            "characters_without_spaces string, document_size bigint, address string, "
            "free_parking boolean, city string, url string, phone bigint, "
            "price double, avg_rating double, free_breakfast boolean, mutated double, "
            "padding string, country string"
        ) if self.columnar_spec["file_format"] == "csv" else None

        items = ("name,email,characters_with_spaces,characters_without_spaces,document_size,address,free_parking,city,"
                 "url,phone,price,avg_rating,free_breakfast,mutated,padding,country")

        for i in range(no_of_dynamic_collection):
            dataset_obj = self.cbas_util.create_external_dataset_obj(self.columnar_cluster, link_type=self.link_type,
                                                                     external_container_names={
                                                                         self.s3_source_bucket: self.aws_region},
                                                                     paths_on_external_container=[
                                                                         path_on_external_container],
                                                                     file_format=self.columnar_spec["file_format"])[0]

            if not self.create_external_dataset(dataset_obj, csv_type=create_dataset_type):
                self.fail("Failed to create external dataset on destination blob storage bucket")

        datasets = self.cbas_util.get_all_dataset_objs("external")
        blob_storage_link = self.cbas_util.get_all_link_objs(self.link_type)[0]
        jobs = Queue()
        results = []

        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            query = "select * from {0} where country = \"{1}\"".format(datasets[i].full_name, "Dominican Republic")
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.columnar_cluster, "source_definition_query": query, "alias_identifier": "ally",
                       "destination_bucket": self.sink_blob_bucket_name,
                       "destination_link_name": blob_storage_link.full_name, "path": path, "timeout": 3600,
                       "analytics_timeout": 3600,
                       "copy_to_type": copy_to_type, "items": items}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        path_on_external_container = "{copy_dataset:string}"
        dataset_obj = self.cbas_util.create_external_dataset_obj(self.columnar_cluster, link_type=self.link_type,
                                                                 external_container_names={
                                                                     self.sink_blob_bucket_name: self.aws_region},
                                                                 paths_on_external_container=[
                                                                     path_on_external_container],
                                                                 file_format=self.columnar_spec["file_format"])[0]

        if not self.create_external_dataset(dataset_obj, csv_type=create_dataset_type):
            self.fail("Failed to create external dataset on destination blob storage bucket")

        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            statement_dataset = "select count(*) from {0} where country = \"{1}\"".format(datasets[i].full_name,
                                                                                          "Dominican Republic")
            dynamic_copy_result = "select count(*) from {0} where copy_dataset = \"{1}\"".format(dataset_obj.full_name,
                                                                                                 path)
            status, metrics, errors, result, _, _ \
                = self.cbas_util.execute_statement_on_cbas_util(self.columnar_cluster, statement_dataset)
            status, metrics, errors, result1, _, _ \
                = self.cbas_util.execute_statement_on_cbas_util(self.columnar_cluster, dynamic_copy_result)

            if result[0]['$1'] != result1[0]['$1']:
                self.log.error("Document count mismatch in blob storage dataset {0} and remote dataset {1}".format(
                    dataset_obj.full_name, dataset_obj[0].full_name
                ))
            results.append(result[0]['$1'] == result1[0]['$1'])

        if not all(results):
            self.fail("The document count does not match in dataset and blob storage")

    def test_create_copyTo_from_collection_order_by_query_drop_standalone_collection(self):
        self.base_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        blob_storage_link = self.cbas_util.get_all_link_objs(self.link_type)[0]
        no_of_docs = self.input.param("no_of_docs", 10000)

        jobs = Queue()
        results = []
        self.log.info("Adding {} documents in standalone dataset. Default doc size is 1KB".format(no_of_docs))
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name,
                       "no_of_docs": no_of_docs}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Some documents were not inserted")

        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.columnar_cluster, "collection_name": datasets[i].name,
                       "dataverse_name": datasets[i].dataverse_name,
                       "database_name": datasets[i].database_name,
                       "alias_identifier": "ally",
                       "destination_bucket": self.sink_blob_bucket_name, "order_by": "ally.avg_rating DESC",
                       "destination_link_name": blob_storage_link.full_name, "path": path}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Failed to execute copy to statement")

        path_on_external_container = "{copy_dataset:string}"
        # create external dataset on the blob storage bucket
        dataset_obj = self.cbas_util.create_external_dataset_obj(self.columnar_cluster, link_type=self.link_type,
                                                                 external_container_names={
                                                                     self.sink_blob_bucket_name: self.aws_region},
                                                                 paths_on_external_container=[
                                                                     path_on_external_container],
                                                                 file_format="json")[0]

        if not self.create_external_dataset(dataset_obj):
            self.fail("Failed to create external dataset on destination blob storage bucket")

        verification_file = []
        if self.link_type == "s3":
            verification_file = [str(x) for x in perform_S3_operation(aws_access_key=self.aws_access_key,
                                                                      aws_secret_key=self.aws_secret_key,
                                                                      region=self.aws_region,
                                                                      aws_session_token=self.aws_session_token,
                                                                      bucket_name=self.sink_blob_bucket_name,
                                                                      endpoint_url=self.aws_endpoint,
                                                                      get_bucket_objects=True)]
        elif self.link_type == "gcs":
            verification_file = [str(x) for x in self.gcs_client.list_objects_in_gcs_bucket(self.sink_blob_bucket_name)]

        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            file_to_download = [x for x in verification_file if x.startswith(path)][0]
            dest_path = os.path.join(os.path.dirname(__file__), "download.json")
            if self.link_type == "s3":
                _ = perform_S3_operation(aws_access_key=self.aws_access_key,
                                         aws_secret_key=self.aws_secret_key,
                                         region=self.aws_region,
                                         aws_session_token=self.aws_session_token,
                                         bucket_name=self.sink_blob_bucket_name,
                                         endpoint_url=self.aws_endpoint,
                                         download_file=True, src_path=file_to_download,
                                         dest_path=dest_path
                                         )
            elif self.link_type == 'gcs':
                self.gcs_client.download_file_from_gcs(self.sink_blob_bucket_name, file_to_download, dest_path)

            json_data = []
            with open(dest_path, 'r') as json_file:
                # Load the JSON data from the file
                for line in json_file:
                    data = json.loads(line)
                    json_data.append(data)

            sorted_data = sorted(json_data, key=lambda x: x['avg_rating'], reverse=True)

            for dict1, dict2 in zip(json_data, sorted_data):
                if dict1 != dict2:
                    self.fail("The data is not in sorted order")

        # need to add data in file ordering checks
        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            statement = "select count(*) from {0} where copy_dataset = \"{1}\"".format(dataset_obj.full_name, path)
            status, metrics, errors, result, _, _ \
                = self.cbas_util.execute_statement_on_cbas_util(self.columnar_cluster, statement)
            doc_count_in_dataset = self.cbas_util.get_num_items_in_cbas_dataset(self.columnar_cluster,
                                                                                datasets[i].full_name)
            if result[0]['$1'] != doc_count_in_dataset:
                self.log.error("Document count mismatch in blob storage dataset {0} and dataset {1}".format(
                    dataset_obj.full_name, datasets[i].full_name
                ))
            results.append(result[0]['$1'] == doc_count_in_dataset)
        if not all(results):
            self.fail("The document count does not match in dataset and blob storage")

    def test_create_copyTo_from_collection_multiple_order_by_query_drop_standalone_collection(self):
        self.base_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        blob_storage_link = self.cbas_util.get_all_link_objs(self.link_type)[0]
        no_of_docs = self.input.param("no_of_docs", 10000)

        jobs = Queue()
        results = []
        self.log.info("Adding {} documents in standalone dataset. Default doc size is 1KB".format(no_of_docs))
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name,
                       "no_of_docs": no_of_docs}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Some documents were not inserted")

        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.columnar_cluster, "collection_name": datasets[i].name,
                       "dataverse_name": datasets[i].dataverse_name,
                       "database_name": datasets[i].database_name,
                       "alias_identifier": "ally",
                       "destination_bucket": self.sink_blob_bucket_name,
                       "order_by": "ally.avg_rating DESC, ally.country DESC",
                       "destination_link_name": blob_storage_link.full_name, "path": path}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Failed to execute copy to statement")

        path_on_external_container = "{copy_dataset:string}"
        # create external dataset on the blob storage bucket
        dataset_obj = self.cbas_util.create_external_dataset_obj(self.columnar_cluster, link_type=self.link_type,
                                                                 external_container_names={
                                                                     self.sink_blob_bucket_name: self.aws_region},
                                                                 paths_on_external_container=[
                                                                     path_on_external_container],
                                                                 file_format="json")[0]

        if not self.create_external_dataset(dataset_obj):
            self.fail("Failed to create external dataset on destination blob storage bucket")

        verification_file = []
        if self.link_type == "s3":
            verification_file = [str(x) for x in perform_S3_operation(aws_access_key=self.aws_access_key,
                                                                      aws_secret_key=self.aws_secret_key,
                                                                      region=self.aws_region,
                                                                      aws_session_token=self.aws_session_token,
                                                                      bucket_name=self.sink_blob_bucket_name,
                                                                      endpoint_url=self.aws_endpoint,
                                                                      get_bucket_objects=True)]
        elif self.link_type == "gcs":
            verification_file = [str(x) for x in self.gcs_client.list_objects_in_gcs_bucket(self.sink_blob_bucket_name)]

        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            file_to_download = [x for x in verification_file if x.startswith(path)][0]
            dest_path = os.path.join(os.path.dirname(__file__), "download.json")
            if self.link_type == "s3":
                _ = perform_S3_operation(aws_access_key=self.aws_access_key,
                                         aws_secret_key=self.aws_secret_key,
                                         region=self.aws_region,
                                         aws_session_token=self.aws_session_token,
                                         bucket_name=self.sink_blob_bucket_name,
                                         endpoint_url=self.aws_endpoint,
                                         download_file=True, src_path=file_to_download,
                                         dest_path=dest_path
                                         )
            elif self.link_type == 'gcs':
                self.gcs_client.download_file_from_gcs(self.sink_blob_bucket_name, file_to_download, dest_path)

            json_data = []
            with open(dest_path, 'r') as json_file:
                # Load the JSON data from the file
                for line in json_file:
                    data = json.loads(line)
                    json_data.append(data)

            sorted_data = sorted(json_data, key=lambda x: (-x['avg_rating'], x['country'][::-1]))

            for dict1, dict2 in zip(json_data, sorted_data):
                if dict1 != dict2:
                    self.fail("The data is not in sorted order")

        # need to add data in file ordering checks
        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            statement = "select count(*) from {0} where copy_dataset = \"{1}\"".format(dataset_obj.full_name, path)
            status, metrics, errors, result, _, _ \
                = self.cbas_util.execute_statement_on_cbas_util(self.columnar_cluster, statement)
            doc_count_in_dataset = self.cbas_util.get_num_items_in_cbas_dataset(self.columnar_cluster,
                                                                                datasets[i].full_name)
            if result[0]['$1'] != doc_count_in_dataset:
                self.log.error("Document count mismatch in blob storage dataset {0} and dataset {1}".format(
                    dataset_obj.full_name, datasets[i].full_name
                ))
            results.append(result[0]['$1'] == doc_count_in_dataset)
        if not all(results):
            self.fail("The document count does not match in dataset and blob storage")

    def test_create_copyTo_from_collection_invalid_link_drop_standalone_collection(self):
        self.base_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        no_of_docs = self.input.param("no_of_docs", 100)
        jobs = Queue()
        results = []
        self.log.info("Adding {} documents in standalone dataset. Default doc size is 1KB".format(no_of_docs))
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name,
                       "database_name": dataset.database_name, "no_of_docs": no_of_docs}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Some documents were not inserted")

        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            destination_link = self.cbas_util.generate_name()
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.columnar_cluster, "collection_name": datasets[i].name,
                       "dataverse_name": datasets[i].dataverse_name,
                       "database_name": datasets[i].database_name,
                       "destination_bucket": self.sink_blob_bucket_name,
                       "destination_link_name": destination_link, "path": path, "validate_error_msg": True,
                       "expected_error": "Link {0} does not exist".format(destination_link),
                       "expected_error_code": 24006}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.fail("Failed to execute copy to statement with error")

    def test_create_copyTo_from_collection_to_different_region_existing_s3_bucket(self):
        if self.link_type != "s3":
            self.log.info("Test only valid for s3 links")
            pass
        self.base_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        no_of_docs = self.input.param("no_of_docs", 100)
        jobs = Queue()
        self.log.info("Adding {} documents in standalone dataset. Default doc size is 1KB".format(no_of_docs))

        external_link_obj = \
            self.cbas_util.create_external_link_obj(self.columnar_cluster, accessKeyId=self.aws_access_key,
                                                    secretAccessKey=self.aws_secret_key,
                                                    serviceEndpoint=self.aws_endpoint,
                                                    regions=["us-west-1"])[0]
        if not self.cbas_util.create_external_link(self.columnar_cluster, external_link_obj.properties):
            self.fail("Failed to create S3 link on different region")

        results = []
        bucket_name = self.cbas_util.generate_name()
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.columnar_cluster, "collection_name": datasets[i].name,
                       "dataverse_name": datasets[i].dataverse_name,
                       "destination_bucket": bucket_name,
                       "destination_link_name": external_link_obj.full_name, "path": path, "validate_error_msg": True,
                       "expected_error": "External sink error. "
                                         "software.amazon.awssdk.services.s3.model.NoSuchBucketException: "
                                         "The specified bucket does not exist",
                       "expected_error_code": 24230}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.fail("Failed to execute copy to statement with error")

    def test_create_copyTo_from_multi_partition_field_to_blob_storage_bucket(self):
        self.base_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        no_of_docs = self.input.param("no_of_docs", 1000)
        blob_storage_link= self.cbas_util.get_all_link_objs(self.link_type)[0]
        jobs = Queue()
        results = []
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name,
                       "database_name": dataset.database_name, "no_of_docs": no_of_docs}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Some documents were not inserted")

        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            jobs.put((self.cbas_util.copy_to_s3,
                      {
                          "cluster": self.columnar_cluster, "collection_name": datasets[i].name,
                          "dataverse_name": datasets[i].dataverse_name,
                          "database_name": datasets[i].database_name,
                          "alias_identifier": "ally",
                          "destination_bucket": self.sink_blob_bucket_name,
                          "destination_link_name": blob_storage_link.full_name, "path": path, "partition_alias": "public_likes",
                          "partition_by": "ally.public_likes"}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.log.error("Failed to execute copy command")

    def test_create_copyTo_from_missing_partition_field_to_blob_storage_bucket(self):
        self.base_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        no_of_docs = self.input.param("no_of_docs", 1000)
        blob_storage_link = self.cbas_util.get_all_link_objs(self.link_type)[0]
        jobs = Queue()
        results = []
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name,
                       "database_name": dataset.database_name, "no_of_docs": no_of_docs, "include_country": "mixed"}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Some documents were not inserted")

        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.columnar_cluster, "collection_name": datasets[i].name,
                       "alias_identifier": "ally",
                       "dataverse_name": datasets[i].dataverse_name, "database_name": datasets[i].database_name,
                       "destination_bucket": self.sink_blob_bucket_name,
                       "destination_link_name": blob_storage_link.full_name, "path": path, "partition_alias": "country",
                       "partition_by": "ally.country"}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.fail("Copy to S3 statement failure")

    def test_create_copyTo_from_multiple_partition_field_to_blob_storage_bucket(self):
        self.base_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        no_of_docs = self.input.param("no_of_docs", 1000)
        blob_storage_link = self.cbas_util.get_all_link_objs(self.link_type)[0]
        jobs = Queue()
        results = []
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name,
                       "database_name": dataset.database_name, "no_of_docs": no_of_docs}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Some documents were not inserted")

        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)

            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.columnar_cluster, "collection_name": datasets[i].name,
                       "alias_identifier": "ally",
                       "dataverse_name": datasets[i].dataverse_name, "database_name": datasets[i].database_name,
                       "destination_bucket": self.sink_blob_bucket_name,
                       "destination_link_name": blob_storage_link.full_name, "path": path, "partition_alias": ["country", "city"],
                       "partition_by": ["ally.country", "ally.city"]}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.fail("Copy to blob storage statement failure")

        # create external link on copied bucket
        path_on_external_container = "{copy_dataset:string}/{country:string}"
        dataset_obj = self.cbas_util.create_external_dataset_obj(self.columnar_cluster, link_type=self.link_type,
                                                                 external_container_names={
                                                                     self.sink_blob_bucket_name: self.aws_region},
                                                                 paths_on_external_container=[
                                                                     path_on_external_container],
                                                                 file_format="json")[0]
        if not self.create_external_dataset(dataset_obj):
            self.fail("Failed to create external dataset on destination blob storage bucket")

        files = []
        if self.link_type == "s3":
            files = perform_S3_operation(aws_access_key=self.aws_access_key,
                                         aws_secret_key=self.aws_secret_key,
                                         region=self.aws_region,
                                         aws_session_token=self.aws_session_token,
                                         endpoint_url=self.aws_endpoint,
                                         bucket_name=self.sink_blob_bucket_name,
                                         get_bucket_objects=True)
        elif self.link_type == "gcs":
            files = self.gcs_client.list_objects_in_gcs_bucket(self.sink_blob_bucket_name)
        objects_in_blob_storage = [str(x) for x in files]

        results = []
        dynamic_statement = "select count(*) from {0} where copy_dataset = \"{1}\" and country = \"{2}\""
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            statement = "select country, count(city) as cnt from {0} group by country;".format(datasets[i].full_name)
            status, metrics, errors, result, _, _ \
                = self.cbas_util.execute_statement_on_cbas_util(self.columnar_cluster,
                                                                statement)
            get_city_count_per_country = [x for x in result if x['cnt'] > 1]
            for j in range(2):
                obj = random.choice(get_city_count_per_country)
                files_on_country = obj['cnt']
                country_name = obj['country']
                path_directory = "{0}/{1}".format(path, country_name)
                count = 0
                for obj in objects_in_blob_storage:
                    if obj.startswith(path_directory):
                        count = count + 1
                if count < files_on_country:
                    self.log.error("Not all partitions created")
                results.append(count == files_on_country)

                statement = "select count(*) from {0} where country = \"{1}\"".format(datasets[i], country_name)
                statement2 = dynamic_statement.format(dataset_obj.full_name, path, country_name)
                status, metrics, errors, result1, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                    self.columnar_cluster,
                    statement)
                status, metrics, errors, result2, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                    self.columnar_cluster,
                    statement2)
                if result1[0]['$1'] != result2[0]['$1']:
                    self.log.error("Not all doc are copied for country {}".format(country_name))

                result.append(result1[0]['$1'] == result2[0]['$1'])

        if not all(results):
            self.fail("Verification failed for multiple partition")

    def test_create_copyTo_from_collection_where_partition_already_exist_in_blob_storage(self):
        self.base_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        blob_storage_link = self.cbas_util.get_all_link_objs(self.link_type)[0]
        no_of_docs = self.input.param("no_of_docs", 1000)
        jobs = Queue()
        results = []
        for i in range(len(datasets)):
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": datasets[i].name,
                       "dataverse_name": datasets[i].dataverse_name, "database_name": datasets[i].database_name,
                       "no_of_docs": no_of_docs, "country_type": "mixed"}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.log.error("Not all docs were inserted")
        results = []

        # initiate copy command
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.columnar_cluster, "collection_name": datasets[i].name,
                       "alias_identifier": "ally",
                       "dataverse_name": datasets[i].dataverse_name, "database_name": datasets[i].database_name,
                       "destination_bucket": self.sink_blob_bucket_name,
                       "destination_link_name": blob_storage_link.full_name, "path": path, "partition_alias": "country",
                       "partition_by": "ally.country"}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if self.link_type == "s3":
            if not all(results):
                self.fail("Copy to blob storage statement failure")
            else:
                return
        elif self.link_type == "gcp":
            if all(results):
                self.fail("Copy to blob storage statement failure")
            else:
                return

        files = []
        if self.link_type == "s3":
            files = perform_S3_operation(aws_access_key=self.aws_access_key,
                                         aws_secret_key=self.aws_secret_key,
                                         region=self.aws_region,
                                         aws_session_token=self.aws_session_token,
                                         endpoint_url=self.aws_endpoint,
                                         bucket_name=self.sink_blob_bucket_name,
                                         get_bucket_objects=True)
        elif self.link_type == "gcs":
            files = self.gcs_client.list_objects_in_gcs_bucket(self.sink_blob_bucket_name)

        objects_in_blob_storage = [str(x) for x in files]
        if len(objects_in_blob_storage) == 0:
            self.fail("Failed to execute copy statement, no objects present in S3 bucket")

        # copy again to same destination where partition already exists
        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.columnar_cluster, "collection_name": datasets[i].name,
                       "alias_identifier": "ally",
                       "dataverse_name": datasets[i].dataverse_name, "database_name": datasets[i].database_name,
                       "destination_bucket": self.sink_blob_bucket_name,
                       "destination_link_name": blob_storage_link.full_name, "path": path, "partition_alias": "country",
                       "partition_by": "ally.country", "validate_error_msg": True,
                       "expected_error": "Cannot write to a non-empty directory",
                       "expected_error_code": 23073}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.fail("Copy to blob storage statement failure")

    def test_create_copyTo_from_collection_empty_query_result_in_blob_storge(self):
        self.base_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        blob_storage_link = self.cbas_util.get_all_link_objs(self.link_type)[0]
        jobs = Queue()
        results = []

        # initiate copy command
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            query = "select * from {0} where 1 = 0".format(datasets[i].full_name)
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.columnar_cluster, "alias_identifier": "ally",
                       "source_definition_query": query,
                       "destination_bucket": self.sink_blob_bucket_name,
                       "destination_link_name": blob_storage_link.full_name, "path": path, "partition_alias": "country",
                       "partition_by": "ally.country"}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.fail("Copy to S3 statement failure")

        files = []
        if self.link_type == "s3":
            files = perform_S3_operation(aws_access_key=self.aws_access_key,
                                         aws_secret_key=self.aws_secret_key,
                                         region=self.aws_region,
                                         aws_session_token=self.aws_session_token,
                                         endpoint_url=self.aws_endpoint,
                                         bucket_name=self.sink_blob_bucket_name,
                                         get_bucket_objects=True)
        elif self.link_type == "gcs":
            files = self.gcs_client.list_objects_in_gcs_bucket(self.sink_blob_bucket_name)

        objects_in_s3 = [str(x) for x in files]
        if len(objects_in_s3) != 0:
            self.fail("Failed to execute copy statement, objects present for empty query in blob storage bucket")

    def test_create_copyTo_from_collection_aggregate_group_by_result_in_blob_storage(self):
        self.base_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        blob_storage_link = self.cbas_util.get_all_link_objs(self.link_type)[0]
        no_of_docs = self.input.param("no_of_docs", 1000)
        jobs = Queue()
        results = []
        for i in range(len(datasets)):
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": datasets[i].name,
                       "dataverse_name": datasets[i].dataverse_name, "database_name": datasets[i].database_name,
                       "no_of_docs": no_of_docs}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.log.error("Not all docs were inserted")
        results = []

        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            query = "SELECT country, ARRAY_AGG(city) AS city FROM {0} GROUP BY country".format(datasets[i].full_name)
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.columnar_cluster, "source_definition_query": query, "alias_identifier": "ally",
                       "destination_bucket": self.sink_blob_bucket_name,
                       "destination_link_name": blob_storage_link.full_name, "path": path, "partition_alias": "country",
                       "partition_by": "ally.country"}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.fail("Copy to S3 statement failure")

        path_on_external_container_string = "{copy_dataset:string}/{country:string}"
        dataset_obj_string = self.cbas_util.create_external_dataset_obj(self.columnar_cluster, link_type=self.link_type,
                                                                        external_container_names={
                                                                            self.sink_blob_bucket_name: self.aws_region},
                                                                        paths_on_external_container=[
                                                                            path_on_external_container_string],
                                                                        file_format="json")[0]
        if not self.create_external_dataset(dataset_obj_string):
            self.fail("Failed to create external dataset on destination blob storage bucket")

        # verification step
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            for j in range(5):
                statement = "select * from {0} limit 1".format(datasets[i].full_name)
                status, metrics, errors, result, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                    self.columnar_cluster,
                    statement)
                country_name = ((result[0])[CBASHelper.unformat_name(datasets[i].name)]["country"]).replace("&amp;", "")
                query_statement = ("SELECT ARRAY_LENGTH(ARRAY_AGG(city)) as city FROM {0} "
                                   "where country = \"{1}\"").format(datasets[i].full_name, str(country_name))
                dynamic_statement = "select * from {0} where copy_dataset = \"{1}\" and country = \"{2}\"".format(
                    dataset_obj_string.full_name, path, str(country_name))

                status, metrics, errors, result, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                    self.columnar_cluster,
                    dynamic_statement)
                length_of_city_array_from_blob_storge = len((result[0][dataset_obj_string.name])['city'])

                status, metrics, errors, result2, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                    self.columnar_cluster,
                    query_statement)

                length_of_city_array_from_dataset = result2[0]["city"]
                if length_of_city_array_from_blob_storge != length_of_city_array_from_dataset:
                    self.log.error("Length of city aggregate failed")
                results.append(length_of_city_array_from_blob_storge == length_of_city_array_from_dataset)

        if not all(results):
            self.fail("Copy to statement copied the wrong results")

    def remote_cluster_setup(self):
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
            self.remote_cluster_certificate = (remote_cluster_certificate_request.json()["certificate"])
        else:
            self.fail("Failed to get cluster certificate")

        # creating bucket scope and collections for remote collection
        no_of_remote_buckets = self.input.param("no_of_remote_bucket", 1)
        self.create_bucket_scopes_collections_in_capella_cluster(
            self.tenant, self.remote_cluster, no_of_remote_buckets,
            bucket_ram_quota=1024)

    def test_mini_volume_copy_to_blob_storage(self):
        self.copy_to_s3_job = Queue()
        self.copy_to_s3_results = []
        self.mongo_util = MongoUtil(
            task_manager=self.task_manager,
            hostname=self.input.param("mongo_hostname"),
            username=self.input.param("mongo_username"),
            password=self.input.param("mongo_password")
        )

        # Initialize variables for Kafka
        self.kafka_topic_prefix = f"on_off_{int(time.time())}"

        # Initializing Confluent util and Confluent cluster object.
        self.confluent_util = ConfluentUtils(
            cloud_access_key=self.input.param("confluent_cloud_access_key"),
            cloud_secret_key=self.input.param("confluent_cloud_secret_key"))
        self.confluent_cluster_obj = self.confluent_util.generate_confluent_kafka_object(
            kafka_cluster_id=self.input.param("confluent_cluster_id"),
            topic_prefix=self.kafka_topic_prefix)
        if not self.confluent_cluster_obj:
            self.fail("Unable to initialize Confluent Kafka cluster object")

        # Initializing KafkaConnect Util and kafka connect server hostnames
        self.kafka_connect_util = KafkaConnectUtil()
        kafka_connect_hostname = self.input.param('kafka_connect_hostname')
        self.kafka_connect_hostname_cdc_confluent = (
            f"{kafka_connect_hostname}:{KafkaConnectUtil.CONFLUENT_CDC_PORT}")
        self.kafka_connect_hostname_non_cdc_confluent = (
            f"{kafka_connect_hostname}:{KafkaConnectUtil.CONFLUENT_NON_CDC_PORT}")

        self.kafka_topics = {
            "confluent": {
                "MONGODB": [
                    {
                        "topic_name": "do-not-delete-mongo-cdc.Product_Template.10GB",
                        "key_serialization_type": "json",
                        "value_serialization_type": "json",
                        "cdc_enabled": True,
                        "source_connector": "DEBEZIUM",
                        "num_items": 10000000
                    },
                    {
                        "topic_name": "do-not-delete-mongo-non-cdc.Product_Template.10GB",
                        "key_serialization_type": "json",
                        "value_serialization_type": "json",
                        "cdc_enabled": False,
                        "source_connector": "DEBEZIUM",
                        "num_items": 10000000
                    },
                ],
                "POSTGRESQL": [],
                "MYSQLDB": []
            }
        }

        # creating bucket scope and collections for remote collection
        self.create_bucket_scopes_collections_in_capella_cluster(
            self.tenant, self.remote_cluster,
            self.input.param("num_buckets", 1))

        self.setup_infra_for_mongo()
        confluent_kafka_cluster_details = [
            self.confluent_util.generate_confluent_kafka_cluster_detail(
                brokers_url=self.confluent_cluster_obj.bootstrap_server,
                auth_type="PLAIN", encryption_type="TLS",
                api_key=self.confluent_cluster_obj.cluster_access_key,
                api_secret=self.confluent_cluster_obj.cluster_secret_key)]

        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=self.remote_cluster,
            external_collection_file_formats=["json"],
            confluent_kafka_cluster_details=confluent_kafka_cluster_details,
            external_dbs=["MONGODB"],
            kafka_topics=self.kafka_topics)

        self.columnar_spec["standalone_dataset"]["primary_key"] = [
            {"name": "string", "email": "string"}]
        self.columnar_spec["index"]["indexed_fields"] = ["price:double"]
        self.columnar_spec["kafka_dataset"]["primary_key"] = [
            {"_id": "string"}]

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
        if not result:
            self.fail(msg)

        start_time = time.time()
        self.mini_volume = MiniVolume(self)
        self.mini_volume.calculate_volume_per_source()
        # initiate copy to blob storage
        for i in range(1, 5):
            if i % 2 == 0:
                self.mini_volume.run_processes(i, 2 ** (i - 1), False)
            else:
                self.mini_volume.run_processes(i, 2 ** (i + 1), False)
            self.mini_volume.stop_process()

            datasets = self.cbas_util.get_all_dataset_objs()
            s3_link = self.cbas_util.get_all_link_objs(self.link_type)[0]
            for j in range(len(datasets)):
                path = "copy_dataset_" + str(i) + datasets[j].full_name
                self.copy_to_s3_job.put((self.cbas_util.copy_to_s3,
                                         {"cluster": self.columnar_cluster, "collection_name": datasets[j].name,
                                          "dataverse_name": datasets[j].dataverse_name,
                                          "database_name": datasets[j].database_name,
                                          "destination_bucket": self.sink_blob_bucket_name,
                                          "destination_link_name": s3_link.full_name,
                                          "path": path, "analytics_timeout": 10000000, "timeout": 10000000}))
            self.log.info("Running copy to S3")
            time.sleep(180)
            self.cbas_util.run_jobs_in_parallel(
                self.copy_to_s3_job, self.copy_to_s3_results, self.sdk_clients_per_user, async_run=False)

            if not all(self.copy_to_s3_results):
                self.log.error("Some documents were not inserted")

        self.log.info("Time taken to run mini-volume: {} minutes".format((time.time() - start_time) / 60))
        self.log.info("Mini-Volume for backup-restore finished")


    def test_create_copyTo_from_standalone_collection_nested_array(self):
        self.base_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        blob_storage_link = self.cbas_util.get_all_link_objs(self.link_type)[0]
        no_of_docs = self.input.param("no_of_docs", 1000)

        # insert nested array in the standalone collection
        jobs = Queue()
        results = []
        self.log.info("Adding {} documents in standalone dataset. Default doc size is 1KB".format(no_of_docs))
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name,
                       "no_of_docs": no_of_docs, "nested_level": 3}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Some documents were not inserted")

        # COPY TO s3 in parquet format
        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.columnar_cluster, "collection_name": datasets[i].name,
                       "dataverse_name": datasets[i].dataverse_name, "database_name": datasets[i].database_name,
                       "destination_bucket": self.sink_blob_bucket_name,
                       "destination_link_name": blob_storage_link.full_name, "file_format": self.columnar_spec["file_format"],
                       "path": path}))

        self.cbas_util.run_jobs_in_parallel(
                jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.fail("Copy to blob storage statement failure")


        # Create external dataset on the blob storage bucket
        path_on_external_container = "{copy_dataset:string}"
        dataset_obj = self.cbas_util.create_external_dataset_obj(
            self.columnar_cluster, link_type=self.link_type,
            external_container_names={
                self.sink_blob_bucket_name: self.aws_region},
            paths_on_external_container=[path_on_external_container],
            file_format="parquet")[0]
        if not self.create_external_dataset(dataset_obj):
            self.fail("Failed to create external dataset on destination blob storage bucket")


        # Verify the data in dataset and blob storage (s3)
        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            statement = "select count(*) from {0} where copy_dataset = \"{1}\"".format(dataset_obj.full_name, path)
            status, metrics, errors, result, _, _ = self.cbas_util.execute_statement_on_cbas_util(self.columnar_cluster,
                                                                                                  statement)
            doc_count_in_dataset = self.cbas_util.get_num_items_in_cbas_dataset(self.columnar_cluster,
                                                                                datasets[i].full_name)
            if result[0]['$1'] != doc_count_in_dataset:
                self.log.error("Document count mismatch in blob storage dataset {0} and dataset {1}".format(
                    dataset_obj.full_name, datasets[i].full_name
                ))
            results.append(result[0]['$1'] == doc_count_in_dataset)

        if not all(results):
            self.fail("The document count does not match in dataset and blob storage")
        self.log.info("Nested array COPY TO blob storage finished")