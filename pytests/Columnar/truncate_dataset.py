"""
Created on 11-Aoril-2025

@author: anisha.sinha@couchbase.com
"""

import json
import os
import random
import string
import time
from queue import Queue
from capellaAPI.capella.columnar.CapellaAPI import CapellaAPI as ColumnarAPI
from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI as CapellaAPIv2
from Columnar.columnar_base import ColumnarBaseTest
from Columnar.columnar_rbac_cloud import generate_random_entity_name
from Jython_tasks.sirius_task import CouchbaseUtil
from gcs import GCS
from kafka_util.confluent_utils import ConfluentUtils
from kafka_util.kafka_connect_util import KafkaConnectUtil
from sirius_client_framework.sirius_constants import SiriusCodes

gcs_certificate = os.getenv('gcp_access_file')


class TruncateDatasetTest(ColumnarBaseTest):
    def __init__(self, methodName: str = "runTest"):
        super().__init__(methodName)
        self.pod = None
        self.tenant = None
        self.no_of_docs = None

    def setUp(self):
        super(TruncateDatasetTest, self).setUp()
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
        self.sink_blob_bucket_name = None
        self.link_type = self.input.param("external_link_source", "s3")
        self.gcs_client = None
        if self.link_type == "gcs":
            with open(gcs_certificate, 'r') as file:
                # Load JSON data from file
                credentials = json.load(file)
                self.gcs_client = GCS(credentials)
        self.initial_doc_count = self.input.param("initial_doc_count", 100)
        self.doc_size = self.input.param("doc_size", 1024)
        if not self.columnar_spec_name:
            self.columnar_spec_name = "full_template"

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

        super(TruncateDatasetTest, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished", stage="Teardown")

    def load_doc_to_remote_collections(self, start, end):
        for remote_bucket in self.remote_cluster.buckets:
            for scope_name, scope in remote_bucket.scopes.items():
                if scope_name != "_system" and scope != "_mobile":
                    for collection_name, collection in (
                            scope.collections.items()):
                        self.log.info(
                            f"Loading docs in {remote_bucket.name}."
                            f"{scope_name}.{collection_name}")
                        cb_doc_loading_task = self.couchbase_doc_loader.load_docs_in_couchbase_collection(
                            bucket=remote_bucket.name, scope=scope_name,
                            collection=collection_name, start=start,
                            end=end,
                            doc_template=SiriusCodes.Templates.PRODUCT,
                            doc_size=self.doc_size, sdk_batch_size=1000
                        )
                        if not cb_doc_loading_task.result:
                            self.fail(
                                f"Failed to load docs in couchbase collection "
                                f"{remote_bucket.name}.{scope_name}.{collection_name}")
                        else:
                            collection.num_items = (
                                start + cb_doc_loading_task.success_count)

    def test_truncate_standalone_collection(self):
        database_name = self.input.param("database", "Default")
        dataverse_name = self.input.param("dataverse", "Default")
        no_of_collection = self.input.param("num_standalone_collections", 1)
        key = json.loads(self.input.param("key", None)) if self.input.param("key", None) is not None else None
        primary_key = dict()

        if key is None:
            primary_key = None
        else:
            for key_value in key:
                primary_key[str(key_value)] = str(key[key_value])
        jobs = Queue()
        results = []
        for i in range(no_of_collection):
            dataset_obj = self.cbas_util.create_standalone_dataset_obj(self.columnar_cluster,
                                                                       database_name=database_name,
                                                                       dataverse_name=dataverse_name)
            jobs.put((self.cbas_util.create_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": dataset_obj[0].name,
                       "dataverse_name": dataset_obj[0].dataverse_name,
                       "database_name": dataset_obj[0].database_name, "primary_key": primary_key}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )

        if not all(results):
            self.fail("Failed to create some collection with key {0}".format(str(key)))

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

        for dataset in datasets:
            cmd = "Truncate Dataset {}".format(dataset.full_name)
            status, metrics, errors, result, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, cmd)

            if status != 'success':
                self.fail("Unable to Truncate dataset {} with error {}".format(dataset.full_name, errors))


        # Verify that the dataset is truncated.
        for dataset in datasets:
            statement = "Select count(*) from {}".format(dataset.full_name)
            status, metrics, errors, results, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, statement)
            if status == "success":
                if any(any(value != 0 for value in item.values()) for item in results):
                    self.fail("Collection {} did not get truncated".format(dataset.full_name))

        # Verify insertion of data post running truncate statement.
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name,
                       "no_of_docs": self.initial_doc_count, "document_size": self.doc_size}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )

        if not all(results):
            self.fail("Failed to load data into standalone collection after running truncate.")

    def test_truncate_copy_from_blob_storage(self):
        file_format = "json"
        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            external_collection_file_formats=[file_format])

        self.columnar_spec["standalone_dataset"]["primary_key"] = [
            {"id": "string", "product_name": "string"}]

        self.columnar_spec["standalone_dataset"][
            "standalone_collection_properties"] = self.columnar_spec[
            "external_dataset"]["external_dataset_properties"]

        self.columnar_spec["standalone_dataset"]["data_source"] = [self.link_type]
        dataset_properties = self.columnar_spec["standalone_dataset"][
            "standalone_collection_properties"][0]
        dataset_properties["include"] = "*.{0}".format(file_format)

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.columnar_cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        jobs = Queue()
        results = []
        for standalone_coll in datasets:
            jobs.put((self.cbas_util.copy_from_external_resource_into_standalone_collection,
                      {
                          "cluster": self.columnar_cluster, "collection_name": standalone_coll.name,
                          "aws_bucket_name": standalone_coll.dataset_properties["external_container_name"],
                          "external_link_name": standalone_coll.link_name,
                          "dataverse_name": standalone_coll.dataverse_name,
                          "database_name": standalone_coll.database_name,
                          "files_to_include": standalone_coll.dataset_properties["include"],
                          "file_format": standalone_coll.dataset_properties["file_format"],
                          "type_parsing_info": standalone_coll.dataset_properties["object_construction_def"],
                          "path_on_aws_bucket": standalone_coll.dataset_properties["path_on_external_container"],
                          "header": standalone_coll.dataset_properties["header"],
                          "null_string": standalone_coll.dataset_properties["null_string"],
                          "files_to_exclude": standalone_coll.dataset_properties["exclude"],
                          "parse_json_string": standalone_coll.dataset_properties["parse_json_string"],
                          "convert_decimal_to_double": standalone_coll.dataset_properties["convert_decimal_to_double"],
                          "timezone": standalone_coll.dataset_properties["timezone"]
                      }))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=True)

        self.log.info("Sleeping 15 seconds, waiting all copy statement to execute")
        time.sleep(5)

        for dataset in datasets:
            cmd = "Truncate Dataset {}".format(dataset.full_name)
            status, metrics, errors, result, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, cmd)

            if status != 'success':
                self.fail("Unable to Truncate dataset {} with error {}".format(dataset.full_name, errors))

        # Verify that the dataset is truncated.
        for dataset in datasets:
            statement = "Select count(*) from {}".format(dataset.full_name)
            status, metrics, errors, results, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, statement)
            if status == "success":
                if any(any(value != 0 for value in item.values()) for item in results):
                    self.fail("Collection {} did not get truncated".format(dataset.full_name))

        for standalone_coll in datasets:
            jobs.put((self.cbas_util.copy_from_external_resource_into_standalone_collection,
                      {
                          "cluster": self.columnar_cluster, "collection_name": standalone_coll.name,
                          "aws_bucket_name": standalone_coll.dataset_properties["external_container_name"],
                          "external_link_name": standalone_coll.link_name,
                          "dataverse_name": standalone_coll.dataverse_name,
                          "database_name": standalone_coll.database_name,
                          "files_to_include": standalone_coll.dataset_properties["include"],
                          "file_format": standalone_coll.dataset_properties["file_format"],
                          "type_parsing_info": standalone_coll.dataset_properties["object_construction_def"],
                          "path_on_aws_bucket": standalone_coll.dataset_properties["path_on_external_container"],
                          "header": standalone_coll.dataset_properties["header"],
                          "null_string": standalone_coll.dataset_properties["null_string"],
                          "files_to_exclude": standalone_coll.dataset_properties["exclude"],
                          "parse_json_string": standalone_coll.dataset_properties["parse_json_string"],
                          "convert_decimal_to_double": standalone_coll.dataset_properties["convert_decimal_to_double"],
                          "timezone": standalone_coll.dataset_properties["timezone"]
                      }))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=True)

        query = "select * from {} limit 1000"
        for dataset in self.cbas_util.get_all_dataset_objs("standalone"):
            jobs.put((
                self.cbas_util.execute_statement_on_cbas_util,
                {"cluster": self.columnar_cluster,
                 "statement": query.format(dataset.full_name)}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not results[2]:
            self.fail("Failed to execute query after copying to collection post truncate")

    def test_truncate_kafka_linked_collection(self):
        # Initialize variables for Kafka
        self.kafka_topic_prefix = f"confluent_regression_{int(time.time())}"

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
                "POSTGRESQL": []
            }
        }

        self.serialization_type = self.input.param("serialization_type",
                                                   "JSON")
        self.schema_registry_url = self.input.param("schema_registry_url")
        self.schema_registry_api_key = self.input.param(
            "schema_registry_api_key")
        self.schema_registry_secret_key = self.input.param(
            "schema_registry_secret_key")

        try:
            source_db = "POSTGRESQL"

            self.kafka_topics["confluent"]["POSTGRESQL"].append(
                {
                    "topic_name": "postgres2.public.employee_data",
                    "key_serialization_type": "json",
                    "value_serialization_type": "json",
                    "cdc_enabled": False,
                    "source_connector": "DEBEZIUM",
                    "num_items": 1000000
                }
            )

            confluent_kafka_cluster_details = [
                self.confluent_util.generate_confluent_kafka_cluster_detail(
                    brokers_url=self.confluent_cluster_obj.bootstrap_server,
                    auth_type="PLAIN", encryption_type="TLS",
                    api_key=self.confluent_cluster_obj.cluster_access_key,
                    api_secret=self.confluent_cluster_obj.cluster_secret_key)]

            schema_registry_details = []
            use_schema_registry = self.input.param("use_schema_registry", False)
            if use_schema_registry:
                schema_registry_details = [
                    self.confluent_util.generate_confluent_schema_registry_detail(
                        self.schema_registry_url, self.schema_registry_api_key,
                        self.schema_registry_secret_key)]

            self.columnar_spec = self.populate_columnar_infra_spec(
                columnar_spec=self.cbas_util.get_columnar_spec(
                    self.columnar_spec_name),
                confluent_kafka_cluster_details=confluent_kafka_cluster_details,
                confluent_kafka_schema_registry_details=schema_registry_details,
                external_dbs=[source_db], kafka_topics=self.kafka_topics)

            self.columnar_spec["kafka_dataset"]["primary_key"] = [
                {"id": "INT"}]

            result, msg = self.cbas_util.create_cbas_infra_from_spec(
                cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
                bucket_util=self.bucket_util, wait_for_ingestion=False)
            if not result:
                self.fail(msg)

            for collection in self.cbas_util.get_all_dataset_objs("standalone"):
                result = self.cbas_util.wait_for_ingestion_complete(
                    self.columnar_cluster, collection.full_name,
                    collection.num_of_items, 3600)
                if not result:
                    self.fail("Data ingestion did not complete for kafka dataset")

            self.log.info("Data ingestion completed successfully")

            datasets = self.cbas_util.get_all_dataset_objs("standalone")
            dataset = datasets[0]
            cmd = "Truncate Dataset {}".format(dataset.full_name)
            status, metrics, errors, result, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, cmd)

            if status != 'success':
                self.fail("Unable to Truncate dataset {} with error {}".format(dataset.full_name, errors))

            for collection in self.cbas_util.get_all_dataset_objs("standalone"):
                result = self.cbas_util.wait_for_ingestion_complete(
                    self.columnar_cluster, collection.full_name,
                    collection.num_of_items, 3600)
                if not result:
                    self.fail("Data ingestion did not complete for kafka dataset after running Truncate.")

        finally:
            delete_confluent_dlq_topic = (
                self.confluent_util.kafka_cluster_util.delete_topic_by_topic_prefix(
                    self.kafka_topic_prefix))
            try:
                self.confluent_util.confluent_apis.delete_api_key(
                    self.confluent_cluster_obj.cluster_access_key)
            except Exception as err:
                self.log.error(str(err))
            if not delete_confluent_dlq_topic:
                self.fail("Unable to cleanup Confluent Kafka resources")

    def test_truncate_remote_linked_dataset(self):
        # creating bucket scope and collections for remote collection
        self.create_bucket_scopes_collections_in_capella_cluster(
            self.tenant, self.remote_cluster,
            self.input.param("num_buckets", 1))

        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=self.remote_cluster)

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
        if not result:
            self.fail(msg)

        self.load_doc_to_remote_collections(0, self.initial_doc_count)

        remote_links = self.cbas_util.get_all_link_objs("couchbase")
        remote_datasets = self.cbas_util.get_all_dataset_objs("remote")

        for link in remote_links:
            if not self.cbas_util.connect_link(self.columnar_cluster, link.full_name):
                self.fail("Failed to connect link")

        self.cbas_util.refresh_remote_dataset_item_count(self.bucket_util)

        for dataset in remote_datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.columnar_cluster, dataset.full_name,
                    dataset.num_of_items):
                self.fail("Doc count mismatch.")

        datasets = self.cbas_util.get_all_dataset_objs("remote")
        dataset = datasets[0]
        cmd = "Truncate Dataset {}".format(dataset.full_name)
        status, metrics, errors, result, _, _ = self.cbas_util.execute_statement_on_cbas_util(
            self.columnar_cluster, cmd)

        if status != 'success':
            self.fail("Unable to Truncate dataset {} with error {}".format(dataset.full_name, errors))

        for collection in self.cbas_util.get_all_dataset_objs("remote"):
            result = self.cbas_util.wait_for_ingestion_complete(
                self.columnar_cluster, collection.full_name,
                collection.num_of_items, 3600)
            if not result:
                self.fail("Data ingestion did not complete for remote dataset after running Truncate.")

    def test_different_collection_after_truncate(self):
        """
        Test that a second collection does not get truncated when first collection is truncated from a remote link.
        """
        self.check_new_ingestion = self.input.param("check_new_ingestion", False)
        self.create_bucket_scopes_collections_in_capella_cluster(
            self.tenant, self.remote_cluster,
            self.input.param("num_buckets", 1))

        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=self.remote_cluster)

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
        if not result:
            self.fail(msg)

        self.load_doc_to_remote_collections(0, self.initial_doc_count)

        remote_links = self.cbas_util.get_all_link_objs("couchbase")
        remote_datasets = self.cbas_util.get_all_dataset_objs("remote")

        for link in remote_links:
            if not self.cbas_util.connect_link(self.columnar_cluster, link.full_name):
                self.fail("Failed to connect link")

        self.cbas_util.refresh_remote_dataset_item_count(self.bucket_util)

        for dataset in remote_datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.columnar_cluster, dataset.full_name,
                    dataset.num_of_items):
                self.fail("Doc count mismatch.")

        for link in remote_links:
            if not self.cbas_util.disconnect_link(self.columnar_cluster, link.full_name):
                self.fail("Failed to disconnect link")

        datasets = self.cbas_util.get_all_dataset_objs("remote")
        dataset = datasets[0]
        cmd = "Truncate Dataset {}".format(dataset.full_name)
        status, metrics, errors, result, _, _ = self.cbas_util.execute_statement_on_cbas_util(
            self.columnar_cluster, cmd)

        if status != 'success':
            self.fail("Unable to Truncate dataset {} with error {}".format(dataset.full_name, errors))

        dataset = datasets[1]
        cmd = "select count(*) from {}".format(dataset.full_name)
        status, metrics, errors, result, _, _ = self.cbas_util.execute_statement_on_cbas_util(
            self.columnar_cluster, cmd)

        if list(result[0].values())[0] != self.initial_doc_count:
            self.fail("Second dataset does not have correct data.")

        if self.check_new_ingestion:
            self.load_doc_to_remote_collections(self.initial_doc_count, self.initial_doc_count*2)
            datasets = self.cbas_util.get_all_dataset_objs("remote")
            dataset_0 = datasets[0]
            dataset_1 = datasets[1]
            for link in remote_links:
                if not self.cbas_util.connect_link(self.columnar_cluster, link.full_name):
                    self.fail("Failed to connect link")
            while True:
                cmd = "select count(*) from {}".format(dataset_1.full_name)
                status, metrics, errors, result, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                    self.columnar_cluster, cmd)
                dataset_1_count = list(result[0].values())[0]
                cmd = "select count(*) from {}".format(dataset_0.full_name)
                status, metrics, errors, result, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                    self.columnar_cluster, cmd)
                dataset_0_count = list(result[0].values())[0]
                if dataset_0_count < self.initial_doc_count:
                    if dataset_1_count > self.initial_doc_count:
                        self.fail("Ingestion for the second dataset started before the old changes are ingested in"
                                  "the truncated dataset.")
                else:
                    break
                self.log.info("Waiting for ingestion to reach the initial dataset point in first collection.")
                time.sleep(1)

            for collection in self.cbas_util.get_all_dataset_objs("remote"):
                result = self.cbas_util.wait_for_ingestion_complete(
                    self.columnar_cluster, collection.full_name,
                    collection.num_of_items, 3600)
                if not result:
                    self.fail("Data ingestion did not complete for remote dataset after running Truncate.")

    def test_truncate_metadata_collection(self):
        validate_error = self.input.param("validate_error", False)
        error_message = str(self.input.param("error_message", None))
        error_code = self.input.param("error_code", None)
        cmd = "Truncate dataset Metadata.`Index`"
        status, metrics, errors, result, _, _ = self.cbas_util.execute_statement_on_cbas_util(
            self.columnar_cluster, cmd)
        if validate_error:
            if not self.cbas_util.validate_error_and_warning_in_response(status,errors,error_message,error_code):
                self.fail("Did not get an appropriate error in truncating metadata collection.")


    def test_truncate_view(self):
        validate_error = self.input.param("validate_error", False)
        error_message = str(self.input.param("error_message", None))
        error_code = self.input.param("error_code", None)
        with_synonym = self.input.param("with_synonym", False)
        database_name = self.input.param("database", "Default")
        dataverse_name = self.input.param("dataverse", "Default")
        no_of_collection = self.input.param("num_standalone_collections", 1)
        key = json.loads(self.input.param("key", None)) if self.input.param("key", None) is not None else None
        primary_key = dict()
        if key is None:
            primary_key = None
        else:
            for key_value in key:
                primary_key[str(key_value)] = str(key[key_value])
        jobs = Queue()
        results = []
        for i in range(no_of_collection):
            dataset_obj = self.cbas_util.create_standalone_dataset_obj(self.columnar_cluster,
                                                                       database_name=database_name,
                                                                       dataverse_name=dataverse_name)
            jobs.put((self.cbas_util.create_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": dataset_obj[0].name,
                       "dataverse_name": dataset_obj[0].dataverse_name,
                       "database_name": dataset_obj[0].database_name, "primary_key": primary_key}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )

        if not all(results):
            self.fail("Failed to create some collection with key {0}".format(str(key)))

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
        for dataset in datasets:
            view_defn = "SELECT name, email from {0}".format(
                self.cbas_util.format_name(dataset.full_name))
            view_name = '`'+ generate_random_entity_name(type="view")+'`'

            if not self.cbas_util.create_analytics_view(
                self.columnar_cluster,
                view_name,
                view_defn):
                self.fail("Unable to create a view.")

            cmd = "Truncate dataset {}".format(view_name)
            status, metrics, errors, result, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, cmd)
            if validate_error:
                if not self.cbas_util.validate_error_and_warning_in_response(status, errors, error_message, error_code):
                    self.fail("Did not get an appropriate error in truncating view.")

            if with_synonym:
                syn_error_message = str(self.input.param("syn_error_message", None))
                syn_error_code = self.input.param("syn_error_code", None)
                synonym_name = '`' + generate_random_entity_name(type="syn") + '`'
                self.cbas_util.create_analytics_synonym(
                    self.columnar_cluster,
                    synonym_name,
                    view_name)
                cmd = "Truncate dataset {}".format(synonym_name)
                status, metrics, errors, result, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                    self.columnar_cluster, cmd)
                if not self.cbas_util.validate_error_and_warning_in_response(
                        status,
                        errors,
                        syn_error_message,
                        syn_error_code):
                    self.fail("Did not get an appropriate error in truncating synonym on view.")

    def test_truncate_with_rbac(self):
        error_status_code = self.input.param("error_status_code", 403)
        error_type = self.input.param("error_type", "")
        error_message = self.input.param("error_message", "")
        role = "organizationMember"
        setup_capella_api = CapellaAPIv2(self.pod.url_public, self.tenant.api_secret_key,
                                         self.tenant.api_access_key, self.tenant.user,
                                         self.tenant.pwd)
        name = "Test_User_" + str(1)
        usrname = self.tenant.user.split('@')
        username = usrname[0] + "+" + ''.join(random.choices(string.ascii_letters + string.digits, k=9)) + "@" + usrname[1]
        create_user_resp = setup_capella_api.create_user(self.tenant.id,
                                                         name,
                                                         username,
                                                         "Password@123",
                                                         [role])
        self.log.info("User creation response - {}".format(create_user_resp.content))
        self.columnarAPIrole = ColumnarAPI(self.pod.url_public, '', '', username,
                                       "Password@123")

        cmd = "Truncate dataset collection1"
        resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                      self.tenant.project_id,
                                                      self.columnar_cluster.instance_id,
                                                      cmd)
        assert resp.status_code == error_status_code, "Incorrect status code."
        resp_content = str(resp.content)
        assert error_type in resp_content, "Incorrect error type."
        assert error_message in resp_content, "Incorrect error message."

    def test_truncate_external_datasets(self):
        file_format = "json"
        error_code = self.input.param("error_code", "")
        error_message = self.input.param("error_message", "")
        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            external_collection_file_formats=[file_format])
        dataset_properties = self.columnar_spec["external_dataset"][
            "external_dataset_properties"][0]
        dataset_properties["include"] = "*.{0}".format(file_format)
        dataset_properties["path_on_external_container"] = (
            self.input.param("path_on_external_container",
                             "level_1_folder_1"))
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.columnar_cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        datasets = self.cbas_util.get_all_dataset_objs("external")
        for dataset in datasets:
            cmd = "Truncate dataset {}".format(dataset.full_name)
            status, metrics, errors, result, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, cmd)
            if not self.cbas_util.validate_error_and_warning_in_response(
                    status,
                    errors,
                    error_message,
                    error_code):
                self.fail("Did not get an appropriate error in truncating external dataset.")
