"""
Created on 4-April-2024

@author: umang.agrawal@couchbase.com
Parameters for test -
analytics_compute_storage_separation=True
aws_bucket_region=us-west-1
kv_quota_percent=10
cbas_quota_percent=90
services_init=kv:cbas-kv:cbas|kv:n1ql-kv:n1ql
nodes_init=2|2
num_of_clusters=2
use_sdk_for_cbas=True
sdk_clients_per_user=5
bucket_spec=analytics.default
num_buckets=2,num_scopes=2,num_collections=2
bucket_size=auto,num_items=100,doc_size=1024
num_db=0
num_dv=0
num_remote_links=1
num_external_links=0
num_kafka_links=0
num_remote_collections=1
num_external_collections=5 (atleast 5, this will ensure all file formats are covered)
num_standalone_collections=1
num_copy_from_S3=5 (atleast 5, this will ensure all file formats are covered)
num_kafka_collections=0
num_synonyms=0,num_indexes=0
num_copy_to_kv=2
num_copy_to_s3=2
"""
import json
import random
import time
from queue import Queue

from Columnar.build_verification_sanity.columnar_bfv_base import BFVBase
from couchbase_utils.security_utils.x509main import x509main
from columnarbasetest import ColumnarBaseTest
from cbas_utils.cbas_utils_on_prem import CBASRebalanceUtil

# Imports for Sirius data loaders
from sirius_client_framework.multiple_database_config import (
    MongoLoader, CouchbaseLoader, CassandraLoader, MySQLLoader,
    DynamoDBLoader, ColumnarLoader)
from sirius_client_framework.operation_config import WorkloadOperationConfig
from Jython_tasks.sirius_task import WorkLoadTask, DatabaseManagementTask, \
    BlobLoadTask
from sirius_client_framework.sirius_constants import SiriusCodes
from awsLib.s3_data_helper import perform_S3_operation


class E2EBuildVerification(BFVBase):
    """
    This test is meant to validate columnar server build before promoting
    it to AMI for Capella Columnar
    """

    doc_count_level_1_folder_1 = {
        "json": 1560000, "parquet": 1560000,
        "csv": 1560000, "tsv": 1560000, "avro": 1560000}
    doc_count_per_format = {
        "json": 7800000, "parquet": 7800000,
        "csv": 7800000, "tsv": 7800000, "avro": 7800000}

    def setUp(self):
        super(E2EBuildVerification, self).setUp()

        self.columnar_spec_name = self.input.param(
            "columnar_spec_name", "sanity.on-prem.bvf_e2e")
        self.columnar_spec = self.columnar_cbas_utils.get_columnar_spec(
            self.columnar_spec_name)

        for cluster_name, cluster in self.cb_clusters.items():
            if hasattr(cluster, "cbas_cc_node"):
                self.analytics_cluster = cluster
            else:
                self.remote_cluster = cluster

        self.sdk_clients_per_user = self.input.param("sdk_clients_per_user", 1)
        ColumnarBaseTest.init_sdk_pool_object(
            self.analytics_cluster, self.sdk_clients_per_user,
            self.analytics_cluster.master.rest_username,
            self.analytics_cluster.master.rest_password)
        self.rebalance_util = CBASRebalanceUtil(
            self.cluster_util, self.bucket_util, self.task, False,
            self.columnar_cbas_utils)

        self.s3_source_bucket = "columnar-functional-sanity-test-data"

        self.copy_to_s3_bucket = ""
        self.copy_to_s3_bucket_created = False

    def tearDown(self):
        self.columnar_cbas_utils.cleanup_cbas(self.analytics_cluster)
        if self.copy_to_s3_bucket_created:
            self.log.info("Deleting AWS S3 bucket for Copy to S3 - {}".format(
                self.copy_to_s3_bucket))
            if not self.s3_obj.delete_bucket(self.copy_to_s3_bucket):
                self.log.error("AWS bucket failed to delete")
        super(E2EBuildVerification, self).tearDown()

    def build_cbas_columnar_infra(self):
        num_databases = self.input.param("num_db", 1)
        num_dataverses = self.input.param("num_dv", 1)

        num_remote_links = self.input.param("num_remote_links", 0)
        num_external_links = self.input.param("num_external_links", 0)
        num_kafka_links = self.input.param("num_kafka_links", 0)

        num_remote_collections = self.input.param("num_remote_collections", 0)
        num_external_collections = self.input.param("num_external_collections",
                                                    0)

        # This defines number of standalone collections created for
        # inser/upsert/delete and for copy from S3. Eg - if it is set to 2
        # then total 4 collections will be created, 2 for
        # insert/upsert/delete and 2 for copy from S3
        num_standalone_collections = self.input.param(
            "num_standalone_collections", 0)
        num_kafka_collections = self.input.param("num_kafka_collections", 0)

        num_synonyms = self.input.param("num_synonyms", 0)
        num_indexes = self.input.param("num_indexes", 0)

        # Updating Database spec
        self.columnar_spec["database"]["no_of_databases"] = num_databases

        # Updating Dataverse/Scope spec
        self.columnar_spec["dataverse"]["no_of_dataverses"] = num_dataverses

        # Updating Remote Links Spec
        status, certificate, header = x509main(
            self.remote_cluster.master)._get_cluster_ca_cert()
        if status:
            certificate = json.loads(certificate)["cert"]["pem"]

        self.columnar_spec["remote_link"][
            "no_of_remote_links"] = num_remote_links
        self.columnar_spec["remote_link"]["properties"] = [{
            "type": "couchbase",
            "hostname": self.remote_cluster.master.ip,
            "username": self.remote_cluster.master.rest_username,
            "password": self.remote_cluster.master.rest_password,
            "encryption": "full",
            "certificate": certificate}]

        # Updating External Links Spec
        self.columnar_spec["external_link"][
            "no_of_external_links"] = num_external_links
        self.columnar_spec["external_link"]["properties"] = [{
            "type": "s3",
            "region": self.aws_bucket_region,
            "accessKeyId": self.aws_access_key,
            "secretAccessKey": self.aws_secret_key,
            "serviceEndpoint": None
        }]

        # Updating Kafka Links Spec
        self.columnar_spec["kafka_link"]["no_of_kafka_links"] = num_kafka_links

        # Updating Remote Dataset Spec
        self.columnar_spec["remote_dataset"][
            "num_of_remote_datasets"] = num_remote_collections

        # Updating External Datasets Spec
        self.columnar_spec["external_dataset"][
            "num_of_external_datasets"] = num_external_collections
        for prop in self.columnar_spec["external_dataset"][
            "external_dataset_properties"]:
            prop["external_container_name"] = self.s3_source_bucket
            prop["region"] = self.aws_bucket_region
            prop["path_on_external_container"] = ("level_{level_no:int}_"
                                                  "folder_{folder_no:int}")
            if prop["file_format"] in ["csv", "tsv"]:
                prop["object_construction_def"] = (
                    "id string,product_name string,product_link string,"
                    "product_features string,product_specs string,"
                    "product_image_links string,product_reviews string,"
                    "product_category string, price double,avg_rating double,"
                    "num_sold int,upload_date string,weight double,quantity int,"
                    "seller_name string,seller_location string,"
                    "seller_verified boolean,template_name string,mutated int,"
                    "padding string")
                prop["header"] = True
                prop["redact_warning"] = False
                prop["null_string"] = None
            elif prop["file_format"] == "parquet":
                prop["convert_decimal_to_double"] = 1
                prop["parse_json_string"] = 1
                prop["timezone"] = "GMT"

        # Update Standalone Collection Spec
        self.columnar_spec["standalone_dataset"][
            "num_of_standalone_coll"] = num_standalone_collections

        # Update Kafka Datasets Spec here.
        self.columnar_spec["kafka_dataset"][
            "num_of_ds_on_external_db"] = num_kafka_collections

        # Update Synonym Spec here
        self.columnar_spec["synonym"]["no_of_synonyms"] = num_synonyms

        # Update Index Spec here
        self.columnar_spec["index"]["no_of_indexes"] = num_indexes
        self.columnar_spec["index"]["indexed_fields"] = [
            "id:string", "id:string-product_name:string"]

        if not self.columnar_cbas_utils.create_cbas_infra_from_spec(
                cluster=self.analytics_cluster, cbas_spec=self.columnar_spec,
                bucket_util=self.bucket_util, wait_for_ingestion=False,
                remote_clusters=[self.remote_cluster]):
            self.fail("Error while creating analytics entities.")

    def test_e2e_sanity(self):

        initial_operation_config = WorkloadOperationConfig(
            start=0, end=self.num_items,
            template=SiriusCodes.Templates.PRODUCT,
            doc_size=self.doc_size,
        )

        self.log.info("Creating Buckets, Scopes and Collection on Remote "
                      "cluster.")
        self.collectionSetUp(cluster=self.remote_cluster, load_data=False, create_sdk_clients=False)

        self.log.info("Started Doc loading on remote cluster")
        copy_to_kv_bucket = random.choice(self.remote_cluster.buckets)
        copy_to_kv_dest_collections = list()

        for remote_bucket in self.remote_cluster.buckets:
            for scope_name, scope in remote_bucket.scopes.iteritems():
                if scope_name != "_system":
                    for collection_name, collection in (
                            scope.collections.iteritems()):
                        if remote_bucket.name == copy_to_kv_bucket.name:
                            copy_to_kv_dest_collections.append(
                                "{0}.{1}.{2}".format(
                                    remote_bucket.name, scope_name, collection_name))
                        else:
                            database_information = CouchbaseLoader(
                                username=self.remote_cluster.master.rest_username,
                                password=self.remote_cluster.master.rest_password,
                                connection_string="couchbases://{0}".format(
                                    self.remote_cluster.master.ip),
                                bucket=remote_bucket.name, scope=scope_name,
                                collection=collection_name, sdk_batch_size=200)
                            task_insert = WorkLoadTask(
                                task_manager=self.task_manager,
                                op_type=SiriusCodes.DocOps.BULK_CREATE,
                                database_information=database_information,
                                operation_config=initial_operation_config,
                            )
                            self.task_manager.add_new_task(task_insert)
                            self.task_manager.get_task_result(task_insert)
                            if task_insert.fail_count == self.num_items:
                                self.fail("Doc loading failed for {0}.{1}.{"
                                          "2}".format(remote_bucket.name, scope_name,
                                                      collection_name))
                            else:
                                if task_insert.fail_count > 0:
                                    self.log.error(
                                        "{0} Docs failed to load for {1}.{2}.{"
                                        "3}".format(
                                            task_insert.fail_count,
                                            remote_bucket.name, scope_name,
                                            collection_name))
                                    collection.num_items = (self.num_items -
                                                            task_insert.fail_count)
                                else:
                                    collection.num_items = self.num_items

        self.log.info("Creating analytics entities")
        self.build_cbas_columnar_infra()

        self.columnar_cbas_utils.refresh_remote_dataset_item_count(
            self.bucket_util)

        # self.log.info("Started Doc loading on External Databases")

        self.log.info("Started Doc loading on standalone collections using "
                      "Insert statements")
        standalone_collections = self.columnar_cbas_utils.get_all_dataset_objs(
            "standalone")
        for standalone_collection in standalone_collections:
            database_information = ColumnarLoader(
                username=self.analytics_cluster.master.rest_username,
                password=self.analytics_cluster.master.rest_password,
                connection_string="couchbases://{0}".format(
                    self.analytics_cluster.master.ip),
                bucket=standalone_collection.database_name,
                scope=standalone_collection.dataverse_name,
                collection=standalone_collection.name,
                hosted_on_prem=True, sdk_batch_size=200)
            task_insert = WorkLoadTask(
                task_manager=self.task_manager,
                op_type=SiriusCodes.DocOps.BULK_CREATE,
                database_information=database_information,
                operation_config=initial_operation_config,
            )
            self.task_manager.add_new_task(task_insert)
            self.task_manager.get_task_result(task_insert)
            if task_insert.fail_count == self.num_items:
                self.fail(
                    "Doc loading failed for {0}.{1}.{2}".format(
                        standalone_collection.database_name,
                        standalone_collection.dataverse_name,
                        standalone_collection.name))
            else:
                if task_insert.fail_count > 0:
                    self.log.error(
                        "{0} Docs failed to load for {1}.{2}.{"
                        "3}".format(
                            task_insert.fail_count,
                            standalone_collection.database_name,
                            standalone_collection.dataverse_name,
                            standalone_collection.name))
                    standalone_collection.num_of_items = (
                            self.num_items - task_insert.fail_count)
                else:
                    standalone_collection.num_of_items = self.num_items

        # wait for initial ingestion to complete.
        self.log.info("Waiting for initial ingestion into Remote dataset")
        for dataset in self.columnar_cbas_utils.get_all_dataset_objs("remote"):
            if not self.columnar_cbas_utils.wait_for_ingestion_complete(
                    self.analytics_cluster, dataset.full_name,
                    dataset.num_of_items, 3600):
                self.fail("FAILED: Initial ingestion into {}.".format(
                    dataset.full_name))

        self.log.info("Waiting for initial ingestion into Standalone "
                      "datasets (Insert Statements)")
        for dataset in standalone_collections:
            if not self.columnar_cbas_utils.wait_for_ingestion_complete(
                    self.analytics_cluster, dataset.full_name,
                    dataset.num_of_items, 3600):
                self.fail("FAILED: Initial ingestion into {}.".format(
                    dataset.full_name))

        self.log.info("Verifying doc count in external collections")
        query = "select count(*) from {} where level_no=1 and folder_no=1"
        for dataset in self.columnar_cbas_utils.get_all_dataset_objs(
                "external"):
            self.log.debug("Executing query - {}".format(
                query.format(dataset.full_name)))
            status, _, error, result, _ = \
                self.columnar_cbas_utils.execute_statement_on_cbas_util(
                    self.analytics_cluster, query.format(dataset.full_name)
                )
            file_format = dataset.dataset_properties["file_format"]

            if status != "success":
                self.fail("Query execution failed with error - {}".format(
                    error))
            elif (result[0]["$1"] !=
                  E2EBuildVerification.doc_count_level_1_folder_1[file_format]):
                self.fail(
                    "Doc count mismatch. Expected - {0}, Actual - {1}".format(
                        E2EBuildVerification.doc_count_level_1_folder_1[
                            file_format], result[0]["$1"]))

        self.log.info("Rebalance-In a KV+CBAS node in analytics cluster")
        rebalance_task, self.available_servers = self.rebalance_util.rebalance(
            cluster=self.analytics_cluster, cbas_nodes_in=1,
            available_servers=self.available_servers,
            in_node_services="kv,cbas")
        if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                rebalance_task, self.analytics_cluster, True, True):
            self.fail("Error while rebalance-In KV+CBAS node in analytics "
                      "cluster")

        # Update Standalone Datasets Spec for copy from S3
        self.columnar_spec["standalone_dataset"]["num_of_standalone_coll"] =\
            self.input.param("num_copy_from_S3", 5)
        self.columnar_spec["standalone_dataset"]["data_source"] = ["s3"]
        self.columnar_spec["standalone_dataset"][
            "standalone_collection_properties"] = self.columnar_spec[
            "external_dataset"]["external_dataset_properties"]

        if not self.columnar_cbas_utils.create_standalone_dataset_from_spec(
                self.analytics_cluster, self.columnar_spec):
            self.fail("Error while creating standalone collections for "
                      "insert statements")

        self.log.info("Starting Copy from S3 into standalone collections")
        copy_from_s3_collections = self.columnar_cbas_utils.get_all_dataset_objs(
            "standalone")
        # Remove all the standalone datasets from datasets list, in which
        # docs were inserted using Insert query.
        for collection in copy_from_s3_collections:
            if collection.data_source != "s3":
                copy_from_s3_collections.remove(collection)

        jobs = Queue()
        results = []
        for standalone_coll in copy_from_s3_collections:
            jobs.put(
                (
                    self.columnar_cbas_utils.copy_from_external_resource_into_standalone_collection,
                    {
                        "cluster": self.analytics_cluster,
                        "collection_name": standalone_coll.name,
                        "aws_bucket_name": standalone_coll.dataset_properties[
                            "external_container_name"],
                        "external_link_name": standalone_coll.link_name,
                        "dataverse_name": standalone_coll.dataverse_name,
                        "database_name": standalone_coll.database_name,
                         "files_to_include": standalone_coll.dataset_properties[
                             "include"],
                         "file_format": standalone_coll.dataset_properties[
                             "file_format"],
                         "type_parsing_info": standalone_coll.dataset_properties[
                             "object_construction_def"],
                         "path_on_aws_bucket": "",
                         "header": standalone_coll.dataset_properties["header"],
                         "null_string": standalone_coll.dataset_properties[
                             "null_string"],
                         "files_to_exclude": standalone_coll.dataset_properties[
                             "exclude"],
                         "parse_json_string": standalone_coll.dataset_properties[
                             "parse_json_string"],
                         "convert_decimal_to_double":
                             standalone_coll.dataset_properties[
                                 "convert_decimal_to_double"],
                         "timezone": standalone_coll.dataset_properties[
                             "timezone"]
                    }))

        self.columnar_cbas_utils.run_jobs_in_parallel(
            jobs, results, 3, async_run=False)

        if not all(results):
            self.fail("FAILED : Copy from S3 into standalone collections")

        self.log.info("Verifying doc count in standalone collections on which "
                      "copy from S3 was run")
        for dataset in copy_from_s3_collections:
            file_format = dataset.dataset_properties["file_format"]
            result = self.columnar_cbas_utils.get_num_items_in_cbas_dataset(
                self.analytics_cluster, dataset.full_name)
            if result != E2EBuildVerification.doc_count_per_format[file_format]:
                self.fail(
                    "Doc count mismatch. Expected - {0}, Actual - {1}".format(
                        E2EBuildVerification.doc_count_per_format[
                            file_format], result))
            dataset.num_of_items = result

        self.log.info("Rebalance-Out a KV+CBAS node in analytics cluster")
        rebalance_task, self.available_servers = self.rebalance_util.rebalance(
            cluster=self.analytics_cluster, cbas_nodes_out=1,
            available_servers=self.available_servers)
        if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                rebalance_task, self.analytics_cluster, True, True):
            self.fail("Error while Rebalance-Out KV+CBAS node in analytics "
                      "cluster")

        all_datasets = self.columnar_cbas_utils.get_all_dataset_objs()

        jobs = Queue()
        self.log.info("Starting Copy to KV")
        copy_to_kv_info = []
        results = []
        for i in range(self.input.param("num_copy_to_kv", 0)):
            copy_to_kv_info.append(
                {
                    "cluster": self.analytics_cluster,
                    "collection_name": random.choice(all_datasets).full_name,
                    "dest_bucket": self.columnar_cbas_utils.format_name(
                        copy_to_kv_dest_collections[i % len(
                            copy_to_kv_dest_collections)]),
                    "link_name": random.choice(
                        self.columnar_cbas_utils.get_all_link_objs(
                            "couchbase")).full_name,
                    "timeout": 3600, "analytics_timeout": 3600
                }
            )
            jobs.put(
                (
                    self.columnar_cbas_utils.copy_to_kv,
                    copy_to_kv_info[-1]
                ))
        self.columnar_cbas_utils.run_jobs_in_parallel(
            jobs, results, 3, async_run=False)
        if not all(results):
            self.fail("FAILED : Copy to KV")

        self.log.info("Verifying Copy to KV results")
        for info in copy_to_kv_info:
            cbas_doc_count = self.columnar_cbas_utils.get_num_items_in_cbas_dataset(
                self.analytics_cluster, info["collection_name"]
            )
            dest_info = self.columnar_cbas_utils.unformat_name(
                info["dest_bucket"]).split(".")
            retry = 0
            while True:
                try:
                    kv_collection_doc_count = self.bucket_util.get_total_items_count_in_a_collection(
                        self.remote_cluster, dest_info[0], dest_info[1], dest_info[2])
                    if cbas_doc_count != kv_collection_doc_count:
                        if retry < 3:
                            retry += 1
                            self.sleep(10)
                        else:
                            self.fail("Copy To KV : Doc count mismatch. Expected "
                                      "{0}, Actual {1}".format(
                                cbas_doc_count, kv_collection_doc_count))
                    else:
                        break
                except Exception as err:
                    if retry >= 3:
                        raise err
                    else:
                        retry += 1

        self.log.info("Rebalance-Swap a KV+CBAS node in analytics cluster")
        rebalance_task, self.available_servers = self.rebalance_util.rebalance(
            cluster=self.analytics_cluster, cbas_nodes_in=1, cbas_nodes_out=1,
            available_servers=self.available_servers,
            in_node_services="kv,cbas")
        if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                rebalance_task, self.analytics_cluster, True, True):
            self.fail("Error while Rebalance-Swap KV+CBAS node in analytics "
                      "cluster")

        self.log.info("Creating AWS S3 bucket for Copy to S3")
        for i in range(5):
            try:
                self.copy_to_s3_bucket = "copy-to-s3-" + str(int(
                    time.time()))
                self.log.info("Creating S3 bucket")
                self.copy_to_s3_bucket_created = self.s3_obj.create_bucket(
                    self.copy_to_s3_bucket, self.aws_bucket_region)
                break
            except Exception as e:
                self.log.error(
                    "Creating S3 bucket - {0} in region {1}. "
                    "Failed.".format(
                        self.copy_to_s3_bucket, self.aws_bucket_region))
                self.log.error(str(e))

        if not self.copy_to_s3_bucket_created:
            self.fail("Unable to create S3 bucket for Copy to S3")

        self.log.info("Starting Copy to S3")
        copy_to_s3_info = []
        results = []
        for i in range(self.input.param("num_copy_to_s3", 0)):
            copy_to_s3_info.append(
                {"cluster": self.analytics_cluster,
                 "collection_name": random.choice(all_datasets).full_name,
                 "destination_bucket": self.copy_to_s3_bucket,
                 "destination_link_name": random.choice(
                        self.columnar_cbas_utils.get_all_link_objs(
                            "s3")).full_name,
                 "path": "copy_dataset_" + str(i)}
            )
            jobs.put(
                (
                    self.columnar_cbas_utils.copy_to_s3,
                    copy_to_s3_info[-1]
                ))
        self.columnar_cbas_utils.run_jobs_in_parallel(
            jobs, results, 3, async_run=False)
        if not all(results):
            self.fail("FAILED : Copy to S3")

        path_on_external_container = "{copy_dataset:string}"
        # create external dataset on the S3 bucket
        external_dataset_obj = self.columnar_cbas_utils.create_external_dataset_obj(
            self.analytics_cluster,
            external_container_names={
                self.copy_to_s3_bucket: self.aws_bucket_region},
            paths_on_external_container=[path_on_external_container],
            file_format="json")[0]

        if not self.columnar_cbas_utils.create_dataset_on_external_resource(
                self.analytics_cluster, external_dataset_obj.name,
                external_dataset_obj.dataset_properties["external_container_name"],
                external_dataset_obj.link_name, False,
                external_dataset_obj.dataverse_name,
                external_dataset_obj.database_name,
                external_dataset_obj.dataset_properties["object_construction_def"],
                external_dataset_obj.dataset_properties["path_on_external_container"],
                external_dataset_obj.dataset_properties["file_format"],
                external_dataset_obj.dataset_properties["redact_warning"],
                external_dataset_obj.dataset_properties["header"],
                external_dataset_obj.dataset_properties["null_string"],
                external_dataset_obj.dataset_properties["include"],
                external_dataset_obj.dataset_properties["exclude"],
                external_dataset_obj.dataset_properties["parse_json_string"],
                external_dataset_obj.dataset_properties["convert_decimal_to_double"],
                external_dataset_obj.dataset_properties["timezone"],
                timeout=300,
                analytics_timeout=300):
            self.fail(
                "Failed to create external dataset on destination S3 bucket")

        self.log.info("Verifying Copy to S3 results")
        for info in copy_to_s3_info:
            statement = ("select count(*) from {0} where copy_dataset = "
                         "\"{1}\"").format(
                external_dataset_obj.full_name, info["path"])
            status, metrics, errors, result, _ = self.columnar_cbas_utils.execute_statement_on_cbas_util(
                self.analytics_cluster, statement)

            doc_count_in_dataset = self.columnar_cbas_utils.get_num_items_in_cbas_dataset(
                self.analytics_cluster, info["collection_name"])
            if result[0]['$1'] != doc_count_in_dataset:
                self.fail(
                    "Copy to S3: Document count mismatch. Expected data in S3 "
                    "{0} , Actual data in S3 {1}".format(
                        doc_count_in_dataset, result[0]['$1']))

        self.log.info(
            "Failover with add back a KV+CBAS node in analytics cluster")
        self.available_servers, kv_failover_nodes, cbas_failover_nodes = \
            self.rebalance_util.failover(
                cluster=self.analytics_cluster, cbas_nodes=1,
                action="FullRecovery", available_servers=self.available_servers)

        self.log.info("Upserting docs in Standalonne collections")
        for standalone_collection in standalone_collections:
            if not standalone_collection.data_source:
                database_information = ColumnarLoader(
                    username=self.analytics_cluster.master.rest_username,
                    password=self.analytics_cluster.master.rest_password,
                    connection_string="couchbases://{0}".format(
                        self.analytics_cluster.master.ip),
                    bucket=standalone_collection.database_name,
                    scope=standalone_collection.dataverse_name,
                    collection=standalone_collection.name,
                    hosted_on_prem=True, sdk_batch_size=200)
                task_upsert = WorkLoadTask(
                    task_manager=self.task_manager,
                    op_type=SiriusCodes.DocOps.BULK_UPDATE,
                    database_information=database_information,
                    operation_config=initial_operation_config,
                )
                self.task_manager.add_new_task(task_upsert)
                self.task_manager.get_task_result(task_upsert)
                if task_upsert.fail_count == self.num_items:
                    self.fail(
                        "Updating Docs failed for {0}".format(
                            standalone_collection.full_name))
                doc_count_in_dataset = self.columnar_cbas_utils.get_num_items_in_cbas_dataset(
                    self.analytics_cluster, standalone_collection.full_name)
                if doc_count_in_dataset < standalone_collection.num_of_items:
                    self.fail("Few docs are missing after upserting data in "
                              "standalone collection")
                standalone_collection.num_of_items = doc_count_in_dataset

        self.log.info("Rebalance-In a KV+CBAS node in analytics cluster")
        rebalance_task, self.available_servers = self.rebalance_util.rebalance(
            cluster=self.analytics_cluster, cbas_nodes_in=1,
            available_servers=self.available_servers,
            in_node_services="kv,cbas")
        if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                rebalance_task, self.analytics_cluster, True, True):
            self.fail("Error while rebalance-In KV+CBAS node in analytics "
                      "cluster")

        self.log.info("Deleting docs from Standalonne collections")
        for standalone_collection in standalone_collections:
            if not standalone_collection.data_source:
                statement = "select count(*) from {0} where avg_rating < 1;".format(
                    external_dataset_obj.full_name)
                status, metrics, errors, result, _ = self.columnar_cbas_utils.execute_statement_on_cbas_util(
                    self.analytics_cluster, statement)
                num_docs_to_be_deleted = result[0]['$1']
                doc_count_pre_deletion = standalone_collection.num_of_items
                if not self.columnar_cbas_utils.delete_from_standalone_collection(
                        cluster=self.analytics_cluster,
                        collection_name=standalone_collection.full_name,
                        where_clause="avg_rating < 1"):
                    self.fail(
                        "Error while deleting docs from standalone collection "
                        "{0}".format(standalone_collection.full_name))
                doc_count_post_deletion = self.columnar_cbas_utils.get_num_items_in_cbas_dataset(
                    self.analytics_cluster, standalone_collection.full_name)
                if ((doc_count_pre_deletion - doc_count_post_deletion) !=
                        num_docs_to_be_deleted):
                    self.fail(
                        "Docs missing in standalone collection {0}. Expected "
                        "{1}, Actual {2}".format(
                            standalone_collection.full_name,
                            doc_count_pre_deletion - num_docs_to_be_deleted,
                            doc_count_post_deletion))
                standalone_collection.num_of_items = doc_count_post_deletion

        self.log.info(
            "Failover with Rebalance-Out a KV+CBAS node in analytics cluster")
        self.available_servers, kv_failover_nodes, cbas_failover_nodes = \
            self.rebalance_util.failover(
                cluster=self.analytics_cluster, cbas_nodes=1,
                action="RebalanceOut",
                available_servers=self.available_servers)

        for dataset in all_datasets:
            doc_count = self.columnar_cbas_utils.get_num_items_in_cbas_dataset(
                self.analytics_cluster, dataset.full_name)
            if doc_count != dataset.num_of_items:
                self.fail(
                    "Docs missing in collection {0}. Expected {1}, Actual "
                    "{2}".format(
                        dataset.full_name, dataset.num_of_items, doc_count))
