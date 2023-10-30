# coding=utf-8
"""
@author: Abhay Aggarwal(abhay.aggrawal@couchbase.com)

End-to-End test for Goldfish Private Preview
Assumption for test -
1) CBAS Entities -
    a) Dataverse - 5
    b) Links -
        I) Remote links -
            i) None encryption - 1
            ii) Half encryption - 1
            iii) Full encryption with password - 1
            iv) Full encryption with secret key - 1
            v) Full encryption with encrypted key - 1
        II) External Links
            i)S3 - 1
        III) Kafka Links
            i) MongoDB - 1
            ii) DynamoDB - 1
            iii) Cassandra - 1
    c) Standalone collections -
        I) Ingesting from S3 - 1
        II) Ingesting from MongoDB - 1
        III) Ingesting from Cassandra - 1
        IV) Ingesting from DynamoDb - 1
        V) Ingestion from remote couchbase bucket - 1
    d) Remote collection - 1
    e) External collection
        I) On S3 - 1
    f) Index - 1 on each collection
    g) Copy to S3 - 1 query for each collection
"""

import random
import time
from Queue import Queue
from threading import Thread

from CbasLib.CBASOperations import CBASHelper
from CbasLib.cbas_entity import ExternalDB
from Goldfish.goldfish_base import GoldFishBaseTest
from cbas_utils.cbas_utils import StandaloneCollectionLoader
from cbas_utils.cbas_utils import CbasUtil, External_Dataset, Standalone_Dataset


class GoldfishE2E(GoldFishBaseTest):

    def setUp(self):
        super(GoldfishE2E, self).setUp()

        self.clusters = self.list_all_clusters()

        if not self.gf_spec_name:
            self.gf_spec_name = "end2end.privatePreviewSpec"

        self.gf_spec = self.cbas_util.get_goldfish_spec(self.gf_spec_name)

        self.source_s3_regions = self.input.param("source_s3_regions", None)
        self.source_s3_bucket = self.input.param("source_s3_bucket", None)
        self.mongo_connection_uri = self.input.param("mongo_connection_uri", None)
        self.dynamo_access_key_id = self.input.param("dynamo_access_key_id", None)
        self.dynamo_security_access_key = self.input.param("dynamo_security_access_key", None)
        self.dynamo_regions = self.input.param("dynamo_regions", None)
        self.rds_hostname = self.input.param("rds_hostname", None)
        self.rds_port = self.input.param("rds_port", None)
        self.rds_username = self.input.param("rds_username", None)
        self.rds_password = self.input.param("rds_password", None)
        self.rds_server_id = self.input.param("rds_server_id", None)
        self.num_of_CRUD_on_datasets = self.input.param("num_of_CRUD_on_datasets", 5)
        self.num_iterations = self.input.param("num_iterations", 1)

        mongo_collection = self.input.param("mongo_collection", "").split('|')
        dynamo_collection = self.input.param("dynamo_collection", "").split('|')
        rds_collection = self.input.param("rds_collection", "").split('|')

        self.include_external_collections = dict()
        if len(mongo_collection) > 0:
            self.include_external_collections["mongo"] = mongo_collection
        if len(dynamo_collection) > 0:
            self.include_external_collections["dynamo"] = dynamo_collection
        if len(rds_collection) > 0:
            self.include_external_collections["rds"] = rds_collection

        self.run_concurrent_query = self.input.param("run_query", False)
        if self.run_concurrent_query:
            self.start_query_task()

        self.doc_count_per_format = {
            "json": 7400000, "parquet": 7300000,
            "csv": 7400000, "tsv": 7400000}

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)

        if self.run_concurrent_query:
            self.stop_query_task()

        super(GoldfishE2E, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage="Teardown")

    def update_external_link_spec(self, cbas_spec):
        """
        Create external link spec based on values of s3, asure, gcp.
        Current only support for S3 for private preview
        """
        external_link_properties = list()
        if self.source_s3_regions:
            for aws_region in self.source_s3_regions.split("|"):
                external_link_properties.append({
                    "type": "s3", "region": aws_region,
                    "accessKeyId": self.input.param("aws_access_key"),
                    "secretAccessKey": self.input.param("aws_secret_key")
                })
        cbas_spec["external_link"]["properties"] = external_link_properties

    def update_remote_link_spec(self, cbas_spec):
        """
        Create remote link spec based on secondary cluster.
        """
        remote_link_properties = list()
        for encryption in ["none"]:
            cluster = random.choice(self.to_clusters)
            remote_link_properties.append(
                {"type": "couchbase", "hostname": cluster.master.ip,
                 "username": cluster.master.rest_username,
                 "password": cluster.master.rest_password,
                 "encryption": encryption})
        cbas_spec["remote_link"]["properties"] = remote_link_properties

    def update_kafka_link_spec(self, cbas_spec):
        """
        Create kafka link specs for mongo, dynamo, rds.
        """
        if self.mongo_connection_uri:
            mongo_connection_uri_list = self.mongo_connection_uri.split('|')
            cbas_spec["kafka_link"]["external_database_details"]["mongo"] = list()
            for url in mongo_connection_uri_list:
                mongo_obj = ExternalDB(db_type="mongo",
                                       mongo_connection_uri=url)
                cbas_spec["kafka_link"]["external_database_details"]["mongo"].append(
                    mongo_obj.get_source_db_detail_object_for_kafka_links())
            cbas_spec["kafka_link"]["database_type"].append("mongo")

        if self.dynamo_regions and self.dynamo_access_key_id and self.dynamo_security_access_key:
            dynamo_regions = self.dynamo_regions.split("|")
            cbas_spec["kafka_link"]["external_database_details"]["dynamo"] = list()
            for region in dynamo_regions:
                dynamo_obj = ExternalDB(db_type="dynamo",
                                        dynamo_access_key=self.dynamo_access_key_id,
                                        dynamo_secret_key=self.dynamo_security_access_key,
                                        dynamo_region=region
                                        )
                cbas_spec["kafka_link"]["external_database_details"]["dynamo"].append(
                    dynamo_obj.get_source_db_detail_object_for_kafka_links())
                cbas_spec["kafka_link"]["database_type"].append("dynamo")

        if self.rds_port and self.rds_hostname and self.rds_password and self.rds_username and self.rds_server_id:
            cbas_spec["kafka_link"]["external_database_details"]["rds"] = list()
            rds_obj = ExternalDB(db_type="rds", rds_port=self.rds_port, rds_hostname=self.rds_hostname,
                                 rds_password=self.rds_password, rds_username=self.rds_username,
                                 rds_server_id=self.rds_server_id)
            cbas_spec["kafka_link"]["external_database_details"]["rds"].append(
                rds_obj.get_source_db_detail_object_for_kafka_links())
            cbas_spec["kafka_link"]["database_type"].append("rds")

    def update_external_dataset_spec(self, cbas_spec):
        """
        Create external collection on S3
        """
        external_dataset_properties_list = list()
        for file_format in ["json", "csv", "tsv", "parquet"]:
            external_dataset_properties = {
                "external_container_name": self.source_s3_bucket,
                "path_on_external_container": None,
                "file_format": file_format,
                "include": ["*.{0}".format(file_format)],
                "exclude": None,
                "region": self.source_s3_regions,
                "object_construction_def": None,
                "redact_warning": None,
                "header": None,
                "null_string": None,
                "parse_json_string": 0,
                "convert_decimal_to_double": 0,
                "timezone": ""
            }
            if file_format in ["csv", "tsv"]:
                external_dataset_properties.update({
                    "object_construction_def": (
                        "address string,avgrating double,city string,country string,"
                        "email string,freebreakfast boolean,freeparking boolean,"
                        "name string,phone string,price double,publiclikes string,"
                        "reviews string,`type` string,url string,extra string"),
                    "redact_warning": None,
                    "header": True,
                    "null_string": "",
                })
            if file_format == "parquet":
                external_dataset_properties.update({
                    "parse_json_string": "",
                    "convert_decimal_to_double": 1,
                    "timezone": "GMT"
                })
            external_dataset_properties_list.append(external_dataset_properties)
        cbas_spec["external_dataset"][
            "external_dataset_properties"] = external_dataset_properties_list

    def update_standalone_collection_spec(self, cbas_spec):
        """
        Create standalone collection specs.
        Based on test-plan currently creating standalone collection on s3, kafka links and
        """
        cbas_spec["standalone_dataset"]["data_source"] = ["s3", None]
        dataset_properties_list = list()
        for file_format in ["json", "csv", "tsv", "parquet"]:
            dataset_properties = {
                "external_container_name": self.source_s3_bucket,
                "path_on_external_container": None,
                "file_format": file_format,
                "include": ["*.{0}".format(file_format)],
                "exclude": "",
                "region": self.source_s3_regions,
                "object_construction_def": None,
                "redact_warning": None,
                "header": None,
                "null_string": None,
                "parse_json_string": 0,
                "convert_decimal_to_double": 0,
                "timezone": ""
            }

            if file_format in ["csv", "tsv"]:
                dataset_properties.update({
                    "object_construction_def": (
                        "address string,avgrating double,city string,country string,"
                        "email string,freebreakfast boolean,freeparking boolean,"
                        "name string,phone string,price double,publiclikes string,"
                        "reviews string,`type` string,url string,extra string"),
                    "redact_warning": None,
                    "header": True,
                    "null_string": "",
                })

            if file_format == "parquet":
                dataset_properties.update({
                    "parse_json_string": "",
                    "convert_decimal_to_double": 1,
                    "timezone": "GMT"
                })

            dataset_properties_list.append(dataset_properties)
        cbas_spec["standalone_dataset"]["standalone_collection_properties"] = dataset_properties_list

    def update_kafka_dataset_spec(self, cbas_spec):
        """
        Update the kafka datasourece in template
        """
        cbas_spec["kafka_dataset"]["data_source"] = ["mongo", "dynamo", "rds"]

    def run_random_queries_on_dataset(self, cluster, query_list, collection_name, dataverse_name,
                                      time_for_query_in_mins):
        """
        Execute queries for a specified time
        """
        start_time = time.time()
        while True:
            collection_full_name = "{0}.{1}".format(
                CBASHelper.format_name(dataverse_name),
                CBASHelper.format_name(collection_name))
            query = random.choice(query_list).format(collection_full_name)
            self.log.info("Running: {0}".format(query))
            status, metrics, errors, results, _ = cluster.cbas_util.execute_statement_on_cbas_util(
                cluster, query, username=None, password=None, timeout=300,
                analytics_timeout=300)
            if status != "success":
                self.log.error(str(errors))
            if time.time() - start_time > time_for_query_in_mins * 60:
                break

    def start_thread_processes(self, cluster, jobs_queue, start=True, aysnc_run=True):
        """
        Start and stop the thread processes
        """
        if start:
            cluster.cbas_util.run_jobs_in_parallel(
                jobs_queue, cluster.results, self.sdk_clients_per_user, async_run=aysnc_run)
        if not start:
            jobs_queue.join()

    def run_queries_on_standalone_collections(self, cluster):
        """
        Runs queries in paralllel on standalone collection
        """
        query_list = ["Select * from {0} where name like \"a%\" or name like \"r%\";",
                      "Select * from {0} Limit 10;",
                      "SELECT AVG(rating) AS averageRating FROM {0};"]

        standalone_collections = cluster.cbas_util.list_all_dataset_objs(
            "standalone")
        for standalone_collection in standalone_collections:
            cluster.jobs.put((
                self.run_random_queries_on_dataset,
                {"cluster": cluster, "query_list": query_list, "collection_name": standalone_collection.name,
                 "dataverse_name": standalone_collection.dataverse_name, "time_for_query_in_mins": 1}
            ))

    def perform_copy_and_crud_on_standalone_collection(self, cluster, document_size=10000, no_of_docs=100):
        """
        Ingest data from external datasources like S3, GCP and Azure into
        standalone collection.
        Perform crud on standalone collection
        """
        standalone_loader = StandaloneCollectionLoader(cluster.cbas_util, self.use_sdk_for_cbas)
        standalone_collections = cluster.cbas_util.list_all_dataset_objs(
            "standalone")
        for standalone_collection in standalone_collections:
            if standalone_collection.data_source in ["s3", "azure", "gcp"]:
                cluster.jobs.put((
                    cluster.cbas_util.copy_from_external_resource_into_standalone_collection,
                    {"cluster": cluster, "collection_name": standalone_collection.name,
                     "aws_bucket_name": standalone_collection.dataset_properties["external_container_name"],
                     "external_link_name": standalone_collection.link_name,
                     "dataverse_name": standalone_collection.dataverse_name,
                     "files_to_include": standalone_collection.dataset_properties["include"],
                     "file_format": standalone_collection.dataset_properties["file_format"],
                     "type_parsing_info": standalone_collection.dataset_properties["object_construction_def"],
                     "path_on_aws_bucket": standalone_collection.dataset_properties["path_on_external_container"],
                     "header": standalone_collection.dataset_properties["header"],
                     "null_string": standalone_collection.dataset_properties["null_string"],
                     "files_to_exclude": standalone_collection.dataset_properties["exclude"],
                     "parse_json_string": standalone_collection.dataset_properties["parse_json_string"],
                     "convert_decimal_to_double": standalone_collection.dataset_properties["convert_decimal_to_double"],
                     "timezone": standalone_collection.dataset_properties["timezone"]}
                ))
            elif standalone_collection.data_source is None:
                cluster.jobs.put((
                    standalone_loader.load_doc_to_standalone_collection,
                    {'cluster': cluster, 'collection_name': standalone_collection.name,
                     'dataverse_name': standalone_collection.dataverse_name, 'no_of_docs': no_of_docs,
                     'document_size': document_size, 'batch_size': 25, 'max_concurrent_batches': 5}
                ))

    def check_dataset_for_completion(self, cluster):
        """
        Validate the initial load for different type of collections from different sources.
        Sources include s3, mongo, dynamo, rds, and remote links
        """
        for dataset in cluster.cbas_util.list_all_dataset_objs("external"):
            item_count = cluster.cbas_util.get_num_items_in_cbas_dataset(cluster, dataset.full_name,
                                                            timeout=3600, analytics_timeout=3600)
            if item_count[0] != self.doc_count_per_format[dataset.dataset_properties["file_format"]]:
                self.log.error("Doc count mismatch for external collection: {}".format(dataset.full_name))

        for dataset in cluster.cbas_util.list_all_dataset_objs("standalone"):
            if dataset.data_source is None:
                data_count = cluster.cbas_util.get_num_items_in_cbas_dataset(cluster, dataset.full_name,
                                                            timeout=3600, analytics_timeout=3600)[0]
                if data_count == 0:
                    self.log.error("Doc count mismatch for external collection: {}".format(dataset.full_name))
                else:
                    self.log.info("No of CRUD Docs in {0}: {1}".format(dataset.full_name, data_count))
            elif dataset.data_source is 's3':
                item_count = cluster.cbas_util.get_num_items_in_cbas_dataset(cluster, dataset.full_name,
                                                                             timeout=3600, analytics_timeout=3600)
                if item_count[0] != self.doc_count_per_format[dataset.dataset_properties["file_format"]]:
                    self.log.error("Doc count mismatch for external collection: {}".format(dataset.full_name))

            elif dataset.data_source in ['dynamo', 'mongo', 'rds']:
                data_count =  cluster.cbas_util.get_num_items_in_cbas_dataset(cluster, dataset.full_name,
                                                                   timeout=3600, analytics_timeout=3600)[0]
                if data_count == 0:
                    self.log.error("Doc count mismatch for external collection: {}".format(dataset.full_name))
                else:
                    self.log.info("Docs in kafka dataset {0}: {1}".format(dataset.full_name, data_count))
    def update_goldfish_spec(self):
        """
        Update the goldfish spec.
        """
        # self.generate_bucket_obj_for_remote_cluster_obj()
        self.update_external_link_spec(self.gf_spec)
        # self.update_remote_link_spec(self.gf_spec)
        self.update_kafka_link_spec(self.gf_spec)
        self.update_external_dataset_spec(self.gf_spec)
        self.update_standalone_collection_spec(self.gf_spec)
        self.update_kafka_dataset_spec(self.gf_spec)
        self.to_clusters = None

    def setup_for_test(self, cluster):
        """
        Process and create entities based on cbas_spec template
        """
        cluster.cbas_util = CbasUtil(self.task, self.use_sdk_for_cbas)
        status, msg = cluster.cbas_util.create_cbas_infra_from_spec(cluster, self.gf_spec,
                                                                    self.bucket_util, False,
                                                                    remote_clusters=self.to_clusters,
                                                                    include_collections=self.include_external_collections)
        if not status:
            self.log.error(msg)
            self.log.error("All infra are not created. Check logs for error")

        cluster.jobs = Queue()
        cluster.results = []
        self.perform_copy_and_crud_on_standalone_collection(cluster)
        self.run_queries_on_standalone_collections(cluster)
        self.start_thread_processes(cluster, cluster.jobs)

    def run_goldfishE2E(self):
        """
        Main test function
        """

        # update the specs
        self.update_goldfish_spec()

        # create the infrastructure and start the basic ingestion in all kinds of collections
        for cluster in self.clusters:
            self.setup_for_test(cluster)

        # intial cluster is 3 nodes loading 100 GB data in total per cluster
        # wait for the data loading in various standalone collections
        for cluster in self.clusters:
            self.start_thread_processes(cluster, cluster.jobs, start=False)

        # validate the data in various dataset(standalone, external and remote collection)
        for cluster in self.clusters:
            self.check_dataset_for_completion(cluster)

        # scale the instance from 3 to 5 compute units
        # scaling code to come from Sujay

        # start kafka docloader, copy to s3 and crud on standalone collection to increase the volume from 100GB to 1TB
        # utilities to be added by Sarthak in GoldfishRestAPI repo

        for cluster in self.clusters:
            self.check_dataset_for_completion(cluster)

        # scale down the instance from 5 to 3 compute unit
        # scaling code to come from Sujay

        # perform crud on collections to create and delete standalone, external and kafka collections for 2 hrs.

        # scale the instance from 3 node to 8 compute units
        # scaling code to come from Sujay

        # start kafka docloader, copy to s3 and crud on standalone collection to increase the volume from 1TB to 5TB
        # utilities to be added by Sarthak in GoldfishRestAPI repo

        for cluster in self.clusters:
            self.check_dataset_for_completion(cluster)
