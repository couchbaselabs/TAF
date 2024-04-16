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

import copy
import random
import time

import requests
from Queue import Queue


from couchbase_utils.capella_utils.dedicated import CapellaUtils
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI
from CbasLib.CBASOperations import CBASHelper
from CbasLib.cbas_entity_columnar import ExternalDB
from Goldfish.columnar_base import GoldFishBaseTest
from cbas_utils.cbas_utils import StandaloneCollectionLoader
from cbas_utils.cbas_utils import CbasUtil, External_Dataset, Standalone_Dataset, Remote_Dataset
from goldfish_utils.goldfish_utils import GoldfishUtils
from goldfishAPI.GoldfishAPIs.DocloadingAPIs.DocloadingAPIs import DocloadingAPIs


class GoldfishE2E(GoldFishBaseTest):

    def setUp(self):
        super(GoldfishE2E, self).setUp()

        self.capella_provisioned_cluster_setup()

        self.clusters = self.user.project.clusters
        self.goldfish_utils = GoldfishUtils(self.log)

        if not self.gf_spec_name:
            self.gf_spec_name = "end2end.privatePreviewSpec"

        self.gf_spec = self.cbas_util.get_columnar_spec(self.gf_spec_name)
        self.doc_loader_url = self.input.param("doc_loader_url", None)
        self.doc_loader_port = self.input.param("doc_loader_port", None)
        self.external_db_doc = self.input.param("external_db_doc", 10000)
        self.ingestion_timeout = self.input.param("ingestion_timeout", 10000)

        if self.doc_loader_url and self.doc_loader_port:
            self.doc_loader = DocloadingAPIs(self.doc_loader_url, self.doc_loader_port)

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

        self.run_concurrent_query = self.input.param("run_query", False)

        self.doc_count_per_format = {
            "json": 7400000, "parquet": 7300000,
            "csv": 7400000, "tsv": 7400000}

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def capella_provisioned_cluster_setup(self):

        self.pod.url_public = (self.pod.url_public).replace("https://api", "https://cloudapi")
        self.capellaAPI = CapellaAPI(self.pod.url_public, '', '', self.user.email, self.user.password, '')
        resp = (self.capellaAPI.create_control_plane_api_key(self.user.org_id, 'init api keys')).json()
        self.capellaAPI.cluster_ops_apis.SECRET = resp['secretKey']
        self.capellaAPI.cluster_ops_apis.ACCESS = resp['id']
        self.capellaAPI.cluster_ops_apis.bearer_token = resp['token']
        self.capellaAPI.org_ops_apis.SECRET = resp['secretKey']
        self.capellaAPI.org_ops_apis.ACCESS = resp['id']
        self.capellaAPI.org_ops_apis.bearer_token = resp['token']

        # create the first V4 API KEY WITH organizationOwner role, which will
        # be used to perform further operations on capella cluster
        resp = self.capellaAPI.org_ops_apis.create_api_key(
            organizationId=self.user.org_id,
            name=self.cbas_util.generate_name(),
            organizationRoles=["organizationOwner"],
            description=self.cbas_util.generate_name())
        if resp.status_code == 201:
            self.capella_cluster_keys = resp.json()
        else:
            self.fail("Error while creating API key for organization owner")

        cluster_name = self.cbas_util.generate_name()
        self.expected_result = {
            "name": cluster_name,
            "description": None,
            "cloudProvider": {
                "type": "aws",
                "region": "us-east-1",
                "cidr": CapellaUtils.get_next_cidr() + "/20"
            },
            "couchbaseServer": {
                "version": self.input.capella.get(
                    "capella_server_version", "7.2")
            },
            "serviceGroups": [
                {
                    "node": {
                        "compute": {
                            "cpu": 4,
                            "ram": 16
                        },
                        "disk": {
                            "storage": 100,
                            "type": "gp3",
                            "iops": 7000,
                            "autoExpansion": "on"
                        }
                    },
                    "numOfNodes": 3,
                    "services": [
                        "data"
                    ]
                }
            ],
            "availability": {
                "type": "single"
            },
            "support": {
                "plan": "basic",
                "timezone": "GMT"
            },
            "currentState": None,
            "audit": {
                "createdBy": None,
                "createdAt": None,
                "modifiedBy": None,
                "modifiedAt": None,
                "version": None
            }
        }
        cluster_created = False
        while not cluster_created:
            resp = self.capellaAPI.cluster_ops_apis.create_cluster(
                self.user.org_id, self.user.project.project_id, cluster_name,
                self.expected_result['cloudProvider'],
                self.expected_result['couchbaseServer'],
                self.expected_result['serviceGroups'],
                self.expected_result['availability'],
                self.expected_result['support'])
            if resp.status_code == 202:
                cluster_created = True
            else:
                self.expected_result['cloudProvider'][
                    "cidr"] = CapellaUtils.get_next_cidr() + "/20"
        self.cluster_id = resp.json()['id']
        # wait for cluster to be deployed

        wait_start_time = time.time()
        health_status = "deploying"
        while time.time() < wait_start_time + 1500:
            resp = (self.capellaAPI.cluster_ops_apis.fetch_cluster_info(self.user.org_id,
                                                                        self.user.project.project_id,
                                                                        self.cluster_id)).json()
            health_status = resp[
                "currentState"]
            if health_status == "healthy":
                self.log.info("Successfully deployed remote cluster")
                self.remote_cluster_connection_string = resp["connectionString"]
                break
            else:
                self.log.info("Cluster is still deploying, waiting 15 seconds")
                time.sleep(15)

        if health_status != "healthy":
            self.fail("Unable to deploy a provisioned cluster for remote links")

        # allow 0.0.0.0/0 to allow access from anywhere
        resp = self.capellaAPI.cluster_ops_apis.add_CIDR_to_allowed_CIDRs_list(self.user.org_id,
                                                                               self.user.projects.project_id,
                                                                               self.cluster_id, "0.0.0.0/0")
        if resp.status_code == 201:
            self.log.info("Added allowed IP 0.0.0.0/0")
        else:
            self.fail("Failed to add allowed IP")

        # create a database access credentials
        access = [{
            "privileges": ["data_reader",
                           "data_writer"],
            "resources": {}
        }]

        self.remote_cluster_username = "Administrator"
        self.remote_cluster_password = "Password#123"
        resp = self.capellaAPI.cluster_ops_apis.create_database_user(self.user.org_id,
                                                                     self.user.project.project_id,
                                                                     self.cluster_id, "Administrator", access,
                                                                     "Password#123")
        if resp.status_code == 201:
            self.log.info("Database user added")
        else:
            self.fail("Failed to add database user")

        # creating bucket scope and collection to pump data
        bucket_name = "hotel"
        scope = None
        collection = None

        resp = self.capellaAPI.cluster_ops_apis.create_bucket(self.user.org_id,
                                                              self.user.project.project_id, self.cluster_id,
                                                              bucket_name, "couchbase", "couchstore", 2000, "seqno",
                                                              "majorityAndPersistActive", 0, True, 1000000)
        if resp.status_code == 201:
            self.bucket_id = resp.json()["id"]
            self.log.info("Bucket created successfully")
        else:
            self.fail("Error creating bucket in remote_cluster")

        if bucket_name and scope and collection:
            self.remote_collection = "{}.{}.{}".format(bucket_name, scope, collection)
        else:
            self.remote_collection = "{}.{}.{}".format(bucket_name, "_default", "_default")
        resp = self.capellaAPI.cluster_ops_apis.get_cluster_certificate(self.user.org_id,
                                                                        self.user.project.project_id,
                                                                        self.cluster_id)
        if resp.status_code == 200:
            self.remote_cluster_certificate = (resp.json())["certificate"]
        else:
            self.fail("Failed to get cluster certificate")

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)

        # super(GoldfishE2E, self).tearDown()
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
        remote_link_properties.append(
            {"type": "couchbase", "hostname": self.remote_cluster_connection_string,
             "username": self.remote_cluster_username,
             "password": self.remote_cluster_password,
             "encryption": "full",
             "certificate": self.remote_cluster_certificate})
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
            dynamo_regions = self.dynamo_regions
            cbas_spec["kafka_link"]["external_database_details"]["dynamo"] = list()
            dynamo_obj = ExternalDB(db_type="dynamo",
                                    dynamo_access_key=self.dynamo_access_key_id,
                                    dynamo_secret_key=self.dynamo_security_access_key,
                                    dynamo_region=dynamo_regions
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
        mongo_collection = self.input.param("mongo_collection", None)
        mongo_collection = mongo_collection.split('|') if mongo_collection else None
        dynamo_collection = self.input.param("dynamo_collection", None)
        dynamo_collection = dynamo_collection.split('|') if dynamo_collection else None
        rds_collection = self.input.param("rds_collection", None)
        rds_collection = rds_collection.split('|') if rds_collection else None

        self.include_external_collections = dict()
        if mongo_collection and len(mongo_collection) > 0:
            self.include_external_collections["mongo"] = mongo_collection
        if dynamo_collection and len(dynamo_collection) > 0:
            self.include_external_collections["dynamo"] = dynamo_collection
        if rds_collection and len(rds_collection) > 0:
            self.include_external_collections["rds"] = rds_collection
        cbas_spec["kafka_dataset"]["include_external_collections"] = self.include_external_collections

    def update_remote_dataset_spec(self, cbas_spec):
        remote_collection = [self.remote_collection]
        self.include_external_collections["remote"] = remote_collection
        cbas_spec["remote_dataset"]["include_collections"] = remote_collection

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

    def run_queries_on_collections(self, cluster, no_of_parallel_query=3, time_for_query=0):
        """
        Runs queries in paralllel on standalone collection
        """
        query_list = ["Select * from {0} where email like \"a%\" or name like \"r%\" Limit 100;",
                      "Select * from {0} Limit 100;",
                      "SELECT AVG(r.rating.overall) AS average_overall_rating FROM {0} "
                      "AS b UNNEST b.reviews AS r WHERE ARRAY_COUNT(b.reviews) > 0;"]

        datasets = cluster.cbas_util.get_all_dataset_objs()
        for i in range(no_of_parallel_query):
            collection = random.choice(datasets)
            cluster.jobs.put((
                self.run_random_queries_on_dataset,
                {"cluster": cluster, "query_list": query_list, "collection_name": collection.name,
                 "dataverse_name": collection.dataverse_name, "time_for_query_in_mins": time_for_query}
            ))

    def start_thread_processes(self, cbas_utils, results, jobs_queue, start=True, async_run=True):
        """
        Start and stop the thread processes
        """
        if start:
            cbas_utils.run_jobs_in_parallel(
                jobs_queue, results, self.sdk_clients_per_user, async_run=async_run)
        if not start:
            jobs_queue.join()

    def perform_copy_and_load_on_standalone_collection(self, cluster, document_size=10000, no_of_docs=1000):
        """
        Ingest data from external datasources like S3, GCP and Azure into
        standalone collection.
        Perform crud on standalone collection
        """
        standalone_loader = StandaloneCollectionLoader(cluster.cbas_util, self.use_sdk_for_cbas)
        standalone_collections = cluster.cbas_util.get_all_dataset_objs(
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

    def validate_data_in_standalone_collection_with_source_none(self, cluster, dataset, no_of_docs=None):
        """
        Validate doc in standalone dataset where docs are loaded using insert, upsert and delete statements
        """
        data_count = cluster.cbas_util.get_num_items_in_cbas_dataset(cluster, dataset.full_name,
                                                                     timeout=3600, analytics_timeout=3600)[
            0]
        valid_count = 0 if not no_of_docs else no_of_docs
        if data_count != valid_count:
            return False, valid_count, data_count[0]
        return True, valid_count, data_count[0]

    def validate_data_in_external_dataset(self, cluster, dataset):
        """
        Validate data in datasets made from S3 links
        """
        item_count = cluster.cbas_util.get_num_items_in_cbas_dataset(cluster, dataset.full_name,
                                                                     timeout=3600, analytics_timeout=3600)
        if item_count[0] != self.doc_count_per_format[dataset.dataset_properties["file_format"]]:
            return False, self.doc_count_per_format[dataset.dataset_properties["file_format"]], item_count[0]
        return True, self.doc_count_per_format[dataset.dataset_properties["file_format"]], item_count[0]

    def validate_data_in_kafka_dataset(self, cluster, dataset):
        """
        Validate data in kafka datasets of mongo, dynamo and rds
        """
        external_collection_name = dataset.external_collection_name
        if dataset.data_source == "mongo":
            database = external_collection_name.split('.')[0]
            collection = external_collection_name.split('.')[1]
            source_doc_count = (self.doc_loader.get_mongo_doc_count('', database, collection,
                                                                    atlas_url=self.mongo_connection_uri).json())[
                "count"]
            dest_doc_count = cluster.cbas_util.get_num_items_in_cbas_dataset(cluster, dataset.full_name,
                                                                             timeout=3600, analytics_timeout=3600)[0]

            if source_doc_count != dest_doc_count:
                return False, source_doc_count, dest_doc_count[0]
            return True, source_doc_count, dest_doc_count[0]

        if dataset.data_source == "dynamo":
            source_doc_count = (self.doc_loader.count_dynamo_documents(self.dynamo_access_key_id,
                                                                       self.dynamo_security_access_key,
                                                                       self.dynamo_regions,
                                                                       external_collection_name).json())["count"]
            dest_doc_count = cluster.cbas_util.get_num_items_in_cbas_dataset(cluster, dataset.full_name,
                                                                             timeout=3600, analytics_timeout=3600)[0]
            if source_doc_count != dest_doc_count:
                return False, source_doc_count, dest_doc_count[0]
            return True, source_doc_count, dest_doc_count[0]

        if dataset.data_source == "rds":
            database = external_collection_name.split('.')[0]
            table = external_collection_name.split('.')[1]
            source_doc_count = (self.doc_loader.count_mysql_documents(self.rds_hostname, self.rds_port,
                                                                      self.rds_username, self.rds_password,
                                                                      database, table)).json()["count"]
            dest_doc_count = cluster.cbas_util.get_num_items_in_cbas_dataset(cluster, dataset.full_name,
                                                                             timeout=3600, analytics_timeout=3600)[0]
            if source_doc_count != dest_doc_count:
                return False, source_doc_count, dest_doc_count[0]
            return True, source_doc_count, dest_doc_count[0]

    def validate_data_in_remote_dataset(self, cluster, dataset):
        """
        Validate doc in remote datasets
        """
        item_count = cluster.cbas_util.get_num_items_in_cbas_dataset(cluster, dataset.full_name,
                                                                     timeout=3600, analytics_timeout=3600)
        resp = self.capellaAPI.cluster_ops_apis.fetch_bucket_info(self.user.org_id,
                                                                  self.user.project.project_id,
                                                                  self.cluster_id,
                                                                  self.bucket_id)
        if resp.status_code == 200:
            current_doc_count = (resp.json())["stats"]["itemCount"]
        if item_count[0] != current_doc_count:
            return False, current_doc_count, item_count[0]
        return True, current_doc_count, item_count[0]

    def return_doc_match_log_info(self, state, full_name, type="remote", expected=0, actual=0):
        if state:
            self.log.info("Valid doc count in {} dataset {}, Expected: {} , Actual: {}".format(type, full_name,
                                                                                               expected, actual))
        else:
            self.log.error("Mismatch doc count in {} dataset {}, Expected: {} , Actual: {}".format(type, full_name,
                                                                                                   expected, actual))

    def check_docs_in_all_datasets(self, cluster):
        """
        Validate the initial load for different type of collections from different sources.
        Sources include s3, mongo, dynamo, rds, and remote links
        """
        try:
            for dataset in cluster.cbas_util.get_all_dataset_objs("remote"):
                state, expected, actual = self.validate_data_in_remote_dataset(cluster, dataset)
                self.return_doc_match_log_info(state, dataset.full_name, expected=expected, actual=actual)

            for dataset in cluster.cbas_util.get_all_dataset_objs("external"):
                state, expected, actual = self.validate_data_in_external_dataset(cluster, dataset)
                self.return_doc_match_log_info(state, dataset.full_name, type="external", expected=expected,
                                               actual=actual)

            for dataset in cluster.cbas_util.get_all_dataset_objs("standalone"):
                if dataset.data_source is None:
                    state, expected, actual = self.validate_data_in_standalone_collection_with_source_none(cluster,
                                                                                                           dataset)
                    self.return_doc_match_log_info(state, dataset.full_name, type="standalone", expected=expected,
                                                   actual=actual)
                elif dataset.data_source is 's3':
                    state, expected, actual = self.validate_data_in_external_dataset(cluster, dataset)
                    self.return_doc_match_log_info(state, dataset.full_name, type="standalone", expected=expected,
                                                   actual=actual)
                elif dataset.data_source in ['dynamo', 'mongo', 'rds']:
                    state, expected, actual = self.validate_data_in_kafka_dataset(cluster, dataset)
                    self.return_doc_match_log_info(state, dataset.full_name, type="standalone", expected=expected,
                                                   actual=actual)
        except Exception as err:
            self.log.error("Failed to execute queries: {}".format(str(err)))

    def update_goldfish_spec(self):
        """
        Update the columnar spec.
        """
        # self.generate_bucket_obj_for_remote_cluster_obj()
        self.update_external_link_spec(self.gf_spec)
        self.update_remote_link_spec(self.gf_spec)
        self.update_kafka_link_spec(self.gf_spec)
        self.update_external_dataset_spec(self.gf_spec)
        self.update_standalone_collection_spec(self.gf_spec)
        self.update_kafka_dataset_spec(self.gf_spec)
        self.update_remote_dataset_spec(self.gf_spec)
        self.start_source_ingestion(self.external_db_doc, 10000)
        self.wait_for_source_ingestion(self.external_db_doc)

    def create_crud_standalone_collection(self, cluster, new_dataset, timeout=300):
        """
        Create standlaone collection for the crud operations
        """
        if new_dataset.data_source in ["mongo", "dynamo", "rds"]:
            return
        dataverse = None
        cluster_entities = cluster.cbas_util
        while not dataverse:
            dataverse = random.choice(cluster_entities.crud_dataverses.values())
        new_dataset.dataverse_name = dataverse_name = dataverse.name
        new_dataset.full_name = CBASHelper.format_name(new_dataset.dataverse_name, new_dataset.name)
        self.log.info("Creating dataset: {}".format(new_dataset.full_name))
        if new_dataset.dataverse_name == "Default":
            dataverse_name = None
        creation_methods = [
            "DATASET", "COLLECTION",
            "ANALYTICS COLLECTION"]
        creation_method = random.choice(creation_methods)
        storage_format = random.choice(
            ["row", "column"])
        if new_dataset.data_source in ["s3"]:
            if not cluster.cbas_util.create_standalone_collection(
                    cluster, new_dataset.name, creation_method, False,
                    dataverse_name, new_dataset.primary_key, "",
                    False, storage_format):
                self.log.info("Failed to create standalone collection: {}".format(new_dataset.full_name))
            else:
                dataverse.standalone_datasets[
                    new_dataset.name] = new_dataset
                self.validate_data_in_external_dataset(cluster, new_dataset)
        elif new_dataset.data_source is None:
            if not cluster.cbas_util.create_standalone_collection(
                    cluster, new_dataset.name, creation_method, False,
                    dataverse_name, new_dataset.primary_key, "",
                    False, storage_format):
                self.log.info("Failed to create standalone collection: {}".format(new_dataset.full_name))
            else:
                dataverse.standalone_datasets[
                    new_dataset.name] = new_dataset
                standalone_loader = StandaloneCollectionLoader(cluster.cbas_util, self.use_sdk_for_cbas)
                standalone_loader.load_doc_to_standalone_collection(cluster, new_dataset.name,
                                                                    new_dataset.dataverse_name,
                                                                    1000, 10000)
                self.validate_data_in_standalone_collection_with_source_none(cluster, new_dataset, 1000)

    def create_crud_external_collection(self, cluster, new_dataset, timeout=300):
        dataverse = None
        cluster_entities = cluster.cbas_util
        while not dataverse:
            dataverse = random.choice(cluster_entities.crud_dataverses.values())
        new_dataset.dataverse_name = dataverse_name = dataverse.name
        new_dataset.full_name = CBASHelper.format_name(new_dataset.dataverse_name, new_dataset.name)
        if dataverse_name == "Default":
            dataverse_name = None
        self.log.info("Creating dataset: {}".format(new_dataset.full_name))
        if not cluster.cbas_util.create_dataset_on_external_resource(
                cluster, new_dataset.name,
                new_dataset.dataset_properties[
                    "external_container_name"],
                new_dataset.link_name, False,
                dataverse_name,
                new_dataset.dataset_properties[
                    "object_construction_def"],
                new_dataset.dataset_properties[
                    "path_on_external_container"],
                new_dataset.dataset_properties[
                    "file_format"],
                new_dataset.dataset_properties[
                    "redact_warning"],
                new_dataset.dataset_properties[
                    "header"],
                new_dataset.dataset_properties[
                    "null_string"],
                new_dataset.dataset_properties[
                    "include"],
                new_dataset.dataset_properties[
                    "exclude"],
                new_dataset.dataset_properties[
                    "parse_json_string"],
                new_dataset.dataset_properties[
                    "convert_decimal_to_double"],
                new_dataset.dataset_properties[
                    "timezone"],
                False, False, None, None, None, None,
                timeout=timeout,
                analytics_timeout=timeout):
            self.log.error("Failed to create external dataset {0}".format(new_dataset.full_name))
        dataverse.external_datasets[
            new_dataset.name] = new_dataset
        if not self.validate_data_in_external_dataset(cluster, new_dataset):
            self.log.error("Mismatch number of items in external dataset: {}".format(new_dataset.full_name))

    def perform_collection_crud(self, cluster, time_for_crud=10, timeout=300):
        """
        Create and delete collections on clusters
        """
        start_time = time.time()
        datasets = cluster.cbas_util.get_all_dataset_objs()
        while time.time() - start_time <= time_for_crud * 60:
            if datasets is None or len(datasets) == 0:
                self.log.error("No datasets found to create crud collections from")
                return False
            dataset = random.choice(datasets)
            ops_type = random.choice(["create", "delete"])
            if ops_type == "create":
                new_dataset = copy.deepcopy(dataset)
                new_dataset.name = cluster.cbas_util.generate_name()
                if isinstance(new_dataset, Remote_Dataset):
                    continue
                if isinstance(new_dataset, External_Dataset):
                    self.create_crud_external_collection(cluster, new_dataset, timeout)
                if isinstance(new_dataset, Standalone_Dataset):
                    self.create_crud_standalone_collection(cluster, new_dataset, timeout)
            if ops_type == "delete":
                crud_datasets = cluster.cbas_util.get_all_dataset_objs()
                if crud_datasets and len(crud_datasets) > 0:
                    dataset = random.choice(crud_datasets)
                    if not cluster.cbas_util.drop_dataset(
                            cluster, dataset.full_name):
                        self.fail("Error while dropping datasets {0}".format(
                            dataset.full_name))
                    else:
                        self.log.info("Dropped dataset :{}".format(dataset.full_name))
                        if isinstance(dataset, Remote_Dataset):
                            del cluster.cbas_util.crud_dataverses[
                                dataset.dataverse_name].remote_datasets[
                                dataset.name]
                        elif isinstance(dataset, External_Dataset):
                            del cluster.cbas_util.crud_dataverses[
                                dataset.dataverse_name].external_datasets[
                                dataset.name]
                        elif isinstance(dataset, Standalone_Dataset):
                            del cluster.cbas_util.crud_dataverses[
                                dataset.dataverse_name].standalone_datasets[
                                dataset.name]

    def start_source_ingestion(self, no_of_docs=1000000, doc_size=100000):
        mongo_collections = dynamo_collections = rds_collections = []
        if "mongo" in self.include_external_collections:
            mongo_collections = set(self.include_external_collections["mongo"])
        if "dynamo" in self.include_external_collections:
            dynamo_collections = set(self.include_external_collections["dynamo"])
        if "rds" in self.include_external_collections:
            rds_collections = set(self.include_external_collections["rds"])
        if "remote" in self.include_external_collections:
            remote_collections = set(self.include_external_collections["remote"])

        for collection in remote_collections:
            bucket = collection.split(".")[0]
            scope = collection.split(".")[1]
            collection = collection.split(".")[2]
            url = self.input.param("sirius_url", None)
            resp = self.capellaAPI.cluster_ops_apis.fetch_bucket_info(self.user.org_id,
                                                                      self.user.project.project_id,
                                                                      self.cluster_id,
                                                                      self.bucket_id)
            if resp.status_code == 200:
                current_doc_count = (resp.json())["stats"]["itemCount"]
            else:
                current_doc_count = 0
            data = {
                "identifierToken": "hotel",
                "clusterConfig": {
                    "username": self.remote_cluster_username,
                    "password": self.remote_cluster_password,
                    "connectionString": "couchbases://" + self.remote_cluster_connection_string
                },
                "bucket": bucket,
                "scope": scope,
                "collection": collection,
                "operationConfig": {
                    "start": current_doc_count,
                    "end": no_of_docs,
                    "docSize": doc_size,
                    "template": "hotel"
                },
                "insertOptions": {
                    "timeout": 5
                }
            }
            if url is not None:
                url = "http://" + url + "/bulk-create"
                response = requests.post(url, json=data)
                if response.status_code != 200:
                    self.log.error("Failed to start loader for remote collection")

        for collection in mongo_collections:
            database = collection.split(".")[0]
            collection_name = collection.split(".")[1]
            status = \
                (self.doc_loader.start_mongo_loader('', database, collection_name, atlas_url=self.mongo_connection_uri,
                                                    document_size=doc_size, initial_doc_count=no_of_docs).json())[
                    "status"]
            if status != 'running':
                self.log.error("Failed to start loader for MongoDB collection")

        for collection in dynamo_collections:
            status = (self.doc_loader.start_dynamo_loader(self.dynamo_access_key_id, self.dynamo_security_access_key,
                                                          self.gf_spec["kafka_dataset"]["primary_key"], collection,
                                                          self.dynamo_regions, no_of_docs, doc_size).json())["status"]
            if status != 'running':
                self.log.error("Failed to start loader for DynamoDB collection")

        for collection in rds_collections:
            database = collection.split('.')[0]
            table = collection.split('.')[1]
            columns = "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, address VARCHAR(255), avg_rating FLOAT, city " \
                      "VARCHAR(255), country VARCHAR(255), email VARCHAR(255) NULL, free_breakfast BOOLEAN, " \
                      "free_parking BOOLEAN, name VARCHAR(255), phone VARCHAR(255), price FLOAT, public_likes JSON, " \
                      "reviews JSON, type VARCHAR(255), url VARCHAR(255)"
            status = \
                (self.doc_loader.start_mysql_loader(self.rds_hostname, self.rds_port, self.rds_username,
                                                    self.rds_password,
                                                    database, table, columns, no_of_docs, doc_size).json())[
                    "status"]
            if status != 'running':
                self.log.error("Failed to start loader for RDS collection")

    def wait_for_source_ingestion(self, no_of_docs=100000, timeout=1000000):
        """
        Wait for all the external databases to reach the required number of documnets
        """
        start_time = time.time()
        mongo_collections = dynamo_collections = rds_collections = []
        if "mongo" in self.include_external_collections:
            mongo_collections = set(self.include_external_collections["mongo"])
        if "dynamo" in self.include_external_collections:
            dynamo_collections = set(self.include_external_collections["dynamo"])
        if "rds" in self.include_external_collections:
            rds_collections = set(self.include_external_collections["rds"])
        if "remote" in self.include_external_collections:
            remote_collection = set(self.include_external_collections["remote"])
        while time.time() < start_time + timeout:
            self.log.info("Waiting for data to be loaded in source databases")
            for collection in mongo_collections:
                database = collection.split(".")[0]
                collection_name = collection.split(".")[1]
                if (self.doc_loader.get_mongo_doc_count('', database, collection_name,
                                                        atlas_url=self.mongo_connection_uri).json())[
                    "count"] == no_of_docs:
                    self.log.info("Doc loading complete for mongo collection: {}".format(collection))
                    mongo_collections.remove(collection)
            for collection in dynamo_collections:
                resp = self.doc_loader.count_dynamo_documents(self.dynamo_access_key_id,
                                                              self.dynamo_security_access_key,
                                                              self.dynamo_regions, collection)
                if resp.json()["count"] == no_of_docs:
                    self.log.info("Doc loading complete for dynamo collection: {}".format(collection))
                    dynamo_collections.remove(collection)
            for collection in rds_collections:
                database = collection.split('.')[0]
                table = collection.split('.')[1]
                if (self.doc_loader.count_mysql_documents(self.rds_hostname, self.rds_port,
                                                          self.rds_username, self.rds_password,
                                                          database, table)).json()["count"] == no_of_docs:
                    self.log.info("Doc loading complete for rds collection: {}".format(collection))
                    rds_collections.remove(collection)

            for collection in remote_collection:
                resp = self.capellaAPI.cluster_ops_apis.fetch_bucket_info(self.user.org_id,
                                                                          self.user.project.project_id,
                                                                          self.cluster_id,
                                                                          self.bucket_id)
                if resp.status_code == 200 and (resp.json())["stats"]["itemCount"] == no_of_docs:
                    self.log.info("Doc loading complete for remote collection: {}".format(collection))
                    remote_collection.remove(collection)

            final_set = remote_collection
            if len(final_set) == 0:
                self.log.info("Doc loading is complete for all sources")
                return True
            time.sleep(30)
        self.log.error("Failed to wait for ingestion timeout {} sec reached".format(timeout))
        return False

    def wait_for_destination_ingestion(self, cluster, timeout=100000):
        """
        Wait for the ingestion in all kind of dataset to be completed with external source
        """
        start_time = time.time()
        datasets = cluster.cbas_util.get_all_dataset_objs()
        to_remove = []
        for collection in datasets:
            if isinstance(collection, Standalone_Dataset):
                if collection.data_source is None:
                    to_remove.append(collection)
        for i in to_remove:
            if i in datasets:
                datasets.remove(i)
        while time.time() < start_time + timeout:
            for collection in datasets:
                if isinstance(collection, Remote_Dataset):
                    status, _, _ = self.validate_data_in_remote_dataset(cluster, collection)
                    if status:
                        datasets.remove(collection)
                elif isinstance(collection, Standalone_Dataset):
                    if collection.data_source in ["mongo", "dynamo", "rds"]:
                        status, _, _ = self.validate_data_in_kafka_dataset(cluster, collection)
                        if status:
                            datasets.remove(collection)
                    if collection.data_source in ["s3"]:
                        status, _, _ = self.validate_data_in_external_dataset(cluster, collection)
                        if status:
                            datasets.remove(collection)
                elif isinstance(collection, External_Dataset):
                    status, _, _ = self.validate_data_in_external_dataset(cluster, collection)
                    if status:
                        datasets.remove(collection)

                if len(datasets) == 0:
                    self.log.info("All data ingested in all datasets")
                    return True
        self.log.error("Not all datasets ingestion are complete")
        return False

    def stop_crud_on_kafka_source(self, doc_loader_ids):
        for database in doc_loader_ids:
            for key, value in database:
                if database == "mongo":
                    status = (self.doc_loader.stop_crud_on_mongo(value).json())["status"]
                    if status == "stopped":
                        self.log.info("Stopped crud operation on mongo collection: {}".format(key))
                    else:
                        self.log.error("Failed to stop crud on mongo collection: {}".format(key))
                if database == "dynamo":
                    status = (self.doc_loader.stop_crud_on_dynamo(value).json())["status"]
                    if status == "stopped":
                        self.log.info("Stopped crud operation on dynamo collection: {}".format(key))
                    else:
                        self.log.error("Failed to stop crud on dynamo collection: {}".format(key))
                if database == "rds":
                    status = (self.doc_loader.stop_crud_on_mysql(value).json())["status"]
                    if status == "stopped":
                        self.log.info("Stopped crud operation on rds collection: {}".format(key))
                    else:
                        self.log.error("Failed to stop crud on rds collection: {}".format(key))
    def start_crud_on_kafka_source(self):
        mongo_collections = dynamo_collections = rds_collections = []
        if "mongo" in self.include_external_collections:
            mongo_collections = set(self.include_external_collections["mongo"])
        if "dynamo" in self.include_external_collections:
            dynamo_collections = set(self.include_external_collections["dynamo"])
        if "rds" in self.include_external_collections:
            rds_collections = set(self.include_external_collections["rds"])
        if "remote" in self.include_external_collections:
            remote_collections = set(self.include_external_collections["remote"])

        doc_loader_ids = {"mongo": dict(), "dynamo": dict(), "rds": dict()}
        for collection in mongo_collections:
            database = collection.split(".")[0]
            collection_name = collection.split(".")[1]
            response = \
                (self.doc_loader.start_crud_on_mongo('', database, collection_name, atlas_url=self.mongo_connection_uri,
                                                     document_size=1000000).json())
            if response["status"] == 'running':
                doc_loader_ids["mongo"][collection] = response["loader_id"]
                self.log.info("Started crud on mongo collection: {}".format(collection))
            else:
                self.log.error("Failed to start loader for MongoDB collection")

        for collection in dynamo_collections:
            response = (self.doc_loader.start_crud_on_dynamo(self.dynamo_access_key_id, self.dynamo_security_access_key,
                                                             self.gf_spec["kafka_dataset"]["primary_key"], collection,
                                                             self.dynamo_regions, document_size=1000000).json())

            if response["status"] == 'running':
                doc_loader_ids["dynamo"][collection] = response["loader_id"]
                self.log.info("Started crud on dynamo collection: {}".format(collection))
            else:
                self.log.error("Failed to start loader for DynamoDB collection")

        for collection in rds_collections:
            database = collection.split('.')[0]
            table = collection.split('.')[1]
            columns = "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, address VARCHAR(255), avg_rating FLOAT, city " \
                      "VARCHAR(255), country VARCHAR(255), email VARCHAR(255) NULL, free_breakfast BOOLEAN, " \
                      "free_parking BOOLEAN, name VARCHAR(255), phone VARCHAR(255), price FLOAT, public_likes JSON, " \
                      "reviews JSON, type VARCHAR(255), url VARCHAR(255)"
            response = \
                (self.doc_loader.start_crud_on_mysql(self.rds_hostname, self.rds_port, self.rds_username,
                                                     self.rds_password,
                                                     database, table, columns, document_size=1000000).json())
            if response["status"] == 'running':
                doc_loader_ids["rds"][collection] = response["loader_id"]
                self.log.info("Started crud on rds collection: {}".format(collection))
            else:
                self.log.error("Failed to start loader for RDS collection")

        return doc_loader_ids

    def scale_clusters(self, unit):
        """
        Scale all cluster to the required unit
        """
        scale_operation = Queue()
        results = []
        for user in self.users:
            for project in user.projects:
                for cluster in project.clusters:
                    results.append(self.goldfish_utils.scale_goldfish_cluster(self.pod, user, cluster, unit))
                    scale_operation.put((
                        self.goldfish_utils.wait_for_cluster_scaling_operation_to_complete,
                        {"pod": self.pod, "user": user, "cluster": cluster, "timeout": 3600}
                    ))
        self.start_thread_processes(self.cbas_util, results, scale_operation, async_run=False)
        return all(results)

    def remove_unhealthy_clusters(self):
        """
        Get the health of a cluster
        """
        for user in self.users:
            for project in user.projects:
                for cluster in project.clusters:
                    if self.goldfish_utils.get_cluster_info(self.pod, user, cluster)["state"] != "healthy":
                        self.clusters = [instance for instance in self.clusters if instance.name != cluster.name]

    def setup_for_test(self, cluster):
        """
        Process and create entities based on cbas_spec template
        """
        cluster.cbas_util = CbasUtil(self.task, self.use_sdk_for_cbas)
        cluster.goldish_utils = GoldfishUtils(self.log)
        status, msg = cluster.cbas_util.create_cbas_infra_from_spec(cluster, self.gf_spec, self.bucket_util, False)
        if not status:
            self.log.error(msg)
            self.log.error("All infra are not created. Check logs for error")

        cluster.jobs = Queue()
        cluster.results = []
        self.perform_copy_and_load_on_standalone_collection(cluster, no_of_docs=10000)
        self.run_queries_on_collections(cluster, time_for_query=90)
        cluster.jobs.put((self.wait_for_destination_ingestion,
                          {"cluster": cluster, "timeout": self.ingestion_timeout}))
        self.start_thread_processes(cluster.cbas_util, cluster.results, cluster.jobs)

    def run_goldfishE2E(self):
        """
        Main test function
        """

        # update the specs
        # 100Gb per source
        # *********** Start Scenario:1 **********************
        self.update_goldfish_spec()
        # Basic data to be present already of 100GB
        # create the infrastructure and start the basic ingestion in all kinds of collections
        for cluster in self.clusters:
            self.setup_for_test(cluster)

        # initial cluster is 2 nodes loading 500 GB data in total per cluster
        # wait for the data loading in various collections
        for cluster in self.clusters:
            self.start_thread_processes(cluster.cbas_util, cluster.results, cluster.jobs, start=False)
        # validate the data in various dataset(standalone, external and remote collection)
        for cluster in self.clusters:
            self.check_docs_in_all_datasets(cluster)
        # *********** End Scenario:1 **********************

        # start the loader to load more docs on  and
        # scale the instance from 2 to 4 compute units
        # start kafka docloader, copy to s3 and crud on standalone collection to increase the volume from 500GB to 1.5TB
        # external database now to have 200GB data
        # remote collection to have 200GB data
        # to add 100GB in remote collection sirius code
        # *********** Start Scenario:2 **********************
        # disconnect all kafka links
        for cluster in self.clusters:
            kafka_links = cluster.cbas_util.get_all_link_objs("kafka")
            for link in kafka_links:
                cluster.cbas_util.disconnect_link(cluster, link.full_name)
        for cluster in self.clusters:
            cluster.cbas_util.wait_for_kafka_links(cluster, "DISCONNECTED")
            self.check_docs_in_all_datasets(cluster)

        self.start_source_ingestion(self.external_db_doc * 2, 10000)
        for cluster in self.clusters:
            cluster.jobs.put((self.wait_for_source_ingestion,
                              {"no_of_docs": self.external_db_doc * 2, "timeout": self.ingestion_timeout}))
            cluster.jobs.put((self.run_queries_on_collections,
                              {"cluster": cluster, "no_of_parallel_query": 3, "time_for_query": 120}))
            self.start_thread_processes(cluster.cbas_util, cluster.results, cluster.jobs)

        # scale up cluster
        scaling_op = self.scale_clusters(4)
        # Not perform operation on unhealthy nodes
        if not scaling_op:
            self.remove_unhealthy_clusters()

        # scale up cluster
        scaling_op = self.scale_clusters(6)
        # Not perform operation on unhealthy nodes
        if not scaling_op:
            self.remove_unhealthy_clusters()

        for cluster in self.clusters:
            # self.check_docs_in_all_datasets(cluster)
            cluster.cbas_util.connect_links(cluster, self.gf_spec)
            cluster.jobs.put((cluster.cbas_util.wait_for_kafka_links,
                              {"cluster": cluster}))

        # increasing number of remote datasets from 2 to 6
        for cluster in self.clusters:
            remote_datasets = cluster.cbas_util.get_all_dataset_objs("remote")
            for i in range(3):
                for dataset in remote_datasets:
                    dataset.name = cluster.cbas_util.generate_name()
                    dataverse = random.choice(cluster.cbas_util.dataverses.values())
                    dataset.dataverse_name = dataverse.name
                    if not cluster.cbas_util.create_remote_dataset(
                            cluster, dataset.name,
                            dataset.full_kv_entity_name, dataset.link_name,
                            dataset.dataverse_name, False, False, None,
                            None, "column", False,
                            False, None, None, None,
                            timeout=self.gf_spec.get("api_timeout",
                                                     300),
                            analytics_timeout=self.gf_spec.get(
                                "cbas_timeout", 300)):
                        self.log.error("Failed to create remote dataset {0}".format(dataset.name))
                    else:
                        dataverse = cluster.cbas_util.dataverses[dataset.dataverse_name]
                        dataverse.remote_datasets[dataset.name] = dataset

        # wait for source ingestion to complete
        for cluster in self.clusters:
            self.start_thread_processes(cluster.cbas_util, cluster.results, cluster.jobs, start=False)
            cluster.jobs.put((self.wait_for_destination_ingestion,
                              {"cluster": cluster}))
            self.start_thread_processes(cluster.cbas_util, cluster.results, cluster.jobs)

        for cluster in self.clusters:
            self.start_thread_processes(cluster.cbas_util, cluster.results, cluster.jobs, start=False)
            self.check_docs_in_all_datasets(cluster)

        # *********** End Scenario:2 **********************

        # *********** Start Scenario:3 **********************
        # # scale down the instance from 6 to 4 compute unit
        scaling_op = self.scale_clusters(4)
        # # Not perform operation on unhealthy nodes
        if not scaling_op:
            self.remove_unhealthy_clusters()

        # create dataverse for crud on collections operations.
        # perform crud on collections to create and delete standalone, external and kafka collections for provided
        # time hrs.
        results = []
        self.log.info("Starting create and delete collection crud")
        for cluster in self.clusters:
            cluster.cbas_util.create_crud_dataverses(cluster, 1)
            standalone_loader = StandaloneCollectionLoader(cluster.cbas_util, self.use_sdk_for_cbas)
            standalone_collection = random.choice([x for x in cluster.cbas_util.get_all_dataset_objs(
                "standalone") if x.data_source is None])
            cluster.jobs.put((
                standalone_loader.crud_on_standalone_collection,
                {'cluster': cluster, 'collection_name': standalone_collection.name,
                 'dataverse_name': standalone_collection.dataverse_name,
                 'target_num_docs': 10000, 'time_for_crud_in_mins': 120,
                 "where_clause_for_delete_op": "alias.id in (SELECT VALUE "
                                               "x.id FROM {0} as x limit {1})",
                 "use_alias": True}))
            cluster.jobs.put((self.perform_collection_crud,
                              {"cluster": cluster, "time_for_crud": 5, "timeout": 120}))
            cluster.jobs.put((self.run_queries_on_collections,
                              {"cluster": cluster, "no_of_parallel_query": 3, "time_for_query": 120}))

        for cluster in self.clusters:
            self.start_thread_processes(self.cbas_util, results, cluster.jobs, async_run=False)

        # *********** End Scenario:3 **********************

        # *********** Start Scenario:4 **********************
        # scale the instance from 6 node to 8 compute units
        # increasing the load to 4TB
        scaling_op = self.scale_clusters(8)
        # # Not perform operation on unhealthy nodes
        if not scaling_op:
            self.remove_unhealthy_clusters()

        # start kafka docloader, copy to s3 and crud on standalone collection to increase the volume from 1.5TB to 4TB
        # kafka links to have 300GB data, 3TB
        # remote collection to have 300GB data, total 600GB
        # 3 standalone collection to copy from s3, total 300GB
        # 1 standalone collection insert and upsert, total 100 GB
        self.start_source_ingestion(self.external_db_doc * 3, 10000)
        for cluster in self.clusters:
            cluster.jobs.put((self.wait_for_source_ingestion,
                              {"no_of_docs": self.external_db_doc * 3, "timeout": self.ingestion_timeout}))
            cluster.jobs.put((self.run_queries_on_collections,
                              {"cluster": cluster, "no_of_parallel_query": 3, "time_for_query": 120}))
            self.start_thread_processes(cluster.cbas_util, cluster.results, cluster.jobs)

        # create 1 more remote collection
        for cluster in self.clusters:
            remote_datasets = random.choice(cluster.cbas_util.get_all_dataset_objs("remote"))
            remote_datasets.name = cluster.cbas_util.generate_name()
            remote_datasets.full_name = CBASHelper.format_name(remote_datasets.dataverse_name,
                                                               remote_datasets.name)
            if not cluster.cbas_util.create_remote_dataset(
                    cluster, remote_datasets.name,
                    remote_datasets.full_kv_entity_name, remote_datasets.link_name,
                    remote_datasets.dataverse_name, False, False, None,
                    None, "column", False,
                    False, None, None, None,
                    timeout=self.gf_spec.get("api_timeout",
                                             300),
                    analytics_timeout=self.gf_spec.get(
                        "cbas_timeout", 300)):
                self.log.error("Failed to create remote dataset {0}".format(remote_datasets.name))
            else:
                dataverse = cluster.cbas_util.dataverses[remote_datasets.dataverse_name]
                dataverse.remote_datasets[remote_datasets.name] = remote_datasets

            standalone_collection = random.choice(
                [x for x in cluster.cbas_util.get_all_dataset_objs("standalone") if x.data_source
                 in ["s3", "azure", "gcp"]])
            new_collection = []
            creation_method = "COLLECTION"
            for i in range(2):
                standalone_collection.name = cluster.cbas_util.generate_name()
                standalone_collection.full_name = CBASHelper.format_name(standalone_collection.dataverse_name,
                                                                         standalone_collection.name)
                if not cluster.cbas_util.create_standalone_collection(
                        cluster, standalone_collection.name, creation_method, False,
                        standalone_collection.dataverse_name, standalone_collection.primary_key, "",
                        False, "column"):
                    self.log.error("Failed to create standalone dataset {0}".format(standalone_collection.name))
                else:
                    dataverse = cluster.cbas_util.dataverses
                    dataverse.standalone_datasets[standalone_collection.name] = standalone_collection
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
                         "convert_decimal_to_double": standalone_collection.dataset_properties[
                             "convert_decimal_to_double"],
                         "timezone": standalone_collection.dataset_properties["timezone"]}
                    ))

        # deleting all kafka datasets and kafka links and recreating all kafka links and datasets
        for cluster in self.clusters:
            kafka_links = cluster.cbas_util.get_all_link_objs("kafka")
            standalone_datasets = cluster.cbas_util.get_all_dataset_objs("standalone")
            kafka_datasets = [x for x in standalone_datasets if x.data_source in ["dynamo", "mongo", "rds"]]

            # deleting all kafka datasets
            for dataset in kafka_datasets:
                if not cluster.cbas_util.drop_dataset(
                        cluster, dataset.full_name):
                    self.log.error("Error while dropping datasets {0}".format(
                        dataset.full_name))
                else:
                    self.log.info("Dropped dataset {0}".format(dataset.full_name))

            # deleting all kafka links
            for link in kafka_links:
                if not cluster.cbas_util.drop_link(cluster, link.full_name):
                    self.log.error("Error while deleting link {0}".format(link.full_name))

            # creating all kafka links again
            for link in kafka_links:
                if not cluster.cbas_util.create_kafka_link(
                        cluster, link.name, link.external_database_details,
                        link.dataverse_name, timeout=300,
                        analytics_timeout=300):
                    self.log.error("Failed to create link {0} of type {1}".format(link.full_name, link.db_type))
                else:
                    self.log.info("Created link {0} of type {1}".format(link.full_name, link.db_type))

            # creating all kafka datasets again
            for dataset in kafka_datasets:
                creation_method = "COLLECTION"
                storage_format = "column"
                if not cluster.cbas_util.create_standalone_collection_using_links(
                        cluster, dataset.name, creation_method, False,
                        dataset.dataverse_name, dataset.primary_key,
                        dataset.link_name, dataset.external_collection_name,
                        False, storage_format):
                    self.log.error(
                        "Error while creating dataset {0} on link {1}".format(dataset.full_name, dataset.link_name))
                else:
                    self.log.info("Created dataset {0} on link {1}".format(dataset.full_name, dataset.link_name))

            cluster.cbas_util.connect_links(cluster, self.gf_spec)
            cluster.jobs.put((cluster.cbas_util.wait_for_kafka_links,
                              {"cluster": cluster, "state": "CONNECTED"}))

        for cluster in self.clusters:
            remote_datasets = cluster.cbas_util.get_all_dataset_objs("remote")
            for i in range(4):
                dataset = random.choice(remote_datasets)
                dataset.name = cluster.cbas_util.generate_name()
                dataverse = random.choice(cluster.cbas_util.dataverses.values())
                dataset.dataverse_name = dataverse.name
                creation_method = "COLLECTION"
                if not cluster.cbas_util.create_remote_dataset(
                        cluster, dataset.name,
                        dataset.full_kv_entity_name, dataset.link_name,
                        dataset.dataverse_name, False, False, None,
                        None, "column", False,
                        False, None, None, None,
                        timeout=self.gf_spec.get("api_timeout",
                                                 300),
                        analytics_timeout=self.gf_spec.get(
                            "cbas_timeout", 300)):
                    self.log.error("Failed to create remote dataset {0}".format(dataset.name))
                else:
                    dataverse = cluster.cbas_util.dataverses[dataverse.name]
                    dataverse.remote_datasets[dataset.name] = dataset

            cluster.cbas_util.connect_links(cluster, self.gf_spec)
            cluster.jobs.put((cluster.cbas_util.wait_for_kafka_links,
                              {"cluster": cluster, "state": "CONNECTED"}))

        # to add remote collection code to start ingestion and create 1 more remote collection
        # to add 100GB in standalone collection
        # to add 2 more copy from s3 datasets
        for cluster in self.clusters:
            self.start_thread_processes(cluster.cbas_util, cluster.results, cluster.jobs, start=False)
            cluster.jobs.put((self.wait_for_destination_ingestion,
                              {"cluster": cluster}))
            self.start_thread_processes(cluster.cbas_util, cluster.results, cluster.jobs)

        for cluster in self.clusters:
            self.start_thread_processes(cluster.cbas_util, cluster.results, cluster.jobs, start=False)
            self.check_docs_in_all_datasets(cluster)
        # *********** End Scenario:4 **********************
