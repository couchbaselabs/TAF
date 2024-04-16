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
from cbas.cbas_base import CBASBaseTest
from security_utils.security_utils import SecurityUtils
from rbac_utils.Rbac_ready_functions import RbacUtils
from couchbase_utils.security_utils.x509_multiple_CA_util import x509main
from CbasLib.cbas_entity_columnar import ExternalDB
import random, json, copy
from BucketLib.BucketOperations import BucketHelper
from cbas_utils.cbas_utils import CBASRebalanceUtil
from CbasLib.cbas_entity_columnar import (
    Remote_Dataset, External_Dataset, Standalone_Dataset)
from Jython_tasks.task import RunQueriesTask


class GoldFish(CBASBaseTest):
    """
    Reqired test parameters

    """

    def setUp(self):
        super(GoldFish, self).setUp()
        self.security_util = SecurityUtils(self.log)
        self.to_clusters = list()

        for cluster in self.cb_clusters.values():
            if hasattr(cluster, "cbas_cc_node"):
                self.analytics_cluster = cluster
            else:
                self.to_clusters.append(cluster)
            cluster.rbac_util = RbacUtils(cluster.master)

        self.rebalance_util = CBASRebalanceUtil(
            self.cluster_util, self.bucket_util, self.task, True, self.cbas_util)

        self.source_s3_regions = self.input.param(
            "source_s3_regions", None)
        self.source_s3_bucket = self.input.param(
            "source_s3_bucket", None)
        self.mongo_connection_uri = self.input.param(
            "mongo_connection_uri", None)
        self.dynamo_access_key_id = self.input.param(
            "dynamo_access_key_id", None)
        self.dynamo_security_access_key = self.input.param(
            "dynamo_security_access_key", None)
        self.dynamo_regions = self.input.param("dynamo_regions", None)
        self.num_of_CRUD_on_datasets = self.input.param(
            "num_of_CRUD_on_datasets", 5)
        self.num_iterations = self.input.param("num_iterations", 1)

        self.run_concurrent_query = self.input.param("run_query", False)
        if self.run_concurrent_query:
            self.start_query_task()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def start_query_task(self):
        queries = list()
        self.query_task = RunQueriesTask(
            self.analytics_cluster, queries, self.task_manager,
            self.cbas_util, "cbas", run_infinitely=True, parallelism=10,
            is_prepared=False, record_results=True, regenerate_queries=True)
        self.task_manager.add_new_task(self.query_task)

    def stop_query_task(self):
        if hasattr(self, "query_task") and self.query_task:
            if self.query_task.exception:
                self.task_manager.get_task_result(self.query_task)
            self.task_manager.stop_task(self.query_task)
            self.log.info(self.query_task.result)
            delattr(self, "query_task")

    def tearDown(self):
        """
        Teardown to delete all entity and destroy the cluster
        """
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        if self.run_concurrent_query:
            self.stop_query_task()

        super(GoldFish, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.tearDown.__name__)

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
                    "accessKeyId": self.aws_access_key,
                    "secretAccessKey": self.aws_secret_key
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
        Create kafka link specs for mongo, dynamo, cassandra.
        Current support for mongo(many) and dynamo(one).
        Cassandra under development by dev team
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

        if (self.dynamo_access_key_id and self.dynamo_security_access_key and
                self.dynamo_regions):
            dynamo_regions = self.dynamo_regions.split("|")
            cbas_spec["kafka_link"]["external_database_details"][
                "dynamo"] = list()
            for region in dynamo_regions:
                dynamo_obj = ExternalDB(
                    db_type="mongo",
                    dynamo_access_key=self.dynamo_access_key_id,
                    dynamo_secret_key=self.dynamo_security_access_key,
                    dynamo_region=region
                )
                cbas_spec["kafka_link"]["external_database_details"][
                    "dynamo"].append(
                    dynamo_obj.get_source_db_detail_object_for_kafka_links())
            cbas_spec["kafka_link"]["database_type"].append("dynamo")

    def update_external_dataset_spec(self, cbas_spec):
        external_dataset_properties_list = list()
        #for file_format in ["json", "csv", "tsv", "avro", "parquet"]:
        for file_format in ["json"]:
            external_dataset_properties = {
                "external_container_name": self.source_s3_bucket,
                "path_on_external_container": "",
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
                external_dataset_properties += {
                    "object_construction_def": "",
                    "redact_warning": None,
                    "header": "",
                    "null_string": "",
                }
            if file_format == "parquet":
                external_dataset_properties += {
                    "parse_json_string": "",
                    "convert_decimal_to_double": 1,
                    "timezone": "GMT"
                }
            external_dataset_properties_list.append(external_dataset_properties)
        cbas_spec["external_dataset"][
            "external_dataset_properties"] = external_dataset_properties_list

    def update_standalone_collection_spec(self, cbas_spec):
        """
        Create standalone collection specs.
        Based on test-plan currently creating standalone collection on s3 and kafka links
        """
        cbas_spec["standalone_dataset"]["data_source"] = ["s3"]
        dataset_properties_list = list()
        # for file_format in ["json", "csv", "tsv", "avro", "parquet"]:
        for file_format in ["json"]:
            dataset_properties = {
                "external_container_name": self.source_s3_bucket,
                "path_on_external_container": "",
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
                dataset_properties += {
                    "object_construction_def": "",
                    "redact_warning": None,
                    "header": "",
                    "null_string": "",
                }
            if file_format == "parquet":
                dataset_properties += {
                    "parse_json_string": "",
                    "convert_decimal_to_double": 1,
                    "timezone": "GMT"
                }
            dataset_properties_list.append(
                dataset_properties)
        cbas_spec["standalone_dataset"][
            "standalone_collection_properties"] = dataset_properties_list

    def update_kafka_dataset_spec(self, cbas_spec):
        cbas_spec["kafka_dataset"]["data_source"] = ["mongo"]

    def generate_bucket_obj_for_remote_cluster_obj(self):
        for cluster in self.to_clusters:
            if not cluster.buckets:
                bucket_helper = BucketHelper(cluster.master)
                cluster.buckets = self.bucket_util.get_all_buckets(
                    cluster)
                for bucket in cluster.buckets:
                    collection_item_count = \
                        self.bucket_util.get_doc_count_per_collection(
                            cluster, bucket)
                    status, content = bucket_helper.list_collections(
                        bucket.name)
                    json_parsed = json.loads(content)
                    scopes = json_parsed["scopes"]
                    for scope in scopes:
                        self.bucket_util.create_scope_object(
                            bucket, scope_spec={"name": scope["name"]})
                        collections = scope["collections"]
                        for collection in collections:
                            del collection["uid"]
                            collection["num_items"] = collection_item_count[
                                scope["name"]][collection["name"]]["items"]
                            self.bucket_util.create_collection_object(
                                bucket, scope["name"], collection)

    def setup_for_test(self):
        """
        Process and create entities based on cbas_spec template
        """
        cbas_spec_name = self.input.param("cbas_spec", 'columnar')
        cbas_spec = self.cbas_util.get_cbas_spec(cbas_spec_name)
        self.generate_bucket_obj_for_remote_cluster_obj()
        self.update_external_link_spec(cbas_spec)
        self.update_remote_link_spec(cbas_spec)
        self.update_kafka_link_spec(cbas_spec)
        self.update_external_dataset_spec(cbas_spec)
        self.update_standalone_collection_spec(cbas_spec)

        status, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.analytics_cluster, cbas_spec, self.bucket_util,
            False, remote_clusters=self.to_clusters)
        if not status:
            self.log.error(msg)
            self.fail("Error while creating infra from CBAS spec")

        # Ingest data from external datasources like S3, GCP and Azure into
        # standalone collection.
        standalone_collections = self.cbas_util.list_all_dataset_objs(
            "standalone")
        for standalone_collection in standalone_collections:
            if standalone_collection.data_source in ["s3", "azure", "gcp"]:
                if not self.cbas_util.copy_from_external_resource_into_standalone_collection(
                    self.analytics_cluster, standalone_collection.name,
                    standalone_collection.dataset_properties["external_container_name"],
                    standalone_collection.link_name,
                    standalone_collection.dataverse_name,
                    standalone_collection.dataset_properties["include"],
                    standalone_collection.dataset_properties["file_format"],
                    standalone_collection.dataset_properties["object_construction_def"],
                    standalone_collection.dataset_properties["path_on_external_container"],
                    standalone_collection.dataset_properties["header"],
                    standalone_collection.dataset_properties["null_string"],
                    standalone_collection.dataset_properties["exclude"],
                    standalone_collection.dataset_properties["parse_json_string"],
                    standalone_collection.dataset_properties["convert_decimal_to_double"],
                    standalone_collection.dataset_properties["timezone"]
                ):
                    self.fail("Unable to copy data from {0} into standalone "
                              "collection {1]".format(
                        standalone_collection.data_source,
                        standalone_collection.full_name))
        self.validate_data_ingestion_into_datasets()

    def validate_data_ingestion_into_datasets(self):
        datasets = self.cbas_util.list_all_dataset_objs()
        for dataset in datasets:
            retry = 0
            dataset_doc_count = 0
            while retry < 5:
                dataset_doc_count = self.cbas_util.get_num_items_in_cbas_dataset(
                    self.analytics_cluster, dataset.full_name)[0]
                if dataset_doc_count > 0:
                    self.log.info("Doc count in dataset {0} is {1}".format(
                        dataset.full_name, dataset_doc_count))
                    break
                else:
                    retry += 1
            if dataset_doc_count == 0:
                self.log.warn("No data was ingested into dataset {0}. "
                              "Can be a bug. Cross verify to confirm".format(
                    dataset.full_name))

    def perform_create_delete_ops_on_datasets(self):
        # Starting Create and delete ops on datasets.
        for i in range(self.num_of_CRUD_on_datasets):
            datasets = self.cbas_util.list_all_dataset_objs()
            ops_type = random.choice(["create", "delete"])
            dataset = random.choice(datasets)
            if ops_type == "create":
                new_dataset = copy.deepcopy(dataset)
                new_dataset.name = self.cbas_util.generate_name()
                new_dataset.reset_full_name()
                if isinstance(new_dataset, Remote_Dataset):
                    if not self.cbas_util.create_remote_dataset(
                            self.analytics_cluster, new_dataset.name,
                            new_dataset.full_kv_entity_name,
                            new_dataset.link_name,
                            new_dataset.dataverse_name, False, False,
                            None, None, new_dataset.storage_format,
                            random.choice([True, False]),
                            False, None, None, None,
                            timeout=3600, analytics_timeout=3600):
                        self.fail("Error while creating remote datasets {"
                                  "0}".format(new_dataset.full_name))
                    else:
                        self.cbas_util.dataverses[
                            new_dataset.dataverse_name].remote_datasets[
                            new_dataset.name] = new_dataset
                elif isinstance(new_dataset, External_Dataset):
                    if not self.create_dataset_on_external_resource(
                            self.analytics_cluster, new_dataset.name,
                            new_dataset.dataset_properties[
                                "external_container_name"],
                            new_dataset.link_name, False,
                            new_dataset.dataverse_name,
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
                            timeout=3600,
                            analytics_timeout=3600):
                        self.fail("Error while creating external datasets {"
                                  "0}".format(new_dataset.full_name))
                    else:
                        self.cbas_util.dataverses[
                            new_dataset.dataverse_name].external_datasets[
                            new_dataset.name] = new_dataset
                elif isinstance(new_dataset, Standalone_Dataset):
                    if new_dataset.data_source in ["s3", "azure", "gcp"]:
                        if not self.cbas_util.create_standalone_collection(
                                self.analytics_cluster, new_dataset.name,
                                "random", False, new_dataset.dataverse_name,
                                new_dataset.primary_key, "",
                                False, new_dataset.storage_format):
                            self.fail(
                                "Error while creating standalone collection {"
                                "0}".format(new_dataset.full_name))
                        if not self.cbas_util.copy_from_external_resource_into_standalone_collection(
                                self.analytics_cluster, new_dataset.name,
                                new_dataset.dataset_properties[
                                    "external_container_name"],
                                new_dataset.link_name,
                                new_dataset.dataverse_name,
                                new_dataset.dataset_properties[
                                    "include"],
                                new_dataset.dataset_properties[
                                    "file_format"],
                                new_dataset.dataset_properties[
                                    "object_construction_def"],
                                new_dataset.dataset_properties[
                                    "path_on_external_container"],
                                new_dataset.dataset_properties["header"],
                                new_dataset.dataset_properties[
                                    "null_string"],
                                new_dataset.dataset_properties[
                                    "exclude"],
                                new_dataset.dataset_properties[
                                    "parse_json_string"],
                                new_dataset.dataset_properties[
                                    "convert_decimal_to_double"],
                                new_dataset.dataset_properties[
                                    "timezone"]):
                            self.fail(
                                "Unable to copy data from {0} into standalone "
                                "collection {1]".format(
                                    new_dataset.data_source,
                                    new_dataset.full_name))
                    else:
                        if not self.cbas_util.disconnect_link(
                                self.analytics_cluster, new_dataset.link_name):
                            self.fail("Unable to disconnect link {0}".format(
                                new_dataset.link_name))
                        if not self.cbas_util.create_standalone_collection_using_links(
                                self.analytics_cluster, new_dataset.name,
                                "random", False, new_dataset.dataverse_name,
                                new_dataset.primary_key,
                                new_dataset.link_name,
                                new_dataset.external_collection_name,
                                False, new_dataset.storage_format):
                            self.fail(
                                "Error while creating standalone collection {"
                                "0} on external db {1}".format(
                                    new_dataset.full_name,
                                    new_dataset.data_source))
                        if not self.cbas_util.connect_link(
                                self.analytics_cluster, new_dataset.link_name):
                            self.fail("Unable to connect link {0}".format(
                                new_dataset.link_name))
                    self.cbas_util.dataverses[
                        new_dataset.dataverse_name].standalone_datasets[
                        new_dataset.name] = new_dataset
            else:
                if not self.cbas_util.drop_dataset(
                        self.analytics_cluster, dataset.full_name):
                    self.fail("Error while dropping datasets {0}".format(
                        dataset.full_name))
                if isinstance(dataset, Remote_Dataset):
                    del self.cbas_util.dataverses[
                        dataset.dataverse_name].remote_datasets[
                        dataset.name]
                elif isinstance(dataset, External_Dataset):
                    del self.cbas_util.dataverses[
                        dataset.dataverse_name].external_datasets[
                        dataset.name]
                elif isinstance(dataset, Standalone_Dataset):
                    del self.cbas_util.dataverses[
                        dataset.dataverse_name].standalone_datasets[
                        dataset.name]



    def test_goldfish_deploy(self):
        """
        Main test function
        """
        self.setup_for_test()
        for i in range(self.num_iterations):
            self.perform_create_delete_ops_on_datasets()
            self.validate_data_ingestion_into_datasets()
            for rebalance_type in ["in", "swap", "out"]:
                if rebalance_type == "in":
                    cbas_node_in = 1
                    cbas_node_out = 0
                    exclude_node = []
                elif rebalance_type == "swap":
                    cbas_node_in = 1
                    cbas_node_out = 1
                    exclude_node = [self.analytics_cluster.master]
                elif rebalance_type == "swap":
                    cbas_node_in = 0
                    cbas_node_out = 1
                    exclude_node = [self.analytics_cluster.master]
                rebalance_task, self.available_servers = self.rebalance_util.rebalance(
                    self.analytics_cluster, 0, 0, cbas_node_in, cbas_node_out,
                    self.available_servers, exclude_node)
                if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                        rebalance_task, self.analytics_cluster, True):
                    self.fail("Rebalance failed.")
                self.validate_data_ingestion_into_datasets()
        self.log.info("Testing spec file")
