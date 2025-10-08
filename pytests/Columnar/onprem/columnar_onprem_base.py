"""
Created on 4-April-2024

@author: umang.agrawal@couchbase.com
"""
from cbas.cbas_base import CBASBaseTest
from couchbase_utils.cbas_utils.cbas_utils_columnar import CbasUtil as columnarCBASUtil
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader


class ColumnarOnPremBase(CBASBaseTest):

    def setUp(self):
        super(ColumnarOnPremBase, self).setUp()
        self.use_sdk_for_cbas = self.input.param("use_sdk_for_cbas", False)

        self.aws_region = self.input.param("aws_region", "us-west-1")
        self.s3_source_bucket = self.input.param("s3_source_bucket", "columnar-functional-sanity-test-data")
        self.columnar_cbas_utils = columnarCBASUtil(
            self.task, self.use_sdk_for_cbas)
        
        for cluster in self.cb_clusters.values():
            cluster.srv = None
            if hasattr(cluster, "cbas_cc_node"):
                self.analytics_cluster = cluster
                self.columnar_cluster = cluster
            else:
                self.remote_cluster = cluster

    def tearDown(self):
        super(ColumnarOnPremBase, self).tearDown()

    def load_remote_collections(self, cluster, buckets=None,
                                create_start_index=None, create_end_index=None,
                                read_start_index=None, read_end_index=None,
                                update_start_index=None, update_end_index=None,
                                delete_start_index=None, delete_end_index=None,
                                expiry_start_index=None, expiry_end_index=None,
                                wait_for_completion=True,
                                template="Hotel"):
        buckets = buckets or cluster.buckets
        thread_count = 0
        for bucket in buckets:
            for scope in bucket.scopes.keys():
                if scope == "_system":
                    continue
                for collection in bucket.scopes[scope].collections.keys():
                    thread_count += 1
        per_coll_ops = self.input.param("ops_rate", 20000)//thread_count
        tasks = list()

        for bucket in buckets:
            pattern = [100, 0, 0, 0, 0]
            for scope in bucket.scopes.keys():
                for i, collection in enumerate(bucket.scopes[scope].collections.keys()):
                    valType = template
                    if scope == "_system":
                        continue
                    loader = SiriusCouchbaseLoader(
                        server_ip=cluster.master.ip, server_port=cluster.master.port,
                        username="Administrator", password="password",
                        bucket=bucket,
                        scope_name=scope, collection_name=collection,
                        key_prefix="test_docs-", key_size=20, doc_size=256,
                        key_type="SimpleKey", value_type=valType,
                        create_percent=pattern[0], read_percent=pattern[1], update_percent=pattern[2],
                        delete_percent=pattern[3], expiry_percent=pattern[4],
                        create_start_index=create_start_index, create_end_index=create_end_index,
                        read_start_index=read_start_index, read_end_index=read_end_index,
                        update_start_index=update_start_index, update_end_index=update_end_index,
                        delete_start_index=delete_start_index, delete_end_index=delete_end_index,
                        expiry_start_index=expiry_start_index, expiry_end_index=expiry_end_index,
                        process_concurrency=1, task_identifier="", ops=per_coll_ops,
                        suppress_error_table=False,
                        track_failures=True,
                        mutate=0,
                        elastic=False, model=self.model, mockVector=self.mockVector, dim=self.dim, base64=self.base64)
                    loader.create_doc_load_task()
                    self.task_manager.add_new_task(loader)
                    tasks.append(loader)
        if wait_for_completion:
            self.wait_for_completion(tasks)
        return tasks

    def wait_for_completion(self, tasks):
        for loader in tasks:
            loader.result = self.task_manager.get_task_result(loader)
            if loader.fail_count == loader.create_end_index:
                self.fail("Doc loading failed for {0}.{1}.{2}"
                            .format(loader.bucket.name,
                                    loader.scope,
                                    loader.collection))
            else:
                if loader.fail_count > 0:
                    self.log.error(
                        "{0} Docs failed to load "
                        "for {1}.{2}.{3}"
                        .format(loader.fail_count,
                                loader.bucket.name, loader.scope,
                                loader.collection))
                    loader.bucket.scopes[loader.scope].collections[loader.collection].num_items = (
                            loader.create_end_index -
                            loader.fail_count)
                else:
                    loader.bucket.scopes[loader.scope].collections[loader.collection].num_items = loader.create_end_index

    """
    This method populates the columnar spec that will be used create 
    columnar entities.
    :param columnar_spec <dict> columnar specs
    :param remote_cluster <obj> remote cluster object.
    :param external_collection_file_formats <list> List of external 
    collection file formats. Accepted values are json, csv, tsv, parquet and avro.
    :param external_dbs <list> List of external databases from where the 
    data has to be ingested. Accepted values are MONGODB, MYSQLDB, POSTGRESQL.
    :param kafka_topics <dict> Should be in below format
    kafka_topics = {
            "confluent": {"MONGODB": [], "POSTGRESQL": [], "MYSQLDB": []},
            "aws_kafka": {"MONGODB": [], "POSTGRESQL": [], "MYSQLDB": []}
        }
    """
    def populate_columnar_infra_spec(
            self, columnar_spec, remote_cluster=None,
            external_collection_file_formats=[], path_on_external_container="",
            aws_kafka_cluster_details=[], confluent_kafka_cluster_details=[],
            external_dbs=[], kafka_topics={},
            aws_kafka_schema_registry_details=[],
            confluent_kafka_schema_registry_details=[]):

        # Updating Database spec
        columnar_spec["database"]["no_of_databases"] = self.input.param(
            "num_db", 1)

        # Updating Dataverse/Scope spec
        columnar_spec["dataverse"]["no_of_dataverses"] = self.input.param(
            "num_dv", 1)

        # Updating Remote Links Spec
        columnar_spec["remote_link"][
            "no_of_remote_links"] = self.input.param("num_remote_links", 0)
        if columnar_spec["remote_link"]["no_of_remote_links"] and remote_cluster:
            status, content = remote_cluster.rest.security.get_trusted_root_certificates()
            if status:
                certificate = content[0]["pem"]
            else:
                self.fail("Failed to get cluster certificate")

            columnar_spec["remote_link"]["properties"] = [{
                "type": "couchbase",
                "hostname": remote_cluster.master.ip,
                "username": remote_cluster.master.rest_username,
                "password": remote_cluster.master.rest_password,
                "encryption": "full",
                "certificate": certificate}]

        # Updating External Links Spec
        columnar_spec["external_link"][
            "no_of_external_links"] = self.input.param("num_external_links", 0)
        if columnar_spec["external_link"]["no_of_external_links"]:
            if self.input.param("external_link_source", "s3") == "azureblob":
                columnar_spec["external_link"]["properties"] = [{
                    "type": "azureblob",
                    "region": self.aws_region,
                    "accountName": self.aws_access_key,
                    "accountKey": self.aws_secret_key,
                    "endpoint": self.aws_endpoint
                }]
            else:
                columnar_spec["external_link"]["properties"] = [{
                    "type": "s3",
                    "region": self.aws_region,
                    "accessKeyId": self.aws_access_key,
                    "secretAccessKey": self.aws_secret_key,
                    "serviceEndpoint": self.aws_endpoint
                }]

        # Updating Kafka Links Spec
        columnar_spec["kafka_link"]["no_of_kafka_links"] = self.input.param(
            "num_kafka_links", 0)

        if columnar_spec["kafka_link"]["no_of_kafka_links"]:

            if aws_kafka_cluster_details:
                columnar_spec["kafka_link"]["vendors"].append("aws_kafka")
                columnar_spec["kafka_link"]["kafka_cluster_details"][
                    "aws_kafka"].extend(aws_kafka_cluster_details)
                columnar_spec["kafka_link"]["schema_registry_details"][
                    "aws_kafka"].extend(aws_kafka_schema_registry_details)

            if confluent_kafka_cluster_details:
                columnar_spec["kafka_link"]["vendors"].append("confluent")
                columnar_spec["kafka_link"]["kafka_cluster_details"][
                    "confluent"].extend(confluent_kafka_cluster_details)
                columnar_spec["kafka_link"]["schema_registry_details"][
                    "confluent"].extend(
                    confluent_kafka_schema_registry_details)

        # Updating Remote Dataset Spec
        columnar_spec["remote_dataset"][
            "num_of_remote_datasets"] = self.input.param(
            "num_remote_collections", 0)
        columnar_spec["remote_dataset"]["storage_format"] = "column"

        # Updating External Datasets Spec
        columnar_spec["external_dataset"][
            "num_of_external_datasets"] = self.input.param(
            "num_external_collections", 0)
        columnar_spec["external_dataset"]["external_dataset_properties"] = []
        for file_format in external_collection_file_formats:
            prop = {
                "external_container_name": self.s3_source_bucket,
                "region": self.aws_region,
                "path_on_external_container": path_on_external_container
            }

            if file_format == "json":
                prop.update({
                    "file_format": "json",
                    "include": ["*.json"],
                    "exclude": None,
                    "object_construction_def": None,
                    "redact_warning": None,
                    "header": None,
                    "null_string": None,
                    "parse_json_string": 0,
                    "convert_decimal_to_double": 0,
                    "timezone": ""
                })
            elif file_format == "parquet":
                prop.update({
                    "file_format": "parquet",
                    "include": ["*.parquet"],
                    "exclude": None,
                    "object_construction_def": None,
                    "redact_warning": None,
                    "header": None,
                    "null_string": None,
                    "parse_json_string": 1,
                    "convert_decimal_to_double": 1,
                    "timezone": "GMT"
                })
            elif file_format == "avro":
                prop.update({
                    "file_format": "avro",
                    "include": ["*.avro"],
                    "exclude": None,
                    "object_construction_def": None,
                    "redact_warning": None,
                    "header": None,
                    "null_string": None,
                    "parse_json_string": 0,
                    "convert_decimal_to_double": 0,
                    "timezone": ""
                })
            elif file_format == "csv":
                prop.update({
                    "file_format": "csv",
                    "include": ["*.csv"],
                    "exclude": None,
                    "object_construction_def": (
                        "id string,product_name string,product_link string,"
                        "product_features string,product_specs string,"
                        "product_image_links string,product_reviews string,"
                        "product_category string, price double,avg_rating "
                        "double,num_sold int,upload_date string,weight "
                        "double,quantity int,seller_name string,"
                        "seller_location string,seller_verified boolean,"
                        "template_name string,mutated int,padding string"),
                    "redact_warning": False,
                    "header": True,
                    "null_string": None,
                    "parse_json_string": 0,
                    "convert_decimal_to_double": 0,
                    "timezone": ""
                })
            elif file_format == "tsv":
                prop.update({
                    "file_format": "tsv",
                    "include": ["*.tsv"],
                    "exclude": None,
                    "object_construction_def": (
                        "id string,product_name string,product_link string,"
                        "product_features string,product_specs string,"
                        "product_image_links string,product_reviews string,"
                        "product_category string, price double,avg_rating "
                        "double,num_sold int,upload_date string,weight "
                        "double,quantity int,seller_name string,"
                        "seller_location string,seller_verified boolean,"
                        "template_name string,mutated int,padding string"),
                    "redact_warning": False,
                    "header": True,
                    "null_string": None,
                    "parse_json_string": 0,
                    "convert_decimal_to_double": 0,
                    "timezone": ""
                })
            columnar_spec["external_dataset"][
                "external_dataset_properties"].append(prop)

        # This defines number of standalone collections created for
        # inser/upsert/delete and copy from s3
        # Update Standalone Collection Spec
        columnar_spec["standalone_dataset"][
            "num_of_standalone_coll"] = self.input.param(
            "num_standalone_collections", 0)
        columnar_spec["standalone_dataset"]["storage_format"] = "column"

        # Update Kafka Datasets Spec here.
        columnar_spec["kafka_dataset"][
            "num_of_ds_on_external_db"] = self.input.param(
            "num_kafka_collections", 0)
        if columnar_spec["kafka_dataset"]["num_of_ds_on_external_db"] > 0:
            columnar_spec["kafka_dataset"]["storage_format"] = "column"
            columnar_spec["kafka_dataset"]["data_source"] = external_dbs
            columnar_spec["kafka_dataset"]["kafka_topics"] = kafka_topics

        # Update Synonym Spec here
        columnar_spec["synonym"]["no_of_synonyms"] = self.input.param(
            "num_synonyms", 0)

        # Update Index Spec here
        columnar_spec["index"]["no_of_indexes"] = self.input.param(
            "num_indexes", 0)
        if columnar_spec["index"]["no_of_indexes"] > 0:
            columnar_spec["index"]["indexed_fields"] = [
                "id:string", "id:string-product_name:string"]
        columnar_spec["file_format"] = self.input.param("file_format", "json")
        return columnar_spec
