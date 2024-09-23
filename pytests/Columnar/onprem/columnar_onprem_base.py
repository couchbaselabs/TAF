"""
Created on 4-April-2024

@author: umang.agrawal@couchbase.com
"""
from cbas.cbas_base import CBASBaseTest
from cbas_utils.cbas_utils_columnar import CbasUtil as columnarCBASUtil


class ColumnarOnPremBase(CBASBaseTest):

    def setUp(self):
        super(ColumnarOnPremBase, self).setUp()
        self.use_sdk_for_cbas = self.input.param("use_sdk_for_cbas", False)
        self.columnar_cbas_utils = columnarCBASUtil(
            self.task, self.use_sdk_for_cbas)

        self.aws_region = self.input.param("aws_region", "ap-south-1")
        self.s3_source_bucket = self.input.param("s3_source_bucket", None)

    def tearDown(self):
        super(ColumnarOnPremBase, self).tearDown()

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
            external_collection_file_formats=[], aws_kafka_cluster_details=[],
            confluent_kafka_cluster_details=[], external_dbs=[],
            kafka_topics={}):

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
            columnar_spec["external_link"]["properties"] = [{
                "type": "s3",
                "region": self.aws_region,
                "accessKeyId": self.aws_access_key,
                "secretAccessKey": self.aws_secret_key,
                "serviceEndpoint": None
            }]

        # Updating Kafka Links Spec
        columnar_spec["kafka_link"]["no_of_kafka_links"] = self.input.param(
            "num_kafka_links", 0)

        if columnar_spec["kafka_link"]["no_of_kafka_links"]:

            if aws_kafka_cluster_details:
                columnar_spec["kafka_link"]["vendors"].append("aws_kafka")
                columnar_spec["kafka_link"]["kafka_cluster_details"][
                    "aws_kafka"].extend(aws_kafka_cluster_details)

            if confluent_kafka_cluster_details:
                columnar_spec["kafka_link"]["vendors"].append("confluent")
                columnar_spec["kafka_link"]["kafka_cluster_details"][
                    "confluent"].extend(confluent_kafka_cluster_details)

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
                "path_on_external_container": "level_{level_no:int}_folder_{folder_no:int}"
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

        return columnar_spec
