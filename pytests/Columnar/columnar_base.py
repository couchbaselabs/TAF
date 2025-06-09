"""
Created on 17-Oct-2023
@author: Umang Agrawal
"""
import os

from basetestcase import BaseTestCase
from TestInput import TestInputSingleton
from cbas_utils.cbas_utils import CbasUtil
from BucketLib.bucket import Bucket
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI
from BucketLib.BucketOperations import BucketHelper
import json
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader


class ColumnarBaseTest(BaseTestCase):

    def setUp(self):
        """
        Since BaseTestCase will initialize at least one cluster, we pass service
        for the master node of that cluster
        """
        if not hasattr(self, "input"):
            self.input = TestInputSingleton.input

        super(ColumnarBaseTest, self).setUp()

        if self._testMethodDoc:
            self.log.info("Starting Test: %s - %s"
                          % (self._testMethodName, self._testMethodDoc))
        else:
            self.log.info("Starting Test: %s" % self._testMethodName)

        self.use_sdk_for_cbas = self.input.param("use_sdk_for_cbas", False)
        self.sdk_clients_per_user = self.input.param("sdk_clients_per_user", 1)

        if self.use_sdk_for_cbas:
            for instance in self.tenant.columnar_instances:
                self.init_sdk_pool_object(
                    instance, self.sdk_clients_per_user,
                    instance.master.rest_username,
                    instance.master.rest_password)

        # Common properties
        self.num_concurrent_queries = self.input.param('num_queries', 5000)
        self.concurrent_batch_size = self.input.param('concurrent_batch_size',
                                                      100)
        self.retry_time = self.input.param("retry_time", 300)
        self.num_retries = self.input.param("num_retries", 1)

        self.columnar_spec_name = self.input.param("columnar_spec_name", None)

        self.cbas_util = CbasUtil(self.task, self.use_sdk_for_cbas)

        self.perform_columnar_instance_cleanup = self.input.param(
            "perform_columnar_instance_cleanup", True)

        # AWS credentials and other info
        self.aws_access_key = self.input.param("aws_access_key", "")
        self.aws_secret_key = self.input.param("aws_secret_key", "")
        self.aws_session_token = self.input.param("aws_session_token", "")
        self.aws_region = self.input.param("aws_region", "us-west-1")
        self.aws_endpoint = self.input.param("aws_endpoint", "")

        self.columnar_aws_access_key = self.input.param("columnar_aws_access_key", self.aws_access_key)
        self.columnar_aws_secret_key = self.input.param("columnar_aws_secret_key", self.aws_secret_key)
        self.columnar_aws_session_token = self.input.param("columnar_aws_session_token", self.aws_session_token)
        self.columnar_aws_region = self.input.param("columnar_aws_region", self.aws_region)
        self.columnar_aws_endpoint = self.input.param("columnar_aws_endpoint", self.aws_endpoint)

        # For sanity tests we are hard coding the bucket from which the data
        # will be read. This will ensure stable and consistent test runs.
        # Override this variable in your test setup if you want to use a
        # different bucket
        self.s3_source_bucket = self.input.param(
            "s3_source_bucket", "columnar-functional-sanity-test-data")

        # Initialising capella V4 API object, which can used to make capella
        # V4 API calls.
        self.capellaAPI = CapellaAPI(
            url=self.pod.url_public, secret='', access='',
            user=self.tenant.user, pwd=self.tenant.pwd,
            bearer_token=None, TOKEN_FOR_INTERNAL_SUPPORT=self.pod.TOKEN)
        response = self.capellaAPI.create_control_plane_api_key(
            self.tenant.id, 'init api keys')
        if response.status_code == 201:
            response = response.json()
        else:
            self.log.error("Error while creating V2 control plane API key")
            self.fail("{}".format(response.content))
        self.capellaAPI.cluster_ops_apis.SECRET = response['secretKey']
        self.capellaAPI.cluster_ops_apis.ACCESS = response['id']
        self.capellaAPI.cluster_ops_apis.bearer_token = response['token']
        self.capellaAPI.org_ops_apis.SECRET = response['secretKey']
        self.capellaAPI.org_ops_apis.ACCESS = response['id']
        self.capellaAPI.org_ops_apis.bearer_token = response['token']

        # create the first V4 API KEY WITH organizationOwner role, which will
        # be used to perform further V4 api operations
        resp = self.capellaAPI.org_ops_apis.create_api_key(
            organizationId=self.tenant.id,
            name=self.cbas_util.generate_name(),
            organizationRoles=["organizationOwner"],
            description=self.cbas_util.generate_name())
        if resp.status_code == 201:
            org_owner_key = resp.json()
        else:
            self.fail("Error while creating V4 API key for organization owner")

        self.capellaAPI.org_ops_apis.bearer_token = \
            self.capellaAPI.cluster_ops_apis.bearer_token = \
            org_owner_key["token"]

        for instance in self.tenant.columnar_instances:
            if not self.cbas_util.wait_for_cbas_to_recover(instance):
                self.fail("Analytics service failed to start")

        self.log.info("=== CBAS_BASE setup was finished for test #{0} {1} ==="
                      .format(self.case_number, self._testMethodName))

    def tearDown(self):
        if self.perform_columnar_instance_cleanup:
            for instance in self.tenant.columnar_instances:
                self.cbas_util.cleanup_cbas(instance)

        super(ColumnarBaseTest, self).tearDown()

    def create_bucket_scopes_collections_in_capella_cluster(
            self, tenant, cluster, num_buckets=1, bucket_ram_quota=1024,
            num_scopes_per_bucket=1, num_collections_per_scope=1):
        for i in range(0, num_buckets):
            bucket = Bucket(
                {Bucket.name: self.cbas_util.generate_name(),
                 Bucket.ramQuotaMB: bucket_ram_quota,
                 Bucket.maxTTL: self.bucket_ttl,
                 Bucket.replicaNumber: self.num_replicas,
                 Bucket.storageBackend: self.bucket_storage,
                 Bucket.evictionPolicy: self.bucket_eviction_policy,
                 Bucket.durabilityMinLevel: self.bucket_durability_level,
                 Bucket.flushEnabled: True})
            response = self.capellaAPI.cluster_ops_apis.create_bucket(
                tenant.id, tenant.project_id, cluster.id, bucket.name,
                "couchbase", bucket.storageBackend, bucket.ramQuotaMB, "seqno",
                bucket.durabilityMinLevel, bucket.replicaNumber,
                bucket.flushEnabled, bucket.maxTTL)
            if response.status_code == 201:
                self.log.info("Created bucket {}".format(bucket.name))
                bucket.uuid = response.json()["id"]
                cluster.buckets.append(bucket)
            else:
                self.fail("Error creating bucket {0} on cluster {1}".format(
                    bucket.name, cluster.name))

            # since default scope is already present in the bucket.
            for j in range(1, num_scopes_per_bucket):
                scope_name = self.cbas_util.generate_name()
                resp = self.capellaAPI.cluster_ops_apis.create_scope(
                    tenant.id, tenant.project_id, cluster.id,
                    bucket.uuid, scope_name)
                if resp.status_code == 201:
                    self.log.info("Created scope {} on bucket {}".format(
                        scope_name, bucket.name))
                    self.bucket_util.create_scope_object(
                        bucket, scope_spec={"name": scope_name})
                else:
                    self.fail("Failed while creating scope {} on bucket {"
                              "}".format(scope_name, bucket.name))

            for scope_name, scope in bucket.scopes.items():
                if scope_name != "_system":
                    collections_to_create = num_collections_per_scope
                    if "_default" in scope.collections:
                        collections_to_create -= 1
                    for k in range(0, collections_to_create):
                        collection_name = self.cbas_util.generate_name()
                        resp = self.capellaAPI.cluster_ops_apis.create_collection(
                            tenant.id, tenant.project_id, clusterId=cluster.id,
                            bucketId=bucket.uuid, scopeName=scope_name,
                            name=collection_name)

                        if resp.status_code == 201:
                            self.log.info(
                                "Create collection {} in scope {}".format(
                                    collection_name, scope_name))
                            self.bucket_util.create_collection_object(
                                bucket, scope_name,
                                collection_spec={"name": collection_name})
                        else:
                            self.fail(
                                "Failed creating collection {} in scope {}".format(
                                    collection_name, scope_name))

    def delete_all_buckets_from_capella_cluster(self, tenant, cluster):
        resp = self.capellaAPI.cluster_ops_apis.list_buckets(
            tenant.id, tenant.project_id, cluster.id)
        if resp.status_code == 200:
            failed_to_delete_buckets = []
            data = resp.json()["data"]
            for bucket in data:
                resp = self.capellaAPI.cluster_ops_apis.delete_bucket(
                    tenant.id, tenant.project_id, cluster.id, bucket["id"])
                if resp.status_code == 204:
                    self.log.info("Bucket {0} deleted successfully".format(
                        bucket["name"]))
                else:
                    self.log.error(
                        "Bucket {0} deletion failed".format(bucket["name"]))
                    failed_to_delete_buckets.append(bucket["name"])
            if failed_to_delete_buckets:
                self.fail("Following buckets were not deleted {0}".format(
                    failed_to_delete_buckets))
        else:
            self.fail("Error while fetching bucket list for cluster {"
                      "0}".format(cluster.id))

    def generate_bucket_object_for_existing_buckets(self, tenant, cluster):
        for bucket in cluster.buckets:
            resp = self.capellaAPI.cluster_ops_apis.list_scopes(
                tenant.id, tenant.project_id, cluster.id, bucket.uuid)
            if resp.status_code == 200:
                scope_data = resp.json()["scopes"]
                for scope_info in scope_data:
                    if scope_info["name"] not in ["_default", "_system"]:
                        self.bucket_util.create_scope_object(
                            bucket, scope_spec={"name": scope_info["name"]})

                    for collection_info in scope_info["collections"]:
                        if collection_info["name"] != "_default":
                            self.bucket_util.create_collection_object(
                                bucket, scope_info["name"],
                                collection_spec={
                                    "name": collection_info["name"],
                                    "maxTTL": collection_info["maxTTL"]})
            else:
                self.fail(
                    f"Error while fetching scope list for bucket {bucket.name}")

        bucket_helper_obj = BucketHelper(cluster.master)
        stats_api = bucket_helper_obj.base_url + "/pools/default/stats/range"
        def get_stats_param(bucket_name, scope_name, collection_id):
            stats_param = [
                {"applyFunctions": ["sum"],
                 "metric":[
                     {"label": "name", "value": "kv_collection_item_count"},
                     {"label": "bucket", "value": bucket_name},
                     {"label": "scope", "value": scope_name},
                     {"label": "collection_id",
                      "value": hex(int(collection_id, 16))}
                 ],
                 "nodesAggregation": "sum",
                 "start": -3,
                 "step": 3,
                 "timeWindow": 360}]
            return json.dumps(stats_param)

        for bucket in cluster.buckets:
            for scope_name, scope in bucket.scopes.items():
                if scope_name != "_system":
                    for collection_name, collection in (
                            scope.collections.items()):
                        coll_id = bucket_helper_obj.get_collection_id(
                            bucket, scope_name, collection_name)
                        param = get_stats_param(bucket.name, scope_name, coll_id)
                        status, content, _ = bucket_helper_obj.request(
                            stats_api, "POST", params=param)
                        if status:
                            collection.num_items = int(content[0]["data"][0][
                                "values"][-1][1])

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
            external_collection_file_formats=[],
            path_on_external_container="", aws_kafka_cluster_details=[],
            confluent_kafka_cluster_details=[], external_dbs=[],
            kafka_topics={}, aws_kafka_schema_registry_details=[],
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
        if columnar_spec["remote_link"][
            "no_of_remote_links"] and remote_cluster:
            resp = self.capellaAPI.cluster_ops_apis.get_cluster_certificate(
                self.tenant.id, self.tenant.project_id, remote_cluster.id)
            if resp.status_code == 200:
                certificate = resp.json()["certificate"]
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
            if self.input.param("external_link_source", "s3") == "gcs":
                try:
                    # gcs_certificate variable should be set in jenkins to use here
                    self.log.info("Fetching certificate file path from the env: gcs_certificate")
                    gcs_certificate = os.getenv('gcp_access_file')
                    with open(gcs_certificate, 'r') as file:
                        # Load JSON data from file
                        data = json.load(file)
                    columnar_spec["external_link"]["properties"] = [{
                        "type": "gcs",
                        "jsonCredentials": data
                    }]
                except Exception as err:
                    raise err
            else: columnar_spec["external_link"]["properties"] = [{
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
                    "confluent"].extend(confluent_kafka_schema_registry_details)

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
                    "timezone": "",
                    "timestamp_to_long": 0,
                    "date_to_int": 0
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
                    "timezone": "GMT",
                    "timestamp_to_long": 0,
                    "date_to_int": 0
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
                    "timezone": "",
                    "timestamp_to_long": 0,
                    "date_to_int": 0
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
                    "timezone": "",
                    "timestamp_to_long": 0,
                    "date_to_int": 0
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
                    "timezone": "",
                    "timestamp_to_long": 0,
                    "date_to_int": 0
                })
            elif file_format == "delta":
                prop.update({
                    "file_format": "delta",
                    "include": None,
                    "exclude": None,
                    "object_construction_def": None,
                    "redact_warning": None,
                    "header": None,
                    "null_string": None,
                    "parse_json_string": 0,
                    "convert_decimal_to_double": 0,
                    "timezone": "",
                    "timestamp_to_long": 0,
                    "date_to_int": 0
                })
            columnar_spec["external_dataset"][
                "external_dataset_properties"].append(prop)

        # This defines number of standalone collections created for
        # inser/upsert/delete and copy from KV and s3
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
        columnar_spec["index"]["no_of_indexes"] = self.input.param("num_indexes", 0)
        if columnar_spec["index"]["no_of_indexes"] > 0:
            columnar_spec["index"]["indexed_fields"] = [
                "id:string", "id:string-product_name:string"]
        columnar_spec["file_format"] = self.input.param("file_format", "json")
        return columnar_spec

    def load_doc_to_remote_collections(self, cluster, valType, buckets=None,
                                create_start_index=None, create_end_index=None,
                                read_start_index=None, read_end_index=None,
                                update_start_index=None, update_end_index=None,
                                delete_start_index=None, delete_end_index=None,
                                expiry_start_index=None, expiry_end_index=None,
                                wait_for_completion=True, create_percent=100,
                                       read_percent=0, update_percent=0, delete_percent=0, expiry_percent=0):
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
            pattern = [create_percent, read_percent, update_percent, delete_percent, expiry_percent]
            for scope in bucket.scopes.keys():
                for i, collection in enumerate(bucket.scopes[scope].collections.keys()):
                    valType = valType
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