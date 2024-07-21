"""
Created on 3-January-2024

@author: abhay.aggrawal@couchbase.com
"""
import math
import json
import os.path
import random
import time
import requests
from queue import Queue

from couchbase_utils.capella_utils.dedicated import CapellaUtils
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI
from Columnar.columnar_base import ColumnarBaseTest
from CbasLib.CBASOperations import CBASHelper
from cbas_utils.cbas_utils_columnar import External_Dataset, Standalone_Dataset, Remote_Dataset
from itertools import combinations, product
from BucketLib.bucket import Bucket

from awsLib.s3_data_helper import perform_S3_operation
from Columnar.mini_volume_code_template import MiniVolume


class CopyToS3(ColumnarBaseTest):
    def setUp(self):
        super(CopyToS3, self).setUp()
        self.cluster = self.tenant.columnar_instances[0]
        self.doc_loader_url = self.input.param("doc_loader_url", None)
        self.doc_loader_port = self.input.param("doc_loader_port", None)

        self.sink_s3_bucket_name = None
        for i in range(5):
            try:
                self.sink_s3_bucket_name = "copy-to-s3-" + str(random.randint(1, 100000))
                self.log.info("Creating S3 bucket for : {}".format(self.sink_s3_bucket_name))
                self.sink_bucket_created = perform_S3_operation(
                    aws_access_key=self.aws_access_key,
                    aws_secret_key=self.aws_secret_key,
                    aws_session_token=self.aws_session_token,
                    create_bucket=True, bucket_name=self.sink_s3_bucket_name,
                    region=self.aws_region)
                break
            except Exception as e:
                self.log.error("Creating S3 bucket - {0} in region {1} failed".format(self.sink_s3_bucket_name,
                                                                                      self.aws_region))
                self.log.error(str(e))
            finally:
                if i == 4:
                    self.sink_s3_bucket_name = None
                    self.fail("Unable to create S3 bucket even after 5 retries")

        if not self.columnar_spec_name:
            self.columnar_spec_name = "regressions.copy_to_s3"

        self.columnar_spec = self.cbas_util.get_columnar_spec(
            self.columnar_spec_name)

        self.capellaAPI = CapellaAPI(self.pod.url_public, '', '', self.tenant.user, self.tenant.pwd, '')

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def delete_s3_bucket(self):
        """
            Delete S3 bucket created for copying files
            """
        for i in range(5):
            try:
                if perform_S3_operation(
                        aws_access_key=self.aws_access_key,
                        aws_secret_key=self.aws_secret_key,
                        aws_session_token=self.aws_session_token,
                        empty_bucket=True,
                        bucket_name=self.sink_s3_bucket_name,
                        region=self.aws_region):
                    break
            except Exception as e:
                self.log.error("Unable to empty S3 bucket - {0}".format(
                    self.sink_s3_bucket_name))
                self.log.error(str(e))
            finally:
                if i == 4:
                    self.fail("Unable to empty S3 bucket even after 5 "
                              "retries")

        # Delete the created S3 bucket
        self.log.info("Deleting AWS S3 bucket - {0}".format(
            self.sink_s3_bucket_name))

        for i in range(5):
            try:
                if perform_S3_operation(
                        aws_access_key=self.aws_access_key,
                        aws_secret_key=self.aws_secret_key,
                        aws_session_token=self.aws_session_token,
                        delete_bucket=True,
                        bucket_name=self.sink_s3_bucket_name,
                        region=self.aws_region):
                    break
            except Exception as e:
                self.log.error("Unable to delete S3 bucket - {0}".format(
                    self.sink_s3_bucket_name))
                self.log.error(str(e))
            finally:
                if i == 4:
                    self.fail("Unable to delete S3 bucket even after 5 "
                              "retries")

    def capella_provisioned_cluster_setup(self):

        for key in self.cb_clusters:
            self.remote_cluster = self.cb_clusters[key]
            break
        resp = (self.capellaAPI.create_control_plane_api_key(self.tenant.id, 'init api keys')).json()
        self.capellaAPI.cluster_ops_apis.SECRET = resp['secretKey']
        self.capellaAPI.cluster_ops_apis.ACCESS = resp['id']
        self.capellaAPI.cluster_ops_apis.bearer_token = resp['token']
        self.capellaAPI.org_ops_apis.SECRET = resp['secretKey']
        self.capellaAPI.org_ops_apis.ACCESS = resp['id']
        self.capellaAPI.org_ops_apis.bearer_token = resp['token']
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

        # creating bucket scope and collection to pump data
        bucket_name = "hotel"
        scope = None
        collection = None

        resp = self.capellaAPI.cluster_ops_apis.create_bucket(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.remote_cluster.id,
                                                              bucket_name, "couchbase", "couchstore", 100, "seqno",
                                                              "majorityAndPersistActive", 1, True, 1000000)
        buckets = json.loads(CapellaUtils.get_all_buckets(self.pod, self.tenant, self.remote_cluster)
                             .content)["buckets"]["data"]
        for bucket in buckets:
            bucket = bucket["data"]
            bucket_obj = Bucket({
                Bucket.name: bucket["name"],
                Bucket.ramQuotaMB: bucket["memoryAllocationInMb"],
                Bucket.replicaNumber: bucket["replicas"],
                Bucket.conflictResolutionType:
                    bucket["bucketConflictResolution"],
                Bucket.flushEnabled: bucket["flush"],
                Bucket.durabilityMinLevel: bucket["durabilityLevel"],
                Bucket.maxTTL: bucket["timeToLive"],
            })
            bucket_obj.uuid = bucket["id"]
            bucket_obj.stats.itemCount = bucket["stats"]["itemCount"]
            bucket_obj.stats.memUsed = bucket["stats"]["memoryUsedInMib"]
            self.remote_cluster.buckets.append(bucket_obj)

        if resp.status_code == 201:
            self.bucket_id = resp.json()["id"]
            self.log.info("Bucket created successfully")
        else:
            self.fail("Error creating bucket in remote_cluster")

        if bucket_name and scope and collection:
            self.remote_collection = "{}.{}.{}".format(bucket_name, scope, collection)
        else:
            self.remote_collection = "{}.{}.{}".format(bucket_name, "_default", "_default")
        resp = self.capellaAPI.cluster_ops_apis.get_cluster_certificate(self.tenant.id,
                                                                        self.tenant.project_id,
                                                                        self.remote_cluster.id)
        if resp.status_code == 200:
            self.remote_cluster_certificate = (resp.json())["certificate"]
        else:
            self.fail("Failed to get cluster certificate")

    def tearDown(self):
        """
        Delete all the analytics link and columnar instance
        """
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.cluster, self.columnar_spec):
            self.fail("Error while deleting cbas entities")

        if self.sink_s3_bucket_name:
            self.delete_s3_bucket()

        super(CopyToS3, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished", stage="Teardown")

    def create_external_dataset(self, dataset_obj):
        if self.cbas_util.create_dataset_on_external_resource(
                self.cluster, dataset_obj.name,
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
                analytics_timeout=300):
            return True
        return False

    def wait_for_source_ingestion(self, no_of_docs=100000, timeout=1000000):
        """
        Wait for all the external databases to reach the required number of documnets
        """
        start_time = time.time()
        mongo_collections = dynamo_collections = rds_collections = remote_collection = []
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
                resp = self.capellaAPI.cluster_ops_apis.fetch_bucket_info(self.tenant.id,
                                                                          self.tenant.project_id,
                                                                          self.remote_cluster.id,
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

    def start_source_ingestion(self, no_of_docs=1000000, doc_size=100000):
        mongo_collections = dynamo_collections = rds_collections = remote_collections = []
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
            resp = self.capellaAPI.cluster_ops_apis.fetch_bucket_info(self.tenant.id,
                                                                      self.tenant.project_id,
                                                                      self.remote_cluster.id,
                                                                      self.bucket_id)
            if resp.status_code == 200:
                current_doc_count = (resp.json())["stats"]["itemCount"]
            else:
                current_doc_count = 0
            data = {
                "identifierToken": "hotel",
                "dbType": "couchbase",
                "username": self.remote_cluster.username,
                "password": self.remote_cluster.password,
                "connectionString": "couchbases://" + str(self.remote_cluster.srv),
                "extra": {
                    "bucket": bucket,
                    "scope": scope,
                    "collection": collection,
                },
                "operationConfig": {
                    "start": 0,
                    "end": no_of_docs,
                    "docSize": doc_size,
                    "template": "hotel"
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
                                                          self.columnar_spec["kafka_dataset"]["primary_key"],
                                                          collection,
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

    def base_infra_setup(self, primary_key=None):
        self.columnar_spec["dataverse"]["no_of_dataverses"] = self.input.param(
            "no_of_scopes", 1)

        self.columnar_spec["remote_link"]["no_of_remote_links"] = self.input.param(
            "no_of_remote_links", 0)

        if self.input.param("no_of_remote_links", 0):
            remote_link_properties = list()
            remote_link_properties.append(
                {"type": "couchbase", "hostname": str(self.remote_cluster.srv),
                 "username": self.remote_cluster.username,
                 "password": self.remote_cluster.password,
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
        if primary_key is not None:
            self.columnar_spec["standalone_dataset"]["primary_key"] = primary_key
        else:
            self.columnar_spec["standalone_dataset"]["primary_key"] = [{"name": "string", "email": "string"}]

        self.columnar_spec["external_dataset"]["num_of_external_datasets"] = self.input.param("num_of_external_coll", 0)
        if self.input.param("num_of_external_coll", 0):
            external_dataset_properties = [{
                "external_container_name": self.s3_source_bucket,
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
        if not hasattr(self, "remote_cluster"):
            remote_cluster = None
        else:
            remote_cluster = [self.remote_cluster]
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.columnar_spec, self.bucket_util, False, remote_clusters=remote_cluster)
        if not result:
            self.fail(msg)

    def validate_data_in_remote_dataset(self, dataset):
        """
        Validate doc in remote datasets
        """
        item_count = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, dataset.full_name,
                                                                  timeout=3600, analytics_timeout=3600)
        resp = self.capellaAPI.cluster_ops_apis.fetch_bucket_info(self.tenant.id,
                                                                  self.tenant.project_id,
                                                                  self.remote_cluster.id,
                                                                  self.bucket_id)
        if resp.status_code == 200:
            current_doc_count = (resp.json())["stats"]["itemCount"]
        if item_count[0] != current_doc_count:
            return False, current_doc_count, item_count[0]
        return True, current_doc_count, item_count[0]

    def validate_data_in_external_dataset(self, cluster, dataset, doc_count):
        """
        Validate data in datasets made from S3 links
        """
        item_count = self.cbas_util.get_num_items_in_cbas_dataset(cluster, dataset.full_name,
                                                                  timeout=3600, analytics_timeout=3600)
        if item_count[0] != doc_count:
            return False, doc_count, item_count[0]
        return True, doc_count, item_count[0]

    def wait_for_destination_ingestion(self, doc_count, check_external_dataset=False, timeout=1000):
        """
        Wait for the ingestion in all kind of dataset to be completed with external source
        """
        datasets = self.cbas_util.get_all_dataset_objs()
        to_remove = []
        for collection in datasets:
            if isinstance(collection, Standalone_Dataset):
                if collection.data_source is None:
                    to_remove.append(collection)
        for i in to_remove:
            if i in datasets:
                datasets.remove(i)
        end_time = time.time() + timeout
        while time.time() < end_time:
            for collection in datasets:
                if isinstance(collection, Remote_Dataset):
                    status, _, _ = self.validate_data_in_remote_dataset(collection)
                    if status:
                        datasets.remove(collection)
                elif isinstance(collection, Standalone_Dataset):
                    if collection.data_source in ["mongo", "dynamo", "rds"]:
                        status, _, _ = self.validate_data_in_kafka_dataset(self.cluster, collection)
                        if status:
                            datasets.remove(collection)
                    if collection.data_source in ["s3"]:
                        status, _, _ = self.validate_data_in_external_dataset(self.cluster, collection, doc_count)
                        if status:
                            datasets.remove(collection)
                elif isinstance(collection, External_Dataset):
                    if not check_external_dataset:
                        datasets.remove(collection)
                    else:
                        status, _, _ = self.validate_data_in_external_dataset(self.cluster, collection, doc_count)
                        if status:
                            datasets.remove(collection)

                if len(datasets) == 0:
                    self.log.info("All data ingested in all datasets")
                    return True
        self.log.error("Not all datasets ingestion are complete")
        return False

    def pairs(self, *lists):
        for t in combinations(lists, 2):
            for pair in product(*t):
                yield pair

    def test_create_copyToS3_from_standalone_dataset_query_drop_standalone_collection(self):
        self.base_infra_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        s3_link = self.cbas_util.get_all_link_objs("s3")[0]
        no_of_docs = self.input.param("no_of_docs", 1000)
        jobs = Queue()
        results = []
        self.log.info("Adding {} documents in standalone dataset. Default doc size is 1KB".format(no_of_docs))
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name
                          , "no_of_docs": no_of_docs}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Some documents were not inserted")

        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.cluster, "collection_name": datasets[i].name,
                       "dataverse_name": datasets[i].dataverse_name, "database_name": datasets[i].database_name,
                       "destination_bucket": self.sink_s3_bucket_name, "destination_link_name": s3_link.full_name,
                       "path": path}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.fail("Failed to execute copy to statement")

        path_on_external_container = "{copy_dataset:string}"
        # create external dataset on the S3 bucket
        dataset_obj = self.cbas_util.create_external_dataset_obj(
            self.cluster,
            external_container_names={
                self.sink_s3_bucket_name: self.aws_region},
            paths_on_external_container=[path_on_external_container],
            file_format="json")[0]

        if not self.create_external_dataset(dataset_obj):
            self.fail("Failed to create external dataset on destination S3 bucket")

        # Verify the data in dataset and s3
        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            statement = "select count(*) from {0} where copy_dataset = \"{1}\"".format(dataset_obj.full_name, path)
            status, metrics, errors, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster, statement)
            doc_count_in_dataset = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, datasets[i].full_name)
            if result[0]['$1'] != doc_count_in_dataset:
                self.log.error("Document count mismatch in S3 dataset {0} and dataset {1}".format(
                    dataset_obj.full_name, datasets[i].full_name
                ))
            results.append(result[0]['$1'] == doc_count_in_dataset)
        if not all(results):
            self.fail("The document count does not match in dataset and S3")

    def test_create_copyToS3_from_remote_collection_query_drop_standalone_collection(self):
        self.capella_provisioned_cluster_setup()
        self.columnar_spec["remote_link"]["no_of_remote_links"] = self.input.param(
            "no_of_remote_links", 1)
        remote_link_properties = list()
        remote_link_properties.append(
            {"type": "couchbase", "hostname": str(self.remote_cluster.srv),
             "username": self.remote_cluster.username,
             "password": self.remote_cluster.password,
             "encryption": "full",
             "certificate": self.remote_cluster_certificate})
        self.columnar_spec["remote_link"]["properties"] = remote_link_properties
        self.include_external_collections = dict()
        remote_collection = [self.remote_collection]
        self.include_external_collections["remote"] = remote_collection
        self.columnar_spec["remote_dataset"]["include_collections"] = remote_collection
        self.columnar_spec["remote_dataset"]["num_of_remote_datasets"] = self.input.param("num_of_remote_coll", 1)
        self.base_infra_setup()
        no_of_docs = self.input.param("no_of_docs", 10000)
        self.start_source_ingestion(no_of_docs=no_of_docs, doc_size=1024)
        self.wait_for_source_ingestion(no_of_docs=no_of_docs)
        self.wait_for_destination_ingestion(no_of_docs)
        datasets = self.cbas_util.get_all_dataset_objs("remote")
        s3_link = self.cbas_util.get_all_link_objs("s3")[0]
        jobs = Queue()
        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.cluster, "collection_name": datasets[i].name,
                       "dataverse_name": datasets[i].dataverse_name, "database_name": datasets[i].database_name,
                       "destination_bucket": self.sink_s3_bucket_name, "destination_link_name": s3_link.full_name,
                       "path": path}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.fail("Copy to S3 statement failure")

        path_on_external_container = "{copy_dataset:string}"
        dataset_obj = self.cbas_util.create_external_dataset_obj(self.cluster,
                                                                 external_container_names={
                                                                     self.sink_s3_bucket_name: self.aws_region},
                                                                 paths_on_external_container=[
                                                                     path_on_external_container],
                                                                 file_format="json")[0]
        if not self.create_external_dataset(dataset_obj):
            self.fail("Failed to create external dataset on destination S3 bucket")

        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            statement = "select count(*) from {0} where copy_dataset = \"{1}\"".format(dataset_obj.full_name, path)
            status, metrics, errors, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster, statement)
            doc_count_in_dataset = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, datasets[i].full_name)
            if result[0]['$1'] != doc_count_in_dataset:
                self.log.error("Document count mismatch in S3 dataset {0} and remote dataset {1}".format(
                    dataset_obj.full_name, datasets[i].full_name
                ))
            results.append(result[0]['$1'] == doc_count_in_dataset)

        if not all(results):
            self.fail("The document count does not match in remote source and S3")

    def test_create_copyToS3_from_external_collection_query_drop_standalone_collection(self):
        self.base_infra_setup()
        external_datasets = self.cbas_util.get_all_dataset_objs("external")
        s3_link = self.cbas_util.get_all_link_objs("s3")[0]
        jobs = Queue()
        results = []
        for i in range(len(external_datasets)):
            path = "copy_dataset_" + str(i)
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.cluster, "collection_name": external_datasets[i].name,
                       "dataverse_name": external_datasets[i].dataverse_name,
                       "database_name": external_datasets[i].database_name,
                       "destination_bucket": self.sink_s3_bucket_name,
                       "destination_link_name": s3_link.full_name, "path": path, "timeout": 3600,
                       "analytics_timeout": 3600}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.fail("Copy to S3 statement failure")

        path_on_external_container = "{copy_dataset:string}"
        dataset_obj = self.cbas_util.create_external_dataset_obj(self.cluster,
                                                                 external_container_names={
                                                                     self.sink_s3_bucket_name: self.aws_region},
                                                                 paths_on_external_container=[
                                                                     path_on_external_container],
                                                                 file_format="json")[0]
        if not self.create_external_dataset(dataset_obj):
            self.fail("Failed to create external dataset on destination S3 bucket")

        results = []
        for i in range(len(external_datasets)):
            path = "copy_dataset_" + str(i)
            statement = "select count(*) from {0} where copy_dataset = \"{1}\"".format(dataset_obj.full_name, path)
            status, metrics, errors, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster, statement)
            doc_count_in_dataset = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster,
                                                                                external_datasets[i].full_name)
            if result[0]['$1'] != doc_count_in_dataset:
                self.log.error("Document count mismatch in S3 dataset {0} and external dataset {1}".format(
                    dataset_obj.full_name, external_datasets[i].full_name
                ))
            results.append(result[0]['$1'] == doc_count_in_dataset)

        if not all(results):
            self.fail("The document count does not match in external source and S3")

    def test_create_copyToS3_from_multiple_collection_query_drop_standalone_collection(self):
        self.capella_provisioned_cluster_setup()
        self.base_infra_setup()
        remote_dataset = self.cbas_util.get_all_dataset_objs("remote")
        external_dataset = self.cbas_util.get_all_dataset_objs("external")
        standalone_dataset = self.cbas_util.get_all_dataset_objs("standalone")
        unique_pairs = []
        for pair in self.pairs(remote_dataset, external_dataset, standalone_dataset):
            unique_pairs.append(pair)
        s3_link = self.cbas_util.get_all_link_objs("s3")[0]
        no_of_docs = self.input.param("no_of_docs", 100)
        self.start_source_ingestion(no_of_docs=no_of_docs, doc_size=1024)
        jobs = Queue()
        results = []
        for dataset in standalone_dataset:
            if dataset.data_source is None:
                jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                          {"cluster": self.cluster, "collection_name": dataset.name,
                           "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name,
                           "no_of_docs": no_of_docs}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        self.wait_for_source_ingestion(no_of_docs=no_of_docs)
        self.wait_for_destination_ingestion(no_of_docs)
        results = []
        for i in range(len(unique_pairs)):
            path = "copy_dataset_" + str(i)
            statement = "select * from {0} as a, {1} as b where a.avg_rating > 0.4 and b.avg_rating > 0.4".format(
                (unique_pairs[i][0]).full_name, (unique_pairs[i][1]).full_name
            )
            jobs.put((
                self.cbas_util.copy_to_s3,
                {"cluster": self.cluster, "source_definition_query": statement, "alias_identifier": "ally",
                 "destination_bucket": self.sink_s3_bucket_name,
                 "destination_link_name": s3_link.full_name, "path": path, "timeout": 3600,
                 "analytics_timeout": 3600}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.log.error("Copy to S3 statement failure")

        path_on_external_container = "{copy_dataset:string}"
        dataset_obj = self.cbas_util.create_external_dataset_obj(self.cluster,
                                                                 external_container_names={
                                                                     self.sink_s3_bucket_name: self.aws_region},
                                                                 paths_on_external_container=[
                                                                     path_on_external_container],
                                                                 file_format="json")[0]
        if not self.create_external_dataset(dataset_obj):
            self.fail("Failed to create external dataset on destination S3 bucket")

        for i in range(len(unique_pairs)):
            path = "copy_dataset_" + str(i)
            statement = "select count(*) from {0} as a, {1} as b where a.avg_rating > 0.4 and b.avg_rating > 0.4".format(
                (unique_pairs[i][0]).full_name, (unique_pairs[i][1]).full_name
            )
            status, metrics, errors, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster, statement)

            statement = "select count(*) from {0} where copy_dataset = \"{1}\"".format(dataset_obj.full_name, path)
            status, metrics, errors, result1, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster, statement)

            if result[0]['$1'] != result1[0]['$1']:
                self.log.error("Document count mismatch in S3 dataset {0} and remote dataset {1}".format(
                    dataset_obj.full_name, dataset_obj[0].full_name
                ))
            results.append(result[0]['$1'] == result1[0]['$1'])

        if not all(results):
            self.fail("The document count does not match in remote source and S3")

    def test_create_copyToS3_from_collection_with_different_key_type_query_drop_standalone_collection(self):
        self.base_infra_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        s3_link = self.cbas_util.get_all_link_objs("s3")[0]
        no_of_docs = self.input.param("no_of_docs", 1000)
        jobs = Queue()
        results = []
        for i in range(len(datasets)):
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.cluster, "collection_name": datasets[i].name,
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
                      {"cluster": self.cluster, "collection_name": datasets[i].name, "alias_identifier": "ally",
                       "dataverse_name": datasets[i].dataverse_name, "database_name": datasets[i].database_name,
                       "destination_bucket": self.sink_s3_bucket_name,
                       "destination_link_name": s3_link.full_name, "path": path, "partition_alias": "country",
                       "partition_by": "ally.country"}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.fail("Copy to S3 statement failure")

        # create dataset based on dynamic prefixes
        path_on_external_container_int = "{copy_dataset:string}/{country:int}"
        dataset_obj_int = self.cbas_util.create_external_dataset_obj(self.cluster,
                                                                     external_container_names={
                                                                         self.sink_s3_bucket_name: self.aws_region},
                                                                     paths_on_external_container=[
                                                                         path_on_external_container_int],
                                                                     file_format="json")[0]
        if not self.create_external_dataset(dataset_obj_int):
            self.fail("Failed to create external dataset on destination S3 bucket")

        path_on_external_container_string = "{copy_dataset:string}/{country:string}"
        dataset_obj_string = self.cbas_util.create_external_dataset_obj(self.cluster,
                                                                        external_container_names={
                                                                            self.sink_s3_bucket_name: self.aws_region},
                                                                        paths_on_external_container=[
                                                                            path_on_external_container_string],
                                                                        file_format="json")[0]
        if not self.create_external_dataset(dataset_obj_string):
            self.fail("Failed to create external dataset on destination S3 bucket")

        # verification step
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            for j in range(5):
                statement = "select * from {0} limit 1".format(datasets[i].full_name)
                status, metrics, errors, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster,
                                                                                                   statement)
                country_name = (result[0])[CBASHelper.unformat_name(datasets[i].name)]["country"]
                if isinstance(country_name, int):
                    statement = "select count(*) from {0} where country = {1}".format(datasets[i].full_name,
                                                                                      country_name)
                    dynamic_statement = "select count(*) from {0} where copy_dataset = \"{1}\" and country = {2}".format(
                        dataset_obj_int.full_name, path, country_name
                    )
                    status, metrics, errors, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster,
                                                                                                       dynamic_statement)
                    result_val = result[0]['$1']
                else:
                    statement = "select count(*) from {0} where country = \"{1}\"".format(datasets[i].full_name,
                                                                                          str(country_name))
                    dynamic_statement = "select count(*) from {0} where copy_dataset = \"{1}\" and country = \"{2}\"".format(
                        dataset_obj_string.full_name, path, str(country_name)
                    )
                    status, metrics, errors, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster,
                                                                                                       dynamic_statement)
                    result_val = result[0]['$1']
                status, metrics, errors, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster,
                                                                                                   statement)
                dataset_result = result[0]['$1']
                if dataset_result != result_val:
                    self.fail("Document Count Mismatch")

    def test_create_copyToS3_from_collection_with_gzip_compression_drop_standalone_collection(self):
        self.base_infra_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        s3_link = self.cbas_util.get_all_link_objs("s3")[0]
        no_of_docs = self.input.param("no_of_docs", 1000)
        jobs = Queue()
        results = []
        for dataset in datasets:
            if dataset.data_source is None:
                jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                          {"cluster": self.cluster, "collection_name": dataset.name,
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
                      {"cluster": self.cluster, "collection_name": datasets[i].name,
                       "dataverse_name": datasets[i].dataverse_name,
                       "database_name": datasets[i].database_name,
                       "destination_bucket": self.sink_s3_bucket_name,
                       "destination_link_name": s3_link.full_name, "path": path, "timeout": 3600,
                       "analytics_timeout": 3600, "compression": "gzip"}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.fail("Copy to command failed for some datasets")

        path_on_external_container = "{copy_dataset:string}"
        dataset_obj = self.cbas_util.create_external_dataset_obj(self.cluster,
                                                                 external_container_names={
                                                                     self.sink_s3_bucket_name: self.aws_region},
                                                                 paths_on_external_container=[
                                                                     path_on_external_container],
                                                                 file_format="json")[0]
        if not self.create_external_dataset(dataset_obj):
            self.fail("Failed to create external dataset on destination S3 bucket")

        files = perform_S3_operation(aws_access_key=self.aws_access_key,
                                     aws_secret_key=self.aws_secret_key,
                                     region=self.aws_region,
                                     aws_session_token=self.aws_session_token,
                                     bucket_name=self.sink_s3_bucket_name,
                                     get_bucket_objects=True)

        objects_in_s3 = [str(x) for x in files]

        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            for obj in objects_in_s3:
                if not obj.endswith('.gzip'):
                    self.fail("Not all files are gzip")
            statement = "select count(*) from {0} where copy_dataset = \"{1}\"".format(dataset_obj.full_name, path)
            status, metrics, errors, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster, statement)
            doc_count_in_dataset = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster,
                                                                                datasets[i].full_name)
            if result[0]['$1'] != doc_count_in_dataset:
                self.log.error("Document count mismatch in S3 dataset {0} and remote dataset {1}".format(
                    dataset_obj.full_name, datasets[i].full_name
                ))
            results.append(result[0]['$1'] == doc_count_in_dataset)

        if not all(results):
            self.fail("The document count does not match in remote source and S3")

    def test_create_copyToS3_from_collection_with_max_object_compression_drop_standalone_collection(self):
        self.base_infra_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        s3_link = self.cbas_util.get_all_link_objs("s3")[0]
        max_object_per_file = self.input.param("max_object_per_file", 100)
        no_of_docs = self.input.param("no_of_docs", 10000)
        jobs = Queue()
        results = []
        for dataset in datasets:
            if dataset.data_source is None:
                jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                          {"cluster": self.cluster, "collection_name": dataset.name,
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
                      {"cluster": self.cluster, "collection_name": datasets[i].name,
                       "dataverse_name": datasets[i].dataverse_name,
                       "database_name": datasets[i].database_name,
                       "destination_bucket": self.sink_s3_bucket_name,
                       "destination_link_name": s3_link.full_name, "path": path, "timeout": 3600,
                       "analytics_timeout": 3600, "compression": "gzip", "max_object_per_file": max_object_per_file}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.fail("Copy to command failed for some datasets")

        path_on_external_container = "{copy_dataset:string}"
        dataset_obj = self.cbas_util.create_external_dataset_obj(self.cluster,
                                                                 external_container_names={
                                                                     self.sink_s3_bucket_name: self.aws_region},
                                                                 paths_on_external_container=[
                                                                     path_on_external_container],
                                                                 file_format="json")[0]
        if not self.create_external_dataset(dataset_obj):
            self.fail("Failed to create external dataset on destination S3 bucket")

        files = perform_S3_operation(aws_access_key=self.aws_access_key,
                                     aws_secret_key=self.aws_secret_key,
                                     region=self.aws_region,
                                     aws_session_token=self.aws_session_token,
                                     bucket_name=self.sink_s3_bucket_name,
                                     get_bucket_objects=True)

        objects_in_s3 = [str(x) for x in files]

        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            no_of_files_at_path = [x for x in objects_in_s3 if x.startswith(path)]
            doc_count_in_dataset = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster,
                                                                                datasets[i].full_name)
            if len(no_of_files_at_path) < math.ceil(doc_count_in_dataset / max_object_per_file):
                self.fail("Number of files expected: {0}, actual: {1}".format(math.ceil(doc_count_in_dataset /
                                                                                        max_object_per_file),
                                                                              len(no_of_files_at_path)))
            statement = "select count(*) from {0} where copy_dataset = \"{1}\"".format(dataset_obj.full_name, path)
            status, metrics, errors, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster, statement)
            if result[0]['$1'] != doc_count_in_dataset:
                self.log.error("Document count mismatch in S3 dataset {0} and remote dataset {1}".format(
                    dataset_obj.full_name, datasets[i].full_name
                ))
            results.append(result[0]['$1'] == doc_count_in_dataset)

        if not all(results):
            self.fail("The document count does not match in remote source and S3")

    def test_create_copyToS3_from_collection_to_non_empty_s3_bucket(self):
        self.base_infra_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        s3_link = self.cbas_util.get_all_link_objs("s3")[0]
        no_of_docs = self.input.param("no_of_docs", 10000)
        jobs = Queue()
        results = []
        self.log.info("Adding {} documents in standalone dataset. Default doc size is 1KB".format(no_of_docs))
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name
                          , "no_of_docs": no_of_docs}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Some documents were not inserted")

        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.cluster, "collection_name": datasets[i].name,
                       "dataverse_name": datasets[i].dataverse_name,
                       "database_name": datasets[i].database_name,
                       "destination_bucket": self.sink_s3_bucket_name,
                       "destination_link_name": s3_link.full_name, "path": path}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Failed to execute copy to statement")

        path_on_external_container = "{copy_dataset:string}"
        # create external dataset on the S3 bucket
        dataset_obj = self.cbas_util.create_external_dataset_obj(self.cluster,
                                                                 external_container_names={
                                                                     self.sink_s3_bucket_name: self.aws_region},
                                                                 paths_on_external_container=[
                                                                     path_on_external_container],
                                                                 file_format="json")[0]

        if not self.create_external_dataset(dataset_obj):
            self.fail("Failed to create external dataset on destination S3 bucket")

        # Verify the data in dataset and s3
        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            statement = "select count(*) from {0} where copy_dataset = \"{1}\"".format(dataset_obj.full_name, path)
            status, metrics, errors, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster, statement)
            doc_count_in_dataset = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, datasets[i].full_name)
            if result[0]['$1'] != doc_count_in_dataset:
                self.log.error("Document count mismatch in S3 dataset {0} and dataset {1}".format(
                    dataset_obj.full_name, datasets[i].full_name
                ))
            results.append(result[0]['$1'] == doc_count_in_dataset)
        if not all(results):
            self.fail("The document count does not match in dataset and S3")

        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i) + "_2"
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.cluster, "collection_name": datasets[i].name,
                       "dataverse_name": datasets[i].dataverse_name,
                       "database_name": datasets[i].database_name,
                       "destination_bucket": self.sink_s3_bucket_name,
                       "destination_link_name": s3_link.full_name, "path": path}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Failed to execute copy to statement")

        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i) + "_2"
            statement = "select count(*) from {0} where copy_dataset = \"{1}\"".format(dataset_obj.full_name, path)
            status, metrics, errors, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster, statement)
            doc_count_in_dataset = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, datasets[i].full_name)
            if result[0]['$1'] != doc_count_in_dataset:
                self.log.error("Document count mismatch in S3 dataset {0} and dataset {1}".format(
                    dataset_obj.full_name, datasets[i].full_name
                ))
            results.append(result[0]['$1'] == doc_count_in_dataset)
        if not all(results):
            self.fail("The document count does not match in dataset and S3")

    def test_create_copyToS3_from_collection_to_non_existing_s3_bucket(self):
        self.base_infra_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        s3_link = self.cbas_util.get_all_link_objs("s3")[0]
        jobs = Queue()

        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.cluster, "collection_name": datasets[i].name,
                       "dataverse_name": datasets[i].dataverse_name,
                       "database_name": datasets[i].database_name,
                       "destination_bucket": self.cbas_util.generate_name(),
                       "destination_link_name": s3_link.full_name, "path": path, "validate_error_msg": True,
                       "expected_error": "External sink error. software.amazon.awssdk.services.s3.model.NoSuchBucketException: The specified bucket does not exist",
                       "expected_error_code": 24230}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.fail("Failed to execute copy to statement with error")

    def test_create_copyToS3_from_collection_to_non_existing_directory_s3_bucket(self):
        self.base_infra_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        s3_link = self.cbas_util.get_all_link_objs("s3")[0]
        no_of_docs = self.input.param("no_of_docs", 100)
        jobs = Queue()
        results = []
        self.log.info("Adding {} documents in standalone dataset. Default doc size is 1KB".format(no_of_docs))
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name,
                       "database_name": dataset.database_name
                          , "no_of_docs": no_of_docs}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Some documents were not inserted")

        results = []
        for i in range(len(datasets)):
            path = "main/copy_dataset_" + str(i)
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.cluster, "collection_name": datasets[i].name,
                       "dataverse_name": datasets[i].dataverse_name,
                       "database_name": datasets[i].database_name,
                       "destination_bucket": self.sink_s3_bucket_name,
                       "destination_link_name": s3_link.full_name, "path": path}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.fail("Failed to execute copy to statement")

        path_on_external_container = "main/{copy_dataset:string}"
        # create external dataset on the S3 bucket
        dataset_obj = self.cbas_util.create_external_dataset_obj(self.cluster,
                                                                 external_container_names={
                                                                     self.sink_s3_bucket_name: self.aws_region},
                                                                 paths_on_external_container=[
                                                                     path_on_external_container],
                                                                 file_format="json")[0]
        if not self.create_external_dataset(dataset_obj):
            self.fail("Failed to create external dataset on destination S3 bucket")

        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            statement = "select count(*) from {0} where copy_dataset = \"{1}\"".format(dataset_obj.full_name, path)
            status, metrics, errors, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster, statement)
            doc_count_in_dataset = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, datasets[i].full_name)
            if result[0]['$1'] != doc_count_in_dataset:
                self.log.error("Document count mismatch in S3 dataset {0} and dataset {1}".format(
                    dataset_obj.full_name, datasets[i].full_name
                ))
            results.append(result[0]['$1'] == doc_count_in_dataset)
        if not all(results):
            self.fail("The document count does not match in dataset and S3")

    def test_create_copyToS3_from_dynamic_prefix_collection_query_drop_standalone_collection(self):
        self.base_infra_setup()
        path_on_external_container = "{country:string}"
        # create external dataset on the S3 bucket
        no_of_dynamic_collection = self.input.param("no_of_dynamic_collection", 1)
        for i in range(no_of_dynamic_collection):
            dataset_obj = self.cbas_util.create_external_dataset_obj(self.cluster,
                                                                     external_container_names={
                                                                         self.s3_source_bucket: self.aws_region},
                                                                     paths_on_external_container=[
                                                                         path_on_external_container],
                                                                     file_format="json")[0]

            if not self.create_external_dataset(dataset_obj):
                self.fail("Failed to create external dataset on destination S3 bucket")

        datasets = self.cbas_util.get_all_dataset_objs("external")
        s3_link = self.cbas_util.get_all_link_objs("s3")[0]
        jobs = Queue()
        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            query = "select * from {0} where country = \"{1}\"".format(datasets[i].full_name, "Dominican Republic")
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.cluster, "source_definition_query": query, "alias_identifier": "ally",
                       "destination_bucket": self.sink_s3_bucket_name,
                       "destination_link_name": s3_link.full_name, "path": path, "timeout": 3600,
                       "analytics_timeout": 3600}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        path_on_external_container = "{copy_dataset:string}"
        dataset_obj = self.cbas_util.create_external_dataset_obj(self.cluster,
                                                                 external_container_names={
                                                                     self.sink_s3_bucket_name: self.aws_region},
                                                                 paths_on_external_container=[
                                                                     path_on_external_container],
                                                                 file_format="json")[0]

        if not self.create_external_dataset(dataset_obj):
            self.fail("Failed to create external dataset on destination S3 bucket")

        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            statement_dataset = "select count(*) from {0} where country = \"{1}\"".format(datasets[i].full_name,
                                                                                          "Dominican Republic")
            dynamic_copy_result = "select count(*) from {0} where copy_dataset = \"{1}\"".format(dataset_obj.full_name,
                                                                                                 path)
            status, metrics, errors, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster,
                                                                                               statement_dataset)
            status, metrics, errors, result1, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster,
                                                                                                dynamic_copy_result)

            if result[0]['$1'] != result1[0]['$1']:
                self.log.error("Document count mismatch in S3 dataset {0} and remote dataset {1}".format(
                    dataset_obj.full_name, dataset_obj[0].full_name
                ))
            results.append(result[0]['$1'] == result1[0]['$1'])

        if not all(results):
            self.fail("The document count does not match in dataset and S3")

    def test_create_copyToS3_from_collection_order_by_query_drop_standalone_collection(self):
        # blocked by MB-60394
        self.base_infra_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        s3_link = self.cbas_util.get_all_link_objs("s3")[0]
        no_of_docs = self.input.param("no_of_docs", 10000)

        jobs = Queue()
        results = []
        self.log.info("Adding {} documents in standalone dataset. Default doc size is 1KB".format(no_of_docs))
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name
                          , "no_of_docs": no_of_docs}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Some documents were not inserted")

        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.cluster, "collection_name": datasets[i].name,
                       "dataverse_name": datasets[i].dataverse_name,
                       "database_name": datasets[i].database_name,
                       "alias_identifier": "ally",
                       "destination_bucket": self.sink_s3_bucket_name, "order_by": "ally.avg_rating DESC",
                       "destination_link_name": s3_link.full_name, "path": path}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Failed to execute copy to statement")

        path_on_external_container = "{copy_dataset:string}"
        # create external dataset on the S3 bucket
        dataset_obj = self.cbas_util.create_external_dataset_obj(self.cluster,
                                                                 external_container_names={
                                                                     self.sink_s3_bucket_name: self.aws_region},
                                                                 paths_on_external_container=[
                                                                     path_on_external_container],
                                                                 file_format="json")[0]

        if not self.create_external_dataset(dataset_obj):
            self.fail("Failed to create external dataset on destination S3 bucket")

        verification_file = [str(x) for x in perform_S3_operation(aws_access_key=self.aws_access_key,
                                                                  aws_secret_key=self.aws_secret_key,
                                                                  region=self.aws_region,
                                                                  aws_session_token=self.aws_session_token,
                                                                  bucket_name=self.sink_s3_bucket_name,
                                                                  get_bucket_objects=True)][0]
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            file_to_download = [x for x in verification_file if x.startswith(path)][0]
            dest_path = os.path.join(os.path.dirname(__file__), "download.json")
            _ = perform_S3_operation(aws_access_key=self.aws_access_key,
                                     aws_secret_key=self.aws_secret_key,
                                     region=self.aws_region,
                                     aws_session_token=self.aws_session_token,
                                     bucket_name=self.sink_s3_bucket_name,
                                     download_file=True, src_path=file_to_download,
                                     dest_path=dest_path
                                     )

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
            status, metrics, errors, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster, statement)
            doc_count_in_dataset = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, datasets[i].full_name)
            if result[0]['$1'] != doc_count_in_dataset:
                self.log.error("Document count mismatch in S3 dataset {0} and dataset {1}".format(
                    dataset_obj.full_name, datasets[i].full_name
                ))
            results.append(result[0]['$1'] == doc_count_in_dataset)
        if not all(results):
            self.fail("The document count does not match in dataset and S3")

    def test_create_copyToS3_from_collection_multiple_order_by_query_drop_standalone_collection(self):
        # blocked by MB-60394
        self.base_infra_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        s3_link = self.cbas_util.get_all_link_objs("s3")[0]
        no_of_docs = self.input.param("no_of_docs", 10000)

        jobs = Queue()
        results = []
        self.log.info("Adding {} documents in standalone dataset. Default doc size is 1KB".format(no_of_docs))
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name
                          , "no_of_docs": no_of_docs}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Some documents were not inserted")

        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.cluster, "collection_name": datasets[i].name,
                       "dataverse_name": datasets[i].dataverse_name,
                       "database_name": datasets[i].database_name,
                       "alias_identifier": "ally",
                       "destination_bucket": self.sink_s3_bucket_name,
                       "order_by": "ally.avg_rating DESC, ally.country DESC",
                       "destination_link_name": s3_link.full_name, "path": path}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Failed to execute copy to statement")

        path_on_external_container = "{copy_dataset:string}"
        # create external dataset on the S3 bucket
        dataset_obj = self.cbas_util.create_external_dataset_obj(self.cluster,
                                                                 external_container_names={
                                                                     self.sink_s3_bucket_name: self.aws_region},
                                                                 paths_on_external_container=[
                                                                     path_on_external_container],
                                                                 file_format="json")[0]

        if not self.create_external_dataset(dataset_obj):
            self.fail("Failed to create external dataset on destination S3 bucket")

        verification_file = [str(x) for x in perform_S3_operation(aws_access_key=self.aws_access_key,
                                                                  aws_secret_key=self.aws_secret_key,
                                                                  region=self.aws_region,
                                                                  aws_session_token=self.aws_session_token,
                                                                  bucket_name=self.sink_s3_bucket_name,
                                                                  get_bucket_objects=True)][0]

        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            file_to_download = [x for x in verification_file if x.startswith(path)][0]
            dest_path = os.path.join(os.path.dirname(__file__), "download.json")
            _ = perform_S3_operation(aws_access_key=self.aws_access_key,
                                     aws_secret_key=self.aws_secret_key,
                                     region=self.aws_region,
                                     aws_session_token=self.aws_session_token,
                                     bucket_name=self.sink_s3_bucket_name,
                                     download_file=True, src_path=file_to_download,
                                     dest_path=dest_path
                                     )

            json_data = []
            with open(dest_path, 'r') as json_file:
                # Load the JSON data from the file
                for line in json_file:
                    data = json.loads(line)
                    json_data.append(data)

            sorted_data = sorted(json_data, key=lambda x: (-x['avg_rating'], -x['country']))

            for dict1, dict2 in zip(json_data, sorted_data):
                if dict1 != dict2:
                    self.fail("The data is not in sorted order")

        # need to add data in file ordering checks
        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            statement = "select count(*) from {0} where copy_dataset = \"{1}\"".format(dataset_obj.full_name, path)
            status, metrics, errors, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster, statement)
            doc_count_in_dataset = self.cbas_util.get_num_items_in_cbas_dataset(self.cluster, datasets[i].full_name)
            if result[0]['$1'] != doc_count_in_dataset:
                self.log.error("Document count mismatch in S3 dataset {0} and dataset {1}".format(
                    dataset_obj.full_name, datasets[i].full_name
                ))
            results.append(result[0]['$1'] == doc_count_in_dataset)
        if not all(results):
            self.fail("The document count does not match in dataset and S3")

    def test_create_copyToS3_from_collection_invalid_link_drop_standalone_collection(self):
        self.base_infra_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        s3_link = self.cbas_util.get_all_link_objs("s3")[0]
        no_of_docs = self.input.param("no_of_docs", 100)
        jobs = Queue()
        results = []
        self.log.info("Adding {} documents in standalone dataset. Default doc size is 1KB".format(no_of_docs))
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name,
                       "database_name": dataset.database_name
                          , "no_of_docs": no_of_docs}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Some documents were not inserted")

        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            destination_link = self.cbas_util.generate_name()
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.cluster, "collection_name": datasets[i].name,
                       "dataverse_name": datasets[i].dataverse_name,
                       "database_name": datasets[i].database_name,
                       "destination_bucket": self.sink_s3_bucket_name,
                       "destination_link_name": destination_link, "path": path, "validate_error_msg": True,
                       "expected_error": "Link {0} does not exist".format(destination_link),
                       "expected_error_code": 24006}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.fail("Failed to execute copy to statement with error")

    def test_create_copyToS3_from_collection_to_different_region_existing_s3_bucket(self):
        self.base_infra_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        no_of_docs = self.input.param("no_of_docs", 100)
        jobs = Queue()
        results = []
        self.log.info("Adding {} documents in standalone dataset. Default doc size is 1KB".format(no_of_docs))

        external_link_obj = self.cbas_util.create_external_link_obj(self.cluster, accessKeyId=self.aws_access_key,
                                                                    secretAccessKey=self.aws_secret_key,
                                                                    regions=["us-west-1"])[0]
        if not self.cbas_util.create_external_link(self.cluster, external_link_obj.properties):
            self.fail("Failed to create S3 link on different region")

        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.cluster, "collection_name": datasets[i].name,
                       "dataverse_name": datasets[i].dataverse_name,
                       "destination_bucket": self.cbas_util.generate_name(),
                       "destination_link_name": external_link_obj.full_name, "path": path, "validate_error_msg": True,
                       "expected_error": "External sink error. software.amazon.awssdk.services.s3.model.NoSuchBucketException: The specified bucket does not exist",
                       "expected_error_code": 24230}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.fail("Failed to execute copy to statement with error")

    def test_create_copyToS3_from_multi_partition_field_to_s3_bucket(self):
        self.base_infra_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        no_of_docs = self.input.param("no_of_docs", 1000)
        s3_link = self.cbas_util.get_all_link_objs("s3")[0]
        jobs = Queue()
        results = []
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name,
                       "database_name": dataset.database_name
                          , "no_of_docs": no_of_docs}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Some documents were not inserted")

        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            jobs.put((self.cbas_util.copy_to_s3,
                      {
                          "cluster": self.cluster, "collection_name": datasets[i].name,
                          "dataverse_name": datasets[i].dataverse_name,
                          "database_name": datasets[i].database_name,
                          "alias_identifier": "ally",
                          "destination_bucket": self.sink_s3_bucket_name,
                          "destination_link_name": s3_link.full_name, "path": path, "partition_alias": "public_likes",
                          "partition_by": "ally.public_likes"}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.log.error("Failed to execute copy command")

        # need to add verification/ waiting for v2 of copy to s3

    def test_create_copyToS3_from_missing_partition_field_to_s3_bucket(self):
        self.base_infra_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        no_of_docs = self.input.param("no_of_docs", 1000)
        s3_link = self.cbas_util.get_all_link_objs("s3")[0]
        jobs = Queue()
        results = []
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name,
                       "database_name": dataset.database_name
                          , "no_of_docs": no_of_docs, "include_country": "mixed"}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Some documents were not inserted")

        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.cluster, "collection_name": datasets[i].name, "alias_identifier": "ally",
                       "dataverse_name": datasets[i].dataverse_name, "database_name": datasets[i].database_name,
                       "destination_bucket": self.sink_s3_bucket_name,
                       "destination_link_name": s3_link.full_name, "path": path, "partition_alias": "country",
                       "partition_by": "ally.country"}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.fail("Copy to S3 statement failure")
        # need to add verification/ waiting for v2 of copy to s3

    def test_create_copyToS3_from_multiple_partition_field_to_s3_bucket(self):
        self.base_infra_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        no_of_docs = self.input.param("no_of_docs", 1000)
        s3_link = self.cbas_util.get_all_link_objs("s3")[0]
        jobs = Queue()
        results = []
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name,
                       "database_name": dataset.database_name
                          , "no_of_docs": no_of_docs}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Some documents were not inserted")

        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)

            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.cluster, "collection_name": datasets[i].name, "alias_identifier": "ally",
                       "dataverse_name": datasets[i].dataverse_name, "database_name": datasets[i].database_name,
                       "destination_bucket": self.sink_s3_bucket_name,
                       "destination_link_name": s3_link.full_name, "path": path, "partition_alias": ["country", "city"],
                       "partition_by": ["ally.country", "ally.city"]}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.fail("Copy to S3 statement failure")

        # create external link on copied bucket
        path_on_external_container = "{copy_dataset:string}/{country:string}"
        dataset_obj = self.cbas_util.create_external_dataset_obj(self.cluster,
                                                                 external_container_names={
                                                                     self.sink_s3_bucket_name: self.aws_region},
                                                                 paths_on_external_container=[
                                                                     path_on_external_container],
                                                                 file_format="json")[0]
        if not self.create_external_dataset(dataset_obj):
            self.fail("Failed to create external dataset on destination S3 bucket")

        files = perform_S3_operation(aws_access_key=self.aws_access_key,
                                     aws_secret_key=self.aws_secret_key,
                                     region=self.aws_region,
                                     aws_session_token=self.aws_session_token,
                                     bucket_name=self.sink_s3_bucket_name,
                                     get_bucket_objects=True)
        objects_in_s3 = [str(x) for x in files]

        results = []
        dynamic_statement = "select count(*) from {0} where copy_dataset = \"{1}\" and country = \"{2}\""
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            statement = "select country, count(city) as cnt from {0} group by country;".format(datasets[i].full_name)
            status, metrics, errors, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster,
                                                                                               statement)
            get_city_count_per_country = [x for x in result if x['cnt'] > 1]
            for j in range(2):
                obj = random.choice(get_city_count_per_country)
                files_on_country = obj['cnt']
                country_name = obj['country']
                path_directory = "{0}/{1}".format(path, country_name)
                count = 0
                for obj in objects_in_s3:
                    if obj.startswith(path_directory):
                        count = count + 1
                if count < files_on_country:
                    self.log.error("Not all partitions created")
                results.append(count == files_on_country)

                statement = "select count(*) from {0} where country = \"{1}\"".format(datasets[i], country_name)
                statement2 = dynamic_statement.format(dataset_obj.full_name, path, country_name)
                status, metrics, errors, result1, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster,
                                                                                                    statement)
                status, metrics, errors, result2, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster,
                                                                                                    statement2)
                if result1[0]['$1'] != result2[0]['$1']:
                    self.log.error("Not all doc are copied for country {}".format(country_name))

                result.append(result1[0]['$1'] == result2[0]['$1'])

        if not all(results):
            self.fail("Verification failed for multiple partition")

    def test_create_copyToS3_from_collection_where_partition_already_exist_in_S3(self):
        self.base_infra_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        s3_link = self.cbas_util.get_all_link_objs("s3")[0]
        no_of_docs = self.input.param("no_of_docs", 1000)
        jobs = Queue()
        results = []
        for i in range(len(datasets)):
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.cluster, "collection_name": datasets[i].name,
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
                      {"cluster": self.cluster, "collection_name": datasets[i].name, "alias_identifier": "ally",
                       "dataverse_name": datasets[i].dataverse_name, "database_name": datasets[i].database_name,
                       "destination_bucket": self.sink_s3_bucket_name,
                       "destination_link_name": s3_link.full_name, "path": path, "partition_alias": "country",
                       "partition_by": "ally.country"}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.fail("Copy to S3 statement failure")

        files = perform_S3_operation(aws_access_key=self.aws_access_key,
                                     aws_secret_key=self.aws_secret_key,
                                     region=self.aws_region,
                                     aws_session_token=self.aws_session_token,
                                     bucket_name=self.sink_s3_bucket_name,
                                     get_bucket_objects=True)

        objects_in_s3 = [str(x) for x in files]
        if len(objects_in_s3) == 0:
            self.fail("Failed to execute copy statement, no objects present in S3 bucket")

        # copy again to same destination where partition already exists
        results = []
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.cluster, "collection_name": datasets[i].name, "alias_identifier": "ally",
                       "dataverse_name": datasets[i].dataverse_name, "database_name": datasets[i].database_name,
                       "destination_bucket": self.sink_s3_bucket_name,
                       "destination_link_name": s3_link.full_name, "path": path, "partition_alias": "country",
                       "partition_by": "ally.country", "validate_error_msg": True,
                       "expected_error": "Cannot write to a non-empty directory",
                       "expected_error_code": 23073}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.fail("Copy to S3 statement failure")

    def test_create_copyToS3_from_collection_empty_query_result_in_S3(self):
        self.base_infra_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        s3_link = self.cbas_util.get_all_link_objs("s3")[0]
        jobs = Queue()
        results = []

        # initiate copy command
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            query = "select * from {0} where 1 = 0".format(datasets[i].full_name)
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.cluster, "alias_identifier": "ally",
                       "source_definition_query": query,
                       "destination_bucket": self.sink_s3_bucket_name,
                       "destination_link_name": s3_link.full_name, "path": path, "partition_alias": "country",
                       "partition_by": "ally.country"}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.fail("Copy to S3 statement failure")

        files = perform_S3_operation(aws_access_key=self.aws_access_key,
                                     aws_secret_key=self.aws_secret_key,
                                     region=self.aws_region,
                                     aws_session_token=self.aws_session_token,
                                     bucket_name=self.sink_s3_bucket_name,
                                     get_bucket_objects=True)

        objects_in_s3 = [str(x) for x in files]
        if len(objects_in_s3) != 0:
            self.fail("Failed to execute copy statement, objects present for empty query in S3 bucket")

    def test_create_copyToS3_from_collection_aggregate_group_by_result_in_S3(self):
        self.base_infra_setup()
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        s3_link = self.cbas_util.get_all_link_objs("s3")[0]
        no_of_docs = self.input.param("no_of_docs", 1000)
        jobs = Queue()
        results = []
        for i in range(len(datasets)):
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.cluster, "collection_name": datasets[i].name,
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
                      {"cluster": self.cluster, "source_definition_query": query, "alias_identifier": "ally",
                       "destination_bucket": self.sink_s3_bucket_name,
                       "destination_link_name": s3_link.full_name, "path": path, "partition_alias": "country",
                       "partition_by": "ally.country"}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.fail("Copy to S3 statement failure")

        path_on_external_container_string = "{copy_dataset:string}/{country:string}"
        dataset_obj_string = self.cbas_util.create_external_dataset_obj(self.cluster,
                                                                        external_container_names={
                                                                            self.sink_s3_bucket_name: self.aws_region},
                                                                        paths_on_external_container=[
                                                                            path_on_external_container_string],
                                                                        file_format="json")[0]
        if not self.create_external_dataset(dataset_obj_string):
            self.fail("Failed to create external dataset on destination S3 bucket")

        # verification step
        for i in range(len(datasets)):
            path = "copy_dataset_" + str(i)
            for j in range(5):
                statement = "select * from {0} limit 1".format(datasets[i].full_name)
                status, metrics, errors, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster,
                                                                                                   statement)
                country_name = ((result[0])[CBASHelper.unformat_name(datasets[i].name)]["country"]).replace("&amp;", "")
                query_statement = "SELECT ARRAY_LENGTH(ARRAY_AGG(city)) as city FROM {0} where country = \"{1}\"".format(
                    datasets[i].full_name, str(country_name))
                dynamic_statement = "select * from {0} where copy_dataset = \"{1}\" and country = \"{2}\"".format(
                    dataset_obj_string.full_name, path, str(country_name))

                status, metrics, errors, result, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster,
                                                                                                   dynamic_statement)
                length_of_city_array_from_s3 = len((result[0][dataset_obj_string.name])['city'])

                status, metrics, errors, result2, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster,
                                                                                                    query_statement)

                length_of_city_array_from_dataset = result2[0]["city"]
                if length_of_city_array_from_s3 != length_of_city_array_from_dataset:
                    self.log.error("Length of city aggregate failed")
                results.append(length_of_city_array_from_s3 == length_of_city_array_from_dataset)

        if not all(results):
            self.fail("Copy to statement copied the wrong results")

    def test_mini_volume_copy_to_s3(self):
        primary_key = []
        self.base_infra_setup(primary_key)
        self.copy_to_s3_job = Queue()
        self.copy_to_s3_results = []
        self.mini_volume = MiniVolume(self, "http://127.0.0.1:4000")
        self.mini_volume.calculate_volume_per_source()
        # initiate copy to kv
        for i in range(1, 5):
            if i % 2 == 0:
                self.mini_volume.run_processes(i, 2 ** (i - 1), False)
            else:
                self.mini_volume.run_processes(i, 2 ** (i + 1), False)
            self.mini_volume.start_crud_on_data_sources(self.remote_start, self.remote_end)
            self.mini_volume.stop_process()
            self.mini_volume.stop_crud_on_data_sources()
            self.cbas_util.wait_for_ingestion_complete()
            datasets = self.cbas_util.get_all_dataset_objs()
            s3_link = self.cbas_util.get_all_link_objs("s3")[0]
            for j in range(len(datasets)):
                path = "copy_dataset_" + str(i) + datasets[j].full_name
                self.copy_to_s3_job.put((self.cbas_util.copy_to_s3,
                                         {"cluster": self.cluster, "collection_name": datasets[j].name,
                                          "dataverse_name": datasets[j].dataverse_name,
                                          "database_name": datasets[j].database_name,
                                          "destination_bucket": self.sink_s3_bucket_name,
                                          "destination_link_name": s3_link.full_name,
                                          "path": path}))
            self.cbas_util.run_jobs_in_parallel(
                self.copy_to_s3_job, self.copy_to_s3_results, self.sdk_clients_per_user, async_run=False)

            if not all(self.copy_to_s3_results):
                self.log.error("Some documents were not inserted")
