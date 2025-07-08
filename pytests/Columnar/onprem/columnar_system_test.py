"""
Created on 2025
@author: Anisha Sinha
"""

import time
import json
import random
from queue import Queue
import threading

from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from pytests.Columnar.onprem.columnar_onprem_base import ColumnarOnPremBase
from couchbase_utils.security_utils.x509main import x509main
from cbas_utils.cbas_utils_on_prem import CBASRebalanceUtil
from cb_server_rest_util.rest_client import RestConnection
from CbasLib.CBASOperations_PythonSDK import CBASHelper


class ColumnarOnPremSystemTest(ColumnarOnPremBase):
    """
    System test class for onprem columnar testing
    """
    

    def setUp(self):
        super(ColumnarOnPremSystemTest, self).setUp()

        if self._testMethodDoc:
            self.log.info("Starting Test: %s - %s"
                          % (self._testMethodName, self._testMethodDoc))
        else:
            self.log.info("Starting Test: %s" % self._testMethodName)

        # Get scope and collection configuration from yml file
        self.num_scopes = self.input.param("scopes", 2)  # Default from yml
        self.num_collections = self.input.param("collections", 4)  # Default from yml
        self.num_remote_links = self.input.param("remote_links", 1)
        self.num_remote_collections = self.input.param("num_remote_collections", 1)
        self.num_external_collections = self.input.param("num_external_collections", 2)
        self.columnar_spec_name = self.input.param(
            "columnar_spec_name", "bvf_e2e")
        self.columnar_spec = self.columnar_cbas_utils.get_columnar_spec(
            self.columnar_spec_name)
        self.copy_to_s3_bucket_created = False
        self.create_start_index = 0
        self.rebalance_util = CBASRebalanceUtil(
            self.cluster_util, self.bucket_util, self.task,
            self.input.param("vbucket_check", True), self.cbas_util)
        self.steady_state_workload_sleep = self.input.param("steady_state_workload_sleep", 600)
        self.cbas_helper = CBASHelper(self.analytics_cluster)

    def tearDown(self):
        self.columnar_cbas_utils.cleanup_cbas(self.analytics_cluster)
        self.log.info("Delete the copy to bucket created on Netapp storage.")
        if self.copy_to_s3_bucket_created:
            self.log.info("Deleting AWS S3 bucket for Copy to S3 - {}".format(
                self.sink_blob_bucket_name))
            if not self.columnar_s3_obj.delete_bucket(self.sink_blob_bucket_name):
                self.log.error("AWS bucket failed to delete")
        super(ColumnarOnPremSystemTest, self).tearDown()

    def build_cbas_columnar_infra(self):
        num_databases = self.input.param("num_db", 1)
        num_dataverses = self.input.param("num_dv", 1)

        num_remote_links = self.num_remote_links
        num_external_links = self.input.param("num_external_links", 1)
        num_kafka_links = self.input.param("num_kafka_links", 0)

        num_remote_collections = self.num_remote_collections
        num_external_collections = self.input.param("num_external_collections",
                                                    1)
        # This defines number of standalone collections
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

        # S3 credentials
        self.s3_access_key = self.input.param("s3_access_key", "")
        self.s3_secret_key = self.input.param("s3_secret_key", "")
        self.s3_region = self.input.param("s3_region", "us-west-1")
        self.s3_endpoint = self.input.param("s3_endpoint", "")

        self.columnar_spec["external_link"]["properties"] = [{
            "type": "s3",
            "region": self.s3_region,
            "accessKeyId": self.s3_access_key,
            "secretAccessKey": self.s3_secret_key,
            "serviceEndpoint": self.s3_endpoint
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
            prop["region"] = self.aws_region
            # prop["path_on_external_container"] = ("level_{level_no:int}_"
            #                                       "folder_{folder_no:int}")
            prop["path_on_external_container"] = None
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

    def add_mutate_data_on_remote_server(self, done_event):
        """
        Add data to the remote server and mutate data on the remote server.
        """

        while not done_event.is_set():
            self.log.info("Mutating data on remote cluster.")
            self.log.info("Inserting items in remote dataset. start_index {0} num_items {1}".format(
                int(self.create_start_index),
                int(self.num_items / 10)))
            self.load_remote_collections(self.remote_cluster,
                                         create_start_index=self.create_start_index,
                                         create_end_index=self.create_start_index + self.num_items / 10,
                                         wait_for_completion=True,
                                         template="Product")

            self.create_start_index = self.create_start_index + self.num_items / 10
            self.log.info("Upserting items in remote dataset. num_items {0}".format(int(self.create_start_index)))
            self.load_remote_collections(self.remote_cluster,
                                         update_start_index=0,
                                         update_end_index=self.create_start_index,
                                         wait_for_completion=True,
                                         template="Product")

            self.log.info("Sleep for 5 mins before next mutation.")
            time.sleep(300)

    def rebalance_nodes(self, done_event):
        """
        Rebalance in,out and swap the cluster.
        """

        while not done_event.is_set():
            self.log.info("Step 1: Rebalance-In a KV+CBAS node in analytics cluster")
            rebalance_task, self.analytics_cluster.available_servers = \
                self.rebalance_util.rebalance(
                    cluster=self.analytics_cluster, cbas_nodes_in=1,
                    available_servers=self.analytics_cluster.available_servers,
                    in_node_services="kv,cbas",
                    wait_for_complete=True)
            if not rebalance_task.result:
                self.fail("Error while Rebalance-In KV+CBAS node in analytics "
                          "cluster")
            self.sleep(self.steady_state_workload_sleep,
                       "Wait after rebalance in for {} seconds".format(self.steady_state_workload_sleep))

            self.log.info("Step 2: Rebalance-Out a KV+CBAS node in analytics cluster")
            rebalance_task, self.analytics_cluster.available_servers = \
                self.rebalance_util.rebalance(
                cluster=self.analytics_cluster, cbas_nodes_out=1,
                available_servers=self.analytics_cluster.available_servers,
                wait_for_complete=True)
            if not rebalance_task.result:
                self.fail("Error while Rebalance-Out KV+CBAS node in analytics "
                          "cluster")
            self.sleep(self.steady_state_workload_sleep,
                       "Wait after rebalance out for {} seconds".format(self.steady_state_workload_sleep))
            self.analytics_cluster.rest = RestConnection(self.analytics_cluster.cbas_cc_node)

            self.log.info("Step 3: Rebalance-swap a KV+CBAS node in analytics cluster")
            rebalance_task, self.analytics_cluster.available_servers = \
                self.rebalance_util.rebalance(
                cluster=self.analytics_cluster, cbas_nodes_in=1, cbas_nodes_out=1,
                available_servers=self.analytics_cluster.available_servers,
                in_node_services="kv,cbas",
                wait_for_complete=True)
            if not rebalance_task.result:
                self.fail("Error while Rebalance-Swap KV+CBAS node in analytics "
                          "cluster")
            self.analytics_cluster.rest = RestConnection(self.analytics_cluster.cbas_cc_node)
            self.sleep(self.steady_state_workload_sleep,
                       "Wait after rebalance swap for {} seconds".format(self.steady_state_workload_sleep))

    def run_query_on_remote_dataset(self, done_event):
        """
        Run queries on remote datasets.
        """

        # Get all remote datasets
        remote_datasets = self.columnar_cbas_utils.get_all_dataset_objs("remote")
        if not remote_datasets:
            self.fail("No remote datasets found")

        # Run queries on each remote dataset
        for dataset in remote_datasets:
            # Define the queries for this dataset
            queries = [
                f"SELECT c.category, p.product_name, p.num_sold FROM {dataset.full_name} p JOIN (SELECT product_category, MAX(num_sold) AS max_sold FROM {dataset.full_name} GROUP BY product_category) c ON p.product_category = c.product_category AND p.num_sold = c.max_sold GROUP BY c.category, p.product_name, p.num_sold ORDER BY c.category, p.num_sold DESC;",
                f"SELECT s.seller_name, AVG(p.avg_rating) AS avg_rating, SUM(p.price * p.num_sold) AS total_revenue FROM {dataset.full_name} p JOIN (SELECT seller_name, COUNT(*) AS product_count FROM {dataset.full_name} GROUP BY seller_name HAVING COUNT(*) >= 10) s ON p.seller_name = s.seller_name GROUP BY s.seller_name ORDER BY total_revenue DESC;",
                f"SELECT c.category, p.product_name, p.price, AVG(r.product_rating.rating_value) AS avg_rating, COUNT(r.product_rating) AS num_reviews FROM {dataset.full_name} p LEFT JOIN (SELECT product_name, product_rating FROM {dataset.full_name}) r ON p.product_name = r.product_name JOIN (SELECT product_category, MAX(price) AS max_price FROM {dataset.full_name} GROUP BY product_category) c ON p.product_category = c.product_category AND p.price = c.max_price GROUP BY c.category, p.product_name, p.price ORDER BY c.category, p.price DESC limit 1000;",
                f"SELECT p.seller_name, p.seller_location, AVG(p.avg_rating) AS avg_rating FROM (SELECT seller_name, seller_location, avg_rating FROM {dataset.full_name} WHERE 'Passenger car medium' in product_category ) p GROUP BY p.seller_name, p.seller_location ORDER BY avg_rating DESC;"
            ]

            # Run queries in parallel for 2 hrs
            start_time = time.time()
            end_time = start_time + 7200

            self.log.info("Starting parallel query execution on remote datasets for 60 minutes")
            while time.time() < end_time:
                try:
                    # Execute queries
                    for query in queries:
                        self.cbas_helper.execute_statement_on_cbas(query, None)
                except Exception as e:
                    self.log.info("Query failed due to: {}", str(e))
                time.sleep(5)  # Small delay to prevent overwhelming the system

        self.log.info("Completed parallel query execution on remote datasets")
        done_event.set()

    def run_query_on_external_collections(self):
        """
        Run queries on external collections.
        """

        # Run queries on each external dataset
        for dataset_name in self.external_datasets:
            # Define the queries for this dataset
            queries = [
                f"SELECT c.category, p.product_name, p.num_sold FROM {dataset_name} p JOIN (SELECT product_category, MAX(num_sold) AS max_sold FROM {dataset_name} GROUP BY product_category) c ON p.product_category = c.product_category AND p.num_sold = c.max_sold GROUP BY c.category, p.product_name, p.num_sold ORDER BY c.category, p.num_sold DESC;",
                f"SELECT s.seller_name, AVG(p.avg_rating) AS avg_rating, SUM(p.price * p.num_sold) AS total_revenue FROM {dataset_name} p JOIN (SELECT seller_name, COUNT(*) AS product_count FROM {dataset_name} GROUP BY seller_name HAVING COUNT(*) >= 10) s ON p.seller_name = s.seller_name GROUP BY s.seller_name ORDER BY total_revenue DESC;",
                f"SELECT c.category, p.product_name, p.price, AVG(r.product_rating.rating_value) AS avg_rating, COUNT(r.product_rating) AS num_reviews FROM {dataset_name} p LEFT JOIN (SELECT product_name, product_rating FROM {dataset_name}) r ON p.product_name = r.product_name JOIN (SELECT product_category, MAX(price) AS max_price FROM {dataset_name} GROUP BY product_category) c ON p.product_category = c.product_category AND p.price = c.max_price GROUP BY c.category, p.product_name, p.price ORDER BY c.category, p.price DESC limit 1000;",
                f"SELECT p.seller_name, p.seller_location, AVG(p.avg_rating) AS avg_rating FROM (SELECT seller_name, seller_location, avg_rating FROM {dataset_name} WHERE 'Passenger car medium' in product_category ) p GROUP BY p.seller_name, p.seller_location ORDER BY avg_rating DESC;"
            ]

            # Run queries in parallel for 2 hrs
            start_time = time.time()
            end_time = start_time + 7200

            self.log.info("Starting parallel query execution on external datasets for 60 minutes")
            while time.time() < end_time:
                # Execute queries.
                for query in queries:
                    self.cbas_helper.execute_statement_on_cbas(query, None)
                time.sleep(5)  # Small delay to prevent overwhelming the system

        self.log.info("Completed parallel query execution on external datasets")

    def test_system_columnar_operations(self):
        """
        Test system columnar operations.
        """
        self.log.info("Starting test columnar operations test")

        self.log.info("Creating Buckets, Scopes and Collection on Remote "
                      "cluster.")
        self.collectionSetUp(cluster=self.remote_cluster, load_data=False, create_sdk_clients=False)
        for bucket in self.remote_cluster.buckets:
            SiriusCouchbaseLoader.create_clients_in_pool(
                self.remote_cluster.master, self.remote_cluster.master.rest_username,
                self.remote_cluster.master.rest_password,
                bucket.name, req_clients=1)
        self.log.info("Started Doc loading on remote cluster")

        self.load_remote_collections(self.remote_cluster,
                                     create_start_index=self.create_start_index,
                                     create_end_index=self.num_items,
                                     template="Product")
        self.create_start_index = self.num_items
        self.log.info("Creating analytics entities")
        self.build_cbas_columnar_infra()

        # wait for initial ingestion to complete.
        self.log.info("Waiting for initial ingestion into Remote dataset")
        for dataset in self.columnar_cbas_utils.get_all_dataset_objs("remote"):
            if not self.columnar_cbas_utils.wait_for_ingestion_complete(
                    self.analytics_cluster, dataset.full_name,
                    dataset.num_of_items, 72000):
                self.fail("FAILED: Initial ingestion into {}.".format(
                    dataset.full_name))

        self.log.info("Run queries, mutations and rebalance in parallel.")
        done_event = threading.Event()
        t1 = threading.Thread(target=self.run_query_on_remote_dataset, args=(done_event,))
        t2 = threading.Thread(target=self.add_mutate_data_on_remote_server, args=(done_event,))
        t3 = threading.Thread(target=self.rebalance_nodes, args=(done_event,))

        t1.start()
        t2.start()
        t3.start()
        t1.join()
        t2.join()
        t3.join()

        # Create external link for NetApp Storage
        self.log.info("Creating external link for NetApp Storage")
        external_link_properties = {
            "accessKeyId": self.aws_access_key,
            "secretAccessKey": self.aws_secret_key,
            "region": self.aws_region,
            "serviceEndpoint": self.aws_endpoint,
            "name": "netapp_external_link",
            "type": "s3"
        }

        if not self.columnar_cbas_utils.create_external_link(
                self.analytics_cluster,
                link_properties=external_link_properties):
            self.fail("Failed to create external link for AWS")
        self.log.info("Successfully created external link for AWS")

        # Create external datasets for different file formats
        file_formats = ["json", "parquet", "avro"]

        self.log.info("Create external datasets for the file formats.")
        self.external_datasets = []
        for i in range(self.num_external_collections):
            for file_format in file_formats:
                dataset_name = "netapp_external_" + file_format + "_" + str(i)
                self.external_datasets.append(dataset_name)
                dataset_container_name = 'columnar-functional-sanity-test-data'
                if not self.cbas_util.create_dataset_on_external_resource(cluster=self.columnar_cluster,
                                                                          dataset_name=dataset_name,
                                                                          external_container_name=dataset_container_name,
                                                                          link_name="netapp_external_link",
                                                                          file_format=file_format,
                                                                          include="*." + file_format):
                    self.fail("Failed to create dataset on netapp storage.")

        self.log.info("Run query on external collections.")
        self.run_query_on_external_collections()

        self.log.info("Copy to Netapp Storage.")
        self.sink_blob_bucket_name = "copy-to-blob-" + str(random.randint(1, 100000))
        self.copy_to_s3_bucket_created = True
        self.columnar_s3_obj.create_bucket(self.sink_blob_bucket_name, self.aws_region)
        jobs = Queue()
        results = []
        for i, dataset in enumerate(self.columnar_cbas_utils.get_all_dataset_objs("remote")):
            path = "copy_dataset_" + str(i)
            jobs.put((self.cbas_util.copy_to_s3,
                      {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name,
                       "destination_bucket": self.sink_blob_bucket_name,
                       "destination_link_name": "netapp_external_link",
                       "path": path}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.fail("Copy to netapp storage statement failure")
