'''
Created on 25-OCTOBER-2023

@author: umang.agrawal
'''

from Queue import Queue
import time

from CbasLib.cbas_entity import ExternalDB
from Goldfish.goldfish_base import GoldFishBaseTest
from CbasLib.CBASOperations import CBASHelper


class StandaloneCollectionDynamo(GoldFishBaseTest):

    def setUp(self):
        super(StandaloneCollectionDynamo, self).setUp()

        # Since all the test cases are being run on 1 cluster only
        self.cluster = self.list_all_clusters()[0]

        if not self.gf_spec_name:
            self.gf_spec_name = "sanity.standalone_collection_on_external_db"
        self.gf_spec = self.cbas_util.get_goldfish_spec(
            self.gf_spec_name)

        self.dynamo_access_key = self.input.param("aws_access_key")
        self.dynamo_secret_key = self.input.param("aws_secret_key")
        self.dynamo_region = self.input.param("dynamo_region", "us-east-2")

        self.initial_doc_count = self.input.param("initial_doc_count", 100)

        self.dynamo_table = "sanity-test-{}".format(
            self.cbas_util.generate_name(max_length=5))

        self.loader_id = None

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)

        if self.loader_id:
            resp = self.doc_loading_APIs.stop_crud_on_dynamo(self.loader_id)
            if resp.status_code != 200 or resp.json()["status"] != "stopped":
                self.log.error("Failed to stop CRUD on Dynamo DB")

        resp = self.doc_loading_APIs.delete_dynamo_table(
            self.dynamo_table, self.dynamo_access_key, self.dynamo_secret_key,
            self.dynamo_region)
        if resp.status_code != 200:
            self.fail("Error while deleting dynamo table - {}".format(
                self.dynamo_table))

        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.cluster, self.gf_spec):
            self.fail("Error while deleting cbas entities")

        super(StandaloneCollectionDynamo, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage="Teardown")

    def start_initial_data_load(self):
        resp = self.doc_loading_APIs.start_dynamo_loader(
            self.dynamo_access_key, self.dynamo_secret_key, "id",
            self.dynamo_table, self.dynamo_region, self.initial_doc_count,
            True)
        if resp.status_code != 200:
            self.log.error("Failed to load initial docs into Dynamo")
            return False
        return True

    def wait_for_initial_data_load(self, expected_count, timeout=600):
        endtime = time.time() + timeout
        doc_count = 0
        self.log.info("waiting for initial data load in dynamo to finish")
        while time.time() < endtime:
            resp = self.doc_loading_APIs.count_dynamo_documents(
                self.dynamo_access_key, self.dynamo_secret_key,
                self.dynamo_region, self.dynamo_table)
            if resp.status_code == 200:
                doc_count = resp.json()["count"]
                if doc_count == expected_count:
                    return True
            else:
                self.log.error("Failed to fetch dynamo doc count. Retrying...")
        self.log.error("Initial data loading on Dynamo did not finish within "
                       "stipulated time. Doc Count - Actual - {}, Expected - "
                       "{}".format(doc_count, expected_count))
        return False

    def perform_CRUD_on_dynamo(self, wait_time=300):
        resp = self.doc_loading_APIs.start_crud_on_dynamo(
            self.dynamo_access_key, self.dynamo_secret_key, "id",
            self.dynamo_table, self.dynamo_region,
            num_buffer=10)

        if resp.status_code != 200:
            self.log.error("Failed to start CRUD on Dynamo DB")
            return 0
        else:
            self.loader_id = resp.json()["loader_id"]

        self.sleep(wait_time, "Waiting for CRUD on dynamo table.")

        resp = self.doc_loading_APIs.stop_crud_on_dynamo(self.loader_id)
        if resp.status_code != 200 or resp.json()["status"] != "stopped":
            self.log.error("Failed to stop CRUD on Dynamo DB")
            return 0
        self.loader_id = None

        self.sleep(30, "Waiting after stopping CRUD on Dynamo collection.")

        resp = self.doc_loading_APIs.count_dynamo_documents(
            self.dynamo_access_key, self.dynamo_secret_key,
            self.dynamo_region, self.dynamo_table)
        if resp.status_code == 200:
            return resp.json()["count"]
        else:
            self.log.error("Failed to fetch dynamo doc count.")
            return 0

    def test_create_query_drop_standalone_collection_for_dynamo(self):
        # start initial data load on dynamo table
        if not self.start_initial_data_load():
            self.fail("Failed to start initial data load on Dynamo.")

        # Update goldfish spec based on conf file params
        self.gf_spec["dataverse"]["no_of_dataverses"] = self.input.param(
            "no_of_scopes", 1)

        self.gf_spec["kafka_link"]["no_of_kafka_links"] = self.input.param(
            "no_of_links", 1)
        self.gf_spec["kafka_link"]["database_type"] = ["dynamo"]
        self.gf_spec["kafka_link"]["external_database_details"][
            "dynamo"] = list()
        dynamo_obj = ExternalDB(
            db_type="dynamo", dynamo_access_key=self.dynamo_access_key,
            dynamo_secret_key=self.dynamo_secret_key,
            dynamo_region=self.dynamo_region)
        self.gf_spec["kafka_link"]["external_database_details"][
            "dynamo"].append(
            dynamo_obj.get_source_db_detail_object_for_kafka_links())

        self.gf_spec["kafka_dataset"][
            "num_of_ds_on_external_db"] = self.input.param(
            "num_of_ds_on_external_db", 1)
        self.gf_spec["kafka_dataset"]["data_source"] = ["dynamo"]
        self.gf_spec["kafka_dataset"]["include_external_collections"][
            "dynamo"] = [CBASHelper.format_name(self.dynamo_table)]
        self.gf_spec["kafka_dataset"]["primary_key"] = [{"id": "string"}]

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.gf_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        # Wait for initial data load to finish
        if not self.wait_for_initial_data_load(self.initial_doc_count):
            self.fail("Initial doc loading into Dynamo failed.")

        datasets = self.cbas_util.list_all_dataset_objs()
        # validate doc count on datasets
        for dataset in datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.cluster, dataset.full_name, self.initial_doc_count,
                    600):
                self.fail("Ingestion failed from Dynamo into standalone "
                          "collection.")

        doc_count_after_crud = self.perform_CRUD_on_dynamo()

        # validate doc count on datasets
        for dataset in datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.cluster, dataset.full_name, doc_count_after_crud, 600):
                self.fail("Ingestion failed from Dynamo into standalone "
                          "collection.")

        # Validate doc count after disconnecting and connecting kafka links
        resp = self.doc_loading_APIs.start_crud_on_dynamo(
            self.dynamo_access_key, self.dynamo_secret_key, "id",
            self.dynamo_table, self.dynamo_region,
            num_buffer=10)
        if resp.status_code != 200:
            self.fail("Failed to start CRUD on Dynamo DB")
        else:
            self.loader_id = resp.json()["loader_id"]

        jobs = Queue()
        results = []
        links = self.cbas_util.list_all_link_objs()

        for link in links:
            jobs.put((self.cbas_util.disconnect_link,
                      {"cluster": self.cluster, "link_name": link.full_name}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.fail("Error while disconnecting kafka links")

        if not self.cbas_util.wait_for_kafka_links(
                self.cluster, state="DISCONNECTED"):
            self.fail("Kafka Link was unable to diconnect")

        results = []
        for link in links:
            jobs.put((self.cbas_util.connect_link,
                      {"cluster": self.cluster, "link_name": link.full_name}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.fail("Error while connecting kafka links")

        if not self.cbas_util.wait_for_kafka_links(
                self.cluster, state="CONNECTED"):
            self.fail("Kafka Link was unable to diconnect")

        resp = self.doc_loading_APIs.stop_crud_on_dynamo(self.loader_id)
        if resp.status_code != 200 or resp.json()["status"] != "stopped":
            self.fail("Failed to stop CRUD on Dynamo DB")
        self.loader_id = None

        self.sleep(30, "Waiting after stopping CRUD on Dynamo collection.")

        doc_count_after_connect_disconnect = 0
        resp = self.doc_loading_APIs.count_dynamo_documents(
            self.dynamo_access_key, self.dynamo_secret_key,
            self.dynamo_region, self.dynamo_table)
        if resp.status_code == 200:
            doc_count_after_connect_disconnect = resp.json()["count"]

        for dataset in datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.cluster, dataset.full_name,
                    doc_count_after_connect_disconnect, 600):
                self.fail("Ingestion failed from Dynamo into standalone "
                          "collection.")

        results = []
        query = "select * from {} limit 100"
        for dataset in datasets:
            jobs.put((
                self.cbas_util.execute_statement_on_cbas_util,
                {"cluster": self.cluster,
                 "statement": query.format(dataset.full_name)}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        for result in results:
            if result[0] != "success":
                self.fail("Query execution failed with error - {}".format(
                    result[2]))
            elif not (len(result[3]) == 100 or len(
                    result[3]) == doc_count_after_connect_disconnect):
                self.fail("Doc count mismatch. Expected - {}, Actual - {"
                          "}".format(doc_count_after_connect_disconnect,
                                     len(result[3])))

    def test_data_ingestion_when_collection_created_after_connecting_link(
            self):
        # start initial data load on dynamo table
        if not self.start_initial_data_load():
            self.fail("Failed to start initial data load on Dynamo.")

        # Update goldfish spec based on conf file params
        self.gf_spec["dataverse"]["no_of_dataverses"] = self.input.param(
            "no_of_scopes", 1)

        self.gf_spec["kafka_link"]["no_of_kafka_links"] = self.input.param(
            "no_of_links", 1)
        self.gf_spec["kafka_link"]["database_type"] = ["dynamo"]
        self.gf_spec["kafka_link"]["external_database_details"][
            "dynamo"] = list()
        dynamo_obj = ExternalDB(
            db_type="dynamo", dynamo_access_key=self.dynamo_access_key,
            dynamo_secret_key=self.dynamo_secret_key,
            dynamo_region=self.dynamo_region)
        self.gf_spec["kafka_link"]["external_database_details"][
            "dynamo"].append(
            dynamo_obj.get_source_db_detail_object_for_kafka_links())

        self.gf_spec["kafka_dataset"][
            "num_of_ds_on_external_db"] = self.input.param(
            "num_of_ds_on_external_db", 1)
        self.gf_spec["kafka_dataset"]["data_source"] = ["dynamo"]
        self.gf_spec["kafka_dataset"]["include_external_collections"][
            "dynamo"] = [CBASHelper.format_name(self.dynamo_table)]
        self.gf_spec["kafka_dataset"]["primary_key"] = [{"id": "string"}]

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.gf_spec, self.bucket_util, False, None, True)
        if not result:
            self.fail(msg)

        # Wait for initial data load to finish
        if not self.wait_for_initial_data_load(self.initial_doc_count):
            self.fail("Initial doc loading into Dynamo failed.")

        if not self.cbas_util.wait_for_kafka_links(self.cluster, "CONNECTED"):
            self.fail("Kafka link did not connect within timeout.")

        datasets = self.cbas_util.list_all_dataset_objs()
        # validate doc count on datasets
        for dataset in datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.cluster, dataset.full_name, self.initial_doc_count,
                    600):
                self.fail("Ingestion failed from Dynamo into standalone "
                          "collection.")
