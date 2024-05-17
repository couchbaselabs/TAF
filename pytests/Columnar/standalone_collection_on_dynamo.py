'''
Created on 25-OCTOBER-2023

@author: umang.agrawal
'''
import random
import time
from queue import Queue

from CbasLib.cbas_entity_columnar import ExternalDB
from Columnar.columnar_base import ColumnarBaseTest
from CbasLib.CBASOperations import CBASHelper
from awsLib.s3_data_helper import perform_S3_operation


class StandaloneCollectionDynamo(ColumnarBaseTest):

    def setUp(self):
        super(StandaloneCollectionDynamo, self).setUp()

        # Since all the test cases are being run on 1 cluster only
        self.instance = self.project.instances[0]

        if not self.columnar_spec_name:
            self.columnar_spec_name = "sanity.standalone_collection_on_external_db"
        self.columnar_spec = self.cbas_util.get_columnar_spec(
            self.columnar_spec_name)

        self.initial_doc_count = self.input.param("initial_doc_count", 100)
        self.doc_size = self.input.param("doc_size", 1024)
        self.dynamo_access_key = self.input.param("aws_access_key")
        self.dynamo_secret_key = self.input.param("aws_secret_key")

        # Choose as many regions as the number of links to be created.
        aws_region_list = perform_S3_operation(
            aws_access_key=self.dynamo_access_key,
            aws_secret_key=self.dynamo_secret_key,
            aws_session_token="", get_regions=True)
        selected_regions = [random.choice(aws_region_list) for i in range(
            self.input.param("no_of_links", 1))]

        # Generate dynamo table name for the chosen regions.
        self.dynomo_region_table_dict = dict()
        self.dynamo_tables = list()
        for i in range(0, self.input.param("num_of_ds_on_external_db", 1)):
            selected_region = random.choice(selected_regions)
            if selected_region not in self.dynomo_region_table_dict:
                self.dynomo_region_table_dict[selected_region] = []
            table_name = "Sanity-Test-{}".format(
                self.cbas_util.generate_name(max_length=5))
            self.dynomo_region_table_dict[selected_region].append(table_name)
            self.dynamo_tables.append((selected_region, table_name))

        self.loader_ids = list()

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)

        for loader_id in self.loader_ids:
            resp = self.doc_loading_APIs.stop_crud_on_mongo(loader_id)
            if resp.status_code != 200 or resp.json()["status"] != "stopped":
                self.log.error("Failed to stop CRUD Loader {} on Mongo "
                               "DB.".format(loader_id))

        for region, table_names in self.dynomo_region_table_dict.iteritems():
            for table_name in table_names:
                resp = self.doc_loading_APIs.delete_dynamo_table(
                    table_name, self.dynamo_access_key, self.dynamo_secret_key,
                    region)
                if resp.status_code != 200:
                    self.fail("Error while deleting dynamo table - {}".format(
                        table_name))

        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.instance, self.columnar_spec):
            self.fail("Error while deleting cbas entities")

        super(StandaloneCollectionDynamo, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage="Teardown")

    def start_initial_data_load(self):
        for region, table_names in self.dynomo_region_table_dict.iteritems():
            for table_name in table_names:
                resp = self.doc_loading_APIs.start_dynamo_loader(
                    self.dynamo_access_key, self.dynamo_secret_key, "id",
                    table_name, region, self.initial_doc_count,
                    self.doc_size, True)
                if resp.status_code != 200:
                    self.log.error(
                        "Failed to load initial docs into Dynamo Table {} in "
                        "region {}".format(table_name, region))
                    return False
        return True

    def wait_for_initial_data_load(self, expected_count, timeout=600):
        endtime = time.time() + timeout
        doc_count = 0
        results = []

        self.log.info("waiting for initial data load in dynamo to finish")
        while time.time() < endtime:
            for region, table_names in (self.dynomo_region_table_dict.iteritems()):
                for table_name in table_names:
                    if table_name not in results:
                        resp = self.doc_loading_APIs.count_dynamo_documents(
                            self.dynamo_access_key, self.dynamo_secret_key,
                            region, table_name)
                        if resp.status_code == 200:
                            doc_count = resp.json()["count"]
                            if doc_count == expected_count:
                                results.append(table_name)
                        else:
                            self.log.error("Failed to fetch dynamo doc count. Retrying...")
            if len(results) == self.input.param("num_of_ds_on_external_db", 1):
                return True
            else:
                self.sleep(15,
                           "Still Waiting for initial data load on Dynamo to "
                           "finish")
        for region, table_names in self.dynomo_region_table_dict.iteritems():
            for table_name in table_names:
                if table_name not in results:
                    self.log.error(
                        "Initial data loading on Dynamo table {} in {} did not "
                        "finish within stipulated time. Doc Count - Actual - {}, "
                        "Expected - {}".format(table_name, region, doc_count,
                                               expected_count))
        return False

    def start_CRUD(self, dynomo_region_table_dict):
        for region, table_names in dynomo_region_table_dict.iteritems():
            for table_name in table_names:
                resp = self.doc_loading_APIs.start_crud_on_dynamo(
                    self.dynamo_access_key, self.dynamo_secret_key, "id",
                    table_name, region,
                    num_buffer=self.initial_doc_count // 10,
                    document_size=self.doc_size)

                if resp.status_code != 200:
                    self.log.error("Failed to start CRUD on Dynamo table {} in "
                                   "region {}".format(table_name, region))
                    return False
                else:
                    self.loader_ids.append(resp.json()["loader_id"])
        return True

    def stop_CRUD(self, loader_ids):
        for loader_id in loader_ids:
            resp = self.doc_loading_APIs.stop_crud_on_dynamo(loader_id)
            if resp.status_code != 200 or resp.json()["status"] != "stopped":
                self.log.error("Failed to stop CRUD Loader {} on Dynamo "
                               "DB".format(loader_id))
                return False
        return True

    def get_dynamo_table_doc_count(self, dynomo_region_table_dict):
        results = {}
        for region, table_names in dynomo_region_table_dict.iteritems():
            for table_name in table_names:
                resp = self.doc_loading_APIs.count_dynamo_documents(
                    self.dynamo_access_key, self.dynamo_secret_key, region,
                    table_name)
                if resp.status_code == 200:
                    results[CBASHelper.format_name(table_name)] = resp.json()["count"]
                else:
                    self.log.error("Failed to fetch dynamo doc count.")
                    return {}
        return results

    def perform_CRUD_on_dynamo(self, wait_time=300):

        if not self.start_CRUD(self.dynomo_region_table_dict):
            return {}

        self.sleep(wait_time, "Waiting for CRUD on dynamo table.")

        if not self.stop_CRUD(self.loader_ids):
            return {}

        self.loader_ids = list()

        self.sleep(30, "Waiting after stopping CRUD on Dynamo collection.")

        return self.get_dynamo_table_doc_count(self.dynomo_region_table_dict)

    def test_create_query_drop_standalone_collection_for_dynamo(self):
        # start initial data load on dynamo table
        if not self.start_initial_data_load():
            self.fail("Failed to start initial data load on Dynamo.")

        # Update Columnar spec based on conf file params
        self.columnar_spec["dataverse"]["no_of_dataverses"] = self.input.param(
            "no_of_scopes", 1)

        self.columnar_spec["kafka_link"]["no_of_kafka_links"] = self.input.param(
            "no_of_links", 1)
        self.columnar_spec["kafka_link"]["database_type"] = ["dynamo"]
        self.columnar_spec["kafka_link"]["external_database_details"][
            "dynamo"] = list()
        for region in self.dynomo_region_table_dict:
            dynamo_obj = ExternalDB(
                db_type="dynamo", dynamo_access_key=self.dynamo_access_key,
                dynamo_secret_key=self.dynamo_secret_key,
                dynamo_region=region)
            self.columnar_spec["kafka_link"]["external_database_details"][
                "dynamo"].append(
                dynamo_obj.get_source_db_detail_object_for_kafka_links())

        self.columnar_spec["kafka_dataset"][
            "num_of_ds_on_external_db"] = self.input.param(
            "num_of_ds_on_external_db", 1)
        self.columnar_spec["kafka_dataset"]["data_source"] = ["dynamo"]

        self.columnar_spec["kafka_dataset"]["include_external_collections"][
            "dynamo"] = self.dynamo_tables
        self.columnar_spec["kafka_dataset"]["primary_key"] = [{"id": "string"}]

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.instance, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        # Wait for initial data load to finish
        if not self.wait_for_initial_data_load(self.initial_doc_count, 3600):
            self.fail("Initial doc loading into Dynamo failed.")

        datasets = self.cbas_util.get_all_dataset_objs()
        # validate doc count on datasets
        for dataset in datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.instance, dataset.full_name, self.initial_doc_count,
                    600):
                self.fail("Ingestion failed from Dynamo into standalone "
                          "collection.")

        doc_counts_after_crud = self.perform_CRUD_on_dynamo()

        # validate doc count on datasets
        for dataset in datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.instance, dataset.full_name,
                    doc_counts_after_crud[dataset.external_collection_name],
                    600):
                self.fail("Ingestion failed from Dynamo into standalone "
                          "collection.")

        # Validate doc count after disconnecting and connecting kafka links
        if not self.start_CRUD(self.dynomo_region_table_dict):
            self.fail("Failed to start CRUD on Dynamo DB")

        jobs = Queue()
        results = []
        links = self.cbas_util.get_all_link_objs()

        for link in links:
            jobs.put((self.cbas_util.disconnect_link,
                      {"cluster": self.instance, "link_name": link.full_name}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.fail("Error while disconnecting kafka links")

        if not self.cbas_util.wait_for_kafka_links(
                self.instance, state="DISCONNECTED"):
            self.fail("Kafka Link was unable to diconnect")

        results = []
        for link in links:
            jobs.put((self.cbas_util.connect_link,
                      {"cluster": self.instance, "link_name": link.full_name}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.fail("Error while connecting kafka links")

        if not self.cbas_util.wait_for_kafka_links(
                self.instance, state="CONNECTED"):
            self.fail("Kafka Link was unable to diconnect")

        if not self.stop_CRUD(self.loader_ids):
            self.fail("Failed to stop CRUD on Dynamo DB")

        self.loader_ids = list()

        self.sleep(30, "Waiting after stopping CRUD on Dynamo collection.")

        doc_counts_after_connect_disconnect = self.get_dynamo_table_doc_count(
            self.dynomo_region_table_dict)

        for dataset in datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.instance, dataset.full_name,
                    doc_counts_after_connect_disconnect[
                        dataset.external_collection_name], 600):
                self.fail("Ingestion failed from Dynamo into standalone "
                          "collection.")

        limit_value = min(doc_counts_after_connect_disconnect.values())
        if limit_value > 100:
            limit_value = 100

        results = []
        query = "select * from {} limit {}"
        for dataset in datasets:
            jobs.put((
                self.cbas_util.execute_statement_on_cbas_util,
                {"cluster": self.instance,
                 "statement": query.format(dataset.full_name, limit_value)}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        for result in results:
            if result[0] != "success":
                self.fail("Query execution failed with error - {}".format(
                    result[2]))
            elif len(result[3]) != limit_value:
                self.fail("Doc count mismatch. Expected - {}, Actual - {"
                          "}".format(limit_value, len(result[3])))

    def test_data_ingestion_when_collection_created_after_connecting_link(
            self):
        # start initial data load on dynamo table
        if not self.start_initial_data_load():
            self.fail("Failed to start initial data load on Dynamo.")

        # Update columnar spec based on conf file params
        self.columnar_spec["dataverse"]["no_of_dataverses"] = self.input.param(
            "no_of_scopes", 1)

        self.columnar_spec["kafka_link"]["no_of_kafka_links"] = self.input.param(
            "no_of_links", 1)
        self.columnar_spec["kafka_link"]["database_type"] = ["dynamo"]
        self.columnar_spec["kafka_link"]["external_database_details"][
            "dynamo"] = list()
        for region in self.dynomo_region_table_dict:
            dynamo_obj = ExternalDB(
                db_type="dynamo", dynamo_access_key=self.dynamo_access_key,
                dynamo_secret_key=self.dynamo_secret_key,
                dynamo_region=region)
            self.columnar_spec["kafka_link"]["external_database_details"][
                "dynamo"].append(
                dynamo_obj.get_source_db_detail_object_for_kafka_links())

        self.columnar_spec["kafka_dataset"][
            "num_of_ds_on_external_db"] = self.input.param(
            "num_of_ds_on_external_db", 1)
        self.columnar_spec["kafka_dataset"]["data_source"] = ["dynamo"]

        self.columnar_spec["kafka_dataset"]["include_external_collections"][
            "dynamo"] = self.dynamo_tables
        self.columnar_spec["kafka_dataset"]["primary_key"] = [{"id": "string"}]

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.instance, self.columnar_spec, self.bucket_util, False, None, True)
        if not result:
            self.fail(msg)

        # Wait for initial data load to finish
        if not self.wait_for_initial_data_load(self.initial_doc_count, 3600):
            self.fail("Initial doc loading into Dynamo failed.")

        if not self.cbas_util.wait_for_kafka_links(self.instance, "CONNECTED"):
            self.fail("Kafka link did not connect within timeout.")

        datasets = self.cbas_util.get_all_dataset_objs()
        # validate doc count on datasets
        for dataset in datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.instance, dataset.full_name, self.initial_doc_count,
                    600):
                self.fail("Ingestion failed from Dynamo into standalone "
                          "collection.")
