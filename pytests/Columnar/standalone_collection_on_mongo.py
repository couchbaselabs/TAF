'''
Created on 25-OCTOBER-2023

@author: umang.agrawal
'''
from Queue import Queue
import time

from CbasLib.cbas_entity import ExternalDB
from Columnar.columnar_base import ColumnarBaseTest
from CbasLib.CBASOperations import CBASHelper


class StandaloneCollectionMongo(ColumnarBaseTest):

    def setUp(self):
        super(StandaloneCollectionMongo, self).setUp()

        # Since all the test cases are being run on 1 cluster only
        self.instance = self.project.instances[0]

        if not self.columnar_spec_name:
            self.columnar_spec_name = "sanity.standalone_collection_on_external_db"
        self.columnar_spec = self.cbas_util.get_columnar_spec(
            self.columnar_spec_name)

        self.mongo_db_name = "sanity-db"
        self.mongo_colletions = list()

        self.mongo_atlas_url = self.input.param("mongo_atlas_url", None)
        self.mongo_on_prem_url = self.input.param("mongo_on_prem_url", None)

        self.initial_doc_count = self.input.param("initial_doc_count", 1000)

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

        for mongo_collection in self.mongo_colletions:
            resp = self.doc_loading_APIs.delete_mongo_collection(
                None, 27017, self.mongo_db_name,
                mongo_collection, self.mongo_atlas_url,
                None, None)
            if resp.status_code != 200:
                self.fail("Error while deleting mongo collection - {}".format(
                    mongo_collection))

        resp = self.doc_loading_APIs.drop_mongo_database(
            None, self.mongo_db_name,
            self.mongo_atlas_url, None,
            None)
        if resp.status_code != 200:
            self.fail("Error while deleting mongo database - {}".format(
                self.mongo_db_name))

        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.instance, self.columnar_spec):
            self.fail("Error while deleting cbas entities")

        super(StandaloneCollectionMongo, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage="Teardown")

    def start_initial_data_load(self):
        if not (self.mongo_atlas_url or self.mongo_on_prem_url):
            self.fail("Unable to load inital data into mongo. Please pass "
                      "username and password for either Mongo Atlas or Mongo "
                      "On-prem Cluster.")
        else:
            # Create same number of mongo collections as standalone
            # collections where the data would be ingested and load
            # data in them.
            for i in range(1, self.input.param("num_of_ds_on_external_db",
                                               1) + 1):
                mongo_collection = "sanity-collection-{}".format(i)
                resp = self.doc_loading_APIs.start_mongo_loader(
                    None, self.mongo_db_name,
                    mongo_collection, self.mongo_atlas_url, 27017,
                    None, None,
                    initial_doc_count=self.initial_doc_count)
                if resp.status_code != 200:
                    self.log.error("Failed to load initial docs into Mongo "
                                   "collection {}".format(mongo_collection))
                    return False
                self.mongo_colletions.append(mongo_collection)
            self.fully_qualified_mongo_collection_name = [
                ("", CBASHelper.format_name(
                    self.mongo_db_name, coll)) for coll in
                self.mongo_colletions]
            return True

    def wait_for_initial_data_load(self, expected_count, timeout=600):
        endtime = time.time() + timeout
        doc_count = 0
        results = []

        self.log.info("waiting for initial data load in mongo to finish")
        while time.time() < endtime:
            for mongo_coll in self.mongo_colletions:
                if mongo_coll not in results:
                    resp = self.doc_loading_APIs.get_mongo_doc_count(
                        None, self.mongo_db_name,
                        mongo_coll, None,
                        None, self.mongo_atlas_url)
                    if resp.status_code == 200:
                        doc_count = resp.json()["count"]
                        if doc_count == expected_count:
                            results.append(mongo_coll)
                    else:
                        self.log.error("Failed to fetch mongo doc count. Retrying...")
            if len(results) == len(self.mongo_colletions):
                return True
            else:
                self.sleep(15, "Still Waiting for initial data load on mongo to finish")
        for mongo_coll in self.mongo_colletions:
            if mongo_coll not in results:
                self.log.error("Initial data loading on Mongo collection {} "
                               "did not finish within stipulated time. Doc "
                               "Count - Actual - {}, Expected - {}".format(
                    mongo_coll, doc_count, expected_count))
        return False

    def start_CRUD(self, mongo_collections):
        for mongo_collection in mongo_collections:
            resp = self.doc_loading_APIs.start_crud_on_mongo(
                None, self.mongo_db_name, mongo_collection,
                self.mongo_atlas_url, 27017, None,
                None,
                num_buffer=self.initial_doc_count//10)

            if resp.status_code != 200:
                self.log.error("Failed to start CRUD on Mongo DB collection "
                               "{}".format(mongo_collection))
                return False
            else:
                self.loader_ids.append(resp.json()["loader_id"])
        return True

    def stop_CRUD(self, loader_ids):
        for loader_id in loader_ids:
            resp = self.doc_loading_APIs.stop_crud_on_mongo(loader_id)
            if resp.status_code != 200 or resp.json()["status"] != "stopped":
                self.log.error("Failed to stop CRUD Loader {} on Mongo "
                               "DB.".format(loader_id))
                return False
        return True

    def get_mongo_collection_doc_count(self, mongo_collections):
        results = {}
        for mongo_collection in mongo_collections:
            resp = self.doc_loading_APIs.get_mongo_doc_count(
                None, self.mongo_db_name,
                mongo_collection, None,
                None, self.mongo_atlas_url)
            if resp.status_code == 200:
                results[CBASHelper.format_name(
                    self.mongo_db_name, mongo_collection)] = resp.json()["count"]
            else:
                self.log.error("Failed to fetch mongo doc count.")
                return {}
        return results

    def perform_CRUD_on_mongo(self, wait_time=300):

        if not self.start_CRUD(self.mongo_colletions):
            return {}

        self.sleep(wait_time, "Waiting for CRUD on mongo collection.")

        if not self.stop_CRUD(self.loader_ids):
            return {}

        self.loader_ids = list()

        self.sleep(30, "Waiting after stopping CRUD on mongo collection.")

        return self.get_mongo_collection_doc_count(self.mongo_colletions)

    # Sanity Test
    def test_create_query_drop_standalone_collection_for_mongo(self):
        # start initial data load on mongo atlas or on-prem cluster
        if not self.start_initial_data_load():
            self.fail("Failed to start initial data load on Mongo.")

        # Update columnar spec based on conf file params
        self.columnar_spec["dataverse"]["no_of_dataverses"] = self.input.param(
            "no_of_scopes", 1)

        self.columnar_spec["kafka_link"]["no_of_kafka_links"] = self.input.param(
            "no_of_links", 1)
        self.columnar_spec["kafka_link"]["database_type"] = ["mongo"]
        self.columnar_spec["kafka_link"]["external_database_details"][
            "mongo"] = list()
        mongo_obj = ExternalDB(
            db_type="mongo", mongo_connection_uri=self.mongo_on_prem_url if
            self.mongo_on_prem_url else self.mongo_atlas_url)
        self.columnar_spec["kafka_link"]["external_database_details"][
            "mongo"].append(
            mongo_obj.get_source_db_detail_object_for_kafka_links())

        self.columnar_spec["kafka_dataset"][
            "num_of_ds_on_external_db"] = self.input.param(
            "num_of_ds_on_external_db", 1)
        self.columnar_spec["kafka_dataset"]["data_source"] = ["mongo"]
        self.columnar_spec["kafka_dataset"]["include_external_collections"][
            "mongo"] = self.fully_qualified_mongo_collection_name

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.instance, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        # Wait for initial data load to finish
        if not self.wait_for_initial_data_load(self.initial_doc_count):
            self.fail("Initial doc loading into mongo failed.")

        datasets = self.cbas_util.get_all_dataset_objs()
        # validate doc count on datasets
        for dataset in datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.instance, dataset.full_name, self.initial_doc_count,
                    600):
                self.fail("Ingestion failed from Mongo into standalone "
                          "collection.")

        doc_counts_after_crud = self.perform_CRUD_on_mongo()

        # validate doc count on datasets
        for dataset in datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.instance, dataset.full_name,
                    doc_counts_after_crud[dataset.external_collection_name],
                    600):
                self.fail("Ingestion failed from Mongo into standalone "
                          "collection.")

        # Validate doc count after disconnecting and connecting kafka links
        if not self.start_CRUD(self.mongo_colletions):
            self.fail("Failed to start CRUD on Mongo DB collection")

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
            self.fail("Failed to stop CRUD on Mongo DB")

        self.loader_ids = list()

        self.sleep(300, "Waiting after stopping CRUD on mongo collection.")

        doc_counts_after_connect_disconnect = (
            self.get_mongo_collection_doc_count(self.mongo_colletions))

        for dataset in datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.instance, dataset.full_name,
                    doc_counts_after_connect_disconnect[
                        dataset.external_collection_name],
                    600):
                self.fail("Ingestion failed from Mongo into standalone "
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

    # Sanity Test
    def test_data_ingestion_when_collection_created_after_connecting_link(
            self):
        # start initial data load on mongo atlas or on-prem cluster
        if not self.start_initial_data_load():
            self.fail("Failed to start initial data load on Mongo.")

        # Update columnar spec based on conf file params
        self.columnar_spec["dataverse"]["no_of_dataverses"] = self.input.param(
            "no_of_scopes", 1)

        self.columnar_spec["kafka_link"]["no_of_kafka_links"] = self.input.param(
            "no_of_links", 1)
        self.columnar_spec["kafka_link"]["database_type"] = ["mongo"]
        self.columnar_spec["kafka_link"]["external_database_details"][
            "mongo"] = list()
        mongo_obj = ExternalDB(
            db_type="mongo", mongo_connection_uri=self.mongo_on_prem_url if
            self.mongo_on_prem_url else self.mongo_atlas_url)
        self.columnar_spec["kafka_link"]["external_database_details"][
            "mongo"].append(
            mongo_obj.get_source_db_detail_object_for_kafka_links())

        self.columnar_spec["kafka_dataset"][
            "num_of_ds_on_external_db"] = self.input.param(
            "num_of_ds_on_external_db", 1)
        self.columnar_spec["kafka_dataset"]["data_source"] = ["mongo"]
        self.columnar_spec["kafka_dataset"]["include_external_collections"][
            "mongo"] = self.fully_qualified_mongo_collection_name

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.instance, self.columnar_spec, self.bucket_util, False, None, True)
        if not result:
            self.fail(msg)

        # Wait for initial data load to finish
        if not self.wait_for_initial_data_load(self.initial_doc_count):
            self.fail("Initial doc loading into mongo failed.")

        if not self.cbas_util.wait_for_kafka_links(self.instance, "CONNECTED"):
            self.fail("Kafka link did not connect within timeout.")

        datasets = self.cbas_util.get_all_dataset_objs()
        # validate doc count on datasets
        for dataset in datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.instance, dataset.full_name, self.initial_doc_count,
                    600):
                self.fail("Ingestion failed from Mongo into standalone "
                          "collection.")