'''
Created on 25-OCTOBER-2023

@author: umang.agrawal
'''

from Queue import Queue
import time

from CbasLib.cbas_entity import ExternalDB
from Goldfish.goldfish_base import GoldFishBaseTest
import urllib
from CbasLib.CBASOperations import CBASHelper


class StandaloneCollectionMongo(GoldFishBaseTest):

    def setUp(self):
        super(StandaloneCollectionMongo, self).setUp()

        # Since all the test cases are being run on 1 cluster only
        self.cluster = self.list_all_clusters()[0]

        if not self.gf_spec_name:
            self.gf_spec_name = "sanity.standalone_collection_on_external_db"
        self.gf_spec = self.cbas_util.get_goldfish_spec(
            self.gf_spec_name)

        self.mongo_db_name = "sanity-db"
        self.mongo_coll_name = "sanity-collection"

        self.mongo_coll_full_name = CBASHelper.format_name(
            self.mongo_db_name, self.mongo_coll_name)

        mongo_atlas_username = self.input.param(
            "mongo_atlas_username", None)
        mongo_atlas_password = self.input.param(
            "mongo_atlas_password", None)
        self.mongo_atlas_url = (
            "mongodb+srv://{0}:{1}@couchbase.2yp38b3.mongodb.net/".format(
                urllib.quote_plus(mongo_atlas_username),
                urllib.quote_plus(mongo_atlas_password))) if (
            mongo_atlas_username) else None

        self.mongo_on_prem_host = self.input.param("mongo_on_prem_host", None)
        self.mongo_on_prem_username = self.input.param(
            "mongo_on_prem_username", None)
        self.mongo_on_prem_password = self.input.param(
            "mongo_on_prem_password", None)
        self.mongo_on_prem_url = "mongodb://{}:27017/".format(
            self.mongo_on_prem_host) if self.mongo_on_prem_host else None

        self.initial_doc_count = self.input.param("initial_doc_count", 1000)

        self.loader_id = None

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)

        if self.loader_id:
            resp = self.doc_loading_APIs.stop_crud_on_mongo(self.loader_id)
            if resp.status_code != 200 or resp.json()["status"] != "stopped":
                self.log.error("Failed to stop CRUD on Mongo DB")

        resp = self.doc_loading_APIs.delete_mongo_collection(
            self.mongo_on_prem_host, 27017, self.mongo_db_name,
            self.mongo_coll_name, self.mongo_atlas_url,
            self.mongo_on_prem_username, self.mongo_on_prem_password)
        if resp.status_code != 200:
            self.fail("Error while deleting mongo collection - {}".format(
                self.mongo_coll_name))

        resp = self.doc_loading_APIs.drop_mongo_database(
            self.mongo_on_prem_host, self.mongo_db_name,
            self.mongo_coll_name, self.mongo_atlas_url,
            self.mongo_on_prem_username, self.mongo_on_prem_password)
        if resp.status_code != 200:
            self.fail("Error while deleting mongo database - {}".format(
                self.mongo_db_name))

        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.cluster, self.gf_spec):
            self.fail("Error while deleting cbas entities")

        super(StandaloneCollectionMongo, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage="Teardown")

    def start_initial_data_load(self):
        if not (self.mongo_atlas_url or self.mongo_on_prem_username):
            self.fail("Unable to load inital data into mongo. Please pass "
                      "username and password for either Mongo Atlas or Mongo "
                      "On-prem Cluster.")
        else:
            resp = self.doc_loading_APIs.start_mongo_loader(
                self.mongo_on_prem_host, self.mongo_db_name,
                self.mongo_coll_name, self.mongo_atlas_url, 27017,
                self.mongo_on_prem_username, self.mongo_on_prem_password,
                None, self.initial_doc_count, None)
            if resp.status_code != 200:
                self.log.error("Failed to load initial docs into Mongo")
                return False
            return True

    def wait_for_initial_data_load(self, expected_count, timeout=600):
        endtime = time.time() + timeout
        doc_count = 0
        self.log.info("waiting for initial data load in mongo to finish")
        while time.time() < endtime:
            resp = self.doc_loading_APIs.get_mongo_doc_count(
                self.mongo_on_prem_host, self.mongo_db_name,
                self.mongo_coll_name, self.mongo_on_prem_username,
                self.mongo_on_prem_password, self.mongo_atlas_url)
            if resp.status_code == 200:
                doc_count = resp.json()["count"]
                if doc_count == expected_count:
                    return True
            else:
                self.log.error("Failed to fetch mongo doc count. Retrying...")
        self.log.error("Initial data loading on Mongo did not finish within "
                       "stipulated time. Doc Count - Actual - {}, Expected - "
                       "{}".format(doc_count, expected_count))
        return False

    def perform_CRUD_on_mongo(self, wait_time=300):
        resp = self.doc_loading_APIs.start_crud_on_mongo(
            self.mongo_on_prem_host, self.mongo_db_name, self.mongo_coll_name,
            self.mongo_atlas_url, 27017, self.mongo_on_prem_username,
            self.mongo_on_prem_password, None, 100)

        if resp.status_code != 200:
            self.log.error("Failed to start CRUD on Mongo DB")
            return 0
        else:
            self.loader_id = resp.json()["loader_id"]

        self.sleep(wait_time, "Waiting for CRUD on mongo collection.")

        resp = self.doc_loading_APIs.stop_crud_on_mongo(self.loader_id)
        if resp.status_code != 200 or resp.json()["status"] != "stopped":
            self.log.error("Failed to stop CRUD on Mongo DB")
            return 0
        self.loader_id = None

        self.sleep(30, "Waiting after stopping CRUD on mongo collection.")

        resp = self.doc_loading_APIs.get_mongo_doc_count(
            self.mongo_on_prem_host, self.mongo_db_name,
            self.mongo_coll_name, self.mongo_on_prem_username,
            self.mongo_on_prem_password, self.mongo_atlas_url)
        if resp.status_code == 200:
            return resp.json()["count"]
        else:
            self.log.error("Failed to fetch mongo doc count.")
            return 0

    def test_create_query_drop_standalone_collection_for_mongo(self):
        # start initial data load on mongo atlas or on-prem cluster
        if not self.start_initial_data_load():
            self.fail("Failed to start initial data load on Mongo.")

        # Update goldfish spec based on conf file params
        self.gf_spec["dataverse"]["no_of_dataverses"] = self.input.param(
            "no_of_scopes", 1)

        self.gf_spec["kafka_link"]["no_of_kafka_links"] = self.input.param(
            "no_of_links", 1)
        self.gf_spec["kafka_link"]["database_type"] = ["mongo"]
        self.gf_spec["kafka_link"]["external_database_details"][
            "mongo"] = list()
        mongo_obj = ExternalDB(
            db_type="mongo", mongo_connection_uri=self.mongo_on_prem_url if
            self.mongo_on_prem_url else self.mongo_atlas_url)
        self.gf_spec["kafka_link"]["external_database_details"][
            "mongo"].append(
            mongo_obj.get_source_db_detail_object_for_kafka_links())

        self.gf_spec["kafka_dataset"][
            "num_of_ds_on_external_db"] = self.input.param(
            "num_of_ds_on_external_db", 1)
        self.gf_spec["kafka_dataset"]["data_source"] = ["mongo"]
        self.gf_spec["kafka_dataset"]["include_external_collections"][
            "mongo"] = [self.mongo_coll_full_name]

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.gf_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        # Wait for initial data load to finish
        if not self.wait_for_initial_data_load(self.initial_doc_count):
            self.fail("Initial doc loading into mongo failed.")

        datasets = self.cbas_util.list_all_dataset_objs()
        # validate doc count on datasets
        for dataset in datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.cluster, dataset.full_name, self.initial_doc_count,
                    600):
                self.fail("Ingestion failed from Mongo into standalone "
                          "collection.")

        doc_count_after_crud = self.perform_CRUD_on_mongo()

        # validate doc count on datasets
        for dataset in datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.cluster, dataset.full_name, doc_count_after_crud, 600):
                self.fail("Ingestion failed from Mongo into standalone "
                          "collection.")

        # Validate doc count after disconnecting and connecting kafka links
        resp = self.doc_loading_APIs.start_crud_on_mongo(
            self.mongo_on_prem_host, self.mongo_db_name, self.mongo_coll_name,
            self.mongo_atlas_url, 27017, self.mongo_on_prem_username,
            self.mongo_on_prem_password, None, 100)
        if resp.status_code != 200:
            self.fail("Failed to start CRUD on Mongo DB")
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

        resp = self.doc_loading_APIs.stop_crud_on_mongo(self.loader_id)
        if resp.status_code != 200 or resp.json()["status"] != "stopped":
            self.fail("Failed to stop CRUD on Mongo DB")

        self.loader_id = None

        self.sleep(30, "Waiting after stopping CRUD on mongo collection.")

        doc_count_after_connect_disconnect = 0
        resp = self.doc_loading_APIs.get_mongo_doc_count(
            self.mongo_on_prem_host, self.mongo_db_name,
            self.mongo_coll_name, self.mongo_on_prem_username,
            self.mongo_on_prem_password, self.mongo_atlas_url)
        if resp.status_code == 200:
            doc_count_after_connect_disconnect = resp.json()["count"]

        for dataset in datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.cluster, dataset.full_name,
                    doc_count_after_connect_disconnect, 600):
                self.fail("Ingestion failed from Mongo into standalone "
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
        # start initial data load on mongo atlas or on-prem cluster
        if not self.start_initial_data_load():
            self.fail("Failed to start initial data load on Mongo.")

        # Update goldfish spec based on conf file params
        self.gf_spec["dataverse"]["no_of_dataverses"] = self.input.param(
            "no_of_scopes", 1)

        self.gf_spec["kafka_link"]["no_of_kafka_links"] = self.input.param(
            "no_of_links", 1)
        self.gf_spec["kafka_link"]["database_type"] = ["mongo"]
        self.gf_spec["kafka_link"]["external_database_details"][
            "mongo"] = list()
        mongo_obj = ExternalDB(
            db_type="mongo", mongo_connection_uri=self.mongo_on_prem_url if
            self.mongo_on_prem_url else self.mongo_atlas_url)
        self.gf_spec["kafka_link"]["external_database_details"][
            "mongo"].append(
            mongo_obj.get_source_db_detail_object_for_kafka_links())

        self.gf_spec["kafka_dataset"][
            "num_of_ds_on_external_db"] = self.input.param(
            "num_of_ds_on_external_db", 1)
        self.gf_spec["kafka_dataset"]["data_source"] = ["mongo"]
        self.gf_spec["kafka_dataset"]["include_external_collections"][
            "mongo"] = [self.mongo_coll_full_name]

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.gf_spec, self.bucket_util, False, None, True)
        if not result:
            self.fail(msg)

        # Wait for initial data load to finish
        if not self.wait_for_initial_data_load(self.initial_doc_count):
            self.fail("Initial doc loading into mongo failed.")

        if not self.cbas_util.wait_for_kafka_links(self.cluster, "CONNECTED"):
            self.fail("Kafka link did not connect within timeout.")

        datasets = self.cbas_util.list_all_dataset_objs()
        # validate doc count on datasets
        for dataset in datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.cluster, dataset.full_name, self.initial_doc_count,
                    600):
                self.fail("Ingestion failed from Mongo into standalone "
                          "collection.")
