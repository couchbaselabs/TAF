"""
Created on 29-January-2024

@author: abhay.aggrawal@couchbase.com
"""
import json
from Queue import Queue
from Goldfish.goldfish_base import GoldFishBaseTest
class StandaloneCollection(GoldFishBaseTest):
    def setUp(self):
        super(StandaloneCollection, self).setUp()
        self.cluster = self.user.project.clusters[0]

        self.initial_doc_count = self.input.param("initial_doc_count", 100)
        self.doc_size = self.input.param("doc_size", 1024)

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.cluster):
            self.fail("Error while deleting cbas entities")
        # super(StandaloneCollection, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished", stage="Teardown")

    def test_create_drop_standalone_collection(self):
        database_name = self.input.param("database", "Default")
        dataverse_name = self.input.param("dataverse", "Default")
        no_of_collection = self.input.param("num_of_standalone_coll", 1)
        key = json.loads(self.input.param("key", None)) if self.input.param("key", None) != None else None
        primary_key = dict()
        validate_error = self.input.param("validate_error", False)
        error_message = str(self.input.param("error_message", None))
        if key is None:
            primary_key = None
        else:
            for key_value in key:
                primary_key[str(key_value)] = str(key[key_value])
        jobs = Queue()
        results = []
        for i in range(no_of_collection):
            dataset_obj = self.cbas_util.create_standalone_dataset_obj(self.cluster, database_name=database_name,
                                                                       dataverse_name=dataverse_name)
            jobs.put((self.cbas_util.create_standalone_collection,
                      {"cluster": self.cluster, "collection_name": dataset_obj[0].name,
                       "dataverse_name": dataset_obj[0].dataverse_name,
                       "database_name": dataset_obj[0].database_name, "primary_key": primary_key,
                       "validate_error_msg": validate_error, "expected_error": error_message}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )

        if not all(results):
            self.fail("Failed to create some collection with key {0}".format(str(key)))
        if validate_error:
            return

        datasets = self.cbas_util.list_all_dataset_objs()
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name,
                       "no_of_docs": self.initial_doc_count, "document_size": self.doc_size}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )

        if not all(results):
            self.fail("Failed to load data into standalone collection")

