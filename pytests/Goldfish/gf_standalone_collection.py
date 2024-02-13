"""
Created on 29-January-2024

@author: abhay.aggrawal@couchbase.com
"""
import json
from Queue import Queue

from CbasLib.cbas_entity import Synonym
from Goldfish.goldfish_base import GoldFishBaseTest


class StandaloneCollection(GoldFishBaseTest):
    def setUp(self):
        super(StandaloneCollection, self).setUp()
        self.cluster = self.user.project.clusters[0]

        self.initial_doc_count = self.input.param("initial_doc_count", 100)
        self.doc_size = self.input.param("doc_size", 1024)

        if not self.gf_spec_name:
            self.gf_spec_name = "regressions.copy_to_s3"
        self.gf_spec = self.cbas_util.get_goldfish_spec(self.gf_spec_name)

        self.gf_spec["database"]["no_of_databases"] = self.input.param("no_of_databases", 1)
        self.gf_spec["dataverse"]["no_of_dataverses"] = self.input.param(
            "no_of_dataverses", 1)
        self.gf_spec["standalone_dataset"][
            "num_of_standalone_coll"] = self.input.param(
            "num_of_standalone_coll", 1)
        self.gf_spec["standalone_dataset"]["primary_key"] = [{"name": "string", "email": "string"}]

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
        key = json.loads(self.input.param("key", None)) if self.input.param("key", None) is not None else None
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

    def test_create_drop_standalone_collection_already_exist(self):
        no_of_collection = self.input.param("num_of_standalone_coll", 1)
        database_name = self.input.param("database", "Default")
        dataverse_name = self.input.param("dataverse", "Default")
        validate_error = self.input.param("validate_error", False)
        error_message = str(self.input.param("error_message", None))
        dataset_objs = []
        jobs = Queue()
        results = []
        for i in range(no_of_collection):
            dataset_obj = self.cbas_util.create_standalone_dataset_obj(self.cluster, database_name=database_name,
                                                                       dataverse_name=dataverse_name)
            dataset_objs.append(dataset_obj)
            jobs.put((self.cbas_util.create_standalone_collection,
                      {"cluster": self.cluster, "collection_name": dataset_obj[0].name,
                       "dataverse_name": dataset_obj[0].dataverse_name,
                       "database_name": dataset_obj[0].database_name}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )

        if not all(results):
            self.fail("Failed to create some collection")

        for i in range(len(dataset_objs)):
            error_message_to_verify = error_message.format(dataset_objs[i][0].name)
            jobs.put((self.cbas_util.create_standalone_collection,
                      {"cluster": self.cluster, "collection_name": dataset_objs[i][0].name,
                       "if_not_exists": False,
                       "dataverse_name": dataset_objs[i][0].dataverse_name,
                       "database_name": dataset_objs[i][0].database_name, "primary_key": None,
                       "validate_error_msg": validate_error, "expected_error": error_message_to_verify}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )

        if not all(results):
            self.fail("Failed to create some collection")

    def test_drop_non_existing_standalone_collection(self):
        database_name = self.input.param("database", "Default")
        dataverse_name = self.input.param("dataverse", "Default")
        validate_error = self.input.param("validate_error", False)
        error_message = str(self.input.param("error_message", None))
        jobs = Queue()
        results = []
        for i in range(10):
            dataset_obj = self.cbas_util.create_standalone_dataset_obj(self.cluster, database_name=database_name,
                                                                       dataverse_name=dataverse_name)
            error_message_to_verify = error_message.format(dataset_obj[0].name, dataset_obj[0].database_name,
                                                           dataset_obj[0].dataverse_name)
            jobs.put((self.cbas_util.drop_dataset,
                      {"cluster": self.cluster, "dataset_name": dataset_obj[0].full_name,
                       "validate_error_msg": validate_error, "expected_error": error_message_to_verify}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )

        if not all(results):
            self.fail("Some non-existing collections were deleted")

    def test_synonym_standalone_collection(self):
        self.gf_spec["synonym"]["no_of_synonyms"] = self.input.param(
            "num_of_synonyms", 1)
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.gf_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)
        jobs = Queue()
        results = []
        synonyms = self.cbas_util.list_all_synonym_objs()

        for synonym in synonyms:
            # load data to standalone collections
            jobs.put((self.cbas_util.crud_on_standalone_collection,
                      {"cluster": self.cluster, "collection_name": synonyms.name,
                       "dataverse_name": synonym.dataverse_name, "database_name": synonym.database_name,
                       "target_num_docs": self.initial_doc_count, "doc_size": self.doc_size}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )

        if not all(results):
            self.fail("Failed to load data into standalone collection using synonyms")

        for synonym in synonyms:
            jobs.put((self.cbas_util.get_num_items_in_cbas_dataset,
                      {"cluster": self.cluster, "dataset_name": synonym.full_name}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )

        if not all(results):
            self.fail("Failed to run query on standalone collection using synonyms")

