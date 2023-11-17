"""
Created on 25-OCTOBER-2023

@author: umang.agrawal
"""

from Goldfish.goldfish_base import GoldFishBaseTest
from Queue import Queue


class StandaloneCollection(GoldFishBaseTest):

    def setUp(self):
        super(StandaloneCollection, self).setUp()

        # Since all the test cases are being run on 1 cluster only
        self.cluster = self.list_all_clusters()[0]

        if not self.gf_spec_name:
            self.gf_spec_name = "sanity.insert_upsert_delete_standalone_collection"

        self.gf_spec = self.cbas_util.get_goldfish_spec(
            self.gf_spec_name)

        self.initial_doc_count = self.input.param("initial_doc_count", 100)
        self.doc_size = self.input.param("doc_size", 1024)

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.cluster, self.gf_spec):
            self.fail("Error while deleting cbas entities")
        super(StandaloneCollection, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished", stage="Teardown")

    def test_standalone_collection_lifecycle_with_insert_upsert_doc(self):
        # Update goldfish spec based on conf file params
        self.gf_spec["dataverse"]["no_of_dataverses"] = self.input.param(
            "no_of_scopes", 1)

        self.gf_spec["standalone_dataset"][
            "num_of_standalone_coll"] = self.input.param(
            "num_of_standalone_coll", 1)

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.gf_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        datasets = self.cbas_util.list_all_dataset_objs("standalone")
        for dataset in datasets:
            if not self.cbas_util.load_doc_to_standalone_collection(
                    self.cluster, dataset.name, dataset.dataverse_name,
                    self.initial_doc_count, self.doc_size, 100):
                self.fail("Error while loading initial docs into standalone "
                          "collection {}".format(dataset.full_name))

        jobs = Queue()
        results = []
        for dataset in datasets:
            doc_count, _ = self.cbas_util.get_num_items_in_cbas_dataset(
                self.cluster, dataset.full_name)
            if doc_count != self.initial_doc_count:
                self.fail("Number of docs inserted does not match the actual "
                          "number of docs present in dataset. Expected - {},"
                          " Actual - {}".format(
                    self.initial_doc_count, doc_count))
            jobs.put((
                self.cbas_util.crud_on_standalone_collection,
                {"cluster": self.cluster, "collection_name": dataset.name,
                 "dataverse_name": dataset.dataverse_name,
                 "target_num_docs": self.initial_doc_count,
                 "time_for_crud_in_mins": 5,
                 "where_clause_for_delete_op": "alias.id in (SELECT VALUE "
                                               "x.id FROM {0} as x limit {1})",
                 "doc_size": self.doc_size, "use_alias": True}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user)

        if not all(results):
            self.fail("CRUD failed on standalone collection")

        results = []

        query = "select * from {} limit 100"
        # running queries multiple times
        for dataset in datasets:
            jobs.put((
                self.cbas_util.execute_statement_on_cbas_util,
                {"cluster": self.cluster,
                 "statement": query.format(dataset.full_name)}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user)

        for result in results:
            if result[0] != "success":
                self.fail("Query execution failed with error - {}".format(
                    result[2]))
            elif len(result[3]) != 100:
                self.fail("Doc count mismatch. Expected - {}, Actual - {"
                          "}".format(100, len(result[3])))
