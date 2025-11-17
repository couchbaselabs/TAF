"""
Created on 12-Nov-2025

@author: himanshu.jain@couchbase.com
"""

from Columnar.onprem.columnar_onprem_base import ColumnarOnPremBase as ColumnarBaseTest
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
import json

class IndexAdvisor(ColumnarBaseTest):
    def setUp(self):
        super(IndexAdvisor, self).setUp()

        self.initial_doc_count = self.input.param("initial_doc_count", 1000)

        if not self.columnar_spec_name:
            self.columnar_spec_name = "full_template"
        self.columnar_spec = self.cbas_util.get_columnar_spec(
            self.columnar_spec_name)

        self.log_setup_status(
            self.__class__.__name__, "Finished", stage=self.setUp.__name__
        )

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        super(IndexAdvisor, self).tearDown()
        self.log_setup_status(self.__class__.__name__,
                              "Finished", stage="Teardown")

    def setup_remote_collection(self):
        self.log.info(
            f"Remote cluster: {self.remote_cluster.master.ip if self.remote_cluster else 'None'}")
        self.log.info(
            f"Columnar cluster: {self.columnar_cluster.master.ip if self.columnar_cluster else 'None'}")
        self.log.info(f"Initial doc count: {self.initial_doc_count}")

        self.collectionSetUp(cluster=self.remote_cluster, load_data=False)

        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=self.remote_cluster)

        # create remote link and remote collection in columnar
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
        if not result:
            self.fail(msg)

        for bucket in self.remote_cluster.buckets:
            SiriusCouchbaseLoader.create_clients_in_pool(
                self.remote_cluster.master, self.remote_cluster.master.rest_username,
                self.remote_cluster.master.rest_password,
                bucket.name, req_clients=1)

        self.log.info("Started Doc loading on remote cluster")
        self.load_remote_collections(self.remote_cluster,
                                     create_start_index=0, create_end_index=self.initial_doc_count)

        remote_links = self.cbas_util.get_all_link_objs("couchbase")
        remote_datasets = self.cbas_util.get_all_dataset_objs("remote")

        for link in remote_links:
            if not self.cbas_util.connect_link(self.columnar_cluster, link.full_name):
                self.fail("Failed to connect link")

        self.cbas_util.refresh_remote_dataset_item_count(self.bucket_util)

        for dataset in remote_datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.columnar_cluster, dataset.full_name,
                    dataset.num_of_items):
                self.fail("Doc count mismatch.")

        self.log.info(
            f"{self.initial_doc_count} docs loaded into remote cluster")

    def advisor(self, cluster, collection, query, skip_analyze=False, validate_current_index=False, validate_recommended_index=False, validate_error=False, error_code=None, create_advised_index=False):
        # analyze collection
        if not skip_analyze and not self.cbas_util.create_sample_for_analytics_collections(cluster, collection, sample_size="high", analytics=False):
            self.fail("Error while creating sample")

        # advise
        status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
            cluster, f"advise {query}")
        if status != "success" or len(results) == 0:
            if validate_error:
                assert errors[0][
                    "code"] == error_code, f"Expected error code: {error_code}, but got: {errors[0]['code']}"
                return
            self.fail(f"Failed to run the query: {query}")

        # analyze response
        advice = results[0][0].get('advice', {})
        if advice.get('#operator') != 'IndexAdvice':
            return None

        adviseinfo = advice['adviseinfo']
        current_indexes = adviseinfo['current_indexes']
        recommended_indexes = adviseinfo['recommended_indexes']["indexes"]
        self.log.info(f"Current indexes: {current_indexes}")
        self.log.info(f"Recommended indexes: {recommended_indexes}")

        if validate_recommended_index:
            if not recommended_indexes:
                self.fail("No recommended indexes found")
            self.log.info("Recommended indexes found")
            index_stmt = recommended_indexes[0]["index_statement"]
            if create_advised_index:
                status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                    self.columnar_cluster, index_stmt, timeout=1800, analytics_timeout=1800)
                if status != "success":
                    self.fail(f"Failed to create index: {index_stmt}")
                self.log.info(f"Created index: {index_stmt}")
            return index_stmt

        if validate_current_index:
            if not current_indexes:
                self.fail("No current indexes found")
            self.log.info("Current index found")

        if not current_indexes and not recommended_indexes:
            self.log.info("No index advice")

    def test_validate_advisor_response(self):
        self.setup_remote_collection()

        datasets = self.cbas_util.get_all_dataset_objs("remote")
        print(f"Number of datasets: {len(datasets)}")

        for dataset in datasets:
            query = f"select count(*) from {dataset.full_name} where price = 1000"

            # advise index
            self.advisor(self.columnar_cluster, dataset.full_name, query,
                         validate_recommended_index=True, create_advised_index=True)

            # validate advise in current index
            self.advisor(self.columnar_cluster, dataset.full_name,
                         query, skip_analyze=True, validate_current_index=True)

    def test_no_analyze_no_adise(self):
        self.setup_remote_collection()

        datasets = self.cbas_util.get_all_dataset_objs("remote")
        print(f"Number of datasets: {len(datasets)}")

        for dataset in datasets:
            query = f"select count(*) from {dataset.full_name} where price = 1000"
            self.advisor(self.columnar_cluster, dataset.full_name, query,
                         skip_analyze=True, validate_error=True, error_code=24284)
            self.log.info("No analyze no advise validation passed")

    def test_disable_cbo(self):
        self.setup_remote_collection()

        datasets = self.cbas_util.get_all_dataset_objs("remote")
        print(f"Number of datasets: {len(datasets)}")

        for dataset in datasets:
            query = f"select count(*) from {dataset.full_name} where price = 1000"
            # advise
            status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, f"SET `compiler.cbo` 'false'; advise {query}")
            if status != "success" or len(results) == 0:
                assert errors[0][
                    "code"] == 24001, f"Expected error code: 24001, but got: {errors[0]['code']}"
                assert errors[0]["msg"] == "Compilation error: Index advise cannot be used without CBO mode.", "Unexpected error message"
                self.log.info("Disable CBO validation passed")
                return
            self.fail(f"Failed to validate disable CBO")

    def test_predicate_like(self):
        self.setup_remote_collection()

        datasets = self.cbas_util.get_all_dataset_objs("remote")
        print(f"Number of datasets: {len(datasets)}")

        for dataset in datasets:
            query = f"select address from {dataset.full_name} where address LIKE '1%'"
            self.advisor(self.columnar_cluster, dataset.full_name,
                         query, validate_recommended_index=True)

    def test_predicate_equality(self):
        self.setup_remote_collection()

        datasets = self.cbas_util.get_all_dataset_objs("remote")
        print(f"Number of datasets: {len(datasets)}")

        for dataset in datasets:
            queries = [f"select count(*) from {dataset.full_name} where price = 1000",
                       f"select count(*) from {dataset.full_name} where price > 1800"]
            for query in queries:
                self.advisor(self.columnar_cluster, dataset.full_name,
                             query, validate_recommended_index=True)

    def test_predicate_in_array(self):
        self.setup_remote_collection()

        datasets = self.cbas_util.get_all_dataset_objs("remote")
        print(f"Number of datasets: {len(datasets)}")

        for dataset in datasets:
            query = f"select public_likes from {dataset.full_name} where 'Gerlach' in public_likes;"
            self.advisor(self.columnar_cluster, dataset.full_name,
                         query, validate_recommended_index=True)

    def test_predicate_in_subquery(self):
        self.setup_remote_collection()

        datasets = self.cbas_util.get_all_dataset_objs("remote")
        print(f"Number of datasets: {len(datasets)}")

        for dataset in datasets:
            query = f"select h.`name`, h.`city`, h.`country`, h.`price` from {dataset.full_name} as h where h.`city` IN ( select value h2.`city` from {dataset.full_name} as h2 where h2.`type` = 'Suites' )"
            self.advisor(self.columnar_cluster, dataset.full_name,
                         query, validate_recommended_index=True)

    def test_join_advisor(self):
        self.setup_remote_collection()

        datasets = self.cbas_util.get_all_dataset_objs("remote")
        print(f"Number of datasets: {len(datasets)}")

        for dataset in datasets:
            query = f"SELECT h1.`city` h1city, h2.`city` h2city FROM {dataset.full_name} as h1, {dataset.full_name} as h2 WHERE h1.`price` = 1100"
            self.advisor(self.columnar_cluster, dataset.full_name,
                         query, validate_recommended_index=True)

    def test_multiple_index_multiple_predicate(self):
        self.setup_remote_collection()

        datasets = self.cbas_util.get_all_dataset_objs("remote")
        print(f"Number of datasets: {len(datasets)}")

        for dataset in datasets:
            queries = [f"select free_breakfast,`type` from {dataset.full_name} where free_breakfast=true and `type`='Inn'", f"select name,`type` from {dataset.full_name} where name LIKE 'A%' and `type`='Inn'",
                       f"select name,`type` from {dataset.full_name} where name LIKE 'A%' and `free_breakfast`=false", f"select name,email from {dataset.full_name} where name LIKE 'A%' and email LIKE 'A%'"]
            for query in queries:
                self.advisor(self.columnar_cluster, dataset.full_name,
                             query, validate_recommended_index=True)

    def test_single_index_multiple_predicates(self):
        self.setup_remote_collection()

        datasets = self.cbas_util.get_all_dataset_objs("remote")
        print(f"Number of datasets: {len(datasets)}")

        for dataset in datasets:
            queries = [f"select name,email from {dataset.full_name} where name LIKE 'A%' and not email LIKE 'a%'",
                       f"select name,email from {dataset.full_name} where price != 1000 and price = 1100"]
            for query in queries:
                self.advisor(self.columnar_cluster, dataset.full_name,
                             query, validate_recommended_index=True)

    def test_no_index(self):
        self.setup_remote_collection()

        datasets = self.cbas_util.get_all_dataset_objs("remote")
        print(f"Number of datasets: {len(datasets)}")

        for dataset in datasets:
            query = f"select name,email from {dataset.full_name} where price is null;"
            self.advisor(self.columnar_cluster, dataset.full_name, query)

    def test_groupby_having_orderby(self):
        self.setup_remote_collection()

        datasets = self.cbas_util.get_all_dataset_objs("remote")
        print(f"Number of datasets: {len(datasets)}")

        for dataset in datasets:
            query = f"SELECT h.`country`, COUNT(*) AS hotel_count, AVG(h.`price`) AS avg_price FROM {dataset.full_name} AS h WHERE h.`price` > 1000 AND h.`free_parking` = true GROUP BY h.`country` HAVING COUNT(*) > 5 ORDER BY hotel_count DESC;"
            self.advisor(self.columnar_cluster, dataset.full_name,
                         query, validate_recommended_index=True)

    def test_udf_adisor(self):
        self.setup_remote_collection()

        datasets = self.cbas_util.get_all_dataset_objs("remote")
        print(f"Number of datasets: {len(datasets)}")

        for dataset in datasets:
            # create udf
            if not self.cbas_util.create_udf(self.columnar_cluster, "is_price_equal", "Default", "Default", body="hotel.`price` = price_value", parameters=["hotel", "price_value"]):
                self.fail("Error while creating UDF")

            # advise
            query = f"SELECT h.`price` FROM {dataset.full_name} AS h WHERE is_price_equal(h, 1221)"
            self.advisor(self.columnar_cluster, dataset.full_name,
                         query, validate_recommended_index=True)

    def test_validate_plan_before_after_advisor(self):
        self.setup_remote_collection()

        datasets = self.cbas_util.get_all_dataset_objs("remote")
        print(f"Number of datasets: {len(datasets)}")

        for dataset in datasets:
            query = f"select count(*) from {dataset.full_name} where price = 1000"
            recommendedIndex = self.advisor(
                self.columnar_cluster, dataset.full_name, query, validate_recommended_index=True, create_advised_index=True)

            if not recommendedIndex:
                self.fail("No recommended index found")

            # extract index name
            indexName = recommendedIndex.split(
                "CREATE INDEX ", 1)[1].split()[0]

            # verify plan uses index
            self.log.info(f"Verifying plan uses index: {indexName}")
            status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, f"explain {query}")
            if status != "success" or len(results) == 0:
                self.fail(f"Failed to run the query: {query}")

            plan_str = json.dumps(results) if results else ""

            if ("index-search" in plan_str) and (indexName in plan_str):
                self.log.info(f"Index '{indexName}' is used in the query plan")
            else:
                self.fail("Index is not used")
