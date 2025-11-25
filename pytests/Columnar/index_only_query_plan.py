"""
Created on 24-Nov-2025

@author: himanshu.jain@couchbase.com
"""

from Columnar.onprem.columnar_onprem_base import ColumnarOnPremBase as ColumnarBaseTest
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from queue import Queue
import json


class IndexOnlyQueryPlan(ColumnarBaseTest):
    def setUp(self):
        super(IndexOnlyQueryPlan, self).setUp()

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
        super(IndexOnlyQueryPlan, self).tearDown()
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

    def create_load_documents_standalone_collection(self):
        # create standalone collection
        database_name = self.input.param("database", "Default")
        dataverse_name = self.input.param("dataverse", "Default")
        no_of_collection = self.input.param("num_standalone_collections", 1)
        validate_error = self.input.param("validate_error", False)
        error_message = str(self.input.param("error_message", None))
        jobs = Queue()
        results = []
        for i in range(no_of_collection):
            dataset_obj = self.cbas_util.create_standalone_dataset_obj(self.columnar_cluster,
                                                                       database_name=database_name,
                                                                       dataverse_name=dataverse_name)
            jobs.put((self.cbas_util.create_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": dataset_obj[0].name,
                       "dataverse_name": dataset_obj[0].dataverse_name,
                       "database_name": dataset_obj[0].database_name,
                       "validate_error_msg": validate_error, "expected_error": error_message}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )

        if not all(results):
            self.fail("Failed to create some collection")
        if validate_error:
            return

        # load docs into standalone collection
        datasets = self.cbas_util.get_all_dataset_objs()
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name,
                       "no_of_docs": self.initial_doc_count, "document_size": self.doc_size}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )

        if not all(results):
            self.fail("Failed to load data into standalone collection")

    def validate_index_only_query_plan(self, query, indexName, ignoreFailure=False):
        status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
            self.columnar_cluster, f"explain {query}")
        if status != "success" or len(results) == 0:
            self.fail(f"Failed to run the query: {query}")
        plan_str = json.dumps(results) if results else ""
        if (plan_str.count("index-search") == 1) and (plan_str.count(indexName) == 1) and (plan_str.count("data-scan") == 0):
            self.log.info(
                f"Index '{indexName}' is used in the query plan exactly once ; Validated index-only query plan")
        else:
            if ignoreFailure:
                self.log.info(
                    f"Index '{indexName}' is not used in the query plan exactly once ; Ignoring failure")
                return
            self.fail("Index is not used / Or used more than once")

    def test_single_field_string_equality_query_plan(self):
        self.setup_remote_collection()

        datasets = self.cbas_util.get_all_dataset_objs("remote")
        self.log.info(f"Number of datasets: {len(datasets)}")

        for dataset in datasets:
            queries = [f"select country from {dataset.full_name} where country='Japan';",
                       f"select city from {dataset.full_name} where country='Japan';"]
            indexName = "idx_country_city"

            # create index
            if not self.cbas_util.create_cbas_index(self.columnar_cluster, indexName, ["country", "city"], dataset.full_name):
                self.fail(f"Failed to create index: {indexName}")

            # validate plan
            for query in queries:
                self.validate_index_only_query_plan(query, indexName)

    def test_single_field_string_like_query_plan(self):
        self.setup_remote_collection()

        datasets = self.cbas_util.get_all_dataset_objs("remote")
        self.log.info(f"Number of datasets: {len(datasets)}")

        for dataset in datasets:
            query = f"select country from {dataset.full_name} where country LIKE 'Mor%';"
            indexName = "idx_country"

            # create index
            if not self.cbas_util.create_cbas_index(self.columnar_cluster, indexName, ["country"], dataset.full_name):
                self.fail(f"Failed to create index: {indexName}")

            # validate plan
            self.validate_index_only_query_plan(query, indexName)

    def test_single_field_numeric_equality_query_plan(self):
        self.setup_remote_collection()

        datasets = self.cbas_util.get_all_dataset_objs("remote")
        self.log.info(f"Number of datasets: {len(datasets)}")

        for dataset in datasets:
            query = f"select price from {dataset.full_name} where price = 1100;"
            indexName = "idx_price"

            # create index
            if not self.cbas_util.create_cbas_index(self.columnar_cluster, indexName, ["price"], dataset.full_name):
                self.fail(f"Failed to create index: {indexName}")

            # validate plan
            self.validate_index_only_query_plan(query, indexName)

    def test_multiple_field_single_predicate_query_plan(self):
        self.setup_remote_collection()

        datasets = self.cbas_util.get_all_dataset_objs("remote")
        self.log.info(f"Number of datasets: {len(datasets)}")

        for dataset in datasets:
            queries = [f"select country, city from {dataset.full_name} where country = 'Morocco';",
                       f"select city, country from {dataset.full_name} where country = 'Morocco';"]
            indexName = "idx_country_city"

            # create index
            if not self.cbas_util.create_cbas_index(self.columnar_cluster, indexName, ["country", "city"], dataset.full_name):
                self.fail(f"Failed to create index: {indexName}")

            # validate plan
            for query in queries:
                self.validate_index_only_query_plan(query, indexName)

    def test_multiple_field_multiple_predicate_query_plan(self):
        self.setup_remote_collection()

        datasets = self.cbas_util.get_all_dataset_objs("remote")
        self.log.info(f"Number of datasets: {len(datasets)}")

        for dataset in datasets:
            queries = [f"select country, city from {dataset.full_name} where country = 'Morocco' and city = 'Trompport';",
                       f"select city, country from {dataset.full_name} where country = 'Morocco' and city = 'Trompport';",
                       f"select country, city from {dataset.full_name} where city = 'Trompport' and country = 'Morocco';",
                       f"select city, country from {dataset.full_name} where city = 'Trompport' and country = 'Morocco';"]
            indexName = "idx_country_city"

            # create index
            if not self.cbas_util.create_cbas_index(self.columnar_cluster, indexName, ["country", "city"], dataset.full_name):
                self.fail(f"Failed to create index: {indexName}")

            # validate plan
            for query in queries:
                self.validate_index_only_query_plan(query, indexName)

    def test_single_field_multiple_predicate_query_plan(self):
        self.setup_remote_collection()

        datasets = self.cbas_util.get_all_dataset_objs("remote")
        self.log.info(f"Number of datasets: {len(datasets)}")

        for dataset in datasets:
            queries = [f"select country from {dataset.full_name} where country = 'Morocco' and city = 'Trompport';",
                       f"select city from {dataset.full_name} where country = 'Morocco' and city = 'Trompport';",
                       f"select country from {dataset.full_name} where city = 'Trompport' and country = 'Morocco';",
                       f"select city from {dataset.full_name} where city = 'Trompport' and country = 'Morocco';"]
            indexName = "idx_country_city"

            # create index
            if not self.cbas_util.create_cbas_index(self.columnar_cluster, indexName, ["country", "city"], dataset.full_name):
                self.fail(f"Failed to create index: {indexName}")

            # validate plan
            for query in queries:
                self.validate_index_only_query_plan(query, indexName)

    def test_distinct_by_query_plan(self):
        self.setup_remote_collection()

        datasets = self.cbas_util.get_all_dataset_objs("remote")
        self.log.info(f"Number of datasets: {len(datasets)}")

        for dataset in datasets:
            query = f"SELECT DISTINCT country FROM {dataset.full_name} WHERE country = 'Morocco';"
            indexName = "idx_country"

            # create index
            if not self.cbas_util.create_cbas_index(self.columnar_cluster, indexName, ["country"], dataset.full_name):
                self.fail(f"Failed to create index: {indexName}")

            # validate plan
            self.validate_index_only_query_plan(query, indexName)

    def test_range_query_plan(self):
        self.setup_remote_collection()

        datasets = self.cbas_util.get_all_dataset_objs("remote")
        self.log.info(f"Number of datasets: {len(datasets)}")

        for dataset in datasets:
            queries = [f"SELECT price FROM {dataset.full_name} WHERE price BETWEEN 1000 AND 1110;",
                       f"SELECT price FROM {dataset.full_name} WHERE price > 1800;"]
            indexName = "idx_price"

            # create index
            if not self.cbas_util.create_cbas_index(self.columnar_cluster, indexName, ["price"], dataset.full_name):
                self.fail(f"Failed to create index: {indexName}")

            # validate plan
            for query in queries:
                self.validate_index_only_query_plan(query, indexName)

    def test_aggregate_on_secondary_field(self):
        self.setup_remote_collection()

        datasets = self.cbas_util.get_all_dataset_objs("remote")
        self.log.info(f"Number of datasets: {len(datasets)}")

        for dataset in datasets:
            query = f"SELECT AVG(price) FROM {dataset.full_name} WHERE price > 1800;"
            indexName = "idx_price"

            # create index
            if not self.cbas_util.create_cbas_index(self.columnar_cluster, indexName, ["price"], dataset.full_name):
                self.fail(f"Failed to create index: {indexName}")

            # validate plan
            self.validate_index_only_query_plan(query, indexName)

    def test_join_query_plan(self):
        self.setup_remote_collection()

        datasets = self.cbas_util.get_all_dataset_objs("remote")
        self.log.info(f"Number of datasets: {len(datasets)}")

        for dataset in datasets:
            # Simple self-join query that uses index-only plan
            # Join on country field and select only indexed fields
            queries = [f"SELECT d1.country, d2.city FROM {dataset.full_name} d1 JOIN {dataset.full_name} d2 ON d1.country = d2.country WHERE d1.country = 'Morocco';",
                       f"SELECT d1.country, d2.city FROM {dataset.full_name} d1 JOIN {dataset.full_name} d2 ON d1.country = d2.country WHERE d1.country = 'Morocco' and d2.city='Trompport';"]
            indexName = "idx_country_city"

            # create index on country and city
            if not self.cbas_util.create_cbas_index(self.columnar_cluster, indexName, ["country", "city"], dataset.full_name):
                self.fail(f"Failed to create index: {indexName}")

            # validate plan
            for query in queries:
                self.validate_index_only_query_plan(query, indexName)

    def test_fallback_primary_index_query_plan(self):
        self.setup_remote_collection()

        datasets = self.cbas_util.get_all_dataset_objs("remote")
        self.log.info(f"Number of datasets: {len(datasets)}")

        for dataset in datasets:
            query = f"SELECT * FROM {dataset.full_name} WHERE price = 1800;"
            indexName = "idx_price"

            # create index
            if not self.cbas_util.create_cbas_index(self.columnar_cluster, indexName, ["price"], dataset.full_name):
                self.fail(f"Failed to create index: {indexName}")

            # validate plan
            self.validate_index_only_query_plan(
                query, indexName, ignoreFailure=True)

    def test_update_secondary_index_data(self):
        self.create_load_documents_standalone_collection()

        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        self.log.info(f"Number of datasets: {len(datasets)}")

        for dataset in datasets:
            indexName = "idx_price"

            # create index
            if not self.cbas_util.create_cbas_index(self.columnar_cluster, indexName, ["price"], dataset.full_name):
                self.fail(f"Failed to create index: {indexName}")

            count = 5
            while count > 0:
                upsert_query = f"UPSERT INTO {dataset.full_name} SELECT VALUE OBJECT_PUT(d, 'price', 1000) FROM {dataset.full_name} d WHERE d.price BETWEEN 4000 and 5000;"

                status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                    self.columnar_cluster, upsert_query, timeout=300, analytics_timeout=300)
                if status != "success":
                    self.fail(
                        f"Failed to execute statement: {upsert_query} with errors: {errors}")

                # validate plan
                query = f"SELECT price FROM {dataset.full_name} WHERE price = 1000;"
                self.validate_index_only_query_plan(query, indexName)

                upsert_query = f"UPSERT INTO {dataset.full_name} SELECT VALUE OBJECT_PUT(d, 'price', 1200) FROM {dataset.full_name} d WHERE d.price = 1000;"
                status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                    self.columnar_cluster, upsert_query, timeout=300, analytics_timeout=300)
                if status != "success":
                    self.fail(
                        f"Failed to execute statement: {upsert_query} with errors: {errors}")

                # validate plan
                query = f"SELECT price FROM {dataset.full_name} WHERE price = 1000;"
                self.validate_index_only_query_plan(query, indexName)

                count -= 1

    def test_delete_secondary_index_data(self):
        self.create_load_documents_standalone_collection()

        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        self.log.info(f"Number of datasets: {len(datasets)}")

        for dataset in datasets:
            indexName = "idx_price"
            # create index
            if not self.cbas_util.create_cbas_index(self.columnar_cluster, indexName, ["price"], dataset.full_name):
                self.fail(f"Failed to create index: {indexName}")

            delete_query = f"DELETE FROM {dataset.full_name} WHERE price = 4000;"
            status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, delete_query, timeout=300, analytics_timeout=300)
            if status != "success":
                self.fail(
                    f"Failed to execute statement: {delete_query} with errors: {errors}")

            # validate plan
            query = f"SELECT price FROM {dataset.full_name} WHERE price = 4000;"
            self.validate_index_only_query_plan(query, indexName)
