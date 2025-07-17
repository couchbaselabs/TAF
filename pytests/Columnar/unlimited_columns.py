"""
Created on 14-July-2025

@author: himanshu.jain@couchbase.com
"""

from queue import Queue
from Columnar.columnar_base import ColumnarBaseTest
from Jython_tasks.sirius_task import CouchbaseUtil
from common_lib import sleep


class UnlimitedColumnsTest(ColumnarBaseTest):
    def __init__(self, methodName: str = "runTest"):
        super().__init__(methodName)
        self.pod = None
        self.tenant = None
        self.no_of_docs = None

    """
    In .conf file to create remote collection
    add num_remote_collections=1
    remove num_clusters=0
    """

    def setUp(self):
        super(UnlimitedColumnsTest, self).setUp()
        self.columnar_cluster = self.tenant.columnar_instances[0]
        self.remote_cluster = None
        if len(self.tenant.clusters) > 0:
            self.remote_cluster = self.tenant.clusters[0]
            self.couchbase_doc_loader = CouchbaseUtil(
                task_manager=self.task_manager,
                hostname=self.remote_cluster.master.ip,
                username=self.remote_cluster.master.rest_username,
                password=self.remote_cluster.master.rest_password,
            )

        self.initial_doc_count = self.input.param("initial_doc_count", 100)
        self.no_of_columns = self.input.param("no_of_columns", 10000)
        self.no_of_levels = self.input.param("no_of_levels", 0)
        self.steps = self.input.param("steps", 10)
        self.doc_size = self.input.param("doc_size", 1024)

        if not self.columnar_spec_name:
            self.columnar_spec_name = "full_template"
        self.columnar_spec = self.cbas_util.get_columnar_spec(
            self.columnar_spec_name)

        self.log_setup_status(
            self.__class__.__name__, "Finished", stage=self.setUp.__name__
        )

    def tearDown(self):
        self.log_setup_status(
            self.__class__.__name__, "Started", stage=self.tearDown.__name__
        )

        current_writer = self.get_storage_page_zero_writer()
        self.log.info(f"current_writer: {current_writer}")
        if current_writer != "default":
            self.set_storage_page_zero_writer("default")

        if not self.cbas_util.delete_cbas_infra_created_from_spec(
            self.columnar_cluster
        ):
            self.fail("Error while deleting cbas entities")

        if hasattr(self, "remote_cluster") and self.remote_cluster:
            self.delete_all_buckets_from_capella_cluster(
                self.tenant, self.remote_cluster
            )

        super(UnlimitedColumnsTest, self).tearDown()

        self.log_setup_status(self.__class__.__name__,
                              "Finished", stage="Teardown")

    def create_standalone_collection(self):
        database_name = self.input.param("database", "Default")
        dataverse_name = self.input.param("dataverse", "Default")
        no_of_collection = self.input.param("num_standalone_collections", 1)
        primary_key = {"id": "string"}
        validate_error = self.input.param("validate_error", False)
        error_message = str(self.input.param("error_message", None))
        jobs = Queue()
        results = []
        for i in range(no_of_collection):
            dataset_obj = self.cbas_util.create_standalone_dataset_obj(
                self.columnar_cluster,
                database_name=database_name,
                dataverse_name=dataverse_name,
            )
            jobs.put(
                (
                    self.cbas_util.create_standalone_collection,
                    {
                        "cluster": self.columnar_cluster,
                        "collection_name": dataset_obj[0].name,
                        "dataverse_name": dataset_obj[0].dataverse_name,
                        "database_name": dataset_obj[0].database_name,
                        "primary_key": primary_key,
                        "validate_error_msg": validate_error,
                        "expected_error": error_message,
                    },
                )
            )

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )

        if not all(results):
            self.fail("Failed to create some collection")
        if validate_error:
            return

    def load_documents_standalone_collection(
        self,
        no_of_columns,
        no_of_levels,
        sparse=False,
        allArrays=False,
        max_concurrent_batches=10,
        upsert=False,
    ):
        jobs = Queue()
        results = []
        datasets = self.cbas_util.get_all_dataset_objs()
        for dataset in datasets:
            self.log.info(
                f"Loading docs into standalone collection {dataset.name}")
            jobs.put(
                (
                    self.cbas_util.load_doc_to_standalone_collection,
                    {
                        "cluster": self.columnar_cluster,
                        "collection_name": dataset.name,
                        "dataverse_name": dataset.dataverse_name,
                        "database_name": dataset.database_name,
                        "max_concurrent_batches": max_concurrent_batches,
                        "upsert": upsert,
                        "no_of_docs": self.initial_doc_count,
                        "doc_template": "unlimited_columns",
                        "doc_template_params": {
                            "no_of_columns": no_of_columns,
                            "no_of_levels": no_of_levels,
                            "sparse": sparse,
                            "allArrays": allArrays,
                        },
                    },
                )
            )

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )

        if not all(results):
            self.fail("Failed to load data into standalone collection")

    def validate_queries(self, queries):
        datasets = self.cbas_util.get_all_dataset_objs()
        for dataset in datasets:
            for query, expected_result in queries:
                query = query.replace("<dataset_name>", dataset.name)
                status, metrics, errors, result, _, warnings = (
                    self.cbas_util.execute_statement_on_cbas_util(
                        self.columnar_cluster, query
                    )
                )

                if status != "success":
                    self.fail(f"Failed to run the query: {query}")
                self.assertEqual(result, expected_result)
                self.log.info(f"Query: {query} passed")

    def get_storage_page_zero_writer(self):
        status, content, response = (
            self.cbas_util.fetch_service_parameter_configuration_on_cbas(
                self.columnar_cluster
            )
        )
        if not status:
            self.fail(f"Failed to get storage page zero writer: {content}")
        return content["storagePageZeroWriter"]

    def update_storage_page_zero_writer(self, config_map):
        status, content, response = (
            self.cbas_util.update_service_parameter_configuration_on_cbas(
                self.columnar_cluster, config_map
            )
        )
        if not status:
            self.fail(f"Failed to update storage page zero writer: {content}")
        return content

    def restart_cluster(self):
        status, content, response = self.cbas_util.restart_analytics_cluster_uri(
            self.columnar_cluster
        )
        if not status:
            self.fail(f"Failed to restart cluster: {content}")
        return content

    def set_storage_page_zero_writer(self, writer):
        self.update_storage_page_zero_writer({"storagePageZeroWriter": writer})
        self.restart_cluster()
        sleep(30)
        current_writer = self.get_storage_page_zero_writer()
        self.log.info(f"updated writer: {current_writer}")

    # Total # of columns = doc * ( col + (col * levels) )
    def calculate_columns(self, no_of_docs, no_of_columns, no_of_levels):
        return no_of_docs * (no_of_columns + (no_of_columns * no_of_levels))

    def test_single_doc_multiple_columns(self):
        """
        (1, 10000, 0)
        """
        self.create_standalone_collection()
        self.load_documents_standalone_collection(
            no_of_columns=self.no_of_columns,
            no_of_levels=self.no_of_levels,
            max_concurrent_batches=1,
        )

        queries = [
            (f"select count(*) from <dataset_name>", [{"$1": 1}]),
            (
                f"select value [column1, column2, column10000] from <dataset_name>;",
                [[True, "value2", None]],
            ),
            (
                f"FROM <dataset_name> AS o LET c = (SELECT VALUE count(*) FROM object_values(o) AS v WHERE (v = 3.14) or (v = 10) or (v is null) or (v = true) or (v = false) or (v like 'value%'))[0] SELECT sum(c);",
                [
                    {
                        "$1": self.calculate_columns(
                            self.initial_doc_count,
                            self.no_of_columns,
                            self.no_of_levels,
                        )
                    }
                ],
            ),
        ]
        self.validate_queries(queries)
        return

    def test_increasing_columns(self):
        """
        (1000, 10, 0) -> ... -> (1000, 100, 0)
        """
        self.create_standalone_collection()

        for i in range(1, self.steps + 1):
            columns = self.no_of_columns * i
            self.log.info(
                f"Loading docs into standalone collection with {columns} columns"
            )
            queries = [
                (
                    f"select count(*) from <dataset_name>;",
                    [{"$1": self.initial_doc_count}],
                ),
                (
                    f"FROM <dataset_name> AS o LET c = (SELECT VALUE count(*) FROM object_values(o) AS v WHERE (v = 3.14) or (v = 10) or (v is null) or (v = true) or (v = false) or (v like 'value%'))[0] SELECT sum(c);",
                    [
                        {
                            "$1": self.calculate_columns(
                                self.initial_doc_count,
                                self.no_of_columns,
                                self.no_of_levels,
                            )
                        }
                    ],
                ),
            ]
            self.load_documents_standalone_collection(
                no_of_columns=self.no_of_columns,
                no_of_levels=self.no_of_levels,
                max_concurrent_batches=1,
                upsert=True,
            )
            self.validate_queries(queries)
        return

    def test_index_multiple_columns(self):
        """
        (1000, 50, 10)
        create index idx_column1 on test(column1);
        select column1 from test where column1=false;
        """
        no_of_index = 10

        self.create_standalone_collection()
        self.load_documents_standalone_collection(
            no_of_columns=self.no_of_columns, no_of_levels=self.no_of_levels
        )

        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for dataset in datasets:
            jobs = Queue()
            results = []
            for i in range(1, no_of_index + 1):
                field = f"column{i}"
                index_name = f"idx_{field}"
                index_name = self.cbas_util.format_name(index_name)
                index_fields = [".".join(field.split("_"))]

                jobs.put(
                    (
                        self.cbas_util.create_cbas_index,
                        {
                            "cluster": self.columnar_cluster,
                            "index_name": index_name,
                            "indexed_fields": index_fields,
                            "dataset_name": dataset.name,
                        },
                    )
                )
            self.cbas_util.run_jobs_in_parallel(
                jobs, results, self.sdk_clients_per_user, async_run=False
            )

            if not all(results):
                self.fail("Failed to create index")
        return

    def test_single_doc_increasing_nesting_columns(self):
        """
        (1,1,100) -> ... -> (1,1,500)
        """
        self.create_standalone_collection()

        for i in range(1, self.steps + 1):
            levels = self.no_of_levels * i
            self.log.info(
                f"Loading docs into standalone collection with {levels} levels"
            )
            self.load_documents_standalone_collection(
                no_of_columns=self.no_of_columns,
                no_of_levels=levels,
                max_concurrent_batches=1,
                upsert=True,
            )

            field = ""
            for i in range(1, levels + 1):
                field += f"level{i}."
            field += f"column{levels+1}"

            queries = [
                (
                    f"select count({field}) from <dataset_name> where id = '1';",
                    [{"$1": 1}],
                )
            ]
            self.validate_queries(queries)

        return

    def test_multiple_doc_multiple_columns(self):
        """
        (1000, 100, 10)
        """
        self.create_standalone_collection()
        self.load_documents_standalone_collection(
            no_of_columns=self.no_of_columns, no_of_levels=self.no_of_levels
        )

        queries = [
            (f"select count(*) from <dataset_name>;",
             [{"$1": self.initial_doc_count}]),
        ]
        self.validate_queries(queries)
        return

    def test_multiple_doc_increasing_nesting_columns(self):
        """
        (1000, 10, 10) -> ... -> (1000, 10, 50)
        """
        self.create_standalone_collection()

        for i in range(1, self.steps + 1):
            levels = self.no_of_levels * i
            self.log.info(
                f"Loading docs into standalone collection with {levels} levels"
            )
            self.load_documents_standalone_collection(
                no_of_columns=self.no_of_columns,
                no_of_levels=levels,
                max_concurrent_batches=1,
                upsert=True,
            )

            filter_id = ""
            for i in range(1, levels + 1):
                filter_id += f"level{i}."
            filter_id += f"column{(levels * self.no_of_columns)+1}"

            queries = [
                (
                    f"select count(*) from <dataset_name> where {filter_id} is not missing;",
                    [{"$1": 1}],
                )
            ]
            self.validate_queries(queries)
        return

    def test_sparse_columns(self):
        """
        (1000, 1000, 0)
        """
        current_writer = self.get_storage_page_zero_writer()
        self.log.info(f"current_writer: {current_writer}")
        if current_writer != "sparse":
            self.set_storage_page_zero_writer("sparse")

        self.create_standalone_collection()
        self.load_documents_standalone_collection(
            no_of_columns=self.no_of_columns,
            no_of_levels=self.no_of_levels,
            sparse=True,
        )

        queries = [
            (
                f"FROM <dataset_name> AS o LET c = (SELECT VALUE count(*) FROM object_values(o) AS v WHERE (v = 3.14) or (v = 10) or (v is null) or (v = true) or (v = false) or (v like 'value%'))[0] SELECT sum(c);",
                [
                    {
                        "$1": self.calculate_columns(
                            self.initial_doc_count,
                            self.no_of_columns,
                            self.no_of_levels,
                        )
                    }
                ],
            )
        ]
        self.validate_queries(queries)
        return

    def test_adaptive_columns(self):
        """
        (1000, 1000, 0)
        """
        current_writer = self.get_storage_page_zero_writer()
        self.log.info(f"current_writer: {current_writer}")
        if current_writer != "adaptive":
            self.set_storage_page_zero_writer("adaptive")

        self.create_standalone_collection()
        self.load_documents_standalone_collection(
            no_of_columns=self.no_of_columns,
            no_of_levels=self.no_of_levels,
            sparse=True,
        )

        queries = [
            (f"select count(*) from <dataset_name>;",
             [{"$1": self.initial_doc_count}]),
            (
                f"FROM <dataset_name> AS o LET c = (SELECT VALUE count(*) FROM object_values(o) AS v WHERE (v = 3.14) or (v = 10) or (v is null) or (v = true) or (v = false) or (v like 'value%'))[0] SELECT sum(c);",
                [
                    {
                        "$1": self.calculate_columns(
                            self.initial_doc_count,
                            self.no_of_columns,
                            self.no_of_levels,
                        )
                    }
                ],
            ),
        ]
        self.validate_queries(queries)
        return

    def test_select_all_columns(self):
        """
        (100, 100, 0)
        select column1...columnnN.valueM from test;
        """
        self.create_standalone_collection()
        self.load_documents_standalone_collection(
            no_of_columns=self.input.param("no_of_columns", 500),
            no_of_levels=self.input.param("no_of_levels", 10),
            sparse=True,
        )

        columns = ""
        for i in range(self.initial_doc_count):
            colId = (i * self.no_of_columns) + 1
            columns += f"column{colId},"
        columns = columns[:-1]

        queries = [
            (
                f"select count(*) from (SELECT {columns} FROM <dataset_name>) as o;",
                [{"$1": self.initial_doc_count}],
            ),
        ]
        self.validate_queries(queries)
        return

    def test_common_field_unlimited_columns(self):
        """
        (1000, 1000, 0)
        select count(*) from test where commonColumn = "commonValue";
        """
        self.create_standalone_collection()
        self.load_documents_standalone_collection(
            no_of_columns=self.no_of_columns, no_of_levels=self.no_of_levels
        )

        queries = [
            (
                f"select count(*) from <dataset_name> where commonColumn = 'commonValue';",
                [{"$1": self.initial_doc_count}],
            ),
        ]
        self.validate_queries(queries)
        return

    def test_query_missing_columns(self):
        """
        (1000, 1000, 0)
        select count(*) from <dataset_name> where column1000001 is not missing;
        """
        self.create_standalone_collection()
        self.load_documents_standalone_collection(
            no_of_columns=self.no_of_columns, no_of_levels=self.no_of_levels
        )

        queries = [
            (
                f"select count(*) from <dataset_name> where column{(self.no_of_columns*self.initial_doc_count)+1} is not missing;",
                [{"$1": 0}],
            ),
        ]
        self.validate_queries(queries)
        return

    def test_filter_rare_columns(self):
        """
        (1000, 1000, 0)
        select count(*) from <dataset_name> where rareColumn=10;
        """
        self.create_standalone_collection()
        self.load_documents_standalone_collection(
            no_of_columns=self.no_of_columns,
            no_of_levels=self.no_of_levels,
            sparse=True,
        )

        queries = [
            (f"select count(*) from <dataset_name> where rareColumn=15;",
             [{"$1": 3}]),
        ]
        self.validate_queries(queries)
        return

    def test_expression_rare_fields(self):
        """
        (1000, 1000, 0)
        select sum(rareColumn) from <dataset_name>; // 45
        select avg(rareColumn) from <dataset_name>; // 15
        """
        self.create_standalone_collection()
        self.load_documents_standalone_collection(
            no_of_columns=self.no_of_columns,
            no_of_levels=self.no_of_levels,
            sparse=True,
        )

        queries = [
            (f"select sum(rareColumn) from <dataset_name>;", [{"$1": 45}]),
            (f"select avg(rareColumn) from <dataset_name>;", [{"$1": 15}]),
        ]
        self.validate_queries(queries)
        return

    def test_array_fields_unlimited_columns(self):
        """
        (1000, 1000, 0)
        allArrays = True
        """
        self.create_standalone_collection()
        self.load_documents_standalone_collection(
            no_of_columns=self.no_of_columns,
            no_of_levels=self.no_of_levels,
            allArrays=True,
        )

        queries = [
            (f"select count(*) from <dataset_name>;",
             [{"$1": self.initial_doc_count}]),
        ]
        self.validate_queries(queries)
        return

    def test_upsert_workload_unlimited_columns(self):
        """
        loop 10:
            upsert (500, 500, 0)
        validate queries
        """
        self.create_standalone_collection()
        for i in range(10):
            self.log.info(f"Upsert Workload - Iteration {i+1}")
            self.load_documents_standalone_collection(
                no_of_columns=self.no_of_columns,
                no_of_levels=self.no_of_levels,
                upsert=True,
            )

        queries = [
            (f"select count(*) from <dataset_name>;",
             [{"$1": self.initial_doc_count}]),
        ]
        self.validate_queries(queries)
        return

    def test_delete_workload_unlimited_columns(self):
        """
        loop 10:
            insert (1000, 1000, 0)
            delete all docs
        validate queries
        """
        self.create_standalone_collection()

        iter = 10
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        for i in range(iter):
            self.log.info(
                f"Delete Workload on {datasets[0].name} - Iteration {i+1}")
            self.load_documents_standalone_collection(
                no_of_columns=self.no_of_columns, no_of_levels=self.no_of_levels
            )
            self.cbas_util.delete_from_standalone_collection(
                self.columnar_cluster, datasets[0].name
            )

        self.load_documents_standalone_collection(
            no_of_columns=self.no_of_columns, no_of_levels=self.no_of_levels
        )
        queries = [
            (f"select count(*) from <dataset_name>;",
             [{"$1": self.initial_doc_count}]),
        ]
        self.validate_queries(queries)

        return

    def test_join_operation_unlimited_columns(self):
        """
        (1000, 1000, 0)
        """
        self.create_standalone_collection()

        self.load_documents_standalone_collection(
            no_of_columns=self.no_of_columns, no_of_levels=self.no_of_levels
        )

        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        queries = [
            (
                f"select count(1) from {datasets[0].name} x join {datasets[1].name} y on x.column2 != y.column3 and x.column1 = y.column1;",
                [{"$1": 1}],
            ),
            (
                f"select count(1) from {datasets[0].name} x join {datasets[1].name} y on x.commonColumn = y.commonColumn;",
                [
                    {
                        "$1": self.calculate_columns(
                            self.initial_doc_count,
                            self.no_of_columns,
                            self.no_of_levels,
                        )
                    }
                ],
            ),
        ]
        self.validate_queries(queries)
        return

    def test_object_function(self):
        """
        (1000, 1000, 0)
        total fields = calculate_columns + 3 (id, commonColumn, rareColumn)
        """
        self.create_standalone_collection()
        self.load_documents_standalone_collection(
            no_of_columns=self.no_of_columns,
            no_of_levels=self.no_of_levels,
            sparse=True,
        )

        queries = [
            (
                f"object_length(object_concat((select <dataset_name>.* from <dataset_name>)));",
                [
                    self.calculate_columns(
                        self.initial_doc_count, self.no_of_columns, self.no_of_levels
                    )
                    + 3
                ],
            ),
        ]
        self.validate_queries(queries)
        return
