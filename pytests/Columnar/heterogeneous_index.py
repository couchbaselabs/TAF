"""
Created on 27-May-2025

@author: himanshu.jain@couchbase.com
"""

from queue import Queue

from Columnar.columnar_base import ColumnarBaseTest
from Columnar.columnar_rbac_cloud import generate_random_entity_name
from kafka_util.confluent_utils import ConfluentUtils
from kafka_util.kafka_connect_util import KafkaConnectUtil
from cbas_utils.cbas_utils_columnar import CBOUtil

from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from Jython_tasks.sirius_task import CouchbaseUtil
from common_lib import sleep


class HeterogeneousIndexValidation(ColumnarBaseTest):

    def __init__(self, cluster, collection_name, index_list, cbas_util):
        self.columnar_cluster = cluster
        self.collection_name = collection_name
        self.index_list = index_list
        self.cbas_util = cbas_util

    def __find_index_search_expression(self,data, target_index=None):
        results = []

        if isinstance(data, dict):
            for key, value in data.items():
                if key == "expressions" and isinstance(value, list):
                    filtered = [
                        expr for expr in value
                        if isinstance(expr, str)
                           and expr.startswith("index-search")
                           and (target_index is None or target_index in expr)
                    ]
                    if filtered:
                        results.append(filtered)
                else:
                    results.extend(self.__find_index_search_expression(value, target_index))
        elif isinstance(data, list):
            for item in data:
                results.extend(self.__find_index_search_expression(item, target_index))

        return results

    def validate_index_present(self, cmd):
        cmd = "explain " + cmd
        status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
            self.columnar_cluster, cmd)

        if status != "success":
            self.fail(f"Failed to run the query: {cmd}")

        for index_name in self.index_list:
            index_name = index_name.strip('`')
            if not self.__find_index_search_expression(results, index_name):
                self.fail(f"Failed to find index: {cmd}")


    def run_select_query(self, cmd):
        status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
            self.columnar_cluster, cmd)
        if status != "success" or len(results) == 0:
            self.fail(f"Failed to run the query: {cmd}")
        return results


    def validate_all(self, cmds, skip_index_validation=False):
        results = dict()
        try:
            for i, cmd in enumerate(cmds):
                if not skip_index_validation:
                    self.validate_index_present(cmd)
                results[i] = self.run_select_query(cmd)
        except Exception as e:
            self.fail(f"Failed to run validation: {e}")
        return results

class HeterogeneousIndexTest(ColumnarBaseTest):
    def __init__(self, methodName: str = "runTest"):
        super().__init__(methodName)
        self.pod = None
        self.tenant = None
        self.no_of_docs = None

    def setUp(self):
        super(HeterogeneousIndexTest, self).setUp()
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
        self.doc_size = self.input.param("doc_size", 1024)

        if not self.columnar_spec_name:
            self.columnar_spec_name = "full_template"
        self.columnar_spec = self.cbas_util.get_columnar_spec(self.columnar_spec_name)

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)


    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)

        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.columnar_cluster):
            self.fail("Error while deleting cbas entities")

        if hasattr(self, "remote_cluster") and self.remote_cluster:
            self.delete_all_buckets_from_capella_cluster(
                self.tenant, self.remote_cluster)

        super(HeterogeneousIndexTest, self).tearDown()

        self.log_setup_status(self.__class__.__name__, "Finished", stage="Teardown")

    def load_remote_collection(self):
        # creating bucket scope and collections for remote collection
        self.create_bucket_scopes_collections_in_capella_cluster(
            self.tenant, self.remote_cluster)


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
        self.load_doc_to_remote_collections(self.remote_cluster,"HeterogeneousHotel",
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

        self.log.info(f"{self.initial_doc_count} docs loaded into remote cluster")


    def create_load_documents_standalone_collection(self):
        # create standalone collection
        database_name = self.input.param("database", "Default")
        dataverse_name = self.input.param("dataverse", "Default")
        no_of_collection = self.input.param("num_standalone_collections", 1)
        primary_key = {"id": "string"}
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
                       "database_name": dataset_obj[0].database_name, "primary_key": primary_key,
                       "validate_error_msg": validate_error, "expected_error": error_message}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )

        if not all(results):
            self.fail("Failed to create some collection with key {0}".format(str(key)))
        if validate_error:
            return

        # load docs into standalone collection
        datasets = self.cbas_util.get_all_dataset_objs()
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name,
                       "no_of_docs": self.initial_doc_count, "document_size": self.doc_size, "heterogeneous": True}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )

        if not all(results):
            self.fail("Failed to load data into standalone collection")


    def test_single_field_index(self):
        self.create_load_documents_standalone_collection()

        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for dataset in datasets:
            jobs = Queue()
            results = []

            cmds = {"age": [
                f"select * from {dataset.name} where age = 30;",
                f"select * from {dataset.name} where age = '30';",
                f"select * from {dataset.name} where age < 30;",
                f"select * from {dataset.name} where age between 30 and 40;",
                f"select * from {dataset.name} where age between '30' and '40';"
            ], "name": [
                f"select name from {dataset.name} WHERE name LIKE 'a%';"
            ], "email": [
                f"select id, name from {dataset.name} WHERE email LIKE 'a%';"
            ], "description": [
                f"select description from {dataset.name} WHERE description LIKE 'A%';"
            ], "spare": [
                f"select spare from {dataset.name} where spare = false;"
            ]}

            # create index
            for field in cmds.keys():
                index_name = self.cbas_util.format_name(f"idx_{field}")
                index_fields = [field]

                jobs.put((self.cbas_util.create_cbas_index,
                          {"cluster": self.columnar_cluster, "index_name": index_name,
                           "indexed_fields": index_fields, "dataset_name": dataset.name}))

            self.cbas_util.run_jobs_in_parallel(
                jobs, results, self.sdk_clients_per_user, async_run=False
            )

            if not all(results):
                self.fail("Failed to create index")


            # validate queries
            try:
                for field in cmds.keys():
                    index_name = self.cbas_util.format_name(f"idx_{field}")
                    index_validator = HeterogeneousIndexValidation(self.columnar_cluster, dataset.name, [index_name], self.cbas_util)
                    index_validator.validate_all(cmds[field])
                    self.log.info(f"Validation completed for index {index_name} in collection {dataset.name}")
            except Exception as e:
                self.fail(f"Failed to validate index {e}")

        self.log.info("Validation completed for test_single_field_index")
        return



    def test_multiple_field_index(self):
        self.create_load_documents_standalone_collection()

        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for dataset in datasets:
            jobs = Queue()
            results = []

            cmds = {"age_name": [
                f"select * from {dataset.name} where age = 30 and name LIKE 'a%';",
            ], "name_email": [
                f"select * from {dataset.name} WHERE name LIKE 'a%' and email LIKE 'a%';"
            ]}

            # create index
            for field in cmds.keys():
                index_name = self.cbas_util.format_name(f"idx_{field}")
                index_fields = list(field.split("_"))

                jobs.put((self.cbas_util.create_cbas_index,
                          {"cluster": self.columnar_cluster, "index_name": index_name,
                           "indexed_fields": index_fields, "dataset_name": dataset.name}))

            self.cbas_util.run_jobs_in_parallel(
                jobs, results, self.sdk_clients_per_user, async_run=False
            )

            if not all(results):
                self.fail("Failed to create index")


            # validate queries
            try:
                for field in cmds.keys():
                    index_name = self.cbas_util.format_name(f"idx_{field}")
                    index_validator = HeterogeneousIndexValidation(self.columnar_cluster, dataset.name, [index_name],
                                                                   self.cbas_util)
                    index_validator.validate_all(cmds[field])
                    self.log.info(f"Validation completed for index {index_name} in collection {dataset.name}")
            except Exception as e:
                self.fail(f"Failed to validate index {e}")

        self.log.info("Validation completed for test_multiple_field_index")
        return


    def test_single_nested_field_index(self):
        self.create_load_documents_standalone_collection()

        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for dataset in datasets:
            jobs = Queue()
            results = []

            cmds = {"name_firstname": [
                f"select * from {dataset.name} where name.firstname LIKE 'a%';",
            ], "address_primaryAddress_city": [
                f"select name from {dataset.name} WHERE address.primaryAddress.city LIKE 'A%';"
            ]}

            # create index

            for field in cmds.keys():
                index_name = self.cbas_util.format_name(f"idx_{field}")
                index_fields = [".".join(field.split("_"))]

                jobs.put((self.cbas_util.create_cbas_index,
                          {"cluster": self.columnar_cluster, "index_name": index_name,
                           "indexed_fields": index_fields, "dataset_name": dataset.name}))

            self.cbas_util.run_jobs_in_parallel(
                jobs, results, self.sdk_clients_per_user, async_run=False
            )

            if not all(results):
                self.fail("Failed to create index")

            # validate queries
            try:
                for field in cmds.keys():
                    index_name = self.cbas_util.format_name(f"idx_{field}")
                    index_validator = HeterogeneousIndexValidation(self.columnar_cluster, dataset.name, [index_name],
                                                                   self.cbas_util)
                    index_validator.validate_all(cmds[field])
                    self.log.info(f"Validation completed for index_name {index_name} in collection {dataset.name}")
            except Exception as e:
                self.fail(f"Failed to validate index {e}")

        self.log.info("Validation completed for test_single_nested_field_index")
        return



    def test_multiple_nested_field_index(self, create_standalone_collection=True, dataset_type="standalone", cmds = None):
        if create_standalone_collection:
            self.create_load_documents_standalone_collection()

        datasets = self.cbas_util.get_all_dataset_objs(dataset_type)

        for dataset in datasets:
            jobs = Queue()
            results = []

            if cmds:
                for key, queries in cmds.items():
                    cmds[key] = [query.replace("datasetName", dataset.name) for query in queries]
            else:
                cmds = {"name_firstname-name_lastname": [
                    f"select * from {dataset.name} where name.firstname LIKE 'a%' and name.lastname LIKE 'a%';",
                ], "address_primaryAddress_city-address_secondaryAddress_city": [
                    f"select name from {dataset.name} WHERE address.primaryAddress.city LIKE 'A%' and address.secondaryAddress.city LIKE 'A%';",
                ]}

            # create index
            for field in cmds.keys():
                index_name = self.cbas_util.format_name(f"idx_{field}")
                index_fields = [".".join(f.split("_")) for f in field.split("-")]

                jobs.put((self.cbas_util.create_cbas_index,
                          {"cluster": self.columnar_cluster, "index_name": index_name,
                           "indexed_fields": index_fields, "dataset_name": dataset.name}))

            self.cbas_util.run_jobs_in_parallel(
                jobs, results, self.sdk_clients_per_user, async_run=False
            )

            if not all(results):
                self.fail("Failed to create index")

            # validate queries
            try:
                for field in cmds.keys():
                    index_name = self.cbas_util.format_name(f"idx_{field}")
                    index_validator = HeterogeneousIndexValidation(self.columnar_cluster, dataset.name, [index_name],
                                                                   self.cbas_util)
                    index_validator.validate_all(cmds[field])
                    self.log.info(f"Validation completed for index_name {index_name} in collection {dataset.name}")
            except Exception as e:
                self.fail(f"Failed to validate index {e}")

        self.log.info("Validation completed for test_multiple_nested_field_index")
        return


    """
    create idx_age, idx_email
    select id, name from JB WHERE email LIKE 'a%' and age = 30; -> Both idx_age, idx_email is used
    """
    def test_query_multiple_fields_single_index(self):
        self.create_load_documents_standalone_collection()

        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for dataset in datasets:
            jobs = Queue()
            results = []

            fields = ["age", "email"]
            index_list = []

            cmds =[
                f"select age, email from {dataset.name} WHERE email LIKE 'a%' and age = 30;"
            ]

            # create index
            for field in fields:
                index_name = f"idx_{field}"
                index_name = self.cbas_util.format_name(index_name)
                index_list.append(index_name)
                index_fields = list(field.split("_"))

                jobs.put((self.cbas_util.create_cbas_index,
                          {"cluster": self.columnar_cluster, "index_name": index_name,
                           "indexed_fields": index_fields, "dataset_name": dataset.name}))

            self.cbas_util.run_jobs_in_parallel(
                jobs, results, self.sdk_clients_per_user, async_run=False
            )

            if not all(results):
                self.fail("Failed to create index")

            # validate queries
            try:
                index_validator = HeterogeneousIndexValidation(self.columnar_cluster, dataset.name, index_list,
                                                                   self.cbas_util)
                index_validator.validate_all(cmds)
                self.log.info(f"Validation completed for index list {index_list} in collection {dataset.name}")

            except Exception as e:
                self.fail(f"Failed to validate index {e}")

        self.log.info("Validation completed for test_query_multiple_fields_single_index")
        return



    def test_join_indexed_fields(self):
        """
        create 2 standalone collections with 1000 docs each (age (str, int): 28-32)
        ANALYZE ANALYTICS COLLECTION KS1 with {"sample":"high"};
        ANALYZE ANALYTICS COLLECTION KS2 with {"sample":"high"};
        CREATE INDEX idx_age ON KS1(age);
        CREATE INDEX idx_age ON KS2(age);
        select count(1) from KS1 x join KS2 y on x.age <= 28;
        select count(1) from KS1 x join KS2 y on y.age <= 28;
        """
        self.create_load_documents_standalone_collection()

        datasets = self.cbas_util.get_all_dataset_objs("standalone")


        jobs = Queue()
        results = []

        cmds = {"age": [
            f"select count(1) from {datasets[0].name} x join {datasets[1].name} y on x.age <= 28;",
            f"select count(1) from {datasets[0].name} x join {datasets[1].name} y on y.age <= 28;"
        ]}

        # analyze collection
        for dataset in datasets:
            cboutil = CBOUtil()
            cboutil.create_sample_for_analytics_collections(self.columnar_cluster, dataset.name, sample_size="high")

        # create index
        for dataset in datasets:
            for field in cmds.keys():
                index_name = self.cbas_util.format_name(f"idx_{field}")
                index_fields = [field]

                jobs.put((self.cbas_util.create_cbas_index,
                    {"cluster": self.columnar_cluster, "index_name": index_name,
                    "indexed_fields": index_fields, "dataset_name": dataset.name}))

            self.cbas_util.run_jobs_in_parallel(
                jobs, results, self.sdk_clients_per_user, async_run=False
            )

            if not all(results):
                self.fail("Failed to create index")

        # validate queries
        for cmd in cmds["age"]:
            dataset = datasets[0].name
            try:
                index_name = self.cbas_util.format_name(f"idx_age")
                index_validator = HeterogeneousIndexValidation(self.columnar_cluster, dataset, [index_name],
                                                                self.cbas_util)
                index_validator.validate_all([cmd])
                self.log.info(f"Validation completed for index_name {index_name} in collection {dataset}")
            except Exception as e:
                self.fail(f"Failed to validate index {e}")

        self.log.info("Validation completed for test_join_indexed_fields")
        return

    def test_list_field_index(self):
        """
        insert 1000 docs in standalone collection
        CREATE INDEX idx_hobbies ON KS1(hobbies);
        select count(*) from KS1 where hobbies = ["Reading", "Writing", "Drawing"];
        """
        self.create_load_documents_standalone_collection()

        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for dataset in datasets:
            jobs = Queue()
            results = []

            cmds = {"hobbies": [
                f"select count(*) from {dataset.name} where hobbies = ['Reading', 'Writing', 'Drawing'];"
            ]}

            # create index
            for field in cmds.keys():
                index_name = self.cbas_util.format_name(f"idx_{field}")
                index_fields = [field]

                jobs.put((self.cbas_util.create_cbas_index,
                          {"cluster": self.columnar_cluster, "index_name": index_name,
                           "indexed_fields": index_fields, "dataset_name": dataset.name}))

            self.cbas_util.run_jobs_in_parallel(
                jobs, results, self.sdk_clients_per_user, async_run=False
            )

            if not all(results):
                self.fail("Failed to create index")

            # validate queries
            try:
                for field in cmds.keys():
                    index_name = self.cbas_util.format_name(f"idx_{field}")
                    index_validator = HeterogeneousIndexValidation(self.columnar_cluster, dataset.name, [index_name],
                                                                   self.cbas_util)
                    index_validator.validate_all(cmds[field])
                    self.log.info(f"Validation completed for index_name {index_name} in collection {dataset.name}")
            except Exception as e:
                self.fail(f"Failed to validate index {e}")

        self.log.info("Validation completed for test_list_field_index")
        return

    def test_include_exclude_key_index(self):
        self.create_load_documents_standalone_collection()

        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for dataset in datasets:
            # create index
            index_name = "idx_age"
            index_fields = ["age"]

            result = self.cbas_util.create_cbas_index(self.columnar_cluster, index_name, index_fields, dataset.name,
                                                      validate_error_msg= True, include=True,
                                                      expected_error="Compilation error: Cannot specify exclude/include unknown for untyped keys in the index definition.")
            if not result:
                self.fail("Error message is not a expected")

            result = self.cbas_util.create_cbas_index(self.columnar_cluster, index_name, index_fields, dataset.name,
                                                      validate_error_msg=True, exclude=True,
                                                      expected_error="Compilation error: Cannot specify exclude/include unknown for untyped keys in the index definition.")
            if not result:
                self.fail("Error message is not a expected")

        self.log.info("Validation completed for test_include_exclude_key_index")
        return



    def test_mixed_field_index(self):
        self.create_load_documents_standalone_collection()

        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for dataset in datasets:
            # create index
            index_name = self.cbas_util.format_name("idx_mixed_field")
            index_fields = ["name", "age:INT"]

            result = self.cbas_util.create_cbas_index(self.columnar_cluster,index_name,index_fields, dataset.name,
                                                      validate_error_msg=True,expected_error="Compilation error: Typed keys cannot be combined with untyped keys in the index definition.")

            if not result:
                self.fail("Failed to create mixed index")

        self.log.info("Validation completed for test_mixed_field_index")
        return



    # create index (validate) -> drop index (validate)
    def test_drop_index(self):
        self.test_single_field_index()

        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        for dataset in datasets:
            jobs = Queue()
            results = []

            fields = ["age", "name", "email", "description", "spare"]

            # drop index
            for field in fields:
                index_name = f"idx_{field}"
                index_name = self.cbas_util.format_name(index_name)

                jobs.put((self.cbas_util.drop_cbas_index,
                          {"cluster": self.columnar_cluster, "index_name": index_name,
                            "dataset_name": dataset.name}))

            self.cbas_util.run_jobs_in_parallel(
                jobs, results, self.sdk_clients_per_user, async_run=False
            )

            if not all(results):
                self.fail("Failed to drop index")

            # validate queries
            cmds = {"age": [
                f"select * from {dataset.name} where age = 30;",
                f"select * from {dataset.name} where age = '30';",
                f"select * from {dataset.name} where age < 30;",
                f"select * from {dataset.name} where age between 30 and 40;",
                f"select * from {dataset.name} where age between '30' and '40';"
            ], "name": [
                f"select name from {dataset.name} WHERE name LIKE 'a%';"
            ], "email": [
                f"select id, name from {dataset.name} WHERE email LIKE 'a%';"
            ], "description": [
                f"select description from {dataset.name} WHERE description LIKE 'A%';"
            ], "spare": [
                f"select spare from {dataset.name} where spare = false;"
            ]}

            try:
                for field in cmds.keys():
                    index_name = self.cbas_util.format_name(f"idx_{field}")
                    index_validator = HeterogeneousIndexValidation(self.columnar_cluster, dataset.name, [index_name],
                                                                   self.cbas_util)
                    index_validator.validate_all(cmds[field], skip_index_validation=True)
                    self.log.info(f"Validation completed for index {index_name} in collection {dataset.name}")
            except Exception as e:
                self.fail(f"Failed to validate index {e}")


        self.log.info("Validation completed for test_drop_index")
        return



    def test_create_drop_n_indexes_same_field(self):
        self.create_load_documents_standalone_collection()

        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for dataset in datasets:
            jobs = Queue()
            results = []

            # create n index
            self.num_indexes = self.input.param("num_indexes", 10)
            self.log.info("Number of indexes: {}".format(self.num_indexes))
            index_list = []
            for i in range(self.num_indexes):
                index_name = f"idx_age{i}"
                index_name = self.cbas_util.format_name(index_name)
                index_list.append(index_name)
                index_fields = ["age"]

                jobs.put((self.cbas_util.create_cbas_index,
                          {"cluster": self.columnar_cluster, "index_name": index_name,
                           "indexed_fields": index_fields, "dataset_name": dataset.name}))

            self.cbas_util.run_jobs_in_parallel(
                jobs, results, self.sdk_clients_per_user, async_run=False
            )

            if not all(results):
                self.fail("Failed to create index")

            # drop index and validate
            index_list = sorted(index_list)
            for i in range(len(index_list)-1):
                result = self.cbas_util.drop_cbas_index(self.columnar_cluster, index_list[i], dataset.name)
                if not result:
                    self.fail("Failed to drop index")

                # validate
                cmds = [ f"select id, name from {dataset.name} where age = 30;" ,
                         f"select * from {dataset.name} where age < 30;" ,
                         f"select * from {dataset.name} where age between 30 and 40;",
                         f"select * from {dataset.name} where age = '30';"]
                try:
                    index_name = self.cbas_util.format_name(index_list[i+1])
                    index_validator = HeterogeneousIndexValidation(self.columnar_cluster, dataset.name,
                                                                       [index_name],
                                                                       self.cbas_util)
                    index_validator.validate_all(cmds)
                    self.log.info(f"Validation completed for index_name {index_name} in collection {dataset.name}")
                except Exception as e:
                    self.fail(f"Failed to validate index {e}")

        self.log.info("Validation completed for test_create_drop_n_indexes_same_field")
        return




    def test_convert_docs_homogeneous(self):
        # create hetero index and validate
        self.test_single_field_index()

        # upsert to homogeneous data
        # load docs into standalone collection
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        for dataset in datasets:
            jobs = Queue()
            results = []

            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name,
                       "no_of_docs": self.initial_doc_count, "document_size": self.doc_size, "heterogeneous": True,
                       "heterogeneity": 0, "upsert": True}))

            self.cbas_util.run_jobs_in_parallel(
                jobs, results, self.sdk_clients_per_user, async_run=False
            )

            if not all(results):
                self.fail("Failed to update data into standalone collection")

            # validate queries
            cmds = {"age": [
                f"select * from {dataset.name} where age = 30;",
                f"select * from {dataset.name} where age < 30;",
                f"select * from {dataset.name} where age between 30 and 40;",
            ], "name": [
                f"select name from {dataset.name} WHERE name LIKE 'a%';"
            ], "email": [
                f"select id, name from {dataset.name} WHERE email LIKE 'a%';"
            ], "description": [
                f"select description from {dataset.name} WHERE description LIKE 'A%';"
            ], "spare": [
                f"select spare from {dataset.name} where spare = false;"
            ]}

            try:
                for field in cmds.keys():
                    index_name = self.cbas_util.format_name(f"idx_{field}")
                    index_validator = HeterogeneousIndexValidation(self.columnar_cluster, dataset.name, [index_name],
                                                                   self.cbas_util)
                    index_validator.validate_all(cmds[field])
                    self.log.info(f"Validation completed for index {index_name} in collection {dataset.name}")
            except Exception as e:
                self.fail(f"Failed to validate index {e}")

        self.log.info("Validation completed for test_convert_docs_homogeneous")
        return


    """
    ingest 1M -> create index -> query -> mutate (delete 50% docs) -> query -> verify results before and after mutate
    """
    def test_mutate_data(self):

        # ingest data
        self.load_remote_collection()


        datasets = self.cbas_util.get_all_dataset_objs("remote")
        for dataset in datasets:
            # create index
            index_name = self.cbas_util.format_name("idx_overall_rating")
            index_fields = ["overall_rating"]

            result = self.cbas_util.create_cbas_index(self.columnar_cluster, index_name, index_fields, dataset.name)

            if not result:
                self.fail("Failed to create mixed index")


            # validate queries
            cmds = {"overall_rating": [
                f"select * from {dataset.name} where overall_rating = 2;"
            ]}
            before_results = dict()
            after_results = dict()

            try:
                for field in cmds.keys():
                    index_name = f"idx_{field}"
                    index_name = self.cbas_util.format_name(index_name)

                    index_validator = HeterogeneousIndexValidation(self.columnar_cluster, dataset.name, [index_name],
                                                                   self.cbas_util)
                    before_results[field] = index_validator.validate_all(cmds[field])
                    self.log.info(f"Validation completed for index_name {index_name} in collection {dataset.name}")
            except Exception as e:
                self.fail(f"Failed to validate index {e}")



            # delete docs from capella

            for bucket in self.remote_cluster.buckets:
                SiriusCouchbaseLoader.create_clients_in_pool(
                    self.remote_cluster.master, self.remote_cluster.master.rest_username,
                    self.remote_cluster.master.rest_password,
                    bucket.name, req_clients=1)

            delete_end_index = self.initial_doc_count // 5
            self.log.info(f"Deleting {delete_end_index} docs from remote cluster")
            self.load_doc_to_remote_collections(self.remote_cluster, "HeterogeneousHotel",
                                                    delete_start_index=0, delete_end_index=delete_end_index,create_percent=0,delete_percent=100)


            # validate query
            retry = 0
            max_retries = 10
            success = False
            while retry < max_retries:
                try:
                    for field in cmds.keys():
                        index_name = f"idx_{field}"
                        index_name = self.cbas_util.format_name(index_name)

                        index_validator = HeterogeneousIndexValidation(self.columnar_cluster, dataset.name, [index_name],
                                                                       self.cbas_util)
                        after_results[field] = index_validator.validate_all(cmds[field])
                        self.log.info(f"Validation completed for index_name {index_name} in collection {dataset.name}")
                except Exception as e:
                    self.fail(f"Failed to validate index {e}")

                success = True
                for field in before_results.keys():
                    for cmd_index in before_results[field].keys():
                        before_mutation_count = len(before_results[field][cmd_index])
                        after_mutation_count = len(after_results[field][cmd_index]) + (delete_end_index // 5)
                        self.log.info(f"Query {cmd_index}: Before Mutation = {before_mutation_count}; After Mutation = {after_mutation_count}")
                        if before_mutation_count != after_mutation_count:
                            success = False
                if success:
                    break
                retry += 1
                sleep(10)

            if not success:
                raise AssertionError("Timeout reached; Mutation count does not match")

        self.log.info("Validation completed for test_mutate_data")
        return


    def test_remote_multiple_nested_field_index(self):
        self.load_remote_collection()

        cmds = {"name_firstname-name_lastname": [
            f"select name from datasetName where name.firstname like 'A%' and name.lastname like 'A%';"
        ]}
        self.test_multiple_nested_field_index(create_standalone_collection=False, dataset_type="remote", cmds=cmds)

        self.log.info("Validation completed for test_remote_multiple_nested_field_index")
        return
