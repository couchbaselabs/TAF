"""
Created on 11-November-2024

@author: abhay.aggrawal@couchbase.com
"""
import random
from queue import Queue

from Columnar.columnar_base import ColumnarBaseTest


class GCSLinksDatasets(ColumnarBaseTest):

    def __init__(self, methodName: str = "runTest"):
        super().__init__(methodName)
        self.tenant = None

    def setUp(self):
        super(GCSLinksDatasets, self).setUp()

        # Since all the test cases are being run on 1 cluster only
        self.columnar_cluster = self.tenant.columnar_instances[0]

        if not self.columnar_spec_name:
            self.columnar_spec_name = "full_template"

        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            external_collection_file_formats=[self.input.param("file_format")])

        self.doc_count_per_format = {
            "json": 120000, "parquet": 120000,
            "csv": 120000, "tsv": 120000, "avro": 120000}

        self.doc_count_level_1_folder_1 = {
            "json": 120000, "parquet": 120000,
            "csv": 120000, "tsv": 120000, "avro": 120000}

        # path variable type
        #level_{level_no:int}_folder_{folder_no:int}

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)


    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.columnar_cluster, self.columnar_spec):
            self.fail("Error while deleting cbas entities")
        super(GCSLinksDatasets, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage="Teardown")


    def test_create_query_drop_external_datasets(self):
        # Update columnar spec based on conf file params
        file_format = self.input.param("file_format", "json")
        dataset_properties = self.columnar_spec["external_dataset"][
            "external_dataset_properties"][0]
        dataset_properties["include"] = "*.{0}".format(file_format)
        if self.input.param("dynamic_prefix", False) is True:
            dataset_properties["path_on_external_container"] = (
                self.input.param("path_on_external_container",
                                 "level_{level_no:int}_folder_{folder_no:int}"))
        else: dataset_properties["path_on_external_container"] = (
            self.input.param("path_on_external_container",
                             ""))

        if file_format in ["csv", "tsv"]:
            dataset_properties["object_construction_def"] = (
                "id int,product_name string,product_link string,"
                "product_features string,product_specs string,"
                "product_image_links string,product_reviews string,"
                "product_category string, price double,avg_rating double,"
                "num_sold int,upload_date string,weight double,quantity int,"
                "seller_name string,seller_location string,"
                "seller_verified boolean,template_name string,mutated int,"
                "padding string")
            dataset_properties["header"] = True
            dataset_properties["redact_warning"] = False
            dataset_properties["null_string"] = None

        elif file_format == "parquet":
            dataset_properties["parse_json_string"] = 1
            dataset_properties["convert_decimal_to_double"] = 1
            dataset_properties["timezone"] = "GMT"

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.columnar_cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        jobs = Queue()
        datasets = self.cbas_util.get_all_dataset_objs("external")
        results = []
        # Read all the docs in the aws s3 bucket
        if self.input.param("dynamic_prefix", False) is False:
            for dataset in datasets:
                jobs.put((
                    self.cbas_util.get_num_items_in_cbas_dataset,
                    {"cluster": self.columnar_cluster, "dataset_name": dataset.full_name,
                     "timeout": 3600, "analytics_timeout": 3600}))
            self.cbas_util.run_jobs_in_parallel(
                jobs, results, self.sdk_clients_per_user, async_run=False)
            if self.input.param("path_on_external_container", "") != "":
                self.doc_count_per_format = self.doc_count_level_1_folder_1
            for result in results:
                    if result != self.doc_count_per_format[file_format]:
                        self.fail("Doc count mismatch. Expected - {0}, Actual - {1}".format(
                            self.doc_count_per_format[file_format], result))
        else:
            query = "select count(*) from {} where level_no=1 and folder_no=1"
            for dataset in datasets:
                jobs.put((
                    self.cbas_util.execute_statement_on_cbas_util,
                    {"cluster": self.columnar_cluster,
                     "statement": query.format(dataset.full_name)}))
            self.cbas_util.run_jobs_in_parallel(
                jobs, results, self.sdk_clients_per_user, async_run=False)
            for result in results:
                if result[0] != "success":
                    self.fail("Query execution failed with error - {}".format(
                        result[2]))
                elif result[3][0]["$1"] != self.doc_count_level_1_folder_1[file_format]:
                    self.fail(
                        "Doc count mismatch. Expected - {0}, Actual - {1}".format(
                            self.doc_count_level_1_folder_1[file_format],
                            len(result[3])))

            # Verify docs read from a particular folder are embedded with
            # compute fields used in dynamic prefix.
            dataset = random.choice(datasets)
            query = ("select * from {} where level_no=1 and folder_no=1 limit "
                     "1").format(dataset.full_name)
            status, metrics, errors, results, _, _ = \
                self.cbas_util.execute_statement_on_cbas_util(
                    self.columnar_cluster, query, timeout=300, analytics_timeout=300)
            if not ("level_no" in results[0][dataset.name] and
                    "folder_no" in results[0][dataset.name]):
                self.fail("Dynamic prefix compute fields are not present in "
                          "the document")