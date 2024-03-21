"""
Created on 25-OCTOBER-2023

@author: umang.agrawal
"""

from Columnar.columnar_base import ColumnarBaseTest
from Queue import Queue


class S3LinksDatasets(ColumnarBaseTest):

    def setUp(self):
        super(S3LinksDatasets, self).setUp()

        # Since all the test cases are being run on 1 cluster only
        self.instance = self.project.instances[0]

        self.aws_access_key = self.input.param("aws_access_key")
        self.aws_secret_key = self.input.param("aws_secret_key")
        self.aws_session_token = self.input.param("aws_session_token", "")

        # For sanity tests we are hard coding the bucket from which the data
        # will be read. This will ensure stable and consistent test runs.
        self.aws_region = "us-west-1"
        self.aws_bucket_name = "columnar-sanity-test-data"

        if not self.columnar_spec_name:
            self.columnar_spec_name = "sanity.S3_external_datasets"

        self.columnar_spec = self.cbas_util.get_columnar_spec(
            self.columnar_spec_name)

        self.doc_count_per_format = {
            "json": 7400000, "parquet": 7300000,
            "csv": 7400000, "tsv": 7400000}

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.instance, self.columnar_spec):
            self.fail("Error while deleting cbas entities")
        super(S3LinksDatasets, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage="Teardown")

    def test_create_query_drop_external_datasets(self):
        # Update columnar spec based on conf file params
        self.columnar_spec["database"]["no_of_databases"] = self.input.param(
            "no_of_DBs", 1)
        self.columnar_spec["dataverse"]["no_of_dataverses"] = self.input.param(
            "no_of_scopes", 1)

        self.columnar_spec["external_link"][
            "no_of_external_links"] = self.input.param(
            "no_of_links", 1)
        self.columnar_spec["external_link"]["properties"] = [{
            "type": "s3",
            "region": self.aws_region,
            "accessKeyId": self.aws_access_key,
            "secretAccessKey": self.aws_secret_key,
            "serviceEndpoint": None
        }]

        self.columnar_spec["external_dataset"]["num_of_external_datasets"] = self.input.param(
            "num_of_external_datasets", 1)
        file_format = self.input.param("file_format", "json")
        dataset_properties = self.columnar_spec["external_dataset"][
            "external_dataset_properties"][0]
        dataset_properties["external_container_name"] = self.aws_bucket_name
        dataset_properties["file_format"] = file_format
        dataset_properties["include"] = "*.{0}".format(file_format)
        dataset_properties["region"] = self.aws_region
        dataset_properties["path_on_external_container"] = (
            "Depth_{depth-no:int}_Folder_{folder-no:int}")

        if file_format in ["csv", "tsv"]:
            dataset_properties["object_construction_def"] = (
                "address string,avgrating double,city string,country string,"
                "email string,freebreakfast boolean,freeparking boolean,"
                "name string,phone string,price double,publiclikes string,"
                "reviews string,`type` string,url string,extra string")
            dataset_properties["header"] = True
            dataset_properties["redact_warning"] = False
            dataset_properties["null_string"] = None

        elif file_format == "parquet":
            dataset_properties["parse_json_string"] = 1
            dataset_properties["convert_decimal_to_double"] = 1
            dataset_properties["timezone"] = "GMT"

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.instance, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        jobs = Queue()
        results = []
        for dataset in self.cbas_util.get_all_dataset_objs("external"):
            jobs.put((
                self.cbas_util.get_num_items_in_cbas_dataset,
                {"cluster": self.instance, "dataset_name": dataset.full_name,
                 "timeout": 3600, "analytics_timeout": 3600}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        for result in results:
            if result != self.doc_count_per_format[file_format]:
                self.fail("Doc count mismatch. Expected - {0}, Actual - {"
                          "1}".format(
                    self.doc_count_per_format[file_format], result))

        results = []
        query = "select * from {} limit 1000"
        for dataset in self.cbas_util.get_all_dataset_objs("external"):
            jobs.put((
                self.cbas_util.execute_statement_on_cbas_util,
                {"cluster": self.instance,
                 "statement": query.format(dataset.full_name)}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        for result in results:
            if result[0] != "success":
                self.fail("Query execution failed with error - {}".format(
                    result[2]))
            elif len(result[3]) != 1000:
                self.fail("Doc count mismatch. Expected - 1000, Actual - {"
                          "0}".format(len(result[3])))
