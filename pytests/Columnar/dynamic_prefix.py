from Columnar.columnar_base import ColumnarBaseTest
from Queue import Queue


class DynamicPrefix(ColumnarBaseTest):

    def setUp(self):
        super(DynamicPrefix, self).setUp()

        # Since all the test cases are being run on 1 cluster only
        self.cluster = self.tenant.columnar_instances[0]

        self.aws_access_key = self.input.param("aws_access_key")
        self.aws_secret_key = self.input.param("aws_secret_key")
        self.aws_session_token = self.input.param("aws_session_token", "")
        self.skip_item_count_check = self.input.param("skip_item_count_check", False)

        # For sanity tests we are hard coding the bucket from which the data
        # will be read. This will ensure stable and consistent test runs.
        self.aws_region = "us-west-1"
        self.aws_bucket_name = "columnar-functional-sanity-test-data"

        if not self.columnar_spec_name:
            self.columnar_spec_name = "sanity.S3_external_datasets"

        self.columnar_spec = self.cbas_util.get_columnar_spec(
            self.columnar_spec_name)

        self.doc_count_per_format = {
            "json": 7800000, "parquet": 7800000,
            "csv": 7800000, "tsv": 7800000, "avro": 7800000}

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.cluster, self.columnar_spec):
            self.fail("Error while deleting cbas entities")
        super(DynamicPrefix, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage="Teardown")

    def test_create_external_collections_with_dynamic_prefixes(self):
        testcases = [
            {
                "description": "Create external collection with invalid compute field type",
                "path": "level_{level_no:int}_folder_{folder_no:float}",
                "expected_error": "Unsupported computed field type: 'float'"
            },
            {
                "description": "Create external collection with duplicate compute fields in the path",
                "path": "level_{level_no:int}_folder_{level_no:int}",
                "expected_error": "Duplicate field name 'level_no'"
            }
        ]

        self.columnar_spec["external_link"]["properties"] = [{
             "type": "s3",
             "region": self.aws_region,
             "accessKeyId": self.aws_access_key,
             "secretAccessKey": self.aws_secret_key,
             "serviceEndpoint": None
        }]

        failed_testcases = list()

        for testcase in testcases:
            try:
                self.log.info(testcase["description"])

                self.cbas_util.create_external_link_from_spec(self.cluster, self.columnar_spec)

                self.cbas_util.create_external_dataset_obj(
                    self.cluster, external_container_names={
                    self.aws_bucket_name: self.aws_region},
                    paths_on_external_container=testcase["path"])

                for dataset_obj in self.cbas_util.get_all_dataset_objs():
                    if not self.cbas_util.create_dataset_on_external_resource(
                    self.cluster, dataset_obj.name,
                    dataset_obj.dataset_properties[
                        "external_container_name"],
                    dataset_obj.link_name, False,
                    dataset_obj.dataverse_name,
                    dataset_obj.database_name,
                    dataset_obj.dataset_properties[
                        "object_construction_def"],
                    testcase["path"],
                    dataset_obj.dataset_properties[
                        "file_format"],
                    dataset_obj.dataset_properties[
                        "redact_warning"],
                    dataset_obj.dataset_properties[
                        "header"],
                    dataset_obj.dataset_properties[
                        "null_string"],
                    dataset_obj.dataset_properties[
                        "include"],
                    dataset_obj.dataset_properties[
                        "exclude"],
                    dataset_obj.dataset_properties[
                        "parse_json_string"],
                    dataset_obj.dataset_properties[
                        "convert_decimal_to_double"],
                    dataset_obj.dataset_properties[
                        "timezone"],
                    True, False, None, testcase["expected_error"], None):
                        raise Exception("Error while creating dataset")

            except Exception as err:
                self.log.error(str(err))
                failed_testcases.append(testcase["description"])

        if failed_testcases:
            self.fail("Following testcases failed - {0}".format(
                str(failed_testcases)))

    def test_create_query_external_collections_with_mismatch_in_actual_and_expected_path(self):
        # Update columnar spec based on conf file params
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
            self.input.param("path_on_external_container",
                             "level_{level-no:int}_folder_{folder-no:int}"))

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        jobs = Queue()
        results = []
        for dataset in self.cbas_util.get_all_dataset_objs("external"):
            jobs.put((
                self.cbas_util.get_num_items_in_cbas_dataset,
                {"cluster": self.cluster, "dataset_name": dataset.full_name,
                 "timeout": 3600, "analytics_timeout": 3600}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        for result in results:
            if result != 0:
                self.fail("Doc count mismatch. Expected - {0}, Actual - {"
                          "1}".format(0, result[0]))

        results = []
        query = "select * from {} limit 1000"
        for dataset in self.cbas_util.get_all_dataset_objs("external"):
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
            elif len(result[3]) != 0:
                self.fail("Doc count mismatch. Expected - 0, Actual - {"
                          "0}".format(len(result[3])))

    def test_create_query_external_collections_with_dynamic_prefixes_with_diff_path_formats(self):
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
            self.input.param("path_on_external_container",
                             "level_{level_no:int}_folder_{folder_no:int}"))

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
            self.cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        if not self.skip_item_count_check:
            jobs = Queue()
            datasets = self.cbas_util.get_all_dataset_objs("external")
            results = []
            # Read all the docs in the aws s3 bucket
            for dataset in datasets:
                jobs.put((
                    self.cbas_util.get_num_items_in_cbas_dataset,
                    {"cluster": self.cluster, "dataset_name": dataset.full_name,
                    "timeout": 3600, "analytics_timeout": 3600}))
            self.cbas_util.run_jobs_in_parallel(
                jobs, results, self.sdk_clients_per_user, async_run=False)
            for result in results:
                if result != self.doc_count_per_format[file_format]:
                    self.fail("Doc count mismatch. Expected - {0}, Actual - {"
                            "1}".format(
                        self.doc_count_per_format[file_format], result))
