"""
Created on 25-OCTOBER-2023

@author: umang.agrawal
"""
import time

from Columnar.columnar_base import ColumnarBaseTest
from Queue import Queue
from CbasLib.CBASOperations import CBASHelper


class CopyIntoStandaloneCollectionFromS3(ColumnarBaseTest):

    def setUp(self):
        super(CopyIntoStandaloneCollectionFromS3, self).setUp()

        # Since all the test cases are being run on 1 cluster only
        self.instance = self.tenant.columnar_instances[0]

        self.aws_access_key = self.input.param("aws_access_key")
        self.aws_secret_key = self.input.param("aws_secret_key")
        self.aws_session_token = self.input.param("aws_session_token", "")

        # For sanity tests we are hard coding the bucket from which the data
        # will be read. This will ensure stable and consistent test runs.
        self.aws_region = "us-west-1"
        self.aws_bucket_name = "columnar-functional-sanity-test-data"

        if not self.columnar_spec_name:
            self.columnar_spec_name = "sanity.copy_into_standalone_collection_from_s3"

        self.columnar_spec = self.cbas_util.get_columnar_spec(
            self.columnar_spec_name)

        self.doc_count_per_format = {
            "json": 7920000, "parquet": 7920000,
            "csv": 7920000, "tsv": 7920000}

        self.files_to_use_in_include = {
            "json": [["*/file_1.json"], 7800000],
            "csv": [["*/file_2.csv"], 7800000],
            "tsv": [["*/file_3.tsv"], 7800000],
            "parquet": [["*/file_5.parquet"], 7800000]
        }

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        """
        Delete all the analytics link and columnar instance
        """
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.instance, self.columnar_spec):
            self.fail("Error while deleting cbas entities")
        super(CopyIntoStandaloneCollectionFromS3, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished", stage="Teardown")

    def test_create_copyinto_query_using_path_drop_standalone_collection(self):
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
        self.columnar_spec["standalone_dataset"][
            "num_of_standalone_coll"] = self.input.param(
            "num_of_standalone_coll", 1)
        self.columnar_spec["standalone_dataset"]["primary_key"] = [{"id": "string", "product_name": "string"}]

        file_format = self.input.param("file_format", "json")
        dataset_properties = self.columnar_spec["standalone_dataset"][
            "standalone_collection_properties"][0]
        dataset_properties["external_container_name"] = self.aws_bucket_name
        dataset_properties["file_format"] = file_format
        dataset_properties["path_on_external_container"] = "level_1_folder_1"
        use_include = self.input.param("use_include", False)
        if use_include:
            dataset_properties["file_format"] = self.files_to_use_in_include[file_format][0]
        else:
            dataset_properties["include"] = "*.{0}".format(file_format)
        dataset_properties["region"] = self.aws_region

        if file_format in ["csv", "tsv"]:
            dataset_properties["object_construction_def"] = (
                "id string,product_name string,product_link string,"
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
            self.instance, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        jobs = Queue()
        results = []
        for standalone_coll in datasets:
            jobs.put((self.cbas_util.copy_from_external_resource_into_standalone_collection,
                      {
                          "cluster": self.instance, "collection_name": standalone_coll.name,
                          "aws_bucket_name": standalone_coll.dataset_properties["external_container_name"],
                          "external_link_name":  standalone_coll.link_name,
                          "dataverse_name": standalone_coll.dataverse_name,
                          "database_name": standalone_coll.database_name,
                          "files_to_include": standalone_coll.dataset_properties["include"],
                          "file_format":  standalone_coll.dataset_properties["file_format"],
                          "type_parsing_info": standalone_coll.dataset_properties["object_construction_def"],
                          "path_on_aws_bucket": standalone_coll.dataset_properties["path_on_external_container"],
                          "header": standalone_coll.dataset_properties["header"],
                          "null_string": standalone_coll.dataset_properties["null_string"],
                          "files_to_exclude": standalone_coll.dataset_properties["exclude"],
                          "parse_json_string": standalone_coll.dataset_properties["parse_json_string"],
                          "convert_decimal_to_double": standalone_coll.dataset_properties["convert_decimal_to_double"],
                          "timezone": standalone_coll.dataset_properties["timezone"]
                      }))

        self.cbas_util.run_jobs_in_parallel(
                jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.fail("Copy into command failure")

        doc_count = {
            "json": 1560000, "csv": 1560000, "tsv": 1560000, "parquet": 1560000
        }
        jobs = Queue()
        results = []

        for dataset in self.cbas_util.get_all_dataset_objs("standalone"):
            jobs.put((
                self.cbas_util.get_num_items_in_cbas_dataset,
                {"cluster": self.instance, "dataset_name": dataset.full_name,
                 "timeout": 3600, "analytics_timeout": 3600}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        for result in results:
            if not (result == doc_count[file_format]):
                self.fail("Expected doc count between {0}. Actual doc "
                          "count {1}".format(doc_count[file_format], result))

        results = []
        query = "select * from {} limit 1000"
        for dataset in self.cbas_util.get_all_dataset_objs("standalone"):
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
                self.fail("Doc count mismatch. Expected - 1000, Actual - {0}".format(len(result[3])))

    def test_create_copyinto_query_drop_standalone_collection(self):
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

        self.columnar_spec["standalone_dataset"][
            "num_of_standalone_coll"] = self.input.param(
            "num_of_standalone_coll", 1)
        self.columnar_spec["standalone_dataset"]["primary_key"] = [{"id": "string", "product_name": "string"}]

        file_format = self.input.param("file_format", "json")
        dataset_properties = self.columnar_spec["standalone_dataset"][
            "standalone_collection_properties"][0]
        dataset_properties["external_container_name"] = self.aws_bucket_name
        dataset_properties["file_format"] = file_format
        use_include = self.input.param("use_include", False)
        if use_include:
            dataset_properties["include"] = self.files_to_use_in_include[file_format][0]
        else:
            dataset_properties["include"] = "*.{0}".format(file_format)
        dataset_properties["region"] = self.aws_region

        if file_format in ["csv", "tsv"]:
            dataset_properties["object_construction_def"] = (
                "id string,product_name string,product_link string,"
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
            self.instance, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for standalone_coll in datasets:
            if not (
                    self.cbas_util.copy_from_external_resource_into_standalone_collection(
                        self.instance, standalone_coll.name,
                        standalone_coll.dataset_properties["external_container_name"],
                        standalone_coll.link_name, standalone_coll.dataverse_name,
                        standalone_coll.database_name,
                        standalone_coll.dataset_properties["include"],
                        standalone_coll.dataset_properties["file_format"],
                        standalone_coll.dataset_properties["object_construction_def"],
                        standalone_coll.dataset_properties["path_on_external_container"],
                        standalone_coll.dataset_properties["header"],
                        standalone_coll.dataset_properties["null_string"],
                        standalone_coll.dataset_properties["exclude"],
                        standalone_coll.dataset_properties["parse_json_string"],
                        standalone_coll.dataset_properties["convert_decimal_to_double"],
                        standalone_coll.dataset_properties["timezone"]
                    )):
                self.fail("Error while copying data from S3 into {0} "
                          "standalone collection".format(standalone_coll.full_name))

        jobs = Queue()
        results = []

        for dataset in self.cbas_util.get_all_dataset_objs("standalone"):
            jobs.put((
                self.cbas_util.get_num_items_in_cbas_dataset,
                {"cluster": self.instance, "dataset_name": dataset.full_name,
                 "timeout": 3600, "analytics_timeout": 3600}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        for result in results:
            if use_include:
                if not result == self.files_to_use_in_include[file_format][1]:
                    self.fail("Expected doc count between 0-{0}. Actual doc "
                              "count {1}".format(self.files_to_use_in_include[file_format][1], result))
            else:
                if not result == self.doc_count_per_format[file_format]:
                    self.fail("Expected doc count between 0-{0}. Actual doc "
                              "count {1}".format(self.doc_count_per_format[file_format], result))

        results = []
        query = "select * from {} limit 1000"
        for dataset in self.cbas_util.get_all_dataset_objs("standalone"):
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
                self.fail("Doc count mismatch. Expected - 1000, Actual - {0}".format(len(result[3])))

    def test_create_copyinto_query_missing_typedef_drop_standalone_collection(self):
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

        self.columnar_spec["standalone_dataset"][
            "num_of_standalone_coll"] = self.input.param(
            "num_of_standalone_coll", 1)
        self.columnar_spec["standalone_dataset"]["primary_key"] = [{"name": "string", "email": "string"}]

        file_format = self.input.param("file_format", "json")
        dataset_properties = self.columnar_spec["standalone_dataset"][
            "standalone_collection_properties"][0]
        dataset_properties["external_container_name"] = self.aws_bucket_name
        dataset_properties["file_format"] = file_format
        use_include = self.input.param("use_include", False)
        if use_include:
            dataset_properties["include"] = self.files_to_use_in_include[file_format][0]
        else:
            dataset_properties["include"] = "*.{0}".format(file_format)
        dataset_properties["region"] = self.aws_region

        if file_format in ["csv", "tsv"]:
            dataset_properties["object_construction_def"] = None
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

        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for standalone_coll in datasets:
            if not (
                    self.cbas_util.copy_from_external_resource_into_standalone_collection(
                        self.instance, standalone_coll.name,
                        standalone_coll.dataset_properties["external_container_name"],
                        standalone_coll.link_name, standalone_coll.dataverse_name,
                        standalone_coll.database_name,
                        standalone_coll.dataset_properties["include"],
                        standalone_coll.dataset_properties["file_format"],
                        standalone_coll.dataset_properties["object_construction_def"],
                        standalone_coll.dataset_properties["path_on_external_container"],
                        standalone_coll.dataset_properties["header"],
                        standalone_coll.dataset_properties["null_string"],
                        standalone_coll.dataset_properties["exclude"],
                        standalone_coll.dataset_properties["parse_json_string"],
                        standalone_coll.dataset_properties["convert_decimal_to_double"],
                        standalone_coll.dataset_properties["timezone"],
                        expected_error="Inline type definition is required",
                        expected_error_code=24108, validate_error_msg=True
                    )):
                self.fail("Error while copying data from S3 into {0} "
                          "standalone collection".format(standalone_coll.full_name))

    def test_create_copyinto_query_missing_with_clause_drop_standalone_collection(self):
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

        self.columnar_spec["standalone_dataset"][
            "num_of_standalone_coll"] = self.input.param(
            "num_of_standalone_coll", 1)
        self.columnar_spec["standalone_dataset"]["primary_key"] = [{"name": "string", "email": "string"}]
        dataset_properties = self.columnar_spec["standalone_dataset"][
            "standalone_collection_properties"][0]
        dataset_properties["external_container_name"] = self.aws_bucket_name
        dataset_properties["region"] = self.aws_region

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.instance, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for standalone_coll in datasets:
            if not (
                    self.cbas_util.copy_from_external_resource_into_standalone_collection(
                        self.instance, standalone_coll.name,
                        standalone_coll.dataset_properties["external_container_name"],
                        standalone_coll.link_name, standalone_coll.dataverse_name,
                        standalone_coll.database_name,
                        standalone_coll.dataset_properties["include"],
                        standalone_coll.dataset_properties["file_format"],
                        standalone_coll.dataset_properties["object_construction_def"],
                        standalone_coll.dataset_properties["path_on_external_container"],
                        standalone_coll.dataset_properties["header"],
                        standalone_coll.dataset_properties["null_string"],
                        standalone_coll.dataset_properties["exclude"],
                        standalone_coll.dataset_properties["parse_json_string"],
                        standalone_coll.dataset_properties["convert_decimal_to_double"],
                        standalone_coll.dataset_properties["timezone"],
                        expected_error="Parameter(s) format must be specified",
                        expected_error_code=21010, validate_error_msg=True
                    )):
                self.fail("Error while copying data from S3 into {0} "
                          "standalone collection".format(standalone_coll.full_name))


    def test_create_copyinto_query_drop_link(self):
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
        self.columnar_spec["standalone_dataset"][
            "num_of_standalone_coll"] = self.input.param(
            "num_of_standalone_coll", 1)
        self.columnar_spec["standalone_dataset"]["primary_key"] = [{"id": "string", "product_name": "string"}]

        file_format = self.input.param("file_format", "json")
        dataset_properties = self.columnar_spec["standalone_dataset"][
            "standalone_collection_properties"][0]
        dataset_properties["external_container_name"] = self.aws_bucket_name
        dataset_properties["file_format"] = file_format
        dataset_properties["include"] = "*.{0}".format(file_format)
        dataset_properties["region"] = self.aws_region
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.instance, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)
        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        jobs = Queue()
        results = []
        for standalone_coll in datasets:
            jobs.put((self.cbas_util.copy_from_external_resource_into_standalone_collection,
                      {
                          "cluster": self.instance, "collection_name": standalone_coll.name,
                          "aws_bucket_name": standalone_coll.dataset_properties["external_container_name"],
                          "external_link_name": standalone_coll.link_name,
                          "dataverse_name": standalone_coll.dataverse_name,
                          "database_name": standalone_coll.database_name,
                          "files_to_include": standalone_coll.dataset_properties["include"],
                          "file_format": standalone_coll.dataset_properties["file_format"],
                          "type_parsing_info": standalone_coll.dataset_properties["object_construction_def"],
                          "path_on_aws_bucket": standalone_coll.dataset_properties["path_on_external_container"],
                          "header": standalone_coll.dataset_properties["header"],
                          "null_string": standalone_coll.dataset_properties["null_string"],
                          "files_to_exclude": standalone_coll.dataset_properties["exclude"],
                          "parse_json_string": standalone_coll.dataset_properties["parse_json_string"],
                          "convert_decimal_to_double": standalone_coll.dataset_properties["convert_decimal_to_double"],
                          "timezone": standalone_coll.dataset_properties["timezone"]
                      }))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=True)

        self.log.info("Sleeping 15 seconds, waiting all copy statement to execute")
        time.sleep(5)
        external_links = self.cbas_util.get_all_link_objs("external")
        self.log.info("Disconnecting external links")
        for link in external_links:
            if not self.cbas_util.drop_link(self.instance, link.full_name):
                self.fail("Fail to delete link while copy from s3")
        jobs.join()
        if not all(results):
            self.fail("Copy to statement failed after link disconnect")
    def test_create_copyinto_query_incorrect_file_format_clause_drop_standalone_collection(self):
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

        self.columnar_spec["standalone_dataset"][
            "num_of_standalone_coll"] = self.input.param(
            "num_of_standalone_coll", 1)
        self.columnar_spec["standalone_dataset"]["primary_key"] = [{"id": "string", "product_name": "string"}]
        dataset_properties = self.columnar_spec["standalone_dataset"][
            "standalone_collection_properties"][0]
        dataset_properties["external_container_name"] = self.aws_bucket_name
        dataset_properties["region"] = self.aws_region
        dataset_properties["file_format"] = "null"

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.instance, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for standalone_coll in datasets:
            if not (
                    self.cbas_util.copy_from_external_resource_into_standalone_collection(
                        self.instance, standalone_coll.name,
                        standalone_coll.dataset_properties["external_container_name"],
                        standalone_coll.link_name, standalone_coll.dataverse_name,
                        standalone_coll.database_name,
                        standalone_coll.dataset_properties["include"],
                        standalone_coll.dataset_properties["file_format"],
                        standalone_coll.dataset_properties["object_construction_def"],
                        standalone_coll.dataset_properties["path_on_external_container"],
                        standalone_coll.dataset_properties["header"],
                        standalone_coll.dataset_properties["null_string"],
                        standalone_coll.dataset_properties["exclude"],
                        standalone_coll.dataset_properties["parse_json_string"],
                        standalone_coll.dataset_properties["convert_decimal_to_double"],
                        standalone_coll.dataset_properties["timezone"],
                        expected_error="Invalid value for parameter 'format'",
                        expected_error_code=21008, validate_error_msg=True
                    )):
                self.fail("Error while copying data from S3 into {0} "
                          "standalone collection".format(standalone_coll.full_name))