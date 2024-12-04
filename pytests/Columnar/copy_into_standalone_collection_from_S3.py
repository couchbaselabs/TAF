"""
Created on 25-OCTOBER-2023

@author: umang.agrawal
"""
import time

from queue import Queue

from Columnar.columnar_base import ColumnarBaseTest


class CopyIntoStandaloneCollectionFromS3(ColumnarBaseTest):

    def setUp(self):
        super(CopyIntoStandaloneCollectionFromS3, self).setUp()

        # Since all the test cases are being run on 1 cluster only
        self.columnar_cluster = self.tenant.columnar_instances[0]

        if not self.columnar_spec_name:
            self.columnar_spec_name = "full_template"

        self.doc_count_per_format = {
            "json": 7920000, "parquet": 7920000,
            "csv": 7920000, "tsv": 7920000, "avro": 7920000}

        self.files_to_use_in_include = {
            "json": [["*/file_1.json"], 7800000],
            "csv": [["*/file_2.csv"], 7800000],
            "tsv": [["*/file_3.tsv"], 7800000],
            "parquet": [["*/file_5.parquet"], 7800000],
            "avro": [["*/file_4.avro"], 7800000]
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
                self.columnar_cluster, self.columnar_spec):
            self.fail("Error while deleting cbas entities")
        super(CopyIntoStandaloneCollectionFromS3, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished", stage="Teardown")

    def test_create_copyinto_query_using_path_drop_standalone_collection(self):
        file_format = self.input.param("file_format", "json")
        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            external_collection_file_formats=[file_format],
            path_on_external_container="level_1_folder_1")

        if file_format == "parquet":
            self.columnar_spec["standalone_dataset"]["primary_key"] = [
                {"`name=id`": "string", "`name=product_name`": "string"}]
        else:
            self.columnar_spec["standalone_dataset"]["primary_key"] = [
                {"id": "string", "product_name": "string"}]

        self.columnar_spec["standalone_dataset"][
            "standalone_collection_properties"] = self.columnar_spec[
            "external_dataset"]["external_dataset_properties"]

        self.columnar_spec["standalone_dataset"]["data_source"] = ["s3"]

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.columnar_cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        jobs = Queue()
        results = []
        for standalone_coll in datasets:
            jobs.put((self.cbas_util.copy_from_external_resource_into_standalone_collection,
                      {
                          "cluster": self.columnar_cluster, "collection_name": standalone_coll.name,
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
                {"cluster": self.columnar_cluster, "dataset_name": dataset.full_name,
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
                {"cluster": self.columnar_cluster,
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
        file_format = self.input.param("file_format", "json")
        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            external_collection_file_formats=[file_format])

        if file_format == "parquet":
            self.columnar_spec["standalone_dataset"]["primary_key"] = [
                {"`name=id`": "string", "`name=product_name`": "string"}]
        else:
            self.columnar_spec["standalone_dataset"]["primary_key"] = [
                {"id": "string", "product_name": "string"}]

        self.columnar_spec["standalone_dataset"][
            "standalone_collection_properties"] = self.columnar_spec[
            "external_dataset"]["external_dataset_properties"]

        self.columnar_spec["standalone_dataset"]["data_source"] = ["s3"]

        dataset_properties = self.columnar_spec["standalone_dataset"][
            "standalone_collection_properties"][0]

        use_include = self.input.param("use_include", False)
        if use_include:
            dataset_properties["include"] = self.files_to_use_in_include[file_format][0]
        else:
            dataset_properties["include"] = "*.{0}".format(file_format)

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.columnar_cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for standalone_coll in datasets:
            if not (
                    self.cbas_util.copy_from_external_resource_into_standalone_collection(
                        self.columnar_cluster, standalone_coll.name,
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
                {"cluster": self.columnar_cluster, "dataset_name": dataset.full_name,
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
                {"cluster": self.columnar_cluster,
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
        file_format = self.input.param("file_format", "json")
        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            external_collection_file_formats=[file_format])

        self.columnar_spec["standalone_dataset"]["primary_key"] = [
            {"id": "string", "product_name": "string"}]

        self.columnar_spec["standalone_dataset"][
            "standalone_collection_properties"] = self.columnar_spec[
            "external_dataset"]["external_dataset_properties"]

        self.columnar_spec["standalone_dataset"]["data_source"] = ["s3"]

        dataset_properties = self.columnar_spec["standalone_dataset"][
            "standalone_collection_properties"][0]
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
            self.columnar_cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for standalone_coll in datasets:
            if not (
                    self.cbas_util.copy_from_external_resource_into_standalone_collection(
                        self.columnar_cluster, standalone_coll.name,
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
        file_format = self.input.param("file_format", "json")
        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            external_collection_file_formats=[file_format])

        self.columnar_spec["standalone_dataset"]["primary_key"] = [
            {"id": "string", "product_name": "string"}]

        self.columnar_spec["standalone_dataset"][
            "standalone_collection_properties"] = self.columnar_spec[
            "external_dataset"]["external_dataset_properties"]

        self.columnar_spec["standalone_dataset"]["data_source"] = ["s3"]

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.columnar_cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        statement = "COPY INTO {0} FROM `{1}` AT {2} PATH \"\";"
        for standalone_coll in datasets:
            statement_to_execute = statement.format(standalone_coll.full_name,
                                                    standalone_coll.dataset_properties["external_container_name"],
                                                    standalone_coll.link_name)
            status, metrics, errors, results, _, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.columnar_cluster, statement_to_execute)
            if not self.cbas_util.validate_error_and_warning_in_response(status, errors,
                                                                         expected_error="Parameter(s) format or table-format must be specified",
                                                                         expected_error_code=21010):
                self.fail("Error while copying data from S3 into {0} "
                          "standalone collection".format(standalone_coll.full_name))

    def test_create_copyinto_query_drop_link(self):
        file_format = self.input.param("file_format", "json")
        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            external_collection_file_formats=[file_format])

        self.columnar_spec["standalone_dataset"]["primary_key"] = [
            {"id": "string", "product_name": "string"}]

        self.columnar_spec["standalone_dataset"][
            "standalone_collection_properties"] = self.columnar_spec[
            "external_dataset"]["external_dataset_properties"]

        self.columnar_spec["standalone_dataset"]["data_source"] = ["s3"]

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.columnar_cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        datasets = self.cbas_util.get_all_dataset_objs("standalone")
        jobs = Queue()
        results = []
        for standalone_coll in datasets:
            jobs.put((self.cbas_util.copy_from_external_resource_into_standalone_collection,
                      {
                          "cluster": self.columnar_cluster, "collection_name": standalone_coll.name,
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
        self.log.info("Dropping external links")
        for link in external_links:
            if not self.cbas_util.drop_link(self.columnar_cluster, link.full_name):
                self.fail("Fail to delete link while copy from s3")
        jobs.join()
        if not all(results):
            self.fail("Copy to statement failed after link disconnect")

    def test_create_copyinto_query_incorrect_file_format_clause_drop_standalone_collection(self):
        file_format = self.input.param("file_format", "json")
        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            external_collection_file_formats=[file_format])

        self.columnar_spec["standalone_dataset"]["primary_key"] = [
            {"id": "string", "product_name": "string"}]

        self.columnar_spec["standalone_dataset"][
            "standalone_collection_properties"] = self.columnar_spec[
            "external_dataset"]["external_dataset_properties"]

        self.columnar_spec["standalone_dataset"]["data_source"] = ["s3"]

        dataset_properties = self.columnar_spec["standalone_dataset"][
            "standalone_collection_properties"][0]
        dataset_properties["file_format"] = "null"

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.columnar_cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        for standalone_coll in datasets:
            if not (
                    self.cbas_util.copy_from_external_resource_into_standalone_collection(
                        self.columnar_cluster, standalone_coll.name,
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