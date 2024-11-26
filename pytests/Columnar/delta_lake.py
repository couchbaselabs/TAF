"""
Created on 8-November-2024

@author: umang.agrawal
"""

import concurrent.futures
import json
import time

from awsLib.S3 import S3
from Columnar.columnar_base import ColumnarBaseTest
from copy import deepcopy
from delta_lake_util.delta_lake_util import DeltaLakeUtils
from deepdiff import DeepDiff


class DeltaLakeDatasets(ColumnarBaseTest):

    def setUp(self):
        super(DeltaLakeDatasets, self).setUp()

        # Since all the test cases are being run on 1 cluster only
        self.cluster = self.tenant.columnar_instances[0]

        if not self.columnar_spec_name:
            self.columnar_spec_name = "full_template"

        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            external_collection_file_formats=[self.input.param("file_format")])

        self.s3_obj = S3(self.aws_access_key, self.aws_secret_key,
                         region=self.aws_region)

        # Create delta lake util object and initialise spark session which
        # will be used to perform operation on Delta Lake table.
        self.delta_lake_util = DeltaLakeUtils()
        self.delta_lake_util.create_spark_session(
            self.aws_access_key, self.aws_secret_key)

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        for bucket in self.delta_table_info:
            self.s3_obj.delete_bucket(bucket)

        super(DeltaLakeDatasets, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage="Teardown")

    def setup_delta_lake_infra(
            self, num_s3_bucket=1, num_delta_tables_per_bucket=1, 
            load_data=True, num_docs=1000, doc_size=1024,
            include_date_time_fields=False, include_decimal=False):
        self.delta_table_info = dict()
        for i in range(0, num_s3_bucket):
            aws_bucket_name = "columnar-delta-lake-regression-" + str(
                int(time.time()))
            for j in range(5):
                try:
                    self.log.info(f"Creating S3 bucket {aws_bucket_name}")
                    if self.s3_obj.create_bucket(
                            aws_bucket_name, self.aws_region):
                        self.delta_table_info[aws_bucket_name] = list()
                        break
                except Exception as e:
                    self.log.error(
                        "Creating S3 bucket - {0} in region {1}. "
                        "Failed.".format(
                            aws_bucket_name, self.aws_region))
                    self.log.error(str(e))
            # If load_data is False, this will just create dummy entries in 
            # the self.delta_table_info dict, actual table will be created when 
            # we write data.
            for k in range(0, num_delta_tables_per_bucket):
                delta_table_name = f"delta_table_{k}/"
                self.delta_table_info[aws_bucket_name].append(delta_table_name)
                if load_data:
                    # Write in batches of 1000 docs for faster execution and
                    # to prevent overloading memory.
                    start = 0
                    end = 1000
                    while end <= num_docs:
                        data_to_write = list()
                        for l in range(start, end):
                            data_to_write.append(self.cbas_util.generate_docs(
                                document_size=doc_size,
                                include_reviews_field=False,
                                include_date_time_fields=include_date_time_fields,
                                include_decimal_field=include_decimal
                            ))
                        self.delta_lake_util.write_data_into_table(
                            data=data_to_write,
                            s3_bucket_name=aws_bucket_name,
                            delta_table_path=delta_table_name)
                        start = end
                        doc_remaining = num_docs - end
                        if doc_remaining > 1000:
                            end += 1000
                        elif 0 < doc_remaining < 1000:
                            end += doc_remaining
                        else:
                            break

    def perform_CRUD_on_data_in_delta_table(
            self, s3_bucket_name, delta_table_path, num_docs_to_create=0,
            doc_size=1024, delete_condition=None, update_condition=None,
            updated_values={}):

        if delete_condition:
            self.delta_lake_util.delete_data_from_table(
                s3_bucket_name=s3_bucket_name,
                delta_table_path=delta_table_path,
                delete_condition=delete_condition)

        if update_condition and updated_values:
            self.delta_lake_util.update_data_in_table(
                s3_bucket_name=s3_bucket_name,
                delta_table_path=delta_table_path, data_updates=updated_values,
                update_condition=update_condition)

        if num_docs_to_create:
            # Write in batches of 1000 docs for faster execution and
            # to prevent overloading memory.
            start = 0
            end = 1000
            while end <= num_docs_to_create:
                data_to_write = list()
                for l in range(start, end):
                    data_to_write.append(self.cbas_util.generate_docs(
                        document_size=doc_size,
                        include_reviews_field=False))
                self.delta_lake_util.write_data_into_table(
                    data=data_to_write, s3_bucket_name=s3_bucket_name,
                    delta_table_path=delta_table_path)
                start = end
                doc_remaining = num_docs_to_create - end
                if doc_remaining > 1000:
                    end += 1000
                elif 0 < doc_remaining < 1000:
                    end += doc_remaining
                else:
                    break

    def test_create_query_drop_external_datasets_on_delta_lake_table(self):
        self.setup_delta_lake_infra(
            num_s3_bucket=self.input.param("num_s3_bucket", 1),
            num_delta_tables_per_bucket=self.input.param(
                "num_delta_tables_per_bucket", 1),
            load_data=True,
            num_docs=self.input.param("num_docs", 1000),
            doc_size=self.input.param("doc_size", 1024)

        )
        # Update columnar spec based on conf file params
        dataset_properties = deepcopy(self.columnar_spec["external_dataset"][
                                          "external_dataset_properties"][0])
        self.columnar_spec["external_dataset"][
            "external_dataset_properties"] = list()
        for s3_bucket in self.delta_table_info:
            for delta_table in self.delta_table_info[s3_bucket]:
                dataset_properties["external_container_name"] = s3_bucket
                dataset_properties["path_on_external_container"] = delta_table
                self.columnar_spec["external_dataset"][
                    "external_dataset_properties"].append(dataset_properties)

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        datasets = self.cbas_util.get_all_dataset_objs("external")
        for dataset in datasets:
            status, _, errors, results, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.cluster, f"select value ds from {dataset.full_name} as "
                              f"ds")
            if status != "success":
                self.fail(f"Unable to query dataset {dataset.full_name}. "
                          f"Error - {errors}")
            data_in_delta_table = self.delta_lake_util.read_data_from_table(
                dataset.dataset_properties["external_container_name"],
                dataset.dataset_properties["path_on_external_container"])
            diff = DeepDiff(results, data_in_delta_table, ignore_order=True)
            if diff:
                self.fail(
                    f"Data mismatch between Delta lake table "
                    f"{dataset.dataset_properties['path_on_external_container']}"
                    f" and external collection {dataset.full_name}. "
                    f"\nDifferences - {diff}")

    def test_run_query_on_collection_while_performing_CRUD_on_associated_table(
            self):
        self.setup_delta_lake_infra(
            num_s3_bucket=self.input.param("num_s3_bucket", 1),
            num_delta_tables_per_bucket=self.input.param(
                "num_delta_tables_per_bucket", 1),
            load_data=True,
            num_docs=self.input.param("num_docs", 1000),
            doc_size=self.input.param("doc_size", 1024)

        )
        # Update columnar spec based on conf file params
        dataset_properties = deepcopy(self.columnar_spec["external_dataset"][
                                          "external_dataset_properties"][0])
        self.columnar_spec["external_dataset"][
            "external_dataset_properties"] = list()
        for s3_bucket in self.delta_table_info:
            for delta_table in self.delta_table_info[s3_bucket]:
                dataset_properties["external_container_name"] = s3_bucket
                dataset_properties["path_on_external_container"] = delta_table
                self.columnar_spec["external_dataset"][
                    "external_dataset_properties"].append(dataset_properties)

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        datasets = self.cbas_util.get_all_dataset_objs("external")
        for dataset in datasets:
            status, _, errors, results, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.cluster, f"select value ds from {dataset.full_name} as "
                              f"ds")
            if status != "success":
                self.fail(f"Unable to query dataset {dataset.full_name}. "
                          f"Error - {errors}")
            data_in_delta_table = self.delta_lake_util.read_data_from_table(
                dataset.dataset_properties["external_container_name"],
                dataset.dataset_properties["path_on_external_container"])
            diff = DeepDiff(results, data_in_delta_table, ignore_order=True)
            if diff:
                self.fail(
                    f"Data mismatch between Delta lake table "
                    f"{dataset.dataset_properties['path_on_external_container']}"
                    f" and external collection {dataset.full_name}. "
                    f"\nDifferences - {diff}")

        self.log.info("Performing CRUD on delta table while running query "
                      "parallely on external collection")

        for dataset in datasets:
            data_in_delta_table = self.delta_lake_util.read_data_from_table(
                dataset.dataset_properties["external_container_name"],
                dataset.dataset_properties["path_on_external_container"])
            with concurrent.futures.ThreadPoolExecutor() as executor:
                # Submit the jobs
                query_job = executor.submit(
                    self.cbas_util.execute_statement_on_cbas_util,
                    self.cluster,
                    f"select value ds from {dataset.full_name} as ds")
                crud_job = executor.submit(
                    self.perform_CRUD_on_data_in_delta_table,
                    dataset.dataset_properties["external_container_name"],
                    dataset.dataset_properties["path_on_external_container"],
                    self.input.param("num_docs", 1000), "free_parking = false",
                    "free_parking = true", {"price": "price + 10"}
                )

                # Collect results
                query_result = query_job.result()
                time.sleep(1)
                crud_job.result()

            if query_result[0] == "success":
                diff = DeepDiff(query_result[3], data_in_delta_table,
                                ignore_order=True)
                if diff:
                    self.fail(
                        f"Data mismatch between Delta lake table "
                        f"{dataset.dataset_properties['path_on_external_container']}"
                        f" and external collection {dataset.full_name}. "
                        f"\nDifferences - {diff}")
            else:
                self.fail("Error while running query parallely while "
                          "performing CRUD on associated Delta collection. "
                          "Error - {0}".format(query_result[2]))

    def test_run_query_on_collection_post_performing_CRUD_on_associated_table(
            self):
        self.setup_delta_lake_infra(
            num_s3_bucket=self.input.param("num_s3_bucket", 1),
            num_delta_tables_per_bucket=self.input.param(
                "num_delta_tables_per_bucket", 1),
            load_data=True,
            num_docs=self.input.param("num_docs", 1000),
            doc_size=self.input.param("doc_size", 1024)

        )
        # Update columnar spec based on conf file params
        dataset_properties = deepcopy(self.columnar_spec["external_dataset"][
                                          "external_dataset_properties"][0])
        self.columnar_spec["external_dataset"][
            "external_dataset_properties"] = list()
        for s3_bucket in self.delta_table_info:
            for delta_table in self.delta_table_info[s3_bucket]:
                dataset_properties["external_container_name"] = s3_bucket
                dataset_properties["path_on_external_container"] = delta_table
                self.columnar_spec["external_dataset"][
                    "external_dataset_properties"].append(dataset_properties)

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        datasets = self.cbas_util.get_all_dataset_objs("external")
        for dataset in datasets:
            status, _, errors, results, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.cluster, f"select value ds from {dataset.full_name} as "
                              f"ds")
            if status != "success":
                self.fail(f"Unable to query dataset {dataset.full_name}. "
                          f"Error - {errors}")
            data_in_delta_table = self.delta_lake_util.read_data_from_table(
                dataset.dataset_properties["external_container_name"],
                dataset.dataset_properties["path_on_external_container"])
            diff = DeepDiff(results, data_in_delta_table, ignore_order=True)
            if diff:
                self.fail(
                    f"Data mismatch between Delta lake table "
                    f"{dataset.dataset_properties['path_on_external_container']}"
                    f" and external collection {dataset.full_name}. "
                    f"\nDifferences - {diff}")

        self.log.info("Performing CRUD on delta table and validating query "
                      "result on associated external collection")

        for dataset in datasets:
            self.perform_CRUD_on_data_in_delta_table(
                s3_bucket_name=dataset.dataset_properties[
                    "external_container_name"],
                delta_table_path=dataset.dataset_properties[
                    "path_on_external_container"],
                num_docs_to_create=self.input.param("num_docs", 1000),
                delete_condition="free_parking = false",
                update_condition="free_parking = true",
                updated_values={"price": "price + 10"}
            )
            status, _, errors, results, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.cluster, f"select value ds from {dataset.full_name} as "
                              f"ds")
            if status != "success":
                self.fail(f"Unable to query dataset {dataset.full_name}. "
                          f"Error - {errors}")
            data_in_delta_table = self.delta_lake_util.read_data_from_table(
                dataset.dataset_properties["external_container_name"],
                dataset.dataset_properties["path_on_external_container"])
            diff = DeepDiff(results, data_in_delta_table, ignore_order=True)
            if diff:
                self.fail(
                    f"Data mismatch between Delta lake table "
                    f"{dataset.dataset_properties['path_on_external_container']}"
                    f" and external collection {dataset.full_name}. "
                    f"\nDifferences - {diff}")

    def test_run_query_on_collection_after_restoring_old_version_of_associated_table(
            self):
        self.setup_delta_lake_infra(
            num_s3_bucket=self.input.param("num_s3_bucket", 1),
            num_delta_tables_per_bucket=self.input.param(
                "num_delta_tables_per_bucket", 1),
            load_data=True,
            num_docs=self.input.param("num_docs", 1000),
            doc_size=self.input.param("doc_size", 1024)

        )
        # Update columnar spec based on conf file params
        dataset_properties = deepcopy(self.columnar_spec["external_dataset"][
                                          "external_dataset_properties"][0])
        self.columnar_spec["external_dataset"][
            "external_dataset_properties"] = list()
        for s3_bucket in self.delta_table_info:
            for delta_table in self.delta_table_info[s3_bucket]:
                dataset_properties["external_container_name"] = s3_bucket
                dataset_properties["path_on_external_container"] = delta_table
                self.columnar_spec["external_dataset"][
                    "external_dataset_properties"].append(dataset_properties)

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        datasets = self.cbas_util.get_all_dataset_objs("external")
        for dataset in datasets:
            status, _, errors, results, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.cluster, f"select value ds from {dataset.full_name} as "
                              f"ds")
            if status != "success":
                self.fail(f"Unable to query dataset {dataset.full_name}. "
                          f"Error - {errors}")
            data_in_delta_table = self.delta_lake_util.read_data_from_table(
                dataset.dataset_properties["external_container_name"],
                dataset.dataset_properties["path_on_external_container"])
            diff = DeepDiff(results, data_in_delta_table, ignore_order=True)
            if diff:
                self.fail(
                    f"Data mismatch between Delta lake table "
                    f"{dataset.dataset_properties['path_on_external_container']}"
                    f" and external collection {dataset.full_name}. "
                    f"\nDifferences - {diff}")

        self.log.info("Performing CRUD on delta table and validating query "
                      "result on associated external collection")

        for dataset in datasets:
            self.perform_CRUD_on_data_in_delta_table(
                s3_bucket_name=dataset.dataset_properties[
                    "external_container_name"],
                delta_table_path=dataset.dataset_properties[
                    "path_on_external_container"],
                num_docs_to_create=self.input.param("num_docs", 1000),
                delete_condition="free_parking = false",
                update_condition="free_parking = true",
                updated_values={"price": "price + 10"}
            )
            status, _, errors, results, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.cluster, f"select value ds from {dataset.full_name} as "
                              f"ds")
            if status != "success":
                self.fail(f"Unable to query dataset {dataset.full_name}. "
                          f"Error - {errors}")
            data_in_delta_table = self.delta_lake_util.read_data_from_table(
                dataset.dataset_properties["external_container_name"],
                dataset.dataset_properties["path_on_external_container"])
            diff = DeepDiff(results, data_in_delta_table, ignore_order=True)
            if diff:
                self.fail(
                    f"Data mismatch between Delta lake table "
                    f"{dataset.dataset_properties['path_on_external_container']}"
                    f" and external collection {dataset.full_name}. "
                    f"\nDifferences - {diff}")

            version_list = self.delta_lake_util.list_all_table_versions(
                dataset.dataset_properties["external_container_name"],
                dataset.dataset_properties["path_on_external_container"])

            self.log.info(
                f"Restoring delta table "
                f"{dataset.dataset_properties['path_on_external_container']} "
                f"to previous version")
            self.delta_lake_util.restore_previous_version_of_delta_table(
                dataset.dataset_properties["external_container_name"],
                dataset.dataset_properties["path_on_external_container"],
                version_list[-1])

            status, _, errors, results, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.cluster, f"select value ds from {dataset.full_name} as "
                              f"ds")
            if status != "success":
                self.fail(f"Unable to query dataset {dataset.full_name}. "
                          f"Error - {errors}")
            data_in_delta_table = self.delta_lake_util.read_data_from_table(
                dataset.dataset_properties["external_container_name"],
                dataset.dataset_properties["path_on_external_container"])
            diff = DeepDiff(results, data_in_delta_table, ignore_order=True)
            if diff:
                self.fail(
                    f"Data mismatch between Delta lake table "
                    f"{dataset.dataset_properties['path_on_external_container']}"
                    f" and external collection {dataset.full_name}. "
                    f"\nDifferences - {diff}")

    def test_run_query_on_collection_post_performing_vacuum_on_associated_table(
            self):
        self.setup_delta_lake_infra(
            num_s3_bucket=self.input.param("num_s3_bucket", 1),
            num_delta_tables_per_bucket=self.input.param(
                "num_delta_tables_per_bucket", 1),
            load_data=True,
            num_docs=self.input.param("num_docs", 1000),
            doc_size=self.input.param("doc_size", 1024)

        )
        # Update columnar spec based on conf file params
        dataset_properties = deepcopy(self.columnar_spec["external_dataset"][
                                          "external_dataset_properties"][0])
        self.columnar_spec["external_dataset"][
            "external_dataset_properties"] = list()
        for s3_bucket in self.delta_table_info:
            for delta_table in self.delta_table_info[s3_bucket]:
                dataset_properties["external_container_name"] = s3_bucket
                dataset_properties["path_on_external_container"] = delta_table
                self.columnar_spec["external_dataset"][
                    "external_dataset_properties"].append(dataset_properties)

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        datasets = self.cbas_util.get_all_dataset_objs("external")
        for dataset in datasets:
            self.log.info(f"Performing Vaccum on delta table "
                          f"{dataset.dataset_properties['path_on_external_container']}")
            self.delta_lake_util.perform_vacuum_on_delta_lake_table(
                dataset.dataset_properties["external_container_name"],
                dataset.dataset_properties["path_on_external_container"])

            status, _, errors, results, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.cluster, f"select value ds from {dataset.full_name} as "
                              f"ds")
            if status != "success":
                self.fail(f"Unable to query dataset {dataset.full_name}. "
                          f"Error - {errors}")

            data_in_delta_table = self.delta_lake_util.read_data_from_table(
                dataset.dataset_properties["external_container_name"],
                dataset.dataset_properties["path_on_external_container"])
            diff = DeepDiff(results, data_in_delta_table, ignore_order=True)
            if diff:
                self.fail(
                    f"Data mismatch between Delta lake table "
                    f"{dataset.dataset_properties['path_on_external_container']}"
                    f" and external collection {dataset.full_name}. "
                    f"\nDifferences - {diff}")

    def test_run_query_on_collection_while_performing_vacuum_on_associated_table(
            self):
        self.setup_delta_lake_infra(
            num_s3_bucket=self.input.param("num_s3_bucket", 1),
            num_delta_tables_per_bucket=self.input.param(
                "num_delta_tables_per_bucket", 1),
            load_data=True,
            num_docs=self.input.param("num_docs", 1000),
            doc_size=self.input.param("doc_size", 1024)

        )
        # Update columnar spec based on conf file params
        dataset_properties = deepcopy(self.columnar_spec["external_dataset"][
                                          "external_dataset_properties"][0])
        self.columnar_spec["external_dataset"][
            "external_dataset_properties"] = list()
        for s3_bucket in self.delta_table_info:
            for delta_table in self.delta_table_info[s3_bucket]:
                dataset_properties["external_container_name"] = s3_bucket
                dataset_properties["path_on_external_container"] = delta_table
                self.columnar_spec["external_dataset"][
                    "external_dataset_properties"].append(dataset_properties)

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        datasets = self.cbas_util.get_all_dataset_objs("external")
        for dataset in datasets:
            status, _, errors, results, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.cluster, f"select value ds from {dataset.full_name} as "
                              f"ds")
            if status != "success":
                self.fail(f"Unable to query dataset {dataset.full_name}. "
                          f"Error - {errors}")
            data_in_delta_table = self.delta_lake_util.read_data_from_table(
                dataset.dataset_properties["external_container_name"],
                dataset.dataset_properties["path_on_external_container"])
            diff = DeepDiff(results, data_in_delta_table, ignore_order=True)
            if diff:
                self.fail(
                    f"Data mismatch between Delta lake table "
                    f"{dataset.dataset_properties['path_on_external_container']}"
                    f" and external collection {dataset.full_name}. "
                    f"\nDifferences - {diff}")

        self.log.info("Performing vacuum operation on delta table while "
                      "running query parallely on external collection")

        for dataset in datasets:
            data_in_delta_table = self.delta_lake_util.read_data_from_table(
                dataset.dataset_properties["external_container_name"],
                dataset.dataset_properties["path_on_external_container"])
            with concurrent.futures.ThreadPoolExecutor() as executor:
                # Submit the jobs
                query_job = executor.submit(
                    self.cbas_util.execute_statement_on_cbas_util,
                    self.cluster,
                    f"select value ds from {dataset.full_name} as ds")

                vacuum_job = executor.submit(
                    self.delta_lake_util.perform_vacuum_on_delta_lake_table,
                    dataset.dataset_properties["external_container_name"],
                    dataset.dataset_properties["path_on_external_container"]
                )
                # Collect results
                query_result = query_job.result()
                time.sleep(1)
                vacuum_job.result()

            if query_result[0] == "success":
                diff = DeepDiff(query_result[3], data_in_delta_table,
                                ignore_order=True)
                if diff:
                    self.fail(
                        f"Data mismatch between Delta lake table "
                        f"{dataset.dataset_properties['path_on_external_container']}"
                        f" and external collection {dataset.full_name}. "
                        f"\nDifferences - {diff}")
            else:
                self.fail("Error while running query parallely while "
                          "performing CRUD on associated Delta collection. "
                          "Error - {0}".format(query_result[2]))

    def test_run_query_on_collection_while_deleting_the_associated_table(
            self):
        self.setup_delta_lake_infra(
            num_s3_bucket=self.input.param("num_s3_bucket", 1),
            num_delta_tables_per_bucket=self.input.param(
                "num_delta_tables_per_bucket", 1),
            load_data=True,
            num_docs=self.input.param("num_docs", 1000),
            doc_size=self.input.param("doc_size", 1024)

        )
        # Update columnar spec based on conf file params
        dataset_properties = deepcopy(self.columnar_spec["external_dataset"][
                                          "external_dataset_properties"][0])
        self.columnar_spec["external_dataset"][
            "external_dataset_properties"] = list()
        for s3_bucket in self.delta_table_info:
            for delta_table in self.delta_table_info[s3_bucket]:
                dataset_properties["external_container_name"] = s3_bucket
                dataset_properties["path_on_external_container"] = delta_table
                self.columnar_spec["external_dataset"][
                    "external_dataset_properties"].append(dataset_properties)

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        datasets = self.cbas_util.get_all_dataset_objs("external")
        for dataset in datasets:
            status, _, errors, results, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.cluster, f"select value ds from {dataset.full_name} as "
                              f"ds")
            if status != "success":
                self.fail(f"Unable to query dataset {dataset.full_name}. "
                          f"Error - {errors}")
            data_in_delta_table = self.delta_lake_util.read_data_from_table(
                dataset.dataset_properties["external_container_name"],
                dataset.dataset_properties["path_on_external_container"])
            diff = DeepDiff(results, data_in_delta_table, ignore_order=True)
            if diff:
                self.fail(
                    f"Data mismatch between Delta lake table "
                    f"{dataset.dataset_properties['path_on_external_container']}"
                    f" and external collection {dataset.full_name}. "
                    f"\nDifferences - {diff}")

        self.log.info("Performing vacuum operation on delta table while "
                      "running query parallely on external collection")

        for dataset in datasets:
            data_in_delta_table = self.delta_lake_util.read_data_from_table(
                dataset.dataset_properties["external_container_name"],
                dataset.dataset_properties["path_on_external_container"])
            with concurrent.futures.ThreadPoolExecutor() as executor:
                # Submit the jobs
                query_job = executor.submit(
                    self.cbas_util.execute_statement_on_cbas_util,
                    self.cluster,
                    f"select value ds from {dataset.full_name} as ds")

                delete_table_job = executor.submit(
                    self.s3_obj.delete_folder,
                    dataset.dataset_properties["external_container_name"],
                    dataset.dataset_properties["path_on_external_container"]
                )
                # Collect results
                query_result = query_job.result()
                time.sleep(1)
                delete_table_job.result()

            if query_result[0] == "success":
                diff = DeepDiff(query_result[3], data_in_delta_table,
                                ignore_order=True)
                if diff:
                    self.fail(
                        f"Data mismatch between Delta lake table "
                        f"{dataset.dataset_properties['path_on_external_container']}"
                        f" and external collection {dataset.full_name}. "
                        f"\nDifferences - {diff}")
            else:
                self.fail("Error while running query parallely while "
                          "performing CRUD on associated Delta collection. "
                          "Error - {0}".format(query_result[2]))

    def test_error_condition_for_with_flags(self):
        self.setup_delta_lake_infra()
        # Update columnar spec based on conf file params
        dataset_properties = deepcopy(self.columnar_spec["external_dataset"][
                                          "external_dataset_properties"][0])
        self.columnar_spec["external_dataset"][
            "external_dataset_properties"] = list()
        for s3_bucket in self.delta_table_info:
            for delta_table in self.delta_table_info[s3_bucket]:
                dataset_properties["external_container_name"] = s3_bucket
                dataset_properties["path_on_external_container"] = delta_table
                self.columnar_spec["external_dataset"][
                    "external_dataset_properties"].append(dataset_properties)

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        dataset = self.cbas_util.get_all_dataset_objs("external")[0]
        link = self.cbas_util.get_all_link_objs("s3")[0]

        testcases = [
            {
                "description": "Invalid Value for table-format flag",
                "expected_error": "Invalid value for parameter 'table-format': deltatable",
                "with_flag": "table-format",
                "flag_value": "deltatable"
            },
            {
                "description": "Unsupported with clause flag - Include",
                "with_flag": "include",
                "flag_value": "*.parquet"
            },
            {
                "description": "Unsupported with clause flag - Exclude",
                "with_flag": "exclude",
                "flag_value": "*.json"
            },
            {
                "description": "Unsupported with clause flag - format",
                "expected_error": "Supported file format for 'delta' tables "
                                  "is 'parquet', but 'json' was provided.",
                "with_flag": "format",
                "flag_value": "json"
            },
            {
                "description": "Unsupported with clause flag - redact-warnings",
                "with_flag": "redact-warnings",
                "flag_value": True
            },
            {
                "description": "Unsupported with clause flag - null",
                "with_flag": "null",
                "flag_value": "null"
            },
            {
                "description": "Unsupported with clause flag - header",
                "with_flag": "header",
                "flag_value": True
            },
            {
                "description": "table-format flag missing",
                "missing_with_flag": True,
                "expected_error": "Parameter(s) format or table-format must be specified",
                "with_flag": "table-format",
                "flag_value": "delta"
            },
            {
                "description": "If delta table missing",
                "delta_table_missing": True,
                "expected_error": "External source error. io.delta.kernel.exceptions.TableNotFoundException",
                "with_flag": "table-format",
                "flag_value": "delta"
            },
        ]

        for testcase in testcases:
            self.log.info(f"Executing scenario - {testcase['description']}")
            dataset_name = self.cbas_util.generate_name()
            if testcase.get("delta_table_missing", False):
                delta_table_name = dataset_properties['path_on_external_container']
                dataset_properties['path_on_external_container'] = "dummy"
            ddl = (f"CREATE EXTERNAL DATASET {dataset_name} on "
                   f"`{dataset.dataset_properties['external_container_name']}`"
                   f" at {link.name} path "
                   f"\"{dataset_properties['path_on_external_container']}\" "
                   f"with ")
            with_parameters = {
                testcase["with_flag"]: testcase["flag_value"]
            }
            if "table-format" not in with_parameters:
                with_parameters["table-format"] = "delta"

            if testcase.get("missing_with_flag", False):
                with_parameters = {}

            ddl += json.dumps(with_parameters) + ";"

            status, _, errors, results, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.cluster, ddl)
            if status == "fatal":
                if testcase["expected_error"] not in errors[0]["msg"]:
                    self.fail(
                        f"Expected Error - {testcase['expected_error']}, "
                        f"Actual Error - {errors[0]['msg']}")
            else:
                status, _, errors, results, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                    self.cluster, f"select count(*) from {dataset_name}")
                if status == "fatal":
                    self.fail(f"Error while running query on {dataset_name}. "
                              f"Error - {errors}")
            if testcase.get("delta_table_missing", False):
                 dataset_properties['path_on_external_container'] = delta_table_name

    def test_decimal_to_double_flag(self):
        decimal_to_double = self.input.param("decimal_to_double", True)

        self.setup_delta_lake_infra(
            num_s3_bucket=self.input.param("num_s3_bucket", 1),
            num_delta_tables_per_bucket=self.input.param(
                "num_delta_tables_per_bucket", 1),
            load_data=True,
            num_docs=self.input.param("num_docs", 1000),
            doc_size=self.input.param("doc_size", 1024),
            include_decimal=True
        )
        # Update columnar spec based on conf file params
        dataset_properties = deepcopy(self.columnar_spec["external_dataset"][
                                          "external_dataset_properties"][0])
        self.columnar_spec["external_dataset"][
            "external_dataset_properties"] = list()
        for s3_bucket in self.delta_table_info:
            for delta_table in self.delta_table_info[s3_bucket]:
                dataset_properties["external_container_name"] = s3_bucket
                dataset_properties["path_on_external_container"] = delta_table
                if decimal_to_double:
                    dataset_properties["convert_decimal_to_double"] = 1
                else:
                    dataset_properties["convert_decimal_to_double"] = 2
                self.columnar_spec["external_dataset"][
                    "external_dataset_properties"].append(dataset_properties)

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        datasets = self.cbas_util.get_all_dataset_objs("external")
        for dataset in datasets:
            status, _, errors, results, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.cluster, f"select value ds from {dataset.full_name} as "
                              f"ds")
            if status == "success":
                data_in_delta_table = self.delta_lake_util.read_data_from_table(
                    dataset.dataset_properties["external_container_name"],
                    dataset.dataset_properties["path_on_external_container"])
                diff = DeepDiff(results, data_in_delta_table, ignore_order=True)
                if diff:
                    self.fail(
                        f"Data mismatch between Delta lake table "
                        f"{dataset.dataset_properties['path_on_external_container']}"
                        f" and external collection {dataset.full_name}. "
                        f"\nDifferences - {diff}")
            else:
                if not decimal_to_double:
                    if errors[0]["code"] != 24167:
                        self.fail(f"Error code mismatch. Expected - {24167},"
                                  f"Actual - {errors['code']}")
                else:
                    self.fail(f"Unable to query dataset {dataset.full_name}. "
                              f"Error - {errors}")

    def test_timestamp_to_long_flag(self):
        timestamp_to_long = self.input.param("timestamp_to_long", True)

        self.setup_delta_lake_infra(
            num_s3_bucket=self.input.param("num_s3_bucket", 1),
            num_delta_tables_per_bucket=self.input.param(
                "num_delta_tables_per_bucket", 1),
            load_data=True,
            num_docs=self.input.param("num_docs", 1000),
            doc_size=self.input.param("doc_size", 1024),
            include_date_time_fields=True
        )
        # Update columnar spec based on conf file params
        dataset_properties = deepcopy(self.columnar_spec["external_dataset"][
                                          "external_dataset_properties"][0])
        self.columnar_spec["external_dataset"][
            "external_dataset_properties"] = list()
        for s3_bucket in self.delta_table_info:
            for delta_table in self.delta_table_info[s3_bucket]:
                dataset_properties["external_container_name"] = s3_bucket
                dataset_properties["path_on_external_container"] = delta_table
                if timestamp_to_long:
                    dataset_properties["timestamp_to_long"] = 1
                else:
                    dataset_properties["timestamp_to_long"] = 2
                self.columnar_spec["external_dataset"][
                    "external_dataset_properties"].append(dataset_properties)

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        datasets = self.cbas_util.get_all_dataset_objs("external")
        if timestamp_to_long:
            query = ("select value ds from {0} as ds where "
                     "datetime_ISO!=datetime_in_epoch")
        else:
            query = ("select value ds from {0} as ds where "
                     "STR_TO_MILLIS(to_string("
                     "ds.datetime_ISO))!=ds.datetime_in_epoch")
        for dataset in datasets:
            status, _, errors, results, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.cluster, query.format(dataset.full_name))
            if status == "success":
                if len(results) > 0:
                    self.fail(
                        f"Timestamp to long conversion returned wrong results "
                        f"for following docs -{results}")
            else:
                self.fail(f"Unable to query dataset {dataset.full_name}. "
                          f"Error - {errors}")

    def test_date_to_int_flag(self):
        date_to_int = self.input.param("date_to_int", True)

        self.setup_delta_lake_infra(
            num_s3_bucket=self.input.param("num_s3_bucket", 1),
            num_delta_tables_per_bucket=self.input.param(
                "num_delta_tables_per_bucket", 1),
            load_data=True,
            num_docs=self.input.param("num_docs", 1000),
            doc_size=self.input.param("doc_size", 1024),
            include_date_time_fields=True
        )
        # Update columnar spec based on conf file params
        dataset_properties = deepcopy(self.columnar_spec["external_dataset"][
                                          "external_dataset_properties"][0])
        self.columnar_spec["external_dataset"][
            "external_dataset_properties"] = list()
        for s3_bucket in self.delta_table_info:
            for delta_table in self.delta_table_info[s3_bucket]:
                dataset_properties["external_container_name"] = s3_bucket
                dataset_properties["path_on_external_container"] = delta_table
                if date_to_int:
                    dataset_properties["date_to_int"] = 1
                else:
                    dataset_properties["date_to_int"] = 2
                self.columnar_spec["external_dataset"][
                    "external_dataset_properties"].append(dataset_properties)

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.columnar_spec, self.bucket_util, False)
        if not result:
            self.fail(msg)

        datasets = self.cbas_util.get_all_dataset_objs("external")
        if date_to_int:
            query = ("select value ds from {0} as ds where "
                     "date!=days_since_epoch")
        else:
            query = ("select value ds from {0} as ds where "
                     "FLOOR(STR_TO_MILLIS(to_string("
                     "ds.date)) / 86400000) !=ds.days_since_epoch")
        for dataset in datasets:
            status, _, errors, results, _, _ = self.cbas_util.execute_statement_on_cbas_util(
                self.cluster, query.format(dataset.full_name))
            if status == "success":
                if len(results) > 0:
                    self.fail(
                        f"Date to int conversion returned wrong results "
                        f"for following docs -{results}")
            else:
                self.fail(f"Unable to query dataset {dataset.full_name}. "
                          f"Error - {errors}")