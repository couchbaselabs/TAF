"""
Created on 04-Jan-2026

@author: himanshu.jain@couchbase.com
"""


from Columnar.onprem.columnar_onprem_base import ColumnarOnPremBase as ColumnarBaseTest
from queue import Queue
from icebergLib.iceberg_base import IcebergBase
from icebergLib.iceberg_util import IcebergUtil
from pyspark.sql.types import *

import os
import json
import datetime
import random
import shutil


class IcebergTest(ColumnarBaseTest):
    def setUp(self):
        super(IcebergTest, self).setUp()

        gcs_certificate = os.getenv('gcp_access_file')
        self.log.info(f"GCS credentials file: {gcs_certificate}")

        self.catalog_type = self.input.param("catalog_type", "None")
        self.initial_doc_count = self.input.param("initial_doc_count", 1000)

        self.partitioned_field = self.input.param("partitioned_field", '')
        self.partitioned_field = self.partitioned_field.split(
            ",") if self.partitioned_field else []  # [], [type], [type, free_breakfast]

        self.filter_by = self.input.param("filter_by", None)
        if self.filter_by:
            self.filter_by = self.filter_by.replace("$NE$", "!=")

        self.num_snapshots = int(self.input.param("num_snapshots", 0))
        self.create_external_collection_with_sanpshotId = bool(
            self.input.param("create_external_collection_with_sanpshotId", False))
        self.create_external_collection_with_sanpshotTimestamp = bool(
            self.input.param("create_external_collection_with_sanpshotTimestamp", False))

        self.sigv4SigningName = self.input.param("sigv4SigningName", None)
        self.iceberg_base = IcebergBase(
            aws_access_key=self.aws_access_key,
            aws_secret_key=self.aws_secret_key,
            aws_session_token=self.aws_session_token,
            gcs_credentials=gcs_certificate,
            catalog_type=self.catalog_type,
        )

        self.s3link_name = self.cbas_util.generate_name(name_cardinality=1)
        self.gcslink_name = self.cbas_util.generate_name(name_cardinality=1)
        self.catalog_name = self.cbas_util.generate_name(name_cardinality=1)
        self.external_collection_name = self.cbas_util.generate_name(
            name_cardinality=1)
        self.external_collection_with_snapshot_name = self.cbas_util.generate_name(
            name_cardinality=1)

        if not self.columnar_spec_name:
            self.columnar_spec_name = "full_template"

        self.log_setup_status(
            self.__class__.__name__, "Finished", stage=self.setUp.__name__
        )

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)

        # delete external collection
        self.cbas_util.drop_dataset(
            self.analytics_cluster, self.external_collection_name, if_exists=True)
        self.log.info(
            f"Successfully deleted external collection: {self.external_collection_name}")

        self.cbas_util.drop_dataset(
            self.analytics_cluster, self.external_collection_with_snapshot_name, if_exists=True)
        self.log.info(
            f"Successfully deleted external collection: {self.external_collection_with_snapshot_name}")

        # delete catalog
        self.cbas_util.drop_catalog(
            self.analytics_cluster, self.catalog_name, if_exists=True)
        self.log.info(f"Successfully deleted catalog: {self.catalog_name}")

        # delete s3 link
        self.cbas_util.drop_link(
            self.analytics_cluster, self.s3link_name, if_exists=True)
        self.log.info(f"Successfully deleted s3 link: {self.s3link_name}")

        # delete gcs link
        self.cbas_util.drop_link(
            self.analytics_cluster, self.gcslink_name, if_exists=True)
        self.log.info(f"Successfully deleted gcs link: {self.gcslink_name}")

        super(IcebergTest, self).tearDown()
        self.log_setup_status(self.__class__.__name__,
                              "Finished", stage="Teardown")

    def create_load_documents_standalone_collection(self):
        database_name = self.input.param("database", "Default")
        dataverse_name = self.input.param("dataverse", "Default")
        no_of_collection = self.input.param("num_standalone_collections", 1)
        validate_error = self.input.param("validate_error", False)
        error_message = str(self.input.param("error_message", None))

        # Create standalone collections
        jobs = Queue()
        for _ in range(no_of_collection):
            dataset_obj = self.cbas_util.create_standalone_dataset_obj(
                self.columnar_cluster,
                database_name=database_name,
                dataverse_name=dataverse_name
            )
            dataset = dataset_obj[0]
            jobs.put((self.cbas_util.create_standalone_collection, {
                "cluster": self.columnar_cluster,
                "collection_name": dataset.name,
                "dataverse_name": dataset.dataverse_name,
                "database_name": dataset.database_name,
                "validate_error_msg": validate_error,
                "expected_error": error_message
            }))

        results = []
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )
        if not all(results):
            self.fail("Failed to create some collection")
        if validate_error:
            return

        # Load documents into standalone collections
        jobs = Queue()
        for dataset in self.cbas_util.get_all_dataset_objs():
            jobs.put((self.cbas_util.load_doc_to_standalone_collection, {
                "cluster": self.columnar_cluster,
                "collection_name": dataset.name,
                "dataverse_name": dataset.dataverse_name,
                "database_name": dataset.database_name,
                "no_of_docs": self.initial_doc_count,
                "document_size": self.doc_size
            }))

        results = []
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False
        )
        if not all(results):
            self.fail("Failed to load data into standalone collection")

    def skip_if_already_exists(self, name, resource):
        """
        Return True if the resource does NOT exist (caller should try to create).
        Return False if the resource already exists (caller should skip creation).
        Uses Metadata to check for existing link or catalog.
        """
        if resource == "link":
            query = f"select value ds.Name from Metadata.`Link` as ds where Name='{name}';"
            status, _, _, results, _, _ = (
                self.cbas_util.execute_statement_on_cbas_util(
                    self.analytics_cluster, query))
            if status == "success" and results and name in results:
                self.log.info(
                    f"Link {name} already exists, skipping creation")
                return False
        if resource == "catalog":
            query = f"select value ds.CatalogName from Metadata.`Catalog` as ds where CatalogName='{name}';"
            status, _, _, results, _, _ = (
                self.cbas_util.execute_statement_on_cbas_util(
                    self.analytics_cluster, query))
            if status == "success" and results and name in results:
                self.log.info(
                    f"Catalog {name} already exists, skipping creation")
                return False
        return True

    def _setup_aws_glue_in_analytics(self, external_collection_name, snapshotId, snapshotTimestamp):
        """Setup for AWS Glue catalog: S3 link + Glue catalog + external collection."""
        if self.skip_if_already_exists(self.s3link_name, "link") and not self.cbas_util.create_external_link(self.analytics_cluster, link_properties={
            "name": self.s3link_name,
            "type": "s3",
            "accessKeyId": self.iceberg_base.aws_access_key,
            "secretAccessKey": self.iceberg_base.aws_secret_key,
            "sessionToken": self.iceberg_base.aws_session_token,
            "region": self.iceberg_base.iceberg_region
        }):
            self.fail("Failed to create S3 link")
        self.log.info(f"Successfully created s3 link: {self.s3link_name}")

        # full_warehouse_path = f"{self.iceberg_base.s3_warehouse_path}/{self.iceberg_base.database_name}.db/{self.iceberg_base.table_name}"
        if self.skip_if_already_exists(self.catalog_name, "catalog") and not self.cbas_util.create_catalog(
                self.analytics_cluster,
                catalog_name=self.catalog_name,
                catalog_type="Iceberg",
                catalog_source="AWS_GLUE",
                link_name=self.s3link_name):
            self.fail("Failed to create Iceberg catalog")
        self.log.info(
            f"Successfully created Iceberg catalog: {self.catalog_name}")

        if not self.cbas_util.create_dataset_on_external_resource(
                self.analytics_cluster,
                external_collection_name,
                self.catalog_name,
                self.s3link_name,
                table_format="iceberg",
                namespace=self.iceberg_base.database_name,
                table_name=self.iceberg_base.table_name,
                snapshotId=snapshotId,
                snapshotTimestamp=snapshotTimestamp,
                convert_decimal_to_double=1):
            self.fail("Failed to create Iceberg external collection")
        self.log.info(
            f"Successfully created Iceberg external collection: {external_collection_name}")

    def _setup_aws_glue_rest_in_analytics(self, external_collection_name, snapshotId, snapshotTimestamp):
        """Setup for AWS Glue REST catalog: S3 link + Glue REST catalog + external collection."""
        if self.skip_if_already_exists(self.s3link_name, "link") and not self.cbas_util.create_external_link(self.analytics_cluster, link_properties={
            "name": self.s3link_name,
            "type": "s3",
            "accessKeyId": self.iceberg_base.aws_access_key,
            "secretAccessKey": self.iceberg_base.aws_secret_key,
            "sessionToken": self.iceberg_base.aws_session_token,
            "region": self.iceberg_base.iceberg_region
        }):
            self.fail("Failed to create S3 link")
        self.log.info(f"Successfully created s3 link: {self.s3link_name}")

        if self.skip_if_already_exists(self.catalog_name, "catalog") and not self.cbas_util.create_catalog(
                self.analytics_cluster,
                catalog_name=self.catalog_name,
                catalog_type="Iceberg",
                catalog_source="AWS_GLUE_REST",
                link_name=self.s3link_name,
                uri=f"https://glue.{self.iceberg_base.iceberg_region}.amazonaws.com/iceberg",
                sigv4SigningRegion=self.iceberg_base.iceberg_region):
            self.fail("Failed to create Iceberg catalog")
        self.log.info(
            f"Successfully created Iceberg catalog: {self.catalog_name}")

        if not self.cbas_util.create_dataset_on_external_resource(
                self.analytics_cluster,
                external_collection_name,
                self.catalog_name,
                self.s3link_name,
                table_format="iceberg",
                namespace=self.iceberg_base.database_name,
                table_name=self.iceberg_base.table_name,
                snapshotId=snapshotId,
                snapshotTimestamp=snapshotTimestamp,
                convert_decimal_to_double=1):
            self.fail("Failed to create Iceberg external collection")
        self.log.info(
            f"Successfully created Iceberg external collection: {external_collection_name}")

    def _setup_s3_tables_in_analytics(self, sigv4SigningName=None, external_collection_name=None, snapshotId=None, snapshotTimestamp=None):
        """Setup for S3 Tables catalog: S3 link + S3 Tables catalog + external collection."""
        if self.skip_if_already_exists(self.s3link_name, "link") and not self.cbas_util.create_external_link(self.analytics_cluster, link_properties={
            "name": self.s3link_name,
            "type": "s3",
            "accessKeyId": self.iceberg_base.aws_access_key,
            "secretAccessKey": self.iceberg_base.aws_secret_key,
            "sessionToken": self.iceberg_base.aws_session_token,
            "region": self.iceberg_base.iceberg_region
        }):
            self.fail("Failed to create S3 link")
        self.log.info(f"Successfully created s3 link: {self.s3link_name}")

        if sigv4SigningName == "s3tables":
            warehouse = self.iceberg_base.s3_table_bucket_arn
        elif sigv4SigningName == "glue":
            warehouse = f"{self.iceberg_base.aws_account_id}:s3tablescatalog/{self.iceberg_base.iceberg_bucket}"

        if self.skip_if_already_exists(self.catalog_name, "catalog") and not self.cbas_util.create_catalog(
                self.analytics_cluster,
                catalog_name=self.catalog_name,
                catalog_type="Iceberg",
                catalog_source="S3_TABLES",
                link_name=self.s3link_name,
                uri=f"https://{sigv4SigningName}.{self.iceberg_base.iceberg_region}.amazonaws.com/iceberg",
                warehouse_path=warehouse,
                sigv4SigningName=sigv4SigningName,
                sigv4SigningRegion=self.iceberg_base.iceberg_region):
            self.fail("Failed to create Iceberg catalog")
        self.log.info(
            f"Successfully created Iceberg catalog: {self.catalog_name}")

        if not self.cbas_util.create_dataset_on_external_resource(
                self.analytics_cluster,
                external_collection_name,
                self.catalog_name,
                self.s3link_name,
                table_format="iceberg",
                namespace=self.iceberg_base.database_name,
                snapshotId=snapshotId,
                snapshotTimestamp=snapshotTimestamp,
                table_name=self.iceberg_base.table_name):
            self.fail("Failed to create Iceberg external collection")
        self.log.info(
            f"Successfully created Iceberg external collection: {external_collection_name}")

    def _setup_biglake_metastore_in_analytics(self, external_collection_name, snapshotId, snapshotTimestamp):
        """Setup for BigLake Metastore catalog: GCS link + BigLake catalog + external collection."""
        if self.skip_if_already_exists(self.gcslink_name, "link") and not self.cbas_util.create_external_link(self.analytics_cluster, link_properties={
            "name": self.gcslink_name,
            "type": "gcs",
            "jsonCredentials": json.load(open(self.iceberg_base.gcs_credentials))
        }):
            self.fail("Failed to create GCS link")
        self.log.info(f"Successfully created gcs link: {self.gcslink_name}")

        if self.skip_if_already_exists(self.catalog_name, "catalog") and not self.cbas_util.create_catalog(
                self.analytics_cluster,
                catalog_name=self.catalog_name,
                catalog_type="Iceberg",
                catalog_source="BIGLAKE_METASTORE",
                link_name=self.gcslink_name,
                warehouse_path=self.iceberg_base.gcs_bucket_path,
                uri="https://biglake.googleapis.com/iceberg/v1/restcatalog",
                quotaProjectId=self.iceberg_base.gcs_project_id):
            self.fail("Failed to create Iceberg catalog")
        self.log.info(
            f"Successfully created Iceberg catalog: {self.catalog_name}")

        if not self.cbas_util.create_dataset_on_external_resource(
                self.analytics_cluster,
                external_collection_name,
                self.catalog_name,
                self.gcslink_name,
                table_format="iceberg",
                namespace=self.iceberg_base.database_name,
                snapshotId=snapshotId,
                snapshotTimestamp=snapshotTimestamp,
                table_name=self.iceberg_base.table_name):
            self.fail("Failed to create Iceberg external collection")
        self.log.info(
            f"Successfully created Iceberg external collection: {external_collection_name}")

    def base_setup(self, catalog_type=None, sigv4SigningName=None, external_collection_name=None, snapshotId=None, snapshotTimestamp=None):
        if catalog_type == "AWS_GLUE":
            self._setup_aws_glue_in_analytics(
                external_collection_name=external_collection_name, snapshotId=snapshotId, snapshotTimestamp=snapshotTimestamp)
        elif catalog_type == "AWS_GLUE_REST":
            self._setup_aws_glue_rest_in_analytics(
                external_collection_name=external_collection_name, snapshotId=snapshotId, snapshotTimestamp=snapshotTimestamp)
        elif catalog_type == "S3_TABLES":
            self._setup_s3_tables_in_analytics(
                sigv4SigningName=sigv4SigningName, external_collection_name=external_collection_name, snapshotId=snapshotId, snapshotTimestamp=snapshotTimestamp)
        elif catalog_type == "BIGLAKE_METASTORE":
            self._setup_biglake_metastore_in_analytics(
                external_collection_name=external_collection_name, snapshotId=snapshotId, snapshotTimestamp=snapshotTimestamp)
        else:
            raise ValueError(
                f"Unsupported catalog_type: {catalog_type}. "
                "Supported: AWS_GLUE, BIGLAKE_METASTORE, AWS_GLUE_REST, S3_TABLES")

    def test_glue_catalog(self):
        iceberg_util = IcebergUtil(
            state=self.iceberg_base
        )
        try:
            # Generate data
            data = [self.cbas_util.generate_docs(document_size=self.doc_size)
                    for _ in range(self.initial_doc_count)]

            # setup glue catalog with the data
            iceberg_util.setup_glue_catalog(
                data, partitioned_field=self.partitioned_field, filter_by=self.filter_by)

            # create iceberg base setup
            self.base_setup(catalog_type=self.catalog_type,
                            external_collection_name=self.external_collection_name)

            # Query the EXTERNAL COLLECTION
            cmd = f"SELECT count(*) FROM {self.external_collection_name}"
            expected_count = self.initial_doc_count
            if self.filter_by:
                cmd += f" where not {self.filter_by}"
                expected_count = 0

            status, metrics, errors, results, handle, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.analytics_cluster,
                cmd)
            if status != "success":
                self.fail("Failed to query Iceberg external collection")
            self.log.info(f"Query results: {results}")
            count = results[0]['$1']
            self.assertEqual(count, expected_count,
                             f"Count {count} does not equal expected_count {expected_count}")
            self.log.info(
                f"Successfully queried data from {self.catalog_type} Catalog")

            if self.num_snapshots:
                for snapshot_num in range(1, self.num_snapshots + 1):
                    iceberg_util.spark.sql(f"""
                        UPDATE {self.iceberg_base.catalog_type}.{self.iceberg_base.database_name}.{self.iceberg_base.table_name}
                        SET price = price + 10000
                    """)

                    count = iceberg_util.spark.table(
                        f"{self.iceberg_base.catalog_type}.{self.iceberg_base.database_name}.{self.iceberg_base.table_name}").count()
                    self.log.info(
                        f"Snapshot {snapshot_num} created. Total rows: {count}\n")

                    # Query the EXTERNAL COLLECTION
                    status, metrics, errors, results, handle, warnings = self.cbas_util.execute_statement_on_cbas_util(
                        self.analytics_cluster,
                        f"SELECT count(*) FROM {self.external_collection_name}")
                    if status != "success":
                        self.fail(
                            "Failed to query Iceberg external collection")

                    self.log.info(f"Query results: {results}")
                    count = results[0]['$1']
                    self.assertEqual(count, self.initial_doc_count,
                                     f"Count {count} does not equal initial_doc_count {self.initial_doc_count}")
                    self.log.info(
                        f"Successfully queried data from {self.catalog_type} Catalog")

                # List all snapshots
                self.log.info("\n=== Snapshot history ===")
                try:
                    snapshots_df = iceberg_util.spark.sql(
                        f"SELECT * FROM {self.iceberg_base.catalog_type}.{self.iceberg_base.database_name}.{self.iceberg_base.table_name}.history")
                    snapshots_df.show(truncate=False)
                except Exception as e:
                    self.log.error(f"Error querying history table: {e}")

                # Create external collection with snapshot
                snapshot_list = [
                    {
                        "id": str(row.snapshot_id),
                        "timestamp_ms": str(int(
                            row.made_current_at.timestamp() * 1000
                            if hasattr(row.made_current_at, "timestamp") else
                            datetime.datetime.fromisoformat(
                                row.made_current_at).timestamp() * 1000
                        ))
                    }
                    for row in snapshots_df.collect()
                ]
                randomSnapshotId, randomSnapshotTimestamp = random.choice(
                    snapshot_list).values()

                if self.create_external_collection_with_sanpshotId:
                    self.base_setup(catalog_type=self.catalog_type,
                                    external_collection_name=self.external_collection_with_snapshot_name,
                                    snapshotId=randomSnapshotId)

                    # Query the EXTERNAL COLLECTION
                    status, metrics, errors, results, handle, warnings = self.cbas_util.execute_statement_on_cbas_util(
                        self.analytics_cluster,
                        f"SELECT count(*) FROM {self.external_collection_with_snapshot_name}")
                    if status != "success":
                        self.fail(
                            "Failed to query Iceberg external collection")

                    self.log.info(f"Query results: {results}")
                    count = results[0]['$1']
                    self.assertEqual(count, self.initial_doc_count,
                                     f"Count {count} does not equal initial_doc_count {self.initial_doc_count}")
                    self.log.info(
                        f"Successfully queried data from snapshot {randomSnapshotId} in {self.catalog_type} Catalog")

                if self.create_external_collection_with_sanpshotTimestamp:
                    self.base_setup(catalog_type=self.catalog_type,
                                    external_collection_name=self.external_collection_with_snapshot_name,
                                    snapshotTimestamp=randomSnapshotTimestamp)

                    # Query the EXTERNAL COLLECTION
                    status, metrics, errors, results, handle, warnings = self.cbas_util.execute_statement_on_cbas_util(
                        self.analytics_cluster,
                        f"SELECT count(*) FROM {self.external_collection_with_snapshot_name}")
                    if status != "success":
                        self.fail(
                            "Failed to query Iceberg external collection")

                    self.log.info(f"Query results: {results}")
                    count = results[0]['$1']
                    self.assertEqual(count, self.initial_doc_count,
                                     f"Count {count} does not equal initial_doc_count {self.initial_doc_count}")
                    self.log.info(
                        f"Successfully queried data from snapshot {randomSnapshotTimestamp} in {self.catalog_type} Catalog")

        except Exception as e:
            self.log.error(f"Error while testing iceberg: {str(e)}")
            raise e
        finally:
            # Destroy Glue catalog
            self.log.info(f"Destroying {self.catalog_type} catalog")
            iceberg_util.destroy_glue_catalog()

            # Stop spark session
            self.log.info("Stopping spark session")
            iceberg_util.spark.stop()
            if os.path.exists(iceberg_util.state.sparkPath):
                self.log.info("Removing Spark local directory")
                shutil.rmtree(iceberg_util.state.sparkPath)

    def test_glue_rest_catalog(self):
        iceberg_util = IcebergUtil(
            state=self.iceberg_base
        )
        try:
            # Generate data
            data = [self.cbas_util.generate_docs(document_size=self.doc_size)
                    for _ in range(self.initial_doc_count)]

            # setup glue catalog with the data
            iceberg_util.setup_glue_catalog(
                data, partitioned_field=self.partitioned_field, filter_by=self.filter_by)

            # create iceberg base setup
            self.base_setup(catalog_type=self.catalog_type,
                            external_collection_name=self.external_collection_name)

            # Query the EXTERNAL COLLECTION
            cmd = f"SELECT count(*) FROM {self.external_collection_name}"
            expected_count = self.initial_doc_count
            if self.filter_by:
                cmd += f" where not {self.filter_by}"
                expected_count = 0

            status, metrics, errors, results, handle, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.analytics_cluster,
                cmd)
            if status != "success":
                self.fail("Failed to query Iceberg external collection")
            self.log.info(f"Query results: {results}")
            count = results[0]['$1']
            self.assertEqual(count, expected_count,
                             f"Count {count} does not equal expected_count {expected_count}")
            self.log.info(
                f"Successfully queried data from {self.catalog_type} Catalog")

            if self.num_snapshots:
                for snapshot_num in range(1, self.num_snapshots + 1):
                    iceberg_util.spark.sql(f"""
                        UPDATE {self.iceberg_base.catalog_type}.{self.iceberg_base.database_name}.{self.iceberg_base.table_name}
                        SET price = price + 10000
                    """)

                    count = iceberg_util.spark.table(
                        f"{self.iceberg_base.catalog_type}.{self.iceberg_base.database_name}.{self.iceberg_base.table_name}").count()
                    self.log.info(
                        f"Snapshot {snapshot_num} created. Total rows: {count}\n")

                    # Query the EXTERNAL COLLECTION
                    status, metrics, errors, results, handle, warnings = self.cbas_util.execute_statement_on_cbas_util(
                        self.analytics_cluster,
                        f"SELECT count(*) FROM {self.external_collection_name}")
                    if status != "success":
                        self.fail(
                            "Failed to query Iceberg external collection")

                    self.log.info(f"Query results: {results}")
                    count = results[0]['$1']
                    self.assertEqual(count, self.initial_doc_count,
                                     f"Count {count} does not equal initial_doc_count {self.initial_doc_count}")
                    self.log.info(
                        f"Successfully queried data from {self.catalog_type} Catalog")

                # List all snapshots
                self.log.info("\n=== Snapshot history ===")
                try:
                    snapshots_df = iceberg_util.spark.sql(
                        f"SELECT * FROM {self.iceberg_base.catalog_type}.{self.iceberg_base.database_name}.{self.iceberg_base.table_name}.history")
                    snapshots_df.show(truncate=False)
                except Exception as e:
                    self.log.error(f"Error querying history table: {e}")

                # Create external collection with snapshot
                snapshot_list = [
                    {
                        "id": str(row.snapshot_id),
                        "timestamp_ms": str(int(
                            row.made_current_at.timestamp() * 1000
                            if hasattr(row.made_current_at, "timestamp") else
                            datetime.datetime.fromisoformat(
                                row.made_current_at).timestamp() * 1000
                        ))
                    }
                    for row in snapshots_df.collect()
                ]
                randomSnapshotId, randomSnapshotTimestamp = random.choice(
                    snapshot_list).values()

                if self.create_external_collection_with_sanpshotId:
                    self.base_setup(catalog_type=self.catalog_type,
                                    external_collection_name=self.external_collection_with_snapshot_name,
                                    snapshotId=randomSnapshotId)

                    # Query the EXTERNAL COLLECTION
                    status, metrics, errors, results, handle, warnings = self.cbas_util.execute_statement_on_cbas_util(
                        self.analytics_cluster,
                        f"SELECT count(*) FROM {self.external_collection_with_snapshot_name}")
                    if status != "success":
                        self.fail(
                            "Failed to query Iceberg external collection")

                    self.log.info(f"Query results: {results}")
                    count = results[0]['$1']
                    self.assertEqual(count, self.initial_doc_count,
                                     f"Count {count} does not equal initial_doc_count {self.initial_doc_count}")
                    self.log.info(
                        f"Successfully queried data from snapshot {randomSnapshotId} in {self.catalog_type} Catalog")

                if self.create_external_collection_with_sanpshotTimestamp:
                    self.base_setup(catalog_type=self.catalog_type,
                                    external_collection_name=self.external_collection_with_snapshot_name,
                                    snapshotTimestamp=randomSnapshotTimestamp)

                    # Query the EXTERNAL COLLECTION
                    status, metrics, errors, results, handle, warnings = self.cbas_util.execute_statement_on_cbas_util(
                        self.analytics_cluster,
                        f"SELECT count(*) FROM {self.external_collection_with_snapshot_name}")
                    if status != "success":
                        self.fail(
                            "Failed to query Iceberg external collection")

                    self.log.info(f"Query results: {results}")
                    count = results[0]['$1']
                    self.assertEqual(count, self.initial_doc_count,
                                     f"Count {count} does not equal initial_doc_count {self.initial_doc_count}")
                    self.log.info(
                        f"Successfully queried data from snapshot {randomSnapshotTimestamp} in {self.catalog_type} Catalog")

        except Exception as e:
            self.log.error(f"Error while testing iceberg: {str(e)}")
            raise e
        finally:
            # Destroy Glue catalog
            self.log.info(f"Destroying {self.catalog_type} catalog")
            iceberg_util.destroy_glue_catalog()

            # Stop spark session
            self.log.info("Stopping spark session")
            iceberg_util.spark.stop()
            if os.path.exists(iceberg_util.state.sparkPath):
                self.log.info("Removing Spark local directory")
                shutil.rmtree(iceberg_util.state.sparkPath)

    def test_s3_tables_catalog(self):
        iceberg_util = IcebergUtil(
            state=self.iceberg_base
        )
        try:
            # Generate data
            data = [self.cbas_util.generate_docs(document_size=self.doc_size)
                    for _ in range(self.initial_doc_count)]

            # setup s3 tables catalog with the data
            iceberg_util.setup_s3_tables_catalog(
                data, partitioned_field=self.partitioned_field, filter_by=self.filter_by)

            # create iceberg base setup
            self.base_setup(catalog_type=self.catalog_type,
                            sigv4SigningName=self.sigv4SigningName, external_collection_name=self.external_collection_name)

            # Query the EXTERNAL COLLECTION
            cmd = f"SELECT count(*) FROM {self.external_collection_name}"
            expected_count = self.initial_doc_count
            if self.filter_by:
                cmd += f" where not {self.filter_by}"
                expected_count = 0

            status, metrics, errors, results, handle, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.analytics_cluster,
                cmd)
            if status != "success":
                self.fail("Failed to query Iceberg external collection")
            self.log.info(f"Query results: {results}")
            count = results[0]['$1']
            self.assertEqual(count, expected_count,
                             f"Count {count} does not equal expected_count {expected_count}")
            self.log.info(
                f"Successfully queried data from {self.catalog_type} Catalog")

            if self.num_snapshots:
                for snapshot_num in range(1, self.num_snapshots + 1):
                    iceberg_util.spark.sql(f"""
                        UPDATE {self.iceberg_base.catalog_type}.{self.iceberg_base.database_name}.{self.iceberg_base.table_name}
                        SET price = price + 10000
                    """)

                    count = iceberg_util.spark.table(
                        f"{self.iceberg_base.catalog_type}.{self.iceberg_base.database_name}.{self.iceberg_base.table_name}").count()
                    self.log.info(
                        f"Snapshot {snapshot_num} created. Total rows: {count}\n")

                    # Query the EXTERNAL COLLECTION
                    status, metrics, errors, results, handle, warnings = self.cbas_util.execute_statement_on_cbas_util(
                        self.analytics_cluster,
                        f"SELECT count(*) FROM {self.external_collection_name}")
                    if status != "success":
                        self.fail(
                            "Failed to query Iceberg external collection")

                    self.log.info(f"Query results: {results}")
                    count = results[0]['$1']
                    self.assertEqual(count, self.initial_doc_count,
                                     f"Count {count} does not equal initial_doc_count {self.initial_doc_count}")
                    self.log.info(
                        f"Successfully queried data from {self.catalog_type} Catalog")

                # List all snapshots
                self.log.info("\n=== Snapshot history ===")
                try:
                    snapshots_df = iceberg_util.spark.sql(
                        f"SELECT * FROM {self.iceberg_base.catalog_type}.{self.iceberg_base.database_name}.{self.iceberg_base.table_name}.history")
                    snapshots_df.show(truncate=False)
                except Exception as e:
                    self.log.error(f"Error querying history table: {e}")

                # Create external collection with snapshot
                snapshot_list = [
                    {
                        "id": str(row.snapshot_id),
                        "timestamp_ms": str(int(
                            row.made_current_at.timestamp() * 1000
                            if hasattr(row.made_current_at, "timestamp") else
                            datetime.datetime.fromisoformat(
                                row.made_current_at).timestamp() * 1000
                        ))
                    }
                    for row in snapshots_df.collect()
                ]
                randomSnapshotId, randomSnapshotTimestamp = random.choice(
                    snapshot_list).values()

                if self.create_external_collection_with_sanpshotId:
                    self.base_setup(catalog_type=self.catalog_type,
                                    external_collection_name=self.external_collection_with_snapshot_name,
                                    snapshotId=randomSnapshotId)

                    # Query the EXTERNAL COLLECTION
                    status, metrics, errors, results, handle, warnings = self.cbas_util.execute_statement_on_cbas_util(
                        self.analytics_cluster,
                        f"SELECT count(*) FROM {self.external_collection_with_snapshot_name}")
                    if status != "success":
                        self.fail(
                            "Failed to query Iceberg external collection")

                    self.log.info(f"Query results: {results}")
                    count = results[0]['$1']
                    self.assertEqual(count, self.initial_doc_count,
                                     f"Count {count} does not equal initial_doc_count {self.initial_doc_count}")
                    self.log.info(
                        f"Successfully queried data from snapshot {randomSnapshotId} in {self.catalog_type} Catalog")

                if self.create_external_collection_with_sanpshotTimestamp:
                    self.base_setup(catalog_type=self.catalog_type,
                                    external_collection_name=self.external_collection_with_snapshot_name,
                                    snapshotTimestamp=randomSnapshotTimestamp)

                    # Query the EXTERNAL COLLECTION
                    status, metrics, errors, results, handle, warnings = self.cbas_util.execute_statement_on_cbas_util(
                        self.analytics_cluster,
                        f"SELECT count(*) FROM {self.external_collection_with_snapshot_name}")
                    if status != "success":
                        self.fail(
                            "Failed to query Iceberg external collection")

                    self.log.info(f"Query results: {results}")
                    count = results[0]['$1']
                    self.assertEqual(count, self.initial_doc_count,
                                     f"Count {count} does not equal initial_doc_count {self.initial_doc_count}")
                    self.log.info(
                        f"Successfully queried data from snapshot {randomSnapshotTimestamp} in {self.catalog_type} Catalog")

        except Exception as e:
            self.log.error(f"Error while testing iceberg: {str(e)}")
            raise e
        finally:
            # Destroy S3 Tables catalog
            self.log.info(f"Destroying {self.catalog_type} catalog")
            iceberg_util.destroy_s3_tables_catalog()

            # Stop spark session
            self.log.info("Stopping spark session")
            iceberg_util.spark.stop()
            if os.path.exists(iceberg_util.state.sparkPath):
                self.log.info("Removing Spark local directory")
                shutil.rmtree(iceberg_util.state.sparkPath)

    def test_biglake_metastore_catalog(self):
        iceberg_util = IcebergUtil(
            state=self.iceberg_base
        )
        try:
            # Generate data
            data = [self.cbas_util.generate_docs(document_size=self.doc_size)
                    for _ in range(self.initial_doc_count)]
            schema = iceberg_util.infer_schema_from_list(data)

            # setup biglake metastore catalog with the data
            iceberg_util.setup_biglake_metastore_catalog(
                data, partitioned_field=self.partitioned_field, schema=schema, filter_by=self.filter_by)

            # create iceberg base setup
            self.base_setup(catalog_type=self.catalog_type,
                            external_collection_name=self.external_collection_name)

            # Query the EXTERNAL COLLECTION
            cmd = f"SELECT count(*) FROM {self.external_collection_name}"
            expected_count = self.initial_doc_count
            if self.filter_by:
                cmd += f" where not {self.filter_by}"
                expected_count = 0

            status, metrics, errors, results, handle, warnings = self.cbas_util.execute_statement_on_cbas_util(
                self.analytics_cluster,
                cmd)
            if status != "success":
                self.fail("Failed to query Iceberg external collection")
            self.log.info(f"Query results: {results}")
            count = results[0]['$1']
            self.assertEqual(count, expected_count,
                             f"Count {count} does not equal expected_count {expected_count}")
            self.log.info(
                f"Successfully queried data from {self.catalog_type} Catalog")

            if self.num_snapshots:
                for snapshot_num in range(1, self.num_snapshots + 1):
                    iceberg_util.spark.sql(f"""
                        UPDATE {self.iceberg_base.catalog_type}.{self.iceberg_base.database_name}.{self.iceberg_base.table_name}
                        SET price = price + 10000
                    """)

                    count = iceberg_util.spark.table(
                        f"{self.iceberg_base.catalog_type}.{self.iceberg_base.database_name}.{self.iceberg_base.table_name}").count()
                    self.log.info(
                        f"Snapshot {snapshot_num} created. Total rows: {count}\n")

                    # Query the EXTERNAL COLLECTION
                    status, metrics, errors, results, handle, warnings = self.cbas_util.execute_statement_on_cbas_util(
                        self.analytics_cluster,
                        f"SELECT count(*) FROM {self.external_collection_name}")
                    if status != "success":
                        self.fail(
                            "Failed to query Iceberg external collection")

                    self.log.info(f"Query results: {results}")
                    count = results[0]['$1']
                    self.assertEqual(count, self.initial_doc_count,
                                     f"Count {count} does not equal initial_doc_count {self.initial_doc_count}")
                    self.log.info(
                        f"Successfully queried data from {self.catalog_type} Catalog")

                # List all snapshots
                self.log.info("\n=== Snapshot history ===")
                try:
                    snapshots_df = iceberg_util.spark.sql(
                        f"SELECT * FROM {self.iceberg_base.catalog_type}.{self.iceberg_base.database_name}.{self.iceberg_base.table_name}.history")
                    snapshots_df.show(truncate=False)
                except Exception as e:
                    self.log.error(f"Error querying history table: {e}")

                # Create external collection with snapshot
                snapshot_list = [
                    {
                        "id": str(row.snapshot_id),
                        "timestamp_ms": str(int(
                            row.made_current_at.timestamp() * 1000
                            if hasattr(row.made_current_at, "timestamp") else
                            datetime.datetime.fromisoformat(
                                row.made_current_at).timestamp() * 1000
                        ))
                    }
                    for row in snapshots_df.collect()
                ]
                randomSnapshotId, randomSnapshotTimestamp = random.choice(
                    snapshot_list).values()

                if self.create_external_collection_with_sanpshotId:
                    self.base_setup(catalog_type=self.catalog_type,
                                    external_collection_name=self.external_collection_with_snapshot_name,
                                    snapshotId=randomSnapshotId)

                    # Query the EXTERNAL COLLECTION
                    status, metrics, errors, results, handle, warnings = self.cbas_util.execute_statement_on_cbas_util(
                        self.analytics_cluster,
                        f"SELECT count(*) FROM {self.external_collection_with_snapshot_name}")
                    if status != "success":
                        self.fail(
                            "Failed to query Iceberg external collection")

                    self.log.info(f"Query results: {results}")
                    count = results[0]['$1']
                    self.assertEqual(count, self.initial_doc_count,
                                     f"Count {count} does not equal initial_doc_count {self.initial_doc_count}")
                    self.log.info(
                        f"Successfully queried data from snapshot {randomSnapshotId} in {self.catalog_type} Catalog")

                if self.create_external_collection_with_sanpshotTimestamp:
                    self.base_setup(catalog_type=self.catalog_type,
                                    external_collection_name=self.external_collection_with_snapshot_name,
                                    snapshotTimestamp=randomSnapshotTimestamp)

                    # Query the EXTERNAL COLLECTION
                    status, metrics, errors, results, handle, warnings = self.cbas_util.execute_statement_on_cbas_util(
                        self.analytics_cluster,
                        f"SELECT count(*) FROM {self.external_collection_with_snapshot_name}")
                    if status != "success":
                        self.fail(
                            "Failed to query Iceberg external collection")

                    self.log.info(f"Query results: {results}")
                    count = results[0]['$1']
                    self.assertEqual(count, self.initial_doc_count,
                                     f"Count {count} does not equal initial_doc_count {self.initial_doc_count}")
                    self.log.info(
                        f"Successfully queried data from snapshot {randomSnapshotTimestamp} in {self.catalog_type} Catalog")

        except Exception as e:
            self.log.error(
                f"Error while testing {self.catalog_type} catalog: {str(e)}")
            raise e
        finally:
            # Destroy BigLake Metastore catalog
            self.log.info(f"Destroying {self.catalog_type} catalog")
            iceberg_util.destroy_biglake_metastore_catalog()

            # Stop spark session
            self.log.info("Stopping spark session")
            iceberg_util.spark.stop()
            if os.path.exists(iceberg_util.state.sparkPath):
                self.log.info("Removing Spark local directory")
                shutil.rmtree(iceberg_util.state.sparkPath)
