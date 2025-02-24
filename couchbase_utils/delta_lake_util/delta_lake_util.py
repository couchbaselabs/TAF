"""
Created on 6-Nov-2024

@author: Umang Agrawal

Utility for using Delta Lake with Apache Spark for both AWS S3 & Google Cloud Storage (GCS).
This utility supports:
- delta-spark  3.2.1
- pyspark      3.5.3
- Required JARs:
  - org.apache.hadoop:hadoop-aws:3.3.4
  - org.apache.hadoop:hadoop-common:3.3.4
  - com.amazonaws:aws-java-sdk-bundle:1.12.262
  - com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.5
"""

import json
import logging
import os
import pyspark
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from cbas_utils.cbas_utils_columnar import BaseUtil


class DeltaLakeUtils:
    """
    Utility to create Apache Spark session for both AWS S3 and GCS.
    Allows data operations on Delta Lake tables.
    """

    def __init__(self, storage_type, credentials, sql_partitions=4, cores_to_use=2):
        """
        Initialize DeltaLakeUtils for AWS S3 or Google Cloud Storage (GCS).

        :param storage_type: <str> Either "s3" for AWS S3 or "gcs" for Google Cloud Storage.
        :param credentials: <dict> Dictionary containing authentication details.
        :param sql_partitions: <int> Number of Spark partitions.
        :param cores_to_use: <int> Number of cores to use for Spark.
        """
        self.log = logging.getLogger(__name__)
        self.spark_session = None
        self.storage_type = storage_type.lower()
        self.sql_partitions = sql_partitions
        self.cores_to_use = cores_to_use

        os.environ["SPARK_LOCAL_HOSTNAME"] = "127.0.0.1"

        # Initialize Spark session based on storage type
        if self.storage_type == "s3":
            self.create_spark_session_s3(
                aws_access_key=credentials.get("aws_access_key"),
                aws_secret_key=credentials.get("aws_secret_key"),
            )
        elif self.storage_type == "gcs":
            self.create_spark_session_gcs(
                gcs_service_account_path=credentials.get("gcs_service_account_path")
            )
        else:
            raise ValueError("Invalid storage type. Use 's3' for AWS S3 or 'gcs' for Google Cloud Storage.")

    @staticmethod
    def format_storage_path(storage_type, bucket_name, delta_table_path):
        """
        Format storage path dynamically for S3 or GCS.

        :param storage_type: <str> Either "s3" or "gcs".
        :param bucket_name: <str> Bucket name.
        :param delta_table_path: <str> Path to the Delta table.
        :return: <str> Formatted storage path.
        """
        if storage_type == "s3":
            return f"s3a://{bucket_name}/{delta_table_path}"
        elif storage_type == "gcs":
            return f"gs://{bucket_name}/{delta_table_path}"
        else:
            raise ValueError("Invalid storage type. Use 's3' for AWS S3 or 'gcs' for Google Cloud Storage.")

    def create_spark_session_s3(self, aws_access_key, aws_secret_key):
        """
        Create an Apache Spark session configured for AWS S3.
        """
        app_name = BaseUtil.generate_name(max_length=10)
        conf = (
            pyspark.conf.SparkConf()
            .setAppName(app_name)
            .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .set("spark.hadoop.fs.s3a.access.key", aws_access_key)
            .set("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
            .set("spark.sql.shuffle.partitions", str(self.sql_partitions))
            .set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            .setMaster(f"local[{self.cores_to_use}]")
        )

        extra_packages = [
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "org.apache.hadoop:hadoop-common:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        ]

        builder = SparkSession.builder.appName("MyApp").config(conf=conf)
        self.spark_session = configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()

    def create_spark_session_gcs(self, gcs_service_account_path):
        """
        Create an Apache Spark session configured for Google Cloud Storage (GCS).
        """
        app_name = BaseUtil.generate_name(max_length=10)
        conf = (
            pyspark.conf.SparkConf()
            .setAppName(app_name)
            .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
            .set("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
            .set("google.cloud.auth.service.account.enable", "true")
            .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcs_service_account_path)
            .set("spark.sql.shuffle.partitions", str(self.sql_partitions))
            .set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            .set("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
            .set("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
            .setMaster(f"local[{self.cores_to_use}]")
        )

        extra_packages = [
            "org.apache.hadoop:hadoop-common:3.3.4",
            "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.5",
        ]

        builder = SparkSession.builder.appName(app_name).config(conf=conf)
        self.spark_session = configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()

    def close_spark_session(self):
        """
        Closes an Apache Spark session.
        """
        if self.spark_session:
            self.spark_session.stop()

    def write_data_into_table(self, data, bucket_name, delta_table_path, write_mode="append"):
        """
        Write data into a Delta Lake table.
        """
        try:
            dataframe = self.spark_session.createDataFrame(data)
            dataframe.write.format("delta").mode(write_mode).save(
                self.format_storage_path(self.storage_type, bucket_name, delta_table_path)
            )
            return True
        except Exception as err:
            self.log.error(str(err))
            return False

    def read_data_from_table(self, bucket_name, delta_table_path):
        """
        Reads Data from a Delta Lake table.
        """
        delta_df = self.spark_session.read.format("delta").load(
            self.format_storage_path(self.storage_type, bucket_name, delta_table_path)
        )
        json_strings = delta_df.toJSON().collect()
        return [json.loads(record) for record in json_strings]

    def list_all_table_versions(self, bucket_name, delta_table_path):
        """
        List all versions of a Delta table.
        """
        delta_table = DeltaTable.forPath(
            self.spark_session, self.format_storage_path(self.storage_type, bucket_name, delta_table_path)
        )
        versions = delta_table.history().select("version").rdd.flatMap(lambda x: x).collect()
        return versions

    def restore_previous_version_of_delta_table(self, bucket_name, delta_table_path, version_to_restore):
        """
        Restore Delta Lake table to a previous version.
        """
        delta_table = DeltaTable.forPath(
            self.spark_session, self.format_storage_path(self.storage_type, bucket_name, delta_table_path)
        )
        delta_table.restoreToVersion(version_to_restore)

    def perform_vacuum_on_delta_lake_table(self, bucket_name, delta_table_path, retention_time=0):
        """
        Performs vacuum on Delta Lake table.
        :param bucket_name <str>: Name of the S3/GCS bucket where the Delta table is present.
        :param delta_table_path <str>: Full path on S3/GCS where the Delta Lake table is present. Format: folder/
        :param retention_time <float/int>: Retention time in minutes.
        """
        delta_table = DeltaTable.forPath(
            self.spark_session, self.format_storage_path(self.storage_type, bucket_name, delta_table_path)
        )
        retention_time_hrs = round(retention_time / 60, 2)
        delta_table.vacuum(retentionHours=retention_time_hrs)

    def delete_data_from_table(self, bucket_name, delta_table_path, delete_condition=None):
        """
        Deletes data from Delta Lake table based on a condition.
        If no condition is specified, deletes all data.

        :param bucket_name <str>: Name of the S3/GCS bucket where the Delta table is present.
        :param delta_table_path <str>: Full path on S3/GCS where the Delta Lake table is present. Format: folder/
        :param delete_condition <str>: Predicate using SQL formatted string. Eg - "age > 5"
        """
        delta_table = DeltaTable.forPath(
            self.spark_session, self.format_storage_path(self.storage_type, bucket_name, delta_table_path)
        )
        delta_table.delete(delete_condition)

    def update_data_in_table(self, bucket_name, delta_table_path, data_updates, update_condition=None):
        """
        Updates data in Delta Lake table based on a condition.

        :param bucket_name <str>: Name of the S3/GCS bucket where the Delta table is present.
        :param delta_table_path <str>: Full path on S3/GCS where the Delta Lake table is present. Format: folder/
        :param data_updates <dict>: Fields and their updated values.
        :param update_condition <str>: Predicate using SQL formatted string. Eg - "age > 5"
        """
        delta_table = DeltaTable.forPath(
            self.spark_session, self.format_storage_path(self.storage_type, bucket_name, delta_table_path)
        )
        delta_table.update(condition=update_condition, set=data_updates)

    def get_doc_count_in_table(self, bucket_name, delta_table_path):
        """
        Gets total number of rows/documents in Delta Lake table.

        :param bucket_name <str>: Name of the S3/GCS bucket where the Delta table is present.
        :param delta_table_path <str>: Full path on S3/GCS where the Delta Lake table is present. Format: folder/
        :return: <int> Number of documents in the Delta table.
        """
        delta_table = DeltaTable.forPath(
            self.spark_session, self.format_storage_path(self.storage_type, bucket_name, delta_table_path)
        )

        # Use DeltaTable API to get the count of records
        return int(delta_table.toDF().count())

