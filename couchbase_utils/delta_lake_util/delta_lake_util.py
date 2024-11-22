"""
Created on 6-Nov-2024

@author: Umang Agrawal

Utility for using delta lake
The spark configuration in this utility is tied to following python module
versions-
delta-spark  3.2.1
pyspark      3.5.3
supports following required jar packages
org.apache.hadoop:hadoop-aws:3.3.4
org.apache.hadoop:hadoop-common:3.3.4
com.amazonaws:aws-java-sdk-bundle:1.12.262
If using different version of delta-spark or pyspark, please update jar
package versions accordingly.
"""

import json
import logging
import os
import pyspark

from delta import *
from cbas_utils.cbas_utils_columnar import BaseUtil


class DeltaLakeUtils(object):
    """
    Utility to create apache spark session and use it to perform action on
    delta lake tables.
    """

    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.spark_session = None
        os.environ["SPARK_LOCAL_HOSTNAME"] = "127.0.0.1"

    @staticmethod
    def format_s3_path(s3_bucket, delta_table_folder_path):
        return f"s3a://{s3_bucket}/{delta_table_folder_path}"

    def create_spark_session(self, aws_access_key, aws_secret_key,
                             sql_partitions=4, cores_to_use=2):
        """
        Create an Apache spark session.
        :param aws_access_key <str> Access key for AWS S3
        :param aws_secret_key <str> Secret access key for AWS S3
        :param sql_partitions <int>
        :param cores_to_use <int> Number of cores utilized by Spark
        """
        app_name = BaseUtil.generate_name(max_length=10)
        conf = (
            pyspark.conf.SparkConf()
            .setAppName(app_name)
            .set(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .set("spark.sql.extensions",
                 "io.delta.sql.DeltaSparkSessionExtension")
            .set("spark.hadoop.fs.s3a.access.key", aws_access_key)
            .set("spark.hadoop.fs.s3a.secret.key",
                 aws_secret_key)
            .set("spark.sql.shuffle.partitions", str(sql_partitions))
            .set("spark.databricks.delta.retentionDurationCheck.enabled",
                 "false")
            .setMaster(f"local[{cores_to_use}]")
            # replace the * with your desired number of cores. * for use all.
        )
        extra_packages = [
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "org.apache.hadoop:hadoop-common:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        ]
        builder = pyspark.sql.SparkSession.builder.appName("MyApp").config(
            conf=conf)
        self.spark_session = configure_spark_with_delta_pip(
            builder, extra_packages=extra_packages
        ).getOrCreate()

    def close_spark_session(self):
        """
        Closes an Apache spark session
        """
        if self.spark_session:
            self.spark_session.stop()

    def write_data_into_table(self, data, s3_bucket_name, delta_table_path,
                              write_mode="append"):
        """
        Write data into Delta Lake table.
        :param data <list of json objects> Data to be written in Delta-Lake 
        table.
        :param s3_bucket_name <str> Name of the S3 bucket where the delta 
        table is present.
        :param delta_table_path <str> full path on S3 where the delta lake
        table is present (create if not exists). Format folder/
        :param write_mode <str> Write mode, Accepted values - overwrite and 
        append
        """
        try:
            dataframe = self.spark_session.createDataFrame(data)
            dataframe.write.format("delta").mode(write_mode).save(
                self.format_s3_path(s3_bucket_name, delta_table_path))
            return True
        except Exception as err:
            self.log.error(str(err))
            return False

    def read_data_from_table(self, s3_bucket_name, delta_table_path):
        """
        Reads Data from Delta lake table.
        :param s3_bucket_name <str> Name of the S3 bucket where the delta 
        table is present.
        :param delta_table_path: <str> full path on S3 where the delta lake
        table is present. Format folder/
        :return: List of json abjects.
        Note - Use it for reading small size data, since everything is kept
        in memory.
        """
        delta_df = self.spark_session.read.format("delta").load(
            self.format_s3_path(s3_bucket_name, delta_table_path))
        # Convert Spark DataFrame to JSON strings and collect as a list
        json_strings = delta_df.toJSON().collect()

        # Convert JSON strings to dictionaries
        json_data = [json.loads(record) for record in json_strings]
        return json_data

    def stream_data_from_table(self, s3_bucket_name, delta_table_path,
                               stream_processing_function=None):
        """
        Streams Data from Delta lake table.
        :param s3_bucket_name <str> Name of the S3 bucket where the delta 
        table is present.
        :param delta_table_path: <str> full path on S3 where the delta lake
        table is present. Format folder/
        :param stream_processing_function <func> Function that performs some
        action or validation on stream of data being read. Function should
        accept a list of dict.
        :return:
        """
        # Read the Delta table as a streaming DataFrame
        streaming_df = self.spark_session.readStream.format("delta").load(
            self.format_s3_path(s3_bucket_name, delta_table_path))

        # Define a function to process each batch
        def process_batch(batch_df, batch_id):
            self.log.info(f"Processing batch ID: {batch_id}")

            # Convert Spark DataFrame to JSON strings and collect as a list
            json_strings = batch_df.toJSON().collect()

            # Convert JSON strings to dictionaries
            json_data = [json.loads(record) for record in json_strings]
            stream_processing_function(json_data)

        # Use foreachBatch to process each batch in the stream
        query = streaming_df.writeStream \
            .foreachBatch(process_batch) \
            .outputMode("append") \
            .start()

        # Wait for the streaming query to finish
        query.awaitTermination()

    def stream_data_into_json_file(self, s3_bucket_name, delta_table_path,
                                   output_file_path):
        """
        Streams Data from Delta lake table and stores in a json file.
        :param s3_bucket_name <str> Name of the S3 bucket where the delta 
        table is present.
        :param delta_table_path: <str> full path on S3 where the delta lake
        table is present. Format folder/
        :param output_file_path <str> Path of the json file where the read
        data is to be stored.
        :return:
        """
        # Read the Delta table as a streaming DataFrame
        streaming_df = self.spark_session.readStream.format("delta").load(
            self.format_s3_path(s3_bucket_name, delta_table_path))

        def append_to_single_json(batch_df, batch_id):
            self.log.info(f"Processing batch ID: {batch_id}")
            # Convert batch DataFrame to JSON records
            json_data = batch_df.toJSON().collect()

            # Append each record to a single JSON file
            with open(output_file_path, "a") as f:
                for record in json_data:
                    json.dump(json.loads(record), f)
                    f.write("\n")  # Write as newline-delimited JSON

        # Stream data from Delta Lake and apply foreachBatch
        query = streaming_df.writeStream \
            .foreachBatch(append_to_single_json) \
            .outputMode("append") \
            .start()

        query.awaitTermination()

    def list_all_table_versions(self, s3_bucket_name, delta_table_path):
        """
        List all versions of delta table.
        :param s3_bucket_name <str> Name of the S3 bucket where the delta 
        table is present.
        :param delta_table_path <str> full path on S3 where the delta lake
        table is present. Format folder/
        :return: List if table versions
        """
        delta_table = DeltaTable.forPath(
            self.spark_session, self.format_s3_path(
                s3_bucket_name, delta_table_path))
        history_df = delta_table.history()
        versions = history_df.select("version").rdd.flatMap(
            lambda x: x).collect()
        self.log.debug(f"Delta table {delta_table_path} has following "
                       f"versions {versions}")
        return versions

    def restore_previous_version_of_delta_table(
            self, s3_bucket_name, delta_table_path, version_to_restore):
        """
        Restore Delta Lake table to previous version.
        :param s3_bucket_name <str> Name of the S3 bucket where the delta 
        table is present.
        :param delta_table_path: <str> full path on S3 where the delta lake
        table is present. Format folder/
        :param version_to_restore: <int> version number to which the table
        has to be restored to.
        :return:
        """

        delta_table = DeltaTable.forPath(
            self.spark_session, self.format_s3_path(
                s3_bucket_name, delta_table_path))
        delta_table.restoreToVersion(version_to_restore)

    def perform_vacuum_on_delta_lake_table(
            self, s3_bucket_name, delta_table_path, retention_time=0):
        """
        Performs vacuum on Delta lake table.
        :param s3_bucket_name <str> Name of the S3 bucket where the delta 
        table is present.
        :param delta_table_path <str> full path on S3 where the delta lake
        table is present. Format folder/
        :param retention_time: <float/int> retention time in minutes
        :return:
        """
        delta_table = DeltaTable.forPath(
            self.spark_session,
            self.format_s3_path(s3_bucket_name, delta_table_path))
        retention_time_hrs = round(retention_time/60, 2)
        delta_table.vacuum(retentionHours=retention_time_hrs)

    def delete_data_from_table(self, s3_bucket_name, delta_table_path,
                               delete_condition=None):
        """
        Deletes data from Delta lake table based on some condition. If no
        condition is specifed then deletes all data.
        :param s3_bucket_name <str> Name of the S3 bucket where the delta 
        table is present.
        :param delta_table_path: <str> full path on S3 where the delta lake
        table is present. Format folder/
        :param delete_condition: <str> predicate using SQL formatted string.
        Eg - "age > 5"
        :return:
        """
        delta_table = DeltaTable.forPath(
            self.spark_session,
            self.format_s3_path(s3_bucket_name, delta_table_path))
        delta_table.delete(delete_condition)

    def update_data_in_table(self, s3_bucket_name, delta_table_path,
                             data_updates, update_condition=None):
        """
        Deletes data from Delta lake table based on some condition. If no
        condition is specifed then deletes all data.
        :param s3_bucket_name <str> Name of the S3 bucket where the delta 
        table is present.
        :param delta_table_path: <str> full path on S3 where the delta lake
        table is present. Format folder/
        :param data_updates <dict> Fields and their updated values.
        :param update_condition: <str> predicate using SQL formatted string.
        Eg - "age > 5"
        :return:
        """
        delta_table = DeltaTable.forPath(
            self.spark_session,
            self.format_s3_path(s3_bucket_name, delta_table_path))
        delta_table.update(condition=update_condition, set=data_updates)

    def get_doc_count_in_table(self, s3_bucket_name, delta_table_path):
        """
        Gets total number of rows/documents in Delta lake table
        :param s3_bucket_name <str> Name of the S3 bucket where the delta 
        table is present.
        :param delta_table_path: <str> full path on S3 where the delta lake
        table is present. Format folder/
        """
        # Load the Delta table
        delta_table = DeltaTable.forPath(
            self.spark_session,
            self.format_s3_path(s3_bucket_name, delta_table_path))

        # Use DeltaTable API to get the count of records
        return int(delta_table.toDF().count())
