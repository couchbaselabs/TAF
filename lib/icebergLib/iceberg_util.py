from icebergLib.iceberg_base import IcebergBase
from lib.icebergLib.iceberg_glue import GlueCatalog
from lib.icebergLib.iceberg_s3tables import S3TablesCatalog
from lib.icebergLib.iceberg_biglake_metastore import BigLakeMetastoreCatalog
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from typing import Any, List, Dict



class IcebergUtil:
    def __init__(self, state: IcebergBase):
        self.state = state
        self.spark = None
        self.glue_catalog = GlueCatalog(self.state)
        self.s3_tables_catalog = S3TablesCatalog(self.state)
        self.biglake_metastore_catalog = BigLakeMetastoreCatalog(self.state)

    def create_iceberg_table(self, catalog_type, data, partitioned_field=[], schema=None, filter_by=None):
        """Create a Glue table if it doesn't exist."""
        df = self.spark.createDataFrame(data, schema=schema)
        df.printSchema()

        if filter_by:
            print(f"Filter by: {filter_by}")
            df = df.filter(filter_by)

        table_path = f"{catalog_type}.{self.state.database_name}.{self.state.table_name}"
        writer = df.writeTo(table_path).using("iceberg").tableProperty(
            "format-version", "2").tableProperty("write-format", "parquet")

        if partitioned_field:
            print(f"Partitioning by field: {partitioned_field}")
            writer = writer.partitionedBy(*partitioned_field)

        writer.createOrReplace()
        print(
            f"Data successfully written to Iceberg Table via {catalog_type}!")

    def infer_spark_type(self, value: Any) -> DataType:
        """Infer Spark SQL type from a Python value."""
        if isinstance(value, bool):
            return BooleanType()
        elif isinstance(value, int):
            return LongType()  # Use LongType for large integers
        elif isinstance(value, float):
            return DoubleType()
        elif isinstance(value, str):
            return StringType()
        elif isinstance(value, list):
            # Empty list? Assume array of strings
            if not value:
                return ArrayType(StringType())
            # Check if list of dicts
            if all(isinstance(v, dict) for v in value):
                # Infer schema from first element (could be improved to merge all elements)
                return ArrayType(self.infer_spark_schema(value[0]))
            else:
                # Infer type from first element
                return ArrayType(self.infer_spark_type(value[0]))
        elif isinstance(value, dict):
            return self.infer_spark_schema(value)
        else:
            return StringType()  # fallback

    def infer_spark_schema(self, data: Dict[str, Any]) -> StructType:
        """Recursively infer Spark StructType schema from a Python dict."""
        fields = []
        for key, value in data.items():
            spark_type = self.infer_spark_type(value)
            fields.append(StructField(key, spark_type, True))
        return StructType(fields)

    def infer_schema_from_list(self, data_list: List[Dict[str, Any]]) -> StructType:
        """Infer Spark schema from a list of Python dictionaries."""
        if not data_list:
            return StructType([])  # Empty schema
        # Start with first element
        schema = self.infer_spark_schema(data_list[0])
        return schema

    def create_spark_session(self, catalog_type=None):
        builder = (
            SparkSession.builder
            .appName("Load data into the Catalog for testing Iceberg")
            .config("spark.local.dir", self.state.sparkPath)
            .config("spark.driver.host", "localhost")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.memory", "4g")
            .config("spark.driver.maxResultSize", "2g")
            .config("spark.executor.memory", "4g")
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.sql.files.maxPartitionBytes", "134217728")
            .config("spark.memory.fraction", "0.8")
            .config("spark.memory.storageFraction", "0.3")
            .config(
                "spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
                "software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.4,"
                "software.amazon.awssdk:bundle:2.29.26,"
                "software.amazon.awssdk:url-connection-client:2.29.26,"
                "org.apache.iceberg:iceberg-gcp-bundle:1.6.1",
            )
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(f"spark.sql.catalog.{catalog_type}", "org.apache.iceberg.spark.SparkCatalog")
        )
        if catalog_type == "AWS_GLUE" or catalog_type == "AWS_GLUE_REST":
            print(f"Configuring Spark session for {catalog_type}...")
            builder = (
                builder
                .config(f"spark.sql.catalog.{catalog_type}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
                .config(f"spark.sql.catalog.{catalog_type}.warehouse", self.state.s3_warehouse_path)
                .config(f"spark.sql.catalog.{catalog_type}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .config(f"spark.sql.catalog.{catalog_type}.client.region", self.state.iceberg_region)
            )
        elif catalog_type == "S3_TABLES":
            builder = (
                builder
                .config(f"spark.sql.catalog.{catalog_type}.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog")
                .config(f"spark.sql.catalog.{catalog_type}.warehouse", self.state.s3_table_bucket_arn)
                .config(f"spark.sql.catalog.{catalog_type}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .config(f"spark.sql.catalog.{catalog_type}.client.region", self.state.iceberg_region)
            )
        elif catalog_type == "BIGLAKE_METASTORE":
            builder = (
                builder
                .config(f"spark.sql.catalog.{catalog_type}.type", "rest")
                .config(f"spark.sql.catalog.{catalog_type}.uri", "https://biglake.googleapis.com/iceberg/v1/restcatalog")
                .config(f"spark.sql.catalog.{catalog_type}.warehouse", self.state.gcs_bucket_path)
                .config(f"spark.sql.catalog.{catalog_type}.header.x-goog-user-project", self.state.gcs_project_id)
                .config(f"spark.sql.catalog.{catalog_type}.header.Authorization", f"Bearer {self.state.gcp_access_token()}")
                .config(f"spark.sql.catalog.{catalog_type}.io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO")
                .config(f"spark.sql.catalog.{catalog_type}.rest-metrics-reporting-enabled", "false")
                .config("spark.sql.defaultCatalog", catalog_type)
            )
        else:
            raise ValueError(
                f"Unsupported catalog_type: {catalog_type}. Use 'AWS_GLUE', 'AWS_GLUE_REST', 'S3_TABLES' or 'BIGLAKE_METASTORE'.")
        self.spark = builder.getOrCreate()

    def setup_glue_catalog(self, data, partitioned_field=[], filter_by=None):
        # create a new iceberg_bucket
        self.glue_catalog.create_s3_bucket()

        self.create_spark_session(catalog_type=self.state.catalog_type)

        # Create Glue database if it doesn't exist
        self.glue_catalog.create_glue_database()

        # Delete Glue table if it exists
        self.glue_catalog.delete_glue_table()

        # Load data
        self.create_iceberg_table(
            catalog_type=self.state.catalog_type,
            data=data, partitioned_field=partitioned_field, filter_by=filter_by)

        # Read from Glue Catalog Iceberg Table (Has to be done using EA)
        df = self.spark.table(
            f"{self.state.catalog_type}.{self.state.database_name}.{self.state.table_name}")
        df.printSchema()
        print(f"Number of rows read: {df.count()}")
        df.show(5)

    def destroy_glue_catalog(self):
        """Destroy Glue catalog."""
        self.glue_catalog.delete_s3_bucket()
        self.glue_catalog.delete_glue_table()
        self.glue_catalog.delete_glue_database()

    def setup_s3_tables_catalog(self, data, partitioned_field=[], filter_by=None):
        """Setup S3 Tables catalog."""
        # Create S3 table resources
        self.s3_tables_catalog.create_s3_table_bucket()

        # Create Spark session
        self.create_spark_session(catalog_type="S3_TABLES")

        # Load data
        self.create_iceberg_table(
            catalog_type=self.state.catalog_type,
            data=data, partitioned_field=partitioned_field, filter_by=filter_by)

        # Read from S3 Tables Iceberg Table (Has to be done using EA)
        df = self.spark.table(
            f"{self.state.catalog_type}.{self.state.database_name}.{self.state.table_name}")
        df.printSchema()
        print(f"Number of rows read: {df.count()}")
        df.show(5)

    def destroy_s3_tables_catalog(self):
        """Destroy S3 Tables catalog."""
        self.s3_tables_catalog.delete_s3_table()

    def setup_biglake_metastore_catalog(self, data, partitioned_field=[], schema=None, filter_by=None):
        self.biglake_metastore_catalog.create_gcs_bucket()
        self.biglake_metastore_catalog.create_biglake_metastore_catalog()
        self.create_spark_session(catalog_type="BIGLAKE_METASTORE")

        self.spark.sql("SHOW CATALOGS;").show(truncate=False)
        self.spark.sql(f"USE {self.state.catalog_type};")

        self.spark.sql(
            f"DROP TABLE IF EXISTS {self.state.database_name}.{self.state.table_name};")
        self.spark.sql(
            f"DROP NAMESPACE IF EXISTS {self.state.database_name} CASCADE;")

        self.spark.sql(f"SHOW NAMESPACES;").show(truncate=False)

        """ START OF ICEBERG TABLE CREATION """

        self.spark.sql(
            f"CREATE NAMESPACE IF NOT EXISTS {self.state.database_name};")
        self.spark.sql(f"SHOW NAMESPACES;").show(truncate=False)

        self.create_iceberg_table(
            catalog_type=self.state.catalog_type,
            data=data, partitioned_field=partitioned_field, schema=schema, filter_by=filter_by)

        self.spark.sql(f"SHOW TABLES IN {self.state.database_name};").show(
            truncate=False)

        self.spark.sql(f"DESCRIBE {self.state.database_name}.{self.state.table_name}").show(
            truncate=False)

        self.spark.sql(
            f"SELECT COUNT(*) FROM {self.state.database_name}.{self.state.table_name}").show(truncate=False)

        """ END OF ICEBERG TABLE CREATION """

    def destroy_biglake_metastore_catalog(self):
        self.create_spark_session(catalog_type="BIGLAKE_METASTORE")
        self.spark.sql("SHOW CATALOGS;").show(truncate=False)
        self.spark.sql(f"USE {self.state.catalog_type};")

        self.spark.sql(f"SHOW NAMESPACES;").show(truncate=False)
        self.spark.sql(f"SHOW TABLES IN {self.state.database_name};").show(
            truncate=False)

        self.spark.sql(
            f"DROP TABLE IF EXISTS {self.state.database_name}.{self.state.table_name};")
        self.spark.sql(
            f"DROP NAMESPACE IF EXISTS {self.state.database_name} CASCADE;")
        self.spark.sql(f"SHOW NAMESPACES;").show(truncate=False)

        self.biglake_metastore_catalog.delete_biglake_metastore_catalog()
        self.biglake_metastore_catalog.delete_gcs_bucket()

