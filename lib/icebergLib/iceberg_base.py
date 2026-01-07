import time
import boto3
import os

from google.auth import default
from google.auth.transport.requests import Request
from lib.awsLib.S3 import S3

class IcebergBase:
    def __init__(self, aws_access_key=None, aws_secret_key=None, aws_session_token=None, gcs_credentials=None, catalog_type=None):
        # Common
        self.database_name = "icebergdb"  # aka Namespace
        self.table_name = "hotel"
        self.iceberg_bucket = f"analytics-iceberg-testing-{str(int(time.time()))}"
        self.catalog_type = catalog_type
        self.aws_account_id = "516524556673"
        self.sparkPath = os.path.join(os.getcwd(), "data/sparkSession") # "/data/sparkSession"

        # AWS Glues
        self.s3_warehouse_path = f"s3://{self.iceberg_bucket}"
        self.iceberg_region = "us-east-1"
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.aws_session_token = aws_session_token

        # S3 Tables
        self.s3_table_bucket_arn = None

        # GCP
        self.gcs_project_id = "cbc-dev-bbf356999fa41d60"
        self.gcs_credentials = gcs_credentials
        self.gcs_bucket_path = f"gs://{self.iceberg_bucket}"
        self.gcs_bucket_location = "us-east1"

        os.environ["AWS_SHARED_CREDENTIALS_FILE"] = "/dev/null"
        os.environ["AWS_CONFIG_FILE"] = "/dev/null"
        os.environ["AWS_ACCESS_KEY_ID"] = str(self.aws_access_key)
        os.environ["AWS_SECRET_ACCESS_KEY"] = str(self.aws_secret_key)
        os.environ["AWS_SESSION_TOKEN"] = str(self.aws_session_token)
        os.environ["AWS_REGION"] = str(self.iceberg_region)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(
            self.gcs_credentials)

        # Setup boto3
        boto3.setup_default_session(
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
            aws_session_token=self.aws_session_token,
            region_name=self.iceberg_region
        )

        self.glue_boto3_client = boto3.client(
            'glue',
            region_name=self.iceberg_region
        )

        self.s3tables_boto3_client = boto3.client(
            's3tables', region_name=self.iceberg_region)

        # Create S3 client
        self.s3_client = S3(
            access_key=self.aws_access_key,
            secret_key=self.aws_secret_key,
            session_token=self.aws_session_token,
            region=self.iceberg_region
        )

    def gcp_access_token(self):
        """Return GCP access token for cloud-platform scope (e.g. BigLake REST catalog)."""
        credentials, _ = default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"])
        credentials.refresh(Request())
        return credentials.token

