from icebergLib.iceberg_base import IcebergBase
from botocore.exceptions import ClientError

class GlueCatalog:
    def __init__(self, state: IcebergBase):
        self.state = state

    def delete_s3_bucket(self):
        """Delete S3 bucket if it exists."""
        try:
            if self.state.s3_client.delete_bucket(self.state.iceberg_bucket):
                print(
                    f"S3 bucket {self.state.iceberg_bucket} deleted successfully.")
            else:
                print(
                    f"S3 bucket {self.state.iceberg_bucket} does not exist or failed to delete.")
        except Exception as e:
            print(
                f"Error while deleting S3 bucket {self.state.iceberg_bucket}: {str(e)}")

    def create_s3_bucket(self):
        """Create S3 bucket if it doesn't exist."""
        if not self.state.s3_client.create_bucket(self.state.iceberg_bucket, self.state.iceberg_region):
            raise Exception(
                f"Error while creating S3 bucket {self.state.iceberg_bucket}")
        print(
            f"S3 bucket {self.state.iceberg_bucket} created successfully.")

    def create_glue_database(self):
        """Create Glue database if it doesn't exist."""
        try:
            self.state.glue_boto3_client.get_database(
                Name=self.state.database_name)
            print(f"Database {self.state.database_name} already exists.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                self.state.glue_boto3_client.create_database(
                    DatabaseInput={
                        'Name': self.state.database_name,
                        'Description': 'Database for Iceberg tables'
                    }
                )
                print(
                    f"Database {self.state.database_name} created successfully.")
            else:
                print(
                    f"Error while creating Glue database {self.state.database_name}: {str(e)}")
                raise e

    def delete_glue_database(self):
        """Delete a Glue database if it exists."""
        try:
            self.state.glue_boto3_client.delete_database(
                Name=self.state.database_name
            )
            print(f"Database {self.state.database_name} deleted successfully.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                print(
                    f"Database {self.state.database_name} does not exist, skipping delete.")
            else:
                raise e

    def delete_glue_table(self):
        """Delete a Glue table if it exists."""
        try:
            self.state.glue_boto3_client.delete_table(
                DatabaseName=self.state.database_name,
                Name=self.state.table_name
            )
            print(
                f"Table {self.state.database_name}.{self.state.table_name} deleted successfully.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                print(
                    f"Table {self.state.database_name}.{self.state.table_name} does not exist, skipping delete.")
            else:
                raise e

