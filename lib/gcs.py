from google.cloud import storage


class GCS:
    def __init__(self, credentials):
        import logging
        logging.basicConfig()
        self.logger = logging.getLogger("GCS_Util")
        credentials = credentials
        self.client = storage.Client.from_service_account_info(info=credentials)

    def create_gcs_bucket(self, bucket_name, location="US"):
        try:
            bucket = self.client.bucket(bucket_name)
            self.client.create_bucket(bucket, location=location)
            self.logger.info(f"Bucket: {bucket_name}, created successfully")
            return True
        except Exception as err:
            self.logger.error(str(err))
            return False

    def empty_gcs_bucket(self, bucket_name):
        try:
            bucket = self.client.bucket(bucket_name)
            blobs = bucket.list_blobs()  # Get all objects in the bucket
            for blob in blobs:
                blob.delete()  # Delete each object
                self.logger.info(f"Deleted {blob.name} from bucket '{bucket_name}'")
            self.logger.info(f"All objects deleted from bucket '{bucket_name}'.")
            return True
        except Exception as e:
            self.logger.error(f"Failed to empty bucket '{bucket_name}': {e}")
            return False

    def delete_gcs_bucket(self, bucket_name):
        try:
            self.empty_gcs_bucket(bucket_name)
            bucket = self.client.bucket(bucket_name)
            bucket.delete()
            self.logger.info("Bucket {0} deleted".format(bucket_name))
            return True
        except Exception as err:
            self.logger.error("Failed to delete gcs bucket, reason: {}".format(str(err)))
            return False

    def list_objects_in_gcs_bucket(self, bucket_name):
        blob_list = []
        try:
            bucket = self.client.bucket(bucket_name)
            blobs = bucket.list_blobs()  # Get all objects in the bucket
            for blob in blobs:
                blob_list.append(blob.name)
            return blob_list
        except Exception as e:
            self.logger.error(f"Failed to empty bucket '{bucket_name}': {e}")
            return None

    def download_file_from_gcs(self, bucket_name, blob_name, destination_file_name):
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        try:
            blob.download_to_filename(destination_file_name)
            self.logger.info(f"File {blob_name} downloaded to {destination_file_name}.")
        except Exception as e:
            self.logger.error(f"Failed to download file from bucket '{bucket_name}': {e}")
