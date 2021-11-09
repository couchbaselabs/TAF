import boto3
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
import argparse
import json

class S3():

    def __init__(self, access_key, secret_key, session_token=None):
        import logging
        logging.basicConfig()
        self.logger = logging.getLogger("AWS_Util")
        self.create_session(access_key, secret_key, session_token)
        self.s3_client = self.create_service_client(service_name="s3")
        self.s3_resource = self.create_service_resource(resource_name="s3")

    def create_session(self, access_key, secret_key, session_token=None):
        """
        Create a session to AWS using the credentials provided.
        If no credentials are provided then, credentials are read from aws_config.json file.

        :param access_key: access key for the IAM user
        :param secret_key: secret key for the IAM user
        :param access_token:
        """
        try:
            if session_token:
                self.aws_session = boto3.Session(aws_access_key_id=access_key,
                                             aws_secret_access_key=secret_key,
                                             aws_session_token=session_token)
            else:
                self.aws_session = boto3.Session(aws_access_key_id=access_key,
                                                 aws_secret_access_key=secret_key)
        except Exception as e:
            self.logger.error(e)

    def create_service_client(self, service_name, region=None):
        """
        Create a low level client for the service specified.
        If a region is not specified, the client is created in the default region (us-east-1).
        :param service_name: name of the service for which the client has to be created
        :param region: region in which the client has to created.
        """
        try:
            if region is None:
                return self.aws_session.client(service_name)
            else:
                return self.aws_session.client(service_name, region_name=region)
        except ClientError as e:
            self.logger.error(e)

    def create_service_resource(self, resource_name):
        """
        Create a service resource object, to access resources related to service.
        """
        try:
            return self.aws_session.resource(resource_name)
        except Exception as e:
            self.logger.error(e)

    def get_region_list(self):
        # Retrieves all regions/endpoints
        ec2 = self.create_service_client(service_name="ec2",region="us-west-1")
        response = ec2.describe_regions()
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            return [region['RegionName'] for region in response["Regions"]]
        else:
            return []

    def create_bucket(self, bucket_name, region):
        """
        Create an S3 bucket in a specified region
        If a region is not specified, the bucket is created in the S3 default region (us-east-1).

        :param bucket_name: Bucket to create
        :param region: String region to create bucket in, e.g., 'us-west-2'
        :return: True if bucket created, else False
        """
        # Create bucket
        try:
            location = {'LocationConstraint': region}
            response = self.s3_resource.Bucket(bucket_name).create(CreateBucketConfiguration=location)
            if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                return True
            else:
                return False
        except Exception as e:
            self.logger.error(e)
            return False

    def delete_bucket(self, bucket_name, max_retry=5, retry_attempt=0):
        """
        Deletes a bucket
        :param bucket_name: Bucket to delete
        """
        try:
            bucket_deleted = False
            if retry_attempt < max_retry:
                if bucket_name in self.list_existing_buckets():
                    if self.empty_bucket(bucket_name):
                        response = self.s3_resource.Bucket(bucket_name).delete()
                        if response["ResponseMetadata"]["HTTPStatusCode"] == 204:
                            bucket_deleted = True
                    if not bucket_deleted:
                        self.delete_bucket(bucket_name, max_retry, retry_attempt + 1)
                    else:
                        return bucket_deleted
                else:
                    return False
            else:
                return False
        except Exception as e:
            self.logger.error(e)
            return False

    def delete_file(self, bucket_name, file_path, version_id=None):
        """
        Deletes a single file/object, identified by object name.
        :param bucket_name: Bucket whose objects are to be deleted.
        :param file_path: path of the file, relative to S3 bucket
        :param version_id: to delete a specific version of an object.
        """
        try:
            s3_object = self.s3_resource.Object(bucket_name, file_path)
            if version_id:
                response = s3_object.delete(VersionId=version_id)
            else:
                response = s3_object.delete()
            if response["ResponseMetadata"]["HTTPStatusCode"] == 204:
                return True
            else:
                return False
        except Exception as e:
            self.logger.error(e)
            return False

    def delete_folder(self, bucket_name, folder_path):
        try:
            bucket_resource = self.s3_resource.Bucket(bucket_name)
            for obj in bucket_resource.objects.filter(Prefix="{}/".format(folder_path)):
                self.s3_resource.Object(bucket_resource.name, obj.key).delete()
            return True
        except Exception as e:
            self.logger.error(e)
            return False

    def empty_bucket(self, bucket_name):
        """
        Deletes all the objects in the bucket.
        :param bucket_name: Bucket whose objects are to be deleted.
        """
        try:
            # Checking whether versioning is enabled on the bucket or not
            response = self.s3_resource.BucketVersioning(bucket_name).status
            if not response:
                versioning = True
            else:
                versioning = False

            # Create a bucket resource object
            bucket_resource = self.s3_resource.Bucket(bucket_name)

            # Empty the bucket before deleting it.
            # If versioning is enabled delete all versions of all the objects, otherwise delete all the objects.
            if versioning:
                response = bucket_resource.object_versions.all().delete()
            else:
                response = bucket_resource.objects.all().delete()
            status = True
            for item in response:
                if item["ResponseMetadata"]["HTTPStatusCode"] != 200:
                    status = status and False
            return status
        except Exception as e:
            self.logger.error(e)
            return False

    def list_existing_buckets(self):
        """
        List all the S3 buckets.
        """
        try:
            response = self.s3_client.list_buckets()
            if response:
                return [x["Name"] for x in response['Buckets']]
            else:
                return []
        except Exception as e:
            self.logger.error(e)
            return []

    def get_region_wise_buckets(self):
        """
        Fetch all buckets in all regions.
        """
        try:
            buckets = self.list_existing_buckets()
            if buckets:
                region_wise_bucket = {}
                for bucket in buckets:
                    region = self.get_bucket_region(bucket)
                    if region in region_wise_bucket:
                        region_wise_bucket[region].append(bucket)
                    else:
                        region_wise_bucket[region] = [bucket]
                return region_wise_bucket
            else:
                return {}
        except Exception as e:
            self.logger.error(e)
            return {}

    def get_bucket_region(self, bucket_name):
        """
        Gets the region where the bucket is located
        """
        try:
            response = self.s3_client.list_buckets(Bucket=bucket_name)
            if response:
                if response["LocationConstraint"] == None:
                    return "us-east-1"
                else:
                    return response["LocationConstraint"]
            else:
                return ""
        except Exception as e:
            self.logger.error(e)
            return ""

    def upload_file(self, bucket_name, source_path, destination_path):
        """
        Uploads a file to bucket specified.
        :param bucket_name: name of the bucket where file has to be uploaded.
        :param source_path: path of the file to be uploaded.
        :param destination_path: path relative to aws bucket. If only file name is specified
        then file will be loaded in root folder relative to bucket.
        :return: True/False
        """
        try:
            response = self.s3_resource.Bucket(bucket_name).upload_file(source_path, destination_path)
            if not response:
                return True
            else:
                return False
        except Exception as e:
            self.logger.error(e)
            return False

    def upload_large_file(self, bucket_name, source_path, destination_path,
                          multipart_threshold=1024 * 1024 * 8, max_concurrency=10,
                          multipart_chunksize=1024 * 1024 * 8, use_threads=True):
        """
        Uploads a large file to bucket specified.
        :param bucket_name: name of the bucket where file has to be uploaded.
        :param source_path: path of the file to be uploaded.
        :param destination_path: path relative to aws bucket. If only file name is specified
        then file will be loaded in root folder relative to bucket.
        :param multipart_threshold: The transfer size threshold.
        :param max_concurrency: The maximum number of threads that will be
            making requests to perform a transfer. If ``use_threads`` is
            set to ``False``, the value provided is ignored as the transfer
            will only ever use the main thread.
        :param multipart_chunksize: The partition size of each part for a
            multipart transfer.
        :param use_threads: If True, threads will be used when performing
            S3 transfers. If False, no threads will be used in
            performing transfers
        :return: True/False
        """
        """
        WARNING : Please use this function if you want to upload a large file only (ex - file above 10 MB, 
        again this value is only subjective), as this API call to AWS is charged extra.
        """
        try:
            config = TransferConfig(multipart_threshold=multipart_threshold, max_concurrency=max_concurrency,
                                    multipart_chunksize=multipart_chunksize, use_threads=use_threads)
            response = self.s3_resource.Bucket(bucket_name).upload_file(source_path, destination_path,
                                                                        Config=config)
            if not response:
                return True
            else:
                return False
        except Exception as e:
            self.logger.error(e)
            return False

    def download_file(self, bucket_name, filename, dest_path):
        try:
            response = self.s3_resource.Bucket(bucket_name).download_file(filename, dest_path)
            if not response:
                return True
            else:
                return False
        except Exception as e:
            self.logger.error(e)
            return False

def main():
    parser = argparse.ArgumentParser()

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--new_bucket", help="create new bucket with name")
    group.add_argument("--existing_bucket", help="use existing bucket with name")
    group.add_argument("--list_bucket", help="list existing buckets")
    group.add_argument("--list_regionwise_bucket", help="list existing buckets")
    group.add_argument("--get_regions", help="fetches all the regions", action="store_true")

    parser.add_argument("access_key", help="access key for aws")
    parser.add_argument("secret_key", help="secret key for aws")
    parser.add_argument("session_token", help="session token for aws")
    parser.add_argument("--region", help="AWS region")
    parser.add_argument("--upload_file", nargs=2, help="specify source file path and path on aws")
    parser.add_argument("--upload_large_file", nargs=2, help="specify source file path and path on aws")
    parser.add_argument("--delete_bucket", help="to be used with --existing_bucket flag", action="store_true")
    parser.add_argument("--download_file", nargs=2, help="specify path on aws and dest file path")
    parser.add_argument("--delete_file", help="specify path on aws and dest file path")
    parser.add_argument("--empty_bucket", help="to be used with --existing_bucket flag", action="store_true")

    args = parser.parse_args()

    if args.session_token:
        s3_obj = S3(args.access_key, args.secret_key, args.session_token)
    else:
        s3_obj = S3(args.access_key, args.secret_key)

    if args.new_bucket:
        result = {"result": s3_obj.create_bucket(args.new_bucket, args.region)}
        print(json.dumps(result))
    elif args.existing_bucket:
        if args.upload_file:
            result = {"result": s3_obj.upload_file(args.existing_bucket, args.upload_file[0], args.upload_file[1])}
            print(json.dumps(result))
        elif args.upload_large_file:
            result = {"result": s3_obj.upload_large_file(args.existing_bucket,
                                                         args.upload_large_file[0], args.upload_large_file[1])}
            print(json.dumps(result))
        elif args.delete_bucket:
            result = {"result": s3_obj.delete_bucket(args.existing_bucket)}
            print(json.dumps(result))
        elif args.download_file:
            result = {"result": s3_obj.download_file(args.existing_bucket,
                                                     args.download_file[0], args.download_file[1])}
            print(json.dumps(result))
        elif args.delete_file:
            result = {"result": s3_obj.delete_file(args.existing_bucket, args.delete_file)}
            print(json.dumps(result))
        elif args.empty_bucket:
            result = {"result": s3_obj.empty_bucket(args.existing_bucket)}
            print(json.dumps(result))
    elif args.get_regions:
        result = {"result": s3_obj.get_region_list()}
        print(json.dumps(result))
    elif args.list_bucket:
        result = {"result": s3_obj.list_existing_buckets()}
        print(json.dumps(result))
    elif args.list_regionwise_bucket:
        result = {"result": s3_obj.get_region_wise_buckets()}
        print(json.dumps(result))


if __name__ == "__main__":
    main()
