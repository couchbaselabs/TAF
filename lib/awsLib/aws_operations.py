import boto3
import json
from botocore.exceptions import ClientError
import os

class awsUtil(object):
    
    def __init__(self, user_type="read_only_user", aws_config_path=None, 
                 logger=None):
        if not logger:
            import logging
            self.logger = logging.getLogger("AWS_Util")
        else:
            self.logger = logger
        self.aws_config = None
        if not aws_config_path:
            aws_config_path = os.path.join(os.path.dirname(__file__),"aws_config.json")
        with open(aws_config_path, "r") as fh:
            self.aws_config = json.loads(fh.read())
        self.user_type = user_type
    
    def create_session(self, access_key=None, secret_key=None, session_token=None):
        """
        Create a session to AWS using the credentials provided.
        If no credentials are provided then, credentials are read from aws_config.json file.
        
        :param access_key: access key for the IAM user
        :param secret_key: secret key for the IAM user
        :param access_token: 
        """
        if not access_key:
            access_key = self.aws_config["Users"][self.user_type]["accessKeyId"]
        if not secret_key:
            secret_key = self.aws_config["Users"][self.user_type]["secretAccessKey"]
        if session_token:
            self.session = boto3.Session(aws_access_key_id=access_key, 
                                    aws_secret_access_key=secret_key, 
                                    aws_session_token=session_token)
        else:
            self.session = boto3.Session(aws_access_key_id=access_key, 
                                    aws_secret_access_key=secret_key)
        self.create_service_client(self.aws_config["Users"][self.user_type]["service"])
        self.create_service_resource(self.aws_config["Users"][self.user_type]["resource"])
    
    def create_service_client(self, service_name="s3", region=None):
        """
        Create a low level client for the service specified.
        If a region is not specified, the client is created in the default region (us-east-1).
        :param service_name: name of the service for which the client has to be created
        :param region: region in which the client has to created.
        """
        try:
            if region is None:
                self.client = self.session.client(service_name)
            else:
                self.client = self.session.client(service_name, region_name=region)
        except ClientError as e:
            self.logger.error(e)
    
    def create_service_resource(self, resource_name="s3"):
        """
        Create a service resource object, to access resources related to service. 
        """
        try:
            self.s3_resource = self.session.resource(resource_name)
        except Exception as e:
            self.logger.error(e)
            
    def create_bucket(self, bucket_name, region=None, return_response=False):
        """
        Create an S3 bucket in a specified region
        If a region is not specified, the bucket is created in the S3 default region (us-east-1).
    
        :param bucket_name: Bucket to create
        :param region: String region to create bucket in, e.g., 'us-west-2'
        :return: True if bucket created, else False
        """
        # Create bucket
        try:
            if region is None:
                response = self.client.create_bucket(Bucket=bucket_name)
            else:
                location = {'LocationConstraint': region}
                response = self.client.create_bucket(Bucket=bucket_name,
                                        CreateBucketConfiguration=location)
            if return_response:
                return response
            elif response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                return True
            else:
                return False
        except ClientError as e:
            self.logger.error(e)
            return False
    
    def delete_bucket(self, bucket_name, max_retry=5 ,retry_attempt=0):
        """
        Deletes a bucket
        :param bucket_name: Bucket to delete
        """
        try:
            bucket_deleted = False
            if retry_attempt < max_retry:
                if self.empty_bucket(bucket_name):
                    response = self.client.delete_bucket(Bucket=bucket_name)
                    if response["ResponseMetadata"]["HTTPStatusCode"] == 204:
                        bucket_deleted = True
                if not bucket_deleted:
                    self.delete_bucket(bucket_name, max_retry, retry_attempt+1)
                else:
                    return bucket_deleted
            else:
                return False
        except ClientError as e:
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
            if version_id:
                response = self.client.delete_object(Bucket=bucket_name, Key=file_path, VersionId= version_id)
            else:
                response = self.client.delete_object(Bucket=bucket_name, Key=file_path)
            if response["ResponseMetadata"]["HTTPStatusCode"] == 204:
                return True
            else:
                return False
        except ClientError as e:
            self.logger.error(e)
            return False
    
    def delete_folder(self, bucket_name, folder_path):
        try:
            bucket_resource = self.s3_resource.Bucket(bucket_name)
            for obj in bucket_resource.objects.filter(Prefix="{}/".format(folder_path)):
                self.s3_resource.Object(bucket_resource.name,obj.key).delete()
            return True
        except ClientError as e:
            self.logger.error(e)
            return False    
    
    def empty_bucket(self, bucket_name):
        """
        Deletes all the objects in the bucket.
        :param bucket_name: Bucket whose objects are to be deleted.
        """
        try:
            # Checking whether versioning is enabled on the bucket or not
            response = self.client.get_bucket_versioning(Bucket=bucket_name)
            if response["ResponseMetadata"]["HTTPStatusCode"] == 200 and response.get("Status", None):
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
        except ClientError as e:
            self.logger.error(e)
            return False
    
    def list_existing_buckets(self, return_response=False):
        """
        List all the S3 buckets.
        :param return_response: Returns response received from AWS if set to true, else returns a
        list of buckets.
        """
        try:
            response = self.client.list_buckets()
            if return_response:
                return response
            else:
                return response['Buckets']
        except ClientError as e:
            self.logger.error(e)
            return []
    
    