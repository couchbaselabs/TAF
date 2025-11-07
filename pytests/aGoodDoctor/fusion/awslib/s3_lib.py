"""
AWS S3 Library for TAF
Provides functionality to list buckets, files in S3 buckets, get file sizes, and delete files.
"""

import boto3
from botocore.exceptions import ClientError
from typing import List, Dict, Optional, Any
import logging
from datetime import datetime


class S3Lib:
    """
    S3 library for managing S3 buckets and objects, including listing, 
    file operations, and size management.
    """

    def __init__(self, access_key, secret_key, session_token=None, region=None, endpoint_url=None):
        """
        Initialize S3 client.
        
        :param access_key: AWS access key
        :param secret_key: AWS secret key
        :param session_token: AWS session token (optional)
        :param region: AWS region (optional, defaults to us-east-1)
        :param endpoint_url: Custom endpoint URL (optional)
        """
        logging.basicConfig()
        logging.getLogger('boto3').setLevel(logging.ERROR)
        logging.getLogger('botocore').setLevel(logging.ERROR)
        self.logger = logging.getLogger("AWS_S3_Util")
        self.region = region or 'us-east-1'
        self.endpoint_url = endpoint_url
        
        # Create AWS session
        if session_token:
            self.aws_session = boto3.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                aws_session_token=session_token)
        else:
            self.aws_session = boto3.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key)
        
        # Create S3 client and resource
        self.s3_client = self.aws_session.client(
            's3', 
            region_name=self.region,
            endpoint_url=self.endpoint_url
        )
        self.s3_resource = self.aws_session.resource(
            's3',
            region_name=self.region,
            endpoint_url=self.endpoint_url
        )

    def list_buckets(self) -> List[Dict[str, Any]]:
        """
        List all S3 buckets.
        
        :return: List of bucket dictionaries with metadata
        """
        try:
            response = self.s3_client.list_buckets()
            buckets = []
            
            for bucket in response['Buckets']:
                bucket_info = {
                    'Name': bucket['Name'],
                    'CreationDate': bucket['CreationDate'],
                    'Region': self.get_bucket_region(bucket['Name'])
                }
                buckets.append(bucket_info)
            
            return buckets
        except ClientError as e:
            self.logger.error(f"Error listing buckets: {e}")
            return []

    def get_bucket_region(self, bucket_name: str) -> str:
        """
        Get the region where a bucket is located.
        
        :param bucket_name: Name of the bucket
        :return: Region name or 'us-east-1' if not specified
        """
        try:
            response = self.s3_client.get_bucket_location(Bucket=bucket_name)
            location = response.get('LocationConstraint')
            return location if location else 'us-east-1'
        except ClientError as e:
            self.logger.error(f"Error getting bucket region for {bucket_name}: {e}")
            return 'us-east-1'

    def list_files_in_bucket(self, bucket_name: str, prefix: str = '', 
                           max_keys: int = 1000) -> List[Dict[str, Any]]:
        """
        List files/objects in an S3 bucket.
        
        :param bucket_name: Name of the S3 bucket
        :param prefix: Prefix to filter objects (optional)
        :param max_keys: Maximum number of objects to return
        :return: List of object dictionaries with metadata
        """
        try:
            files = []
            paginator = self.s3_client.get_paginator('list_objects_v2')
            
            page_params = {
                'Bucket': bucket_name,
                'MaxKeys': max_keys
            }
            if prefix:
                page_params['Prefix'] = prefix
            
            for page in paginator.paginate(**page_params):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        file_info = {
                            'Key': obj['Key'],
                            'Size': obj['Size'],
                            'LastModified': obj['LastModified'],
                            'StorageClass': obj.get('StorageClass', 'STANDARD'),
                            'ETag': obj['ETag']
                        }
                        files.append(file_info)
            
            return files
        except ClientError as e:
            self.logger.error(f"Error listing files in bucket {bucket_name}: {e}")
            return []

    def get_file_size(self, bucket_name: str, file_key: str) -> Optional[int]:
        """
        Get the size of a specific file in S3.
        
        :param bucket_name: Name of the S3 bucket
        :param file_key: Key/path of the file in the bucket
        :return: File size in bytes or None if not found
        """
        try:
            response = self.s3_client.head_object(Bucket=bucket_name, Key=file_key)
            return response['ContentLength']
        except ClientError as e:
            self.logger.error(f"Error getting file size for {file_key} in {bucket_name}: {e}")
            return None

    def get_file_metadata(self, bucket_name: str, file_key: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata for a specific file in S3.
        
        :param bucket_name: Name of the S3 bucket
        :param file_key: Key/path of the file in the bucket
        :return: Dictionary with file metadata or None if not found
        """
        try:
            response = self.s3_client.head_object(Bucket=bucket_name, Key=file_key)
            metadata = {
                'Key': file_key,
                'Size': response['ContentLength'],
                'LastModified': response['LastModified'],
                'StorageClass': response.get('StorageClass', 'STANDARD'),
                'ETag': response['ETag'],
                'ContentType': response.get('ContentType', ''),
                'Metadata': response.get('Metadata', {})
            }
            return metadata
        except ClientError as e:
            self.logger.error(f"Error getting file metadata for {file_key} in {bucket_name}: {e}")
            return None

    def delete_file(self, bucket_name: str, file_key: str) -> bool:
        """
        Delete a single file from S3.
        
        :param bucket_name: Name of the S3 bucket
        :param file_key: Key/path of the file to delete
        :return: True if successful, False otherwise
        """
        try:
            response = self.s3_client.delete_object(Bucket=bucket_name, Key=file_key)
            return response['ResponseMetadata']['HTTPStatusCode'] == 204
        except ClientError as e:
            self.logger.error(f"Error deleting file {file_key} from {bucket_name}: {e}")
            return False

    def delete_multiple_files(self, bucket_name: str, file_keys: List[str]) -> Dict[str, bool]:
        """
        Delete multiple files from S3.
        
        :param bucket_name: Name of the S3 bucket
        :param file_keys: List of file keys to delete
        :return: Dictionary mapping file keys to success status
        """
        results = {}
        
        try:
            # Delete objects in batches of 1000 (S3 limit)
            batch_size = 1000
            for i in range(0, len(file_keys), batch_size):
                batch = file_keys[i:i + batch_size]
                
                objects_to_delete = [{'Key': key} for key in batch]
                response = self.s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={
                        'Objects': objects_to_delete,
                        'Quiet': False
                    }
                )
                
                # Process successful deletions
                for deleted in response.get('Deleted', []):
                    results[deleted['Key']] = True
                
                # Process errors
                for error in response.get('Errors', []):
                    results[error['Key']] = False
                    self.logger.error(f"Error deleting {error['Key']}: {error['Message']}")
                    
        except ClientError as e:
            self.logger.error(f"Error deleting multiple files from {bucket_name}: {e}")
            # Mark all remaining files as failed
            for key in file_keys:
                if key not in results:
                    results[key] = False
        
        return results

    def delete_files_by_prefix(self, bucket_name: str, prefix: str) -> Dict[str, bool]:
        """
        Delete all files with a specific prefix from S3.
        
        :param bucket_name: Name of the S3 bucket
        :param prefix: Prefix to match files for deletion
        :return: Dictionary mapping file keys to success status
        """
        try:
            # First, list all files with the prefix
            files = self.list_files_in_bucket(bucket_name, prefix=prefix)
            file_keys = [file['Key'] for file in files]
            
            if not file_keys:
                self.logger.info(f"No files found with prefix '{prefix}' in bucket {bucket_name}")
                return {}
            
            # Delete all files
            return self.delete_multiple_files(bucket_name, file_keys)
            
        except Exception as e:
            self.logger.error(f"Error deleting files by prefix '{prefix}' from {bucket_name}: {e}")
            return {}

    def get_bucket_size(self, bucket_name: str, prefix: str = '') -> Dict[str, Any]:
        """
        Get the total size and count of files in a bucket.
        
        :param bucket_name: Name of the S3 bucket
        :param prefix: Prefix to filter objects (optional)
        :return: Dictionary with total size, file count, and size by storage class
        """
        try:
            total_size = 0
            file_count = 0
            size_by_storage_class = {}
            
            files = self.list_files_in_bucket(bucket_name, prefix=prefix)
            
            for file_info in files:
                size = file_info['Size']
                storage_class = file_info['StorageClass']
                
                total_size += size
                file_count += 1
                
                if storage_class in size_by_storage_class:
                    size_by_storage_class[storage_class] += size
                else:
                    size_by_storage_class[storage_class] = size
            
            return {
                'total_size_bytes': total_size,
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'total_size_gb': round(total_size / (1024 * 1024 * 1024), 2),
                'file_count': file_count,
                'size_by_storage_class': size_by_storage_class
            }
        except Exception as e:
            self.logger.error(f"Error getting bucket size for {bucket_name}: {e}")
            return {
                'total_size_bytes': 0,
                'total_size_mb': 0,
                'total_size_gb': 0,
                'file_count': 0,
                'size_by_storage_class': {}
            }

    def list_files_by_size_range(self, bucket_name: str, min_size: int = 0, 
                                max_size: int = None, prefix: str = '') -> List[Dict[str, Any]]:
        """
        List files in a bucket within a specific size range.
        
        :param bucket_name: Name of the S3 bucket
        :param min_size: Minimum file size in bytes
        :param max_size: Maximum file size in bytes (None for no limit)
        :param prefix: Prefix to filter objects (optional)
        :return: List of file dictionaries within the size range
        """
        try:
            all_files = self.list_files_in_bucket(bucket_name, prefix=prefix)
            filtered_files = []
            
            for file_info in all_files:
                size = file_info['Size']
                if size >= min_size and (max_size is None or size <= max_size):
                    filtered_files.append(file_info)
            
            return filtered_files
        except Exception as e:
            self.logger.error(f"Error listing files by size range in {bucket_name}: {e}")
            return []

    def list_large_files(self, bucket_name: str, size_threshold_mb: int = 100, 
                        prefix: str = '') -> List[Dict[str, Any]]:
        """
        List files larger than a specified threshold.
        
        :param bucket_name: Name of the S3 bucket
        :param size_threshold_mb: Size threshold in MB
        :param prefix: Prefix to filter objects (optional)
        :return: List of large file dictionaries
        """
        size_threshold_bytes = size_threshold_mb * 1024 * 1024
        return self.list_files_by_size_range(
            bucket_name, 
            min_size=size_threshold_bytes, 
            prefix=prefix
        )

    def search_files_by_name(self, bucket_name: str, name_pattern: str, 
                           prefix: str = '') -> List[Dict[str, Any]]:
        """
        Search for files by name pattern in a bucket.
        
        :param bucket_name: Name of the S3 bucket
        :param name_pattern: Pattern to search for in file names
        :param prefix: Prefix to filter objects (optional)
        :return: List of matching file dictionaries
        """
        try:
            all_files = self.list_files_in_bucket(bucket_name, prefix=prefix)
            matching_files = []
            
            for file_info in all_files:
                if name_pattern.lower() in file_info['Key'].lower():
                    matching_files.append(file_info)
            
            return matching_files
        except Exception as e:
            self.logger.error(f"Error searching files by name in {bucket_name}: {e}")
            return []

    def get_bucket_summary(self, bucket_name: str) -> Dict[str, Any]:
        """
        Get a comprehensive summary of a bucket.
        
        :param bucket_name: Name of the S3 bucket
        :return: Dictionary with bucket summary information
        """
        try:
            # Get basic bucket info
            bucket_info = self.get_bucket_size(bucket_name)
            
            # Get file count by storage class
            files = self.list_files_in_bucket(bucket_name)
            file_count_by_storage_class = {}
            
            for file_info in files:
                storage_class = file_info['StorageClass']
                file_count_by_storage_class[storage_class] = file_count_by_storage_class.get(storage_class, 0) + 1
            
            # Get recent files (last 7 days)
            recent_files = []
            seven_days_ago = datetime.now().timestamp() - (7 * 24 * 60 * 60)
            
            for file_info in files:
                if file_info['LastModified'].timestamp() > seven_days_ago:
                    recent_files.append(file_info)
            
            summary = {
                'bucket_name': bucket_name,
                'region': self.get_bucket_region(bucket_name),
                'total_files': bucket_info['file_count'],
                'total_size_bytes': bucket_info['total_size_bytes'],
                'total_size_mb': bucket_info['total_size_mb'],
                'total_size_gb': bucket_info['total_size_gb'],
                'size_by_storage_class': bucket_info['size_by_storage_class'],
                'file_count_by_storage_class': file_count_by_storage_class,
                'recent_files_count': len(recent_files),
                'largest_file': max(files, key=lambda x: x['Size']) if files else None,
                'smallest_file': min(files, key=lambda x: x['Size']) if files else None
            }
            
            return summary
        except Exception as e:
            self.logger.error(f"Error getting bucket summary for {bucket_name}: {e}")
            return {}

    def create_bucket(self, bucket_name: str, region: str = None) -> bool:
        """
        Create a new S3 bucket.
        
        :param bucket_name: Name of the bucket to create
        :param region: Region to create the bucket in (defaults to instance region)
        :return: True if successful, False otherwise
        """
        try:
            region = region or self.region
            
            if region == 'us-east-1':
                # us-east-1 doesn't need LocationConstraint
                self.s3_client.create_bucket(Bucket=bucket_name)
            else:
                self.s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': region}
                )
            
            return True
        except ClientError as e:
            self.logger.error(f"Error creating bucket {bucket_name}: {e}")
            return False

    def delete_bucket(self, bucket_name: str, force: bool = False) -> bool:
        """
        Delete an S3 bucket.
        
        :param bucket_name: Name of the bucket to delete
        :param force: If True, delete all objects first
        :return: True if successful, False otherwise
        """
        try:
            if force:
                # Delete all objects first
                files = self.list_files_in_bucket(bucket_name)
                file_keys = [file['Key'] for file in files]
                if file_keys:
                    self.delete_multiple_files(bucket_name, file_keys)
            
            self.s3_client.delete_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            self.logger.error(f"Error deleting bucket {bucket_name}: {e}")
            return False

    def _normalize_prefix(self, prefix: str) -> str:
        """
        Ensure S3 prefix ends with '/'. Empty prefix returns '' (bucket root).
        """
        if not prefix:
            return ''
        return prefix if prefix.endswith('/') else prefix + '/'

    def _list_common_prefixes(self, bucket_name: str, prefix: str = '') -> List[str]:
        """
        List immediate sub-folder prefixes under the given prefix using Delimiter '/'.

        :param bucket_name: S3 bucket name
        :param prefix: Parent prefix (acts like a folder path)
        :return: List of common prefixes (child "folders")
        """
        try:
            normalized = self._normalize_prefix(prefix)
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_params = {
                'Bucket': bucket_name,
                'Delimiter': '/'
            }
            if normalized:
                page_params['Prefix'] = normalized

            subfolders = []
            for page in paginator.paginate(**page_params):
                for cp in page.get('CommonPrefixes', []):
                    subfolders.append(cp['Prefix'])
            return subfolders
        except ClientError as e:
            self.logger.error(f"Error listing common prefixes in {bucket_name} for '{prefix}': {e}")
            return []

    def _sum_prefix_size(self, bucket_name: str, prefix: str) -> Dict[str, Any]:
        """
        Sum sizes and count of all objects under a given prefix (recursive).
        """
        total_size = 0
        file_count = 0
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_params = {
                'Bucket': bucket_name,
                'Prefix': prefix
            }
            for page in paginator.paginate(**page_params):
                for obj in page.get('Contents', []):
                    total_size += obj['Size']
                    file_count += 1
        except ClientError as e:
            self.logger.error(f"Error summing prefix size in {bucket_name} for '{prefix}': {e}")

        return {
            'size_bytes': total_size,
            'size_mb': round(total_size / (1024 * 1024), 2),
            'size_gb': round(total_size / (1024 * 1024 * 1024), 2),
            'file_count': file_count
        }

    def get_folder_sizes(self, bucket_name: str, prefix: str = '') -> Dict[str, Any]:
        """
        Given a "folder" path (S3 prefix), return the size of all immediate
        sub-folders within it.

        Example:
            If prefix='data/', and bucket contains keys like
              data/a/file1, data/a/file2, data/b/file3
            This returns sizes for 'data/a/' and 'data/b/'.

        :param bucket_name: S3 bucket name
        :param prefix: Parent prefix (folder path)
        :return: {
            'bucket': <bucket>,
            'prefix': <normalized_prefix>,
            'folders': {
                '<child_prefix>': { size_bytes, size_mb, size_gb, file_count }, ...
            },
            'common_prefix_count': <int>
        }
        """
        normalized = self._normalize_prefix(prefix)
        folders = {}

        sub_prefixes = self._list_common_prefixes(bucket_name, normalized)
        total_size = 0
        for sub in sub_prefixes:
            folders[sub] = self._sum_prefix_size(bucket_name, sub)
            total_size += folders[sub]['size_gb']

        return {
            'bucket': bucket_name,
            'prefix': normalized,
            'folders': folders,
            'common_prefix_count': len(sub_prefixes),
            'total_size_gb': total_size
        }
