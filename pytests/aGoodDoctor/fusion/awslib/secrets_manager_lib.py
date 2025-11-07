"""
AWS Secrets Manager Library for TAF
Provides functionality to filter secrets by tags and fetch secret values.
"""

import boto3
from botocore.exceptions import ClientError
from typing import List, Dict, Optional, Any
import logging
import json


class SecretsManagerLib:
    """
    Secrets Manager library for managing AWS secrets, including filtering by tags
    and fetching secret values.
    """

    def __init__(self, access_key, secret_key, session_token=None, region=None, endpoint_url=None):
        """
        Initialize Secrets Manager client.
        
        :param access_key: AWS access key
        :param secret_key: AWS secret key
        :param session_token: AWS session token (optional)
        :param region: AWS region (optional, defaults to us-east-1)
        :param endpoint_url: Custom endpoint URL (optional)
        """
        logging.basicConfig()
        logging.getLogger('boto3').setLevel(logging.ERROR)
        logging.getLogger('botocore').setLevel(logging.ERROR)
        self.logger = logging.getLogger("AWS_SecretsManager_Util")
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
        
        # Create Secrets Manager client
        self.secrets_client = self.aws_session.client(
            'secretsmanager',
            region_name="us-east-1",
            endpoint_url=self.endpoint_url
        )

    def get_secret_by_name(self, secret_name: str) -> Dict[str, Any]:
        """
        Get a secret by name.
        
        :param secret_name: Name of the secret
        :return: Secret dictionary with metadata
        """
        try:
            response = self.secrets_client.get_secret_value(SecretId=secret_name)
            return {
                'Name': secret_name,
                'SecretValue': response.get('SecretString', ''),
                'SecretBinary': response.get('SecretBinary'),
                'VersionId': response.get('VersionId'),
                'VersionStages': response.get('VersionStages', []),
                'CreatedDate': response.get('CreatedDate'),
                'ARN': response.get('ARN'),
                'success': True
            }
        except ClientError as e:
            self.logger.error(f"Error getting secret by name {secret_name}: {e}")
            return {
                'secret_name': secret_name,
                'secret_value': '',
                'secret_binary': None,
                'success': False,
                'error': str(e)
            }

    def get_secret_by_tag(self, tag_key: str, tag_value: str = None) -> Dict[str, Any]:
        """
        List secrets filtered by a tag key (and optionally tag value), and retrieve their details
        using 'get_secret_value' via secret id.

        :param tag_key: Key of the tag to filter by
        :param tag_value: Value of the tag to filter by
        :return: Secret dictionary with secret value (if readable)
        """
        try:
            results = []
            paginator = self.secrets_client.get_paginator('list_secrets')
            for page in paginator.paginate():
                for secret in page.get('SecretList', []):
                    tags_dict = {tag['Key']: tag['Value'] for tag in secret.get('Tags', [])}
                    self.logger.debug(f"Tags: {tags_dict}")
                    if tag_key:
                        if tag_value is not None:
                            if tags_dict.get(tag_key) != tag_value:
                                continue
                        else:
                            if tag_key not in tags_dict:
                                continue
                    secret_info = {
                        'Name': secret['Name'],
                        'Description': secret.get('Description', ''),
                        'ARN': secret['ARN'],
                        'CreatedDate': secret.get('CreatedDate'),
                        'LastChangedDate': secret.get('LastChangedDate'),
                        'LastAccessedDate': secret.get('LastAccessedDate'),
                        'Tags': tags_dict,
                        'SecretVersionsToStages': secret.get('SecretVersionsToStages', {}),
                        'DeletedDate': secret.get('DeletedDate'),
                        'KmsKeyId': secret.get('KmsKeyId'),
                    }
                    # Try to fetch the secret value using 'get_secret_value'
                    try:
                        value_resp = self.secrets_client.get_secret_value(SecretId=secret['Name'])
                        # The secret value could be under 'SecretString' or (rarely) 'SecretBinary'
                        if 'SecretString' in value_resp:
                            secret_info['SecretValue'] = value_resp['SecretString']
                        elif 'SecretBinary' in value_resp:
                            secret_info['SecretValue'] = value_resp['SecretBinary']
                        else:
                            secret_info['SecretValue'] = None
                    except ClientError as ce:
                        self.logger.error(f"Unable to fetch value for secret {secret['Name']}: {ce}")
                        secret_info['SecretValue'] = None
                    results.append(secret_info)
            return results
        except ClientError as e:
            self.logger.error(f"Error listing secrets: {e}")
            return []
