"""
AWS EC2 Library for TAF
Provides functionality to poll EC2 instances, filter by tags, SSH into machines, and run shell commands.
"""

import time
import boto3
from botocore.exceptions import ClientError
from typing import List, Dict, Optional, Any
import logging


class AWSBase:
    """Base class for AWS services with common functionality."""

    def __init__(self, access_key, secret_key, session_token=None, endpoint_url=None):
        """
        Initialize AWS base client.
        
        :param access_key: AWS access key
        :param secret_key: AWS secret key
        :param session_token: AWS session token (optional)
        :param endpoint_url: Custom endpoint URL (optional)
        """
        logging.basicConfig()
        logging.getLogger('boto3').setLevel(logging.ERROR)
        logging.getLogger('botocore').setLevel(logging.ERROR)
        self.logger = logging.getLogger("AWS_EC2_Util")
        self.endpoint_url = endpoint_url
        self.create_session(access_key, secret_key, session_token)

    def create_session(self, access_key, secret_key, session_token):
        """Create a session to AWS using the credentials provided."""
        try:
            if session_token:
                self.aws_session = boto3.Session(
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key,
                    aws_session_token=session_token)
            else:
                self.aws_session = boto3.Session(
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key)
        except Exception as e:
            self.logger.error(e)

    def create_service_client(self, service_name, region=None):
        """Create a low level client for the service specified."""
        try:
            if region is None:
                return self.aws_session.client(service_name, endpoint_url=self.endpoint_url)
            else:
                return self.aws_session.client(
                    service_name, region_name=region,
                    endpoint_url=self.endpoint_url)
        except ClientError as e:
            self.logger.error(e)

    def create_service_resource(self, resource_name, region=None):
        """Create a service resource object, to access resources related to service."""
        try:
            if region is None:
                return self.aws_session.resource(resource_name, endpoint_url=self.endpoint_url)
            else:
                return self.aws_session.resource(resource_name, region_name=region, endpoint_url=self.endpoint_url)
        except Exception as e:
            self.logger.error(e)


class EC2Lib(AWSBase):
    """
    EC2 library for managing EC2 instances, including polling, filtering by tags,
    SSH connections, and running shell commands.
    """

    def __init__(self, access_key, secret_key, session_token=None, region=None, endpoint_url=None):
        """
        Initialize EC2 client.
        
        :param access_key: AWS access key
        :param secret_key: AWS secret key
        :param session_token: AWS session token (optional)
        :param region: AWS region (optional, defaults to us-east-1)
        :param endpoint_url: Custom endpoint URL (optional)
        """
        super(EC2Lib, self).__init__(access_key, secret_key, session_token, endpoint_url)
        self.region = region or 'us-east-1'
        self.ec2_client = self.create_service_client(service_name="ec2", region=self.region)
        self.ec2_resource = self.create_service_resource(resource_name="ec2", region=self.region)

    def list_instances(self, instance_ids: List[str] = None, filters: List[Dict] = None) -> List[Dict]:
        """
        List EC2 instances with optional filtering.
        
        :param instance_ids: List of specific instance IDs to retrieve
        :param filters: List of filters to apply (e.g., [{'Name': 'instance-state-name', 'Values': ['running']}])
        :return: List of instance dictionaries
        """
        try:
            kwargs = {}
            if instance_ids:
                kwargs['InstanceIds'] = instance_ids
            if filters:
                kwargs['Filters'] = filters
                
            response = self.ec2_client.describe_instances(**kwargs)
            
            instances = []
            for reservation in response['Reservations']:
                for instance in reservation['Instances']:
                    instances.append(instance)
                    
            return instances
        except ClientError as e:
            self.logger.error(f"Error listing instances: {e}")
            return []

    def list_volumes_by_instance_id_with_tags(self, instance_id: str, tags: Dict[str, str] = None) -> List[Dict]:
        """
        List EBS volumes attached to an EC2 instance, filtered by tags.
        :param instance_id: The instance ID of the EC2 instance
        :param tags: Dictionary of tag key-value pairs to filter volumes (e.g., {'Name': 'my-volume', 'Environment': 'prod'})
        :return: List of volume dictionaries matching the instance ID and tags
        """
        try:
            filters = [{'Name': 'attachment.instance-id', 'Values': [instance_id]}]
            # Add tag filters if provided
            if tags:
                for key, value in tags.items():
                    filters.append({
                        'Name': f'tag:{key}',
                        'Values': [value]
                    })
            response = self.ec2_client.describe_volumes(Filters=filters)
            return response.get('Volumes', [])
        except Exception as e:
            self.logger.error(f"Error listing volumes by instance ID {instance_id} with tags: {e}")
            return []

    def get_instances_by_tags(self, tags: Dict[str, str]) -> List[Dict]:
        """
        Get EC2 instances filtered by tags.
        
        :param tags: Dictionary of tag key-value pairs to filter by
        :return: List of matching instance dictionaries
        """
        try:
            filters = []
            for key, value in tags.items():
                filters.append({
                    'Name': f'tag:{key}',
                    'Values': [value]
                })
            
            return self.list_instances(filters=filters)
        except Exception as e:
            self.logger.error(f"Error filtering instances by tags: {e}")
            return []

    def get_instance_by_id(self, instance_id: str) -> Optional[Dict]:
        """
        Get a specific EC2 instance by ID.
        
        :param instance_id: The instance ID to retrieve
        :return: Instance dictionary or None if not found
        """
        try:
            instances = self.list_instances(instance_ids=[instance_id])
            return instances[0] if instances else None
        except Exception as e:
            self.logger.error(f"Error getting instance {instance_id}: {e}")
            return None

    def get_instance_state(self, instance_id: str) -> Optional[str]:
        """
        Get the current state of an EC2 instance.
        
        :param instance_id: The instance ID to check
        :return: Instance state (e.g., 'running', 'stopped', 'pending') or None
        """
        try:
            instance = self.get_instance_by_id(instance_id)
            return instance['State']['Name'] if instance else None
        except Exception as e:
            self.logger.error(f"Error getting instance state for {instance_id}: {e}")
            return None

    def wait_for_instance_state(self, instance_id: str, target_state: str, timeout: int = 300) -> bool:
        """
        Wait for an instance to reach a specific state.
        
        :param instance_id: The instance ID to monitor
        :param target_state: The target state to wait for
        :param timeout: Maximum time to wait in seconds
        :return: True if target state reached, False if timeout
        """
        try:
            start_time = time.time()
            while time.time() - start_time < timeout:
                current_state = self.get_instance_state(instance_id)
                if current_state == target_state:
                    return True
                time.sleep(10)
            return False
        except Exception as e:
            self.logger.error(f"Error waiting for instance state: {e}")
            return False

    def start_instance(self, instance_id: str) -> bool:
        """
        Start an EC2 instance.
        
        :param instance_id: The instance ID to start
        :return: True if successful, False otherwise
        """
        try:
            response = self.ec2_client.start_instances(InstanceIds=[instance_id])
            return response['ResponseMetadata']['HTTPStatusCode'] == 200
        except ClientError as e:
            self.logger.error(f"Error starting instance {instance_id}: {e}")
            return False

    def stop_instance(self, instance_id: str) -> bool:
        """
        Stop an EC2 instance.
        
        :param instance_id: The instance ID to stop
        :return: True if successful, False otherwise
        """
        try:
            response = self.ec2_client.stop_instances(InstanceIds=[instance_id])
            return response['ResponseMetadata']['HTTPStatusCode'] == 200
        except ClientError as e:
            self.logger.error(f"Error stopping instance {instance_id}: {e}")
            return False

    def terminate_instance(self, instance_id: str) -> bool:
        """
        Terminate an EC2 instance.
        
        :param instance_id: The instance ID to terminate
        :return: True if successful, False otherwise
        """
        try:
            response = self.ec2_client.terminate_instances(InstanceIds=[instance_id])
            return response['ResponseMetadata']['HTTPStatusCode'] == 200
        except ClientError as e:
            self.logger.error(f"Error terminating instance {instance_id}: {e}")
            return False

    def get_instance_public_ip(self, instance_id: str) -> Optional[str]:
        """
        Get the public IP address of an EC2 instance.
        
        :param instance_id: The instance ID
        :return: Public IP address or None if not available
        """
        try:
            instance = self.get_instance_by_id(instance_id)
            return instance.get('PublicIpAddress') if instance else None
        except Exception as e:
            self.logger.error(f"Error getting public IP for {instance_id}: {e}")
            return None

    def get_instance_private_ip(self, instance_id: str) -> Optional[str]:
        """
        Get the private IP address of an EC2 instance.
        
        :param instance_id: The instance ID
        :return: Private IP address or None if not available
        """
        try:
            instance = self.get_instance_by_id(instance_id)
            return instance.get('PrivateIpAddress') if instance else None
        except Exception as e:
            self.logger.error(f"Error getting private IP for {instance_id}: {e}")
            return None

    def is_instance_ssm_ready(self, instance_id: str) -> bool:
        """
        Check if an instance is ready for SSM commands.
        
        :param instance_id: The instance ID to check
        :return: True if instance is SSM ready, False otherwise
        """
        try:
            ssm_client = self.create_service_client(service_name="ssm", region=self.region)
            
            # Check if instance is managed by SSM
            response = ssm_client.describe_instance_information(
                Filters=[
                    {
                        'Key': 'InstanceIds',
                        'Values': [instance_id]
                    }
                ]
            )
            
            if response['InstanceInformationList']:
                instance_info = response['InstanceInformationList'][0]
                return instance_info['PingStatus'] == 'Online'
            
            return False
        except Exception as e:
            self.logger.error(f"Error checking SSM readiness for {instance_id}: {e}")
            return False

    def wait_for_ssm_ready(self, instance_id: str, timeout: int = 300) -> bool:
        """
        Wait for an instance to be ready for SSM commands.
        
        :param instance_id: The instance ID to wait for
        :param timeout: Maximum time to wait in seconds
        :return: True if SSM ready, False if timeout
        """
        try:
            start_time = time.time()
            while time.time() - start_time < timeout:
                if self.is_instance_ssm_ready(instance_id):
                    return True
                time.sleep(10)
            return False
        except Exception as e:
            self.logger.error(f"Error waiting for SSM readiness for {instance_id}: {e}")
            return False

    def run_shell_command(self, instance_id: str, command: str, 
                         timeout: int = 30, document_name: str = 'AWS-RunShellScript',
                         check_ssm_ready: bool = True) -> Dict[str, Any]:
        """
        Run a shell command on an EC2 instance via AWS Systems Manager (SSM).
        
        :param instance_id: The instance ID to run the command on
        :param command: The shell command to execute
        :param timeout: Command timeout in seconds
        :param document_name: SSM document name (default: 'AWS-RunShellScript')
        :param check_ssm_ready: Whether to check if instance is SSM ready before executing
        :return: Dictionary with stdout, stderr, and return code
        """
        result = {
            'stdout': '',
            'stderr': '',
            'return_code': -1,
            'success': False
        }
        
        try:
            # Check if instance is SSM ready
            if check_ssm_ready and not self.is_instance_ssm_ready(instance_id):
                result['stderr'] = f"Instance {instance_id} is not ready for SSM commands. Please ensure SSM agent is installed and running."
                self.logger.error(result['stderr'])
                return result
            
            # Create SSM client
            ssm_client = self.create_service_client(service_name="ssm", region=self.region)
            
            # Send command via SSM
            response = ssm_client.send_command(
                InstanceIds=[instance_id],
                DocumentName=document_name,
                Parameters={
                    'commands': [command]
                },
                TimeoutSeconds=timeout
            )
            
            command_id = response['Command']['CommandId']
            
            # Wait for command to complete
            waiter = ssm_client.get_waiter('command_executed')
            waiter.wait(
                CommandId=command_id,
                InstanceId=instance_id,
                WaiterConfig={
                    'Delay': 5,
                    'MaxAttempts': timeout // 5
                }
            )
            
            # Get command output
            output_response = ssm_client.get_command_invocation(
                CommandId=command_id,
                InstanceId=instance_id
            )
            
            result['stdout'] = output_response.get('StandardOutputContent', '')
            result['stderr'] = output_response.get('StandardErrorContent', '')
            result['return_code'] = output_response.get('ResponseCode', -1)
            result['success'] = result['return_code'] == 0
            
        except Exception as e:
            result['stderr'] = f"Error running command on {instance_id} via SSM: {e}"
            self.logger.error(result['stderr'])
                
        return result

    def run_multiple_commands(self, instance_id: str, commands: List[str], 
                            timeout: int = 30, document_name: str = 'AWS-RunShellScript',
                            check_ssm_ready: bool = True) -> List[Dict[str, Any]]:
        """
        Run multiple shell commands on an EC2 instance via AWS Systems Manager (SSM).
        
        :param instance_id: The instance ID to run commands on
        :param commands: List of shell commands to execute
        :param timeout: Command timeout in seconds
        :param document_name: SSM document name (default: 'AWS-RunShellScript')
        :param check_ssm_ready: Whether to check if instance is SSM ready before executing
        :return: List of result dictionaries
        """
        results = []
        
        try:
            # Check if instance is SSM ready
            if check_ssm_ready and not self.is_instance_ssm_ready(instance_id):
                # Return failed results for all commands
                for command in commands:
                    results.append({
                        'command': command,
                        'stdout': '',
                        'stderr': f"Instance {instance_id} is not ready for SSM commands. Please ensure SSM agent is installed and running.",
                        'return_code': -1,
                        'success': False
                    })
                return results
            
            # Create SSM client
            ssm_client = self.create_service_client(service_name="ssm", region=self.region)
            
            # Send command via SSM with all commands
            response = ssm_client.send_command(
                InstanceIds=[instance_id],
                DocumentName=document_name,
                Parameters={
                    'commands': commands
                },
                TimeoutSeconds=timeout
            )
            
            command_id = response['Command']['CommandId']
            
            # Wait for command to complete
            waiter = ssm_client.get_waiter('command_executed')
            waiter.wait(
                CommandId=command_id,
                InstanceId=instance_id,
                WaiterConfig={
                    'Delay': 5,
                    'MaxAttempts': timeout // 5
                }
            )
            
            # Get command output
            output_response = ssm_client.get_command_invocation(
                CommandId=command_id,
                InstanceId=instance_id
            )
            
            # Parse the combined output for individual commands
            stdout_content = output_response.get('StandardOutputContent', '')
            stderr_content = output_response.get('StandardErrorContent', '')
            return_code = output_response.get('ResponseCode', -1)
            
            # Split output by commands (assuming each command output is separated by newlines)
            stdout_lines = stdout_content.split('\n') if stdout_content else []
            stderr_lines = stderr_content.split('\n') if stderr_content else []
            
            # Create results for each command
            for i, command in enumerate(commands):
                result = {
                    'command': command,
                    'stdout': stdout_lines[i] if i < len(stdout_lines) else '',
                    'stderr': stderr_lines[i] if i < len(stderr_lines) else '',
                    'return_code': return_code,
                    'success': return_code == 0
                }
                results.append(result)
                
        except Exception as e:
            self.logger.error(f"Error in run_multiple_commands: {e}")
            # Return failed results for all commands
            for command in commands:
                results.append({
                    'command': command,
                    'stdout': '',
                    'stderr': f"Error running multiple commands via SSM: {e}",
                    'return_code': -1,
                    'success': False
                })
                
        return results

    def poll_instances(self, instance_ids: List[str], poll_interval: int = 30, 
                      max_polls: int = 10, target_state: str = 'running') -> Dict[str, bool]:
        """
        Poll multiple instances until they reach a target state or max polls reached.
        
        :param instance_ids: List of instance IDs to poll
        :param poll_interval: Time between polls in seconds
        :param max_polls: Maximum number of polls to perform
        :param target_state: Target state to wait for
        :return: Dictionary mapping instance IDs to success status
        """
        results = {}
        poll_count = 0
        
        while poll_count < max_polls:
            all_ready = True
            
            for instance_id in instance_ids:
                if instance_id not in results or not results[instance_id]:
                    current_state = self.get_instance_state(instance_id)
                    if current_state == target_state:
                        results[instance_id] = True
                    elif current_state in ['terminated', 'shutting-down']:
                        results[instance_id] = False
                    else:
                        all_ready = False
                        results[instance_id] = False
            
            if all_ready:
                break
                
            poll_count += 1
            if poll_count < max_polls:
                time.sleep(poll_interval)
        
        return results

    def poll_instances_by_tag(self, tags: Dict[str, str], poll_interval: int = 30, 
                            max_polls: int = 10, target_state: str = 'running') -> Dict[str, Dict[str, Any]]:
        """
        Poll instances filtered by tags until they reach a target state or max polls reached.
        
        :param tags: Dictionary of tag key-value pairs to filter instances
        :param poll_interval: Time between polls in seconds
        :param max_polls: Maximum number of polls to perform
        :param target_state: Target state to wait for
        :return: Dictionary mapping instance IDs to detailed status information
        """
        results = {}
        poll_count = 0
        
        while poll_count < max_polls:
            # Get instances matching the tags
            instances = self.get_instances_by_tags(tags)
            instance_ids = [instance['InstanceId'] for instance in instances]
            
            all_ready = True
            current_poll_results = {}
            
            for instance in instances:
                instance_id = instance['InstanceId']
                current_state = instance['State']['Name']
                
                status_info = {
                    'instance_id': instance_id,
                    'current_state': current_state,
                    'target_state': target_state,
                    'reached_target': current_state == target_state,
                    'poll_count': poll_count + 1,
                    'public_ip': instance.get('PublicIpAddress'),
                    'private_ip': instance.get('PrivateIpAddress'),
                    'tags': {tag['Key']: tag['Value'] for tag in instance.get('Tags', [])}
                }
                
                if current_state == target_state:
                    status_info['success'] = True
                    current_poll_results[instance_id] = status_info
                elif current_state in ['terminated', 'shutting-down']:
                    status_info['success'] = False
                    status_info['error'] = f"Instance terminated before reaching {target_state}"
                    current_poll_results[instance_id] = status_info
                else:
                    status_info['success'] = False
                    all_ready = False
                    current_poll_results[instance_id] = status_info
            
            # Update results with current poll information
            for instance_id, status_info in current_poll_results.items():
                if instance_id not in results or not results[instance_id].get('success', False):
                    results[instance_id] = status_info
            
            if all_ready:
                break
                
            poll_count += 1
            if poll_count < max_polls:
                time.sleep(poll_interval)
        
        # Add final summary
        for instance_id, status_info in results.items():
            status_info['final_poll_count'] = poll_count
            status_info['max_polls_reached'] = poll_count >= max_polls
        
        return results

    def poll_instances_by_tag_with_ssm(self, tags: Dict[str, str], poll_interval: int = 30, 
                                     max_polls: int = 10, target_state: str = 'running',
                                     ssm_ready: bool = True) -> Dict[str, Dict[str, Any]]:
        """
        Poll instances filtered by tags and optionally check SSM readiness.
        
        :param tags: Dictionary of tag key-value pairs to filter instances
        :param poll_interval: Time between polls in seconds
        :param max_polls: Maximum number of polls to perform
        :param target_state: Target state to wait for
        :param ssm_ready: Whether to also check SSM readiness
        :return: Dictionary mapping instance IDs to detailed status information
        """
        results = {}
        poll_count = 0
        
        while poll_count < max_polls:
            # Get instances matching the tags
            instances = self.get_instances_by_tags(tags)
            instance_ids = [instance['InstanceId'] for instance in instances]
            
            all_ready = True
            current_poll_results = {}
            
            for instance in instances:
                instance_id = instance['InstanceId']
                current_state = instance['State']['Name']
                
                status_info = {
                    'instance_id': instance_id,
                    'current_state': current_state,
                    'target_state': target_state,
                    'reached_target': current_state == target_state,
                    'poll_count': poll_count + 1,
                    'public_ip': instance.get('PublicIpAddress'),
                    'private_ip': instance.get('PrivateIpAddress'),
                    'tags': {tag['Key']: tag['Value'] for tag in instance.get('Tags', [])}
                }
                
                # Check SSM readiness if requested
                if ssm_ready and current_state == target_state:
                    ssm_ready_status = self.is_instance_ssm_ready(instance_id)
                    status_info['ssm_ready'] = ssm_ready_status
                    status_info['ssm_ready_message'] = "SSM ready" if ssm_ready_status else "SSM not ready"
                else:
                    status_info['ssm_ready'] = None
                    status_info['ssm_ready_message'] = "Not checked"
                
                if current_state == target_state:
                    if ssm_ready and not status_info.get('ssm_ready', True):
                        status_info['success'] = False
                        status_info['error'] = f"Instance reached {target_state} but SSM not ready"
                        all_ready = False
                    else:
                        status_info['success'] = True
                    current_poll_results[instance_id] = status_info
                elif current_state in ['terminated', 'shutting-down']:
                    status_info['success'] = False
                    status_info['error'] = f"Instance terminated before reaching {target_state}"
                    current_poll_results[instance_id] = status_info
                else:
                    status_info['success'] = False
                    all_ready = False
                    current_poll_results[instance_id] = status_info
            
            # Update results with current poll information
            for instance_id, status_info in current_poll_results.items():
                if instance_id not in results or not results[instance_id].get('success', False):
                    results[instance_id] = status_info
            
            if all_ready:
                break
                
            poll_count += 1
            if poll_count < max_polls:
                time.sleep(poll_interval)
        
        # Add final summary
        for instance_id, status_info in results.items():
            status_info['final_poll_count'] = poll_count
            status_info['max_polls_reached'] = poll_count >= max_polls
        
        return results

    def get_ebs_volume_by_id(self, volume_id: str) -> Optional[Dict[str, Any]]:
        """
        Get an EBS volume by its ID.
        Returns a volume dict.
        """
        try:
            volume = self.ec2_client.describe_volumes(VolumeIds=[volume_id])
            return volume.get('Volumes', [])[0]
        except Exception as e:
            self.logger.error(f"Error getting volume by id {volume_id}: {e}")
            return None

    def list_volumes_by_cluster_id(self, filters: Dict[str, str] = None) -> List[Dict[str, Any]]:
        """
        List EBS volumes filtered by the cluster id tag ('couchbase-cloud-cluster-id').
        Returns a list of volume dicts.
        """
        if filters is None:
            _filters = []
        else:
            _filters = [{
                'Name': f'tag:{key}', 'Values': [value]
            } for key, value in filters.items() if key != 'iops']
        if 'iops' in filters:
            _filters.append({'Name': 'iops', 'Values': [str(filters['iops'])]})
        volumes = self.ec2_client.describe_volumes(Filters=_filters)
        return volumes.get('Volumes', [])

    def poll_volumes_by_cluster_id(self, cluster_id: str, poll_interval: int = 30, max_polls: int = 10) -> List[Dict[str, Any]]:
        """
        Poll for EBS volumes filtered by the cluster id tag ('couchbase-cloud-cluster-id').
        Returns a list of volume dicts.
        """
        filters = [
            {'Name': 'tag:couchbase-cloud-cluster-id', 'Values': [str(cluster_id)]},
            {'Name': 'tag:couchbase-cloud-function', 'Values': ['fusion-accelerator']}
        ]
        poll_count = 0
        last_volumes = []
        while poll_count < max_polls:
            try:
                response = self.ec2_client.describe_volumes(Filters=filters)
                volumes = response.get('Volumes', [])
                print(f"[poll_volumes_by_cluster_id] Poll {poll_count+1}/{max_polls} - Found {len(volumes)} volumes")
                for volume in volumes:
                    volume_id = volume.get('VolumeId')
                    size = volume.get('Size')
                    state = volume.get('State')
                    iops = volume.get('Iops', 'N/A')
                    attachments = volume.get('Attachments', [])
                    device = attachments[0]['Device'] if attachments else 'N/A'
                    instance_id = attachments[0]['InstanceId'] if attachments else 'N/A'
                    print(f"  VolumeId: {volume_id}, Size: {size} GiB, IOPS: {iops}, State: {state}, Instance: {instance_id}, Device: {device}")
                last_volumes = volumes
            except Exception as e:
                self.logger.error(f"Error polling volumes by cluster id {cluster_id}: {e}")
            poll_count += 1
            if poll_count < max_polls:
                time.sleep(poll_interval)
        return last_volumes

    def get_hostname_public_ip_mapping(self, dns_name:str="l3ocuqshernpf2ih.sandbox.nonprod-project-avengers.com") -> Optional[str]:
        """
        Get the hostname and public IP mapping of an EC2 instance.
        Returns a dictionary with the hostname and public IP of the instance.
        """
        # Get the hostname and public IP mapping from AWS Route53 hosted zones for the given instance.
        try:
            route53_client = self.create_service_client(service_name="route53", region=self.region)
            # Get all hosted zones
            hosted_zones = route53_client.list_hosted_zones_by_name(DNSName=dns_name).get('HostedZones', [])
            for zone in hosted_zones:
                zone_id = zone['Id'].split('/')[-1]
                # List all resource record sets in the zone
                record_sets_paginator = route53_client.get_paginator('list_resource_record_sets')
                for page in record_sets_paginator.paginate(HostedZoneId=zone_id):
                    record_data = dict()
                    for record in page.get('ResourceRecordSets', []):
                        if record.get('Name', '').rstrip('.').find("svc-") != -1:
                            record_data[record.get('Name')] = record.get('ResourceRecords', [])[0].get('Value')
                    if record_data:
                        return record_data
        except Exception as e:
            self.logger.error(f"Error getting hostname/public IP mapping for record {dns_name}: {e}")
            return {}
        
    def list_asgs(self, filters: List[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        List Auto Scaling Groups (ASGs) filtered by tags.

        :param filters: AWS-style filters in format [{'Name': 'tag:key', 'Values': ['value']}]
        Returns a list of ASG dicts.
        """
        try:
            asg_client = self.create_service_client(service_name="autoscaling", region=self.region)

            # Use pagination with NextToken to get all ASGs matching the specified tags
            asgs = []
            next_token = None

            while True:
                kwargs = {}
                if filters:
                    kwargs['Filters'] = filters
                if next_token:
                    kwargs['NextToken'] = next_token

                response = asg_client.describe_auto_scaling_groups(**kwargs)
                asgs.extend(response.get('AutoScalingGroups', []))

                next_token = response.get('NextToken')
                if not next_token:
                    break

            self.logger.info(f"Found {len(asgs)} fusion ASGs matching filters {filters}")
            return asgs
        except Exception as e:
            self.logger.error(f"Error listing ASGs with filters {filters}: {e}")
            return []