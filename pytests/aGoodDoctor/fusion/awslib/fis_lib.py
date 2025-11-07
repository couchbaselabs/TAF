"""
AWS FIS (Fault Injection Simulator) Library for TAF
Provides functionality to create and manage FIS experiments for testing 
fusion accelerator compute node fallback logic and EBS mount failures.

Usage Example:
    from fis_lib import FISLib
    
    fis = FISLib(access_key, secret_key, region='us-east-1')
    
    # Create experiment template for compute node fallback testing
    template_id = fis.create_compute_failure_experiment(
        experiment_name='fusion-accelerator-fallback-test',
        instance_id='i-1234567890abcdef0',
        stop_duration=60
    )
    
    # Start the experiment
    experiment_id = fis.start_experiment(template_id)
    
    # Monitor experiment
    fis.wait_for_experiment_completion(experiment_id, timeout=900)
"""

import time
import boto3
from botocore.exceptions import ClientError
from typing import List, Dict, Optional, Any
import logging
import json


class FISLib:
    """
    AWS Fault Injection Simulator (FIS) library for creating and managing
    fault injection experiments to test system resilience.
    
    Specifically designed for testing fusion accelerator compute node
    fallback logic and EBS mount failure scenarios.
    """

    def __init__(self, access_key: str, secret_key: str, session_token: str = None, 
                 region: Optional[str] = None, endpoint_url: Optional[str] = None):
        """
        Initialize FIS client.
        
        :param access_key: AWS access key
        :param secret_key: AWS secret key
        :param session_token: AWS session token (optional)
        :param region: AWS region (optional, defaults to us-east-1)
        :param endpoint_url: Custom endpoint URL (optional)
        """
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
            raise RuntimeError(f"Failed to create AWS session: {e}")
        
        self.region = region or 'us-east-1'
        self.logger = logging.getLogger("FIS_Lib")
        self.logger.setLevel(logging.INFO)
        
        try:
            self.fis_client = self.aws_session.client(
                'fis', region_name=self.region, endpoint_url=endpoint_url
            )
            # Initialize EC2 client for instance operations
            self.ec2_client = self.aws_session.client(
                'ec2', region_name=self.region, endpoint_url=endpoint_url
            )
        except Exception as e:
            raise RuntimeError(f"Failed to create FIS/EC2 clients: {e}")

    def create_compute_failure_experiment(
        self,
        experiment_name: str,
        instance_ids: List[str],
        stop_duration: int = 60,
        stop_before_creating: bool = True,
        tags: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Create an FIS experiment template for computing node termination testing.
        
        This experiment simulates compute node failures to test the fusion
        accelerator fallback logic where each accelerator node has a list of
        compute nodes. If one isn't available, the next in the list is used.
        
        The experiment uses the aws:ec2:stop-instances action to gracefully
        stop instances, allowing the fallback mechanism to kick in.
        
        :param experiment_name: Name for the experiment template
        :param instance_ids: List of instance IDs to target for failure injection
        :param stop_duration: Duration in seconds to keep instances stopped (default: 60)
        :param stop_before_creating: Whether to stop instances before creating experiment
        :param tags: Optional tags to add to the experiment template
        :return: Experiment template ID
        
        :raises ClientError: If experiment creation fails
        :raises ValueError: If instance_ids is empty or invalid
        
        Example:
            >>> fis = FISLib('key', 'secret', 'us-east-1')
            >>> template_id = fis.create_compute_failure_experiment(
            ...     experiment_name='fusion-fallback-test-arm-nodes',
            ...     instance_ids=['i-1234567890abcdef0', 'i-0987654321fedcba0'],
            ...     stop_duration=120,
            ...     tags={'Purpose': 'fallback-testing', 'Team': 'fusion-team'}
            ... )
            >>> print(template_id)
            'exp-1234567890abcdef0'
        """
        if not instance_ids:
            raise ValueError("instance_ids cannot be empty")
        
        self.logger.info(f"Creating compute failure experiment: {experiment_name}")
        self.logger.info(f"Target instances: {instance_ids}")
        self.logger.info(f"Stop duration: {stop_duration}s")
        
        try:
            # Stop instances if requested
            if stop_before_creating:
                self.logger.info("Stopping instances before creating experiment...")
                self.stop_instances(instance_ids)
                
                # Wait for instances to be stopped
                self.wait_for_instance_state(
                    instance_ids, 'stopped', timeout=300
                )
            
            # Create target resource filters
            resource_filter = {
                "resourceType": "aws:ec2:instance",
                "resourceTags": [],
                "resourceIds": instance_ids,
                "selectionMode": "ALL"
            }
            
            # Define experiment actions
            actions = {
                "stop_instances": {
                    "actionId": "aws:ec2:stop-instances",
                    "parameters": {
                        "forceStopInstances": "false"
                    },
                    "targets": {
                        "Instances": "target-cluster"
                    }
                }
            }
            
            # Define experiment targets
            targets = {
                "target-cluster": resource_filter
            }
            
            # Define experiment stop conditions
            stop_conditions = [
                {
                    "source": "aws:cloudwatch:alarm",
                    "value": "arn:aws:cloudwatch:*:*:alarm:fis-experiment-aborted"
                }
            ]
            
            # Create experiment template
            response = self.fis_client.create_experiment_template(
                description=f"Compute failure injection for fusion accelerator fallback logic testing. Targets: {len(instance_ids)} instances.",
                stopConditions=stop_conditions,
                targets=targets,
                actions=actions,
                roleArn=self._get_fis_experiment_role_arn(),
                tags=tags or {}
            )
            
            template_id = response['experimentTemplate']['id']
            self.logger.info(f"Successfully created experiment template: {template_id}")
            
            return template_id
            
        except ClientError as e:
            self.logger.error(f"Failed to create experiment template: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error creating experiment: {e}")
            raise RuntimeError(f"Failed to create experiment: {e}")

    def start_experiment(
        self,
        template_id: str,
        experiment_name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Start an FIS experiment from a template.
        
        :param template_id: ID of the experiment template to start
        :param experiment_name: Optional name for the experiment instance
        :param tags: Optional tags to add to the experiment
        :return: Experiment ID
        
        :raises ClientError: If experiment start fails
        
        Example:
            >>> fis = FISLib('key', 'secret', 'us-east-1')
            >>> experiment_id = fis.start_experiment(
            ...     template_id='ext-1234567890abcdef0',
            ...     experiment_name='fusion-fallback-test-run-1'
            ... )
            >>> print(experiment_id)
            'exp-0987654321fedcba0'
        """
        self.logger.info(f"Starting experiment from template: {template_id}")
        
        try:
            response = self.fis_client.start_experiment(
                experimentTemplateId=template_id,
                experimentToken=str(int(time.time())),  # Unique token
                tags=tags or {}
            )
            
            experiment_id = response['experiment']['id']
            self.logger.info(f"Successfully started experiment: {experiment_id}")
            
            return experiment_id
            
        except ClientError as e:
            self.logger.error(f"Failed to start experiment: {e}")
            raise

    def get_experiment_status(self, experiment_id: str) -> Dict[str, Any]:
        """
        Get the status of an FIS experiment.
        
        :param experiment_id: ID of the experiment to query
        :return: Dictionary containing experiment status and details
        
        :raises ClientError: If experiment status query fails
        
        Example:
            >>> fis = FISLib('key', 'secret', 'us-east-1')
            >>> status = fis.get_experiment_status('exp-1234567890abcdef0')
            >>> print(status['status'])
            'running'
        """
        try:
            response = self.fis_client.get_experiment(
                id=experiment_id
            )
            
            experiment = response['experiment']
            status_info = {
                'id': experiment['id'],
                'name': experiment.get('name', 'N/A'),
                'status': experiment['status'],
                'state': experiment.get('state', 'N/A'),
                'startTime': experiment.get('startTime'),
                'endTime': experiment.get('endTime'),
                'actions': experiment.get('actions', {}),
                'targets': experiment.get('targets', {})
            }
            
            self.logger.debug(f"Experiment status: {status_info}")
            return status_info
            
        except ClientError as e:
            self.logger.error(f"Failed to get experiment status: {e}")
            raise

    def wait_for_experiment_completion(
        self,
        experiment_id: str,
        timeout: int = 900,
        poll_interval: int = 10
    ) -> Dict[str, Any]:
        """
        Wait for an FIS experiment to complete.
        
        :param experiment_id: ID of the experiment to wait for
        :param timeout: Maximum wait time in seconds (default: 900 = 15min)
        :param poll_interval: Time between status checks in seconds (default: 10)
        :return: Final experiment status dictionary
        
        :raises TimeoutError: If experiment doesn't complete within timeout
        :raises ClientError: If experiment status query fails
        
        Example:
            >>> fis = FISLib('key', 'secret', 'us-east-1')
            >>> final_status = fis.wait_for_experiment_completion(
            ...     'exp-1234567890abcdef0',
            ...     timeout=600,
            ...     poll_interval=15
            ... )
            >>> print(f"Final status: {final_status['status']}")
        """
        start_time = time.time()
        completed_states = ['completed', 'stopped', 'failed']
        
        self.logger.info(f"Waiting for experiment {experiment_id} to complete...")
        
        while time.time() - start_time < timeout:
            try:
                status = self.get_experiment_status(experiment_id)
                current_status = status['status']
                
                if current_status.lower() in completed_states:
                    self.logger.info(f"Experiment completed with status: {current_status}")
                    return status
                
                self.logger.debug(f"Experiment status: {current_status}")
                time.sleep(poll_interval)
                
            except ClientError as e:
                self.logger.error(f"Error checking experiment status: {e}")
                time.sleep(poll_interval)
        
        raise TimeoutError(
            f"Experiment {experiment_id} did not complete within {timeout}s"
        )

    def stop_instances(
        self,
        instance_ids: List[str],
        force: bool = False
    ) -> bool:
        """
        Stop EC2 instances as part of fault injection.
        
        :param instance_ids: List of instance IDs to stop
        :param force: Whether to force stop instances (default: False)
        :return: True if successful
        
        :raises ClientError: If instance stop operation fails
        
        Example:
            >>> fis = FISLib('key', 'secret', 'us-east-1')
            >>> fis.stop_instances(['i-1234567890abcdef0'], force=False)
        """
        self.logger.info(f"Stopping instances: {instance_ids}")
        
        try:
            response = self.ec2_client.stop_instances(
                InstanceIds=instance_ids,
                Force=force
            )
            
            self.logger.info(f"Successfully initiated stop for {len(instance_ids)} instances")
            return True
            
        except ClientError as e:
            self.logger.error(f"Failed to stop instances: {e}")
            raise

    def start_instances(
        self,
        instance_ids: List[str]
    ) -> bool:
        """
        Start EC2 instances after fault injection is complete.
        
        :param instance_ids: List of instance IDs to start
        :return: True if successful
        
        :raises ClientError: If instance start operation fails
        
        Example:
            >>> fis = FISLib('key', 'secret', 'us-east-1')
            >>> fis.start_instances(['i-1234567890abcdef0'])
        """
        self.logger.info(f"Starting instances: {instance_ids}")
        
        try:
            response = self.ec2_client.start_instances(
                InstanceIds=instance_ids
            )
            
            self.logger.info(f"Successfully initiated start for {len(instance_ids)} instances")
            return True
            
        except ClientError as e:
            self.logger.error(f"Failed to start instances: {e}")
            raise

    def wait_for_instance_state(
        self,
        instance_ids: List[str],
        desired_state: str,
        timeout: int = 300,
        poll_interval: int = 5
    ) -> bool:
        """
        Wait for instances to reach a desired state.
        
        :param instance_ids: List of instance IDs to monitor
        :param desired_state: Desired state ('running', 'stopped', etc.)
        :param timeout: Maximum wait time in seconds (default: 300 = 5min)
        :param poll_interval: Time between status checks in seconds (default: 5)
        :return: True if all instances reached desired state
        
        :raises TimeoutError: If instances don't reach desired state within timeout
        
        Example:
            >>> fis = FISLib('key', 'secret', 'us-east-1')
            >>> fis.wait_for_instance_state(
            ...     ['i-1234567890abcdef0'],
            ...     'stopped',
            ...     timeout=120
            ... )
        """
        start_time = time.time()
        
        self.logger.info(f"Waiting for instances {instance_ids} to reach state: {desired_state}")
        
        while time.time() - start_time < timeout:
            try:
                response = self.ec2_client.describe_instance_status(
                    InstanceIds=instance_ids
                )
                
                all_desired = True
                for status in response['InstanceStatuses']:
                    current_state = status['InstanceState']['Name']
                    if current_state != desired_state:
                        all_desired = False
                        break
                
                if all_desired:
                    self.logger.info(f"All instances reached state: {desired_state}")
                    return True
                
                time.sleep(poll_interval)
                
            except ClientError as e:
                self.logger.error(f"Error checking instance status: {e}")
                time.sleep(poll_interval)
        
        raise TimeoutError(
            f"Instances {instance_ids} did not reach state {desired_state} within {timeout}s"
        )

    def list_experiment_templates(
        self,
        filters: Optional[Dict[str, Any]] = None,
        max_results: int = 50
    ) -> List[Dict[str, Any]]:
        """
        List FIS experiment templates.
        
        :param filters: Optional filters to apply
        :param max_results: Maximum number of results to return (default: 50)
        :return: List of experiment template dictionaries
        
        :raises ClientError: If list operation fails
        
        Example:
            >>> fis = FISLib('key', 'secret', 'us-east-1')
            >>> templates = fis.list_experiment_templates(
            ...     filters={'tags': {'Purpose': 'fallback-testing'}}
            ... )
            >>> for template in templates:
            ...     print(f"Template: {template['id']} - {template['description']}")
        """
        self.logger.info("Listing experiment templates...")
        
        try:
            response = self.fis_client.list_experiment_templates(
                maxResults=max_results
            )
            
            templates = []
            for template in response['experimentTemplates']:
                templates.append({
                    'id': template['id'],
                    'description': template.get('description', 'N/A'),
                    'state': template['state'],
                    'creationTime': template.get('creationTime'),
                    'lastUpdateTime': template.get('lastUpdateTime'),
                    'tags': template.get('tags', {})
                })
            
            self.logger.info(f"Found {len(templates)} experiment templates")
            return templates
            
        except ClientError as e:
            self.logger.error(f"Failed to list experiment templates: {e}")
            raise

    def delete_experiment_template(self, template_id: str) -> bool:
        """
        Delete an FIS experiment template.
        
        :param template_id: ID of the template to delete
        :return: True if successful
        
        :raises ClientError: If deletion fails
        
        Example:
            >>> fis = FISLib('key', 'secret', 'us-east-1')
            >>> fis.delete_experiment_template('ext-1234567890abcdef0')
        """
        self.logger.info(f"Deleting experiment template: {template_id}")
        
        try:
            response = self.fis_client.delete_experiment_template(
                id=template_id
            )
            
            self.logger.info(f"Successfully deleted experiment template: {template_id}")
            return True
            
        except ClientError as e:
            self.logger.error(f"Failed to delete experiment template: {e}")
            raise

    def _get_fis_experiment_role_arn(self) -> str:
        """
        Get the ARN of the IAM role used for FIS experiments.
        
        This method looks for a role named 'AWSServiceRoleForFIS' which is
        the standard service-linked role for AWS FIS. If not found, it will
        construct a standard role ARN.
        
        :return: IAM role ARN string
        """
        # Standard service-linked role for FIS
        # Account ID is needed to construct the ARN
        try:
            sts_client = self.aws_session.client('sts', region_name=self.region)
            response = sts_client.get_caller_identity()
            account_id = response['Account']
            
            # Standard FIS service-linked role format
            return f"arn:aws:iam::{account_id}:role/AWSServiceRoleForFIS"
            
        except Exception as e:
            self.logger.warning(f"Could not get account ID for FIS role: {e}")
            # Return a generic role ARN - this may need to be adjusted based on setup
            return "arn:aws:iam::*:role/AWSServiceRoleForFIS"

    def get_instance_architecture(self, instance_id: str) -> str:
        """
        Get the architecture type of an EC2 instance.
        
        Useful for testing ARM vs x86 fallback logic in fusion accelerators.
        
        :param instance_id: ID of the instance to query
        :return: Architecture type ('x86_64', 'arm64', etc.)
        
        :raises ClientError: If instance description query fails
        """
        try:
            response = self.ec2_client.describe_instances(
                InstanceIds=[instance_id]
            )
            
            instance = response['Reservations'][0]['Instances'][0]
            architecture = instance.get('Architecture', 'unknown')
            
            self.logger.info(f"Instance {instance_id} architecture: {architecture}")
            return architecture
            
        except ClientError as e:
            self.logger.error(f"Failed to get instance architecture: {e}")
            raise

    def filter_instances_by_architecture(
        self,
        instance_ids: List[str],
        architecture: str = 'arm64'
    ) -> List[str]:
        """
        Filter instances by their architecture type.
        
        Useful for separating ARM and x86 instances for targeted fallback testing.
        
        :param instance_ids: List of instance IDs to filter
        :param architecture: Architecture to filter by (default: 'arm64')
        :return: List of instance IDs matching the architecture
        
        Example:
            >>> fis = FISLib('key', 'secret', 'us-east-1')
            >>> arm_instances = fis.filter_instances_by_architecture(
            ...     ['i-1234567890abcdef0', 'i-0987654321fedcba0'],
            ...     architecture='arm64'
            ... )
            >>> print(f"Found {len(arm_instances)} ARM instances")
        """
        filtered = []
        for instance_id in instance_ids:
            try:
                arch = self.get_instance_architecture(instance_id)
                if arch == architecture:
                    filtered.append(instance_id)
            except Exception as e:
                self.logger.warning(f"Could not determine architecture for {instance_id}: {e}")
                continue
        
        self.logger.info(f"Filtered {len(filtered)} instances with architecture: {architecture}")
        return filtered

    def create_architecture_fallback_test(
        self,
        experiment_name_base: str,
        all_instance_ids: List[str],
        stop_duration: int = 60
    ) -> Dict[str, str]:
        """
        Create separate FIS experiment templates for each architecture type
        to test fusion accelerator fallback logic across ARM and x86 compute nodes.
        
        This method automatically groups instances by architecture and creates
        separate experiments for each group, enabling comprehensive testing of
        fallback logic where accelerators try nodes from their preferred architecture
        list before falling back to other architectures.
        
        :param experiment_name_base: Base name for experiment templates
        :param all_instance_ids: List of all instance IDs to test
        :param stop_duration: Duration in seconds to keep instances stopped
        :return: Dictionary mapping architectures to template IDs
        
        Example:
            >>> fis = FISLib('key', 'secret', 'us-east-1')
            >>> templates = fis.create_architecture_fallback_test(
            ...     experiment_name_base='fusion-accelerator-test',
            ...     all_instance_ids=['i-1234567890abcdef0', 'i-0987654321fedcba0'],
            ...     stop_duration=120
            ... )
            >>> for arch, template_id in templates.items():
            ...     print(f"{arch}: {template_id}")
        """
        results = {}
        
        # Group instances by architecture
        arch_groups = {}
        for instance_id in all_instance_ids:
            try:
                arch = self.get_instance_architecture(instance_id)
                if arch not in arch_groups:
                    arch_groups[arch] = []
                arch_groups[arch].append(instance_id)
            except Exception as e:
                self.logger.warning(f"Could not get architecture for {instance_id}: {e}")
        
        # Create experiments for each architecture group
        for arch, instances in arch_groups.items():
            if not instances:
                self.logger.warning(f"No instances found for architecture: {arch}")
                continue
            
            experiment_name = f"{experiment_name_base}-{arch}"
            self.logger.info(f"Creating experiment for {arch} architecture")
            
            try:
                template_id = self.create_compute_failure_experiment(
                    experiment_name=experiment_name,
                    instance_ids=instances,
                    stop_duration=stop_duration,
                    tags={'Architecture': arch, 'Purpose': 'fallback-testing'}
                )
                results[arch] = template_id
            except Exception as e:
                self.logger.error(f"Failed to create experiment for {arch}: {e}")
                results[arch] = None
        
        self.logger.info(f"Created experiments for architectures: {list(results.keys())}")
        return results

    def print_fallback_test_report(self, experiment_results: Dict[str, Any]):
        """
        Generate a formatted report showing fallback test results.
        
        :param experiment_results: Dictionary containing experiment results
        """
        from prettytable import PrettyTable
        
        table = PrettyTable()
        table.field_names = ["Architecture", "Template ID", "Experiment ID", "Status", "Duration"]
        
        for arch, result in experiment_results.items():
            table.add_row([
                arch,
                result.get('template_id', 'N/A'),
                result.get('experiment_id', 'N/A'),
                result.get('status', 'N/A'),
                result.get('duration_seconds', 'N/A')
            ])
        
        self.logger.info(f"\n=== Fusion Accelerator Fallback Test Report ===")
        self.logger.info(f"{table}\n")

    # Placeholder methods for EBS mount failure simulations (future implementation)
    def create_ebs_mount_failure_experiment(self, *args, **kwargs):
        """
        Placeholder for EBS mount failure experiment creation.
        
        This method will be implemented to simulate EBS mount failures
        for testing fusion accelerator EBS fallback logic.
        
        :raises NotImplementedError: This method is not yet implemented
        """
        raise NotImplementedError(
            "EBS mount failure simulation is not yet implemented. "
            "This feature will be added in a future update."
        )

    def simulate_volume_attach_failure(self, *args, **kwargs):
        """
        Placeholder for volume attach failure simulation.
        
        This method will be implemented to simulate volume attachment
        failures for testing fusion accelerator volume handling.
        
        :raises NotImplementedError: This method is not yet implemented
        """
        raise NotImplementedError(
            "Volume attach failure simulation is not yet implemented. "
            "This feature will be added in a future update."
        )
