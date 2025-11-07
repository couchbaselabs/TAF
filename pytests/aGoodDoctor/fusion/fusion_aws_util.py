"""
Fusion AWS Utility - High-level AWS orchestration facade for fusion testing operations.

This class provides a unified interface for AWS operations needed by fusion testing,
wrapping EC2, S3, and Secrets Manager clients with fusion-specific functionality
like instance discovery, accelerator management, and error scanning.
"""

from .awslib.ec2_lib import EC2Lib
from .awslib.s3_lib import S3Lib
from .awslib.secrets_manager_lib import SecretsManagerLib
import time, datetime
from prettytable import PrettyTable
import logging

class FusionAWSUtil:

    # Fusion accelerator instances have 16000 IOPS as per fusion architecture
    FUSION_ACCELERATOR_IOPS = 16000

    def __init__(self, access_key, secret_key, region='us-east-1'):
        """
        Initialize Fusion AWS utility with AWS credentials.

        Creates AWS clients for EC2, S3, and Secrets Manager operations.

        :param access_key: AWS access key ID
        :param secret_key: AWS secret access key
        :param region: AWS region (default: us-east-1)
        """
        self.ec2 = EC2Lib(access_key, secret_key, region=region)
        self.s3 = S3Lib(access_key, secret_key, region=region)
        self.secrets = SecretsManagerLib(access_key, secret_key, region=region)
        self.log = logging.getLogger(__name__)
        self.log.setLevel(logging.INFO)

    def list_instances(self, filters: list[dict[str, str]], log="Fusion Accelerator", suppress_log=False) -> list:
        """
        List EC2 instances with detailed volume and fusion rebalance information.

        Retrieves instances matching the provided filters, filters to running state only,
        and enriches with EBS volume details and fusion rebalance tags.

        :param filters: List of EC2 filter dictionaries (e.g., [{'Name': 'tag:couchbase-cloud-cluster-id', 'Values': ['cluster-123']}])
        :param log: Custom log prefix for logging output (default: "Fusion Accelerator")
        :param suppress_log: Whether to suppress table logging (default: False)
        :return: List of running instance dictionaries with enriched volume and tag information
        """
        log = f"{log} Instances"
        instances = self.ec2.list_instances(filters=filters)

        # Create detailed table with instance, volume, and fusion rebalance information
        table = PrettyTable()
        table.field_names = ["Instance ID", "Instance Type", "VolumeId", "Disk Size (GiB)", "IOPS", "Public IP", "Instance Create Time", "Volume Create Time", "Time Alive", "FusionRebalance"]

        # Filter only instances which are in 'running' state for meaningful monitoring
        instances = [instance for instance in instances if instance.get('State', {}).get('Name') == 'running']

        for instance in instances:
            temp = dict()
            temp['InstanceId'] = instance.get('InstanceId', 'N/A')
            temp['InstanceType'] = instance.get('InstanceType', 'N/A')
            temp['PublicIpAddress'] = instance.get('PublicIpAddress', 'N/A')
            temp['InstanceCreateTime'] = instance.get('LaunchTime', None)
            temp['FusionRebalance'] = 'N/A'

            # Extract fusion rebalance tag if present
            for tag in instance.get('Tags', []):
                if tag.get('Key') == 'couchbase-cloud-fusion-rebalance':
                    temp['FusionRebalance'] = tag.get('Value')
                    break

            # Process EBS volume information from block device mappings
            block_devices = instance.get('BlockDeviceMappings', [])
            for block_device in block_devices:
                if 'Ebs' in block_device:
                    ebs = block_device['Ebs']
                    volume_id = ebs.get('VolumeId', None)
                    if volume_id:
                        temp['VolumeId'] = volume_id
                        # Get volume details using ec2_lib for IOPS and size information
                        volume = self.ec2.get_ebs_volume_by_id(volume_id)
                        if volume:
                            temp['DiskSize'] = volume.get('Size', 'N/A')
                            temp['IOPS'] = volume.get('Iops', 'N/A')
                            temp['VolumeCreateTime'] = volume.get('CreateTime', None)
                        else:
                            temp['DiskSize'] = 'N/A'
                            temp['IOPS'] = 'N/A'
                            temp['VolumeCreateTime'] = None
                        table.add_row([
                            temp['InstanceId'],
                            temp['InstanceType'],
                            volume_id,
                            temp['DiskSize'] if temp['DiskSize'] else 'N/A',
                            temp['IOPS'] if temp['IOPS'] else 'N/A',
                            temp['PublicIpAddress'],
                            temp['InstanceCreateTime'].strftime('%Y-%m-%d %H:%M:%S') if temp['InstanceCreateTime'] else 'N/A',
                            temp['VolumeCreateTime'].strftime('%Y-%m-%d %H:%M:%S') if temp['VolumeCreateTime'] else 'N/A',
                            str(datetime.datetime.now(datetime.timezone.utc) - temp['InstanceCreateTime']) if temp['InstanceCreateTime'] else 'N/A',
                            temp['FusionRebalance'] if temp['FusionRebalance'] else 'N/A'])
            temp = dict()            
            temp['InstanceId'] = instance.get('InstanceId', 'N/A')
            temp['InstanceType'] = instance.get('InstanceType', 'N/A')
            temp['PublicIpAddress'] = instance.get('PublicIpAddress', 'N/A')
            temp['InstanceCreateTime'] = instance.get('LaunchTime', None)
            temp['FusionRebalance'] = 'N/A'
            for tag in instance.get('Tags', []):
                if tag.get('Key') == 'couchbase-cloud-fusion-rebalance':
                    temp['FusionRebalance'] = tag.get('Value')
                    break
            block_devices = instance.get('BlockDeviceMappings', [])
            # Assume info only for root device
            for block_device in block_devices:
                if 'Ebs' in block_device:
                    ebs = block_device['Ebs']
                    volume_id = ebs.get('VolumeId', None)
                    if volume_id:
                        temp['VolumeId'] = volume_id
                        # Get volume details using ec2_lib
                        volume = self.ec2.get_ebs_volume_by_id(volume_id)
                        if volume:
                            temp['DiskSize'] = volume.get('Size', 'N/A')
                            temp['IOPS'] = volume.get('Iops', 'N/A')
                            temp['VolumeCreateTime'] = volume.get('CreateTime', None)
                        else:
                            temp['DiskSize'] = 'N/A'
                            temp['IOPS'] = 'N/A'
                            temp['VolumeCreateTime'] = None
                        table.add_row([
                            temp['InstanceId'],
                            temp['InstanceType'],
                            volume_id,
                            temp['DiskSize'] if temp['DiskSize'] else 'N/A',
                            temp['IOPS'] if temp['IOPS'] else 'N/A',
                            temp['PublicIpAddress'],
                            temp['InstanceCreateTime'].strftime('%Y-%m-%d %H:%M:%S') if temp['InstanceCreateTime'] else 'N/A',
                            temp['VolumeCreateTime'].strftime('%Y-%m-%d %H:%M:%S') if temp['VolumeCreateTime'] else 'N/A',
                            str(datetime.datetime.now(datetime.timezone.utc) - temp['InstanceCreateTime']) if temp['InstanceCreateTime'] else 'N/A',
                            temp['FusionRebalance'] if temp['FusionRebalance'] else 'N/A'])
        if table.rowcount > 0 and not suppress_log:
            self.log.info(f"{log}: \n" + str(table))
        return instances

    def list_accelerator_instances(self, filters: list[dict[str, str]], log="Fusion Accelerator") -> list:
        """
        List and filter fusion accelerator instances by IOPS.

        Retrieves instances matching filters and returns only those with fusion accelerator
        instances (identified by FUSION_ACCELERATOR_IOPS = 16000). Logs detailed information
        including instance count and warnings for instances awaiting termination.

        :param filters: List of EC2 filter dictionaries
        :param log: Custom log prefix (default: "Fusion Accelerator")
        :return: List of fusion accelerator instances (IOPS == FUSION_ACCELERATOR_IOPS)
        """
        log = f"{log} Instances"
        instances = self.ec2.list_instances(filters=filters)

        # Create detailed table with instance and volume information
        table = PrettyTable()
        table.field_names = ["Instance ID", "Instance Type", "VolumeId", "Disk Size (GiB)", "IOPS", "Public IP", "Instance Create Time", "Volume Create Time", "Time Alive", "FusionRebalance"]

        # Filter only instances which are in 'running' state for meaningful monitoring
        instances = [instance for instance in instances if instance.get('State', {}).get('Name') == 'running']
        return_instances = []

        for instance in instances:
            temp = dict()
            temp['InstanceId'] = instance.get('InstanceId', 'N/A')
            temp['InstanceType'] = instance.get('InstanceType', 'N/A')
            temp['PublicIpAddress'] = instance.get('PublicIpAddress', 'N/A')
            temp['InstanceCreateTime'] = instance.get('LaunchTime', None)
            temp['FusionRebalance'] = 'N/A'

            # Extract fusion rebalance tag if present
            for tag in instance.get('Tags', []):
                if tag.get('Key') == 'couchbase-cloud-fusion-rebalance':
                    temp['FusionRebalance'] = tag.get('Value')
                    break

            # Process EBS volume information from block device mappings
            block_devices = instance.get('BlockDeviceMappings', [])
            for block_device in block_devices:
                if 'Ebs' in block_device:
                    ebs = block_device['Ebs']
                    volume_id = ebs.get('VolumeId', None)
                    if volume_id:
                        temp['VolumeId'] = volume_id
                        # Get volume details using ec2_lib for IOPS filtering
                        volume = self.ec2.get_ebs_volume_by_id(volume_id)
                        if volume:
                            temp['DiskSize'] = volume.get('Size', 'N/A')
                            temp['IOPS'] = volume.get('Iops', 'N/A')
                            temp['VolumeCreateTime'] = volume.get('CreateTime', None)
                        else:
                            temp['DiskSize'] = 'N/A'
                            temp['IOPS'] = 'N/A'
                            temp['VolumeCreateTime'] = None
                        table.add_row([
                            temp['InstanceId'],
                            temp['InstanceType'],
                            volume_id,
                            temp['DiskSize'] if temp['DiskSize'] else 'N/A',
                            temp['IOPS'] if temp['IOPS'] else 'N/A',
                            temp['PublicIpAddress'],
                            temp['InstanceCreateTime'].strftime('%Y-%m-%d %H:%M:%S') if temp['InstanceCreateTime'] else 'N/A',
                            temp['VolumeCreateTime'].strftime('%Y-%m-%d %H:%M:%S') if temp['VolumeCreateTime'] else 'N/A',
                            str(datetime.datetime.now(datetime.timezone.utc) - temp['InstanceCreateTime']) if temp['InstanceCreateTime'] else 'N/A',
                            temp['FusionRebalance'] if temp['FusionRebalance'] else 'N/A'])
                        # Filter and return only fusion accelerator instances (16K IOPS)
                        if temp["IOPS"] == self.FUSION_ACCELERATOR_IOPS:
                            return_instances.append(instance)

        # Log accelerator instance details with count
        if table.rowcount > 0:
            self.log.info(f"{log}{' Count: ' + str(len(return_instances)) if len(return_instances) > 0 else ''} - Details: \n {table}")

        # Warning for instances that are not yet terminated (likely in cleanup process)
        if len(return_instances) != len(instances):
            self.log.warning(f"{log}: Watch out for accelerator instances which are yet to be terminated ({len(instances) - len(return_instances)})")

        return return_instances

    def scan_logs_for_errors_on_cluster_instances(self, cluster_id):
        """
        Scan cluster instances for core dumps and memcached log errors.

        Connects to all instances in the cluster using AWS SSM and checks for:
        1. Core dump files in /opt/couchbase/var/lib/couchbase/crash
        2. CRITICAL errors and 'Failed to hydrate fusion' messages in memcached logs

        :param cluster_id: Cluster identifier to filter instances
        :return: True if any errors are found, False otherwise
        """
        errors_found = False
        instances = self.list_instances(filters=[{
            'Name': 'tag:couchbase-cloud-cluster-id', 'Values': [str(cluster_id)]
        }])

        for instance in instances:
            instance_id = instance.get('InstanceId', 'N/A')

            try:
                public_ip = instance.get('PublicIpAddress')
                if not public_ip:
                    self.log.warning(f"Instance {instance_id} does not have a Public IP. Skipping SSM check.")
                    continue

                self.log.info(f"Checking for core dumps on instance {instance_id} [{public_ip}] using SSM...")

                # 1. Check for core dumps in crash directory
                result = self.ec2.run_shell_command(instance.get('InstanceId', 'N/A'), 'ls -ltr /opt/couchbase/var/lib/couchbase/crash 2>/dev/null;')
                core_output = result.get('stdout', '')
                if 'core' in core_output or 'core.' in core_output:
                    self.log.warning(f"Core dump(s) found on instance {instance_id}: {core_output}")
                    errors_found = True
                else:
                    self.log.info(f"No core dumps found on instance {instance_id}.")

                # 2. Scan memcached logs for CRITICAL errors and fusion hydration failures
                self.log.info(f"Grepping memcached logs for critical errors on instance {instance_id} [{public_ip}]...")

                # List all memcached log files for scanning
                list_files_cmd = 'ls /opt/couchbase/var/lib/couchbase/logs/memcached* 2>/dev/null || true'
                ls_result = self.ec2.run_shell_command(instance.get('InstanceId', 'N/A'), list_files_cmd)
                files_list = ls_result.get('stdout', '').strip().splitlines()

                if not files_list or (len(files_list) == 1 and files_list[0] == ''):
                    self.log.info(f"No memcached log files found on instance {instance_id}.")
                else:
                    # Search for critical error patterns in each log file
                    grep_patterns = ['CRITICAL', 'Failed to hydrate fusion']
                    for log_file in files_list:
                        for pattern in grep_patterns:
                            self.log.info(f"Grepping {pattern} in {log_file} on instance {instance_id}...")
                            grep_cmd = 'grep -E "{}" {} | grep -v "Failed to start audit daemon" 2>/dev/null || true'.format(pattern, log_file)
                            grep_result = self.ec2.run_shell_command(instance.get('InstanceId', 'N/A'), grep_cmd)
                            grep_output = grep_result.get('stdout', '').strip()
                            if grep_output:
                                self.log.critical(f"{pattern} found in {log_file} on instance {instance_id}:\n{grep_output}")
                                errors_found = True

            except Exception as e:
                self.log.error(f"Failed to check for core dumps and scan memcached logs on {instance_id}: {e}")

        return errors_found

    def list_cluster_fusion_asg(self, cluster_id):
        """
        List Auto Scaling Groups for fusion accelerator instances in a cluster.

        Filters ASGs that are tagged with the cluster ID and fusion-accelerator function.
        Useful for monitoring ASG cleanup after fusion operations complete.

        :param cluster_id: Cluster identifier to filter ASGs
        :return: List of Auto Scaling Group objects
        """
        filters = [
            {'Name': 'tag:couchbase-cloud-cluster-id', 'Values': [str(cluster_id)]},
            {'Name': 'tag:couchbase-cloud-function', 'Values': ['fusion-accelerator']},
        ]
        asgs = self.ec2.list_asgs(filters=filters)
        return asgs