"""
Fusion AWS Utility - High-level AWS orchestration facade for fusion testing operations.

This class provides a unified interface for AWS operations needed by fusion testing,
wrapping EC2, S3, and Secrets Manager clients with fusion-specific functionality
like instance discovery, accelerator management, and error scanning.
"""

from .awslib.ec2_lib import EC2Lib
from .awslib.fis_lib import FISLib
from .awslib.s3_lib import S3Lib
from .awslib.secrets_manager_lib import SecretsManagerLib
import time, datetime
import concurrent.futures
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
        self.fis = FISLib(access_key, secret_key, region=region)
        self.s3 = S3Lib(access_key, secret_key, region=region)
        self.secrets = SecretsManagerLib(access_key, secret_key, region=region)
        self.log = logging.getLogger(__name__)
        self.log.setLevel(logging.INFO)

    def _cluster_filter(self, cluster_id, extra_tags=None):
        """Return an EC2 filter list scoped to a single cluster, with optional extra tag filters."""
        filters = [{'Name': 'tag:couchbase-cloud-cluster-id', 'Values': [str(cluster_id)]}]
        if extra_tags:
            filters.extend(extra_tags)
        return filters

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
        instances = self.list_instances(filters=self._cluster_filter(cluster_id))

        def scan_instance(instance):
            instance_id = instance.get('InstanceId', 'N/A')
            local_errors_found = False

            try:
                public_ip = instance.get('PublicIpAddress')
                if not public_ip:
                    self.log.warning(f"Instance {instance_id} does not have a Public IP. Skipping SSM check.")
                    return False

                self.log.info(f"Checking for core dumps on instance {instance_id} [{public_ip}] using SSM...")

                # 1. Check for core dumps in crash directory
                result = self.ec2.run_shell_command(instance.get('InstanceId', 'N/A'), 'ls -ltr /opt/couchbase/var/lib/couchbase/crash 2>/dev/null;')
                core_output = result.get('stdout', '')
                if 'core' in core_output or 'core.' in core_output:
                    self.log.warning(f"Core dump(s) found on instance {instance_id}: {core_output}")
                    local_errors_found = True
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
                                local_errors_found = True

            except Exception as e:
                self.log.error(f"Failed to check for core dumps and scan memcached logs on {instance_id}: {e}")

            return local_errors_found

        if instances:
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(instances), 50)) as executor:
                results = executor.map(scan_instance, instances)
                if any(results):
                    errors_found = True

        return errors_found

    def scan_dp_agent_logs_for_errors_on_cluster_instances(self, cluster_id):
        """
        Scan dp-agent logs for ERROR entries on all cluster instances.

        Connects to all instances in the cluster using AWS SSM and greps
        case-insensitively for ERROR in dp-agent log files.

        :param cluster_id: Cluster identifier to filter instances by couchbase-cloud-cluster-id tag
        :return: True if any errors are found, False otherwise
        """
        errors_found = False
        instances = self.list_instances(filters=self._cluster_filter(cluster_id))

        def scan_instance(instance):
            instance_id = instance.get('InstanceId', 'N/A')
            local_errors_found = False

            try:
                public_ip = instance.get('PublicIpAddress')
                if not public_ip:
                    self.log.warning(f"Instance {instance_id} does not have a Public IP. Skipping dp-agent log scan.")
                    return False

                self.log.info(f"Scanning dp-agent logs for errors on instance {instance_id} [{public_ip}]...")

                grep_cmd = 'journalctl -u dp-agent --no-pager 2>/dev/null | grep -i ERROR | grep -i "Main process exited" || true'
                grep_result = self.ec2.run_shell_command(instance_id, grep_cmd)
                grep_output = grep_result.get('stdout', '').strip()
                if grep_output:
                    self.log.critical(f"ERROR found in dp-agent journal on instance {instance_id} [{public_ip}]:\n{grep_output}")
                    local_errors_found = True
                else:
                    self.log.info(f"No dp-agent errors found in journal on instance {instance_id}.")

            except Exception as e:
                self.log.error(f"Failed to scan dp-agent logs on {instance_id}: {e}")

            return local_errors_found

        if instances:
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(instances), 50)) as executor:
                results = executor.map(scan_instance, instances)
                if any(results):
                    errors_found = True

        return errors_found

    def list_cluster_fusion_asg(self, cluster_id):
        """
        List Auto Scaling Groups for fusion accelerator instances in a cluster.

        Filters ASGs that are tagged with the cluster ID and fusion-accelerator function.
        Useful for monitoring ASG cleanup after fusion operations complete.

        :param cluster_id: Cluster identifier to filter ASGs
        :return: List of Auto Scaling Group objects
        """
        filters = self._cluster_filter(cluster_id, [{'Name': 'tag:couchbase-cloud-function', 'Values': ['fusion-accelerator']}])
        asgs = self.ec2.list_asgs(filters=filters)
        return asgs

    def suspend_asg_launch_process(self, asg_names: list) -> list:
        """
        Suspend the Launch scaling process on the given ASGs so that Auto Scaling
        makes no RunInstances calls until the process is resumed.

        Call this immediately after detecting the ASGs and before starting a FIS
        experiment, to guarantee that FIS is active before the first launch attempt.

        :param asg_names: List of ASG names to suspend Launch on
        :return: The same list (for convenient reference in cleanup)
        """
        self.ec2.suspend_asg_launch_process(asg_names)
        return asg_names

    def resume_asg_launch_process(self, asg_names: list) -> None:
        """
        Resume the Launch scaling process on the given ASGs so that Auto Scaling
        resumes making RunInstances calls (which will hit any active FIS fault).

        :param asg_names: List of ASG names to resume Launch on
        """
        self.ec2.resume_asg_launch_process(asg_names)

    def get_az_names_for_cluster_asgs(self, cluster_id: str) -> list:
        """
        Return the set of AZ names used by the cluster's fusion ASGs.

        Reads the VPCZoneIdentifier of the first ASG (all ASGs in a rebalance
        use the same subnet/AZ) and resolves the subnet to an AZ name.

        :param cluster_id: Cluster identifier
        :return: Sorted list of unique AZ names
        """
        asgs = self.list_cluster_fusion_asg(cluster_id)
        if not asgs:
            raise RuntimeError(f"No fusion ASGs found for cluster {cluster_id}")

        az_names = set()
        for asg in asgs:
            vpc_zone = asg.get("VPCZoneIdentifier", "")
            subnet_ids = [s.strip() for s in vpc_zone.split(",") if s.strip()]
            for subnet in self.ec2.describe_subnets(subnet_ids):
                az_names.add(subnet["AvailabilityZone"])
        return sorted(az_names)

    def get_asg_ordered_instance_types(self, cluster_id: str) -> list:
        """
        Return the instance type override list from a cluster's fusion ASG in
        priority order (index 0 = highest priority).

        All ASGs in a fusion rebalance share the same override list (derived
        from unifiedInstanceTypes in autoscalinggroups.go).

        :param cluster_id: Cluster identifier
        :return: Ordered list of instance type strings
        """
        asgs = self.list_cluster_fusion_asg(cluster_id)
        if not asgs:
            raise RuntimeError(f"No fusion ASGs found for cluster {cluster_id}")

        # All ASGs share the same override list; use the first one
        asg = asgs[0]
        policy = asg.get("MixedInstancesPolicy") or {}
        lt = policy.get("LaunchTemplate") or {}
        overrides = lt.get("Overrides") or []
        return [o["InstanceType"] for o in overrides if "InstanceType" in o]

    def count_capacity_failures_per_asg(
        self,
        cluster_id: str,
        since_time: datetime.datetime,
    ) -> dict:
        """
        Return a mapping of {asg_name: failure_count} for all fusion ASGs in
        the cluster, counting InsufficientInstanceCapacity failures since the
        given timestamp.

        :param cluster_id: Cluster identifier
        :param since_time: Count only failures that started after this UTC time
        :return: Dict of asg_name → failure count
        """
        asgs = self.list_cluster_fusion_asg(cluster_id)
        result = {}
        for asg in asgs:
            name = asg["AutoScalingGroupName"]
            result[name] = self.fis.get_asg_capacity_failure_count(name, since_time)
        return result

    def wait_for_min_capacity_failures_per_asg(
        self,
        cluster_id: str,
        min_failures: int,
        since_time: datetime.datetime,
        timeout: int = 300,
        poll_interval: int = 5,
    ) -> dict:
        """
        Block until every fusion ASG in the cluster has seen at least
        `min_failures` InsufficientInstanceCapacity failures since `since_time`.

        Returns the final failure-count mapping when the condition is met.

        :param cluster_id: Cluster identifier
        :param min_failures: Minimum failures required per ASG
        :param since_time: Count failures after this UTC-aware datetime
        :param timeout: Maximum wait time in seconds
        :param poll_interval: Seconds between polls
        :return: Dict of asg_name → failure count
        :raises TimeoutError: If the condition isn't met within timeout
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            counts = self.count_capacity_failures_per_asg(cluster_id, since_time)
            if counts and all(v >= min_failures for v in counts.values()):
                self.log.info(
                    f"All {len(counts)} ASGs reached {min_failures} capacity failures: {counts}"
                )
                return counts
            self.log.info(
                f"Waiting for {min_failures} failures per ASG: {counts}"
            )
            time.sleep(poll_interval)
        raise TimeoutError(
            f"ASGs for cluster {cluster_id} did not reach {min_failures} capacity failures "
            f"within {timeout}s"
        )

    def get_instance_type_per_asg(self, cluster_id: str) -> dict:
        """
        Return the instance type of the current InService instance for each
        fusion ASG in the cluster. ASGs with no running instance are omitted.

        :param cluster_id: Cluster identifier
        :return: Dict of asg_name → instance type string
        """
        asgs = self.list_cluster_fusion_asg(cluster_id)
        result = {}
        for asg in asgs:
            for inst in asg.get("Instances", []):
                if inst.get("LifecycleState") == "InService":
                    result[asg["AutoScalingGroupName"]] = inst["InstanceType"]
                    break
        return result