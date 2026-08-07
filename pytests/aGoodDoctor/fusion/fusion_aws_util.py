"""
Fusion AWS Utility - High-level AWS orchestration facade for fusion testing operations.

This class provides a unified interface for AWS operations needed by fusion testing,
wrapping EC2, S3, and Secrets Manager clients with fusion-specific functionality
like instance discovery, accelerator management, and error scanning.
"""

from .awslib.ec2_lib import EC2Lib
from .awslib.fis_lib import FISLib, iso8601_duration_to_seconds
from .awslib.iam_lib import IAMLib
from .awslib.s3_lib import S3Lib
from .awslib.secrets_manager_lib import SecretsManagerLib
import time, datetime
import concurrent.futures
import os
import yaml
from prettytable import PrettyTable
from global_vars import logger as global_logger


ERROR_LOG_CONFIG_PATH = os.path.join(
    os.path.dirname(__file__), '..', '..', '..',
    'lib', 'couchbase_helper', 'error_log_config.yaml')


FUSION_ASSUME_ROLE_NAME = "jenkins-cp-cli"


def resolve_fusion_aws_credentials(test_input, region='us-east-1'):
    """
    Resolve the AWS credentials fusion tests should use for EC2/S3/Secrets
    Manager/FIS access.

    If aws_access_key/aws_secret_key test params are explicitly given, they're
    used as-is (long-lived override, e.g. for local runs). Otherwise, a role is
    assumed via STS using the AWS_ACCESS_KEY_ID_004/AWS_SECRET_ACCESS_KEY_004
    env vars as the base/source credentials (see IAMLib), and the resulting
    temporary credentials are returned instead.

    The role ARN defaults to arn:aws:iam::{account_id}:role/jenkins-cp-cli,
    where account_id is read from the [capella] ini section (same
    self.input.capella.get("account_id") convention used elsewhere in the
    framework, e.g. dedicatedbasetestcase.py). Pass aws_assume_role_arn as a
    test param to override this default (e.g. for a differently-named role).

    :param test_input: TestInputSingleton.input (or equivalent) — needs
                        .param(name, default) and .capella.get("account_id")
    :param region: AWS region to assume the role in
    :return: (access_key, secret_key, session_token, iam) tuple. session_token
             and iam are None when explicit long-lived aws_access_key/aws_secret_key
             were used. Otherwise iam is the IAMLib instance that assumed the
             role — pass it to FusionAWSUtil/S3Lib/etc. (via get_boto3_session())
             so their AWS clients auto-refresh once the assumed credentials expire,
             and use iam.get_credentials() to fetch fresh creds for subprocess/CLI
             calls that can't consume a boto3.Session directly.
    """
    access_key = test_input.param("aws_access_key", None)
    secret_key = test_input.param("aws_secret_key", None)
    if access_key and secret_key:
        return access_key, secret_key, None, None

    role_arn = test_input.param("aws_assume_role_arn", None)
    if not role_arn:
        account_id = test_input.capella.get("account_id")
        if not account_id:
            raise ValueError(
                "Either aws_access_key/aws_secret_key, or aws_assume_role_arn, "
                "or an account_id under the [capella] ini section (to derive "
                f"arn:aws:iam::<account_id>:role/{FUSION_ASSUME_ROLE_NAME}), "
                "is required")
        role_arn = f"arn:aws:iam::{account_id}:role/{FUSION_ASSUME_ROLE_NAME}"
    external_id = test_input.param("jenkins_cpcli_role_external_id", "f7bdb290-7b15-4ab7-afbf-28f3464a6144")
    duration_seconds = test_input.param("aws_assume_role_duration_seconds", None)

    iam = IAMLib(region=region)
    if not iam.assume_role(role_arn, external_id=external_id,
                            duration_seconds=duration_seconds):
        raise ValueError(f"Failed to assume role {role_arn} for fusion AWS access")
    creds = iam.get_credentials()
    return creds["AccessKeyId"], creds["SecretAccessKey"], creds["SessionToken"], iam


class FusionAWSUtil:

    # Fusion accelerator instances have 16000 IOPS as per fusion architecture
    FUSION_ACCELERATOR_IOPS = 16000

    FUSION_GUEST_VOL_TAG_KEY = "couchbase-cloud-guestvolume"
    FUSION_GUEST_VOL_TAG_VAL = "true"

    CLOUD_SNAPSHOT_BACKUP_SNAPSHOT_ID_FIELDS = [
        "snapshotIds",
        "snapshots",
        "ebsSnapshots",
        "volumeSnapshots",
        "ebs_snapshots",
        "snapshot_ids",
    ]

    def __init__(self, access_key, secret_key, session_token=None, region='us-east-1', boto3_session=None):
        """
        Initialize Fusion AWS utility with AWS credentials.

        Creates AWS clients for EC2, S3, and Secrets Manager operations.

        :param access_key: AWS access key ID
        :param secret_key: AWS secret access key
        :param session_token: AWS session token (required when access_key/secret_key
                               are temporary assumed-role credentials)
        :param region: AWS region (default: us-east-1)
        :param boto3_session: Pre-built boto3.Session to use as-is for every
                               underlying client instead of access_key/secret_key/
                               session_token (e.g. an auto-refreshing session from
                               IAMLib.get_boto3_session(), so EC2/FIS/S3/Secrets
                               Manager calls keep working after the assumed-role
                               credentials expire mid-test). Optional.
        """
        self.ec2 = EC2Lib(access_key, secret_key, session_token=session_token, region=region, boto3_session=boto3_session)
        self.fis = FISLib(access_key, secret_key, session_token=session_token, region=region, boto3_session=boto3_session)
        self.s3 = S3Lib(access_key, secret_key, session_token=session_token, region=region, boto3_session=boto3_session)
        self.secrets = SecretsManagerLib(access_key, secret_key, session_token=session_token, region=region, boto3_session=boto3_session)
        self.log = global_logger.get("infra")

    def _cluster_filter(self, cluster_id, extra_tags=None):
        """Return an EC2 filter list scoped to a single cluster, with optional extra tag filters."""
        filters = [{'Name': 'tag:couchbase-cloud-cluster-id', 'Values': [str(cluster_id)]}]
        if extra_tags:
            filters.extend(extra_tags)
        return filters

    def list_instances(self, filters: list[dict[str, str]], log="Couchbase Cluster", suppress_log=False) -> list:
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
        accelerator_tag = {'Name': 'tag:couchbase-cloud-function', 'Values': ['fusion-accelerator']}
        if accelerator_tag not in filters:
            filters = list(filters) + [accelerator_tag]
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
                        if temp["IOPS"] == self.FUSION_ACCELERATOR_IOPS and instance not in return_instances:
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

        # Reuse the memcached.log exclude_patterns from the shared on-prem
        # error-log config (lib/couchbase_helper/error_log_config.yaml) so
        # known-benign messages only need to be allow-listed in one place.
        exclude_patterns = ['Failed to start audit daemon']
        with open(ERROR_LOG_CONFIG_PATH, 'r') as fp:
            y_data = yaml.safe_load(fp)
        for file_entry in y_data.get('file_name_patterns', []):
            if file_entry.get('file') == 'memcached.log.*':
                for grep_pattern in file_entry.get('grep_for', []):
                    exclude_patterns.extend(grep_pattern.get('exclude_patterns', []))

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
                    exclude_pipe = ''.join(
                        ' | grep -v "{}"'.format(pattern)
                        for pattern in exclude_patterns)
                    for log_file in files_list:
                        for pattern in grep_patterns:
                            self.log.info(f"Grepping {pattern} in {log_file} on instance {instance_id}...")
                            grep_cmd = 'grep -E "{}" {}{} 2>/dev/null || true'.format(pattern, log_file, exclude_pipe)
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
        instances = self.list_instances(filters=self._cluster_filter(cluster_id), suppress_log=True)

        def scan_instance(instance):
            instance_id = instance.get('InstanceId', 'N/A')
            local_errors_found = False

            try:
                public_ip = instance.get('PublicIpAddress')
                if not public_ip:
                    self.log.warning(f"Instance {instance_id} does not have a Public IP. Skipping dp-agent log scan.")
                    return False

                self.log.info(f"Scanning dp-agent logs for errors on instance {instance_id} [{public_ip}]...")

                grep_cmd = 'journalctl -u dp-agent --no-pager 2>/dev/null | grep -B 5 -i "Main process exited" || true'
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

    def check_dp_agent_health_on_cluster_instances(self, cluster_id, lookback_minutes=10):
        """
        Check that dp-agent is active and has not crashed on all cluster instances.

        Per instance (concurrently via SSM):
          1. Checks systemd active state — must be 'active'.
          2. Reads NRestarts from systemd — logged for visibility.
          3. Greps the journal for crash indicators (killed, segfault, core dump,
             non-zero exit status) since the current service run started.

        :param cluster_id: Cluster ID used to filter instances by tag
        :param lookback_minutes: Fallback journal lookback window when the
            service start time cannot be determined (default 10 min)
        :return: True if dp-agent is healthy on every instance, False otherwise
        """
        instances = self.list_instances(filters=self._cluster_filter(cluster_id))
        if not instances:
            self.log.warning(
                f"No instances found for cluster {cluster_id} — "
                "skipping dp-agent health check")
            return True, []

        crash_pattern = r"killed|segfault|core.dump|Main process exited.*status=[^0]|status=[1-9][0-9]*$"

        # Single compound shell command — one SSM round-trip per instance.
        # Returns lines of the form:
        #   IS_ACTIVE=<active|inactive|...>
        #   RESTARTS=<N>
        #   CRASHES=<matching journal lines, if any>
        cmd = (
            "IS_ACTIVE=$(systemctl is-active dp-agent 2>/dev/null || echo inactive); "
            "RESTARTS=$(systemctl show dp-agent --property=NRestarts --value 2>/dev/null || echo unknown); "
            "SINCE=$(systemctl show dp-agent --property=ActiveEnterTimestamp --value 2>/dev/null || echo ''); "
            f"if [ -n \"$SINCE\" ]; then "
            f"  CRASHES=$(journalctl -u dp-agent --since \"$SINCE\" --no-pager 2>/dev/null "
            f"    | grep -iE '{crash_pattern}' | head -20 || true); "
            f"else "
            f"  CRASHES=$(journalctl -u dp-agent --since \"{lookback_minutes} minutes ago\" --no-pager 2>/dev/null "
            f"    | grep -iE '{crash_pattern}' | head -20 || true); "
            f"fi; "
            "echo \"IS_ACTIVE=$IS_ACTIVE\"; "
            "echo \"RESTARTS=$RESTARTS\"; "
            "echo \"CRASHES=$CRASHES\""
        )

        all_healthy = True

        def check_instance(instance):
            instance_id = instance.get('InstanceId', 'N/A')
            public_ip = instance.get('PublicIpAddress', 'N/A')
            try:
                result = self.ec2.run_shell_command(instance_id, cmd)
                output = result.get('stdout', '')
                is_active = 'unknown'
                restarts = 'unknown'
                crash_lines = ''
                for line in output.splitlines():
                    if line.startswith('IS_ACTIVE='):
                        is_active = line.split('=', 1)[1].strip()
                    elif line.startswith('RESTARTS='):
                        restarts = line.split('=', 1)[1].strip()
                    elif line.startswith('CRASHES='):
                        crash_lines = line.split('=', 1)[1].strip()
                    elif crash_lines:
                        # Continuation of multi-line CRASHES output
                        crash_lines += '\n' + line

                healthy = (is_active == 'active') and not crash_lines
                if not healthy:
                    if is_active != 'active':
                        self.log.critical(
                            f"dp-agent is NOT active on instance {instance_id} "
                            f"[{public_ip}]: state={is_active} restarts={restarts}")
                    if crash_lines:
                        self.log.critical(
                            f"dp-agent crash indicators on instance {instance_id} "
                            f"[{public_ip}] (restarts={restarts}):\n{crash_lines}")
                else:
                    self.log.info(
                        f"dp-agent healthy on instance {instance_id} "
                        f"[{public_ip}]: state={is_active} restarts={restarts}")
                return instance_id, public_ip, is_active, restarts, crash_lines, healthy
            except Exception as e:
                self.log.error(
                    f"Failed to check dp-agent health on instance {instance_id}: {e}")
                return instance_id, public_ip, 'error', 'error', str(e), False

        results = []
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=min(len(instances), 50)) as executor:
            results = list(executor.map(check_instance, instances))

        if not all(r[5] for r in results):
            all_healthy = False

        return all_healthy, results

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

    def wait_for_fis_experiment_to_finish(
        self,
        cluster_id: str,
        fis_experiment_id: str,
        since_time: datetime.datetime,
        duration: str,
        poll_interval: int = 30,
        timeout_buffer: int = 300,
    ) -> dict:
        """
        Observe per-ASG InsufficientInstanceCapacity failure counts for the
        life of the FIS experiment, without stopping it early. Returns once
        FIS itself reaches a terminal state (completed/stopped/failed) — i.e.
        once the experiment's own `duration` has elapsed — rather than as
        soon as some minimum failure count is observed.

        This lets AWS's FIS experiment lifecycle (not the test) decide when
        the fault lifts and ASGs are free to launch instances; the caller is
        expected to assert on the returned counts that failures actually
        occurred.

        :param cluster_id: Cluster identifier
        :param fis_experiment_id: The running FIS experiment id to track
        :param since_time: Count failures after this UTC-aware datetime
        :param duration: The experiment's configured ISO 8601 duration —
                         used only to size the safety-net timeout below
        :param poll_interval: Seconds between polls
        :param timeout_buffer: Extra seconds allowed beyond `duration` before
                               giving up, in case FIS is slow to transition
        :return: Final per-ASG failure-count mapping
        :raises TimeoutError: If FIS doesn't reach a terminal state in time
        """
        deadline = time.time() + iso8601_duration_to_seconds(duration) + timeout_buffer
        counts = {}
        while time.time() < deadline:
            status = self.fis.get_experiment_status(fis_experiment_id)["status"]
            counts = self.count_capacity_failures_per_asg(cluster_id, since_time)
            self.log.info(
                f"FIS experiment {fis_experiment_id} status={status}, "
                f"capacity failure counts: {counts}"
            )
            if status.lower() in ("completed", "stopped", "failed"):
                self.log.info(
                    f"FIS experiment {fis_experiment_id} reached terminal state '{status}'"
                )
                return counts
            time.sleep(poll_interval)
        raise TimeoutError(
            f"FIS experiment {fis_experiment_id} for cluster {cluster_id} did not reach a "
            f"terminal state within duration={duration}+{timeout_buffer}s; "
            f"last observed failure counts: {counts}"
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

    def corrupt_fusion_log_store(self, s3_bucket_name: str, bucket_uuid: str,
                                 num_folders: int = 3, num_files: int = 5) -> dict:
        """
        Delete a few vBucket/shard folders and a few individual files from a
        fusion bucket's S3 log store, to force a fusion rebalance to fail
        (accelerator download hits missing objects) without wiping the whole
        bucket -- a targeted, deterministic alternative to deleting the entire
        S3 bucket, which was observed to leave the CP hot-looping "Replacing
        Node" indefinitely rather than detecting the outage and failing
        (Jenkins build 16485).

        Fusion log store objects for a Couchbase bucket live under the
        kv/<bucket_uuid>/ prefix, with one immediate sub-folder per
        vBucket/shard (see FusionMonitorUtil.get_fusion_log_store_data_size_on_s3
        and the kv/<bucket.bucket_uuid> convention used throughout this
        package). This picks up to `num_folders` of those sub-folders and
        deletes every object under each, then picks up to `num_files`
        individual objects from the remaining (non-deleted) folders and
        deletes just those files -- leaving the majority of the log store
        intact so the corruption is targeted rather than total.

        :param s3_bucket_name: S3 bucket name (from _get_s3_bucket_name_from_uri)
        :param bucket_uuid: Couchbase bucket UUID (bucket.bucket_uuid)
        :param num_folders: Max number of vBucket/shard folders to delete entirely
        :param num_files: Max number of individual files to delete from the
            remaining folders
        :return: {"folders_deleted": [...], "files_deleted": [...]}
        """
        kv_prefix = f"kv/{bucket_uuid}"
        folders = sorted(self.s3._list_common_prefixes(s3_bucket_name, prefix=kv_prefix))
        if not folders:
            self.log.warning(
                f"No vBucket/shard folders found under {kv_prefix} in {s3_bucket_name} "
                f"-- log store may not be synced yet")
            return {"folders_deleted": [], "files_deleted": []}

        folders_to_delete = folders[:num_folders]
        remaining_folders = folders[num_folders:] or folders_to_delete

        folders_deleted = []
        for folder in folders_to_delete:
            result = self.s3.delete_files_by_prefix(s3_bucket_name, folder)
            if result and all(result.values()):
                folders_deleted.append(folder)
                self.log.info(
                    f"Deleted vBucket/shard folder {folder} ({len(result)} object(s)) "
                    f"from {s3_bucket_name}")
            else:
                self.log.warning(
                    f"Failed to fully delete vBucket/shard folder {folder} "
                    f"from {s3_bucket_name}: {result}")

        files_deleted = []
        for folder in remaining_folders:
            if len(files_deleted) >= num_files:
                break
            candidates = self.s3.list_files_in_bucket(
                s3_bucket_name, prefix=folder, max_keys=num_files - len(files_deleted))
            file_keys = [f["Key"] for f in candidates]
            if not file_keys:
                continue
            result = self.s3.delete_multiple_files(s3_bucket_name, file_keys)
            files_deleted.extend(key for key, ok in result.items() if ok)

        self.log.info(
            f"Corrupted fusion log store for bucket_uuid={bucket_uuid} in "
            f"{s3_bucket_name}: {len(folders_deleted)} folder(s) deleted "
            f"({folders_deleted}), {len(files_deleted)} individual file(s) "
            f"deleted ({files_deleted})")
        return {"folders_deleted": folders_deleted, "files_deleted": files_deleted}
    def get_guest_volumes_for_cluster(self, cluster_id: str) -> dict:
        """
        Retrieve the guest volume inventory for a cluster via AWS EC2 API.

        Fusion guest volumes are tagged:
          couchbase-cloud-cluster-id = cluster_id
          couchbase-cloud-function = fusion-accelerator
          couchbase-cloud-fusion-guest-volume = true

        The guest-volume tag is REQUIRED: couchbase-cloud-function=
        fusion-accelerator alone also matches the accelerator EC2 instance's
        own root/boot EBS volume (applied via the launch template), so
        omitting it over-counts guest volumes and mismatches the EBS snapshot
        count for a backup. Only genuine guest volumes carry
        couchbase-cloud-fusion-guest-volume=true (set by FusionGuestVolumeTag()
        in couchbase-cloud's internal/clusters/tags/tags.go). Volumes already
        in AWS "deleting" state are excluded too — a volume mid-deletion at
        backup time won't get a fresh snapshot. This mirrors the shared
        fusion_cp_resource_monitor.get_current_guest_volume_ids filter.

        Returns {instance_id: [volume_id, ...], ...}.
        Unattached volumes map to key "unattached".
        """
        self.log.info(
            "Fetching guest volumes for cluster {} via EC2 API".format(cluster_id))
        try:
            volumes = self.ec2.list_volumes_by_cluster_id(
                filters={
                    "couchbase-cloud-cluster-id": cluster_id,
                    "couchbase-cloud-function": "fusion-accelerator",
                    "couchbase-cloud-fusion-guest-volume": "true",
                })
        except Exception as e:
            self.log.warning(
                "get_guest_volumes_for_cluster failed for cluster {}: {}".format(
                    cluster_id, e))
            return {}
        result = {}
        for vol in volumes:
            if vol.get("State") == "deleting":
                continue
            vol_id = vol.get("VolumeId")
            attachments = vol.get("Attachments", [])
            instance_id = (
                attachments[0].get("InstanceId") if attachments else None)
            if instance_id:
                result.setdefault(instance_id, []).append(vol_id)
            else:
                result.setdefault("unattached", []).append(vol_id)
        self.log.info(
            "Found {} guest volumes across {} nodes on cluster {}".format(
                sum(len(v) for v in result.values()), len(result), cluster_id))
        return result

    def find_fusion_s3_bucket(self, cluster_id: str) -> str:
        """
        Find the Fusion S3 log-store bucket for a cluster via the AWS
        ResourceGroupsTagging API. Filters by BOTH the cluster id and the
        `couchbase-cloud-storage-use=fusion` tag so we don't pick up the
        cluster's generic storage bucket (`cbc-storage-<cluster-id>`),
        which also carries the cluster-id tag.

        Returns bucket name, or None if not found.
        """
        try:
            buckets = self.s3.find_buckets_by_tags([
                {"Key": "couchbase-cloud-cluster-id", "Values": [cluster_id]},
                {"Key": "couchbase-cloud-storage-use", "Values": ["fusion"]},
            ])
        except Exception as e:
            self.log.warning(
                "find_fusion_s3_bucket failed for cluster {}: {}".format(
                    cluster_id, e))
            return None
        if buckets:
            self.log.info("Found Fusion S3 bucket for cluster {}: {}".format(
                cluster_id, buckets[0]))
            return buckets[0]
        self.log.warning(
            "No S3 bucket tagged couchbase-cloud-cluster-id={} AND "
            "couchbase-cloud-storage-use=fusion found".format(cluster_id))
        return None

    def get_ebs_snapshots_for_backup(self, backup_id: str,
                                     backup_record: dict = None) -> list:
        """
        Retrieve EBS snapshots created by a cloud snapshot backup.

        Per Capella dev, there is no REST endpoint that returns the snapshot
        IDs for a cloud snapshot backup — the backup record itself does not
        embed them either.  The supported way is to query EC2 directly,
        filtering by the `couchbase-cloud-backup-id` tag that Capella stamps
        on every snapshot it creates for that backup, e.g.:

            aws ec2 describe-snapshots \\
              --filters "Name=tag:couchbase-cloud-backup-id,Values=<id>"

        Guest-volume snapshots additionally carry
        `couchbase-cloud-guestvolume=true`; primary disk snapshots don't.
        Classification happens in classify_snapshots().

        The `backup_record` parameter is accepted for backwards-compat with
        existing callers but is no longer consulted.
        """
        _ = backup_record  # intentionally unused
        self.log.info(
            "Querying EC2 for snapshots tagged "
            "couchbase-cloud-backup-id={}".format(backup_id))
        snapshots = self.ec2.list_snapshots_by_tags([
            {"Name": "tag:couchbase-cloud-backup-id", "Values": [backup_id]},
        ])
        self.log.info(
            "EC2 returned {} snapshot(s) for backup {}".format(
                len(snapshots), backup_id))
        return snapshots

    def classify_snapshots(self, snapshots: list) -> tuple:
        """
        Split snapshots into (primary_disk_snapshots, guest_volume_snapshots).

        Primary snapshots do NOT carry couchbase-cloud-guestvolume tag.
        Guest volume snapshots carry couchbase-cloud-guestvolume=true.
        """
        primary = []
        guest = []
        for snap in snapshots:
            tags = {t["Key"]: t["Value"] for t in snap.get("Tags", [])}
            if tags.get(self.FUSION_GUEST_VOL_TAG_KEY, "").lower() == \
                    self.FUSION_GUEST_VOL_TAG_VAL:
                guest.append(snap)
            else:
                primary.append(snap)
        return primary, guest

    @staticmethod
    def get_tag_value(snapshot: dict, key: str):
        """Return the value of a tag key on a snapshot dict, or None."""
        for tag in snapshot.get("Tags", []):
            if tag["Key"] == key:
                return tag["Value"]
        return None

