"""
Fusion CP Resource Monitor - AWS control plane resource monitoring utilities.

This class provides comprehensive monitoring for AWS resources managed by the fusion control plane,
including EBS guest volumes, accelerator instances, ASG cleanup, and error scanning.
"""

import datetime
import time
from prettytable import PrettyTable
from botocore.exceptions import ClientError, ConnectionError
from .fusion_monitor_util import FusionMonitorUtil


class FusionCPResourceMonitor:
    """
    Utility class for monitoring AWS control plane resources during fusion operations.

    Provides monitoring capabilities for:
    - EBS guest volume lifecycle (creation, attachment, hydration, cleanup)
    - Fusion accelerator instance management (creation, scaling, termination)
    - Autoscaling Group (ASG) cleanup verification
    - Cluster instance error scanning (core dumps, memcached errors)
    - Accelerator log processing from S3 storage
    """

    # Fusion accelerator instances use 16K IOPS volumes
    FUSION_ACCELERATOR_IOPS = 16000
    DEFAULT_TIMEOUT = FusionMonitorUtil.DEFAULT_TIMEOUT
    # Timeout for EBS volume cleanup operations (20 minutes)
    EBS_CLEANUP_TIMEOUT = 1200

    def __init__(self, logger, fusion_aws_util):
        """
        Initialize Fusion CP Resource Monitor.

        :param logger: Logger instance for monitoring operations
        :param fusion_aws_util: FusionAWSUtil instance for AWS client operations
        """
        self.log = logger
        self.fusion_aws_util = fusion_aws_util

    @staticmethod
    def get_fusion_rebalance_tag(volume):
        """
        Extract the couchbase-cloud-fusion-rebalance tag value from a volume.

        Fusion operations tag EBS volumes with rebalance IDs for tracking and coordination.
        This method extracts the rebalance ID tag if present.

        :param volume: Volume dictionary containing AWS tag information
        :return: Fusion rebalance tag value or None if not found
        """
        tags = volume.get('Tags', [])
        for tag in tags:
            if tag.get('Key') == 'couchbase-cloud-fusion-rebalance':
                return tag.get('Value')
        return None

    def log_fusion_guest_volumes_table(self, volumes):
        """
        Log fusion guest volumes in structured PrettyTable format.

        Provides detailed view of EBS guest volumes including size, IOPS, state,
        attachment information, and fusion rebalance association.

        :param volumes: List of volume dictionaries from AWS EC2 API
        """
        table = PrettyTable()
        table.field_names = ["Volume ID", "Size (GiB)", "IOPS", "State", "Instance", "Create Time", "Fusion Rebalance"]
        # Sort volumes by creation time for chronological analysis
        for volume in sorted(volumes, key=lambda x: x.get('CreateTime') or ''):
            fusion_rebalance_value = self.get_fusion_rebalance_tag(volume)
            table.add_row([
                volume.get('VolumeId'),
                volume.get('Size'),
                volume.get('Iops'),
                volume.get('State'),
                volume.get("Attachments")[0].get('InstanceId') if volume.get("Attachments") else 'N/A',
                volume.get('CreateTime').strftime('%Y-%m-%d %H:%M:%S') if volume.get('CreateTime') else 'N/A',
                fusion_rebalance_value if fusion_rebalance_value else 'N/A'
            ])
        self.log.info(f"Fusion Guest Volumes Table:\n{table}")

    @staticmethod
    def compute_ebs_cleanup_timeout(volumes, throughput_mbps=35):
        """Compute EBS cleanup timeout from actual volume state.

        Formula: max(sum of volume sizes per node) * 1024 / throughput_MBps
        Takes the worst-case node (highest total GiB attached) as the driver.

        :param volumes: List of volume dicts from AWS EC2 API
        :param throughput_mbps: Effective cleanup throughput per guest volume in MBps
        :return: Timeout in seconds (floor: EBS_CLEANUP_TIMEOUT)
        """
        if not volumes:
            return FusionCPResourceMonitor.EBS_CLEANUP_TIMEOUT
        volumes_by_instance = {}
        for v in volumes:
            attachments = v.get('Attachments', [])
            iid = attachments[0]['InstanceId'] if attachments else 'unattached'
            volumes_by_instance.setdefault(iid, []).append(v)
        max_total_size_gib = max(
            sum(v.get('Size', 0) for v in vols)
            for vols in volumes_by_instance.values()
        )
        computed = int(max_total_size_gib * 1024 / throughput_mbps)
        return max(computed, FusionCPResourceMonitor.EBS_CLEANUP_TIMEOUT)

    def monitor_fusion_guest_volumes(self, tenant, cluster, rebalance_task, fusion_monitor_util, fusion_rebalances, wait_for_hydration_complete=True, timeout=None, find_master_func=None):
        """
        Monitor fusion guest volumes during rebalance operations with hydration tracking.

        Tracks the complete lifecycle of EBS guest volumes: creation, attachment, hydration,
        and cleanup. Monitors fusion rebalance task status and validates volume transitions.

        Key monitored phases:
        - Volume creation and attachment (0 -> N volumes)
        - Hydration process (volumes remain attached)
        - Volume cleanup (N -> 0 volumes after hydration)

        :param tenant: Tenant object containing cluster configuration
        :param cluster: Cluster object with fusion configuration
        :param rebalance_task: Rebalance task object for status tracking
        :param fusion_monitor_util: FusionMonitorUtil instance for cluster operations
        :param fusion_rebalances: List to collect and track fusion rebalance IDs
        :param wait_for_hydration_complete: Whether to wait for full hydration completion (default: True)
        :param timeout: Maximum monitoring duration in seconds (default: DEFAULT_TIMEOUT)
        :param find_master_func: Optional callback function to locate master node
        :return: True if monitoring completes successfully, False on errors or timeout
        """
        if timeout is None:
            timeout = self.DEFAULT_TIMEOUT
        start_time = time.time()
        ebs_cleanup_timeout = 1200
        ebs_cleanup_start_time = time.time()
        volume_transition_started = False

        # Get network mapping for cluster node IP resolution
        fusion_monitor_util.get_hostname_public_ip_mapping(cluster)

        # Phase 1: Wait for volume creation and determine rebalance ID
        while time.time() - start_time < timeout:
            # Check for rebalance failure states
            if rebalance_task.state in ["deployment_failed",
                                  "deploymentFailed",
                                  "redeploymentFailed",
                                  "rebalance_failed",
                                  "rebalanceFailed",
                                  "scaleFailed"]:
                return False
            # Check for rebalance completion
            if rebalance_task.state == "healthy":
                return True

            try:
                # Query EBS volumes tagged with fusion accelerator and correct IOPS
                ebs_guest_volumes = self.fusion_aws_util.ec2.list_volumes_by_cluster_id(
                    filters={
                        'couchbase-cloud-cluster-id': cluster.id,
                        'couchbase-cloud-function': 'fusion-accelerator',
                        'couchbase-cloud-fusion-rebalance': fusion_rebalances[-1] if fusion_rebalances else '',
                        'iops': str(self.FUSION_ACCELERATOR_IOPS)
                        })
            except (ClientError, ConnectionError) as e:
                self.log.error(f"Failed to list volumes for cluster {cluster.id}: {e}")
                time.sleep(5)
                continue

            # No volumes created yet, continue waiting
            if len(ebs_guest_volumes) == 0:
                self.log.info(f"No guest volumes created for cluster {cluster.id}.")
                time.sleep(5)
                continue

            # Log initial volume discovery with detailed information
            self.log_fusion_guest_volumes_table(ebs_guest_volumes)
            break

        # Phase 2: Monitor hydration process and volume transitions
        while time.time() - start_time < timeout:
            # Get count of volumes currently attached to cluster nodes
            attached_volumes = fusion_monitor_util.get_attached_ebs_volumes_count(tenant, cluster, find_master_func=find_master_func)

            try:
                # List all fusion accelerator volumes (including available/hydrating)
                volumes = self.fusion_aws_util.ec2.list_volumes_by_cluster_id(
                    filters={
                        'couchbase-cloud-cluster-id': cluster.id,
                        'couchbase-cloud-function': 'fusion-accelerator'
                        })
            except (ClientError, ConnectionError) as e:
                self.log.error(f"Failed to list volumes for cluster {cluster.id}: {e}")
                time.sleep(5)
                continue

            # Critical: CP cleaned volumes while hydration was in progress
            if len(volumes) == 0 and attached_volumes > 0:
                self.log.critical(f"No volumes found for cluster {cluster.id}. CP has cleaned all the guest volumes while hydration was in progress.")
                return False

            # Detect initial volume attachment (hydration start)
            if not volume_transition_started:
                if attached_volumes > 0:
                    self.log.info(f"Attached volumes transitioned from 0 to {attached_volumes} for cluster {cluster.id}")
                    volume_transition_started = True
                else:
                    time.sleep(2)
                    continue

            # Monitor hydration completion if requested
            if wait_for_hydration_complete:
                if attached_volumes == 0:
                    self.log.info(f"Hydration process completed successfully. Attached volumes transitioned back to 0 for cluster {cluster.id}")
                    ebs_cleanup_start_time = time.time()
                    return True
                time.sleep(5)
            else:
                return True

        return False

    def check_ebs_guest_vol_deletion(self, tenant, cluster, fusion_monitor_util, stop_run_event, find_master_func=None):
        """
        Check if control plane is cleaning up hydrated EBS guest volumes.

        :param tenant: Tenant object
        :param cluster: Cluster object
        :param fusion_monitor_util: FusionMonitorUtil instance
        :param stop_run_event: Threading Event to stop monitoring
        :param find_master_func: Optional callback function to find master node (signature: find_master(tenant, cluster))
        """
        while not stop_run_event.is_set():
            self.log.info(f"Checking if CP is cleaning up the hydrated EBS guest volumes for cluster {cluster.id}")
            instances = self.fusion_aws_util.list_instances(
                self.fusion_aws_util._cluster_filter(cluster.id),
                log="EBS Guest Volumes Attached to Cluster", suppress_log=True
            )
            volumes = self.fusion_aws_util.ec2.list_volumes_by_cluster_id(filters={
                    'couchbase-cloud-cluster-id': cluster.id,
                    'couchbase-cloud-function': 'fusion-accelerator'
                    })
            volumes_by_instance = {}
            for volume in volumes:
                attachments = volume.get('Attachments', [])
                instance_id = attachments[0]['InstanceId'] if attachments else None
                if instance_id not in volumes_by_instance:
                    volumes_by_instance[instance_id] = []
                volumes_by_instance[instance_id].append(volume)
            try:
                if find_master_func:
                    find_master_func(tenant, cluster)
                from couchbase_utils.cb_server_rest_util.fusion.fusion_api import FusionRestAPI
                status, content = FusionRestAPI(cluster.master).get_active_guest_volumes()
                table = PrettyTable()
                table.field_names = ["Node ID", "Public IP", "Instance ID", "Attached GVs", "Existing GVs", "GV IDs", "Fusion Rebalance"]
                fusion_monitor_util.get_hostname_public_ip_mapping(cluster, suppress_log=True)
                for node_id in list(content):
                    public_ip = cluster.hostname_public_ip_mapping.get(node_id.split("@")[1])
                    instance_id = next((instance.get('InstanceId') for instance in instances if instance.get('PublicIpAddress') == public_ip), None)
                    volumes = volumes_by_instance.get(instance_id, [])
                    if len(volumes) > 0:
                        for volume in volumes:
                            fusion_rebalance_value = self.get_fusion_rebalance_tag(volume) or 'N/A'
                            table.add_row([
                                node_id.split("@")[1].split(".")[0],
                                public_ip if public_ip else 'N/A',
                                instance_id if instance_id else 'N/A',
                                len(content[node_id]),
                                len(volumes),
                                volume.get('VolumeId') + " (" + volume.get('State') + ")",
                                fusion_rebalance_value])
                    else:
                        table.add_row([
                                node_id.split("@")[1].split(".")[0],
                                public_ip if public_ip else 'N/A',
                                instance_id if instance_id else 'N/A',
                                len(content[node_id]),
                                len(volumes),
                                'N/A',
                                'N/A'])
                if None in volumes_by_instance:
                    for volume in volumes_by_instance[None]:
                        fusion_rebalance_value = self.get_fusion_rebalance_tag(volume) or 'N/A'
                        table.add_row([
                            'N/A',
                            'N/A',
                            'N/A',
                            'N/A',
                            'N/A',
                            volume.get('VolumeId') + " (" + volume.get('State') + ")",
                            fusion_rebalance_value])
            except Exception as e:
                import traceback
                traceback.print_exc()
                self.log.error(f"Failed to get active guest volumes for cluster {cluster.id}: {e}")
                time.sleep(300)
                continue
            self.log.info(f"EBS Guest Volumes attached to the cluster {cluster.id}:\n{table}")
            time.sleep(300)
        return True

    def monitor_ebs_cleanup(self, cluster, stop_run_event, timeout=None):
        """
        Monitor EBS cleanup for a cluster.

        :param cluster: Cluster object
        :param stop_run_event: Threading Event to stop monitoring
        :param timeout: Timeout in seconds (default: DEFAULT_TIMEOUT); dynamically
            computed from current volume count and size if the computed value is larger
        :return: True if cleanup completed, False otherwise
        """
        if timeout is None:
            timeout = self.DEFAULT_TIMEOUT
        try:
            initial_volumes = self.fusion_aws_util.ec2.list_volumes_by_cluster_id(
                filters={
                    'couchbase-cloud-cluster-id': cluster.id,
                    'couchbase-cloud-function': 'fusion-accelerator'
                })
            # Build per-node volume breakdown for logging
            volumes_by_instance = {}
            for v in initial_volumes:
                attachments = v.get('Attachments', [])
                iid = attachments[0]['InstanceId'] if attachments else 'unattached'
                volumes_by_instance.setdefault(iid, []).append(v)
            node_table = PrettyTable()
            node_table.field_names = ["Instance ID", "# Volumes", "Total Size (GiB)"]
            for iid, vols in sorted(volumes_by_instance.items()):
                total_gib = sum(v.get('Size', 0) for v in vols)
                node_table.add_row([iid, len(vols), total_gib])
            total_size_gib = sum(v.get('Size', 0) for v in initial_volumes)
            self.log.info(
                f"EBS guest volumes for cluster {cluster.id}: "
                f"{len(initial_volumes)} volumes across {len(volumes_by_instance)} node(s), "
                f"total size {total_size_gib} GiB\n{node_table}")
            dynamic_timeout = self.compute_ebs_cleanup_timeout(initial_volumes)
            timeout = max(timeout, dynamic_timeout)
            self.log.info(
                f"EBS cleanup timeout for cluster {cluster.id}: "
                f"dynamic={dynamic_timeout}s, effective={timeout}s "
                f"(worst-case node drives {max(sum(v.get('Size', 0) for v in vols) for vols in volumes_by_instance.values())} GiB @ 35 MBps)")
        except (ClientError, ConnectionError) as e:
            self.log.warning(
                f"Could not compute dynamic EBS cleanup timeout for cluster {cluster.id}: {e}")
        self.log.info(f"Checking if CP has cleaned all the guest volumes on cluster {cluster.id}")
        start_time = time.time()
        while time.time() - start_time < timeout and not stop_run_event.is_set():
            try:
                volumes = self.fusion_aws_util.ec2.list_volumes_by_cluster_id(
                    filters={
                        'couchbase-cloud-cluster-id': cluster.id,
                        'couchbase-cloud-function': 'fusion-accelerator'
                        })
                instances = self.fusion_aws_util.list_instances(
                    self.fusion_aws_util._cluster_filter(cluster.id),
                    log="EBS Guest Volumes Attached to Cluster"
                )
            except (ClientError, ConnectionError) as e:
                self.log.error(f"Failed to list volumes/instances for cluster {cluster.id}: {e}")
                time.sleep(10)
                continue
            if len(volumes) == 0:
                self.log.info(f"No ebs volumes found for cluster {cluster.id}. CP has cleaned all the ebs guest volumes.")
                return True
            table = PrettyTable()
            table.field_names = ["Volume ID", "Size (GiB)", "IOPS", "State", "Instance", "Public IP", "Create Time", "Fusion Rebalance"]
            for volume in sorted(volumes, key=lambda x: x.get('CreateTime') or ''):
                fusion_rebalance_value = self.get_fusion_rebalance_tag(volume)
                public_ip = None
                for instance in instances:
                    if volume.get("Attachments") and instance.get('InstanceId') == volume.get("Attachments")[0].get('InstanceId'):
                        public_ip = instance.get('PublicIpAddress')
                        break
                table.add_row([
                    volume.get('VolumeId'),
                    volume.get('Size'),
                    volume.get('Iops'),
                    volume.get('State'),
                    volume.get("Attachments")[0].get('InstanceId') if volume.get("Attachments") else 'N/A',
                    public_ip if public_ip else 'N/A',
                    volume.get('CreateTime').strftime('%Y-%m-%d %H:%M:%S') if volume.get('CreateTime') else 'N/A',
                    fusion_rebalance_value if fusion_rebalance_value else 'N/A'
                ])
            self.log.info(f"Fusion Guest Volumes still attached to the cluster {cluster.id}:\n{table}")
            time.sleep(10)
        self.log.info(f"EBS cleanup timeout reached. CP has not cleaned all the guest volumes on cluster {cluster.id}")
        return False

    def monitor_fusion_accelerator_nodes_killed_after_rebalance(
            self, cluster, timeout=None, max_node_lifetime_seconds=1800):
        """
        Monitor fusion accelerator nodes after rebalance to ensure they're killed.

        Fails immediately if any still-alive node has a LaunchTime older than
        *max_node_lifetime_seconds* (default 30 min).  Every polling iteration
        logs alive instances with their FusionRebalance tag so diagnostics show
        which rebalance the lingering node belongs to.

        :param cluster: Cluster object
        :param timeout: Timeout in seconds (default: DEFAULT_TIMEOUT)
        :param max_node_lifetime_seconds: Hard limit on how long an accelerator
            node may live before the check is considered a failure (default 1800)
        :return: True if all nodes are killed in time, False otherwise
        """
        if timeout is None:
            timeout = self.DEFAULT_TIMEOUT
        self.log.info(
            f"Checking if Fusion Accelerator nodes are still present for cluster {cluster.id}"
        )
        start_time = time.time()
        while time.time() - start_time < timeout:
            instances = self.fusion_aws_util.list_instances(
                self.fusion_aws_util._cluster_filter(
                    cluster.id,
                    [{'Name': 'tag:couchbase-cloud-function',
                      'Values': ['fusion-accelerator']}]
                ),
                log="Fusion Accelerator"
            )
            if len(instances) == 0:
                self.log.info(
                    f"Fusion Accelerator nodes not found for cluster {cluster.id}"
                )
                return True

            now = datetime.datetime.now(datetime.timezone.utc)
            info_table = PrettyTable()
            info_table.field_names = [
                "Instance ID", "Launch Time", "Age (s)", "FusionRebalance"
            ]
            violations = []
            for inst in instances:
                inst_id = inst.get('InstanceId', 'N/A')
                launch_time = inst.get('LaunchTime')
                age_s = int((now - launch_time).total_seconds()) if launch_time else 0
                fusion_rebalance = next(
                    (t['Value'] for t in inst.get('Tags', [])
                     if t['Key'] == 'couchbase-cloud-fusion-rebalance'),
                    'N/A'
                )
                info_table.add_row([
                    inst_id,
                    launch_time.strftime('%Y-%m-%d %H:%M:%S') if launch_time else 'N/A',
                    age_s,
                    fusion_rebalance,
                ])
                if age_s > max_node_lifetime_seconds:
                    violations.append((inst_id, launch_time, age_s, fusion_rebalance))

            self.log.info(
                f"Fusion Accelerator nodes still present for cluster {cluster.id}:\n"
                f"{info_table}"
            )

            if violations:
                viol_table = PrettyTable()
                viol_table.field_names = [
                    "Instance ID", "Launch Time", "Age (s)", "FusionRebalance"
                ]
                for inst_id, launch_time, age_s, fusion_rebalance in violations:
                    viol_table.add_row([
                        inst_id,
                        launch_time.strftime('%Y-%m-%d %H:%M:%S') if launch_time else 'N/A',
                        age_s,
                        fusion_rebalance,
                    ])
                self.log.error(
                    f"Accelerator node lifetime VIOLATION on cluster {cluster.id} — "
                    f"node(s) alive >{max_node_lifetime_seconds}s:\n{viol_table}"
                )
                return False

            time.sleep(10)
        self.log.info(
            f"Fusion Accelerator nodes timeout reached. "
            f"Fusion Accelerator nodes still present for cluster {cluster.id}"
        )
        return False

    def monitor_cluster_accelerator_instances(self, cluster, rebalance_task, fusion_rebalances, timeout=None):
        """
        Monitor cluster accelerator instances during rebalance.

        :param cluster: Cluster object
        :param rebalance_task: Rebalance task object
        :param fusion_rebalances: List to store fusion rebalance IDs
        :param timeout: Timeout in seconds (default: DEFAULT_TIMEOUT)
        :return: True if monitoring successful, False otherwise
        """
        if timeout is None:
            timeout = self.DEFAULT_TIMEOUT
        instances_count = 0
        start_time = time.time()
        transition_started = False
        while time.time() - start_time < timeout:
            if rebalance_task.state in ["deployment_failed",
                                  "deploymentFailed",
                                  "redeploymentFailed",
                                  "rebalance_failed",
                                  "rebalanceFailed",
                                  "scaleFailed"]:
                return False
            if rebalance_task.state == "healthy":
                return instances_count == 0
            try:
                instances = self.fusion_aws_util.list_accelerator_instances(
                    self.fusion_aws_util._cluster_filter(cluster.id, [{'Name': 'tag:couchbase-cloud-function', 'Values': ['fusion-accelerator']}]),
                    log="Fusion Accelerator"
                )
                instances_count = len(instances)
                if not transition_started:
                    if instances_count > 0:
                        self.log.info(f"Acceleration process started. Fusion Accelerator instances transitioned from 0 to {instances_count} for cluster {cluster.id}")
                        transition_started = True
                        for tag in instances[0].get('Tags', []):
                            if tag.get('Key') == 'couchbase-cloud-fusion-rebalance':
                                if tag.get('Value') not in fusion_rebalances:
                                    fusion_rebalances.append(tag.get('Value'))
                                    self.log.info(f"Fusion Rebalance: {fusion_rebalances}")
                                    break
                                else:
                                    self.log.info(f"Fusion Rebalance already exists: {tag.get('Value')}")
                                    raise Exception(f"Fusion Rebalance already exists: {tag.get('Value')}")
                    else:
                        self.log.info(f"Waiting for Fusion Accelerator instances creation for cluster {cluster.id}")
                        time.sleep(5)
                        continue
                else:
                    if instances_count == 0:
                        self.log.info(f"Acceleration/Downaload completed successfully. Fusion Accelerator instances transitioned back to 0 for cluster {cluster.id}")
                        return True
                    self.log.info(f"Waiting for Fusion Accelerator instances completion for cluster {cluster.id}")
                    time.sleep(5)
            except (ClientError, ConnectionError) as e:
                self.log.error(f"Failed to monitor Fusion Accelerator instances for cluster {cluster.id}: {e}")
                time.sleep(10)
                continue
        self.log.info(f"Acceleration/Download process timed out. Fusion Accelerator instances did not transition back to 0 for cluster {cluster.id}")
        return False

    def monitor_available_volumes_by_fusion_rebalance(self, cluster, fusion_rebalances, stop_run_event):
        """
        Monitor available volumes by fusion rebalance ID.

        :param cluster: Cluster object
        :param fusion_rebalances: List of fusion rebalance IDs
        :param stop_run_event: Threading Event to stop monitoring
        :return: True when monitoring stops
        """
        while not stop_run_event.is_set():
            table = PrettyTable()
            table.field_names = ["Serial No", "Fusion Rebalance", "Available Volumes", "Volume IDs"]
            serial_no = 1
            for rebalance in fusion_rebalances:
                try:
                    volumes = self.fusion_aws_util.ec2.list_volumes_by_cluster_id(
                        filters={
                            'couchbase-cloud-cluster-id': cluster.id,
                            'couchbase-cloud-fusion-rebalance': rebalance,
                            'State': 'available'
                        })
                except (ClientError, ConnectionError) as e:
                    self.log.error(f"Failed to list volumes for cluster {cluster.id}: {e}")
                    continue
                for volume in volumes:
                    table.add_row([serial_no, rebalance, len(volumes), volume.get('VolumeId')])
                    serial_no += 1
            if table.rowcount > 0:
                self.log.info(f"Available Volumes by Fusion Rebalance:\n{table}")
            time.sleep(30)
        return True

    def check_asg_cleanup_after_rebalance(self, clusters):
        """
        Check if ASG cleanup is running for all clusters.

        :param clusters: List of cluster objects
        """
        for cluster in clusters:
            self.log.info(f"Checking if ASG cleanup thread is running for cluster {cluster.id}")
            asgs = self.fusion_aws_util.list_cluster_fusion_asg(cluster.id)
            self.log.critical(f"Fusion accelerator ASGs pending deletion for cluster {cluster.id}: {len(asgs)} ASGs")

    def scan_memcached_logs_for_errors(self, clusters, steady_state_workload_sleep):
        """
        Scan memcached logs for errors on all cluster instances.

        :param clusters: List of cluster objects
        :param steady_state_workload_sleep: Sleep time before scanning
        :return: List of clusters with errors found
        """
        self.log.info(f"Sleeping for {steady_state_workload_sleep} seconds before scanning memcached logs for errors on cluster instances")
        time.sleep(steady_state_workload_sleep)
        errors_found = []
        for cluster in clusters:
            result = self.fusion_aws_util.scan_logs_for_errors_on_cluster_instances(cluster.id)
            if result:
                errors_found.append(cluster)
        return errors_found

    def scan_dp_agent_logs_for_errors(self, clusters, stop_run_event, interval=300):
        """
        Background thread: periodically scan dp-agent logs for ERROR entries on all cluster instances.

        Runs until stop_run_event is set, polling every `interval` seconds.

        :param clusters: List of cluster objects
        :param stop_run_event: Threading Event to stop monitoring
        :param interval: Seconds between scans (default: 300)
        :return: True when monitoring stops
        """
        while not stop_run_event.is_set():
            for cluster in clusters:
                self.log.info(f"Scanning dp-agent logs for errors on cluster {cluster.id}")
                result = self.fusion_aws_util.scan_dp_agent_logs_for_errors_on_cluster_instances(cluster.id)
                if result:
                    self.log.critical(f"dp-agent ERROR(s) found on cluster {cluster.id}")
                else:
                    self.log.info(f"No dp-agent errors found on cluster {cluster.id}")
            stop_run_event.wait(interval)
        return True

    def get_current_guest_volume_ids(self, cluster):
        """
        Return the list of EBS volume IDs currently tagged as fusion accelerator
        guest volumes for the given cluster.

        Queries AWS directly; returns an empty list on any error so callers can
        do a simple truthiness / len() check without extra error handling.

        :param cluster: Cluster object
        :return: List of volume ID strings (may be empty)
        """
        try:
            volumes = self.fusion_aws_util.ec2.list_volumes_by_cluster_id(
                filters={
                    'couchbase-cloud-cluster-id': cluster.id,
                    'couchbase-cloud-function': 'fusion-accelerator',
                }
            )
            return [v.get('VolumeId') for v in volumes if v.get('VolumeId')]
        except Exception as e:
            self.log.error(
                f"Failed to list guest volume IDs for cluster {cluster.id}: {e}"
            )
            return []

    def verify_guest_volumes_attached_to_cluster(self, cluster):
        """
        Verify every attached EBS guest volume is attached to an instance that
        belongs to this cluster (either a KV/data node or an accelerator instance).

        Unattached ('available') volumes are ignored — they are in the teardown
        window between CBS releasing them and CP deleting them, which is valid.

        Logs a PrettyTable summary of all volumes and their attachment state.

        :param cluster: Cluster object
        :return: True if all attached volumes belong to cluster instances, False otherwise
        """
        try:
            cluster_instances = self.fusion_aws_util.list_instances(
                self.fusion_aws_util._cluster_filter(cluster.id),
                log="All Cluster Instances",
                suppress_log=True,
            )
        except Exception as e:
            self.log.error(
                f"Failed to list cluster instances for {cluster.id}: {e}"
            )
            return False

        cluster_instance_ids = {
            i.get('InstanceId') for i in cluster_instances if i.get('InstanceId')
        }

        volumes = self.fusion_aws_util.ec2.list_volumes_by_cluster_id(
            filters={
                'couchbase-cloud-cluster-id': cluster.id,
                'couchbase-cloud-function': 'fusion-accelerator',
            }
        )

        table = PrettyTable()
        table.field_names = [
            "Volume ID", "State", "Attached Instance", "Is Cluster Instance"
        ]

        all_correct = True
        for volume in volumes:
            vol_id = volume.get('VolumeId', 'N/A')
            state = volume.get('State', 'N/A')
            attachments = volume.get('Attachments', [])

            if not attachments:
                table.add_row([vol_id, state, 'N/A (available)', 'N/A'])
                continue

            for attachment in attachments:
                instance_id = attachment.get('InstanceId', 'N/A')
                in_cluster = instance_id in cluster_instance_ids
                table.add_row([vol_id, state, instance_id, str(in_cluster)])
                if not in_cluster:
                    all_correct = False
                    self.log.error(
                        f"Volume {vol_id} is attached to instance {instance_id} "
                        f"which does NOT belong to cluster {cluster.id}"
                    )

        self.log.info(
            f"Guest volume attachment check for cluster {cluster.id}:\n{table}"
        )
        return all_correct

    def check_dp_agent_not_crashing(self, cluster, lookback_minutes=10):
        """
        Verify dp-agent is running and has not crashed on all cluster instances.

        Intended for point-in-time checks immediately after a cluster turn-on
        to confirm the dp-agent came back healthy on every node.

        Logs a PrettyTable with per-instance results (state, restart count,
        and any crash lines found in the journal since the current run started).

        :param cluster: Cluster object
        :param lookback_minutes: Journal window when service start time is
            unavailable (default 10 min)
        :return: True if dp-agent is healthy on all instances, False otherwise
        """
        self.log.info(
            f"Checking dp-agent health on all instances of cluster {cluster.id}")
        all_healthy, results = \
            self.fusion_aws_util.check_dp_agent_health_on_cluster_instances(
                cluster.id, lookback_minutes=lookback_minutes)

        table = PrettyTable()
        table.field_names = [
            "Instance ID", "Public IP", "State", "Restarts", "Healthy", "Crash Lines"
        ]
        for instance_id, public_ip, state, restarts, crash_lines, healthy in results:
            # Truncate long crash output so the table stays readable
            crash_summary = (crash_lines[:120] + '…') if len(crash_lines) > 120 else crash_lines
            table.add_row([
                instance_id, public_ip, state, restarts,
                "YES" if healthy else "NO",
                crash_summary or "—"
            ])

        level = self.log.info if all_healthy else self.log.critical
        level(
            f"dp-agent health check for cluster {cluster.id}:\n{table}"
        )
        return all_healthy

    def parse_accelerator_logs(self, clusters, fusion_rebalances, access_key, secret_key, region):
        """
        Parse accelerator logs for all clusters.

        :param clusters: List of cluster objects
        :param fusion_rebalances: List of fusion rebalance IDs
        :param access_key: AWS access key
        :param secret_key: AWS secret key
        :param region: AWS region
        """
        import subprocess
        import os

        for cluster in clusters:
            bucket_name = f"cbc-storage-{str(cluster.id)[-6:]}"
            rebalance_id = fusion_rebalances[-1]
            log_script = os.path.join(os.path.dirname(__file__), "download_accelerator_logs.sh")
            cmd = [
                log_script,
                access_key,
                secret_key,
                region,
                bucket_name,
                rebalance_id
            ]
            try:
                result = subprocess.run(
                    cmd,
                    check=False,
                    capture_output=True,
                    text=True,
                )
                self.log.info(
                    f"download_accelerator_logs.sh returned {result.returncode} for cluster {cluster.id}.\n"
                    f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
                )
            except Exception as e:
                self.log.error(f"Failed to run download_accelerator_logs.sh for cluster {cluster.id}: {e}")
