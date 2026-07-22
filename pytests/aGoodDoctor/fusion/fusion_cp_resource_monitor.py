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

    def log_fusion_guest_volumes_table(self, cluster, volumes):
        """
        Log fusion guest volumes in structured PrettyTable format.

        Provides detailed view of EBS guest volumes including size, IOPS, state,
        attachment information, and fusion rebalance association.

        :param cluster: Cluster object (used only for the log heading)
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
        self.log.info(f"Fusion Guest Volumes Table for cluster {cluster.id}:\n{table}")

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
            self.log_fusion_guest_volumes_table(cluster, ebs_guest_volumes)
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

    def guest_volume_attached_vs_ns_server_reported(self, tenant, cluster, fusion_monitor_util, find_master_func=None):
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
            return
        self.log.info(f"EBS Guest Volumes attached to the cluster {cluster.id}:\n{table}")

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
            self.guest_volume_attached_vs_ns_server_reported(tenant, cluster, fusion_monitor_util, find_master_func=find_master_func)
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
                    log="EBS Guest Volumes Attached to Cluster",
                    suppress_log=True
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
                    self.fusion_aws_util._cluster_filter(cluster.id),
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
                self.log.info(f"Available Volumes by Fusion Rebalance for cluster {cluster.id}:\n{table}")
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

    def scan_memcached_logs_for_errors(self, cluster, sleep_before_scan=60):
        """
        Scan memcached logs for errors on a cluster's instances.

        :param cluster: Cluster object
        :param sleep_before_scan: Sleep time before scanning (default: 60s)
        :return: True if errors were found on the cluster, False otherwise
        """
        if sleep_before_scan:
            self.log.info(f"Sleeping for {sleep_before_scan} seconds before scanning memcached logs for errors on cluster {cluster.id}")
            time.sleep(sleep_before_scan)
        self.log.info(f"Scanning memcached logs for errors on cluster {cluster.id}")
        return self.fusion_aws_util.scan_logs_for_errors_on_cluster_instances(cluster.id)

    def get_main_volume_disk_usage_percent(self, cluster):
        """
        Poll each cluster instance's main persistent-data EBS/LVM volume disk
        usage percent via SSM ``df``.

        This is the ``/opt/couchbase/var/lib/couchbase`` mount
        (``VG_CB-LV_persistent_data``) — the volume Capella's diskAutoScaling
        is expected to grow before it fills up (AV-137329: hydration filled
        this volume to 100% on every node with no resize ever attempted).

        :param cluster: Cluster object
        :return: dict mapping instance ID -> usage percent (int), or None
                 for any instance whose check failed
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        instances = self.fusion_aws_util.list_instances(
            self.fusion_aws_util._cluster_filter(cluster.id),
            log="Couchbase Cluster Instances", suppress_log=True)

        cmd = (
            "df -h /opt/couchbase/var/lib/couchbase "
            "--output=pcent 2>/dev/null | tail -1 | tr -d '% '"
        )

        def check_instance(instance):
            instance_id = instance.get('InstanceId', 'N/A')
            try:
                result = self.fusion_aws_util.ec2.run_shell_command(instance_id, cmd)
                stdout = result.get('stdout', '').strip()
                if result.get('success') and stdout.isdigit():
                    return instance_id, int(stdout)
                self.log.warning(
                    f"Could not parse main volume disk usage on {instance_id}: "
                    f"stdout={stdout!r} stderr={result.get('stderr')}")
            except Exception as e:
                self.log.error(
                    f"Failed to check main volume disk usage on {instance_id}: {e}")
            return instance_id, None

        usage = {}
        if instances:
            with ThreadPoolExecutor(max_workers=min(len(instances), 50)) as executor:
                futures = {
                    executor.submit(check_instance, inst): inst for inst in instances
                }
                for future in as_completed(futures):
                    instance_id, pct = future.result()
                    usage[instance_id] = pct

        table = PrettyTable()
        table.field_names = ["Instance ID", "Main Volume Disk Usage %"]
        for instance_id, pct in usage.items():
            table.add_row([instance_id, pct if pct is not None else "N/A"])
        self.log.info(
            f"Main volume disk usage for cluster {cluster.id}:\n{table}")

        return usage

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

        # NOTE: 'couchbase-cloud-function: fusion-accelerator' is applied to
        # every EBS volume associated with an accelerator node -- including
        # the accelerator EC2 instance's own root/boot volume (tagged via the
        # launch template's per-resource TagSpecifications). Only the actual
        # guest volume additionally carries 'couchbase-cloud-fusion-guest-volume:
        # true' (see couchbase-cloud internal/clusters/tags/tags.go
        # FusionGuestVolumeTag()), so that tag must be included here to avoid
        # counting root/boot volumes as guest volumes.
        volumes = self.fusion_aws_util.ec2.list_volumes_by_cluster_id(
            filters={
                'couchbase-cloud-cluster-id': cluster.id,
                'couchbase-cloud-function': 'fusion-accelerator',
                'couchbase-cloud-fusion-guest-volume': 'true',
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

    def parse_accelerator_logs(self, clusters, fusion_rebalances, access_key, secret_key, region, session_token=None):
        """
        Parse accelerator logs for all clusters.

        :param clusters: List of cluster objects
        :param fusion_rebalances: List of fusion rebalance IDs
        :param access_key: AWS access key
        :param secret_key: AWS secret key
        :param region: AWS region
        :param session_token: AWS session token (required when access_key/secret_key
                               are temporary assumed-role credentials)
        """
        import subprocess
        import os

        env = os.environ.copy()
        if session_token:
            env["AWS_SESSION_TOKEN"] = session_token
        else:
            env.pop("AWS_SESSION_TOKEN", None)

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
                    env=env,
                )
                self.log.info(
                    f"download_accelerator_logs.sh returned {result.returncode} for cluster {cluster.id}.\n"
                    f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
                )
            except Exception as e:
                self.log.error(f"Failed to run download_accelerator_logs.sh for cluster {cluster.id}: {e}")

    def get_current_guest_volume_ids(self, cluster) -> list:
        """
        Return the EBS Volume IDs of all current fusion guest volumes for a cluster.

        Guest volumes are tagged with couchbase-cloud-cluster-id=<cluster.id>,
        couchbase-cloud-function=fusion-accelerator, AND
        couchbase-cloud-fusion-guest-volume=true.

        Note: couchbase-cloud-function=fusion-accelerator alone is NOT sufficient
        to identify a guest volume -- it is also applied (via the accelerator's
        launch template TagSpecifications) to the accelerator EC2 instance's own
        root/boot EBS volume. Only the actual guest volume additionally carries
        couchbase-cloud-fusion-guest-volume=true (set by FusionGuestVolumeTag() in
        couchbase-cloud's internal/clusters/tags/tags.go, applied only at the
        guest-volume-creation call sites). Omitting this tag here inflates the
        count with root/boot volumes of any still-running accelerator instances,
        which then mismatches against the EBS snapshot count for a backup (only
        genuine guest volumes get snapshotted/tagged couchbase-cloud-guestvolume=true).

        :param cluster: Cluster object with .id attribute
        :return: List of volume ID strings (may be empty)
        """
        try:
            volumes = self.fusion_aws_util.ec2.list_volumes_by_cluster_id(filters={
                "couchbase-cloud-cluster-id": cluster.id,
                "couchbase-cloud-function": "fusion-accelerator",
                "couchbase-cloud-fusion-guest-volume": "true",
            })
            ids = [v.get("VolumeId") for v in volumes if v.get("VolumeId")]
            self.log.info(
                f"Current guest volumes for cluster {cluster.id}: {ids}"
            )
            return ids
        except Exception as e:
            self.log.error(f"Error listing guest volumes for cluster {cluster.id}: {e}")
            return []

    def verify_old_guest_volumes_deleted(
        self, cluster, pre_restore_volume_ids: list, timeout: int = 600
    ) -> bool:
        """
        Verify that guest volumes that existed before a restore are gone.

        After restore + subsequent fusion rebalance the control plane should
        detach and delete the pre-restore guest volumes.  This polls until all
        of them disappear or the timeout expires.

        :param cluster: Cluster object
        :param pre_restore_volume_ids: Volume IDs captured before the restore
        :param timeout: Max seconds to wait
        :return: True once all pre-restore volumes are no longer tagged to this cluster
        """
        if not pre_restore_volume_ids:
            self.log.info(
                f"No pre-restore guest volumes to verify for cluster {cluster.id}"
            )
            return True

        target_ids = set(pre_restore_volume_ids)
        self.log.info(
            f"Waiting for {len(target_ids)} pre-restore guest volumes to be "
            f"deleted on cluster {cluster.id}: {target_ids}"
        )
        deadline = time.time() + timeout
        while time.time() < deadline:
            current_ids = set(self.get_current_guest_volume_ids(cluster))
            still_present = target_ids & current_ids
            if not still_present:
                self.log.info(
                    f"All pre-restore guest volumes deleted for cluster {cluster.id}"
                )
                return True
            self.log.info(
                f"Still waiting — pre-restore volumes still present on {cluster.id}: "
                f"{still_present}"
            )
            time.sleep(30)

        remaining = target_ids & set(self.get_current_guest_volume_ids(cluster))
        self.log.error(
            f"Timed out ({timeout}s): pre-restore volumes still present on "
            f"{cluster.id}: {remaining}"
        )
        return False

    def verify_guest_volume_snapshots_for_backup(self, cluster, backup_id: str, num_snapshots: int) -> bool:
        """
        Verify EBS guest volume snapshots exist for a completed backup.

        The backup process creates EBS snapshots tagged with:
          - couchbase-cloud-guestvolume: "true"  (identifies fusion guest volume snapshots)
          - couchbase-cloud-backup-id: <backup_id>
          - couchbase-cloud-cluster-id: <cluster_id>

        :param cluster: Cluster object with .id attribute
        :param backup_id: Backup ID from the completed backup
        :return: True if at least one guest volume snapshot found with a completed state
        """

        filters = [
            {"Name": "tag:couchbase-cloud-guestvolume", "Values": ["true"]},
            {"Name": "tag:couchbase-cloud-backup-id", "Values": [backup_id]},
            {"Name": "tag:couchbase-cloud-cluster-id", "Values": [cluster.id]},
        ]
        snapshots = self.fusion_aws_util.ec2.list_snapshots_by_tags(filters)

        table = PrettyTable()
        table.field_names = ["Snapshot ID", "State", "Volume Size (GiB)", "Progress", "Start Time"]
        for snap in snapshots:
            start = snap.get("StartTime")
            table.add_row([
                snap.get("SnapshotId"),
                snap.get("State"),
                snap.get("VolumeSize"),
                snap.get("Progress"),
                start.strftime("%Y-%m-%d %H:%M:%S") if start else "N/A",
            ])
        self.log.info(f"Guest volume snapshots for backup {backup_id} on cluster {cluster.id}:\n{table}")

        if len(snapshots) != num_snapshots:
            self.log.error(
                f"Mismatch in number of guest volume snapshots for backup {backup_id} on cluster {cluster.id}"
            )
            return False

        completed = [s for s in snapshots if s.get("State") == "completed"]
        self.log.info(
            f"Snapshot summary for backup {backup_id}: "
            f"{len(snapshots)} total, {len(completed)} completed"
        )
        return True

    def monitor_full_cluster_teardown(self, cluster_id, resources, timeout=600):
        """
        Poll AWS until every resource the CP creates for a fusion cluster is
        gone, then return a list of human-readable failure strings (empty
        list == fully clean).

        Checked resources:
          - ALL EBS volumes tagged to the cluster (couchbase-cloud-cluster-id
            only, no function filter) -- covers CBS/KV node data/root
            volumes, accelerator root/boot volumes, and guest volumes alike.
          - Fusion guest volumes specifically (couchbase-cloud-function=
            fusion-accelerator AND couchbase-cloud-fusion-guest-volume=true)
            -- reported separately from the broad check above so a failure
            message can distinguish "some cluster volume didn't clean up"
            from "a fusion guest volume specifically didn't clean up". Not
            just a subset check of the broad list: the two use different tag
            filters, so an untagged/mistagged guest volume would surface
            here even if the broad check happened to look clean.
          - Fusion Auto Scaling Groups
          - Fusion accelerator EC2 instances
          - All cluster EC2 nodes (CBS/KV) in a non-terminated state
          - Fusion S3 log-store bucket (if resources['s3_bucket_name'] set)
          - Accelerator IAM instance profile (if resources['iam_profile_name'] set)

        This is monitoring/verification logic only -- it returns data, it
        never asserts. Callers (test layer) decide how to fail the test.

        :param cluster_id: Cluster identifier the resources belong to
        :param resources: dict with optional 's3_bucket_name' and
            'iam_profile_name' keys -- see
            FusionClusterDestroyTest._capture_pre_destroy_resources()
        :param timeout: Max seconds to poll for full cleanup before taking a
            final point-in-time snapshot for the failure report
        :return: list of failure description strings (empty == fully clean)
        """
        s3_bucket_name = resources.get("s3_bucket_name")
        iam_profile_name = resources.get("iam_profile_name")
        acc_filter = self.fusion_aws_util._cluster_filter(
            cluster_id,
            [{'Name': 'tag:couchbase-cloud-function', 'Values': ['fusion-accelerator']}])
        all_nodes_filter = self.fusion_aws_util._cluster_filter(cluster_id)

        def _all_cluster_volumes():
            return self.fusion_aws_util.ec2.list_volumes_by_cluster_id(filters={
                "couchbase-cloud-cluster-id": cluster_id,
            })

        def _guest_volumes():
            return self.fusion_aws_util.ec2.list_volumes_by_cluster_id(filters={
                "couchbase-cloud-cluster-id": cluster_id,
                "couchbase-cloud-function": "fusion-accelerator",
                "couchbase-cloud-fusion-guest-volume": "true",
            })

        def _running_nodes():
            all_nodes = self.fusion_aws_util.list_instances(
                all_nodes_filter, log="ClusterNodeCheck", suppress_log=True)
            return [
                n for n in all_nodes
                if n.get("State", {}).get("Name") not in ("terminated", "shutting-down")
            ]

        def _s3_bucket_exists():
            try:
                self.fusion_aws_util.s3.s3_client.head_bucket(Bucket=s3_bucket_name)
                return True
            except Exception:
                return False

        deadline = time.time() + timeout
        while time.time() < deadline:
            all_volumes = _all_cluster_volumes()
            guest_volumes = _guest_volumes()
            asgs = self.fusion_aws_util.list_cluster_fusion_asg(cluster_id)
            acc_instances = self.fusion_aws_util.list_accelerator_instances(
                acc_filter, log="DestroyCleanup")
            running_nodes = _running_nodes()
            if not all_volumes and not guest_volumes and not asgs \
                    and not acc_instances and not running_nodes:
                self.log.info(
                    f"All compute resources cleaned up for cluster {cluster_id}")
                break
            self.log.info(
                f"Cleanup in progress — {len(all_volumes)} cluster EBS vols "
                f"({len(guest_volumes)} guest vols), {len(asgs)} ASGs, "
                f"{len(acc_instances)} acc instances, {len(running_nodes)} cluster nodes — "
                f"{int(deadline - time.time())}s remaining")
            time.sleep(15)

        # Final point-in-time checks across all resource types
        failures = []

        final_all_volumes = _all_cluster_volumes()
        if final_all_volumes:
            failures.append(
                f"{len(final_all_volumes)} cluster EBS volume(s) remain (any type): "
                f"{[v['VolumeId'] for v in final_all_volumes]}")

        final_guest_volumes = _guest_volumes()
        if final_guest_volumes:
            failures.append(
                f"{len(final_guest_volumes)} fusion guest volume(s) remain: "
                f"{[v['VolumeId'] for v in final_guest_volumes]}")

        final_asgs = self.fusion_aws_util.list_cluster_fusion_asg(cluster_id)
        if final_asgs:
            failures.append(f"{len(final_asgs)} ASG(s) remain: "
                            f"{[a['AutoScalingGroupName'] for a in final_asgs]}")

        final_acc = self.fusion_aws_util.list_accelerator_instances(
            acc_filter, log="FinalAccCheck")
        if final_acc:
            failures.append(f"{len(final_acc)} accelerator instance(s) remain: "
                            f"{[i['InstanceId'] for i in final_acc]}")

        running_final = _running_nodes()
        if running_final:
            failures.append(f"{len(running_final)} cluster node(s) still running: "
                            f"{[n['InstanceId'] for n in running_final]}")

        if s3_bucket_name:
            if _s3_bucket_exists():
                failures.append(f"S3 bucket {s3_bucket_name} still exists")
            else:
                self.log.info(f"S3 bucket {s3_bucket_name} confirmed deleted")

        if iam_profile_name:
            if self.fusion_aws_util.ec2.check_iam_instance_profile_exists(iam_profile_name):
                failures.append(f"IAM instance profile {iam_profile_name} still exists")
            else:
                self.log.info(f"IAM instance profile {iam_profile_name} confirmed deleted")

        if not failures:
            self.log.info(f"All AWS resources verified clean for cluster {cluster_id}")
        return failures
