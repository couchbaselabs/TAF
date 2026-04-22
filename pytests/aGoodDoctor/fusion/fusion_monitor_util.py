'''
Created on Mar 24, 2025

Fusion Monitor Util - Core fusion stats monitoring utilities
'''

from membase.api.rest_client import RestConnection
from prettytable import PrettyTable
import ast
import json
import socket
import statistics
from datetime import datetime
from BucketLib.BucketOperations import BucketHelper
from couchbase_utils.cb_server_rest_util.fusion.fusion_api import FusionRestAPI


class FusionMonitorUtil():
    """Utility class for monitoring core fusion stats and status."""

    VBUCKET_COUNT = 128
    DEFAULT_TIMEOUT = 1800

    def __init__(self, logger, fusion_aws_util):
        """
        Initialize Fusion Monitor Util.

        :param logger: Logger instance for logging
        :param fusion_aws_util: FusionAWSUtil instance for AWS operations
        """
        self.log = logger
        self.fusion_aws_util = fusion_aws_util
        # hostedOPD.__init__(self)

    def set_admin_credentials(self, cluster):
        """Set admin credentials for the cluster."""
        fusion_aws_util = self.fusion_aws_util
        if hasattr(cluster, 'rest_username') and cluster.rest_username:
            cluster.master.rest_username = cluster.rest_username
            cluster.master.rest_password = cluster.rest_password
            cluster.master.username = cluster.rest_username
            cluster.master.password = cluster.rest_password
        else:
            secret_name = f"{cluster.id}_dp-admin"
            secret = fusion_aws_util.secrets.get_secret_by_name(secret_name)
            cluster.master.rest_username = "couchbase-cloud-admin"
            cluster.master.rest_password = r"{}".format(secret.get('SecretValue'))
            cluster.rest_username = "couchbase-cloud-admin"
            cluster.rest_password = r"{}".format(secret.get('SecretValue'))

        self.log.info(f"Rest Username = {cluster.master.rest_username}, Rest Password = {cluster.master.rest_password}")

    def log_fusion_pending_bytes(self, tenant, clusters, find_master_func):
        """
        Log fusion pending bytes for all clusters.

        :param clusters: List of cluster objects
        """
        for cluster in clusters:
            self.set_admin_credentials(cluster)
            find_master_func(tenant, cluster)
            status, content = FusionRestAPI(cluster.master).get_fusion_status()
            if status:
                table = PrettyTable()
                table.field_names = ["Node ID", "Bucket Name", "Pending Bytes", "Sync Session Completed Bytes", "Sync Session Total Bytes"]
                nodes = content.get("nodes") or {}
                for node, stats in nodes.items():
                    buckets = stats.get("buckets") or {}
                    for bucket_name, bucket_stats in buckets.items():
                        pending_bytes = bucket_stats.get("snapshotPendingBytes")
                        syncSessionCompletedBytes = bucket_stats.get("syncSessionCompletedBytes")
                        syncSessionTotalBytes = bucket_stats.get("syncSessionTotalBytes")
                        table.add_row([node, bucket_name, pending_bytes, syncSessionCompletedBytes, syncSessionTotalBytes])
                self.log.info(f"Fusion Pending Bytes for cluster {cluster.id}:\n{table}")
            else:
                self.log.error(f"Failed to get Fusion status for cluster {cluster.id}: {content}")

    def wait_for_fusion_status(self, cluster, state="enabled", timeout=None):
        """
        Wait for fusion to reach a specific state.

        :param cluster: Cluster object
        :param state: Expected fusion state (default: "enabled")
        :param timeout: Timeout in seconds (default: DEFAULT_TIMEOUT)
        :return: True when state is reached, raises assertion if timeout
        """
        if timeout is None:
            timeout = self.DEFAULT_TIMEOUT
        self.set_admin_credentials(cluster)
        import time
        start_time = time.time()
        while time.time() - start_time < timeout:
            status, content = FusionRestAPI(cluster.master).get_fusion_status()
            self.log.info(f"Status = {status}, Content = {content}")
            if content['state'] == state:
                return
            time.sleep(10)
        raise AssertionError(f"Fusion is not {state} on cluster {cluster.id}")

    def get_fusion_s3_uri(self, cluster, bucket_name=None):
        """
        Return the ep_magma_fusion_logstore_uri (Fusion S3 URI) from any one node in the cluster.

        :param cluster: Cluster object
        :param bucket_name: Optional bucket name; defaults to first bucket or "default"
        :return: S3 URI string or None if not found
        """
        self.set_admin_credentials(cluster)
        bucket = bucket_name or (cluster.buckets[0].name if getattr(cluster, "buckets", None) else "default")

        try:
            instances = self.fusion_aws_util.list_instances(
                self.fusion_aws_util._cluster_filter(cluster.id),
                suppress_log=True
            )
        except Exception as e:
            self.log.error(f"Instance discovery failed for cluster {cluster.id}: {e}")
            return None

        if not instances:
            self.log.warning(f"No instances found for cluster {cluster.id}")
            return None

        stat_key = "ep_magma_fusion_logstore_uri"
        cmd = (
            f"/opt/couchbase/bin/cbstats localhost:11210 all "
            f"-b {bucket} "
            f"-u {cluster.master.rest_username} "
            f"-p '{cluster.master.rest_password}' "
            f"| grep {stat_key}"
        )

        for inst in instances:
            instance_id = inst.get("InstanceId")
            try:
                result = self.fusion_aws_util.ec2.run_shell_command(instance_id, cmd)
                if not result.get("success"):
                    continue
                stdout = (result.get("stdout") or "").strip()
                if not stdout:
                    continue

                for line in stdout.splitlines():
                    if stat_key in line:
                        uri = None
                        if "s3://" in line:
                            start = line.find("s3://")
                            uri = line[start:].strip()
                        else:
                            parts = line.split(":", 1)
                            if len(parts) == 2:
                                uri = parts[1].strip()
                        if uri:
                            uri = uri.strip().strip('"').strip("'")
                            self.log.info(f"Fusion S3 URI for bucket {bucket} on cluster {cluster.id}: {uri}")
                            return uri
            except Exception as e:
                self.log.debug(f"cbstats on {instance_id} failed: {e}")

        self.log.warning(f"{stat_key} not found on any node in cluster {cluster.id}")
        return None

    def get_fusion_log_store_data_size_on_s3(self, cluster, bucket):
        """
        Get fusion log store data size on S3 for a bucket.

        :param cluster: Cluster object
        :param bucket: Bucket object
        :return: None (updates bucket.total_s3_size attribute)
        """
        uri = self.get_fusion_s3_uri(cluster, bucket.name)
        uri = uri.split("?")[0] if uri else None
        bucket_uuid = None
        if uri:
            try:
                rest = RestConnection(cluster.master)
                bucket_uuid = None
                info = rest.get_bucket_details(bucket_name=bucket.name)
                bucket_uuid = info.get("uuid")

                if bucket_uuid:
                    self.log.info(f"Bucket UUID for {bucket.name}: {bucket_uuid}")
                else:
                    self.log.warning(f"Bucket UUID not found for {bucket.name}")
            except Exception as e:
                self.log.error(f"Failed to fetch bucket UUID for {bucket.name}: {e}")
            result = self.fusion_aws_util.s3.get_folder_sizes(uri.split("/")[-1], f"kv/{bucket_uuid}")
            bucket.total_s3_size = result.get("total_size_gb", 0)
            table = PrettyTable()
            table.field_names = ["Storage Class", "Size (GB)", "Files Count"]
            for storage_class, metadata in result.get("folders", {}).items():
                size = metadata.get("size_gb", 0)
                file_count = metadata.get("file_count", 0)
                table.add_row([storage_class, f"{size:.2f} GB", file_count])
            self.log.info(f"Fusion Log Store Data Size on S3 for bucket {bucket.name} on cluster {cluster.id}: \n{table}")
            self.log.info(f"Total Fusion Log Store Data Size on S3 for bucket {bucket.name} on cluster {cluster.id}: {bucket.total_s3_size:.2f} GB")
        return None

    def run_cbstats_on_all_nodes(self, cluster, bucket, stat_key="ep_fusion_log_store_data_size", subcommand="all"):
        """
        Run cbstats command on all Couchbase server nodes via SSM and grep for a specific stat.

        :param cluster: The cluster object containing node information
        :param bucket: The bucket object to query
        :param stat_key: The stat key to grep for (default: "ep_fusion_log_store_data_size")
        :return: Dictionary mapping node IPs to their stat values
        """
        self.set_admin_credentials(cluster)
        instances = self.fusion_aws_util.list_instances(
            self.fusion_aws_util._cluster_filter(cluster.id),
            log="Couchbase Cloud Cluster", suppress_log=True
        )

        cmd = (
            f"/opt/couchbase/bin/cbstats localhost:11210 {subcommand} "
            f"-b {bucket.name} "
            f"-u {cluster.master.rest_username} "
            f"-p '{cluster.master.rest_password}' "
            f"| grep {stat_key}"
        )
        self.log.info(f"Running cbstats command: {cmd}")

        def run_on_instance(instance):
            instance_id = instance.get('InstanceId')
            public_ip = instance.get('PublicIpAddress', 'N/A')
            try:
                result = self.fusion_aws_util.ec2.run_shell_command(instance_id, cmd)
                self.log.info(f"cbstats command result for instance {instance_id}: {result}")
                if result.get('success'):
                    stdout = result.get('stdout', '').strip()
                    if stdout:
                        for line in stdout.splitlines():
                            if stat_key in line:
                                parts = line.split(':')
                                value = parts[1].strip() if len(parts) >= 2 else line
                                return instance_id, public_ip, value, "Success"
                    return instance_id, public_ip, "N/A", "No output"
                else:
                    error = result.get('stderr', 'Unknown error')
                    return instance_id, public_ip, f"Error: {error[:50]}", f"Error: {error[:50]}"
            except Exception as e:
                return instance_id, public_ip, f"Exception: {str(e)[:50]}", f"Exception: {str(e)[:50]}"

        from concurrent.futures import ThreadPoolExecutor, as_completed
        rows = []
        with ThreadPoolExecutor(max_workers=len(instances) or 1) as executor:
            futures = {executor.submit(run_on_instance, inst): inst for inst in instances}
            for future in as_completed(futures):
                rows.append(future.result())
        return rows

    def _populate_fusion_uploader_map(self, tenant, cluster, find_master_func=None):
        """
        Populate fusion uploader map from the cluster.

        :param tenant: Tenant object
        :param cluster: Cluster object
        :param find_master_func: Optional callback function to find master node (signature: find_master_func(tenant, cluster))
        """
        instances = self.fusion_aws_util.list_instances(
            self.fusion_aws_util._cluster_filter(cluster.id),
            log="Couchbase Cloud Cluster"
        )
        try:
            if find_master_func:
                find_master_func(tenant, cluster)
            self.set_admin_credentials(cluster)
            for bucket in cluster.buckets:
                server_vb_map, server_list, num_replica = BucketHelper(cluster.master).get_vbucket_map_and_server_list(bucket.name)
                cluster.fusion_uploader_dict[bucket.name] = dict()
                cmd = f"curl -sk -u '{cluster.master.rest_username}:{cluster.master.rest_password}' --data 'ns_bucket:get_fusion_uploaders(\"{cluster.buckets[0].name}\").' https://localhost:18091/diag/eval"
                self.log.info(f"Running command to get fusion uploader map for bucket {bucket.name} on cluster {cluster.id}: {cmd}")
                result = self.fusion_aws_util.ec2.run_shell_command(instances[0].get('InstanceId'), cmd)
                stdout = result.get('stdout')
                if result.get('success') and stdout:
                    raw_str = stdout.replace("\n", "")
                    tuple_str = raw_str.replace("{", "(").replace("}", ")").replace("'", '"').replace("undefined", "None")
                    try:
                        parsed_list = ast.literal_eval(tuple_str)
                    except Exception as e:
                        self.log.error(f"Failed to parse fusion uploader map for bucket {bucket.name}: {e}. Raw: {raw_str}")
                        continue
                    vb_no = 0
                    for node in parsed_list:
                        if node[0] in cluster.fusion_uploader_dict[bucket.name]:
                            cluster.fusion_uploader_dict[bucket.name][node[0]] += 1
                        else:
                            cluster.fusion_uploader_dict[bucket.name][node[0]] = 1
                        if vb_no not in cluster.fusion_vb_uploader_map[bucket.name].keys():
                            cluster.fusion_vb_uploader_map[bucket.name][vb_no] = dict()
                        if node[0] != None:
                            node_name = node[0].split("@")[1].split(".")[0]
                            vb_nodes_indexes = server_vb_map[vb_no]
                            node_index = server_list.index(node[0].split("@")[1])
                            if vb_nodes_indexes[0] == node_index:
                                node_name = node_name + "-A"
                            else:
                                node_name = node_name + "-R"
                            if cluster.fusion_vb_uploader_map[bucket.name].get(vb_no, {}).get("node") is not None:
                                if node_name != cluster.fusion_vb_uploader_map[bucket.name][vb_no]["node"][-1]:
                                    cluster.fusion_vb_uploader_map[bucket.name][vb_no]["node"].append(node_name)
                            else:
                                cluster.fusion_vb_uploader_map[bucket.name][vb_no]["node"] = [node_name]
                        cluster.fusion_vb_uploader_map[bucket.name][vb_no]["term"] = int(node[1])
                        vb_no += 1
                else:
                    self.log.error(f"Fusion uploader command returned no output for bucket {bucket.name} on cluster {cluster.id}")
                    continue
        except Exception as e:
            self.log.error(e)

    def log_fusion_uploader_map(self, cluster):
        """
        Log fusion uploader map for the cluster.

        :param cluster: Cluster object
        """
        fusion_uploader_dict = getattr(cluster, 'fusion_uploader_dict', {})
        fusion_vb_uploader_map = getattr(cluster, 'fusion_vb_uploader_map', {})

        for bucket_name, nodes in fusion_uploader_dict.items():
            uploader_table = PrettyTable()
            uploader_table.field_names = ["Bucket Name", "Node", "Uploader Count"]
            final_count = 0
            for node, count in nodes.items():
                final_count += count
                uploader_table.add_row([bucket_name, node, count])
            self.log.info(f"Fusion Uploader Distribution for bucket {bucket_name}:\n{uploader_table}")

        vb_table = PrettyTable()
        vb_table.field_names = ["Bucket Name", "VB No", "Node", "Term"]
        for bucket_name, vb_map in fusion_vb_uploader_map.items():
            vb_count = 0
            for vb_no, details in vb_map.items():
                vb_count += 1
                vb_table.add_row([bucket_name, vb_no, details.get("node"), details.get("term")])
            self.log.info(f"Fusion VB Uploader Map for bucket {bucket_name}:\n{vb_table}")
            if vb_count != self.VBUCKET_COUNT:
                self.log.critical(f"VB Uploader Count: {vb_count} is not equal to number of vBuckets: 128")

    def get_fusion_uploader_map(self, tenant, cluster, find_master_func=None):
        """
        Get fusion uploader map for the cluster.

        :param tenant: Tenant object
        :param cluster: Cluster object
        :param find_master_func: Optional callback function to find master node (signature: find_master_func(tenant, cluster))
        """
        import time

        max_retries = 5
        for retry in range(max_retries):
            self._populate_fusion_uploader_map(tenant, cluster, find_master_func)

            all_correct = True
            fusion_uploader_dict = getattr(cluster, 'fusion_uploader_dict', {})
            for bucket_name, nodes in fusion_uploader_dict.items():
                final_count = sum(nodes.values())
                if final_count != self.VBUCKET_COUNT:
                    all_correct = False
                    self.log.warning(f"Retry {retry + 1}/{max_retries}: Final Uploader Count: {final_count} is not equal to number of vBuckets: 128")
                    break

            if all_correct:
                break

            if retry < max_retries - 1:
                time.sleep(5)

        self.log_fusion_uploader_map(cluster)

    def log_fusion_log_store_data_size(self, clusters):
        """
        Log fusion log store data size for all buckets in all clusters.

        :param clusters: List of cluster objects
        """
        for cluster in clusters:
            for bucket in cluster.buckets:
                try:
                    rows = self.run_cbstats_on_all_nodes(cluster, bucket=bucket, stat_key="ep_fusion_log_store_data_size")
                    table = PrettyTable()
                    table.field_names = ["Instance ID", "Public IP", "Bucket", "Size (GB)", "Status"]
                    total_size_gb = 0
                    for instance_id, public_ip, raw_value, status in rows:
                        try:
                            gb = int(str(raw_value).replace(",", "").strip()) / (1024 ** 3)
                            total_size_gb += gb
                            display = f"{gb:.4f}"
                        except (ValueError, AttributeError):
                            display = raw_value
                        table.add_row([instance_id, public_ip, bucket.name, display, status])
                    bucket.fusion_log_store_data_size_gb = total_size_gb
                    self.log.info(f"Fusion Log Store Data Size for bucket {bucket.name} on cluster {cluster.id}:\n{table}")
                    self.get_fusion_log_store_data_size_on_s3(cluster, bucket)
                    self.log.info(f"{bucket.name} Fusion Log Store Data Size (GB): {bucket.fusion_log_store_data_size_gb:.4f}")
                    self.log.info(f"{bucket.name} Fusion Log Store Data Size on S3 (GB): {bucket.total_s3_size}")
                except Exception as e:
                    self.log.debug(f"cbstats monitor exception: {e}")

    def log_fusion_dcp_items_remaining(self, clusters):
        """
        Log DCP items remaining for all buckets in all clusters.

        :param clusters: List of cluster objects
        """
        for cluster in clusters:
            for bucket in cluster.buckets:
                try:
                    rows = self.run_cbstats_on_all_nodes(cluster, bucket,
                                                         stat_key="ep_dcp_items_remaining",
                                                         subcommand="dcp")
                    table = PrettyTable()
                    table.field_names = ["Instance ID", "Public IP", "Bucket", "ep_dcp_items_remaining", "Status"]
                    for instance_id, public_ip, value, status in rows:
                        table.add_row([instance_id, public_ip, bucket.name, value, status])
                    self.log.info(f"DCP items remaining for bucket {bucket.name} on cluster {cluster.id}:\n{table}")
                except Exception as e:
                    self.log.debug(f"DCP items remaining monitor exception for cluster {cluster.id}, bucket {bucket.name}: {e}")

    def get_attached_ebs_volumes_count(self, tenant, cluster, timeout=None, find_master_func=None):
        """
        Get count of attached EBS guest volumes.

        :param tenant: Tenant object
        :param cluster: Cluster object
        :param timeout: Timeout in seconds (default: DEFAULT_TIMEOUT)
        :param find_master_func: Optional callback function to find master node (signature: find_master_func(tenant, cluster))
        :return: Number of attached EBS volumes
        """
        if timeout is None:
            timeout = self.DEFAULT_TIMEOUT
        attached_volumes = 0
        import time
        start_time = time.time()
        while True:
            if time.time() - start_time > timeout:
                self.log.info(f"Timeout reached. No attached guest volumes found for cluster {cluster.id}")
                return 0
            self.log.info(f"Getting active guest volumes for cluster {cluster.id}..")
            if find_master_func:
                find_master_func(tenant, cluster)
            status, content = FusionRestAPI(cluster.master).get_active_guest_volumes()
            if status:
                break
            self.log.critical(f"Failed to get active guest volumes for cluster {cluster.id}: {status}")
            time.sleep(2)
        for node_id in list(content):
            attached_volumes += len(content[node_id])
        if attached_volumes > 0:
            self.log.info(f"Attached EBS Guest Volumes: {attached_volumes} for cluster {cluster.id}")
        else:
            self.log.info(f"No attached guest volumes found for cluster {cluster.id}")
        return attached_volumes

    def get_hostname_public_ip_mapping(self, cluster, suppress_log=False):
        """
        Get hostname to public IP mapping for cluster nodes.

        :param cluster: Cluster object
        :param suppress_log: Whether to suppress logging
        """
        from membase.api.rest_client import RestConnection
        cluster.hostname_public_ip_mapping = dict()
        table = PrettyTable()
        table.field_names = ["Hostname", "Public IP"]
        self.set_admin_credentials(cluster)
        nodes = RestConnection(cluster.master).node_statuses()
        for node in nodes:
            try:
                public_ip = socket.gethostbyname(node.ip)
                cluster.hostname_public_ip_mapping[node.ip] = public_ip
                table.add_row([node.ip, public_ip])
            except Exception as e:
                self.log.error(f"Unexpected error resolving hostname '{node.ip}': {e}")
        if not suppress_log:
            self.log.info(f"Hostname and public IP mapping:\n{table}")
        for node in cluster.nodes_in_cluster:
            node.aws_public_ip = cluster.hostname_public_ip_mapping.get(node.ip)

    @staticmethod
    def _fmt_time(s):
        """Format seconds into a human-readable string."""
        if s is None:
            return "N/A"
        if s >= 60:
            return f"{int(s // 60)}m {s % 60:.1f}s"
        return f"{s:.1f}s"

    def _fetch_rebalance_report(self, cluster):
        """
        Fetch the latest NS Server rebalance report via SSM from a cluster node.

        Fetches /pools/default/tasks to get the lastReportURI, then fetches
        the full rebalance report JSON.

        :param cluster: Cluster object
        :return: Parsed rebalance report dict, or None on failure
        """
        self.set_admin_credentials(cluster)
        username = cluster.master.rest_username
        password = cluster.master.rest_password

        # Step 1: Get lastReportURI from /pools/default/tasks
        try:
            # api = f"https://{cluster.master.ip}:18091/pools/default/tasks"
            # result = RestConnection(cluster.master).urllib_request(api)
            # if result.status_code != 200:
            #     return None
            # tasks = json.loads(result.content.decode('utf-8'))
            # report_uri = None
            # for task in tasks:
            #     if task.get('type') == 'rebalance' and task.get('status') == 'notRunning':
            #         report_uri = task.get('lastReportURI')
            #         break

            # if not report_uri:
            #     self.log.debug(f"No lastReportURI found in tasks on cluster {cluster.id}")
            #     return None

            # Step 2: Fetch the full rebalance report
            report_uri = f"https://{cluster.master.ip}:18091/logs/rebalanceReport"
            result = RestConnection(cluster.master).urllib_request(report_uri)
            if result.status_code != 200:
                return None
            report_stdout = result.content.decode('utf-8')

            return json.loads(report_stdout)

        except json.JSONDecodeError as e:
            self.log.debug(f"JSON parse error from instance {cluster.id}: {e}")
        except Exception as e:
            self.log.debug(f"Error fetching rebalance report from instance {cluster.id}: {e}")

        self.log.warning(f"Could not fetch rebalance report from any node in cluster {cluster.id}")
        return None

    def _analyze_rebalance_report(self, data, cluster_id):
        """
        Analyze the NS Server rebalance report and log a detailed timing breakdown.

        Produces a per-bucket breakdown of fusion mount, cache transfer (snapshot_waiting),
        DCP backfill, takeover, and persistence times as both absolute duration and
        percentage of the end-to-end rebalance time.

        :param data: Parsed rebalance report JSON dict
        :param cluster_id: Cluster ID for logging context
        """
        fmt = "%Y-%m-%dT%H:%M:%S.%fZ"

        stage_info = data.get("stageInfo", {})
        di = stage_info.get("data")
        if not di:
            self.log.warning(f"Rebalance report for cluster {cluster_id} has no 'data' stageInfo")
            return

        start_time_str = di.get("startTime")
        completed_time_str = di.get("completedTime")
        if not start_time_str or not completed_time_str:
            self.log.warning(f"Rebalance report for cluster {cluster_id} missing start/completed time")
            return

        try:
            t0 = datetime.strptime(start_time_str, fmt)
            t1 = datetime.strptime(completed_time_str, fmt)
        except ValueError as e:
            self.log.warning(f"Could not parse rebalance timestamps for cluster {cluster_id}: {e}")
            return

        e2e = (t1 - t0).total_seconds()
        if e2e <= 0:
            self.log.warning(f"Invalid E2E duration ({e2e}s) for cluster {cluster_id}")
            return

        details = di.get("details", {})
        if not details:
            self.log.info(f"Rebalance report for cluster {cluster_id}: E2E={self._fmt_time(e2e)}, no bucket details")
            return

        for bucket_name, bucket_data in details.items():
            # Fusion mount wall-clock time
            mv = bucket_data.get("mountingVolumes", {})
            mount_s = None
            if mv.get("startTime") and mv.get("completedTime"):
                try:
                    mount_s = (
                        datetime.strptime(mv["completedTime"], fmt)
                        - datetime.strptime(mv["startTime"], fmt)
                    ).total_seconds()
                except (ValueError, TypeError):
                    mount_s = None

            vbuckets = bucket_data.get("vbucketLevelInfo", {}).get("vbucketInfo", [])
            if not vbuckets:
                self.log.info(
                    f"Rebalance Report [{cluster_id}] Bucket={bucket_name}: "
                    f"E2E={self._fmt_time(e2e)}, no vbucket info"
                )
                continue

            # Find start of first vBucket move
            move_starts = []
            for vb in vbuckets:
                move_st = vb.get("move", {}).get("startTime")
                if isinstance(move_st, str):
                    try:
                        move_starts.append(datetime.strptime(move_st, fmt))
                    except ValueError:
                        pass

            if not move_starts:
                self.log.info(
                    f"Rebalance Report [{cluster_id}] Bucket={bucket_name}: "
                    f"E2E={self._fmt_time(e2e)}, no vBucket move start times"
                )
                continue

            first_move = min(move_starts)
            move_wall = (t1 - first_move).total_seconds()

            # Accumulate sums for weighted ratio
            sums = dict(move=0, backfill=0, snap=0, dcp_backfill=0, takeover=0, persist=0)
            snap_bf_ratios = []

            for vb in vbuckets:
                move_t = vb.get("move", {}).get("timeTaken")
                backfill_t = vb.get("backfill", {}).get("timeTaken")
                snap_t = vb.get("snapshot_waiting", {}).get("timeTaken")
                takeover_t = vb.get("takeover", {}).get("timeTaken")
                persist_t = vb.get("persistence", {}).get("timeTaken")
                if not move_t:
                    continue
                sums["move"] += move_t
                sums["backfill"] += backfill_t or 0
                sums["snap"] += snap_t or 0
                sums["takeover"] += takeover_t or 0
                sums["persist"] += persist_t or 0
                if backfill_t and snap_t is not None:
                    sums["dcp_backfill"] += backfill_t - snap_t
                if backfill_t and backfill_t > 0 and snap_t is not None:
                    snap_bf_ratios.append(snap_t / backfill_t)

            def weighted_wall(stage_sum):
                if sums["move"] == 0:
                    return 0.0
                return (stage_sum / sums["move"]) * move_wall

            # Build PrettyTable report
            table = PrettyTable()
            table.field_names = ["Stage", "Ratio", "Time", "% E2E"]
            table.align["Stage"] = "l"
            table.align["Ratio"] = "r"
            table.align["Time"] = "r"
            table.align["% E2E"] = "r"

            def add_row(label, stage_sum=None, fixed_s=None):
                if fixed_s is not None:
                    approx = fixed_s
                    ratio_col = ""
                else:
                    approx = weighted_wall(stage_sum)
                    ratio = stage_sum / sums["move"] if sums["move"] else 0
                    ratio_col = f"{ratio * 100:.1f}%"
                pct = f"{approx / e2e * 100:.1f}%" if e2e > 0 else "N/A"
                table.add_row([label, ratio_col, self._fmt_time(approx), pct])

            add_row("fusion mount", fixed_s=mount_s)
            add_row("snapshot_waiting (cache transfer)", sums["snap"])
            add_row("dcp_backfill (backfill-snapshot_waiting)", sums["dcp_backfill"])
            add_row("takeover", sums["takeover"])
            add_row("persistence", sums["persist"])

            # Summary row
            table.add_row(["---", "---", "---", "---"])
            table.add_row(["total (E2E)", "", self._fmt_time(e2e), "100.0%"])

            summary_lines = [
                f"Rebalance Report [{cluster_id}] Bucket={bucket_name}",
                f"  Start: {start_time_str}\n  End: {completed_time_str}\n  E2E: {self._fmt_time(e2e)}",
            ]
            if snap_bf_ratios:
                mean_sb = statistics.mean(snap_bf_ratios)
                median_sb = statistics.median(snap_bf_ratios)
                summary_lines.append(
                    f"  cache transfer / backfill  mean {mean_sb * 100:.1f}%  median {median_sb * 100:.1f}%"
                )
                if move_wall > 0:
                    summary_lines.append(
                        f"  avg concurrent vb moves    {sums['move'] / 1000 / move_wall:.1f}"
                    )

            self.log.info("\n".join(summary_lines) + f"\n{table}")

    def log_rebalance_report(self, cluster):
        """
        Fetch and log the NS Server rebalance report for a cluster.

        Retrieves the latest rebalance report from the cluster via SSM and
        logs a detailed per-bucket timing breakdown including fusion mount,
        cache transfer, DCP backfill, takeover, and persistence stages.

        :param cluster: Cluster object
        """
        self.log.info(f"Fetching NS Server rebalance report for cluster {cluster.id}...")
        try:
            data = self._fetch_rebalance_report(cluster)
            if data:
                self._analyze_rebalance_report(data, cluster.id)
            else:
                self.log.warning(f"No rebalance report available for cluster {cluster.id}")
        except Exception as e:
            self.log.error(f"Failed to fetch/analyze rebalance report for cluster {cluster.id}: {e}")
