import ast
from collections import deque
from copy import deepcopy
import json
import os
import subprocess
import threading
import time

from TestInput import TestInputServer
from basetestcase import BaseTestCase
from cb_server_rest_util.buckets.buckets_api import BucketRestApi
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_server_rest_util.fusion.fusion_api import FusionRestAPI
from cb_tools.cbstats import Cbstats
from rebalance_utils.rebalance_util import RebalanceUtil
from shell_util.remote_connection import RemoteMachineShellConnection
from custom_exceptions.exception import RebalanceFailedException
from cb_constants import CbServer


nfs_server_remove = """
                    systemctl stop nfs-kernel-server;
                    apt remove --purge -y nfs-kernel-server nfs-common rpcbind;
                    apt autoremove -y;
                    rm -rf /etc/exports /var/lib/nfs /etc/idmapd.conf /data/nfs/share;
                    rm -rf /root/nfs_scripts;
                    rm -rf /data/nfs
                    """

nfs_client_remove = """
            umount -lf /mnt/nfs/share;
            sed -i '/nfs/d' /etc/fstab;
            systemctl stop nfs-client.target;
            systemctl disable nfs-client.target;
            apt remove --purge -y nfs-common rpcbind || yum remove -y nfs-utils rpcbind || dnf remove -y nfs-utils rpcbind || zypper remove -y nfs-client rpcbind;
            apt autoremove -y;
            rm -rf /etc/idmapd.conf /var/lib/nfs;
            ps aux | grep nfs | awk '{print $2}' | xargs kill -9 || true;
            rm -rf /root/nfs_scripts;
            rm -rf /mnt/nfs;
            """

class FusionBase(BaseTestCase):
    def setUp(self):
        super(FusionBase, self).setUp()

        self.nfs_server_ip = self.input.param("nfs_server_ip", "172.23.219.42")
        self.nfs_server_path = self.input.param("nfs_server_path", "/data/nfs/share/buckets")

        self.skip_fusion_setup = self.input.param("skip_fusion_setup", False)

        # Virtual env path for rebalance script execution on the remote node
        self.venv_path = "/root/myenv"

        # This is where all the fusion scripts (rebalance, accelerator, config file) will be stored
        self.fusion_scripts_dir = "/root/fusion"

        # This is where all the NFS scripts (client setup, server setup) will be stored
        self.nfs_scripts_dir = "/root/nfs_scripts"

        self.nfs_server = TestInputServer()
        self.nfs_server.ip = self.nfs_server_ip
        self.nfs_server.ssh_username = "root"
        self.nfs_server.ssh_password = "couchbase"

        self.script_path = os.path.abspath(__file__)
        self.local_test_path = os.path.dirname(self.script_path)

        self.log.info(f"Full script path: {self.script_path}")
        self.log.info(f"Test file directory: {self.local_test_path}")
        split_path = self.local_test_path.split("/")

        self.local_scripts_path = "/" + os.path.join("/".join(split_path[1:4]), "scripts", "fusion_scripts")
        self.log.info(f"Local scripts path: {self.local_scripts_path}")

        self.fusion_output_dir = "/" + os.path.join("/".join(split_path[1:4]), "fusion_output")
        self.log.info(f"Fusion output dir = {self.fusion_output_dir}")
        subprocess.run(f"mkdir -p {self.fusion_output_dir}", shell=True, executable="/bin/bash")

        ip_cmd = """ip -o -4 addr show scope global | awk '{split($4,a,"/"); print a[1]}'"""
        result = subprocess.run(ip_cmd, shell=True, executable="/bin/bash", capture_output=True, text=True)
        self.slave_ip = result.stdout.strip()
        self.log.info(f"Slave IP = {self.slave_ip}")

        self.client_share_dir = "share"

        if not self.skip_fusion_setup:
            self.setup_nfs_server_new()

            th_arr1 = list()
            th_arr2 = list()

            for nfs_client in self.cluster.servers:
                th1 = threading.Thread(target=self.setup_nfs_client_new, args=[nfs_client])
                th2 = threading.Thread(target=self.copy_scripts_to_nodes, args=[nfs_client])
                th1.start()
                th2.start()
                th_arr1.append(th1)
                th_arr2.append(th2)

            for th in th_arr1:
                th.join()

            for th in th_arr2:
                th.join()
        else:
            self.log.info("Skipping Fusion Set Up")
            self.log.info("Deleting contents on log store")
            ssh = RemoteMachineShellConnection(self.nfs_server)
            nfs_base_path = "/".join(self.nfs_server_path.split("/")[:-1])
            nfs_cleanup_cmd = f"rm -rf {nfs_base_path}/*"
            self.log.info(f"Executing CMD: {nfs_cleanup_cmd}")
            o, e = ssh.execute_command(nfs_cleanup_cmd)
            ssh.disconnect()

        self.nfs_server_path = f"/data/nfs/{self.client_share_dir}/buckets"
        self.log.info(f"NFS Server path: {self.nfs_server_path}")

        self.monitor = True
        self.crash_loop = False

        self.retry_get_process_num = self.input.param("retry_get_process_num", 300)

        self.num_nodes_to_rebalance_in = self.input.param("num_nodes_to_rebalance_in", 0)
        self.num_nodes_to_rebalance_out = self.input.param("num_nodes_to_rebalance_out", 0)
        self.num_nodes_to_swap_rebalance = self.input.param("num_nodes_to_swap_rebalance", 0)

        self.spare_nodes = list()

        # Enable diag/eval on non-local hosts for all servers
        self.log.info("Enabling diag/eval on non-local hosts for all servers")
        for server in self.cluster.servers:
            shell = RemoteMachineShellConnection(server)
            output, error = shell.enable_diag_eval_on_non_local_hosts()
            shell.disconnect()
        self.reb_plan_uuids = list()


    def setup_nfs_server(self):

        self.log.info(f"Setting up NFS on server: {self.nfs_server.ip}")

        shell = RemoteMachineShellConnection(self.nfs_server)

        # Remove existing packages and dependencies
        self.log.info(f"Removing NFS packages on the server: {self.nfs_server.ip}")
        output, error = shell.execute_command(nfs_server_remove)
        shell.log_command_output(output, error)

        self.sleep(20, "Wait after removing packages")

        # Create required directories
        create_dir_cmd = f"mkdir -p {self.nfs_scripts_dir}"
        o, e = shell.execute_command(create_dir_cmd)

        # Copy over nfs server script
        self.log.info(f"Copying over NFS server script to server: {self.nfs_server.ip}")
        copy_cmd = 'sshpass -p "{2}" scp -o StrictHostKeyChecking=no' \
                    ' {3}/setup_server.sh root@{0}:{1}/setup_server.sh'\
                        .format(self.nfs_server.ip, self.nfs_scripts_dir, "couchbase", self.local_scripts_path)
        subprocess.run(copy_cmd, shell=True, executable="/bin/bash")

        # Provide execute permissions to the script and run it
        self.log.info(f"Running NFS server setup on {self.nfs_server.ip}")
        permissions_cmd = f"chmod +x {self.nfs_scripts_dir}/setup_server.sh; bash {self.nfs_scripts_dir}/setup_server.sh"
        o, e = shell.execute_command(permissions_cmd)
        shell.log_command_output(o, e)

        self.sleep(20, "Wait after setting up NFS server")

        shell.disconnect()


    def setup_nfs_client(self, client):

        self.log.info(f"Setting up NFS client on server: {client.ip}")

        ssh = RemoteMachineShellConnection(client)

        # Remove existing packages and dependencies
        self.log.info("Removing NFS client packages")
        o, e = ssh.execute_command(nfs_client_remove)

        self.sleep(30, "Wait after removing packages on client")

        df_cmd = "df -H"
        o, e = ssh.execute_command(df_cmd)
        self.log.info(f"After removing NFS client mount on {client.ip} = {o}")

        self.log.info(f"Creating NFS script directory on server: {client.ip}")
        create_cmd = f"mkdir -p {self.nfs_scripts_dir}"
        o, e = ssh.execute_command(create_cmd)

        # Copy over nfs server script
        self.log.info(f"Copying over NFS client setup script to: {client.ip}")
        copy_cmd = 'sshpass -p "{2}" scp -o StrictHostKeyChecking=no' \
                    ' {3}/setup_client_mount.sh root@{0}:{1}/setup_client_mount.sh'\
                        .format(client.ip, self.nfs_scripts_dir, "couchbase", self.local_scripts_path)
        subprocess.run(copy_cmd, shell=True, executable="/bin/bash")

        # Provide execute permissions to the script and run it
        self.log.info(f"Running NFS client script on server: {client.ip}")
        permissions_cmd = f"chmod +x {self.nfs_scripts_dir}/setup_client_mount.sh; bash {self.nfs_scripts_dir}/setup_client_mount.sh {self.nfs_server.ip}"
        o, e = ssh.execute_command(permissions_cmd)

        self.sleep(30, "Wait after setting up NFS client")

        o, e = ssh.execute_command(df_cmd)
        self.log.info(f"After mouting NFS client on {client.ip} = {o}")

        ssh.disconnect()


    def setup_nfs_server_new(self):

        shell = RemoteMachineShellConnection(self.nfs_server)

        # Create required directories
        create_dir_cmd = f"mkdir -p {self.nfs_scripts_dir}"
        o, e = shell.execute_command(create_dir_cmd)

        # Copy over nfs server script
        self.log.info(f"Copying over NFS server script to server: {self.nfs_server.ip}")
        copy_cmd = 'sshpass -p "{2}" scp -o StrictHostKeyChecking=no' \
                    ' {3}/setup_server_new.sh root@{0}:{1}/setup_server_new.sh'\
                    .format(self.nfs_server.ip, self.nfs_scripts_dir, "couchbase", self.local_scripts_path)
        subprocess.run(copy_cmd, shell=True, executable="/bin/bash")

        # Provide execute permissions to the script and run it
        self.log.info(f"Running NFS server setup on {self.nfs_server.ip}")
        run_cmd = f"chmod +x {self.nfs_scripts_dir}/setup_server_new.sh; bash {self.nfs_scripts_dir}/setup_server_new.sh"
        self.log.info(f"Server Setup CMD: {run_cmd}")
        o, e = shell.execute_command(run_cmd)
        shell.log_command_output(o, e)

        self.sleep(20, "Wait after setting up NFS server")

        new_share_dir = None
        for line in o:
            if line.startswith("NEW_SHARE_DIR="):
                new_share_dir = line.split("=", 1)[1].strip()
                break
        self.log.info(f"New Shared dir = {new_share_dir}")
        self.client_share_dir = new_share_dir.split("/")[-1]
        self.log.info(f"Client shared dir = {self.client_share_dir}")

        shell.disconnect()


    def setup_nfs_client_new(self, client):

        self.log.info(f"Setting up NFS client on server: {client.ip}")

        ssh = RemoteMachineShellConnection(client)

        # Remove existing packages and dependencies
        self.log.info("Removing NFS client packages")
        o, e = ssh.execute_command(nfs_client_remove)

        self.sleep(30, "Wait after removing packages on client")

        df_cmd = "df -H"
        o, e = ssh.execute_command(df_cmd)
        self.log.info(f"After removing NFS client mount on {client.ip} = {o}")

        self.log.info(f"Creating NFS script directory on server: {client.ip}")
        create_cmd = f"mkdir -p {self.nfs_scripts_dir}"
        o, e = ssh.execute_command(create_cmd)

        # Copy over nfs server script
        self.log.info(f"Copying over NFS client setup script to: {client.ip}")
        copy_cmd = 'sshpass -p "{2}" scp -o StrictHostKeyChecking=no' \
                    ' {3}/setup_client_mount_new.sh root@{0}:{1}/setup_client_mount_new.sh'\
                        .format(client.ip, self.nfs_scripts_dir, "couchbase", self.local_scripts_path)
        subprocess.run(copy_cmd, shell=True, executable="/bin/bash")

        # Provide execute permissions to the script and run it
        self.log.info(f"Running NFS client script on server: {client.ip}")
        run_cmd = f"chmod +x {self.nfs_scripts_dir}/setup_client_mount_new.sh; bash {self.nfs_scripts_dir}/setup_client_mount_new.sh {self.nfs_server.ip} {self.client_share_dir}"
        self.log.info(f"Client Setup CMD: {run_cmd}")
        o, e = ssh.execute_command(run_cmd)

        self.sleep(30, "Wait after setting up NFS client")

        o, e = ssh.execute_command(df_cmd)
        self.log.info(f"After mouting NFS client on {client.ip} = {o}")

        ssh.disconnect()

    def copy_scripts_to_nodes(self, client):

        self.log.info(f"Copying Fusion Scripts to server: {client.ip}")

        ssh = RemoteMachineShellConnection(client)

        remove_dir_cmd = f"rm -rf {self.fusion_scripts_dir}"
        o, e = ssh.execute_command(remove_dir_cmd)

        create_dir_cmd = f"mkdir -p {self.fusion_scripts_dir}"
        o, e = ssh.execute_command(create_dir_cmd)

        for script in ["run_fusion_rebalance.py", "run_local_accelerator.sh", "config.json"]:
            self.log.info(f"Copying script: {script} on server: {client.ip}")
            copy_cmd = 'sshpass -p "{2}" scp -o StrictHostKeyChecking=no' \
                ' {4}/{3} root@{0}:{1}/{3}'\
                .format(client.ip, self.fusion_scripts_dir, "couchbase", script, self.local_scripts_path)
            subprocess.run(copy_cmd, shell=True, executable="/bin/bash")

            self.log.info(f"Providing executable permission to {script}")
            permissions_cmd = f"chmod +x {self.fusion_scripts_dir}/{script}"
            o, e = ssh.execute_command(permissions_cmd)

        ssh.disconnect()

    def get_bucket_uuid(self, bucket):

        rest_client = BucketRestApi(self.cluster.master)
        status, json_parsed = rest_client.get_bucket_info(bucket)
        if status:
            bucket_uuid = json_parsed["uuid"]

        return bucket_uuid

    def get_kvstore_directories(self, bucket):

        kvstore_dirs = list()
        bucket_uuid = self.get_bucket_uuid(bucket)
        self.log.info(f"Bucket UUID: {bucket_uuid}")

        for kvstore in range(128):
            kvstore_path = f"{self.nfs_server_path}/kv/{bucket_uuid}/kvstore-{kvstore}"
            kvstore_dirs.append(kvstore_path)

        return kvstore_dirs

    def get_timestamp_dirs(self, ssh, dir):

        cmd = f"date +%s; stat -c '%s %Y %n' {dir}/*"
        output, _ = ssh.execute_command(cmd)
        timestamp = int(output[0].strip())

        log_dict = dict()
        for line in output[1:]:
            line_split = line.strip().split(" ")
            log_file = line_split[-1].split("/")[-1]
            if "tmp" in log_file:
                continue
            log_file_size = int(line_split[0])
            timestamp = int(line_split[1])
            log_dict[log_file] = [log_file_size, timestamp]

        return timestamp, log_dict

    def monitor_kvstores(self, bucket, dir, interval=1, validate=False):

        ssh = RemoteMachineShellConnection(self.nfs_server)

        ssh2 = RemoteMachineShellConnection(self.cluster.master)

        kvstore_num = int(dir.split("-")[1])
        self.kvstore_stats[bucket][kvstore_num]["term"] = -1
        self.kvstore_stats[bucket][kvstore_num]["checkpointLogID"] = list()
        self.kvstore_stats[bucket][kvstore_num]["leases"] = list()
        self.kvstore_stats[bucket][kvstore_num]["file_creation"] = deque([], maxlen=5)
        self.kvstore_stats[bucket][kvstore_num]["file_deletion"] = deque([], maxlen=5)

        self.kvstore_violations[bucket][kvstore_num]["file_creation"] = list()
        self.kvstore_violations[bucket][kvstore_num]["file_deletion"] = list()
        self.kvstore_violations[bucket][kvstore_num]["file_size"] = list()
        self.kvstore_violations[bucket][kvstore_num]["file_name"] = list()


        i = 0
        tmp_arr = dir.split("/")
        for i in range(len(tmp_arr)):
            if tmp_arr[i] == "buckets":
                break

        volume_name = "/".join(tmp_arr[i+1:])
        chronicle_cmd = '/opt/couchbase/bin/fusion/metadata_dump --uri "chronicle://localhost:8091" --volume-id "' + volume_name + '"'
        self.log.debug(f"Chronicle cmd = {chronicle_cmd}")

        prev_log_files = list()

        while self.monitor:
            curr_timestamp, curr_log_dict = self.get_timestamp_dirs(ssh, dir)

            o, e = ssh2.execute_command(chronicle_cmd)
            json_str = "\n".join(o)
            try:
                stat = json.loads(json_str)
                if stat["term"] != self.kvstore_stats[bucket][kvstore_num]["term"]:
                    self.kvstore_stats[bucket][kvstore_num]["term"] = stat["term"]
                if len(self.kvstore_stats[bucket][kvstore_num]["checkpointLogID"]) == 0 or \
                    (len(self.kvstore_stats[bucket][kvstore_num]["checkpointLogID"]) > 0 and \
                    stat["checkpoint"]["logID"] != self.kvstore_stats[bucket][kvstore_num]["checkpointLogID"][-1][0]):
                    self.kvstore_stats[bucket][kvstore_num]["checkpointLogID"].append([stat["checkpoint"]["logID"], curr_timestamp])
                if stat["leases"] != self.kvstore_stats[bucket][kvstore_num]["leases"]:
                    self.kvstore_stats[bucket][kvstore_num]["leases"] = stat["leases"]
            except Exception as e:
                self.log.debug(e)

            curr_log_files = list(curr_log_dict.keys())
            # Do a set difference to see if any log files have been added/deleted
            curr_log_files_set = set(curr_log_files)
            prev_log_files_set = set(prev_log_files)

            # This tells us what files have been added
            diff1 = curr_log_files_set - prev_log_files_set
            # This tells us if any log files have been deleted
            diff2 = prev_log_files_set - curr_log_files_set

            if len(diff1) != 0:
                for file in list(diff1):
                    file_creation_timestamp = curr_log_dict[file][1]
                    log_file_size = curr_log_dict[file][0]
                    if validate:
                        # Timestamp validation
                        if len(self.kvstore_stats[bucket][kvstore_num]["file_creation"]) > 0:
                            prev_timestamp = self.kvstore_stats[bucket][kvstore_num]["file_creation"][-1][1]
                            time_diff = curr_timestamp-prev_timestamp
                            if time_diff > self.fusion_upload_interval:
                                self.kvstore_violations[bucket][kvstore_num]["file_creation"].append([file, prev_timestamp, curr_timestamp, time_diff])

                            # Log Size Validation
                            if log_file_size > 104857600:
                                self.kvstore_violations[bucket][kvstore_num]["file_size"].append([file, log_file_size])

                            # Log file naming validation
                            prev_file_name = self.kvstore_stats[bucket][kvstore_num]["file_creation"][-1][0]
                            expected_log_name = "log-" + str(self.kvstore_stats[bucket][kvstore_num]["term"]) + "." + str(int(prev_file_name.split(".")[1]) + 1)
                            if file != expected_log_name:
                                self.kvstore_violations[bucket][kvstore_num]["file_name"].append([file, expected_log_name])

                    self.kvstore_stats[bucket][kvstore_num]["file_creation"].append([file, file_creation_timestamp, log_file_size])

            if len(diff2) != 0:
                for file in list(diff2):
                    log_file_checkpoint = self.kvstore_stats[bucket][kvstore_num]["checkpointLogID"][-1][0]
                    self.kvstore_stats[bucket][kvstore_num]["file_deletion"].append([file, log_file_checkpoint, curr_timestamp])
                    if validate:
                        checkpoint_log_id = int(log_file_checkpoint.split(".")[1])
                        log_file_delete_num = int(file.split(".")[1])
                        if log_file_delete_num >= checkpoint_log_id:
                            self.kvstore_violations[bucket][kvstore_num]["file_deletions"].append([file, log_file_checkpoint])

            prev_log_files = curr_log_files
            time.sleep(interval)

        ssh.disconnect()
        ssh2.disconnect()

    def start_monitor_dir(self, validate=False):

        monitor_threads = list()

        for bucket in self.cluster.buckets:
            kvstore_dirs = self.get_kvstore_directories(bucket.name)

            for dir in kvstore_dirs:
                self.log.debug(f"Bucket: {bucket}, Monitoring dir: {dir} for log file creation/deletion and chronicle metadata updates")
                monitor_th = threading.Thread(target=self.monitor_kvstores, args=[bucket.name, dir, 1, validate])
                monitor_threads.append(monitor_th)
                monitor_th.start()

        return monitor_threads

    def monitor_extent_migration(self, server, bucket, duration=18000, interval=2):

        ssh = RemoteMachineShellConnection(server)

        cbstats_cmd = f"/opt/couchbase/bin/cbstats localhost:11210 all -b {bucket.name} -u Administrator -p password | grep fusion | grep -E 'migrated|migration'"

        self.log.info(f"Monitoring extent migration on server: {server.ip} for bucket: {bucket.name}\n{cbstats_cmd}")

        start_time = time.time()
        end_time = start_time + duration

        stat_dict = dict()
        zero_retry_count = 0

        while time.time() < end_time:
            try:
                o, e = ssh.execute_command(cbstats_cmd)
                for stat in o:
                    k = stat.split(":")[0].strip()
                    v = int(stat.split(":")[1].strip())
                    stat_dict[k] = v
                self.log.debug(f"Extent Migration stats on {server.ip} :{stat_dict}")

                if int(stat_dict['ep_fusion_migration_total_bytes']) == 0:
                    zero_retry_count += 1
                    if zero_retry_count == 10:
                        self.log.info(f"No extent migration is taking place on {server.ip}:{bucket.name}")
                        break
                    continue

                extent_migration_percent = round((stat_dict['ep_fusion_migration_completed_bytes'] / stat_dict['ep_fusion_migration_total_bytes']) * 100, 2)
                self.log.info(f"Extent Migration Progress: {extent_migration_percent}% "
                    f"({stat_dict['ep_fusion_migration_completed_bytes']} / {stat_dict['ep_fusion_migration_total_bytes']})")
                if stat_dict['ep_fusion_migration_completed_bytes'] == stat_dict['ep_fusion_migration_total_bytes']:
                    self.log.info(f"Extent migration complete on node: {server.ip}")
                    break

            except Exception as e:
                self.log.info(f"Waiting for cbstats on {server.ip}")
            time.sleep(interval)

        ssh.disconnect()

    def monitor_active_guest_volumes(self, nodes_to_monitor, duration=600, interval=2):

        fusion_rest = FusionRestAPI(self.cluster.master)

        start_time = time.time()
        end_time = start_time + duration

        otp_nodes = dict()
        for node in nodes_to_monitor:
            otp_node = "ns_1@" + node.ip
            otp_nodes[otp_node] = 0

        while time.time() < end_time:
            status, content = fusion_rest.get_active_guest_volumes()
            self.log.info(f"Active Guest Volumes: {content}")
            if status:
                for node_id in list(otp_nodes):
                    try:
                        self.log.info(f"Server: {node_id}, Active Guest Volumes: {content[node_id]}")
                        if len(content[node_id]) > 0:
                            otp_nodes[otp_node] += 1
                        if len(content[node_id]) == 0 and otp_nodes[otp_node] != 0:
                            otp_nodes.pop(node_id)
                            self.log.info(f"Extent Migration complete for {node_id}")
                    except Exception as e:
                        self.log.info(f"Waiting for {node_id} in active guest volumes list")

            if len(otp_nodes) == 0:
                self.log.info("Extent Migration complete for all nodes")
                break

            time.sleep(interval)


    def run_rebalance(self, output_dir, rebalance_count=1, rebalance_sleep_time=120,
                      rebalance_master=False, replica_update=False, skip_file_linking=False,
                      wait_for_rebalance_to_complete=True, force_sync_during_sleep=False, stop_before_rebalance=False,
                      min_storage_size=None, skip_add_nodes=False):

        # Populate spare nodes list
        if rebalance_count == 1:
            self.spare_nodes = self.cluster.servers[len(self.cluster.nodes_in_cluster):]
            self.log.info(f"Spare nodes = {self.spare_nodes}")

        current_nodes_str = ""
        for node in self.cluster.nodes_in_cluster:
            current_nodes_str += node.ip + ","
        current_nodes_str = current_nodes_str[:-1]
        self.log.info(f"Current nodes str = {current_nodes_str}")

        new_master = self.cluster.master

        if self.num_nodes_to_rebalance_in > 0:
            new_node_str = current_nodes_str

            new_node_counter = 0
            for node in self.spare_nodes:
                new_node_str += "," + node.ip
                new_node_counter += 1
                if new_node_counter == self.num_nodes_to_rebalance_in:
                    break

            self.spare_nodes = self.spare_nodes[self.num_nodes_to_rebalance_in:]

            self.log.info(f"Spare nodes: {self.spare_nodes}")

        elif self.num_nodes_to_rebalance_out > 0:

            if not rebalance_master:
                nodes_to_rebalance_out = list()
                for node in self.cluster.nodes_in_cluster:
                    if node.ip != self.cluster.master.ip:
                        nodes_to_rebalance_out.append(node.ip)
                        self.spare_nodes.append(node)
                        if len(nodes_to_rebalance_out) == self.num_nodes_to_rebalance_out:
                            break

            else:
                self.log.info(f"Cluster master = {self.cluster.master}")
                nodes_to_rebalance_out = [self.cluster.master.ip]
                self.spare_nodes.append(self.cluster.master)
                tmp_server_list = [server for server in self.cluster.nodes_in_cluster
                                   if server.ip != self.cluster.master.ip]
                self.log.info(f"Temp Server list = {tmp_server_list}")
                for node in tmp_server_list:
                    if len(nodes_to_rebalance_out) == self.num_nodes_to_rebalance_out:
                        break
                    nodes_to_rebalance_out.append(node.ip)
                    self.spare_nodes.append(node)

                # Update cluster.master
                for server in tmp_server_list:
                    if server.ip not in nodes_to_rebalance_out:
                        self.cluster.master = server
                        break
                self.log.info(f"New master = {self.cluster.master}")

            self.log.info(f"Nodes to rebalance out = {nodes_to_rebalance_out}")

            new_node_str = ""
            for server in self.cluster.nodes_in_cluster:
                if server.ip not in nodes_to_rebalance_out:
                    new_node_str += server.ip + ","
            new_node_str = new_node_str[:-1]

            self.log.info(f"Spare nodes = {self.spare_nodes}")

        elif self.num_nodes_to_swap_rebalance > 0:

            self.log.info(f"Spare nodes = {self.spare_nodes}")
            nodes_to_add = self.spare_nodes[:self.num_nodes_to_swap_rebalance]
            self.log.info(f"Nodes to add: {nodes_to_add}")
            self.spare_nodes = self.spare_nodes[self.num_nodes_to_swap_rebalance:]
            self.log.info(f"Spare nodes = {self.spare_nodes}")

            if self.num_nodes_to_swap_rebalance == len(self.cluster.nodes_in_cluster):

                new_master = nodes_to_add[0]
                new_node_str = ""
                for new_node in nodes_to_add:
                    new_node_str += new_node.ip + ","
                new_node_str = new_node_str[:-1]

            else:

                if not rebalance_master:
                    nodes_to_swap = list()
                    for node in self.cluster.nodes_in_cluster:
                        if node.ip != self.cluster.master.ip:
                            nodes_to_swap.append(node.ip)
                            self.spare_nodes.append(node)
                            if len(nodes_to_swap) == self.num_nodes_to_swap_rebalance:
                                break
                    self.log.info(f"Nodes to swap = {nodes_to_swap}")

                else:
                    self.log.info(f"Cluster master = {self.cluster.master}")
                    nodes_to_swap = [self.cluster.master.ip]
                    self.spare_nodes.append(self.cluster.master)
                    tmp_server_list = [server for server in self.cluster.nodes_in_cluster
                                    if server.ip != self.cluster.master.ip]
                    for node in tmp_server_list:
                        if len(nodes_to_swap) == self.num_nodes_to_swap_rebalance:
                            break
                        nodes_to_swap.append(node.ip)
                        self.spare_nodes.append(node)
                    self.log.info(f"Nodes to swap = {nodes_to_swap}")

                    # Update cluster.master
                    for server in tmp_server_list:
                        if server.ip not in nodes_to_swap:
                            self.cluster.master = server
                            break
                    self.log.info(f"New master = {self.cluster.master}")

                new_node_str = ""
                for server in self.cluster.nodes_in_cluster:
                    if server.ip not in nodes_to_swap:
                        new_node_str += server.ip + ","
                new_node_str = new_node_str[:-1]
                for new_node in nodes_to_add:
                    new_node_str += "," + new_node.ip

                self.log.info(f"Spare nodes = {self.spare_nodes}")

        else:
            new_node_str = current_nodes_str

        self.log.info(f"New nodes str = {new_node_str}")

        fusion_rebalance_setup = True
        fusion_rebalance_result = True

        # Create a virtual environment and install necessary packages
        commands = f"""
        rm -rf {self.venv_path}
        python3 -m venv {self.venv_path}
        source {self.venv_path}/bin/activate
        pip install --upgrade pip
        pip install paramiko requests
        python3 {self.fusion_scripts_dir}/run_fusion_rebalance.py --current-nodes {current_nodes_str} --new-nodes {new_node_str} --env local --config {self.fusion_scripts_dir}/config.json --sleep-time {rebalance_sleep_time} --reb-count {rebalance_count}
        """

        if replica_update:
            commands = commands.rstrip() + " --replica-update"
        if skip_file_linking:
            commands = commands.rstrip() + " --skip-file-linking"
        if force_sync_during_sleep:
            commands = commands.rstrip() + " --force-sync-during-sleep"
        if stop_before_rebalance:
            commands = commands.rstrip() + " --stop-before-rebalance"
        if min_storage_size is not None:
            commands = commands.rstrip() + f" --min-storage-size {min_storage_size}"
        if skip_add_nodes:
            commands = commands.rstrip() + " --skip-add-nodes"

        ssh = RemoteMachineShellConnection(self.cluster.master)
        self.log.info(f"Running fusion rebalance: {commands}")
        o, e = ssh.execute_command(commands, timeout=1800)
        self.log.info(f"Output = {o[-50:]}, Error = {e}")

        plan_uuid = None
        involved_nodes_list = None

        if stop_before_rebalance:
            for line in o:
                if line.startswith("PLAN_UUID="):
                    plan_uuid = line.split("=", 1)[1].strip()
                if line.startswith("INVOLVED_NODES="):
                    involved_nodes_str = line.split("=", 1)[1].strip()
                    involved_nodes_list = involved_nodes_str.split(",")

            self.log.info(f"Extracted plan_uuid: {plan_uuid}")
            self.log.info(f"Extracted involved_nodes: {involved_nodes_list}")

        self.fusion_rebalance_output = os.path.join(output_dir, "fusion_stdout" + str(rebalance_count) + ".log")
        self.fusion_rebalance_error = os.path.join(output_dir, "fusion_stderr" + str(rebalance_count) + ".log")

        with open(self.fusion_rebalance_output, "w") as fp:
            for line in o:
                fp.write(line + "\n")
            fp.close()

        with open(self.fusion_rebalance_error, "w") as fp:
            for line in e:
                fp.write(line + "\n")
            fp.close()

        ssh.disconnect()

        # Monitor rebalance progress, and wait until it's done
        if wait_for_rebalance_to_complete and not stop_before_rebalance:
            self.start_time = time.time()
            self.sleep(10, "Wait before checking rebalance progress")
            try:
                rebalance_result = RebalanceUtil(self.cluster).monitor_rebalance(progress_count=500)
                if not rebalance_result:
                    fusion_rebalance_result = False
            except Exception as ex:
                self.log.error(f"Fusion Rebalance failed: {ex}")
                fusion_rebalance_result = False

        # Parse Fusion error logs to look for any failures/issues
        # Exclude known patterns like "Temporary failure in name resolution"
        self.log.info("Parsing Fusion Error logs to look for issues/failures")
        grep_cmd = f"grep -v -E 'Temporary failure in name resolution|unable to resolve host|WARNING|^\[[0-9.]+\][[:space:]]*$|^[[:space:]]*$' {self.fusion_rebalance_error}"
        result = subprocess.run(grep_cmd, shell=True, executable="/bin/bash", capture_output=True, text=True)
        output = result.stdout.strip()
        error = result.stderr.strip()
        self.log.info(f"Output = {output}, Error = {error}")
        if output or error:
            self.log.error("Found ERROR logs during Fusion rebalance automation")
            fusion_rebalance_setup = False

        nodes_to_monitor = list()
        ssh = RemoteMachineShellConnection(self.cluster.master)
        reb_plan_path = os.path.join(self.fusion_scripts_dir, "reb_plan{}.json".format(str(rebalance_count)))
        cat_cmd = f"cat {reb_plan_path}"
        o, e = ssh.execute_command(cat_cmd)
        json_str = "\n".join(o)
        data = json.loads(json_str)
        self.reb_plan_uuids.append(data['planUUID'])
        involved_nodes = list(data['nodes'].keys())
        node_ips_to_monitor = set([node.split("@", 1)[1] for node in involved_nodes])
        self.log.info(f"Node IPs to monitor = {node_ips_to_monitor}")
        for server in self.cluster.servers:
            if server.ip in node_ips_to_monitor:
                nodes_to_monitor.append(server)
        ssh.disconnect()

        # Save rebalance plan on the slave to artifact it
        self.log.info("Copying over rebalance plan from nodes to slave")
        copy_cmd = 'sshpass -p "{0}" scp -o StrictHostKeyChecking=no root@{1}:{2} {3}/'\
                .format("couchbase", self.cluster.master.ip, reb_plan_path, output_dir)
        self.log.info(f"Copy CMD: {copy_cmd}")
        subprocess.run(copy_cmd, shell=True, executable="/bin/bash")

        if not fusion_rebalance_setup:
            self.fail("Error during Fusion Rebalance setup")
        if not fusion_rebalance_result and not stop_before_rebalance:
            self.fail("Error during Fusion Rebalance")

        # Updating nodes_in_cluster after rebalance
        self.cluster.nodes_in_cluster = list()
        new_node_set = set(new_node_str.split(","))
        for server in self.cluster.servers:
            if server.ip in new_node_set:
                self.cluster.nodes_in_cluster.append(server)
        self.cluster.kv_nodes = self.cluster.nodes_in_cluster
        self.cluster.master = new_master

        self.log.info(f"Nodes in cluster = {self.cluster.nodes_in_cluster}")
        self.log.info(f"KV Nodes = {self.cluster.kv_nodes}")

        if not stop_before_rebalance and wait_for_rebalance_to_complete:
            return nodes_to_monitor
        else:
            return nodes_to_monitor, current_nodes_str, new_node_str

    def log_store_rebalance_cleanup(self, nodes, rebalance_count=1):

        ssh = RemoteMachineShellConnection(self.nfs_server)

        cleanup_cmd = f"rm -rf /data/nfs/{self.client_share_dir}/fusion-manifests"
        o, e = ssh.execute_command(cleanup_cmd)
        self.log.info(f"Executing CMD: {cleanup_cmd}, O = {o}, E = {e}")

        for node in nodes:
            cleanup_cmd = f"rm -rf /data/nfs/{self.client_share_dir}/guest_storage/ns_1@{node.ip}/reb{rebalance_count}"
            o, e = ssh.execute_command(cleanup_cmd)
            self.log.info(f"Executing CMD: {cleanup_cmd}, O = {o}, E = {e}")

        ssh.disconnect()

    def kill_memcached_on_nodes(self, interval):

        shell_dict = dict()
        for node in self.cluster.servers:
            shell_dict[node] = RemoteMachineShellConnection(node)

        while self.crash_loop:
            for node, shell in shell_dict.items():
                self.log.info(f"Killing memcached on {node.ip}")
                shell.kill_memcached()

            self.sleep(interval, "Sleep before killing memcached")


    def get_fusion_uploader_info(self, buckets=None):

        self.fusion_uploader_dict = dict()
        self.fusion_vb_uploader_map = dict()

        buckets = buckets if buckets is not None else self.cluster.buckets

        for bucket in buckets:
            self.fusion_uploader_dict[bucket.name] = dict()
            self.fusion_vb_uploader_map[bucket.name] = dict()
            status, content = ClusterRestAPI(self.cluster.master).diag_eval('ns_bucket:get_fusion_uploaders("{}").'.format(bucket.name))

            if status:
                raw_str = content.decode("utf-8")

                tuple_str = raw_str.replace("{", "(").replace("}", ")").replace("undefined", "None")

                parsed_list = ast.literal_eval(tuple_str)

                vb_no = 0
                for node in parsed_list:
                    if node[0] in self.fusion_uploader_dict[bucket.name]:
                        self.fusion_uploader_dict[bucket.name][node[0]] += 1
                    else:
                        self.fusion_uploader_dict[bucket.name][node[0]] = 1

                    self.fusion_vb_uploader_map[bucket.name][vb_no] = dict()
                    if node[0] != None:
                        self.fusion_vb_uploader_map[bucket.name][vb_no]["node"] = node[0].split("@")[1]
                    else:
                        self.fusion_vb_uploader_map[bucket.name][vb_no]["node"] = node[0]
                    self.fusion_vb_uploader_map[bucket.name][vb_no]["term"] = int(node[1])
                    vb_no += 1

        self.log.info(f"Fusion Uploader Distribution = {self.fusion_uploader_dict}")
        self.log.info(f"Fusion VB Uploader Map = {self.fusion_vb_uploader_map}")

    def get_memory_usage(self):
        """
        Local wrapper for get_memory_footprint() that returns the memory value.
        Uses the existing base class method but ensures we get the return value.
        """
        out = subprocess.Popen(['ps', 'v', '-p', str(os.getpid())], stdout=subprocess.PIPE).communicate()[0].split(b'\n')
        vsz_index = out[0].split().index(b'RSS')
        mem = float(out[1].split()[vsz_index]) / 1024
        self.log.debug(f"Memory footprint: {mem} MB")
        return mem

    def execute_fusion_workflow_after_magma_test(self):
        """
        Executes the complete fusion workflow after magma test completion.

        This method encapsulates the standard post-magma fusion operations:
        1. Captures pre-fusion rebalance resource usage (memory and disk)
        2. Executes fusion rebalance operation
        3. Monitors extent migration across nodes
        4. Validates post-fusion state and compares resource usage

        This function eliminates code duplication across all fusion test methods
        by centralizing the common fusion workflow steps.

        Returns:
            None

        Raises:
            Exception: If any step in the fusion workflow fails
        """
        # Step 1: Capture pre-fusion rebalance resource usage
        self.log.info("Capturing pre-fusion rebalance resource usage")
        pre_fusion_disk = {}
        pre_fusion_memory = self.get_memory_usage()

        for bucket in self.cluster.buckets:
            pre_fusion_disk[bucket.name] = self.get_disk_usage(bucket)
            self.log.info(f"Pre-fusion rebalance disk usage for {bucket.name}: {pre_fusion_disk[bucket.name]}")

        self.log.info(f"Pre-fusion rebalance memory usage: {pre_fusion_memory} MB")

        # Step 2: Execute fusion rebalance
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1)

        self.log.info("Fusion rebalance completed successfully")

        # Step 3: Monitor extent migration after rebalance
        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        for th in extent_migration_array:
            th.join()

        self.log.info("Extent migration monitoring completed successfully")

        # Step 3.5: Log Fusion storage consistency stats
        self.log.info("Collecting Fusion storage consistency stats")
        self.log_fusion_storage_stats()

        # Step 4: Validate item counts after fusion operations
        self.log.info("Starting post-fusion validation")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)

        # Step 5: Validate resource usage after fusion operations
        self.log.info("Validating post-fusion resource usage")
        post_fusion_memory = self.get_memory_usage()

        for bucket in self.cluster.buckets:
            post_fusion_disk = self.get_disk_usage(bucket)
            self.log.info(f"Post-fusion disk usage for {bucket.name}: {post_fusion_disk}")

            # Compare with pre-fusion usage
            if bucket.name in pre_fusion_disk:
                disk_diff = [post - pre for post, pre in zip(post_fusion_disk, pre_fusion_disk[bucket.name])]
                self.log.info(f"Disk usage change for {bucket.name}: {disk_diff} MB [kvstore, wal, keyTree, seqTree]")

        memory_diff = post_fusion_memory - pre_fusion_memory
        self.log.info(f"Post-fusion memory usage: {post_fusion_memory} MB")
        self.log.info(f"Memory usage change: {memory_diff} MB")

        self.log.info("Post-fusion validation completed successfully")

    def log_fusion_storage_stats(self):
        """
        Logs Fusion storage consistency stats for analysis.
        Compares NFS actual storage vs Fusion's internal tracking.
        """

        self.log.info("=" * 60)
        self.log.info("FUSION STORAGE CONSISTENCY STATS")
        self.log.info("=" * 60)

        for bucket in self.cluster.buckets:
            self.log.info(f"Analyzing bucket: {bucket.name}")

            # Get NFS actual storage size
            try:
                bucket_uuid = self.get_bucket_uuid(bucket.name)
                nfs_bucket_path = f"{self.nfs_server_path}/kv/{bucket_uuid}"

                ssh = RemoteMachineShellConnection(self.nfs_server)
                du_cmd = f"du -sb {nfs_bucket_path}"
                output, error = ssh.execute_command(du_cmd)
                ssh.disconnect()

                if output and len(output) > 0:
                    nfs_actual_size = int(output[0].split()[0])
                    self.log.info(f"  NFS Actual Size: {nfs_actual_size:,} bytes ({nfs_actual_size / (1024**3):.2f} GB)")
                else:
                    self.log.warning(f"  NFS Actual Size: Could not determine (output: {output}, error: {error})")
                    continue

            except Exception as e:
                self.log.warning(f"  NFS Actual Size: Error getting size - {e}")
                continue

            # Get Fusion's internal tracking
            fusion_total_data_size = 0
            fusion_total_summary_size = 0

            self.log.info(f"  Fusion Internal Tracking:")

            for node in self.cluster.nodes_in_cluster:
                try:
                    cbstats = Cbstats(node)
                    result = cbstats.all_stats(bucket.name)

                    data_size = int(result.get("ep_fusion_log_store_data_size", 0))
                    summary_size = int(result.get("ep_fusion_log_store_summary_size", 0))

                    fusion_total_data_size += data_size
                    fusion_total_summary_size += summary_size

                    self.log.info(f"    Node {node.ip}: data_size={data_size:,}, summary_size={summary_size:,}")

                    cbstats.disconnect()

                except Exception as e:
                    self.log.warning(f"    Node {node.ip}: Error getting stats - {e}")

            fusion_total_size = fusion_total_data_size + fusion_total_summary_size

            self.log.info(f"  Fusion Total Data Size: {fusion_total_data_size:,} bytes ({fusion_total_data_size / (1024**3):.2f} GB)")
            self.log.info(f"  Fusion Total Summary Size: {fusion_total_summary_size:,} bytes ({fusion_total_summary_size / (1024**3):.2f} GB)")
            self.log.info(f"  Fusion Combined Size: {fusion_total_size:,} bytes ({fusion_total_size / (1024**3):.2f} GB)")

            # Log the difference for future analysis
            if fusion_total_size > 0:
                size_diff = nfs_actual_size - fusion_total_size
                self.log.info(f"  Size Difference: {size_diff:,} bytes ({size_diff / (1024**3):.2f} GB)")
                self.log.info(f"  NFS vs Fusion Ratio: {nfs_actual_size / fusion_total_size:.3f}")
            else:
                self.log.info(f"  Size Difference: Cannot calculate (Fusion size is 0)")

            self.log.info("-" * 40)

        self.log.info("=" * 60)

    def induce_rebalance_test_condition(self, servers, test_failure_condition,
                                        bucket_name="default",
                                        vb_num=1,
                                        delay_time=60000):
        if test_failure_condition == "verify_replication":
            condition = 'fail, "{}"'.format(bucket_name)
            set_command = 'testconditions:set(verify_replication, {' \
                          + condition + '})'
        elif test_failure_condition == "backfill_done":
            condition = 'for_vb_move, "{0}", {1}, fail'.format(bucket_name, vb_num)
            set_command = 'testconditions:set(backfill_done, {' \
                          + condition + '})'
        elif test_failure_condition == "delay_rebalance_start":
            condition = 'delay, {}'.format(delay_time)
            set_command = 'testconditions:set(rebalance_start, {' \
                          + condition + '}).'
        elif test_failure_condition == "delay_verify_replication":
            condition = 'delay, "{0}", {1}'.format(bucket_name, delay_time)
            set_command = 'testconditions:set(verify_replication, {' \
                          + condition + '})'
        elif test_failure_condition == "delay_backfill_done":
            condition = 'for_vb_move, "{0}", {1}, '.format(bucket_name, vb_num)
            sub_cond = 'delay, {}'.format(delay_time)
            set_command = 'testconditions:set(backfill_done, {' \
                          + condition + '{' + sub_cond + '}})'
        elif test_failure_condition == 'delay_failover_start':
            condition = 'delay, {}'.format(delay_time)
            set_command = 'testconditions:set(failover_start, {' \
                          + condition + '}).'
        else:
            set_command = "testconditions:set({}, fail)" \
                          .format(test_failure_condition)

        if test_failure_condition.startswith("delay_"):
            test_failure_condition = test_failure_condition[6:]
        get_command = "testconditions:get({})".format(test_failure_condition)

        for server in servers:
            cluster_api = ClusterRestAPI(server)

            # Execute set command
            status, content = cluster_api.diag_eval(set_command)
            self.log.info("Set Command: {0}. Status: {1}, Return: {2}".format(
                set_command, status, content))

            # Execute get command to verify
            status, content = cluster_api.diag_eval(get_command)
            self.log.info("Get Command: {0}. Status: {1}, Return: {2}".format(
                get_command, status, content))

    def get_otp_node(self, rest_node, target_node):
        nodes = self.cluster_util.get_nodes(rest_node)
        for node in nodes:
            if node.ip == target_node.ip:
                return node


    def perform_workload(self, start, end, doc_op="create", wait=True, buckets=None, ops_rate=None):

        ops_rate = ops_rate if ops_rate is not None else 20000

        self.reset_doc_params(doc_ops=doc_op)
        if doc_op == "create":
            self.create_start = start
            self.create_end = end
            self.num_items = self.create_end
        elif doc_op == "update":
            self.update_start = start
            self.update_end = end
        elif doc_op == "read":
            self.read_start = start
            self.read_end = end

        doc_loading_tasks, _ = self.java_doc_loader(wait=False,
                                                    skip_default=self.skip_load_to_default_collection,
                                                    ops_rate=ops_rate, doc_ops=doc_op,
                                                    monitor_ops=False,
                                                    buckets=buckets)
        if wait:
            for task in doc_loading_tasks:
                self.doc_loading_tm.get_task_result(task)
        else:
            return doc_loading_tasks

    def validate_term_number(self, prev_uploader_map, new_uploader_map):
        """
        Validate that term numbers change appropriately after rebalance.
        Term number should increment by 1 when uploader changes.
        Term number should remain same when uploader doesn't change.
        """
        for bucket in self.cluster.buckets:
            for vb_no in range(bucket.numVBuckets):
                prev_uploader = prev_uploader_map[bucket.name][vb_no]['node']
                prev_term = prev_uploader_map[bucket.name][vb_no]['term']

                new_uploader = new_uploader_map[bucket.name][vb_no]['node']
                new_term = new_uploader_map[bucket.name][vb_no]['term']

                if prev_uploader != new_uploader:
                    if new_term == prev_term + 1:
                        self.log.info(f"Term number changed for {bucket.name}:vb_{vb_no} as expected")
                    else:
                        self.log.info(f"Expected term number: {prev_term+1}, Actual term number: {new_term}")
                else:
                    if prev_term == new_term:
                        self.log.info(f"Term number not changed for {bucket.name}:vb_{vb_no} as expected")
                    else:
                        self.log.info(f"Expected term number: {prev_term}, Actual term number: {new_term}")

    def get_fusion_status_info(self, duration=3600):

        end_time = time.time() + duration
        self.monitor_fusion_info = True

        while self.monitor_fusion_info and time.time() < end_time:

            status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
            self.log.info(f"Status = {status}, Content = {content}")

            bucket_du = dict()
            ssh = RemoteMachineShellConnection(self.nfs_server)

            for bucket in self.cluster.buckets:
                try:
                    bucket_uuid = self.get_bucket_uuid(bucket.name)
                    log_store_bucket_path = os.path.join(self.nfs_server_path, "kv", bucket_uuid)
                    du_cmd = f"du -sh {log_store_bucket_path}"
                    o, e = ssh.execute_command(du_cmd)
                    bucket_du[bucket.name] = o[0].split("\t")[0]
                except Exception as ex:
                    self.log.info(f"Exception = {ex}, O = {o}, E = {e}")

            ssh.disconnect()
            self.log.info(f"Log store Bucket DU = {bucket_du}")
            time.sleep(5)
