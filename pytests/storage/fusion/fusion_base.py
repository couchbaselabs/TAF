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
from rebalance_utils.rebalance_util import RebalanceUtil
from shell_util.remote_connection import RemoteMachineShellConnection
from custom_exceptions.exception import RebalanceFailedException

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

        self.current_version = self.input.param("current_version", "0.0.0-3025")

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

        if not self.skip_cluster_reset:
            self.setup_nfs_server()

            th_arr1 = list()
            th_arr2 = list()

            for nfs_client in self.cluster.servers:
                th1 = threading.Thread(target=self.setup_nfs_client, args=[nfs_client])
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

        self.monitor = True
        self.crash_loop = False

        self.retry_get_process_num = self.input.param("retry_get_process_num", 300)

        self.spare_nodes = list()


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

    def get_log_file_count_size(self, dir_path, migration_count=1, rebalance_count=1):

        ssh = RemoteMachineShellConnection(self.cluster.master)
        reb_plan_path = os.path.join(dir_path, "reb_plan{}.json".format(str(rebalance_count)))
        cat_cmd = f"cat {reb_plan_path}"
        o, e = ssh.execute_command(cat_cmd)
        json_str = "\n".join(o)
        data = json.loads(json_str)

        if migration_count == 1:
            self.total_size = dict()
            self.total_log_files = dict()

        for node in data['nodes']:
            if node[5:] not in self.total_size:
                self.total_size[node[5:]] = 0
            if node[5:] not in self.total_log_files:
                self.total_log_files[node[5:]] = 0
            for log_manifest in data['nodes'][node]:
                for log_file in log_manifest['logFiles']:
                    self.total_size[node[5:]] += log_file['size']
                    self.total_log_files[node[5:]] += 1
        self.log.info(f"Total size of all log files = {self.total_size}")
        self.log.info(f"Total log files = {self.total_log_files}")

        guest_storage_dir = os.path.join("/".join(self.nfs_server_path.split("/")[:-1]), "guest_storage")
        self.log.info(f"Guest Storage dir = {guest_storage_dir}")
        nfs_ssh = RemoteMachineShellConnection(self.nfs_server)

        output, e = nfs_ssh.execute_command(f"ls {guest_storage_dir}")
        self.guest_storage_log_files = dict()
        for node in output:
            node_path = os.path.join(guest_storage_dir, node.strip())
            o, e = nfs_ssh.execute_command(f"find {node_path} -name 'log-*' | wc -l")
            self.log.info(f"Node: {node}, Guest Storage log files: {o}")
            self.guest_storage_log_files[node[5:]] = int(o[0].strip())
        self.log.info(f"Guest Storage Log Files: {self.guest_storage_log_files}")

        ssh.disconnect()
        nfs_ssh.disconnect()

    def monitor_extent_migration(self, server, bucket, duration=3600, interval=2):

        ssh = RemoteMachineShellConnection(server)

        cbstats_cmd = f"/opt/couchbase/bin/cbstats localhost:11210 all -b {bucket.name} -u Administrator -p password | grep fusion | grep -E 'migrated|migration'"

        self.log.info(f"Monitoring extent migration on server: {server.ip} for bucket: {bucket.name}\n{cbstats_cmd}")

        start_time = time.time()
        end_time = start_time + duration

        stat_dict = dict()

        while time.time() < end_time:
            try:
                o, e = ssh.execute_command(cbstats_cmd)
                for stat in o:
                    k = stat.split(":")[0].strip()
                    v = int(stat.split(":")[1].strip())
                    stat_dict[k] = v
                self.log.debug(f"Extent Migration stats on {server.ip} :{stat_dict}")

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
                      rebalance_master=False, replica_update=False, skip_file_linking=False):

        # Populate spare nodes list
        if rebalance_count == 1:
            self.spare_nodes = self.cluster.servers[len(self.cluster.nodes_in_cluster):]
            self.log.info(f"Spare nodes = {self.spare_nodes}")

        # Fetch last rebalance task to track starting of current rebalance
        self.prev_rebalance_status_id = None
        server_task = self.cluster_util.get_cluster_tasks(
            self.cluster.master, task_type="rebalance",
            task_sub_type="rebalance")
        if server_task and "statusId" in server_task:
            self.prev_rebalance_status_id = server_task["statusId"]
        self.log.debug("Last known rebalance status_id: %s"
                       % self.prev_rebalance_status_id)

        current_nodes_str = ""
        for node in self.cluster.nodes_in_cluster:
            current_nodes_str += node.ip + ","
        current_nodes_str = current_nodes_str[:-1]
        self.log.info(f"Current nodes str = {current_nodes_str}")

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

        ssh = RemoteMachineShellConnection(self.cluster.master)
        self.log.info(f"Running fusion rebalance: {commands}")
        o, e = ssh.execute_command(commands, timeout=1800)
        self.log.info(f"Output = {o[-50:]}, Error = {e}")

        # Monitor rebalance progress, and wait until it's done
        self.start_time = time.time()
        self.sleep(10, "Wait before checking rebalance progress")
        rebalance_result = RebalanceUtil(self.cluster).monitor_rebalance()
        if not rebalance_result:
            self.log.fail("Fusion Rebalance failed")

        self.fusion_rebalance_output = os.path.join(output_dir, "fusion_stdout" + str(rebalance_count) + ".txt")
        self.fusion_rebalance_error = os.path.join(output_dir, "fusion_stderr" + str(rebalance_count) + ".txt")

        with open(self.fusion_rebalance_output, "w") as fp:
            for line in o:
                fp.write(line + "\n")
            fp.close()

        with open(self.fusion_rebalance_error, "w") as fp:
            for line in e:
                fp.write(line + "\n")
            fp.close()

        ssh.disconnect()

        # Parse Fusion error logs to look for any failures/issues
        # Exclude known patterns like "Temporary failure in name resolution"
        self.log.info("Parsing Fusion Error logs to look for issues/failures")
        grep_cmd = f"grep -v -e 'Temporary failure in name resolution' -e 'WARNING' {self.fusion_rebalance_error}"
        result = subprocess.run(grep_cmd, shell=True, executable="/bin/bash", capture_output=True, text=True)
        output = result.stdout.strip()
        error = result.stderr.strip()
        self.log.info(f"Output = {output}, Error = {error}")

        if output or error:
            self.fail("Found ERROR logs during Fusion rebalance automation")


        nodes_to_monitor = list()
        ssh = RemoteMachineShellConnection(self.cluster.master)
        reb_plan_path = os.path.join(self.fusion_scripts_dir, "reb_plan{}.json".format(str(rebalance_count)))
        cat_cmd = f"cat {reb_plan_path}"
        o, e = ssh.execute_command(cat_cmd)
        json_str = "\n".join(o)
        data = json.loads(json_str)
        involved_nodes = list(data['nodes'].keys())
        node_ips_to_monitor = set([node.split("@", 1)[1] for node in involved_nodes])
        self.log.info(f"Node IPs to monitor = {node_ips_to_monitor}")
        for server in self.cluster.servers:
            if server.ip in node_ips_to_monitor:
                nodes_to_monitor.append(server)
        ssh.disconnect()

        # Updating nodes_in_cluster after rebalance
        self.cluster.nodes_in_cluster = list()
        new_node_set = set(new_node_str.split(","))
        for server in self.cluster.servers:
            if server.ip in new_node_set:
                self.cluster.nodes_in_cluster.append(server)
        self.cluster.kv_nodes = self.cluster.nodes_in_cluster

        self.log.info(f"Nodes in cluster = {self.cluster.nodes_in_cluster}")
        self.log.info(f"KV Nodes = {self.cluster.kv_nodes}")

        return nodes_to_monitor

    def log_store_rebalance_cleanup(self):

        ssh = RemoteMachineShellConnection(self.nfs_server)

        cleanup_cmd = f"rm -rf /data/nfs/share/fusion-manifests; rm -rf /data/nfs/share/guest_storage"
        o, e = ssh.execute_command(cleanup_cmd)

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
