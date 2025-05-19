import json
import os
import subprocess
import threading
import time

from TestInput import TestInputServer
from basetestcase import BaseTestCase
from cb_server_rest_util.buckets.buckets_api import BucketRestApi
from shell_util.remote_connection import RemoteMachineShellConnection

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

        self.setup_nfs_server()

        th_arr1 = list()
        th_arr2 = list()

        for nfs_client in self.cluster.servers:
            # self.setup_nfs_client(nfs_client)
            th1 = threading.Thread(target=self.setup_nfs_client, args=[nfs_client])
            # self.copy_scripts_to_nodes(nfs_client)
            th2 = threading.Thread(target=self.copy_scripts_to_nodes, args=[nfs_client])
            th1.start()
            th2.start()
            th_arr1.append(th1)
            th_arr2.append(th2)

        for th in th_arr1:
            th.join()

        for th in th_arr2:
            th.join()

        self.monitor = True
        self.crash_loop = False


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
            kvstore_path = f"{self.nfs_server_path}/kv/{bucket}/{bucket_uuid}/kvstore-{kvstore}"
            kvstore_dirs.append(kvstore_path)

        return kvstore_dirs

    def get_timestamp_dirs(self, ssh, dir):

        cmd = f"date +%s; ls -ltr {dir} | awk '{{print $9}}'"
        output, _ = ssh.execute_command(cmd)
        timestamp = int(output[0].strip())
        dirs = [dir.strip() for dir in output[2:]]

        return timestamp, dirs

    def monitor_kvstores(self, bucket, dir, interval=1):

        ssh = RemoteMachineShellConnection(self.nfs_server)

        ssh2 = RemoteMachineShellConnection(self.cluster.master)

        kvstore_num = int(dir.split("-")[1])
        self.kvstore_stats[bucket][kvstore_num]["term"] = -1
        self.kvstore_stats[bucket][kvstore_num]["checkpointLogID"] = list()
        self.kvstore_stats[bucket][kvstore_num]["leases"] = list()
        self.kvstore_stats[bucket][kvstore_num]["file_creation"] = list()
        self.kvstore_stats[bucket][kvstore_num]["file_deletion"] = list()

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
            curr_timestamp, curr_log_files = self.get_timestamp_dirs(ssh, dir)

            o, e = ssh2.execute_command(chronicle_cmd)
            json_str = "\n".join(o)
            stat = json.loads(json_str)

            # Do a set difference to see if any log files have been added/deleted
            curr_log_files_set = set(curr_log_files)
            prev_log_files_set = set(prev_log_files)

            # This tells us what files have been added
            diff1 = curr_log_files_set - prev_log_files_set
            # This tells us if any log files have been deleted
            diff2 = prev_log_files_set - curr_log_files_set

            if len(diff1) != 0:
                for file in list(diff1):
                    if "tmp" in file:
                        continue
                    full_file_path = os.path.join(dir, file)
                    cmd2 = f"stat --format='%Y' {full_file_path}"
                    timestamp_output, _  = ssh.execute_command(cmd2)
                    file_creation_timestamp = int(timestamp_output[0].strip())
                    self.kvstore_stats[bucket][kvstore_num]["file_creation"].append([file, file_creation_timestamp])
            if len(diff2) != 0:
                for file in list(diff2):
                    if "tmp" in file:
                        continue
                    self.kvstore_stats[bucket][kvstore_num]["file_deletion"].append([file, curr_timestamp])

            if stat["term"] != self.kvstore_stats[bucket][kvstore_num]["term"]:
                self.kvstore_stats[bucket][kvstore_num]["term"] = stat["term"]
            if len(self.kvstore_stats[bucket][kvstore_num]["checkpointLogID"]) == 0 or \
                (len(self.kvstore_stats[bucket][kvstore_num]["checkpointLogID"]) > 0 and \
                stat["checkpointLogID"] != self.kvstore_stats[bucket][kvstore_num]["checkpointLogID"][-1][0]):
                self.kvstore_stats[bucket][kvstore_num]["checkpointLogID"].append([stat["checkpointLogID"], curr_timestamp])
            if stat["leases"] != self.kvstore_stats[bucket][kvstore_num]["leases"]:
                self.kvstore_stats[bucket][kvstore_num]["leases"] = stat["leases"]

            # prev_timestamp = curr_timestamp
            prev_log_files = curr_log_files
            time.sleep(interval)

        ssh.disconnect()
        ssh2.disconnect()

    def start_monitor_dir(self):

        monitor_threads = list()

        for bucket in self.cluster.buckets:
            kvstore_dirs = self.get_kvstore_directories(bucket.name)

            for dir in kvstore_dirs:
                self.log.debug(f"Bucket: {bucket}, Monitoring dir: {dir} for log file creation/deletion and chronicle metadata updates")
                monitor_th = threading.Thread(target=self.monitor_kvstores, args=[bucket.name, dir])
                monitor_threads.append(monitor_th)
                monitor_th.start()

        return monitor_threads

    def validate_log_file_count_size(self, dir_path):

        ssh = RemoteMachineShellConnection(self.cluster.master)
        reb_plan_path = os.path.join(dir_path, "reb_plan.json")
        cat_cmd = f"cat {reb_plan_path}"
        o, e = ssh.execute_command(cat_cmd)
        json_str = "\n".join(o)
        data = json.loads(json_str)

        self.total_size = dict()
        self.total_log_files = dict()
        for node in data['nodes']:
            self.total_size[node[5:]] = 0
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

    def monitor_extent_migration(self, server, bucket, duration=600, interval=2):

        ssh = RemoteMachineShellConnection(server)

        cbstats_cmd = f"/opt/couchbase/bin/cbstats localhost:11210 all -b {bucket.name} -u Administrator -p password | grep fusion | grep migrated"

        self.log.info(f"Monitoring extent migration on server: {server.ip} for bucket: {bucket.name}\n{cbstats_cmd}")

        start_time = time.time()
        end_time = start_time + duration

        stat_dict = dict()
        total_log_file_count = self.total_log_files[server.ip]

        while time.time() < end_time:
            o, e = ssh.execute_command(cbstats_cmd)
            for stat in o:
                k = stat.split(":")[0].strip()
                v = int(stat.split(":")[1].strip())
                stat_dict[k] = v
            self.log.debug(f"Extent Migration stats on {server.ip} :{stat_dict}")
            extent_migration_percent = round((stat_dict['ep_fusion_logs_migrated'] / self.total_log_files[server.ip]) * 100, 2)
            self.log.info(f"Extent Migration Progress: {extent_migration_percent}% "
                          f"({stat_dict['ep_fusion_logs_migrated']} / {self.total_log_files[server.ip]})")
            if stat_dict['ep_fusion_logs_migrated'] == total_log_file_count:
                self.log.info(f"Extent migration complete on node: {server.ip}")
                break
            time.sleep(interval)

        ssh.disconnect()

    def run_rebalance(self, output_dir, rebalance_count):

        ssh = RemoteMachineShellConnection(self.cluster.master)

        current_nodes_str = ""
        for node in self.cluster.nodes_in_cluster:
            current_nodes_str += node.ip + ","
        current_nodes_str = current_nodes_str[:-1]
        self.log.info(f"Current nodes str = {current_nodes_str}")

        if self.num_nodes_to_rebalance_in > 0:
            new_node_str = current_nodes_str
            for node in self.cluster.servers[len(self.cluster.nodes_in_cluster):len(self.cluster.nodes_in_cluster)+self.num_nodes_to_rebalance_in]:
                new_node_str += "," + node.ip
            self.log.info(f"New nodes str = {new_node_str}")
        elif self.num_nodes_to_rebalance_out > 0:
            new_node_str = ""
            for node in self.cluster.servers[:len(self.cluster.nodes_in_cluster)-abs(self.num_nodes_to_rebalance_out)]:
                new_node_str += node.ip + ","
            new_node_str = new_node_str[:-1]
            self.log.info(f"New nodes str = {new_node_str}")

        # Monitoring Extent Migration post completion of rebalance
        if self.num_nodes_to_rebalance_in > 0:
            # Extent Migration will take place on nodes that are newly added
            nodes_to_monitor = self.cluster.servers[len(self.cluster.nodes_in_cluster):len(self.cluster.nodes_in_cluster)+self.num_nodes_to_rebalance_in]
        elif self.num_nodes_to_rebalance_out > 0:
            # Extent Migration would take place on a subset of nodes that are still part of the cluster
            nodes_to_monitor = self.cluster.servers[:len(self.cluster.nodes_in_cluster)-abs(self.num_nodes_to_rebalance_out)]
        elif self.num_nodes_to_swap_rebalance > 0:
            # Extent Migration would take place on the nodes being swapped
            nodes_to_monitor = self.cluster.servers[len(self.cluster.nodes_in_cluster):len(self.cluster.nodes_in_cluster)+self.num_nodes_to_swap_rebalance]

        # Create a virtual environment and install necessary packages
        commands = f"""
        rm -rf {self.venv_path}
        python3 -m venv {self.venv_path}
        source {self.venv_path}/bin/activate
        pip install --upgrade pip
        pip install paramiko requests
        python3 {self.fusion_scripts_dir}/run_fusion_rebalance.py --current-nodes {current_nodes_str} --new-nodes {new_node_str} --env local --config {self.fusion_scripts_dir}/config.json
        """

        self.fusion_rebalance_output = os.path.join(output_dir, "fusion_stdout" + str(rebalance_count) + ".txt")
        self.fusion_rebalance_error = os.path.join(output_dir, "fusion_stderr" + str(rebalance_count) + ".txt")

        self.log.info(f"Running fusion rebalance: {commands}")
        o, e = ssh.execute_command(commands)
        ssh.log_command_output(o, e)

        with open(self.fusion_rebalance_output, "w") as fp:
            for line in o:
                fp.write(line + "\n")
            fp.close()

        with open(self.fusion_rebalance_error, "w") as fp:
            for line in e:
                fp.write(line + "\n")
            fp.close()

        ssh.disconnect()

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

