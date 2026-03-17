from copy import deepcopy
import json
import os
import random
import subprocess
import threading
import time
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_server_rest_util.fusion.fusion_api import FusionRestAPI
from cb_server_rest_util.server_groups.server_groups_api import ServerGroupsAPI
from cb_tools.cbstats import Cbstats
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_base import MagmaBaseTest
from shell_util.remote_connection import RemoteMachineShellConnection


class FusionS3(MagmaBaseTest, FusionBase):

    def setUp(self):
        super(FusionS3, self).setUp()

        self.log.info("FusionS3 setUp")

        self.s3_rate_limit = self.input.param("s3_rate_limit", None)
        self.s3_latency = self.input.param("s3_latency", None)
        self.s3_packet_loss = self.input.param("s3_packet_loss", None)

        self.skip_file_linking = self.input.param("skip_file_linking", True)
        self.corruption_type = self.input.param("corruption_type", "append")
        self.include_log_manifest = self.input.param("include_log_manifest", False)

        self.num_kvstores = self.input.param("num_kvstores", None)
        self.num_log_files = self.input.param("num_log_files", 2)


    def tearDown(self):

        super(FusionS3, self).tearDown()


    def test_fusion_rebalance_s3(self):

        self.log.info("Running Fusion Rebalance Test with S3")

        self.interrupt_s3_traffic = self.input.param("interrupt_s3_traffic", False)

        self.initial_load()
        sleep_time = 600
        self.sleep(sleep_time, "Sleep after data loading")
        self.bucket_util.print_bucket_stats(self.cluster)

        # Set Sync Rate Limit to 0 so that syncs don't take place
        status, content = ClusterRestAPI(self.cluster.master).\
                            manage_global_memcached_setting(fusion_sync_rate_limit=0)
        self.log.info(f"Setting Sync Rate limit to 0, Status = {status}, Content = {content}")

        # Sync data to S3
        self.stop_start_all_servers(action="stop")
        self.copy_data_to_s3()
        self.stop_start_all_servers(action="start")

        self.sleep(60, "Wait before starting a Fusion rebalance")

        if self.interrupt_s3_traffic:
            s3_traffic_thread = threading.Thread(target=self.restore_block_s3_traffic, args=[15, 86400, 100])
            s3_traffic_thread.start()

        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1,
                                              log_store="s3")

        if self.interrupt_s3_traffic:
            self.toggle_s3_traffic = False
            s3_traffic_thread.join()

        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        for th in extent_migration_array:
            th.join()

        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        self.log.info("Validating item count after rebalance")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster,
                                                  self.num_items)


    def test_fusion_corruption_with_s3(self):

        self.log.info("Running Fusion S3 Corruption Test")

        self.initial_load()
        sleep_time = 600
        self.sleep(sleep_time, "Sleep after data loading")
        self.bucket_util.print_bucket_stats(self.cluster)

        # Set Sync Rate Limit to 0 so that syncs don't take place
        status, content = ClusterRestAPI(self.cluster.master).\
                            manage_global_memcached_setting(fusion_sync_rate_limit=0)
        self.log.info(f"Setting Sync Rate limit to 0, Status = {status}, Content = {content}")

        # Corrupt log files
        self.corrupt_log_files_on_log_store(corruption_type=self.corruption_type, num_kvstores=self.num_kvstores,
                                            num_log_files=self.num_log_files, include_manifest=self.include_log_manifest)

        # Sync data to S3
        self.stop_start_all_servers(action="stop")
        self.copy_data_to_s3()
        self.stop_start_all_servers(action="start")

        self.sleep(60, "Wait before starting a Fusion rebalance")

        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1,
                                              log_store="s3")

        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        for th in extent_migration_array:
            th.join()

        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        self.log.info("Validating item count after rebalance")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster,
                                                  self.num_items)


    def test_fusion_with_s3_end_to_end(self):

        '''
        log_store_uri = s3://cb-fusion-test/buckets?region=us-west-2
        '''

        self.log.info("Running Fusion S3 End-to-End Test")

        self.throttle_s3_download = self.input.param("throttle_s3_download", False)
        self.interrupt_s3_upload = self.input.param("interrupt_s3_upload", False)
        self.crash_during_upload = self.input.param("crash_during_upload", False)
        self.simulate_disk_full = self.input.param("simulate_disk_full", False)
        self.guest_storage_dest_path = self.input.param("guest_storage_dest_path", "/data/guest_storage")

        monitor_th = threading.Thread(target=self.get_fusion_sync_stats_continuously)
        monitor_th.start()

        if self.interrupt_s3_upload:
            s3_traffic_thread = threading.Thread(target=self.restore_block_s3_traffic)
            s3_traffic_thread.start()

        if self.crash_during_upload:
            # Start a crash thread which kills memcached in random intervals
            crash_th = threading.Thread(target=self.crash_during_sync)
            crash_th.start()

        self.initial_load()

        if self.interrupt_s3_upload:
            self.toggle_s3_traffic = False
            s3_traffic_thread.join()

        if self.crash_during_upload:
            self.stop_crash = True
            crash_th.join()

        sleep_time = 600
        self.sleep(sleep_time, "Sleep after data loading")
        self.bucket_util.print_bucket_stats(self.cluster)

        self.sleep(60, "Wait before starting a Fusion rebalance")

        if self.throttle_s3_download:
            # Apply S3 throttling
            throttling_thread = threading.Thread(target=self.throttle_restore_network_traffic, args=["s3",
                                                                    self.s3_rate_limit,
                                                                    self.s3_latency,
                                                                    self.s3_packet_loss])
            throttling_thread.start()

        if self.simulate_disk_full:
            disk_full_th = threading.Thread(target=self.simulate_disk_full_scenario)
            disk_full_th.start()
            self.sleep(20, "Wait after starting disk_full thread")

        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1,
                                              log_store="s3",
                                              rebalance_sleep_time=60,
                                              manifest_parts=1,
                                              guest_storage_dest_path=self.guest_storage_dest_path)

        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        for th in extent_migration_array:
            th.join()

        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        self.log.info("Validating item count after rebalance")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster,
                                                  self.num_items)

        self.monitor_sync_stats = False
        monitor_th.join()


    def copy_data_to_s3(self, delete=False):

        print("Copying data over to S3")

        ssh = RemoteMachineShellConnection(self.cluster.master)

        cmd = f"aws s3 sync /mnt/nfs/share/buckets s3://cb-fusion-test/buckets &> /root/sync_s3_logs.txt"
        if delete:
            cmd += " --delete"

        self.log.info(f"Running CMD: {cmd}")
        o, e = ssh.execute_command(cmd, timeout=86400)

        ssh.disconnect()


    def stop_start_all_servers(self, action="stop"):

        for server in self.cluster.nodes_in_cluster:

            ssh = RemoteMachineShellConnection(server)
            cmd = f"systemctl {action} couchbase-server"
            self.log.info(f"Server: {server.ip}, Running CMD: {cmd}")
            o, e = ssh.execute_command(cmd)
            self.log.info(f"O = {o}, E = {e}")

            ssh.disconnect()

        self.sleep(30, "Wait after stopping/starting servers")


    def fetch_log_files_from_log_dump(self):

        self.kvstore_log_dict = dict()
        ssh = RemoteMachineShellConnection(self.cluster.master)

        local_nfs_path = self.fusion_log_store_uri.split("//")[-1]

        for bucket in self.cluster.buckets:

            self.kvstore_log_dict[bucket.uuid] = dict()

            kvstore_dirs = self.get_kvstore_directories(bucket)

            for dir in kvstore_dirs:

                kvstore_num = int(dir.split("-")[1])
                self.kvstore_log_dict[bucket.uuid][kvstore_num] = list()

                i = 0
                tmp_arr = dir.split("/")
                for i in range(len(tmp_arr)):
                    if tmp_arr[i] == "buckets":
                        break

                volume_name = "/".join(tmp_arr[i+1:])
                chronicle_cmd = '/opt/couchbase/bin/fusion/metadata_dump --uri "chronicle://localhost:8091" --volume-id "' + volume_name + '"'
                self.log.info(f"Metadata_dump CMD = {chronicle_cmd}")
                o, e = ssh.execute_command(chronicle_cmd)
                json_str = "\n".join(o)
                stat = json.loads(json_str)
                checkpoint_logid = stat["checkpoint"]["logID"]
                volumeID = stat["volumeID"]

                log_dump_cmd = f"""/opt/couchbase/bin/fusion/log_dump -l /mnt/nfs/share/buckets/{volumeID}/{checkpoint_logid} | grep 'LogID' | awk '{{print "log-" substr($2,6) "." substr($3,7)}}'"""
                o, e = ssh.execute_command(log_dump_cmd)
                for log in o:
                    full_log_file_path = os.path.join(local_nfs_path, volumeID, log.strip())
                    self.kvstore_log_dict[bucket.uuid][kvstore_num].append(full_log_file_path)

        self.log.info(f"Kvstore log file dict = {self.kvstore_log_dict}")


    def corrupt_log_files_on_log_store(self, corruption_type, num_kvstores, num_log_files, include_manifest=False):

        self.fetch_log_files_from_log_dump()

        for bucket in self.cluster.buckets:
            random_kvstores = random.sample(list(self.kvstore_log_dict[bucket.uuid].keys()), num_kvstores)
            self.log.info(f"Random kvstores = {random_kvstores}")

            for kvstore in random_kvstores:
                if include_manifest:
                    random_log_file_list = [self.kvstore_log_dict[bucket.uuid][kvstore[0]]]
                    random_log_file_list += random.sample(self.kvstore_log_dict[bucket.uuid][kvstore][1:], min(num_log_files-1, len(self.kvstore_log_dict[bucket.uuid][kvstore])))
                else:
                    random_log_file_list = random.sample(self.kvstore_log_dict[bucket.uuid][kvstore][1:], min(num_log_files, len(self.kvstore_log_dict[bucket.uuid][kvstore])))

                self.log.info(f"List of log files to be corrupted/deleted in Bucket: {bucket.name}, {kvstore} = {random_log_file_list}")

                self.corrupt_delete_log_files(random_log_file_list, corruption_type)


    def corrupt_delete_log_files(self, log_files, corruption_type):

        shell = RemoteMachineShellConnection(self.cluster.master)

        for log_file in log_files:

            if corruption_type == "append":
                corrupt_cmd = f"dd if=/dev/urandom of='{log_file}' bs=1 count=16 seek=$(stat -c%s '{log_file}') conv=notrunc"

            elif corruption_type == "overwrite":
                corrupt_cmd = (
                    f"size=$(stat -c%s '{log_file}'); "
                    f"mid=$((size / 2)); "
                    f"dd if=/dev/urandom of='{log_file}' bs=1 count=16 seek=$mid conv=notrunc"
                )

            elif corruption_type == "truncate":
                corrupt_cmd = f"truncate -s -16 '{log_file}'"

            elif corruption_type == "delete":
                corrupt_cmd = f"rm {log_file}"

            elif corruption_type == "truncate_full":
                corrupt_cmd = f"truncate -s 0 {log_file}"

            o, e = shell.execute_command(corrupt_cmd)
            self.log.info(f"CMD: {corrupt_cmd}, Output = {o}, Error = {e}")

        shell.disconnect()


    def restore_block_s3_traffic(self, interval=15, timeout=86400, wait_before_start=1):

        self.sleep(wait_before_start, "Wait before interrupting S3 traffic")

        block_cmd = "sudo iptables -I OUTPUT -p tcp --dport 443 -j DROP"
        restore_cmd = "sudo iptables -D OUTPUT -p tcp --dport 443 -j DROP"

        self.toggle_s3_traffic = True
        end_time = time.time() + timeout

        counter = 1

        ssh_dict = dict()
        for server in self.cluster.servers:
            shell = RemoteMachineShellConnection(server)
            ssh_dict[server.ip] = shell

        while self.toggle_s3_traffic and time.time() < end_time:

            if counter % 2 == 0:
                cmd = restore_cmd
            else:
                cmd = block_cmd

            for server in self.cluster.servers:
                o, e = ssh_dict[server.ip].execute_command(cmd)

                self.log.info(f"Server: {server.ip}, CMD: {cmd}, O = {o}, E = {e}")

            self.sleep(interval, "Wait before blocking/restoring S3 traffic again")
            counter += 1

        self.log.info(f"Restoring network traffic after exiting loop")
        for server in self.cluster.servers:
            o, e = ssh_dict[server.ip].execute_command(restore_cmd)
            self.log.info(f"Server: {server.ip}, CMD: {restore_cmd}, O = {o}, E = {e}")

        for key in ssh_dict.keys():
            ssh_dict[key].disconnect()


    def throttle_restore_network_traffic(self, mode="s3", rate_limit=None, latency_ms=None, loss_percent=None,
                                         throttling_duration=300, wait_time=100):

        self.sleep(wait_time, "Wait before applying throttling limits")

        self.throttle_network_traffic(mode=mode, rate_limit=rate_limit, latency_ms=latency_ms, loss_percent=loss_percent)

        self.sleep(throttling_duration, "Wait before removing network throttling")

        self.remove_network_throttling()


    def throttle_network_traffic(self, mode="nfs", rate_limit=None, latency_ms=None, loss_percent=None):

        for server in self.cluster.servers:

            self.log.info(f"Throttling {mode} traffic on {server.ip}")

            shell = RemoteMachineShellConnection(server)

            # Interface detection
            if mode == "nfs":
                interface_cmd = f"ip route get {self.nfs_server_ip} | grep -oP 'dev \K\S+'"
            else:
                interface_cmd = "ip route get 8.8.8.8 | grep -oP 'dev \K\S+'"

            o, e = shell.execute_command(interface_cmd)
            interface = o[0].strip()
            self.log.info(f"Server: {server.ip}, Interface: {interface}")

            commands_to_run = []

            # Root qdisc
            if rate_limit:
                cmd1 = f"sudo tc qdisc add dev {interface} root handle 1: htb default 12"
                cmd2 = f"sudo tc class add dev {interface} parent 1: classid 1:1 htb rate {rate_limit} ceil {rate_limit}"
                commands_to_run.extend([cmd1, cmd2])
                parent_class = "1:1"
            else:
                cmd1 = f"sudo tc qdisc add dev {interface} root handle 1: prio"
                commands_to_run.append(cmd1)
                parent_class = "1:1"

            # Netem args
            netem_args = []
            if latency_ms:
                netem_args.append(f"delay {latency_ms}ms")
            if loss_percent:
                netem_args.append(f"loss {loss_percent}%")

            if netem_args:
                netem_cmd = (
                    f"sudo tc qdisc add dev {interface} parent {parent_class} "
                    f"handle 10: netem {' '.join(netem_args)}"
                )
                commands_to_run.append(netem_cmd)

            # ============================
            # FILTERS
            # ============================

            if mode == "nfs":
                # Only IP-based filtering
                commands_to_run.append(
                    f"sudo tc filter add dev {interface} protocol ip parent 1:0 prio 1 "
                    f"u32 match ip dst {self.nfs_server_ip} flowid {parent_class}"
                )
                commands_to_run.append(
                    f"sudo tc filter add dev {interface} protocol ip parent 1:0 prio 1 "
                    f"u32 match ip src {self.nfs_server_ip} flowid {parent_class}"
                )

            elif mode == "s3":
                # Only port-based filtering (HTTPS)
                commands_to_run.append(
                    f"sudo tc filter add dev {interface} protocol ip parent 1:0 prio 1 "
                    f"u32 match ip dport 443 0xffff flowid {parent_class}"
                )
                commands_to_run.append(
                    f"sudo tc filter add dev {interface} protocol ip parent 1:0 prio 1 "
                    f"u32 match ip sport 443 0xffff flowid {parent_class}"
                )

            else:
                raise ValueError("mode must be 'nfs' or 's3'")

            # Execute
            for cmd in commands_to_run:
                self.log.info(f"Server: {server.ip}, Executing CMD: {cmd}")
                shell.execute_command(cmd)

            shell.disconnect()


    def remove_network_throttling(self):

        for server in self.cluster.servers:

            self.log.info(f"Removing throttling on {server.ip}")

            shell = RemoteMachineShellConnection(server)

            interface_cmd = "ip route get 8.8.8.8 | grep -oP 'dev \\K\\S+'"
            o, e = shell.execute_command(interface_cmd)
            interface = o[0].strip()
            self.log.info(f"Server: {server.ip}, Interface: {interface}")

            cmd = f"sudo tc qdisc del dev {interface} root"

            self.log.info(f"Executing CMD: {cmd}")
            shell.execute_command(cmd)

            shell.disconnect()


    def simulate_disk_full_scenario(self, disk_full_duration=300):

        self.fill_disk(self.nfs_server, path="/data", free=500)

        self.sleep(disk_full_duration, "Wait before clearing out disk space")

        self.free_disk(self.nfs_server, path="/data")


    def fill_disk(self, server, free=100, path=None):

        path = path if path is not None else server.data_path

        def _get_disk_usage_in_MB(remote_client, path):
            disk_info = remote_client.get_disk_info(in_MB=True, path=path)
            disk_space = disk_info[1].split()[-3][:-1]
            return disk_space

        remote_client = RemoteMachineShellConnection(server)
        du = int(_get_disk_usage_in_MB(remote_client, path)) - free
        _file = os.path.join(path, "full_disk_")

        cmd = "fallocate -l {0}M {1}"
        cmd = cmd.format(du, _file + str(du) + "MB_" + str(time.time()))
        self.log.debug(cmd)
        _, error = remote_client.execute_command(cmd,
                                                 use_channel=True)
        if error:
            self.log.error("".join(error))

        du = int(_get_disk_usage_in_MB(remote_client, path))
        self.log.info("disk usage after disk full {}".format(du))

        remote_client.disconnect()


    def free_disk(self, server, path=None):

        path = path if path is not None else server.data_path

        remote_client = RemoteMachineShellConnection(server)
        _file = os.path.join(path, "full_disk_")
        command = "rm -rf {}*".format(_file)
        output, error = remote_client.execute_command(command)
        if output:
            self.log.info(output)
        if error:
            self.log.error(error)
        remote_client.disconnect()
        self.sleep(10, "Wait for files to clean up from the disk")
