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

        self.log.info("Monitoring active guest volumes")
        guest_volume_th = threading.Thread(target=self.monitor_active_guest_volumes)
        guest_volume_th.start()
        guest_volume_th.join()

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

        self.log.info("Monitoring active guest volumes")
        guest_volume_th = threading.Thread(target=self.monitor_active_guest_volumes)
        guest_volume_th.start()
        guest_volume_th.join()

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

        self.log.info("Monitoring active guest volumes")
        guest_volume_th = threading.Thread(target=self.monitor_active_guest_volumes)
        guest_volume_th.start()
        guest_volume_th.join()

        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        self.log.info("Validating item count after rebalance")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster,
                                                  self.num_items)

        self.monitor_sync_stats = False
        monitor_th.join()


    def test_s3_disk_usage_vs_local(self):
        """
        Verifies that the S3 log-store footprint for each bucket stays within
        an acceptable overhead ratio of the local on-disk footprint at two
        distinct checkpoints in the data lifecycle:

          Checkpoint 1 — after initial CREATE workload + S3 sync.
          Checkpoint 2 — after UPDATE workload + S3 sync.

        The ratio check (s3_bytes <= local_bytes * s3_overhead_ratio) is
        enforced independently at each checkpoint so that S3 amplification
        introduced specifically by the update / compaction cycle is visible
        and caught separately from the baseline create footprint.

        Steps:
          1. Load initial data via initial_load().
          2. Sleep sync_wait_sec for Fusion to upload to S3.
          3. Checkpoint 1: compare S3 vs local DU for every bucket.
          4. For each update iteration (num_upsert_iterations):
             a. Run one update pass over the full key range.
             b. Sleep sync_wait_sec for Fusion to sync to S3.
             c. Checkpoint: compare S3 vs local DU for every bucket.
          5. Assert no ratio violations were recorded at any checkpoint.

        Parameters (test params):
          s3_bucket_name       : S3 bucket name (default: cb-fusion-test)
          s3_key_prefix        : key prefix for bucket data (default: buckets/kv)
          sync_wait_sec        : seconds to wait for S3 sync at each checkpoint
                                 (default: 600)
          s3_overhead_ratio    : maximum allowed S3/local ratio (default: 1.5)
          num_upsert_iterations: update passes over the full key range
                                 (default: 2)
        """
        s3_bucket_name = self.input.param("s3_bucket_name", "cb-fusion-test")
        s3_key_prefix = self.input.param("s3_key_prefix", "buckets/kv")
        sync_wait_sec = self.input.param("sync_wait_sec", 300)
        s3_overhead_ratio = self.input.param("s3_overhead_ratio", 1.5)
        num_upsert_iterations = self.input.param("num_upsert_iterations", 2)

        def _parse_s3_human_size(size_str):
            """Convert an AWS --human-readable size string to bytes.

            Handles '4.5 GiB', '1.2 MiB', '512 KiB', '1 Byte' / '3 Bytes'
            and decimal variants (KB/MB/GB).
            """
            _units = {
                'B': 1, 'BYTE': 1, 'BYTES': 1,
                'KB': 1000, 'KIB': 1024,
                'MB': 1000 ** 2, 'MIB': 1024 ** 2,
                'GB': 1000 ** 3, 'GIB': 1024 ** 3,
                'TB': 1000 ** 4, 'TIB': 1024 ** 4,
            }
            parts = size_str.strip().split()
            if len(parts) == 2:
                value = float(parts[0])
                unit = parts[1].upper().rstrip('S')
                return int(value * _units.get(unit, 1))
            return int(float(size_str.strip()))

        def _compare_du(checkpoint_label, failures):
            """Measure local + S3 DU for every bucket and record violations."""
            self.log.info(f"--- DU Checkpoint: {checkpoint_label} ---")
            for bucket in self.cluster.buckets:
                bucket_uuid = self.get_bucket_uuid(bucket.name)
                self.log.info(
                    f"[{checkpoint_label}] Bucket '{bucket.name}' "
                    f"(uuid={bucket_uuid})"
                )

                # Local disk usage — sum across all nodes
                local_bytes = 0
                for server in self.cluster.nodes_in_cluster:
                    ssh = RemoteMachineShellConnection(server)
                    du_cmd = f"du -sb {self.data_path}/{bucket_uuid}"
                    output, error = ssh.execute_command(du_cmd)
                    ssh.disconnect()
                    if output:
                        try:
                            local_bytes += int(output[0].split()[0])
                        except (IndexError, ValueError) as exc:
                            self.log.warning(
                                f"[{server.ip}] Could not parse du output "
                                f"'{output}': {exc}"
                            )
                    else:
                        self.log.warning(
                            f"[{server.ip}] du returned no output for "
                            f"{self.data_path}/{bucket_uuid} (error={error})"
                        )

                self.log.info(
                    f"[{checkpoint_label}] Bucket '{bucket.name}': "
                    f"local = {local_bytes:,} bytes "
                    f"({local_bytes / (1024 ** 3):.3f} GiB) "
                    f"across {len(self.cluster.nodes_in_cluster)} node(s)"
                )

                if local_bytes == 0:
                    self.log.warning(
                        f"[{checkpoint_label}] Bucket '{bucket.name}': "
                        f"local disk usage is 0, skipping S3 comparison"
                    )
                    continue

                # S3 disk usage
                s3_path = (
                    f"s3://{s3_bucket_name}/{s3_key_prefix}/{bucket_uuid}/"
                )
                s3_cmd = (
                    f"aws s3 ls {s3_path} "
                    f"--recursive --human-readable --summarize | tail -n 2"
                )
                self.log.info(
                    f"[{checkpoint_label}] Running S3 usage cmd: {s3_cmd}"
                )
                ssh = RemoteMachineShellConnection(self.cluster.master)
                s3_output, s3_error = ssh.execute_command(s3_cmd)
                ssh.disconnect()
                self.log.info(
                    f"[{checkpoint_label}] S3 output: {s3_output}, "
                    f"error: {s3_error}"
                )

                s3_bytes = None
                for line in s3_output:
                    if "Total Size" in line:
                        size_part = line.split("Total Size:")[-1].strip()
                        try:
                            s3_bytes = _parse_s3_human_size(size_part)
                        except (ValueError, IndexError) as exc:
                            self.log.warning(
                                f"[{checkpoint_label}] Bucket '{bucket.name}':"
                                f" could not parse S3 size from '{line}': {exc}"
                            )
                        break

                if s3_bytes is None:
                    self.log.warning(
                        f"[{checkpoint_label}] Bucket '{bucket.name}': "
                        f"'Total Size' not found in S3 output — skipping"
                    )
                    continue

                self.log.info(
                    f"[{checkpoint_label}] Bucket '{bucket.name}': "
                    f"S3 = {s3_bytes:,} bytes "
                    f"({s3_bytes / (1024 ** 3):.3f} GiB)"
                )

                ratio = s3_bytes / local_bytes
                max_allowed = local_bytes * s3_overhead_ratio
                self.log.info(
                    f"[{checkpoint_label}] Bucket '{bucket.name}': "
                    f"S3/local ratio = {ratio:.3f} "
                    f"(limit = {s3_overhead_ratio}x, "
                    f"max_allowed_s3 = {max_allowed:,.0f} bytes)"
                )

                if s3_bytes > max_allowed:
                    msg = (
                        f"[{checkpoint_label}] Bucket '{bucket.name}': "
                        f"S3 usage ({s3_bytes:,} bytes) exceeds "
                        f"{s3_overhead_ratio}x local usage "
                        f"({local_bytes:,} bytes). Ratio = {ratio:.3f}"
                    )
                    self.log.error(msg)
                    failures.append(msg)
                else:
                    self.log.info(
                        f"[{checkpoint_label}] Bucket '{bucket.name}': "
                        f"PASSED ({ratio:.3f} <= {s3_overhead_ratio})"
                    )

        failures = []

        # ---- Phase 1: initial CREATE workload ----
        self.log.info("Phase 1: Loading initial data")
        self.initial_load()
        self.bucket_util.print_bucket_stats(self.cluster)

        # ---- Phase 2: wait for S3 sync, then Checkpoint 1 ----
        self.sleep(sync_wait_sec, "Waiting for Fusion to sync CREATE data to S3")
        _compare_du("after_create", failures)

        # ---- Phase 3: UPDATE workload — DU checked after every iteration ----
        self.log.info(
            f"Phase 3: Running update workload "
            f"({num_upsert_iterations} iteration(s)), "
            f"DU comparison after each iteration"
        )
        self.sleep(30, "Wait before starting update workload")
        mutate = 1
        remaining = num_upsert_iterations
        while remaining > 0:
            iteration = num_upsert_iterations - remaining + 1
            self.log.info(
                f"Phase 3: Update iteration {iteration}/{num_upsert_iterations}"
            )
            self.doc_ops = "update"
            self.reset_doc_params()
            self.update_start = 0
            self.update_end = self.num_items
            self.java_doc_loader(
                wait=True,
                skip_default=self.skip_load_to_default_collection,
                monitor_ops=False,
                mutate=mutate,
                ops_rate=self.ops_rate,
            )
            self.bucket_util.print_bucket_stats(self.cluster)
            self.sleep(
                sync_wait_sec,
                f"Waiting for Fusion to sync UPDATE data to S3 "
                f"(iteration {iteration}/{num_upsert_iterations})",
            )
            _compare_du(f"after_update_iter_{iteration}", failures)
            remaining -= 1
            mutate += 1
            self.sleep(30, "Wait after update iteration")

        # ---- Final assertion ----
        self.assertEqual(
            len(failures), 0,
            f"S3 disk usage exceeded the {s3_overhead_ratio}x ratio at "
            f"{len(failures)} checkpoint(s):\n" + "\n".join(failures)
        )
        self.log.info("test_s3_disk_usage_vs_local complete")

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
