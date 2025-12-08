import json
import os
import random
import subprocess
import threading
import time
from BucketLib.bucket import Bucket
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_server_rest_util.fusion.fusion_api import FusionRestAPI
from cb_tools.cbstats import Cbstats
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_base import MagmaBaseTest
from shell_util.remote_connection import RemoteMachineShellConnection


class FusionSync(MagmaBaseTest, FusionBase):
    def setUp(self):
        super(FusionSync, self).setUp()

        self.log.info("FusionSync setUp Started")

        self.upsert_iterations = self.input.param("upsert_iterations", 2)
        self.monitor_log_store = self.input.param("monitor_log_store", True)

        # Maintain two dicts. One to monitor and one to record violations
        if self.monitor_log_store:
            self.kvstore_stats = dict()
            self.kvstore_violations = dict()
            for bucket in self.cluster.buckets:
                self.kvstore_stats[bucket.name] = dict()
                self.kvstore_violations[bucket.name] = dict()
                for i in range(bucket.numVBuckets):
                    self.kvstore_stats[bucket.name][i] = dict()
                    self.kvstore_violations[bucket.name][i] = dict()

    def tearDown(self):
        if self.monitor_log_store:
            self.monitor = False
            self.sleep(10, "Wait after stopping monitor threads")

            for bucket in self.cluster.buckets:
                for kvstore_num in range(bucket.numVBuckets):
                    self.kvstore_stats[bucket.name][kvstore_num]["file_creation"] = \
                        list(self.kvstore_stats[bucket.name][kvstore_num]["file_creation"])
                    self.kvstore_stats[bucket.name][kvstore_num]["file_deletion"] = \
                        list(self.kvstore_stats[bucket.name][kvstore_num]["file_deletion"])

            stat_file_path = os.path.join(self.fusion_output_dir, "kvstore_stats.json")
            with open(stat_file_path, "w") as f:
                json.dump(self.kvstore_stats, f, indent=4)

            violation_file_path = os.path.join(self.fusion_output_dir, "kvstore_violations.json")
            with open(violation_file_path, "w") as f:
                json.dump(self.kvstore_violations, f, indent=4)

        super(FusionSync, self).tearDown()


    def test_fusion_sync(self):

        self.log.info("Fusion Sync Test Started")

        if self.monitor_log_store:
            monitor_threads = self.start_monitor_dir(validate=True)

        self.log.info("Starting initial load")
        self.initial_load()
        self.sleep(30, "Sleep after data loading")

        num_upsert_iterations = self.upsert_iterations
        while num_upsert_iterations > 0:
            self.doc_ops = "update"
            self.reset_doc_params()
            self.update_start = 0
            self.update_end = self.num_items
            self.log.info(f"Performing update workload iteration: {self.upsert_iterations - num_upsert_iterations + 1}")
            self.log.info(f"Update start = {self.update_start}, Update End = {self.update_end}")
            self.java_doc_loader(wait=True, skip_default=self.skip_load_to_default_collection, monitor_ops=False)
            num_upsert_iterations -= 1
            self.sleep(30, "Wait after update workload")

        if self.monitor_log_store:
            self.monitor = False
            for th in monitor_threads:
                th.join()


    def test_fusion_sync_network_failure(self):

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 60
        self.sleep(sleep_time, "Sleep after data loading")
        self.bucket_util.print_bucket_stats(self.cluster)

        # Block NFS Traffic
        self.block_nfs_traffic()

        self.sleep(30, "Wait after blocking traffic")

        du_th = threading.Thread(target=self.monitor_fusion_disk_usage)
        for bucket in self.cluster.buckets:
            stats_th = threading.Thread(target=self.monitor_fusion_sync_stats, args=[bucket])

        du_th.start()
        stats_th.start()

        # Perform another data workload
        self.log.info("Starting another workload after blocking NFS traffic")
        self.perform_workload(self.num_items,
                              self.num_items * 2,
                              ops_rate=10000)

        self.sleep(600, "Sleep after data loading")
        self.bucket_util.print_bucket_stats(self.cluster)

        # Restore NFS traffic
        self.restore_nfs_traffic()

        self.sleep(600, "Sleep after restoring NFS traffic for Fusion Sync to resume")

        # Stopping monitoring threads
        self.monitor_du = False
        self.monitor_stats = False
        du_th.join()
        stats_th.join()

        # Perform a Fusion Rebalance
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1)

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


    def test_fusion_sync_with_crash(self):

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 60
        self.sleep(sleep_time, "Sleep after data loading")
        self.bucket_util.print_bucket_stats(self.cluster)

        # Start a crash thread which kills memcached in random intervals
        crash_th = threading.Thread(target=self.crash_during_sync)
        crash_th.start()

        self.sleep(30, "Sleep after starting crash thread")

        # Start another data workload
        self.log.info("Starting another workload")
        self.perform_workload(self.num_items,
                              self.num_items * 2,
                              ops_rate=10000)

        self.sleep(600, "Sleep after data loading")
        self.bucket_util.print_bucket_stats(self.cluster)

        # Stopping crash thread
        self.stop_crash = True
        crash_th.join()

        self.sleep(30, "Wait before performing a Fusion Rebalance")

        # Perform a Fusion Rebalance
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1)

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


    def test_fusion_sync_throttling(self):

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 60
        self.sleep(sleep_time, "Sleep after data loading")
        self.bucket_util.print_bucket_stats(self.cluster)

        self.sleep(60, "Wait before applying throttling limits")

        # Apply NFS throttling
        self.throttle_nfs_traffic(rate_limit="800kbit")

        self.sleep(30, "Wait after applying throttling limits")

        du_th = threading.Thread(target=self.monitor_fusion_disk_usage)
        for bucket in self.cluster.buckets:
            stats_th = threading.Thread(target=self.monitor_fusion_sync_stats, args=[bucket])

        du_th.start()
        stats_th.start()

        # Perform another data load
        self.log.info("Starting another workload")
        self.perform_workload(self.num_items,
                              self.num_items * 2,
                              ops_rate=10000)

        self.sleep(300, "Sleep after data loading")
        self.bucket_util.print_bucket_stats(self.cluster)

        # Remove NFS throttling
        self.remove_nfs_throttling()

        self.sleep(600, "Sleep after removing NFS throttling for Fusion Sync to resume")

        # Stopping monitoring threads
        self.monitor_du = False
        self.monitor_stats = False
        du_th.join()
        stats_th.join()

        # Perform a Fusion Rebalance
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1)

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


    def test_fusion_reupload_from_scratch(self):

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 60
        self.sleep(sleep_time, "Sleep after data loading")
        self.bucket_util.print_bucket_stats(self.cluster)

        # Make files on log store immutable to pause log cleaning
        self.make_log_files_immutable()
        self.sleep(30, "Wait after making files immutable")

        # Delete Fusion sync state files
        self.delete_sync_state_files()

        du_th = threading.Thread(target=self.monitor_fusion_disk_usage)
        du_th.start()

        self.sleep(60, "Wait after deleting sync state files")

        # Load more data so that a sync is made to the log store
        self.perform_workload(self.num_items, self.num_items + 500000,
                              "create", ops_rate=5000)
        self.sleep(300, "Wait after performing data load")

        self.monitor_du = False
        du_th.join()

        # Perform a Fusion Rebalance
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1)

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

        # Make files on log store mutable to resume log cleaning
        self.make_log_files_mutable()

        du_th = threading.Thread(target=self.monitor_fusion_disk_usage)
        du_th.start()

        self.sleep(180, "Wait, monitoring log store DU")

        self.monitor_du = False
        du_th.join()


    def test_crash_during_large_file_sync(self):

        crash_interval = self.input.param("crash_interval", 60)

        self.log.info("Verifying that Fusion is disabled initially")
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Status = {status}, Content = {content}")

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")
        self.bucket_util.print_bucket_stats(self.cluster)

        # Enable Fusion
        self.log.info("Configuring Fusion settings")
        self.configure_fusion()
        self.log.info("Enabling Fusion midway")
        enable_fusion_th = threading.Thread(target=self.enable_fusion, args=[])
        enable_fusion_th.start()

        crash_th = threading.Thread(target=self.crash_during_sync, args=[int(crash_interval), 600])
        crash_th.start()

        enable_fusion_th.join()
        self.stop_crash = True
        crash_th.join()

        self.sleep(60, "Wait after Fusion is enabled")

        # Perform a workload
        self.perform_workload(self.num_items, self.num_items * 1.5,
                              "create", ops_rate=10000)
        self.sleep(sleep_time, "Wait after performing data load")
        self.bucket_util.print_bucket_stats(self.cluster)

        # Perform a Fusion Rebalance
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1)

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


    def test_crash_during_upload_with_rebalance(self):

        num_iterations = self.input.param("num_iterations", 5)
        crash_wait_time = self.input.param("crash_wait_time", 30)

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        self.log.info("Setting migration rate limit to {0} MB/s".format(
            self.fusion_migration_rate_limit / (1024 * 1024)))
        ClusterRestAPI(self.cluster.master).\
            manage_global_memcached_setting(fusion_migration_rate_limit=self.fusion_migration_rate_limit)

        self.log.info("Starting continuous overwrite workload at 5000 ops/sec")
        doc_loading_tasks = self.perform_workload(0, self.num_items, doc_op="update",
                                                   wait=False, ops_rate=5000)

        rebalance_count = 1
        for iteration in range(1, num_iterations + 1):
            self.log.info("=== Iteration {0}/{1} ===".format(iteration, num_iterations))

            self.log.info("Killing memcached on all nodes")
            for server in self.cluster.nodes_in_cluster:
                shell = RemoteMachineShellConnection(server)
                try:
                    self.log.info("Killing memcached on {0}".format(server.ip))
                    shell.kill_memcached()
                finally:
                    shell.disconnect()

            self.sleep(crash_wait_time, "Wait for bucket recovery")
            self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)

            self.log.info("Validating reads from disk after crash")
            self.perform_batch_reads(num_docs_to_validate=min(self.num_items, 1000000),
                                     batch_size=500000, validate_docs=True)

            self.log.info("Running a Fusion rebalance (swap rebalance)")
            nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                                  rebalance_count=rebalance_count)
            rebalance_count += 1

            self.log.info("Monitoring extent migration on nodes: {0}".format(nodes_to_monitor))
            extent_migration_array = list()
            for node in nodes_to_monitor:
                for bucket in self.cluster.buckets:
                    extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                    extent_th.start()
                    extent_migration_array.append(extent_th)

            for th in extent_migration_array:
                th.join()

            self.cluster_util.print_cluster_stats(self.cluster)
            self.bucket_util.print_bucket_stats(self.cluster)

            self.log.info("Validating reads from FusionFS after rebalance")
            self.perform_batch_reads(num_docs_to_validate=min(self.num_items, 1000000),
                                     batch_size=500000, validate_docs=True)

            self.log.info("Checking uploader info (validate no upload from scratch)")
            self.get_fusion_uploader_info()

        self.log.info("Stopping continuous update workload")
        for task in doc_loading_tasks:
            self.doc_loading_tm.get_task_result(task)

        self.log.info("Validating item count after all iterations")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)

        self.log.info("Checking final stats for sync, migration, and read failures")
        for node in self.cluster.nodes_in_cluster:
            for bucket in self.cluster.buckets:
                cbstats = Cbstats(node)
                stats = cbstats.all_stats(bucket.name)
                sync_failures = int(stats['ep_fusion_sync_failures'])
                migration_failures = int(stats['ep_fusion_migration_failures'])
                read_failures = int(stats['ep_data_read_failed'])
                
                self.log.info("Final - Node {0}, Bucket {1}: "
                            "Sync failures={2}, Migration failures={3}, Read failures={4}".format(
                                node.ip, bucket.name, sync_failures, migration_failures, read_failures))
                
                self.assertEqual(sync_failures, 0,
                    "Sync failures on {0}:{1}".format(node.ip, bucket.name))
                self.assertEqual(migration_failures, 0,
                    "Migration failures on {0}:{1}".format(node.ip, bucket.name))
                self.assertEqual(read_failures, 0,
                    "Read failures on {0}:{1}".format(node.ip, bucket.name))
                
                cbstats.disconnect()


    def test_fusion_sync_remove_write_permissions(self):

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")
        self.bucket_util.print_bucket_stats(self.cluster)

        # Remove write permission on the log store
        self.remove_write_permissions_log_store()

        self.sleep(30, "Wait after removing permissions")

        # Perform another data load
        self.perform_workload(self.num_items, self.num_items * 2,
                              "create", ops_rate=10000)
        self.sleep(sleep_time, "Wait after performing data load")
        self.bucket_util.print_bucket_stats(self.cluster)

        # Perform a Fusion Rebalance
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1)

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

        du_th = threading.Thread(target=self.monitor_fusion_disk_usage, args=[5])
        du_th.start()

        self.sleep(30, "Wait before restoring write permissions")
        self.restore_write_permissions_log_store()

        self.sleep(200, "Wait after restoring write permissions")

        self.monitor_du = False
        du_th.join()

        # Perform another Fusion Rebalance
        self.log.info("Running a Fusion rebalance after restoring write permissions")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=2,
                                              rebalance_sleep_time=60)
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


    def crash_during_sync(self, interval=None, timeout=None):

        self.stop_crash = False
        end_time = time.time() + timeout if timeout else None

        while not self.stop_crash and (end_time is None or time.time() < end_time):

            for server in self.cluster.nodes_in_cluster:

                self.log.info(f"Killing memcached on {server.ip}")
                shell = RemoteMachineShellConnection(server)
                shell.kill_memcached()

            if not interval:
                sleep_time = random.randint(int(self.fusion_upload_interval) - 30, int(self.fusion_upload_interval) + 30)
            else:
                sleep_time = interval
            self.sleep(sleep_time, f"Waiting for {sleep_time} sec to kill memcached on all nodes")


    def block_nfs_traffic(self):

        for server in self.cluster.servers:

            shell = RemoteMachineShellConnection(server)
            incoming_traffic_cmd = f"sudo iptables -A INPUT  -p tcp -s {self.nfs_server_ip} --sport 2049 -j DROP"
            outgoing_traffic_cmd = f"sudo iptables -A OUTPUT -p tcp -d {self.nfs_server_ip} --dport 2049 -j DROP"

            self.log.info(f"Blocking NFS network traffic on {server.ip}")
            o, e = shell.execute_command(incoming_traffic_cmd)
            o, e = shell.execute_command(outgoing_traffic_cmd)

            shell.disconnect()


    def restore_nfs_traffic(self):

        for server in self.cluster.servers:

            shell = RemoteMachineShellConnection(server)
            incoming_traffic_cmd = f"sudo iptables -D INPUT  -p tcp -s {self.nfs_server_ip} --sport 2049 -j DROP"
            outgoing_traffic_cmd = f"sudo iptables -D OUTPUT -p tcp -d {self.nfs_server_ip} --dport 2049 -j DROP"

            self.log.info(f"Restoring NFS network traffic on {server.ip}")
            o, e = shell.execute_command(incoming_traffic_cmd)
            o, e = shell.execute_command(outgoing_traffic_cmd)

            shell.disconnect()


    def throttle_nfs_traffic(self, rate_limit):

        for server in self.cluster.servers:

            self.log.info(f"Throttling NFS traffic on {server.ip}")

            shell = RemoteMachineShellConnection(server)

            interface_cmd = f"ip route get {self.nfs_server_ip} | grep -oP 'dev \K\S+'"
            o, e = shell.execute_command(interface_cmd)
            interface = o[0].strip()

            cmd1 = f"sudo tc qdisc add dev {interface} root handle 1: htb default 12"
            cmd2 = f"sudo tc class add dev {interface} parent 1: classid 1:1 htb rate {rate_limit} ceil {rate_limit}"
            cmd3 = f"sudo tc filter add dev {interface} protocol ip parent 1:0 prio 1 u32 match ip dst {self.nfs_server_ip} flowid 1:1"

            for cmd in [cmd1, cmd2, cmd3]:
                self.log.info(f"Executing CMD: {cmd}")
                shell.execute_command(cmd)

            shell.disconnect()


    def remove_nfs_throttling(self):

        for server in self.cluster.servers:

            self.log.info(f"Removing NFS throttling on {server.ip}")

            shell = RemoteMachineShellConnection(server)

            interface_cmd = f"ip route get {self.nfs_server_ip} | grep -oP 'dev \K\S+'"
            o, e = shell.execute_command(interface_cmd)
            interface = o[0].strip()

            cmd = f"sudo tc qdisc del dev {interface} root"

            self.log.info(f"Executing CMD: {cmd}")
            shell.execute_command(cmd)

            shell.disconnect()


    def monitor_fusion_disk_usage(self, interval=10):

        du_cmd = f"du -sb {self.nfs_server_path}"
        ssh = RemoteMachineShellConnection(self.nfs_server)

        self.monitor_du = True

        while self.monitor_du:

            o, e = ssh.execute_command(du_cmd)
            current_du = int(o[0].split("\t")[0])

            self.log.info(f"Log Store DU = {current_du}")

            time.sleep(interval)


    def monitor_fusion_sync_stats(self, bucket, interval=10):

        stats_to_monitor = ["ep_fusion_bytes_synced", "ep_fusion_log_store_data_size",
                            "ep_fusion_sync_failures", "ep_fusion_sync_session_completed_bytes",
                            "ep_fusion_sync_session_total_bytes", "ep_fusion_syncs"]

        self.monitor_stats = True

        while self.monitor_stats:

            stat_dict = dict()

            for server in self.cluster.nodes_in_cluster:
                stat_dict[server.ip] = dict()
                cbstats = Cbstats(server)
                result = cbstats.all_stats(bucket.name)

                for stat in stats_to_monitor:
                    stat_dict[server.ip][stat] = result[stat]

            self.log.info(f"Bucket: {bucket.name}, Sync Stats = {stat_dict}")

            time.sleep(interval)


    def make_log_files_immutable(self):

        self.log.info("Making Fusion log file operations immutable")

        shell = RemoteMachineShellConnection(self.nfs_server)

        for bucket in self.cluster.buckets:

            cmd = f"find /data/nfs/{self.client_share_dir}/buckets/kv/{bucket.uuid}/ -type f -exec chattr +i {{}} \\;"

            self.log.info(f"Executing CMD: {cmd}")
            shell.execute_command(cmd)

        shell.disconnect()


    def make_log_files_mutable(self):

        self.log.info("Making Fusion log file operations mutable")

        shell = RemoteMachineShellConnection(self.nfs_server)

        for bucket in self.cluster.buckets:

            cmd = f"find /data/nfs/{self.client_share_dir}/buckets/kv/{bucket.uuid}/ -type f -exec chattr -i {{}} \\;"

            self.log.info(f"Executing CMD: {cmd}")
            shell.execute_command(cmd)

        shell.disconnect()


    def delete_sync_state_files(self):

        self.log.info("Deleting Fusion Sync State files")

        for server in self.cluster.nodes_in_cluster:

            shell = RemoteMachineShellConnection(server)

            shell.stop_couchbase()
            self.sleep(20, "Wait after stopping server")

            for bucket in self.cluster.buckets:

                bucket_path = os.path.join(self.data_path, bucket.uuid)

                delete_cmd = f"find {bucket_path}/ -type f -name 'sync-*' -delete"

                self.log.info(f"Server: {server.ip}, Bucket: {bucket.name}, Executing CMD: {delete_cmd}")
                shell.execute_command(delete_cmd)

            shell.start_couchbase()
            self.sleep(10, "Wait after starting server")

            shell.disconnect()


    def remove_write_permissions_log_store(self):

        self.log.info("Removing write permissions on the log store")

        local_nfs_path = self.fusion_log_store_uri.split("//")[-1]

        cmd = f"chmod -R a-w {local_nfs_path}/"

        for server in self.cluster.servers:
            shell = RemoteMachineShellConnection(server)
            o, e = shell.execute_command(cmd)
            self.log.info(f"Removing permissions on server: {server.ip}")
            self.log.info(f"CMD: {cmd}, O = {o}, E = {e}")
            shell.disconnect()


    def restore_write_permissions_log_store(self):

        self.log.info("Restoring write permissions on the log store")

        local_nfs_path = self.fusion_log_store_uri.split("//")[-1]

        cmd = f"chmod -R u+w {local_nfs_path}/"

        for server in self.cluster.servers:
            shell = RemoteMachineShellConnection(server)
            o, e = shell.execute_command(cmd)
            self.log.info(f"Restoring permissions on server: {server.ip}")
            self.log.info(f"CMD: {cmd}, O = {o}, E = {e}")
            shell.disconnect()


    def test_block_size_change_during_sync(self):
        new_key_block_size = self.input.param("new_magma_key_tree_data_block_size", 2048)
        new_seq_block_size = self.input.param("new_magma_seq_tree_data_block_size", 8192)

        self.log.info("Starting initial load")
        self.initial_load()

        self.override_fusion_settings()
        status, content = ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
                            fusion_sync_rate_limit=self.fusion_sync_rate_limit,
                            fusion_migration_rate_limit=self.fusion_migration_rate_limit)
        self.log.info(f"Status = {status}, Content = {content}")


        self.log.info("Enabling Fusion")
        self.configure_fusion()
        self.enable_fusion()

        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Print sync stats before changing block sizes
        self.log.info("Printing Fusion sync stats before changing block sizes")
        for bucket in self.cluster.buckets:
            for node in self.cluster.nodes_in_cluster:
                cbstats = Cbstats(node)
                stats = cbstats.all_stats(bucket.name)
                self.log.info("Node {0}, Bucket {1}: "
                            "bytes_synced={2}, log_store_size={3}, "
                            "sync_session_completed={4}, sync_session_total={5}, "
                            "total_syncs={6}, sync_failures={7}".format(
                                node.ip, bucket.name,
                                stats['ep_fusion_bytes_synced'],
                                stats['ep_fusion_log_store_data_size'],
                                stats['ep_fusion_sync_session_completed_bytes'],
                                stats['ep_fusion_sync_session_total_bytes'],
                                stats['ep_fusion_syncs'],
                                stats['ep_fusion_sync_failures']))
                cbstats.disconnect()

        self.log.info(f"Changing block sizes: key={new_key_block_size}, seq={new_seq_block_size}")
        for bucket in self.cluster.buckets:
            self.bucket_util.update_bucket_property(
                self.cluster.master, bucket,
                magma_key_tree_data_block_size=new_key_block_size,
                magma_seq_tree_data_block_size=new_seq_block_size
            )

        for bucket in self.cluster.buckets:
            self.assertEqual(bucket.magmaKeyTreeDataBlockSize, new_key_block_size)
            self.assertEqual(bucket.magmaSeqTreeDataBlockSize, new_seq_block_size)

        # Check log files before disabling Fusion
        self.log.info("Checking log files on NFS before disabling Fusion")
        log_files_exist_before, log_count_before = self.check_log_files_on_nfs(self.nfs_server, self.nfs_server_path)
        self.log.info(f"Log files exist before disable: {log_files_exist_before}, Count: {log_count_before}")

        # Disable Fusion
        self.log.info("Disabling Fusion")
        self.disable_fusion()

        # Wait for upload interval to allow cleanup
        self.sleep(self.fusion_upload_interval, "Wait after disabling Fusion for log cleanup")

        # Verify log files are deleted after disabling Fusion
        self.log.info("Checking if log files are deleted from NFS after disabling Fusion")
        log_files_exist_after, log_count_after = self.check_log_files_on_nfs(self.nfs_server, self.nfs_server_path)
        self.log.info(f"Log files exist after disable: {log_files_exist_after}, Count: {log_count_after}")

        # Validation: Log files should be deleted after disabling Fusion
        if log_files_exist_after:
            self.log.warning(f"Log files still exist after disabling Fusion. Count: {log_count_after}")
        else:
            self.log.info("Validation passed: Log files deleted successfully after disabling Fusion")

        # Re-enable Fusion
        self.log.info("Re-enabling Fusion")
        self.enable_fusion()

        # Wait for Fusion to stabilize after re-enabling
        self.sleep(60, "Wait for Fusion to stabilize after re-enabling")

        self.log.info("Starting Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir, rebalance_count=1)

        extent_migration_threads = []
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_migration_threads.append(th)
                th.start()

        for th in extent_migration_threads:
            th.join()

        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)

        # Final validation: Check for sync, migration, and read failures
        self.log.info("Checking final stats for sync, migration, and read failures")
        for node in self.cluster.nodes_in_cluster:
            for bucket in self.cluster.buckets:
                cbstats = Cbstats(node)
                stats = cbstats.all_stats(bucket.name)
                sync_failures = int(stats['ep_fusion_sync_failures'])
                migration_failures = int(stats['ep_fusion_migration_failures'])
                read_failures = int(stats['ep_data_read_failed'])
                
                self.log.info("Final - Node {0}, Bucket {1}: "
                            "Sync failures={2}, Migration failures={3}, Read failures={4}".format(
                                node.ip, bucket.name, sync_failures, migration_failures, read_failures))
                
                self.assertEqual(sync_failures, 0,
                    "Sync failures on {0}:{1}".format(node.ip, bucket.name))
                self.assertEqual(migration_failures, 0,
                    "Migration failures on {0}:{1}".format(node.ip, bucket.name))
                self.assertEqual(read_failures, 0,
                    "Read failures on {0}:{1}".format(node.ip, bucket.name))
                
                cbstats.disconnect()


    def test_flush_during_sync(self):
        self.log.info("Starting initial load")
        self.initial_load()

        self.override_fusion_settings()
        ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
            fusion_sync_rate_limit=self.fusion_sync_rate_limit,
            fusion_migration_rate_limit=self.fusion_migration_rate_limit)

        self.configure_fusion()
        self.enable_fusion()

        # Wait for approximately 20GB to be synced before flushing
        target_bytes_synced = 20 * 1024 * 1024 * 1024  # 20GB in bytes
        self.log.info(f"Waiting for approximately 20GB ({target_bytes_synced} bytes) to be synced before flush")
        
        max_wait_time = 3600  # 1 hour max wait
        start_time = time.time()
        sync_threshold_reached = False
        
        while time.time() - start_time < max_wait_time:
            total_bytes_synced = 0
            for bucket in self.cluster.buckets:
                for node in self.cluster.nodes_in_cluster:
                    cbstats = Cbstats(node)
                    stats = cbstats.all_stats(bucket.name)
                    bytes_synced = int(stats.get('ep_fusion_bytes_synced', 0))
                    total_bytes_synced += bytes_synced
                    cbstats.disconnect()
            
            self.log.info(f"Total bytes synced so far: {total_bytes_synced} bytes ({total_bytes_synced / (1024**3):.2f} GB)")
            
            if total_bytes_synced >= target_bytes_synced:
                self.log.info(f"Target sync threshold of 20GB reached. Proceeding with flush.")
                sync_threshold_reached = True
                break
            
            self.sleep(30, "Waiting for more data to sync")
        
        if not sync_threshold_reached:
            self.log.warning(f"Did not reach 20GB sync target in {max_wait_time}s. Proceeding with flush anyway.")

        # Check NFS directory size before flush
        ssh = RemoteMachineShellConnection(self.nfs_server)
        self.log.info(f"Checking NFS directory size BEFORE flush: {self.nfs_server_path}")
        o, e = ssh.execute_command(f"du -sh {self.nfs_server_path}")
        self.log.info(f"NFS directory size BEFORE flush: {o}")
        ssh.disconnect()

        # Capture sync stats before flush
        stats_before_flush = {}
        for bucket in self.cluster.buckets:
            stats_before_flush[bucket.name] = {}
            for node in self.cluster.nodes_in_cluster:
                cbstats = Cbstats(node)
                stats = cbstats.all_stats(bucket.name)
                stats_before_flush[bucket.name][node.ip] = {
                    'bytes_synced': int(stats['ep_fusion_bytes_synced']),
                    'total_syncs': int(stats['ep_fusion_syncs'])
                }
                cbstats.disconnect()

        # Enable flush and flush all buckets
        for bucket in self.cluster.buckets:
            self.bucket_util.update_bucket_property(self.cluster.master, bucket,
                                                    flush_enabled=Bucket.FlushBucket.ENABLED)
            self.bucket_util.flush_bucket(self.cluster, bucket)

        # Wait for upload interval after flush to observe NFS state
        sleep_time = self.fusion_upload_interval
        self.sleep(sleep_time, "Wait after flushing buckets for upload interval")

        # Validation: Check NFS directory size - it should NOT be empty/zero
        ssh = RemoteMachineShellConnection(self.nfs_server)
        self.log.info(f"Checking NFS directory size after flush: {self.nfs_server_path}")
        o, e = ssh.execute_command(f"du -sh {self.nfs_server_path}")
        self.log.info(f"NFS directory size after flush: {o}")
        
        # Verify directory exists and has some data
        dir_check_o, dir_check_e = ssh.execute_command(f"[ -d {self.nfs_server_path} ] && echo 'exists' || echo 'not_exists'")
        ssh.disconnect()
        
        self.assertIn('exists', dir_check_o[0], f"Log store path should exist after flush")
        self.log.info("Validation passed: NFS directory exists after flush (not empty/deleted)")

        # Validation: Sync stats should remain same after flush
        for bucket in self.cluster.buckets:
            for node in self.cluster.nodes_in_cluster:
                cbstats = Cbstats(node)
                stats = cbstats.all_stats(bucket.name)
                bytes_synced_after = int(stats['ep_fusion_bytes_synced'])
                total_syncs_after = int(stats['ep_fusion_syncs'])
                bytes_synced_before = stats_before_flush[bucket.name][node.ip]['bytes_synced']
                total_syncs_before = stats_before_flush[bucket.name][node.ip]['total_syncs']

                self.assertEqual(bytes_synced_after, bytes_synced_before,
                    "Sync stats should remain same after flush on {0}:{1}".format(node.ip, bucket.name))
                self.assertEqual(total_syncs_after, total_syncs_before,
                    "Total syncs should remain same after flush on {0}:{1}".format(node.ip, bucket.name))
                cbstats.disconnect()

        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, 0)

        # Refresh bucket metadata after flush
        self.log.info("Refreshing bucket metadata after flush")
        for bucket in self.cluster.buckets:
            self.bucket_util.get_updated_bucket_server_list(self.cluster, bucket)

        # Load data after flush using cbc-pillowfight
        self.log.info("Loading 100k documents after flush using cbc-pillowfight")
        self.num_items = 100000
        for bucket in self.cluster.buckets:
            self.load_data_cbc_pillowfight(self.cluster.master, bucket, 
                                          self.num_items, self.doc_size, 
                                          key_prefix="post_flush", threads=4)

        sleep_time = 120 + self.fusion_upload_interval + 60
        self.sleep(sleep_time, "Wait for Fusion sync after post-flush load")

        # Check log files before disabling Fusion
        self.log.info("Checking log files on NFS before disabling Fusion")
        log_files_exist_before, log_count_before = self.check_log_files_on_nfs(self.nfs_server, self.nfs_server_path)
        self.log.info(f"Log files exist before disable: {log_files_exist_before}, Count: {log_count_before}")

        # Disable Fusion
        self.log.info("Disabling Fusion")
        self.disable_fusion()

        # Wait for upload interval to allow cleanup
        self.sleep(self.fusion_upload_interval, "Wait after disabling Fusion for log cleanup")

        # Verify log files are deleted after disabling Fusion
        self.log.info("Checking if log files are deleted from NFS after disabling Fusion")
        log_files_exist_after, log_count_after = self.check_log_files_on_nfs(self.nfs_server, self.nfs_server_path)
        self.log.info(f"Log files exist after disable: {log_files_exist_after}, Count: {log_count_after}")

        # Validation: Log files should be deleted after disabling Fusion
        if log_files_exist_after:
            self.log.warning(f"Log files still exist after disabling Fusion. Count: {log_count_after}")
        else:
            self.log.info("Validation passed: Log files deleted successfully after disabling Fusion")

        # Re-enable Fusion
        self.log.info("Re-enabling Fusion")
        self.enable_fusion()

        # Wait for Fusion to stabilize after re-enabling
        self.sleep(60, "Wait for Fusion to stabilize after re-enabling")

        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir, rebalance_count=1)

        extent_migration_threads = []
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_migration_threads.append(th)
                th.start()

        for th in extent_migration_threads:
            th.join()

        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, 100000)
