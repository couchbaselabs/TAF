import threading
import time
from copy import deepcopy
import json

from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_tools.cbstats import Cbstats
from rebalance_utils.rebalance_util import RebalanceUtil
import os
import subprocess
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_base import MagmaBaseTest
from shell_util.remote_connection import RemoteMachineShellConnection


class FusionDcpRebalance(MagmaBaseTest, FusionBase):
    def setUp(self):
        super(FusionDcpRebalance, self).setUp()
        self.log.info("Setting up Fusion DCP Rebalance test")
        self.manifest_parts = self.input.param("manifest_parts", 20)
        # Ensure rebalance mode is defined, else run_rebalance() will break
        self.num_nodes_to_rebalance_in = self.input.param("num_nodes_to_rebalance_in", 1)
        self.num_nodes_to_rebalance_out = self.input.param("num_nodes_to_rebalance_out", 0)
        self.num_nodes_to_swap_rebalance = self.input.param("num_nodes_to_swap_rebalance", 0)
        self.extent_migration_rate_limit = self.input.param("extent_migration_rate_limit", 10 * 1024 * 1024)  # 10 MB/s default
        self.wait_for_extent_migration = self.input.param("wait_for_extent_migration", True)  # Wait for migration by default
        split_path = self.local_test_path.split("/")
        self.fusion_output_dir = "/" + os.path.join("/".join(split_path[1:4]), "fusion_output")
        self.log.info(f"Fusion output dir = {self.fusion_output_dir}")
        subprocess.run(f"mkdir -p {self.fusion_output_dir}", shell=True, executable="/bin/bash")

        # Initialize uploader management tracking
        self.rebalance_counter = 1
        self.reb_plan_dict = dict()
        self.vb_log_ckpt_dict = dict()
        self.bucket_uuid_map = dict()

    def tearDown(self):
        super(FusionDcpRebalance, self).tearDown()

    
    def wait_for_rebalance_to_complete(self, task, wait_step=120):
        """Wait for rebalance task to complete"""
        self.task.jython_task_manager.get_task_result(task)
        if not task.result:
            self.task.jython_task_manager.abort_all_tasks()
        self.assertTrue(task.result, "Rebalance Failed")

    
    def wait_for_extent_migration_to_start(self, server, bucket, output_dir, duration=3600, interval=10):
        self.log.info("Waiting for extent migration to start")
        
        ssh = RemoteMachineShellConnection(server)
        stat_dict = {}  # Initialize stat_dict


        cbstats_cmd = f"/opt/couchbase/bin/cbstats localhost:11210 all -b {bucket.name} -u Administrator -p password | grep fusion | grep -E 'migrated|migration'"

        self.log.info(f"Monitoring extent migration on server: {server.ip} for bucket: {bucket.name}\n{cbstats_cmd}")

        start_time = time.time()
        end_time = start_time + duration

        while time.time() < end_time:
            o, e = ssh.execute_command(cbstats_cmd)
            for stat in o:
                k = stat.split(":")[0].strip()
                v = int(stat.split(":")[1].strip())
                stat_dict[k] = v
            self.log.debug(f"Extent Migration stats on {server.ip} :{stat_dict}")  
            if stat_dict['ep_fusion_migration_completed_bytes'] > 0:
                self.log.info(f"Extent migration started on server: {server.ip}")
                break
            time.sleep(interval)
        ssh.disconnect()   

    # ============ UPLOADER MANAGEMENT VALIDATION METHODS ============
    
    def validate_stale_log_deletion(self):
        """
        Validate stale log file deletion after rebalance.
        Imported logic from fusion_uploader_management.py
        """
        self.log.info("Validating stale log file deletion")
        
        if not hasattr(self, 'reb_plan_dict') or not self.reb_plan_dict:
            self.log.warning("No rebalance plan data available for stale log validation")
            return
            
        ssh = RemoteMachineShellConnection(self.nfs_server)
        
        try:
            for bucket in self.cluster.buckets:
                if bucket.name not in self.reb_plan_dict:
                    continue
                    
                for vb_no in list(self.reb_plan_dict[bucket.name]):
                    if vb_no not in self.vb_log_ckpt_dict[bucket.name]:
                        continue
                        
                    volume_id = self.vb_log_ckpt_dict[bucket.name][vb_no]["volume_id"]
                    term_num = int(self.vb_log_ckpt_dict[bucket.name][vb_no]["ckpt"].split("-")[1].split(".")[0])
                    ckpt_no = int(self.vb_log_ckpt_dict[bucket.name][vb_no]["ckpt"].split("-")[1].split(".")[1])
                    kvstore_path = os.path.join(self.nfs_server_path, volume_id)
                    
                    ls_cmd = f"ls -ltr {kvstore_path} 2>/dev/null || echo 'Directory not found'"
                    o, e = ssh.execute_command(ls_cmd)
                    
                    if "Directory not found" in str(o):
                        self.log.debug(f"Directory {kvstore_path} not found, skipping validation")
                        continue
                    
                    stale_log_files = list()
                    
                    for line in o[1:]:  # Skip first line (total)
                        line = line.strip()
                        if not line or "total" in line:
                            continue
                        line_arr = line.split(" ")
                        log_name = line_arr[-1]
                        
                        if "log-" not in log_name:
                            continue
                            
                        try:
                            tno = int(log_name.split("-")[1].split(".")[0])
                            cno = int(log_name.split("-")[1].split(".")[1])
                            
                            if tno == term_num and cno > ckpt_no:
                                stale_log_files.append(log_name)
                            elif tno == term_num and cno <= ckpt_no:
                                continue
                            elif tno > term_num:
                                break
                        except (IndexError, ValueError) as ex:
                            self.log.debug(f"Error parsing log file name {log_name}: {ex}")
                    
                    self.log.info(f"Bucket:{bucket.name}, VB:{vb_no}, Stale log files: {len(stale_log_files)}")
                    
        finally:
            ssh.disconnect()
    
    def parse_reb_plan_dict(self, rebalance_counter):
        """
        Parse rebalance plan dictionary from fusion rebalance output.
        Imported logic from fusion_uploader_management.py
        """
        self.log.info(f"Parsing rebalance plan for rebalance #{rebalance_counter}")
        
        self.reb_plan_dict = dict()
        self.vb_log_ckpt_dict = dict()
        self.bucket_uuid_map = dict()
        
        ssh = RemoteMachineShellConnection(self.cluster.master)
        
        try:
            reb_plan_path = os.path.join(self.fusion_scripts_dir, f"reb_plan{rebalance_counter}.json")
            cat_cmd = f"cat {reb_plan_path}"
            o, e = ssh.execute_command(cat_cmd)
            
            if not o or e:
                self.log.warning(f"Could not read rebalance plan file: {reb_plan_path}")
                return
                
            json_str = "\n".join(o)
            data = json.loads(json_str)
            
            # Initialize dictionaries for each bucket
            for bucket in self.cluster.buckets:
                self.reb_plan_dict[bucket.name] = set()
                self.vb_log_ckpt_dict[bucket.name] = dict()
                bucket_uuid = self.get_bucket_uuid(bucket.name)
                self.bucket_uuid_map[bucket_uuid] = bucket.name
            
            # Parse node data
            for node in data['nodes']:
                for kvstore_info in data['nodes'][node]:
                    volume_id = kvstore_info['volumeID']
                    log_ckpt = kvstore_info["logManifestName"]
                    bucket_uuid = volume_id.split('/')[1]
                    
                    if bucket_uuid not in self.bucket_uuid_map:
                        continue
                        
                    bucket_name = self.bucket_uuid_map[bucket_uuid]
                    kvstore_num = int(volume_id.split('/')[2].split('-')[1])
                    
                    self.reb_plan_dict[bucket_name].add(kvstore_num)
                    self.vb_log_ckpt_dict[bucket_name][kvstore_num] = {
                        "ckpt": log_ckpt,
                        "volume_id": volume_id
                    }
            
            self.log.info(f"Rebalance plan parsed - affected vBuckets: {dict(self.reb_plan_dict)}")
            
        except Exception as ex:
            self.log.error(f"Error parsing rebalance plan: {ex}")
        finally:
            ssh.disconnect()

    def test_perform_dcp_rebalance(self):
        self.log.info("Performing DCP rebalance with uploader management validation")
        
        # Initialize vb_term_number_dict like the original
        self.vb_term_number_dict = dict()
        for bucket in self.cluster.buckets:
            self.vb_term_number_dict[bucket.name] = dict()
            for vb_no in range(bucket.numVBuckets):
                self.vb_term_number_dict[bucket.name][vb_no] = 1

        self.log.info(f"VB term number dict = {self.vb_term_number_dict}")
        
        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Get initial uploader state
        self.get_fusion_uploader_info()
        initial_uploader_map = deepcopy(self.fusion_vb_uploader_map)
        self.log.info("Captured initial uploader state")

        #change the extent migration rate limit
        ClusterRestAPI(self.cluster.master).\
            manage_global_memcached_setting(fusion_migration_rate_limit=self.extent_migration_rate_limit)

        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir, 
                                            rebalance_count=self.rebalance_counter,
                                            rebalance_sleep_time=120)

        self.cluster_util.print_cluster_stats(self.cluster)

        extent_migration_array = list()
        if self.wait_for_extent_migration and self.extent_migration_rate_limit > 0:
            self.log.info(f"Extent migration rate limit = {self.extent_migration_rate_limit} bytes/sec")
            self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
            for node in nodes_to_monitor:
                for bucket in self.cluster.buckets:
                    extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                    extent_th.start()
                    extent_migration_array.append(extent_th)

            # Wait for extent migration to complete
            for th in extent_migration_array:
                th.join()
            self.log.info("Extent migration monitoring completed")
        else:
            self.log.info(f"Skipping extent migration monitoring (wait_for_extent_migration={self.wait_for_extent_migration}, extent_migration_rate_limit={self.extent_migration_rate_limit})")
        
        self.parse_reb_plan_dict(self.rebalance_counter)

        # ========== FUSION REBALANCE VALIDATION ==========
        self.log.info("=== Starting Fusion Rebalance Validation ===")
        
        # Get uploader state after fusion rebalance (AFTER extent migration completes)
        self.get_fusion_uploader_info()
        post_fusion_uploader_map = deepcopy(self.fusion_vb_uploader_map)
        
        # Validate fusion rebalance results
        self.validate_term_number(initial_uploader_map, post_fusion_uploader_map)
        
        # Validate stale log deletion
        self.validate_stale_log_deletion()
        
        self.log.info("=== Fusion Rebalance Validation Completed ===")
        self.rebalance_counter += 1

        # ========== DCP REBALANCE SECTION ==========
        # Get DCP rebalance type from conf (in/out/swap)
        dcp_rebalance_type = self.input.param("dcp_rebalance_type", "in")
        self.log.info(f"Starting a DCP rebalance-{dcp_rebalance_type} operation")
        
        # Capture uploader state before DCP rebalance
        uploader_map_before_dcp = deepcopy(self.fusion_vb_uploader_map)

        if dcp_rebalance_type == "in":
            # Get one available server that's not in the cluster
            available_servers = [server for server in self.cluster.servers 
                                if server not in self.cluster.nodes_in_cluster]
            
            if not available_servers:
                self.fail("No available servers for DCP rebalance-in operation")
            
            # Select one node to add to the cluster
            node_to_add = available_servers[0]
            self.log.info(f"DCP rebalancing-in node: {node_to_add.ip}")

            operation = self.task.async_rebalance(
                self.cluster, 
                [node_to_add],  # ADD one node to cluster
                [],             # nodes to remove (empty for rebalance-in)
                services=["kv"]
            )
        
        elif dcp_rebalance_type == "out":
            # CRITICAL: Remove a node that underwent extent migration
            # Select from nodes_to_monitor (nodes involved in Fusion rebalance)
            nodes_with_migration = [node for node in nodes_to_monitor 
                                   if node.ip != self.cluster.master.ip]
            
            if not nodes_with_migration:
                self.fail("No nodes available for DCP rebalance-out (no nodes underwent extent migration)")
            
            node_to_remove = nodes_with_migration[0]
            self.log.info(f"DCP rebalancing-out node that underwent extent migration: {node_to_remove.ip}")
            self.log.info(f"Nodes that underwent extent migration: {[n.ip for n in nodes_to_monitor]}")

            operation = self.task.async_rebalance(
                self.cluster, 
                [],                 # nodes to add (empty for rebalance-out)
                [node_to_remove],   # REMOVE node that underwent extent migration
                services=["kv"]
            )
        
        elif dcp_rebalance_type == "swap":
            # CRITICAL: Swap a node that underwent extent migration
            # Get available server to add
            available_servers = [server for server in self.cluster.servers 
                                if server not in self.cluster.nodes_in_cluster]
            
            # Get node to remove from those that underwent extent migration
            nodes_with_migration = [node for node in nodes_to_monitor 
                                   if node.ip != self.cluster.master.ip]
            
            if not available_servers:
                self.fail("No available servers for DCP swap rebalance")
            if not nodes_with_migration:
                self.fail("No nodes available to swap out (no nodes underwent extent migration)")
            
            node_to_add = available_servers[0]
            node_to_remove = nodes_with_migration[0]
            self.log.info(f"DCP swap rebalance: adding {node_to_add.ip}, removing {node_to_remove.ip} (node that underwent extent migration)")
            self.log.info(f"Nodes that underwent extent migration: {[n.ip for n in nodes_to_monitor]}")

            operation = self.task.async_rebalance(
                self.cluster, 
                [node_to_add],      # ADD new node
                [node_to_remove],   # REMOVE node that underwent extent migration
                services=["kv"]
            )
        
        else:
            self.fail(f"Invalid dcp_rebalance_type: {dcp_rebalance_type}. Use 'in', 'out', or 'swap'")
        
        self.wait_for_rebalance_to_complete(operation)
        
        # ========== DCP REBALANCE VALIDATION ==========
        self.log.info(f"=== Starting DCP Rebalance-{dcp_rebalance_type.upper()} Validation ===")
        
        # Get uploader state after DCP rebalance
        self.get_fusion_uploader_info()
        uploader_map_after_dcp = deepcopy(self.fusion_vb_uploader_map)
        
        # Validate uploaders didn't change during DCP rebalance
        self.validate_term_number(uploader_map_before_dcp, uploader_map_after_dcp)

        self.log.info(f"=== DCP Rebalance-{dcp_rebalance_type.upper()} Validation Completed ===")
        
        # Final validation
        self.log.info("=== Final Validation ===")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)
        self.log.info(f"DCP rebalance-{dcp_rebalance_type} test completed successfully with full uploader validation")

    def test_verify_migration_blocked_during_dcp_catchup(self):
        """
        Verify extent migration is blocked during DCP catch-up phase.
        Migration should start only after rebalance reaches 100%.
        """
        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")
        
        ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
            fusion_migration_rate_limit=self.extent_migration_rate_limit)
        
        self.log.info("Starting Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(
            output_dir=self.fusion_output_dir,
            rebalance_count=1,
            rebalance_sleep_time=120,
            wait_for_rebalance_to_complete=False)
        
        rebalance_timestamp = {'time': None}
        migration_timestamp = {'time': None}
        
        rebalance_thread = threading.Thread(
            target=self.wait_for_rebalance_completion,
            args=(rebalance_timestamp,))
        
        migration_thread = threading.Thread(
            target=self.wait_for_migration_start,
            args=(nodes_to_monitor, migration_timestamp, rebalance_timestamp))
        
        rebalance_thread.start()
        migration_thread.start()
        
        rebalance_thread.join()
        migration_thread.join()
        
        self.assertIsNotNone(rebalance_timestamp['time'], 
                            "Failed to detect rebalance 100% completion")
        self.assertIsNotNone(migration_timestamp['time'], 
                            "Migration never started after rebalance")
        
        time_diff = migration_timestamp['time'] - rebalance_timestamp['time']
        self.log.info(f"Time between rebalance 100% and migration start: {time_diff:.2f} seconds")
        self.assertGreater(time_diff, 0, 
                          f"Migration must start AFTER 100% completion (diff={time_diff}s)")
        
        self.log.info("Validation passed: Migration correctly blocked during DCP catch-up")
        
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)
        self.log.info("Test completed successfully")

    def wait_for_rebalance_completion(self, timestamp_dict, interval=5):
        """
        Wait for rebalance to reach 100% and capture timestamp.
        """
        rebalance_util = RebalanceUtil(self.cluster)
        max_wait_time = 3600
        start_time = time.time()
        
        self.log.info("Monitoring rebalance progress")
        
        while time.time() - start_time < max_wait_time:
            try:
                status, progress = rebalance_util._rebalance_status_and_progress()
                
                if progress is not None and progress >= 100:
                    timestamp_dict['time'] = time.time()
                    self.log.info(f"Rebalance reached 100% at timestamp: {timestamp_dict['time']}")
                    return
                
                if status == 'none':
                    timestamp_dict['time'] = time.time()
                    self.log.info(f"Rebalance completed at timestamp: {timestamp_dict['time']}")
                    return
                    
            except Exception as e:
                self.log.debug(f"Error checking rebalance: {e}")
            
            time.sleep(interval)
        
        self.log.error("Rebalance did not complete within timeout")

    def wait_for_migration_start(self, nodes_to_monitor, timestamp_dict, rebalance_timestamp_dict, interval=5):
        """
        Wait for migration bytes to become non-zero and capture timestamp.
        Also validates migration doesn't start before rebalance completion.
        """
        max_wait_time = 3600
        start_time = time.time()
        
        self.log.info("Monitoring migration stats")
        
        while time.time() - start_time < max_wait_time:
            for node in nodes_to_monitor:
                for bucket in self.cluster.buckets:
                    try:
                        cbstats = Cbstats(node)
                        stats = cbstats.all_stats(bucket.name)
                        migration_completed_bytes = int(stats['ep_fusion_migration_completed_bytes'])
                        migration_bytes = int(stats['ep_fusion_bytes_migrated'])
                        cbstats.disconnect()
                        
                        if migration_completed_bytes > 0 or migration_bytes > 0:
                            timestamp_dict['time'] = time.time()
                            self.log.info(f"Migration started on {node.ip}:{bucket.name} "
                                        f"at timestamp: {timestamp_dict['time']}, "
                                        f"completed_bytes={migration_completed_bytes}, "
                                        f"bytes_migrated={migration_bytes}")
                            
                            if rebalance_timestamp_dict['time'] is None:
                                self.log.error(f"VIOLATION: Migration started BEFORE rebalance 100% completion!")
                            
                            return
                    except Exception as e:
                        self.log.debug(f"Error checking migration stats: {e}")
            
            time.sleep(interval)
        
        self.log.error("Migration did not start within timeout")


    def test_perform_dcp_and_fusion_rebalance_alternate(self):
        self.log.info("Test: Perform DCP and Fusion rebalance Alternately with comprehensive validation")
        
        # Initialize vb_term_number_dict like the original
        self.vb_term_number_dict = dict()
        for bucket in self.cluster.buckets:
            self.vb_term_number_dict[bucket.name] = dict()
            for vb_no in range(bucket.numVBuckets):
                self.vb_term_number_dict[bucket.name][vb_no] = 1

        self.log.info(f"VB term number dict = {self.vb_term_number_dict}")
        
        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        #change the extent migration rate limit
        ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
            fusion_migration_rate_limit=self.extent_migration_rate_limit
        )

        # Define the rebalance sequence: (rebalance_type, operation)
        # Scale UP: 1->2->3->4 nodes alternating Fusion->DCP->Fusion
        # Scale DOWN: 4->3->2->1 nodes alternating Fusion->DCP->Fusion
        rebalance_sequence = [
            # Scale UP phase
            ("fusion", "in"),    # 1->2 nodes (Fusion)
            ("dcp", "in"),       # 2->3 nodes (DCP)
            ("fusion", "in"),    # 3->4 nodes (Fusion)
            # Scale DOWN phase
            ("fusion", "out"),   # 4->3 nodes (Fusion)
            ("dcp", "out"),      # 3->2 nodes (DCP)
            ("fusion", "out"),   # 2->1 nodes (Fusion)
        ]

        # Execute rebalance sequence with comprehensive validation
        for i, (rebalance_type, operation) in enumerate(rebalance_sequence, 1):
            current_nodes = len(self.cluster.nodes_in_cluster)
            self.log.info(f"Step {i}: {rebalance_type.upper()} rebalance-{operation} "
                         f"(Current nodes: {current_nodes})")

            # Capture uploader state before this rebalance
            self.get_fusion_uploader_info()
            uploader_map_before = deepcopy(self.fusion_vb_uploader_map)
            self.log.info("uploader_map_before : " + str(uploader_map_before))

            # Set rebalance parameters
            self.num_nodes_to_rebalance_in = 1 if operation == "in" else 0
            self.num_nodes_to_rebalance_out = 1 if operation == "out" else 0
            self.num_nodes_to_swap_rebalance = 0

            if rebalance_type == "fusion":
                # Use Fusion rebalance
                self.log.info("Running a Fusion rebalance")
                nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir, 
                                                    rebalance_count=self.rebalance_counter,
                                                    rebalance_sleep_time=120)
                self.sleep(60, "Sleep after Fusion rebalance")

                self.cluster_util.print_cluster_stats(self.cluster)

                extent_migration_array = list()
                self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
                for node in nodes_to_monitor:
                    for bucket in self.cluster.buckets:
                        extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                        extent_th.start()
                        extent_migration_array.append(extent_th)

                # Wait for extent migration to complete
                for th in extent_migration_array:
                    th.join()
                
                self.parse_reb_plan_dict(self.rebalance_counter)
                
                # ========== FUSION REBALANCE VALIDATION ==========
                self.log.info(f"=== Fusion Rebalance #{self.rebalance_counter} Validation ===")
                
                # Get uploader state after fusion rebalance (AFTER extent migration completes)
                self.get_fusion_uploader_info()
                uploader_map_after = deepcopy(self.fusion_vb_uploader_map)
                self.log.info("uploader_map_after : " + str(uploader_map_after))
                
                # Validate fusion rebalance results
                self.validate_term_number(uploader_map_before, uploader_map_after)
                
                # Validate stale log deletion
                self.validate_stale_log_deletion()
                
                self.log.info(f"=== Fusion Rebalance #{self.rebalance_counter} Validation Completed ===")
                self.rebalance_counter += 1

            else:  # DCP rebalance
                self.log.info("Starting a DCP rebalance operation")
                
                # Capture uploader state before DCP rebalance
                self.get_fusion_uploader_info()
                uploader_map_before_dcp = deepcopy(self.fusion_vb_uploader_map)
                
                if operation == "in":
                    # Get available server for rebalance-in
                    available_servers = [s for s in self.cluster.servers
                                       if s not in self.cluster.nodes_in_cluster]
                    if not available_servers:
                        self.fail("No available servers for DCP rebalance-in")

                    node_to_add = available_servers[0]
                    self.log.info(f"DCP rebalancing-in node: {node_to_add.ip}")

                    rebalance_task = self.task.async_rebalance(
                        self.cluster, [node_to_add], [], services=["kv"])

                else:  # rebalance-out
                    # Get non-master node for rebalance-out
                    nodes_to_remove = [n for n in self.cluster.nodes_in_cluster
                                     if n.ip != self.cluster.master.ip]
                    if not nodes_to_remove:
                        self.fail("No non-master nodes for DCP rebalance-out")

                    node_to_remove = nodes_to_remove[0]
                    self.log.info(f"DCP rebalancing-out node: {node_to_remove.ip}")

                    rebalance_task = self.task.async_rebalance(
                        self.cluster, [], [node_to_remove], services=["kv"])

                # Wait for DCP rebalance to complete
                self.wait_for_rebalance_to_complete(rebalance_task)
                
                # ========== DCP REBALANCE VALIDATION ==========
                self.log.info(f"=== DCP Rebalance Step {i} Validation ===")
                
                # IMPORTANT: Refresh spare nodes list after DCP rebalance
                # DCP rebalance updates cluster.nodes_in_cluster but doesn't update fusion's spare_nodes
                # This prevents duplicate nodes in subsequent fusion rebalances
                self.spare_nodes = [node for node in self.cluster.servers 
                                   if node not in self.cluster.nodes_in_cluster]
                self.log.info(f"Refreshed spare nodes after DCP rebalance: {self.spare_nodes}")
                
                # Get uploader state after DCP rebalance
                self.get_fusion_uploader_info()
                uploader_map_after_dcp = deepcopy(self.fusion_vb_uploader_map)

                self.validate_term_number(uploader_map_before_dcp, uploader_map_after_dcp)

                self.log.info(f"=== DCP Rebalance Step {i} Validation Completed ===")

            # Print cluster stats after each rebalance
            self.cluster_util.print_cluster_stats(self.cluster)

            # Check if we've reached max nodes and need to switch to rebalance-out
            if len(self.cluster.nodes_in_cluster) == len(self.cluster.servers):
                self.log.info("Reached maximum nodes, switching to rebalance-out for remaining operations")
        
        
        # Data validation
        self.log.info("Validating item count after all rebalances")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)
        
        self.log.info("="*80)
        self.log.info("Alternating Fusion/DCP lifecycle rebalance completed successfully with full validation")

    def test_enable_fusion_during_dcp_rebalance(self):
        self.log.info("Test: Enable Fusion during DCP rebalance")
        
        # Get DCP rebalance type from conf (in/out/swap)
        dcp_rebalance_type = self.input.param("dcp_rebalance_type", "in")
        
        # Initial data load with Fusion disabled
        self.log.info("Starting initial load")
        self.initial_load()
        self.sleep(30, "Wait after initial load")
        initial_item_count = self.num_items
        
        # Prepare nodes for DCP rebalance based on type
        nodes_in = []
        nodes_out = []
        
        if dcp_rebalance_type == "in":
            available_servers = [s for s in self.cluster.servers 
                                if s not in self.cluster.nodes_in_cluster]
            if not available_servers:
                self.fail("No available servers for DCP rebalance-in")
            nodes_in = [available_servers[0]]
            self.log.info(f"Starting DCP rebalance-in, adding node: {nodes_in[0].ip}")
            
        elif dcp_rebalance_type == "out":
            nodes_to_remove = [n for n in self.cluster.nodes_in_cluster
                              if n.ip != self.cluster.master.ip]
            if not nodes_to_remove:
                self.fail("No non-master nodes for DCP rebalance-out")
            nodes_out = [nodes_to_remove[0]]
            self.log.info(f"Starting DCP rebalance-out, removing node: {nodes_out[0].ip}")
            
        elif dcp_rebalance_type == "swap":
            available_servers = [s for s in self.cluster.servers 
                                if s not in self.cluster.nodes_in_cluster]
            nodes_to_remove = [n for n in self.cluster.nodes_in_cluster
                              if n.ip != self.cluster.master.ip]
            if not available_servers or not nodes_to_remove:
                self.fail("No servers available for DCP swap rebalance")
            nodes_in = [available_servers[0]]
            nodes_out = [nodes_to_remove[0]]
            self.log.info(f"Starting DCP swap rebalance, adding {nodes_in[0].ip}, removing {nodes_out[0].ip}")
        else:
            self.fail(f"Invalid dcp_rebalance_type: {dcp_rebalance_type}. Use 'in', 'out', or 'swap'")
        
        # Start DCP rebalance
        rebalance_task = self.task.async_rebalance(
            self.cluster, nodes_in, nodes_out, 
            check_vbucket_shuffling=False,
            retry_get_process_num=self.retry_get_process_num
        )
        
        self.sleep(10, "Wait for rebalance to be in progress")
        
        # Enable Fusion mid-rebalance
        self.log.info("Enabling Fusion during DCP rebalance")
        self.configure_fusion()
        self.enable_fusion()
        
        # Wait for DCP rebalance to complete
        self.task.jython_task_manager.get_task_result(rebalance_task)
        self.assertTrue(rebalance_task.result, 
                       "DCP rebalance failed after enabling Fusion")
        
        sync_settle_time = 30 + self.fusion_upload_interval + 30
        self.sleep(sync_settle_time, "Wait for Fusion sync to settle")
        
        # Validate uploader assignment
        self.log.info("Validating Fusion uploader assignment")
        self.get_fusion_uploader_info()
        
        for bucket in self.cluster.buckets:
            undefined_count = self.fusion_uploader_dict[bucket.name].get(None, 0)
            self.assertEqual(undefined_count, 0, 
                           f"Found {undefined_count} vBuckets with undefined uploaders")
            
            total_uploaders = sum(self.fusion_uploader_dict[bucket.name].values())
            self.assertEqual(total_uploaders, bucket.numVBuckets,
                           f"Expected {bucket.numVBuckets} uploaders, got {total_uploaders}")
        
        self.log.info("All uploaders assigned properly")
        
        # Check log files on NFS
        log_files_exist, log_count = self.check_log_files_on_nfs(
            self.nfs_server, self.nfs_server_path
        )
        self.assertTrue(log_files_exist, "No log files found on NFS")
        self.log.info(f"Found {log_count} log files on NFS")
        
        # Check Fusion sync stats
        for node in self.cluster.nodes_in_cluster:
            for bucket in self.cluster.buckets:
                cbstats = Cbstats(node)
                stats = cbstats.all_stats(bucket.name)
                
                sync_failures = int(stats.get('ep_fusion_sync_failures', 0))
                migration_failures = int(stats.get('ep_fusion_migration_failures', 0))
                read_failures = int(stats.get('ep_data_read_failed', 0))
                
                self.assertEqual(sync_failures, 0,
                               f"Sync failures on {node.ip}:{bucket.name}")
                self.assertEqual(migration_failures, 0,
                               f"Migration failures on {node.ip}:{bucket.name}")
                self.assertEqual(read_failures, 0,
                               f"Read failures on {node.ip}:{bucket.name}")
                
                cbstats.disconnect()
        
        self.log.info("No sync/migration/read failures")
        
        # Verify data consistency
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, initial_item_count)
        
        # DCP rebalance updates cluster.nodes_in_cluster but doesn't update fusion's spare_nodes
        self.spare_nodes = [node for node in self.cluster.servers 
                           if node not in self.cluster.nodes_in_cluster]
        self.log.info(f"Refreshed spare nodes after DCP rebalance: {self.spare_nodes}")
        
        # Perform Fusion rebalance
        self.log.info("Performing Fusion rebalance")
        self.get_fusion_uploader_info()
        uploader_map_before_fusion = deepcopy(self.fusion_vb_uploader_map)
        
        ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
            fusion_migration_rate_limit=self.extent_migration_rate_limit
        )
        
        nodes_to_monitor = self.run_rebalance(
            output_dir=self.fusion_output_dir,
            rebalance_count=1,
            rebalance_sleep_time=120
        )
        
        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(
                    target=self.monitor_extent_migration, 
                    args=[node, bucket]
                )
                extent_th.start()
                extent_migration_array.append(extent_th)
        
        for th in extent_migration_array:
            th.join()
        
        # Validate uploader changes after Fusion rebalance
        self.get_fusion_uploader_info()
        uploader_map_after_fusion = deepcopy(self.fusion_vb_uploader_map)
        self.validate_term_number(uploader_map_before_fusion, uploader_map_after_fusion)
        
        # Check final sync stats
        for node in self.cluster.nodes_in_cluster:
            for bucket in self.cluster.buckets:
                cbstats = Cbstats(node)
                stats = cbstats.all_stats(bucket.name)
                
                sync_failures = int(stats.get('ep_fusion_sync_failures', 0))
                migration_failures = int(stats.get('ep_fusion_migration_failures', 0))
                read_failures = int(stats.get('ep_data_read_failed', 0))
                
                self.log.info(f"Node {node.ip}, Bucket {bucket.name}: "
                            f"sync_failures={sync_failures}, "
                            f"migration_failures={migration_failures}, "
                            f"read_failures={read_failures}")
                
                self.assertEqual(sync_failures, 0)
                self.assertEqual(migration_failures, 0)
                self.assertEqual(read_failures, 0)
                
                cbstats.disconnect()
        
        # Final data validation
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, initial_item_count)
        self.log.info("Enable Fusion during DCP rebalance test completed")

    def test_trigger_dcp_rebalance_during_enable_fusion(self):
        self.log.info("Test: Enable Fusion during DCP rebalance")

        # Get DCP rebalance type from conf (in/out/swap)
        dcp_rebalance_type = self.input.param("dcp_rebalance_type", "in")

        # Initial data load with Fusion disabled
        self.log.info("Starting initial load")
        self.initial_load()
        self.sleep(30, "Wait after initial load")
        initial_item_count = self.num_items

        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Enable Fusion in parallel while DCP rebalance is running
        self.log.info("Enabling Fusion during DCP rebalance")

        def enable_fusion_thread():
            self.configure_fusion()
            self.enable_fusion()

        fusion_thread = threading.Thread(target=enable_fusion_thread)
        fusion_thread.start()
        self.log.info("Fusion enabling thread started")

        # Prepare nodes for DCP rebalance based on type
        nodes_in = []
        nodes_out = []

        if dcp_rebalance_type == "in":
            available_servers = [s for s in self.cluster.servers
                                 if s not in self.cluster.nodes_in_cluster]
            if not available_servers:
                self.fail("No available servers for DCP rebalance-in")
            nodes_in = [available_servers[0]]
            self.log.info(f"Starting DCP rebalance-in, adding node: {nodes_in[0].ip}")

        elif dcp_rebalance_type == "out":
            nodes_to_remove = [n for n in self.cluster.nodes_in_cluster
                               if n.ip != self.cluster.master.ip]
            if not nodes_to_remove:
                self.fail("No non-master nodes for DCP rebalance-out")
            nodes_out = [nodes_to_remove[0]]
            self.log.info(f"Starting DCP rebalance-out, removing node: {nodes_out[0].ip}")

        elif dcp_rebalance_type == "swap":
            available_servers = [s for s in self.cluster.servers
                                 if s not in self.cluster.nodes_in_cluster]
            nodes_to_remove = [n for n in self.cluster.nodes_in_cluster
                               if n.ip != self.cluster.master.ip]
            if not available_servers or not nodes_to_remove:
                self.fail("No servers available for DCP swap rebalance")
            nodes_in = [available_servers[0]]
            nodes_out = [nodes_to_remove[0]]
            self.log.info(f"Starting DCP swap rebalance, adding {nodes_in[0].ip}, removing {nodes_out[0].ip}")
        else:
            self.fail(f"Invalid dcp_rebalance_type: {dcp_rebalance_type}. Use 'in', 'out', or 'swap'")

        # Start DCP rebalance
        rebalance_task = self.task.async_rebalance(
            self.cluster, nodes_in, nodes_out,
            check_vbucket_shuffling=False,
            retry_get_process_num=self.retry_get_process_num
        )

        # Enable Fusion in parallel while DCP rebalance is running
        self.log.info("Enabling Fusion during DCP rebalance")
        def enable_fusion_thread():
            self.configure_fusion()
            self.enable_fusion()
        
        fusion_thread = threading.Thread(target=enable_fusion_thread)
        fusion_thread.start()
        self.log.info("Fusion enabling thread started")

        # Wait for DCP rebalance to complete
        self.task.jython_task_manager.get_task_result(rebalance_task)
        self.assertTrue(rebalance_task.result,
                        "DCP rebalance failed after enabling Fusion")

        # Wait for Fusion enabling thread to complete
        self.log.info("Waiting for Fusion enabling thread to complete")
        fusion_thread.join()
        self.log.info("Fusion enabling completed")

        sync_settle_time = 30 + self.fusion_upload_interval + 30
        self.sleep(sync_settle_time, "Wait for Fusion sync to settle")

        # Validate uploader assignment
        self.log.info("Validating Fusion uploader assignment")
        self.get_fusion_uploader_info()

        for bucket in self.cluster.buckets:
            undefined_count = self.fusion_uploader_dict[bucket.name].get(None, 0)
            self.assertEqual(undefined_count, 0,
                             f"Found {undefined_count} vBuckets with undefined uploaders")

            total_uploaders = sum(self.fusion_uploader_dict[bucket.name].values())
            self.assertEqual(total_uploaders, bucket.numVBuckets,
                             f"Expected {bucket.numVBuckets} uploaders, got {total_uploaders}")

        self.log.info("All uploaders assigned properly")

        # Check log files on NFS
        log_files_exist, log_count = self.check_log_files_on_nfs(
            self.nfs_server, self.nfs_server_path
        )
        self.assertTrue(log_files_exist, "No log files found on NFS")
        self.log.info(f"Found {log_count} log files on NFS")

        # Check Fusion sync stats
        for node in self.cluster.nodes_in_cluster:
            for bucket in self.cluster.buckets:
                cbstats = Cbstats(node)
                stats = cbstats.all_stats(bucket.name)

                sync_failures = int(stats.get('ep_fusion_sync_failures', 0))
                migration_failures = int(stats.get('ep_fusion_migration_failures', 0))
                read_failures = int(stats.get('ep_data_read_failed', 0))

                self.assertEqual(sync_failures, 0,
                                 f"Sync failures on {node.ip}:{bucket.name}")
                self.assertEqual(migration_failures, 0,
                                 f"Migration failures on {node.ip}:{bucket.name}")
                self.assertEqual(read_failures, 0,
                                 f"Read failures on {node.ip}:{bucket.name}")

                cbstats.disconnect()

        self.log.info("No sync/migration/read failures")

        # Verify data consistency
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, initial_item_count)

        # DCP rebalance updates cluster.nodes_in_cluster but doesn't update fusion's spare_nodes
        self.spare_nodes = [node for node in self.cluster.servers 
                           if node not in self.cluster.nodes_in_cluster]
        self.log.info(f"Refreshed spare nodes after DCP rebalance: {self.spare_nodes}")

        # Perform Fusion rebalance
        self.log.info("Performing Fusion rebalance")
        self.get_fusion_uploader_info()
        uploader_map_before_fusion = deepcopy(self.fusion_vb_uploader_map)

        ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
            fusion_migration_rate_limit=self.extent_migration_rate_limit
        )

        nodes_to_monitor = self.run_rebalance(
            output_dir=self.fusion_output_dir,
            rebalance_count=1,
            rebalance_sleep_time=120
        )

        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(
                    target=self.monitor_extent_migration,
                    args=[node, bucket]
                )
                extent_th.start()
                extent_migration_array.append(extent_th)

        for th in extent_migration_array:
            th.join()

        # Validate uploader changes after Fusion rebalance
        self.get_fusion_uploader_info()
        uploader_map_after_fusion = deepcopy(self.fusion_vb_uploader_map)
        self.validate_term_number(uploader_map_before_fusion, uploader_map_after_fusion)

        # Check final sync stats
        for node in self.cluster.nodes_in_cluster:
            for bucket in self.cluster.buckets:
                cbstats = Cbstats(node)
                stats = cbstats.all_stats(bucket.name)

                sync_failures = int(stats.get('ep_fusion_sync_failures', 0))
                migration_failures = int(stats.get('ep_fusion_migration_failures', 0))
                read_failures = int(stats.get('ep_data_read_failed', 0))

                self.log.info(f"Node {node.ip}, Bucket {bucket.name}: "
                              f"sync_failures={sync_failures}, "
                              f"migration_failures={migration_failures}, "
                              f"read_failures={read_failures}")

                self.assertEqual(sync_failures, 0)
                self.assertEqual(migration_failures, 0)
                self.assertEqual(read_failures, 0)

                cbstats.disconnect()

        # Final data validation
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, initial_item_count)
        self.log.info("Enable Fusion during DCP rebalance test completed")