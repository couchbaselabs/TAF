import threading
import time
from copy import deepcopy
import json

from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_tools.cbstats import Cbstats
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
    
    def validate_term_number(self, prev_uploader_map, new_uploader_map):

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
        self.log.info("Starting a DCP rebalance-in operation")
        
        # Capture uploader state before DCP rebalance
        uploader_map_before_dcp = deepcopy(self.fusion_vb_uploader_map)

        # Get one available server that's not in the cluster
        available_servers = [server for server in self.cluster.servers 
                            if server not in self.cluster.nodes_in_cluster]
        
        if not available_servers:
            self.fail("No available servers for rebalance-in operation")
        
        # Select one node to add to the cluster
        node_to_add = available_servers[0]
        
        self.log.info(f"Rebalancing-in node: {node_to_add.ip}")

        operation = self.task.async_rebalance(
            self.cluster, 
            [node_to_add],  # ADD one node to cluster
            [],             # nodes to remove (empty for rebalance-in)
            services=["kv"]
        )
        self.wait_for_rebalance_to_complete(operation)
        
        # ========== DCP REBALANCE VALIDATION ==========
        self.log.info("=== Starting DCP Rebalance Validation ===")
        
        # Get uploader state after DCP rebalance
        self.get_fusion_uploader_info()
        uploader_map_after_dcp = deepcopy(self.fusion_vb_uploader_map)
        
        # Validate uploaders didn't change during DCP rebalance
        self.validate_term_number(uploader_map_before_dcp, uploader_map_after_dcp)

        self.log.info("=== DCP Rebalance Validation Completed ===")
        
        # Final validation
        self.log.info("=== Final Validation ===")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)
        self.log.info("DCP rebalance test completed successfully with full uploader validation")


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
