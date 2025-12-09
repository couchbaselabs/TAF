import os
import random
import subprocess
import threading
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_server_rest_util.fusion.fusion_api import FusionRestAPI
from cb_tools.cbstats import Cbstats
from shell_util.remote_connection import RemoteMachineShellConnection
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_base import MagmaBaseTest


class FusionMigration(MagmaBaseTest, FusionBase):
    def setUp(self):
        super(FusionMigration, self).setUp()

        self.log.info("FusionMigration setUp Started")
        self.rate_limit_toggle_stop = False

    def tearDown(self):
        self.rate_limit_toggle_stop = True
        super(FusionMigration, self).tearDown()

    def select_rebalance_type(self, rebalance_type):
        """Select and configure rebalance type"""
        self.num_nodes_to_rebalance_in = 0
        self.num_nodes_to_rebalance_out = 0
        self.num_nodes_to_swap_rebalance = 0

        if rebalance_type == "random":
            available_types = []
            if len(self.cluster.servers) > len(self.cluster.nodes_in_cluster):
                available_types.append("in")
            if len(self.cluster.nodes_in_cluster) > 1:
                available_types.append("out")
            if len(self.cluster.servers) > len(self.cluster.nodes_in_cluster) and len(self.cluster.nodes_in_cluster) > 1:
                available_types.append("swap")
            
            if not available_types:
                self.fail("No valid rebalance types available")
            
            rebalance_type = random.choice(available_types)
            self.log.info(f"Randomly selected rebalance type: {rebalance_type}")

        if rebalance_type == "in":
            self.num_nodes_to_rebalance_in = 1
        elif rebalance_type == "out":
            self.num_nodes_to_rebalance_out = 1
        elif rebalance_type == "swap":
            self.num_nodes_to_swap_rebalance = 1
        else:
            self.fail(f"Invalid rebalance_type: {rebalance_type}. Use 'in', 'out', 'swap', or 'random'")

        return rebalance_type

    def toggle_rate_limit(self, interval, rate_limit_value, rebalance_count):
        """Toggle migration rate limit between X and 0"""
        toggle_count = 0
        while not self.rate_limit_toggle_stop:
            current_limit = rate_limit_value if toggle_count % 2 == 0 else 0
            self.log.info(f"[Rebalance {rebalance_count}] Toggle {toggle_count + 1}: Setting migration rate limit to {current_limit}")
            ClusterRestAPI(self.cluster.master).\
                manage_global_memcached_setting(fusion_migration_rate_limit=current_limit)
            toggle_count += 1
            self.sleep(interval, f"[Rebalance {rebalance_count}] Wait before next rate limit toggle")

    def toggle_guest_volume_permissions(self, interval, nodes_to_monitor, rebalance_count):
        """Toggle guest volume permissions between read-only and no-access"""
        toggle_count = 0
        guest_storage_base = os.path.join(os.path.dirname(self.nfs_server_path), "guest_storage")
        
        while not self.rate_limit_toggle_stop:
            allow_read = toggle_count % 2 == 0
            permission = "755" if allow_read else "000"
            action = "Allow read" if allow_read else "Block read"
            
            self.log.info(f"[Rebalance {rebalance_count}] Toggle {toggle_count + 1}: {action} on guest volumes (chmod {permission})")
            
            ssh = RemoteMachineShellConnection(self.nfs_server)
            try:
                for node in nodes_to_monitor:
                    node_id = "ns_1@{0}".format(node.ip)
                    
                    # Find guest directory for this node (subdirectory structure: node_id/reb*)
                    find_cmd = f"ls -d {guest_storage_base}/{node_id}/reb* 2>/dev/null | tail -1"
                    output, error = ssh.execute_command(find_cmd)
                    
                    if not output or not output[0].strip():
                        self.log.warning(f"[Rebalance {rebalance_count}] No guest directory found for {node_id}")
                        continue
                    
                    node_guest_dir = output[0].strip()
                    self.log.debug(f"[Rebalance {rebalance_count}] Using guest directory: {node_guest_dir}")
                    
                    chmod_cmd = "chmod -R {0} {1}".format(permission, node_guest_dir)
                    output, error = ssh.execute_command(chmod_cmd)
                    
                    if error:
                        self.log.warning(f"[Rebalance {rebalance_count}] Error changing permissions on {node_guest_dir}: {error}")
            finally:
                ssh.disconnect()
            
            toggle_count += 1
            self.sleep(interval, f"[Rebalance {rebalance_count}] Wait before next permission toggle")


    def test_fusion_extent_migration(self):

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir)

        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        for th in extent_migration_array:
            th.join()


    def test_crud_during_extent_migration(self):

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Set Migration Rate Limit to 0 so that extent migration doesn't take place
        ClusterRestAPI(self.cluster.master).\
            manage_global_memcached_setting(fusion_migration_rate_limit=0)

        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir)

        # Perform reads when no extent migration has taken place yet
        self.perform_batch_reads()

        # Update Migration Rate Limit so that extent migration process starts
        ClusterRestAPI(self.cluster.master).\
                manage_global_memcached_setting(fusion_migration_rate_limit=self.fusion_migration_rate_limit)

        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        for th in extent_migration_array:
            th.join()

        # Deleting guest volumes after extent migration
        self.log_store_rebalance_cleanup()

        self.log.info("Performing a read workload post extent migration")
        self.perform_batch_reads()


    def test_monitor_active_guest_volumes(self):

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir)

        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        active_guest_volumes_th = threading.Thread(target=self.monitor_active_guest_volumes, args=[nodes_to_monitor, 120])
        active_guest_volumes_th.start()

        for th in extent_migration_array:
            th.join()
        active_guest_volumes_th.join()


    def test_rebalance_in_during_migration(self):

        self.rebalance_in_method = self.input.param("rebalance_in_method", "fusion") # ["fusion", "dcp"]
        self.rebalance_in_nodes_during_migration = self.input.param("rebalance_in_nodes_during_migration", 1)
        self.rebalance_migration_rate_limit = self.input.param("rebalance_migration_rate_limit", 1048576)

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Set Migration Rate Limit to 0 so that extent migration doesn't take place
        ClusterRestAPI(self.cluster.master).\
            manage_global_memcached_setting(fusion_migration_rate_limit=self.rebalance_migration_rate_limit)

        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir)
        self.sleep(15, "Wait after rebalance")

        self.cluster_util.print_cluster_stats(self.cluster)

        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        self.sleep(10, "Wait before performing rebalance-in during migration")
        nodes_to_monitor2 = list()
        if self.rebalance_in_method == "dcp":
            nodes_to_rebalance_in = self.cluster.servers[len(self.cluster.nodes_in_cluster):
                                        len(self.cluster.nodes_in_cluster)+self.rebalance_in_nodes_during_migration]

            self.log.info(f"Rebalancing-in {nodes_to_rebalance_in} during extent migration")
            result = self.task.rebalance(self.cluster, nodes_to_rebalance_in,
                                         [], services=["kv"]*self.rebalance_in_nodes_during_migration)
            self.assertTrue(result, "Rebalance-in during migration failed")
            self.cluster.nodes_in_cluster.extend(self.cluster.servers[
                len(self.cluster.nodes_in_cluster):len(self.cluster.nodes_in_cluster)+self.rebalance_in_nodes_during_migration])

        elif self.rebalance_in_method == "fusion":
            self.num_nodes_to_rebalance_in = self.rebalance_in_nodes_during_migration
            self.num_nodes_to_rebalance_out = 0
            self.num_nodes_to_swap_rebalance = 0
            self.log.info("Running a Fusion rebalance")
            nodes_to_monitor2 = self.run_rebalance(output_dir=self.fusion_output_dir,
                                                   rebalance_count=2,
                                                   rebalance_sleep_time=15)
            self.sleep(30, "Wait after rebalance-in during migration")

        self.cluster_util.print_cluster_stats(self.cluster)

        # Update Migration Rate Limit so that extent migration process starts
        ClusterRestAPI(self.cluster.master).\
                manage_global_memcached_setting(fusion_migration_rate_limit=self.fusion_migration_rate_limit)

        extent_migration_array2 = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor2}")
        for node in nodes_to_monitor2:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array2.append(extent_th)

        for th in extent_migration_array:
            th.join()

        for th in extent_migration_array2:
            th.join()


    def test_rebalance_out_during_migration(self):

        self.rebalance_out_method = self.input.param("rebalance_out_method", "fusion") # ["fusion", "dcp"]
        self.rebalance_out_nodes_during_migration = self.input.param("rebalance_out_nodes_during_migration", 1)

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Set Migration Rate Limit to 0 so that extent migration doesn't take place
        ClusterRestAPI(self.cluster.master).\
            manage_global_memcached_setting(fusion_migration_rate_limit=0)

        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir)
        self.sleep(30, "Wait after rebalance")

        self.cluster_util.print_cluster_stats(self.cluster)

        self.sleep(10, "Wait before performing rebalance-out during migration")
        if self.rebalance_out_method == "dcp":
            nodes_to_rebalance_out = self.cluster.nodes_in_cluster[-self.rebalance_out_nodes_during_migration:]

            self.log.info(f"Rebalancing-out {nodes_to_rebalance_out} during extent migration")
            result = self.task.rebalance(self.cluster, [],
                                         nodes_to_rebalance_out, services=["kv"])
            self.assertFalse(result, "Rebalance-in during migration failed")

        elif self.rebalance_out_method == "fusion":
            self.num_nodes_to_rebalance_in = 0
            self.num_nodes_to_rebalance_out = self.rebalance_out_nodes_during_migration
            self.num_nodes_to_swap_rebalance = 0
            self.log.info("Running a Fusion rebalance")
            nodes_to_monitor2 = self.run_rebalance(output_dir=self.fusion_output_dir,
                                                   rebalance_count=2)
            self.sleep(30, "Wait after rebalance-out during migration")

        self.cluster_util.print_cluster_stats(self.cluster)

        # Update Migration Rate Limit so that extent migration process starts
        ClusterRestAPI(self.cluster.master).\
            manage_global_memcached_setting(fusion_migration_rate_limit=self.fusion_migration_rate_limit)

        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        for th in extent_migration_array:
            th.join()

    def test_swap_rebalance_during_extent_migration(self):

        self.swap_rebalance_method = self.input.param("swap_rebalance_method", "fusion") # ["fusion", "dcp"]
        self.swap_rebalance_nodes_during_migration = self.input.param("swap_rebalance_nodes_during_migration", 1)
        self.rebalance_migration_rate_limit = self.input.param("rebalance_migration_rate_limit", 1048576)

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Set Migration Rate Limit to 0 so that extent migration doesn't take place
        ClusterRestAPI(self.cluster.master).\
            manage_global_memcached_setting(fusion_migration_rate_limit=self.rebalance_migration_rate_limit)

        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir)
        self.sleep(30, "Wait after rebalance")

        self.cluster_util.print_cluster_stats(self.cluster)

        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        self.sleep(10, "Wait before performing swap rebalance during migration")
        if self.swap_rebalance_method == "dcp":
            nodes_to_rebalance_out = self.cluster.nodes_in_cluster[-self.swap_rebalance_nodes_during_migration:]
            nodes_to_rebalance_in = self.cluster.servers[len(self.cluster.nodes_in_cluster):\
                                    len(self.cluster.nodes_in_cluster)+self.swap_rebalance_nodes_during_migration]

            self.log.info(f"Swap Rebalancing {nodes_to_rebalance_out} during extent migration")
            result = self.task.rebalance(self.cluster, nodes_to_rebalance_in,
                                         nodes_to_rebalance_out,
                                         services=["kv"]*self.swap_rebalance_nodes_during_migration)
            self.assertFalse(result, "Swap rebalance during migration failed")

        elif self.swap_rebalance_method == "fusion":
            self.num_nodes_to_rebalance_in = 0
            self.num_nodes_to_rebalance_out = 0
            self.num_nodes_to_swap_rebalance = self.swap_rebalance_nodes_during_migration
            self.log.info("Running a Fusion rebalance")
            nodes_to_monitor2 = self.run_rebalance(output_dir=self.fusion_output_dir,
                                                   rebalance_count=2,
                                                   rebalance_sleep_time=30)
            self.sleep(30, "Wait after rebalance-out during migration")

        self.cluster_util.print_cluster_stats(self.cluster)

        # Update Migration Rate Limit so that extent migration process starts
        ClusterRestAPI(self.cluster.master).\
            manage_global_memcached_setting(fusion_migration_rate_limit=self.fusion_migration_rate_limit)

        extent_migration_array2 = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor2}")
        for node in nodes_to_monitor2:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array2.append(extent_th)

        for th in extent_migration_array:
            th.join()

        for th in extent_migration_array2:
            th.join()


    def test_rebalances_post_extent_migration(self):

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir)
        self.sleep(30, "Wait after rebalance")

        self.cluster_util.print_cluster_stats(self.cluster)

        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        for th in extent_migration_array:
            th.join()

        self.log_store_rebalance_cleanup()

        rebalance_counter = 2
        rebalance_types = ["rebalance_in", "rebalance_out", "swap_rebalance"]

        #while len(rebalance_types) > 0:
        for rebalance_type in rebalance_types:

            self.num_nodes_to_rebalance_in = 0
            self.num_nodes_to_rebalance_out = 0
            self.num_nodes_to_swap_rebalance = 0

            # # Pick a random rebalance type to execute
            # rebalance_type = random.choice(rebalance_types)

            if rebalance_type == "rebalance_in":
                self.num_nodes_to_rebalance_in = 1
            elif rebalance_type == "rebalance_out":
                self.num_nodes_to_rebalance_out = 1
            elif rebalance_type == "swap_rebalance":
                self.num_nodes_to_swap_rebalance = 1

            self.log.info(f"Running a Fusion rebalance of type: {rebalance_type}")
            nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                                  rebalance_count=rebalance_counter)
            self.sleep(30, "Wait after rebalance")

            self.cluster_util.print_cluster_stats(self.cluster)

            extent_migration_array = list()
            self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
            for node in nodes_to_monitor:
                for bucket in self.cluster.buckets:
                    extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                    extent_th.start()
                    extent_migration_array.append(extent_th)

            for th in extent_migration_array:
                th.join()

            self.log_store_rebalance_cleanup()

            rebalance_types.remove(rebalance_type)

            rebalance_counter += 1


    def test_delete_guest_volumes_during_migration(self):

        num_volumes_to_delete = self.input.param("num_volumes_to_delete", 3)
        min_storage_size = self.input.param("min_storage_size", 536870912)  # 0.5GB default

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Set Migration Rate Limit to 0 so that extent migration doesn't take place
        ClusterRestAPI(self.cluster.master).\
            manage_global_memcached_setting(fusion_migration_rate_limit=0)

        self.log.info(f"Running a Fusion rebalance with min_storage_size={min_storage_size} bytes")
        nodes_to_monitor = self.run_rebalance(
            output_dir=self.fusion_output_dir,
            min_storage_size=min_storage_size
        )

        self.sleep(30, "Wait after rebalance")
        
        self.log.info(f"Deleting {num_volumes_to_delete} guest volumes from NFS")
        
        guest_storage_base = os.path.join("/".join(self.nfs_server_path.split("/")[:-1]), "guest_storage")
        
        # Delete guest volumes for each node
        for node in nodes_to_monitor:
            node_id = f"ns_1@{node.ip}"
            ssh = RemoteMachineShellConnection(self.nfs_server)
            
            # Find guest directory for this node (subdirectory structure: node_id/reb*)
            find_cmd = f"ls -d {guest_storage_base}/{node_id}/reb* 2>/dev/null | tail -1"
            output, error = ssh.execute_command(find_cmd)
            
            if not output or not output[0].strip():
                self.log.warning(f"No guest directory found for {node_id}")
                ssh.disconnect()
                continue
            
            node_guest_dir = output[0].strip()
            self.log.info(f"Using guest directory: {node_guest_dir}")
            
            # List all available guest volumes
            list_cmd = f"ls -1 {node_guest_dir} | grep '^guest' | sort"
            self.log.info(f"Listing guest volumes in {node_guest_dir}")
            output, error = ssh.execute_command(list_cmd)
            
            if output:
                available_guest_volumes = [vol.strip() for vol in output if vol.strip()]
                self.log.info(f"Available guest volumes: {available_guest_volumes}")
                
                # Pick first N volumes to delete
                volumes_to_delete = available_guest_volumes[:num_volumes_to_delete]
                self.log.info(f"Selected volumes to delete: {volumes_to_delete}")
                
                for guest_vol in volumes_to_delete:
                    guest_volume_path = os.path.join(node_guest_dir, guest_vol)
                    self.log.info(f"Deleting {guest_volume_path}")
                    ssh.execute_command(f"rm -rf {guest_volume_path}")
            else:
                self.log.warning(f"Failed to list guest volumes: {error}")
            
            ssh.disconnect()

        self.sleep(120, "Wait after deleting to guest volumes")

        # Update Migration Rate Limit so that extent migration process starts
        ClusterRestAPI(self.cluster.master).\
                manage_global_memcached_setting(fusion_migration_rate_limit=self.fusion_migration_rate_limit)

        self.sleep(60, "Wait for migration to detect missing guest volumes")

        # Validate migration errors
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                cbstats = Cbstats(node)
                stats = cbstats.all_stats(bucket.name)
                error_count = int(stats['ep_fusion_migration_failures'])
                self.log.info(f"Node {node.ip}, Bucket {bucket.name}: Migration failures = {error_count}")
                self.assertGreater(error_count, 0, f"Expected migration failures on {node.ip}:{bucket.name}")
                cbstats.disconnect()

        self.cluster_util.print_cluster_stats(self.cluster)


    def test_kill_memcached_during_extent_migration(self):

        kill_after_seconds = self.input.param("kill_after_seconds", 10)
        min_storage_size = self.input.param("min_storage_size", 53687091200)
        num_iterations = self.input.param("num_iterations", 5)
        kill_type = self.input.param("kill_type", "hard_kill")
        kill_during_pause = self.input.param("kill_during_pause", False)
        enable_memcached_kill = self.input.param("enable_memcached_kill", True)
        num_rebalances = self.input.param("num_rebalances", 1)
        read_ops_rate = self.input.param("read_ops_rate", 10000)
        rebalance_type = self.input.param("rebalance_type", "in")
        rate_limit_toggle_interval = self.input.param("rate_limit_toggle_interval", 30)

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        self.log.info(f"Starting continuous read workload at {read_ops_rate} ops/sec")
        self.log.info("Read workload will run throughout all rebalances")
        doc_loading_tasks = self.perform_workload(0, self.num_items, doc_op="read",
                                                   wait=False, ops_rate=read_ops_rate)
        self.sleep(10, "Wait for read workload to start")

        for rebalance_count in range(1, num_rebalances + 1):
            selected_rebalance_type = self.select_rebalance_type(rebalance_type)
            self.log.info(f"\n{'='*80}")
            self.log.info(f"REBALANCE {rebalance_count}/{num_rebalances}: Type={selected_rebalance_type}, Cluster size {len(self.cluster.nodes_in_cluster)}")
            self.log.info(f"{'='*80}\n")

            ClusterRestAPI(self.cluster.master).\
                manage_global_memcached_setting(fusion_migration_rate_limit=0)

            self.log.info(f"[Rebalance {rebalance_count}] Running a Fusion rebalance with min_storage_size={min_storage_size} bytes")
            nodes_to_monitor = self.run_rebalance(
                output_dir=self.fusion_output_dir,
                rebalance_count=rebalance_count,
                min_storage_size=min_storage_size
            )

            self.sleep(30, f"[Rebalance {rebalance_count}] Wait after rebalance")

            if enable_memcached_kill and kill_during_pause:
                self.log.info(f"[Rebalance {rebalance_count}] Killing memcached while migration is PAUSED")
                for node in nodes_to_monitor:
                    shell = RemoteMachineShellConnection(node)
                    shell.kill_memcached()
                    shell.disconnect()

                self.sleep(30, f"[Rebalance {rebalance_count}] Wait for recovery after killing memcached during pause")

            rate_limit_thread = threading.Thread(target=self.toggle_rate_limit,
                                                args=(rate_limit_toggle_interval,
                                                      self.fusion_migration_rate_limit,
                                                      rebalance_count))
            rate_limit_thread.daemon = True
            rate_limit_thread.start()

            self.log.info(f"[Rebalance {rebalance_count}] Waiting {kill_after_seconds} seconds for migration to start")
            self.sleep(kill_after_seconds, f"[Rebalance {rebalance_count}] Wait for migration to start")

            status, guest_volumes_before = FusionRestAPI(self.cluster.master).get_active_guest_volumes()
            self.log.info(f"[Rebalance {rebalance_count}] Active guest volumes before crash: {guest_volumes_before}")

            migration_stats_before = {}
            for node in nodes_to_monitor:
                migration_stats_before[node.ip] = {}
                for bucket in self.cluster.buckets:
                    cbstats = Cbstats(node)
                    stats = cbstats.all_stats(bucket.name)
                    migration_stats_before[node.ip][bucket.name] = {
                        'completed_bytes': int(stats['ep_fusion_migration_completed_bytes']),
                        'total_bytes': int(stats['ep_fusion_migration_total_bytes'])
                    }
                    cbstats.disconnect()
            self.log.info(f"[Rebalance {rebalance_count}] Migration stats before crash: {migration_stats_before}")

            if enable_memcached_kill:
                for iteration in range(1, num_iterations + 1):
                    self.log.info(f"[Rebalance {rebalance_count}] Iteration {iteration}/{num_iterations} - Kill Type: {kill_type}")

                    for node in nodes_to_monitor:
                        shell = RemoteMachineShellConnection(node)
                        if kill_type == "hard_kill":
                            self.log.info(f"[Rebalance {rebalance_count}] Iteration {iteration}: Hard killing memcached on {node.ip}")
                            shell.kill_memcached()
                        elif kill_type == "graceful_restart":
                            self.log.info(f"[Rebalance {rebalance_count}] Iteration {iteration}: Restarting couchbase server on {node.ip}")
                            shell.restart_couchbase()
                        else:
                            shell.disconnect()
                            self.fail(f"Invalid kill_type: {kill_type}. Use 'hard_kill' or 'graceful_restart'")
                        shell.disconnect()

                    self.sleep(30, f"[Rebalance {rebalance_count}] Wait for service to recover (iteration {iteration})")

                    if iteration < num_iterations:
                        self.sleep(kill_after_seconds, f"[Rebalance {rebalance_count}] Wait before next iteration")
            else:
                self.log.info(f"[Rebalance {rebalance_count}] Memcached kill disabled, monitoring migration progress without crashes")
                total_wait_time = num_iterations * (30 + kill_after_seconds)
                self.sleep(total_wait_time, f"[Rebalance {rebalance_count}] Wait for migration progress")

            self.sleep(30, f"[Rebalance {rebalance_count}] Wait for migration to resume after all iterations")

            status, guest_volumes_after = FusionRestAPI(self.cluster.master).get_active_guest_volumes()
            self.log.info(f"[Rebalance {rebalance_count}] Active guest volumes after crash: {guest_volumes_after}")

            for node_id in guest_volumes_before:
                if node_id in guest_volumes_after:
                    volumes_before = set(guest_volumes_before[node_id])
                    volumes_after = set(guest_volumes_after[node_id])

                    extra_volumes = volumes_after - volumes_before
                    if extra_volumes:
                        self.log.error(f"[Rebalance {rebalance_count}] Node {node_id}: New volumes appeared after crash: {extra_volumes}")
                        self.fail(f"Guest volumes that were already migrated re-appeared after crash: {extra_volumes}")

                    completed_volumes = volumes_before - volumes_after
                    self.log.info(f"[Rebalance {rebalance_count}] Node {node_id}: Volumes completed during test: {len(completed_volumes)}")

            self.sleep(60, f"[Rebalance {rebalance_count}] Wait for migration to make progress after crash")

            migration_stats_after = {}
            for node in nodes_to_monitor:
                migration_stats_after[node.ip] = {}
                for bucket in self.cluster.buckets:
                    cbstats = Cbstats(node)
                    stats = cbstats.all_stats(bucket.name)
                    migration_stats_after[node.ip][bucket.name] = {
                        'completed_bytes': int(stats['ep_fusion_migration_completed_bytes']),
                        'total_bytes': int(stats['ep_fusion_migration_total_bytes']),
                        'failures': int(stats['ep_fusion_migration_failures'])
                    }
                    cbstats.disconnect()

            for node in nodes_to_monitor:
                for bucket in self.cluster.buckets:
                    before = migration_stats_before[node.ip][bucket.name]['completed_bytes']
                    after = migration_stats_after[node.ip][bucket.name]['completed_bytes']
                    failures = migration_stats_after[node.ip][bucket.name]['failures']

                    self.log.info(f"[Rebalance {rebalance_count}] Node {node.ip}, Bucket {bucket.name}: "
                                f"Before={before}, After={after}, Failures={failures}")

                    self.assertGreaterEqual(after, before,
                        f"Migration did not resume on {node.ip}:{bucket.name}")

            self.cluster_util.print_cluster_stats(self.cluster)

            self.log.info(f"[Rebalance {rebalance_count}] Stopping rate limit toggle thread")
            self.rate_limit_toggle_stop = True
            self.sleep(5, "Wait for rate limit thread to stop")
            self.rate_limit_toggle_stop = False

            self.log.info(f"[Rebalance {rebalance_count}] Completed successfully")

            if rebalance_count < num_rebalances:
                self.sleep(60, f"Cooldown before next rebalance (read workload continues)")

        self.log.info("Waiting for continuous read workload to complete")
        for task in doc_loading_tasks:
            self.doc_loading_tm.get_task_result(task)
        self.log.info("Continuous read workload completed successfully")

        self.log.info("Performing final data validation")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)

        self.log.info("Checking final migration and read stats across all nodes")
        final_failures_detected = False
        final_read_failures_detected = False
        
        for node in self.cluster.nodes_in_cluster:
            for bucket in self.cluster.buckets:
                cbstats = Cbstats(node)
                stats = cbstats.all_stats(bucket.name)
                migration_failures = int(stats['ep_fusion_migration_failures'])
                read_failures = int(stats['ep_data_read_failed'])
                
                self.log.info(f"Final stats - Node {node.ip}, Bucket {bucket.name}: "
                            f"Migration failures={migration_failures}, Read failures={read_failures}")
                
                if migration_failures > 0:
                    self.log.error(f"Node {node.ip}, Bucket {bucket.name}: "
                                 f"Migration failures detected: {migration_failures}")
                    final_failures_detected = True
                
                if read_failures > 0:
                    self.log.error(f"Node {node.ip}, Bucket {bucket.name}: "
                                 f"Read failures detected: {read_failures}")
                    final_read_failures_detected = True
                
                cbstats.disconnect()
        
        self.assertFalse(final_failures_detected, 
                        "Migration failures detected at end of test")
        self.assertFalse(final_read_failures_detected,
                        "Read failures detected at end of test")

    def test_toggle_guest_volume_permissions_during_extent_migration(self):

        kill_after_seconds = self.input.param("kill_after_seconds", 10)
        min_storage_size = self.input.param("min_storage_size", 53687091200)
        num_iterations = self.input.param("num_iterations", 5)
        kill_type = self.input.param("kill_type", "hard_kill")
        kill_during_pause = self.input.param("kill_during_pause", False)
        enable_memcached_kill = self.input.param("enable_memcached_kill", True)
        num_rebalances = self.input.param("num_rebalances", 1)
        read_ops_rate = self.input.param("read_ops_rate", 10000)
        rebalance_type = self.input.param("rebalance_type", "in")
        permission_toggle_interval = self.input.param("permission_toggle_interval", 30)

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        self.log.info(f"Starting continuous read workload at {read_ops_rate} ops/sec")
        doc_loading_tasks = self.perform_workload(0, self.num_items, doc_op="read",
                                                   wait=False, ops_rate=read_ops_rate)
        self.sleep(10, "Wait for read workload to start")

        for rebalance_count in range(1, num_rebalances + 1):
            selected_rebalance_type = self.select_rebalance_type(rebalance_type)
            self.log.info(f"\n{'='*80}")
            self.log.info(f"REBALANCE {rebalance_count}/{num_rebalances}: Type={selected_rebalance_type}")
            self.log.info(f"{'='*80}\n")

            ClusterRestAPI(self.cluster.master).\
                manage_global_memcached_setting(fusion_migration_rate_limit=0)

            nodes_to_monitor = self.run_rebalance(
                output_dir=self.fusion_output_dir,
                rebalance_count=rebalance_count,
                min_storage_size=min_storage_size
            )

            self.sleep(30, f"[Rebalance {rebalance_count}] Wait after rebalance")

            if enable_memcached_kill and kill_during_pause:
                for node in nodes_to_monitor:
                    shell = RemoteMachineShellConnection(node)
                    shell.kill_memcached()
                    shell.disconnect()
                self.sleep(30, f"[Rebalance {rebalance_count}] Wait for recovery")

            ClusterRestAPI(self.cluster.master).\
                manage_global_memcached_setting(fusion_migration_rate_limit=self.fusion_migration_rate_limit)

            permission_toggle_thread = threading.Thread(target=self.toggle_guest_volume_permissions,
                                                       args=(permission_toggle_interval,
                                                             nodes_to_monitor,
                                                             rebalance_count))
            permission_toggle_thread.daemon = True
            permission_toggle_thread.start()

            self.sleep(kill_after_seconds, f"[Rebalance {rebalance_count}] Wait for migration to start")

            migration_stats_before = {}
            for node in nodes_to_monitor:
                migration_stats_before[node.ip] = {}
                for bucket in self.cluster.buckets:
                    cbstats = Cbstats(node)
                    stats = cbstats.all_stats(bucket.name)
                    migration_stats_before[node.ip][bucket.name] = {
                        'completed_bytes': int(stats['ep_fusion_migration_completed_bytes'])
                    }
                    cbstats.disconnect()

            if enable_memcached_kill:
                for iteration in range(1, num_iterations + 1):
                    for node in nodes_to_monitor:
                        shell = RemoteMachineShellConnection(node)
                        if kill_type == "hard_kill":
                            shell.kill_memcached()
                        elif kill_type == "graceful_restart":
                            shell.restart_couchbase()
                        else:
                            shell.disconnect()
                            self.fail(f"Invalid kill_type: {kill_type}")
                        shell.disconnect()

                    self.sleep(30, f"[Rebalance {rebalance_count}] Wait for recovery (iteration {iteration})")
                    if iteration < num_iterations:
                        self.sleep(kill_after_seconds)
            else:
                total_wait_time = num_iterations * (30 + kill_after_seconds)
                self.sleep(total_wait_time, f"[Rebalance {rebalance_count}] Monitoring migration")

            self.sleep(60, f"[Rebalance {rebalance_count}] Wait for migration progress")

            migration_stats_after = {}
            for node in nodes_to_monitor:
                migration_stats_after[node.ip] = {}
                for bucket in self.cluster.buckets:
                    cbstats = Cbstats(node)
                    stats = cbstats.all_stats(bucket.name)
                    migration_stats_after[node.ip][bucket.name] = {
                        'completed_bytes': int(stats['ep_fusion_migration_completed_bytes'])
                    }
                    cbstats.disconnect()

            for node in nodes_to_monitor:
                for bucket in self.cluster.buckets:
                    before = migration_stats_before[node.ip][bucket.name]['completed_bytes']
                    after = migration_stats_after[node.ip][bucket.name]['completed_bytes']
                    self.assertGreaterEqual(after, before,
                        f"Migration did not progress on {node.ip}:{bucket.name}")

            self.log.info(f"[Rebalance {rebalance_count}] Stopping permission toggle and restoring access")
            self.rate_limit_toggle_stop = True
            self.sleep(5)
            
            guest_storage_base = os.path.join(os.path.dirname(self.nfs_server_path), "guest_storage")
            ssh = RemoteMachineShellConnection(self.nfs_server)
            for node in nodes_to_monitor:
                node_id = "ns_1@{0}".format(node.ip)
                
                # Find guest directory for this node (subdirectory structure: node_id/reb*)
                find_cmd = f"ls -d {guest_storage_base}/{node_id}/reb* 2>/dev/null | tail -1"
                output, error = ssh.execute_command(find_cmd)
                
                if output and output[0].strip():
                    node_guest_dir = output[0].strip()
                    ssh.execute_command("chmod -R 755 {0}".format(node_guest_dir))
                else:
                    self.log.warning(f"No guest directory found for {node_id} during permission restore")
            ssh.disconnect()
            
            self.rate_limit_toggle_stop = False

            if rebalance_count < num_rebalances:
                self.sleep(60)

        self.log.info("Waiting for read workload to complete")
        for task in doc_loading_tasks:
            self.doc_loading_tm.get_task_result(task)

        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)

        self.log.info("Checking final stats")
        final_failures_detected = False
        final_read_failures_detected = False
        
        for node in self.cluster.nodes_in_cluster:
            for bucket in self.cluster.buckets:
                cbstats = Cbstats(node)
                stats = cbstats.all_stats(bucket.name)
                migration_failures = int(stats['ep_fusion_migration_failures'])
                read_failures = int(stats['ep_data_read_failed'])
                
                if migration_failures > 0:
                    final_failures_detected = True
                if read_failures > 0:
                    final_read_failures_detected = True
                
                cbstats.disconnect()
        
        self.assertFalse(final_failures_detected, "Migration failures detected")
        self.assertFalse(final_read_failures_detected, "Read failures detected")

    def test_progressive_rebalances_with_memcached_kills(self):

        num_kill_iterations = self.input.param("num_kill_iterations", 10)
        kill_type = self.input.param("kill_type", "hard_kill")
        read_ops_rate = self.input.param("read_ops_rate", 10000)
        num_rebalances = self.input.param("num_rebalances", 3)

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after initial load and first upload sync")

        self.log.info(f"Starting continuous read workload at {read_ops_rate} ops/sec")
        self.log.info("Read workload will run throughout all rebalances")
        doc_loading_tasks = self.perform_workload(0, self.num_items, doc_op="read",
                                                   wait=False, ops_rate=read_ops_rate)
        self.sleep(10, "Wait for read workload to start")

        for rebalance_count in range(1, num_rebalances + 1):
            self.log.info(f"\n{'='*80}")
            self.log.info(f"REBALANCE {rebalance_count}/{num_rebalances}: Cluster size {len(self.cluster.nodes_in_cluster)} → {len(self.cluster.nodes_in_cluster) + 1}")
            self.log.info(f"{'='*80}\n")

            self.log.info(f"[Rebalance {rebalance_count}] Setting migration rate limit to 0 (pause)")
            ClusterRestAPI(self.cluster.master).\
                manage_global_memcached_setting(fusion_migration_rate_limit=0)

            self.log.info(f"[Rebalance {rebalance_count}] Running a Fusion rebalance")
            nodes_to_monitor = self.run_rebalance(
                output_dir=self.fusion_output_dir,
                rebalance_count=rebalance_count
            )

            self.sleep(30, f"[Rebalance {rebalance_count}] Wait after rebalance")

            self.log.info(f"[Rebalance {rebalance_count}] Resuming migration with rate_limit={self.fusion_migration_rate_limit / (1024*1024)} MB/s")
            ClusterRestAPI(self.cluster.master).\
                manage_global_memcached_setting(fusion_migration_rate_limit=self.fusion_migration_rate_limit)

            self.sleep(30, f"[Rebalance {rebalance_count}] Wait for migration to start")

            status, guest_volumes_before = FusionRestAPI(self.cluster.master).get_active_guest_volumes()
            self.log.info(f"[Rebalance {rebalance_count}] Active guest volumes before crash: {guest_volumes_before}")

            migration_stats_before = {}
            for node in nodes_to_monitor:
                migration_stats_before[node.ip] = {}
                for bucket in self.cluster.buckets:
                    cbstats = Cbstats(node)
                    stats = cbstats.all_stats(bucket.name)
                    migration_stats_before[node.ip][bucket.name] = {
                        'completed_bytes': int(stats['ep_fusion_migration_completed_bytes']),
                        'total_bytes': int(stats['ep_fusion_migration_total_bytes'])
                    }
                    cbstats.disconnect()
            self.log.info(f"[Rebalance {rebalance_count}] Migration stats before crash: {migration_stats_before}")

            self.log.info(f"[Rebalance {rebalance_count}] Starting {num_kill_iterations} memcached kill iterations")
            for iteration in range(1, num_kill_iterations + 1):
                self.log.info(f"[Rebalance {rebalance_count}] Iteration {iteration}/{num_kill_iterations} - Kill Type: {kill_type}")
                
                for node in nodes_to_monitor:
                    shell = RemoteMachineShellConnection(node)
                    if kill_type == "hard_kill":
                        self.log.info(f"[Rebalance {rebalance_count}] Iteration {iteration}: Hard killing memcached on {node.ip}")
                        shell.kill_memcached()
                    elif kill_type == "graceful_restart":
                        self.log.info(f"[Rebalance {rebalance_count}] Iteration {iteration}: Restarting couchbase server on {node.ip}")
                        shell.restart_couchbase()
                    else:
                        shell.disconnect()
                        self.fail(f"Invalid kill_type: {kill_type}. Use 'hard_kill' or 'graceful_restart'")
                    shell.disconnect()

                self.sleep(30, f"[Rebalance {rebalance_count}] Wait for service to recover (iteration {iteration})")

            self.sleep(30, f"[Rebalance {rebalance_count}] Wait for migration to resume after all iterations")

            status, guest_volumes_after = FusionRestAPI(self.cluster.master).get_active_guest_volumes()
            self.log.info(f"[Rebalance {rebalance_count}] Active guest volumes after crash: {guest_volumes_after}")

            for node_id in guest_volumes_before:
                if node_id in guest_volumes_after:
                    volumes_before = set(guest_volumes_before[node_id])
                    volumes_after = set(guest_volumes_after[node_id])

                    extra_volumes = volumes_after - volumes_before
                    if extra_volumes:
                        self.log.error(f"[Rebalance {rebalance_count}] Node {node_id}: New volumes appeared after crash: {extra_volumes}")
                        self.fail(f"Guest volumes that were already migrated re-appeared after crash: {extra_volumes}")

                    completed_volumes = volumes_before - volumes_after
                    self.log.info(f"[Rebalance {rebalance_count}] Node {node_id}: Volumes completed during test: {len(completed_volumes)}")

            self.sleep(60, f"[Rebalance {rebalance_count}] Wait for migration to make progress after crash")

            migration_stats_after = {}
            for node in nodes_to_monitor:
                migration_stats_after[node.ip] = {}
                for bucket in self.cluster.buckets:
                    cbstats = Cbstats(node)
                    stats = cbstats.all_stats(bucket.name)
                    migration_stats_after[node.ip][bucket.name] = {
                        'completed_bytes': int(stats['ep_fusion_migration_completed_bytes']),
                        'total_bytes': int(stats['ep_fusion_migration_total_bytes'])
                    }
                    cbstats.disconnect()

            for node in nodes_to_monitor:
                for bucket in self.cluster.buckets:
                    before = migration_stats_before[node.ip][bucket.name]['completed_bytes']
                    after = migration_stats_after[node.ip][bucket.name]['completed_bytes']

                    self.log.info(f"[Rebalance {rebalance_count}] Node {node.ip}, Bucket {bucket.name}: "
                                f"Before={before}, After={after}")

                    self.assertGreaterEqual(after, before,
                        f"Migration did not resume on {node.ip}:{bucket.name}")

            self.cluster_util.print_cluster_stats(self.cluster)

            self.log.info(f"[Rebalance {rebalance_count}] Completed successfully")

            if rebalance_count < num_rebalances:
                self.sleep(60, f"Cooldown before next rebalance (read workload continues)")

        self.log.info("Waiting for continuous read workload to complete")
        for task in doc_loading_tasks:
            self.doc_loading_tm.get_task_result(task)
        self.log.info("Continuous read workload completed successfully")

        self.log.info("Performing final data validation")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)

        self.log.info("Checking final stats for migration and read failures")
        for node in self.cluster.nodes_in_cluster:
            for bucket in self.cluster.buckets:
                cbstats = Cbstats(node)
                stats = cbstats.all_stats(bucket.name)
                migration_failures = int(stats['ep_fusion_migration_failures'])
                read_failures = int(stats['ep_data_read_failed'])
                
                self.log.info(f"Final - Node {node.ip}, Bucket {bucket.name}: "
                            f"Migration failures={migration_failures}, Read failures={read_failures}")
                
                self.assertEqual(migration_failures, 0, 
                               f"Migration failures on {node.ip}:{bucket.name}")
                self.assertEqual(read_failures, 0,
                               f"Read failures on {node.ip}:{bucket.name}")
                
                cbstats.disconnect()

    def test_delete_guest_volumes_before_rebalance(self):

        num_volumes_to_delete = self.input.param("num_volumes_to_delete", 5)
        min_storage_size = self.input.param("min_storage_size", 536870912)  # 0.5GB default

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        self.log.info(f"Running Fusion rebalance up to acceleration (stopping before rebalance) with min_storage_size={min_storage_size} bytes")
        nodes_to_monitor, plan_uuid, involved_nodes = self.run_rebalance(
            output_dir=self.fusion_output_dir,
            stop_before_rebalance=True,
            min_storage_size=min_storage_size
        )

        self.sleep(30, "Wait for acceleration to complete")

        status, guest_volumes_before = FusionRestAPI(self.cluster.master).get_active_guest_volumes()
        self.log.info(f"Guest volumes after acceleration: {guest_volumes_before}")

        self.log.info(f"Deleting {num_volumes_to_delete} guest volumes from NFS")
        
        guest_storage_base = os.path.join(os.path.dirname(self.nfs_server_path), "guest_storage")
        
        ssh = RemoteMachineShellConnection(self.nfs_server)
        for node in nodes_to_monitor:
            node_id = f"ns_1@{node.ip}"
            
            # Find guest directory for this node (subdirectory structure: node_id/reb*)
            find_cmd = f"ls -d {guest_storage_base}/{node_id}/reb* 2>/dev/null | tail -1"
            output_find, error_find = ssh.execute_command(find_cmd)
            
            if not output_find or not output_find[0].strip():
                self.log.warning(f"No guest directory found for {node_id}")
                continue
            
            node_guest_dir = output_find[0].strip()
            self.log.info(f"Using guest directory: {node_guest_dir}")
            
            # List all available guest volumes dynamically
            list_cmd = f"ls -1 {node_guest_dir} | grep '^guest' | sort"
            self.log.info(f"Listing guest volumes in {node_guest_dir}")
            output, error = ssh.execute_command(list_cmd)
            
            if output:
                available_guest_volumes = [vol.strip() for vol in output if vol.strip()]
                self.log.info(f"Available guest volumes: {available_guest_volumes}")
                
                # Select random volumes to delete
                volumes_to_delete = random.sample(available_guest_volumes, 
                                                 min(num_volumes_to_delete, len(available_guest_volumes)))
                self.log.info(f"Selected volumes to delete: {volumes_to_delete}")
                
                for guest_vol in volumes_to_delete:
                    guest_volume_path = os.path.join(node_guest_dir, guest_vol)
                    self.log.info(f"Deleting {guest_volume_path}")
                    ssh.execute_command(f"rm -rf {guest_volume_path}")
            else:
                self.log.warning(f"Failed to list guest volumes: {error}")
        
        ssh.disconnect()

        self.sleep(10, "Wait after deleting guest volumes")

        self.log.info(f"Manually starting rebalance with plan_uuid: {plan_uuid}")
    
        current_nodes = [node.ip for node in self.cluster.nodes_in_cluster]
        new_nodes = [node.replace("ns_1@", "") for node in involved_nodes]
        
        all_nodes = set(current_nodes) | set(new_nodes)
        known_nodes = [f"ns_1@{node}" for node in all_nodes]
        
        ejected_nodes = set(current_nodes) - set(new_nodes)
        eject_nodes = [f"ns_1@{node}" for node in ejected_nodes]
        
        self
        self.log.info(f"Known nodes: {known_nodes}")
        self.log.info(f"Ejected nodes: {eject_nodes}")
        
        rebalance_failed = False
        rebalance_error_msg = ""
        
        try:
            status, content = ClusterRestAPI(self.cluster.master).rebalance(
                known_nodes=known_nodes,
                eject_nodes=eject_nodes,
                plan_uuid=plan_uuid
            )
            
            if not status:
                rebalance_failed = True
                rebalance_error_msg = str(content)
                self.log.info(f"Rebalance API call failed as expected: {content}")
            else:
                self.log.info("Rebalance API call succeeded, monitoring for failure")
                self.sleep(30, "Wait for rebalance to detect missing guest volumes")
                
                progress_status, progress_content = ClusterRestAPI(self.cluster.master).rebalance_progress()
                if progress_status:
                    if progress_content.get("status") == "running":
                        self.sleep(60, "Wait for rebalance to fail")
                        progress_status, progress_content = ClusterRestAPI(self.cluster.master).rebalance_progress()
                    
                    if progress_content.get("status") in ["notRunning", "none"]:
                        error_msg = progress_content.get("errorMessage", "")
                        if error_msg:
                            rebalance_failed = True
                            rebalance_error_msg = error_msg
                            self.log.info(f"Rebalance failed with error: {error_msg}")
                        else:
                            rebalance_failed = False
                            self.log.warning("Rebalance completed but no error found")
        except Exception as e:
            rebalance_failed = True
            rebalance_error_msg = str(e)
            self.log.info(f"Rebalance failed with exception as expected: {e}")

        self.assertTrue(rebalance_failed, 
                       "Expected rebalance to fail due to missing guest volumes, but it succeeded")
        
        self.log.info(f"Rebalance failure message: {rebalance_error_msg}")
        
        self.cluster_util.print_cluster_stats(self.cluster)