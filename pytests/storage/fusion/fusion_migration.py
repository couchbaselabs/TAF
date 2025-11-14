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

    def tearDown(self):

        super(FusionMigration, self).tearDown()


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
            node_guest_dir = os.path.join(guest_storage_base, node_id)
            ssh = RemoteMachineShellConnection(self.nfs_server)
            
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
        min_storage_size = self.input.param("min_storage_size", 536870912)  # 0.5GB default
        num_iterations = self.input.param("num_iterations", 10)  # Number of kill/restart cycles
        kill_type = self.input.param("kill_type", "hard_kill")  # hard_kill or graceful_restart

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

        # Update Migration Rate Limit so that extent migration process starts
        ClusterRestAPI(self.cluster.master).\
                manage_global_memcached_setting(fusion_migration_rate_limit=self.fusion_migration_rate_limit)

        # Start parallel read workload with 10k ops/s
        self.log.info("Starting parallel read workload with 10k ops/s")
        doc_loading_tasks = self.perform_workload(0, self.num_items, doc_op="read", 
                                                   wait=False, ops_rate=10000)

        self.log.info(f"Waiting {kill_after_seconds} seconds for migration to start")
        self.sleep(kill_after_seconds, "Wait for migration to start")

        # Capture guest volumes before crash
        status, guest_volumes_before = FusionRestAPI(self.cluster.master).get_active_guest_volumes()
        self.log.info(f"Active guest volumes before crash: {guest_volumes_before}")

        # Capture migration stats before crash
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
        self.log.info(f"Migration stats before crash: {migration_stats_before}")

        # Perform multiple kill/restart iterations
        for iteration in range(1, num_iterations + 1):
            self.log.info(f"=== Iteration {iteration}/{num_iterations} - Kill Type: {kill_type} ===")
            
            for node in nodes_to_monitor:
                shell = RemoteMachineShellConnection(node)
                if kill_type == "hard_kill":
                    self.log.info(f"Iteration {iteration}: Hard killing memcached on {node.ip}")
                    shell.kill_memcached()
                elif kill_type == "graceful_restart":
                    self.log.info(f"Iteration {iteration}: Restarting couchbase server on {node.ip}")
                    shell.restart_couchbase()
                else:
                    shell.disconnect()
                    self.fail(f"Invalid kill_type: {kill_type}. Use 'hard_kill' or 'graceful_restart'")
                shell.disconnect()
            
            self.sleep(30, f"Wait for service to recover (iteration {iteration})")
            
            # If not the last iteration, wait before next kill
            if iteration < num_iterations:
                self.sleep(kill_after_seconds, f"Wait before next iteration")

        self.sleep(30, "Wait for migration to resume after all iterations")

        # Capture guest volumes after crash
        status, guest_volumes_after = FusionRestAPI(self.cluster.master).get_active_guest_volumes()
        self.log.info(f"Active guest volumes after crash: {guest_volumes_after}")

        # Validate guest volumes consistency
        for node_id in guest_volumes_before:
            if node_id in guest_volumes_after:
                volumes_before = set(guest_volumes_before[node_id])
                volumes_after = set(guest_volumes_after[node_id])
                
                # Volumes after should be subset of volumes before (some may have completed)
                extra_volumes = volumes_after - volumes_before
                if extra_volumes:
                    self.log.error(f"Node {node_id}: New volumes appeared after crash: {extra_volumes}")
                    self.fail(f"Guest volumes that were already migrated re-appeared after crash: {extra_volumes}")
                
                completed_volumes = volumes_before - volumes_after
                self.log.info(f"Node {node_id}: Volumes completed during test: {len(completed_volumes)}")

        # Wait for migration to continue
        self.sleep(60, "Wait for migration to make progress after crash")

        # Capture migration stats after crash
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

        # Validate migration resumed
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                before = migration_stats_before[node.ip][bucket.name]['completed_bytes']
                after = migration_stats_after[node.ip][bucket.name]['completed_bytes']
                failures = migration_stats_after[node.ip][bucket.name]['failures']
                
                self.log.info(f"Node {node.ip}, Bucket {bucket.name}: "
                            f"Before={before}, After={after}, Failures={failures}")
                
                # Migration should have made progress or completed
                self.assertGreaterEqual(after, before, 
                    f"Migration did not resume on {node.ip}:{bucket.name}")
                
                # No failures expected for crash recovery
                self.assertEqual(failures, 0, 
                    f"Unexpected failures after crash recovery on {node.ip}:{bucket.name}")

        # Wait for parallel read workload to complete
        self.log.info("Waiting for parallel read workload to complete")
        for task in doc_loading_tasks:
            self.doc_loading_tm.get_task_result(task)
        self.log.info("Parallel read workload completed successfully")

        self.cluster_util.print_cluster_stats(self.cluster)

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
            node_guest_dir = os.path.join(guest_storage_base, node_id)
            
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