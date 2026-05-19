import os
import random
import threading
import time
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_server_rest_util.fusion.fusion_api import FusionRestAPI
from cb_tools.cbstats import Cbstats
from storage.fusion.fusion_base import FusionBase
from shell_util.remote_connection import RemoteMachineShellConnection
from storage.magma.magma_disk_full import MagmaDiskFull


class FusionDiskFull(MagmaDiskFull, FusionBase):
    def setUp(self):
        super(FusionDiskFull, self).setUp()

        self.log.info("FusionDiskFull setUp Started")

    def tearDown(self):
        super(FusionDiskFull, self).tearDown()


    def test_fusion_log_store_disk_full(self):

        kill_memcached = self.input.param("kill_memcached", False)

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 60
        self.sleep(sleep_time, "Sleep after data loading")

        self.fill_disk(server=self.nfs_server, path="/data", free=2000)

        # Enable Fusion
        self.log.info("Configuring Fusion settings")
        self.configure_fusion()
        status, content = FusionRestAPI(self.cluster.master).enable_fusion()
        self.log.info(f"Enabling Fusion, Status = {status}, Content = {content}")
        self.assertTrue(status, "Enabling Fusion failed")

        self.sleep(10, "Wait before monitoring Fusion status")
        monitor_fusion_th = threading.Thread(target=self.get_fusion_status_info)
        monitor_fusion_th.start()

        if kill_memcached:
            self.sleep(200, "Wait before crashing continuously")
            self.crash_in_intervals(timeout=300)
            self.sleep(300, "Wait before freeing up disk space")
        else:
            self.sleep(900, "Wait before freeing up disk space")

        self.free_disk(server=self.nfs_server, path="/data")

        status, content = ClusterRestAPI(self.cluster.master).\
                        manage_global_memcached_setting(fusion_sync_rate_limit=78643200)
        self.log.info(f"Updating Sync Rate Limit, Status: {status}, Content: {content}")

        self.sleep(600, "Wait until Fusion Initial Sync is fully complete")
        self.monitor_fusion_info = False

        # Perform a Fusion Rebalance
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1,
                                              rebalance_sleep_time=60)
        self.log.info("Monitoring active guest volumes")
        guest_volume_th = threading.Thread(target=self.monitor_active_guest_volumes)
        guest_volume_th.start()
        guest_volume_th.join()

        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)



    def test_fusion_disk_full_during_extent_migration(self):

        kill_memcached = self.input.param("kill_memcached", False)

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 60
        self.sleep(sleep_time, "Sleep after data loading")

        # Set Migration Rate Limit to 0 so that extent migration doesn't take place
        ClusterRestAPI(self.cluster.master).\
                manage_global_memcached_setting(fusion_migration_rate_limit=0)

        # Perform a Fusion Rebalance
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1,
                                              rebalance_sleep_time=60)

        # Fill local data directory on new nodes
        for node in nodes_to_monitor:
            self.log.info(f"Filling disk on {node.ip}")
            self.fill_disk(server=node, free=1000)

        # Update Migration Rate Limit
        ClusterRestAPI(self.cluster.master).\
                manage_global_memcached_setting(fusion_migration_rate_limit=self.fusion_migration_rate_limit)

        self.log.info("Monitoring active guest volumes")
        guest_volume_th = threading.Thread(target=self.monitor_active_guest_volumes)
        guest_volume_th.start()

        if kill_memcached:
            self.sleep(200, "Wait before crashing continuously")
            self.crash_in_intervals(timeout=300)
            self.sleep(120, "Wait before freeing up disk space")
        else:
            self.sleep(600, "Wait before freeing up disk space")

        for node in nodes_to_monitor:
            self.log.info(f"Freeing disk on {node.ip}")
            self.free_disk(server=node)

        guest_volume_th.join()

        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        # Perform a read workload
        self.log_store_rebalance_cleanup(nodes=nodes_to_monitor)
        self.sleep(20)
        # Clear page cache
        self.clear_page_cache()
        self.sleep(30, "Wait after clearing page cache")
        self.log.info("Performing a read workload after the completion of extent migration")
        self.perform_workload(0, self.num_items, "read", ops_rate=20000)


    def test_fusion_local_disk_full_during_sync(self):

        validate_du = self.input.param("validate_du", True)

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 60
        self.sleep(sleep_time, "Sleep after data loading")

        # Enable Fusion
        self.log.info("Configuring Fusion settings")
        self.configure_fusion()
        self.enable_fusion()

        self.sleep(2 * self.fusion_upload_interval, "Wait after enabling Fusion")

        if validate_du:
            # Verify that log store contains data after 'enabling' Fusion
            o, e, initial_size = self.get_log_store_du()
            self.assertTrue(len(o) > 0, "DU command should return output")
            self.assertFalse(e, f"DU command failed with error: {e}")
            self.assertGreater(initial_size, 0, "Log store should contain data after enabling Fusion")

        # Fill local data directory on nodes
        for node in self.cluster.nodes_in_cluster:
            self.log.info(f"Filling disk on {node.ip}")
            self.fill_disk(server=node, free=200)

        for server in self.cluster.nodes_in_cluster:
            self.fill_final_space_left_on_file_system(server)

        # Disable Fusion
        self.disable_fusion()
        self.sleep(60, "Wait after disabling Fusion")

        if validate_du:
            # Verify that the log store is cleaned up
            o, e, cleanup_size = self.get_log_store_du()
            self.assertTrue(len(o) > 0, "DU command should return output")
            self.assertEqual(cleanup_size, 0, "Log store should be empty after disabling Fusion")

        # Re-Enable Fusion
        self.log.info("Configuring Fusion settings")
        self.configure_fusion()
        enable_th = threading.Thread(target=self.enable_fusion)
        enable_th.start()

        self.sleep(600, "Wait before clearing up disk space on log store")

        for server in self.cluster.nodes_in_cluster:
            self.free_disk(server)

        enable_th.join()

        if validate_du:
            # Verify that log store contains data after 're-enabling' Fusion
            o, e, initial_size = self.get_log_store_du()
            self.assertTrue(len(o) > 0, "DU command should return output")
            self.assertFalse(e, f"DU command failed with error: {e}")
            self.assertGreater(initial_size, 0, "Log store should contain data after re-enabling Fusion")

        # Disable after freeing disk
        self.disable_fusion()
        self.sleep(60, "Wait after disabling Fusion")

        if validate_du:
            # Verify that the log store is cleaned up
            o, e, cleanup_size = self.get_log_store_du()
            self.assertTrue(len(o) > 0, "DU command should return output")
            self.assertEqual(cleanup_size, 0, "Log store should be empty after disabling Fusion")

        # Re-Enable Fusion again
        self.enable_fusion()
        self.sleep(60, "Wait after Enabling Fusion")

        if validate_du:
            # Verify that log store contains data after 're-enabling' Fusion
            o, e, initial_size = self.get_log_store_du()
            self.assertTrue(len(o) > 0, "DU command should return output")
            self.assertFalse(e, f"DU command failed with error: {e}")
            self.assertGreater(initial_size, 0, "Log store should contain data after re-enabling Fusion")

        # Perform a Fusion Rebalance
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1,
                                              rebalance_sleep_time=60)
        self.log.info("Monitoring active guest volumes")
        guest_volume_th = threading.Thread(target=self.monitor_active_guest_volumes)
        guest_volume_th.start()
        guest_volume_th.join()

        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)


    def fill_final_space_left_on_file_system(self, server, path=None):

        path = path if path is not None else server.data_path

        shell = RemoteMachineShellConnection(server)

        self.sleep(10)

        output_file = os.path.join(path, "full_disk_2")
        cmd = f"dd if=/dev/urandom of={output_file} bs=1M"
        self.log.info(f"Running CMD: {cmd}")
        o, e = shell.execute_command(cmd)
        self.log.info(f"O = {o}, E = {e}")

        self.sleep(20)

        output_file = os.path.join(path, "full_disk_3")
        cmd = f"dd if=/dev/urandom of={output_file} bs=1K"
        self.log.info(f"Running CMD: {cmd}")
        o, e = shell.execute_command(cmd)
        self.log.info(f"O = {o}, E = {e}")

        df_cmd = "df -Thl"
        self.log.info(f"Running CMD: {df_cmd}")
        o, e = shell.execute_command(df_cmd)
        self.log.info(f"Server: {server.ip}, O = {o}, E = {e}")

        self.log.info(f"Disk filled completely on {server.ip}")


    def crash_in_intervals(self, interval=45, timeout=None):

        self.stop_crash = False
        end_time = time.time() + timeout if timeout else None

        while not self.stop_crash and (end_time is None or time.time() < end_time):

            for server in self.cluster.nodes_in_cluster:

                self.log.info(f"Killing memcached on {server.ip}")
                shell = RemoteMachineShellConnection(server)
                shell.kill_memcached()

            self.sleep(interval, f"Waiting to kill memcached on all nodes")
