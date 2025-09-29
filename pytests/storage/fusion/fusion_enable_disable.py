import json
import os
import subprocess
import threading
import time
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_server_rest_util.fusion.fusion_api import FusionRestAPI
from cb_tools.cbstats import Cbstats
from rebalance_utils.rebalance_util import RebalanceUtil
from shell_util.remote_connection import RemoteMachineShellConnection
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_base import MagmaBaseTest


class FusionEnableDisable(MagmaBaseTest, FusionBase):
    def setUp(self):
        super(FusionEnableDisable, self).setUp()

        self.cluster.kv_nodes = self.cluster_util.get_kv_nodes(
                    self.cluster, self.cluster.nodes_in_cluster)
        self.log.info(f"KV nodes = {self.cluster.kv_nodes}")

        self.log.info("FusionEnableDisable setUp started")

        # Override Fusion default settings
        for bucket in self.cluster.buckets:
            self.change_fusion_settings(bucket, upload_interval=self.fusion_upload_interval,
                                        checkpoint_interval=self.fusion_log_checkpoint_interval,
                                        logstore_frag_threshold=self.logstore_frag_threshold)
        # Set sync rate limit and migration rate limit
        status, content = ClusterRestAPI(self.cluster.master).\
            manage_global_memcached_setting(fusion_sync_rate_limit=self.fusion_sync_rate_limit,
                                            fusion_migration_rate_limit=self.fusion_migration_rate_limit)
        self.log.info(f"Status = {status}, Content = {content}")

        for server in self.cluster.servers:
            self.log.info(f"Enabling diag/eval on non local hosts for server: {server.ip}")
            shell = RemoteMachineShellConnection(server)
            o, e = shell.enable_diag_eval_on_non_local_hosts()
            self.log.info(f"Output = {o}, Error = {e}")
            shell.disconnect()


    def tearDown(self):
        super(FusionEnableDisable, self).tearDown()


    def monitor_sync_stats(self, server, bucket, timeout=300):

        self.log.info(f"Monitoring Sync Stats on server: {server.ip}, bucket: {bucket.name}")

        end_time = time.time() + timeout
        cbstats_obj = Cbstats(server)
        sync_complete = False

        while time.time() < end_time:

            result = cbstats_obj.all_stats(bucket.name)
            completed_bytes = result["ep_fusion_sync_session_completed_bytes"]
            total_bytes = result["ep_fusion_sync_session_total_bytes"]

            self.log.info(f"Server: {server.ip}, Bucket: {bucket.name}, "
                          f"Completed bytes: {completed_bytes}, Total bytes: {total_bytes}")

            if int(completed_bytes) == int(total_bytes) and int(total_bytes) != 0:
                sync_complete = True
                break
            time.sleep(2)

        if sync_complete:
            self.log.info(f"Sync complete for bucket: {bucket.name} on server: {server.ip}")
        else:
            self.log.info(f"Sync not complete for bucket: {bucket.name} on server: {server.ip} even after {timeout} seconds")


    def test_fusion_enable_midway(self):

        self.enable_bucket_count = self.input.param("enable_bucket_count", None)
        if self.enable_bucket_count is not None:
            self.fusion_enabled_buckets = self.cluster.buckets[:int(self.enable_bucket_count)]
        else:
            self.fusion_enabled_buckets = self.cluster.buckets

        self.log.info("Fusion Enabled buckets")
        for bucket in self.fusion_enabled_buckets:
            self.log.info(f"Bucket: {bucket.name}")

        self.log.info("Verifying that Fusion is disabled initially")
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Status = {status}, Content = {content}")

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        fusion_enable_buckets = None
        if self.enable_bucket_count is not None:
            fusion_enable_buckets = ",".join(bucket.name for bucket in self.cluster.buckets[:int(self.enable_bucket_count)])
            self.log.info(f"Enabling Fusion on a subset of buckets: {fusion_enable_buckets}")

        self.log.info("Enabling Fusion after initial loading")
        self.configure_fusion()

        enable_fusion_th = threading.Thread(target=self.enable_fusion, args=[fusion_enable_buckets])
        enable_fusion_th.start()

        monitor_sync_threads = list()
        for server in self.cluster.nodes_in_cluster:
            for bucket in self.fusion_enabled_buckets:
                th = threading.Thread(target=self.monitor_sync_stats, args=[server, bucket])
                monitor_sync_threads.append(th)
                th.start()

        enable_fusion_th.join()

        for th in monitor_sync_threads:
            th.join()

        # Get Uploader Map after enabling Fusion
        self.get_fusion_uploader_info(buckets=self.fusion_enabled_buckets)

        # Load more data after Fusion is enabled
        self.log.info("Performing data load after Fusion is enabled")
        self.perform_workload(self.num_items, self.create_start + (self.create_start // 2), "create", True)
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after subsequent data loading")

        # Perform a Fusion Rebalance
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1)

        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.fusion_enabled_buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        for th in extent_migration_array:
            th.join()

        # Get Uploader Map after Fusion Rebalance
        self.get_fusion_uploader_info(buckets=self.fusion_enabled_buckets)

        self.cluster_util.print_cluster_stats(self.cluster)

        self.log.info("Validating item count after rebalance")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)


    def test_disable_fusion_midway(self):

        perform_dcp_rebalance = self.input.param("perform_dcp_rebalance", True)

        self.log.info("Verifying that Fusion is enabled initially")
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Status = {status}, Content = {content}")

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Verify that log store initially contains data
        ssh = RemoteMachineShellConnection(self.nfs_server)
        o, e = ssh.execute_command(f"du -sh {self.nfs_server_path}")
        self.log.info(f"NFS path DU check, Output = {o}, Error = {e}")
        ssh.disconnect()

        # Get Initial Uploader Map
        self.get_fusion_uploader_info()

        status, content = FusionRestAPI(self.cluster.master).disable_fusion()
        self.log.info(f"Status = {status}, Content = {content}")
        self.assertTrue(status, "Disabling Fusion failed")

        self.sleep(30, "Wait after disabling Fusion")

        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Status = {status}, Content = {content}")

        # Verify that the log store is cleaned up
        ssh = RemoteMachineShellConnection(self.nfs_server)
        o, e = ssh.execute_command(f"du -sh {self.nfs_server_path}")
        self.log.info(f"NFS path DU check, Output = {o}, Error = {e}")
        ssh.disconnect()

        # Get Uploader Map after disabling Fusion
        self.get_fusion_uploader_info()

        # Load more data after Fusion is disabled
        self.log.info("Performing data load after Fusion is enabled")
        self.perform_workload(self.num_items, self.create_start + (self.create_start // 2), "create", True)
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after subsequent data loading")

        # Verify that nothing is being uploaded to the log store
        ssh = RemoteMachineShellConnection(self.nfs_server)
        o, e = ssh.execute_command(f"du -sh {self.nfs_server_path}")
        self.log.info(f"NFS path DU check, Output = {o}, Error = {e}")
        ssh.disconnect()

        # Perform a DCP rebalance
        if perform_dcp_rebalance:
            self.spare_node = self.cluster.servers[self.nodes_init]
            self.log.info("DCP Rebalance starting...")
            rebalance_task = self.task.async_rebalance(
                self.cluster,
                to_add=[self.spare_node],
                check_vbucket_shuffling=False,
                services=["kv"],
                retry_get_process_num=self.retry_get_process_num)

            self.task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "DCP Rebalance post disabling Fusion failed")

            self.cluster_util.print_cluster_stats(self.cluster)

            # Get Uploader Map after DCP rebalance
            self.get_fusion_uploader_info()

            self.log.info("Validating item count after rebalance")
            self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
            self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)


    def test_disable_fusion_during_extent_migration(self):

        ###
        # Set a low migration rate limit so that it takes longer to finish
        # e.g: 10MB/s
        ###

        self.log.info("Verifying that Fusion is enabled initially")
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Status = {status}, Content = {content}")

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Verify that the log store initially contains some data
        ssh = RemoteMachineShellConnection(self.nfs_server)
        o, e = ssh.execute_command(f"du -sh {self.nfs_server_path}")
        self.log.info(f"NFS path DU check, Output = {o}, Error = {e}")
        ssh.disconnect()

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

        self.sleep(30, "Wait before disabling Fusion")

        # Disable Fusion during extent migration
        status, content = FusionRestAPI(self.cluster.master).disable_fusion()
        self.log.info(f"Status = {status}, Content = {content}")
        self.assertFalse(status, "Disabling Fusion during extent migration succeeded")

        monitor_fusion_th = threading.Thread(target=self.get_fusion_status_info)
        monitor_fusion_th.start()

        for th in extent_migration_array:
            th.join()

        self.monitor_fusion_info = False
        monitor_fusion_th.join()


    def test_disable_fusion_during_rebalance(self):

        self.log.info("Verifying that Fusion is enabled initially")
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Status = {status}, Content = {content}")

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Verify that the log store initially contains some data
        ssh = RemoteMachineShellConnection(self.nfs_server)
        o, e = ssh.execute_command(f"du -sh {self.nfs_server_path}")
        self.log.info(f"NFS path DU check, Output = {o}, Error = {e}")
        ssh.disconnect()

        # Perform a data workload in parallel when rebalance is taking place
        self.log.info("Performing data load in parallel when rebalance is taking place")
        doc_loading_tasks = self.perform_workload(self.num_items, self.num_items * 2, "create", False)

        # Perform a Fusion Rebalance
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1,
                                              wait_for_rebalance_to_complete=False)
        self.sleep(10, "Wait before checking rebalance progress")
        rebalance_monitor_thread = threading.Thread(target=RebalanceUtil(self.cluster).monitor_rebalance)
        rebalance_monitor_thread.start()

        self.sleep(10, "Wait before disabling Fusion during a rebalance")

        # Disable Fusion during a rebalance
        status, content = FusionRestAPI(self.cluster.master).disable_fusion()
        self.log.info(f"Disabling Fusion, Status = {status}, Content = {content}")
        self.assertFalse(status, "Disabling Fusion during Fusion rebalance succeeded")

        monitor_fusion_th = threading.Thread(target=self.get_fusion_status_info)
        monitor_fusion_th.start()

        rebalance_monitor_thread.join()

        # Wait for doc load to complete
        for task in doc_loading_tasks:
            self.doc_loading_tm.get_task_result(task)

        self.sleep(5, "Wait before monitoring extent migration")
        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        for th in extent_migration_array:
            th.join()

        self.sleep(30, "Wait after completion of the entire rebalance/migration process")

        self.monitor_fusion_info = False
        monitor_fusion_th.join()

        # Disable Fusion after the completion of rebalance
        status, content = FusionRestAPI(self.cluster.master).disable_fusion()
        self.log.info(f"Disabling Fusion, Status = {status}, Content = {content}")
        self.assertTrue(status, "Disabling Fusion after Fusion rebalance failed")

        self.sleep(30, "Wait before fetching Fusion status")
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Status = {status}, Content = {content}")


    def test_fusion_rebalance_while_disabling(self):

        # Set Extent Migration Rate Limit to 0
        # Perform Fusion Rebalance
        # Call /disable, but since there are active guest volumes, state would be stuck in 'disabling'
        # Perform another Fusion Rebalance, prepareRebalance API should return an error

        self.log.info("Verifying that Fusion is enabled initially")
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Status = {status}, Content = {content}")

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Perform a Fusion Rebalance
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1)

        self.sleep(10, "Wait before disabling Fusion during a rebalance")

        # Disable Fusion when extent migration is set to 0
        status, content = FusionRestAPI(self.cluster.master).disable_fusion()
        self.log.info(f"Disabling Fusion, Status = {status}, Content = {content}")

        monitor_fusion_th = threading.Thread(target=self.get_fusion_status_info)
        monitor_fusion_th.start()

        # Get Active Guest Volumes
        self.sleep(30, "Wait before fetching active guest volumes")
        status, content = FusionRestAPI(self.cluster.master).get_active_guest_volumes()
        self.log.info(f"Active guest volumes, Status = {status}, Content = {content}")

        # Start another Fusion Rebalance-in while state = 'disabling'
        self.log.info("Starting another Fusion Rebalance while Fusion is being disabled")
        new_node = self.cluster.servers[len(self.cluster.nodes_in_cluster)]
        self.log.info(f"Adding new node {new_node.ip}")
        status, content = ClusterRestAPI(self.cluster.master).add_node(
                                        new_node.ip, new_node.rest_username,
                                        new_node.rest_password, ["kv"])
        self.log.info(f"Adding node, Status = {status}, Content = {content}")

        keep_nodes = list()
        for server in self.cluster.nodes_in_cluster:
            keep_nodes.append(f"ns_1@{server.ip}")
        keep_nodes.append(f"ns_1@{new_node.ip}")
        self.log.info(f"Keep nodes = {keep_nodes}")

        status, content = FusionRestAPI(self.cluster.master).prepare_rebalance(keep_nodes)
        self.log.info(f"Status = {status}, Content = {content}")
        self.assertFalse(status, "PrepareRebalance API succeeded during disabling state")

        self.monitor_fusion_info = False
        monitor_fusion_th.join()


    def test_disable_fusion_during_upload(self):

        ###
        # Set sync rate limit to a low value. e.g: 1MB/s
        # Set enableSyncThreshold to a low value. e.g: 10MB
        ###

        self.log.info("Verifying that Fusion is disabled initially")
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Status = {status}, Content = {content}")

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Enable Fusion
        self.log.info("Configuring Fusion settings")
        # Set enableSyncThreshold to a low value. e.g: 10MB
        self.configure_fusion()
        self.log.info("Enabling Fusion midway")
        status, content = FusionRestAPI(self.cluster.master).enable_fusion()
        self.log.info(f"Status = {status}, Content = {content}")

        monitor_sync_threads = list()
        for server in self.cluster.nodes_in_cluster:
            for bucket in self.cluster.buckets:
                th = threading.Thread(target=self.monitor_sync_stats, args=[server, bucket, 180])
                monitor_sync_threads.append(th)
                th.start()

        # Get Uploader Map after enabling Fusion
        self.sleep(10, "Wait before fetching uploader map")
        self.get_fusion_uploader_info()

        self.sleep(30, "Sleep before disabling Fusion during snapshot upload")
        status, content = FusionRestAPI(self.cluster.master).disable_fusion()
        self.log.info(f"Status = {status}, Content = {content}")
        self.assertTrue(status, "Disabling Fusion during snapshot upload failed")

        self.sleep(15, "Sleeping after disabling Fusion")

        # Verify that the log store is cleaned up
        ssh = RemoteMachineShellConnection(self.nfs_server)
        o, e = ssh.execute_command(f"du -sh {self.nfs_server_path}")
        self.log.info(f"NFS path DU check, Output = {o}, Error = {e}")
        ssh.disconnect()

        # Get Uploader Map after disabling Fusion
        self.get_fusion_uploader_info()

        for th in monitor_sync_threads:
            th.join()


    def test_fusion_remove_delete_permissions_log_store(self):

        self.log.info("Verifying that Fusion is enabled initially")
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Status = {status}, Content = {content}")

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Remove permissions for 'couchbase' user from the log store directory
        log_store_dir = "/" + self.fusion_log_store_uri.split("///")[-1]
        remove_perm_cmd = f"chown -R root:root {log_store_dir}"
        self.log.info(f"Removing permissions CMD: {remove_perm_cmd}")
        ssh = RemoteMachineShellConnection(self.cluster.master)
        o, e = ssh.execute_command(remove_perm_cmd)

        # Disable Fusion
        status, content = FusionRestAPI(self.cluster.master).disable_fusion()
        self.log.info(f"Disabling Fusion, Status = {status}, Content = {content}")

        monitor_fusion_th = threading.Thread(target=self.get_fusion_status_info)
        monitor_fusion_th.start()

        self.sleep(60, "Wait before re-introduing permissions")
        restore_perm_cmd = f"chown -R couchbase:couchbase {log_store_dir}"
        o, e = ssh.execute_command(restore_perm_cmd)
        ssh.disconnect()

        self.sleep(60, "Wait before stopping all monitoring threads")
        self.monitor_fusion_info = False
        monitor_fusion_th.join()


    def test_fusion_rebalance_while_enabling(self):

        self.log.info("Verifying that Fusion is disabled initially")
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Status = {status}, Content = {content}")

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        self.log.info("Enabling Fusion after initial loading")
        self.configure_fusion()

        enable_fusion_th = threading.Thread(target=self.enable_fusion, args=[])
        enable_fusion_th.start()

        monitor_sync_threads = list()
        for server in self.cluster.nodes_in_cluster:
            for bucket in self.cluster.buckets:
                th = threading.Thread(target=self.monitor_sync_stats, args=[server, bucket])
                monitor_sync_threads.append(th)
                th.start()

        self.sleep(20, "Wait before calling Fusion PrepareRebalance")

        keep_nodes = [f"ns_1@{server.ip}" for server in self.cluster.nodes_in_cluster[:-1]]
        self.log.info(f"Keep nodes = {keep_nodes}")

        status, content = FusionRestAPI(self.cluster.master).prepare_rebalance(keep_nodes)
        self.log.info(f"Status = {status}, Content = {content}")
        self.assertFalse(status, "PrepareRebalance API succeeded during enabling state")

        enable_fusion_th.join()

        for th in monitor_sync_threads:
            th.join()


    def test_stop_fusion_midway(self):

        post_stop_step = self.input.param("post_stop_step", "enable") # enable/disable
        perform_dcp_rebalance = self.input.param("perform_dcp_rebalance", False)
        perform_fusion_rebalance = self.input.param("perform_fusion_rebalance", False)
        enable_bucket_count = self.input.param("enable_bucket_count", None)

        self.log.info("Verifying that Fusion is enabled initially")
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Status = {status}, Content = {content}")

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        self.get_fusion_uploader_info()

        ssh = RemoteMachineShellConnection(self.nfs_server)
        o, e = ssh.execute_command(f"du -sh {self.nfs_server_path}")
        self.log.info(f"NFS path DU check, Output = {o}, Error = {e}")
        ssh.disconnect()

        # Stop Fusion
        status, content = FusionRestAPI(self.cluster.master).stop_fusion()
        self.log.info(f"Stopping Fusion, Status: {status}, Content: {content}")
        self.assertTrue(status, "Stopping Fusion failed")

        self.sleep(30, "Wait after stopping Fusion")
        monitor_fusion_th = threading.Thread(target=self.get_fusion_status_info)
        monitor_fusion_th.start()

        # Perform a data workload while fusion is in stopped state
        self.log.info("Performing data load while fusion is in stopped state")
        self.perform_workload(self.num_items, self.create_start + (self.num_items // 2), "create", True)
        self.sleep(60, "Wait after data loading")

        # Try a Fusion Rebalance
        keep_nodes = [f"ns_1@{server.ip}" for server in self.cluster.nodes_in_cluster[:-1]]
        self.log.info(f"Keep nodes = {keep_nodes}")

        status, content = FusionRestAPI(self.cluster.master).prepare_rebalance(keep_nodes)
        self.log.info(f"Status = {status}, Content = {content}")
        self.assertFalse(status, "PrepareRebalance API Succeeded while Fusion is in stopped state")

        if perform_dcp_rebalance:
            self.spare_node = self.cluster.servers[self.nodes_init]
            self.log.info("DCP Rebalance-in starting...")
            rebalance_task = self.task.async_rebalance(
                                self.cluster,
                                to_add=[self.spare_node],
                                check_vbucket_shuffling=False,
                                services=["kv"],
                                retry_get_process_num=self.retry_get_process_num)
            self.task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "DCP Rebalance post stopping Fusion failed")
            self.cluster_util.print_cluster_stats(self.cluster)

            self.get_fusion_uploader_info()

            # Perform a data workload after DCP rebalance
            self.log.info("Performing data load after DCP rebalance")
            self.perform_workload(self.num_items, self.create_start + (self.num_items // 2), "create", True)
            self.sleep(60, "Wait after data loading")

        self.sleep(30, "Wait before stopping monitoring threads")
        self.monitor_fusion_info = False
        monitor_fusion_th.join()

        if post_stop_step == "enable":

            if enable_bucket_count is not None:
                self.fusion_enabled_buckets = self.cluster.buckets[:int(enable_bucket_count)]
                fusion_enable_buckets = ",".join(bucket.name for bucket in self.cluster.buckets[:int(enable_bucket_count)])
                self.log.info(f"Enabling Fusion on a subset of buckets: {fusion_enable_buckets}")
            else:
                self.fusion_enabled_buckets = self.cluster.buckets
                fusion_enable_buckets = None

            self.enable_fusion(buckets=fusion_enable_buckets)

            self.get_fusion_uploader_info(buckets=self.fusion_enabled_buckets)

            # Perform a data workload after fusion is enabled again
            self.log.info("Performing data load after Fusion is enabled again")
            self.perform_workload(self.num_items, self.create_start + (self.num_items // 2), "create", True)
            sleep_time = 120 + self.fusion_upload_interval + 30
            self.sleep(sleep_time, "Sleep after data loading")

            ssh = RemoteMachineShellConnection(self.nfs_server)
            o, e = ssh.execute_command(f"du -sh {self.nfs_server_path}")
            self.log.info(f"NFS path DU check, Output = {o}, Error = {e}")
            ssh.disconnect()

            if perform_fusion_rebalance:
                self.log.info("Running a Fusion rebalance")
                self.num_nodes_to_rebalance_in = 1
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

                self.get_fusion_uploader_info(buckets=self.fusion_enabled_buckets)

                # Perform a data workload after the Fusion rebalance
                self.log.info("Performing data load after Fusion rebalance")
                self.perform_workload(self.num_items, self.create_start + (self.num_items // 2), "create", True)
                sleep_time = 120 + self.fusion_upload_interval + 30
                self.sleep(sleep_time, "Sleep after data loading")

                ssh = RemoteMachineShellConnection(self.nfs_server)
                o, e = ssh.execute_command(f"du -sh {self.nfs_server_path}")
                self.log.info(f"NFS path DU check, Output = {o}, Error = {e}")
                ssh.disconnect()


        elif post_stop_step == "disable":

            status, content = FusionRestAPI(self.cluster.master).disable_fusion()
            self.log.info(f"Stopping Fusion, Status: {status}, Content: {content}")
            self.assertTrue(status, "Disabling Fusion from stopped state failed")

            self.sleep(30, "Wait after disabling Fusion")
            status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
            self.log.info(f"Status = {status}, Content = {content}")

            ssh = RemoteMachineShellConnection(self.nfs_server)
            o, e = ssh.execute_command(f"du -sh {self.nfs_server_path}")
            self.log.info(f"NFS path DU check, Output = {o}, Error = {e}")
            ssh.disconnect()


    def test_stop_fusion_while_enabling_and_enable_again(self):

        enable_bucket_count = self.input.param("enable_bucket_count", None)

        self.log.info("Verifying that Fusion is disabled initially")
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Status = {status}, Content = {content}")

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        self.log.info("Enabling Fusion after initial loading")
        self.configure_fusion()

        status, content = FusionRestAPI(self.cluster.master).enable_fusion()
        self.log.info(f"Enabling Fusion, Status = {status}, Content = {content}")
        self.assertTrue(status, "Enabling Fusion failed")

        self.sleep(20, "Wait before monitoring Fusion status")
        monitor_fusion_th = threading.Thread(target=self.get_fusion_status_info)
        monitor_fusion_th.start()

        monitor_sync_threads = list()
        for server in self.cluster.nodes_in_cluster:
            for bucket in self.cluster.buckets:
                th = threading.Thread(target=self.monitor_sync_stats, args=[server, bucket])
                monitor_sync_threads.append(th)
                th.start()

        self.sleep(30, "Wait before stopping during enabling")
        status, content = FusionRestAPI(self.cluster.master).stop_fusion()
        self.log.info(f"Stopping Fusion, Status = {status}, Content = {content}")
        self.assertTrue(status, "Stopping Fusion during enabling failed")

        self.sleep(30, "Wait after stopping Fusion")

        if enable_bucket_count is not None:
            self.fusion_enabled_buckets = self.cluster.buckets[:int(enable_bucket_count)]
            fusion_enable_buckets = ",".join(bucket.name for bucket in self.cluster.buckets[:int(enable_bucket_count)])
            self.log.info(f"Enabling Fusion on a subset of buckets: {fusion_enable_buckets}")
        else:
            self.fusion_enabled_buckets = self.cluster.buckets
            fusion_enable_buckets = None

        self.log.info("Re-enabling Fusion")
        self.enable_fusion(buckets=fusion_enable_buckets)

        self.sleep(30, "Wait before stopping monitoring threads")
        self.monitor_fusion_info = False
        monitor_fusion_th.join()

        for th in monitor_sync_threads:
            th.join()

    def test_stop_fusion_during_rebalance_or_migration(self):

        stop_fusion_during = self.input.param("stop_fusion_during", "rebalance") # rebalance/migration

        self.log.info("Verifying that Fusion is enabled initially")
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Status = {status}, Content = {content}")

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Verify that the log store initially contains some data
        ssh = RemoteMachineShellConnection(self.nfs_server)
        o, e = ssh.execute_command(f"du -sh {self.nfs_server_path}")
        self.log.info(f"NFS path DU check, Output = {o}, Error = {e}")
        ssh.disconnect()

        # Perform a data workload in parallel when rebalance is taking place
        self.log.info("Performing data load during rebalance")
        doc_loading_tasks = self.perform_workload(self.num_items, self.num_items*2, "create", False)

        self.sleep(30, "Wait before starting a Fusion rebalance")

        # Perform a Fusion Rebalance
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1,
                                              wait_for_rebalance_to_complete=False)
        self.sleep(10, "Wait before checking rebalance progress")
        rebalance_monitor_thread = threading.Thread(target=RebalanceUtil(self.cluster).monitor_rebalance)
        rebalance_monitor_thread.start()

        if stop_fusion_during == "rebalance":
            self.sleep(5, "Wait before stopping Fusion during a rebalance")
            status, content = FusionRestAPI(self.cluster.master).stop_fusion()
            self.log.info(f"Stopping Fusion during rebalance, Status = {status}, Content = {content}")
            self.assertFalse(status, "Stopping Fusion during Fusion rebalance succeeded")

            monitor_fusion_th = threading.Thread(target=self.get_fusion_status_info)
            monitor_fusion_th.start()

        rebalance_monitor_thread.join()

        # Wait for doc load to complete
        for task in doc_loading_tasks:
            self.doc_loading_tm.get_task_result(task)

        self.sleep(5, "Wait before monitoring extent migration")
        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        if stop_fusion_during == "migration":
            self.sleep(30, "Wait before stopping Fusion during extent migration")
            status, content = FusionRestAPI(self.cluster.master).stop_fusion()
            self.log.info(f"Stopping Fusion, Status = {status}, Content = {content}")
            # self.assertFalse(status, "Stopping Fusion during Fusion rebalance succeeded")

            monitor_fusion_th = threading.Thread(target=self.get_fusion_status_info)
            monitor_fusion_th.start()

        for th in extent_migration_array:
            th.join()

        self.sleep(30, "Wait after completion of the entire rebalance/migration process")

        self.monitor_fusion_info = False
        monitor_fusion_th.join()


    def test_stop_or_disable_fusion_during_dcp_rebalance(self):

        step_during_rebalance = self.input.param("step_during_rebalance", "stop") #stop/disable

        self.log.info("Verifying that Fusion is enabled initially")
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Status = {status}, Content = {content}")

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Verify that the log store initially contains some data
        ssh = RemoteMachineShellConnection(self.nfs_server)
        o, e = ssh.execute_command(f"du -sh {self.nfs_server_path}")
        self.log.info(f"NFS path DU check, Output = {o}, Error = {e}")
        ssh.disconnect()

        # Perform a DCP rebalance
        self.spare_node = self.cluster.servers[self.nodes_init]
        self.log.info("DCP Rebalance-in starting...")
        rebalance_task = self.task.async_rebalance(
                self.cluster,
                to_add=[self.spare_node],
                check_vbucket_shuffling=False,
                services=["kv"],
                retry_get_process_num=self.retry_get_process_num)

        self.sleep(10, "Wait before stopping/disabling Fusion")
        if step_during_rebalance == "stop":
            status, content = FusionRestAPI(self.cluster.master).stop_fusion()
            self.log.info(f"Stopping Fusion, Status = {status}, Content = {content}")
        elif step_during_rebalance == "disable":
            status, content = FusionRestAPI(self.cluster.master).disable_fusion()
            self.log.info(f"Disabling Fusion, Status = {status}, Content = {content}")

        monitor_fusion_th = threading.Thread(target=self.get_fusion_status_info)
        monitor_fusion_th.start()

        self.task_manager.get_task_result(rebalance_task)
        self.assertTrue(rebalance_task.result, "DCP Rebalance failed")

        self.sleep(30, "Wait before stopping monitoring threads")
        self.monitor_fusion_info = False
        monitor_fusion_th.join()


    def test_create_new_buckets_after_stopping_or_disabling(self):

        fusion_state_change = self.input.param("fusion_state_change", "stop") #stop/disable
        new_bucket_count = self.input.param("new_bucket_count", 2)

        self.log.info("Verifying that Fusion is enabled initially")
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Status = {status}, Content = {content}")

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Verify that the log store initially contains some data
        ssh = RemoteMachineShellConnection(self.nfs_server)
        o, e = ssh.execute_command(f"du -sh {self.nfs_server_path}")
        self.log.info(f"NFS path DU check, Output = {o}, Error = {e}")
        ssh.disconnect()

        if fusion_state_change == "stop":
            status, content = FusionRestAPI(self.cluster.master).stop_fusion()
            self.log.info(f"Stopping Fusion, Status = {status}, Content = {content}")
            self.assertTrue(status, "Stopping Fusion failed")
        elif fusion_state_change == "disable":
            status, content = FusionRestAPI(self.cluster.master).disable_fusion()
            self.log.info(f"Disabling Fusion, Status = {status}, Content = {content}")
            self.assertTrue(status, "Disabling Fusion failed")

        self.sleep(10, "Wait before fetching status info")
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Status = {status}, Content = {content}")

        # Create new bucket/s
        for i in range(new_bucket_count):
            bucket_name = "new_bucket" + str(i+1)
            self.log.info("Creating bucket: ")
            self.bucket_util.create_default_bucket(
                    self.cluster,
                    bucket_type=self.bucket_type,
                    ram_quota=self.bucket_ram_quota,
                    replica=self.num_replicas,
                    storage=self.bucket_storage,
                    bucket_name=bucket_name)
        self.bucket_util.print_bucket_stats(self.cluster)

        new_buckets = list()
        old_buckets = list()
        for bucket in self.cluster.buckets:
            if "new_bucket" in bucket.name:
                new_buckets.append(bucket)
            else:
                old_buckets.append(bucket)

        self.log.info("Creating clients for new buckets")
        for bucket in new_buckets:
            SiriusCouchbaseLoader.create_clients_in_pool(
                self.cluster.master,
                self.cluster.master.rest_username,
                self.cluster.master.rest_password,
                bucket.name,
                req_clients=5)
        # Override Fusion default settings
        for bucket in new_buckets:
            self.change_fusion_settings(bucket, upload_interval=self.fusion_upload_interval,
                                        checkpoint_interval=self.fusion_log_checkpoint_interval,
                                        logstore_frag_threshold=self.logstore_frag_threshold)

        self.sleep(30, "Wait before enabling Fusion")
        enable_fusion_th = threading.Thread(target=self.enable_fusion, args=[])
        enable_fusion_th.start()

        monitor_fusion_th = threading.Thread(target=self.get_fusion_status_info)
        monitor_fusion_th.start()

        # Wait until Fusion is enabled
        enable_fusion_th.join()

        self.log.info("Starting data workload on existing buckets")
        workload_th1 = threading.Thread(target=self.perform_workload, args=[self.num_items, self.num_items*2, "create", True, old_buckets])
        workload_th1.start()
        self.sleep(20, "Wait before starting workloads on new buckets")
        workload_th2 = threading.Thread(target=self.perform_workload, args=[0, self.num_items, "create", True, new_buckets])
        workload_th2.start()

        workload_th1.join()
        workload_th2.join()

        self.sleep(30, "Wait before stopping monitoring threads")
        self.monitor_fusion_info = False
        monitor_fusion_th.join()


    def get_fusion_status_info(self, duration=3600):

        end_time = time.time() + duration
        self.monitor_fusion_info = True

        while self.monitor_fusion_info and time.time() < end_time:

            status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
            self.log.info(f"Status = {status}, Content = {content}")

            bucket_du = dict()
            ssh = RemoteMachineShellConnection(self.nfs_server)

            for bucket in self.cluster.buckets:
                try:
                    bucket_uuid = self.get_bucket_uuid(bucket.name)
                    log_store_bucket_path = os.path.join(self.nfs_server_path, "kv", bucket_uuid)
                    du_cmd = f"du -sh {log_store_bucket_path}"
                    o, e = ssh.execute_command(du_cmd)
                    bucket_du[bucket.name] = o[0].split("\t")[0]
                except Exception as ex:
                    self.log.info(f"Exception = {ex}, O = {o}, E = {e}")

            ssh.disconnect()
            self.log.info(f"Log store Bucket DU = {bucket_du}")
            time.sleep(5)

    def perform_workload(self, start, end, doc_op="create", wait=True, buckets=None):

        self.reset_doc_params(doc_ops=doc_op)
        if doc_op == "create":
            self.create_start = start
            self.create_end = end
            self.num_items = self.create_end
        elif doc_op == "update":
            self.update_start = start
            self.update_end = end

        doc_loading_tasks, _ = self.java_doc_loader(wait=False,
                                                    skip_default=self.skip_load_to_default_collection,
                                                    ops_rate=20000, doc_ops=doc_op,
                                                    monitor_ops=False,
                                                    buckets=buckets)
        if wait:
            for task in doc_loading_tasks:
                self.doc_loading_tm.get_task_result(task)
        else:
            return doc_loading_tasks
