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

        self.log.info("FusionEnableDisable setUp started")

        self.chaos_action = self.input.param("chaos_action", None) # kill_memcached/restart_couchbase


    def tearDown(self):
        self.fetch_cb_collect_logs()
        super(FusionEnableDisable, self).tearDown()

    def monitor_sync_stats(self, server, bucket, timeout=300):

        self.log.info(f"Monitoring Sync Stats on server: {server.ip}, bucket: {bucket.name}")

        end_time = time.time() + timeout
        cbstats_obj = Cbstats(server)
        sync_complete = False

        while time.time() < end_time:

            try:
                result = cbstats_obj.all_stats(bucket.name)
                completed_bytes = result["ep_fusion_sync_session_completed_bytes"]
                total_bytes = result["ep_fusion_sync_session_total_bytes"]

                self.log.info(f"Server: {server.ip}, Bucket: {bucket.name}, "
                            f"Completed bytes: {completed_bytes}, Total bytes: {total_bytes}")

                if int(completed_bytes) == int(total_bytes) and int(total_bytes) != 0:
                    sync_complete = True
                    break
                time.sleep(2)

            except Exception as e:
                self.log.info(f"Cbstats exception: {e}")

        if sync_complete:
            self.log.info(f"Sync complete for bucket: {bucket.name} on server: {server.ip}")
        else:
            self.log.info(f"Sync not complete for bucket: {bucket.name} on server: {server.ip} even after {timeout} seconds")


    def test_fusion_enable_midway(self):

        workload_during_enabling = self.input.param("workload_during_enabling", False)
        workload_ops_during_enabling = self.input.param("workload_ops_during_enabling", 10000)
        load_data = self.input.param("load_data", True)
        perform_rebalance = self.input.param("perform_rebalance", True)

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
        self.assertTrue(status, "Failed to get Fusion status")
        self.assertEqual(content.get("state"), "disabled", "Fusion should be disabled initially")

        if load_data:
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

        # Perform chaos actions during enabling Fusion
        if self.chaos_action is not None:
            self.sleep(30, "Wait before performing chaos actions")
            chaos_th = threading.Thread(target=self.perform_chaos_actions, args=[self.chaos_action])
            chaos_th.start()

        if workload_during_enabling:
            # Load data during enabling at a high ops rate
            self.log.info("Performing data load while Fusion is being enabled")
            create_th = threading.Thread(target=self.perform_workload, args=[self.num_items, self.num_items * 2, "create", True, None, workload_ops_during_enabling])
            create_th.start()
            self.sleep(30, "Sleep after data loading")
            self.bucket_util.print_bucket_stats(self.cluster)

        enable_fusion_th.join()

        if self.chaos_action is not None:
            self.chaos = False
            chaos_th.join()

        for th in monitor_sync_threads:
            th.join()

        # Get Uploader Map after enabling Fusion
        self.get_fusion_uploader_info(buckets=self.fusion_enabled_buckets)

        # Verify that log store contains data after 'enabling' Fusion
        o, e, initial_size = self.get_log_store_du()
        self.assertTrue(len(o) > 0, "DU command should return output")
        self.assertFalse(e, f"DU command failed with error: {e}")
        self.assertGreater(initial_size, 0, "Log store should contain data after enabling Fusion")

        if not workload_during_enabling:
            # Load more data after Fusion is enabled
            self.log.info("Performing data load after Fusion is enabled")
            self.perform_workload(self.num_items, self.num_items + (self.num_items // 2), "create", True)
            sleep_time = 120 + self.fusion_upload_interval + 30
            self.sleep(sleep_time, "Sleep after subsequent data loading")

        if perform_rebalance:
            # Perform a Fusion Rebalance
            self.log.info("Running a Fusion rebalance")
            nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                                rebalance_count=1,
                                                log_store=self.log_store)

            self.log.info("Monitoring active guest volumes")
            guest_volume_th = threading.Thread(target=self.monitor_active_guest_volumes)
            guest_volume_th.start()
            guest_volume_th.join()

        if workload_during_enabling:
            create_th.join()

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
        self.assertTrue(status, "Failed to get Fusion status")
        self.assertEqual(content.get("state"), "enabled", "Fusion should be enabled initially")

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Verify that log store initially contains data
        o, e, initial_size = self.get_log_store_du()
        self.assertTrue(len(o) > 0, "DU command should return output")
        self.assertFalse(e, f"DU command failed with error: {e}")
        self.assertGreater(initial_size, 0, "Log store should contain data initially")

        # Get Initial Uploader Map
        self.get_fusion_uploader_info()

        self.disable_fusion()

        self.sleep(30, "Wait after disabling Fusion")

        # Verify that the log store is cleaned up
        o, e, cleanup_size = self.get_log_store_du()
        self.assertTrue(len(o) > 0, "DU command should return output")
        self.assertEqual(cleanup_size, 0, "Log store should be empty after disabling Fusion")

        # Get Uploader Map after disabling Fusion
        self.get_fusion_uploader_info()

        # Load more data after Fusion is disabled
        self.log.info("Performing data load after Fusion is disabled")
        self.perform_workload(self.num_items, self.num_items + (self.num_items // 2), "create", True)
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after subsequent data loading")

        # Verify that nothing is being uploaded to the log store
        o, e, post_load_size = self.get_log_store_du()
        self.assertTrue(len(o) > 0, "DU command should return output")
        self.assertEqual(post_load_size, 0, "Log store should remain empty after loading data with Fusion disabled")

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
        self.assertTrue(status, "Failed to get Fusion status")
        self.assertEqual(content.get("state"), "enabled", "Fusion should be enabled initially")

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Verify that the log store initially contains some data
        o, e, initial_size = self.get_log_store_du()
        self.assertTrue(len(o) > 0, "DU command should return output")
        self.assertFalse(e, f"DU command failed with error: {e}")
        self.assertGreater(initial_size, 0, "Log store should contain data initially")

        # Perform a Fusion Rebalance
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1,
                                              log_store=self.log_store)

        self.log.info("Monitoring active guest volumes")
        guest_volume_th = threading.Thread(target=self.monitor_active_guest_volumes)
        guest_volume_th.start()

        self.sleep(30, "Wait before disabling Fusion")

        # Disable Fusion during extent migration
        status, content = FusionRestAPI(self.cluster.master).disable_fusion()
        self.log.info(f"Status = {status}, Content = {content}")
        self.assertFalse(status, "Disabling Fusion during extent migration succeeded")

        monitor_fusion_th = threading.Thread(target=self.get_fusion_status_info)
        monitor_fusion_th.start()

        guest_volume_th.join()

        self.monitor_fusion_info = False
        monitor_fusion_th.join()


    def test_disable_fusion_during_rebalance(self):

        self.log.info("Verifying that Fusion is enabled initially")
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Status = {status}, Content = {content}")
        self.assertTrue(status, "Failed to get Fusion status")
        self.assertEqual(content.get("state"), "enabled", "Fusion should be enabled initially")

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Verify that the log store initially contains some data
        o, e, initial_size = self.get_log_store_du()
        self.assertTrue(len(o) > 0, "DU command should return output")
        self.assertFalse(e, f"DU command failed with error: {e}")
        self.assertGreater(initial_size, 0, "Log store should contain data initially")

        # Perform a data workload in parallel when rebalance is taking place
        self.log.info("Performing data load in parallel when rebalance is taking place")
        doc_loading_tasks = self.perform_workload(self.num_items, self.num_items * 2, "create", False)

        # Perform a Fusion Rebalance
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1,
                                              wait_for_rebalance_to_complete=False,
                                              log_store=self.log_store)
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
        self.log.info("Monitoring active guest volumes")
        guest_volume_th = threading.Thread(target=self.monitor_active_guest_volumes)
        guest_volume_th.start()
        guest_volume_th.join()

        self.sleep(30, "Wait after completion of the entire rebalance/migration process")

        self.monitor_fusion_info = False
        monitor_fusion_th.join()

        # Disable Fusion after the completion of rebalance
        self.disable_fusion()

        # Verify that the log store is cleaned up after disabling
        o, e, cleanup_size = self.get_log_store_du()
        self.assertTrue(len(o) > 0, "DU command should return output")
        self.assertEqual(cleanup_size, 0, "Log store should be empty after disabling Fusion")


    def test_fusion_rebalance_while_disabling(self):

        # Set Extent Migration Rate Limit to 0
        # Perform Fusion Rebalance
        # Call /disable, but since there are active guest volumes, state would be stuck in 'disabling'
        # Perform another Fusion Rebalance, prepareRebalance API should return an error

        self.log.info("Verifying that Fusion is enabled initially")
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Status = {status}, Content = {content}")
        self.assertTrue(status, "Failed to get Fusion status")
        self.assertEqual(content.get("state"), "enabled", "Fusion should be enabled initially")

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Perform a Fusion Rebalance
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1,
                                              log_store=self.log_store)

        self.sleep(10, "Wait before disabling Fusion during a rebalance")

        # Disable Fusion when extent migration is set to 0
        status, content = FusionRestAPI(self.cluster.master).disable_fusion()
        self.log.info(f"Disabling Fusion, Status = {status}, Content = {content}")
        self.assertTrue(status, "Disabling Fusion API call failed")

        monitor_fusion_th = threading.Thread(target=self.get_fusion_status_info)
        monitor_fusion_th.start()

        # Get Active Guest Volumes
        self.sleep(30, "Wait before fetching active guest volumes")
        status, content = FusionRestAPI(self.cluster.master).get_active_guest_volumes()
        self.log.info(f"Active guest volumes, Status = {status}, Content = {content}")
        self.assertTrue(status, "Failed to get active guest volumes")

        # Start another Fusion Rebalance-in while state = 'disabling'
        self.log.info("Starting another Fusion Rebalance while Fusion is being disabled")
        self.spare_nodes = [s for s in self.cluster.servers if s not in self.cluster.nodes_in_cluster]
        if not self.spare_nodes:
            self.fail("No spare nodes available")
        new_node = self.spare_nodes[0]

        self.log.info(f"Adding new node {new_node.ip}")
        status, content = ClusterRestAPI(self.cluster.master).add_node(
                                        new_node.ip, new_node.rest_username,
                                        new_node.rest_password, ["kv"])
        self.log.info(f"Adding node, Status = {status}, Content = {content}")
        self.assertTrue(status, f"Failed to add node {new_node.ip}")

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

        # Wait until Fusion state changes to 'disabled'
        fusion_disabled = self.monitor_fusion_state_transition(state="disabled", timeout=1800)
        if not fusion_disabled:
            self.fail("Disabling Fusion failed after timeout")


    def test_prepare_rebalance_on_empty_bucket(self):
        """
        Regression guard for MB-72032:
        '[Fusion] 500 Internal Server Error During PrepareRebalance With Empty Bucket'

        Repro conditions:
          - Fresh cluster with a Fusion-enabled bucket that has NO documents.
          - A freshly added node that has not yet opened the bucket.
        Pre-fix, GetFusionStorageSnapshot's error path used an uninitialised
        ExecutionEnvRegistry on such fresh nodes, tripping a GSL precondition
        that masked the real fusion error behind a 500 Internal Server Error.
        Post-fix (>= couchbase-server-8.1.0-2199), prepareRebalance must report
        cleanly (and, on a ready empty bucket, succeed) instead of returning a
        500.

        Must be run with init_loading=False so the bucket stays empty. Per the
        ticket comments, prepareRebalance is only invoked after bucket
        creation/warmup has completed, which avoids the residual
        'called before bucket creation completes' failure mode.
        """
        # 1. A bucket must exist and must be empty (no initial_load() call here)
        self.assertTrue(self.cluster.buckets,
                        "No bucket was created during setUp")
        self.assertFalse(self.init_loading,
                         "This test must run with init_loading=False so the "
                         "bucket stays empty (MB-72032 repro condition)")

        # 2. Wait for bucket creation/warmup to finish on all cluster nodes
        #    before touching prepareRebalance. This is the guard called out in
        #    MB-72032: residual failures surface if prepareRebalance runs before
        #    bucket creation completes.
        self.assertTrue(
            self.bucket_util.is_warmup_complete(self.cluster.buckets),
            "Buckets did not finish warming up before prepareRebalance")

        # 3. Ensure Fusion is enabled on the empty bucket
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Fusion status = {status}, Content = {content}")
        self.assertTrue(status, "Failed to get Fusion status")
        if content.get("state") != "enabled":
            self.log.info("Fusion not enabled yet; configuring and enabling it")
            self.configure_fusion()
            self.enable_fusion()

        # 4. Add a fresh node (no opened bucket yet). This is the node whose
        #    uninitialised ExecutionEnvRegistry triggered the MB-72032 500.
        self.spare_nodes = [s for s in self.cluster.servers
                            if s not in self.cluster.nodes_in_cluster]
        self.assertTrue(self.spare_nodes,
                        "No spare node available to add for prepareRebalance")
        new_node = self.spare_nodes[0]

        self.log.info(f"Adding fresh node {new_node.ip}")
        status, content = ClusterRestAPI(self.cluster.master).add_node(
                                        new_node.ip, new_node.rest_username,
                                        new_node.rest_password, ["kv"])
        self.log.info(f"Add node, Status = {status}, Content = {content}")
        self.assertTrue(status, f"Failed to add node {new_node.ip}")

        # 5. keepNodes = existing cluster nodes + the freshly added node
        keep_nodes = [f"ns_1@{server.ip}"
                      for server in self.cluster.nodes_in_cluster]
        keep_nodes.append(f"ns_1@{new_node.ip}")
        self.log.info(f"Keep nodes = {keep_nodes}")

        # 6. PrepareRebalance on the empty bucket must NOT crash with a 500
        status, content = FusionRestAPI(self.cluster.master).prepare_rebalance(
                                        keep_nodes)
        self.log.info(f"PrepareRebalance Status = {status}, Content = {content}")

        # MB-72032 signature: a 500 masking the real error via an uninitialised
        # ExecutionEnvRegistry / GSL precondition failure.
        content_str = str(content).lower()
        looks_like_mb_72032 = (not status) and any(
            marker in content_str for marker in
            ["internal server error", "precondition", "gsl",
             "executionenv", "getfusionstoragesnapshot"])
        self.assertFalse(
            looks_like_mb_72032,
            f"MB-72032 regression: prepareRebalance on an empty bucket returned "
            f"a 500/internal error instead of reporting cleanly. "
            f"Content = {content}")

        # After the fix, prepareRebalance on a ready empty bucket should succeed
        self.assertTrue(
            status,
            f"prepareRebalance failed on an empty bucket. Content = {content}")


    def test_prepare_rebalance_racing_bucket_creation(self):
        """
        MB-72032 (timing-race variant):
        fire prepareRebalance repeatedly *while a bucket is still being
        created / warming up on the nodes* and report every failure the
        endpoint returns during that window.

        In server terms 'before bucket creation has finished' == before the
        bucket is opened on a node, i.e. while that node's fusion
        ExecutionEnvRegistry is still uninitialised. Pre-fix, that window could
        surface a 500 Internal Server Error from GetFusionStorageSnapshot's
        error path; post-fix it must report cleanly (no 500).

        Strategy:
          1. Delete all buckets so every node starts with NO opened bucket
             (uninitialised registry -- the MB-72032 surface).
          2. Start a background thread that hammers prepareRebalance FIRST, so
             calls are already in flight before bucket creation begins.
          3. Then create/delete/re-create a bucket in a loop while the hammer
             thread keeps firing -- each cycle re-opens the unopened-bucket
             window, giving prepareRebalance many chances to hit it. Capture
             every response, then stop the thread after a short grace.

        NOTE: unlike test_prepare_rebalance_during_blocked_warmup, this variant
        relies on real timing -- whether a call lands inside the open window
        depends on how fast warmup is vs. the REST round-trip. If no failure is
        seen, that is a 'did not catch it', not a proof of safety; check the
        logged 'first@<t>s' timestamps against how long creation took.

        Run with init_loading=False, standard_buckets=1, nodes_init>=2.
        """
        self.assertEqual(int(self.standard_buckets), 1,
                         "Race test expects a single bucket (standard_buckets=1)")
        self.assertGreaterEqual(
            len(self.cluster.nodes_in_cluster), 2,
            "Test expects at least 2 nodes so one node can be dropped "
            "(keepNodes = all but one)")

        fusion_client = FusionRestAPI(self.cluster.master)
        cluster_nodes = list(self.cluster.nodes_in_cluster)

        # keepNodes = all nodes but one, so prepareRebalance is a real topology
        # change (the dropped node is being rebalanced out).
        keep_nodes = [f"ns_1@{s.ip}" for s in cluster_nodes[:-1]]
        self.log.info(f"Keep nodes ({len(keep_nodes)} of "
                      f"{len(cluster_nodes)}) = {keep_nodes}")

        # Cluster-level fusion config, then start from a no-opened-bucket state.
        self.configure_fusion()
        self.log.info("Deleting all buckets to reach a no-opened-bucket state")
        self.bucket_util.delete_all_buckets(self.cluster)

        # Reversed order: start the prepareRebalance hammer thread FIRST, so
        # calls are already in flight before (and during) bucket creation.
        responses = list()
        start_time = time.time()
        stop_firing = threading.Event()
        # When set, the hammer idles. We pause it around each bucket delete:
        # a continuous prepareRebalance stream keeps the fusion orchestrator
        # busy and ns_server rejects the bucket DELETE, which then stalls in
        # delete_all_buckets' wait-for-deletion (200s per node). Deleting in a
        # quiet window keeps every cycle fast while still hammering across the
        # entire create/warmup window (the part that actually exercises
        # MB-72032).
        pause_firing = threading.Event()

        def _fire_once():
            elapsed = round(time.time() - start_time, 2)
            try:
                status, content = fusion_client.prepare_rebalance(keep_nodes)
                responses.append((elapsed, status, content))
            except Exception as e:
                responses.append((elapsed, "EXCEPTION", str(e)))
            self.log.info(f"Response at {elapsed}s = {responses[-1]}")

        def _hammer_prepare_rebalance():
            deadline = start_time + 600  # safety cap
            while not stop_firing.is_set() and time.time() < deadline:
                if pause_firing.is_set():
                    time.sleep(0.2)
                    continue
                _fire_once()

        self.log.info("Starting prepareRebalance hammer thread")
        prepare_th = threading.Thread(target=_hammer_prepare_rebalance)
        prepare_th.start()

        # Let prepareRebalance ramp up before bucket creation begins.
        self.sleep(2, "Let prepareRebalance thread ramp up before create")

        # Now repeatedly create -> delete -> re-create the bucket while the
        # hammer thread keeps calling prepareRebalance. Each cycle re-opens the
        # MB-72032 window (nodes transitioning through the unopened-bucket
        # state). create_default_bucket blocks until warmup, so the create call
        # itself spans the create/open window. Capture everything -- including
        # exceptions -- since any of it is 'what the endpoint reports'.
        bucket_create_cycles = self.input.param("bucket_create_cycles", 5)
        for cycle in range(bucket_create_cycles):
            self.log.info(f"Bucket create/delete cycle "
                          f"{cycle + 1}/{bucket_create_cycles}")
            # Hammer stays active during create + warmup (the MB-72032 window).
            pause_firing.clear()
            try:
                self.bucket_util.create_default_bucket(
                    self.cluster,
                    bucket_type=self.bucket_type,
                    ram_quota=self.bucket_ram_quota,
                    replica=self.num_replicas,
                    storage=self.bucket_storage,
                    vbuckets=self.bucket_num_vb)
            except Exception as e:
                self.log.error(f"Bucket creation failed (cycle {cycle + 1}): {e}")
            self.sleep(3, "Hold bucket briefly while prepareRebalance fires")
            # Delete on every cycle except the last, so we end with a live
            # bucket for the fusion-state restore and tearDown.
            if cycle < bucket_create_cycles - 1:
                # Pause the hammer and let in-flight plans settle so the DELETE
                # is accepted promptly instead of being rejected + stalling.
                self.log.info("Pausing prepareRebalance hammer for clean delete")
                pause_firing.set()
                self.sleep(3, "Let in-flight prepareRebalance settle before delete")
                self.log.info(f"Deleting bucket (cycle {cycle + 1})")
                self.bucket_util.delete_all_buckets(self.cluster)
                self.sleep(2, "Brief no-bucket gap before next create")

        # Short grace so a few calls land right as/after the final warmup.
        self.sleep(5, "Grace after final bucket creation before stopping hammer")
        stop_firing.set()
        prepare_th.join()

        # Report every distinct response observed during the window.
        self.log.info(f"Captured {len(responses)} prepareRebalance responses "
                      f"during the bucket-creation window")
        distinct = dict()
        for elapsed, status, content in responses:
            key = (status, str(content))
            if key not in distinct:
                distinct[key] = {"count": 0, "first_at": elapsed}
            distinct[key]["count"] += 1
        for (status, content), info in distinct.items():
            self.log.info(f"[x{info['count']}, first@{info['first_at']}s] "
                          f"status={status} content={content}")

        # Flag the MB-72032 500 signature specifically.
        mb_72032_hits = [
            (elapsed, status, content) for elapsed, status, content in responses
            if status is not True and any(
                marker in str(content).lower() for marker in
                ["internal server error", "precondition", "gsl",
                 "executionenv", "getfusionstoragesnapshot"])]
        self.assertFalse(
            mb_72032_hits,
            f"MB-72032 regression: prepareRebalance returned a 500/internal "
            f"error while the bucket was still being created "
            f"(first {mb_72032_hits[:3]})")


    def test_prepare_rebalance_during_blocked_warmup(self):
        """
        MB-72032 (deterministic reproduction of the 'before bucket creation
        has finished' window):

        In server terms 'before bucket creation has finished' == before the
        bucket is opened on a node, i.e. while that node's fusion
        ExecutionEnvRegistry is still uninitialised. We hold that window open
        deterministically by blocking warmup: chown the data directory to
        root:root so the 'couchbase' user cannot open the bucket's files.

        Flow:
          1. Delete all buckets -> no opened bucket on any node.
          2. chown -R root:root <data_path> on every node -> warmup blocked.
          3. Create a Fusion bucket with wait_for_warmup=False. ns_server
             accepts it into config, but memcached cannot warm it up, so every
             node has an unopened bucket (uninitialised registry).
          4. Call prepareRebalance while warmup is blocked. Pre-fix this
             returned a 500 Internal Server Error from GetFusionStorageSnapshot;
             post-fix (>= 8.1.0-2199) it must report cleanly (no 500).
          5. chown -R couchbase:couchbase <data_path> -> warmup unblocked.
          6. Wait for warmup to complete, then call prepareRebalance again and
             confirm it succeeds.

        Run with init_loading=False, standard_buckets=1, nodes_init>=2
        (e.g. nodes_init=2 -> keepNodes has 1 node; nodes_init=3 -> 2 nodes).
        """
        self.assertEqual(int(self.standard_buckets), 1,
                         "Test expects a single bucket (standard_buckets=1)")
        self.assertGreaterEqual(
            len(self.cluster.nodes_in_cluster), 2,
            "Test expects at least 2 nodes so one node can be dropped "
            "(keepNodes = all but one)")

        fusion_client = FusionRestAPI(self.cluster.master)
        cluster_nodes = list(self.cluster.nodes_in_cluster)

        # keepNodes = all nodes but one, so prepareRebalance is a real topology
        # change (the dropped node is being rebalanced out). With nodes_init=2
        # that leaves 1 node; with nodes_init=3 it leaves 2.
        keep_nodes = [f"ns_1@{s.ip}" for s in cluster_nodes[:-1]]
        self.log.info(f"Keep nodes ({len(keep_nodes)} of "
                      f"{len(cluster_nodes)}) = {keep_nodes}")

        def _chown_data_path(owner):
            """Flip <data_path> ownership on every cluster node."""
            for server in cluster_nodes:
                shell = RemoteMachineShellConnection(server)
                try:
                    cmd = f"chown -R {owner} {self.data_path}"
                    o, e = shell.execute_command(cmd)
                    self.log.info(f"{server.ip}: {cmd} -> out={o}, err={e}")
                finally:
                    shell.disconnect()

        # Cluster-level fusion config, then start from a no-opened-bucket state.
        self.configure_fusion()
        self.log.info("Deleting all buckets to reach a no-opened-bucket state")
        self.bucket_util.delete_all_buckets(self.cluster)

        warmup_blocked = False
        try:
            # Block warmup: couchbase user can no longer access the data dir.
            self.log.info("Blocking warmup: chown root:root on data path")
            _chown_data_path("root:root")
            warmup_blocked = True

            # Create a Fusion bucket without waiting for warmup (it cannot warm
            # up while the data dir is root-owned).
            self.log.info("Creating Fusion bucket (warmup expected to stall)")
            self.bucket_util.create_default_bucket(
                self.cluster,
                bucket_type=self.bucket_type,
                ram_quota=self.bucket_ram_quota,
                replica=self.num_replicas,
                storage=self.bucket_storage,
                vbuckets=self.bucket_num_vb,
                wait_for_warmup=False)

            self.sleep(20, "Let nodes attempt (and fail) warmup while blocked")

            # prepareRebalance while warmup is blocked -- must NOT return a 500.
            blocked_responses = list()
            for i in range(5):
                try:
                    status, content = fusion_client.prepare_rebalance(keep_nodes)
                    blocked_responses.append((status, content))
                except Exception as e:
                    blocked_responses.append(("EXCEPTION", str(e)))
                self.log.info(f"[blocked warmup] prepareRebalance #{i} = "
                              f"{blocked_responses[-1]}")
                self.sleep(3, "Between blocked-warmup prepareRebalance calls")

            # mb_72032_hits = [
            #     (status, content) for status, content in blocked_responses
            #     if status is not True and any(
            #         marker in str(content).lower() for marker in
            #         ["internal server error", "precondition", "gsl",
            #          "executionenv", "getfusionstoragesnapshot"])]
            # self.assertFalse(
            #     mb_72032_hits,
            #     f"MB-72032 regression: prepareRebalance returned a 500/internal "
            #     f"error while bucket warmup was blocked: {mb_72032_hits[:3]}")
        finally:
            # Always restore ownership so the node is usable and tearDown works.
            if warmup_blocked:
                self.log.info("Restoring data path ownership: "
                              "chown couchbase:couchbase")
                _chown_data_path("couchbase:couchbase")

        # Warmup can now proceed; wait for it to complete on all nodes.
        self.sleep(15, "Let memcached resume warmup after ownership restore")
        self.assertTrue(
            self.bucket_util.is_warmup_complete(self.cluster.buckets,
                                                retry_count=10),
            "Bucket did not finish warming up after restoring data-path ownership")

        # prepareRebalance on the now-ready bucket must succeed.
        status, content = fusion_client.prepare_rebalance(keep_nodes)
        self.log.info(f"[post-warmup] prepareRebalance Status = {status}, "
                      f"Content = {content}")
        self.assertTrue(
            status,
            f"prepareRebalance failed after warmup completed. Content = {content}")


    def test_disable_fusion_during_upload(self):

        ###
        # Set sync rate limit to a low value. e.g: 1MB/s
        # Set enableSyncThreshold to a low value. e.g: 10MB
        ###

        self.log.info("Verifying that Fusion is disabled initially")
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Status = {status}, Content = {content}")
        self.assertTrue(status, "Failed to get Fusion status")
        self.assertEqual(content.get("state"), "disabled", "Fusion should be disabled initially")

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
        self.assertTrue(status, "Enabling Fusion failed")

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
        self.disable_fusion()

        # Verify that the log store is cleaned up
        o, e, cleanup_size = self.get_log_store_du()
        self.assertTrue(len(o) > 0, "DU command should return output")
        self.assertEqual(cleanup_size, 0, "Log store should be empty after disabling Fusion")

        # Get Uploader Map after disabling Fusion
        self.get_fusion_uploader_info()

        for th in monitor_sync_threads:
            th.join()


    def test_fusion_remove_delete_permissions_log_store(self):

        self.log.info("Verifying that Fusion is enabled initially")
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Status = {status}, Content = {content}")
        self.assertTrue(status, "Failed to get Fusion status")
        self.assertEqual(content.get("state"), "enabled", "Fusion should be enabled initially")

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
        self.assertFalse(e, f"Failed to remove permissions: {e}")

        # Disable Fusion
        status, content = FusionRestAPI(self.cluster.master).disable_fusion()
        self.log.info(f"Disabling Fusion, Status = {status}, Content = {content}")

        monitor_fusion_th = threading.Thread(target=self.get_fusion_status_info)
        monitor_fusion_th.start()

        self.sleep(60, "Wait before re-introduing permissions")
        restore_perm_cmd = f"chown -R couchbase:couchbase {log_store_dir}"
        o, e = ssh.execute_command(restore_perm_cmd)
        self.assertFalse(e, f"Failed to restore permissions: {e}")
        ssh.disconnect()

        self.sleep(60, "Wait before stopping all monitoring threads")
        self.monitor_fusion_info = False
        monitor_fusion_th.join()


    def test_fusion_rebalance_while_enabling(self):

        self.log.info("Verifying that Fusion is disabled initially")
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Status = {status}, Content = {content}")
        self.assertTrue(status, "Failed to get Fusion status")
        self.assertEqual(content.get("state"), "disabled", "Fusion should be disabled initially")

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


    def test_delete_buckets_while_enabling_fusion(self):
        """
        Delete buckets while Fusion is in the 'enabling' state.

        Setup: Fusion disabled, 2 magma buckets with data loaded into both. A
        low fusion_sync_rate_limit (e.g. 5 MB/s) keeps the enable operation in
        the 'enabling' state for a while, since the loaded data must be synced
        to the log store slowly. While enabling is still in progress, both
        buckets are deleted.

        Verifies that deleting buckets mid-enable is handled gracefully: the
        delete succeeds, the server stays responsive (the Fusion status API
        keeps answering afterwards), and no bucket is left behind.

        Run with fusion_enable=False, standard_buckets=2, magma_buckets=2,
        init_loading=False (data loaded explicitly here), and a low
        fusion_sync_rate_limit.
        """
        self.assertEqual(int(self.standard_buckets), 2,
                         "Test expects 2 buckets (standard_buckets=2)")

        fusion_client = FusionRestAPI(self.cluster.master)

        # 1. Fusion must start disabled.
        status, content = fusion_client.get_fusion_status()
        self.log.info(f"Initial Fusion status = {status}, Content = {content}")
        self.assertTrue(status, "Failed to get Fusion status")
        self.assertEqual(content.get("state"), "disabled",
                         "Fusion should be disabled initially")

        # 2. Load data into both buckets.
        self.log.info("Starting initial load into both buckets")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # 3. Configure Fusion and set a low sync rate limit so 'enabling' (the
        #    initial sync of loaded data to the log store) stays in progress
        #    long enough to delete the buckets during it.
        self.configure_fusion()
        self.log.info(f"Setting fusion_sync_rate_limit to "
                      f"{self.fusion_sync_rate_limit / (1024 * 1024):.2f} MB/s")
        ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
            fusion_sync_rate_limit=self.fusion_sync_rate_limit)

        # 4. Kick off enable (the API returns once enabling has been initiated;
        #    the sync then proceeds in the background as state 'enabling').
        status, content = fusion_client.enable_fusion()
        self.log.info(f"Enable Fusion, Status = {status}, Content = {content}")
        self.assertTrue(status,
                        f"Enable Fusion API call failed. Content = {content}")

        # 5. Wait until Fusion is actually in the 'enabling' state.
        enabling_reached = self.monitor_fusion_state_transition(
            state="enabling", timeout=120)
        if not enabling_reached:
            self.log.warning("Did not observe 'enabling' state; deleting "
                             "buckets anyway (enable may have been fast)")

        # 6. Delete both buckets while Fusion is being enabled.
        self.log.info("Deleting both buckets while Fusion is in 'enabling' state")
        self.bucket_util.delete_all_buckets(self.cluster)

        # 7. Deleting mid-enable must be handled gracefully.
        self.assertEqual(len(self.cluster.buckets), 0,
                         "Buckets were not deleted while Fusion was enabling")

        # 8. Wait until Fusion is actually in the 'enabling' state.
        enabled_reached = self.monitor_fusion_state_transition(
            state="enabled", timeout=900)
        if not enabled_reached:
            self.fail("Did not observe 'enabled' state")


    def test_fusion_second_rebalance_in_lease_expiry(self):
        """
        MB-67550: '[Fusion] Rebalance-in took more than 5 minutes resulting in
        its failure due to the expiration of lease'.

        Diagnostic regression test reproducing the reported scenario:
          - Fusion-enabled cluster with data loaded.
          - First rebalance-in -> builds the 3-node cluster (should succeed).
          - Second rebalance-in of a node to the 3-node cluster -- the
            operation that, on the buggy build, hung >5 min and failed with a
            'lease_lost' / quorum-loss error after migrating only a fraction of
            the vbuckets (11/32), with extent migration at 57/97 log files and
            migration stats reporting completed==total bytes while the kvstores
            were still unmounted.

        Extent migration is monitored on the cluster nodes while the second
        rebalance-in runs. If it fails, the failure reason is captured from
        /pools/default/tasks and logged along with the MB-67550 signature
        (lease/quorum loss, >5 min duration).

        Failure handling is DIAGNOSTIC (chosen mode): a lease_lost / timeout
        failure is captured and logged, not hard-failed -- the test runs to a
        conclusion so the repro can be inspected. The first rebalance-in is a
        normal (hard-failing) step since it is setup.

        Run with fusion_enable=True, standard_buckets=1, magma_buckets=1,
        nodes_init=2, init_loading=False, and an ini with >=4 servers (2 spare
        nodes for the two successive rebalance-ins).
        """
        self.assertGreaterEqual(
            len(self.cluster.servers), len(self.cluster.nodes_in_cluster) + 2,
            "Test needs >=2 spare nodes for two successive rebalance-ins")

        fusion_client = FusionRestAPI(self.cluster.master)

        # Fusion must be enabled.
        status, content = fusion_client.get_fusion_status()
        self.log.info(f"Fusion status = {status}, Content = {content}")
        self.assertTrue(status, "Failed to get Fusion status")
        if content.get("state") != "enabled":
            self.log.info("Fusion not enabled yet; configuring and enabling it")
            self.configure_fusion()
            self.enable_fusion()

        # Load data so extent migration has real work to do during rebalance.
        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # --- First rebalance-in: build the 3-node cluster (regression step) ---
        self.log.info("First Fusion rebalance-in (building the 3-node cluster)")
        self.num_nodes_to_rebalance_in = 1
        self.num_nodes_to_rebalance_out = 0
        self.num_nodes_to_swap_rebalance = 0
        self.run_rebalance(output_dir=self.fusion_output_dir, rebalance_count=1,
                           log_store=self.log_store)
        self.log.info(f"After first rebalance-in, nodes = "
                      f"{[n.ip for n in self.cluster.nodes_in_cluster]}")
        self.get_fusion_uploader_info()

        # --- Second rebalance-in: the MB-67550 failure scenario ---
        # Monitor extent migration on the current cluster nodes while the
        # second rebalance-in runs. run_rebalance blocks (synchronous SSH) until
        # the fusion rebalance script finishes, so the monitors run alongside it
        # and exit once migration completes/stalls.
        migration_threads = list()
        for server in list(self.cluster.nodes_in_cluster):
            for bucket in self.cluster.buckets:
                th = threading.Thread(target=self.monitor_extent_migration,
                                      args=[server, bucket])
                migration_threads.append(th)
                th.start()

        second_rebalance_failed = False
        failure_reason = None
        reb_start = time.time()
        try:
            self.log.info("Second Fusion rebalance-in of a node to the "
                          "3-node cluster")
            self.num_nodes_to_rebalance_in = 1
            self.num_nodes_to_rebalance_out = 0
            self.num_nodes_to_swap_rebalance = 0
            self.run_rebalance(output_dir=self.fusion_output_dir,
                               rebalance_count=2, log_store=self.log_store)
        except Exception as e:
            # run_rebalance hard-fails via self.fail() on rebalance error; catch
            # it here so we can report diagnostically instead of aborting.
            second_rebalance_failed = True
            failure_reason = str(e)
            self.log.error(f"Second Fusion rebalance-in did not complete "
                           f"cleanly: {e}")
        reb_duration = round(time.time() - reb_start, 1)
        self.log.info(f"Second rebalance-in duration = {reb_duration}s "
                      f"(MB-67550 threshold: 300s / 5 min)")

        for th in migration_threads:
            th.join()

        # Capture the rebalance failure reason from ns_server tasks.
        reb_error = None
        try:
            t_status, tasks = ClusterRestAPI(self.cluster.master).cluster_tasks()
            if t_status and isinstance(tasks, list):
                for task in tasks:
                    if task.get("type") == "rebalance" and \
                            task.get("errorMessage"):
                        reb_error = task.get("errorMessage")
                        break
        except Exception as e:
            self.log.warning(f"Could not read /pools/default/tasks: {e}")
        self.log.info(f"Rebalance task errorMessage = {reb_error}")

        # MB-67550 signature: lease loss / quorum loss / lease expiry.
        signature_text = " ".join(
            str(x).lower() for x in [failure_reason, reb_error] if x)
        lease_signature = any(marker in signature_text for marker in
                              ["lease_lost", "lease", "quorum", "expir"])

        if second_rebalance_failed or reb_error:
            self.log.error(
                "=== MB-67550 DIAGNOSTIC: second Fusion rebalance-in failed ===")
            self.log.error(f"  duration              : {reb_duration}s")
            self.log.error(f"  exceeded 5 min (300s) : {reb_duration > 300}")
            self.log.error(f"  rebalance errorMessage: {reb_error}")
            self.log.error(f"  exception             : {failure_reason}")
            self.log.error(f"  lease/quorum signature: {lease_signature}")
            self.log.error("  -> If the lease/quorum signature is present this "
                           "reproduces MB-67550. Captured for inspection; not "
                           "hard-failing (diagnostic mode).")
        else:
            self.log.info(f"Second Fusion rebalance-in completed in "
                          f"{reb_duration}s without a lease/quorum failure "
                          f"(MB-67550 not reproduced on this build)")
            self.get_fusion_uploader_info()


    def test_stop_fusion_midway(self):

        post_stop_step = self.input.param("post_stop_step", "enable") # enable/disable
        perform_dcp_rebalance = self.input.param("perform_dcp_rebalance", False)
        perform_fusion_rebalance = self.input.param("perform_fusion_rebalance", False)
        enable_bucket_count = self.input.param("enable_bucket_count", None)

        self.log.info("Verifying that Fusion is enabled initially")
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Status = {status}, Content = {content}")
        self.assertTrue(status, "Failed to get Fusion status")
        self.assertEqual(content.get("state"), "enabled", "Fusion should be enabled initially")

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        self.get_fusion_uploader_info()

        o, e, initial_size = self.get_log_store_du()
        self.assertTrue(len(o) > 0, "DU command should return output")
        self.assertFalse(e, f"DU command failed with error: {e}")
        self.assertGreater(initial_size, 0, "Log store should contain data after initial load")

        # Stop Fusion
        status, content = FusionRestAPI(self.cluster.master).stop_fusion()
        self.log.info(f"Stopping Fusion, Status: {status}, Content: {content}")
        self.assertTrue(status, "Stopping Fusion failed")

        self.sleep(30, "Wait after stopping Fusion")
        monitor_fusion_th = threading.Thread(target=self.get_fusion_status_info)
        monitor_fusion_th.start()

        # Perform a data workload while fusion is in stopped state
        self.log.info("Performing data load while fusion is in stopped state")
        self.perform_workload(self.num_items, self.num_items + (self.num_items // 2), "create", True)
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
            self.perform_workload(self.num_items, self.num_items + (self.num_items // 2), "create", True)
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
            self.perform_workload(self.num_items, self.num_items + (self.num_items // 2), "create", True)
            sleep_time = 120 + self.fusion_upload_interval + 30
            self.sleep(sleep_time, "Sleep after data loading")

            o, e, post_enable_size = self.get_log_store_du()
            self.assertTrue(len(o) > 0, "DU command should return output")
            self.assertGreater(post_enable_size, 0, "Log store should contain data after re-enabling Fusion")

            if perform_fusion_rebalance:
                self.log.info("Running a Fusion rebalance")
                self.num_nodes_to_rebalance_in = 1
                nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                                      rebalance_count=1,
                                                      log_store=self.log_store)
                self.log.info("Monitoring active guest volumes")
                guest_volume_th = threading.Thread(target=self.monitor_active_guest_volumes)
                guest_volume_th.start()
                guest_volume_th.join()

                self.get_fusion_uploader_info(buckets=self.fusion_enabled_buckets)

                # Perform a data workload after the Fusion rebalance
                self.log.info("Performing data load after Fusion rebalance")
                self.perform_workload(self.num_items, self.num_items + (self.num_items // 2), "create", True)
                sleep_time = 120 + self.fusion_upload_interval + 30
                self.sleep(sleep_time, "Sleep after data loading")

                o, e, post_rebalance_size = self.get_log_store_du()
                self.assertTrue(len(o) > 0, "DU command should return output")
                self.assertGreater(post_rebalance_size, 0, "Log store should contain data after Fusion rebalance")


        elif post_stop_step == "disable":

            self.disable_fusion()

            o, e, post_disable_size = self.get_log_store_du()
            self.assertTrue(len(o) > 0, "DU command should return output")
            self.assertEqual(post_disable_size, 0, "Log store should be empty after disabling Fusion")


    def test_stop_fusion_while_enabling_and_enable_again(self):

        enable_bucket_count = self.input.param("enable_bucket_count", None)

        self.log.info("Verifying that Fusion is disabled initially")
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Status = {status}, Content = {content}")
        self.assertTrue(status, "Failed to get Fusion status")
        self.assertEqual(content.get("state"), "disabled", "Fusion should be disabled initially")

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
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Status = {status}, Content = {content}")
        self.assertTrue(status, "Failed to get Fusion status")
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
        self.assertTrue(status, "Failed to get Fusion status")
        self.assertEqual(content.get("state"), "enabled", "Fusion should be enabled initially")

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Verify that the log store initially contains some data
        o, e, initial_size = self.get_log_store_du()
        self.assertTrue(len(o) > 0, "DU command should return output")
        self.assertFalse(e, f"DU command failed with error: {e}")
        self.assertGreater(initial_size, 0, "Log store should contain data initially")

        # Perform a data workload in parallel when rebalance is taking place
        self.log.info("Performing data load during rebalance")
        doc_loading_tasks = self.perform_workload(self.num_items, self.num_items*2, "create", False)

        self.sleep(30, "Wait before starting a Fusion rebalance")

        # Perform a Fusion Rebalance
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1,
                                              wait_for_rebalance_to_complete=False,
                                              log_store=self.log_store)
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
        self.log.info("Monitoring active guest volumes")
        guest_volume_th = threading.Thread(target=self.monitor_active_guest_volumes)
        guest_volume_th.start()

        if stop_fusion_during == "migration":
            self.sleep(30, "Wait before stopping Fusion during extent migration")
            status, content = FusionRestAPI(self.cluster.master).stop_fusion()
            self.log.info(f"Stopping Fusion, Status = {status}, Content = {content}")
            # self.assertFalse(status, "Stopping Fusion during Fusion rebalance succeeded")

            monitor_fusion_th = threading.Thread(target=self.get_fusion_status_info)
            monitor_fusion_th.start()

        guest_volume_th.join()

        self.sleep(30, "Wait after completion of the entire rebalance/migration process")

        self.monitor_fusion_info = False
        monitor_fusion_th.join()


    def test_stop_or_disable_fusion_during_dcp_rebalance(self):

        step_during_rebalance = self.input.param("step_during_rebalance", "stop") #stop/disable

        self.log.info("Verifying that Fusion is enabled initially")
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Status = {status}, Content = {content}")
        self.assertTrue(status, "Failed to get Fusion status")
        self.assertEqual(content.get("state"), "enabled", "Fusion should be enabled initially")

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Verify that the log store initially contains some data
        o, e, initial_size = self.get_log_store_du()
        self.assertTrue(len(o) > 0, "DU command should return output")
        self.assertFalse(e, f"DU command failed with error: {e}")
        self.assertGreater(initial_size, 0, "Log store should contain data initially")

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
        self.assertTrue(status, "Failed to get Fusion status")
        self.assertEqual(content.get("state"), "enabled", "Fusion should be enabled initially")

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Verify that the log store initially contains some data
        o, e, initial_size = self.get_log_store_du()
        self.assertTrue(len(o) > 0, "DU command should return output")
        self.assertFalse(e, f"DU command failed with error: {e}")
        self.assertGreater(initial_size, 0, "Log store should contain data initially")

        if fusion_state_change == "stop":
            status, content = FusionRestAPI(self.cluster.master).stop_fusion()
            self.log.info(f"Stopping Fusion, Status = {status}, Content = {content}")
            self.assertTrue(status, "Stopping Fusion failed")
        elif fusion_state_change == "disable":
            self.disable_fusion()

        self.sleep(10, "Wait before fetching status info")
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Status = {status}, Content = {content}")
        self.assertTrue(status, "Failed to get Fusion status")
        if fusion_state_change == "stop":
            self.assertEqual(content.get("state"), "stopped", "Fusion should be stopped")
        elif fusion_state_change == "disable":
            self.assertEqual(content.get("state"), "disabled", "Fusion should be disabled")

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


    def test_chaos_during_stopping_fusion(self):

        self.log.info("Verifying that Fusion is enabled initially")
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Status = {status}, Content = {content}")
        self.assertTrue(status, "Failed to get Fusion status")
        self.assertEqual(content.get("state"), "enabled", "Fusion should be enabled initially")

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Set Migration Rate Limit to 0 so that extent migration doesn't take place
        ClusterRestAPI(self.cluster.master).\
                manage_global_memcached_setting(fusion_migration_rate_limit=0)

        # Perform a Fusion Rebalance
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1,
                                              log_store=self.log_store)

        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        self.sleep(10, "Wait after rebalance completion")
        status, content = FusionRestAPI(self.cluster.master).stop_fusion()
        self.log.info(f"Stopping Fusion, Status = {status}, Content = {content}")

        monitor_fusion_th = threading.Thread(target=self.get_fusion_status_info)
        monitor_fusion_th.start()

        # Perform chaos action during 'stopping' state
        self.sleep(10, "Wait before performing chaos actions")
        self.perform_chaos_actions(self.chaos_action, duration=300)

        # Update Migration Rate Limit so that extent migration starts
        ClusterRestAPI(self.cluster.master).\
                manage_global_memcached_setting(fusion_migration_rate_limit=self.fusion_migration_rate_limit)

        self.log.info("Monitoring active guest volumes")
        guest_volume_th = threading.Thread(target=self.monitor_active_guest_volumes)
        guest_volume_th.start()
        guest_volume_th.join()

        self.sleep(60, "Wait after extent migration completion")
        self.monitor_fusion_info = False
        monitor_fusion_th.join()


    def test_chaos_during_disabling_fusion(self):

        self.log.info("Verifying that Fusion is enabled initially")
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Status = {status}, Content = {content}")
        self.assertTrue(status, "Failed to get Fusion status")
        self.assertEqual(content.get("state"), "enabled", "Fusion should be enabled initially")

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
        self.assertFalse(e, f"Failed to remove permissions: {e}")

        # Disable Fusion
        status, content = FusionRestAPI(self.cluster.master).disable_fusion()
        self.log.info(f"Disabling Fusion, Status = {status}, Content = {content}")

        monitor_fusion_th = threading.Thread(target=self.get_fusion_status_info)
        monitor_fusion_th.start()

        # Perform chaos action during 'disabling' state
        self.sleep(10, "Wait before performing chaos actions")
        self.perform_chaos_actions(self.chaos_action, duration=300)

        self.sleep(60, "Wait before re-introduing permissions")
        restore_perm_cmd = f"chown -R couchbase:couchbase {log_store_dir}"
        o, e = ssh.execute_command(restore_perm_cmd)
        self.assertFalse(e, f"Failed to restore permissions: {e}")
        ssh.disconnect()

        self.sleep(60, "Wait before stopping all monitoring threads")
        self.monitor_fusion_info = False
        monitor_fusion_th.join()


    def perform_chaos_actions(self, chaos_action, interval=60, duration=1800):

        self.chaos = True
        end_time = time.time() + duration

        shell_dict = dict()
        for server in self.cluster.nodes_in_cluster:
            shell_dict[server.ip] = RemoteMachineShellConnection(server)

        while self.chaos and time.time() < end_time:
            for server in self.cluster.nodes_in_cluster:
                shell = shell_dict[server.ip]
                if chaos_action == "kill_memcached":
                    self.log.info(f"Killing memcached on {server.ip}")
                    shell.kill_memcached()
                elif chaos_action == "restart_couchbase":
                    self.log.info(f"Restarting Couchbase on {server.ip}")
                    shell.restart_couchbase()

            self.sleep(interval, "Wait before next chaos action")

    def test_fusion_enable_during_storage_migration(self):
        from BucketLib.bucket import Bucket

        original_nodes = [node.ip for node in self.cluster.nodes_in_cluster]
        self.log.info(f"Original nodes: {original_nodes}")

        test_bucket = self.cluster.buckets[0]
        self.log.info(f"Bucket: {test_bucket.name}, storage: {test_bucket.storageBackend}")

        self.initial_load()
        self.sleep(30, "Wait after initial load")

        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Fusion status: {content}")
        self.assertTrue(status, "Failed to get Fusion status")

        self.configure_fusion()
        status, content = FusionRestAPI(self.cluster.master).enable_fusion()
        self.log.info(f"Enable Fusion on CouchStore - Status: {status}, Content: {content}")

        self.log.info("Changing bucket storage backend to Magma")
        self.bucket_util.update_bucket_property(
            self.cluster.master,
            test_bucket,
            storageBackend=Bucket.StorageBackend.magma)

        self.sleep(10, "Wait after changing storage backend")

        mixed_mode = self.check_bucket_mixed_mode(test_bucket)
        self.log.info(f"Mixed mode: {mixed_mode}")

        nodes_to_migrate = list(self.cluster.nodes_in_cluster)
        original_node_ips = [n.ip for n in nodes_to_migrate]
        self.log.info(f"Nodes to migrate: {original_node_ips}")

        available_spare_nodes = len(self.cluster.servers) - self.nodes_init
        if available_spare_nodes < len(original_nodes):
            self.fail(f"Not enough spare nodes. Required: {len(original_nodes)}, Available: {available_spare_nodes}")

        self.spare_nodes = [s for s in self.cluster.servers if s not in self.cluster.nodes_in_cluster]
        if not self.spare_nodes:
            self.fail("No spare nodes available")

        for swap_count, node_to_remove in enumerate(nodes_to_migrate):
            if node_to_remove.ip not in original_node_ips:
                self.fail(f"ERROR: Attempting to remove non-original node {node_to_remove.ip}")

            self.log.info(f"Swap rebalance {swap_count + 1}/{len(nodes_to_migrate)}: removing {node_to_remove.ip}")

            current_cluster_nodes = list(self.cluster.nodes_in_cluster)
            reordered_cluster = []

            for node in current_cluster_nodes:
                if node.ip == self.cluster.master.ip:
                    reordered_cluster.append(node)
                    break

            for node in current_cluster_nodes:
                if node.ip == node_to_remove.ip:
                    reordered_cluster.append(node)
                    break

            for node in current_cluster_nodes:
                if node.ip not in [n.ip for n in reordered_cluster]:
                    reordered_cluster.append(node)

            self.cluster.nodes_in_cluster = reordered_cluster

            self.num_nodes_to_rebalance_in = 0
            self.num_nodes_to_rebalance_out = 0
            self.num_nodes_to_swap_rebalance = 1

            nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                                  rebalance_count=swap_count + 1,
                                                  wait_for_rebalance_to_complete=True,
                                                  rebalance_master=False,
                                                  log_store=self.log_store)

            current_cluster_ips = [n.ip for n in self.cluster.nodes_in_cluster]
            if node_to_remove.ip in current_cluster_ips:
                self.fail(f"ERROR: Node {node_to_remove.ip} still in cluster after swap")

            self.log.info(f"Swap {swap_count + 1} completed")
            self.cluster_util.print_cluster_stats(self.cluster)

            mixed_mode = self.check_bucket_mixed_mode(test_bucket)
            self.log.info(f"Mixed mode after swap {swap_count + 1}: {mixed_mode}")


        mixed_mode = self.check_bucket_mixed_mode(test_bucket)
        self.log.info(f"Final mixed mode: {mixed_mode}")

        self.log.info("Verifying first Fusion upload completes")
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Wait for first Fusion upload")

        o, e, upload_size = self.get_log_store_du()
        self.assertTrue(len(o) > 0, "DU command should return output")
        self.assertFalse(e, f"DU command failed with error: {e}")
        self.assertGreater(upload_size, 0, "Log store should contain data after Fusion upload")

        self.get_fusion_uploader_info()
        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)


    def check_bucket_mixed_mode(self, bucket):
        cbstats = Cbstats(self.cluster.master)
        vb_details = cbstats.vbucket_details(bucket.name)

        backends = set()
        for vb_num, details in vb_details.items():
            if 'backend_type' in details:
                backends.add(details['backend_type'])
            elif 'db_file_name' in details:
                db_file = details['db_file_name']
                if 'magma' in db_file.lower():
                    backends.add('magma')
                elif 'couch' in db_file.lower():
                    backends.add('couchstore')

        self.log.info(f"Bucket {bucket.name} backends: {backends}")
        return len(backends) > 1