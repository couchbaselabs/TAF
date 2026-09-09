import time
import re
import threading
import subprocess
import os
from datetime import datetime
from shell_util.remote_connection import RemoteMachineShellConnection
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_server_rest_util.fusion.fusion_api import FusionRestAPI
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_base import MagmaBaseTest
from cb_tools.cbstats import Cbstats


class FusionUploaderRateLimitTest(MagmaBaseTest, FusionBase):

    def setUp(self):
        super().setUp()
        self.upsert_iterations = self.input.param("upsert_iterations", 2)
        self.monitor_interval = self.input.param("monitor_interval", 5)  # seconds
        self.upload_ops_rate = self.input.param("upload_ops_rate", 20 * 1024 * 1024)  # 20 MB/s default
        self.rate_limit = self.input.param("rate_limit", 10 * 1024 * 1024)  # 10 MB/s default
        self.rate_limit_target = self.input.param("rate_limit_target", "migration")  # "migration" | "download"
        self.rate_limit_toggle_interval = self.input.param("rate_limit_toggle_interval", 30)
        self.enable_memcached_kill = self.input.param("enable_memcached_kill", True)
        self.kill_after_seconds = self.input.param("kill_after_seconds", 30)
        self.num_rebalances = self.input.param("num_rebalances", 3)
        self.rebalance_type = self.input.param("rebalance_type", "in")
        self.rate_limit_toggle_stop = False
        self.log.info(f"[SETUP] monitor_interval={self.monitor_interval}s, "
                      f"upload_ops_rate={self.upload_ops_rate / (1024*1024)}MB/s, "
                      f"rate_limit={self.rate_limit / (1024*1024)}MB/s, "
                      f"rate_limit_target={self.rate_limit_target}")

    def tearDown(self):
        self.rate_limit_toggle_stop = True
        super(FusionUploaderRateLimitTest, self).tearDown()

    def get_stat(self, server, bucket, stat_key):
        cb = Cbstats(server)
        try:
            stats = cb.all_stats(bucket.name)
            if stat_key not in stats:
                raise Exception(f"{stat_key} not found for {bucket.name} on {server.ip}")
            return int(stats[stat_key])
        finally:
            cb.disconnect()

    def monitor_rate_dynamic(self, server, bucket, stat_key, rate_limit, idle_intervals=3, max_time=900):
        last_val = self.get_stat(server, bucket, stat_key)
        last_time = time.time()
        max_rate_bps = 0
        activity_detected = False
        idle_count = 0

        start_time = last_time
        self.log.info(f"[MONITOR] Starting dynamic monitoring of {stat_key} for up to {max_time}s...")
        while True:
            if time.time() - start_time > max_time:
                self.log.warn("Max monitoring time reached, stopping.")
                break

            self.sleep(self.monitor_interval, f"Monitoring {stat_key}")
            cur_val = self.get_stat(server, bucket, stat_key)
            cur_time = time.time()

            delta_bytes = cur_val - last_val
            delta_time = cur_time - last_time

            if delta_time > 0 and delta_bytes >= 0:
                rate_bps = delta_bytes / delta_time
                max_rate_bps = max(max_rate_bps, rate_bps)
                if rate_bps > 0:
                    activity_detected = True
                    idle_count = 0
                else:
                    idle_count += 1

                self.log.info(f"[MONITOR:{stat_key}] Current rate={rate_bps / (1024*1024):.2f} MB/s, "
                              f"Max rate so far={max_rate_bps / (1024*1024):.2f} MB/s, "
                              f"Limit={rate_limit / (1024*1024):.2f} MB/s, "
                              f"Delta bytes={delta_bytes}, Delta time={delta_time:.2f}s, "
                              f"Idle count={idle_count}")

            if idle_count >= idle_intervals:
                self.log.info("[MONITOR] Uploads appear completed (stat stable).")
                break

            last_val = cur_val
            last_time = cur_time

        assert activity_detected, f"No activity detected for {stat_key}"
        assert max_rate_bps <= rate_limit * 1.05, \
            f"{stat_key}: observed {max_rate_bps} > limit {rate_limit}"
        self.log.info(f"[MONITOR] Finished monitoring {stat_key}. Max observed rate={max_rate_bps / (1024*1024):.2f} MB/s")

    def parse_accelerator_download_rate(self, rate_limit):
        """
        Parse the accelerator-cli download-files log captured in
        self.fusion_rebalance_output (written by run_rebalance()) and assert
        the achieved average download throughput stays within rate_limit.

        There is no cbstat for this stage (log store -> guest volumes), so
        this reads accelerator-cli's own timestamped log lines instead of
        polling a stat like monitor_rate_dynamic() does. Assumes a single
        accelerator-cli invocation contributed to the log (i.e. a
        single-node rebalance-in): -rate-limit caps each invocation
        independently, so a combined multi-node total would not need to
        respect the same cap.
        """
        ts_line = re.compile(r'^(?:\[\S+\]\s*)?(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})\s+(.*)$')
        ts_format = "%Y/%m/%d %H:%M:%S"

        start_time = None
        last_complete_time = None
        total_downloaded_bytes = 0
        files_completed = 0

        with open(self.fusion_rebalance_output) as f:
            for line in f:
                match = ts_line.match(line)
                if not match:
                    continue
                timestamp_str, message = match.groups()
                try:
                    timestamp = datetime.strptime(timestamp_str, ts_format)
                except ValueError:
                    continue

                if start_time is None and re.search(r'Files to download: \d+ \(\d+ bytes\)', message):
                    start_time = timestamp
                    continue

                complete_match = re.search(r'Download complete for .+ \((\d+) bytes\)', message)
                if complete_match:
                    total_downloaded_bytes += int(complete_match.group(1))
                    files_completed += 1
                    last_complete_time = timestamp

        self.assertIsNotNone(start_time,
                             "accelerator-cli 'Files to download' line not "
                             f"found in {self.fusion_rebalance_output}")
        self.assertGreater(files_completed, 0,
                           "No 'Download complete' lines found in "
                           f"{self.fusion_rebalance_output} -- accelerator-cli "
                           "did not report downloading any files")

        elapsed_seconds = (last_complete_time - start_time).total_seconds()
        self.assertGreater(elapsed_seconds, 0,
                           "Download completed too quickly to measure a rate "
                           "(log timestamps have 1s resolution) -- increase "
                           "the test's dataset size")

        achieved_rate_bps = total_downloaded_bytes / elapsed_seconds
        self.log.info(f"[DOWNLOAD RATE] {files_completed} files, "
                      f"{total_downloaded_bytes} bytes over {elapsed_seconds:.1f}s "
                      f"= {achieved_rate_bps / (1024*1024):.2f} MB/s "
                      f"(limit={rate_limit / (1024*1024):.2f} MB/s)")
        self.assertLessEqual(achieved_rate_bps, rate_limit * 1.05,
                            f"Achieved download rate {achieved_rate_bps} bytes/sec "
                            f"exceeded limit {rate_limit} bytes/sec")

    def custom_data_load(self):
        total_data_mb = (self.num_items * self.doc_size) / (1024 * 1024)
        self.log.info(f"[DATA LOAD] Loading {self.num_items} docs, {self.doc_size} bytes each "
                      f"= {total_data_mb:.2f} MB total")

        self.create_start = 0
        self.create_end = self.num_items
        self.generate_docs(doc_ops="create")

        self.log.info("[DATA LOAD] Starting java_doc_loader...")
        self.java_doc_loader(generator=self.gen_create,
                             doc_ops="create",
                             process_concurrency=self.process_concurrency,
                             ops_rate=self.ops_rate,
                             skip_default=self.skip_load_to_default_collection,
                             wait=True)
        self.log.info("[DATA LOAD] Data load completed")

    def wait_for_upload_start(self, server, bucket, stat_key, timeout=300):
        self.log.info(f"[WAIT] Waiting for upload to start for {stat_key}...")
        start_time = time.time()
        while time.time() - start_time < timeout:
            val = self.get_stat(server, bucket, stat_key)
            if val > 0:
                return
            self.sleep(self.monitor_interval, "Waiting for upload to start")
        raise AssertionError("Upload did not start in time")

    def test_fusion_upload_rate_limit_default(self):
        """
        Test Fusion uploader respects the configured rate limit in MiB/s.

        Flow:
        1. Start with Fusion disabled (requires fusion_enable=False in the conf)
        2. Load data while Fusion is disabled, so nothing is uploaded yet
        3. Set the global fusion_sync_rate_limit
        4. Enable Fusion - the initial sync of the already loaded data starts here
        5. Monitor the upload rate while Fusion is being enabled and assert it
           never exceeds the configured limit
        """
        bucket = self.cluster.buckets[0]
        self.log.info("[TEST] Starting test_fusion_upload_rate_limit_default...")

        # Fusion must be disabled at the start, so that all the data loaded below
        # is uploaded only once Fusion is enabled
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"[TEST] Fusion status = {status}, content = {content}")
        self.assertTrue(status, "Failed to get Fusion status")
        self.assertEqual(content.get("state"), "disabled",
                         "Fusion should be disabled at the start of the test. "
                         "Run this test with fusion_enable=False")

        # Load test data with Fusion still disabled
        self.custom_data_load()

        # Set global Fusion sync rate limit in bytes (MiB)
        ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
            fusion_sync_rate_limit=self.rate_limit
        )
        self.log.info(f"[TEST] Set global fusion_sync_rate_limit={self.rate_limit / (1024 * 1024):.2f} MiB/s")

        server = self.cluster.master

        # Enable Fusion, uploads of the loaded data start as part of enabling
        self.log.info("[TEST] Enabling Fusion after data load")
        self.configure_fusion()
        enable_fusion_th = threading.Thread(target=self.enable_fusion)
        enable_fusion_th.start()

        try:
            # Wait until upload starts
            self.wait_for_upload_start(server, bucket, "ep_fusion_bytes_synced")

            # Monitor upload rate while Fusion is being enabled,
            # asserting it stays below the MiB limit
            self.monitor_rate_dynamic(server,
                bucket,
                stat_key="ep_fusion_bytes_synced",
                rate_limit=self.rate_limit
            )
        finally:
            enable_fusion_th.join()

        # Fusion should have reached the 'enabled' state by now
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"[TEST] Fusion status after enabling = {status}, content = {content}")
        self.assertTrue(status, "Failed to get Fusion status")
        self.assertEqual(content.get("state"), "enabled",
                         "Fusion should be enabled after the upload completes")

        self.log.info("[TEST] Finished test_fusion_upload_rate_limit_default successfully")

    def test_fusion_extent_migration_rate_limit(self):
        """
        Test Fusion extent migration respects the configured rate limit in MiB/s.

        rate_limit_target=migration (default): throttles fusion_migration_rate_limit
        (guest volumes -> KV node), as below.
        rate_limit_target=download: throttles accelerator-cli's own -rate-limit
        flag (log store -> guest volumes) via run_rebalance() instead, and
        validates the achieved rate from the accelerator's own log.
        """
        bucket = self.cluster.buckets[0]
        self.log.info("[TEST] Starting test_fusion_extent_migration_rate_limit "
                      f"(rate_limit_target={self.rate_limit_target})...")

        if self.rate_limit_target == "download":
            # Load test data
            self.custom_data_load()

            # Run a Fusion rebalance with the accelerator's download step
            # capped via -rate-limit
            self.run_rebalance(output_dir=self.fusion_output_dir, rebalance_count=1,
                               rate_limit=self.rate_limit)

            # Validate the achieved rate from accelerator-cli's own log
            self.parse_accelerator_download_rate(self.rate_limit)
        else:
            # Initially gate extent migration completely
            ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
                fusion_migration_rate_limit=0
            )
            self.log.info("[TEST] Set global fusion_extent_migration_rate_limit=0")

            # Load test data
            self.custom_data_load()

            # Run a Fusion rebalance (migration will be queued/stalled at cap=0)
            nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir, rebalance_count=1)

            # Open the cap to the test's configured limit (bytes/sec)
            ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
                fusion_migration_rate_limit=self.rate_limit
            )

            self.log.info(f"[TEST] Set global fusion_extent_migration_rate_limit="
                          f"{self.rate_limit / (1024 * 1024):.2f} MB/s")

            # Wait until extent migration starts on one of the nodes
            self.wait_for_upload_start(nodes_to_monitor[0], bucket, "ep_fusion_bytes_migrated", timeout=900)

            # Monitor extent migration; assert peak rate ≤ configured limit
            self.monitor_rate_dynamic(
                nodes_to_monitor[0],
                bucket,
                stat_key="ep_fusion_bytes_migrated",
                rate_limit=self.rate_limit
            )

        self.log.info("[TEST] Finished test_fusion_extent_migration_rate_limit successfully")

    def toggle_upload_rate_limit(self, interval, rate_limit_value):
        """Toggle fusion_sync_rate_limit between rate_limit_value and 0"""
        while not self.rate_limit_toggle_stop:
            ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
                fusion_sync_rate_limit=rate_limit_value)
            self.log.info("Set fusion_sync_rate_limit to {0} MB/s".format(rate_limit_value / (1024 * 1024)))
            
            self.sleep(interval, "Wait before toggling rate limit")
            if self.rate_limit_toggle_stop:
                break
            
            ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
                fusion_sync_rate_limit=0)
            self.log.info("Set fusion_sync_rate_limit to 0 MB/s")
            
            self.sleep(interval, "Wait before toggling rate limit")

    def select_rebalance_type(self, rebalance_type):
        """Configure rebalance parameters based on type"""
        self.num_nodes_to_rebalance_in = 0
        self.num_nodes_to_rebalance_out = 0
        self.num_swap_rebalance = 0
        
        if rebalance_type == "in":
            self.num_nodes_to_rebalance_in = 1
        elif rebalance_type == "out":
            self.num_nodes_to_rebalance_out = 1
        elif rebalance_type == "swap":
            self.num_nodes_to_swap_rebalance = 1
        elif rebalance_type == "random":
            num_kv_nodes = len([n for n in self.cluster.nodes_in_cluster if "kv" in n.services])
            if num_kv_nodes < 4:
                self.num_nodes_to_rebalance_in = 1
            else:
                import random
                choice = random.choice(["in", "out", "swap"])
                if choice == "in":
                    self.num_nodes_to_rebalance_in = 1
                elif choice == "out":
                    self.num_nodes_to_rebalance_out = 1
                else:
                    self.num_nodes_to_swap_rebalance = 1
        
        self.log.info("Rebalance type: {0}, in={1}, out={2}, swap={3}".format(
            rebalance_type, self.num_nodes_to_rebalance_in,
            self.num_nodes_to_rebalance_out, self.num_nodes_to_swap_rebalance))

    def test_toggle_upload_rate_limit_with_memcached_kills(self):
        """
        Test upload sync stability with:
        1. Continuous rate limit toggling (10MB/s <-> 0)
        2. Continuous read workload
        3. Periodic memcached kills
        4. Multiple rebalances
        """
        self.log.info("Starting test_toggle_upload_rate_limit_with_memcached_kills")
        
        self.log.info("Starting initial load")
        self.initial_load()
        
        self.log.info("Waiting for initial Fusion upload sync")
        sleep_time = 120 + self.fusion_upload_interval + 60
        self.sleep(sleep_time, "Wait for Fusion upload sync")
        
        self.log.info("Starting continuous rate limit toggle thread")
        toggle_thread = threading.Thread(
            target=self.toggle_upload_rate_limit,
            args=(self.rate_limit_toggle_interval, self.rate_limit))
        toggle_thread.daemon = True
        toggle_thread.start()
        
        self.log.info("Starting continuous read workload")
        read_tasks = self.perform_workload(
            0, self.num_items, doc_op="read",
            wait=False, ops_rate=self.read_ops_rate)
        
        for rebalance_count in range(1, self.num_rebalances + 1):
            self.log.info("=== Rebalance iteration {0}/{1} ===".format(rebalance_count, self.num_rebalances))
            
            self.select_rebalance_type(self.rebalance_type)
            
            self.log.info("Running Fusion rebalance")
            nodes_to_monitor = self.run_rebalance(
                output_dir=self.fusion_output_dir,
                rebalance_count=rebalance_count)
            
            if self.enable_memcached_kill:
                self.log.info("Starting memcached kill loop")
                for kill_iteration in range(10):
                    self.sleep(self.kill_after_seconds, "Wait before killing memcached")
                    
                    for node in self.cluster.nodes_in_cluster:
                        shell = RemoteMachineShellConnection(node)
                        try:
                            self.log.info("Killing memcached on {0}".format(node.ip))
                            shell.kill_memcached()
                        finally:
                            shell.disconnect()
                    
                    self.log.info("Killed memcached iteration {0}/10".format(kill_iteration + 1))
            else:
                self.log.info("Memcached kill disabled, waiting for rebalance progress")
                self.sleep(300, "Wait for rebalance progress without kills")
            
            self.log.info("Monitoring upload sync stats")
            for node in self.cluster.nodes_in_cluster:
                cbstats = Cbstats(node)
                for bucket in self.cluster.buckets:
                    stats = cbstats.all_stats(bucket.name)
                    self.log.info("Node: {0}, Bucket: {1}, ep_fusion_bytes_synced: {2}, ep_fusion_sync_failures: {3}".format(
                        node.ip, bucket.name, stats.get('ep_fusion_bytes_synced', 0),
                        stats.get('ep_fusion_sync_failures', 0)))
                cbstats.disconnect()
        
        self.log.info("Stopping rate limit toggle thread")
        self.rate_limit_toggle_stop = True
        toggle_thread.join(timeout=60)
        
        self.log.info("Waiting for read workload to complete")
        for task in read_tasks:
            self.doc_loading_tm.get_task_result(task)
        
        self.log.info("Final comprehensive stats validation")
        for node in self.cluster.nodes_in_cluster:
            cbstats = Cbstats(node)
            for bucket in self.cluster.buckets:
                stats = cbstats.all_stats(bucket.name)
                sync_failures = int(stats['ep_fusion_sync_failures'])
                data_read_failed = int(stats['ep_data_read_failed'])
                
                self.log.info("Node: {0}, Bucket: {1}, ep_fusion_sync_failures: {2}, ep_data_read_failed: {3}".format(
                    node.ip, bucket.name, sync_failures, data_read_failed))
                
                self.assertEqual(sync_failures, 0,
                    "Sync failures detected on {0} for bucket {1}: {2}".format(
                        node.ip, bucket.name, sync_failures))
                self.assertEqual(data_read_failed, 0,
                    "Data read failures detected on {0} for bucket {1}: {2}".format(
                        node.ip, bucket.name, data_read_failed))
            cbstats.disconnect()
        
        self.log.info("Test completed successfully")