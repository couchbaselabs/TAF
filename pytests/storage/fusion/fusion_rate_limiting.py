import time
import threading
import subprocess
import os
from shell_util.remote_connection import RemoteMachineShellConnection
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
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
        self.log.info(f"[SETUP] monitor_interval={self.monitor_interval}s, "
                      f"upload_ops_rate={self.upload_ops_rate / (1024*1024)}MB/s, "
                      f"rate_limit={self.rate_limit / (1024*1024)}MB/s")

    def tearDown(self):
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
        """
        bucket = self.cluster.buckets[0]
        self.log.info("[TEST] Starting test_fusion_upload_rate_limit_default...")

        # Load test data
        self.custom_data_load()

        # Set global Fusion sync rate limit in bytes (MiB)
        ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
            fusion_sync_rate_limit=self.rate_limit
        )
        self.log.info(f"[TEST] Set global fusion_sync_rate_limit={self.rate_limit / (1024 * 1024):.2f} MiB/s")

        # Idle period before uploads start
        self.sleep(60, "Idle period before uploads start")

        # Override upload interval for test
        self.fusion_upload_interval = 10  # seconds
        self.log.info(f"[TEST] Setting fusion_upload_interval={self.fusion_upload_interval}s")

        # Apply upload interval to all buckets
        for bkt in self.cluster.buckets:
            self.change_fusion_settings(bkt, upload_interval=self.fusion_upload_interval)
            self.log.info(f"[TEST] Updated fusion_upload_interval for bucket {bkt.name}")

        server = self.cluster.master

        # Wait until upload starts
        self.wait_for_upload_start(server, bucket, "ep_fusion_bytes_synced")

        # Monitor upload rate, asserting it stays below the MiB limit
        self.monitor_rate_dynamic(server,
            bucket,
            stat_key="ep_fusion_bytes_synced",
            rate_limit=self.rate_limit
        )

        self.log.info("[TEST] Finished test_fusion_upload_rate_limit_default successfully")

    def test_fusion_extent_migration_rate_limit(self):
        """
        Test Fusion extent migration respects the configured rate limit in MiB/s.
        """
        bucket = self.cluster.buckets[0]
        self.log.info("[TEST] Starting test_fusion_extent_migration_rate_limit...")

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