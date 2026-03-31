"""
Test module for Magma compression configuration feature.

This module tests the new bucket-level compression settings:
- magma_index_compression_algo
- magma_data_compression_algo
- magma_compacteddata_compression_algo
- magma_enable_index_block_autotuning
- magma_enable_data_block_autotuning
"""

import os
import re
import threading
import time

from cb_tools.cbstats import Cbstats
from cb_constants import CbServer
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from Jython_tasks.task_manager import TaskManager
from shell_util.remote_connection import RemoteMachineShellConnection
from storage.magma.magma_base import MagmaBaseTest


class MagmaCompressionTests(MagmaBaseTest):
    def setUp(self):
        super(MagmaCompressionTests, self).setUp()

        self.temp_log_path = self.input.param("logs_folder", "/tmp")

        self.compression_index_algo = self.input.param("compression_index_algo", "lz4")
        self.compression_data_algo = self.input.param("compression_data_algo", "lz4")
        self.compression_compacted_algo = self.input.param("compression_compacted_algo", "lz4")
        self.enable_index_autotuning = self.input.param("enable_index_autotuning", False)
        self.enable_data_autotuning = self.input.param("enable_data_autotuning", False)

        self.expected_error = self.input.param("expected_error", False)
        self.invalid_param_type = self.input.param("invalid_param_type", None)
        self.invalid_value = self.input.param("invalid_value", None)

        self.num_iterations = self.input.param("num_iterations", 1)
        self.data_pattern = self.input.param("data_pattern", "json_compressible")
        self.wait_after_load = self.input.param("wait_after_load", 30)

        # Per-iteration compression sequence params for test_disk_usage_vs_fragmentation.
        # Format: "algo1:algo2:algo3:algo4" — one colon-separated value per iteration.
        # Falls back to single compression_*_algo value repeated for all iterations.
        raw_hot = self.input.param("hot_compression", None)
        if raw_hot is not None:
            self.hot_algo_sequence = raw_hot.split(":")
            if len(self.hot_algo_sequence) != self.num_iterations:
                raise ValueError(
                    f"hot_compression has {len(self.hot_algo_sequence)} value(s) but "
                    f"num_iterations={self.num_iterations}. Provide exactly "
                    f"{self.num_iterations} colon-separated values."
                )
        else:
            self.hot_algo_sequence = [self.compression_data_algo] * self.num_iterations

        raw_cold = self.input.param("cold_compression", None)
        if raw_cold is not None:
            self.cold_algo_sequence = raw_cold.split(":")
            if len(self.cold_algo_sequence) != self.num_iterations:
                raise ValueError(
                    f"cold_compression has {len(self.cold_algo_sequence)} value(s) but "
                    f"num_iterations={self.num_iterations}. Provide exactly "
                    f"{self.num_iterations} colon-separated values."
                )
        else:
            self.cold_algo_sequence = [self.compression_compacted_algo] * self.num_iterations

        raw_index = self.input.param("index_compression", None)
        if raw_index is not None:
            self.index_algo_sequence = raw_index.split(":")
            if len(self.index_algo_sequence) != self.num_iterations:
                raise ValueError(
                    f"index_compression has {len(self.index_algo_sequence)} value(s) but "
                    f"num_iterations={self.num_iterations}. Provide exactly "
                    f"{self.num_iterations} colon-separated values."
                )
        else:
            self.index_algo_sequence = [self.compression_index_algo] * self.num_iterations

        # Loader throughput controls (override cb_basetest defaults of 10k/20)
        self.ops_rate = self.input.param("ops_rate", 50000)
        self.process_concurrency = self.input.param("process_concurrency", 4)
        # Sirius DocLoader value_type: SimpleValue (default, highly compressible),
        # Hotel, Product, Vector, RandomlyNestedJson, etc.
        self.value_type = self.input.param("value_type", "SimpleValue")

        # load_docs_using=cbc_pillowfight loads binary via cbc-pillowfight;
        # default is the Sirius Java SDK. pillowfight_json adds -J for JSON.
        self.use_pillowfight = \
            self.input.param("load_docs_using", "sirius_java_sdk") == "cbc_pillowfight"
        self.pillowfight_json = self.input.param("pillowfight_json", False)

        # NOMINAL logical data size — num_items x doc_size. Exact ONLY for SimpleValue;
        # Hotel/Product/RandomlyNestedJson ignore doc_size and emit natural sizes, so this
        # is a requested-size reference, NOT the basis for savings in the disk test.
        self.logical_data_loaded_gb = (
                                              self.num_items_per_collection * self.doc_size
                                      ) / (1024 ** 3)
        # MEASURED live logical size (GB) — set at iteration 1 of
        # test_disk_usage_vs_fragmentation from cbstats vb_active_logical_data_size.
        # Valid for all schemas; the real basis for compression-savings math.
        self.measured_logical_gb = None

        self.log.info("=" * 80)
        self.log.info("Magma Compression Test Setup")
        self.log.info("=" * 80)
        self.log.info("Compression Parameters:")
        self.log.info(f"  Index Algorithm: {' -> '.join(self.index_algo_sequence)}")
        self.log.info(f"  Data Algorithm: {' -> '.join(self.hot_algo_sequence)}")
        self.log.info(f"  Compacted Data Algorithm: {' -> '.join(self.cold_algo_sequence)}")
        self.log.info(f"  Index Auto-tuning: {self.enable_index_autotuning}")
        self.log.info(f"  Data Auto-tuning: {self.enable_data_autotuning}")
        self.log.info("Test Parameters:")
        self.log.info(f"  Iterations: {self.num_iterations}")
        self.log.info(f"  Items per iteration: {self.num_items_per_collection}")
        self.log.info(f"  Doc Size: {self.doc_size} bytes")
        self.log.info(f"  Logical Data to Load: {self.logical_data_loaded_gb:.2f} GB "
                      f"({self.num_items_per_collection:,} docs x {self.doc_size} bytes)")
        self.log.info(f"  Data Pattern: {self.data_pattern}")
        self.log.info("=" * 80)

        # Apply compression settings ONCE, before any iteration runs (if not
        # testing invalid parameters). The algo is fixed for the whole test —
        # no per-iteration changes. Uses sequence[0], which equals the single
        # compression_*_algo param when no *_compression sequence was provided.
        # Compression algos are magma-only. On couchstore there is no algo knob,
        # so skip the bucket-property update and let it use the Couchbase default.
        if not self.expected_error and self.bucket_storage == "magma":
            self._apply_compression_settings(
                data_algo=self.hot_algo_sequence[0],
                compacted_algo=self.cold_algo_sequence[0],
                index_algo=self.index_algo_sequence[0],
            )
        elif self.bucket_storage != "magma":
            self.log.info(
                "bucket_storage={}: skipping magma compression settings "
                "(using backend default)".format(self.bucket_storage))

    def tearDown(self):
        # Gate the S3 cbcollect upload behind a param. Defaults to True to keep
        # the existing always-on behavior for the benchmarking suites; pass
        # collect_cbcollect=False to skip it (e.g. for quick local runs).
        collect_cbcollect = self.input.param("collect_cbcollect", True)
        if not collect_cbcollect:
            self.log.info("collect_cbcollect=False -> skipping cbcollect upload")
            super(MagmaCompressionTests, self).tearDown()
            return

        self.log.info("Collecting cbcollect logs with upload to S3 (always-on for compression tests)")
        try:
            customer = f"testcase{self.case_number}_compression_benchmarking"
            upload_host = "https://cb-engineering.s3.amazonaws.com"

            for _, cluster in self.cb_clusters.items():
                rest = ClusterRestAPI(cluster.master)

                self.log.info(
                    f"Triggering cbcollect with upload: "
                    f"host={upload_host}, customer={customer}"
                )
                # Pass nodes="*" so the controller collects on every node by
                # itself — avoids the otpNode-id mismatch on single-node
                # clusters (ns_1@127.0.0.1 vs ns_1@<ip>).
                status, _ = rest.start_logs_collection(
                    nodes="*",
                    upload_host=upload_host,
                    customer=customer,
                )
                if not status:
                    self.log.error("cbcollect trigger failed")
                    continue

                # Let the clusterLogsCollection task register in
                # /pools/default/tasks before polling. Without this,
                # get_cluster_tasks finds no matching task and returns the
                # full task LIST, and wait_for_cb_collect_to_complete then
                # does list['progress'] -> "list indices must be integers".
                self.sleep(30, "Wait for cbcollect task to start")

                self.cluster_util.wait_for_cb_collect_to_complete(cluster)
                # Logs are uploaded straight to S3 by ns_server (upload_host
                # + customer above); no local copy needed — the local zip is
                # removed post-upload, which is why copy_cb_collect_logs fails.

            self.log.info(
                f"cbcollect uploaded to: "
                f"{upload_host}/{customer}/"
            )
        except Exception as e:
            self.log.error(f"cbcollect failed in tearDown: {e}")
        super(MagmaCompressionTests, self).tearDown()

    def _log_cbstats_compression_configs(self, tag="Config State"):
        """
        Fetches all cbstats from the master node and explicitly logs the actual
        configurations for the compression parameters.
        """
        self.log.info(f"--- {tag} ---")
        try:
            bucket = self.cluster.buckets[0]
            cbstat_obj = Cbstats(self.cluster.master)
            stats = cbstat_obj.all_stats(bucket.name)
            cbstat_obj.disconnect()

            target_keys = [
                "magma_index_compression_algo",
                "magma_data_compression_algo",
                "magma_compacteddata_compression_algo",
                "magma_enable_index_block_autotuning",
                "magma_enable_data_block_autotuning"
            ]

            found = False
            for k, v in stats.items():
                if any(target in k.lower() for target in target_keys):
                    self.log.info(f"  {k} = {v}")
                    found = True

            if not found:
                self.log.warning("  No compression config keys found in cbstats! (Older build?)")
        except Exception as e:
            self.log.error(f"  Failed to fetch cbstats: {str(e)}")
        self.log.info("-" * 40)

    def _apply_compression_settings(self, data_algo=None, compacted_algo=None, index_algo=None):
        """
        Apply compression settings to all buckets via update_bucket_props.
        Uses the same pattern as magma_base.py for setting magma parameters.
        Optional per-call overrides take precedence over self.* defaults —
        used by test_disk_usage_vs_fragmentation to apply per-iteration sequences.
        """
        data_algo = data_algo or self.compression_data_algo
        compacted_algo = compacted_algo or self.compression_compacted_algo
        index_algo = index_algo or self.compression_index_algo

        props = "magma"
        props += f";magma_index_compression_algo={index_algo}"
        props += f";magma_data_compression_algo={data_algo}"
        props += f";magma_compacteddata_compression_algo={compacted_algo}"
        props += f";magma_enable_index_block_autotuning={str(self.enable_index_autotuning).lower()}"
        props += f";magma_enable_data_block_autotuning={str(self.enable_data_autotuning).lower()}"

        self.log.info("Applying compression settings to bucket(s)...")
        self.log.info(f"Props string: {props}")

        self._log_cbstats_compression_configs(tag="BEFORE Config Update")

        try:
            self.bucket_util.update_bucket_props(
                "backend", props,
                self.cluster, self.cluster.buckets
            )
            self.log.info("Compression settings applied successfully")

            self._log_cbstats_compression_configs(tag="AFTER Config Update")

        except Exception as e:
            self.log.error(f"Failed to apply compression settings: {str(e)}")
            raise

    # ── CPU monitoring helpers ──────────────────────────────────────────

    def _cpu_poll(self, stop_event, samples):
        """Background thread: polls cluster_util.get_cluster_stats every 5s."""
        while not stop_event.is_set():
            try:
                stats = self.cluster_util.get_cluster_stats(self.cluster.master)
                for _hostname, node_stats in stats.items():
                    cpu = node_stats.get("cpu_utilization", 0) or 0
                    samples.append({
                        "ts": time.time(),
                        "cpu_pct": round(float(cpu), 2),
                    })
                    break  # single node — take first
            except Exception as exc:
                self.log.warning(f"[cpu_monitor] poll error: {exc}")
            stop_event.wait(timeout=5)

    def _start_cpu_monitor(self):
        """Start background CPU polling. Returns (stop_event, samples, thread)."""
        stop_event = threading.Event()
        samples = []
        thread = threading.Thread(
            target=self._cpu_poll, args=(stop_event, samples), daemon=True
        )
        thread.start()
        return stop_event, samples, thread

    def _stop_cpu_monitor(self, stop_event, samples, thread, label):
        """Stop polling, compute stats, return {label, max, avg, min, samples}."""
        stop_event.set()
        thread.join(timeout=10)
        vals = [s["cpu_pct"] for s in samples]
        if not vals:
            self.log.warning(f"[cpu_monitor] {label}: no samples collected")
            return {"label": label, "max": 0.0, "avg": 0.0, "min": 0.0, "samples": samples}
        return {
            "label": label,
            "max": round(max(vals), 1),
            "avg": round(sum(vals) / len(vals), 1),
            "min": round(min(vals), 1),
            "samples": samples,
        }

    def _print_cpu_summary_table(self, cpu_log):
        """Print a compact CPU utilisation summary table below disk summary."""
        n = max(len(cpu_log["write"]), 1)
        self.log.info("")
        self.log.info("=" * 80)
        self.log.info("CPU UTILISATION SUMMARY  (polled via /pools/default every 5s)")
        self.log.info("=" * 80)
        header = f"{'Iteration':<12}{'Write Avg%':>10}{'Write Max%':>10}{'Write Min%':>10}{'Read Avg%':>10}{'Read Max%':>10}{'Read Min%':>10}"
        sep = "-" * len(header)
        self.log.info(header)
        self.log.info(sep)
        for i in range(n):
            w = cpu_log["write"][i] if i < len(cpu_log["write"]) else None
            r = cpu_log["read"][i] if i < len(cpu_log["read"]) else None
            w_avg = f"{w['avg']:.1f}" if w else "-"
            w_max = f"{w['max']:.1f}" if w else "-"
            w_min = f"{w['min']:.1f}" if w else "-"
            r_avg = f"{r['avg']:.1f}" if r else "-"
            r_max = f"{r['max']:.1f}" if r else "-"
            r_min = f"{r['min']:.1f}" if r else "-"
            self.log.info(
                f"{i + 1:<12}{w_avg:>10}{w_max:>10}{w_min:>10}{r_avg:>10}{r_max:>10}{r_min:>10}"
            )
        self.log.info(sep)

    def _measure_compression_metrics(self, iteration):
        """
        Measure all compression-related metrics using cbstats and disk usage.
        Args:
            iteration (int): Current iteration number
        Returns:
            dict: Dictionary containing all measured metrics
        """
        self.log.info(f"Measuring compression metrics for iteration {iteration}...")

        bucket = self.cluster.buckets[0]

        cbstat_obj = Cbstats(self.cluster.master)
        stats = cbstat_obj.all_stats(bucket.name)
        cbstat_obj.disconnect()

        disk_usage_breakdown = self.get_disk_usage(
            bucket, self.cluster.nodes_in_cluster
        )

        # Direct key access (no defaults): a KeyError here means the stat name
        # does not exist on this build - fix the key name before proceeding.
        compressed_size = int(stats["ep_magma_data_blocks_compressed_size"])
        uncompressed_size = int(stats["ep_magma_data_blocks_uncompressed_size"])
        compression_ratio = float(stats["ep_magma_data_blocks_compression_ratio"])
        space_reduction_pct = float(stats["ep_magma_data_blocks_space_reduction_estimate_pct"])

        logical_data_size = int(stats["ep_magma_logical_data_size"])
        logical_disk_size = int(stats["ep_magma_logical_disk_size"])

        fragmentation = float(stats["ep_magma_fragmentation"])

        # Disk usage breakdown: [kvstore, wal, keyTree, seqTree]
        disk_usage_kvstore_mb = disk_usage_breakdown[0]
        disk_usage_wal_mb = disk_usage_breakdown[1]
        disk_usage_keytree_mb = disk_usage_breakdown[2]
        disk_usage_seqtree_mb = disk_usage_breakdown[3]

        # Iteration 1 (CREATE): only live data exists, no dead versions yet → 100 GB.
        # Iteration 2+ (UPDATE): Magma holds live + dead versions. With 50% fragmentation
        # cap configured, dead data == live data → adjusted logical = 2x live data.
        disk_kvstore_gb = disk_usage_kvstore_mb / 1024.0
        adjusted_logical_gb = (
            self.logical_data_loaded_gb if iteration == 1
            else self.logical_data_loaded_gb * 2
        )
        manual_compression_ratio = (
            adjusted_logical_gb / disk_kvstore_gb
            if disk_kvstore_gb > 0 else 0.0
        )

        metrics = {
            'iteration': iteration,
            # Manual ground truth
            'logical_data_loaded_gb': self.logical_data_loaded_gb,
            'adjusted_logical_gb': adjusted_logical_gb,
            'manual_compression_ratio': manual_compression_ratio,
            # Magma reported (for comparison only)
            'compressed_size': compressed_size,
            'uncompressed_size': uncompressed_size,
            'compression_ratio': compression_ratio,
            'space_reduction_pct': space_reduction_pct,
            'logical_data_size': logical_data_size,
            'logical_disk_size': logical_disk_size,
            'fragmentation': fragmentation,
            'disk_usage_kvstore_mb': disk_usage_kvstore_mb,
            'disk_usage_wal_mb': disk_usage_wal_mb,
            'disk_usage_keytree_mb': disk_usage_keytree_mb,
            'disk_usage_seqtree_mb': disk_usage_seqtree_mb,
        }

        self.log.info("Metrics collected successfully")
        return metrics

    def _log_metrics(self, metrics, baseline_disk_kvstore_mb=None):
        """
        Log compression metrics split into two sections:
          1. Manually computed (ground truth — what you loaded vs what du shows)
          2. Magma reported stats (for comparison only, not used for validation)

        Args:
            metrics (dict): Metrics dictionary from _measure_compression_metrics
            baseline_disk_kvstore_mb (float|None): kvstore MB from iteration 1.
                When provided, shows disk vs baseline ratio for this iteration.
        """
        self.log.info("")
        self.log.info("--- MANUALLY COMPUTED (Ground Truth) ---")
        self.log.info(f"  Logical Data Loaded (live)   : {metrics['logical_data_loaded_gb']:.2f} GB"
                      f"  ({metrics['logical_data_loaded_gb'] * 1024:.0f} MB)")
        self.log.info(f"  Adjusted Logical (live+dead) : {metrics['adjusted_logical_gb']:.2f} GB"
                      f"  (iter1=live only, iter2+=2x due to 50% frag cap)")
        self.log.info(f"  Disk Usage (du / kvstore)    : "
                      f"{metrics['disk_usage_kvstore_mb'] / 1024:.2f} GB"
                      f"  ({metrics['disk_usage_kvstore_mb']} MB)")
        self.log.info(f"  Disk Usage (du / WAL)        : {metrics['disk_usage_wal_mb']} MB")
        self.log.info(f"  Manual Compression Ratio     : {metrics['manual_compression_ratio']:.2f}x"
                      f"  ({metrics['adjusted_logical_gb']:.2f} GB adjusted logical"
                      f" / {metrics['disk_usage_kvstore_mb'] / 1024:.2f} GB on disk)")
        if baseline_disk_kvstore_mb is not None and baseline_disk_kvstore_mb > 0:
            disk_vs_baseline = metrics['disk_usage_kvstore_mb'] / float(baseline_disk_kvstore_mb)
            self.log.info(f"  Disk vs Baseline             : {disk_vs_baseline:.2f}x"
                          f"  ({metrics['disk_usage_kvstore_mb']} MB"
                          f" / {baseline_disk_kvstore_mb} MB baseline)")

        self.log.info("")
        self.log.info("--- MAGMA REPORTED STATS (For Comparison Only) ---")
        self.log.info(f"  Magma Compression Ratio      : {metrics['compression_ratio']:.2f}x")
        self.log.info(f"  Magma Compressed Size        : {metrics['compressed_size'] / (1024 ** 2):.2f} MB")
        self.log.info(f"  Magma Uncompressed Size      : {metrics['uncompressed_size'] / (1024 ** 2):.2f} MB")
        self.log.info(f"  Magma Space Reduction        : {metrics['space_reduction_pct']:.2f}%")
        self.log.info(f"  Magma Logical Data Size      : {metrics['logical_data_size'] / (1024 ** 2):.2f} MB")
        self.log.info(f"  Magma Logical Disk Size      : {metrics['logical_disk_size'] / (1024 ** 2):.2f} MB")
        self.log.info(f"  Magma Fragmentation          : {metrics['fragmentation'] * 100:.2f}%")

        self.log.info("")
        self.log.info("--- COMPARISON ---")
        diff = abs(metrics['manual_compression_ratio'] - metrics['compression_ratio'])
        self.log.info(f"  Manual Ratio vs Magma Ratio  : "
                      f"{metrics['manual_compression_ratio']:.2f}x vs {metrics['compression_ratio']:.2f}x"
                      f"  (diff={diff:.2f})")
        if diff < 0.5:
            self.log.info("  -> Close match, Magma reporting accurately")
        else:
            self.log.info("  -> Gap detected, Magma may be reporting differently than OS-level disk")


    def _generate_docs_by_pattern(self, doc_ops="create"):
        """
        Generate documents based on data_pattern parameter.
        Supports different data types for compression testing.
        Supported patterns:
        - json_compressible (default): Repetitive JSON data (high compression ratio)
        - json_random: Random JSON values (lower compression ratio)
        Args:
            doc_ops (str): Document operation type (create/update/delete)
        """
        if self.data_pattern == "json_random":
            self.randomize_value = True
        else:
            self.randomize_value = False

        self.generate_docs(doc_ops=doc_ops)

    def test_basic_compression_settings(self):
        """
        Test that compression settings can be applied to buckets.
        Validates basic API functionality for all compression parameters.
        Shows compression metrics for the configured algorithm.
        Test Steps:
        1. Apply compression settings (done in setUp)
        2. Load sample data
        3. Measure and log compression metrics
        4. Verify compression working
        """
        self.log.info("=" * 80)
        self.log.info("TEST: Basic Compression Settings")
        self.log.info("=" * 80)
        self.log.info(f"Algorithm: {self.compression_data_algo}")
        self.log.info("=" * 80)

        self.log.info(f"Loading {self.num_items_per_collection} documents...")
        self.create_start = 0
        self.create_end = self.num_items_per_collection
        self._generate_docs_by_pattern(doc_ops="create")

        self.java_doc_loader(
            wait=True,
            doc_ops="create",
            skip_default=False,
            ops_rate=self.ops_rate,
            process_concurrency=self.process_concurrency,
            value_type=self.value_type
        )

        self.log.info("Document loading completed")

        self.sleep(self.wait_after_load, "Waiting for Magma flush")

        metrics = self._measure_compression_metrics(iteration=1)
        self._log_metrics(metrics)

        self.assertGreater(metrics['compressed_size'], 0,
                           "Compressed size should be greater than 0")
        self.assertGreater(metrics['uncompressed_size'], 0,
                           "Uncompressed size should be greater than 0")

        if self.compression_data_algo != "none":
            self.assertGreater(metrics['compression_ratio'], 0,
                               "Compression ratio should be greater than 0")
            self.log.info(f"\n✓ Compression verified: ratio = {metrics['compression_ratio']:.2f}x, "
                          f"space reduction = {metrics['space_reduction_pct']:.2f}%")
        else:
            self.log.info(f"\n✓ No compression (as expected): ratio = {metrics['compression_ratio']:.2f}x")

        self.log.info("=" * 80)
        self.log.info("TEST PASSED")
        self.log.info("=" * 80)

    def test_invalid_compression_parameters(self):
        """
        Test validation of invalid compression parameters.
        Expects operation to fail or produce appropriate errors.
        Test Steps:
        1. Attempt to apply invalid compression settings
        2. Verify error is raised or logged
        3. Verify bucket remains functional
        """
        self.log.info("=" * 80)
        self.log.info("TEST: Invalid Compression Parameters")
        self.log.info("=" * 80)
        self.log.info(f"Testing invalid parameter type: {self.invalid_param_type}")
        self.log.info(f"Invalid value: {self.invalid_value}")

        if self.invalid_param_type == "algorithm":
            props = f"magma;magma_data_compression_algo={self.invalid_value}"
        elif self.invalid_param_type == "zstd_level":
            props = f"magma;magma_data_compression_algo={self.invalid_value}"
        elif self.invalid_param_type == "zstd_syntax":
            props = f"magma;magma_data_compression_algo={self.invalid_value}"
        elif self.invalid_param_type == "empty":
            props = f"magma;magma_data_compression_algo={self.invalid_value}"
        elif self.invalid_param_type == "case":
            props = f"magma;magma_data_compression_algo={self.invalid_value}"
        else:
            props = f"magma;magma_data_compression_algo={self.invalid_value}"

        self.log.info(f"Attempting to apply: {props}")

        error_occurred = False
        error_message = ""

        try:
            self.bucket_util.update_bucket_props(
                "backend", props,
                self.cluster, self.cluster.buckets
            )
            self.log.info("update_bucket_props completed without raising exception")

            try:
                buckets_warmed_up = self.bucket_util.is_warmup_complete(
                    self.cluster.buckets,
                    retry_count=3
                )
                if not buckets_warmed_up:
                    error_occurred = True
                    error_message = "Buckets failed to warm up"
            except Exception as warmup_error:
                error_occurred = True
                error_message = str(warmup_error)

        except Exception as e:
            error_occurred = True
            error_message = str(e)
            self.log.info(f"Exception raised (expected): {error_message}")

        if self.expected_error:
            self.assertTrue(error_occurred,
                            f"Expected error for invalid value '{self.invalid_value}' but operation succeeded")
            self.log.info(f"✓ Validation PASSED: Invalid parameter correctly rejected")
            self.log.info(f"  Error: {error_message}")
        else:
            self.assertFalse(error_occurred,
                             f"Unexpected error for value '{self.invalid_value}': {error_message}")
            self.log.info(f"✓ Validation PASSED: Parameter accepted as expected")

        self.log.info("=" * 80)
        self.log.info("TEST PASSED: Parameter validation working correctly")
        self.log.info("=" * 80)

    def test_boundary_values(self):
        """
        Test boundary values for zstd compression levels (1 and 22).
        Shows compression metrics for boundary values.
        Test Steps:
        1. Apply boundary compression settings (done in setUp)
        2. Load sample data
        3. Measure and log compression metrics
        4. Verify boundary value accepted
        """
        self.log.info("=" * 80)
        self.log.info("TEST: Boundary Values")
        self.log.info("=" * 80)
        self.log.info(f"Testing boundary value: {self.compression_data_algo}")
        self.log.info("=" * 80)

        self.log.info(f"Loading {self.num_items_per_collection} documents...")
        self.create_start = 0
        self.create_end = self.num_items_per_collection
        self._generate_docs_by_pattern(doc_ops="create")

        self.java_doc_loader(
            wait=True,
            doc_ops="create",
            skip_default=False,
            ops_rate=self.ops_rate,
            process_concurrency=self.process_concurrency,
            value_type=self.value_type
        )

        self.log.info("Document loading completed")
        self.sleep(self.wait_after_load, "Waiting for Magma flush")

        metrics = self._measure_compression_metrics(iteration=1)
        self._log_metrics(metrics)

        self.assertGreater(metrics['compressed_size'], 0,
                           "Compressed size should be greater than 0")
        self.assertGreater(metrics['compression_ratio'], 0,
                           "Compression ratio should be greater than 0")

        self.log.info(f"\n✓ Boundary value {self.compression_data_algo} verified: "
                      f"ratio = {metrics['compression_ratio']:.2f}x, "
                      f"space reduction = {metrics['space_reduction_pct']:.2f}%")

        self.log.info("=" * 80)
        self.log.info("TEST PASSED")
        self.log.info("=" * 80)


    def _cbexport_to_jsonl(self, node, jsonl_path):
        """
        Export the first bucket to a JSONL file (one JSON doc per line) on `node`
        using cbexport. This is the input format magma_dump compute-blocksize
        expects (NOT a magma instance path).

        Returns the remote path of the generated JSONL file.
        """
        bucket = self.cluster.buckets[0]
        # cbexport expects a connection string with a scheme, not host:port.
        # couchbase://<ip> is the repo-wide convention for cluster CLI tools.
        conn_str = "couchbase://{}".format(node.ip)
        log_path = jsonl_path + ".log"

        # Quote user/password so special characters don't break the shell
        # (cbexport itself reminds you of this on a bootstrap failure).
        cmd = (
            "/opt/couchbase/bin/cbexport json "
            "-c {host} -u '{user}' -p '{password}' -b {bucket} "
            "--include-key key -o {out} "
            "--scope-field scopeID --collection-field collectionID "
            "-f lines -t 8 -l {log}".format(
                host=conn_str,
                user=node.rest_username,
                password=node.rest_password,
                bucket=bucket.name,
                out=jsonl_path,
                log=log_path,
            )
        )

        self.log.info("Exporting bucket '{}' to JSONL: {}".format(bucket.name, jsonl_path))
        shell = RemoteMachineShellConnection(node)
        try:
            out, err = shell.execute_command(cmd)
            self.log.info("cbexport output: {}".format(out))
            if err:
                self.log.warning("cbexport stderr: {}".format(err))

            # Verify the file exists and has content
            line_count_out, _ = shell.execute_command(
                "wc -l {} | awk '{{print $1}}'".format(jsonl_path))
            size_out, _ = shell.execute_command(
                "du -m {} | awk '{{print $1}}'".format(jsonl_path))
            num_docs = int(line_count_out[0].strip()) if line_count_out and line_count_out[0].strip() else 0
            size_mb = int(size_out[0].strip()) if size_out and size_out[0].strip() else 0
            self.log.info("Exported JSONL: {} docs, ~{} MB".format(num_docs, size_mb))

            # compute-blocksize requires >=10 docs and >=4MB of data
            self.assertGreaterEqual(
                num_docs, 10,
                "compute-blocksize needs >=10 docs, exported only {}".format(num_docs))
            self.assertGreaterEqual(
                size_mb, 4,
                "compute-blocksize needs >=4MB of data, exported only {} MB".format(size_mb))
        finally:
            shell.disconnect()
        return jsonl_path

    def _run_compute_blocksize(self, node, jsonl_path, label=None):
        """
        Run `magma_dump <jsonl> compute-blocksize --extended-output` on `node`
        and log the full block-size / compression report. --extended-output
        tests every algorithm built into magma_dump (Snappy, LZ4 and all
        configured Zstd levels) and reports both compressed-value and
        uncompressed-value block stats.

        `label` is shown in the report header (defaults to the loaded value_type);
        pass the file name when running over pre-generated JSONL files.

        Returns the raw stdout lines of the tool.
        """
        # Redirect the report to a file on the NODE, then read it back. This
        # persists the output (survives a flaky channel / lets you re-inspect)
        # and the read itself is instant. magma_dump can run for a long time at
        # large doc sizes, so use timeout=None (no timeout) — block until it
        # finishes, exactly like running the command by hand.
        out_file = "{}.blocksize.out".format(jsonl_path)
        cmd = ("/opt/couchbase/bin/magma_dump {} compute-blocksize "
               "--extended-output > {} 2>&1".format(jsonl_path, out_file))

        self.log.info("Running (no timeout): {}".format(cmd))
        shell = RemoteMachineShellConnection(node)
        exit_code = None
        try:
            _out, _err, exit_code = shell.execute_command(
                cmd, timeout=None, get_exit_code=True)
            # Pull the captured report back (fast — output already on disk).
            report, _ = shell.execute_command(
                "cat {}".format(out_file), timeout=None)
        finally:
            shell.disconnect()

        header = label if label else "value_type={}".format(self.value_type)
        self.log.info("=" * 80)
        self.log.info("COMPUTE-BLOCKSIZE REPORT ({}) | exit={}".format(header, exit_code))
        self.log.info("=" * 80)
        self.log.info("\n" + "\n".join(report))
        self.log.info("=" * 80)

        self.assertTrue(
            report,
            "magma_dump compute-blocksize produced no output (exit={}); "
            "inspect {} on {}".format(exit_code, out_file, node.ip))
        if exit_code not in (0, None):
            self.log.warning(
                "magma_dump exit code {} for {} — report may be partial"
                .format(exit_code, jsonl_path))
        return report

    def test_load_and_compute_blocksize(self):
        """
        Load documents of a given Sirius value_type, then compute the optimal
        Magma block size for that data shape.

        Test Steps:
        1. Load num_items docs of self.value_type (SimpleValue / Hotel /
           Product / RandomlyNestedJson).
        2. Wait for Magma flush.
        3. cbexport the bucket to a JSONL file (>=10 docs, >=4MB required).
        4. Run magma_dump compute-blocksize on the JSONL and log the report
           (block sizes 4KB-128KB, per-algorithm compression/decompression stats).
        """
        self.log.info("=" * 80)
        self.log.info("TEST: Load + Compute Blocksize")
        self.log.info("  value_type : {}".format(self.value_type))
        self.log.info("  num_items  : {}".format(self.num_items_per_collection))
        self.log.info("=" * 80)

        # Build a SiriusCouchbaseLoader per collection.
        # The Sirius Java loader generates keys/values internally from the
        # create index range + value_type, so no Python doc generators are used.
        self.log.info("Loading {} documents...".format(self.num_items_per_collection))
        self.create_start = 0
        self.create_end = self.num_items_per_collection

        doc_loading_tm = TaskManager(self.process_concurrency)
        load_tasks = []
        for bucket in self.cluster.buckets:
            for scope in bucket.scopes.keys():
                if scope == CbServer.system_scope:
                    continue
                for collection in bucket.scopes[scope].collections.keys():
                    if self.skip_load_to_default_collection and \
                            collection == "_default" and scope == "_default":
                        continue
                    self.log.info(f"Loading data into {bucket.name}:{scope}:{collection}")
                    loader = SiriusCouchbaseLoader(
                        server_ip=self.cluster.master.ip,
                        server_port=self.cluster.master.port,
                        username=self.cluster.master.rest_username,
                        password=self.cluster.master.rest_password,
                        bucket=bucket, scope_name=scope, collection_name=collection,
                        key_prefix=self.key, key_size=self.key_size,
                        doc_size=self.doc_size,
                        key_type=self.key_type,
                        create_percent=100, read_percent=0,
                        update_percent=0, delete_percent=0,
                        expiry_percent=0,
                        create_start_index=self.create_start,
                        create_end_index=self.create_end,
                        read_start_index=0, read_end_index=0,
                        update_start_index=0, update_end_index=0,
                        delete_start_index=0, delete_end_index=0,
                        expiry_start_index=0, expiry_end_index=0,
                        exp=0,
                        process_concurrency=self.process_concurrency,
                        validate_docs=False,
                        ops=self.ops_rate,
                        mutate=0,
                        value_type=self.value_type
                    )
                    loader.create_doc_load_task()
                    load_tasks.append(loader)

        for task in load_tasks:
            doc_loading_tm.add_new_task(task)
        for task in load_tasks:
            doc_loading_tm.get_task_result(task)
        self.log.info("Document loading completed")

        self.sleep(self.wait_after_load, "Waiting for Magma flush")

        # cbexport and magma_dump both run ON the node, so the JSONL path must
        # exist on the NODE's filesystem — NOT self.temp_log_path (logs_folder),
        # which is a directory on the test-runner host. Use a node-local dir.
        node = self.cluster.master
        jsonl_path = "/tmp/compute_blocksize_{}.jsonl".format(self.value_type)

        self._cbexport_to_jsonl(node, jsonl_path)
        self._run_compute_blocksize(node, jsonl_path)

        self.log.info("=" * 80)
        self.log.info("TEST PASSED")
        self.log.info("=" * 80)

    def _reset_timings(self):
        """
        Reset cbstats timing histograms on all cluster nodes.
        Called before the read workload so captured timings reflect
        only the read phase, with no noise from prior write iterations.
        """
        for node in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(node)
            shell.execute_command(
                f"/opt/couchbase/bin/cbstats localhost:11210 "
                f"-u {node.rest_username} -p {node.rest_password} reset"
            )
            shell.disconnect()
        self.log.info("cbstats timing histograms reset on all nodes")

    def _parse_timings_output(self, output, metrics_of_interest):
        """
        Parse raw cbstats timings histogram text into a usable dict.

        Expected format per metric:
          bg_load (50000000 total)
              95us -  183us : ( 12.4178%) 6208902
              ...
              Avg             : (  339us)

        Percentages in the histogram are CUMULATIVE — p50/p99 are the upper
        bound of the first bucket where the running total crosses 50%/99%.
        Zero-count buckets (same bounds, count=0) are skipped.

        Args:
            output (list): Raw lines from get_timings()
            metrics_of_interest (set): Metric names to extract

        Returns:
            dict: {metric_name: {'total': int, 'avg_us': float,
                                  'p50_us': float, 'p99_us': float}}
        """
        result = {}
        current_metric = None
        p50_found = False
        p99_found = False
        unit_to_us = {'us': 1.0, 'ms': 1000.0, 's': 1_000_000.0}

        for line in output:
            # Header: " bg_load (50000000 total)"
            m = re.match(r'^\s+([a-z_]+)\s+\((\d+)\s+total\)', line)
            if m:
                name = m.group(1)
                current_metric = name if name in metrics_of_interest else None
                if current_metric:
                    result[current_metric] = {
                        'total': int(m.group(2)),
                        'avg_us': None,
                        'p50_us': None,
                        'p99_us': None,
                    }
                    p50_found = False
                    p99_found = False
                continue

            if current_metric:
                # Avg line: "    Avg             : (  339us)"
                m_avg = re.match(r'\s+Avg\s+:\s+\(\s*([0-9.]+)(us|ms|s)\s*\)', line)
                if m_avg:
                    multiplier = unit_to_us.get(m_avg.group(2), 1.0)
                    result[current_metric]['avg_us'] = float(m_avg.group(1)) * multiplier
                    current_metric = None
                    continue

                # Bucket line: "  95us -  183us : ( 12.4178%) 6208902"
                # Units can be us/ms/s and may differ between lower and upper bound.
                m_bucket = re.match(
                    r'\s+(\d+(?:\.\d+)?)(us|ms|s)\s+-\s+(\d+(?:\.\d+)?)(us|ms|s)'
                    r'\s+:\s+\(\s*(\d+\.\d+)%\)\s+(\d+)',
                    line
                )
                if m_bucket:
                    count = int(m_bucket.group(6))
                    if count == 0:
                        continue
                    upper_us = float(m_bucket.group(3)) * unit_to_us.get(m_bucket.group(4), 1.0)
                    cumulative_pct = float(m_bucket.group(5))
                    if not p50_found and cumulative_pct >= 50.0:
                        result[current_metric]['p50_us'] = upper_us
                        p50_found = True
                    if not p99_found and cumulative_pct >= 99.0:
                        result[current_metric]['p99_us'] = upper_us
                        p99_found = True

        return result

    def _capture_timings(self, bucket_name, metrics_of_interest=None):
        """
        Capture timing histograms from cbstats timings for the requested metrics.
        Must be called after _reset_timings() and the workload of interest.

        Read latency uses {'bg_load', 'bg_wait'} (default); write latency uses
        {'disk_commit'}.

        Args:
            bucket_name (str): Bucket to query.
            metrics_of_interest (set): Histogram names to extract. Defaults to
                the read-latency set {'bg_load', 'bg_wait'}.

        Returns:
            dict: {metric: {'total': int, 'avg_us': float,
                            'p50_us': float, 'p99_us': float}}
        """
        if metrics_of_interest is None:
            metrics_of_interest = {'bg_load', 'bg_wait'}
        cbstat_obj = Cbstats(self.cluster.master)
        # command="" avoids the default "raw" flag so we get
        # the human-readable histogram format our parser expects.
        output, error = cbstat_obj.get_timings(bucket_name, command="")
        cbstat_obj.disconnect()
        return self._parse_timings_output(output, metrics_of_interest)

    def _get_magma_op_counters(self):
        """
        Sum the cumulative ep_magma_sets / ep_magma_gets counters across all KV
        nodes for the first bucket. These are monotonic counters (since memcached
        warmup, confirmed against kv_engine's io_num_write / io_bg_fetch_docs_read
        tracking), so throughput is derived from the DELTA across a phase, never
        the absolute value.

        Returns:
            dict: {'sets': int, 'gets': int} summed cluster-wide
        """
        bucket_name = self.cluster.buckets[0].name
        total_sets = 0
        total_gets = 0
        for node in self.cluster.nodes_in_cluster:
            cbstat_obj = Cbstats(node)
            stats = cbstat_obj.all_stats(bucket_name)
            cbstat_obj.disconnect()
            if self.bucket_storage == "couchstore":
                # Couchstore has no ep_magma_sets/gets; use the KV command
                # counters (also monotonic) for the throughput delta.
                total_sets += int(stats.get("cmd_set", 0))
                total_gets += int(stats.get("cmd_get", 0))
            else:
                total_sets += int(stats.get("ep_magma_sets", 0))
                total_gets += int(stats.get("ep_magma_gets", 0))
        return {"sets": total_sets, "gets": total_gets}

    def _get_index_blocks_stats(self):
        """
        Collect TotalIndexBlocksSize (compressed) and TotalIndexBlocksUncompressedSize
        from cbstats kvstore rw_N:magma JSON blobs for all cluster nodes.

        TotalIndexBlocksSize (top-level) is key+seq+local combined (compressed).
        TotalIndexBlocksUncompressedSize lives inside keyStats/seqStats/localStats.

        Returns:
            dict: {
                'per_shard': list of {'shard': str, 'compressed': int, 'uncompressed': int},
                'total_compressed': int,   # bytes, cluster-wide
                'total_uncompressed': int, # bytes, cluster-wide
            }
        """
        import json as _json

        bucket_name = self.cluster.buckets[0].name
        shard_map = {}  # "rw_N@ip" -> {'compressed': int, 'uncompressed': int}
        # Match "  rw_0:magma:   {...}" lines. cbstats output has leading
        # whitespace before the key, so do NOT anchor with ^. The ":magma:"
        # (colon after magma) excludes "rw_N:magma_kvstore" and similar keys.
        _magma_line_re = re.compile(r'(rw_\d+):magma:\s+(\{.+\})')

        for node in self.cluster.nodes_in_cluster:
            cbstat_obj = Cbstats(node)
            try:
                raw_output, _ = cbstat_obj.get_stats(bucket_name, "kvstore",
                                                     field_to_grep="magma")
            finally:
                cbstat_obj.disconnect()

            if isinstance(raw_output, str):
                raw_output = raw_output.splitlines()

            for line in raw_output:
                m = _magma_line_re.search(line)
                if not m:
                    continue
                shard_id = m.group(1)                  # "rw_0"
                shard_label = "{}@{}".format(shard_id, node.ip)
                d = _json.loads(m.group(2))
                compressed = d.get('TotalIndexBlocksSize', 0)
                uncompressed = sum(
                    d.get(tree, {}).get('TotalIndexBlocksUncompressedSize', 0)
                    for tree in ('keyStats', 'seqStats', 'localStats')
                )
                entry = shard_map.setdefault(shard_label, {'compressed': 0, 'uncompressed': 0})
                entry['compressed'] += compressed
                entry['uncompressed'] += uncompressed

        per_shard = [
            {'shard': label, 'compressed': v['compressed'], 'uncompressed': v['uncompressed']}
            for label, v in sorted(shard_map.items())
        ]
        return {
            'per_shard': per_shard,
            'total_compressed': sum(s['compressed'] for s in per_shard),
            'total_uncompressed': sum(s['uncompressed'] for s in per_shard),
        }

    def _log_index_blocks_stats(self, index_stats):
        """
        Log per-shard and total index block sizes in a table.

        Args:
            index_stats (dict): Output of _get_index_blocks_stats()
        """
        per_shard = index_stats['per_shard']
        total_c = index_stats['total_compressed']
        total_u = index_stats['total_uncompressed']

        self.log.info("")
        self.log.info("=== INDEX BLOCKS SIZE (cbstats kvstore TotalIndexBlocksSize) ===")
        hdr = "{:<22} {:>20} {:>22} {:>8}".format(
            "Shard", "Compressed (bytes)", "Uncompressed (bytes)", "Ratio")
        sep = "-" * 75
        self.log.info(hdr)
        self.log.info(sep)
        for s in per_shard:
            c, u = s['compressed'], s['uncompressed']
            ratio = "{:.2f}x".format(u / c) if c > 0 else "N/A"
            self.log.info("{:<22} {:>20} {:>22} {:>8}".format(
                s['shard'], c, u, ratio))
        self.log.info(sep)
        ratio_total = "{:.2f}x".format(total_u / total_c) if total_c > 0 else "N/A"
        self.log.info("{:<22} {:>20} {:>22} {:>8}".format(
            "TOTAL", total_c, total_u, ratio_total))
        self.log.info("{:<22} {:>19.4f}MB {:>21.4f}MB".format(
            "", total_c / (1024 ** 2), total_u / (1024 ** 2)))
        self.log.info("=" * 75)

    def _get_bucket_disk_usage_mb(self, bucket):
        """
        Returns total on-disk size of the bucket directory in MB by running
        a single du command per node across all cluster nodes.
        Path: {data_path}/{bucket.uuid}
        """
        total_mb = 0
        bucket_path = os.path.join(self.data_path, bucket.uuid)
        for node in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(node)
            out, _ = shell.execute_command(
                f"du -cm {bucket_path} | tail -1 | awk '{{print $1}}'"
            )
            shell.disconnect()
            total_mb += int(out[0].strip())
        return total_mb

    def _collect_disk_fragmentation_metrics(self, iteration):
        """
        Collect Magma reported stats and total on-disk size via du.
        Magma stats are printed for comparison only — validation uses raw disk numbers.

        Args:
            iteration (int): Current iteration number

        Returns:
            dict: All collected metrics
        """
        bucket = self.cluster.buckets[0]

        # Magma reported stats — printed for visibility, NOT used in validation
        cbstat_obj = Cbstats(self.cluster.master)
        stats = cbstat_obj.all_stats(bucket.name)
        cbstat_obj.disconnect()

        # Couchstore has none of the ep_magma_* stats — map to its equivalents.
        if self.bucket_storage == "couchstore":
            return self._collect_couchstore_frag_metrics(iteration, bucket, stats)

        magma_fragmentation_pct = float(stats["ep_magma_fragmentation"]) * 100
        logical_data_size = int(stats["ep_magma_logical_data_size"])
        logical_disk_size = int(stats["ep_magma_logical_disk_size"])
        space_reduction_pct = float(stats["ep_magma_data_blocks_space_reduction_estimate_pct"])
        # ep_magma_data_blocks_uncompressed_size / _compressed_size: true apples-to-apples
        #   data-block pair — both measure the same SSTable value blocks before/after
        #   compression. Use these (not logical_data/db_data) for compression savings.
        data_blocks_uncompressed_size = int(stats["ep_magma_data_blocks_uncompressed_size"])
        data_blocks_compressed_size = int(stats["ep_magma_data_blocks_compressed_size"])
        # vb_active_logical_data_size: logical (uncompressed) size of ACTIVE
        #   vbuckets only — excludes replica/pending/history noise.
        # ep_db_data_size: storage-engine-reported physical size of valid data
        #   on disk (compressed, excludes file overhead) — compare against du.
        active_logical_data_size = int(stats["vb_active_logical_data_size"])
        db_data_size = int(stats["ep_db_data_size"])

        # Single du on the full bucket directory — captures everything
        # (kvstore, WAL, keyTree, seqTree) in one SSH call per node.
        total_disk_mb = self._get_bucket_disk_usage_mb(bucket)
        total_disk_gb = total_disk_mb / 1024.0

        # kvstore component du (magma.*/kv*) — the data store + indexes, but
        # EXCLUDES the WAL (magma.*/wal is a sibling). Used as the basis for the
        # manual fragmentation number so write-ahead-log churn does not inflate it.
        kvstore_mb = self.get_disk_usage(
            bucket, self.cluster.nodes_in_cluster
        )[0]

        # Index block sizes from cbstats kvstore rw_N:magma JSON
        index_stats = self._get_index_blocks_stats()

        return {
            'iteration': iteration,
            # Magma reported (comparison/visibility only)
            'magma_fragmentation_pct': magma_fragmentation_pct,
            'logical_data_size': logical_data_size,
            'logical_disk_size': logical_disk_size,
            'space_reduction_pct': space_reduction_pct,
            'data_blocks_uncompressed_size': data_blocks_uncompressed_size,
            'data_blocks_compressed_size': data_blocks_compressed_size,
            'active_logical_data_size': active_logical_data_size,
            'db_data_size': db_data_size,
            # Manually computed via du (source of truth for validation)
            'total_disk_mb': total_disk_mb,
            'total_disk_gb': total_disk_gb,
            # kvstore-only du (excludes WAL) — basis for the manual frag number
            'kvstore_mb': kvstore_mb,
            # Index block sizes (compressed + uncompressed) from cbstats kvstore
            'index_stats': index_stats,
            'index_blocks_compressed': index_stats['total_compressed'],
            'index_blocks_uncompressed': index_stats['total_uncompressed'],
        }

    def _collect_couchstore_frag_metrics(self, iteration, bucket, stats):
        """Couchstore counterpart of _collect_disk_fragmentation_metrics.

        Couchstore exposes none of the ep_magma_* stats, so:
          - fragmentation = (ep_db_file_size - ep_db_data_size) / ep_db_file_size
          - logical/disk sizes come from vb_active_logical_data_size / ep_db_*
          - compression + index-block metrics are 0 (N/A): couchstore's default
            compression is passive, so it reports no ratio / space reduction and
            has no magma index-block stats.
        Throughput (cmd_set/cmd_get) and latency (disk_commit / bg_load / bg_wait)
        use the shared counters/timings and need no special handling here.
        """
        db_data_size = int(stats["ep_db_data_size"])
        db_file_size = int(stats["ep_db_file_size"])
        active_logical = int(stats["vb_active_logical_data_size"])
        frag_pct = ((db_file_size - db_data_size) / db_file_size * 100.0) \
            if db_file_size else 0.0
        # Whole bucket-dir du (backend-agnostic). Couchstore has no magma.*/wal
        # split, so this also serves as the kvstore basis for the manual frag.
        total_disk_mb = self._get_bucket_disk_usage_mb(bucket)
        index_stats = self._get_index_blocks_stats()  # returns zeros on couchstore
        return {
            'iteration': iteration,
            'magma_fragmentation_pct': frag_pct,
            'logical_data_size': active_logical,
            'logical_disk_size': db_file_size,
            'space_reduction_pct': 0.0,
            'data_blocks_uncompressed_size': 0,
            'data_blocks_compressed_size': 0,
            'active_logical_data_size': active_logical,
            'db_data_size': db_data_size,
            'total_disk_mb': total_disk_mb,
            'total_disk_gb': total_disk_mb / 1024.0,
            'kvstore_mb': total_disk_mb,
            'index_stats': index_stats,
            'index_blocks_compressed': index_stats['total_compressed'],
            'index_blocks_uncompressed': index_stats['total_uncompressed'],
        }

    def _log_disk_fragmentation_iteration(self, metrics):
        """
        Log Magma reported stats and manually computed disk stats side by side.
        This makes it easy to spot discrepancies between what Magma reports
        and what is actually on disk at the OS level.

        Args:
            metrics (dict): Metrics from _collect_disk_fragmentation_metrics
        """
        self.log.info("")
        self.log.info("=== MAGMA REPORTED STATS (cbstats) ===")
        self.log.info(f"  Magma Fragmentation          : {metrics['magma_fragmentation_pct']:.2f}%")
        self.log.info(f"  Logical Data Size            : {metrics['logical_data_size'] / (1024 ** 2):.2f} MB")
        self.log.info(f"  Logical Disk Size            : {metrics['logical_disk_size'] / (1024 ** 2):.2f} MB")
        self.log.info(f"  Active Logical Data Size     : {metrics['active_logical_data_size'] / (1024 ** 2):.2f} MB")
        self.log.info(f"  Data Blocks Uncompressed     : {metrics['data_blocks_uncompressed_size'] / (1024 ** 2):.2f} MB")
        self.log.info(f"  Data Blocks Compressed       : {metrics['data_blocks_compressed_size'] / (1024 ** 2):.2f} MB")
        self.log.info(f"  DB Data Size (valid on disk) : {metrics['db_data_size'] / (1024 ** 2):.2f} MB")

        if 'index_stats' in metrics:
            self._log_index_blocks_stats(metrics['index_stats'])

        self.log.info("")
        self.log.info("=== DISK USAGE (du /data) ===")
        self.log.info("  Total disk: {}MB  ({:.2f}GB)".format(
            metrics['total_disk_mb'], metrics['total_disk_gb']))

        if 'net_savings_gb' in metrics:
            net = metrics['net_savings_gb']
            cs = metrics['compression_savings_gb']
            fo = metrics['fragmentation_overhead_gb']
            iwo = metrics.get('index_wal_overhead_gb', 0)
            logical_data = metrics.get('logical_data_gb', 0)
            logical_disk = metrics.get('logical_disk_gb', 0)
            db_data = metrics.get('db_data_gb', 0)
            baseline = metrics.get('baseline_compressed_gb', 0)
            actual = metrics['total_disk_gb']
            iteration = metrics['iteration']
            self.log.info("--- Compression savings vs fragmentation (iter {}) ---".format(iteration))
            self.log.info("  logical_data        : {:.2f} GB  (one true copy, uncompressed)".format(logical_data))
            self.log.info("  logical_disk        : {:.2f} GB  (live + dead, uncompressed, real frag)".format(logical_disk))
            self.log.info("  db_data (compressed): {:.2f} GB  (compressed values on disk, no index/WAL)".format(db_data))
            self.log.info("  du / total_disk     : {:.2f} GB  (values + index + WAL + metadata)".format(actual))
            self.log.info("  baseline_disk       : {:.2f} GB  (iter 1 du, zero fragmentation)".format(baseline))
            self.log.info("  compression_savings : {:.2f} GB  = data_blocks_uncompressed - data_blocks_compressed".format(cs))
            self.log.info("  index_wal_overhead  : {:.2f} GB  = du - db_data (machinery, not a frag loss)".format(iwo))
            self.log.info("  frag_overhead       : {:.2f} GB  = du - baseline_disk".format(fo))
            self.log.info("  net_savings         : {:.2f} GB  = compression_savings - frag_overhead".format(net))
            if net > 0:
                self.log.info("  compression ({:.2f}GB) > fragmentation ({:.2f}GB) "
                              "-- savings intact".format(cs, fo))
            else:
                self.log.warning("  fragmentation ({:.2f}GB) > compression ({:.2f}GB) "
                                 "-- savings negated, net={:.2f}GB".format(fo, cs, net))

    def _validate_disk_vs_fragmentation(self, current, previous, baseline_disk_mb,
                                        baseline_kvstore_mb=None, frag_threshold=50.0):
        """
        Validate on-disk size against fragmentation using raw disk numbers only.
        Does NOT rely on Magma compression stats.

        Checks:
          1. Disk growth between consecutive iterations stays within 2x
          2. Manually computed fragmentation (vs baseline) stays below threshold

        Args:
            current (dict): Current iteration metrics
            previous (dict): Previous iteration metrics
            baseline_disk_mb (float): iter-1 whole-dir du (used for disk-growth log)
            baseline_kvstore_mb (float): iter-1 kvstore-only du (WAL excluded) —
                basis for the manual fragmentation number
            frag_threshold (float): Max acceptable fragmentation % (default 50.0)

        Returns:
            dict: Validation results
        """
        disk_after = current['total_disk_mb']
        disk_before = previous['total_disk_mb']

        disk_growth_ratio = (
            disk_after / float(disk_before) if disk_before > 0 else float('inf')
        )

        # Manual fragmentation — dead data / total, computed purely from du.
        # Basis is the kvstore-only component (magma.*/kv*), which EXCLUDES the
        # WAL so write-ahead-log churn does not inflate the number. Compared
        # against the clean iter-1 kvstore baseline (no dead versions yet).
        kvstore_after = current['kvstore_mb']
        manual_frag_pct = (
            (kvstore_after - baseline_kvstore_mb) / float(kvstore_after) * 100
        ) if kvstore_after > 0 else 0.0

        self.log.info("")
        self.log.info("=" * 80)
        self.log.info(f"ITERATION {current['iteration']} - VALIDATION")
        self.log.info("=" * 80)
        self.log.info("Disk Usage (du /data):")
        self.log.info(
            f"  Baseline: {baseline_disk_mb:.2f} MB, Current: {disk_after:.2f} MB, "
            f"Ratio vs prev: {disk_growth_ratio:.2f}x"
        )
        self.log.info(
            f"  Manual frag (kvstore, WAL-excluded, vs baseline): {manual_frag_pct:.2f}%"
        )

        # Check 1: disk growth <= 2x between consecutive iterations
        disk_growth_passed = disk_after <= 2 * disk_before
        if disk_growth_passed:
            self.log.info(
                f"\n✓ Disk growth check PASSED: {disk_after:.2f} MB <= "
                f"{2 * disk_before:.2f} MB (ratio={disk_growth_ratio:.2f}x)"
            )
        else:
            self.log.warning(
                f"\n⚠ Disk growth check FAILED: {disk_after:.2f} MB > "
                f"{2 * disk_before:.2f} MB (ratio={disk_growth_ratio:.2f}x)"
            )

        # Check 2: manual fragmentation below threshold
        frag_threshold_passed = manual_frag_pct < frag_threshold
        if frag_threshold_passed:
            self.log.info(
                f"✓ Fragmentation threshold PASSED: "
                f"{manual_frag_pct:.2f}% < {frag_threshold}%"
            )
        else:
            self.log.warning(
                f"⚠ Fragmentation threshold FAILED: "
                f"{manual_frag_pct:.2f}% >= {frag_threshold}%"
            )

        self.log.info("=" * 80)

        validation_passed = disk_growth_passed and frag_threshold_passed
        error_parts = []
        if not disk_growth_passed:
            error_parts.append(
                f"Disk growth {disk_growth_ratio:.2f}x > 2x threshold"
            )
        if not frag_threshold_passed:
            error_parts.append(
                f"Manual frag {manual_frag_pct:.2f}% >= {frag_threshold}%"
            )

        return {
            'iteration': current['iteration'],
            'disk_growth_ratio': disk_growth_ratio,
            'manual_frag_pct': manual_frag_pct,
            'disk_growth_passed': disk_growth_passed,
            'frag_threshold_passed': frag_threshold_passed,
            'validation_passed': validation_passed,
            'error_msg': "; ".join(error_parts),
        }

    def _print_disk_fragmentation_summary(self, metrics_log, iteration_results):
        """
        Print a transposed summary table for the disk vs fragmentation test.
        Shows both Magma reported stats and manually computed disk stats per iteration.
        Read latency (bg_load / bg_wait) is captured per iteration and shown as
        its own per-iteration rows in the table.

        Args:
            metrics_log (list): metrics dicts from all iterations
            iteration_results (list): validation result dicts from all iterations
        """
        n = len(metrics_log)
        label_w = 34
        col_w = 15

        def row(label, values):
            return f"{label:<{label_w}}" + "".join(f"{str(v):<{col_w}}" for v in values)

        def read_row(kind):
            """Build a per-iteration read-latency row from each metrics' read_timing."""
            vals = []
            for m in metrics_log:
                t = m.get('read_timing', {})
                bl = t.get('bg_load', {})
                bw = t.get('bg_wait', {})
                if kind == 'bg_load_avg':
                    v = bl.get('avg_us')
                elif kind == 'total_p50':
                    a, b = bw.get('p50_us'), bl.get('p50_us')
                    v = (a + b) if a is not None and b is not None else None
                elif kind == 'total_p99':
                    a, b = bw.get('p99_us'), bl.get('p99_us')
                    v = (a + b) if a is not None and b is not None else None
                else:  # total_avg
                    a, b = bw.get('avg_us'), bl.get('avg_us')
                    v = (a + b) if a is not None and b is not None else None
                vals.append(f"{v:.1f}" if v is not None else "N/A")
            return vals

        def write_lat_row(kind):
            """Build a per-iteration write-latency (disk_commit) row."""
            vals = []
            for m in metrics_log:
                dc = m.get('write_latency', {}).get('disk_commit', {})
                if kind == 'avg':
                    v = dc.get('avg_us')
                elif kind == 'p50':
                    v = dc.get('p50_us')
                else:  # p99
                    v = dc.get('p99_us')
                vals.append(f"{v:.1f}" if v is not None else "N/A")
            return vals

        def throughput_row(key):
            """Build a per-iteration throughput row (write/read ops/sec)."""
            vals = []
            for m in metrics_log:
                v = m.get(key)
                vals.append(f"{v:,.0f}" if v is not None else "N/A")
            return vals

        total_w = label_w + col_w * n
        sep = "=" * total_w
        div = "-" * total_w

        col_headers = [
            f"Iter {m['iteration']}(BASE)" if m['iteration'] == 1 else f"Iter {m['iteration']}"
            for m in metrics_log
        ]

        disk_ratios, manual_frags = [], []
        disk_checks, frag_checks, overalls, errors = [], [], [], []

        for r in iteration_results:
            if r['iteration'] == 1:
                disk_ratios.append("N/A(base)")
                manual_frags.append("0.00%")
                disk_checks.append("BASELINE")
                frag_checks.append("BASELINE")
                overalls.append("BASELINE")
                errors.append("")
            else:
                dr = r['disk_growth_ratio']
                disk_ratios.append(f"{dr:.2f}x")
                manual_frags.append(f"{r['manual_frag_pct']:.2f}%")
                disk_checks.append(
                    f"PASS({dr:.2f}x)" if r['disk_growth_passed'] else f"FAIL({dr:.2f}x)"
                )
                frag_checks.append(
                    f"PASS" if r['frag_threshold_passed'] else f"FAIL"
                )
                overalls.append("PASSED" if r['validation_passed'] else "FAILED")
                errors.append(r['error_msg'][:col_w - 1] if r['error_msg'] else "")

        nominal_gb = self.logical_data_loaded_gb
        live_gb = metrics_log[0].get('measured_logical_gb') or nominal_gb
        logical_disk0_gb = metrics_log[0].get('logical_disk_gb', live_gb)
        db_data0_gb = metrics_log[0].get('db_data_gb', 0)
        baseline_disk_gb = metrics_log[0]['total_disk_gb']
        savings_gb = metrics_log[0].get('compression_savings_gb', None)
        savings_str = f"{savings_gb:.2f} GB" if savings_gb is not None else "N/A"

        lines = [
            sep,
            "HOW METRICS ARE CALCULATED",
            sep,
            f"  [1] Logical Data              = cbstats ep_magma_logical_data_size — one true copy, UNCOMPRESSED",
            f"                                = {live_gb:.2f} GB  (constant across update iterations)",
            f"                                  nominal num_items x doc_size = {self.num_items_per_collection:,} x {self.doc_size} bytes = {nominal_gb:.2f} GB",
            f"                                  (nominal is exact only for SimpleValue; other schemas ignore doc_size)",
            f"",
            f"  [2] Logical Disk              = cbstats ep_magma_logical_disk_size — live + dead, UNCOMPRESSED",
            f"                                = {logical_disk0_gb:.2f} GB at iter 1 (= logical_data; no dead versions yet)",
            f"                                  grows with updates, capped at ~2x by the 50% fragmentation limit",
            f"",
            f"  [3] DB Data (compressed)      = cbstats ep_db_data_size — COMPRESSED values on disk, no index/WAL",
            f"                                = {db_data0_gb:.2f} GB",
            f"",
            f"  [4] Total Disk (du /data)     = du on full bucket dir = values + index + WAL + metadata (OS truth)",
            f"      Baseline (iter 1)         : {baseline_disk_gb:.2f} GB  (clean state, zero fragmentation)",
            f"",
            f"  [5] Compression Savings       = data_blocks_uncompressed - data_blocks_compressed",
            f"                                  (same SSTable blocks before/after compression — true apples-to-apples)",
            f"                                = {savings_str} at iter 1",
            f"                                  how much compression shrank the data itself",
            f"",
            f"  [6] Index/WAL Overhead        = du - db_data",
            f"                                  key index + seq index + WAL + metadata (large for many small docs)",
            f"                                  NOT a compression failure — just directory machinery",
            f"",
            f"  [7] Fragmentation Overhead    = current_disk - baseline_disk",
            f"                                  extra physical disk from dead versions piling up after updates",
            f"",
            f"  [8] Net Savings               = compression_savings - fragmentation_overhead",
            f"                                  > 0  compression winning despite fragmentation",
            f"                                  <= 0 fragmentation negated compression gains",
            f"",
            f"  [9] Manual Frag vs Baseline   = (current_disk - baseline_disk) / current_disk x 100",
            f"                                  same approach as Magma's ep_magma_fragmentation",
            f"                                  never negative — measures dead data % at current point in time",
            f"",
            f"  Validation Thresholds:",
            f"    iter-1 compression gate     = data_blocks_compressed < data_blocks_uncompressed  (compression must shrink SSTable blocks)",
            f"    ep_magma_fragmentation      = Magma reported frag  must be <= 50% (hard cap)",
            f"    Disk Growth Ratio           = current_disk / prev_disk  must be <= 2x",
            sep,
            "TEST CONFIGURATION",
            sep,
            f"  magma_data_compression_algo         : {' -> '.join(self.hot_algo_sequence)}",
            f"  magma_compacteddata_compression_algo: {' -> '.join(self.cold_algo_sequence)}",
            f"  magma_index_compression_algo        : {' -> '.join(self.index_algo_sequence)}",
            f"  magma_enable_index_block_autotuning : {self.enable_index_autotuning}",
            f"  magma_enable_data_block_autotuning  : {self.enable_data_autotuning}",
            f"  num_iterations                      : {self.num_iterations}",
            f"  num_items_per_collection            : {self.num_items_per_collection}",
            f"  value_type (doc schema)             : {self.value_type}",
            f"  doc_size                            : {self.doc_size} bytes",
            f"  data_pattern                        : {self.data_pattern}",
            f"  wait_after_load                     : {self.wait_after_load}s",
            sep,
            "SUMMARY - ALL ITERATIONS",
            sep,
            row("Metric", col_headers),
            div,

            row("Total Items Loaded", [self.num_items_per_collection] * n),
            "",
            "=== MAGMA REPORTED STATS (cbstats) ===",
            row("Magma Fragmentation (%)",
                [f"{m['magma_fragmentation_pct']:.2f}" for m in metrics_log]),
            row("Logical Data Size (MB)",
                [f"{m['logical_data_size'] / (1024 ** 2):.2f}" for m in metrics_log]),
            row("Logical Disk Size (MB)",
                [f"{m['logical_disk_size'] / (1024 ** 2):.2f}" for m in metrics_log]),
            row("Active Logical Data (MB)",
                [f"{m['active_logical_data_size'] / (1024 ** 2):.2f}" for m in metrics_log]),
            row("Data Blocks Uncompressed (MB)",
                [f"{m['data_blocks_uncompressed_size'] / (1024 ** 2):.2f}" for m in metrics_log]),
            row("Data Blocks Compressed (MB)",
                [f"{m['data_blocks_compressed_size'] / (1024 ** 2):.2f}" for m in metrics_log]),
            row("DB Data Size (MB)",
                [f"{m['db_data_size'] / (1024 ** 2):.2f}" for m in metrics_log]),
            row("Space Reduction (%)",
                [f"{m['space_reduction_pct']:.2f}" for m in metrics_log]),
            row("Manual Space Reduction (%)",
                [f"{m.get('manual_space_reduction_pct', 0.0):.2f}" for m in metrics_log]),
            row("Manual Compression Savings (GB)",
                [f"{m.get('manual_compression_savings_gb', 0.0):.2f}" for m in metrics_log]),
            "",
            "=== DISK USAGE (du /data) ===",
            row("Total Disk (MB)",
                [m['total_disk_mb'] for m in metrics_log]),
            row("Total Disk (GB)",
                [f"{m['total_disk_gb']:.2f}" for m in metrics_log]),
            "",
            "=== INDEX BLOCKS SIZE (cbstats kvstore) ===",
            row("Index Compressed (bytes)",
                [m.get('index_blocks_compressed', 'N/A') for m in metrics_log]),
            row("Index Uncompressed (bytes)",
                [m.get('index_blocks_uncompressed', 'N/A') for m in metrics_log]),
            row("Index Compressed (MB)",
                [f"{m['index_blocks_compressed'] / (1024**2):.4f}"
                 if 'index_blocks_compressed' in m else 'N/A'
                 for m in metrics_log]),
            row("Index Ratio",
                [f"{m['index_blocks_uncompressed'] / m['index_blocks_compressed']:.2f}x"
                 if m.get('index_blocks_compressed', 0) > 0 else 'N/A'
                 for m in metrics_log]),
            "",
            "=== NET COMPRESSION SAVINGS VS FRAGMENTATION ===",
            row("Compression Savings (GB)",
                [f"{m['compression_savings_gb']:.2f}" if 'compression_savings_gb' in m else "N/A"
                 for m in metrics_log]),
            row("Index/WAL Overhead (GB)",
                [f"{m['index_wal_overhead_gb']:.2f}" if 'index_wal_overhead_gb' in m else "N/A"
                 for m in metrics_log]),
            row("Fragmentation Overhead (GB)",
                [f"{m['fragmentation_overhead_gb']:.2f}" if 'fragmentation_overhead_gb' in m else "N/A"
                 for m in metrics_log]),
            row("Net Savings (comp - frag) (GB)",
                [f"{m['net_savings_gb']:.2f}" if 'net_savings_gb' in m else "N/A"
                 for m in metrics_log]),
            "",
            "=== VALIDATION METRICS ===",
            row("Disk Growth Ratio vs Prev", disk_ratios),
            row("Manual Frag vs Baseline (%)", manual_frags),
            "",
            "=== VALIDATION RESULTS ===",
            row("Disk Growth Check", disk_checks),
            row("Magma Frag Check (<=50%)",
                [
                    f"PASS({m['magma_fragmentation_pct']:.1f}%)"
                    if m['magma_fragmentation_pct'] <= 50.0
                    else f"FAIL({m['magma_fragmentation_pct']:.1f}%)"
                    for m in metrics_log
                ]),
            row("Overall Validation", overalls),
            row("Validation Error", errors),
            "",
            "=== READ LATENCY (per iteration, cbstats timings) ===",
            row("Read bg_load Avg (us)", read_row('bg_load_avg')),
            row("Read bg_wait+bg_load p50 (us)", read_row('total_p50')),
            row("Read bg_wait+bg_load p99 (us)", read_row('total_p99')),
            row("Read bg_wait+bg_load Avg (us)", read_row('total_avg')),
            "",
            "=== THROUGHPUT (ep_magma_sets/gets delta / elapsed, ops/sec) ===",
            row("Write ops/sec (magma_sets)", throughput_row('write_throughput')),
            row("Read ops/sec (magma_gets)", throughput_row('read_throughput')),
            "",
            "=== WRITE LATENCY (per iteration, cbstats timings disk_commit) ===",
            row("Write disk_commit Avg (us)", write_lat_row('avg')),
            row("Write disk_commit p50 (us)", write_lat_row('p50')),
            row("Write disk_commit p99 (us)", write_lat_row('p99')),
            sep,
        ]

        self.log.info("\n" + "\n".join(lines))

    def _run_read_workload_and_capture(self, iteration):
        """
        Run a cache-cold read workload over the full loaded keyspace and capture
        bg_load / bg_wait timings for THIS iteration. Timings are reset first so
        the captured numbers reflect only this iteration's reads — giving a read
        latency reading at the current compression/fragmentation state.

        Returns:
            tuple: (timings, read_throughput) where timings is the output of
                _capture_timings() (bg_load/bg_wait histograms) and
                read_throughput is ops/sec from the ep_magma_gets delta.
        """
        self.log.info("")
        self.log.info("-" * 80)
        self.log.info(f"READ LATENCY PHASE (iteration {iteration})")
        self.log.info("-" * 80)
        self._reset_timings()
        self.sleep(5, "Waiting after timing reset before read workload")

        self.log.info(f"Running read workload on {self.num_items_per_collection} docs...")
        # Build all read loaders before starting the measurement window so that
        # loader construction overhead is excluded from elapsed time.
        read_tm = TaskManager(self.process_concurrency)
        read_tasks = []
        if not self.use_pillowfight:
            for bucket in self.cluster.buckets:
                for scope in bucket.scopes.keys():
                    if scope == CbServer.system_scope:
                        continue
                    for collection in bucket.scopes[scope].collections.keys():
                        if self.skip_load_to_default_collection and \
                                collection == "_default" and scope == "_default":
                            continue
                        loader = SiriusCouchbaseLoader(
                            server_ip=self.cluster.master.ip,
                            server_port=self.cluster.master.port,
                            username=self.cluster.master.rest_username,
                            password=self.cluster.master.rest_password,
                            bucket=bucket, scope_name=scope, collection_name=collection,
                            key_prefix=self.key, key_size=self.key_size,
                            doc_size=self.doc_size,
                            key_type=self.key_type,
                            create_percent=0, read_percent=100,
                            update_percent=0, delete_percent=0,
                            expiry_percent=0,
                            create_start_index=0, create_end_index=0,
                            read_start_index=0,
                            read_end_index=self.num_items_per_collection,
                            update_start_index=0, update_end_index=0,
                            delete_start_index=0, delete_end_index=0,
                            expiry_start_index=0, expiry_end_index=0,
                            exp=0,
                            process_concurrency=self.process_concurrency,
                            validate_docs=False,
                            ops=self.ops_rate,
                            mutate=0,
                            value_type=self.value_type
                        )
                        loader.create_doc_load_task()
                        read_tasks.append(loader)

        # Snapshot counter and start clock together, then submit all pre-built
        # tasks so the timed window contains only actual read activity.
        _gets_before = self._get_magma_op_counters()["gets"]
        _read_t0 = time.time()
        if self.use_pillowfight:
            for bucket in self.cluster.buckets:
                shell = RemoteMachineShellConnection(self.cluster.master)
                rcmd = (
                    "/opt/couchbase/bin/cbc-pillowfight "
                    "-U couchbase://{ip}/{bkt} -u {user} -P {pwd} "
                    "-I {items} --no-population -r 0 -c 1 -t {threads} -p {kp} -T"
                ).format(
                    ip=self.cluster.master.ip, bkt=bucket.name,
                    user=self.cluster.master.rest_username,
                    pwd=self.cluster.master.rest_password,
                    items=self.num_items_per_collection,
                    threads=self.process_concurrency, kp=self.key)
                for sc in self._pillowfight_target_collections():
                    rcmd += " --collection {}".format(sc)
                if self.ops_rate:
                    rcmd += " --rate-limit {}".format(self.ops_rate)
                rcmd += " -Dtimeout=10"
                self.log.info("Executing pillowfight read: {}".format(rcmd))
                shell.execute_command(rcmd, timeout=600)
                shell.disconnect()
        else:
            for task in read_tasks:
                read_tm.add_new_task(task)

            for task in read_tasks:
                read_tm.get_task_result(task)

        # Capture counter first, then stop the clock.
        _gets_after = self._get_magma_op_counters()["gets"]
        _read_elapsed = time.time() - _read_t0
        read_throughput = (
            (_gets_after - _gets_before) / _read_elapsed
            if _read_elapsed > 0 else 0.0
        )
        self.log.info(
            f"Iteration {iteration} read throughput: "
            f"{read_throughput:,.0f} ops/sec "
            f"(magma_gets delta={_gets_after - _gets_before:,} "
            f"over {_read_elapsed:.1f}s)"
        )

        self.sleep(self.wait_after_load,
                   "Waiting after read workload before capturing timings")
        timing_result = self._capture_timings(self.cluster.buckets[0].name)
        _bl = timing_result.get('bg_load', {}).get('avg_us')
        _bw = timing_result.get('bg_wait', {}).get('avg_us')
        self.log.info(
            f"Iteration {iteration} read latency captured: "
            f"bg_load avg={'N/A' if _bl is None else f'{_bl:.1f}us'}, "
            f"bg_wait avg={'N/A' if _bw is None else f'{_bw:.1f}us'}"
        )
        return timing_result, read_throughput

    def _pillowfight_load(self, collections):
        """Binary load via cbc-pillowfight. --random-body writes random
        (near-incompressible) bytes; --sequential --start-at 0 -I num_items over
        -r 100 --populate-only SETs the full key range and exits, so re-running
        on a later iteration overwrites the same keys (update phase).
        pillowfight_json=True adds -J for JSON instead of binary.

        :param collections: "scope.collection" targets, or [] for _default.
        """
        for bucket in self.cluster.buckets:
            shell = RemoteMachineShellConnection(self.cluster.master)
            cmd = (
                "/opt/couchbase/bin/cbc-pillowfight "
                "-U couchbase://{ip}/{bkt} "
                "-u {user} -P {pwd} "
                "-I {items} --sequential --start-at 0 "
                "-m {sz} -M {sz} "
                "-r 100 --populate-only --random-body "
                "-t {threads} -p {kp} -T"
            ).format(
                ip=self.cluster.master.ip, bkt=bucket.name,
                user=self.cluster.master.rest_username,
                pwd=self.cluster.master.rest_password,
                items=self.num_items_per_collection,
                sz=self.doc_size, threads=self.process_concurrency,
                kp=self.key)
            for sc in collections:
                cmd += " --collection {}".format(sc)
            if self.pillowfight_json:
                cmd += " -J"
            if self.ops_rate:
                cmd += " --rate-limit {}".format(self.ops_rate)
            cmd += " -Dtimeout=10"
            self.log.info("Executing pillowfight: {}".format(cmd))
            output, err = shell.execute_command(cmd, timeout=3600)
            shell.disconnect()
            # -T writes timings to stderr on success, so non-empty stderr is not
            # a failure; scan for error markers and surface via log_failure().
            combined = "\n".join((output or []) + (err or []))
            self.log.info(
                "pillowfight output (bucket {}):\n{}".format(
                    bucket.name, combined[-2000:]))
            for marker in ("LCB_ERR", "Failed to", "does not exist",
                           "Couldn't connect", "Authentication failed"):
                if marker in combined:
                    self.log_failure(
                        "cbc-pillowfight load FAILED for bucket {} "
                        "(marker '{}'): {}".format(
                            bucket.name, marker, combined[-2000:]))
                    break

    def _pillowfight_target_collections(self):
        """scope.collection targets matching the Sirius path; [] for _default."""
        targets = []
        for bucket in self.cluster.buckets:
            for scope in bucket.scopes.keys():
                if scope == CbServer.system_scope:
                    continue
                for collection in bucket.scopes[scope].collections.keys():
                    if self.skip_load_to_default_collection and \
                            collection == "_default" and scope == "_default":
                        continue
                    if scope == "_default" and collection == "_default":
                        continue
                    targets.append("{}.{}".format(scope, collection))
        return targets

    def test_disk_usage_vs_fragmentation(self):
        """
        Validate on-disk size against fragmentation across multiple iterations.
        Does NOT rely on Magma compression stats for validation — uses raw du numbers.

        Per-iteration compression settings are applied via hot_compression,
        cold_compression, and index_compression sequence params (colon-separated).

        Each iteration:
        1. Apply compression settings for this iteration (hot/cold/index)
        2. Reset cbstats timings, then load/update documents while capturing
           write throughput (ep_magma_sets delta / elapsed) and write latency
           (disk_commit histogram)
        3. Collect ep_magma_fragmentation (cbstats) — hard assert <= 50%
        4. Collect raw disk usage via du — source of truth for 2x growth check
        5. Log Magma reported stats alongside du for comparison
        6. Read workload capturing read throughput (ep_magma_gets delta / elapsed)
           and read latency (bg_load / bg_wait histograms)

        Validation:
        - ep_magma_fragmentation <= 50% per iteration (Magma hard cap)
        - Disk growth between consecutive iterations <= 2x (Magma 50% frag cap)

        Metrics captured (informational, in summary tables):
        - Write/Read throughput in ops/sec from cumulative ep_magma_sets/gets
          counters (delta over the active load/read window)
        - Write latency (disk_commit) and read latency (bg_load/bg_wait) from
          cbstats timings histograms, reset before each phase
        - CPU utilisation during writes (all iters) and reads (last iter only)

        Test Steps:
        - Iteration 1 (baseline): create documents, measure baseline disk
        - Iterations 2-N: update documents, validate disk growth and fragmentation
        - Read latency phase: measure bg_load / bg_wait decompression cost
        - Print full summary table across all iterations
        """
        self.log.info("=" * 80)
        self.log.info("TEST: Disk Usage vs Fragmentation Validation")
        self.log.info("=" * 80)
        self.log.info("Configuration:")
        self.log.info(f"  Data Algorithm      : {' -> '.join(self.hot_algo_sequence)}")
        self.log.info(f"  Compacted Algorithm : {' -> '.join(self.cold_algo_sequence)}")
        self.log.info(f"  Index Algorithm     : {' -> '.join(self.index_algo_sequence)}")
        self.log.info(f"  Items per iteration : {self.num_items_per_collection}")
        self.log.info(f"  Number of iterations: {self.num_iterations}")
        self.log.info(f"  Data pattern        : {self.data_pattern}")
        self.log.info(f"  Ops rate            : {self.ops_rate}")
        self.log.info(f"  Process concurrency : {self.process_concurrency}")
        self.log.info(f"  Validation          : ep_magma_fragmentation <= 50%, disk growth <= 2x")
        self.log.info("=" * 80)

        metrics_log = []
        iteration_results = []
        baseline_total_disk = None
        baseline_kvstore = None
        baseline_compressed_gb = None
        compression_savings_gb = None

        cpu_log = {"write": [], "read": []}

        for iteration in range(1, self.num_iterations + 1):
            self.log.info("")
            self.log.info("=" * 80)
            if iteration == 1:
                self.log.info(f"ITERATION {iteration}/{self.num_iterations} (BASELINE)")
            else:
                self.log.info(f"ITERATION {iteration}/{self.num_iterations}")
            self.log.info("=" * 80)
            # Compression algo is applied ONCE in setUp and stays fixed for the
            # whole test — it is intentionally NOT re-applied per iteration.
            self.log.info(
                f"  Compression (fixed): hot={self.hot_algo_sequence[0]}, "
                f"cold={self.cold_algo_sequence[0]}, index={self.index_algo_sequence[0]}"
            )

            _write_stop, _write_samples, _write_thread = self._start_cpu_monitor()

            # Reset cbstats timings so the disk_commit histogram reflects only
            # this iteration's writes.
            self._reset_timings()

            # Build all loaders before starting the measurement window so that
            # loader object construction and create_doc_load_task() overhead are
            # excluded from elapsed time. The clock starts only when tasks are
            # ready to be submitted.
            doc_loading_tm = TaskManager(self.process_concurrency)
            load_tasks = []

            # pillowfight runs inside the timed window below; iter 1 creates,
            # iter 2+ overwrite the same keys (update). Set range bookkeeping only.
            if self.use_pillowfight:
                if iteration == 1:
                    self.log.info(f"Loading {self.num_items_per_collection} documents (cbc-pillowfight)...")
                    self.create_start = 0
                    self.create_end = self.num_items_per_collection
                else:
                    self.log.info(f"Updating {self.num_items_per_collection} documents (cbc-pillowfight)...")
                    self.update_start = 0
                    self.update_end = self.num_items_per_collection
                    self.mutate += 1
            elif iteration == 1:
                self.log.info(f"Loading {self.num_items_per_collection} documents...")
                self.create_start = 0
                self.create_end = self.num_items_per_collection

                for bucket in self.cluster.buckets:
                    for scope in bucket.scopes.keys():
                        if scope == CbServer.system_scope:
                            continue
                        for collection in bucket.scopes[scope].collections.keys():
                            if self.skip_load_to_default_collection and \
                                    collection == "_default" and scope == "_default":
                                continue
                            self.log.info(f"Loading data into {bucket.name}:{scope}:{collection}")
                            loader = SiriusCouchbaseLoader(
                                server_ip=self.cluster.master.ip,
                                server_port=self.cluster.master.port,
                                username=self.cluster.master.rest_username,
                                password=self.cluster.master.rest_password,
                                bucket=bucket, scope_name=scope, collection_name=collection,
                                key_prefix=self.key, key_size=self.key_size,
                                doc_size=self.doc_size,
                                key_type=self.key_type,
                                create_percent=100, read_percent=0,
                                update_percent=0, delete_percent=0,
                                expiry_percent=0,
                                create_start_index=self.create_start,
                                create_end_index=self.create_end,
                                read_start_index=0, read_end_index=0,
                                update_start_index=0, update_end_index=0,
                                delete_start_index=0, delete_end_index=0,
                                expiry_start_index=0, expiry_end_index=0,
                                exp=0,
                                process_concurrency=self.process_concurrency,
                                validate_docs=False,
                                ops=self.ops_rate,
                                mutate=0,
                                value_type=self.value_type
                            )
                            loader.create_doc_load_task()
                            load_tasks.append(loader)
            else:
                self.log.info(f"Updating {self.num_items_per_collection} documents...")
                self.update_start = 0
                self.update_end = self.num_items_per_collection

                for bucket in self.cluster.buckets:
                    for scope in bucket.scopes.keys():
                        if scope == CbServer.system_scope:
                            continue
                        for collection in bucket.scopes[scope].collections.keys():
                            if self.skip_load_to_default_collection and \
                                    collection == "_default" and scope == "_default":
                                continue
                            self.log.info(f"Starting updates on {bucket.name}:{scope}:{collection}")
                            loader = SiriusCouchbaseLoader(
                                server_ip=self.cluster.master.ip,
                                server_port=self.cluster.master.port,
                                username=self.cluster.master.rest_username,
                                password=self.cluster.master.rest_password,
                                bucket=bucket, scope_name=scope, collection_name=collection,
                                key_prefix=self.key, key_size=self.key_size,
                                doc_size=self.doc_size,
                                key_type=self.key_type,
                                create_percent=0, read_percent=0,
                                update_percent=100, delete_percent=0,
                                expiry_percent=0,
                                create_start_index=0, create_end_index=0,
                                read_start_index=0, read_end_index=0,
                                update_start_index=self.update_start,
                                update_end_index=self.update_end,
                                delete_start_index=0, delete_end_index=0,
                                expiry_start_index=0, expiry_end_index=0,
                                exp=0,
                                process_concurrency=self.process_concurrency,
                                validate_docs=False,
                                ops=self.ops_rate,
                                mutate=self.mutate,
                                value_type=self.value_type
                            )
                            loader.create_doc_load_task()
                            load_tasks.append(loader)
                self.mutate += 1

            # Snapshot counter and start clock together, then submit all
            # pre-built tasks so the timed window contains only actual write
            # activity, not loader construction overhead.
            _sets_before = self._get_magma_op_counters()["sets"]
            _write_t0 = time.time()
            if self.use_pillowfight:
                self._pillowfight_load(self._pillowfight_target_collections())
            else:
                for task in load_tasks:
                    doc_loading_tm.add_new_task(task)

                for task in load_tasks:
                    doc_loading_tm.get_task_result(task)

            # Capture counter first, then stop the clock so any op that reached
            # Magma before task completion is included in the delta.
            _sets_after = self._get_magma_op_counters()["sets"]
            _write_elapsed = time.time() - _write_t0
            write_throughput = (
                (_sets_after - _sets_before) / _write_elapsed
                if _write_elapsed > 0 else 0.0
            )
            self.log.info(
                f"Iteration {iteration} write throughput: "
                f"{write_throughput:,.0f} ops/sec "
                f"(magma_sets delta={_sets_after - _sets_before:,} "
                f"over {_write_elapsed:.1f}s)"
            )

            write_cpu = self._stop_cpu_monitor(
                _write_stop, _write_samples, _write_thread,
                label=str(iteration)
            )
            cpu_log["write"].append(write_cpu)

            self.log.info("Document operation completed")
            self.sleep(self.wait_after_load,
                       f"Waiting {self.wait_after_load}s for Magma flush")

            # Capture write latency (disk_commit histogram) after the flush has
            # settled so it reflects all of this iteration's disk commits.
            write_latency = self._capture_timings(
                self.cluster.buckets[0].name, {'disk_commit'}
            )
            _dc = write_latency.get('disk_commit', {}).get('avg_us')
            self.log.info(
                f"Iteration {iteration} write latency captured: "
                f"disk_commit avg="
                f"{'N/A' if _dc is None else f'{_dc:.1f}us'}"
            )

            metrics = self._collect_disk_fragmentation_metrics(iteration)
            metrics['write_throughput'] = write_throughput
            metrics['write_latency'] = write_latency

            self.assertLessEqual(
                metrics['magma_fragmentation_pct'], 50.0,
                f"Iteration {iteration}: ep_magma_fragmentation "
                f"{metrics['magma_fragmentation_pct']:.2f}% exceeded hard cap of 50%"
            )

            # --- Logical sizes (GB), straight from cbstats -------------------------
            # logical_data : one true copy of every item, UNCOMPRESSED. Constant across
            #   update iterations (10M x 1KB stays ~10GB no matter how many updates).
            # logical_disk : live + dead versions, UNCOMPRESSED. Grows with updates but
            #   capped at ~2x by the 50% fragmentation limit. Real frag — not a 2x guess.
            # db_data      : COMPRESSED valid data on disk (values only, no index/WAL).
            logical_data_gb = metrics['logical_data_size'] / (1024.0 ** 3)
            logical_disk_gb = metrics['logical_disk_size'] / (1024.0 ** 3)
            db_data_gb = metrics['db_data_size'] / (1024.0 ** 3)
            data_blocks_uncompressed_gb = metrics['data_blocks_uncompressed_size'] / (1024.0 ** 3)
            data_blocks_compressed_gb = metrics['data_blocks_compressed_size'] / (1024.0 ** 3)

            # Manual space reduction — engine-agnostic (magma AND couchstore):
            # uncompressed logical (vb_active_logical_data_size) vs on-disk valid
            # data (ep_db_data_size). Both stats exist on both backends. This is
            # the ONLY compression measure available on couchstore (no data-block
            # stats); on magma it is a coarser cross-check of the block-level
            # space_reduction_pct — it reads lower because ep_db_data_size
            # includes on-disk metadata/index, not just values.
            active_logical_gb = metrics['active_logical_data_size'] / (1024.0 ** 3)
            manual_space_reduction_pct = (
                (active_logical_gb - db_data_gb) / active_logical_gb * 100.0
                if active_logical_gb > 0 else 0.0)
            manual_compression_savings_gb = active_logical_gb - db_data_gb

            if iteration == 1:
                # logical_data is the "one true copy" reference; clean iter-1 du is the
                # zero-fragmentation baseline for measuring fragmentation growth later.
                self.measured_logical_gb = logical_data_gb
                baseline_total_disk = metrics['total_disk_mb']
                baseline_kvstore = metrics['kvstore_mb']
                baseline_compressed_gb = metrics['total_disk_gb']

            # Compression savings — true apples-to-apples on the same SSTable data blocks.
            #   data_blocks_uncompressed - data_blocks_compressed (both measure identical blocks).
            compression_savings_gb = data_blocks_uncompressed_gb - data_blocks_compressed_gb

            # Index/WAL/metadata machinery — the slice of du that is NOT document values.
            #   Large for many small docs (10M keys -> big index). Surfaced explicitly so
            #   it is never mistaken for a compression failure.
            index_wal_overhead_gb = metrics['total_disk_gb'] - db_data_gb

            # Fragmentation overhead — extra physical disk from dead versions piling up,
            #   measured against the clean iter-1 du baseline.
            fragmentation_overhead_gb = metrics['total_disk_gb'] - baseline_compressed_gb

            # Net savings — does compression still win after fragmentation eats into it?
            net_savings_gb = compression_savings_gb - fragmentation_overhead_gb

            metrics['logical_data_gb'] = logical_data_gb
            metrics['logical_disk_gb'] = logical_disk_gb
            metrics['db_data_gb'] = db_data_gb
            metrics['manual_space_reduction_pct'] = manual_space_reduction_pct
            metrics['manual_compression_savings_gb'] = manual_compression_savings_gb
            metrics['data_blocks_uncompressed_gb'] = data_blocks_uncompressed_gb
            metrics['data_blocks_compressed_gb'] = data_blocks_compressed_gb
            metrics['baseline_compressed_gb'] = baseline_compressed_gb
            metrics['measured_logical_gb'] = self.measured_logical_gb
            metrics['nominal_logical_gb'] = self.logical_data_loaded_gb
            metrics['compression_savings_gb'] = compression_savings_gb
            metrics['index_wal_overhead_gb'] = index_wal_overhead_gb
            metrics['fragmentation_overhead_gb'] = fragmentation_overhead_gb
            metrics['net_savings_gb'] = net_savings_gb
            # Real with-fragmentation logical size (replaces the old manual 2x model).
            metrics['logical_data_loaded_gb'] = logical_disk_gb

            metrics_log.append(metrics)
            self._log_disk_fragmentation_iteration(metrics)

            # Iteration-1 gate: compression must actually shrink the data blocks.
            # Uses data_blocks_uncompressed vs data_blocks_compressed — true apples-to-apples
            # on the same SSTable blocks; index/WAL/metadata cannot cause a false failure.
            # Magma-only: couchstore reports no data-block stats (they are 0), and no
            # compression algo is applied there, so this gate does not apply.
            if iteration == 1 and self.hot_algo_sequence[0] != 'none' \
                    and self.bucket_storage == "magma":
                self.assertLess(
                    data_blocks_compressed_gb, data_blocks_uncompressed_gb,
                    f"FAIL: Algorithm '{self.hot_algo_sequence[0]}' did not compress data on"
                    f" clean data (zero fragmentation). data_blocks_compressed="
                    f"{data_blocks_compressed_gb:.2f} GB >= data_blocks_uncompressed="
                    f"{data_blocks_uncompressed_gb:.2f} GB."
                )

            if iteration > 1:
                result = self._validate_disk_vs_fragmentation(
                    metrics, metrics_log[iteration - 2], baseline_total_disk,
                    baseline_kvstore_mb=baseline_kvstore
                )
                iteration_results.append(result)
            else:
                iteration_results.append({
                    'iteration': iteration,
                    'disk_growth_ratio': None,
                    'manual_frag_pct': 0.0,
                    'disk_growth_passed': True,
                    'frag_threshold_passed': True,
                    'validation_passed': True,
                    'error_msg': '',
                })

            # Per-iteration read latency: run a read workload now so we capture
            # read cost at THIS iteration's compression/fragmentation state.
            # Attached to the same metrics dict already in metrics_log.
            if iteration == self.num_iterations:
                _read_stop, _read_samples, _read_thread = self._start_cpu_monitor()

            metrics['read_timing'], metrics['read_throughput'] = \
                self._run_read_workload_and_capture(iteration)

            if iteration == self.num_iterations:
                read_cpu = self._stop_cpu_monitor(
                    _read_stop, _read_samples, _read_thread,
                    label=str(iteration)
                )
                cpu_log["read"].append(read_cpu)

        self._print_disk_fragmentation_summary(metrics_log, iteration_results)

        self._print_cpu_summary_table(cpu_log)

        self.log.info("")
        self.log.info("ITERATION RESULTS:")
        passed_count = 0
        failed_count = 0

        for result in iteration_results:
            if result['iteration'] == 1:
                self.log.info(f"  Iteration {result['iteration']}: ✓ BASELINE")
                passed_count += 1
            else:
                if result['validation_passed']:
                    status = "✓ PASSED"
                    passed_count += 1
                else:
                    status = "✗ FAILED"
                    failed_count += 1

                self.log.info(
                    f"  Iteration {result['iteration']}: {status} "
                    f"(Disk Growth: {result['disk_growth_ratio']:.2f}x, "
                    f"Frag: {result['manual_frag_pct']:.2f}%)"
                )
                if not result['validation_passed'] and result['error_msg']:
                    self.log.info(f"    Error: {result['error_msg']}")

        self.log.info("")
        self.log.info(
            f"Total: {passed_count} passed, {failed_count} failed "
            f"out of {len(iteration_results)} iterations"
        )

        all_passed = all(r['validation_passed'] for r in iteration_results)

        if all_passed:
            self.log.info("=" * 80)
            self.log.info("TEST PASSED: Disk usage vs fragmentation validated successfully")
            self.log.info("=" * 80)
        else:
            self.log.error("=" * 80)
            self.log.error("TEST FAILED: Disk usage or fragmentation exceeded thresholds")
            self.log.error("=" * 80)

        self.assertTrue(
            all_passed,
            f"{failed_count} iteration(s) exceeded disk growth 2x threshold"
        )
