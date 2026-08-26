"""
Functional correctness tests for Magma bucket-level compression settings:
- magma_index_compression_algo
- magma_data_compression_algo
- magma_compacteddata_compression_algo

Unlike magma_compression_benchmarking.py (ratio/CPU/latency), these tests only
check whether data stays correct across a compression algo change - readback
of the changed config, mixed old/new algo blocks on disk, compaction, crash
recovery, replica promotion, invalid values, and rebalance-in.
"""

import time

from cb_server_rest_util.buckets.buckets_api import BucketRestApi
from cb_tools.cbstats import Cbstats
from storage.magma.magma_base import MagmaBaseTest


class MagmaCompressionFunctionalTests(MagmaBaseTest):
    def setUp(self):
        super().setUp()

        self.value_type = self.input.param("value_type", "SimpleValue")
        self.range_size = self.input.param("range_size", 5000000)
        self.compression_data_algo = self.input.param("compression_data_algo", "lz4")
        self.compression_index_algo = self.input.param("compression_index_algo", "lz4")
        self.compression_compacted_algo = self.input.param("compression_compacted_algo", "lz4")
        self.second_compression_algo = self.input.param("second_compression_algo", "zstd")
        self.invalid_value = self.input.param("invalid_value", "gzip")
        self.iterations = self.input.param("iterations", 1)

        self.read_ops_rate = self.ops_rate
        self.read_process_concurrency = self.process_concurrency

        self._set_compression_algo(context="setUp_default")

    def _read_compression_config(self, node):
        target_keys = ["magma_index_compression_algo", "magma_data_compression_algo",
                      "magma_compacteddata_compression_algo"]
        config = {}
        for bucket in self.cluster.buckets:
            cbstat_obj = Cbstats(node)
            stats = cbstat_obj.all_stats(bucket.name)
            cbstat_obj.disconnect()
            bucket_config = {}
            for target in target_keys:
                for stat_key, stat_value in stats.items():
                    if target in stat_key.lower():
                        bucket_config[target] = stat_value
                        break
            config[bucket.name] = bucket_config
        return config

    def _set_compression_algo(self, data_algo=None, index_algo=None, compacted_algo=None,
                              context="set_compression_algo"):
        data_algo = data_algo or self.compression_data_algo
        index_algo = index_algo or self.compression_index_algo
        compacted_algo = compacted_algo or self.compression_compacted_algo

        for node in self.cluster_util.get_kv_nodes(self.cluster):
            self.log.info(f"[{context}] Requesting data={data_algo}, index={index_algo}, "
                          f"compacted={compacted_algo} | current config on {node.ip}: "
                          f"{self._read_compression_config(node)}")

        # Public bucket-settings REST API (documented), not diag_eval - see
        # kv_engine commit ecfa337 (MB-71037) for the public_since param names.
        params = {
            "magmaDataCompressionAlgo": data_algo,
            "magmaIndexCompressionAlgo": index_algo,
            "magmaCompacteddataCompressionAlgo": compacted_algo,
        }
        rest = BucketRestApi(self.cluster.master)
        for bucket in self.cluster.buckets:
            status, content = rest.edit_bucket(bucket.name, params)
            self.assertTrue(status, f"[{context}] edit_bucket failed for {bucket.name}: {content}")

        warmed_up = self.bucket_util.is_warmup_complete(self.cluster.buckets, retry_count=10)
        self.assertTrue(warmed_up, f"[{context}] Bucket did not warm up after compression config "
                                   f"change via REST API")

        self.sleep(10, f"[{context}] Wait 10 seconds for memcached to apply the new compression config")

        for node in self.cluster_util.get_kv_nodes(self.cluster):
            config = self._read_compression_config(node)
            self.log.info(f"[{context}] Applied data={data_algo}, index={index_algo}, "
                          f"compacted={compacted_algo} | config on {node.ip} after change: {config}")
            for bucket in self.cluster.buckets:
                bucket_config = config.get(bucket.name, {})
                self.assertEqual(bucket_config.get("magma_data_compression_algo"), data_algo,
                                 f"[{context}] magma_data_compression_algo not applied on "
                                 f"{bucket.name}@{node.ip}")
                self.assertEqual(bucket_config.get("magma_index_compression_algo"), index_algo,
                                 f"[{context}] magma_index_compression_algo not applied on "
                                 f"{bucket.name}@{node.ip}")
                self.assertEqual(bucket_config.get("magma_compacteddata_compression_algo"), compacted_algo,
                                 f"[{context}] magma_compacteddata_compression_algo not applied on "
                                 f"{bucket.name}@{node.ip}")

    def _assert_fragmentation_below(self, threshold=50):
        for node in self.cluster_util.get_kv_nodes(self.cluster):
            cbstat_obj = Cbstats(node)
            for bucket in self.cluster.buckets:
                stats = cbstat_obj.all_stats(bucket.name)
                frag_pct = float(stats["ep_magma_fragmentation"]) * 100
                self.log.info(f"Magma fragmentation for {bucket.name}@{node.ip}: {frag_pct:.2f}%")
                self.assertLess(frag_pct, threshold,
                                f"Fragmentation {frag_pct:.2f}% on {bucket.name}@{node.ip} "
                                f"exceeds {threshold}%")
            cbstat_obj.disconnect()

    def _load_range(self, start, end):
        self.create_start = start
        self.create_end = end
        self.java_doc_loader(doc_ops="create", wait=True, value_type=self.value_type,
                             ops_rate=self.ops_rate, process_concurrency=self.process_concurrency)

        self.update_start = start
        self.update_end = end
        for i in range(self.iterations - 1):
            self.log.info(f"Update iteration {i + 1}/{self.iterations - 1} on range [{start}, {end})")
            self.java_doc_loader(doc_ops="update", wait=True, value_type=self.value_type,
                                 ops_rate=self.ops_rate, process_concurrency=self.process_concurrency)

        self._assert_fragmentation_below(50)

    def _switch_to_second_algo(self):
        self._set_compression_algo(data_algo=self.second_compression_algo,
                                   index_algo=self.second_compression_algo,
                                   compacted_algo=self.second_compression_algo,
                                   context="switch_to_second_algo")

    def test_config_change_applied(self):
        self._load_range(0, self.range_size)
        self._switch_to_second_algo()

    def test_mixed_algo_read_correctness(self):
        self._load_range(0, self.range_size)
        self._switch_to_second_algo()
        self._load_range(self.range_size, self.range_size * 2)

        self.perform_batch_reads(num_docs_to_validate=self.range_size * 2,
                                 batch_size=self.range_size * 2, validate_docs=True)

    def test_compaction_rewrites_old_blocks_correctly(self):
        self._load_range(0, self.range_size)
        self._switch_to_second_algo()
        self._load_range(self.range_size, self.range_size * 2)

        for bucket in self.cluster.buckets:
            task = self.task.async_compact_bucket(self.cluster.master, bucket)
            self.task_manager.get_task_result(task)

        self.perform_batch_reads(num_docs_to_validate=self.range_size * 2,
                                 batch_size=self.range_size * 2, validate_docs=True)

    def test_compaction_disk_usage_after_algo_switch(self):
        initial_algo = self.compression_data_algo
        self._load_range(0, self.range_size)

        for bucket in self.cluster.buckets:
            task = self.task.async_compact_bucket(self.cluster.master, bucket)
            self.task_manager.get_task_result(task)
        initial_algo_disk_usage = self.get_disk_usage(self.cluster.buckets[0], self.cluster.nodes_in_cluster)[0]
        self.log.info(f"Disk usage after compaction with {initial_algo}: {initial_algo_disk_usage}MB")

        self._switch_to_second_algo()

        for bucket in self.cluster.buckets:
            task = self.task.async_compact_bucket(self.cluster.master, bucket)
            self.task_manager.get_task_result(task)
        new_algo_disk_usage = self.get_disk_usage(self.cluster.buckets[0], self.cluster.nodes_in_cluster)[0]
        self.log.info(f"Disk usage after compaction with {self.second_compression_algo}: {new_algo_disk_usage}MB")

        delta = initial_algo_disk_usage - new_algo_disk_usage
        delta_pct = (delta / initial_algo_disk_usage * 100) if initial_algo_disk_usage else 0
        rows = [
            ("Algo", "Disk Usage (MB)"),
            (initial_algo, f"{initial_algo_disk_usage:.2f}"),
            (self.second_compression_algo, f"{new_algo_disk_usage:.2f}"),
            ("Delta", f"{delta:.2f} ({delta_pct:.2f}%)"),
        ]
        table = "\n".join(f"{label:<20}{value:>20}" for label, value in rows)
        self.log.info(f"\nDisk usage comparison after compaction (range_size={self.range_size}):\n{table}")

        self.perform_batch_reads(num_docs_to_validate=self.range_size,
                                 batch_size=self.range_size, validate_docs=True)

    def test_crash_during_algo_switch_before_compaction(self):
        self._load_range(0, self.range_size)
        self._switch_to_second_algo()
        self._load_range(self.range_size, self.range_size * 2)

        self.sigkill_memcached()

        self.perform_batch_reads(num_docs_to_validate=self.range_size * 2,
                                 batch_size=self.range_size * 2, validate_docs=True)

    def test_promoted_replica_serves_data_from_both_algos(self):
        self._load_range(0, self.range_size)
        self._switch_to_second_algo()
        self._load_range(self.range_size, self.range_size * 2)

        failover_node = [node for node in self.cluster.nodes_in_cluster
                         if node.ip != self.cluster.master.ip][0]
        self.task.failover(self.cluster, failover_nodes=[failover_node], graceful=False)

        otp_node = next((n.id for n in self.cluster_util.get_nodes(
            self.cluster.master, inactive_failed=True) if n.ip == failover_node.ip), None)
        self.assertIsNotNone(otp_node, f"Could not find otp-node id for failed-over node {failover_node.ip}")
        self.cluster_util.rebalance(self.cluster, ejected_nodes=[otp_node])

        self.perform_batch_reads(num_docs_to_validate=self.range_size * 2,
                                 batch_size=self.range_size * 2, validate_docs=True)

    def test_invalid_value_rejected_without_warmup_issue(self):
        self._load_range(0, self.range_size)

        baseline = self._read_compression_config(self.cluster.master)

        rest = BucketRestApi(self.cluster.master)
        warmup_start = time.time()
        for bucket in self.cluster.buckets:
            status, content = rest.edit_bucket(bucket.name, {"magmaDataCompressionAlgo": self.invalid_value})
            self.log.info(f"edit_bucket with invalid value on {bucket.name} returned "
                          f"status={status}, content={content}")

        warmed_up = self.bucket_util.is_warmup_complete(self.cluster.buckets, retry_count=1)
        warmup_duration = time.time() - warmup_start

        self.assertTrue(warmed_up,
                        f"Cluster did not warm up after an invalid compression value was attempted "
                        f"(waited {warmup_duration:.0f}s)")
        self.assertLess(warmup_duration, self.wait_timeout,
                        f"Bucket took {warmup_duration:.0f}s to warm up after an invalid compression "
                        f"value was attempted (wait_timeout={self.wait_timeout}s) - a long warmup "
                        f"suggests memcached is stuck retrying/crash-looping on the invalid value "
                        f"instead of just rejecting it and coming back up")

        # MB-71037: invalid values return HTTP 200 and are only rejected later by
        # memcached, so this check must not be gated on the REST call's status.
        current = self._read_compression_config(self.cluster.master)
        self.assertEqual(baseline, current,
                         "Compression config changed even though the invalid value should have been rejected")
        self.sleep(30, "Wait 30 seconds to see if memcached crashes on the invalid value")

        self._switch_to_second_algo()
        self._load_range(self.range_size, self.range_size * 2)
        self.perform_batch_reads(num_docs_to_validate=self.range_size * 2,
                                 batch_size=self.range_size * 2, validate_docs=True)

    def test_independent_algo_values_per_param(self):
        data_algo, index_algo, compacted_algo = "lz4", "zstd", "zstd_19"
        self._set_compression_algo(data_algo=data_algo, index_algo=index_algo,
                                   compacted_algo=compacted_algo,
                                   context="independent_algo_values")

        self._load_range(0, self.range_size)

        for bucket in self.cluster.buckets:
            task = self.task.async_compact_bucket(self.cluster.master, bucket)
            self.task_manager.get_task_result(task)

        self._load_range(self.range_size, self.range_size * 2)

        self.perform_batch_reads(num_docs_to_validate=self.range_size * 2,
                                 batch_size=self.range_size * 2, validate_docs=True)

    def test_new_node_gets_current_compression_config_after_rebalance(self):
        self._switch_to_second_algo()
        self._load_range(0, self.range_size)

        new_node = self.cluster.servers[self.nodes_init]
        self.cluster_util.add_node(self.cluster, new_node)

        config = self._read_compression_config(new_node)
        for bucket in self.cluster.buckets:
            bucket_config = config.get(bucket.name, {})
            self.assertEqual(bucket_config.get("magma_data_compression_algo"), self.second_compression_algo,
                             f"New node did not come up with the current data compression config for {bucket.name}")
            self.assertEqual(bucket_config.get("magma_index_compression_algo"), self.second_compression_algo,
                             f"New node did not come up with the current index compression config for {bucket.name}")
            self.assertEqual(bucket_config.get("magma_compacteddata_compression_algo"), self.second_compression_algo,
                             f"New node did not come up with the current compacted compression config for {bucket.name}")

        self.perform_batch_reads(num_docs_to_validate=self.range_size,
                                 batch_size=self.range_size, validate_docs=True)

    def test_multi_bucket_compression_config_independent(self):
        self._load_range(0, self.range_size)
        self._switch_to_second_algo()
        self._load_range(self.range_size, self.range_size * 2)

        for bucket in self.cluster.buckets:
            task = self.task.async_compact_bucket(self.cluster.master, bucket)
            self.task_manager.get_task_result(task)

        config = self._read_compression_config(self.cluster.master)
        for bucket in self.cluster.buckets:
            bucket_config = config.get(bucket.name, {})
            self.assertEqual(bucket_config.get("magma_data_compression_algo"), self.second_compression_algo,
                             f"{bucket.name} data compression config incorrect: {bucket_config}")
            self.assertEqual(bucket_config.get("magma_index_compression_algo"), self.second_compression_algo,
                             f"{bucket.name} index compression config incorrect: {bucket_config}")
            self.assertEqual(bucket_config.get("magma_compacteddata_compression_algo"), self.second_compression_algo,
                             f"{bucket.name} compacted compression config incorrect: {bucket_config}")

        self.perform_batch_reads(num_docs_to_validate=self.range_size * 2,
                                 batch_size=self.range_size * 2, validate_docs=True)
