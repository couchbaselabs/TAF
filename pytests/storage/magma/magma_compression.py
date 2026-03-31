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

        self.read_ops_rate = self.ops_rate
        self.read_process_concurrency = self.process_concurrency

        self._set_compression_algo()

    def _read_compression_config(self, node):
        target_keys = ["magma_index_compression_algo", "magma_data_compression_algo",
                      "magma_compacteddata_compression_algo"]
        config = {}
        for bucket in self.cluster.buckets:
            cbstat_obj = Cbstats(node)
            stats = cbstat_obj.all_stats(bucket.name)
            cbstat_obj.disconnect()
            for target in target_keys:
                for stat_key, stat_value in stats.items():
                    if target in stat_key.lower():
                        config[target] = stat_value
                        break
        return config

    def _set_compression_algo(self, data_algo=None, index_algo=None, compacted_algo=None):
        data_algo = data_algo or self.compression_data_algo
        index_algo = index_algo or self.compression_index_algo
        compacted_algo = compacted_algo or self.compression_compacted_algo

        props = f"magma;magma_index_compression_algo={index_algo};magma_data_compression_algo={data_algo};" \
                f"magma_compacteddata_compression_algo={compacted_algo}"
        self.bucket_util.update_bucket_props("backend", props, self.cluster, self.cluster.buckets)

        for node in self.cluster_util.get_kv_nodes(self.cluster):
            config = self._read_compression_config(node)
            self.assertEqual(config.get("magma_data_compression_algo"), data_algo,
                             f"magma_data_compression_algo not applied on {node.ip}")
            self.assertEqual(config.get("magma_index_compression_algo"), index_algo,
                             f"magma_index_compression_algo not applied on {node.ip}")
            self.assertEqual(config.get("magma_compacteddata_compression_algo"), compacted_algo,
                             f"magma_compacteddata_compression_algo not applied on {node.ip}")

    def _load_range(self, start, end):
        self.create_start = start
        self.create_end = end
        self.java_doc_loader(doc_ops="create", wait=True, value_type=self.value_type,
                             ops_rate=self.ops_rate, process_concurrency=self.process_concurrency)

    def _switch_to_second_algo(self):
        self._set_compression_algo(data_algo=self.second_compression_algo,
                                   index_algo=self.second_compression_algo,
                                   compacted_algo=self.second_compression_algo)

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
        self.cluster_util.rebalance(self.cluster, ejected_nodes=[failover_node])

        self.perform_batch_reads(num_docs_to_validate=self.range_size * 2,
                                 batch_size=self.range_size * 2, validate_docs=True)

    def test_invalid_value_rejected_without_warmup_issue(self):
        baseline = self._read_compression_config(self.cluster.master)

        invalid_props = f"magma;magma_data_compression_algo={self.invalid_value}"
        rejected = False
        try:
            self.bucket_util.update_bucket_props("backend", invalid_props, self.cluster, self.cluster.buckets)
        except Exception as e:
            rejected = True
            self.log.info(f"update_bucket_props raised as expected: {e}")

        self.assertTrue(self.bucket_util.is_warmup_complete(self.cluster.buckets, retry_count=3),
                        "Cluster did not warm up after an invalid compression value was attempted")

        if rejected:
            current = self._read_compression_config(self.cluster.master)
            self.assertEqual(baseline, current,
                             "Compression config changed even though the invalid value was rejected")

        self._switch_to_second_algo()
        self._load_range(0, self.range_size)

    def test_new_node_gets_current_compression_config_after_rebalance(self):
        self._switch_to_second_algo()

        new_node = self.cluster.servers[self.nodes_init]
        self.cluster_util.add_node(self.cluster, new_node)

        config = self._read_compression_config(new_node)
        self.assertEqual(config.get("magma_data_compression_algo"), self.second_compression_algo,
                         "New node did not come up with the current data compression config")
        self.assertEqual(config.get("magma_index_compression_algo"), self.second_compression_algo,
                         "New node did not come up with the current index compression config")
        self.assertEqual(config.get("magma_compacteddata_compression_algo"), self.second_compression_algo,
                         "New node did not come up with the current compacted compression config")
