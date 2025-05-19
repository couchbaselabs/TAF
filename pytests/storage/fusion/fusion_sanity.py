import json
import os
import subprocess
import threading
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_base import MagmaBaseTest


class FusionSanity(MagmaBaseTest, FusionBase):
    def setUp(self):
        super(FusionSanity, self).setUp()

        self.log.info("FusionSanity setUp")

        self.num_nodes_to_rebalance_in = self.input.param("num_nodes_to_rebalance_in", 1)
        self.num_nodes_to_rebalance_out = self.input.param("num_nodes_to_rebalance_out", 0)
        self.num_nodes_to_swap_rebalance = self.input.param("num_nodes_to_swap_rebalance", 0)

        self.upsert_iterations = self.input.param("upsert_iterations", 1)
        self.validate_docs = self.input.param("validate_docs", True)
        self.read_ops_rate = self.input.param("read_ops_rate", 10000)
        self.num_docs_to_validate = self.input.param("num_docs_to_validate", 1000000)
        self.read_process_concurrency = self.input.param("read_process_concurrency", 8)
        self.validate_batch_size = self.input.param("validate_batch_size", 500000)

        self.crash_during_test = self.input.param("crash_during_test", False)
        self.crash_interval = self.input.param("crash_interval", 120)

        split_path = self.local_test_path.split("/")
        self.fusion_output_dir = "/" + os.path.join("/".join(split_path[1:4]), "fusion_output")
        self.log.info(f"Fusion output dir = {self.fusion_output_dir}")
        subprocess.run(f"mkdir -p {self.fusion_output_dir}", shell=True, executable="/bin/bash")

        self.kvstore_stats = dict()
        for bucket in self.cluster.buckets:
            self.kvstore_stats[bucket.name] = dict()
            for i in range(bucket.numVBuckets):
                self.kvstore_stats[bucket.name][i] = dict()


    def test_fusion_data_lifecycle(self):

        self.log.info("Running Fusion Data Lifecycle Test")

        monitor_threads = self.start_monitor_dir()

        if self.crash_during_test:
            crash_th = threading.Thread(target=self.kill_memcached_on_nodes, args=[self.crash_interval])
            crash_th.start()

        self.initial_load()

        self.sleep(30, "Sleep after data loading")

        num_rebalances_to_perform = 2 * (len(self.cluster.servers) - 1)
        num_rebalance_count = 1

        while num_rebalances_to_perform > 0:
            num_upsert_iterations = self.upsert_iterations
            while num_upsert_iterations > 0:
                self.doc_ops = "update"
                self.reset_doc_params()
                self.update_start = 0
                self.update_end = self.num_items
                self.log.info(f"Performing update workload iteration: {self.upsert_iterations - num_upsert_iterations + 1}")
                self.log.info(f"Update start = {self.update_start}, Update End = {self.update_end}, Update perc = {self.update_perc}")
                self.java_doc_loader(wait=True, skip_default=self.skip_load_to_default_collection)
                num_upsert_iterations -= 1
                self.sleep(30, "Wait after update workload")

            self.log.info("Running a Fusion rebalance")
            nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                                  rebalance_count=num_rebalance_count)

            self.validate_log_file_count_size(dir_path=self.fusion_scripts_dir)

            extent_migration_array = list()
            self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
            for node in nodes_to_monitor:
                for bucket in self.cluster.buckets:
                    extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                    extent_th.start()
                    extent_migration_array.append(extent_th)

            for th in extent_migration_array:
                th.join()

            if self.num_nodes_to_rebalance_in > 0:
                self.cluster.nodes_in_cluster.extend(self.cluster.servers[
                    len(self.cluster.nodes_in_cluster):len(self.cluster.nodes_in_cluster)+self.num_nodes_to_rebalance_in])
            elif self.num_nodes_to_rebalance_out > 0:
                self.cluster.nodes_in_cluster = self.cluster.servers[
                    :len(self.cluster.nodes_in_cluster)-abs(self.num_nodes_to_rebalance_out)]

            self.cluster_util.print_cluster_stats(self.cluster)

            self.log.info("Performing a read workload post extent migration")
            self.doc_ops = "read"
            self.reset_doc_params()
            read_start, read_end = 0, min(self.num_docs_to_validate, self.validate_batch_size)
            while read_start < self.num_docs_to_validate:
                self.read_start = read_start
                self.read_end = read_end
                self.generate_docs()
                self.log.info(f"Read start = {self.read_start}, Read End = {self.read_end}, Read perc = {self.read_perc}")
                self.java_doc_loader(wait=True,
                                    skip_default=self.skip_load_to_default_collection,
                                    validate_docs=self.validate_docs,
                                    ops_rate=self.read_ops_rate,
                                    process_concurrency=self.read_process_concurrency)
                read_start = read_end
                read_end = min(read_end + self.validate_batch_size, self.num_docs_to_validate)
                self.sleep(15, "Wait after reading the current batch")

            if len(self.cluster.nodes_in_cluster) == len(self.cluster.servers):
                self.num_nodes_to_rebalance_out = 1
                self.num_nodes_to_rebalance_in = 0

            num_rebalances_to_perform -= 1
            num_rebalance_count += 1
            self.log_store_rebalance_cleanup()

            self.log.info("Validating item count after rebalance")
            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets)
            self.bucket_util.verify_stats_all_buckets(self.cluster,
                                                      self.num_items)

        self.monitor = False
        self.crash_loop = False
        if self.crash_during_test:
            crash_th.join()
        for th in monitor_threads:
            th.join()

        stat_file_path = os.path.join(self.fusion_output_dir, "kvstore_stats.json")
        with open(stat_file_path, "w") as f:
            json.dump(self.kvstore_stats, f, indent=4)

        self.log.info(f"Copying over fusion output from host to {self.cluster.master}")
        copy_cmd = 'sshpass -p "{0}" scp -o StrictHostKeyChecking=no' \
                    ' -r {1} root@{2}:{3}'\
                        .format("couchbase", self.fusion_output_dir, self.cluster.master.ip,
                                os.path.join(self.fusion_scripts_dir, 'fusion_output'))
        self.log.debug(f"Running CMD: {copy_cmd}")
        subprocess.run(copy_cmd, shell=True, executable="/bin/bash")
