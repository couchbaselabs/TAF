import json
import os
import subprocess
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_base import MagmaBaseTest


class FusionSync(MagmaBaseTest, FusionBase):
    def setUp(self):
        super(FusionSync, self).setUp()

        self.log.info("FusionSync setUp Started")

        split_path = self.local_test_path.split("/")
        self.fusion_output_dir = "/" + os.path.join("/".join(split_path[1:4]), "fusion_output")
        self.log.info(f"Fusion output dir = {self.fusion_output_dir}")
        subprocess.run(f"mkdir -p {self.fusion_output_dir}", shell=True, executable="/bin/bash")

        self.upsert_iterations = self.input.param("upsert_iterations", 2)
        self.monitor_log_store = self.input.param("monitor_log_store", True)

        # Override Fusion default settings
        for bucket in self.cluster.buckets:
            self.change_fusion_settings(bucket, upload_interval=self.fusion_upload_interval,
                                        checkpoint_interval=self.fusion_log_checkpoint_interval,
                                        logstore_frag_threshold=self.logstore_frag_threshold)
        # Set Migration Rate Limit
        ClusterRestAPI(self.cluster.master).\
            manage_global_memcached_setting(fusion_migration_rate_limit=self.fusion_migration_rate_limit)

        # Maintain two dicts. One to monitor and one to record violations
        if self.monitor_log_store:
            self.kvstore_stats = dict()
            self.kvstore_violations = dict()
            for bucket in self.cluster.buckets:
                self.kvstore_stats[bucket.name] = dict()
                self.kvstore_violations[bucket.name] = dict()
                for i in range(bucket.numVBuckets):
                    self.kvstore_stats[bucket.name][i] = dict()
                    self.kvstore_violations[bucket.name][i] = dict()

    def tearDown(self):
        if self.monitor_log_store:
            self.monitor = False
            self.sleep(10, "Wait after stopping monitor threads")

            for bucket in self.cluster.buckets:
                for kvstore_num in range(bucket.numVBuckets):
                    self.kvstore_stats[bucket.name][kvstore_num]["file_creation"] = \
                        list(self.kvstore_stats[bucket.name][kvstore_num]["file_creation"])
                    self.kvstore_stats[bucket.name][kvstore_num]["file_deletion"] = \
                        list(self.kvstore_stats[bucket.name][kvstore_num]["file_deletion"])

            stat_file_path = os.path.join(self.fusion_output_dir, "kvstore_stats.json")
            with open(stat_file_path, "w") as f:
                json.dump(self.kvstore_stats, f, indent=4)

            violation_file_path = os.path.join(self.fusion_output_dir, "kvstore_violations.json")
            with open(violation_file_path, "w") as f:
                json.dump(self.kvstore_violations, f, indent=4)

        super(FusionSync, self).tearDown()


    def test_fusion_sync(self):

        self.log.info("Fusion Sync Test Started")

        if self.monitor_log_store:
            monitor_threads = self.start_monitor_dir(validate=True)

        self.log.info("Starting initial load")
        self.initial_load()
        self.sleep(30, "Sleep after data loading")

        num_upsert_iterations = self.upsert_iterations
        while num_upsert_iterations > 0:
            self.doc_ops = "update"
            self.reset_doc_params()
            self.update_start = 0
            self.update_end = self.num_items
            self.log.info(f"Performing update workload iteration: {self.upsert_iterations - num_upsert_iterations + 1}")
            self.log.info(f"Update start = {self.update_start}, Update End = {self.update_end}")
            self.java_doc_loader(wait=True, skip_default=self.skip_load_to_default_collection, monitor_ops=False)
            num_upsert_iterations -= 1
            self.sleep(30, "Wait after update workload")

        if self.monitor_log_store:
            self.monitor = False
            for th in monitor_threads:
                th.join()
