import json
import os
import subprocess
import threading
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_server_rest_util.fusion.fusion_api import FusionRestAPI
from cb_tools.cbstats import Cbstats
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_base import MagmaBaseTest
from shell_util.remote_connection import RemoteMachineShellConnection


class FusionSanity(MagmaBaseTest, FusionBase):
    def setUp(self):
        super(FusionSanity, self).setUp()

        self.log.info("FusionSanity setUp")

        self.upsert_iterations = self.input.param("upsert_iterations", 1)
        self.validate_docs = self.input.param("validate_docs", True)
        self.read_ops_rate = self.input.param("read_ops_rate", 10000)
        self.num_docs_to_validate = self.input.param("num_docs_to_validate", 1000000)
        self.rebalance_num_docs = self.input.param("rebalance_num_docs", 1000000)
        self.read_process_concurrency = self.input.param("read_process_concurrency", 8)
        self.validate_batch_size = self.input.param("validate_batch_size", 500000)

        self.crash_during_test = self.input.param("crash_during_test", False)
        self.crash_interval = self.input.param("crash_interval", 120)
        self.monitor_log_store = self.input.param("monitor_log_store", True)

        self.rebalance_master = self.input.param("rebalance_master", False)

        self.kvstore_stats = dict()
        for bucket in self.cluster.buckets:
            self.kvstore_stats[bucket.name] = dict()
            for i in range(bucket.numVBuckets):
                self.kvstore_stats[bucket.name][i] = dict()


    def tearDown(self):

        super(FusionSanity, self).tearDown()


    def test_fusion_data_lifecycle(self):

        self.log.info("Running Fusion Data Lifecycle Test")

        if self.monitor_log_store:
            monitor_threads = self.start_monitor_dir()

        if self.crash_during_test:
            crash_th = threading.Thread(target=self.kill_memcached_on_nodes, args=[self.crash_interval])
            crash_th.start()

        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        num_rebalances_to_perform = 2 * (len(self.cluster.servers) - self.nodes_init)
        num_rebalance_count = 1

        while num_rebalances_to_perform > 0:
            num_upsert_iterations = self.upsert_iterations
            while num_upsert_iterations > 0:
                self.log.info(f"Performing update workload iteration: {self.upsert_iterations - num_upsert_iterations + 1}")
                num_upsert_iterations -= 1
                self.perform_workload(0, self.num_items, doc_op="update")
                self.sleep(30, "Wait after update workload")

            sleep_time = 120 + self.fusion_upload_interval + 30
            self.sleep(sleep_time, "Sleep after update workload")

            # Perform reads while log cleaning is happening
            self.perform_batch_reads()
            self.capture_fusion_stats()

            # Set Migration Rate Limit to 0 so that extent migration doesn't take place
            ClusterRestAPI(self.cluster.master).\
                    manage_global_memcached_setting(fusion_migration_rate_limit=0)

            # Perform a data workload in parallel when rebalance is taking place
            self.log.info("Performing data load during rebalance")
            doc_loading_tasks = self.perform_workload(self.num_items,
                                                      self.num_items + self.rebalance_num_docs,
                                                      wait=False, ops_rate=10000)

            self.log.info("Running a Fusion rebalance")
            nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                                  rebalance_count=num_rebalance_count)

            # Wait for doc load to complete
            for task in doc_loading_tasks:
                self.doc_loading_tm.get_task_result(task)

            # Perform reads when no extent migration has taken place yet
            self.perform_batch_reads()
            self.capture_fusion_stats()

            # Update Migration Rate Limit so that extent migration process starts
            ClusterRestAPI(self.cluster.master).\
                    manage_global_memcached_setting(fusion_migration_rate_limit=self.fusion_migration_rate_limit)

            extent_migration_array = list()
            self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
            for node in nodes_to_monitor:
                for bucket in self.cluster.buckets:
                    extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                    extent_th.start()
                    extent_migration_array.append(extent_th)

            for th in extent_migration_array:
                th.join()

            self.cluster_util.print_cluster_stats(self.cluster)

            status, content = FusionRestAPI(self.cluster.master).get_active_guest_volumes()
            self.log.info(f"Active Guest Volumes, Status: {status}, Content: {content}")

            # Deleting guest volumes after extent migration
            self.log_store_rebalance_cleanup()

            self.log.info("Performing a read workload post extent migration")
            self.perform_batch_reads()
            self.capture_fusion_stats()

            if len(self.cluster.nodes_in_cluster) == len(self.cluster.servers):
                self.num_nodes_to_rebalance_out = 1
                self.num_nodes_to_rebalance_in = 0

            num_rebalances_to_perform -= 1
            num_rebalance_count += 1

            self.log.info("Validating item count after rebalance")
            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets)
            self.bucket_util.verify_stats_all_buckets(self.cluster,
                                                      self.num_items)

        self.monitor = False
        self.crash_loop = False
        if self.crash_during_test:
            crash_th.join()
        if self.monitor_log_store:
            for th in monitor_threads:
                th.join()

        stat_file_path = os.path.join(self.fusion_output_dir, "kvstore_stats.json")
        with open(stat_file_path, "w") as f:
            json.dump(self.kvstore_stats, f, indent=4)


    def test_fusion_rebalance(self):

        self.validate_reupload = self.input.param("validate_reupload", False)

        self.log.info("Running Fusion Rebalance Test")

        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 60
        self.sleep(sleep_time, "Sleep after data loading")

        # Perform a data workload in parallel when rebalance is taking place
        doc_loading_tasks = self.perform_workload(self.num_items,
                                                  self.num_items + self.rebalance_num_docs,
                                                  wait=False, ops_rate=10000)

        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1,
                                              rebalance_master=self.rebalance_master)

        # Wait for doc load to complete
        for task in doc_loading_tasks:
            self.doc_loading_tm.get_task_result(task)

        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        for th in extent_migration_array:
            th.join()

        self.cluster_util.print_cluster_stats(self.cluster)

        self.log.info("Validating item count after rebalance")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster,
                                                  self.num_items)

        if self.validate_reupload:
            # Perform a data load post rebalance
            self.perform_workload(self.num_items, self.num_items + (self.num_items // 2), ops_rate=20000)
            sleep_time = 120 + (2 * self.fusion_upload_interval) + 60
            self.sleep(sleep_time, "Sleep after data loading")

            self.verify_fusion_reupload()


    def test_fusion_rebalance_at_scale(self):

        self.log.info("Running Fusion Rebalance Test")
        num_bucket_batch_size = self.input.param("num_bucket_batch_size", 2)
        rebalance_data_load = self.input.param("rebalance_data_load", True)
        wait_before_rebalance_load_time = self.input.param("wait_before_rebalance_load_time", 100)
        self.rebalance_ops_rate = self.input.param("rebalance_ops_rate", 10000)
        rebalance_sleep_time = self.input.param("rebalance_sleep_time", 120)

        for i in range(0, len(self.cluster.buckets), num_bucket_batch_size):
            buckets_batch = self.cluster.buckets[i:i+num_bucket_batch_size]
            bucket_names = ', '.join(bucket.name for bucket in buckets_batch)
            self.log.info(f"Performing data load on {bucket_names}")
            self.perform_workload(0, self.num_items, buckets=buckets_batch, ops_rate=10000)
            self.sleep(10, "Wait after one batch")

        sleep_time = 300 + self.fusion_upload_interval + 60
        self.sleep(sleep_time, "Sleep after data loading")

        # Perform a data workload in parallel when rebalance is taking place
        if rebalance_data_load:
            doc_load_th = threading.Thread(target=self.run_workload_custom, args=[num_bucket_batch_size, int(wait_before_rebalance_load_time)])
            doc_load_th.start()

        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1,
                                              rebalance_master=self.rebalance_master,
                                              rebalance_sleep_time=rebalance_sleep_time)

        if rebalance_data_load:
            doc_load_th.join()

        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        for th in extent_migration_array:
            th.join()

        self.cluster_util.print_cluster_stats(self.cluster)

        self.log.info("Validating item count after rebalance")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster,
                                                  self.num_items)


    def capture_fusion_stats(self):

        self.fusion_read_stats = dict()

        for bucket in self.cluster.buckets:
            self.fusion_read_stats[bucket.name] = dict()
            for node in self.cluster.nodes_in_cluster:
                self.fusion_read_stats[bucket.name][node.ip] = dict()
                cbstats = Cbstats(node)
                result = cbstats.all_stats(bucket.name)
                for stat in ["ep_fusion_log_store_reads", "ep_fusion_log_store_remote_deletes",
                             "ep_fusion_log_store_remote_gets", "ep_fusion_log_store_remote_lists",
                             "ep_fusion_log_store_remote_puts"]:
                    self.fusion_read_stats[bucket.name][node.ip][stat] = result[stat]

        self.log.info(f"Fusion Stats: {self.fusion_read_stats}")


    def verify_fusion_reupload(self):

        shell = RemoteMachineShellConnection(self.nfs_server)

        for bucket in self.cluster.buckets:
            for kvstore_num in range(128):
                kvstore_path = f"{self.nfs_server_path}/kv/{bucket.uuid}/kvstore-{kvstore_num}/"
                find_cmd = f"find {kvstore_path} -name 'log-1.*'"
                o, e = shell.execute_command(find_cmd)
                self.log.info(f"Executing CMD: {find_cmd}, O = {o[:2]}, E = {e[:2]}")
                if len(o) == 0:
                    self.log.info(f"No log files with term num 1 found in bucket: {bucket.name}, kvstore-{kvstore_num}")

        shell.disconnect()


    def run_workload_custom(self, bucket_batch_size, sleep_before_start=0):

        self.sleep(sleep_before_start, "Sleep before running workloads during rebalance")

        for i in range(0, len(self.cluster.buckets), bucket_batch_size):
            buckets_batch = self.cluster.buckets[i:i+bucket_batch_size]
            bucket_names = ', '.join(bucket.name for bucket in buckets_batch)
            self.log.info(f"Performing data load on {bucket_names}")
            self.perform_workload(self.num_items, self.num_items + self.rebalance_num_docs,
                                  buckets=buckets_batch, ops_rate=self.rebalance_ops_rate)
            self.sleep(10, "Wait after one batch")
