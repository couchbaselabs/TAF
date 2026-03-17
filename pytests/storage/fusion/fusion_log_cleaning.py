import os
import threading
import time
from cb_tools.cbstats import Cbstats
from storage.fusion.fusion_base import FusionBase
from storage.fusion.fusion_sync import FusionSync
from shell_util.remote_connection import RemoteMachineShellConnection


class FusionLogCleaning(FusionSync, FusionBase):
    def setUp(self):
        super(FusionLogCleaning, self).setUp()
        self.monitor_log_store = self.input.param("monitor_log_store", True)
        self.upload_ops_rate = self.input.param("upload_ops_rate", 20000)

        self.log.info("FusionLogCleaning setUp Started")

    def tearDown(self):
        super(FusionLogCleaning, self).tearDown()


    def test_monitor_fusion_disk_usage(self):

        bucket = self.cluster.buckets[0]

        self.log.info("Monitor Fusion Log Store Disk Usage Test Started")

        # Start Monitoring Fusion Sync Stats
        self.sync_stats_th = threading.Thread(target=self.get_fusion_sync_stats_continuously)
        self.sync_stats_th.start()

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        monitor_du_threads = list()
        monitor_cbstats_threads = list()
        for bucket in self.cluster.buckets:
            th = threading.Thread(target=self.monitor_fusion_du, args=[bucket, True])
            monitor_du_threads.append(th)
            th.start()
            th1 = threading.Thread(target=self.monitor_log_store_stats, args=[bucket])
            monitor_cbstats_threads.append(th1)
            th1.start()

        self.perform_multiple_updates(upsert_iterations=self.upsert_iterations)

        self.monitor_stats = False
        self.monitor_sync_stats = True
        for th in monitor_du_threads:
            th.join()
        for th in monitor_cbstats_threads:
            th.join()
        self.sync_stats_th.join()


    def test_log_cleaning_during_rebalance(self):

        bucket = self.cluster.buckets[0]

        self.log.info("Log Cleaning During Rebalance Test Started")

        monitor_count_th_array = list()
        for bucket in self.cluster.buckets:
            monitor_th = threading.Thread(target=self.monitor_log_count, args=[bucket, 5, 18000])
            monitor_count_th_array.append(monitor_th)
            monitor_th.start()

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 20
        self.sleep(sleep_time, "Sleep after data loading")

        update_th = threading.Thread(target=self.perform_multiple_updates, args=[1, 90])
        update_th.start()

        monitor_th_array = list()
        monitor_du_threads = list()
        for bucket in self.cluster.buckets:
            monitor_th = threading.Thread(target=self.monitor_log_store_stats, args=[bucket])
            monitor_th_array.append(monitor_th)
            monitor_th.start()
            th = threading.Thread(target=self.monitor_fusion_du, args=[bucket])
            monitor_du_threads.append(th)
            th.start()

        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1,
                                              rebalance_sleep_time=900)

        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        for th in extent_migration_array:
            th.join()

        self.sleep(600, "Wait before stopping monitor threads")

        self.monitor_stats = False
        update_th.join()
        for th in monitor_th_array:
            th.join()
        for th in monitor_du_threads:
            th.join()
        self.log_count_monitor = False
        for th in monitor_count_th_array:
            th.join()


    def test_log_cleaning_with_history(self):
        '''
        Enable History on the bucket
        log_store_frag_threshold = 0.2
        Verify using magma_dump that all sequence numbers are present before and after Fusion Rebalance
        '''

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")
        self.bucket_util.print_bucket_stats(self.cluster)

        monitor_du_threads = list()
        monitor_cbstats_threads = list()
        for bucket in self.cluster.buckets:
            th = threading.Thread(target=self.monitor_fusion_du, args=[bucket, True])
            monitor_du_threads.append(th)
            th.start()
            th1 = threading.Thread(target=self.monitor_log_store_stats, args=[bucket])
            monitor_cbstats_threads.append(th1)
            th1.start()

        self.perform_multiple_updates(upsert_iterations=self.upsert_iterations)

        self.sleep(120, "Sleep after update workload")
        self.bucket_util.print_bucket_stats(self.cluster)

        # Get sequence number count
        seq_count_before_rebalance = dict()
        for bucket in self.cluster.buckets:
            count = self.get_seqnumber_count(bucket=bucket, with_history=True)
            seq_count_before_rebalance[bucket.name] = count
        self.log.info(f"Seq number count before rebalance = {seq_count_before_rebalance}")

        self.monitor_stats = False
        for th in monitor_du_threads:
            th.join()
        for th in monitor_cbstats_threads:
            th.join()

        # Perform a Fusion Rebalance
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1,
                                              rebalance_sleep_time=60)
        self.sleep(30, "Wait after rebalance")

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
        self.bucket_util.print_bucket_stats(self.cluster)

        # Get sequence number count
        seq_count_after_rebalance = dict()
        for bucket in self.cluster.buckets:
            count = self.get_seqnumber_count(bucket=bucket, with_history=True)
            seq_count_after_rebalance[bucket.name] = count
        self.log.info(f"Seq number count after rebalance = {seq_count_after_rebalance}")

        # Verify that all seq numbers are present
        mismatch = 0
        for bucket in self.cluster.buckets:
            if seq_count_before_rebalance[bucket.name] != seq_count_after_rebalance[bucket.name]:
                mismatch += 1
                self.log.error(f"Seq count mismatch for bucket: {bucket.name}, "
                              f"Expected: {seq_count_before_rebalance[bucket.name]}, "
                              f"Actual: {seq_count_after_rebalance[bucket.name]}")

        if mismatch > 0:
            self.fail("Mismtach in seq number validation")


    def test_validate_log_deletes(self):

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 20
        self.sleep(sleep_time, "Sleep after data loading")

        self.log.info("Starting monitoring tool which checks for correct file deletes")
        monitor_threads = self.start_monitor_dir(validate=True)

        self.perform_multiple_updates(upsert_iterations=self.upsert_iterations)

        self.monitor = False
        for th in monitor_threads:
            th.join()


    def test_fusion_log_count_based_cleaning(self):

        monitor_th_array = list()
        for bucket in self.cluster.buckets:
            monitor_th = threading.Thread(target=self.monitor_log_count, args=[bucket])
            monitor_th_array.append(monitor_th)
            monitor_th.start()

        self.log.info("Starting initial load")
        self.initial_load()
        self.sleep(30, "Sleep after data loading")

        # Perform another workload
        self.log.info("Starting another create workload")
        self.perform_workload(self.num_items, self.num_items * 2, doc_op="create", ops_rate=20000)
        sleep_time = 120 + self.fusion_upload_interval + 60
        self.sleep(sleep_time, "Sleep after data loading")

        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1,
                                              rebalance_sleep_time=60)
        self.sleep(10, "Wait after rebalance")

        # Perform another workload
        self.log.info("Starting a create workload after rebalance")
        doc_loading_tasks = self.perform_workload(self.num_items, self.num_items * 1.5,
                                                  doc_op="create", ops_rate=20000, wait=False)

        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        for th in extent_migration_array:
            th.join()

        for task in doc_loading_tasks:
            self.doc_loading_tm.get_task_result(task)

        self.sleep(120, "Wait before stopping monitor threads")

        self.log_count_monitor = False
        for th in monitor_th_array:
            th.join()


    def test_log_cleaning_stress_test(self):

        '''
        Log Cleaning + FrontEnd Workload
        log_cleaning_threshold = 0.2
        '''

        initial_num_items = self.num_items

        num_bucket_batch_size = self.input.param("num_bucket_batch_size", 1)
        num_upsert_iterations = self.input.param("num_upsert_iterations", 2)
        upsert_data_pct = self.input.param("upsert_data_pct", 1) # 100%
        total_ops_rate = self.input.param("total_ops_rate", 10000)

        monitor_cbstats_threads = list()
        for bucket in self.cluster.buckets:
            th1 = threading.Thread(target=self.monitor_log_store_stats, args=[bucket])
            monitor_cbstats_threads.append(th1)
            th1.start()

        self.sync_stats_th = threading.Thread(target=self.get_fusion_sync_stats_continuously, args=[172800, 60, False])
        self.sync_stats_th.start()

        create_ops_rate = int(total_ops_rate // num_bucket_batch_size)
        self.log.info(f"Create ops rate = {create_ops_rate}")

        self.log.info(f"Starting initial workload")
        for i in range(0, len(self.cluster.buckets), num_bucket_batch_size):
            buckets_batch = self.cluster.buckets[i:i+num_bucket_batch_size]
            bucket_names = ', '.join(bucket.name for bucket in buckets_batch)
            self.log.info(f"Performing data load on {bucket_names}")
            self.perform_workload(0, self.num_items, buckets=buckets_batch, ops_rate=create_ops_rate)
            self.sleep(10, "Wait after one batch")

        sleep_time = 120
        self.sleep(sleep_time, "Sleep after initial workload")
        self.bucket_util.print_bucket_stats(self.cluster)

        create_ops_rate = (total_ops_rate // num_bucket_batch_size) // 2
        update_ops_rate = (total_ops_rate // num_bucket_batch_size) // 2
        self.log.info(f"Create ops rate = {create_ops_rate}")
        self.log.info(f"Update ops rate = {update_ops_rate}")

        mutate = 1
        for i in range(num_upsert_iterations):
            for i in range(0, len(self.cluster.buckets), num_bucket_batch_size):
                buckets_batch = self.cluster.buckets[i:i+num_bucket_batch_size]
                bucket_names = ', '.join(bucket.name for bucket in buckets_batch)
                self.log.info(f"Performing data load on {bucket_names}")
                num_items = self.num_items
                update_th = threading.Thread(target=self.perform_workload, args=[
                    0, initial_num_items * upsert_data_pct, "update", True, buckets_batch, int(update_ops_rate), mutate])
                update_th.start()
                self.sleep(60, "Sleep before starting create workload in parallel")
                self.log.info("Starting create workload in parallel")
                create_th = threading.Thread(target=self.perform_workload, args= [
                    num_items, num_items + initial_num_items, "create", True, buckets_batch, int(create_ops_rate)])
                create_th.start()

                # Wait till update and create workloads are complete
                update_th.join()
                create_th.join()
                self.bucket_util.print_bucket_stats(self.cluster)

            mutate += 1

        sleep_time = 120
        self.sleep(sleep_time, "Sleep after update:create workload")

        # Perform a Fusion Rebalance
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1,
                                              rebalance_sleep_time=60)
        self.sleep(10, "Wait after rebalance")

        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        for th in extent_migration_array:
            th.join()

        # Stop all monitoring threads
        self.monitor_stats = False
        for th in monitor_cbstats_threads:
            th.join()
        self.monitor_sync_stats = False
        self.sync_stats_th.join()


    def perform_multiple_updates(self, upsert_iterations=None, wait_time_before_start=30, ops_rate=10000):

        self.sleep(wait_time_before_start, "Wait before starting update workload")

        num_upsert_iterations = upsert_iterations if upsert_iterations is not None else self.upsert_iterations

        mutate = 1
        while num_upsert_iterations > 0:
            self.doc_ops = "update"
            self.reset_doc_params()
            self.update_start = 0
            self.update_end = self.num_items
            self.log.info(f"Performing update workload iteration: {self.upsert_iterations - num_upsert_iterations + 1}")
            self.log.info(f"Update start = {self.update_start}, Update End = {self.update_end}")
            self.java_doc_loader(wait=True, skip_default=self.skip_load_to_default_collection, monitor_ops=False, mutate=mutate, ops_rate=ops_rate)
            num_upsert_iterations -= 1
            mutate += 1
            self.sleep(30, "Wait after update workload")


    def monitor_log_count(self, bucket, interval=5, timeout=3600):

        self.log.info(f"Monitoring log file count for bucket: {bucket.name}")

        ssh = RemoteMachineShellConnection(self.nfs_server)

        violation_count = 0
        violation_array = list()
        max_log_count = bucket.numVBuckets * self.fusion_max_num_log_files
        self.log.info(f"Max log count: {max_log_count}")

        self.log_count_monitor = True
        end_time = time.time() + timeout

        bucket_path = os.path.join(self.nfs_server_path, "kv", bucket.uuid)
        log_count_cmd = f"find {bucket_path} -name 'log-*' | wc -l"
        self.log.info(f"Log Count CMD: {log_count_cmd}")

        while self.log_count_monitor and time.time() < end_time:

            try:
                o, e = ssh.execute_command(log_count_cmd)
                self.log.info(f"Bucket: {bucket.name}, Log Count: {o[0]}")

                if int(o[0]) > max_log_count:
                    violation_count += 1
                    violation_array.append(int(o[0]))

            except Exception as e:
                self.log.info(e)

            self.sleep(interval)

        self.log.info(f"Bucket: {bucket.name}, Violation Count: {violation_count}, Violation Array: {violation_array}")
