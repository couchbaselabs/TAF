import os
import threading
import time
from storage.fusion.fusion_base import FusionBase
from shell_util.remote_connection import RemoteMachineShellConnection
from storage.magma.magma_base import MagmaBaseTest


class FusionLogCleaning(MagmaBaseTest, FusionBase):
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
        self.bucket_util.print_bucket_stats(self.cluster)

        monitor_du_threads = list()
        monitor_cbstats_threads = list()
        for bucket in self.cluster.buckets:
            th = threading.Thread(target=self.monitor_fusion_du, args=[bucket, True])
            monitor_du_threads.append(th)
            th.start()
            th2 = threading.Thread(target=self.monitor_log_store_stats, args=[bucket, 15, True])
            monitor_cbstats_threads.append(th2)
            th2.start()

        self.perform_multiple_updates(upsert_iterations=self.upsert_iterations)

        self.monitor_stats = False
        for th in monitor_du_threads:
            th.join()
        for th in monitor_cbstats_threads:
            th.join()
        self.sync_stats_th.join()

        if not self.frag_cbstats_pass:
            self.fail("Cbstats Log Store Frag Values Above Threshold were found")


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
        self.bucket_util.print_bucket_stats(self.cluster)

        update_th = threading.Thread(target=self.perform_multiple_updates, args=[1, 90])
        update_th.start()

        monitor_du_threads = list()
        monitor_cbstats_threads = list()
        for bucket in self.cluster.buckets:
            th = threading.Thread(target=self.monitor_fusion_du, args=[bucket])
            monitor_du_threads.append(th)
            th.start()
            th2 = threading.Thread(target=self.monitor_log_store_stats, args=[bucket])
            monitor_cbstats_threads.append(th2)
            th2.start()

        self.log.info("Running a Fusion rebalance")
        self.run_rebalance(output_dir=self.fusion_output_dir,
                            rebalance_count=1,
                            rebalance_sleep_time=900)

        self.log.info("Monitoring active guest volumes")
        guest_volume_th = threading.Thread(target=self.monitor_active_guest_volumes)
        guest_volume_th.start()
        guest_volume_th.join()

        self.sleep(600, "Wait before stopping monitor threads")

        self.monitor_stats = False
        update_th.join()
        for th in monitor_du_threads:
            th.join()
        for th in monitor_cbstats_threads:
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
            th2 = threading.Thread(target=self.monitor_log_store_stats, args=[bucket, 15, True])
            monitor_cbstats_threads.append(th2)
            th2.start()

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

        self.log.info("Monitoring active guest volumes")
        guest_volume_th = threading.Thread(target=self.monitor_active_guest_volumes)
        guest_volume_th.start()
        guest_volume_th.join()

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

        if not self.frag_cbstats_pass:
            self.fail("Log Store Frag Values Above Threshold were found")
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

        self.log.info("Monitoring active guest volumes")
        guest_volume_th = threading.Thread(target=self.monitor_active_guest_volumes)
        guest_volume_th.start()
        guest_volume_th.join()

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

        monitor_du_threads = list()
        monitor_cbstats_threads = list()
        for bucket in self.cluster.buckets:
            th1 = threading.Thread(target=self.monitor_fusion_du, args=[bucket, True, 30])
            monitor_du_threads.append(th1)
            th1.start()
            th2 = threading.Thread(target=self.monitor_log_store_stats, args=[bucket, 30, True])
            monitor_cbstats_threads.append(th2)
            th2.start()

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
        self.run_rebalance(output_dir=self.fusion_output_dir,
                            rebalance_count=1,
                            rebalance_sleep_time=60)
        self.sleep(10, "Wait after rebalance")

        self.log.info("Monitoring active guest volumes")
        guest_volume_th = threading.Thread(target=self.monitor_active_guest_volumes)
        guest_volume_th.start()
        guest_volume_th.join()

        # Stop all monitoring threads
        self.monitor_stats = False
        for th in monitor_du_threads:
            th.join()
        for th in monitor_cbstats_threads:
            th.join()
        self.monitor_sync_stats = False
        self.sync_stats_th.join()

        if not self.frag_cbstats_pass:
            self.fail("Log Store Frag Values Above Threshold were found")


    def test_nfs_disk_usage_vs_local(self):
        """
        Verifies that the NFS log-store footprint for each bucket stays within
        an acceptable overhead ratio of the local on-disk footprint at two
        distinct checkpoints in the data lifecycle:

          Checkpoint 1 — after initial CREATE workload + sync wait.
          Checkpoint 2 — after UPDATE workload + sync wait.

        Both local and NFS sizes are measured with ``du -sb`` (bytes), so no
        unit conversion is required.  Local usage is summed across all cluster
        nodes; NFS usage is measured on self.nfs_server under
        self.nfs_server_path/kv/<bucket_uuid>.

        Steps:
          1. Load initial data via initial_load().
          2. Sleep sync_wait_sec for Fusion to flush log segments to NFS.
          3. Checkpoint 1: compare NFS vs local DU for every bucket.
          4. Run update workload (perform_multiple_updates).
          5. Sleep sync_wait_sec for Fusion to sync updated log segments.
          6. Checkpoint 2: compare NFS vs local DU for every bucket.
          7. Assert no ratio violations were recorded at either checkpoint.

        Parameters (test params):
          sync_wait_sec        : seconds to wait for NFS sync at each checkpoint
                                 (default: 600)
          nfs_overhead_ratio   : maximum allowed NFS/local ratio (default: 1.5)
          num_upsert_iterations: update passes over the full key range
                                 (default: 2)
        """
        sync_wait_sec = self.input.param("sync_wait_sec", 300)
        nfs_overhead_ratio = self.input.param("nfs_overhead_ratio", 1.5)
        num_upsert_iterations = self.input.param("num_upsert_iterations", 2)

        def _compare_du(checkpoint_label, failures):
            """Measure local + NFS DU for every bucket and record violations."""
            self.log.info(f"--- DU Checkpoint: {checkpoint_label} ---")
            for bucket in self.cluster.buckets:
                bucket_uuid = self.get_bucket_uuid(bucket.name)
                self.log.info(
                    f"[{checkpoint_label}] Bucket '{bucket.name}' "
                    f"(uuid={bucket_uuid})"
                )

                # Local disk usage — sum across all cluster nodes
                local_bytes = 0
                for server in self.cluster.nodes_in_cluster:
                    ssh = RemoteMachineShellConnection(server)
                    du_cmd = f"du -sb {self.data_path}/{bucket_uuid}"
                    output, error = ssh.execute_command(du_cmd)
                    ssh.disconnect()
                    if output:
                        try:
                            local_bytes += int(output[0].split()[0])
                        except (IndexError, ValueError) as exc:
                            self.log.warning(
                                f"[{server.ip}] Could not parse du output "
                                f"'{output}': {exc}"
                            )
                    else:
                        self.log.warning(
                            f"[{server.ip}] du returned no output for "
                            f"{self.data_path}/{bucket_uuid} (error={error})"
                        )

                self.log.info(
                    f"[{checkpoint_label}] Bucket '{bucket.name}': "
                    f"local = {local_bytes:,} bytes "
                    f"({local_bytes / (1024 ** 3):.3f} GiB) "
                    f"across {len(self.cluster.nodes_in_cluster)} node(s)"
                )

                if local_bytes == 0:
                    self.log.warning(
                        f"[{checkpoint_label}] Bucket '{bucket.name}': "
                        f"local disk usage is 0, skipping NFS comparison"
                    )
                    continue

                # NFS disk usage
                nfs_bucket_path = os.path.join(
                    self.nfs_server_path, "kv", bucket_uuid
                )
                du_cmd = f"du -sb {nfs_bucket_path}"
                self.log.info(
                    f"[{checkpoint_label}] Running NFS du cmd on "
                    f"{self.nfs_server_ip}: {du_cmd}"
                )
                ssh = RemoteMachineShellConnection(self.nfs_server)
                output, error = ssh.execute_command(du_cmd)
                ssh.disconnect()

                if not output:
                    self.log.warning(
                        f"[{checkpoint_label}] Bucket '{bucket.name}': "
                        f"du returned no output for {nfs_bucket_path} "
                        f"(error={error}) — skipping"
                    )
                    continue

                try:
                    nfs_bytes = int(output[0].split()[0])
                except (IndexError, ValueError) as exc:
                    self.log.warning(
                        f"[{checkpoint_label}] Bucket '{bucket.name}': "
                        f"Could not parse NFS du output '{output}': {exc} "
                        f"— skipping"
                    )
                    continue

                self.log.info(
                    f"[{checkpoint_label}] Bucket '{bucket.name}': "
                    f"NFS = {nfs_bytes:,} bytes "
                    f"({nfs_bytes / (1024 ** 3):.3f} GiB) "
                    f"at {nfs_bucket_path}"
                )

                ratio = nfs_bytes / local_bytes
                max_allowed = local_bytes * nfs_overhead_ratio
                self.log.info(
                    f"[{checkpoint_label}] Bucket '{bucket.name}': "
                    f"NFS/local ratio = {ratio:.3f} "
                    f"(limit = {nfs_overhead_ratio}x, "
                    f"max_allowed_nfs = {max_allowed:,.0f} bytes)"
                )

                if nfs_bytes > max_allowed:
                    msg = (
                        f"[{checkpoint_label}] Bucket '{bucket.name}': "
                        f"NFS usage ({nfs_bytes:,} bytes) exceeds "
                        f"{nfs_overhead_ratio}x local usage "
                        f"({local_bytes:,} bytes). Ratio = {ratio:.3f}"
                    )
                    self.log.error(msg)
                    failures.append(msg)
                else:
                    self.log.info(
                        f"[{checkpoint_label}] Bucket '{bucket.name}': "
                        f"PASSED ({ratio:.3f} <= {nfs_overhead_ratio})"
                    )

        failures = []

        # ---- Phase 1: initial CREATE workload ----
        self.log.info("Phase 1: Loading initial data")
        self.initial_load()
        self.bucket_util.print_bucket_stats(self.cluster)

        # ---- Phase 2: wait for NFS sync, then Checkpoint 1 ----
        self.sleep(sync_wait_sec, "Waiting for Fusion to sync CREATE data to NFS")
        _compare_du("after_create", failures)

        # ---- Phase 3: UPDATE workload — DU checked after every iteration ----
        self.log.info(
            f"Phase 3: Running update workload "
            f"({num_upsert_iterations} iteration(s)), "
            f"DU comparison after each iteration"
        )
        self.sleep(30, "Wait before starting update workload")
        mutate = 1
        remaining = num_upsert_iterations
        while remaining > 0:
            iteration = num_upsert_iterations - remaining + 1
            self.log.info(
                f"Phase 3: Update iteration {iteration}/{num_upsert_iterations}"
            )
            self.doc_ops = "update"
            self.reset_doc_params()
            self.update_start = 0
            self.update_end = self.num_items
            self.java_doc_loader(
                wait=True,
                skip_default=self.skip_load_to_default_collection,
                monitor_ops=False,
                mutate=mutate,
                ops_rate=self.ops_rate,
            )
            self.bucket_util.print_bucket_stats(self.cluster)
            self.sleep(
                sync_wait_sec,
                f"Waiting for Fusion to sync UPDATE data to NFS "
                f"(iteration {iteration}/{num_upsert_iterations})",
            )
            _compare_du(f"after_update_iter_{iteration}", failures)
            remaining -= 1
            mutate += 1
            self.sleep(30, "Wait after update iteration")

        # ---- Final assertion ----
        self.assertEqual(
            len(failures), 0,
            f"NFS disk usage exceeded the {nfs_overhead_ratio}x ratio at "
            f"{len(failures)} checkpoint(s):\n" + "\n".join(failures)
        )
        self.log.info("test_nfs_disk_usage_vs_local complete")

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
