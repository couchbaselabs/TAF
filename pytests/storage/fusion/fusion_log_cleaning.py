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

        self.num_nodes_to_rebalance_in = self.input.param("num_nodes_to_rebalance_in", 0)
        self.num_nodes_to_rebalance_out = self.input.param("num_nodes_to_rebalance_out", 0)
        self.num_nodes_to_swap_rebalance = self.input.param("num_nodes_to_swap_rebalance", 1)

        self.log.info("FusionLogCleaning setUp Started")

    def tearDown(self):
        super(FusionLogCleaning, self).tearDown()

    def monitor_fusion_du(self, bucket, validate=False, time_interval=15):

        self.log.info("Monitoring Log Store Disk Usage")

        if validate:
            shell = RemoteMachineShellConnection(self.nfs_server)
            bucket_uuid = self.get_bucket_uuid(bucket.name)
            fusion_bucket_path = f"{self.nfs_server_path}/kv/{bucket_uuid}/{bucket_uuid}"
            o, e = shell.execute_command(f"du -sb {fusion_bucket_path}")
            current_du = int(o[0].split("\t")[0])
            self.log.info(f"Log Store Size of {bucket.name} after initial creates = {current_du}")
            self.max_allowed_log_store_du = (1 + self.logstore_frag_threshold + 0.25) * current_du
            self.log.info(f"Log Store DU should be always under {self.max_allowed_log_store_du} bytes")

        bucket_uuid = self.get_bucket_uuid(bucket.name)
        self.log.info(f"Bucket UUID: {bucket_uuid}")

        fusion_bucket_path = f"{self.nfs_server_path}/kv/{bucket_uuid}/{bucket_uuid}"

        du_cmd = f"du -sb {fusion_bucket_path}"

        ssh = RemoteMachineShellConnection(self.nfs_server)

        du_violations = 0
        total_checks = 0
        max_du_observed = 0

        while self.monitor_stats:
            o, e = ssh.execute_command(du_cmd)
            current_du = int(o[0].split("\t")[0])
            self.log.info(f"Current Log Store DU for {bucket.name} = {current_du}")

            if validate and current_du > self.max_allowed_log_store_du:
                du_violations += 1
                max_du_observed = max(max_du_observed, current_du)

            total_checks += 1
            time.sleep(time_interval)

        ssh.disconnect()

        if validate:
            self.log.info(f"Total DU violations: ({du_violations} / {total_checks}) checks")
            self.log.info(f"Max DU Observed = {max_du_observed}")

    def monitor_log_store_stats(self, bucket, time_interval=15):

        self.log.info("Monitoring log store stats")

        cbstats_obj_dict = dict()
        for server in self.cluster.nodes_in_cluster:
            cbstats_obj_dict[server] = Cbstats(server)

        self.monitor_stats = True
        while self.monitor_stats:
            server_stat_dict = dict()
            for server in self.cluster.nodes_in_cluster:
                server_stat_dict[server.ip] = dict()
                try:
                    cbstats_obj = cbstats_obj_dict[server]
                    result = cbstats_obj.all_stats(bucket.name)
                    log_store_size = result["ep_fusion_log_store_size"]
                    garbage_size = result["ep_fusion_log_store_garbage_size"]
                    pending_size = result["ep_fusion_log_store_pending_delete_size"]
                    frag_perc = int(garbage_size) / int(log_store_size)

                    server_stat_dict[server.ip]["log_store_size"] = int(log_store_size)
                    server_stat_dict[server.ip]["garbage_size"] = int(garbage_size)
                    server_stat_dict[server.ip]["frag_perc"] = float(frag_perc)
                    server_stat_dict[server.ip]["pending_size"] = int(pending_size)

                except Exception as e:
                    self.log.info(f"Cbstats failed: {e}")
                    cbstats_obj_dict = dict()
                    for server in self.cluster.nodes_in_cluster:
                        cbstats_obj_dict[server] = Cbstats(server)
                    break

            self.log.info(f"Log Store Stats = {server_stat_dict}")

            time.sleep(time_interval)


    def test_monitor_fusion_disk_usage(self):

        bucket = self.cluster.buckets[0]

        self.log.info("Monitor Fusion Log Store Disk Usage Test Started")

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 20
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
        for th in monitor_du_threads:
            th.join()
        for th in monitor_cbstats_threads:
            th.join()


    def test_log_cleaning_during_rebalance(self):

        bucket = self.cluster.buckets[0]

        self.log.info("Log Cleaning During Rebalance Test Started")

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


    def perform_multiple_updates(self, upsert_iterations=None, wait_time_before_start=30):

        self.sleep(wait_time_before_start, "Wait before starting update workload")

        num_upsert_iterations = upsert_iterations if upsert_iterations is not None else self.upsert_iterations
        while num_upsert_iterations > 0:
            self.doc_ops = "update"
            self.reset_doc_params()
            self.update_start = 0
            self.update_end = self.num_items
            self.log.info(f"Performing update workload iteration: {self.upsert_iterations - num_upsert_iterations + 1}")
            self.log.info(f"Update start = {self.update_start}, Update End = {self.update_end}")
            self.java_doc_loader(wait=True, skip_default=self.skip_load_to_default_collection, ops_rate=self.upload_ops_rate, monitor_ops=False)
            num_upsert_iterations -= 1
            self.sleep(30, "Wait after update workload")