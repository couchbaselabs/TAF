from random import choice
import threading
import time
from BucketLib.bucket import Bucket
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_server_rest_util.fusion.fusion_api import FusionRestAPI
from cb_tools.cbstats import Cbstats
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_base import MagmaBaseTest


class FusionSyncManager(MagmaBaseTest, FusionBase):
    def setUp(self):
        super(FusionSyncManager, self).setUp()

        self.log.info("FusionSyncManager setUp Started")

        self.compaction_timeout = self.input.param("compaction_timeout", 300)

    def tearDown(self):
        super(FusionSyncManager, self).tearDown()


    def test_fusion_sync_manager(self):

        num_loops = self.input.param("num_loops", 1)
        bucket_ops_rate = self.input.param("bucket_ops_rate", 50000)
        run_read_workload = self.input.param("run_read_workload", False)

        self.sleep(60, "Wait before starting data load")

        # Start Monitoring Fusion Sync Stats
        self.sync_stats_th = threading.Thread(target=self.get_fusion_sync_stats_continuously)
        self.sync_stats_th.start()

        num_items = self.num_items
        start_offset = 0
        end_offset = num_items

        for i in range(num_loops):

            self.log.info(f"Loading items from {start_offset}:{end_offset}")
            self.perform_workload(start_offset, end_offset, "create", ops_rate=bucket_ops_rate)

            self.sleep(15, "Wait after data load")
            self.bucket_util.print_bucket_stats(self.cluster)

            start_offset = end_offset
            end_offset += num_items

        if run_read_workload:

            start_offset = 0
            end_offset = num_items

            for i in range(num_loops):

                self.log.info(f"Running a read workload from {start_offset}:{end_offset}")
                self.perform_workload(start_offset, end_offset, "read", ops_rate=self.read_ops_rate)

                self.sleep(15, "Wait after read workload")

                start_offset = end_offset
                end_offset += num_items

        self.sleep(120, "Wait before running a Fusion Rebalance")

        # Run a Fusion Rebalance
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_sleep_time=20,
                                              skip_file_linking=True)

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

        self.monitor_sync_stats = False
        self.sync_stats_th.join()


    def test_fusion_sync_manager_small_bucket(self):

        '''
        - set sync interval to 10 minutes
        - set lag_threshold to 2GB
        - load data into small bucket with < 1GB data.

        check the small bucket never gets synced.
        '''

        num_loops = self.input.param("num_loops", 1)
        bucket_ops_rate = self.input.param("bucket_ops_rate", 50000)

        first_sync_interval = time.time()

        bucket = self.cluster.buckets[0]
        num_items = self.num_items
        start_offset = 0
        end_offset = num_items

        self.sleep(60, "Wait before starting data load")

        for i in range(num_loops):

            # Check stats
            self.get_fusion_sync_stats()

            data_load_start_time = time.time()

            self.log.info(f"Loading items from {start_offset}:{end_offset} for {bucket.name}")
            self.perform_workload(start_offset, end_offset, "create", ops_rate=bucket_ops_rate)

            data_load_end_time = time.time()
            time_taken = data_load_end_time - data_load_start_time
            self.log.info(f"Total time taken for data load = {time_taken} seconds")
            self.bucket_util.print_bucket_stats(self.cluster)

            time_delta = self.fusion_upload_interval - time_taken
            self.sleep(time_delta, "Sleep after data loading")

            # Check stats
            self.get_fusion_sync_stats()

            start_offset = end_offset
            end_offset += num_items

        total_test_time = time.time() - first_sync_interval
        sleep_time = self.fusion_max_upload_interval - total_test_time + 60
        self.sleep(sleep_time, "Wait before checking stats after force sync")

        # Check stats
        self.get_fusion_sync_stats()


    def test_fusion_sync_manager_multi_bucket(self):

        '''
        - set sync interval to 10 minutes
        - set lag_threshold to 2GB
        - load data into large bucket with 10GB data.
        - load data into small bucket with < 1GB data.
        - data loading into both buckets should be done concurrently.

        check the small bucket never gets synced.
        '''

        num_loops = self.input.param("num_loops", 1)
        small_bucket_ratio = self.input.param("small_bucket_ratio", 0.1)
        bucket_ops_rate = self.input.param("bucket_ops_rate", 50000)

        bucket1 = self.cluster.buckets[0]
        bucket2 = self.cluster.buckets[1]
        num_items = self.num_items
        start_offset = 0
        end_offset = num_items

        self.sleep(60, "Wait before starting data load")

        for i in range(num_loops):

            # Check stats
            self.get_fusion_sync_stats()

            data_load_start_time = time.time()

            self.log.info(f"Loading items from {start_offset}:{end_offset} for {bucket1.name}")
            th1 = threading.Thread(target=self.perform_workload, args=[start_offset, end_offset, "create", True, [bucket1], bucket_ops_rate])
            th1.start()
            th1.join()

            self.sleep(10, "Wait before starting load for the second bucket")

            self.log.info(f"Loading items from {int(start_offset*small_bucket_ratio)}:{int(end_offset*small_bucket_ratio)} for {bucket2.name}")
            th2 = threading.Thread(target=self.perform_workload, args=[int(start_offset*small_bucket_ratio), int(end_offset*small_bucket_ratio), "create", True, [bucket2], bucket_ops_rate])
            th2.start()
            th2.join()
            data_load_end_time = time.time()

            time_taken = data_load_end_time - data_load_start_time

            self.log.info(f"Total time taken for data load = {time_taken} seconds")
            self.bucket_util.print_bucket_stats(self.cluster)

            time_delta = self.fusion_upload_interval - time_taken
            self.sleep(time_delta+15, "Sleep after data loading")

            # Check stats
            self.get_fusion_sync_stats()

            start_offset = end_offset
            end_offset += num_items


    def test_fusion_purge_interval_sync(self):

        bucket_ops_rate = self.input.param("bucket_ops_rate", 50000)
        block_syncs = self.input.param("block_syncs", False)
        throttle_syncs = self.input.param("throttle_syncs", False)
        throttle_rate_limit = self.input.param("throttle_rate_limit", "110bit")

        num_items = self.num_items
        purge_interval_in_seconds = self.purge_interval * 86400

        # Start Monitoring Fusion Sync Stats
        self.sync_stats_th = threading.Thread(target=self.get_fusion_sync_stats_continuously)
        self.sync_stats_th.start()

        if block_syncs:
            # Block NFS Traffic
            self.block_nfs_traffic()

        # Load some data into the bucket
        self.log.info(f"Loading {self.num_items} into the bucket")
        self.perform_workload(0, self.num_items, "create", True, ops_rate=bucket_ops_rate)
        self.bucket_util.print_bucket_stats(self.cluster)

        if block_syncs or throttle_syncs:
            self.sleep(0.25 * purge_interval_in_seconds + 90, "Wait before running delete workload")

        if throttle_syncs:
            self.throttle_nfs_traffic(rate_limit=throttle_rate_limit)

        # Delete half of the items
        self.perform_workload(0, self.num_items // 2, "delete", True, ops_rate=bucket_ops_rate)
        self.bucket_util.print_bucket_stats(self.cluster)
        self.sleep(60, "Wait after running delete workload")

        # Fetch tombstone count after deletes
        self.sleep(120, "Wait before fetching tombstone count after deletes")
        ts_count = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
        self.log.info(f"Tombstone count after deletes = {ts_count}")

        self.sleep(purge_interval_in_seconds+120, "Sleeping until purge interval passes")

        # Load more data
        start_offset = num_items
        end_offset = num_items + (num_items // 10)
        self.log.info(f"Loading {start_offset}:{end_offset} into the bucket")
        self.perform_workload(start_offset, end_offset, "create", True, ops_rate=bucket_ops_rate)
        self.sleep(30, "Sleep after loading")
        self.bucket_util.print_bucket_stats(self.cluster)

        # Run a compaction
        self.log.info("Running a compaction after the purge interval has passed")
        self.bucket_util._run_compaction(self.cluster, number_of_times=1, timeout=self.compaction_timeout)

        # Fetch tombstone count after compaction
        self.sleep(120, "Wait before fetching tombstone count after compaction")
        ts_count = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
        self.log.info(f"Tombstone count after compaction = {ts_count}")

        if block_syncs:
            # Restore NFS traffic
            self.restore_nfs_traffic()

            # Wait for a sync to be triggered
            sleep_time = (0.25 * purge_interval_in_seconds) + 120
            self.sleep(sleep_time, "Wait before running a Fusion Rebalance")

        else:
            self.sleep(60, "Wait before running a Fusion Rebalance")

        if throttle_syncs:
            remove_th = threading.Thread(target=self.remove_nfs_throttling_after_sleep)
            remove_th.start()

        # Run a Fusion Rebalance
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_sleep_time=20,
                                              skip_file_linking=True)

        if throttle_syncs:
            remove_th.join()

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

        # Fetch tombstone count after rebalance
        self.log.info("Fetching tombstone count after rebalance")
        ts_count = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
        self.log.info(f"Tombstone count after rebalance = {ts_count}")

        self.monitor_sync_stats = False
        self.sync_stats_th.join()


    def test_fusion_dynamic_update_of_pending_bytes(self):

        bucket_ops_rate = self.input.param("bucket_ops_rate", 10000)

        monitor_th = threading.Thread(target=self.get_fusion_sync_stats_continuously)
        monitor_th.start()

        update_th = threading.Thread(target=self.dynamic_update_max_pending_bytes)
        update_th.start()

        # Load some data into the bucket
        self.log.info(f"Loading {self.num_items} into the bucket")
        self.perform_workload(0, self.num_items, "create", True, ops_rate=bucket_ops_rate)
        self.bucket_util.print_bucket_stats(self.cluster)

        self.monitor_sync_stats = False
        self.update_setting = False
        monitor_th.join()
        update_th.join()


    def test_fusion_crash_during_tombstone_sync(self):

        '''
        upload interval = 300 seconds
        max_pending_bytes = 200GB
        purge interval = 4 hours
        Load large dataset ~ 20GB
        Delete 15GB
        Sync rate Limit ~ 50MB/s
        After completion of data load, wait until max_upload_interval
        During above, keep crashing every 30 seconds
        '''

        bucket_ops_rate = self.input.param("bucket_ops_rate", 50000)
        delete_data_pct = self.input.param("delete_data_pct", 0.75)
        crash_interval = self.input.param("crash_interval", 45)

        max_upload_interval = self.purge_interval * 86400 * 0.25

        data_load_start_time = time.time()

        monitor_th = threading.Thread(target=self.get_fusion_sync_stats_continuously)
        monitor_th.start()

        # Load some data into the bucket
        self.log.info(f"Loading {self.num_items} into the bucket")
        self.perform_workload(0, self.num_items, "create", True, ops_rate=bucket_ops_rate)
        self.sleep(60, "Wait after initial workload")
        self.bucket_util.print_bucket_stats(self.cluster)

        # Delete half of the items
        self.perform_workload(0, self.num_items * delete_data_pct, "delete", True, ops_rate=bucket_ops_rate)
        self.bucket_util.print_bucket_stats(self.cluster)
        self.sleep(60, "Wait after running delete workload")
        self.bucket_util.print_bucket_stats(self.cluster)

        data_load_end_time = time.time()
        data_load_time_taken = data_load_end_time - data_load_start_time
        self.log.info(f"Time taken for data load = {data_load_time_taken} seconds")

        time_delta = max_upload_interval - data_load_time_taken + 900

        crash_th = threading.Thread(target=self.crash_during_sync, args=[int(crash_interval), 43200])
        crash_th.start()

        self.sleep(time_delta, "Wait until max_upload_interval")

        self.stop_crash = True
        crash_th.join()

        # Run a Fusion Rebalance
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_sleep_time=20,
                                              skip_file_linking=True)

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

        self.monitor_sync_stats = False
        monitor_th.join()


    def test_fusion_trigger_sync_for_log_cleaning(self):

        '''
        log_store_frag_threshold = 0.2
        upload_interval = 20 minutes

        initially, sync manager is disabled
        load 25GB data and let it sync in sync1 at T_20
        after sync1, update 25GB data and let it sync in sync2 at T_40
        this creates a frag of 50% (above threshold of 0.2)

        enable sync manager now with:
            max_upload_interval = 86400
            max_pending_bytes = 200GB
            purge interval = 3 days (default)

        load more 25GB into the bucket
        see if a sync is issued for log cleaning
        '''

        bucket_ops_rate = self.input.param("bucket_ops_rate", 50000)
        update_data_pct = self.input.param("update_data_pct", 1)
        new_fusion_max_pending_upload_bytes = self.input.param("new_fusion_max_pending_upload_bytes", 200161927680) # 200GB
        new_lwm_percent = self.input.param("new_lwm_percent", 100)

        num_items = self.num_items

        monitor_th = threading.Thread(target=self.get_fusion_sync_stats_continuously, args=[172800, 120])
        monitor_th.start()

        initial_load_start_time = time.time()

        # Load some data into the bucket
        self.log.info(f"Loading {self.num_items} into the bucket")
        self.perform_workload(0, self.num_items, "create", True, ops_rate=bucket_ops_rate)
        self.sleep(60, "Wait after initial workload")
        self.bucket_util.print_bucket_stats(self.cluster)

        initial_load_end_time = time.time()
        initial_load_time_taken = initial_load_end_time - initial_load_start_time
        self.log.info(f"Initial load time taken = {initial_load_time_taken} seconds")

        time_delta = self.fusion_upload_interval - initial_load_time_taken + 300
        self.sleep(time_delta, "Wait for sync1 containing creates to take place")

        update_load_start_time = time.time()

        # Update items
        self.perform_workload(0, self.num_items * update_data_pct, "update", True, ops_rate=bucket_ops_rate, mutate=1)
        self.bucket_util.print_bucket_stats(self.cluster)
        self.sleep(60, "Wait after running update workload")
        self.bucket_util.print_bucket_stats(self.cluster)

        update_load_end_time = time.time()
        update_load_time_taken = update_load_end_time - update_load_start_time
        self.log.info(f"Time taken for update workload = {update_load_time_taken} seconds")

        frag_th = threading.Thread(target=self.monitor_log_store_stats, args=[self.cluster.buckets[0], 30])
        frag_th.start()

        time_delta = (3 * self.fusion_upload_interval)
        self.sleep(time_delta, "Wait for sync2 containing updates to take place")

        # Enable Sync Manager now
        status, content = ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
                            fusion_max_pending_upload_bytes=new_fusion_max_pending_upload_bytes,
                            fusion_max_pending_upload_bytes_lwm_percentage=new_lwm_percent)
        self.log.info(f"Enabling Sync Manager, Status: {status}, Content: {content}")

        self.sleep(120, "Wait after enabling Sync Manager")

        # Load more data
        self.log.info(f"Loading {num_items}:{num_items*1.5} into the bucket")
        self.perform_workload(num_items, num_items*1.5, "create", True, ops_rate=bucket_ops_rate)
        self.sleep(60, "Wait after workload")
        self.bucket_util.print_bucket_stats(self.cluster)

        self.sleep(2 * self.fusion_upload_interval, "Wait to see if a sync is issued for log cleaning")

        self.monitor_stats = False
        frag_th.join()

        self.monitor_sync_stats = False
        monitor_th.join()


    def test_fusion_small_purge_interval(self):

        '''
        purge_interval = 2 minutes
        '''

        bucket_ops_rate = self.input.param("bucket_ops_rate", 50000)

        purge_interval_in_seconds = self.input.param("purge_interval_in_seconds", 120)
        purge_interval_in_days = purge_interval_in_seconds / 86400.0
        self.set_metadata_purge_interval(
            value=purge_interval_in_days, buckets=self.cluster.buckets)
        self.sleep(180, "sleeping after setting metadata purge interval using diag/eval")

        num_items = self.num_items
        max_upload_interval = int(purge_interval_in_seconds * 0.25)

        # Start Monitoring Fusion Sync Stats
        self.sync_stats_th = threading.Thread(target=self.get_fusion_sync_stats_continuously, args=[172800, max_upload_interval])
        self.sync_stats_th.start()

        # Load some data into the bucket
        self.log.info(f"Loading {self.num_items} into the bucket")
        self.perform_workload(0, self.num_items, "create", True, ops_rate=bucket_ops_rate)
        self.sleep(60, "Wait after initial workload")
        self.bucket_util.print_bucket_stats(self.cluster)

        # Run a delete workload
        self.log.info(f"Deleting {int(num_items*0.75)} from the bucket")
        self.perform_workload(0, int(num_items*0.75), "delete", True, ops_rate=bucket_ops_rate)
        self.sleep(60, "Wait after delete workload")
        self.bucket_util.print_bucket_stats(self.cluster)

        self.sleep(purge_interval_in_seconds+30, "Sleeping until purge interval passes")

        # Run a compaction
        self.log.info("Running a compaction after the purge interval has passed")
        self.bucket_util._run_compaction(self.cluster, number_of_times=1, timeout=3600, async_run=False)

        # Run a Fusion Rebalance
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_sleep_time=20,
                                              skip_file_linking=True)

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

        self.monitor_sync_stats = False
        self.sync_stats_th.join()


    def test_fusion_sync_manager_with_expiry(self):

        '''
        maxTTL = 5 minutes
        purge_interval = 5 minutes
        exp_pager_stime = 5 minutes
        upload_interval = 2 minutes
        max_pending_bytes = 100GB
        '''

        bucket_ops_rate = self.input.param("bucket_ops_rate", 50000)
        exp_pager_stime = self.input.param("exp_pager_stime", 5)

        purge_interval_in_seconds = self.input.param("purge_interval_in_seconds", 300)
        purge_interval_in_days = purge_interval_in_seconds / 86400.0
        self.set_metadata_purge_interval(
            value=purge_interval_in_days, buckets=self.cluster.buckets)
        self.sleep(180, "sleeping after setting metadata purge interval using diag/eval")

        num_items = self.num_items
        max_upload_interval = int(purge_interval_in_seconds * 0.25)

        # Start Monitoring Fusion Sync Stats
        self.sync_stats_th = threading.Thread(target=self.get_fusion_sync_stats_continuously, args=[172800, max_upload_interval])
        self.sync_stats_th.start()

        # Load some data into the bucket
        self.log.info(f"Loading {self.num_items} into the bucket")
        self.perform_workload(0, self.num_items, "create", True, ops_rate=bucket_ops_rate)
        self.sleep(60, "Wait after initial workload")
        self.bucket_util.print_bucket_stats(self.cluster)

        self.bucket_util._expiry_pager(self.cluster, exp_pager_stime)
        self.sleep(self.maxTTL*2, "Wait for docs to expire")
        self.sleep(exp_pager_stime*3, "Wait until exp_pager_stime for kv_purger to kickoff")

        self.sleep(purge_interval_in_seconds, "Sleeping until purge interval passes")

        # Run a compaction
        self.log.info("Running a compaction after the purge interval has passed")
        self.bucket_util._run_compaction(self.cluster, number_of_times=1, timeout=3600, async_run=False)

        # Run a Fusion Rebalance
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_sleep_time=20,
                                              skip_file_linking=True)

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

        self.monitor_sync_stats = False
        self.sync_stats_th.join()


    def test_fusion_sync_manager_target_vbuckets(self):

        '''
        Choose a node and load data only to that node
        Sync Manager will trigger syncs only on that particular node
        '''

        num_loops = self.input.param("num_loops", 1)
        bucket_ops_rate = self.input.param("bucket_ops_rate", 50000)

        t_node = choice(self.cluster.nodes_in_cluster)
        self.log.info(f"Target node: {t_node.ip}")
        cb_stats = Cbstats(t_node)
        bucket = self.cluster.buckets[0]

        target_vbs = cb_stats.vbucket_list(bucket.name, Bucket.vBucket.ACTIVE)
        cb_stats.disconnect()
        target_vbs = list(set(target_vbs))
        self.log.info(f"Targeting vbs: {target_vbs}")

        self.sleep(60, "Wait before starting data load")

        # Start Monitoring Fusion Sync Stats
        self.sync_stats_th = threading.Thread(target=self.get_fusion_sync_stats_continuously)
        self.sync_stats_th.start()

        num_items = self.num_items
        start_offset = 0
        end_offset = num_items

        for i in range(num_loops):

            self.log.info(f"Loading items from {start_offset}:{end_offset}")
            self.perform_workload(start_offset, end_offset, "create", ops_rate=bucket_ops_rate, target_vbs=target_vbs)

            self.sleep(15, "Wait after data load")
            self.bucket_util.print_bucket_stats(self.cluster)

            start_offset = end_offset
            end_offset += num_items

        self.sleep(120, "Wait before running a Fusion Rebalance")

        # Run a Fusion Rebalance
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_sleep_time=20,
                                              skip_file_linking=True)

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

        self.monitor_sync_stats = False
        self.sync_stats_th.join()


    def remove_nfs_throttling_after_sleep(self, sleep_time=60):

        self.sleep(sleep_time, "Wait before removing throttling")
        self.remove_nfs_throttling()


    def dynamic_update_max_pending_bytes(self, interval=180, timeout=43200):

        self.update_setting = True
        end_time = time.time() + timeout

        counter = 1
        self.sleep(interval, "Wait before dynamically updating pending_bytes")

        while self.update_setting and time.time() < end_time:

            if counter % 2 == 1:
                max_pending_bytes = 0
            else:
                max_pending_bytes = self.fusion_max_pending_upload_bytes

            self.log.info(f"Setting fusion_max_pending_bytes = {max_pending_bytes}")
            status, content = ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
                                fusion_max_pending_upload_bytes=max_pending_bytes)
            self.log.info(f"Status = {status}, Content = {content}")

            counter += 1
            self.sleep(interval)
