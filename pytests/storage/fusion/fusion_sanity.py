from copy import deepcopy
import json
import os
import random
import subprocess
import threading
import time
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_server_rest_util.fusion.fusion_api import FusionRestAPI
from cb_server_rest_util.server_groups.server_groups_api import ServerGroupsAPI
from cb_tools.cbstats import Cbstats
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_base import MagmaBaseTest
from shell_util.remote_connection import RemoteMachineShellConnection


class FusionSanity(MagmaBaseTest, FusionBase):
    def setUp(self):
        super(FusionSanity, self).setUp()

        self.log.info("FusionSanity setUp")

        self.crash_during_test = self.input.param("crash_during_test", False)
        self.crash_interval = self.input.param("crash_interval", 120)
        self.monitor_log_store = self.input.param("monitor_log_store", False)

        self.rebalance_master = self.input.param("rebalance_master", False)
        self.validate_sync_manager = self.input.param("validate_sync_manager", False)

        # Monitoring threads
        self.monitor = True
        self.crash_loop = True
        self.monitor_sync_stats = True

        self.kvstore_stats = dict()
        for bucket in self.cluster.buckets:
            self.kvstore_stats[bucket.name] = dict()
            for i in range(bucket.numVBuckets):
                self.kvstore_stats[bucket.name][i] = dict()


    def tearDown(self):

        super(FusionSanity, self).tearDown()

        self.log.info("Stopping all monitoring threads")
        self.monitor = False
        self.crash_loop = False
        self.monitor_sync_stats = False
        if self.crash_during_test:
            self.crash_th.join()
        if self.monitor_log_store:
            for th in self.monitor_threads:
                th.join()
        if self.validate_sync_manager:
            self.sync_stats_th.join()


    def test_fusion_data_lifecycle(self):

        aggressive_variant = self.input.param("aggressive_variant", False)
        self.validate_reupload = self.input.param("validate_reupload", False)

        self.log.info("Running Fusion Data Lifecycle Test")

        if self.monitor_log_store:
            self.monitor_threads = self.start_monitor_dir()

        if self.crash_during_test:
            self.crash_th = threading.Thread(target=self.kill_memcached_on_nodes, args=[self.crash_interval])
            self.crash_th.start()

        if self.validate_sync_manager:
            self.sync_stats_th = threading.Thread(target=self.get_fusion_sync_stats_continuously)
            self.sync_stats_th.start()

        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        num_rebalances_to_perform = 2 * (len(self.cluster.servers) - self.nodes_init)
        num_rebalance_count = 1

        while num_rebalances_to_perform > 0:

            if not aggressive_variant:

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

            if self.validate_reupload:
                # Get Uploader info before rebalance
                self.get_fusion_uploader_info()
                self.uploader_map1 = deepcopy(self.fusion_vb_uploader_map)

            self.log.info("Running a Fusion rebalance")
            nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                                  rebalance_count=num_rebalance_count)

            if not aggressive_variant:
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
                self.log_store_rebalance_cleanup(nodes=nodes_to_monitor, rebalance_count=num_rebalance_count)
                self.log.info("Performing a read workload post extent migration")
                self.perform_batch_reads()
                self.capture_fusion_stats()

            if self.validate_reupload:
                # Get Uploader info after rebalance
                self.get_fusion_uploader_info()
                self.uploader_map2 = deepcopy(self.fusion_vb_uploader_map)

                # Perform a data load post rebalance
                self.perform_workload(self.num_items, self.num_items + (self.num_items // 2), ops_rate=20000)
                sleep_time = 120 + (2 * self.fusion_upload_interval) + 60
                self.sleep(sleep_time, "Sleep after data loading")

                self.verify_fusion_reupload(prev_uploader_map=self.uploader_map1, new_uploader_map=self.uploader_map2)

            num_rebalances_to_perform -= 1
            num_rebalance_count += 1

            if not aggressive_variant:
                self.log.info("Validating item count after rebalance")
                self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                            self.cluster.buckets)
                self.bucket_util.verify_stats_all_buckets(self.cluster,
                                                        self.num_items)

            if len(self.cluster.nodes_in_cluster) == len(self.cluster.servers):
                self.num_nodes_to_rebalance_out = 1
                self.num_nodes_to_rebalance_in = 0

        stat_file_path = os.path.join(self.fusion_output_dir, "kvstore_stats.json")
        with open(stat_file_path, "w") as f:
            json.dump(self.kvstore_stats, f, indent=4)


    def test_fusion_rebalance(self):

        self.validate_reupload = self.input.param("validate_reupload", False)
        self.validate_cache_transfer = self.input.param("validate_cache_transfer", False)

        self.log.info("Running Fusion Rebalance Test")

        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 60
        self.sleep(sleep_time, "Sleep after data loading")
        self.bucket_util.print_bucket_stats(self.cluster)

        if self.validate_reupload:
            # Get Uploader info before rebalance
            self.get_fusion_uploader_info()
            self.uploader_map1 = deepcopy(self.fusion_vb_uploader_map)

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
        self.bucket_util.print_bucket_stats(self.cluster)

        self.log.info("Validating item count after rebalance")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster,
                                                  self.num_items)

        if self.validate_reupload:
            # Get Uploader info after rebalance
            self.get_fusion_uploader_info()
            self.uploader_map2 = deepcopy(self.fusion_vb_uploader_map)

            # Perform a data load post rebalance
            self.perform_workload(self.num_items, self.num_items + (self.num_items // 2), ops_rate=20000)
            sleep_time = 120 + (2 * self.fusion_upload_interval) + 60
            self.sleep(sleep_time, "Sleep after data loading")

            self.verify_fusion_reupload(prev_uploader_map=self.uploader_map1, new_uploader_map=self.uploader_map2)

        if self.validate_cache_transfer:
            # Fetch ep_bg_fetched before read workload
            bg_fetched_before = dict()
            for node in self.cluster.nodes_in_cluster:
                bg_fetched_before[node.ip] = dict()
                cbstats_obj = Cbstats(node)
                for bucket in self.cluster.buckets:
                    result = cbstats_obj.all_stats(bucket.name)
                    bg_fetched_before[node.ip][bucket.name] = result['ep_bg_fetched']
            self.log.info(f"Bg fetches before read workload = {bg_fetched_before}")

            # Run a read workload
            self.log.info("Running a read workload after rebalance completion")
            self.perform_workload(0, self.num_items, doc_op="read", ops_rate=self.read_ops_rate)
            self.sleep(60, "Wait after read workload completion")

            bg_fetched_after = dict()
            for node in self.cluster.nodes_in_cluster:
                bg_fetched_after[node.ip] = dict()
                cbstats_obj = Cbstats(node)
                for bucket in self.cluster.buckets:
                    result = cbstats_obj.all_stats(bucket.name)
                    bg_fetched_after[node.ip][bucket.name] = result['ep_bg_fetched']
            self.log.info(f"Bg fetches after read workload = {bg_fetched_after}")


    def test_fusion_rebalance_at_scale(self):

        self.log.info("Running Fusion Rebalance Test")
        num_bucket_batch_size = self.input.param("num_bucket_batch_size", 2)
        item_batch_size = self.input.param("item_batch_size", self.num_items)
        rebalance_data_load = self.input.param("rebalance_data_load", True)
        wait_before_rebalance_load_time = self.input.param("wait_before_rebalance_load_time", 100)
        rebalance_sleep_time = self.input.param("rebalance_sleep_time", 120)
        num_upsert_iterations = self.input.param("num_upsert_iterations", 1)
        upsert_data_pct = self.input.param("upsert_data_pct", 0.75) # 75%
        data_load_ops_rate = self.input.param("data_load_ops_rate", 10000)

        if self.validate_sync_manager:
            self.sync_stats_th = threading.Thread(target=self.get_fusion_sync_stats_continuously, args=[172800, 60, True])
            self.sync_stats_th.start()

        self.sleep(600, "Wait before starting data load")
        total_num_items = self.num_items

        for i in range(0, len(self.cluster.buckets), num_bucket_batch_size):
            buckets_batch = self.cluster.buckets[i:i+num_bucket_batch_size]
            bucket_names = ', '.join(bucket.name for bucket in buckets_batch)
            for item_start in range(0, total_num_items, item_batch_size):
                items_in_batch = min(item_batch_size, total_num_items - item_start)
                self.log.info(f"Loading items {item_start} to {item_start + items_in_batch} on {bucket_names}")
                self.perform_workload(item_start, item_start + items_in_batch, buckets=buckets_batch, ops_rate=data_load_ops_rate)
                self.sleep(10, "Wait after item batch")

        sleep_time = 300 + self.fusion_upload_interval + 60
        self.sleep(sleep_time, "Sleep after initial workload")
        self.bucket_util.print_bucket_stats(self.cluster)

        mutate = 1
        total_upsert_items = int(self.num_items * upsert_data_pct)
        upsert_item_batch_size = self.input.param("upsert_item_batch_size", total_upsert_items)
        for i in range(num_upsert_iterations):
            for j in range(0, len(self.cluster.buckets), num_bucket_batch_size):
                buckets_batch = self.cluster.buckets[j:j+num_bucket_batch_size]
                bucket_names = ', '.join(bucket.name for bucket in buckets_batch)
                for item_start in range(0, total_upsert_items, upsert_item_batch_size):
                    items_in_batch = min(upsert_item_batch_size, total_upsert_items - item_start)
                    self.log.info(f"Upserting items {item_start} to {item_start + items_in_batch} on {bucket_names}")
                    self.perform_workload(item_start, item_start + items_in_batch, buckets=buckets_batch,
                                          ops_rate=data_load_ops_rate, doc_op="update", mutate=mutate)
                    self.sleep(10, "Wait after item batch")

            sleep_time = 300 + self.fusion_upload_interval + 60
            self.sleep(sleep_time, "Sleep after upsert workload")
            self.bucket_util.print_bucket_stats(self.cluster)
            mutate += 1

        # Perform a data workload in parallel when rebalance is taking place
        if rebalance_data_load:
            doc_load_th = threading.Thread(target=self.run_workload_custom,
                                           args=[num_bucket_batch_size, self.num_items,
                                                self.num_items + self.rebalance_num_docs,
                                                self.rebalance_ops_rate,
                                                int(wait_before_rebalance_load_time)])
            doc_load_th.start()

        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1,
                                              rebalance_master=self.rebalance_master,
                                              rebalance_sleep_time=rebalance_sleep_time)

        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        if rebalance_data_load:
            doc_load_th.join()

        for th in extent_migration_array:
            th.join()

        self.cluster_util.print_cluster_stats(self.cluster)

        self.log.info("Validating item count after rebalance")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster,
                                                  self.num_items)


    def test_multiple_random_fusion_rebalances(self):

        self.num_rebalances = self.input.param("num_rebalances", 3)
        aggressive_variant = self.input.param("aggressive_variant", False)

        self.log.info("Running Multiple Random Fusion Rebalance Test")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 60
        self.sleep(sleep_time, "Sleep after data loading")
        self.bucket_util.print_bucket_stats(self.cluster)

        rebalance_count = 1

        while rebalance_count <= self.num_rebalances:

            # Get Uploader info before rebalance
            self.get_fusion_uploader_info()
            self.uploader_map1 = deepcopy(self.fusion_vb_uploader_map)

            self.num_nodes_to_rebalance_in = 0
            self.num_nodes_to_rebalance_out = 0
            self.num_nodes_to_swap_rebalance = 0

            if len(self.cluster.nodes_in_cluster) <= 1:
                reb_operations = ["rebalance_in", "swap_rebalance"]
            else:
                reb_operations = ["rebalance_in", "rebalance_out", "swap_rebalance"]

            reb_op = random.choice(reb_operations)
            self.log.info(f"Performing a rebalance operation of type: {reb_op}")

            if reb_op == "rebalance_in":
                self.num_nodes_to_rebalance_in = random.randint(1, len(self.cluster.servers) - len(self.cluster.nodes_in_cluster))
            elif reb_op == "rebalance_out":
                self.num_nodes_to_rebalance_out = random.randint(1, len(self.cluster.nodes_in_cluster) - 1)
            elif reb_op == "swap_rebalance":
                self.num_nodes_to_swap_rebalance = random.randint(1, min(len(self.cluster.nodes_in_cluster), len(self.cluster.servers) - len(self.cluster.nodes_in_cluster)))

            self.log.info("Running a Fusion rebalance")
            nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                                  rebalance_count=rebalance_count,
                                                  rebalance_master=self.rebalance_master)

            if not aggressive_variant:
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

            # Get Uploader info after rebalance
            self.get_fusion_uploader_info()
            self.uploader_map2 = deepcopy(self.fusion_vb_uploader_map)

            if not aggressive_variant:

                self.log.info("Validating item count after rebalance")
                self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
                self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)

                # Perform a data load post rebalance
                self.perform_workload(self.num_items, self.num_items + (self.num_items // 2), ops_rate=20000)
                sleep_time = 120 + (2 * self.fusion_upload_interval) + 60
                self.sleep(sleep_time, "Sleep after data loading")
                self.bucket_util.print_bucket_stats(self.cluster)

                self.verify_fusion_reupload(prev_uploader_map=self.uploader_map1, new_uploader_map=self.uploader_map2)

            rebalance_count += 1


    def test_fusion_with_server_groups(self):

        self.validate_reupload = self.input.param("validate_reupload", True)

        self.log.info("Running Fusion Rebalance Test With Server Groups")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 60
        self.sleep(sleep_time, "Sleep after data loading")
        self.bucket_util.print_bucket_stats(self.cluster)

        if self.validate_reupload:
            # Get Uploader info before rebalance
            self.get_fusion_uploader_info()
            self.uploader_map1 = deepcopy(self.fusion_vb_uploader_map)

        # Create a new Server Group
        group_name = "ServerGroup2"
        server_group_rest = ServerGroupsAPI(self.cluster.master)
        status, content = server_group_rest.create_server_group(server_group_name=group_name)
        self.log.info(f"Creating a server group, Status: {status}, Content: {content}")

        # Retrieve server group UUID
        status, content = server_group_rest.get_server_groups_info()
        self.log.info(f"Retrieving Server Groups Info: {status}")
        for group in content.get("groups", []):
            if group.get("name") == group_name:
                uri = group.get("uri", "")
                server_group_uuid = uri.rstrip("/").split("/")[-1]
        self.log.info(f"New Server Group UUID: {server_group_uuid}")

        # Add new nodes to new server group
        self.spare_nodes = self.cluster.servers[len(self.cluster.nodes_in_cluster):]
        self.log.info(f"Spare nodes = {self.spare_nodes}")
        for server in self.spare_nodes[:self.num_nodes_to_rebalance_in]:
            status, content = server_group_rest.add_node(hostname=server.ip, port=server.port,
                                                         uuid=server_group_uuid,
                                                         username=server.rest_username,
                                                         password=server.rest_password, services=["kv"])
            self.log.info(f"Adding Server: {server.ip}, Status: {status}, Content: {content}")

        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1,
                                              rebalance_master=self.rebalance_master,
                                              skip_add_nodes=True)

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
            # Get Uploader info after rebalance
            self.get_fusion_uploader_info()
            self.uploader_map2 = deepcopy(self.fusion_vb_uploader_map)

            # Perform a data load post rebalance
            self.perform_workload(self.num_items, self.num_items + (self.num_items // 2), ops_rate=20000)
            sleep_time = 120 + (2 * self.fusion_upload_interval) + 60
            self.sleep(sleep_time, "Sleep after data loading")

            self.verify_fusion_reupload(prev_uploader_map=self.uploader_map1, new_uploader_map=self.uploader_map2)


    def test_fusion_rebalance_scale_up_down_loop(self):

        aggressive_variant = self.input.param("aggressive_variant", True)
        num_rebalances_to_perform = self.input.param("num_rebalances_to_perform", 1000)
        num_bucket_batch_size = self.input.param("num_bucket_batch_size", 2)

        self.log.info("Running Fusion Rebalance Test Scale Up/Down in a loop")

        if self.validate_sync_manager:
            self.sync_stats_th = threading.Thread(target=self.get_fusion_sync_stats_continuously)
            self.sync_stats_th.start()

        for i in range(0, len(self.cluster.buckets), num_bucket_batch_size):
            buckets_batch = self.cluster.buckets[i:i+num_bucket_batch_size]
            bucket_names = ', '.join(bucket.name for bucket in buckets_batch)
            self.log.info(f"Performing data load on {bucket_names}")
            self.perform_workload(0, self.num_items, buckets=buckets_batch, ops_rate=10000)
            self.sleep(10, "Wait after one batch")

        sleep_time = 300 + self.fusion_upload_interval + 60
        self.sleep(sleep_time, "Sleep after data loading")
        self.bucket_util.print_bucket_stats(self.cluster)

        # Perform a data workload in parallel when rebalance is taking place
        self.log.info("Performing Write Workload during rebalance")

        write_load_th = threading.Thread(target=self.run_workload_custom,
                                         args=[num_bucket_batch_size, self.num_items, self.num_items * 2, 1000, 0, "create"])
        write_load_th.start()

        read_load_th = threading.Thread(target=self.run_workload_custom,
                                         args=[num_bucket_batch_size, 0, self.num_items, 2500, 0, "read"])
        read_load_th.start()

        self.num_rebalance_count = 1

        cleanup_thread = threading.Thread(target=self.cleanup_nfs_guest_volumes_periodically)
        cleanup_thread.start()

        while num_rebalances_to_perform > 0:

            if self.num_rebalance_count % 2 == 1:
                self.num_nodes_to_rebalance_in = 3
                self.num_nodes_to_rebalance_out = 0
                self.num_nodes_to_swap_rebalance = 0
            else:
                self.num_nodes_to_rebalance_in = 0
                self.num_nodes_to_rebalance_out = 3
                self.num_nodes_to_swap_rebalance = 0

            self.log.info(f"Running Fusion rebalance count: {self.num_rebalance_count}")
            nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                                  rebalance_count=self.num_rebalance_count,
                                                  skip_file_linking=True,
                                                  rebalance_sleep_time=30)

            if not aggressive_variant:

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
            self.sleep(30, "Wait after rebalance")

            num_rebalances_to_perform -= 1
            self.num_rebalance_count += 1

        write_load_th.join()
        read_load_th.join()

        self.log.info("Validating item count after rebalance")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster,
                                                  self.num_items)

        self.check_guest_vols = False
        cleanup_thread.join()


    def test_fusion_continuous_get_stats(self):

        self.log.info("Running Fusion Test With getStats() call in a loop")

        doc_loading_tasks = self.perform_workload(0, self.num_items, doc_op="create", wait=False)

        self.get_stats_loop = True

        # Perform getStats() call in parallel during Fusion Sync
        stats_th_array = list()
        for bucket in self.cluster.buckets:
            stats_th = threading.Thread(target=self.get_stats_in_a_loop, args=[bucket])
            stats_th_array.append(stats_th)
            stats_th.start()

        for task in doc_loading_tasks:
            self.doc_loading_tm.get_task_result(task)

        self.get_stats_loop = False
        for th in stats_th_array:
            th.join()


    def test_fusion_accelerator_cli_retry_logic(self):

        self.log.info("Running Fusion Rebalance Test With Server Groups")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 60
        self.sleep(sleep_time, "Sleep after data loading")
        self.bucket_util.print_bucket_stats(self.cluster)

        # Dynamically remove and re-introduce read permissions
        perm_th = threading.Thread(target=self.remove_reintroduce_read_permissions)
        perm_th.start()

        # Perform a Fusion Rebalance
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1,
                                              skip_file_linking=True)

        self.change_permissions = False
        perm_th.join()

        # Perform read workload during migration
        doc_loading_tasks = self.perform_workload(0, self.num_items, doc_op="read", wait=False)

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

        self.cluster_util.print_cluster_stats(self.cluster)


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


    def verify_fusion_reupload(self, prev_uploader_map, new_uploader_map):

        self.log.info("Verifying Fusion Re-Upload from scratch")

        shell = RemoteMachineShellConnection(self.nfs_server)

        reupload_count_dict = dict()

        for bucket in self.cluster.buckets:
            reupload_count_dict[bucket.name] = list()
            for kvstore_num in range(128):
                prev_term = prev_uploader_map[bucket.name][kvstore_num]["term"]
                new_term = new_uploader_map[bucket.name][kvstore_num]["term"]
                if prev_term != new_term:
                    kvstore_path = f"{self.nfs_server_path}/kv/{bucket.uuid}/kvstore-{kvstore_num}/"
                    find_cmd = f"find {kvstore_path} -name 'log-{prev_term}.*'"
                    o, e = shell.execute_command(find_cmd)
                    self.log.info(f"Executing CMD: {find_cmd}, O = {o[:2]}, E = {e[:2]}")
                    if len(o) == 0:
                        self.log.info(f"No log files with term num {prev_term} found in bucket: {bucket.name}, kvstore-{kvstore_num}")
                        reupload_count_dict[bucket.name].append(kvstore_num)

        shell.disconnect()

        self.log.info(f"Re-Upload Count dict = {reupload_count_dict}")

        return reupload_count_dict


    def run_workload_custom(self, bucket_batch_size, start, end, ops_rate=10000,
                            sleep_before_start=0, doc_op="create"):

        self.sleep(sleep_before_start, "Sleep before running workloads during rebalance")

        for i in range(0, len(self.cluster.buckets), bucket_batch_size):
            buckets_batch = self.cluster.buckets[i:i+bucket_batch_size]
            bucket_names = ', '.join(bucket.name for bucket in buckets_batch)
            self.log.info(f"Performing data load of type: {doc_op} on {bucket_names}")
            self.perform_workload(start, end, buckets=buckets_batch,
                                  ops_rate=ops_rate,
                                  doc_op=doc_op)
            self.sleep(10, "Wait after one batch")


    def get_stats_in_a_loop(self, bucket, timeout=18000):

        end_time = time.time() + timeout

        counter = 1

        while self.get_stats_loop and time.time() < end_time:

            result = self.get_fusion_kvstore_stats(bucket)
            self.log.info(f"getStats loop count: {counter}, Bucket: {bucket.name}, Fusion Stats: {result[self.cluster.master.ip]['rw_0:magma']['FusionFSStats']['LogStoreDataSize']}")

            time.sleep(0.5)
            counter += 1


    def remove_reintroduce_read_permissions(self, timeout=14400, max_count=14400):

        count = 1
        end_time = time.time() + timeout

        ssh = RemoteMachineShellConnection(self.cluster.master)

        self.change_permissions = True

        self.sleep(60, "Wait before changing permissions")

        while time.time() < end_time and self.change_permissions and count <= max_count:

            if count % 2 == 0:
                # Re-introduce permissions
                msg = "Re-introducing read permissions on the log store"
                cmd = f"find /mnt/nfs/share/buckets/kv -type f -name 'log-*' -exec chmod a+r {{}} +"
                # cmd = f"chown -R couchbase:couchbase /mnt/nfs/share/buckets/kv"

            else:
                # Remove permissions
                msg = "Removing read permissions from the log store"
                cmd = f"find /mnt/nfs/share/buckets/kv -type f -name 'log-*' -exec chmod a-r {{}} +"
                # cmd = f"chown -R root:root /mnt/nfs/share/buckets/kv"

            o, e = ssh.execute_command(cmd)
            self.log.info(f"{msg}, O = {o}, E = {e}")

            count += 1
            time.sleep(5)
