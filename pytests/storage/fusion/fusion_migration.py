import os
import random
import subprocess
import threading
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_base import MagmaBaseTest


class FusionMigration(MagmaBaseTest, FusionBase):
    def setUp(self):
        super(FusionMigration, self).setUp()

        self.log.info("FusionMigration setUp Started")

        # Override Fusion default settings
        for bucket in self.cluster.buckets:
            self.change_fusion_settings(bucket, upload_interval=self.fusion_upload_interval,
                                        checkpoint_interval=self.fusion_log_checkpoint_interval,
                                        logstore_frag_threshold=self.logstore_frag_threshold)
        # Set Migration Rate Limit
        ClusterRestAPI(self.cluster.master).\
            manage_global_memcached_setting(fusion_migration_rate_limit=self.fusion_migration_rate_limit)

    def tearDown(self):

        super(FusionMigration, self).tearDown()


    def test_fusion_extent_migration(self):

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir)

        self.get_log_file_count_size(dir_path=self.fusion_scripts_dir)

        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        for th in extent_migration_array:
            th.join()


    def test_crud_during_extent_migration(self):

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Set Migration Rate Limit to 0 so that extent migration doesn't take place
        ClusterRestAPI(self.cluster.master).\
            manage_global_memcached_setting(fusion_migration_rate_limit=0)

        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir)

        self.get_log_file_count_size(dir_path=self.fusion_scripts_dir)

        # Perform reads when no extent migration has taken place yet
        self.perform_batch_reads()

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

        # Deleting guest volumes after extent migration
        self.log_store_rebalance_cleanup()

        self.log.info("Performing a read workload post extent migration")
        self.perform_batch_reads()


    def test_monitor_active_guest_volumes(self):

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir)

        self.get_log_file_count_size(dir_path=self.fusion_scripts_dir)

        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        active_guest_volumes_th = threading.Thread(target=self.monitor_active_guest_volumes, args=[nodes_to_monitor, 120])
        active_guest_volumes_th.start()

        for th in extent_migration_array:
            th.join()
        active_guest_volumes_th.join()


    def test_rebalance_in_during_migration(self):

        self.rebalance_in_method = self.input.param("rebalance_in_method", "fusion") # ["fusion", "dcp"]
        self.rebalance_in_nodes_during_migration = self.input.param("rebalance_in_nodes_during_migration", 1)

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Set Migration Rate Limit to 0 so that extent migration doesn't take place
        ClusterRestAPI(self.cluster.master).\
            manage_global_memcached_setting(fusion_migration_rate_limit=0)

        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir)
        self.sleep(30, "Wait after rebalance")

        self.cluster_util.print_cluster_stats(self.cluster)

        self.get_log_file_count_size(dir_path=self.fusion_scripts_dir)

        self.sleep(10, "Wait before performing rebalance-in during migration")
        nodes_to_monitor2 = list()
        if self.rebalance_in_method == "dcp":
            nodes_to_rebalance_in = self.cluster.servers[len(self.cluster.nodes_in_cluster):
                                        len(self.cluster.nodes_in_cluster)+self.rebalance_in_nodes_during_migration]

            self.log.info(f"Rebalancing-in {nodes_to_rebalance_in} during extent migration")
            result = self.task.rebalance(self.cluster, nodes_to_rebalance_in,
                                         [], services=["kv"]*self.rebalance_in_nodes_during_migration)
            self.assertTrue(result, "Rebalance-in during migration failed")
            self.cluster.nodes_in_cluster.extend(self.cluster.servers[
                len(self.cluster.nodes_in_cluster):len(self.cluster.nodes_in_cluster)+self.rebalance_in_nodes_during_migration])

        elif self.rebalance_in_method == "fusion":
            self.num_nodes_to_rebalance_in = self.rebalance_in_nodes_during_migration
            self.num_nodes_to_rebalance_out = 0
            self.num_nodes_to_swap_rebalance = 0
            self.log.info("Running a Fusion rebalance")
            nodes_to_monitor2 = self.run_rebalance(output_dir=self.fusion_output_dir,
                                                   rebalance_count=2)
            self.sleep(30, "Wait after rebalance-in during migration")
            self.get_log_file_count_size(dir_path=self.fusion_scripts_dir,
                                         migration_count=2,
                                         rebalance_count=2)

        self.cluster_util.print_cluster_stats(self.cluster)

        # Update Migration Rate Limit so that extent migration process starts
        ClusterRestAPI(self.cluster.master).\
                manage_global_memcached_setting(fusion_migration_rate_limit=self.fusion_migration_rate_limit)

        extent_migration_array = list()
        total_nodes_to_monitor = nodes_to_monitor + nodes_to_monitor2
        self.log.info(f"Monitoring extent migration on nodes: {total_nodes_to_monitor}")
        for node in total_nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        for th in extent_migration_array:
            th.join()


    def test_rebalance_out_during_migration(self):

        self.rebalance_out_method = self.input.param("rebalance_out_method", "fusion") # ["fusion", "dcp"]
        self.rebalance_out_nodes_during_migration = self.input.param("rebalance_out_nodes_during_migration", 1)

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Set Migration Rate Limit to 0 so that extent migration doesn't take place
        ClusterRestAPI(self.cluster.master).\
            manage_global_memcached_setting(fusion_migration_rate_limit=0)

        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir)
        self.sleep(30, "Wait after rebalance")

        self.cluster_util.print_cluster_stats(self.cluster)

        self.sleep(10, "Wait before performing rebalance-out during migration")
        if self.rebalance_out_method == "dcp":
            nodes_to_rebalance_out = self.cluster.nodes_in_cluster[-self.rebalance_out_nodes_during_migration:]

            self.log.info(f"Rebalancing-out {nodes_to_rebalance_out} during extent migration")
            result = self.task.rebalance(self.cluster, [],
                                         nodes_to_rebalance_out, services=["kv"])
            self.assertFalse(result, "Rebalance-in during migration failed")

        elif self.rebalance_out_method == "fusion":
            self.num_nodes_to_rebalance_in = 0
            self.num_nodes_to_rebalance_out = self.rebalance_out_nodes_during_migration
            self.num_nodes_to_swap_rebalance = 0
            self.log.info("Running a Fusion rebalance")
            nodes_to_monitor2 = self.run_rebalance(output_dir=self.fusion_output_dir,
                                                   rebalance_count=2)
            self.sleep(30, "Wait after rebalance-out during migration")

        self.cluster_util.print_cluster_stats(self.cluster)

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

    def test_swap_rebalance_during_extent_migration(self):

        self.swap_rebalance_method = self.input.param("swap_rebalance_method", "fusion") # ["fusion", "dcp"]
        self.swap_rebalance_nodes_during_migration = self.input.param("swap_rebalance_nodes_during_migration", 1)

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Set Migration Rate Limit to 0 so that extent migration doesn't take place
        ClusterRestAPI(self.cluster.master).\
            manage_global_memcached_setting(fusion_migration_rate_limit=0)

        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir)
        self.sleep(30, "Wait after rebalance")

        self.cluster_util.print_cluster_stats(self.cluster)

        self.sleep(10, "Wait before performing swap rebalance during migration")
        if self.swap_rebalance_method == "dcp":
            nodes_to_rebalance_out = self.cluster.nodes_in_cluster[-self.swap_rebalance_nodes_during_migration:]
            nodes_to_rebalance_in = self.cluster.servers[len(self.cluster.nodes_in_cluster):\
                                    len(self.cluster.nodes_in_cluster)+self.swap_rebalance_nodes_during_migration]

            self.log.info(f"Swap Rebalancing {nodes_to_rebalance_out} during extent migration")
            result = self.task.rebalance(self.cluster, nodes_to_rebalance_in,
                                         nodes_to_rebalance_out,
                                         services=["kv"]*self.swap_rebalance_nodes_during_migration)
            self.assertFalse(result, "Swap rebalance during migration failed")

        elif self.swap_rebalance_method == "fusion":
            self.num_nodes_to_rebalance_in = 0
            self.num_nodes_to_rebalance_out = 0
            self.num_nodes_to_swap_rebalance = self.swap_rebalance_nodes_during_migration
            self.log.info("Running a Fusion rebalance")
            nodes_to_monitor2 = self.run_rebalance(output_dir=self.fusion_output_dir,
                                                   rebalance_count=2)
            self.sleep(30, "Wait after rebalance-out during migration")

        self.cluster_util.print_cluster_stats(self.cluster)

        # Update Migration Rate Limit so that extent migration process starts
        ClusterRestAPI(self.cluster.master).\
            manage_global_memcached_setting(fusion_migration_rate_limit=self.fusion_migration_rate_limit)

        extent_migration_array = list()
        total_nodes_to_monitor = nodes_to_monitor + nodes_to_monitor2
        self.log.info(f"Monitoring extent migration on nodes: {total_nodes_to_monitor}")
        for node in total_nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        for th in extent_migration_array:
            th.join()


    def test_rebalances_post_extent_migration(self):

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir)
        self.sleep(30, "Wait after rebalance")

        self.cluster_util.print_cluster_stats(self.cluster)

        self.get_log_file_count_size(dir_path=self.fusion_scripts_dir)

        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        for th in extent_migration_array:
            th.join()

        self.log_store_rebalance_cleanup()

        rebalance_counter = 2
        rebalance_types = ["rebalance_in", "rebalance_out", "swap_rebalance"]

        #while len(rebalance_types) > 0:
        for rebalance_type in rebalance_types:

            self.num_nodes_to_rebalance_in = 0
            self.num_nodes_to_rebalance_out = 0
            self.num_nodes_to_swap_rebalance = 0

            # # Pick a random rebalance type to execute
            # rebalance_type = random.choice(rebalance_types)

            if rebalance_type == "rebalance_in":
                self.num_nodes_to_rebalance_in = 1
            elif rebalance_type == "rebalance_out":
                self.num_nodes_to_rebalance_out = 1
            elif rebalance_type == "swap_rebalance":
                self.num_nodes_to_swap_rebalance = 1

            self.log.info(f"Running a Fusion rebalance of type: {rebalance_type}")
            nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                                  rebalance_count=rebalance_counter)
            self.sleep(30, "Wait after rebalance")

            self.cluster_util.print_cluster_stats(self.cluster)

            self.get_log_file_count_size(dir_path=self.fusion_scripts_dir,
                                         rebalance_count=rebalance_counter)

            extent_migration_array = list()
            self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
            for node in nodes_to_monitor:
                for bucket in self.cluster.buckets:
                    extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                    extent_th.start()
                    extent_migration_array.append(extent_th)

            for th in extent_migration_array:
                th.join()

            self.log_store_rebalance_cleanup()

            rebalance_types.remove(rebalance_type)

            rebalance_counter += 1