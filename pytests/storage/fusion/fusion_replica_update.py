import os
import subprocess
import threading
import time
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_tools.cbstats import Cbstats
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_base import MagmaBaseTest


class FusionReplicaUpdate(MagmaBaseTest, FusionBase):
    def setUp(self):
        super(FusionReplicaUpdate, self).setUp()

        self.log.info("FusionReplicaUpdate setUp Started")

        self.num_nodes_to_rebalance_in = self.input.param("num_nodes_to_rebalance_in", 0)
        self.num_nodes_to_rebalance_out = self.input.param("num_nodes_to_rebalance_out", 0)
        self.num_nodes_to_swap_rebalance = self.input.param("num_nodes_to_swap_rebalance", 0)

        split_path = self.local_test_path.split("/")
        self.fusion_output_dir = "/" + os.path.join("/".join(split_path[1:4]), "fusion_output")
        self.log.info(f"Fusion output dir = {self.fusion_output_dir}")
        subprocess.run(f"mkdir -p {self.fusion_output_dir}", shell=True, executable="/bin/bash")

        self.skip_file_linking = self.input.param("skip_file_linking", False)

        # Override Fusion default settings
        for bucket in self.cluster.buckets:
            self.change_fusion_settings(bucket, upload_interval=self.fusion_upload_interval,
                                        checkpoint_interval=self.fusion_log_checkpoint_interval,
                                        logstore_frag_threshold=self.logstore_frag_threshold)
        # Set Migration Rate Limit
        ClusterRestAPI(self.cluster.master).\
            manage_global_memcached_setting(fusion_migration_rate_limit=self.fusion_migration_rate_limit)

    def tearDown(self):
        super(FusionReplicaUpdate, self).tearDown()


    def test_fusion_replica_update(self):

        self.new_replica_count = self.input.param("new_replica_count", 2)
        subsequent_data_load = self.input.param("subsequent_data_load", True)

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        self.bucket_util.print_bucket_stats(self.cluster)

        # Update replicas from N to N + 1
        for bucket in self.cluster.buckets:
            self.log.info(f"Updating replica count to {self.new_replica_count} for {bucket.name}")
            self.bucket_util.update_bucket_property(self.cluster.master, bucket,
                                                    replica_number=self.new_replica_count)

        if subsequent_data_load:
            # Perform a data workload in parallel when rebalance is taking place
            self.reset_doc_params(doc_ops="create")
            self.create_start = self.num_items
            self.create_end = self.create_start + (self.create_start // 2)
            self.num_items += (self.create_start // 2)
            self.log.info("Performing data load during replica update rebalance")
            self.log.info(f"Create start = {self.create_start}, Create End = {self.create_end}")
            doc_loading_tasks, _ = self.java_doc_loader(wait=False,
                                                     skip_default=self.skip_load_to_default_collection,
                                                     ops_rate=10000, doc_ops="create",
                                                     monitor_ops=False)

        # Run a Fusion Rebalance to generate an extra replica copy
        self.log.info(f"Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1,
                                              replica_update=True,
                                              skip_file_linking=self.skip_file_linking)
        self.sleep(10, "Wait after rebalance")

        if subsequent_data_load:
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

        self.bucket_util.print_bucket_stats(self.cluster)

        # Item count validation
        self.validate_active_replica_item_count()


    def validate_active_replica_item_count(self):

        results = {}
        cbstats_dict = dict()
        for server in self.cluster.nodes_in_cluster:
            cbstats_dict[server.ip] = Cbstats(server)

        def validate_bucket_item_count(bucket, timeout=300):

            self.log.info(f"Validating Active/Replica Item Count on bucket: {bucket.name}")

            end_time = time.time() + timeout
            while time.time() < end_time:
                active_item_count = replica_item_count = 0
                for server in self.cluster.nodes_in_cluster:
                    active_item_count += int(cbstats_dict[server.ip].\
                                            all_stats(bucket.name)["curr_items"])
                    replica_item_count += int(cbstats_dict[server.ip].\
                                            all_stats(bucket.name)["vb_replica_curr_items"])

                self.log.debug(f"Bucket: {bucket.name}, Active Item count: {active_item_count}, "
                              f"Replica Item Count: {replica_item_count}")

                active_item_count_validated = (active_item_count == self.num_items)
                replica_item_count_validated = (replica_item_count == self.num_items * self.new_replica_count)

                if active_item_count_validated and replica_item_count_validated:
                    self.log.info(f"Bucket: {bucket.name}, Active and Replica Item count validated")
                    results[bucket.name] = True
                    return
                else:
                    self.log.debug("Item count mismatch, wait before fetching stats again")
                    time.sleep(2)

            self.log_failure(f"Item count validation failed for bucket: {bucket.name}")
            results[bucket.name] = False

        th_array = list()
        for bucket in self.cluster.buckets:
            th = threading.Thread(target=validate_bucket_item_count, args=[bucket])
            th_array.append(th)
            th.start()

        for th in th_array:
            th.join()

        self.log.info(f"Bucket Validation results: {results}")
        if all(results.values()):
            self.log.info("Item Count Validation passed for all buckets")
        else:
            self.log.fail("Validation failed for one or more buckets")
