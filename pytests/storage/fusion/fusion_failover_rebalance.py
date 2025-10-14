import threading
from BucketLib.bucket import Bucket
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_server_rest_util.fusion.fusion_api import FusionRestAPI
from rebalance_utils.rebalance_util import RebalanceUtil
from shell_util.remote_connection import RemoteMachineShellConnection
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_base import MagmaBaseTest


class FusionFailoverRebalance(MagmaBaseTest, FusionBase):
    def setUp(self):
        super(FusionFailoverRebalance, self).setUp()

        self.log.info("FusionFailoverRebalance setUp started")


    def tearDown(self):
        super(FusionFailoverRebalance, self).tearDown()


    def test_fusion_magma_couchstore(self):

        self.enable_magma_bucket_count = self.input.param("enable_magma_bucket_count", self.magma_buckets)

        self.log.info("Check Fusion state initially")
        status, content = FusionRestAPI(self.cluster.master).get_fusion_status()
        self.log.info(f"Status = {status}, Content = {content}")

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 20
        self.sleep(sleep_time, "Sleep after data loading")

        # Enable Fusion if it is disabled at start
        if content["state"] == "disabled":
            self.fusion_enabled_buckets = list()
            for bucket in self.cluster.buckets:
                if bucket.storageBackend == Bucket.StorageBackend.magma:
                    self.fusion_enabled_buckets.append(bucket)
                    if len(self.fusion_enabled_buckets) == int(self.enable_magma_bucket_count):
                        break

            fusion_enable_buckets = None
            if int(self.enable_magma_bucket_count) != int(self.magma_buckets):
                fusion_enable_buckets = ",".join(bucket.name for bucket in self.fusion_enabled_buckets)
                self.log.info(f"Enabling Fusion on a subset of buckets: {fusion_enable_buckets}")

            self.configure_fusion()
            self.enable_fusion(buckets=fusion_enable_buckets)

        # Perform a data workload after enabling Fusion
        self.log.info("Performing data load after enabling Fusion")
        self.perform_workload(self.num_items, self.num_items + (self.num_items // 2), "create", True)
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        self.log.info("Start a Fusion rebalance process")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1)
        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.fusion_enabled_buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        for th in extent_migration_array:
            th.join()

        self.cluster_util.print_cluster_stats(self.cluster)

        self.log.info("Validating item count after rebalance")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)


    def test_fusion_failover_recovery(self):

        post_failover_step = self.input.param("post_failover_step", "recovery") #recovery/rebalance_out
        recovery_type = self.input.param("recovery_type", "full") # full/delta
        failover_type = self.input.param("failover_type", "graceful") # graceful/hard

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Get Initial Uploader Map
        self.get_fusion_uploader_info()

        # Perform graceful failover
        candidates = [server for server in self.cluster.nodes_in_cluster if server.ip != self.cluster.master.ip]
        node_to_failover = candidates[0] if candidates else None
        if not node_to_failover:
            self.fail("No node can be failed over")
        self.log.info(f"Node to failover: {node_to_failover.ip}")

        rest = ClusterRestAPI(self.cluster.master)
        otp_node = self.get_otp_node(self.cluster.master, node_to_failover)

        if failover_type == "graceful":

            self.log.info(f"Gracefully Failing over the node {otp_node.id}")
            success, _ = rest.perform_graceful_failover(otp_node.id)
            self.assertTrue(success, "Failover unsuccessful")

            # Monitor failover rebalance
            rebalance_passed = RebalanceUtil(self.cluster).monitor_rebalance()
            self.assertTrue(rebalance_passed, "Graceful failover rebalance failed")

        elif failover_type == "hard":

            self.log.info(f"Hard Failover of node {otp_node.id}")
            success, _ = rest.perform_hard_failover(otp_node.id)
            self.assertTrue(success, "Failover unsuccessful")

        self.sleep(60, "Wait before recovering/rebalancing-out the node")

        # Get Uploader Map after failover
        self.get_fusion_uploader_info()

        if post_failover_step == "recovery":
            # Perform delta/full recovery
            rest.set_failover_recovery_type(otp_node.id, recovery_type)
            self.sleep(15, "Wait after setting failover recovery type")

        elif post_failover_step == "rebalance_out":
            self.num_nodes_to_rebalance_out = 1

        # Perform a Fusion Recovery Rebalance
        self.log.info("Running a Fusion rebalance")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1,
                                              rebalance_sleep_time=60)
        extent_migration_array = list()
        self.log.info(f"Monitoring extent migration on nodes: {nodes_to_monitor}")
        for node in nodes_to_monitor:
            for bucket in self.cluster.buckets:
                extent_th = threading.Thread(target=self.monitor_extent_migration, args=[node, bucket])
                extent_th.start()
                extent_migration_array.append(extent_th)

        for th in extent_migration_array:
            th.join()

        # Get Uploader Map after recovery
        self.get_fusion_uploader_info()


    def test_fusion_rebalance_crash_retry(self):

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        self.log.info("Start a Fusion rebalance process")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1,
                                              wait_for_rebalance_to_complete=False)
        # Kill memcached
        self.sleep(5, "Wait before killing memcached")
        selected_node = nodes_to_monitor[0]
        self.log.info(f"Killing memcached on {selected_node.ip}")
        shell = RemoteMachineShellConnection(selected_node)
        shell.kill_memcached()
        shell.disconnect()

        self.sleep(20, "Wait after crashing")
        RebalanceUtil(self.cluster).print_ui_logs()

        self.sleep(20, "Wait before retrying rebalance")

        self.log.info("Re-trying Fusion rebalance process")
        self.num_nodes_to_rebalance_in = 0
        self.num_nodes_to_rebalance_out = 0
        self.num_nodes_to_swap_rebalance = 0
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=2,
                                              rebalance_sleep_time=30)

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
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)
