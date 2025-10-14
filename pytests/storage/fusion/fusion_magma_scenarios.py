import threading
import time
from BucketLib.bucket import Bucket
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from cb_server_rest_util.buckets.buckets_api import BucketRestApi
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_server_rest_util.fusion.fusion_api import FusionRestAPI
from cb_tools.cbstats import Cbstats
from py_constants.cb_constants.CBServer import CbServer
from rebalance_utils.rebalance_util import RebalanceUtil
from shell_util.remote_connection import RemoteMachineShellConnection
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_base import MagmaBaseTest


class FusionMagmaScenarios(MagmaBaseTest, FusionBase):
    def setUp(self):
        super(FusionMagmaScenarios, self).setUp()

        self.upsert_iterations = self.input.param("upsert_iterations", 3)

        self.log.info("FusionMagmaScenarios setUp started")


    def tearDown(self):
        super(FusionMagmaScenarios, self).tearDown()


    def test_fusion_with_history(self):

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 20
        self.sleep(sleep_time, "Sleep after data loading")

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

        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after update workload")

        self.bucket_seq_count = dict()
        for bucket in self.cluster.buckets:
            seq_count = self.get_seqnumber_count(bucket)
            self.bucket_seq_count[bucket.name] = seq_count

        self.log.info(f"Seq number count before rebalance = {self.bucket_seq_count}")

        self.log.info("Start a Fusion rebalance process")
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

        self.cluster_util.print_cluster_stats(self.cluster)

        self.log.info("Validating item count after rebalance")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)

        self.bucket_seq_count2 = dict()
        for bucket in self.cluster.buckets:
            seq_count = self.get_seqnumber_count(bucket)
            self.bucket_seq_count2[bucket.name] = seq_count

        self.log.info(f"Seq number count dict after rebalance = {self.bucket_seq_count2}")


    def test_fusion_rebalance_after_expiry(self):

        # Fusion sync interval = 60 seconds
        # TTL = 600 seconds

        coll_ttl = self.input.param("coll_ttl", 600)

        bucket = self.cluster.buckets[0]

        status, content = BucketRestApi(self.cluster.master).edit_collection(
                                        bucket.name, "_default", "_default",
                                        collection_spec={"maxTTL": int(coll_ttl)})
        self.log.info(f"Editing coll TTL, Status = {status}, Content = {content}")
        if status:
            bucket.scopes["_default"].collections["_default"].maxTTL = int(coll_ttl)

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Start expiry pager
        self.bucket_util._expiry_pager(self.cluster, val=1)

        expiry_wait_time = 1.5 * int(coll_ttl)
        self.sleep(expiry_wait_time, "Wait until docs expire")

        self.bucket_util.print_bucket_stats(self.cluster)

        tombstone_count = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
        self.log.info(f"Tomstone count after expiration = {tombstone_count}")

        self.sleep(90, "Wait before all tombstones are synced to Fusion")

        # Perform a Fusion Rebalance
        self.log.info("Start a Fusion rebalance process")
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

        self.sleep(60, "Wait after completion of rebalance")
        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        tombstone_count = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
        self.log.info(f"Tomstone count after Fusion Rebalance = {tombstone_count}")


    def test_fusion_rebalance_during_expiry(self):

        # Fusion sync interval = 60 seconds
        # TTL = 600 seconds

        coll_ttl = self.input.param("coll_ttl", 600)

        bucket = self.cluster.buckets[0]

        status, content = BucketRestApi(self.cluster.master).edit_collection(
                                        bucket.name, "_default", "_default",
                                        collection_spec={"maxTTL": int(coll_ttl)})
        self.log.info(f"Editing coll TTL, Status = {status}, Content = {content}")
        if status:
            bucket.scopes["_default"].collections["_default"].maxTTL = int(coll_ttl)

        self.log.info("Starting initial load")
        self.initial_load()
        end_time = time.time()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading to ensure sync to Fusion is complete")
        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        # Start expiry pager
        self.bucket_util._expiry_pager(self.cluster, val=1)

        expiry_wait_time = (1.5 * int(coll_ttl)) - (time.time() - end_time)
        self.sleep(expiry_wait_time, "Wait until all initial docs expire")

        tombstone_count = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
        self.log.info(f"Tomstone count after initial expiration of docs = {tombstone_count}")
        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        # Start another workload
        self.perform_workload(self.num_items, self.num_items * 2, "create", True)
        end_time = time.time()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading to ensure sync to Fusion is complete")

        # Perform a Fusion Rebalance
        self.log.info("Start a Fusion rebalance process")
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

        self.sleep(60, "Wait after completion of rebalance")
        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        tombstone_count = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
        self.log.info(f"Tomstone count after Fusion Rebalance = {tombstone_count}")

        expiry_wait_time = (1.5 * int(coll_ttl)) - (time.time() - end_time)
        if expiry_wait_time > 0:
            self.sleep(expiry_wait_time, "Wait until docs expire")

        tombstone_count = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
        self.log.info(f"Tomstone count after complete expiration = {tombstone_count}")


    def test_fusion_rebalance_before_expiry(self):

        coll_ttl = self.input.param("coll_ttl", 600)

        bucket = self.cluster.buckets[0]

        status, content = BucketRestApi(self.cluster.master).edit_collection(
                                        bucket.name, "_default", "_default",
                                        collection_spec={"maxTTL": int(coll_ttl)})
        self.log.info(f"Editing coll TTL, Status = {status}, Content = {content}")
        if status:
            bucket.scopes["_default"].collections["_default"].maxTTL = int(coll_ttl)

        self.log.info("Starting initial load")
        self.initial_load()
        end_time = time.time()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading to ensure sync to Fusion is complete")
        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        # Start expiry pager
        self.bucket_util._expiry_pager(self.cluster, val=1)

        # Perform a Fusion Rebalance
        self.log.info("Start a Fusion rebalance process")
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

        self.sleep(60, "Wait after completion of rebalance")
        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        # Start expiry pager
        self.bucket_util._expiry_pager(self.cluster, val=1)

        expiry_wait_time = (1.5 * int(coll_ttl)) - (time.time() - end_time)
        if expiry_wait_time > 0:
            self.sleep(expiry_wait_time, "Wait until docs expire")

        tombstone_count = self.get_tombstone_count_key(self.cluster.nodes_in_cluster)
        self.log.info(f"Tomstone count after Fusion Rebalance = {tombstone_count}")

        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)


    def test_fusion_collection_drop_recreate(self):

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading to ensure sync to Fusion is complete")
        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        self.sleep(90, "Wait before dropping collection")
        coll_to_drop = None
        for coll in list(self.cluster.buckets[0].scopes[CbServer.default_scope].collections.keys()):
            if coll != CbServer.default_collection:
                coll_to_drop = coll
                break
        for bucket in self.cluster.buckets:
            self.log.info(f"Dropping collection {CbServer.default_scope}:{coll_to_drop} in bucket: {bucket.name}")
            self.bucket_util.drop_collection(self.cluster.master,
                                             bucket,
                                             CbServer.default_scope,
                                             coll_to_drop)
        self.sleep(sleep_time, "Sleep after collection drop to ensure sync to Fusion is complete")
        self.bucket_util.print_bucket_stats(self.cluster)

        # Re-create collection
        for bucket in self.cluster.buckets:
            self.log.info(f"Creating scope::collection {CbServer.default_scope}:{coll_to_drop}")
            self.bucket_util.create_collection(
                self.cluster.master, bucket,
                CbServer.default_scope, {"name": coll_to_drop})

        self.sleep(sleep_time, "Wait after re-creating the collection")

        self.log.info("Starting data load after re-creating the collection")
        self.perform_workload(0, self.num_items * 1.5, "create", True, ops_rate=30000)
        self.sleep(sleep_time, "Sleep after data loading to ensure sync to Fusion is complete")
        self.bucket_util.print_bucket_stats(self.cluster)

        # Perform a Fusion Rebalance
        self.log.info("Start a Fusion rebalance process")
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

        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        self.log.info("Validating item count after rebalance")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)


    def test_fusion_rebalance_before_collection_drop_sync(self):

        recreate_coll = self.input.param("recreate_coll", False)

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading to ensure sync to Fusion is complete")
        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        # Pause sync by setting sync rate limit to 0
        status, content = ClusterRestAPI(self.cluster.master).\
                            manage_global_memcached_setting(fusion_sync_rate_limit=0)
        self.log.info(f"Setting sync rate limit to 0, Status = {status}, Content = {content}")

        self.sleep(90, "Wait before dropping collection")
        coll_to_drop = None
        for coll in list(self.cluster.buckets[0].scopes[CbServer.default_scope].collections.keys()):
            if coll != CbServer.default_collection:
                coll_to_drop = coll
                break
        for bucket in self.cluster.buckets:
            self.log.info(f"Dropping collection {CbServer.default_scope}:{coll_to_drop} in bucket: {bucket.name}")
            self.bucket_util.drop_collection(self.cluster.master,
                                             bucket,
                                             CbServer.default_scope,
                                             coll_to_drop)
        self.bucket_util.print_bucket_stats(self.cluster)
        self.sleep(30, "Wait after dropping collections")

        if recreate_coll:
            self.sleep(60, "Wait before re-creating collections")
            for bucket in self.cluster.buckets:
                self.log.info(f"Creating scope::collection {CbServer.default_scope}:{coll_to_drop}")
                self.bucket_util.create_collection(
                    self.cluster.master, bucket,
                    CbServer.default_scope, {"name": coll_to_drop})
            self.sleep(30, "Wait after re-creating collections")
            self.log.info("Starting data load after re-creating the collection")
            self.perform_workload(0, self.num_items * 0.75, "create", True, ops_rate=30000)
            self.sleep(60, "Sleep after data loading")
            self.bucket_util.print_bucket_stats(self.cluster)

        # Perform a Fusion Rebalance
        self.sleep(60, "Wait before performing a Fusion Rebalance")
        self.log.info("Start a Fusion rebalance process")
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

        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        # Resume sync by updating sync rate limit
        status, content = ClusterRestAPI(self.cluster.master).\
                            manage_global_memcached_setting(fusion_sync_rate_limit=self.fusion_sync_rate_limit)
        self.log.info(f"Updating sync rate limit, Status = {status}, Content = {content}")


    def test_fusion_bucket_delete_recreate(self):

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading to ensure sync to Fusion is complete")
        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        # Delete the bucket/s
        for bucket in self.cluster.buckets:
            self.log.info(f"Deleting bucket: {bucket.name}")
            self.bucket_util.delete_bucket(self.cluster, bucket)

        self.sleep(120, "Wait after bucket/s deletion")

        # Re-create the bucket/s
        self.log.info("Re-creating buckets with the same name")
        buckets_created = self.bucket_util.create_multiple_buckets(
            self.cluster, self.num_replicas,
            bucket_count=self.standard_buckets,
            storage={"couchstore": self.standard_buckets - self.magma_buckets,
                     "magma": self.magma_buckets},
            eviction_policy=self.bucket_eviction_policy,
            bucket_name=self.bucket_name,
            ram_quota=self.bucket_ram_quota)
        self.assertTrue(buckets_created, "Unable to re-create multiple buckets")
        self.bucket_util.print_bucket_stats(self.cluster)

        self.sleep(60, "Wait after re-creating buckets")

        self.log.info("Creating SDK clients for newly created buckets")
        for bucket in self.cluster.buckets:
            self.log.info(f"Creating clients for bucket: {bucket.name}")
            SiriusCouchbaseLoader.create_clients_in_pool(
                self.cluster.master,
                self.cluster.master.rest_username,
                self.cluster.master.rest_password,
                bucket.name,
                req_clients=2)

        self.log.info("Performing data load on the newly created buckets")
        self.perform_workload(0, self.num_items * 0.75, "create", True, ops_rate=30000)
        self.sleep(sleep_time, "Sleep after data loading")
        self.bucket_util.print_bucket_stats(self.cluster)

        # Perform a Fusion Rebalance
        self.log.info("Start a Fusion rebalance process")
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

        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)


    def test_fusion_with_bucket_flush(self):

        stop_sync = self.input.param("stop_sync", False)

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading to ensure sync to Fusion is complete")
        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        if stop_sync:
            # Pause sync by setting sync rate limit to 0
            status, content = ClusterRestAPI(self.cluster.master).\
                                manage_global_memcached_setting(fusion_sync_rate_limit=0)
            self.log.info(f"Setting sync rate limit to 0, Status = {status}, Content = {content}")

        # Flush all buckets
        for bucket in self.cluster.buckets:
            self.log.info(f"Flushing bucket: {bucket.name}")
            self.bucket_util.flush_bucket(self.cluster, bucket)
        self.sleep(30, "Wait after flushing buckets")
        self.bucket_util.print_bucket_stats(self.cluster)

        if not stop_sync:
            self.sleep(sleep_time, "Sleep after flushing buckets")

        # Perform a Fusion Rebalance
        self.log.info("Start a Fusion rebalance process")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1,
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
        self.bucket_util.print_bucket_stats(self.cluster)

        self.sleep(30, "Wait after Fusion Rebalance")
        self.log_store_rebalance_cleanup()

        # Perform data load on the bucket/s
        self.log.info("Performing data load after flushing")
        self.perform_workload(0, self.num_items * 0.75, "create", True, ops_rate=30000)
        if not stop_sync:
            self.sleep(sleep_time, "Sleep after data loading")
        else:
            self.sleep(60, "Sleep after data loading")
        self.bucket_util.print_bucket_stats(self.cluster)

        # Perform another Fusion Rebalance
        self.log.info("Start a Fusion rebalance process")
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
        self.bucket_util.print_bucket_stats(self.cluster)

        if stop_sync:
            # Resume sync by updating sync rate limit
            status, content = ClusterRestAPI(self.cluster.master).\
                                manage_global_memcached_setting(fusion_sync_rate_limit=self.fusion_sync_rate_limit)
            self.log.info(f"Updating sync rate limit, Status = {status}, Content = {content}")


    def test_fusion_with_bucket_compaction(self):

        self.log.info("Starting initial load")
        self.initial_load()
        self.sleep(30, "Sleep after data loading to ensure sync to Fusion is complete")
        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        # Update all docs
        self.log.info("Performing update workload")
        self.perform_workload(0, self.num_items, "update", True, ops_rate=30000)
        self.sleep(30, "Sleep after updating docs")

        # Trigger bucket compaction
        compaction_tasks = list()
        for bucket in self.cluster.buckets:
            self.log.info(f"Compacting bucket: {bucket.name}")
            compaction_tasks.append(self.task.async_compact_bucket(
                                    self.cluster.master, bucket))
        for task in compaction_tasks:
            self.task_manager.get_task_result(task)

        # Run magma_dump to get seq number count
        seq_number_dict = dict()
        for bucket in self.cluster.buckets:
            count = self.get_seqnumber_count(bucket)
            seq_number_dict[bucket.name] = count
        self.log.info(f"Seq number count before Rebalance = {seq_number_dict}")

        sleep_time = 120 + (2 * self.fusion_upload_interval) + 30
        self.sleep(sleep_time, "Sleep to ensure sync to Fusion is complete")

        # Perform a Fusion Rebalance
        self.log.info("Start a Fusion rebalance process")
        nodes_to_monitor = self.run_rebalance(output_dir=self.fusion_output_dir,
                                              rebalance_count=1,
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
        self.bucket_util.print_bucket_stats(self.cluster)

        # Run magma_dump to get seq number count
        seq_number_dict2 = dict()
        for bucket in self.cluster.buckets:
            count = self.get_seqnumber_count(bucket)
            seq_number_dict2[bucket.name] = count
        self.log.info(f"Seq number count before Rebalance = {seq_number_dict2}")
