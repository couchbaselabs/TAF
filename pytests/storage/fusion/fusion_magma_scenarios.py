import threading
import time
from copy import deepcopy
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


    # ------------------------------------------------------------------ #
    #  Helpers for the flush re-upload scenario                          #
    # ------------------------------------------------------------------ #
    def _delete_fusion_sync_state(self, bucket):
        """Delete the .fusion sync-state directories for every VB of the
        bucket across all magma shards, on all cluster nodes. Couchbase must
        be stopped before calling this so the files are not in use.

        Path layout (per the scenario):
          <data_path>/<bucket_uuid>/magma.<shard>/kvstore-<vb>/rev-*/.fusion
        """
        bucket_uuid = self.get_bucket_uuid(bucket.name)
        fusion_glob = (f"{self.data_path}/{bucket_uuid}/magma.*/kvstore-*/"
                       f"rev-*/.fusion")
        for server in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(server)
            # Log how many .fusion dirs exist before deletion for traceability
            count_cmd = (f"find {self.data_path}/{bucket_uuid} -type d "
                         f"-name .fusion 2>/dev/null | wc -l")
            o, _ = shell.execute_command(count_cmd)
            self.log.info(f".fusion dir count on {server.ip} before delete = {o}")
            rm_cmd = f"rm -rf {fusion_glob}"
            self.log.info(f"Deleting fusion sync state on {server.ip}: {rm_cmd}")
            out, err = shell.execute_command(rm_cmd)
            shell.log_command_output(out, err)
            shell.disconnect()

    def _restart_cluster_with_fusion_state_cleanup(self, bucket):
        """Stop Couchbase on all nodes, delete fusion sync state for the
        bucket, then start Couchbase again and wait for warmup. This forces
        Fusion to re-upload all data from scratch for every VB."""
        shells = {}
        for server in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(server)
            shells[server] = shell
            self.log.info(f"Stopping couchbase-server on {server.ip}")
            shell.stop_couchbase()
        self.sleep(30, "Wait after stopping couchbase-server on all nodes")

        # Delete the sync-state files while the server is down.
        self._delete_fusion_sync_state(bucket)

        for server, shell in shells.items():
            self.log.info(f"Starting couchbase-server on {server.ip}")
            shell.start_couchbase()
            shell.disconnect()

        self.cluster_util.wait_for_ns_servers_or_assert(
            self.cluster.nodes_in_cluster)
        self.assertTrue(
            self.bucket_util.is_warmup_complete(self.cluster.buckets),
            "Buckets did not warm up after restart with fusion state cleanup")

    def _count_log_store_kvstore_dirs(self, bucket):
        """Count the kvstore-* directories present on the (NFS) log store for
        the bucket — i.e. how many VBs have uploaded data.

        Mirrors the manual check:
          ls -l <nfs_path>/kv/<uuid> | grep 'kvstore-' | wc -l
        """
        bucket_uuid = self.get_bucket_uuid(bucket.name)
        kv_path = f"{self.nfs_server_path}/kv/{bucket_uuid}"
        ssh = RemoteMachineShellConnection(self.nfs_server)
        cmd = f"ls -l {kv_path} 2>/dev/null | grep 'kvstore-' | wc -l"
        o, e = ssh.execute_command(cmd)
        ssh.disconnect()
        count = 0
        if o:
            try:
                count = int(o[0].strip())
            except (ValueError, IndexError):
                count = 0
        self.log.info(
            f"Log store kvstore dir count for {bucket.name} = {count} "
            f"(path={kv_path})")
        return count

    def test_fusion_flush_reupload_all_vbs(self):
        """Regression: after a from-scratch re-upload + swap rebalance + bucket
        flush, a fresh data load must upload data for ALL VBs to the log store
        (the observed bug uploaded only 112/128 VBs).

        Flow:
          1. Load data into a fusion magma bucket and wait for sync.
          2. Stop the cluster, delete all .fusion sync-state files, restart ->
             forces Fusion to re-upload every VB from scratch.
          3. Swap-rebalance one node out for a spare node in.
          4. Flush the bucket (item count -> 0, log files removed; term per VB
             is expected to increment).
          5. Load data again and assert every VB uploads to the log store.
        """
        bucket = self.cluster.buckets[0]
        num_vbuckets = int(bucket.numVBuckets)
        sleep_time = 120 + self.fusion_upload_interval + 30
        reupload_wait = self.input.param(
            "reupload_wait", 300 + self.fusion_upload_interval)

        # 1. Initial load + wait for sync to the log store.
        self.log.info("Starting initial load")
        self.initial_load()
        total_items = self.num_items
        self.sleep(sleep_time,
                   "Sleep after data loading to ensure sync to Fusion")
        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        kvstore_count = self._count_log_store_kvstore_dirs(bucket)
        self.assertEqual(
            kvstore_count, num_vbuckets,
            f"After initial load, only {kvstore_count}/{num_vbuckets} VBs "
            f"uploaded to the log store")

        # 2. Stop cluster, delete fusion sync state, restart -> re-upload all
        #    VBs from scratch.
        self.log.info("Deleting fusion sync state and restarting the cluster")
        self._restart_cluster_with_fusion_state_cleanup(bucket)
        self.sleep(reupload_wait,
                   "Wait for Fusion to re-upload all VBs from scratch")
        kvstore_count = self._count_log_store_kvstore_dirs(bucket)
        self.assertEqual(
            kvstore_count, num_vbuckets,
            f"After from-scratch re-upload, only {kvstore_count}/"
            f"{num_vbuckets} VBs uploaded to the log store")

        # 3. Swap rebalance: remove a non-master node, add a spare node.
        remove_node = next(
            (n for n in self.cluster.nodes_in_cluster
             if n.ip != self.cluster.master.ip), None)
        spare_nodes = [n for n in self.cluster.servers
                       if n not in self.cluster.nodes_in_cluster]
        self.assertIsNotNone(
            remove_node, "Could not find a non-master node to rebalance out")
        self.assertTrue(
            spare_nodes, "No spare node available to swap in")
        add_node = spare_nodes[0]

        self.log.info(
            f"Swap rebalance: out={remove_node.ip}, in={add_node.ip}")
        nodes_to_monitor = self.run_rebalance(
            output_dir=self.fusion_output_dir, rebalance_count=1,
            rebalance_sleep_time=60,
            add_nodes=[add_node], remove_nodes=[remove_node])

        # Wait for guest volumes to drain and extent migration to complete.
        self.log.info("Monitoring active guest volumes")
        guest_volume_th = threading.Thread(
            target=self.monitor_active_guest_volumes)
        guest_volume_th.start()
        guest_volume_th.join()

        self.log.info("Monitoring extent migration on involved nodes")
        for server in nodes_to_monitor:
            self.monitor_extent_migration(server, bucket)

        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        # 4. Capture uploader/term map, flush, then re-capture to observe the
        #    term increment.
        self.get_fusion_uploader_info()
        prev_uploader_map = deepcopy(self.fusion_vb_uploader_map)

        self.log.info(f"Flushing bucket: {bucket.name}")
        self.bucket_util.flush_bucket(self.cluster, bucket)
        self.sleep(60, "Wait after flushing the bucket")
        self.bucket_util.print_bucket_stats(self.cluster)

        # Item count must drop to 0.
        items_after_flush = self.bucket_util.get_buckets_item_count(
            self.cluster, bucket.name)
        self.log.info(f"Item count after flush = {items_after_flush}")
        self.assertEqual(
            items_after_flush, 0,
            f"Item count did not become 0 after flush: {items_after_flush}")

        # Log files should be removed from the log store after flush.
        post_flush_kvstore_count = self._count_log_store_kvstore_dirs(bucket)
        self.log.info(
            f"Log store kvstore dir count after flush = "
            f"{post_flush_kvstore_count}")

        # Observe term increment per VB after flush.
        self.get_fusion_uploader_info()
        new_uploader_map = deepcopy(self.fusion_vb_uploader_map)
        incremented = 0
        for vb_no in range(num_vbuckets):
            prev_term = prev_uploader_map[bucket.name][vb_no]["term"]
            new_term = new_uploader_map[bucket.name][vb_no]["term"]
            if new_term > prev_term:
                incremented += 1
        self.log.info(
            f"Term incremented after flush for {incremented}/{num_vbuckets} "
            f"VBs")

        # 5. Load data again after flush and assert ALL VBs upload.
        self.log.info("Starting data load after flush")
        self.perform_workload(0, total_items, "create", True, ops_rate=30000)
        self.sleep(sleep_time,
                   "Sleep after data loading to ensure sync to Fusion")
        self.bucket_util.print_bucket_stats(self.cluster)

        kvstore_count = self._count_log_store_kvstore_dirs(bucket)
        self.assertEqual(
            kvstore_count, num_vbuckets,
            f"After post-flush data load, only {kvstore_count}/{num_vbuckets} "
            f"VBs uploaded to the log store (regression: not all VBs are "
            f"uploading after flush)")
        self.log.info(
            f"All {num_vbuckets} VBs uploaded to the log store after the "
            f"post-flush data load")

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
        self.log.info("Monitoring active guest volumes")
        guest_volume_th = threading.Thread(target=self.monitor_active_guest_volumes)
        guest_volume_th.start()
        guest_volume_th.join()

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
        self.log.info("Monitoring active guest volumes")
        guest_volume_th = threading.Thread(target=self.monitor_active_guest_volumes)
        guest_volume_th.start()
        guest_volume_th.join()

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
        self.log.info("Monitoring active guest volumes")
        guest_volume_th = threading.Thread(target=self.monitor_active_guest_volumes)
        guest_volume_th.start()
        guest_volume_th.join()

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
        self.log.info("Monitoring active guest volumes")
        guest_volume_th = threading.Thread(target=self.monitor_active_guest_volumes)
        guest_volume_th.start()
        guest_volume_th.join()

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
        self.log.info("Monitoring active guest volumes")
        guest_volume_th = threading.Thread(target=self.monitor_active_guest_volumes)
        guest_volume_th.start()
        guest_volume_th.join()

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
        self.log.info("Monitoring active guest volumes")
        guest_volume_th = threading.Thread(target=self.monitor_active_guest_volumes)
        guest_volume_th.start()
        guest_volume_th.join()

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
        tmp_bucket_list = self.cluster.buckets
        for bucket in tmp_bucket_list:
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
        self.log.info("Monitoring active guest volumes")
        guest_volume_th = threading.Thread(target=self.monitor_active_guest_volumes)
        guest_volume_th.start()
        guest_volume_th.join()

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
        self.log.info("Monitoring active guest volumes")
        guest_volume_th = threading.Thread(target=self.monitor_active_guest_volumes)
        guest_volume_th.start()
        guest_volume_th.join()

        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        self.sleep(30, "Wait after Fusion Rebalance")
        self.log_store_rebalance_cleanup(nodes=nodes_to_monitor)

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
        self.log.info("Monitoring active guest volumes")
        guest_volume_th = threading.Thread(target=self.monitor_active_guest_volumes)
        guest_volume_th.start()
        guest_volume_th.join()

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
        self.log.info("Monitoring active guest volumes")
        guest_volume_th = threading.Thread(target=self.monitor_active_guest_volumes)
        guest_volume_th.start()
        guest_volume_th.join()

        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        # Run magma_dump to get seq number count
        seq_number_dict2 = dict()
        for bucket in self.cluster.buckets:
            count = self.get_seqnumber_count(bucket)
            seq_number_dict2[bucket.name] = count
        self.log.info(f"Seq number count before Rebalance = {seq_number_dict2}")
