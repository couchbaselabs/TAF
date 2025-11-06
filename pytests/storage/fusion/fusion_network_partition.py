import json
import os
import subprocess
import threading
import time

from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_tools.cbepctl import Cbepctl
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from rebalance_utils.rebalance_util import RebalanceUtil
from sdk_client3 import SDKClient
from shell_util.remote_connection import RemoteMachineShellConnection
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_base import MagmaBaseTest


class FusionNetworkPartition(MagmaBaseTest, FusionBase):
    def setUp(self):
        super(FusionNetworkPartition, self).setUp()

        self.log.info("FusionNetworkPartition setUp started")

        split_path = self.local_test_path.split("/")
        self.fusion_output_dir = "/" + os.path.join("/".join(split_path[1:4]), "fusion_output")
        subprocess.run(f"mkdir -p {self.fusion_output_dir}", shell=True, executable="/bin/bash")

        for bucket in self.cluster.buckets:
            self.change_fusion_settings(bucket, upload_interval=self.fusion_upload_interval,
                                        checkpoint_interval=self.fusion_log_checkpoint_interval,
                                        logstore_frag_threshold=self.logstore_frag_threshold)
        
        self.log.info("Setting magma_fusion_max_log_size to 10MB on all nodes")
        for server in self.cluster.servers:
            shell = RemoteMachineShellConnection(server)
            cbepctl = Cbepctl(shell)
            for bucket in self.cluster.buckets:
                try:
                    cbepctl.set(bucket.name, "flush_param", "magma_fusion_max_log_size", "10000000")
                    self.log.info(f"Set max_log_size=10MB for bucket {bucket.name} on {server.ip}")
                except Exception as e:
                    self.log.warning(f"Failed to set max_log_size on {server.ip}: {e}")
            shell.disconnect()

        for server in self.cluster.servers:
            self.log.info(f"Enabling diag/eval on non local hosts for server: {server.ip}")
            shell = RemoteMachineShellConnection(server)
            o, e = shell.enable_diag_eval_on_non_local_hosts()
            shell.disconnect()

    def tearDown(self):
        self.log.info("FusionNetworkPartition tearDown started")
        self.remove_all_network_partitions()
        super(FusionNetworkPartition, self).tearDown()

    def test_uploader_chronicle_partition(self):

        self.log.info("Setting fusion_sync_rate_limit to 0 (unlimited)")
        ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
            fusion_sync_rate_limit=0)
        
        self.log.info("Collecting sync-related stats BEFORE initial load")
        self.print_fusion_sync_stats()

        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Wait after initial load to ensure uploads complete")

        self.get_fusion_uploader_info()

        node3 = self.cluster.nodes_in_cluster[2]
        node1 = self.cluster.nodes_in_cluster[0]
        node2 = self.cluster.nodes_in_cluster[1]

        node3_vbuckets = {}
        for bucket in self.cluster.buckets:
            node3_vbuckets[bucket.name] = [
                vb_no for vb_no, vb_info in self.fusion_vb_uploader_map[bucket.name].items()
                if vb_info['node'] == node3.ip
            ]
        self.log.info(f"vBuckets on node3 ({node3.ip}): {sum(len(v) for v in node3_vbuckets.values())} total")

        self.log.info(f"Creating network partition to isolate {node3.ip}")
        self.block_traffic_between_two_nodes(node3, node1)
        self.block_traffic_between_two_nodes(node3, node2)
        self.block_traffic_between_two_nodes(node1, node3)
        self.block_traffic_between_two_nodes(node2, node3)
        self.nodes_affected = [node3, node1, node2]
        self.log.info("Network partition created")

        rest = ClusterRestAPI(self.cluster.master)
        otp_node = self.get_otp_node(self.cluster.master, node3)

        self.log.info(f"Performing hard failover on node {otp_node.id}")
        success, content = rest.perform_hard_failover(otp_node.id)
        if not success:
            self.fail(f"Hard failover failed: {content}")

        self.log.info("Hard failover successful, node marked as failed")

        self.log.info("Checking uploader map after failover (before rebalance)")
        self.get_fusion_uploader_info()
        undefined_count = 0
        for bucket in self.cluster.buckets:
            for vb_no, vb_info in self.fusion_vb_uploader_map[bucket.name].items():
                if vb_info['node'] is None:
                    undefined_count += 1
        self.log.info(f"Uploader map has {undefined_count} undefined vBuckets after failover")
        if undefined_count == 0:
            self.log.warning("Expected some undefined uploaders after failover, but all are assigned")

        self.log.info("Rebalancing cluster to eject failed node and transfer ownership")
        known_nodes = [node.id for node in self.cluster_util.get_nodes(self.cluster.master, inactive_failed=True)]
        self.log.info(f"Known nodes before rebalance: {known_nodes}")
        status, content = rest.rebalance(known_nodes=known_nodes, eject_nodes=[otp_node.id])
        if not status:
            self.fail(f"Failed to start rebalance: {content}")

        rebalance_passed = RebalanceUtil(self.cluster).monitor_rebalance()
        if not rebalance_passed:
            self.fail("Rebalance failed to eject node")

        self.log.info("Rebalance completed - node3 ejected, ownership transferred to node1/node2 with term=2")
        self.cluster.nodes_in_cluster.remove(node3)

        self.log.info("Checking uploader map after rebalance")
        self.get_fusion_uploader_info()
        undefined_count = 0
        for bucket in self.cluster.buckets:
            for vb_no, vb_info in self.fusion_vb_uploader_map[bucket.name].items():
                if vb_info['node'] is None:
                    undefined_count += 1
        if undefined_count > 0:
            self.fail(f"Found {undefined_count} undefined uploaders after rebalance")
        self.log.info("All uploaders assigned after rebalance")

        self.log.info("Setting fusion_sync_rate_limit to 100MB/s")
        rate_limit_100mb = 100 * 1024 * 1024
        
        ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
            fusion_sync_rate_limit=rate_limit_100mb)
        self.log.info(f"Set rate limit on active cluster: node1={node1.ip}, node2={node2.ip}")
        
        shell = RemoteMachineShellConnection(node3)
        rate_limit_cmd = f'curl -X POST http://localhost:8091/pools/default/settings/memcached/global -u Administrator:password -d "fusion_sync_rate_limit={rate_limit_100mb}"'
        o, e = shell.execute_command(rate_limit_cmd)
        shell.disconnect()
        self.log.info(f"Set rate limit on zombie node3: {node3.ip}")

        term2_count = 0
        term_mismatches = []
        for bucket in self.cluster.buckets:
            for vb_no in node3_vbuckets[bucket.name]:
                vb_info = self.fusion_vb_uploader_map[bucket.name][vb_no]
                if vb_info['term'] == 2:
                    term2_count += 1
                else:
                    term_mismatches.append(f"vb_{vb_no} in {bucket.name} has term {vb_info['term']}")

        if term_mismatches:
            self.fail(f"Expected all vBuckets to have term=2 after rebalance, but found: {term_mismatches}")

        self.log.info(f"Verified {term2_count} failed-over vBuckets have term=2")
        
        test_vbucket = node3_vbuckets[self.cluster.buckets[0].name][0]
        self.log.info(f"Loading data to vBucket {test_vbucket} on active cluster and zombie node")

        for bucket in self.cluster.buckets:
            doc_gen = doc_generator(
                "conflict_test", 0, 1000,
                doc_size=1024,
                doc_type=self.doc_type,
                target_vbucket=[test_vbucket],
                vbuckets=bucket.numVBuckets)

            task = self.task.async_load_gen_docs(
                self.cluster, bucket, doc_gen, "create", 0,
                batch_size=100, process_concurrency=1, timeout_secs=120)
            self.task.jython_task_manager.get_task_result(task)
            self.num_items += 1000

            sdk_client = SDKClient(self.cluster, bucket, servers=[node3])
            docs_loaded = 0

            doc_gen.reset()
            while doc_gen.has_next():
                try:
                    key, _ = doc_gen.next()
                    doc = {"value": f"zombie_data_{docs_loaded}", "type": "conflict"}
                    result = sdk_client.insert(key, doc, timeout=5)
                    if result["status"]:
                        docs_loaded += 1
                except Exception as e:
                    self.log.warning(f"Failed to insert key on zombie node: {e}")
                    pass

            sdk_client.close()
            self.log.info(f"Loaded {docs_loaded} docs to vBucket {test_vbucket} on zombie node")

        self.sleep(self.fusion_upload_interval + 60, "Wait for uploads")

        self.log.info("Verifying Chronicle fencing - zombie node cannot delete term-2 files")
        self.validate_zombie_node_cannot_delete_term2_files(node3)
        
        self.log.info(f"Resetting zombie node {node3.ip} to clean state")
        try:
            rest_node3 = ClusterRestAPI(node3)
            status, content = rest_node3.reset_node()
            if not status:
                self.fail(f"Failed to reset node {node3.ip}: {content}")
            self.log.info(f"Successfully reset node {node3.ip} - bucket data cleared")
            self.sleep(10, "Wait for node reset to complete")
        except Exception as e:
            self.log.error(f"Failed to reset node {node3.ip}: {e}")
            self.fail(f"Node reset failed: {e}")
        
        self.log.info("Removing network partition after node reset")
        self.remove_network_split()
        self.sleep(30, "Wait for network to stabilize after removing partition")
        
        if self.num_nodes_to_swap_rebalance > 0:
            self.log.info(f"Removing node3 {node3.ip} from available servers to prevent it being used in swap rebalance")
            if node3 in self.cluster.servers:
                self.cluster.servers.remove(node3)
                self.log.info(f"Removed node3 from cluster.servers. Available servers: {[s.ip for s in self.cluster.servers]}")
            
            self.log.info("Performing swap rebalance (node3 will NOT be added, using other spare nodes)")
            nodes_to_monitor = self.run_rebalance(
                output_dir=self.fusion_output_dir,
                rebalance_count=1,
                wait_for_rebalance_to_complete=True)
            
            self.sleep(30, "Wait after swap rebalance")
            
            extent_migration_threads = []
            for node in nodes_to_monitor:
                for bucket in self.cluster.buckets:
                    th = threading.Thread(
                        target=self.monitor_extent_migration,
                        args=[node, bucket])
                    th.start()
                    extent_migration_threads.append(th)
            
            for th in extent_migration_threads:
                th.join()
        
        self.log.info("Validating final cluster stats")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)

    def print_fusion_sync_stats(self):
        sync_stats_to_print = [
            "fusion_sync_rate_limit",
            "ep_fusion_syncs",
            "ep_fusion_bytes_synced",
            "ep_fusion_sync_failures",
            "ep_fusion_sync_session_completed_bytes",
            "ep_fusion_sync_session_total_bytes"
        ]
        
        self.log.info("=" * 80)
        self.log.info("FUSION SYNC STATS")
        self.log.info("=" * 80)
        
        for bucket in self.cluster.buckets:
            self.log.info(f"Bucket: {bucket.name}")
            for node in self.cluster.nodes_in_cluster:
                self.log.info(f"  Node: {node.ip}")
                cbstats = Cbstats(node)
                try:
                    result = cbstats.all_stats(bucket.name)
                    for stat_key in sync_stats_to_print:
                        if stat_key in result:
                            self.log.info(f"    {stat_key}: {result[stat_key]}")
                        else:
                            self.log.warning(f"    {stat_key}: NOT FOUND")
                except Exception as e:
                    self.log.error(f"    Error getting stats: {e}")
                finally:
                    cbstats.disconnect()
        
        self.log.info("=" * 80)

    def verify_term_files_exist(self, term_num):
        ssh = RemoteMachineShellConnection(self.nfs_server)
        term_files_found = False
        
        for bucket in self.cluster.buckets:
            bucket_uuid = self.get_bucket_uuid(bucket.name)
            for vb_no in range(bucket.numVBuckets):
                kvstore_path = f"{self.nfs_server_path}/kv/{bucket_uuid}/kvstore-{vb_no}"
                ls_cmd = f"ls {kvstore_path}/log-{term_num}.* 2>/dev/null | wc -l"
                o, e = ssh.execute_command(ls_cmd)
                if o and int(o[0].strip()) > 0:
                    term_files_found = True
                    break
            if term_files_found:
                break
        
        ssh.disconnect()
        self.log.info(f"Term-{term_num} files: {'Found' if term_files_found else 'Not found'}")
        return term_files_found

    def check_term_supersession_logs(self, node):
        shell = RemoteMachineShellConnection(node)
        grep_cmd = 'grep -i "term.*superseded" /opt/couchbase/var/lib/couchbase/logs/memcached.log* 2>/dev/null | tail -20'
        o, e = shell.execute_command(grep_cmd)
        shell.disconnect()
        
        if o and len(o) > 0:
            self.log.info(f"Term supersession detected on {node.ip}")
            for line in o[:5]:
                self.log.info(f"  {line.strip()}")
            return True
        return False
    
    def validate_zombie_node_cannot_delete_term2_files(self, zombie_node):
        self.log.info("Monitoring zombie node stats for 5 minutes")
        
        for bucket in self.cluster.buckets:
            cbstats = Cbstats(zombie_node)
            try:
                result = cbstats.all_stats(bucket.name)
                initial_syncs = int(result.get("ep_fusion_syncs", 0))
                initial_remote_deletes = int(result.get("ep_fusion_log_store_remote_deletes", 0))
                initial_pending = int(result.get("ep_fusion_log_store_pending_delete_size", 0))
                
                self.log.info(f"Initial stats for bucket {bucket.name}")
                self.log.info(f"  ep_fusion_syncs: {initial_syncs}")
                self.log.info(f"  ep_fusion_log_store_remote_deletes: {initial_remote_deletes}")
                self.log.info(f"  ep_fusion_log_store_pending_delete_size: {initial_pending}")
                
                if initial_remote_deletes > 0:
                    self.fail(f"Chronicle fencing failed - remote_deletes={initial_remote_deletes}")
                
                previous_syncs = initial_syncs
                for interval in [60, 120, 180, 240, 300]:
                    self.sleep(60, f"Monitoring stats ({interval}s/300s)")
                    
                    result = cbstats.all_stats(bucket.name)
                    current_syncs = int(result.get("ep_fusion_syncs", 0))
                    current_remote_deletes = int(result.get("ep_fusion_log_store_remote_deletes", 0))
                    current_pending = int(result.get("ep_fusion_log_store_pending_delete_size", 0))
                    
                    self.log.info(f"Stats at {interval}s")
                    self.log.info(f"  ep_fusion_syncs: {current_syncs} (+{current_syncs - previous_syncs}, total: +{current_syncs - initial_syncs})")
                    self.log.info(f"  ep_fusion_log_store_remote_deletes: {current_remote_deletes}")
                    self.log.info(f"  ep_fusion_log_store_pending_delete_size: {current_pending}")
                    
                    if current_remote_deletes > 0:
                        self.fail(f"Chronicle fencing failed at {interval}s - zombie deleted {current_remote_deletes} remote files")
                    
                    previous_syncs = current_syncs
                
                syncs_increase = current_syncs - initial_syncs
                self.log.info(f"Validation complete for {bucket.name}: syncs increased by {syncs_increase}, remote_deletes=0")
                
            except Exception as e:
                self.log.error(f"Error monitoring zombie node: {e}")
                self.fail(f"Chronicle fencing validation failed: {e}")
            finally:
                cbstats.disconnect()

    def block_traffic_between_two_nodes(self, node1, node2):
        shell = RemoteMachineShellConnection(node1)
        self.log.info(f"Blocking traffic between {node1.ip} <-> {node2.ip} (on {node1.ip})")
        shell.execute_command(f"iptables -A INPUT -s {node2.ip} -j DROP")
        shell.execute_command(f"iptables -A OUTPUT -d {node2.ip} -j DROP")
        shell.execute_command(f"nft add rule ip filter INPUT ip saddr {node2.ip} counter drop 2>/dev/null || true")
        shell.execute_command(f"nft add rule ip filter OUTPUT ip daddr {node2.ip} counter drop 2>/dev/null || true")
        shell.disconnect()

    def remove_network_split(self):
        for node in self.nodes_affected:
            shell = RemoteMachineShellConnection(node)
            shell.execute_command("/sbin/iptables -F")
            shell.execute_command("nft flush ruleset 2>/dev/null || true")
            shell.disconnect()

    def remove_all_network_partitions(self):
        for node in self.cluster.servers:
            try:
                shell = RemoteMachineShellConnection(node)
                shell.execute_command("/sbin/iptables -F")
                shell.execute_command("nft flush ruleset 2>/dev/null || true")
                shell.disconnect()
            except Exception as e:
                self.log.warning(f"Failed to clear rules on {node.ip}: {e}")
