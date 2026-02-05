'''
Magma endurance test: targets single vbucket with 3 doc classes (cold/warm/hot),
runs continuous hot updates, performs checkpoint operations after 3B seqno progression.
'''
import os
import random
import threading
import time
from BucketLib.bucket import Bucket
from cb_constants.CBServer import CbServer
from cb_tools.cbstats import Cbstats
from couchbase_utils.xdcr_utils.xdcr_util import XDCRUtils
from cluster_utils.cluster_ready_functions import CBCluster
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from membase.api.rest_client import RestConnection
from shell_util.remote_connection import RemoteMachineShellConnection
from storage.magma.magma_base import MagmaBaseTest
from error_simulation.cb_error import CouchbaseError

# MB-67762 diagnostic script (added to scripts/ with the MB-66959 test)
_CHECK_VULNERABLE_LOCAL = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "..", "..", "scripts", "check_vulnerable_vbuckets.sh")
_CHECK_VULNERABLE_REMOTE = "/root/check_vulnerable_vbuckets.sh"
# maxSn threshold hard-coded in check_vulnerable_vbuckets.sh
_CHECK_VULNERABLE_THRESHOLD = 2000000000


class MagmaEnduranceTest(MagmaBaseTest):
    """
    Endurance test with cold/warm/hot doc classes, continuous hot updates,
    checkpoint actions (rollback, compaction, rebalance, failover) after 3B seqno progression.
    """

    def setUp(self):
        super(MagmaEnduranceTest, self).setUp()

        status, _ = self.rest.update_auto_failover_settings(enabled="false", timeout=600)
        self.assertTrue(status, "AutoFailover disabling failed")

        self.target_vb = [random.randint(0, self.bucket_num_vb - 1)]
        self.seqno_threshold_per_cycle = self.input.param("seqno_threshold_per_cycle", 3000000000)
        self.ops = self.input.param("ops", 50000)
        self.slow_phase_duration = self.input.param("slow_phase_duration", 150)
        self.num_cycles = self.input.param("num_cycles", 10)
        self.max_threads = self.input.param("max_threads", 16)
        self.seqno_check_interval = self.input.param("seqno_check_interval", 1800)
        self.target_index = self.input.param("target_index", "keyIndex")

        self.cold_docs = self.input.param("cold_docs", 100000)
        self.warm_docs = self.input.param("warm_docs", 100000)
        self.hot_docs = self.input.param("hot_docs", 100000)

        self.use_target_vbucket = self.input.param("use_target_vbucket", True)
        self.loader_batch_size = self.input.param("loader_batch_size", 1000)
        self.loader_process_concurrency = self.input.param("loader_process_concurrency", 8)

        self.cold_prefix = "cold_"
        self.warm_prefix = "warm_"
        self.hot_prefix = "hot_"
        self.rollback_prefix = "roll_"
        self.key_prefix_list = [self.cold_prefix, self.warm_prefix, self.hot_prefix]

        self.target_scope = CbServer.default_scope
        self.target_collection = CbServer.default_collection

        self.stop_mutations = False
        # Wakes the seqno monitor immediately when a cycle is stopped
        self.stop_event = threading.Event()
        self.hot_loader = None
        self.mutation_thread = None
        self.monitor_thread = None
        # Set by the background threads; the main loop fails the test on them
        self.loader_error = None
        self.monitor_error = None
        # Consecutive unreadable maxSn polls before the monitor gives up
        self.max_sn_read_failures = self.input.param("max_sn_read_failures", 5)
        self.current_target_index_sn = 0
        self.cycle_start_sn = 0
        self.current_cycle = 0
        self.vb_node_owners = []
        self.shard_path = None
        self.available_servers = self.cluster.servers[self.nodes_init:]
        self.threshold_reached = threading.Event()

        self.enable_xdcr = self.input.param("enable_xdcr", False)
        self.xdcr_remote_nodes = self.input.param("xdcr_remote_nodes", 1)
        self.xdcr_cluster = None
        self.xdcr_remote_name = "xdcr_remote"
        self.xdcr_repl_id = None
        self.current_xdcr_type = None

        self.gsi_index_name = self.input.param("gsi_index_name", "endurance_gsi_idx")

        self.cluster_util.update_cluster_nodes_service_list(self.cluster)
        self.log.info("KV nodes list = {}".format(self.cluster.kv_nodes))

        self.log.info("Setup: target_vb={}, target_index={}, seqno_threshold={}, ops={}".format(
            self.target_vb, self.target_index, self.seqno_threshold_per_cycle, self.ops))

    def tearDown(self):
        self._stop_cycle_threads()
        super(MagmaEnduranceTest, self).tearDown()

    def _stop_cycle_threads(self):
        """Stop hot updates and the seqno monitor, then wait for both threads."""
        self.stop_mutations = True
        self.stop_event.set()
        if self.hot_loader is not None and not self.hot_loader.completed:
            self.hot_loader.end_task()
        for th in (self.mutation_thread, self.monitor_thread):
            if th is not None:
                th.join()

    def _get_target_vbuckets(self):
        return self.target_vb if self.use_target_vbucket else None

    def PrintStep(self, msg=None):
        print("\n\t" + "#" * 60)
        print("\t#  {}".format(msg))
        print("\t" + "#" * 60 + "\n")

    def find_vb_node_owners(self, bucket):
        self.vb_node_owners = []
        for server in self.cluster.nodes_in_cluster:
            cbstat_obj = Cbstats(server)
            active_vbs = set(cbstat_obj.vbucket_list(bucket.name, vbucket_type="active"))
            replica_vbs = set(cbstat_obj.vbucket_list(bucket.name, vbucket_type="replica"))
            if self.target_vb[0] in active_vbs or self.target_vb[0] in replica_vbs:
                self.vb_node_owners.append(server)
            cbstat_obj.disconnect()
        self.log.info("VB {} owners: {}".format(self.target_vb[0], [s.ip for s in self.vb_node_owners]))
        return self.vb_node_owners

    def find_kvstore_path(self, bucket):
        if not self.vb_node_owners:
            self.find_vb_node_owners(bucket)

        find_cmd = "find {}/{} -name kvstore-{}".format(
            self.data_path, bucket.uuid, self.target_vb[0])
        self.log.info("CMD: {}".format(find_cmd))
        shell = RemoteMachineShellConnection(self.vb_node_owners[0])
        o, e = shell.execute_command(find_cmd)
        shell.disconnect()

        # Keeping the previous shard_path here would point magma_dump and
        # maxSn reads at the wrong place after the vb moved
        self.assertTrue(o, "kvstore-{} not found under {}/{} on {} (stderr: {})".format(
            self.target_vb[0], self.data_path, bucket.uuid,
            self.vb_node_owners[0].ip, e))
        kvstore_path = o[0].strip()
        self.shard_path = "/".join(kvstore_path.split("/")[:-1])
        self.log.info("Shard path: {}".format(self.shard_path))
        return self.shard_path

    def get_max_sn(self, server=None, shard_path=None, target_vb=None):
        server = server or (self.vb_node_owners[0] if self.vb_node_owners else self.cluster.master)
        shard_path = shard_path or self.shard_path
        target_vb = target_vb or self.target_vb[0]

        if not shard_path:
            self.log.warning("Shard path not set")
            return {}

        shell = RemoteMachineShellConnection(server)
        cmd = "/opt/couchbase/bin/magma_dump {} tree-state --latest --kvstore {} | grep maxSn | tr -d ' ,' | cut -d: -f2".format(
            shard_path, target_vb)
        self.log.debug("CMD: {}".format(cmd))
        o, e = shell.execute_command(cmd)
        shell.disconnect()

        idx_sn = {}
        if o and len(o) >= 3:
            try:
                idx_sn["keyIndex"] = int(o[0].strip())
                idx_sn["seqIndex"] = int(o[1].strip())
                idx_sn["localIndex"] = int(o[2].strip())
            except (ValueError, IndexError) as ex:
                self.log.warning("Failed to parse maxSn: {}".format(ex))

        if idx_sn:
            self.log.debug("maxSn: key={}, seq={}, local={}".format(
                idx_sn.get("keyIndex"), idx_sn.get("seqIndex"), idx_sn.get("localIndex")))
        return idx_sn

    def get_current_target_index_sn(self):
        idx_sn = self.get_max_sn()
        if idx_sn and self.target_index in idx_sn:
            self.current_target_index_sn = idx_sn[self.target_index]
            return self.current_target_index_sn
        self.log.warning("Target index '{}' not found in maxSn".format(self.target_index))
        return 0

    def monitor_seqno_threshold(self, bucket):
        self.log.info("Monitoring {} seqno, target: {}".format(
            self.target_index, self.cycle_start_sn + self.seqno_threshold_per_cycle))

        read_failures = 0
        while not self.stop_event.is_set() and not self.threshold_reached.is_set():
            # An unreadable maxSn would otherwise read as 0 progress forever
            if self.target_index not in self.get_max_sn():
                read_failures += 1
                self.log.warning("Could not read {} maxSn ({}/{})".format(
                    self.target_index, read_failures, self.max_sn_read_failures))
                if read_failures >= self.max_sn_read_failures:
                    self.monitor_error = (
                        "Could not read {} maxSn for vb {} on {} {} times in a row "
                        "(shard_path={})".format(
                            self.target_index, self.target_vb[0],
                            [s.ip for s in self.vb_node_owners],
                            read_failures, self.shard_path))
                    break
                self.stop_event.wait(self.seqno_check_interval)
                continue
            read_failures = 0

            current_sn = self.get_current_target_index_sn()
            seqno_progress = current_sn - self.cycle_start_sn
            target_sn = self.seqno_threshold_per_cycle

            self.log.info("{} seqno: current={}, progress={}/{} ({:.2f}%)".format(
                self.target_index, current_sn, seqno_progress, target_sn,
                (seqno_progress / target_sn * 100) if target_sn > 0 else 0))

            if seqno_progress >= target_sn:
                self.log.info("Threshold reached: {} seqno progressed by {}".format(
                    self.target_index, seqno_progress))
                self.threshold_reached.set()
                break

            self.log.info("Next seqno check in {}s".format(self.seqno_check_interval))
            self.stop_event.wait(self.seqno_check_interval)

    def run_magma_dump_check_duplicates(self, servers=None, key_prefixes=None, shard_path=None):
        key_prefixes = key_prefixes or self.key_prefix_list
        servers = servers or self.vb_node_owners
        shard_path = shard_path or self.shard_path

        results = {}
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            results[server.ip] = {}
            
            # Hot docs use CircularKey (a RandomKey): keys are '<random>-<index>'
            # and never contain the hot_ prefix, so hot is derived from the
            # total. The total must be taken per index, otherwise hot key/seq
            # differ only by the cold/warm deltas and hot duplicates go unseen
            total_docs = {}
            for idx in ["key", "seq"]:
                total_cmd = "/opt/couchbase/bin/magma_dump {} docs --index {} --kvstore {} | wc -l".format(
                    shard_path, idx, self.target_vb[0])
                self.log.info("CMD: {}".format(total_cmd))
                total_out, _ = shell.execute_command(total_cmd)
                total_docs[idx] = int(total_out[0].strip()) if total_out else 0
            self.log.info("Total docs in vb {}: {}".format(self.target_vb[0], total_docs))

            for prefix in key_prefixes:
                if prefix == self.hot_prefix:
                    continue
                
                results[server.ip][prefix] = {}
                for idx in ["key", "seq"]:
                    cmd = "/opt/couchbase/bin/magma_dump {} docs --index {} --kvstore {} | grep -c '{}' || true".format(
                        shard_path, idx, self.target_vb[0], prefix)
                    self.log.info("CMD: {}".format(cmd))
                    o, e = shell.execute_command(cmd)
                    count = int(o[0].strip()) if o else 0
                    results[server.ip][prefix][idx] = count
                    
                    if count == 0 and idx == "key":
                        sample_cmd = "/opt/couchbase/bin/magma_dump {} docs --index key --kvstore {} | head -5".format(
                            shard_path, self.target_vb[0])
                        sample_out, _ = shell.execute_command(sample_cmd)
                        if sample_out:
                            self.log.debug("Sample output: {}".format(sample_out))
                    
                    self.log.info("{} {} {}: {} docs".format(server.ip, prefix, idx, count))
            
            results[server.ip][self.hot_prefix] = {}
            for idx in ["key", "seq"]:
                cold_count = results[server.ip].get(self.cold_prefix, {}).get(idx, 0)
                warm_count = results[server.ip].get(self.warm_prefix, {}).get(idx, 0)
                hot_count = total_docs[idx] - cold_count - warm_count
                results[server.ip][self.hot_prefix][idx] = hot_count
                self.log.info("{} hot_{} calculated: total={} - cold={} - warm={} = {}".format(
                    server.ip, idx, total_docs[idx], cold_count, warm_count, hot_count))
            
            shell.disconnect()

        duplicates_found = False
        duplicate_errors = []
        for server_ip, prefixes in results.items():
            for prefix, indices in prefixes.items():
                key_count = indices.get("key", 0)
                seq_count = indices.get("seq", 0)
                if key_count != seq_count and (key_count > 0 or seq_count > 0):
                    error_msg = "DUPLICATE: {} {} key={} seq={}".format(server_ip, prefix, key_count, seq_count)
                    self.log.error(error_msg)
                    duplicate_errors.append(error_msg)
                    duplicates_found = True

        if duplicates_found:
            self.fail("Duplicates found:\n" + "\n".join(duplicate_errors))

        return results, duplicates_found

    def parallel_read_docs_sirius(self, bucket, scope, collection,
                                  key_prefix, start, end, max_threads=None):
        self.log.info("Reading {} docs: {}:{}".format(key_prefix, start, end))

        key_type = "CircularKey" if key_prefix == self.hot_prefix else "SimpleKey"

        loader = SiriusCouchbaseLoader(
            server_ip=self.cluster.master.ip,
            server_port=self.cluster.master.port,
            username=self.cluster.master.rest_username,
            password=self.cluster.master.rest_password,
            bucket=bucket,
            scope_name=scope,
            collection_name=collection,
            key_prefix=key_prefix,
            key_size=self.key_size,
            doc_size=self.doc_size,
            key_type=key_type,
            read_percent=100,
            read_start_index=start,
            read_end_index=end,
            ops=10000,
            process_concurrency=self.loader_process_concurrency,
            num_vbuckets=self.bucket_num_vb,
            target_vbuckets=self._get_target_vbuckets(),
            track_failures=True
        )
        loader.create_doc_load_task()
        self.task_manager.add_new_task(loader)
        self.task_manager.get_task_result(loader)

        total_docs = end - start
        doc_not_found = 0
        other_errors = 0
        self.log.info(loader.fail.items())
        for key, error_dict in loader.fail.items():
            if "DocumentNotFoundException" in str(error_dict.get("error", "")):
                doc_not_found += 1
            else:
                other_errors += 1
        doc_found = total_docs - len(loader.fail)

        self.log.info("Read: found={}, notfound={}, errors={}".format(doc_found, doc_not_found, other_errors))
        return doc_found, doc_not_found, other_errors

    def evict_cache_and_read(self, bucket):
        self.PrintStep("Evict cache and read documents")
        self.sigkill_memcached(nodes=self.vb_node_owners)

        read_results = {}
        doc_configs = [
            (self.cold_prefix, 0, self.cold_docs),
            (self.warm_prefix, 0, self.warm_docs),
            (self.hot_prefix, 0, self.hot_docs)
        ]

        for prefix, start, end in doc_configs:
            found, not_found, errors = self.parallel_read_docs_sirius(
                bucket, self.target_scope, self.target_collection,
                prefix, start, end)
            read_results[prefix] = {"found": found, "not_found": not_found, "errors": errors}
            if not_found > 0:
                self.log.error("{} notfound: {}".format(prefix, not_found))

        return read_results

    def get_gsi_count(self, bucket, scope, collection):
        if not self.cluster.query_nodes:
            self.log.warning("No query nodes")
            return -1

        query = "SELECT count(*) as items FROM default:`{}`.`{}`.`{}` WHERE meta().id LIKE '%'".format(
            bucket.name, scope, collection)

        try:
            rest = RestConnection(self.cluster.query_nodes[0])
            # request_plus: count must include every KV mutation so far,
            # otherwise index lag reads as a mismatch
            result = rest.query_tool(query, timeout=300,
                                     query_params={"scan_consistency": "request_plus"})
            if result.get("status") == "success":
                count = result["results"][0]["items"]
                self.log.info("GSI count: {}".format(count))
                return count
        except Exception as e:
            self.log.error("GSI count failed: {}".format(e))
        return -1

    def trigger_rollback(self, bucket, mem_only_items=5000):
        self.PrintStep("Trigger rollback with {} mem-only items".format(mem_only_items))

        # Stop persistence on all nodes in cluster
        self.log.info("Stopping persistence on all nodes in cluster")
        persistence_error_sims = []
        for node in self.cluster.nodes_in_cluster:
            self.log.info("Stopping persistence on node {}".format(node.ip))
            shell_conn = RemoteMachineShellConnection(node)
            error_sim = CouchbaseError(self.log, shell_conn, node=node)
            error_sim.create(CouchbaseError.STOP_PERSISTENCE, bucket_name=bucket.name)
            persistence_error_sims.append((error_sim, shell_conn))

        self.sleep(30, "Wait after stopping persistence on all nodes")

        self.log.info("Loading {} {} docs (mem-only, will be discarded)".format(mem_only_items, self.rollback_prefix))
        
        # Same Sirius pattern as the other loads: genrate_docs_basic hashes
        # keys for 1024 vBuckets, so its docs would miss target_vb here
        loader = SiriusCouchbaseLoader(
            server_ip=self.cluster.master.ip,
            server_port=self.cluster.master.port,
            username=self.cluster.master.rest_username,
            password=self.cluster.master.rest_password,
            bucket=bucket,
            scope_name=self.target_scope,
            collection_name=self.target_collection,
            key_prefix=self.rollback_prefix,
            key_size=self.key_size,
            doc_size=self.doc_size,
            key_type="SimpleKey",
            create_percent=100,
            create_start_index=0,
            create_end_index=mem_only_items,
            process_concurrency=self.loader_process_concurrency,
            num_vbuckets=self.bucket_num_vb,
            target_vbuckets=self._get_target_vbuckets())
        loader.create_doc_load_task()
        self.task_manager.add_new_task(loader)
        self.assertTrue(self.task_manager.get_task_result(loader),
                        "Rollback mem-only doc load failed")

        # The mem-only docs land on target_vb, whose active copy need not be
        # on master. _wait_for_stat swallows timeouts, so wait on the task
        # directly and fail on its exception
        if self.use_target_vbucket:
            active_node = None
            for node in self.cluster.nodes_in_cluster:
                cbstat_obj = Cbstats(node)
                if self.target_vb[0] in cbstat_obj.vbucket_list(bucket.name, vbucket_type="active"):
                    active_node = node
                cbstat_obj.disconnect()
            self.assertIsNotNone(active_node, "No active node found for vb {}".format(self.target_vb[0]))
            self.log.info("Waiting for {} mem-only items in disk write queue on {} (active vb {})"
                          .format(mem_only_items, active_node.ip, self.target_vb[0]))
            stat_task = self.task.async_wait_for_stats(
                [active_node], bucket, "checkpoint",
                "persistence:num_items_for_cursor", "==", mem_only_items, timeout=300)
            self.task_manager.get_task_result(stat_task)
            self.assertIsNone(stat_task.exception,
                              "Mem-only items not queued on {}: {}".format(
                                  active_node.ip, stat_task.exception))
            self.log.info("Mem-only items confirmed in disk write queue (not persisted to disk)")
        else:
            self.log.info("use_target_vbucket=False: mem-only docs spread over all vBuckets, "
                          "per-node queue size not predictable, relying on post-rollback item count")

        self.sleep(30, "Mem-only items in memory")

        # Stop memcached on all nodes in cluster
        self.log.info("Stopping memcached on all nodes in cluster to trigger rollback")
        memcached_error_sims = []
        for node in self.cluster.nodes_in_cluster:
            self.log.info("Stopping memcached on node {}".format(node.ip))
            shell_conn = RemoteMachineShellConnection(node)
            error_sim = CouchbaseError(self.log, shell_conn, node=node)
            error_sim.create(CouchbaseError.KILL_MEMCACHED)
            memcached_error_sims.append((error_sim, shell_conn))

        self.sleep(5, "Wait for memcached to stop on all nodes")

        self.log.info("Waiting for warmup to complete on all nodes")
        self.assertTrue(self.bucket_util._wait_warmup_completed(
            bucket, servers=self.cluster.nodes_in_cluster,
            wait_time=self.wait_timeout * 10),
            "Warmup failed on cluster")

        # Restart persistence on all nodes
        self.log.info("Starting persistence on all nodes")
        for error_sim, shell_conn in persistence_error_sims:
            self.log.info("Starting persistence on node with shell {}".format(shell_conn.ip))
            error_sim.revert(CouchbaseError.STOP_PERSISTENCE, bucket_name=bucket.name)
            shell_conn.disconnect()

        # Disconnect memcached shells
        for error_sim, shell_conn in memcached_error_sims:
            shell_conn.disconnect()

        self.log.info("Rollback complete, verifying collection stats")
        # Verify collection-specific stats to confirm rollback (mem-only docs should be discarded)
        expected_items = self.cold_docs + self.warm_docs + self.hot_docs

        # Get collection-specific count (exclude system collections)
        collection_data = self.bucket_util.get_doc_count_per_collection(self.cluster, bucket)
        actual_items = collection_data[self.target_scope][self.target_collection]['items']

        self.log.info("Collection {}.{}: expected={}, actual={}".format(
            self.target_scope, self.target_collection, expected_items, actual_items))

        if actual_items == expected_items:
            self.log.info("Rollback verification passed: collection has {} items (rollback docs discarded)".format(
                expected_items))
        else:
            error_msg = "Rollback FAILED: expected {} items in collection {}.{}, but found {}".format(
                expected_items, self.target_scope, self.target_collection, actual_items)
            self.log.error(error_msg)
            self.fail(error_msg)

    def trigger_compaction(self, bucket):
        self.PrintStep("Trigger compaction")
        compaction_task = self.task.async_compact_bucket(self.cluster.master, bucket)
        self.task_manager.get_task_result(compaction_task)
        # CompactBucketTask can return before compaction finishes; the
        # magma_dump duplicate check that follows needs it complete
        self.assertTrue(
            self.bucket_util.wait_till_compaction_end(
                self.cluster.master, bucket.name, timeout_in_seconds=1800),
            "Compaction of {} did not complete".format(bucket.name))
        self.log.info("Compaction complete")

    def check_vulnerable_vbuckets(self, cycle_num):
        """Run the MB-67762 check_vulnerable_vbuckets.sh on every KV node.
        Fails if the script cannot read some kvstores (SCRIPT_FAILED), or if
        the cycle pushed maxSn past the script's threshold and no node flags
        the target vBucket."""
        target_line = "kvstore-{}: maxSn=".format(self.target_vb[0])
        flagged_on = []
        for node in self.cluster.kv_nodes:
            shell = RemoteMachineShellConnection(node)
            try:
                shell.copy_file_local_to_remote(_CHECK_VULNERABLE_LOCAL,
                                                _CHECK_VULNERABLE_REMOTE)
                o, _ = shell.execute_command("bash {} {}".format(
                    _CHECK_VULNERABLE_REMOTE, self.data_path))
            finally:
                shell.disconnect()
            self.log.info("check_vulnerable_vbuckets on {}:\n{}".format(
                node.ip, "\n".join(o)))
            status = [line.strip() for line in o if line.strip().startswith("Status:")]
            self.assertTrue(status, "Cycle {}: no Status line from check_vulnerable_vbuckets "
                                    "on {}".format(cycle_num, node.ip))
            self.assertFalse(status[-1].startswith("Status:SCRIPT_FAILED"),
                             "Cycle {}: check_vulnerable_vbuckets failed on {}: {}".format(
                                 cycle_num, node.ip, status[-1]))
            if any(target_line in line and "(VULNERABLE)" in line for line in o):
                flagged_on.append(node.ip)

        self.log.info("Cycle {}: vb {} flagged VULNERABLE on {}".format(
            cycle_num, self.target_vb[0], flagged_on))
        if self.seqno_threshold_per_cycle > _CHECK_VULNERABLE_THRESHOLD:
            self.assertTrue(flagged_on,
                            "Cycle {}: maxSn advanced {} this cycle but no node flags vb {} "
                            "as VULNERABLE".format(cycle_num, self.seqno_threshold_per_cycle,
                                                   self.target_vb[0]))

    def perform_swap_rebalance(self, nodes_in=1, nodes_out=1):
        self.PrintStep("Swap rebalance: in={}, out={}".format(nodes_in, nodes_out))

        if len(self.available_servers) < nodes_in:
            self.log.warning("Not enough available servers")
            return False

        servs_in = self.available_servers[:nodes_in]
        kv_nodes = [n for n in self.cluster.kv_nodes if n.ip != self.cluster.master.ip]
        if len(kv_nodes) < nodes_out:
            self.log.warning("Not enough KV nodes to remove")
            return False
        servs_out = kv_nodes[:nodes_out]

        # Get services from outgoing nodes to use for incoming nodes
        services_list = []
        for node_out in servs_out:
            # Find the node info from cluster to get current services
            node_info = self.cluster_util.find_node_info(self.cluster.master, node_out)
            if node_info and hasattr(node_info, 'services') and node_info.services:
                # Services might be an array like ["kv", "index", "n1ql"] or already a string
                if isinstance(node_info.services, list):
                    services_list.append(",".join(node_info.services))
                else:
                    services_list.append(node_info.services)
            else:
                services_list.append("kv")
        
        self.log.info("Rebalance: in={}, out={}, services={}".format(
            [s.ip for s in servs_in], [s.ip for s in servs_out], services_list))

        rebalance_task = self.task.async_rebalance(
            self.cluster, servs_in, servs_out,
            services=services_list,
            retry_get_process_num=3000)
        self.task_manager.get_task_result(rebalance_task)

        if rebalance_task.result:
            self.available_servers = [s for s in self.available_servers if s not in servs_in]
            self.available_servers.extend(servs_out)
            self.log.info("Swap rebalance complete")
            return True
        else:
            self.log.error("Swap rebalance failed")
            return False

    def perform_failover_recovery(self, recovery_type="full"):
        self.PrintStep("Failover + {} recovery".format(recovery_type))

        rest = RestConnection(self.cluster.master)
        kv_nodes = [n for n in self.cluster.kv_nodes if n.ip != self.cluster.master.ip]
        if not kv_nodes:
            self.log.warning("No nodes for failover")
            return False

        chosen_node = random.choice(kv_nodes)
        failover_node = self.cluster_util.find_node_info(self.cluster.master, chosen_node)

        self.log.info("Failover node: {} ({})".format(chosen_node.ip, failover_node.id))

        success = rest.fail_over(failover_node.id, graceful=True)
        if not success:
            self.log.error("Failover failed")
            return False

        self.sleep(60, "Failover completion")
        self.assertTrue(rest.monitorRebalance(progress_count=50000), "Failover rebalance failed")

        self.log.info("Setting recovery type: {}".format(recovery_type))
        rest.set_recovery_type(otpNode=failover_node.id, recoveryType=recovery_type)
        self.sleep(30, "Recovery type set")

        rebalance_task = self.task.async_rebalance(
            self.cluster, [], [],
            retry_get_process_num=3000)
        self.task_manager.get_task_result(rebalance_task)

        if rebalance_task.result:
            self.log.info("{} recovery complete".format(recovery_type))
            return True
        else:
            self.log.error("{} recovery failed".format(recovery_type))
            return False

    def create_drop_collection(self, bucket):
        self.PrintStep("Create and drop temp collection")

        temp_collection = "temp_coll_{}".format(int(time.time()))

        self.bucket_util.create_collection(
            self.cluster.master, bucket,
            self.target_scope, {"name": temp_collection})
        self.sleep(5, "Collection created")
        loader = SiriusCouchbaseLoader(
            server_ip=self.cluster.master.ip,
            server_port=self.cluster.master.port,
            username=self.cluster.master.rest_username,
            password=self.cluster.master.rest_password,
            bucket=bucket,
            scope_name=self.target_scope,
            collection_name=temp_collection,
            key_prefix="temp_",
            key_size=self.key_size,
            doc_size=self.doc_size,
            key_type="SimpleKey",
            create_percent=100,
            create_start_index=0,
            create_end_index=1000,
            process_concurrency=self.loader_process_concurrency,
            num_vbuckets=self.bucket_num_vb,
            target_vbuckets=self._get_target_vbuckets()
        )
        loader.create_doc_load_task()
        self.task_manager.add_new_task(loader)
        self.task_manager.get_task_result(loader)

        self.sleep(5, "Docs loaded to temp collection")

        self.bucket_util.drop_collection(
            self.cluster.master, bucket,
            self.target_scope, temp_collection)
        self.sleep(5, "Collection dropped")

        self.log.info("Temp collection {} lifecycle complete".format(temp_collection))

    def slow_mutation_phase(self, bucket, duration=None):
        duration = duration or self.slow_phase_duration
        self.PrintStep("Slow mutation phase: {} sec".format(duration))
        loader = SiriusCouchbaseLoader(
            server_ip=self.cluster.master.ip,
            server_port=self.cluster.master.port,
            username=self.cluster.master.rest_username,
            password=self.cluster.master.rest_password,
            bucket=bucket,
            scope_name=self.target_scope,
            collection_name=self.target_collection,
            key_prefix=self.warm_prefix,
            key_size=self.key_size,
            doc_size=self.doc_size,
            key_type="SimpleKey",
            update_percent=100,
            update_start_index=0,
            update_end_index=self.warm_docs,
            ops=100,
            process_concurrency=self.loader_process_concurrency,
            num_vbuckets=self.bucket_num_vb,
            target_vbuckets=self._get_target_vbuckets()
        )
        loader.create_doc_load_task()
        self.task_manager.add_new_task(loader)

        self.sleep(duration, "Slow mutation phase")
        loader.end_task()
        self.log.info("Slow mutation complete")

    def setup_xdcr(self, src_bucket, xdcr_type):
        if not self.enable_xdcr:
            return

        self.current_xdcr_type = xdcr_type
        self.PrintStep("Setup XDCR: type={}".format(xdcr_type))

        if xdcr_type == "inter":
            self._setup_inter_cluster_xdcr(src_bucket)
        else:
            self._setup_intra_cluster_xdcr(src_bucket)

    def _setup_inter_cluster_xdcr(self, src_bucket):
        if len(self.available_servers) < self.xdcr_remote_nodes:
            self.log.warning("Not enough servers for XDCR remote cluster")
            return

        remote_nodes = self.available_servers[:self.xdcr_remote_nodes]
        self.available_servers = self.available_servers[self.xdcr_remote_nodes:]

        self.log.info("Creating XDCR remote cluster: {} nodes {}".format(
            self.xdcr_remote_nodes, [n.ip for n in remote_nodes]))

        self.xdcr_cluster = CBCluster(
            name="xdcr_remote", servers=remote_nodes,
            vbuckets=self.vbuckets or CbServer.total_vbuckets)
        self.xdcr_cluster.nodes_in_cluster.append(self.xdcr_cluster.master)

        self._initialize_nodes(
            self.task, self.xdcr_cluster,
            self.disabled_consistent_view,
            self.rebalanceIndexWaitingDisabled,
            self.rebalanceIndexPausingDisabled,
            self.maxParallelIndexers,
            self.maxParallelReplicaIndexers,
            self.port, self.quota_percent)

        if len(remote_nodes) > 1:
            self.task.rebalance(self.xdcr_cluster, remote_nodes[1:], [],
                                validate_bucket_ranking=False)
            self.sleep(10, "Remote cluster rebalance")

        self.log.info("Creating bucket {} on remote cluster".format(src_bucket.name))
        remote_replicas = 0
        if len(self.xdcr_cluster.servers) > 1:
            remote_replicas = min(src_bucket.replicaNumber, len(self.xdcr_cluster.servers) - 1)
        
        xdcr_bucket = Bucket({
            Bucket.name: src_bucket.name,
            Bucket.bucketType: src_bucket.bucketType,
            Bucket.ramQuotaMB: src_bucket.ramQuotaMB,
            Bucket.replicaNumber: remote_replicas,
            Bucket.storageBackend: src_bucket.storageBackend,
            Bucket.evictionPolicy: src_bucket.evictionPolicy,
            Bucket.flushEnabled: Bucket.FlushBucket.ENABLED
        })
        
        bucket_max_retries = 3
        bucket_retry_delay = 10
        for attempt in range(bucket_max_retries):
            try:
                self.bucket_util.create_bucket(self.xdcr_cluster, xdcr_bucket)
                self.log.info("Bucket created on remote cluster")
                break
            except Exception:
                if attempt < bucket_max_retries - 1:
                    self.log.warning("Bucket creation attempt {}/{} failed, retrying".format(
                        attempt + 1, bucket_max_retries))
                    self.sleep(bucket_retry_delay, "Retry bucket creation")
                    bucket_retry_delay *= 2
                else:
                    self.log.error("Bucket creation failed after {} attempts".format(bucket_max_retries))
                    raise

        self.sleep(10, "Remote cluster ready")

        self.log.info("Verifying remote cluster: {}:{}".format(
            self.xdcr_cluster.master.ip, CbServer.port))
        try:
            remote_rest = RestConnection(self.xdcr_cluster.master)
            pools_info = remote_rest.get_pools_info()
            if pools_info:
                self.log.info("Remote cluster accessible")
            else:
                self.log.warning("Remote cluster pools info empty")
        except Exception as e:
            self.log.warning("Remote cluster health check failed: {}".format(e))

        rest = RestConnection(self.cluster.master)
        remote_port = CbServer.port
        max_retries = 5
        retry_delay = 10
        for attempt in range(max_retries):
            try:
                self.log.info("Adding remote cluster reference (attempt {}): {}:{}".format(
                    attempt + 1, self.xdcr_cluster.master.ip, remote_port))
                rest.add_remote_cluster(
                    self.xdcr_cluster.master.ip, remote_port,
                    self.xdcr_cluster.master.rest_username,
                    self.xdcr_cluster.master.rest_password,
                    self.xdcr_remote_name)
                self.log.info("Remote cluster reference added")
                break
            except Exception as e:
                self.log.warning("Add remote cluster attempt {} failed: {}".format(attempt + 1, e))
                if attempt < max_retries - 1:
                    self.sleep(retry_delay, "Retry remote cluster connection")
                    retry_delay *= 2
                else:
                    self.log.error("Failed after {} attempts".format(max_retries))
                    raise

        self.xdcr_repl_id = rest.start_replication(
            "continuous", src_bucket.name, self.xdcr_remote_name,
            rep_type="xmem", toBucket=src_bucket.name)

        if self.xdcr_repl_id:
            self.log.info("XDCR replication started: {}".format(self.xdcr_repl_id))
        else:
            self.log.error("XDCR replication start failed")

    def _setup_intra_cluster_xdcr(self, src_bucket):
        dest_bucket_name = "{}_xdcr_dest".format(src_bucket.name)
        self.log.info("Creating intra-cluster XDCR dest bucket: {}".format(dest_bucket_name))

        dest_bucket = Bucket({
            Bucket.name: dest_bucket_name,
            Bucket.bucketType: src_bucket.bucketType,
            Bucket.ramQuotaMB: src_bucket.ramQuotaMB,
            Bucket.replicaNumber: src_bucket.replicaNumber,
            Bucket.storageBackend: src_bucket.storageBackend,
            Bucket.evictionPolicy: src_bucket.evictionPolicy,
            Bucket.flushEnabled: Bucket.FlushBucket.ENABLED
        })
        self.bucket_util.create_bucket(self.cluster, dest_bucket)
        self.xdcr_dest_bucket_name = dest_bucket_name

        rest = RestConnection(self.cluster.master)
        rest.add_remote_cluster(
            self.cluster.master.ip, CbServer.port,
            self.cluster.master.rest_username,
            self.cluster.master.rest_password,
            self.xdcr_remote_name)

        self.xdcr_repl_id = rest.start_replication(
            "continuous", src_bucket.name, self.xdcr_remote_name,
            toBucket=dest_bucket_name)

        if self.xdcr_repl_id:
            self.log.info("Intra-cluster XDCR started: {}".format(self.xdcr_repl_id))
        else:
            self.log.error("XDCR start failed")

    def cleanup_xdcr(self):
        if not self.enable_xdcr:
            return

        self.PrintStep("Cleanup XDCR: type={}".format(self.current_xdcr_type))
        try:
            rest = RestConnection(self.cluster.master)

            try:
                rest.remove_all_replications()
                self.log.info("XDCR replications stopped")
            except Exception as e:
                self.log.warning("Failed to stop replications: {}".format(e))

            try:
                rest.remove_remote_cluster(self.xdcr_remote_name)
                self.log.info("Remote cluster reference removed")
            except Exception as e:
                self.log.warning("Failed to remove remote cluster: {}".format(e))

            if self.current_xdcr_type == "intra" and hasattr(self, 'xdcr_dest_bucket_name'):
                dest_bucket = None
                for bucket in self.cluster.buckets:
                    if bucket.name == self.xdcr_dest_bucket_name:
                        dest_bucket = bucket
                        break
                if dest_bucket:
                    self.bucket_util.delete_bucket(self.cluster, dest_bucket)
                    self.log.info("Deleted intra-cluster destination bucket: {}".format(self.xdcr_dest_bucket_name))

            if self.current_xdcr_type == "inter" and self.xdcr_cluster:
                try:
                    for bucket in self.xdcr_cluster.buckets:
                        self.bucket_util.delete_bucket(self.xdcr_cluster, bucket)
                        self.log.info("Deleted remote bucket: {}".format(bucket.name))
                except Exception as e:
                    self.log.warning("Failed to delete remote bucket: {}".format(e))
                
                # Return XDCR cluster servers back to available_servers pool for next cycle
                if self.xdcr_cluster.servers:
                    self.available_servers.extend(self.xdcr_cluster.servers)
                    self.log.info("Returned {} XDCR servers to available pool".format(len(self.xdcr_cluster.servers)))

            # Reset state variables for clean next run
            self.xdcr_repl_id = None
            if self.current_xdcr_type == "inter":
                self.xdcr_cluster = None
            if self.current_xdcr_type == "intra" and hasattr(self, 'xdcr_dest_bucket_name'):
                delattr(self, 'xdcr_dest_bucket_name')
            self.current_xdcr_type = None

            self.log.info("XDCR cleanup complete")
        except Exception as e:
            self.log.warning("XDCR cleanup failed: {}".format(e))

    def run_xdcr_tests(self, bucket):
        if not self.enable_xdcr:
            return

        xdcr_types = ["intra", "inter"]

        for xdcr_type in xdcr_types:
            self.PrintStep("XDCR test: type={}".format(xdcr_type))

            self.setup_xdcr(bucket, xdcr_type=xdcr_type)

            if not self.xdcr_repl_id:
                self.cleanup_xdcr()
                self.fail("XDCR setup failed for type={} (enable_xdcr=True)".format(xdcr_type))

            self.verify_xdcr_consistency(bucket)
            
            self.PrintStep("Final validation for XDCR type={}".format(xdcr_type))
            final_read = self.evict_cache_and_read(bucket)
            for prefix, results in final_read.items():
                self.assertEqual(results["not_found"], 0,
                                 "Docs missing for {} in XDCR type={}".format(prefix, xdcr_type))
            
            self.validate_item_counts(bucket)
            self.verify_xdcr_consistency(bucket)
            self.cleanup_xdcr()
            
            self.log.info("XDCR test complete: type={}".format(xdcr_type))

    def verify_xdcr_consistency(self, bucket, timeout=300):
        if not self.enable_xdcr or not self.xdcr_repl_id:
            return True

        self.PrintStep("Verify XDCR consistency: type={}".format(self.current_xdcr_type))

        src_rest = RestConnection(self.cluster.master)
        xdcr_utils = XDCRUtils(self.cluster.master)
        
        self.log.info("Waiting for XDCR replication to complete")
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                changes_left = xdcr_utils.get_xdcr_queue_size(bucket.name)
                if changes_left is not None:
                    self.log.info("XDCR replication_changes_left: {}".format(changes_left))
                    if changes_left == 0:
                        self.log.info("XDCR replication completed")
                        break
                else:
                    self.log.warning("Failed to get XDCR queue size: returned None")
            except Exception as e:
                self.log.warning("Failed to get XDCR queue size: {}".format(e))
            self.sleep(10, "Waiting for XDCR replication")
        else:
            self.log.warning("XDCR replication wait timeout after {} seconds".format(timeout))

        self.sleep(5, "Final sync after replication_changes_left=0")

        # Get collection-specific counts (exclude system collections)
        src_collection_data = self.bucket_util.get_doc_count_per_collection(self.cluster, bucket)
        src_count = src_collection_data[self.target_scope][self.target_collection]['items']
        self.log.info("Source collection {}.{} count: {}".format(
            self.target_scope, self.target_collection, src_count))

        if self.current_xdcr_type == "inter" and self.xdcr_cluster:
            dest_bucket = self.xdcr_cluster.buckets[0]
            dest_collection_data = self.bucket_util.get_doc_count_per_collection(
                self.xdcr_cluster, dest_bucket)
            dest_count = dest_collection_data[self.target_scope][self.target_collection]['items']
            self.log.info("Destination collection {}.{} count: {}".format(
                self.target_scope, self.target_collection, dest_count))
        else:
            # Intra-cluster: get destination bucket from current cluster
            dest_bucket = None
            for bkt in self.cluster.buckets:
                if bkt.name == self.xdcr_dest_bucket_name:
                    dest_bucket = bkt
                    break
            if dest_bucket:
                dest_collection_data = self.bucket_util.get_doc_count_per_collection(
                    self.cluster, dest_bucket)
                dest_count = dest_collection_data[self.target_scope][self.target_collection]['items']
                self.log.info("Destination collection {}.{} count: {}".format(
                    self.target_scope, self.target_collection, dest_count))
            else:
                # Fallback to bucket-level stats if destination bucket not found
                dest_bucket_info = src_rest.get_bucket_details(bucket_name=self.xdcr_dest_bucket_name)
                dest_count = dest_bucket_info['basicStats']['itemCount']
                self.log.warning("Destination bucket {} not found in cluster, using bucket-level count: {}".format(
                    self.xdcr_dest_bucket_name, dest_count))

        self.log.info("XDCR count: src={}, dest={}".format(src_count, dest_count))

        if src_count == dest_count:
            self.log.info("XDCR consistent")
            return True
        else:
            diff = abs(src_count - dest_count)
            error_msg = "XDCR mismatch: src={}, dest={}, diff={}".format(src_count, dest_count, diff)
            self.fail(error_msg)

    def validate_item_counts(self, bucket):
        self.PrintStep("Validate item counts")
        self.assertTrue(
            self.bucket_util.validate_active_replica_item_count(
                self.cluster, bucket, timeout=300),
            "Active/replica item count validation failed")
        self.log.info("Item count validation passed")

    def create_gsi_index(self, bucket, defer_build=False):
        if not self.cluster.query_nodes:
            self.log.warning("No query nodes")
            return False

        self.PrintStep("Create GSI index: {}".format(self.gsi_index_name))

        if defer_build:
            create_query = "CREATE INDEX `{}` ON `{}`.`{}`.`{}`(META().id) WITH {{'defer_build': true}}".format(
                self.gsi_index_name, bucket.name, self.target_scope, self.target_collection)
        else:
            create_query = "CREATE INDEX `{}` ON `{}`.`{}`.`{}`(META().id) USING GSI".format(
                self.gsi_index_name, bucket.name, self.target_scope, self.target_collection)

        try:
            rest = RestConnection(self.cluster.query_nodes[0])
            self.log.info("Query: {}".format(create_query))
            result = rest.query_tool(create_query, timeout=600)
            if result.get("status") == "success":
                self.log.info("GSI index created (defer_build={})".format(defer_build))
                return True
            else:
                self.log.error("GSI index creation failed: {}".format(result))
                return False
        except Exception as e:
            self.log.error("GSI index error: {}".format(e))
            return False

    def build_gsi_index(self, bucket):
        if not self.cluster.query_nodes:
            return False

        build_query = "BUILD INDEX on `{}`.`{}`.`{}`({}) USING GSI".format(
            bucket.name, self.target_scope, self.target_collection, self.gsi_index_name)

        try:
            rest = RestConnection(self.cluster.query_nodes[0])
            self.log.info("Query: {}".format(build_query))
            result = rest.query_tool(build_query, timeout=600)
            if result.get("status") == "success":
                self.log.info("GSI index build started")
                return True
        except Exception as e:
            self.log.warning("GSI build error: {}".format(e))
        return False

    def drop_gsi_index(self, bucket):
        if not self.cluster.query_nodes:
            return False

        drop_query = "DROP INDEX `{}` ON `{}`.`{}`.`{}` USING GSI".format(
            self.gsi_index_name, bucket.name, self.target_scope, self.target_collection)

        try:
            rest = RestConnection(self.cluster.query_nodes[0])
            self.log.info("Query: {}".format(drop_query))
            result = rest.query_tool(drop_query, timeout=300)
            if result.get("status") == "success":
                self.log.info("GSI index dropped")
                return True
        except Exception as e:
            self.log.warning("GSI drop error: {}".format(e))
        return False

    def wait_for_index_online(self, index_name, timeout=300):
        self.log.info("Waiting for index {} online".format(index_name))
        rest = RestConnection(self.cluster.query_nodes[0])
        start_time = time.time()

        while time.time() - start_time < timeout:
            query = "SELECT state FROM system:indexes WHERE name='{}'".format(index_name)
            try:
                result = rest.query_tool(query, timeout=60)
                if result.get("status") == "success" and result.get("results"):
                    state = result["results"][0].get("state")
                    if state == "online":
                        self.log.info("Index online")
                        return True
                    self.log.debug("Index state: {}".format(state))
            except Exception as e:
                self.log.warning("Index state check error: {}".format(e))
            self.sleep(10, "Index online check")

        self.log.warning("Index online timeout")
        return False

    def rebuild_gsi_index(self, bucket):
        if not self.cluster.query_nodes:
            self.log.warning("No query nodes")
            return False

        self.PrintStep("Rebuild GSI index (OSO via DCP backfill)")

        self.drop_gsi_index(bucket)
        self.sleep(5, "Index dropped")

        if not self.create_gsi_index(bucket, defer_build=True):
            return False

        if not self.build_gsi_index(bucket):
            return False

        if not self.wait_for_index_online(self.gsi_index_name):
            self.log.warning("Index online timeout")
            return False

        self.log.info("GSI rebuild complete")
        return True

    def load_initial_documents(self, bucket):
        self.PrintStep("Load initial docs to vb {}".format(self.target_vb))

        self.log.info("Loading {} cold docs".format(self.cold_docs))
        cold_loader = SiriusCouchbaseLoader(
            server_ip=self.cluster.master.ip,
            server_port=self.cluster.master.port,
            username=self.cluster.master.rest_username,
            password=self.cluster.master.rest_password,
            bucket=bucket,
            scope_name=self.target_scope,
            collection_name=self.target_collection,
            key_prefix=self.cold_prefix,
            key_size=self.key_size,
            doc_size=self.doc_size,
            key_type="SimpleKey",
            update_percent=100,
            update_start_index=0,
            update_end_index=self.cold_docs,
            ops=self.ops,
            process_concurrency=self.loader_process_concurrency,
            num_vbuckets=self.bucket_num_vb,
            target_vbuckets=self._get_target_vbuckets()
        )
        cold_loader.create_doc_load_task()
        self.task_manager.add_new_task(cold_loader)
        self.task_manager.get_task_result(cold_loader)

        self.log.info("Loading {} warm docs".format(self.warm_docs))
        warm_loader = SiriusCouchbaseLoader(
            server_ip=self.cluster.master.ip,
            server_port=self.cluster.master.port,
            username=self.cluster.master.rest_username,
            password=self.cluster.master.rest_password,
            bucket=bucket,
            scope_name=self.target_scope,
            collection_name=self.target_collection,
            key_prefix=self.warm_prefix,
            key_size=self.key_size,
            doc_size=self.doc_size,
            key_type="SimpleKey",
            update_percent=100,
            update_start_index=0,
            update_end_index=self.warm_docs,
            ops=self.ops,
            process_concurrency=self.loader_process_concurrency,
            num_vbuckets=self.bucket_num_vb,
            target_vbuckets=self._get_target_vbuckets()
        )
        warm_loader.create_doc_load_task()
        self.task_manager.add_new_task(warm_loader)
        self.task_manager.get_task_result(warm_loader)

        self.log.info("Loading {} hot docs".format(self.hot_docs))
        hot_loader = SiriusCouchbaseLoader(
            server_ip=self.cluster.master.ip,
            server_port=self.cluster.master.port,
            username=self.cluster.master.rest_username,
            password=self.cluster.master.rest_password,
            bucket=bucket,
            scope_name=self.target_scope,
            collection_name=self.target_collection,
            key_prefix=self.hot_prefix,
            key_size=self.key_size,
            doc_size=self.doc_size,
            key_type="CircularKey",
            update_percent=100,
            update_start_index=0,
            update_end_index=self.hot_docs,
            ops=self.ops,
            process_concurrency=self.loader_process_concurrency,
            num_vbuckets=self.bucket_num_vb,
            target_vbuckets=self._get_target_vbuckets()
        )
        hot_loader.create_doc_load_task()
        self.task_manager.add_new_task(hot_loader)
        self.task_manager.get_task_result(hot_loader)

        self.sleep(10, "Initial doc load")
        self.bucket_util.print_bucket_stats(self.cluster)
        self.log.info("Initial load complete")

    def continuous_hot_updates(self, bucket):
        """
        Continuously update hot_ documents until stop_mutations is set.
        Uses CircularKey with iterations=-1 for infinite continuous updates.
        Runs in separate thread per cycle, stopped before checkpoint actions
        via _stop_cycle_threads(). If the loader dies on its own, loader_error
        is set so the main loop fails the test instead of waiting forever.
        """
        self.log.info("Starting continuous hot updates with CircularKey (iterations=-1)")
        error = None
        try:
            hot_loader = SiriusCouchbaseLoader(
                server_ip=self.cluster.master.ip,
                server_port=self.cluster.master.port,
                username=self.cluster.master.rest_username,
                password=self.cluster.master.rest_password,
                bucket=bucket,
                scope_name=self.target_scope,
                collection_name=self.target_collection,
                key_prefix=self.hot_prefix,
                key_size=self.key_size,
                doc_size=self.doc_size,
                key_type="CircularKey",
                update_percent=100,
                update_start_index=0,
                update_end_index=self.hot_docs,
                ops=self.ops,
                iterations=-1,
                process_concurrency=self.loader_process_concurrency,
                num_vbuckets=self.bucket_num_vb,
                target_vbuckets=self._get_target_vbuckets()
            )
            hot_loader.create_doc_load_task()
            self.task_manager.add_new_task(hot_loader)
            self.hot_loader = hot_loader
            self.log.info("Hot updates running: {} ops/sec on vb {}".format(self.ops, self.target_vb))

            # Blocks until end_task() is called, or until Sirius reports the
            # task failed / stopped making progress
            if not self.task_manager.get_task_result(hot_loader):
                error = "hot update loader reported failure"
        except Exception as e:
            error = "hot update loader raised: {}".format(e)

        if not self.stop_mutations:
            self.loader_error = error or "hot update loader ended before the seqno threshold"
        self.log.info("Continuous hot updates stopped")

    def perform_checkpoint_actions(self, bucket, cycle_num):
        current_sn = self.get_current_target_index_sn()
        self.PrintStep("Checkpoint {} ({} seqno: {})".format(cycle_num, self.target_index, current_sn))

        self.slow_mutation_phase(bucket)

        # Fail here rather than at the end: a later rebalance/failover can
        # hide docs lost in this cycle
        read_results = self.evict_cache_and_read(bucket)
        for prefix, results in read_results.items():
            self.assertEqual(results["not_found"], 0,
                             "Cycle {}: {} docs not found for {}".format(
                                 cycle_num, results["not_found"], prefix))

        self.validate_item_counts(bucket)

        if self.cluster.query_nodes:
            if cycle_num == 1:
                self.PrintStep("Create GSI index")
                self.assertTrue(self.create_gsi_index(bucket),
                                "Cycle 1: GSI index creation failed")
            expected_count = self.cold_docs + self.warm_docs + self.hot_docs
            gsi_count = self.get_gsi_count(bucket, self.target_scope, self.target_collection)
            self.assertEqual(gsi_count, expected_count,
                             "Cycle {}: GSI count mismatch".format(cycle_num))

        self.trigger_rollback(bucket)
        self.trigger_compaction(bucket)
        # MB-67762: the diagnostic is only valid once compaction has completed
        self.check_vulnerable_vbuckets(cycle_num)
        if self.cluster.query_nodes:
            self.assertTrue(self.rebuild_gsi_index(bucket),
                            "Cycle {}: GSI rebuild failed".format(cycle_num))

        # Refresh vb location before magma_dump check
        self.find_vb_node_owners(bucket)
        self.find_kvstore_path(bucket)
        results, duplicates = self.run_magma_dump_check_duplicates()

        # Perform topology changes (vb may move to different nodes)
        if len(self.available_servers) >= 1 and len(self.cluster.kv_nodes) > 1:
            self.assertTrue(self.perform_swap_rebalance(nodes_in=1, nodes_out=1),
                            "Cycle {}: swap rebalance failed".format(cycle_num))

        recovery_type = "full" if cycle_num % 2 == 0 else "delta"
        if len(self.cluster.kv_nodes) > self.num_replicas + 1:
            self.assertTrue(self.perform_failover_recovery(recovery_type=recovery_type),
                            "Cycle {}: failover + {} recovery failed".format(
                                cycle_num, recovery_type))

        # Refresh vb location after topology changes for any subsequent operations in checkpoint
        self.PrintStep("Refresh vb location after rebalance/failover")
        self.find_vb_node_owners(bucket)
        self.find_kvstore_path(bucket)

        # XDCR before the temp collection create/drop so its counts can be
        # asserted strictly
        self.run_xdcr_tests(bucket)

        self.create_drop_collection(bucket)

        result = self.check_coredump_exist(self.cluster.nodes_in_cluster)
        self.assertFalse(result, "CRASH detected at checkpoint {}".format(cycle_num))

        post_checkpoint_sn = self.get_current_target_index_sn()
        self.log.info("{} seqno after checkpoint: {}".format(self.target_index, post_checkpoint_sn))
        self.log.info("Checkpoint {} complete".format(cycle_num))

    def test_magma_endurance(self):
        """
        Load initial docs, run continuous hot updates, perform checkpoint actions after 3B seqno progression
        """
        bucket = self.cluster.buckets[0]

        self.PrintStep("Initialize vb {} info".format(self.target_vb))
        self.find_vb_node_owners(bucket)
        self.find_kvstore_path(bucket)

        initial_sn = self.get_current_target_index_sn()
        self.log.info("Initial {} seqno: {}".format(self.target_index, initial_sn))

        self.PrintStep("Load initial documents")
        self.load_initial_documents(bucket)

        post_load_sn = self.get_current_target_index_sn()
        self.log.info("{} seqno after load: {} (delta: {})".format(
            self.target_index, post_load_sn, post_load_sn - initial_sn))

        for cycle in range(1, self.num_cycles + 1):
            self.current_cycle = cycle
            self.threshold_reached.clear()

            # Refresh vb location at start of each cycle (vb may have moved during previous checkpoint)
            self.PrintStep("Cycle {}: Refresh vb location after topology changes".format(cycle))
            self.find_vb_node_owners(bucket)
            self.find_kvstore_path(bucket)

            self.cycle_start_sn = self.get_current_target_index_sn()
            target_sn = self.cycle_start_sn + self.seqno_threshold_per_cycle

            self.PrintStep("Cycle {}/{}: Wait for {} seqno {} (current={}, need +{})".format(
                cycle, self.num_cycles, self.target_index, target_sn, self.cycle_start_sn,
                self.seqno_threshold_per_cycle))

            # Start hot updates for this cycle - will run until threshold reached
            self.PrintStep("Cycle {}: Start continuous hot updates".format(cycle))
            self.stop_mutations = False
            self.stop_event.clear()
            self.hot_loader = None
            self.mutation_thread = threading.Thread(
                target=self.continuous_hot_updates,
                args=(bucket,),
                name="HotUpdatesThread_Cycle_{}".format(cycle))
            self.mutation_thread.start()

            self.monitor_thread = threading.Thread(
                target=self.monitor_seqno_threshold,
                args=(bucket,),
                name="SeqnoMonitorThread")
            self.monitor_thread.start()

            # Wait for seqno threshold to be reached
            # Hot updates running in background drive seqno progression.
            # No time limit: only the threshold or a thread failure ends the wait
            while not (self.threshold_reached.is_set()
                       or self.loader_error or self.monitor_error):
                self.sleep(30, "Cycle {}: wait for seqno threshold".format(cycle))

            # Stop hot updates before checkpoint validation
            self.PrintStep("Cycle {}: Stop hot updates for checkpoint validation".format(cycle))
            self._stop_cycle_threads()
            self.assertIsNone(self.loader_error, "Cycle {}: {}".format(cycle, self.loader_error))
            self.assertIsNone(self.monitor_error, "Cycle {}: {}".format(cycle, self.monitor_error))
            self.log.info("Hot updates stopped for cycle {}".format(cycle))

            final_cycle_sn = self.get_current_target_index_sn()
            self.log.info("Cycle {} complete: {} seqno {} -> {} (delta: {})".format(
                cycle, self.target_index, self.cycle_start_sn, final_cycle_sn, final_cycle_sn - self.cycle_start_sn))

            self.perform_checkpoint_actions(bucket, cycle)

        self.PrintStep("Final Validation")
        final_sn = self.get_current_target_index_sn()
        self.log.info("Final {} seqno: {} (progression: {})".format(
            self.target_index, final_sn, final_sn - initial_sn))

        final_read = self.evict_cache_and_read(bucket)
        for prefix, results in final_read.items():
            self.assertEqual(results["not_found"], 0,
                             "Final: docs missing for {}".format(prefix))

        self.validate_item_counts(bucket)

        if self.cluster.query_nodes:
            expected_count = self.cold_docs + self.warm_docs + self.hot_docs
            gsi_count = self.get_gsi_count(bucket, self.target_scope, self.target_collection)
            self.assertEqual(gsi_count, expected_count,
                             "Final GSI mismatch: expected={}, got={}".format(expected_count, gsi_count))

        result = self.check_coredump_exist(self.cluster.nodes_in_cluster)
        self.assertFalse(result, "CRASH detected")

        self.log.info("Endurance test complete: {} cycles, {} seqno progression: {}".format(
            self.num_cycles, self.target_index, final_sn - initial_sn))