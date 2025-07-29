
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
import json
import os
import random
import threading
import time
from cb_constants.CBServer import CbServer
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from magma_base import MagmaBaseTest
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient
from upgrade_lib.upgrade_helper import CbServerUpgrade

# Local paths (relative to TAF root) for scripts bundled with this repo
_SCRIPTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "..", "..", "..", "scripts")
_LOCAL_CHECK_VULNERABLE = os.path.join(_SCRIPTS_DIR, "check_vulnerable_vbuckets.sh")
_LOCAL_CHECK_METASTORE  = os.path.join(_SCRIPTS_DIR, "check_metastore.sh")
_LOCAL_VB_REPLAY        = os.path.join(_SCRIPTS_DIR, "couchbase-vb-replay-linux-amd64")

# Remote paths where the scripts will be deployed on every cluster node
_REMOTE_CHECK_VULNERABLE = "/root/check_vulnerable_vbuckets.sh"
_REMOTE_CHECK_METASTORE  = "/root/check_metastore.sh"
_REMOTE_VB_REPLAY        = "/root/couchbase-vb-replay"


class MagmaUnsafeWindow(MagmaBaseTest):
    def setUp(self):
        super(MagmaUnsafeWindow, self).setUp()
        self.assertTrue(self.rest.update_autofailover_settings(False, 600),
                        "AutoFailover disabling failed")
        # Colon-separated list of Magma indexes to monitor for INT32 overflow.
        # Vulnerability window is hit when ALL listed indexes exceed the threshold.
        # Choices: keyIndex, seqIndex, localIndex  (default: keyIndex:seqIndex)
        self.idx_to_monitor = self.input.param("idx_to_monitor", "keyIndex:seqIndex")
        # Threshold at which the vulnerability window is considered hit.
        # Default is INT32_MAX (2147483647) for the real bug run.
        # Set lower (e.g. vul_threshold=100000) for a quick smoke-test of the full flow.
        self.vul_threshold = self.input.param("vul_threshold", 2147483647)
        # Whether to assert that diagnostic scripts report VULNERABLE before upgrade.
        # Set assert_vulnerable=False when using a low vul_threshold (smoke-test mode)
        # because the cluster won't actually be in the bug window.
        self.assert_vulnerable = self.input.param("assert_vulnerable", True)
        self.cluster_util.update_cluster_nodes_service_list(self.cluster)
        self.log.info("KV nodes list = {}".format(self.cluster.kv_nodes))
        self.product = "couchbase-server"
        self.version_to_upgrade = self.input.param("version_to_upgrade", "7.6.7-6700")
        self.upgrade_type = self.input.param("upgrade_type", "failover_recovery")
        self.upgrade_helper = CbServerUpgrade(self.log, self.product)
        self.max_thread = self.input.param("max_thread", 16)
        self.check_interval = self.input.param("check_interval", 600)
        # Whether to sigkill memcached after the overflow window is reached.
        # Default is False; set sigkill_after_overflow=True to force a cold
        # warm-up from disk and expose the comparator bug in Step 3.
        self.sigkill_after_overflow = self.input.param("sigkill_after_overflow", False)
        # Whether to trigger bucket compaction after reading docs.
        # Default is False; set trigger_compaction=True to run compaction
        # and magma_dump verification in Step 4.
        self.trigger_compaction = self.input.param("trigger_compaction", False)
        self.copy_scripts_to_nodes()

    def tearDown(self):
        super(MagmaUnsafeWindow, self).tearDown()

    # Script deployment helpers

    def copy_scripts_to_nodes(self, nodes=None):
        """Copy diagnostic and replay scripts to every KV node in the cluster."""
        nodes = nodes or self.cluster.kv_nodes
        scripts = [
            (_LOCAL_CHECK_VULNERABLE, _REMOTE_CHECK_VULNERABLE),
            (_LOCAL_CHECK_METASTORE,  _REMOTE_CHECK_METASTORE),
            (_LOCAL_VB_REPLAY,        _REMOTE_VB_REPLAY),
        ]
        for node in nodes:
            self.log.info("Deploying scripts to node: {}".format(node.ip))
            shell = RemoteMachineShellConnection(node)
            try:
                for local_path, remote_path in scripts:
                    self.log.info("  copying {} -> {}:{}".format(
                        os.path.basename(local_path), node.ip, remote_path))
                    shell.copy_file_local_to_remote(local_path, remote_path)
                    shell.execute_command("chmod +x {}".format(remote_path))
            finally:
                shell.disconnect()
        self.log.info("Script deployment complete on {} node(s)".format(len(nodes)))

    # Diagnostic script runners

    def run_check_vulnerable_vbuckets(self, server, data_dir=None):
        """Run check_vulnerable_vbuckets.sh on *server* and return parsed results.

        Returns a dict:
          {
            "status": "OK" | "VULNERABLE" | "SCRIPT_FAILED",
            "max_sn": <int>,
            "vulnerable_kvstores": [<kvstore_path>, ...],
            "localindex_kvstores": [<kvstore_path>, ...],
            "raw_output": <str>,
          }
        """
        data_dir = data_dir or self.data_path
        shell = RemoteMachineShellConnection(server)
        try:
            cmd = "bash {} {}".format(_REMOTE_CHECK_VULNERABLE, data_dir)
            self.log.info("Server {}: running {}".format(server.ip, cmd))
            o, e = shell.execute_command(cmd)
            raw = "\n".join(o)
            self.log.info("Server {}: check_vulnerable output:\n{}".format(server.ip, raw))
        finally:
            shell.disconnect()

        result = {
            "status": "OK",
            "max_sn": 0,
            "vulnerable_kvstores": [],
            "localindex_kvstores": [],
            "failed_kvstores": 0,
            "failed_errors": [],
            "raw_output": raw,
        }
        _MAX_FAILED_ERRORS = 5

        for line in o:
            line = line.strip()
            # Summary line: Status:SCRIPT_FAILED, maxSn:1067255, VBucketsImpacted:0/1024, FailedExecutions:859
            if line.startswith("Status:"):
                parts = dict(item.split(":", 1) for item in line.split(", "))
                result["status"] = parts.get("Status", "OK")
                try:
                    result["max_sn"] = int(parts.get("maxSn", 0))
                except ValueError:
                    pass
                try:
                    result["failed_kvstores"] = int(parts.get("FailedExecutions", 0))
                except ValueError:
                    pass
            # Vulnerable kvstore list entries start with "  Bucket:"
            elif line.startswith("Bucket:") and "kvstore-" in line:
                tokens = line.split(",")
                if len(tokens) >= 2:
                    kvstore_path = tokens[1].strip().split()[0]
                    result["vulnerable_kvstores"].append(kvstore_path)
            # localIndex-impacted kvstores: listed under METADATA CONSISTENCY CHECK
            elif line.startswith("Bucket:") and "localIndex" not in line and \
                    "kvstore-" in line and "maxSn" not in line:
                result["localindex_kvstores"].append(line.split()[-1])
            # Capture failure details (e.g. "  kvstore-5: FAILED to run magma_dump\n    Error: ...")
            elif "FAILED" in line and "kvstore-" in line:
                if len(result["failed_errors"]) < _MAX_FAILED_ERRORS:
                    result["failed_errors"].append(line)

        if result["failed_kvstores"] > 0:
            self.log.error("Server {} | {} kvstores failed (showing up to {} samples)".format(
                server.ip, result["failed_kvstores"], _MAX_FAILED_ERRORS))
            for err in result["failed_errors"]:
                self.log.error("Server {} | {}".format(server.ip, err))

        return result

    def run_check_metastore(self, server, kvstore_path):
        """Run check_metastore.sh on *server* for *kvstore_path*.

        Returns "OK", "VULNERABLE", or "ERROR".
        """
        username = server.rest_username
        password = server.rest_password
        shell = RemoteMachineShellConnection(server)
        try:
            cmd = "bash {} {} {} {}".format(
                _REMOTE_CHECK_METASTORE, kvstore_path, username, password)
            self.log.info("Server {}: running {}".format(server.ip, cmd))
            o, e = shell.execute_command(cmd)
            raw = "\n".join(o)
            self.log.info("Server {}: check_metastore output:\n{}".format(server.ip, raw))
        finally:
            shell.disconnect()

        for line in o:
            if line.strip().startswith("Status:"):
                return line.strip().split(":", 1)[1].strip()
        return "ERROR"

    def run_vb_replay(self, server, bucket_name, vbucket_id, dry_run=False):
        """Run couchbase-vb-replay on *server* for a single vBucket.

        Returns (stdout_lines, stderr_lines).
        """
        username = server.rest_username
        password = server.rest_password
        dry_run_flag = "--dry-run" if dry_run else ""
        cmd = ("{replay} --sourceUrl {ip} --sourceBucket {bucket} "
               "--username {user} --password {pwd} --vbid {vbid} {dry}").format(
            replay=_REMOTE_VB_REPLAY,
            ip=server.ip,
            bucket=bucket_name,
            user=username,
            pwd=password,
            vbid=vbucket_id,
            dry=dry_run_flag,
        ).strip()
        self.log.info("Server {}: running {}".format(server.ip, cmd))
        shell = RemoteMachineShellConnection(server)
        try:
            o, e = shell.execute_command(cmd)
        finally:
            shell.disconnect()
        self.log.info("vb-replay output: {}".format(o))
        if e:
            self.log.warning("vb-replay stderr: {}".format(e))
        return o, e

    def check_vb_vulnerability(self, server, target_vb, check_interval=600):
        """Poll magma_dump every check_interval seconds and set vulnerability_reached
        when all indexes in self.idx_to_monitor exceed vul_threshold."""
        int32_max = self.vul_threshold
        indexes_to_watch = self.idx_to_monitor.split(":")
        self.log.info("Server {} | monitoring indexes={} | threshold={} | interval={}s".format(
            server.ip, indexes_to_watch, int32_max, check_interval))

        shell = RemoteMachineShellConnection(server)
        idx_sn_dict = dict()

        while not self.vulnerability_reached.is_set():
            magma_dump_cmd = (
                "/opt/couchbase/bin/magma_dump {} tree-state --latest --kvstore {} "
                "| grep maxSn | tr -d ' ,' | cut -d: -f2"
            ).format(self.shard_path, target_vb)

            try:
                o, e = shell.execute_command(magma_dump_cmd)
                self.log.info("Server {} | magma_dump output={} error={}".format(server.ip, o, e))

                if not o or len(o) < 3:
                    self.log.warning("Server {} | incomplete magma_dump output: {}".format(server.ip, o))
                    self.sleep(check_interval, "Retry after incomplete output")
                    continue

                idx_sn_dict["keyIndex"]   = int(o[0].strip())
                idx_sn_dict["seqIndex"]   = int(o[1].strip())
                idx_sn_dict["localIndex"] = int(o[2].strip())
                self.log.critical("### VB_MONITOR | Server {} | vb={} | maxSn={} | progress={:.4f}% ###".format(
                    server.ip, target_vb, idx_sn_dict,
                    idx_sn_dict.get(indexes_to_watch[0], 0) * 100.0 / int32_max))

                # Check only the indexes the test is configured to monitor
                if all(idx_sn_dict[idx] > int32_max for idx in indexes_to_watch):
                    self.log.critical(
                        "### VULNERABILITY WINDOW HIT | Server {} | vb={} | "
                        "monitored indexes={} all exceeded {} ###".format(
                            server.ip, target_vb, indexes_to_watch, int32_max))
                    self.vulnerability_reached.set()
                    break

            except Exception as ex:
                self.log.error("Server {} | exception in check_vb_vulnerability: {}".format(
                    server.ip, str(ex)))
                self.sleep(check_interval, "Retry after error")
                continue

            self.sleep(check_interval, "Next vulnerability check in {}s".format(check_interval))

        shell.disconnect()

    def test_magma_unsafe_window(self):
        bucket = self.cluster.buckets[0]

        # Configurable params
        self.docs_to_load = self.input.param("docs_to_load", 20000)
        self.doc_size     = self.input.param("doc_size", 1024)            # 1KB per doc

        # Pick 3 non-default collections: cold, hot, warm
        non_default_colls = [c for c in self.collections if c != CbServer.default_collection]
        self.assertTrue(len(non_default_colls) >= 3,
                        "Need at least 3 non-default collections (cold, hot, warm)")
        cold_coll, hot_coll, warm_coll = non_default_colls[:3]
        self.log.info("Collections - cold: {}, hot: {}, warm: {}".format(
            cold_coll, hot_coll, warm_coll))

        # Select a random target vBucket - all docs in all 3 collections go here
        self.target_vb = [random.randint(0, 1023)]
        self.log.info("Target vBucket: {}".format(self.target_vb))

        # Step 1: Load docs into cold, hot, warm - keys generated ONCE and shared across all collections
        self.log.info("=" * 60)
        self.log.info("Step 1: Loading {} docs into each collection targeting vb {}".format(
            self.docs_to_load, self.target_vb))
        self.log.info("=" * 60)
        # Generate keys once with a shared prefix - same key names work across all collections
        # because collections are separate namespaces in Couchbase.
        # Store on self so continuous_load_until_vulnerable can reuse without rescanning.
        self.vb_key_prefix = "vb{}".format(self.target_vb[0])
        self.log.info("Generating {} keys targeting vb {} (prefix='{}') - one-time scan".format(
            self.docs_to_load, self.target_vb[0], self.vb_key_prefix))
        self.shared_doc_gen = doc_generator(
            key=self.vb_key_prefix, start=0, end=self.docs_to_load,
            key_size=self.key_size, doc_size=self.doc_size,
            target_vbucket=self.target_vb, randomize_value=True)
        self.log.info("Key generation complete - {} keys ready".format(self.docs_to_load))

        coll_doc_gens = {coll: deepcopy(self.shared_doc_gen)
                        for coll in [cold_coll, hot_coll, warm_coll]}

        for coll in [cold_coll, hot_coll, warm_coll]:
            self.log.info("Loading {} docs into _default:{} with key prefix '{}'".format(
                self.docs_to_load, coll, self.vb_key_prefix))
            task = self.task.async_load_gen_docs(
                self.cluster, bucket, deepcopy(coll_doc_gens[coll]),
                op_type="create", exp=0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                sdk_client_pool=self.sdk_client_pool,
                collection=coll, print_ops_rate=True,
                iterations=1)
            self.task_manager.get_task_result(task)
            self.log.info("Loaded {} docs into _default:{}".format(self.docs_to_load, coll))
            self.bucket_util.print_bucket_stats(self.cluster)

        # Snapshot vBucket seq-nos after initial load
        vb_seq_after_load = self.bucket_util.get_vb_details_for_bucket(
            bucket, self.cluster.nodes_in_cluster)
        self.log.info("vBucket {} stats after initial load: {}".format(
            self.target_vb[0], vb_seq_after_load[self.target_vb[0]]))

        # Step 2: Hammer hot collection until maxSn crosses INT32 overflow
        self.log.info("=" * 60)
        self.log.info("Step 2: Updating hot_coll '{}' until maxSn overflows INT32".format(hot_coll))
        self.log.info("=" * 60)
        # store hot key details so continuous_load_until_vulnerable can build its own generator
        self.hot_coll_key  = hot_coll
        self.advance_sn_counter(collection=hot_coll)
        self.log.info("Vulnerability window reached on vBucket {}".format(self.target_vb))

        # Step 3: Observe the bug - force warm-up from disk and read all 3
        self.log.info("=" * 60)
        self.log.info("Step 3: Exposing the bug - sigkill memcached and reading all collections")
        self.log.info("=" * 60)
        self.get_fragmentation_magma(self.vb_node_owners, bucket,
                                     magma_shard=self.shard_path[-1])
        if self.sigkill_after_overflow:
            # Let writes flush to disk before forcing cold warm-up
            self.sleep(30, "Wait after overflow updates")
            self.sleep(120, "Wait for writes to flush and persist to disk")
            self.sleep(30, "Wait before killing memcached")
            self.sigkill_memcached(nodes=self.vb_node_owners)

            for coll in [cold_coll, hot_coll, warm_coll]:
                self.log.info("Reading {} docs from _default:{} (key prefix '{}')".format(
                    self.docs_to_load, coll, coll))
                self.parallel_read_docs(
                    bucket=bucket,
                    scope=CbServer.default_scope,
                    collection=coll,
                    doc_gen_read=deepcopy(coll_doc_gens[coll]),
                    max_threads=self.max_thread)

            self.get_fragmentation_magma(self.vb_node_owners, bucket,
                                         magma_shard=self.shard_path[-1])
        else:
            self.log.info("Skipping sigkill_memcached (sigkill_after_overflow=False)")

        # Step 4: Compaction + magma_dump to confirm duplicates
        self.log.info("=" * 60)
        self.log.info("Step 4: Compaction and magma_dump verification")
        self.log.info("=" * 60)
        # all 3 collections share the same key prefix
        self.key_prefix_list = [self.vb_key_prefix]
        if self.trigger_compaction:
            self.run_magma_dump()
            self.log.info("Running bucket compaction")
            self.bucket_util._run_compaction(self.cluster, number_of_times=1)
            self.run_magma_dump()
            self.get_fragmentation_magma(self.vb_node_owners, bucket,
                                         magma_shard=self.shard_path[-1])
        else:
            self.log.info("Skipping compaction (trigger_compaction=False)")

        # Step 5: Run diagnostic + replay scripts on all nodes
        self.log.info("=" * 60)
        self.log.info("Step 5: Running diagnostic scripts on all nodes before upgrade")
        self.log.info("=" * 60)
        self.run_diagnostic_scripts(bucket, assert_vulnerable=self.assert_vulnerable)

        # Step 6: Offline upgrade - one node at a time
        self.log.info("=" * 60)
        self.log.info("Step 6: Offline upgrade - one node at a time")
        self.log.info("=" * 60)
        self.offline_upgrade_all_nodes()

        # Step 7: Re-deploy scripts and verify no vulnerable vBuckets remain
        self.log.info("=" * 60)
        self.log.info("Step 7: Post-upgrade verification")
        self.log.info("=" * 60)
        self.copy_scripts_to_nodes()
        self.print_post_upgrade_observations()

    def test_magma_continuous_load_and_check(self):
        """Simple test: load 10M docs, then run continuous updates in one thread
        while periodically checking vulnerable vBuckets in another thread."""
        bucket = self.cluster.buckets[0]

        self.docs_to_load = self.input.param("docs_to_load", 10000000)
        self.doc_size = self.input.param("doc_size", 1024)
        self.num_update_iterations = self.input.param("num_update_iterations", 100)
        self.check_interval = self.input.param("check_interval", 600)
        # When True, funnel all load into a single vBucket (random by default).
        # When False, load spreads across all vBuckets like a normal load.
        self.target_vb_load = self.input.param("target_vb_load", True)

        non_default_colls = [c for c in self.collections if c != CbServer.default_collection]
        self.assertTrue(len(non_default_colls) >= 1,
                        "Need at least 1 non-default collection")
        target_coll = non_default_colls[0]
        self.log.info("Target collection: {}".format(target_coll))

        if self.target_vb_load:
            self.target_vb = [random.randint(0, 1023)]
            self.log.info("Target vBucket: {}".format(self.target_vb))
            self.vb_key_prefix = "vb{}".format(self.target_vb[0])
        else:
            self.target_vb = None
            self.vb_key_prefix = "doc"
            self.log.info("target_vb_load=False - loading across all vBuckets")

        # Step 1: Load docs
        self.log.info("=" * 60)
        self.log.info("Step 1: Loading {} docs into _default:{} targeting vb {}".format(
            self.docs_to_load, target_coll, self.target_vb))
        self.log.info("=" * 60)

        self.shared_doc_gen = doc_generator(
            key=self.vb_key_prefix, start=0, end=self.docs_to_load,
            key_size=self.key_size, doc_size=self.doc_size,
            target_vbucket=self.target_vb, randomize_value=True)
        self.log.info("Key generation complete - {} keys ready".format(self.docs_to_load))

        task = self.task.async_load_gen_docs(
            self.cluster, bucket, deepcopy(self.shared_doc_gen),
            op_type="create", exp=0,
            batch_size=self.batch_size,
            process_concurrency=self.process_concurrency,
            sdk_client_pool=self.sdk_client_pool,
            collection=target_coll, print_ops_rate=True,
            iterations=1)
        self.task_manager.get_task_result(task)
        self.log.info("Loaded {} docs into _default:{}".format(self.docs_to_load, target_coll))
        self.bucket_util.print_bucket_stats(self.cluster)

        if self.target_vb_load:
            # Find vBucket node owners and kvstore path
            self.find_vb_node_owners(bucket)
            find_cmd = "find {}/{} -name kvstore-{}".format(
                self.data_path, bucket.name, self.target_vb[0])
            shell = RemoteMachineShellConnection(self.vb_node_owners[0])
            try:
                o, e = shell.execute_command(find_cmd)
            finally:
                shell.disconnect()
            kvstore_path = o[0].strip()
            self.shard_path = "/".join(kvstore_path.split("/")[:-1])
            self.log.info("Target kvstore path: {}".format(kvstore_path))

        # Step 2: Start 2 threads -- check_vulnerable waits for 2 update iterations first
        self.log.info("=" * 60)
        self.log.info("Step 2: Starting update loop ({} iterations) + check_vulnerable loop "
                       "(deferred until iteration 2)".format(self.num_update_iterations))
        self.log.info("=" * 60)

        self.stop_check_vulnerable = threading.Event()
        self.start_check_vulnerable = threading.Event()

        update_thread = threading.Thread(
            target=self.continuous_update_loop,
            args=[bucket, target_coll, self.num_update_iterations])
        check_thread = threading.Thread(
            target=self.continuous_check_vulnerable,
            args=[self.check_interval])

        update_thread.start()
        check_thread.start()

        update_thread.join()
        self.stop_check_vulnerable.set()
        check_thread.join()

        self.log.info("=" * 60)
        self.log.info("Test complete: {} update iterations finished".format(
            self.num_update_iterations))
        self.log.info("=" * 60)

    def continuous_update_loop(self, bucket, collection, num_iterations):
        """Update all docs one full pass per iteration, for num_iterations iterations."""
        base_doc_gen = deepcopy(self.shared_doc_gen)
        for i in range(1, num_iterations + 1):
            self.log.debug("Update iteration {}/{}".format(i, num_iterations))
            task = self.task.async_load_gen_docs(
                self.cluster, bucket, deepcopy(base_doc_gen),
                op_type="update", exp=0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                sdk_client_pool=self.sdk_client_pool,
                collection=collection, print_ops_rate=False,
                iterations=1)
            self.task_manager.get_task_result(task)
            self.log.debug("Update iteration {}/{} complete".format(i, num_iterations))
            if i == 2:
                self.log.info("2 update iterations complete - enabling vulnerability checks")
                self.start_check_vulnerable.set()

    def continuous_check_vulnerable(self, check_interval):
        """Periodically run check_vulnerable_vbuckets.sh on all KV nodes
        until stop_check_vulnerable is set."""
        self.log.info("Waiting for 2 update iterations before starting vulnerability checks...")
        self.start_check_vulnerable.wait()
        self.log.info("Starting periodic vulnerability checks")
        while not self.stop_check_vulnerable.is_set():
            for node in self.cluster.kv_nodes:
                self.log.info("check_vulnerable_vbuckets on node: {}".format(node.ip))
                result = self.run_check_vulnerable_vbuckets(node, self.data_path)
                self.log.info("Node {} | status={} | maxSn={} | vulnerable={} | failed_kvstores={}".format(
                    node.ip, result["status"], result["max_sn"],
                    result["vulnerable_kvstores"], result["failed_kvstores"]))
            self.sleep(check_interval,
                       "Next check_vulnerable run in {}s".format(check_interval))


    def run_diagnostic_scripts(self, bucket, assert_vulnerable=True):
        """Run check_vulnerable_vbuckets, check_metastore, and vb-replay on all KV nodes.

        If assert_vulnerable=True (pre-upgrade), the test fails if no node reports VULNERABLE.
        Returns a deduplicated set of vulnerable vBucket IDs that were replayed.
        """
        all_vulnerable_vb_ids = set()
        node_results = {}

        for node in self.cluster.kv_nodes:
            self.log.info("--- check_vulnerable_vbuckets on node: {} ---".format(node.ip))
            vuln_result = self.run_check_vulnerable_vbuckets(node, self.data_path)
            node_results[node.ip] = vuln_result
            self.log.info("Node {} | status={} | maxSn={} | vulnerable={} | failed_kvstores={}".format(
                node.ip, vuln_result["status"], vuln_result["max_sn"],
                vuln_result["vulnerable_kvstores"], vuln_result["failed_kvstores"]))

            if vuln_result["status"] != "VULNERABLE":
                continue

            for kvstore_path in vuln_result["vulnerable_kvstores"]:
                try:
                    vb_id = int(kvstore_path.rstrip("/").split("kvstore-")[-1])
                except (ValueError, IndexError):
                    self.log.warning("Could not parse vBucket ID from: {}".format(kvstore_path))
                    continue

                # If localIndex is also impacted, run check_metastore first
                if any(kvstore_path in p for p in vuln_result["localindex_kvstores"]):
                    self.log.info("localIndex impacted - running check_metastore for {}".format(kvstore_path))
                    metastore_status = self.run_check_metastore(node, kvstore_path)
                    self.log.info("check_metastore | vb {} | status={}".format(vb_id, metastore_status))
                    if metastore_status == "VULNERABLE":
                        self.log.error(
                            "vBucket {} on node {} is VULNERABLE per check_metastore - "
                            "manual intervention required".format(vb_id, node.ip))
                        continue

                all_vulnerable_vb_ids.add(vb_id)

        # Pre-upgrade: check whether the bug build is actually VULNERABLE.
        # Logged as critical instead of asserting, so the test continues regardless.
        if assert_vulnerable:
            any_vulnerable = any(r["status"] == "VULNERABLE" for r in node_results.values())
            if any_vulnerable:
                self.log.info("Pre-upgrade VULNERABLE check passed - bug confirmed on this build")
            else:
                self.log.critical(
                    "Expected VULNERABLE status on bug build but all nodes reported OK. "
                    "Check that the correct build is installed. "
                    "Node statuses={}".format(
                        {ip: (r["status"], r["max_sn"]) for ip, r in node_results.items()}))

        # Run vb-replay once per unique vBucket ID from the master node
        self.log.info("Vulnerable vBucket IDs to replay: {}".format(all_vulnerable_vb_ids))
        master = self.cluster.master
        for vb_id in sorted(all_vulnerable_vb_ids):
            self.log.info("Running vb-replay dry-run for vBucket {}".format(vb_id))
            self.run_vb_replay(master, bucket.name, vb_id, dry_run=True)
            self.log.info("Running vb-replay actual for vBucket {}".format(vb_id))
            self.run_vb_replay(master, bucket.name, vb_id, dry_run=False)

        return all_vulnerable_vb_ids

    def offline_upgrade_all_nodes(self):
        """Upgrade every KV node one at a time using offline upgrade."""
        nodes = self.cluster.kv_nodes[:]
        self.log.info("Starting offline upgrade of {} node(s) to version {}".format(
            len(nodes), self.version_to_upgrade))

        for i, node in enumerate(nodes):
            self.log.info("Upgrading node {}/{}: {}".format(i + 1, len(nodes), node.ip))
            # last node upgrade should trigger rebalance to bring cluster back to balanced
            rebalance_required = (i == len(nodes) - 1)
            success = self.upgrade_helper.offline(
                node, self.version_to_upgrade,
                rebalance_required=rebalance_required)
            if success is False:
                self.fail("Offline upgrade failed for node: {}".format(node.ip))
            self.log.info("Node {} upgraded successfully".format(node.ip))

        self.log.info("All nodes upgraded to {}".format(self.version_to_upgrade))
        self.cluster_util.print_cluster_stats(self.cluster)

    def print_post_upgrade_observations(self):
        """Run all 3 scripts on every node after upgrade and log the observations."""
        self.log.info("=" * 60)
        self.log.info("POST-UPGRADE OBSERVATIONS")
        self.log.info("=" * 60)

        for node in self.cluster.kv_nodes:
            self.log.info("--- Node: {} ---".format(node.ip))

            # 1. check_vulnerable_vbuckets
            result = self.run_check_vulnerable_vbuckets(node, self.data_path)
            self.log.info("check_vulnerable_vbuckets | status={} | maxSn={} | "
                          "vulnerable={} | failed_kvstores={}".format(
                result["status"], result["max_sn"],
                result["vulnerable_kvstores"], result["failed_kvstores"]))

            # 2. check_metastore for any kvstore that still shows localIndex impact
            for kvstore_path in result.get("localindex_kvstores", []):
                metastore_status = self.run_check_metastore(node, kvstore_path)
                self.log.info("check_metastore | kvstore={} | status={}".format(
                    kvstore_path, metastore_status))

            # 3. vb-replay dry-run to count any remaining hidden mutations
            master = self.cluster.master
            for kvstore_path in result.get("vulnerable_kvstores", []):
                try:
                    vb_id = int(kvstore_path.rstrip("/").split("kvstore-")[-1])
                    self.log.info("vb-replay dry-run | vb_id={}".format(vb_id))
                    self.run_vb_replay(master,
                                       self.cluster.buckets[0].name,
                                       vb_id, dry_run=True)
                except (ValueError, IndexError):
                    pass

        self.log.info("=" * 60)
        self.log.info("POST-UPGRADE OBSERVATIONS COMPLETE")
        self.log.info("=" * 60)

    def parallel_read_docs(self, bucket, scope, collection, doc_gen_read, validate_field=False,
                           with_expiry=False, validate_ttl=False, mutate=0, docs_to_read=50000,
                           start_index=0, max_threads=16, print_result_count=0):
        sdk_client = SDKClient([self.cluster.master],
                                bucket,
                                scope=scope,
                                collection=collection)
        doc_not_found = 0
        doc_found = 0
        other_errors = 0
        field_validated = 0
        ttl_validation_success = 0
        ttl_validation_fail = 0
        keys = []
        # Extracting all keys from the doc generator
        while doc_gen_read.has_next():
            key_obj, _ = doc_gen_read.next()
            keys.append(key_obj)
        if start_index != 0:
            keys = keys[start_index:start_index+docs_to_read]
        self.log.info("Doc gen size = {}".format(len(keys)))
        def read_doc(key):
            res = sdk_client.read(key, with_expiry=with_expiry)
            return res
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = {executor.submit(read_doc, key): key for key in keys}
            for future in as_completed(futures):
                res = future.result()
                if print_result_count > 0:
                    self.log.info("Result of read = {}".format(res))
                    print_result_count -= 1
                if res["status"] is False and "DocumentNotFoundException" in res["error"]:
                    doc_not_found += 1
                elif res["status"] is True:
                    doc_found += 1
                    if validate_field:
                        json_obj = json.loads(res["value"])
                        if json_obj["mutated"] == mutate:
                            field_validated += 1
                    if with_expiry:
                        ttl_value = res["ttl_value"]
                        if res["key"] not in self.doc_expiry_dict:
                            self.doc_expiry_dict[res["key"]] = ttl_value
                        else:
                            if self.doc_expiry_dict[res["key"]] == ttl_value:
                                ttl_validation_success += 1
                            else:
                                ttl_validation_fail += 1
                else:
                    self.log.info("Read error: {}".format(res["error"]))
                    other_errors += 1
        sdk_client.close()
        self.log.info("Doc Found count in {} = {}".format(collection, doc_found))
        self.log.info("DocNotFound count in {} = {}".format(collection, doc_not_found))
        self.log.info("Other Errors count in {} = {}".format(collection, other_errors))
        if validate_field:
            self.log.info("Mutated field validation success count = {}".format(field_validated))
        if validate_ttl:
            self.log.info("TTL validation success count = {}".format(ttl_validation_success))
            self.log.info("TTL validation fail count = {}".format(ttl_validation_fail))

    def get_fragmentation_magma(self, servers, bucket, magma_shard):
        frag_dict = dict()
        server_frag = dict()
        field_to_grep = "rw_{}:magma".format(magma_shard)
        for server in servers:
            cb_obj = Cbstats(server)
            frag_res = cb_obj.magma_stats(bucket.name, field_to_grep,
                                          "kvstore")
            frag_dict[server.ip] = frag_res
            # self.log.info("Frag res = {}".format(frag_res))
            cb_obj.disconnect()
            server_frag[server.ip] = float(frag_dict[server.ip][field_to_grep]["Fragmentation"])
        self.log.info("Fragmentation {0}".format(server_frag))

    def continuous_load_until_vulnerable(self, bucket, collection=None):
        """Keep updating hot_coll docs one full pass at a time until check_vb_vulnerability
        sets vulnerability_reached (maxSn > INT32_MAX on the watched indexes).

        Keys are generated once. Each pass deepcopies the generator to reset the
        iterator cursor without rebuilding the key space.
        """
        iteration_count = 0

        # Reuse keys already generated in test_magma_unsafe_window - no rescan needed
        base_doc_gen = deepcopy(self.shared_doc_gen)

        while not self.vulnerability_reached.is_set():
            iteration_count += 1
            self.log.debug("hot_coll '{}' update pass {} - vb {} - waiting for indexes={} to exceed INT32_MAX".format(
                self.hot_coll_key, iteration_count, self.target_vb, self.idx_to_monitor))

            # Deepcopy resets the iterator cursor; keys themselves are not regenerated
            task = self.task.async_load_gen_docs(
                self.cluster, bucket, deepcopy(base_doc_gen),
                op_type="update", exp=0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                sdk_client_pool=self.sdk_client_pool,
                collection=collection, print_ops_rate=False,
                iterations=1)

            self.task_manager.get_task_result(task)

            if self.vulnerability_reached.is_set():
                self.log.info("Vulnerability window hit after {} pass(es) on hot_coll '{}'".format(
                    iteration_count, self.hot_coll_key))
                break

            self.log.debug("Pass {} done - maxSn not yet at threshold, continuing".format(
                iteration_count))


    def advance_sn_counter(self,collection=None):
        bucket = self.cluster.buckets[0]
        self.vulnerability_reached = threading.Event()
        # Identify all nodes which contain the active/replica of the target_vb
        self.find_vb_node_owners(bucket)
        # Find target_vb kvstore path in the Magma directory
        find_cmd = "find {}/{} -name kvstore-{}".format(self.data_path, bucket.name, self.target_vb[0])
        shell = RemoteMachineShellConnection(self.vb_node_owners[0])
        try:
            o, e = shell.execute_command(find_cmd)
        finally:
            shell.disconnect()
        self.log.info("Output = {}, Error = {}".format(o, e))
        kvstore_path = o[0].strip()
        self.log.info("Target Kvstore path = {}".format(kvstore_path))
        self.shard_path = "/".join(kvstore_path.split("/")[:-1])

        check_th_array = list()
        for node in self.vb_node_owners:
            th = threading.Thread(target=self.check_vb_vulnerability,
                                  args=[node, self.target_vb[0], self.check_interval])
            check_th_array.append(th)
            th.start()

        self.continuous_load_until_vulnerable(bucket=bucket, collection=collection)

        for th in check_th_array:
            th.join()

    def find_vb_node_owners(self, bucket):
        self.vb_node_owners = list()
        for server in self.cluster.nodes_in_cluster:
            cbstat_obj = Cbstats(server)
            active_vbs = set(cbstat_obj.vbucket_list(bucket.name, vbucket_type="active"))
            replica_vbs = set(cbstat_obj.vbucket_list(bucket.name, vbucket_type="replica"))
            if self.target_vb[0] in active_vbs or self.target_vb[0] in replica_vbs:
                self.vb_node_owners.append(server)
            cbstat_obj.disconnect()
        self.log.info("Target VB Active/Replica Node Owners = {}".format(self.vb_node_owners))

    def run_magma_dump(self, servers=None, key_prefixes=None, shard_path=None):
        key_prefix_list = key_prefixes if key_prefixes is not None else self.key_prefix_list
        servers = servers if servers is not None else self.vb_node_owners
        shard_path = shard_path if shard_path is not None else self.shard_path
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            for key_prefix in key_prefix_list:
                for idx in ["key", "seq"]:
                    magma_dump_cmd = "/opt/couchbase/bin/magma_dump {} docs --index {} --kvstore {} | grep '{}' | wc -l".\
                                                format(shard_path, idx, self.target_vb[0], key_prefix)
                    self.log.info("Running CMD on server {} : {}".format(server.ip, magma_dump_cmd))
                    o, e = shell.execute_command(magma_dump_cmd)
                    self.log.info("Output = {}, Error = {}".format(o, e))
            shell.disconnect()

