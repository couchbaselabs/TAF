
import os
import random
import threading
from cb_constants.CBServer import CbServer
from cb_tools.cbstats import Cbstats
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from magma_base import MagmaBaseTest
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from upgrade_utils.upgrade_util import CbServerUpgrade

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
        # upgrade=False: run on the installed build (already has the fix)
        # and skip the offline upgrade step
        self.upgrade = self.input.param("upgrade", True)
        self.version_to_upgrade = self.input.param("version_to_upgrade", "7.6.7-6700")
        self.upgrade_type = self.input.param("upgrade_type", "failover_recovery")
        self.upgrade_helper = CbServerUpgrade(self.log, self.product)
        # Sirius Java SDK ops/sec for loads, update passes and reads
        self.ops = self.input.param("ops", 50000)
        self.check_interval = self.input.param("check_interval", 600)
        # couchbase-vb-replay binary on the machine running TAF;
        # defaults to the copy in scripts/, override with vb_replay_path
        self.vb_replay_path = self.input.param("vb_replay_path", _LOCAL_VB_REPLAY)
        if not os.path.isfile(self.vb_replay_path):
            self.fail("vb-replay binary not found at {}. Place it there or "
                      "pass -p vb_replay_path=<path to couchbase-vb-replay>"
                      .format(self.vb_replay_path))
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
            (self.vb_replay_path,     _REMOTE_VB_REPLAY),
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
            # Vulnerable summary entry:
            #   "Bucket: <name>, <kvstore_path> (maxSn: N[, localIndexMaxSn: M])"
            # The METADATA CONSISTENCY section repeats localIndex ones without
            # the comma ("Bucket: <name> <kvstore_path>"), so they are skipped here.
            elif line.startswith("Bucket:") and "kvstore-" in line:
                tokens = line.split(",")
                if len(tokens) >= 2:
                    kvstore_path = tokens[1].strip().split()[0]
                    result["vulnerable_kvstores"].append(kvstore_path)
                    if "localIndexMaxSn" in line:
                        result["localindex_kvstores"].append(kvstore_path)
            # Capture failure details (e.g. "kvstore-5: FAILED to run magma_dump")
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
            self.log.info("Server {}: running check_metastore for {}".format(
                server.ip, kvstore_path))
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

        Returns (stdout_lines, stderr_lines, exit_code).
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
        self.log.info("Server {}: running vb-replay for vbucket {}{}".format(
            server.ip, vbucket_id, " (dry-run)" if dry_run else ""))
        shell = RemoteMachineShellConnection(server)
        try:
            o, e, exit_code = shell.execute_command(cmd, timeout=3600,
                                                    get_exit_code=True)
        finally:
            shell.disconnect()
        self.log.info("vb-replay exit={} output: {}".format(exit_code, o))
        if e:
            self.log.warning("vb-replay stderr: {}".format(e))
        return o, e, exit_code

    def check_vb_vulnerability(self, server, target_vb, check_interval=600):
        """Poll magma_dump every check_interval seconds and set vulnerability_reached
        when all indexes in self.idx_to_monitor exceed vul_threshold."""
        int32_max = self.vul_threshold
        indexes_to_watch = self.idx_to_monitor.split(":")
        self.log.info("Server {} | monitoring indexes={} | threshold={} | interval={}s".format(
            server.ip, indexes_to_watch, int32_max, check_interval))

        shell = RemoteMachineShellConnection(server)
        idx_sn_dict = dict()

        # stop_monitor.wait() instead of self.sleep() so the thread exits as
        # soon as the load loop stops (window hit or loader error)
        while not self.vulnerability_reached.is_set() \
                and not self.stop_monitor.is_set():
            magma_dump_cmd = (
                "/opt/couchbase/bin/magma_dump {} tree-state --latest --kvstore {} "
                "| grep maxSn | tr -d ' ,' | cut -d: -f2"
            ).format(self.shard_path, target_vb)

            try:
                o, e = shell.execute_command(magma_dump_cmd)
                self.log.info("Server {} | magma_dump output={} error={}".format(server.ip, o, e))

                if not o or len(o) < 3:
                    self.log.warning("Server {} | incomplete magma_dump output: {}".format(server.ip, o))
                    self.stop_monitor.wait(check_interval)
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
                self.stop_monitor.wait(check_interval)
                continue

            self.log.info("Server {} | next vulnerability check in {}s".format(
                server.ip, check_interval))
            self.stop_monitor.wait(check_interval)

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

        # Select a random target vBucket - all docs in all 3 collections go here.
        # Range comes from the bucket actually created (bucket_num_vb / vbuckets)
        self.num_vbs = bucket.numVBuckets or self.bucket_num_vb
        self.target_vb = [random.randint(0, self.num_vbs - 1)]
        self.log.info("Target vBucket: {} (bucket has {} vBuckets)".format(
            self.target_vb, self.num_vbs))

        # Step 1: Load docs into cold, hot, warm - keys generated ONCE and shared across all collections
        self.log.info("=" * 60)
        self.log.info("Step 1: Loading {} docs into each collection targeting vb {}".format(
            self.docs_to_load, self.target_vb))
        self.log.info("=" * 60)
        # Same SimpleKey names ('vb<N>' + padding + index) in every collection,
        # since collections are separate namespaces. Sirius picks indexes
        # whose keys hash to the target vBucket
        self.vb_key_prefix = "vb{}".format(self.target_vb[0])
        self.collections_to_check = [cold_coll, hot_coll, warm_coll]
        # A negative SimpleKey padding kills the Sirius task without writing
        min_key_size = len(self.vb_key_prefix) + len(str(max(1, self.docs_to_load - 1)))
        self.assertTrue(self.key_size >= min_key_size,
                        "key_size={} too small for prefix '{}' and {} docs, need >= {}".format(
                            self.key_size, self.vb_key_prefix, self.docs_to_load, min_key_size))

        for coll in self.collections_to_check:
            self.log.info("Loading {} docs into _default:{} with key prefix '{}'".format(
                self.docs_to_load, coll, self.vb_key_prefix))
            ok, _ = self.run_sirius_task(bucket, coll, create_percent=100,
                                         create_start_index=0,
                                         create_end_index=self.docs_to_load)
            self.assertTrue(ok, "Initial load into _default:{} failed".format(coll))
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

        # Let writes flush to disk
        self.sleep(30, "Wait after overflow updates")
        self.sleep(120, "Wait for writes to flush and persist to disk")

        # Step 3: Observe the bug - force warm-up from disk and read all 3
        self.log.info("=" * 60)
        self.log.info("Step 3: Exposing the bug - sigkill memcached and reading all collections")
        self.log.info("=" * 60)
        self.get_fragmentation_magma(self.vb_node_owners, bucket,
                                     magma_shard=self.shard_path.rsplit(".", 1)[-1])
        self.sleep(30, "Wait before killing memcached")
        self.sigkill_memcached(nodes=self.vb_node_owners)

        if self.upgrade:
            # Bug build: missing docs here are the expected MB-66959 symptom
            for coll in self.collections_to_check:
                self.read_docs(bucket, coll)
        else:
            # Build with the fix: every doc must survive the overflow + warmup
            self.verify_all_docs_readable(bucket, "post-warmup")

        self.get_fragmentation_magma(self.vb_node_owners, bucket,
                                     magma_shard=self.shard_path.rsplit(".", 1)[-1])

        # Steps 4 onwards follow the MB-67762 resolution procedure in order:
        # compaction -> diagnostics -> upgrade -> swap rebalance -> vb-replay
        # Step 4: Compaction + magma_dump. check_metastore.sh is only valid
        # once compaction has completed (MB-67762), so it is mandatory here
        self.log.info("=" * 60)
        self.log.info("Step 4: Bucket compaction and magma_dump")
        self.log.info("=" * 60)
        # all 3 collections share the same key prefix
        self.key_prefix_list = [self.vb_key_prefix]
        self.run_magma_dump()
        self.compact_and_wait(bucket)
        self.run_magma_dump()
        self.get_fragmentation_magma(self.vb_node_owners, bucket,
                                     magma_shard=self.shard_path.rsplit(".", 1)[-1])

        # Step 5: Diagnostic scripts on the bug build
        self.log.info("=" * 60)
        self.log.info("Step 5: Running diagnostic scripts on all nodes before upgrade")
        self.log.info("=" * 60)
        vulnerable_vb_ids, vulnerable_nodes = self.run_diagnostic_scripts(
            assert_vulnerable=self.assert_vulnerable)

        # Step 6: Offline upgrade - one node at a time. MB-67762 allows
        # skipping it when the cluster already runs a build with the fix
        self.log.info("=" * 60)
        self.log.info("Step 6: Offline upgrade - one node at a time")
        self.log.info("=" * 60)
        if self.upgrade:
            self.offline_upgrade_all_nodes()
            self.copy_scripts_to_nodes()
        else:
            self.log.info("upgrade=False - cluster already on the build under test")

        # Step 7: Previously hidden documents must be readable after upgrade
        self.log.info("=" * 60)
        self.log.info("Step 7: Verify all docs are readable post upgrade")
        self.log.info("=" * 60)
        self.verify_all_docs_readable(bucket, "post-upgrade")

        # Step 8: Swap rebalance of the vulnerable nodes resets the maxSn
        # counter, so no vBucket may be reported vulnerable afterwards
        self.log.info("=" * 60)
        self.log.info("Step 8: Rebalance vulnerable nodes and re-check")
        self.log.info("=" * 60)
        self.rebalance_out_in_nodes(vulnerable_nodes)
        for node in self.cluster.kv_nodes:
            result = self.run_check_vulnerable_vbuckets(node, self.data_path)
            self.assertEqual(
                result["status"], "OK",
                "Node {} still reports status={} (maxSn={}, vulnerable={}, "
                "failed_kvstores={}) after upgrade + rebalance".format(
                    node.ip, result["status"], result["max_sn"],
                    result["vulnerable_kvstores"], result["failed_kvstores"]))

        # Step 9: No GSI indexes are created by this test, so the MB-67762
        # "rebuild indexes" step does not apply. Replay hidden mutations
        # once per vulnerable vBucket
        self.log.info("=" * 60)
        self.log.info("Step 9: vb-replay for vBuckets {}".format(
            sorted(vulnerable_vb_ids)))
        self.log.info("=" * 60)
        for vb_id in sorted(vulnerable_vb_ids):
            for dry_run in (True, False):
                _, e, exit_code = self.run_vb_replay(
                    self.cluster.master, bucket.name, vb_id, dry_run=dry_run)
                self.assertEqual(
                    exit_code, 0,
                    "vb-replay{} for vBucket {} exited with {}: {}".format(
                        " --dry-run" if dry_run else "", vb_id, exit_code, e))

        # Step 10: Data must still be fully readable at the end
        self.log.info("=" * 60)
        self.log.info("Step 10: Final read-back of all docs")
        self.log.info("=" * 60)
        self.verify_all_docs_readable(bucket, "final")


    def run_diagnostic_scripts(self, assert_vulnerable=True):
        """Run check_vulnerable_vbuckets (and check_metastore where localIndex
        is impacted) on all KV nodes. Must run after compaction has completed.

        If assert_vulnerable=True (pre-upgrade), the test fails if no node reports VULNERABLE.
        Returns (set of vulnerable vBucket IDs, list of nodes that reported VULNERABLE).
        """
        all_vulnerable_vb_ids = set()
        vulnerable_nodes = list()
        node_results = {}

        for node in self.cluster.kv_nodes:
            self.log.info("--- check_vulnerable_vbuckets on node: {} ---".format(node.ip))
            vuln_result = self.run_check_vulnerable_vbuckets(node, self.data_path)
            node_results[node.ip] = vuln_result
            self.log.info("Node {} | status={} | maxSn={} | vulnerable_kvstores={} | "
                          "failed_kvstores={}".format(
                node.ip, vuln_result["status"], vuln_result["max_sn"],
                vuln_result["vulnerable_kvstores"], vuln_result["failed_kvstores"]))

            if vuln_result["status"] != "VULNERABLE":
                continue
            vulnerable_nodes.append(node)

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
                    # MB-67762: metastore-vulnerable vBuckets need the
                    # Advanced Recovery procedure, which this test does not cover
                    if metastore_status != "OK":
                        self.fail("check_metastore reported {} for vBucket {} on "
                                  "node {}; standard MB-67762 procedure does not "
                                  "apply".format(metastore_status, vb_id, node.ip))

                all_vulnerable_vb_ids.add(vb_id)

        # Pre-upgrade: assert that the bug build is actually VULNERABLE
        if assert_vulnerable:
            any_vulnerable = any(r["status"] == "VULNERABLE" for r in node_results.values())
            self.assertTrue(any_vulnerable,
                            "Expected VULNERABLE status on bug build but all nodes reported OK. "
                            "Check that the correct build is installed.")
            self.log.info("Pre-upgrade VULNERABLE check passed - bug confirmed on this build")

        self.log.info("Vulnerable vBucket IDs: {}, nodes: {}".format(
            sorted(all_vulnerable_vb_ids), [n.ip for n in vulnerable_nodes]))
        return all_vulnerable_vb_ids, vulnerable_nodes

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

        # offline() returns None both on success and on a failed rebalance,
        # so verify the outcome directly
        for node in nodes:
            version = RestConnection(node).get_nodes_self().version
            self.assertTrue(version.startswith(self.version_to_upgrade),
                            "Node {} is on {} after upgrade, expected {}".format(
                                node.ip, version, self.version_to_upgrade))
        self.assertTrue(RestConnection(self.cluster.master).cluster_status()["balanced"],
                        "Cluster not balanced after offline upgrade")
        self.log.info("All nodes upgraded to {}".format(self.version_to_upgrade))
        self.cluster_util.print_cluster_stats(self.cluster)

    def compact_and_wait(self, bucket):
        """Compact the bucket and confirm it has really finished.
        CompactBucketTask can return True before compaction completes."""
        self.bucket_util._run_compaction(self.cluster, number_of_times=1)
        self.assertTrue(
            self.bucket_util.wait_till_compaction_end(
                self.cluster.master, bucket.name, timeout_in_seconds=1800),
            "Compaction of {} did not complete".format(bucket.name))

    def rebalance_out_in_nodes(self, nodes):
        """Stand-in for the MB-67762 swap rebalance without needing spare
        upgraded nodes: rebalancing a node out and back in moves (and so
        rebuilds) its vBuckets, which resets their maxSn counter."""
        for node in nodes:
            for to_add, to_remove in (([], [node]), ([node], [])):
                self.assertTrue(
                    self.task.rebalance(self.cluster, to_add, to_remove,
                                        check_vbucket_shuffling=False),
                    "Rebalance (add={}, remove={}) failed".format(
                        [n.ip for n in to_add], [n.ip for n in to_remove]))
        self.cluster_util.update_cluster_nodes_service_list(self.cluster)

    def verify_all_docs_readable(self, bucket, stage):
        """Fail unless every loaded key in every collection is readable."""
        failures = dict()
        for coll in self.collections_to_check:
            res = self.read_docs(bucket, coll)
            if res["found"] != self.docs_to_load:
                failures[coll] = res
        self.assertFalse(failures, "{}: not all {} docs readable per collection: {}"
                         .format(stage, self.docs_to_load, failures))

    def run_sirius_task(self, bucket, collection, **op_params):
        """Run one Sirius Java SDK task on the target vBucket keys
        ('vb<N>' SimpleKey) and wait for it. Returns (ok, loader)."""
        loader = SiriusCouchbaseLoader(
            server_ip=self.cluster.master.ip,
            server_port=self.cluster.master.port,
            username=self.cluster.master.rest_username,
            password=self.cluster.master.rest_password,
            bucket=bucket,
            scope_name=CbServer.default_scope,
            collection_name=collection,
            key_prefix=self.vb_key_prefix,
            key_size=self.key_size,
            doc_size=self.doc_size,
            key_type="SimpleKey",
            ops=self.ops,
            process_concurrency=self.process_concurrency,
            num_vbuckets=self.num_vbs,
            target_vbuckets=self.target_vb,
            **op_params)
        loader.create_doc_load_task()
        self.task_manager.add_new_task(loader)
        return self.task_manager.get_task_result(loader), loader

    def read_docs(self, bucket, collection):
        """Read every loaded key in the collection via Sirius and count
        found / not-found / other errors from the tracked failures."""
        _, loader = self.run_sirius_task(bucket, collection, read_percent=100,
                                         read_start_index=0,
                                         read_end_index=self.docs_to_load,
                                         track_failures=True)
        not_found = sum(1 for err in loader.fail.values()
                        if "DocumentNotFoundException" in str(err.get("error", "")))
        res = {"found": self.docs_to_load - len(loader.fail),
               "not_found": not_found,
               "other_errors": len(loader.fail) - not_found}
        self.log.info("Read _default:{}: {}".format(collection, res))
        return res

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

        One Sirius task per pass with SimpleKey: a multi-iteration task would
        switch to CircularKey, whose keys differ from the loaded ones.
        """
        iteration_count = 0

        while not self.vulnerability_reached.is_set():
            iteration_count += 1
            self.log.debug("hot_coll '{}' update pass {} - vb {} - waiting for indexes={} to exceed INT32_MAX".format(
                self.hot_coll_key, iteration_count, self.target_vb, self.idx_to_monitor))

            ok, _ = self.run_sirius_task(bucket, collection, update_percent=100,
                                         update_start_index=0,
                                         update_end_index=self.docs_to_load)
            self.assertTrue(ok, "Update pass {} on _default:{} failed".format(
                iteration_count, collection))

            if self.vulnerability_reached.is_set():
                self.log.info("Vulnerability window hit after {} pass(es) on hot_coll '{}'".format(
                    iteration_count, self.hot_coll_key))
                break

            self.log.debug("Pass {} done - maxSn not yet at threshold, continuing".format(
                iteration_count))


    def advance_sn_counter(self,collection=None):
        bucket = self.cluster.buckets[0]
        self.vulnerability_reached = threading.Event()
        self.stop_monitor = threading.Event()
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

        # finally: monitor threads are non-daemon, so a loader exception
        # would otherwise leave them polling forever and hang the run
        try:
            self.continuous_load_until_vulnerable(bucket=bucket, collection=collection)
        finally:
            self.stop_monitor.set()
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

