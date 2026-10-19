import time

from BucketLib.bucket import TravelSample, BeerSample, GamesimSample, Bucket
from cb_constants import DocLoading
from cb_server_rest_util.buckets.buckets_api import BucketRestApi
from cb_tools.cbstats import Cbstats
from cluster_utils.cluster_ready_functions import CBCluster
from couchbase_helper.documentgenerator import doc_generator
from membase.api.rest_client import RestConnection
from platform_constants.os_constants import Linux
from sdk_client3 import SDKClient
from shell_util.remote_connection import RemoteMachineShellConnection
from TestInput import TestInputSingleton
from upgrade.kv_upgrade_checks import KVUpgradeChecks, RebalanceStallMonitor
from upgrade.upgrade_base import UpgradeBase


class KVUpgradeTests(UpgradeBase, KVUpgradeChecks):
    def setUp(self):
        # UpgradeBase.setUp takes servers[nodes_init] as the spare node only
        # after the initial install (~5 mins in). Fail fast instead
        test_input = TestInputSingleton.input
        nodes_init = test_input.param("nodes_init", 1)
        if len(test_input.servers) <= nodes_init:
            self.fail(f"nodes_init={nodes_init} needs at least "
                      f"{nodes_init + 1} servers in the ini (one spare), "
                      f"got {len(test_input.servers)}")
        super(KVUpgradeTests, self).setUp()
        self.log_setup_status("KVUpgradeTests", "started", "setup")
        self.nodes_upgrade = self.input.param("nodes_upgrade", 1)
        self.graceful = self.input.param("graceful", True)
        self.recovery_type = self.input.param("recovery_type", "delta")
        self.init_kv_upgrade_check_params()
        # Mixed-mode availability (default: half of the KV nodes)
        self.mixed_mode_upgraded_nodes = \
            self.input.param("mixed_mode_upgraded_nodes", None)
        self.mixed_mode_hold_secs = \
            self.input.param("mixed_mode_hold_secs", 300)
        # Rebalance-under-DCP-stress
        self.rebalance_stall_secs = \
            self.input.param("rebalance_stall_secs", 600)
        self.rebalance_step_timeout = \
            self.input.param("rebalance_step_timeout", 3600)
        self.dcp_drain_timeout = self.input.param("dcp_drain_timeout", 600)
        # Set for the new-OS-image / new-kernel variant, so the run proves
        # the incoming nodes really were on the new kernel (e.g. "6.17")
        self.expected_kernel_prefix = \
            self.input.param("expected_kernel_prefix", None)
        # Set by tests that leave the cluster mid-upgrade, so tearDown knows
        # to chase bucket deletes across every node
        self.delete_buckets_on_all_nodes = False
        self.log_setup_status("KVUpgradeTests", "completed", "setup")

    def tearDown(self):
        # getattr, not attribute access: a setUp that raises before the flag
        # is set still runs tearDown, and an AttributeError here would mask
        # the real setUp failure
        if getattr(self, "delete_buckets_on_all_nodes", False):
            self.__delete_buckets_on_all_nodes()
        super(KVUpgradeTests, self).tearDown()

    def __delete_buckets_on_all_nodes(self):
        """
        Retry the delete on every node — after a partial upgrade the master
        may not be the node that still answers for the bucket.
        """
        bucket_names = [b.name for b in
                        list(getattr(self.cluster, "buckets", []) or [])]
        nodes = list(getattr(self.cluster, "nodes_in_cluster", []) or [])
        self.log.info(
            f"Upgrade tearDown: buckets={bucket_names}, "
            f"nodes={[n.ip for n in nodes]}")
        for name in bucket_names:
            for attempt in range(6):
                done = False
                for node in nodes:
                    try:
                        status, content = BucketRestApi(node).delete_bucket(
                            name)
                        self.log.info(
                            f"Delete {name} on {node.ip} attempt "
                            f"{attempt}: status={status}, content={content}")
                        if status:
                            done = True
                            break
                    except Exception as e:
                        self.log.warning(
                            f"Delete {name} on {node.ip} raised: {e}")
                if done:
                    break
                time.sleep(10)

    def test_multiple_sample_bucket_failover_upgrade(self):
        '''
            1.Formed the cluster using the 3 nodes.
            2.Loaded all 3 sample buckets.
            3.Failed over node.
            4.Stopped Couchbase Service on node.
            5.Upgraded node to upgrade_version.
            6.Tried to perform recovery
            Ref - MB-53493

        '''
        self.assertTrue(len(self.cluster.nodes_in_cluster) > 1,
                        msg="Not enough nodes to failover and upgrade")

        self.assertTrue(self.nodes_upgrade <= len(self.cluster.nodes_in_cluster),
                        msg="The number of nodes specified for upgrade are more than number of nodes in cluster")

        #Loading Travel Sample Bucket
        travelSampleBucket = TravelSample()
        if float(self.upgrade_chain[0][:3]) < 7.0:
            travelSampleBucket.stats.expected_item_count = 31591
        load_success=self.bucket_util.load_sample_bucket(self.cluster, travelSampleBucket)
        self.assertTrue(load_success,
                        msg="Travel Sample Bucket could not be loaded")
        self.log.info("Travel Sample Bucket Loaded")

        #Loading Beer Sample Bucket
        load_success=self.bucket_util.load_sample_bucket(self.cluster, BeerSample())
        self.assertTrue(load_success,
                        msg = "Beer Sample Bucket could not be loaded")
        self.log.info("Beer Sample Bucket Loaded")

        #Loading Gamesim Sample Bucket
        load_success=self.bucket_util.load_sample_bucket(self.cluster, GamesimSample())
        self.assertTrue(load_success,
                        msg = "Gamesim Sample Bucket could not be loaded")
        self.log.info("Gamesim Sample Bucket Loaded")

        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        node_to_upgrades = self.cluster.nodes_in_cluster[-self.nodes_upgrade:]

        for upgrade_version in self.upgrade_chain[1:]:
            self.upgrade_version = upgrade_version
            for node_to_upgrade in node_to_upgrades:
                self.sleep(120, "Sleeping before starting the next failover upgrade")
                self.failover_recovery(node_to_upgrade=node_to_upgrade,
                                       recovery_type=self.recovery_type,
                                       graceful=self.graceful)
                self.cluster_util.print_cluster_stats(self.cluster)
                self.bucket_util.print_bucket_stats(self.cluster)

    def test_db_dump_with_empty_body_and_empty_xattr(self):
        """
        1. Create empty doc (with no body + no sys_xattr) with data_type=xattr
        2. Stop and perform offline upgrade to some version (broken)
        3. Start xdcr replication to other cluster

        Ref: MB-51373
        """

        # Chosing the latest build for upgrades
        # This is a very specific test case and chain upgrades are not required
        self.upgrade_version = self.upgrade_chain[-1]
        # Install Couchbase server on target_nodes
        self.upgrade_helper.new_install_version_on_all_nodes(
            nodes=self.cluster.servers[self.nodes_init:],
            version=self.upgrade_version)

        upgrade_cluster = self.input.param("upgrade_cluster", "source")
        key, val = "test_key", {"f": "value"}
        sub_doc = ["_key", "value"]
        bucket = self.cluster.buckets[0]
        key_vb = self.bucket_util.get_vbucket_num_for_key(key,
                                                          bucket.numVBuckets)
        in_node = self.cluster.servers[1]
        num_items = self.num_items

        # Install the initial version on the 2nd node as well
        # This was not done initially
        self.upgrade_helper.new_install_version_on_all_nodes(
            nodes=self.cluster.servers[1:2], version=self.upgrade_chain[0])

        client = SDKClient(self.cluster, bucket)

        self.log.info("Creating tombstone '%s' with sys-xattr" % key)
        # Create a document
        client.crud(DocLoading.Bucket.DocOps.CREATE, key, val)
        # Load sys-xattr for the document
        client.crud(DocLoading.Bucket.SubDocOps.INSERT,
                    key, sub_doc, xattr=True)
        # Wait for ep_queue_size to become Zero
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, [bucket])
        # Delete the document
        client.crud(DocLoading.Bucket.DocOps.DELETE, key)
        # Wait for ep_queue_size to become Zero
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, [bucket])
        client.crud(DocLoading.Bucket.SubDocOps.LOOKUP, key, sub_doc[0],
                    xattr=True, access_deleted=True)

        self.log.info("Loading docs to make the tombstone doc as non-resident")
        is_resident = True
        start_index = 0
        batch_size = 1000
        shell = RemoteMachineShellConnection(self.cluster.master)

        hash_dump_cmd = \
            "%s -u %s -p %s localhost:%d raw \"_hash-dump %d\" | grep %s" \
            % (Linux.COUCHBASE_BIN_PATH + "cbstats",
               self.cluster.master.rest_username,
               self.cluster.master.rest_password,
               self.cluster.master.memcached_port, key_vb, key)

        # Loading docs until the target doc is evicted from memory
        while is_resident:
            doc_gen = doc_generator("docs", start_index, batch_size,
                                    key_size=100, doc_size=10240,
                                    vbuckets=bucket.numVBuckets,
                                    target_vbucket=[key_vb])
            while doc_gen.has_next():
                d_key, val = doc_gen.next()
                client.crud(DocLoading.Bucket.DocOps.CREATE, d_key, val)

            output, _ = shell.execute_command(hash_dump_cmd)
            if not output:
                is_resident = False
            start_index = doc_gen.itr
            num_items += batch_size

        # Close the shell connections
        shell.disconnect()

        result = self.task.rebalance(self.cluster, [in_node], [])
        self.assertTrue(result, "Rebalance_in failed")

        cbstat = Cbstats(self.cluster.master)
        replica_vbs = cbstat.vbucket_list(bucket.name, Bucket.vBucket.REPLICA)
        cbstat.disconnect()
        if key_vb not in replica_vbs:
            # Swap the nodes in-order to maintain the vbucket consistency
            in_node = self.cluster.servers[0]
            self.cluster.master = self.cluster.servers[1]

            cbstat = Cbstats(self.cluster.servers[1])
            replica_vbs = cbstat.vbucket_list(bucket.name,
                                              Bucket.vBucket.REPLICA)
            cbstat.disconnect()
        self.assertTrue(key_vb in replica_vbs, "vBucket is still active vb")

        client.crud(DocLoading.Bucket.SubDocOps.REMOVE, key, sub_doc[0],
                    xattr=True, access_deleted=True)
        client.close()

        # Rebalance out the new node
        result = self.task.rebalance(self.cluster, [], [in_node])
        self.assertTrue(result, "Rebalance_out failed")

        # Performing upgrade
        if upgrade_cluster == "source":
            self.log.info("Upgrading node: {}".format(self.cluster.master.ip))
            self.upgrade_function[self.upgrade_type](self.cluster.master)
        elif upgrade_cluster == "remote":
            self.log.info("Upgrading node: {}".format(in_node.ip))
            self.upgrade_helper.new_install_version_on_all_nodes(
                nodes=[in_node], version=self.upgrade_version)


        self.log.info("Starting XDCR replication")
        xdcr_cluster = CBCluster("C2", servers=[in_node])
        xdcr_cluster.nodes_in_cluster = [in_node]
        RestConnection(in_node).init_node()
        self.bucket_util.create_default_bucket(
            cluster=xdcr_cluster, ram_quota=self.bucket_size,
            storage=self.bucket_storage,
            replica=0,
            wait_for_warmup=True)

        rest = RestConnection(self.cluster.master)
        rest.add_remote_cluster(xdcr_cluster.master.ip,
                                xdcr_cluster.master.port,
                                xdcr_cluster.master.rest_username,
                                xdcr_cluster.master.rest_password,
                                xdcr_cluster.master.ip)
        rest.start_replication("continuous",
                               self.cluster.buckets[0].name,
                               xdcr_cluster.master.ip,
                               toBucket=xdcr_cluster.buckets[0].name)
        try:
            self.log.info("Waiting for all items to get replicated")
            self.bucket_util.verify_stats_all_buckets(xdcr_cluster,
                                                      num_items, timeout=180)
        finally:
            self.log.info("Removing xdcr bucket and remote references")
            self.bucket_util.delete_all_buckets(xdcr_cluster)
            rest.remove_all_replications()
            rest.remove_all_remote_clusters()

    def test_rate_limit_across_upgrade(self):
        """
        Rate-limit edits across one upgrade timeline:
          1. mixed mode (a single node upgraded) - the edit must be rejected
          2. uniform 8.5+ - the edit must succeed and both values persist

        The two phases are one ordered sequence, not independent scenarios:
        reaching mixed mode already requires the upgrade that phase 2 needs.
        Phase 1 records through log_failure instead of asserting, so a
        mixed-mode regression does not hide the post-upgrade behaviour.
        """

        def _set_rate_limit(master_node):
            edit_params = {
                Bucket.throttleReserved: self.bucket_throttle_reserved,
                Bucket.throttleHardLimit: self.bucket_throttle_hard_limit,
            }
            return BucketRestApi(master_node).edit_bucket(self.bucket.name,
                                                          edit_params)

        self.delete_buckets_on_all_nodes = True
        self.upgrade_version = self.upgrade_chain[-1]

        # Phase 1 - upgrade one node to put the cluster into mixed mode
        node_to_upgrade = self.fetch_node_to_upgrade()
        self.assertIsNotNone(node_to_upgrade, "No node found to upgrade")
        self.log.info(f"Upgrading {node_to_upgrade.ip} to enter mixed mode")
        self.upgrade_function[self.upgrade_type](node_to_upgrade)

        # fetch_node_to_upgrade() re-runs find_orchestrator, so cluster.master
        # is a node still in the cluster. online_swap rebalances the node it
        # upgrades *out* and leaves cluster.nodes_in_cluster untouched, so
        # neither that list nor a cached master survives an upgrade step.
        node_to_upgrade = self.fetch_node_to_upgrade()
        self.assertIsNotNone(
            node_to_upgrade,
            "Every node reports the target version; mixed mode never reached")
        status, content = _set_rate_limit(self.cluster.master)
        self.log.info(f"Mixed-mode rate-limit edit: status={status}, "
                      f"content={content}")
        if status:
            self.log_failure(
                f"Rate limit edit should be rejected in mixed-mode: {content}")

        # Phase 2 - finish the upgrade, then the same edit must stick
        while node_to_upgrade is not None:
            self.log.info(f"Upgrading node {node_to_upgrade.ip}")
            self.upgrade_function[self.upgrade_type](node_to_upgrade)
            node_to_upgrade = self.fetch_node_to_upgrade()

        status, content = _set_rate_limit(self.cluster.master)
        self.log.info(f"Post-upgrade rate-limit edit: status={status}, "
                      f"content={content}")
        if not status:
            self.log_failure(
                f"Rate limit edit should succeed post-upgrade: {content}")
        else:
            _, buckets = BucketRestApi(
                self.cluster.master).get_bucket_info()
            bucket_info = next(b for b in buckets
                               if b['name'] == self.bucket.name)
            self.log.info(f"Bucket limits post-upgrade: {bucket_info}")
            for field, expected in (
                    (Bucket.throttleReserved, self.bucket_throttle_reserved),
                    (Bucket.throttleHardLimit,
                     self.bucket_throttle_hard_limit)):
                if bucket_info.get(field) != expected:
                    self.log_failure(
                        f"{field} mismatch post-upgrade: expected "
                        f"{expected}, got {bucket_info.get(field)}")

        self.validate_test_failure()

    # KV upgrade gaps for Totoro: mixed-version availability, upgrade
    # rebalance under DCP stress, post-upgrade engine behaviour. The checks
    # live in KVUpgradeChecks (kv_upgrade_checks.py).

    def __upgrade_node(self, node):
        """
        online_swap runs the sub_data_spec load itself during its swap
        rebalance. Other upgrade types get the same load around the upgrade,
        as UpgradeTests.test_upgrade does
        """
        load_task = None
        if self.upgrade_with_data_load and self.upgrade_type != "online_swap":
            load_task = self.load_during_rebalance(self.sub_data_spec,
                                                   async_load=True)
        try:
            self.upgrade_function[self.upgrade_type](node)
        finally:
            if load_task is not None:
                load_task.stop_indefinite_doc_loading_tasks()
                self.task_manager.get_task_result(load_task)
        self.cluster_util.print_cluster_stats(self.cluster)

    def __upgrade_intermediate_hops(self):
        """
        Multi-hop chains: take every node through the intermediate versions
        first, so the checks run on the final hop to upgrade_version
        """
        for version in self.upgrade_chain[1:-1]:
            self.upgrade_version = version
            node = self.fetch_node_to_upgrade()
            while node is not None:
                self.__upgrade_node(node)
                node = self.fetch_node_to_upgrade()
        self.upgrade_version = self.upgrade_chain[-1]

    def __validate_spec_doc_counts(self):
        self.apply_kv_check_doc_counts()
        if float(self.upgrade_version[:3]) >= 7.6 \
                > float(self.initial_version[:3]):
            self.add_system_scope_to_all_buckets()
        self.bucket_util._wait_for_stats_all_buckets(
            self.cluster, self.cluster.buckets,
            cbstat_cmd="checkpoint", stat_name="num_items_for_persistence",
            timeout=300)
        self.bucket_util.validate_docs_per_collections_all_buckets(
            self.cluster, timeout=1200)

    def __stop_probe_on_error(self, probe):
        probe.stop()
        probe.close()

    def test_mixed_mode_kv_availability(self):
        """
        KV availability, durability and data integrity while the cluster is
        half upgraded.
        1. Start the availability probe (plain + durable writes, reads) on
           the source version. The pre-upgrade baseline must be error free
        2. Upgrade mixed_mode_upgraded_nodes KV nodes (default: half). Each
           upgrade window may lose up to max_error_pct_in_rebalance % of
           ops to transient errors, never to DurabilityImpossible
        3. Hold the cluster in mixed mode for mixed_mode_hold_secs. Steady
           mixed mode must be error free. Locked-key / CAS checks run here
        4. Finish the upgrade. Post-upgrade baseline must be error free,
           every acked write readable at its acked version and the
           per-collection doc counts must match
        """
        self.delete_buckets_on_all_nodes = True
        self.__upgrade_intermediate_hops()

        num_kv_nodes = len(self.cluster_util.get_kv_nodes(self.cluster))
        upgrade_first = self.mixed_mode_upgraded_nodes \
            or max(1, num_kv_nodes // 2)
        if upgrade_first >= num_kv_nodes:
            self.fail(f"mixed_mode_upgraded_nodes={upgrade_first} leaves no "
                      f"old node in a {num_kv_nodes} KV node cluster")

        locked_key_before = self.locked_key_signature(self.bucket, "pre")
        probe = self.start_availability_probe(self.bucket)
        try:
            self.sleep(self.probe_baseline_secs, "Pre-upgrade probe baseline")
            self.assert_probe_phase(probe, "pre_upgrade")

            step = 0
            for _ in range(upgrade_first):
                node = self.fetch_node_to_upgrade()
                self.assertIsNotNone(node, "No node left to upgrade")
                step += 1
                probe.set_phase(f"upgrade_step_{step}")
                self.__upgrade_node(node)
                self.assert_probe_phase(probe, f"upgrade_step_{step}",
                                        self.max_error_pct_in_rebalance)

            self.assertTrue(self.cluster_util.is_cluster_mixed(self.cluster),
                            f"Cluster not mixed after upgrading {step} nodes")
            probe.set_phase("mixed_mode_steady")
            locked_key_mixed = self.locked_key_signature(self.bucket, "mixed")
            self.sleep(self.mixed_mode_hold_secs, "Hold cluster in mixed mode")
            self.assert_probe_phase(probe, "mixed_mode_steady")

            node = self.fetch_node_to_upgrade()
            while node is not None:
                step += 1
                probe.set_phase(f"upgrade_step_{step}")
                self.__upgrade_node(node)
                self.assert_probe_phase(probe, f"upgrade_step_{step}",
                                        self.max_error_pct_in_rebalance)
                node = self.fetch_node_to_upgrade()
        except BaseException:
            self.__stop_probe_on_error(probe)
            raise

        self.finish_availability_probe(probe)
        self.compare_locked_key_signatures(
            locked_key_before, locked_key_mixed,
            self.locked_key_signature(self.bucket, "post"))
        self.__validate_spec_doc_counts()
        self.validate_test_failure()

    def __kernel_info(self, node):
        shell = RemoteMachineShellConnection(node)
        try:
            output, _ = shell.execute_command(
                "uname -r; sysctl -n net.ipv4.tcp_rmem net.core.rmem_max")
        finally:
            shell.disconnect()
        output = [line.strip() for line in output]
        return (output[0] if output else "unknown"), " | ".join(output[1:])

    def __upgrade_step_under_stress(self, node, step):
        # The node that joins the cluster: the spare for a swap rebalance,
        # the node itself (reinstalled) otherwise
        incoming = self.spare_node if self.upgrade_type == "online_swap" \
            else node
        kernel, rmem = self.__kernel_info(incoming)
        self.log.info(f"Step {step}: upgrading {node.ip}, incoming node "
                      f"{incoming.ip} kernel={kernel}, rmem={rmem}")
        if self.expected_kernel_prefix \
                and not kernel.startswith(self.expected_kernel_prefix):
            self.fail(f"Incoming node {incoming.ip} runs kernel {kernel}, "
                      f"the new-kernel variant needs "
                      f"{self.expected_kernel_prefix}*")

        monitor = RebalanceStallMonitor(
            self.cluster, self.cluster_util, self.bucket.name, [incoming],
            self.log, stall_secs=self.rebalance_stall_secs)
        monitor.start()
        start_time = time.time()
        try:
            self.__upgrade_node(node)
        except AssertionError:
            if monitor.stall_reason:
                self.fail(f"Upgrade step {step} ({node.ip}): "
                          f"{monitor.stall_reason}")
            raise
        finally:
            monitor.stop()
        duration = time.time() - start_time
        self.log.info(f"Step {step} took {duration:.0f}s, max warmup STAT "
                      f"latency: {monitor.max_stat_latency}")
        if monitor.stall_reason:
            self.log_failure(f"Upgrade step {step} ({node.ip}) stalled: "
                             f"{monitor.stall_reason}")
        if duration > self.rebalance_step_timeout:
            self.log_failure(f"Upgrade step {step} ({node.ip}) took "
                             f"{duration:.0f}s, limit "
                             f"{self.rebalance_step_timeout}s")

        kv_nodes = self.cluster_util.get_kv_nodes(self.cluster)
        for bucket in self.cluster.buckets:
            if not self.bucket_util._wait_warmup_completed(
                    bucket, servers=kv_nodes, wait_time=300):
                self.log_failure(f"{bucket.name} warmup not complete after "
                                 f"upgrade step {step}")
            pending = self.wait_for_dcp_drain(bucket, self.dcp_drain_timeout)
            if pending:
                self.log_failure(f"{bucket.name} DCP not drained "
                                 f"{self.dcp_drain_timeout}s after upgrade "
                                 f"step {step}: {pending}")

    def test_upgrade_rebalance_under_dcp_stress(self):
        """
        Upgrade rebalances under heavy mutation / DCP replication load
        (CBSE-22926 / MB-70640: DCP stall on new kernel, CBSE-22272:
        warmup STAT blocked -> rebalance timeout).
        Variants come from the conf: upgrade_type=online_swap for the swap
        rebalance, spare nodes on the new OS image plus
        expected_kernel_prefix for the new-kernel variant, a high ops_rate
        and replicas=2 for DCP pressure.
        Per upgraded node:
        - RebalanceStallMonitor fails the step if rebalance progress stops
          moving or a warmup STAT call blocks, and stops that rebalance
        - the step must finish within rebalance_step_timeout
        - warmup complete and DCP drained on every KV node afterwards
        Throughout: the availability probe. At the end: no data loss and
        matching per-collection doc counts
        """
        self.delete_buckets_on_all_nodes = True
        self.__upgrade_intermediate_hops()

        probe = self.start_availability_probe(self.bucket)
        try:
            self.sleep(self.probe_baseline_secs, "Pre-upgrade probe baseline")
            self.assert_probe_phase(probe, "pre_upgrade")

            step = 0
            node = self.fetch_node_to_upgrade()
            while node is not None:
                step += 1
                probe.set_phase(f"upgrade_step_{step}")
                self.__upgrade_step_under_stress(node, step)
                self.assert_probe_phase(probe, f"upgrade_step_{step}",
                                        self.max_error_pct_in_rebalance)
                if self.test_failure:
                    break
                node = self.fetch_node_to_upgrade()
        except BaseException:
            self.__stop_probe_on_error(probe)
            raise

        if self.test_failure:
            self.__stop_probe_on_error(probe)
            self.validate_test_failure()
        self.finish_availability_probe(probe)
        self.__validate_spec_doc_counts()
        self.validate_test_failure()

    def test_post_upgrade_kv_engine_sanity(self):
        """
        Post-upgrade KV behaviour signals - stats / behaviour checks, not
        perf timing:
        - engine defaults (executor pool sizes incl. NonIO threads, pager
          and getl config, memcached global thread settings) compared
          before vs after. Changes not in allowed_engine_changes are
          flagged, expected_engine_values pinned (CBSE-23155)
        - TTL tombstones created on the source version, and after the
          upgrade, are purged once purgeInterval elapses (CBSE-22954)
        - getAndLock / CAS behaviour identical before, in mixed mode and
          after the upgrade, plus post-upgrade invariants (CBSE-22160)
        """
        self.delete_buckets_on_all_nodes = True
        self.__upgrade_intermediate_hops()

        state = self.kv_sanity_pre_upgrade(self.bucket)
        node = self.fetch_node_to_upgrade()
        while node is not None:
            self.__upgrade_node(node)
            node = self.fetch_node_to_upgrade()
            if node is not None and state["locked_key_mixed"] is None:
                state["locked_key_mixed"] = self.locked_key_signature(
                    self.bucket, "mixed")

        self.kv_sanity_post_upgrade(self.bucket, state)
        self.__validate_spec_doc_counts()
        self.validate_test_failure()
