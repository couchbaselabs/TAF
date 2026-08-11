import json
import os
import re
import subprocess
import time

from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_server_rest_util.fusion.fusion_api import FusionRestAPI
from cb_tools.cbstats import Cbstats
from cluster_utils.cluster_ready_functions import CBCluster
from sdk_client3 import SDKClientPool
from shell_util.remote_connection import RemoteMachineShellConnection
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_base import MagmaBaseTest


class FusionNativeBackupRestore(MagmaBaseTest, FusionBase):
    """Exercise Fusion's native, accelerator-driven snapshot backup/restore.

    One manifest per source bucket is captured with accelerator-cli
    generate-manifest, and ALL of them (one or more source buckets, one or
    more clones per bucket) are handed to
    /controller/fusion/prepareSnapshotRestore in a SINGLE call, producing one
    restore plan that is expanded/downloaded onto guest volumes with
    accelerator-cli split-manifest + download-files and completed with a
    single /controller/fusion/restoreSnapshot call - mirroring the shell
    script's habit of restoring several clones of one bucket in one
    prepare/restore pair, generalised to N source buckets x M clones each.

    NOTE on cluster targeting: apps/ns_server/src/fusion_backup.erl
    (validate_restore/2, MB-71396) shows restoreSnapshot re-reads the
    CURRENT cluster's active KV node list at restore time and compares it
    against both the plan captured at prepare time and the request's own
    "nodes" list - and a plan minted by one cluster's ns_orchestrator/
    chronicle isn't even visible to a different cluster's. So
    prepareSnapshotRestore, split-manifest/download-files, and
    restoreSnapshot all take an optional target_cluster param, and every
    test method here supports BOTH modes via a single cross_cluster conf
    flag rather than having a separate test per mode:
      - cross_cluster=False (default): target_cluster resolves to
        self.cluster - prepare/restore run against the same cluster the
        bucket lives on, and the new clone bucket(s) land there too.
      - cross_cluster=True: resolve_target_cluster() provisions a real,
        independent second cluster (self.dest_cluster, via
        ensure_dest_cluster) and every step targets it instead - a genuine
        attempt at using self.cluster's bucket manifest to seed a restore
        entirely on a cluster that never had any of this data. No outcome
        is assumed ahead of time either way - each call's own assertTrue
        reports what actually happens.

    Contrast with FusionBackupRestore (storage.fusion.fusion_backup_restore):
    that test simulates a Capella EBS snapshot by node_remap-cloning the
    entire cluster onto a separate set of destination nodes. This test
    never touches node identity or on-disk config - it restores data into
    new buckets on the SAME cluster that is already running, exactly as
    scripts/fusion_scripts/fusion_native_backup_restore.sh and ns_server's
    own reference test (cluster_tests/testsets/fusion_tests.py
    restore_test, MB-71396) do it.
    """

    def setUp(self):
        super(FusionNativeBackupRestore, self).setUp()

        self.log.info("FusionNativeBackupRestore setUp Started")

        # accelerator-cli ships as part of the couchbase-server install.
        self.cb_install_dir = self.input.param("cb_install_dir", "/opt/couchbase")
        self.accelerator_cli = "{0}/bin/fusion/accelerator-cli".format(
            self.cb_install_dir)

        # accelerator-cli split-manifest tunables, mirroring
        # fusion_accelerator_cli.py's manifest_parts/min_storage_size.
        self.restore_manifest_parts = self.input.param("restore_manifest_parts", 2)
        self.restore_min_storage_size = self.input.param("restore_min_storage_size", 0)

        # Config for the restored (clone) bucket(s).
        self.restore_replica_number = self.input.param("restore_replica_number", 0)
        self.restore_bucket_ram_quota = self.input.param(
            "restore_bucket_ram_quota", self.bucket_ram_quota or 256)

        # restoreSnapshot tunables: a settle sleep after guest volumes are
        # downloaded/mounted-symlinked but before the restore call, plus
        # retries (with sleep in between) since restoreSnapshot can 500/404
        # transiently right after the download step completes.
        self.restore_snapshot_pre_sleep = self.input.param(
            "restore_snapshot_pre_sleep", 30)
        self.restore_snapshot_max_attempts = self.input.param(
            "restore_snapshot_max_attempts", 3)
        self.restore_snapshot_retry_sleep = self.input.param(
            "restore_snapshot_retry_sleep", 30)

        # Unique id for this restore run - keys the NFS-shared manifest dir
        # and the /guests mount point so re-runs/parallel tests don't collide.
        self.restore_id = "native_restore_{0}".format(self.case_number)

        # NFS root mounted identically on every node (client-side), e.g.
        # /mnt/nfs/share for a log store URI of local:///mnt/nfs/share/buckets.
        # Manifests/plans are written under here so every node can read its
        # own slice without an explicit copy - the same trick
        # run_local_accelerator.sh/run_fusion_rebalance.py already rely on
        # for rebalance plans (see fusion_base.py log_store_rebalance_cleanup).
        self.restore_nfs_root = os.path.dirname(
            self.log_store_base_uri().rstrip("/"))
        self.restore_manifest_dir = "{0}/fusion-manifests/{1}".format(
            self.restore_nfs_root, self.restore_id)
        self.restore_guest_storage_root = "{0}/guest_storage".format(
            self.restore_nfs_root)

        self.clone_buckets = []

        # Every test method here supports BOTH same-cluster and
        # cross-cluster restore via this single flag, rather than having a
        # separate test per mode - see resolve_target_cluster().
        # self.dest_cluster is provisioned lazily (only when cross_cluster
        # is actually set) - see provision_dest_cluster()/ensure_dest_cluster().
        self.cross_cluster = self.input.param("cross_cluster", False)
        self.dest_cluster = None

    def tearDown(self):
        if self.dest_cluster is not None:
            self.cleanup_dest_cluster()
        super(FusionNativeBackupRestore, self).tearDown()

    def cleanup_dest_cluster(self):
        """Reset self.dest_cluster back to how it was before
        provision_dest_cluster() ran, so its servers are free for a
        subsequent cross_cluster=True test's ensure_dest_cluster() to reuse
        cleanly: delete the bucket(s) restoreSnapshot created there, then
        rebalance the extra nodes back out (undoing the scale-up
        rebalance-in provision_dest_cluster() did for dest_num_nodes > 1).
        Runs after the test's own verification is done (tearDown always
        follows the test method body); failures here are logged, not
        raised, so they don't mask the test's actual pass/fail result."""
        self.log.info("Cleaning up destination cluster {0}".format(
            self.dest_cluster.master.ip))
        # try:
        #     self.bucket_util.delete_all_buckets(self.dest_cluster)
        # except Exception as e:
        #     self.log.warning("Failed to delete buckets on destination "
        #                      "cluster {0}: {1}".format(
        #                          self.dest_cluster.master.ip, e))

        extra_nodes = self.dest_cluster.nodes_in_cluster[1:]
        if extra_nodes:
            try:
                self.task.rebalance(self.dest_cluster, [], extra_nodes,
                                   validate_bucket_ranking=False)
                self.dest_cluster.nodes_in_cluster = [self.dest_cluster.master]
            except Exception as e:
                self.log.warning("Failed to rebalance out destination "
                                 "cluster nodes {0}: {1}".format(
                                     [n.ip for n in extra_nodes], e))

    # ------------------------------------------------------------------ #
    # Independent second cluster (cross_cluster=True mode only)
    # ------------------------------------------------------------------ #

    def ensure_dest_cluster(self):
        """Provision self.dest_cluster on first use and cache it."""
        if self.dest_cluster is None:
            self.dest_cluster = self.provision_dest_cluster()
        return self.dest_cluster

    def resolve_target_cluster(self):
        """The cluster prepare_restore_plan/split_and_download_restore_plan/
        restore_snapshot should target: self.dest_cluster (provisioned on
        first use) when cross_cluster=True, else self.cluster. Every test
        method calls this once and threads the result through as
        target_cluster - same flow either way, just a different target, so
        there's no separate cross-cluster test method to keep in sync."""
        return self.ensure_dest_cluster() if self.cross_cluster else self.cluster

    def provision_dest_cluster(self):
        """Stand up a brand-new, independently-initialized, empty Couchbase
        cluster from spare nodes (servers in this test's .ini that aren't
        part of self.cluster) - a REAL second cluster (init_node/rebalance),
        exactly like pytests/aGoodDoctor/Hospital.py builds its XDCR remote
        cluster, NOT a node_remap clone of self.cluster (that's what
        storage.fusion.fusion_backup_restore.FusionBackupRestore does).

        The LogStore URI is set AND Fusion is enabled here (own distinct
        LogStore - see prepare_dest_log_store_uri) so that
        prepareSnapshotRestore against this cluster gets past the trivial
        412 "Fusion is not enabled" and cross_cluster=True runs can exercise
        the actual prepare/restore behavior instead of being blocked at the
        first step."""
        self.dest_num_nodes = self.input.param("dest_num_nodes", 1)

        source_ips = {n.ip for n in self.cluster.nodes_in_cluster}
        free_servers = [s for s in self.cluster.servers if s.ip not in source_ips]
        if len(free_servers) < self.dest_num_nodes:
            self.fail("Need {0} spare server(s) beyond self.cluster to "
                      "provision the independent destination cluster; only "
                      "{1} free".format(self.dest_num_nodes, len(free_servers)))
        dest_servers = free_servers[:self.dest_num_nodes]

        self.log.info("Provisioning independent destination cluster on {0}".format(
            [s.ip for s in dest_servers]))
        dest_cluster = CBCluster(name="dest", servers=dest_servers,
                                 vbuckets=self.cluster.vbuckets)
        dest_cluster.nodes_in_cluster.append(dest_cluster.master)

        self._initialize_nodes(self.task, dest_cluster,
                               self.disabled_consistent_view,
                               self.rebalanceIndexWaitingDisabled,
                               self.rebalanceIndexPausingDisabled,
                               self.maxParallelIndexers,
                               self.maxParallelReplicaIndexers,
                               self.port, self.quota_percent)

        if len(dest_servers) > 1:
            self.task.rebalance(dest_cluster, dest_servers[1:], [],
                                validate_bucket_ranking=False)
            dest_cluster.nodes_in_cluster = list(dest_servers)

        self.cluster_util.update_cluster_nodes_service_list(dest_cluster)
        self.bucket_util.add_rbac_user(dest_cluster.master)

        dest_log_store_uri = self.prepare_dest_log_store_uri(server=dest_cluster.master)
        self.log.info("Pointing the destination cluster at its own distinct "
                      "LogStore ({0})".format(dest_log_store_uri))
        status, content = FusionRestAPI(dest_cluster.master).manage_fusion_settings(
            log_store_uri=dest_log_store_uri,
            enable_sync_threshold=self.enable_sync_threshold)
        self.assertTrue(status, "Failed to configure Fusion LogStore on "
                        "destination cluster {0}: {1}".format(
                            dest_cluster.master.ip, content))

        self.log.info("Enabling Fusion on the destination cluster")
        status, content = FusionRestAPI(dest_cluster.master).enable_fusion()
        self.assertTrue(status, "Failed to enable Fusion on destination "
                        "cluster {0}: {1}".format(dest_cluster.master.ip, content))

        end_time = time.time() + 300
        enabled = False
        while time.time() < end_time:
            status, content = FusionRestAPI(dest_cluster.master).get_fusion_status()
            if status and content.get("state") == "enabled":
                enabled = True
                break
            time.sleep(2)
        self.assertTrue(enabled, "Fusion did not become enabled on destination "
                        "cluster {0} within 300s".format(dest_cluster.master.ip))

        return dest_cluster

    # ------------------------------------------------------------------ #
    # accelerator-cli / REST plumbing
    # ------------------------------------------------------------------ #

    def log_store_base_uri(self):
        """The log store in the bare form accelerator-cli's -base-uri
        expects: the "local://" scheme stripped for NFS, or the s3:// URI
        as-is."""
        uri = self.fusion_log_store_uri or ""
        if uri.startswith("local://"):
            return uri[len("local://"):]
        return uri

    def prepare_dest_log_store_uri(self, server=None):
        """Derive a distinct LogStore URI for the destination cluster, so it
        has its own logically-separate log store rather than sharing
        self.cluster's - mirrors
        FusionBackupRestore.prepare_dest_log_store_uri() and the shell
        script's own intent ("In real use cases, the destination would
        point to a different log store"). Even though the restored bucket
        gets a brand-new UUID (no on-disk collision risk with self.cluster's
        data), keeping the two log stores logically distinct is what makes
        the second download-files call in split_and_download_restore_plan
        (source LogStore -> destination LogStore) an actual copy instead of
        a no-op self-copy when target_cluster is dest_cluster.

        For an NFS store this is a sibling directory under the same
        already-mounted NFS share - created directly on `server` (defaults
        to self.dest_cluster.master; provision_dest_cluster passes its local
        dest_cluster.master explicitly, since self.dest_cluster isn't set
        yet at that point) via its client-side mount path (e.g.
        /mnt/nfs/share/buckets_dest), then chowned to the couchbase-server
        user so the server process can actually read/write it - not via the
        NFS server's own export path. For S3 it's a sibling key prefix,
        requiring no extra setup."""
        if getattr(self, "dest_fusion_log_store_uri", None):
            return self.dest_fusion_log_store_uri

        server = server or self.dest_cluster.master
        base_uri = self.fusion_log_store_uri or ""
        region_suffix = ""
        if "?" in base_uri:
            base_uri, region = base_uri.split("?", 1)
            region_suffix = "?" + region
        self.dest_fusion_log_store_uri = base_uri.rstrip("/") + "_dest" + region_suffix
        self.log.info(f"Dest Log Store URI: {self.dest_fusion_log_store_uri}")

        if self.log_store == "nfs":
            dest_path = self.dest_fusion_log_store_uri
            if dest_path.startswith("local://"):
                dest_path = dest_path[len("local://"):]
            self.log.info("Creating fresh LogStore dir for the destination "
                          "cluster on {0}: {1}".format(server.ip, dest_path))
            shell = RemoteMachineShellConnection(server)
            try:
                shell.execute_command("mkdir -p {0}".format(dest_path))
                shell.execute_command("chown -R couchbase:couchbase {0}".format(
                    dest_path))
            finally:
                shell.disconnect()

        return self.dest_fusion_log_store_uri

    def dest_log_store_base_uri(self):
        """The destination LogStore in the bare form accelerator-cli's
        -base-uri/-dest expects (scheme stripped for NFS, s3:// as-is)."""
        uri = self.prepare_dest_log_store_uri()
        if uri.startswith("local://"):
            return uri[len("local://"):]
        return uri

    def otp_node_name(self, server):
        return "ns_1@{0}".format(server.ip)

    def run_shell_cmd(self, server, cmd, timeout=300):
        """Run a plain shell command over SSH on `server`, logging the exact
        command before it runs and its output after - every accelerator-cli
        invocation, ls/mkdir/rm/ln, etc. in this file goes through here (or
        run_accelerator_cli, which wraps it) so the log always shows what
        ran, on which node, and what it returned."""
        shell = RemoteMachineShellConnection(server)
        try:
            self.log.info("[{0}] CMD: {1}".format(server.ip, cmd))
            o, e = shell.execute_command(cmd, timeout=timeout)
            shell.log_command_output(o, e)
            return o, e
        finally:
            shell.disconnect()

    def run_accelerator_cli(self, server, args, timeout=1800):
        """Run an accelerator-cli subcommand over SSH on `server`, failing
        the test if its output looks like an error."""
        cmd = "{0} {1}".format(self.accelerator_cli, args)
        o, e = self.run_shell_cmd(server, cmd, timeout=timeout)
        combined = "\n".join(o) + "\n" + "\n".join(e)
        if re.search(r"error|failed|panic|traceback", combined, re.IGNORECASE):
            self.fail("accelerator-cli failed on {0}: {1}\nOutput: {2}\n"
                      "Error: {3}".format(server.ip, cmd, o, e))
        return o, e

    def read_remote_json(self, server, path):
        shell = RemoteMachineShellConnection(server)
        try:
            o, e = shell.execute_command("cat {0}".format(path))
            shell.log_command_output(o, e)
            return json.loads("\n".join(o))
        finally:
            shell.disconnect()

    def write_remote_json(self, server, path, data):
        """Write a JSON payload to a file on `server` via a local scratch
        file + scp - the same idiom FusionBase/FusionBackupRestore use to
        move rebalance plans/config onto nodes - since the restore plan
        response can be too large to inline safely into one SSH command."""
        shell = RemoteMachineShellConnection(server)
        try:
            shell.execute_command("mkdir -p {0}".format(os.path.dirname(path)))
        finally:
            shell.disconnect()

        local_path = os.path.join(
            self.fusion_output_dir,
            "restore_plan_test{0}.json".format(self.case_number))
        with open(local_path, "w") as fp:
            json.dump(data, fp)

        copy_cmd = ('sshpass -p "{0}" scp -o StrictHostKeyChecking=no '
                   '{1} root@{2}:{3}').format(
            server.ssh_password, local_path, server.ip, path)
        self.log.info("Copying restore plan to {0}:{1}".format(server.ip, path))
        subprocess.run(copy_cmd, shell=True, executable="/bin/bash")

    def create_sdk_clients_for_bucket(self, bucket, target_cluster=None):
        """Create the doc-loader client pool entry for a bucket that just
        appeared on target_cluster (defaults to self.cluster) - the pool
        built in MagmaBaseTest.setUp only covers the buckets that existed
        at setup time, and target_cluster (when it's a freshly-provisioned
        dest_cluster) never had a pool at all. For sirius_java_sdk this is
        a process-wide singleton keyed by bucket name only (not by cluster),
        so registering the clone bucket's name against target_cluster.master
        here is what makes verify_docs_readable's reads land on the right
        cluster afterward."""
        target_cluster = target_cluster or self.cluster
        max_clients = min(self.task_manager.number_of_threads, 20)
        if self.load_docs_using == "default_loader":
            if getattr(target_cluster, "sdk_client_pool", None) is None:
                target_cluster.sdk_client_pool = SDKClientPool()
            target_cluster.sdk_client_pool.create_clients(
                target_cluster, bucket, [target_cluster.master], max_clients,
                compression_settings=self.sdk_compression)
        elif self.load_docs_using == "sirius_java_sdk":
            SiriusCouchbaseLoader.create_clients_in_pool(
                target_cluster.master, target_cluster.master.rest_username,
                target_cluster.master.rest_password, bucket.name, max_clients)

    # ------------------------------------------------------------------ #
    # Backup (manifest) -> prepare -> split/download -> restore
    # ------------------------------------------------------------------ #

    def generate_source_manifest(self, bucket):
        """accelerator-cli generate-manifest for `bucket`'s LogStore
        namespace (kv/<bucket-uuid>), run on the cluster's master. Returns
        the manifest as a parsed dict, ready to embed in a restore request.
        """
        bucket_uuid = self.get_bucket_uuid(bucket.name)
        manifest_path = "{0}/backup_manifest_{1}.json".format(
            self.restore_manifest_dir, bucket.name)

        shell = RemoteMachineShellConnection(self.cluster.master)
        try:
            shell.execute_command("mkdir -p {0}".format(self.restore_manifest_dir))
        finally:
            shell.disconnect()

        self.run_accelerator_cli(
            self.cluster.master,
            "generate-manifest -base-uri {0} -namespace kv/{1} "
            "-type namespace -manifest {2}".format(
                self.log_store_base_uri(), bucket_uuid, manifest_path))

        return self.read_remote_json(self.cluster.master, manifest_path)

    def build_bucket_configs(self, source_buckets, manifests, num_clones):
        """Build the combined "buckets" payload for a single
        prepareSnapshotRestore call: for every source bucket, num_clones
        clone-bucket configs all referencing that bucket's own manifest. All
        of it goes into ONE prepare/restore call, so an N-bucket x M-clone
        snapshot restores atomically as a single plan - the same reason the
        shell script prepares its two clones of one bucket in a single call
        rather than two.

        NOTE: the field is "ramQuotaMB", not "ramQuota" - prepareSnapshotRestore
        validates bucket configs through the same path as normal bucket
        creation (menelaus_web_buckets:parse_new_buckets), which expects the
        standard bucket-creation field name. The shell script this test is
        based on uses "ramQuota", which is stale/incorrect against the
        actual merged API.

        Returns (buckets_payload, clone_name_map) where clone_name_map maps
        each generated clone bucket name back to its source bucket name.
        """
        buckets_payload = []
        clone_name_map = dict()
        for bucket in source_buckets:
            manifest = manifests[bucket.name]
            for i in range(1, num_clones + 1):
                clone_name = "{0}Clone{1}".format(bucket.name, i)
                buckets_payload.append({
                    "config": {
                        "name": clone_name,
                        "replicaNumber": self.restore_replica_number,
                        "ramQuotaMB": self.restore_bucket_ram_quota,
                    },
                    "manifest": manifest,
                })
                clone_name_map[clone_name] = bucket.name
        return buckets_payload, clone_name_map

    def prepare_restore_plan(self, buckets_payload, target_cluster=None):
        """POST /controller/fusion/prepareSnapshotRestore against
        target_cluster.master (defaults to self.cluster). The plan records
        the TARGET cluster's current active KV node list
        (fusion_backup.erl builds RestoreBlueprint with {nodes, KVNodes}
        read at this moment) - restoreSnapshot later requires that list to
        still match exactly, so no rebalance/node membership change may
        happen on target_cluster between this call and restore_snapshot.
        target_cluster only ever differs from self.cluster when
        cross_cluster=True (see resolve_target_cluster())."""
        target_cluster = target_cluster or self.cluster
        status, content = FusionRestAPI(target_cluster.master).\
            prepare_snapshot_restore(buckets_payload)
        self.assertTrue(status, "prepareSnapshotRestore failed: {0}".format(content))
        plan_uuid = content["planUUID"]
        self.log.info("Restore plan prepared against {0}, planUUID={1}".format(
            target_cluster.master.ip, plan_uuid))
        return plan_uuid, content

    def split_and_download_restore_plan(self, plan_uuid, plan_response, target_cluster=None):
        """Expand the prepared plan into per-node/per-part manifests via
        accelerator-cli split-manifest, and for every part download it TWICE
        (mirroring fusion_native_backup_restore.sh's per-part loop and
        ns_server's own MB-71396 reference test):
        1) source LogStore -> a guest volume on the matching node of
           target_cluster (defaults to self.cluster), and
        2) source LogStore -> the DESTINATION LogStore - self.log_store_base_uri()
           itself (a self-copy) when target_cluster is self.cluster, since
           there's only one LogStore in that case; dest_log_store_base_uri()
           - a genuinely distinct location - when target_cluster is
           dest_cluster (see prepare_dest_log_store_uri).

        split-manifest writes one directory per node, keyed by that node's
        OTP name - and since prepareSnapshotRestore ran against
        target_cluster, those names are exactly
        target_cluster.nodes_in_cluster's own otp_node_name()s.

        Returns {otp_node_name: [guest_volume_path, ...]} for EVERY node
        currently in target_cluster.nodes_in_cluster (nodes with no assigned
        parts still get an empty list) - restoreSnapshot's validator
        requires the "nodes" list to name every active KV node exactly, or
        it fails with "Nodes do not match restore plan" even if the missing
        node simply had nothing to mount."""
        target_cluster = target_cluster or self.cluster
        dest_log_store_uri = (self.log_store_base_uri()
                              if target_cluster is self.cluster
                              else self.dest_log_store_base_uri())
        plan_path = "{0}/prepare_response_{1}.json".format(
            self.restore_manifest_dir, plan_uuid)
        self.write_remote_json(target_cluster.master, plan_path, plan_response)

        split_output_dir = "{0}/acc_manifests_{1}".format(
            self.restore_manifest_dir, plan_uuid)
        self.run_accelerator_cli(
            target_cluster.master,
            "split-manifest -min-storage-size {0} -manifest {1} "
            "-parts {2} -base-uri {3} -output-dir {4}".format(
                self.restore_min_storage_size, plan_path,
                self.restore_manifest_parts, self.log_store_base_uri(),
                split_output_dir))

        # split-manifest writes under the NFS-shared manifest dir, so every
        # node can see every node's part files without an explicit copy.
        o, e = self.run_shell_cmd(target_cluster.master,
                                  "ls -1 {0}".format(split_output_dir))
        node_dirs = [line.strip() for line in o if line.strip()]

        guest_volume_paths = dict()
        for node_dir in node_dirs:
            # node_dir is the OTP node name accelerator-cli assigned this
            # node in the plan (e.g. "ns_1@172.23.x.y"), matching
            # otp_node_name() for one of target_cluster's own nodes.
            server = next((s for s in target_cluster.nodes_in_cluster
                          if self.otp_node_name(s) == node_dir), None)
            if server is None:
                self.fail("Restore plan node {0} is not part of the target "
                          "cluster ({1}) - prepareSnapshotRestore's plan "
                          "must match restoreSnapshot's target cluster "
                          "exactly (MB-71396)".format(
                              node_dir,
                              [self.otp_node_name(s)
                               for s in target_cluster.nodes_in_cluster]))

            part_dir = "{0}/{1}".format(split_output_dir, node_dir)
            o, e = self.run_shell_cmd(server, "ls -1 {0}".format(part_dir))
            part_files = sorted(line.strip() for line in o if line.strip())

            # Scoped by plan_uuid (not just self.restore_id): a retry that
            # re-prepares a smaller plan for just the buckets that failed
            # gets its own guest volume directories here, rather than
            # colliding with (and rm -rf'ing) a PREVIOUS attempt's guest
            # volumes that are still actively mounted for a bucket that
            # already succeeded - see backup_and_restore_buckets_with_retry.
            private_prefix = "{0}/{1}/{2}_{3}".format(
                self.restore_guest_storage_root, node_dir, self.restore_id,
                plan_uuid)
            guests_root = "/guests/{0}_{1}".format(self.restore_id, plan_uuid)
            guest_volume_paths[node_dir] = []

            self.run_shell_cmd(server, "rm -rf {0} {1}".format(
                private_prefix, guests_root))
            self.run_shell_cmd(server, "mkdir -p {0}".format(private_prefix))
            self.run_shell_cmd(server, "mkdir -p {0}".format(guests_root))

            for idx, part_file in enumerate(part_files, start=1):
                guest_dir = "{0}/guest{1}".format(private_prefix, idx)
                guest_symlink = "{0}/guest{1}".format(guests_root, idx)

                self.run_shell_cmd(server, "mkdir -p {0}".format(guest_dir))
                self.run_shell_cmd(server, "ln -sfn {0} {1}".format(
                    guest_dir, guest_symlink))

                self.log.info("Downloading part {0} from LogStore to guest "
                              "volume on {1}".format(part_file, server.ip))
                self.run_accelerator_cli(
                    server,
                    "download-files -manifest {0}/{1} -dest {2} "
                    "-base-uri {3}".format(
                        part_dir, part_file, guest_dir, self.log_store_base_uri()))

                self.log.info("Downloading part {0} from source LogStore to "
                              "destination LogStore ({1})".format(
                                  part_file, dest_log_store_uri))
                self.run_accelerator_cli(
                    server,
                    "download-files -manifest {0}/{1} -dest {2} "
                    "-base-uri {3}".format(
                        part_dir, part_file, dest_log_store_uri,
                        self.log_store_base_uri()))

                guest_volume_paths[node_dir].append(guest_symlink)

            # download-files runs over SSH as root, so everything it just
            # wrote under these two top-level directories is root-owned -
            # re-chown them (again, on top of the one-time chown at
            # directory-creation time in prepare_dest_log_store_uri()) so
            # couchbase-server can actually read what was just downloaded.
            self.run_shell_cmd(server, "chown -R couchbase:couchbase {0}".format(
                self.restore_guest_storage_root))
            self.run_shell_cmd(server, "chown -R couchbase:couchbase {0}".format(
                dest_log_store_uri))

        # restoreSnapshot's node list must name every currently active KV
        # node exactly - fill in an empty list for any node that split-
        # manifest didn't assign any part to.
        for server in target_cluster.nodes_in_cluster:
            guest_volume_paths.setdefault(self.otp_node_name(server), [])

        return guest_volume_paths

    def restore_snapshot(self, plan_uuid, guest_volume_paths, target_cluster=None):
        """POST /controller/fusion/restoreSnapshot?planUUID=... against
        target_cluster.master (defaults to self.cluster) - synchronous,
        blocks until the restore completes. target_cluster only ever
        differs from self.cluster when cross_cluster=True (see
        resolve_target_cluster()), pointing this at a genuinely independent
        second cluster; whatever the server does with that is what the
        caller reports - no outcome is assumed ahead of time.

        A SINGLE attempt only - no retry here. restoreSnapshot has no
        partial-plan concept (a planUUID's BucketInfos are fixed at prepare
        time and always ALL get attempted), so retrying with the exact same
        plan re-attempts buckets that already succeeded too, which then
        fails with "Bucket with given name already exists" since
        restoreSnapshot (re-)creates buckets from scratch every call.
        Retries that re-prepare a smaller plan scoped to just the buckets
        that actually failed live in
        prepare_split_and_restore_with_retry() instead.

        Returns (status, content) - the caller decides whether/how to retry."""
        target_cluster = target_cluster or self.cluster
        nodes = [{"name": node, "guestVolumePaths": paths}
                for node, paths in guest_volume_paths.items()]
        self.log.info("Restoring snapshot planUUID={0} against {1}, "
                      "nodes={2}".format(plan_uuid, target_cluster.master.ip,
                                        nodes))
        return FusionRestAPI(target_cluster.master).\
            restore_snapshot(plan_uuid, nodes)

    def extract_failed_bucket_names(self, content, buckets_payload):
        """restoreSnapshot's error response is a dict keyed by bucket name
        for per-bucket failures, e.g.
        {"defaultClone2": "Failed nodes while mounting volumes: [...]"} or
        {"defaultClone1": {"name": "Bucket with given name already exists"}}
        - but it can also be a flat, non-bucket-scoped error, e.g.
        {"nodes": "Nodes do not match restore plan"}. Only treat keys that
        are actually names of buckets we submitted as bucket-scoped
        failures, so a flat error doesn't get misread as "bucket 'nodes'
        failed"."""
        if not isinstance(content, dict):
            return set()
        submitted_names = {b["config"]["name"] for b in buckets_payload}
        return {name for name in content.keys() if name in submitted_names}

    def prepare_split_and_restore_with_retry(self, buckets_payload, target_cluster):
        """Prepare + split/download + restoreSnapshot, retrying up to
        self.restore_snapshot_max_attempts times (sleeping
        self.restore_snapshot_retry_sleep seconds in between). Each retry
        re-prepares a FRESH plan scoped to only the clone bucket(s) that
        failed in the previous attempt (via extract_failed_bucket_names) -
        not the buckets that already succeeded there, since resubmitting
        the full plan would try to (re)create those from scratch and fail
        with "Bucket with given name already exists".

        Returns the guest-volume-paths map from whichever attempt actually
        succeeded (for verify_guest_volumes_active) - not a union across
        every attempt, since a failed attempt's downloaded guest volumes
        for the bucket(s) that failed are superseded by the next attempt's
        fresh download and shouldn't be checked as "active"."""
        pending_payload = buckets_payload
        guest_volume_paths = dict()
        status = False
        content = None

        for attempt in range(1, self.restore_snapshot_max_attempts + 1):
            plan_uuid, plan_response = self.prepare_restore_plan(
                pending_payload, target_cluster=target_cluster)

            self.log.info("Expanding and downloading restore plan onto guest volumes")
            guest_volume_paths = self.split_and_download_restore_plan(
                plan_uuid, plan_response, target_cluster=target_cluster)

            self.sleep(self.restore_snapshot_pre_sleep,
                      "Wait for guest volumes to settle before calling "
                      "restoreSnapshot")

            status, content = self.restore_snapshot(
                plan_uuid, guest_volume_paths, target_cluster=target_cluster)
            if status:
                return guest_volume_paths

            pending_names = self.extract_failed_bucket_names(
                content, pending_payload)
            self.log.warning(
                "restoreSnapshot attempt {0}/{1} against {2} failed for "
                "bucket(s) {3}: {4}".format(
                    attempt, self.restore_snapshot_max_attempts,
                    target_cluster.master.ip,
                    sorted(pending_names) if pending_names else "(unattributed)",
                    content))

            if attempt < self.restore_snapshot_max_attempts:
                if pending_names:
                    pending_payload = [b for b in pending_payload
                                      if b["config"]["name"] in pending_names]
                # else: couldn't attribute the failure to specific bucket(s)
                # (e.g. a cluster-wide error) - retry the same payload as-is.
                self.sleep(self.restore_snapshot_retry_sleep,
                          "Wait before retrying restoreSnapshot (attempt "
                          "{0}/{1}) for bucket(s) {2}".format(
                              attempt + 1, self.restore_snapshot_max_attempts,
                              [b["config"]["name"] for b in pending_payload]))

        self.assertTrue(status, "restoreSnapshot failed after {0} attempt(s): "
                        "{1}".format(self.restore_snapshot_max_attempts, content))
        return guest_volume_paths

    def backup_and_restore_buckets(self, source_buckets, num_clones=1, target_cluster=None):
        """Run the full native backup/restore flow for `source_buckets`
        (one or more buckets on self.cluster), restoring each into
        num_clones new clone buckets on target_cluster (defaults to
        self.cluster) via a SINGLE prepare/restore call. Populates
        self.clone_buckets with the resulting Bucket objects.

        target_cluster only ever differs from self.cluster when
        cross_cluster=True (see resolve_target_cluster()) -
        generate_source_manifest still always runs against self.cluster
        (that's where the bucket being backed up actually is), but
        prepare/split/download/restore and the resulting clone bucket(s) go
        to target_cluster.

        Returns the restore plan's node -> guestVolumePaths map."""
        target_cluster = target_cluster or self.cluster
        manifests = dict()
        for bucket in source_buckets:
            self.log.info("Generating source manifest for bucket {0}".format(
                bucket.name))
            manifests[bucket.name] = self.generate_source_manifest(bucket)

        buckets_payload, clone_name_map = self.build_bucket_configs(
            source_buckets, manifests, num_clones)

        guest_volume_paths = self.prepare_split_and_restore_with_retry(
            buckets_payload, target_cluster)

        target_cluster.buckets = self.bucket_util.get_all_buckets(target_cluster)
        clone_names = set(clone_name_map.keys())
        self.clone_buckets = [b for b in target_cluster.buckets
                              if b.name in clone_names]
        self.assertEqual(len(self.clone_buckets), len(clone_names),
                         "Not all restored clone buckets showed up on the "
                         "cluster: expected {0}, found {1}".format(
                             sorted(clone_names),
                             sorted(b.name for b in self.clone_buckets)))
        for clone_bucket in self.clone_buckets:
            self.create_sdk_clients_for_bucket(clone_bucket, target_cluster=target_cluster)

        return guest_volume_paths

    # ------------------------------------------------------------------ #
    # Verification
    # ------------------------------------------------------------------ #

    def verify_docs_readable(self, bucket, num_docs):
        """Read back every doc via the SDK, proving the clone serves data
        straight off its guest volumes. No target_cluster param needed:
        java_doc_loader routes by the bucket-name-keyed sirius client pool
        (see create_sdk_clients_for_bucket), not by self.cluster."""
        self.perform_workload(0, num_docs, doc_op="read", buckets=[bucket])

    def verify_no_pending_bytes(self, target_cluster=None):
        """/fusion/status shapes "nodes" and each node's "buckets" as dicts
        keyed by name, not lists:
        {"state": ..., "nodes": {"ns_1@1.2.3.4": {"buckets":
            {"default": {"snapshotPendingBytes": 0}, ...}, "deleting": []}}}

        Not a strict validation: snapshotPendingBytes lingering non-zero
        right after restore doesn't necessarily mean anything is wrong (it
        may just not have settled yet), so this only logs a warning rather
        than failing the test."""
        target_cluster = target_cluster or self.cluster
        _, content = FusionRestAPI(target_cluster.master).get_fusion_status()
        for node_name, node_data in content.get("nodes", {}).items():
            for bucket_name, bucket_stat in node_data.get("buckets", {}).items():
                pending = bucket_stat.get("snapshotPendingBytes", 0)
                if pending != 0:
                    self.log.warning("Expected no snapshotPendingBytes after "
                                     "restore, found {0} on {1}:{2}".format(
                                         pending, node_name, bucket_name))

    def verify_item_count(self, bucket, expected_docs, target_cluster=None):
        """Total item count (active + replica copies) across all nodes must
        match the source bucket's doc count."""
        target_cluster = target_cluster or self.cluster
        effective_replicas = min(self.restore_replica_number,
                                 len(target_cluster.nodes_in_cluster) - 1)
        expected_total = expected_docs * (1 + max(effective_replicas, 0))

        self.bucket_util._wait_for_stats_all_buckets(target_cluster, [bucket])
        total_items = 0
        for server in target_cluster.nodes_in_cluster:
            cbstats = Cbstats(server)
            stats = cbstats.all_stats(bucket.name)
            cbstats.disconnect()
            total_items += int(stats.get("curr_items_tot", 0))

        self.assertEqual(total_items, expected_total,
                         "{0}: total item count {1} != expected {2}".format(
                             bucket.name, total_items, expected_total))

    def verify_guest_volumes_active(self, guest_volume_paths, target_cluster=None):
        """/fusion/activeGuestVolumes must report every guest volume path
        this restore mounted, grouped by node."""
        target_cluster = target_cluster or self.cluster
        _, active = FusionRestAPI(target_cluster.master).get_active_guest_volumes()
        self.log.info("activeGuestVolumes: {0}".format(active))
        for node, paths in guest_volume_paths.items():
            active_paths = active.get(node, [])
            for path in paths:
                self.assertIn(path, active_paths,
                             "Restored guest volume {0} on {1} not reported "
                             "active: {2}".format(path, node, active_paths))

    def verify_cluster_balanced(self, target_cluster=None):
        target_cluster = target_cluster or self.cluster
        _, content = ClusterRestAPI(target_cluster.master).cluster_details()
        self.assertTrue(content.get("balanced", False),
                        "Cluster not balanced after restore: "
                        "servicesNeedRebalance={0}, bucketsNeedRebalance={1}".format(
                            content.get("servicesNeedRebalance"),
                            content.get("bucketsNeedRebalance")))

    # ------------------------------------------------------------------ #
    # Guest volume migration control
    # ------------------------------------------------------------------ #

    def pin_migration_rate_limit(self, rate_limit, target_cluster=None):
        """Set fusion_migration_rate_limit on target_cluster (defaults to
        self.cluster). Pinning to 0 before the restore keeps guest volumes
        active long enough to verify (verify_guest_volumes_active);
        restoring the configured rate afterward (wait_for_guest_volumes_drain)
        lets migration actually drain them."""
        target_cluster = target_cluster or self.cluster
        status, content = ClusterRestAPI(target_cluster.master).\
            manage_global_memcached_setting(fusion_migration_rate_limit=rate_limit)
        self.log.info("Set migration rate limit to {0} on {1}: status={2}, "
                      "content={3}".format(rate_limit, target_cluster.master.ip,
                                          status, content))
        self.assertTrue(status, "Failed to set migration rate limit {0} on "
                        "{1}: {2}".format(rate_limit, target_cluster.master.ip,
                                          content))

    def log_migration_stats(self, target_cluster):
        """Log ep_fusion_migration_completed_bytes/ep_fusion_migration_total_bytes/
        ep_fusion_migration_failures per node/bucket, for visibility into
        migration progress while monitor_guest_volumes_until_drained polls -
        the guest-volume list alone doesn't show how close migration is to
        finishing or whether it's failing outright."""
        for server in target_cluster.nodes_in_cluster:
            cbstats = Cbstats(server)
            for bucket in self.clone_buckets:
                try:
                    stats = cbstats.all_stats(bucket.name)
                except Exception as e:
                    self.log.warning("[GUEST VOLUMES] Could not fetch migration "
                                     "stats from {0}:{1}: {2}".format(
                                         server.ip, bucket.name, e))
                    continue
                self.log.info(
                    "[GUEST VOLUMES] {0}:{1} ep_fusion_migration_completed_bytes="
                    "{2} ep_fusion_migration_total_bytes={3} "
                    "ep_fusion_migration_failures={4}".format(
                        server.ip, bucket.name,
                        stats.get("ep_fusion_migration_completed_bytes", "?"),
                        stats.get("ep_fusion_migration_total_bytes", "?"),
                        stats.get("ep_fusion_migration_failures", "?")))
            cbstats.disconnect()

    def monitor_guest_volumes_until_drained(self, target_cluster, duration=1800, interval=30):
        """Local re-implementation of FusionBase.monitor_active_guest_volumes(),
        parameterized by target_cluster - the inherited one is hardcoded to
        self.cluster, so it can't be reused for dest_cluster. Used for BOTH
        same-cluster and cross-cluster (wait_for_guest_volumes_drain no
        longer special-cases self.cluster to the inherited method), so
        log_migration_stats's extra visibility applies either way."""
        start_time = time.time()
        end_time = start_time + duration
        seen_volumes = False
        self.log.info("[GUEST VOLUMES] Monitoring {0} (max duration: {1}s, "
                      "poll interval: {2}s)".format(
                          target_cluster.master.ip, duration, interval))
        while time.time() < end_time:
            status, content = FusionRestAPI(target_cluster.master).\
                get_active_guest_volumes()
            elapsed = round(time.time() - start_time, 1)
            self.log.info("[GUEST VOLUMES] elapsed={0}s | Active Guest Volumes: "
                          "{1}".format(elapsed, content))
            self.log_migration_stats(target_cluster)
            if status:
                all_guests = []
                for guests in content.values():
                    all_guests.extend(guests)
                if all_guests:
                    seen_volumes = True
                elif seen_volumes:
                    self.log.info("[GUEST VOLUMES] All guest volumes drained. "
                                  "Total time: {0}s".format(elapsed))
                    return
            time.sleep(interval)
        self.log.warning("[GUEST VOLUMES] Monitor timed out after {0}s without "
                         "all volumes draining".format(duration))

    def wait_for_guest_volumes_drain(self, target_cluster=None, interval=30):
        """Resume migration (restore the configured fusion_migration_rate_limit,
        e.g. 75MB/s) and block until every active guest volume drains,
        asserting none remain - proving the restored data actually migrated
        off the guest volumes into the nodes' own local storage."""
        target_cluster = target_cluster or self.cluster
        self.pin_migration_rate_limit(self.fusion_migration_rate_limit,
                                      target_cluster=target_cluster)

        timeout = (self.calculate_guest_volume_monitor_timeout()
                  if target_cluster is self.cluster else 1800)
        self.log.info("Monitoring guest volumes drain (migration_rate_limit="
                      "{0}, timeout={1}s)".format(
                          self.fusion_migration_rate_limit, timeout))
        self.monitor_guest_volumes_until_drained(
            target_cluster, duration=timeout, interval=interval)

        _, active = FusionRestAPI(target_cluster.master).get_active_guest_volumes()
        remaining = sum(len(v) for v in active.values()) if isinstance(active, dict) else 0
        self.assertEqual(remaining, 0,
                         "Expected all guest volumes to drain after restoring "
                         "migration rate limit, {0} still active: {1}".format(
                             remaining, active))

    def run_all_verifications(self, checks):
        """Run every (description, callable) in `checks`, continuing past
        failures so every verification stage gets a chance to run instead of
        stopping at the first one that fails - mirrors
        FusionBase.validate_fusion_health()'s "collect failures, report once
        at the end" pattern. Fails the test once, at the end, listing every
        stage that failed (if any)."""
        failures = []
        for description, check in checks:
            try:
                check()
            except Exception as e:
                self.log.error("Verification failed: {0}: {1}".format(
                    description, e))
                failures.append("{0}: {1}".format(description, e))
        if failures:
            self.fail("{0} verification stage(s) failed:\n{1}".format(
                len(failures), "\n".join(failures)))

    # ------------------------------------------------------------------ #
    # Tests
    # ------------------------------------------------------------------ #

    def test_native_backup_restore_single_clone(self):
        """Restore a bucket's LogStore snapshot into a single new clone
        bucket via the native accelerator-driven
        prepareSnapshotRestore/restoreSnapshot flow. Targets self.cluster
        itself by default, or a genuinely independent second cluster
        (self.dest_cluster) when cross_cluster=True - see
        resolve_target_cluster()."""
        source_bucket = self.cluster.buckets[0]
        target_cluster = self.resolve_target_cluster()

        self.log.info("Starting initial load")
        self.initial_load()
        self.sleep(120 + self.fusion_upload_interval + 30,
                  "Wait for data to get persisted and synced to LogStore")

        # Freeze the LogStore's on-disk state while the manifest/plan are
        # built - the bucket keeps running, so a sync mid-manifest would
        # make the snapshot inconsistent with what accelerator-cli read.
        self.log.info("Pinning sync rate limit to 0 to freeze LogStore state")
        ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
            fusion_sync_rate_limit=0)

        self.log.info("Pinning migration rate limit to 0 so restored guest "
                      "volumes stay active for verification")
        self.pin_migration_rate_limit(0, target_cluster=target_cluster)

        guest_volume_paths = self.backup_and_restore_buckets(
            [source_bucket], num_clones=1, target_cluster=target_cluster)

        self.log.info("Restoring sync rate limit after the restore")
        ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
            fusion_sync_rate_limit=self.fusion_sync_rate_limit)

        clone_bucket = self.clone_buckets[0]

        self.log.info("========== VERIFICATION ==========")
        self.run_all_verifications([
            ("docs_readable", lambda: self.verify_docs_readable(
                clone_bucket, self.num_items)),
            ("no_pending_bytes", lambda: self.verify_no_pending_bytes(
                target_cluster=target_cluster)),
            ("item_count", lambda: self.verify_item_count(
                clone_bucket, self.num_items, target_cluster=target_cluster)),
            ("guest_volumes_active", lambda: self.verify_guest_volumes_active(
                guest_volume_paths, target_cluster=target_cluster)),
            ("guest_volumes_drain", lambda: self.wait_for_guest_volumes_drain(
                target_cluster=target_cluster)),
            ("cluster_balanced", lambda: self.verify_cluster_balanced(
                target_cluster=target_cluster)),
        ])

    def test_native_backup_restore_multiple_clones(self):
        """Restore the same source bucket into two clone buckets via a
        single prepare/restore call, mirroring the shell script's
        defaultClone/defaultClone2 pair. Targets self.cluster itself by
        default, or a genuinely independent second cluster
        (self.dest_cluster) when cross_cluster=True - see
        resolve_target_cluster()."""
        source_bucket = self.cluster.buckets[0]
        target_cluster = self.resolve_target_cluster()

        self.log.info("Starting initial load")
        self.initial_load()
        self.sleep(120 + self.fusion_upload_interval + 30,
                  "Wait for data to get persisted and synced to LogStore")

        self.log.info("Pinning sync rate limit to 0 to freeze LogStore state")
        ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
            fusion_sync_rate_limit=0)

        self.log.info("Pinning migration rate limit to 0 so restored guest "
                      "volumes stay active for verification")
        self.pin_migration_rate_limit(0, target_cluster=target_cluster)

        guest_volume_paths = self.backup_and_restore_buckets(
            [source_bucket], num_clones=2, target_cluster=target_cluster)

        self.log.info("Restoring sync rate limit after the restore")
        ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
            fusion_sync_rate_limit=self.fusion_sync_rate_limit)

        self.log.info("========== VERIFICATION ==========")
        checks = []
        for clone_bucket in self.clone_buckets:
            checks.append(("docs_readable:{0}".format(clone_bucket.name),
                          lambda b=clone_bucket: self.verify_docs_readable(
                              b, self.num_items)))
            checks.append(("item_count:{0}".format(clone_bucket.name),
                          lambda b=clone_bucket: self.verify_item_count(
                              b, self.num_items, target_cluster=target_cluster)))
        checks.extend([
            ("no_pending_bytes", lambda: self.verify_no_pending_bytes(
                target_cluster=target_cluster)),
            ("guest_volumes_active", lambda: self.verify_guest_volumes_active(
                guest_volume_paths, target_cluster=target_cluster)),
            ("guest_volumes_drain", lambda: self.wait_for_guest_volumes_drain(
                target_cluster=target_cluster)),
            ("cluster_balanced", lambda: self.verify_cluster_balanced(
                target_cluster=target_cluster)),
        ])
        self.run_all_verifications(checks)

    def select_source_buckets(self):
        """Pick which of self.cluster's buckets to restore, for exercising
        the "restore all buckets" vs "restore a subset" axis:

        - restore_bucket_names=<comma-separated names>: restore exactly
          those buckets (e.g. "magma.0,magma.1" out of 3 buckets created via
          standard_buckets=3 - see bucket_ready_functions.py's
          "{backend}.{count}" naming).
        - num_buckets_to_restore=<N> (default: all): otherwise, restore the
          first N of self.cluster.buckets.
        """
        restore_bucket_names = self.input.param("restore_bucket_names", None)
        if restore_bucket_names:
            wanted = set(name.strip() for name in restore_bucket_names.split(","))
            source_buckets = [b for b in self.cluster.buckets if b.name in wanted]
            self.assertEqual(len(source_buckets), len(wanted),
                             "Some requested restore_bucket_names not found "
                             "on the cluster: wanted {0}, have {1}".format(
                                 sorted(wanted),
                                 sorted(b.name for b in self.cluster.buckets)))
            return source_buckets

        num_buckets_to_restore = self.input.param(
            "num_buckets_to_restore", len(self.cluster.buckets))
        return list(self.cluster.buckets)[:num_buckets_to_restore]

    def test_native_backup_restore_matrix(self):
        """Generic, fully input-param-driven entry point for sweeping
        combinations of: which/how many source buckets get restored
        (select_source_buckets - all vs a selective subset), clones per
        bucket (num_clones), and the restored replica count relative to the
        source bucket's own replicas (restore_replica_number vs replicas).
        Node count (nodes_init) is already an independent conf param, and
        cross_cluster (see resolve_target_cluster()) drives whether this
        targets self.cluster or a genuinely independent second cluster, so
        a single test method reused across many conf combinations covers
        the whole matrix without one hardcoded test per combination."""
        num_clones = self.input.param("num_clones", 1)
        source_buckets = self.select_source_buckets()
        target_cluster = self.resolve_target_cluster()

        self.log.info("Starting initial load")
        self.initial_load()
        self.sleep(120 + self.fusion_upload_interval + 30,
                  "Wait for data to get persisted and synced to LogStore")

        self.log.info("Pinning sync rate limit to 0 to freeze LogStore state")
        ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
            fusion_sync_rate_limit=0)

        self.log.info("Pinning migration rate limit to 0 so restored guest "
                      "volumes stay active for verification")
        self.pin_migration_rate_limit(0, target_cluster=target_cluster)

        guest_volume_paths = self.backup_and_restore_buckets(
            source_buckets, num_clones=num_clones, target_cluster=target_cluster)

        self.log.info("Restoring sync rate limit after the restore")
        ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
            fusion_sync_rate_limit=self.fusion_sync_rate_limit)

        self.assertEqual(len(self.clone_buckets), len(source_buckets) * num_clones,
                         "Expected {0} clone buckets ({1} source buckets x "
                         "{2} clones), found {3}".format(
                             len(source_buckets) * num_clones, len(source_buckets),
                             num_clones, len(self.clone_buckets)))

        self.log.info("========== VERIFICATION ==========")
        checks = []
        for clone_bucket in self.clone_buckets:
            checks.append(("docs_readable:{0}".format(clone_bucket.name),
                          lambda b=clone_bucket: self.verify_docs_readable(
                              b, self.num_items)))
            checks.append(("item_count:{0}".format(clone_bucket.name),
                          lambda b=clone_bucket: self.verify_item_count(
                              b, self.num_items, target_cluster=target_cluster)))
        checks.extend([
            ("no_pending_bytes", lambda: self.verify_no_pending_bytes(
                target_cluster=target_cluster)),
            ("guest_volumes_active", lambda: self.verify_guest_volumes_active(
                guest_volume_paths, target_cluster=target_cluster)),
            ("guest_volumes_drain", lambda: self.wait_for_guest_volumes_drain(
                target_cluster=target_cluster)),
            ("cluster_balanced", lambda: self.verify_cluster_balanced(
                target_cluster=target_cluster)),
        ])
        self.run_all_verifications(checks)

    def test_native_backup_restore_multiple_buckets_multiple_clones(self):
        """Restore ALL of the cluster's buckets into num_clones clones each
        via a single prepare/restore call - e.g. 3 source buckets x 2 clones
        = 6 new buckets, all from one manifest set/one plan/one restore
        call. Targets self.cluster itself by default, or a genuinely
        independent second cluster (self.dest_cluster) when
        cross_cluster=True - see resolve_target_cluster()."""
        source_buckets = list(self.cluster.buckets)
        num_clones = self.input.param("num_clones", 2)
        target_cluster = self.resolve_target_cluster()

        self.log.info("Starting initial load")
        self.initial_load()
        self.sleep(120 + self.fusion_upload_interval + 30,
                  "Wait for data to get persisted and synced to LogStore")

        self.log.info("Pinning sync rate limit to 0 to freeze LogStore state")
        ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
            fusion_sync_rate_limit=0)

        self.log.info("Pinning migration rate limit to 0 so restored guest "
                      "volumes stay active for verification")
        self.pin_migration_rate_limit(0, target_cluster=target_cluster)

        guest_volume_paths = self.backup_and_restore_buckets(
            source_buckets, num_clones=num_clones, target_cluster=target_cluster)

        self.log.info("Restoring sync rate limit after the restore")
        ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
            fusion_sync_rate_limit=self.fusion_sync_rate_limit)

        self.assertEqual(len(self.clone_buckets), len(source_buckets) * num_clones,
                         "Expected {0} clone buckets ({1} source buckets x "
                         "{2} clones), found {3}".format(
                             len(source_buckets) * num_clones, len(source_buckets),
                             num_clones, len(self.clone_buckets)))

        self.log.info("========== VERIFICATION ==========")
        checks = []
        for clone_bucket in self.clone_buckets:
            checks.append(("docs_readable:{0}".format(clone_bucket.name),
                          lambda b=clone_bucket: self.verify_docs_readable(
                              b, self.num_items)))
            checks.append(("item_count:{0}".format(clone_bucket.name),
                          lambda b=clone_bucket: self.verify_item_count(
                              b, self.num_items, target_cluster=target_cluster)))
        checks.extend([
            ("no_pending_bytes", lambda: self.verify_no_pending_bytes(
                target_cluster=target_cluster)),
            ("guest_volumes_active", lambda: self.verify_guest_volumes_active(
                guest_volume_paths, target_cluster=target_cluster)),
            ("guest_volumes_drain", lambda: self.wait_for_guest_volumes_drain(
                target_cluster=target_cluster)),
            ("cluster_balanced", lambda: self.verify_cluster_balanced(
                target_cluster=target_cluster)),
        ])
        self.run_all_verifications(checks)
