import math
import re
import threading
import time

from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_server_rest_util.fusion.fusion_api import FusionRestAPI
from cluster_utils.cluster_ready_functions import CBCluster
from sdk_client3 import SDKClientPool
from sirius_client_framework.sirius_setup import SiriusSetup
from shell_util.remote_connection import RemoteMachineShellConnection
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_base import MagmaBaseTest


class FusionBackupRestore(MagmaBaseTest, FusionBase):
    """Simulate a Capella EBS snapshot backup/restore of a Fusion enabled
    cluster by cloning a source cluster onto a separate destination cluster.

    There is no real backup/restore here: we reproduce the on-disk events a
    server undergoes when a snapshot is attached to a brand new VM. The first
    ``nodes_init`` servers form the source cluster; the destination nodes are
    chosen at clone time to match the source's *current* size (one destination
    node per live source node), since the source may be rebalanced in/out
    before the snapshot. Both clusters are shut down, every source node's
    on-disk state (config + local data) is copied onto its destination node,
    ``node_remap`` rewrites the node identities and brings the destination up
    with auto-failover disabled and Fusion disabled (exactly what a restored
    Capella cluster does), and then Fusion is re-enabled on the destination so
    sync/migration resume.

    This mirrors pytests/ns_server/node_remap_tests.py::run_node_remap (the
    canonical source->dest clone) but adds the Fusion-specific remap flags.
    After the clone, ``self.cluster`` is repointed at the cloned destination
    cluster so all FusionBase helpers (enable_fusion, sync stats, guest
    volumes, rebalance) operate against it.

    Inspired by scripts/fusion_scripts/capella_restore.sh, but that script
    drives a local cluster_run; here we run against real VMs.
    """

    def setUp(self):
        super(FusionBackupRestore, self).setUp()

        self.log.info("FusionBackupRestore setUp Started")

        # Couchbase install layout on the VMs. node_remap and the initargs /
        # config that it reads+rewrites live under here.
        self.cb_install_dir = self.input.param("cb_install_dir", "/opt/couchbase")
        self.cb_var_dir = self.input.param(
            "cb_var_dir", f"{self.cb_install_dir}/var/lib/couchbase")
        self.node_remap_bin = f"{self.cb_install_dir}/bin/node_remap"
        self.node_remap_output_dir = "/tmp/node_remap_output"

        # enableSyncThresholdMB value node_remap stamps into the remapped
        # config, mirroring capella_restore.sh.
        self.node_remap_sync_threshold_mb = self.input.param(
            "node_remap_sync_threshold_mb", 1000)

        # couchbase-server runs as this user, so config files copied back
        # (created as root over SSH) must be re-owned to it.
        self.cb_user = self.input.param("cb_user", "couchbase")

        # After the clone the destination carries the source's bucket UUIDs, so
        # it must be pointed at its own log store before Fusion is re-enabled
        # (else the two clusters would collide on the shared store). A URI can
        # be supplied explicitly; otherwise one is derived from the source URI.
        self.dest_fusion_log_store_uri = self.input.param(
            "dest_fusion_log_store_uri", None)

        # Source -> destination clone topology. The first nodes_init servers
        # are the source cluster (provisioned by the base setUp); the remaining
        # servers are the pool the snapshot is cloned onto. The exact
        # destination nodes are chosen at clone time (select_dest_servers) to
        # match the *current* source size, since the source may be resized
        # (rebalanced in/out) before the snapshot is taken - e.g. a 2-node
        # source that is rebalanced down to 1 node clones onto 1 destination
        # node, not 2.
        if len(self.cluster.servers) <= self.nodes_init:
            self.fail("Need spare servers beyond the {}-node source cluster to "
                      "clone onto a destination; got {} total".format(
                          self.nodes_init, len(self.cluster.servers)))
        self.dest_servers = []

        # dest_ip -> source_ip mapping, populated by run_node_remap
        self.node_map_dict = dict()

        # Active guest volumes on the source, captured pre-clone so their
        # symlinks can be repointed on the destination (run_node_remap).
        self.pre_clone_guest_volumes = {}

    def tearDown(self):
        super(FusionBackupRestore, self).tearDown()

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #

    def count_active_guest_volumes(self):
        """Return (status, content, total_count) for /fusion/activeGuestVolumes.

        content is an object keyed by node, each value a list of volumes,
        e.g. {"ns_1@1.1.1.1": [], "ns_1@1.1.1.2": []}. The count is the
        flattened length across all nodes.
        """
        status, content = FusionRestAPI(self.cluster.master).get_active_guest_volumes()
        total = 0
        if status and isinstance(content, dict):
            for volumes in content.values():
                total += len(volumes)
        return status, content, total

    def copy_data_to_dest_node(self, source_node, dest_node):
        """SCP a source node's on-disk state onto a destination node,
        reproducing a Capella EBS snapshot being attached to a fresh VM.

        The whole Couchbase var dir (config + local magma working set) is
        copied into a scratch dir on the destination, and, when the data mount
        lives outside the var dir (the norm for Fusion, e.g. /data), that mount
        is copied too. Both nodes must already be stopped.
        """
        shell = RemoteMachineShellConnection(dest_node)
        try:
            # rm first so scp -r recreates the scratch dir as a copy of the
            # source var dir (contents at the top level).
            shell.execute_command(f"rm -rf {self.node_remap_output_dir}")
            scp_cfg = (
                f'sshpass -p "{source_node.ssh_password}" scp '
                f'-o StrictHostKeyChecking=no -r '
                f'root@{source_node.ip}:{self.cb_var_dir} '
                f'{self.node_remap_output_dir}')
            self.log.info(f"Copying var dir {source_node.ip} -> {dest_node.ip}, CMD: {scp_cfg}")
            o, e = shell.execute_command(scp_cfg)
            shell.log_command_output(o, e)

            if self.data_path and not self.data_path.startswith(self.cb_var_dir):
                shell.execute_command(f"rm -rf {self.data_path}/*")
                shell.execute_command(f"mkdir -p {self.data_path}")
                scp_data = (
                    f'sshpass -p "{source_node.ssh_password}" scp '
                    f'-o StrictHostKeyChecking=no -r '
                    f'root@{source_node.ip}:{self.data_path}/* '
                    f'{self.data_path}/')
                self.log.info(f"Copying data mount {self.data_path} {source_node.ip} -> {dest_node.ip}, CMD: {scp_data}")
                o, e = shell.execute_command(scp_data)
                shell.log_command_output(o, e)
        finally:
            shell.disconnect()

    def select_dest_servers(self):
        """Choose the destination nodes to clone onto - one per *current*
        source node.

        The source cluster may have been resized since setUp (a rebalance-out
        before the snapshot leaves it smaller, a rebalance-in larger), so the
        destination must match the live source size, not the initial
        nodes_init. Destination nodes are picked from the servers that are not
        part of the source cluster right now.
        """
        source_nodes = list(self.cluster.nodes_in_cluster)
        source_ips = {n.ip for n in source_nodes}
        free_servers = [s for s in self.cluster.servers
                        if s.ip not in source_ips]
        if len(free_servers) < len(source_nodes):
            self.fail("Not enough free servers to clone the source cluster: "
                      "need {0} destination node(s) for {0} source node(s), "
                      "only {1} free".format(len(source_nodes),
                                             len(free_servers)))
        self.dest_servers = free_servers[:len(source_nodes)]
        self.log.info("Cloning {0} source node(s) {1} -> destination node(s) "
                      "{2}".format(len(source_nodes),
                                   [n.ip for n in source_nodes],
                                   [n.ip for n in self.dest_servers]))

    def build_node_remap_cmd(self):
        """Build the node_remap command shared by every destination node.

        Contains a --remap pair for every source->destination node so each
        destination node can pick out its own mapping, plus the Fusion-specific
        flags: disable auto-failover, stamp the sync threshold, rewrite the log
        store URI to the destination's own store (the config carries the
        source's bucket UUIDs, so it must not reference the source's log store)
        and bring the node up with Fusion disabled (what a restored Capella
        cluster does). Also populates self.node_map_dict (dest_ip -> source_ip).
        """
        # Decide the destination log store URI (and create its backing dir)
        # up front so it can be baked into the remapped fusion_config.
        self.prepare_dest_log_store_uri()

        remap_args = ""
        for idx in range(len(self.dest_servers)):
            source_node = self.cluster.nodes_in_cluster[idx]
            dest_node = self.dest_servers[idx]
            remap_args += " --remap ns_1@{0} ns_1@{1}".format(
                source_node.ip, dest_node.ip)
            self.node_map_dict[dest_node.ip] = source_node.ip
            self.log.info("Remap {0} => {1}".format(source_node.ip, dest_node.ip))

        cmd_parts = [
            self.node_remap_bin,
            f"--initargs {self.cb_var_dir}/initargs",
            f"--output-path {self.node_remap_output_dir}",
            "--log-level debug",
            remap_args.strip(),
            "--rewrite '[fusion_config, enable_sync_threshold_mb]' "
            f"{self.node_remap_sync_threshold_mb}",
            # log_store_uri is a string, so the value must itself be quoted.
            "--rewrite '[fusion_config, log_store_uri]' "
            f"'\"{self.dest_fusion_log_store_uri}\"'",
            "--disable-fusion",
        ]
        return " ".join(cmd_parts)

    def run_node_remap(self):
        """Clone the source cluster onto the destination servers via node_remap.

        Mirrors pytests/ns_server/node_remap_tests.py::run_node_remap with the
        Fusion-specific remap flags. All source and destination nodes are
        stopped, each source node's on-disk state is copied onto its
        destination node, node_remap rewrites identity/config on every
        destination node, ownership+mode are restored and all nodes are brought
        back up.
        """
        self.node_map_dict = dict()

        # Pick destination nodes to match the *current* source size (the
        # source may have been rebalanced in/out before the snapshot).
        self.select_dest_servers()
        all_nodes = list(self.cluster.nodes_in_cluster) + list(self.dest_servers)

        # Capture the active guest volumes on the (still-running) source
        # cluster so their symlinks can be repointed on the destination before
        # startup. Empty when there are no active guest volumes (variation 2).
        status, gv_content, _ = self.count_active_guest_volumes()
        self.pre_clone_guest_volumes = \
            gv_content if (status and isinstance(gv_content, dict)) else {}
        self.log.info("Guest volumes captured before clone: {}".format(
            self.pre_clone_guest_volumes))

        # 1. Stop couchbase-server on source and destination nodes
        self.log.info("Stopping couchbase-server on source and destination nodes")
        for node in all_nodes:
            shell = RemoteMachineShellConnection(node)
            shell.stop_server()
            shell.disconnect()
        self.sleep(30, "Wait for couchbase-server to stop on all nodes")

        # 2. Copy each source node's on-disk state onto its destination node
        for idx in range(len(self.dest_servers)):
            self.copy_data_to_dest_node(self.cluster.nodes_in_cluster[idx],
                                        self.dest_servers[idx])

        # 3. Lay the copied config+data down over each destination var dir
        for dest_node in self.dest_servers:
            shell = RemoteMachineShellConnection(dest_node)
            shell.execute_command(f"rm -rf {self.cb_var_dir}/*")
            o, e = shell.execute_command(
                f"yes | cp -Rf {self.node_remap_output_dir}/* {self.cb_var_dir}/")
            shell.log_command_output(o, e)
            shell.disconnect()

        # 4. Run node_remap on every destination node (same cmd; each node
        #    picks its own mapping from the --remap pairs)
        remap_cmd = self.build_node_remap_cmd()
        self.log.info(f"node_remap cmd = {remap_cmd}")
        for dest_node in self.dest_servers:
            shell = RemoteMachineShellConnection(dest_node)
            o, e = shell.execute_command(remap_cmd)
            shell.log_command_output(o, e)
            shell.disconnect()

        # 5. Copy the remapped config back over the var dir, restore
        #    ownership+mode (node_remap and the copy ran as root over SSH so
        #    couchbase-server would otherwise be unable to read its config /
        #    open its data files), then clean up the scratch dir.
        for dest_node in self.dest_servers:
            shell = RemoteMachineShellConnection(dest_node)
            o, e = shell.execute_command(
                f"yes | cp -Rf {self.node_remap_output_dir}/* {self.cb_var_dir}/")
            shell.log_command_output(o, e)
            o, e = shell.execute_command(
                f"chown -R {self.cb_user}:{self.cb_user} {self.cb_var_dir} "
                f"&& chmod -R 0700 {self.cb_var_dir}")
            shell.log_command_output(o, e)
            if self.data_path and not self.data_path.startswith(self.cb_var_dir):
                o, e = shell.execute_command(
                    f"chown -R {self.cb_user}:{self.cb_user} {self.data_path} "
                    f"&& chmod -R 0700 {self.data_path}")
                shell.log_command_output(o, e)
            shell.execute_command(f"rm -rf {self.node_remap_output_dir}")
            shell.disconnect()

        # 5b. Repoint the cloned guest-volume symlinks from the destination
        #     node's IP back to the source node's guest_storage path (the guest
        #     data physically lives on the shared NFS under ns_1@<source_ip>).
        #     Must happen before the servers start.
        if self.pre_clone_guest_volumes:
            self.remap_guest_volume_symlinks(self.pre_clone_guest_volumes)

        # 6. Start couchbase-server on source and destination nodes
        self.log.info("Starting couchbase-server on source and destination nodes")
        for node in all_nodes:
            shell = RemoteMachineShellConnection(node)
            shell.start_server()
            shell.disconnect()

    def _dest_node_for_source_ip(self, source_ip):
        """Return the destination node object cloned from the given source IP
        (via node_map_dict, dest_ip -> source_ip), or None."""
        for dest_node in self.dest_servers:
            if self.node_map_dict.get(dest_node.ip) == source_ip:
                return dest_node
        return None

    def remap_guest_volume_symlinks(self, guest_volume_dict):
        """Repoint cloned guest-volume symlinks back to the source node's path.

        Active guest volumes are symlinks such as
            /guests/reb1/guest1 ->
                /mnt/nfs/share/guest_storage/ns_1@<ip>/reb1/guest1
        After the clone + node_remap they resolve to the *destination* node's
        IP, but the guest data was written on the shared NFS under the *source*
        node's guest_storage path, so each symlink must be pointed back at
        ns_1@<source_ip>. Runs on the destination node paired with each source
        node (from the /fusion/activeGuestVolumes dict captured pre-clone),
        before the servers are started.
        """
        for source_otp, guest_volumes in guest_volume_dict.items():
            if not guest_volumes:
                continue
            # "ns_1@172.23.222.165" (or with a :port) -> "172.23.222.165"
            source_ip = source_otp.split("@")[-1].split(":")[0]
            dest_node = self._dest_node_for_source_ip(source_ip)
            if dest_node is None:
                self.log.warning("No destination node mapped for source {}; "
                                 "skipping its guest volumes".format(source_otp))
                continue

            self.log.info("Repointing {0} guest volume(s) for source {1} on "
                          "destination {2}".format(len(guest_volumes),
                                                   source_otp, dest_node.ip))
            shell = RemoteMachineShellConnection(dest_node)
            try:
                for guest_volume in guest_volumes:
                    o, _ = shell.execute_command(f"readlink {guest_volume}")
                    current_target = o[0].strip() if o else ""
                    if not current_target:
                        self.log.warning("Guest volume {0} on {1} is not a "
                                         "symlink / has no target; "
                                         "skipping".format(guest_volume,
                                                           dest_node.ip))
                        continue
                    new_target = re.sub(r"ns_1@[0-9.]+",
                                        f"ns_1@{source_ip}", current_target)
                    self.log.info("[{0}] {1}: {2} -> {3}".format(
                        dest_node.ip, guest_volume, current_target, new_target))
                    o, e = shell.execute_command(
                        f"ln -sfn {new_target} {guest_volume}")
                    shell.log_command_output(o, e)
            finally:
                shell.disconnect()

    def setup_cloned_cluster(self):
        """Build a CBCluster for the cloned destination cluster and repoint
        self.cluster at it.

        The FusionBase helpers (enable_fusion, sync stats, guest volumes,
        rebalance) are all bound to self.cluster, so after the clone we swap
        self.cluster to the destination and keep a handle to the source in
        self.source_cluster. The remaining servers beyond the destination are
        left as spares so subsequent rebalances have nodes to add.
        """
        self.dest_cluster = CBCluster(name="C2", servers=self.dest_servers,
                                      vbuckets=self.cluster.vbuckets)
        self.dest_cluster.master = self.dest_servers[0]
        self.dest_cluster.nodes_in_cluster = list(self.dest_servers)
        # dest nodes + any servers used by neither cluster, as spares for a
        # later rebalance-in (the source cluster is still live on its nodes).
        used_ips = {n.ip for n in self.cluster.nodes_in_cluster} | \
            {n.ip for n in self.dest_servers}
        spare_servers = [s for s in self.cluster.servers
                         if s.ip not in used_ips]
        self.dest_cluster.servers = list(self.dest_servers) + spare_servers

        # Rebuild kv/index/query/... node lists from the live cloned cluster
        self.cluster_util.update_cluster_nodes_service_list(self.dest_cluster)
        self.dest_cluster.buckets = self.bucket_util.get_all_buckets(
            self.dest_cluster)

        # Create the doc-loader client pool for the cloned cluster. Doc loading
        # targets cluster.master, and the pool is keyed by that host + bucket -
        # the pool created at setUp points at the source master, so the
        # destination needs its own clients.
        self.create_sdk_clients_for_cluster(self.dest_cluster)

        # Repoint self.cluster at the clone so Fusion helpers operate on it
        self.source_cluster = self.cluster
        self.cluster = self.dest_cluster
        self.cluster_util.print_cluster_stats(self.cluster)

    def create_sdk_clients_for_cluster(self, cluster):
        """Create the SDK / Sirius Java client pool for a cluster's buckets.

        Mirrors the client-pool setup MagmaBaseTest.setUp does for the source
        cluster. The cloned destination cluster needs its own clients because
        doc loading (perform_workload -> java_doc_loader) targets
        cluster.master, and the client pool is keyed by that host + bucket.
        """
        max_clients = min(self.task_manager.number_of_threads, 20)
        if self.standard_buckets > 20:
            max_clients = self.standard_buckets
        clients_per_bucket = int(math.ceil(max_clients / self.standard_buckets))
        self.log.info("Creating {0} client(s)/bucket for cloned cluster via "
                      "{1}".format(clients_per_bucket, self.load_docs_using))
        if self.load_docs_using == "default_loader":
            cluster.sdk_client_pool = SDKClientPool()
            for bucket in cluster.buckets:
                cluster.sdk_client_pool.create_clients(
                    cluster, bucket, [cluster.master],
                    clients_per_bucket,
                    compression_settings=self.sdk_compression)
        elif self.load_docs_using == "sirius_java_sdk":
            # The DocLoader client pool is a process-wide singleton keyed by
            # bucket name only (server_ip is not used to route doc_load). The
            # cloned cluster has the SAME bucket names as the source, so merely
            # adding clients leaves those buckets bound to the source-connected
            # clients and workloads would keep hitting the SOURCE. Reset the
            # pool first so the buckets rebind to the destination's clients.
            # Safe here: after the clone we only load to the destination and no
            # Java loader tasks are in flight.
            SiriusSetup.reset_java_loader_tasks(self.thread_to_use)
            for bucket in cluster.buckets:
                SiriusCouchbaseLoader.create_clients_in_pool(
                    cluster.master,
                    cluster.master.rest_username,
                    cluster.master.rest_password,
                    bucket.name,
                    clients_per_bucket)

    def clone_cluster_via_node_remap(self):
        """Full source->dest clone: node_remap the source onto the destination
        nodes, wait for the destination to come up healthy and make it the
        active cluster (self.cluster).
        """
        self.log.info("Cloning source cluster onto destination via node_remap")
        self.run_node_remap()

        self.log.info("Waiting for destination nodes to become healthy")
        for node in self.dest_servers:
            if not self.cluster_util.is_ns_server_running(node):
                self.fail(f"Destination node {node.ip} did not come up after clone")

        self.setup_cloned_cluster()
        self.log.info("Destination (cloned) cluster is online")

    def prepare_dest_log_store_uri(self):
        """Compute the destination's fresh Fusion log store URI (and create its
        backing directory for an NFS store), storing it on
        self.dest_fusion_log_store_uri.

        The destination inherits the source's bucket UUIDs, so it must not
        share the source's log store. A dedicated URI can be supplied via the
        dest_fusion_log_store_uri param; otherwise one is derived by suffixing
        the source log store's final path segment. Idempotent: the URI is
        computed (and the NFS dir created) only on the first call, so it is
        safe to invoke both before the remap - to bake it into the config - and
        again before applying it over REST.
        """
        if self.dest_fusion_log_store_uri:
            return self.dest_fusion_log_store_uri

        base_uri = self.fusion_log_store_uri or ""
        region_suffix = ""
        if "?" in base_uri:
            base_uri, region = base_uri.split("?", 1)
            region_suffix = "?" + region
        self.dest_fusion_log_store_uri = \
            base_uri.rstrip("/") + "_dest" + region_suffix

        # For an NFS store, create the backing directory on the NFS server so
        # the derived URI resolves to a real location.
        if self.log_store == "nfs":
            fresh_server_path = self.nfs_server_path.rstrip("/") + "_dest"
            self.log.info(f"Creating fresh NFS log store dir on "
                          f"{self.nfs_server.ip}: {fresh_server_path}")
            ssh = RemoteMachineShellConnection(self.nfs_server)
            try:
                ssh.execute_command(f"mkdir -p {fresh_server_path} "
                                    f"&& chmod -R 0777 {fresh_server_path}")
            finally:
                ssh.disconnect()

        self.log.info(f"Destination Fusion log store URI = "
                      f"{self.dest_fusion_log_store_uri}")
        return self.dest_fusion_log_store_uri

    def dest_log_store_base_uri(self):
        """The destination log store in the format the rebalance/accelerator
        expect: a bare path for an NFS/local store (the "local://" scheme
        stripped, matching config.json's base_uri), or the s3:// URI as-is.
        """
        self.prepare_dest_log_store_uri()
        uri = self.dest_fusion_log_store_uri or ""
        if uri.startswith("local://"):
            return uri[len("local://"):]
        return uri

    def point_dest_at_fresh_log_store(self):
        """Apply the destination's fresh log store URI over REST before Fusion
        is re-enabled.

        node_remap already bakes the URI into the remapped fusion_config, but
        the enable path takes the URI from settings (like configure_fusion), so
        we also set it explicitly. Must run after the self.cluster swap so the
        setting lands on the destination master.
        """
        fresh_uri = self.prepare_dest_log_store_uri()
        self.log.info(f"Pointing destination cluster at fresh log store URI: "
                      f"{fresh_uri}")
        status, content = FusionRestAPI(self.cluster.master).\
            manage_fusion_settings(
                log_store_uri=fresh_uri,
                enable_sync_threshold=self.enable_sync_threshold)
        self.log.info(f"manage_fusion_settings status={status}, content={content}")
        self.assertTrue(status,
                        f"Failed to set fresh log store URI on destination: {content}")
        self.fusion_log_store_uri = fresh_uri

    def wait_for_guest_volume_api(self, timeout=900, interval=10):
        """The /fusion/activeGuestVolumes API fails until every memcached
        instance has come up after the restart. Poll until it responds.
        """
        fusion_rest = FusionRestAPI(self.cluster.master)
        end_time = time.time() + timeout
        while time.time() < end_time:
            status, content = fusion_rest.get_active_guest_volumes()
            if status:
                self.log.info(f"activeGuestVolumes API is ready: {content}")
                return status, content
            self.sleep(interval, "activeGuestVolumes not ready yet, retrying")
        self.fail("activeGuestVolumes API did not become available after restore")

    def enable_fusion_with_retry(self, timeout=900, interval=5):
        """Re-enable Fusion on the restored cluster.

        fusion/enable returns 503 while the cluster is still settling, so
        retry until it is accepted, then wait for the enabled state.
        """
        fusion_rest = FusionRestAPI(self.cluster.master)
        end_time = time.time() + timeout
        enabled_accepted = False
        while time.time() < end_time:
            status, content = fusion_rest.enable_fusion()
            self.log.info(f"fusion/enable status={status}, content={content}")
            if status:
                enabled_accepted = True
                break
            self.sleep(interval, "fusion/enable not accepted yet (cluster settling)")
        if not enabled_accepted:
            self.fail("fusion/enable was not accepted after restore")

        if not self.monitor_fusion_state_transition(state="enabled", timeout=timeout):
            self.fail("Fusion did not reach enabled state after restore")

    def wait_for_migration_to_drain(self, timeout=1800, interval=30,
                                    settle_time=60):
        """Wait until background migration finishes, i.e. no active guest
        volumes remain. Requires two consecutive empty reads so we don't
        return before migration has even started.
        """
        self.sleep(settle_time, "Wait for background migration to start")
        fusion_rest = FusionRestAPI(self.cluster.master)
        end_time = time.time() + timeout
        consecutive_empty = 0
        while time.time() < end_time:
            status, content = fusion_rest.get_active_guest_volumes()
            _, _, count = self.count_active_guest_volumes()
            self.log.info(f"Active guest volumes (waiting to drain): {content}")
            if status and count == 0:
                consecutive_empty += 1
                if consecutive_empty >= 2:
                    self.log.info("Background migration finished, no active "
                                  "guest volumes remain")
                    return
            else:
                consecutive_empty = 0
            self.sleep(interval, "Wait for migration to drain")
        self.fail("Background migration did not finish within timeout")

    def verify_sync_resumed(self, ops_rate=20000, wait_time=120):
        """Prove Fusion sync resumed after restore by mutating documents and
        confirming the cluster-wide sync count increases.
        """
        _, _, syncs_before = self.get_fusion_sync_stats()
        self.log.info(f"Total syncs before post-restore workload: {syncs_before}")

        self.log.info("Performing a create workload to generate sync activity")
        self.perform_workload(self.num_items, self.num_items * 2, doc_op="create",
                              ops_rate=ops_rate)

        self.sleep(wait_time, "Wait to observe sync progress after restore")

        _, _, syncs_after = self.get_fusion_sync_stats()
        self.log.info(f"Total syncs after post-restore workload: {syncs_after}")

        self.assertGreater(syncs_after, syncs_before,
                           "Fusion sync did not resume after restore")
        self.log.info("Fusion sync resumed after restore")

    # ------------------------------------------------------------------ #
    # Tests
    # ------------------------------------------------------------------ #

    def test_backup_restore_with_guest_volumes(self):
        """Variation 1: active guest volumes present across the clone.

        - load data on the source cluster
        - pause migration (rate limit 0)
        - rebalance-out a node so its vbuckets migrate onto the survivor as
          guest volumes; with migration paused they stay active
        - clone the source cluster onto the destination nodes via node_remap
          and make the destination the active cluster
        - verify sync resumed, active guest volumes recovered, migration
          resumes when the rate limit is restored, and a subsequent rebalance
          works (all on the cloned destination cluster)

        Note: we rebalance OUT (rather than rebalance-in) to create the guest
        volumes. With a rebalance-in the rate-limit=0 only lands on the new
        node after it joins, briefly letting migration drain on a small
        dataset. Pinning the rate limit to 0 and then removing a node
        guarantees guest volumes are still active at snapshot time.
        """
        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Pause migration so guest volumes don't drain before the snapshot
        self.log.info("Setting migration rate limit to 0 (pause migration)")
        ClusterRestAPI(self.cluster.master).\
            manage_global_memcached_setting(fusion_migration_rate_limit=0)

        # Rebalance-out a node -> survivor mounts its volumes as guest volumes
        self.num_nodes_to_rebalance_in = 0
        self.num_nodes_to_rebalance_out = 1
        self.num_nodes_to_swap_rebalance = 0
        self.log.info("Rebalancing out a node to create active guest volumes")
        self.run_rebalance(output_dir=self.fusion_output_dir)
        self.sleep(30, "Wait after rebalance-out")

        self.cluster_util.print_cluster_stats(self.cluster)

        # Confirm guest volumes are active on the source before the snapshot
        _, content, count = self.count_active_guest_volumes()
        self.log.info(f"Active guest volumes before clone (source): {content}")
        self.assertGreater(count, 0,
                           "Expected active guest volumes before clone, found none")

        # Clone the source cluster onto the destination nodes; self.cluster is
        # repointed at the cloned destination cluster after this returns.
        self.clone_cluster_via_node_remap()

        # API is unavailable until all memcached instances are up
        self.wait_for_guest_volume_api()

        # Give the destination its own log store so it doesn't collide with the
        # source (both carry the same bucket UUIDs after the clone).
        self.point_dest_at_fresh_log_store()

        # Active guest volumes should have recovered on the clone after re-enable
        _, content, count = self.count_active_guest_volumes()
        self.log.info(f"Active guest volumes after clone (destination): {content}")
        self.assertGreater(count, 0,
                           "Active guest volumes did not recover on the clone")

        # Read back every document on the destination while the guest volumes
        # are still active, confirming data is fully served from them before
        # migration resumes.
        self.log.info("Performing a read workload on all {} items on the "
                      "destination cluster".format(self.num_items))
        self.perform_workload(0, self.num_items, doc_op="read")

        # Migration should resume once the rate limit is restored
        self.log.info("Restoring migration rate limit to resume migration")
        ClusterRestAPI(self.cluster.master).\
            manage_global_memcached_setting(
                fusion_migration_rate_limit=self.fusion_migration_rate_limit)

        self.log.info("Monitoring active guest volumes drain (migration resumed)")
        guest_volume_th = threading.Thread(target=self.monitor_active_guest_volumes)
        guest_volume_th.start()
        guest_volume_th.join()

        # Cloned cluster boots with Fusion disabled; re-enable it
        self.enable_fusion_with_retry()

        # Sync should resume
        self.verify_sync_resumed()

        # A subsequent rebalance should work (add a node back)
        self.num_nodes_to_rebalance_in = 1
        self.num_nodes_to_rebalance_out = 0
        self.num_nodes_to_swap_rebalance = 0
        self.log.info("Running a subsequent Fusion rebalance (rebalance-in)")
        self.run_rebalance(output_dir=self.fusion_output_dir, rebalance_count=2,
                           log_store=self.log_store,
                           log_store_uri=self.dest_log_store_base_uri())
        self.cluster_util.print_cluster_stats(self.cluster)

        self.log.info("Monitoring active guest volumes")
        guest_volume_th = threading.Thread(target=self.monitor_active_guest_volumes)
        guest_volume_th.start()
        guest_volume_th.join()

    def test_backup_restore_without_guest_volumes(self):
        """Variation 2: no active guest volumes across the clone.

        - load data on the source cluster
        - rebalance-in a node
        - wait for background migrations to finish (no active guest volumes)
        - clone the source cluster onto the destination nodes via node_remap
          and make the destination the active cluster
        - verify sync resumed, no active guest volumes recover, and a
          subsequent rebalance works (all on the cloned destination cluster)
        """
        self.log.info("Starting initial load")
        self.initial_load()
        sleep_time = 120 + self.fusion_upload_interval + 30
        self.sleep(sleep_time, "Sleep after data loading")

        # Rebalance-in a node at the default migration rate so it drains
        self.num_nodes_to_rebalance_in = 1
        self.num_nodes_to_rebalance_out = 0
        self.num_nodes_to_swap_rebalance = 0
        self.log.info("Rebalancing in a node")
        self.run_rebalance(output_dir=self.fusion_output_dir)

        self.cluster_util.print_cluster_stats(self.cluster)

        # Wait for background migrations to finish (guest volumes drain)
        self.log.info("Waiting for background migrations to finish")
        self.wait_for_migration_to_drain()

        _, content, count = self.count_active_guest_volumes()
        self.log.info(f"Active guest volumes before clone (source): {content}")
        self.assertEqual(count, 0,
                         f"Expected no active guest volumes before clone, found {content}")

        # Clone the source cluster onto the destination nodes; self.cluster is
        # repointed at the cloned destination cluster after this returns.
        self.clone_cluster_via_node_remap()

        # API is unavailable until all memcached instances are up
        self.wait_for_guest_volume_api()

        # Give the destination its own log store so it doesn't collide with the
        # source (both carry the same bucket UUIDs after the clone).
        self.point_dest_at_fresh_log_store()

        # Cloned cluster boots with Fusion disabled; re-enable it
        self.enable_fusion_with_retry()

        # No active guest volumes should recover (migration had already drained)
        self.sleep(60, "Wait to confirm no guest volumes recover")
        _, content, count = self.count_active_guest_volumes()
        self.log.info(f"Active guest volumes after clone (destination): {content}")
        self.assertEqual(count, 0,
                         f"Expected no active guest volumes after clone, found {content}")

        # Sync should resume
        self.verify_sync_resumed()

        # A subsequent rebalance should work (remove a node)
        self.num_nodes_to_rebalance_in = 0
        self.num_nodes_to_rebalance_out = 1
        self.num_nodes_to_swap_rebalance = 0
        self.log.info("Running a subsequent Fusion rebalance (rebalance-out)")
        self.run_rebalance(output_dir=self.fusion_output_dir, rebalance_count=2,
                           log_store=self.log_store,
                           log_store_uri=self.dest_log_store_base_uri())
        self.cluster_util.print_cluster_stats(self.cluster)
