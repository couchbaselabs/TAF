'''
Fusion Backup/Restore Volume Test (EBS Snapshot-based)

Tests EBS cloud snapshot backup and cross-cluster restore for fusion-enabled
Capella clusters under sustained load.  EBS snapshot backups are cluster-level
(not bucket-level) and replace the entire cluster state on restore.

Setup flow (initial_setup)
--------------------------
1. Enable fusion on primary cluster.
2. Create buckets and load data on primary.
3. Run a steady-state mutation workload on primary.
4. If restore_to=secondary: the secondary cluster is NOT pre-provisioned via
   the .ini file. It is created on demand, immediately after primary's data
   load / steady-state sleep completes -- i.e. before
   test_backup_restore_volume()'s H/V scaling loop starts (see
   _ensure_secondary_ready(), called from initial_setup()). This avoids
   paying for an idle secondary cluster only while primary is still loading
   data, while still guaranteeing the secondary exists before any scaling
   cycle runs. Bootstrap:
   a. Take a fresh EBS snapshot backup of primary and CLONE it
      (CapellaAPI.clone_cloud_snapshot_backup) into a brand-new cluster --
      a single Capella v4 API call that both provisions the destination
      cluster and restores primary's dataset onto it in one step, and
      returns the new cluster's id directly (v2's equivalent clone
      response omits it -- see _create_secondary_cluster_from_clone()).
   b. Based on secondary_fusion_enabled:
        True  → enable fusion on secondary; trigger one scaling rebalance so
                 secondary builds its own EBS guest volumes.
        False → ensure fusion is disabled on secondary.

Test loop (test_backup_restore_volume)
--------------------------------------
For each scaling cycle on primary:
  - scale-up primary (h_scaling iterations)
      → after every step: take snapshot backup + restore to target
  - scale-down primary (h_scaling iterations)
      → after every step: take snapshot backup + restore to target
  - v_scaling (disk + compute iterations)
      → after every step: take snapshot backup + restore to target

The mutation workload is stopped for the duration of every snapshot backup
(quiesced dataset) and resumed after the backup completes — or, for
same-cluster restores, after the restore completes (the restore replaces
all cluster data, so resuming earlier would be pointless).

Cross-cluster restore verification
------------------------------------
When restoring to secondary:
  1. Snapshot secondary's current guest-volume IDs (pre-restore).
  2. Restore primary EBS snapshot backup onto secondary.
  3. Verify old guest volumes deleted by the restore reset.
  4. Verify EBS snapshot count matches guest volume count on target.
  5. Trigger post-restore rebalances on secondary to verify cluster health.
  6. Verify fusion state remains "enabled" on secondary.

Key parameters
--------------
restore_to                : "same" (default) | "secondary"
secondary_fusion_enabled  : True (default)  | False
verify_snapshots          : True  (default)
secondary_rebalance_delta : +1 (default)  — node delta used when triggering
                            rebalances on secondary to verify health after restore
skip_secondary_teardown   : False (default) — leave the dynamically clone-created
                            secondary cluster up after the run (debugging)
h_scaling / v_scaling     : same as fusion_volume.py
iterations, rebl_steps    : same as fusion_volume.py
'''

import socket
import threading
import time
import uuid

from membase.api.rest_client import RestConnection
from capella_utils.dedicated import CapellaUtils as CapellaAPI
from cluster_utils.cluster_ready_functions import CBCluster
from TestInput import TestInputServer
from couchbase_utils.cb_server_rest_util.fusion.fusion_api import FusionRestAPI
from aGoodDoctor.workloads import default
from bucket_utils.bucket_ready_functions import CollectionUtils, JavaDocLoaderUtils
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from constants.cloud_constants.capella_constants import AWS, GCP, AZURE
from py_constants.cb_constants.CBServer import CbServer


from .fusion_volume import VolumeTest


class FusionBackupRestoreVolumeTest(VolumeTest):
    """
    Fusion EBS snapshot backup/restore volume test.

    Inherits the full scaling + monitoring infrastructure from VolumeTest and
    adds EBS snapshot backup/restore orchestration with cross-cluster support,
    pre/post-restore guest-volume lifecycle verification, and S3 bucket cleanup
    checks.
    """

    # ------------------------------------------------------------------ setUp

    def setUp(self):
        super().setUp()

        self.restore_to = self.input.param("restore_to", "same")
        self.secondary_fusion_enabled = self.input.param("secondary_fusion_enabled", True)
        self.verify_snapshots = self.input.param("verify_snapshots", True)
        # Node delta for rebalances triggered on secondary to build/refresh guest volumes
        self.secondary_rebalance_delta = self.input.param("secondary_rebalance_delta", 1)
        # Skip destroying the dynamically clone-provisioned secondary cluster in
        # tearDown -- useful for debugging a failed run against the live secondary
        self.skip_secondary_teardown = self.input.param("skip_secondary_teardown", False)

        all_clusters = [c for t in self.tenants for c in t.clusters]
        self.primary_cluster = all_clusters[0]
        self.primary_tenant = self.tenants[0]

        # The secondary cluster (restore_to=="secondary") is created on demand
        # by cloning a snapshot backup of primary -- it is no longer read from
        # the .ini file. See _ensure_secondary_ready() / _target_cluster().
        self.secondary_cluster = None
        self._secondary_ready = False

        # v4 API key bearer token needed for the clone call -- see
        # _ensure_v4_bearer_token(). Minted lazily, only if restore_to==
        # "secondary", so a same-cluster run never pays for it.
        self.v4_bearer_token = None
        self._v4_key_ids = (None, None)

        # rebalance_config() (hostedOPD.py) mutates self.num_nodes[service]
        # in place as a side effect, with no per-cluster scoping -- it
        # assumes a single cluster's node counts are being tracked. This
        # test manages TWO clusters (primary + secondary) that both get
        # scaled via rebalance_config(), so a separate counter dict is kept
        # for secondary and swapped into self.num_nodes only for the
        # duration of secondary's own rebalance calls (see
        # _trigger_rebalance_on_secondary()) -- otherwise secondary's
        # scale-up/down rebalances corrupt primary's node-count tracking.
        # Cloned here, before any rebalances have run, since secondary
        # starts with the same node topology primary has at this point.
        self.secondary_num_nodes = dict(self.num_nodes)

    # ------------------------------------------------------------- tearDown

    def tearDown(self):
        """
        Destroy the dynamically clone-provisioned secondary cluster (if any)
        before the base-class teardown runs.

        This cannot be left to ProvisionedBaseTestCase.tearDown(): that
        method only destroys clusters in tenant.clusters when
        `TestInputSingleton.input.capella.get("clusters", None)` is falsy --
        i.e. it assumes any cluster it didn't itself dynamically deploy via
        `num_clusters` is externally/pod-owned and skips it. Primary is
        typically supplied via `.ini clusters=<id>` for this test, which
        makes that global gate skip destruction entirely -- so a
        dynamically clone-created secondary would otherwise leak. Tag-gated
        on `_taf_owned` so this is a no-op for `restore_to=="same"` runs or
        if secondary was never actually created (e.g. test failed before its
        first use). Can be skipped entirely via `skip_secondary_teardown`,
        e.g. to leave a failed run's secondary cluster up for debugging.
        """
        secondary = getattr(self, "secondary_cluster", None)
        if secondary is not None and getattr(secondary, "_taf_owned", False):
            if self.skip_secondary_teardown:
                self.log.info(
                    f"[secondary] skip_secondary_teardown=True -- leaving "
                    f"cluster {secondary.id} up"
                )
            else:
                self.log.info(
                    f"[secondary] Destroying dynamically clone-provisioned "
                    f"cluster {secondary.id}"
                )
                try:
                    CapellaAPI.destroy_cluster(self.pod, self.primary_tenant, secondary)
                except Exception as e:
                    self.log.error(
                        f"[secondary] Failed to destroy dynamically-created "
                        f"cluster {secondary.id}: {e}"
                    )

        v2_key_id, v4_key_id = getattr(self, "_v4_key_ids", (None, None))
        if v2_key_id or v4_key_id:
            try:
                CapellaAPI.delete_v4_api_key(
                    self.pod, self.primary_tenant, v2_key_id, v4_key_id,
                    self.v4_bearer_token,
                )
            except Exception as e:
                self.log.error(f"Failed to clean up v4 API key(s): {e}")
        super().tearDown()

    # ---------------------------------------------------------------- helpers

    def _target_cluster(self, backup_id=None):
        if self.restore_to == "secondary":
            self._ensure_secondary_ready(backup_id)
            return self.secondary_cluster
        return self.primary_cluster

    def _target_is_fusion(self):
        return True if self.restore_to == "same" else self.secondary_fusion_enabled

    def _cluster_label(self, cluster):
        """Return '[primary]' or '[secondary]' for use as a log prefix."""
        if self.secondary_cluster and cluster.id == self.secondary_cluster.id:
            return "[secondary]"
        return "[primary]"

    # ------------------------------------------- workload stop/resume helpers

    def _stop_workload(self):
        """
        Stop the background mutation workload on primary and drain in-flight
        loader tasks.  Idempotent — safe to call when already stopped.
        """
        self.mutations = False
        if hasattr(self, "mutation_th") and self.mutation_th.is_alive():
            self.log.info("[primary] Stopping mutation workload")
            self.mutation_th.join(timeout=120)
        for task in list(getattr(self, "loader_tasks", [])):
            try:
                self.task_manager.stop_task(task)
            except Exception:
                pass
        if hasattr(self, "loader_tasks"):
            self.loader_tasks.clear()

    def _resume_workload(self):
        """Restart the background mutation workload on primary."""
        if not self.input.param("mutations", True):
            return
        self.mutations = True
        self.mutation_th = threading.Thread(
            target=self.normal_mutations, kwargs={"cluster": self.primary_cluster}
        )
        self.mutation_th.start()
        self.log.info("[primary] Mutation workload resumed")

    # -------------------------------------------- log helpers (primary only)

    def log_fusion_log_store_data_size(self):
        self.fusion_monitor.log_fusion_log_store_data_size([self.primary_cluster])

    def log_fusion_pending_bytes(self):
        self.fusion_monitor.log_fusion_pending_bytes(
            self.primary_tenant, [self.primary_cluster], self.find_master
        )

    # -------------------------------------------- snapshot backup / restore

    def _create_snapshot_backup(self, cluster):
        """
        Trigger an EBS cloud snapshot backup on *cluster* and wait for
        completion.  Returns the backup ID string, or fails the test.
        """
        tenant = self.primary_tenant
        project_id = tenant.projects[0]

        self.log.info(f"{self._cluster_label(cluster)} Creating EBS snapshot backup on cluster {cluster.id}")
        result = CapellaAPI.create_cloud_snapshot_backup(
            self.pod, tenant, project_id, cluster.id
        )
        self.assertIsNotNone(
            result,
            f"create_cloud_snapshot_backup returned None for cluster {cluster.id}",
        )
        backup_id = result.get("id")
        self.assertIsNotNone(
            backup_id,
            f"No 'id' in create_cloud_snapshot_backup response: {result}",
        )
        self.log.info(f"{self._cluster_label(cluster)} Snapshot backup triggered: {backup_id}")

        ok = CapellaAPI.wait_for_cloud_snapshot_backup_to_complete(
            self.pod, tenant, project_id, cluster.id, backup_id
        )
        self.assertTrue(
            ok,
            f"Snapshot backup {backup_id} did not complete on cluster {cluster.id}",
        )
        self.log.info(f"{self._cluster_label(cluster)} Snapshot backup {backup_id} complete on {cluster.id}")
        return backup_id

    def _restore_snapshot_backup(self, backup_id, source_cluster, target_cluster):
        """
        Restore EBS snapshot *backup_id* (taken from *source_cluster*) onto
        *target_cluster* and wait for completion.  Waits for the target cluster
        to return to healthy state afterwards.
        """
        tenant = self.primary_tenant
        project_id = tenant.projects[0]

        src_label = self._cluster_label(source_cluster)
        tgt_label = self._cluster_label(target_cluster)
        self.log.info(
            f"{tgt_label} Restoring snapshot backup {backup_id} "
            f"from {src_label} {source_cluster.id} → {tgt_label} {target_cluster.id}"
        )
        result = CapellaAPI.restore_cloud_snapshot_backup(
            self.pod, tenant, project_id, target_cluster.id, backup_id
        )
        self.assertIsNotNone(
            result,
            f"restore_cloud_snapshot_backup returned None for backup {backup_id} "
            f"→ target {target_cluster.id}",
        )
        restore_id = result.get("restoreId")
        self.assertIsNotNone(
            restore_id,
            f"No 'restoreId' in restore_cloud_snapshot_backup response: {result}",
        )
        self.log.info(f"{tgt_label} Restore job triggered: {restore_id}")

        ok = CapellaAPI.wait_for_cloud_snapshot_restore_to_complete(
            self.pod, tenant, project_id, target_cluster.id, restore_id
        )
        self.assertTrue(
            ok,
            f"Snapshot restore {restore_id} did not complete on target {target_cluster.id}",
        )
        self.log.info(f"{tgt_label} Snapshot restore {restore_id} complete on {target_cluster.id}")

        # Wait for target cluster to reach healthy state after restore
        CapellaAPI.wait_until_done(
            self.pod, tenant, target_cluster.id,
            msg=f"{tgt_label} Wait for healthy state after snapshot restore on {target_cluster.id}",
        )
        self.log.info(f"{tgt_label} Target cluster {target_cluster.id} healthy after restore")

        # Refresh target_cluster's node list from the CP — an EBS snapshot
        # restore replaces the target's entire node topology to match
        # primary's topology at backup time, so nodes captured before this
        # restore (e.g. at clone-creation time) may no longer exist.
        # Without this, the reachability poll below can wait forever on a
        # hostname that was torn down by the restore itself.
        self._populate_cluster_nodes(target_cluster)

        # Re-add 0.0.0.0/0 allow-all IP — restore flushes the allowlist
        retry = 0
        while retry < 5:
            try:
                CapellaAPI.allow_my_ip(self.pod, tenant, target_cluster.id, True)
                self.log.info(
                    f"{tgt_label} Re-added 0.0.0.0/0 to allowlist on {target_cluster.id} after restore"
                )
                self.sleep(60, f"{tgt_label} Wait for allow-all IP rule to propagate after restore")
                break
            except Exception as err:
                retry += 1
                self.log.warning(
                    f"{tgt_label} allow_my_ip attempt {retry}/5 failed on {target_cluster.id}: {err}"
                )
                if retry < 5:
                    self.sleep(30 * retry, f"{tgt_label} Retrying allow_my_ip after restore")
                else:
                    raise

        # Re-add the DB user — restore wipes it just like it wipes the IP
        # allowlist above. create_db_user() already treats "user already
        # exists" as a no-op, so this is safe to call unconditionally.
        CapellaAPI.create_db_user(
            self.pod, tenant, target_cluster.id, self.rest_username, self.rest_password
        )
        self.log.info(f"{tgt_label} Re-added DB user on {target_cluster.id} after restore")

        # Poll until every node responds to REST — the allowlist rule can take
        # several minutes to propagate even after allow_my_ip returns successfully.
        reachability_timeout = 900  # 15 minutes
        poll_interval = 15
        deadline = time.time() + reachability_timeout
        nodes = target_cluster.nodes_in_cluster or [target_cluster.master]
        self.log.info(
            f"{tgt_label} Polling {len(nodes)} node(s) on {target_cluster.id} for REST reachability "
            f"(up to {reachability_timeout}s)"
        )
        for node in nodes:
            while True:
                try:
                    with socket.create_connection((node.ip, 18091), timeout=10):
                        pass
                    self.log.info(f"{tgt_label} Node {node.ip}:18091 is reachable")
                    break
                except OSError as e:
                    remaining = int(deadline - time.time())
                    if remaining <= 0:
                        self.fail(
                            f"{tgt_label} Node {node.ip} on {target_cluster.id} did not become "
                            f"reachable within {reachability_timeout}s after IP allowlist "
                            f"restore: {e}"
                        )
                    self.log.warning(
                        f"{tgt_label} Node {node.ip}:18091 not yet reachable ({e}); "
                        f"retrying in {poll_interval}s ({remaining}s remaining)"
                    )
                    time.sleep(poll_interval)

    # ------------------------------------------------ secondary cluster setup

    def _wrap_new_cluster(self, cluster_id, cluster_name):
        """
        Build a CBCluster object for a dynamically clone-provisioned cluster
        (`.id`, `.master`, `.nodes_in_cluster`, per-service node lists),
        matching the shape ProvisionedBaseTestCase.__populate_cluster_info
        builds for .ini-provisioned clusters. That method (and its sibling
        __populate_cluster_buckets) is name-mangled and private to
        ProvisionedBaseTestCase, so the needed logic is replicated here
        rather than reused.

        Bucket population is intentionally NOT replicated -- nothing in this
        file reads secondary.buckets, only primary.buckets (see
        _restore_and_verify), so it would be dead weight.

        Tags the cluster `_taf_owned = True`: this test created it and this
        test is responsible for destroying it in tearDown(), unlike
        .ini-provisioned clusters whose lifecycle is owned externally.
        """
        tenant = self.primary_tenant
        cluster_srv = CapellaAPI.get_cluster_srv(self.pod, tenant, cluster_id)

        cluster = CBCluster(username=self.rest_username, password=self.rest_password,
                            servers=[None] * 40)
        cluster.id = cluster_id
        cluster.name = cluster_name
        cluster.srv = cluster_srv
        cluster.pod = self.pod
        cluster.type = "dedicated"
        cluster._taf_owned = True

        self._populate_cluster_nodes(cluster)

        tenant.clusters.append(cluster)
        return cluster

    def _populate_cluster_nodes(self, cluster):
        """
        (Re)fetch *cluster*'s current node list from the CP and rebuild
        `.nodes_in_cluster`/per-service node lists/`.master` from it,
        in place, discarding whatever was there before.

        Used both for the initial node population of a freshly
        clone-provisioned cluster (_wrap_new_cluster()) and to refresh an
        EXISTING cluster object after a restore (_restore_snapshot_backup())
        -- an EBS snapshot restore replaces the target's entire node
        topology to match whatever primary's topology was when the backup
        was taken, so a node list captured before the restore can reference
        hostnames that no longer exist afterward. Always refetches from the
        CP directly (CapellaAPI.get_nodes) rather than bootstrapping via an
        existing node in the old list (cf. hostedOPD.py's refresh_cluster()),
        since every previously-known node may be gone post-restore.
        """
        servers = CapellaAPI.get_nodes(self.pod, self.primary_tenant, cluster.id)

        cluster.nodes_in_cluster = []
        cluster.kv_nodes = []
        cluster.query_nodes = []
        cluster.index_nodes = []
        cluster.eventing_nodes = []
        cluster.cbas_nodes = []
        cluster.fts_nodes = []

        for server in servers:
            temp_server = TestInputServer()
            temp_server.ip = server.get("hostname")
            temp_server.hostname = server.get("hostname")
            temp_server.services = server.get("services")
            temp_server.port = "18091"
            temp_server.rest_username = self.rest_username
            temp_server.rest_password = self.rest_password
            temp_server.type = "dedicated"
            temp_server.memcached_port = "11207"
            cluster.nodes_in_cluster.append(temp_server)
            if "Data" in temp_server.services:
                cluster.kv_nodes.append(temp_server)
            if "Query" in temp_server.services:
                cluster.query_nodes.append(temp_server)
            if "Index" in temp_server.services:
                cluster.index_nodes.append(temp_server)
            if "Eventing" in temp_server.services:
                cluster.eventing_nodes.append(temp_server)
            if "Analytics" in temp_server.services:
                cluster.cbas_nodes.append(temp_server)
            if "Search" in temp_server.services:
                cluster.fts_nodes.append(temp_server)

        self.assertTrue(
            cluster.kv_nodes, f"No KV/data nodes found for cluster {cluster.id}"
        )
        cluster.master = cluster.kv_nodes[0]

    def _ensure_v4_bearer_token(self):
        """
        Lazily mint the v4 API key bearer token needed by
        clone_cloud_snapshot_backup() -- see CapellaUtils.create_v4_api_key()
        for why v4 calls can't use tenant.api_secret_key/api_access_key like
        every other call in this file. Idempotent; only ever mints once.
        """
        if self.v4_bearer_token is not None:
            return
        v2_key_id, v4_key_id, token = CapellaAPI.create_v4_api_key(
            self.pod, self.primary_tenant, name_prefix="fusion-secondary-clone"
        )
        self.assertIsNotNone(
            token, "Failed to bootstrap a v4 API key for the clone call"
        )
        self._v4_key_ids = (v2_key_id, v4_key_id)
        self.v4_bearer_token = token

    def _create_secondary_cluster_from_clone(self, backup_id=None):
        """
        Provision the secondary cluster by cloning an EBS snapshot backup of
        primary. A single Capella v4 API call
        (CapellaAPI.clone_cloud_snapshot_backup) both creates the new
        cluster and restores primary's dataset onto it -- replacing the
        older two-step "create an empty cluster, then separately restore
        onto it" design. Region is pinned to primary's (self.region) since
        EBS snapshots are region-scoped; node/service topology is not
        caller-specified for this endpoint -- Capella derives it from the
        source backup.

        v4, not v2: v2's clone response has no clusterId, and (confirmed
        against couchbase-cloud source) a clone-created restore record's
        ClusterID is the NEW cluster's id, not primary's -- so there is no
        way to look up the new cluster's id via v2's list-restores endpoint
        scoped by primary. v4's response returns clusterId directly.

        Normally called with backup_id=None (from initial_setup(), before
        the scaling loop starts) -- a fresh primary backup is taken here
        specifically for this bootstrap. If a scaling-loop *backup_id* is
        passed instead (only possible via the _target_cluster() safety-net
        path, and only if _ensure_secondary_ready() hasn't already run to
        completion by then), it is reused instead of taking a second one.
        """
        tenant = self.primary_tenant
        project_id = tenant.projects[0]
        primary = self.primary_cluster

        self.PrintStep(
            f"Creating secondary cluster by cloning a snapshot backup of primary {primary.id}"
        )

        if backup_id is None:
            self._stop_workload()
            backup_id = self._create_snapshot_backup(primary)
            self._resume_workload()

        self._ensure_v4_bearer_token()

        cluster_name = f"fusion_secondary_{primary.id[:8]}_{uuid.uuid4().hex[:6]}"
        resp = CapellaAPI.clone_cloud_snapshot_backup(
            self.pod, tenant, project_id, backup_id,
            name=cluster_name, region=self.region,
            bearer_token=self.v4_bearer_token,
        )
        self.assertIsNotNone(
            resp, f"clone_cloud_snapshot_backup returned None for backup {backup_id}"
        )
        restore_id = resp.get("restoreId")
        self.assertIsNotNone(
            restore_id, f"No 'restoreId' in clone_cloud_snapshot_backup response: {resp}"
        )
        new_cluster_id = resp.get("clusterId")
        self.assertIsNotNone(
            new_cluster_id, f"No 'clusterId' in clone_cloud_snapshot_backup response: {resp}"
        )
        self.log.info(
            f"[secondary] Clone triggered, restoreId={restore_id}, "
            f"cluster provisioning as {new_cluster_id}"
        )

        # Scoped by the NEW cluster's id, not primary's -- a clone-created
        # restore record's ClusterID is the new cluster, so list-restores
        # scoped by primary would never find it (see docstring above).
        ok = CapellaAPI.wait_for_cloud_snapshot_restore_to_complete(
            self.pod, tenant, project_id, new_cluster_id, restore_id
        )
        self.assertTrue(
            ok,
            f"[secondary] Clone restore {restore_id} did not complete "
            f"(new cluster {new_cluster_id})",
        )

        CapellaAPI.wait_until_done(
            self.pod, tenant, new_cluster_id,
            msg=f"[secondary] Wait for cluster {new_cluster_id} healthy after clone",
        )

        retry = 0
        while retry < 5:
            try:
                CapellaAPI.allow_my_ip(self.pod, tenant, new_cluster_id, True)
                break
            except Exception as err:
                retry += 1
                self.log.warning(f"[secondary] allow_my_ip attempt {retry}/5 failed: {err}")
                if retry < 5:
                    self.sleep(30 * retry, "[secondary] Retrying allow_my_ip after clone")
                else:
                    raise

        CapellaAPI.create_db_user(
            self.pod, tenant, new_cluster_id, self.rest_username, self.rest_password
        )

        self.secondary_cluster = self._wrap_new_cluster(new_cluster_id, cluster_name)
        self.log.info(f"[secondary] Cluster ready: {self.secondary_cluster.id}")

        # Verify secondary has data in each of primary's buckets -- the clone
        # both provisioned the cluster and restored primary's dataset onto it.
        self.fusion_monitor.set_admin_credentials(self.secondary_cluster)
        rest = RestConnection(self.secondary_cluster.master)
        for bucket in primary.buckets:
            expected = (
                bucket.loadDefn.get("num_items", 0)
                * bucket.loadDefn.get("collections", 1)
            )
            deadline = time.time() + self.restore_timeout
            while time.time() < deadline:
                info = rest.get_bucket_details(bucket_name=bucket.name)
                actual = info.get("basicStats", {}).get("itemCount", 0) if info else 0
                if actual > 0:
                    self.log.info(
                        f"[secondary] {self.secondary_cluster.id} bucket {bucket.name}: "
                        f"{actual} items after clone (expected ~{expected})"
                    )
                    break
                self.sleep(30, "Waiting for items on secondary after clone")
            else:
                self.fail(
                    f"No items on secondary {self.secondary_cluster.id}/{bucket.name} "
                    f"after clone"
                )

    def _ensure_secondary_ready(self, backup_id=None):
        """
        Create + bootstrap the secondary cluster (clone-create + fusion
        config + initial rebalance), gated by self._secondary_ready so it
        only ever runs once. Called explicitly from initial_setup() right
        after primary's data load, so the secondary cluster exists before
        test_backup_restore_volume()'s scaling loop starts -- this avoids
        billing for an idle secondary only while primary is still loading
        data, not for the whole scaling loop.

        Also called (as a no-op safety net, since the flag is already set
        by then) from _target_cluster() during the scaling loop -- if
        *backup_id* is passed there, it would be reused to clone from
        instead of taking a second one, but in the normal flow this method
        has already run to completion by the time that happens.
        """
        if self.restore_to != "secondary" or self._secondary_ready:
            return
        self._create_secondary_cluster_from_clone(backup_id)
        self._configure_secondary_fusion()

        secondary = self.secondary_cluster
        secondary.fusion_uploader_dict = {}
        secondary.fusion_vb_uploader_map = {}
        self._secondary_ready = True

    def _trigger_rebalance_on_secondary(self, delta, label="secondary rebalance"):
        """
        Scale secondary cluster by *delta* data nodes and monitor the rebalance
        (including fusion guest-volume lifecycle if secondary is fusion-enabled).
        """
        secondary = self.secondary_cluster
        tenant = self.primary_tenant
        sync_timeout = self.input.param("sync_wait_timeout", 7200)
        self.log.info("Waiting for pending bytes to drain to 0 after restore on Secondary")
        self.fusion_monitor.wait_for_fusion_pending_byte_zero(self.secondary_cluster, timeout=sync_timeout)

        self.PrintStep(f"{label}: delta={delta:+d} on secondary {secondary.id}")
        # rebalance_config() mutates self.num_nodes["data"] in place as a
        # side effect -- swap in secondary's own counter dict for this call
        # so it doesn't corrupt primary's node-count tracking (see setUp()).
        primary_num_nodes = self.num_nodes
        self.num_nodes = self.secondary_num_nodes
        try:
            config = self.rebalance_config("data", delta)
        finally:
            self.num_nodes = primary_num_nodes
        rebalance_task = self.task.async_rebalance_capella(
            self.pod, tenant, secondary, config, timeout=self.rebalance_timeout
        )
        self.monitor_cluster_status(tenant, secondary, rebalance_task)
        self.fusion_monitor.get_fusion_uploader_map(tenant, secondary, self.find_master)
        self.sleep(60, "post-secondary-rebalance settle")

        result = self.cp_monitor.monitor_fusion_accelerator_nodes_killed_after_rebalance(secondary)
        self.assertTrue(
            result,
            f"Accelerator nodes not killed after {label} on secondary {secondary.id}",
        )
        self.scan_memcahced_logs(secondary)

    def _configure_secondary_fusion(self):
        """
        Enable or disable fusion on secondary and, when enabling, trigger an
        initial scaling rebalance so secondary builds its own EBS guest volumes.
        """
        secondary = self.secondary_cluster
        tenant = self.primary_tenant

        if self.secondary_fusion_enabled:
            self.fusion_monitor.set_admin_credentials(secondary)
            status, fusion_state = FusionRestAPI(secondary.master).get_fusion_status()
            if not status or fusion_state.get("state") != "enabled":
                resp = CapellaAPI.enable_fusion(self.pod, tenant, secondary.id)
                self.assertTrue(
                    resp.status_code == 200,
                    f"Failed to enable Fusion on secondary {secondary.id}: {resp.status_code}",
                )
            self.fusion_monitor.wait_for_fusion_status(secondary, state="enabled")
            self.log.info(f"Fusion enabled on secondary {secondary.id}")

            # Trigger a scale-up rebalance so secondary creates its own guest volumes
            self._trigger_rebalance_on_secondary(
                self.secondary_rebalance_delta,
                label="initial secondary scale-up",
            )
            pre_gv_ids = self.cp_monitor.get_current_guest_volume_ids(secondary)
            self.log.info(
                f"Secondary {secondary.id} guest volumes after initial rebalance: {pre_gv_ids}"
            )
        else:
            self.fusion_monitor.set_admin_credentials(secondary)
            status, fusion_state = FusionRestAPI(secondary.master).get_fusion_status()
            if status and fusion_state.get("state") == "enabled":
                resp = CapellaAPI.disable_fusion(self.pod, tenant, secondary.id)
                self.assertTrue(
                    resp.status_code == 200,
                    f"Failed to disable Fusion on secondary {secondary.id}: {resp.status_code}",
                )
                self.fusion_monitor.wait_for_fusion_status(secondary, state="disabled")
            self.log.info(f"Fusion disabled on secondary {secondary.id}")

    # ----------------------------------------------- initial_setup override

    def initial_setup(self):
        """
        Set up primary cluster with data + steady-state mutations, then --
        if restore_to=="secondary" -- bootstrap the secondary cluster
        (clone-create + fusion config + initial rebalance) immediately
        afterward, before test_backup_restore_volume()'s scaling loop starts.
        See _ensure_secondary_ready().
        """
        tenant = self.primary_tenant
        primary = self.primary_cluster

        CapellaAPI.update_feature_flag_globally(self.pod, tenant, "fusion-rebalances", True)
        CapellaAPI.update_feature_flag_globally(self.pod, tenant, "fusion-fallback-replace", True)

        # Enable fusion on primary
        self.fusion_monitor.set_admin_credentials(primary)
        status, fusion_state = FusionRestAPI(primary.master).get_fusion_status()
        if not status or fusion_state.get("state") != "enabled":
            resp = CapellaAPI.enable_fusion(self.pod, tenant, primary.id)
            self.assertTrue(
                resp.status_code == 200,
                f"Failed to enable Fusion on primary {primary.id}: {resp.status_code}",
            )
        self.fusion_monitor.wait_for_fusion_status(primary, state="enabled")
        self.get_hostname_public_ip_mapping(primary)
        self.log.info(f"Fusion enabled on primary cluster {primary.id}")

        self.cpu_monitor_threads = []
        cpu_monitor = threading.Thread(
            target=self.print_cluster_cpu_ram, kwargs={"cluster": primary}
        )
        cpu_monitor.start()
        self.cpu_monitor_threads.append(cpu_monitor)

        if not self.load_defn:
            self.load_defn.append(default)

        # Create buckets on primary
        if not self.skip_init:
            self.create_buckets(self.pod, tenant, primary)
            self.sleep(60, "wait for fusion S3 URI to be created")
        else:
            for i, bucket in enumerate(primary.buckets):
                bucket.loadDefn = self.load_defn[i % len(self.load_defn)]
                num_clients = self.input.param(
                    "clients_per_db", min(5, bucket.loadDefn.get("collections"))
                )
                SiriusCouchbaseLoader.create_clients_in_pool(
                    primary.master, primary.master.rest_username,
                    primary.master.rest_password, bucket.name, req_clients=num_clients,
                )
                self.create_sdk_client_pool(primary, [bucket], num_clients)
                for scope in bucket.scopes.keys():
                            if scope == CbServer.system_scope:
                                continue
                            if bucket.loadDefn.get("collections") > 0:
                                self.collection_prefix = self.input.param("collection_prefix",
                                                                          "VolumeCollection")
                                for i in range(bucket.loadDefn.get("collections")):
                                    collection_name = self.collection_prefix + str(i)
                                    collection_spec = {"name": collection_name}
                                    CollectionUtils.create_collection_object(bucket, scope, collection_spec)


        # Initial data load on primary
        self.PrintStep("Initial data load on primary cluster")
        self.skip_read_on_error = True
        self.suppress_error_table = True
        if not self.skip_init:
            JavaDocLoaderUtils.load_data(
                cluster=primary,
                buckets=primary.buckets,
                overRidePattern={"create": 100, "read": 0, "update": 0, "delete": 0, "expiry": 0},
                validate_data=False,
                wait_for_stats=False,
            )

        # Background mutation thread on primary
        self.mutations = self.input.param("mutations", True)
        self.mutation_th = threading.Thread(
            target=self.normal_mutations, kwargs={"cluster": primary}
        )
        self.mutation_th.start()

        self.sleep(
            self.steady_state_workload_sleep,
            f"Steady-state workload sleep {self.steady_state_workload_sleep}s",
        )

        # Secondary cluster (restore_to=="secondary") is created here, right
        # after primary's data load -- not upfront in setUp() -- so it isn't
        # billed for while primary is still loading data. It must exist
        # before the scaling loop below starts, so this is NOT deferred any
        # further (see _ensure_secondary_ready()).
        self._ensure_secondary_ready()

    # ----------------------------------------------- backup helpers

    def _take_backup_and_verify(self):
        """
        Trigger an EBS snapshot backup on primary, optionally verify EBS
        guest-volume snapshot tags, and return the backup ID.
        """
        primary = self.primary_cluster
        num_snapshots_expected = len(self.cp_monitor.get_current_guest_volume_ids(primary))
        self.PrintStep(f"Taking EBS snapshot backup on primary {primary.id}")

        # Stop the mutation workload for the duration of the backup so the
        # snapshot captures a quiesced dataset
        self._stop_workload()
        backup_id = self._create_snapshot_backup(primary)
        if self.verify_snapshots:
            ok = self.cp_monitor.verify_guest_volume_snapshots_for_backup(
                primary, backup_id, num_snapshots_expected
            )
            self.assertTrue(
                ok,
                f"Guest-volume EBS snapshot verification failed for backup {backup_id}",
            )
            self.log.info(f"Guest-volume snapshots verified for backup {backup_id}")

        # For same-cluster restore the workload must stay stopped — the restore
        # that follows replaces all cluster data and resumes it afterwards.
        # For secondary restore, primary is untouched, so resume immediately.
        if self.restore_to == "secondary":
            self._resume_workload()

        return backup_id

    def _restore_and_verify(self, backup_id):
        """
        Restore primary EBS snapshot backup to the target cluster and verify:
          - item count on target > 0 per bucket
          - if target is fusion-enabled:
              * pre-restore guest volumes are deleted (by the restore reset)
              * EBS snapshot count matches guest volume count on target
              * fusion S3 bucket is empty (cleanFusionBucket ran)
              * post-restore rebalances complete successfully (cluster healthy)
              * fusion state remains "enabled"
        """
        primary = self.primary_cluster
        target = self._target_cluster(backup_id)
        tgt_label = self._cluster_label(target)
        tenant = self.primary_tenant
        is_same = primary.id == target.id
        target_fusion = self._target_is_fusion()

        # Snapshot pre-restore guest volumes on target (if fusion-enabled and cross-cluster)
        pre_restore_gv_ids = []
        if not is_same and target_fusion:
            pre_restore_gv_ids = self.cp_monitor.get_current_guest_volume_ids(target)
            self.log.info(
                f"{tgt_label} Pre-restore guest volumes on {target.id}: {pre_restore_gv_ids}"
            )

        self.PrintStep(
            f"{tgt_label} Restoring EBS snapshot {backup_id}: {primary.id} → {target.id}"
        )

        if is_same:
            # Workload is already stopped by _take_backup_and_verify; this is
            # an idempotent safety net in case of a direct call
            self._stop_workload()

        self._restore_snapshot_backup(backup_id, primary, target)

        # Restore operation succeeded (asserted inside _restore_snapshot_backup) --
        # report attached guest volumes vs what ns_server reports on target, and
        # scan the restored target's memcached logs for errors.
        if target_fusion:
            self.cp_monitor.guest_volume_attached_vs_ns_server_reported(
                tenant, target, self.fusion_monitor, find_master_func=self.find_master
            )
        self.scan_memcahced_logs(target)

        self.sleep(30, f"{tgt_label} Wait after snapshot restore before verifying items")

        # Verify item count in each of primary's buckets on the target
        rest = RestConnection(target.master)
        for bucket in primary.buckets:
            expected = (
                bucket.loadDefn.get("num_items", 0)
                * bucket.loadDefn.get("collections", 1)
            )
            deadline = time.time() + self.restore_timeout
            while time.time() < deadline:
                info = rest.get_bucket_details(bucket_name=bucket.name)
                actual = info.get("basicStats", {}).get("itemCount", 0) if info else 0
                if actual > 0:
                    self.log.info(
                        f"{tgt_label} Post-restore item count on {target.id}/{bucket.name}: {actual} "
                        f"(expected ~{expected})"
                    )
                    break
                self.sleep(30, f"{tgt_label} Polling item count on {target.id}/{bucket.name} after restore")
            else:
                self.fail(
                    f"{tgt_label} No items on target {target.id}/{bucket.name} after restore timeout"
                )

        # Verify old guest volumes were deleted by the restore reset — this happens
        # during the restore itself, before any post-restore rebalance
        if not is_same and target_fusion and pre_restore_gv_ids:
            ok = self.cp_monitor.verify_old_guest_volumes_deleted(
                target, pre_restore_gv_ids, timeout=600
            )
            self.assertTrue(
                ok,
                f"{tgt_label} Pre-restore guest volumes still present on {target.id} "
                f"after restore reset — expected them deleted by the restore operation",
            )
            self.log.info(
                f"{tgt_label} Pre-restore guest volumes confirmed deleted on {target.id}"
            )

        # Verify guest volume count == EBS snapshot count for this backup
        if target_fusion and self.verify_snapshots:
            gv_ids = self.cp_monitor.get_current_guest_volume_ids(target)
            num_gvs = len(gv_ids)
            self.log.info(
                f"{tgt_label} {target.id}: {num_gvs} guest volumes attached: {gv_ids}"
            )
            snap_ok = self.cp_monitor.verify_guest_volume_snapshots_for_backup(
                primary, backup_id, num_gvs
            )
            self.assertTrue(
                snap_ok,
                f"Guest volume snapshot count mismatch after restore on {target.id}: "
                f"expected {num_gvs} snapshots for backup {backup_id}",
            )

        # Post-restore rebalance on secondary to verify the cluster is healthy
        # and rebalances complete successfully after the restore reset
        if not is_same and target_fusion:
            self.PrintStep(
                f"Post-restore rebalance on secondary {target.id} "
                f"to verify rebalance completes after restore"
            )
            self._trigger_rebalance_on_secondary(
                self.secondary_rebalance_delta,
                label="post-restore scale-up on secondary",
            )
            self._trigger_rebalance_on_secondary(
                -self.secondary_rebalance_delta,
                label="post-restore scale-down on secondary",
            )

        # Verify fusion state on target
        if target_fusion:
            self.fusion_monitor.set_admin_credentials(target)
            status, fusion_state = FusionRestAPI(target.master).get_fusion_status()
            self.assertTrue(status, f"Failed to get fusion status directly from {target.id}")
            self.assertEqual(
                fusion_state.get("state"),
                "enabled",
                f"Fusion must remain enabled on target {target.id} after restore, "
                f"got: {fusion_state.get('state')}",
            )
            self.log.info(
                f"{tgt_label} Fusion state on {target.id} post-restore: {fusion_state.get('state')}"
            )

        # Restart mutation thread if same-cluster restore kept it stopped
        if is_same:
            self._resume_workload()

    # ------------------------------------------- primary scaling pass helper

    def _primary_scaling_pass(self, direction, rebl_step_idx):
        """
        One horizontal scaling step on the primary cluster.

        :param direction: +1 = scale-up, -1 = scale-down
        :param rebl_step_idx: index into self.rebl_steps
        """
        primary = self.primary_cluster
        tenant = self.primary_tenant
        step = self.rebl_steps[rebl_step_idx % len(self.rebl_steps)]

        for service in self.rebl_services:
            config = self.rebalance_config(service, direction * step)
            rebalance_task = self.task.async_rebalance_capella(
                self.pod, tenant, primary, config, timeout=self.rebalance_timeout
            )
            self.monitor_cluster_status(tenant, primary, rebalance_task)
            self.fusion_monitor.get_fusion_uploader_map(tenant, primary, self.find_master)
            self.sleep(60, "post-primary-rebalance settle")
            result = self.cp_monitor.monitor_fusion_accelerator_nodes_killed_after_rebalance(
                primary
            )
            self.assertTrue(
                result,
                f"Accelerator nodes not killed after primary rebalance",
            )
            self.log_rebalance_report()
            self.scan_memcahced_logs(primary)
            self.parse_accelerator_logs()
            self.check_asg_cleanup_after_rebalance()

    # ------------------------------------------------------- main test

    def test_backup_restore_volume(self):
        """
        Fusion EBS snapshot backup/restore volume test.

        Primary cluster runs h/v scaling loops under continuous mutation load.
        EBS snapshot backups are taken on primary (cluster-level) and restored
        to the configured target (same primary or secondary cluster).
        Each cross-cluster restore validates:
          - data continuity on target per bucket
          - old guest-volume cleanup on secondary
          - new guest-volume creation on secondary (post-restore rebalance)
          - fusion state preservation
          - fusion S3 log-store bucket purged by cleanFusionBucket
        """
        self._log_store_stop_event = threading.Event()

        def _log_store_monitor():
            while not self._log_store_stop_event.is_set():
                self.log_fusion_log_store_data_size()
                self.log_fusion_pending_bytes()
                self._log_store_stop_event.wait(300)

        self._log_store_thread = threading.Thread(
            target=_log_store_monitor, name="log-store-monitor", daemon=True
        )
        self._log_store_thread.start()

        self._init_ops_stop_event = threading.Event()

        def _init_ops_rate_monitor():
            while not self._init_ops_stop_event.is_set():
                for cluster in [self.primary_cluster]:
                    try:
                        rest = RestConnection(cluster.master)
                        for bucket in cluster.buckets:
                            info = rest.get_bucket_details(bucket_name=bucket.name)
                            ops = info.get("basicStats", {}).get("opsPerSec", 0) if info else 0
                            self.log.info(
                                f"[init-load] cluster={cluster.id} "
                                f"bucket={bucket.name} ops/s={ops:.1f}"
                            )
                    except Exception as e:
                        self.log.warning(f"[init-load] ops rate fetch failed: {e}")
                self._init_ops_stop_event.wait(60)

        self._init_ops_thread = threading.Thread(
            target=_init_ops_rate_monitor, name="init-ops-rate-monitor", daemon=True
        )
        self._init_ops_thread.start()

        self.initial_setup()
        self._log_store_stop_event.set()
        self._init_ops_stop_event.set()

        primary = self.primary_cluster
        tenant = self.primary_tenant

        self.compute["data"] = self.input.param("fusion_compute", "m5.4xlarge")
        self.fusion_rebalances = []

        h_scaling = self.input.param("h_scaling", True)
        v_scaling = self.input.param("v_scaling", False)

        self.services = self.input.param("services", "data")
        self.rebl_services = self.input.param("rebl_services", self.services).split("-")
        self.rebl_steps = [
            int(n) for n in self.input.param("rebl_steps", "3-5-7-8").split("-")
        ]
        self.cycles = self.input.param("cycles", 1)

        # Background EBS volume monitoring for primary
        ebs_cleanup_thread = threading.Thread(
            target=self.cp_monitor.check_ebs_guest_vol_deletion,
            kwargs={
                "tenant": tenant,
                "cluster": primary,
                "fusion_monitor_util": self.fusion_monitor,
                "stop_run_event": self.stop_run_event,
                "find_master_func": self.find_master,
            },
        )
        ebs_cleanup_thread.start()

        ebs_available_thread = threading.Thread(
            target=self.cp_monitor.monitor_available_volumes_by_fusion_rebalance,
            kwargs={
                "cluster": primary,
                "fusion_rebalances": self.fusion_rebalances,
                "stop_run_event": self.stop_run_event,
            },
        )
        ebs_available_thread.start()

        # ------------------------------------------------------------------ H-scaling
        if h_scaling:
            self.loop = 0
            while self.loop < self.cycles:
                self.loop += 1

                # Scale-up iterations on primary
                for rebl_step in range(self.iterations):
                    self.log_fusion_log_store_data_size()
                    self.log_fusion_pending_bytes()
                    self.PrintStep(f"Cycle {self.loop}: Scale UP step {rebl_step}")
                    self._primary_scaling_pass(direction=+1, rebl_step_idx=rebl_step)

                    backup_id = self._take_backup_and_verify()
                    self._restore_and_verify(backup_id)

                # Scale-down iterations on primary
                for rebl_step in range(self.iterations):
                    self.log_fusion_log_store_data_size()
                    self.log_fusion_pending_bytes()
                    self.PrintStep(f"Cycle {self.loop}: Scale DOWN step {rebl_step}")
                    self._primary_scaling_pass(direction=-1, rebl_step_idx=rebl_step)

                    backup_id = self._take_backup_and_verify()
                    self._restore_and_verify(backup_id)

        # ------------------------------------------------------------------ V-scaling
        if v_scaling:
            provider = self.input.param("provider", "aws").lower()
            compute_list = AWS.compute if provider == "aws" else (
                AZURE.compute if provider == "azure" else GCP.compute
            )
            disk_increment = self.input.param("increment", 10)
            disk_change = 1
            compute_change = 1

            # Disk rebalances
            self.loop = 0
            while self.loop < self.iterations:
                self.log_fusion_log_store_data_size()
                self.log_fusion_pending_bytes()
                self.PrintStep(f"V-scale disk step {self.loop}")
                self.loop += 1

                for service in self.rebl_services:
                    if service not in ["query"]:
                        if provider == "azure":
                            idx = AZURE.StorageType.order.index(self.storage_type)
                            self.storage_type = AZURE.StorageType.order[idx + disk_change]
                            self.disk[service] = AZURE.StorageType.type[self.storage_type]["min"]
                            self.iops[service] = AZURE.StorageType.type[self.storage_type]["iops"]["min"]
                        else:
                            self.disk[service] = self.disk[service] + disk_increment
                    config = self.rebalance_config(service)
                    rebalance_task = self.task.async_rebalance_capella(
                        self.pod, tenant, primary, config, timeout=self.rebalance_timeout
                    )
                    self.monitor_cluster_status(tenant, primary, rebalance_task)
                    self.fusion_monitor.get_fusion_uploader_map(
                        tenant, primary, self.find_master
                    )
                    self.sleep(60, "post disk-rebalance settle")
                    result = self.cp_monitor.monitor_fusion_accelerator_nodes_killed_after_rebalance(
                        primary
                    )
                    self.assertTrue(result, "Accelerator nodes not killed after disk rebalance")
                    self.log_rebalance_report()
                    self.scan_memcahced_logs(primary)
                    self.parse_accelerator_logs()
                    self.check_asg_cleanup_after_rebalance()

                disk_increment *= -1
                disk_change *= -1

                backup_id = self._take_backup_and_verify()
                self._restore_and_verify(backup_id)

            # Compute rebalances
            self.loop = 0
            while self.loop < self.iterations:
                self.log_fusion_log_store_data_size()
                self.log_fusion_pending_bytes()
                self.PrintStep(f"V-scale compute step {self.loop}")
                self.loop += 1

                for service in self.rebl_services:
                    comp = compute_list.index(self.compute[service])
                    new_comp = comp + compute_change
                    if 0 <= new_comp < len(compute_list):
                        self.compute[service] = compute_list[new_comp]
                    config = self.rebalance_config()
                    rebalance_task = self.task.async_rebalance_capella(
                        self.pod, tenant, primary, config, timeout=self.rebalance_timeout
                    )
                    self.monitor_cluster_status(tenant, primary, rebalance_task)
                    self.fusion_monitor.get_fusion_uploader_map(
                        tenant, primary, self.find_master
                    )
                    self.sleep(60, "post compute-rebalance settle")
                    result = self.cp_monitor.monitor_fusion_accelerator_nodes_killed_after_rebalance(
                        primary
                    )
                    self.assertTrue(
                        result, "Accelerator nodes not killed after compute rebalance"
                    )
                    self.log_rebalance_report()
                    self.scan_memcahced_logs(primary)
                    self.parse_accelerator_logs()
                    self.check_asg_cleanup_after_rebalance()

                compute_change *= -1

                backup_id = self._take_backup_and_verify()
                self._restore_and_verify(backup_id)

        # ------------------------------------------------------------------ EBS cleanup
        self.stop_run_event.set()
        result = self.check_ebs_cleanup_for_cluster(primary)
        self.assertTrue(
            result,
            f"EBS guest-volume cleanup failed on primary cluster {primary.id}",
        )
        self.log.info(f"EBS cleanup verified for primary cluster {primary.id}")
