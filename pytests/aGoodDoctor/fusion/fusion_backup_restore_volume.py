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
4. If restore_to=secondary:
   a. Snapshot-backup primary → restore to secondary
      →  both clusters start with the same dataset.
   b. Based on secondary_fusion_enabled:
        True  → enable fusion on secondary; trigger one scaling rebalance so
                 secondary builds its own EBS guest volumes.
        False → ensure fusion is disabled on secondary.

Test loop (test_backup_restore_volume)
--------------------------------------
For each scaling cycle on primary:
  - [optional] take snapshot backup before scaling
  - scale-up primary (h_scaling iterations)
  - take snapshot backup + restore to target
  - scale-down primary (h_scaling iterations)
  - take snapshot backup + restore to target

Cross-cluster restore verification
------------------------------------
When restoring to secondary:
  1. Snapshot secondary's current guest-volume IDs (pre-restore).
  2. Restore primary EBS snapshot backup onto secondary.
  3. Trigger a fusion rebalance on secondary so new guest volumes are created
     to match primary's restored topology.
  4. Monitor the rebalance: verify new guest volumes come up.
  5. Verify old (pre-restore) guest volumes are deleted.
  6. Verify fusion state remains "enabled" on secondary.
  7. Verify fusion S3 bucket is purged (cleanFusionBucket ran on restore).

Key parameters
--------------
restore_to                : "same" (default) | "secondary"
secondary_fusion_enabled  : True (default)  | False
backup_before_scaling     : False (default)
backup_after_scaling      : True  (default)
verify_snapshots          : True  (default)
verify_fusion_bucket_clean: True  (default)
secondary_rebalance_delta : +1 (default)  — node delta used when triggering
                            rebalances on secondary to establish/refresh guest volumes
h_scaling / v_scaling     : same as fusion_volume.py
iterations, rebl_steps    : same as fusion_volume.py
'''

import threading
import time

from membase.api.rest_client import RestConnection
from capella_utils.dedicated import CapellaUtils as CapellaAPI
from aGoodDoctor.workloads import default
from bucket_utils.bucket_ready_functions import CollectionUtils, JavaDocLoaderUtils
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from constants.cloud_constants.capella_constants import AWS, GCP, AZURE
from .awslib.cloudtrail_delete_setup import CloudTrailSetup
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
        self.backup_before_scaling = self.input.param("backup_before_scaling", True)
        self.backup_after_scaling = self.input.param("backup_after_scaling", True)
        self.verify_snapshots = self.input.param("verify_snapshots", True)
        self.verify_fusion_bucket_clean = self.input.param("verify_fusion_bucket_clean", True)
        # Node delta for rebalances triggered on secondary to build/refresh guest volumes
        self.secondary_rebalance_delta = self.input.param("secondary_rebalance_delta", 1)

        all_clusters = [c for t in self.tenants for c in t.clusters]
        self.primary_cluster = all_clusters[0]
        self.primary_tenant = self.tenants[0]

        if self.restore_to == "secondary":
            if len(all_clusters) < 2:
                self.fail(
                    "restore_to=secondary requires at least 2 clusters in the .ini file"
                )
            self.secondary_cluster = all_clusters[1]
        else:
            self.secondary_cluster = None

    # ---------------------------------------------------------------- helpers

    def _target_cluster(self):
        return self.secondary_cluster if self.restore_to == "secondary" else self.primary_cluster

    def _target_is_fusion(self):
        return True if self.restore_to == "same" else self.secondary_fusion_enabled

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

        self.log.info(f"Creating EBS snapshot backup on cluster {cluster.id}")
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
        self.log.info(f"Snapshot backup triggered: {backup_id}")

        ok = CapellaAPI.wait_for_cloud_snapshot_backup_to_complete(
            self.pod, tenant, project_id, cluster.id, backup_id
        )
        self.assertTrue(
            ok,
            f"Snapshot backup {backup_id} did not complete on cluster {cluster.id}",
        )
        self.log.info(f"Snapshot backup {backup_id} complete on {cluster.id}")
        return backup_id

    def _restore_snapshot_backup(self, backup_id, source_cluster, target_cluster):
        """
        Restore EBS snapshot *backup_id* (taken from *source_cluster*) onto
        *target_cluster* and wait for completion.  Waits for the target cluster
        to return to healthy state afterwards.
        """
        tenant = self.primary_tenant
        project_id = tenant.projects[0]

        self.log.info(
            f"Restoring snapshot backup {backup_id} from {source_cluster.id} "
            f"→ {target_cluster.id}"
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
        self.log.info(f"Restore job triggered: {restore_id}")

        ok = CapellaAPI.wait_for_cloud_snapshot_restore_to_complete(
            self.pod, tenant, project_id, target_cluster.id, restore_id
        )
        self.assertTrue(
            ok,
            f"Snapshot restore {restore_id} did not complete on target {target_cluster.id}",
        )
        self.log.info(f"Snapshot restore {restore_id} complete on {target_cluster.id}")

        # Wait for target cluster to reach healthy state after restore
        CapellaAPI.wait_until_done(
            self.pod, tenant, target_cluster.id,
            msg=f"Wait for healthy state after snapshot restore on {target_cluster.id}",
        )
        self.log.info(f"Target cluster {target_cluster.id} healthy after restore")

    # ------------------------------------------------ secondary cluster setup

    def _initial_data_sync_to_secondary(self):
        """
        Snapshot-backup primary and restore onto secondary so both clusters
        start with the same dataset before fusion is configured on secondary.
        """
        primary = self.primary_cluster
        secondary = self.secondary_cluster

        self.PrintStep(
            f"Initial data sync: EBS snapshot backup primary {primary.id} "
            f"→ restore to secondary {secondary.id}"
        )

        backup_id = self._create_snapshot_backup(primary)
        self._restore_snapshot_backup(backup_id, primary, secondary)

        self.sleep(30, "Wait after initial snapshot restore to secondary")

        # Verify secondary has data in each of primary's buckets
        rest = RestConnection(secondary.master)
        for bucket in primary.buckets:
            expected = (
                bucket.loadDefn.get("num_items", 0)
                * bucket.loadDefn.get("collections", 1)
            )
            deadline = time.time() + self.index_timeout
            while time.time() < deadline:
                info = rest.get_bucket_details(bucket_name=bucket.name)
                actual = info.get("basicStats", {}).get("itemCount", 0) if info else 0
                if actual > 0:
                    self.log.info(
                        f"Secondary {secondary.id} bucket {bucket.name}: "
                        f"{actual} items after initial sync (expected ~{expected})"
                    )
                    break
                self.sleep(30, f"Waiting for items on secondary after initial sync")
            else:
                self.fail(
                    f"No items on secondary {secondary.id}/{bucket.name} "
                    f"after initial data sync"
                )

    def _trigger_rebalance_on_secondary(self, delta, label="secondary rebalance"):
        """
        Scale secondary cluster by *delta* data nodes and monitor the rebalance
        (including fusion guest-volume lifecycle if secondary is fusion-enabled).
        """
        secondary = self.secondary_cluster
        tenant = self.primary_tenant

        self.PrintStep(f"{label}: delta={delta:+d} on secondary {secondary.id}")
        config = self.rebalance_config("data", delta)
        rebalance_task = self.task.async_rebalance_capella(
            self.pod, tenant, secondary, config, timeout=self.index_timeout
        )
        self.monitor_cluster_status(tenant, secondary, rebalance_task)
        self.fusion_monitor.get_fusion_uploader_map(tenant, secondary, self.find_master)
        self.sleep(60, "post-secondary-rebalance settle")

        result = self.cp_monitor.monitor_fusion_accelerator_nodes_killed_after_rebalance(secondary)
        self.assertTrue(
            result,
            f"Accelerator nodes not killed after {label} on secondary {secondary.id}",
        )

    def _configure_secondary_fusion(self):
        """
        Enable or disable fusion on secondary and, when enabling, trigger an
        initial scaling rebalance so secondary builds its own EBS guest volumes.
        """
        secondary = self.secondary_cluster
        tenant = self.primary_tenant

        if self.secondary_fusion_enabled:
            fusion_state = CapellaAPI.get_fusion_status(self.pod, tenant, secondary.id)
            if fusion_state.get("state") != "enabled":
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
            fusion_state = CapellaAPI.get_fusion_status(self.pod, tenant, secondary.id)
            if fusion_state.get("state") == "enabled":
                resp = CapellaAPI.disable_fusion(self.pod, tenant, secondary.id)
                self.assertTrue(
                    resp.status_code == 200,
                    f"Failed to disable Fusion on secondary {secondary.id}: {resp.status_code}",
                )
                self.fusion_monitor.wait_for_fusion_status(secondary, state="disabled")
            self.log.info(f"Fusion disabled on secondary {secondary.id}")

    def _setup_secondary_cluster(self):
        """
        Full secondary cluster bootstrap:
          1. Snapshot-backup primary → restore to secondary.
          2. Configure fusion state on secondary.
          3. If fusion-enabled: trigger initial rebalance → guest volumes created.
        """
        self._initial_data_sync_to_secondary()
        self._configure_secondary_fusion()

        secondary = self.secondary_cluster
        secondary.fusion_uploader_dict = {}
        secondary.fusion_vb_uploader_map = {}

    # ----------------------------------------------- initial_setup override

    def initial_setup(self):
        """
        Set up primary cluster with data + steady-state mutations, then bootstrap
        secondary (data sync + fusion config + initial rebalance if applicable).
        """
        tenant = self.primary_tenant
        primary = self.primary_cluster

        CapellaAPI.update_feature_flag_globally(self.pod, tenant, "fusion-rebalances", True)
        CapellaAPI.update_feature_flag_globally(self.pod, tenant, "fusion-fallback-replace", True)

        # Enable fusion on primary
        fusion_state = CapellaAPI.get_fusion_status(self.pod, tenant, primary.id)
        if fusion_state.get("state") != "enabled":
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


        # CloudTrail for primary buckets
        try:
            self.cloudtrail = CloudTrailSetup(
                self.aws_access_key, self.aws_secret_key, region=self.aws_region
            )
        except Exception as e:
            self.cloudtrail = None
            self.log.warning(f"CloudTrail init failed; continuing without it: {e}")

        if self.cloudtrail:
            for bucket in primary.buckets:
                try:
                    uri = self.fusion_monitor.get_fusion_s3_uri(primary, bucket.name)
                    if not uri:
                        continue
                    uri_clean = uri.split("?")[0].replace("s3://", "")
                    s3_bucket = uri_clean.split("/")[0]
                    if not s3_bucket:
                        continue
                    trail_name = f"fusion_bkrs_{primary.id}_{bucket.name}"
                    log_prefix = f"s3-object-deletes/{primary.id}/{bucket.name}"
                    self.cloudtrail.setup_cloudtrail_delete_obj_s3_logging(
                        source_bucket=s3_bucket,
                        target_bucket="fusion-accesslogs-ritesh-agarwal",
                        trail_name=trail_name,
                        log_prefix=log_prefix,
                    )
                    self.cloudtrail_targets.append((trail_name, log_prefix))
                except Exception as e:
                    self.log.warning(
                        f"CloudTrail setup failed for {primary.id}/{bucket.name}: {e}"
                    )

        # Fusion uploader map for primary
        primary.fusion_uploader_dict = {b.name: {} for b in primary.buckets}
        primary.fusion_vb_uploader_map = {b.name: {} for b in primary.buckets}
        self.fusion_monitor.get_fusion_uploader_map(tenant, primary, self.find_master)

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
            target=self.normal_mutations, kwargs={"cluster": primary,
                                                  "wait_for_completion": False}
        )
        self.mutation_th.start()

        self.sleep(
            self.steady_state_workload_sleep,
            f"Steady-state workload sleep {self.steady_state_workload_sleep}s",
        )

        if self.restore_to == "secondary":
            self._setup_secondary_cluster()

    # ----------------------------------------------- backup helpers

    def _take_backup_and_verify(self):
        """
        Trigger an EBS snapshot backup on primary, optionally verify EBS
        guest-volume snapshot tags, and return the backup ID.
        """
        primary = self.primary_cluster
        num_snapshots_expected = len(self.fusion_aws_util.ec2.list_volumes_by_cluster_id(filters={
                    'couchbase-cloud-cluster-id': primary.id,
                    'couchbase-cloud-function': 'fusion-accelerator'
                    }))
        self.PrintStep(f"Taking EBS snapshot backup on primary {primary.id}")
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

        return backup_id

    def _restore_and_verify(self, backup_id):
        """
        Restore primary EBS snapshot backup to the target cluster and verify:
          - item count on target > 0 per bucket
          - if target is fusion-enabled:
              * new guest volumes come up (via post-restore rebalance + monitoring)
              * pre-restore guest volumes are deleted
              * fusion state remains "enabled"
              * fusion S3 bucket is empty (cleanFusionBucket ran)
        """
        primary = self.primary_cluster
        target = self._target_cluster()
        tenant = self.primary_tenant
        is_same = primary.id == target.id
        target_fusion = self._target_is_fusion()

        # Snapshot pre-restore guest volumes on target (if fusion-enabled and cross-cluster)
        pre_restore_gv_ids = []
        if not is_same and target_fusion:
            pre_restore_gv_ids = self.cp_monitor.get_current_guest_volume_ids(target)
            self.log.info(
                f"Pre-restore guest volumes on secondary {target.id}: {pre_restore_gv_ids}"
            )

        self.PrintStep(
            f"Restoring EBS snapshot {backup_id}: {primary.id} → {target.id}"
        )

        if is_same:
            # Stop mutations — the cluster data will be replaced entirely
            self.mutations = False
            if hasattr(self, "mutation_th") and self.mutation_th.is_alive():
                self.mutation_th.join(timeout=120)

        self._restore_snapshot_backup(backup_id, primary, target)

        self.sleep(30, "Wait after snapshot restore before verifying items")

        # Verify item count in each of primary's buckets on the target
        rest = RestConnection(target.master)
        for bucket in primary.buckets:
            expected = (
                bucket.loadDefn.get("num_items", 0)
                * bucket.loadDefn.get("collections", 1)
            )
            deadline = time.time() + self.index_timeout
            while time.time() < deadline:
                info = rest.get_bucket_details(bucket_name=bucket.name)
                actual = info.get("basicStats", {}).get("itemCount", 0) if info else 0
                if actual > 0:
                    self.log.info(
                        f"Post-restore item count on {target.id}/{bucket.name}: {actual} "
                        f"(expected ~{expected})"
                    )
                    break
                self.sleep(30, f"Polling item count on {target.id}/{bucket.name} after restore")
            else:
                self.fail(
                    f"No items on target {target.id}/{bucket.name} after restore timeout"
                )

        # Verify fusion S3 bucket was cleaned up by cleanFusionBucket
        if target_fusion and self.verify_fusion_bucket_clean:
            for bucket in primary.buckets:
                ok = self.fusion_monitor.verify_fusion_bucket_empty_after_restore(
                    target, bucket.name
                )
                self.assertTrue(
                    ok,
                    f"Fusion S3 bucket not empty after restore on "
                    f"{target.id}/{bucket.name}",
                )
                self.log.info(
                    f"Fusion S3 bucket empty verified on {target.id}/{bucket.name}"
                )

        # Verify guest volume count == EBS snapshot count for this backup
        if target_fusion and self.verify_snapshots:
            gv_ids = self.cp_monitor.get_current_guest_volume_ids(target)
            num_gvs = len(gv_ids)
            self.log.info(
                f"Target {target.id}: {num_gvs} guest volumes attached: {gv_ids}"
            )
            snap_ok = self.cp_monitor.verify_guest_volume_snapshots_for_backup(
                primary, backup_id, num_gvs
            )
            self.assertTrue(
                snap_ok,
                f"Guest volume snapshot count mismatch after restore on {target.id}: "
                f"expected {num_gvs} snapshots for backup {backup_id}",
            )

        # Post-restore rebalance on secondary to refresh guest volumes
        if not is_same and target_fusion:
            self.PrintStep(
                f"Post-restore rebalance on secondary {target.id} "
                f"to establish new guest volumes"
            )
            self._trigger_rebalance_on_secondary(
                self.secondary_rebalance_delta,
                label="post-restore scale-up on secondary",
            )
            self._trigger_rebalance_on_secondary(
                -self.secondary_rebalance_delta,
                label="post-restore scale-down on secondary",
            )

            # Verify old guest volumes (pre-restore) are gone
            ok = self.cp_monitor.verify_old_guest_volumes_deleted(
                target, pre_restore_gv_ids, timeout=600
            )
            self.assertTrue(
                ok,
                f"Pre-restore guest volumes still present on secondary {target.id} "
                f"after post-restore rebalance",
            )
            self.log.info(
                f"Pre-restore guest volumes confirmed deleted on {target.id}"
            )

        # Verify fusion state on target
        if target_fusion:
            fusion_state = CapellaAPI.get_fusion_status(self.pod, tenant, target.id)
            self.assertEqual(
                fusion_state.get("state"),
                "enabled",
                f"Fusion must remain enabled on target {target.id} after restore, "
                f"got: {fusion_state.get('state')}",
            )
            self.log.info(
                f"Fusion state on {target.id} post-restore: {fusion_state.get('state')}"
            )

        # Restart mutation thread if same-cluster restore paused it
        if is_same:
            self.mutations = True
            self.mutation_th = threading.Thread(
                target=self.normal_mutations,
                kwargs={"cluster": primary},
            )
            self.mutation_th.start()

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
                self.pod, tenant, primary, config, timeout=self.index_timeout
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
            self.scan_memcahced_logs_for_errors()
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
        self.initial_setup()
        self._log_store_stop_event.set()

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

                if self.backup_before_scaling:
                    backup_id = self._take_backup_and_verify()
                    self._restore_and_verify(backup_id)

                # Scale-up iterations on primary
                for rebl_step in range(self.iterations):
                    self.log_fusion_log_store_data_size()
                    self.log_fusion_pending_bytes()
                    self.PrintStep(f"Cycle {self.loop}: Scale UP step {rebl_step}")
                    self._primary_scaling_pass(direction=+1, rebl_step_idx=rebl_step)

                if self.backup_after_scaling:
                    backup_id = self._take_backup_and_verify()
                    self._restore_and_verify(backup_id)

                # Scale-down iterations on primary
                for rebl_step in range(self.iterations):
                    self.log_fusion_log_store_data_size()
                    self.log_fusion_pending_bytes()
                    self.PrintStep(f"Cycle {self.loop}: Scale DOWN step {rebl_step}")
                    self._primary_scaling_pass(direction=-1, rebl_step_idx=rebl_step)

                if self.backup_after_scaling:
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
                        self.pod, tenant, primary, config, timeout=self.index_timeout
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
                    self.scan_memcahced_logs_for_errors()
                    self.parse_accelerator_logs()
                    self.check_asg_cleanup_after_rebalance()

                disk_increment *= -1
                disk_change *= -1

                if self.backup_after_scaling:
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
                        self.pod, tenant, primary, config, timeout=self.index_timeout
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
                    self.scan_memcahced_logs_for_errors()
                    self.parse_accelerator_logs()
                    self.check_asg_cleanup_after_rebalance()

                compute_change *= -1

                if self.backup_after_scaling:
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
