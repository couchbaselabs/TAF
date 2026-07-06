"""
Shared Backup Archive – Rebalance on Destination Cluster

Scenario:
  Same as the source-rebalance scenario, but the node scale-out rebalance is
  triggered on the *destination* cluster (the restore target) instead of the
  source.  Validates that cbbackupmgr restore tolerates an actively rebalancing
  destination while a concurrent backup runs undisturbed on the source.

Steps:
  1.  Allow IP on both clusters.
  2.  Load travel-sample into source cluster.
  3.  Create matching bucket on destination cluster.
  4.  Configure shared S3 backup repository.
  5.  Take first full backup from source cluster.
  6.  List backups; capture timestamp.
  7.  Trigger node scale-out (+1 node) on destination cluster — do NOT wait.
  8.  Concurrently:
        a. Second (incremental) backup from source cluster.
        b. Restore to destination cluster (--obj-read-only) while it rebalances.
  9.  Assert both operations completed successfully.
  10. Wait for destination rebalance to finish.
  11. (Optional) Scale destination cluster back to original size.
"""

import threading

from backup_restore.shared_archive_base import SharedArchiveBaseTest


class SharedArchiveRebalanceDestTest(SharedArchiveBaseTest):
    """Concurrent backup + restore where the *destination* cluster is rebalancing."""

    def test_rebalance_dest_concurrent_backup_restore(self):
        # ---- Steps 1-3: Access control and data load ----
        self.log.info("=== Steps 1-3: Access control and data load ===")
        self._add_allowed_ip(self.source_cluster)
        self._add_allowed_ip(self.dest_cluster)
        self._load_sample_bucket(self.source_cluster)
        self._create_destination_bucket()

        # ---- Step 4: Configure backup repository ----
        self._configure_repo()

        # ---- Step 5: First full backup ----
        self.log.info("=== Step 5: First full backup ===")
        stdout, stderr, rc = self._take_backup(
            self.source_cluster,
            self.staging_dir_backup,
            full_backup=True,
            label="backup-1",
        )
        self._assert_success(stdout, stderr, rc, "Backup completed successfully", "backup-1")
        self.log.info("First full backup completed.")

        # ---- Step 6: List backups, capture timestamp ----
        info_output = self._list_backups(self.staging_dir_backup)
        backup_ts = self._parse_latest_backup_timestamp(info_output)

        # ---- Step 7: Trigger scale-out rebalance on DESTINATION cluster ----
        self.log.info(
            "=== Step 7: Triggering scale-out rebalance on destination cluster ==="
        )
        dest_specs = self._get_current_specs(cluster=self.dest_cluster)
        scaled_specs, original_count = self._trigger_scale_out(
            dest_specs, cluster=self.dest_cluster, delta=1
        )
        self._wait_for_rebalance_started(cluster=self.dest_cluster)

        # ---- Step 8: Concurrent second backup (source) + restore (dest, rebalancing) ----
        self.log.info(
            "=== Step 8: Concurrent backup (2nd) + restore while destination rebalances ==="
        )

        backup2_result = {}
        restore_result = {}

        def run_second_backup():
            try:
                out, err, code = self._take_backup(
                    self.source_cluster,
                    self.staging_dir_backup,
                    full_backup=False,
                    label="backup-2",
                )
                backup2_result["stdout"] = out
                backup2_result["stderr"] = err
                backup2_result["rc"] = code
            except Exception as exc:
                backup2_result["stdout"] = ""
                backup2_result["stderr"] = str(exc)
                backup2_result["rc"] = -1
                backup2_result["exc"] = exc

        def run_restore():
            try:
                out, err, code = self._restore(
                    self.dest_cluster,
                    start_ts=backup_ts,
                    end_ts=backup_ts,
                    staging_dir=self.staging_dir_restore,
                    label="restore-1",
                )
                restore_result["stdout"] = out
                restore_result["stderr"] = err
                restore_result["rc"] = code
            except Exception as exc:
                restore_result["stdout"] = ""
                restore_result["stderr"] = str(exc)
                restore_result["rc"] = -1
                restore_result["exc"] = exc

        backup_thread = threading.Thread(target=run_second_backup, name="backup-2")
        restore_thread = threading.Thread(target=run_restore, name="restore-1")
        backup_thread.start()
        restore_thread.start()
        backup_thread.join()
        restore_thread.join()

        # ---- Step 9: Verify cbbackupmgr results ----
        self.log.info("=== Step 9: Verifying cbbackupmgr results ===")
        self._assert_success(
            backup2_result.get("stdout", ""),
            backup2_result.get("stderr", ""),
            backup2_result.get("rc", -1),
            "Backup completed successfully",
            "backup-2",
        )
        self.log.info("Second (concurrent) backup on source succeeded.")

        self._assert_success(
            restore_result.get("stdout", ""),
            restore_result.get("stderr", ""),
            restore_result.get("rc", -1),
            "Restore completed successfully",
            "restore-1",
        )
        self.log.info("Restore to destination cluster succeeded while it was rebalancing.")

        # ---- Step 10: Wait for destination rebalance to finish ----
        self.log.info("=== Step 10: Waiting for destination rebalance to complete ===")
        self._wait_for_cluster_healthy(cluster=self.dest_cluster)

        # ---- Step 11: (Optional) Scale destination back to original size ----
        if self.input.param("scale_back", True):
            self.log.info(
                "=== Step 11: Scaling destination cluster back to original size ==="
            )
            self._scale_back_in(scaled_specs, original_count, cluster=self.dest_cluster)

        self.log.info(
            "=== Destination-rebalance concurrent backup + restore test PASSED ==="
        )
