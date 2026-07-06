"""
Shared Backup Archive – Rebalance Scenario

Scenario:
  Same as Happy Path, but a node scale-out rebalance is triggered on the
  source cluster before the concurrent backup + restore begin.  Validates
  that cbbackupmgr tolerates an actively rebalancing cluster.

Steps:
  1.  Allow IP on both clusters.
  2.  Load travel-sample into source cluster.
  3.  Create matching bucket on destination cluster.
  4.  Configure shared S3 backup repository.
  5.  Take first full backup from source cluster.
  6.  List backups; capture timestamp.
  7.  Trigger node scale-out (+1 node) on source cluster — do NOT wait.
  8.  Concurrently:
        a. Second (incremental) backup from source cluster (rebalance ongoing).
        b. Restore to destination using first backup (--obj-read-only).
  9.  Assert both operations completed successfully.
  10. Wait for rebalance to finish.
  11. (Optional) Scale source cluster back to original size.
"""

import threading

from backup_restore.shared_archive_base import SharedArchiveBaseTest


class SharedArchiveRebalanceTest(SharedArchiveBaseTest):
    """Concurrent backup + restore while source cluster rebalances."""

    def test_rebalance_concurrent_backup_restore(self):
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

        # ---- Step 7: Trigger scale-out rebalance (non-blocking) ----
        self.log.info("=== Step 7: Triggering scale-out rebalance on source cluster ===")
        current_specs = self._get_current_specs()
        scaled_specs, original_count = self._trigger_scale_out(current_specs, delta=1)
        self._wait_for_rebalance_started(cluster=self.source_cluster)

        # ---- Step 8: Concurrent second backup + restore ----
        self.log.info(
            "=== Step 8: Concurrent backup (2nd) + restore while rebalance is active ==="
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
        self.log.info("Second (concurrent) backup succeeded during rebalance.")

        self._assert_success(
            restore_result.get("stdout", ""),
            restore_result.get("stderr", ""),
            restore_result.get("rc", -1),
            "Restore completed successfully",
            "restore-1",
        )
        self.log.info("Restore to destination cluster succeeded during rebalance.")

        # ---- Step 10: Wait for rebalance to finish ----
        self.log.info("=== Step 10: Waiting for rebalance to complete ===")
        self._wait_for_cluster_healthy()

        # ---- Step 11: (Optional) Scale back to original size ----
        if self.input.param("scale_back", True):
            self.log.info("=== Step 11: Scaling source cluster back to original size ===")
            self._scale_back_in(scaled_specs, original_count)

        self.log.info("=== Rebalance concurrent backup + restore test PASSED ===")
