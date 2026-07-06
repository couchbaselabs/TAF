"""
Shared Backup Archive – Happy Path (Test 1)

Scenario:
  Two Capella clusters (source + destination) share the same S3 backup archive.
  A full backup is taken first; then a second incremental backup and a restore
  run concurrently using two independent cbbackupmgr instances.

Steps:
  1.  Allow IP on both clusters.
  2.  Load travel-sample into source cluster.
  3.  Create matching bucket on destination cluster.
  4.  Configure shared S3 backup repository.
  5.  Take first full backup from source cluster.
  6.  List backups; capture timestamp.
  7.  Concurrently:
        a. Second (incremental) backup from source cluster.
        b. Restore to destination using first backup (--obj-read-only).
  8.  Assert both operations completed successfully.
"""

import threading

from backup_restore.shared_archive_base import SharedArchiveBaseTest


class SharedArchiveHappyPathTest(SharedArchiveBaseTest):
    """Happy-path concurrent backup + restore against a shared S3 archive."""

    def test_happy_path_concurrent_backup_restore(self):
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

        # ---- Step 7: Concurrent second backup + restore ----
        self.log.info("=== Step 7: Concurrent backup (2nd) + restore ===")

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

        # ---- Step 8: Verify results ----
        self.log.info("=== Step 8: Verifying results ===")
        self._assert_success(
            backup2_result.get("stdout", ""),
            backup2_result.get("stderr", ""),
            backup2_result.get("rc", -1),
            "Backup completed successfully",
            "backup-2",
        )
        self.log.info("Second (concurrent) backup completed successfully.")

        self._assert_success(
            restore_result.get("stdout", ""),
            restore_result.get("stderr", ""),
            restore_result.get("rc", -1),
            "Restore completed successfully",
            "restore-1",
        )
        self.log.info("Restore to destination cluster completed successfully.")
        self.log.info("=== Happy path test PASSED ===")
