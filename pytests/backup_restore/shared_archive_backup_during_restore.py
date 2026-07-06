"""
Shared Backup Archive – Backup While Restore Is In Progress

Two sub-scenarios based on the --start / --end time range passed to the restore:

Sub-case A — Exclusive timerange
  Restore is already running (reading backup-1, a committed snapshot).
  A new backup (backup-2) starts AFTER restore is already in progress.
  The restore's --start/--end points only at backup-1 (exclusive of backup-2).
  Expected: both backup-2 and restore complete successfully.  Writing a new
  snapshot to S3 must not corrupt or block a concurrent read of an older one.

Sub-case B — Inclusive timerange
  Backup-2 is in progress (not yet committed to S3).
  A restore is attempted using backup-2's in-progress timestamp as --start/--end.
  Expected: restore either fails (backup not yet committed) or succeeds if
  cbbackupmgr can read partial data — both observed outcomes are logged.
  After backup-2 finishes and its metadata is committed, restore must succeed.

Steps (Sub-case A):
  1.  Allow IP on both clusters.
  2.  Load travel-sample into source; create matching bucket on destination.
  3.  Configure backup repository.
  4.  Take backup-1 (full, blocking).
  5.  List backups; capture backup-1 timestamp.
  6.  Start restore (non-blocking) using backup-1 timestamp.
  7.  Sleep briefly to let restore establish connections.
  8.  Start backup-2 (incremental, non-blocking) while restore is running.
  9.  Wait for both; assert both completed successfully.

Steps (Sub-case B):
  1-5. Same setup as Sub-case A.
  6.   Start backup-2 (incremental, non-blocking).
  7.   Sleep to let backup-2 start writing to S3.
  8.   Probe cbbackupmgr info to detect backup-2's in-progress timestamp.
  9.   Attempt restore using backup-2's timestamp (while backup-2 still running).
  10.  Log restore result (may fail — expected for an uncommitted backup).
  11.  Wait for backup-2 to complete.
  12.  Run restore again using backup-2's timestamp (now committed).
  13.  Assert final restore succeeds.
"""

import re
import threading
import time

from backup_restore.shared_archive_base import SharedArchiveBaseTest

# Seconds to wait after restore starts before launching backup-2 (Sub-case A).
_BACKUP_START_DELAY = 15
# Seconds to wait for backup-2 to start writing metadata before probing (Sub-case B).
_PROBE_DELAY = 15


class SharedArchiveBackupDuringRestoreTest(SharedArchiveBaseTest):
    """Backup starts while a restore is already in progress — two time-range sub-cases."""

    # ------------------------------------------------------------------
    # Common setup helper (shared by both tests)
    # ------------------------------------------------------------------

    def _common_setup(self):
        """Allow IPs, load data, configure repo, take backup-1. Return backup-1 ts."""
        self._add_allowed_ip(self.source_cluster)
        self._add_allowed_ip(self.dest_cluster)
        self._load_sample_bucket(self.source_cluster)
        self._create_destination_bucket()
        self._configure_repo()

        self.log.info("=== Taking backup-1 (full) ===")
        stdout, stderr, rc = self._take_backup(
            self.source_cluster,
            self.staging_dir_backup,
            full_backup=True,
            label="backup-1",
        )
        self._assert_success(stdout, stderr, rc, "Backup completed successfully", "backup-1")
        self.log.info("backup-1 completed.")

        info_output = self._list_backups(self.staging_dir_backup)
        backup1_ts = self._parse_latest_backup_timestamp(info_output)
        self.log.info("backup-1 timestamp: %s", backup1_ts)
        return backup1_ts

    def _collect_process(self, proc, label):
        """Wait for a Popen process to finish; return (stdout, stderr, rc)."""
        stdout, stderr = proc.communicate()
        rc = proc.returncode
        if stdout:
            self.log.info("[%s] STDOUT:\n%s", label, stdout.strip())
        if stderr:
            self.log.info("[%s] STDERR:\n%s", label, stderr.strip())
        self.log.info("[%s] exited with rc=%d", label, rc)
        return stdout, stderr, rc

    # ------------------------------------------------------------------
    # Sub-case A: backup-2 starts AFTER restore is already running
    # (exclusive timerange — restore reads backup-1 only)
    # ------------------------------------------------------------------

    def test_backup_starts_after_restore_begins_exclusive_timerange(self):
        """
        Restore is already reading backup-1 when backup-2 starts writing.
        The restore's --start/--end is backup-1's timestamp (exclusive of backup-2).
        Both operations must complete successfully.
        """
        self.log.info(
            "=== Sub-case A: backup starts AFTER restore begins (exclusive timerange) ==="
        )

        # ---- Setup ----
        backup1_ts = self._common_setup()

        # ---- Start restore first (non-blocking) ----
        self.log.info(
            "=== Starting restore (non-blocking) using backup-1 ts=%s ===", backup1_ts
        )
        restore_proc = self._start_restore_process(
            self.dest_cluster,
            start_ts=backup1_ts,
            end_ts=backup1_ts,
            staging_dir=self.staging_dir_restore,
            label="restore-1",
        )

        # ---- Wait to ensure restore has started before launching backup-2 ----
        self.log.info(
            "Sleeping %ds to let restore-1 establish connections before backup-2 starts…",
            _BACKUP_START_DELAY,
        )
        time.sleep(_BACKUP_START_DELAY)

        # ---- Start backup-2 (non-blocking) while restore is running ----
        self.log.info("=== Starting backup-2 (incremental, non-blocking) ===")
        backup2_proc = self._run_cbbackupmgr_process(
            self._build_backup_args(
                self.source_cluster,
                self.staging_dir_backup,
                full_backup=False,
            ),
            label="backup-2",
        )

        # ---- Wait for both ----
        self.log.info("Waiting for restore-1 and backup-2 to finish…")
        stdout_r, stderr_r, rc_r = self._collect_process(restore_proc, "restore-1")
        stdout_b2, stderr_b2, rc_b2 = self._collect_process(backup2_proc, "backup-2")

        # ---- Assertions ----
        self.log.info("=== Verifying results ===")
        self._assert_success(
            stdout_r, stderr_r, rc_r,
            "Restore completed successfully",
            "restore-1",
        )
        self.log.info("restore-1 succeeded.")

        self._assert_success(
            stdout_b2, stderr_b2, rc_b2,
            "Backup completed successfully",
            "backup-2",
        )
        self.log.info("backup-2 succeeded.")

        self.log.info(
            "=== Sub-case A PASSED: backup-2 writing to S3 did not interfere "
            "with restore-1 reading an exclusive snapshot ==="
        )

    # ------------------------------------------------------------------
    # Sub-case B: restore uses backup-2's timestamp while backup-2 is
    # still in progress (inclusive timerange)
    # ------------------------------------------------------------------

    def test_restore_inclusive_of_active_backup_timerange(self):
        """
        A restore is attempted using backup-2's timestamp while backup-2 is
        still being written.  cbbackupmgr may fail (backup not yet committed)
        or succeed — both outcomes are observed and logged.  After backup-2
        finishes and its metadata is committed to S3, the restore must succeed.
        """
        self.log.info(
            "=== Sub-case B: restore using in-progress backup's timestamp (inclusive timerange) ==="
        )

        # ---- Setup ----
        backup1_ts = self._common_setup()

        # ---- Start backup-2 (non-blocking) ----
        self.log.info("=== Starting backup-2 (incremental, non-blocking) ===")
        backup2_proc = self._run_cbbackupmgr_process(
            self._build_backup_args(
                self.source_cluster,
                self.staging_dir_backup,
                full_backup=False,
            ),
            label="backup-2",
        )

        # ---- Wait for backup-2 to start writing metadata to S3 ----
        self.log.info(
            "Sleeping %ds to let backup-2 write initial metadata before probing…",
            _PROBE_DELAY,
        )
        time.sleep(_PROBE_DELAY)

        # ---- Probe cbbackupmgr info to detect backup-2's in-progress timestamp ----
        self.log.info("Probing cbbackupmgr info to detect backup-2 timestamp…")
        backup2_ts = None
        try:
            info_output = self._list_backups(self.staging_dir_backup)
            all_timestamps = re.findall(
                r"(\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}[^\s]+)", info_output
            )
            # backup-2's timestamp is newer than backup-1's.
            newer = [ts for ts in all_timestamps if ts != backup1_ts]
            if newer:
                backup2_ts = newer[-1]
                self.log.info(
                    "Detected backup-2 timestamp while it is still running: %s",
                    backup2_ts,
                )
            else:
                self.log.warning(
                    "backup-2 timestamp not yet visible in cbbackupmgr info — "
                    "metadata has not been committed to S3 yet. "
                    "Skipping mid-run restore attempt."
                )
        except Exception as exc:
            self.log.warning("cbbackupmgr info raised while backup-2 in flight: %s", exc)

        # ---- Attempt restore using backup-2's in-progress timestamp ----
        if backup2_ts:
            self.log.info(
                "=== Attempting restore with backup-2's in-progress timestamp: %s ===",
                backup2_ts,
            )
            stdout_mid, stderr_mid, rc_mid = self._restore(
                self.dest_cluster,
                start_ts=backup2_ts,
                end_ts=backup2_ts,
                staging_dir=self.staging_dir_restore,
                label="restore-mid-backup2",
            )
            if rc_mid == 0:
                self.log.info(
                    "Restore of in-progress backup-2 SUCCEEDED — "
                    "cbbackupmgr tolerated restoring from a partially committed backup."
                )
            else:
                self.log.warning(
                    "Restore of in-progress backup-2 FAILED (rc=%d) — "
                    "expected: backup metadata not yet committed.\n"
                    "STDOUT: %s\nSTDERR: %s",
                    rc_mid, stdout_mid, stderr_mid,
                )
        else:
            self.log.info(
                "Skipped mid-run restore attempt (backup-2 not yet visible in S3 info)."
            )

        # ---- Wait for backup-2 to complete ----
        self.log.info("=== Waiting for backup-2 to finish ===")
        stdout_b2, stderr_b2, rc_b2 = self._collect_process(backup2_proc, "backup-2")
        self._assert_success(
            stdout_b2, stderr_b2, rc_b2,
            "Backup completed successfully",
            "backup-2",
        )
        self.log.info("backup-2 completed and metadata committed to S3.")

        # If backup-2 wasn't visible earlier, get its timestamp now.
        if not backup2_ts:
            info_output = self._list_backups(self.staging_dir_backup)
            all_timestamps = re.findall(
                r"(\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}[^\s]+)", info_output
            )
            newer = [ts for ts in all_timestamps if ts != backup1_ts]
            self.assertTrue(
                newer,
                "Could not find backup-2 timestamp after it completed.\n%s" % info_output,
            )
            backup2_ts = newer[-1]
            self.log.info("backup-2 timestamp (post-completion): %s", backup2_ts)

        # ---- Restore using backup-2's committed timestamp ----
        self.log.info(
            "=== Restoring using committed backup-2 timestamp: %s ===", backup2_ts
        )
        stdout_final, stderr_final, rc_final = self._restore(
            self.dest_cluster,
            start_ts=backup2_ts,
            end_ts=backup2_ts,
            staging_dir=self.staging_dir_restore,
            label="restore-final",
        )
        self._assert_success(
            stdout_final, stderr_final, rc_final,
            "Restore completed successfully",
            "restore-final",
        )
        self.log.info(
            "=== Sub-case B PASSED: restore succeeded once backup-2's "
            "snapshot was fully committed to S3 ==="
        )
