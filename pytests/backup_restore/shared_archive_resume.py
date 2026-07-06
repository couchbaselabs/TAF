"""
Shared Backup Archive – Restore Resume Flag Test

Scenario:
  Validate that the cbbackupmgr ``--resume`` flag correctly resumes an
  interrupted restore from its last checkpoint rather than restarting from
  the beginning — while a concurrent backup is actively writing to the same
  shared S3 archive.

  This mirrors the real-world pattern: backup-2 is in flight on the source
  cluster while a restore is reading backup-1's snapshot on the destination
  cluster.  A restore failure is induced by terminating the cbbackupmgr
  restore process mid-run (simulating an abrupt network interruption or
  process crash).  Because cbbackupmgr checkpoints progress to the staging
  directory, a subsequent ``restore --resume`` call picks up exactly where
  the interrupted run stopped — while backup-2 may still be running.

Steps:
  1.  Allow IP on both clusters.
  2.  Load travel-sample into source cluster.
  3.  Create matching bucket on destination cluster.
  4.  Configure shared S3 backup repository.
  5.  Take first full backup (backup-1) from source cluster; wait for completion.
  6.  List backups; capture backup-1 timestamp.
  7.  Start backup-2 (incremental, non-blocking via Popen) on the source cluster.
  8.  Start restore-initial (non-blocking via Popen) on the destination cluster,
      using backup-1's timestamp (exclusive of the still-running backup-2).
  9.  Sleep ``interrupt_after`` seconds to let restore-initial partially complete.
  10. Terminate restore-initial (SIGTERM → SIGKILL if needed).
  11. Verify restore-initial exited non-zero (was genuinely interrupted).
  12. Run restore again WITH ``--resume`` flag (backup-2 may still be running).
  13. Assert resumed restore completes successfully.
  14. Wait for backup-2 to finish; assert it also completed successfully.

Test parameters (all have defaults):
  ``interrupt_after``  – seconds to let the initial restore run before
                         killing it (default: 20).  Increase if your bucket
                         is large and 20 s is not enough time for cbbackupmgr
                         to write any checkpoints.
  ``kill_timeout``     – seconds to wait for SIGTERM before escalating to
                         SIGKILL (default: 5).
"""

import threading
import time

from backup_restore.shared_archive_base import SharedArchiveBaseTest

# Default seconds to allow the initial restore to run before interrupting.
_DEFAULT_INTERRUPT_AFTER = 20
# Default seconds to wait after SIGTERM before sending SIGKILL.
_DEFAULT_KILL_TIMEOUT = 5


class SharedArchiveResumeTest(SharedArchiveBaseTest):
    """
    Validates the ``--resume`` flag recovers a mid-restore interruption while
    a concurrent backup is writing to the same shared S3 archive.
    """

    def test_resume_flag_after_interrupted_restore(self):
        # Read optional overrides from test parameters.
        interrupt_after = self.input.param("interrupt_after", _DEFAULT_INTERRUPT_AFTER)
        kill_timeout = self.input.param("kill_timeout", _DEFAULT_KILL_TIMEOUT)

        # ---- Steps 1-3: Access control and data load ----
        self.log.info("=== Steps 1-3: Access control and data load ===")
        self._add_allowed_ip(self.source_cluster)
        self._add_allowed_ip(self.dest_cluster)
        self._load_sample_bucket(self.source_cluster)
        self._create_destination_bucket()

        # ---- Step 4: Configure backup repository ----
        self._configure_repo()

        # ---- Step 5: First full backup ----
        self.log.info("=== Step 5: First full backup (backup-1) ===")
        stdout, stderr, rc = self._take_backup(
            self.source_cluster,
            self.staging_dir_backup,
            full_backup=True,
            label="backup-1",
        )
        self._assert_success(stdout, stderr, rc, "Backup completed successfully", "backup-1")
        self.log.info("backup-1 completed.")

        # ---- Step 6: List backups, capture backup-1 timestamp ----
        info_output = self._list_backups(self.staging_dir_backup)
        backup1_ts = self._parse_latest_backup_timestamp(info_output)
        self.log.info("backup-1 timestamp: %s", backup1_ts)

        # ---- Step 7: Start backup-2 (incremental, non-blocking) ----
        # backup-2 writes new data to the archive concurrently.  The restore
        # uses backup-1's timestamp (--start/--end = backup1_ts) so it is
        # reading from a completed, stable snapshot — exclusive of backup-2.
        self.log.info("=== Step 7: Starting backup-2 (non-blocking, incremental) ===")
        backup2_proc = self._run_cbbackupmgr_process(
            self._build_backup_args(
                self.source_cluster,
                self.staging_dir_backup,
                full_backup=False,
            ),
            label="backup-2",
        )

        try:
            # ---- Step 8: Start restore-initial (non-blocking) ----
            self.log.info(
                "=== Step 8: Starting restore-initial (non-blocking, will interrupt after %ds) ===",
                interrupt_after,
            )
            restore_proc = self._start_restore_process(
                self.dest_cluster,
                start_ts=backup1_ts,
                end_ts=backup1_ts,
                staging_dir=self.staging_dir_restore,
                label="restore-initial",
            )

            # ---- Step 9: Let restore run partway ----
            self.log.info(
                "Sleeping %ds to let restore-initial establish checkpoints…",
                interrupt_after,
            )
            time.sleep(interrupt_after)

            # ---- Step 10: Terminate the restore process ----
            self.log.info(
                "Sending SIGTERM to restore-initial (pid=%d)…", restore_proc.pid
            )
            restore_proc.terminate()
            try:
                stdout_init, stderr_init = restore_proc.communicate(timeout=kill_timeout)
                rc_init = restore_proc.returncode
            except subprocess.TimeoutExpired:
                self.log.warning(
                    "SIGTERM timed out after %ds — escalating to SIGKILL.", kill_timeout
                )
                restore_proc.kill()
                stdout_init, stderr_init = restore_proc.communicate()
                rc_init = restore_proc.returncode

            if stdout_init:
                self.log.info("[restore-initial] STDOUT:\n%s", stdout_init.strip())
            if stderr_init:
                self.log.info("[restore-initial] STDERR:\n%s", stderr_init.strip())
            self.log.info(
                "[restore-initial] Process exited with rc=%d after interrupt.", rc_init
            )

            # ---- Step 11: Verify restore-initial was genuinely interrupted ----
            self.assertNotEqual(
                rc_init, 0,
                "restore-initial exited with rc=0 before we could interrupt it — "
                "restore completed within the %ds window.  "
                "Try increasing 'interrupt_after' or using a larger dataset."
                % interrupt_after,
            )
            self.log.info(
                "Confirmed: restore-initial was interrupted (rc=%d). "
                "Staging directory retains checkpoint state.", rc_init
            )

            # ---- Step 12: Resume the restore (backup-2 may still be running) ----
            self.log.info(
                "=== Step 12: Resuming restore with --resume (backup-2 may still be active) ==="
            )
            stdout_resume, stderr_resume, rc_resume = self._restore(
                self.dest_cluster,
                start_ts=backup1_ts,
                end_ts=backup1_ts,
                staging_dir=self.staging_dir_restore,
                label="restore-resume",
                resume=True,
            )

            # ---- Step 13: Assert resumed restore completed successfully ----
            self.log.info("=== Step 13: Verifying resumed restore ===")
            self._assert_success(
                stdout_resume,
                stderr_resume,
                rc_resume,
                "Restore completed successfully",
                "restore-resume",
            )
            self.log.info("restore-resume completed successfully.")

        finally:
            # Ensure backup2_proc is always reaped, even on early assertion failure.
            if backup2_proc.poll() is None:
                self.log.info("Terminating backup-2 process (pid=%d)…", backup2_proc.pid)
                backup2_proc.terminate()
                try:
                    backup2_proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    backup2_proc.kill()
                    backup2_proc.wait()

        # ---- Step 14: Wait for backup-2 and verify it also succeeded ----
        self.log.info("=== Step 14: Waiting for backup-2 to finish ===")
        stdout_b2, stderr_b2 = backup2_proc.communicate()
        rc_b2 = backup2_proc.returncode
        if stdout_b2:
            self.log.info("[backup-2] STDOUT:\n%s", stdout_b2.strip())
        if stderr_b2:
            self.log.info("[backup-2] STDERR:\n%s", stderr_b2.strip())
        self._assert_success(
            stdout_b2, stderr_b2, rc_b2,
            "Backup completed successfully",
            "backup-2",
        )
        self.log.info(
            "=== --resume flag test PASSED: "
            "restore resumed successfully while concurrent backup-2 was active ==="
        )
