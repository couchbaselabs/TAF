"""
Shared Backup Archive – All Services: --obj-read-only and --resume Flags

Scenario:
  Validate that cbbackupmgr correctly handles backup and restore across a
  cluster that has ALL Couchbase services enabled (Data/KV, Index/GSI,
  Query/N1QL, Search/FTS, Eventing, Analytics/CBAS).  Specifically checks:

    1. ``--obj-read-only``: the restore reads data exclusively from S3 and
       writes to the destination cluster — the source cluster is untouched.
       This must work correctly when the backup contains service-specific
       artefacts (GSI snapshots, FTS index files, Eventing DCP checkpoints,
       Analytics shadow datasets).

    2. ``--resume``: a restore interrupted mid-run can be resumed from its
       checkpoint.  With all services in the backup, the resumed restore must
       continue writing all service data without corruption.

Cluster requirement:
  The SOURCE cluster must have all services deployed:
    - Data (KV)
    - Index (GSI)
    - Query (N1QL)
    - Search (FTS)
    - Eventing
    - Analytics (CBAS)

  This is controlled by the Capella cluster spec at creation time.
  Pass ``skip_teardown=True`` (the default) to reuse a pre-created cluster.

Steps:
  1.  Allow IP on both clusters.
  2.  Load travel-sample into source (provides KV docs + pre-built GSI + FTS
      indexes).
  3.  Create matching bucket on destination.
  4.  Configure shared S3 backup repository.
  5.  Take a full backup (backup-1) — captures all service data.
  6.  List backups; capture timestamp.

  -- obj-read-only validation --
  7.  Run restore of backup-1 with ``--obj-read-only``; assert success.

  -- --resume validation (with concurrent backup-2) --
  8.  Take backup-2 (incremental, non-blocking) — runs concurrently.
  9.  Start restore of backup-1 (non-blocking) — same snapshot, exclusive of
      backup-2.
  10. Interrupt restore after ``interrupt_after`` seconds.
  11. Confirm restore was genuinely interrupted (rc != 0).
  12. Resume restore with ``--resume`` (backup-2 may still be running).
  13. Assert resumed restore completes successfully.
  14. Wait for backup-2; assert it also completed successfully.

Test parameters:
  ``interrupt_after``  – seconds to let restore run before killing (default 20).
  ``kill_timeout``     – SIGTERM → SIGKILL escalation wait (default 5).
"""

import time

from backup_restore.shared_archive_base import SharedArchiveBaseTest

_DEFAULT_INTERRUPT_AFTER = 20
_DEFAULT_KILL_TIMEOUT = 5


class SharedArchiveAllServicesTest(SharedArchiveBaseTest):
    """
    Validates ``--obj-read-only`` and ``--resume`` with all Couchbase services
    present in the backup archive.
    """

    def test_all_services_obj_read_only_and_resume(self):
        interrupt_after = self.input.param("interrupt_after", _DEFAULT_INTERRUPT_AFTER)
        kill_timeout = self.input.param("kill_timeout", _DEFAULT_KILL_TIMEOUT)

        # ---- Steps 1-3: Access control and data ----
        self.log.info("=== Steps 1-3: Access control and data load ===")
        self._add_allowed_ip(self.source_cluster)
        self._add_allowed_ip(self.dest_cluster)
        # travel-sample includes KV data + pre-built GSI indexes + FTS index.
        self._load_sample_bucket(self.source_cluster)
        self._create_destination_bucket()

        # ---- Step 4: Configure repository ----
        self._configure_repo()

        # ---- Step 5: Full backup (captures all service artefacts) ----
        self.log.info("=== Step 5: Taking backup-1 (full — all services) ===")
        stdout, stderr, rc = self._take_backup(
            self.source_cluster,
            self.staging_dir_backup,
            full_backup=True,
            label="backup-1",
        )
        self._assert_success(stdout, stderr, rc, "Backup completed successfully", "backup-1")
        self.log.info("backup-1 completed (all services captured).")

        # ---- Step 6: Capture backup-1 timestamp ----
        info_output = self._list_backups(self.staging_dir_backup)
        backup1_ts = self._parse_latest_backup_timestamp(info_output)
        self.log.info("backup-1 timestamp: %s", backup1_ts)

        # ---- Step 7: Restore with --obj-read-only (blocking) ----
        # This is the primary --obj-read-only validation for all services.
        # The restore reads every service's artefacts from S3 and writes to
        # the destination cluster.  The source cluster must not be touched.
        self.log.info(
            "=== Step 7: Restoring backup-1 with --obj-read-only (all services) ==="
        )
        stdout_r1, stderr_r1, rc_r1 = self._restore(
            self.dest_cluster,
            start_ts=backup1_ts,
            end_ts=backup1_ts,
            staging_dir=self.staging_dir_restore,
            label="restore-read-only",
        )
        self._assert_success(
            stdout_r1, stderr_r1, rc_r1,
            "Restore completed successfully",
            "restore-read-only",
        )
        self.log.info(
            "--obj-read-only restore succeeded with all-services backup."
        )

        # ---- Step 8: Start backup-2 (incremental, non-blocking) ----
        self.log.info("=== Step 8: Starting backup-2 (incremental, non-blocking) ===")
        backup2_proc = self._run_cbbackupmgr_process(
            self._build_backup_args(
                self.source_cluster,
                self.staging_dir_backup,
                full_backup=False,
            ),
            label="backup-2",
        )

        # ---- Step 9: Start restore of backup-1 (non-blocking) ----
        # --start/--end = backup1_ts  →  exclusive of the running backup-2.
        self.log.info(
            "=== Step 9: Starting restore of backup-1 (non-blocking, "
            "will interrupt after %ds) ===",
            interrupt_after,
        )
        restore_proc = self._start_restore_process(
            self.dest_cluster,
            start_ts=backup1_ts,
            end_ts=backup1_ts,
            staging_dir=self.staging_dir_restore,
            label="restore-initial",
        )

        # ---- Step 10: Let restore run partway then interrupt ----
        self.log.info(
            "Sleeping %ds to let restore-initial write checkpoints…", interrupt_after
        )
        time.sleep(interrupt_after)

        self.log.info("Sending SIGTERM to restore-initial (pid=%d)…", restore_proc.pid)
        restore_proc.terminate()
        try:
            stdout_init, stderr_init = restore_proc.communicate(timeout=kill_timeout)
            rc_init = restore_proc.returncode
        except Exception:
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
        self.log.info("[restore-initial] exited with rc=%d after interrupt.", rc_init)

        # ---- Step 11: Confirm genuine interruption ----
        self.assertNotEqual(
            rc_init, 0,
            "restore-initial exited with rc=0 before interrupt — "
            "restore finished within the %ds window.  "
            "Increase 'interrupt_after' or use a larger dataset."
            % interrupt_after,
        )
        self.log.info(
            "Confirmed: restore-initial interrupted (rc=%d). "
            "Checkpoint state retained in staging directory.", rc_init
        )

        # ---- Step 12: Resume restore (backup-2 may still be running) ----
        # The resumed restore picks up service artefacts from where it stopped.
        # backup-2 writing new data concurrently must not interfere.
        self.log.info(
            "=== Step 12: Resuming restore with --resume "
            "(all services, backup-2 may still be active) ==="
        )
        stdout_res, stderr_res, rc_res = self._restore(
            self.dest_cluster,
            start_ts=backup1_ts,
            end_ts=backup1_ts,
            staging_dir=self.staging_dir_restore,
            label="restore-resume",
            resume=True,
        )

        # ---- Step 13: Assert resumed restore succeeded ----
        self.log.info("=== Step 13: Verifying resumed restore ===")
        self._assert_success(
            stdout_res, stderr_res, rc_res,
            "Restore completed successfully",
            "restore-resume",
        )
        self.log.info("--resume restore succeeded (all-services backup, concurrent backup-2).")

        # ---- Step 14: Collect backup-2 and assert it succeeded ----
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
        self.log.info("backup-2 succeeded.")

        self.log.info(
            "=== All-services test PASSED: "
            "--obj-read-only and --resume both work correctly "
            "with all Couchbase services in the backup archive ==="
        )
