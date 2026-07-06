"""
Shared Backup Archive – Failover Scenario 1

Scenario:
  While a restore is in progress on the destination cluster using the second
  cbbackupmgr instance (--obj-read-only), hard-failover one node on the
  SOURCE cluster (the cluster where the concurrent backup is being taken).
  Validates that:
    - The restore completes successfully (reads only from S3, not from source).
    - The concurrent backup either completes or fails gracefully.

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
        c. After a brief delay, hard-failover one non-orchestrator node on
           the source cluster (while backup + restore are in flight).
  8.  Assert restore completed successfully.
  9.  Log backup result (may succeed or fail — both are acceptable outcomes
      given the mid-backup failover).
  10. Rebalance source cluster to eject the failed-over node.
  11. Wait for source cluster to return to healthy state.

Test parameters:
  ``failover_delay_secs`` – seconds to wait after starting backup + restore
                            before triggering failover (default: 30).
"""

import threading
import time

from backup_restore.shared_archive_base import SharedArchiveBaseTest


class SharedArchiveFailoverSrcTest(SharedArchiveBaseTest):
    """
    Restore to destination succeeds while source cluster node is failed over
    mid-backup.
    """

    def test_failover_src_node_during_backup_restore(self):
        failover_delay = self.input.param("failover_delay_secs", 30)

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

        # ---- Step 7: Concurrent backup + restore + failover ----
        self.log.info(
            "=== Step 7: Starting concurrent backup + restore; "
            "will failover a source node after %ds ===",
            failover_delay,
        )

        # Identify the orchestrator and pick a safe failover target.
        all_otp_nodes, node_to_failover = self._pick_non_orchestrator_node(
            self.source_cluster
        )
        self.log.info("Node selected for failover: %s", node_to_failover)

        backup2_result = {}
        restore_result = {}
        failover_result = {}

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

        def run_failover():
            self.log.info(
                "Failover thread sleeping %ds before triggering failover…",
                failover_delay,
            )
            time.sleep(failover_delay)
            try:
                rc = self._hard_failover_node(self.source_cluster, node_to_failover)
                failover_result["rc"] = rc
            except Exception as exc:
                failover_result["rc"] = -1
                failover_result["exc"] = exc

        backup_thread = threading.Thread(target=run_second_backup, name="backup-2")
        restore_thread = threading.Thread(target=run_restore, name="restore-1")
        failover_thread = threading.Thread(target=run_failover, name="failover-src")

        backup_thread.start()
        restore_thread.start()
        failover_thread.start()

        backup_thread.join()
        restore_thread.join()
        failover_thread.join()

        # ---- Step 8: Assert restore succeeded ----
        self.log.info("=== Step 8: Verifying restore result ===")
        self._assert_success(
            restore_result.get("stdout", ""),
            restore_result.get("stderr", ""),
            restore_result.get("rc", -1),
            "Restore completed successfully",
            "restore-1",
        )
        self.log.info("Restore succeeded despite source cluster node failover.")

        # ---- Step 9: Log backup result (informational) ----
        backup_rc = backup2_result.get("rc", -1)
        if backup_rc == 0:
            self.log.info(
                "Concurrent backup (backup-2) also completed successfully "
                "(cbbackupmgr tolerated the mid-backup failover)."
            )
        else:
            self.log.warning(
                "Concurrent backup (backup-2) failed (rc=%d) — expected when "
                "a node is hard-failed-over mid-stream.\nSTDOUT: %s\nSTDERR: %s",
                backup_rc,
                backup2_result.get("stdout", ""),
                backup2_result.get("stderr", ""),
            )

        # ---- Step 10: Rebalance source cluster to eject failed-over node ----
        self.log.info("=== Step 10: Rebalancing source cluster to eject failed node ===")
        self._rebalance_cluster(
            self.source_cluster,
            known_nodes=all_otp_nodes,
            eject_nodes=[node_to_failover],
        )

        # ---- Step 11: Wait for source cluster to stabilise ----
        self.log.info("=== Step 11: Waiting for source cluster to reach healthy state ===")
        self._wait_for_cluster_healthy(cluster=self.source_cluster)

        self.log.info(
            "=== Failover-src concurrent backup + restore test PASSED ==="
        )
