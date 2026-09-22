import json

from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_server_rest_util.fusion.fusion_api import FusionRestAPI
from shell_util.remote_connection import RemoteMachineShellConnection
from storage.fusion.fusion_base import FusionBase
from storage.magma.magma_base import MagmaBaseTest


class FusionCheckpointGating(MagmaBaseTest, FusionBase):
    def setUp(self):
        super(FusionCheckpointGating, self).setUp()
        self.target_kvstore = self.input.param("target_kvstore", 0)
        self.block_log_count = self.input.param("block_log_count", 100)
        self.janitor_intervals = self.input.param("janitor_intervals", 4)
        self.upsert_iterations = self.input.param("upsert_iterations", 2)
        # nfs_server_path is the export on the NFS server, these commands run
        # on the cluster node where the same share is mounted
        self.log_store_path = self.fusion_log_store_uri.split("://")[-1]
        self.sync_manager_pending_bytes = self.input.param(
            "sync_manager_pending_bytes", 48318382080)
        self.sync_manager_lwm_percentage = self.input.param(
            "sync_manager_lwm_percentage", 60)
        # Corrects a base class default, so it runs after super().setUp()
        self.enable_sync_manager(self.cluster.buckets[0])

    def run_on_master(self, cmd):
        ssh = RemoteMachineShellConnection(self.cluster.master)
        output, error = ssh.execute_command(cmd)
        ssh.disconnect()
        return "\n".join(output), "\n".join(error)

    def enable_sync_manager(self, bucket):
        '''
        Turns the Fusion sync manager on, and fails if it did not take

        magma_base setUp pushes fusion_max_pending_upload_bytes, whose TAF
        default of 0 gives a zero low watermark. canSync() then returns OK
        before the sync manager thresholds are read, so a periodic sync never
        returns RetryLater and the log store janitor is never called. Read
        back rather than assumed, the setting is cluster wide and sticky
        '''
        ClusterRestAPI(self.cluster.master).manage_global_memcached_setting(
            fusion_max_pending_upload_bytes=self.sync_manager_pending_bytes,
            fusion_max_pending_upload_bytes_lwm_percentage=self.sync_manager_lwm_percentage)
        self.sleep(30, "Wait for the sync manager setting to apply")

        stats = self.get_cbstats_all_stats(self.cluster.master, bucket.name)
        max_pending = int(stats["fusion_max_pending_upload_bytes"])
        ratio = float(stats["fusion_max_pending_upload_bytes_lwm_ratio"])

        if max_pending * ratio <= 0:
            self.fail(f"The sync manager is disabled, max pending upload bytes "
                      f"{max_pending} and low watermark ratio {ratio} give a "
                      f"zero low watermark. Every sync then returns early and "
                      f"the log store janitor is never reached, so this test "
                      f"cannot observe the behaviour it asserts")

        self.log.info(f"Sync manager enabled, max pending upload bytes "
                      f"{max_pending}, low watermark ratio {ratio}")

    def load_with_pillowfight(self, bucket, items):
        '''
        Loads or rewrites 'items' keys over the same key range

        Rerunning this upserts every key, which is what turns the logs already
        on the log store into garbage without changing the item count
        '''
        cmd = ("/opt/couchbase/bin/cbc-pillowfight "
               f"-U couchbase://{self.cluster.master.ip}/{bucket.name} "
               f"-u {self.cluster.master.rest_username} "
               f"-P {self.cluster.master.rest_password} "
               f"-I {items} -m {self.doc_size} -M {self.doc_size} "
               "--populate-only -t 4")
        self.log.info(f"Writing {items} items of {self.doc_size} bytes")
        self.run_on_master(cmd)

    def get_checkpoint(self, volume_id):
        cmd = ("/opt/couchbase/bin/fusion/metadata_dump "
               f"--uri chronicle://{self.cluster.master.ip}:8091 "
               f"--volume-id {volume_id}")
        output, error = self.run_on_master(cmd)
        try:
            return json.loads(output)["checkpoint"]["logID"]
        except (ValueError, KeyError):
            self.fail(f"Could not read the checkpoint for {volume_id}. "
                      f"Output: {output}, Error: {error}")

    def get_logs_on_store(self, volume_dir):
        output, _ = self.run_on_master(f"ls {volume_dir}")
        return [name for name in output.split()
                if name.startswith("log-") and not name.endswith(".tmp")]

    def map_log_ids_to_files(self, log_files):
        '''
        A part log is named log-<term>.<seqno>.p but referenced without the .p
        '''
        return {name[:-2] if name.endswith(".p") else name: name
                for name in log_files}

    def get_logs_named_by_checkpoint(self, volume_dir, checkpoint_file):
        '''
        Returns the log IDs the checkpoint's own manifest references, or None
        when the checkpoint log cannot be read. The first entry is the
        checkpoint itself and is dropped
        '''
        cmd = f"/opt/couchbase/bin/fusion/log_dump -l {volume_dir}/{checkpoint_file}"
        output, _ = self.run_on_master(cmd)
        if "Unable to open" in output:
            return None

        log_ids = list()
        for line in output.splitlines():
            line = line.strip()
            if line.startswith("LogID: Term="):
                term, seqno = line.replace("LogID: Term=", "").split(" Seqno=")
                log_ids.append(f"log-{term.strip()}.{seqno.strip()}")
        return log_ids[1:]

    def validate_checkpoint_referenced_logs(self, volume_id, volume_dir, label):
        '''
        The MB-73999 invariant, logging the checkpoint chronicle still holds
        against what is actually on the log store, so a deletion and the
        checkpoint left pointing at it can be read out of the test log

        Returns (ok, message)
        '''
        checkpoint = self.get_checkpoint(volume_id)
        log_files = sorted(self.get_logs_on_store(volume_dir))
        present = self.map_log_ids_to_files(log_files)
        self.log.info(f"{label}: chronicle checkpoint is {checkpoint}, logs on "
                      f"the log store are {log_files}")

        if checkpoint not in present:
            return False, (f"Chronicle still points at checkpoint {checkpoint} "
                           f"but that log has been deleted. Logs present: "
                           f"{log_files}")

        referenced = self.get_logs_named_by_checkpoint(volume_dir,
                                                       present[checkpoint])
        if referenced is None:
            return False, f"Could not read the manifest of {checkpoint}"

        missing = [log_id for log_id in referenced if log_id not in present]
        self.log.info(f"{label}: checkpoint {checkpoint} references "
                      f"{referenced}, of which {missing if missing else 'none'} "
                      f"have been deleted")
        if missing:
            return False, (f"Checkpoint {checkpoint} still references "
                           f"{referenced} but {missing} have been deleted. "
                           f"Logs present: {log_files}")
        return True, (f"All {len(referenced)} logs referenced by {checkpoint} "
                      f"are present")

    def block_final_log_writes(self, volume_dir, checkpoint):
        '''
        Makes the next 'block_log_count' log names undeletable directories

        A log is written as <name>.tmp and renamed, so a directory at that path
        fails the write with EISDIR. Part logs carry a .p suffix and are not
        blocked, so the sync splits and retires logs before it fails
        '''
        seqno = int(checkpoint.split(".")[1])
        names = " ".join(f"{volume_dir}/log-1.{seqno + i}.tmp"
                         for i in range(1, self.block_log_count + 1))
        self.run_on_master(f"mkdir -p {names}")
        self.log.info(f"Blocked log-1.{seqno + 1} to "
                      f"log-1.{seqno + self.block_log_count}")

    def unblock_final_log_writes(self, volume_dir):
        '''
        Released as soon as the sync has failed and before the janitor window,
        otherwise the janitor's own writes fail and hide the behaviour tested
        '''
        self.run_on_master(f"rmdir {volume_dir}/log-1.*.tmp 2>/dev/null")
        self.log.info("Released the blocked log names")

    def get_failed_sync_count(self):
        cmd = ("cat /opt/couchbase/var/lib/couchbase/logs/memcached.log* "
               "2>/dev/null | grep createFusionCheckpoint "
               f"| grep 'kvstore-{self.target_kvstore}/' | grep status:IOError")
        output, _ = self.run_on_master(cmd)
        lines = [line for line in output.splitlines() if line.strip()]
        return len(lines), (lines[-1] if lines else None)

    def log_sync_timeline(self, label, tail=8):
        '''
        Logs the sync outcomes memcached recorded for the target kvstore

        The janitor's deletes are not written to memcached.log at all, so this
        is the only server side record available, and it shows the volume
        syncing cleanly right up to the injected fault
        '''
        cmd = ("cat /opt/couchbase/var/lib/couchbase/logs/memcached.log* "
               "2>/dev/null | grep createFusionCheckpoint "
               f"| grep 'kvstore-{self.target_kvstore}/' | tail -{tail}")
        output, _ = self.run_on_master(cmd)
        self.log.info(f"{label}: last {tail} syncs on "
                      f"kvstore-{self.target_kvstore}")
        for line in output.splitlines():
            if line.strip():
                self.log.info(f"    {line.strip()}")

    def get_fusion_log_stats(self, bucket):
        '''
        These counters are bucket wide, summed over every kvstore
        '''
        stats = self.get_cbstats_all_stats(self.cluster.master, bucket.name)
        return {
            "pending_delete_size": int(
                stats["ep_fusion_log_store_pending_delete_size"]),
            "logs_cleaned": int(stats["ep_fusion_logs_cleaned"]),
            "remote_deletes": int(
                stats["ep_fusion_log_store_remote_deletes"]),
        }

    def test_log_delete_gated_on_checkpoint(self):
        '''
        MB-73999: a log the checkpoint's manifest still references must not be
        deleted by the log store janitor

        A sync that fails part way retires logs into the pending delete list
        but never advances the checkpoint. The janitor then drains that list
        without checking the checkpoint moved, so the volume is left pointing
        at logs that no longer exist and cannot be rebalanced
        '''
        bucket = self.cluster.buckets[0]
        volume_id = (f"kv/{self.get_bucket_uuid(bucket.name)}"
                     f"/kvstore-{self.target_kvstore}")
        volume_dir = f"{self.log_store_path}/{volume_id}"

        self.load_with_pillowfight(bucket, self.num_items)
        FusionRestAPI(self.cluster.master).sync_log_store()
        self.sleep(30, "Wait for the sync after the initial load")

        # A healthy split and clean cycle. The rewrites turn the logs written
        # above into garbage, so this sync retires them legitimately
        for _ in range(self.upsert_iterations):
            self.load_with_pillowfight(bucket, self.num_items)
        FusionRestAPI(self.cluster.master).sync_log_store()
        self.sleep(60, "Wait for the log store to settle")

        gated, message = self.validate_checkpoint_referenced_logs(
            volume_id, volume_dir, "Control")
        if not gated:
            self.fail(f"The volume was already broken before any fault was "
                      f"injected, nothing below is meaningful: {message}")
        self.log.info(f"Control: {message}")

        self.log.info("Creating garbage for the janitor to retire")
        self.load_with_pillowfight(bucket, self.num_items)

        stats_before = self.get_fusion_log_stats(bucket)
        failed_before, _ = self.get_failed_sync_count()

        # Read last, a periodic sync between this and the block below would
        # move the checkpoint and the blocked names would already be spent
        checkpoint_before = self.get_checkpoint(volume_id)
        self.block_final_log_writes(volume_dir, checkpoint_before)
        try:
            FusionRestAPI(self.cluster.master).sync_log_store()
            self.sleep(30, "Wait for the split sync to fail")
        finally:
            self.unblock_final_log_writes(volume_dir)

        failed_after, failure_line = self.get_failed_sync_count()
        if failed_after <= failed_before:
            self.fail(f"The sync did not fail, so nothing was retired behind a "
                      f"stale checkpoint and the run proves nothing. Failed "
                      f"syncs on kvstore-{self.target_kvstore} "
                      f"{failed_before} -> {failed_after}")
        self.log.info(f"The split sync failed: {failure_line}")

        checkpoint_after = self.get_checkpoint(volume_id)
        if checkpoint_after != checkpoint_before:
            self.fail(f"The checkpoint advanced {checkpoint_before} -> "
                      f"{checkpoint_after} even though the sync failed, so the "
                      f"volume is healthy and the run proves nothing")

        stats_after = self.get_fusion_log_stats(bucket)
        if stats_after["pending_delete_size"] <= stats_before["pending_delete_size"]:
            self.fail(f"The failed sync retired no logs, so the janitor has "
                      f"nothing to delete. Pending delete size "
                      f"{stats_before['pending_delete_size']} -> "
                      f"{stats_after['pending_delete_size']}")

        self.log.info(f"Checkpoint stranded at {checkpoint_before}, pending "
                      f"delete size {stats_before['pending_delete_size']} -> "
                      f"{stats_after['pending_delete_size']}")
        self.log_sync_timeline("After the failed sync")
        self.validate_checkpoint_referenced_logs(
            volume_id, volume_dir, "After the failed sync")

        violation = None
        for interval in range(self.janitor_intervals):
            self.sleep(self.fusion_upload_interval + 10,
                       f"Janitor window {interval + 1}/{self.janitor_intervals}")
            checkpoint_now = self.get_checkpoint(volume_id)
            stats_now = self.get_fusion_log_stats(bucket)
            self.log.info(f"Janitor window {interval + 1}: checkpoint "
                          f"{checkpoint_now}, pending delete size "
                          f"{stats_now['pending_delete_size']}, logs cleaned "
                          f"{stats_now['logs_cleaned']}, remote deletes "
                          f"{stats_now['remote_deletes']}")

            if checkpoint_now != checkpoint_before:
                self.fail(f"The checkpoint advanced {checkpoint_before} -> "
                          f"{checkpoint_now} during the janitor window, so the "
                          f"volume healed itself and the run proves nothing")

            gated, message = self.validate_checkpoint_referenced_logs(
                volume_id, volume_dir, f"Janitor window {interval + 1}")
            if not gated:
                violation = message
                self.log.error(f"Log deletion was not gated on the checkpoint. "
                               f"{message}")
                # Syncs kept succeeding after the block came off, so the
                # checkpoint stayed put while its log was deleted underneath it
                self.log_sync_timeline("At the violation")
                break

        if violation is None:
            # Nothing observable says the janitor ran and declined: once gated
            # it deletes nothing, drains nothing and logs nothing. That it was
            # reached at all rests on the sync manager check in setUp, since a
            # RetryLater sync is the only caller of doLogStoreCleanup
            self.log.info(f"Log deletion was gated on the checkpoint. {message}")

        # A volume whose checkpoint names a deleted log cannot be accelerated,
        # so the rebalance is expected to fail exactly when the invariant broke
        self.log.info("Running a Fusion rebalance")
        self.run_rebalance(output_dir=self.fusion_output_dir,
                           rebalance_count=1,
                           expect_rebalance_failure=violation is not None)

        if violation is not None:
            self.fail(f"MB-73999: log deletion was not gated on the checkpoint. "
                      f"{violation}")
