import re
import time
from datetime import datetime, timezone

from BucketLib.bucket import Bucket
from backup_restore.continuous_backup.encrypted_file_validator import (
    aggregate_status, scan_remote_directory)
from pytests.bucket_collections.collections_base import CollectionBase
from shell_util.remote_connection import RemoteMachineShellConnection

"""
NFS setup requirements:
- Server: /data directory exported with appropriate permissions
- Client: Mount NFS export at /mnt/nfs_data
- Validation: Ensure server export and client mount are working
- Cleanup: Unique subdirectory created under mount point and removed after test
Scipts to setup NFS Server : https://github.com/couchbaselabs/test_infra_runner/tree/master/scripts/pitr_scripts
"""


class ContinuousBackupBase(CollectionBase):
    def setUp(self):
        super(ContinuousBackupBase, self).setUp()

        # The interpreter-shutdown guard that used to be armed here now lives
        # in HelperLib.arm_shutdown_guard() (armed by testrunner for every
        # suite), so the same protection covers columnar and other runs too.
        self.bucket = self.cluster.buckets[0]
        self.bucket_name = self.bucket.name
        self.shell = RemoteMachineShellConnection(self.cluster.master)

        # Verify the on-disk encryption state of both backup locations matches
        # what ear_bk / ear_contbk declared. CollectionBase.collection_setup()
        # has already taken the initial cbbackupmgr backup and waited one
        # continuous-backup interval by this point, so both locations have
        # content to scan.
        self._verify_backup_file_encryption_state()

    def tearDown(self):

        # Delete the shell connection if exists
        try:
            self.shell.disconnect()
        except Exception as e:
            self.log.error("Exception during removing shell: %s" % str(e))

        super(ContinuousBackupBase, self).tearDown()

    @staticmethod
    def _parse_cbcontbk_timestamp(ts_str):
        """cbcontbk emits Go-formatted timestamps that Python's
        datetime.fromisoformat() (<3.11) can't parse as-is in two ways:

        1. Fractional seconds beyond 6 digits (e.g. nanoseconds:
           "...T01:24:12.32134144-07:00") -- fromisoformat only accepts
           exactly 0, 3, or 6 fractional digits. Normalize to 6 (truncate
           or zero-pad).
        2. A bare "Z" UTC suffix (e.g. Go's zero-time sentinel for "no data
           yet": "0001-01-01T00:00:00Z") -- fromisoformat before 3.11 only
           accepts an explicit "+00:00" offset, not "Z". Normalize "Z" to
           "+00:00". The zero-time sentinel then parses to year 1, which
           correctly compares as always-older-than-since, so callers don't
           need to special-case it separately -- it just means "keep
           polling" like any other range.end that hasn't reached since yet.
        """
        def _fix_frac(match):
            return "." + (match.group(1) + "000000")[:6]
        ts_str = re.sub(r'\.(\d+)', _fix_frac, ts_str)
        if ts_str.endswith("Z"):
            ts_str = ts_str[:-1] + "+00:00"
        return datetime.fromisoformat(ts_str)

    def _wait_for_continuous_backup_catchup(self, since_timestamp,
                                            timeout=1200, poll_interval=15,
                                            quiet_checkpoints=2):
        """
        Wait for the continuous-backup log to actually cover
        `since_timestamp` (unix epoch seconds) before attempting an
        "everything" (latest) restore.

        Polls `cbcontbk info -l <location> --json`, which reports
        range.end -- cbcontbk's own authoritative bookkeeping of how far
        the continuous log extends for a bucket -- rather than inferring
        completeness from the log store's own object upload timestamps.
        The latter was tried first and found unreliable: in a real CI run
        (build 246480, test_pitr_timestamps_across_rebalance_in) object
        uploads went quiet for 45s+ while `cbcontbk info` run retrospectively
        against that same location showed the log was still ~8 minutes from
        actually catching up -- a heuristic "quiet period" can't distinguish
        a genuine mid-flush lull (128 vbuckets flushing independently) from
        real completion, but cbcontbk's own range.end always can.

        A single crossing of range.end >= since_timestamp is NOT enough,
        though (see docs/agent-context/backup-restore/
        BUG-contbk-info-range-end-lags-actual-data.md): two CI runs restored
        ~3% short even after range.end first confirmed since_timestamp was
        covered, because range.end itself kept climbing for ~11 more
        minutes before settling -- inspecting the collect-logs bundle
        showed every one of 128 vbuckets converges on the identical
        checkpoint instant (not a per-vbucket straggler issue), i.e.
        range.end is published on its own periodic cycle rather than
        continuously. So this waits for range.end to both cover
        since_timestamp AND stop changing for `quiet_checkpoints` full
        continuous_backup_intervals in a row before trusting it -- the same
        quiet-period principle as the discarded object-timestamp heuristic,
        but applied to cbcontbk's own authoritative signal instead of raw
        S3/NFS mtimes, so a genuine multi-cycle climb (as observed) can't
        be mistaken for settling the way a single crossing can.
        """
        since_dt = datetime.fromtimestamp(since_timestamp, tz=timezone.utc)
        quiet_period = quiet_checkpoints * self.continuous_backup_interval * 60
        deadline = time.time() + timeout
        last_range_end_str = None
        last_range_end_dt = None
        quiet_since = None
        while time.time() < deadline:
            info = self.cont_bk_mgr.info(self.continuous_backup_location)
            bucket_info = (info or {}).get(self.bucket.name, {})
            range_end_str = bucket_info.get("range", {}).get("end")
            if range_end_str:
                range_end_dt = self._parse_cbcontbk_timestamp(range_end_str)
                last_range_end_str = range_end_str
                if range_end_dt >= since_dt:
                    if range_end_dt != last_range_end_dt:
                        last_range_end_dt = range_end_dt
                        quiet_since = time.time()
                    elif time.time() - quiet_since >= quiet_period:
                        self.log.info(
                            f"Continuous backup log confirmed caught up "
                            f"via cbcontbk info: range.end={range_end_str} "
                            f">= since={since_dt.isoformat()}, unchanged "
                            f"for {quiet_period}s ({quiet_checkpoints} "
                            f"checkpoint cycles)")
                        # range.end settling isn't sufficient on its own --
                        # a restore right after can still come back short
                        # (observed: expected=20000, actual=19300). One more
                        # interval gives the underlying data time to land.
                        self.sleep(
                            self.continuous_backup_interval * 60,
                            "Buffering one more backup interval after "
                            "catch-up settled, before trusting it")
                        return
            self.sleep(
                poll_interval,
                f"Waiting for continuous backup log to catch up and "
                f"settle (cbcontbk info range.end={last_range_end_str}, "
                f"since={since_dt.isoformat()})")
        self.fail(
            f"Continuous backup log did not catch up to "
            f"{since_dt.isoformat()} within {timeout}s (last cbcontbk info "
            f"range.end: {last_range_end_str})")

    def _verify_doc_count(self, expected_count, bucket_name=None, timeout=300):
        if bucket_name is None:
            bucket_name = self.bucket.name
        self.log.info(f"Verifying document count for bucket '{bucket_name}'. Expected: {expected_count}")
        end_time = time.time() + timeout
        while time.time() < end_time:
            actual_items = self.bucket_util.get_buckets_item_count(self.cluster, bucket_name)
            if actual_items == expected_count:
                self.log.info(f"Document count for bucket '{bucket_name}' verified: {actual_items}")
                return
            self.log.info(f"Current doc counts for bucket '{bucket_name}'. Actual: {actual_items}, Expected: {expected_count}. Retrying in 10s...")
            self.sleep(10)
        self.fail(f"Document count mismatch for bucket '{bucket_name}'. Expected: {expected_count}, Actual: {actual_items}")

    def _assert_restore_succeeded(self, output, error, timestamp):
        """CbContBk.restore only logs failures and returns (output, error),
        so unchecked calls let a failed/partial restore surface much later
        as a doc-count mismatch. Fail here with the cbcontbk output instead."""
        error_lines = [line for line in (output or [])
                       if line.strip().lower().startswith("error")]
        if error or not output or error_lines:
            self.fail(f"cbcontbk restore to timestamp {timestamp} failed. "
                      f"stdout: {output}, stderr: {error}")

    def _create_restore_bucket(self, restore_bucket_name):
        self.log.info("Creating new bucket for restore: %s" % restore_bucket_name)
        ram_quota = self.input.param("bucket_size", 100)
        # Flush-enabled so tests can reset the bucket between restores
        # via flush instead of delete + recreate.
        self.bucket_util.create_default_bucket(self.cluster,
                                               bucket_name=restore_bucket_name,
                                               bucket_type=self.bucket_type,
                                               ram_quota=ram_quota,
                                               replica=self.num_replicas,
                                               storage=self.bucket_storage,
                                               flush_enabled=Bucket.FlushBucket.ENABLED)

    def _restore_entire_bucket(self, timestamp, target_bucket_name,
                               include_data=None, map_data=None,
                               assert_success=True):
        """Restore the whole bucket at `timestamp` into `target_bucket_name`.

        Always returns the raw (output, error) from cbcontbk. By default
        asserts the restore succeeded; pass assert_success=False to inspect an
        expected failure at the call site instead (e.g. retention deleted data)."""
        self.log.info(f"Restoring entire bucket to {target_bucket_name} at timestamp {timestamp}")
        if map_data is None:
            map_data = f"{self.bucket.name}={target_bucket_name}"
        output, error = self.cont_bk_mgr.restore(
            self.backup_archive_dir, self.backup_repo_name,
            location=self.continuous_backup_location,
            temp_dir="/data/tmp",
            timestamp=timestamp,
            include_data=include_data,
            map_data=map_data,
            obj_staging_dir=self.obj_staging_dir_cont_bkp
        )
        if assert_success:
            self._assert_restore_succeeded(output, error, timestamp)
            self.log.info("Entire bucket restore completed")
        return output, error

    def _flush_restore_bucket(self, restore_bucket_name):
        """Flush the restore bucket back to an empty state and verify it.

        Flush matches how users reset a restore target (they don't delete
        and recreate buckets between restores), and cbcontbk restore needs
        an empty bucket to produce the full expected count."""
        restore_bucket_obj = self.bucket_util.get_bucket_obj(
            self.cluster.buckets, restore_bucket_name)
        if restore_bucket_obj is None:
            self.fail(f"Restore bucket '{restore_bucket_name}' not found for flush")
        if not self.bucket_util.flush_bucket(self.cluster, restore_bucket_obj):
            self.fail(f"Flush of restore bucket '{restore_bucket_name}' failed")
        self._verify_doc_count(0, bucket_name=restore_bucket_name)

    def _verify_backup_file_encryption_state(self):
        """
        Check that the files on disk under `backup_archive_dir` and `continuous_backup_location` match
        what `ear_bk` and `ear_contbk` declared. Fails the test on mismatch.
        Catches wiring regressions where an ear_* flag silently fails to take effect (doc-count/content
        assertions in tests would otherwise pass regardless).

        Each surface is scanned independently and only when it's a local / NFS path.
        The file-format validator's remote scan uses shell commands (`find`, `head -c`) that don't reach into
        object-store URLs (s3://, gs://, az://).
        A surface backed by cloud storage is skipped with a per-surface warning while the other surface is still checked.
        """
        if not (self.ear_bk or self.ear_contbk):
            return

        # Absolute POSIX paths (NFS / single-node local) start with `/`.
        # Anything else (`s3://`, `gs://`, `az://`) can't be shell-scanned.
        def _is_scannable(location):
            return bool(location) and location.startswith("/")

        surfaces = [
            (self.backup_archive_dir, self.ear_bk, "ear_bk"),
            (self.continuous_backup_location, self.ear_contbk, "ear_contbk"),
        ]
        scannable = [(loc, expected, name) for loc, expected, name in surfaces
                     if _is_scannable(loc)]
        for loc, expected, name in surfaces:
            if not _is_scannable(loc):
                self.log.warning(
                    f"_verify_backup_file_encryption_state: {name} surface "
                    f"backed by non-local location {loc!r}; skipping "
                    f"on-disk encryption check for it. (expected_encrypted="
                    f"{expected})")

        if not scannable:
            return

        shell = RemoteMachineShellConnection(self.cluster.master)
        try:
            for location, expected_encrypted, flag_name in scannable:
                self._check_location_encryption(
                    shell, location,
                    expected_encrypted=expected_encrypted,
                    flag_name=flag_name)
        finally:
            shell.disconnect()

    def _check_location_encryption(self, shell, location, expected_encrypted,
                                   flag_name):
        """
        Scan `location` on the given shell and assert its aggregate encryption
        state matches `expected_encrypted`. When expected_encrypted is True,
        we require at least one file to carry the Couchbase Encrypted magic
        (partial or full — metadata files in a valid encrypted archive stay
        plaintext, so "full" is not always achievable). When False, we
        require zero encrypted files.
        """
        scan = scan_remote_directory(shell, location)
        status = aggregate_status(scan)
        self.log.info(
            f"_check_location_encryption: {flag_name}={expected_encrypted}, "
            f"location={location}, aggregate_status={status}, "
            f"files_scanned={len(scan)}")
        if expected_encrypted and status == "unencrypted":
            self.fail(
                f"{flag_name}=True but no files under {location} carry the "
                f"Couchbase Encrypted magic. The encryption flag did not "
                f"take effect on this surface.")
        if not expected_encrypted and status != "unencrypted":
            self.fail(
                f"{flag_name}=False but files under {location} carry the "
                f"Couchbase Encrypted magic (state: {status}). The "
                f"unencrypted-side surface was accidentally encrypted.")

    def load_data_cbc_pillowfight(self, bucket=None, server=None,
                                  total_data_mb=1, doc_size=1024,
                                  key_prefix="contbk_docs", threads=1,
                                  ops_rate=None, persist_wait=30):
        """
        Seed a bucket with real documents using cbc-pillowfight so
        backup/restore tests have data to validate against.

        Defaults load ~1 MB of data as 1 KB documents (i.e. 1024 docs).
        Modelled on storage.storage_base.StorageBase.load_data_cbc_pillowfight
        so any ContinuousBackupBase subclass (WORM, PITR, etc.) can reuse it.

        :param bucket: bucket object to load into (default: self.bucket)
        :param server: node to run cbc-pillowfight from (default: cluster master)
        :param total_data_mb: total data volume to load, in MB (default 1)
        :param doc_size: size of each document in bytes (default 1024 = 1 KB)
        :param key_prefix: key prefix for the generated documents
        :param threads: number of pillowfight worker threads
        :param ops_rate: optional --rate-limit value
        :param persist_wait: seconds to wait for docs to persist before counting
        :returns: number of items in the bucket after loading
        """
        server = server or self.cluster.master
        bucket = bucket or self.bucket
        items = max(1, (total_data_mb * 1024 * 1024) // doc_size)
        self.log.info("Loading %d docs of %d bytes (~%d MB) into bucket '%s' "
                      "with cbc-pillowfight (key_prefix=%s)"
                      % (items, doc_size, total_data_mb, bucket.name,
                         key_prefix))
        shell = RemoteMachineShellConnection(server)
        try:
            cmd = ("/opt/couchbase/bin/cbc-pillowfight "
                   "-U couchbase://{ip}/{bkt} -u {user} -P {pwd} "
                   "-I {items} -t {threads} -m {size} -M {size} "
                   "--populate-only --random-body --key-prefix={prefix} "
                   "-Dtimeout=10").format(
                       ip=server.ip, bkt=bucket.name,
                       user=self.cluster.master.rest_username,
                       pwd=self.cluster.master.rest_password,
                       items=items, threads=threads, size=doc_size,
                       prefix=key_prefix)
            if ops_rate is not None:
                cmd += " --rate-limit {}".format(ops_rate)
            self.log.info("Executing pillowfight command: %s" % cmd)
            output, error = shell.execute_command(cmd, timeout=600)
            self.log.debug("pillowfight output=%s error=%s" % (output, error))
        finally:
            shell.disconnect()

        self.sleep(persist_wait, "Wait for pillowfight docs to persist")
        self.bucket_util._wait_for_stats_all_buckets(
            self.cluster, self.cluster.buckets)
        loaded = self.bucket_util.get_buckets_item_count(
            self.cluster, bucket.name)
        self.log.info("Bucket '%s' now reports %d items after pillowfight load"
                      % (bucket.name, loaded))

        return loaded
