import time

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
