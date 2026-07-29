import time

from BucketLib.bucket import Bucket
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
            temp_dir="/tmp",
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
