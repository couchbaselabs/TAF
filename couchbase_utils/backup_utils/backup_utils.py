import time
import traceback

from BucketLib.bucket import Bucket
from cb_server_rest_util.backup.backup_api import BackupRestApi
from global_vars import logger
from couchbase_utils.cb_tools.cbbackupmgr import CbBackupMgr
from couchbase_utils.cb_tools.cbcontbk import CbContBk


class ContinuousBackupUtil(object):
    def __init__(self, shell_conn, username, password, log=None):
        self.shell_conn = shell_conn
        self.username = username
        self.password = password
        self.log = log if log else logger.get("test")

        self.backup_mgr = CbBackupMgr(shell_conn,
                                       username=username,
                                       password=password,
                                       log=self.log)
        self.cont_bk_mgr = CbContBk(shell_conn,
                                     username=username,
                                     password=password,
                                     log=self.log)

    def enable_continuous_backup(self, bucket_util, cluster, buckets, continuous_backup_location="/tmp/cont_bkp", continuous_backup_interval=5):
        """Enable continuous backup on all buckets in the cluster
        Args:
            bucket_util: BucketUtil instance for bucket operations
            cluster: Cluster object
            buckets: List of Bucket objects to enable CB on
            continuous_backup_location: Location for continuous backup files
            continuous_backup_interval: Continuous backup interval in minutes
        """
        self.log.info("Enabling continuous backup on all buckets")
        for bucket in buckets:
            bucket_util.update_bucket_property(
                cluster.master, bucket,
                continuous_backup_enabled=True,
                continuous_backup_location=continuous_backup_location,
                continuous_backup_interval=continuous_backup_interval)
        self.log.info(f"Waiting 10 seconds for continuous backup to be enabled")
        time.sleep(10)

    def verify_continuous_backup_params(self, bucket_util, cluster, buckets):
        """Verify continuous backup parameters are set correctly"""
        for bucket in buckets:
            params = bucket_util.get_continuous_backup_params(cluster, bucket.name)
            self.log.info(f"Bucket {bucket.name} continuous backup params:")
            for key, value in params.items():
                self.log.info(f"  - {key}: {value}")
            expected_enabled = "true"
            actual_enabled = str(params.get("continuousBackupEnabled", "")).lower()
            if actual_enabled != expected_enabled:
                raise AssertionError(
                    f"continuousBackupEnabled mismatch for bucket {bucket.name}")

    def verify_backup_and_restore(self, bucket_util, cluster, buckets, backup_archive_dir="/tmp/archive",
                                   backup_repo_name="repo1", continuous_backup_location="/tmp/cont_bkp",
                                   continuous_backup_interval=5):
        """
        Verify traditional restore and continuous restore (PITR) after rebalance

        NOTE: This method assumes that backup repository has already been created
        and initial traditional backup has been taken (e.g., by the base class setup
        when cont_bkp_test=NFS is set). This method only performs restore verification.
        Args:
            bucket_util: BucketUtil instance for bucket operations
            cluster: Cluster object
            buckets: List of Bucket objects to verify
            backup_archive_dir: Directory for backup archives
            backup_repo_name: Name of backup repository
            continuous_backup_location: Location for continuous backup files
            continuous_backup_interval: Continuous backup interval in minutes
        """
        self.log.info("=" * 80)
        self.log.info("Starting backup and restore verification")
        self.log.info("=" * 80)

        # Poll until a backup appears in the repo, up to 2x the configured interval
        self.log.info(f"Waiting for continuous backup to complete (interval={continuous_backup_interval} min)...")
        deadline = time.time() + continuous_backup_interval * 120
        backed_up = False
        while time.time() < deadline:
            output, _ = self.backup_mgr.list_backups(backup_archive_dir, backup_repo_name)
            if output and any(line.strip() for line in output):
                self.log.info("Backup data detected in repository")
                backed_up = True
                break
            time.sleep(15)
        if not backed_up:
            self.log.warning("No backup data detected within timeout; proceeding anyway")

        # Get document counts before any operations
        original_item_counts = {}
        for bucket in buckets:
            original_item_counts[bucket.name] = \
                bucket_util.get_buckets_item_count(cluster, bucket.name)
            self.log.info(f"Original item count for {bucket.name}: {original_item_counts[bucket.name]}")

        try:
            timestamp_before_restore = self.cont_bk_mgr.get_cluster_timestamp()
            self.log.info(f"Timestamp captured: {timestamp_before_restore}")
        except Exception as e:
            raise AssertionError(f"Failed to get cluster timestamp: {e}\n{traceback.format_exc()}")

        # Loop through each bucket for continuous restore verification
        for bucket in buckets:
            if (bucket.storageBackend != Bucket.StorageBackend.magma or bucket.bucketType == 'ephemeral' or
                    bucket.name == "default"):
                self.log.info(f"Skipping continuous restore verification for {bucket.name}: "
                              f"storage backend is {bucket.storageBackend}, not magma")
                continue
            restore_bucket_name = f"{bucket.name}_cont_restore_{int(time.time())}"
            self.log.info(f"Performing continuous restore (PITR) for bucket: {bucket.name}")
            self._create_restore_bucket(bucket_util, cluster, restore_bucket_name, bucket)
            try:
                cluster_host = f"http://{cluster.master.ip}:8091"
                output, error = self.cont_bk_mgr.restore(
                    backup_archive_dir, backup_repo_name,
                    cluster_host=cluster_host,
                    location=continuous_backup_location,
                    temp_dir="/tmp",
                    timestamp=timestamp_before_restore,
                    map_data=f"{bucket.name}={restore_bucket_name}")

                combined_output = (output or []) + (error or [])
                skip_messages = [
                    "a required backup has been removed",
                    "traditional backup has the same or newer data than the log backup"
                ]
                if any(msg in line for line in combined_output for msg in skip_messages):
                    self.log.warning(f"Skipping restore verification for {bucket.name}: "
                                     f"{next(msg for msg in skip_messages for line in combined_output if msg in line)}")
                    continue

                bucket_util._wait_for_stats_all_buckets(cluster, cluster.buckets)

                end_time = time.time() + 300  # 5 minutes
                while time.time() < end_time:
                    expected_count = bucket_util.get_buckets_item_count(cluster, bucket.name)
                    cont_restored_count = bucket_util.get_buckets_item_count(cluster, restore_bucket_name)
                    self.log.info(f"Continuous restore count: {cont_restored_count}, Expected: {expected_count}")
                    if cont_restored_count == expected_count:
                        break
                    time.sleep(10)
                assert cont_restored_count == expected_count, \
                    f"Continuous restore failed for {bucket.name}: got {cont_restored_count}, expected {expected_count}"
            except AssertionError as e:
                raise e
            except Exception as e:
                raise AssertionError(f"Continuous restore verification failed for {bucket.name}: {e}\n{traceback.format_exc()}")
            finally:
                restore_bucket_obj = bucket_util.get_bucket_obj(cluster.buckets, restore_bucket_name)
                if restore_bucket_obj:
                    bucket_util.delete_bucket(cluster, restore_bucket_obj)

        self.log.info("Backup and restore verification completed successfully")
        self.log.info("=" * 80)

    def _create_restore_bucket(self, bucket_util, cluster, restore_bucket_name, source_bucket):
        """Create a new bucket for restore"""
        self.log.info(f"Creating restore bucket: {restore_bucket_name}")
        bucket_type = source_bucket.bucketType
        replica = source_bucket.replicaNumber
        storage = source_bucket.storageBackend
        ram_quota = max(source_bucket.ramQuotaMB, 1024) if storage == Bucket.StorageBackend.magma else source_bucket.ramQuotaMB

        bucket_util.create_default_bucket(
            cluster,
            bucket_name=restore_bucket_name,
            bucket_type=bucket_type,
            ram_quota=ram_quota,
            replica=replica,
            storage=storage)


class BackupUtil(object):
    def __init__(self, cluster_node):
        self.server = cluster_node
        self.rest = BackupRestApi(self.server)
        self.log = logger.get("test")

    def reset_cluster_node(self, cluster_node):
        self.server = cluster_node
        self.rest = BackupRestApi(self.server)

    def archive_all_repos(self):
        status, repos = self.rest.get_repository_information("active")
        if status:
            for repo in repos:
                self.log.info("Archiving backup_repo '%s'" % repo["id"])
                status, content = self.rest.archive_repository(repo["id"])
                if not status:
                    self.log.critical("Failed to archive '%s': %s"
                                      % (repo["id"], content))
        return status

    def delete_all_archive_repos(self, remove_repository=False):
        status, repos = self.rest.get_repository_information("archived")
        if status:
            for repo in repos:
                self.log.info("Deleting archive repo '%s'" % repo["id"])
                status, content = self.rest.delete_repository(
                    repo["id"], remove_repository=remove_repository)
                if status:
                    self.log.critical("Failed to delete repo '%s': %s"
                                      % (repo["id"], content))
        return status
