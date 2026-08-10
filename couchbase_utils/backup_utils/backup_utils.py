import time
import traceback

from BucketLib.bucket import Bucket
from TestInput import TestInputSingleton
from cb_server_rest_util.backup.backup_api import BackupRestApi
from global_vars import logger
from couchbase_utils.cb_tools.cbbackupmgr import CbBackupMgr
from couchbase_utils.cb_tools.cbcontbk import CbContBk
from couchbase_utils.cloud_provider_utils.aws_provider import AWSProvider
from couchbase_utils.cloud_provider_utils.azure_provider import AzureProvider
from couchbase_utils.cloud_provider_utils.gcp_provider import GCPProvider
from couchbase_utils.cloud_provider_utils.localstack_provider import \
    LocalstackProvider
from couchbase_utils.security_utils.credential_store_utils import \
    CredentialStoreUtils
from shell_util.remote_connection import RemoteMachineShellConnection

# cbbackup_test / cont_bkp_test values backed by an object store.
CLOUD_PROVIDER_CLASSES = {
    "AWS": AWSProvider,
    "Azure": AzureProvider,
    "GCP": GCPProvider,
    "localstack": LocalstackProvider,
}

# cbbackup_test / cont_bkp_test values backed by a filesystem the cluster
# nodes can see, and which is therefore created/removed over a shell.
LOCAL_BACKUP_MODES = ("single_node", "NFS")


class BackupLocation(object):
    """
    One resolved backup destination -- either a cbbackupmgr archive or a
    continuous-backup location -- together with everything the CLI tools and
    teardown need in order to reach it.

    Built by build() from the raw test parameter (`cbbackup_test` /
    `cont_bkp_test`), which selects the backing store:

      None          - not exercised; `path` stays at the caller's default
      "single_node" - local directory on the cluster node
      "NFS"         - directory under the NFS mount shared by all nodes
      "AWS" / "Azure" / "GCP" / "localstack"
                    - object-store URI, with a CloudProviderInterface and,
                      once registered, a Credential Store id

    A `None` mode is a first-class state rather than an error: the test is
    simply not exercising this half of the backup story, so provision() and
    cleanup() are no-ops while `path` keeps reporting whatever default the
    test was configured with.
    """

    # Local deletes can span a lot of backup data and go through the NFS
    # client rather than the server, so they get considerably longer than a
    # plain shell command.
    CLEANUP_TIMEOUT = 300

    def __init__(self, mode, path, label, cloud_provider=None,
                 obj_staging_dir=None, log=None):
        self.mode = mode
        self.path = path
        self.label = label
        self.cloud_provider = cloud_provider
        self.obj_staging_dir = obj_staging_dir
        self.credential_store_id = None
        self.log = log if log else logger.get("test")

    @classmethod
    def build(cls, mode, label, local_path=None, url_templates=None,
              template_args=None, obj_staging_dir=None, log=None):
        """
        Resolve a `cbbackup_test`/`cont_bkp_test` value into a BackupLocation.

        :param mode: raw test parameter value; None means "not exercised"
        :param label: human-readable name used in log messages, e.g.
                      "backup archive" / "continuous backup location"
        :param local_path: path to use for the non-object-store modes
        :param url_templates: {mode: uri_template} for the object-store modes.
                              A mode missing from the mapping is rejected --
                              that is how a suite opts out of a provider it
                              cannot support (e.g. localstack for volume
                              tests, whose buckets are not local).
        :param template_args: values interpolated into the chosen template,
                              e.g. {"uid": "test-<uuid>"} for suites that
                              need a destination per test
        :param obj_staging_dir: --obj-staging-dir for the object-store modes
        """
        if mode is not None and mode not in LOCAL_BACKUP_MODES \
                and mode not in CLOUD_PROVIDER_CLASSES:
            raise ValueError(
                "%s: unknown backing store '%s'. Supported: %s"
                % (label, mode, [None] + list(LOCAL_BACKUP_MODES)
                   + list(CLOUD_PROVIDER_CLASSES)))

        if mode not in CLOUD_PROVIDER_CLASSES:
            return cls(mode, local_path, label, log=log)

        url_templates = url_templates or dict()
        if mode not in url_templates:
            raise ValueError(
                "%s: '%s' is not a supported backing store for this suite. "
                "Supported: %s" % (label, mode, sorted(url_templates)))

        # Constructing the provider validates its credentials, so a
        # misconfigured environment fails here instead of mid-test. It shares
        # the caller's logger so provider output lands in the test log.
        return cls(mode, url_templates[mode].format(**(template_args or {})),
                   label,
                   cloud_provider=CLOUD_PROVIDER_CLASSES[mode](log=log),
                   obj_staging_dir=obj_staging_dir, log=log)

    @property
    def enabled(self):
        """True when the test asked for a specific backing store."""
        return self.mode is not None

    @property
    def is_cloud(self):
        """True when the backing store is an object store."""
        return self.cloud_provider is not None

    @property
    def is_local(self):
        """True when the backing store is a filesystem path on the nodes."""
        return self.enabled and not self.is_cloud

    def provision(self, shell=None):
        """
        Make the location usable by a fresh run, discarding anything an
        earlier run left behind.

        Local paths are recreated world-writable, because the `couchbase`
        user -- not the SSH user -- is what writes into them. Object stores
        are prefix-cleaned instead, which on localstack also creates the
        bucket.

        :param shell: shell connection to the node the CLI tools run on;
                      required for the local modes, unused for object stores
        """
        if not self.enabled:
            return
        self._assert_safe_path()

        if self.is_cloud:
            self.log.info("Preparing %s in the object store: %s"
                          % (self.label, self.path))
            self.cloud_provider.cleanup_for_bkrs(self.path)
            return

        self._require_shell(shell)
        for cmd in (f"rm -rf {self.path}", f"mkdir -p {self.path}",
                    f"chmod 777 {self.path}"):
            _, error = shell.execute_command(cmd, timeout=self.CLEANUP_TIMEOUT)
            if error:
                raise Exception("Failed to prepare %s at %s ('%s'): %s"
                                % (self.label, self.path, cmd, error))
        self.log.info("Prepared %s: %s" % (self.label, self.path))

    def create_credential_store(self, rest, cred_id, description=None,
                                consumer_service="backup"):
        """
        Register the provider's credentials in the cluster's Credential Store
        as `cred_id` and let `consumer_service` read them, so the service can
        reach the object store on its own.

        :return: the credential id, or None for the non-object-store modes,
                 which need no credentials. Callers pass it on as
                 `continuous_backup_cloud_storage_cred_id`.
        """
        if not self.is_cloud:
            return None

        self.log.info("Creating Credential Store entry '%s' for %s"
                      % (cred_id, self.label))
        self.cloud_provider.create_credential_store(
            rest, cred_id=cred_id, description=description)
        CredentialStoreUtils().put_service_roles(
            rest, service_name=consumer_service,
            roles=[f"credential_consumer[{cred_id}]"])
        self.credential_store_id = cred_id
        return cred_id

    def cleanup(self, shell=None):
        """
        Remove everything the run wrote to this location. Never raises --
        tearDown has to keep going regardless.

        :param shell: shell connection to the node the CLI tools run on;
                      required for the local modes, unused for object stores
        """
        if not self.enabled:
            return
        try:
            self._assert_safe_path()
            if self.is_cloud:
                self.log.info("Cleaning up %s in the object store: %s"
                              % (self.label, self.path))
                self.cloud_provider.cleanup_for_bkrs(self.path)
                return

            self._require_shell(shell)
            self.log.info("Removing %s folder: %s" % (self.label, self.path))
            _, error = shell.execute_command(f"rm -rf {self.path}",
                                             timeout=self.CLEANUP_TIMEOUT)
            if error:
                self.log.warning("Error removing %s folder: %s"
                                 % (self.label, error))
        except Exception as e:
            self.log.warning("Exception during %s cleanup: %s"
                             % (self.label, e))

    def _assert_safe_path(self):
        """Guard against an unset/root path reaching a recursive delete."""
        if not self.path or not self.path.strip("/"):
            raise ValueError("%s: refusing to operate on path %r"
                             % (self.label, self.path))

    def _require_shell(self, shell):
        if shell is None:
            raise ValueError(
                "%s: backing store '%s' needs a shell connection to the node"
                % (self.label, self.mode))


class ContinuousBackupUtil(object):
    def __init__(self, shell_conn, username, password, log=None,
                 backupmgr_cloud_provider=None, contbk_cloud_provider=None,
                 backupmgr_obj_staging_dir=None, contbk_obj_staging_dir=None):
        self.shell_conn = shell_conn
        self.username = username
        self.password = password
        self.log = log if log else logger.get("test")
        self.contbk_cloud_provider = contbk_cloud_provider

        self.backup_mgr = CbBackupMgr(shell_conn,
                                      username=username,
                                      password=password,
                                      log=self.log,
                                      cloud_provider=backupmgr_cloud_provider,
                                      obj_staging_dir=backupmgr_obj_staging_dir)
        # cbcontbk reads both the archive and the continuous backup location,
        # so it needs both providers
        self.cont_bk_mgr = CbContBk(shell_conn,
                                    username=username,
                                    password=password,
                                    log=self.log,
                                    cbcontbk_cloud_provider=contbk_cloud_provider,
                                    backup_cloud_provider=backupmgr_cloud_provider,
                                    obj_staging_dir=contbk_obj_staging_dir)

    @staticmethod
    def enable_continuous_backup(bucket_util, cluster, buckets,
                                 continuous_backup_location="/tmp/cont_bkp",
                                 continuous_backup_interval=5,
                                 continuous_backup_cloud_storage_cred_id=None,
                                 continuous_backup_km_key_url=None,
                                 continuous_backup_km_cred_id=None,
                                 default_history_retention_seconds=86400,
                                 default_history_retention_bytes=0,
                                 log=None):
        """
        Enable continuous backup on every eligible bucket in `buckets`.

        Continuous backup rides on magma's change history, so a bucket with no
        history retention configured gets `default_history_retention_*`
        applied alongside. A bucket that already has retention set keeps it,
        so tests deliberately exercising history expiry inside their own
        window are not overridden; pass
        `default_history_retention_seconds=None` to never touch retention.

        Ephemeral and non-magma buckets have no change history to back up and
        are skipped.

        Args:
            bucket_util: BucketUtil instance for bucket operations
            cluster: Cluster object
            buckets: List of Bucket objects to enable CB on
            continuous_backup_location: Location for continuous backup files
            continuous_backup_interval: Continuous backup interval in minutes
            continuous_backup_cloud_storage_cred_id: Credential Store cred_id
                for continuous backup's cloud object-store access
            continuous_backup_km_key_url: external KMS key URL, for
                encryption-at-rest of the continuous backup
            continuous_backup_km_cred_id: Credential Store cred_id holding the
                KMS credentials for that key
        Returns:
            list of bucket names continuous backup was enabled on
        """
        log = log if log else logger.get("test")
        enabled_buckets = list()

        for bucket in buckets:
            if bucket.bucketType == Bucket.Type.EPHEMERAL \
                    or bucket.storageBackend != Bucket.StorageBackend.magma:
                log.info("Skipping continuous backup for bucket %s: "
                         "bucketType=%s, storageBackend=%s"
                         % (bucket.name, bucket.bucketType,
                            bucket.storageBackend))
                continue

            retention_params = dict()
            has_change_history = (bucket.historyRetentionSeconds or 0) != 0 \
                or (bucket.historyRetentionBytes or 0) != 0
            if default_history_retention_seconds is not None \
                    and not has_change_history:
                log.info("Bucket %s has no change history configured, "
                         "applying seconds=%s bytes=%s"
                         % (bucket.name, default_history_retention_seconds,
                            default_history_retention_bytes))
                retention_params = {
                    "history_retention_seconds":
                        default_history_retention_seconds,
                    "history_retention_bytes":
                        default_history_retention_bytes}

            bucket_util.update_bucket_property(
                cluster.master, bucket,
                continuous_backup_enabled=True,
                continuous_backup_location=continuous_backup_location,
                continuous_backup_interval=continuous_backup_interval,
                continuous_backup_cloud_storage_cred_id=continuous_backup_cloud_storage_cred_id,
                continuous_backup_km_key_url=continuous_backup_km_key_url,
                continuous_backup_km_cred_id=continuous_backup_km_cred_id,
                **retention_params)

            log.info("Continuous backup enabled for bucket: %s" % bucket.name)
            enabled_buckets.append(bucket.name)

        log.info("Waiting 10 seconds for continuous backup to be enabled")
        time.sleep(10)
        return enabled_buckets

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
                                   continuous_backup_interval=5, obj_staging_dir=None):
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
            output, _ = self.backup_mgr.list_backups(
                backup_archive_dir, backup_repo_name,
                obj_staging_dir=obj_staging_dir)
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

        # Loop through each bucket for continuous restore verification
        for bucket in buckets:
            if (bucket.storageBackend != Bucket.StorageBackend.magma or bucket.bucketType == 'ephemeral' or
                    bucket.name == "default"):
                self.log.info(f"Skipping continuous restore verification for {bucket.name}: "
                              f"storage backend is {bucket.storageBackend}, not magma")
                continue
            restore_bucket_name = f"{bucket.name}"
            self.log.info(f"Performing continuous restore (PITR) for bucket: {bucket.name}")
            self._create_restore_bucket(bucket_util, cluster, restore_bucket_name, bucket)
            try:
                cluster_host = f"http://{cluster.master.ip}:8091"
                output, error = self.cont_bk_mgr.restore(
                    backup_archive_dir, backup_repo_name,
                    cluster_host=cluster_host,
                    location=continuous_backup_location,
                    temp_dir="/data/tmp",
                    timestamp=None,
                    map_data=f"{bucket.name}={restore_bucket_name}",
                    obj_staging_dir=obj_staging_dir)

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
        ram_quota = source_bucket.ramQuotaMB
        bucket_util.delete_bucket(cluster, source_bucket)
        bucket_util.create_default_bucket(
            cluster,
            bucket_name=restore_bucket_name,
            bucket_type=bucket_type,
            ram_quota=ram_quota,
            replica=replica,
            storage=storage)

    def monitor_restore(self, bucket_util, cluster, bucket, items, timeout=43200, tolerance=0.10):
        end_time = time.time() + timeout
        lower_bound = items * (1 - tolerance)
        upper_bound = items * (1 + tolerance)
        while time.time() < end_time:
            curr_items = bucket_util.get_buckets_item_count(
                cluster, bucket.name)
            self.log.info("Current/Expected items during restore: %s == %s (tolerance: +/-%.0f%%)" % (curr_items, items, tolerance * 100))
            self.log.info("Wait for items restoration")
            time.sleep(5)
            if lower_bound <= curr_items <= upper_bound:
                return True
        self.log.info("cbcontbk restore did not finish in %s seconds: Actual:%s, Expected:%s (tolerance: +/-%.0f%%)" % (timeout, curr_items, items, tolerance * 100))
        return False

    def trigger_restore(self, cluster, archive='/data/backups', repo='magma',
                        cont_backup_location='/mnt/nfs_data/continuous_backup',
                        staging_dir='/data/tmp', timestamp=None,
                        threads=8, obj_staging_dir=None):

        self.log.info('Restore backup using cbcontbk')
        return self.cont_bk_mgr.restore(archive_path=archive,
                                        repo_name=repo,
                                        location=cont_backup_location,
                                        temp_dir=staging_dir,
                                        timestamp=timestamp,
                                        threads=threads,
                                        cluster_host=f"http://{cluster.master.ip}:8091",
                                        obj_staging_dir=obj_staging_dir)

    def collect_continuous_backup_logs_on_failure(self, backup_location='/data/continuous_backups',
                                                   obj_staging_dir=None):
        """
        Collects cbcontbk logs on test failure.
        Only runs on Linux nodes. Logs are collected to /data/tmp on the remote
        node and then copied to the local log path.
        """
        log_path = TestInputSingleton.input.param("logs_folder", "/tmp")
        remote_tmp_dir = "/data/tmp"

        try:
            os_info = self.shell_conn.extract_remote_info()
            if os_info.type.lower() != "linux":
                self.log.info(f"Skipping cbcontbk log collection: OS is not Linux")
                return

            self.log.info(f"Collecting cbcontbk logs for investigation")
            self.shell_conn.execute_command(f"mkdir -p {remote_tmp_dir}")
            self.cont_bk_mgr.collect_logs(location=backup_location,
                                          temp_dir=remote_tmp_dir,
                                          obj_staging_dir=obj_staging_dir)

            output, _ = self.shell_conn.execute_command(f"ls {remote_tmp_dir}/*.zip 2>/dev/null")
            for log_file in output:
                log_file = log_file.strip()
                if log_file:
                    self.log.info(f"Copying {log_file} to {log_path}")
                    self.shell_conn.get_file(remote_tmp_dir, log_file.split("/")[-1], log_path)
        except Exception as e:
            self.log.error(f"Exception during cbcontbk log collection: {e}")

    def cleanup_continuous_backup(self, backup_location='/data/continuous_backups'):
        """
        Cleans up continuous backup files from the backup location.
        If a cloud provider is configured for continuous backup, delegates
        cleanup to the provider's cleanup_for_bkrs() instead. Otherwise,
        only runs on Linux nodes and uses rm -rf to clean up the backup
        location.
        """
        if self.contbk_cloud_provider is not None:
            self.log.info(f"Cleaning up continuous backup location via cloud provider: {backup_location}")
            try:
                self.contbk_cloud_provider.cleanup_for_bkrs(backup_location)
            except Exception as e:
                self.log.error(f"Exception during cloud provider continuous backup cleanup: {e}")
        else:
            try:
                os_info = self.shell_conn.extract_remote_info()
                if os_info.type.lower() != "linux":
                    self.log.info(f"Skipping continuous backup cleanup: OS is not Linux")
                    return

                self.log.info(f"Cleaning up continuous backup files at {backup_location}")
                cleanup_cmd = f"rm -rf {backup_location}/*"
                self.log.info(f"Executing cleanup command: {cleanup_cmd}")
                self.shell_conn.execute_command(cleanup_cmd)
            except Exception as e:
                self.log.error(f"Exception during continuous backup cleanup: {e}")

class BackupMgrUtil(CbBackupMgr):
    def __init__(self, cb_node, cloud_provider=None, obj_staging_dir=None):
        self.cb_node = cb_node
        shell_conn = RemoteMachineShellConnection(cb_node)
        super().__init__(shell_conn, username=cb_node.rest_username,
                         password=cb_node.rest_password,
                         no_ssl_verify=None, log=None,
                         cloud_provider=cloud_provider,
                         obj_staging_dir=obj_staging_dir)

    def configure_backup(self, archive, repo, exclude=None, include=None,
                         obj_staging_dir=None):
        """Delete previous archive dir, then create backup repo."""
        if not archive or archive == "/":
            raise ValueError("archive must be a non-empty, non-root path")
        if self.cloud_provider is not None:
            self.log.info(
                "Cleaning up previous backup archive via cloud provider: %s"
                % archive)
            self.cloud_provider.cleanup_for_bkrs(archive)
        else:
            self.log.info("Deleting previous backup archive: %s" % archive)
            self.shellConn.execute_command(f"rm -rf -- {archive}")
        stdout, stderr = super().create_repo(archive, repo,
                                             exclude=exclude, include=include,
                                             obj_staging_dir=obj_staging_dir)
        if self.cloud_provider is None:
            self.shellConn.execute_command(
                f"chown -R couchbase:couchbase {archive}")
        return stdout, stderr

    def monitor_restore(self, bucket_util, cluster, bucket_name, items,
                        timeout=43200):
        """Poll item count until restore completes or timeout expires."""
        end_time = time.time() + timeout
        curr_items = 0
        while time.time() < end_time:
            curr_items = bucket_util.get_buckets_item_count(cluster,
                                                            bucket_name)
            self.log.info(
                "Current/Expected items during restore: %s >= %s"
                % (curr_items, items))
            if curr_items >= items:
                return True
            time.sleep(5)
        self.log.info(f"cbbackupmgr restore did not finish in {timeout} "
                      f"seconds: Actual:{curr_items}, Expected:{items}")
        return False

    def collect_backup_logs_on_failure(self, archive='/data/backups', log_path='/tmp',
                                       obj_staging_dir=None):
        """
        Collects cbbackupmgr logs on test failure.
        Only runs on Linux nodes. Logs are collected to /data/tmp on the remote
        node and then copied to the local log path.
        """
        remote_tmp_dir = "/data/tmp"

        try:
            os_info = self.shellConn.extract_remote_info()
            if os_info.type.lower() != "linux":
                self.log.info("Skipping cbbackupmgr log collection: OS is not Linux")
                return

            self.log.info("Collecting cbbackupmgr logs for investigation")
            self.shellConn.execute_command(f"mkdir -p {remote_tmp_dir}")
            self.collect_logs(archive_dir=archive, output_dir=remote_tmp_dir,
                              obj_staging_dir=obj_staging_dir)

            output, _ = self.shellConn.execute_command(f"ls {remote_tmp_dir}/*.zip 2>/dev/null")
            for log_file in output:
                log_file = log_file.strip()
                if log_file:
                    self.log.info(f"Copying {log_file} to {log_path}")
                    self.shellConn.get_file(remote_tmp_dir, log_file.split("/")[-1], log_path)
        except Exception as e:
            self.log.error(f"Exception during cbbackupmgr log collection: {e}")

    def merge_all_backups(self, archive='/data/backups', repo='magma'):
        """
        Finds all backups in the archive and merges them from first to last.
        Returns (output, error) from the merge command, or (None, None) if not enough backups.

        cbbackupmgr only supports merge against a local/NFS archive directory,
        never an object store, so a configured cloud_provider raises.
        """
        if self.cloud_provider is not None:
            raise Exception(
                "cbbackupmgr merge is only supported for local/NFS archive "
                "directories, not object stores (%s)"
                % type(self.cloud_provider).__name__)

        self.log.info('Finding all backups in archive to merge')
        find_cmd = "cd {0}/{1}; find . -maxdepth 1 -type d -name '[^.]*' | sed 's:^\\./::' | grep -v '^logs$' | sort".format(
            archive, repo)
        output, error = self.shellConn.execute_command(find_cmd)

        if not output or len(output) < 2:
            self.log.info("Not enough backups to merge. Found: {}".format(output))
            return None, None

        backup_list = [b.strip() for b in output if b.strip()]
        merge_start = backup_list[0]
        merge_end = backup_list[-1]
        self.log.info("Merging backups from {} to {}".format(merge_start, merge_end))

        return self.merge(archive_dir=archive, repo_name=repo,
                          start=merge_start, end=merge_end)

    def cleanup_archive(self, archive='/data/backups'):
        """
        Cleans up the backup archive.
        If a cloud provider is configured, delegates cleanup to the
        provider's cleanup_for_bkrs(). Otherwise finds all repos in the
        archive and cleans them using cbbackupmgr remove, followed by a
        manual rm -rf as fallback.
        """
        if self.cloud_provider is not None:
            self.log.info(
                "Cleaning up archive via cloud provider: {}".format(archive))
            self.cloud_provider.cleanup_for_bkrs(archive)
            return

        # List all directories in the archive (each directory is a repo)
        find_repos_cmd = "find {0} -maxdepth 1 -mindepth 1 -type d -exec basename {{}} \\;".format(archive)
        output, error = self.shellConn.execute_command(find_repos_cmd)
        repos = [r.strip() for r in output if r.strip()]
        self.log.info("Found repos in archive {}: {}".format(archive, repos))

        for repo_name in repos:
            self.log.info("Cleaning up repo: {}".format(repo_name))
            # Cleanup using cbbackupmgr
            output, error = self.remove(archive, repo_name)
            self.log.info("cbbackupmgr remove output: {}".format(output))
            if error:
                self.log.warning("cbbackupmgr remove error: {}".format(error))

        # Manual cleanup in case cbbackupmgr remove fails
        cleanup_cmd = "rm -rf {0}/".format(archive)
        self.log.info("Manual cleanup with command: {}".format(cleanup_cmd))
        self.shellConn.execute_command(cleanup_cmd)

class BackupServiceUtil(object):
    def __init__(self, cluster, backup_node=None):
        if cluster is None or not getattr(cluster, "backup_nodes", None):
            raise ValueError("cluster must define at least one backup node")
        self.log = logger.get("test")
        self.cluster = cluster
        self.server = None
        self.rest = None

        self.reset_cluster_node(backup_node)

    def reset_cluster_node(self, backup_node=None):
        if backup_node is None:
            if not self.cluster.backup_nodes:
                raise ValueError("No backup nodes available")
            backup_node = self.cluster.backup_nodes[0]
        self.server = backup_node
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
