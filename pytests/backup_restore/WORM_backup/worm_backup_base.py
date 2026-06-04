import re
import time

from backup_restore.continuous_backup.continuous_backup_base import ContinuousBackupBase
from backup_restore.WORM_backup.worm_cloud_helper import WormCloudHelper
from cb_constants import CbServer
from cb_tools.cbbackupmgr import CbBackupMgr
from Jython_tasks.task import FunctionCallTask
from pytests.bucket_collections.collections_base import CollectionBase
from sdk_client3 import SDKClient
from shell_util.remote_connection import RemoteMachineShellConnection


class WormBackupBase(ContinuousBackupBase):
    def setUp(self):
        super(WormBackupBase, self).setUp()
        self.worm_period_days = int(self.input.param("worm_period_days", 3))
        self.storage_provider = self.input.param("storage_provider", None)
        self.s3_bucket_name = self.input.param("s3_bucket_name", None)
        self.s3_key_prefix = self.input.param("s3_key_prefix", "worm_backup/archive")
        self.archive_dir = self.input.param(
            "archive_dir", self._default_archive_dir())
        if self.storage_provider in [None, "aws"] \
                and not self._param_supplied("archive_dir") \
                and not self.s3_bucket_name:
            self.skipTest("S3 WORM tests require s3_bucket_name or archive_dir")
        self.repo_name = self.input.param("repo_name", self.backup_repo_name)
        self.unique_repo_name = self._param_bool("unique_repo_name", True)
        self.validate_csp_locks = self._param_bool("validate_csp_locks", False)
        self.inspect_archive_state = self._param_bool("inspect_archive_state", False)
        self.interrupt_timeout = int(self.input.param("interrupt_timeout", 300))
        self.restore_bucket_name = self.input.param("restore_bucket_name", None)
        self._restore_bucket_counter = 0
        self.restore_timeout = int(self.input.param("restore_timeout", 300))
        self.obj_staging_dir = self.input.param("obj_staging_dir", None)
        if self.unique_repo_name:
            self.repo_name = self._unique_repo_name(self.repo_name)
        if self.obj_staging_dir is None and self._archive_uses_object_store(self.archive_dir):
            self.obj_staging_dir = "/tmp/%s_obj_staging" % self.repo_name
        self.backup_repo_name = self.repo_name
        self.backup_archive_dir = self.archive_dir
        if self.obj_staging_dir:
            output, error = self.shell.execute_command(
                "mkdir -p %s" % self.obj_staging_dir)
            self._assert_command_success(output, error)
        if self._archive_uses_object_store(self.archive_dir) \
                and self.storage_provider in (None, "aws"):
            if not self.aws_access_key or not self.aws_secret_key:
                self.skipTest("S3 archive requires AWS_ACCESS_KEY_ID and "
                              "AWS_SECRET_ACCESS_KEY environment variables")
            if not self._configure_aws_credentials_on_host(
                    self.cluster.master,
                    self.aws_access_key, self.aws_secret_key,
                    aws_session_token=self.aws_session_token,
                    aws_region=self.aws_region):
                self.skipTest("Failed to provision AWS credentials on %s"
                              % self.cluster.master.ip)
        self.backup_mgr = CbBackupMgr(
            self.shell,
            username=self.cluster.master.rest_username,
            password=self.cluster.master.rest_password,
            log=self.log,
            obj_staging_dir=self.obj_staging_dir,
            aws_region=self.aws_region)
        self.cloud_helper = None
        if self.inspect_archive_state or self.validate_csp_locks:
            try:
                self.cloud_helper = WormCloudHelper(
                    self.archive_dir, storage_provider=self.storage_provider,
                    test_obj=self)
            except Exception as error:
                self.skipTest("CSP helper setup failed: %s" % error)

    def tearDown(self):
        self.log.info("WORM archive retained for compliance cleanup: %s repo=%s"
                      % (self.archive_dir, self.repo_name))
        super(WormBackupBase, self).tearDown()

    def _param_bool(self, name, default=False):
        value = self.input.param(name, default)
        if isinstance(value, bool):
            return value
        return str(value).lower() in ["true", "1", "yes"]

    def _param_supplied(self, name):
        return name in getattr(self.input, "test_params", {})

    def _default_archive_dir(self):
        if self.storage_provider in [None, "aws"] and self.s3_bucket_name:
            return "s3://%s/%s" % (
                self.s3_bucket_name.strip("/"),
                self.s3_key_prefix.strip("/"))
        return self.backup_archive_dir

    def _unique_repo_name(self, repo_name):
        safe_test_name = re.sub(r"[^a-zA-Z0-9_]", "_", self._testMethodName)
        return "%s_%s_%s" % (repo_name, safe_test_name, int(time.time()))

    @staticmethod
    def _command_text(output, error):
        return "%s\n%s" % ("\n".join(output or []), "\n".join(error or []))

    @staticmethod
    def _command_stream_text(stream):
        text = "\n".join(stream or []).strip()
        return text or "<empty>"

    @staticmethod
    def _looks_like_failure(command_text):
        normalised_text = command_text.lower()
        return any(token in normalised_text for token in
                   ["error", "failed", "failure", "denied", "invalid",
                    "unsupported", "not found", "cannot", "can't", "unable",
                    "must", "requires", "blocked", "rejected", "corrupt",
                    "mismatch", "incomplete"])

    @staticmethod
    def _archive_uses_object_store(archive_dir):
        return str(archive_dir).lower().startswith(
            ("s3://", "az://", "azure://", "azblob://", "gs://", "gcs://"))

    def _require_cloud_helper(self, reason):
        if not self._archive_uses_object_store(self.archive_dir):
            self.skipTest("%s requires an object-store archive" % reason)
        if self.cloud_helper is None:
            try:
                self.cloud_helper = WormCloudHelper(
                    self.archive_dir, storage_provider=self.storage_provider,
                    test_obj=self)
            except Exception as error:
                self.skipTest("%s requires CSP credentials/helper support: %s"
                              % (reason, error))
        return self.cloud_helper

    def _require_storage_provider(self, providers, reason):
        helper = self._require_cloud_helper(reason)
        if isinstance(providers, str):
            providers = [providers]
        if helper.storage_provider not in providers:
            self.skipTest("%s requires storage_provider in %s" % (reason, providers))
        return helper

    def _require_param(self, name, reason):
        value = self.input.param(name, None)
        if value in [None, ""]:
            self.skipTest("Set %s to run %s" % (name, reason))
        return value

    def _assert_command_success(self, output, error, expected_text=None):
        command_text = self._command_text(output, error)
        if error:
            self.fail("Command failed unexpectedly: %s" % command_text)
        if expected_text and expected_text.lower() not in command_text.lower():
            self.fail("Expected command output to contain '%s'. Output: %s"
                      % (expected_text, command_text))
        return command_text

    def _assert_command_failure(self, output, error, expected_texts=None):
        command_text = self._command_text(output, error)
        stdout_text = self._command_stream_text(output)
        stderr_text = self._command_stream_text(error)
        inferred_failure = self._looks_like_failure(command_text)
        if error:
            failure_reason = "stderr returned output"
        elif inferred_failure:
            failure_reason = "failure keywords were detected in command output"
        else:
            failure_reason = None

        if failure_reason is None:
            self.log.error(
                "Expected command failure but command did not look failed. stdout=%s stderr=%s",
                stdout_text, stderr_text)
            self.fail(
                "Command did not fail as expected. stderr was empty and no failure keywords "
                "were detected. stdout=%s stderr=%s"
                % (stdout_text, stderr_text))

        if expected_texts:
            normalised_text = command_text.lower()
            matched_texts = [
                text for text in expected_texts
                if text.lower() in normalised_text
            ]
            if not matched_texts:
                self.log.error(
                    "Command failed via %s but did not match expected markers %s. "
                    "stdout=%s stderr=%s",
                    failure_reason, expected_texts, stdout_text, stderr_text)
                self.fail(
                    "Command failed via %s, but none of the expected markers %s were found. "
                    "stdout=%s stderr=%s"
                    % (failure_reason, expected_texts, stdout_text, stderr_text))
            self.log.debug(
                "Command failure matched expected markers %s via %s",
                matched_texts, failure_reason)
            return command_text
        return command_text

    def _assert_tamper_blocked(self, succeeded, detail):
        self.assertFalse(succeeded, "Expected tamper operation to fail: %s" % detail)
        self.assertTrue(any(token in detail.lower() for token in
                            ["lock", "retain", "immut", "accessdenied",
                             "access denied", "forbidden", "conditionnotmet",
                             "retention", "legal hold"]),
                        "Tamper failed for an unexpected reason: %s" % detail)

    def _run_cbbackupmgr_raw(self, arguments, include_obj_staging=True,
                             include_cli_flags=True):
        arguments = arguments.strip()
        if arguments.startswith(("/", "./")) or "cbbackupmgr" in arguments \
                or "couchbase-cli" in arguments:
            cmd = arguments
        else:
            cmd = "%s %s" % (self.backup_mgr.cbstatCmd, arguments)
        if include_obj_staging and self.backup_mgr.obj_staging_dir \
                and "--archive" in arguments and "--obj-staging-dir" not in arguments:
            cmd += " --obj-staging-dir %s" % self.backup_mgr.obj_staging_dir
        if include_cli_flags:
            cmd += self.backup_mgr.cli_flags
        cmd = self.backup_mgr.prepare_command(cmd)
        self.log.debug("Executing command: %s" % cmd)
        return self.shell.execute_command(cmd)

    def _run_required_remote_command(self, param_name, reason):
        command = self._require_param(param_name, reason)
        output, error = self.shell.execute_command(command)
        self.log.info("Command '%s' output=%s error=%s" % (param_name, output, error))
        return output, error

    def _create_repo(self, worm_period=None, default_retention=None):
        output, error = self.backup_mgr.create_repo(
            self.archive_dir, self.repo_name, worm_period=worm_period,
            default_retention=default_retention)
        return self._assert_command_success(output, error)

    def _create_worm_repo(self):
        return self._create_repo(worm_period=self.worm_period_days)

    def _enable_worm(self, period=None):
        output, error = self.backup_mgr.worm(
            self.archive_dir, self.repo_name, period or self.worm_period_days)
        return self._assert_command_success(output, error)

    def _run_backup(self, resume=False, purge=False, full_backup=False,
                    no_progress_bar=True, threads=None):
        output, error = self.backup_mgr.backup(
            self.archive_dir, self.repo_name, resume=resume, purge=purge,
            full_backup=full_backup, no_progress_bar=no_progress_bar,
            threads=threads)
        return self._assert_command_success(output, error)

    def _run_restore(self, map_data=None, auto_create_buckets=False,
                     allow_non_worm=False, backup_id=None):
        output, error = self.backup_mgr.restore(
            self.archive_dir, self.repo_name, no_progress_bar=True,
            map_data=map_data, auto_create_buckets=auto_create_buckets,
            allow_non_worm=allow_non_worm, backup_id=backup_id)
        return self._assert_command_success(output, error)

    def _assert_restore_failure(self, map_data=None, allow_non_worm=False,
                                backup_id=None, expected_texts=None):
        output, error = self.backup_mgr.restore(
            self.archive_dir, self.repo_name, no_progress_bar=True,
            map_data=map_data, allow_non_worm=allow_non_worm,
            backup_id=backup_id)
        if expected_texts is None:
            expected_texts = ["worm", "restore", "backup", "incomplete",
                              "compliance", "allow-non-worm"]
        return self._assert_command_failure(output, error, expected_texts)

    def _load_data_and_return_count(self):
        CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
        self.bucket_util._wait_for_stats_all_buckets(
            self.cluster, self.cluster.buckets)
        return self.bucket_util.get_buckets_item_count(
            self.cluster, self.bucket.name)

    def _generate_additional_docs_and_return_count(self, num_docs=50,
                                                   doc_size=256):
        output, error = self.backup_mgr.generate_docs(
            num_docs=num_docs, bucket_name=self.bucket.name, size=doc_size)
        self._assert_command_success(output, error)
        self.bucket_util._wait_for_stats_all_buckets(
            self.cluster, self.cluster.buckets)
        return self.bucket_util.get_buckets_item_count(
            self.cluster, self.bucket.name)

    def _collection_count(self):
        count = 0
        for bucket in self.cluster.buckets:
            for scope in self.bucket_util.get_active_scopes(bucket):
                count += len(self.bucket_util.get_active_collections(
                    bucket, scope.name))
        return count

    def _assert_collection_count_at_least(self, expected_count):
        actual_count = self._collection_count()
        self.assertGreaterEqual(
            actual_count, expected_count,
            "Expected at least %s collections, found %s. Use a 10k collection bucket_spec for this case."
            % (expected_count, actual_count))
        return actual_count

    def _create_gsi_indexes_for_all_collections(self):
        sdk_client = SDKClient(self.cluster, None)
        try:
            CollectionBase.create_indexes_for_all_collections(self, sdk_client)
        finally:
            sdk_client.close()

    def _index_count_for_bucket(self, bucket_name):
        sdk_client = SDKClient(self.cluster, None)
        try:
            query = "SELECT RAW COUNT(*) FROM system:indexes WHERE bucket_id='%s' OR keyspace_id='%s'" \
                    % (bucket_name, bucket_name)
            result = sdk_client.cluster.query(query)
            rows = list(result.rows())
            if rows:
                return rows[0]
            return 0
        finally:
            sdk_client.close()

    def _repo_info(self):
        output, error = self.backup_mgr.info(self.archive_dir, self.repo_name)
        return self._assert_command_success(output, error)

    def _assert_repo_reports_worm(self):
        info_output = self._repo_info().lower()
        self.assertIn("worm", info_output,
                      "cbbackupmgr info did not report WORM state. "
                      "Run this suite against a WORM-enabled cbbackupmgr/Couchbase build.")
        self.assertIn(str(self.worm_period_days), info_output,
                      "cbbackupmgr info did not report the configured WORM period")
        return info_output

    def _latest_backup_name(self):
        if not self.cloud_helper:
            self.fail("Cloud metadata validation is required to locate backups")
        backup_name = self.cloud_helper.find_latest_backup_name(self.repo_name)
        self.assertTrue(backup_name, "No backup directory found in WORM repo")
        return backup_name

    def _assert_latest_backup_is_worm_locked(self):
        helper = self._require_cloud_helper("WORM lock validation")
        backup_name = self._latest_backup_name()
        target_path = helper.find_first_data_object(self.repo_name)
        if target_path is None:
            target_path = self._find_required_metadata_path(
                [".worm", "%s/.statusflag" % backup_name, ".statusflag"])
        succeeded, detail = self.cloud_helper.attempt_overwrite(
            self.repo_name, target_path)
        self.assertFalse(succeeded,
                         "Expected CSP overwrite of a WORM object to fail: %s"
                         % detail)
        self.assertTrue(any(token in detail.lower() for token in
                            ["lock", "retain", "immut", "accessdenied",
                             "access denied", "forbidden", "conditionnotmet"]),
                        "Overwrite failed for an unexpected reason: %s" % detail)
        return backup_name

    def _find_required_metadata_path(self, names):
        helper = self._require_cloud_helper("metadata validation")
        metadata_path = helper.find_metadata_path(self.repo_name, names)
        self.assertTrue(metadata_path, "Could not find metadata file: %s" % names)
        return metadata_path

    def _assert_no_statusflag_for_latest_backup(self):
        helper = self._require_cloud_helper("statusflag validation")
        backup_name = self._latest_backup_name()
        statusflag_paths = helper.find_relative_paths(
            self.repo_name, suffix="%s/.statusflag" % backup_name)
        self.assertFalse(statusflag_paths,
                         "Incomplete WORM backup unexpectedly has .statusflag: %s"
                         % statusflag_paths)

    def _assert_incomplete_backup_is_not_restorable(self):
        map_data = "%s=%s" % (self.bucket.name, self._create_restore_bucket())
        return self._assert_restore_failure(
            map_data=map_data,
            expected_texts=["incomplete", "resume", "backup", "restore",
                            "status", "not found", "worm"])

    def _create_restore_bucket(self, suffix=None):
        restore_bucket_name = self.restore_bucket_name
        if restore_bucket_name is None:
            self._restore_bucket_counter += 1
            restore_bucket_name = "restore_%s_%s" % (
                int(time.time()), self._restore_bucket_counter)
        elif suffix:
            restore_bucket_name = "%s_%s" % (restore_bucket_name, suffix)
        self.bucket_util.create_default_bucket(
            self.cluster,
            bucket_name=restore_bucket_name,
            bucket_type=self.bucket_type,
            ram_quota=self.input.param("bucket_size", 100),
            replica=self.num_replicas,
            storage=self.bucket_storage)
        return restore_bucket_name

    def _start_backup_in_background(self):
        if CbServer.use_https:
            cluster_host = "https://%s:%s" % (self.cluster.master.ip,
                                              self.backup_mgr.port)
        else:
            cluster_host = "http://%s:%s" % (self.cluster.master.ip,
                                             self.backup_mgr.port)
        backup_cmd = "%s backup --archive %s --repo %s --cluster %s -u %s -p %s --no-progress-bar%s" % (
            self.backup_mgr.cbstatCmd, self.archive_dir, self.repo_name,
            cluster_host, self.backup_mgr.username, self.backup_mgr.password,
            self.backup_mgr.cli_flags)
        if self.backup_mgr.obj_staging_dir:
            backup_cmd += " --obj-staging-dir %s" % self.backup_mgr.obj_staging_dir
        backup_cmd = self.backup_mgr.prepare_command(backup_cmd)
        pid_file = "/tmp/%s_cbbackupmgr.pid" % self.repo_name
        log_file = "/tmp/%s_cbbackupmgr.log" % self.repo_name
        command = "nohup %s > %s 2>&1 & echo $! > %s" % (
            backup_cmd, log_file, pid_file)
        output, error = self.shell.execute_command(command)
        self._assert_command_success(output, error)
        return pid_file, log_file

    def _wait_for_background_process(self, pid_file, timeout=None):
        if timeout is None:
            timeout = self.restore_timeout
        pid_output, pid_error = self.shell.execute_command("cat %s" % pid_file)
        self._assert_command_success(pid_output, pid_error)
        pid = pid_output[0].strip()
        end_time = time.time() + timeout
        while time.time() < end_time:
            output, error = self.shell.execute_command("kill -0 %s" % pid)
            if error:
                return self._command_text(output, error)
            time.sleep(10)
        self.fail("Timed out waiting for background cbbackupmgr process %s" % pid)

    def _interrupt_background_backup_after_objects(self):
        self._require_cloud_helper("interrupted backup archive inspection")
        # Run the backup as a background framework task on its own shell so
        # self.shell stays free to issue the interrupt mid-flight.
        task_shell = RemoteMachineShellConnection(self.cluster.master)
        task_backup_mgr = CbBackupMgr(
            task_shell,
            username=self.cluster.master.rest_username,
            password=self.cluster.master.rest_password,
            log=self.log,
            obj_staging_dir=self.obj_staging_dir,
            aws_region=self.aws_region)
        backup_task = FunctionCallTask(task_backup_mgr.backup, kwds={
            "archive_dir": self.archive_dir, "repo_name": self.repo_name,
            "no_progress_bar": True})
        self.task_manager.add_new_task(backup_task)
        try:
            if not self.cloud_helper.wait_for_objects(
                    self.repo_name, timeout=self.interrupt_timeout):
                self.fail("No WORM backup objects appeared before interrupt timeout")
            # Interrupt by killing the remote cbbackupmgr process; repo_name is
            # unique per test so the pkill match is unambiguous.
            self.shell.execute_command("pkill -9 -f '%s'" % self.repo_name)
        finally:
            self.task_manager.stop_task(backup_task)
            task_shell.disconnect()
        self.log.info("Interrupted cbbackupmgr backup for repo %s" % self.repo_name)
