import re
import shlex
import time
import uuid

from backup_restore.continuous_backup.continuous_backup_base import ContinuousBackupBase
from cb_constants import CbServer
from cb_tools.cbbackupmgr import CbBackupMgr
from Jython_tasks.task import FunctionCallTask
from pytests.bucket_collections.collections_base import CollectionBase
from sdk_client3 import SDKClient
from shell_util.remote_connection import RemoteMachineShellConnection


class WormBackupBase(ContinuousBackupBase):

    WORM_URL_TEMPLATES = {
        "AWS": "s3://test-worm-taf/backups/{uid}",
        "Azure": "az://test-worm-taf/backups/{uid}",
        "GCP": "gs://test-worm-taf/backups/{uid}",
    }

    def setUp(self):
        super(WormBackupBase, self).setUp()
        self.worm_period_days = int(self.input.param("worm_period_days", 3))
        self.validate_csp_locks = self._param_bool("validate_csp_locks", False)
        self.inspect_archive_state = self._param_bool("inspect_archive_state", False)
        self.interrupt_timeout = int(self.input.param("interrupt_timeout", 300))
        self.restore_bucket_name = self.input.param("restore_bucket_name", None)
        self._restore_bucket_counter = 0
        self.restore_timeout = int(self.input.param("restore_timeout", 300))

        if self.cbbackup_test in self.CLOUD_PROVIDER_CLASSES:
            self.backup_archive_dir = self.WORM_URL_TEMPLATES[self.cbbackup_test].format(
                uid=f"test-{uuid.uuid4()}")
            self.backup_repo_name = self.input.param("repo_name", f"test_{uuid.uuid4()}")

        if self.backup_cloud_provider:
            self.backup_cloud_provider.cleanup_for_bkrs(self.backup_archive_dir)

    def tearDown(self):
        self.log.info("WORM archive retained for compliance cleanup: %s repo=%s"
                      % (self.backup_archive_dir, self.backup_repo_name))
        super(WormBackupBase, self).tearDown()

    def _param_bool(self, name, default=False):
        value = self.input.param(name, default)
        if isinstance(value, bool):
            return value
        return str(value).lower() in ["true", "1", "yes"]

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

    def _require_param(self, name, reason):
        value = self.input.param(name, None)
        if value in [None, ""]:
            self.skipTest("Set %s to run %s" % (name, reason))
        return value

    def _require_cloud_helper(self, reason):
        """
        Skip the test unless cbbackup_test is configured with a real cloud
        object-store provider (AWS/Azure/GCP) — the one CollectionBase.setUp()
        already built as self.backup_cloud_provider. WORM object-store
        inspection (list/read/overwrite/delete/versions/retention) only makes
        sense against a real cloud provider, not NFS/single_node.

        Returns self.backup_cloud_provider for the caller to use directly.
        """
        if self.backup_cloud_provider is None:
            self.skipTest(
                "Set cbbackup_test to AWS/Azure/GCP to run %s" % reason)
        return self.backup_cloud_provider

    def _require_storage_provider(self, providers, reason):
        """
        Skip the test unless cbbackup_test matches one of `providers`
        (a single name or a list of names, case-insensitive against the
        CollectionBase.CLOUD_PROVIDER_CLASSES keys: AWS/Azure/GCP).

        Returns self.backup_cloud_provider for the caller to use directly.
        """
        if isinstance(providers, str):
            providers = [providers]
        allowed = {provider.lower() for provider in providers}
        if self.cbbackup_test is None or self.cbbackup_test.lower() not in allowed:
            self.skipTest(
                "Set cbbackup_test to one of %s to run %s"
                % (sorted(allowed), reason))
        return self.backup_cloud_provider

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

    def _assert_output_contains_any(self, command_text, expected_texts, context):
        normalised_text = command_text.lower()
        if any(text.lower() in normalised_text for text in expected_texts):
            return
        self.fail("%s did not contain any expected markers %s. Output: %s"
                  % (context, expected_texts, command_text))

    def _assert_tamper_blocked(self, succeeded, detail):
        self.assertFalse(succeeded, "Expected tamper operation to fail: %s" % detail)
        self.assertTrue(any(token in detail.lower() for token in
                            ["lock", "retain", "immut", "accessdenied",
                             "access denied", "forbidden", "conditionnotmet",
                             "retention", "legal hold"]),
                        "Tamper failed for an unexpected reason: %s" % detail)

    def _cbbackupmgr_cloud_flags(self):
        """
        Cloud object-store CLI flags for the currently configured
        cbbackup_test provider (empty string for NFS/single_node/None) —
        self.backup_mgr.cli_flags only ever holds --no-ssl-verify, the
        cloud credentials/region flags are added per-call via the provider.
        """
        if self.backup_cloud_provider is None:
            return ""
        return " " + self.backup_cloud_provider.get_cbbackupmgr_flags(self.shell)

    def _run_cbbackupmgr_raw(self, arguments, include_obj_staging=True,
                             include_cli_flags=True):
        arguments = arguments.strip()
        if arguments.startswith(("/", "./")) or "cbbackupmgr" in arguments \
                or "couchbase-cli" in arguments:
            cmd = arguments
        else:
            cmd = "%s %s" % (self.backup_mgr.cbstatCmd, arguments)
        if include_obj_staging and self.obj_staging_dir_cbbackup \
                and "--archive" in arguments and "--obj-staging-dir" not in arguments:
            cmd += " --obj-staging-dir %s" % self.obj_staging_dir_cbbackup
        if include_cli_flags:
            cmd += self.backup_mgr.cli_flags
            if "--obj-" not in arguments:
                cmd += self._cbbackupmgr_cloud_flags()
        self.log.debug("Executing command: %s" % cmd)
        return self.shell.execute_command(cmd)

    def _cluster_host(self):
        if CbServer.use_https:
            return "https://%s:%s" % (self.cluster.master.ip,
                                      self.backup_mgr.port)
        return "http://%s:%s" % (self.cluster.master.ip,
                                 self.backup_mgr.port)

    def _build_backup_command(self, resume=False, purge=False,
                              no_progress_bar=True, threads=None):
        cmd = "%s backup --archive %s --repo %s --cluster %s -u %s -p %s" % (
            self.backup_mgr.cbstatCmd, self.backup_archive_dir,
            self.backup_repo_name, self._cluster_host(),
            self.backup_mgr.username, self.backup_mgr.password)
        if resume:
            cmd += " --resume"
        if purge:
            cmd += " --purge"
        if threads:
            cmd += " --threads %d" % threads
        if no_progress_bar:
            cmd += " --no-progress-bar"
        cmd += self.backup_mgr.cli_flags
        cmd += self._cbbackupmgr_cloud_flags()
        if self.backup_cloud_provider is not None and self.obj_staging_dir_cbbackup:
            cmd += " --obj-staging-dir %s" % self.obj_staging_dir_cbbackup
        return cmd

    def _build_worm_command(self, period=None):
        cmd = "%s worm --archive %s --repo %s --period %s" % (
            self.backup_mgr.cbstatCmd, self.backup_archive_dir,
            self.backup_repo_name,
            period if period is not None else self.worm_period_days)
        cmd += self.backup_mgr.cli_flags
        cmd += self._cbbackupmgr_cloud_flags()
        if self.backup_cloud_provider is not None and self.obj_staging_dir_cbbackup:
            cmd += " --obj-staging-dir %s" % self.obj_staging_dir_cbbackup
        return cmd

    def _run_backup_with_shell_prefix(self, shell_prefix, resume=False,
                                      purge=False, no_progress_bar=True,
                                      threads=None):
        backup_cmd = self._build_backup_command(
            resume=resume, purge=purge, no_progress_bar=no_progress_bar,
            threads=threads)
        command = "sh -lc %s" % shlex.quote("%s; %s" % (
            shell_prefix, backup_cmd))
        self.log.debug("Executing prefixed backup command: %s" % command)
        return self.shell.execute_command(command)

    def _run_concurrent_worm_enable_commands(self, command_count=2):
        work_dir = "/tmp/%s_worm_concurrent" % self.backup_repo_name
        commands = ["rm -rf %s", "mkdir -p %s"]
        commands = [command % work_dir for command in commands]
        for index in range(command_count):
            worm_cmd = self._build_worm_command(self.worm_period_days)
            commands.append(
                "(%s > %s/out_%d 2> %s/err_%d; echo $? > %s/exit_%d) &"
                % (worm_cmd, work_dir, index, work_dir, index, work_dir, index))
        commands.append("wait || true")
        for index in range(command_count):
            commands.append("echo EXIT_%d=$(cat %s/exit_%d 2>/dev/null || echo missing)"
                            % (index, work_dir, index))
            commands.append("cat %s/out_%d" % (work_dir, index))
            commands.append("cat %s/err_%d" % (work_dir, index))
        output, error = self.shell.execute_command("; ".join(commands))
        command_text = self._assert_command_success(output, error)
        success_count = len(re.findall(r"EXIT_\d+=0", command_text))
        return command_text, success_count

    def _run_required_remote_command(self, param_name, reason):
        command = self._require_param(param_name, reason)
        output, error = self.shell.execute_command(command)
        self.log.info("Command '%s' output=%s error=%s" % (param_name, output, error))
        return output, error

    def _run_required_success_command(self, param_name, reason):
        output, error = self._run_required_remote_command(param_name, reason)
        return self._assert_command_success(output, error)

    def _create_repo(self, worm_period=None, default_retention=None):
        output, error = self.backup_mgr.create_repo(
            self.backup_archive_dir, self.backup_repo_name, worm_period=worm_period,
            default_retention=default_retention,
            obj_staging_dir=self.obj_staging_dir_cbbackup)
        return self._assert_command_success(output, error)

    def _create_worm_repo(self):
        return self._create_repo(worm_period=self.worm_period_days)

    def _enable_worm(self, period=None):
        output, error = self.backup_mgr.worm(
            self.backup_archive_dir, self.backup_repo_name,
            period or self.worm_period_days,
            obj_staging_dir=self.obj_staging_dir_cbbackup)
        return self._assert_command_success(output, error)

    def _run_backup(self, resume=False, purge=False, full_backup=False,
                    no_progress_bar=True, threads=None):
        output, error = self.backup_mgr.backup(
            self.backup_archive_dir, self.backup_repo_name, resume=resume, purge=purge,
            full_backup=full_backup, no_progress_bar=no_progress_bar,
            threads=threads, obj_staging_dir=self.obj_staging_dir_cbbackup)
        return self._assert_command_success(output, error)

    def _run_restore(self, map_data=None, auto_create_buckets=False,
                     allow_non_worm=False, backup_id=None):
        output, error = self.backup_mgr.restore(
            self.backup_archive_dir, self.backup_repo_name, no_progress_bar=True,
            map_data=map_data, auto_create_buckets=auto_create_buckets,
            allow_non_worm=allow_non_worm, backup_id=backup_id,
            obj_staging_dir=self.obj_staging_dir_cbbackup)
        return self._assert_command_success(output, error)

    def _assert_restore_failure(self, map_data=None, allow_non_worm=False,
                                backup_id=None, expected_texts=None):
        output, error = self.backup_mgr.restore(
            self.backup_archive_dir, self.backup_repo_name, no_progress_bar=True,
            map_data=map_data, allow_non_worm=allow_non_worm,
            backup_id=backup_id, obj_staging_dir=self.obj_staging_dir_cbbackup)
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
            num_docs=num_docs, bucket_name=self.bucket.name, size=doc_size,
            obj_staging_dir=self.obj_staging_dir_cbbackup)
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
        output, error = self.backup_mgr.info(
            self.backup_archive_dir, self.backup_repo_name,
            obj_staging_dir=self.obj_staging_dir_cbbackup)
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
        helper = self._require_cloud_helper("locating backups")
        backup_name = helper.find_latest_backup_name(
            self.backup_archive_dir, self.backup_repo_name)
        self.assertTrue(backup_name, "No backup directory found in WORM repo")
        return backup_name

    def _assert_latest_backup_is_worm_locked(self):
        helper = self._require_cloud_helper("WORM lock validation")
        backup_name = self._latest_backup_name()
        target_path = helper.find_first_data_object(
            self.backup_archive_dir, self.backup_repo_name)
        if target_path is None:
            target_path = self._find_required_metadata_path(
                [".worm", "%s/.statusflag" % backup_name, ".statusflag"])
        succeeded, detail = helper.attempt_overwrite(
            self.backup_archive_dir, self.backup_repo_name, target_path)
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
        metadata_path = helper.find_metadata_path(
            self.backup_archive_dir, self.backup_repo_name, names)
        self.assertTrue(metadata_path, "Could not find metadata file: %s" % names)
        return metadata_path

    def _assert_no_statusflag_for_latest_backup(self):
        helper = self._require_cloud_helper("statusflag validation")
        backup_name = self._latest_backup_name()
        statusflag_paths = helper.find_relative_paths(
            self.backup_archive_dir, self.backup_repo_name,
            suffix="%s/.statusflag" % backup_name)
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

    def _start_backup_task(self, resume=False, purge=False, threads=None):
        """Launch cbbackupmgr backup in the background via the TAF task
        framework.

        A dedicated shell/CbBackupMgr is created so the foreground test can keep
        issuing commands over self.shell while the backup runs on its own
        paramiko channel (the two cannot share one SSH channel concurrently).
        Pair with _wait_for_backup_task().
        """
        shell = RemoteMachineShellConnection(self.cluster.master)
        backup_mgr = CbBackupMgr(
            shell,
            username=self.cluster.master.rest_username,
            password=self.cluster.master.rest_password,
            log=self.log,
            cloud_provider=self.backup_cloud_provider)
        task = FunctionCallTask(backup_mgr.backup, kwds={
            "archive_dir": self.backup_archive_dir, "repo_name": self.backup_repo_name,
            "resume": resume, "purge": purge, "threads": threads,
            "no_progress_bar": True,
            "obj_staging_dir": self.obj_staging_dir_cbbackup})
        task.shell = shell
        self.task_manager.add_new_task(task)
        return task

    def _wait_for_backup_task(self, task):
        """Block until a background backup task finishes, then release its
        dedicated shell. Returns the combined command output text."""
        try:
            result = self.task_manager.get_task_result(task)
        finally:
            task.shell.disconnect()
        output, error = result if isinstance(result, tuple) else ([], [])
        return self._command_text(output, error)

    def _interrupt_background_backup_after_objects(self):
        helper = self._require_cloud_helper("interrupted backup archive inspection")
        # Run the backup as a background framework task on its own shell so
        # self.shell stays free to issue the interrupt mid-flight.
        task_shell = RemoteMachineShellConnection(self.cluster.master)
        task_backup_mgr = CbBackupMgr(
            task_shell,
            username=self.cluster.master.rest_username,
            password=self.cluster.master.rest_password,
            log=self.log,
            cloud_provider=self.backup_cloud_provider)
        backup_task = FunctionCallTask(task_backup_mgr.backup, kwds={
            "archive_dir": self.backup_archive_dir, "repo_name": self.backup_repo_name,
            "no_progress_bar": True,
            "obj_staging_dir": self.obj_staging_dir_cbbackup})
        self.task_manager.add_new_task(backup_task)
        try:
            if not helper.wait_for_objects(
                    self.backup_archive_dir, self.backup_repo_name,
                    timeout=self.interrupt_timeout):
                self.fail("No WORM backup objects appeared before interrupt timeout")
            # Interrupt by killing the remote cbbackupmgr process; repo_name is
            # unique per test so the pkill match is unambiguous.
            self.shell.execute_command("pkill -9 -f '%s'" % self.backup_repo_name)
        finally:
            self.task_manager.stop_task(backup_task)
            task_shell.disconnect()
        self.log.info("Interrupted cbbackupmgr backup for repo %s" % self.backup_repo_name)
