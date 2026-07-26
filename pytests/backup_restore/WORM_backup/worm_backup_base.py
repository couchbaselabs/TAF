import re
import shlex
import time
import uuid

from backup_restore.continuous_backup.continuous_backup_base import ContinuousBackupBase
from cb_constants import CbServer
from pytests.bucket_collections.collections_base import CollectionBase
from sdk_client3 import SDKClient


class WormBackupBase(ContinuousBackupBase):

    # Object-store URL scheme per provider. The bucket/container is supplied
    # via the `worm_bucket` param (default "test-worm-taf") so the suite can
    # run against any lock-enabled bucket -- S3 bucket names are globally
    # unique, so a different AWS account cannot reuse the default name.
    WORM_URL_SCHEMES = {
        "AWS": "s3",
        "Azure": "az",
        "GCP": "gs",
    }
    DEFAULT_WORM_BUCKET = "test-worm-taf"

    # cbbackupmgr names each backup "2026-07-26T03_19_04.43253846-07_00"
    BACKUP_DIR_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}[_:]\d{2}")

    def setUp(self):
        super(WormBackupBase, self).setUp()
        self.worm_period_days = int(self.input.param("worm_period_days", 3))
        self.validate_csp_locks = self._param_bool("validate_csp_locks", False)
        self.inspect_archive_state = self._param_bool("inspect_archive_state", False)
        self.interrupt_timeout = int(self.input.param("interrupt_timeout", 300))
        self.restore_bucket_name = self.input.param("restore_bucket_name", None)
        self._restore_bucket_counter = 0
        self.restore_timeout = int(self.input.param("restore_timeout", 300))
        self.background_backup_timeout = int(
            self.input.param("background_backup_timeout", 900))
        self.doc_count_timeout = int(self.input.param("doc_count_timeout", 120))
        self._staging_dirs_to_remove = []

        self.worm_bucket = self.input.param("worm_bucket",
                                            self.DEFAULT_WORM_BUCKET)
        if self.cbbackup_test in self.CLOUD_PROVIDER_CLASSES:
            self.backup_archive_dir = "%s://%s/backups/test-%s" % (
                self.WORM_URL_SCHEMES[self.cbbackup_test], self.worm_bucket,
                uuid.uuid4())
            self.backup_repo_name = self.input.param("repo_name", f"test_{uuid.uuid4()}")

        self.log.info(
            "WORM: setUp complete. cbbackup_test=%s, archive=%s, repo=%s, "
            "worm_period_days=%s, validate_csp_locks=%s, cloud_provider=%s"
            % (self.cbbackup_test, self.backup_archive_dir, self.backup_repo_name,
               self.worm_period_days, self.validate_csp_locks,
               type(self.backup_cloud_provider).__name__
               if self.backup_cloud_provider else None))

        if self.backup_cloud_provider:
            self.log.info("WORM: cleaning up any pre-existing objects at %s"
                          % self.backup_archive_dir)
            self.backup_cloud_provider.cleanup_for_bkrs(self.backup_archive_dir)

    def tearDown(self):
        self.log.info("WORM archive retained for compliance cleanup: %s repo=%s"
                      % (self.backup_archive_dir, self.backup_repo_name))
        self._remove_registered_staging_dirs()
        super(WormBackupBase, self).tearDown()

    def _register_staging_dir_for_cleanup(self, path):
        """Have tearDown remove a test-created staging dir.

        onPrem_basetestcase resets obj_staging_dir_cbbackup itself, but not
        variants a test derives from it.
        """
        self._staging_dirs_to_remove.append(path)

    def _remove_registered_staging_dirs(self):
        for path in getattr(self, "_staging_dirs_to_remove", []):
            if not path or not path.startswith(self.obj_staging_dir_cbbackup):
                self.log.warning("WORM: refusing to remove unexpected staging "
                                 "path '%s'" % path)
                continue
            self.log.info("WORM: removing staging dir %s" % path)
            try:
                self.shell.execute_command("rm -rf %s" % path)
            except Exception as exception:
                self.log.warning("WORM: failed to remove staging dir %s: %s"
                                 % (path, exception))
        self._staging_dirs_to_remove = []

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

    # cbbackupmgr prints these on a successful run; they outrank keyword
    # matching so a success can never be read as a failure.
    SUCCESS_MARKERS = ["completed successfully", "created successfully",
                       "has been locked and is immutable until"]

    @staticmethod
    def _looks_like_failure(command_text):
        # Drop the summary table before scanning: its column header contains
        # "Errored", which otherwise matches the "error" token on every single
        # backup/restore, making success indistinguishable from failure.
        normalised_text = "\n".join(
            line for line in command_text.splitlines()
            if not line.strip().startswith("|")).lower()
        if any(marker in normalised_text
               for marker in WormBackupBase.SUCCESS_MARKERS):
            return False
        return any(token in normalised_text for token in
                   ["error", "failed", "failure", "denied", "invalid",
                    "unsupported", "not found", "cannot", "can't", "unable",
                    "must", "requires", "blocked", "rejected", "corrupt",
                    "mismatch", "incomplete", "unknown flag"])

    def _require_param(self, name, reason):
        value = self.input.param(name, None)
        if value in [None, ""]:
            self.fail("Set %s to run %s" % (name, reason))
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
            self.fail(
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
            self.fail(
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
        # No cli_flags here: those are --no-ssl-verify, which `worm` rejects
        # ("Unknown flag", exit 64). `worm` only touches the archive, never the
        # cluster, so it has no cluster TLS to skip -- its object-store
        # equivalent is --obj-no-ssl-verify, supplied via the cloud flags below.
        # CbBackupMgr.worm() omits cli_flags for the same reason.
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

    @staticmethod
    def _join_shell_statements(statements):
        """Join shell statements, honouring `&` as its own terminator.

        Joining everything with "; " yields `... ) &; wait || true`, and `&;`
        is a bash syntax error -- the whole command aborted before running, so
        the caller only ever saw "syntax error near unexpected token `;'".
        """
        joined = ""
        for index, statement in enumerate(statements):
            if index:
                joined += " " if statements[index - 1].rstrip().endswith("&") \
                    else "; "
            joined += statement
        return joined

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
        script = self._join_shell_statements(commands)
        self.log.debug("Concurrent WORM enable script: %s" % script)
        output, error = self.shell.execute_command(script)
        command_text = self._assert_command_success(output, error)
        success_count = len(re.findall(r"EXIT_\d+=0", command_text))
        # Log at info, not just on the failure path: for a race test, which
        # invocation won and how the losers were rejected is the result, and a
        # passing run would otherwise discard it.
        self.log.info(
            "Concurrent WORM enable: %s/%s succeeded. Per-command output:\n%s"
            % (success_count, command_count, command_text))
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
        self.log.info(
            "WORM: creating repo '%s' in archive '%s' "
            "(worm_period=%s, default_retention=%s, obj_staging_dir=%s)"
            % (self.backup_repo_name, self.backup_archive_dir, worm_period,
               default_retention, self.obj_staging_dir_cbbackup))
        output, error = self.backup_mgr.create_repo(
            self.backup_archive_dir, self.backup_repo_name, worm_period=worm_period,
            default_retention=default_retention,
            obj_staging_dir=self.obj_staging_dir_cbbackup)
        command_text = self._assert_command_success(output, error)
        self.log.info("WORM: repo created. cbbackupmgr output: %s" % command_text)
        return command_text

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
        self.log.info(
            "WORM: running backup on repo '%s' (resume=%s, purge=%s, "
            "full_backup=%s, threads=%s)"
            % (self.backup_repo_name, resume, purge, full_backup, threads))
        output, error = self.backup_mgr.backup(
            self.backup_archive_dir, self.backup_repo_name, resume=resume, purge=purge,
            full_backup=full_backup, no_progress_bar=no_progress_bar,
            threads=threads, obj_staging_dir=self.obj_staging_dir_cbbackup)
        command_text = self._assert_command_success(output, error)
        self.log.info("WORM: backup completed. cbbackupmgr output: %s" % command_text)
        return command_text

    def _run_restore(self, map_data=None, auto_create_buckets=False,
                     allow_non_worm=False, backup_id=None):
        self.log.info(
            "WORM: running restore from repo '%s' (map_data=%s, "
            "auto_create_buckets=%s, allow_non_worm=%s, backup_id=%s)"
            % (self.backup_repo_name, map_data, auto_create_buckets,
               allow_non_worm, backup_id))
        output, error = self.backup_mgr.restore(
            self.backup_archive_dir, self.backup_repo_name, no_progress_bar=True,
            map_data=map_data, auto_create_buckets=auto_create_buckets,
            allow_non_worm=allow_non_worm, backup_id=backup_id,
            obj_staging_dir=self.obj_staging_dir_cbbackup)
        command_text = self._assert_command_success(output, error)
        self.log.info("WORM: restore completed. cbbackupmgr output: %s" % command_text)
        return command_text

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
        # Seed real documents so backup/restore has data to validate. The
        # collection spec loads yielded 0 items, so use cbc-pillowfight
        # (ContinuousBackupBase.load_data_cbc_pillowfight) instead. Defaults to
        # ~1 MB of 1 KB docs; override with pillowfight_data_mb/pillowfight_doc_size.
        total_data_mb = int(self.input.param("pillowfight_data_mb", 1))
        doc_size = int(self.input.param("pillowfight_doc_size", 1024))
        return self.load_data_cbc_pillowfight(
            total_data_mb=total_data_mb, doc_size=doc_size)

    def _generate_additional_docs_and_return_count(self, num_docs=50,
                                                   doc_size=256):
        """Generate docs and return the count once all of them are visible.

        A single read after _wait_for_stats_all_buckets is not enough: the
        count can still be catching up, so callers got a partial total (e.g.
        1062 of 1074) and then compared a later backup against it.
        """
        baseline = self.bucket_util.get_buckets_item_count(
            self.cluster, self.bucket.name)
        output, error = self.backup_mgr.generate_docs(
            num_docs=num_docs, bucket_name=self.bucket.name, size=doc_size,
            obj_staging_dir=self.obj_staging_dir_cbbackup)
        self._assert_command_success(output, error)
        self.bucket_util._wait_for_stats_all_buckets(
            self.cluster, self.cluster.buckets)
        expected_count = baseline + num_docs
        end_time = time.time() + self.doc_count_timeout
        actual_count = None
        while time.time() < end_time:
            actual_count = self.bucket_util.get_buckets_item_count(
                self.cluster, self.bucket.name)
            if actual_count >= expected_count:
                return actual_count
            self.sleep(5, "Waiting for generated docs to become visible "
                          "(%s/%s)" % (actual_count, expected_count))
        self.fail(
            "Only %s of the expected %s items were visible within %ss after "
            "generating %s docs (baseline %s)"
            % (actual_count, expected_count, self.doc_count_timeout, num_docs,
               baseline))

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
        self.log.info("WORM: fetching cbbackupmgr info for repo '%s'"
                      % self.backup_repo_name)
        output, error = self.backup_mgr.info(
            self.backup_archive_dir, self.backup_repo_name,
            obj_staging_dir=self.obj_staging_dir_cbbackup)
        command_text = self._assert_command_success(output, error)
        self.log.info("WORM: cbbackupmgr info output:\n%s" % command_text)
        return command_text

    def _assert_repo_reports_worm(self):
        info_output = self._repo_info().lower()
        self.assertIn("worm", info_output,
                      "cbbackupmgr info did not report WORM state. "
                      "Run this suite against a WORM-enabled cbbackupmgr/Couchbase build.")
        self.assertIn(str(self.worm_period_days), info_output,
                      "cbbackupmgr info did not report the configured WORM period")
        self.log.info("WORM: repo '%s' reports WORM enabled with period=%s days"
                      % (self.backup_repo_name, self.worm_period_days))
        return info_output

    def _latest_backup_name(self):
        """Newest timestamped backup directory in the repo.

        Not helper.find_latest_backup_name(): that returns the last entry of a
        sorted list of every non-dot top-level name, so "logs" (or README.md /
        backup-meta.json) sorts after "2026-..." and it never returned a real
        backup -- callers ended up looking under "logs/" instead.
        """
        helper = self._require_cloud_helper("locating backups")
        backup_names = self._timestamped_backup_names(helper)
        self.assertTrue(backup_names,
                        "No backup directory found in WORM repo")
        return sorted(backup_names)[-1]

    def _assert_latest_backup_is_worm_locked(self):
        helper = self._require_cloud_helper("WORM lock validation")
        backup_name = self._latest_backup_name()
        target_path = helper.find_first_data_object(
            self.backup_archive_dir, self.backup_repo_name)
        if target_path is None:
            target_path = self._find_required_metadata_path(
                [".worm", "%s/.status_flag" % backup_name, ".status_flag"])
        # CSP Object Lock protects a specific object VERSION from being deleted
        # or overwritten in place; it does NOT prevent writing a NEW version.
        # A plain put/overwrite always succeeds on a versioning-enabled bucket
        # (versioning is mandatory for S3/Azure/GCP object lock) and does not
        # violate WORM, because a completed backup is self-describing and a
        # restore only reads the first/locked version of each object
        # (design doc 5.7). So an overwrite "succeeding" is expected and is NOT
        # evidence that WORM is broken. We instead assert that the object
        # carries an active retention lock whose expiry lies in the future --
        # that is the actual WORM guarantee.
        retain_until = helper.get_retention_until(
            self.backup_archive_dir, self.backup_repo_name, target_path)
        self.assertIsNotNone(
            retain_until,
            "WORM object '%s' has no retention lock -- WORM is not being "
            "enforced at the CSP" % target_path)
        now = time.time()
        self.assertGreater(
            retain_until, now,
            "WORM object '%s' retention has already expired "
            "(retain_until_epoch=%s, now_epoch=%s)"
            % (target_path, retain_until, now))
        self.log.info(
            "WORM: object '%s' is retention-locked until epoch %s "
            "(%.1f days out) -- immutability enforced at the CSP"
            % (target_path, retain_until, (retain_until - now) / 86400.0))
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
            suffix="%s/.status_flag" % backup_name)
        self.assertFalse(statusflag_paths,
                         "Incomplete WORM backup unexpectedly has .status_flag: %s"
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

    def _wait_for_remote_backup_process(self, pid, log_path, timeout=None):
        """Block until a node-launched backup exits; return its output.

        Pairs with _start_remote_backup_process(). Replaces the old
        FunctionCallTask helpers, which returned in ~130ms with empty
        stdout/stderr -- the backup never ran and callers could not tell.
        """
        timeout = timeout or self.background_backup_timeout
        end_time = time.time() + timeout
        while self._remote_pid_alive(pid):
            if time.time() >= end_time:
                self.shell.execute_command("kill -9 %s 2>/dev/null" % pid)
                self.fail("Background cbbackupmgr pid %s did not finish "
                          "within %ss" % (pid, timeout))
            self.sleep(5, "Waiting for background cbbackupmgr %s to finish"
                          % pid)
        command_text = self._read_remote_file(log_path)
        self.shell.execute_command("rm -f %s" % log_path)
        if command_text == "<empty>":
            self.fail("Background cbbackupmgr pid %s produced no output, so "
                      "it never ran -- do not treat this as a completed "
                      "backup." % pid)
        self.log.info("Background cbbackupmgr pid %s finished" % pid)
        return command_text

    def _interrupt_background_backup_after_objects(self):
        """Kill a backup that is provably mid-flight, and prove it afterwards.

        Launched on the node via nohup so we own cbbackupmgr's own pid. The
        previous FunctionCallTask version returned in ~130ms with empty
        stdout/stderr, and waited only for *any* object under the repo prefix
        -- which repo creation has already written -- so the pkill hit nothing
        and the "interrupted" backup was never started. Every caller then
        tested a fully complete backup while believing otherwise.
        """
        helper = self._require_cloud_helper("interrupted backup archive inspection")
        pid, log_path = self._start_remote_backup_process()
        try:
            backup_name = self._wait_for_backup_in_progress(
                helper, pid, self.interrupt_timeout)
            if backup_name is None:
                remote_log = self._read_remote_file(log_path)
                self.fail(
                    "Backup never reached a mid-flight state, so there is "
                    "nothing to interrupt (pid=%s alive=%s). cbbackupmgr log: %s"
                    % (pid, self._remote_pid_alive(pid), remote_log))
            self.shell.execute_command("kill -9 %s" % pid)
            self.sleep(5, "Waiting for killed cbbackupmgr to exit")
            if self._remote_pid_alive(pid):
                self.fail("cbbackupmgr pid %s survived kill -9" % pid)
        finally:
            self.shell.execute_command(
                "kill -9 %s 2>/dev/null; rm -f %s" % (pid, log_path))
        self.log.info(
            "Interrupted cbbackupmgr backup '%s' (pid %s) for repo %s"
            % (backup_name, pid, self.backup_repo_name))
        return backup_name

    def _start_remote_backup_process(self, resume=False, purge=False,
                                     threads=None):
        """Launch cbbackupmgr backup on the node and return (pid, log_path).

        The script execs the command so the pid we hold is cbbackupmgr itself
        and `kill -9` reaches it directly -- no pkill pattern matching, which
        was unreliable because repo_name is a shared constant from the conf.
        """
        marker = "worm_bkp_%s" % uuid.uuid4().hex[:12]
        script_path = "/tmp/%s.sh" % marker
        log_path = "/tmp/%s.log" % marker
        backup_cmd = self._build_backup_command(
            resume=resume, purge=purge, threads=threads)
        self.shell.execute_command(
            "cat > %s <<'WORM_EOF'\n#!/bin/sh\nexec %s\nWORM_EOF"
            % (script_path, backup_cmd))
        output, _ = self.shell.execute_command(
            "chmod +x %s; nohup %s > %s 2>&1 & echo $!"
            % (script_path, script_path, log_path))
        pid = "".join(output).strip()
        if not pid.isdigit():
            self.fail("Failed to launch background cbbackupmgr (pid=%r)" % pid)
        self.log.info("Started background cbbackupmgr pid=%s log=%s"
                      % (pid, log_path))
        return pid, log_path

    def _remote_pid_alive(self, pid):
        output, _ = self.shell.execute_command(
            "kill -0 %s 2>/dev/null && echo ALIVE || echo DEAD" % pid)
        return "ALIVE" in "".join(output)

    def _read_remote_file(self, path):
        output, _ = self.shell.execute_command("cat %s 2>/dev/null" % path)
        return self._command_stream_text(output)

    def _timestamped_backup_names(self, helper):
        """Only real backup directories.

        find_backup_names() returns every non-dot top-level entry, so
        README.md / logs / backup-meta.json come back as 'backups'.
        """
        return [name for name in helper.find_backup_names(
                    self.backup_archive_dir, self.backup_repo_name)
                if self.BACKUP_DIR_PATTERN.match(name)]

    def _wait_for_backup_in_progress(self, helper, pid, timeout):
        """Wait until the running backup has written real data.

        Returns the backup name, or None if the process died or nothing
        appeared before the timeout.
        """
        metadata_names = (".worm", ".status_flag", ".obj_versions", "plan.json")
        end_time = time.time() + timeout
        while time.time() < end_time:
            if not self._remote_pid_alive(pid):
                return None
            for name in self._timestamped_backup_names(helper):
                data_paths = [
                    path for path in helper.find_relative_paths(
                        self.backup_archive_dir, self.backup_repo_name,
                        contains="%s/" % name)
                    if not path.endswith(metadata_names)]
                if data_paths:
                    self.log.info(
                        "Backup '%s' is mid-flight (%s data objects written)"
                        % (name, len(data_paths)))
                    return name
            self.sleep(5, "Waiting for backup data objects to appear")
        return None
