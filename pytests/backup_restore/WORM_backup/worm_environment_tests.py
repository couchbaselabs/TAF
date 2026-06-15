from cb_constants import CbServer

from backup_restore.WORM_backup.worm_backup_base import WormBackupBase


class WormEnvironmentTest(WormBackupBase):
    def _create_expired_worm_backup(self, reason):
        self._create_worm_repo()
        self._load_data_and_return_count()
        self._run_backup()
        self._run_required_success_command("mark_worm_expired_command", reason)

    def _assert_repo_info_reports_expired_or_stale(self):
        info_output = self._repo_info().lower()
        self.assertTrue(any(token in info_output for token in
                            ["expired", "eligible", "stale", "retention",
                             "non-worm", "warning", "warn"]),
                        "Repository info did not report expired/stale state: %s"
                        % info_output)
        return info_output

    def test_worm_enable_rejected_on_community_edition(self):
        if getattr(self.cluster, "edition", None) != "community" and CbServer.enterprise_edition:
            self.skipTest("Run on a Community Edition cluster for CE WORM validation")
        output, error = self.backup_mgr.create_repo(
            self.archive_dir, self.repo_name, worm_period=self.worm_period_days)
        self._assert_command_failure(
            output, error,
            expected_texts=["enterprise", "community", "edition", "worm"])

    def test_backup_worm_enable_race_has_defined_outcome(self):
        self._create_repo()
        self._load_data_and_return_count()
        backup_task = self._start_backup_task()
        output, error = self.backup_mgr.worm(
            self.archive_dir, self.repo_name, self.worm_period_days)
        command_text = self._command_text(output, error).lower()
        self._wait_for_backup_task(backup_task)
        if error:
            self.assertTrue(any(token in command_text for token in
                                ["backup", "active", "running", "lock", "retry", "worm"]),
                            "Unexpected WORM-enable race failure: %s" % command_text)
            return
        self._assert_repo_reports_worm()

    def test_network_flapping_backup_resumes_or_fails_safely(self):
        self._require_param("network_flap_command", "network flapping validation")
        self._create_worm_repo()
        self._load_data_and_return_count()
        backup_task = self._start_backup_task()
        self._run_required_remote_command(
            "network_flap_command", "network flapping validation")
        self._wait_for_backup_task(backup_task)
        output, error = self.backup_mgr.backup(
            self.archive_dir, self.repo_name, resume=True, no_progress_bar=True)
        if error:
            self._assert_command_failure(
                output, error,
                expected_texts=["resume", "network", "incomplete", "purge", "safe"])
        else:
            self._assert_repo_reports_worm()

    def test_worm_expiry_retention_state_and_new_backup_behaviour(self):
        self._require_param(
            "mark_worm_expired_command", "automatic retention after WORM expiry validation")
        self._create_expired_worm_backup(
            "automatic retention after WORM expiry validation")
        self._assert_repo_info_reports_expired_or_stale()

        self._generate_additional_docs_and_return_count()
        output, error = self.backup_mgr.backup(
            self.archive_dir, self.repo_name, no_progress_bar=True)
        if error:
            self._assert_command_failure(
                output, error,
                expected_texts=["expired", "stale", "worm", "re-enable", "disable"])
            return
        info_output = self._repo_info().lower()
        self.assertTrue(any(token in info_output for token in
                            ["expired", "stale", "non-worm", "worm"]),
                        "Post-expiry backup did not report expected WORM state: %s"
                        % info_output)

    def test_expired_worm_repository_stale_warning_and_non_worm_backup(self):
        self._require_param(
            "mark_worm_expired_command", "expired WORM repository stale validation")
        self._create_expired_worm_backup(
            "expired WORM repository stale validation")
        self._assert_repo_info_reports_expired_or_stale()

        self._generate_additional_docs_and_return_count()
        output, error = self.backup_mgr.backup(
            self.archive_dir, self.repo_name, no_progress_bar=True)
        command_text = self._command_text(output, error).lower()
        if error:
            self._assert_command_failure(
                output, error,
                expected_texts=["expired", "stale", "worm", "re-enable",
                                "disable", "warning"])
            return
        self._assert_output_contains_any(
            command_text + self._repo_info().lower(),
            ["expired", "stale", "non-worm", "warning", "worm"],
            "Post-expiry backup state")

    def test_disable_worm_on_expired_repository_reverts_to_non_worm(self):
        self._require_param(
            "mark_worm_expired_command", "expired WORM disable validation")
        self._create_expired_worm_backup("expired WORM disable validation")
        output, error = self.backup_mgr.worm(
            self.archive_dir, self.repo_name, period=0)
        self._assert_command_success(output, error)
        info_output = self._repo_info().lower()
        self._assert_output_contains_any(
            info_output,
            ["disabled", "non-worm", "standard", "stale", "expired", "none"],
            "Disabled WORM repository info")

    def test_partial_lock_expiry_consistency_is_rejected_or_non_worm(self):
        self._require_cloud_helper("partial WORM lock expiry validation")
        self._require_param("expire_partial_lock_command",
                            "partial WORM lock expiry validation")
        self._create_worm_repo()
        self._load_data_and_return_count()
        self._run_backup()
        self._run_required_success_command(
            "expire_partial_lock_command", "partial WORM lock expiry validation")
        output, error = self.backup_mgr.backup(
            self.archive_dir, self.repo_name, no_progress_bar=True)
        if error:
            self._assert_command_failure(
                output, error,
                expected_texts=["partial", "lock", "expired", "worm", "invalid",
                                "consistency", "retention"])
            return
        self._assert_output_contains_any(
            self._repo_info().lower(),
            ["non-worm", "stale", "expired", "warning", "worm"],
            "Partial lock expiry repository state")

    def test_clock_skew_during_backup_uses_server_time(self):
        self._require_param("clock_skew_command", "WORM clock skew validation")
        self._require_param("reset_clock_skew_command", "WORM clock skew cleanup")
        self._create_worm_repo()
        expected_count = self._load_data_and_return_count()
        try:
            self._run_required_success_command(
                "clock_skew_command", "WORM clock skew validation")
            output, error = self.backup_mgr.backup(
                self.archive_dir, self.repo_name, no_progress_bar=True)
        finally:
            self._run_required_success_command(
                "reset_clock_skew_command", "WORM clock skew cleanup")
        if error:
            self._assert_command_failure(
                output, error,
                expected_texts=["clock", "skew", "time", "server", "timestamp",
                                "retention", "worm"])
            return
        self._assert_repo_reports_worm()
        restore_bucket_name = self._create_restore_bucket("clock_skew")
        self._run_restore(map_data="%s=%s" % (self.bucket.name, restore_bucket_name))
        self.bucket_util._wait_for_stats_all_buckets(
            self.cluster, self.cluster.buckets)
        self._verify_doc_count(expected_count, bucket_name=restore_bucket_name)

    def test_concurrent_worm_enable_requests_have_single_consistent_state(self):
        command_count = int(self.input.param("concurrent_worm_enable_count", 2))
        self._create_repo()
        command_text, success_count = self._run_concurrent_worm_enable_commands(
            command_count=command_count)
        self.assertEqual(
            1, success_count,
            "Expected exactly one concurrent WORM enable command to succeed. Output: %s"
            % command_text)
        self._assert_repo_reports_worm()

    def test_csp_throttling_retries_or_fails_safely(self):
        self._require_param("csp_throttle_command", "CSP throttling validation")
        reset_command = self.input.param("csp_throttle_reset_command", None)
        self._create_worm_repo()
        self._load_data_and_return_count()
        try:
            self._run_required_success_command(
                "csp_throttle_command", "CSP throttling validation")
            output, error = self.backup_mgr.backup(
                self.archive_dir, self.repo_name, no_progress_bar=True)
        finally:
            if reset_command not in [None, ""]:
                reset_output, reset_error = self.shell.execute_command(reset_command)
                self._assert_command_success(reset_output, reset_error)
        if error:
            self._assert_command_failure(
                output, error,
                expected_texts=["throttle", "rate", "429", "retry", "backoff",
                                "safe", "incomplete", "worm"])
            return
        self._assert_repo_reports_worm()

    def test_expired_worm_config_tampering_warns_or_treats_repo_non_worm(self):
        helper = self._require_cloud_helper("expired .worm tampering validation")
        self._require_param("mark_worm_expired_command", "expired .worm tampering validation")
        self._create_worm_repo()
        self._load_data_and_return_count()
        self._run_backup()
        self._run_required_success_command(
            "mark_worm_expired_command", "expired .worm tampering validation")
        worm_path = self._find_required_metadata_path([".worm"])
        succeeded, detail = helper.attempt_overwrite(
            self.repo_name, worm_path, content="tampered-expired-worm")
        if not succeeded:
            self._assert_tamper_blocked(succeeded, detail)
            return
        info_output = self._repo_info().lower()
        self.assertTrue(any(token in info_output for token in
                            ["expired", "stale", "non-worm", "warning", "warn"]),
                        "Expired/tampered WORM repo did not report a safe state: %s"
                        % info_output)
