from cb_constants import CbServer

from backup_restore.WORM_backup.worm_backup_base import WormBackupBase


class WormEnvironmentTest(WormBackupBase):
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
        pid_file, _ = self._start_backup_in_background()
        output, error = self.backup_mgr.worm(
            self.archive_dir, self.repo_name, self.worm_period_days)
        command_text = self._command_text(output, error).lower()
        self._wait_for_background_process(pid_file)
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
        pid_file, _ = self._start_backup_in_background()
        self._run_required_remote_command(
            "network_flap_command", "network flapping validation")
        self._wait_for_background_process(pid_file)
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
        self._create_worm_repo()
        self._load_data_and_return_count()
        self._run_backup()
        self._run_required_remote_command(
            "mark_worm_expired_command", "automatic retention after WORM expiry validation")
        info_output = self._repo_info().lower()
        self.assertTrue(any(token in info_output for token in
                            ["expired", "eligible", "stale", "retention"]),
                        "Repository info did not report expired/retention state: %s"
                        % info_output)

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

    def test_expired_worm_config_tampering_warns_or_treats_repo_non_worm(self):
        helper = self._require_cloud_helper("expired .worm tampering validation")
        self._require_param("mark_worm_expired_command", "expired .worm tampering validation")
        self._create_worm_repo()
        self._load_data_and_return_count()
        self._run_backup()
        self._run_required_remote_command(
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
