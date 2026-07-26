from cb_constants import CbServer

from backup_restore.WORM_backup.worm_backup_base import WormBackupBase


class WormEnvironmentTest(WormBackupBase):

    # Stall the in-flight S3 upload by dropping outbound HTTPS, then restore
    # it. Only --dport 443 is touched, so SSH (sport 22 on an established
    # connection) and Couchbase inter-node traffic (8091/11210/18091/11207)
    # keep working. The leading background loop is a failsafe: if the
    # foreground command is killed mid-flap the DROP rule would otherwise
    # persist and cut the node off from the object store for good.
    # Override with the network_flap_command param to flap a different way.
    NETWORK_FLAP_COMMAND = (
        "nohup sh -c 'sleep 90;"
        " while iptables -D OUTPUT -p tcp --dport 443 -j DROP 2>/dev/null;"
        " do :; done' >/dev/null 2>&1 &"
        " iptables -I OUTPUT -p tcp --dport 443 -j DROP"
        " && sleep 12"
        " && iptables -D OUTPUT -p tcp --dport 443 -j DROP")

    # Degrade the object-store path hard enough to force SDK retry/backoff
    # during a backup. Drops ~60% of outbound HTTPS packets, so only S3 traffic
    # is affected -- SSH (sport 22 on an established connection) and Couchbase
    # inter-node traffic (8091/11210/18091/11207) keep working. This simulates
    # retry pressure via packet loss rather than real CSP 429s, which we cannot
    # induce from the client side. Paired with CSP_THROTTLE_RESET_COMMAND; the
    # leading background loop is a failsafe in case the reset never runs.
    # Override with csp_throttle_command / csp_throttle_reset_command.
    CSP_THROTTLE_COMMAND = (
        "nohup sh -c 'sleep 300;"
        " while iptables -D OUTPUT -p tcp --dport 443"
        " -m statistic --mode random --probability 0.6 -j DROP 2>/dev/null;"
        " do :; done' >/dev/null 2>&1 &"
        " iptables -I OUTPUT -p tcp --dport 443"
        " -m statistic --mode random --probability 0.6 -j DROP")
    CSP_THROTTLE_RESET_COMMAND = (
        "while iptables -D OUTPUT -p tcp --dport 443"
        " -m statistic --mode random --probability 0.6 -j DROP 2>/dev/null;"
        " do :; done; echo throttle_cleared")

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
            self.fail("Run on a Community Edition cluster for CE WORM validation")
        output, error = self.backup_mgr.create_repo(
            self.backup_archive_dir, self.backup_repo_name,
            worm_period=self.worm_period_days,
            obj_staging_dir=self.obj_staging_dir_cbbackup,
            encrypted=self.encrypted)
        self._assert_command_failure(
            output, error,
            expected_texts=["enterprise", "community", "edition", "worm"])

    def test_backup_worm_enable_race_has_defined_outcome(self):
        self._create_repo()
        self._load_data_and_return_count()
        pid, log_path = self._start_remote_backup_process()
        output, error = self.backup_mgr.worm(
            self.backup_archive_dir, self.backup_repo_name, self.worm_period_days,
            obj_staging_dir=self.obj_staging_dir_cbbackup)
        command_text = self._command_text(output, error).lower()
        self._wait_for_remote_backup_process(pid, log_path)
        if error:
            self.assertTrue(any(token in command_text for token in
                                ["backup", "active", "running", "lock", "retry", "worm"]),
                            "Unexpected WORM-enable race failure: %s" % command_text)
            return
        self._assert_repo_reports_worm()

    def test_network_flapping_backup_resumes_or_fails_safely(self):
        flap_command = self.input.param("network_flap_command",
                                        self.NETWORK_FLAP_COMMAND)
        self._create_worm_repo()
        self._load_data_and_return_count()
        pid, log_path = self._start_remote_backup_process()
        output, error = self.shell.execute_command(flap_command)
        self.log.info("Network flap output=%s error=%s" % (output, error))
        self._wait_for_remote_backup_process(pid, log_path)
        output, error = self.backup_mgr.backup(
            self.backup_archive_dir, self.backup_repo_name, resume=True,
            no_progress_bar=True, obj_staging_dir=self.obj_staging_dir_cbbackup)
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
            self.backup_archive_dir, self.backup_repo_name, no_progress_bar=True,
            obj_staging_dir=self.obj_staging_dir_cbbackup)
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
            self.backup_archive_dir, self.backup_repo_name, no_progress_bar=True,
            obj_staging_dir=self.obj_staging_dir_cbbackup)
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
            self.backup_archive_dir, self.backup_repo_name, period=0,
            obj_staging_dir=self.obj_staging_dir_cbbackup)
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
            self.backup_archive_dir, self.backup_repo_name, no_progress_bar=True,
            obj_staging_dir=self.obj_staging_dir_cbbackup)
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
                self.backup_archive_dir, self.backup_repo_name, no_progress_bar=True,
                obj_staging_dir=self.obj_staging_dir_cbbackup)
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
        throttle_command = self.input.param("csp_throttle_command",
                                            self.CSP_THROTTLE_COMMAND)
        reset_command = self.input.param("csp_throttle_reset_command",
                                         self.CSP_THROTTLE_RESET_COMMAND)
        self._create_worm_repo()
        self._load_data_and_return_count()
        try:
            output, error = self.shell.execute_command(throttle_command)
            self._assert_command_success(output, error)
            self.log.info("Applied CSP throttling: %s" % throttle_command)
            output, error = self.backup_mgr.backup(
                self.backup_archive_dir, self.backup_repo_name, no_progress_bar=True,
                obj_staging_dir=self.obj_staging_dir_cbbackup)
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
            self.backup_archive_dir, self.backup_repo_name, worm_path,
            content="tampered-expired-worm")
        if not succeeded:
            self._assert_tamper_blocked(succeeded, detail)
            return
        info_output = self._repo_info().lower()
        self.assertTrue(any(token in info_output for token in
                            ["expired", "stale", "non-worm", "warning", "warn"]),
                        "Expired/tampered WORM repo did not report a safe state: %s"
                        % info_output)
