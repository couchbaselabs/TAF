import time

from StatsLib.StatsOperations import StatsHelper

from backup_restore.WORM_backup.worm_backup_base import WormBackupBase


class WormUiContractTest(WormBackupBase):
    @staticmethod
    def _normalise_metric_timestamp(value):
        if value > 9999999999:
            return value / 1000
        return value

    def _run_optional_contract_command(self, param_name, reason, expected_texts=None):
        command = self.input.param(param_name, None)
        if command in [None, ""]:
            return False
        output, error = self._run_cbbackupmgr_raw(
            command, include_obj_staging=False)
        if expected_texts is None:
            self._assert_command_success(output, error)
        else:
            self._assert_command_failure(output, error, expected_texts=expected_texts)
        self.log.info("Validated %s" % reason)
        return True

    def _run_required_shell_contract_command(self, param_name, reason,
                                             expected_texts):
        command = self._require_param(param_name, reason)
        output, error = self.shell.execute_command(command)
        command_text = self._assert_command_success(output, error)
        self._assert_output_contains_any(command_text, expected_texts, reason)
        self.log.info("Validated %s" % reason)
        return command_text

    def _get_worm_metric_value(self):
        metric_name = self.input.param("worm_valid_metric_name", "worm_valid_until")
        wait_secs = int(self.input.param("metric_wait_secs", 30))
        if wait_secs:
            self.sleep(wait_secs, "Waiting for WORM metrics to be scraped")

        lines = []
        for server in self.cluster.servers:
            try:
                lines.extend(StatsHelper(server).get_all_metrics())
            except Exception as error:
                self.log.warning("Unable to fetch metrics from %s: %s"
                                 % (server.ip, error))
        candidates = [str(line).strip() for line in lines
                      if str(line).startswith(metric_name)]
        repo_candidates = [line for line in candidates if self.repo_name in line]
        if repo_candidates:
            candidates = repo_candidates
        self.assertTrue(candidates,
                        "Metric %s was not found in Prometheus output"
                        % metric_name)
        for line in candidates:
            parts = line.rsplit(" ", 1)
            if len(parts) != 2:
                continue
            try:
                return self._normalise_metric_timestamp(float(parts[1]))
            except ValueError:
                continue
        self.fail("Metric %s had no parseable timestamp value: %s"
                  % (metric_name, candidates))

    def _create_backup_and_get_worm_metric(self):
        self._create_worm_repo()
        self._load_data_and_return_count()
        self._run_backup()
        return self._get_worm_metric_value()

    def test_backup_service_ui_worm_command_contracts(self):
        exercised = self._run_optional_contract_command(
            "retention_obj_versions_command",
            "UI retention delete-all-object-versions contract validation")
        exercised = self._run_optional_contract_command(
            "enable_worm_with_retention_command",
            "UI enable-WORM retention prompt contract validation",
            expected_texts=["obj", "version", "retention", "worm", "prompt"]) or exercised
        exercised = self._run_optional_contract_command(
            "enable_worm_unpaused_repo_command",
            "UI pause-before-WORM-enable contract validation",
            expected_texts=["pause", "paused", "active", "repository", "worm"]) or exercised
        if not exercised:
            self.skipTest("Set a UI contract command parameter to run this validation")

    def test_ns_server_worm_expiry_alert_contract(self):
        self._create_worm_repo()
        setup_command = self.input.param("mark_worm_expiring_command", None)
        if setup_command not in [None, ""]:
            output, error = self.shell.execute_command(setup_command)
            self._assert_command_success(output, error)
        self._run_required_shell_contract_command(
            "worm_expiry_alert_command",
            "ns_server WORM expiry alert validation",
            ["worm", "expire", "expiration", "valid", "alert", "warning"])

    def test_prometheus_worm_validity_metric_reports_expiry(self):
        metric_value = self._create_backup_and_get_worm_metric()
        now = time.time()
        self.assertGreater(metric_value, now,
                           "worm_valid_until metric is not in the future")
        self.assertLessEqual(
            metric_value,
            now + ((self.worm_period_days + 2) * 24 * 60 * 60),
            "worm_valid_until metric is outside the configured WORM period")

    def test_backup_service_worm_event_logging_contract(self):
        self._run_required_shell_contract_command(
            "backup_service_worm_event_command",
            "Backup Service WORM event logging validation",
            ["worm", "event", "audit", "compliance", "expiration", "enabled"])

    def test_prometheus_worm_validity_metric_drift_with_archive(self):
        helper = self._require_cloud_helper("WORM metric drift validation")
        metric_value = self._create_backup_and_get_worm_metric()
        worm_path = self._find_required_metadata_path([".worm"])
        retention_until = helper.get_retention_until(self.repo_name, worm_path)
        self.assertTrue(retention_until,
                        "No CSP retention timestamp found for %s" % worm_path)
        retention_until = self._normalise_metric_timestamp(float(retention_until))
        tolerance_secs = int(self.input.param("metric_drift_tolerance_secs", 3600))
        self.assertLessEqual(
            abs(metric_value - retention_until), tolerance_secs,
            "worm_valid_until drifted from CSP retention by more than %s seconds. "
            "metric=%s retention=%s"
            % (tolerance_secs, metric_value, retention_until))

    def test_alert_spam_prevention_contract(self):
        self._run_required_shell_contract_command(
            "alert_spam_command",
            "WORM alert spam prevention validation",
            ["dedup", "deduplicated", "rate", "limited", "suppress",
             "single", "alert"])
