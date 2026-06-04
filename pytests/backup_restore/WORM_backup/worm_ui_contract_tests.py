from backup_restore.WORM_backup.worm_backup_base import WormBackupBase


class WormUiContractTest(WormBackupBase):
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
