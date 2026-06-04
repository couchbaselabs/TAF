from cb_tools.cbbackupmgr import CbBackupMgr

from backup_restore.WORM_backup.worm_backup_base import WormBackupBase


class WormIntegrityTest(WormBackupBase):
    def _prepare_interrupted_backup(self):
        self._require_cloud_helper("interrupted WORM integrity validation")
        self._create_worm_repo()
        self._load_data_and_return_count()
        self._interrupt_background_backup_after_objects()

    def _prepare_completed_backup(self):
        self._create_worm_repo()
        self._load_data_and_return_count()
        self._run_backup()

    def _external_write_then_resume_fails(self):
        helper = self._require_cloud_helper("external write resume validation")
        self._prepare_interrupted_backup()
        target_path = helper.find_first_data_object(self.repo_name)
        self.assertTrue(target_path, "No object found to modify in interrupted backup")
        succeeded, detail = helper.attempt_overwrite(
            self.repo_name, target_path, content="external-write")
        if not succeeded:
            self._assert_tamper_blocked(succeeded, detail)
            return
        output, error = self.backup_mgr.backup(
            self.archive_dir, self.repo_name, resume=True, no_progress_bar=True)
        self._assert_command_failure(
            output, error,
            expected_texts=["version", "obj_versions", "integrity", "tamper", "mismatch"])

    def test_incomplete_backup_tampering_and_staging_mismatch_are_rejected(self):
        helper = self._require_cloud_helper("status flag tampering validation")
        self._prepare_interrupted_backup()

        alternate_staging_dir = "%s_alt" % self.obj_staging_dir
        output, error = self.shell.execute_command(
            "mkdir -p %s" % alternate_staging_dir)
        self._assert_command_success(output, error)
        alternate_mgr = CbBackupMgr(
            self.shell,
            username=self.cluster.master.rest_username,
            password=self.cluster.master.rest_password,
            log=self.log,
            obj_staging_dir=alternate_staging_dir,
            aws_region=self.aws_region)
        output, error = alternate_mgr.backup(
            self.archive_dir, self.repo_name, resume=True, no_progress_bar=True)
        self._assert_command_failure(
            output, error,
            expected_texts=["staging", "resume", "obj", "state", "different"])

        backup_name = self._latest_backup_name()
        succeeded, detail = helper.upload_text(
            self.repo_name, "%s/.statusflag" % backup_name,
            '{"purged": false}')
        if not succeeded:
            self._assert_tamper_blocked(succeeded, detail)
            return
        self._assert_incomplete_backup_is_not_restorable()

    def test_fake_backup_metadata_is_not_trusted(self):
        helper = self._require_cloud_helper("dual status flag race validation")
        self._create_worm_repo()
        backup_name = "fake_race_backup"
        succeeded, detail = helper.upload_text(
            self.repo_name, "%s/.statusflag" % backup_name,
            '{"purged": false}')
        self.assertTrue(succeeded, "Failed to create race status flag: %s" % detail)
        self._assert_restore_failure(
            backup_id=backup_name,
            expected_texts=["missing", "metadata", "invalid", "integrity", "backup", "restore"])

        fake_backup = "fake_backup"
        status_ok, status_detail = helper.upload_text(
            self.repo_name, "%s/.statusflag" % fake_backup,
            '{"purged": false}')
        plan_ok, plan_detail = helper.upload_text(
            self.repo_name, "%s/plan.json" % fake_backup,
            '{"fake": true}')
        self.assertTrue(status_ok and plan_ok,
                        "Failed to create fake backup metadata: %s %s"
                        % (status_detail, plan_detail))
        self._assert_restore_failure(
            backup_id=fake_backup,
            expected_texts=["invalid", "integrity", "metadata", "backup", "restore", "trusted"])

    def test_external_write_version_mismatch_resume_validation_fails(self):
        self._external_write_then_resume_fails()

    def test_metadata_tampering_blocks_restore_or_is_lock_blocked(self):
        helper = self._require_cloud_helper("metadata corruption validation")
        self._prepare_completed_backup()
        metadata_path = self._find_required_metadata_path(
            ["plan.json", ".obj_versions", ".statusflag"])
        succeeded, detail = helper.attempt_overwrite(
            self.repo_name, metadata_path, content="not-json")
        if not succeeded:
            self._assert_tamper_blocked(succeeded, detail)
        else:
            self._assert_restore_failure(
                expected_texts=["metadata", "invalid", "corrupt", "integrity", "restore"])

        metadata_path = self._find_required_metadata_path(
            ["plan.json", ".obj_versions", ".statusflag"])
        succeeded, detail = helper.delete_object(self.repo_name, metadata_path)
        if not succeeded:
            self._assert_tamper_blocked(succeeded, detail)
            return
        self._assert_restore_failure(
            expected_texts=["missing", "metadata", "invalid", "integrity", "restore"])
