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
        expected_count = self._load_data_and_return_count()
        self._run_backup()
        return expected_count

    def _restore_to_new_bucket_and_verify(self, expected_count, suffix):
        restore_bucket_name = self._create_restore_bucket(suffix)
        self._run_restore(map_data="%s=%s" % (self.bucket.name, restore_bucket_name))
        self.bucket_util._wait_for_stats_all_buckets(
            self.cluster, self.cluster.buckets)
        self._verify_doc_count(expected_count, bucket_name=restore_bucket_name)

    def _restore_after_injection_succeeds_or_fails_cleanly(self, expected_count,
                                                           suffix):
        restore_bucket_name = self._create_restore_bucket(suffix)
        map_data = "%s=%s" % (self.bucket.name, restore_bucket_name)
        output, error = self.backup_mgr.restore(
            self.archive_dir, self.repo_name, no_progress_bar=True,
            map_data=map_data)
        if error:
            self._assert_command_failure(
                output, error,
                expected_texts=["metadata", "invalid", "integrity", "missing",
                                "restore", "ignore", "unknown"])
            return
        self._assert_command_success(output, error)
        self.bucket_util._wait_for_stats_all_buckets(
            self.cluster, self.cluster.buckets)
        self._verify_doc_count(expected_count, bucket_name=restore_bucket_name)

    def _delete_local_obj_versions(self):
        command = "find %s -name .obj_versions -type f -print -delete" \
                  % self.obj_staging_dir
        output, error = self.shell.execute_command(command)
        self._assert_command_success(output, error)
        self.assertTrue(output,
                        "No local .obj_versions file found in %s"
                        % self.obj_staging_dir)
        return output

    def _prepare_completed_backup_injection(self, reason):
        helper = self._require_cloud_helper(reason)
        expected_count = self._prepare_completed_backup()
        backup_name = self._latest_backup_name()
        return helper, expected_count, backup_name

    def _assert_third_party_addition_is_ignored(self, helper, expected_count,
                                                backup_name):
        succeeded, detail = helper.upload_text(
            self.repo_name, "%s/third_party/untracked-object.txt" % backup_name,
            "third-party-object")
        self.assertTrue(succeeded,
                        "Failed to add third-party object: %s" % detail)
        self._restore_to_new_bucket_and_verify(expected_count,
                                               "third_party_addition")

    def _assert_purged_flag_injection_is_blocked_or_ignored(self, helper,
                                                            expected_count,
                                                            backup_name):
        statusflag_path = self._find_required_metadata_path(
            ["%s/.statusflag" % backup_name, ".statusflag"])
        succeeded, detail = helper.attempt_overwrite(
            self.repo_name, statusflag_path, content='{"purged": true}')
        if not succeeded:
            self._assert_tamper_blocked(succeeded, detail)
            return
        self._restore_to_new_bucket_and_verify(expected_count,
                                               "purged_flag_injection")

    def _assert_extra_metadata_injection_is_clean(self, helper, expected_count,
                                                  backup_name):
        succeeded, detail = helper.upload_text(
            self.repo_name, "%s/injected-extra-metadata.json" % backup_name,
            '{"objects": [{"path": "missing-object", "versionId": "fake"}]}')
        self.assertTrue(succeeded,
                        "Failed to add extra metadata object: %s" % detail)
        self._restore_after_injection_succeeds_or_fails_cleanly(
            expected_count, "extra_metadata_injection")

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

    def test_completed_backup_untrusted_additions_are_ignored_or_blocked(self):
        helper, expected_count, backup_name = self._prepare_completed_backup_injection(
            "completed backup injection validation")
        self._assert_third_party_addition_is_ignored(
            helper, expected_count, backup_name)
        self._assert_purged_flag_injection_is_blocked_or_ignored(
            helper, expected_count, backup_name)
        self._assert_extra_metadata_injection_is_clean(
            helper, expected_count, backup_name)

    def test_third_party_addition_is_ignored_by_restore(self):
        helper, expected_count, backup_name = self._prepare_completed_backup_injection(
            "third-party addition validation")
        self._assert_third_party_addition_is_ignored(
            helper, expected_count, backup_name)

    def test_completed_purged_flag_injection_is_blocked_or_ignored(self):
        helper, expected_count, backup_name = self._prepare_completed_backup_injection(
            "purged flag injection validation")
        self._assert_purged_flag_injection_is_blocked_or_ignored(
            helper, expected_count, backup_name)

    def test_extra_metadata_injection_is_ignored_or_fails_cleanly(self):
        helper, expected_count, backup_name = self._prepare_completed_backup_injection(
            "extra metadata injection validation")
        self._assert_extra_metadata_injection_is_clean(
            helper, expected_count, backup_name)

    def test_resume_with_missing_obj_versions_fails_safely(self):
        self._prepare_interrupted_backup()
        deleted_paths = self._delete_local_obj_versions()
        self.log.info("Deleted local .obj_versions files before resume: %s"
                      % deleted_paths)
        output, error = self.backup_mgr.backup(
            self.archive_dir, self.repo_name, resume=True, no_progress_bar=True)
        self._assert_command_failure(
            output, error,
            expected_texts=["obj_versions", "resume", "missing", "purge",
                            "staging", "state", "incomplete"])

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
