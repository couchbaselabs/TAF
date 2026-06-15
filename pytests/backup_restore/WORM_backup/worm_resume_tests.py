from backup_restore.WORM_backup.worm_backup_base import WormBackupBase


class WormResumeTest(WormBackupBase):
    def _prepare_interrupted_worm_backup(self):
        self._require_cloud_helper("interrupted WORM resume validation")
        self._create_worm_repo()
        self._load_data_and_return_count()
        self._interrupt_background_backup_after_objects()

    def test_interrupted_backup_resume_window_statusflag_and_metadata(self):
        self._prepare_interrupted_worm_backup()
        info_output = self._repo_info().lower()
        self.assertTrue(any(token in info_output for token in
                            ["resume", "resumable", "expiry", "expire", "worm"]),
                        "Incomplete backup metadata does not expose WORM/resume expiry")
        self._assert_no_statusflag_for_latest_backup()
        self._assert_incomplete_backup_is_not_restorable()

        self._run_backup(resume=True)
        info_output = self._assert_repo_reports_worm()
        self.assertTrue(any(token in info_output for token in
                            ["valid", "until", "expiry", "expire", "worm"]),
                        "Completed backup metadata does not expose WORM period end")

    def test_resume_outside_window_requires_purge(self):
        self._prepare_interrupted_worm_backup()
        self._run_required_success_command(
            "expire_resume_window_command",
            "resume outside WORM window validation")
        output, error = self.backup_mgr.backup(
            self.archive_dir, self.repo_name, resume=True, no_progress_bar=True)
        self._assert_command_failure(
            output, error,
            expected_texts=["resume", "expired", "expire", "window", "purge",
                            "obj_versions", "incomplete", "worm"])

    def test_s3_multipart_upload_strategy_resumes(self):
        self._require_storage_provider("aws", "S3 multipart WORM resume validation")
        large_doc_size = int(self.input.param("large_doc_size", 6 * 1024 * 1024))
        self._create_worm_repo()
        self._generate_additional_docs_and_return_count(
            num_docs=int(self.input.param("large_doc_count", 2)),
            doc_size=large_doc_size)

        self._interrupt_background_backup_after_objects()
        self._run_backup(resume=True)
        self._assert_repo_reports_worm()
