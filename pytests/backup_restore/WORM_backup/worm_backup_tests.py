from backup_restore.WORM_backup.worm_backup_base import WormBackupBase


class WormBackupTest(WormBackupBase):
    def test_worm_backup_restore_and_locked_overwrite_semantics(self):
        self._create_worm_repo()
        expected_count = self._load_data_and_return_count()
        self._run_backup()
        self._assert_repo_reports_worm()
        if self.validate_csp_locks:
            self._require_cloud_helper("WORM lock validation")
            self._assert_latest_backup_is_worm_locked()

        restore_bucket_name = self._create_restore_bucket("worm_restore")
        self._run_restore(map_data="%s=%s" % (self.bucket.name, restore_bucket_name))
        self.bucket_util._wait_for_stats_all_buckets(
            self.cluster, self.cluster.buckets)
        self._verify_doc_count(expected_count, bucket_name=restore_bucket_name)

    def test_locked_worm_backup_lifecycle_operations_fail_safely(self):
        self._create_worm_repo()
        self._load_data_and_return_count()
        self._run_backup()

        if self.input.param("reduce_retention_command", None) not in [None, ""]:
            self._run_required_remote_command(
                "reduce_retention_command", "retention reduction validation")

        output, error = self.backup_mgr.remove(
            self.archive_dir, self.repo_name, backup_range="1")
        self._assert_command_failure(
            output, error,
            expected_texts=["worm", "compliance", "locked", "retention"])

        output, error = self.backup_mgr.remove(self.archive_dir, self.repo_name)
        self._assert_command_failure(
            output, error,
            expected_texts=["worm", "compliance", "locked", "retention", "backup"])

        output, error = self.backup_mgr.worm(
            self.archive_dir, self.repo_name, period=0)
        command_text = self._command_text(output, error).lower()
        if error:
            self.assertTrue(any(token in command_text for token in
                                ["worm", "retention", "compliance", "expired", "active"]),
                            "Unexpected WORM disable failure: %s" % command_text)
        output, error = self.backup_mgr.remove(
            self.archive_dir, self.repo_name, backup_range="1")
        self._assert_command_failure(
            output, error,
            expected_texts=["worm", "compliance", "locked", "retention"])
