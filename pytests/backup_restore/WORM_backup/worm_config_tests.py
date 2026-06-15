from backup_restore.WORM_backup.worm_backup_base import WormBackupBase


class WormConfigTest(WormBackupBase):
    def _run_existing_repo_mixed_restore_contract(self):
        self._create_repo()
        first_backup_count = self._load_data_and_return_count()
        self._run_backup()

        self._enable_worm()
        info_output = self._assert_repo_reports_worm()
        self.assertTrue(any(token in info_output for token in
                            ["active", "valid", "until", "worm"]),
                        "Active WORM metadata needed to disable UI action is missing")
        second_backup_count = self._generate_additional_docs_and_return_count()
        self.assertGreater(second_backup_count, first_backup_count,
                           "Expected the second backup to contain more documents")
        self._run_backup()

        restore_bucket_name = self._create_restore_bucket("mixed_allow")
        map_data = "%s=%s" % (self.bucket.name, restore_bucket_name)
        self._assert_restore_failure(
            map_data=map_data,
            expected_texts=["allow-non-worm", "warning", "worm", "non-worm", "restore"])

        self._run_restore(map_data=map_data, allow_non_worm=True)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(second_backup_count, bucket_name=restore_bucket_name)

        latest_restore_bucket = self._create_restore_bucket("latest_worm")
        self._run_restore(
            map_data="%s=%s" % (self.bucket.name, latest_restore_bucket))
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(second_backup_count, bucket_name=latest_restore_bucket)

    def test_worm_repo_info_contract_reports_period_and_validity(self):
        self._create_worm_repo()
        info_output = self._assert_repo_reports_worm()
        self.assertTrue(any(token in info_output for token in
                            ["default", "period", "retention", "valid"]),
                        "Repo metadata did not expose default WORM/validity fields")
        self.assertTrue(any(token in info_output for token in
                            ["valid", "until", "expiry", "expire", "retention"]),
                        "Repo metadata does not include WORM validity information")

    def test_invalid_worm_repo_configuration_is_rejected(self):
        output, error = self.backup_mgr.create_repo(
            self.archive_dir, self.repo_name,
            worm_period=self.worm_period_days,
            default_retention=max(1, self.worm_period_days - 1))
        self._assert_command_failure(
            output, error,
            expected_texts=["retention", "worm", "period", "conflict"])

        output, error = self.backup_mgr.create_repo(
            self.archive_dir, self.repo_name, worm_period=-1)
        self._assert_command_failure(
            output, error,
            expected_texts=["worm", "period", "invalid", "positive", "range"])

    def test_enable_worm_on_existing_repo_mixed_restore_contract(self):
        self._run_existing_repo_mixed_restore_contract()

    def test_mixed_mode_restore_integrity_with_allow_non_worm(self):
        self._run_existing_repo_mixed_restore_contract()
