from backup_restore.WORM_backup.worm_backup_base import WormBackupBase


class WormProviderTest(WormBackupBase):
    def _create_backup_and_select_object(self):
        helper = self._require_cloud_helper("provider WORM validation")
        self._create_worm_repo()
        self._load_data_and_return_count()
        self._run_backup()
        target_path = helper.find_first_data_object(self.repo_name)
        if target_path is None:
            target_path = self._find_required_metadata_path([".worm", ".statusflag"])
        return helper, target_path

    def _assert_object_has_retention(self, provider):
        helper = self._require_storage_provider(provider, "%s WORM retention validation" % provider)
        self._create_worm_repo()
        self._load_data_and_return_count()
        self._run_backup()
        target_path = helper.find_first_data_object(self.repo_name)
        if target_path is None:
            target_path = self._find_required_metadata_path([".worm", ".statusflag"])
        retain_until = helper.get_retention_until(self.repo_name, target_path)
        self.assertTrue(retain_until, "No retention timestamp reported for %s" % target_path)

    def test_azure_version_immutability_is_applied(self):
        self._assert_object_has_retention("azure")

    def test_gcp_retention_lock_is_applied(self):
        self._assert_object_has_retention("gcs")

    def test_gcp_orphaned_composite_parts_are_cleaned_or_ignored(self):
        helper = self._require_storage_provider(
            "gcs", "GCP orphaned composite parts validation")
        self._create_worm_repo()
        expected_count = self._load_data_and_return_count()
        self._interrupt_background_backup_after_objects()
        part_paths_before_resume = helper.find_relative_paths(
            self.repo_name, contains="part")
        self.log.info("GCP part-like objects before resume: %s"
                      % part_paths_before_resume)

        self._run_backup(resume=True)
        self._assert_repo_reports_worm()

        restore_bucket_name = self._create_restore_bucket("gcp_parts")
        self._run_restore(map_data="%s=%s" % (self.bucket.name, restore_bucket_name))
        self.bucket_util._wait_for_stats_all_buckets(
            self.cluster, self.cluster.buckets)
        self._verify_doc_count(expected_count, bucket_name=restore_bucket_name)

    def test_single_version_storage_and_versioning_disable_contract(self):
        helper = self._require_storage_provider(
            ["aws", "azure", "gcs"], "single-version storage validation")
        helper, target_path = self._create_backup_and_select_object()
        versions = helper.list_object_versions(self.repo_name, target_path)
        live_versions = [version for version in versions
                         if not version.get("delete_marker")]
        self.assertEqual(
            1, len(live_versions),
            "Expected exactly one live object version for %s, found %s"
            % (target_path, live_versions))

        if self.input.param("disable_versioning_command", None) in [None, ""]:
            return
        output, error = self._run_required_remote_command(
            "disable_versioning_command",
            "disabling versioning after WORM validation")
        command_text = self._command_text(output, error)
        if error:
            self.assertTrue(any(token in command_text.lower() for token in
                                ["worm", "lock", "retention", "versioning", "denied"]),
                            "Unexpected versioning disable failure: %s" % command_text)
            return
        backup_name = self._latest_backup_name()
        output, error = self.backup_mgr.remove(
            self.archive_dir, self.repo_name, backup_range=backup_name)
        self._assert_command_failure(
            output, error,
            expected_texts=["worm", "lock", "retention", "compliance"])

    def test_retention_delete_locked_versions_fails_safely(self):
        helper, target_path = self._create_backup_and_select_object()
        backup_name = self._latest_backup_name()
        output, error = self.backup_mgr.remove(
            self.archive_dir, self.repo_name, backup_range=backup_name,
            obj_versions=True)
        self._assert_command_failure(
            output, error,
            expected_texts=["worm", "lock", "retention", "compliance",
                            "version", "object"])
        versions = helper.list_object_versions(self.repo_name, target_path)
        live_versions = [version for version in versions
                         if not version.get("delete_marker")]
        self.assertTrue(
            live_versions,
            "Locked object versions were removed despite active WORM retention")

    def test_s3_object_lock_disabled_backup_fails(self):
        self._require_storage_provider("aws", "S3 object lock disabled validation")
        archive_dir = self._require_param(
            "s3_object_lock_disabled_archive_dir",
            "S3 object lock disabled validation")
        output, error = self.backup_mgr.create_repo(
            archive_dir, self.repo_name, worm_period=self.worm_period_days)
        self._assert_command_failure(
            output, error,
            expected_texts=["object lock", "retention", "worm", "lock", "configuration"])

    def test_azure_missing_immutability_policy_fails_validation(self):
        self._require_storage_provider("azure", "Azure missing immutability validation")
        archive_dir = self._require_param(
            "azure_missing_immutability_archive_dir",
            "Azure missing immutability validation")
        output, error = self.backup_mgr.create_repo(
            archive_dir, self.repo_name, worm_period=self.worm_period_days)
        self._assert_command_failure(
            output, error,
            expected_texts=["immutability", "retention", "worm", "policy", "lock"])

    def test_non_worm_restore_and_obj_versions_cleanup_behaviour(self):
        helper = self._require_storage_provider(
            ["aws", "azure", "gcs"], "obj-versions cleanup validation")
        self._create_repo()
        expected_count = self._load_data_and_return_count()
        self._run_backup()

        restore_bucket_name = self._create_restore_bucket("non_worm_restore")
        self._run_restore(map_data="%s=%s" % (self.bucket.name, restore_bucket_name))
        self.bucket_util._wait_for_stats_all_buckets(
            self.cluster, self.cluster.buckets)
        self._verify_doc_count(expected_count, bucket_name=restore_bucket_name)

        target_path = helper.find_first_data_object(self.repo_name)
        self.assertTrue(target_path, "No backup object found for version cleanup")
        succeeded, detail = helper.upload_text(
            self.repo_name, target_path, "older-version")
        self.assertTrue(succeeded, "Failed to create older object version: %s" % detail)
        versions_before = helper.list_object_versions(self.repo_name, target_path)
        self.assertGreaterEqual(len(versions_before), 2,
                                "Expected multiple object versions before cleanup")
        output, error = self.backup_mgr.remove(
            self.archive_dir, self.repo_name, backup_range="1", obj_versions=True)
        self._assert_command_success(output, error)
        versions_after = helper.list_object_versions(self.repo_name, target_path)
        self.assertFalse(versions_after,
                         "Expected --obj-versions remove to clean all object versions")

    def test_worm_log_versioning_cleanup_limits_non_locked_log_versions(self):
        helper = self._require_storage_provider(
            ["aws", "azure", "gcs"], "WORM log versioning cleanup validation")
        self._create_worm_repo()
        self._load_data_and_return_count()
        self._run_backup()
        self._generate_additional_docs_and_return_count()
        self._run_backup()

        log_paths = [path for path in helper.find_relative_paths(self.repo_name)
                     if "log" in path.lower()]
        if not log_paths:
            self.skipTest("No archive log objects found for version cleanup validation")
        for log_path in log_paths:
            versions = helper.list_object_versions(self.repo_name, log_path)
            live_versions = [version for version in versions
                             if not version.get("delete_marker")]
            self.assertLessEqual(
                len(live_versions), 1,
                "Expected at most one live version for log object %s, found %s"
                % (log_path, live_versions))
