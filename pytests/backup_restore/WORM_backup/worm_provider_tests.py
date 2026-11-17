from backup_restore.WORM_backup.worm_backup_base import WormBackupBase


class WormProviderTest(WormBackupBase):
    def _create_backup_and_select_object(self):
        helper = self._require_cloud_helper("provider WORM validation")
        self._create_worm_repo()
        self._load_data_and_return_count()
        self._run_backup()
        target_path = helper.find_first_data_object(
            self.backup_archive_dir, self.backup_repo_name)
        if target_path is None:
            target_path = self._find_required_metadata_path([".worm", ".status_flag"])
        return helper, target_path

    def _assert_object_has_retention(self, provider):
        helper = self._require_storage_provider(provider, "%s WORM retention validation" % provider)
        self._create_worm_repo()
        self._load_data_and_return_count()
        self._run_backup()
        target_path = helper.find_first_data_object(
            self.backup_archive_dir, self.backup_repo_name)
        if target_path is None:
            target_path = self._find_required_metadata_path([".worm", ".status_flag"])
        retain_until = helper.get_retention_until(
            self.backup_archive_dir, self.backup_repo_name, target_path)
        self.assertTrue(retain_until, "No retention timestamp reported for %s" % target_path)

    def test_azure_version_immutability_is_applied(self):
        self._assert_object_has_retention("Azure")

    def test_gcp_retention_lock_is_applied(self):
        self._assert_object_has_retention("GCP")

    def _gcp_composite_parts(self, helper):
        # GCS data files are assembled from parts named
        # "<key>-mpu-<upload id>-<part id>"
        return helper.find_relative_paths(
            self.backup_archive_dir, self.backup_repo_name, contains="-mpu-")

    def test_gcp_orphaned_composite_parts_are_cleaned_or_ignored(self):
        helper = self._require_storage_provider(
            "GCP", "GCP orphaned composite parts validation")
        self._create_worm_repo()
        expected_count = self._load_data_and_return_count()
        self._interrupt_background_backup_after_objects()
        parts_before = self._gcp_composite_parts(helper)
        self.log.info(f"GCP composite part objects before resume: "
                      f"{len(parts_before)}, e.g. {parts_before[:5]}")

        # 8.5.0-1442 refused this resume ("can't be resumed and must be
        # purged"); log the parts before asserting
        output, error = self.backup_mgr.backup(
            self.backup_archive_dir, self.backup_repo_name, resume=True,
            no_progress_bar=True, obj_staging_dir=self.obj_staging_dir_cbbackup)
        parts_after = self._gcp_composite_parts(helper)
        self.log.info(f"GCP composite part objects after resume: "
                      f"{len(parts_after)}, e.g. {parts_after[:5]}")
        self._assert_command_success(output, error)
        self._assert_repo_reports_worm()

        restore_bucket_name = self._create_restore_bucket("gcp_parts")
        self._run_restore(map_data="%s=%s" % (self.bucket.name, restore_bucket_name))
        self.bucket_util._wait_for_stats_all_buckets(
            self.cluster, self.cluster.buckets)
        self._verify_doc_count(expected_count, bucket_name=restore_bucket_name)

    def test_single_version_storage_and_versioning_disable_contract(self):
        self._require_storage_provider(
            ["AWS", "Azure", "GCP"], "single-version storage validation")
        helper, target_path = self._create_backup_and_select_object()
        versions = helper.list_object_versions(
            self.backup_archive_dir, self.backup_repo_name, target_path)
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
            self.backup_archive_dir, self.backup_repo_name, backup_range=backup_name,
            obj_staging_dir=self.obj_staging_dir_cbbackup)
        self._assert_command_failure(
            output, error,
            expected_texts=["worm", "lock", "retention", "compliance"])

    def test_retention_delete_locked_versions_fails_safely(self):
        helper, target_path = self._create_backup_and_select_object()
        backup_name = self._latest_backup_name()
        output, error = self.backup_mgr.remove(
            self.backup_archive_dir, self.backup_repo_name, backup_range=backup_name,
            obj_versions=True, obj_staging_dir=self.obj_staging_dir_cbbackup)
        self._assert_command_failure(
            output, error,
            expected_texts=["worm", "lock", "retention", "compliance",
                            "version", "object"])
        versions = helper.list_object_versions(
            self.backup_archive_dir, self.backup_repo_name, target_path)
        live_versions = [version for version in versions
                         if not version.get("delete_marker")]
        self.assertTrue(
            live_versions,
            "Locked object versions were removed despite active WORM retention")

    def test_s3_object_lock_disabled_backup_fails(self):
        self._require_storage_provider("AWS", "S3 object lock disabled validation")
        archive_dir = self._require_param(
            "s3_object_lock_disabled_archive_dir",
            "S3 object lock disabled validation")
        output, error = self.backup_mgr.create_repo(
            archive_dir, self.backup_repo_name, worm_period=self.worm_period_days,
            obj_staging_dir=self.obj_staging_dir_cbbackup, encrypted=self.ear_bk)
        self._assert_command_failure(
            output, error,
            expected_texts=["object lock", "retention", "worm", "lock", "configuration"])

    def test_azure_missing_immutability_policy_fails_validation(self):
        self._require_storage_provider("Azure", "Azure missing immutability validation")
        archive_dir = self._require_param(
            "azure_missing_immutability_archive_dir",
            "Azure missing immutability validation")
        output, error = self.backup_mgr.create_repo(
            archive_dir, self.backup_repo_name, worm_period=self.worm_period_days,
            obj_staging_dir=self.obj_staging_dir_cbbackup, encrypted=self.ear_bk)
        self._assert_command_failure(
            output, error,
            expected_texts=["immutability", "retention", "worm", "policy", "lock"])

    def test_non_worm_restore_and_obj_versions_cleanup_behaviour(self):
        helper = self._require_storage_provider(
            ["AWS", "Azure", "GCP"], "obj-versions cleanup validation")
        self._create_repo()
        expected_count = self._load_data_and_return_count()
        self._run_backup()

        restore_bucket_name = self._create_restore_bucket("non_worm_restore")
        self._run_restore(map_data="%s=%s" % (self.bucket.name, restore_bucket_name))
        self.bucket_util._wait_for_stats_all_buckets(
            self.cluster, self.cluster.buckets)
        self._verify_doc_count(expected_count, bucket_name=restore_bucket_name)

        target_path = helper.find_first_data_object(
            self.backup_archive_dir, self.backup_repo_name)
        self.assertTrue(target_path, "No backup object found for version cleanup")
        succeeded, detail = helper.upload_text(
            self.backup_archive_dir, self.backup_repo_name, target_path,
            "older-version")
        self.assertTrue(succeeded, "Failed to create older object version: %s" % detail)
        versions_before = helper.list_object_versions(
            self.backup_archive_dir, self.backup_repo_name, target_path)
        self.assertGreaterEqual(len(versions_before), 2,
                                "Expected multiple object versions before cleanup")
        output, error = self.backup_mgr.remove(
            self.backup_archive_dir, self.backup_repo_name, backup_range="1",
            obj_versions=True, obj_staging_dir=self.obj_staging_dir_cbbackup)
        self._assert_command_success(output, error)
        versions_after = helper.list_object_versions(
            self.backup_archive_dir, self.backup_repo_name, target_path)
        self.assertFalse(versions_after,
                         "Expected --obj-versions remove to clean all object versions")

    def _log_live_version_counts(self, helper, label):
        """Log live (non-delete-marker) version counts of each log object
        and of backup-meta.json, whose old versions cbbackupmgr prunes."""
        counts = {}
        for path in helper.find_relative_paths(self.backup_archive_dir,
                                               self.backup_repo_name):
            if "log" not in path.lower() and \
                    not path.endswith("backup-meta.json"):
                continue
            versions = helper.list_object_versions(
                self.backup_archive_dir, self.backup_repo_name, path)
            counts[path] = len([version for version in versions
                                if not version.get("delete_marker")])
        self.log.info(f"Live object versions {label}: {counts}")

    def test_worm_log_versioning_cleanup_limits_non_locked_log_versions(self):
        helper = self._require_storage_provider(
            ["AWS", "Azure", "GCP"], "WORM log versioning cleanup validation")
        self._create_worm_repo()
        self._load_data_and_return_count()
        self._run_backup()
        self._log_live_version_counts(helper, "after backup 1")
        self._generate_additional_docs_and_return_count()
        self._run_backup()
        self._log_live_version_counts(helper, "after backup 2")
        # Jenkins saw 2 log versions after 2 backups. A third shows whether
        # old versions are never pruned (3) or pruned a backup late (2).
        self._run_backup()
        self._log_live_version_counts(helper, "after backup 3")

        log_paths = [path for path in helper.find_relative_paths(
                        self.backup_archive_dir, self.backup_repo_name)
                     if "log" in path.lower()]
        if not log_paths:
            self.fail("No archive log objects found for version cleanup validation")
        for log_path in log_paths:
            versions = helper.list_object_versions(
                self.backup_archive_dir, self.backup_repo_name, log_path)
            live_versions = [version for version in versions
                             if not version.get("delete_marker")]
            self.assertLessEqual(
                len(live_versions), 1,
                "Expected at most one live version for log object %s, found %s"
                % (log_path, live_versions))
