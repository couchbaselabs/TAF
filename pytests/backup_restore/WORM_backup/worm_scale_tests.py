from backup_restore.WORM_backup.worm_backup_base import WormBackupBase


class WormScaleTest(WormBackupBase):
    def test_file_descriptor_stress_has_clear_outcome(self):
        expected_collections = int(self.input.param("expected_collection_count", 10000))
        fd_limit = int(self.input.param("fd_ulimit", 256))
        threads = int(self.input.param("backup_threads", 32))
        self._assert_collection_count_at_least(expected_collections)
        self._create_worm_repo()
        self._load_data_and_return_count()

        output, error = self._run_backup_with_shell_prefix(
            "ulimit -n %d" % fd_limit, threads=threads)
        self.log.info(f"Backup under ulimit -n {fd_limit}: "
                      f"{self._command_text(output, error)}")
        # cbbackupmgr prints errors on stdout, not stderr. It needs a hard
        # limit of 512 + 2 per vBucket, else it refuses up front:
        # "open max file limit (256) is smaller than required (768)"
        if error or self._fatal_output_lines(output):
            self._assert_command_failure(
                output, error,
                expected_texts=["file limit", "too many open files",
                                "file descriptor", "ulimit"])
        else:
            self._assert_command_success(output, error)
        self._assert_repo_reports_worm()

    def test_scale_gsi_restore_metadata_and_high_concurrency_backup(self):
        expected_collections = int(self.input.param("expected_collection_count", 10000))
        threads = int(self.input.param("backup_threads", 32))
        self._assert_collection_count_at_least(expected_collections)
        self._create_gsi_indexes_for_all_collections()
        expected_index_count = self._index_count_for_bucket(self.bucket.name)
        self.assertGreaterEqual(expected_index_count, expected_collections)

        self._create_worm_repo()
        expected_count = self._load_data_and_return_count()
        self._run_backup(threads=threads)
        self._assert_repo_reports_worm()

        restore_bucket_name = self._create_restore_bucket("scale_restore")
        self._run_restore(map_data="%s=%s" % (self.bucket.name, restore_bucket_name))
        self.bucket_util._wait_for_stats_all_buckets(
            self.cluster, self.cluster.buckets)
        self._verify_doc_count(expected_count, bucket_name=restore_bucket_name)
        restored_index_count = self._index_count_for_bucket(restore_bucket_name)
        self.assertGreaterEqual(
            restored_index_count, expected_index_count,
            "Restored bucket has fewer GSI definitions than source bucket")
