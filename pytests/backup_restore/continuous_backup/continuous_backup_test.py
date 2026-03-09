import importlib

from backup_restore.continuous_backup.continuous_backup_base import ContinuousBackupBase
from collections_helper.collections_spec_constants import MetaConstants
from pytests.bucket_collections.collections_base import CollectionBase


class ContinuousBackupTest(ContinuousBackupBase):
    def setUp(self):
        super(ContinuousBackupTest, self).setUp()
        self.log.info(f"Loading spec from: {self.spec_name}")
        spec_module = importlib.import_module(f"bucket_collections.bucket_templates.{self.spec_name}")
        self.spec = spec_module.spec

    def tearDown(self):
        super(ContinuousBackupTest, self).tearDown()

    def _get_total_docs(self, multiplier=1):
        num_scopes = self.spec.get(MetaConstants.NUM_SCOPES_PER_BUCKET, 1)
        num_collections = self.spec.get(MetaConstants.NUM_COLLECTIONS_PER_SCOPE, 1)
        num_items = self.spec.get(MetaConstants.NUM_ITEMS_PER_COLLECTION, 0)
        return num_scopes * num_collections * num_items * multiplier

    def _verify_continuous_backup_params(self):
        self.log.info("Getting continuous backup params from bucket: %s" % self.bucket.name)
        params = self.bucket_util.get_continuous_backup_params(self.cluster, self.bucket.name)
        self.log.info("Retrieved continuous backup params:")
        for key, value in params.items():
            self.log.info("  - %s: %s" % (key, value))
        verification_passed = True
        expected_enabled = "true" if self.continuous_backup_enabled else "false"
        actual_enabled = str(params.get("continuousBackupEnabled", "")).lower()
        if actual_enabled != expected_enabled:
            self.log.error("continuousBackupEnabled mismatch. Expected: %s, Actual: %s" %
                         (expected_enabled, actual_enabled))
            verification_passed = False
        else:
            self.log.info("continuousBackupEnabled verified: %s" % params.get("continuousBackupEnabled"))
        if params.get("continuousBackupLocation") != self.continuous_backup_location:
            self.log.error("continuousBackupLocation mismatch. Expected: %s, Actual: %s" %
                         (self.continuous_backup_location, params.get("continuousBackupLocation")))
            verification_passed = False
        else:
            self.log.info("continuousBackupLocation verified: %s" % params.get("continuousBackupLocation"))
        if params.get("continuousBackupInterval") != self.continuous_backup_interval:
            self.log.error("continuousBackupInterval mismatch. Expected: %s, Actual: %s" %
                         (self.continuous_backup_interval, params.get("continuousBackupInterval")))
            verification_passed = False
        else:
            self.log.info("continuousBackupInterval verified: %s" % params.get("continuousBackupInterval"))
        expected_history_retention = 6000
        if params.get("historyRetentionSeconds") != expected_history_retention:
            self.log.error("historyRetentionSeconds mismatch. Expected: %s, Actual: %s" %
                         (expected_history_retention, params.get("historyRetentionSeconds")))
            verification_passed = False
        else:
            self.log.info("historyRetentionSeconds verified: %s" % params.get("historyRetentionSeconds"))
        if verification_passed:
            self.log.info("Continuous backup parameter verification completed successfully")
        else:
            self.log.error("Continuous backup parameter verification failed")
            self.fail("Continuous backup parameter verification failed")

    def test_pitr_happy_path(self):
        """
        Happy path for point-in-time recovery.
        """
        self._verify_continuous_backup_params()

        CollectionBase.load_data_from_spec_file(self, self.data_spec_name)

        self.sleep(self.continuous_backup_interval * 60, f"Waiting for {self.continuous_backup_interval} minutes...")

        self.log.info("Deleting bucket for restore: %s" % self.bucket.name)
        self.bucket_util.delete_bucket(self.cluster, self.bucket)

        self.log.info("Creating bucket: %s" % self.bucket.name)
        self.bucket_util.create_bucket(self.cluster, self.bucket)
        self.log.info("Restoring from backup")
        self.backup_mgr.restore(self.backup_archive_dir, self.backup_repo_name)
        self._verify_doc_count(self._get_total_docs())

        self.log.info("Performing continuous backup restore")
        for scope_name, scope in self.bucket.scopes.items():
            if scope_name.startswith("_system"):
                continue
            for collection_name, collection in scope.collections.items():
                include_data = f"{self.bucket.name}.{scope_name}.{collection_name}"
                map_data = f"{self.bucket.name}.{scope_name}.{collection_name}={self.bucket.name}.{scope_name}.{collection_name}"
                self.cont_bk_mgr.restore(
                    self.backup_archive_dir, self.backup_repo_name,
                    location=self.continuous_backup_location,
                    temp_dir="/tmp",
                    include_data=include_data,
                    map_data=map_data
                )
        self.log.info("Continuous backup restore completed for all collections")

        self._verify_doc_count(self._get_total_docs(2))
        self.log.info("Test completed successfully")

    def test_pitr_timestamp_after_first_cont_backup(self):
        """
        Point-in-time recovery with a specific timestamp after the first continuous backup.
        """
        self._verify_continuous_backup_params()

        CollectionBase.load_data_from_spec_file(self, self.data_spec_name)

        self.sleep(self.continuous_backup_interval * 60, f"Waiting for {self.continuous_backup_interval} minutes...")

        timestamp = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"Timestamp after second load: {timestamp}")
        
        CollectionBase.load_data_from_spec_file(self, self.data_spec_name)

        self.sleep(self.continuous_backup_interval * 60, f"Waiting for {self.continuous_backup_interval} minutes...")

        self.log.info("Deleting bucket for restore: %s" % self.bucket.name)
        self.bucket_util.delete_bucket(self.cluster, self.bucket)

        self.log.info("Creating bucket: %s" % self.bucket.name)
        self.bucket_util.create_bucket(self.cluster, self.bucket)
        self.log.info("Restoring from backup")
        self.backup_mgr.restore(self.backup_archive_dir, self.backup_repo_name)

        self.log.info(f"Performing continuous backup restore with timestamp: {timestamp}")
        for scope_name, scope in self.bucket.scopes.items():
            if scope_name.startswith("_system"):
                continue
            for collection_name, collection in scope.collections.items():
                include_data = f"{self.bucket.name}.{scope_name}.{collection_name}"
                map_data = f"{self.bucket.name}.{scope_name}.{collection_name}={self.bucket.name}.{scope_name}.{collection_name}"
                self.cont_bk_mgr.restore(
                    self.backup_archive_dir, self.backup_repo_name,
                    location=self.continuous_backup_location,
                    temp_dir="/tmp",
                    timestamp=timestamp,
                    include_data=include_data,
                    map_data=map_data
                )
        self.log.info("Continuous backup restore completed for all collections")

        self._verify_doc_count(self._get_total_docs(2))
        self.log.info("Test completed successfully")

    def test_pitr_timestamp_after_second_cont_backup(self):
        """
        Point-in-time recovery with a specific timestamp after the second continuous backup.
        """
        self._verify_continuous_backup_params()
        CollectionBase.load_data_from_spec_file(self, self.data_spec_name)

        CollectionBase.load_data_from_spec_file(self, self.data_spec_name)

        self.sleep(self.continuous_backup_interval * 60, f"Waiting for {self.continuous_backup_interval} minutes...")

        CollectionBase.load_data_from_spec_file(self, self.data_spec_name)

        self.sleep(self.continuous_backup_interval * 60, f"Waiting for {self.continuous_backup_interval} minutes...")

        timestamp = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"Timestamp after third load: {timestamp}")
        self.log.info("Deleting bucket for restore: %s" % self.bucket.name)
        self.bucket_util.delete_bucket(self.cluster, self.bucket)

        self.log.info("Creating bucket: %s" % self.bucket.name)
        self.bucket_util.create_bucket(self.cluster, self.bucket)
        self.log.info("Restoring from backup")
        self.backup_mgr.restore(self.backup_archive_dir, self.backup_repo_name)

        self.log.info(f"Performing continuous backup restore with timestamp: {timestamp}")
        for scope_name, scope in self.bucket.scopes.items():
            if scope_name.startswith("_system"):
                continue
            for collection_name, collection in scope.collections.items():
                include_data = f"{self.bucket.name}.{scope_name}.{collection_name}"
                map_data = f"{self.bucket.name}.{scope_name}.{collection_name}={self.bucket.name}.{scope_name}.{collection_name}"
                self.cont_bk_mgr.restore(
                    self.backup_archive_dir, self.backup_repo_name,
                    location=self.continuous_backup_location,
                    temp_dir="/tmp",
                    timestamp=timestamp,
                    include_data=include_data,
                    map_data=map_data
                )
        self.log.info("Continuous backup restore completed for all collections")

        self._verify_doc_count(self._get_total_docs(3))
        self.log.info("Test completed successfully")
