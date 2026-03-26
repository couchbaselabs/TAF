import importlib
import random
import time
from concurrent.futures import ThreadPoolExecutor

from backup_restore.continuous_backup.continuous_backup_base import ContinuousBackupBase
from collections_helper.collections_spec_constants import MetaConstants, MetaCrudParams
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

    def _load_data_and_get_task(self, data_spec_name):
        self.log.info("Load docs using spec file %s" % data_spec_name)
        doc_loading_spec = \
            self.bucket_util.get_crud_template_from_package(data_spec_name)
        # Process params to over_ride values if required
        CollectionBase.over_ride_doc_loading_template_params(
            self, doc_loading_spec)
        CollectionBase.set_retry_exceptions(
            doc_loading_spec, self.durability_level)
        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.cluster.buckets,
                doc_loading_spec,
                mutation_num=0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                load_using=self.load_docs_using)
        if doc_loading_task.result is False:
            self.fail("Initial doc_loading failed")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        return doc_loading_task

    def _perform_continuous_restore(self, timestamp=None, restore_to_new_bucket=False):
        self.log.info(f"Performing continuous backup restore with timestamp: {timestamp}")
        for scope_name, scope in self.bucket.scopes.items():
            if scope_name.startswith("_system"):
                continue
            for collection_name, collection in scope.collections.items():
                include_data = f"{self.bucket.name}.{scope_name}.{collection_name}"
                if restore_to_new_bucket:
                    map_data = f"{self.bucket.name}.{scope_name}.{collection_name}={self.restore_bucket_name}.{scope_name}.{collection_name}"
                else:
                    map_data = f"{self.bucket.name}.{scope_name}.{collection_name}={self.bucket.name}.{scope_name}.{collection_name}"
                self.cont_bk_mgr.restore(
                    self.backup_archive_dir, self.backup_repo_name,
                    location=self.continuous_backup_location,
                    temp_dir="/tmp",
                    timestamp=timestamp,
                    include_data=include_data,
                    map_data=map_data
                )
        self.log.info("Continuous backup restore completed")

    def _restore_entire_bucket(self, timestamp, target_bucket_name, include_data=None, map_data=None):
        self.log.info(f"Restoring entire bucket to {target_bucket_name} at timestamp {timestamp}")
        if map_data is None:
            map_data = f"{self.bucket.name}={target_bucket_name}"
        self.cont_bk_mgr.restore(
            self.backup_archive_dir, self.backup_repo_name,
            location=self.continuous_backup_location,
            temp_dir="/tmp",
            timestamp=timestamp,
            include_data=include_data,
            map_data=map_data
        )
        self.log.info("Entire bucket restore completed")

    def _create_backup_intervals(self, num_intervals=5):
        timestamps = []
        item_counts = []
        for i in range(num_intervals):
            self.log.info(f"Loading data for interval {i+1}")
            CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
            self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
            item_counts.append(self.bucket_util.get_buckets_item_count(self.cluster, self.bucket.name))
            self.sleep(self.continuous_backup_interval * 60, f"Waiting for {self.continuous_backup_interval} minutes...")
            timestamp = self.cont_bk_mgr.get_cluster_timestamp()
            timestamps.append(timestamp)
            self.log.info(f"Timestamp after interval {i+1}: {timestamp}")
        return timestamps, item_counts

    def _reset_bucket_and_restore_from_backup(self, expected_item_count):
        self.log.info("Deleting bucket for restore: %s" % self.bucket.name)
        self.bucket_util.delete_bucket(self.cluster, self.bucket)
        self.log.info("Creating bucket: %s" % self.bucket.name)
        self.bucket_util.create_bucket(self.cluster, self.bucket)
        self.log.info("Restoring from backup")
        self.backup_mgr.restore(self.backup_archive_dir, self.backup_repo_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(expected_item_count)

    def _create_and_restore_to_new_bucket(self, expected_item_count):
        self.restore_bucket_name = f"restore_bucket_{int(time.time())}"
        self.log.info("Creating new bucket for restore: %s" % self.restore_bucket_name)
        ram_quota = self.input.param("bucket_size", 100)
        self.bucket_util.create_default_bucket(self.cluster,
                                               bucket_name=self.restore_bucket_name,
                                               bucket_type=self.bucket_type,
                                               ram_quota=ram_quota,
                                               replica=self.num_replicas,
                                               storage=self.bucket_storage)
        self.log.info("Restoring from backup to new bucket")
        self.backup_mgr.restore(self.backup_archive_dir, self.backup_repo_name,
                                map_data=f"{self.bucket.name}={self.restore_bucket_name}")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(expected_item_count, bucket_name=self.restore_bucket_name)

    def _create_restore_bucket(self, restore_bucket_name):
        self.log.info("Creating new bucket for restore: %s" % restore_bucket_name)
        ram_quota = self.input.param("bucket_size", 100)
        self.bucket_util.create_default_bucket(self.cluster,
                                               bucket_name=restore_bucket_name,
                                               bucket_type=self.bucket_type,
                                               ram_quota=ram_quota,
                                               replica=self.num_replicas,
                                               storage=self.bucket_storage)

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
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        item_count = self.bucket_util.get_buckets_item_count(self.cluster, self.bucket.name)

        self.sleep(self.continuous_backup_interval * 60, f"Waiting for {self.continuous_backup_interval} minutes...")

        self._reset_bucket_and_restore_from_backup(self._get_total_docs())
        self._perform_continuous_restore()
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(item_count)
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
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        item_count = self.bucket_util.get_buckets_item_count(self.cluster, self.bucket.name)

        self.sleep(self.continuous_backup_interval * 60, f"Waiting for {self.continuous_backup_interval} minutes...")

        self._reset_bucket_and_restore_from_backup(self._get_total_docs())
        self._perform_continuous_restore(timestamp)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(item_count)
        self.log.info("Test completed successfully")

    def test_pitr_timestamp_after_second_cont_backup(self):
        """
        Point-in-time recovery with a specific timestamp after the second continuous backup.
        """
        self._verify_continuous_backup_params()
        CollectionBase.load_data_from_spec_file(self, self.data_spec_name)

        self.sleep(self.continuous_backup_interval * 60, f"Waiting for {self.continuous_backup_interval} minutes...")

        CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)

        self.sleep(self.continuous_backup_interval * 60, f"Waiting for {self.continuous_backup_interval} minutes...")

        item_count = self.bucket_util.get_buckets_item_count(self.cluster, self.bucket.name)
        timestamp = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"Timestamp after third load: {timestamp}")

        self._reset_bucket_and_restore_from_backup(self._get_total_docs())
        self._perform_continuous_restore(timestamp)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(item_count)
        self.log.info("Test completed successfully")

    def test_pitr_five_sequential_restores(self):
        """
        Takes 5 continuous backups, then restores them sequentially,
        verifying the doc count at each step.
        """
        self._verify_continuous_backup_params()
        timestamps, item_counts = self._create_backup_intervals()

        self._reset_bucket_and_restore_from_backup(self._get_total_docs())

        for i, timestamp in enumerate(timestamps):
            self._perform_continuous_restore(timestamp)
            self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
            self._verify_doc_count(item_counts[i])

        self.log.info("Test completed successfully")

    def test_pitr_five_haphazard_restores(self):
        """
        Takes 5 continuous backups, then restores them haphazardly.
        The final doc count should match the latest restore point.
        """
        self._verify_continuous_backup_params()
        num_intervals = 5
        timestamps, item_counts = self._create_backup_intervals(num_intervals)

        self._reset_bucket_and_restore_from_backup(self._get_total_docs())

        restore_order = random.sample(range(num_intervals), num_intervals)
        self.log.info(f"Restore order: {restore_order}")
        max_restored_interval = -1
        for i in restore_order:
            timestamp = timestamps[i]
            if i > max_restored_interval:
                max_restored_interval = i
            self._perform_continuous_restore(timestamp)
            self.log.info(f"Max restored interval is {max_restored_interval + 1}")
            self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
            self._verify_doc_count(item_counts[max_restored_interval])

        self.log.info("Final doc count check after all restores")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(item_counts[4])
        self.log.info("Test completed successfully")

    def test_pitr_restore_before_mutation(self):
        """
        Restores to a point-in-time before a mutation and verifies doc content.
        """
        self._verify_continuous_backup_params()

        # 1. Load initial data and take a backup
        doc_loading_task = self._load_data_and_get_task(self.data_spec_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        initial_item_count = self.bucket_util.get_buckets_item_count(self.cluster, self.bucket.name)
        self.log.info("Creating backup repository")
        self.backup_mgr.create_repo(self.backup_archive_dir, self.backup_repo_name)
        self.log.info("Performing initial backup")
        self.backup_mgr.backup(self.backup_archive_dir, self.backup_repo_name)

        # 2. Wait for a backup interval and capture timestamp
        self.sleep(self.continuous_backup_interval * 60, f"Waiting for {self.continuous_backup_interval} minutes...")
        timestamp_before_mutation = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"Timestamp before mutation: {timestamp_before_mutation}")

        # 3. Mutate a subset of documents
        self.log.info("Mutating 50% of documents")
        CollectionBase.mutate_history_retention_data(self, update_percent=50, update_itrs=1)
        self.sleep(self.continuous_backup_interval * 60, f"Waiting for {self.continuous_backup_interval} minutes...")

        # 4. Reset bucket and restore from backup
        self._reset_bucket_and_restore_from_backup(initial_item_count)

        # 5. Restore to the point-in-time before the mutation
        self._perform_continuous_restore(timestamp_before_mutation)

        # 6. Verify document content
        self.log.info("Verifying document content is restored to pre-mutation state")
        self.validate_cruds_from_collection_mutation(doc_loading_task)
        self.log.info("Test completed successfully")

    def test_pitr_restore_after_mutation(self):
        """
        Restores to a point-in-time after a mutation and verifies doc content.
        """
        self._verify_continuous_backup_params()

        # 1. Load initial data and take a backup
        self._load_data_and_get_task(self.data_spec_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        initial_item_count = self.bucket_util.get_buckets_item_count(self.cluster, self.bucket.name)
        self.log.info("Creating backup repository")
        self.backup_mgr.create_repo(self.backup_archive_dir, self.backup_repo_name)
        self.log.info("Performing initial backup")
        self.backup_mgr.backup(self.backup_archive_dir, self.backup_repo_name)
        self.sleep(self.continuous_backup_interval * 60, f"Waiting for {self.continuous_backup_interval} minutes...")

        # 2. Mutate a subset of documents and capture the task
        self.log.info("Mutating 50% of documents")
        doc_loading_task = CollectionBase.mutate_history_retention_data(self, update_percent=50, update_itrs=1)
        self.sleep(self.continuous_backup_interval * 60, f"Waiting for {self.continuous_backup_interval} minutes...")
        timestamp_after_mutation = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"Timestamp after mutation: {timestamp_after_mutation}")

        # 3. Reset bucket and restore from backup
        self._reset_bucket_and_restore_from_backup(initial_item_count)

        # 4. Restore to the point-in-time after the mutation
        self._perform_continuous_restore(timestamp_after_mutation)

        # 5. Verify document content
        self.log.info("Verifying document content is restored to post-mutation state")
        self.validate_cruds_from_collection_mutation(doc_loading_task)
        self.log.info("Test completed successfully")

    def test_pitr_restore_to_new_bucket_with_multiple_timestamps_1(self):
        """
        Takes 3 continuous backups, then restores to a new bucket from timestamps
        between the 1st and 2nd backups, and between the 2nd and 3rd backups.
        """
        self._verify_continuous_backup_params()
        self.restore_bucket_name = "restore_bucket_1"
        self._create_restore_bucket(self.restore_bucket_name)

        timestamps, item_counts = self._create_backup_intervals(num_intervals=3)

        # Restore between 1st and 2nd backup
        self._restore_entire_bucket(timestamps[0], self.restore_bucket_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(item_counts[0], bucket_name=self.restore_bucket_name)

        # Restore between 2nd and 3rd backup
        self._restore_entire_bucket(timestamps[1], self.restore_bucket_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(item_counts[1], bucket_name=self.restore_bucket_name)

        self.log.info("Test completed successfully")

    def test_pitr_restore_to_new_bucket_with_multiple_timestamps_2(self):
        """
        Takes 3 continuous backups, then restores to a new bucket from timestamps
        between the 2nd and 3rd backups, and after the 3rd backup.
        """
        self._verify_continuous_backup_params()
        self.restore_bucket_name = "restore_bucket_2"
        self._create_restore_bucket(self.restore_bucket_name)

        timestamps, item_counts = self._create_backup_intervals(num_intervals=3)

        # Restore between 2nd and 3rd backup
        self._restore_entire_bucket(timestamps[1], self.restore_bucket_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(item_counts[1], bucket_name=self.restore_bucket_name)

        # Restore after 3rd backup
        self._restore_entire_bucket(timestamps[2], self.restore_bucket_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(item_counts[2], bucket_name=self.restore_bucket_name)

        self.log.info("Test completed successfully")

    def test_pitr_with_incremental_merge_with_numbers(self):
        """
        Tests PITR with traditional incremental backups and merges happening in the background.
        """
        timestamps = []
        item_counts = []

        restore_bucket_name = f"restore_bucket_{int(time.time())}"
        self._create_restore_bucket(restore_bucket_name)

        # 1. First incremental backup and timestamp
        CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
        self.backup_mgr.backup(self.backup_archive_dir, self.backup_repo_name)
        self.sleep(self.continuous_backup_interval * 60)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        item_counts.append(self.bucket_util.get_buckets_item_count(self.cluster, self.bucket.name))
        timestamps.append(self.cont_bk_mgr.get_cluster_timestamp())

        # 2. Second incremental backup and timestamp
        CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
        self.backup_mgr.backup(self.backup_archive_dir, self.backup_repo_name)
        self.sleep(self.continuous_backup_interval * 60)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        item_counts.append(self.bucket_util.get_buckets_item_count(self.cluster, self.bucket.name))
        timestamps.append(self.cont_bk_mgr.get_cluster_timestamp())

        # 3. Merge the first two backups
        self.backup_mgr.merge(self.backup_archive_dir, self.backup_repo_name, start=1, end=2)

        # 4. Restore and verify using timestamps
        for i, (ts, count) in enumerate(zip(timestamps, item_counts)):
            self.cont_bk_mgr.restore(self.backup_archive_dir, self.backup_repo_name,
                                     location=self.continuous_backup_location,
                                     temp_dir="/tmp", # Added temp_dir
                                     timestamp=ts,
                                     map_data=f"{self.bucket.name}={restore_bucket_name}")
            self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
            self._verify_doc_count(count, bucket_name=restore_bucket_name)

        self.log.info("Test completed successfully")

    def test_pitr_with_merge_specific_range(self):
        """
        Tests PITR with a specific range merge happening in the background.
        """
        # 1. Create a chain of 5 backups and capture timestamps
        # Initial state before loop
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        item_counts = [self.bucket_util.get_buckets_item_count(self.cluster, self.bucket.name)]
        timestamps = []
        for i in range(5):
            CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
            if i<4:
                self.backup_mgr.backup(self.backup_archive_dir, self.backup_repo_name)
            self.sleep(self.continuous_backup_interval * 60)
            self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
            item_counts.append(self.bucket_util.get_buckets_item_count(self.cluster, self.bucket.name))
            timestamps.append(self.cont_bk_mgr.get_cluster_timestamp())

        # 2. Merge backups 3 and 4
        self.backup_mgr.merge(self.backup_archive_dir, self.backup_repo_name, start=3, end=4)

        restore_bucket_name = f"restore_bucket_{int(time.time())}"
        self._create_restore_bucket(restore_bucket_name)

        # 3. Verify restores from all valid PITR points
        for i, (ts, count) in enumerate(zip(timestamps, item_counts[1:])):
            self.cont_bk_mgr.restore(self.backup_archive_dir, self.backup_repo_name,
                                     location=self.continuous_backup_location,
                                     temp_dir="/tmp", # Added temp_dir
                                     timestamp=ts,
                                     map_data=f"{self.bucket.name}={restore_bucket_name}")
            self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
            self._verify_doc_count(count, bucket_name=restore_bucket_name)

        self.log.info("Test completed successfully")

    def test_pitr_with_sequential_merges(self):
        """
        Tests PITR with multiple, non-overlapping merges on the same repository.
        """
        # 1. Create 6 backups and capture timestamps
        item_counts = []
        timestamps = []
        for i in range(7):
            CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
            if i<6:
                self.backup_mgr.backup(self.backup_archive_dir, self.backup_repo_name, full_backup=(i == 0))
            self.sleep(self.continuous_backup_interval * 60)
            self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
            item_counts.append(self.bucket_util.get_buckets_item_count(self.cluster, self.bucket.name))
            timestamps.append(self.cont_bk_mgr.get_cluster_timestamp())

        # 2. Merge backups 1-3
        self.backup_mgr.merge(self.backup_archive_dir, self.backup_repo_name, start=1, end=3)

        # 3. Merge backups 4-6
        # When we merged 1-3, backup #1 is now the merged backup representing state 1-3. 
        # The remaining backups (#4, #5, #6) are now shifted in the sequence to #2, #3, #4.
        # So we merge indices 2-4 instead of 4-6
        self.backup_mgr.merge(self.backup_archive_dir, self.backup_repo_name, start=2, end=4)

        restore_bucket_name = f"restore_bucket_{int(time.time())}"
        self._create_restore_bucket(restore_bucket_name)

        # 4. Verify restores from various timestamps
        for i in [2, 5]:  # Restore from after 3rd and 6th backups
            ts = timestamps[i]
            count = item_counts[i]
            self.cont_bk_mgr.restore(self.backup_archive_dir, self.backup_repo_name,
                                     location=self.continuous_backup_location,
                                     temp_dir="/tmp", # Added temp_dir
                                     timestamp=ts,
                                     map_data=f"{self.bucket.name}={restore_bucket_name}")
            self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
            self._verify_doc_count(count, bucket_name=restore_bucket_name)

        self.log.info("Test completed successfully")

    def test_pitr_with_merge_containing_full_backup(self):
        """
        Tests PITR with merging a range that contains a full backup in the middle.
        """
        # 1. Create backups with a full backup in the middle and capture timestamps
        item_counts = []
        timestamps = []
        for i in range(5):
            CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
            if i<4:
                self.backup_mgr.backup(self.backup_archive_dir, self.backup_repo_name, full_backup=(i == 0 or i == 2))
            self.sleep(self.continuous_backup_interval * 60)
            self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
            item_counts.append(self.bucket_util.get_buckets_item_count(self.cluster, self.bucket.name))
            timestamps.append(self.cont_bk_mgr.get_cluster_timestamp())

        # 2. Merge the entire range
        self.backup_mgr.merge(self.backup_archive_dir, self.backup_repo_name, start=1, end=4)

        # 3. Verify restore from the final timestamp
        ts = timestamps[3]
        count = item_counts[3]
        restore_bucket_name = "restore_bucket_full_merge"
        self._create_restore_bucket(restore_bucket_name)
        self.cont_bk_mgr.restore(self.backup_archive_dir, self.backup_repo_name,
                                 location=self.continuous_backup_location,
                                 temp_dir="/tmp", # Added temp_dir
                                 timestamp=ts,
                                 map_data=f"{self.bucket.name}={restore_bucket_name}")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count, bucket_name=restore_bucket_name)

        self.log.info("Test completed successfully")

    def test_pitr_sequential_restore_after_merge(self):
        """
        Verifies sequential PITR restores after a merge operation.
        """
        item_counts, timestamps = [], []
        for i in range(4):
            CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
            if i<3:
                self.backup_mgr.backup(self.backup_archive_dir, self.backup_repo_name, full_backup=(i == 0))
            self.sleep(self.continuous_backup_interval * 60)
            self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
            item_counts.append(self.bucket_util.get_buckets_item_count(self.cluster, self.bucket.name))
            timestamps.append(self.cont_bk_mgr.get_cluster_timestamp())

        self.backup_mgr.merge(self.backup_archive_dir, self.backup_repo_name, start=1, end=2)

        restore_bucket_name = f"restore_bucket_{int(time.time())}"
        self._create_restore_bucket(restore_bucket_name)

        for i, (ts, count) in enumerate(zip(timestamps, item_counts)):
            self.cont_bk_mgr.restore(self.backup_archive_dir, self.backup_repo_name,
                                     location=self.continuous_backup_location,
                                     temp_dir="/tmp", # Added temp_dir
                                     timestamp=ts,
                                     map_data=f"{self.bucket.name}={restore_bucket_name}")
            self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
            self._verify_doc_count(count, bucket_name=restore_bucket_name)

        self.log.info("Test completed successfully")

    def test_pitr_haphazard_restore_after_merge(self):
        """
        Verifies haphazard PITR restores after a merge operation.
        """
        item_counts, timestamps = [], []
        for i in range(4):
            CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
            if i<3:
                self.backup_mgr.backup(self.backup_archive_dir, self.backup_repo_name, full_backup=(i == 0))
            self.sleep(self.continuous_backup_interval * 60)
            self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
            item_counts.append(self.bucket_util.get_buckets_item_count(self.cluster, self.bucket.name))
            timestamps.append(self.cont_bk_mgr.get_cluster_timestamp())

        self.backup_mgr.merge(self.backup_archive_dir, self.backup_repo_name, start=1, end=2)

        restore_bucket_name = f"restore_bucket_{int(time.time())}"
        self._create_restore_bucket(restore_bucket_name)

        restore_order = list(range(len(timestamps)))
        random.shuffle(restore_order)
        self.log.info(f"Restore order: {restore_order}")
        max_restored_interval = -1
        for i in restore_order:
            ts = timestamps[i]
            if i > max_restored_interval:
                max_restored_interval = i
            self.cont_bk_mgr.restore(self.backup_archive_dir, self.backup_repo_name,
                                     location=self.continuous_backup_location,
                                     temp_dir="/tmp", # Added temp_dir
                                     timestamp=ts,
                                     map_data=f"{self.bucket.name}={restore_bucket_name}")
            self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
            self._verify_doc_count(item_counts[max_restored_interval], bucket_name=restore_bucket_name)

        self.log.info("Test completed successfully")

    def test_pitr_with_remove_range(self):
        """
        Tests PITR functionality after removing a specific range of backups.
        """
        item_counts, timestamps = [], []
        
        # Take 5 backups
        for i in range(5):
            CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
            self.backup_mgr.backup(self.backup_archive_dir, self.backup_repo_name, full_backup=(i == 0))
            self.sleep(self.continuous_backup_interval * 60)
            self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
            item_counts.append(self.bucket_util.get_buckets_item_count(self.cluster, self.bucket.name))
            timestamps.append(self.cont_bk_mgr.get_cluster_timestamp())

        # Remove backups 2 and 3
        self.backup_mgr.remove(self.backup_archive_dir, self.backup_repo_name, backup_range="2-3")

        restore_bucket_name = f"restore_bucket_{int(time.time())}"
        self._create_restore_bucket(restore_bucket_name)

        # Restore from timestamp after the 4th backup (index 3) - this should still work
        # Since backups 2 and 3 were removed, but PITR relies on continuous logs,
        # we check if continuous restore can still rebuild from the start up to timestamp 4.
        ts_valid = timestamps[3]
        count_valid = item_counts[3]
        
        self.cont_bk_mgr.restore(self.backup_archive_dir, self.backup_repo_name,
                                 location=self.continuous_backup_location,
                                 temp_dir="/tmp",
                                 timestamp=ts_valid,
                                 map_data=f"{self.bucket.name}={restore_bucket_name}")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_valid, bucket_name=restore_bucket_name)

        self.log.info("Test completed successfully")

    def test_pitr_with_ttl_and_deletions(self):
        """
        Tests PITR restores documents correctly when some documents are deleted or expired (TTL).
        """
        # Load initial data
        CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
        
        # Get count before deletions
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        initial_count = self.bucket_util.get_buckets_item_count(self.cluster, self.bucket.name)
        self.sleep(self.continuous_backup_interval * 60)
        ts_before_delete = self.cont_bk_mgr.get_cluster_timestamp()

        # Update spec for deletions
        delete_spec = self.bucket_util.get_crud_template_from_package(self.data_spec_name)
        delete_spec[MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION] = 20  # Delete 20%
        delete_spec[MetaCrudParams.DOC_TTL] = 20  # Expire 20% (TTL) using a 20s TTL for touch/update operations
        delete_spec[MetaCrudParams.DocCrud.TOUCH_PERCENTAGE_PER_COLLECTION] = 20 
        delete_spec[MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 0
        delete_spec[MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 0
        CollectionBase.over_ride_doc_loading_template_params(self, delete_spec)
        
        self.bucket_util.run_scenario_from_spec(
            self.task, self.cluster, self.cluster.buckets, delete_spec, mutation_num=1,
            batch_size=self.batch_size, process_concurrency=self.process_concurrency)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        
        # Wait for TTL to expire and for continuous backup to capture changes
        self.sleep(max(60, 2 * self.continuous_backup_interval * 60))
        self.bucket_util._expiry_pager(self.cluster, val=1)
        self.sleep(30, "Wait for items to get purged")
        
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        final_count = self.bucket_util.get_buckets_item_count(self.cluster, self.bucket.name)
        ts_after_delete = self.cont_bk_mgr.get_cluster_timestamp()

        restore_bucket_name = f"restore_bucket_{int(time.time())}"
        self._create_restore_bucket(restore_bucket_name)

        # 1. Restore to before deletions -> Should have initial count
        self.cont_bk_mgr.restore(self.backup_archive_dir, self.backup_repo_name,
                                 location=self.continuous_backup_location,
                                 temp_dir="/tmp",
                                 timestamp=ts_before_delete,
                                 map_data=f"{self.bucket.name}={restore_bucket_name}")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(initial_count, bucket_name=restore_bucket_name)
        
        # Delete restore bucket and recreate for next test
        restore_bucket_obj = self.bucket_util.get_bucket_obj(self.cluster.buckets, restore_bucket_name)
        if restore_bucket_obj:
            self.bucket_util.delete_bucket(self.cluster, restore_bucket_obj)
        self._create_restore_bucket(restore_bucket_name)

        # 2. Restore to after deletions -> Should have final count
        self.cont_bk_mgr.restore(self.backup_archive_dir, self.backup_repo_name,
                                 location=self.continuous_backup_location,
                                 temp_dir="/tmp",
                                 timestamp=ts_after_delete,
                                 map_data=f"{self.bucket.name}={restore_bucket_name}")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(final_count, bucket_name=restore_bucket_name)

        self.log.info("Test completed successfully")

    def test_pitr_concurrent_backup_and_restore(self):
        """
        Attempt a PITR restore while a cbbackupmgr backup operation is actively running.
        """
        CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
        self.backup_mgr.backup(self.backup_archive_dir, self.backup_repo_name)
        self.sleep(self.continuous_backup_interval * 60)
        
        ts_valid = self.cont_bk_mgr.get_cluster_timestamp()
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        count_valid = self.bucket_util.get_buckets_item_count(self.cluster, self.bucket.name)

        restore_bucket_name = f"restore_bucket_{int(time.time())}"
        self._create_restore_bucket(restore_bucket_name)

        # Start a heavy load to make the backup take some time
        load_spec = self.bucket_util.get_crud_template_from_package(self.data_spec_name)
        load_spec[MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 50
        CollectionBase.over_ride_doc_loading_template_params(self, load_spec)
        self.bucket_util.run_scenario_from_spec(
            self.task, self.cluster, self.cluster.buckets, load_spec, mutation_num=1,
            batch_size=self.batch_size, process_concurrency=self.process_concurrency)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)

        # Execute backup and restore concurrently
        with ThreadPoolExecutor(max_workers=2) as executor:
            backup_future = executor.submit(self.backup_mgr.backup, self.backup_archive_dir, self.backup_repo_name)
            
            # small sleep to ensure backup has started
            self.sleep(2) 
            
            restore_future = executor.submit(
                self.cont_bk_mgr.restore, self.backup_archive_dir, self.backup_repo_name,
                location=self.continuous_backup_location, temp_dir="/tmp",
                timestamp=ts_valid, map_data=f"{self.bucket.name}={restore_bucket_name}"
            )
            
            # Wait for both to finish
            backup_future.result()
            restore_future.result()

        # Verify restore was successful
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_valid, bucket_name=restore_bucket_name)

        self.log.info("Test completed successfully")

    def test_pitr_bucket_flush(self):
        """
        Test PITR when bucket is flushed during continuous backup.
        """
        # Load initial data
        CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        initial_count = self.bucket_util.get_buckets_item_count(self.cluster, self.bucket.name)
        self.sleep(self.continuous_backup_interval * 60)
        ts_before_flush = self.cont_bk_mgr.get_cluster_timestamp()

        # Flush bucket
        self.log.info(f"Flushing bucket: {self.bucket.name}")
        self.bucket_util.flush_bucket(self.cluster, self.bucket)
        
        # Wait for continuous backup to capture flush
        self.sleep(self.continuous_backup_interval * 60)
        ts_after_flush = self.cont_bk_mgr.get_cluster_timestamp()
        
        # Load some new data
        CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
        self.sleep(self.continuous_backup_interval * 60)
        ts_after_new_load = self.cont_bk_mgr.get_cluster_timestamp()
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        final_count = self.bucket_util.get_buckets_item_count(self.cluster, self.bucket.name)

        restore_bucket_name = f"restore_bucket_{int(time.time())}"
        self._create_restore_bucket(restore_bucket_name)

        # 1. Restore to before flush -> Should have initial count
        self.cont_bk_mgr.restore(self.backup_archive_dir, self.backup_repo_name,
                                 location=self.continuous_backup_location,
                                 temp_dir="/tmp", timestamp=ts_before_flush,
                                 map_data=f"{self.bucket.name}={restore_bucket_name}")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(initial_count, bucket_name=restore_bucket_name)
        
        # Delete and recreate restore bucket
        restore_bucket_obj = self.bucket_util.get_bucket_obj(self.cluster.buckets, restore_bucket_name)
        if restore_bucket_obj:
            self.bucket_util.delete_bucket(self.cluster, restore_bucket_obj)
        self._create_restore_bucket(restore_bucket_name)

        # 2. Restore to after flush -> Should have 0 documents
        self.cont_bk_mgr.restore(self.backup_archive_dir, self.backup_repo_name,
                                 location=self.continuous_backup_location,
                                 temp_dir="/tmp", timestamp=ts_after_flush,
                                 map_data=f"{self.bucket.name}={restore_bucket_name}")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(0, bucket_name=restore_bucket_name)
        
        # Delete and recreate restore bucket
        restore_bucket_obj = self.bucket_util.get_bucket_obj(self.cluster.buckets, restore_bucket_name)
        if restore_bucket_obj:
            self.bucket_util.delete_bucket(self.cluster, restore_bucket_obj)
        self._create_restore_bucket(restore_bucket_name)

        # 3. Restore to after new load -> Should have final count
        self.cont_bk_mgr.restore(self.backup_archive_dir, self.backup_repo_name,
                                 location=self.continuous_backup_location,
                                 temp_dir="/tmp", timestamp=ts_after_new_load,
                                 map_data=f"{self.bucket.name}={restore_bucket_name}")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(final_count, bucket_name=restore_bucket_name)

        self.log.info("Test completed successfully")

    def test_pitr_with_filtering(self):
        """
        Test PITR with scope/collection filtering using include_data and map_data during restore.
        """
        # Load data with multiple scopes/collections from spec
        CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        
        # Get total document count across all scopes/collections
        total_count = self.bucket_util.get_buckets_item_count(self.cluster, self.bucket.name)
        self.log.info(f"Total document count before filtering: {total_count}")
        
        # Get scope and collection information from the bucket
        num_scopes = self.spec.get(MetaConstants.NUM_SCOPES_PER_BUCKET, 1)
        num_collections = self.spec.get(MetaConstants.NUM_COLLECTIONS_PER_SCOPE, 1)
        num_items = self.spec.get(MetaConstants.NUM_ITEMS_PER_COLLECTION, 0)
        expected_scope_count = num_scopes * num_collections * num_items
        self.log.info(f"Expected per-scope count (if evenly distributed): {expected_scope_count}")
        
        self.sleep(self.continuous_backup_interval * 60)
        ts_valid = self.cont_bk_mgr.get_cluster_timestamp()

        # Get first scope and first collection name from the bucket
        bucket_obj = self.cluster.buckets[0]
        if bucket_obj.scopes and bucket_obj.scopes[list(bucket_obj.scopes.keys())[0]].collections:
            first_scope = list(bucket_obj.scopes.keys())[0]
            first_collection = list(bucket_obj.scopes[first_scope].collections.keys())[0]
            
            # Define test cases for filtering
            test_cases = [
                {
                    "name": "include_data only",
                    "include_data": f"{self.bucket.name}.{first_scope}.{first_collection}",
                    "map_data": None
                },
                {
                    "name": "include_data with map_data",
                    "include_data": f"{self.bucket.name}.{first_scope}.{first_collection}",
                    "map_data": None  # Will be set inside loop
                }
            ]
            
            for i, test_case in enumerate(test_cases, 1):
                restore_bucket_name = f"restore_bucket_{int(time.time())}"
                self._create_restore_bucket(restore_bucket_name)

                self.log.info(f"Test {i}: {test_case['name']}")

                # For the second test, create map_data with different target collection
                if test_case["map_data"] is None and test_case["name"] == "include_data with map_data":
                    target_collection_name = "mapped_collection"
                    target_bucket_obj = self.bucket_util.get_bucket_obj(self.cluster.buckets, restore_bucket_name)
                    if target_bucket_obj:
                        self.log.info(f"Creating target scope and collection: {first_scope}.{target_collection_name}")
                    test_case["map_data"] = f"{self.bucket.name}.{first_scope}.{first_collection}={restore_bucket_name}.{first_scope}.{target_collection_name}"
                    self.log.info(f"Mapping from {self.bucket.name}.{first_scope}.{first_collection} to {restore_bucket_name}.{first_scope}.{target_collection_name}")
                elif test_case["map_data"] is None:
                    self.log.info(f"Filtering with include_data: {test_case['include_data']}")

                self._restore_entire_bucket(
                    timestamp=ts_valid,
                    target_bucket_name=restore_bucket_name,
                    include_data=test_case["include_data"],
                    map_data=test_case["map_data"]
                )

                # Verify the restore
                self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
                restored_count = self.bucket_util.get_buckets_item_count(self.cluster, restore_bucket_name)
                self.log.info(f"Restored count: {restored_count}")

                if num_items > 0:
                    self.log.info(f"Filter restored ~{restored_count} docs (expected ~{num_items})")
                    self.assertEqual(num_items, restored_count)
                else:
                    self.log.warning("Spec has 0 items per collection, cannot verify filter count")

                # Clean up restore bucket
                restore_bucket_obj = self.bucket_util.get_bucket_obj(self.cluster.buckets, restore_bucket_name)
                if restore_bucket_obj:
                    self.bucket_util.delete_bucket(self.cluster, restore_bucket_obj)
        else:
            self.log.warning("Bucket has no scopes or collections, cannot test filtering")

        self.log.info("Test completed successfully")

    def test_pitr_param_changes(self):
        """
        Tests PITR behavior when continuous backup parameters are changed.
        - Disables continuous backup and verifies restore.
        - Changes backup interval and verifies restore succeeds.
        """
        restore_bucket_name = f"restore_bucket_params_{int(time.time())}"
        self._create_restore_bucket(restore_bucket_name)

        # --- Test continuousBackupEnabled toggle ---
        self.log.info("Testing continuousBackupEnabled toggle")
        CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        count_before_disable = self.bucket_util.get_buckets_item_count(self.cluster, self.bucket.name)
        self.sleep(self.continuous_backup_interval * 60, f"Waiting for {self.continuous_backup_interval} minutes...")

        self.log.info("Disabling continuous backup")
        self.bucket_util.update_bucket_property(self.cluster.master, self.bucket, continuous_backup_enabled=False)
        self.sleep(10, "Waiting for settings to apply")

        self.log.info("Loading more data while continuous backup is disabled")
        CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
        self.sleep(self.continuous_backup_interval * 60, "Waiting for backup interval to pass (backup should not run)")

        self.log.info("Restoring to latest point in time")
        self._restore_entire_bucket(None, restore_bucket_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_before_disable, bucket_name=restore_bucket_name)
        self.log.info("Restore only contained data from before continuous backup was disabled, as expected.")

        self.log.info("Re-enabling continuous backup")
        self.bucket_util.update_bucket_property(self.cluster.master, self.bucket, continuous_backup_enabled=True,
                                                continuous_backup_location=self.continuous_backup_location)
        self.sleep(10, "Waiting for settings to apply")

        # --- Test continuousBackupInterval change ---
        self.log.info("Testing continuousBackupInterval change")
        new_interval = 3  # minutes
        self.log.info(f"Changing continuous backup interval to {new_interval} minutes")
        self.bucket_util.update_bucket_property(self.cluster.master, self.bucket, continuous_backup_interval=new_interval,
                                                continuous_backup_location=self.continuous_backup_location)
        self.continuous_backup_interval = new_interval

        CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        count_after_interval_change = self.bucket_util.get_buckets_item_count(self.cluster, self.bucket.name)
        
        self.log.info(f"Waiting for new interval of {new_interval} minutes...")
        self.sleep(new_interval * 60)
        ts_after_interval_change = self.cont_bk_mgr.get_cluster_timestamp()

        self.log.info("Restoring with new interval setting")
        self._restore_entire_bucket(ts_after_interval_change, restore_bucket_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_after_interval_change, bucket_name=restore_bucket_name)
        self.log.info("Restore with new interval succeeded as expected.")

        self.log.info("Test completed successfully")
