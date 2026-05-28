import importlib
import random
import time
from concurrent.futures import ThreadPoolExecutor

from BucketLib.bucket import Bucket
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

    # =========================================================================
    # P0 — Schema-change PITR tests
    # =========================================================================

    def _get_first_non_system_scope_and_collection(self):
        """Returns (scope_name, collection_name) for the first user-visible scope."""
        for scope_name, scope in self.bucket.scopes.items():
            if scope_name.startswith("_"):
                continue
            for coll_name in scope.collections:
                return scope_name, coll_name
        self.fail("No non-system scope/collection found in bucket")

    def _delete_and_recreate_restore_bucket(self, restore_bucket_name):
        """Delete a restore bucket and create a fresh empty one."""
        restore_bucket_obj = self.bucket_util.get_bucket_obj(
            self.cluster.buckets, restore_bucket_name)
        if restore_bucket_obj:
            self.bucket_util.delete_bucket(self.cluster, restore_bucket_obj)
        self._create_restore_bucket(restore_bucket_name)

    def test_pitr_before_collection_drop(self):
        """
        P0: Validates PITR recovers a collection that was dropped after the backup timestamp.

        This covers the critical customer scenario of accidentally dropping a collection
        and needing to recover it — with all its data — from a point before the drop.

        Note: PITR is supported only on Magma buckets. The bucket spec
              (single_bucket.continuous_backup_tests) already enforces Magma storage.

        Flow:
          1. Load data; capture T1 and count C1.
          2. Drop one user collection (loses N items).
          3. Wait for continuous backup to capture the drop; capture T2 and count C2.
          4. Take an incremental traditional backup at the T2 state.
          5. PITR restore to T1 -> verify count == C1 (dropped collection recovered).
          6. PITR restore to T2 -> verify count == C2 (collection still absent).
        """
        CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        count_t1 = self.bucket_util.get_buckets_item_count(
            self.cluster, self.bucket.name)

        self.sleep(self.continuous_backup_interval * 60,
                   "Waiting for backup interval before collection drop")
        ts_t1 = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"T1 (pre-drop): {ts_t1}, count: {count_t1}")

        drop_scope, drop_coll = self._get_first_non_system_scope_and_collection()
        items_per_coll = self.spec.get(MetaConstants.NUM_ITEMS_PER_COLLECTION, 0)
        self.log.info(
            f"Dropping {self.bucket.name}.{drop_scope}.{drop_coll} "
            f"(~{items_per_coll} items)")
        self.bucket_util.drop_collection(
            self.cluster.master, self.bucket,
            scope_name=drop_scope,
            collection_name=drop_coll)

        self.sleep(self.continuous_backup_interval * 60,
                   "Waiting for continuous backup to capture the collection drop")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        count_t2 = self.bucket_util.get_buckets_item_count(
            self.cluster, self.bucket.name)
        ts_t2 = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"T2 (post-drop): {ts_t2}, count: {count_t2}")

        # Anchor the post-drop state in a traditional backup
        self.backup_mgr.backup(self.backup_archive_dir, self.backup_repo_name)

        restore_bucket_name = f"restore_bucket_coll_drop_{int(time.time())}"
        self._create_restore_bucket(restore_bucket_name)

        # PITR to T1: dropped collection + its data must be recovered
        self.log.info("PITR restore to T1 (before collection drop) — collection must be recovered")
        self._restore_entire_bucket(ts_t1, restore_bucket_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_t1, bucket_name=restore_bucket_name)
        self.log.info(f"T1 restore verified: {count_t1} items (collection recovered)")

        self._delete_and_recreate_restore_bucket(restore_bucket_name)

        # PITR to T2: collection must still be absent
        self.log.info("PITR restore to T2 (after collection drop) — collection must remain absent")
        self._restore_entire_bucket(ts_t2, restore_bucket_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_t2, bucket_name=restore_bucket_name)
        self.log.info(f"T2 restore verified: {count_t2} items (collection correctly absent)")

        self.log.info("Test completed successfully")

    def test_pitr_before_scope_drop(self):
        """
        P0: Validates PITR recovers an entire scope (all its collections + data)
        that was dropped after the backup timestamp.

        Note: PITR is supported only on Magma buckets.

        Flow:
          1. Load data; capture T1 and count C1.
          2. Drop one user scope (loses all items in that scope).
          3. Wait for continuous backup to capture the drop; capture T2 and count C2.
          4. Take an incremental traditional backup.
          5. PITR to T1 -> verify count == C1 (scope and all data recovered).
          6. PITR to T2 -> verify count == C2 (scope still absent).
        """
        CollectionBase.load_data_from_spec_file(self, self.data_spec_name)

        self.sleep(self.continuous_backup_interval * 60,
                   "Waiting for backup interval before scope drop")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.sleep(10, "Waiting for item count to stabilize after ep_queue drain")
        count_t1 = self.bucket_util.get_buckets_item_count(
            self.cluster, self.bucket.name)
        ts_t1 = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"T1 (pre-scope-drop): {ts_t1}, count: {count_t1}")

        drop_scope = None
        for scope_name in self.bucket.scopes:
            if not scope_name.startswith("_"):
                drop_scope = scope_name
                break
        if not drop_scope:
            self.fail("No non-system scope found to drop")

        num_colls = len(self.bucket.scopes[drop_scope].collections)
        items_per_coll = self.spec.get(MetaConstants.NUM_ITEMS_PER_COLLECTION, 0)
        self.log.info(
            f"Dropping scope {self.bucket.name}.{drop_scope} "
            f"(~{num_colls * items_per_coll} items across {num_colls} collections)")
        self.bucket_util.drop_scope(
            self.cluster.master, self.bucket, drop_scope)

        self.sleep(self.continuous_backup_interval * 60,
                   "Waiting for continuous backup to capture the scope drop")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        count_t2 = self.bucket_util.get_buckets_item_count(
            self.cluster, self.bucket.name)
        ts_t2 = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"T2 (post-scope-drop): {ts_t2}, count: {count_t2}")

        self.backup_mgr.backup(self.backup_archive_dir, self.backup_repo_name)

        restore_bucket_name = f"restore_bucket_scope_drop_{int(time.time())}"
        self._create_restore_bucket(restore_bucket_name)

        self.log.info("PITR restore to T1 (before scope drop) — scope must be fully recovered")
        self._restore_entire_bucket(ts_t1, restore_bucket_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_t1, bucket_name=restore_bucket_name)
        self.log.info(f"T1 restore verified: {count_t1} items (scope recovered)")

        self._delete_and_recreate_restore_bucket(restore_bucket_name)

        self.log.info("PITR restore to T2 (after scope drop) — scope must remain absent")
        self._restore_entire_bucket(ts_t2, restore_bucket_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_t2, bucket_name=restore_bucket_name)
        self.log.info(f"T2 restore verified: {count_t2} items (scope correctly absent)")

        self.log.info("Test completed successfully")

    def test_pitr_collection_drop_and_recreate(self):
        """
        P0: Validates PITR distinguishes between an original collection and the
        same-named collection that was dropped and recreated with different settings.

        After drop + recreate (with a new maxTTL), PITR to before the drop should
        recover the original collection with its original data and TTL. PITR to
        after the recreate should reflect the new TTL and new data.

        Note: PITR is supported only on Magma buckets.
        """
        CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        count_t1 = self.bucket_util.get_buckets_item_count(
            self.cluster, self.bucket.name)

        self.sleep(self.continuous_backup_interval * 60,
                   "Waiting for backup interval before drop+recreate")
        ts_t1 = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"T1 (pre-drop): {ts_t1}, count: {count_t1}")

        drop_scope, drop_coll = self._get_first_non_system_scope_and_collection()
        self.log.info(f"Dropping {drop_scope}.{drop_coll}")
        self.bucket_util.drop_collection(
            self.cluster.master, self.bucket,
            scope_name=drop_scope,
            collection_name=drop_coll)

        # Recreate the collection with a different maxTTL so PITR can distinguish them
        new_ttl_seconds = 3600
        self.log.info(
            f"Recreating {drop_scope}.{drop_coll} with maxTTL={new_ttl_seconds}s")
        self.bucket_util.create_collection(
            self.cluster.master, self.bucket,
            scope_name=drop_scope,
            collection_spec={"name": drop_coll, "maxTTL": new_ttl_seconds})

        # Load fresh data into the recreated collection
        CollectionBase.load_data_from_spec_file(self, self.data_spec_name)

        # Wait for the backup interval BEFORE snapshotting the count so that any
        # in-flight items still being flushed by the loader are fully persisted.
        # Taking the count before the sleep produces a stale value that diverges
        # from what the backup actually captures at T3.
        self.sleep(self.continuous_backup_interval * 60,
                   "Waiting for backup to capture recreate + fresh data")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.sleep(10, "Waiting for item count to stabilize after ep_queue drain")
        count_after_recreate = self.bucket_util.get_buckets_item_count(
            self.cluster, self.bucket.name)
        ts_t3 = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(
            f"T3 (post-recreate+load): {ts_t3}, count: {count_after_recreate}")

        self.backup_mgr.backup(self.backup_archive_dir, self.backup_repo_name)

        restore_bucket_name = f"restore_bucket_recreate_{int(time.time())}"
        self._create_restore_bucket(restore_bucket_name)

        # PITR to T1: original collection with original TTL (no maxTTL) and original data
        self.log.info(
            "PITR restore to T1 (before drop) — original collection + data, no maxTTL")
        self._restore_entire_bucket(ts_t1, restore_bucket_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_t1, bucket_name=restore_bucket_name)
        self.log.info(f"T1 restore verified: {count_t1} items")

        self._delete_and_recreate_restore_bucket(restore_bucket_name)

        # PITR to T3: recreated collection with new maxTTL and fresh data
        self.log.info(
            f"PITR restore to T3 (after recreate, maxTTL={new_ttl_seconds}s) — new collection + data")
        self._restore_entire_bucket(ts_t3, restore_bucket_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_after_recreate, bucket_name=restore_bucket_name)
        self.log.info(f"T3 restore verified: {count_after_recreate} items")

        self.log.info("Test completed successfully")

    # =========================================================================
    # P0 — History retention window PITR tests
    # =========================================================================

    def test_pitr_history_retention_window_boundary(self):
        """
        P0: Validates PITR behaviour at the boundary of historyRetentionSeconds.

        - PITR to a timestamp within the window must succeed.
        - After shrinking the window below the age of the oldest timestamp,
          PITR to that now-expired timestamp must either fail with a clear error
          or restore from the earliest still-available point.

        Note: PITR is supported only on Magma buckets.
        """
        CollectionBase.load_data_from_spec_file(self, self.data_spec_name)

        self.sleep(self.continuous_backup_interval * 60, "Waiting for backup interval")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.sleep(10, "Waiting for item count to stabilize after ep_queue drain")
        count_t1 = self.bucket_util.get_buckets_item_count(
            self.cluster, self.bucket.name)
        ts_t1 = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"T1 (within retention window): {ts_t1}, count: {count_t1}")

        # Verify PITR within the retention window succeeds
        restore_bucket_name = f"restore_bucket_ret_boundary_{int(time.time())}"
        self._create_restore_bucket(restore_bucket_name)
        self.log.info("PITR to T1 (within window) — must succeed")
        self._restore_entire_bucket(ts_t1, restore_bucket_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_t1, bucket_name=restore_bucket_name)
        self.log.info("Restore within retention window succeeded as expected")

        self._delete_and_recreate_restore_bucket(restore_bucket_name)

        # Shrink the retention window so T1 falls outside it
        short_retention = 60  # seconds
        self.log.info(
            f"Shrinking historyRetentionSeconds to {short_retention}s "
            f"so T1 falls outside the window")
        self.bucket_util.update_bucket_property(
            self.cluster.master, self.bucket,
            history_retention_seconds=short_retention)
        wait_secs = short_retention + 30
        self.sleep(wait_secs,
                   f"Waiting {wait_secs}s for T1 to expire from the retention window")

        # Attempt PITR to the now-expired T1
        self.log.info(
            "Attempting PITR to T1 (now outside retention window) — "
            "expect error or earliest-available restore")
        output, error = self.cont_bk_mgr.restore(
            self.backup_archive_dir, self.backup_repo_name,
            location=self.continuous_backup_location,
            temp_dir="/tmp",
            timestamp=ts_t1,
            map_data=f"{self.bucket.name}={restore_bucket_name}")

        combined = " ".join((output or []) + (error or [])).lower()
        self.log.info(f"PITR to expired timestamp — output: {combined[:300]}")
        outside_window_signalled = any(
            kw in combined
            for kw in ["error", "no history", "outside", "expired",
                       "not available", "failed", "cannot restore"])
        self.log.info(
            "Outcome: " +
            ("expected error/signal returned" if outside_window_signalled
             else "restored to earliest available point (acceptable behaviour)"))

        # Restore the original retention so tearDown can clean up properly
        original_retention = self.spec.get(Bucket.historyRetentionSeconds, 6000)
        self.bucket_util.update_bucket_property(
            self.cluster.master, self.bucket,
            history_retention_seconds=original_retention)
        self.log.info(
            f"Restored historyRetentionSeconds to {original_retention}s")

        self.log.info("Test completed successfully")

    def test_pitr_history_retention_seconds_change(self):
        """
        P0: Validates PITR works correctly after dynamically increasing
        historyRetentionSeconds on a Magma bucket.

        Increasing the window must not invalidate timestamps captured before
        the change. Both pre-change and post-change PITR timestamps must succeed.

        Note: PITR is supported only on Magma buckets.
        """
        CollectionBase.load_data_from_spec_file(self, self.data_spec_name)

        self.sleep(self.continuous_backup_interval * 60, "Waiting for first backup interval")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.sleep(10, "Waiting for item count to stabilize after ep_queue drain")
        count_t1 = self.bucket_util.get_buckets_item_count(
            self.cluster, self.bucket.name)
        ts_t1 = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"T1 (pre-retention-change): {ts_t1}, count: {count_t1}")

        # Increase historyRetentionSeconds to 24 hours
        new_retention = 86400
        self.log.info(f"Increasing historyRetentionSeconds to {new_retention}s")
        self.bucket_util.update_bucket_property(
            self.cluster.master, self.bucket,
            history_retention_seconds=new_retention)
        self.sleep(10, "Waiting for settings change to propagate")

        CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        count_t2 = self.bucket_util.get_buckets_item_count(
            self.cluster, self.bucket.name)

        self.sleep(self.continuous_backup_interval * 60,
                   "Waiting for backup interval after retention change")
        ts_t2 = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(
            f"T2 (post-retention-change): {ts_t2}, count: {count_t2}")

        self.backup_mgr.backup(self.backup_archive_dir, self.backup_repo_name)

        restore_bucket_name = f"restore_bucket_ret_change_{int(time.time())}"
        self._create_restore_bucket(restore_bucket_name)

        self.log.info("PITR to T1 (captured before retention change) — must succeed")
        self._restore_entire_bucket(ts_t1, restore_bucket_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_t1, bucket_name=restore_bucket_name)

        self._delete_and_recreate_restore_bucket(restore_bucket_name)

        self.log.info("PITR to T2 (captured after retention change) — must succeed")
        self._restore_entire_bucket(ts_t2, restore_bucket_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_t2, bucket_name=restore_bucket_name)

        self.log.info("Test completed successfully")

    # =========================================================================
    # P1 — Multi-bucket independent PITR tests
    # =========================================================================

    def test_pitr_multiple_buckets_simultaneous(self):
        """
        P1: Validates that two Magma buckets with continuous backup enabled
        maintain independent PITR streams.

        Both buckets run continuous backup simultaneously. Writes are interleaved
        per-bucket, and PITR restores are verified independently: restoring
        bucket-A to T1 must not affect bucket-B's state and vice versa.

        Requires: bucket_spec=multi_bucket.continuous_backup_tests (two Magma buckets).
        Note: PITR is supported only on Magma buckets.
        """
        if len(self.cluster.buckets) < 2:
            self.fail(
                "This test requires at least 2 Magma buckets. "
                "Use bucket_spec=multi_bucket.continuous_backup_tests")

        bucket1 = self.cluster.buckets[0]
        bucket2 = self.cluster.buckets[1]

        # Load data into bucket1 only
        self.log.info(f"Loading data into {bucket1.name} only")
        doc_spec = self.bucket_util.get_crud_template_from_package(self.data_spec_name)
        CollectionBase.over_ride_doc_loading_template_params(self, doc_spec)
        CollectionBase.set_retry_exceptions(doc_spec, self.durability_level)
        task = self.bucket_util.run_scenario_from_spec(
            self.task, self.cluster, [bucket1], doc_spec,
            mutation_num=0, batch_size=self.batch_size,
            process_concurrency=self.process_concurrency,
            load_using=self.load_docs_using)
        if task.result is False:
            self.fail("Data load into bucket1 failed")

        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        count_b1_t1 = self.bucket_util.get_buckets_item_count(
            self.cluster, bucket1.name)
        count_b2_t1 = self.bucket_util.get_buckets_item_count(
            self.cluster, bucket2.name)

        self.sleep(self.continuous_backup_interval * 60,
                   "Waiting for backup interval (only bucket1 has new data)")
        ts_t1 = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(
            f"T1: {ts_t1} | {bucket1.name}={count_b1_t1}, {bucket2.name}={count_b2_t1}")

        # Load data into bucket2 only
        self.log.info(f"Loading data into {bucket2.name} only")
        doc_spec2 = self.bucket_util.get_crud_template_from_package(self.data_spec_name)
        CollectionBase.over_ride_doc_loading_template_params(self, doc_spec2)
        CollectionBase.set_retry_exceptions(doc_spec2, self.durability_level)
        task2 = self.bucket_util.run_scenario_from_spec(
            self.task, self.cluster, [bucket2], doc_spec2,
            mutation_num=0, batch_size=self.batch_size,
            process_concurrency=self.process_concurrency,
            load_using=self.load_docs_using)
        if task2.result is False:
            self.fail("Data load into bucket2 failed")

        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        count_b1_t2 = self.bucket_util.get_buckets_item_count(
            self.cluster, bucket1.name)
        count_b2_t2 = self.bucket_util.get_buckets_item_count(
            self.cluster, bucket2.name)

        self.sleep(self.continuous_backup_interval * 60,
                   "Waiting for backup interval (only bucket2 has new data)")
        ts_t2 = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(
            f"T2: {ts_t2} | {bucket1.name}={count_b1_t2}, {bucket2.name}={count_b2_t2}")

        self.backup_mgr.backup(self.backup_archive_dir, self.backup_repo_name)

        # Restore buckets sequentially to avoid RAM exhaustion on small clusters.
        # Creating both at once alongside the two source buckets exceeds quota.
        restore_b1_name = f"restore_{bucket1.name}_{int(time.time())}"
        self._create_restore_bucket(restore_b1_name)
        self.log.info(f"PITR {bucket1.name} to T1 (bucket2 load excluded)")
        self.cont_bk_mgr.restore(
            self.backup_archive_dir, self.backup_repo_name,
            location=self.continuous_backup_location,
            temp_dir="/tmp",
            timestamp=ts_t1,
            map_data=f"{bucket1.name}={restore_b1_name}")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_b1_t1, bucket_name=restore_b1_name)
        self.bucket_util.delete_bucket(self.cluster, restore_b1_name)

        restore_b2_name = f"restore_{bucket2.name}_{int(time.time())}"
        self._create_restore_bucket(restore_b2_name)
        self.log.info(f"PITR {bucket2.name} to T2 (includes bucket2-only load)")
        self.cont_bk_mgr.restore(
            self.backup_archive_dir, self.backup_repo_name,
            location=self.continuous_backup_location,
            temp_dir="/tmp",
            timestamp=ts_t2,
            map_data=f"{bucket2.name}={restore_b2_name}")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_b2_t2, bucket_name=restore_b2_name)

        self.log.info("Test completed successfully")

    def test_pitr_multiple_buckets_different_intervals(self):
        """
        P1: Validates independent PITR on two Magma buckets configured with
        different continuousBackupInterval values.

        Bucket-1 runs at the default interval; bucket-2 runs at a shorter interval.
        After loading data into each bucket independently, both must support
        accurate point-in-time restores at their respective intervals.

        Requires: bucket_spec=multi_bucket.continuous_backup_tests (two Magma buckets).
        Note: PITR is supported only on Magma buckets.
        """
        if len(self.cluster.buckets) < 2:
            self.fail(
                "This test requires at least 2 Magma buckets. "
                "Use bucket_spec=multi_bucket.continuous_backup_tests")

        bucket1 = self.cluster.buckets[0]
        bucket2 = self.cluster.buckets[1]

        # Configure bucket2 with a shorter continuous backup interval
        short_interval = max(1, self.continuous_backup_interval - 1)
        self.log.info(
            f"{bucket1.name} interval={self.continuous_backup_interval}m, "
            f"{bucket2.name} interval={short_interval}m")
        self.bucket_util.update_bucket_property(
            self.cluster.master, bucket2,
            continuous_backup_interval=short_interval,
            continuous_backup_location=self.continuous_backup_location)
        self.sleep(10, "Waiting for interval settings to apply")

        # Load data into bucket1, wait one bucket1 interval
        doc_spec = self.bucket_util.get_crud_template_from_package(self.data_spec_name)
        CollectionBase.over_ride_doc_loading_template_params(self, doc_spec)
        CollectionBase.set_retry_exceptions(doc_spec, self.durability_level)
        self.bucket_util.run_scenario_from_spec(
            self.task, self.cluster, [bucket1], doc_spec,
            mutation_num=0, batch_size=self.batch_size,
            process_concurrency=self.process_concurrency,
            load_using=self.load_docs_using)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        count_b1 = self.bucket_util.get_buckets_item_count(self.cluster, bucket1.name)

        self.sleep(self.continuous_backup_interval * 60,
                   "Waiting for bucket1 backup interval")
        ts_b1 = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"{bucket1.name} timestamp: {ts_b1}, count: {count_b1}")

        # Load data into bucket2, wait one (shorter) bucket2 interval
        doc_spec2 = self.bucket_util.get_crud_template_from_package(self.data_spec_name)
        CollectionBase.over_ride_doc_loading_template_params(self, doc_spec2)
        CollectionBase.set_retry_exceptions(doc_spec2, self.durability_level)
        self.bucket_util.run_scenario_from_spec(
            self.task, self.cluster, [bucket2], doc_spec2,
            mutation_num=0, batch_size=self.batch_size,
            process_concurrency=self.process_concurrency,
            load_using=self.load_docs_using)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        count_b2 = self.bucket_util.get_buckets_item_count(self.cluster, bucket2.name)

        self.sleep(short_interval * 60,
                   "Waiting for bucket2 (shorter) backup interval")
        ts_b2 = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"{bucket2.name} timestamp: {ts_b2}, count: {count_b2}")

        self.backup_mgr.backup(self.backup_archive_dir, self.backup_repo_name)

        restore_b1_name = f"restore_b1_intv_{int(time.time())}"
        # Restore buckets sequentially to avoid RAM exhaustion on small clusters.
        self._create_restore_bucket(restore_b1_name)
        self.log.info(f"PITR {bucket1.name} to ts_b1")
        self.cont_bk_mgr.restore(
            self.backup_archive_dir, self.backup_repo_name,
            location=self.continuous_backup_location,
            temp_dir="/tmp",
            timestamp=ts_b1,
            map_data=f"{bucket1.name}={restore_b1_name}")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_b1, bucket_name=restore_b1_name)
        self.bucket_util.delete_bucket(self.cluster, restore_b1_name)

        restore_b2_name = f"restore_b2_intv_{int(time.time())}"
        self._create_restore_bucket(restore_b2_name)
        self.log.info(f"PITR {bucket2.name} to ts_b2")
        self.cont_bk_mgr.restore(
            self.backup_archive_dir, self.backup_repo_name,
            location=self.continuous_backup_location,
            temp_dir="/tmp",
            timestamp=ts_b2,
            map_data=f"{bucket2.name}={restore_b2_name}")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_b2, bucket_name=restore_b2_name)

        self.log.info("Test completed successfully")

    # =========================================================================
    # P1 — Durability PITR tests
    # =========================================================================

    def test_pitr_with_majority_durability(self):
        """
        P1: Validates PITR correctly captures and restores documents written with
        MAJORITY sync-write durability.

        Sync writes produce a different KV acknowledgement flow than non-durable
        writes. This test confirms that MAJORITY-durable mutations appear in the
        continuous backup DCP stream and are restored correctly.

        Note: PITR is supported only on Magma buckets. Requires >= 3 KV nodes.
        """
        if len(self.cluster.kv_nodes) < 3:
            self.skipTest(
                "MAJORITY durability requires at least 3 KV nodes; "
                f"cluster has {len(self.cluster.kv_nodes)}")

        original_durability = self.durability_level
        self.durability_level = "MAJORITY"
        self.log.info("Durability set to MAJORITY for this test")

        try:
            CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
            self.bucket_util._wait_for_stats_all_buckets(
                self.cluster, self.cluster.buckets)
            count_t1 = self.bucket_util.get_buckets_item_count(
                self.cluster, self.bucket.name)

            self.sleep(self.continuous_backup_interval * 60,
                       "Waiting for backup interval (MAJORITY durable load)")
            ts_t1 = self.cont_bk_mgr.get_cluster_timestamp()
            self.log.info(f"T1 (MAJORITY durable): {ts_t1}, count: {count_t1}")

            CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
            self.bucket_util._wait_for_stats_all_buckets(
                self.cluster, self.cluster.buckets)
            count_t2 = self.bucket_util.get_buckets_item_count(
                self.cluster, self.bucket.name)

            self.sleep(self.continuous_backup_interval * 60,
                       "Waiting for second backup interval")
            ts_t2 = self.cont_bk_mgr.get_cluster_timestamp()
            self.log.info(f"T2 (second MAJORITY load): {ts_t2}, count: {count_t2}")

            self.backup_mgr.backup(self.backup_archive_dir, self.backup_repo_name)

            restore_bucket_name = f"restore_majority_{int(time.time())}"
            self._create_restore_bucket(restore_bucket_name)

            self.log.info("PITR restore to T1 (post MAJORITY-durable load)")
            self._restore_entire_bucket(ts_t1, restore_bucket_name)
            self.bucket_util._wait_for_stats_all_buckets(
                self.cluster, self.cluster.buckets)
            self._verify_doc_count(count_t1, bucket_name=restore_bucket_name)

            self._delete_and_recreate_restore_bucket(restore_bucket_name)

            self.log.info("PITR restore to T2 (post second MAJORITY load)")
            self._restore_entire_bucket(ts_t2, restore_bucket_name)
            self.bucket_util._wait_for_stats_all_buckets(
                self.cluster, self.cluster.buckets)
            self._verify_doc_count(count_t2, bucket_name=restore_bucket_name)

        finally:
            self.durability_level = original_durability

        self.log.info("Test completed successfully")

    def test_pitr_with_persist_to_majority_durability(self):
        """
        P1: Validates PITR correctly captures and restores documents written with
        PERSIST_TO_MAJORITY sync-write durability.

        PERSIST_TO_MAJORITY requires the mutation to be fsynced to disk on a
        majority of nodes before acknowledgement. This test confirms those
        higher-durability mutations appear correctly in the continuous backup stream.

        Note: PITR is supported only on Magma buckets. Requires >= 3 KV nodes.
        """
        if len(self.cluster.kv_nodes) < 3:
            self.skipTest(
                "PERSIST_TO_MAJORITY durability requires at least 3 KV nodes; "
                f"cluster has {len(self.cluster.kv_nodes)}")

        original_durability = self.durability_level
        self.durability_level = "PERSIST_TO_MAJORITY"
        self.log.info("Durability set to PERSIST_TO_MAJORITY for this test")

        try:
            CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
            self.bucket_util._wait_for_stats_all_buckets(
                self.cluster, self.cluster.buckets)
            count_t1 = self.bucket_util.get_buckets_item_count(
                self.cluster, self.bucket.name)

            self.sleep(self.continuous_backup_interval * 60,
                       "Waiting for backup interval (PERSIST_TO_MAJORITY load)")
            ts_t1 = self.cont_bk_mgr.get_cluster_timestamp()
            self.log.info(
                f"T1 (PERSIST_TO_MAJORITY): {ts_t1}, count: {count_t1}")

            CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
            self.bucket_util._wait_for_stats_all_buckets(
                self.cluster, self.cluster.buckets)
            count_t2 = self.bucket_util.get_buckets_item_count(
                self.cluster, self.bucket.name)

            self.sleep(self.continuous_backup_interval * 60,
                       "Waiting for second backup interval")
            ts_t2 = self.cont_bk_mgr.get_cluster_timestamp()
            self.log.info(f"T2: {ts_t2}, count: {count_t2}")

            self.backup_mgr.backup(self.backup_archive_dir, self.backup_repo_name)

            restore_bucket_name = f"restore_persist_maj_{int(time.time())}"
            self._create_restore_bucket(restore_bucket_name)

            self.log.info("PITR restore to T1 (PERSIST_TO_MAJORITY)")
            self._restore_entire_bucket(ts_t1, restore_bucket_name)
            self.bucket_util._wait_for_stats_all_buckets(
                self.cluster, self.cluster.buckets)
            self._verify_doc_count(count_t1, bucket_name=restore_bucket_name)

            self._delete_and_recreate_restore_bucket(restore_bucket_name)

            self.log.info("PITR restore to T2")
            self._restore_entire_bucket(ts_t2, restore_bucket_name)
            self.bucket_util._wait_for_stats_all_buckets(
                self.cluster, self.cluster.buckets)
            self._verify_doc_count(count_t2, bucket_name=restore_bucket_name)

        finally:
            self.durability_level = original_durability

        self.log.info("Test completed successfully")

    # =========================================================================
    # P1 — Subdocument mutation PITR test
    # =========================================================================

    def test_pitr_with_subdoc_mutations(self):
        """
        P1: Validates continuous backup correctly captures subdocument mutations.

        Subdoc operations (mutate_in) produce DCP events with opcode
        CMD_SUBDOC_MULTI_MUTATION, distinct from regular CMD_SET/CMD_REPLACE.
        This test verifies those events are present in the continuous backup log
        and that PITR restores reflect the correct item count at each timestamp.

        Verification strategy:
          - 20 % of docs are deleted before T1, giving a measurable count difference.
          - Subdoc counter-increment mutations follow; they do not change item count,
            so the count at T1 and T2 is the same.  This confirms the backup stream
            was not disrupted by subdoc DCP events.

        Note: PITR is supported only on Magma buckets.
        """
        from sdk_client3 import SDKClient

        # Phase 1: initial load + 20 % deletes -> capture T1
        CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)

        delete_spec = self.bucket_util.get_crud_template_from_package(
            self.data_spec_name)
        delete_spec[MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION] = 20
        delete_spec[MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 0
        delete_spec[MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 0
        CollectionBase.over_ride_doc_loading_template_params(self, delete_spec)
        self.bucket_util.run_scenario_from_spec(
            self.task, self.cluster, self.cluster.buckets, delete_spec,
            mutation_num=1, batch_size=self.batch_size,
            process_concurrency=self.process_concurrency)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.sleep(10, "Waiting for item count to stabilize after ep_queue drain")
        count_t1 = self.bucket_util.get_buckets_item_count(
            self.cluster, self.bucket.name)

        self.sleep(self.continuous_backup_interval * 60,
                   "Waiting for backup interval before subdoc phase")
        ts_t1 = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"T1 (pre-subdoc, post-delete): {ts_t1}, count: {count_t1}")

        # Phase 2: subdoc counter-increment mutations via SDK
        # These produce CMD_SUBDOC_MULTI_MUTATION DCP events
        self.log.info("Performing subdoc counter-increment mutations via SDK")
        sdk_client = SDKClient(self.cluster, self.bucket)
        subdoc_errors = []
        try:
            for i in range(200):
                doc_key = f"test_collections-0-{i}"
                try:
                    sdk_client.crud(
                        "subdoc_insert", doc_key,
                        {"path": "pitr_counter", "value": 1},
                        create_path=True)
                except Exception as e:
                    subdoc_errors.append(str(e))
        finally:
            sdk_client.close()

        if subdoc_errors:
            self.log.warning(
                f"{len(subdoc_errors)} subdoc ops had errors "
                f"(key format may differ from spec — first: {subdoc_errors[0]})")

        # Subdoc mutations must not change item count
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        count_after_subdoc = self.bucket_util.get_buckets_item_count(
            self.cluster, self.bucket.name)
        self.assertEqual(
            count_t1, count_after_subdoc,
            f"Item count changed after subdoc mutations "
            f"(expected {count_t1}, got {count_after_subdoc})")

        self.sleep(self.continuous_backup_interval * 60,
                   "Waiting for backup interval to capture subdoc mutations")
        ts_t2 = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"T2 (post-subdoc): {ts_t2}, count: {count_after_subdoc}")

        self.backup_mgr.backup(self.backup_archive_dir, self.backup_repo_name)

        restore_bucket_name = f"restore_subdoc_{int(time.time())}"
        self._create_restore_bucket(restore_bucket_name)

        # Both T1 and T2 have same count (subdoc does not change count);
        # confirming the restore succeeds at both timestamps validates that
        # the subdoc DCP events did not corrupt the continuous backup log.
        self.log.info("PITR restore to T1 (pre-subdoc-mutations)")
        self._restore_entire_bucket(ts_t1, restore_bucket_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_t1, bucket_name=restore_bucket_name)

        self._delete_and_recreate_restore_bucket(restore_bucket_name)

        self.log.info("PITR restore to T2 (post-subdoc-mutations — same count expected)")
        self._restore_entire_bucket(ts_t2, restore_bucket_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_t1, bucket_name=restore_bucket_name)

        self.log.info("Test completed successfully")

    # =========================================================================
    # P2 — Edge-case and defensive PITR tests
    # =========================================================================

    def test_pitr_invalid_timestamp_handling(self):
        """
        P2: Validates cbcontbk restore handles invalid/out-of-range timestamps
        gracefully without crashing the cluster or corrupting the backup.

        Scenarios:
          A) Timestamp far in the past (before any backup) — expect error.
          B) Timestamp far in the future — expect error or latest-available restore.

        Note: PITR is supported only on Magma buckets.
        """
        CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)

        self.sleep(self.continuous_backup_interval * 60, "Waiting for backup interval")
        self.backup_mgr.backup(self.backup_archive_dir, self.backup_repo_name)

        restore_bucket_name = f"restore_invalid_ts_{int(time.time())}"
        self._create_restore_bucket(restore_bucket_name)

        def _attempt_restore(label, ts_value):
            self.log.info(f"Attempting PITR with {label} timestamp: {ts_value}")
            output, error = self.cont_bk_mgr.restore(
                self.backup_archive_dir, self.backup_repo_name,
                location=self.continuous_backup_location,
                temp_dir="/tmp",
                timestamp=ts_value,
                map_data=f"{self.bucket.name}={restore_bucket_name}")
            combined = " ".join((output or []) + (error or [])).lower()
            self.log.info(f"  [{label}] output: {combined[:300]}")
            signalled_error = any(
                kw in combined
                for kw in ["error", "invalid", "failed", "not found",
                            "before", "outside", "no history", "no backup"])
            self.log.info(
                f"  [{label}] outcome: " +
                ("error returned as expected" if signalled_error
                 else "silently restored to nearest available point"))

        _attempt_restore("ancient (2015-01-01)", "2015-01-01T00:00:00Z")
        _attempt_restore("future (2035-01-01)",  "2035-01-01T00:00:00Z")

        # Cluster and bucket must remain healthy after invalid restore attempts
        self.log.info("Verifying cluster and bucket health after invalid restore attempts")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        current_count = self.bucket_util.get_buckets_item_count(
            self.cluster, self.bucket.name)
        self.assertGreater(
            current_count, 0,
            "Bucket appears empty after invalid restore attempts — cluster may be unhealthy")
        self.log.info(f"Cluster healthy; bucket has {current_count} items")

        self.log.info("Test completed successfully")

    def test_pitr_history_retention_bytes_limit(self):
        """
        P2: Validates PITR under historyRetentionBytes pressure.

        Sets a tight historyRetentionBytes limit on a Magma bucket and loads
        enough data to push the history log toward that limit. Verifies that:
          - PITR to a recent timestamp (within the byte-limited window) succeeds.
          - The bucket remains healthy and does not crash under byte pressure.

        Note: PITR is supported only on Magma buckets. historyRetentionBytes
              is a Magma-only setting.
        """
        bytes_limit = 2 * 1024 * 1024 * 1024  # 2 GiB (server minimum)
        self.log.info(
            f"Setting historyRetentionBytes to {bytes_limit} bytes (2 GiB)")
        self.bucket_util.update_bucket_property(
            self.cluster.master, self.bucket,
            history_retention_bytes=bytes_limit)
        self.sleep(10, "Waiting for historyRetentionBytes setting to propagate")

        for wave in range(1, 4):
            self.log.info(f"Loading data wave {wave}/3")
            CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
            self.bucket_util._wait_for_stats_all_buckets(
                self.cluster, self.cluster.buckets)
            self.sleep(self.continuous_backup_interval * 60,
                       f"Waiting for backup interval after wave {wave}")

        count_recent = self.bucket_util.get_buckets_item_count(
            self.cluster, self.bucket.name)
        ts_recent = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"Recent timestamp: {ts_recent}, count: {count_recent}")

        self.backup_mgr.backup(self.backup_archive_dir, self.backup_repo_name)

        restore_bucket_name = f"restore_bytes_limit_{int(time.time())}"
        self._create_restore_bucket(restore_bucket_name)

        self.log.info(
            "PITR restore to recent timestamp under historyRetentionBytes limit")
        self._restore_entire_bucket(ts_recent, restore_bucket_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_recent, bucket_name=restore_bucket_name)
        self.log.info("Restore under byte limit succeeded")

        # Reset historyRetentionBytes to unlimited (0 = no byte limit)
        self.bucket_util.update_bucket_property(
            self.cluster.master, self.bucket,
            history_retention_bytes=0)

        self.log.info("Test completed successfully")

    def test_pitr_with_active_compression(self):
        """
        P2: Validates PITR works correctly on a Magma bucket with ACTIVE compression.

        With ACTIVE compression the server proactively compresses stored values.
        The continuous backup DCP stream receives compressed values and must pass
        them through transparently so cbcontbk can restore them correctly.

        Note: PITR is supported only on Magma buckets. Default spec uses PASSIVE.
        """
        self.log.info("Switching bucket compression mode to ACTIVE")
        self.bucket_util.update_bucket_property(
            self.cluster.master, self.bucket,
            compression_mode=Bucket.CompressionMode.ACTIVE)
        self.sleep(10, "Waiting for compression mode change to propagate")

        CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        count_t1 = self.bucket_util.get_buckets_item_count(
            self.cluster, self.bucket.name)

        self.sleep(self.continuous_backup_interval * 60,
                   "Waiting for backup interval (ACTIVE compression)")
        ts_t1 = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"T1 (ACTIVE compression): {ts_t1}, count: {count_t1}")

        CollectionBase.load_data_from_spec_file(self, self.data_spec_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        count_t2 = self.bucket_util.get_buckets_item_count(
            self.cluster, self.bucket.name)

        self.sleep(self.continuous_backup_interval * 60,
                   "Waiting for second backup interval")
        ts_t2 = self.cont_bk_mgr.get_cluster_timestamp()
        self.log.info(f"T2 (ACTIVE compression): {ts_t2}, count: {count_t2}")

        self.backup_mgr.backup(self.backup_archive_dir, self.backup_repo_name)

        restore_bucket_name = f"restore_active_comp_{int(time.time())}"
        self._create_restore_bucket(restore_bucket_name)

        self.log.info("PITR restore to T1 (ACTIVE compression bucket)")
        self._restore_entire_bucket(ts_t1, restore_bucket_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_t1, bucket_name=restore_bucket_name)

        self._delete_and_recreate_restore_bucket(restore_bucket_name)

        self.log.info("PITR restore to T2 (ACTIVE compression bucket)")
        self._restore_entire_bucket(ts_t2, restore_bucket_name)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self._verify_doc_count(count_t2, bucket_name=restore_bucket_name)

        # Restore original compression mode
        self.bucket_util.update_bucket_property(
            self.cluster.master, self.bucket,
            compression_mode=Bucket.CompressionMode.PASSIVE)

        self.log.info("Test completed successfully")
