from backup_restore.continuous_backup.continuous_backup_base import ContinuousBackupBase

from couchbase_helper.documentgenerator import doc_generator
from cb_tools.cbbackupmgr import CbBackupMgr
from shell_util.remote_connection import RemoteMachineShellConnection
from cb_constants import DocLoading


class ContinuousBackupTest(ContinuousBackupBase):
    def setUp(self):
        super(ContinuousBackupTest, self).setUp()

    def tearDown(self):
        super(ContinuousBackupTest, self).tearDown()

    def test_continuous_backup_params(self):
        """
        Test to generate docs using cbbackupmgr, enable continuous backup,
        and verify continuous backup params are set correctly.
        """
        self.log.info("Starting continuous backup test")

        # Step 1: Generate documents using cbbackupmgr generate_docs
        self.log.info("Generating %d documents into bucket: %s using cbbackupmgr" % (self.num_docs, self.bucket.name))

        load_gen = doc_generator("docs", 0, self.num_docs,
                                 key_size=20, doc_size=1024)
        self.log.info("Loading documents into the bucket")
        load_task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, load_gen, DocLoading.Bucket.DocOps.CREATE,
            batch_size=200, process_concurrency=4, print_ops_rate=False,
            skip_read_on_error=True)
        self.task_manager.get_task_result(load_task)

        # Step 2: Enable continuous backup on the bucket
        self.log.info("Enabling continuous backup on bucket: %s" % self.bucket.name)
        self.log.info("Continuous backup parameters:")
        self.log.info("  - historyRetentionSeconds: 6000")
        self.log.info("  - continuousBackupEnabled: %s" % self.continuous_backup_enabled)
        self.log.info("  - continuousBackupLocation: %s" % self.continuous_backup_location)
        self.log.info("  - continuousBackupInterval: %s" % self.continuous_backup_interval)

        self.bucket_util.update_bucket_property(
            cluster_node=self.cluster.master,
            bucket=self.bucket,
            history_retention_seconds=6000,
            continuous_backup_enabled=self.continuous_backup_enabled,
            continuous_backup_location=self.continuous_backup_location,
            continuous_backup_interval=self.continuous_backup_interval
        )

        self.log.info("Continuous backup enabled successfully")

        # Step 3: Get continuous backup params and verify
        self.log.info("Getting continuous backup params from bucket: %s" % self.bucket.name)

        params = self.bucket_util.get_continuous_backup_params(self.cluster, self.bucket.name)

        self.log.info("Retrieved continuous backup params:")
        for key, value in params.items():
            self.log.info("  - %s: %s" % (key, value))

        # Verify the params are correct
        verification_passed = True

        # Check continuousBackupEnabled
        # Normalize comparison by converting both to lowercase
        expected_enabled = "true" if self.continuous_backup_enabled else "false"
        actual_enabled = str(params.get("continuousBackupEnabled", "")).lower()
        if actual_enabled != expected_enabled:
            self.log.error("continuousBackupEnabled mismatch. Expected: %s, Actual: %s" %
                         (expected_enabled, actual_enabled))
            verification_passed = False
        else:
            self.log.info("continuousBackupEnabled verified: %s" % params.get("continuousBackupEnabled"))

        # Check continuousBackupLocation
        if params.get("continuousBackupLocation") != self.continuous_backup_location:
            self.log.error("continuousBackupLocation mismatch. Expected: %s, Actual: %s" %
                         (self.continuous_backup_location, params.get("continuousBackupLocation")))
            verification_passed = False
        else:
            self.log.info("continuousBackupLocation verified: %s" % params.get("continuousBackupLocation"))

        # Check continuousBackupInterval
        if params.get("continuousBackupInterval") != self.continuous_backup_interval:
            self.log.error("continuousBackupInterval mismatch. Expected: %s, Actual: %s" %
                         (self.continuous_backup_interval, params.get("continuousBackupInterval")))
            verification_passed = False
        else:
            self.log.info("continuousBackupInterval verified: %s" % params.get("continuousBackupInterval"))

        # Check historyRetentionSeconds
        expected_history_retention = 6000
        if params.get("historyRetentionSeconds") != expected_history_retention:
            self.log.error("historyRetentionSeconds mismatch. Expected: %s, Actual: %s" %
                         (expected_history_retention, params.get("historyRetentionSeconds")))
            verification_passed = False
        else:
            self.log.info("historyRetentionSeconds verified: %s" % params.get("historyRetentionSeconds"))

        # Step 4: Print success message
        if verification_passed:
            self.log.info("test completed successfully")
            self.log.info("Continuous backup test completed successfully")
        else:
            self.log.error("Continuous backup parameter verification failed")
            self.fail("Continuous backup parameter verification failed")
