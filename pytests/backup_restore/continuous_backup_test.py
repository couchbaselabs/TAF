from basetestcase import ClusterSetup
from couchbase_helper.documentgenerator import doc_generator
from cb_tools.cbbackupmgr import CbBackupMgr
from shell_util.remote_connection import RemoteMachineShellConnection
from cb_constants import DocLoading


class ContinuousBackupTest(ClusterSetup):
    def setUp(self):
        super(ContinuousBackupTest, self).setUp()
        self.key = "test_backup_docs"
        self.num_docs = self.input.param("num_docs", 100)
        self.archive_dir = self.input.param("archive_dir", "/tmp/backup")
        self.repo_name = self.input.param("repo_name", "test_repo")

        # Continuous backup parameters
        self.bucket_name = self.input.param("bucket_name", "cb_cont_bkp_bucket")
        self.continuous_backup_location = self.input.param("continuous_backup_location", "/tmp/cont_bkp")
        self.continuous_backup_interval = self.input.param("continuous_backup_interval", 5)
        self.continuous_backup_enabled = self.input.param("continuous_backup_enabled", True)

        # Create continuous backup folder
        self.shell = RemoteMachineShellConnection(self.cluster.master)
        output, error = self.shell.execute_command(f"mkdir -p {self.continuous_backup_location}")
        if error:
            self.fail("Error creating continuous backup folder: %s" % error)
        else:
            self.log.info("Created continuous backup folder: %s" % self.continuous_backup_location)
            # Set permissions for couchbase user to write
            output, error = self.shell.execute_command(f"chmod 777 {self.continuous_backup_location}")
            if error:
                self.fail("Error setting permissions on continuous backup folder: %s" % error)
            else:
                self.log.info("Set permissions on continuous backup folder")

        # Create bucket with self.bucket_name
        self.bucket_util.create_default_bucket(
            self.cluster,
            bucket_name=self.bucket_name)

        self.bucket = self.cluster.buckets[0]
        self.bucket_util.add_rbac_user(self.cluster.master)

        # Create backup manager instance
        self.backup_mgr = CbBackupMgr(self.shell,
                                      username=self.cluster.master.rest_username,
                                      password=self.cluster.master.rest_password)

    def tearDown(self):
        # Clean up continuous backup folder
        try:
            if hasattr(self, 'shell') and self.shell:
                self.log.info("Removing continuous backup folder: %s" % self.continuous_backup_location)
                output, error = self.shell.execute_command(f"rm -rf {self.continuous_backup_location}")
                if error:
                    self.log.warning("Error removing continuous backup folder: %s" % error)
        except Exception as e:
            self.log.warning("Exception during cleanup: %s" % str(e))

        # Delete the continuous backup bucket if exists
        try:
            if hasattr(self, 'cluster') and hasattr(self, 'bucket_util'):
                buckets = self.bucket_util.get_all_buckets(self.cluster)
                for bucket in buckets:
                    if bucket.name == self.bucket_name:
                        self.log.info("Deleting bucket: %s" % bucket.name)
                        self.bucket_util.delete_bucket(self.cluster, bucket)
                        self.log.info("Bucket %s deleted successfully" % bucket.name)
                        break
        except Exception as e:
            self.fail("Exception during bucket deletion: %s" % str(e))

        finally:
            # Disconnect backup manager and shell connection
            if hasattr(self, 'backup_mgr') and self.backup_mgr:
                try:
                    self.backup_mgr.disconnect()
                except:
                    pass
            if hasattr(self, 'shell') and self.shell:
                try:
                    self.shell.disconnect()
                except:
                    pass

        super(ContinuousBackupTest, self).tearDown()

    def test_continuous_backup_params(self):
        """
        Test to generate docs using cbbackupmgr, enable continuous backup,
        and verify continuous backup params are set correctly.
        """
        self.log.info("Starting continuous backup test")

        # Step 1: Generate documents using cbbackupmgr generate_docs
        self.log.info("Generating %d documents into bucket: %s using cbbackupmgr" % (self.num_docs, self.bucket.name))

        load_gen = doc_generator("docs", 0, 100,
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
