from basetestcase import ClusterSetup
from couchbase_helper.documentgenerator import doc_generator
from cb_tools.cbbackupmgr import CbBackupMgr
from shell_util.remote_connection import RemoteMachineShellConnection


class SimpleBackupTest(ClusterSetup):
    def setUp(self):
        super(SimpleBackupTest, self).setUp()
        self.key = "test_backup_docs"
        self.num_docs = self.input.param("num_docs", 100)
        self.archive_dir = self.input.param("archive_dir", "/tmp/backup")
        self.repo_name = self.input.param("repo_name", "test_repo")
        
        # Ensure default bucket exists
        if len(self.cluster.buckets) == 0:
            self.bucket_util.create_default_bucket(self.cluster)
        
        self.bucket = self.cluster.buckets[0]
        self.bucket_util.add_rbac_user(self.cluster.master)
        
        # Create shell connection and backup manager instance
        self.shell = RemoteMachineShellConnection(self.cluster.master)
        self.backup_mgr = CbBackupMgr(self.shell,
                                      username=self.cluster.master.rest_username,
                                      password=self.cluster.master.rest_password)

    def tearDown(self):
        # Clean up backup repository
        try:
            if hasattr(self, 'backup_mgr') and self.backup_mgr:
                self.log.info("Removing backup repository: %s in archive: %s" % 
                             (self.repo_name, self.archive_dir))
                output, error = self.backup_mgr.remove(self.archive_dir, self.repo_name)
                if error:
                    self.log.warning("Error removing backup repository: %s" % error)
                else:
                    self.log.info("Backup repository removed successfully")
        except Exception as e:
            self.log.warning("Exception during repository cleanup: %s" % str(e))
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
        
        super(SimpleBackupTest, self).tearDown()

    def test_simple_backup(self):
        """
        Test to add 100 documents to default bucket, create a backup and do a restore
        """
        self.log.info("Starting simple backup test")
        
        # Step 1: Add documents to the default bucket
        self.log.info("Loading %d documents into bucket: %s" % (self.num_docs, self.bucket.name))
        
        # Create SDK clients in Java pool if using sirius_java_sdk
        if self.load_docs_using == "sirius_java_sdk":
            self.log.info("Creating SDK clients in Java side for bucket: %s" % self.bucket.name)
            from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
            SiriusCouchbaseLoader.create_clients_in_pool(
                self.cluster.master, 
                self.cluster.master.rest_username,
                self.cluster.master.rest_password,
                self.bucket.name, 
                req_clients=2)  # Create 2 clients for the pool
        
        self.load_gen = doc_generator(
            self.key, 0, self.num_docs,
            doc_size=self.doc_size,
            doc_type=self.doc_type,
            vbuckets=self.bucket.numVBuckets)
        
        task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, self.load_gen, "create", 0,
            batch_size=10, process_concurrency=2,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            load_using=self.load_docs_using)
        
        # Wait for task to complete
        self.task.jython_task_manager.get_task_result(task)
        
        # Wait for stats to be updated
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_docs)
        
        self.log.info("Successfully loaded %d documents" % self.num_docs)
        
        # Step 2: Create backup repository
        self.log.info("Creating backup repository: %s in archive: %s" % (self.repo_name, self.archive_dir))
        
        # Create repository
        output, error = self.backup_mgr.create_repo(self.archive_dir, self.repo_name)
        if error:
            self.log.warning("Repository creation output: %s, errors: %s" % (output, error))
        else:
            self.log.info("Repository created successfully")
        
        # Step 3: Perform backup
        self.log.info("Starting backup operation")
        output, error = self.backup_mgr.backup(
            archive_dir=self.archive_dir,
            repo_name=self.repo_name,
            no_progress_bar=True
        )
        
        # Check for backup success
        output_str = ' '.join(output) if output else ''
        error_str = ' '.join(error) if error else ''
        combined_output = output_str + ' ' + error_str
        
        if 'Backup completed successfully' in combined_output or 'Backup successfully completed' in combined_output:
            self.log.info("Backup completed successfully")
            self.log.info("Backup output: %s" % output_str)
        else:
            self.log.error("Backup may have failed. Output: %s, Error: %s" % (output_str, error_str))
            # Don't fail the test, just log the issue
            if error and len(error) > 0:
                self.log.warning("Backup errors encountered: %s" % error_str)
        
        # Step 4: Delete the bucket
        self.log.info("Deleting bucket: %s" % self.bucket.name)
        self.bucket_util.delete_bucket(self.cluster, self.bucket)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets)
        self.log.info("Bucket %s deleted successfully" % self.bucket.name)
        
        # Step 5: Restore from backup
        self.log.info("Restoring from backup repository: %s in archive: %s" % 
                     (self.repo_name, self.archive_dir))
        output, error = self.backup_mgr.restore(
            archive_dir=self.archive_dir,
            repo_name=self.repo_name,
            no_progress_bar=True,
            auto_create_buckets=True
        )
        
        # Check for restore success
        output_str = ' '.join(output) if output else ''
        error_str = ' '.join(error) if error else ''
        combined_output = output_str + ' ' + error_str
        
        if 'Restore completed successfully' in combined_output or 'Restore successfully completed' in combined_output:
            self.log.info("Restore completed successfully")
            self.log.info("Restore output: %s" % output_str)
        else:
            self.log.error("Restore may have failed. Output: %s, Error: %s" % (output_str, error_str))
            if error and len(error) > 0:
                self.log.warning("Restore errors encountered: %s" % error_str)
        
        # Step 6: Verify restored bucket and document count
        self.log.info("Verifying restored bucket: %s" % self.bucket.name)
        
        # Get all buckets from cluster using bucket_util
        buckets = self.bucket_util.get_all_buckets(self.cluster)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, buckets)
        
        # Get the restored bucket
        restored_bucket = None
        for bucket in buckets:
            if bucket.name == self.bucket.name:
                restored_bucket = bucket
                break
        
        if restored_bucket is None:
            self.fail("Restored bucket %s not found" % self.bucket.name)
        
        # Verify document count
        actual_items = self.bucket_util.get_buckets_item_count(self.cluster, restored_bucket.name)
        self.log.info("Document count in restored bucket %s: %d (expected: %d)" % 
                     (restored_bucket.name, actual_items, self.num_docs))
        
        if actual_items != self.num_docs:
            self.fail("Document count mismatch after restore. Expected: %d, Actual: %d" % 
                     (self.num_docs, actual_items))
        
        self.log.info("Restore verification successful. Document count matches: %d" % actual_items)
        self.log.info("Simple backup test completed")
