import re
import copy
import json
from random import randrange, randint
from threading import Thread
from membase.api.rest_client import RestConnection, RestHelper
from couchbase_helper.documentgenerator import BlobGenerator, DocumentGenerator
from couchbase_helper.documentgenerator import doc_generator
from BucketLib.bucket import Bucket
from BucketLib.BucketOperations import BucketHelper
from bucket_utils.bucket_ready_functions import BucketUtils
from cluster_utils.cluster_ready_functions import ClusterUtils
from remote.remote_util import RemoteUtilHelper, RemoteMachineShellConnection
from enterprise_bkrs_basetest import EnterpriseBKRSNewBaseTest

from java.util.concurrent import ExecutionException


class EnterpriseBackupRestoreTransactionTest(EnterpriseBKRSNewBaseTest):
    def setUp(self):
        super(EnterpriseBackupRestoreTransactionTest, self).setUp()
        for server in [self.backupset.backup_host, self.backupset.restore_cluster_host]:
            conn = RemoteMachineShellConnection(server)
            conn.extract_remote_info()
            conn.terminate_processes(conn.info, ["cbbackupmgr"])
            conn.disconnect()
        self.atomicity = self.input.param("atomicity", True)
        self.kv_gen = None

    def tearDown(self):
        super(EnterpriseBackupRestoreTransactionTest, self).tearDown()

    def test_backup_create(self):
        self.backup_create_validate()

    def key_generators(self, op_type="verify"):
        start = 0
        if op_type == "create":
            end = self.num_items
        elif op_type == "update":
            end = self.num_items / 2
        elif op_type == "delete":
            end = self.num_items / 4
        else:
            end = self.num_items
        return doc_generator("bkrs", start, end)

    def load_buckets(self, cluster, buckets, ops_type, commit):
        tasks = []
        self.kv_gen = self.key_generators(ops_type)
        if self.atomicity:
            tasks.append(self.task.async_load_gen_docs_atomicity(cluster, buckets,
                                                                 self.kv_gen, ops_type, exp=0,
                                                                 commit=commit,
                                                                 batch_size=10,
                                                                 process_concurrency=8,
                                                                 num_threads=self.num_threads))
        else:
            for bucket in buckets:
                tasks.append(self.task.async_load_gen_docs(
                    cluster, bucketa, self.kv_gen, ops_type, 0, batch_size=20,
                    process_concurrency=1))
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

    def verify_commit_txn_docs(self, cluster, buckets):
        tasks = []
        if self.atomicity:
            tasks.append(self.task.async_load_gen_docs_atomicity(cluster, buckets,
                                                                 self.kv_gen, "verify", exp=0,
                                                                 commit=self.commit, batch_size=10,
                                                                 process_concurrency=8))
        else:
            for bucket in buckets:
                tasks.append(self.task.async_load_gen_docs(cluster, bucket, self.kv_gen,
                                                           "verify", 0, batch_size=20,
                                                           process_concurrency=1))
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

    def test_backup_restore_txn_sanity(self):
        """
        1. Create default bucket on the cluster
        2. Loads bucket with docs (5 docs per txn)
        2. Perform updates or delete and run backup
        3. Perform restores and verify docs
        """
        self.bk_cluster = self.get_cb_cluster_by_name("C1")
        self.rs_cluster = self.get_cb_cluster_by_name("C2")

        self.bucket_name = 'default'
        bk_rest = RestConnection(self.bk_cluster.master)
        bucket = Bucket({"name": self.bucket_name, "replicaNumber": self.num_replicas})
        self.bk_cluster.bucket_util.create_bucket(bucket)
        msg = 'create_bucket succeeded but bucket {0} does not exist'.format(self.bucket_name)
        self.assertTrue(self.bk_cluster.bucket_util.wait_for_bucket_creation(bucket), msg)
        self.bk_cluster.bucket_util.add_rbac_user()
        self.backup_create()
        self.sleep(5)
        self.log.info("*** start to load items to all buckets")
        self.load_buckets(self.bk_cluster, self.bk_cluster.bucket_util.buckets, "create", self.commit)
        if self.ops_type != "create":
            self.load_buckets(self.bk_cluster,
                              self.bk_cluster.bucket_util.buckets,
                              self.ops_type, self.commit)
        self.log.info("*** done to load txn items to all buckets")
        self.expected_error = self.input.param("expected_error", None)

        self.backup_cluster()

        self.log.info("*** start to restore cluster")
        start = randrange(1, self.backupset.number_of_backups + 1)
        if start == self.backupset.number_of_backups:
            end = start
        else:
            end = randrange(start, self.backupset.number_of_backups + 1)

        if self.reset_restore_cluster:
            self.log.info("*** start to reset cluster")
            self.backup_reset_clusters(self.rs_cluster_utils)
            if self.same_cluster:
                self._initialize_nodes(Cluster(), self.servers[:self.nodes_init])
            else:
                self._initialize_nodes(Cluster(), self.input.clusters[0][:self.nodes_init])
            self.log.info("Done reset cluster")

        """ Add built-in user cbadminbucket to restore cluster """
        self.rs_cluster.bucket_util.add_rbac_user()
        rs_rest = RestConnection(self.rs_cluster.master)
        self.rs_cluster.bucket_util.create_bucket(bucket)
        self.assertTrue(self.rs_cluster.bucket_util.wait_for_bucket_creation(bucket), msg)
        self.backupset.start = start
        self.backupset.end = end
        self.backup_restore()

        self.verify_txn_in_bkrs_bucket()

    def test_backup_with_txn_pending(self):
        """
        1. Creates default bucket on the backup cluster
        2. Loads 100K docs with num_threads = 100
        2. While loading, run backup.  Backup will not backup doc due to txn in pending
        3. Verify docs in backup repo less than 100K
        """
        self.bk_cluster = self.get_cb_cluster_by_name("C1")
        self.rs_cluster = self.get_cb_cluster_by_name("C2")

        self.bucket_name = 'default'
        bk_rest = RestConnection(self.bk_cluster.master)
        bucket = Bucket({"name": self.bucket_name, "replicaNumber": self.num_replicas})
        self.bk_cluster.bucket_util.create_bucket(bucket)
        msg = 'create_bucket succeeded but bucket {0} does not exist'.format(self.bucket_name)
        self.assertTrue(self.bk_cluster.bucket_util.wait_for_bucket_creation(bucket), msg)
        self.bk_cluster.bucket_util.add_rbac_user()
        self.backup_create()

        self.kv_gen = self.key_generators(self.ops_type)
        bk_threads = []
        load_thread = Thread(target=self.load_buckets,
                             args=(self.bk_cluster, self.bk_cluster.bucket_util.buckets,
                                   "create", self.commit))
        bk_threads.append(load_thread)
        load_thread.start()
        bk_thread = Thread(target=self.backup_cluster)
        bk_threads.append(bk_thread)
        bk_thread.start()
        for bk_thread in bk_threads:
            bk_thread.join()

        _, output, _ = self.backup_list()
        backup_docs = 0
        for x in output:
            if "data" in x:
                backup_docs = int(x.split(" ").strip()[1])
                break
        if backup_docs >= int(self.num_items):
            self.fail("pending txn should not backup")
