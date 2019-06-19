from random import randint

from basetestcase import BaseTestCase
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from couchbase_helper.durability_helper import DurabilityHelper
from crash_test.constants import signum
from remote.remote_util import RemoteMachineShellConnection


class CrashTest(BaseTestCase):
    def setUp(self):
        super(CrashTest, self).setUp()

        self.key = 'test_docs'.rjust(self.key_size, '0')
        self.doc_ops = self.input.param("doc_ops", None)
        self.process_name = self.input.param("process", None)
        self.service_name = self.input.param("service", "data")
        self.sig_type = self.input.param("sig_type", "SIGKILL").upper()
        self.target_node = self.input.param("target_node", "active")

        self.pre_warmup_stats = {}
        self.timeout = 120
        self.new_docs_to_add = 10000

        if self.doc_ops is not None:
            self.doc_ops = self.doc_ops.split(";")

        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend([self.cluster.master]+nodes_init)
        self.durability_helper = DurabilityHelper(
            self.log, self.nodes_init,
            durability=self.durability_level,
            replicate_to=self.replicate_to,
            persist_to=self.persist_to)
        self.bucket_util.create_default_bucket(
            bucket_type=self.bucket_type, ram_quota=self.bucket_size,
            replica=self.num_replicas, compression_mode="off")
        self.bucket_util.add_rbac_user()

        verification_dict = dict()
        verification_dict["ops_create"] = self.num_items
        verification_dict["sync_write_aborted_count"] = 0
        verification_dict["rollback_item_count"] = 0
        verification_dict["pending_writes"] = 0
        if self.durability_level:
            verification_dict["sync_write_committed_count"] = self.num_items
        # Create Shell connection and cbstats object for verification
        shell = RemoteMachineShellConnection(self.cluster.master)
        cbstat_obj = Cbstats(shell)

        # Load initial documents into the buckets
        gen_create = doc_generator(self.key, 0, self.num_items)
        for bucket in self.bucket_util.buckets:
            task = self.task.async_load_gen_docs(
                self.cluster, bucket, gen_create, "create", self.maxttl,
                persist_to=self.persist_to,
                replicate_to=self.replicate_to,
                durability=self.durability_level,
                batch_size=10, process_concurrency=8)
            self.task.jython_task_manager.get_task_result(task)

            # Verify cbstats vbucket-details
            stats_failed = self.durability_helper.verify_vbucket_details_stats(
                bucket, cbstat_obj,
                vbuckets=self.vbuckets, expected_val=verification_dict)

            if stats_failed:
                self.fail("Cbstats verification failed")

        # Disconnect the Shell connection
        shell.disconnect()

        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)
        self.log.info("==========Finished CrashTest setup========")

    def tearDown(self):
        super(CrashTest, self).tearDown()

    def getTargetNode(self):
        if len(self.cluster.nodes_in_cluster) > 1:
            return self.cluster.nodes_in_cluster[randint(0, self.nodes_init-1)]
        return self.cluster.master

    def getVbucketNumbers(self, shell_conn, bucket_name, replica_type):
        cb_stats = Cbstats(shell_conn)
        return cb_stats.vbucket_list(bucket_name, replica_type)

    def test_stop_process(self):
        """
        1. Starting loading docs into the default bucket
        2. Stop the requested process, which will impact the
           memcached operations
        3. Wait for load bucket task to complete
        4. Validate the docs for durability
        """
        def_bucket = self.bucket_util.buckets[0]
        target_node = self.getTargetNode()
        remote = RemoteMachineShellConnection(target_node)
        target_vbuckets = self.getVbucketNumbers(remote, def_bucket.name,
                                                 self.target_node)
        if len(target_vbuckets) == 0:
            self.log.error("No target vbucket list generated to load data")
            remote.disconnect()
            return

        # Create doc_generator targeting only the active/replica vbuckets
        # present in the target_node
        gen_load = doc_generator(self.key, self.num_items,
                                 self.num_items+self.new_docs_to_add,
                                 target_vbucket=target_vbuckets)
        self.num_items += self.new_docs_to_add
        task = self.task.async_load_gen_docs(
            self.cluster, def_bucket, gen_load, "create", 0,
            batch_size=10, timeout_secs=self.sdk_timeout)

        self.log.info("Stopping {0}:{1} on node {2}"
                      .format(self.process_name, self.service_name,
                              target_node.ip))
        remote.kill_process(self.process_name, self.service_name,
                            signum=signum[self.sig_type])

        self.sleep(20, "Wait before resuming the process"
                       .format(self.process_name))
        remote.kill_process(self.process_name, self.service_name,
                            signum=signum["SIGCONT"])
        remote.disconnect()

        # Wait for doc loading task to complete
        self.task.jython_task_manager.get_task_result(task)

        # Validate doc count
        # TODO: Add verification for rollbacks based on active/replica failure
        # self.bucket_util.verify_unacked_bytes_all_buckets()
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

    def test_crash_process(self):
        """
        1. Starting loading docs into the default bucket
        2. Crash the requested process, which will not impact the
           memcached operations
        3. Wait for load bucket task to complete
        4. Validate the docs for durability
        """
        def_bucket = self.bucket_util.buckets[0]
        target_node = self.getTargetNode()
        remote = RemoteMachineShellConnection(target_node)
        target_vbuckets = range(0, self.vbuckets)
        retry_exceptions = list()

        # If Memcached is killed, we should not perform KV ops on
        # particular node. If not we can target all nodes for KV operation.
        if self.process_name == "memcached":
            target_vbuckets = self.getVbucketNumbers(remote, def_bucket.name,
                                                     self.target_node)
            if self.target_node == "active":
                retry_exceptions = [
                    "com.couchbase.client.core.error.RequestTimeoutException"]
        if len(target_vbuckets) == 0:
            self.log.error("No target vbucket list generated to load data")
            remote.disconnect()
            return

        # Create doc_generator targeting only the active/replica vbuckets
        # present in the target_node
        gen_load = doc_generator(self.key, self.num_items,
                                 self.num_items+self.new_docs_to_add,
                                 target_vbucket=target_vbuckets)
        self.num_items += self.new_docs_to_add
        task = self.task.async_load_gen_docs(
            self.cluster, def_bucket, gen_load, "create", 0,
            batch_size=10, durability=self.durability_level,
            timeout_secs=self.sdk_timeout, skip_read_on_error=True)

        task_info = dict()
        task_info[task] = self.bucket_util.get_doc_op_info_dict(
            def_bucket, "create", 0,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            timeout=self.sdk_timeout, time_unit="seconds",
            retry_exceptions=retry_exceptions)

        self.sleep(10, "Wait for doc_ops to start")
        self.log.info("Killing {0}:{1} on node {2}"
                      .format(self.process_name, self.service_name,
                              target_node.ip))
        remote.kill_process(self.process_name, self.service_name,
                            signum=signum[self.sig_type])
        remote.disconnect()
        # Wait for tasks completion and validate failures
        self.bucket_util.verify_doc_op_task_exceptions(task_info, self.cluster)
        self.bucket_util.log_doc_ops_task_failures(task_info)

        # Verification stats
        verification_dict = dict()
        verification_dict["ops_create"] = self.num_items
        verification_dict["sync_write_aborted_count"] = 0
        verification_dict["rollback_item_count"] = 0
        verification_dict["pending_writes"] = 0
        if self.durability_level:
            verification_dict["sync_write_committed_count"] = self.num_items

        # Validate doc count
        shell = RemoteMachineShellConnection(self.cluster.master)
        cbstat_obj = Cbstats(shell)
        stats_failed = self.durability_helper.verify_vbucket_details_stats(
            def_bucket, cbstat_obj,
            vbuckets=self.vbuckets, expected_val=verification_dict)
        if stats_failed:
            self.fail("Cbstats verification failed")
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)
