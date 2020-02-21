from random import randint

from BucketLib.bucket import Bucket
from basetestcase import BaseTestCase
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from couchbase_helper.durability_helper import DurabilityHelper
from crash_test.constants import signum
from error_simulation.cb_error import CouchbaseError
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient

from sdk_exceptions import SDKException


class CrashTest(BaseTestCase):
    def setUp(self):
        super(CrashTest, self).setUp()

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
        if not self.atomicity:
            self.durability_helper = DurabilityHelper(
                self.log, self.nodes_init,
                durability=self.durability_level,
                replicate_to=self.replicate_to,
                persist_to=self.persist_to)
        self.bucket_util.create_default_bucket(
            bucket_type=self.bucket_type, ram_quota=self.bucket_size,
            replica=self.num_replicas, compression_mode="off",
            storage=self.bucket_storage,
            eviction_policy=self.bucket_eviction_policy)
        self.bucket_util.add_rbac_user()

        verification_dict = dict()
        verification_dict["ops_create"] = self.num_items
        verification_dict["sync_write_aborted_count"] = 0
        verification_dict["rollback_item_count"] = 0
        verification_dict["pending_writes"] = 0
        if self.durability_level:
            verification_dict["sync_write_committed_count"] = self.num_items

        # Load initial documents into the buckets
        gen_create = doc_generator(
            self.key, 0, self.num_items,
            key_size=self.key_size,
            doc_size=self.doc_size,
            doc_type=self.doc_type,
            target_vbucket=self.target_vbucket,
            vbuckets=self.cluster_util.vbuckets)
        if self.atomicity:
            task = self.task.async_load_gen_docs_atomicity(
                self.cluster, self.bucket_util.buckets, gen_create, "create",
                exp=0,
                batch_size=10,
                process_concurrency=self.process_concurrency,
                replicate_to=self.replicate_to,
                persist_to=self.persist_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                update_count=self.update_count,
                transaction_timeout=self.transaction_timeout,
                commit=True,
                sync=self.sync)
            self.task.jython_task_manager.get_task_result(task)
        else:
            for bucket in self.bucket_util.buckets:
                task = self.task.async_load_gen_docs(
                    self.cluster, bucket, gen_create, "create", self.maxttl,
                    persist_to=self.persist_to,
                    replicate_to=self.replicate_to,
                    durability=self.durability_level,
                    batch_size=10, process_concurrency=8)
                self.task.jython_task_manager.get_task_result(task)

                # Verify cbstats vbucket-details
                stats_failed = \
                    self.durability_helper.verify_vbucket_details_stats(
                        bucket, self.cluster_util.get_kv_nodes(),
                        vbuckets=self.cluster_util.vbuckets,
                        expected_val=verification_dict)

                if stats_failed:
                    self.fail("Cbstats verification failed")

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
        error_to_simulate = self.input.param("simulate_error", None)
        def_bucket = self.bucket_util.buckets[0]
        target_node = self.getTargetNode()
        remote = RemoteMachineShellConnection(target_node)
        error_sim = CouchbaseError(self.log, remote)
        target_vbuckets = self.getVbucketNumbers(remote, def_bucket.name,
                                                 self.target_node)
        if len(target_vbuckets) == 0:
            self.log.error("No target vbucket list generated to load data")
            remote.disconnect()
            return

        # Create doc_generator targeting only the active/replica vbuckets
        # present in the target_node
        gen_load = doc_generator(
            self.key, self.num_items, self.new_docs_to_add,
            key_size=self.key_size,
            doc_size=self.doc_size,
            doc_type=self.doc_type,
            target_vbucket=target_vbuckets,
            vbuckets=self.cluster_util.vbuckets)

        if self.atomicity:
            task = self.task.async_load_gen_docs_atomicity(
                self.cluster, self.bucket_util.buckets, gen_load, "create",
                exp=0,
                batch_size=10,
                process_concurrency=self.process_concurrency,
                replicate_to=self.replicate_to,
                persist_to=self.persist_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                update_count=self.update_count,
                transaction_timeout=self.transaction_timeout,
                commit=True,
                sync=self.sync)
        else:
            task = self.task.async_load_gen_docs(
                self.cluster, def_bucket, gen_load, "create",
                exp=0,
                batch_size=1,
                process_concurrency=8,
                replicate_to=self.replicate_to,
                persist_to=self.persist_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                skip_read_on_error=True)

        # Induce the error condition
        error_sim.create(error_to_simulate)

        self.sleep(20, "Wait before reverting the error condition")
        # Revert the simulated error condition and close the ssh session
        error_sim.revert(error_to_simulate)
        remote.disconnect()

        # Wait for doc loading task to complete
        self.task.jython_task_manager.get_task_result(task)
        if not self.atomicity:
            if len(task.fail.keys()) != 0:
                if self.target_node == "active" or self.num_replicas in [2, 3]:
                    self.log_failure("Unwanted failures for keys: %s"
                                     % task.fail.keys())

            validate_passed = \
                self.durability_helper.validate_durability_exception(
                    task.fail,
                    SDKException.DurabilityAmbiguousException)
            if not validate_passed:
                self.log_failure("Unwanted exception seen during validation")

            # Create SDK connection for CRUD retries
            sdk_client = SDKClient([self.cluster.master],
                                   def_bucket)
            for doc_key, crud_result in task.fail.items():
                result = sdk_client.crud("create",
                                         doc_key,
                                         crud_result["value"],
                                         replicate_to=self.replicate_to,
                                         persist_to=self.persist_to,
                                         durability=self.durability_level,
                                         timeout=self.sdk_timeout)
                if result["status"] is False:
                    self.log_failure("Retry of doc_key %s failed: %s"
                                     % (doc_key, result["error"]))
            # Close the SDK connection
            sdk_client.close()

        # Update self.num_items
        self.num_items += self.new_docs_to_add

        if not self.atomicity:
            # Validate doc count
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(self.num_items)

        self.validate_test_failure()

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
        target_vbuckets = range(0, self.cluster_util.vbuckets)
        retry_exceptions = list()

        # If Memcached is killed, we should not perform KV ops on
        # particular node. If not we can target all nodes for KV operation.
        if self.process_name == "memcached":
            target_vbuckets = self.getVbucketNumbers(remote, def_bucket.name,
                                                     self.target_node)
            if self.target_node == "active":
                retry_exceptions = [SDKException.TimeoutException]
        if len(target_vbuckets) == 0:
            self.log.error("No target vbucket list generated to load data")
            remote.disconnect()
            return

        # Create doc_generator targeting only the active/replica vbuckets
        # present in the target_node
        gen_load = doc_generator(
            self.key, self.num_items, self.new_docs_to_add,
            key_size=self.key_size,
            doc_size=self.doc_size,
            doc_type=self.doc_type,
            target_vbucket=target_vbuckets,
            vbuckets=self.cluster_util.vbuckets)
        if self.atomicity:
            task = self.task.async_load_gen_docs_atomicity(
                self.cluster, self.bucket_util.buckets, gen_load, "create",
                exp=0,
                batch_size=10,
                process_concurrency=self.process_concurrency,
                replicate_to=self.replicate_to,
                persist_to=self.persist_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                update_count=self.update_count,
                transaction_timeout=self.transaction_timeout,
                commit=True,
                sync=self.sync)
        else:
            task = self.task.async_load_gen_docs(
                self.cluster, def_bucket, gen_load, "create",
                exp=0,
                batch_size=10,
                process_concurrency=8,
                replicate_to=self.replicate_to,
                persist_to=self.persist_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                skip_read_on_error=True)

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
        if self.atomicity:
            self.task.jython_task_manager.get_task_result(task)
        if not self.atomicity:
            self.bucket_util.verify_doc_op_task_exceptions(task_info,
                                                           self.cluster)
            self.bucket_util.log_doc_ops_task_failures(task_info)

        # Update self.num_items
        self.num_items += self.new_docs_to_add

        # Verification stats
        verification_dict = dict()
        verification_dict["ops_create"] = self.num_items
        verification_dict["sync_write_aborted_count"] = 0
        verification_dict["rollback_item_count"] = 0
        verification_dict["pending_writes"] = 0
        if self.durability_level:
            verification_dict["sync_write_committed_count"] = self.num_items

        if self.bucket_type == Bucket.Type.EPHEMERAL \
                and self.process_name == "memcached":
            result = self.task.rebalance(self.servers[:self.nodes_init],
                                         [], [])
            self.assertTrue(result, "Rebalance failed")

        # Validate doc count
        if not self.atomicity:
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(self.num_items)

            if self.process_name != "memcached":
                stats_failed = \
                    self.durability_helper.verify_vbucket_details_stats(
                        def_bucket, self.cluster_util.get_kv_nodes(),
                        vbuckets=self.cluster_util.vbuckets, expected_val=verification_dict)
                if stats_failed:
                    self.fail("Cbstats verification failed")
