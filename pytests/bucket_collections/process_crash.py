from random import randint, sample

from BucketLib.bucket import Bucket
from Cb_constants import CbServer
from bucket_collections.collections_base import CollectionBase
from bucket_utils.bucket_ready_functions import BucketUtils
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from couchbase_helper.durability_helper import DurabilityHelper
from crash_test.constants import signum
from error_simulation.cb_error import CouchbaseError
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient
from couchbase_helper.tuq_helper import N1QLHelper

from sdk_exceptions import SDKException


class CrashTest(CollectionBase):
    def setUp(self):
        super(CrashTest, self).setUp()

        self.doc_ops = self.input.param("doc_ops", None)
        self.process_name = self.input.param("process", None)
        self.service_name = self.input.param("service", "data")
        self.sig_type = self.input.param("sig_type", "SIGKILL").upper()
        self.target_node = self.input.param("target_node", "active")
        self.client_type = self.input.param("client_type", "sdk").lower()
        self.N1qltxn = self.input.param("N1qltxn", False)

        self.pre_warmup_stats = dict()
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

        verification_dict = dict()
        verification_dict["ops_create"] = self.num_items
        verification_dict["sync_write_aborted_count"] = 0
        verification_dict["rollback_item_count"] = 0
        verification_dict["pending_writes"] = 0
        if self.durability_level:
            verification_dict["sync_write_committed_count"] = self.num_items

        # Load initial documents into the buckets
        transaction_gen_create = doc_generator(
            "transaction_key", 0, self.num_items,
            key_size=self.key_size,
            doc_size=self.doc_size,
            doc_type=self.doc_type,
            target_vbucket=self.target_vbucket,
            vbuckets=self.cluster_util.vbuckets)
        gen_create = doc_generator(
            self.key, 0, self.num_items,
            key_size=self.key_size,
            doc_size=self.doc_size,
            doc_type=self.doc_type,
            target_vbucket=self.target_vbucket,
            vbuckets=self.cluster_util.vbuckets)
        if self.atomicity:
            transaction_task = self.task.async_load_gen_docs_atomicity(
                self.cluster, self.bucket_util.buckets,
                transaction_gen_create, "create",
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
            self.task.jython_task_manager.get_task_result(transaction_task)
        for bucket in self.bucket_util.buckets:
            task = self.task.async_load_gen_docs(
                self.cluster, bucket, gen_create, "create", self.maxttl,
                persist_to=self.persist_to,
                replicate_to=self.replicate_to,
                durability=self.durability_level,
                batch_size=10, process_concurrency=8)
            self.task.jython_task_manager.get_task_result(task)

            self.bucket_util._wait_for_stats_all_buckets()
            # Verify cbstats vbucket-details
            stats_failed = \
                self.durability_helper.verify_vbucket_details_stats(
                    bucket, self.cluster_util.get_kv_nodes(),
                    vbuckets=self.cluster_util.vbuckets,
                    expected_val=verification_dict)

            if stats_failed:
                self.fail("Cbstats verification failed")

            self.bucket_util.verify_stats_all_buckets(self.num_items)
        self.bucket = self.bucket_util.buckets[0]
        if self.N1qltxn:
            self.n1ql_server = self.cluster_util.get_nodes_from_services_map(
                                service_type="n1ql",
                                get_all_nodes=True)
            self.n1ql_helper = N1QLHelper(server=self.n1ql_server,
                                              use_rest=True,
                                              buckets = self.bucket_util.buckets,
                                              log=self.log,
                                              scan_consistency='REQUEST_PLUS',
                                              num_collection=3,
                                              num_buckets=1,
                                              num_savepoints=1,
                                              override_savepoint=False,
                                              num_stmt=10,
                                              load_spec=self.data_spec_name)
            self.bucket_col = self.n1ql_helper.get_collections()
            self.stmts = self.n1ql_helper.get_stmt(self.bucket_col)
            self.stmts = self.n1ql_helper.create_full_stmts(self.stmts)
        self.log.info("==========Finished CrashTest setup========")

    def tearDown(self):
        super(CrashTest, self).tearDown()

    def getTargetNode(self):
        if len(self.cluster.nodes_in_cluster) > 1:
            return self.cluster.nodes_in_cluster[randint(0, self.nodes_init-1)]
        return self.cluster.master

    def start_doc_loading_tasks(self, target_vbuckets,
                                scope_name, collection_obj):
        # Create doc_generator targeting only the active/replica vbuckets
        # present in the target_node
        transaction_gen_load = doc_generator(
            "transaction_key", self.num_items, self.new_docs_to_add,
            key_size=self.key_size,
            doc_size=self.doc_size,
            doc_type=self.doc_type,
            target_vbucket=target_vbuckets,
            vbuckets=self.cluster_util.vbuckets)
        gen_load = doc_generator(
            self.key, self.num_items, self.new_docs_to_add,
            key_size=self.key_size,
            doc_size=self.doc_size,
            doc_type=self.doc_type,
            target_vbucket=target_vbuckets,
            vbuckets=self.cluster_util.vbuckets)
        if self.atomicity:
            self.transaction_load_task = \
                self.task.async_load_gen_docs_atomicity(
                    self.cluster, self.bucket_util.buckets,
                    transaction_gen_load, "create",
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
            collection_obj.num_items += self.new_docs_to_add
        elif self.N1qltxn:
            self.N1ql_load_task = self.task.async_n1qlTxn_query( self.stmts,
                 n1ql_helper=self.n1ql_helper,
                 commit=True,
                 scan_consistency="REQUEST_PLUS")
        self.doc_loading_task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_load, "create",
            exp=0,
            batch_size=10,
            process_concurrency=8,
            replicate_to=self.replicate_to,
            persist_to=self.persist_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            scope=scope_name, collection=collection_obj.name,
            skip_read_on_error=True)
        collection_obj.num_items += self.new_docs_to_add

    @staticmethod
    def getVbucketNumbers(shell_conn, bucket_name, replica_type):
        cb_stats = Cbstats(shell_conn)
        return cb_stats.vbucket_list(bucket_name, replica_type)

    def test_create_remove_scope_with_node_crash(self):
        """
        1. Select a error scenario to simulate in random
        2. Create error scenario either before or after scope create/delete
        3. Initiate scope creation/deletion under the bucket
        4. Validate the outcome of scope creation/deletion
        """
        def create_scope(client_type, bucket_obj, scope):
            if client_type == "sdk":
                client.create_scope(scope)
            elif client_type == "rest":
                self.bucket_util.create_scope(self.cluster.master, bucket_obj,
                                              {"name": scope})
            else:
                self.log_failure("Invalid client_type provided")

        def remove_scope(client_type, bucket_obj, scope):
            if client_type == "sdk":
                client.drop_scope(scope)
            elif client_type == "rest":
                self.bucket_util.drop_scope(self.cluster.master,
                                            bucket_obj,
                                            scope)
            else:
                self.log_failure("Invalid client_type provided")

        kv_nodes = self.cluster_util.get_kv_nodes()
        if len(kv_nodes) == 1:
            self.fail("Need atleast two KV nodes to run this test")

        client = None
        action = self.input.param("action", "create")
        crash_during = self.input.param("crash_during", "pre_action")
        data_load_option = self.input.param("data_load_option", None)
        crash_type = self.input.param("simulate_error",
                                      CouchbaseError.KILL_MEMCACHED)

        # Always use a random scope name to create/remove
        # since CREATE/DROP not supported for default scope
        self.scope_name = BucketUtils.get_random_name()

        # Select a KV node other than master node from the cluster
        node_to_crash = kv_nodes[sample(range(1, len(kv_nodes)), 1)[0]]

        # Create a required client object
        if self.client_type == "sdk":
            client = SDKClient([self.cluster.master], self.bucket)

        if action == "remove":
            # Create a scope to be removed
            use_client = sample(["sdk", "rest"], 1)[0]
            create_scope(use_client, self.bucket, self.scope_name)

        # Create a error scenario
        shell = RemoteMachineShellConnection(node_to_crash)
        cb_error = CouchbaseError(self.log, shell)
        cbstat_obj = Cbstats(shell)
        active_vbs = cbstat_obj.vbucket_list(self.bucket.name,
                                             vbucket_type="active")
        target_vbuckets = list(
            set(range(0, 1024)).difference(set(active_vbs)))
        doc_gen = doc_generator(self.key, 0, 1000,
                                target_vbucket=target_vbuckets)

        if crash_during == "pre_action":
            cb_error.create(crash_type)

        if action == "create":
            create_scope(self.client_type, self.bucket, self.scope_name)
        elif action == "remove":
            remove_scope(self.client_type, self.bucket, self.scope_name)

        if crash_during == "post_action":
            cb_error.create(crash_type)

        if data_load_option == "mutate_default_collection":
            task = self.task.async_load_gen_docs(
                self.cluster, self.bucket, doc_gen, "update",
                exp=self.maxttl,
                batch_size=200, process_concurrency=8,
                compression=self.sdk_compression,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout)
            self.task_manager.get_task_result(task)

        self.sleep(60, "Wait before reverting the error scenario")
        cb_error.revert(crash_type)

        # Close SSH and SDK connections
        shell.disconnect()
        if self.client_type == "sdk":
            client.close()

        self.bucket_util.validate_docs_per_collections_all_buckets()
        self.validate_test_failure()

    def test_create_remove_collection_with_node_crash(self):
        """
        1. Select a error scenario to simulate in random
        2. Create error scenario either before or after collection action
        3. Initiate collection creation/deletion under the bucket
        4. Validate the outcome of collection creation/deletion
        """
        def create_collection(client_type, bucket_obj, scope, collection):
            if client_type == "sdk":
                client.create_collection(collection, scope)
            elif client_type == "rest":
                self.bucket_util.create_scope(self.cluster.master, bucket_obj,
                                              {"name": scope})
            else:
                self.log_failure("Invalid client_type provided")

        def remove_collection(client_type, bucket_obj, scope, collection):
            if client_type == "sdk":
                client.drop_collection(scope, collection)
            elif client_type == "rest":
                self.bucket_util.drop_scope(self.cluster.master,
                                            bucket_obj,
                                            scope)
            else:
                self.log_failure("Invalid client_type provided")

        kv_nodes = self.cluster_util.get_kv_nodes()
        if len(kv_nodes) == 1:
            self.fail("Need atleast two KV nodes to run this test")

        client = None
        action = self.input.param("action", "create")
        crash_during = self.input.param("crash_during", "pre_action")
        data_load_option = self.input.param("data_load_option", None)
        crash_type = self.input.param("simulate_error",
                                      CouchbaseError.KILL_MEMCACHED)

        if self.scope_name != CbServer.default_scope:
            self.scope_name = BucketUtils.get_random_name()
            self.bucket_util.create_scope(self.cluster.master, self.bucket,
                                          {"name", self.scope_name})
        if self.collection_name != CbServer.default_collection:
            self.collection_name = BucketUtils.get_random_name()

        # Select a KV node other than master node from the cluster
        node_to_crash = kv_nodes[sample(range(1, len(kv_nodes)), 1)[0]]

        # Create a required client object
        if self.client_type == "sdk":
            client = SDKClient([self.cluster.master], self.bucket)

        if action == "remove" \
                and self.collection_name != CbServer.default_collection:
            # Create a collection to be removed
            use_client = sample(["sdk", "rest"], 1)[0]
            create_collection(use_client, self.bucket,
                              self.scope_name, self.collection_name)

        # Create a error scenario
        self.log.info("Selected scenario for test '%s'" % crash_type)
        shell = RemoteMachineShellConnection(node_to_crash)
        cb_error = CouchbaseError(self.log, shell)
        cbstat_obj = Cbstats(shell)
        active_vbs = cbstat_obj.vbucket_list(self.bucket.name,
                                             vbucket_type="active")
        target_vbuckets = list(
            set(range(0, 1024)).difference(set(active_vbs)))
        doc_gen = doc_generator(self.key, 0, 1000,
                                target_vbucket=target_vbuckets)

        if crash_during == "pre_action":
            cb_error.create(crash_type)

        if action == "create":
            create_collection(self.client_type, self.bucket,
                              self.scope_name, self.collection_name)
        elif action == "remove":
            remove_collection(self.client_type, self.bucket,
                              self.scope_name, self.collection_name)

        if crash_during == "post_action":
            cb_error.create(crash_type)

        if data_load_option == "mutate_default_collection":
            task = self.task.async_load_gen_docs(
                self.cluster, self.bucket, doc_gen, "update",
                exp=self.maxttl,
                batch_size=200, process_concurrency=8,
                compression=self.sdk_compression,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout)
            self.task_manager.get_task_result(task)

        self.sleep(60, "Wait before reverting the error scenario")
        cb_error.revert(crash_type)

        # Close SSH and SDK connections
        shell.disconnect()
        if self.client_type == "sdk":
            client.close()

        self.bucket_util.validate_docs_per_collections_all_buckets()
        self.validate_test_failure()

    def test_stop_process(self):
        """
        1. Starting loading docs into the default bucket
        2. Stop the requested process, which will impact the
           memcached operations
        3. Wait for load bucket task to complete
        4. Validate the docs for durability
        """
        error_to_simulate = self.input.param("simulate_error", None)
        target_node = self.getTargetNode()
        remote = RemoteMachineShellConnection(target_node)
        error_sim = CouchbaseError(self.log, remote)
        target_vbuckets = CrashTest.getVbucketNumbers(
            remote, self.bucket.name, self.target_node)

        bucket_dict = BucketUtils.get_random_collections(
            self.bucket_util.buckets,
            req_num=1,
            consider_scopes="all",
            consider_buckets="all")

        bucket = BucketUtils.get_bucket_obj(self.bucket_util.buckets,
                                            bucket_dict.keys()[0])
        scope_name = bucket_dict[bucket.name]["scopes"].keys()[0]
        collection_name = bucket_dict[bucket.name][
            "scopes"][scope_name]["collections"].keys()[0]
        scope = BucketUtils.get_scope_obj(
            bucket, scope_name)
        collection = BucketUtils.get_collection_obj(
            scope, collection_name)

        if len(target_vbuckets) == 0:
            self.log.error("No target vbucket list generated to load data")
            remote.disconnect()
            return

        self.start_doc_loading_tasks(target_vbuckets, scope_name, collection)

        # Induce the error condition
        error_sim.create(error_to_simulate)

        self.sleep(20, "Wait before reverting the error condition")
        # Revert the simulated error condition and close the ssh session
        error_sim.revert(error_to_simulate)
        remote.disconnect()

        # Wait for doc loading task to complete
        self.task.jython_task_manager.get_task_result(self.doc_loading_task)
        if self.atomicity:
            self.task.jython_task_manager.get_task_result(
                self.transaction_load_task)
        elif self.N1qltxn:
            self.task.jython_task_manager.get_task_result(
                self.N1ql_load_task)

        if len(self.doc_loading_task.fail.keys()) != 0:
            if self.target_node == "active" or self.num_replicas in [2, 3]:
                self.log_failure("Unwanted failures for keys: %s"
                                 % self.doc_loading_task.fail.keys())

        validate_passed = \
            self.durability_helper.validate_durability_exception(
                self.doc_loading_task.fail,
                SDKException.DurabilityAmbiguousException)
        if not validate_passed:
            self.log_failure("Unwanted exception seen during validation")

        # Create SDK connection for CRUD retries
        sdk_client = SDKClient([self.cluster.master], self.bucket)
        for doc_key, crud_result in self.doc_loading_task.fail.items():
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

        self.validate_test_failure()

        # Update self.num_items and validate docs per collection
        collection.num_items += self.new_docs_to_add
        if not self.N1qltxn:
            self.bucket_util.validate_docs_per_collections_all_buckets()

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
        self.transaction_load_task = None
        self.doc_loading_task = None
        self.N1ql_load_task = None

        # If Memcached is killed, we should not perform KV ops on
        # particular node. If not we can target all nodes for KV operation.
        if self.process_name == "memcached":
            target_vbuckets = CrashTest.getVbucketNumbers(
                remote, def_bucket.name, self.target_node)
            if self.target_node == "active":
                retry_exceptions = [SDKException.TimeoutException]
        if len(target_vbuckets) == 0:
            self.log.error("No target vbucket list generated to load data")
            remote.disconnect()
            return

        bucket_dict = BucketUtils.get_random_collections(
            self.bucket_util.buckets,
            req_num=1,
            consider_scopes="all",
            consider_buckets="all")

        bucket = BucketUtils.get_bucket_obj(self.bucket_util.buckets,
                                            bucket_dict.keys()[0])
        scope_name = bucket_dict[bucket.name]["scopes"].keys()[0]
        collection_name = bucket_dict[bucket.name][
            "scopes"][scope_name]["collections"].keys()[0]
        scope = BucketUtils.get_scope_obj(
            bucket, scope_name)
        collection = BucketUtils.get_collection_obj(
            scope, collection_name)

        self.start_doc_loading_tasks(target_vbuckets, scope_name, collection)

        task_info = dict()
        task_info[self.doc_loading_task] = \
            self.bucket_util.get_doc_op_info_dict(
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
        if self.transaction_load_task:
            self.task.jython_task_manager.get_task_result(
                self.transaction_load_task)
        if self.N1qltxn:
            self.task.jython_task_manager.get_task_result(
                self.N1ql_load_task)
        self.task_manager.get_task_result(self.doc_loading_task)
        self.bucket_util.verify_doc_op_task_exceptions(task_info,
                                                       self.cluster)
        self.bucket_util.log_doc_ops_task_failures(task_info)

        # Verification stats
        verification_dict = dict()
        verification_dict["ops_create"] = 2*self.num_items
        verification_dict["sync_write_aborted_count"] = 0
        verification_dict["rollback_item_count"] = 0
        verification_dict["pending_writes"] = 0
        if self.durability_level:
            verification_dict["sync_write_committed_count"] = 2*self.num_items

        if self.bucket_type == Bucket.Type.EPHEMERAL \
                and self.process_name == "memcached":
            result = self.task.rebalance(self.servers[:self.nodes_init],
                                         [], [])
            self.assertTrue(result, "Rebalance failed")

        # Validate doc count
        if self.process_name != "memcached":
            stats_failed = \
                self.durability_helper.verify_vbucket_details_stats(
                    def_bucket, self.cluster_util.get_kv_nodes(),
                    vbuckets=self.cluster_util.vbuckets,
                    expected_val=verification_dict)
            if stats_failed:
                self.fail("Cbstats verification failed")

        # Doc count validation per collection
        if not self.N1qltxn:
            self.bucket_util.validate_docs_per_collections_all_buckets()
