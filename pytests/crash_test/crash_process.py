from random import randint, choice

from BucketLib.bucket import Bucket
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from basetestcase import ClusterSetup
from cb_constants import DocLoading
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from couchbase_helper.durability_helper import DurabilityHelper
from crash_test.constants import signum
from error_simulation.cb_error import CouchbaseError
from sdk_exceptions import SDKException
from sdk_client3 import SDKClient
from shell_util.remote_connection import RemoteMachineShellConnection


class CrashTest(ClusterSetup):
    def setUp(self):
        super(CrashTest, self).setUp()

        self.doc_ops = self.input.param("doc_ops", None)
        self.process_name = self.input.param("process", None)
        self.service_name = self.input.param("service", "data")
        self.sig_type = self.input.param("sig_type", "SIGKILL").upper()
        self.target_node = self.input.param("target_node", "active")
        self.load_initial_docs = self.input.param("load_initial_docs", True)

        self.pre_warmup_stats = {}
        self.timeout = 120
        self.new_docs_to_add = 10000

        if self.doc_ops is not None:
            self.doc_ops = self.doc_ops.split(";")

        if not self.atomicity:
            self.durability_helper = DurabilityHelper(
                self.log, self.nodes_init,
                durability=self.durability_level,
                replicate_to=self.replicate_to,
                persist_to=self.persist_to)
        self.bucket_util.create_default_bucket(
            cluster=self.cluster,
            bucket_type=self.bucket_type, ram_quota=self.bucket_size,
            replica=self.num_replicas, compression_mode="off",
            storage=self.bucket_storage,
            eviction_policy=self.bucket_eviction_policy)
        self.bucket_util.add_rbac_user(self.cluster.master)

        if self.load_docs_using == "sirius_java_sdk":
            for bucket in self.cluster.buckets:
                SiriusCouchbaseLoader.create_clients_in_pool(
                    self.cluster.master, self.cluster.master.rest_username,
                    self.cluster.master.rest_password,
                    bucket.name, req_clients=self.sdk_pool_capacity)
        if self.cluster.sdk_client_pool:
            self.log.info("Creating SDK clients for client_pool")
            for bucket in self.cluster.buckets:
                self.cluster.sdk_client_pool.create_clients(
                    self.cluster, bucket,
                    req_clients=self.sdk_pool_capacity,
                    compression_settings=self.sdk_compression)

        self.__is_sync_write_enabled = DurabilityHelper.is_sync_write_enabled(
            self.bucket_durability_level, self.durability_level)

        verification_dict = dict()
        verification_dict["ops_create"] = self.num_items
        verification_dict["sync_write_aborted_count"] = 0
        verification_dict["rollback_item_count"] = 0
        verification_dict["pending_writes"] = 0
        if self.__is_sync_write_enabled:
            verification_dict["sync_write_committed_count"] = self.num_items

        if self.load_initial_docs:
            # Load initial documents into the buckets
            self.log.info("Loading initial documents")
            gen_create = doc_generator(
                self.key, 0, self.num_items,
                key_size=self.key_size,
                doc_size=self.doc_size,
                doc_type=self.doc_type,
                target_vbucket=self.target_vbucket,
                vbuckets=self.cluster.buckets[0].numVBuckets)
            if self.atomicity:
                task = self.task.async_load_gen_docs_atomicity(
                    self.cluster, self.cluster.buckets, gen_create, "create",
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
                    sync=self.sync,
                    binary_transactions=self.binary_transactions)
                self.task.jython_task_manager.get_task_result(task)
            else:
                for bucket in self.cluster.buckets:
                    task = self.task.async_load_gen_docs(
                        self.cluster, bucket, gen_create,
                        DocLoading.Bucket.DocOps.CREATE, self.maxttl,
                        persist_to=self.persist_to,
                        replicate_to=self.replicate_to,
                        durability=self.durability_level,
                        batch_size=10, process_concurrency=8,
                        load_using=self.load_docs_using)
                    self.task.jython_task_manager.get_task_result(task)

                    self.bucket_util._wait_for_stats_all_buckets(
                        self.cluster, self.cluster.buckets)
                    # Verify cbstats vbucket-details
                    stats_failed = \
                        self.durability_helper.verify_vbucket_details_stats(
                            bucket, self.cluster_util.get_kv_nodes(self.cluster),
                            expected_val=verification_dict)

                    if stats_failed:
                        self.fail("Cbstats verification failed")

                self.bucket_util.verify_stats_all_buckets(self.cluster,
                                                          self.num_items)
        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)
        self.log.info("==========Finished CrashTest setup========")

    def tearDown(self):
        super(CrashTest, self).tearDown()

    def getTargetNode(self):
        if len(self.cluster.nodes_in_cluster) > 1:
            return self.cluster.nodes_in_cluster[randint(0, self.nodes_init-1)]
        return self.cluster.master

    def test_stop_process(self):
        """
        1. Starting loading docs into the default bucket
        2. Stop the requested process, which will impact the
           memcached operations
        3. Wait for load bucket task to complete
        4. Validate the docs for durability
        """
        error_to_simulate = self.input.param("simulate_error", None)
        def_bucket = self.cluster.buckets[0]
        target_node = self.getTargetNode()
        remote = RemoteMachineShellConnection(target_node)
        error_sim = CouchbaseError(self.log,
                                   remote,
                                   node=target_node)
        cb_stat = Cbstats(target_node)
        target_vbuckets = cb_stat.vbucket_list(
                def_bucket.name, self.target_node)
        cb_stat.disconnect()
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
            vbuckets=def_bucket.numVBuckets)

        if self.atomicity:
            task = self.task.async_load_gen_docs_atomicity(
                self.cluster, self.cluster.buckets, gen_load, "create",
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
                sync=self.sync,
                binary_transactions=self.binary_transactions)
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
                skip_read_on_error=True,
                load_using=self.load_docs_using)

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
            sdk_client = SDKClient(self.cluster, def_bucket)
            for doc_key, crud_result in task.fail.items():
                val = None
                if "value" not in crud_result:
                    val = crud_result["value"] if "value" in crud_result \
                        else {}
                result = sdk_client.crud(DocLoading.Bucket.DocOps.CREATE,
                                         doc_key, val,
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
            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets)
            self.bucket_util.verify_stats_all_buckets(self.cluster,
                                                      self.num_items)

        self.validate_test_failure()

    def test_crash_process(self):
        """
        1. Starting loading docs into the default bucket
        2. Crash the requested process, which will not impact the
           memcached operations
        3. Wait for load bucket task to complete
        4. Validate the docs for durability
        """
        def_bucket = self.cluster.buckets[0]
        target_node = self.getTargetNode()
        remote = RemoteMachineShellConnection(target_node)
        target_vbuckets = range(0, def_bucket.numVBuckets)
        retry_exceptions = list()

        # If Memcached is killed, we should not perform KV ops on
        # particular node. If not we can target all nodes for KV operation.
        if self.process_name == "memcached":
            cb_stat = Cbstats(target_node)
            target_vbuckets = cb_stat.vbucket_list(
                def_bucket.name, self.target_node)
            cb_stat.disconnect()
            if self.target_node == "active":
                retry_exceptions.extend(SDKException.TimeoutException)
        if len(target_vbuckets) == 0:
            self.log.error("No target vbucket list generated to load data")
            return

        # Create doc_generator targeting only the active/replica vbuckets
        # present in the target_node
        gen_load = doc_generator(
            self.key, self.num_items, self.new_docs_to_add,
            key_size=self.key_size,
            doc_size=self.doc_size,
            doc_type=self.doc_type,
            target_vbucket=target_vbuckets,
            vbuckets=def_bucket.numVBuckets)
        if self.atomicity:
            task = self.task.async_load_gen_docs_atomicity(
                self.cluster, self.cluster.buckets, gen_load, "create",
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
                sync=self.sync,
                binary_transactions=self.binary_transactions)
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
                skip_read_on_error=True,
                load_using=self.load_docs_using)

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
            self.bucket_util.verify_doc_op_task_exceptions(
                task_info, self.cluster, load_using=self.load_docs_using)
            self.bucket_util.log_doc_ops_task_failures(task_info)

        # Update self.num_items
        self.num_items += self.new_docs_to_add

        # Verification stats
        verification_dict = dict()
        verification_dict["ops_create"] = self.num_items
        verification_dict["sync_write_aborted_count"] = 0
        verification_dict["rollback_item_count"] = 0
        verification_dict["pending_writes"] = 0
        if self.__is_sync_write_enabled:
            verification_dict["sync_write_committed_count"] = self.num_items

        if self.bucket_type == Bucket.Type.EPHEMERAL \
                and self.process_name == "memcached":
            self.sleep(10, "Wait for memcached to recover from the crash")
            result = self.task.rebalance(self.cluster, [], [])
            self.assertTrue(result, "Rebalance failed")

        # Validate doc count
        if not self.atomicity:
            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets)
            self.bucket_util.verify_stats_all_buckets(self.cluster,
                                                      self.num_items)

            if self.process_name != "memcached":
                stats_failed = \
                    self.durability_helper.verify_vbucket_details_stats(
                        def_bucket,
                        self.cluster_util.get_kv_nodes(self.cluster),
                        expected_val=verification_dict)
                if stats_failed:
                    self.fail("Cbstats verification failed")

    def test_process_error_on_nodes(self):
        """
        Test to validate OoO returns feature
        1. Start parallel CRUDs using single client
        2. Perform process crash / stop with doc_ops in parallel
        3. Make sure no crash or ep_eng issue is seen with the err_simulation
        """
        tasks = list()
        node_data = dict()
        bucket = self.cluster.buckets[0]
        revert_errors = [CouchbaseError.STOP_MEMCACHED,
                         CouchbaseError.STOP_SERVER,
                         CouchbaseError.STOP_BEAMSMP,
                         CouchbaseError.STOP_PERSISTENCE]
        # Overriding sdk_timeout to max
        self.sdk_timeout = 60

        # Disable auto-failover to avoid failover of nodes
        status, _ = ClusterRestAPI(self.cluster.master) \
            .update_auto_failover_settings("false", 120)
        self.assertTrue(status, msg="Failure during disabling auto-failover")

        # Can take 'all_nodes' / 'single node'
        crash_on = self.input.param("crash_on", "single_node")
        error_to_simulate = self.input.param("simulate_error",
                                             CouchbaseError.KILL_MEMCACHED)
        num_times_to_affect = self.input.param("times_to_affect", 20)
        nodes_to_affect = self.cluster_util.get_kv_nodes(self.cluster)
        if crash_on == "single_node":
            nodes_to_affect = [choice(nodes_to_affect)]

        create_gen = doc_generator(self.key, self.num_items, self.num_items*2)
        update_gen = doc_generator(self.key, 0, int(self.num_items/2))
        delete_gen = doc_generator(self.key, int(self.num_items/2),
                                   self.num_items)

        for node in nodes_to_affect:
            shell = RemoteMachineShellConnection(node)
            node_data[node] = dict()
            node_data[node]["cb_err"] = CouchbaseError(self.log,
                                                       shell,
                                                       node=node)

        self.log.info("Starting doc-ops")
        for index, doc_op in enumerate(self.doc_ops):
            load_gen = update_gen
            if doc_op == DocLoading.Bucket.DocOps.CREATE:
                load_gen = create_gen
            elif doc_op == DocLoading.Bucket.DocOps.DELETE:
                load_gen = delete_gen
            task = self.task.async_load_gen_docs(
                self.cluster, bucket, load_gen, doc_op,
                replicate_to=self.replicate_to,
                persist_to=self.persist_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                batch_size=10,
                process_concurrency=1,
                skip_read_on_error=True,
                print_ops_rate=False,
                task_identifier="%s_%s" % (doc_op, index),
                load_using=self.load_docs_using)
            tasks.append(task)

        self.log.info("Starting error_simulation on %s" % nodes_to_affect)
        for itr in range(1, num_times_to_affect+1):
            self.log.info("Iteration :: %d" % itr)
            for node in nodes_to_affect:
                node_data[node]["cb_err"].create(error_to_simulate,
                                                 bucket.name)
            if error_to_simulate in revert_errors:
                self.sleep(30, "Sleep before reverting the error")
                for node in nodes_to_affect:
                    node_data[node]["cb_err"].revert(error_to_simulate,
                                                     bucket.name)
            else:
                self.sleep(10, "Wait for process to come back online")

        # Wait for doc_ops to complete
        for task in tasks:
            self.task_manager.get_task_result(task)

    def test_MB_55694(self):
        '''
        1. Create auto-delete Ephemeral bucket with at least 1 replica
        2. Turn off auto reprovision
        3. Start workload
        4. kill -9 one node
        5. Stop workload
        6. Sum  ht_cache_size stat for all replica vBuckets (cbstats vbucket-details)
           and check that ht_mem_used_replica (cbstats memory) is the same
        '''

        self.rest = ClusterRestAPI(self.cluster.master)

        # Creating ephemeral buckets with full eviction
        for i in range(self.num_buckets):
            b_name = "bucket{}".format(i)
            self.bucket_util.create_default_bucket(self.cluster,
                                                   bucket_type=self.bucket_type,
                                                   replica=self.num_replicas,
                                                   eviction_policy=self.bucket_eviction_policy,
                                                   ram_quota=self.bucket_size,
                                                   bucket_name=b_name)
            self.log.info("Bucket {} created".format(b_name))

        self.bucket_util.print_bucket_stats(self.cluster)

        status = self.rest.update_auto_reprovision_settings("false")
        self.assertTrue(status, "Could not turn off auto reprovision")
        self.log.info("Auto reprovision successfully turned off")

        status = self.rest.update_auto_failover_settings("false", 1800)
        self.assertTrue(status, "Could not turn off auto failover")
        self.log.info("Auto failover successfully turned off")

        self.doc_gen = doc_generator(key=self.key,
                                     start=0,
                                     end=self.num_items,
                                     doc_size=self.doc_size)
        tasks = self.bucket_util._async_load_all_buckets(self.cluster,
                                                         self.doc_gen,
                                                         "create",0,
                                                         process_concurrency=7,
                                                         track_failures=False,
                                                         skip_read_on_error=True,
                                                         retries=0,
                                                         suppress_error_table=True)

        self.sleep(180, "Workload generated and wait for load on all buckets")

        # Killing memcached on the node
        node_to_stop = choice(self.cluster.nodes_in_cluster)
        remote = RemoteMachineShellConnection(node_to_stop)
        error_sim = CouchbaseError(self.log,
                                   remote,
                                   node=node_to_stop)
        error_sim.create(action=CouchbaseError.KILL_MEMCACHED)
        self.sleep(10, "Wait for memcached to be back up")

        for task in tasks:
            self.task_manager.stop_task(task)
        self.log.info("Workload stopped on all buckets")

        self.sleep(30, "Wait for num_items to be reflected in ephemeral buckets")

        for bucket in self.cluster.buckets:
            for node in self.cluster.nodes_in_cluster:
                cbstats_obj = Cbstats(node)
                vbucket_stats = cbstats_obj.vbucket_details(bucket_name=bucket.name)
                mem_stats = cbstats_obj.all_stats(bucket.name, "memory")
                cbstats_obj.disconnect()
                ht_mem_used_replica_stat = mem_stats["ht_mem_used_inactive"]
                vbucket_mem_used = 0
                for vbucket in vbucket_stats:
                    if vbucket_stats[vbucket]["type"] == "replica":
                        vbucket_mem_used += vbucket_stats[vbucket]["ht_cache_size"]

                self.assertEqual(
                    int(vbucket_mem_used), int(ht_mem_used_replica_stat),
                    "Sum ht_cache_size stat for all replica vBuckets "
                    "(cbstats vbucket-details) and "
                    "ht_mem_used_replica (cbstats memory) "
                    "are not the same {}!={} on node {}"
                    .format(vbucket_mem_used, ht_mem_used_replica_stat, node.ip))
                self.log.info(
                    "Sum ht_cache_size stat for all replica vBuckets "
                    "(cbstats vbucket-details) = {} and ht_mem_used_replica "
                    "(cbstats memory) = {} are equal on node {}"
                    .format(vbucket_mem_used, ht_mem_used_replica_stat, node.ip))
