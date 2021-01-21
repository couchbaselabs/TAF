import time

from BucketLib.BucketOperations import BucketHelper
from Cb_constants import CbServer
from collections_helper.collections_spec_constants import MetaCrudParams
from couchbase_helper.documentgenerator import doc_generator
from bucket_collections.collections_base import CollectionBase

from couchbase_helper.tuq_helper import N1QLHelper
from pytests.N1qlTransaction.N1qlBase import N1qlBase

from couchbase_utils.cb_tools.cbstats import Cbstats
from membase.api.rest_client import RestConnection, RestHelper
from remote.remote_util import RemoteMachineShellConnection

from sdk_exceptions import SDKException


class CollectionsRebalance(CollectionBase):
    def setUp(self):
        super(CollectionsRebalance, self).setUp()
        self.bucket_util._expiry_pager()
        self.load_gen = doc_generator(self.key, 0, self.num_items)
        self.bucket = self.bucket_util.buckets[0]
        self.rest = RestConnection(self.cluster.master)
        self.data_load_spec = self.input.param("data_load_spec", "volume_test_load")
        self.data_load_stage = self.input.param("data_load_stage", "before")
        self.data_load_type = self.input.param("data_load_type", "async")
        self.nodes_swap = self.input.param("nodes_swap", 1)
        self.nodes_failover = self.input.param("nodes_failover", 1)
        self.failover_ops = ["graceful_failover_rebalance_out", "hard_failover_rebalance_out",
                             "graceful_failover_recovery", "hard_failover_recovery"]
        self.step_count = self.input.param("step_count", -1)
        self.recovery_type = self.input.param("recovery_type", "full")
        self.compaction = self.input.param("compaction", False)
        if self.compaction:
            self.disable_auto_compaction()
        self.warmup = self.input.param("warmup", False)
        self.update_replica = self.input.param("update_replica", False)  # for replica + rebalance tests
        self.updated_num_replicas = self.input.param("updated_num_replicas",
                                                     1)  # for replica + rebalance tests, forced hard failover
        self.forced_hard_failover = self.input.param("forced_hard_failover", False)  # for forced hard failover tests
        self.change_ram_quota_cluster = self.input.param("change_ram_quota_cluster",
                                                         False)  # To change during rebalance
        self.skip_validations = self.input.param("skip_validations", True)
        self.compaction_tasks = list()
        self.dgm_ttl_test = self.input.param("dgm_ttl_test", False)  # if dgm with ttl
        self.dgm = self.input.param("dgm", "100")  # Initial dgm threshold, for dgm test; 100 means no dgm
        self.N1ql_txn = self.input.param("N1ql_txn", False)
        if self.N1ql_txn:
            self.num_stmt_txn = self.input.param("num_stmt_txn", 5)
            self.num_collection = self.input.param("num_collection", 1)
            self.num_savepoints = self.input.param("num_savepoints", 0)
            self.override_savepoint = self.input.param("override_savepoint", 0)
            self.num_buckets = self.input.param("num_buckets", 1)

    def tearDown(self):
        super(CollectionsRebalance, self).tearDown()

    def setup_N1ql_txn(self):
        self.n1ql_server = self.cluster_util.get_nodes_from_services_map(
                                service_type="n1ql",
                                get_all_nodes=True)
        self.n1ql_helper = N1QLHelper(server=self.n1ql_server,
                                          use_rest=True,
                                          buckets = self.bucket_util.buckets,
                                          log=self.log,
                                          scan_consistency='REQUEST_PLUS',
                                          num_collection=self.num_collection,
                                          num_buckets=self.num_buckets,
                                          num_savepoints=self.num_savepoints,
                                          override_savepoint=self.override_savepoint,
                                          num_stmt=self.num_stmt_txn,
                                          load_spec=self.data_spec_name)
        self.bucket_col = self.n1ql_helper.get_collections()
        self.stmts = self.n1ql_helper.get_stmt(self.bucket_col)

    def execute_N1qltxn(self, server=None):
        if self.N1ql_txn:
            if self.services_init:
                self.server=server
            else:
                self.server=None
            self.retry_n1qltxn = False
            self.sleep(20, "wait for rebalance to start")
            self.n1ql_fun = N1qlBase()
            try:
                query_params = self.n1ql_helper.create_txn(server=self.server)
                self.collection_savepoint, self.savepoints, self.queries, rerun = \
                    self.n1ql_fun.full_execute_query(self.stmts, True, query_params,
                                        N1qlhelper=self.n1ql_helper, server=self.server)
                if not isinstance(self.collection_savepoint, dict):
                    self.log.info("N1ql txn failed will be retried")
                    self.retry_n1qltxn = True
            except:
                self.retry_n1qltxn = True

    def validate_N1qltxn_data(self):
        if self.retry_n1qltxn:
            self.n1ql_server = self.cluster_util.get_nodes_from_services_map(
                                service_type="n1ql",
                                get_all_nodes=True)
            self.execute_N1qltxn(self.n1ql_server[0])
        doc_gen_list = self.n1ql_helper.get_doc_gen_list(self.bucket_col)
        if isinstance(self.collection_savepoint, dict):
            results = [[self.collection_savepoint, self.savepoints]]
            self.log.info("queries ran are %s" % self.queries)
            self.n1ql_fun.process_value_for_verification(self.bucket_col,
                                 doc_gen_list, results,
                                 buckets = self.bucket_util.buckets)
        else:
            self.fail(self.collection_savepoint)

    def disable_auto_compaction(self):
        buckets = self.bucket_util.get_all_buckets()
        for bucket in buckets:
            if bucket.bucketType == "couchbase":
                self.bucket_util.disable_compaction(bucket=str(bucket.name))

    def compact_all_buckets(self):
        self.sleep(10, "wait for rebalance to start")
        self.log.info("Starting compaction for each bucket")
        for bucket in self.bucket_util.buckets:
            self.compaction_tasks.append(self.task.async_compact_bucket(
                self.cluster.master, bucket))

    def warmup_node(self, node):
        self.log.info("Warmuping up node...")
        shell = RemoteMachineShellConnection(node)
        shell.stop_couchbase()
        self.sleep(30)
        shell.start_couchbase()
        shell.disconnect()
        self.log.info("Done warming up...")

    def set_ram_quota_cluster(self):
        self.sleep(45, "Wait for rebalance have some progress")
        self.log.info("Changing cluster RAM size")
        status = self.rest.init_cluster_memoryQuota(self.cluster.master.rest_username,
                                                    self.cluster.master.rest_password,
                                                    memoryQuota=2500)
        self.assertTrue(status, "RAM quota wasn't changed")

    def set_retry_exceptions(self, doc_loading_spec):
        retry_exceptions = []
        if self.data_load_stage == "during" or (self.data_load_stage == "before" and self.data_load_type == "async"):
            retry_exceptions.append(SDKException.AmbiguousTimeoutException)
            retry_exceptions.append(SDKException.TimeoutException)
            retry_exceptions.append(SDKException.RequestCanceledException)
            if self.durability_level:
                retry_exceptions.append(SDKException.DurabilityAmbiguousException)
                retry_exceptions.append(SDKException.DurabilityImpossibleException)
        doc_loading_spec[MetaCrudParams.RETRY_EXCEPTIONS] = retry_exceptions

    def get_active_resident_threshold(self, bucket_name):
        self.rest_client = BucketHelper(self.cluster.master)
        dgm = self.rest_client.fetch_bucket_stats(
            bucket_name)["op"]["samples"]["vb_active_resident_items_ratio"][-1]
        return dgm

    def load_to_dgm(self):
        if self.dgm_ttl_test:
            maxttl = 300
        else:
            maxttl = 0
        self.log.info("Loading docs with maxttl:{0} in default collection "
                      "in order to load bucket in dgm".format(maxttl))
        self.key = "test_collections"
        start = self.bucket.scopes[CbServer.default_scope] \
            .collections[CbServer.default_collection] \
            .num_items
        load_gen = doc_generator(self.key, start, start + 1)
        tasks = []
        tasks.append(self.task.async_load_gen_docs(
            self.cluster, self.bucket, load_gen, "create", maxttl,
            batch_size=1000, process_concurrency=8,
            timeout_secs=60,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            active_resident_threshold=self.dgm,
            compression=self.sdk_compression,
            scope=CbServer.default_scope,
            collection=CbServer.default_collection))
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)
            if task.fail:
                self.fail("preload dgm failed")
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.print_bucket_stats()

    def data_load_after_failover(self):
        self.log.info("Starting a sync data load after failover")
        self.subsequent_data_load()  # sync data load
        # Until we recover/rebalance-out, we can't call - self.bucket_util.validate_docs_per_collections_all_buckets()
        self.bucket_util._wait_for_stats_all_buckets()

    def wait_for_failover_or_assert(self, expected_failover_count, timeout=180):
        time_start = time.time()
        time_max_end = time_start + timeout
        actual_failover_count = 0
        while time.time() < time_max_end:
            actual_failover_count = self.get_failover_count()
            if actual_failover_count == expected_failover_count:
                break
            time.sleep(20)
        time_end = time.time()
        if actual_failover_count != expected_failover_count:
            self.log.info(self.rest.print_UI_logs())
        self.assertTrue(actual_failover_count == expected_failover_count,
                        "{0} nodes failed over, expected : {1}"
                        .format(actual_failover_count,
                                expected_failover_count))
        self.log.info("{0} nodes failed over as expected in {1} seconds"
                      .format(actual_failover_count, time_end - time_start))

    def get_failover_count(self):
        rest = RestConnection(self.cluster.master)
        cluster_status = rest.cluster_status()
        failover_count = 0
        # check for inactiveFailed
        for node in cluster_status['nodes']:
            if node['clusterMembership'] == "inactiveFailed":
                failover_count += 1
        return failover_count

    def forced_failover_operation(self, known_nodes=None, failover_nodes=None, wait_for_pending=120):
        self.log.info("Updating all the bucket replicas to {0}".format(self.updated_num_replicas))
        self.bucket_util.update_all_bucket_replicas(self.updated_num_replicas)
        failover_count = 0
        for failover_node in failover_nodes:
            failover_operation = self.task.failover(known_nodes, failover_nodes=[failover_node],
                                                    graceful=False, wait_for_pending=wait_for_pending)
            failover_count = failover_count + 1
            self.wait_for_failover_or_assert(failover_count)
        operation = self.task.async_rebalance(known_nodes, [], failover_nodes)
        self.execute_N1qltxn()
        self.data_load_after_failover()
        return operation

    def rebalance_operation(self, rebalance_operation, known_nodes=None, add_nodes=None, remove_nodes=None,
                            failover_nodes=None, wait_for_pending=120, tasks=None):
        self.log.info("Starting rebalance operation of type : {0}".format(rebalance_operation))
        step_count = self.step_count
        if rebalance_operation == "rebalance_out":
            if step_count == -1:
                if self.warmup:
                    node = known_nodes[-1]
                    self.warmup_node(node)
                    operation = self.task.async_rebalance(known_nodes, [], remove_nodes)
                    self.execute_N1qltxn(remove_nodes[0])
                    self.task.jython_task_manager.get_task_result(operation)
                    if not operation.result:
                        self.log.info("rebalance was failed as expected")
                        for bucket in self.bucket_util.buckets:
                            self.assertTrue(self.bucket_util._wait_warmup_completed(
                                [node], bucket))
                        self.log.info("second attempt to rebalance")
                        self.sleep(60, "wait before starting rebalance after warmup")
                        operation = self.task.async_rebalance(known_nodes, [], remove_nodes)
                        self.wait_for_rebalance_to_complete(operation)
                    self.sleep(60)
                else:
                    if self.update_replica:
                        self.log.info("Updating all the bucket replicas to {0}".format(self.updated_num_replicas))
                        self.bucket_util.update_all_bucket_replicas(self.updated_num_replicas)
                        self.bucket_util.print_bucket_stats()
                    # all at once
                    operation = self.task.async_rebalance(known_nodes, [], remove_nodes)
                    self.execute_N1qltxn(remove_nodes[0])
                    if self.compaction:
                        self.compact_all_buckets()
                    if self.change_ram_quota_cluster:
                        self.set_ram_quota_cluster()
            else:
                # list of lists each of length step_count
                remove_list = []
                for i in range(0, len(remove_nodes), step_count):
                    if i + step_count >= len(remove_nodes):
                        remove_list.append(remove_nodes[i:])
                    else:
                        remove_list.append(remove_nodes[i:i + step_count])
                iter_count = 0
                # start each intermediate rebalance and wait for it to finish before
                # starting new one
                for new_remove_nodes in remove_list:
                    operation = self.task.async_rebalance(known_nodes, [], new_remove_nodes)
                    known_nodes = [node for node in known_nodes if node not in new_remove_nodes]
                    iter_count = iter_count + 1
                    # if this is last intermediate rebalance, don't wait
                    if iter_count == len(remove_list):
                        continue
                    self.wait_for_rebalance_to_complete(operation)
        elif rebalance_operation == "rebalance_in":
            if step_count == -1:
                if self.warmup:
                    node = known_nodes[-1]
                    self.warmup_node(node)
                    operation = self.task.async_rebalance(known_nodes, add_nodes, [])
                    self.execute_N1qltxn()
                    self.task.jython_task_manager.get_task_result(operation)
                    if not operation.result:
                        self.log.info("rebalance was failed as expected")
                        for bucket in self.bucket_util.buckets:
                            self.assertTrue(self.bucket_util._wait_warmup_completed(
                                [node], bucket))
                        self.log.info("second attempt to rebalance")
                        self.sleep(60, "wait before starting rebalance after warmup")
                        operation = self.task.async_rebalance(known_nodes + add_nodes, [], [])
                        self.wait_for_rebalance_to_complete(operation)
                    self.sleep(60)
                else:
                    if self.update_replica:
                        self.log.info("Updating all the bucket replicas to {0}".format(self.updated_num_replicas))
                        self.bucket_util.update_all_bucket_replicas(self.updated_num_replicas)
                        self.bucket_util.print_bucket_stats()
                    # all at once
                    operation = self.task.async_rebalance(known_nodes, add_nodes, [])
                    self.execute_N1qltxn()
                    if self.compaction:
                        self.compact_all_buckets()
                    if self.change_ram_quota_cluster:
                        self.set_ram_quota_cluster()
            else:
                # list of lists each of length step_count
                add_list = []
                for i in range(0, len(add_nodes), step_count):
                    if i + step_count >= len(add_nodes):
                        add_list.append(add_nodes[i:])
                    else:
                        add_list.append(add_nodes[i:i + step_count])
                iter_count = 0
                # start each intermediate rebalance and wait for it to finish before
                # starting new one
                for new_add_nodes in add_list:
                    operation = self.task.async_rebalance(known_nodes, new_add_nodes, [])
                    known_nodes.append(new_add_nodes)
                    iter_count = iter_count + 1
                    # if this is last intermediate rebalance, don't wait
                    if iter_count == len(add_list):
                        continue
                    self.wait_for_rebalance_to_complete(operation)
        elif rebalance_operation == "swap_rebalance":
            if (step_count == -1):
                if self.warmup:
                    for node in add_nodes:
                        self.rest.add_node(self.cluster.master.rest_username, self.cluster.master.rest_password,
                                           node.ip, self.cluster.servers[self.nodes_init].port)
                    node = known_nodes[-1]
                    self.warmup_node(node)
                    operation = self.task.async_rebalance(self.cluster.servers[:self.nodes_init], [], remove_nodes,
                                                          check_vbucket_shuffling=False)
                    self.execute_N1qltxn(remove_nodes[0])
                    self.task.jython_task_manager.get_task_result(operation)
                    if not operation.result:
                        self.log.info("rebalance was failed as expected")
                        for bucket in self.bucket_util.buckets:
                            self.assertTrue(self.bucket_util._wait_warmup_completed(
                                [node], bucket))
                        self.log.info("second attempt to rebalance")
                        self.sleep(60, "wait before starting rebalance after warmup")
                        operation = self.task.async_rebalance(self.cluster.servers[:self.nodes_init], [], remove_nodes)
                        self.wait_for_rebalance_to_complete(operation)
                    self.sleep(60)
                else:
                    if self.update_replica:
                        self.log.info("Updating all the bucket replicas to {0}".format(self.updated_num_replicas))
                        self.bucket_util.update_all_bucket_replicas(self.updated_num_replicas)
                        self.bucket_util.print_bucket_stats()
                    for node in add_nodes:
                        self.rest.add_node(self.cluster.master.rest_username, self.cluster.master.rest_password,
                                           node.ip, self.cluster.servers[self.nodes_init].port)
                    operation = self.task.async_rebalance(self.cluster.servers[:self.nodes_init], [], remove_nodes,
                                                          check_vbucket_shuffling=False)
                    self.execute_N1qltxn(remove_nodes[0])
                    if self.compaction:
                        self.compact_all_buckets()
                    if self.change_ram_quota_cluster:
                        self.set_ram_quota_cluster()
            else:
                # list of lists each of length step_count
                add_list = []
                remove_list = []
                for i in range(0, len(add_nodes), step_count):
                    if i + step_count >= len(add_nodes):
                        add_list.append(add_nodes[i:])
                        remove_list.append(remove_nodes[i:])
                    else:
                        add_list.append(add_nodes[i:i + step_count])
                        remove_list.append(remove_nodes[i:i + step_count])
                iter_count = 0
                # start each intermediate rebalance and wait for it to finish before
                # starting new one
                for new_add_nodes, new_remove_nodes in zip(add_list, remove_list):
                    operation = self.task.async_rebalance(known_nodes, new_add_nodes, new_remove_nodes,
                                                          check_vbucket_shuffling=False)
                    known_nodes = [node for node in known_nodes if node not in new_remove_nodes]
                    known_nodes.extend(new_add_nodes)
                    iter_count = iter_count + 1
                    # if this is last intermediate rebalance, don't wait
                    if iter_count == len(add_list):
                        continue
                    self.wait_for_rebalance_to_complete(operation)
        elif rebalance_operation == "rebalance_in_out":
            if self.warmup:
                for node in add_nodes:
                    self.rest.add_node(self.cluster.master.rest_username, self.cluster.master.rest_password,
                                       node.ip, self.cluster.servers[self.nodes_init].port)
                node = known_nodes[-1]
                self.warmup_node(node)
                operation = self.task.async_rebalance(self.cluster.servers[:self.nodes_init], [], remove_nodes)
                self.execute_N1qltxn(remove_nodes[0])
                self.task.jython_task_manager.get_task_result(operation)
                if not operation.result:
                    self.log.info("rebalance was failed as expected")
                    for bucket in self.bucket_util.buckets:
                        self.assertTrue(self.bucket_util._wait_warmup_completed(
                            [node], bucket))
                    self.log.info("second attempt to rebalance")
                    self.sleep(60, "wait before starting rebalance after warmup")
                    operation = self.task.async_rebalance(self.cluster.servers[:self.nodes_init], [], remove_nodes)
                    self.wait_for_rebalance_to_complete(operation)
                self.sleep(60)
            else:
                if self.update_replica:
                    self.log.info("Updating all the bucket replicas to {0}".format(self.updated_num_replicas))
                    self.bucket_util.update_all_bucket_replicas(self.updated_num_replicas)
                    self.bucket_util.print_bucket_stats()
                for node in add_nodes:
                    self.rest.add_node(self.cluster.master.rest_username, self.cluster.master.rest_password,
                                       node.ip, self.cluster.servers[self.nodes_init].port)
                operation = self.task.async_rebalance(self.cluster.servers[:self.nodes_init], [], remove_nodes)
                self.execute_N1qltxn(remove_nodes[0])
                if self.compaction:
                    self.compact_all_buckets()
                if self.change_ram_quota_cluster:
                    self.set_ram_quota_cluster()
        elif rebalance_operation == "graceful_failover_rebalance_out":
            if step_count == -1:
                failover_count = 0
                for failover_node in failover_nodes:
                    failover_operation = self.task.failover(known_nodes, failover_nodes=[failover_node],
                                                            graceful=True, wait_for_pending=wait_for_pending)
                    failover_count = failover_count + 1
                    self.wait_for_failover_or_assert(failover_count)
                if tasks is not None:
                    self.wait_for_async_data_load_to_complete(tasks)
                if self.compaction:
                    self.compact_all_buckets()
                self.data_load_after_failover()
                operation = self.task.async_rebalance(known_nodes, [], failover_nodes)
                self.execute_N1qltxn(failover_nodes[0])
                if self.change_ram_quota_cluster:
                    self.set_ram_quota_cluster()
            else:
                # list of lists each of length step_count
                failover_list = []
                for i in range(0, len(failover_nodes), step_count):
                    if i + step_count >= len(failover_nodes):
                        failover_list.append(failover_nodes[i:])
                    else:
                        failover_list.append(failover_nodes[i:i + step_count])
                # For each set of step_count number of failover nodes we failover and rebalance them out
                iter_count = 0
                for new_failover_nodes in failover_list:
                    failover_count = 0
                    self.execute_N1qltxn(new_failover_nodes[0])
                    for failover_node in new_failover_nodes:
                        failover_operation = self.task.failover(known_nodes, failover_nodes=[failover_node],
                                                                graceful=True, wait_for_pending=wait_for_pending)
                        failover_count = failover_count + 1
                        self.wait_for_failover_or_assert(failover_count)
                    if tasks is not None:
                        self.wait_for_async_data_load_to_complete(tasks)
                        tasks = None
                    self.data_load_after_failover()
                    operation = self.task.async_rebalance(known_nodes, [], new_failover_nodes)
                    iter_count = iter_count + 1
                    known_nodes = [node for node in known_nodes if node not in new_failover_nodes]
                    if iter_count == len(failover_list):
                        continue
                    self.wait_for_rebalance_to_complete(operation)
        elif rebalance_operation == "hard_failover_rebalance_out":
            if step_count == -1:
                failover_count = 0
                for failover_node in failover_nodes:
                    failover_operation = self.task.failover(known_nodes, failover_nodes=[failover_node],
                                                            graceful=False, wait_for_pending=wait_for_pending)
                    failover_count = failover_count + 1
                    self.wait_for_failover_or_assert(failover_count)
                if tasks is not None:
                    self.wait_for_async_data_load_to_complete(tasks)
                if self.compaction:
                    self.compact_all_buckets()
                self.data_load_after_failover()
                operation = self.task.async_rebalance(known_nodes, [], failover_nodes)
                self.execute_N1qltxn(failover_nodes[0])
                if self.change_ram_quota_cluster:
                    self.set_ram_quota_cluster()
            else:
                # list of lists each of length step_count
                failover_list = []
                for i in range(0, len(failover_nodes), step_count):
                    if i + step_count >= len(failover_nodes):
                        failover_list.append(failover_nodes[i:])
                    else:
                        failover_list.append(failover_nodes[i:i + step_count])
                # For each set of step_count number of failover nodes we failover and rebalance them out
                iter_count = 0
                for new_failover_nodes in failover_list:
                    failover_count = 0
                    for failover_node in new_failover_nodes:
                        failover_operation = self.task.failover(known_nodes, failover_nodes=[failover_node],
                                                                graceful=False, wait_for_pending=wait_for_pending)
                        failover_count = failover_count + 1
                        self.wait_for_failover_or_assert(failover_count)
                    if tasks is not None:
                        self.wait_for_async_data_load_to_complete(tasks)
                        tasks = None
                    self.data_load_after_failover()
                    operation = self.task.async_rebalance(known_nodes, [], new_failover_nodes)
                    self.execute_N1qltxn(new_failover_nodes[0])
                    iter_count = iter_count + 1
                    known_nodes = [node for node in known_nodes if node not in new_failover_nodes]
                    if iter_count == len(failover_list):
                        continue
                    self.wait_for_rebalance_to_complete(operation)
        elif rebalance_operation == "graceful_failover_recovery":
            if (step_count == -1):
                failover_count = 0
                for failover_node in failover_nodes:
                    failover_operation = self.task.failover(known_nodes, failover_nodes=[failover_node],
                                                            graceful=True, wait_for_pending=wait_for_pending)
                    failover_count = failover_count + 1
                    self.wait_for_failover_or_assert(failover_count)
                    self.execute_N1qltxn(failover_node)
                if tasks is not None:
                    self.wait_for_async_data_load_to_complete(tasks)
                self.data_load_after_failover()
                # Mark the failover nodes for recovery
                for failover_node in failover_nodes:
                    self.rest.set_recovery_type(otpNode='ns_1@' + failover_node.ip, recoveryType=self.recovery_type)
                if self.compaction:
                    self.compact_all_buckets()
                # Rebalance all the nodes
                operation = self.task.async_rebalance(known_nodes, [], [])
                if self.change_ram_quota_cluster:
                    self.set_ram_quota_cluster()
            else:
                # list of lists each of length step_count
                failover_list = []
                for i in range(0, len(failover_nodes), step_count):
                    if i + step_count >= len(failover_nodes):
                        failover_list.append(failover_nodes[i:])
                    else:
                        failover_list.append(failover_nodes[i:i + step_count])
                # For each set of step_count number of failover nodes we failover and recover
                iter_count = 0
                for new_failover_nodes in failover_list:
                    failover_count = 0
                    for failover_node in new_failover_nodes:
                        failover_operation = self.task.failover(known_nodes, failover_nodes=[failover_node],
                                                                graceful=True, wait_for_pending=wait_for_pending)

                        failover_count = failover_count + 1
                        self.wait_for_failover_or_assert(failover_count)
                        self.execute_N1qltxn(failover_node)
                    if tasks is not None:
                        self.wait_for_async_data_load_to_complete(tasks)
                        tasks = None
                    self.data_load_after_failover()
                    # Mark the failover nodes for recovery
                    for failover_node in new_failover_nodes:
                        self.rest.set_recovery_type(otpNode='ns_1@' + failover_node.ip,
                                                    recoveryType=self.recovery_type)
                    operation = self.task.async_rebalance(known_nodes, [], [])
                    iter_count = iter_count + 1
                    if iter_count == len(failover_list):
                        continue
                    self.wait_for_rebalance_to_complete(operation)
        elif rebalance_operation == "hard_failover_recovery":
            if (step_count == -1):
                failover_count = 0
                for failover_node in failover_nodes:
                    failover_operation = self.task.failover(known_nodes, failover_nodes=[failover_node],
                                                            graceful=False, wait_for_pending=wait_for_pending)
                    failover_count = failover_count + 1
                    self.wait_for_failover_or_assert(failover_count)
                    self.execute_N1qltxn(failover_node)
                if tasks is not None:
                    self.wait_for_async_data_load_to_complete(tasks)
                self.data_load_after_failover()
                # Mark the failover nodes for recovery
                for failover_node in failover_nodes:
                    self.rest.set_recovery_type(otpNode='ns_1@' + failover_node.ip, recoveryType=self.recovery_type)
                if self.compaction:
                    self.compact_all_buckets()
                # Rebalance all the nodes
                operation = self.task.async_rebalance(known_nodes, [], [])
                if self.change_ram_quota_cluster:
                    self.set_ram_quota_cluster()
            else:
                # list of lists each of length step_count
                failover_list = []
                for i in range(0, len(failover_nodes), step_count):
                    if i + step_count >= len(failover_nodes):
                        failover_list.append(failover_nodes[i:])
                    else:
                        failover_list.append(failover_nodes[i:i + step_count])
                # For each set of step_count number of failover nodes we failover and recover
                iter_count = 0
                for new_failover_nodes in failover_list:
                    failover_count = 0
                    for failover_node in new_failover_nodes:
                        failover_operation = self.task.failover(known_nodes, failover_nodes=[failover_node],
                                                                graceful=False, wait_for_pending=wait_for_pending)

                        failover_count = failover_count + 1
                        self.wait_for_failover_or_assert(failover_count)
                        self.execute_N1qltxn(failover_node)
                    if tasks is not None:
                        self.wait_for_async_data_load_to_complete(tasks)
                        tasks = None
                    self.data_load_after_failover()
                    # Mark the failover nodes for recovery
                    for failover_node in new_failover_nodes:
                        self.rest.set_recovery_type(otpNode='ns_1@' + failover_node.ip,
                                                    recoveryType=self.recovery_type)
                    operation = self.task.async_rebalance(known_nodes, [], [])
                    iter_count = iter_count + 1
                    if iter_count == len(failover_list):
                        continue
                    self.wait_for_rebalance_to_complete(operation)
        else:
            self.fail("rebalance_operation is not defined")
        return operation

    def subsequent_data_load(self, async_load=False, data_load_spec=None):
        if data_load_spec is None:
            data_load_spec = self.data_load_spec
        doc_loading_spec = self.bucket_util.get_crud_template_from_package(data_load_spec)
        self.over_ride_doc_loading_template_params(doc_loading_spec)
        self.set_retry_exceptions(doc_loading_spec)
        if self.dgm < 100:
            # No new items are created during dgm + rebalance/failover tests
            doc_loading_spec["doc_crud"][MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 0
            doc_loading_spec["doc_crud"][MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS] = 0
        if self.forced_hard_failover and self.spec_name == "multi_bucket.buckets_for_rebalance_tests_more_collections":
            # create collections, else if other bucket_spec - then just "create" ops
            doc_loading_spec[MetaCrudParams.COLLECTIONS_TO_ADD_PER_BUCKET] = 20
        tasks = self.bucket_util.run_scenario_from_spec(self.task,
                                                        self.cluster,
                                                        self.bucket_util.buckets,
                                                        doc_loading_spec,
                                                        mutation_num=0,
                                                        async_load=async_load,
                                                        batch_size=self.batch_size,
                                                        validate_task=(not self.skip_validations))
        return tasks

    def async_data_load(self):
        tasks = self.subsequent_data_load(async_load=True)
        return tasks

    def sync_data_load(self):
        self.subsequent_data_load()

    def wait_for_async_data_load_to_complete(self, task):
        self.task.jython_task_manager.get_task_result(task)
        if not self.skip_validations:
            self.bucket_util.validate_doc_loading_results(task)
            if task.result is False:
                self.fail("Doc_loading failed")

    def wait_for_compaction_to_complete(self):
        for task in self.compaction_tasks:
            self.task_manager.get_task_result(task)
            self.assertTrue(task.result, "Compaction failed for bucket: %s" %
                            task.bucket.name)

    def wait_for_rebalance_to_complete(self, task):
        self.task.jython_task_manager.get_task_result(task)
        self.assertTrue(task.result, "Rebalance Failed")
        if self.compaction:
            self.wait_for_compaction_to_complete()

    def data_validation_collection(self):
        if not self.skip_validations:
            if self.data_load_spec == "ttl_load" or self.data_load_spec == "ttl_load1":
                self.bucket_util._expiry_pager()
                self.sleep(400, "wait for maxttl to finish")
                # Compact buckets to delete non-resident expired items
                self.compact_all_buckets()
                self.wait_for_compaction_to_complete()
                self.sleep(60, "wait after compaction")
                items = 0
                self.bucket_util._wait_for_stats_all_buckets()
                for bucket in self.bucket_util.buckets:
                    items = items + self.bucket_helper_obj.get_active_key_count(bucket)
                if items != 0:
                    self.fail("Items did not go to 0")
            elif self.forced_hard_failover:
                pass
            else:
                self.bucket_util._wait_for_stats_all_buckets()
                self.bucket_util.validate_docs_per_collections_all_buckets()

    def load_collections_with_rebalance(self, rebalance_operation):
        tasks = None
        rebalance = None
        self.log.info("Doing collection data load {0} {1}".format(self.data_load_stage, rebalance_operation))
        if self.data_load_stage == "before":
            if self.data_load_type == "async":
                tasks = self.async_data_load()
            else:
                self.sync_data_load()
        if self.dgm < 100:
            self.load_to_dgm()
        if self.N1ql_txn:
            self.setup_N1ql_txn()
        if rebalance_operation == "rebalance_in":
            rebalance = self.rebalance_operation(rebalance_operation="rebalance_in",
                                                 known_nodes=self.cluster.servers[:self.nodes_init],
                                                 add_nodes=self.cluster.servers[
                                                           self.nodes_init:self.nodes_init + self.nodes_in],
                                                 tasks=tasks)

        elif rebalance_operation == "rebalance_out":
            rebalance = self.rebalance_operation(rebalance_operation="rebalance_out",
                                                 known_nodes=self.cluster.servers[:self.nodes_init],
                                                 remove_nodes=self.cluster.servers[:self.nodes_init][-self.nodes_out:],
                                                 tasks=tasks)
        elif rebalance_operation == "swap_rebalance":
            rebalance = self.rebalance_operation(rebalance_operation="swap_rebalance",
                                                 known_nodes=self.cluster.servers[:self.nodes_init],
                                                 add_nodes=self.cluster.servers[
                                                           self.nodes_init:self.nodes_init + self.nodes_swap],
                                                 remove_nodes=self.cluster.servers[:self.nodes_init][-self.nodes_swap:],
                                                 tasks=tasks)
        elif rebalance_operation == "rebalance_in_out":
            rebalance = self.rebalance_operation(rebalance_operation="rebalance_in_out",
                                                 known_nodes=self.cluster.servers[:self.nodes_init],
                                                 add_nodes=self.cluster.servers[
                                                           self.nodes_init:self.nodes_init + self.nodes_in],
                                                 remove_nodes=self.cluster.servers[:self.nodes_init][-self.nodes_out:],
                                                 tasks=tasks)
        elif rebalance_operation == "graceful_failover_rebalance_out":
            rebalance = self.rebalance_operation(rebalance_operation="graceful_failover_rebalance_out",
                                                 known_nodes=self.cluster.servers[:self.nodes_init],
                                                 failover_nodes=self.cluster.servers[:self.nodes_init]
                                                 [-self.nodes_failover:],
                                                 tasks=tasks)
        elif rebalance_operation == "hard_failover_rebalance_out":
            rebalance = self.rebalance_operation(rebalance_operation="hard_failover_rebalance_out",
                                                 known_nodes=self.cluster.servers[:self.nodes_init],
                                                 failover_nodes=self.cluster.servers[:self.nodes_init]
                                                 [-self.nodes_failover:],
                                                 tasks=tasks)
        elif rebalance_operation == "graceful_failover_recovery":
            rebalance = self.rebalance_operation(rebalance_operation="graceful_failover_recovery",
                                                 known_nodes=self.cluster.servers[:self.nodes_init],
                                                 failover_nodes=self.cluster.servers[:self.nodes_init]
                                                 [-self.nodes_failover:],
                                                 tasks=tasks)
        elif rebalance_operation == "hard_failover_recovery":
            rebalance = self.rebalance_operation(rebalance_operation="hard_failover_recovery",
                                                 known_nodes=self.cluster.servers[:self.nodes_init],
                                                 failover_nodes=self.cluster.servers[:self.nodes_init]
                                                 [-self.nodes_failover:],
                                                 tasks=tasks)
        elif rebalance_operation == "forced_hard_failover_rebalance_out":
            rebalance = self.forced_failover_operation(known_nodes=self.cluster.servers[:self.nodes_init],
                                                       failover_nodes=self.cluster.servers[:self.nodes_init]
                                                       [-self.nodes_failover:]
                                                       )

        if self.data_load_stage == "during":
            if self.data_load_type == "async":
                tasks = self.async_data_load()
            else:
                self.sync_data_load()
        if not self.warmup:
            self.wait_for_rebalance_to_complete(rebalance)
        if self.data_load_stage == "during" or self.data_load_stage == "before":
            if self.data_load_type == "async":
                # for failover + before + async, wait_for_async_data_load_to_complete is already done
                if self.data_load_stage == "before" and rebalance_operation in self.failover_ops:
                    pass
                else:
                    self.wait_for_async_data_load_to_complete(tasks)
            self.data_validation_collection()
        if self.data_load_stage == "after":
            self.sync_data_load()
            self.data_validation_collection()
        if self.N1ql_txn:
            self.validate_N1qltxn_data()

    def test_data_load_collections_with_rebalance_in(self):
        self.load_collections_with_rebalance(rebalance_operation="rebalance_in")

    def test_data_load_collections_with_rebalance_out(self):
        self.load_collections_with_rebalance(rebalance_operation="rebalance_out")

    def test_data_load_collections_with_swap_rebalance(self):
        self.load_collections_with_rebalance(rebalance_operation="swap_rebalance")

    def test_data_load_collections_with_rebalance_in_out(self):
        self.load_collections_with_rebalance(rebalance_operation="rebalance_in_out")

    def test_data_load_collections_with_graceful_failover_rebalance_out(self):
        self.load_collections_with_rebalance(rebalance_operation="graceful_failover_rebalance_out")

    def test_data_load_collections_with_hard_failover_rebalance_out(self):
        self.load_collections_with_rebalance(rebalance_operation="hard_failover_rebalance_out")

    def test_data_load_collections_with_graceful_failover_recovery(self):
        self.load_collections_with_rebalance(rebalance_operation="graceful_failover_recovery")

    def test_data_load_collections_with_hard_failover_recovery(self):
        self.load_collections_with_rebalance(rebalance_operation="hard_failover_recovery")

    def test_data_load_collections_with_forced_hard_failover_rebalance_out(self):
        self.load_collections_with_rebalance(rebalance_operation="forced_hard_failover_rebalance_out")
