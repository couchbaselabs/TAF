import time

from BucketLib.BucketOperations import BucketHelper
from Cb_constants import CbServer
from collections_helper.collections_spec_constants import MetaCrudParams
from couchbase_helper.documentgenerator import doc_generator
from bucket_collections.collections_base import CollectionBase

from couchbase_helper.tuq_helper import N1QLHelper
from pytests.N1qlTransaction.N1qlBase import N1qlBase

from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from StatsLib.StatsOperations import StatsHelper

from sdk_exceptions import SDKException
from FtsLib.FtsOperations import FtsHelper


class CollectionsRebalance(CollectionBase):
    def setUp(self):
        super(CollectionsRebalance, self).setUp()
        self.bucket_util._expiry_pager(self.cluster)
        self.bucket = self.cluster.buckets[0]
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
        self.change_ephemeral_purge_age_and_interval = self.input.param("change_ephemeral_purge_age_and_interval",
                                                                        True)
        if self.change_ephemeral_purge_age_and_interval:
            self.set_ephemeral_purge_age_and_interval()
        self.skip_validations = self.input.param("skip_validations", True)
        self.compaction_tasks = list()
        self.dgm_ttl_test = self.input.param("dgm_ttl_test", False)  # if dgm with ttl
        self.dgm = self.input.param("dgm", "100")  # Initial dgm threshold, for dgm test; 100 means no dgm
        self.scrape_interval = self.input.param("scrape_interval", None)
        self.sleep_before_validation_of_ttl = self.input.param("sleep_before_validation_of_ttl", 400)
        if self.scrape_interval:
            self.log.info("Changing scrape interval to {0}".format(self.scrape_interval))
            # scrape_timeout cannot be greater than scrape_interval,
            # so for now we are setting scrape_timeout same as scrape_interval
            StatsHelper(self.cluster.master).change_scrape_timeout(self.scrape_interval)
            StatsHelper(self.cluster.master).change_scrape_interval(self.scrape_interval)
        self.rebalance_moves_per_node = self.input.param("rebalance_moves_per_node", None)
        if self.rebalance_moves_per_node:
            self.cluster_util.set_rebalance_moves_per_nodes(
                self.cluster.master,
                rebalanceMovesPerNode=self.rebalance_moves_per_node)
        self.N1ql_txn = self.input.param("N1ql_txn", False)
        if self.N1ql_txn:
            self.num_stmt_txn = self.input.param("num_stmt_txn", 5)
            self.num_collection = self.input.param("num_collection", 1)
            self.num_savepoints = self.input.param("num_savepoints", 0)
            self.override_savepoint = self.input.param("override_savepoint", 0)
            self.num_buckets = self.input.param("num_buckets", 1)
        self.create_metakv_entries = self.input.param("create_metakv_entries", False)
        if self.create_metakv_entries:
            self.log.info("Creating metakv entries start")
            self.load_metakv_entries_using_fts()
            self.log.info("Creating metakv entries end")

    def tearDown(self):
        self.bucket_util.print_bucket_stats(self.cluster)
        if self.scrape_interval:
            self.log.info("Reverting prometheus settings back to default")
            StatsHelper(self.cluster.master).reset_stats_settings_from_diag_eval()
        if self.rebalance_moves_per_node:
            self.cluster_util.set_rebalance_moves_per_nodes(
                self.cluster.master,
                rebalanceMovesPerNode=4)
        super(CollectionsRebalance, self).tearDown()

    def setup_N1ql_txn(self):
        self.n1ql_server = self.cluster_util.get_nodes_from_services_map(
            cluster=self.cluster,
            service_type=CbServer.Services.N1QL,
            get_all_nodes=True)
        self.n1ql_helper = N1QLHelper(server=self.n1ql_server,
                                      use_rest=True,
                                      buckets=self.cluster.buckets,
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
                self.server = server
            else:
                self.server = None
            self.retry_n1qltxn = False
            self.sleep(5, "wait for rebalance to start")
            self.n1ql_fun = N1qlBase()
            try:
                query_params = self.n1ql_helper.create_txn(server=self.server, txtimeout=2)
                self.collection_savepoint, self.savepoints, self.queries, rerun = \
                    self.n1ql_fun.full_execute_query(self.stmts, True, query_params,
                                                     N1qlhelper=self.n1ql_helper, server=self.server)
                if not isinstance(self.collection_savepoint, dict):
                    self.log.info("N1ql txn failed will be retried")
                    self.retry_n1qltxn = True
            except Exception as error:
                self.log.info("error is %s" % error)
                self.retry_n1qltxn = True

    def load_metakv_entries_using_fts(self):
        self.fts_index_partitions = self.input.param("fts_index_partition", 6)
        self.fts_indexes_to_create_drop = self.input.param("fts_indexes_to_create_drop", 500)

        def create_fts_index(bucket_obj, t_index, s_name, c_name):
            fts_index_name = "%s_fts_%d" % (bucket_obj.name.replace(".", ""), t_index)
            status, content = fts_helper.create_fts_index_from_json(
                fts_index_name,
                fts_param_template % (fts_index_name,
                                      bucket_obj.name,
                                      self.fts_index_partitions,
                                      s_name, c_name))
            if status is False:
                self.fail("Failed to create fts index %s: %s"
                          % (fts_index_name, content))

        def drop_fts_index(b_name, t_index):
            fts_index_name = "%s_fts_%d" % (b_name.replace(".", ""), t_index)
            status, content = fts_helper.delete_fts_index(fts_index_name)
            if status is False:
                self.fail("Failed to drop fts index %s: %s"
                          % (fts_index_name, content))

        fts_helper = FtsHelper(self.cluster_util.get_nodes_from_services_map(
            cluster=self.cluster,
            service_type=CbServer.Services.FTS,
            get_all_nodes=False))
        fts_param_template = '{ \
          "type": "fulltext-index", \
          "name": "%s", \
          "sourceType": "gocbcore", \
          "sourceName": "%s", \
          "planParams": { \
            "maxPartitionsPerPIndex": 1024, \
            "indexPartitions": %d \
          }, \
          "params": { \
            "doc_config": { \
              "docid_prefix_delim": "", \
              "docid_regexp": "", \
              "mode": "scope.collection.type_field", \
              "type_field": "type" \
            }, \
            "mapping": { \
              "analysis": {}, \
              "default_analyzer": "standard", \
              "default_datetime_parser": "dateTimeOptional", \
              "default_field": "_all", \
              "default_mapping": { \
                "dynamic": true, \
                "enabled": false \
              }, \
              "default_type": "_default", \
              "docvalues_dynamic": false, \
              "index_dynamic": true, \
              "store_dynamic": false, \
              "type_field": "_type", \
              "types": { \
                "%s.%s": { \
                  "dynamic": true, \
                  "enabled": true \
                } \
              } \
            }, \
            "store": { \
              "indexType": "scorch", \
              "segmentVersion": 15 \
            } \
          }, \
          "sourceParams": {} \
        }'

        bucket = self.cluster.buckets[0]
        for index in range(0, self.fts_indexes_to_create_drop):
            random_scope_collection = \
                self.bucket_util.get_random_collections([bucket], 1, 1, 1)[bucket.name]["scopes"]
            scope_name = random_scope_collection.keys()[0]
            col_name = \
                random_scope_collection[scope_name]["collections"].keys()[0]
            create_fts_index(bucket, index, scope_name, col_name)
            drop_fts_index(bucket.name, index)

    def validate_N1qltxn_data(self):
        if self.retry_n1qltxn:
            self.n1ql_server = self.cluster_util.get_nodes_from_services_map(
                cluster=self.cluster,
                service_type=CbServer.Services.N1QL,
                get_all_nodes=True)
            self.execute_N1qltxn(self.n1ql_server[0])
        doc_gen_list = self.n1ql_helper.get_doc_gen_list(self.bucket_col)
        if isinstance(self.collection_savepoint, dict):
            results = [[self.collection_savepoint, self.savepoints]]
            self.log.info("queries ran are %s" % self.queries)
            self.n1ql_fun.process_value_for_verification(self.bucket_col,
                                                         doc_gen_list, results,
                                                         buckets=self.cluster.buckets)
        else:
            self.fail(self.collection_savepoint)

    def disable_auto_compaction(self):
        buckets = self.bucket_util.get_all_buckets(self.cluster)
        for bucket in buckets:
            if bucket.bucketType == "couchbase":
                self.bucket_util.disable_compaction(self.cluster,
                                                    bucket=bucket.name)

    def compact_all_buckets(self):
        self.sleep(10, "wait for rebalance to start")
        self.log.info("Starting compaction for each bucket")
        for bucket in self.cluster.buckets:
            self.compaction_tasks.append(self.task.async_compact_bucket(
                self.cluster.master, bucket))

    def set_ephemeral_purge_age_and_interval(self, ephemeral_metadata_purge_age=0,
                                             ephemeral_metadata_purge_interval=1):
        """
        Enables diag eval on master node and updates the above two parameters
        for all ephemeral buckets on the cluster
        """
        shell = RemoteMachineShellConnection(self.cluster.master)
        shell.enable_diag_eval_on_non_local_hosts()
        shell.disconnect()
        ephemeral_buckets = [bucket for bucket in self.cluster.buckets if bucket.bucketType == "ephemeral"]
        for ephemeral_bucket in ephemeral_buckets:
            status, content = self.rest.set_ephemeral_purge_age_and_interval \
                (bucket=ephemeral_bucket.name,
                 ephemeral_metadata_purge_age=ephemeral_metadata_purge_age,
                 ephemeral_metadata_purge_interval=ephemeral_metadata_purge_interval)
            if not status:
                raise Exception(content)

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
        status = self.rest.set_service_mem_quota(
            {CbServer.Settings.KV_MEM_QUOTA: 2500})
        self.assertTrue(status, "RAM quota wasn't changed")

    def set_retry_exceptions(self, doc_loading_spec):
        retry_exceptions = list()
        if self.data_load_stage == "during" or \
                (self.data_load_stage == "before" and self.data_load_type == "async"):
            retry_exceptions.append(SDKException.AmbiguousTimeoutException)
            retry_exceptions.append(SDKException.TimeoutException)
            retry_exceptions.append(SDKException.RequestCanceledException)
            retry_exceptions.append(SDKException.DocumentNotFoundException)
            # This is needed as most of the magma testing would be done < 10% DGM
            retry_exceptions.append(SDKException.ServerOutOfMemoryException)
            if self.durability_level:
                retry_exceptions.append(SDKException.DurabilityAmbiguousException)
                retry_exceptions.append(SDKException.DurabilityImpossibleException)
        doc_loading_spec[MetaCrudParams.RETRY_EXCEPTIONS] = retry_exceptions

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
        tasks = list()
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

        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets,
                                                     timeout=1200)
        self.bucket_util.print_bucket_stats(self.cluster)

    def data_load_after_failover(self):
        self.log.info("Starting a sync data load after failover")
        self.subsequent_data_load()  # sync data load
        # Until we recover/rebalance-out, we can't call,
        # self.bucket_util.validate_docs_per_collections_all_buckets()
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets,
                                                     timeout=1200)

    def forced_failover_operation(self, known_nodes=None, failover_nodes=None,
                                  wait_for_pending=300):
        self.log.info("Updating all bucket's replica = %s"
                      % self.updated_num_replicas)
        self.bucket_util.update_all_bucket_replicas(self.cluster,
                                                    self.updated_num_replicas)
        self.log.info("failing over nodes {0}".format(failover_nodes))
        for failover_node in failover_nodes:
            result = self.task.failover(known_nodes,
                                        failover_nodes=[failover_node],
                                        graceful=False,
                                        wait_for_pending=wait_for_pending)
            self.assertTrue(result, "Failover of node {0} failed".
                            format(failover_node.ip))
        operation = self.task.async_rebalance(
            known_nodes, [], failover_nodes,
            retry_get_process_num=self.retry_get_process_num)
        self.execute_N1qltxn()
        self.data_load_after_failover()
        return operation

    def rebalance_operation(self, rebalance_operation, known_nodes=None, add_nodes=None, remove_nodes=None,
                            failover_nodes=None, wait_for_pending=300, tasks=None):
        self.log.info("Starting rebalance operation of type : {0}".format(rebalance_operation))
        step_count = self.step_count
        if rebalance_operation == "rebalance_out":
            if step_count == -1:
                if self.warmup:
                    node = known_nodes[-1]
                    self.warmup_node(node)
                    operation = self.task.async_rebalance(known_nodes, [], remove_nodes,
                                                          retry_get_process_num=self.retry_get_process_num)
                    self.execute_N1qltxn(remove_nodes[0])
                    self.task.jython_task_manager.get_task_result(operation)
                    if not operation.result:
                        self.log.info("rebalance was failed as expected")
                        for bucket in self.cluster.buckets:
                            self.assertTrue(self.bucket_util._wait_warmup_completed(
                                [node], bucket))
                        self.log.info("second attempt to rebalance")
                        self.sleep(60, "wait before starting rebalance after warmup")
                        operation = self.task.async_rebalance(known_nodes, [], remove_nodes,
                                                              retry_get_process_num=self.retry_get_process_num)
                        self.wait_for_rebalance_to_complete(operation)
                    self.sleep(60)
                else:
                    if self.update_replica:
                        self.log.info("Updating all the bucket replicas to {0}".format(self.updated_num_replicas))
                        self.bucket_util.update_all_bucket_replicas(
                            self.cluster, self.updated_num_replicas)
                        self.bucket_util.print_bucket_stats(self.cluster)
                    # all at once
                    operation = self.task.async_rebalance(known_nodes, [], remove_nodes,
                                                          retry_get_process_num=self.retry_get_process_num)
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
                    operation = self.task.async_rebalance(known_nodes, [], new_remove_nodes,
                                                          retry_get_process_num=self.retry_get_process_num)
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
                    operation = self.task.async_rebalance(known_nodes, add_nodes, [],
                                                          retry_get_process_num=self.retry_get_process_num)
                    self.execute_N1qltxn()
                    self.task.jython_task_manager.get_task_result(operation)
                    if not operation.result:
                        self.log.info("rebalance was failed as expected")
                        for bucket in self.cluster.buckets:
                            self.assertTrue(self.bucket_util._wait_warmup_completed(
                                [node], bucket))
                        self.log.info("second attempt to rebalance")
                        self.sleep(60, "wait before starting rebalance after warmup")
                        operation = self.task.async_rebalance(known_nodes + add_nodes, [], [],
                                                              retry_get_process_num=self.retry_get_process_num)
                        self.wait_for_rebalance_to_complete(operation)
                    self.sleep(60)
                else:
                    if self.update_replica:
                        self.log.info("Updating all the bucket replicas to {0}".format(self.updated_num_replicas))
                        self.bucket_util.update_all_bucket_replicas(
                            self.cluster, self.updated_num_replicas)
                        self.bucket_util.print_bucket_stats(self.cluster)
                    # all at once
                    operation = self.task.async_rebalance(known_nodes, add_nodes, [],
                                                          retry_get_process_num=self.retry_get_process_num)
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
                    operation = self.task.async_rebalance(known_nodes, new_add_nodes, [],
                                                          retry_get_process_num=self.retry_get_process_num)
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
                                                          check_vbucket_shuffling=False,
                                                          retry_get_process_num=self.retry_get_process_num)
                    self.execute_N1qltxn(remove_nodes[0])
                    self.task.jython_task_manager.get_task_result(operation)
                    if not operation.result:
                        self.log.info("rebalance was failed as expected")
                        for bucket in self.cluster.buckets:
                            self.assertTrue(self.bucket_util._wait_warmup_completed(
                                [node], bucket))
                        self.log.info("second attempt to rebalance")
                        self.sleep(60, "wait before starting rebalance after warmup")
                        operation = self.task.async_rebalance(self.cluster.servers[:self.nodes_init], [], remove_nodes,
                                                              retry_get_process_num=self.retry_get_process_num)
                        self.wait_for_rebalance_to_complete(operation)
                    self.sleep(60)
                else:
                    if self.update_replica:
                        self.log.info("Updating all the bucket replicas to {0}".format(self.updated_num_replicas))
                        self.bucket_util.update_all_bucket_replicas(
                            self.cluster, self.updated_num_replicas)
                        self.bucket_util.print_bucket_stats(self.cluster)
                    for node in add_nodes:
                        self.rest.add_node(self.cluster.master.rest_username, self.cluster.master.rest_password,
                                           node.ip, self.cluster.servers[self.nodes_init].port)
                    operation = self.task.async_rebalance(self.cluster.servers[:self.nodes_init], [], remove_nodes,
                                                          check_vbucket_shuffling=False,
                                                          retry_get_process_num=self.retry_get_process_num)
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
                                                          check_vbucket_shuffling=False,
                                                          retry_get_process_num=self.retry_get_process_num)
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
                operation = self.task.async_rebalance(self.cluster.servers[:self.nodes_init], [], remove_nodes,
                                                      retry_get_process_num=self.retry_get_process_num)
                self.execute_N1qltxn(remove_nodes[0])
                self.task.jython_task_manager.get_task_result(operation)
                if not operation.result:
                    self.log.info("rebalance was failed as expected")
                    for bucket in self.cluster.buckets:
                        self.assertTrue(self.bucket_util._wait_warmup_completed(
                            [node], bucket))
                    self.log.info("second attempt to rebalance")
                    self.sleep(60, "wait before starting rebalance after warmup")
                    operation = self.task.async_rebalance(self.cluster.servers[:self.nodes_init], [], remove_nodes,
                                                          retry_get_process_num=self.retry_get_process_num)
                    self.wait_for_rebalance_to_complete(operation)
                self.sleep(60)
            else:
                if self.update_replica:
                    self.log.info("Updating all the bucket replicas to {0}".format(self.updated_num_replicas))
                    self.bucket_util.update_all_bucket_replicas(
                        self.cluster, self.updated_num_replicas)
                    self.bucket_util.print_bucket_stats(self.cluster)
                for node in add_nodes:
                    self.rest.add_node(self.cluster.master.rest_username, self.cluster.master.rest_password,
                                       node.ip, self.cluster.servers[self.nodes_init].port)
                operation = self.task.async_rebalance(self.cluster.servers[:self.nodes_init], [], remove_nodes,
                                                      retry_get_process_num=self.retry_get_process_num)
                self.execute_N1qltxn(remove_nodes[0])
                if self.compaction:
                    self.compact_all_buckets()
                if self.change_ram_quota_cluster:
                    self.set_ram_quota_cluster()
        elif rebalance_operation == "graceful_failover_rebalance_out":
            if step_count == -1:
                self.log.info("failing over nodes {0}".format(failover_nodes))
                for failover_node in failover_nodes:
                    result = self.task.failover(known_nodes, failover_nodes=[failover_node],
                                                graceful=True, wait_for_pending=wait_for_pending)
                    self.assertTrue(result, "Failover of node {0} failed".
                                    format(failover_node.ip))
                if tasks is not None:
                    self.wait_for_async_data_load_to_complete(tasks)
                if self.compaction:
                    self.compact_all_buckets()
                self.data_load_after_failover()
                operation = self.task.async_rebalance(known_nodes, [], failover_nodes,
                                                      retry_get_process_num=self.retry_get_process_num)
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
                    self.log.info("failing over nodes {0}".format(new_failover_nodes))
                    self.execute_N1qltxn(new_failover_nodes[0])
                    for failover_node in new_failover_nodes:
                        result = self.task.failover(known_nodes, failover_nodes=[failover_node],
                                                    graceful=True, wait_for_pending=wait_for_pending)
                        self.assertTrue(result, "Failover of node {0} failed".
                                        format(failover_node.ip))
                    if tasks is not None:
                        self.wait_for_async_data_load_to_complete(tasks)
                        tasks = None
                    self.data_load_after_failover()
                    operation = self.task.async_rebalance(known_nodes, [], new_failover_nodes,
                                                          retry_get_process_num=self.retry_get_process_num)
                    iter_count = iter_count + 1
                    known_nodes = [node for node in known_nodes if node not in new_failover_nodes]
                    if iter_count == len(failover_list):
                        continue
                    self.wait_for_rebalance_to_complete(operation)
        elif rebalance_operation == "hard_failover_rebalance_out":
            if step_count == -1:
                self.log.info("failing over nodes {0}".format(failover_nodes))
                for failover_node in failover_nodes:
                    result = self.task.failover(known_nodes, failover_nodes=[failover_node],
                                                graceful=False, wait_for_pending=wait_for_pending)
                    self.assertTrue(result, "Failover of node {0} failed".
                                    format(failover_node.ip))
                if tasks is not None:
                    self.wait_for_async_data_load_to_complete(tasks)
                if self.compaction:
                    self.compact_all_buckets()
                self.data_load_after_failover()
                operation = self.task.async_rebalance(known_nodes, [], failover_nodes,
                                                      retry_get_process_num=self.retry_get_process_num)
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
                    self.log.info("failing over nodes {0}".format(new_failover_nodes))
                    for failover_node in new_failover_nodes:
                        result = self.task.failover(known_nodes, failover_nodes=[failover_node],
                                                    graceful=False, wait_for_pending=wait_for_pending)
                        self.assertTrue(result, "Failover of node {0} failed".
                                        format(failover_node.ip))
                    if tasks is not None:
                        self.wait_for_async_data_load_to_complete(tasks)
                        tasks = None
                    self.data_load_after_failover()
                    operation = self.task.async_rebalance(known_nodes, [], new_failover_nodes,
                                                          retry_get_process_num=self.retry_get_process_num)
                    self.execute_N1qltxn(new_failover_nodes[0])
                    iter_count = iter_count + 1
                    known_nodes = [node for node in known_nodes if node not in new_failover_nodes]
                    if iter_count == len(failover_list):
                        continue
                    self.wait_for_rebalance_to_complete(operation)
        elif rebalance_operation == "graceful_failover_recovery":
            if step_count == -1:
                self.log.info("failing over nodes {0}".format(failover_nodes))
                for failover_node in failover_nodes:
                    result = self.task.failover(known_nodes, failover_nodes=[failover_node],
                                                graceful=True, wait_for_pending=wait_for_pending)
                    self.assertTrue(result, "Failover of node {0} failed".
                                    format(failover_node.ip))
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
                operation = self.task.async_rebalance(known_nodes, [], [],
                                                      retry_get_process_num=self.retry_get_process_num)
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
                    self.log.info("failing over nodes {0}".format(new_failover_nodes))
                    for failover_node in new_failover_nodes:
                        result = self.task.failover(known_nodes, failover_nodes=[failover_node],
                                                    graceful=True, wait_for_pending=wait_for_pending)
                        self.assertTrue(result, "Failover of node {0} failed".
                                        format(failover_node.ip))
                        self.execute_N1qltxn(failover_node)
                    if tasks is not None:
                        self.wait_for_async_data_load_to_complete(tasks)
                        tasks = None
                    self.data_load_after_failover()
                    # Mark the failover nodes for recovery
                    for failover_node in new_failover_nodes:
                        self.rest.set_recovery_type(otpNode='ns_1@' + failover_node.ip,
                                                    recoveryType=self.recovery_type)
                    operation = self.task.async_rebalance(known_nodes, [], [],
                                                          retry_get_process_num=self.retry_get_process_num)
                    iter_count = iter_count + 1
                    if iter_count == len(failover_list):
                        continue
                    self.wait_for_rebalance_to_complete(operation)
        elif rebalance_operation == "hard_failover_recovery":
            if step_count == -1:
                self.log.info("failing over nodes {0}".format(failover_nodes))
                for failover_node in failover_nodes:
                    result = self.task.failover(known_nodes, failover_nodes=[failover_node],
                                                graceful=False, wait_for_pending=wait_for_pending)
                    self.assertTrue(result, "Failover of node {0} failed".
                                    format(failover_node.ip))
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
                operation = self.task.async_rebalance(known_nodes, [], [],
                                                      retry_get_process_num=self.retry_get_process_num)
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
                    self.log.info("failing over nodes {0}".format(new_failover_nodes))
                    for failover_node in new_failover_nodes:
                        result = self.task.failover(known_nodes, failover_nodes=[failover_node],
                                                    graceful=False, wait_for_pending=wait_for_pending)
                        self.assertTrue(result, "Failover of node {0} failed".
                                        format(failover_node.ip))
                        self.execute_N1qltxn(failover_node)
                    if tasks is not None:
                        self.wait_for_async_data_load_to_complete(tasks)
                        tasks = None
                    self.data_load_after_failover()
                    # Mark the failover nodes for recovery
                    for failover_node in new_failover_nodes:
                        self.rest.set_recovery_type(otpNode='ns_1@' + failover_node.ip,
                                                    recoveryType=self.recovery_type)
                    operation = self.task.async_rebalance(known_nodes, [], [],
                                                          retry_get_process_num=self.retry_get_process_num)
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
                                                        self.cluster.buckets,
                                                        doc_loading_spec,
                                                        mutation_num=0,
                                                        async_load=async_load,
                                                        batch_size=self.batch_size,
                                                        process_concurrency=self.process_concurrency,
                                                        validate_task=(not self.skip_validations))
        return tasks

    def async_data_load(self):
        tasks = self.subsequent_data_load(async_load=True)
        return tasks

    def sync_data_load(self):
        self.subsequent_data_load()

    def bulk_api_load(self, bucket_name, num_scopes=1, num_collections_per_scope=15,
                      wait_for_rebalance_to_start=True):
        """
        Pre-requisites - bucket must have only _default scope and _default collection
        creates one cycle of creates and drops of scopes and collections
        new scopes/collections are named 0,1,2 etc
        This is just for load, won't update any collection/scope objects of our libs
        """

        def create_collections_using_manifest_import():
            json_content = dict()
            json_content["scopes"] = list()
            json_content["scopes"].append({"name": "_default", "collections": [{"name": "_default"}]})
            for i in range(num_scopes):
                scope = dict()
                scope["name"] = str(i)
                scope["collections"] = list()
                for j in range(num_collections_per_scope):
                    col = dict()
                    col["name"] = str(j)
                    scope["collections"].append(col)
                json_content["scopes"].append(scope)
            BucketHelper(self.cluster.master) \
                .import_collection_using_manifest(bucket_name,
                                                  str(json_content).replace("'", '"'))

        def delete_collections_using_manifest_import():
            json_content = dict()
            json_content["scopes"] = list()
            json_content["scopes"].append({"name": "_default", "collections": [{"name": "_default"}]})
            BucketHelper(self.cluster.master) \
                .import_collection_using_manifest(bucket_name,
                                                  str(json_content).replace("'", '"'))

        if wait_for_rebalance_to_start:
            end_time = time.time() + 60
            while (self.rest._rebalance_progress_status() != "running") and (time.time() < end_time):
                self.sleep(2, "wait for rebalance to start")
        create_collections_using_manifest_import()
        self.sleep(10, "wait before dropping collections using bulk api")
        delete_collections_using_manifest_import()

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
                self.bucket_util._expiry_pager(self.cluster)
                self.sleep(self.sleep_before_validation_of_ttl, "wait for maxttl to finish")
                # Compact buckets to delete non-resident expired items
                self.compact_all_buckets()
                self.wait_for_compaction_to_complete()
                self.sleep(60, "wait after compaction")
                items = 0
                self.bucket_util._wait_for_stats_all_buckets(
                    self.cluster, self.cluster.buckets, timeout=1200)
                for bucket in self.cluster.buckets:
                    items = items + self.bucket_helper_obj.get_active_key_count(bucket)
                if items != 0:
                    self.fail("Items did not go to 0")
            elif self.forced_hard_failover:
                pass
            else:
                self.bucket_util._wait_for_stats_all_buckets(
                    self.cluster, self.cluster.buckets, timeout=1200)
                self.bucket_util.validate_docs_per_collections_all_buckets(
                    self.cluster)

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

    def test_rebalance_cycles(self):
        """
        Do rebalance in and out in loop for a couple of cycles
        """
        self.cycles = self.input.param("cycles", 4)
        self.bulk_api_crud = self.input.param("bulk_api_crud", False)
        for cycle in range(self.cycles):
            self.log.info("Cycle {0}".format(cycle))

            known_nodes = self.cluster.servers[:self.nodes_init]
            add_nodes = self.cluster.servers[
                        self.nodes_init:self.nodes_init + self.nodes_in]
            operation = self.task.async_rebalance(known_nodes, add_nodes, [],
                                                  retry_get_process_num=self.retry_get_process_num)
            tasks = self.async_data_load()
            if self.bulk_api_crud:
                self.bulk_api_load(self.bucket.name)
            self.wait_for_rebalance_to_complete(operation)
            self.wait_for_async_data_load_to_complete(tasks)
            self.data_validation_collection()

            known_nodes = self.cluster.servers[:self.nodes_init + self.nodes_in]
            remove_nodes = known_nodes[-self.nodes_in:]
            operation = self.task.async_rebalance(known_nodes, [], remove_nodes,
                                                  retry_get_process_num=self.retry_get_process_num)
            tasks = self.async_data_load()
            if self.bulk_api_crud:
                self.bulk_api_load(self.bucket.name)
            self.wait_for_rebalance_to_complete(operation)
            self.wait_for_async_data_load_to_complete(tasks)
            self.data_validation_collection()

    def test_Orchestrator_Node_failover(self):
        """
        Failover orchestrator node and remove it and rebalance-in it back
        Repeats the above for a couple of cycles
        """
        self.graceful = self.input.param("graceful", False)
        self.cycles = self.input.param("cycles", 4)
        self.nodes_in_cluster = self.cluster.servers[:self.nodes_init]
        for cycle in range(self.cycles):
            self.log.info("Cycle {0}".format(cycle))
            _, content = self.cluster_util.find_orchestrator(self.cluster)
            orchestrator_node_ip = content.split("@")[1]
            failover_nodes = list()
            for server in self.nodes_in_cluster:
                if server.ip == orchestrator_node_ip:
                    failover_nodes.append(server)
                    self.nodes_in_cluster.remove(server)
                    break
            self.cluster.master = self.nodes_in_cluster[0]
            self.log.info("Failing over node(s): {0}".format(failover_nodes))
            result = self.task.failover(self.nodes_in_cluster,
                                        failover_nodes=failover_nodes,
                                        graceful=self.graceful)
            self.assertTrue(result, "Failover failed")

            rebalance_task = self.task.async_rebalance(
                self.nodes_in_cluster, [], [],
                retry_get_process_num=self.retry_get_process_num)
            self.wait_for_rebalance_to_complete(rebalance_task)

            # Set self.cluster.master to new orchestrator
            _, content = self.cluster_util.find_orchestrator(self.cluster)
            orchestrator_node_ip = content.split("@")[1]
            for server in self.nodes_in_cluster:
                if server.ip == orchestrator_node_ip:
                    self.cluster.master = server
                    break

            # Rebalance-in back the node
            rebalance_task = self.task.async_rebalance(
                self.nodes_in_cluster, failover_nodes, [],
                retry_get_process_num=self.retry_get_process_num)
            self.wait_for_rebalance_to_complete(rebalance_task)
            for node in failover_nodes:
                self.nodes_in_cluster.append(node)
