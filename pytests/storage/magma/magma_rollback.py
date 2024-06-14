'''
Created on Dec 12, 2019

@author: riteshagarwal
'''

import copy
import random
import time

from cb_constants.CBServer import CbServer
from cb_tools.cbepctl import Cbepctl
from cb_tools.cbstats import Cbstats
from constants.cb_constants import DocLoading
from couchbase_helper.documentgenerator import doc_generator
from magma_base import MagmaBaseTest
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient, TransactionConfig

from reactor.util.function import Tuples
from com.couchbase.test.docgen import WorkLoadSettings
from com.couchbase.test.loadgen import TransactionWorkLoadGenerate
from com.couchbase.test.transactions import Transaction


class MagmaRollbackTests(MagmaBaseTest):
    def setUp(self):
        super(MagmaRollbackTests, self).setUp()
        self.graceful = self.input.param("graceful", False)
        self.available_servers = list()
        self.available_servers = self.cluster.servers[self.nodes_init:]
        self.vbucket_check = self.input.param("vbucket_check", True)
        self.run_transactions = self.input.param("run_transactions", None)
        self.transaction_tasks = list()

    def tearDown(self):
        super(MagmaRollbackTests, self).tearDown()

    def _run_transaction(self):
        transaction_app = Transaction()
        trans_conf = TransactionConfig(self.transaction_durability_level,
                                       self.transaction_timeout)

        workload = dict()
        workload["keyPrefix"] = "trans_basics"
        workload["keySize"] = 20
        workload["docSize"] = 256
        workload["mutated"] = 0
        workload["keyRange"] = Tuples.of(0, self.num_items)
        workload["batchSize"] = 1
        workload["workers"] = 2
        workload["transaction_pattern"] = [
            [CbServer.default_scope, CbServer.default_collection,
             [["C", "R"]]]
        ]

        work_load = WorkLoadSettings(
            workload["keyPrefix"],
            workload["keySize"],
            workload["docSize"],
            workload["mutated"],
            workload["keyRange"],
            workload["batchSize"],
            workload["transaction_pattern"],
            workload["workers"])
        work_load.setTransactionRollback(True)
        client = SDKClient(self.cluster, self.cluster.buckets[0],
                           transaction_config=trans_conf)
        for index, load_pattern in enumerate(work_load.transaction_pattern):
            th_name = "Transaction_%s" % index
            batch_size = load_pattern[0]
            num_transactions = load_pattern[1]
            trans_pattern = load_pattern[2]

            task = TransactionWorkLoadGenerate(
                th_name, client.cluster, client.bucketObj,
                client.cluster.transactions(),
                work_load.doc_gen, batch_size, num_transactions, trans_pattern,
                work_load.commit_transaction, work_load.rollback_transaction,
                transaction_app)
            self.transaction_tasks.append(task)
            self.tm.submit(task)
        self.tm.getAllTaskResult()
        client.close()
        result = \
            self.check_fragmentation_using_magma_stats(self.cluster.buckets[0])
        self.assertTrue(result, "Magma framentation error")

    def crash_sigkill(self, nodes=None):
        nodes = nodes or self.cluster.nodes_in_cluster
        loop_itr = 0
        self.stop_crash = False

        shell_conn = list()
        for node in nodes:
            shell = RemoteMachineShellConnection(node)
            shell_conn.append(shell)

        while not self.stop_crash:
            loop_itr += 1
            for shell in shell_conn:
                shell.kill_memcached()
                self.sleep(1)
            for server in nodes:
                result = self.bucket_util._wait_warmup_completed(
                    self.cluster.buckets[0],
                    servers=[server],
                    wait_time=self.wait_timeout * 5)
                if not result:
                    self.stop_crash = True
                    msg = "Server = {}, Bucket stuck in warm up state after memCached kill"
                    self.assertTrue(result, msg.format(server))
            sleep = random.randint(30, 60)
            self.sleep(sleep,
                       "Crash Iteration:{} finished, waiting for {} sec to kill memcached on all nodes".
                       format(loop_itr, sleep))

    def compute_docs(self, start, mem_only_items):
        ops_len = len(self.doc_ops.split(":"))
        if "create" in self.doc_ops:
            self.create_start = start
            self.create_end = mem_only_items
        if ops_len == 1:
            if "update" in self.doc_ops:
                self.update_start = 0
                self.update_end = mem_only_items
            if "delete" in self.doc_ops:
                self.delete_start = 0
                self.delete_end = mem_only_items
            if "expiry" in self.doc_ops:
                self.expiry_start = 0
                self.expiry_end =  mem_only_items
        elif ops_len == 2:
            self.expiry_start = 0
            self.expiry_end = mem_only_items
            self.delete_start = start // 2
            self.delete_end = mem_only_items
            if "update" in self.doc_ops:
                self.delete_start = 0
                self.delete_end = mem_only_items
                self.update_start = start // 2
                self.update_end = mem_only_items
        else:
            self.expiry_start = 0
            self.expiry_end = mem_only_items
            self.delete_start = start // 3
            self.delete_end = mem_only_items
            self.update_start = (2 * start) // 3
            self.update_end = mem_only_items

    def test_magma_rollback_with_CDC(self):
        '''
         -- Multiple upsert ops so that some historical data
             is present
         -- Stop persistence on master node
         -- Start dedupe load on master node(say Node A)
         -- Kill MemCached on master node(Node A)
         -- Trigger roll back on other/replica nodes
         -- ReStart persistence on master node
         -- Repeat all the above steps for num_rollback times
        '''

        self.assertTrue(self.rest.update_autofailover_settings(False, 600),
                        "AutoFailover disabling failed")
        self.set_history_in_test = self.input.param("set_history_in_test", False)
        self.wipe_history = self.input.param("wipe_history", False)
        self.retention_seconds_to_wipe_history = self.input.param("retention_seconds_to_wipe_history", 86400)
        self.retention_bytes_to_wipe_history = self.input.param("retention_bytes_to_wipe_history", 100000000000)

        if self.nodes_init < 2 or self.num_replicas < 1:
            self.fail("Not enough nodes/replicas in the cluster/bucket \
            to test rollback")

        mem_only_items = self.input.param("rollback_items", 500)
        num_collections_for_rollback = self.input.param("num_collections_for_rollback", 2)
        init_history_start_seq = self.get_history_start_seq_for_each_vb()

        count = 1
        while count < self.test_itr + 1:
            self.PrintStep("Step 2.{} ==> Update {} items/collections".format(count, self.init_items_per_collection))
            self.generate_docs(doc_ops="update", update_start=0,
                               update_end=self.init_items_per_collection)
            for collection in self.collections:
                self.loadgen_docs(_sync=True, doc_ops="update",
                                  retry_exceptions=self.retry_exceptions,
                                  collection=collection)
            if count == 1 and self.set_history_in_test:
                self.PrintStep("Step 2.{}.1: Setting history params after first upsert iteration"% (count))
                self.bucket_util.update_bucket_property(
                    self.cluster.master, self.cluster.buckets[0],
                    history_retention_seconds=86400, history_retention_bytes=96000000000)
                self.sleep(30, "sleep after updating history settings")
                init_history_start_seq = self.get_history_start_seq_for_each_vb()
            count += 1

        self.num_rollbacks = self.input.param("num_rollbacks", 10)

        shell = RemoteMachineShellConnection(self.cluster.master)
        cbstats = Cbstats(self.cluster.master)
        self.target_vbucket = cbstats.vbucket_list(self.cluster.buckets[0].name)
        cbstats.disconnect()
        self.log.info("Node=={} targt_vbuckets=={}".format(self.cluster.master.ip, self.target_vbucket))

        '''
        STEP - 2,  Stop persistence on master node
        '''
        for i in range(1, self.num_rollbacks+1):
            self.PrintStep("Roll back Iteration == {}".format(i))

            # Stopping persistence on NodeA
            self.log.debug("Iteration == {}, stopping persistence".format(i))
            Cbepctl(shell).persistence(self.cluster.buckets[0].name, "stop")

            ###################################################################
            '''
            STEP - 3
              -- Doc ops on master node for  self.duration * 60 seconds
              -- This step ensures new state files (number equal to self.duration)
            '''

            self.generate_docs(doc_ops="update", update_start=0,
                               update_end=mem_only_items,
                               target_vbucket=self.target_vbucket)
            self.batch_size = 500
            self.process_concurrency = 1
            task_info = dict()
            self.collections.remove("_default")
            collections_for_rollback = random.sample(self.collections, int(num_collections_for_rollback))
            self.collections.append("_default")
            for collection in collections_for_rollback:
                temp_task_info = self.loadgen_docs(_sync=False, doc_ops="update",
                              retry_exceptions=self.retry_exceptions,
                              collection=collection,
                              track_failures=False,
                              iterations=5)
                task_info.update(temp_task_info.items())

            for task in task_info:
                self.task_manager.get_task_result(task)

            self.target_vbs = []
            task_info = dict()
            for node in self.cluster.nodes_in_cluster:
                if node == self.cluster.master:
                    continue
                cbstats = Cbstats(node)
                self.target_vbs.append(cbstats.vbucket_list(self.cluster.buckets[0].name))
                cbstats.disconnect()

            self.target_vbs = [vb for vb_lst in self.target_vbs for vb in vb_lst]
            self.log.info("Target vbs on non master nodes".format(self.target_vbs))
            self.generate_docs(doc_ops="update", update_start=mem_only_items,
                               update_end=mem_only_items*5,
                               target_vbucket=self.target_vbs)
            for collection in self.collections:
                temp_task_info = self.loadgen_docs(_sync=False, doc_ops="update",
                                  retry_exceptions=self.retry_exceptions,
                                  collection=collection,
                                  track_failures=False,
                                  iterations=3)
                task_info.update(temp_task_info.items())
            for task in task_info:
                self.task_manager.get_task_result(task)
            if i == self.num_rollbacks and self.wipe_history:
                self.bucket_util.update_bucket_property(
                    self.cluster.master, self.cluster.buckets[0],
                    history_retention_seconds=self.retention_seconds_to_wipe_history,
                    history_retention_bytes=self.retention_seconds_to_wipe_history)

            self.sleep(10, "sleep after updating history params")

            shell.kill_memcached()

            self.assertTrue(self.bucket_util._wait_warmup_completed(
                [self.cluster.master],
                self.cluster.buckets[0],
                wait_time=self.wait_timeout * 10))

            '''
            STEP -5
              -- Restarting persistence on master node(Node A)
            '''

            self.log.debug("Iteration=={}, Re-Starting persistence".format(i))
            Cbepctl(shell).persistence(self.cluster.buckets[0].name, "start")
            self.sleep(5, "Iteration=={}, sleep after restarting persistence".format(i))
            ###################################################################

        shell.disconnect()

    def test_magma_rollback_basic(self):
        items = self.num_items
        mem_only_items = self.input.param("rollback_items", 100000)
        if self.nodes_init < 2 or self.num_replicas < 1:
            self.fail("Not enough nodes/replicas in the cluster/bucket \
            to test rollback")
        self.num_rollbacks = self.input.param("num_rollbacks", 10)
        shell = RemoteMachineShellConnection(self.cluster.master)
        cbstats = Cbstats(self.cluster.master)
        self.target_vbucket = cbstats.vbucket_list(self.cluster.buckets[0].
                                                   name)
        cbstats.disconnect()
        start = self.num_items
        self.gen_read = copy.deepcopy(self.gen_create)
        for _ in xrange(1, self.num_rollbacks+1):
            # Stopping persistence on NodeA
            mem_client = MemcachedClientHelper.direct_client(
                self.cluster.nodes_in_cluster[0], self.cluster.buckets[0])
            mem_client.stop_persistence()
            self.gen_create = self.genrate_docs_basic(start, mem_only_items,
                                                      self.target_vbucket)

            self.loadgen_docs(_sync=True,
                              retry_exceptions=self.retry_exceptions)
            start = self.gen_create.key_counter

            ep_queue_size_map = {self.cluster.nodes_in_cluster[0]:
                                 mem_only_items}
            vb_replica_queue_size_map = {self.cluster.nodes_in_cluster[0]: 0}

            for node in self.cluster.nodes_in_cluster[1:]:
                ep_queue_size_map.update({node: 0})
                vb_replica_queue_size_map.update({node: 0})

            for bucket in self.cluster.buckets:
                self.bucket_util._wait_for_stat(bucket, ep_queue_size_map)
                self.bucket_util._wait_for_stat(
                    bucket,
                    vb_replica_queue_size_map,
                    cbstat_cmd="all",
                    stat_name="vb_replica_queue_size",
                    timeout=900)

            # Kill memcached on NodeA to trigger rollback on other Nodes
            shell.kill_memcached()

            self.assertTrue(self.bucket_util._wait_warmup_completed(
                self.cluster.buckets[0],
                servers=[self.cluster.master],
                wait_time=self.wait_timeout * 10))
            self.sleep(10, "Not Required, but waiting for 10s after warm up")

            self.bucket_util.verify_stats_for_bucket(self.cluster, self.buckets[0], items,
                                                     timeout=300)

        data_validation = self.task.async_validate_docs(
                self.cluster, self.cluster.buckets[0],
                self.gen_read, "create", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                timeout_secs=self.sdk_timeout)
        self.task.jython_task_manager.get_task_result(data_validation)

        shell.disconnect()

    def test_magma_rollback_to_same_snapshot(self):
        '''
         -- Ensure creation of at least a single state file
         -- Stop persistence on master node(say NodeA)
         -- Start load on master node for a given duration(self.duration * 60 seconds)
         -- Above step ensures creation of new state files (# equal to self.duration)
         -- Kill MemCached on master node(Node A)
         -- Trigger roll back on other/replica nodes
         -- Repeat all the above steps for num_rollback times
        '''
        items = self.num_items
        mem_only_items = self.input.param("rollback_items", 10000)
        ops_len = len(self.doc_ops.split(":"))
        self.assertTrue(self.rest.update_autofailover_settings(False, 600),
                        "AutoFailover disabling failed")

        if self.nodes_init < 2 or self.num_replicas < 1:
            self.fail("Not enough nodes/replicas in the cluster/bucket \
            to test rollback")

        self.duration = self.input.param("duration", 2)
        self.num_rollbacks = self.input.param("num_rollbacks", 10)

        shell = RemoteMachineShellConnection(self.cluster.master)
        cbstats = Cbstats(self.cluster.master)
        self.target_vbucket = cbstats.vbucket_list(self.cluster.buckets[0].name)
        cbstats.disconnect()

        self.gen_read = copy.deepcopy(self.gen_create)

        #######################################################################
        '''
        STEP - 1, Ensures creation of at least one snapshot

        To ensure at least one snapshot should get created before rollback
        starts, we need to sleep for 60 seconds as per magma design which
        create state file every 60s

        '''
        self.log.info("State files after initial creates %s"
                     % self.get_state_files(self.buckets[0]))

        self.sleep(60, "Ensures creation of at least one snapshot")
        self.log.info("State files after 60 second of sleep %s"
                      % self.get_state_files(self.buckets[0]))

        #######################################################################
        '''
        STEP - 2,  Stop persistence on master node
        '''
        start = self.num_items
        master_itr = 0

        for i in range(1, self.num_rollbacks+1):
            self.log.info("Roll back Iteration {}".format(i))

            mem_item_count = 0
            self.log.debug("State files before stopping persistence == %s"
                           % self.get_state_files(self.buckets[0]))

            # Stopping persistence on master node (NodeA)
            self.log.debug("Iteration == {}, stopping persistence".format(i))
            Cbepctl(shell).persistence(self.cluster.buckets[0].name, "stop")
            self.sleep(60, "sleep after stopping persistence")

            ###################################################################
            '''
            STEP - 3
              -- Doc ops on master node for  self.duration * 60 seconds
              -- This step ensures new state files (number equal to self.duration)
            '''
            self.compute_docs(start, mem_only_items)
            self.gen_create = None
            self.gen_update = None
            self.gen_delete = None
            self.gen_expiry = None
            time_end = time.time() + 60 * self.duration
            while time.time() < time_end:
                master_itr += 1
                time_start = time.time()
                mem_item_count += mem_only_items * ops_len
                self.generate_docs(doc_ops=self.doc_ops,
                                   target_vbucket=self.target_vbucket)

                self.loadgen_docs(_sync=True,
                                  retry_exceptions=self.retry_exceptions)

                if self.gen_create is not None:
                    self.create_start = self.gen_create.key_counter
                if self.gen_update is not None:
                    self.update_start = self.gen_update.key_counter
                if self.gen_delete is not None:
                    self.delete_start = self.gen_delete.key_counter
                if self.gen_expiry is not None:
                    self.expiry_start = self.gen_expiry.key_counter

                if time.time() < time_start + 60:
                    self.sleep(time_start + 60 - time.time(),
                               "Sleep to ensure creation of state files for roll back, Itr = {}"
                               .format(master_itr))
                self.log.info("master_itr == {} , state files== {}".
                              format(master_itr,
                                     self.get_state_files(self.buckets[0])))

            ep_queue_size_map = {self.cluster.nodes_in_cluster[0]:
                                 mem_item_count}
            vb_replica_queue_size_map = {self.cluster.nodes_in_cluster[0]: 0}

            for node in self.cluster.nodes_in_cluster[1:]:
                ep_queue_size_map.update({node: 0})
                vb_replica_queue_size_map.update({node: 0})

            ###################################################################
            '''
            STEP - 4
              -- Kill Memcached on master node(Node A) and trigger rollback on replica/ nodes
            '''
            shell.kill_memcached()

            self.assertTrue(self.bucket_util._wait_warmup_completed(
                self.cluster.buckets[0],
                servers=[self.cluster.master],
                wait_time=self.wait_timeout * 10))

            self.log.info("Iteration= {}, State files after killing memcached on master node== {}".
                          format(i, self.get_state_files(self.buckets[0])))

            self.sleep(10, "Not Required, but waiting for 10s after warm up")
            self.bucket_util.verify_stats_for_bucket(self.cluster, self.buckets[0],
                                                     items, timeout=900)
            ###################################################################
            '''
            STEP -5
              -- Restarting persistence on master node
            '''

            self.log.debug("Iteration=={}, Re-Starting persistence".format(i))
            Cbepctl(shell).persistence(self.cluster.buckets[0].name, "start")
            self.sleep(5, "Iteration=={}, sleep after restarting persistence".format(i))
        ###################################################################
        '''
        STEP - 6
          -- Data Validation
        '''
        data_validation = self.task.async_validate_docs(
                self.cluster, self.cluster.buckets[0],
                self.gen_read, "create", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                timeout_secs=self.sdk_timeout)
        self.task.jython_task_manager.get_task_result(data_validation)
        #######################################################################
        shell.disconnect()

    def test_magma_rollback_to_new_snapshot(self):
        '''
         -- Ensure creation of at least a single state file
         -- Stop persistence on master node
         -- Start load on master node(say Node A) for a given duration(self.duration * 60 seconds)
         -- Above step ensures creation of new state files (# equal to self.duration)
         -- Kill MemCached on master node(Node A)
         -- Trigger roll back on other/replica nodes
         -- ReStart persistence on master node
         -- Start doc loading on all the nodes(ensure creation of state file)
         -- Above two steps ensure, roll back to new snapshot
         -- Repeat all the above steps for num_rollback times
         --
        '''
        items = self.num_items
        mem_only_items = self.input.param("rollback_items", 10000)
        divisor = self.input.param("divisor", 5)
        ops_len = len(self.doc_ops.split(":"))
        self.assertTrue(self.rest.update_autofailover_settings(False, 600),
                        "AutoFailover disabling failed")

        if self.nodes_init < 2 or self.num_replicas < 1:
            self.fail("Not enough nodes/replicas in the cluster/bucket \
            to test rollback")

        self.duration = self.input.param("duration", 2)
        self.num_rollbacks = self.input.param("num_rollbacks", 10)

        shell = RemoteMachineShellConnection(self.cluster.master)
        cbstats = Cbstats(self.cluster.master)
        self.target_vbucket = cbstats.vbucket_list(self.cluster.buckets[0].name)
        cbstats.disconnect()

        #######################################################################
        '''
        STEP - 1, Ensures creation of at least one snapshot

        To ensure at least one snapshot should get created before rollback
        starts, we need to sleep for 60 seconds as per magma design which
        create state file every 60s

        '''

        self.log.info("State files after initial creates == %s"
                      % self.get_state_files(self.buckets[0]))

        self.sleep(60, "Ensures creation of at least one snapshot")
        self.log.info("State files after 60 second of sleep %s"
                      % self.get_state_files(self.buckets[0]))

        #######################################################################
        '''
        STEP - 2,  Stop persistence on master node
        '''
        master_itr = 0
        for i in range(1, self.num_rollbacks+1):
            start = items
            self.log.info("Roll back Iteration == {}".format(i))

            mem_item_count = 0

            # Stopping persistence on NodeA
            self.log.debug("Iteration == {}, stopping persistence".format(i))
            Cbepctl(shell).persistence(self.cluster.buckets[0].name, "stop")

            ###################################################################
            '''
            STEP - 3
              -- Doc ops on master node for  self.duration * 60 seconds
              -- This step ensures new state files (number equal to self.duration)
            '''
            self.log.info("Just before compute docs, iteration {}".format(i))
            self.compute_docs(start, mem_only_items)
            self.gen_create = None
            self.gen_update = None
            self.gen_delete = None
            self.gen_expiry = None
            time_end = time.time() + 60 * self.duration
            while time.time() < time_end:
                master_itr += 1
                time_start = time.time()
                mem_item_count += mem_only_items * ops_len
                self.generate_docs(doc_ops=self.doc_ops,
                                   target_vbucket=self.target_vbucket)
                self.loadgen_docs(_sync=True,
                                  retry_exceptions=self.retry_exceptions)
                if self.gen_create is not None:
                    self.create_start = self.gen_create.key_counter
                if self.gen_update is not None:
                    self.update_start = self.gen_update.key_counter
                if self.gen_delete is not None:
                    self.delete_start = self.gen_delete.key_counter
                if self.gen_expiry is not None:
                    self.expiry_start = self.gen_expiry.key_counter

                if time.time() < time_start + 60:
                    self.sleep(time_start + 60 - time.time(),
                               "master_itr == {}, Sleep to ensure creation of state files for roll back,"
                               .format(master_itr))
                self.log.info("master_itr == {}, state files== {}".
                              format(master_itr,
                                     self.get_state_files(self.buckets[0])))

            ep_queue_size_map = {self.cluster.nodes_in_cluster[0]:
                                 mem_item_count}
            vb_replica_queue_size_map = {self.cluster.nodes_in_cluster[0]: 0}

            for node in self.cluster.nodes_in_cluster[1:]:
                ep_queue_size_map.update({node: 0})
                vb_replica_queue_size_map.update({node: 0})

            ###################################################################
            '''
            STEP - 4
              -- Kill Memcached on master node(Node A) and trigger rollback on replica/other nodes
            '''

            shell.kill_memcached()

            self.assertTrue(self.bucket_util._wait_warmup_completed(
                self.cluster.buckets[0],
                servers=[self.cluster.master],
                wait_time=self.wait_timeout * 10))

            self.log.info("Iteration == {},State files after killing memcached {}".
                          format(i, self.get_state_files(self.buckets[0])))

            self.sleep(10, "Not Required, but waiting for 10s after warm up")
            #self.bucket_util.verify_stats_all_buckets(items, timeout=300)
            #for bucket in self.cluster.buckets:
            #    self.log.debug(cbstats.failover_stats(bucket.name))
            ###################################################################
            '''
            STEP -5
              -- Restarting persistence on master node(Node A)
            '''

            self.log.debug("Iteration=={}, Re-Starting persistence".format(i))
            Cbepctl(shell).persistence(self.cluster.buckets[0].name, "start")
            self.sleep(5, "Iteration=={}, sleep after restarting persistence".format(i))
            ###################################################################
            '''
            STEP - 6
              -- Load Docs on all the nodes
              -- Loading of doc for 60 seconds
              -- Ensures creation of new state file
            '''
            if i != self.num_rollbacks:
                self.create_start = items
                self.create_end = items + items // divisor
                self.generate_docs(doc_ops="create", target_vbucket=None)

                time_end = time.time() + 60
                while time.time() < time_end:
                    time_start = time.time()
                    _ = self.loadgen_docs(self.retry_exceptions,
                                          self.ignore_exceptions,
                                          _sync=True,
                                          doc_ops="create")
                    self.bucket_util._wait_for_stats_all_buckets(
                        self.cluster, self.cluster.buckets, timeout=1200)
                    if time.time() < time_start + 60:
                        self.sleep(time_start + 60 - time.time(), "After new creates, sleeping , itr={}".format(i))
                items = items + items // divisor
                self.log.debug("Iteration == {}, Total num_items {}".format(i, items))

        shell.disconnect()

    def test_magma_rollback_on_all_nodes(self):
        '''
         -- Ensure creation of at least a single state file
         -- Stop persistence on master node
         -- Start load on master node(say Node A) for a given duration(self.duration * 60 seconds)
         -- Above step ensures creation of new state files (# equal to self.duration)
         -- Kill MemCached on master node(Node A)
         -- Trigger roll back on other/replica nodes
         -- ReStart persistence on master node,
         -- Stop persistence on other nodes(other than node A)
         -- Start doc loading on all the nodes
         -- Kill MemCached on all the other nodes(other than node A)
         -- Above step triggers roll back on node A
         -- Also replica on other nodes start sync with Node A(master
         -- Repeat all the above steps for num_rollback times
        '''
        items = self.num_items
        mem_only_items = self.input.param("rollback_items", 10000)

        if self.nodes_init < 2 or self.num_replicas < 1:
            self.fail("Not enough nodes/replicas in the cluster/bucket \
            to test rollback")

        self.duration = self.input.param("duration", 2)
        self.num_rollbacks = self.input.param("num_rollbacks", 10)

        shell_conn = list()
        for node in self.cluster.nodes_in_cluster:
            shell_conn.append(RemoteMachineShellConnection(node))

        cbstats = Cbstats(self.cluster.nodes_in_cluster[0])
        self.target_vbucket = cbstats.vbucket_list(self.cluster.buckets[0].
                                                   name)
        cbstats.disconnect()

        target_vbs_replicas = list()
        for node in self.cluster.nodes_in_cluster[1:]:
            cbstats = Cbstats(node)
            target_vbs_replicas.append(cbstats.vbucket_list(self.cluster.buckets[0].name))
            cbstats.disconnect()

        target_vbs_replicas = [val for vb_lst in target_vbs_replicas for val in vb_lst]

        #######################################################################
        '''
        STEP - 1, Ensures creation of at least one snapshot

        To ensure at least one snapshot should get created before rollback
        starts, we need to sleep for 60 seconds as per magma design which
        create state file every 60s

        '''

        self.log.info("State files after initial creates == %s"
                      % self.get_state_files(self.buckets[0]))

        self.sleep(60, "Ensures creation of at least one snapshot")
        self.log.info("State files after 60 second of sleep == %s"
                      % self.get_state_files(self.buckets[0]))

        #######################################################################
        '''
        STEP - 2,  Stop persistence on master node
        '''

        start = items
        start_2 = items

        master_itr = 0
        slave_itr = 0
        for i in range(1, self.num_rollbacks+1):
            self.log.info("Roll back Iteration == {}".format(i))

            mem_item_count = 0
            self.log.debug("Iteration == {}, State files before stopping persistence == {}".
                           format(i, self.get_state_files(self.buckets[0])))

            # Stopping persistence on NodeA
            self.log.debug("Iteration == {}, Stopping persistence on master node".format(i))
            Cbepctl(shell_conn[0]).persistence(self.cluster.buckets[0].name, "stop")

        #######################################################################
            '''
            STEP - 3
              -- Load documents on master node for  self.duration * 60 seconds
              -- This step ensures new state files (number equal to self.duration)
            '''

            time_end = time.time() + 60 * self.duration
            while time.time() < time_end:
                master_itr += 1
                time_start = time.time()
                mem_item_count += mem_only_items

                self.gen_create = self.genrate_docs_basic(start, mem_only_items,
                                                           self.target_vbucket)
                self.loadgen_docs(_sync=True,
                                  retry_exceptions=self.retry_exceptions)

                start = self.gen_create.key_counter

                if time.time() < time_start + 60:
                    self.sleep(time_start + 60 - time.time(),
                               "master_itr == {}, Sleep to ensure creation of state files for roll back"
                               .format(master_itr))
                self.log.info("master_itr == {}, state files== {}".
                              format(master_itr,
                                     self.get_state_files(self.buckets[0])))

            ep_queue_size_map = {self.cluster.nodes_in_cluster[0]:
                                 mem_item_count}
            vb_replica_queue_size_map = {self.cluster.nodes_in_cluster[0]: 0}

            for node in self.cluster.nodes_in_cluster[1:]:
                ep_queue_size_map.update({node: 0})
                vb_replica_queue_size_map.update({node: 0})

            for bucket in self.cluster.buckets:
                self.bucket_util._wait_for_stat(bucket, ep_queue_size_map)
                self.bucket_util._wait_for_stat(
                    bucket,
                    vb_replica_queue_size_map,
                    cbstat_cmd="all",
                    stat_name="vb_replica_queue_size",
                    timeout=300)

            ###################################################################
            '''
            STEP - 4
              -- Kill Memcached on master node(Node A) and trigger rollback on replica/ nodes
            '''

            shell_conn[0].kill_memcached()

            self.assertTrue(self.bucket_util._wait_warmup_completed(
                self.cluster.buckets[0],
                servers=[self.cluster.master],
                wait_time=self.wait_timeout * 10))

            self.log.debug("Iteration == {}, State files after killing memcached on master node == {}".
                          format(i, self.get_state_files(self.buckets[0])))

            self.bucket_util.verify_stats_for_bucket(self.cluster, self.buckets[0],
                                                     items, timeout=300)
            ###################################################################
            '''
            STEP -5
              -- Restarting persistence on master node(Node A)
              -- Stopping persistence on other nodes
            '''

            self.log.debug("Iteration=={}, Re-Starting persistence on master node".format(i))
            Cbepctl(shell_conn[0]).persistence(self.cluster.buckets[0].name, "start")

            for shell in shell_conn[1:]:
                Cbepctl(shell).persistence(self.cluster.buckets[0].name, "stop")

            ###################################################################
            '''
            STEP - 6
              -- Load Docs on slave nodes(Other than node A)
              -- Load Docs on master node(Node A)
              -- Loading of doc is for self.duation * 60 seconds
              -- Ensures creation of new state file (equal to self.duration) on master node
            '''
            mem_item_count = 0

            time_end = time.time() + 60 * self.duration
            while time.time() < time_end:
                slave_itr += 1
                mem_item_count += mem_only_items
                time_start = time.time()

                self.log.debug("slave_itr == {}, Loading docs on slaves".format(slave_itr))
                self.gen_create = self.genrate_docs_basic(start_2, mem_only_items,
                                                          target_vbs_replicas)

                self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=True)

                start_2 = self.gen_create.key_counter

                self.log.debug("slave_itr == {}, Loading docs on master".format(slave_itr))
                self.gen_create = self.genrate_docs_basic(start_2, mem_only_items,
                                                          self.target_vbucket)
                self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=True)

                start_2 = self.gen_create.key_counter

                ep_queue_size_map = {self.cluster.nodes_in_cluster[0]:
                                     0}
                vb_replica_queue_size_map = {self.cluster.nodes_in_cluster[0]: 0}
                for bucket in self.cluster.buckets:
                    self.bucket_util._wait_for_stat(bucket, ep_queue_size_map)
                    self.bucket_util._wait_for_stat(bucket, vb_replica_queue_size_map,
                                                    cbstat_cmd="all",
                                                    stat_name="vb_replica_queue_size")

                if time.time() < time_start + 60:
                    self.sleep(time_start + 60 - time.time(), "After new creates, sleeping , slave_itr == {}".
                               format(slave_itr))

            items = items + mem_item_count
            start = start_2
            self.log.debug("Iteration == {}, Total num_items {}".format(i, items))

            ###################################################################
            '''
            STEP -7
              -- Kill MemCached on Slave nodes, and trigger rollback on master
              -- ReStart persistence on Slave nodes
            '''

            for shell in shell_conn[1:]:
                shell.kill_memcached()

            for node in self.cluster.nodes_in_cluster[1:]:
                self.assertTrue(self.bucket_util._wait_warmup_completed(
                    self.cluster.buckets[0], servers=[node],
                    wait_time=self.wait_timeout * 10))

            for shell in shell_conn[1:]:
                Cbepctl(shell).persistence(self.cluster.buckets[0].name, "start")

            self.log.info("State file at end of iteration-{} are == {}".
                          format(i, self.get_state_files(self.buckets[0])))

            self.log.debug("'Wait for stats' after starting persistence, Iteration{}".format(i))
            self.sleep(5)
            for bucket in self.cluster.buckets:
                self.bucket_util._wait_for_stat(bucket, vb_replica_queue_size_map,
                                                cbstat_cmd="all",
                                                stat_name="vb_replica_queue_size")
                self.bucket_util._wait_for_stat(bucket, ep_queue_size_map)

        for shell in shell_conn:
            shell.disconnect()

    def test_iteratively_rollback_all_nodes_to_same_snapshot(self):
        '''
        Test focus: Stopping persistence one by one on all nodes,
                    and trigger roll back on other  nodes.

        STEPS:
         -- Ensure creation of at least a single state file
         -- Below steps will be repeated on all nodes, with stopping peristence on one at a time
         -- Stop persistence on node x
         -- Start load on node x for a given duration(self.duration * 60 seconds)
         -- Above step ensures creation of new state files (# equal to self.duration)
         -- Kill MemCached on Node x
         -- Trigger roll back on other/replica nodes
         -- ReStart persistence on Node -x
         -- After every iteration of roll back on all nodes,
         -- Load new data to ensure in every iteration roll back is to new snapshot
         -- Repeat all the above steps for num_rollback times
        '''
        items = copy.deepcopy(self.num_items)
        mem_only_items = self.input.param("rollback_items", 10000)
        self.assertTrue(self.rest.update_autofailover_settings(False, 600),
                        "AutoFailover disabling failed")

        ops_len = len(self.doc_ops.split(":"))
        self.gen_read = copy.deepcopy(self.gen_create)

        if self.nodes_init < 2 or self.num_replicas < 1:
            self.fail("Not enough nodes/replicas in the cluster/bucket \
            to test rollback")

        self.duration = self.input.param("duration", 2)
        self.num_rollbacks = self.input.param("num_rollbacks", 3)

        #######################################################################
        '''
        STEP - 1, Ensures creation of at least one snapshot

        To ensure at least one snapshot should get created before rollback
        starts, we need to sleep for 60 seconds as per magma design which
        create state file every 60s

        '''

        self.log.info("State files after initial creates == %s"
                      % self.get_state_files(self.buckets[0]))

        self.sleep(60, "Ensures creation of at least one snapshot")
        self.log.info("State files after 60 second of sleep == %s"
                      % self.get_state_files(self.buckets[0]))

        #######################################################################
        '''
        STEP - 2,  Stop persistence on node - x
        '''
        start = items
        for i in range(1, self.num_rollbacks+1):
            self.log.info("Roll back Iteration == {}".format(i))
            for x, node in enumerate(self.cluster.nodes_in_cluster):
                #start = items
                shell = RemoteMachineShellConnection(node)
                cbstats = Cbstats(node)
                self.target_vbucket = cbstats.vbucket_list(self.cluster.buckets[0].
                                                           name)
                cbstats.disconnect()
                mem_item_count = 0
                self.log.debug("Iteration == {}, State files before stopping persistence == {}".
                           format(i, self.get_state_files(self.buckets[0])))
                # Stopping persistence on Node-x
                self.log.debug("Iteration=={}, Stopping persistence on Node-{}, ip=={}"
                               .format(i, x+1, node))
                Cbepctl(shell).persistence(self.cluster.buckets[0].name, "stop")

                ###############################################################
                '''
                STEP - 3
                  -- Doc Ops on node  x for  self.duration * 60 seconds
                  -- This step ensures new state files (number equal to self.duration)
                '''
                self.compute_docs(start, mem_only_items)
                self.gen_create = None
                self.gen_update = None
                self.gen_delete = None
                self.gen_expiry = None
                time_end = time.time() + 60 * self.duration
                itr = 0
                while time.time() < time_end:
                    itr += 1
                    time_start = time.time()
                    mem_item_count += mem_only_items * ops_len
                    self.generate_docs(doc_ops=self.doc_ops,
                                       target_vbucket=self.target_vbucket)

                    self.loadgen_docs(_sync=True,
                                      retry_exceptions=self.retry_exceptions)
                    if self.gen_create is not None:
                        self.create_start = self.gen_create.key_counter
                    if self.gen_update is not None:
                        self.update_start = self.gen_update.key_counter
                    if self.gen_delete is not None:
                        self.delete_start = self.gen_delete.key_counter
                    if self.gen_expiry is not None:
                        self.expiry_start = self.gen_expiry.key_counter

                    if time.time() < time_start + 60:
                        self.log.info("Rollback Iteration== {}, itr== {}, Active-Node== {}, Node=={}".format(i, itr, x+1, node))
                        self.sleep(time_start + 60 - time.time(),
                                   "Sleep to ensure creation of state files for roll back")
                        self.log.info("state files == {}".format(
                                     self.get_state_files(self.buckets[0])))

                ep_queue_size_map = {node:
                                     mem_item_count}
                if self.durability_level:
                    self.log.info("updating the num_items on disk check to double due to durability")
                    ep_queue_size_map = {node:
                                     mem_item_count * 2}
                vb_replica_queue_size_map = {node: 0}

                for nod in self.cluster.nodes_in_cluster:
                    if nod != node:
                        ep_queue_size_map.update({nod: 0})
                        vb_replica_queue_size_map.update({nod: 0})

                ###############################################################
                '''
                STEP - 4
                  -- Kill Memcached on Node - x and trigger rollback on other nodes
                '''

                shell.kill_memcached()
                self.assertTrue(self.bucket_util._wait_warmup_completed(
                    self.cluster.buckets[0],
                    servers=[node],
                    wait_time=self.wait_timeout * 10))

                self.log.debug("Iteration == {}, Node-- {} State files after killing memcached ".
                          format(i, x+1, self.get_state_files(self.buckets[0])))

                ###############################################################
                '''
                STEP -5
                   -- Restarting persistence on Node -- x
                '''

                self.log.debug("Iteration=={}, Re-Starting persistence on Node -- {}".format(i, x))
                Cbepctl(shell).persistence(self.cluster.buckets[0].name, "start")

                self.log.info("State file at end of iteration-{} are == {}".
                              format(i, self.get_state_files(self.buckets[0])))
                self.sleep(5, "Sleep after re-starting persistence, Iteration{}".format(i))
                for nod in self.cluster.nodes_in_cluster:
                    ep_queue_size_map.update({nod: 0})
                    vb_replica_queue_size_map.update({nod: 0})
                self.log.info("Iteration-{}, node-{}, check for wait for stats".format(i, x+1))
                #for bucket in self.cluster.buckets:
                #    self.bucket_util._wait_for_stat(bucket,
                #                                    ep_queue_size_map, timeout=300)
                #    self.bucket_util._wait_for_stat(bucket,
                #                                    vb_replica_queue_size_map,
                #                                    cbstat_cmd="all",
                #                                    stat_name="vb_replica_queue_size", timeout=300)

                shell.disconnect()

        #######################################################################
        '''
        STEP - 6
          -- Data Validation
        '''
        data_validation = self.task.async_validate_docs(
                self.cluster, self.cluster.buckets[0],
                self.gen_read, "create", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                timeout_secs=self.sdk_timeout)
        self.task.jython_task_manager.get_task_result(data_validation)
        #######################################################################

    def test_iteratively_rollback_all_nodes_to_new_snapshot(self):
        '''
        Test focus: Stopping persistence one by one on all nodes,
                    and trigger roll back on other  nodes
                    Above step will be done num_rollback
                    (variable defined in test) times
                    At the end of every iteration load new items to
                    ensure in every iteration roll back is to new snapshot

        STEPS:
         -- Ensure creation of at least a single state file
         -- Below steps will be repeated on all nodes, with stopping peristence on one at a time
         -- Stop persistence on node x
         -- Start load on node x for a given duration(self.duration * 60 seconds)
         -- Above step ensures creation of new state files (# equal to self.duration)
         -- Kill MemCached on Node x
         -- Trigger roll back on other/replica nodes
         -- ReStart persistence on Node -x
         -- Repeat all the above steps for num_rollback times
        '''
        self.assertTrue(self.rest.update_autofailover_settings(False, 600),
                        "AutoFailover disabling failed")
        items = copy.deepcopy(self.num_items)
        mem_only_items = self.input.param("rollback_items", 10000)
        divisor = self.input.param("divisor", 5)

        ops_len = len(self.doc_ops.split(":"))

        if self.nodes_init < 2 or self.num_replicas < 1:
            self.fail("Not enough nodes/replicas in the cluster/bucket \
            to test rollback")

        self.duration = self.input.param("duration", 2)
        self.num_rollbacks = self.input.param("num_rollbacks", 3)

        #######################################################################
        '''
        STEP - 1, Ensures creation of at least one snapshot

        To ensure at least one snapshot should get created before rollback
        starts, we need to sleep for 60 seconds as per magma design which
        create state file every 60s

        '''

        self.log.info("State files after initial creates == %s"
                      % self.get_state_files(self.buckets[0]))

        self.sleep(60, "Ensures creation of at least one snapshot")
        self.log.info("State files after 60 second of sleep == %s"
                      % self.get_state_files(self.buckets[0]))

        #######################################################################
        '''
        STEP - 2,  Stop persistence on node - x
        '''

        for i in range(1, self.num_rollbacks+1):
            self.log.info("Roll back Iteration == {}".format(i))
            start = items
            for x, node in enumerate(self.cluster.nodes_in_cluster):
                shell = RemoteMachineShellConnection(node)
                cbstats = Cbstats(node)
                self.target_vbucket = cbstats.vbucket_list(self.cluster.buckets[0].
                                                   name)
                cbstats.disconnect()
                mem_item_count = 0
                self.log.debug("Iteration == {}, State files before stopping persistence == {}".
                               format(i, self.get_state_files(self.buckets[0])))
                # Stopping persistence on Node-x
                self.log.debug("Iteration == {}, Stopping persistence on Node-{}, ip ={}"
                               .format(i, x+1, node))
                Cbepctl(shell).persistence(self.cluster.buckets[0].name, "stop")

                ###############################################################
                '''
                STEP - 3
                  -- Load documents on node  x for  self.duration * 60 seconds
                  -- This step ensures new state files (number equal to self.duration)
                '''
                self.compute_docs(start, mem_only_items)
                self.gen_create = None
                self.gen_update = None
                self.gen_delete = None
                self.gen_expiry = None
                time_end = time.time() + 60 * self.duration
                itr = 0
                while time.time() < time_end:
                    itr += 1
                    time_start = time.time()
                    mem_item_count += mem_only_items * ops_len
                    self.generate_docs(doc_ops=self.doc_ops,
                                       target_vbucket=self.target_vbucket)
                    self.loadgen_docs(_sync=True,
                                      retry_exceptions=self.retry_exceptions)
                    if self.gen_create is not None:
                        self.create_start = self.gen_create.key_counter
                    if self.gen_update is not None:
                        self.update_start = self.gen_update.key_counter
                    if self.gen_delete is not None:
                        self.delete_start = self.gen_delete.key_counter
                    if self.gen_expiry is not None:
                        self.expiry_start = self.gen_expiry.key_counter

                    if time.time() < time_start + 60:
                        self.log.info("Rollback Iteration== {}, itr== {}, Active-Node=={}, Node=={}".format(i, itr, x+1, node))
                        self.sleep(time_start + 60 - time.time(),
                                   "Sleep to ensure creation of state files for roll back")
                        self.log.info("state files == {}".format(
                                     self.get_state_files(self.buckets[0])))

                ep_queue_size_map = {node:
                                     mem_item_count}
                if self.durability_level:
                    self.log.info("updating the num_items on disk check to double due to durability")
                    ep_queue_size_map = {node:
                                     mem_item_count * 2}
                vb_replica_queue_size_map = {node: 0}

                for nod in self.cluster.nodes_in_cluster:
                    if nod != node:
                        ep_queue_size_map.update({nod: 0})
                        vb_replica_queue_size_map.update({nod: 0})

                ###############################################################
                '''
                STEP - 4
                  -- Kill Memcached on Node - x and trigger rollback on other nodes
                '''

                shell.kill_memcached()
                self.assertTrue(self.bucket_util._wait_warmup_completed(
                    self.cluster.buckets[0],
                    servers=[node],
                    wait_time=self.wait_timeout * 10))

                self.log.debug("Iteration == {}, Node-- {} State files after killing memcached ".
                          format(i, node, self.get_state_files(self.buckets[0])))

                ###############################################################
                '''
                STEP -5
                 -- Restarting persistence on Node -- x
                '''

                self.log.debug("Iteration=={}, Re-Starting persistence on Node -- {}".format(i, node))
                Cbepctl(shell).persistence(self.cluster.buckets[0].name, "start")

                self.log.info("State file at end of iteration-{} are == {}".
                              format(i, self.get_state_files(self.buckets[0])))

                self.sleep(5, "Sleep after re-starting persistence, Iteration{}".format(i))
                for nod in self.cluster.nodes_in_cluster:
                    ep_queue_size_map.update({nod: 0})
                    vb_replica_queue_size_map.update({nod: 0})
                self.log.info("Iteration-{}, node-{}, check for wait for stats".format(i, x+1))
                #for bucket in self.cluster.buckets:
                #    self.bucket_util._wait_for_stat(bucket,
                #                                    ep_queue_size_map, timeout=600)
                #    self.bucket_util._wait_for_stat(bucket,
                #                                    vb_replica_queue_size_map,
                #                                    cbstat_cmd="all",
                #                                    stat_name="vb_replica_queue_size", timeout=600)
                shell.disconnect()
            ###################################################################
            '''
            STEP - 6
              -- Load Docs on all the nodes
              -- Loading of doc for 60 seconds
              -- Ensures creation of new state file
            '''
            self.create_start = items
            self.create_end = items + items // divisor
            self.generate_docs(doc_ops="create", target_vbucket=None)

            time_end = time.time() + 60
            while time.time() < time_end:
                time_start = time.time()
                self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions, _sync=True,
                                  doc_ops="create")
                self.bucket_util._wait_for_stats_all_buckets(
                    self.cluster, self.cluster.buckets, timeout=1200)
                if time.time() < time_start + 60:
                    self.sleep(time_start + 60 - time.time(), "After new creates, sleeping , itr={}".format(i))
            items = items + items // divisor
            self.log.debug("Iteration == {}, Total num_items {}".format(i, items))

    def test_rollback_with_multiCollections(self):
        '''
        Test focus: Test roll back with multiple collections,
                    Stopping persistence one by one on all
                    nodes and trigger roll back on other nodes

        STEPS:
         -- Disabled initial loading in setup
         -- Loaded self.num_items in all the collections
         -- Doc loading may create state file(s)
         -- But ensure creation of at least a single state file,
             sleep for 60 seconds
         -- Below steps will be repeated on all nodes,
             with stopping peristence on one at a time
         -- Stop persistence on node x
         -- Start doc ops on node x on all collections(for self.duration iterations)
         -- Above step ensures creation of new state files
         -- Kill MemCached on Node x
         -- Trigger roll back on other/replica nodes
         -- ReStart persistence on Node -x
         -- Repeat above steps on all the remaining nodes
         -- After every iteration of roll back on all nodes
         -- Repeat all the above steps for num_rollback times
        '''
        self.assertTrue(self.rest.update_autofailover_settings(False, 600),
                        "AutoFailover disabling failed")
        if self.nodes_init < 2 or self.num_replicas < 1:
            self.fail("Not enough nodes/replicas in the cluster/bucket \
            to test rollback")
        #######################################################################
        '''
        STEP - 1, Initial doc loading to all collections
        '''
        start_items = self.num_items
        scope_name = CbServer.default_scope
        collection_prefix = "FunctionCollection"

        for i in range(self.num_collections):
            collection_name = collection_prefix + str(i)
            self.log.info("Creating scope::collection {} {}\
            ".format(scope_name, collection_name))
            self.bucket_util.create_collection(
                self.cluster.master, self.buckets[0],
                scope_name, {"name": collection_name})
            self.sleep(2)

        collections = self.buckets[0].scopes[scope_name].collections.keys()
        self.log.debug("Collections list == {}".format(collections))

        tasks_info = dict()

        for collection in collections:
            self.generate_docs(doc_ops="create", target_vbucket=None)
            tem_tasks_info = self.loadgen_docs(
                self.retry_exceptions,
                self.ignore_exceptions,
                scope=scope_name,
                collection=collection,
                _sync=False,
                doc_ops="create")
            tasks_info.update(tem_tasks_info.items())

        self.num_items -= start_items
        for task in tasks_info:
            self.task_manager.get_task_result(task)
        self.bucket_util.verify_doc_op_task_exceptions(
            tasks_info, self.cluster)
        self.bucket_util.log_doc_ops_task_failures(tasks_info)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets,
                                                     timeout=1200)
        self.bucket_util.verify_stats_for_bucket(self.cluster, self.buckets[0],
                                                 self.num_items)

        items = self.num_items
        mem_only_items = self.input.param("rollback_items", 10000)
        ops_len = len(self.doc_ops.split(":"))

        self.duration = self.input.param("duration", 1)
        self.num_rollbacks = self.input.param("num_rollbacks", 10)

        #######################################################################
        '''
        STEP - 2, Ensures creation of at least one snapshot

        To ensure at least one snapshot should get created before rollback
        starts, we need to sleep for 60 seconds as per magma design which
        create state file every 60s

        '''

        self.log.info("State files after initial creates == %s"
                      % self.get_state_files(self.buckets[0]))

        self.sleep(60, "Ensures creation of at least one snapshot")
        self.log.info("State files after 60 second of sleep == %s"
                      % self.get_state_files(self.buckets[0]))

        #######################################################################
        '''
        STEP - 3,  Stop persistence on node - x
        '''

        for i in range(1, self.num_rollbacks+1):
            self.log.info("Roll back Iteration == {}".format(i))
            for x, node in enumerate(self.cluster.nodes_in_cluster):
                start = start_items
                shell = RemoteMachineShellConnection(node)
                cbstats = Cbstats(node)
                self.target_vbucket = cbstats.vbucket_list(
                    self.cluster.buckets[0].name)
                cbstats.disconnect()
                mem_item_count = 0
                self.log.debug("Iteration == {}, State files before stopping persistence == {}".
                           format(i, self.get_state_files(self.buckets[0])))
                # Stopping persistence on Node-x
                self.log.debug("Iteration == {}, Stopping persistence on Node-{}"
                               .format(i, x+1))
                Cbepctl(shell).persistence(self.cluster.buckets[0].name, "stop")

                ###############################################################
                '''
                STEP - 4
                  -- Doc Ops on node  x for  self.duration * 60 seconds
                  -- This step ensures new state files (number equal to self.duration)
                '''
                self.compute_docs(start, mem_only_items)
                self.gen_create = None
                self.gen_update = None
                self.gen_delete = None
                self.gen_expiry = None

                itr = 0
                while itr < self.duration:
                    itr += 1
                    time_start = time.time()
                    mem_item_count += mem_only_items * ops_len
                    self.generate_docs(doc_ops=self.doc_ops,
                                       target_vbucket=self.target_vbucket)
                    tasks_in = dict()
                    for collection in collections:
                        tem_tasks_in = self.loadgen_docs(retry_exceptions=self.retry_exceptions,
                                                           ignore_exceptions=self.ignore_exceptions,
                                                           scope=scope_name,
                                                           collection=collection,
                                                           _sync=False)
                        tasks_in.update(tem_tasks_in.items())

                    for task in tasks_in:
                        self.task_manager.get_task_result(task)
                    self.bucket_util.verify_doc_op_task_exceptions(
                        tasks_in, self.cluster)
                    self.bucket_util.log_doc_ops_task_failures(tasks_in)

                    if self.gen_create is not None:
                        self.create_start = self.gen_create.key_counter
                    if self.gen_update is not None:
                        self.update_start = self.gen_update.key_counter
                    if self.gen_delete is not None:
                        self.delete_start = self.gen_delete.key_counter
                    if self.gen_expiry is not None:
                        self.expiry_start = self.gen_expiry.key_counter
                    self.log.info("Rollback Iteration== {}, itr== {}, Active-Node== {}, Node=={}".
                                  format(i, itr, x+1, node))
                    if time.time() < time_start + 60:
                        self.sleep(time_start + 60 - time.time(),
                                   "Sleep to ensure creation of state files for roll back")
                    self.log.info("state files == {}".format(
                                     self.get_state_files(self.buckets[0])))

                mem_item_count += mem_item_count * self.num_collections

                ep_queue_size_map = {node:
                                     mem_item_count}
                vb_replica_queue_size_map = {node: 0}

                for nod in self.cluster.nodes_in_cluster:
                    if nod != node:
                        ep_queue_size_map.update({nod: 0})
                        vb_replica_queue_size_map.update({nod: 0})

                for bucket in self.cluster.buckets:
                    self.bucket_util._wait_for_stat(bucket, ep_queue_size_map)
                    self.bucket_util._wait_for_stat(bucket, vb_replica_queue_size_map,
                                                    cbstat_cmd="all",
                                                    stat_name="vb_replica_queue_size",
                                                    timeout=300)
                ###############################################################
                '''
                STEP - 5
                  -- Kill Memcached on Node - x and trigger rollback on other nodes
                '''

                shell.kill_memcached()
                self.sleep(10, "sleep after MemCached kill on node {}".format(node))
                self.assertTrue(self.bucket_util._wait_warmup_completed(
                    self.cluster.buckets[0],
                    server=[self.cluster.master],
                    wait_time=self.wait_timeout * 10))

                self.log.debug("Iteration == {}, Node-- {} State files after killing memcached ".
                               format(i, x+1, self.get_state_files(self.buckets[0])))

                self.bucket_util.verify_stats_for_bucket(self.cluster, self.buckets[0], items,
                                                          timeout=300)
                ###############################################################
                '''
                STEP - 6
                   -- Restarting persistence on Node -- x
                '''

                self.log.debug("Iteration=={}, Re-Starting persistence on Node -- {}".format(i, x))
                Cbepctl(shell).persistence(self.cluster.buckets[0].name, "start")

                self.log.info("State file at end of iteration-{} are == {}".
                              format(i, self.get_state_files(self.buckets[0])))
                self.sleep(10, "Sleep after re-starting persistence, Iteration-{}".
                           format(i))

                for nod in self.cluster.nodes_in_cluster:
                    ep_queue_size_map.update({nod: 0})
                    vb_replica_queue_size_map.update({nod: 0})

                for bucket in self.cluster.buckets:
                    self.bucket_util._wait_for_stat(bucket,
                                                    ep_queue_size_map)
                    self.bucket_util._wait_for_stat(bucket,
                                                    vb_replica_queue_size_map,
                                                    cbstat_cmd="all",
                                                    stat_name="vb_replica_queue_size",
                                                    timeout=300)

                shell.disconnect()
        #######################################################################

    def test_crash_during_rollback(self):
        '''
        Test focus: Stopping persistence on x(where x < num_nodes) nodes,
                    and trigger roll back on other  nodes.
                    and during rollback crash on replica nodes.
                    Above step will be done num_rollback
                    (variable defined in test) times
                    At the end of every iteration load new items to
                    ensure in every iteration roll back is to new snapshot

        STEPS:
         -- Ensure creation of at least a single state file
         -- Below steps will be repeated on all nodes,
               with stopping persistence on x nodes
               (where x >= 1 and x < num_nodes)
               at a time
         -- Start load on all the x nodes for a given duration
             (self.duration * 60 seconds)
         -- Above step ensures creation of new state files
             (# equal or greater than to self.duration)
         -- Kill MemCached on all the x nodes
         -- Trigger roll back on other/replica nodes
         -- During roll back SigKill on replica nodes
         -- ReStart persistence on all the x nodes
         -- Repeat all the above steps for num_rollback times
        '''
        self.assertTrue(self.rest.update_autofailover_settings(False, 600),
                        "AutoFailover disabling failed")
        mem_only_items = self.input.param("rollback_items", 10000)
        target_active_nodes = self.input.param("target_active_nodes", 1)
        num_crashes = self.input.param("num_crashes", 5)
        collections_for_rollback = self.input.param("collections_for_rollback", 1)
        load_during_rollback = self.input.param("load_during_rollback", False)
        divisor = self.input.param("divisor", 30)
        if self.nodes_init < 2 or self.num_replicas < 1:
            self.fail("Not enough nodes/replicas in the cluster/bucket \
            to test rollback")

        self.duration = self.input.param("duration", 1)
        self.num_rollbacks = self.input.param("num_rollbacks", 10)
        #######################################################################
        '''
        STEP - 1, Initial doc loading to all collections
        '''
        start_items = self.num_items
        scope_name = CbServer.default_scope
        '''
        Commenting below code, since we have
        added collection creation in magma_base
        '''
        # collection_prefix = "FunctionCollection"

        # for i in range(self.num_collections):
        #    collection_name = collection_prefix + str(i)
        #    self.log.info("Creating scope::collection {} {}\
        #    ".format(scope_name, collection_name))
        #    self.bucket_util.create_collection(
        #        self.cluster.master, self.buckets[0],
        #        scope_name, {"name": collection_name})
        #    self.sleep(2)

        collections = self.buckets[0].scopes[scope_name].collections.keys()
        self.log.debug("Collections list == {}".format(collections))

        tasks_info = dict()

        for collection in collections:
            self.generate_docs(doc_ops="create", target_vbucket=None)
            tem_tasks_info = self.loadgen_docs(
                self.retry_exceptions,
                self.ignore_exceptions,
                scope=scope_name,
                collection=collection,
                _sync=False,
                doc_ops="create")
            tasks_info.update(tem_tasks_info.items())

        self.num_items -= start_items
        for task in tasks_info:
            self.task_manager.get_task_result(task)
        self.bucket_util.verify_doc_op_task_exceptions(
            tasks_info, self.cluster)
        self.bucket_util.log_doc_ops_task_failures(tasks_info)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets,
                                                     timeout=1200)
        self.bucket_util.verify_stats_for_bucket(self.cluster, self.buckets[0],
                                                 self.num_items, timeout=600)

        shell_conn = list()
        for node in self.cluster.nodes_in_cluster:
            if "kv" in node.services.lower():
                shell_conn.append(RemoteMachineShellConnection(node))

        target_vbs_active = list()
        target_vbs_replica = list()
        for node in self.cluster.nodes_in_cluster[:target_active_nodes]:
            if "kv" in node.services.lower():
                cbstats = Cbstats(node)
                target_vbs_active.append(
                    cbstats.vbucket_list(self.cluster.buckets[0].name))
                cbstats.disconnect()

        target_vbs_active = [val for vb_lst in target_vbs_active for val in vb_lst]
        self.log.debug("target_vbs_active == {}".format(target_vbs_active))
        for node in self.cluster.nodes_in_cluster[target_active_nodes:]:
            cbstats = Cbstats(node)
            target_vbs_replica.append(cbstats.vbucket_list(self.cluster.buckets[0].name))
            cbstats.disconnect()
        target_vbs_replica = [val for vb_lst in target_vbs_replica for val in vb_lst]
        self.log.info("target_vbs_active={} and target_vbs_replica={}".format(target_vbs_active, target_vbs_replica))

        #######################################################################
        '''
        STEP - 1, Ensures creation of at least one snapshot

        To ensure at least one snapshot should get created before rollback
        starts, we need to sleep for 60 seconds as per magma design which
        create state file every 60s

        '''

        self.log.info("State files after initial creates == %s"
                      % self.get_state_files(self.buckets[0]))

        self.sleep(60, "Ensures creation of at least one snapshot")
        self.log.info("State files after 60 second of sleep == %s"
                      % self.get_state_files(self.buckets[0]))

        #######################################################################
        '''
        STEP - 2,  Stop persistence on all the x nodes
        '''

        for i in range(1, self.num_rollbacks+1):
            self.log.info("Roll back Iteration == {}".format(i))
            start = start_items
            self.log.debug("Iteration == {}, State files before stopping persistence == {}".
                           format(i, self.get_state_files(self.buckets[0])))
            # Stopping persistence on Node-x
            for x, shell in enumerate(shell_conn[0:target_active_nodes]):
                self.log.debug("Iteration == {}, Stopping persistence on Nodes-{}"
                               .format(i, self.cluster.nodes_in_cluster[x]))
                Cbepctl(shell).persistence(self.cluster.buckets[0].name, "stop")

            ###############################################################
            '''
            STEP - 3
              -- Load documents on all the x number of nodes
                 for  self.duration * 60 seconds
              -- This step ensures new state files (number equal to self.duration)
            '''
            time_start = time.time()
            tasks_in = dict()
            for collection in collections[0:collections_for_rollback]:
                self.compute_docs(start, mem_only_items)
                self.gen_create = None
                self.gen_update = None
                self.gen_delete = None
                self.gen_expiry = None
                self.generate_docs(doc_ops=self.doc_ops,
                                   target_vbucket=target_vbs_active)
                tem_tasks_info = self.loadgen_docs(
                    retry_exceptions=self.retry_exceptions,
                    scope=scope_name,
                    collection=collection,
                    _sync=False)
                tasks_in.update(tem_tasks_info.items())

            for task in tasks_in:
                self.task_manager.get_task_result(task)
            self.bucket_util.verify_doc_op_task_exceptions(
                tasks_in, self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_in)

            if time.time() < time_start + 60:
                self.sleep(time_start + 60 - time.time(),
                           "Sleep to ensure creation of state files for roll back")
            self.log.info("Rollback Iteration== {},\
            state file after stopping persistence and after doc-ops == {}".
            format(i, self.get_state_files(self.buckets[0])))

            ###############################################################
            '''
            STEP - 4
              -- Kill Memcached on all the x num of nodes
                 and trigger roll back on other nodes
             -- SigKill on all the replica nodes during roll back
            '''
            for shell in shell_conn:
                shell.kill_memcached()
            for server in self.cluster.nodes_in_cluster:
                if "kv" in node.services.lower():
                    self.assertTrue(self.bucket_util._wait_warmup_completed(
                        self.cluster.buckets[0], servers=[server],
                        wait_time=self.wait_timeout * 30))
            if not load_during_rollback:
                crash_count = 1
                while num_crashes > 0:
                    self.log.info("Rollback Itr= {}, crash_count ={}".format(i, crash_count))
                    for shell in shell_conn[target_active_nodes:]:
                        shell.kill_memcached()
                    for server in self.cluster.nodes_in_cluster[target_active_nodes:]:
                        if "kv" in node.services.lower():
                            self.assertTrue(self.bucket_util._wait_warmup_completed(
                                self.cluster.buckets[0], servers=[server],
                                wait_time=self.wait_timeout * 5))
                    self.sleep(30, "30s sleep after crash")
                    num_crashes -= 1
                    crash_count += 1
                num_crashes = 5
            else:
                tasks_in = dict()
                '''
                   Disabling Crash thread for load during rollback
                '''
                #th = threading.Thread(target=self.crash_sigkill,
                #                      kwargs=dict(nodes=nodes))
                #th.start()
                for collection in collections[0:collections_for_rollback]:
                    self.compute_docs(start, mem_only_items)
                    self.gen_create = None
                    self.gen_update = None
                    self.gen_delete = None
                    self.gen_expiry = None
                    self.generate_docs(doc_ops="create:expiry:update",
                                       target_vbucket=target_vbs_replica)
                    tem_tasks_info = self.loadgen_docs(retry_exceptions=self.retry_exceptions,
                                                       scope=scope_name,
                                                       collection=collection,
                                                       _sync=False)
                    tasks_in.update(tem_tasks_info.items())
                for task in tasks_in:
                    self.task_manager.get_task_result(task)

                if self.gen_create is not None:
                        start_items = self.gen_create.key_counter
                self.log.debug("start_items after load during rollback is {}"
                               .format(start_items))
                #self.stop_crash = True
                #th.join()
                #self.bucket_util.verify_doc_op_task_exceptions(tasks_in, self.cluster)
                #self.bucket_util.log_doc_ops_task_failures(tasks_in)
            self.log.debug("Iteration == {},State files after killing memCached ".
                           format(i, self.get_state_files(self.buckets[0])))
            ###############################################################
            '''
            STEP -5
               -- Restarting persistence on all the x num of nodes
            '''
            for x, shell in enumerate(shell_conn[0:target_active_nodes]):
                self.log.debug("Iteration=={}, Re-Starting persistence on Node -- {}".
                               format(i, self.cluster.nodes_in_cluster[x]))
                Cbepctl(shell).persistence(self.cluster.buckets[0].name, "start")

            self.log.info("State file at end of iteration-{} are == {}".
                          format(i, self.get_state_files(self.buckets[0])))

            self.sleep(5, "Sleep after re-starting persistence, Iteration{}".format(i))

            ###################################################################
            '''
            STEP - 6
              -- Load Docs on all the nodes
              -- Loading of doc for 60 seconds
              -- Ensures creation of new state file
            '''
            self.create_start = start_items
            self.create_end = start_items + start_items // divisor
            tasks_info = dict()
            self.generate_docs(doc_ops="create", target_vbucket=None)
            time_start = time.time()
            for collection in collections:
                tem_tasks_info = self.loadgen_docs(
                    self.retry_exceptions,
                    self.ignore_exceptions,
                    scope=scope_name,
                    collection=collection,
                    _sync=False,
                    doc_ops="create")
                tasks_info.update(tem_tasks_info.items())

            for task in tasks_info:
                self.task_manager.get_task_result(task)

            self.bucket_util.verify_doc_op_task_exceptions(
                tasks_info, self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)
            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets,
                                                         timeout=1200)

            if time.time() < time_start + 60:
                self.sleep(time_start + 60 - time.time(),
                               "After new creates, sleeping , itr={}".
                               format(i))

            start_items = start_items + start_items // divisor
            self.log.debug("Iteration == {}, start_items={}".format(i, start_items))

        for shell in shell_conn:
            shell.disconnect()
        self.validate_seq_itr()

    def test_rebalance_during_rollback(self):
        '''
        Test focus: Stopping persistence on master node,
                    and trigger roll back on other  nodes.
                    and during roll back trigger rebalance task
                    along with load on replica nodes.
                    Above step will be done num_rollback
                    (variable defined in test) times
                    At the end of every iteration load new items to
                    ensure in every iteration roll back is to new snapshot

        STEPS:
         -- Ensure creation of at least a single state file
         -- Stop persistence on master node
         -- Start load on master node)
         -- Above step ensures creation of at least one new state file
         -- Kill MemCached on master node
         -- Above step triggers roll back on other/replica nodes
         -- During rollback trigger Rebalance task
         -- Along with rebalance start load on replica nodes
         -- Start persistence on master node
         -- Load on all the nodes(ensures creation of new state files)
         -- Repeat all the above steps for num_rollback times
        '''
        mem_only_items = self.input.param("rollback_items", 10000)
        rebalance_out_master = self.input.param("rebalance_out_master", False)
        init_nodes_count = len(self.cluster.nodes_in_cluster)

        collections_for_rollback = self.input.param("collections_for_rollback", 1)
        if self.nodes_init < 2 or self.num_replicas < 1:
            self.fail("Not enough nodes/replicas in the cluster/bucket \
            to test rollback")

        self.num_rollbacks = self.input.param("num_rollbacks", 10)
        #######################################################################
        '''
        STEP - 1, Initial doc loading to all collections
        '''
        start_items = self.num_items
        scope_name = CbServer.default_scope

        collections = self.buckets[0].scopes[scope_name].collections.keys()
        self.log.debug("Collections list == {}".format(collections))

        tasks_info = dict()

        for collection in collections:
            self.generate_docs(doc_ops="create", target_vbucket=None)
            tem_tasks_info = self.loadgen_docs(
                self.retry_exceptions,
                self.ignore_exceptions,
                scope=scope_name,
                collection=collection,
                _sync=False,
                doc_ops="create")
            tasks_info.update(tem_tasks_info.items())

        self.num_items -= start_items
        for task in tasks_info:
            self.task_manager.get_task_result(task)
        self.bucket_util.verify_doc_op_task_exceptions(
            tasks_info, self.cluster)
        self.bucket_util.log_doc_ops_task_failures(tasks_info)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets,
                                                     timeout=1200)
        # self.bucket_util.verify_stats_all_buckets(self.num_items)

        ######################################################################
        '''
        STEP - 1, Ensures creation of at least one snapshot

        To ensure at least one snapshot should get created before rollback
        starts, we need to sleep for 60 seconds as per magma design which
        create state file every 60s

        '''

        self.log.info("State files after initial creates == %s"
                      % self.get_state_files(self.buckets[0]))

        self.sleep(60, "Ensures creation of at least one snapshot")
        self.log.info("State files after 60 second of sleep == %s"
                      % self.get_state_files(self.buckets[0]))

        #######################################################################
        '''
        STEP - 2,  Stop persistence on master node
        '''

        for i in range(1, self.num_rollbacks+1):
            self.log.info("Roll back Iteration == {}".format(i))
            cbstats = Cbstats(self.cluster.nodes_in_cluster[0])
            self.target_vbucket = cbstats.vbucket_list(self.cluster.buckets[0].name)
            cbstats.disconnect()
            target_vbs_replicas = list()
            for node in self.cluster.nodes_in_cluster[1:]:
                cbstats = Cbstats(node)
                target_vbs_replicas.append(cbstats.vbucket_list(self.cluster.buckets[0].name))
                cbstats.disconnect()
            target_vbs_replicas = [val for vb_lst in target_vbs_replicas for val in vb_lst]

            '''
            Initial config for rebalance
            '''
            servs_in = random.sample(self.available_servers, self.nodes_in)
            self.nodes_cluster = self.cluster.nodes_in_cluster[:]
            self.nodes_cluster.remove(self.cluster.master)
            if rebalance_out_master and self.nodes_out > 0:
                self.nodes_out -= 1
            servs_out = random.sample(self.nodes_cluster, self.nodes_out)

            if rebalance_out_master:
                self.nodes_out += 1
                servs_out.append(self.cluster.master)
                self.nodes_cluster.insert(0, self.cluster.master)

            if self.nodes_in == self.nodes_out:
                self.vbucket_check = False

            start = start_items
            self.log.debug("Iteration == {}, State files before stopping persistence == {}".
                           format(i, self.get_state_files(self.buckets[0])))
            shell_conn = list()
            for node in self.cluster.nodes_in_cluster:
                shell_conn.append(RemoteMachineShellConnection(node))
            # Stopping persistence on Node-x
            self.log.debug("Iteration == {}, stopping persistence".format(i))
            Cbepctl(shell_conn[0]).persistence(self.cluster.buckets[0].name, "stop")

            ###############################################################
            '''
            STEP - 3
              -- Load documents on master node
              -- This step ensures creation of atleast one new
                 state file
            '''
            time_start = time.time()
            tasks_in = dict()
            for collection in collections[0:collections_for_rollback]:
                self.compute_docs(start, mem_only_items)
                self.gen_create = None
                self.gen_update = None
                self.gen_delete = None
                self.gen_expiry = None
                self.generate_docs(doc_ops=self.doc_ops,
                                   target_vbucket=self.target_vbucket)
                tem_tasks_info = self.loadgen_docs(self.retry_exceptions,
                                  scope=scope_name,
                                  collection=collection,
                                  _sync=False)
                tasks_in.update(tem_tasks_info.items())

            for task in tasks_in:
                self.task_manager.get_task_result(task)
            self.bucket_util.verify_doc_op_task_exceptions(
                tasks_in, self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_in)

            if time.time() < time_start + 60:
                self.sleep(time_start + 60 - time.time(),
                                   "Sleep to ensure creation of state files for roll back")
            self.log.info("Rollback Iteration== {},\
            state file after stopping persistence and after doc-ops == {}".
            format(i, self.get_state_files(self.buckets[0])))

            ###############################################################
            '''
            STEP - 4
              -- Kill Memcached on master node
                 which triggers roll back on other nodes
             -- Start rebalance task
             -- Also start doc ops on replica nodes
            '''
            tasks_in = dict()
            shell_conn[0].kill_memcached()
            self.assertTrue(self.bucket_util._wait_warmup_completed(
                self.cluster.buckets[0], servers=[self.cluster.master],
                wait_time=self.wait_timeout * 20))
            rebalance_task = self.task.async_rebalance(self.cluster,
                                                       servs_in, servs_out,
                                                       check_vbucket_shuffling=self.vbucket_check,
                                                       retry_get_process_num=150)
            for collection in collections[0:collections_for_rollback]:
                self.compute_docs(start, mem_only_items)
                self.gen_create = None
                self.gen_update = None
                self.gen_delete = None
                self.gen_expiry = None
                self.generate_docs(doc_ops="create:expiry:update",
                                   target_vbucket=target_vbs_replicas)
                tem_tasks_info = self.loadgen_docs(retry_exceptions=self.retry_exceptions,
                                                   scope=scope_name,
                                                   collection=collection,
                                                   _sync=False)
                tasks_in.update(tem_tasks_info.items())

            self.task.jython_task_manager.get_task_result(rebalance_task)
            self.assertTrue(rebalance_task.result, "Rebalance Failed")

            for task in tasks_in:
                self.task_manager.get_task_result(task)

            if self.gen_create is not None:
                start_items = self.gen_create.key_counter
                self.log.debug("Iteration-{},start_items after load during rollback is {}"
                               .format(i, start_items))

            self.log.debug("Iteration == {},State files after killing memCached ".
                           format(i, self.get_state_files(self.buckets[0])))

            self.available_servers = [servs for servs in self.available_servers
                                      if servs not in servs_in]
            self.available_servers += servs_out
            self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster)
                                             - set(servs_out))
            if rebalance_out_master:
                self.log.debug("Updating master")
                self.cluster.master = self.cluster.nodes_in_cluster[0]

            ###############################################################
            '''
            STEP -5
               -- Restarting persistence master node
            '''
            self.log.debug("Iteration == {}, Restarting persistence".format(i))
            Cbepctl(shell_conn[0]).persistence(self.cluster.buckets[0].name, "start")

            self.sleep(5, "Sleep after re-starting persistence, Iteration{}".format(i))

            ###################################################################
            '''
            STEP - 6
              -- Load Docs on all the nodes
              -- Loading of doc for 60 seconds
              -- Ensures creation of new state file
            '''
            self.create_start = start_items
            self.create_end = start_items + start_items // 5
            tasks_info = dict()
            self.generate_docs(doc_ops="create", target_vbucket=None)
            time_start = time.time()
            for collection in collections:
                tem_tasks_info = self.loadgen_docs(
                    self.retry_exceptions,
                    self.ignore_exceptions,
                    scope=scope_name,
                    collection=collection,
                    _sync=False,
                    doc_ops="create")
                tasks_info.update(tem_tasks_info.items())

            for task in tasks_info:
                self.task_manager.get_task_result(task)

            self.bucket_util.verify_doc_op_task_exceptions(
                tasks_info, self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)
            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets,
                                                         timeout=1200)

            if time.time() < time_start + 60:
                self.sleep(time_start + 60 - time.time(),
                           "After new creates, sleeping , itr=%s" % i)

            start_items = start_items + start_items // 5
            self.log.debug("Iteration == {}, start_items={}".format(i, start_items))

            ###################################################################
            '''
            STEP - 7
               -- Ensures,  For rebalance in next iteration
                  number of nodes should be equal to number of nodes
                  which were availble during initial iteration
            '''
            rebalance_required = False
            if len(self.cluster.nodes_in_cluster) < init_nodes_count:
                nodes_in = init_nodes_count - len(self.cluster.nodes_in_cluster)
                servs_in = random.sample(self.available_servers, nodes_in)
                servs_out = []
                rebalance_required = True

            if len(self.cluster.nodes_in_cluster) > init_nodes_count:
                nodes_out = len(self.cluster.nodes_in_cluster) - init_nodes_count
                self.nodes_cluster = self.cluster.nodes_in_cluster[:]
                self.nodes_cluster.remove(self.cluster.master)
                servs_out = random.sample(self.nodes_cluster, nodes_out)
                servs_in = []
                rebalance_required = True

            if rebalance_required:
                self.log.debug("Iteration=={}, Rebalance before moving to next iteration")
                rebalance_task = self.task.async_rebalance(
                    self.cluster, servs_in, servs_out,
                    check_vbucket_shuffling=self.vbucket_check,
                    retry_get_process_num=150)

                self.task.jython_task_manager.get_task_result(rebalance_task)
                self.assertTrue(rebalance_task.result, "Rebalance Failed")

                self.available_servers = [servs for servs in self.available_servers
                                  if servs not in servs_in]
                self.available_servers += servs_out
                self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster)
                                             - set(servs_out))

            self.cluster.nodes_in_cluster.remove(self.cluster.master)
            self.cluster.nodes_in_cluster.insert(0, self.cluster.master)

            for shell in shell_conn:
                shell.disconnect()


    def test_magma_rollback_with_large_docs(self):

        # Number of docs of different sizes
        num_2mb_docs = self.input.param("num_2mb_docs", 1000)
        num_4mb_docs = self.input.param("num_4mb_docs", 1000)
        num_8mb_docs = self.input.param("num_8mb_docs", 1000)
        num_16mb_docs = self.input.param("num_16mb_docs", 500)
        num_20mb_docs = self.input.param("num_20mb_docs", 500)

        upsert_iterations = self.input.param("upsert_iterations", 0)
        bucket = self.cluster.buckets[0]
        self.cluster.kv_nodes = self.cluster_util.get_kv_nodes(self.cluster,
                                                               self.cluster.nodes_in_cluster)

        # Initial data load
        self.load_data_cbc_pillowfight(self.cluster.master, bucket, num_2mb_docs, 2548000, "2mbbig_docs")
        self.load_data_cbc_pillowfight(self.cluster.master, bucket, num_4mb_docs, 4548000, "4mbbig_docs")
        self.load_data_cbc_pillowfight(self.cluster.master, bucket, num_8mb_docs, 8548000, "8mbbig_docs")
        self.load_data_cbc_pillowfight(self.cluster.master, bucket, num_16mb_docs, 16548000, "16mbbig_docs")
        self.load_data_cbc_pillowfight(self.cluster.master, bucket, num_20mb_docs, 20548000, "20mbbig_docs")

        self.sleep(20, "Wait for num_items to get reflected")
        self.bucket_util.print_bucket_stats(self.cluster)
        self.fetch_vbucket_size(bucket)

        total_iter = upsert_iterations
        while upsert_iterations > 0:
            self.log.info("Upserting docs. Iteration: {}".format(total_iter - upsert_iterations + 1))
            self.load_data_cbc_pillowfight(self.cluster.master, bucket, num_2mb_docs, 2548000, "2mbbig_docs")
            self.load_data_cbc_pillowfight(self.cluster.master, bucket, num_4mb_docs, 4548000, "4mbbig_docs")
            self.load_data_cbc_pillowfight(self.cluster.master, bucket, num_8mb_docs, 8548000, "8mbbig_docs")
            self.load_data_cbc_pillowfight(self.cluster.master, bucket, num_16mb_docs, 16548000, "16mbbig_docs")
            self.load_data_cbc_pillowfight(self.cluster.master, bucket, num_20mb_docs, 20548000, "20mbbig_docs")
            upsert_iterations -= 1

        self.fetch_vbucket_size(bucket)

        shell = RemoteMachineShellConnection(self.cluster.master)
        cbstats = Cbstats(self.cluster.master)
        self.target_vbucket = cbstats.vbucket_list(bucket.name)
        self.log.info("Vbuckets on the master node = {}".format(self.target_vbucket))

        # Ensure state files are created
        self.log.info("State files after initial creates {}"
                     .format(self.get_state_files(bucket)))

        self.sleep(60+10, "Ensures creation of at least one snapshot")
        self.log.info("State files after 60 second of sleep {}"
                      .format(self.get_state_files(bucket)))

        self.sleep(20, "Wait before fetching vbucket-details")
        vb_dict = self.bucket_util.get_vb_details_for_bucket(bucket,
                                        self.cluster.nodes_in_cluster)

        # Stopping persistence on master node (node A)
        Cbepctl(shell).persistence(bucket.name, "stop")
        self.sleep(60+10, "Wait after stopping persistence on the master node")

        self.log.info("State files before data load on the master node {}"
                     .format(self.get_state_files(bucket)))

        self.log.info("Data load on the vbuckets in the master node")
        gen_create = doc_generator("large_docs", 0, 1000,
                                   doc_size=1024000,
                                   target_vbucket=self.target_vbucket,
                                   randomize_value=True)
        task = self.task.async_load_gen_docs(
                        self.cluster, bucket,
                        gen_create, DocLoading.Bucket.DocOps.CREATE,
                        timeout_secs=self.sdk_timeout,
                        process_concurrency=4,
                        skip_read_on_error=True)
        self.task_manager.get_task_result(task)

        self.sleep(60+10, "Wait to ensure creation of state files")
        self.log.info("State files after data load on the master node {}"
                     .format(self.get_state_files(bucket)))

        self.bucket_util.print_bucket_stats(self.cluster)

        vb_dict1 = self.bucket_util.get_vb_details_for_bucket(bucket,
                                        self.cluster.nodes_in_cluster)

        self.log.info("High seqno of active vbuckets")
        for vbucket in range(self.vbuckets):
            vbucket_name = "vb_" + str(vbucket)
            vb_active_seq_prev = vb_dict[vbucket]['active']['high_seqno']
            vb_active_seq = vb_dict1[vbucket]['active']['high_seqno']
            self.log.info("{0}: {1}".format(vbucket_name, vb_active_seq))
            if vbucket in self.target_vbucket:
                self.assertTrue(vb_active_seq > vb_active_seq_prev,
                "High seqno of the active target vbucket {} has not been incremented"
                .format(vbucket_name))

        self.log.info("High seqno of replica vbuckets")
        for vbucket in range(self.vbuckets):
            vbucket_name = "vb_" + str(vbucket)
            replica_list_prev = vb_dict[vbucket]['replica']
            replica_list = vb_dict1[vbucket]['replica']
            for j in range(len(replica_list)):
                vb_replica_seq_prev = replica_list_prev[j]['high_seqno']
                vb_replica_seq = replica_list[j]['high_seqno']
                self.log.info("{0}: {1}".format(vbucket_name, vb_replica_seq))
                if vbucket in self.target_vbucket:
                    self.assertTrue(vb_replica_seq > vb_replica_seq_prev,
                    "High seqno of the replica target vbucket {} has not been incremented"
                    .format(vbucket_name))

        self.log.info("Kill memcached on the master node")
        shell.kill_memcached()

        self.log.info("Wait until warmup is complete")
        self.assertTrue(self.bucket_util._wait_warmup_completed(
                        bucket,
                        servers=[self.cluster.master],
                        wait_time=self.wait_timeout * 10))

        self.log.info("State files after killing memcached on master node== {}".
                          format(self.get_state_files(bucket)))

        # Restarting persistence on the master node
        Cbepctl(shell).persistence(bucket.name, "start")

        self.sleep(20, "Wait before fetching vbucket-details")
        vb_dict2 = self.bucket_util.get_vb_details_for_bucket(bucket,
                                        self.cluster.nodes_in_cluster)

        self.log.info("High seqno of active vbuckets")
        for vbucket in range(self.vbuckets):
            vbucket_name = "vb_" + str(vbucket)
            vb_active_seq_prev = vb_dict[vbucket]['active']['high_seqno']
            vb_active_seq = vb_dict2[vbucket]['active']['high_seqno']
            self.log.info("{0}: {1}".format(vbucket_name, vb_active_seq))
            if vbucket in self.target_vbucket:
                self.assertTrue(vb_active_seq == vb_active_seq_prev,
                "High seqno of the active target vbucket {} does not match after rollback"
                .format(vbucket_name))

        self.log.info("High seqno of replica vbuckets")
        for vbucket in range(self.vbuckets):
            vbucket_name = "vb_" + str(vbucket)
            replica_list_prev = vb_dict[vbucket]['replica']
            replica_list = vb_dict2[vbucket]['replica']
            for j in range(len(replica_list)):
                vb_replica_seq_prev = replica_list_prev[j]['high_seqno']
                vb_replica_seq = replica_list[j]['high_seqno']
                self.log.info("{0}: {1}".format(vbucket_name, vb_replica_seq))
                if vbucket in self.target_vbucket:
                    self.assertTrue(vb_replica_seq == vb_replica_seq_prev,
                    "High seqno of the replica target vbucket {} does not match after rollback"
                    .format(vbucket_name))

        shell.disconnect()
