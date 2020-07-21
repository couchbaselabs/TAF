'''
Created on Dec 12, 2019

@author: riteshagarwal
'''

import copy
import time

from cb_tools.cbstats import Cbstats
from cb_tools.cbepctl import Cbepctl
from couchbase_helper.documentgenerator import doc_generator
from magma_base import MagmaBaseTest
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection
from sdk_exceptions import SDKException


retry_exceptions = [SDKException.TimeoutException,
                    SDKException.AmbiguousTimeoutException,
                    SDKException.RequestCanceledException,
                    SDKException.UnambiguousTimeoutException]


class MagmaRollbackTests(MagmaBaseTest):

    def setUp(self):
        super(MagmaRollbackTests, self).setUp()

        self.create_start = 0
        self.create_end = self.num_items

        self.generate_docs(doc_ops="create")

        self.init_loading = self.input.param("init_loading", True)
        if self.init_loading:
            self.result_task = self._load_all_buckets(
                self.cluster, self.gen_create,
                "create", 0,
                batch_size=self.batch_size,
                dgm_batch=self.dgm_batch)

            if self.active_resident_threshold != 100:
                for task in self.result_task.keys():
                    self.num_items = task.doc_index

            self.log.info("Verifying num_items counts after doc_ops")
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(self.num_items)

        self.cluster_util.print_cluster_stats()
        self.bucket_util.print_bucket_stats()
        self.graceful = self.input.param("graceful", False)

    def tearDown(self):
        super(MagmaRollbackTests, self).tearDown()

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

    def test_magma_rollback_n_times(self):
        items = self.num_items
        mem_only_items = self.input.param("rollback_items", 100000)
        if self.nodes_init < 2 or self.num_replicas < 1:
            self.fail("Not enough nodes/replicas in the cluster/bucket \
            to test rollback")
        self.num_rollbacks = self.input.param("num_rollbacks", 10)
        shell = RemoteMachineShellConnection(self.cluster_util.cluster.master)
        cbstats = Cbstats(shell)
        self.target_vbucket = cbstats.vbucket_list(self.bucket_util.buckets[0].
                                                   name)
        start = self.num_items
        self.gen_read = copy.deepcopy(self.gen_create)
        for _ in xrange(1, self.num_rollbacks+1):
            # Stopping persistence on NodeA
            mem_client = MemcachedClientHelper.direct_client(
                self.cluster_util.cluster.master, self.bucket_util.buckets[0])
            mem_client.stop_persistence()
            self.gen_create = self.gen_docs_basic_for_target_vbucket(start, mem_only_items,
                                                                     self.target_vbucket)

            self.loadgen_docs(_sync=True,
                              retry_exceptions=retry_exceptions)
            start = self.gen_create.key_counter

            ep_queue_size_map = {self.cluster.nodes_in_cluster[0]:
                                 mem_only_items}
            vb_replica_queue_size_map = {self.cluster.nodes_in_cluster[0]: 0}

            for node in self.cluster.nodes_in_cluster[1:]:
                ep_queue_size_map.update({node: 0})
                vb_replica_queue_size_map.update({node: 0})

            for bucket in self.bucket_util.buckets:
                self.bucket_util._wait_for_stat(bucket, ep_queue_size_map)
                self.bucket_util._wait_for_stat(
                    bucket,
                    vb_replica_queue_size_map,
                    stat_name="vb_replica_queue_size")

            # Kill memcached on NodeA to trigger rollback on other Nodes
            # replica vBuckets
            for bucket in self.bucket_util.buckets:
                self.log.debug(cbstats.failover_stats(bucket.name))
            shell.kill_memcached()

            self.assertTrue(self.bucket_util._wait_warmup_completed(
                [self.cluster_util.cluster.master],
                self.bucket_util.buckets[0],
                wait_time=self.wait_timeout * 10))
            self.sleep(10, "Not Required, but waiting for 10s after warm up")

            self.bucket_util.verify_stats_all_buckets(items, timeout=300)
            for bucket in self.bucket_util.buckets:
                self.log.debug(cbstats.failover_stats(bucket.name))

        data_validation = self.task.async_validate_docs(
                self.cluster, self.bucket_util.buckets[0],
                self.gen_read, "create", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                pause_secs=5, timeout_secs=self.sdk_timeout)
        self.task.jython_task_manager.get_task_result(data_validation)

        shell.disconnect()

    def test_magma_rollback_n_times_with_del_op(self):
        self.log.info("test_magma_rollback_n_times_with_del_op starts")

        if self.nodes_init < 2 or self.num_replicas < 1:
            self.fail("Not enough nodes/replicas in the cluster/bucket \
            to test rollback")

        items = self.num_items
        shell = RemoteMachineShellConnection(self.cluster_util.cluster.master)
        cbstats = Cbstats(shell)
        self.target_vbucket = cbstats.vbucket_list(self.bucket_util.buckets[0].name)
        self.gen_create = self.gen_docs_basic_for_target_vbucket(0, self.num_items,
                                                                 self.target_vbucket)

        self.doc_ops = "create"
        self.loadgen_docs(_sync=True, retry_exceptions=retry_exceptions)
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(items)

        self.cluster_util.print_cluster_stats()
        self.bucket_util.print_bucket_stats()

        mem_only_items = self.input.param("rollback_items", 100000)
        self.num_rollbacks = self.input.param("num_rollbacks", 10)

        self.doc_ops = "delete"
        start = 0
        for i in range(1, self.num_rollbacks+1):
            # Stopping persistence on NodeA
            self.log.info("Iteration=={}".format(i))

            mem_client = MemcachedClientHelper.direct_client(
                self.cluster_util.cluster.master, self.bucket_util.buckets[0])
            mem_client.stop_persistence()

            self.gen_delete = self.gen_docs_basic_for_target_vbucket(start, mem_only_items,
                                                                     self.target_vbucket)

            self.loadgen_docs(_sync=True,
                              retry_exceptions=retry_exceptions)
            # start = self.gen_delete.key_counter

            ep_queue_size_map = {self.cluster.nodes_in_cluster[0]:
                                 mem_only_items}
            vb_replica_queue_size_map = {self.cluster.nodes_in_cluster[0]: 0}

            for node in self.cluster.nodes_in_cluster[1:]:
                ep_queue_size_map.update({node: 0})
                vb_replica_queue_size_map.update({node: 0})

            for bucket in self.bucket_util.buckets:
                self.bucket_util._wait_for_stat(bucket, ep_queue_size_map)
                self.bucket_util._wait_for_stat(
                    bucket,
                    vb_replica_queue_size_map,
                    stat_name="vb_replica_queue_size")

            self.validate_data("delete", self.gen_delete)

            # Kill memcached on NodeA to trigger rollback on other Nodes
            # replica vBuckets
            for bucket in self.bucket_util.buckets:
                self.log.debug(cbstats.failover_stats(bucket.name))
            shell.kill_memcached()

            self.assertTrue(self.bucket_util._wait_warmup_completed(
                [self.cluster_util.cluster.master],
                self.bucket_util.buckets[0],
                wait_time=self.wait_timeout * 10))
            self.sleep(10, "Not Required, but waiting for 10s after warm up")

            self.bucket_util.verify_stats_all_buckets(items, timeout=300)
            for bucket in self.bucket_util.buckets:
                self.log.debug(cbstats.failover_stats(bucket.name))

        shell.disconnect()
        self.log.info("test_magma_rollback_n_times_with_del_op ends")

    def test_magma_rollback_to_0(self):
        items = self.num_items
        mem_only_items = self.input.param("rollback_items", 10000)
        if self.nodes_init < 2 or self.num_replicas < 1:
            self.fail("Not enough nodes/replicas in the cluster/bucket\
                      to test rollback")

        self.num_rollbacks = self.input.param("num_rollbacks", 10)
        shell = RemoteMachineShellConnection(self.cluster_util.cluster.master)
        self.target_vbucket = Cbstats(shell).vbucket_list(self.bucket_util.
                                                          buckets[0].name)
        start = self.num_items

        # Stopping persistence on NodeA
        mem_client = MemcachedClientHelper.direct_client(
            self.input.servers[0], self.bucket_util.buckets[0])
        mem_client.stop_persistence()

        for i in xrange(1, self.num_rollbacks+1):
            self.gen_create = doc_generator(
                self.key, start, mem_only_items,
                doc_size=self.doc_size, doc_type=self.doc_type,
                target_vbucket=self.target_vbucket,
                vbuckets=self.cluster_util.vbuckets,
                randomize_doc_size=self.randomize_doc_size,
                randomize_value=self.randomize_value)

            self.loadgen_docs(_sync=True,
                              retry_exceptions=retry_exceptions)

            start = self.gen_create.key_counter
            stat_map = {self.cluster.nodes_in_cluster[0]: mem_only_items*i}
            for node in self.cluster.nodes_in_cluster[1:]:
                stat_map.update({node: 0})

            for bucket in self.bucket_util.buckets:
                self.bucket_util._wait_for_stat(bucket, stat_map)
            self.sleep(60)

        shell.kill_memcached()
        self.assertTrue(self.bucket_util._wait_warmup_completed(
            [self.cluster_util.cluster.master],
            self.bucket_util.buckets[0],
            wait_time=self.wait_timeout * 10))
        self.bucket_util.verify_stats_all_buckets(items)
        shell.disconnect()
        self.log.info("test_magma_rollback_n_times_during_del_op starts")

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

        if self.nodes_init < 2 or self.num_replicas < 1:
            self.fail("Not enough nodes/replicas in the cluster/bucket \
            to test rollback")

        self.duration = self.input.param("duration", 2)
        self.num_rollbacks = self.input.param("num_rollbacks", 10)

        shell = RemoteMachineShellConnection(self.cluster_util.cluster.master)
        cbstats = Cbstats(shell)
        self.target_vbucket = cbstats.vbucket_list(self.bucket_util.buckets[0].name)

        self.gen_read = copy.deepcopy(self.gen_create)

        ###############################################################################
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

        ###############################################################################
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
            mem_client = MemcachedClientHelper.direct_client(
                self.cluster_util.cluster.master, self.bucket_util.buckets[0])
            mem_client.stop_persistence()

        ###############################################################################
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
                                  retry_exceptions=retry_exceptions)

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

            for bucket in self.bucket_util.buckets:
                self.bucket_util._wait_for_stat(bucket, ep_queue_size_map)
                self.bucket_util._wait_for_stat(
                    bucket,
                    vb_replica_queue_size_map,
                    stat_name="vb_replica_queue_size")

            # replica vBuckets
            for bucket in self.bucket_util.buckets:
                self.log.debug(cbstats.failover_stats(bucket.name))
        ###############################################################################
            '''
            STEP - 4
              -- Kill Memcached on master node(Node A) and trigger rollback on replica/ nodes
            '''
            shell.kill_memcached()

            self.assertTrue(self.bucket_util._wait_warmup_completed(
                [self.cluster_util.cluster.master],
                self.bucket_util.buckets[0],
                wait_time=self.wait_timeout * 10))

            self.log.info("Iteration= {}, State files after killing memcached on master node== {}".
                          format(i, self.get_state_files(self.buckets[0])))

            self.sleep(10, "Not Required, but waiting for 10s after warm up")
            self.bucket_util.verify_stats_all_buckets(items, timeout=300)
            for bucket in self.bucket_util.buckets:
                self.log.debug(cbstats.failover_stats(bucket.name))
        ###############################################################################
        '''
        STEP - 5
          -- Data Validation
        '''
        data_validation = self.task.async_validate_docs(
                self.cluster, self.bucket_util.buckets[0],
                self.gen_read, "create", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                pause_secs=5, timeout_secs=self.sdk_timeout)
        self.task.jython_task_manager.get_task_result(data_validation)
        ###############################################################################
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
        ops_len = len(self.doc_ops.split(":"))

        if self.nodes_init < 2 or self.num_replicas < 1:
            self.fail("Not enough nodes/replicas in the cluster/bucket \
            to test rollback")

        self.duration = self.input.param("duration", 2)
        self.num_rollbacks = self.input.param("num_rollbacks", 10)

        shell = RemoteMachineShellConnection(self.cluster_util.cluster.master)
        cbstats = Cbstats(shell)
        self.target_vbucket = cbstats.vbucket_list(self.bucket_util.buckets[0].name)

        ###############################################################################
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

        ###############################################################################
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
            Cbepctl(shell).persistence(self.bucket_util.buckets[0].name, "stop")

        ###############################################################################
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
                                  retry_exceptions=retry_exceptions)
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

            for bucket in self.bucket_util.buckets:
                self.bucket_util._wait_for_stat(bucket, ep_queue_size_map)
                self.bucket_util._wait_for_stat(
                    bucket,
                    vb_replica_queue_size_map,
                    stat_name="vb_replica_queue_size")

            # replica vBuckets
            for bucket in self.bucket_util.buckets:
                self.log.debug(cbstats.failover_stats(bucket.name))
        ###############################################################################
            '''
            STEP - 4
              -- Kill Memcached on master node(Node A) and trigger rollback on replica/other nodes
            '''

            shell.kill_memcached()

            self.assertTrue(self.bucket_util._wait_warmup_completed(
                [self.cluster_util.cluster.master],
                self.bucket_util.buckets[0],
                wait_time=self.wait_timeout * 10))

            self.log.info("Iteration == {},State files after killing memcached {}".
                          format(i, self.get_state_files(self.buckets[0])))

            self.sleep(10, "Not Required, but waiting for 10s after warm up")
            self.bucket_util.verify_stats_all_buckets(items, timeout=300)
            for bucket in self.bucket_util.buckets:
                self.log.debug(cbstats.failover_stats(bucket.name))
        ###############################################################################
            '''
            STEP -5
              -- Restarting persistence on master node(Node A)
            '''

            self.log.debug("Iteration=={}, Re-Starting persistence".format(i))
            Cbepctl(shell).persistence(self.bucket_util.buckets[0].name, "start")
        ###############################################################################
            '''
            STEP - 6
              -- Load Docs on all the nodes
              -- Loading of doc for 60 seconds
              -- Ensures creation of new state file
            '''
            self.create_start = items
            self.create_end = items + items // 3
            self.generate_docs(doc_ops="create", target_vbucket=None)

            time_end = time.time() + 60
            while time.time() < time_end:
                time_start = time.time()
                _ = self.loadgen_docs(self.retry_exceptions,
                                      self.ignore_exceptions,
                                      _sync=True,
                                      doc_ops="create")
                self.bucket_util._wait_for_stats_all_buckets()
                if time.time() < time_start + 60:
                    self.sleep(time_start + 60 - time.time(), "After new creates, sleeping , itr={}".format(i))

            items = items + items // 3
            self.log.debug("Iteration == {}, Total num_items {}".format(i, items))
        ###############################################################################

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

        cbstats = Cbstats(shell_conn[0])
        self.target_vbucket = cbstats.vbucket_list(self.bucket_util.buckets[0].
                                                   name)

        target_vbs_replicas = list()
        for shell in shell_conn[1:]:
            cbstats = Cbstats(shell)
            target_vbs_replicas.append(cbstats.vbucket_list(self.bucket_util.buckets[0].name))

        target_vbs_replicas = [val for vb_lst in target_vbs_replicas for val in vb_lst]

        ###############################################################################
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

        ###############################################################################
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
            Cbepctl(shell_conn[0]).persistence(self.bucket_util.buckets[0].name, "stop")

        ###############################################################################
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

                self.gen_create = self.gen_docs_basic_for_target_vbucket(start,
                                                                         mem_only_items,
                                                                         self.target_vbucket)
                self.loadgen_docs(_sync=True,
                                  retry_exceptions=retry_exceptions)

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

            for bucket in self.bucket_util.buckets:
                self.bucket_util._wait_for_stat(bucket, ep_queue_size_map)
                self.bucket_util._wait_for_stat(
                    bucket,
                    vb_replica_queue_size_map,
                    stat_name="vb_replica_queue_size")

            # replica vBuckets
            for bucket in self.bucket_util.buckets:
                self.log.debug(cbstats.failover_stats(bucket.name))

        ###############################################################################
            '''
            STEP - 4
              -- Kill Memcached on master node(Node A) and trigger rollback on replica/ nodes
            '''

            shell_conn[0].kill_memcached()

            self.assertTrue(self.bucket_util._wait_warmup_completed(
                [self.cluster_util.cluster.master],
                self.bucket_util.buckets[0],
                wait_time=self.wait_timeout * 10))

            self.log.debug("Iteration == {}, State files after killing memcached on master node == {}".
                          format(i, self.get_state_files(self.buckets[0])))

            self.bucket_util.verify_stats_all_buckets(items, timeout=300)
            for bucket in self.bucket_util.buckets:
                self.log.debug(cbstats.failover_stats(bucket.name))

        ###############################################################################
            '''
            STEP -5
              -- Restarting persistence on master node(Node A)
              -- Stopping persistence on other nodes
            '''

            self.log.debug("Iteration=={}, Re-Starting persistence on master node".format(i))
            Cbepctl(shell_conn[0]).persistence(self.bucket_util.buckets[0].name, "start")

            for shell in shell_conn[1:]:
                Cbepctl(shell).persistence(self.bucket_util.buckets[0].name, "stop")

        ###############################################################################
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
                self.gen_create = self.gen_docs_basic_for_target_vbucket(start_2,
                                                                         mem_only_items,
                                                                         target_vbs_replicas)

                self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=True)

                start_2 = self.gen_create.key_counter

                self.log.debug("slave_itr == {}, Loading docs on master".format(slave_itr))
                self.gen_create = self.gen_docs_basic_for_target_vbucket(start_2,
                                                                         mem_only_items,
                                                                         self.target_vbucket)
                self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=True)

                start_2 = self.gen_create.key_counter

                ep_queue_size_map = {self.cluster.nodes_in_cluster[0]:
                                     0}
                vb_replica_queue_size_map = {self.cluster.nodes_in_cluster[0]: 0}
                for bucket in self.bucket_util.buckets:
                    self.bucket_util._wait_for_stat(bucket, ep_queue_size_map)
                    self.bucket_util._wait_for_stat(bucket, vb_replica_queue_size_map,
                                                     stat_name="vb_replica_queue_size")

                if time.time() < time_start + 60:
                    self.sleep(time_start + 60 - time.time(), "After new creates, sleeping , slave_itr == {}".
                               format(slave_itr))

            items = items + mem_item_count
            start = start_2
            self.log.debug("Iteration == {}, Total num_items {}".format(i, items))

        ###############################################################################
            '''
            STEP -7
              -- Kill MemCached on Slave nodes, and trigger rollback on master
              -- ReStart persistence on Slave nodes
            '''

            for shell in shell_conn[1:]:
                shell.kill_memcached()

            for node in self.cluster.nodes_in_cluster[1:]:
                self.assertTrue(self.bucket_util._wait_warmup_completed(
                    [node], self.bucket_util.buckets[0],
                    wait_time=self.wait_timeout * 10))

            for shell in shell_conn[1:]:
                Cbepctl(shell).persistence(self.bucket_util.buckets[0].name, "start")
        ###############################################################################

            self.log.info("State file at end of iteration-{} are == {}".
                          format(i, self.get_state_files(self.buckets[0])))

            self.log.debug("'Wait for stats' after starting persistence, Iteration{}".format(i))
            self.sleep(5)
            for bucket in self.bucket_util.buckets:
                self.bucket_util._wait_for_stat(bucket, vb_replica_queue_size_map,
                                                stat_name="vb_replica_queue_size")
                self.bucket_util._wait_for_stat(bucket, ep_queue_size_map)

        for shell in shell_conn:
            shell.disconnect()

    def test_magma_rollback_on_all_nodes_one_at_a_time_to_same_snapshot(self):
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
        items = self.num_items
        mem_only_items = self.input.param("rollback_items", 10000)
        self.gen_read = copy.deepcopy(self.gen_create)

        if self.nodes_init < 2 or self.num_replicas < 1:
            self.fail("Not enough nodes/replicas in the cluster/bucket \
            to test rollback")

        self.duration = self.input.param("duration", 2)
        self.num_rollbacks = self.input.param("num_rollbacks", 10)

        ###############################################################################
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

        ###############################################################################
        '''
        STEP - 2,  Stop persistence on node - x
        '''

        for i in range(1, self.num_rollbacks+1):
            self.log.info("Roll back Iteration == {}".format(i))
            for x, node in enumerate(self.cluster.nodes_in_cluster):
                start = items
                shell = RemoteMachineShellConnection(node)
                cbstats = Cbstats(shell)
                self.target_vbucket = cbstats.vbucket_list(self.bucket_util.buckets[0].
                                                   name)
                mem_item_count = 0
                self.log.debug("Iteration == {}, State files before stopping persistence == {}".
                           format(i, self.get_state_files(self.buckets[0])))
                # Stopping persistence on Node-x
                self.log.debug("Iteration == {}, Stopping persistence on Node-{}"
                               .format(i, x+1))
                Cbepctl(shell).persistence(self.bucket_util.buckets[0].name, "stop")

        ###############################################################################
                '''
                STEP - 3
                  -- Load documents on node  x for  self.duration * 60 seconds
                  -- This step ensures new state files (number equal to self.duration)
                '''
                time_end = time.time() + 60 * self.duration
                itr = 0
                while time.time() < time_end:
                    itr += 1
                    time_start = time.time()
                    mem_item_count += mem_only_items
                    self.gen_create = self.gen_docs_basic_for_target_vbucket(start,
                                                                         mem_only_items,
                                                                         self.target_vbucket)
                    self.loadgen_docs(_sync=True,
                                      retry_exceptions=retry_exceptions)

                    start = self.gen_create.key_counter

                    if time.time() < time_start + 60:
                        self.log.info("Rollback Iteration = {}, itr == {}, Active-Node- {}".format(i, itr, x+1))
                        self.sleep(time_start + 60 - time.time(),
                                   "Sleep to ensure creation of state files for roll back")
                        self.log.info("state files == {}".format(
                                     self.get_state_files(self.buckets[0])))

                ep_queue_size_map = {node:
                                     mem_item_count}
                vb_replica_queue_size_map = {node: 0}

                for nod in self.cluster.nodes_in_cluster:
                    if nod != node:
                        ep_queue_size_map.update({nod: 0})
                        vb_replica_queue_size_map.update({nod: 0})

                for bucket in self.bucket_util.buckets:
                    self.bucket_util._wait_for_stat(bucket, ep_queue_size_map)
                    self.bucket_util._wait_for_stat(bucket, vb_replica_queue_size_map,
                                                    stat_name="vb_replica_queue_size")
                # replica vBuckets
                for bucket in self.bucket_util.buckets:
                    self.log.debug(cbstats.failover_stats(bucket.name))

        ###############################################################################
                '''
                STEP - 4
                  -- Kill Memcached on Node - x and trigger rollback on other nodes
                '''

                shell.kill_memcached()
                self.assertTrue(self.bucket_util._wait_warmup_completed(
                    [self.cluster_util.cluster.master],
                    self.bucket_util.buckets[0],
                    wait_time=self.wait_timeout * 10))

                self.log.debug("Iteration == {}, Node-- {} State files after killing memcached ".
                          format(i, x, self.get_state_files(self.buckets[0])))

                self.bucket_util.verify_stats_all_buckets(items, timeout=300)
                for bucket in self.bucket_util.buckets:
                    self.log.debug(cbstats.failover_stats(bucket.name))

        ###############################################################################
                '''
                STEP -5
                   -- Restarting persistence on Node -- x
                '''

                self.log.debug("Iteration=={}, Re-Starting persistence on Node -- {}".format(i, x))
                Cbepctl(shell).persistence(self.bucket_util.buckets[0].name, "start")

                self.log.info("State file at end of iteration-{} are == {}".
                              format(i, self.get_state_files(self.buckets[0])))
                self.sleep(5, "Sleep after re-starting persistence, Iteration{}".format(i))

                shell.disconnect()
        ###############################################################################
        '''
        STEP - 6
          -- Data Validation
        '''
        data_validation = self.task.async_validate_docs(
                self.cluster, self.bucket_util.buckets[0],
                self.gen_read, "create", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                pause_secs=5, timeout_secs=self.sdk_timeout)
        self.task.jython_task_manager.get_task_result(data_validation)
        ###############################################################################

    def test_magma_rollback_on_all_nodes_one_at_a_time_to_new_snapshot(self):
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
        items = self.num_items
        mem_only_items = self.input.param("rollback_items", 10000)
        self.gen_read = copy.deepcopy(self.gen_create)

        if self.nodes_init < 2 or self.num_replicas < 1:
            self.fail("Not enough nodes/replicas in the cluster/bucket \
            to test rollback")

        self.duration = self.input.param("duration", 2)
        self.num_rollbacks = self.input.param("num_rollbacks", 10)

        ###############################################################################
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

        ###############################################################################
        '''
        STEP - 2,  Stop persistence on node - x
        '''

        for i in range(1, self.num_rollbacks+1):
            self.log.info("Roll back Iteration == {}".format(i))
            for x, node in enumerate(self.cluster.nodes_in_cluster):
                start = items
                shell = RemoteMachineShellConnection(node)
                cbstats = Cbstats(shell)
                self.target_vbucket = cbstats.vbucket_list(self.bucket_util.buckets[0].
                                                   name)
                mem_item_count = 0
                self.log.debug("Iteration == {}, State files before stopping persistence == {}".
                           format(i, self.get_state_files(self.buckets[0])))
                # Stopping persistence on Node-x
                self.log.debug("Iteration == {}, Stopping persistence on Node-{}"
                               .format(i, x+1))
                Cbepctl(shell).persistence(self.bucket_util.buckets[0].name, "stop")

        ###############################################################################
                '''
                STEP - 3
                  -- Load documents on node  x for  self.duration * 60 seconds
                  -- This step ensures new state files (number equal to self.duration)
                '''
                time_end = time.time() + 60 * self.duration
                itr = 0
                while time.time() < time_end:
                    itr += 1
                    time_start = time.time()
                    mem_item_count += mem_only_items
                    self.gen_create = self.gen_docs_basic_for_target_vbucket(start,
                                                                         mem_only_items,
                                                                         self.target_vbucket)
                    self.loadgen_docs(_sync=True,
                                      retry_exceptions=retry_exceptions)

                    start = self.gen_create.key_counter

                    if time.time() < time_start + 60:
                        self.log.info("Rollback Iteration = {}, itr == {}, Active-Node- {}".format(i, itr, x+1))
                        self.sleep(time_start + 60 - time.time(),
                                   "Sleep to ensure creation of state files for roll back")
                        self.log.info("state files == {}".format(
                                     self.get_state_files(self.buckets[0])))

                ep_queue_size_map = {node:
                                     mem_item_count}
                vb_replica_queue_size_map = {node: 0}

                for nod in self.cluster.nodes_in_cluster:
                    if nod != node:
                        ep_queue_size_map.update({nod: 0})
                        vb_replica_queue_size_map.update({nod: 0})

                for bucket in self.bucket_util.buckets:
                    self.bucket_util._wait_for_stat(bucket, ep_queue_size_map)
                    self.bucket_util._wait_for_stat(bucket, vb_replica_queue_size_map,
                                                    stat_name="vb_replica_queue_size")
                # replica vBuckets
                for bucket in self.bucket_util.buckets:
                    self.log.debug(cbstats.failover_stats(bucket.name))

        ###############################################################################
                '''
                STEP - 4
                  -- Kill Memcached on Node - x and trigger rollback on other nodes
                '''

                shell.kill_memcached()
                self.assertTrue(self.bucket_util._wait_warmup_completed(
                    [self.cluster_util.cluster.master],
                    self.bucket_util.buckets[0],
                    wait_time=self.wait_timeout * 10))

                self.log.debug("Iteration == {}, Node-- {} State files after killing memcached ".
                          format(i, x, self.get_state_files(self.buckets[0])))

                self.bucket_util.verify_stats_all_buckets(items, timeout=300)
                for bucket in self.bucket_util.buckets:
                    self.log.debug(cbstats.failover_stats(bucket.name))

        ###############################################################################
                '''
                STEP -5
                 -- Restarting persistence on Node -- x
                '''

                self.log.debug("Iteration=={}, Re-Starting persistence on Node -- {}".format(i, x))
                Cbepctl(shell).persistence(self.bucket_util.buckets[0].name, "start")

                self.log.info("State file at end of iteration-{} are == {}".
                              format(i, self.get_state_files(self.buckets[0])))

                self.sleep(5, "Sleep after re-starting persistence, Iteration{}".format(i))

            shell.disconnect()
        ###############################################################################
            '''
            STEP - 6
              -- Load Docs on all the nodes
              -- Loading of doc for 60 seconds
              -- Ensures creation of new state file
            '''
            self.create_start = items
            self.create_end = items + items // 3
            self.generate_docs(doc_ops="create")

            time_end = time.time() + 60
            while time.time() < time_end:
                time_start = time.time()
                self.loadgen_docs(self.retry_exceptions,
                              self.ignore_exceptions, _sync=True)
                self.bucket_util._wait_for_stats_all_buckets()
                if time.time() < time_start + 60:
                    self.sleep(time_start + 60 - time.time(), "After new creates, sleeping , itr={}".format(i))
            items = items + items // 3
            self.log.debug("Iteration == {}, Total num_items {}".format(i, items))
        ###############################################################################


class MagmaSpaceAmplification(MagmaBaseTest):

    def test_space_amplification(self):
        self.num_updates = self.input.param("num_updates", 50)
        self.assertTrue(self.rest.update_autofailover_settings(False, 600),
                        "AutoFailover disabling failed")
        self.doc_ops = "update"

        initial_data_size = 0
        for bucket in self.bucket_util.buckets:
            initial_data_size += self.get_disk_usage(bucket)[0]

        metadata_size = 200
        exact_size = self.num_items*(self.doc_size + metadata_size)\
            * (1 + self.num_replicas)
        max_size = initial_data_size * 2.5
        for i in xrange(1, self.num_updates+1):
            self.log.info("Iteration: {}, updating {} items".
                          format(i, self.num_items))

            self.gen_update = doc_generator(
                self.key, 0, self.num_items,
                doc_size=self.doc_size, doc_type=self.doc_type,
                target_vbucket=self.target_vbucket,
                vbuckets=self.cluster_util.vbuckets,
                randomize_doc_size=self.randomize_doc_size,
                randomize_value=self.randomize_value)
            self.loadgen_docs(_sync=True,
                              retry_exceptions=retry_exceptions)

            self.bucket_util._wait_for_stats_all_buckets(timeout=self.wait_timeout*20)
            self.bucket_util.print_bucket_stats()

            disk_size = self.get_disk_usage(bucket)[0]
            self.assertTrue(
                disk_size >= initial_data_size and disk_size <= max_size,
                "Exact Data Size {} \n\
                Actual Size {} \n\
                Max Expected Size {}".format(exact_size, disk_size, max_size)
                )
            self.bucket_util.verify_stats_all_buckets(self.num_items,
                                                      timeout=self.wait_timeout*20)
        data_validation = self.task.async_validate_docs(
            self.cluster, self.bucket_util.buckets[0],
            self.gen_update, "update", 0, batch_size=self.batch_size,
            process_concurrency=self.process_concurrency)
        self.task.jython_task_manager.get_task_result(data_validation)
