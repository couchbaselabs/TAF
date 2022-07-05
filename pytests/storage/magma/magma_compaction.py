import copy
import threading

from Cb_constants.CBServer import CbServer
from cb_tools.cbepctl import Cbepctl
from cb_tools.cbstats import Cbstats
from magma_base import MagmaBaseTest
from remote.remote_util import RemoteMachineShellConnection
from sdk_constants.java_client import SDKConstants


class MagmaCompactionTests(MagmaBaseTest):
    def setUp(self):
        super(MagmaCompactionTests, self).setUp()
        self.sdk_timeout = self.input.param("sdk_timeout", 10)
        self.time_unit = "seconds"
        self.graceful = self.input.param("graceful", False)
        self.assertTrue(self.rest.update_autofailover_settings(False, 600),
                        "AutoFailover disabling failed")
        self.crash_th = None
        self.compaction_th = None
        self.compact_before = self.input.param("compact_before", False)
        self.compact_after = self.input.param("compact_after", False)
        self.sdk_retry_strategy = self.input.param("sdk_retry_strategy",
                                                   SDKConstants.RetryStrategy.FAIL_FAST)
    def tearDown(self):
        self.stop_crash = True
        self.stop_compaction = True
        if self.crash_th and self.crash_th.is_alive():
            self.crash_th.join()
        if self.compaction_th and self.compaction_th.is_alive():
            self.compaction_th.join()
        super(MagmaCompactionTests, self).tearDown()

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

    def test_crash_during_compaction(self):
        '''
         -- This test covers below scenarios
            -- Crash during compaction
            -- Compaction_after crash
        '''
        self.log.info("====test_crash_during_compaction starts====")
        wait_warmup = self.input.param("wait_warmup", True)
        kill_itr = self.input.param("kill_itr", 1)

        self.compute_docs_ranges()

        tasks_info = dict()
        for collection in self.collections:
            self.generate_docs(doc_ops=self.doc_ops, target_vbucket=None)
            tem_tasks_info = self.loadgen_docs(
                self.retry_exceptions,
                self.ignore_exceptions,
                scope=CbServer.default_scope,
                collection=collection,
                suppress_error_table=True,
                skip_read_on_error=True,
                _sync=False,
                doc_ops=self.doc_ops,
                track_failures=False,
                sdk_retry_strategy=self.sdk_retry_strategy)
            tasks_info.update(tem_tasks_info.items())

        self.crash_th = threading.Thread(target=self.crash,
                                         kwargs=dict(kill_itr=kill_itr,
                                                     graceful=self.graceful,
                                                     wait=wait_warmup))
        self.compaction_th = threading.Thread(target=self.compact_bucket)

        self.compaction_th.start()
        self.crash_th.start()
        for task in tasks_info:
            self.task_manager.get_task_result(task)

        self.stop_crash = True
        self.stop_compaction = True
        self.crash_th.join()
        self.compaction_th.join()
        self.assertFalse(self.crash_failure, "CRASH | CRITICAL | WARN messages found in cb_logs")

    def test_rollback_during_compaction(self):
        '''
        '''
        self.assertTrue(self.rest.update_autofailover_settings(False, 600),
                        "AutoFailover disabling failed")
        items = copy.deepcopy(self.num_items)
        mem_only_items = self.input.param("rollback_items", 10000)

        ops_len = len(self.doc_ops.split(":"))

        if self.nodes_init < 2 or self.num_replicas < 1:
            self.fail("Not enough nodes/replicas in the cluster/bucket \
            to test rollback")

        self.num_rollbacks = self.input.param("num_rollbacks", 1)

        #######################################################################
        '''
        STEP - 1,  Stop persistence on node - x
        '''

        for i in range(1, self.num_rollbacks+1):
            self.log.info("Roll back Iteration == {}".format(i))
            start = items
            shell = RemoteMachineShellConnection(self.cluster.master)
            cbstats = Cbstats(self.cluster.master)
            self.target_vbucket = cbstats.vbucket_list(self.cluster.buckets[0].
                                                   name)
            mem_item_count = 0
            # Stopping persistence on Node-x
            self.log.debug("Stopping persistence on Node-{}"
                               .format(self.cluster.master))
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
            mem_item_count += mem_only_items * ops_len
            self.generate_docs(doc_ops=self.doc_ops,
                               target_vbucket=self.target_vbucket)
            self.loadgen_docs(_sync=True,
                              retry_exceptions=self.retry_exceptions,
                              ignore_exceptions=self.ignore_exceptions)
            if self.gen_create is not None:
                self.create_start = self.gen_create.key_counter
            if self.gen_update is not None:
                self.update_start = self.gen_update.key_counter
            if self.gen_delete is not None:
                self.delete_start = self.gen_delete.key_counter
            if self.gen_expiry is not None:
                self.expiry_start = self.gen_expiry.key_counter

            ep_queue_size_map = {self.cluster.nodes_in_cluster[0]:
                                 mem_item_count}
            if self.durability_level:
                self.log.info("updating the num_items on disk check to double due to durability")
                ep_queue_size_map = {self.cluster.nodes_in_cluster[0]:
                                     mem_item_count * 2}
                vb_replica_queue_size_map = {self.cluster.nodes_in_cluster[0]:
                                             0}

            for node in self.cluster.nodes_in_cluster[1:]:
                ep_queue_size_map.update({node: 0})
                vb_replica_queue_size_map.update({node: 0})

                #for bucket in self.cluster.buckets:
                #    self.bucket_util._wait_for_stat(bucket, ep_queue_size_map,
                #                                    timeout=1200)
                #    self.bucket_util._wait_for_stat(bucket, vb_replica_queue_size_map,
                #                                    cbstat_cmd="all",
                #                                    stat_name="vb_replica_queue_size",
                #                                    timeout=1200)
                # replica vBuckets
                #for bucket in self.cluster.buckets:
                #    self.log.debug(cbstats.failover_stats(bucket.name))

            ###############################################################
            '''
            STEP - 4
                -- Kill Memcached on master node and trigger rollback on other nodes
            '''
            if self.compact_before:
                compaction_tasks=[]
                for bucket in self.cluster.buckets:
                    compaction_tasks.append(self.task.async_compact_bucket(self.cluster.master,
                                               bucket))
            shell.kill_memcached()

            if self.compact_after:
                self.bucket_util._run_compaction(self.cluster,
                                                 number_of_times=1)
            if self.compact_before:
                for task in compaction_tasks:
                    self.task_manager.get_task_result(task)

            self.assertTrue(self.bucket_util._wait_warmup_completed(
                self.cluster.buckets[0],
                servers=[self.cluster.master],
                wait_time=self.wait_timeout * 10))

            ###############################################################
            '''
            STEP -5
                -- Restarting persistence on Node -- x
            '''

            self.log.debug("RollBack Iteration=={}, Re-Starting persistence on Node -- {}".format(i, self.cluster.master))
            Cbepctl(shell).persistence(self.cluster.buckets[0].name, "start")

            self.sleep(5, "Sleep after re-starting persistence, Iteration{}".format(i))
            for nod in self.cluster.nodes_in_cluster:
                ep_queue_size_map.update({nod: 0})
                vb_replica_queue_size_map.update({nod: 0})
            for bucket in self.cluster.buckets:
                self.bucket_util._wait_for_stat(bucket,
                                                    ep_queue_size_map, timeout=600)
                self.bucket_util._wait_for_stat(bucket,
                                                vb_replica_queue_size_map,
                                                cbstat_cmd="all",
                                                stat_name="vb_replica_queue_size", timeout=600)
            shell.disconnect()
            ###################################################################
            '''
            STEP - 6
              -- Load Docs on all the nodes
              -- Loading of doc for 60 seconds
              -- Ensures creation of new state file
            '''
            self.create_start = items
            self.create_end = items + 100000
            self.generate_docs(doc_ops="create", target_vbucket=None)
            self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions, _sync=True,
                                  doc_ops="create")
            self.bucket_util._wait_for_stats_all_buckets(
                self.cluster, self.cluster.buckets, timeout=1200)
            items = items + 100000
            self.log.debug("Iteration == {}, Total num_items {}".format(i, items))
