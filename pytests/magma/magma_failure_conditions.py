'''
Created on Dec 12, 2019

@author: riteshagarwal
'''

import random

from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from magma_base import MagmaBaseTest
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection
import copy
import json as Json
from sdk_exceptions import SDKException
from sdk_client3 import SDKClient
from Cb_constants.CBServer import CbServer
import threading

retry_exceptions = [SDKException.TimeoutException,
                    SDKException.AmbiguousTimeoutException,
                    SDKException.RequestCanceledException,
                    SDKException.UnambiguousTimeoutException]


class MagmaFailures(MagmaBaseTest):
    def setUp(self):
        super(MagmaFailures, self).setUp()

        self.gen_create = doc_generator(
            self.key, 0, self.num_items,
            doc_size=self.doc_size, doc_type=self.doc_type,
            target_vbucket=self.target_vbucket,
            vbuckets=self.cluster_util.vbuckets,
            key_size=self.key_size,
            randomize_doc_size=self.randomize_doc_size,
            randomize_value=self.randomize_value,
            mix_key_size=self.mix_key_size,
            deep_copy=self.deep_copy)

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
        super(MagmaFailures, self).tearDown()

    def kill_magma_check_wal_file_size(self):
        nIter = 200
        while nIter > 0:
            shell = RemoteMachineShellConnection(
                self.cluster_util.cluster.master)
            shell.kill_memcached()
#             self.bucket_util._wait_warmup_completed()
            self.sleep(10, "sleep of 5s so that memcached can restart")

    def crash(self, nodes=None, kill_itr=1, graceful=False):
        self.stop_crash = False
        if not nodes:
            nodes = self.cluster.nodes_in_cluster

        connections = list()
        for node in nodes:
            shell = RemoteMachineShellConnection(node)
            connections.append(shell)

        while not self.stop_crash:
            sleep = random.randint(30, 60)
            self.sleep(sleep,
                       "waiting for %s sec to kill memcached on all nodes" %
                       sleep)
            for shell in connections:
                if graceful:
                    shell.restart_couchbase()
                else:
                    while kill_itr > 0:
                        shell.kill_memcached()
                        self.sleep(1)
                        kill_itr -= 1

            crashes = self.check_coredump_exist(self.cluster.nodes_in_cluster)
            if len(crashes) > 0:
                self.task.jython_task_manager.abort_all_tasks()
                for shell in connections:
                    shell.disconnect()

            self.assertTrue(len(crashes) == 0,
                            "Found servers having crashes")

            self.assertTrue(self.bucket_util._wait_warmup_completed(
                [self.cluster_util.cluster.master],
                self.bucket_util.buckets[0],
                wait_time=self.wait_timeout * 20))

        for shell in connections:
            shell.disconnect()

class MagmaCrashTests(MagmaFailures):

    def test_crash_magma_n_times(self):
        self.num_crashes = self.input.param("num_crashes", 10)
        items = self.num_items
        start = self.num_items
        self.assertTrue(self.rest.update_autofailover_settings(False, 600),
                        "AutoFailover disabling failed")
        for _ in xrange(1, self.num_crashes+1):
            end = start + random.randint(items, items*2)
            for node in self.cluster.nodes_in_cluster:
                shell = RemoteMachineShellConnection(node)
                shell.kill_memcached()
                shell.disconnect()

            self.assertTrue(self.bucket_util._wait_warmup_completed(
                [self.cluster_util.cluster.master],
                self.bucket_util.buckets[0],
                wait_time=self.wait_timeout * 20))

            self.gen_create = doc_generator(
                self.key, start, end,
                doc_size=self.doc_size,
                doc_type=self.doc_type,
                target_vbucket=self.target_vbucket,
                vbuckets=self.cluster_util.vbuckets,
                randomize_doc_size=self.randomize_doc_size,
                randomize_value=self.randomize_value)

            self.loadgen_docs(_sync=True,
                              retry_exceptions=retry_exceptions)
            self.bucket_util._wait_for_stats_all_buckets()

            data_validation = self.task.async_validate_docs(
                self.cluster, self.bucket_util.buckets[0],
                self.gen_create, "create", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                timeout_secs=self.sdk_timeout)
            self.task.jython_task_manager.get_task_result(data_validation)
            start = end
            self.bucket_util.verify_stats_all_buckets(end, timeout=300)

            self.gen_update = self.gen_create

    def test_crash_during_ops(self):
        self.graceful = self.input.param("graceful", False)
        self.assertTrue(self.rest.update_autofailover_settings(False, 600),
                        "AutoFailover disabling failed")

        ops_len= len(self.doc_ops.split(":"))

        self.create_start = self.num_items
        self.create_end = self.num_items * 2

        if ops_len == 1:
            self.update_start = 0
            self.update_end = self.num_items
            self.expiry_start = 0
            self.expiry_end =  self.num_items
            self.delete_start = 0
            self.delete_end = self.num_items
        elif ops_len == 2:
            self.update_start = 0
            self.update_end = self.num_items // 2
            self.delete_start = self.num_items // 2
            self.delete_end = self.num_items

            if "expiry" in self.doc_ops:
                self.delete_start = 0
                self.delete_end = self.num_items // 2
                self.expiry_start = self.num_items // 2
                self.expiry_end = self.num_item
        else:
            self.update_start = 0
            self.update_end = self.num_items // 3
            self.expiry_start = self.num_items // 3
            self.expiry_end = (2 * self.num_items) // 3
            self.delete_start = (2 * self.num_items) // 3
            self.delete_end = self.num_items

        self.generate_docs(doc_ops=self.doc_ops)

        th = threading.Thread(target=self.crash, kwargs={"graceful": self.graceful})
        th.start()

        tasks = self.loadgen_docs(retry_exceptions=retry_exceptions,
                                  skip_read_on_error=True,
                                  suppress_error_table=True,
                                  _sync=False)

        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

        self.stop_crash = True
        th.join()

    def test_crash_during_recovery(self):
        ops_len= len(self.doc_ops.split(":"))

        self.create_start = self.num_items
        self.create_end = self.num_items * 2

        if ops_len == 1:
            self.update_start = 0
            self.update_end = self.num_items
            self.expiry_start = 0
            self.expiry_end =  self.num_items
            self.delete_start = 0
            self.delete_end = self.num_items
        elif ops_len == 2:
            self.update_start = 0
            self.update_end = self.num_items // 2
            self.delete_start = self.num_items // 2
            self.delete_end = self.num_items

            if "expiry" in self.doc_ops:
                self.delete_start = 0
                self.delete_end = self.num_items // 2
                self.expiry_start = self.num_items // 2
                self.expiry_end = self.num_item
        else:
            self.update_start = 0
            self.update_end = self.num_items // 3
            self.expiry_start = self.num_items // 3
            self.expiry_end = (2 * self.num_items) // 3
            self.delete_start = (2 * self.num_items) // 3
            self.delete_end = self.num_items

        self.generate_docs(doc_ops=self.doc_ops)

        tasks = self.loadgen_docs(retry_exceptions=retry_exceptions,
                                  skip_read_on_error=True,
                                  suppress_error_table=True,
                                  _sync=False)

        th = threading.Thread(target=self.crash, kwargs={"kill_itr": 5})
        th.start()

        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)

        self.stop_crash = True
        th.join()

    def test_crash_before_upserts(self):
        self.log.info("test_update_multi starts")
        self.enable_disable_swap_space(self.cluster.nodes_in_cluster)

        upsert_doc_list = self.get_fragmentation_upsert_docs_list()

        th = threading.Thread(target=self.abort_tasks_after_crash)
        th.start()

        count = 0
        self.mutate = 0
        while count < self.test_itr:
            self.log.info("Iteration == {}".format(count+1))

            self.sigkill_memcached(graceful=self.graceful)

            for itr in upsert_doc_list:
                self.doc_ops = "update"
                self.update_start = 0
                self.update_end = itr

                if self.rev_update:
                    self.update_start = -int(itr - 1)
                    self.update_end = 1

                self.generate_docs(doc_ops="update")
                _ = self.loadgen_docs(self.retry_exceptions,
                                      self.ignore_exceptions,
                                      _sync=True)
                self.bucket_util._wait_for_stats_all_buckets()

            if self.stop:
                break;

            count += 1

        self.stop = True
        th.join()

        self.validate_data("update", self.gen_update)
        self.enable_disable_swap_space(self.cluster.nodes_in_cluster,
                                       disable=False)
        self.log.info("====test_update_multi ends====")

    def test_crash_before_multi_update_deletes(self):
        self.log.info("===test_crash_before_multi_update_deletes starts===")
        self.enable_disable_swap_space(self.cluster.nodes_in_cluster)

        count = 0
        self.mutate = 0
        for i in range(self.test_itr):
            self.log.info("Step 1, Iteration= {}".format(i+1))
            while count < self.update_itr:
                self.sigkill_memcached(graceful=self.graceful)

                self.doc_ops = "update"
                self.update_start = 0
                self.update_end = self.num_items

                self.generate_docs(doc_ops="update")
                _ = self.loadgen_docs(self.retry_exceptions,
                                      self.ignore_exceptions,
                                      _sync=True)
                self.bucket_util._wait_for_stats_all_buckets()

                count += 1
            self.update_itr += self.update_itr

            # data validation is done only for the last iteration
            if i+1 == self.test_itr:
                self.validate_data("update", self.gen_update)

            self.log.debug("Step 2, Iteration {}".format(i+1))
            self.sigkill_memcached()

            self.doc_ops = "delete"
            self.delete_start = 0
            self.delete_end = self.num_items//2

            self.generate_docs(doc_ops="delete")
            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=True)
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(self.num_items)

            self.log.debug("Step 3, Iteration= {}".format(i+1))
            self.sigkill_memcached()

            self.gen_create = copy.deepcopy(self.gen_delete)
            self.doc_ops = "create"

            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=True)
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(self.num_items)

        self.validate_data("create", self.gen_create)
        self.enable_disable_swap_space(self.cluster.nodes_in_cluster,
                                       disable=False)
        self.log.info("===test_crash_before_multi_update_deletes ends===")

    def test_crash_during_get_ops(self):

        self.log.info("test_crash_during_get_ops starts")
        self.enable_disable_swap_space(self.cluster.nodes_in_cluster)

        tasks_info = dict()
        upsert_doc_list = self.get_fragmentation_upsert_docs_list()

        for itr in upsert_doc_list:
            self.doc_ops = "update"
            self.update_start = 0
            self.update_end = itr
            self.mutate = -1
            self.generate_docs(doc_ops="update")

            update_task_info = self.loadgen_docs(
                self.retry_exceptions,
                self.ignore_exceptions,
                _sync=False)
            tasks_info.update(update_task_info.items())

        self.doc_ops = "read"
        self.generate_docs(doc_ops="read")
        start = -int(self.num_items - 1)
        end = 1
        reverse_read_gen = self.genrate_docs_basic(start, end)

        th = threading.Thread(target=self.crash, kwargs={"graceful": self.graceful})
        th.start()

        count = 0
        while count < self.read_thread_count:
            read_task_info = self.loadgen_docs(
                self.retry_exceptions,
                self.ignore_exceptions,
                _sync=False)
            tasks_info.update(read_task_info.items())
            count += 1
            if count < self.read_thread_count:
                read_task_info = self.bucket_util._async_validate_docs(
                    self.cluster, reverse_read_gen, "read", 0,
                    batch_size=self.batch_size,
                    process_concurrency=self.process_concurrency,
                    pause_secs=5, timeout_secs=self.sdk_timeout,
                    retry_exceptions=self.retry_exceptions,
                    ignore_exceptions=self.ignore_exceptions)
                tasks_info.update(read_task_info.items())
                count += 1
        for task in tasks_info:
            self.task_manager.get_task_result(task)

        self.stop_crash = True
        th.join()

        self.bucket_util._wait_for_stats_all_buckets()

        self.enable_disable_swap_space(self.cluster.nodes_in_cluster,
                                       disable=False)
        self.log.info("test_crash_during_get_ops ends")

    def test_crash_during_upserts_using_multithreads(self):
        self.log.info("test_crash_during_upserts_using_multithreads starts")
        self.enable_disable_swap_space(self.cluster.nodes_in_cluster)

        tasks_info = dict()
        self.doc_ops = "update"
        self.update_start = 0
        self.update_end = self.num_items

        th = threading.Thread(target=self.crash, kwargs={"graceful": self.graceful})
        th.start()

        count = 0
        while count < self.read_thread_count:
            self.generate_docs(doc_ops="update")
            update_task_info = self.loadgen_docs(
                self.retry_exceptions,
                self.ignore_exceptions,
                _sync=False)
            tasks_info.update(update_task_info.items())
            count += 1
            self.sleep(5)

        for task in tasks_info:
            self.task_manager.get_task_result(task)

        self.stop_crash = True
        th.join()

        self.bucket_util._wait_for_stats_all_buckets()

        self.enable_disable_swap_space(self.cluster.nodes_in_cluster,
                                       disable=False)
        self.log.info("test_crash_during_upserts_using_multithreads ends")

    def test_crash_during_multi_updates_of_single_doc(self):

        self.log.info("==test_crash_during_multi_updates_of_single_doc starts==")
        self.enable_disable_swap_space(self.cluster.nodes_in_cluster)

        self.client = SDKClient([self.cluster.master],
                                self.bucket_util.buckets[0],
                                scope=CbServer.default_scope,
                                collection=CbServer.default_collection)

        self.doc_ops = "update"
        self.gen_update = self.genrate_docs_basic(start=0, end=1)
        key, val = self.gen_update.next()

        def upsert_doc(start_num, end_num, key_obj, val_obj):
            for i in range(start_num, end_num):
                val_obj.put("mutated", i)
                self.client.upsert(key_obj, val_obj)

        th1 = threading.Thread(target=self.crash, kwargs={"graceful": self.graceful})
        th1.start()

        threads = []
        start = 0
        end = 0
        for _ in range(10):
            start = end
            end += 10
            th = threading.Thread(
                target=upsert_doc, args=[start, end, key, val])
            th.start()
            threads.append(th)

        for th in threads:
            th.join()

        self.stop_crash = True
        th1.join()

        self.bucket_util._wait_for_stats_all_buckets()

        success, _ = self.client.get_multi([key],
                                           self.wait_timeout)
        self.assertIs(key in success, True,
                      msg="key {} doesn't exist\
                      ".format(key))

        expected_val = Json.loads(val.toString())
        actual_val = Json.loads(success[key][
            'value'].toString())
        self.assertIs(expected_val == actual_val, True,
                      msg="expected_val-{} != Actual_val-{}\
                      ".format(expected_val, actual_val))

        self.enable_disable_swap_space(self.cluster.nodes_in_cluster,
                                       disable=False)
        self.log.info("==test_crash_during_multi_updates_of_single_doc ends==")

    def test_crash_during_val_movement_across_trees(self):

        self.log.info("==test_crash_during_val_movement_across_trees starts==")
        self.enable_disable_swap_space(self.cluster.nodes_in_cluster)

        upsert_size = 0
        if self.doc_size < 32:
            upsert_size = 2048

        self.update_start = 0
        self.update_end = self.num_items
        if self.rev_update:
                self.update_start = -int(self.num_items - 1)
                self.update_end = 1
        self.doc_ops = "update"

        th = threading.Thread(target=self.crash, kwargs={"graceful": self.graceful})
        th.start()

        count = 0
        while count < self.test_itr:
            self.log.info("Iteration == {}".format(count))

            self.mutate += 1
            self.gen_update = doc_generator(
                self.key, self.update_start,
                self.update_end,
                doc_size=upsert_size,
                doc_type=self.doc_type,
                target_vbucket=self.target_vbucket,
                vbuckets=self.cluster_util.vbuckets,
                key_size=self.key_size,
                mutate=self.mutate,
                randomize_doc_size=self.randomize_doc_size,
                randomize_value=self.randomize_value,
                mix_key_size=self.mix_key_size,
                deep_copy=self.deep_copy)

            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions, _sync=True)
            self.bucket_util._wait_for_stats_all_buckets()

            self.generate_docs(doc_ops="update")
            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions, _sync=True)
            self.bucket_util._wait_for_stats_all_buckets()

            count += 1
        self.stop_crash = True
        th.join()

        self.validate_data("update", self.gen_update)

        self.enable_disable_swap_space(self.cluster.nodes_in_cluster, disable=False)

        self.log.info("==test_crash_during_val_movement_across_trees ends==")

class MagmaRollbackTests(MagmaFailures):

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
                self.input.servers[0], self.bucket_util.buckets[0])
            mem_client.stop_persistence()

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
        self.loadgen_docs(_sync=True,
                              retry_exceptions=retry_exceptions)
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
                self.input.servers[0], self.bucket_util.buckets[0])
            mem_client.stop_persistence()

            self.gen_delete = self.gen_docs_basic_for_target_vbucket(start, mem_only_items,
                                                                     self.target_vbucket)

            self.loadgen_docs(_sync=True,
                              retry_exceptions=retry_exceptions)
            #start = self.gen_delete.key_counter

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

class MagmaSpaceAmplification(MagmaFailures):

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
