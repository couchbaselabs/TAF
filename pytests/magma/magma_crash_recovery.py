'''
Created on Dec 12, 2019

@author: riteshagarwal
'''

import copy
import random
import threading

from Cb_constants.CBServer import CbServer
from couchbase_helper.documentgenerator import doc_generator
import json as Json
from magma_base import MagmaBaseTest
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient
from sdk_exceptions import SDKException


retry_exceptions = [SDKException.TimeoutException,
                    SDKException.AmbiguousTimeoutException,
                    SDKException.RequestCanceledException,
                    SDKException.UnambiguousTimeoutException]


class MagmaCrashTests(MagmaBaseTest):

    def setUp(self):
        super(MagmaCrashTests, self).setUp()
        self.graceful = self.input.param("graceful", False)

    def tearDown(self):
        super(MagmaCrashTests, self).tearDown()

    def compute_docs_ranges(self):
        self.multiplier = self.input.param("multiplier", 2)
        ops_len = len(self.doc_ops.split(":"))

        self.create_start = self.init_items_per_collection
        self.create_end = self.init_items_per_collection * self.multiplier

        if "create" in self.doc_ops:
            self.create_end = self.init_items_per_collection * self.multiplier

        if ops_len == 1:
            self.update_start = 0
            self.update_end = self.init_items_per_collection
            self.expiry_start = 0
            self.expiry_end = self.init_items_per_collection * self.multiplier
            self.delete_start = 0
            self.delete_end = self.init_items_per_collection
        elif ops_len == 2:
            self.update_start = 0
            self.update_end = self.init_items_per_collection // 2
            self.delete_start = self.init_items_per_collection // 2
            self.delete_end = self.init_items_per_collection

            if "expiry" in self.doc_ops:
                self.delete_start = 0
                self.delete_end = self.init_items_per_collection // 2
                self.expiry_start = self.init_items_per_collection // 2
                self.expiry_end = self.init_items_per_collection * self.multiplier
        else:
            self.update_start = 0
            self.update_end = self.init_items_per_collection // 3
            self.delete_start = self.init_items_per_collection // 3
            self.delete_end = (2 * self.init_items_per_collection) // 3
            self.expiry_start = (2 * self.init_items_per_collection) // 3
            self.expiry_end = self.init_items_per_collection * self.multiplier

    def kill_magma_check_wal_file_size(self):
        nIter = 200
        while nIter > 0:
            shell = RemoteMachineShellConnection(
                self.cluster_util.cluster.master)
            shell.kill_memcached()
#             self.bucket_util._wait_warmup_completed()
            self.sleep(10, "sleep of 5s so that memcached can restart")

    def test_crash_during_ops(self):
        self.graceful = self.input.param("graceful", False)
        wait_warmup = self.input.param("wait_warmup", True)
        self.log.info("====test_crash_during_ops starts====")
        self.assertTrue(self.rest.update_autofailover_settings(False, 600),
                        "AutoFailover disabling failed")

        self.compute_docs_ranges()

        th = threading.Thread(target=self.crash,
                              kwargs=dict(graceful=self.graceful,
                                          wait=wait_warmup))
        th.start()
        tasks_info = dict()
        for collection in self.collections:
            self.generate_docs(doc_ops=self.doc_ops, target_vbucket=None)
            tem_tasks_info = self.loadgen_docs(
                self.retry_exceptions,
                self.ignore_exceptions,
                scope=self.scope_name,
                collection=collection,
                suppress_error_table=True,
                skip_read_on_error=True,
                _sync=False,
                doc_ops=self.doc_ops,
                track_failures=False)
            tasks_info.update(tem_tasks_info.items())

        for task in tasks_info:
            self.task_manager.get_task_result(task)

        self.stop_crash = True
        th.join()
        self.validate_seq_itr()

    def test_crash_during_recovery(self):
        self.compute_docs_ranges()

        th = threading.Thread(target=self.crash, kwargs={"kill_itr": 5})
        th.start()

        tasks_info = dict()
        for collection in self.collections:
            self.generate_docs(doc_ops=self.doc_ops, target_vbucket=None)
            tem_tasks_info = self.loadgen_docs(
                self.retry_exceptions,
                self.ignore_exceptions,
                scope=self.scope_name,
                collection=collection,
                suppress_error_table=True,
                skip_read_on_error=True,
                _sync=False,
                doc_ops=self.doc_ops,
                track_failures=False)
            tasks_info.update(tem_tasks_info.items())

        for task in tasks_info:
            self.task_manager.get_task_result(task)

        self.stop_crash = True
        th.join()
        self.validate_seq_itr()

    def test_crash_before_upserts(self):
        self.log.info("test_update_multi starts")
        self.change_swap_space(self.cluster.nodes_in_cluster)

        upsert_doc_list = self.get_fragmentation_upsert_docs_list()

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

                _ = self.loadgen_docs(
                    self.retry_exceptions,
                    self.ignore_exceptions,
                    suppress_error_table=True,
                    _sync=True,
                    doc_ops="update")

                self.bucket_util._wait_for_stats_all_buckets()

            count += 1

        self.validate_data("update", self.gen_update)
        self.change_swap_space(self.cluster.nodes_in_cluster,
                               disable=False)
        self.validate_seq_itr()
        self.log.info("====test_update_multi ends====")

    def test_crash_before_multi_update_deletes(self):
        self.log.info("===test_crash_before_multi_update_deletes starts===")
        self.change_swap_space(self.cluster.nodes_in_cluster)

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
                                      suppress_error_table=True,
                                      _sync=True,
                                      doc_ops="update")

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
                                  suppress_error_table=True,
                                  _sync=True,
                                  doc_ops="delete")

            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(self.num_items)

            self.log.debug("Step 3, Iteration= {}".format(i+1))
            self.sigkill_memcached()

            self.gen_create = copy.deepcopy(self.gen_delete)
            self.doc_ops = "create"
            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  suppress_error_table=True,
                                  _sync=True,
                                  doc_ops="create")

            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(self.num_items)

        self.validate_data("create", self.gen_create)
        self.change_swap_space(self.cluster.nodes_in_cluster,
                               disable=False)
        self.validate_seq_itr()
        self.log.info("===test_crash_before_multi_update_deletes ends===")

    def test_crash_during_get_ops(self):

        self.log.info("test_crash_during_get_ops starts")
        self.change_swap_space(self.cluster.nodes_in_cluster)

        tasks_info = dict()
        upsert_doc_list = self.get_fragmentation_upsert_docs_list()

        for itr in upsert_doc_list:
            self.doc_ops = "update"
            self.update_start = 0
            self.update_end = itr
            self.mutate = -1
            self.generate_docs(doc_ops="update")
            update_task_info = self.loadgen_docs(
                self.retry_exceptions, self.ignore_exceptions,
                suppress_error_table=True, _sync=False)

            #update_task_info = self.loadgen_docs(
            #    self.retry_exceptions,
            #    self.ignore_exceptions,
            #    _sync=False)
            tasks_info.update(update_task_info.items())

        self.doc_ops = "read"
        self.generate_docs(doc_ops="read")
        start = -int(self.num_items - 1)
        end = 1
        reverse_read_gen = self.genrate_docs_basic(start, end)

        th = threading.Thread(target=self.crash, kwargs={"graceful":
                                                         self.graceful})
        th.start()

        count = 0
        while count < self.read_thread_count:
            read_task_info = self.loadgen_docs(self.retry_exceptions,
                                               self.ignore_exceptions,
                                               suppress_error_table=True,
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
                    ignore_exceptions=self.ignore_exceptions,
                    suppress_error_table=False)
                tasks_info.update(read_task_info.items())
                count += 1
        for task in tasks_info:
            self.task_manager.get_task_result(task)

        self.stop_crash = True
        th.join()

        self.bucket_util._wait_for_stats_all_buckets()

        self.change_swap_space(self.cluster.nodes_in_cluster,
                               disable=False)
        self.validate_seq_itr()
        self.log.info("test_crash_during_get_ops ends")

    def test_crash_during_upserts_using_multithreads(self):
        self.log.info("test_crash_during_upserts_using_multithreads starts")
        self.change_swap_space(self.cluster.nodes_in_cluster)

        tasks_info = dict()
        self.doc_ops = "update"
        self.update_start = 0
        self.update_end = self.num_items

        th = threading.Thread(target=self.crash, kwargs={"graceful":
                                                         self.graceful})
        th.start()

        count = 0
        while count < self.read_thread_count:
            self.generate_docs(doc_ops="update")
            update_task_info = self.loadgen_docs(
                self.retry_exceptions,
                self.ignore_exceptions,
                suppress_error_table=True,
                skip_read_on_error=True,
                _sync=False,
                track_failures=False)
            tasks_info.update(update_task_info.items())
            count += 1
            self.sleep(5)

        for task in tasks_info:
            self.task_manager.get_task_result(task)

        self.stop_crash = True
        th.join()

        self.bucket_util._wait_for_stats_all_buckets()

        self.change_swap_space(self.cluster.nodes_in_cluster,
                               disable=False)
        self.validate_seq_itr()
        self.log.info("test_crash_during_upserts_using_multithreads ends")

    def test_crash_during_multi_updates_of_single_doc(self):

        self.log.info("==test_crash_during_multi_updates_of_single_doc starts==")
        self.change_swap_space(self.cluster.nodes_in_cluster)

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

        th1 = threading.Thread(target=self.crash, kwargs={"graceful":
                                                          self.graceful})
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

        self.change_swap_space(self.cluster.nodes_in_cluster,
                               disable=False)
        self.validate_seq_itr()
        self.log.info("==test_crash_during_multi_updates_of_single_doc ends==")

    def test_crash_during_val_movement_across_trees(self):

        self.log.info("==test_crash_during_val_movement_across_trees starts==")
        self.change_swap_space(self.cluster.nodes_in_cluster)

        upsert_size = 0
        if self.doc_size < 32:
            upsert_size = 2048

        self.update_start = 0
        self.update_end = self.num_items
        if self.rev_update:
                self.update_start = -int(self.num_items - 1)
                self.update_end = 1
        self.doc_ops = "update"

        th = threading.Thread(target=self.crash, kwargs={"graceful":
                                                         self.graceful})
        th.start()

        count = 0
        while count < self.test_itr:
            self.log.info("Iteration == {}".format(count))

            self.mutate += 1
            self.gen_update = doc_generator(self.key, self.update_start,
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
                                  self.ignore_exceptions,
                                  suppress_error_table=True,
                                  skip_read_on_error=True,
                                  _sync=True,
                                  track_failures=False)
            self.bucket_util._wait_for_stats_all_buckets()

            self.generate_docs(doc_ops="update")
            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  suppress_error_table=True,
                                  skip_read_on_error=True,
                                  _sync=True,
                                  track_failures=False)
            self.bucket_util._wait_for_stats_all_buckets()

            count += 1
        self.stop_crash = True
        th.join()

        self.change_swap_space(self.cluster.nodes_in_cluster,
                               disable=False)
        self.validate_seq_itr()
        self.log.info("==test_crash_during_val_movement_across_trees ends==")
