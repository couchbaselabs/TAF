'''
Created on Dec 12, 2019

@author: riteshagarwal
'''

import copy
import threading
import random

from Cb_constants.CBServer import CbServer
from TestInput import TestInputSingleton
from couchbase_helper.documentgenerator import doc_generator
import json as Json
from magma_base import MagmaBaseTest
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient
from sdk_constants.java_client import SDKConstants
from com.couchbase.test.docgen import DocRange
from couchbase.test.docgen import DRConstants
from java.util import HashMap


class MagmaCrashTests(MagmaBaseTest):

    def setUp(self):
        self.input = TestInputSingleton.input
        self.input.test_params.update({"random_key": True})
        super(MagmaCrashTests, self).setUp()
        self.sdk_timeout = self.input.param("sdk_timeout", 10)
        self.time_unit = "seconds"
        self.graceful = self.input.param("graceful", False)
        self.assertTrue(self.rest.update_autofailover_settings(False, 600),
                        "AutoFailover disabling failed")
        self.crash_th = None

    def tearDown(self):
        self.stop_crash = True
        if self.crash_th and self.crash_th.is_alive():
            self.crash_th.join()
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, self.cluster.buckets,
                                                     check_ep_items_remaining=True, timeout=1200)
        self.cluster_util.print_cluster_stats(self.cluster)
        for bucket in self.cluster.buckets:
            result = self.bucket_util.validate_active_replica_item_count(self.cluster, bucket, timeout=900)
            self.assertTrue(result, "Active/Replica items count match failed: %s" % bucket.name)
        super(MagmaCrashTests, self).tearDown()

    def kill_magma_check_wal_file_size(self):
        nIter = 200
        while nIter > 0:
            shell = RemoteMachineShellConnection(
                self.cluster.master)
            shell.kill_memcached()
#             self.bucket_util._wait_warmup_completed()
            self.sleep(10, "sleep of 5s so that memcached can restart")

    def drop_recreate_collections(self, num_collections_to_drop=None):
        self._itr = 0
        drop_lst = [collection for collection in self.collections[::2] if collection != "_default"]

        if num_collections_to_drop:
            self.collections.remove("_default")
            drop_lst = random.sample(self.collections, int(num_collections_to_drop))
            self.collections.append("_default")

        self.log.info("drop collection list:: {}".format(drop_lst))
        while not self.stop_crash:
            self._itr += 1
            self.log.info("collection drop iteration {}".format(self._itr))
            for collection in drop_lst:
                self.bucket_util.drop_collection(self.cluster.master,
                                                 self.buckets[0],
                                                 scope_name=CbServer.default_scope,
                                                 collection_name=collection)
                self.sleep(2, "sleep before dropping collection")
            self.sleep(10, "sleep before recreating collections")
            for collection in drop_lst:
                self.bucket_util.create_collection(self.cluster.master,
                                                   self.buckets[0],
                                                   CbServer.default_scope,
                                                   {"name": collection})
                self.sleep(2, "sleep before next collection creation")
            sleep = random.randint(90, 120)

            self.sleep(sleep, "Wait for next drop/create collection iteration")

    def reset_doc_params(self, doc_ops=None):
        self.create_perc = 0
        self.delete_perc = 0
        self.update_perc = 0
        self.expiry_perc = 0
        self.read_perc = 0
        self.create_end = 0
        self.create_start = 0
        self.update_end = 0
        self.update_start = 0
        self.delete_end = 0
        self.delete_start = 0
        self.expire_end = 0
        self.expire_start = 0
        doc_ops = doc_ops or self.doc_ops
        doc_ops = doc_ops.split(":")
        perc = 100//len(doc_ops)
        if "update" in doc_ops:
            self.update_perc = perc
        if "create" in doc_ops:
            self.create_perc = perc
        if "read" in doc_ops:
            self.read_perc = perc
        if "expiry" in doc_ops:
            self.expiry_perc = perc
            self.maxttl = random.randint(5, 10)
            self.bucket_util._expiry_pager(self.cluster, 10000000000)
        if "delete" in doc_ops:
            self.delete_perc = perc

    def test_magma_failures_with_CDC(self):
        self.induce_failures = self.input.param("induce_failures", True)
        self.enable_disable_history = self.input.param("enable_disable_history", False)
        self.change_bucket_history_params = self.input.param("change_bucket_history_params", False)
        self.exp_pager_stime = self.input.param("exp_pager_stime", 10)

        self.create_start = 0
        self.create_end = self.init_items_per_collection
        self.process_concurrency = self.standard_buckets * self.num_collections * self.num_scopes
        self.new_loader(wait=True)
        if self.bucket_dedup_retention_seconds == 0 and self.bucket_dedup_retention_bytes == 0:
            for bucket in self.cluster.buckets:
                self.bucket_util.update_bucket_property(
                    self.cluster.master, bucket,
                    history_retention_seconds=86400, history_retention_bytes=96000000000)
        self.sleep(30, "sleep after updating history settings")
        self.track_failures = False

        count = 1
        while count < self.test_itr+1:
            self.log.info("Iteration == {}".format(count))
            self.reset_doc_params()
            self.compute_docs_ranges()

            temp_tasks = self.new_loader(wait=False)
            self.graceful = self.input.param("graceful", False)
            wait_warmup = self.input.param("wait_warmup", True)

            if self.change_bucket_history_params:
                self.change_history_param_th = threading.Thread(target=self.change_history_retention_values)
                self.change_history_param_th.start()

            if self.induce_failures:
                self.crash_th = threading.Thread(target=self.crash,
                                                 kwargs=dict(graceful=self.graceful,
                                                             wait=wait_warmup))
                self.crash_th.start()

            if self.enable_disable_history:
                self.enable_disable_history_th = threading.Thread(target=self.start_stop_history_retention_for_collections)
                self.enable_disable_history_th.start()

            self.doc_loading_tm.getAllTaskResult()

            if self.induce_failures:
                self.stop_crash = True
                self.crash_th.join()

            if self.enable_disable_history:
                self.stop_enable_disable_history = True
                self.enable_disable_history_th.join()

            if self.change_bucket_history_params:
                self.change_history = True
                self.change_history_param_th.join()

            if "delete" in self.doc_ops:
                create_start = self.delete_start
                create_end = self.delete_end
                self.reset_doc_params(doc_ops="create")
                self.create_start = create_start
                self.create_end = create_end
                self.num_items_per_collection += self.create_end - self.create_start
                self.new_loader(wait=True)
            if "expiry" in self.doc_ops:
                self.sleep(self.maxttl, "Wait for docs to expire")
                self.bucket_util._expiry_pager(self.cluster, self.exp_pager_stime)
                self.sleep(self.exp_pager_stime, "Wait until exp_pager_stime for kv_purger\
             to kickoff")
                self.sleep(self.exp_pager_stime*20, "Wait for KV purger to scan expired docs and add \
            tombstones.")
                create_start = self.expiry_start
                create_end = self.expiry_end
                self.reset_doc_params(doc_ops="create")
                self.create_start = create_start
                self.create_end = create_end
                self.num_items_per_collection += self.create_end - self.create_start
                self.new_loader(wait=True)
            count += 1

    def test_crash_during_dedupe(self):
        self.graceful = self.input.param("graceful", False)
        self.num_collections_to_drop = self.input.param("num_collections_to_drop", None)
        self.dedupe_iterations = self.input.param("dedupe_iterations", 5000)
        self.sdk_retry_strategy = SDKConstants.RetryStrategy.FAIL_FAST
        wait_warmup = self.input.param("wait_warmup", True)
        self.log.info("====test_crash_during_dedupe starts====")
        count = 1
        self.generate_docs(doc_ops="update", update_start=0,
                               update_end=self.init_items_per_collection)
        while count < self.test_itr + 1:
            self.PrintStep("Step 2.{} ==> Update {} items/collections".format(count, self.init_items_per_collection))
            for scope in self.scopes:
                for collection in self.collections:
                    self.loadgen_docs(_sync=True, doc_ops="update",
                                      retry_exceptions=self.retry_exceptions,
                                      collection=collection)
            count += 1

        self.compute_docs_ranges()
        self.batch_size=500
        self.process_concurrency=1
        tasks_info = dict()
        self.generate_docs(doc_ops="update", update_end=10000, update_start=0,
                           target_vbucket=None)
        for scope in self.scopes:
            for collection in self.collections:
                tem_tasks_info = self.loadgen_docs(
                    self.retry_exceptions,
                    self.ignore_exceptions,
                    scope=scope,
                    collection=collection,
                    suppress_error_table=True,
                    skip_read_on_error=True,
                    _sync=False,
                    doc_ops="update",
                    track_failures=False,
                    sdk_retry_strategy=self.sdk_retry_strategy,
                    iterations=self.dedupe_iterations)
                tasks_info.update(tem_tasks_info.items())

        self.crash_th = threading.Thread(target=self.crash,
                                         kwargs=dict(graceful=self.graceful,
                                                     wait=wait_warmup))
        self.crash_th.start()

        if self.num_collections_to_drop:
            self.drop_collection_th = threading.Thread(target=self.drop_recreate_collections,
                                                       kwargs=dict(num_collections_to_drop=self.num_collections_to_drop))
            self.drop_collection_th.start()
        for task in tasks_info:
            self.task_manager.get_task_result(task)

        self.stop_crash = True

        if self.num_collections_to_drop:
            self.drop_collection_th.join()

        self.crash_th.join()
        self.assertFalse(self.crash_failure, "CRASH | CRITICAL | WARN messages found in cb_logs")
        for node in self.cluster.nodes_in_cluster:
            if "kv" in node.services:
                self.assertTrue(self.bucket_util._wait_warmup_completed(
                    [node], self.cluster.buckets[0],
                    wait_time=self.wait_timeout * 5))

    def test_crash_during_ops_new(self):
        self.graceful = self.input.param("graceful", False)
        self.ops_rate = self.input.param("ops_rate", 10000)
        self.drop_collections = self.input.param("drop_collections", False)
        self.log.info("====test_crash_during_ops_new starts====")
        self.create_start = 0
        self.create_end = self.init_items_per_collection

        self.new_loader(wait=True)
        self.compute_docs_ranges()
        self.sdk_retry_strategy = SDKConstants.RetryStrategy.FAIL_FAST
        self.ops_rate = self.ops_rate * 3

        self.create_perc = self.input.param("create_perc", 0)
        self.read_perc = self.input.param("read_perc", 0)
        self.update_perc = self.input.param("update_perc", 0)
        self.delete_perc = self.input.param("delete_perc", 0)
        self.expiry_perc = self.input.param("expiry_perc", 0)
        self.track_failures = False

        self.new_loader()
        self.graceful = self.input.param("graceful", False)
        wait_warmup = self.input.param("wait_warmup", True)
        self.crash_th = threading.Thread(target=self.crash,
                                         kwargs=dict(graceful=self.graceful,
                                                     wait=wait_warmup))
        self.crash_th.start()
        if self.drop_collections:
            self.drop_collection_th = threading.Thread(target=self.drop_recreate_collections)
            self.drop_collection_th.start()
        self.doc_loading_tm.getAllTaskResult()
        self.stop_crash = True
        if self.drop_collections:
            self.drop_collection_th.join()
        self.crash_th.join()
        self.assertFalse(self.crash_failure, "CRASH | CRITICAL | WARN messages found in cb_logs")
        for node in self.cluster.nodes_in_cluster:
            if "kv" in node.services:
                self.assertTrue(self.bucket_util._wait_warmup_completed(
                    [node], self.cluster.buckets[0],
                    wait_time=self.wait_timeout * 5))

    def test_crash_during_recovery_new(self):
        self.graceful = self.input.param("graceful", False)
        self.ops_rate = self.input.param("ops_rate", 10000)
        self.log.info("====test_crash_during_recovery_new starts====")
        self.create_start = 0
        self.create_end = self.init_items_per_collection

        self.new_loader(wait=True)
        self.compute_docs_ranges()
        self.sdk_retry_strategy = SDKConstants.RetryStrategy.FAIL_FAST
        self.ops_rate = self.ops_rate * 3

        self.create_perc = self.input.param("create_perc", 0)
        self.read_perc = self.input.param("read_perc", 0)
        self.update_perc = self.input.param("update_perc", 0)
        self.delete_perc = self.input.param("delete_perc", 0)
        self.expiry_perc = self.input.param("expiry_perc", 0)
        self.track_failures = False

        self.new_loader()
        self.crash_th = threading.Thread(target=self.crash, kwargs={"kill_itr": 5})
        self.crash_th.start()
        self.doc_loading_tm.getAllTaskResult()
        self.stop_crash = True
        self.crash_th.join()
        self.assertFalse(self.crash_failure, "CRASH | CRITICAL | WARN messages found in cb_logs")
        for node in self.cluster.nodes_in_cluster:
            if "kv" in node.services:
                self.assertTrue(self.bucket_util._wait_warmup_completed(
                    [node], self.cluster.buckets[0],
                    wait_time=self.wait_timeout * 5))

    def test_crash_during_ops(self):
        self.graceful = self.input.param("graceful", False)
        self.sdk_retry_strategy = SDKConstants.RetryStrategy.FAIL_FAST
        wait_warmup = self.input.param("wait_warmup", True)
        self.log.info("====test_crash_during_ops starts====")

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
                                         kwargs=dict(graceful=self.graceful,
                                                     wait=wait_warmup))
        self.crash_th.start()
        for task in tasks_info:
            self.task_manager.get_task_result(task)

        self.stop_crash = True
        self.crash_th.join()
        self.assertFalse(self.crash_failure, "CRASH | CRITICAL | WARN messages found in cb_logs")
        for node in self.cluster.nodes_in_cluster:
            if "kv" in node.services:
                self.assertTrue(self.bucket_util._wait_warmup_completed(
                    [node], self.cluster.buckets[0],
                    wait_time=self.wait_timeout * 5))
        self.validate_seq_itr()

    def test_crash_during_recovery(self):
        self.sdk_retry_strategy = SDKConstants.RetryStrategy.FAIL_FAST
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

        self.crash_th = threading.Thread(target=self.crash, kwargs={"kill_itr": 5})
        self.crash_th.start()
        for task in tasks_info:
            self.task_manager.get_task_result(task)

        self.stop_crash = True
        self.crash_th.join()
        self.assertFalse(self.crash_failure, "CRASH | CRITICAL | WARN messages found in cb_logs")
        for node in self.cluster.nodes_in_cluster:
            if "kv" in node.services:
                self.assertTrue(self.bucket_util._wait_warmup_completed(
                    [node], self.cluster.buckets[0],
                    wait_time=self.wait_timeout * 5))
        self.validate_seq_itr()

    def test_wal_replay(self):
        self.log.info("===== test_wal_replay starts===")
        items = self.init_items_per_collection
        self.doc_ops = "create:update"
        self.create_start = items
        self.create_end = items + self.init_items_per_collection
        self.update_start = 0
        self.update_end = self.init_items_per_collection
        count = 0
        while count < self.test_itr:
            self.generate_docs()
            tasks_info = dict()
            self.log.info("WAL Replay Iteration == {}".format(count+1))
            for collection in self.collections:
                tem_tasks_info = self.loadgen_docs(self.retry_exceptions,
                                               self.ignore_exceptions,
                                               scope=CbServer.default_scope,
                                               collection=collection,
                                               suppress_error_table=True,
                                               skip_read_on_error=True,
                                               _sync=False,
                                               doc_ops=self.doc_ops,
                                               track_failures=False)
                tasks_info.update(tem_tasks_info.items())
                for task in tasks_info:
                    self.task_manager.get_task_result(task)
                self.bucket_util._wait_for_stats_all_buckets(
                    self.cluster, self.cluster.buckets)
                for node in self.cluster.nodes_in_cluster:
                    shell = RemoteMachineShellConnection(node)
                if self.graceful:
                    shell.restart_couchbase()
                else:
                    shell.kill_memcached()
                self.create_start = self.create_end
                self.create_end = self.create_end + self.init_items_per_collection
                self.update_start = self.update_end
                self.update_end = self.update_start + self.init_items_per_collection
                items = self.create_end
                self.validate_data("update", self.gen_update)
                self.validate_data("create", self.gen_create)
                count += 1

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

                self.bucket_util._wait_for_stats_all_buckets(
                    self.cluster, self.cluster.buckets)

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

                self.bucket_util._wait_for_stats_all_buckets(
                    self.cluster, self.cluster.buckets)

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

            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets)
            self.bucket_util.verify_stats_all_buckets(self.cluster,
                                                      self.num_items)

            self.log.debug("Step 3, Iteration= {}".format(i+1))
            self.sigkill_memcached()

            self.gen_create = copy.deepcopy(self.gen_delete)
            self.doc_ops = "create"
            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  suppress_error_table=True,
                                  _sync=True,
                                  doc_ops="create")

            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets)
            self.bucket_util.verify_stats_all_buckets(self.cluster,
                                                      self.num_items)

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
                    timeout_secs=self.sdk_timeout,
                    retry_exceptions=self.retry_exceptions,
                    ignore_exceptions=self.ignore_exceptions,
                    suppress_error_table=False)
                tasks_info.update(read_task_info.items())
                count += 1

        self.crash_th = threading.Thread(target=self.crash,
                                         kwargs={"graceful": self.graceful})
        self.crash_th.start()
        for task in tasks_info:
            self.task_manager.get_task_result(task)

        self.stop_crash = True
        self.crash_th.join()
        self.assertFalse(self.crash_failure, "CRASH | CRITICAL | WARN messages found in cb_logs")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)

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

        count = 0
        while count < self.read_thread_count:
            self.generate_docs(doc_ops="update")
            update_task_info = self.loadgen_docs(
                self.retry_exceptions,
                self.ignore_exceptions,
                suppress_error_table=True,
                skip_read_on_error=True,
                _sync=False,
                track_failures=False,
                sdk_retry_strategy=self.sdk_retry_strategy)
            tasks_info.update(update_task_info.items())
            count += 1
            self.sleep(5)

        self.crash_th = threading.Thread(target=self.crash,
                                         kwargs={"graceful": self.graceful})
        self.crash_th.start()
        for task in tasks_info:
            self.task_manager.get_task_result(task)

        self.stop_crash = True
        self.crash_th.join()
        self.assertFalse(self.crash_failure, "CRASH | CRITICAL | WARN messages found in cb_logs")

        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)

        self.change_swap_space(self.cluster.nodes_in_cluster,
                               disable=False)
        self.validate_seq_itr()
        self.log.info("test_crash_during_upserts_using_multithreads ends")

    def test_crash_during_multi_updates_of_single_doc(self):

        self.log.info("==test_crash_during_multi_updates_of_single_doc starts==")
        self.change_swap_space(self.cluster.nodes_in_cluster)

        self.client = SDKClient([self.cluster.master],
                                self.cluster.buckets[0],
                                scope=CbServer.default_scope,
                                collection=CbServer.default_collection)

        self.doc_ops = "update"
        self.gen_update = self.genrate_docs_basic(start=0, end=1)
        key, val = self.gen_update.next()

        def upsert_doc(start_num, end_num, key_obj, val_obj):
            for i in range(start_num, end_num):
                val_obj.put("mutated", i)
                self.client.upsert(key_obj, val_obj)

        self.crash_th = threading.Thread(target=self.crash, kwargs={"graceful":
                                                          self.graceful})
        self.crash_th.start()

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
        self.crash_th.join()
        self.assertFalse(self.crash_failure, "CRASH | CRITICAL | WARN messages found in cb_logs")

        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)

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

        self.crash_th = threading.Thread(target=self.crash, kwargs={"graceful":
                                                         self.graceful})
        self.crash_th.start()

        count = 0
        while count < self.test_itr:
            self.log.info("Iteration == {}".format(count))

            self.mutate += 1
            self.gen_update = doc_generator(
                self.key, self.update_start, self.update_end,
                doc_size=upsert_size,
                doc_type=self.doc_type,
                target_vbucket=self.target_vbucket,
                vbuckets=self.cluster.vbuckets,
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
                                  track_failures=False,
                                  sdk_retry_strategy=self.sdk_retry_strategy)
            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets)

            self.generate_docs(doc_ops="update")
            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  suppress_error_table=True,
                                  skip_read_on_error=True,
                                  _sync=True,
                                  track_failures=False,
                                  sdk_retry_strategy=self.sdk_retry_strategy)
            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets)

            count += 1
        self.stop_crash = True
        self.crash_th.join()
        self.assertFalse(self.crash_failure, "CRASH | CRITICAL | WARN messages found in cb_logs")

        self.change_swap_space(self.cluster.nodes_in_cluster,
                               disable=False)
        self.validate_seq_itr()
        self.log.info("==test_crash_during_val_movement_across_trees ends==")
