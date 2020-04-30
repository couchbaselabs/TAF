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
from sdk_exceptions import SDKException
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

    def crash(self, nodes=None):
        self.stop_crash = False
        if not nodes:
            nodes = self.cluster.nodes_in_cluster

        while not self.stop_crash:
            sleep = random.randint(30, 60)
            self.sleep(sleep,
                       "waiting for %s sec to kill memcached on all nodes" %
                       sleep)
            for node in nodes:
                shell = RemoteMachineShellConnection(node)
                shell.kill_memcached()
                shell.disconnect()

            crashes = self.check_coredump_exist(self.cluster.nodes_in_cluster)
            if len(crashes) > 0:
                self.task.jython_task_manager.abort_all_tasks()

            self.assertTrue(len(crashes) == 0,
                            "Found servers having crashes")

            self.assertTrue(self.bucket_util._wait_warmup_completed(
                [self.cluster_util.cluster.master],
                self.bucket_util.buckets[0],
                wait_time=self.wait_timeout * 20))


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
        self.assertTrue(self.rest.update_autofailover_settings(False, 600),
                        "AutoFailover disabling failed")

        self.gen_create = doc_generator(
            self.key, self.num_items, self.num_items * 20,
            doc_size=self.doc_size,
            doc_type=self.doc_type,
            target_vbucket=self.target_vbucket,
            vbuckets=self.cluster_util.vbuckets,
            randomize_doc_size=self.randomize_doc_size,
            randomize_value=self.randomize_value)

        self.gen_update = doc_generator(
            self.key, 0, self.num_items/2,
            doc_size=self.doc_size,
            doc_type=self.doc_type,
            target_vbucket=self.target_vbucket,
            vbuckets=self.cluster_util.vbuckets,
            randomize_doc_size=self.randomize_doc_size,
            randomize_value=self.randomize_value)

        self.gen_delete = doc_generator(
            self.key, self.num_items/2, self.num_items,
            doc_size=self.doc_size,
            doc_type=self.doc_type,
            target_vbucket=self.target_vbucket,
            vbuckets=self.cluster_util.vbuckets,
            randomize_doc_size=self.randomize_doc_size,
            randomize_value=self.randomize_value)

        th = threading.Thread(target=self.crash)
        th.start()

        tasks = self.loadgen_docs(retry_exceptions=retry_exceptions,
                                  skip_read_on_error=True,
                                  suppress_error_table=True,
                                  _sync=False)

        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)
        self.stop_crash = True
        th.join()


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
