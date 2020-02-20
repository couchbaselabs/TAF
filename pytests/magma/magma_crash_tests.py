'''
Created on Dec 12, 2019

@author: riteshagarwal
'''

import os
import random

from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from magma_base import MagmaBaseTest
from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection


class MagmaCrashTests(MagmaBaseTest):
    def setUp(self):
        super(MagmaCrashTests, self).setUp()

    def tearDown(self):
        super(MagmaCrashTests, self).tearDown()

    def kill_magma_check_wal_file_size(self):
        nIter = 200
        while nIter > 0:
            shell = RemoteMachineShellConnection(self.cluster_util.cluster.master)
            shell.kill_memcached()
#             self.bucket_util._wait_warmup_completed()
            self.sleep(10, "sleep of 5s so that memcached can restart")

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
                wait_time=self.wait_timeout * 10))
            if self.doc_size_randomize:
                self.doc_size = random.randint(0, self.doc_size)
            self.gen_create = doc_generator(self.key,
                                            start,
                                            end,
                                            doc_size=self.doc_size,
                                            doc_type=self.doc_type,
                                            target_vbucket=self.target_vbucket,
                                            vbuckets=self.cluster_util.vbuckets,
                                            randomize_doc_size=self.randomize_doc_size,
                                            randomize_value=self.randomize_value)
            self.loadgen_docs(_sync=True)
            self.bucket_util._wait_for_stats_all_buckets()
            if not (self.randomize_doc_size or self.randomize_value):
                data_validation = self.task.async_validate_docs(
                    self.cluster, self.bucket_util.buckets[0],
                    self.gen_create, "create", 0, batch_size=10)
                self.task.jython_task_manager.get_task_result(data_validation)
            start = end
            self.bucket_util.verify_stats_all_buckets(end, timeout=300)
            self.gen_update = self.gen_create

    def test_magma_rollback_n_times(self):
        items = self.num_items
        mem_only_items = self.input.param("rollback_items", 100000)
        if self.nodes_init < 2 or self.num_replicas < 1:
            self.fail("Not enough nodes/replicas in the cluster/bucket to test rollback")
        self.num_rollbacks = self.input.param("num_rollbacks", 10)
        shell = RemoteMachineShellConnection(self.cluster_util.cluster.master)
        cbstats = Cbstats(shell)
        self.target_vbucket = cbstats.vbucket_list(self.bucket_util.buckets[0].name)
        start = self.num_items
        for _ in xrange(1, self.num_rollbacks+1):
            # Stopping persistence on NodeA
            mem_client = MemcachedClientHelper.direct_client(
                self.input.servers[0], self.bucket_util.buckets[0])
            mem_client.stop_persistence()
            self.gen_create = doc_generator(self.key,
                                            start,
                                            mem_only_items,
                                            doc_size=self.doc_size,
                                            doc_type=self.doc_type,
                                            target_vbucket=self.target_vbucket,
                                            vbuckets=self.cluster_util.vbuckets,
                                            randomize_doc_size=self.randomize_doc_size,
                                            randomize_value=self.randomize_value)
            self.loadgen_docs(_sync=True)
            start = self.gen_create.key_counter
            ep_queue_size_map = {self.cluster.nodes_in_cluster[0]: mem_only_items}
            vb_replica_queue_size_map = {self.cluster.nodes_in_cluster[0]: 0}
            for node in self.cluster.nodes_in_cluster[1:]:
                ep_queue_size_map.update({node: 0})
                vb_replica_queue_size_map.update({node: 0})

            for bucket in self.bucket_util.buckets:
                self.bucket_util._wait_for_stat(bucket, ep_queue_size_map)
                self.bucket_util._wait_for_stat(bucket,
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
            self.bucket_util.verify_stats_all_buckets(items, timeout=300)
            self.sleep(5, "Not Required, but waiting for 5s after warmup")
            for bucket in self.bucket_util.buckets:
                self.log.debug(cbstats.failover_stats(bucket.name))
        shell.disconnect()

    def test_magma_rollback_to_0(self):
        items = self.num_items
        mem_only_items = self.input.param("rollback_items", 10000)
        if self.nodes_init < 2 or self.num_replicas < 1:
            self.fail("Not enough nodes/replicas in the cluster/bucket to test rollback")
        self.num_rollbacks = self.input.param("num_rollbacks", 10)
        shell = RemoteMachineShellConnection(self.cluster_util.cluster.master)
        self.target_vbucket = Cbstats(shell).vbucket_list(self.bucket_util.buckets[0].name)
        start = self.num_items
        # Stopping persistence on NodeA
        mem_client = MemcachedClientHelper.direct_client(
            self.input.servers[0], self.bucket_util.buckets[0])
        mem_client.stop_persistence()
        for i in xrange(1, self.num_rollbacks+1):
            self.gen_create = doc_generator(self.key,
                                            start,
                                            mem_only_items,
                                            doc_size=self.doc_size,
                                            doc_type=self.doc_type,
                                            target_vbucket=self.target_vbucket,
                                            vbuckets=self.cluster_util.vbuckets,
                                            randomize_doc_size=self.randomize_doc_size,
                                            randomize_value=self.randomize_value)
            self.loadgen_docs(_sync=True)
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

    def test_space_amplification(self):
        self.num_updates = self.input.param("num_updates", 50)
        items = self.num_items
        self.assertTrue(self.rest.update_autofailover_settings(False, 600),
                        "AutoFailover disabling failed")
        self.doc_ops = "update"
        metadata_size = 200
        exact_size = self.num_items*(self.doc_size + metadata_size)\
            * (1 + self.num_replicas)
        max_size = exact_size * 2
        for i in xrange(1, self.num_updates+1):
            self.log.info("Iteration: {}, updating {} items".format(i, items))
            start = 0
            end = items
            self.assertTrue(self.bucket_util._wait_warmup_completed(
                [self.cluster_util.cluster.master],
                self.bucket_util.buckets[0],
                wait_time=self.wait_timeout * 10))
            self.gen_update = doc_generator(self.key,
                                            start,
                                            end,
                                            doc_size=self.doc_size,
                                            doc_type=self.doc_type,
                                            target_vbucket=self.target_vbucket,
                                            vbuckets=self.cluster_util.vbuckets,
                                            randomize_doc_size=self.randomize_doc_size,
                                            randomize_value=self.randomize_value)
            self.loadgen_docs(_sync=True)
            self.bucket_util._wait_for_stats_all_buckets()
            data_validation = self.task.async_validate_docs(
                self.cluster, self.bucket_util.buckets[0],
                self.gen_update, "update", 0, batch_size=10)
            self.task.jython_task_manager.get_task_result(data_validation)
            self.bucket_util.verify_stats_all_buckets(end, timeout=300)

            # check for space amplification here
            data_size = 0
            for server in self.cluster.nodes_in_cluster:
                shell = RemoteMachineShellConnection(server)
                for bucket in self.bucket_util.buckets:
                    data_size += int(
                        shell.execute_command(
                            "du -c %s | tail -1 | awk '{print $1}'" %
                            os.path.join(
                                RestConnection(server).get_data_path(),
                                bucket.name, "magma.*/kvstore*")
                            )[0][0].rstrip("\n")
                        )
                shell.disconnect()

            data_size *= 1024

            self.assertTrue(
                data_size >= exact_size and data_size <= max_size,
                "Exact Data Size {} \n\
                Actual Size {} \n\
                Max Expected Size {}".format(exact_size, data_size, max_size)
                )
