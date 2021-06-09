'''
Created on 16-Feb-2021

@author: riteshagarwal
'''
import copy
import os
import threading
import time

from Cb_constants.CBServer import CbServer
from cb_tools.cbstats import Cbstats
from magma.magma_base import MagmaBaseTest
from memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection
from sdk_constants.java_client import SDKConstants


class MagmaDiskFull(MagmaBaseTest):

    def setUp(self):
        super(MagmaDiskFull, self).setUp()
        self.free_disk(self.cluster.master)
        self.crash_on_disk_full = False
        self.assertTrue(self.rest.update_autofailover_settings(False, 600),
                        "AutoFailover disabling failed")

    def tearDown(self):
        for node in self.servers:
            self.free_disk(node)
        MagmaBaseTest.tearDown(self)

    def simulate_persistence_failure(self, servers=None):
        servers = servers or [self.cluster.master]
        ep_data_write_failed = dict()
        for server in servers:
            ep_data_write_failed.update({server: 0})
        self.create_start = self.init_items_per_collection
        self.create_end = self.create_start
        iterations = 10
        while iterations > 0:
            tasks_info = dict()
            self.doc_ops = "create"
            self.create_start = self.create_end
            self.create_end = self.create_start + 1000000
            self.generate_docs()
            for collection in self.collections:
                tem_tasks_info = self.loadgen_docs(self.retry_exceptions,
                                                   self.ignore_exceptions,
                                                   scope=CbServer.default_scope,
                                                   collection=collection,
                                                   suppress_error_table=True,
                                                   skip_read_on_error=True,
                                                   _sync=False,
                                                   doc_ops=self.doc_ops,
                                                   track_failures=False,
                                                   sdk_retry_strategy=SDKConstants.RetryStrategy.FAIL_FAST)
                tasks_info.update(tem_tasks_info.items())

            for task in tasks_info:
                self.task_manager.get_task_result(task)

            for bucket in self.bucket_util.buckets:
                try:
                    self.bucket_util._wait_for_stat(bucket, ep_data_write_failed,
                                                    cbstat_cmd="all",
                                                    stat_name="ep_data_write_failed",
                                                    stat_cond=">", timeout=60)
                    self.get_bucket_dgm(bucket)
                    return
                except:
                    pass
            iterations -= 1
        raise Exception("Could not hit Write Commit Failures: ep_data_write_failed>0")

    def fill_disk(self, server, chunk=1024, free=50):
        def _get_disk_usage_in_MB(remote_client, path):
            disk_info = remote_client.get_disk_info(in_MB=True, path=path)
            disk_space = disk_info[1].split()[-3][:-1]
            return disk_space

        # Fill up the disk
        remote_client = RemoteMachineShellConnection(server)
        du = int(_get_disk_usage_in_MB(remote_client, server.data_path)) - free
        _file = os.path.join(self.cluster.master.data_path, "full_disk_")
        while int(du) > 0:
            cmd = "dd if=/dev/zero of={0}{1} bs={2}M count=1"
            cmd = cmd.format(_file, str(du) + "MB_" + str(time.time()), chunk)
            self.log.debug(cmd)
            _, error = remote_client.execute_command(cmd,
                                                     use_channel=True)
            if error:
                self.log.error("".join(error))
            du -= chunk
            if du < chunk:
                chunk = du
        remote_client.disconnect()

    def free_disk(self, server):
        remote_client = RemoteMachineShellConnection(server)
        _file = os.path.join(server.data_path, "full_disk_")
        command = "rm -rf {}*".format(_file)
        output, error = remote_client.execute_command(command)
        if output:
            self.log.info(output)
        if error:
            self.log.error(error)
        remote_client.disconnect()
        self.sleep(10, "Wait for files to clean up from the disk")

    def test_simple_disk_full(self):
        th = list()
        for node in self.cluster.nodes_in_cluster:
            t = threading.Thread(target=self.fill_disk,
                                 kwargs=dict(server=node,
                                             free=0))
            t.start()
            th.append(t)
        for t in th:
            t.join()
        self.simulate_persistence_failure(self.cluster.nodes_in_cluster)

        if self.crash_on_disk_full:
            th = threading.Thread(target=self.crash)
            th.start()
            self.sleep(300)
            self.stop_crash = True
            th.join()
            self.assertFalse(self.crash_failure, "CRASH | CRITICAL | WARN messages found in cb_logs")

        for node in self.cluster.nodes_in_cluster:
            self.free_disk(node)

        self.bucket_util._wait_for_stats_all_buckets()
        self.doc_ops = "update"
        self.generate_docs(update_start=self.create_start,
                           update_end=self.create_end)
        tasks_info = dict()
        for collection in self.collections:
            tem_tasks_info = self.loadgen_docs(self.retry_exceptions,
                                               self.ignore_exceptions,
                                               scope=CbServer.default_scope,
                                               collection=collection,
                                               suppress_error_table=True,
                                               doc_ops=self.doc_ops)
            tasks_info.update(tem_tasks_info.items())

        for task, task_info in tasks_info.items():
            self.assertFalse(task_info["ops_failed"],
                             "Doc ops failed for task: {}".format(task.thread_name))

    def test_crash_recovery_disk_full(self):
        self.crash_on_disk_full = True
        self.test_simple_disk_full()

    def test_reads_on_disk_full(self):
        th = list()
        for node in self.cluster.nodes_in_cluster:
            t = threading.Thread(target=self.fill_disk,
                                 kwargs=dict(server=node,
                                             free=0))
            t.start()
            th.append(t)
        for t in th:
            t.join()
        self.simulate_persistence_failure(self.cluster.nodes_in_cluster)
        self.read_start = 0
        self.read_end = self.init_items_per_collection
        self.doc_ops = "read"
        self.generate_docs()
        tasks_info = dict()
        for collection in self.collections:
            tem_tasks_info = self.loadgen_docs(self.retry_exceptions,
                                               self.ignore_exceptions,
                                               scope=CbServer.default_scope,
                                               collection=collection,
                                               suppress_error_table=True,
                                               doc_ops=self.doc_ops)
            tasks_info.update(tem_tasks_info.items())

    def test_rollback_after_disk_full(self):
        self.doc_ops = "create"
        self.create_start = self.init_items_per_collection
        self.create_end = self.init_items_per_collection*2
        start = self.num_items
        items = self.num_items
        mem_only_items = self.input.param("rollback_items", 100000)
        self.gen_read = copy.deepcopy(self.gen_create)

        # Fill Disk on nodeB leaving 100MB
        self.fill_disk(self.cluster.nodes_in_cluster[-1], free=100)

        # Stopping persistence on NodeA
        shell = RemoteMachineShellConnection(self.cluster_util.cluster.master)
        cbstats = Cbstats(shell)
        self.target_vbucket = cbstats.vbucket_list(self.bucket_util.buckets[0].
                                                   name)
        mem_client = MemcachedClientHelper.direct_client(
            self.cluster_util.cluster.master, self.bucket_util.buckets[0])
        mem_client.stop_persistence()
        self.gen_create = self.genrate_docs_basic(start, mem_only_items,
                                                  self.target_vbucket)

        self.loadgen_docs(_sync=True, retry_exceptions=self.retry_exceptions)
        start = self.gen_create.key_counter

        ep_queue_size_map = {self.cluster.nodes_in_cluster[0]:
                             mem_only_items}
        ep_data_write_failed = {self.cluster.nodes_in_cluster[-1]: 0}

        for bucket in self.bucket_util.buckets:
            self.bucket_util._wait_for_stat(bucket, ep_queue_size_map)
            self.bucket_util._wait_for_stat(
                bucket,
                ep_data_write_failed,
                cbstat_cmd="all",
                stat_name="ep_data_write_failed",
                stat_cond=">",
                timeout=300)

        # Kill memcached on NodeA to trigger rollback on other Nodes
        # replica vBuckets
        self.sleep(120)
        shell.kill_memcached()
        self.sleep(10, "sleep after MemCached kill on node {}".format(shell.ip))

        self.free_disk(self.cluster.nodes_in_cluster[-1])
        self.assertTrue(self.bucket_util._wait_warmup_completed(
            self.cluster.nodes_in_cluster,
            self.bucket_util.buckets[0],
            wait_time=self.wait_timeout * 10))
        self.sleep(10, "Not Required, but waiting for 10s after warm up")
        self.bucket_util.verify_stats_all_buckets(items, timeout=300)

        data_validation = self.task.async_validate_docs(
                self.cluster, self.bucket_util.buckets[0],
                self.gen_read, "create", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                pause_secs=5, timeout_secs=self.sdk_timeout)
        self.task.jython_task_manager.get_task_result(data_validation)

        shell.disconnect()

    def test_random_keyTree_chmod(self):
        self.gen_read = copy.deepcopy(self.gen_create)
        keyIndex = self.get_random_keyIndex()
        for server in self.cluster.nodes_in_cluster:
            chmod_th = threading.Thread(target=self.chmod, kwargs=dict(
                server=server, path=keyIndex, mod="000"))
            chmod_th.start()
        self.doc_ops = "create"
        self.create_start = self.init_items_per_collection
        self.create_end = self.init_items_per_collection*2
        self.generate_docs()

        tasks_info = dict()
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

        data_validation = self.task.async_validate_docs(
                self.cluster, self.bucket_util.buckets[0],
                self.gen_read, "create", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                pause_secs=5, timeout_secs=self.sdk_timeout)
        self.task.jython_task_manager.get_task_result(data_validation)

        for task in tasks_info:
            self.task_manager.get_task_result(task)
        self.stop_chmod = True

        for server in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(server)
            shell.execute_command("chmod {} {}".format("777", keyIndex))
            shell.disconnect()

        self.doc_ops = "update"
        self.update_start = self.init_items_per_collection
        self.update_end = self.init_items_per_collection*2
        self.generate_docs()

        tasks_info = dict()
        for collection in self.collections:
            tem_tasks_info = self.loadgen_docs(self.retry_exceptions,
                                               self.ignore_exceptions,
                                               scope=CbServer.default_scope,
                                               collection=collection,
                                               doc_ops=self.doc_ops)
            tasks_info.update(tem_tasks_info.items())
        for task in tasks_info:
            self.assertTrue(len(task.fail) == 0,
                            "Doc ops failed for task: {}".format(task.thread_name))

    def test_random_seqTree_chmod(self):
        self.gen_read = copy.deepcopy(self.gen_create)
        seqIndex = self.get_random_seqIndex()
        for server in self.cluster.nodes_in_cluster:
            chmod_th = threading.Thread(target=self.chmod, kwargs=dict(
                server=server, path=seqIndex, mod="000"))
            chmod_th.start()
        self.doc_ops = "create"
        self.create_start = self.init_items_per_collection
        self.create_end = self.init_items_per_collection*2
        self.generate_docs()

        tasks_info = dict()
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

        data_validation = self.task.async_validate_docs(
                self.cluster, self.bucket_util.buckets[0],
                self.gen_read, "create", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                pause_secs=5, timeout_secs=self.sdk_timeout)
        self.task.jython_task_manager.get_task_result(data_validation)

        for task in tasks_info:
            self.task_manager.get_task_result(task)
        self.stop_chmod = True

        for server in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(server)
            shell.execute_command("chmod {} {}".format("777", seqIndex))
            shell.disconnect()

        self.doc_ops = "update"
        self.update_start = self.init_items_per_collection
        self.update_end = self.init_items_per_collection*2
        self.generate_docs()

        tasks_info = dict()
        for collection in self.collections:
            tem_tasks_info = self.loadgen_docs(self.retry_exceptions,
                                               self.ignore_exceptions,
                                               scope=CbServer.default_scope,
                                               collection=collection,
                                               doc_ops=self.doc_ops)
            tasks_info.update(tem_tasks_info.items())
        for task in tasks_info:
            self.assertTrue(len(task.fail) == 0,
                            "Doc ops failed for task: {}".format(task.thread_name))

    def test_random_wal_chmod(self):
        self.gen_read = copy.deepcopy(self.gen_create)
        wal = self.get_random_wal()
        for server in self.cluster.nodes_in_cluster:
            chmod_th = threading.Thread(target=self.chmod, kwargs=dict(
                server=server, path=wal, mod="000"))
            chmod_th.start()
        self.doc_ops = "create"
        self.create_start = self.init_items_per_collection
        self.create_end = self.init_items_per_collection*2
        self.generate_docs()

        tasks_info = dict()
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

        data_validation = self.task.async_validate_docs(
                self.cluster, self.bucket_util.buckets[0],
                self.gen_read, "create", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                pause_secs=5, timeout_secs=self.sdk_timeout)
        self.task.jython_task_manager.get_task_result(data_validation)

        for task in tasks_info:
            self.task_manager.get_task_result(task)
        self.stop_chmod = True

        for server in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(server)
            shell.execute_command("chmod {} {}".format("777", wal))
            shell.disconnect()

        self.doc_ops = "update"
        self.update_start = self.init_items_per_collection
        self.update_end = self.init_items_per_collection*2
        self.generate_docs()

        tasks_info = dict()
        for collection in self.collections:
            tem_tasks_info = self.loadgen_docs(self.retry_exceptions,
                                               self.ignore_exceptions,
                                               scope=CbServer.default_scope,
                                               collection=collection,
                                               doc_ops=self.doc_ops)
            tasks_info.update(tem_tasks_info.items())
        for task in tasks_info:
            self.assertTrue(len(task.fail) == 0,
                            "Doc ops failed for task: {}".format(task.thread_name))

    def test_random_kvStore_chmod(self):
        self.gen_read = copy.deepcopy(self.gen_create)
        kvstore = self.get_random_kvstore()
        for server in self.cluster.nodes_in_cluster:
            chmod_th = threading.Thread(target=self.chmod, kwargs=dict(
                server=server, path=kvstore, mod="000"))
            chmod_th.start()
        self.doc_ops = "create"
        self.create_start = self.init_items_per_collection
        self.create_end = self.init_items_per_collection*2
        self.generate_docs()

        tasks_info = dict()
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

        data_validation = self.task.async_validate_docs(
                self.cluster, self.bucket_util.buckets[0],
                self.gen_read, "create", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                pause_secs=5, timeout_secs=self.sdk_timeout)
        self.task.jython_task_manager.get_task_result(data_validation)

        for task in tasks_info:
            self.task_manager.get_task_result(task)
        self.stop_chmod = True

        for server in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(server)
            shell.execute_command("chmod {} {}".format("777", kvstore))
            shell.disconnect()

        self.doc_ops = "update"
        self.update_start = self.init_items_per_collection
        self.update_end = self.init_items_per_collection*2
        self.generate_docs()

        tasks_info = dict()
        for collection in self.collections:
            tem_tasks_info = self.loadgen_docs(self.retry_exceptions,
                                               self.ignore_exceptions,
                                               scope=CbServer.default_scope,
                                               collection=collection,
                                               doc_ops=self.doc_ops)
            tasks_info.update(tem_tasks_info.items())
        for task in tasks_info:
            self.assertTrue(len(task.fail) == 0,
                            "Doc ops failed for task: {}".format(task.thread_name))

    def test_disk_full_reduce_replica(self):
        self.gen_read = copy.deepcopy(self.gen_create)
        self.doc_ops = "create"
        self.create_start = self.init_items_per_collection
        self.create_end = self.init_items_per_collection*2
        self.generate_docs()
        self.gen_update = copy.deepcopy(self.gen_create)

        th = list()
        for node in self.cluster.nodes_in_cluster:
            t = threading.Thread(target=self.fill_disk,
                                 kwargs=dict(server=node,
                                             free=200))
            t.start()
            th.append(t)
        for t in th:
            t.join()

        self.simulate_persistence_failure(self.cluster.nodes_in_cluster)
        data_validation = self.task.async_validate_docs(
            self.cluster, self.bucket_util.buckets[0],
            self.gen_read, "create", 0,
            batch_size=self.batch_size,
            process_concurrency=self.process_concurrency,
            pause_secs=5, timeout_secs=self.sdk_timeout)
        self.task.jython_task_manager.get_task_result(data_validation)

        self.bucket_util.update_all_bucket_replicas(replicas=1)
        rebalance_result = self.task.rebalance(self.cluster.nodes_in_cluster,
                                               [], [])
        self.assertTrue(rebalance_result)

        # check further doc ops go in well
        tasks_info = dict()
        self.doc_ops = "update"
        for collection in self.collections:
            tem_tasks_info = self.loadgen_docs(self.retry_exceptions,
                                               self.ignore_exceptions,
                                               scope=CbServer.default_scope,
                                               collection=collection,
                                               doc_ops=self.doc_ops)
            tasks_info.update(tem_tasks_info.items())

        for task in tasks_info:
            self.assertTrue(len(task.fail) == 0,
                            "Doc ops failed for task: {}".format(task.thread_name))

    def test_disk_full_on_increasing_replica(self):
        self.gen_read = copy.deepcopy(self.gen_create)
        ep_data_write_failed = dict()
        th = list()
        for node in self.cluster.nodes_in_cluster:
            ep_data_write_failed.update({node: 0})
            t = threading.Thread(target=self.fill_disk,
                                 kwargs=dict(server=node,
                                             free=200))
            t.start()
            th.append(t)
        for t in th:
            t.join()

        self.bucket_util.update_all_bucket_replicas(replicas=2)
        rebalance_result = self.task.rebalance(self.cluster.nodes_in_cluster,
                                               [], [])
        self.assertFalse(rebalance_result)
        self.rest.stop_rebalance(wait_timeout=300)

        data_validation = self.task.async_validate_docs(
            self.cluster, self.bucket_util.buckets[0],
            self.gen_read, "create", 0,
            batch_size=self.batch_size,
            process_concurrency=self.process_concurrency,
            pause_secs=5, timeout_secs=self.sdk_timeout)
        self.task.jython_task_manager.get_task_result(data_validation)

        self.bucket_util.update_all_bucket_replicas(replicas=1)
        rebalance_result = self.task.rebalance(self.cluster.nodes_in_cluster,
                                               [], [])
        self.assertTrue(rebalance_result)
        for node in self.cluster.nodes_in_cluster:
            self.free_disk(node)

        self.doc_ops = "create"
        self.create_start = self.init_items_per_collection
        self.create_end = self.init_items_per_collection*2
        self.generate_docs()

        self.fill_disk(self.cluster.master, free=100)
        tasks_info = dict()
        for collection in self.collections:
            tem_tasks_info = self.loadgen_docs(self.retry_exceptions,
                                               self.ignore_exceptions,
                                               scope=CbServer.default_scope,
                                               collection=collection,
                                               doc_ops=self.doc_ops)
            tasks_info.update(tem_tasks_info.items())

        for task in tasks_info:
            self.assertTrue(len(task.fail) == 0,
                            "Doc ops failed for task: {}".format(task.thread_name))

    def test_deletes_disk_full(self):
        self.gen_delete = copy.deepcopy(self.gen_create)
        self.gen_update = copy.deepcopy(self.gen_create)

        # create 50% fragmentation
        self.doc_ops = "update"
        tasks_info = dict()
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

        th = list()
        for node in self.cluster.nodes_in_cluster:
            t = threading.Thread(target=self.fill_disk,
                                 kwargs=dict(server=node,
                                             free=200))
            t.start()
            th.append(t)
        for t in th:
            t.join()

        self.simulate_persistence_failure(self.cluster.nodes_in_cluster)
        self.doc_ops = "delete"
        tasks_info = dict()
        for collection in self.collections:
            tem_tasks_info = self.loadgen_docs(self.retry_exceptions,
                                               self.ignore_exceptions,
                                               scope=CbServer.default_scope,
                                               collection=collection,
                                               doc_ops=self.doc_ops)
            tasks_info.update(tem_tasks_info.items())

        for task in tasks_info:
            self.assertTrue(len(task.fail) == 0,
                            "Doc ops failed for task: {}".format(task.thread_name))

    def test_delete_bucket_disk_full(self):
        th = list()
        for node in self.cluster.nodes_in_cluster:
            t = threading.Thread(target=self.fill_disk,
                                 kwargs=dict(server=node,
                                             free=200))
            t.start()
            th.append(t)
        for t in th:
            t.join()

        # Insert items so that disk gets full
        self.simulate_persistence_failure(self.cluster.nodes_in_cluster)

        for bucket in self.bucket_util.buckets:
            result = self.bucket_util.delete_bucket(self.cluster.master, bucket)
            self.assertTrue(result, "Bucket deletion failed: %s" % bucket.name)

    def test_disk_full_insert_ts(self):
        pass

    def test_disk_full_compaction(self):
        pass

    def test_magma_dump_disk_full(self):
        th = list()
        for node in self.cluster.nodes_in_cluster:
            t = threading.Thread(target=self.fill_disk,
                                 kwargs=dict(server=node,
                                             free=200))
            t.start()
            th.append(t)
        for t in th:
            t.join()

        # Insert items so that disk gets full
        self.simulate_persistence_failure(self.cluster.nodes_in_cluster)

        for server in self.cluster.nodes_in_cluster:
            self.get_tombstone_count_key(server)

    def test_disk_full_random_repeat(self):
        pass

    def test_bucket_flush_disk_full(self):
        th = list()
        for node in self.cluster.nodes_in_cluster:
            t = threading.Thread(target=self.fill_disk,
                                 kwargs=dict(server=node,
                                             free=200))
            t.start()
            th.append(t)
        for t in th:
            t.join()

        # Insert items so that disk gets full
        self.simulate_persistence_failure(self.cluster.nodes_in_cluster)

        # Check for data write failures due to disk full
        for bucket in self.bucket_util.buckets:
            self.assertTrue(self.bucket_util.flush_bucket(self.cluster.master,
                                                          bucket))

    def test_unmount_mount_partition(self):
        pass

    def test_disk_full_add_nodes(self):
        self.doc_ops = "create"
        self.create_start = self.init_items_per_collection
        self.create_end = self.init_items_per_collection*2
        self.generate_docs()
        self.gen_update = copy.deepcopy(self.gen_create)

        th = list()
        for node in self.cluster.nodes_in_cluster:
            t = threading.Thread(target=self.fill_disk,
                                 kwargs=dict(server=node,
                                             free=200))
            t.start()
            th.append(t)
        for t in th:
            t.join()

        # Insert items so that disk gets full
        self.simulate_persistence_failure(self.cluster.nodes_in_cluster)

        # Add a node to add disk to the cluster
        rebalance_result = self.task.rebalance(
            self.cluster.nodes_in_cluster,
            self.servers[self.nodes_init:self.nodes_init+1], [])
        self.assertTrue(rebalance_result)

        # Retry previous create failed after adding a node
        tasks_info = dict()
        self.doc_ops = "update"
        for collection in self.collections:
            tem_tasks_info = self.loadgen_docs(self.retry_exceptions,
                                               self.ignore_exceptions,
                                               scope=CbServer.default_scope,
                                               collection=collection,
                                               doc_ops=self.doc_ops)
            tasks_info.update(tem_tasks_info.items())

        for task in tasks_info:
            self.assertTrue(len(task.fail) == 0,
                            "Doc ops failed for task: {}".format(task.thread_name))
