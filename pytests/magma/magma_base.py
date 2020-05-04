import os
import copy

from BucketLib.bucket import Bucket
from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import doc_generator
from membase.api.rest_client import RestConnection
from sdk_exceptions import SDKException
from remote.remote_util import RemoteMachineShellConnection
from cb_tools.cbstats import Cbstats
from testconstants import INDEX_QUOTA, CBAS_QUOTA, FTS_QUOTA


class MagmaBaseTest(BaseTestCase):
    def setUp(self):
        super(MagmaBaseTest, self).setUp()
        self.rest = RestConnection(self.cluster.master)
        self.doc_ops = self.input.param("doc_ops", "create")
        self.key_size = self.input.param("key_size", 8)
        self.replica_to_update = self.input.param("new_replica", None)
        self.randomize_doc_size = self.input.param("randomize_doc_size", False)
        self.randomize_value = self.input.param("randomize_value", False)
        self.rev_write = self.input.param("rev_write", False)
        self.rev_read = self.input.param("rev_read", False)
        self.rev_update = self.input.param("rev_update", False)
        self.rev_del = self.input.param("rev_del", False)
        self.random_key = self.input.param("random_key", False)
        self.mix_key_size = self.input.param("mix_key_size", False)
        self.key = 'test_docs'
        if self.random_key:
            self.key = "random_keys"
        self.items = self.num_items
        self.check_temporary_failure_exception = False
        self.fragmentation = int(self.input.param("fragmentation", 50))
        self.dgm_batch = self.input.param("dgm_batch", 5000)
        self.retry_exceptions = [SDKException.TimeoutException,
                                 SDKException.AmbiguousTimeoutException,
                                 SDKException.RequestCanceledException,
                                 SDKException.UnambiguousTimeoutException]
        self.ignore_exceptions = []
        self.info = self.rest.get_nodes_self()
        self.rest.init_cluster(username=self.cluster.master.rest_username,
                               password=self.cluster.master.rest_password)
        self.kv_memory = int(self.info.mcdMemoryReserved) - 100
        if "index" in self.cluster.master.services:
            self.kv_memory -= INDEX_QUOTA
        if "fts" in self.cluster.master.services:
            self.kv_memory -= FTS_QUOTA
        if "cbas" in self.cluster.master.services:
            self.kv_memory -= CBAS_QUOTA
        self.rest.init_cluster_memoryQuota(memoryQuota=self.kv_memory)
        nodes_init = self.cluster.servers[
            1:self.nodes_init] if self.nodes_init != 1 else []
        if nodes_init:
            result = self.task.rebalance([self.cluster.master], nodes_init, [])
            self.assertTrue(result, "Initial rebalance failed")
        self.cluster.nodes_in_cluster.extend(
            [self.cluster.master] + nodes_init)
        self.check_replica = self.input.param("check_replica", False)
        self.bucket_storage = self.input.param("bucket_storage",
                                               Bucket.StorageBackend.magma)
        self.bucket_eviction_policy = self.input.param(
            "bucket_eviction_policy",
            Bucket.EvictionPolicy.FULL_EVICTION)
        self.bucket_util.add_rbac_user()
        # below will come in to picture if we'll have standard_buckets > 1
        self.magma_buckets = self.input.param("magma_buckets", 0)
        if self.standard_buckets > 10:
            self.bucket_util.change_max_buckets(self.standard_buckets)
        if self.standard_buckets == 1:
            self._create_default_bucket()
        else:
            self._create_multiple_buckets()

        self.disable_magma_commit_points = self.input.param(
            "disable_magma_commit_points", False)

        props = "magma"
        update_bucket_props = False

        if self.disable_magma_commit_points:
            props += ";magma_max_commit_points=0"
            update_bucket_props = True

        if self.fragmentation != 50:
            props += ";magma_delete_frag_ratio=%s" % str(self.fragmentation/100.0)
            update_bucket_props = True

        if update_bucket_props:
            self.bucket_util.update_bucket_props(
                    "backend", props,
                    self.bucket_util.buckets)

        self.doc_size = self.input.param("doc_size", 2048)
        self.test_itr = self.input.param("test_itr", 4)
        self.update_itr = self.input.param("update_itr", 10)
        self.next_half = self.input.param("next_half", False)
        self.deep_copy = self.input.param("deep_copy", False)
        if self.active_resident_threshold < 100:
            self.check_temporary_failure_exception = True
        self.gen_create = None
        self.gen_delete = None
        self.gen_read = None
        self.gen_update = None

        # self.thread_count is used to define number of thread use
        # to read same number of documents parallelly
        self.read_thread_count = self.input.param("read_thread_count", 4)
        self.log.info("==========Finished magma base setup========")

    def _create_default_bucket(self):
        if self.kv_memory < 100:
            self.kv_memory = 100
        self.bucket_util.create_default_bucket(
            ram_quota=self.kv_memory,
            bucket_type=self.bucket_type,
            replica=self.num_replicas,
            storage=self.bucket_storage,
            eviction_policy=self.bucket_eviction_policy)

    def _create_multiple_buckets(self):
        buckets_created = self.bucket_util.create_multiple_buckets(
            self.cluster.master,
            self.num_replicas,
            bucket_count=self.standard_buckets,
            bucket_type=self.bucket_type,
            storage={"couchstore": self.standard_buckets - self.magma_buckets,
                     "magma": self.magma_buckets},
            eviction_policy=self.bucket_eviction_policy)
        self.assertTrue(buckets_created, "Unable to create multiple buckets")

        for bucket in self.bucket_util.buckets:
            ready = self.bucket_util.wait_for_memcached(
                self.cluster.master,
                bucket)
            self.assertTrue(ready, msg="Wait_for_memcached failed")

    def tearDown(self):
        self.cluster_util.print_cluster_stats()
        super(MagmaBaseTest, self).tearDown()

    def _load_all_buckets(self, cluster, kv_gen, op_type, exp, flag=0,
                          only_store_hash=True, batch_size=1000, pause_secs=1,
                          timeout_secs=30, compression=True, dgm_batch=5000):

        retry_exceptions = list([SDKException.AmbiguousTimeoutException])
        tasks_info = self.bucket_util.sync_load_all_buckets(
            cluster, kv_gen, op_type, exp, flag,
            persist_to=self.persist_to, replicate_to=self.replicate_to,
            durability=self.durability_level, timeout_secs=timeout_secs,
            only_store_hash=only_store_hash, batch_size=batch_size,
            pause_secs=pause_secs, sdk_compression=compression,
            process_concurrency=self.process_concurrency,
            retry_exceptions=retry_exceptions,
            active_resident_threshold=self.active_resident_threshold)
        if self.active_resident_threshold < 100:
            for task, _ in tasks_info.items():
                self.num_items = task.doc_index
        self.assertTrue(self.bucket_util.doc_ops_tasks_status(tasks_info),
                        "Doc_ops failed in rebalance_base._load_all_buckets")
        return tasks_info

    def start_parallel_cruds(self,
                             retry_exceptions=[],
                             ignore_exceptions=[],
                             _sync=True):
        tasks_info = dict()
        read_tasks_info = dict()
        read_task = False
        if "update" in self.doc_ops and self.gen_update is not None:
            tem_tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, self.gen_update, "update", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level, pause_secs=5,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                retry_exceptions=retry_exceptions,
                ignore_exceptions=ignore_exceptions)
            tasks_info.update(tem_tasks_info.items())
        if "create" in self.doc_ops and self.gen_create is not None:
            tem_tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, self.gen_create, "create", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level, pause_secs=5,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                retry_exceptions=retry_exceptions,
                ignore_exceptions=ignore_exceptions)
            tasks_info.update(tem_tasks_info.items())
            self.num_items += (self.gen_create.end - self.gen_create.start)
        if "read" in self.doc_ops and self.gen_read is not None:
            read_tasks_info = self.bucket_util._async_validate_docs(
               self.cluster, self.gen_read, "read", 0,
               batch_size=self.batch_size,
               process_concurrency=self.process_concurrency,
               pause_secs=5, timeout_secs=self.sdk_timeout,
               retry_exceptions=retry_exceptions,
               ignore_exceptions=ignore_exceptions)
            read_task = True
        if "delete" in self.doc_ops and self.gen_delete is not None:
            tem_tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, self.gen_delete, "delete", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level, pause_secs=5,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                retry_exceptions=retry_exceptions,
                ignore_exceptions=ignore_exceptions)
            tasks_info.update(tem_tasks_info.items())
            self.num_items -= (self.gen_delete.end - self.gen_delete.start)

        if _sync:
            for task in tasks_info:
                self.task_manager.get_task_result(task)

            self.bucket_util.verify_doc_op_task_exceptions(tasks_info,
                                                           self.cluster)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)

        if read_task:
            # TODO: Need to converge read_tasks_info into tasks_info before
            #       itself to avoid confusions during _sync=False case
            tasks_info.update(read_tasks_info.items())
            if _sync:
                for task in read_tasks_info:
                    self.task_manager.get_task_result(task)

        return tasks_info

    def loadgen_docs(self,
                     retry_exceptions=[],
                     ignore_exceptions=[],
                     _sync=True):

        if self.check_temporary_failure_exception:
            retry_exceptions.append(SDKException.TemporaryFailureException)
        loaders = self.start_parallel_cruds(retry_exceptions,
                                            ignore_exceptions,
                                            _sync)
        return loaders

    def get_magma_stats(self, bucket, servers=None, field_to_grep=None):
        magma_stats_for_all_servers = dict()
        if servers is None:
            servers = self.cluster.nodes_in_cluster
        if type(servers) is not list:
            servers = [servers]
        for server in servers:
            result = dict()
            shell = RemoteMachineShellConnection(server)
            cbstat_obj = Cbstats(shell)
            result = cbstat_obj.magma_stats(bucket.name,
                                            field_to_grep=field_to_grep)
            shell.disconnect()
            magma_stats_for_all_servers[server.ip] = result
        return magma_stats_for_all_servers

    def get_disk_usage(self, bucket, servers=None):
        disk_usage = []
        if servers is None:
            servers = self.cluster.nodes_in_cluster
        if type(servers) is not list:
            servers = [servers]
        kvstore = 0
        wal = 0
        keyTree = 0
        seqTree = 0
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            kvstore += int(shell.execute_command("du -cm %s | tail -1 | awk '{print $1}'\
            " % os.path.join(RestConnection(server).get_data_path(),
                             bucket.name, "magma.*/kv*"))[0][0].split('\n')[0])
            wal += int(shell.execute_command("du -cm %s | tail -1 | awk '{print $1}'\
            " % os.path.join(RestConnection(server).get_data_path(),
                             bucket.name, "magma.*/wal"))[0][0].split('\n')[0])
            keyTree += int(shell.execute_command("du -cb %s | tail -1 | awk '{print $1}'\
            " % os.path.join(RestConnection(server).get_data_path(),
                             bucket.name, "magma.*/kv*/rev*/key*"))[0][0].split('\n')[0])
            seqTree += int(shell.execute_command("du -cb %s | tail -1 | awk '{print $1}'\
            " % os.path.join(RestConnection(server).get_data_path(),
                             bucket.name, "magma.*/kv*/rev*/seq*"))[0][0].split('\n')[0])
            shell.disconnect()
        self.log.info("Disk usage stats for bucekt {} is below".format(bucket.name))
        self.log.info("Total Disk usage for kvstore is {}MB".format(kvstore))
        self.log.debug("Total Disk usage for wal is {}MB".format(wal))
        self.log.debug("Total Disk usage for keyTree is {}B".format(keyTree))
        self.log.debug("Total Disk usage for seqTree is {}B".format(seqTree))
        disk_usage.extend([kvstore, wal, keyTree, seqTree])
        return disk_usage

    def enable_disable_swap_space(self, servers=None, disable=True):
        if servers is None:
            servers = self.cluster.nodes_in_cluster
        if type(servers) is not list:
            servers = [servers]
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            if disable:
                _ = shell.execute_command("swapoff -a")
                self.sleep(5)
                output = shell.execute_command(
                    "free | tail -1 | awk '{print $2}'")[0][0].split('\n')[0]
                self.assertEqual(
                    int(output), 0,
                    msg="Failed to disable swap space on server {} having value {} \
                     ".format(server, output))
            else:
                _ = shell.execute_command("swapon -a")
                self.sleep(5)
                output = shell.execute_command(
                    "free | tail -1 | awk '{print $2}'")[0][0].split('\n')[0]
                self.assertNotEqual(
                    int(output), 0,
                    msg="Failed to enable swap space on server {} having value {} \
                    ".format(server, output))
        return