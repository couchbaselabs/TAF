import math
import os

from BucketLib.bucket import Bucket
from Cb_constants.CBServer import CbServer
from basetestcase import BaseTestCase
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from sdk_exceptions import SDKException
from testconstants import INDEX_QUOTA, CBAS_QUOTA, FTS_QUOTA


class MagmaBaseTest(BaseTestCase):
    def setUp(self):
        super(MagmaBaseTest, self).setUp()
        self.rest = RestConnection(self.cluster.master)
        self.doc_ops = self.input.param("doc_ops", "create")
        self.key_size = self.input.param("key_size", 8)
        self.replica_to_update = self.input.param("new_replica", None)
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

        self.ep_queue_stats = self.input.param("ep_queue_stats", True)
        self.monitor_stats =["doc_ops", "ep_queue_size"]
        if not self.ep_queue_stats:
            self.monitor_stats =["doc_ops"]
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
        self.mutate = 0
        self.start = 0
        self.init_items = 0
        self.end = 0
        self.create_end = 0
        self.create_start = 0
        self.update_end = 0
        self.update_start = 0
        self.delete_end = 0
        self.delete_start = 0
        self.read_end = 0
        self.read_start = 0
        self.buckets = self.bucket_util.get_all_buckets()
        self.num_collections = self.input.param("num_collections", 2)
        self.num_scopes = self.input.param("num_scopes", 1)

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
                          timeout_secs=30, compression=True, dgm_batch=5000,
                          skip_read_on_error=False,
                          suppress_error_table=False):

        retry_exceptions = self.retry_exceptions
        tasks_info = self.bucket_util.sync_load_all_buckets(
            cluster, kv_gen, op_type, exp, flag,
            persist_to=self.persist_to, replicate_to=self.replicate_to,
            durability=self.durability_level, timeout_secs=timeout_secs,
            only_store_hash=only_store_hash, batch_size=batch_size,
            pause_secs=pause_secs, sdk_compression=compression,
            process_concurrency=self.process_concurrency,
            retry_exceptions=retry_exceptions,
            active_resident_threshold=self.active_resident_threshold,
            skip_read_on_error=skip_read_on_error,
            suppress_error_table=suppress_error_table,
            dgm_batch=dgm_batch,
            monitor_stats=self.monitor_stats)
        if self.active_resident_threshold < 100:
            for task, _ in tasks_info.items():
                self.num_items = task.doc_index
        self.assertTrue(self.bucket_util.doc_ops_tasks_status(tasks_info),
                        "Doc_ops failed in MagmaBase._load_all_buckets")
        return tasks_info

    def start_parallel_cruds(self,
                             retry_exceptions=[],
                             ignore_exceptions=[],
                             skip_read_on_error=False,
                             suppress_error_table=False,
                             scope=None,
                             collection=None,
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
                ignore_exceptions=ignore_exceptions,
                skip_read_on_error=skip_read_on_error,
                suppress_error_table=suppress_error_table,
                scope=scope,
                collection=collection,
                monitor_stats=self.monitor_stats)
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
                ignore_exceptions=ignore_exceptions,
                skip_read_on_error=skip_read_on_error,
                suppress_error_table=suppress_error_table,
                scope=scope,
                collection=collection,
                monitor_stats=self.monitor_stats)
            tasks_info.update(tem_tasks_info.items())
            self.num_items += (self.gen_create.end - self.gen_create.start)
        if "read" in self.doc_ops and self.gen_read is not None:
            read_tasks_info = self.bucket_util._async_validate_docs(
               self.cluster, self.gen_read, "read", 0,
               batch_size=self.batch_size,
               process_concurrency=self.process_concurrency,
               pause_secs=5, timeout_secs=self.sdk_timeout,
               retry_exceptions=retry_exceptions,
               ignore_exceptions=ignore_exceptions,
               scope=scope,
               collection=collection)
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
                ignore_exceptions=ignore_exceptions,
                skip_read_on_error=skip_read_on_error,
                suppress_error_table=suppress_error_table,
                scope=scope,
                collection=collection,
                monitor_stats=self.monitor_stats)
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
                     skip_read_on_error=False,
                     suppress_error_table=False,
                     scope=CbServer.default_scope,
                     collection=CbServer.default_collection,
                     _sync=True):

        if self.check_temporary_failure_exception:
            retry_exceptions.append(SDKException.TemporaryFailureException)
        loaders = self.start_parallel_cruds(retry_exceptions,
                                            ignore_exceptions,
                                            skip_read_on_error=skip_read_on_error,
                                            suppress_error_table=suppress_error_table,
                                            _sync=_sync,
                                            scope=scope,
                                            collection=collection)
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
            keyTree += int(shell.execute_command("du -cm %s | tail -1 | awk '{print $1}'\
            " % os.path.join(RestConnection(server).get_data_path(),
                             bucket.name, "magma.*/kv*/rev*/key*"))[0][0].split('\n')[0])
            seqTree += int(shell.execute_command("du -cm %s | tail -1 | awk '{print $1}'\
            " % os.path.join(RestConnection(server).get_data_path(),
                             bucket.name, "magma.*/kv*/rev*/seq*"))[0][0].split('\n')[0])
            shell.disconnect()
        self.log.info("Disk usage stats for bucekt {} is below".format(bucket.name))
        self.log.info("Total Disk usage for kvstore is {}MB".format(kvstore))
        self.log.debug("Total Disk usage for wal is {}MB".format(wal))
        self.log.debug("Total Disk usage for keyTree is {}MB".format(keyTree))
        self.log.debug("Total Disk usage for seqTree is {}MB".format(seqTree))
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

    def check_fragmentation_using_magma_stats(self, bucket, servers=None):
        result = dict()
        stats = list()
        if servers is None:
            servers = self.cluster.nodes_in_cluster
        if type(servers) is not list:
            servers = [servers]
        for server in servers:
            fragmentation_values = list()
            shell = RemoteMachineShellConnection(server)
            output = shell.execute_command(
                    "lscpu | grep 'CPU(s)' | head -1 | awk '{print $2}'"
                    )[0][0].split('\n')[0]
            self.log.debug("machine: {} - core(s): {}\
            ".format(server.ip, output))
            for i in range(int(output)):
                grep_field = "rw_{}:magma".format(i)
                _res = self.get_magma_stats(
                    bucket, [server],
                    field_to_grep=grep_field)
                fragmentation_values.append(
                    float(_res[server.ip][grep_field][
                        "Fragmentation"]))
                stats.append(_res)
            result.update({server.ip: fragmentation_values})
        self.log.info("magma stats fragmentation result {} \
        ".format(result))
        for value in result.values():
            if max(value) > self.fragmentation:
                self.log.info(stats)
                return False
        return True

    def check_fragmentation_using_bucket_stats(self, bucket, servers=None):
        # Disabling the check for time being
        return True
        result = dict()
        if servers is None:
            servers = self.cluster.nodes_in_cluster
        if type(servers) is not list:
            servers = [servers]
        for server in servers:
            frag_val = self.bucket_util.get_fragmentation_kv(
                bucket, server)
            self.log.debug("Current Fragmentation for node {} is {} \
            ".format(server.ip, frag_val))
            result.update({server.ip: frag_val})
        self.log.info("KV stats fragmentation values {}".format(result))
        for value in result.values():
            if value > self.fragmentation:
                return False
        return True

    def genrate_docs_basic(self, start, end, mutate=0):
        return doc_generator(self.key, start, end,
                             doc_size=self.doc_size,
                             doc_type=self.doc_type,
                             target_vbucket=self.target_vbucket,
                             vbuckets=self.cluster_util.vbuckets,
                             key_size=self.key_size,
                             randomize_doc_size=self.randomize_doc_size,
                             randomize_value=self.randomize_value,
                             mix_key_size=self.mix_key_size,
                             mutate=mutate,
                             deep_copy=self.deep_copy)

    def generate_docs(self, doc_ops=None,
                      create_end=None, create_start=None,
                      create_mutate=0,
                      update_end=None, update_start=None,
                      update_mutate=0,
                      read_end=None, read_start=None,
                      read_mutate=0,
                      delete_end=None, delete_start=None):

        if doc_ops is None:
            doc_ops = self.doc_ops

        if "update" in doc_ops:
            if update_start is not None:
                self.update_start = update_start
            if update_end is not None:
                self.update_end = update_end

            self.mutate += 1
            self.gen_update = self.genrate_docs_basic(self.update_start,
                                                      self.update_end,
                                                      self.mutate)

        if "delete" in doc_ops:
            if delete_start is not None:
                self.delete_start = delete_start
            if delete_end is not None:
                self.delete_end = delete_end

            self.gen_delete = self.genrate_docs_basic(self.delete_start,
                                                      self.delete_end,
                                                      read_mutate)

        if "create" in doc_ops:
            if create_start is not None:
                self.create_start = create_start
            if create_end is not None:
                self.create_end = create_end

            self.gen_create = self.genrate_docs_basic(self.create_start,
                                                      self.create_end,
                                                      create_mutate)

        if "read" in doc_ops:
            if read_start is not None:
                self.read_start = read_start
            if read_end is not None:
                self.read_end = read_end

            self.gen_read = self.genrate_docs_basic(self.read_start,
                                                    self.read_end)

    def get_fragmentation_upsert_docs_list(self):
        """
         This function gives the list of "number of docs" need
         to be updated to touch the given fragmentation value
        """
        update_doc_count = int(math.ceil(float(
                    self.fragmentation * self.num_items) / (
                        100 - self.fragmentation)))

        upsert_doc_list = list()
        while update_doc_count > self.num_items:
            upsert_doc_list.append(self.num_items)
            update_doc_count -= self.num_items
        if update_doc_count > 0:
            upsert_doc_list.append(update_doc_count)
        self.log.info("Upsert list {}".format(upsert_doc_list))
        return upsert_doc_list

    def validate_data(self,  op_type, kv_gen):
        self.log.info("Validating Docs")
        for bucket in self.bucket_util.buckets:
            task = self.task.async_validate_docs(
                    self.cluster, bucket, kv_gen, op_type, 0,
                    batch_size=self.batch_size,
                    process_concurrency=self.process_concurrency,
                    pause_secs=5, timeout_secs=self.sdk_timeout)

        self.task.jython_task_manager.get_task_result(task)

    def sigkill_memcached(self, nodes=None, graceful=False):
        if not nodes:
            nodes = self.cluster.nodes_in_cluster
            for node in nodes:
                shell = RemoteMachineShellConnection(node)
                if graceful:
                    shell.restart_couchbase()
                else:
                    shell.kill_memcached()
                shell.disconnect()
                self.assertTrue(self.bucket_util._wait_warmup_completed(
                    [self.cluster_util.cluster.master],
                    self.bucket_util.buckets[0],
                    wait_time=self.wait_timeout * 20))

    def abort_tasks_after_crash(self):
        self.stop = False

        while not self.stop:
            self.log.info("Crash check")
            crashes = self.check_coredump_exist(self.cluster.nodes_in_cluster)

            if len(crashes) > 0:
                self.stop = True
                self.task.jython_task_manager.abort_all_tasks()
                self.assertEqual(len(crashes), 0,
                                 msg="Coredump found on servers {}"
                                 .format(crashes))
