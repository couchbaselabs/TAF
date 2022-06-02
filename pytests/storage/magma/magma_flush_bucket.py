import copy
import threading
import random
import time

from Cb_constants.CBServer import CbServer
from cb_tools.cbepctl import Cbepctl
from cb_tools.cbstats import Cbstats
from magma_base import MagmaBaseTest
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient
from sdk_constants.java_client import SDKConstants
from sdk_exceptions import SDKException


class MagmaFlushBucketTests(MagmaBaseTest):
    def setUp(self):
        super(MagmaFlushBucketTests, self).setUp()
        self.sdk_timeout = self.input.param("sdk_timeout", 100)
        self.assertTrue(self.rest.update_autofailover_settings(False, 600),
                        "AutoFailover disabling failed")
        self.sigkill = self.input.param("sigkill", False)
        self.multiplier = self.input.param("multiplier", 2)
        self.flush_th = None
        self.shell_conn = list()
        for node in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(node)
            self.shell_conn.append(shell)

    def tearDown(self):
        self.stop_flush = True
        if self.flush_th and self.flush_th.is_alive():
            self.flush_th.join()
        super(MagmaFlushBucketTests, self).tearDown()

    def bucket_flush(self, sigkill=False, wait=False):
        self.stop_flush = False
        flush_iteration = 1
        while not self.stop_flush:
            for bucket in self.buckets:
                result = self.bucket_util.flush_bucket(self.cluster, bucket)
                if sigkill and result:
                    for shell in self.shell_conn:
                        shell.kill_memcached()
            if wait:
                for node in self.cluster.nodes_in_cluster:
                    if "kv" in node.services:
                        result = self.bucket_util._wait_warmup_completed(
                                    [node],
                                    self.cluster.buckets[0],
                                    wait_time=self.wait_timeout * 5)
                        if not result:
                            msg = "warm-up couldn't complete in %s seconds" %\
                                (self.wait_timeout * 5)
                            self.log.critical(msg)
                            self.task.jython_task_manager.abort_all_tasks()
                            self.stop_flush = True
            sleep = random.randint(30, 60)
            self.sleep(sleep, "Iteration:{} done,  waiting for {} sec after bucket flush".
                       format(flush_iteration, sleep))
            flush_iteration += 1

    def loadgen_docs_per_bucket(self, bucket,
                     retry_exceptions=[],
                     ignore_exceptions=[],
                     skip_read_on_error=False,
                     suppress_error_table=False,
                     scope=CbServer.default_scope,
                     collection=CbServer.default_collection,
                     _sync=True,
                     track_failures=True,
                     doc_ops=None):
        doc_ops = doc_ops or self.doc_ops

        tasks_info = dict()
        tem_tasks_info = dict()
        read_tasks_info = dict()
        read_task = False

        if self.check_temporary_failure_exception:
            retry_exceptions.append(SDKException.TemporaryFailureException)

        if "update" in doc_ops and self.gen_update is not None:
            task = self.bucket_util.async_load_bucket(
                self.cluster, bucket, self.gen_update,  "update", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level,
                sdk_timeout=self.sdk_timeout, retries=self.sdk_retries,
                skip_read_on_error=skip_read_on_error,
                suppress_error_table=suppress_error_table,
                scope=scope,
                collection=collection,
                monitor_stats=self.monitor_stats,
                track_failures=track_failures)
            tem_tasks_info[task] = self.bucket_util.get_doc_op_info_dict(
                bucket, "update", 0,
                scope=scope,
                collection=collection,
                replicate_to=self.replicate_to,
                persist_to=self.persist_to,
                durability=self.durability_level,
                timeout=self.sdk_timeout, time_unit="seconds",
                ignore_exceptions=ignore_exceptions,
                retry_exceptions=retry_exceptions)
            tasks_info.update(tem_tasks_info.items())
        if "create" in doc_ops and self.gen_create is not None:
            task = self.bucket_util.async_load_bucket(
                self.cluster, bucket, self.gen_create, "create", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level,
                sdk_timeout=self.sdk_timeout, retries=self.sdk_retries,
                skip_read_on_error=skip_read_on_error,
                suppress_error_table=suppress_error_table,
                scope=scope,
                collection=collection,
                monitor_stats=self.monitor_stats,
                track_failures=track_failures)
            tem_tasks_info[task] = self.bucket_util.get_doc_op_info_dict(
                bucket, "create", 0,
                scope=scope,
                collection=collection,
                replicate_to=self.replicate_to,
                persist_to=self.persist_to,
                durability=self.durability_level,
                timeout=self.sdk_timeout, time_unit="seconds",
                ignore_exceptions=ignore_exceptions,
                retry_exceptions=retry_exceptions)
            tasks_info.update(tem_tasks_info.items())
            self.num_items += (self.gen_create.end - self.gen_create.start)
        if "expiry" in doc_ops and self.gen_expiry is not None and self.maxttl:
            task = self.bucket_util.async_load_bucket(
                self.cluster, bucket, self.gen_expiry, "update", self.maxttl,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level,
                sdk_timeout=self.sdk_timeout, retries=self.sdk_retries,
                skip_read_on_error=skip_read_on_error,
                suppress_error_table=suppress_error_table,
                scope=scope,
                collection=collection,
                monitor_stats=self.monitor_stats,
                track_failures=track_failures)
            tem_tasks_info[task] = self.bucket_util.get_doc_op_info_dict(
                bucket, "update", 0,
                scope=scope,
                collection=collection,
                replicate_to=self.replicate_to,
                persist_to=self.persist_to,
                durability=self.durability_level,
                timeout=self.sdk_timeout, time_unit="seconds",
                ignore_exceptions=ignore_exceptions,
                retry_exceptions=retry_exceptions)
            tasks_info.update(tem_tasks_info.items())
            self.num_items -= (self.gen_expiry.end - self.gen_expiry.start)
        if "read" in doc_ops and self.gen_read is not None:
            read_tasks_info = self.bucket_util.async_validate_docs(
               self.cluster, bucket, self.gen_read, "read", 0,
               batch_size=self.batch_size,
               process_concurrency=self.process_concurrency,
               timeout_secs=self.sdk_timeout,
               retry_exceptions=retry_exceptions,
               ignore_exceptions=ignore_exceptions,
               scope=scope,
               collection=collection)
            read_task = True
        if "delete" in doc_ops and self.gen_delete is not None:
            task = self.bucket_util.async_load_bucket(
                self.cluster, bucket, self.gen_delete, "delete", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level,
                sdk_timeout=self.sdk_timeout, retries=self.sdk_retries,
                skip_read_on_error=skip_read_on_error,
                suppress_error_table=suppress_error_table,
                scope=scope,
                collection=collection,
                monitor_stats=self.monitor_stats,
                track_failures=track_failures)
            tem_tasks_info[task] = self.bucket_util.get_doc_op_info_dict(
                bucket, "delete", 0,
                scope=scope,
                collection=collection,
                replicate_to=self.replicate_to,
                persist_to=self.persist_to,
                durability=self.durability_level,
                timeout=self.sdk_timeout, time_unit="seconds",
                ignore_exceptions=ignore_exceptions,
                retry_exceptions=retry_exceptions)
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

    def compute_docs(self, start, mem_only_items, doc_ops=None):
        doc_ops = doc_ops or self.doc_ops
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

    def test_flush_bucket_during_creates(self):

        self.log.info("====test_flush_bucket_during_creates starts====")
        self.create_start = self.init_items_per_collection
        self.create_end = self.init_items_per_collection * self.multiplier
        self.generate_docs(doc_ops=self.doc_ops, target_vbucket=None)

        tasks_info = dict()
        for scope in self.scopes:
            for collection in self.collections:
                task_info = self.loadgen_docs(
                self.retry_exceptions,
                self.ignore_exceptions,
                scope=scope,
                collection=collection,
                suppress_error_table=True,
                skip_read_on_error=True,
                _sync=False,
                doc_ops=self.doc_ops,
                track_failures=False,
                sdk_retry_strategy=SDKConstants.RetryStrategy.FAIL_FAST)
            tasks_info.update(task_info.items())

        self.flush_th = threading.Thread(target=self.bucket_flush,
                                         kwargs=dict(sigkill=self.sigkill))
        self.flush_th.start()
        for task in tasks_info:
            self.task_manager.get_task_result(task)

        self.stop_flush = True
        self.flush_th.join()

    def test_create_fragmentation_before_flush_bucket(self):
        self.log.info("====test_create_fragmentation_before_flush_bucket starts====")
        self.doc_ops = "update"
        self.num_threads = self.input.param("num_threads", 100)
        count = 0

        self.client = SDKClient([self.cluster.master],
                                self.cluster.buckets[0],
                                scope=CbServer.default_scope,
                                collection=CbServer.default_collection)

        self.gen_update = self.genrate_docs_basic(start=0, end=1)

        key, val = self.gen_update.next()

        def upsert_doc(start_num, end_num, key_obj, val_obj):
            print threading.currentThread().getName(), 'Starting'
            for i in range(start_num, end_num):
                val_obj.put("mutated", i)
                self.client.upsert(key_obj, val_obj)
            if threading.currentThread().getName() == 't'+str(self.num_threads):
                self.bucket_util.flush_bucket(self.cluster, self.cluster.buckets[0])

            print threading.currentThread().getName(), 'Exiting'

        while count < self.test_itr:
            self.log.info("Iteration : {}".format(count))
            self.generate_docs(create_start =0, create_end =self.init_items_per_collection,
                               doc_ops="create")
            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  doc_ops="create",
                                  _sync=True)
            threads = []
            start = 0
            end = 0
            for i in range(self.num_threads+1):
                start = end
                end += self.init_items_per_collection
                th = threading.Thread(name='t'+str(i),
                                      target=upsert_doc, args=[start, end, key, val])
                th.start()
                threads.append(th)

            for th in threads:
                th.join()

            count +=1

    def test_flush_bucket_during_rollback(self):
        '''
        Test focus: Stopping persistence one by one on all nodes,
                    and trigger roll back on other  nodes,
                    During rollback flush the data
                    Above step will be done num_rollback
                    (variable defined in test) times

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
        self.assertTrue(self.rest.update_autofailover_settings(False, 600),
                        "AutoFailover disabling failed")
        items = copy.deepcopy(self.init_items_per_collection)
        mem_only_items = self.input.param("rollback_items", 10000)

        ops_len = len(self.doc_ops.split(":"))

        if self.nodes_init < 2 or self.num_replicas < 1:
            self.fail("Not enough nodes/replicas in the cluster/bucket \
            to test rollback")

        self.duration = self.input.param("duration", 2)
        self.num_rollbacks = self.input.param("num_rollbacks", 3)

        #######################################################################
        '''
        STEP - 1, Ensures creation of at least one snapshot

        To ensure at least one snapshot should get created before rollback
        starts, we need to sleep for 60 seconds as per magma design which
        create state file every 60s

        '''
        self.sleep(60, "Ensures creation of at least one snapshot")
        #######################################################################
        '''
        STEP - 2,  Stop persistence on node - x
        '''

        for i in range(1, self.num_rollbacks+1):
            self.log.info("Roll back Iteration == {}".format(i))
            start = items
            for x, node in enumerate(self.cluster.nodes_in_cluster):
                shell = RemoteMachineShellConnection(node)
                cbstats = Cbstats(node)
                self.target_vbucket = cbstats.vbucket_list(self.cluster.buckets[0].
                                                   name)
                mem_item_count = 0
                # Stopping persistence on Node-x
                self.log.debug("Iteration == {}, Stopping persistence on Node-{}, ip ={}"
                               .format(i, x+1, node))
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
                time_end = time.time() + 60 * self.duration
                itr = 0
                while time.time() < time_end:
                    itr += 1
                    time_start = time.time()
                    mem_item_count += mem_only_items * ops_len
                    self.generate_docs(doc_ops=self.doc_ops,
                                       target_vbucket=self.target_vbucket)
                    self.loadgen_docs(_sync=True,
                                      retry_exceptions=self.retry_exceptions)
                    if self.gen_create is not None:
                        self.create_start = self.gen_create.key_counter
                    if self.gen_update is not None:
                        self.update_start = self.gen_update.key_counter
                    if self.gen_delete is not None:
                        self.delete_start = self.gen_delete.key_counter
                    if self.gen_expiry is not None:
                        self.expiry_start = self.gen_expiry.key_counter

                    if time.time() < time_start + 60:
                        self.log.info("Rollback Iteration== {}, itr== {}, Active-Node=={}, Node=={}".format(i, itr, x+1, node))
                        self.sleep(time_start + 60 - time.time(),
                                   "Sleep to ensure creation of state files for roll back")
                        self.log.info("state files == {}".format(
                                     self.get_state_files(self.buckets[0])))
                ep_queue_size_map = {node:
                                     mem_item_count}
                if self.durability_level:
                    self.log.info("updating the num_items on disk check to double due to durability")
                    ep_queue_size_map = {node:
                                     mem_item_count * 2}
                vb_replica_queue_size_map = {node: 0}

                for nod in self.cluster.nodes_in_cluster:
                    if nod != node:
                        ep_queue_size_map.update({nod: 0})
                        vb_replica_queue_size_map.update({nod: 0})

                for bucket in self.cluster.buckets:
                    self.bucket_util._wait_for_stat(bucket, ep_queue_size_map,
                                                    timeout=1200)
                    self.bucket_util._wait_for_stat(bucket, vb_replica_queue_size_map,
                                                    cbstat_cmd="all",
                                                    stat_name="vb_replica_queue_size",
                                                    timeout=1200)
                # replica vBuckets
                for bucket in self.cluster.buckets:
                    self.log.debug(cbstats.failover_stats(bucket.name))

                ###############################################################
                '''
                STEP - 4
                  -- Kill Memcached on Node - x and trigger rollback on other nodes
                  -- After 20 seconds , flush bucket
                '''

                shell.kill_memcached()
                self.sleep(20, "sleep after killing memcached")
                self.bucket_util.flush_bucket(self.cluster, self.cluster.buckets[0])
                ###############################################################
                '''
                STEP -5
                 -- Restarting persistence on Node -- x
                '''
                self.assertTrue(self.bucket_util._wait_warmup_completed(
                    [self.cluster.master],
                    self.cluster.buckets[0],
                    wait_time=self.wait_timeout * 10))

                self.log.debug("Iteration=={}, Re-Starting persistence on Node -- {}".format(i, node))
                Cbepctl(shell).persistence(self.cluster.buckets[0].name, "start")

                self.sleep(5, "Sleep after re-starting persistence, Iteration{}".format(i))
                shell.disconnect()
                ###################################################################
                '''
                STEP - 6
                  -- Load Docs on all the nodes
                  -- Loading of doc for 60 seconds
                  -- Ensures creation of new state file
                '''
                self.create_start = 0
                self.create_end = self.init_items_per_collection
                self.generate_docs(doc_ops="create", target_vbucket=None)
                self.loadgen_docs(self.retry_exceptions,
                              self.ignore_exceptions, _sync=True,
                              doc_ops="create")
                self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                             self.cluster.buckets,
                                                             timeout=1200)

    def test_flush_bucket_during_create_delete_kvstores(self):
        """
        Test Focus: Create and Delete bucket multiple times.

                    Since buckets are already created in magma base
                    we'll start by deleting the buckets, then will recreate

        STEPS:
             -- Doc ops on bucket which we'll not be deleting(optional)
             -- Delete already exisiting buckets
             -- Recreate new buckets
             -- Doc ops on buckets
             -- Repaeat all the above steps
        """
        self.log.info("=====test_create_delete_bucket_n_times starts=====")
        count = 0
        self.num_delete_buckets = self.input.param("num_delete_buckets", 1)
        '''
        Sorting bucket list
        '''
        bucket_lst = []
        for bucket in self.cluster.buckets:
            bucket_lst.append((bucket, bucket.name))
        bucket_lst = sorted(bucket_lst, key = lambda x : x[-1])
        self.log.debug ("bucket list is {} ".format(bucket_lst))
        bucket_ram_quota = bucket_lst[0][0].ramQuotaMB
        self.log.debug("ram_quota is {}".format(bucket_ram_quota))

        scope_name = CbServer.default_scope

        while count < self.test_itr:
            self.log.info("Iteration=={}".format(count+1))
            start = self.init_items_per_collection
            '''
            Step 1
              -- Doc loading to buckets, which will not be getting deleted
            '''
            self.compute_docs(start, start//2)
            self.generate_docs(doc_ops=self.doc_ops, target_vbucket=None)
            tasks_info = dict()
            for bucket, _ in bucket_lst[self.num_delete_buckets:]:
                self.log.debug("Iteration=={}, Bucket=={}".format(count+1, bucket.name))
                for collection in self.collections:
                    tem_tasks_info = self.loadgen_docs_per_bucket(bucket, self.retry_exceptions,
                                                                      self.ignore_exceptions,
                                                                      scope=scope_name,
                                                                      collection=collection,
                                                                      _sync=False,
                                                                      doc_ops=self.doc_ops)
                    tasks_info.update(tem_tasks_info.items())
            '''
            Step 2
             -- Start flushing the buckets
             -- Deletion of buckets
            '''
            self.flush_th = threading.Thread(target=self.bucket_flush,
                                         kwargs=dict(sigkill=self.sigkill))
            self.flush_th.start()

            for bucket, _ in bucket_lst[:self.num_delete_buckets]:
                self.log.info("Iteration=={}, Deleting bucket=={}".format(count+1, bucket.name))
                self.bucket_util.delete_bucket(self.cluster, bucket)
                self.sleep(30, "waiting for 30 seconds after deletion of bucket")

            for task in tasks_info:
                self.task_manager.get_task_result(task)

            self.stop_flush = True
            self.flush_th.join()

            '''
            Step 4
            -- Loading to buckets which are not deleted
            -- Bucket recreation steps
            '''
            self.log.debug("Doc loading after flush")
            start = 0
            self.compute_docs(start, self.init_items_per_collection, doc_ops="create")
            self.generate_docs(doc_ops="create", target_vbucket=None)
            tasks_info = dict()
            for bucket, _ in bucket_lst[self.num_delete_buckets:]:
                self.log.debug("Loading after flush, Iteration=={}, Bucket=={}".
                               format(count+1, bucket.name))
                for collection in self.collections:
                    tem_tasks_info = self.loadgen_docs_per_bucket(bucket, self.retry_exceptions,
                                                                      self.ignore_exceptions,
                                                                      scope=scope_name,
                                                                      collection=collection,
                                                                      _sync=False,
                                                                      doc_ops="create")
                    tasks_info.update(tem_tasks_info.items())
            self.sleep(30, "Ensuring doc loading for 30 seconds")

            self.flush_th = threading.Thread(target=self.bucket_flush,
                                         kwargs=dict(sigkill=self.sigkill))
            self.flush_th.start()
            buckets_created = self.bucket_util.create_multiple_buckets(
                self.cluster, self.num_replicas,
                bucket_count=self.num_delete_buckets,
                bucket_type=self.bucket_type,
                storage={"couchstore": 0,
                         "magma": self.num_delete_buckets},
                eviction_policy=self.bucket_eviction_policy,
                ram_quota=bucket_ram_quota,
                bucket_name=self.bucket_name)

            self.stop_flush = True
            self.flush_th.join()

            if not buckets_created:
                buckets_created = self.bucket_util.create_multiple_buckets(
                    self.cluster, self.num_replicas,
                    bucket_count=self.num_delete_buckets,
                    bucket_type=self.bucket_type,
                    storage={"couchstore": 0,
                             "magma": self.num_delete_buckets},
                    eviction_policy=self.bucket_eviction_policy,
                    ram_quota=bucket_ram_quota,
                    bucket_name=self.bucket_name,
                    flush_enabled=1)

            self.assertTrue(buckets_created, "Unable to create multiple buckets after bucket deletion")

            for bucket in self.cluster.buckets:
                ready = self.bucket_util.wait_for_memcached(
                    self.cluster.master,
                    bucket)
                self.assertTrue(ready, msg="Wait_for_memcached failed")

            task_info = dict()
            bucket_lst = []
            for bucket in self.cluster.buckets:
                bucket_lst.append((bucket, bucket.name))
                bucket_lst = sorted(bucket_lst, key = lambda x : x[-1])
            self.log.debug("Iteration=={}, Bucket list after recreation of bucket =={} ".
                           format(count+1, bucket_lst))

            '''
            Step 5
            -- Doc loading in all buckets
            '''
            self.generate_docs(create_end=self.init_items_per_collection,
                               create_start=0,
                               doc_ops="create",
                               target_vbucket=None)
            for bucket, _ in bucket_lst:
                self.log.info("Iteration=={}, doc loading  to bucket=={}".format(count+1, bucket.name))
                for collection in self.collections:
                    tem_task_info = self.loadgen_docs_per_bucket(bucket, self.retry_exceptions,
                                                                 self.ignore_exceptions,
                                                                 scope=scope_name,
                                                                 collection=collection,
                                                                 _sync=False,
                                                                 doc_ops="create")
                    task_info.update(tem_task_info.items())

            for task in task_info:
                self.task_manager.get_task_result(task)
            count += 1

    def test_flush_bucket_during_data_persistence(self):
        self.assertTrue(self.rest.update_autofailover_settings(False, 600),
                        "AutoFailover disabling failed")
        count = 0
        start = copy.deepcopy(self.init_items_per_collection)
        while count < self.test_itr:
            self.log.info("Iteration {}".format(count+1))
            self.compute_docs(start, start)
            for shell in self.shell_conn:
                for bucket in self.cluster.buckets:
                    Cbepctl(shell).persistence(bucket.name, "stop")
            self.generate_docs()
            tasks_info = dict()
            for scope in self.scopes:
                for collection in self.collections:
                    task_info = self.loadgen_docs(
                        self.retry_exceptions,
                        self.ignore_exceptions,
                        scope=scope,
                        collection=collection,
                        suppress_error_table=True,
                        skip_read_on_error=True,
                        _sync=False,
                        doc_ops=self.doc_ops,
                        track_failures=False,
                        sdk_retry_strategy=SDKConstants.RetryStrategy.FAIL_FAST)
                    tasks_info.update(task_info.items())
            for task in tasks_info:
                self.task_manager.get_task_result(task)
            for shell in self.shell_conn:
                for bucket in self.cluster.buckets:
                    Cbepctl(shell).persistence(bucket.name, "start")
            self.sleep(10, "sleep before flush thread")
            for bucket in self.buckets:
                self.bucket_util.flush_bucket(self.cluster, bucket)
            count += 1
