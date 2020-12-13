from Cb_constants.CBServer import CbServer
from couchbase_helper.documentgenerator import doc_generator
from magma_base import MagmaBaseTest
from remote.remote_util import RemoteMachineShellConnection
from sdk_exceptions import SDKException
from BucketLib.bucket import Bucket

class KVStoreTests(MagmaBaseTest):
    def setUp(self):
        super(KVStoreTests, self).setUp()

    def tearDown(self):
        super(KVStoreTests, self).tearDown()

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
                durability=self.durability_level, pause_secs=5,
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
                durability=self.durability_level, pause_secs=5,
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
                durability=self.durability_level, pause_secs=5,
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
               pause_secs=5, timeout_secs=self.sdk_timeout,
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
                durability=self.durability_level, pause_secs=5,
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

    def compute_docs_ranges(self, start):
        self.divisor = self.input.param("divisor", 2)
        ops_len = len(self.doc_ops.split(":"))

        self.create_start = start
        self.create_end = start + start // self.divisor

        if "create" in self.doc_ops:
            self.create_end = start + start // self.divisor

        if ops_len == 1:
            self.update_start = 0
            self.update_end = start
            self.expiry_start = 0
            self.expiry_end = start
            self.delete_start = 0
            self.delete_end = start
        elif ops_len == 2:
            self.update_start = 0
            self.update_end = start // 2
            self.delete_start = start // 2
            self.delete_end = start

            if "expiry" in self.doc_ops:
                self.delete_start = 0
                self.delete_end = start // 2
                self.expiry_start = start // 2
                self.expiry_end = start
        else:
            self.update_start = 0
            self.update_end = start // 3
            self.delete_start = start // 3
            self.delete_end = (2 * start) // 3
            self.expiry_start = (2 * start) // 3
            self.expiry_end = start

    def test_create_delete_bucket_n_times(self):
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
        for bucket in self.bucket_util.buckets:
            bucket_lst.append((bucket, bucket.name))
        bucket_lst = sorted(bucket_lst, key = lambda x : x[-1])
        self.log.debug ("bucket list is {} ".format(bucket_lst))
        bucket_ram_quota = bucket_lst[0][0].ramQuotaMB
        self.log.debug("ram_quota is {}".format(bucket_ram_quota))

        scope_name = CbServer.default_scope
        start = self.init_items_per_collection

        while count < self.test_itr:
            self.log.info("Iteration=={}".format(count+1))
            '''
            Step 1
              -- Doc loading to buckets, which will not be getting deleted
              -- This step is optional.
              -- In case we pass parameter to delete all buckets.
                  it will be skipped
            '''
            if self.num_delete_buckets <  self.standard_buckets:
                self.compute_docs_ranges(start)
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
             -- Deletion of buckets
            '''

            for bucket, _ in bucket_lst[:self.num_delete_buckets]:
                self.log.info("Iteration=={}, Deleting bucket=={}".format(count+1, bucket.name))
                self.bucket_util.delete_bucket(self.cluster.master, bucket)
                self.sleep(30, "waiting for 30 seconds after deletion of bucket")

            if self.num_delete_buckets <  self.standard_buckets:
                for task in tasks_info:
                    self.task_manager.get_task_result(task)
                if "create" in self.doc_ops:
                    start = self.create_end
                    self.log.info("Iteration=={}, start=={}".format(count+1, start))
            '''
            Step 3
            -- Recreate docs which got deleted/expired
            -- This step will also be optional
            '''

            self.gen_create = None
            self.gen_update = None

            if "delete" in self.doc_ops and "expriy" in self.doc_ops:
                self.create_start = self.delete_start
                self.create_end = self.delete_end
                self.update_start = self.expiry_start
                self.update_end = self.expiry_end
                self.generate_docs(doc_ops="create:update", target_vbucket=None)
            elif "expiry" in self.doc_ops:
                self.update_start = self.expiry_start
                self.update_end = self.expiry_end
                self.generate_docs(doc_ops="update", target_vbucket=None)
            elif "delete" in self.doc_ops:
                self.create_start = self.delete_start
                self.create_end = self.delete_end
                self.generate_docs(doc_ops="create", target_vbucket=None)

            if self.gen_create is not None or self.gen_update is not None:
                tasks_in = dict()
                for bucket in self.bucket_util.buckets:
                    self.log.debug("Iteration=={}, Loading earlier deleted/expirted items in bucket=={}"
                                   .format(count+1, bucket.name))
                    for collection in self.collections:
                        tem_tasks_in = self.loadgen_docs_per_bucket(bucket, self.retry_exceptions,
                                                                      self.ignore_exceptions,
                                                                      scope=scope_name,
                                                                      collection=collection,
                                                                      _sync=False,
                                                                      doc_ops="create:update")
                    tasks_in.update(tem_tasks_in.items())
                for task in tasks_in:
                    self.task_manager.get_task_result(task)

            '''
            Step 4
            -- Bucket recreation steps
            '''
            buckets_created = self.bucket_util.create_multiple_buckets(
            self.cluster.master,
            self.num_replicas,
            bucket_count=self.num_delete_buckets,
            bucket_type=self.bucket_type,
            storage={"couchstore": 0,
                     "magma": self.num_delete_buckets},
            eviction_policy=self.bucket_eviction_policy,
            ram_quota=bucket_ram_quota)
            self.assertTrue(buckets_created, "Unable to create multiple buckets after bucket deletion")
            for bucket in self.bucket_util.buckets:
                ready = self.bucket_util.wait_for_memcached(
                    self.cluster.master,
                    bucket)
                self.assertTrue(ready, msg="Wait_for_memcached failed")

            task_info = dict()
            bucket_lst = []
            for bucket in self.bucket_util.buckets:
                bucket_lst.append((bucket, bucket.name))
                bucket_lst = sorted(bucket_lst, key = lambda x : x[-1])
            self.log.debug("Iteration=={}, Bucket list after recreation of bucket =={} ".
                           format(count+1, bucket_lst))

            '''
            Step 5
            -- Doc loading in recreated buckets
            '''
            self.generate_docs(create_end=self.init_items_per_collection,
                               create_start=0,
                               doc_ops="create",
                               target_vbucket=None)
            for bucket, _ in bucket_lst[:self.num_delete_buckets]:
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
