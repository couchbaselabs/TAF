import copy
import math

from couchbase_helper.documentgenerator import doc_generator
from magma_base import MagmaBaseTest
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient
from Cb_constants.CBServer import CbServer
from com.couchbase.client.java.kv import GetAllReplicasOptions,\
    GetAnyReplicaOptions
from com.couchbase.client.core.error import DocumentUnretrievableException


class BasicCrudTests(MagmaBaseTest):
    def setUp(self):
        super(BasicCrudTests, self).setUp()
        start = 0
        end = self.num_items
        start_read = 0
        end_read = self.num_items
        if self.rev_write:
            start = -int(self.num_items - 1)
            end = 1
        if self.rev_read:
            start = -int(self.num_items - 1)
            end = 1
        self.gen_create = doc_generator(
            self.key, start, end,
            doc_size=self.doc_size,
            doc_type=self.doc_type,
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
        self.disk_usage = dict()
        if self.standard_buckets == 1 or self.standard_buckets == self.magma_buckets:
            for bucket in self.bucket_util.get_all_buckets():
                disk_usage = self.get_disk_usage(
                    bucket, self.servers)
                self.disk_usage[bucket.name] = disk_usage[0]
                self.log.info(
                    "For bucket {} disk usage after initial creation is {}MB\
                    ".format(bucket.name,
                        self.disk_usage[bucket.name]))
        self.gen_read = doc_generator(
            self.key, start, end,
            doc_size=self.doc_size,
            doc_type=self.doc_type,
            target_vbucket=self.target_vbucket,
            vbuckets=self.cluster_util.vbuckets,
            key_size=self.key_size,
            randomize_doc_size=self.randomize_doc_size,
            randomize_value=self.randomize_value,
            mix_key_size=self.mix_key_size,
            deep_copy=self.deep_copy)
        self.cluster_util.print_cluster_stats()
        self.bucket_util.print_bucket_stats()

    def tearDown(self):
        super(BasicCrudTests, self).tearDown()

    def test_expiry(self):
        result = True
        self.gen_create = doc_generator(
            self.key, 0, 10,
            doc_size=20,
            doc_type=self.doc_type,
            key_size=self.key_size)

        tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, self.gen_create, "create", 10,
                batch_size=10,
                process_concurrency=1,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level, pause_secs=5,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                )
        self.task.jython_task_manager.get_task_result(tasks_info.keys()[0])
        self.sleep(20)
        self.client = SDKClient([self.cluster.master],
                                self.bucket_util.buckets[0],
                                scope=CbServer.default_scope,
                                collection=CbServer.default_collection)
        for i in range(10):
            key = (self.key + "-" + str(i).zfill(self.key_size-len(self.key)))
            try:
                getReplicaResult = self.client.collection.getAnyReplica(
                    key, GetAnyReplicaOptions.getAnyReplicaOptions())
                if getReplicaResult:
                    result = False
                    try:
                        self.log.info("Able to retreive: %s" %
                                      {"key": key,
                                       "value": getReplicaResult.contentAsObject(),
                                       "cas": getReplicaResult.cas()})
                    except Exception as e:
                        print str(e)
            except DocumentUnretrievableException as e:
                pass
            if len(self.client.getFromAllReplica(key)) > 0:
                result = False
        self.client.close()
        self.assertTrue(result, "SDK is able to retrieve expired documents")

    def test_basic_create_read(self):
        """
        Write and Read docs parallely , While reading we are using
        old doc generator (self.gen_create)
        using which we already created docs in magam_base
        for writing we are creating a new doc generator.
        Befor we start read, killing memcached to make sure,
        all reads happen from magma/storage
        """
        self.log.info("Loading and Reading docs parallel")
        count = 0
        init_items = self.num_items
        while count < self.test_itr:
            for node in self.cluster.nodes_in_cluster:
                shell = RemoteMachineShellConnection(node)
                shell.kill_memcached()
                shell.disconnect()
            self.doc_ops = "create:read"
            start = self.num_items
            end = self.num_items+init_items
            start_read = self.num_items
            end_read = self.num_items+init_items
            if self.rev_write:
                start = -int(self.num_items+init_items - 1)
                end = -int(self.num_items - 1)
            if self.rev_read:
                start_read = -int(self.num_items+init_items - 1)
                end_read = -int(self.num_items - 1)
            self.gen_create = doc_generator(
                self.key, start, end,
                doc_size=self.doc_size,
                doc_type=self.doc_type,
                target_vbucket=self.target_vbucket,
                vbuckets=self.cluster_util.vbuckets,
                key_size=self.key_size,
                randomize_doc_size=self.randomize_doc_size,
                randomize_value=self.randomize_value,
                mix_key_size=self.mix_key_size,
                deep_copy=self.deep_copy)
            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=True)
            self.log.info("Verifying doc counts after create doc_ops")
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(self.num_items)
            self.gen_read = doc_generator(
                self.key, start_read, end_read,
                doc_size=self.doc_size,
                doc_type=self.doc_type,
                target_vbucket=self.target_vbucket,
                vbuckets=self.cluster_util.vbuckets,
                key_size=self.key_size,
                randomize_doc_size=self.randomize_doc_size,
                randomize_value=self.randomize_value,
                mix_key_size=self.mix_key_size,
                deep_copy=self.deep_copy)
            if self.doc_size <= 32:
                for bucket in self.bucket_util.get_all_buckets():
                    disk_usage = self.get_disk_usage(
                        bucket, self.servers)
                    self.assertIs(
                        disk_usage[2] > disk_usage[3], True,
                        "For Bucket {} , Disk Usage for seqIndex \
                        After new Creates count {} \
                        exceeds keyIndex disk \
                        usage".format(bucket.name, count+1))
            if self.standard_buckets > 1 and self.standard_buckets == self.magma_buckets:
                disk_usage = dict()
                for bucket in self.bucket_util.get_all_buckets():
                    usage = self.get_disk_usage(
                        bucket, self.servers)
                    disk_usage[bucket.name] = usage[0]
                    self.assertTrue(
                        all([disk_usage[disk_usage.keys()[0]] == disk_usage[
                            key] for key in disk_usage.keys()]),
                        '''Disk Usage for magma buckets
                        is not equal for same number of docs ''')
            count += 1
        self.log.info("====test_basic_create_read ends====")

    def test_update_multi(self):
        """
        Update all the docs n times, and after each iteration
        check for space amplificationa and data validation
        """
        self.log.info("Updating half the docs multiple times")
        count = 0
        mutated = 1
        update_doc_count = int(
            math.ceil(
                float(
                    self.fragmentation * self.num_items) / (
                        100 - self.fragmentation)))
        self.log.info("Count of docs to be update is {}\
        ".format(update_doc_count))
        num_update = list()
        while update_doc_count > self.num_items:
            num_update.append(self.num_items)
            update_doc_count -= self.num_items
        if update_doc_count > 0:
            num_update.append(update_doc_count)
        while count < self.test_itr:
            for node in self.cluster.nodes_in_cluster:
                shell = RemoteMachineShellConnection(node)
                shell.kill_memcached()
                shell.disconnect()
                self.assertTrue(self.bucket_util._wait_warmup_completed(
                                [self.cluster_util.cluster.master],
                                self.bucket_util.buckets[0],
                                wait_time=self.wait_timeout * 10))
            self.log.debug("List of docs to be updated {}\
            ".format(num_update))
            for itr in num_update:
                self.doc_ops = "update"
                start = 0
                end = itr
                if self.rev_update:
                    start = -int(itr - 1)
                    end = 1
                self.gen_update = doc_generator(
                    self.key, start, end,
                    doc_size=self.doc_size,
                    doc_type=self.doc_type,
                    target_vbucket=self.target_vbucket,
                    vbuckets=self.cluster_util.vbuckets,
                    key_size=self.key_size,
                    mutate=mutated,
                    randomize_doc_size=self.randomize_doc_size,
                    randomize_value=self.randomize_value,
                    mix_key_size=self.mix_key_size,
                    deep_copy=self.deep_copy)
                mutated += 1
                _ = self.loadgen_docs(
                    self.retry_exceptions,
                    self.ignore_exceptions,
                    _sync=True)
                self.log.info("Waiting for ep-queues to get drained")
                self.bucket_util._wait_for_stats_all_buckets()
            disk_usage = self.get_disk_usage(
                self.bucket_util.get_all_buckets()[0],
                self.servers)
            _res = disk_usage[0]
            self.log.info("After count {} disk usage is {}\
            ".format(_res, count + 1))
            usage_factor = (
                (float(
                    self.num_items + sum(num_update)
                    ) / self.num_items) + 0.5)
            self.log.debug("Disk usage factor is {}".format(usage_factor))
            self.assertIs(
                _res > usage_factor * self.disk_usage[
                    self.disk_usage.keys()[0]],
                False, "Disk Usage {} After Update \
                Count {} exceeds Actual \
                disk usage {} by {} \
                times".format(
                    _res, count,
                    self.disk_usage[self.disk_usage.keys()[0]],
                    usage_factor))
            count += 1
        data_validation = self.task.async_validate_docs(
            self.cluster, self.bucket_util.buckets[0],
            self.gen_update, "update", 0,
            batch_size=self.batch_size,
            process_concurrency=self.process_concurrency,
            pause_secs=5, timeout_secs=self.sdk_timeout)
        self.task.jython_task_manager.get_task_result(data_validation)
        self.log.info("====test_update_multi ends====")

    def test_multi_update_delete(self):
        """
        Update all the docs 3 times, and after each iteration
        check for space amplificationa and data validation
        Delete half the docs, and recreate deleted docs again
        After delete and recreate check for space amplification
        and data validation
        """
        count = 0
        for i in range(self.test_itr):
            while count < self.update_itr:
                for node in self.cluster.nodes_in_cluster:
                    shell = RemoteMachineShellConnection(node)
                    shell.kill_memcached()
                    shell.disconnect()
                    self.assertTrue(self.bucket_util._wait_warmup_completed(
                                    [self.cluster_util.cluster.master],
                                    self.bucket_util.buckets[0],
                                    wait_time=self.wait_timeout * 10))
                self.doc_ops = "update"
                start = 0
                end = self.num_items
                if self.rev_update:
                    start = -int(self.num_items - 1)
                    end = 1
                self.gen_update = doc_generator(
                    self.key, start, end,
                    doc_size=self.doc_size,
                    doc_type=self.doc_type,
                    target_vbucket=self.target_vbucket,
                    vbuckets=self.cluster_util.vbuckets,
                    key_size=self.key_size,
                    mutate=count+1,
                    randomize_doc_size=self.randomize_doc_size,
                    randomize_value=self.randomize_value,
                    mix_key_size=self.mix_key_size,
                    deep_copy=self.deep_copy)
                _ = self.loadgen_docs(self.retry_exceptions,
                                      self.ignore_exceptions,
                                      _sync=True)
                self.log.info("Waiting for ep-queues to get drained")
                self.bucket_util._wait_for_stats_all_buckets()
                data_validation = self.task.async_validate_docs(
                    self.cluster, self.bucket_util.buckets[0],
                    self.gen_update, "update", 0,
                    batch_size=self.batch_size,
                    process_concurrency=self.process_concurrency,
                    pause_secs=5, timeout_secs=self.sdk_timeout)
                self.task.jython_task_manager.get_task_result(data_validation)
                disk_usage = self.get_disk_usage(
                    self.bucket_util.get_all_buckets()[0],
                    self.servers)
                _res = disk_usage[0] - disk_usage[1]
                self.log.info("disk usage after update count {}\
                is {}".format(count+1, _res))
                self.assertIs(
                    _res > 4 * self.disk_usage[self.disk_usage.keys()[0]],
                    False, "Disk Usage {} After \
                    Update Count {} exceeds Actual \
                    disk usage {} by four \
                    times".format(_res, count,
                                  self.disk_usage[self.disk_usage.keys()[0]]))
                count += 1
            self.update_itr += self.update_itr
            self.gen_update = doc_generator(
                self.key, self.num_items//2,
                self.num_items,
                doc_size=self.doc_size,
                doc_type=self.doc_type,
                target_vbucket=self.target_vbucket,
                vbuckets=self.cluster_util.vbuckets,
                key_size=self.key_size,
                mutate=count,
                randomize_doc_size=self.randomize_doc_size,
                randomize_value=self.randomize_value,
                mix_key_size=self.mix_key_size,
                deep_copy=self.deep_copy)
            start_del = 0
            end_del = self.num_items//2
            if self.rev_del:
                start_del = -int(self.num_items//2 - 1)
                end_del = 1
            self.gen_delete = doc_generator(
                self.key, start_del, end_del,
                doc_size=self.doc_size,
                doc_type=self.doc_type,
                target_vbucket=self.target_vbucket,
                vbuckets=self.cluster_util.vbuckets,
                key_size=self.key_size,
                randomize_doc_size=self.randomize_doc_size,
                randomize_value=self.randomize_value,
                mix_key_size=self.mix_key_size,
                deep_copy=self.deep_copy)
            self.log.info("Deleting num_items//2 docs")
            self.doc_ops = "delete"
            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=True)
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(self.num_items)
            disk_usage = self.get_disk_usage(
                self.bucket_util.get_all_buckets()[0],
                self.servers)
            _res = disk_usage[0] - disk_usage[1]
            self.log.info("disk usage after delete is {}".format(_res))
            self.assertIs(
                _res > 4 * self.disk_usage[
                    self.disk_usage.keys()[0]],
                False, "Disk Usage {} After \
                Delete count {} exceeds Actual \
                disk usage {} by four \
                times".format(_res, i+1,
                              self.disk_usage[self.disk_usage.keys()[0]]))
            self.gen_create = copy.deepcopy(self.gen_delete)
            self.log.info("Recreating num_items//2 docs")
            self.doc_ops = "create"
            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=True)
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(self.num_items)
            data_validation = []
            data_validation.extend([self.task.async_validate_docs(
                self.cluster, self.bucket_util.buckets[0],
                self.gen_update, "update", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                pause_secs=5,
                timeout_secs=self.sdk_timeout), self.task.async_validate_docs(
                self.cluster, self.bucket_util.buckets[0],
                self.gen_create, "create", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                pause_secs=5, timeout_secs=self.sdk_timeout)])
            for task in data_validation:
                self.task.jython_task_manager.get_task_result(task)
            disk_usage = self.get_disk_usage(
                self.bucket_util.get_all_buckets()[0],
                self.servers)
            _res = disk_usage[0] - disk_usage[1]
            self.log.info("disk usage after new create \
            is {}".format(_res))
            self.assertIs(
                _res > 4 * self.disk_usage[
                    self.disk_usage.keys()[0]],
                False, "Disk Usage {} After \
                new Creates count {} exceeds \
                Actual disk usage {} by \
                four times".format(_res, i+1,
                                   self.disk_usage[self.disk_usage.keys()[0]]))
        self.log.info("====test_multiUpdate_delete ends====")

    def test_update_rev_update(self):
        count = 0
        mutated = 1
        for i in range(self.test_itr):
            while count < self.update_itr:
                for node in self.cluster.nodes_in_cluster:
                    shell = RemoteMachineShellConnection(node)
                    shell.kill_memcached()
                    shell.disconnect()
                    self.assertTrue(self.bucket_util._wait_warmup_completed(
                                    [self.cluster_util.cluster.master],
                                    self.bucket_util.buckets[0],
                                    wait_time=self.wait_timeout * 10))
                tasks_info = dict()
                data_validation = []
                g_update = doc_generator(
                    self.key, 0, self.num_items//2,
                    doc_size=self.doc_size,
                    doc_type=self.doc_type,
                    target_vbucket=self.target_vbucket,
                    vbuckets=self.cluster_util.vbuckets,
                    key_size=self.key_size,
                    mutate=mutated,
                    randomize_doc_size=self.randomize_doc_size,
                    randomize_value=self.randomize_value,
                    mix_key_size=self.mix_key_size,
                    deep_copy=self.deep_copy)
                mutated += 1
                tem_tasks_info = self.bucket_util._async_load_all_buckets(
                    self.cluster, g_update, "update", 0,
                    batch_size=self.batch_size,
                    process_concurrency=self.process_concurrency,
                    persist_to=self.persist_to, replicate_to=self.replicate_to,
                    durability=self.durability_level, pause_secs=5,
                    timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                    retry_exceptions=self.retry_exceptions,
                    ignore_exceptions=self.ignore_exceptions)
                tasks_info.update(tem_tasks_info.items())
                start = - (self.num_items // 2 - 1)
                end = 1
                r_update = doc_generator(
                    self.key, start, end,
                    doc_size=self.doc_size,
                    doc_type=self.doc_type,
                    target_vbucket=self.target_vbucket,
                    vbuckets=self.cluster_util.vbuckets,
                    key_size=self.key_size,
                    mutate=mutated,
                    randomize_doc_size=self.randomize_doc_size,
                    randomize_value=self.randomize_value,
                    mix_key_size=self.mix_key_size,
                    deep_copy=self.deep_copy)
                mutated += 1
                if self.next_half:
                    mutated -= 2
                    start = - (self.num_items - 1)
                    end = - (self.num_items // 2 - 1)
                    r_update = doc_generator(
                        self.key, start, end,
                        doc_size=self.doc_size,
                        doc_type=self.doc_type,
                        target_vbucket=self.target_vbucket,
                        vbuckets=self.cluster_util.vbuckets,
                        key_size=self.key_size,
                        mutate=mutated,
                        randomize_doc_size=self.randomize_doc_size,
                        randomize_value=self.randomize_value,
                        mix_key_size=self.mix_key_size,
                        deep_copy=self.deep_copy)
                    mutated += 1
                    tem_tasks_info = self.bucket_util._async_load_all_buckets(
                        self.cluster, r_update, "update", 0,
                        batch_size=self.batch_size,
                        process_concurrency=self.process_concurrency,
                        persist_to=self.persist_to,
                        replicate_to=self.replicate_to,
                        durability=self.durability_level,
                        pause_secs=5,
                        timeout_secs=self.sdk_timeout,
                        retries=self.sdk_retries,
                        retry_exceptions=self.retry_exceptions,
                        ignore_exceptions=self.ignore_exceptions)
                    tasks_info.update(tem_tasks_info.items())
                for task in tasks_info:
                    self.task_manager.get_task_result(task)
                self.bucket_util.verify_doc_op_task_exceptions(
                    tasks_info, self.cluster)
                self.bucket_util.log_doc_ops_task_failures(tasks_info)
                if not self.next_half:
                    tem_tasks_info = self.bucket_util._async_load_all_buckets(
                        self.cluster, r_update, "update", 0,
                        batch_size=self.batch_size,
                        process_concurrency=self.process_concurrency,
                        persist_to=self.persist_to,
                        replicate_to=self.replicate_to,
                        durability=self.durability_level,
                        pause_secs=5,
                        timeout_secs=self.sdk_timeout,
                        retries=self.sdk_retries,
                        retry_exceptions=self.retry_exceptions,
                        ignore_exceptions=self.ignore_exceptions)
                    for task in tem_tasks_info:
                        self.task_manager.get_task_result(task)
                    self.bucket_util.verify_doc_op_task_exceptions(
                            tem_tasks_info, self.cluster)
                    self.bucket_util.log_doc_ops_task_failures(tem_tasks_info)
                self.log.info("Waiting for ep-queues to get drained")
                self.bucket_util._wait_for_stats_all_buckets()
                if self.next_half:
                    data_validation.extend([self.task.async_validate_docs(
                        self.cluster, self.bucket_util.buckets[0],
                        g_update, "update", 0,
                        batch_size=self.batch_size,
                        process_concurrency=self.process_concurrency,
                        pause_secs=5,
                        timeout_secs=self.sdk_timeout),
                        self.task.async_validate_docs(
                            self.cluster,
                            self.bucket_util.buckets[0],
                            r_update, "update", 0,
                            batch_size=self.batch_size,
                            process_concurrency=self.process_concurrency,
                            pause_secs=5, timeout_secs=self.sdk_timeout)])
                else:
                    data_validation.append(self.task.async_validate_docs(
                        self.cluster, self.bucket_util.buckets[0],
                        r_update, "update", 0,
                        batch_size=self.batch_size,
                        process_concurrency=self.process_concurrency,
                        pause_secs=5,
                        timeout_secs=self.sdk_timeout))
                for task in data_validation:
                    self.task.jython_task_manager.get_task_result(task)
                disk_usage = self.get_disk_usage(
                    self.bucket_util.get_all_buckets()[0],
                    self.servers)
                _res = disk_usage[0] - disk_usage[1]
                self.log.info("disk usage after update count {}\
                is {}".format(count+1, _res))
                self.assertIs(
                    _res > 4 * self.disk_usage[self.disk_usage.keys()[0]],
                    False, "Disk Usage {} After \
                    Update Count {} exceeds \
                    Actual disk usage {} by four \
                    times".format(_res, count,
                                  self.disk_usage[self.disk_usage.keys()[0]]))
                count += 1
            self.update_itr += self.update_itr
            start_del = 0
            end_del = self.num_items//2
            if self.rev_del:
                start_del = -int(self.num_items//2 - 1)
                end_del = 1
            self.gen_delete = doc_generator(
                self.key, start_del, end_del,
                doc_size=self.doc_size,
                doc_type=self.doc_type,
                target_vbucket=self.target_vbucket,
                vbuckets=self.cluster_util.vbuckets,
                key_size=self.key_size,
                randomize_doc_size=self.randomize_doc_size,
                randomize_value=self.randomize_value,
                mix_key_size=self.mix_key_size,
                deep_copy=self.deep_copy)
            self.log.info("Deleting num_items//2 docs")
            self.doc_ops = "delete"
            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=True)
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(self.num_items)
            disk_usage = self.get_disk_usage(
                self.bucket_util.get_all_buckets()[0],
                self.servers)
            _res = disk_usage[0] - disk_usage[1]
            self.log.info("disk usage after delete is {}".format(_res))
            self.assertIs(
                _res > 4 * self.disk_usage[self.disk_usage.keys()[0]],
                False, "Disk Usage {} After \
                Delete count {} exceeds Actual \
                disk usage {} by four \
                times".format(_res, i+1,
                              self.disk_usage[self.disk_usage.keys()[0]]))
            self.gen_create = copy.deepcopy(self.gen_delete)
            self.log.info("Recreating num_items//2 docs")
            self.doc_ops = "create"
            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=True)
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(self.num_items)
            d_validation = self.task.async_validate_docs(
                self.cluster, self.bucket_util.buckets[0],
                self.gen_create, "create", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                pause_secs=5, timeout_secs=self.sdk_timeout)
            self.task.jython_task_manager.get_task_result(d_validation)
            disk_usage = self.get_disk_usage(
                self.bucket_util.get_all_buckets()[0],
                self.servers)
            _res = disk_usage[0] - disk_usage[1]
            self.log.info("disk usage after new create \
            is {}".format(_res))
            self.assertIs(
                _res > 4 * self.disk_usage[self.disk_usage.keys()[0]],
                False, "Disk Usage {} After \
                new Creates count {} exceeds \
                Actual disk usage {} by four \
                times".format(_res, i+1,
                              self.disk_usage[self.disk_usage.keys()[0]]))
        self.log.info("====test_update_rev_update ends====")
