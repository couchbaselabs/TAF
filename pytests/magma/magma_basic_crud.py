import copy
import threading

from Cb_constants.CBServer import CbServer
from com.couchbase.client.core.error import DocumentUnretrievableException
from com.couchbase.client.java.kv import GetAnyReplicaOptions
from couchbase_helper.documentgenerator import doc_generator
import json as Json
from magma_base import MagmaBaseTest
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient


class BasicCrudTests(MagmaBaseTest):
    def setUp(self):
        super(BasicCrudTests, self).setUp()
        #self.enable_disable_swap_space(self.cluster.nodes_in_cluster)
        self.disk_usage = dict()

        self.create_start = 0
        self.create_end = self.num_items
        if self.rev_write:
            self.create_start = -int(self.num_items - 1)
            self.create_end = 1

        self.read_start = 0
        self.read_end = self.num_items
        if self.rev_read:
            self.read_start = -int(self.num_items - 1)
            self.read_end = 1

        self.delete_start = 0
        self.delete_end = self.num_items
        if self.rev_del:
            self.delete_start = -int(self.num_items - 1)
            self.delete_end = 1

        self.update_start = 0
        self.update_end = self.num_items
        if self.rev_update:
            self.update_start = -int(self.num_items - 1)
            self.update_end = 1

        self.generate_docs(doc_ops="create")

        self.init_loading = self.input.param("init_loading", True)
        if self.init_loading:
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

            if self.standard_buckets == 1 or self.standard_buckets == self.magma_buckets:
                for bucket in self.bucket_util.get_all_buckets():
                    disk_usage = self.get_disk_usage(
                        bucket, self.cluster.nodes_in_cluster)
                    self.disk_usage[bucket.name] = disk_usage[0]
                    self.log.info(
                        "For bucket {} disk usage after initial creation is {}MB\
                        ".format(bucket.name,
                                 self.disk_usage[bucket.name]))
            self.init_items = self.num_items
            self.end = self.num_items

        self.generate_docs(doc_ops="update:read:delete")

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
            if len(self.client.get_from_all_replicas(key)) > 0:
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
            self.log.info("Create Iteration count == {}".format(count))
            for node in self.cluster.nodes_in_cluster:
                shell = RemoteMachineShellConnection(node)
                shell.kill_memcached()
                shell.disconnect()
            self.doc_ops = "create:read"
            self.create_start = self.num_items
            self.create_end = self.num_items+init_items
            self.read_start = self.num_items
            self.read_end = self.num_items+init_items

            if self.rev_write:
                self.create_start = -int(self.num_items+init_items - 1)
                self.create_end = -int(self.num_items - 1)
            if self.rev_read:
                self.read_start = -int(self.num_items+init_items - 1)
                self.read_end = -int(self.num_items - 1)

            self.generate_docs(doc_ops="create")
            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=True)
            self.log.info("Verifying doc counts after create doc_ops")
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(self.num_items)

            self.generate_docs(doc_ops="read")

            if self.doc_size <= 32:
                for bucket in self.bucket_util.get_all_buckets():
                    disk_usage = self.get_disk_usage(
                        bucket, self.cluster.nodes_in_cluster)
                    msg = "Bucket= {}, For Iteration= {},\
                    SeqTree= {}MB > keyTree= {}MB"
                    self.assertIs(
                        disk_usage[2] > disk_usage[3], True,
                        msg.format(bucket.name, count+1,
                                   disk_usage[3], disk_usage[2]))
            count += 1
        self.log.info("====test_basic_create_read ends====")

    def test_update_multi(self):
        """
        1) Update docs n times, where n gets calculated
        from fragmentataion value
        2) After each iteration check for space amplification
        3) After all iterations validate the data
        """
        self.log.info("test_update_multi starts")

        upsert_doc_list = self.get_fragmentation_upsert_docs_list()

        count = 0
        self.mutate = 0
        while count < self.test_itr:
            self.log.info("Iteration == {}".format(count+1))

            for itr in upsert_doc_list:
                self.doc_ops = "update"
                self.update_start = 0
                self.update_end = itr

                if self.rev_update:
                    self.update_start = -int(itr - 1)
                    self.update_end = 1

                self.generate_docs(doc_ops="update")
                _ = self.loadgen_docs(self.retry_exceptions,
                                      self.ignore_exceptions,
                                      _sync=True)
                self.log.info("Waiting for ep-queues to get drained")
                self.bucket_util._wait_for_stats_all_buckets()

            # Space Amplification check
            msg = "Fragmentation value for {} stats exceeds\
            the configured value"
            _result = self.check_fragmentation_using_magma_stats(
                self.buckets[0],
                self.cluster.nodes_in_cluster)
            self.assertIs(_result, True,
                          msg.format("magma"))

            _r = self.check_fragmentation_using_bucket_stats(
                self.buckets[0], self.cluster.nodes_in_cluster)
            self.assertIs(_r, True,
                          msg.format("KV"))

            disk_usage = self.get_disk_usage(self.buckets[0],
                                             self.cluster.nodes_in_cluster)
            _res = disk_usage[0]
            self.log.info("Update Iteration- {}, Disk Usage- {}MB\
            ".format(count+1, _res))

            usage_factor = ((float(
                    self.num_items + sum(upsert_doc_list)
                    ) / self.num_items) + 0.5)
            self.log.debug("Disk usage factor = {}".format(usage_factor))

            msg = "Iteration= {}, Disk Usage = {}MB\
            exceeds {} times from Actual disk usage = {}MB"
            self.assertIs(_res > usage_factor * self.disk_usage[
                self.disk_usage.keys()[0]],
                False, msg.format(count+1, _res, usage_factor,
                                  self.disk_usage[self.disk_usage.keys()[0]]))
            # Spcae Amplification check ends
            count += 1

        self.validate_data("update", self.gen_update)

        self.enable_disable_swap_space(self.cluster.nodes_in_cluster,
                                       disable=False)
        self.log.info("====test_update_multi ends====")

    def test_multi_update_delete(self):
        """
        Step 1: Kill memcached and Update all the docs update_itr times
        After each iteration check for space amplification
        and for last iteration
        of test_itr validate docs
        Step 2: Delete half the docs, check sapce amplification
        Step 3 Recreate check for space amplification.
        Repeat all above steps test_itr times
        Step 4 : Do data validation for newly create docs
        """
        self.log.info("==== test_multi_update_delete starts =====")

        count = 0
        msg_stats = "Fragmentation value for {} stats exceeds\
        the configured value"
        msg = "{} Iteration= {}, Disk Usage = {}MB\
         exceeds 2.5 times from Actual disk usage = {}MB"

        self.mutate = 0
        for i in range(self.test_itr):
            self.log.info("Step 1, Iteration= {}".format(i+1))
            while count < self.update_itr:
                #for node in self.cluster.nodes_in_cluster:
                #    shell = RemoteMachineShellConnection(node)
                #    shell.kill_memcached()
                #    shell.disconnect()
                #    self.assertTrue(self.bucket_util._wait_warmup_completed(
                #                    [self.cluster_util.cluster.master],
                #                    self.bucket_util.buckets[0],
                #                    wait_time=self.wait_timeout * 10))
                self.doc_ops = "update"
                self.update_start = 0
                self.update_end = self.num_items
                if self.rev_update:
                    self.update_start = -int(self.num_items - 1)
                    self.update_end = 1

                self.generate_docs(doc_ops="update")
                _ = self.loadgen_docs(self.retry_exceptions,
                                      self.ignore_exceptions,
                                      _sync=True)

                self.log.info("Waiting for ep-queues to get drained")
                self.bucket_util._wait_for_stats_all_buckets()

                # Space amplification check
                _result = self.check_fragmentation_using_magma_stats(
                    self.buckets[0],
                    self.cluster.nodes_in_cluster)
                self.assertIs(_result, True,
                              msg_stats.format("magma"))

                _r = self.check_fragmentation_using_bucket_stats(
                    self.buckets[0], self.cluster.nodes_in_cluster)
                self.assertIs(_r, True,
                              msg_stats.format("KV"))

                disk_usage = self.get_disk_usage(self.buckets[0],
                                                 self.cluster.nodes_in_cluster)

                _res = disk_usage[0]

                self.log.info("Update Iteration- {}, Disk Usage- {}MB\
                ".format(count+1, _res))
                self.assertIs(
                    _res > 2.5 * self.disk_usage[self.disk_usage.keys()[0]],
                    False, msg.format("update", count+1, _res,
                                      self.disk_usage[self.disk_usage.keys()[0]]))
                # Spcae Amplification check ends

                count += 1
            self.update_itr += self.update_itr

            # data validation is done only for the last iteration
            if i+1 == self.test_itr:
                self.validate_data("update", self.gen_update)

            self.log.debug("Step 2, Iteration {}".format(i+1))
            self.doc_ops = "delete"

            self.delete_start = 0
            self.delete_end = self.num_items//2
            if self.rev_del:
                self.delete_start = -int(self.num_items//2 - 1)
                self.delete_end = 1

            self.generate_docs(doc_ops="delete")
            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=True)

            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(self.num_items)

            # Space amplification check
            _result = self.check_fragmentation_using_magma_stats(
                self.buckets[0],
                self.cluster.nodes_in_cluster)
            self.assertIs(_result, True,
                          msg_stats.format("magma"))

            _r = self.check_fragmentation_using_bucket_stats(
                 self.buckets[0], self.cluster.nodes_in_cluster)
            self.assertIs(_r, True,
                          msg_stats.format("KV"))

            disk_usage = self.get_disk_usage(self.buckets[0],
                                             self.cluster.nodes_in_cluster)
            _res = disk_usage[0]
            self.log.info("Delete Iteration {}, Disk Usage- {}MB\
            ".format(i+1, _res))
            self.assertIs(
                _res > 2.5 * self.disk_usage[
                    self.disk_usage.keys()[0]],
                False, msg.format(
                    "delete", i+1, _res,
                    self.disk_usage[self.disk_usage.keys()[0]]))
            # Space amplification check ends

            self.log.debug("Step 3, Iteration= {}\
            ".format(i+1))

            self.gen_create = copy.deepcopy(self.gen_delete)
            self.doc_ops = "create"

            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=True)

            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(self.num_items)

            # Space amplification check
            _result = self.check_fragmentation_using_magma_stats(
                self.buckets[0],
                self.cluster.nodes_in_cluster)
            self.assertIs(_result, True,
                          msg_stats.format("magma"))

            _r = self.check_fragmentation_using_bucket_stats(
                self.buckets[0], self.cluster.nodes_in_cluster)
            self.assertIs(_r, True,
                          msg_stats.format("KV"))

            disk_usage = self.get_disk_usage(self.buckets[0],
                                             self.cluster.nodes_in_cluster)
            _res = disk_usage[0]
            self.log.info("Create Iteration{}, Disk Usage= {}MB \
            ".format(i+1, _res))
            self.assertIs(_res > 2.5 * self.disk_usage[
                self.disk_usage.keys()[0]],
                False, msg.format("Create", _res, i+1,
                                  self.disk_usage[self.disk_usage.keys()[0]]))
            # Space amplification Check  ends

        self.validate_data("create", self.gen_create)
        self.log.info("====test_multiUpdate_delete ends====")

    def test_update_rev_update(self):
        count = 0
        mutated = 1
        for i in range(self.test_itr):
            while count < self.update_itr:
                #for node in self.cluster.nodes_in_cluster:
                #    shell = RemoteMachineShellConnection(node)
                #    shell.kill_memcached()
                #    shell.disconnect()
                #    self.assertTrue(self.bucket_util._wait_warmup_completed(
                #                    [self.cluster_util.cluster.master],
                #                    self.bucket_util.buckets[0],
                #                    wait_time=self.wait_timeout * 10))
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
                # Spcae amplification check
                _result = self.check_fragmentation_using_magma_stats(
                    self.buckets[0],
                    self.cluster.nodes_in_cluster)
                self.assertIs(_result, True,
                              "Fragmentation value exceeds from '\n' \
                              the configured fragementaion value")

                _r = self.check_fragmentation_using_bucket_stats(
                    self.buckets[0], self.cluster.nodes_in_cluster)
                self.assertIs(_r, True,
                              "Fragmentation value exceeds from '\n' \
                              the configured fragementaion value")

                disk_usage = self.get_disk_usage(
                    self.buckets[0],
                    self.cluster.nodes_in_cluster)
                _res = disk_usage[0] - disk_usage[1]
                self.log.info("disk usage after update count {}\
                is {}".format(count+1, _res))
                self.assertIs(
                    _res > 2.5 * self.disk_usage[self.disk_usage.keys()[0]],
                    False, "Disk Usage {} After \
                    Update Count {} exceeds \
                    Actual disk usage {} by four \
                    times".format(_res, count,
                                  self.disk_usage[self.disk_usage.keys()[0]]))
                # Spcae amplification check ends
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
            # Space amplifcation check
            _result = self.check_fragmentation_using_magma_stats(
                self.buckets[0],
                self.cluster.nodes_in_cluster)
            self.assertIs(_result, True,
                          "Fragmentation value exceeds from '\n' \
                          the configured fragementaion value")

            _r = self.check_fragmentation_using_bucket_stats(
                self.buckets[0], self.cluster.nodes_in_cluster)
            self.assertIs(_r, True,
                          "Fragmentation value exceeds from '\n' \
                          the configured fragementaion value")

            disk_usage = self.get_disk_usage(
                self.buckets[0],
                self.cluster.nodes_in_cluster)
            _res = disk_usage[0] - disk_usage[1]
            self.log.info("disk usage after delete is {}".format(_res))
            self.assertIs(
                _res > 2.5 * self.disk_usage[self.disk_usage.keys()[0]],
                False, "Disk Usage {} After \
                Delete count {} exceeds Actual \
                disk usage {} by four \
                times".format(_res, i+1,
                              self.disk_usage[self.disk_usage.keys()[0]]))
            # Space amplification check ends
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
            # Space amplification check
            _result = self.check_fragmentation_using_magma_stats(
                self.buckets[0],
                self.cluster.nodes_in_cluster)
            self.assertIs(_result, True,
                          "Fragmentation value exceeds from '\n' \
                          the configured fragementaion value")

            _r = self.check_fragmentation_using_bucket_stats(
                self.buckets[0], self.cluster.nodes_in_cluster)
            self.assertIs(_r, True,
                          "Fragmentation value exceeds from '\n' \
                          the configured fragementaion value")

            disk_usage = self.get_disk_usage(
                self.buckets[0],
                self.cluster.nodes_in_cluster)
            _res = disk_usage[0] - disk_usage[1]
            self.log.info("disk usage after new create \
            is {}".format(_res))
            self.assertIs(
                _res > 2.5 * self.disk_usage[self.disk_usage.keys()[0]],
                False, "Disk Usage {} After \
                new Creates count {} exceeds \
                Actual disk usage {} by four \
                times".format(_res, i+1,
                              self.disk_usage[self.disk_usage.keys()[0]]))
            # Space amplification check ends
        self.log.info("====test_update_rev_update ends====")

    def test_update_single_doc_n_times(self):
        """
        Update a single document n times,
        Important Note: Multithreading is used to update
        single doc, since we are not worried about what
        should be the final val of mutate in doc
        semaphores have been avoided(also to speed up
        the execution of test
        """
        self.doc_ops = "update"

        self.client = SDKClient([self.cluster.master],
                                self.bucket_util.buckets[0],
                                scope=CbServer.default_scope,
                                collection=CbServer.default_collection)

        self.gen_update = self.genrate_docs_basic(start=0, end=1)

        key, val = self.gen_update.next()
        for node in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(node)
            shell.kill_memcached()
            shell.disconnect()
            self.assertTrue(
                self.bucket_util._wait_warmup_completed(
                    [self.cluster_util.cluster.master],
                    self.bucket_util.buckets[0],
                    wait_time=self.wait_timeout * 10))

        def upsert_doc(start_num, end_num, key_obj, val_obj):
            for i in range(start_num, end_num):
                val_obj.put("mutated", i)
                self.client.upsert(key_obj, val_obj)

        threads = []
        start = 0
        end = 0
        for _ in range(10):
            start = end
            end += 100000
            th = threading.Thread(
                target=upsert_doc, args=[start, end, key, val])
            th.start()
            threads.append(th)

        for th in threads:
            th.join()

        self.bucket_util._wait_for_stats_all_buckets()

        # Space amplification check
        msg_stats = "Fragmentation value for {} stats exceeds\
        the configured value"
        _result = self.check_fragmentation_using_magma_stats(
            self.buckets[0],
            self.cluster.nodes_in_cluster)
        self.assertIs(_result, True,
                      msg_stats.format("magma"))

        _r = self.check_fragmentation_using_bucket_stats(
            self.buckets[0], self.cluster.nodes_in_cluster)
        self.assertIs(_r, True,
                      msg_stats.format("KV"))

        disk_usage = self.get_disk_usage(
            self.buckets[0],
            self.cluster.nodes_in_cluster)
        self.log.debug("Disk usage after updates {}".format(
            disk_usage))
        _res = disk_usage[0]
        msg = "Disk Usage = {}MB exceeds 2.2 times \
        from Actual disk usage = {}MB"
        self.assertIs(
            _res > 2.2 * self.disk_usage[
                self.disk_usage.keys()[0]],
            False,
            msg.format(_res, self.disk_usage[self.disk_usage.keys()[0]]))
        # Space amplification check ends

        success, fail = self.client.get_multi([key],
                                              self.wait_timeout)

        self.assertIs(key in success, True,
                      msg="key {} doesn't exist\
                      ".format(key))
        actual_val = dict()
        expected_val = Json.loads(val.toString())
        actual_val = Json.loads(success[key][
            'value'].toString())
        self.log.debug("Expected_val= {} and actual_val = {}\
        ".format(expected_val, actual_val))
        self.assertIs(expected_val == actual_val, True,
                      msg="expected_val-{} != Actual_val-{}\
                      ".format(expected_val, actual_val))

        self.enable_disable_swap_space(self.cluster.nodes_in_cluster,
                                       disable=False)
        self.log.info("====test_update_single_doc_n_times====")

    def test_read_docs_using_multithreads(self):
        """
        Read same docs together using multithreads.
        """
        self.log.info("Reading docs parallelly using multi threading")
        tasks_info = dict()
        update_doc_list = self.get_fragmentation_upsert_docs_list()

        for itr in upsert_doc_list:
            self.doc_ops = "update"
            self.update_start = 0
            self.update_end = itr
            self.mutate = -1
            self.generate_docs(doc_ops="update")

            update_task_info = self.loadgen_docs(
                self.retry_exceptions,
                self.ignore_exceptions,
                _sync=False)
            tasks_info.update(update_task_info.items())

        count = 0
        self.doc_ops = "read"

        # if self.next_half is true then one thread will read
        # in ascending order and other in descending order

        if self.next_half:
            start = -int(self.num_items - 1)
            end = 1
            g_read = doc_generator(
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
        for node in self.cluster.nodes_in_cluster:
                shell = RemoteMachineShellConnection(node)
                shell.kill_memcached()
                shell.disconnect()

        while count < self.read_thread_count:
            read_task_info = self.loadgen_docs(
                self.retry_exceptions,
                self.ignore_exceptions,
                _sync=False)
            tasks_info.update(read_task_info.items())
            count += 1
            if self.next_half and count < self.read_thread_count:
                read_task_info = self.bucket_util._async_validate_docs(
                    self.cluster, g_read, "read", 0,
                    batch_size=self.batch_size,
                    process_concurrency=self.process_concurrency,
                    pause_secs=5, timeout_secs=self.sdk_timeout,
                    retry_exceptions=self.retry_exceptions,
                    ignore_exceptions=self.ignore_exceptions)
                tasks_info.update(read_task_info.items())
                count += 1

        for task in tasks_info:
                self.task_manager.get_task_result(task)

        self.log.info("Waiting for ep-queues to get drained")
        self.bucket_util._wait_for_stats_all_buckets()
        self.log.info("test_read_docs_using_multithreads ends")

    def test_basic_create_delete(self):
        """
        CREATE(n)-> DELETE(n)->DISK_USAGE_CHECK
        REPEAT ABove test_itr_times
        """
        self.log.info("Cretaing  and Deletes docs n times ")

        keyTree, seqTree = (self.get_disk_usage(
                        self.buckets[0],
                        self.cluster.nodes_in_cluster)[2:4])
        self.log.debug("Initial Disk usage for keyTree and SeqTree is '\n'\
        {}MB and {} MB".format(keyTree, seqTree))
        self.delete_start = 0
        self.delete_end = self.num_items
        self.generate_docs(doc_ops="delete")

        count = 0
        while count < self.test_itr:
            self.doc_ops = "delete"
            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=True)
            self.log.info("Verifying doc counts after delete doc_ops")
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(self.num_items)
            #Space Amplification check
            _res = self.check_fragmentation_using_magma_stats(
                self.buckets[0],
                self.cluster.nodes_in_cluster)
            self.assertIs(_res, True,
                          "Fragmentation value exceeds from '\n' \
                          the configured fragementaion value")

            _r = self.check_fragmentation_using_bucket_stats(
                self.buckets[0], self.cluster.nodes_in_cluster)
            self.assertIs(_r, True,
                          "Fragmentation value exceeds from '\n' \
                          the configured fragementaion value")
            #Space Amplification check ends
            self.doc_ops = "create"
            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=True)
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(self.num_items)
            count += 1
        self.log.info("====test_basic_create_delete ends====")

    def test_move_val_btwn_key_and_seq_trees(self):
        """
        Update docs such that values moves between
        seq and key Trees.
        Below are the steps
        Step 1: Update docs with new size , so that
        docs move between tree
        Step 2: Do data validation
        Step 3: Again update docs with intital size
        Step 4: Check space amplification
        Step 5: Again validate docs
        """
        count = 0
        keyTree, seqTree = (self.get_disk_usage(
                        self.buckets[0],
                        self.cluster.nodes_in_cluster)[2:4])
        self.log.debug("DIsk usage after pure creates {}".format((
            self.disk_usage, keyTree, seqTree)))
        upsert_size = 0
        if self.doc_size < 32:
            upsert_size = 2048

        mutated = 1
        while count < self.test_itr:
            self.log.info("Update Iteration count == {}".format(count))
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
                doc_size=upsert_size,
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

            if upsert_size > 32:
                seqTree_update = (self.get_disk_usage(
                        self.buckets[0],
                        self.cluster.nodes_in_cluster)[-1])
                self.log.info("For upsert_size > 32 seqIndex usage-{}\
                ".format(seqTree_update))

            data_validation = self.task.async_validate_docs(
                self.cluster, self.bucket_util.buckets[0],
                self.gen_update, "update", 0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                pause_secs=5,
                timeout_secs=self.sdk_timeout)
            self.task.jython_task_manager.get_task_result(data_validation)

            # Upserting with changed doc size to move between tress

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

            # Space amplification check
            _result = self.check_fragmentation_using_magma_stats(
                self.buckets[0], self.cluster.nodes_in_cluster)
            self.assertIs(_result, True,
                          "Fragmentation value exceeds from '\n' \
                          the configured fragementaion value")

            _r = self.check_fragmentation_using_bucket_stats(
                self.buckets[0], self.cluster.nodes_in_cluster)
            self.assertIs(_r, True,
                          "Fragmentation value exceeds from '\n' \
                          the configured fragementaion value")
            disk_usage = self.get_disk_usage(
                self.buckets[0], self.cluster.nodes_in_cluster)
            _res = disk_usage[0]
            self.log.info("disk usage after upsert count {} is {}MB \
                ".format(count+1, _res))
            if self.doc_size > 32:
                self.assertIs(
                    _res > 1.5 * self.disk_usage[self.disk_usage.keys()[0]],
                    False, "Disk Usage {} After \
                    update count {} exceeds \
                    Actual disk usage {} by 1.5 \
                    times".format(_res, count+1,
                                  self.disk_usage[self.disk_usage.keys()[0]]))
            else:
                self.assertIs(disk_usage[3] > 0.5 * seqTree_update,
                              False, " Current seqTree usage-{} exceeds by'\n'\
                               0.5 times from the earlier '\n' \
                               seqTree usage (after update) -{} \
                              ".format(disk_usage[3], seqTree_update))
            # Space Amplification check ends
            count += 1
        data_validation = self.task.async_validate_docs(
            self.cluster, self.bucket_util.buckets[0],
            self.gen_update, "update", 0,
            batch_size=self.batch_size,
            process_concurrency=self.process_concurrency,
            pause_secs=5, timeout_secs=self.sdk_timeout)
        self.task.jython_task_manager.get_task_result(data_validation)
        self.enable_disable_swap_space(self.cluster.nodes_in_cluster, disable=False)
        self.log.info("====test_move_docs_btwn_key_and_seq_trees ends====")

    def test_parallel_create_update(self):
        """
        Create new docs and updates already created docs
        Check disk_usage after each Iteration
        Data validation for last iteration
        """
        self.log.info("Updating and Creating docs parallelly")
        count = 0
        init_items = self.num_items
        self.doc_ops = "create:update"
        while count < self.test_itr:
            self.log.info("Iteration {}".format(count+1))
            start = self.num_items
            end = self.num_items+init_items
            start_update = self.num_items
            end_update = self.num_items+init_items
            if self.rev_write:
                start = -int(self.num_items+init_items - 1)
                end = -int(self.num_items - 1)
            if self.rev_update:
                start_update = -int(self.num_items+init_items - 1)
                end_update = -int(self.num_items - 1)
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
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(self.num_items)
            if count == self.test_itr - 1:
                data_validation = self.task.async_validate_docs(
                    self.cluster, self.bucket_util.buckets[0],
                    self.gen_update, "update", 0,
                    batch_size=self.batch_size,
                    process_concurrency=self.process_concurrency,
                    pause_secs=5, timeout_secs=self.sdk_timeout)
                self.task.jython_task_manager.get_task_result(
                    data_validation)
            self.gen_update = doc_generator(
                self.key, start_update, end_update,
                doc_size=self.doc_size,
                doc_type=self.doc_type,
                target_vbucket=self.target_vbucket,
                vbuckets=self.cluster_util.vbuckets,
                key_size=self.key_size,
                mutate=1,
                randomize_doc_size=self.randomize_doc_size,
                randomize_value=self.randomize_value,
                mix_key_size=self.mix_key_size,
                deep_copy=self.deep_copy)
            disk_usage = self.get_disk_usage(
                self.buckets[0],
                self.cluster.nodes_in_cluster)
            if self.doc_size <= 32:
                self.assertIs(
                    disk_usage[2] >= disk_usage[3], True,
                    "seqIndex usage = {}MB'\n' \
                    after Iteration {}'\n' \
                    exceeds keyIndex usage={}MB'\n' \
                    ".format(disk_usage[3],
                             count+1,
                             disk_usage[2]))
            self.assertIs(
                disk_usage[0] > 2.2 * (2 * self.disk_usage[
                    self.disk_usage.keys()[0]]),
                False, "Disk Usage {}MB After '\n\'\
                Updates exceeds '\n\'\
                Actual disk usage {}MB by '\n'\
                2.2 times".format(disk_usage[0],
                                  (2 * self.disk_usage[
                                      self.disk_usage.keys()[0]])))
            count += 1
        self.enable_disable_swap_space(self.cluster.nodes_in_cluster, disable=False)
        self.log.info("====test_parallel_create_update ends====")

    def test_parallel_creates_deletes(self):
        """
        Primary focus for this is to check space
        Amplification

        Create new docs and deletes already created docs
        Check disk_usage after each Iteration
        """
        self.log.info("Deletion and Creation of docs parallelly")
        init_items = self.num_items

        self.create_end = self.num_items
        self.delete_end = self.num_items

        self.doc_ops = "create:delete"

        count = 0
        while count < self.test_itr:
            self.log.info("Iteration {}".format(count+1))
            if self.rev_write:
                self.create_start = -int(self.create_end + init_items - 1)
                self.create_end = -int(self.create_end - 1)
            else:
                self.create_start = self.create_end
                self.create_end += init_items

            if self.rev_del:
                self.delete_start = -int(self.delete_end + init_items - 1)
                self.delete_end = -int(self.delete_end - 1)
            else:
                self.delete_start = self.delete_end
                self.delete_end += init_items

            self.generate_docs(doc_ops="create")

            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  _sync=True)

            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(self.num_items)

            self.generate_docs(doc_ops="delete")

            if self.rev_write:
                self.create_end = -(self.create_start-1)
            if self.rev_del:
                self.delete_end = -(self.delete_start-1)

            # Space Amplification Check
            msg_stats = "{} stats fragmentation exceeds configured value"
            _result = self.check_fragmentation_using_magma_stats(
                self.buckets[0], self.cluster.nodes_in_cluster)
            self.assertIs(_result, True, msg_stats.format("magma"))

            _r = self.check_fragmentation_using_bucket_stats(
                self.buckets[0], self.cluster.nodes_in_cluster)
            self.assertIs(_r, True, msg_stats.format("KV"))

            disk_usage = self.get_disk_usage(
                self.buckets[0],
                self.cluster.nodes_in_cluster)
            if self.doc_size <= 32:
                msg = "Iteration={}, SeqTree={}MB exceeds keyTree={}MB"
                self.assertIs(
                    disk_usage[2] >= disk_usage[3], True,
                    msg.format(count+1, disk_usage[3], disk_usage[2]))
            else:
                msg = "Iteration={}, Disk Usage={}MB > {} * init_Usage={}MB"
                self.assertIs(disk_usage[0] > 2.5 * self.disk_usage[
                    self.disk_usage.keys()[0]],
                    False, msg.format(count+1, disk_usage[0], 2.5,
                                      self.disk_usage[self.disk_usage.keys()[0]]))
            #Space Amplifacation check Ends
            count += 1

        self.doc_ops = "delete"
        self.gen_delete = copy.deepcopy(self.gen_create)
        _ = self.loadgen_docs(self.retry_exceptions,
                              self.ignore_exceptions,
                              _sync=True)

        #Space Amplifaction check after all deletes
        _result = self.check_fragmentation_using_magma_stats(
            self.buckets[0], self.cluster.nodes_in_cluster)
        self.assertIs(_result, True, msg_stats.format("magma"))

        _r = self.check_fragmentation_using_bucket_stats(
            self.buckets[0], self.cluster.nodes_in_cluster)
        self.assertIs(_r, True, msg_stats.format("KV"))

        disk_usage = self.get_disk_usage(
                self.buckets[0],
                self.cluster.nodes_in_cluster)
        msg = "Disk Usage={}MB > {} * init_Usage={}MB"
        self.assertIs(disk_usage[0] > 0.4 * self.disk_usage[
            self.disk_usage.keys()[0]], False,
            msg.format(disk_usage[0], 0.4,
                       self.disk_usage[self.disk_usage.keys()[0]]))
        # Space Amplifiaction check ends

        self.enable_disable_swap_space(self.cluster.nodes_in_cluster,
                                       disable=False)
        self.log.info("====test_parallel_create_delete ends====")

    def test_drop_collections_after_upserts(self):
        """
        Test will check space
        Amplification after collection drop
        !) Create multiple collections
        2) Load docs in all collections
        3) Drop a collections
        4)Verify space amplification
        """
        scope_name = CbServer.default_scope
        collection_prefix = "FunctionCollection"

        # # # # Non Default Scope creation # # # #
        if self.num_scopes > 1:
            scope_name = "FunctionScope"
            self.bucket_util.create_scope(self.cluster.master,
                                          self.buckets[0],
                                          {"name": scope_name})

        # # # # Collections Creation # # # #
        for i in range(self.num_collections):
            collection_name = collection_prefix + str(i)
            self.log.info("Creating scope::collection {} {}\
            ".format(scope_name, collection_name))
            self.bucket_util.create_collection(
                self.cluster.master, self.buckets[0],
                scope_name, {"name": collection_name})
            self.sleep(2)
        collections = self.buckets[0].scopes[scope_name].collections.keys()
        if self.num_collections > 1 and scope_name is CbServer.default_scope:
            collections.remove(CbServer.default_collection)
        self.log.info("List of collections {}".format(collections))

        # # # # DOC LOADING # # # #
        end = 0
        init_items = self.num_items
        tasks_info = dict()
        self.doc_ops = "create"
        for collection in collections:
            start = end
            end += init_items
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
            tem_tasks_info = self.loadgen_docs(
                self.retry_exceptions,
                self.ignore_exceptions,
                scope=scope_name,
                collection=collection,
                _sync=False)
            tasks_info.update(tem_tasks_info.items())
        self.num_items -= init_items
        for task in tasks_info:
            self.task_manager.get_task_result(task)
        self.bucket_util.verify_doc_op_task_exceptions(
            tasks_info, self.cluster)
        self.bucket_util.log_doc_ops_task_failures(tasks_info)
        self.log.info("Verifying num_items counts after doc_ops")
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        # # # # Initial Disk Usage # # # #
        disk_usage = self.get_disk_usage(
            self.buckets[0], self.cluster.nodes_in_cluster)
        self.disk_usage[self.buckets[0].name] = disk_usage[0]
        self.log.info(
            "For bucket {} disk usage after initial '\n' \
            creation is {}MB".format(
                self.buckets[0].name,
                self.disk_usage[self.buckets[0].name]))

        # # # # Update docs in a single collection # # # #
        count = 0
        mutated = 1
        self.doc_ops = "update"
        self.log.info("Docs to be updated in collection {}\
        ".format(collections[-1]))
        while count < self.test_itr:
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
            _ = self.loadgen_docs(self.retry_exceptions,
                                  self.ignore_exceptions,
                                  scope=scope_name,
                                  collection=collections[-1],
                                  _sync=True)
            self.log.info("Waiting for ep-queues to get drained")
            self.bucket_util._wait_for_stats_all_buckets()
            count += 1

        # # # # Drop a collection # # # #
        self.log.info("Collection to be dropped {}\
        ".format(collections[0]))
        self.bucket_util.drop_collection(
            self.cluster.master, self.buckets[0],
            scope_name=scope_name,
            collection_name=collections[0])
        self.buckets[0].scopes[scope_name].collections.pop(collections[0])
        collections.remove(collections[0])

        # # # # Space Amplification check # # # #
        _result = self.check_fragmentation_using_magma_stats(
            self.buckets[0], self.cluster.nodes_in_cluster)
        self.assertIs(_result, True,
                      "Fragmentation value exceeds from '\n' \
                      the configured fragementaion value")

        _r = self.check_fragmentation_using_bucket_stats(
             self.buckets[0], self.cluster.nodes_in_cluster)
        self.assertIs(_r, True,
                      "Fragmentation value exceeds from '\n' \
                      the configured fragementaion value")

        disk_usage = self.get_disk_usage(
                self.buckets[0], self.cluster.nodes_in_cluster)
        _res = disk_usage[0]
        self.assertIs(
            _res > 2.5 * self.disk_usage[
                self.disk_usage.keys()[0]],
            False, "Disk Usage {}MB '\n' \
            exceeds Actual'\n' \
            disk usage {}MB by 2.5'\n' \
            times".format(
                    _res,
                    self.disk_usage[self.disk_usage.keys()[0]]))
        # # # # Space Amplification check ends # # # #

        self.log.info("====test_drop_collections_after_upserts====")

    def test_drop_collections_after_deletes(self):
        """
        Test will check space
        Amplification after collection drop
        !) Create multiple collections
        2) Load docs in all collections
        3) Delete 3/4th of docs in one collection
        4) Since default frag is 50, auto compaction shouldn't trigger
        5) Drop a collection
        6)Auto compaction shoudl trigger now, Verify space amplification
        """
        scope_name = CbServer.default_scope
        collection_prefix = "FunctionCollection"

        # # # # Non Default Scope creation # # # #
        if self.num_scopes > 1:
            scope_name = "FunctionScope"
            self.bucket_util.create_scope(self.cluster.master,
                                          self.buckets[0],
                                          {"name": scope_name})

        # # # # Collections Creation # # # #
        for i in range(self.num_collections):
            collection_name = collection_prefix + str(i)
            self.log.info("Creating scope::collection {} {}\
            ".format(scope_name, collection_name))
            self.bucket_util.create_collection(
                self.cluster.master, self.buckets[0],
                scope_name, {"name": collection_name})
            self.sleep(2)
        collections = self.buckets[0].scopes[scope_name].collections.keys()
        if self.num_collections > 1 and scope_name is CbServer.default_scope:
            collections.remove(CbServer.default_collection)
        self.log.info("List of collections {}".format(collections))

        # # # # DOC LOADING # # # #
        end = 0
        init_items = self.num_items
        tasks_info = dict()
        self.doc_ops = "create"
        for collection in collections:
            start = end
            end += init_items
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
            tem_tasks_info = self.loadgen_docs(
                self.retry_exceptions,
                self.ignore_exceptions,
                scope=scope_name,
                collection=collection,
                _sync=False)
            tasks_info.update(tem_tasks_info.items())
        self.num_items -= init_items
        for task in tasks_info:
            self.task_manager.get_task_result(task)
        self.bucket_util.verify_doc_op_task_exceptions(
            tasks_info, self.cluster)
        self.bucket_util.log_doc_ops_task_failures(tasks_info)
        self.log.info("Verifying num_items counts after doc_ops")
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

        # # # # Initial Disk Usage # # # #
        disk_usage = self.get_disk_usage(
            self.buckets[0], self.cluster.nodes_in_cluster)
        self.disk_usage[self.buckets[0].name] = disk_usage[0]
        self.log.info(
            "For bucket {} disk usage after initial '\n' \
            creation is {}MB".format(
                self.buckets[0].name,
                self.disk_usage[self.buckets[0].name]))

        # Space amplification check before deletes
        # This check is to make sure, compaction doesn't get triggerd
        _result = self.check_fragmentation_using_magma_stats(
            self.buckets[0], self.cluster.nodes_in_cluster)
        self.assertIs(_result, True,
                      "Fragmentation value exceeds from '\n' \
                      the configured fragementaion value")

        _r = self.check_fragmentation_using_bucket_stats(
            self.buckets[0], self.cluster.nodes_in_cluster)
        self.assertIs(_r, True,
                      "Fragmentation value exceeds from '\n' \
                      the configured fragementaion value")

        # # # # Delete  3/4th docs in a single collection # # # #
        self.doc_ops = "delete"
        self.log.info("For deletion collection picked is {} \
        ".format(collections[-1]))
        self.gen_delete = doc_generator(
            self.key, start, start + int(0.75 * (end-start)),
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
                              scope=scope_name,
                              collection=collections[-1],
                              _sync=True)
        self.log.info("Waiting for ep-queues to get drained")
        self.bucket_util._wait_for_stats_all_buckets()

        # # # # Drop a collection # # # #
        self.log.info("Collection to be dropped {}\
        ".format(collections[0]))
        self.bucket_util.drop_collection(
            self.cluster.master, self.buckets[0],
            scope_name=scope_name,
            collection_name=collections[0])
        self.buckets[0].scopes[scope_name].collections.pop(collections[0])
        collections.remove(collections[0])

        # # # # Space Amplification check # # # #
        _result = self.check_fragmentation_using_magma_stats(
            self.buckets[0], self.cluster.nodes_in_cluster)
        self.assertIs(_result, True,
                      "Fragmentation value exceeds from '\n' \
                      the configured fragementaion value")

        _r = self.check_fragmentation_using_bucket_stats(
             self.buckets[0], self.cluster.nodes_in_cluster)
        self.assertIs(_r, True,
                      "Fragmentation value exceeds from '\n' \
                      the configured fragementaion value")

        # # # # Space Amplification check ends # # # #

        self.log.info("====test_drop_collections_after_deletes====")
