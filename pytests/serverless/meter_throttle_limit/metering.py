import copy
import random
import string
import threading

from Cb_constants import DocLoading
from cb_tools.mc_stat import Mcthrottle
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient
from LMT_base import LMT
from reactor.util.function import Tuples
from security_utils.audit_ready_functions import audit
from couchbase_helper.documentgenerator import doc_generator
from cb_tools.cbstats import Cbstats
from cb_tools.cbepctl import Cbepctl
from BucketLib.BucketOperations import BucketHelper


class ServerlessMetering(LMT):
    def setUp(self):
        super(ServerlessMetering, self).setUp()
        self.bucket = self.cluster.buckets[0]
        self.sdk_compression = self.input.param("sdk_compression", False)
        self.validate_stat = self.input.param("validate_stat", False)
        compression_settings = {"enabled": self.sdk_compression}
        self.client = SDKClient([self.cluster.master], self.bucket,
                                compression_settings=compression_settings)

    def tearDown(self):
        self.client.close()
        super(ServerlessMetering, self).tearDown()

    def get_key_value(self, num_items, doc_size=1000, char="a"):
        self.key = "metering-"
        key_value = dict()
        for i in range(num_items):
            key = self.key + str(i)
            doc = {"f": char * doc_size}
            key_value[key] = doc
        return key_value

    def perform_operation(self, operation, key_value, bucket,
                          expected_wu=0, expected_ru=0, durability=""):
        for key, value in key_value.iteritems():
            try:
                result = self.client.crud(operation, key, value=value,
                                          durability=durability)
            except:
                result = self.client.crud(operation, key,
                                          durability=durability)
            if result["status"] is False:
                self.log.critical("%s Loading failed: %s" % (key, result["error"]))
                break
        self.get_item_count()
        throttle_limit, ru, wu = self.bucket_util.get_stat_from_metrics(bucket)
        self.compare_ru_wu_stat(ru, wu, expected_ru, expected_wu)

    def test_cu(self):
        nodes = self.cluster_util.get_kv_nodes(self.cluster)
        if len(nodes) == 1:
            shell = RemoteMachineShellConnection(self.cluster.master)
            mc_throttle = Mcthrottle(shell)
            mc_throttle.set_throttle_limit(self.bucket)
            shell.disconnect()

        # enable audit logs
        self.log.info("Enable audit on cluster")
        self.audit_obj = audit(host=self.cluster.master)
        self.audit_obj.setAuditEnable('true')

        # write/update the document
        key_value = self.get_key_value(self.num_items, self.doc_size)
        expected_wu = self.bucket_util.calculate_units(self.doc_size, 0,
                                                       durability=self.durability_level) * self.num_items
        self.perform_operation(DocLoading.Bucket.DocOps.CREATE, key_value,
                               self.bucket, expected_wu,
                               0, durability=self.durability_level)

        # read the document
        self.total_size, ru = self.get_sizeof_document(self.key + str(0))
        expected_ru = ru + self.bucket_util.calculate_units(self.total_size, 0,
                                                read=True,
                                                durability=self.durability_level) * self.num_items
        self.perform_operation(DocLoading.Bucket.DocOps.READ, key_value,
                               self.bucket, expected_wu,
                               expected_ru, durability=self.durability_level)

        # replace the document
        key_value = self.get_key_value(self.num_items, self.doc_size, char="b")
        expected_wu += self.bucket_util.calculate_units(self.total_size, 0,
                                                        durability=self.durability_level) * self.num_items
        self.perform_operation(DocLoading.Bucket.DocOps.REPLACE, key_value,
                               self.bucket, expected_wu,
                               expected_ru, durability=self.durability_level)

        # update the document
        key_value = self.get_key_value(self.num_items, self.doc_size, char="c")
        expected_wu += self.bucket_util.calculate_units(self.total_size, 0,
                                                        durability=self.durability_level) * self.num_items
        self.perform_operation(DocLoading.Bucket.DocOps.UPDATE, key_value,
                               self.bucket, expected_wu,
                               expected_ru, durability=self.durability_level)

        # touch the document
        self.total_size, ru = self.get_sizeof_document(self.key + str(0))
        if self.durability_level != "NONE":
            expected_wu += self.bucket_util.calculate_units(self.total_size, 0,
                                                            durability=self.durability_level) / 2 * self.num_items
        else:
            expected_wu += self.bucket_util.calculate_units(self.total_size, 0,
                                                            durability=self.durability_level) * self.num_items
        expected_ru += ru
        for key, value in key_value.iteritems():
            result = self.client.crud(DocLoading.Bucket.DocOps.TOUCH, key, exp=10,
                                      durability=self.durability_level)
            if self.validate_result(result):
                continue
        throttle_limit, ru, wu = self.get_stat(self.bucket)
        self.compare_ru_wu_stat(ru, wu, expected_ru, expected_wu)

        # delete the document
        if self.durability_level != "NONE":
            expected_wu += (self.num_items * 2)
        else:
            expected_wu += self.num_items
        self.perform_operation(DocLoading.Bucket.DocOps.DELETE, key_value,
                               self.bucket, expected_wu,
                               expected_ru, durability=self.durability_level)

    def test_cu_in_batch_operation(self):
        self.log.info("Loading %s docs into bucket" % self.num_items)
        doc_gen = doc_generator(self.key, 0, self.num_items,
                                doc_size=self.doc_size)
        # create documents
        load_task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, doc_gen,
            DocLoading.Bucket.DocOps.CREATE, 0,
            batch_size=500, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            compression=self.sdk_compression,
            timeout_secs=self.sdk_timeout,
            sdk_client_pool=self.sdk_client_pool,
            print_ops_rate=False)
        self.task_manager.get_task_result(load_task)
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        _, self.ru, self.wu = self.get_stat(self.bucket)
        expected_wu = self.bucket_util.calculate_units(20, self.doc_size, 0,
                                                       durability=self.durability_level) * self.num_items
        self.compare_ru_wu_stat(self.ru, self.wu, 0, expected_wu)

        # Load with doc_ttl set
        self.log.info("Setting doc_ttl=1 for %s docs" % 10000)
        load_task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, doc_gen,
            DocLoading.Bucket.DocOps.UPDATE, exp=1,
            batch_size=2000, process_concurrency=5,
            durability=self.durability_level,
            timeout_secs=30,
            sdk_client_pool=self.sdk_client_pool,
            skip_read_on_error=True,
            print_ops_rate=False)
        self.task_manager.get_task_result(load_task)
        _, self.ru, self.wu = self.get_stat(self.bucket)
        expected_wu += self.bucket_util.calculate_units(20, self.doc_size, 0,
                                                        durability=self.durability_level) * self.num_items
        self.compare_ru_wu_stat(self.ru, self.wu, 0, expected_wu)

        self.sleep(2)
        # Read task to trigger expiry_purger
        load_task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, doc_gen,
            DocLoading.Bucket.DocOps.READ,
            batch_size=500, process_concurrency=8,
            timeout_secs=30,
            sdk_client_pool=self.sdk_client_pool,
            suppress_error_table=True,
            start_task=False,
            print_ops_rate=False)
        self.task_manager.add_new_task(load_task)
        self.task_manager.get_task_result(load_task)
        _, self.ru, self.wu = self.get_stat(self.bucket)
        expected_wu += self.num_items
        self.compare_ru_wu_stat(self.ru, self.wu, 0, expected_wu)

    def validate_result(self, result):
        if result["status"] is False:
            self.log.critical("%s Loading failed: %s" % result)
            return False
        return True

    def get_sizeof_document(self, key, doc_gen="", xattr=False):
        result = self.client.crud(DocLoading.Bucket.DocOps.READ, key)
        size = len(result["key"]) + len(result["value"])
        ru = self.bucket_util.calculate_units(size, 0, read=True)
        if xattr:
            key_value = []
            key, val = next(doc_gen)
            key_value.append(Tuples.of(key, val))
            success, _ = self.client.sub_doc_read_multi(key_value,
                                                        xattr=xattr)
            if success:
                if success[key]["value"][0]:
                    size += len(success[key]["value"][0]) + len(key)
            ru = self.bucket_util.calculate_units(size, 0, read=True) * 2
        return size, ru

    def test_cu_in_subdoc_operations(self):
        self.bucket = self.bucket_util.get_all_buckets(self.cluster)[0]
        self.xattr = self.input.param("xattr", False)
        self.system_xattr = self.input.param("system_xattr", False)
        sub_doc_key = "my-attr"
        if self.system_xattr:
            sub_doc_key = "my._attr"

        # create few documents
        key_value = self.get_key_value(self.num_items, self.doc_size)
        self.expected_wu = self.bucket_util.calculate_units(self.doc_size, 0,
                                                            durability=self.durability_level) * self.num_items
        self.perform_operation(DocLoading.Bucket.DocOps.CREATE, key_value, self.bucket,
                               self.expected_wu, 0, durability=self.durability_level)
        _, self.expected_ru, self.expected_wu = self.get_stat(self.bucket)
        self.total_size, ru = self.get_sizeof_document("metering-0")
        self.expected_ru += ru
        self.total_size += self.sub_doc_size

        # subdoc operations with system xattrs
        for sub_doc_op in ["subdoc_insert", "subdoc_upsert", "subdoc_replace"]:
            value = random.choice(string.ascii_letters) * self.sub_doc_size
            for key in key_value.keys():
                _, failed_items = self.client.crud(sub_doc_op, key,
                                                   [sub_doc_key, value],
                                                   durability=self.durability_level,
                                                   timeout=self.sdk_timeout,
                                                   time_unit="seconds",
                                                   create_path=self.xattr,
                                                   xattr=self.xattr)
                self.assertFalse(failed_items, "Subdoc Xattr operation failed")
            self.expected_wu += (self.bucket_util.calculate_units(self.total_size, 0,
                                                                  durability=self.durability_level) * self.num_items)
            _, self.ru, self.wu = self.get_stat(self.bucket)
            self.compare_ru_wu_stat(self.ru, self.wu, self.expected_ru, self.expected_wu)

        # delete a file with system xattrs, both ru and wu will increase
        self.log.info("performing delete")
        if self.durability_level != "NONE":
            self.expected_wu += (self.num_items * 2)
        else:
            self.expected_wu += self.num_items
        self.perform_operation(DocLoading.Bucket.DocOps.DELETE, key_value,
                               self.bucket, self.expected_wu,
                               self.expected_ru, durability=self.durability_level)

    #############################################################################
    def test_metering_steady_state(self):
        """
        Test Focus: Check WU and RU count after
                    different doc ops
        STEPS:
          -- Create n items
          -- Validate n items
          -- Check for WU and RU count
          -- Different doc ops(passed from test conf)
             and validate wu and ru count
        """
        self.PrintStep(" Step 1: Initial loading with new loader starts")
        self.create_start = 0
        self.create_end = self.num_items
        self.perform_load(validate_data=False)
        expected_wu = self.bucket_util.calculate_units(self.key_size, self.doc_size,
                                           num_items=self.create_end - self.create_start,
                                                       durability=self.durability_level)
        expected_ru = self.bucket_util.calculate_units(self.key_size, self.doc_size,
                                           read=True, num_items=self.read_end - self.read_start,
                                                       durability=self.durability_level)
        for bucket in self.cluster.buckets:
            _, self.ru, self.wu = self.bucket_util.get_stat_from_metrics(bucket)
            msg = "bucket = {}, expected_wu {} != mcstats wu {}".format(bucket.name, expected_wu, self.wu)
            self.assertEqual(self.wu, expected_wu, msg)
        count = 0
        while count < self.test_itr:
            msg = "Step {}: Starting doc_ops == {}".format(count, self.doc_ops)
            self.PrintStep(msg)
            self.log.info("Create_start=={}, create_End=={}, read_start=={},"
                          "read_end=={}, update_Start=={}, update_End=={} \
            del_start=={}, del_end=={}".format(self.create_start, self.create_end, self.read_start,
                                               self.read_end, self.update_start, self.update_end,
                                               self.delete_start, self.delete_end))
            self.create_start += self.num_items
            self.compute_docs_ranges(start=self.create_end, doc_ops=self.doc_ops)
            self.log.info("create_perc {}, update_perc {}, read_perc {}".format(self.create_perc,
                                                                                self.update_perc,
                                                                                self.read_perc))
            self.log.info("Create_start=={}, create_End=={}, read_start=={}, read_end=={},"
                          "update_Start=={}, update_End=={} del_start=={}, "
                          "del_end=={}".format(self.create_start, self.create_end,
                                               self.read_start, self.read_end,
                                               self.update_start, self.update_end,
                                               self.delete_start, self.delete_end))
            self.perform_load(validate_data=False)
            count += 1
            if "update" in self.doc_ops:
                expected_wu += self.bucket_util.calculate_units(self.key_size, self.doc_size,
                                                    num_items=self.update_end - self.update_start,
                                                                durability=self.durability_level)

            if "delete" in self.doc_ops:
                expected_wu += self.bucket_util.calculate_units(self.key_size, self.doc_size,
                                                    num_items = self.delete_end - self.delete_start,
                                                                durability=self.durability_level)

            if "create" in self.doc_ops:
                expected_wu += self.bucket_util.calculate_units(self.key_size, self.doc_size,
                                                    num_items = self.create_end - self.create_start,
                                                                durability=self.durability_level)

            if "expiry" in self.doc_ops:
                self.sleep(self.maxttl, "Wait for docs to expire")
                self.bucket_util._expiry_pager(self.cluster, self.exp_pager_stime)
                self.sleep(self.exp_pager_stime, "Wait until exp_pager_stime for kv_purger\
             to kickoff")
                self.sleep(self.exp_pager_stime*30, "Wait for KV purger to scan expired docs and add \
            tombstones.")
                expected_wu += self.bucket_util.calculate_units(self.key_size, self.doc_size,
                                                    num_items = self.expiry_end - self.expiry_start,
                                                                durability=self.durability_level)
            if "read" in self.doc_ops:
                expected_ru += self.bucket_util.calculate_units(self.key_size, self.doc_size,
                                                    num_items = self.read_end - self.read_start,
                                                                durability=self.durability_level)
            self.log.info("Expected wu after doc ops == {}".format(expected_wu))
            for bucket in self.cluster.buckets:
                _, self.ru, self.wu = self.bucket_util.get_stat_from_metrics(bucket)
                msg = "bucket = {}, expected_ru {} != mcstats ru {}".format(bucket.name, expected_ru, self.ru)
                self.assertEqual(self.ru, expected_ru, msg)
                msg = "bucket = {}, expected_wu {} != mcstats wu {}".format(bucket.name, expected_wu, self.wu)
                self.assertEqual(self.wu, expected_wu, msg)

    def test_ru_after_multi_get_ops(self):
        self.PrintStep(" Step 1: Initial loading with new loader starts")
        self.create_start = 0
        self.create_end = self.num_items
        self.perform_load(validate_data=False)

        self.num_read_threads = self.input.param("num_read_threads", 4)
        self.compute_docs_ranges(doc_ops="read")
        self.PrintStep(" Step 2: Get Ops using multiple threads")
        temp_tasks= list()
        for _ in range(self.num_read_threads):
            task = self.perform_load(wait_for_load=False, validate_data=False)
            temp_tasks.extend(task)
        self.wait_for_doc_load_completion(temp_tasks)
        expected_ru = self.num_read_threads * (self.bucket_util.calculate_units(self.key_size,
                                                                    self.doc_size,
                                                                    read=True,
                                                                    num_items=self.num_items,
                                                                    durability=self.durability_level))
        _, self.ru, _ = self.get_stat(self.bucket)
        msg = "expected_ru {} != mcstats ru {}".format(expected_ru, self.ru)
        self.assertEqual(self.ru, expected_ru, msg)

    def test_metering_after_rollback(self):
        '''
         -- Load bucket with num_items
         -- Verify wu count
         -- Stop persistence on a node
         -- Start load on master node(say Node A)
         -- Kill MemCached on master node(Node A)
         -- Trigger roll back on other/replica nodes
         -- ReStart persistence on master node
        '''
        self.generate_docs(doc_ops="create",
                           create_start=0,
                           create_end=self.num_items)
        _ = self.loadgen_docs(self.retry_exceptions,
                              self.ignore_exceptions,
                              _sync=True)
        self.log.info("Waiting for ep-queues to get drained")
        self.bucket_util._wait_for_stats_all_buckets(
            self.cluster, self.cluster.buckets, timeout=3600)

        expected_wu = self.bucket_util.calculate_units(self.key_size, self.doc_size,
                                           num_items=self.create_end - self.create_start,
                                                       durability=self.durability_level)
        _, self.ru, self.wu = self.get_stat(self.bucket)
        msg = "expected_wu {} != mcstats wu {}".format(expected_wu, self.wu)
        self.log.info(msg)
        self.assertEqual(self.wu, expected_wu, msg)

        mem_only_items = self.input.param("rollback_items", 10000)
        ops_len = len(self.doc_ops)
        self.assertTrue(self.rest.update_autofailover_settings(False, 600),
                        "AutoFailover disabling failed")

        if self.nodes_init < 2 or self.num_replicas < 1:
            self.fail("Not enough nodes/replicas in the cluster/bucket \
            to test rollback")

        self.num_rollbacks = self.input.param("num_rollbacks", 2)

        shell = RemoteMachineShellConnection(self.cluster.master)
        cbstats = Cbstats(self.cluster.master)
        self.target_vbucket = cbstats.vbucket_list(self.cluster.buckets[0].name)

        #######################################################################
        '''
        STEP - 2,  Stop persistence on master node
        '''
        for i in range(1, self.num_rollbacks+1):
            self.log.info("Roll back Iteration == {}".format(i))

            mem_item_count = 0

            # Stopping persistence on NodeA
            self.log.debug("Iteration == {}, stopping persistence".format(i))
            Cbepctl(shell).persistence(self.cluster.buckets[0].name, "stop")

            ###################################################################
            '''
            STEP - 3
              -- Doc ops on master node
            '''
            self.log.info("Just before compute docs, iteration {}".format(i))
            self.create_start = self.num_items
            self.create_end = mem_only_items
            self.gen_create = None
            self.gen_update = None
            self.gen_delete = None
            self.gen_expiry = None
            mem_item_count += mem_only_items * ops_len
            self.generate_docs(doc_ops="create",
                               target_vbucket=self.target_vbucket)
            self.loadgen_docs(_sync=True,
                              retry_exceptions=self.retry_exceptions)

            ep_queue_size_map = {self.cluster.nodes_in_cluster[0]:
                                 mem_item_count}
            vb_replica_queue_size_map = {self.cluster.nodes_in_cluster[0]: 0}

            for node in self.cluster.nodes_in_cluster[1:]:
                ep_queue_size_map.update({node: 0})
                vb_replica_queue_size_map.update({node: 0})

            ###################################################################
            '''
            STEP - 4
              -- Kill Memcached on master node(Node A) and trigger rollback on 
              replica/other nodes
            '''

            shell.kill_memcached()

            self.assertTrue(self.bucket_util._wait_warmup_completed(
                self.cluster.buckets[0],
                servers=[self.cluster.master],
                wait_time=self.wait_timeout * 10))

            ###################################################################
            '''
            STEP -5
              -- Restarting persistence on master node(Node A)
            '''

            self.log.debug("Iteration=={}, Re-Starting persistence".format(i))
            Cbepctl(shell).persistence(self.cluster.buckets[0].name, "start")
            self.sleep(5, "Iteration=={}, sleep after restarting persistence".format(i))
            ###################################################################
            '''
            STEP - 6
              -- Verify wu count after rollback
            '''
            _, self.ru, self.wu = self.get_stat(self.bucket)
            self.assertEqual(self.wu, expected_wu, msg)

        shell.disconnect()

    def check_ru_wu_for_transaction(self):
        if self.validate_stat:
            num_throttled, ru, wu = self.get_stat(self.bucket)
            if self.throttling:
                self.expected_num_throttled += self.num_items
                if num_throttled < self.expected_num_throttled:
                    self.fail("Actual num_throttled:%s, expected num_throttled:%s,"
                                 % (num_throttled, self.expected_num_throttled))

            if ru < self.expected_ru or wu < self.expected_wu:
                self.fail("Actual ru:%s, expected ru:%s,"
                              "Actual wu:%s, expected_wu:%s"
                              % (ru, self.expected_ru, wu, self.expected_wu))
            self.expected_num_throttled, self.expected_ru, self.expected_wu = \
                num_throttled, ru, wu

    def test_metering_transactions(self):
        """
        # create a transaction, perform CRUD on documents,
            commit/rollback the transaction,
            validate the ru, wu
        """
        self.docs = []
        self.keys = []
        self.expected_num_throttled = 0
        self.throttling = self.input.param("throttling", False)
        write_units = self.bucket_util.calculate_units(15, self.doc_size,
                                                       durability=self.durability_level) * 3
        read_units = self.bucket_util.calculate_units(15, self.doc_size, read=True) * 3
        self.sdk_timeout = (write_units / self.bucket_throttling_limit) + 10
        gen_create = doc_generator("throttling", 0, self.num_items,
                                   doc_size=self.doc_size,
                                   mutation_type="create",
                                   randomize_value=True)
        while gen_create.has_next():
            key, val = next(gen_create)
            doc = Tuples.of(key, val)
            self.docs.append(doc)
            self.keys.append(key)
        self.transaction = self.create_Transaction(self.client)

        # create, update and delete docs in transactions
        self.transaction_operations(self.client, create_docs=self.docs,
                                    commit=True)
        self.expected_ru = 1
        self.expected_wu = write_units * len(self.docs)
        self.check_ru_wu_for_transaction()

        # update
        self.transaction_operations(self.client, update_docs=self.keys,
                                    commit=self.transaction_commit)
        self.expected_ru += read_units * len(self.keys)
        self.expected_wu += write_units * len(self.keys)
        self.check_ru_wu_for_transaction()

        # delete
        self.transaction_operations(self.client, delete_docs=self.keys,
                                    commit=self.transaction_commit)
        self.expected_ru += read_units * len(self.keys)
        self.expected_wu += write_units / 2 * len(self.keys)
        self.check_ru_wu_for_transaction()

        if not self.transaction_commit:
            for key in self.keys:
                self.client.crud(DocLoading.Bucket.DocOps.DELETE, key)
            self.expected_wu += len(self.keys)

        # create, update, delete
        size = len(self.keys) / 2
        self.transaction_operations(self.client, create_docs=self.docs,
                                    update_docs=self.keys[:size],
                                    delete_docs=self.keys[size:],
                                    commit=self.transaction_commit)
        self.expected_ru += 4 + (read_units * size)
        self.expected_wu += write_units * len(self.docs)
        self.check_ru_wu_for_transaction()

    def test_metering_delete_collection(self):
        self.bucket = self.bucket_util.get_all_buckets(self.cluster)[0]
        # create a scope
        scope_name = "my_scope"
        self.log.info("Creating scope '%s'" % scope_name)
        self.bucket_util.create_scope(self.cluster.master,
                                      self.bucket,
                                      {"name": scope_name})
        # create a collection
        collection_name = "my_collection"
        self.bucket_util.create_collection(self.cluster.master,
                                           self.bucket,
                                           scope_name,
                                           {"name": collection_name})

        # load data to the scope and collection
        gen_add = doc_generator(self.key, 0, self.num_items)
        task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_add, "create", 0,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            compression=self.sdk_compression,
            timeout_secs=self.sdk_timeout,
            scope=scope_name,
            collection=collection_name)
        self.task_manager.get_task_result(task)
        # check metering
        storage, num_throttled, ru, wu = self.get_stat_from_prometheus(self.bucket)
        self.get_storage_from_node(self.bucket)
        # delete the collection
        self.bucket_util.drop_collection(self.cluster.master,
                                           self.bucket,
                                           scope_name,
                                           collection_name)
        # perform load again in the bucket, check if throttling is happening
        task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, gen_add, "create", 0,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            durability=self.durability_level,
            compression=self.sdk_compression,
            timeout_secs=self.sdk_timeout)
        self.task_manager.get_task_result(task)
        storage, num_throttled, ru, wu = self.get_stat_from_prometheus(self.bucket)
        self.log.info("add validation check")

    def test_limits_boundary_values(self):
        """ throttling limit = -1 to 2147483647
            storage limit = -1 to 2147483647
        """
        def check_error_msg(status, output, storagelimit=False):
            import json
            if status == False:
                content = json.loads(output)["errors"]
                if storagelimit:
                    actual_error = content["dataStorageLimit"]
                    expected_error = '"dataStorageLimit" must be an integer between -1 and 100000'
                else:
                    actual_error = content["dataThrottleLimit"]
                    expected_error = '"dataThrottleLimit" must be an integer between -1 and 2147483647'
                self.assertEqual(actual_error, expected_error)
            else:
                self.fail("expected to fail but passsed")

        bucket = self.bucket_util.get_all_buckets(self.cluster)[0]
        server = random.choice(bucket.servers)
        bucket_helper = BucketHelper(server)
        status, content = bucket_helper.set_throttle_n_storage_limit(bucket.name,
                                                                     throttle_limit=-2)
        check_error_msg(status, content)
        status, content = bucket_helper.set_throttle_n_storage_limit(bucket.name,
                                                                     throttle_limit=2147483648)
        check_error_msg(status, content)

        status, content = bucket_helper.set_throttle_n_storage_limit(bucket.name,
                                                                     storage_limit=-2)
        check_error_msg(status, content, True)
        status, content = bucket_helper.set_throttle_n_storage_limit(bucket.name,
                                                                     storage_limit=2147483648)
        check_error_msg(status, content, True)

        status, content = bucket_helper.set_throttle_n_storage_limit(bucket.name,
                                                                     throttle_limit=-2,
                                                                     storage_limit=-2)
        check_error_msg(status, content)
        check_error_msg(status, content, True)
        status, content = bucket_helper.set_throttle_n_storage_limit(bucket.name,
                                                                     throttle_limit=2147483648,
                                                                     storage_limit=2147483648)
        check_error_msg(status, content)
        check_error_msg(status, content, True)

    def thread_change_limit(self, bucket, throttling_limit, storage_limit):
        self.sleep(20)
        self.bucket_util.set_throttle_n_storage_limit(bucket, throttling_limit, storage_limit)
        self.assertEqual(self.bucket_util.get_throttle_limit(bucket), throttling_limit)

    def test_zero_limits(self):
        bucket = self.bucket_util.get_all_buckets(self.cluster)[0]
        for i in [1, 2]:
            if i == 1:
                self.bucket_util.set_throttle_n_storage_limit(bucket, throttle_limit=0)
                gen_add = doc_generator(self.key, 0, 100)
                self.expected_wu = self.bucket_util.calculate_units(15, self.doc_size, num_items=100)
            else:
                self.bucket_util.set_throttle_n_storage_limit(bucket, storage_limit=0)
                gen_add = doc_generator(self.key, 100, 200)
                self.expected_wu += self.bucket_util.calculate_units(15, self.doc_size, num_items=100)
            thread = threading.Thread(target=self.thread_change_limit, args=(bucket, 5000, 10))
            thread.start()
            task = self.task.async_load_gen_docs(
                self.cluster, bucket, gen_add, "create", 0,
                batch_size=10, process_concurrency=8,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=self.durability_level,
                compression=self.sdk_compression,
                timeout_secs=self.sdk_timeout)
            thread.join()
            self.task_manager.get_task_result(task)
            num_throttled, ru, wu = self.bucket_util.get_stat_from_metrics(bucket)
            self.assertEqual(self.expected_wu, wu)

