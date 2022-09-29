from Cb_constants.CBServer import CbServer
from couchbase_helper.documentgenerator import doc_generator
from magma_base import MagmaBaseTest
import time
import math
from BucketLib.BucketOperations import BucketHelper


class BasicCrudTests(MagmaBaseTest):
    def setUp(self):
        super(BasicCrudTests, self).setUp()
        if not self.windows_platform:
            self.change_swap_space(self.cluster.nodes_in_cluster)
        self.generate_docs(doc_ops="update:read:delete")
        self.items = self.num_items

    def tearDown(self):
        super(BasicCrudTests, self).tearDown()

    def test_mb_52264(self):
        # control params set
        self.start = 0
        self.end = self.num_items_per_collection
        self.process_concurrency = 2
        reduced_ops_rate = self.input.param("reduced_ops_rate", 4000)
        mem_reduction_factor = self.input.param("mem_reduction_factor", 2)
        wait_timeout = self.input.param("wait_timeout", 180)
        final_quota = self.input.param("final_quota", 128)
        data_ops = self.input.param("data_ops", True)
        wait_for_ops = self.input.param("wait_for_ops", False)
        pre_delete_docs = self.input.param("pre_delete_docs", False)
        mem_reduction_wait = self.input.param("mem_reduction_wait", 120)
        delete_per_collection = self.input.param("delete_per_collection", 0)
        create_per_collection = self.input.param("create_per_collection", 0)
        update_per_collection = self.input.param("update_per_collection", 0)

        def init_loading():
            self.create_start = 0
            self.create_perc = 100
            self.create_end = self.init_items_per_collection
            self.log.info("Initial loading with new loader starts")
            self.new_loader(wait=True)
            self.sleep(5)

        def set_input_perc():
            self.create_perc = self.input.param("create_perc", 0)
            self.update_perc = self.input.param("update_perc", 0)
            self.delete_perc = self.input.param("delete_perc", 0)
            self.assertIs(
                self.create_perc + self.delete_perc + self.update_perc
                == 100, True, "ops total % should sum to 100")

        def pre_delete():
            self.update_perc = 0
            self.create_perc = 0
            self.delete_perc = 100
            self.delete_start = self.start
            self.delete_end = self.delete_start + int(
                math.floor((self.end - self.start) * 0.60))
            self.start = self.delete_end
            self.num_items_per_collection = self.end - self.start
            self.new_loader(wait=True)

        def load_from_spec():
            """ params start and end takes care of resuming
            loading that can be controlled by _per_collection input params"""
            if delete_per_collection > 0:
                self.delete_start = self.start
                self.delete_end = self.delete_start + int(
                    math.floor((self.end - self.start) * (
                        delete_per_collection/100.0)))
                self.start = self.delete_end
            if update_per_collection > 0:
                self.update_start = self.start
                self.update_end = self.update_start + int(
                    math.floor((self.end - self.start) *
                               (update_per_collection / 100.0)))
            if create_per_collection > 0:
                self.create_start = self.end
                self.create_end = self.create_start + int(math.floor(
                    (self.end - self.start) * (create_per_collection / 100.0)))
                self.end = self.create_end
            self.num_items_per_collection = self.end - self.start
            return self.new_loader(wait=wait_for_ops)

        def reduce_mem(desired_mem, timeout=180):
            """
            updates memory with given param with a timeout
            """
            self.bucket_util.update_bucket_property(self.cluster.master,
                                                    self.cluster.buckets[0],
                                                    ram_quota_mb=desired_mem)
            check_timeout = int(time.time()) + timeout
            while time.time() < check_timeout:
                if bucket_helper.get_buckets_json()[0]["basicStats"][
                    "storageTotals"]["ram"]["quotaUsedPerNode"] / 1048576 \
                        == desired_mem:
                    break
                self.sleep(1, "waiting for memory quota update")
            else:
                return False
            return True

        def reduce_mem_quota_rec(initial_mem):
            """
            recursively starts ops from input params and decreases memory
            stopping condition  -> [checking memory reached <= desired memory]
            """
            self.sleep(15, "waiting before iteration")
            ops_task = None
            if initial_mem <= final_quota:
                return True
            else:
                desired_mem = int(initial_mem -
                                  (initial_mem/mem_reduction_factor))
                self.ops_rate = reduced_ops_rate
                self.log.info("loading data for reduction from %d -> %d" % (
                    initial_mem, desired_mem))
                if pre_delete_docs:
                    pre_delete()
                if data_ops:
                    set_input_perc()
                    ops_task = load_from_spec()
                self.sleep(mem_reduction_wait, "Waiting for data load to "
                                               "make progress...")
                result = reduce_mem(desired_mem, timeout=wait_timeout)
                self.assertIs(result, True, "Memory update timed out")
                if not result:
                    return False
                if not wait_for_ops:
                    self.doc_loading_tm.getAllTaskResult()
                    self.printOps.end_task()
                    self.retry_failures(ops_task)
                return reduce_mem_quota_rec(desired_mem)
        bucket_helper = BucketHelper(self.cluster.master)

        # test execution start point
        init_loading()
        self.ops_rate = reduced_ops_rate
        self.assertIs(reduce_mem_quota_rec(self.bucket_ram_quota), True,
                      "Operation Failed")

    def test_MB_38315(self):
        self.log.info("Deleting half of the items")
        self.doc_ops = "delete"
        self.generate_docs(doc_ops=self.doc_ops,
                           delete_start=0, delete_end=self.num_items/2)
        _ = self.loadgen_docs(self.retry_exceptions,
                              self.ignore_exceptions)
        self.log.info("Verifying doc counts after create doc_ops")
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)

        tasks_info = self.bucket_util._async_validate_docs(
               self.cluster, self.gen_delete, "delete", 0,
               batch_size=self.batch_size,
               process_concurrency=self.process_concurrency,
               timeout_secs=self.sdk_timeout,
               retry_exceptions=self.retry_exceptions,
               ignore_exceptions=self.ignore_exceptions)

        for task in tasks_info:
                self.task_manager.get_task_result(task)

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
                vbuckets=self.cluster.vbuckets,
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
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)

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
                vbuckets=self.cluster.vbuckets,
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
            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets)
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
                vbuckets=self.cluster.vbuckets,
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
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.verify_stats_all_buckets(self.cluster, self.num_items)

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
            vbuckets=self.cluster.vbuckets,
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
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)

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
