from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import doc_generator
from BucketLib.BucketOperations import BucketHelper
from couchbase_helper.durability_helper import DurabilityHelper, \
    DurableExceptions
from membase.api.rest_client import RestConnection
from sdk_client3 import SDKClient


class BucketParamTest(BaseTestCase):

    def setUp(self):
        super(BucketParamTest, self).setUp()
        self.key = 'test_docs'.rjust(self.key_size, '0')
        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend(
            [self.cluster.master] + nodes_init)
        self.bucket_util.create_default_bucket(
            replica=self.num_replicas, compression_mode=self.compression_mode)
        self.bucket_util.add_rbac_user()
        self.src_bucket = self.bucket_util.get_all_buckets()

        # Reset active_resident_threshold to avoid further data load as DGM
        self.active_resident_threshold = 0
        self.def_bucket = self.bucket_util.get_all_buckets()[0]

        doc_create = doc_generator(self.key, 0, self.num_items,
                                   doc_size=self.doc_size,
                                   doc_type=self.doc_type,
                                   vbuckets=self.vbuckets)

        if self.atomicity:
            task = self.task.async_load_gen_docs_atomicity(
                self.cluster, self.bucket_util.buckets, doc_create,
                "create", 0, batch_size=20, process_concurrency=8,
                replicate_to=self.replicate_to,
                persist_to=self.persist_to,
                timeout_secs=self.sdk_timeout,
                transaction_timeout=self.transaction_timeout,
                commit=self.transaction_commit,
                durability=self.durability_level,
                sync=self.sync)
            self.task.jython_task_manager.get_task_result(task)
        else:
            for bucket in self.bucket_util.buckets:
                task = self.task.async_load_gen_docs(
                    self.cluster, bucket, doc_create, "create", 0,
                    persist_to=self.persist_to, replicate_to=self.replicate_to,
                    durability=self.durability_level,
                    timeout_secs=self.sdk_timeout,
                    batch_size=10, process_concurrency=8)
                self.task.jython_task_manager.get_task_result(task)

        self.cluster_util.print_cluster_stats()
        self.bucket_util.print_bucket_stats()

        # Verify initial doc load count
        if not self.atomicity:
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(self.num_items)
        self.log.info("==========Finished Bucket_param_test setup========")

    def tearDown(self):
        super(BucketParamTest, self).tearDown()

    def load_docs_atomicity(self, doc_ops, start_doc_for_insert, doc_count,
                            doc_update, doc_create, doc_delete):
        tasks = []
        if "update" in doc_ops:
            tasks.append(
                self.task.async_load_gen_docs_atomicity(
                    self.cluster, self.bucket_util.buckets, doc_update,
                    "rebalance_only_update", 0, batch_size=20,
                    process_concurrency=8,
                    replicate_to=self.replicate_to,
                    persist_to=self.persist_to,
                    timeout_secs=self.sdk_timeout,
                    transaction_timeout=self.transaction_timeout,
                    update_count=self.update_count,
                    commit=self.transaction_commit,
                    durability=self.durability_level,
                    sync=self.sync))
            self.sleep(10, "To avoid overlap of multiple tasks in parallel")
        if "create" in doc_ops:
            tasks.append(
                self.task.async_load_gen_docs_atomicity(
                    self.cluster, self.bucket_util.buckets, doc_create,
                    "create", 0, batch_size=20,
                    process_concurrency=8,
                    replicate_to=self.replicate_to,
                    persist_to=self.persist_to,
                    timeout_secs=self.sdk_timeout,
                    transaction_timeout=self.transaction_timeout,
                    commit=self.transaction_commit,
                    durability=self.durability_level,
                    sync=self.sync))
            doc_count += (doc_create.end - doc_create.start)
            start_doc_for_insert += self.num_items
        if "delete" in doc_ops:
            tasks.append(
                self.task.async_load_gen_docs_atomicity(
                    self.cluster, self.bucket_util.buckets, doc_delete,
                    "rebalance_delete", 0, batch_size=20,
                    process_concurrency=8,
                    replicate_to=self.replicate_to,
                    persist_to=self.persist_to,
                    timeout_secs=self.sdk_timeout,
                    transaction_timeout=self.transaction_timeout,
                    commit=self.transaction_commit,
                    durability=self.durability_level,
                    sync=self.sync))
            doc_count -= (doc_delete.end - doc_delete.start)

        return tasks, doc_count, start_doc_for_insert

    def load_docs(self, doc_ops, start_doc_for_insert, doc_count, doc_update,
                  doc_create, doc_delete, suppress_error_table=False):
        tasks = []
        if "create" in doc_ops:
            # Start doc create task in parallel with replica_update
            tasks.append(self.task.async_load_gen_docs(
                self.cluster, self.def_bucket, doc_create, "create", 0,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                batch_size=10, process_concurrency=8,
                suppress_error_table=suppress_error_table))
            doc_count += (doc_create.end - doc_create.start)
            start_doc_for_insert += self.num_items
        if "update" in doc_ops:
            # Start doc update task in parallel with replica_update
            tasks.append(self.task.async_load_gen_docs(
                self.cluster, self.def_bucket, doc_update, "update", 0,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                batch_size=10, process_concurrency=8,
                suppress_error_table=suppress_error_table))
        if "delete" in doc_ops:
            # Start doc update task in parallel with replica_update
            tasks.append(self.task.async_load_gen_docs(
                self.cluster, self.def_bucket, doc_delete, "delete", 0,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                batch_size=10, process_concurrency=8,
                suppress_error_table=suppress_error_table))
            doc_count -= (doc_delete.end - doc_delete.start)

        return tasks, doc_count, start_doc_for_insert

    def doc_ops_operations(self, doc_ops, start_doc_for_insert, doc_count,
                           doc_update, doc_create, doc_delete,
                           suppress_error_table=False):
        if self.atomicity:
            tasks, doc_count, start_doc_for_insert = self.load_docs_atomicity(
                doc_ops,
                start_doc_for_insert,
                doc_count,
                doc_update,
                doc_create,
                doc_delete)
        else:
            tasks, doc_count, start_doc_for_insert = self.load_docs(
                doc_ops,
                start_doc_for_insert,
                doc_count,
                doc_update,
                doc_create,
                doc_delete,
                suppress_error_table=suppress_error_table)

        return tasks, doc_count, start_doc_for_insert

    def generic_replica_update(self, doc_count, doc_ops, bucket_helper_obj,
                               replicas_to_update, start_doc_for_insert):
        durability_helper = DurabilityHelper(
            self.log, len(self.cluster.nodes_in_cluster),
            self.durability_level)
        for replica_num in replicas_to_update:
            # Creating doc creator to be used by test cases
            doc_create = doc_generator(self.key, start_doc_for_insert,
                                       start_doc_for_insert + self.num_items,
                                       doc_size=self.doc_size,
                                       doc_type=self.doc_type,
                                       vbuckets=self.vbuckets)

            # Creating doc updater to be used by test cases
            doc_update = doc_generator(
                self.key,
                start_doc_for_insert - (self.num_items/2),
                start_doc_for_insert,
                doc_size=self.doc_size,
                doc_type=self.doc_type,
                vbuckets=self.vbuckets)

            # Creating doc updater to be used by test cases
            doc_delete = doc_generator(
                self.key,
                start_doc_for_insert - self.num_items,
                start_doc_for_insert - (self.num_items/2),
                doc_size=self.doc_size, doc_type=self.doc_type,
                vbuckets=self.vbuckets)

            self.log.info("Updating replica count of bucket to {0}"
                          .format(replica_num))

            bucket_helper_obj.change_bucket_props(
                self.def_bucket, replicaNumber=replica_num)
            self.bucket_util.print_bucket_stats()

            tasks, doc_count, start_doc_for_insert = self.doc_ops_operations(
                doc_ops,
                start_doc_for_insert,
                doc_count,
                doc_update,
                doc_create,
                doc_delete)

            # Start rebalance task with doc_ops in parallel
            rebalance = self.task.async_rebalance(self.cluster.servers, [], [])
            self.sleep(10, "Wait for rebalance to start")

            doc_ops_list = list()
            doc_ops_failed = False
            if "create" in doc_ops:
                doc_ops_list.append("create")
            if "update" in doc_ops:
                doc_ops_list.append("update")
            if "delete" in doc_ops:
                doc_ops_list.append("delete")

            suppress_error_table = False
            if self.def_bucket.replicaNumber == 3:
                suppress_error_table = True

            # Wait for all tasks to complete
            assert_msg = ""
            self.task.jython_task_manager.get_task_result(rebalance)
            sdk_client = SDKClient(RestConnection(self.cluster.master),
                                   self.bucket_util.buckets[0])
            for index, task in enumerate(tasks):
                self.task.jython_task_manager.get_task_result(task)
                if not self.atomicity:
                    if self.def_bucket.replicaNumber == 3:
                        if len(task.success.keys()) != 0:
                            valid = \
                                durability_helper.validate_durability_exception(
                                    task.fail,
                                    DurableExceptions.DurabilityImpossibleException)
                            if not valid:
                                doc_ops_failed = True
                                assert_msg = "Invalid exception for replica=3"
                    else:
                        durability_helper.validate_durability_exception(
                            task.fail,
                            DurableExceptions.DurabilityImpossibleException)
                        doc_ops_failed = durability_helper.retry_with_no_error(
                            sdk_client, task.fail, doc_ops_list[index],
                            timeout=30)
                        if doc_ops_failed:
                            doc_ops_failed = True
                            assert_msg = "Retry of d_impossible exception " \
                                         "failed"

            # Disconnect the SDK client before possible assert
            sdk_client.close()
            self.assertFalse(doc_ops_failed, assert_msg)

            # Assert if rebalance failed
            self.assertTrue(rebalance.result,
                            "Rebalance failed after replica update")

            suppress_error_table = False
            if replica_num == 3:
                suppress_error_table = True

            self.log.info("Performing doc_ops(update) after rebalance")
            tasks, _, _ = self.doc_ops_operations(
                "update",
                start_doc_for_insert,
                doc_count,
                doc_update,
                doc_create,
                doc_delete,
                suppress_error_table=suppress_error_table)

            assert_msg = "CRUD failed after rebalance for replica update"
            doc_ops_failed = False
            for task in tasks:
                self.task.jython_task_manager.get_task_result(task)
                if not self.atomicity:
                    if replica_num == 3:
                        if len(task.success.keys()) != 0:
                            doc_ops_failed = True
                            passed = \
                                durability_helper.validate_durability_exception(
                                    task.fail,
                                    DurableExceptions.DurabilityImpossibleException)
                            if not passed:
                                assert_msg = "Exception validation failed"
                                doc_ops_failed = True
                    elif len(task.fail.keys()) != 0:
                        doc_ops_failed = True

            self.assertFalse(doc_ops_failed, assert_msg)

            # Update the bucket's replica number
            self.def_bucket.replicaNumber = replica_num

            # Verify doc load count after each mutation cycle
            if not self.atomicity:
                self.bucket_util._wait_for_stats_all_buckets()
                self.bucket_util.verify_stats_all_buckets(doc_count)
        return doc_count, start_doc_for_insert

    def test_replica_update(self):
        if self.nodes_init < 2:
            self.log.error("Test not supported for < 2 node cluster")
            return

        doc_ops = self.input.param("doc_ops", "")
        bucket_helper = BucketHelper(self.cluster.master)

        doc_count = self.num_items
        start_doc_for_insert = self.num_items

        # Replica increment tests
        doc_count, start_doc_for_insert = self.generic_replica_update(
            doc_count,
            doc_ops,
            bucket_helper,
            range(1, min(4, self.nodes_init)),
            start_doc_for_insert)

        # Replica decrement tests
        _, _ = self.generic_replica_update(
            doc_count,
            doc_ops,
            bucket_helper,
            range(min(4, self.nodes_init)-2, -1, -1),
            start_doc_for_insert)
