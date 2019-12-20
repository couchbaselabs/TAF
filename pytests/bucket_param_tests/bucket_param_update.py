from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import doc_generator
from BucketLib.BucketOperations import BucketHelper
from sdk_exceptions import SDKException


class BucketParamTest(BaseTestCase):
    def setUp(self):
        super(BucketParamTest, self).setUp()
        self.new_replica = self.input.param("new_replica", 1)
        self.key = 'test_docs'.rjust(self.key_size, '0')
        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend(
            [self.cluster.master] + nodes_init)
        self.bucket_util.create_default_bucket(
            replica=self.num_replicas, compression_mode=self.compression_mode,
            storage=self.bucket_storage)
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
                  doc_create, doc_delete, suppress_error_table=False,
                  ignore_exceptions=[], retry_exceptions=[]):
        tasks_info = dict()
        if "create" in doc_ops:
            # Start doc create task in parallel with replica_update
            tasks_info.update(self.bucket_util._async_load_all_buckets(
                self.cluster, doc_create, "create", exp=0,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                batch_size=10, process_concurrency=8,
                ignore_exceptions=ignore_exceptions,
                retry_exceptions=retry_exceptions,
                suppress_error_table=suppress_error_table))
            doc_count += (doc_create.end - doc_create.start)
            start_doc_for_insert += self.num_items
        if "update" in doc_ops:
            # Start doc update task in parallel with replica_update
            tasks_info.update(self.bucket_util._async_load_all_buckets(
                self.cluster, doc_update, "update", exp=0,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                batch_size=10, process_concurrency=8,
                ignore_exceptions=ignore_exceptions,
                retry_exceptions=retry_exceptions,
                suppress_error_table=suppress_error_table))
        if "delete" in doc_ops:
            # Start doc update task in parallel with replica_update
            tasks_info.update(self.bucket_util._async_load_all_buckets(
                self.cluster, doc_delete, "delete", exp=0,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                batch_size=10, process_concurrency=8,
                ignore_exceptions=ignore_exceptions,
                retry_exceptions=retry_exceptions,
                suppress_error_table=suppress_error_table))
            doc_count -= (doc_delete.end - doc_delete.start)

        return tasks_info, doc_count, start_doc_for_insert

    def doc_ops_operations(self, doc_ops, start_doc_for_insert, doc_count,
                           doc_update, doc_create, doc_delete,
                           suppress_error_table=False,
                           ignore_exceptions=[], retry_exceptions=[]):
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
                suppress_error_table=suppress_error_table,
                ignore_exceptions=ignore_exceptions,
                retry_exceptions=retry_exceptions)

        return tasks, doc_count, start_doc_for_insert

    def generic_replica_update(self, doc_count, doc_ops, bucket_helper_obj,
                               replicas_to_update, start_doc_for_insert):
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

            d_impossible_exception = \
                SDKException.DurabilityImpossibleException
            ignore_exceptions = list()
            retry_exceptions = [SDKException.DurabilityAmbiguousException,
                                SDKException.TimeoutException]

            suppress_error_table = False
            if self.def_bucket.replicaNumber == 3 or replica_num == 3:
                doc_ops = "update"
                suppress_error_table = True
                ignore_exceptions = [d_impossible_exception]+retry_exceptions
                retry_exceptions = list()
            else:
                retry_exceptions.append(d_impossible_exception)

            tasks, doc_count, start_doc_for_insert = self.doc_ops_operations(
                doc_ops,
                start_doc_for_insert,
                doc_count,
                doc_update,
                doc_create,
                doc_delete,
                suppress_error_table=suppress_error_table,
                retry_exceptions=retry_exceptions,
                ignore_exceptions=ignore_exceptions)

            # Start rebalance task with doc_ops in parallel
            rebalance = self.task.async_rebalance(self.cluster.servers, [], [])
            self.sleep(10, "Wait for rebalance to start")

            # Wait for all tasks to complete
            self.task.jython_task_manager.get_task_result(rebalance)
            if self.atomicity:
                for task in tasks:
                    self.task.jython_task_manager.get_task_result(task)
            else:
                # Wait for doc_ops to complete and retry & validate failures
                self.bucket_util.verify_doc_op_task_exceptions(tasks,
                                                               self.cluster)
                self.bucket_util.log_doc_ops_task_failures(tasks)

                for _, task_info in tasks.items():
                    self.assertFalse(
                        task_info["ops_failed"],
                        "Doc ops failure during %s" % task_info["op_type"])

            # Assert if rebalance failed
            self.assertTrue(rebalance.result,
                            "Rebalance failed after replica update")

            suppress_error_table = False
            if replica_num == 3:
                suppress_error_table = True

            ignore_exceptions = list()
            if replica_num == 3:
                ignore_exceptions.append(
                    SDKException.DurabilityImpossibleException)

            self.log.info("Performing doc_ops(update) after rebalance")
            tasks, _, _ = self.doc_ops_operations(
                "update",
                start_doc_for_insert,
                doc_count,
                doc_update,
                doc_create,
                doc_delete,
                suppress_error_table=suppress_error_table,
                ignore_exceptions=ignore_exceptions)

            if self.atomicity:
                for task in tasks:
                    self.task.jython_task_manager.get_task_result(task)
            else:
                # Wait for doc_ops to complete and retry & validate failures
                self.bucket_util.verify_doc_op_task_exceptions(tasks,
                                                               self.cluster)
                self.bucket_util.log_doc_ops_task_failures(tasks)

                for task, task_info in tasks.items():
                    if replica_num == 3:
                        self.assertTrue(
                            len(task.fail.keys()) == (self.num_items/2),
                            "Few doc_ops succeeded")
                    self.assertFalse(
                        task_info["ops_failed"],
                        "Doc update failed after replica update rebalance")

            # Update the bucket's replica number
            self.def_bucket.replicaNumber = replica_num

            # Verify doc load count after each mutation cycle
            if not self.atomicity:
                self.bucket_util._wait_for_stats_all_buckets()
                self.bucket_util.verify_stats_all_buckets(doc_count)
        return doc_count, start_doc_for_insert

    def test_replica_update(self):
        if self.atomicity:
            replica_count = 3
        else:
            replica_count = 4
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
            range(1, min(replica_count, self.nodes_init)),
            start_doc_for_insert)

        # Replica decrement tests
        _, _ = self.generic_replica_update(
            doc_count,
            doc_ops,
            bucket_helper,
            range(min(replica_count, self.nodes_init)-2, -1, -1),
            start_doc_for_insert)

    def test_MB_34947(self):
        # Update already Created docs with async_writes
        load_gen = doc_generator(self.key, 0, self.num_items,
                                 doc_size=self.doc_size,
                                 doc_type=self.doc_type,
                                 vbuckets=self.vbuckets)
        task = self.task.async_load_gen_docs(
            self.cluster, self.def_bucket, load_gen, "update", 0,
            persist_to=self.persist_to, replicate_to=self.replicate_to,
            timeout_secs=self.sdk_timeout,
            batch_size=10, process_concurrency=8)
        self.task.jython_task_manager.get_task_result(task)

        # Update bucket replica to new value
        bucket_helper = BucketHelper(self.cluster.master)
        bucket_helper.change_bucket_props(
            self.def_bucket, replicaNumber=self.new_replica)
        self.bucket_util.print_bucket_stats()

        # Start rebalance task
        rebalance = self.task.async_rebalance(self.cluster.servers, [], [])
        self.sleep(10, "Wait for rebalance to start")

        # Wait for rebalance task to complete
        self.task.jython_task_manager.get_task_result(rebalance)

        # Assert if rebalance failed
        self.assertTrue(rebalance.result,
                        "Rebalance failed after replica update")
