from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import doc_generator
from BucketLib.BucketOperations import BucketHelper


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

        doc_create = doc_generator(self.key, 0, self.num_items,
                                   doc_size=self.doc_size,
                                   doc_type=self.doc_type,
                                   vbuckets=self.vbuckets)
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
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)
        self.log.info("==========Finished Bucket_param_test setup========")

    def tearDown(self):
        super(BucketParamTest, self).tearDown()

    def generic_replica_update(self, doc_count, doc_ops, bucket_helper_obj,
                               replicas_to_update, start_doc_for_insert):
        def_bucket = self.bucket_util.get_all_buckets()[0]

        for replica_num in replicas_to_update:
            tasks = list()
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
                def_bucket.name, replicaNumber=replica_num)
            self.bucket_util.print_bucket_stats()

            if "create" in doc_ops:
                # Start doc create task in parallel with replica_update
                tasks.append(self.task.async_load_gen_docs(
                    self.cluster, def_bucket, doc_create, "create", 0,
                    persist_to=self.persist_to, replicate_to=self.replicate_to,
                    durability=self.durability_level,
                    timeout_secs=self.sdk_timeout,
                    batch_size=10, process_concurrency=8))
                doc_count += (doc_create.end - doc_create.start)
                start_doc_for_insert += self.num_items
            if "update" in doc_ops:
                # Start doc update task in parallel with replica_update
                tasks.append(self.task.async_load_gen_docs(
                    self.cluster, def_bucket, doc_update, "update", 0,
                    persist_to=self.persist_to, replicate_to=self.replicate_to,
                    durability=self.durability_level,
                    timeout_secs=self.sdk_timeout,
                    batch_size=10, process_concurrency=8))
            if "delete" in doc_ops:
                # Start doc update task in parallel with replica_update
                tasks.append(self.task.async_load_gen_docs(
                    self.cluster, def_bucket, doc_delete, "delete", 0,
                    persist_to=self.persist_to, replicate_to=self.replicate_to,
                    durability=self.durability_level,
                    timeout_secs=self.sdk_timeout,
                    batch_size=10, process_concurrency=8))
                doc_count -= (doc_delete.end - doc_delete.start)

            # Start rebalance task with doc_ops in parallel
            rebalance = self.task.async_rebalance(self.cluster.servers, [], [])
            self.sleep(5, "Wait for rebalance to start")

            # Wait for all tasks to complete
            doc_ops_failed = False
            self.task.jython_task_manager.get_task_result(rebalance)
            for task in tasks:
                self.task.jython_task_manager.get_task_result(task)
                if len(task.fail.keys()) != 0:
                    doc_ops_failed = True

            if doc_ops_failed:
                self.assertFalse("Few doc_ops failed")

            # Assert if rebalance failed
            self.assertTrue(rebalance.result,
                            "Rebalance failed after replica update")

            self.log.info("Performing doc_ops after rebalance operation")
            update_task = self.task.async_load_gen_docs(
                self.cluster, def_bucket, doc_update, "update", 0,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                batch_size=10, process_concurrency=8)
            self.task_manager.get_task_result(update_task)

            if len(update_task.fail.keys()) != 0:
                self.fail("Update failed after rebalance for replica update")

            # Update the bucket's replica number
            def_bucket.replicaNumber = replica_num

            # Verify doc load count after each mutation cycle
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
