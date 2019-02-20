from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import doc_generator
from BucketLib.BucketOperations_Rest import BucketHelper


class Bucket_param_test(BaseTestCase):
    def setUp(self):
        super(Bucket_param_test, self).setUp()
        self.key = 'test_docs'.rjust(self.key_size, '0')
        nodes_init = self.cluster.servers[1:self.nodes_init] if self.nodes_init != 1 else [
        ]
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend(
            [self.cluster.master] + nodes_init)
        self.bucket_util.create_default_bucket(replica=self.num_replicas,
                                               compression_mode=self.compression_mode)
        self.bucket_util.add_rbac_user()
        self.src_bucket = self.bucket_util.get_all_buckets()
        # Reset active_resident_threshold to avoid further data load as DGM
        self.active_resident_threshold = 0

        doc_create = doc_generator(self.key, 0, self.num_items,
                                   doc_size=self.doc_size,
                                   doc_type=self.doc_type,
                                   vbuckets=self.vbuckets)
        self.print_cluster_stat_task = self.cluster_util.async_print_cluster_stats()
        for bucket in self.bucket_util.buckets:
            task = self.task.async_load_gen_docs(self.cluster, bucket,
                                                 doc_create, "create", 0,
                                                 persist_to=self.persist_to,
                                                 replicate_to=self.replicate_to,
                                                 batch_size=10,
                                                 process_concurrency=8)
            self.task.jython_task_manager.get_task_result(task)
            # Verify initial doc load count
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(self.num_items)
        self.log.info("==========Finished Bucket_param_test setup========")

    def tearDown(self):
        #super(Bucket_param_test, self).tearDown()
        pass

    def generic_replica_update(self, doc_ops, bucket_helper_obj, replicas_to_update):
        update_replicateTo_persistTo = self.input.param("update_replicateTo_persistTo", False)
        def_bucket = self.bucket_util.get_all_buckets()[0]
        start_doc_for_delete = 0
        incr_val_for_delete = int(self.num_items/3)

        for replica_num in replicas_to_update:
            tasks = list()
            # Creating doc creator to be used by test cases
            doc_create = doc_generator(self.key, self.num_items,
                                       self.num_items * 2,
                                       doc_size=self.doc_size,
                                       doc_type=self.doc_type,
                                       vbuckets=self.vbuckets)

            # Creating doc updater to be used by test cases
            doc_update = doc_generator(self.key,
                                       int(self.num_items/2), self.num_items,
                                       doc_size=self.doc_size,
                                       doc_type=self.doc_type,
                                       vbuckets=self.vbuckets)

            # Creating doc updater to be used by test cases
            doc_delete = doc_generator(self.key,
                                       start_doc_for_delete,
                                       start_doc_for_delete+incr_val_for_delete,
                                       doc_size=self.doc_size,
                                       doc_type=self.doc_type,
                                       vbuckets=self.vbuckets)

            self.log.info("Updating replica count of bucket to {0}"
                          .format(replica_num))

            if update_replicateTo_persistTo:
                if self.self.durability_level is None:
                    self.replicate_to = replica_num
                    self.persist_to = replica_num

            bucket_helper_obj.change_bucket_props(def_bucket.name,
                                                  replicaNumber=replica_num)

            if "create" in doc_ops:
                # Start doc create task in parallel with replica_update
                tasks.append(self.task.async_load_gen_docs(self.cluster,
                                                           def_bucket,
                                                           doc_create, "create", 0,
                                                           persist_to=self.persist_to,
                                                           replicate_to=self.replicate_to,
                                                           batch_size=10,
                                                           process_concurrency=8))
                self.num_items *= 2
            if "update" in doc_ops:
                # Start doc update task in parallel with replica_update
                tasks.append(self.task.async_load_gen_docs(self.cluster,
                                                           def_bucket,
                                                           doc_update, "update", 0,
                                                           persist_to=self.persist_to,
                                                           replicate_to=self.replicate_to,
                                                           batch_size=10,
                                                           process_concurrency=8))
            if "delete" in doc_ops:
                # Start doc update task in parallel with replica_update
                tasks.append(self.task.async_load_gen_docs(self.cluster,
                                                           def_bucket,
                                                           doc_delete, "delete", 0,
                                                           persist_to=self.persist_to,
                                                           replicate_to=self.replicate_to,
                                                           batch_size=10,
                                                           process_concurrency=8))
                self.num_items -= (incr_val_for_delete - start_doc_for_delete)
                start_doc_for_delete += incr_val_for_delete

            # Start rebalance task with doc_ops in parallel
            tasks.append(self.task.async_rebalance(self.cluster.servers,
                                                   [], []))

            for task in tasks:
                self.task.jython_task_manager.get_task_result(task)

            # Verify doc load count after each mutation cycle
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(self.num_items)

    def test_replica_update(self):
        if self.nodes_init < 2:
            self.log.error("Test not supported for < 2 node cluster")
            return

        doc_ops = self.input.param("doc_ops", "")

        if doc_ops == "":
            doc_ops = None
        else:
            doc_ops = doc_ops.split(":")

        bucket_helper = BucketHelper(self.cluster.master)

        # Replica increment tests
        self.generic_replica_update(doc_ops, bucket_helper, range(1,
                                                                  min(4, self.nodes_init)))
        # Replica decrement tests
        self.generic_replica_update(doc_ops, bucket_helper, range(min(4, self.nodes_init),
                                                                  1, -1))
