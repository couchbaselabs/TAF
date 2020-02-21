from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import doc_generator


class Bucket_DGM_Tests(BaseTestCase):
    def setUp(self):
        super(Bucket_DGM_Tests, self).setUp()
        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend(
            [self.cluster.master] + nodes_init)
        self.bucket_util.create_default_bucket(
            ram_quota=self.bucket_size, replica=self.num_replicas,
            maxTTL=self.maxttl, compression_mode=self.compression_mode)
        self.bucket_util.add_rbac_user()

        self.cluster_util.print_cluster_stats()
        doc_create = doc_generator(
            self.key, 0, self.num_items, key_size=self.key_size,
            doc_size=self.doc_size, doc_type=self.doc_type,
            vbuckets=self.cluster_util.vbuckets)
        for bucket in self.bucket_util.buckets:
            task = self.task.async_load_gen_docs(
                self.cluster, bucket, doc_create, "create", 0,
                persist_to=self.persist_to,
                replicate_to=self.replicate_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                batch_size=10,
                process_concurrency=8)
            self.task.jython_task_manager.get_task_result(task)
            # Verify initial doc load count
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(self.num_items)
        self.log.info("========= Finished Bucket_DGM_Tests setup =======")

    def tearDown(self):
        super(Bucket_DGM_Tests, self).tearDown()

    def test_dgm_to_non_dgm(self):
        # Prepare DGM scenario
        bucket = self.bucket_util.get_all_buckets()[0]
        dgm_gen = doc_generator(
            self.key, self.num_items, self.num_items+1)
        dgm_task = self.task.async_load_gen_docs(
            self.cluster, bucket, dgm_gen, "create", 0,
            persist_to=self.persist_to,
            replicate_to=self.replicate_to,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            batch_size=10,
            process_concurrency=4,
            active_resident_threshold=self.active_resident_threshold)
        self.task_manager.get_task_result(dgm_task)
        num_items = dgm_task.doc_index

        gen_create = doc_generator(self.key, num_items,
                                   num_items+self.num_items,
                                   key_size=self.key_size,
                                   doc_size=self.doc_size,
                                   doc_type=self.doc_type,
                                   vbuckets=self.cluster_util.vbuckets)
        gen_update = doc_generator(self.key, 0, self.num_items,
                                   key_size=self.key_size,
                                   doc_size=self.doc_size,
                                   doc_type=self.doc_type,
                                   vbuckets=self.cluster_util.vbuckets)
        gen_delete = doc_generator(self.key, self.num_items, num_items,
                                   key_size=self.key_size,
                                   doc_size=self.doc_size,
                                   doc_type=self.doc_type,
                                   vbuckets=self.cluster_util.vbuckets)

        # Perform continuous updates while bucket moves from DGM->non-DGM state
        if not self.atomicity:
            tasks = list()
            tasks.append(self.task.async_load_gen_docs(
                self.cluster, bucket, gen_update, "update", 0,
                persist_to=self.persist_to,
                replicate_to=self.replicate_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                batch_size=10, process_concurrency=2))
            tasks.append(self.task.async_load_gen_docs(
                self.cluster, bucket, gen_delete, "delete", 0,
                persist_to=self.persist_to,
                replicate_to=self.replicate_to,
                durability=self.durability_level,
                batch_size=10, process_concurrency=2,
                timeout_secs=self.sdk_timeout,
                skip_read_on_error=True))
            tasks.append(self.task.async_load_gen_docs(
                self.cluster, bucket, gen_create, "create", 0,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                batch_size=10, process_concurrency=2))
            for task in tasks:
                self.task.jython_task_manager.get_task_result(task)
        else:
            task = self.task.async_load_gen_docs_atomicity(
                          self.cluster, self.bucket_util.buckets, gen_create,
                          "create", 0, batch_size=20,
                          process_concurrency=8,
                          replicate_to=self.replicate_to,
                          persist_to=self.persist_to,
                          timeout_secs=self.sdk_timeout,
                          retries=self.sdk_retries,
                          transaction_timeout=self.transaction_timeout,
                          commit=self.transaction_commit,
                          durability=self.durability_level,
                          sync=self.sync)
            self.task.jython_task_manager.get_task_result(task)
            task = self.task.async_load_gen_docs_atomicity(
                          self.cluster, self.bucket_util.buckets, gen_create,
                          "update", 0, batch_size=20,
                          process_concurrency=8,
                          replicate_to=self.replicate_to,
                          persist_to=self.persist_to,
                          timeout_secs=self.sdk_timeout,
                          retries=self.sdk_retries,
                          transaction_timeout= self.transaction_timeout,
                          update_count=self.update_count,
                          commit=self.transaction_commit,
                          durability=self.durability_level,
                          sync=self.sync)
            self.task.jython_task_manager.get_task_result(task)
            task = self.task.async_load_gen_docs_atomicity(
                          self.cluster, self.bucket_util.buckets, gen_delete,
                          "rebalance_delete", 0, batch_size=20,
                          process_concurrency=8,
                          replicate_to=self.replicate_to,
                          persist_to=self.persist_to,
                          timeout_secs=self.sdk_timeout,
                          retries=self.sdk_retries,
                          transaction_timeout=self.transaction_timeout,
                          commit=self.transaction_commit,
                          durability=self.durability_level,
                          sync=self.sync)
            self.task.jython_task_manager.get_task_result(task)
