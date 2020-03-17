from bucket_collections.collections_base import CollectionBase
from cb_tools.cbepctl import Cbepctl
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from remote.remote_util import RemoteMachineShellConnection


class RollbackTests(CollectionBase):
    def setUp(self):
        super(RollbackTests, self).setUp()
        self.num_rollbacks = self.input.param("num_rollbacks", 5)
        self.rollback_size = 100
        self.bucket = self.bucket_util.buckets[0]

        # Creating required doc_generators for rollback loading
        self.create_gen = doc_generator(self.key,
                                        self.num_items,
                                        self.num_items+self.rollback_size)
        self.update_gen = doc_generator(self.key, 0, self.num_items)

    def tearDown(self):
        super(RollbackTests, self).tearDown()

    def validate_seq_no_post_rollback(self, init_stat, post_stat):
        status = False
        # TODO: Need to implement this comparison function
        if status is False:
            self.log_failure("Seqno validation failed")
        return status

    def load_docs(self):
        if self.doc_ops == "create":
            doc_gen = self.create_gen
        else:
            doc_gen = self.update_gen
        for _, scope in self.bucket.scopes.items():
            for _, collection in scope.collections.items():
                task = self.bucket_util.async_load_bucket(
                    self.cluster, self.bucket, doc_gen, self.doc_ops,
                    durability=self.durability_level,
                    sdk_timeout=self.sdk_timeout,
                    batch_size=100, process_concurrency=8,
                    scope=scope.name, collection=collection.name)
                self.task_manager.get_task_result(task)
                if task.fail:
                    self.log_failure("Doc loading failed for %s:%s:%s - %s"
                                     % (self.bucket.name,
                                        scope.name, collection.name,
                                        task.fail))

    def test_rollback_n_times(self):
        items = self.num_items
        mem_only_items = self.input.param("rollback_items", 100)
        if self.nodes_init < 2 or self.num_replicas < 1:
            self.fail("Not enough nodes/replicas to test rollback")
        shell = RemoteMachineShellConnection(self.cluster_util.cluster.master)
        cbstats = Cbstats(shell)
        self.target_vbucket = cbstats.vbucket_list(self.bucket.name)
        start = self.num_items
        self.gen_validate = self.gen_create
        init_vb_stat = cbstats.vbucket_details(self.bucket.name)
        for _ in xrange(1, self.num_rollbacks+1):
            # Stopping persistence on NodeA
            cbepctl = Cbepctl(shell)
            cbepctl.persistence(self.bucket.name, "stop")
            self.gen_create = doc_generator(
                self.key,
                start,
                mem_only_items,
                doc_size=self.doc_size,
                doc_type=self.doc_type,
                target_vbucket=self.target_vbucket,
                vbuckets=self.cluster_util.vbuckets,
                randomize_doc_size=self.randomize_doc_size,
                randomize_value=self.randomize_value)
            self.load_docs()
            start = self.gen_create.key_counter
            ep_queue_size_map = {
                self.cluster.nodes_in_cluster[0]: mem_only_items}
            vb_replica_queue_size_map = {self.cluster.nodes_in_cluster[0]: 0}
            for node in self.cluster.nodes_in_cluster[1:]:
                ep_queue_size_map.update({node: 0})
                vb_replica_queue_size_map.update({node: 0})

            for bucket in self.bucket_util.buckets:
                self.bucket_util._wait_for_stat(bucket, ep_queue_size_map)
                self.bucket_util._wait_for_stat(
                    bucket,
                    vb_replica_queue_size_map,
                    stat_name="vb_replica_queue_size")

            # Kill memcached on NodeA to trigger rollback on other Nodes
            # replica vBuckets
            for bucket in self.bucket_util.buckets:
                self.log.debug(cbstats.failover_stats(bucket.name))
            shell.kill_memcached()
            self.assertTrue(self.bucket_util._wait_warmup_completed(
                [self.cluster_util.cluster.master],
                self.bucket,
                wait_time=self.wait_timeout * 10))
            self.sleep(10, "Wait after warmup complete. Not required !!")
            self.bucket_util.verify_stats_all_buckets(items, timeout=300)
            for bucket in self.bucket_util.buckets:
                self.log.debug(cbstats.failover_stats(bucket.name))

            data_validation = self.task.async_validate_docs(
                self.cluster, self.bucket,
                self.gen_validate, "create", 0, batch_size=10)
            self.task.jython_task_manager.get_task_result(data_validation)
            post_vb_stat = cbstats.vbucket_details(self.bucket.name)
            self.validate_seq_no_post_rollback(init_vb_stat, post_vb_stat)

        shell.disconnect()
        self.validate_test_failure()

    def test_rollback_to_zero(self):
        items = self.num_items
        mem_only_items = self.input.param("rollback_items", 10000)
        if self.nodes_init < 2 or self.num_replicas < 1:
            self.fail("Not enough nodes/replicas to test rollback")
        shell = RemoteMachineShellConnection(self.cluster_util.cluster.master)
        self.target_vbucket = Cbstats(shell).vbucket_list(self.bucket.name)
        start = self.num_items

        # Fetch vbucket stats for validation
        cbstats = Cbstats(shell)
        init_vb_stat = cbstats.vbucket_details(self.bucket.name)

        # Stopping persistence on NodeA
        cbepctl = Cbepctl(shell)
        cbepctl.persistence(self.bucket.name, "stop")

        for i in xrange(1, self.num_rollbacks+1):
            self.gen_create = doc_generator(
                self.key,
                start,
                mem_only_items,
                doc_size=self.doc_size,
                doc_type=self.doc_type,
                target_vbucket=self.target_vbucket,
                vbuckets=self.cluster_util.vbuckets,
                randomize_doc_size=self.randomize_doc_size,
                randomize_value=self.randomize_value)
            self.load_docs()
            start = self.gen_create.key_counter
            stat_map = {self.cluster.nodes_in_cluster[0]: mem_only_items*i}
            for node in self.cluster.nodes_in_cluster[1:]:
                stat_map.update({node: 0})

            for bucket in self.bucket_util.buckets:
                self.bucket_util._wait_for_stat(bucket, stat_map)
            self.sleep(60)
            post_vb_stat = cbstats.vbucket_details(self.bucket.name)
            self.validate_seq_no_post_rollback(init_vb_stat, post_vb_stat)

        shell.kill_memcached()
        self.assertTrue(self.bucket_util._wait_warmup_completed(
            [self.cluster_util.cluster.master],
            self.bucket,
            wait_time=self.wait_timeout * 10))
        self.bucket_util.verify_stats_all_buckets(items)
        shell.disconnect()
        self.validate_test_failure()
