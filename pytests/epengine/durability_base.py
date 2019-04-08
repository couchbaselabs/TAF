from random import randint

from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import doc_generator


class DurabilityTestsBase(BaseTestCase):
    def setUp(self):
        super(DurabilityTestsBase, self).setUp()

        self.key = 'test_docs'.rjust(self.key_size, '0')
        self.simulate_error = self.input.param("simulate_error", None)
        self.error_type = self.input.param("error_type", "memory")
        self.doc_ops = self.input.param("doc_ops", None)
        self.with_non_sync_writes = self.input.param("with_non_sync_writes",
                                                     False)
        self.crud_batch_size = 100
        self.num_nodes_affected = 1
        if self.num_replicas > 1:
            self.num_nodes_affected = 2

        if self.doc_ops:
            self.doc_ops = self.doc_ops.split(';')

        # Initialize cluster using given nodes
        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend([self.cluster.master]+nodes_init)

        # Create default bucket and add rbac user
        self.bucket_util.create_default_bucket(
            replica=self.num_replicas, compression_mode=self.compression_mode)
        self.bucket_util.add_rbac_user()

        self.bucket = self.bucket_util.buckets[0]

        self.log.info("Creating doc_generator..")
        doc_create = doc_generator(
            self.key, 0, self.num_items, doc_size=self.doc_size,
            doc_type=self.doc_type, target_vbucket=self.target_vbucket,
            vbuckets=self.vbuckets)
        self.log.info("doc_generator created")

        self.log.info("Loading {0} items into bucket".format(self.num_items))
        task = self.task.async_load_gen_docs(
            self.cluster, self.bucket, doc_create, "create", 0,
            batch_size=10, process_concurrency=8,
            replicate_to=self.replicate_to, persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries)
        self.task.jython_task_manager.get_task_result(task)

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)
        self.log.info("=== DurabilityBaseTests setup complete ===")

    def tearDown(self):
        super(DurabilityTestsBase, self).tearDown()

    def get_random_node(self):
        rand_node_index = randint(1, self.nodes_init-1)
        return self.cluster.nodes_in_cluster[rand_node_index]

    def getTargetNodes(self):
        def select_randam_node(nodes):
            rand_node_index = randint(1, self.nodes_init-1)
            if self.cluster.nodes_in_cluster[rand_node_index] not in node_list:
                nodes.append(self.cluster.nodes_in_cluster[rand_node_index])

        node_list = list()
        if len(self.cluster.nodes_in_cluster) > 1:
            # Choose random nodes, if the cluster is not a single node cluster
            while len(node_list) != self.num_nodes_affected:
                select_randam_node(node_list)
        else:
            node_list.append(self.cluster.master)
        return node_list
