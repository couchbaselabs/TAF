# -*- coding: utf-8 -*-

from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import doc_generator
from membase.api.rest_client import RestConnection
from couchbase_helper.document import View


class DocumentKeysTests(BaseTestCase):
    def setUp(self):
        super(DocumentKeysTests, self).setUp()
        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend([self.cluster.master]+nodes_init)
        self.bucket_util.create_default_bucket(
            bucket_type=self.bucket_type,
            replica=self.num_replicas,
            storage=self.bucket_storage,
            eviction_policy=self.bucket_eviction_policy)
        self.bucket_util.add_rbac_user()
        self.cluster_util.print_cluster_stats()
        self.bucket_util.print_bucket_stats()
        self.log.info("====== DocumentKeysTests setUp complete ======")

    def tearDown(self):
        super(DocumentKeysTests, self).tearDown()

    def _persist_and_verify(self):
        """
        Helper function to wait for persistence and
        then verify data/stats on all buckets
        """
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.verify_stats_all_buckets(self.num_items)

    """Helper function to verify the data using view query"""
    def _verify_with_views(self, expected_rows):

        for bucket in self.bucket_util.buckets:
            default_map_func = 'function (doc, meta) { emit(meta.id, null);}'
            default_view = View("View", default_map_func, None, False)
            ddoc_name = "key_ddoc"

            self.bucket_util.create_views(
                self.cluster.master, ddoc_name, [default_view], bucket.name)
            query = {"stale": "false", "connection_timeout": 60000}
            self.bucket_util.query_view(self.cluster.master, ddoc_name,
                                        default_view.name, query,
                                        expected_rows, bucket=bucket.name)

    """
    Perform create/update/delete data ops on the input document key and verify
    """
    def _dockey_data_ops(self, dockey="dockey"):
        target_vb = None
        if self.target_vbucket is not None:
            target_vb = [self.target_vbucket]
        gen_load = doc_generator(dockey, 0, self.num_items,
                                 key_size=self.key_size,
                                 doc_size=self.doc_size,
                                 doc_type=self.doc_type,
                                 vbuckets=self.cluster_util.vbuckets,
                                 target_vbucket=target_vb)

        bucket = self.bucket_util.get_all_buckets()[0]
        for op_type in ["create", "update", "delete"]:
            task = self.task.async_load_gen_docs(
                self.cluster, bucket, gen_load, op_type, 0, batch_size=20,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                sdk_client_pool=self.sdk_client_pool)
            self.task.jython_task_manager.get_task_result(task)
            if op_type == "delete":
                self.num_items = 0
            self._persist_and_verify()

    """Perform verification with views after loading data"""
    def _dockey_views(self, dockey="dockey"):
        gen_load = doc_generator(dockey, 0, self.num_items,
                                 key_size=self.key_size,
                                 doc_size=self.doc_size,
                                 doc_type=self.doc_type,
                                 vbuckets=self.cluster_util.vbuckets)
        bucket = self.bucket_util.get_all_buckets()[0]
        task = self.task.async_load_gen_docs(self.cluster, bucket,
                                             gen_load, "create", 0,
                                             batch_size=20,
                                             persist_to=self.persist_to,
                                             replicate_to=self.replicate_to,
                                             durability=self.durability_level,
                                             timeout_secs=self.sdk_timeout,
                                             sdk_client_pool=self.sdk_client_pool)
        self.task.jython_task_manager.get_task_result(task)
        self._persist_and_verify()
        self._verify_with_views(self.num_items)

    """
    This function loads data in  bucket and waits for persistence.
    One node is failed over after that and it is verified,
    data can be retrieved
    """
    def _dockey_dcp(self, dockey="dockey"):
        gen_load = doc_generator(dockey, 0, self.num_items,
                                 key_size=self.key_size,
                                 doc_size=self.doc_size,
                                 doc_type=self.doc_type,
                                 vbuckets=self.cluster_util.vbuckets)
        bucket = self.bucket_util.get_all_buckets()[0]
        task = self.task.async_load_gen_docs(self.cluster, bucket,
                                             gen_load, "create", 0,
                                             batch_size=20,
                                             persist_to=self.persist_to,
                                             replicate_to=self.replicate_to,
                                             durability=self.durability_level,
                                             timeout_secs=self.sdk_timeout,
                                             sdk_client_pool=self.sdk_client_pool)
        self.task.jython_task_manager.get_task_result(task)
        self._persist_and_verify()

        # assert if there are not enough nodes to failover
        rest = RestConnection(self.cluster.master)
        num_nodes = len(rest.node_statuses())
        self.assertTrue(num_nodes > 1,
                        "ERROR: Not enough nodes to do failover")

        # failover 1 node(we have 1 replica) and verify the keys
        rest = RestConnection(self.cluster.master)
        node_status = rest.node_statuses()
        for node_to_failover in self.servers[(num_nodes - 1):num_nodes]:
            for node in node_status:
                if node_to_failover.ip == node.ip \
                        and int(node_to_failover.port) == int(node.port):
                    rest.fail_over(node.id, graceful=False)

        self.cluster.nodes_in_cluster = \
            list(set(self.cluster.nodes_in_cluster)
                 - set(self.servers[(num_nodes - 1):num_nodes]))
        self._persist_and_verify()

    def test_dockey_whitespace_data_ops(self):
        generic_key = "d o c k e y"
        if self.key_size:
            self.key_size = self.key_size-len(generic_key)
            generic_key = generic_key + "_" * self.key_size
        self._dockey_data_ops(generic_key)

    def test_dockey_binary_data_ops(self):
        generic_key = "d\ro\nckey"
        if self.key_size:
            self.key_size = self.key_size-len(generic_key)
            generic_key = generic_key + "\n" * self.key_size
        self._dockey_data_ops(generic_key)

    def test_dockey_unicode_data_ops(self):
        generic_key = "\u00CA"
        if self.key_size:
            self.key_size = self.key_size-len(generic_key)
            generic_key = generic_key + "é" * self.key_size
        self._dockey_data_ops(generic_key)

    def test_dockey_whitespace_views(self):
        self._dockey_views("doc    key  ")

    def test_dockey_binary_views(self):
        self._dockey_views("docke\0y\n")

    def test_dockey_unicode_views(self):
        self._dockey_views("México")

    def test_dockey_whitespace_dcp(self):
        self._dockey_dcp("d o c k e y")

    def test_dockey_binary_dcp(self):
        self._dockey_dcp("d\rocke\0y")

    def test_dockey_unicode_dcp(self):
        self._dockey_dcp("привет")
