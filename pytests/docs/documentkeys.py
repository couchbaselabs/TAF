# -*- coding: utf-8 -*-

from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import doc_generator
from membase.api.rest_client import RestConnection
from couchbase_helper.document import View
from memcached.helper.data_helper import MemcachedClientHelper


class DocumentKeysTests(BaseTestCase):

    def setUp(self):
        super(DocumentKeysTests, self).setUp()
        self.key = 'test_docs'.rjust(self.key_size, '0')
        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend([self.cluster.master]+nodes_init)
        self.bucket_util.create_default_bucket()
        self.bucket_util.add_rbac_user()

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

        for bucket in self.buckets:
            default_map_func = 'function (doc, meta) { emit(meta.id, null);}'
            default_view = View("View", default_map_func, None, False)
            ddoc_name = "key_ddoc"

            self.create_views(self.cluster.master, ddoc_name, [default_view],
                              bucket.name)
            query = {"stale": "false", "connection_timeout": 60000}
            self.cluster.query_view(self.cluster.master, ddoc_name,
                                    default_view.name, query,
                                    expected_rows, bucket=bucket.name)

    """
    Perform create/update/delete data ops on the input document key and verify
    """
    def _dockey_data_ops(self, dockey="dockey"):
        gen_load = doc_generator(dockey, 0, self.num_items, doc_type="json")
        bucket = self.bucket_util.get_all_buckets()[0]
        for op_type in ["create", "update", "delete"]:
            task = self.task.async_load_gen_docs(
                self.cluster, bucket, gen_load, op_type, 0, batch_size=20,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                pause_secs=5, timeout_secs=self.sdk_timeout,
                retries=self.sdk_retries)
            self.task.jython_task_manager.get_task_result(task)
            if op_type == "delete":
                self.num_items = 0
            self._persist_and_verify()

    """Perform verification with views after loading data"""
    def _dockey_views(self, dockey="dockey"):
        gen_load = doc_generator(dockey, 0, self.num_items, doc_type="json")
        bucket = self.bucket_util.get_all_buckets()[0]
        task = self.task.async_load_gen_docs(self.cluster, bucket,
                                             gen_load, "create", 0,
                                             batch_size=20,
                                             persist_to=self.persist_to,
                                             replicate_to=self.replicate_to,
                                             pause_secs=5,
                                             timeout_secs=self.sdk_timeout,
                                             retries=self.sdk_retries)
        self.task.jython_task_manager.get_task_result(task)
        self._persist_and_verify()
        self._verify_with_views(self.num_items)

    """
    This function loads data in  bucket and waits for persistence.
    One node is failed over after that and it is verified,
    data can be retrieved
    """
    def _dockey_tap(self, dockey="dockey"):
        gen_load = doc_generator(dockey, 0, self.num_items, doc_type="json")
        bucket = self.bucket_util.get_all_buckets()[0]
        task = self.task.async_load_gen_docs(self.cluster, bucket,
                                             gen_load, "create", 0,
                                             batch_size=20,
                                             persist_to=self.persist_to,
                                             replicate_to=self.replicate_to,
                                             pause_secs=5,
                                             timeout_secs=self.sdk_timeout,
                                             retries=self.sdk_retries)
        self.task.jython_task_manager.get_task_result(task)
        self._persist_and_verify()

        # assert if there are not enough nodes to failover
        rest = RestConnection(self.cluster.master)
        num_nodes = len(rest.node_statuses())
        self.assertTrue(num_nodes > 1,
                        "ERROR: Not enough nodes to do failover")

        # failover 1 node(we have 1 replica) and verify the keys
        self.cluster.failover(self.servers[:num_nodes],
                              self.servers[(num_nodes - 1):num_nodes])

        self.nodes_init -= 1
        self._persist_and_verify()

    """
    This function perform data ops create/update/delete via moxi for the
    given key and then waits for persistence.
    The data is then verified by doing view query
    """
    def _dockey_data_ops_with_moxi(self, dockey="dockey", data_op="create"):
        expected_rows = self.num_items
        for bucket in self.buckets:
            try:
                client = MemcachedClientHelper.proxy_client(
                    self.cluster.master, bucket.name)
            except Exception as ex:
                self.log.exception("Unable to create memcached client - {0}"
                                   .format(ex))

            try:
                for itr in xrange(self.num_items):
                    key = dockey + str(itr)
                    value = str(itr)
                    if data_op in ["create", "update"]:
                        client.set(key, 0, 0, value)
                    elif data_op == "delete":
                        client.delete(key)
                        expected_rows = 0
            except Exception as ex:
                self.log.exception("Exception {0} while performing data op {1}"
                                   .format(ex, data_op))

        self.bucket_util._wait_for_stats_all_buckets()
        if self.bucket_type != 'ephemeral':
            # views not supported for ephemeral buckets
            self._verify_with_views(expected_rows)

    def test_dockey_whitespace_data_ops(self):
        self._dockey_data_ops("d o c k e y")

    def test_dockey_binary_data_ops(self):
        self._dockey_data_ops("d\ro\nckey")

    def test_dockey_unicode_data_ops(self):
        self._dockey_data_ops("\u00CA")

    def test_dockey_whitespace_views(self):
        self._dockey_views("doc    key  ")

    def test_dockey_binary_views(self):
        self._dockey_views("docke\0y\n")

    def test_dockey_unicode_views(self):
        self._dockey_views("México")

    def test_dockey_whitespace_tap(self):
        self._dockey_tap("d o c k e y")

    def test_dockey_binary_tap(self):
        self._dockey_tap("d\rocke\0y")

    def test_dockey_unicode_tap(self):
        self._dockey_tap("привет")

    def test_dockey_whitespace_data_ops_moxi(self):
        self._dockey_data_ops_with_moxi("d o c k e y", "create")
        self._dockey_data_ops_with_moxi("d o c k e y", "update")
        self._dockey_data_ops_with_moxi("d o c k e y", "delete")

    def test_dockey_binary_data_ops_moxi(self):
        self._dockey_data_ops_with_moxi("d\rocke\0y", "create")
        self._dockey_data_ops_with_moxi("d\rocke\0y", "update")
        self._dockey_data_ops_with_moxi("d\rocke\0y", "delete")

    def test_dockey_unicode_data_ops_moxi(self):
        self._dockey_data_ops_with_moxi("mix 你好", "create")
        self._dockey_data_ops_with_moxi("mix 你好", "update")
        self._dockey_data_ops_with_moxi("mix 你好", "delete")
