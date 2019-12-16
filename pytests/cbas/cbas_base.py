from basetestcase import BaseTestCase
from cbas_utils.cbas_utils import CbasUtil
from couchbase_helper.documentgenerator import DocumentGenerator
from testconstants import FTS_QUOTA, CBAS_QUOTA, INDEX_QUOTA, MIN_KV_QUOTA

from membase.api.rest_client import RestHelper, RestConnection


class CBASBaseTest(BaseTestCase):
    def setUp(self, add_default_cbas_node=True):
        super(CBASBaseTest, self).setUp()
        if self._testMethodDoc:
            self.log.info("Starting Test: %s - %s"
                          % (self._testMethodName, self._testMethodDoc))
        else:
            self.log.info("Starting Test: %s" % self._testMethodName)

        for server in self.cluster.servers:
            if "cbas" in server.services:
                self.cluster.cbas_nodes.append(server)
            if "kv" in server.services:
                self.cluster.kv_nodes.append(server)
            rest = RestConnection(server)
            rest.set_data_path(data_path=server.data_path,
                               index_path=server.index_path,
                               cbas_path=server.cbas_path)

        invalid_ip = '10.111.151.109'
        self._cb_cluster = self.cluster
        self.cb_bucket_name = self.input.param('cb_bucket_name',
                                               'travel-sample')
        self.cbas_bucket_name = self.input.param('cbas_bucket_name', 'travel')
        self.cb_bucket_password = self.input.param('cb_bucket_password', None)
        self.expected_error = self.input.param("error", None)
        if self.expected_error:
            self.expected_error = self.expected_error.replace("INVALID_IP",
                                                              invalid_ip)
            self.expected_error = \
                self.expected_error.replace("PORT", self.cluster.master.port)
        self.cb_server_ip = self.input.param("cb_server_ip", None)
        self.cb_server_ip = \
            self.cb_server_ip.replace('INVALID_IP', invalid_ip) \
            if self.cb_server_ip is not None else None
        self.cbas_dataset_name = self.input.param("cbas_dataset_name",
                                                  'travel_ds')
        self.cbas_bucket_name_invalid = \
            self.input.param('cbas_bucket_name_invalid', self.cbas_bucket_name)
        self.cbas_dataset2_name = self.input.param('cbas_dataset2_name', None)
        self.skip_create_dataset = self.input.param('skip_create_dataset',
                                                    False)
        self.disconnect_if_connected = \
            self.input.param('disconnect_if_connected', False)
        self.cbas_dataset_name_invalid = \
            self.input.param('cbas_dataset_name_invalid',
                             self.cbas_dataset_name)
        self.skip_drop_connection = self.input.param('skip_drop_connection',
                                                     False)
        self.skip_drop_dataset = self.input.param('skip_drop_dataset', False)
        self.query_id = self.input.param('query_id', None)
        self.mode = self.input.param('mode', None)
        self.num_concurrent_queries = self.input.param('num_queries', 5000)
        self.concurrent_batch_size = self.input.param('concurrent_batch_size',
                                                      100)
        self.compiler_param = self.input.param('compiler_param', None)
        self.compiler_param_val = self.input.param('compiler_param_val', None)
        self.expect_reject = self.input.param('expect_reject', False)
        self.expect_failure = self.input.param('expect_failure', False)
        self.index_name = self.input.param('index_name', "NoName")
        self.index_fields = self.input.param('index_fields', None)
        self.test_abort_snapshot = self.input.param("test_abort_snapshot",
                                                    False)
        if self.index_fields:
            self.index_fields = self.index_fields.split("-")
        self.otpNodes = list()
        self.cbas_path = server.cbas_path

        self.rest = RestConnection(self.cluster.master)
        self.log.info("Setting the min possible memory quota so that adding "
                      "more nodes to the cluster wouldn't be a problem.")
        self.rest.set_service_memoryQuota(service='memoryQuota',
                                          memoryQuota=MIN_KV_QUOTA)
        self.rest.set_service_memoryQuota(service='ftsMemoryQuota',
                                          memoryQuota=FTS_QUOTA)
        self.rest.set_service_memoryQuota(service='indexMemoryQuota',
                                          memoryQuota=INDEX_QUOTA)

        self.set_cbas_memory_from_available_free_memory = \
            self.input.param('set_cbas_memory_from_available_free_memory',
                             False)
        if self.set_cbas_memory_from_available_free_memory:
            info = self.rest.get_nodes_self()
            self.cbas_memory_quota = int((info.memoryFree // 1024 ** 2) * 0.9)
            self.log.info("Setting %d memory quota for CBAS"
                          % self.cbas_memory_quota)
            self.rest.set_service_memoryQuota(
                service='cbasMemoryQuota',
                memoryQuota=self.cbas_memory_quota)
        else:
            self.log.info("Setting %d memory quota for CBAS" % CBAS_QUOTA)
            self.cbas_memory_quota = CBAS_QUOTA
            self.rest.set_service_memoryQuota(service='cbasMemoryQuota',
                                              memoryQuota=CBAS_QUOTA)

        self.cbas_util = None
        # Drop any existing buckets and datasets
        if self.cluster.cbas_nodes:
            self.cbas_node = self.cluster.cbas_nodes[0]
            self.cbas_util = CbasUtil(self.cluster.master, self.cbas_node)
            if "cbas" in self.cluster.master.services:
                self.cleanup_cbas()
            if add_default_cbas_node:
                if self.cluster.master.ip != self.cbas_node.ip:
                    self.otpNodes.append(
                        self.cluster_util.add_node(self.cbas_node))
                else:
                    self.otpNodes = self.rest.node_statuses()
                """
                This cbas cleanup is actually not needed.
                When a node is added to the cluster,
                it is automatically cleaned-up.
                """
                self.cleanup_cbas()
                self.cluster.cbas_nodes.remove(self.cbas_node)

        self.log.info("=== CBAS_BASE setup was finished for test #{0} {1} ==="
                      .format(self.case_number, self._testMethodName))

    def tearDown(self):
        super(CBASBaseTest, self).tearDown()

    def cleanup_cbas(self):
        """
        Drops all connections, datasets and buckets from CBAS
        """
        try:
            # Disconnect from all connected buckets
            cmd_get_buckets = "select Name from Metadata.`Bucket`;"
            status, metrics, errors, results, _ = \
                self.cbas_util.execute_statement_on_cbas_util(cmd_get_buckets)
            if (results is not None) & (len(results) > 0):
                for row in results:
                    self.cbas_util.disconnect_from_bucket(
                        row['Name'],
                        disconnect_if_connected=True)
                    self.log.info("******* Disconnected all buckets *******")
            else:
                self.log.info("******* No buckets to disconnect *******")

            # Drop all datasets
            cmd_get_datasets = "select DatasetName from Metadata.`Dataset` where DataverseName != \"Metadata\";"
            status, metrics, errors, results, _ = \
                self.cbas_util.execute_statement_on_cbas_util(cmd_get_datasets)
            if (results is not None) & (len(results) > 0):
                for row in results:
                    self.cbas_util.drop_dataset(row['DatasetName'])
                    self.log.info("********* Dropped all datasets *********")
            else:
                self.log.info("********* No datasets to drop *********")

            # Drop all buckets
            status, metrics, errors, results, _ = \
                self.cbas_util.execute_statement_on_cbas_util(cmd_get_buckets)
            if (results is not None) & (len(results) > 0):
                for row in results:
                    self.cbas_util.drop_cbas_bucket(row['Name'])
                    self.log.info("********* Dropped all buckets *********")
            else:
                self.log.info("********* No buckets to drop *********")
        except Exception as e:
            self.log.info(e.message)

    def perform_doc_ops_in_all_cb_buckets(self, operation,
                                          start_key=0, end_key=1000,
                                          batch_size=10, exp=0,
                                          _async=False,
                                          durability=""):
        """
        Create/Update/Delete docs in all cb buckets
        :param operation: String - "create","update","delete"
        :param start_key: Doc Key to start the operation with
        :param end_key: Doc Key to end the operation with
        :param batch_size: Batch size of doc_ops
        :param exp: MaxTTL used for doc operations
        :param _async: Boolean to decide whether to start ops in parallel
        :param durability: Durability level to use for doc operation
        :return:
        """
        age = range(70)
        first = ['james', 'sharon', 'dave', 'bill', 'mike', 'steve']
        profession = ['doctor', 'lawyer']
        template = '{{"number": {0}, "first_name": "{1}", ' \
                   + '"profession": "{2}", "mutated": 0}}'

        doc_gen = DocumentGenerator('test_docs', template, age, first,
                                    profession, start=start_key, end=end_key)
        try:
            if _async:
                return self.bucket_util._async_load_all_buckets(
                    self.cluster, doc_gen, operation, exp,
                    durability=durability,
                    batch_size=batch_size,
                    suppress_error_table=True)
            else:
                self.bucket_util.sync_load_all_buckets(
                    self.cluster, doc_gen, operation, exp,
                    durability=durability,
                    batch_size=batch_size,
                    suppress_error_table=True)
                self.bucket_util.verify_stats_all_buckets(self.num_items)
        except Exception as e:
            self.log.error(e.message)

    def remove_node(self, otpnode=None, wait_for_rebalance=True):
        nodes = self.rest.node_statuses()
        '''This is the case when master node is running cbas service as well'''
        if len(nodes) <= len(otpnode):
            return

        helper = RestHelper(self.rest)
        try:
            removed = helper.remove_nodes(
                knownNodes=[node.id for node in nodes],
                ejectedNodes=[node.id for node in otpnode],
                wait_for_rebalance=wait_for_rebalance)
        except Exception:
            self.sleep(5, "Rebalance failed on Removal. Retry.. THIS IS A BUG")
            removed = helper.remove_nodes(
                knownNodes=[node.id for node in nodes],
                ejectedNodes=[node.id for node in otpnode],
                wait_for_rebalance=wait_for_rebalance)
        if wait_for_rebalance:
            self.assertTrue(removed,
                            "Rebalance operation failed while removing %s"
                            % otpnode)
