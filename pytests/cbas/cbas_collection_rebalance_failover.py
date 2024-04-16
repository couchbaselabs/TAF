'''
@author: Umang
TO-DO : Extend this to support remote cluster rebalance operations also, once cbas_base refactoring is done
'''

from cbas.cbas_base import CBASBaseTest
from cbas_utils.cbas_utils import CBASRebalanceUtil


class CBASRebalance(CBASBaseTest):

    def setUp(self):

        super(CBASRebalance, self).setUp()
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.setUp.__name__)

        # Since all the test cases are being run on 1 cluster only
        self.cluster = self.cb_clusters.values()[0]
        self.cluster.exclude_nodes = list()

        self.data_load_stage = self.input.param("data_load_stage", "during")
        self.skip_validations = self.input.param("skip_validations", True)
        self.parallel_load_percent = int(self.input.param("parallel_load_percent", 0))

        self.rebalance_util = CBASRebalanceUtil(
            self.cluster_util, self.bucket_util, self.task,
            self.input.param("vbucket_check", True), self.cbas_util)

        self.cluster.exclude_nodes.extend(
            [self.cluster.master, self.cluster.cbas_cc_node])

        self.run_parallel_kv_query = self.input.param(
            "run_kv_queries", False)
        self.run_parallel_cbas_query = self.input.param(
            "run_cbas_queries", False)

        self.query_interval = self.input.param("query_interval", 3)

        self.cbas_spec = self.cbas_util.get_cbas_spec(self.cbas_spec_name)
        update_spec = {
            "no_of_dataverses": self.input.param('no_of_dv', 1),
            "no_of_datasets": self.input.param('no_of_dv', 1) * 2,
            "no_of_synonyms": self.input.param('no_of_synonyms', 1),
            "no_of_indexes": self.input.param('no_of_indexes', 1),
            "max_thread_count": self.input.param('no_of_threads', 1)}
        self.cbas_util.update_cbas_spec(self.cbas_spec, update_spec)
        if not self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.cbas_spec, self.bucket_util):
            self.fail("Error while creating infra from CBAS spec")
        self.n1ql_query_task, self.cbas_query_task = self.rebalance_util.start_parallel_queries(
            self.cluster, self.run_parallel_kv_query,
            self.run_parallel_cbas_query, self.num_concurrent_queries)
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status("CBASRebalance", "Started", stage="Teardown")
        self.rebalance_util.stop_parallel_queries(
            self.n1ql_query_task, self.cbas_query_task)
        super(CBASRebalance, self).tearDown()
        self.log_setup_status("CBASRebalance", "Finished", stage="Teardown")

    def load_collections_with_rebalance(
            self, rebalance_operation, kv_nodes_in=0, kv_nodes_out=0,
            cbas_nodes_in=0, cbas_nodes_out=0):

        self.log.info(
            "Doing collection data load {0} {1}".format(
                self.data_load_stage, rebalance_operation))

        if self.data_load_stage == "before":
            if not self.rebalance_util.data_load_collection(
                    self.cluster, self.doc_spec_name, self.skip_validations,
                    async_load=False, durability_level=self.durability_level):
                self.fail("Doc loading failed")

        if self.data_load_stage == "during":
            if self.parallel_load_percent <= 0:
                self.parallel_load_percent = 100
            data_load_task = self.rebalance_util.data_load_collection(
                self.cluster, self.doc_spec_name, self.skip_validations,
                async_load=True, durability_level=self.durability_level,
                create_percentage_per_collection=self.parallel_load_percent)

        rebalance_task, self.available_servers = self.rebalance_util.rebalance(
            self.cluster, kv_nodes_in, kv_nodes_out, cbas_nodes_in,
            cbas_nodes_out, self.available_servers, self.cluster.exclude_nodes)

        if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                rebalance_task, self.cluster):
            self.fail("Rebalance failed")

        if self.data_load_stage == "during":
            if not self.rebalance_util.wait_for_data_load_to_complete(
                    self.cluster, data_load_task, self.skip_validations):
                self.fail("Doc loading failed")

        if self.data_load_stage == "after":
            if not self.rebalance_util.data_load_collection(
                    self.cluster, self.doc_spec_name, self.skip_validations,
                    async_load=False, durability_level=self.durability_level):
                self.fail("Doc loading failed")

        self.rebalance_util.data_validation_collection(
            self.cluster, skip_validations=self.skip_validations,
            doc_and_collection_ttl=False)
        self.bucket_util.print_bucket_stats(self.cluster)
        if not self.cbas_util.validate_docs_in_all_datasets(
            self.cluster, self.bucket_util):
            self.fail("Doc count mismatch between KV and CBAS")

    def load_collections_with_failover(
            self, failover_type="Hard", action="RebalanceOut", kv_nodes=0,
            cbas_nodes=0):

        self.log.info(
            "{0} Failover a node and {1} that node with data load in "
            "parallel".format(failover_type, action))

        if self.data_load_stage == "before":
            if not self.rebalance_util.data_load_collection(
                    self.cluster, self.doc_spec_name, self.skip_validations,
                    async_load=False, durability_level=self.durability_level):
                self.fail("Doc loading failed")

        if self.data_load_stage == "during":
            reset_flag = False
            if (not self.durability_level) and failover_type \
                    == "Hard" and kv_nodes:
                # Force a durability level to prevent data loss during hard failover
                self.log.info("Forcing durability level: MAJORITY")
                self.durability_level = "MAJORITY"
                reset_flag = True
            data_load_task = self.rebalance_util.data_load_collection(
                self.cluster, self.doc_spec_name, self.skip_validations,
                async_load=True, durability_level=self.durability_level,
                create_percentage_per_collection=self.parallel_load_percent)
            if reset_flag:
                self.durability_level = ""

        self.available_servers, _, _ = self.rebalance_util.failover(
            self.cluster, kv_nodes=kv_nodes, cbas_nodes=cbas_nodes,
            failover_type=failover_type, action=action, timeout=7200,
            available_servers=self.available_servers,
            exclude_nodes=self.cluster.exclude_nodes)

        if self.data_load_stage == "during":
            if not self.rebalance_util.wait_for_data_load_to_complete(
                    self.cluster, data_load_task, self.skip_validations):
                self.fail("Doc loading failed")

        if self.data_load_stage == "after":
            if not self.rebalance_util.data_load_collection(
                    self.cluster, self.doc_spec_name, self.skip_validations,
                    async_load=False, durability_level=self.durability_level):
                self.fail("Doc loading failed")

        self.rebalance_util.data_validation_collection(
            self.cluster, skip_validations=self.skip_validations,
            doc_and_collection_ttl=False)

        self.bucket_util.print_bucket_stats(self.cluster)
        if not self.cbas_util.validate_docs_in_all_datasets(
                self.cluster, self.bucket_util):
            self.fail("Doc count mismatch between KV and CBAS")

    def test_cbas_with_kv_rebalance_in(self):
        self.load_collections_with_rebalance(
            rebalance_operation="rebalance_in", kv_nodes_in=1, kv_nodes_out=0,
            cbas_nodes_in=0, cbas_nodes_out=0)

    def test_cbas_with_cbas_rebalance_in(self):
        self.load_collections_with_rebalance(
            rebalance_operation="rebalance_in", kv_nodes_in=0, kv_nodes_out=0,
            cbas_nodes_in=1, cbas_nodes_out=0)

    def test_cbas_with_kv_cbas_rebalance_in(self):
        self.load_collections_with_rebalance(
            rebalance_operation="rebalance_in", kv_nodes_in=1, kv_nodes_out=0,
            cbas_nodes_in=1, cbas_nodes_out=0)

    def test_cbas_with_kv_rebalance_out(self):
        self.load_collections_with_rebalance(
            rebalance_operation="rebalance_out", kv_nodes_in=0, kv_nodes_out=1,
            cbas_nodes_in=0, cbas_nodes_out=0)

    def test_cbas_with_cbas_rebalance_out(self):
        self.load_collections_with_rebalance(
            rebalance_operation="rebalance_out", kv_nodes_in=0, kv_nodes_out=0,
            cbas_nodes_in=0, cbas_nodes_out=1)

    def test_cbas_with_kv_cbas_rebalance_out(self):
        self.load_collections_with_rebalance(
            rebalance_operation="rebalance_out", kv_nodes_in=0, kv_nodes_out=1,
            cbas_nodes_in=0, cbas_nodes_out=1)

    def test_cbas_with_kv_swap_rebalance(self):
        self.load_collections_with_rebalance(
            rebalance_operation="swap_rebalance", kv_nodes_in=1, kv_nodes_out=1,
            cbas_nodes_in=0, cbas_nodes_out=0)

    def test_cbas_with_cbas_swap_rebalance(self):
        self.load_collections_with_rebalance(
            rebalance_operation="swap_rebalance", kv_nodes_in=0, kv_nodes_out=0,
            cbas_nodes_in=1, cbas_nodes_out=1)

    def test_cbas_with_kv_cbas_swap_rebalance(self):
        self.load_collections_with_rebalance(
            rebalance_operation="swap_rebalance", kv_nodes_in=1, kv_nodes_out=1,
            cbas_nodes_in=1, cbas_nodes_out=1)

    def test_cbas_with_kv_rebalance_in_out(self):
        self.load_collections_with_rebalance(
            rebalance_operation="rebalance_in_out", kv_nodes_in=2,
            kv_nodes_out=1, cbas_nodes_in=0, cbas_nodes_out=0)

    def test_cbas_with_cbas_rebalance_in_out(self):
        self.load_collections_with_rebalance(
            rebalance_operation="rebalance_in_out", kv_nodes_in=0,
            kv_nodes_out=0, cbas_nodes_in=2, cbas_nodes_out=1)

    def test_cbas_with_kv_cbas_rebalance_in_out(self):
        self.load_collections_with_rebalance(
            rebalance_operation="rebalance_in_out", kv_nodes_in=2,
            kv_nodes_out=1, cbas_nodes_in=2, cbas_nodes_out=1)

    def test_cbas_with_kv_graceful_failover_rebalance_out(self):
        self.load_collections_with_failover(
            failover_type="Graceful", action="RebalanceOut", kv_nodes=1)

    def test_cbas_with_kv_graceful_failover_full_recovery(self):
        self.load_collections_with_failover(
            failover_type="Graceful", action="FullRecovery", kv_nodes=1)

    def test_cbas_with_kv_graceful_failover_delta_recovery(self):
        self.load_collections_with_failover(
            failover_type="Graceful", action="DeltaRecovery", kv_nodes=1)

    def test_cbas_with_kv_hard_failover_rebalance_out(self):
        self.load_collections_with_failover(
            failover_type="Hard", action="RebalanceOut", kv_nodes=1)

    def test_cbas_with_cbas_hard_failover_rebalance_out(self):
        self.load_collections_with_failover(
            failover_type="Hard", action="RebalanceOut", cbas_nodes=1)

    def test_cbas_with_kv_cbas_hard_failover_rebalance_out(self):
        self.load_collections_with_failover(
            failover_type="Hard", action="RebalanceOut", kv_nodes=1, cbas_nodes=1)

    def test_cbas_with_kv_hard_failover_full_recovery(self):
        self.load_collections_with_failover(
            failover_type="Hard", action="FullRecovery", kv_nodes=1)

    def test_cbas_with_cbas_hard_failover_full_recovery(self):
        self.load_collections_with_failover(
            failover_type="Hard", action="FullRecovery", cbas_nodes=1)

    def test_cbas_with_kv_cbas_hard_failover_full_recovery(self):
        self.load_collections_with_failover(
            failover_type="Hard", action="FullRecovery", kv_nodes=1, cbas_nodes=1)

    def test_cbas_with_kv_hard_failover_delta_recovery(self):
        self.load_collections_with_failover(
            failover_type="Hard", action="DeltaRecovery", kv_nodes=1)
