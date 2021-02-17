'''
@author: Umang
TO-DO : Extend this to support remote cluster rebalance operations also, once cbas_base refactoring is done
'''

from TestInput import TestInputSingleton
from cbas.cbas_base import CBASBaseTest
from cbas_utils.cbas_utils_v2 import CBASRebalanceUtil


class CBASRebalance(CBASBaseTest):
    def setUp(self):
        self.input = TestInputSingleton.input
        if "bucket_spec" not in self.input.test_params:
            self.input.test_params.update({"bucket_spec": "analytics.default"})
        if "set_cbas_memory_from_available_free_memory" not in \
                self.input.test_params:
            self.input.test_params.update(
                {"set_cbas_memory_from_available_free_memory": True})
        super(CBASRebalance, self).setUp()

        self.data_load_stage = self.input.param("data_load_stage", "during")
        self.skip_validations = self.input.param("skip_validations", True)

        self.rebalance_util = CBASRebalanceUtil(
            self.cluster, self.cluster_util, self.bucket_util, self.task,
            self.rest,
            self.input.param("vbucket_check", True), self.cbas_util_v2)
        CBASRebalanceUtil.exclude_nodes.extend(
            [self.cluster.master, self.cbas_node])
        self.run_parallel_kv_query = self.input.param(
            "run_kv_queries", False)
        self.run_parallel_cbas_query = self.input.param(
            "run_cbas_queries", False)
        self.rebalance_util.durability_level = self.durability_level
        CBASRebalanceUtil.query_interval = self.input.param("query_interval", 3)
        CBASRebalanceUtil.available_servers = list(
            set(self.cluster.servers).difference(
                set(self.rebalance_util.cluster.nodes_in_cluster)))
        self.cbas_spec = self.cbas_util_v2.get_cbas_spec(self.cbas_spec_name)
        update_spec = {
            "no_of_dataverses": self.input.param('no_of_dv', 1),
            "no_of_datasets_per_dataverse": self.input.param('ds_per_dv', 1),
            "no_of_synonyms": self.input.param('no_of_synonyms', 1),
            "no_of_indexes": self.input.param('no_of_indexes', 1),
            "max_thread_count": self.input.param('no_of_threads', 1)}
        self.cbas_util_v2.update_cbas_spec(self.cbas_spec, update_spec)
        if not self.cbas_util_v2.create_cbas_infra_from_spec(self.cbas_spec,
                                                             self.bucket_util):
            self.fail("Error while creating infra from CBAS spec")
        self.rebalance_util.start_parallel_queries(
            self.run_parallel_kv_query, self.run_parallel_cbas_query,
            self.num_concurrent_queries)
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status("CBASRebalance", "Started", stage="Teardown")
        self.rebalance_util.stop_parallel_queries()
        super(CBASRebalance, self).tearDown()
        self.log_setup_status("CBASRebalance", "Finished", stage="Teardown")

    def load_collections_with_rebalance(
            self, rebalance_operation, kv_nodes_in=0, kv_nodes_out=0,
            cbas_nodes_in=0, cbas_nodes_out=0):
        self.log.info(
            "Doing collection data load {0} {1}".format(self.data_load_stage,
                                                        rebalance_operation))
        if self.data_load_stage == "before":
            if not self.rebalance_util.data_load_collection(
                    self.doc_spec_name, self.skip_validations,
                    async_load=False):
                self.fail("Doc loading failed")
        if self.data_load_stage == "during":
            if self.parallel_load_percent <= 0:
                self.parallel_load_percent = 100
            data_load_task = self.rebalance_util.data_load_collection(
                self.doc_spec_name, self.skip_validations, async_load=True,
                percentage_per_collection=self.parallel_load_percent)
        rebalance_task = self.rebalance_util.rebalance(
            kv_nodes_in=kv_nodes_in, kv_nodes_out=kv_nodes_out,
            cbas_nodes_in=cbas_nodes_in, cbas_nodes_out=cbas_nodes_out)
        if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                rebalance_task):
            self.fail("Rebalance failed")
        if self.data_load_stage == "during":
            if not self.rebalance_util.wait_for_data_load_to_complete(
                    data_load_task, self.skip_validations):
                self.fail("Doc loading failed")
        if self.data_load_stage == "after":
            if not self.rebalance_util.data_load_collection(
                    self.doc_spec_name, self.skip_validations,
                    async_load=False):
                self.fail("Doc loading failed")
        self.rebalance_util.data_validation_collection(
            skip_validations=self.skip_validations,
            doc_and_collection_ttl=False)
        self.bucket_util.print_bucket_stats()
        if not self.cbas_util_v2.validate_docs_in_all_datasets(
                self.bucket_util):
            self.fail("Doc count mismatch between KV and CBAS")

    def load_collections_with_failover(self, failover_type="Hard",
                                       action="RebalanceOut",
                                       service_type="cbas"):
        self.log.info(
            "{0} Failover a node and {1} that node with data load in "
            "parallel".format(
                failover_type, action))
        if self.data_load_stage == "before":
            if not self.rebalance_util.data_load_collection(
                    self.doc_spec_name, self.skip_validations,
                    async_load=False):
                self.fail("Doc loading failed")
        if self.data_load_stage == "during":
            reset_flag = False
            if (not self.rebalance_util.durability_level) and failover_type \
                    == "Hard" and "kv" in service_type:
                # Force a durability level to prevent data loss during hard failover
                self.log.info("Forcing durability level: MAJORITY")
                self.rebalance_util.durability_level = "MAJORITY"
                reset_flag = True
            data_load_task = self.rebalance_util.data_load_collection(
                self.doc_spec_name, self.skip_validations, async_load=True)
            if reset_flag:
                self.rebalance_util.durability_level = ""
        self.rebalance_util.failover(
            failover_type=failover_type, action=action,
            service_type=service_type, timeout=7200)
        if self.data_load_stage == "during":
            if not self.rebalance_util.wait_for_data_load_to_complete(
                    data_load_task, self.skip_validations):
                self.fail("Doc loading failed")
        self.rebalance_util.data_validation_collection(
            skip_validations=self.skip_validations,
            doc_and_collection_ttl=False)
        if self.data_load_stage == "after":
            if not self.rebalance_util.data_load_collection(
                    self.doc_spec_name, self.skip_validations,
                    async_load=False):
                self.fail("Doc loading failed")
            self.rebalance_util.data_validation_collection(
                skip_validations=self.skip_validations,
                doc_and_collection_ttl=False)

        self.bucket_util.print_bucket_stats()
        if not self.cbas_util_v2.validate_docs_in_all_datasets(
                self.bucket_util):
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
        self.load_collections_with_failover(failover_type="Graceful",
                                            action="RebalanceOut",
                                            service_type="kv")

    def test_cbas_with_kv_graceful_failover_full_recovery(self):
        self.load_collections_with_failover(failover_type="Graceful",
                                            action="FullRecovery",
                                            service_type="kv")

    def test_cbas_with_kv_graceful_failover_delta_recovery(self):
        self.load_collections_with_failover(failover_type="Graceful",
                                            action="DeltaRecovery",
                                            service_type="kv")

    def test_cbas_with_kv_hard_failover_rebalance_out(self):
        self.load_collections_with_failover(failover_type="Hard",
                                            action="RebalanceOut",
                                            service_type="kv")

    def test_cbas_with_cbas_hard_failover_rebalance_out(self):
        self.load_collections_with_failover(failover_type="Hard",
                                            action="RebalanceOut",
                                            service_type="cbas")

    def test_cbas_with_kv_cbas_hard_failover_rebalance_out(self):
        self.load_collections_with_failover(failover_type="Hard",
                                            action="RebalanceOut",
                                            service_type="kv-cbas")

    def test_cbas_with_kv_hard_failover_full_recovery(self):
        self.load_collections_with_failover(failover_type="Hard",
                                            action="FullRecovery",
                                            service_type="kv")

    def test_cbas_with_cbas_hard_failover_full_recovery(self):
        self.load_collections_with_failover(failover_type="Hard",
                                            action="FullRecovery",
                                            service_type="cbas")

    def test_cbas_with_kv_cbas_hard_failover_full_recovery(self):
        self.load_collections_with_failover(failover_type="Hard",
                                            action="FullRecovery",
                                            service_type="kv-cbas")

    def test_cbas_with_kv_hard_failover_delta_recovery(self):
        self.load_collections_with_failover(failover_type="Hard",
                                            action="DeltaRecovery",
                                            service_type="kv")
