from Jython_tasks.task import ConcurrentFailoverTask
from bucket_collections.collections_base import CollectionBase
from cb_tools.cb_cli import CbCli
from collections_helper.collections_spec_constants import MetaCrudParams
from error_simulation.cb_error import CouchbaseError
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from constants.sdk_constants.java_client import SDKConstants

class ClusterManagement(CollectionBase):
    def setUp(self):
        super(ClusterManagement, self).setUp()

    def tearDown(self):
        super(ClusterManagement, self).tearDown()

    @staticmethod
    def __get_collection_load_spec():
        return {
            # Scope/Collection ops params
            MetaCrudParams.COLLECTIONS_TO_FLUSH: 0,
            MetaCrudParams.COLLECTIONS_TO_DROP: 3,

            MetaCrudParams.SCOPES_TO_DROP: 1,
            MetaCrudParams.SCOPES_TO_ADD_PER_BUCKET: 1,
            MetaCrudParams.COLLECTIONS_TO_ADD_FOR_NEW_SCOPES: 3,

            MetaCrudParams.COLLECTIONS_TO_ADD_PER_BUCKET: 5,

            MetaCrudParams.BUCKET_CONSIDERED_FOR_OPS: "all",
            MetaCrudParams.SCOPES_CONSIDERED_FOR_OPS: "all",
            MetaCrudParams.COLLECTIONS_CONSIDERED_FOR_OPS: "all",

            # Doc loading params
            "doc_crud": {
                MetaCrudParams.DocCrud.COMMON_DOC_KEY: "test_collections",
                MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION: 20,
                MetaCrudParams.DocCrud.READ_PERCENTAGE_PER_COLLECTION: 20,
                MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION: 20,
                MetaCrudParams.DocCrud.REPLACE_PERCENTAGE_PER_COLLECTION: 20,
                MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION: 20,
            },

            # Doc_loading task options
            MetaCrudParams.DOC_TTL: 0,
            MetaCrudParams.DURABILITY_LEVEL: SDKConstants.DurabilityLevel.MAJORITY,
            MetaCrudParams.SDK_TIMEOUT: 60,
            MetaCrudParams.SDK_TIMEOUT_UNIT: "seconds",
            MetaCrudParams.TARGET_VBUCKETS: "all",
            MetaCrudParams.SKIP_READ_ON_ERROR: True,
            MetaCrudParams.SUPPRESS_ERROR_TABLE: True,
            MetaCrudParams.SKIP_READ_SUCCESS_RESULTS: True,

            MetaCrudParams.COLLECTIONS_CONSIDERED_FOR_CRUD: "all",
            MetaCrudParams.SCOPES_CONSIDERED_FOR_CRUD: "all",
            MetaCrudParams.BUCKETS_CONSIDERED_FOR_CRUD: "all"
        }

    def test_add_serviceless_node_cli(self):
        exp_msg = "SUCCESS: Server added"
        node_to_add = self.cluster.servers[self.nodes_init]
        shell = RemoteMachineShellConnection(self.cluster.master)
        cb_cli = CbCli(shell)
        output = cb_cli.add_node(node_to_add, "manager-only")
        self.assertEqual(exp_msg, output[0].strip(),
                         "Unexpected response: %s" % output)
        result = self.task.rebalance(self.cluster, [], [])
        self.assertTrue(result, "Serviceless node rebalance failed")

    def test_serviceless_node_rebalance(self):
        add_nodes = list()
        out_nodes = list()
        fo_action = self.input.param("fo_action", None)
        fo_timeout = self.input.param("fo_timeout", 1)

        self.log.info("Enabling auto-failover with timeout={}".format(fo_timeout))
        rest = RestConnection(self.cluster.nodes_in_cluster[0])
        result = rest.update_autofailover_settings(True, fo_timeout, maxCount=1)
        self.assertTrue(result, "Setting auto-failover failed")

        if self.services_in:
            self.services_in = str(self.services_in).split("-")
            for index, service in enumerate(self.services_in):
                if service == "None":
                    self.services_in[index] = ""
                self.services_in[index] = self.services_in[index]\
                    .replace(":", ",")
            num_nodes_to_add = len(self.services_in)
            add_nodes = self.cluster.servers[
                self.nodes_init:self.nodes_init+num_nodes_to_add]

        if self.nodes_out:
            out_nodes = self.cluster.nodes_in_cluster[-self.nodes_out:]

        self.log.info("Starting doc_loading")
        load_task = self.bucket_util.run_scenario_from_spec(
            self.task, self.cluster, self.cluster.buckets,
            self.__get_collection_load_spec(),
            mutation_num=1, batch_size=self.batch_size,
            process_concurrency=1, async_load=True)
        self.log.info("Initializing rebalance task")
        reb_result = self.task.rebalance(
            self.cluster, add_nodes, out_nodes, services=self.services_in)

        self.log.info("Wait for doc_loading to complete")
        self.bucket_util.validate_doc_loading_results(self.cluster, load_task)
        self.assertTrue(reb_result, "Node rebalance task failed")
        self.assertTrue(load_task.result, "Doc loading failed")
