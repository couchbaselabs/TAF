import math

from bucket_collections.collections_base import CollectionBase

from Cb_constants import CbServer
from collections_helper.collections_spec_constants import MetaCrudParams
from platform_utils.remote.remote_util import RemoteMachineShellConnection
from sdk_exceptions import SDKException
from bucket_utils.bucket_ready_functions import BucketUtils


class CollectionsNetworkSplit(CollectionBase):
    def setUp(self):
        super(CollectionsNetworkSplit, self).setUp()
        self.involve_orchestrator = self.input.param("involve_orchestrator", True)
        self.subsequent_action = self.input.param("subsequent-action", "rebalance-out")
        self.failover_orchestrator = self.input.param("failover_orchestrator", False)
        self.set_master_node()
        self.recovery_type = self.input.param("recovery_type", "delta")
        self.allow_unsafe = self.input.param("allow_unsafe", False)

        self.known_nodes = self.cluster.servers[:self.nodes_init]

    def tearDown(self):
        for server in self.known_nodes:
            shell = RemoteMachineShellConnection(server)
            command = "/sbin/iptables -F"
            shell.execute_command(command)
            shell.disconnect()
        self.sleep(10)
        super(CollectionsNetworkSplit, self).tearDown()

    def set_master_node(self, node=None):
        """
        Set master node to 'node' if given.
        else:
            changes the master node to third init node if all the below conditions are met:
            a. subsequent rebalance action is rebalance-out &
            b. if it involves orchestrator &
            c. node to be failovered (and rebalanced-out) is orchestrator
        """
        if node:
            self.master = self.cluster.master = node
            self.log.info("changed master node to {0}".format(self.master))
        elif self.subsequent_action == "rebalance-out" and self.involve_orchestrator and self.failover_orchestrator:
            self.master = self.cluster.master = self.cluster.servers[2]
            self.log.info("changed master node to {0}".format(self.master))

    def block_traffic_between_two_nodes(self, node1, node2):
        shell = RemoteMachineShellConnection(node1)
        self.log.info("Blocking traffic from {0} in {1}"
                            .format(node2.ip, node1.ip))
        command = "iptables -A INPUT -s {0} -j DROP".format(node2.ip)
        shell.execute_command(command)
        shell.disconnect()

    def remove_network_split(self):
        for node in self.nodes_affected:
            shell = RemoteMachineShellConnection(node)
            command = "/sbin/iptables -F"
            shell.execute_command(command)
            shell.disconnect()

    def pick_nodes_and_network_split(self):
        """
        Does split brain (non-mutually-exclusive split)
        ie; one node is common across both halves
        """
        if self.involve_orchestrator:
            self.node1 = self.cluster.servers[0]
            self.node2 = self.cluster.servers[1]
        else:
            self.node1 = self.cluster.servers[1]
            self.node2 = self.cluster.servers[2]
        self.block_traffic_between_two_nodes(self.node1, self.node2)
        self.block_traffic_between_two_nodes(self.node2, self.node1)
        if self.failover_orchestrator:
            self.nodes_failover = [self.node1]
        else:
            self.nodes_failover = [self.node2]
        self.nodes_affected = [self.node1, self.node2]

    def split_the_cluster_into_two_halves(self):
        """
        Splits the entire cluster into 2 symmetric/asymmetric
        separate mutually-exclusive halves
        Note: First_half will contain the majority incase of asymmetric-split
        Returns first_half_nodes, second_half_nodes
        """
        len_first_half = int(math.ceil(self.nodes_init/2.0))
        first_half_nodes = self.known_nodes[:len_first_half] # will always have the majority
        second_half_nodes = self.known_nodes[len_first_half:]
        for first_half_node in first_half_nodes:
            for second_half_node in second_half_nodes:
                self.block_traffic_between_two_nodes(first_half_node, second_half_node)
                self.block_traffic_between_two_nodes(second_half_node, first_half_node)
        self.set_master_node(second_half_nodes[0])
        return first_half_nodes, second_half_nodes

    @staticmethod
    def get_common_spec():
        spec = {
            # Scope/Collection ops params
            MetaCrudParams.COLLECTIONS_TO_FLUSH: 0,
            MetaCrudParams.COLLECTIONS_TO_DROP: 40,

            MetaCrudParams.SCOPES_TO_DROP: 2,
            MetaCrudParams.SCOPES_TO_ADD_PER_BUCKET: 2,
            MetaCrudParams.COLLECTIONS_TO_ADD_FOR_NEW_SCOPES: 40,

            MetaCrudParams.COLLECTIONS_TO_ADD_PER_BUCKET: 40,

            MetaCrudParams.BUCKET_CONSIDERED_FOR_OPS: "all",
            MetaCrudParams.SCOPES_CONSIDERED_FOR_OPS: "all",
            MetaCrudParams.COLLECTIONS_CONSIDERED_FOR_OPS: "all",

            # Doc loading params
            "doc_crud": {

                MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS: 100,

                MetaCrudParams.DocCrud.COMMON_DOC_KEY: "test_collections",
                MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION: 5,
                MetaCrudParams.DocCrud.READ_PERCENTAGE_PER_COLLECTION: 0,
                MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION: 5,
                MetaCrudParams.DocCrud.REPLACE_PERCENTAGE_PER_COLLECTION: 0,
                MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION: 5,
            },

            "subdoc_crud": {
                MetaCrudParams.SubDocCrud.XATTR_TEST: False,

                MetaCrudParams.SubDocCrud.INSERT_PER_COLLECTION: 0,
                MetaCrudParams.SubDocCrud.UPSERT_PER_COLLECTION: 0,
                MetaCrudParams.SubDocCrud.REMOVE_PER_COLLECTION: 0,
                MetaCrudParams.SubDocCrud.LOOKUP_PER_COLLECTION: 0,
            },

            MetaCrudParams.SUPPRESS_ERROR_TABLE: True,

            MetaCrudParams.COLLECTIONS_CONSIDERED_FOR_CRUD: "all",
            MetaCrudParams.SCOPES_CONSIDERED_FOR_CRUD: "all",
            MetaCrudParams.BUCKETS_CONSIDERED_FOR_CRUD: "all"
        }
        return spec

    def wait_for_async_data_load_to_complete(self, task):
        self.task.jython_task_manager.get_task_result(task)
        self.bucket_util.validate_doc_loading_results(task)
        if task.result is False:
            self.fail("Doc_loading failed")

    def data_validation_collection(self):
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_docs_per_collections_all_buckets()

    def set_retry_exceptions(self, doc_loading_spec):
        retry_exceptions = list()
        retry_exceptions.append(SDKException.AmbiguousTimeoutException)
        retry_exceptions.append(SDKException.TimeoutException)
        retry_exceptions.append(SDKException.RequestCanceledException)
        retry_exceptions.append(SDKException.DocumentNotFoundException)
        if self.durability_level:
            retry_exceptions.append(SDKException.DurabilityAmbiguousException)
            retry_exceptions.append(SDKException.DurabilityImpossibleException)
        doc_loading_spec[MetaCrudParams.RETRY_EXCEPTIONS] = retry_exceptions

    def data_load(self, async_load=False):
        doc_loading_spec = self.get_common_spec()
        self.over_ride_doc_loading_template_params(doc_loading_spec)
        self.set_retry_exceptions(doc_loading_spec)
        tasks = self.bucket_util.run_scenario_from_spec(self.task,
                                                        self.cluster,
                                                        self.bucket_util.buckets,
                                                        doc_loading_spec,
                                                        mutation_num=0,
                                                        async_load=async_load,
                                                        batch_size=self.batch_size)
        return tasks

    def test_collections_crud_with_network_split(self):
        """
        0. Start async data load
        1. Simulate split-brain scenario by introducing network partition with parallel data load
        2. (Hard) Failover the node + data load in parallel
            -> Failover orchestrator if involve_orchestrator and failover_orchestrator are set true. else the other node
        3. Sync Data load after failover
        4. Rebalance-out/ delta-recover/ full recover the nodes with data load in parallel
        """
        task = self.data_load(async_load=True)
        self.pick_nodes_and_network_split()
        self.sleep(60, "wait for network split to finish")

        result = self.task.failover(self.known_nodes, failover_nodes=self.nodes_failover,
                           graceful=False, allow_unsafe=self.allow_unsafe)
        self.assertTrue(result, "Hard Failover failed")
        self.wait_for_async_data_load_to_complete(task)
        self.data_load()

        if self.subsequent_action == "rebalance-out":
            task = self.data_load(async_load=True)
            result = self.task.rebalance(self.known_nodes, [], self.nodes_failover)
            self.assertTrue(result,"Rebalance-out failed")
            self.wait_for_async_data_load_to_complete(task)
            #self.data_validation_collection()
            self.remove_network_split()
        else:
            self.remove_network_split()
            self.sleep(30, "wait for iptables rules to take effect")
            for failover_node in self.failover_nodes:
                self.rest.set_recovery_type(otpNode='ns_1@' + failover_node.ip, recoveryType=self.recovery_type)
            task = self.data_load(async_load=True)
            result = self.task.rebalance(self.known_nodes, [], [])
            self.assertTrue(result, "Rebalance-in failed")
            self.wait_for_async_data_load_to_complete(task)
            #self.data_validation_collection()

    def test_quorum_loss_with_network_split(self):
        """
        0. Start async data load
        1. Split into symmetric/asymmetric two halves
        2. Create some collections on second-half cluster
        2. Quorum loss (majority half) failover
        """
        task = self.data_load(async_load=True)
        first_half_nodes, second_half_nodes = self.split_the_cluster_into_two_halves()
        self.sleep(60, "Wait for network split to finish")
        # TODO: Collection creation on second-half is failing with 500 status error
        # BucketUtils.create_collections(
        #     self.cluster,
        #     self.bucket_util.buckets[0],
        #     5,
        #     CbServer.default_scope,
        #     collection_name="collection_from_second_half")
        self.log.info("First half nodes {0}".format(first_half_nodes))
        self.log.info("Second half nodes {0}".format(second_half_nodes))
        self.log.info("Failing over nodes: {0}".format(first_half_nodes))
        result = self.task.failover(self.known_nodes, failover_nodes=first_half_nodes,
                                    graceful=False, allow_unsafe=self.allow_unsafe,
                                    all_at_once=True)
        self.assertTrue(result, "Hard Failover failed")
        result = self.task.rebalance(second_half_nodes, [], [])
        self.assertTrue(result, "Rebalance failed")
        self.wait_for_async_data_load_to_complete(task)

    def test_MB_41383(self):
        """
        1. Introduce network split between orchestrator(node1) and the last node.
        2. Create collections on node1
        3. Create collections on the last node.
        4. Perform data validation
        """
        self.involve_orchestrator = True
        self.node1 = self.cluster.servers[0]
        self.node2 = self.cluster.servers[self.nodes_init-1]
        self.block_traffic_between_two_nodes(self.node1, self.node2)
        self.block_traffic_between_two_nodes(self.node2, self.node1)
        self.sleep(120, "wait for network split to finish")

        BucketUtils.create_collections(
            self.cluster,
            self.bucket_util.buckets[0],
            5,
            CbServer.default_scope,
            collection_name="collection_from_first_node")

        self.cluster.master = self.master = self.node2
        BucketUtils.create_collections(
            self.cluster,
            self.bucket_util.buckets[0],
            5,
            CbServer.default_scope,
            collection_name="collection_from_last_node")
        self.remove_network_split()
        self.sleep(30, "wait for iptables rules to take effect")
        self.data_validation_collection()