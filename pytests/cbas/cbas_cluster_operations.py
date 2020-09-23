# -*- coding: utf-8 -*-
import datetime
import json

from TestInput import TestInputSingleton
from cbas_base import CBASBaseTest
from cbas_utils.cbas_utils import CbasUtil
from couchbase_helper.tuq_generators import JsonGenerator
from membase.api.rest_client import RestConnection, RestHelper
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient


class CBASClusterOperations(CBASBaseTest):
    def setUp(self):
        self.input = TestInputSingleton.input
        self.input.test_params.update({"default_bucket":False})
        self.rebalanceServers = None
        self.nodeType = "KV"
        self.wait_for_rebalance=True
        super(CBASClusterOperations, self).setUp()
        self.num_items = self.input.param("items", 1000)
        self.bucket_util.create_default_bucket(storage=self.bucket_storage)
#         self.cbas_util.createConn("default")
        if 'nodeType' in self.input.test_params:
            self.nodeType = self.input.test_params['nodeType']

        self.rebalance_both = self.input.param("rebalance_cbas_and_kv", False)
        if not self.rebalance_both:
            if self.nodeType == "KV":
                self.rebalanceServers = self.cluster.kv_nodes
                self.wait_for_rebalance=False
            elif self.nodeType == "CBAS":
                self.rebalanceServers = [self.cbas_node] + self.cluster.cbas_nodes
        else:
            self.rebalanceServers = self.cluster.kv_nodes + [self.cbas_node] + self.cluster.cbas_nodes
            self.nodeType = "KV" + "-" +"CBAS"

        self.assertTrue(len(self.rebalanceServers)>1, "Not enough %s servers to run tests."%self.rebalanceServers)
        self.log.info("This test will be running in %s context."%self.nodeType)
        self.load_gen_tasks = []

    def setup_for_test(self, skip_data_loading=False):
        if not skip_data_loading:
            # Load Couchbase bucket first.
            self.perform_doc_ops_in_all_cb_buckets("create", 0,
                                                   self.num_items)
        self.cbas_util.createConn(self.cb_bucket_name)
        # Create dataset on the CBAS bucket
        self.cbas_util.create_dataset_on_bucket(cbas_bucket_name=self.cb_bucket_name,
                                      cbas_dataset_name=self.cbas_dataset_name, compress_dataset=self.compress_dataset)

        # Create indexes on the CBAS bucket
        self.create_secondary_indexes = self.input.param("create_secondary_indexes",False)
        if self.create_secondary_indexes:
            self.index_fields = "profession:string,number:bigint"
            create_idx_statement = "create index {0} on {1}({2});".format(
                self.index_name, self.cbas_dataset_name, self.index_fields)
            status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(
                create_idx_statement)

            self.assertTrue(status == "success", "Create Index query failed")

            self.assertTrue(
                self.cbas_util.verify_index_created(self.index_name, self.index_fields.split(","),
                                          self.cbas_dataset_name)[0])

        # Connect to Bucket
        self.cbas_util.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                               cb_bucket_password=self.cb_bucket_password)

        if not skip_data_loading:
            # Validate no. of items in CBAS dataset
            if not self.cbas_util.validate_cbas_dataset_items_count(
                    self.cbas_dataset_name,
                    self.num_items):
                self.fail(
                    "No. of items in CBAS dataset do not match that in the CB bucket")

    def test_rebalance_in(self):
        '''
        Description: This will test the rebalance in feature i.e. one node coming in to the cluster.
        Then Rebalance. Verify that is has no effect on the data ingested to cbas.

        Steps:
        1. Setup cbas. bucket, datasets/shadows, connect.
        2. Add a node and rebalance. Don't wait for rebalance completion.
        3. During rebalance, do mutations and execute queries on cbas.

        Author: Ritesh Agarwal/Mihir Kamdar
        Date Created: 18/07/2017
        '''
        query = "select count(*) from {0};".format(self.cbas_dataset_name)

        self.setup_for_test()
        self.cluster_util.add_node(node=self.rebalanceServers[1], rebalance=True, wait_for_rebalance_completion=self.wait_for_rebalance)
        self.log.info("Rebalance state:%s"%self.rest._rebalance_progress_status())

        self.perform_doc_ops_in_all_cb_buckets("create",
                                               self.num_items,
                                               self.num_items * 2)

        self.log.info("Rebalance state:%s"%self.rest._rebalance_progress_status())
        self.cbas_util._run_concurrent_queries(query,None,2000,batch_size=self.concurrent_batch_size)

        self.log.info("Rebalance state:%s"%self.rest._rebalance_progress_status())

        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items * 2,
                                                      0):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def test_rebalance_out(self):
        '''
        Description: This will test the rebalance out feature i.e. one node going out of cluster.
        Then Rebalance.

        Steps:
        1. Add a node, Rebalance.
        2. Setup cbas. bucket, datasets/shadows, connect.
        3. Remove a node and rebalance. Don't wait for rebalance completion.
        4. During rebalance, do mutations and execute queries on cbas.

        Author: Ritesh Agarwal/Mihir Kamdar
        Date Created: 18/07/2017
        '''
        self.cluster_util.add_node(node=self.rebalanceServers[1])
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.setup_for_test()
        otpnodes = []
        nodes = self.rest.node_statuses()
        for node in nodes:
            if node.ip == self.rebalanceServers[1].ip:
                otpnodes.append(node)
        self.remove_node(otpnodes, wait_for_rebalance=self.wait_for_rebalance)
        self.log.info("Rebalance state:%s"%self.rest._rebalance_progress_status())

        self.perform_doc_ops_in_all_cb_buckets("create",
                                               self.num_items,
                                               self.num_items * 2)

        self.cbas_util._run_concurrent_queries(query,"immediate",2000,batch_size=self.concurrent_batch_size)

        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items * 2, 0):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def test_swap_rebalance(self):
        '''
        Description: This will test the swap rebalance feature i.e. one node going out and one node coming in cluster.
        Then Rebalance. Verify that is has no effect on the data ingested to cbas.

        Steps:
        1. Setup cbas. bucket, datasets/shadows, connect.
        2. Add a node that is to be swapped against the leaving node. Do not rebalance.
        3. Remove a node and rebalance.
        4. During rebalance, do mutations and execute queries on cbas.

        Author: Ritesh Agarwal/Mihir Kamdar
        Date Created: 20/07/2017
        '''
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.setup_for_test()

        otpnodes=[]
        nodes = self.rest.node_statuses()
        if self.nodeType == "KV":
            service = ["kv"]
        else:
            service = ["cbas"]
        otpnodes.append(self.cluster_util.add_node(node=self.servers[1], services=service))
        self.cluster_util.add_node(node=self.servers[3], services=service,rebalance=False)
        self.remove_node(otpnodes, wait_for_rebalance=self.wait_for_rebalance)

        self.perform_doc_ops_in_all_cb_buckets("create",
                                               self.num_items,
                                               self.num_items * 2)

        self.cbas_util._run_concurrent_queries(query,"immediate",2000,batch_size=self.concurrent_batch_size)

        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items * 2, 0):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    def test_failover(self):
        '''
        Description: This will test the node failover both graceful and hard failover based on
        graceful_failover param in testcase conf file.

        Steps:
        1. Add node to the cluster which will be failed over.
        2. Create docs, setup cbas.
        3. Mark the node for fail over.
        4. Do rebalance asynchronously. During rebalance perform mutations.
        5. Run some CBAS queries.
        6. Check for correct number of items in CBAS datasets.

        Author: Ritesh Agarwal/Mihir Kamdar
        Date Created: 20/07/2017
        '''

        #Add node which will be failed over later.
        self.cluster_util.add_node(node=self.rebalanceServers[1])
        query = "select count(*) from {0};".format(self.cbas_dataset_name)

        graceful_failover = self.input.param("graceful_failover", False)
        self.setup_for_test()
        failover_task = self._cb_cluster.async_failover(self.input.servers,
                                                        [self.rebalanceServers[1]],
                                                        graceful_failover)
        self.task_manager.get_task_result(failover_task)

        result = self.cluster_util.rebalance()
        self.assertTrue(result, "Rebalance operation failed")
        self.perform_doc_ops_in_all_cb_buckets("create",
                                               self.num_items,
                                               self.num_items * 3 / 2)

        self.cbas_util._run_concurrent_queries(query,"immediate",2000,batch_size=self.concurrent_batch_size)

        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,
                                                      self.num_items * 3 / 2,
                                                      0):
            self.fail(
                "No. of items in CBAS dataset do not match that in the CB bucket")

    '''
    -i b/resources/4-nodes-template.ini -t cbas.cbas_cluster_operations.CBASClusterOperations.test_rebalance_in_cb_cbas_together,cb_bucket_name=default,cbas_bucket_name=default_bucket,cbas_dataset_name=default_ds,items=10,nodeType=KV,rebalance_cbas_and_kv=True,wait_for_rebalace=False
    '''

    def test_rebalance_in_cb_cbas_together(self):

        self.log.info("Creates cbas buckets and dataset")
        dataset_count_query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.setup_for_test()

        self.log.info("Rebalance in KV node")
        wait_for_rebalace_complete = self.input.param("wait_for_rebalace", False)
        self.cluster_util.add_node(node=self.rebalanceServers[1], rebalance=False,
                      wait_for_rebalance_completion=wait_for_rebalace_complete)

        self.log.info("Rebalance in CBAS node")
        self.cluster_util.add_node(node=self.rebalanceServers[3], rebalance=True,
                      wait_for_rebalance_completion=wait_for_rebalace_complete)

        self.log.info(
            "Perform document create as rebalance is in progress : Rebalance state:%s" % self.rest._rebalance_progress_status())
        self.perform_doc_ops_in_all_cb_buckets("create", self.num_items, self.num_items * 2)

        self.log.info(
            "Run queries as rebalance is in progress : Rebalance state:%s" % self.rest._rebalance_progress_status())
        handles = self.cbas_util._run_concurrent_queries(dataset_count_query, None, 2000, batch_size=self.concurrent_batch_size)

        self.log.info("Log concurrent query status")
        self.cbas_util.log_concurrent_query_outcome(self.cluster.master, handles)

        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items * 2, 0):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")

    '''
    -i b/resources/4-nodes-template.ini -t cbas.cbas_cluster_operations.CBASClusterOperations.test_rebalance_out_cb_cbas_together,cb_bucket_name=default,cbas_bucket_name=default_bucket,cbas_dataset_name=default_ds,items=10,nodeType=KV,rebalance_cbas_and_kv=True,wait_for_rebalace=False
    '''

    def test_rebalance_out_cb_cbas_together(self):

        self.log.info("Rebalance in KV node and  wait for rebalance to complete")
        self.cluster_util.add_node(node=self.rebalanceServers[1])

        self.log.info("Rebalance in CBAS node and  wait for rebalance to complete")
        self.cluster_util.add_node(node=self.rebalanceServers[3])

        self.log.info("Creates cbas buckets and dataset")
        dataset_count_query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.setup_for_test()

        self.log.info("Fetch and remove nodes to rebalance out")
        wait_for_rebalace_complete = self.input.param("wait_for_rebalace", False)
        otpnodes = []
        nodes = self.rest.node_statuses()
        for node in nodes:
            if node.ip == self.rebalanceServers[1].ip or node.ip == self.rebalanceServers[3].ip:
                otpnodes.append(node)

        for every_node in otpnodes:
            self.remove_node(otpnodes, wait_for_rebalance=wait_for_rebalace_complete)

        self.sleep(30, message="Sleep for 30 seconds for remove node to complete")

        self.log.info(
            "Perform document create as rebalance is in progress : Rebalance state:%s" % self.rest._rebalance_progress_status())
        self.perform_doc_ops_in_all_cb_buckets("create", self.num_items, self.num_items * 2)

        self.log.info(
            "Run queries as rebalance is in progress : Rebalance state:%s" % self.rest._rebalance_progress_status())
        handles = self.cbas_util._run_concurrent_queries(dataset_count_query, "immediate", 2000,
                                               batch_size=self.concurrent_batch_size)

        self.log.info("Log concurrent query status")
        self.cbas_util.log_concurrent_query_outcome(self.cluster.master, handles)

        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items * 2, 0):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")

    '''
    -i b/resources/4-nodes-template.ini -t cbas.cbas_cluster_operations.CBASClusterOperations.test_swap_rebalance_cb_cbas_together,cb_bucket_name=default,cbas_bucket_name=default_bucket,cbas_dataset_name=default_ds,items=10,rebalance_cbas_and_kv=True,wait_for_rebalance=True
    '''

    def test_swap_rebalance_cb_cbas_together(self):

        self.log.info("Creates cbas buckets and dataset")
        wait_for_rebalance = self.input.param("wait_for_rebalance", True)
        dataset_count_query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.setup_for_test()

        self.log.info("Add KV node and don't rebalance")
        self.cluster_util.add_node(node=self.rebalanceServers[1], rebalance=False)

        self.log.info("Add cbas node and don't rebalance")
        self.cluster_util.add_node(node=self.rebalanceServers[3], rebalance=False)

        otpnodes = []
        nodes = self.rest.node_statuses()
        for node in nodes:
            if node.ip == self.rebalanceServers[0].ip or node.ip == self.rebalanceServers[2].ip:
                otpnodes.append(node)

        self.log.info("Remove master node")
        self.remove_node(otpnode=otpnodes, wait_for_rebalance=wait_for_rebalance)
        self.cluster.master = self.rebalanceServers[1]

        self.log.info("Create instances pointing to new master nodes")
        c_utils = CbasUtil(self.rebalanceServers[1], self.rebalanceServers[3], self.task)
        c_utils.createConn(self.cb_bucket_name)

        self.log.info("Create reference to SDK client")
        client = SDKClient(scheme="couchbase", hosts=[self.rebalanceServers[1].ip], bucket=self.cb_bucket_name,
                           password=self.rebalanceServers[1].rest_password)

        self.log.info("Add more document to default bucket")
        documents = ['{"name":"value"}'] * (self.num_items//10)
        document_id_prefix = "custom-id-"
        client.insert_custom_json_documents(document_id_prefix, documents)

        self.log.info(
            "Run queries as rebalance is in progress : Rebalance state:%s" % self.rest._rebalance_progress_status())
        handles = c_utils._run_concurrent_queries(dataset_count_query, "immediate", 2000,
                                               batch_size=self.concurrent_batch_size)

        self.log.info("Log concurrent query status")
        self.cbas_util.log_concurrent_query_outcome(self.cluster.master, handles)

        if not c_utils.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items + (self.num_items//10) , 0):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")

    def test_rebalance_in_multiple_cbas_on_a_busy_system(self):
        node_services = []
        node_services.append(self.input.param('service',"cbas"))
        self.log.info("Setup CBAS")
        self.setup_for_test(skip_data_loading=True)

        self.log.info("Run KV ops in async while rebalance is in progress")
        json_generator = JsonGenerator()
        generators = json_generator.generate_docs_simple(docs_per_day=self.num_items, start=0)
        tasks = self.bucket_util._async_load_all_buckets(self.cluster, generators, "create", 0)

        self.log.info("Run concurrent queries to simulate busy system")
        statement = "select sleep(count(*),50000) from {0} where mutated=0;".format(self.cbas_dataset_name)
        handles = self.cbas_util._run_concurrent_queries(statement, self.mode, self.num_concurrent_queries)

        self.log.info("Rebalance in CBAS nodes")
        self.cluster_util.add_node(node=self.rebalanceServers[1], services=node_services, rebalance=False, wait_for_rebalance_completion=False)
        self.cluster_util.add_node(node=self.rebalanceServers[3], services=node_services, rebalance=True, wait_for_rebalance_completion=True)

        self.log.info("Get KV ops result")
        for task in tasks:
            self.task_manager.get_task_result(task)

        self.log.info("Log concurrent query status")
        self.cbas_util.log_concurrent_query_outcome(self.cluster.master, handles)

        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items, 0):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")

    def test_rebalance_out_multiple_cbas_on_a_busy_system(self):
        node_services = []
        node_services.append(self.input.param('service',"cbas"))
        self.log.info("Rebalance in CBAS nodes")
        self.cluster_util.add_node(node=self.rebalanceServers[1], services=node_services)
        self.cluster_util.add_node(node=self.rebalanceServers[3], services=node_services)

        self.log.info("Setup CBAS")
        self.setup_for_test(skip_data_loading=True)

        self.log.info("Run KV ops in async while rebalance is in progress")
        json_generator = JsonGenerator()
        generators = json_generator.generate_docs_simple(docs_per_day=self.num_items, start=0)
        tasks = self.bucket_util._async_load_all_buckets(self.cluster, generators, "create", 0)

        self.log.info("Run concurrent queries to simulate busy system")
        statement = "select sleep(count(*),50000) from {0} where mutated=0;".format(self.cbas_dataset_name)
        handles = self.cbas_util._run_concurrent_queries(statement, self.mode, self.num_concurrent_queries)

        self.log.info("Fetch and remove nodes to rebalance out")
        self.rebalance_cc = self.input.param("rebalance_cc", False)
        out_nodes = []
        nodes = self.rest.node_statuses()

        if self.rebalance_cc:
            for node in nodes:
                if node.ip == self.cbas_node.ip or node.ip == self.servers[1].ip:
                    out_nodes.append(node)
            self.cbas_util.closeConn()
            self.log.info("Reinitialize CBAS utils with ip %s, since CC node is rebalanced out" %self.servers[3].ip)
            self.cbas_util = CbasUtil(self.cluster.master, self.servers[3], self.task)
            self.cbas_util.createConn("default")
        else:
            for node in nodes:
                if node.ip == self.servers[3].ip or node.ip == self.servers[1].ip:
                    out_nodes.append(node)

        self.log.info("Rebalance out CBAS nodes %s %s" % (out_nodes[0].ip, out_nodes[1].ip))
        self.remove_all_nodes_then_rebalance([out_nodes[0],out_nodes[1]])

        self.log.info("Get KV ops result")
        for task in tasks:
            self.task_manager.get_task_result(task)

        self.log.info("Log concurrent query status")
        self.cbas_util.log_concurrent_query_outcome(self.cluster.master, handles)

        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items, 0):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")

    '''
    cbas.cbas_cluster_operations.CBASClusterOperations.test_rebalance_swap_multiple_cbas_on_a_busy_system,cb_bucket_name=default,cbas_bucket_name=default_bucket,cbas_dataset_name=default_ds,items=10,rebalance_cbas_and_kv=True,service=cbas,rebalance_cc=False
    cbas.cbas_cluster_operations.CBASClusterOperations.test_rebalance_swap_multiple_cbas_on_a_busy_system,cb_bucket_name=default,cbas_bucket_name=default_bucket,cbas_dataset_name=default_ds,items=10,rebalance_cbas_and_kv=True,service=cbas,rebalance_cc=True
    '''
    def test_rebalance_swap_multiple_cbas_on_a_busy_system(self):
        '''
        1. We have 4 node cluster with 1 KV and 3 CBAS. Assume the IPS end with 101(KV), 102(CBAS), 103(CBAS), 104(CBAS)
        2, Post initial setup - 101 running KV and 102 running CBAS as CC node
        3. As part of test test add an extra NC node that we will swap rebalance later - Adding 103 and rebalance
        4. If swap rebalance NC - then select the node added in #3 for remove and 104 to add during swap
        5. If swap rebalance CC - then select the CC node added for remove and 104 to add during swap
        '''

        self.log.info('Read service input param')
        node_services = []
        node_services.append(self.input.param('service', "cbas"))

        self.log.info("Rebalance in CBAS nodes, this node will be removed during swap")
        self.cluster_util.add_node(node=self.rebalanceServers[1], services=node_services)

        self.log.info("Setup CBAS")
        self.setup_for_test(skip_data_loading=True)

        self.log.info("Run KV ops in async while rebalance is in progress")
        json_generator = JsonGenerator()
        generators = json_generator.generate_docs_simple(docs_per_day=self.num_items, start=0)
        tasks = self.bucket_util._async_load_all_buckets(self.cluster, generators, "create", 0)

        self.log.info("Run concurrent queries to simulate busy system")
        statement = "select sleep(count(*),50000) from {0} where mutated=0;".format(self.cbas_dataset_name)
        handles = self.cbas_util._run_concurrent_queries(statement, self.mode, self.num_concurrent_queries)

        self.log.info("Fetch node to remove during rebalance")
        self.rebalance_cc = self.input.param("rebalance_cc", False)
        out_nodes = []
        nodes = self.rest.node_statuses()
        reinitialize_cbas_util = False
        for node in nodes:
            if self.rebalance_cc and (node.ip == self.cbas_node.ip):
                out_nodes.append(node)
                reinitialize_cbas_util = True
            elif not self.rebalance_cc and node.ip == self.rebalanceServers[1].ip:
                out_nodes.append(node)

        self.log.info("Swap rebalance CBAS nodes")
        self.cluster_util.add_node(node=self.rebalanceServers[3], services=node_services, rebalance=False)
        self.remove_node([out_nodes[0]], wait_for_rebalance=True)

        self.log.info("Get KV ops result")
        for task in tasks:
            self.task_manager.get_task_result(task)

        if reinitialize_cbas_util is True:
            self.cbas_util = CbasUtil(self.cluster.master, self.rebalanceServers[3], self.task)
            self.cbas_util.createConn("default")

        self.log.info("Log concurrent query status")
        self.cbas_util.log_concurrent_query_outcome(self.cluster.master, handles)

        count_n1ql = self.rest.query_tool('select count(*) from %s' % (self.cb_bucket_name))['results'][0]['$1']
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, count_n1ql, 0):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")

    '''
    test_fail_over_node_followed_by_rebalance_out_or_add_back,cb_bucket_name=default,graceful_failover=True,cbas_bucket_name=default_cbas,cbas_dataset_name=default_ds,items=10000,nodeType=KV,rebalance_out=True,concurrent_batch_size=500
    test_fail_over_node_followed_by_rebalance_out_or_add_back,cb_bucket_name=default,graceful_failover=True,cbas_bucket_name=default_cbas,cbas_dataset_name=default_ds,items=10000,nodeType=KV,rebalance_out=False,recovery_strategy=full,concurrent_batch_size=500
    test_fail_over_node_followed_by_rebalance_out_or_add_back,cb_bucket_name=default,graceful_failover=True,cbas_bucket_name=default_cbas,cbas_dataset_name=default_ds,items=10000,nodeType=KV,rebalance_out=False,recovery_strategy=delta,concurrent_batch_size=500

    test_fail_over_node_followed_by_rebalance_out_or_add_back,cb_bucket_name=default,graceful_failover=False,cbas_bucket_name=default_cbas,cbas_dataset_name=default_ds,items=10000,nodeType=KV,rebalance_out=True,concurrent_batch_size=500
    test_fail_over_node_followed_by_rebalance_out_or_add_back,cb_bucket_name=default,graceful_failover=False,cbas_bucket_name=default_cbas,cbas_dataset_name=default_ds,items=10000,nodeType=KV,rebalance_out=False,recovery_strategy=full,concurrent_batch_size=500
    test_fail_over_node_followed_by_rebalance_out_or_add_back,cb_bucket_name=default,graceful_failover=False,cbas_bucket_name=default_cbas,cbas_dataset_name=default_ds,items=10000,nodeType=KV,rebalance_out=False,recovery_strategy=delta,concurrent_batch_size=500

    test_fail_over_node_followed_by_rebalance_out_or_add_back,cb_bucket_name=default,graceful_failover=False,cbas_bucket_name=default_cbas,cbas_dataset_name=default_ds,items=10000,nodeType=CBAS,rebalance_out=True,concurrent_batch_size=500
    test_fail_over_node_followed_by_rebalance_out_or_add_back,cb_bucket_name=default,graceful_failover=False,cbas_bucket_name=default_cbas,cbas_dataset_name=default_ds,items=10000,nodeType=CBAS,rebalance_out=False,recovery_strategy=full,concurrent_batch_size=500
    '''

    def test_fail_over_node_followed_by_rebalance_out_or_add_back(self):
        """
        1. Start with an initial setup, having 1 KV and 1 CBAS
        2. Add a node that will be failed over - KV/CBAS
        3. Create CBAS buckets and dataset
        4. Fail over the KV node based in graceful_failover parameter specified
        5. Rebalance out/add back based on input param specified in conf file
        6. Perform doc operations
        7. run concurrent queries
        8. Verify document count on dataset post failover
        """
        self.log.info("Add an extra node to fail-over")
        self.cluster_util.add_node(node=self.rebalanceServers[1])

        self.log.info("Read the failure out type to be performed")
        graceful_failover = self.input.param("graceful_failover", True)

        self.log.info("Set up test - Create cbas buckets and data-sets")
        self.setup_for_test()

        self.log.info("Perform Async doc operations on KV")
        json_generator = JsonGenerator()
        generators = json_generator.generate_docs_simple(docs_per_day=self.num_items * 3 / 2, start=self.num_items)
        kv_task = self.bucket_util._async_load_all_buckets(self.cluster, generators, "create", 0)

        self.log.info("Run concurrent queries on CBAS")
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        handles = self.cbas_util._run_concurrent_queries(query, "async", self.num_concurrent_queries, batch_size=self.concurrent_batch_size)

        self.log.info("fail-over the node")
        fail_task = self._cb_cluster.async_failover(self.input.servers, [self.rebalanceServers[1]], graceful_failover)
        self.task_manager.get_task_result(fail_task)

        self.log.info("Read input param to decide on add back or rebalance out")
        self.rebalance_out = self.input.param("rebalance_out", False)
        if self.rebalance_out:
            self.log.info("Rebalance out the fail-over node")
            result = self.cluster_util.rebalance()
            self.assertTrue(result, "Rebalance operation failed")
        else:
            self.recovery_strategy = self.input.param("recovery_strategy", "full")
            self.log.info("Performing %s recovery" % self.recovery_strategy)
            success = False
            end_time = datetime.datetime.now() + datetime.timedelta(minutes=int(1))
            while datetime.datetime.now() < end_time or not success:
                try:
                    self.sleep(10, message="Wait for fail over complete")
                    self.rest.set_recovery_type('ns_1@' + self.rebalanceServers[1].ip, self.recovery_strategy)
                    success = True
                except Exception:
                    self.log.info("Fail over in progress. Re-try after 10 seconds.")
                    pass
            if not success:
                self.fail("Recovery %s failed." % self.recovery_strategy)
            self.rest.add_back_node('ns_1@' + self.rebalanceServers[1].ip)
            result = self.cluster_util.rebalance()
            self.assertTrue(result, "Rebalance operation failed")

        self.log.info("Get KV ops result")
        for task in kv_task:
            self.task_manager.get_task_result(task)

        self.log.info("Log concurrent query status")
        self.cbas_util.log_concurrent_query_outcome(self.cluster.master, handles)

        self.log.info("Validate dataset count on CBAS")
        count_n1ql = self.rest.query_tool('select count(*) from `%s`' % self.cb_bucket_name)['results'][0]['$1']
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, count_n1ql, 0, timeout=400, analytics_timeout=400):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")

    '''
    test_to_fail_initial_rebalance_and_verify_subsequent_rebalance_succeeds,cb_bucket_name=default,cbas_bucket_name=default_bucket,cbas_dataset_name=default_ds,items=10,nodeType=CBAS,num_queries=10,restart_couchbase_on_incoming_or_outgoing_node=True,rebalance_type=in
    test_to_fail_initial_rebalance_and_verify_subsequent_rebalance_succeeds,cb_bucket_name=default,cbas_bucket_name=default_bucket,cbas_dataset_name=default_ds,items=10,nodeType=CBAS,num_queries=10,restart_couchbase_on_incoming_or_outgoing_node=True,rebalance_type=out
    test_to_fail_initial_rebalance_and_verify_subsequent_rebalance_succeeds,cb_bucket_name=default,cbas_bucket_name=default_bucket,cbas_dataset_name=default_ds,items=10,nodeType=CBAS,num_queries=10,restart_couchbase_on_incoming_or_outgoing_node=True,rebalance_type=swap
    '''
    def test_to_fail_initial_rebalance_and_verify_subsequent_rebalance_succeeds(self):

        self.log.info("Pick the incoming and outgoing nodes during rebalance")
        self.rebalance_type = self.input.param("rebalance_type", "in")
        nodes_to_add = [self.rebalanceServers[1]]
        nodes_to_remove = []
        reinitialize_cbas_util = False
        if self.rebalance_type == 'out':
            nodes_to_remove.append(self.rebalanceServers[1])
            self.cluster_util.add_node(self.rebalanceServers[1])
            nodes_to_add = []
        elif self.rebalance_type == 'swap':
            self.cluster_util.add_node(nodes_to_add[0], rebalance=False)
            nodes_to_remove.append(self.cbas_node)
            reinitialize_cbas_util = True
        self.log.info("Incoming nodes - %s, outgoing nodes - %s. For rebalance type %s " %(nodes_to_add, nodes_to_remove, self.rebalance_type))

        self.log.info("Creates cbas buckets and dataset")
        dataset_count_query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.setup_for_test()

        self.log.info("Perform async doc operations on KV")
        json_generator = JsonGenerator()
        generators = json_generator.generate_docs_simple(docs_per_day=self.num_items * 3 / 2, start=self.num_items)
        kv_task = self.bucket_util._async_load_all_buckets(self.cluster, generators, "create", 0, batch_size=5000)

        self.log.info("Run concurrent queries on CBAS")
        handles = self.cbas_util._run_concurrent_queries(dataset_count_query, "async", self.num_concurrent_queries)

        self.log.info("Fetch the server to restart couchbase on")
        restart_couchbase_on_incoming_or_outgoing_node = self.input.param("restart_couchbase_on_incoming_or_outgoing_node", True)
        if not restart_couchbase_on_incoming_or_outgoing_node:
            node = self.cbas_node
        else:
            node = self.rebalanceServers[1]
        shell = RemoteMachineShellConnection(node)

        self.log.info("Rebalance nodes")
        self.task.async_rebalance(self.servers, nodes_to_add, nodes_to_remove)

        self.log.info("Restart Couchbase on node %s" % node.ip)
        shell.restart_couchbase()
        self.sleep(30, message="Waiting for service to be back again...")

        self.log.info("Verify subsequent rebalance is successful")
        nodes_to_add = [] # Node is already added to cluster in previous rebalance, adding it again will throw exception
        self.assertTrue(self.task.rebalance(self.servers, nodes_to_add, nodes_to_remove))

        if reinitialize_cbas_util is True:
            self.cbas_util = CbasUtil(self.cluster.master, self.rebalanceServers[1], self.task)
            self.cbas_util.createConn("default")
            self.cbas_util.wait_for_cbas_to_recover()

        self.log.info("Get KV ops result")
        for task in kv_task:
            self.task_manager.get_task_result(task)

        self.log.info("Log concurrent query status")
        self.cbas_util.log_concurrent_query_outcome(self.cluster.master, handles)

        self.log.info("Validate dataset count on CBAS")
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items * 3 / 2, 0):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")

    def test_auto_retry_failed_rebalance(self):

        # Auto-retry rebalance settings
        body = {"enabled": "true", "afterTimePeriod": self.retry_time, "maxAttempts": self.num_retries}
        rest = RestConnection(self.cluster.master)
        rest.set_retry_rebalance_settings(body)
        result = rest.get_retry_rebalance_settings()

        self.log.info("Pick the incoming and outgoing nodes during rebalance")
        self.rebalance_type = self.input.param("rebalance_type", "in")
        nodes_to_add = [self.rebalanceServers[1]]
        nodes_to_remove = []
        reinitialize_cbas_util = False
        if self.rebalance_type == 'out':
            nodes_to_remove.append(self.rebalanceServers[1])
            self.cluster_util.add_node(self.rebalanceServers[1])
            nodes_to_add = []
        elif self.rebalance_type == 'swap':
            self.cluster_util.add_node(nodes_to_add[0], rebalance=False)
            nodes_to_remove.append(self.cbas_node)
            reinitialize_cbas_util = True
        self.log.info("Incoming nodes - %s, outgoing nodes - %s. For rebalance type %s " % (
        nodes_to_add, nodes_to_remove, self.rebalance_type))

        self.log.info("Creates cbas buckets and dataset")
        dataset_count_query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.setup_for_test()

        self.log.info("Perform async doc operations on KV")
        json_generator = JsonGenerator()
        generators = json_generator.generate_docs_simple(docs_per_day=self.num_items * 3 / 2, start=self.num_items)
        kv_task = self.bucket_util._async_load_all_buckets(self.cluster, generators, "create", 0, batch_size=5000)

        self.log.info("Run concurrent queries on CBAS")
        handles = self.cbas_util._run_concurrent_queries(dataset_count_query, "async", self.num_concurrent_queries)

        self.log.info("Fetch the server to restart couchbase on")
        restart_couchbase_on_incoming_or_outgoing_node = self.input.param(
            "restart_couchbase_on_incoming_or_outgoing_node", True)
        if not restart_couchbase_on_incoming_or_outgoing_node:
            node = self.cbas_node
        else:
            node = self.rebalanceServers[1]
        shell = RemoteMachineShellConnection(node)

        try:
            self.log.info("Rebalance nodes")
            self.task.async_rebalance(self.servers, nodes_to_add, nodes_to_remove)

            self.sleep(10, message="Restarting couchbase after 10s on node %s" % node.ip)

            shell.restart_couchbase()
            self.sleep(30, message="Waiting for service to be back again...")

            self.sleep(self.retry_time, "Wait for retry time to complete and then check the rebalance results")

            reached = RestHelper(self.rest).rebalance_reached(wait_step=120)
            self.log.info("Rebalance status : {0}".format(reached))
            self.sleep(20)

            self._check_retry_rebalance_succeeded()

            if reinitialize_cbas_util is True:
                self.cbas_util = CbasUtil(self.cluster.master, self.rebalanceServers[1], self.task)
                self.cbas_util.createConn("default")
                self.cbas_util.wait_for_cbas_to_recover()

            self.log.info("Get KV ops result")
            for task in kv_task:
                self.task_manager.get_task_result(task)

            self.log.info("Log concurrent query status")
            self.cbas_util.log_concurrent_query_outcome(self.cluster.master, handles)

            self.log.info("Validate dataset count on CBAS")
            if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items * 3 / 2, 0):
                self.fail("No. of items in CBAS dataset do not match that in the CB bucket")
        except Exception as e:
            self.fail("Some exception occurred : {0}".format(e.message))


        finally:
            body = {"enabled": "false"}
            rest.set_retry_rebalance_settings(body)
    
    '''
    test_rebalance_on_nodes_running_multiple_services,cb_bucket_name=default,cbas_bucket_name=default_bucket,cbas_dataset_name=default_ds,items=10,nodeType=KV,num_queries=10,rebalance_type=in
    test_rebalance_on_nodes_running_multiple_services,cb_bucket_name=default,cbas_bucket_name=default_bucket,cbas_dataset_name=default_ds,items=10,nodeType=KV,num_queries=10,rebalance_type=out
    test_rebalance_on_nodes_running_multiple_services,cb_bucket_name=default,cbas_bucket_name=default_bucket,cbas_dataset_name=default_ds,items=10,num_queries=10,rebalance_type=swap,rebalance_cbas_and_kv=True
    '''
    def test_rebalance_on_nodes_running_multiple_services(self):

        self.log.info("Pick the incoming and outgoing nodes during rebalance")
        active_services = ['cbas,fts,kv']
        self.rebalance_type = self.input.param("rebalance_type", "in")
        nodes_to_add = [self.rebalanceServers[1]]
        nodes_to_remove = []
        if self.rebalance_type == 'out':
            # This node will be rebalanced out
            nodes_to_remove.append(self.rebalanceServers[1])
            # Will be running services as specified in the list - active_services
            self.cluster_util.add_node(nodes_to_add[0], services=active_services)
            # No nodes to remove so making the add notes empty
            nodes_to_add = []
        elif self.rebalance_type == 'swap':
            # Below node will be swapped with the incoming node specified in nodes_to_add
            self.cluster_util.add_node(nodes_to_add[0], services=active_services)
            nodes_to_add = []
            nodes_to_add.append(self.rebalanceServers[3])
            # Below node will be removed and swapped with node that was added earlier
            nodes_to_remove.append(self.rebalanceServers[1])

        self.log.info("Incoming nodes - %s, outgoing nodes - %s. For rebalance type %s " % (
        nodes_to_add, nodes_to_remove, self.rebalance_type))

        self.log.info("Creates cbas buckets and dataset")
        dataset_count_query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.setup_for_test()

        self.log.info("Perform async doc operations on KV")
        json_generator = JsonGenerator()
        generators = json_generator.generate_docs_simple(docs_per_day=self.num_items * 3 / 2, start=self.num_items)
        kv_task = self.bucket_util._async_load_all_buckets(self.cluster, generators, "create", 0, batch_size=5000)

        self.log.info("Run concurrent queries on CBAS")
        handles = self.cbas_util._run_concurrent_queries(dataset_count_query, "async", self.num_concurrent_queries)

        self.log.info("Rebalance nodes")
        # Do not add node to nodes_to_add if already added as add_node earlier
        self.task.rebalance(self.servers, nodes_to_add, nodes_to_remove, services=active_services)

        self.log.info("Get KV ops result")
        for task in kv_task:
            self.task_manager.get_task_result(task)

        self.log.info("Log concurrent query status")
        self.cbas_util.log_concurrent_query_outcome(self.cluster.master, handles)

        self.log.info("Validate dataset count on CBAS")
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items * 3 / 2, 0):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")

    def tearDown(self):
        super(CBASClusterOperations, self).tearDown()

    def _check_retry_rebalance_succeeded(self):
        rest = RestConnection(self.cluster.master)
        result = json.loads(rest.get_pending_rebalance_info())
        self.log.info(result)

        if "retry_rebalance" in result and result["retry_rebalance"] != "not_pending":
            retry_after_secs = result["retry_after_secs"]
            attempts_remaining = result["attempts_remaining"]
            retry_rebalance = result["retry_rebalance"]
            self.log.info("Attempts remaining : {0}, Retry rebalance : {1}".format(attempts_remaining, retry_rebalance))
            while attempts_remaining:
            # wait for the afterTimePeriod for the failed rebalance to restart
                self.sleep(retry_after_secs, message="Waiting for the afterTimePeriod to complete")
                try:
                    result = self.rest.monitorRebalance()
                    msg = "monitoring rebalance {0}"
                    self.log.info(msg.format(result))
                except Exception:
                    result = json.loads(self.rest.get_pending_rebalance_info())
                    self.log.info(result)
                    try:
                        attempts_remaining = result["attempts_remaining"]
                        retry_rebalance = result["retry_rebalance"]
                        retry_after_secs = result["retry_after_secs"]
                    except KeyError:
                        self.fail("Retrying of rebalance still did not help. All the retries exhausted...")
                    self.log.info("Attempts remaining : {0}, Retry rebalance : {1}".format(attempts_remaining,
                                                                                       retry_rebalance))
                else:
                    self.log.info("Retry rebalanced fixed the rebalance failure")
                    break


class MultiNodeFailOver(CBASBaseTest):
    """
    Class contains test cases for multiple analytics node failures.[CC+NC, NC+NC]
    """

    def setUp(self):
        super(MultiNodeFailOver, self).setUp()

        self.log.info("Read the input params")
        self.nc_nc_fail_over = self.input.param("nc_nc_fail_over", True)
        self.create_secondary_indexes = self.input.param("create_secondary_indexes", False)
        # In this fail over we fail first 3 added cbas nodes[CC + first NC + Second NC]
        self.meta_data_node_failure = self.input.param("meta_data_node_failure", False)

        self.log.info("Add CBAS nodes to cluster")
        self.assertIsNotNone(self.cluster_util.add_node(self.cluster.cbas_nodes[0], services=["cbas"], rebalance=False), msg="Add node failed")
        self.assertIsNotNone(self.cluster_util.add_node(self.cluster.cbas_nodes[1], services=["cbas"], rebalance=True), msg="Add node failed")
        # This node won't be failed over
        if self.meta_data_node_failure:
            self.assertIsNotNone(self.cluster_util.add_node(self.cluster.cbas_nodes[2], services=["cbas"], rebalance=True), msg="Add node failed")

        self.log.info("Create connection")
        self.cbas_util.createConn(self.cb_bucket_name)

        self.log.info("Load documents in kv bucket")
        self.perform_doc_ops_in_all_cb_buckets("create", 0, self.num_items)

        self.log.info("Create dataset")
        self.cbas_util.create_dataset_on_bucket(self.cb_bucket_name, self.cbas_dataset_name)

        self.log.info("Create secondary index")
        if self.create_secondary_indexes:
            self.index_fields = "profession:string,number:bigint"
            create_idx_statement = "create index {0} on {1}({2});".format(self.index_name, self.cbas_dataset_name, self.index_fields)
            status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(create_idx_statement)
            self.assertTrue(status == "success", "Create Index query failed")
            self.assertTrue(self.cbas_util.verify_index_created(self.index_name, self.index_fields.split(","), self.cbas_dataset_name)[0])

        self.log.info("Connect Local link")
        self.cbas_util.connect_link()

        self.log.info("Validate dataset count")
        self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items)

        self.log.info("Pick nodes to fail over")
        self.fail_over_nodes = []
        if self.nc_nc_fail_over:
            self.log.info("This is NC+NC fail over")
            self.fail_over_nodes.append(self.cluster.cbas_nodes[0])
            self.fail_over_nodes.append(self.cluster.cbas_nodes[1])
            self.neglect_failures = False
        else:
            self.log.info("This is NC+CC fail over")
            self.fail_over_nodes.append(self.cluster.cbas_nodes[0])
            self.fail_over_nodes.append(self.cbas_node)
            self.cbas_util.closeConn()
            self.cbas_util = CbasUtil(self.cluster.master, self.cluster.cbas_nodes[1], self.task)

            if self.meta_data_node_failure:
                self.fail_over_nodes.append(self.cluster.cbas_nodes[1])
                self.cbas_util = CbasUtil(self.cluster.master, self.cluster.cbas_nodes[2], self.task)

            self.cbas_util.createConn(self.cb_bucket_name)
            self.neglect_failures = True

    def test_cbas_multi_node_fail_over(self):

        self.log.info("fail-over the node")
        fail_over_task = self._cb_cluster.async_failover(self.input.servers, self.fail_over_nodes)
        self.assertTrue(self.task_manager.get_task_result(fail_over_task), msg="Fail over of nodes failed")

        self.log.info("Rebalance remaining nodes")
        result = self.cluster_util.rebalance()
        self.assertTrue(result, "Rebalance operation failed")

        self.log.info("Validate dataset count")
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items), msg="Document count mismatch")

    def test_cbas_multi_node_fail_over_busy_system(self):

        self.log.info("Perform doc operation async")
        tasks = self.perform_doc_ops_in_all_cb_buckets(
            "create",
            start_key=self.num_items,
            end_key=self.num_items+(self.num_items/4),
            _async=True)

        self.log.info("Run concurrent queries to simulate busy system")
        statement = "select sleep(count(*),50000) from {0} where mutated=0;".format(self.cbas_dataset_name)
        try:
            self.cbas_util._run_concurrent_queries(statement, "async", 10, batch_size=10)
        except Exception as e:
            if self.neglect_failures:
                self.log.info("Neglecting failed queries, to handle node fail over CC")
            else:
                raise e

        self.log.info("fail-over the node")
        fail_over_task = self._cb_cluster.async_failover(self.input.servers, self.fail_over_nodes)
        self.assertTrue(self.task_manager.get_task_result(fail_over_task), msg="Fail over of nodes failed")

        self.log.info("Rebalance remaining nodes")
        result = self.cluster_util.rebalance()
        self.assertTrue(result, "Rebalance operation failed")

        for task in tasks:
            self.log.info(self.task_manager.get_task_result(task))

        self.log.info("Validate dataset count")
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.num_items + self.num_items/4), msg="Document count mismatch")      

    def tearDown(self):
        super(MultiNodeFailOver, self).tearDown()
