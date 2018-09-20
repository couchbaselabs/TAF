'''
Created on Jan 31, 2018

@author: riteshagarwal
'''

import time

from cbas.cbas_utils import cbas_utils
from cbas_base import CBASBaseTest, TestInputSingleton
from lib.memcached.helper.data_helper import MemcachedClientHelper
from membase.api.rest_client import RestConnection
from node_utils.node_ready_functions import NodeHelper
from remote.remote_util import RemoteMachineShellConnection


class MetadataReplication(CBASBaseTest):
    def tearDown(self):
        CBASBaseTest.tearDown(self)
        
    def setUp(self):
        self.input = TestInputSingleton.input
        self.input.test_params.update({"default_bucket":False})
        super(MetadataReplication, self).setUp()
        self.nc_otpNodes = []
        if "add_all_cbas_nodes" in self.input.test_params and self.input.test_params["add_all_cbas_nodes"] and len(self.cbas_servers) > 0:
            self.nc_otpNodes = self.add_all_nodes_then_rebalance(self.cbas_servers)
        elif self.input.param("nc_nodes_to_add",0):
            self.nc_otpNodes = self.add_all_nodes_then_rebalance(self.cbas_servers[:self.input.param("nc_nodes_to_add")])
        self.otpNodes += self.nc_otpNodes
            
        self.create_default_bucket()
        self.cbas_util.createConn("default")
        self.shell = RemoteMachineShellConnection(self.master)
        
        #test for number of partitions:
        self.partitions_dict = self.cbas_util.get_num_partitions(self.shell)
        
#         if self.master.cbas_path:
#             for key in self.partitions_dict.keys():
#                 self.assertTrue(self.partitions_dict[key] == len(ast.literal_eval(self.master.cbas_path)), "Number of partitions created are incorrect on cbas nodes.")
        
    def setup_for_test(self, skip_data_loading=False):
        
        if not skip_data_loading:
            # Load Couchbase bucket first.
            self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0,
                                                   self.num_items, batch_size=1000)

        # Create bucket on CBAS
        self.cbas_util.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                                   cb_bucket_name=self.cb_bucket_name,
                                   cb_server_ip=self.cb_server_ip)

        # Create dataset on the CBAS bucket
        self.cbas_util.create_dataset_on_bucket(cbas_bucket_name=self.cb_bucket_name,
                                                cbas_dataset_name=self.cbas_dataset_name)
        
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
                
    def ingestion_in_progress(self):
        self.cbas_util.disconnect_from_bucket(self.cbas_bucket_name)
        
        self.perform_doc_ops_in_all_cb_buckets(self.num_items*2, "create", 0,
                                                   self.num_items*2, batch_size=1000)
        
        
        self.cbas_util.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                               cb_bucket_password=self.cb_bucket_password)
    
    def ingest_more_data(self):
        self.cbas_util.disconnect_from_bucket(self.cbas_bucket_name)
        
        self.perform_doc_ops_in_all_cb_buckets(self.num_items*2, "create", self.num_items*2,
                                                   self.num_items*4, batch_size=1000)
        
        self.cbas_util.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                               cb_bucket_password=self.cb_bucket_password)
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,self.num_items*4):
                self.fail("No. of items in CBAS dataset do not match that in the CB bucket")
                
    def test_rebalance(self):
        self.setup_for_test(skip_data_loading=True)
        self.rebalance_type = self.input.param('rebalance_type','out')
        self.rebalance_node = self.input.param('rebalance_node','CC')
        self.how_many = self.input.param('how_many',1)
        self.restart_rebalance = self.input.param('restart_rebalance',False)
        self.replica_change = self.input.param('replica_change',0)
        
        query = "select sleep(count(*),50000) from {0};".format(self.cbas_dataset_name)
        handles = self.cbas_util._run_concurrent_queries(query,"async",10)
        self.ingestion_in_progress()
        otpNodes = []
        if self.rebalance_node == "CC":
            node_in_test = [self.cbas_node]
            otpNodes = [self.otpNodes[0]]
            
            self.cbas_util.closeConn()
            self.cbas_util = cbas_utils(self.master, self.cbas_servers[0])
            self.cbas_util.createConn("default")
            
            self.cbas_node = self.cbas_servers[0]
        elif self.rebalance_node == "NC":
            node_in_test = self.cbas_servers[:self.how_many]
            otpNodes = self.nc_otpNodes[:self.how_many]
        else:
            node_in_test = [self.cbas_node] + self.cbas_servers[:self.how_many]
            otpNodes = self.otpNodes[:self.how_many+1]
            self.cbas_util.closeConn()
            self.cbas_util = cbas_utils(self.master, self.cbas_servers[self.how_many])
            self.cbas_util.createConn("default")
        replicas_before_rebalance=len(self.cbas_util.get_replicas_info(self.shell))
        
        if self.rebalance_type == 'in':
            if self.restart_rebalance:
                self.cluster_util.add_all_nodes_then_rebalance(self.cbas_servers[self.input.param("nc_nodes_to_add"):self.how_many+self.input.param("nc_nodes_to_add")],wait_for_completion=False)
                self.sleep(2)
                if self.rest._rebalance_progress_status() == "running":
                    self.assertTrue(self.rest.stop_rebalance(wait_timeout=120), "Failed while stopping rebalance.")
                    self.sleep(30,"Wait for some tine after rebalance is stopped.")
                else:
                    self.fail("Rebalance completed before the test could have stopped rebalance.")
                
                self.rebalance(wait_for_completion=False)
            else:
                self.cluster_util.add_all_nodes_then_rebalance(self.cbas_servers[self.input.param("nc_nodes_to_add"):self.how_many+self.input.param("nc_nodes_to_add")],wait_for_completion=False)
            replicas_before_rebalance += self.replica_change
        else:
            if self.restart_rebalance:
                self.cluster_util.remove_node(otpNodes,wait_for_rebalance=False)
                self.sleep(2)
                if self.rest._rebalance_progress_status() == "running":
                    self.assertTrue(self.rest.stop_rebalance(wait_timeout=120), "Failed while stopping rebalance.")
                    self.sleep(30,"Wait for some tine after rebalance is stopped.")
                else:
                    self.fail("Rebalance completed before the test could have stopped rebalance.")
                
                self.rebalance(wait_for_completion=False,ejected_nodes=[node.id for node in otpNodes])
            else:
                self.cluster_util.remove_node(otpNodes,wait_for_rebalance=False)
            replicas_before_rebalance -= self.replica_change
        self.sleep(30)
        str_time = time.time()
        while self.rest._rebalance_progress_status() == "running" and time.time()<str_time+300:
            replicas = self.cbas_util.get_replicas_info(self.shell)
            if replicas:
                for replica in replicas:
                    self.log.info("replica state during rebalance: %s"%replica['status'])
        self.sleep(2)
        replicas = self.cbas_util.get_replicas_info(self.shell)
        replicas_after_rebalance=len(replicas)
        self.assertEqual(replicas_after_rebalance, replicas_before_rebalance, "%s,%s"%(replicas_after_rebalance,replicas_before_rebalance))
        
        for replica in replicas:
            self.log.info("replica state during rebalance: %s"%replica['status'])
            self.assertEqual(replica['status'], "IN_SYNC","Replica state is incorrect: %s"%replica['status'])
                                
#         items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
#         self.log.info("Items before service restart: %s"%items_in_cbas_bucket)
        
        count = 0
        while self.cbas_util.fetch_analytics_cluster_response()['state'] != "ACTIVE" and count < 60:
            self.sleep(5)
            count+=1
            
        items_in_cbas_bucket = 0
        start_time=time.time()
        while (items_in_cbas_bucket == 0 or items_in_cbas_bucket == -1) and time.time()<start_time+60:
            try:
                items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
            except:
                pass
            self.sleep(1)
        self.log.info("After rebalance operation docs in CBAS bucket : %s"%items_in_cbas_bucket)
        if items_in_cbas_bucket < self.num_items*2 and items_in_cbas_bucket>self.num_items:
            self.log.info("Data Ingestion Interrupted successfully")
        elif items_in_cbas_bucket < self.num_items:
            self.log.info("Data Ingestion did interrupted and restarting from 0.")
        else:
            self.log.info("Data Ingestion did not interrupted but complete before rebalance operation.")
            
        run_count = 0
        fail_count = 0
        success_count = 0
        aborted_count = 0
        shell=RemoteMachineShellConnection(node_in_test[0])
        for handle in handles:
            status, hand = self.cbas_util.retrieve_request_status_using_handle(node_in_test, handle, shell)
            if status == "running":
                run_count += 1
                self.log.info("query with handle %s is running."%handle)
            elif status == "failed":
                fail_count += 1
                self.log.info("query with handle %s is failed."%handle)
            elif status == "success":
                success_count += 1
                self.log.info("query with handle %s is successful."%handle)
            else:
                aborted_count +=1
                self.log.info("Queued job is deleted: %s"%status)
                
        self.log.info("After service restart %s queued jobs are Running."%run_count)
        self.log.info("After service restart %s queued jobs are Failed."%fail_count)
        self.log.info("After service restart %s queued jobs are Successful."%success_count)
        self.log.info("After service restart %s queued jobs are Aborted."%aborted_count)
        
        if self.rebalance_node == "NC":
            self.assertTrue(aborted_count==0, "Some queries aborted")
        
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.cbas_util._run_concurrent_queries(query,"immediate",100)
        
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,self.num_items*2):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")
        self.ingest_more_data()
        
    def test_cancel_CC_rebalance(self):
        pass
    
    def test_chain_rebalance_out_cc(self):
        self.setup_for_test(skip_data_loading=True)
        self.ingestion_in_progress()
        
        total_cbas_nodes = len(self.otpNodes)
        while total_cbas_nodes > 1:
            cc_ip = self.cbas_util.retrieve_cc_ip(shell=self.shell)
            for otpnode in self.otpNodes:
                if otpnode.ip == cc_ip:
                    self.cluster_util.remove_node([otpnode],wait_for_rebalance=True)
                    for server in self.cbas_servers:
                        if cc_ip != server.ip:
                            self.cbas_util.closeConn()
                            self.cbas_util = cbas_utils(self.master, server)
                            self.cbas_util.createConn("default")
                            self.cbas_node = server
                            break
#                     items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
#                     self.log.info("Items before service restart: %s"%items_in_cbas_bucket)
                            
                    items_in_cbas_bucket = 0
                    start_time=time.time()
                    while (items_in_cbas_bucket == 0 or items_in_cbas_bucket == -1) and time.time()<start_time+60:
                        try:
                            items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
                        except:
                            pass
                        self.sleep(1)
                    self.log.info("After rebalance operation docs in CBAS bucket : %s"%items_in_cbas_bucket)
                    if items_in_cbas_bucket < self.num_items*2 and items_in_cbas_bucket>self.num_items:
                        self.log.info("Data Ingestion Interrupted successfully")
                    elif items_in_cbas_bucket < self.num_items:
                        self.log.info("Data Ingestion did interrupted and restarting from 0.")
                    else:
                        self.log.info("Data Ingestion did not interrupted but complete before rebalance operation.")
                        
                    query = "select count(*) from {0};".format(self.cbas_dataset_name)
                    self.cbas_util._run_concurrent_queries(query,"immediate",10)
                    break
            total_cbas_nodes -= 1
            
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,self.num_items*2):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")
        self.ingest_more_data()
        
    def test_cc_swap_rebalance(self):
        self.restart_rebalance = self.input.param('restart_rebalance',False)
        
        self.setup_for_test(skip_data_loading=True)
        query = "select sleep(count(*),50000) from {0};".format(self.cbas_dataset_name)
        handles = self.cbas_util._run_concurrent_queries(query,"async",10)
        self.ingestion_in_progress()
        
        replicas_before_rebalance=len(self.cbas_util.get_replicas_info(self.shell))
        
        self.cluster_util.add_node(node=self.cbas_servers[-1],rebalance=False)
        swap_nc = self.input.param('swap_nc', False)
        if not swap_nc:
            out_nodes = [self.otpNodes[0]]
            self.cbas_util.closeConn()
            self.cbas_util = cbas_utils(self.master, self.cbas_servers[0])
            self.cbas_util.createConn("default")
            self.cbas_node = self.cbas_servers[0]
        else:
            out_nodes = [self.otpNodes[1]]    
        
        self.cluster_util.remove_node(out_nodes, wait_for_rebalance=False)
        self.sleep(5, "Wait for sometime after rebalance started.")
        if self.restart_rebalance:
            if self.rest._rebalance_progress_status() == "running":
                self.assertTrue(self.rest.stop_rebalance(wait_timeout=120), "Failed while stopping rebalance.")
                self.sleep(10)
            else:
                self.fail("Rebalance completed before the test could have stopped rebalance.")
            self.rebalance(ejected_nodes=[node.id for node in out_nodes], wait_for_completion=False)
        self.sleep(5)
        str_time = time.time()
        while self.rest._rebalance_progress_status() == "running" and time.time()<str_time+300:
            replicas = self.cbas_util.get_replicas_info(self.shell)
            if replicas:
                for replica in replicas:
                    self.log.info("replica state during rebalance: %s"%replica['status'])
        self.sleep(20)
        
        replicas = self.cbas_util.get_replicas_info(self.shell)
        replicas_after_rebalance=len(replicas)
        self.assertEqual(replicas_after_rebalance, replicas_before_rebalance, "%s,%s"%(replicas_after_rebalance,replicas_before_rebalance))
        
        for replica in replicas:
            self.log.info("replica state during rebalance: %s"%replica['status'])
            self.assertEqual(replica['status'], "IN_SYNC","Replica state is incorrect: %s"%replica['status'])
                                
#         items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
#         self.log.info("Items before service restart: %s"%items_in_cbas_bucket)
                
        items_in_cbas_bucket = 0
        start_time=time.time()
        while (items_in_cbas_bucket == 0 or items_in_cbas_bucket == -1) and time.time()<start_time+60:
            try:
                items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
            except:
                pass
            self.sleep(1)
        self.log.info("After rebalance operation docs in CBAS bucket : %s"%items_in_cbas_bucket)
        if items_in_cbas_bucket < self.num_items*2 and items_in_cbas_bucket>self.num_items:
            self.log.info("Data Ingestion Interrupted successfully")
        elif items_in_cbas_bucket < self.num_items:
            self.log.info("Data Ingestion did interrupted and restarting from 0.")
        else:
            self.log.info("Data Ingestion did not interrupted but complete before rebalance operation.")
            
        run_count = 0
        fail_count = 0
        success_count = 0
        aborted_count = 0
        shell=RemoteMachineShellConnection(self.master)
        for handle in handles:
            status, hand = self.cbas_util.retrieve_request_status_using_handle(self.master, handle, shell)
            if status == "running":
                run_count += 1
                self.log.info("query with handle %s is running."%handle)
            elif status == "failed":
                fail_count += 1
                self.log.info("query with handle %s is failed."%handle)
            elif status == "success":
                success_count += 1
                self.log.info("query with handle %s is successful."%handle)
            else:
                aborted_count +=1
                self.log.info("Queued job is deleted: %s"%status)
                
        self.log.info("After service restart %s queued jobs are Running."%run_count)
        self.log.info("After service restart %s queued jobs are Failed."%fail_count)
        self.log.info("After service restart %s queued jobs are Successful."%success_count)
        self.log.info("After service restart %s queued jobs are Aborted."%aborted_count)
        
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.cbas_util._run_concurrent_queries(query,"immediate",100)
        
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,self.num_items*2):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")
        self.ingest_more_data()
           
    def test_reboot_nodes(self):
        #Test for reboot CC and reboot all nodes.
        self.setup_for_test(skip_data_loading=True)
        self.ingestion_in_progress()
        self.node_type = self.input.param('node_type','CC')
        
        replica_nodes_before_reboot = self.cbas_util.get_replicas_info(self.shell)
        replicas_before_reboot=len(self.cbas_util.get_replicas_info(self.shell))
        if self.node_type == "CC":
            NodeHelper.reboot_server(self.cbas_node, self)
        elif self.node_type == "NC":
            for server in self.cbas_servers:
                NodeHelper.reboot_server(server, self)
        else:
            NodeHelper.reboot_server(self.cbas_node, self)
            for server in self.cbas_servers:
                NodeHelper.reboot_server(server, self)
        
        self.sleep(60)
        replica_nodes_after_reboot = self.cbas_util.get_replicas_info(self.shell)
        replicas_after_reboot=len(replica_nodes_after_reboot)
        
        self.assertTrue(replica_nodes_after_reboot == replica_nodes_before_reboot,
                        "Replica nodes changed after reboot. Before: %s , After : %s"
                        %(replica_nodes_before_reboot,replica_nodes_after_reboot))
        self.assertTrue(replicas_after_reboot == replicas_before_reboot,
                        "Number of Replica nodes changed after reboot. Before: %s , After : %s"
                        %(replicas_before_reboot,replicas_after_reboot))
        
        items_in_cbas_bucket = 0
        start_time=time.time()
        while (items_in_cbas_bucket == 0 or items_in_cbas_bucket == -1) and time.time()<start_time+60:
            try:
                items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
            except:
                pass
            self.sleep(1)
               
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.cbas_util._run_concurrent_queries(query,"immediate",100)
        
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,self.num_items*2):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")
        
        for replica in replica_nodes_after_reboot:
            self.log.info("replica state during rebalance: %s"%replica['status'])
            self.assertEqual(replica['status'], "IN_SYNC","Replica state is incorrect: %s"%replica['status'])
        self.ingest_more_data()
        
    def test_failover(self):
        self.setup_for_test(skip_data_loading=True)
        self.rebalance_node = self.input.param('rebalance_node','CC')
        self.how_many = self.input.param('how_many',1)
        self.restart_rebalance = self.input.param('restart_rebalance',False)
        self.replica_change = self.input.param('replica_change',0)
        self.add_back = self.input.param('add_back',False)
        
        query = "select sleep(count(*),50000) from {0};".format(self.cbas_dataset_name)
        handles = self.cbas_util._run_concurrent_queries(query,"async",10)
        self.ingestion_in_progress()
        
        if self.rebalance_node == "CC":
            node_in_test = [self.cbas_node]
            otpNodes = [self.otpNodes[0]]
            self.cbas_util.closeConn()
            self.cbas_util = cbas_utils(self.master, self.cbas_servers[0])
            self.cbas_util.createConn("default")
            
            self.cbas_node = self.cbas_servers[0]
        elif self.rebalance_node == "NC":
            node_in_test = self.cbas_servers[:self.how_many]
            otpNodes = self.nc_otpNodes[:self.how_many]
        else:
            node_in_test = [self.cbas_node] + self.cbas_servers[:self.how_many]
            otpNodes = self.otpNodes[:self.how_many+1]
            self.cbas_util.closeConn()
            self.cbas_util = cbas_utils(self.master, self.cbas_servers[self.how_many])
            self.cbas_util.createConn("default")
            
        replicas_before_rebalance=len(self.cbas_util.get_replicas_info(self.shell))
        items_in_cbas_bucket = 0
        start_time=time.time()
        while (items_in_cbas_bucket == 0 or items_in_cbas_bucket == -1) and time.time()<start_time+60:
            try:
                items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
            except:
                pass
            self.sleep(1)
        self.log.info("Items before failover node: %s"%items_in_cbas_bucket)
        
        if self.restart_rebalance:
            graceful_failover = self.input.param("graceful_failover", False)
            failover_task = self._cb_cluster.async_failover(self.input.servers,
                                                            node_in_test,
                                                            graceful_failover)
            failover_task.get_result()
            if self.add_back:
                for otpnode in otpNodes:
                    self.rest.set_recovery_type('ns_1@' + otpnode.ip, "full")
                    self.rest.add_back_node('ns_1@' + otpnode.ip)
                self.rebalance(wait_for_completion=False)
            else:
                self.rebalance(ejected_nodes=[node.id for node in otpNodes], wait_for_completion=False)
            self.sleep(2)
            if self.rest._rebalance_progress_status() == "running":
                self.assertTrue(self.rest.stop_rebalance(wait_timeout=120), "Failed while stopping rebalance.")
                if self.add_back:
                    self.rebalance(wait_for_completion=False)
                else:
                    self.rebalance(ejected_nodes=[node.id for node in otpNodes], wait_for_completion=False)
            else:
                self.fail("Rebalance completed before the test could have stopped rebalance.")
        else:
            graceful_failover = self.input.param("graceful_failover", False)
            failover_task = self._cb_cluster.async_failover(self.input.servers,
                                                            node_in_test,
                                                            graceful_failover)
            failover_task.get_result()
            if self.add_back:
                for otpnode in otpNodes:
                    self.rest.set_recovery_type('ns_1@' + otpnode.ip, "full")
                    self.rest.add_back_node('ns_1@' + otpnode.ip)
            self.rebalance(wait_for_completion=False)

        replicas_before_rebalance -= self.replica_change
        self.sleep(5)
        str_time = time.time()
        while self.rest._rebalance_progress_status() == "running" and time.time()<str_time+300:
            replicas = self.cbas_util.get_replicas_info(self.shell)
            if replicas:
                for replica in replicas:
                    self.log.info("replica state during rebalance: %s"%replica['status'])
        self.sleep(15)
        replicas = self.cbas_util.get_replicas_info(self.shell)
        replicas_after_rebalance=len(replicas)
        self.assertEqual(replicas_after_rebalance, replicas_before_rebalance, "%s,%s"%(replicas_after_rebalance,replicas_before_rebalance))
        
        for replica in replicas:
            self.log.info("replica state during rebalance: %s"%replica['status'])
            self.assertEqual(replica['status'], "IN_SYNC","Replica state is incorrect: %s"%replica['status'])
                                
        items_in_cbas_bucket = 0
        start_time=time.time()
        while (items_in_cbas_bucket == 0 or items_in_cbas_bucket == -1) and time.time()<start_time+60:
            try:
                items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
            except:
                pass
            self.sleep(1)
        self.log.info("After rebalance operation docs in CBAS bucket : %s"%items_in_cbas_bucket)
        if items_in_cbas_bucket < self.num_items*2 and items_in_cbas_bucket>self.num_items:
            self.log.info("Data Ingestion Interrupted successfully")
        elif items_in_cbas_bucket < self.num_items:
            self.log.info("Data Ingestion did interrupted and restarting from 0.")
        else:
            self.log.info("Data Ingestion did not interrupted but complete before rebalance operation.")
            
        run_count = 0
        fail_count = 0
        success_count = 0
        aborted_count = 0
        shell=RemoteMachineShellConnection(node_in_test[0])
        for handle in handles:
            status, hand = self.cbas_util.retrieve_request_status_using_handle(node_in_test, handle, shell)
            if status == "running":
                run_count += 1
                self.log.info("query with handle %s is running."%handle)
            elif status == "failed":
                fail_count += 1
                self.log.info("query with handle %s is failed."%handle)
            elif status == "success":
                success_count += 1
                self.log.info("query with handle %s is successful."%handle)
            else:
                aborted_count +=1
                self.log.info("Queued job is deleted: %s"%status)
                
        self.log.info("After service restart %s queued jobs are Running."%run_count)
        self.log.info("After service restart %s queued jobs are Failed."%fail_count)
        self.log.info("After service restart %s queued jobs are Successful."%success_count)
        self.log.info("After service restart %s queued jobs are Aborted."%aborted_count)
        
        if self.rebalance_node == "NC":
            self.assertTrue(aborted_count==0, "Some queries aborted")
        
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.cbas_util._run_concurrent_queries(query,"immediate",100)
        
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,self.num_items*2):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")
            
        self.ingest_more_data()