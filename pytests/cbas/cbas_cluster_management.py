import time

from cbas_base import *
from membase.api.rest_client import RestHelper
from couchbase_cli import CouchbaseCLI
from node_utils.node_ready_functions import NodeHelper
from basetestcase import RemoteMachineShellConnection

class CBASClusterManagement(CBASBaseTest):
    def setUp(self):
        self.input = TestInputSingleton.input
        if "default_bucket" not in self.input.test_params:
            self.input.test_params.update({"default_bucket":False})
        super(CBASClusterManagement, self).setUp(add_defualt_cbas_node = False)
        self.assertTrue(len(self.cbas_servers)>=1, "There is no cbas server running. Please provide 1 cbas server atleast.")

    def setup_cbas_bucket_dataset_connect(self, cb_bucket, num_docs):
        # Create bucket on CBAS
        self.cbas_util.createConn(cb_bucket)
        self.assertTrue(self.cbas_util.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                       cb_bucket_name=cb_bucket),"bucket creation failed on cbas")
        
        self.assertTrue(self.cbas_util.create_dataset_on_bucket(cbas_bucket_name=cb_bucket,
                          cbas_dataset_name=self.cbas_dataset_name), "dataset creation failed on cbas")
        
        self.assertTrue(self.cbas_util.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name),"Connecting cbas bucket to cb bucket failed")
        
        self.assertTrue(self.cbas_util.wait_for_ingestion_complete([self.cbas_dataset_name], num_docs),"Data ingestion to cbas couldn't complete in 300 seconds.")
        return True
    
    def test_add_cbas_node_one_by_one(self):
        '''
        Description: Add cbas nodes 1 by 1 and rebalance on every add.
        
        Steps:
        1. For all the cbas nodes provided in ini file, Add all of them 1by1 and Rebalance.
        
        Author: Ritesh Agarwal
        '''
        nodes_before = len(self.rest.get_nodes_data_from_cluster())
        added = 0
        for node in self.cbas_servers:
            if node.ip != self.master.ip:
                self.cluster_util.add_node(node=node,rebalance=True)
                added += 1
        nodes_after = len(self.rest.get_nodes_data_from_cluster())
        self.assertTrue(nodes_before+added == nodes_after, "While adding cbas nodes seems like some nodes were removed during rebalance.")
        
    def test_add_all_cbas_nodes_in_cluster(self):
        '''
        Description: Add all cbas nodes and then rebalance.

        Steps:
        1. For all the cbas nodes provided in ini file, Add all of them in one go and Rebalance.
        
        Author: Ritesh Agarwal
        '''
        self.cluster_util.add_all_nodes_then_rebalance(self.cbas_servers)
#         
    def test_add_remove_all_cbas_nodes_in_cluster(self):
        '''
        Description: First add all cbas nodes and then rebalance. Remove all added cbas node, rebalance.
        
        Steps:
        1. For all the cbas nodes provided in ini file, Add all of them in one go and Rebalance.
        2. Remove all nodes together and then rebalance.
        
        Author: Ritesh Agarwal
        '''
        cbas_otpnodes = self.cluster_util.add_all_nodes_then_rebalance(self.cbas_servers)
        self.cluster_util.remove_all_nodes_then_rebalance(cbas_otpnodes)

    def test_concurrent_sevice_existence_with_cbas(self):
        '''
        Description: Test add/remove nodes via REST APIs.
        
        Steps:
        1. Add nodes by randomly picking up the services from the service_list.
        2. Check that correct services are running after the node is added.
        
        Author: Ritesh Agarwal
        '''
        service_list = [["kv","cbas","index","n1ql"],
                        ["cbas","n1ql","index"],
                        ["kv","cbas","n1ql"],
                        ["n1ql","cbas","fts"]
                        ]
        for cbas_server in self.servers:
            if cbas_server.ip == self.master.ip:
                continue
            from random import randint
            service = service_list[randint(0, len(service_list)-1)]
            self.log.info("Adding %s to the cluster with services %s"%(cbas_server,service))
            otpNode = self.cluster_util.add_node(node=cbas_server,services=service)
            
            '''Check for the correct services alloted to the nodes.'''
            nodes = self.rest.get_nodes_data_from_cluster()
            for node in nodes:
                if node["otpNode"] == otpNode.id:
                    self.assertTrue(set(node["services"]) == set(service), "Service setting failed") 
                    self.log.info("Successfully added %s to the cluster with services %s"%(otpNode.id,service))
                    
    def test_add_delete_cbas_nodes_CLI(self):
        '''
        Description: Test add/remove nodes via CLI.
        
        Steps:
        1. Add nodes by randomly picking up the services from the service_list.
        2. Check that correct services are running after the node is added.
        
        Author: Ritesh Agarwal
        '''
        service_list = {"data,analytics,index":["kv","cbas","index"],
                        "analytics,query,index":["cbas","n1ql","index"],
                        "data,analytics,query":["kv","cbas","n1ql"],
                        "analytics,query,fts":["cbas","n1ql","fts"],
                        }
        for cbas_server in self.cbas_servers:
            if cbas_server.ip == self.master.ip:
                continue
            import random
            service = random.choice(service_list.keys())
            self.log.info("Adding %s to the cluster with services %s to cluster %s"%(cbas_server,service,self.master))
            
            stdout, stderr, result = CouchbaseCLI(self.master, self.master.rest_username, self.master.rest_password).server_add(cbas_server.ip+":"+cbas_server.port, cbas_server.rest_username, cbas_server.rest_password, None, service, None)
            self.assertTrue(result, "Server %s is not added to the cluster %s . Error: %s"%(cbas_server,self.master,stdout+stderr))
            self.rebalance()
            
            '''Check for the correct services alloted to the nodes.'''
            nodes = self.rest.get_nodes_data_from_cluster()
            for node in nodes:
                if node["otpNode"].find(cbas_server.ip) != -1:
                    actual_services = set(node["services"])
                    expected_servcies = set(service_list[service])
                    self.log.info("Expected:%s Actual:%s"%(expected_servcies,actual_services))
                    self.assertTrue(actual_services == expected_servcies, "Service setting failed") 
                    self.log.info("Successfully added %s to the cluster with services %s"%(node["otpNode"],service))
        
        to_remove = []
        for cbas_server in self.cbas_servers:
            if cbas_server.ip == self.master.ip:
                continue
            else:
                to_remove.append(cbas_server.ip)
        self.log.info("Removing: %s from the cluster: %s"%(to_remove,self.master))
        stdout, stderr, result = CouchbaseCLI(self.master, self.master.rest_username, self.master.rest_password).rebalance(",".join(to_remove))
        if not result:
            self.log.info(15*"#"+"THIS IS A BUG: MB-24968. REMOVE THIS TRY-CATCH ONCE BUG IS FIXED."+15*"#")
            stdout, stderr, result = CouchbaseCLI(self.master, self.master.rest_username, self.master.rest_password).rebalance(",".join(to_remove))
        self.assertTrue(result, "Server %s are not removed from the cluster %s . Console Output: %s , Error: %s"%(to_remove,self.master,stdout,stderr))

    def test_add_another_cbas_node_rebalance(self):
        set_up_cbas = False
        wait_for_rebalance = True
        test_docs = self.num_items
        docs_to_verify = test_docs
        self.create_default_bucket()
        self.perform_doc_ops_in_all_cb_buckets(test_docs, "create", 0, test_docs)
        
        if self.cbas_node.ip == self.master.ip:
            set_up_cbas = self.setup_cbas_bucket_dataset_connect("default", docs_to_verify)
            wait_for_rebalance = False
        i = 1
        for cbas_server in self.cbas_servers:
            if cbas_server.ip == self.master.ip:
                continue
            from random import randint
            service = ["kv","cbas"]
            self.log.info("Adding %s to the cluster with services %s"%(cbas_server,service))
            self.cluster_util.add_node(node=cbas_server,services=service,wait_for_rebalance_completion=wait_for_rebalance)
            
            if not set_up_cbas:
                set_up_cbas = self.setup_cbas_bucket_dataset_connect("default", docs_to_verify)
                wait_for_rebalance = False
            
            # Run some queries while rebalance is in progress after adding further cbas nodes    
            self.assertTrue((self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name))[0] == docs_to_verify,
                            "Number of items in CBAS is different from CB after adding further cbas node.")
            
#             self.disconnect_from_bucket(self.cbas_bucket_name)
            self.perform_doc_ops_in_all_cb_buckets(test_docs, "create", test_docs*i, test_docs*(i+1))
#             self.connect_to_bucket(self.cbas_bucket_name, self.cb_bucket_name)
            
#             if self.rest._rebalance_progress_status() == 'running':
#                 self.assertTrue((self.get_num_items_in_cbas_dataset(self.cbas_dataset_name))[0] == docs_to_verify,
#                             "Number of items in CBAS is different from CB after adding further cbas node.")
            
            docs_to_verify = docs_to_verify + test_docs
            # Wait for the rebalance to be completed.
            result = self.rest.monitorRebalance()
            self.assertTrue(result, "Rebalance operation failed after adding %s cbas nodes,"%self.cbas_servers)
            self.log.info("successfully rebalanced cluster {0}".format(result))
            
            self.assertTrue(self.cbas_util.wait_for_ingestion_complete([self.cbas_dataset_name], docs_to_verify, 300),
                            "Data ingestion could'nt complete after rebalance completion.")
            i+=1
            
    def test_add_cbas_rebalance_runqueries(self):
        '''
        Description: Add CBAS node, rebalance. Run concurrent queries.
        
        Steps:
        1. Add cbas node then do rebalance.
        2. Once rebalance is completed, on cbas node connect to bucket, create shadows.
        3. Data ingestion should start. Run queries.
        
        Author: Ritesh Agarwal
        '''
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.create_default_bucket()
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items)
        self.add_node(node=self.cbas_node)
        self.setup_cbas_bucket_dataset_connect("default", self.num_items)
        self.cbas_util._run_concurrent_queries(query,None,500,batch_size=self.concurrent_batch_size)
        
    def test_add_data_rebalance_runqueries(self):
        '''
        Description: Add data node rebalance. During rebalance setup cbas. Run concurrent queries.
        
        Steps:
        1. Add data node then do rebalance.
        2. While rebalance is happening, on cbas node connect to bucket, create shadows and Run queries.
        
        Author: Ritesh Agarwal
        '''
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.create_default_bucket()
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items)
        self.add_node(node=self.cbas_node)
        self.add_node(node=self.kv_servers[1],wait_for_rebalance_completion=False)
        self.setup_cbas_bucket_dataset_connect("default", self.num_items)
        self.cbas_util._run_concurrent_queries(query,"immediate",500,batch_size=self.concurrent_batch_size)
        
    def test_all_cbas_node_running_queries(self):
        '''
        Description: Test that all the cbas nodes are capable to serve queries.
        
        Steps:
        1. Perform doc operation on the KV node.
        2. Add 1 cbas node and setup cbas.
        3. Add all other cbas nodes.
        4. Verify all cbas nodes should be able to serve queries.
        
        Author: Ritesh Agarwal
        '''
        set_up_cbas = False
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.create_default_bucket()
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items)
        
        if self.cbas_node.ip == self.master.ip:
            set_up_cbas = self.setup_cbas_bucket_dataset_connect("default", self.num_items)
            temp_cbas_util = cbas_utils(self.master, self.cbas_node)
            temp_cbas_util.createConn("default")
            self.cbas_util._run_concurrent_queries(query,None,1000,self.cbas_util)
            temp_cbas_util.closeConn()
        for node in self.cbas_servers:
            if node.ip != self.master.ip:
                self.add_node(node=node)
                if not set_up_cbas:
                    set_up_cbas = self.setup_cbas_bucket_dataset_connect("default", self.num_items)
                temp_cbas_util = cbas_utils(self.master, node)
                temp_cbas_util.createConn("default")    
                self.cbas_util._run_concurrent_queries(query,None,1000,self.cbas_util,batch_size=self.concurrent_batch_size)
                temp_cbas_util.closeConn()

    def test_add_first_cbas_restart_rebalance(self):
        '''
        Description: This test will add the first cbas node then start rebalance and cancel rebalance
        before rebalance completes.
        
        Steps:
        1. Add first cbas node.
        2. Start rebalance.
        3. While rebalance is in progress, stop rebalancing. Again start rebalance
        4. Create bucket, datasets, connect bucket. Data ingestion should start.
        
        Author: Ritesh Agarwal
        '''
        self.load_sample_buckets(bucketName=self.cb_bucket_name, total_items=self.travel_sample_docs_count)
        self.add_node(self.cbas_node, services=["kv","cbas"],wait_for_rebalance_completion=False)
        
        if self.rest._rebalance_progress_status() == "running":
            self.assertTrue(self.rest.stop_rebalance(), "Failed while stopping rebalance.")
        else:
            self.fail("Rebalance completed before the test could have stopped rebalance.")
        
        self.rebalance()
        self.setup_cbas_bucket_dataset_connect(self.cb_bucket_name, self.travel_sample_docs_count)

    def test_add_data_node_cancel_rebalance(self):
        '''
        Description: This test will add the first cbas node then start rebalance and cancel rebalance
        before rebalance completes.
        
        Steps:
        1. Add first cbas node. Start rebalance.
        2. Create bucket, datasets, connect bucket. Data ingestion should start.
        3. Add another data node. Rebalance, while rebalance is in progress, stop rebalancing.
        4. Create bucket, datasets, connect bucket. Data ingestion should start.
        
        Author: Ritesh Agarwal
        '''
        self.load_sample_buckets(bucketName=self.cb_bucket_name, total_items=self.travel_sample_docs_count)
        self.add_node(self.cbas_node)
        self.setup_cbas_bucket_dataset_connect(self.cb_bucket_name, self.travel_sample_docs_count)
        
        self.add_node(self.kv_servers[1],wait_for_rebalance_completion=False)
        if self.rest._rebalance_progress_status() == "running":
            self.assertTrue(self.rest.stop_rebalance(), "Failed while stopping rebalance.")
        else:
            self.fail("Rebalance completed before the test could have stopped rebalance.")
        
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.travel_sample_docs_count),"Data loss in CBAS.")
    
    
    def test_add_data_node_restart_rebalance(self):
        '''
        Description: This test will add the first cbas node then start rebalance and cancel rebalance
        before rebalance completes.
        
        Steps:
        1. Add first cbas node. Start rebalance.
        2. Create bucket, datasets, connect bucket. Data ingestion should start.
        3. Add another data node. Rebalance, while rebalance is in progress, stop rebalancing. Again start rebalance.
        4. Create bucket, datasets, connect bucket. Data ingestion should start.
        
        Author: Ritesh Agarwal
        '''
        self.load_sample_buckets(bucketName=self.cb_bucket_name, total_items=self.travel_sample_docs_count)
        self.add_node(self.cbas_node)
        self.setup_cbas_bucket_dataset_connect(self.cb_bucket_name, self.travel_sample_docs_count)
        
        self.add_node(self.kv_servers[1],wait_for_rebalance_completion=False)
        if self.rest._rebalance_progress_status() == "running":
            self.assertTrue(self.rest.stop_rebalance(), "Failed while stopping rebalance.")
        else:
            self.fail("Rebalance completed before the test could have stopped rebalance.")
        
        self.rebalance()
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.travel_sample_docs_count),"Data loss in CBAS.")
        
    def test_add_first_cbas_stop_rebalance(self):
        '''
        Description: This test will add the first cbas node then start rebalance and cancel rebalance
        before rebalance completes.
        
        Steps:
        1. Add first cbas node.
        2. Start rebalance.
        3. While rebalance is in progress, stop rebalancing.
        4. Verify that the cbas node is not added to the cluster and should not accept queries.
        
        Author: Ritesh Agarwal
        '''
        self.load_sample_buckets(bucketName=self.cb_bucket_name, total_items=self.travel_sample_docs_count)
        self.add_node(self.cbas_node, services=["kv","cbas"],wait_for_rebalance_completion=False)
        if self.rest._rebalance_progress_status() == "running":
            self.assertTrue(self.rest.stop_rebalance(), "Failed while stopping rebalance.")
        else:
            self.fail("Rebalance completed before the test could have stopped rebalance.")
        
        self.assertFalse(self.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                       cb_bucket_name="travel-sample"),"bucket creation failed on cbas")

    def test_add_second_cbas_stop_rebalance(self):
        '''
        Description: This test will add the second cbas node then start rebalance and cancel rebalance
        before rebalance completes.
        
        Steps:
        1. Add first cbas node.
        2. Start rebalance, wait for rebalance complete.
        3. Add another cbas node, rebalance and while rebalance is in progress, stop rebalancing.
        4. Verify that the second cbas node is not added to the cluster and should not accept queries.
        5. First cbas node should be able to serve queries.
        
        Author: Ritesh Agarwal
        '''
        self.load_sample_buckets(bucketName=self.cb_bucket_name, total_items=self.travel_sample_docs_count)
        self.add_node(self.cbas_servers[0], services=["kv","cbas"])
        self.setup_cbas_bucket_dataset_connect(self.cb_bucket_name, self.travel_sample_docs_count)
        
        self.add_node(self.cbas_servers[1], services=["kv","cbas"],wait_for_rebalance_completion=False)
        if self.rest._rebalance_progress_status() == "running":
            self.assertTrue(self.rest.stop_rebalance(), "Failed while stopping rebalance.")
        else:
            self.fail("Rebalance completed before the test could have stopped rebalance.")
        
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
#         self.assertFalse(self.execute_statement_on_cbas_via_rest(query, rest=RestConnection(self.cbas_servers[1])),
#                          "Successfully executed a cbas query from a node which is not part of cluster.")
        
        self.assertTrue(self.cbas_util.execute_statement_on_cbas_util(query, rest=RestConnection(self.cbas_servers[0])),
                         "Successfully executed a cbas query from a node which is not part of cluster.")
    
    def test_reboot_cbas(self):
        '''
        Description: This test will add the second cbas node then start rebalance and cancel rebalance
        before rebalance completes.
        
        Steps:
        1. Add first cbas node.
        2. Start rebalance, wait for rebalance complete.
        3. Create bucket, datasets, connect bucket. Data ingestion should start.
        4. Reboot CBAS node addd in Step 1.
        5. After reboot cbas node should be able to serve queries, validate items count.
        
        Author: Ritesh Agarwal
        '''
        self.load_sample_buckets(bucketName=self.cb_bucket_name, total_items=self.travel_sample_docs_count)
        self.add_node(self.cbas_node, services=["kv","cbas"])
        self.setup_cbas_bucket_dataset_connect(self.cb_bucket_name, self.travel_sample_docs_count)
        
        NodeHelper.reboot_server_new(self.cbas_node, self)
        
        items_in_cbas_bucket = 0
        start_time=time.time()
        while (items_in_cbas_bucket == 0 or items_in_cbas_bucket == -1) and time.time()<start_time+120:
            try:
                items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
            except:
                pass
            self.sleep(1)
            
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.travel_sample_docs_count),"Data loss in CBAS.")

    def test_restart_cb(self):
        '''
        Description: This test will restart CB and verify that CBAS is also up and running with CB.
        
        Steps:
        1. Add first cbas node.
        2. Start rebalance, wait for rebalance complete.
        3. Stop Couchbase service, Start Couchbase Service. Wait for service to get started.
        4. Verify that CBAS service is also up Create bucket, datasets, connect bucket. Data ingestion should start.
        
        Author: Ritesh Agarwal
        '''
        self.load_sample_buckets(bucketName=self.cb_bucket_name, total_items=self.travel_sample_docs_count)
        self.add_node(self.cbas_servers[0], services=["cbas"])
        
        NodeHelper.stop_couchbase(self.cbas_servers[0])
        NodeHelper.start_couchbase(self.cbas_servers[0])
        NodeHelper.wait_service_started(self.cbas_servers[0])
        self.sleep(20)
        self.setup_cbas_bucket_dataset_connect(self.cb_bucket_name, self.travel_sample_docs_count)
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name, self.travel_sample_docs_count),"Data loss in CBAS.")

    def test_run_queries_cbas_shutdown(self):
        '''
        Description: This test the ongoing queries while cbas node goes down.
        
        Steps:
        1. Add first cbas node.
        2. Start rebalance, wait for rebalance complete.
        3. Create bucket, datasets, connect bucket. Data ingestion should start.
        4. Add another cbas node, rebalance.
        5. Start concurrent queries on first cbas node.
        6. Second cbas node added in step 4 should be able to serve queries.
        
        Author: Ritesh Agarwal
        '''
        self.load_sample_buckets(bucketName=self.cb_bucket_name, total_items=self.travel_sample_docs_count)
        otpNode = self.add_node(self.cbas_servers[0], services=["cbas"])
        self.setup_cbas_bucket_dataset_connect(self.cb_bucket_name, self.travel_sample_docs_count)
        self.add_node(self.cbas_servers[1], services=["cbas"])
        
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.cbas_util._run_concurrent_queries(query, None, 2000, rest=RestConnection(self.cbas_servers[0]),batch_size=self.concurrent_batch_size)
        
        NodeHelper.stop_couchbase(self.cbas_servers[0])
        self.rest.fail_over(otpNode=otpNode.id)
        self.rebalance()
        NodeHelper.start_couchbase(self.cbas_servers[0])
        NodeHelper.wait_service_started(self.cbas_servers[0])
        
    def test_primary_cbas_shutdown(self):
        '''
        Description: This test will add the second cbas node then start rebalance and cancel rebalance
        before rebalance completes.
        
        Steps:
        1. Add first cbas node.
        2. Start rebalance, wait for rebalance complete.
        3. Create bucket, datasets, connect bucket. Data ingestion should start.
        4. Add another cbas node, rebalance.
        5. Stop Couchbase service for Node1 added in step 1. Failover the node and rebalance.
        6. Second cbas node added in step 4 should be able to serve queries.
        
        Author: Ritesh Agarwal
        '''
        self.load_sample_buckets(bucketName=self.cb_bucket_name, total_items=self.travel_sample_docs_count)
        otpNode = self.add_node(self.cbas_servers[0], services=["cbas"])
        self.setup_cbas_bucket_dataset_connect(self.cb_bucket_name, self.travel_sample_docs_count)
        self.add_node(self.cbas_servers[1], services=["cbas"])
        NodeHelper.stop_couchbase(self.cbas_servers[0])
        self.rest.fail_over(otpNode=otpNode.id)
        self.rebalance()
        
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.cbas_util._run_concurrent_queries(query, "immediate", 100, rest=RestConnection(self.cbas_servers[1]),batch_size=self.concurrent_batch_size)
        NodeHelper.start_couchbase(self.cbas_servers[0])
        NodeHelper.wait_service_started(self.cbas_servers[0])
        
    def test_remove_all_cbas_nodes_in_cluster_add_last_node_back(self):
        '''
        Steps:
        1. For all the cbas nodes provided in ini file, Add all of them in one go and Rebalance.
        2. Remove all nodes together and then rebalance.
        
        Author: Ritesh Agarwal
        '''
        cbas_otpnodes = []
        self.load_sample_buckets(bucketName=self.cb_bucket_name, total_items=self.travel_sample_docs_count)
        cbas_otpnodes.append(self.add_node(self.cbas_servers[0], services=["cbas"]))
        self.setup_cbas_bucket_dataset_connect(self.cb_bucket_name, self.travel_sample_docs_count)
        
        for node in self.cbas_servers[1:]:
            cbas_otpnodes.append(self.add_node(node, services=["cbas"]))
        cbas_otpnodes.reverse()
        for node in cbas_otpnodes:
            self.remove_node([node])
            
        self.add_node(self.cbas_servers[0], services=["cbas"])
        self.setup_cbas_bucket_dataset_connect(self.cb_bucket_name, self.travel_sample_docs_count)
            
    def test_create_bucket_with_default_port(self):
        query = "create bucket " + self.cbas_bucket_name + " with {\"name\":\"" + self.cb_bucket_name + "\",\"nodes\":\"" + self.master.ip + ":" +"8091" +"\"};"
        self.load_sample_buckets(bucketName=self.cb_bucket_name, total_items=self.travel_sample_docs_count)
        self.add_node(self.cbas_servers[0], services=["cbas"])
        self.cbas_util.createConn(self.cb_bucket_name)
        result = self.cbas_util.execute_statement_on_cbas_util(query, "immediate")[0]
        self.assertTrue(result == "success", "CBAS bucket cannot be created with provided port: %s"%query)
        
        self.assertTrue(self.cbas_util.create_dataset_on_bucket(cbas_bucket_name=self.cb_bucket_name,
                          cbas_dataset_name=self.cbas_dataset_name), "dataset creation failed on cbas")
        self.assertTrue(self.cbas_util.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name, cb_bucket_password="password", cb_bucket_username="Administrator"),
                        "Connecting cbas bucket to cb bucket failed")
        self.assertTrue(self.cbas_util.wait_for_ingestion_complete([self.cbas_dataset_name], self.travel_sample_docs_count),"Data ingestion to cbas couldn't complete in 300 seconds.")


class CBASServiceOperations(CBASBaseTest):

    def setUp(self):
        super(CBASServiceOperations, self).setUp()
    
    def fetch_test_case_arguments(self):
        self.cb_bucket_name = self.input.param("cb_bucket_name", "default")
        self.cbas_bucket_name = self.input.param("cbas_bucket_name", "default_cbas")
        self.dataset_name = self.input.param("dataset_name", "ds")
        self.default_bucket = self.input.param("default_bucket", True)
        self.batch_size = self.input.param("batch_size", 5000)
        self.num_items = self.input.param("items", 10000)
        self.kill_on_nc = self.input.param("kill_on_nc", True)
        self.kill_on_cc = self.input.param("kill_on_cc", True)
        self.process = self.input.param('process', 'java')
        self.service = self.input.param('service', '/opt/couchbase/lib/cbas/runtime/bin/java')
        self.signum = self.input.param('signum', 9)

    def set_up_test(self):
        self.log.info("Fetch test params")
        self.fetch_test_case_arguments()

        self.log.info("Add a KV nodes")
        result = self.add_node(self.servers[1], services=["kv"], rebalance=False)
        self.assertTrue(result, msg="Failed to add KV node.")

        self.log.info("Add a CBAS node")
        result = self.add_node(self.cbas_servers[0], services=["cbas"], rebalance=True)
        self.assertTrue(result, msg="Failed to add CBAS node.")

        self.log.info("Load data in the default bucket")
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", 0, self.num_items, exp=0)

        self.log.info("Create primary index")
        query = "CREATE PRIMARY INDEX ON {0} using gsi".format(self.cb_bucket_name)
        self.rest.query_tool(query)

        self.log.info("Create a connection")
        self.cbas_util.createConn(self.cb_bucket_name)

        self.log.info("Create a CBAS bucket")
        self.cbas_util.create_bucket_on_cbas(cbas_bucket_name=self.cbas_bucket_name,
                                             cb_bucket_name=self.cb_bucket_name)

        self.log.info("Create a default data-set")
        self.cbas_util.create_dataset_on_bucket(cbas_bucket_name=self.cb_bucket_name,
                                                cbas_dataset_name=self.dataset_name)

        self.log.info("Connect to cbas bucket")
        self.cbas_util.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                                         cb_bucket_password=self.cb_bucket_password)
    '''
    -t cbas.cbas_cluster_management.CBASServiceOperations.test_signal_impact_on_cbas,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=default_bucket,dataset_name=default_ds,items=10,batch_size=5000,process=/opt/couchbase/lib/cbas/runtime/bin/java,service=java
    -t cbas.cbas_cluster_management.CBASServiceOperations.test_signal_impact_on_cbas,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=default_bucket,dataset_name=default_ds,items=10,batch_size=5000,process=/opt/couchbase/bin/cbas,service=cbas
    '''
    def test_signal_impact_on_cbas(self):
        self.log.info("Add nodes, create cbas bucket and dataset")
        self.set_up_test()

        self.log.info("Wait for ingestion to complete and verify count")
        self.cbas_util.wait_for_ingestion_complete([self.dataset_name], self.num_items)
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.dataset_name, self.num_items))

        self.log.info("Establish a remote connection")
        con_cbas_node1 = RemoteMachineShellConnection(self.cbas_node)
        con_cbas_node2 = RemoteMachineShellConnection(self.cbas_servers[0])

        self.log.info("SIGSTOP ANALYTICS SERVICE")
        con_cbas_node1.kill_process(self.process, self.service, 19)
        con_cbas_node2.kill_process(self.process, self.service, 19)
        
        self.log.info("Add more documents in the default bucket")
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", self.num_items, self.num_items * 2, exp=0,
                                               batch_size=self.batch_size)
        
        self.log.info("SIGCONT ANALYTICS")
        con_cbas_node1.kill_process(self.process, self.service, 18)
        con_cbas_node2.kill_process(self.process, self.service, 18)
        self.sleep(15)

        self.log.info("Wait for ingestion to complete and verify count")
        self.cbas_util.wait_for_ingestion_complete([self.dataset_name], self.num_items * 2)
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.dataset_name, self.num_items * 2))
        
        self.log.info("SIGSTOP ANALYTICS SERVICE")
        con_cbas_node1.kill_process(self.process, self.service, 19)
        con_cbas_node2.kill_process(self.process, self.service, 19)
        
        self.log.info("Delete documents in the default bucket")
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "delete", 0, self.num_items, exp=0,
                                               batch_size=self.batch_size)

        self.log.info("SIGCONT ANALYTICS")
        con_cbas_node1.kill_process(self.process, self.service, 18)
        con_cbas_node2.kill_process(self.process, self.service, 18)
        self.sleep(15)

        self.log.info("Wait for ingestion to complete and verify count")
        self.cbas_util.wait_for_ingestion_complete([self.dataset_name], self.num_items)
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.dataset_name, self.num_items))
    
    '''
    -t cbas.cbas_cluster_management.CBASServiceOperations.test_restart_of_all_nodes,default_bucket=True,cb_bucket_name=default,cbas_bucket_name=default_bucket,dataset_name=default_ds,items=10,batch_size=5000,restart_kv=true,restart_cbas=True
    '''
    def test_restart_of_all_nodes(self):
        
        self.log.info("Add nodes, create cbas bucket and dataset")
        self.set_up_test()
        
        self.log.info("Wait for ingestion to complete and verify count")
        self.cbas_util.wait_for_ingestion_complete([self.dataset_name], self.num_items)
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.dataset_name, self.num_items))

        self.log.info("Restart nodes")
        restart_kv = self.input.param("restart_kv", True)
        restart_cbas = self.input.param("restart_cbas", True)
        self.restart_servers = []

        if restart_kv:
            for kv_server in self.kv_servers:
                self.restart_servers.append(kv_server)
        if restart_cbas:
            self.restart_servers.append(self.cbas_node)
            for cbas_server in self.cbas_servers:
                self.restart_servers.append(cbas_server)

        for restart_node in self.restart_servers:
            NodeHelper.reboot_server_new(restart_node, self)
        self.sleep(15, message="Wait for service to be up and accept request")
            
        self.log.info("Add more documents in the default bucket")
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", self.num_items, self.num_items * 2, exp=0,
                                               batch_size=self.batch_size)

        self.log.info("Wait for ingestion to complete and verify count")
        self.cbas_util.wait_for_ingestion_complete([self.dataset_name], self.num_items * 2)
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.dataset_name, self.num_items * 2))

        self.log.info("Delete documents in the default bucket")
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "delete", 0, self.num_items, exp=0,
                                               batch_size=self.batch_size)

        self.log.info("Wait for ingestion to complete and verify count")
        self.cbas_util.wait_for_ingestion_complete([self.dataset_name], self.num_items)
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.dataset_name, self.num_items))

    def test_analytics_recovery_on_idle_system(self):

        self.log.info("Load data, create cbas buckets, and datasets")
        self.set_up_test()

        self.log.info("Wait for ingestion to complete")
        self.cbas_util.wait_for_ingestion_complete([self.dataset_name], self.num_items)
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.dataset_name, self.num_items))

        self.log.info("Get the nodes on which kill is to be run")
        self.nodes_to_kill_service_on = []
        if self.kill_on_cc:
            self.nodes_to_kill_service_on.append(self.cbas_node)
        if self.kill_on_nc:
            for cbas_server in self.cbas_servers:
                self.nodes_to_kill_service_on.append(cbas_server)

        self.log.info("Establish a remote connection on node and kill service")
        for node in self.nodes_to_kill_service_on:
            shell = RemoteMachineShellConnection(node)
            shell.kill_process(self.process, self.service, signum=self.signum)
        
        self.sleep(5, "Sleeping for 5 seconds as after killing the service the service takes some time to exit and the service checks get pass by that time.")
        self.log.info("Wait for request to complete and cluster to be active: Using private ping() function")
        service_up = False
        start_time = time.time()
        while time.time() < start_time + 120:
            try:
                status, metrics, _, cbas_result, _ = self.cbas_util.execute_statement_on_cbas_util("set `import-private-functions` `true`;ping();")
                if status == "success":
                    service_up = True
                    break
            except:
                pass
            self.sleep(1)
        self.assertTrue(service_up, msg="CBAS service was not up even after 120 seconds of process kill. Failing the test possible a bug")
        
        self.log.info("Observe no reingestion on node after restart")
        items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.dataset_name)
        self.assertTrue(items_in_cbas_bucket > 0, msg="Items in CBAS bucket must greather than 0. If not re-ingestion has happened")
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.dataset_name, self.num_items))

        self.log.info("Add more documents in the default bucket")
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", self.num_items, self.num_items * 2, exp=0,
                                               batch_size=self.batch_size)

        self.log.info("Wait for ingestion to complete")
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.dataset_name, self.num_items * 2))

    def test_analytics_recovery_on_busy_system(self):

        self.log.info("Load data, create cbas buckets, and datasets")
        self.set_up_test()

        self.log.info("Wait for ingestion to complete")
        self.cbas_util.wait_for_ingestion_complete([self.dataset_name], self.num_items)
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.dataset_name, self.num_items))

        self.log.info("Get the nodes on which kill is to be run")
        self.nodes_to_kill_service_on = []
        if self.kill_on_cc:
            neglect_failures = True
            self.nodes_to_kill_service_on.append(self.cbas_node)
        if self.kill_on_nc:
            for cbas_server in self.cbas_servers:
                self.nodes_to_kill_service_on.append(cbas_server)

        self.log.info("Run concurrent queries to simulate busy system")
        statement = "select sleep(count(*),50000) from {0} where mutated=0;".format(self.dataset_name)
        try:
            self.cbas_util._run_concurrent_queries(statement, "async", 10, batch_size=10)
        except Exception as e:
            if neglect_failures:
                self.log.info("Neglecting failed queries, to handle killing Java/Cbas process kill on CC & NC node %s"%e)
            else:
                raise e

        self.log.info("Establish a remote connection on node and kill service")
        for node in self.nodes_to_kill_service_on:
            shell = RemoteMachineShellConnection(node)
            shell.kill_process(self.process, self.service, signum=self.signum)
        
        self.sleep(5, "Sleeping for 5 seconds as after killing the service the service takes some time to exit and the service checks get pass by that time.")
        self.log.info("Wait for request to complete and cluster to be active: Using private ping() function")
        service_up = False
        start_time = time.time()
        while time.time() < start_time + 120:
            try:
                status, metrics, _, cbas_result, _ = self.cbas_util.execute_statement_on_cbas_util("set `import-private-functions` `true`;ping();",
                                                                                                   timeout=600, analytics_timeout=600)
                if status == "success":
                    service_up = True
                    break
            except:
                pass
            self.sleep(1)
            
        self.assertTrue(service_up, msg="CBAS service was not up even after 120 seconds of process kill. Failing the test possible a bug")

        self.log.info("Observe no reingestion on node after restart")
        items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.dataset_name)
        self.assertTrue(items_in_cbas_bucket > 0, msg="Items in CBAS bucket must greather than 0. If not re-ingestion has happened")
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.dataset_name, self.num_items))

        self.log.info("Add more documents in the default bucket")
        self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", self.num_items, self.num_items * 2, exp=0,
                                               batch_size=self.batch_size)

        self.log.info("Wait for ingestion to complete")
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(self.dataset_name, self.num_items * 2))

    def tearDown(self):
        super(CBASServiceOperations, self).tearDown()