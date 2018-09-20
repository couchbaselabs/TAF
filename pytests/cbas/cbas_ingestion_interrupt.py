'''
Created on Jan 31, 2018

@author: riteshagarwal
'''

from cbas_base import CBASBaseTest, TestInputSingleton
from lib.memcached.helper.data_helper import MemcachedClientHelper
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
import time
from node_utils.node_ready_functions import NodeHelper
from cbas.cbas_utils import cbas_utils

class IngestionInterrupt_CBAS(CBASBaseTest):

    def setUp(self):
        self.input = TestInputSingleton.input
        self.input.test_params.update({"default_bucket":False})
        super(IngestionInterrupt_CBAS, self).setUp()
        
        if "add_all_cbas_nodes" in self.input.test_params and self.input.test_params["add_all_cbas_nodes"] and len(self.cbas_servers) > 0:
            self.otpNodes.extend(self.add_all_nodes_then_rebalance(self.cbas_servers))
            
        self.create_default_bucket()
        self.cb_bucket_name = self.input.param('cb_bucket_name', 'default')
        self.cbas_util.createConn("default")
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
        
        self.perform_doc_ops_in_all_cb_buckets(self.num_items*2, "create", self.num_items,
                                                   self.num_items*3, batch_size=1000)
        
        
        self.cbas_util.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                               cb_bucket_password=self.cb_bucket_password)
        
    def test_service_restart(self):
        self.setup_for_test()
        self.restart_method = self.input.param('restart_method',None)
        self.cbas_node_type = self.input.param('cbas_node_type',None)
        
        query = "select sleep(count(*),50000) from {0};".format(self.cbas_dataset_name)
        handles = self.cbas_util._run_concurrent_queries(query,"async",10)
        self.ingestion_in_progress()
        
        if self.cbas_node_type == "CC":
            node_in_test = self.cbas_node
        else:
            node_in_test = self.cbas_servers[0]
        
        items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
        self.log.info("Items before service restart: %s"%items_in_cbas_bucket)
                
        if self.restart_method == "graceful":
            self.log.info("Gracefully re-starting service on node %s"%node_in_test)
            NodeHelper.do_a_warm_up(node_in_test)
            NodeHelper.wait_service_started(node_in_test)
        else:
            self.log.info("Kill Memcached process on node %s"%node_in_test)
            shell = RemoteMachineShellConnection(node_in_test)
            shell.kill_memcached()
        
        items_in_cbas_bucket = 0
        start_time=time.time()
        while (items_in_cbas_bucket == 0 or items_in_cbas_bucket == -1) and time.time()<start_time+60:
            try:
                items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
            except:
                pass
            
        self.log.info("After graceful service restart docs in CBAS bucket : %s"%items_in_cbas_bucket)
        if items_in_cbas_bucket < self.num_items*3 and items_in_cbas_bucket>self.num_items:
            self.log.info("Data Ingestion Interrupted successfully")
        elif items_in_cbas_bucket < self.num_items:
            self.log.info("Data Ingestion did interrupted and restarting from 0.")
        else:
            self.log.info("Data Ingestion did not interrupted but complete before service restart.")
            
        run_count = 0
        fail_count = 0
        success_count = 0
        aborted_count = 0
        shell=RemoteMachineShellConnection(node_in_test)
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
        
        if self.cbas_node_type == "NC":
            self.assertTrue(fail_count+aborted_count==0, "Some queries failed/aborted")
        
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.cbas_util._run_concurrent_queries(query,"immediate",100)
        
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,self.num_items*3):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")

    def test_kill_analytics_service(self):
        self.setup_for_test()
        process_name = self.input.param('process_name',None)
        service_name = self.input.param('service_name',None)
        cbas_node_type = self.input.param('cbas_node_type',None)
        
        query = "select sleep(count(*),50000) from {0};".format(self.cbas_dataset_name)
        handles = self.cbas_util._run_concurrent_queries(query,"async",10)
        self.ingestion_in_progress()
        
        if cbas_node_type == "CC":
            node_in_test = self.cbas_node
        else:
            node_in_test = self.cbas_servers[0]
        
        items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
        self.log.info("Items before service kill: %s"%items_in_cbas_bucket)
        
        self.log.info("Kill %s process on node %s"%(process_name, node_in_test))
        shell = RemoteMachineShellConnection(node_in_test)
        shell.kill_process(process_name, service_name)
        
        items_in_cbas_bucket = 0
        start_time=time.time()
        while (items_in_cbas_bucket == 0 or items_in_cbas_bucket == -1) and time.time()<start_time+60:
            try:
                items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
            except:
                pass
            
#         start_time = time.time()
#         while items_in_cbas_bucket <=0 and time.time()<start_time+120:
#             items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
#             self.sleep(1)
            
#         items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
        self.log.info("After %s kill, docs in CBAS bucket : %s"%(process_name, items_in_cbas_bucket))
        
        if items_in_cbas_bucket < self.num_items*3 and items_in_cbas_bucket>self.num_items:
            self.log.info("Data Ingestion Interrupted successfully")
        elif items_in_cbas_bucket < self.num_items:
            self.log.info("Data Ingestion did not interrupted but restarting from 0.")
        else:
            self.log.info("Data Ingestion did not interrupted but complete before service restart.")
            
        run_count = 0
        fail_count = 0
        success_count = 0
        aborted_count = 0
        shell=RemoteMachineShellConnection(node_in_test)
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
        
        if cbas_node_type == "NC":
            self.assertTrue((fail_count+aborted_count)==0, "Some queries failed/aborted")
                
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.cbas_util._run_concurrent_queries(query,"immediate",100)
        
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,self.num_items*3):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")

    def test_stop_start_service_ingest_data(self):
        self.setup_for_test()
        self.cbas_node_type = self.input.param('cbas_node_type',None)
        
        query = "select sleep(count(*),50000) from {0};".format(self.cbas_dataset_name)
        handles = self.cbas_util._run_concurrent_queries(query,"async",10)
        self.ingestion_in_progress()
        
        if self.cbas_node_type == "CC":
            node_in_test = self.cbas_node
            self.cbas_util.closeConn()
            self.cbas_util = cbas_utils(self.master, self.cbas_servers[0])
            self.cbas_util.createConn("default")
        else:
            node_in_test = self.cbas_servers[0]
        
        items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
        self.log.info("Items before service restart: %s"%items_in_cbas_bucket)
        
        self.log.info("Gracefully stopping service on node %s"%node_in_test)
        NodeHelper.stop_couchbase(node_in_test)
        NodeHelper.start_couchbase(node_in_test)
        NodeHelper.wait_service_started(node_in_test)
#         self.sleep(10, "wait for service to come up.")
#         
#         items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
#         self.log.info("After graceful STOPPING/STARTING service docs in CBAS bucket : %s"%items_in_cbas_bucket)
#         
#         start_time = time.time()
#         while items_in_cbas_bucket <=0 and time.time()<start_time+60:
#             items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
#             self.sleep(1)
        items_in_cbas_bucket = 0
        start_time=time.time()
        while (items_in_cbas_bucket == 0 or items_in_cbas_bucket == -1) and time.time()<start_time+60:
            try:
                items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
            except:
                pass
                    
        if items_in_cbas_bucket < self.num_items*3 and items_in_cbas_bucket>self.num_items:
            self.log.info("Data Ingestion Interrupted successfully")
        elif items_in_cbas_bucket < self.num_items:
            self.log.info("Data Ingestion did not interrupted but restarting from 0.")
        else:
            self.log.info("Data Ingestion did not interrupted but complete before service restart.")
            
        run_count = 0
        fail_count = 0
        success_count = 0
        aborted_count = 0
        shell=RemoteMachineShellConnection(node_in_test)
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
        
        if self.cbas_node_type == "NC":
            self.assertTrue(fail_count+aborted_count==0, "Some queries failed/aborted")
        
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.cbas_util._run_concurrent_queries(query,"immediate",100)
        
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,self.num_items*3):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")
        
    def test_disk_full_ingest_data(self):
        self.cbas_node_type = self.input.param('cbas_node_type',None)
        if self.cbas_node_type == "CC":
            node_in_test = self.cbas_node
            self.cbas_util = cbas_utils(self.master, self.cbas_servers[0])
        else:
            node_in_test = self.cbas_servers[0]
            
        remote_client = RemoteMachineShellConnection(node_in_test)
        output, error = remote_client.execute_command("rm -rf full_disk*", use_channel=True)
        remote_client.log_command_output(output, error)
        
        self.setup_for_test()
        
        query = "select sleep(count(*),50000) from {0};".format(self.cbas_dataset_name)
        handles = self.cbas_util._run_concurrent_queries(query,"async",10)
        
        def _get_disk_usage_in_MB(remote_client):
            disk_info = remote_client.get_disk_info(in_MB=True)
            disk_space = disk_info[1].split()[-3][:-1]
            return disk_space
                
        du = int(_get_disk_usage_in_MB(remote_client)) - 50
        chunk_size=1024
        while int(du) > 0:
            output, error = remote_client.execute_command("dd if=/dev/zero of=full_disk{0} bs={1}M count=1".format(str(du)+ "_MB" + str(time.time()),chunk_size), use_channel=True)
            remote_client.log_command_output(output, error)
            du -= 1024
            if du < 1024:
                chunk_size = du
        
        self.ingestion_in_progress()
                
        items_in_cbas_bucket_before, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
        items_in_cbas_bucket_after, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
        try:
            while items_in_cbas_bucket_before !=items_in_cbas_bucket_after:
                items_in_cbas_bucket_before, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
                self.sleep(2)
                items_in_cbas_bucket_after, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
        except:
            self.log.info("Ingestion interrupted and server seems to be down")
            
        if items_in_cbas_bucket_before == self.num_items*3:
            self.log.info("Data Ingestion did not interrupted but completed.")
        elif items_in_cbas_bucket_before < self.num_items*3:
            self.log.info("Data Ingestion Interrupted successfully")
        
        output, error = remote_client.execute_command("rm -rf full_disk*", use_channel=True)
        remote_client.log_command_output(output, error)
        remote_client.disconnect()
        self.sleep(10, "wait for service to come up after disk space is made available.")
        
        run_count = 0
        fail_count = 0
        success_count = 0
        aborted_count = 0
        shell=RemoteMachineShellConnection(node_in_test)
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
        
        if self.cbas_node_type == "NC":
            self.assertTrue(fail_count+aborted_count==0, "Some queries failed/aborted")
        
        self.sleep(60)
        
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.cbas_util._run_concurrent_queries(query,"immediate",100)
        
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,self.num_items*3):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")

    def test_stop_network_ingest_data(self):
        self.setup_for_test()
        self.cbas_node_type = self.input.param('cbas_node_type',None)
        
        query = "select sleep(count(*),50000) from {0};".format(self.cbas_dataset_name)
        handles = self.cbas_util._run_concurrent_queries(query,"async",10)
        self.ingestion_in_progress()
        
        # Add the code for stop network here:
        if self.cbas_node_type:
            if self.cbas_node_type == "CC":
                node_in_test = self.cbas_node
                self.cbas_util = cbas_utils(self.master, self.cbas_servers[0])
                self.cbas_util.createConn("default")
            else:
                node_in_test = self.cbas_servers[0]
        # Stop network on KV node to mimic n/w partition on KV
        else:
            node_in_test = self.master
        
        items_in_cbas_bucket_before, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
        self.log.info("Intems before network down: %s"%items_in_cbas_bucket_before)
        RemoteMachineShellConnection(node_in_test).stop_network("30")
#         self.sleep(40, "Wait for network to come up.")

        items_in_cbas_bucket = 0
        start_time=time.time()
        while (items_in_cbas_bucket == 0 or items_in_cbas_bucket == -1) and time.time()<start_time+60:
            try:
                items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
            except:
                pass        
#         items_in_cbas_bucket_after, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
        self.log.info("Items after network is up: %s"%items_in_cbas_bucket)
#         start_time = time.time()
#         while items_in_cbas_bucket_after <=0 and time.time()<start_time+60:
#             items_in_cbas_bucket_after, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
#             self.sleep(1)
#         items_in_cbas_bucket = items_in_cbas_bucket_after
        if items_in_cbas_bucket < self.num_items*3 and items_in_cbas_bucket>self.num_items:
            self.log.info("Data Ingestion Interrupted successfully")
        elif items_in_cbas_bucket < self.num_items:
            self.log.info("Data Ingestion did not interrupted but restarting from 0.")
        else:
            self.log.info("Data Ingestion did not interrupted but complete before service restart.")
            
        run_count = 0
        fail_count = 0
        success_count = 0
        aborted_count = 0
        shell=RemoteMachineShellConnection(node_in_test)
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
        
        if self.cbas_node_type == "NC":
            self.assertTrue(fail_count+aborted_count==0, "Some queries failed/aborted")
        
        query = "select count(*) from {0};".format(self.cbas_dataset_name)
        self.cbas_util._run_concurrent_queries(query,"immediate",100)
        
        if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,self.num_items*3):
            self.fail("No. of items in CBAS dataset do not match that in the CB bucket")

    def test_network_hardening(self):
        self.setup_for_test()
        
        end = self.num_items
        CC = self.cbas_node
        NC = self.cbas_servers          
        KV = self.master
        nodes = [CC] + NC
        
        for node in nodes: 
            for i in xrange(2):        
                NodeHelper.enable_firewall(node)
                
                start = end
                end = start + self.num_items
                tasks = self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", start,
                                                                   end, batch_size=1000,_async=True)
                
                self.sleep(30, "Sleep after enabling firewall on node %s then disbale it."%node.ip)
                NodeHelper.disable_firewall(node)
                
                items_in_cbas_bucket = 0
                start_time=time.time()
                while (items_in_cbas_bucket == 0 or items_in_cbas_bucket == -1) and time.time()<start_time+60:
                    try:
                        items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
                    except:
                        pass        
                self.log.info("Items after network is up: %s"%items_in_cbas_bucket)
        
                if items_in_cbas_bucket < end and items_in_cbas_bucket>start:
                    self.log.info("Data Ingestion Interrupted successfully")
                elif items_in_cbas_bucket < start:
                    self.log.info("Data Ingestion did not interrupted but restarting from 0.")
                else:
                    self.log.info("Data Ingestion did not interrupted but complete before service restart.")
                    
                query = "select count(*) from {0};".format(self.cbas_dataset_name)
                self.cbas_util._run_concurrent_queries(query,"immediate",100)
                
                for task in tasks:
                    task.get_result()
                    
                if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,end):
                    self.fail("No. of items in CBAS dataset do not match that in the CB bucket")
    
                NodeHelper.enable_firewall(node,bidirectional=True)
                
                start = end
                end = start + self.num_items
                tasks = self.perform_doc_ops_in_all_cb_buckets(self.num_items, "create", start,
                                                                   end, batch_size=1000,_async=True)
                
                self.sleep(30, "Sleep after enabling firewall on CC node then disbale it.")
                NodeHelper.disable_firewall(node)
                
                items_in_cbas_bucket = 0
                start_time=time.time()
                while (items_in_cbas_bucket == 0 or items_in_cbas_bucket == -1) and time.time()<start_time+60:
                    try:
                        items_in_cbas_bucket, _ = self.cbas_util.get_num_items_in_cbas_dataset(self.cbas_dataset_name)
                    except:
                        pass        
                self.log.info("Items after network is up: %s"%items_in_cbas_bucket)
        
                if items_in_cbas_bucket < end and items_in_cbas_bucket>start:
                    self.log.info("Data Ingestion Interrupted successfully")
                elif items_in_cbas_bucket < start:
                    self.log.info("Data Ingestion did not interrupted but restarting from 0.")
                else:
                    self.log.info("Data Ingestion did not interrupted but complete before service restart.")
                    
                query = "select count(*) from {0};".format(self.cbas_dataset_name)
                self.cbas_util._run_concurrent_queries(query,"immediate",100)
                
                for task in tasks:
                    task.get_result()
                    
                if not self.cbas_util.validate_cbas_dataset_items_count(self.cbas_dataset_name,end):
                    self.fail("No. of items in CBAS dataset do not match that in the CB bucket")