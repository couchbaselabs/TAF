from cbas_base import *
from membase.api.rest_client import RestHelper
import json
import time

class CBASDDLTests(CBASBaseTest):
    def setUp(self):
        self.input = TestInputSingleton.input
        if "default_bucket" not in self.input.test_params:
            self.input.test_params.update({"default_bucket":False})
        super(CBASDDLTests, self).setUp()
        
        self.validate_error = False
        if self.expected_error:
            self.validate_error = True
            
        ''' Considering all the scenarios where:
        1. There can be 1 KV and multiple cbas nodes(and tests wants to add all cbas into cluster.)
        2. There can be 1 KV and multiple cbas nodes(and tests wants only 1 cbas node)
        3. There can be only 1 node running KV,CBAS service.
        NOTE: Cases pending where there are nodes which are running only cbas. For that service check on nodes is needed.
        '''
        if "add_all_cbas_nodes" in self.input.test_params and self.input.test_params["add_all_cbas_nodes"] and len(self.cbas_servers) >= 1:
            self.add_all_nodes_then_rebalance(self.cbas_servers)
            result = self.load_sample_buckets(servers=list(set(self.cbas_servers + [self.master, self.cbas_node])),
                                              bucketName="travel-sample",
                                              total_items=self.travel_sample_docs_count)
        else:
            result = self.load_sample_buckets(servers=list(set([self.master,self.cbas_node])),
                                              bucketName="travel-sample",
                                              total_items=self.travel_sample_docs_count)

        self.assertTrue(result, msg="wait_for_memcached failed while loading sample bucket: travel-sample")
        self.cbas_util.createConn("travel-sample")
        
    def test_create_link_Local(self):
        code = self.input.param('error_code', None)
        result = self.cbas_util.create_link_on_cbas(link_name="Local",validate_error_msg=self.validate_error, expected_error=self.expected_error, expected_error_code=code)
        self.assertTrue(result, "another local link creation succeeded.")
        
    def test_drop_link_Local(self):
        code = self.input.param('error_code', None)
        result = self.cbas_util.drop_link_on_cbas(link_name="Local",validate_error_msg=self.validate_error, expected_error=self.expected_error, expected_error_code=code)
        self.assertTrue(result, "another local link creation succeeded.")

    def test_create_dataverse_Default(self):
        code = self.input.param('error_code', None)
        result = self.cbas_util.create_dataverse_on_cbas(dataverse_name="Default",validate_error_msg=self.validate_error, expected_error=self.expected_error, expected_error_code=code)
        self.assertTrue(result, "another local link creation succeeded.")
        
    def test_drop_dataverse_Default(self):
        code = self.input.param('error_code', None)
        result = self.cbas_util.drop_dataverse_on_cbas(dataverse_name="Default",validate_error_msg=self.validate_error, expected_error=self.expected_error, expected_error_code=code)
        self.assertTrue(result, "another local link creation succeeded.")
        
    def test_create_multiple_dataverse(self):
        # Create dataset on the CBAS bucket
        result = self.cbas_util.create_dataset_on_bucket(
            cbas_bucket_name=self.cb_bucket_name,
            cbas_dataset_name=self.cbas_dataset_name)
        self.assertTrue(result, "another local link creation succeeded.")
        
        number_of_dataverse = self.input.param("number_of_dataverse",2)
        
        for num in xrange(1,number_of_dataverse+1):
            dataverse_name = "Default"+str(num)
            cbas_dataset_name = self.cbas_dataset_name+str(num)
            
            result = self.cbas_util.create_dataverse_on_cbas(dataverse_name)
            self.assertTrue(result, "Dataverse Creation Failed.")
            
            cmd_use_dataset = "use %s;"%dataverse_name
            status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(
                cmd_use_dataset)
            
            result = self.cbas_util.create_dataset_on_bucket(
                cbas_bucket_name=self.cb_bucket_name,
                cbas_dataset_name=cbas_dataset_name)
            
        self.cbas_util.connect_link()
        
        for num in xrange(1,number_of_dataverse+1):
            dataverse_name = "Default"+str(num)
            cbas_dataset_name = self.cbas_dataset_name+str(num)
        
            cmd_use_dataset = "use %s;"%dataverse_name
            status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(
                cmd_use_dataset)
            self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(cbas_dataset_name, self.travel_sample_docs_count),"Data loss in CBAS.")

    def test_connect_link_dataverse_Local(self):
        # Create dataset on the CBAS bucket
        result = self.cbas_util.create_dataset_on_bucket(
            cbas_bucket_name=self.cb_bucket_name,
            cbas_dataset_name=self.cbas_dataset_name)
        self.assertTrue(result, "another local link creation succeeded.")
        
        number_of_dataverse = self.input.param("number_of_dataverse",2)
        
        for num in xrange(1,number_of_dataverse+1):
            dataverse_name = "Default"+str(num)
            cbas_dataset_name = self.cbas_dataset_name+str(num)
            
            result = self.cbas_util.create_dataverse_on_cbas(dataverse_name)
            self.assertTrue(result, "Dataverse Creation Failed.")
            
            cmd_use_dataset = "use %s;"%dataverse_name
            cmd_create_dataset = 'create dataset travel_ds1 on `travel-sample`;'
            cmd = cmd_use_dataset+cmd_create_dataset
            status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(
                cmd)
            
            self.cbas_util.connect_link(link_name=dataverse_name+".Local")
            self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count(dataverse_name+"."+cbas_dataset_name, self.travel_sample_docs_count),"Data loss in CBAS.")
        
    def test_connect_link_delete_bucket(self):
        # Create dataset on the CBAS bucket
        number_of_datasets = self.input.param("number_of_datasets",2)
        
        for num in xrange(number_of_datasets):
            cbas_dataset_name = self.cbas_dataset_name+str(num)
            
            result = self.cbas_util.create_dataset_on_bucket(
                cbas_bucket_name=self.cb_bucket_name,
                cbas_dataset_name=cbas_dataset_name)
            self.assertTrue(result, "Dataset Creation Failed.")
            
        self.cbas_util.connect_link()
        self.sleep(30, "Wait for 30 secs after connect link.")
        
        self.bucket_util.delete_bucket_or_assert(self.master, "travel-sample", self)
        self.sleep(5, "Wait for 5 secs after deleting bucket.")
        
        self.log.info("Request sent will now either succeed or fail, or its connection will be abruptly closed. Verify the state")
        start_time = time.time()
        
        while start_time+120 > time.time():
            status, content, _ = self.cbas_util.fetch_bucket_state_on_cbas()
            self.assertTrue(status, msg="Fetch bucket state failed")
            content = json.loads(content)
            self.log.info(content)
            if content['buckets'][0]['state'] == "disconnected":
                break
            
        for num in xrange(number_of_datasets):
            cbas_dataset_name = self.cbas_dataset_name+str(num)
            self.assertTrue(self.cbas_util.drop_dataset(cbas_dataset_name),"Drop dataset after deleting KV bucket failed without disconnecting link.")

    def test_drop_one_bucket(self):
        result = self.load_sample_buckets(servers=list(set([self.master,self.cbas_node])),
                                          bucketName="beer-sample",
                                          total_items=self.beer_sample_docs_count)
        self.assertTrue(result, "Bucket Creation Failed.")

        result = self.cbas_util.create_dataset_on_bucket(
            cbas_bucket_name="travel-sample",
            cbas_dataset_name="ds1")
        self.assertTrue(result, "Dataset Creation Failed.")

        result = self.cbas_util.create_dataset_on_bucket(
            cbas_bucket_name="beer-sample",
            cbas_dataset_name="ds2")
        self.assertTrue(result, "Dataset Creation Failed.")
                    
        self.cbas_util.connect_link()
        self.bucket_util.delete_bucket_or_assert(self.master, "beer-sample", self)  
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count("ds1", self.travel_sample_docs_count),"Data loss in CBAS.")

    def test_create_dataset_on_connected_link(self):
        result = self.load_sample_buckets(servers=list(set([self.master,self.cbas_node])),
                                          bucketName="beer-sample",
                                          total_items=self.beer_sample_docs_count)
        self.assertTrue(result, "Bucket Creation Failed.")

        result = self.cbas_util.create_dataset_on_bucket(
            cbas_bucket_name="beer-sample",
            cbas_dataset_name="ds2")
        self.assertTrue(result, "Dataset Creation Failed.")
                    
        self.cbas_util.connect_link()
        
        result = self.cbas_util.create_dataset_on_bucket(
            cbas_bucket_name="travel-sample",
            cbas_dataset_name="ds1")
        self.assertTrue(result, "Dataset Creation Failed.")  
        
        self.cbas_util.connect_link()
        self.assertTrue(self.cbas_util.validate_cbas_dataset_items_count("ds1", self.travel_sample_docs_count),"Data loss in CBAS.")