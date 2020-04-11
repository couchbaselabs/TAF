'''
Created on Mar 13, 2018

@author: riteshagarwal
'''

from cbas_base import *
from sdk_client3 import SDKSmartClient
from couchbase_helper.tuq_generators import JsonGenerator

class CBASSQL_Alignment(CBASBaseTest):
    def setUp(self):
        self.input = TestInputSingleton.input
        if "cb_bucket_name" not in self.input.test_params:
            self.input.test_params.update({"default_bucket":False})

        super(CBASSQL_Alignment, self).setUp()
        self.validate_error = False
        if self.expected_error:
            self.validate_error = True

    def setupForTest(self):
        self.cbas_util.createConn("default")
        json_generator = JsonGenerator()
        generators = json_generator.generate_all_type_documents_for_gsi(docs_per_day=10,
            start=0)
        tasks = self.bucket_util._async_load_all_buckets(self.cluster, generators,"create",0)
        for task in tasks:
            self.task_manager.get_task_result(task)
        # Create dataset on the CBAS bucket
        self.cbas_util.create_dataset_on_bucket(
            cbas_bucket_name=self.cb_bucket_name,
            cbas_dataset_name=self.cbas_dataset_name)

        # Connect to Bucket
        self.cbas_util.connect_to_bucket(cbas_bucket_name=self.cbas_bucket_name,
                               cb_bucket_password=self.cb_bucket_password)
 
        # Allow ingestion to complete
        self.cbas_util.wait_for_ingestion_complete([self.cbas_dataset_name], 10, 300)
        
        #load some data to allow incompatible comparisons.
        data_dict = {"name":[123456,[234234,234234],None,{'key':'value'},True,12345.12345],
                 "age":["String", [234234,234234],None,{'key':'value'},True,12345.12345],
                 "premium_customer":["String", 12345567,[234234,234234,"string"],None,{'key':'value'},123456.123456,],
                 "travel_history":["String", 12345567,None,{'key':'value'},123456.123456,],
                 "address":["String", 12345567,[234234,134234,"string"],None,123456.123456,]
                 }
        self.client = SDKSmartClient(RestConnection(self.cluster.master), "default", self.cluster.master)
        i=0
        for key in data_dict.keys():
            for value in data_dict[key]:
#                 jsonDump = json.dumps({key:value})
                self.client.set("incompatible_doc_%s"%i, 0, 0, {key:value})
                i+=1
        self.client.close()
        
    def test_incompatible_types_comparison(self):
        self.setupForTest()
        
        query_string = "SELECT count(*) FROM default_ds where name < 'a';"
        query_integer = "SELECT count(*) FROM default_ds where age > 1;"
        query_bool = "SELECT count(*) FROM default_ds where premium_customer = True;"
        query_null = "SELECT count(*) FROM default_ds where premium_customer is null;"
        '''Object and array comparisons are out of Vulcan release'''
#         query_array = "SELECT count(*) FROM default_ds where travel_history = ['India'];"
#         query_json_object = "SELECT count(*) FROM default_ds where address < 'a';"
        
        queries = [query_string,query_integer,query_bool,query_null]
        
        for query in queries:
            status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(query)
            self.log.info("Query: %s , Result: %s"%(query,results))
            self.assertTrue(status == "success", "Query failed")
            self.assertTrue(results[0]["$1"]>=0, "Result is incorrect")

    def test_incompatible_types_orderBy(self):
        self.setupForTest()
        
        query_string = "SELECT name FROM default_ds order by name;"
        query_integer = "SELECT age FROM default_ds order by age;"
        query_bool = "SELECT premium_customer FROM default_ds order by premium_customer;"
        query_null = "SELECT premium_customer FROM default_ds order by premium_customer;"
        query_array = "SELECT travel_history FROM default_ds order by travel_history;"
        query_json_object = "SELECT address FROM default_ds order by address;"
        
        queries = [query_string,query_integer,query_bool,query_null,query_array,query_json_object]
        
        for query in queries:
            status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(query)
            self.log.info("Query: %s , Result: %s"%(query,results))
            self.assertTrue(status == "success", "Query failed")
            self.assertTrue(len(results)==38, "Result is incorrect")
        