import time


from basetestcase import BaseTestCase
from couchbase_helper.tuq_generators import JsonGenerator

from membase.api.rest_client import RestConnection
# from memcached.helper.data_helper import VBucketAwareMemcached
from sdk_client3 import SDKClient as VBucketAwareMemcached
from remote.remote_util import RemoteMachineShellConnection
from couchbase_helper.documentgenerator import BlobGenerator, DocumentGenerator
import random
from __builtin__ import True




"""
Capture basic get, set operations, also the meta operations.
This is based on some 4.1.1 test which had separate
bugs with incr and delete with meta and I didn't see an obvious home for them.

This is small now but we will reactively add things

These may be parameterized by:
   - full and value eviction
   - DGM and non-DGM
"""


class basic_ops(BaseTestCase):
    def setUp(self):
        super(basic_ops, self).setUp()

        self.key = 'test_docs'.rjust(self.key_size, '0')

        nodes_init = self.cluster.servers[1:self.nodes_init] if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend([self.cluster.master] + nodes_init)
        self.bucket_util.add_rbac_user()
        
        

        # Reset active_resident_threshold to avoid further data load as DGM
        self.active_resident_threshold = 0
        self.log.info("==========Finished Basic_ops base setup========")

    def tearDown(self):
        super(basic_ops, self).tearDown()

    def get_doc_generator(self, start, end):
        age = range(5)
        first = ['james', 'sharon']
        body = [''.rjust(self.doc_size - 10, 'a')]
        template = '{{ "age": {0}, "first_name": "{1}", "body": "{2}"}}'
        generator = DocumentGenerator(self.key, template, age, first, body, start=start,
                                      end=end)
        return generator
    
    def generate_docs_bigdata(self, docs_per_day, start=0, document_size=1024000):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_bigdata(end=docs_per_day, start=start, value_size=document_size)

        
    def test_basic_commit(self):
        ''' Test transaction commit, rollback, time ahead, time behind scenarios with replica, persist_to and replicate_to settings '''
        # Atomicity.basic_ops.basic_ops.test_basic_commit
        self.transaction_timeout = self.input.param("transaction_timeout", 100)
        self.transaction_commit = self.input.param("transaction_commit", True)
        self.drift_ahead = self.input.param("drift_ahead", False)
        self.drift_behind = self.input.param("drift_behind", False)
        gen_create = self.get_doc_generator(0, self.num_items)
        self.op_type = self.input.param("op_type", 'create')
        self.num_buckets = self.input.param("num_buckets", 0)
        self.default_bucket = self.input.param("default_bucket", True)
        self.standard_bucket = self.input.param("standard_bucket", 1)
        self.exp = self.input.param("expiry", 0)
#         print_ops_task = self.bucket_util.async_print_bucket_ops(def_bucket)

        if self.default_bucket:
            self.bucket_util.create_default_bucket(replica=self.num_replicas,
                                               compression_mode=self.compression_mode, ram_quota=100)
        
        if self.drift_ahead:
            shell = RemoteMachineShellConnection(self.servers[0])
            self.assertTrue(  shell.change_system_time(3600), 'Failed to advance the clock')

            output,error = shell.execute_command('date')
            self.log.info('Date after is set forward {0}'.format( output ))
        
        if self.drift_behind:
            shell = RemoteMachineShellConnection(self.servers[0])
            self.assertTrue(  shell.change_system_time(-3600), 'Failed to advance the clock')

            output,error = shell.execute_command('date')
            self.log.info('Date after is set behind {0}'.format( output ))

        if self.num_buckets:
            self.bucket_util.create_multiple_buckets(self.cluster.master, self.num_replicas, bucket_count=self.num_buckets, bucket_type='ephemeral')
             
             
        if self.standard_bucket:
            self.bucket_util.create_standard_buckets(self.cluster.master, self.standard_buckets, bucket_size=100)
            
        self.def_bucket= self.bucket_util.get_all_buckets()
#         client = VBucketAwareMemcached(RestConnection(self.cluster.master), self.def_bucket)
#         
#         Transaction().nonTxnRemoves(client.collection)
  
        print "going to create a task"
        task = self.task.async_load_gen_docs_atomicity(self.cluster, self.def_bucket,
                                             gen_create, self.op_type , exp=self.exp,
                                             batch_size=10,
                                             process_concurrency=8,
                                             replicate_to=self.replicate_to,
                                             persist_to=self.persist_to, timeout_secs=self.sdk_timeout,
                                             retries=self.sdk_retries, transaction_timeout=self.transaction_timeout, commit=self.transaction_commit)
        print "going to execute the task"
        self.task.jython_task_manager.get_task_result(task)

#         print_ops_task.end_task()



    def test_large_doc_size_commit(self):
        # Atomicity.basic_ops.basic_ops.test_basic_commit
        self.transaction_timeout = self.input.param("transaction_timeout", 100)
        self.transaction_commit = self.input.param("transaction_commit", True)
        gen_create = self.generate_docs_bigdata(docs_per_day=self.num_items, document_size=self.doc_size)
        print_ops_task = self.bucket_util.async_print_bucket_ops(self.def_bucket)
        print "going to create a task"
        task = self.task.async_load_gen_docs_atomicity(self.cluster, self.def_bucket,
                                             gen_create, "created" , exp=0,
                                             batch_size=10,
                                             process_concurrency=8,
                                             replicate_to=self.replicate_to,
                                             persist_to=self.persist_to, timeout_secs=self.transaction_timeout,
                                             retries=self.sdk_retries, commit=self.transaction_commit)
        print "going to execute the task"
        self.task.jython_task_manager.get_task_result(task)
        self.bucket_util._wait_for_stats_all_buckets()
        if (self.transaction_commit == False):
            self.num_items = 0

        print_ops_task.end_task()

        
  

                

            
        
        
            
        
                


              
                
        
            

