import time
from basetestcase import BaseTestCase
from couchbase_helper.tuq_generators import JsonGenerator
from remote.remote_util import RemoteMachineShellConnection
from couchbase_helper.documentgenerator import DocumentGenerator
from __builtin__ import True
from BucketLib.bucket import Bucket

"""
Basic test cases with commit,rollback scenarios
"""

class basic_ops(BaseTestCase):
    def setUp(self):
        super(basic_ops, self).setUp()

        self.key = 'test_docs'.rjust(self.key_size, '0')

        nodes_init = self.cluster.servers[1:self.nodes_init] if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend([self.cluster.master] + nodes_init)
        self.bucket_util.add_rbac_user()
        
        if self.default_bucket:
            self.bucket_util.create_default_bucket(replica=self.num_replicas, ram_quota=self.bucket_size,
                                               compression_mode=self.compression_mode)
            
        if self.num_buckets:
            self.bucket_util.create_multiple_buckets(self.cluster.master, self.num_replicas, bucket_count=self.num_buckets, bucket_type=Bucket.bucket_type.EPHEMERAL, eviction_policy = Bucket.bucket_eviction_policy.NO_EVICTION)
         
        if self.standard_buckets > 1:
            self.bucket_util.create_standard_buckets(self.cluster.master, self.standard_buckets, bucket_size=100)
           
        time.sleep(20)

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
        self.drift_ahead = self.input.param("drift_ahead", False)
        self.drift_behind = self.input.param("drift_behind", False)
        gen_create = self.get_doc_generator(0, self.num_items)
        self.op_type = self.input.param("op_type", 'create')
        
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
        
        self.log.info("going to create a task")
        task = self.task.async_load_gen_docs_atomicity(self.cluster, self.bucket_util.buckets,
                                             gen_create, self.op_type , exp=0,
                                             batch_size=10,
                                             process_concurrency=8,
                                             replicate_to=self.replicate_to,
                                             persist_to=self.persist_to, timeout_secs=self.sdk_timeout,
                                             retries=self.sdk_retries,update_count=self.update_count, transaction_timeout=self.transaction_timeout, 
                                             commit=self.transaction_commit,durability=self.durability_level,sync=self.sync)
        self.log.info("going to execute the task")
        self.task.jython_task_manager.get_task_result(task)
        
        if self.op_type == "time_out": 
            self.sleep(90, "sleep for 90 seconds so that the staged docs will be cleared")
            task = self.task.async_load_gen_docs_atomicity(self.cluster, self.bucket_util.buckets,
                                             gen_create, "create" , exp=0,
                                             batch_size=10,
                                             process_concurrency=8,
                                             replicate_to=self.replicate_to,
                                             persist_to=self.persist_to, timeout_secs=self.sdk_timeout,
                                             retries=self.sdk_retries,update_count=self.update_count, transaction_timeout=200, 
                                             commit=self.transaction_commit,durability=self.durability_level,sync=self.sync)
            self.task_manager.get_task_result(task)

    def test_large_doc_size_commit(self):
        gen_create = self.generate_docs_bigdata(docs_per_day=self.num_items, document_size=self.doc_size)
        self.log.info("going to create a task")
        task = self.task.async_load_gen_docs_atomicity(self.cluster, self.bucket_util.buckets,
                                             gen_create, "create" , exp=0,
                                             batch_size=10,
                                             process_concurrency=self.process_concurrency,
                                             replicate_to=self.replicate_to,
                                             persist_to=self.persist_to, timeout_secs=self.sdk_timeout,
                                             retries=self.sdk_retries,transaction_timeout=self.transaction_timeout, 
                                             commit=self.transaction_commit,durability=self.durability_level,sync=self.sync)
        self.log.info("going to execute the task")
        self.task.jython_task_manager.get_task_result(task)
        

        

        
  

                

            
        
        
            
        
                


              
                
        
            

