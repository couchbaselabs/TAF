from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import doc_generator




class basic_ops(BaseTestCase):
    def setUp(self):
        super(basic_ops, self).setUp()
        self.log.info("==========Started Load_Delete base setup========")
        
        nodes_init = self.cluster.servers[1:self.nodes_init] if self.nodes_init != 1 else []
        if nodes_init:
            self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend([self.cluster.master] + nodes_init)
        self.bucket_util.add_rbac_user()
        self.bucket_util.create_default_bucket(replica=self.num_replicas)
        self.sleep(20)
        
        self.key = 'test_docs'.rjust(self.key_size, '0')
        
        self.delete_items = 500
        self.log.info("==========Finished Load_Delete base setup========")
        
    def tearDown(self):
        super(basic_ops, self).tearDown()
        
    def normal_load_and_transactions(self):
        self.gen_create = doc_generator(self.key, 0, self.num_items, doc_size=self.doc_size,
                             doc_type=self.doc_type,
                             target_vbucket=self.target_vbucket,
                             vbuckets=self.vbuckets)
        self.rebalance_in = self.input.param("rabalance_in",True)
                             
        # Loading of 1M docs through normal loader
        self.log.info("Going to load 1M docs through normal load")
        self.buckets = self.bucket_util.get_all_buckets(self.cluster.master)
        for bucket in self.buckets:
            task = self.task.async_load_gen_docs(
                self.cluster,bucket,self.gen_create, "create", 0,
                batch_size=20,persist_to=self.persist_to, replicate_to=self.replicate_to)
            self.task.jython_task_manager.get_task_result(task)
        self.log.info("Loading of 1M documents complete")
        self.sleep(40, "Bringing the bucket state to stable")
        
        # Transaction operation on top of normal loader
        self.gen_create = doc_generator(self.key, self.num_items, self.num_items+1000, doc_size=self.doc_size,
                             doc_type=self.doc_type,
                             target_vbucket=self.target_vbucket,
                             vbuckets=self.vbuckets)
        self.log.info("Going to perform any Transaction Operation")
        self.op_type = self.input.param("op_type","create")
        self.gen_delete = doc_generator(self.key, 0,self.delete_items, doc_size=self.doc_size,
                             doc_type=self.doc_type,
                             target_vbucket=self.target_vbucket,
                             vbuckets=self.vbuckets)
        
        tasks = []
        if "update" in self.op_type:
            tasks.append(self.task.async_load_gen_docs_atomicity(self.cluster, self.bucket_util.buckets,
                                             self.gen_delete, "rebalance_only_update" , exp=0,
                                             batch_size=10,
                                             process_concurrency=8,
                                             replicate_to=self.replicate_to,
                                             persist_to=self.persist_to, timeout_secs=self.sdk_timeout,
                                             retries=self.sdk_retries,update_count=self.update_count, transaction_timeout=self.transaction_timeout, 
                                             commit=self.transaction_commit,durability=self.durability_level))
            
        if "create" in self.op_type:
            tasks.append(self.task.async_load_gen_docs_atomicity(self.cluster, self.bucket_util.buckets,
                                             self.gen_create, "create" , exp=0,
                                             batch_size=10,
                                             process_concurrency=8,
                                             replicate_to=self.replicate_to,
                                             persist_to=self.persist_to, timeout_secs=self.sdk_timeout,
                                             retries=self.sdk_retries,update_count=self.update_count, transaction_timeout=self.transaction_timeout, 
                                             commit=self.transaction_commit,durability=self.durability_level))
        if "delete" in self.op_type:
            tasks.append(self.task.async_load_gen_docs_atomicity(self.cluster, self.bucket_util.buckets,
                                             self.gen_delete, "rebalance_delete" , exp=0,
                                             batch_size=10,
                                             process_concurrency=8,
                                             replicate_to=self.replicate_to,
                                             persist_to=self.persist_to, timeout_secs=self.sdk_timeout,
                                             retries=self.sdk_retries,update_count=self.update_count, transaction_timeout=self.transaction_timeout, 
                                             commit=self.transaction_commit,durability=self.durability_level))
        
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)
        self.log.info("Transaction Operation Complete")
        
        if self.rebalance_in:
            servs_in = [self.cluster.servers[i + self.nodes_init] for i in range(self.nodes_in)]
            rebalance = self.task.async_rebalance(self.cluster.servers[:self.nodes_init], servs_in, [])
            self.task.jython_task_manager.get_task_result(rebalance)
            self.sleep(60)
            
        if not self.rebalance_in:
            servs_out = [self.cluster.servers[self.nodes_init - i - 1] for i in range(self.nodes_out)]
            rebalance = self.task.async_rebalance(self.cluster.servers[:self.nodes_init], [], servs_out)
            self.task.jython_task_manager.get_task_result(rebalance)
            self.sleep(60)
            
        
