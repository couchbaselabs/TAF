from basetestcase import BaseTestCase
from Jython_tasks.task_manager import TaskManager
from Jython_tasks.task import rebalanceTask, DocloaderTask, TestTask
from com.couchbase.client.java import *;

class RebalanceinJython(BaseTestCase):
    def setUp(self):
        super(RebalanceinJython, self).setUp()
        self.task_manager = TaskManager()
        self.load_gen_tasks = []
        
    def tearDown(self):
        self.task_manager.shutdown_task_manager()
        super(RebalanceinJython, self).tearDown()
    
    def start_load_gen(self, docs, bucket):
        cluster = CouchbaseCluster.create(self.servers[0].ip);
        cluster.authenticate("Administrator","password")
        bucket = cluster.openBucket(bucket);
        k = "rebalance"
        v = {"value": "asd"}
        docloaders=[]
        num_executors = 5
        total_num_executors = 5
        num_docs = docs/total_num_executors
        load_gen_task_name = "Loadgen"
        for i in xrange(total_num_executors):
            task_name = "{0}_{1}".format(load_gen_task_name, i)
            self.load_gen_tasks.append(DocloaderTask(bucket, num_docs, i*num_docs, k, v, task_name))
        for task in self.load_gen_tasks:
            self.task_manager.add_new_task(task)
        
    def finish_load_gen(self):
        for task in self.load_gen_tasks:
            print self.task_manager.get_task_result(task)
            
    def start_rebalance_nodes(self):
        servs_in = [self.servers[i + self.nodes_init] for i in range(self.nodes_in)]
        self.rebalance_task = rebalanceTask(self.servers[:self.nodes_init], to_add=servs_in, to_remove=[])
        self.task_manager.add_new_task(self.rebalance_task)
        
    def get_rebalance_result(self):
        return self.task_manager.get_task_result(self.rebalance_task)
    
    def test_rebalance_in(self):
        self.start_load_gen(100000, self.buckets[0].name)
        self.sleep(5)
        #test_Task = TestTask(30)
        #self.task_manager.add_new_task(test_Task)
        self.start_rebalance_nodes()
        print self.get_rebalance_result()
        #print self.task_manager.get_task_result(test_Task)
        self.finish_load_gen()