from rebalance_base import RebalanceBaseTest
from couchbase_helper.documentgenerator import BlobGenerator, DocumentGenerator


class RebalanceInTests(RebalanceBaseTest):
    
    def setUp(self):
        super(RebalanceInTests, self).setUp()
        
    def tearDown(self):
        super(RebalanceInTests, self).tearDown()
    
    def test_rebalance_in_with_ops(self):
        age = range(5)
        first = ['james', 'sharon']
        body = [''.rjust(self.doc_size - 10, 'a')]
        template = '{{ "age": {0}, "first_name": "{1}", "body": "{2}"}}'
        gen_create = DocumentGenerator(self.key, template, age, first, body, start=self.num_items,
                                       end=self.num_items * 2)
        gen_delete = DocumentGenerator(self.key, template, age, first, body, start=self.num_items / 2, end=self.num_items)
        servs_in = [self.cluster.servers[i + self.nodes_init] for i in range(self.nodes_in)]
        tasks = []
        task = self.task.async_rebalance(self.cluster.servers[:self.nodes_init], servs_in, [])
        tasks.append(task)
        for bucket in self.bucket_util.buckets:
            if(self.doc_ops is not None):
                if("update" in self.doc_ops):
                    tasks.append(self.task.async_load_gen_docs(self.cluster, bucket, gen_create, "update", 0, batch_size=10))
                if("create" in self.doc_ops):
                    tasks.append(self.task.async_load_gen_docs(self.cluster, bucket, gen_create, "create", 0,
                                                               batch_size=10, process_concurrency=8))
                if("delete" in self.doc_ops):
                    tasks.append(self.task.async_load_gen_docs(self.cluster, bucket, gen_delete, "delete", 0, batch_size=10))
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)
        self.cluster.nodes_in_cluster.extend(servs_in)
        tasks = []
        for bucket in self.bucket_util.buckets:
            if (self.doc_ops is not None):
                if ("update" in self.doc_ops):
                    tasks.append(self.task.async_validate_docs(self.cluster, bucket, gen_create, "update", 0,
                                                               batch_size=10))
                if ("create" in self.doc_ops):
                    tasks.append(self.task.async_validate_docs(self.cluster, bucket, gen_create, "create", 0, batch_size=10,
                                                               process_concurrency=8))
                if ("delete" in self.doc_ops):
                    tasks.append(self.task.async_validate_docs(self.cluster, bucket, gen_delete, "delete", 0, batch_size=10))
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)
        self.bucket_util.verify_stats_all_buckets(self.num_items * 2)