from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import BlobGenerator, DocumentGenerator


class RebalanceBaseTest(BaseTestCase):
    
    def setUp(self):
        super(RebalanceBaseTest, self).setUp()
        self.doc_ops = self.input.param("doc_ops", "create")
        self.doc_size = self.input.param("doc_size", 10)
        self.key_size = self.input.param("key_size", 0)
        nodes_init = self.cluster.servers[1:self.nodes_init] if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend([self.cluster.master] + nodes_init)
        self.bucket_util.create_default_bucket()
        self.bucket_util.add_rbac_user()
        age = range(5)
        first = ['james', 'sharon']
        body = [''.rjust(self.doc_size - 10, 'a')]
        template = '{{ "age": {0}, "first_name": "{1}", "body": "{2}"}}'
        self.key = 'test_docs'.rjust(self.key_size, '0')
        gen_create = DocumentGenerator(self.key, template, age, first, body, start=0, end=self.num_items)
        #gen_create = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items + 1, end=self.num_items * 3 / 2)
        self.print_cluster_stat_task = self.cluster_util.async_print_cluster_stats()
        for bucket in self.bucket_util.buckets:
            print_ops_task = self.bucket_util.async_print_bucket_ops(bucket)
            task = self.task.async_load_gen_docs(self.cluster, bucket, gen_create, "create", 0, batch_size=10,
                                                 process_concurrency=8)
            self.task.jython_task_manager.get_task_result(task)
            print_ops_task.end_task()
            self.task_manager.get_task_result(print_ops_task)
    def tearDown(self):
        if hasattr(self, "print_cluster_stat_task") and self.print_cluster_stat_task:
            self.print_cluster_stat_task.end_task()
            self.task_manager.get_task_result(self.print_cluster_stat_task)
        self.cluster_util.print_cluster_stats()
        super(RebalanceBaseTest, self).tearDown()
    
    