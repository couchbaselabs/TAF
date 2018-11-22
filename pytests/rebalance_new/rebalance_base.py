from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import BlobGenerator, DocumentGenerator


class RebalanceBaseTest(BaseTestCase):
    
    def setUp(self):
        super(RebalanceBaseTest, self).setUp()
        self.doc_ops = self.input.param("doc_ops", "create")
        nodes_init = self.cluster.servers[1:self.nodes_init] if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend([self.cluster.master] + nodes_init)
        self.bucket_util.create_default_bucket()
        self.bucket_util.add_rbac_user()
        age = range(5)
        first = ['james', 'sharon']
        template = '{{ "age": {0}, "first_name": "{1}" }}'
        gen_create = DocumentGenerator('test_docs', template, age, first, start=0, end=self.num_items)
        #gen_create = BlobGenerator('mike', 'mike-', self.value_size, start=self.num_items + 1, end=self.num_items * 3 / 2)
        for bucket in self.bucket_util.buckets:
            task = self.task.async_load_gen_docs(self.cluster, bucket, gen_create, "create", 0, batch_size=10,
                                                 process_concurrency=8)
            self.task.jython_task_manager.get_task_result(task)
            print task.__str__()
        
    def tearDown(self):
        super(RebalanceBaseTest, self).tearDown()
    
    