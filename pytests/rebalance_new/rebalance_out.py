from rebalance_base import RebalanceBaseTest
from couchbase_helper.documentgenerator import BlobGenerator, DocumentGenerator

class RebalanceOutTests(RebalanceBaseTest):
    def setUp(self):
        super(RebalanceOutTests, self).setUp()

    def tearDown(self):
        super(RebalanceOutTests, self).tearDown()

    def test_rebalance_out_with_ops(self):
        age = range(5)
        first = ['james', 'sharon']
        template = '{{ "age": {0}, "first_name": "{1}" }}'
        gen_create = DocumentGenerator('test_docs', template, age, first, start=self.num_items,
                                       end=self.num_items * 2)
        gen_delete = DocumentGenerator('test_docs', template, age, first, start=self.num_items / 2, end=self.num_items)
        servs_out = [self.cluster.servers[self.num_servers - i - 1] for i in range(self.nodes_out)]
        tasks = []
        task = self.task.async_rebalance(self.cluster.servers[:self.nodes_init], [], servs_out)
        tasks.append(task)
        for bucket in self.bucket_util.buckets:
            if (self.doc_ops is not None):
                if ("update" in self.doc_ops):
                    tasks.append(
                        self.task.async_load_gen_docs(self.cluster, bucket, gen_create, "update", 0, batch_size=10))
                if ("create" in self.doc_ops):
                    tasks.append(self.task.async_load_gen_docs(self.cluster, bucket, gen_create, "create", 0,
                                                               batch_size=10, process_concurrency=8))
                if ("delete" in self.doc_ops):
                    tasks.append(
                        self.task.async_load_gen_docs(self.cluster, bucket, gen_delete, "delete", 0, batch_size=10))
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)
        self.cluster.nodes_in_cluster = list(set(self.cluster.nodes_in_cluster) - set(servs_out))
        tasks = []
        for bucket in self.bucket_util.buckets:
            if (self.doc_ops is not None):
                if ("update" in self.doc_ops):
                    tasks.append(self.task.async_validate_docs(self.cluster, bucket, gen_create, "update", 0,
                                                               batch_size=10))
                if ("create" in self.doc_ops):
                    tasks.append(
                        self.task.async_validate_docs(self.cluster, bucket, gen_create, "create", 0, batch_size=10,
                                                      process_concurrency=8))
                if ("delete" in self.doc_ops):
                    tasks.append(
                        self.task.async_validate_docs(self.cluster, bucket, gen_delete, "delete", 0, batch_size=10))
        for task in tasks:
            self.task.jython_task_manager.get_task_result(task)
        self.bucket_util.verify_stats_all_buckets(self.num_items * 2)