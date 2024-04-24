from basetestcase import ClusterSetup
from couchbase_helper.documentgenerator import DocumentGenerator


class RecreateBucketTests(ClusterSetup):
    def setUp(self):
        super(RecreateBucketTests, self).setUp()
        self.nodes_init = self.input.param("nodes_init", 1)
        self.nodes_in = self.input.param("nodes_in", 1)
        self.nodes_out = self.input.param("nodes_out", 1)
        self.doc_ops = self.input.param("doc_ops", "create")
        # self.bucket_util.create_default_bucket()

    def tearDown(self):
        super(RecreateBucketTests, self).tearDown()

    def _generate_load(self, bucket):
        age = range(5)
        first = ['james', 'sharon']
        template = '{{ "age": {0}, "first_name": "{1}" }}'
        gen_create = DocumentGenerator('test_docs', template, age, first,
                                       start=0, end=self.num_items,
                                       key_size=self.key_size,
                                       doc_size=self.doc_size,
                                       doc_type=self.doc_type)
        task = self.task.async_load_gen_docs(
            self.cluster, bucket, gen_create, "create", 0,
            batch_size=20, persist_to=self.persist_to,
            replicate_to=self.replicate_to,
            timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
            load_using=self.load_docs_using)
        self.task.jython_task_manager.get_task_result(task)

    def _validate_load(self, bucket):
        age = range(5)
        first = ['james', 'sharon']
        template = '{{ "age": {0}, "first_name": "{1}" }}'
        gen_create = DocumentGenerator('test_docs', template, age, first,
                                       start=0,
                                       end=self.num_items,
                                       key_size=self.key_size,
                                       doc_size=self.doc_size,
                                       doc_type=self.doc_type)
        task = self.task.async_validate_docs(self.cluster, bucket, gen_create, "create", 0,
                                             batch_size=10, process_concurrency=8)
        self.task.jython_task_manager.get_task_result(task)
