from basetestcase import BaseTestCase
from membase.api.rest_client import RestConnection
from bucket_utils.bucket_ready_functions import Bucket
from couchbase_helper.documentgenerator import DocumentGenerator

class RecreateBucketTests(BaseTestCase):

    def setUp(self):
        super(RecreateBucketTests, self).setUp()
        self.nodes_init = self.input.param("nodes_init", 1)
        self.nodes_in = self.input.param("nodes_in", 1)
        self.nodes_out = self.input.param("nodes_out", 1)
        self.doc_ops = self.input.param("doc_ops", "create")
        nodes_init = self.cluster.servers[1:self.nodes_init] if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.append(self.cluster.master)
        #self.bucket_util.create_default_bucket()
        self.bucket_util.add_rbac_user()

    def tearDown(self):
        super(RecreateBucketTests, self).tearDown()

    def _generate_load(self, bucket):
        age = range(5)
        first = ['james', 'sharon']
        template = '{{ "age": {0}, "first_name": "{1}" }}'
        gen_create = DocumentGenerator('test_docs', template, age, first, start=0, end=self.num_items)
        task = self.task.async_load_gen_docs(self.cluster, bucket, gen_create, "create", 0,
                                                           batch_size=10, process_concurrency=8)
        self.task.jython_task_manager.get_task_result(task)

    def _validate_load(self, bucket):
        age = range(5)
        first = ['james', 'sharon']
        template = '{{ "age": {0}, "first_name": "{1}" }}'
        gen_create = DocumentGenerator('test_docs', template, age, first, start=0, end=self.num_items)
        task = self.task.async_validate_docs(self.cluster, bucket, gen_create, "create", 0,
                                             batch_size=10, process_concurrency=8)
        self.task.jython_task_manager.get_task_result(task)

    def test_default_moxi(self):
        bucket_name = 'default'
        rest = RestConnection(self.cluster.master)
        replicaNumber = 1
        bucket = Bucket({"name": bucket_name, "replicaNumber": replicaNumber})
        self.bucket_util.create_bucket(bucket)
        msg = 'create_bucket succeeded but bucket {0} does not exist'.format(bucket_name)
        self.assertTrue(self.bucket_util.wait_for_bucket_creation(bucket, rest), msg)
        self.sleep(5)
        #self.assertTrue(self.bucket_util.wait_for_memcached(self.cluster.master, bucket), "Wait_for_memcached failed")
        try:
            self._generate_load(bucket)
        except Exception as e:
            self.fail("unable to insert any key to memcached")
        try:
            self._validate_load(bucket)
        except Exception as e:
            self.fail("Not all values were stored successfully")
        self.bucket_util.delete_bucket(self.cluster.master, bucket)
        msg = 'bucket "{0}" was not deleted even after waiting for two minutes'.format(bucket_name)
        self.assertTrue(self.bucket_util.wait_for_bucket_deletion(bucket, rest), msg)
        self.bucket_util.create_bucket(bucket)
        msg = 'create_bucket succeeded but bucket {0} does not exist'.format(bucket_name)
        self.assertTrue(self.bucket_util.wait_for_bucket_creation(bucket, rest), msg)
        self.sleep(5)
        #self.assertTrue(self.bucket_util.wait_for_memcached(self.cluster.master, bucket), "Wait_for_memcached failed")
        # try:
        #     self._validate_load(bucket)
        #     self.fail("At least one key found in the bucket")
        # except BaseException as e:
        #     pass

