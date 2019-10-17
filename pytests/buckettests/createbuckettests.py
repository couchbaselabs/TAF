from basetestcase import BaseTestCase
from membase.api.exception import BucketCreationException
from membase.api.rest_client import RestConnection
from BucketLib.bucket import Bucket


class CreateBucketTests(BaseTestCase):
    def setUp(self):
        super(CreateBucketTests, self).setUp()
        self.nodes_init = self.input.param("nodes_init", 1)
        self.nodes_in = self.input.param("nodes_in", 1)
        self.nodes_out = self.input.param("nodes_out", 1)
        self.doc_ops = self.input.param("doc_ops", "create")
        nodes_init = self.cluster.servers[1:self.nodes_init] if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.append(self.cluster.master)
        # self.bucket_util.create_default_bucket()
        self.bucket_util.add_rbac_user()

    def tearDown(self):
        super(CreateBucketTests, self).tearDown()

    def test_default_moxi(self):
        bucket_name = 'default'
        rest = RestConnection(self.cluster.master)
        replicaNumber = 1
        bucket = Bucket({"name": bucket_name, "replicaNumber": replicaNumber})
        self.bucket_util.create_bucket(bucket)
        msg = 'create_bucket succeeded but bucket {0} does not exist'.format(bucket_name)
        self.assertTrue(self.bucket_util.wait_for_bucket_creation(bucket, rest), msg)

    def test_two_replica(self):
        bucket_name = 'default'
        rest = RestConnection(self.cluster.master)
        replicaNumber = 2
        bucket = Bucket({"name": bucket_name, "replicaNumber": replicaNumber})
        self.bucket_util.create_bucket(bucket)
        msg = 'create_bucket succeeded but bucket {0} does not exist'.format(bucket_name)
        self.assertTrue(self.bucket_util.wait_for_bucket_creation(bucket, rest), msg)

    def test_valid_length(self):
        max_len = 100
        name_len = self.input.param('name_length', 100)
        name = 'a' * name_len
        rest = RestConnection(self.cluster.master)
        replicaNumber = 1
        bucket = Bucket({"name": name, "replicaNumber": replicaNumber})
        try:
            self.bucket_util.create_bucket(bucket)
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(name)
            self.assertTrue(self.bucket_util.wait_for_bucket_creation(bucket, rest), msg)
        except BucketCreationException as ex:
            self.log.error(ex)
            self.fail('could not create bucket with valid length')
