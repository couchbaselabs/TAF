import json
from basetestcase import BaseTestCase
from membase.api.exception import BucketCreationException
from membase.api.rest_client import RestConnection
from BucketLib.bucket import Bucket
from remote.remote_util import RemoteMachineShellConnection
from testconstants import LINUX_COUCHBASE_LOGS_PATH


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
        self.assertTrue(self.bucket_util.wait_for_bucket_creation(bucket), msg)

    def test_two_replica(self):
        bucket_name = 'default'
        rest = RestConnection(self.cluster.master)
        replicaNumber = 2
        bucket = Bucket({"name": bucket_name, "replicaNumber": replicaNumber})
        self.bucket_util.create_bucket(bucket)
        msg = 'create_bucket succeeded but bucket {0} does not exist'.format(bucket_name)
        self.assertTrue(self.bucket_util.wait_for_bucket_creation(bucket), msg)

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
            self.assertTrue(self.bucket_util.wait_for_bucket_creation(bucket), msg)
        except BucketCreationException as ex:
            self.log.error(ex)
            self.fail('could not create bucket with valid length')

    def test_audit_logging_for_create_bucket(self):
        rest = RestConnection(self.cluster.master)
        logfilePath = LINUX_COUCHBASE_LOGS_PATH + "/audit.log"
        rest.setAuditSettings()
        self.remote_shell = RemoteMachineShellConnection(self.cluster.master)
        bucketNameValue = self.bucket_util.get_random_name()
        self.bucket_util.create_default_bucket(
            bucket_type=self.bucket_type,
            replica=self.num_replicas,
            storage=self.bucket_storage,
            eviction_policy=self.bucket_eviction_policy,
            bucket_durability=self.bucket_durability_level,
            bucket_name=bucketNameValue)

        contentValue = self.remote_shell.execute_command(
            "cat " + logfilePath + " | grep 'Bucket was created'")

        jsonValue = json.loads(contentValue[0][0])
        self.assertEqual(jsonValue["bucket_name"], bucketNameValue, "Value are not equal")
        self.remote_shell.disconnect()