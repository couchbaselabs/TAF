import json
from basetestcase import BaseTestCase
from custom_exceptions.exception import BucketCreationException
from membase.api.rest_client import RestConnection
from BucketLib.bucket import Bucket
from platform_constants.os_constants import Linux
from remote.remote_util import RemoteMachineShellConnection


class CreateBucketTests(BaseTestCase):
    def setUp(self):
        super(CreateBucketTests, self).setUp()
        self.nodes_init = self.input.param("nodes_init", 1)
        self.nodes_in = self.input.param("nodes_in", 1)
        self.nodes_out = self.input.param("nodes_out", 1)
        self.doc_ops = self.input.param("doc_ops", "create")
        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        self.task.rebalance(self.cluster, nodes_init, [])
        self.bucket_util.add_rbac_user(self.cluster.master)

    def tearDown(self):
        super(CreateBucketTests, self).tearDown()

    def test_two_replica(self):
        name = 'default'
        replica_number = 2
        bucket = Bucket({"name": name, "replicaNumber": replica_number})
        self.bucket_util.create_bucket(self.cluster, bucket)
        msg = 'create_bucket succeeded but bucket %s does not exist' % name
        self.assertTrue(
            self.bucket_util.wait_for_bucket_creation(self.cluster, bucket),
            msg)

    def test_valid_length(self):
        name_len = self.input.param('name_length', 100)
        name = 'a' * name_len
        replica_number = 1
        bucket = Bucket({"name": name, "replicaNumber": replica_number})
        msg = 'create_bucket succeeded but bucket %s does not exist' % name
        try:
            self.bucket_util.create_bucket(self.cluster, bucket)
            self.assertTrue(
                self.bucket_util.wait_for_bucket_creation(self.cluster,
                                                          bucket),
                msg)
        except BucketCreationException as ex:
            self.log.error(ex)
            self.fail('could not create bucket with valid length')

    def test_audit_logging_for_create_bucket(self):
        rest = RestConnection(self.cluster.master)
        logfilePath = Linux.COUCHBASE_LOGS_PATH + "/audit.log"
        rest.setAuditSettings()
        self.remote_shell = RemoteMachineShellConnection(self.cluster.master)
        bucket_name_value = self.bucket_util.get_random_name()
        self.bucket_util.create_default_bucket(
            self.cluster,
            bucket_type=self.bucket_type,
            replica=self.num_replicas,
            storage=self.bucket_storage,
            eviction_policy=self.bucket_eviction_policy,
            bucket_durability=self.bucket_durability_level,
            bucket_name=bucket_name_value)

        content_value = self.remote_shell.execute_command(
            "cat " + logfile_path + " | grep 'Bucket was created'")

        json_value = json.loads(content_value[0][0])
        self.assertEqual(json_value["bucket_name"], bucket_name_value,
                         "Value are not equal")
        self.remote_shell.disconnect()
