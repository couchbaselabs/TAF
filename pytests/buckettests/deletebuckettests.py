import json
import time
from basetestcase import ClusterSetup
from membase.api.rest_client import RestConnection
from platform_constants.os_constants import Linux
from remote.remote_util import RemoteMachineShellConnection


class DeleteBucketTests(ClusterSetup):

    def setUp(self):
        super(DeleteBucketTests, self).setUp()
        self.nodes_in = self.input.param("nodes_in", 1)
        self.nodes_out = self.input.param("nodes_out", 1)
        self.doc_ops = self.input.param("doc_ops", "create")

    def tearDown(self):
        super(DeleteBucketTests, self).tearDown()

    def wait_for_data_files_deletion(self, bucket, remote_connection, rest,
                                     timeout_in_seconds=120):
        self.log.info('waiting for bucket data files deletion from the disk')
        start = time.time()
        while (time.time() - start) <= timeout_in_seconds:
            if self.verify_data_files_deletion(bucket, remote_connection,
                                               rest):
                return True
            else:
                data_file = '{0}-data'.format(bucket)
                self.sleep(2, "Still waiting for %s deletion" % data_file)
        return False

    def verify_data_files_deletion(self, bucket, remote_connection, rest):
        self.log.info("Validating data file deletion for %s" % bucket)
        node = rest.get_nodes_self()
        for item in node.storage:
            # get the path
            data_file = '{0}-data'.format(bucket)
            if remote_connection.file_exists(item.path, data_file):
                return False
        return True

    def test_audit_logging_for_delete_bucket(self):
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

        self.bucket_util.delete_bucket(self.cluster, self.cluster.buckets[0])

        content_value = self.remote_shell.execute_command(
            "cat " + logfile_path + " | grep 'Bucket was deleted'")

        json_value = json.loads(content_value[0][0])
        self.assertEqual(json_value["bucket_name"], bucket_name_value,
                         "Value are not equal")
        self.remote_shell.disconnect()
