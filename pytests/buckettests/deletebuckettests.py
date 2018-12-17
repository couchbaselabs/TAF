import time
from basetestcase import BaseTestCase
from membase.api.rest_client import RestConnection
from bucket_utils.Bucket import Bucket
from remote.remote_util import RemoteMachineShellConnection


class DeleteBucketTests(BaseTestCase):

    def setUp(self):
        super(DeleteBucketTests, self).setUp()
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
        super(DeleteBucketTests, self).tearDown()

    def wait_for_data_files_deletion(self,
                                     bucket,
                                     remote_connection,
                                     rest,
                                     timeout_in_seconds=120):
        self.log.info('waiting for bucket data files deletion from the disk ....')
        start = time.time()
        while (time.time() - start) <= timeout_in_seconds:
            if self.verify_data_files_deletion(bucket, remote_connection, rest):
                return True
            else:
                data_file = '{0}-data'.format(bucket)
                self.log.info("still waiting for deletion of {0} ...".format(data_file))
                time.sleep(2)
        return False

    def verify_data_files_deletion(self,
                                   bucket,
                                   remote_connection,
                                   rest):
        node = rest.get_nodes_self()
        for item in node.storage:
            #get the path
            data_file = '{0}-data'.format(bucket)
            if remote_connection.file_exists(item.path, data_file):
                return False
        return True

    def test_default_moxi(self):
        name = "default"
        replicas = [0, 1, 2, 3]
        rest = RestConnection(self.cluster.master)
        remote = RemoteMachineShellConnection(self.cluster.master)
        for replicaNumber in replicas:
            bucket = Bucket({"name": name, "replicaNumber": replicaNumber})
            self.bucket_util.create_bucket(bucket)
            msg = 'create_bucket succeeded but bucket {0} does not exist'.format(name)
            self.assertTrue(self.bucket_util.wait_for_bucket_creation(bucket, rest), msg)
            self.bucket_util.delete_bucket(self.cluster.master, bucket.name)
            msg = 'bucket "{0}" was not deleted even after waiting for two minutes'.format(name)
            self.assertTrue(self.bucket_util.wait_for_bucket_deletion(bucket, rest), msg)
            msg = 'bucket {0} data files are not deleted after bucket deleted from membase'.format(name)
            self.assertTrue(
                self.wait_for_data_files_deletion(name,
                                                  remote_connection=remote,
                                                  rest=rest, timeout_in_seconds=20), msg=msg)
