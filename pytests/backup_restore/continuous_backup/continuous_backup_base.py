from cb_tools.cbbackupmgr import CbBackupMgr
from pytests.bucket_collections.collections_base import CollectionBase
from shell_util.remote_connection import RemoteMachineShellConnection


"""
NFS setup requirements:
- Server: /data directory exported with appropriate permissions
- Client: Mount NFS export at /mnt/nfs_data
- Validation: Ensure server export and client mount are working
- Cleanup: Unique subdirectory created under mount point and removed after test
"""

class ContinuousBackupBase(CollectionBase):
    def setUp(self):
        super(ContinuousBackupBase, self).setUp()
        self.key = "test_backup_docs"
        self.num_docs = self.input.param("num_docs", 100000)

        self.shell = RemoteMachineShellConnection(self.cluster.master)

        self.bucket = self.cluster.buckets[0]
        self.bucket_name = self.bucket.name

        self.bucket_util.add_rbac_user(self.cluster.master)

        # Create backup manager instance
        self.backup_mgr = CbBackupMgr(self.shell,
                                      username=self.cluster.master.rest_username,
                                      password=self.cluster.master.rest_password)

    def tearDown(self):
        # Delete the continuous backup bucket if exists
        try:
            buckets = self.bucket_util.get_all_buckets(self.cluster)
            for bucket in buckets:
                if bucket.name == self.bucket_name:
                    self.log.info("Deleting bucket: %s" % bucket.name)
                    self.bucket_util.delete_bucket(self.cluster, bucket)
                    self.log.info("Bucket %s deleted successfully" % bucket.name)
                    break
        except Exception as e:
            self.fail("Exception during bucket deletion: %s" % str(e))
        finally:
            self.backup_mgr.disconnect()
            self.shell.disconnect()

        super(ContinuousBackupBase, self).tearDown()
