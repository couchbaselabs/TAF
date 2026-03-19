import time
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

        self.bucket = self.cluster.buckets[0]
        self.bucket_name = self.bucket.name
        self.shell = RemoteMachineShellConnection(self.cluster.master)


    def tearDown(self):

        # Delete the shell connection if exists
        try:
            self.shell.disconnect()
        except Exception as e:
            self.log.error("Exception during removing shell: %s" % str(e))

        super(ContinuousBackupBase, self).tearDown()

    def _verify_doc_count(self, expected_count, bucket_name=None, timeout=300):
        if bucket_name is None:
            bucket_name = self.bucket.name
        self.log.info(f"Verifying document count for bucket '{bucket_name}'. Expected: {expected_count}")
        end_time = time.time() + timeout
        while time.time() < end_time:
            actual_items = self.bucket_util.get_buckets_item_count(self.cluster, bucket_name)
            if actual_items == expected_count:
                self.log.info(f"Document count for bucket '{bucket_name}' verified: {actual_items}")
                return
            self.log.info(f"Current doc counts for bucket '{bucket_name}'. Actual: {actual_items}, Expected: {expected_count}. Retrying in 10s...")
            self.sleep(10)
        self.fail(f"Document count mismatch for bucket '{bucket_name}'. Expected: {expected_count}, Actual: {actual_items}")
