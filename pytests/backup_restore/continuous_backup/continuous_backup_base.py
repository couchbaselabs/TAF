import uuid

from basetestcase import ClusterSetup
from couchbase_helper.documentgenerator import doc_generator
from cb_tools.cbbackupmgr import CbBackupMgr
from shell_util.remote_connection import RemoteMachineShellConnection
from cb_constants import DocLoading
from nfs_utils.nfs_utils import NfsUtil
from TestInput import TestInputServer
from testconstants import PITR_NFS_SERVER


"""
NFS setup requirements:
- Server: /data directory exported with appropriate permissions
- Client: Mount NFS export at /mnt/nfs_data
- Validation: Ensure server export and client mount are working
- Cleanup: Unique subdirectory created under mount point and removed after test
"""

class ContinuousBackupBase(ClusterSetup):
    def setUp(self):
        super(ContinuousBackupBase, self).setUp()
        self.key = "test_backup_docs"
        self.num_docs = self.input.param("num_docs", 100000)

        self.shell = RemoteMachineShellConnection(self.cluster.master)

        self.nfs_test = self.input.param("nfs_test", False)
        if self.nfs_test:

            self.nfs_server_ip = PITR_NFS_SERVER
            self.nfs_server = TestInputServer()
            self.nfs_server.ip = self.nfs_server_ip
            self.nfs_server.ssh_username = "root"
            self.nfs_server.ssh_password = "couchbase"

            self.nfs_util = NfsUtil()

            self.log.info(f"Validating NFS server at {self.nfs_server.ip}")
            self.nfs_util.validate_nfs_server(self.nfs_server)

            for node in self.servers:
                self.log.info(f"Validating NFS client at {node.ip} connected to server {self.nfs_server.ip}")
                try:
                    self.nfs_util.validate_nfs_client(node, self.nfs_server)
                except Exception as e:
                    self.log.warning(f"NFS client validation failed: {e}. Attempting to set up NFS client.")
                    self.nfs_util.setup_nfs_client(node, self.nfs_server.ip, "/data", "/mnt/nfs_data")
                    self.nfs_util.validate_nfs_client(node, self.nfs_server)

            # Create continuous backup folder on NFS server
            self.continuous_backup_location = f"/mnt/nfs_data/test_{uuid.uuid4()}"
            output, error = self.shell.execute_command(f"mkdir -p {self.continuous_backup_location}")
            if error:
                self.fail("Error creating continuous backup folder: %s" % error)
            else:
                self.log.info("Created continuous backup folder: %s" % self.continuous_backup_location)
                # Set permissions for couchbase user
                output, error = self.shell.execute_command(f"chmod 777 {self.continuous_backup_location}")
                if error:
                    self.fail("Error setting permissions on continuous backup folder: %s" % error)
                else:
                    self.log.info("Set permissions on continuous backup folder")

        # Continuous backup parameters
        self.bucket_name = self.input.param("bucket_name", "cb_cont_bkp_bucket")
        self.continuous_backup_interval = self.input.param("continuous_backup_interval", 5)
        self.continuous_backup_enabled = self.input.param("continuous_backup_enabled", True)

        # Create bucket
        self.bucket_util.create_default_bucket(
            self.cluster,
            bucket_name=self.bucket_name)

        self.bucket = self.cluster.buckets[0]
        self.bucket_util.add_rbac_user(self.cluster.master)

        # Create backup manager instance
        self.backup_mgr = CbBackupMgr(self.shell,
                                      username=self.cluster.master.rest_username,
                                      password=self.cluster.master.rest_password)

    def tearDown(self):
        # Clean up continuous backup folder
        if self.nfs_test:
            try:
                self.log.info("Removing continuous backup folder: %s" % self.continuous_backup_location)
                output, error = self.shell.execute_command(f"rm -rf {self.continuous_backup_location}")
                if error:
                    self.log.warning("Error removing continuous backup folder: %s" % error)
            except Exception as e:
                self.log.warning("Exception during cleanup: %s" % str(e))

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
