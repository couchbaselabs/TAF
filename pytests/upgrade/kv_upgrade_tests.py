from BucketLib.bucket import Bucket
from Cb_constants import DocLoading
from cb_tools.cbstats import Cbstats
from cluster_utils.cluster_ready_functions import CBCluster
from couchbase_helper.documentgenerator import doc_generator
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient
from upgrade.upgrade_base import UpgradeBase


class KVUpgradeTests(UpgradeBase):
    def setUp(self):
        super(KVUpgradeTests, self).setUp()

        # Install Couchbase server on target_nodes
        self.install_version_on_node(
            self.cluster.servers[self.nodes_init:],
            self.upgrade_version)

    def tearDown(self):
        super(KVUpgradeTests, self).tearDown()

    def test_db_dump_with_empty_body_and_empty_xattr(self):
        """
        1. Create empty doc (with no body + no sys_xattr) with data_type=xattr
        2. Stop and perform offline upgrade to some version (broken)
        3. Start xdcr replication to other cluster

        Ref: MB-51373
        """
        upgrade_cluster = self.input.param("upgrade_cluster", "source")
        key, val = "test_key", {"f": "value"}
        sub_doc = ["_key", "value"]
        key_vb = self.bucket_util.get_vbucket_num_for_key(key)
        bucket = self.cluster.buckets[0]
        in_node = self.cluster.servers[1]
        num_items = 0

        # Install the initial version on the 2nd node as well
        # (This is not done in the upgrade base part)
        self.install_version_on_node(self.cluster.servers[1:2],
                                     self.initial_version)

        client = SDKClient([self.cluster.master], bucket)

        self.log.info("Creating tombstone '%s' with sys-xattr" % key)
        # Create a document
        client.crud(DocLoading.Bucket.DocOps.CREATE, key, val)
        # Load sys-xattr for the document
        client.crud(DocLoading.Bucket.SubDocOps.INSERT,
                    key, sub_doc, xattr=True)
        # Wait for ep_queue_size to become Zero
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, [bucket])
        # Delete the document
        client.crud(DocLoading.Bucket.DocOps.DELETE, key)
        # Wait for ep_queue_size to become Zero
        self.bucket_util._wait_for_stats_all_buckets(self.cluster, [bucket])
        client.crud(DocLoading.Bucket.SubDocOps.LOOKUP, key, sub_doc[0],
                    xattr=True, access_deleted=True)

        self.log.info("Loading docs to make the tombstone doc as non-resident")
        is_resident = True
        start_index = 0
        batch_size = 1000
        shell = RemoteMachineShellConnection(self.cluster.master)
        cbstat = Cbstats(shell)

        hash_dump_cmd = \
            "%s -u %s -p %s localhost:%d raw \"_hash-dump %d\" | grep %s" \
            % (cbstat.cbstatCmd, cbstat.username, cbstat.password,
               self.cluster.master.memcached_port, key_vb, key)

        # Loading docs until the target doc is evicted from memory
        while is_resident:
            doc_gen = doc_generator("docs", start_index, batch_size,
                                    key_size=100, doc_size=10240,
                                    target_vbucket=[key_vb])
            while doc_gen.has_next():
                d_key, val = doc_gen.next()
                client.crud(DocLoading.Bucket.DocOps.CREATE, d_key, val)

            output, _ = shell.execute_command(hash_dump_cmd)
            if " X.. .D..Cm " in output[0]:
                is_resident = False
            start_index = doc_gen.key_counter
            num_items += batch_size

        result = self.task.rebalance([self.cluster.master], [in_node], [])
        self.assertTrue(result, "Rebalance_in failed")

        replica_vbs = cbstat.vbucket_list(bucket.name, Bucket.vBucket.REPLICA)
        self.assertTrue(key_vb in replica_vbs, "vBucket is still active vb")

        client.crud(DocLoading.Bucket.SubDocOps.REMOVE, key, sub_doc[0],
                    xattr=True, access_deleted=True)
        client.close()

        # Rebalance out the new node
        result = self.task.rebalance(self.cluster.servers[:2],
                                     to_add=[], to_remove=[in_node])
        self.assertTrue(result, "Rebalance_out failed")

        # Performing upgrade
        if upgrade_cluster == "source":
            self.log.info("Upgrading node: %s" % self.cluster.master.ip)
            self.upgrade_function[self.upgrade_type](self.cluster.master,
                                                     self.upgrade_version)
        elif upgrade_cluster == "remote":
            self.log.info("Upgrading node: %s" % self.cluster.servers[1].ip)
            self.install_version_on_node(self.cluster.servers[1:2],
                                         self.initial_version)

        self.log.info("Starting XDCR replication")
        xdcr_cluster = CBCluster("C2", servers=[in_node])
        RestConnection(in_node).init_node()
        self.bucket_util.create_default_bucket(
            cluster=xdcr_cluster, ram_quota=256, replica=0,
            wait_for_warmup=True)

        rest = RestConnection(self.cluster.master)
        rest.add_remote_cluster(xdcr_cluster.master.ip,
                                xdcr_cluster.master.port,
                                xdcr_cluster.master.rest_username,
                                xdcr_cluster.master.rest_password,
                                xdcr_cluster.master.ip)
        rest.start_replication("continuous",
                               self.cluster.buckets[0].name,
                               xdcr_cluster.master.ip,
                               toBucket=xdcr_cluster.buckets[0].name)
        try:
            self.log.info("Waiting for all items to get replicated")
            self.bucket_util.verify_stats_all_buckets(xdcr_cluster,
                                                      num_items, timeout=180)
        finally:
            self.log.info("Removing xdcr bucket and remote references")
            self.bucket_util.delete_all_buckets(xdcr_cluster)
            rest.remove_all_replications()
            rest.remove_all_remote_clusters()
