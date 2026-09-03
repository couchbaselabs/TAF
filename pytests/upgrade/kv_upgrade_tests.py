import time

from BucketLib.bucket import TravelSample, BeerSample, GamesimSample, Bucket
from cb_constants import DocLoading
from cb_server_rest_util.buckets.buckets_api import BucketRestApi
from cb_tools.cbstats import Cbstats
from cluster_utils.cluster_ready_functions import CBCluster
from couchbase_helper.documentgenerator import doc_generator
from membase.api.rest_client import RestConnection
from platform_constants.os_constants import Linux
from sdk_client3 import SDKClient
from shell_util.remote_connection import RemoteMachineShellConnection
from upgrade.upgrade_base import UpgradeBase


class KVUpgradeTests(UpgradeBase):
    def setUp(self):
        super(KVUpgradeTests, self).setUp()
        self.log_setup_status("KVUpgradeTests", "started", "setup")
        self.nodes_upgrade = self.input.param("nodes_upgrade", 1)
        self.graceful = self.input.param("graceful", True)
        self.recovery_type = self.input.param("recovery_type", "delta")
        # Set by tests that leave the cluster mid-upgrade, so tearDown knows
        # to chase bucket deletes across every node
        self.delete_buckets_on_all_nodes = False
        self.log_setup_status("KVUpgradeTests", "completed", "setup")

    def tearDown(self):
        # getattr, not attribute access: a setUp that raises before the flag
        # is set still runs tearDown, and an AttributeError here would mask
        # the real setUp failure
        if getattr(self, "delete_buckets_on_all_nodes", False):
            self.__delete_buckets_on_all_nodes()
        super(KVUpgradeTests, self).tearDown()

    def __delete_buckets_on_all_nodes(self):
        """
        Retry the delete on every node — after a partial upgrade the master
        may not be the node that still answers for the bucket.
        """
        bucket_names = [b.name for b in
                        list(getattr(self.cluster, "buckets", []) or [])]
        nodes = list(getattr(self.cluster, "nodes_in_cluster", []) or [])
        self.log.info(
            f"Upgrade tearDown: buckets={bucket_names}, "
            f"nodes={[n.ip for n in nodes]}")
        for name in bucket_names:
            for attempt in range(6):
                done = False
                for node in nodes:
                    try:
                        status, content = BucketRestApi(node).delete_bucket(
                            name)
                        self.log.info(
                            f"Delete {name} on {node.ip} attempt "
                            f"{attempt}: status={status}, content={content}")
                        if status:
                            done = True
                            break
                    except Exception as e:
                        self.log.warning(
                            f"Delete {name} on {node.ip} raised: {e}")
                if done:
                    break
                time.sleep(10)

    def test_multiple_sample_bucket_failover_upgrade(self):
        '''
            1.Formed the cluster using the 3 nodes.
            2.Loaded all 3 sample buckets.
            3.Failed over node.
            4.Stopped Couchbase Service on node.
            5.Upgraded node to upgrade_version.
            6.Tried to perform recovery
            Ref - MB-53493

        '''
        self.assertTrue(len(self.cluster.nodes_in_cluster) > 1,
                        msg="Not enough nodes to failover and upgrade")

        self.assertTrue(self.nodes_upgrade <= len(self.cluster.nodes_in_cluster),
                        msg="The number of nodes specified for upgrade are more than number of nodes in cluster")

        #Loading Travel Sample Bucket
        travelSampleBucket = TravelSample()
        if float(self.upgrade_chain[0][:3]) < 7.0:
            travelSampleBucket.stats.expected_item_count = 31591
        load_success=self.bucket_util.load_sample_bucket(self.cluster, travelSampleBucket)
        self.assertTrue(load_success,
                        msg="Travel Sample Bucket could not be loaded")
        self.log.info("Travel Sample Bucket Loaded")

        #Loading Beer Sample Bucket
        load_success=self.bucket_util.load_sample_bucket(self.cluster, BeerSample())
        self.assertTrue(load_success,
                        msg = "Beer Sample Bucket could not be loaded")
        self.log.info("Beer Sample Bucket Loaded")

        #Loading Gamesim Sample Bucket
        load_success=self.bucket_util.load_sample_bucket(self.cluster, GamesimSample())
        self.assertTrue(load_success,
                        msg = "Gamesim Sample Bucket could not be loaded")
        self.log.info("Gamesim Sample Bucket Loaded")

        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)

        node_to_upgrades = self.cluster.nodes_in_cluster[-self.nodes_upgrade:]

        for upgrade_version in self.upgrade_chain[1:]:
            self.upgrade_version = upgrade_version
            for node_to_upgrade in node_to_upgrades:
                self.sleep(120, "Sleeping before starting the next failover upgrade")
                self.failover_recovery(node_to_upgrade=node_to_upgrade,
                                       recovery_type=self.recovery_type,
                                       graceful=self.graceful)
                self.cluster_util.print_cluster_stats(self.cluster)
                self.bucket_util.print_bucket_stats(self.cluster)

    def test_db_dump_with_empty_body_and_empty_xattr(self):
        """
        1. Create empty doc (with no body + no sys_xattr) with data_type=xattr
        2. Stop and perform offline upgrade to some version (broken)
        3. Start xdcr replication to other cluster

        Ref: MB-51373
        """

        # Chosing the latest build for upgrades
        # This is a very specific test case and chain upgrades are not required
        self.upgrade_version = self.upgrade_chain[-1]
        # Install Couchbase server on target_nodes
        self.upgrade_helper.new_install_version_on_all_nodes(
            nodes=self.cluster.servers[self.nodes_init:],
            version=self.upgrade_version)

        upgrade_cluster = self.input.param("upgrade_cluster", "source")
        key, val = "test_key", {"f": "value"}
        sub_doc = ["_key", "value"]
        bucket = self.cluster.buckets[0]
        key_vb = self.bucket_util.get_vbucket_num_for_key(key,
                                                          bucket.numVBuckets)
        in_node = self.cluster.servers[1]
        num_items = self.num_items

        # Install the initial version on the 2nd node as well
        # This was not done initially
        self.upgrade_helper.new_install_version_on_all_nodes(
            nodes=self.cluster.servers[1:2], version=self.upgrade_chain[0])

        client = SDKClient(self.cluster, bucket)

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

        hash_dump_cmd = \
            "%s -u %s -p %s localhost:%d raw \"_hash-dump %d\" | grep %s" \
            % (Linux.COUCHBASE_BIN_PATH + "cbstats",
               self.cluster.master.rest_username,
               self.cluster.master.rest_password,
               self.cluster.master.memcached_port, key_vb, key)

        # Loading docs until the target doc is evicted from memory
        while is_resident:
            doc_gen = doc_generator("docs", start_index, batch_size,
                                    key_size=100, doc_size=10240,
                                    vbuckets=bucket.numVBuckets,
                                    target_vbucket=[key_vb])
            while doc_gen.has_next():
                d_key, val = doc_gen.next()
                client.crud(DocLoading.Bucket.DocOps.CREATE, d_key, val)

            output, _ = shell.execute_command(hash_dump_cmd)
            if not output:
                is_resident = False
            start_index = doc_gen.itr
            num_items += batch_size

        # Close the shell connections
        shell.disconnect()

        result = self.task.rebalance(self.cluster, [in_node], [])
        self.assertTrue(result, "Rebalance_in failed")

        cbstat = Cbstats(self.cluster.master)
        replica_vbs = cbstat.vbucket_list(bucket.name, Bucket.vBucket.REPLICA)
        cbstat.disconnect()
        if key_vb not in replica_vbs:
            # Swap the nodes in-order to maintain the vbucket consistency
            in_node = self.cluster.servers[0]
            self.cluster.master = self.cluster.servers[1]

            cbstat = Cbstats(self.cluster.servers[1])
            replica_vbs = cbstat.vbucket_list(bucket.name,
                                              Bucket.vBucket.REPLICA)
            cbstat.disconnect()
        self.assertTrue(key_vb in replica_vbs, "vBucket is still active vb")

        client.crud(DocLoading.Bucket.SubDocOps.REMOVE, key, sub_doc[0],
                    xattr=True, access_deleted=True)
        client.close()

        # Rebalance out the new node
        result = self.task.rebalance(self.cluster, [], [in_node])
        self.assertTrue(result, "Rebalance_out failed")

        # Performing upgrade
        if upgrade_cluster == "source":
            self.log.info("Upgrading node: {}".format(self.cluster.master.ip))
            self.upgrade_function[self.upgrade_type](self.cluster.master)
        elif upgrade_cluster == "remote":
            self.log.info("Upgrading node: {}".format(in_node.ip))
            self.upgrade_helper.new_install_version_on_all_nodes(
                nodes=[in_node], version=self.upgrade_version)


        self.log.info("Starting XDCR replication")
        xdcr_cluster = CBCluster("C2", servers=[in_node])
        xdcr_cluster.nodes_in_cluster = [in_node]
        RestConnection(in_node).init_node()
        self.bucket_util.create_default_bucket(
            cluster=xdcr_cluster, ram_quota=self.bucket_size,
            storage=self.bucket_storage,
            replica=0,
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

    def test_rate_limit_across_upgrade(self):
        """
        Rate-limit edits across one upgrade timeline:
          1. mixed mode (a single node upgraded) - the edit must be rejected
          2. uniform 8.5+ - the edit must succeed and both values persist

        The two phases are one ordered sequence, not independent scenarios:
        reaching mixed mode already requires the upgrade that phase 2 needs.
        Phase 1 records through log_failure instead of asserting, so a
        mixed-mode regression does not hide the post-upgrade behaviour.
        """

        def _set_rate_limit(master_node):
            edit_params = {
                Bucket.throttleReserved: self.bucket_throttle_reserved,
                Bucket.throttleHardLimit: self.bucket_throttle_hard_limit,
            }
            return BucketRestApi(master_node).edit_bucket(self.bucket.name,
                                                          edit_params)

        self.delete_buckets_on_all_nodes = True
        self.upgrade_version = self.upgrade_chain[-1]

        # Phase 1 - upgrade one node to put the cluster into mixed mode
        node_to_upgrade = self.fetch_node_to_upgrade()
        self.assertIsNotNone(node_to_upgrade, "No node found to upgrade")
        self.log.info(f"Upgrading {node_to_upgrade.ip} to enter mixed mode")
        self.upgrade_function[self.upgrade_type](node_to_upgrade)

        # fetch_node_to_upgrade() re-runs find_orchestrator, so cluster.master
        # is a node still in the cluster. online_swap rebalances the node it
        # upgrades *out* and leaves cluster.nodes_in_cluster untouched, so
        # neither that list nor a cached master survives an upgrade step.
        node_to_upgrade = self.fetch_node_to_upgrade()
        self.assertIsNotNone(
            node_to_upgrade,
            "Every node reports the target version; mixed mode never reached")
        status, content = _set_rate_limit(self.cluster.master)
        self.log.info(f"Mixed-mode rate-limit edit: status={status}, "
                      f"content={content}")
        if status:
            self.log_failure(
                f"Rate limit edit should be rejected in mixed-mode: {content}")

        # Phase 2 - finish the upgrade, then the same edit must stick
        while node_to_upgrade is not None:
            self.log.info(f"Upgrading node {node_to_upgrade.ip}")
            self.upgrade_function[self.upgrade_type](node_to_upgrade)
            node_to_upgrade = self.fetch_node_to_upgrade()

        status, content = _set_rate_limit(self.cluster.master)
        self.log.info(f"Post-upgrade rate-limit edit: status={status}, "
                      f"content={content}")
        if not status:
            self.log_failure(
                f"Rate limit edit should succeed post-upgrade: {content}")
        else:
            _, buckets = BucketRestApi(
                self.cluster.master).get_bucket_info()
            bucket_info = next(b for b in buckets
                               if b['name'] == self.bucket.name)
            self.log.info(f"Bucket limits post-upgrade: {bucket_info}")
            for field, expected in (
                    (Bucket.throttleReserved, self.bucket_throttle_reserved),
                    (Bucket.throttleHardLimit,
                     self.bucket_throttle_hard_limit)):
                if bucket_info.get(field) != expected:
                    self.log_failure(
                        f"{field} mismatch post-upgrade: expected "
                        f"{expected}, got {bucket_info.get(field)}")

        self.validate_test_failure()
