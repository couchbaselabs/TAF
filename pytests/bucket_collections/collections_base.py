from basetestcase import BaseTestCase
from cb_tools.cb_cli import CbCli
from couchbase_helper.documentgenerator import doc_generator
from couchbase_helper.durability_helper import DurabilityHelper
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection


class CollectionBase(BaseTestCase):
    def setUp(self):
        super(CollectionBase, self).setUp()

        self.key = 'test_collection'.rjust(self.key_size, '0')
        self.simulate_error = self.input.param("simulate_error", None)
        self.error_type = self.input.param("error_type", "memory")
        self.doc_ops = self.input.param("doc_ops", None)
        self.skip_init_load = self.input.param("skip_init_load", False)
        self.crud_batch_size = 100
        self.num_nodes_affected = 1
        if self.num_replicas > 1:
            self.num_nodes_affected = 2

        if self.doc_ops:
            self.doc_ops = self.doc_ops.split(';')

        self.durability_helper = DurabilityHelper(
            self.log, len(self.cluster.nodes_in_cluster),
            self.durability_level)

        # Enable Collections using dev-preview options from cb-cli
        for server in self.cluster.servers:
            shell_conn = RemoteMachineShellConnection(server)
            cb_cli = CbCli(shell_conn)
            cb_cli.enable_dp()
            shell_conn.disconnect()

        # Initialize cluster using given nodes
        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend([self.cluster.master]+nodes_init)

        # Disable auto-failover to avaid failover of nodes
        status = RestConnection(self.cluster.master) \
            .update_autofailover_settings(False, 120, False)
        self.assertTrue(status, msg="Failure during disabling auto-failover")

        # Create default bucket and add rbac user
        self.bucket_util.create_default_bucket(
            replica=self.num_replicas, compression_mode=self.compression_mode,
            bucket_type=self.bucket_type, storage=self.bucket_storage)
        self.bucket_util.add_rbac_user()

        self.cluster_util.print_cluster_stats()
        self.bucket = self.bucket_util.buckets[0]
        if not self.skip_init_load:
            if self.target_vbucket and type(self.target_vbucket) is not list:
                self.target_vbucket = [self.target_vbucket]

            self.log.info("Creating doc_generator..")
            doc_create = doc_generator(
                self.key,
                0,
                self.num_items,
                doc_size=self.doc_size,
                doc_type=self.doc_type,
                target_vbucket=self.target_vbucket,
                vbuckets=self.cluster_util.vbuckets)
            self.log.info("doc_generator created")

            self.log.info("Loading {0} items into bucket"
                          .format(self.num_items))
            task = self.task.async_load_gen_docs(
                self.cluster, self.bucket, doc_create, "create", 0,
                batch_size=10, process_concurrency=8,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout)
            self.task.jython_task_manager.get_task_result(task)

            # Verify initial doc load count
            self.bucket_util._wait_for_stats_all_buckets()
            self.bucket_util.verify_stats_all_buckets(self.num_items)

        self.bucket_util.print_bucket_stats()
        self.log.info("=== CollectionBase setup complete ===")
