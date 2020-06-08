from random import choice

from bucket_collections.collections_base import CollectionBase
from cb_tools.cbepctl import Cbepctl
from cb_tools.cbstats import Cbstats
from collections_helper.collections_spec_constants import MetaCrudParams
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection


class RollbackTests(CollectionBase):
    def setUp(self):
        super(RollbackTests, self).setUp()
        self.num_rollbacks = self.input.param("num_rollbacks", 2)
        self.rollback_with_multiple_mutation = self.input.param(
            "rollback_with_multiple_mutation", False)
        self.bucket = self.bucket_util.buckets[0]

        # Disable auto-fail_over to avoid fail_over of nodes
        status = RestConnection(self.cluster.master) \
            .update_autofailover_settings(False, 120, False)
        self.assertTrue(status, msg="Failure during disabling auto-failover")

        # Used to calculate expected queue size of validation before rollnback
        self.total_rollback_items = 0
        self.kv_nodes = self.cluster_util.get_kv_nodes()

        # Open shell connections to kv nodes and create cbstat objects
        self.node_shells = dict()
        for node in self.kv_nodes:
            shell_conn = RemoteMachineShellConnection(node)
            self.node_shells[node] = dict()
            self.node_shells[node]["shell"] = shell_conn
            self.node_shells[node]["cbstat"] = Cbstats(shell_conn)

    def tearDown(self):
        # Close all shell_connections before cluster tearDown
        for node in self.node_shells.keys():
            self.node_shells[node]["shell"].disconnect()

        super(RollbackTests, self).tearDown()

    def get_vb_details_cbstats_for_all_nodes(self, stat_key):
        for _, node_dict in self.node_shells.items():
            node_dict[stat_key] = \
                node_dict["cbstat"].vbucket_details(self.bucket.name)

    def validate_seq_no_post_rollback(self, init_stat_key, post_stat_key,
                                      keys_to_verify):
        for node, n_dict in self.node_shells.items():
            for vb, vb_stat_dict in n_dict[post_stat_key].items():
                for stat in keys_to_verify:
                    if vb_stat_dict[stat] != n_dict[init_stat_key][vb][stat]:
                        self.log_failure("vBucket %s - %s stat mismatch. "
                                         "(current) %s != %s (prev)"
                                         % (vb, stat,
                                            vb_stat_dict[stat],
                                            n_dict[init_stat_key][vb][stat]))

    def load_docs(self, rewind_index=True):
        load_spec = dict()
        load_spec["doc_crud"] = dict()
        load_spec[MetaCrudParams.TARGET_VBUCKETS] = self.target_vbucket

        mutation_num = 0
        if "create" in self.doc_ops:
            load_spec["doc_crud"][
                MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 100
        if "update" in self.doc_ops:
            load_spec["doc_crud"][
                MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 100
            mutation_num = 1
        elif "delete" in self.doc_ops:
            load_spec["doc_crud"][
                MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION] = 100

        if self.durability_level:
            load_spec[MetaCrudParams.DURABILITY_LEVEL] = self.durability_level

        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.bucket_util.buckets,
                load_spec,
                mutation_num=mutation_num)
        if doc_loading_task.result is False:
            self.log_failure("Doc operation failed for '%s'" % self.doc_ops)

        # Fetch total affected mutation count
        for bucket, s_dict in doc_loading_task.loader_spec.items():
            for s_name, c_dict in s_dict["scopes"].items():
                for c_name, _ in c_dict["collections"].items():
                    c_crud_data = doc_loading_task.loader_spec[
                        bucket]["scopes"][
                        s_name]["collections"][c_name]
                    for op_type in c_crud_data.keys():
                        self.total_rollback_items += \
                            c_crud_data[op_type]["doc_gen"].end \
                            - c_crud_data[op_type]["doc_gen"].start
                        if rewind_index:
                            self.bucket_util.rewind_doc_index(
                                bucket.scopes[s_name].collections[c_name],
                                op_type,
                                c_crud_data[op_type]["doc_gen"],
                                update_num_items=True)

    def test_rollback_n_times(self):
        ep_queue_size_map = dict()
        vb_replica_queue_size_map = dict()
        expected_num_items = \
            self.bucket_util.get_expected_total_num_items(self.bucket)

        keys_to_verify = ["max_visible_seqno",
                          "num_items",
                          "high_completed_seqno",
                          "purge_seqno"]

        if self.nodes_init < 2 or self.num_replicas < 1:
            self.fail("Not enough nodes/replicas to test rollback")

        # Fetch vbucket stats for validation
        self.get_vb_details_cbstats_for_all_nodes("pre_rollback")

        target_node = choice(self.kv_nodes)
        shell = self.node_shells[target_node]["shell"]
        cb_stats = self.node_shells[target_node]["cbstat"]
        self.target_vbucket = cb_stats.vbucket_list(self.bucket.name)

        for _ in xrange(1, self.num_rollbacks + 1):
            self.total_rollback_items = 0
            self.log.info("Stopping persistence on '%s'" % target_node.ip)
            Cbepctl(shell).persistence(self.bucket.name, "stop")
            self.load_docs()

            if self.rollback_with_multiple_mutation:
                self.doc_ops = "update"
                self.load_docs()
            for node in self.cluster.nodes_in_cluster:
                ep_queue_size = 0
                if node.ip == target_node.ip:
                    ep_queue_size = self.total_rollback_items
                ep_queue_size_map.update({node: ep_queue_size})
                vb_replica_queue_size_map.update({node: 0})

            self.log.info("Validating stats")
            for bucket in self.bucket_util.buckets:
                self.bucket_util._wait_for_stat(bucket, ep_queue_size_map)
                self.bucket_util._wait_for_stat(
                    bucket,
                    vb_replica_queue_size_map,
                    stat_name="vb_replica_queue_size")

            self.log.info("Killing memcached to trigger rollback")
            shell.kill_memcached()
            self.assertTrue(self.bucket_util._wait_warmup_completed(
                [target_node],
                self.bucket,
                wait_time=300))
            self.bucket_util.verify_stats_all_buckets(expected_num_items,
                                                      timeout=120)

            self.get_vb_details_cbstats_for_all_nodes("post_rollback")
            self.validate_seq_no_post_rollback("pre_rollback", "post_rollback",
                                               keys_to_verify)
            self.bucket_util.validate_docs_per_collections_all_buckets()

        self.validate_test_failure()

    def test_rollback_to_zero(self):
        if self.nodes_init < 2 or self.num_replicas < 1:
            self.fail("Not enough nodes/replicas to test rollback")

        keys_to_verify = ["high_completed_seqno",
                          "purge_seqno"]
        # Override num_items to load data into each collection
        self.num_items = 10000

        # Set values to num_items to support loading through
        # collection loading task
        for bucket in self.bucket_util.buckets:
            for _, scope in bucket.scopes.items():
                for _, collection in scope.collections.items():
                    collection.num_items = self.num_items

        # Fetch vbucket stats for validation
        self.get_vb_details_cbstats_for_all_nodes("pre_rollback")

        target_node = choice(self.cluster_util.get_kv_nodes())
        shell = self.node_shells[target_node]["shell"]
        cbstats = self.node_shells[target_node]["cbstat"]
        self.target_vbucket = cbstats.vbucket_list(self.bucket.name)

        self.log.info("Stopping persistence on %s" % target_node.ip)
        Cbepctl(shell).persistence(self.bucket.name, "stop")

        self.total_rollback_items = 0
        for i in xrange(1, self.num_rollbacks + 1):
            self.load_docs(rewind_index=False)
            if self.rollback_with_multiple_mutation:
                self.doc_ops = "update"
                self.load_docs(rewind_index=False)
            stat_map = dict()
            for node in self.cluster.nodes_in_cluster:
                expected_val = 0
                if node.ip == target_node.ip:
                    expected_val = self.total_rollback_items
                stat_map.update({node: expected_val})

            for bucket in self.bucket_util.buckets:
                self.bucket_util._wait_for_stat(bucket, stat_map)
            self.sleep(60)
            self.get_vb_details_cbstats_for_all_nodes("post_rollback")
            self.validate_seq_no_post_rollback("pre_rollback", "post_rollback",
                                               keys_to_verify)

        self.log.info("Killing memcached to trigger rollback")
        shell.kill_memcached()
        self.assertTrue(self.bucket_util._wait_warmup_completed(
            [target_node],
            self.bucket,
            wait_time=300))

        # Reset expected values to '0' for validation
        for bucket in self.bucket_util.buckets:
            for _, scope in bucket.scopes.items():
                for _, collection in scope.collections.items():
                    collection.num_items = 0
        self.bucket_util.validate_docs_per_collections_all_buckets()
        self.validate_test_failure()
