from random import choice

from bucket_collections.collections_base import CollectionBase
from cb_tools.cbepctl import Cbepctl
from cb_tools.cbstats import Cbstats
from collections_helper.collections_spec_constants import MetaCrudParams
from error_simulation.cb_error import CouchbaseError
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection


class RollbackTests(CollectionBase):
    def setUp(self):
        super(RollbackTests, self).setUp()
        self.num_rollbacks = self.input.param("num_rollbacks", 2)
        self.collection_ops_type = self.input.param("collection_ops", None)
        self.rollback_with_multiple_mutation = self.input.param(
            "rollback_with_multiple_mutation", False)
        self.bucket = self.cluster.buckets[0]

        # Disable auto-fail_over to avoid fail_over of nodes
        status = RestConnection(self.cluster.master) \
            .update_autofailover_settings(False, 120, False)
        self.assertTrue(status, msg="Failure during disabling auto-failover")

        # Used to calculate expected queue size of validation before rollback
        self.total_rollback_items = 0
        self.kv_nodes = self.cluster_util.get_kv_nodes(self.cluster)

        self.sync_write_enabled = self.durability_helper.is_sync_write_enabled(
            self.bucket_durability_level, self.durability_level)

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

    def __rewind_doc_index(self, doc_loading_task):
        for bucket, s_dict in doc_loading_task.loader_spec.items():
            for s_name, c_dict in s_dict["scopes"].items():
                for c_name, _ in c_dict["collections"].items():
                    c_crud_data = doc_loading_task.loader_spec[
                        bucket]["scopes"][
                        s_name]["collections"][c_name]
                    for op_type in c_crud_data.keys():
                        self.bucket_util.rewind_doc_index(
                            bucket.scopes[s_name].collections[c_name],
                            op_type,
                            c_crud_data[op_type]["doc_gen"],
                            update_num_items=True)

    def load_docs(self, doc_ops):
        load_spec = dict()
        load_spec["doc_crud"] = dict()
        load_spec["doc_crud"][MetaCrudParams.DocCrud.COMMON_DOC_KEY] \
            = "test_collections"
        load_spec[MetaCrudParams.TARGET_VBUCKETS] = self.target_vbuckets
        load_spec[MetaCrudParams.COLLECTIONS_CONSIDERED_FOR_CRUD] = 3
        load_spec[MetaCrudParams.SCOPES_CONSIDERED_FOR_CRUD] = "all"
        load_spec[MetaCrudParams.SDK_TIMEOUT] = 60

        if self.durability_level:
            load_spec[MetaCrudParams.DURABILITY_LEVEL] = self.durability_level

        mutation_num = 0
        if self.collection_ops_type != "without_cruds":
            if "create" in doc_ops:
                load_spec["doc_crud"][
                    MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] \
                    = 100
            if "update" in doc_ops:
                load_spec["doc_crud"][
                    MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] \
                    = 50
                mutation_num = 1
            if "delete" in doc_ops:
                load_spec["doc_crud"][
                    MetaCrudParams.DocCrud.DELETE_PERCENTAGE_PER_COLLECTION] \
                    = 10

        if self.collection_ops_type in ["without_cruds", "with_cruds"]:
            load_spec[MetaCrudParams.SCOPES_CONSIDERED_FOR_OPS] = "all"
            load_spec[MetaCrudParams.COLLECTIONS_CONSIDERED_FOR_OPS] = "all"
            load_spec[MetaCrudParams.COLLECTIONS_TO_DROP] = 2
            load_spec[MetaCrudParams.SCOPES_TO_ADD_PER_BUCKET] = 1
            load_spec[MetaCrudParams.COLLECTIONS_TO_ADD_FOR_NEW_SCOPES] = 2
            load_spec[MetaCrudParams.COLLECTIONS_TO_ADD_PER_BUCKET] = 2
            load_spec[MetaCrudParams.SCOPES_TO_RECREATE] = 1

            load_spec["doc_crud"][
                MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS] = 100

        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.cluster.buckets,
                load_spec,
                mutation_num=mutation_num)
        if doc_loading_task.result is False:
            self.log_failure("Doc operation failed for '%s'" % doc_ops)

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
        return doc_loading_task

    def test_rollback_n_times(self):
        doc_loading_task_2 = None
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
        error_sim = CouchbaseError(self.log, shell)
        cb_stats = self.node_shells[target_node]["cbstat"]
        self.target_vbuckets = cb_stats.vbucket_list(self.bucket.name)

        for _ in xrange(1, self.num_rollbacks + 1):
            self.total_rollback_items = 0
            error_sim.create(CouchbaseError.STOP_PERSISTENCE, self.bucket.name)
            doc_loading_task_1 = self.load_docs(self.doc_ops)

            if self.rollback_with_multiple_mutation:
                doc_loading_task_2 = self.load_docs("update")
            for node in self.cluster.nodes_in_cluster:
                ep_queue_size = 0
                if node.ip == target_node.ip:
                    ep_queue_size = self.total_rollback_items
                if self.sync_write_enabled:
                    # Includes prepare+commit mutation
                    ep_queue_size *= 2
                ep_queue_size_map.update({node: ep_queue_size})
                vb_replica_queue_size_map.update({node: 0})

            self.log.info("Validating stats")
            for bucket in self.cluster.buckets:
                self.bucket_util._wait_for_stat(bucket, ep_queue_size_map,
                                                timeout=self.wait_timeout)
                self.bucket_util._wait_for_stat(
                    bucket,
                    vb_replica_queue_size_map,
                    stat_name="vb_replica_queue_size",
                    timeout=self.wait_timeout)

            if self.rollback_with_multiple_mutation:
                self.__rewind_doc_index(doc_loading_task_2)
            self.__rewind_doc_index(doc_loading_task_1)

            error_sim.create(CouchbaseError.KILL_MEMCACHED)
            self.assertTrue(self.bucket_util._wait_warmup_completed(
                [target_node],
                self.bucket,
                wait_time=300))
            self.bucket_util.verify_stats_all_buckets(self.cluster,
                                                      expected_num_items,
                                                      timeout=120)

            self.get_vb_details_cbstats_for_all_nodes("post_rollback")
            self.validate_seq_no_post_rollback("pre_rollback", "post_rollback",
                                               keys_to_verify)
            self.bucket_util.validate_docs_per_collections_all_buckets(
                self.cluster)

        self.validate_test_failure()

    def test_rollback_to_zero(self):
        if self.nodes_init < 2 or self.num_replicas < 1:
            self.fail("Not enough nodes/replicas to test rollback")

        keys_to_verify = ["high_completed_seqno",
                          "purge_seqno"]
        doc_loading_task_2 = None
        # Override num_items to load data into each collection
        self.num_items = 10000

        # Set values to num_items to support loading through
        # collection loading task
        for bucket in self.cluster.buckets:
            for _, scope in bucket.scopes.items():
                for _, collection in scope.collections.items():
                    collection.num_items = self.num_items

        # Fetch vbucket stats for validation
        self.get_vb_details_cbstats_for_all_nodes("pre_rollback")

        target_node = choice(self.cluster_util.get_kv_nodes(self.cluster))
        shell = self.node_shells[target_node]["shell"]
        cbstats = self.node_shells[target_node]["cbstat"]
        self.target_vbuckets = cbstats.vbucket_list(self.bucket.name)

        for i in xrange(1, self.num_rollbacks + 1):
            self.total_rollback_items = 0
            self.log.info("Stopping persistence on %s" % target_node.ip)
            Cbepctl(shell).persistence(self.bucket.name, "stop")

            doc_loading_task_1 = self.load_docs(self.doc_ops)
            if self.rollback_with_multiple_mutation:
                doc_loading_task_2 = self.load_docs("update")
            stat_map = dict()
            for node in self.cluster.nodes_in_cluster:
                expected_val = 0
                if node.ip == target_node.ip:
                    expected_val = self.total_rollback_items
                    if self.sync_write_enabled:
                        # Includes prepare+commit mutation
                        expected_val *= 2
                stat_map.update({node: expected_val})

            for bucket in self.cluster.buckets:
                self.bucket_util._wait_for_stat(bucket, stat_map,
                                                timeout=self.wait_timeout)

            if doc_loading_task_2:
                self.__rewind_doc_index(doc_loading_task_2)
            self.__rewind_doc_index(doc_loading_task_1)

            self.log.info("Killing memcached to trigger rollback")
            shell.kill_memcached()
            self.assertTrue(self.bucket_util._wait_warmup_completed(
                [target_node],
                self.bucket,
                wait_time=300))

            self.sleep(10, "Wait after bucket warmup for cbstats to work")
            self.get_vb_details_cbstats_for_all_nodes("post_rollback")
            self.validate_seq_no_post_rollback("pre_rollback", "post_rollback",
                                               keys_to_verify)

        # Reset expected values to '0' for validation
        for bucket in self.cluster.buckets:
            for _, scope in bucket.scopes.items():
                for _, collection in scope.collections.items():
                    collection.num_items = 0
        self.bucket_util.validate_docs_per_collections_all_buckets(
            self.cluster)
        self.validate_test_failure()
