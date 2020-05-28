from copy import deepcopy
from random import randint, choice

from BucketLib.bucket import Bucket
from basetestcase import BaseTestCase
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from couchbase_helper.durability_helper import \
    BucketDurability, \
    DurabilityHelper
from error_simulation.cb_error import CouchbaseError
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from sdk_exceptions import SDKException


class DurabilityTestsBase(BaseTestCase):
    def setUp(self):
        super(DurabilityTestsBase, self).setUp()

        self.simulate_error = self.input.param("simulate_error", None)
        self.error_type = self.input.param("error_type", "memory")
        self.doc_ops = self.input.param("doc_ops", None)
        self.with_non_sync_writes = self.input.param("with_non_sync_writes",
                                                     False)
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

        # Initialize cluster using given nodes
        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend([self.cluster.master]+nodes_init)

        # Disable auto-failover to avoid failover of nodes
        status = RestConnection(self.cluster.master) \
            .update_autofailover_settings(False, 120, False)
        self.assertTrue(status, msg="Failure during disabling auto-failover")

        # Create default bucket and add rbac user
        self.bucket_util.create_default_bucket(
            replica=self.num_replicas, compression_mode=self.compression_mode,
            bucket_type=self.bucket_type, storage=self.bucket_storage,
            eviction_policy=self.bucket_eviction_policy)
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
                key_size=self.key_size,
                doc_size=self.doc_size,
                doc_type=self.doc_type,
                target_vbucket=self.target_vbucket,
                vbuckets=self.cluster_util.vbuckets)

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
        self.log.info("=== DurabilityBaseTests setup complete ===")

    def tearDown(self):
        # Fail the test case, if the failure is set
        super(DurabilityTestsBase, self).tearDown()

    def get_random_node(self):
        rand_node_index = randint(1, self.nodes_init-1)
        return self.cluster.nodes_in_cluster[rand_node_index]

    def getTargetNodes(self):
        def select_randam_node(nodes):
            rand_node_index = randint(1, self.nodes_init-1)
            if self.cluster.nodes_in_cluster[rand_node_index] not in node_list:
                nodes.append(self.cluster.nodes_in_cluster[rand_node_index])

        node_list = list()
        if len(self.cluster.nodes_in_cluster) > 1:
            # Choose random nodes, if the cluster is not a single node cluster
            while len(node_list) != self.num_nodes_affected:
                select_randam_node(node_list)
        else:
            node_list.append(self.cluster.master)
        return node_list


class BucketDurabilityBase(BaseTestCase):
    def setUp(self):
        super(BucketDurabilityBase, self).setUp()

        if len(self.cluster.servers) < self.nodes_init:
            self.fail("Not enough nodes for rebalance")

        # Rebalance-in required nodes for testing
        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [])
        self.cluster.nodes_in_cluster.extend([self.cluster.master]+nodes_init)

        # Disable auto-failover to avoid failover of nodes
        status = RestConnection(self.cluster.master) \
            .update_autofailover_settings(False, 120, False)
        self.assertTrue(status, msg="Failure during disabling auto-failover")
        self.bucket_util.add_rbac_user()

        self.durability_helper = DurabilityHelper(
            self.log,
            len(self.cluster.nodes_in_cluster))
        self.kv_nodes = self.cluster_util.get_kv_nodes()

        self.num_nodes_affected = 1
        if self.num_replicas > 1:
            self.num_nodes_affected = 2

        # Bucket create options representation
        self.bucket_template = dict()
        self.bucket_template[Bucket.name] = "default"
        self.bucket_template[Bucket.ramQuotaMB] = 100
        self.bucket_template[Bucket.replicaNumber] = self.num_replicas
        # These two params will be set during each iteration
        self.bucket_template[Bucket.bucketType] = None
        self.bucket_template[Bucket.durabilityMinLevel] = None

        # Print cluster stats
        self.cluster_util.print_cluster_stats()

        self.bucket_types_to_test = [Bucket.Type.MEMBASE,
                                     Bucket.Type.EPHEMERAL,
                                     Bucket.Type.MEMCACHED]

        self.d_level_order = [
            Bucket.DurabilityLevel.NONE,
            Bucket.DurabilityLevel.MAJORITY,
            Bucket.DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE,
            Bucket.DurabilityLevel.PERSIST_TO_MAJORITY]

        # Dict representing the possible levels supported by each bucket type
        self.possible_d_levels = dict()
        self.possible_d_levels[Bucket.Type.MEMBASE] = \
            self.bucket_util.get_supported_durability_levels()
        self.possible_d_levels[Bucket.Type.EPHEMERAL] = [
            Bucket.DurabilityLevel.NONE,
            Bucket.DurabilityLevel.MAJORITY]
        self.possible_d_levels[Bucket.Type.MEMCACHED] = [
            Bucket.DurabilityLevel.NONE]

        # Dict to store the list of active/replica VBs in each node
        self.vbs_in_node = dict()
        for node in self.cluster_util.get_kv_nodes():
            shell = RemoteMachineShellConnection(node)
            self.vbs_in_node[node] = dict()
            self.vbs_in_node[node]["shell"] = shell
        self.log.info("===== BucketDurabilityBase setup complete =====")

    def tearDown(self):
        # Close all shell_connections opened in setUp()
        for node in self.vbs_in_node:
            self.vbs_in_node[node]["shell"].disconnect()

        super(BucketDurabilityBase, self).tearDown()

        self.summary.display()
        self.validate_test_failure()

    @staticmethod
    def get_cb_stat_verification_dict():
        verification_dict = dict()
        verification_dict["ops_create"] = 0
        verification_dict["ops_update"] = 0
        verification_dict["ops_delete"] = 0
        verification_dict["ops_get"] = 0
        verification_dict["rollback_item_count"] = 0
        verification_dict["sync_write_aborted_count"] = 0
        verification_dict["sync_write_committed_count"] = 0
        return verification_dict

    def get_vbucket_type_mapping(self, bucket_name):
        for node in self.vbs_in_node.keys():
            cb_stat = Cbstats(self.vbs_in_node[node]["shell"])
            self.vbs_in_node[node]["active"] = \
                cb_stat.vbucket_list(bucket_name, "active")
            self.vbs_in_node[node]["replica"] = \
                cb_stat.vbucket_list(bucket_name, "replica")

    def get_bucket_dict(self, bucket_type, bucket_durability):

        bucket_dict = deepcopy(self.bucket_template)
        bucket_dict[Bucket.bucketType] = bucket_type
        bucket_dict[Bucket.durabilityMinLevel] = \
            BucketDurability[bucket_durability]

        return bucket_dict

    def get_supported_durability_for_bucket(self):
        if self.bucket_type == Bucket.Type.EPHEMERAL:
            return [Bucket.DurabilityLevel.NONE,
                    Bucket.DurabilityLevel.MAJORITY]
        return self.bucket_util.get_supported_durability_levels()

    @staticmethod
    def get_vb_and_error_type(d_level):
        # Select target_vb type for testing with CRUDs
        target_vb_type = "replica"
        simulate_error = CouchbaseError.STOP_MEMCACHED

        if d_level == Bucket.DurabilityLevel.MAJORITY:
            target_vb_type = "replica"
            simulate_error = CouchbaseError.STOP_MEMCACHED
        elif d_level == \
                Bucket.DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE:
            target_vb_type = "active"
            simulate_error = CouchbaseError.STOP_PERSISTENCE
        elif d_level == Bucket.DurabilityLevel.PERSIST_TO_MAJORITY:
            target_vb_type = "replica"
            simulate_error = CouchbaseError.STOP_PERSISTENCE
        return target_vb_type, simulate_error

    def validate_durability_with_crud(
            self, bucket, bucket_durability,
            verification_dict,
            doc_start_index=0,
            num_items_to_load=1, op_type="create",
            doc_durability=Bucket.DurabilityLevel.NONE):
        """
        Common API to validate durability settings of the bucket is set
        correctly or not.

        :param bucket: Bucket object to validate
        :param bucket_durability: Durability set for the bucket
                                  Note: Need this because the string within the
                                        bucket object is different than this.
        :param verification_dict: To hold the values for req cbstats to verify
        :param doc_start_index: Starting index to be considered for doc_load
        :param num_items_to_load: Number of items to be loaded to test.
                                  Default is '1'
        :param op_type: Type of CRUD to perform. Default is 'create'
        :param doc_durability: Document durability level to use during CRUD.
                               Default level is 'None'
        :return:
        """
        def get_d_level_used():
            if self.d_level_order.index(bucket_durability) \
                    < self.d_level_order.index(doc_durability):
                return doc_durability
            return bucket_durability

        d_level_to_test = get_d_level_used()
        # Nothing to test for durability_level=None (async_write case)
        if d_level_to_test == Bucket.DurabilityLevel.NONE:
            return

        self.log.info("Performing %s operation to validate d_level %s"
                      % (op_type, d_level_to_test))

        # Can't simulate error conditions for all durability_levels.
        # So only perform CRUD without error_sim
        if len(self.vbs_in_node.keys()) > 1:
            # Pick a random node to perform error sim and load
            random_node = choice(self.vbs_in_node.keys())

            target_vb_type, simulate_error = \
                self.get_vb_and_error_type(d_level_to_test)

            doc_gen = doc_generator(
                self.key, doc_start_index, num_items_to_load,
                target_vbucket=self.vbs_in_node[random_node][target_vb_type])
            error_sim = CouchbaseError(self.log,
                                       self.vbs_in_node[random_node]["shell"])

            doc_load_task = self.task.async_load_gen_docs(
                self.cluster, bucket, doc_gen, op_type,
                exp=self.maxttl,
                replicate_to=self.replicate_to,
                persist_to=self.persist_to,
                durability=doc_durability,
                timeout_secs=32,
                batch_size=1,
                skip_read_on_error=True,
                suppress_error_table=True,
                start_task=False)

            self.sleep(5, "Wait for sdk_client to get warmed_up")
            # Simulate target error condition
            error_sim.create(simulate_error)
            self.sleep(5, "Wait for error_sim to take effect")

            # Start doc_loading task and wait for it to complete
            self.task_manager.add_new_task(doc_load_task)
            self.task_manager.get_task_result(doc_load_task)

            # Revert the induced error condition
            self.sleep(5, "Wait before reverting error_simulation")
            error_sim.revert(simulate_error)

            # Validate failed doc count and exception type from SDK
            if not doc_load_task.fail.keys():
                self.log_failure("Docs inserted without honoring the "
                                 "bucket durability level")
            for key, result in doc_load_task.fail.items():
                if SDKException.DurabilityAmbiguousException \
                        not in str(result["error"]):
                    self.log_failure("Invalid exception for key %s "
                                     "during %s operation: %s"
                                     % (key, op_type, result["error"]))

            verification_dict["sync_write_aborted_count"] += num_items_to_load
        else:
            doc_gen = doc_generator(self.key, doc_start_index,
                                    doc_start_index+num_items_to_load)

        # Retry the same CRUDs without any error simulation in place
        doc_load_task = self.task.async_load_gen_docs(
            self.cluster, bucket, doc_gen, op_type,
            exp=self.maxttl,
            durability=doc_durability,
            timeout_secs=2,
            batch_size=1)
        self.task_manager.get_task_result(doc_load_task)
        if doc_load_task.fail:
            self.log_failure("Failures seen during CRUD without "
                             "error simulation. Keys failed: %s"
                             % doc_load_task.fail.keys())
        else:
            verification_dict["ops_%s" % op_type] += \
                num_items_to_load
            verification_dict["sync_write_committed_count"] += \
                num_items_to_load

    def getTargetNodes(self):
        def select_randam_node(nodes):
            rand_node_index = randint(1, self.nodes_init-1)
            if self.cluster.nodes_in_cluster[rand_node_index] not in node_list:
                nodes.append(self.cluster.nodes_in_cluster[rand_node_index])

        node_list = list()
        if len(self.cluster.nodes_in_cluster) > 1:
            # Choose random nodes, if the cluster is not a single node cluster
            while len(node_list) != self.num_nodes_affected:
                select_randam_node(node_list)
        else:
            node_list.append(self.cluster.master)
        return node_list

    def cb_stat_verify(self, verification_dict):
        failed = self.durability_helper.verify_vbucket_details_stats(
            self.bucket_util.buckets[0],
            self.kv_nodes,
            vbuckets=self.cluster_util.vbuckets,
            expected_val=verification_dict)
        if failed:
            self.log_failure("Cbstat vbucket-details validation failed")
        self.summary.add_step("Cbstat vb-details validation")
