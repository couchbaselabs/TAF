from copy import deepcopy
from random import randint, choice

from BucketLib.bucket import Bucket
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from basetestcase import ClusterSetup
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from cb_tools.cbstats import Cbstats
from couchbase_helper.documentgenerator import doc_generator
from couchbase_helper.durability_helper import DurabilityHelper
from error_simulation.cb_error import CouchbaseError
from sdk_exceptions import SDKException
from constants.sdk_constants.java_client import SDKConstants
from shell_util.remote_connection import RemoteMachineShellConnection
from sirius_client_framework.sirius_setup import SiriusSetup


class DurabilityTestsBase(ClusterSetup):
    def setUp(self):
        super(DurabilityTestsBase, self).setUp()

        # Create default bucket
        self.create_bucket(self.cluster)

        self.simulate_error = self.input.param("simulate_error", None)
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

        # Disable auto-failover to avoid failover of nodes
        ClusterRestAPI(self.cluster.master).update_auto_failover_settings(
            enabled="false")

        self.bucket = self.cluster.buckets[0]

        # Create sdk_clients for pool
        if self.load_docs_using == "default_loader" \
                and self.cluster.sdk_client_pool:
            self.log.info("Creating SDK client pool")
            self.cluster.sdk_client_pool.create_clients(
                self.cluster, self.bucket,
                req_clients=self.sdk_pool_capacity,
                compression_settings=self.sdk_compression)
        elif self.load_docs_using == "sirius_java_sdk":
            self.log.info("Creating SDK clients in Java side")
            for bucket in self.cluster.buckets:
                SiriusCouchbaseLoader.create_clients_in_pool(
                    self.cluster.master, self.cluster.master.rest_username,
                    self.cluster.master.rest_password,
                    bucket.name, req_clients=self.sdk_pool_capacity)

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
                vbuckets=self.cluster.vbuckets)

            self.log.info("Loading {0} items into bucket"
                          .format(self.num_items))
            task = self.task.async_load_gen_docs(
                self.cluster, self.bucket, doc_create, "create", 0,
                batch_size=10, process_concurrency=8,
                replicate_to=self.replicate_to, persist_to=self.persist_to,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout,
                load_using=self.load_docs_using)
            self.task.jython_task_manager.get_task_result(task)

            # Verify initial doc load count
            self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                         self.cluster.buckets)
            self.bucket_util.verify_stats_all_buckets(self.cluster,
                                                      self.num_items)

        self.bucket_util.print_bucket_stats(self.cluster)
        self.log.info("=== DurabilityBaseTests setup complete ===")

    def tearDown(self):
        # Fail the test case, if the failure is set
        super(DurabilityTestsBase, self).tearDown()

    def get_random_node(self):
        rand_node_index = randint(1, self.nodes_init-1)
        return self.cluster.nodes_in_cluster[rand_node_index]

    def getTargetNodes(self):
        def select_random_node(nodes):
            rand_node_index = randint(1, self.nodes_init-1)
            if self.cluster.nodes_in_cluster[rand_node_index] not in node_list:
                nodes.append(self.cluster.nodes_in_cluster[rand_node_index])

        node_list = list()
        if len(self.cluster.nodes_in_cluster) > 1:
            # Choose random nodes, if the cluster is not a single node cluster
            while len(node_list) != self.num_nodes_affected:
                select_random_node(node_list)
        else:
            node_list.append(self.cluster.master)
        return node_list


class BucketDurabilityBase(ClusterSetup):
    def setUp(self):
        super(BucketDurabilityBase, self).setUp()

        if len(self.cluster.servers) < self.nodes_init:
            self.fail("Not enough nodes for rebalance")

        # Disable auto-failover to avoid failover of nodes
        status, _ = ClusterRestAPI(self.cluster.master) \
            .update_auto_failover_settings(enabled="false")
        self.assertTrue(status, msg="Failure during disabling auto-failover")

        self.durability_helper = DurabilityHelper(
            self.log,
            len(self.cluster.nodes_in_cluster))
        self.kv_nodes = self.cluster_util.get_kv_nodes(self.cluster)

        self.num_nodes_affected = 1
        if self.num_replicas > 1:
            self.num_nodes_affected = 2

        # Bucket create options representation
        self.bucket_template = dict()
        self.bucket_template[Bucket.name] = "default"
        self.bucket_template[Bucket.ramQuotaMB] = 256
        self.bucket_template[Bucket.replicaNumber] = self.num_replicas
        if self.bucket_type == Bucket.Type.MEMBASE:
            self.bucket_template[Bucket.storageBackend] = self.bucket_storage

        # These two params will be set during each iteration
        self.bucket_template[Bucket.bucketType] = None
        self.bucket_template[Bucket.durabilityMinLevel] = None

        self.bucket_types_to_test = [Bucket.Type.MEMBASE,
                                     Bucket.Type.EPHEMERAL]

        self.d_level_order = [
            SDKConstants.DurabilityLevel.NONE,
            SDKConstants.DurabilityLevel.MAJORITY,
            SDKConstants.DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE,
            SDKConstants.DurabilityLevel.PERSIST_TO_MAJORITY]

        # Dict representing the possible levels supported by each bucket type
        self.possible_d_levels = dict()
        self.possible_d_levels[Bucket.Type.MEMBASE] = \
            self.bucket_util.get_supported_durability_levels()
        self.possible_d_levels[Bucket.Type.EPHEMERAL] = [
            SDKConstants.DurabilityLevel.NONE,
            SDKConstants.DurabilityLevel.MAJORITY]

        # Dict to store the list of active/replica VBs in each node
        self.vbs_in_node = dict()
        for node in self.cluster_util.get_kv_nodes(self.cluster):
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
    def get_bucket_durability_level(sdk_durability_level):
        sdk_durability_level = sdk_durability_level.upper()
        if sdk_durability_level == \
                SDKConstants.DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE:
            return Bucket.DurabilityMinLevel.MAJORITY_AND_PERSIST_ACTIVE
        return getattr(Bucket.DurabilityMinLevel, sdk_durability_level)

    @staticmethod
    def get_cb_stat_verification_dict():
        verification_dict = dict()
        verification_dict["ops_create"] = 0
        verification_dict["ops_update"] = 0
        verification_dict["ops_delete"] = 0
        verification_dict["rollback_item_count"] = 0
        verification_dict["sync_write_aborted_count"] = 0
        verification_dict["sync_write_committed_count"] = 0
        return verification_dict

    def get_vbucket_type_mapping(self, bucket_name):
        for node in list(self.vbs_in_node.keys()):
            cb_stat = Cbstats(node)
            self.vbs_in_node[node]["active"] = \
                cb_stat.vbucket_list(bucket_name, "active")
            self.vbs_in_node[node]["replica"] = \
                cb_stat.vbucket_list(bucket_name, "replica")
            cb_stat.disconnect()

    def get_bucket_dict(self, bucket_type, bucket_durability):
        bucket_dict = deepcopy(self.bucket_template)
        bucket_dict[Bucket.bucketType] = bucket_type
        bucket_dict[Bucket.durabilityMinLevel] = \
            bucket_durability
        return bucket_dict

    def get_supported_durability_for_bucket(self):
        if self.bucket_type == Bucket.Type.EPHEMERAL:
            return [SDKConstants.DurabilityLevel.NONE,
                    SDKConstants.DurabilityLevel.MAJORITY]
        return self.bucket_util.get_supported_durability_levels()

    def init_java_clients(self, bucket):
        if self.load_docs_using != "sirius_java_sdk":
            return
        SiriusSetup.reset_java_loader_tasks(self.thread_to_use)
        self.log.info("Creating SDK clients in Java side")
        SiriusCouchbaseLoader.create_clients_in_pool(
            self.cluster.master, self.cluster.master.rest_username,
            self.cluster.master.rest_password,
            bucket.name, req_clients=self.sdk_pool_capacity)

    def validate_durability_with_crud(
            self, bucket, bucket_durability,
            verification_dict,
            doc_start_index=0,
            num_items_to_load=1, op_type="create",
            doc_durability=SDKConstants.DurabilityLevel.NONE):
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
        if d_level_to_test == SDKConstants.DurabilityLevel.NONE:
            return

        self.log.info("Performing %s operation to validate d_level %s"
                      % (op_type, d_level_to_test))

        # Can't simulate error conditions for all durability_levels.
        # So only perform CRUD without error_sim
        if len(self.vbs_in_node.keys()) > 1:
            # Pick a random node to perform error sim and load
            random_node = choice(list(self.vbs_in_node.keys()))

            target_vb_type, simulate_error = \
                self.durability_helper.get_vb_and_error_type(d_level_to_test)

            doc_gen = doc_generator(
                self.key, doc_start_index, num_items_to_load,
                vbuckets=bucket.numVBuckets,
                target_vbucket=self.vbs_in_node[random_node][target_vb_type])
            error_sim = CouchbaseError(self.log,
                                       self.vbs_in_node[random_node]["shell"],
                                       node=random_node)

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
                start_task=False,
                load_using=self.load_docs_using)

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
                if not SDKException.check_if_exception_exists(
                        SDKException.DurabilityAmbiguousException,
                        str(result["error"])):
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
            exp=self.maxttl, durability=doc_durability,
            timeout_secs=2, batch_size=1,
            load_using=self.load_docs_using)
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
        def select_random_node(nodes):
            rand_node_index = randint(1, self.nodes_init-1)
            if self.cluster.nodes_in_cluster[rand_node_index] not in node_list:
                nodes.append(self.cluster.nodes_in_cluster[rand_node_index])

        node_list = list()
        if len(self.cluster.nodes_in_cluster) > 1:
            # Choose random nodes, if the cluster is not a single node cluster
            while len(node_list) != self.num_nodes_affected:
                select_random_node(node_list)
        else:
            node_list.append(self.cluster.master)
        return node_list

    def cb_stat_verify(self, verification_dict):
        failed = self.durability_helper.verify_vbucket_details_stats(
            self.cluster.buckets[0],
            self.kv_nodes,
            expected_val=verification_dict)
        if failed:
            self.log_failure("Cbstat vbucket-details validation failed")
        self.summary.add_step("Cbstat vb-details validation")
