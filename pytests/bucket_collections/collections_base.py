from math import ceil

from basetestcase import BaseTestCase
from collections_helper.collections_spec_constants import \
    MetaConstants, MetaCrudParams
from couchbase_helper.durability_helper import DurabilityHelper
from membase.api.rest_client import RestConnection
from BucketLib.BucketOperations import BucketHelper
from sdk_exceptions import SDKException


class CollectionBase(BaseTestCase):
    def setUp(self):
        super(CollectionBase, self).setUp()
        self.log_setup_status("CollectionBase", "started")
        self.key = 'test_collection'.rjust(self.key_size, '0')
        self.simulate_error = self.input.param("simulate_error", None)
        self.error_type = self.input.param("error_type", "memory")
        self.doc_ops = self.input.param("doc_ops", None)
        self.spec_name = self.input.param("bucket_spec",
                                          "single_bucket.default")
        self.over_ride_spec_params = \
            self.input.param("override_spec_params", "").split(";")

        self.action_phase = self.input.param("action_phase",
                                             "before_default_load")
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

        # Create bucket(s) and add rbac user
        self.bucket_util.add_rbac_user()
        buckets_spec = self.bucket_util.get_bucket_template_from_package(
            self.spec_name)
        doc_loading_spec = \
            self.bucket_util.get_crud_template_from_package("initial_load")

        # Process params to over_ride values if required
        self.over_ride_template_params(buckets_spec)
        self.over_ride_template_params(doc_loading_spec)

        # MB-38438, adding CollectionNotFoundException in retry exception
        doc_loading_spec[MetaCrudParams.RETRY_EXCEPTIONS].append(
            SDKException.CollectionNotFoundException)

        self.bucket_util.create_buckets_using_json_data(buckets_spec)
        self.bucket_util.wait_for_collection_creation_to_complete()

        # Create clients in SDK client pool
        if self.sdk_client_pool:
            self.log.info("Creating required SDK clients for client_pool")
            bucket_count = len(self.bucket_util.buckets)
            max_clients = self.task_manager.number_of_threads
            clients_per_bucket = int(ceil(max_clients / bucket_count))
            for bucket in self.bucket_util.buckets:
                self.sdk_client_pool.create_clients(
                    bucket,
                    [self.cluster.master],
                    clients_per_bucket,
                    compression_settings=self.sdk_compression)

        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.bucket_util.buckets,
                doc_loading_spec,
                mutation_num=0)
        if doc_loading_task.result is False:
            self.fail("Initial doc_loading failed")

        self.cluster_util.print_cluster_stats()

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        self.bucket_util.validate_docs_per_collections_all_buckets()

        self.bucket_util.print_bucket_stats()
        self.bucket_helper_obj = BucketHelper(self.cluster.master)
        self.log_setup_status("CollectionBase", "complete")

    def tearDown(self):
        self.bucket_util.remove_scope_collections_and_validate()
        super(CollectionBase, self).tearDown()

    def over_ride_template_params(self, target_spec):
        for over_ride_param in self.over_ride_spec_params:
            if over_ride_param == "num_items":
                target_spec[MetaConstants.NUM_ITEMS_PER_COLLECTION] = \
                    self.num_items
            elif over_ride_param == "durability":
                target_spec[MetaCrudParams.DURABILITY_LEVEL] = \
                    self.durability_level
