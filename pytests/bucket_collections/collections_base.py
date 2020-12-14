from math import ceil

from Cb_constants import CbServer, DocLoading
from basetestcase import BaseTestCase
from collections_helper.collections_spec_constants import \
    MetaConstants, MetaCrudParams
from couchbase_helper.durability_helper import DurabilityHelper
from membase.api.rest_client import RestConnection
from BucketLib.BucketOperations import BucketHelper
from BucketLib.bucket import Bucket
from remote.remote_util import RemoteMachineShellConnection
from cb_tools.cbstats import Cbstats

from java.lang import Exception as Java_base_exception


class CollectionBase(BaseTestCase):
    def setUp(self):
        super(CollectionBase, self).setUp()
        self.log_setup_status("CollectionBase", "started")

        self.MAX_SCOPES = CbServer.max_scopes
        self.MAX_COLLECTIONS = CbServer.max_collections
        self.key = 'test_collection'.rjust(self.key_size, '0')
        self.simulate_error = self.input.param("simulate_error", None)
        self.error_type = self.input.param("error_type", "memory")
        self.doc_ops = self.input.param("doc_ops", None)
        self.spec_name = self.input.param("bucket_spec",
                                          "single_bucket.default")
        self.data_spec_name = self.input.param("data_spec_name",
                                               "initial_load")
        self.remove_default_collection = \
            self.input.param("remove_default_collection", False)

        self.action_phase = self.input.param("action_phase",
                                             "before_default_load")
        self.skip_collections_cleanup = \
            self.input.param("skip_collections_cleanup", False)
        self.validate_docs_count_during_teardown = \
            self.input.param("validate_docs_count_during_teardown", False)
        self.batch_size = self.input.param("batch_size", 200)

        self.crud_batch_size = 100
        self.num_nodes_affected = 1
        if self.num_replicas > 1:
            self.num_nodes_affected = 2

        if self.doc_ops:
            self.doc_ops = self.doc_ops.split(';')

        self.durability_helper = DurabilityHelper(
            self.log, len(self.cluster.nodes_in_cluster),
            self.durability_level)

        services = None
        if self.services_init:
            services = list()
            for service in self.services_init.split("-"):
                services.append(service.replace(":", ","))
            services = services[1:] if len(services) > 1 else None

        # Initialize cluster using given nodes
        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []
        self.task.rebalance([self.cluster.master], nodes_init, [],
                            services=services)
        self.cluster.nodes_in_cluster.extend([self.cluster.master]+nodes_init)

        # Disable auto-failover to avoid failover of nodes
        status = RestConnection(self.cluster.master) \
            .update_autofailover_settings(False, 120, False)
        self.assertTrue(status, msg="Failure during disabling auto-failover")
        self.bucket_helper_obj = BucketHelper(self.cluster.master)

        try:
            self.collection_setup()
        except Java_base_exception as exception:
            self.handle_setup_exception(exception)
        except Exception as exception:
            self.handle_setup_exception(exception)
        self.supported_d_levels = \
            self.bucket_util.get_supported_durability_levels()
        self.log_setup_status("CollectionBase", "complete")

    def tearDown(self):
        shell = RemoteMachineShellConnection(self.cluster.master)
        cbstat_obj = Cbstats(shell)
        for bucket in self.bucket_util.buckets:
            result = cbstat_obj.all_stats(bucket.name)
            self.log.info("Bucket: %s, Active Resident ratio(DGM): %s%%"
                          % (bucket.name,
                             result["vb_active_perc_mem_resident"]))
            self.log.info("Bucket: %s, Replica Resident ratio(DGM): %s%%"
                          % (bucket.name,
                             result["vb_replica_perc_mem_resident"]))
            if not self.skip_collections_cleanup:
                self.bucket_util.remove_scope_collections_for_bucket(bucket)
        shell.disconnect()
        if self.validate_docs_count_during_teardown:
            self.bucket_util.validate_docs_per_collections_all_buckets()
        super(CollectionBase, self).tearDown()

    def collection_setup(self):
        self.bucket_util.add_rbac_user()

        # Create bucket(s) and add rbac user
        if self.bucket_storage == Bucket.StorageBackend.magma:
            # get the TTL value
            buckets_spec_from_conf = \
                self.bucket_util.get_bucket_template_from_package(
                    self.spec_name)
            bucket_ttl = buckets_spec_from_conf.get(Bucket.maxTTL, 0)
            # Blindly override the bucket spec if the backend storage is magma.
            # So, Bucket spec in conf file will not take any effect.
            self.spec_name = "single_bucket.bucket_for_magma_collections"
            magma_bucket_spec = \
                self.bucket_util.get_bucket_template_from_package(
                    self.spec_name)
            magma_bucket_spec[Bucket.maxTTL] = bucket_ttl
            buckets_spec = magma_bucket_spec
        else:
            buckets_spec = self.bucket_util.get_bucket_template_from_package(
                self.spec_name)
        doc_loading_spec = \
            self.bucket_util.get_crud_template_from_package(
                self.data_spec_name)

        # Process params to over_ride values if required
        self.over_ride_bucket_template_params(buckets_spec)
        self.over_ride_doc_loading_template_params(doc_loading_spec)

        self.bucket_util.create_buckets_using_json_data(buckets_spec)
        self.bucket_util.wait_for_collection_creation_to_complete()

        # Prints bucket stats before doc_ops
        self.bucket_util.print_bucket_stats()

        # Init sdk_client_pool if not initialized before
        if self.sdk_client_pool is None:
            self.init_sdk_pool_object()

        # Create clients in SDK client pool
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

        # TODO: remove this once the bug is fixed
        self.sleep(120, "MB-38497")

        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.bucket_util.buckets,
                doc_loading_spec,
                mutation_num=0,
                batch_size=self.batch_size)
        if doc_loading_task.result is False:
            self.fail("Initial doc_loading failed")

        self.cluster_util.print_cluster_stats()

        ttl_buckets = [
            "multi_bucket.buckets_for_rebalance_tests_with_ttl",
            "multi_bucket.buckets_all_membase_for_rebalance_tests_with_ttl",
            "volume_templates.buckets_for_volume_tests_with_ttl"]

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets()
        if self.spec_name not in ttl_buckets:
            self.bucket_util.validate_docs_per_collections_all_buckets()

        # Prints bucket stats after doc_ops
        self.bucket_util.print_bucket_stats()

    def over_ride_bucket_template_params(self, bucket_spec):
        if self.bucket_storage == Bucket.StorageBackend.magma:
            # Blindly override the following params
            bucket_spec[Bucket.evictionPolicy] = \
                Bucket.EvictionPolicy.FULL_EVICTION
        else:
            for key, val in self.input.test_params.items():
                if key == "replicas":
                    bucket_spec[Bucket.replicaNumber] = self.num_replicas
                elif key == "bucket_size":
                    bucket_spec[Bucket.ramQuotaMB] = self.bucket_size
                elif key == "num_items":
                    bucket_spec[MetaConstants.NUM_ITEMS_PER_COLLECTION] = \
                        self.num_items
                elif key == "remove_default_collection":
                    bucket_spec[MetaConstants.REMOVE_DEFAULT_COLLECTION] = \
                        self.input.param(key)
                elif key == "bucket_storage":
                    bucket_spec[Bucket.storageBackend] = self.bucket_storage

    def over_ride_doc_loading_template_params(self, target_spec):
        for key, value in self.input.test_params.items():
            if key == "durability":
                target_spec[MetaCrudParams.DURABILITY_LEVEL] = \
                    self.durability_level
            elif key == "sdk_timeout":
                target_spec[MetaCrudParams.SDK_TIMEOUT] = self.sdk_timeout
            elif key == "doc_size":
                target_spec[MetaCrudParams.DocCrud.DOC_SIZE] = self.doc_size

    def load_data_for_sub_doc_ops(self, verification_dict=None):
        new_data_load_template = \
            self.bucket_util.get_crud_template_from_package("initial_load")
        new_data_load_template[MetaCrudParams.DURABILITY_LEVEL] = ""
        new_data_load_template["doc_crud"][
            MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 100
        new_data_load_template["subdoc_crud"][
            MetaCrudParams.SubDocCrud.INSERT_PER_COLLECTION] = 50
        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.bucket_util.buckets,
                new_data_load_template,
                mutation_num=0,
                batch_size=self.batch_size)
        if doc_loading_task.result is False:
            self.fail("Extra doc loading task failed")

        if verification_dict:
            self.update_verification_dict_from_collection_task(
                verification_dict,
                doc_loading_task)

    def update_verification_dict_from_collection_task(self,
                                                      verification_dict,
                                                      doc_loading_task):
        for bucket, s_dict in doc_loading_task.loader_spec.items():
            for s_name, c_dict in s_dict["scopes"].items():
                for c_name, _ in c_dict["collections"].items():
                    c_crud_data = doc_loading_task.loader_spec[
                        bucket]["scopes"][
                        s_name]["collections"][c_name]
                    for op_type in c_crud_data.keys():
                        total_mutation = \
                            c_crud_data[op_type]["doc_gen"].end \
                            - c_crud_data[op_type]["doc_gen"].start
                        if op_type in DocLoading.Bucket.DOC_OPS:
                            verification_dict["ops_%s" % op_type] \
                                += total_mutation
                        elif op_type in DocLoading.Bucket.SUB_DOC_OPS:
                            verification_dict["ops_update"] \
                                += total_mutation
                        if c_crud_data[op_type]["durability_level"] \
                                in self.supported_d_levels:
                            verification_dict["sync_write_committed_count"] \
                                += total_mutation

    def validate_cruds_from_collection_mutation(self, doc_loading_task):
        # Read all the values to validate the CRUDs
        for bucket, s_dict in doc_loading_task.loader_spec.items():
            client = self.sdk_client_pool.get_client_for_bucket(bucket)
            for s_name, c_dict in s_dict["scopes"].items():
                for c_name, _ in c_dict["collections"].items():
                    c_crud_data = doc_loading_task.loader_spec[
                        bucket]["scopes"][
                        s_name]["collections"][c_name]
                    client.select_collection(s_name, c_name)
                    for op_type in c_crud_data.keys():
                        doc_gen = c_crud_data[op_type]["doc_gen"]
                        is_sub_doc = False
                        if op_type in DocLoading.Bucket.SUB_DOC_OPS:
                            is_sub_doc = True
                        task = self.task.async_validate_docs(
                            self.cluster, bucket,
                            doc_gen, op_type,
                            scope=s_name, collection=c_name,
                            batch_size=self.batch_size,
                            process_concurrency=self.process_concurrency,
                            sdk_client_pool=self.sdk_client_pool,
                            is_sub_doc=is_sub_doc)
                        self.task_manager.get_task_result(task)
