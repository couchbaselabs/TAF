from math import ceil

from Cb_constants import CbServer, DocLoading
from basetestcase import ClusterSetup
from collections_helper.collections_spec_constants import \
    MetaConstants, MetaCrudParams
from couchbase_helper.durability_helper import DurabilityHelper
from membase.api.rest_client import RestConnection
from BucketLib.BucketOperations import BucketHelper
from BucketLib.bucket import Bucket
from remote.remote_util import RemoteMachineShellConnection
from cb_tools.cbstats import Cbstats
from sdk_exceptions import SDKException
from java.lang import Exception as Java_base_exception


class CollectionBase(ClusterSetup):
    def setUp(self):
        super(CollectionBase, self).setUp()
        self.log_setup_status("CollectionBase", "started")

        self.key = 'test_collection'.rjust(self.key_size, '0')
        self.simulate_error = self.input.param("simulate_error", None)
        self.error_type = self.input.param("error_type", "memory")
        self.doc_ops = self.input.param("doc_ops", None)
        # If True, creates bucket/scope/collections with simpler names
        self.use_simple_names = self.input.param("use_simple_names", True)
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
        self.process_concurrency = self.input.param("process_concurrency", 1)
        self.retry_get_process_num = \
            self.input.param("retry_get_process_num", 200)
        self.change_magma_quota = self.input.param("change_magma_quota", False)
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
        status = RestConnection(self.cluster.master) \
            .update_autofailover_settings(False, 120)
        self.assertTrue(status, msg="Failure during disabling auto-failover")
        self.bucket_helper_obj = BucketHelper(self.cluster.master)
        self.disk_optimized_thread_settings = self.input.param("disk_optimized_thread_settings", False)
        if self.disk_optimized_thread_settings:
            self.set_num_writer_and_reader_threads(num_writer_threads="disk_io_optimized",
                                                   num_reader_threads="disk_io_optimized")

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
        cbstat_obj = Cbstats(self.cluster.master)
        for bucket in self.cluster.buckets:
            if bucket.bucketType != Bucket.Type.MEMCACHED:
                result = cbstat_obj.all_stats(bucket.name)
                self.log.info("Bucket: %s, Active Resident ratio(DGM): %s%%"
                              % (bucket.name,
                                 result["vb_active_perc_mem_resident"]))
                self.log.info("Bucket: %s, Replica Resident ratio(DGM): %s%%"
                              % (bucket.name,
                                 result["vb_replica_perc_mem_resident"]))
            if not self.skip_collections_cleanup \
                    and bucket.bucketType != Bucket.Type.MEMCACHED:
                self.bucket_util.remove_scope_collections_for_bucket(
                    self.cluster, bucket)
        if self.validate_docs_count_during_teardown:
            self.bucket_util.validate_docs_per_collections_all_buckets(
                self.cluster)
        if self.disk_optimized_thread_settings:
            self.set_num_writer_and_reader_threads(num_writer_threads="default",
                                                   num_reader_threads="default")
        super(CollectionBase, self).tearDown()

    @staticmethod
    def create_sdk_clients(num_threads, master,
                           buckets, sdk_client_pool, sdk_compression):
        # Fetch num_collections per bucket. Used for 'req_clients' calc
        cols_in_bucket = dict()
        for bucket in buckets:
            collections_in_bucket = 0
            for _, scope in bucket.scopes.items():
                for _, _ in scope.collections.items():
                    collections_in_bucket += 1
            cols_in_bucket[bucket.name] = collections_in_bucket

        # Create clients in SDK client pool
        bucket_count = len(buckets)
        max_clients = num_threads
        clients_per_bucket = int(ceil(max_clients / bucket_count))

        for bucket in buckets:
            req_clients = min(cols_in_bucket[bucket.name], clients_per_bucket)
            if CbServer.cluster_profile == "serverless":
                # Work around to hitting the following error on serverless config
                # Memcached error #50: RATE_LIMITED_MAX_CONNECTIONS
                req_clients = min(req_clients, 10)
            sdk_client_pool.create_clients(
                bucket=bucket, servers=[master],
                req_clients=req_clients,
                compression_settings=sdk_compression)

    def collection_setup(self):
        self.bucket_util.add_rbac_user(self.cluster.master)

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
        buckets_spec[MetaConstants.USE_SIMPLE_NAMES] = self.use_simple_names

        doc_loading_spec = \
            self.bucket_util.get_crud_template_from_package(
                self.data_spec_name)

        # Process params to over_ride values if required
        self.over_ride_bucket_template_params(buckets_spec)
        self.over_ride_doc_loading_template_params(doc_loading_spec)
        self.set_retry_exceptions_for_initial_data_load(doc_loading_spec)
        self.bucket_util.create_buckets_using_json_data(self.cluster,
                                                        buckets_spec)
        self.bucket_util.wait_for_collection_creation_to_complete(self.cluster)

        # Prints bucket stats before doc_ops
        self.bucket_util.print_bucket_stats(self.cluster)

        # Change magma quota only for bloom filter testing
        if self.change_magma_quota:
            bucket_helper = BucketHelper(self.cluster.master)
            bucket_helper.set_magma_quota_percentage()
            self.sleep(30, "Sleep while magma storage quota setting is taking effect")

        # Init sdk_client_pool if not initialized before
        if self.sdk_client_pool is None:
            self.init_sdk_pool_object()

        self.log.info("Creating required SDK clients for client_pool")
        self.create_sdk_clients(self.task_manager.number_of_threads,
                                self.cluster.master,
                                self.cluster.buckets,
                                self.sdk_client_pool,
                                self.sdk_compression)

        doc_loading_task = \
            self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.cluster.buckets,
                doc_loading_spec,
                mutation_num=0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency)
        if doc_loading_task.result is False:
            self.fail("Initial doc_loading failed")

        self.cluster_util.print_cluster_stats(self.cluster)

        ttl_buckets = [
            "multi_bucket.buckets_for_rebalance_tests_with_ttl",
            "multi_bucket.buckets_all_membase_for_rebalance_tests_with_ttl",
            "volume_templates.buckets_for_volume_tests_with_ttl",
            "magma_dgm.5_percent_dgm.5_node_2_replica_magma_ttl_256",
            "magma_dgm.5_percent_dgm.5_node_2_replica_magma_ttl_512",
            "magma_dgm.5_percent_dgm.5_node_2_replica_magma_ttl_1024",
            "magma_dgm.5_percent_dgm.5_node_1_replica_magma_ttl_256_single_bucket",
            "magma_dgm.5_percent_dgm.5_node_1_replica_magma_ttl_512_single_bucket",
            "magma_dgm.10_percent_dgm.5_node_2_replica_magma_ttl_256",
            "magma_dgm.10_percent_dgm.5_node_2_replica_magma_ttl_512",
            "magma_dgm.10_percent_dgm.5_node_2_replica_magma_ttl_1024",
            "magma_dgm.20_percent_dgm.5_node_2_replica_magma_ttl_256",
            "magma_dgm.20_percent_dgm.5_node_2_replica_magma_ttl_512",
            "magma_dgm.20_percent_dgm.5_node_2_replica_magma_ttl_1024",
            "magma_dgm.40_percent_dgm.5_node_2_replica_magma_ttl_512",
            "magma_dgm.80_percent_dgm.5_node_2_replica_magma_ttl_512",
        ]

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets,
                                                     timeout=1200)
        if self.spec_name not in ttl_buckets:
            self.bucket_util.validate_docs_per_collections_all_buckets(
                self.cluster, timeout=2400)

        # Prints bucket stats after doc_ops
        self.bucket_util.print_bucket_stats(self.cluster)

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
                elif key == "compression_mode":
                    bucket_spec[Bucket.compressionMode] = self.compression_mode
                elif key == "flushEnabled":
                    bucket_spec[Bucket.flushEnabled] = int(self.flush_enabled)
                elif key == "bucket_type":
                    bucket_spec[Bucket.bucketType] = self.bucket_type

    def over_ride_doc_loading_template_params(self, target_spec):
        for key, value in self.input.test_params.items():
            if key == "durability":
                target_spec[MetaCrudParams.DURABILITY_LEVEL] = \
                    self.durability_level
            elif key == "sdk_timeout":
                target_spec[MetaCrudParams.SDK_TIMEOUT] = self.sdk_timeout
            elif key == "doc_size":
                target_spec["doc_crud"][MetaCrudParams.DocCrud.DOC_SIZE] \
                    = self.doc_size
            elif key == "randomize_value":
                target_spec["doc_crud"][MetaCrudParams.DocCrud.RANDOMIZE_VALUE] \
                    = self.randomize_value

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
                self.cluster.buckets,
                new_data_load_template,
                mutation_num=0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency)
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

    def set_retry_exceptions_for_initial_data_load(self, doc_loading_spec):
        retry_exceptions = list()
        retry_exceptions.append(SDKException.AmbiguousTimeoutException)
        retry_exceptions.append(SDKException.TimeoutException)
        retry_exceptions.append(SDKException.RequestCanceledException)
        retry_exceptions.append(SDKException.DocumentNotFoundException)
        retry_exceptions.append(SDKException.ServerOutOfMemoryException)
        if self.durability_level:
            retry_exceptions.append(SDKException.DurabilityAmbiguousException)
            retry_exceptions.append(SDKException.DurabilityImpossibleException)
        doc_loading_spec[MetaCrudParams.RETRY_EXCEPTIONS] = retry_exceptions

    def set_num_writer_and_reader_threads(self, num_writer_threads="default", num_reader_threads="default",
                                          num_storage_threads="default"):
        bucket_helper = BucketHelper(self.cluster.master)
        bucket_helper.update_memcached_settings(num_writer_threads=num_writer_threads,
                                                num_reader_threads=num_reader_threads,
                                                num_storage_threads=num_storage_threads)

    def set_allowed_hosts(self):
        """ First operation will fail and the second operation will succeed"""
        allowedhosts = "[\"*.couchbase.com\",\"10.112.0.0/16\",\"172.23.0.0/16\"]"
        host = "[\"*.couchbase.com\"]"
        for node in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(node)
            output = shell.set_allowedhosts("localhost", self.cluster.master.rest_username,
                                            self.cluster.master.rest_password, host)
            self.log.info("expected failure from set_allowedhosts {0}".format(output))
            if "errors" not in output[0]:
                self.fail("Invalid address should fail, address {0}".format(host))
            output = shell.set_allowedhosts("localhost", self.cluster.master.rest_username,
                                            self.cluster.master.rest_password, allowedhosts)
            if len(output) > 2:
                self.fail("Allowed hosts is not changed and error is {0}".format(output))
            output = shell.get_allowedhosts(self.cluster.master.rest_username,
                                            self.cluster.master.rest_password)
            self.assertEqual(output, allowedhosts)
            shell.disconnect()
