import json
import os
from math import ceil

from BucketLib.BucketOperations import BucketHelper
from Cb_constants import CbServer
from basetestcase import BaseTestCase
from bucket_utils.bucket_ready_functions import BucketUtils
from couchbase_helper.document import View
from couchbase_helper.documentgenerator import doc_generator
from couchbase_helper.durability_helper import DurabilityHelper
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from sdk_exceptions import SDKException
from BucketLib.bucket import Bucket
from collections_helper.collections_spec_constants import \
    MetaConstants, MetaCrudParams

from java.lang import Exception as Java_base_exception

retry_exceptions = list([SDKException.AmbiguousTimeoutException,
                         SDKException.DurabilityImpossibleException,
                         SDKException.DurabilityAmbiguousException,
                         SDKException.TimeoutException,
                         SDKException.ServerOutOfMemoryException,
                         SDKException.DocumentNotFoundException])


class RebalanceBaseTest(BaseTestCase):
    def setUp(self):
        super(RebalanceBaseTest, self).setUp()
        self.rest = RestConnection(self.cluster.master)
        self.doc_ops = self.input.param("doc_ops", "create")
        self.key_size = self.input.param("key_size", 0)
        self.zone = self.input.param("zone", 1)
        self.replica_to_update = self.input.param("new_replica", None)
        self.default_view_name = "default_view"
        self.defaul_map_func = "function (doc) {\n  emit(doc._id, doc);\n}"
        self.default_view = View(self.default_view_name, self.defaul_map_func,
                                 None)
        self.max_verify = self.input.param("max_verify", None)
        self.std_vbucket_dist = self.input.param("std_vbucket_dist", None)
        self.flusher_total_batch_limit = self.input.param(
            "flusher_total_batch_limit", None)
        self.test_abort_snapshot = self.input.param("test_abort_snapshot",
                                                    False)
        self.items = self.num_items
        self.logs_folder = self.input.param("logs_folder")
        self.retry_get_process_num = self.input.param("retry_get_process_num", 200)

        nodes_init = self.cluster.servers[1:self.nodes_init] \
            if self.nodes_init != 1 else []

        if not self.cluster.cloud_cluster:
            node_ram_ratio = self.bucket_util.base_bucket_ratio(
                self.cluster.servers)
            info = self.rest.get_nodes_self()
            self.rest.init_cluster(username=self.cluster.master.rest_username,
                                   password=self.cluster.master.rest_password)
            kv_mem_quota = int(info.mcdMemoryReserved*node_ram_ratio)
            self.rest.set_service_mem_quota(
                {CbServer.Settings.KV_MEM_QUOTA: kv_mem_quota})
            self.bucket_util.add_rbac_user(self.cluster.master)

            services = None
            if self.services_init:
                services = list()
                for service in self.services_init.split("-"):
                    services.append(service.replace(":", ","))
                services = services[1:] if len(services) > 1 else None

            if nodes_init:
                result = self.task.rebalance(
                    self.cluster, nodes_init, [], services=services,
                    retry_get_process_num=self.retry_get_process_num)
                self.assertTrue(result, "Initial rebalance failed")

        self.check_temporary_failure_exception = False
        self.cluster.nodes_in_cluster.extend([self.cluster.master])
        self.check_replica = self.input.param("check_replica", False)
        self.spec_name = self.input.param("bucket_spec", None)
        self.disk_optimized_thread_settings = self.input.param("disk_optimized_thread_settings", False)
        if self.disk_optimized_thread_settings:
            self.set_num_writer_and_reader_threads(num_writer_threads="disk_io_optimized",
                                                   num_reader_threads="disk_io_optimized")
        # Buckets creation and initial data load done by bucket_spec
        if self.spec_name is not None:
            try:
                self.collection_setup()
            except Java_base_exception as exception:
                self.handle_setup_exception(exception)
            except Exception as exception:
                self.handle_setup_exception(exception)
        else:
            if self.standard_buckets > 10:
                self.bucket_util.change_max_buckets(self.cluster.master,
                                                    self.standard_buckets)
            self.create_buckets(self.bucket_size)

            # Create Scope/Collection based on inputs given
            for bucket in self.cluster.buckets:
                if self.scope_name != CbServer.default_scope:
                    self.scope_name = BucketUtils.get_random_name()
                    BucketUtils.create_scope(self.cluster.master,
                                             bucket,
                                             {"name": self.scope_name})
                if self.collection_name != CbServer.default_collection:
                    self.collection_name = BucketUtils.get_random_name()
                    BucketUtils.create_collection(
                        self.cluster.master,
                        bucket,
                        self.scope_name,
                        {"name": self.collection_name,
                         "num_items": self.num_items})
                    self.log.info(
                        "Bucket %s using scope::collection - '%s::%s'"
                        % (bucket.name, self.scope_name, self.collection_name))

                # Update required num_items under default collection
                bucket.scopes[self.scope_name] \
                    .collections[self.collection_name] \
                    .num_items = self.num_items

            if self.flusher_total_batch_limit:
                self.bucket_util.set_flusher_total_batch_limit(
                    self.cluster, self.cluster.buckets,
                    self.flusher_total_batch_limit)

            self.gen_create = self.get_doc_generator(0, self.num_items)
            if self.active_resident_threshold < 100:
                self.check_temporary_failure_exception = True
                # Reset num_items=0 since the num_items will be populated
                # by the DGM load task
                for bucket in self.cluster.buckets:
                    bucket.scopes[self.scope_name] \
                        .collections[self.collection_name] \
                        .num_items = 0

            # Create clients in SDK client pool
            if self.sdk_client_pool:
                self.log.info("Creating SDK clients for client_pool")
                for bucket in self.cluster.buckets:
                    self.sdk_client_pool.create_clients(
                        bucket,
                        [self.cluster.master],
                        self.sdk_pool_capacity,
                        compression_settings=self.sdk_compression)

            if not self.atomicity:
                _ = self._load_all_buckets(self.cluster, self.gen_create,
                                           "create", 0,
                                           batch_size=self.batch_size)
                self.log.info("Verifying num_items counts after doc_ops")
                self.bucket_util._wait_for_stats_all_buckets(
                    self.cluster, self.cluster.buckets, timeout=1200)
                self.bucket_util.validate_docs_per_collections_all_buckets(
                    self.cluster,
                    timeout=self.wait_timeout)
            else:
                self.transaction_commit = True
                self._load_all_buckets_atomicty(self.gen_create, "create")
                self.transaction_commit = self.input.param(
                    "transaction_commit", True)

            # Initialize doc_generators
            self.active_resident_threshold = 100
            self.gen_create = None
            self.gen_delete = None
            self.gen_update = self.get_doc_generator(0, (self.items / 2))
            self.durability_helper = DurabilityHelper(
                self.log, len(self.cluster.nodes_in_cluster),
                durability=self.durability_level,
                replicate_to=self.replicate_to, persist_to=self.persist_to)
            self.cluster_util.print_cluster_stats(self.cluster)
            self.bucket_util.print_bucket_stats(self.cluster)
        self.log_setup_status("RebalanceBase", "complete")

    def _create_default_bucket(self, bucket_size):
        if bucket_size:
            available_ram = bucket_size
        else:
            node_ram_ratio = self.bucket_util.base_bucket_ratio(self.servers)
            info = RestConnection(self.cluster.master).get_nodes_self()
            available_ram = int(info.memoryQuota * node_ram_ratio)
            if available_ram < 100 or self.active_resident_threshold < 100:
                available_ram = 100
        self.bucket_util.create_default_bucket(
            self.cluster,
            ram_quota=available_ram,
            bucket_type=self.bucket_type,
            replica=self.num_replicas,
            storage=self.bucket_storage,
            eviction_policy=self.bucket_eviction_policy,
            bucket_durability=self.bucket_durability_level)

    def _create_multiple_buckets(self):
        buckets_created = self.bucket_util.create_multiple_buckets(
            self.cluster,
            self.num_replicas,
            bucket_count=self.standard_buckets,
            bucket_type=self.bucket_type,
            storage=self.bucket_storage,
            eviction_policy=self.bucket_eviction_policy,
            bucket_durability=self.bucket_durability_level)
        self.assertTrue(buckets_created, "Unable to create multiple buckets")

        for bucket in self.cluster.buckets:
            self.assertTrue(self.bucket_util._wait_warmup_completed(
                bucket, servers=self.cluster_util.get_kv_nodes(self.cluster)))

    def create_buckets(self, bucket_size):
        if self.standard_buckets == 1:
            self._create_default_bucket(bucket_size)
        else:
            self._create_multiple_buckets()

    def tearDown(self):
        self.cluster_util.print_cluster_stats(self.cluster)
        if self.disk_optimized_thread_settings:
            self.set_num_writer_and_reader_threads(num_writer_threads="default",
                                                   num_reader_threads="default")
        super(RebalanceBaseTest, self).tearDown()

    def collection_setup(self):
        # If True, creates bucket/scope/collections with simpler names
        self.use_simple_names = self.input.param("use_simple_names", True)
        self.over_ride_spec_params = self.input.param("override_spec_params", "").split(";")
        self.log.info("Creating buckets from spec")
        # Create bucket(s)
        if CbServer.cluster_profile == "default" and self.bucket_storage == \
                Bucket.StorageBackend.magma:
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
            self.bucket_util.get_crud_template_from_package("initial_load")

        buckets_spec[MetaConstants.USE_SIMPLE_NAMES] = self.use_simple_names
        # Process params to over_ride values if required
        self.over_ride_bucket_template_params(buckets_spec)
        self.over_ride_doc_loading_template_params(doc_loading_spec)
        self.set_retry_exceptions_for_initial_data_load(doc_loading_spec)
        self.bucket_util.create_buckets_using_json_data(self.cluster,
                                                        buckets_spec)
        self.bucket_util.wait_for_collection_creation_to_complete(self.cluster)

        # Prints bucket stats before doc_ops
        self.bucket_util.print_bucket_stats(self.cluster)
        # Init sdk_client_pool if not initialized before
        if self.sdk_client_pool is None:
            self.init_sdk_pool_object()

        # Create clients in SDK client pool
        if self.sdk_client_pool:
            self.log.info("Creating required SDK clients for client_pool")
            bucket_count = len(self.cluster.buckets)
            max_clients = self.task_manager.number_of_threads
            clients_per_bucket = int(ceil(max_clients / bucket_count))
            for bucket in self.cluster.buckets:
                self.sdk_client_pool.create_clients(
                    bucket,
                    [self.cluster.master],
                    clients_per_bucket,
                    compression_settings=self.sdk_compression)

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

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets,
                                                     timeout=1200)
        self.bucket_util.validate_docs_per_collections_all_buckets(
            self.cluster,
            timeout=self.wait_timeout)

        self.cluster_util.print_cluster_stats(self.cluster)
        self.bucket_util.print_bucket_stats(self.cluster)
        self.bucket_helper_obj = BucketHelper(self.cluster.master)

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

    def set_num_writer_and_reader_threads(self, num_writer_threads="default", num_reader_threads="default",
                                          num_storage_threads="default"):
        bucket_helper = BucketHelper(self.cluster.master)
        bucket_helper.update_memcached_settings(num_writer_threads=num_writer_threads,
                                                num_reader_threads=num_reader_threads,
                                                num_storage_threads=num_storage_threads)

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

    def shuffle_nodes_between_zones_and_rebalance(self, to_remove=None):
        """
        Shuffle the nodes present in the cluster if zone > 1.
        Rebalance the nodes in the end.
        Nodes are divided into groups iteratively. i.e: 1st node in Group 1,
        2nd in Group 2, 3rd in Group 1 & so on, when zone=2
        :param to_remove: List of nodes to be removed.
        """
        if not to_remove:
            to_remove = []
        serverinfo = self.servers[0]
        rest = RestConnection(serverinfo)
        zones = ["Group 1"]
        nodes_in_zone = {"Group 1": [serverinfo.ip]}
        # Create zones, if not existing, based on params zone in test.
        # Shuffle the nodes between zones.
        if int(self.zone) > 1:
            for i in range(1, int(self.zone)):
                a = "Group "
                zones.append(a + str(i + 1))
                if not rest.is_zone_exist(zones[i]):
                    rest.add_zone(zones[i])
                nodes_in_zone[zones[i]] = []
            # Divide the nodes between zones.
            nodes_in_cluster = \
                [node.ip for node in self.cluster_util.get_nodes_in_cluster(
                    self.cluster)]
            nodes_to_remove = [node.ip for node in to_remove]
            for i in range(1, len(self.servers)):
                if self.servers[i].ip in nodes_in_cluster \
                        and self.servers[i].ip not in nodes_to_remove:
                    server_group = i % int(self.zone)
                    nodes_in_zone[zones[server_group]].append(self.servers[i].ip)
            # Shuffle the nodesS
            for i in range(1, self.zone):
                node_in_zone = list(set(nodes_in_zone[zones[i]]) -
                                    set([node for node in rest.get_nodes_in_zone(zones[i])]))
                rest.shuffle_nodes_in_zones(node_in_zone, zones[0], zones[i])
        otpnodes = [node.id for node in rest.node_statuses()]
        nodes_to_remove = [node.id for node in rest.node_statuses()
                           if node.ip in [t.ip for t in to_remove]]
        # Start rebalance and monitor it.
        started = rest.rebalance(otpNodes=otpnodes,
                                 ejectedNodes=nodes_to_remove)
        if started:
            result = rest.monitorRebalance()
            self.assertTrue(result, msg="Rebalance failed{}".format(result))
            msg = "successfully rebalanced cluster {0}"
            self.log.info(msg.format(result))
        # Verify replicas of one node should not be in the same zone
        # as active vbuckets of the node.
        if self.zone > 1:
            self.cluster_util.verify_replica_distribution_in_zones(
                self.cluster, nodes_in_zone)

    def add_remove_servers_and_rebalance(self, to_add, to_remove):
        """
        Add and/or remove servers and rebalance.
        :param to_add: List of nodes to be added.
        :param to_remove: List of nodes to be removed.
        """
        serverinfo = self.cluster.master
        rest = RestConnection(serverinfo)
        for node in to_add:
            rest.add_node(user=serverinfo.rest_username,
                          password=serverinfo.rest_password,
                          remoteIp=node.ip)
        self.shuffle_nodes_between_zones_and_rebalance(to_remove)
        self.cluster.nodes_in_cluster = \
            list(set(self.cluster.nodes_in_cluster + to_add) - set(to_remove))

    def get_doc_generator(self, start, end):
        return doc_generator(self.key, start, end,
                             doc_size=self.doc_size,
                             doc_type=self.doc_type,
                             target_vbucket=self.target_vbucket,
                             vbuckets=self.cluster.vbuckets,
                             key_size=self.key_size,
                             randomize_doc_size=self.randomize_doc_size,
                             randomize_value=self.randomize_value,
                             mix_key_size=self.mix_key_size)

    def _load_all_buckets(self, cluster, kv_gen, op_type, exp, flag=0,
                          batch_size=1000,
                          timeout_secs=30, compression=True):
        retry_exceptions_local = retry_exceptions \
                                 + [SDKException.RequestCanceledException]

        tasks_info = self.bucket_util.sync_load_all_buckets(
            cluster, kv_gen, op_type, exp, flag,
            persist_to=self.persist_to, replicate_to=self.replicate_to,
            durability=self.durability_level, timeout_secs=timeout_secs,
            batch_size=batch_size,
            sdk_compression=compression,
            process_concurrency=self.process_concurrency,
            retry_exceptions=retry_exceptions_local,
            active_resident_threshold=self.active_resident_threshold,
            scope=self.scope_name,
            collection=self.collection_name,
            sdk_client_pool=self.sdk_client_pool)
        if self.active_resident_threshold < 100:
            for task, _ in tasks_info.items():
                self.num_items = task.doc_index
        self.assertTrue(self.bucket_util.doc_ops_tasks_status(tasks_info),
                        "Doc_ops failed in rebalance_base._load_all_buckets")
        return tasks_info

    def _load_all_buckets_atomicty(self, kv_gen, op_type):
        task = self.task.async_load_gen_docs_atomicity(
            self.cluster, self.cluster.buckets, kv_gen, op_type, 0,
            batch_size=10,
            process_concurrency=8,
            replicate_to=self.replicate_to,
            persist_to=self.persist_to,
            timeout_secs=self.sdk_timeout,
            retries=self.sdk_retries,
            transaction_timeout=self.transaction_timeout,
            commit=self.transaction_commit,
            durability=self.durability_level,
            sync=self.sync)
        self.task.jython_task_manager.get_task_result(task)

    def start_parallel_cruds_atomicity(self, sync=True,
                                       task_verification=True):
        tasks_info = dict()
        if "update" in self.doc_ops:
            tasks_info.update(
                {self.task.async_load_gen_docs_atomicity(
                    self.cluster, self.cluster.buckets, self.gen_update,
                    "rebalance_only_update", 0, batch_size=self.batch_size,
                    process_concurrency=self.process_concurrency,
                    replicate_to=self.replicate_to, persist_to=self.persist_to,
                    timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                    transaction_timeout=self.transaction_timeout,
                    update_count=self.update_count,
                    commit=self.transaction_commit,
                    durability=self.durability_level, sync=sync,
                    defer=self.defer): None})
        if "create" in self.doc_ops:
            tasks_info.update(
                {self.task.async_load_gen_docs_atomicity(
                    self.cluster, self.cluster.buckets, self.gen_create,
                    "create", 0, batch_size=self.batch_size,
                    process_concurrency=self.process_concurrency,
                    replicate_to=self.replicate_to, persist_to=self.persist_to,
                    timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                    transaction_timeout=self.transaction_timeout,
                    commit=self.transaction_commit,
                    durability=self.durability_level,
                    sync=sync, defer=self.defer): None})
        if "delete" in self.doc_ops:
            tasks_info.update(
                {self.task.async_load_gen_docs_atomicity(
                    self.cluster, self.cluster.buckets, self.gen_delete,
                    "rebalance_delete", 0, batch_size=self.batch_size,
                    process_concurrency=self.process_concurrency,
                    replicate_to=self.replicate_to, persist_to=self.persist_to,
                    timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                    transaction_timeout=self.transaction_timeout,
                    commit=self.transaction_commit,
                    durability=self.durability_level,
                    sync=sync, defer=self.defer): None})

        if task_verification:
            for task in tasks_info.keys():
                self.task.jython_task_manager.get_task_result(task)

        return tasks_info

    def start_parallel_cruds(self, retry_exceptions=[], ignore_exceptions=[],
                             task_verification=False):
        tasks_info = dict()
        if "update" in self.doc_ops:
            tem_tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, self.gen_update, "update", 0,
                batch_size=self.batch_size,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                process_concurrency=self.process_concurrency,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                retry_exceptions=retry_exceptions,
                ignore_exceptions=ignore_exceptions,
                scope=self.scope_name, collection=self.collection_name,
                sdk_client_pool=self.sdk_client_pool)
            tasks_info.update(tem_tasks_info.items())
        if "create" in self.doc_ops:
            tem_tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, self.gen_create, "create", 0,
                batch_size=self.batch_size,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                process_concurrency=self.process_concurrency,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                retry_exceptions=retry_exceptions,
                ignore_exceptions=ignore_exceptions,
                scope=self.scope_name, collection=self.collection_name,
                sdk_client_pool=self.sdk_client_pool)
            tasks_info.update(tem_tasks_info.items())
            self.num_items += (self.gen_create.end - self.gen_create.start)
            for bucket in self.cluster.buckets:
                bucket \
                    .scopes[self.scope_name] \
                    .collections[self.collection_name] \
                    .num_items += (self.gen_create.end - self.gen_create.start)
        if "delete" in self.doc_ops:
            tem_tasks_info = self.bucket_util._async_load_all_buckets(
                self.cluster, self.gen_delete, "delete", 0,
                batch_size=self.batch_size,
                persist_to=self.persist_to, replicate_to=self.replicate_to,
                process_concurrency=self.process_concurrency,
                durability=self.durability_level,
                timeout_secs=self.sdk_timeout, retries=self.sdk_retries,
                retry_exceptions=retry_exceptions,
                ignore_exceptions=ignore_exceptions,
                scope=self.scope_name, collection=self.collection_name,
                sdk_client_pool=self.sdk_client_pool)
            tasks_info.update(tem_tasks_info.items())
            for bucket in self.cluster.buckets:
                bucket \
                    .scopes[self.scope_name] \
                    .collections[self.collection_name] \
                    .num_items -= (self.gen_delete.end - self.gen_delete.start)
            self.num_items -= (self.gen_delete.end - self.gen_delete.start)

        if task_verification:
            # Wait for tasks to complete and then verify
            for task in tasks_info:
                self.task_manager.get_task_result(task)

            self.bucket_util.verify_doc_op_task_exceptions(
                tasks_info, self.cluster,
                sdk_client_pool=self.sdk_client_pool)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)

        return tasks_info

    def loadgen_docs(self, retry_exceptions=[], ignore_exceptions=[],
                     task_verification=False):
        retry_exceptions = \
            list(set(retry_exceptions +
                     [SDKException.AmbiguousTimeoutException,
                      SDKException.RequestCanceledException,
                      SDKException.DurabilityImpossibleException,
                      SDKException.DurabilityAmbiguousException]))
        if self.check_temporary_failure_exception:
            retry_exceptions.append(SDKException.TemporaryFailureException)
        if self.atomicity:
            loaders = self.start_parallel_cruds_atomicity(self.sync,
                                                          task_verification)
        else:
            loaders = self.start_parallel_cruds(retry_exceptions,
                                                ignore_exceptions,
                                                task_verification)
        return loaders

    def induce_rebalance_test_condition(self, test_failure_condition):
        if test_failure_condition == "verify_replication":
            set_command = "testconditions:set(verify_replication, {fail, \"" + "default" + "\"})"
        elif test_failure_condition == "backfill_done":
            set_command = "testconditions:set(backfill_done, {for_vb_move, \"" + "default\", 1 , " + "fail})"
        else:
            set_command = "testconditions:set({0}, fail)" \
                .format(test_failure_condition)
        for server in self.servers:
            rest = RestConnection(server)
            shell = RemoteMachineShellConnection(server)
            shell.enable_diag_eval_on_non_local_hosts()
            _, content = rest.diag_eval(set_command)
            self.log.debug("Set Command: {0} Return: {1}"
                           .format(set_command, content))
            shell.disconnect()

    def start_rebalance(self, rebalance_operation):
        self.log.debug("Starting rebalance operation of type: {0}"
                       .format(rebalance_operation))
        if rebalance_operation == "rebalance_out":
            task = self.task.async_rebalance(
                self.servers[:self.nodes_init], [],
                [self.servers[self.nodes_init - 1]],
                retry_get_process_num=self.retry_get_process_num)
        elif rebalance_operation == "rebalance_in":
            task = self.task.async_rebalance(
                self.servers[:self.nodes_init],
                [self.servers[self.nodes_init]], [],
                retry_get_process_num=self.retry_get_process_num)
        elif rebalance_operation == "swap_rebalance":
            self.rest.add_node(self.cluster.master.rest_username,
                               self.cluster.master.rest_password,
                               self.servers[self.nodes_init].ip,
                               self.servers[self.nodes_init].port)
            task = self.task.async_rebalance(
                self.servers[:self.nodes_init], [],
                [self.servers[self.nodes_init - 1]],
                retry_get_process_num=self.retry_get_process_num)
        elif rebalance_operation == "graceful_failover":
            task = self.task.async_failover([self.cluster.master],
                                            failover_nodes=[self.servers[1]],
                                            graceful=True,
                                            wait_for_pending=300)
        return task

    def delete_rebalance_test_condition(self, test_failure_condition):
        delete_command = "testconditions:delete({0})".format(test_failure_condition)
        for server in self.servers:
            rest = RestConnection(server)
            shell = RemoteMachineShellConnection(server)
            shell.enable_diag_eval_on_non_local_hosts()
            _, content = rest.diag_eval(delete_command)
            self.log.debug("Delete Command: {0} Return: {1}"
                           .format(delete_command, content))
            shell.disconnect()

    def check_retry_rebalance_succeeded(self):
        self.sleep(30)
        attempts_remaining = retry_rebalance = retry_after_secs = None
        for i in range(10):
            self.log.info("Getting stats : try {0}".format(i))
            result = json.loads(self.rest.get_pending_rebalance_info())
            self.log.info(result)
            if "retry_after_secs" in result:
                retry_after_secs = result["retry_after_secs"]
                attempts_remaining = result["attempts_remaining"]
                retry_rebalance = result["retry_rebalance"]
                break
            self.sleep(5)
        self.log.debug("Attempts remaining: {0}, Retry rebalance: {1}"
                       .format(attempts_remaining, retry_rebalance))
        while attempts_remaining:
            # wait for the afterTimePeriod for the failed rebalance to restart
            self.sleep(retry_after_secs,
                       message="Waiting for the afterTimePeriod to complete")
            try:
                result = self.rest.monitorRebalance()
                msg = "monitoring rebalance {0}"
                self.log.debug(msg.format(result))
            except Exception:
                result = json.loads(self.rest.get_pending_rebalance_info())
                self.log.debug(result)
                try:
                    attempts_remaining = result["attempts_remaining"]
                    retry_rebalance = result["retry_rebalance"]
                    retry_after_secs = result["retry_after_secs"]
                except KeyError:
                    self.fail("Retrying of rebalance still did not help. "
                              "All the retries exhausted...")
                self.log.info("Attempts remaining: {0}, Retry rebalance: {1}"
                              .format(attempts_remaining, retry_rebalance))
            else:
                self.log.info("Retry rebalanced fixed the rebalance failure")
                break

    def change_retry_rebalance_settings(self, enabled=True,
                                        afterTimePeriod=300, maxAttempts=1):
        # build the body
        body = dict()
        if enabled:
            body["enabled"] = "true"
        else:
            body["enabled"] = "false"
        body["afterTimePeriod"] = afterTimePeriod
        body["maxAttempts"] = maxAttempts
        rest = RestConnection(self.cluster.master)
        rest.set_retry_rebalance_settings(body)
        result = rest.get_retry_rebalance_settings()
        self.log.debug("Retry rebalance settings changed to {0}"
                       .format(json.loads(result)))

    def reset_retry_rebalance_settings(self):
        body = dict()
        body["enabled"] = "false"
        rest = RestConnection(self.cluster.master)
        rest.set_retry_rebalance_settings(body)
        self.log.debug("Retry Rebalance settings reset ....")

    def cbcollect_info(self, trigger=True, validate=True,
                       known_failures=dict()):
        rest = RestConnection(self.cluster.master)
        nodes = rest.get_nodes()
        if trigger:
            status = self.cluster_util.trigger_cb_collect_on_cluster(rest,
                                                                     nodes)
            if status is False:
                self.fail("API perform_cb_collect returned False")
        if validate:
            status = self.cluster_util.wait_for_cb_collect_to_complete(rest, retry_count=1200)
            if status is False:
                self.fail("cb_collect timed out")

            cb_collect_response = rest.ns_server_tasks("clusterLogsCollection")
            per_node_data = cb_collect_response["perNode"]
            skip_node_ips = list()
            for node, reason in known_failures.items():
                if reason in ["in_node", "out_node"]:
                    skip_node_ips.append(node)
                    continue
                n_key = "ns_1@" + node
                if n_key in per_node_data:
                    if per_node_data[n_key]["status"] == reason:
                        skip_node_ips.append(node)
                    else:
                        self.fail("Invalid failure : %s, Expected: %s"
                                  % (per_node_data[n_key]['status'], reason))

            filtered_nodes = list()
            for t_node in nodes:
                for t_skip_node in skip_node_ips:
                    if t_node.ip == t_skip_node:
                        break
                else:
                    filtered_nodes.append(t_node)
            nodes = filtered_nodes

            status = self.cluster_util.copy_cb_collect_logs(
                rest, nodes, self.cluster, self.logs_folder)
            if status is False:
                self.fail("cb_collect zip file copy failed")

            # Remove local cb_collect files in log_dir/test_#
            for file_name in os.listdir(self.logs_folder):
                if file_name.endswith(".zip"):
                    os.remove(os.path.join(self.logs_folder, file_name))
