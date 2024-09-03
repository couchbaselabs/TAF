import json
import os
from math import ceil

from cb_constants import CbServer
from basetestcase import BaseTestCase
from bucket_collections.collections_base import CollectionBase
from bucket_utils.bucket_ready_functions import BucketUtils
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from couchbase_helper.document import View
from couchbase_helper.documentgenerator import doc_generator
from couchbase_helper.durability_helper import DurabilityHelper
from membase.api.rest_client import RestConnection
from rebalance_utils.rebalance_util import RebalanceUtil
from rebalance_utils.retry_rebalance import RetryRebalanceUtil
from sdk_client3 import SDKClientPool
from sdk_exceptions import SDKException

retry_exceptions = list([SDKException.AmbiguousTimeoutException,
                         SDKException.DurabilityImpossibleException,
                         SDKException.DurabilityAmbiguousException,
                         SDKException.TimeoutException,
                         SDKException.ServerOutOfMemoryException,
                         SDKException.DocumentNotFoundException])


class RebalanceBaseTest(BaseTestCase):
    def setUp(self):
        super(RebalanceBaseTest, self).setUp()
        self.rest = ClusterRestAPI(self.cluster.master)
        self.doc_ops = self.input.param("doc_ops", "create")
        self.bucket_size = self.input.param("bucket_size", 256)
        self.key_size = self.input.param("key_size", None)
        self.zone = self.input.param("zone", 1)
        self.replica_to_update = self.input.param("new_replica", None)
        self.default_view_name = "default_view"
        default_map_func = "function (doc) {\n  emit(doc._id, doc);\n}"
        self.default_view = View(self.default_view_name, default_map_func,
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

        if self.cluster.type == "default":
            node_ram_ratio = self.bucket_util.base_bucket_ratio(
                self.cluster.servers)
            _, info = self.rest.node_details()
            self.rest.initialize_node(
                username=self.cluster.master.rest_username,
                password=self.cluster.master.rest_password)
            kv_mem_quota = int(info["mcdMemoryReserved"]*node_ram_ratio)
            self.rest.configure_memory(
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
            self.bucket_util.update_memcached_num_threads_settings(
                self.cluster.master,
                num_writer_threads="disk_io_optimized",
                num_reader_threads="disk_io_optimized")
        # Buckets creation and initial data load done by bucket_spec
        if self.spec_name is not None:
            try:
                self.collection_setup()
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
            if self.load_docs_using == "default_loader" \
                    and self.cluster.sdk_client_pool:
                self.log.info("Creating SDK clients for client_pool")
                for bucket in self.cluster.buckets:
                    self.cluster.sdk_client_pool.create_clients(
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
        self.retry_rebalance_util = RetryRebalanceUtil()
        self.log_setup_status("RebalanceBase", "complete")

    def collection_setup(self):
        CollectionBase.deploy_buckets_from_spec_file(self)

        # Init sdk_client_pool if not initialized before
        if self.cluster.sdk_client_pool is None:
            self.cluster.sdk_client_pool = SDKClientPool()

        # Create clients in SDK client pool
        self.log.info("Creating required SDK clients for client_pool")
        bucket_count = len(self.cluster.buckets)
        max_clients = self.task_manager.number_of_threads
        clients_per_bucket = int(ceil(max_clients / bucket_count))
        for bucket in self.cluster.buckets:
            self.cluster.sdk_client_pool.create_clients(
                bucket,
                [self.cluster.master],
                clients_per_bucket,
                compression_settings=self.sdk_compression)

        CollectionBase.load_data_from_spec_file(self, "initial_load")

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
            ram_quota=self.bucket_size,
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
            self.bucket_util.update_memcached_num_threads_settings(
                self.cluster.master,
                num_writer_threads="default",
                num_reader_threads="default",
                num_storage_threads="default")
        super(RebalanceBaseTest, self).tearDown()

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
        nodes = self.cluster_util.get_nodes(self.servers[0],
                                            inactive_added=True)
        zones = ["Group 1"]
        nodes_in_zone = {"Group 1": [node for node in nodes
                                     if node.ip == self.servers[0].ip]}
        # Create zones, if not existing, based on params zone in test.
        # Shuffle the nodes between zones.
        rest = RestConnection(self.servers[0])
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
                    nodes_in_zone[zones[server_group]].append(
                        [node for node in nodes
                         if node.ip == self.servers[i].ip][0])
            # Shuffle the nodesS
            for i in range(1, self.zone):
                node_in_zone = list(
                    set(nodes_in_zone[zones[i]])
                    - set([node for node in rest.get_nodes_in_zone(zones[i])]))
                rest.shuffle_nodes_in_zones(node_in_zone, zones[0], zones[i])
        otpnodes = [node.id for node in rest.node_statuses()]
        nodes_to_remove = [node.id for node in rest.node_statuses()
                           if node.ip in [t.ip for t in to_remove]]
        # Start rebalance and monitor it.
        started, _ = rest.rebalance(otpNodes=otpnodes,
                                    ejectedNodes=nodes_to_remove)
        if started:
            reb_util = RebalanceUtil(self.cluster)
            result = reb_util.monitor_rebalance()
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
        self.cluster_util.print_cluster_stats(self.cluster)
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
            load_using=self.load_docs_using)
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
            sync=self.sync,
            binary_transactions=self.binary_transactions)
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
                    binary_transactions=self.binary_transactions): None})
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
                    sync=sync,
                    binary_transactions=self.binary_transactions): None})
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
                    sync=sync,
                    binary_transactions=self.binary_transactions): None})

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
                load_using=self.load_docs_using)
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
                load_using=self.load_docs_using)
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
                load_using=self.load_docs_using)
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
                tasks_info, self.cluster, load_using=self.load_docs_using)
            self.bucket_util.log_doc_ops_task_failures(tasks_info)

        return tasks_info

    def loadgen_docs(self, retry_exceptions=[], ignore_exceptions=[],
                     task_verification=False):
        def flatten_list(input_list):
            output_list = list()
            for t_list in input_list:
                if isinstance(t_list, list):
                    output_list += flatten_list(t_list)
                else:
                    output_list.append(t_list)
            return output_list

        retry_exceptions += [
                SDKException.AmbiguousTimeoutException,
                SDKException.RequestCanceledException,
                SDKException.DurabilityImpossibleException,
                SDKException.DurabilityAmbiguousException]
        retry_exceptions = list(set(flatten_list(retry_exceptions)))
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

    def start_rebalance(self, rebalance_operation):
        self.log.debug("Starting rebalance operation of type: {0}"
                       .format(rebalance_operation))
        if rebalance_operation == "rebalance_out":
            task = self.task.async_rebalance(
                self.cluster, [],
                [self.servers[self.nodes_init - 1]],
                retry_get_process_num=self.retry_get_process_num)
        elif rebalance_operation == "rebalance_in":
            task = self.task.async_rebalance(
                self.cluster,
                [self.servers[self.nodes_init]], [],
                retry_get_process_num=self.retry_get_process_num)
        elif rebalance_operation == "swap_rebalance":
            self.rest.add_node(self.cluster.master.rest_username,
                               self.cluster.master.rest_password,
                               self.servers[self.nodes_init].ip,
                               self.servers[self.nodes_init].port)
            task = self.task.async_rebalance(
                self.cluster, [],
                [self.servers[self.nodes_init - 1]],
                retry_get_process_num=self.retry_get_process_num)
        elif rebalance_operation == "graceful_failover":
            task = self.task.async_failover([self.cluster.master],
                                            failover_nodes=[self.servers[1]],
                                            graceful=True,
                                            wait_for_pending=300)
        return task

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
