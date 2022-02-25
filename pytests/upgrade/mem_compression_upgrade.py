'''
Created on 10-Feb-2022
@author: Sanjit
'''

from math import ceil
from Cb_constants import CbServer, DocLoading
from couchbase_helper.documentgenerator import doc_generator
from gsiLib.gsiHelper import GsiHelper
from index_utils.index_ready_functions import IndexUtils
from index_utils.plasma_stats_util import PlasmaStatsUtil
from upgrade.upgrade_base import UpgradeBase
from cbas_utils.cbas_utils import CbasUtil, CBASRebalanceUtil
from membase.api.rest_client import RestConnection
from sdk_exceptions import SDKException
from collections_helper.collections_spec_constants import MetaCrudParams

class MemCompressionUpgradeTests(UpgradeBase):
    def setUp(self):
        super(MemCompressionUpgradeTests, self).setUp()
        self.cbas_util = CbasUtil(self.task)
        self.cbas_spec_name = self.input.param("cbas_spec", "local_datasets")
        self.rebalance_util = CBASRebalanceUtil(
            self.cluster_util, self.bucket_util, self.task,
            vbucket_check=True, cbas_util=self.cbas_util)
        cbas_cc_node_ip = None
        retry = 0
        self.cluster.cbas_nodes = \
            self.cluster_util.get_nodes_from_services_map(
                self.cluster, service_type="cbas", get_all_nodes=True,
                servers=self.cluster.nodes_in_cluster)


    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        super(MemCompressionUpgradeTests, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.tearDown.__name__)

    def create_Stats_Obj_list(self):
        stats_obj_dict = dict()
        for node in self.cluster.index_nodes:
            stat_obj = PlasmaStatsUtil(node, server_task=self.task, cluster=self.cluster)
            stats_obj_dict[str(node.ip)] = stat_obj
        return stats_obj_dict

    def find_nodes_with_service(self, service_type, nodes_list):
        filter_list = list()
        for node in nodes_list:
            if service_type in node.services:
                filter_list.append(node)
        return filter_list

    def test_upgrade(self):
        self.log.info("Upgrading cluster nodes to target version")
        self.index_replicas = self.input.param("index_replicas", 0)
        self.index_count = self.input.param("index_count", 1)
        major_version = float(self.initial_version[:3])
        self.log.info("major version is {}".format(major_version))
        rest = RestConnection(self.cluster.master)
        # Update RAM quota allocated to buckets created before upgrade
        cluster_info = rest.get_nodes_self()
        kv_quota = \
            cluster_info.__getattribute__(CbServer.Settings.KV_MEM_QUOTA)
        bucket_size = kv_quota // (self.input.param("num_buckets", 1) + 1)
        for bucket in self.cluster.buckets:
            self.bucket_util.update_bucket_property(
                self.cluster.master, bucket, bucket_size)
        self.log.info("Creating new buckets with scopes and collections")
        for i in range(1, self.input.param("num_buckets", 1) + 1):
            self.bucket_util.create_default_bucket(
                self.cluster,
                replica=self.num_replicas,
                compression_mode=self.compression_mode,
                ram_quota=bucket_size,
                bucket_type=self.bucket_type,
                storage=self.bucket_storage,
                eviction_policy=self.bucket_eviction_policy,
                bucket_durability=self.bucket_durability_level,
                bucket_name="bucket_{0}".format(i))
        if major_version >= 7.0:
            self.over_ride_spec_params = self.input.param(
                "override_spec_params", "").split(";")
            self.doc_spec_name = self.input.param("doc_spec", "initial_load")
            self.load_data_into_buckets()
        else:
            for bucket in self.cluster.buckets[1:]:
                gen_load = doc_generator(
                    self.key, 0, self.num_items,
                    randomize_doc_size=True, randomize_value=True,
                    randomize=True)
                async_load_task = self.task.async_load_gen_docs(
                    self.cluster, bucket, gen_load,
                    DocLoading.Bucket.DocOps.CREATE,
                    active_resident_threshold=self.active_resident_threshold,
                    timeout_secs=self.sdk_timeout,
                    process_concurrency=8,
                    batch_size=500,
                    sdk_client_pool=self.sdk_client_pool)
                self.task_manager.get_task_result(async_load_task)
                # Update num_items in case of DGM run
                if self.active_resident_threshold != 100:
                    self.num_items = async_load_task.doc_index
                bucket.scopes[CbServer.default_scope].collections[
                    CbServer.default_collection].num_items = self.num_items
                # Verify doc load count
                self.bucket_util._wait_for_stats_all_buckets(
                    self.cluster, self.cluster.buckets)
                self.sleep(30, "Wait for num_items to get reflected")
                current_items = self.bucket_util.get_bucket_current_item_count(
                    self.cluster, bucket)
        field = 'body'
        self.timer = self.input.param("timer", 600)
        self.indexUtil = IndexUtils(server_task=self.task)
        rest.set_indexer_storage_mode(storageMode="plasma")
        self.cluster.index_nodes = self.cluster_util.get_nodes_from_services_map(self.cluster, service_type="index",
                                                                                 get_all_nodes=True)
        self.cluster.query_nodes = self.cluster_util.get_nodes_from_services_map(self.cluster, service_type="n1ql",
                                                                                 get_all_nodes=True)
        indexMap, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                     gsi_base_name="Emp_id_index",
                                                                                     replica=self.index_replicas,
                                                                                     defer=False,
                                                                                     number_of_indexes_per_coll=self.index_count,
                                                                                     field='emp_id', sync=False,
                                                                                     timeout=self.wait_timeout)

        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)

        indexMap, createIndexTasklist = self.indexUtil.create_gsi_on_each_collection(self.cluster,
                                                                                     gsi_base_name="Name_index",
                                                                                     replica=self.index_replicas,
                                                                                     defer=False,
                                                                                     number_of_indexes_per_coll=self.index_count,
                                                                                     field='name', sync=False,
                                                                                     timeout=self.wait_timeout)

        for taskInstance in createIndexTasklist:
            self.task.jython_task_manager.get_task_result(taskInstance)
        node_to_upgrade = self.fetch_node_to_upgrade()
        while node_to_upgrade is not None:
            self.log.info("Selected node for upgrade: %s"
                          % node_to_upgrade.ip)
            self.upgrade_function[self.upgrade_type](node_to_upgrade,
                                                     self.upgrade_version)
            self.cluster_util.print_cluster_stats(self.cluster)
            self.log.info("Changing master")
            try:
                self.cluster.update_master_using_diag_eval(
                    self.cluster.servers[0])
            except Exception:
                self.cluster.update_master_using_diag_eval(
                    self.cluster.servers[self.nodes_init - 1])
            node_to_upgrade = self.fetch_node_to_upgrade()

        self.cluster.index_nodes = self.cluster_util.get_nodes_from_services_map(self.cluster, service_type="index",
                                                                                 get_all_nodes=True)
        self.cluster.query_nodes = self.cluster_util.get_nodes_from_services_map(self.cluster, service_type="n1ql",
                                                                                 get_all_nodes=True)
        self.sweep_interval = self.input.param("sweep_interval", 120)
        rest = GsiHelper(self.cluster.index_nodes[0], self.log)
        self.moi_snapshot_interval = self.input.param("moi_snapshot_interval", 120)
        rest.set_index_settings({"indexer.settings.persisted_snapshot.moi.interval": self.moi_snapshot_interval})
        rest.set_index_settings({"indexer.plasma.mainIndex.evictSweepInterval": self.sweep_interval})
        rest.set_index_settings({"indexer.plasma.backIndex.evictSweepInterval": self.sweep_interval})
        rest.set_index_settings({"indexer.plasma.backIndex.enableInMemoryCompression": True})
        rest.set_index_settings({"indexer.plasma.mainIndex.enableInMemoryCompression": True})
        rest.set_index_settings({"indexer.plasma.backIndex.enableCompressDuringBurst": True})
        rest.set_index_settings({"indexer.plasma.mainIndex.enableCompressDuringBurst": True})
        self.sleep(2 * self.sweep_interval, "Waiting for items to compress")
        self.check_compression_stat(self.cluster.index_nodes)

    def test_upgrade_without_collections(self):
        self.log.info("Upgrading cluster nodes to target version")
        self.index_replicas = self.input.param("index_replicas", 0)
        self.index_count = self.input.param("index_count", 1)
        major_version = float(self.initial_version[:3])
        self.log.info("major version is {}".format(major_version))
        rest = RestConnection(self.cluster.master)
        # Update RAM quota allocated to buckets created before upgrade
        cluster_info = rest.get_nodes_self()
        kv_quota = \
            cluster_info.__getattribute__(CbServer.Settings.KV_MEM_QUOTA)
        bucket_size = kv_quota // (self.input.param("num_buckets", 1) + 1)
        for bucket in self.cluster.buckets:
            self.bucket_util.update_bucket_property(
                self.cluster.master, bucket, bucket_size)
        self.log.info("Creating new buckets with scopes and collections")
        for i in range(1, self.input.param("num_buckets", 1) + 1):
            self.bucket_util.create_default_bucket(
                self.cluster,
                replica=self.num_replicas,
                compression_mode=self.compression_mode,
                ram_quota=bucket_size,
                bucket_type=self.bucket_type,
                storage=self.bucket_storage,
                eviction_policy=self.bucket_eviction_policy,
                bucket_durability=self.bucket_durability_level,
                bucket_name="bucket_{0}".format(i))

        for bucket in self.cluster.buckets[1:]:
            gen_load = doc_generator(
                    self.key, 0, self.num_items,
                    randomize_doc_size=True, randomize_value=True,
                    randomize=True)
            async_load_task = self.task.async_load_gen_docs(
                    self.cluster, bucket, gen_load,
                    DocLoading.Bucket.DocOps.CREATE,
                    active_resident_threshold=self.active_resident_threshold,
                    timeout_secs=self.sdk_timeout,
                    process_concurrency=8,
                    batch_size=500,
                    sdk_client_pool=self.sdk_client_pool)
            self.task_manager.get_task_result(async_load_task)
            # Update num_items in case of DGM run
            if self.active_resident_threshold != 100:
                self.num_items = async_load_task.doc_index
            bucket.scopes[CbServer.default_scope].collections[
                CbServer.default_collection].num_items = self.num_items
            # Verify doc load count
            self.bucket_util._wait_for_stats_all_buckets(
                self.cluster, self.cluster.buckets)
            self.sleep(30, "Wait for num_items to get reflected")
            current_items = self.bucket_util.get_bucket_current_item_count(
                self.cluster, bucket)
        field = 'body'
        self.timer = self.input.param("timer", 600)
        self.indexUtil = IndexUtils(server_task=self.task)
        rest.set_indexer_storage_mode(storageMode="plasma")
        self.cluster.index_nodes = self.cluster_util.get_nodes_from_services_map(self.cluster, service_type="index",
                                                                                 get_all_nodes=True)
        self.cluster.query_nodes = self.cluster_util.get_nodes_from_services_map(self.cluster, service_type="n1ql",
                                                                                 get_all_nodes=True)

        for bucket in self.cluster.buckets:
            for i in range(self.index_count):
                indexName = "Index0" + str(i)
                index_query = "CREATE INDEX `%s` ON `%s`(`body`)" % (indexName,
                                                                     bucket.name)
                self.query_client = RestConnection(self.cluster.query_nodes[0])
                #indexDict[indexName] = index_query
                result = self.query_client.query_tool(index_query)
                self.assertTrue(result["status"] == "success", "Index query failed!")

        node_to_upgrade = self.fetch_node_to_upgrade()
        while node_to_upgrade is not None:
            self.log.info("Selected node for upgrade: %s"
                          % node_to_upgrade.ip)
            self.upgrade_function[self.upgrade_type](node_to_upgrade,
                                                     self.upgrade_version)
            self.cluster_util.print_cluster_stats(self.cluster)
            self.log.info("Changing master")
            try:
                self.cluster.update_master_using_diag_eval(
                    self.cluster.servers[0])
            except Exception:
                self.cluster.update_master_using_diag_eval(
                    self.cluster.servers[self.nodes_init - 1])
            node_to_upgrade = self.fetch_node_to_upgrade()

        self.cluster.index_nodes = self.cluster_util.get_nodes_from_services_map(self.cluster, service_type="index",
                                                                                 get_all_nodes=True)
        self.cluster.query_nodes = self.cluster_util.get_nodes_from_services_map(self.cluster, service_type="n1ql",
                                                                                 get_all_nodes=True)
        self.sweep_interval = self.input.param("sweep_interval", 120)
        rest = GsiHelper(self.cluster.index_nodes[0], self.log)
        self.moi_snapshot_interval = self.input.param("moi_snapshot_interval", 120)
        rest.set_index_settings({"indexer.settings.persisted_snapshot.moi.interval": self.moi_snapshot_interval})
        rest.set_index_settings({"indexer.plasma.mainIndex.evictSweepInterval": self.sweep_interval})
        rest.set_index_settings({"indexer.plasma.backIndex.evictSweepInterval": self.sweep_interval})
        rest.set_index_settings({"indexer.plasma.backIndex.enableInMemoryCompression": True})
        rest.set_index_settings({"indexer.plasma.mainIndex.enableInMemoryCompression": True})
        rest.set_index_settings({"indexer.plasma.backIndex.enableCompressDuringBurst": True})
        rest.set_index_settings({"indexer.plasma.mainIndex.enableCompressDuringBurst": True})
        self.sleep(2 * self.sweep_interval, "Waiting for items to compress")
        self.check_compression_stat(self.cluster.index_nodes)

    def check_compression_stat(self, index_nodes_list):
        comp_stat_verified = True
        for node in index_nodes_list:
            plasma_stats_obj = PlasmaStatsUtil(node, server_task=self.task)
            index_storage_stats = plasma_stats_obj.get_index_storage_stats()
            for bucket in index_storage_stats.keys():
                for index in index_storage_stats[bucket].keys():
                    index_stat_map = index_storage_stats[bucket][index]
                    self.assertTrue(index_stat_map["MainStore"]["num_rec_compressible"] <= (
                                index_stat_map["MainStore"]["num_rec_allocs"] - index_stat_map["MainStore"]["num_rec_frees"] + index_stat_map["MainStore"][
                            "num_rec_compressed"]),
                                    "For MainStore num_rec_compressible is {} num_rec_allocs is {} num_rec_frees is {} num_rec_compressed is {}".format(
                                        index_stat_map["MainStore"]["num_rec_compressible"], index_stat_map["MainStore"]["num_rec_allocs"],
                                        index_stat_map["MainStore"]["num_rec_frees"], index_stat_map["MainStore"]["num_rec_compressed"]))
                    self.assertTrue(index_stat_map["BackStore"]["num_rec_compressible"] <= (
                            index_stat_map["BackStore"]["num_rec_allocs"] - index_stat_map["BackStore"][
                        "num_rec_frees"] + index_stat_map["BackStore"][
                                "num_rec_compressed"]),
                                    "For BackStore num_rec_compressible is {} num_rec_allocs is {} num_rec_frees is {} num_rec_compressed is {}".format(
                                        index_stat_map["BackStore"]["num_rec_compressible"], index_stat_map["BackStore"]["num_rec_allocs"],
                                        index_stat_map["BackStore"]["num_rec_frees"], index_stat_map["BackStore"]["num_rec_compressed"]))
                    self.log.debug("Compression value is: {}".format(index_stat_map["MainStore"]["num_rec_compressed"]))
                    if index_stat_map["MainStore"]["num_rec_compressed"] == 0:
                        if index_stat_map["MainStore"]["num_rec_compressible"] > 0:
                            self.log.debug("num_rec_compressible value is {}".format(index_stat_map["MainStore"]["num_rec_compressible"]))
                            return False
                        else:
                            self.log.debug("Items not compressing as num_rec_compressed is 0")
                    elif index_stat_map["MainStore"]["num_rec_compressed"] < 0 or index_stat_map["BackStore"]["num_rec_compressed"] < 0:
                        self.fail("Negative digit in compressed count")
        return comp_stat_verified

    def load_data_into_buckets(self, doc_loading_spec=None):
        """
        Loads data into buckets using the data spec
        """
        self.over_ride_spec_params = self.input.param(
            "override_spec_params", "").split(";")
        # Init sdk_client_pool if not initialized before
        if self.sdk_client_pool is None:
            self.init_sdk_pool_object()
        self.doc_spec_name = self.input.param("doc_spec", "initial_load")
        # Create clients in SDK client pool
        if self.sdk_client_pool:
            self.log.info("Creating required SDK clients for client_pool")
            bucket_count = len(self.cluster.buckets)
            max_clients = self.task_manager.number_of_threads
            clients_per_bucket = int(ceil(max_clients / bucket_count))
            for bucket in self.cluster.buckets:
                self.sdk_client_pool.create_clients(
                    bucket, [self.cluster.master], clients_per_bucket,
                    compression_settings=self.sdk_compression)

        if not doc_loading_spec:
            doc_loading_spec = self.bucket_util.get_crud_template_from_package(
                self.doc_spec_name)
        self.over_ride_doc_loading_template_params(doc_loading_spec)
        # MB-38438, adding CollectionNotFoundException in retry exception
        doc_loading_spec[MetaCrudParams.RETRY_EXCEPTIONS].append(
            SDKException.CollectionNotFoundException)
        doc_loading_task = self.bucket_util.run_scenario_from_spec(
            self.task, self.cluster, self.cluster.buckets,
            doc_loading_spec, mutation_num=0, batch_size=self.batch_size)
        if doc_loading_task.result is False:
            self.fail("Initial reloading failed")

        # Verify initial doc load count
        self.bucket_util._wait_for_stats_all_buckets(self.cluster,
                                                     self.cluster.buckets)
        self.bucket_util.validate_docs_per_collections_all_buckets(
            self.cluster)

    def over_ride_doc_loading_template_params(self, target_spec):
        for over_ride_param in self.over_ride_spec_params:
            if over_ride_param == "durability":
                target_spec[MetaCrudParams.DURABILITY_LEVEL] = \
                    self.durability_level
            elif over_ride_param == "sdk_timeout":
                target_spec[MetaCrudParams.SDK_TIMEOUT] = self.sdk_timeout
            elif over_ride_param == "doc_size":
                target_spec[MetaCrudParams.DocCrud.DOC_SIZE] = self.doc_size
            elif over_ride_param == "num_scopes":
                target_spec[MetaCrudParams.SCOPES_TO_ADD_PER_BUCKET] = int(
                    self.input.param("num_scopes", 1))
            elif over_ride_param == "num_collections":
                target_spec[MetaCrudParams.COLLECTIONS_TO_ADD_FOR_NEW_SCOPES] = int(
                    self.input.param("num_collections", 1))
            elif over_ride_param == "num_items":
                target_spec["doc_crud"][MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS] = \
                    self.num_items
