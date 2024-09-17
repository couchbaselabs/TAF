import random
import threading
import time
from random import choice
from threading import Thread

from BucketLib.BucketOperations import BucketHelper
from BucketLib.bucket import Bucket
from Jython_tasks.java_loader_tasks import SiriusCouchbaseLoader
from cb_server_rest_util.buckets.buckets_api import BucketRestApi
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from rebalance_utils.rebalance_util import RebalanceUtil
import testconstants
from StatsLib.StatsOperations import StatsHelper
from bucket_collections.collections_base import CollectionBase
from bucket_utils.bucket_ready_functions import CollectionUtils
from cb_constants import DocLoading, CbServer
from cb_tools.cbstats import Cbstats
from constants.sdk_constants.java_client import SDKConstants
from collections_helper.collections_spec_constants import MetaCrudParams, MetaConstants
from couchbase_helper.documentgenerator import doc_generator
from couchbase_helper.durability_helper import DurabilityHelper
from gsiLib.gsiHelper import GsiHelper
from membase.api.rest_client import RestConnection
from sdk_client3 import SDKClient, SDKClientPool
from shell_util.remote_connection import RemoteMachineShellConnection
from upgrade.upgrade_base import UpgradeBase


class UpgradeTests(UpgradeBase):
    def setUp(self):
        super(UpgradeTests, self).setUp()
        self.durability_helper = DurabilityHelper(
            self.log,
            len(self.cluster.nodes_in_cluster))
        self.verification_dict = dict()
        self.verification_dict["ops_create"] = self.num_items
        self.verification_dict["ops_delete"] = 0
        self.cluster_util.update_cluster_nodes_service_list(self.cluster)

    def tearDown(self):
        if self.range_scan_collections > 0:
            self.range_scan_task.stop_task = True
            self.task_manager.get_task_result(self.range_scan_task)
            result = CollectionUtils.get_range_scan_results(
                self.range_scan_task.fail_map, self.range_scan_task.expect_range_scan_failure, self.log)
            self.assertTrue(result, "unexpected failures in range scans")
        super(UpgradeTests, self).tearDown()

    def __wait_for_persistence_and_validate(self):
        self.bucket_util._wait_for_stats_all_buckets(
            self.cluster, self.cluster.buckets,
            cbstat_cmd="checkpoint", stat_name="num_items_for_persistence",
            timeout=300)
        self.bucket_util._wait_for_stats_all_buckets(
            self.cluster, self.cluster.buckets,
            cbstat_cmd="all", stat_name="ep_queue_size",
            timeout=60)
        self.bucket_util.validate_docs_per_collections_all_buckets(
            self.cluster, timeout=4800)

    def __trigger_cbcollect(self, log_path):
        self.log.info("Triggering cb_collect_info")
        rest = RestConnection(self.cluster.master)
        nodes = rest.get_nodes()
        status = self.cluster_util.trigger_cb_collect_on_cluster(rest, nodes)

        if status is True:
            self.cluster_util.wait_for_cb_collect_to_complete(rest)
            status = self.cluster_util.copy_cb_collect_logs(
                rest, nodes, self.cluster, log_path)
            if status is False:
                self.log_failure("API copy_cb_collect_logs detected failure")
        else:
            self.log_failure("API perform_cb_collect returned False")
        return status

    def __play_with_collection(self):
        # MB-44092 - Collection load not working with pre-existing connections
        self.cluster.sdk_client_pool = SDKClientPool()
        self.log.info("Creating required SDK clients for client_pool")
        clients_per_bucket = \
            int(self.thread_to_use / len(self.cluster.buckets))
        for bucket in self.cluster.buckets:
            self.cluster.sdk_client_pool.create_clients(
                self.cluster, bucket,
                req_clients=clients_per_bucket,
                compression_settings=self.sdk_compression)

        # Client based scope/collection crud tests
        client = self.cluster.sdk_client_pool.get_client_for_bucket(
            self.bucket)
        scope_name = self.bucket_util.get_random_name(
            max_length=CbServer.max_scope_name_len)
        collection_name = self.bucket_util.get_random_name(
            max_length=CbServer.max_collection_name_len)

        # Create scope using SDK client
        client.create_scope(scope_name)
        # Create collection under default scope and custom scope
        client.create_collection(collection_name, CbServer.default_scope)
        client.create_collection(collection_name, scope_name)
        # Drop created collections
        client.drop_collection(CbServer.default_scope, collection_name)
        client.drop_collection(scope_name, collection_name)
        # Drop created scope using SDK client
        client.drop_scope(scope_name)
        # Release the acquired client
        self.cluster.sdk_client_pool.release_client(client)

        # Create scopes/collections phase
        collection_load_spec = \
            self.bucket_util.get_crud_template_from_package("initial_load")
        collection_load_spec["doc_crud"][
            MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 0
        collection_load_spec["doc_crud"][
            MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS] = 5000
        collection_load_spec[
            MetaCrudParams.SCOPES_TO_ADD_PER_BUCKET] = 5
        collection_load_spec[
            MetaCrudParams.COLLECTIONS_TO_ADD_FOR_NEW_SCOPES] = 10
        collection_load_spec[
            MetaCrudParams.COLLECTIONS_TO_ADD_PER_BUCKET] = 50
        CollectionBase.set_retry_exceptions(
            collection_load_spec, self.durability_level)
        collection_task = \
            self.bucket_util.run_scenario_from_spec(self.task,
                                                    self.cluster,
                                                    self.cluster.buckets,
                                                    collection_load_spec,
                                                    mutation_num=1,
                                                    batch_size=500,
                                                    load_using=self.load_docs_using)
        if collection_task.result is False:
            self.log_failure("Collection task failed")
            return

        if(collection_task.result is True):
            self.log.info("Collection task 1 completed")
            if not self.upgrade_with_data_load and self.upgrade_type == "offline":
                self.__wait_for_persistence_and_validate()

        # Drop and recreate scope/collections
        collection_load_spec = \
            self.bucket_util.get_crud_template_from_package("initial_load")
        collection_load_spec["doc_crud"][
            MetaCrudParams.DocCrud.CREATE_PERCENTAGE_PER_COLLECTION] = 0
        collection_load_spec[MetaCrudParams.COLLECTIONS_TO_DROP] = 10
        collection_load_spec[MetaCrudParams.SCOPES_TO_DROP] = 2
        collection_task = \
            self.bucket_util.run_scenario_from_spec(self.task,
                                                    self.cluster,
                                                    self.cluster.buckets,
                                                    collection_load_spec,
                                                    mutation_num=1,
                                                    batch_size=500,
                                                    load_using=self.load_docs_using)
        if collection_task.result is False:
            self.log_failure("Drop scope/collection failed")
            return

        if(collection_task.result is True):
            self.log.info("Task 2 completed")
            if not self.upgrade_with_data_load and self.upgrade_type == "offline":
                self.__wait_for_persistence_and_validate()

        self.cluster.sdk_client_pool.shutdown()

    def verify_custom_path_post_upgrade(self):
        rebalance_out_node = None
        for node in self.cluster.servers:
            if node.ip != self.cluster.master.ip:
                rebalance_out_node = node
                break
        rebalanace_out = self.task.rebalance(
            self.cluster_util.get_nodes(self.cluster.master), to_add=[],
            to_remove=[rebalance_out_node])
        self.assertTrue(rebalanace_out, "re-balance out failed")
        new_node_to_add = self.cluster.servers[self.nodes_init]
        self.upgrade_helper.install_version_on_nodes(
            [self.cluster.servers[self.nodes_init]], self.upgrade_version)
        rebalance_in = self.task.rebalance(
            self.cluster_util.get_nodes(self.cluster.master), to_add=[
                rebalance_out_node, new_node_to_add], to_remove=[])
        self.assertTrue(rebalance_in, "rebalance in failed")
        for node in self.cluster.servers:
            shell = RemoteMachineShellConnection(node)
            output = str(
                shell.read_remote_file(testconstants.
                                       COUCHBASE_SINGLE_LOCAL_INI_PATH,
                                       'local.ini'))
            database_dir = ''.join(output.split("database_dir = ")[1].
                                   split("'")[0])
            index_dir = ''.join(output.split("view_index_dir = ")[1].
                                split("'")[0])
            database_dir = database_dir[:-2]
            index_dir = index_dir[:-2]
            self.assertTrue(database_dir == self.disk_location_data)
            self.assertTrue(index_dir == self.disk_location_index)
            shell.restart_couchbase()
            shell.disconnect()
            self.sleep(30, "waiting after cb restart")
        for node in self.cluster.nodes_in_cluster:
            rest = RestConnection(node)
            self.assertTrue(rest.get_data_path() == self.disk_location_data,
                            "custom path not retained")
            self.assertTrue(rest.get_index_path() == self.disk_location_index,
                            "custom path not retained")

    def test_upgrade(self):
        large_docs_start_num = 0
        range_scan_started = False
        range_scan_spare_node = self.spare_node

        ### Upserting all docs to increase fragmentation value ###
        ### Compaction operation is called once frag val hits 50% ###
        if self.magma_upgrade:
            self.PrintStep("Upserting docs to increase fragmentation value")
            self.upsert_docs_to_increase_frag_val()

        if self.include_indexing_query:
            self.create_indexes_pre_upgrade()

        self.PrintStep("Upgrade begins...")
        for upgrade_version in self.upgrade_chain:
            itr = 0
            bucket = self.cluster.buckets[0]
            self.initial_version = self.upgrade_version
            self.upgrade_version = upgrade_version
            ### Fetching the first node to upgrade ###
            node_to_upgrade = self.fetch_node_to_upgrade()

            ### Each node in the cluster is upgraded iteratively ###
            while node_to_upgrade is not None:

                if self.include_indexing_query:
                    self.indexes = set()
                    sdk_client = SDKClient(
                        self.cluster, None,
                        servers=[self.cluster.index_nodes[0]])
                    query = 'SELECT name FROM system:indexes;'
                    result = sdk_client.cluster.query(query).rowsAsObject()
                    for idx in result:
                        self.indexes.add(idx.get("name"))
                    sdk_client.close()

                if self.upgrade_with_data_load:
                    ### Loading docs with size > 1MB ###
                    if self.load_large_docs:
                        self.loading_large_documents(large_docs_start_num)

                    ### Starting async subsequent data load just before the upgrade starts ###
                    ''' Skipping this step for online swap because data load for swap rebalance
                        is started after the rebalance function is called '''
                    if self.upgrade_type != "online_swap":
                        sub_load_spec = self.bucket_util.get_crud_template_from_package(self.sub_data_spec)
                        CollectionBase.over_ride_doc_loading_template_params(self, sub_load_spec)
                        CollectionBase.set_retry_exceptions(
                            sub_load_spec, self.durability_level)

                        if self.alternate_load is True:
                            sub_load_spec["doc_crud"][MetaCrudParams.DocCrud.READ_PERCENTAGE_PER_COLLECTION] = 10
                            sub_load_spec["doc_crud"][MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 10

                        update_task = self.bucket_util.run_scenario_from_spec(
                            self.task,
                            self.cluster,
                            self.cluster.buckets,
                            sub_load_spec,
                            mutation_num=0,
                            async_load=True,
                            batch_size=500,
                            process_concurrency=4,
                            load_using=self.load_docs_using,
                            ops_rate=self.ops_rate)

                ### The upgrade procedure starts ###
                self.log.info("Selected node for upgrade: %s" % node_to_upgrade.ip)

                ### Based on the type of upgrade, the upgrade function is called ###
                if self.upgrade_type in ["failover_delta_recovery",
                                         "failover_full_recovery"]:
                    self.upgrade_function[self.upgrade_type](node_to_upgrade)
                elif self.upgrade_type == "full_offline":
                    self.upgrade_function[self.upgrade_type](self.cluster.nodes_in_cluster, self.upgrade_version)
                else:
                    self.upgrade_function[self.upgrade_type](node_to_upgrade,
                                                             self.upgrade_version)

                # Upgrade of node
                self.cluster_util.print_cluster_stats(self.cluster)

                ### Stopping the data load ###
                if self.upgrade_with_data_load and self.upgrade_type != "online_swap":
                    update_task.stop_indefinite_doc_loading_tasks()
                    self.task_manager.get_task_result(update_task)

                if self.load_large_docs:
                    large_docs_start_num += 1000

                self.sleep(10, "Wait for items to get reflected")

                ## Performing sync write while the cluster is in mixed mode ###
                ## Sync write is performed after the upgrade of each node ###
                sync_load_spec = self.bucket_util.get_crud_template_from_package(
                    self.sync_write_spec)
                CollectionBase.over_ride_doc_loading_template_params(self, sync_load_spec)
                CollectionBase.set_retry_exceptions(sync_load_spec, self.durability_level)

                sync_load_spec[MetaCrudParams.DURABILITY_LEVEL] = SDKConstants.DurabilityLevel.MAJORITY

                ## Collections are dropped and re-created while the cluster is mixed mode ###
                self.log.info("Performing collection ops in mixed mode cluster setting...")
                if self.guardrail_type != "collection_guardrail" and not self.include_indexing_query:
                    sync_load_spec[MetaCrudParams.COLLECTIONS_TO_DROP] = 2
                    sync_load_spec[MetaCrudParams.COLLECTIONS_TO_RECREATE] = 1
                    sync_load_spec["doc_crud"][
                        MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS] = self.items_per_col

                if self.include_indexing_query and itr < self.nodes_init-1:
                    sync_load_spec[MetaCrudParams.SCOPES_TO_ADD_PER_BUCKET] =  1
                    sync_load_spec[MetaCrudParams.COLLECTIONS_TO_ADD_FOR_NEW_SCOPES] = 1
                    sync_load_spec["doc_crud"][
                        MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS] = 100000

                #Validate sync_write results after upgrade
                if self.key_size is not None:
                    sync_load_spec["doc_crud"][MetaCrudParams.DocCrud.DOC_KEY_SIZE] = \
                        self.key_size
                self.PrintStep("Sync Write task starting...")
                sync_write_task = self.bucket_util.run_scenario_from_spec(
                    self.task,
                    self.cluster,
                    self.cluster.buckets,
                    sync_load_spec,
                    mutation_num=0,
                    batch_size=self.batch_size,
                    process_concurrency=self.process_concurrency,
                    validate_task=True,
                    load_using=self.load_docs_using,
                    ops_rate=self.ops_rate)

                if sync_write_task.result is False:
                    self.log_failure("SyncWrite failed during upgrade")

                self.PrintStep("Upgrade of node {0} done".format(itr+1))

                if self.include_indexing_query:
                    self.log.info("Creating indexes for new collections")
                    bucket = self.cluster.buckets[0]
                    for scope in self.bucket_util.get_active_scopes(bucket):
                        if scope.name in ["_system", "myscope"] :
                            continue
                        for col in self.bucket_util.get_active_collections(bucket, scope.name):
                            index_name = "{0}_{1}_{2}_index".format(bucket.name, scope.name, col.name)
                            if index_name not in self.indexes:
                                self.log.info("Creating secondary index {}".format(index_name))
                                query = 'CREATE INDEX `{0}` ON `{1}`.`{2}`.`{3}`(`{4}` include missing)'.format(
                                            index_name, bucket.name, scope.name, col.name, "name")
                                query += ' WITH { "num_replica":1 };'
                                client_query = RestConnection(self.cluster.query_nodes[0])
                                result = client_query.query_tool(query)
                                self.log.info("Result for creation of index {0} = {1}".format(index_name,
                                                                                    result['status']))
                                if result['status'] == 'success':
                                    self.index_count += 1
                                    self.indexes.add(index_name)

                ### Starting Range Scans if enabled ##
                if self.range_scan_collections > 0 and not range_scan_started:
                    self.cluster.master = range_scan_spare_node
                    range_scan_started = True
                    CollectionBase.range_scan_load_setup(self)

                # Halt further upgrade if test has failed during current upgrade
                if self.test_failure is not None:
                    break
                ### Fetching the next node to upgrade ###
                node_to_upgrade = self.fetch_node_to_upgrade()
                itr += 1

            self.cluster.version = upgrade_version
            self.cluster_features = \
                self.upgrade_helper.get_supported_features(self.cluster.version)
            self.set_feature_specific_params()

        ### Printing cluster stats after the upgrade of the whole cluster ###
        self.cluster_util.print_cluster_stats(self.cluster)
        self.PrintStep("Upgrade of the whole cluster to {0} complete".format(
                                                            self.upgrade_version))

        self.cluster.nodes_in_cluster = self.cluster_util.get_kv_nodes(self.cluster)
        self.servers = self.cluster_util.get_kv_nodes(self.cluster)

        if self.test_guardrail_migration or self.test_guardrail_upgrade:
            self.rr_guardrail_limits = {"couchstore": 10, "magma": 1}
            self.bucket_size_guardrail_limits = {"couchstore": 1.6, "magma": 16}
            self.check_and_set_guardrail_limits()

        # Adding _system scope and collections under it to the local bucket object since
        # these are added once the cluster is upgraded to 7.6
        if float(self.upgrade_version[:3]) >= 7.6 and float(self.initial_version[:3]) < 7.6:
            self.add_system_scope_to_all_buckets()

        ### Migration of the storageBackend ###
        if self.migrate_storage_backend:
            self.PrintStep("Migration of the storageBackend to {0}".format(
                                                    self.preferred_storage_mode))
            if self.preferred_storage_mode == Bucket.StorageBackend.couchstore:
                self.disable_history_on_buckets()
            for bucket in self.cluster.buckets:
                try:
                    self.bucket_util.update_bucket_property(self.cluster.master,
                                                        bucket,
                                                        storageBackend=self.preferred_storage_mode)
                    if self.test_guardrail_migration and self.breach_guardrail:
                        self.fail("Migration was allowed even when it would breach guardrail limits")
                except Exception as e:
                    self.log.info("Exception = {}. Updating of storageBackend failed".format(e))
                    err_msg = "Storage mode migration is not allowed"
                    exp = err_msg in str(e)
                    if self.test_guardrail_migration and self.breach_guardrail:
                        self.assertTrue(exp, "Wrong error message")
                    if self.test_guardrail_migration and not self.breach_guardrail:
                        self.fail("Migration was restricted even when it wouldn't breach guardrail limits")
            self.bucket_util.get_all_buckets(self.cluster)

            if self.test_guardrail_migration and self.breach_guardrail:
                self.log.info("Migration is restricted because of guardrail limits")
            else:
                for bucket in self.cluster.buckets:
                    self.verify_storage_key_for_migration(bucket, self.bucket_storage)

                ### Swap Rebalance/Failover recovery of all nodes post upgrade for migration ###
                if self.migration_procedure == "failover_recovery":
                    self.failover_recovery_all_nodes()
                elif self.migration_procedure == "swap_rebalance_all":
                    self.swap_rebalance_all_nodes()
                elif self.migration_procedure == "swap_rebalance":
                    self.swap_rebalance_all_nodes_iteratively()
                elif self.migration_procedure == "randomize_method":
                    self.log.info("Installing the version {0} on the spare node".format(
                                                                    self.upgrade_version))
                    self.upgrade_helper.new_install_version_on_all_nodes(
                        [self.spare_node], self.upgrade_version)
                    migration_methods = ["swap_rebalance", "failover_recovery"]
                    nodes_to_migrate = self.cluster.nodes_in_cluster
                    random_index = random.randint(0,1)
                    for node in nodes_to_migrate:
                        self.migrate_node_random(node, migration_methods[random_index % 2])
                        random_index += 1
                elif self.migration_procedure == "swap_rebalance_batch":
                    self.log.info("Installing the version {0} on the spare node".format(
                                                                    self.upgrade_version))
                    self.upgrade_helper.new_install_version_on_all_nodes(
                        [self.spare_node], self.upgrade_version)
                    nodes_to_migrate = self.cluster.nodes_in_cluster
                    self.spare_nodes = self.cluster.servers[self.nodes_init+1:]
                    self.spare_nodes.append(self.spare_node)
                    batch_size = 2
                    start_index = 0
                    while start_index < len(nodes_to_migrate):
                        nodes = nodes_to_migrate[start_index:start_index+batch_size]
                        self.swap_rebalance_batch_migrate(nodes)
                        start_index += batch_size
                    self.spare_node = self.spare_nodes[0]

                self.cluster.nodes_in_cluster = self.cluster_util.get_kv_nodes(self.cluster)
                self.servers = self.cluster_util.get_kv_nodes(self.cluster)
                self.bucket_util.get_all_buckets(self.cluster)
                self.bucket_util.print_bucket_stats(self.cluster)

                if self.preferred_storage_mode == Bucket.StorageBackend.magma:
                    self.magma_upgrade = True
                else:
                    self.magma_upgrade = False

        if self.test_guardrail_migration or self.test_guardrail_upgrade:
            self.post_upgrade_migration_guardrail_validation()

        ### Enabling CDC ###
        if self.magma_upgrade:
            self.vb_dict_list1 = {}
            self.enable_cdc_and_get_vb_details()

        if self.load_large_docs and self.upgrade_with_data_load:
            self.sleep(30, "Wait for items to get reflected")
            self.cluster.buckets[0].scopes[CbServer.default_scope].collections[
                CbServer.default_collection].num_items += 1000 * self.nodes_init

        if self.include_indexing_query:
            self.cluster.buckets[0].scopes["myscope"].collections[
                    "mycoll"].num_items += self.items_inserted

        self.log.info("starting doc verification")
        self.__wait_for_persistence_and_validate()
        self.log.info("Final doc count verified")

        # Play with collection if upgrade was successful
        if not self.test_failure and not self.test_guardrail_upgrade and \
            not self.test_guardrail_migration and not self.include_indexing_query:
            self.__play_with_collection()

        ### Verifying start sequence numbers ###
        if self.magma_upgrade:
            for bucket in self.cluster.buckets:
                if bucket.storageBackend == Bucket.StorageBackend.magma:
                    self.verify_history_start_sequence_numbers(bucket)

        self.cluster_util.print_cluster_stats(self.cluster)

        ### Editing history for a few collections after enabling CDC ###
        if self.magma_upgrade and self.guardrail_type != "collection_guardrail":
            self.edit_history_for_collections_existing_bucket()

        ### Creation of a new bucket post upgrade ###
        if self.guardrail_type != "bucket_guardrail":
            self.create_new_bucket_post_upgrade_and_load_data()

        if self.include_indexing_query and self.enable_shard_affinity:
            rest = RestConnection(self.cluster.index_nodes[0])
            self.log.info("Enabling file based index rebalance")
            rest.set_indexer_params(redistributeIndexes='true',
                                    enableShardAffinity='true')
            self.log.info("Building a few more indexes...")
            self.create_secondary_indexes(partitioned=True, replica=False,
                                          index_suffix='new_part_index',
                                          field='mutation_type')
            self.test_index_file_based_rebalance()
            self.validate_index_count_and_status()

        self.sleep(120)
        ### Rebalance/failover tasks after the whole cluster is upgraded ###
        if self.rebalance_op != "None":
            self.PrintStep("Starting rebalance/failover tasks post upgrade...")
            self.tasks_post_upgrade()

        # Perform final collection/doc_count validation
        self.validate_test_failure()
        self.PrintStep("Test Complete")

    def test_upgrade_from_ce_to_ee(self):
        itr = 0
        large_docs_start_num = 0
        self.PrintStep("Upgrade begins...")
        node_to_upgrade = self.fetch_node_to_upgrade()

        while node_to_upgrade is not None:
            self.log.info("Selected node for upgrade: %s" % node_to_upgrade.ip)

            if self.load_large_docs:
                self.buckets_to_load = self.cluster.buckets
                self.loading_large_documents(large_docs_start_num)

            self.upgrade_function[self.upgrade_type](node_to_upgrade,
                                                    self.upgrade_version)
            itr += 1
            self.PrintStep("Upgrade of node {0} done".format(itr))
            self.cluster_util.print_cluster_stats(self.cluster)

            ### Performing sync write while the cluster is in mixed mode ###
            sync_load_spec = self.bucket_util.get_crud_template_from_package(self.sync_write_spec)
            CollectionBase.over_ride_doc_loading_template_params(self, sync_load_spec)
            CollectionBase.set_retry_exceptions(sync_load_spec, self.durability_level)
            sync_load_spec[MetaCrudParams.DURABILITY_LEVEL] = SDKConstants.DurabilityLevel.MAJORITY

            ### Collections are dropped and re-created while the cluster is mixed mode ###
            if "collections" in self.cluster_features \
                    and self.perform_collection_ops:
                self.log.info("Performing collection ops in mixed mode cluster setting...")
                sync_load_spec[MetaCrudParams.COLLECTIONS_TO_DROP] = 2
                sync_load_spec[MetaCrudParams.COLLECTIONS_TO_RECREATE] = 1
                sync_load_spec["doc_crud"][
                    MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS] = self.items_per_col

            # Validate sync_write results after upgrade
            self.PrintStep("Sync Write task starting...")
            sync_write_task = self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.cluster.buckets,
                sync_load_spec,
                mutation_num=0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                load_using=self.load_docs_using,
                validate_task=True,
                ops_rate=self.ops_rate)

            if sync_write_task.result is True:
                self.log.info("SyncWrite and CollectionOps task succeeded")
            else:
                self.log_failure("SyncWrite failed during upgrade")

            ### Fetching the next node to upgrade ###
            node_to_upgrade = self.fetch_node_to_upgrade()
            if self.load_large_docs:
                large_docs_start_num += 1000

            CbServer.enterprise_edition = True
            if itr < self.nodes_init:
                self.PrintStep("Starting the upgrade of the next node")

            # Halt further upgrade if test has failed during current upgrade
            if self.test_failure is not None:
                break

        self.cluster_util.print_cluster_stats(self.cluster)
        self.PrintStep("Upgrade of the whole cluster complete")

        if self.load_large_docs:
            self.update_item_count_after_loading_large_docs()

        self.log.info("starting doc verification")
        self.__wait_for_persistence_and_validate()
        self.log.info("Final doc count verified")

        self.cluster_util.print_cluster_stats(self.cluster)

        self.sleep(120)
        ### Rebalance/failover tasks after the whole cluster is upgraded ###
        if self.rebalance_op != "None":
            self.PrintStep("Starting rebalance/failover tasks post upgrade...")
            self.tasks_post_upgrade(data_load=True)

        self.validate_test_failure()

    def test_downgrade(self):
        upgraded_nodes = []
        count = 0
        self.upgrade_version = self.upgrade_chain[-1]

        ### Sync write collection spec ###
        sync_load_spec = self.bucket_util.get_crud_template_from_package(self.sync_write_spec)
        CollectionBase.over_ride_doc_loading_template_params(self, sync_load_spec)
        CollectionBase.set_retry_exceptions(sync_load_spec, self.durability_level)
        sync_load_spec[MetaCrudParams.DURABILITY_LEVEL] = SDKConstants.DurabilityLevel.MAJORITY
        if "collections" in self.cluster_features \
                and self.perform_collection_ops:
            sync_load_spec[MetaCrudParams.COLLECTIONS_TO_DROP] = 2
            sync_load_spec[MetaCrudParams.COLLECTIONS_TO_RECREATE] = 1
            sync_load_spec["doc_crud"][
                MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS] = self.items_per_col

        self.PrintStep("Starting the upgrade of nodes to {0}".format(self.upgrade_version))

        while count < self.nodes_init-1:
            node_to_upgrade = self.fetch_node_to_upgrade()
            self.log.info("Selected node for upgrade : {0}".format(node_to_upgrade.ip))
            upgraded_nodes.append(self.spare_node)

            self.upgrade_function[self.upgrade_type](node_to_upgrade,
                                                    self.upgrade_version)
            self.cluster_util.print_cluster_stats(self.cluster)
            self.bucket_util.print_bucket_stats(self.cluster)

            # Validate sync_write results after upgrade of each node
            self.PrintStep("Sync Write task starting...")
            sync_write_task = self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.cluster.buckets,
                sync_load_spec,
                mutation_num=0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                validate_task=True,
                load_using=self.load_docs_using,
                ops_rate=self.ops_rate)

            if sync_write_task.result is True:
                self.log.info("SyncWrite task succeeded")
                self.bucket_util.print_bucket_stats(self.cluster)
            else:
                self.log_failure("SyncWrite failed in mixed mode cluster state")

            count += 1

        self.PrintStep("Starting the downgrade of nodes to {0}"
                       .format(self.upgrade_chain[0]))

        self.log.info("Nodes in {0} : {1}".format(self.upgrade_version, upgraded_nodes))
        for node_to_downgrade in upgraded_nodes:
            self.log.info("Selected node for downgrade : {0}".format(node_to_downgrade))

            self.upgrade_function[self.upgrade_type](node_to_downgrade,
                                                     self.upgrade_chain[0])

            self.cluster_util.print_cluster_stats(self.cluster)
            self.bucket_util.print_bucket_stats(self.cluster)

            # Validate sync_write results after upgrade of each node
            self.PrintStep("Sync Write task starting...")
            sync_write_task = self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.cluster.buckets,
                sync_load_spec,
                mutation_num=0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                validate_task=True,
                load_using=self.load_docs_using,
                ops_rate=self.ops_rate)

            if sync_write_task.result is True:
                self.log.info("SyncWrite task succeeded")
                self.bucket_util.print_bucket_stats(self.cluster)
            else:
                self.log_failure("SyncWrite failed in mixed mode cluster state")

        self.PrintStep("Downgrade of the cluster complete")

        self.log.info("starting doc verification")
        self.__wait_for_persistence_and_validate()
        self.log.info("Final doc count verified")

        self.upgrade_version = self.upgrade_chain[0]
        self.PrintStep("Starting rebalance/failover tasks post downgrade")
        self.tasks_post_upgrade()

    def test_upgrade_with_large_docs(self):
        # Number of docs of different sizes
        num_2mb_docs = self.input.param("num_2mb_docs", 1000)
        num_4mb_docs = self.input.param("num_4mb_docs", 1000)
        num_8mb_docs = self.input.param("num_8mb_docs", 1000)
        num_16mb_docs = self.input.param("num_16mb_docs", 500)
        num_20mb_docs = self.input.param("num_20mb_docs", 500)
        upgrade_data_load = self.input.param("upgrade_data_load", False)

        bucket = self.cluster.buckets[0]

        # Initial data load
        self.load_data_cbc_pillowfight(self.cluster.master, bucket,
                                       num_2mb_docs, 2548000, "2mbbig_docs")
        self.load_data_cbc_pillowfight(self.cluster.master, bucket,
                                       num_4mb_docs, 4548000, "4mbbig_docs")
        self.load_data_cbc_pillowfight(self.cluster.master, bucket,
                                       num_8mb_docs, 8548000, "8mbbig_docs")
        self.load_data_cbc_pillowfight(self.cluster.master, bucket,
                                       num_16mb_docs, 16548000, "16mbbig_docs")
        self.load_data_cbc_pillowfight(self.cluster.master, bucket,
                                       num_20mb_docs, 20548000, "20mbbig_docs")

        self.sleep(20, "Wait for num_items to get reflected")
        self.bucket_util.print_bucket_stats(self.cluster)

        self.upgrade_version = self.upgrade_chain[-1]
        self.PrintStep(f"Upgrading of nodes to {self.upgrade_version}")

        node_to_upgrade = self.fetch_node_to_upgrade()
        itr = 0
        pillowfight_thread = None

        while node_to_upgrade is not None:
            self.log.info(f"Selected node for upgrade: {node_to_upgrade.ip}")
            if upgrade_data_load:
                self.log.info("Starting data load during rebalance")
                pillowfight_thread = threading.Thread(
                    target=self.load_data_cbc_pillowfight,
                    args=[self.cluster.master, bucket, 5000, 1548000,
                          "1mbbig_docs", 1, 10])
                pillowfight_thread.start()

            if self.upgrade_type in ["failover_delta_recovery",
                                     "failover_full_recovery"]:
                self.upgrade_function[self.upgrade_type](node_to_upgrade)
            else:
                self.upgrade_function[self.upgrade_type](
                    node_to_upgrade, self.upgrade_version)
            if upgrade_data_load:
                pillowfight_thread.join()

            itr += 1
            self.PrintStep("Upgrade of node {0} done".format(itr))
            self.cluster_util.print_cluster_stats(self.cluster)
            node_to_upgrade = self.fetch_node_to_upgrade()

        self.PrintStep("Upgrade of the whole cluster complete")

    def test_bucket_durability_upgrade(self):
        self.key = self.input.param("key", "test_collections")
        update_task = None
        self.sdk_timeout = 60
        create_batch_size = 10000
        if self.atomicity:
            create_batch_size = 10

        # To make sure sync_write can we supported by initial cluster version
        sync_write_support = True
        if float(self.cluster.version[0:3]) < 6.5:
            sync_write_support = False

        if sync_write_support:
            self.verification_dict["rollback_item_count"] = 0
            self.verification_dict["sync_write_aborted_count"] = 0

        self.gen_load = doc_generator(self.key, 0, self.num_items,
                                      randomize_doc_size=True,
                                      randomize_value=True,
                                      randomize=True)

        if self.upgrade_with_data_load:
            self.log.info("Starting async doc updates")
            update_task = self.task.async_continuous_doc_ops(
                self.cluster, self.bucket, self.gen_load,
                op_type=DocLoading.Bucket.DocOps.UPDATE,
                process_concurrency=1,
                persist_to=1, replicate_to=1,
                timeout_secs=30)

        self.log.info("Upgrading cluster nodes to target version")
        node_to_upgrade = self.fetch_node_to_upgrade()
        while node_to_upgrade is not None:
            self.log.info("Selected node for upgrade: %s"
                          % node_to_upgrade.ip)
            self.upgrade_function[self.upgrade_type](node_to_upgrade,
                                                     self.upgrade_version)
            try:
                self.cluster.update_master_using_diag_eval(
                    self.cluster.servers[0])
            except Exception:
                self.cluster.update_master_using_diag_eval(
                    self.cluster.servers[self.nodes_init-1])

            create_gen = doc_generator(self.key, self.num_items,
                                       self.num_items+create_batch_size)
            # Validate sync_write results after upgrade
            if self.atomicity:
                sync_write_task = self.task.async_load_gen_docs_atomicity(
                    self.cluster, self.cluster.buckets,
                    create_gen, DocLoading.Bucket.DocOps.CREATE,
                    process_concurrency=1,
                    transaction_timeout=self.transaction_timeout,
                    record_fail=True)
            else:
                sync_write_task = self.task.async_load_gen_docs(
                    self.cluster, self.bucket, create_gen,
                    DocLoading.Bucket.DocOps.CREATE,
                    timeout_secs=self.sdk_timeout,
                    process_concurrency=4,
                    skip_read_on_error=True,
                    suppress_error_table=True,
                    load_using=self.load_docs_using)
            self.task_manager.get_task_result(sync_write_task)
            self.num_items += create_batch_size

            retry_index = 0
            while retry_index < 5:
                self.sleep(3, "Wait for num_items to match")
                current_items = self.bucket_util.get_buckets_item_count(
                    self.cluster, self.bucket.name)
                if current_items == self.num_items:
                    break
                self.log.debug("Num_items mismatch. Expected: %s, Actual: %s"
                               % (self.num_items, current_items))
            # Doc count validation
            self.cluster_util.print_cluster_stats(self.cluster)

            self.verification_dict["ops_create"] += create_batch_size
            self.summary.add_step("Upgrade %s" % node_to_upgrade.ip)

            # Halt further upgrade if test has failed during current upgrade
            if self.test_failure:
                break

            node_to_upgrade = self.fetch_node_to_upgrade()

        if self.upgrade_with_data_load:
            # Wait for update_task to complete
            update_task.end_task()
            self.task_manager.get_task_result(update_task)
        else:
            self.verification_dict["ops_update"] = 0

        # Cb_stats vb-details validation
        failed = self.durability_helper.verify_vbucket_details_stats(
            self.cluster.buckets[0],
            self.cluster_util.get_kv_nodes(self.cluster),
            vbuckets=self.cluster.vbuckets,
            expected_val=self.verification_dict)
        if failed:
            self.log_failure("Cbstat vbucket-details validation failed")
        self.summary.add_step("Cbstats vb-details verification")

        self.validate_test_failure()

        possible_d_levels = dict()
        possible_d_levels[Bucket.Type.MEMBASE] = [
            Bucket.DurabilityMinLevel.NONE,
            Bucket.DurabilityMinLevel.MAJORITY,
            Bucket.DurabilityMinLevel.MAJORITY_AND_PERSIST_ACTIVE,
            Bucket.DurabilityMinLevel.PERSIST_TO_MAJORITY]
        possible_d_levels[Bucket.Type.EPHEMERAL] = [
            Bucket.DurabilityMinLevel.NONE,
            Bucket.DurabilityMinLevel.MAJORITY]
        len_possible_d_levels = len(possible_d_levels[self.bucket_type]) - 1

        if not sync_write_support:
            self.verification_dict["rollback_item_count"] = 0
            self.verification_dict["sync_write_aborted_count"] = 0

        # Perform bucket_durability update
        key, value = doc_generator("b_durability_doc", 0, 1).next()
        client = SDKClient(self.cluster, self.cluster.buckets[0])
        for index, b_level in enumerate(possible_d_levels[self.bucket_type]):
            self.log.info("Updating bucket_durability=%s" % b_level)
            self.bucket_util.update_bucket_property(
                self.cluster.master,
                self.cluster.buckets[0],
                bucket_durability=b_level)
            self.bucket_util.print_bucket_stats(self.cluster)

            buckets = self.bucket_util.get_all_buckets(self.cluster)
            if buckets[0].durabilityMinLevel != b_level:
                self.log_failure("New bucket_durability not taken")

            self.summary.add_step("Update bucket_durability=%s" % b_level)
            self.sleep(10, "MB-39678: Bucket_d_level change to take effect")

            if index == 0:
                op_type = DocLoading.Bucket.DocOps.CREATE
                self.verification_dict["ops_create"] += 1
            elif index == len_possible_d_levels:
                op_type = DocLoading.Bucket.DocOps.DELETE
                self.verification_dict["ops_delete"] += 1
            else:
                op_type = DocLoading.Bucket.DocOps.UPDATE
                if "ops_update" in self.verification_dict:
                    self.verification_dict["ops_update"] += 1

            result = client.crud(op_type, key, value,
                                 timeout=self.sdk_timeout)
            if result["status"] is False:
                self.log_failure("Doc_op %s failed on key %s: %s"
                                 % (op_type, key, result["error"]))
            self.summary.add_step("Doc_op %s" % op_type)
        client.close()

        # Cb_stats vb-details validation
        failed = self.durability_helper.verify_vbucket_details_stats(
            self.cluster.buckets[0],
            self.cluster_util.get_kv_nodes(self.cluster),
            vbuckets=self.cluster.vbuckets,
            expected_val=self.verification_dict)
        if failed:
            self.log_failure("Cbstat vbucket-details validation failed")
        self.summary.add_step("Cbstats vb-details verification")
        self.validate_test_failure()

    def test_transaction_doc_isolation(self):
        def run_transaction_updates():
            self.log.info("Starting transaction updates in parallel")
            while not stop_thread:
                commit_trans = choice([True, False])
                trans_update_task = self.task.async_load_gen_docs_atomicity(
                    self.cluster, self.cluster.buckets, self.gen_load,
                    DocLoading.Bucket.DocOps.UPDATE,
                    exp=self.maxttl,
                    batch_size=50,
                    process_concurrency=3,
                    timeout_secs=self.sdk_timeout,
                    update_count=self.update_count,
                    transaction_timeout=self.transaction_timeout,
                    commit=commit_trans,
                    durability=self.durability_level,
                    sync=self.sync,
                    retries=0)
                self.task_manager.get_task_result(trans_update_task)

        stop_thread = False
        update_task = None
        self.sdk_timeout = 60

        self.log.info("Upgrading cluster nodes to target version")
        node_to_upgrade = self.fetch_node_to_upgrade()
        while node_to_upgrade is not None:
            self.log.info("Selected node for upgrade: %s"
                          % node_to_upgrade.ip)
            if self.upgrade_with_data_load:
                update_task = Thread(target=run_transaction_updates)
                update_task.start()

            self.upgrade_function[self.upgrade_type](node_to_upgrade,
                                                     self.upgrade_version)
            try:
                self.cluster.update_master_using_diag_eval(
                    self.cluster.servers[0])
            except Exception:
                self.cluster.update_master_using_diag_eval(
                    self.cluster.servers[self.nodes_init-1])

            if self.upgrade_with_data_load:
                stop_thread = True
                update_task.join()

            self.cluster_util.print_cluster_stats(self.cluster)
            self.bucket_util.print_bucket_stats(self.cluster)

            self.summary.add_step("Upgrade %s" % node_to_upgrade.ip)

            # Halt further upgrade if test has failed during current upgrade
            if self.test_failure:
                break

            node_to_upgrade = self.fetch_node_to_upgrade()
            for bucket in self.bucket_util.get_all_buckets(self.cluster):
                tombstone_doc_supported = \
                    "tombstonedUserXAttrs" in bucket.bucketCapabilities
                # This check is req. since the min. initial version is 6.6.X
                if not tombstone_doc_supported:
                    self.log_failure("Tombstone doc support missing for %s"
                                     % bucket.name)

        self.validate_test_failure()

        create_gen = doc_generator(self.key, self.num_items,
                                   self.num_items*2)
        # Start transaction load after node upgrade
        trans_task = self.task.async_load_gen_docs_atomicity(
            self.cluster, self.cluster.buckets,
            create_gen, DocLoading.Bucket.DocOps.CREATE, exp=self.maxttl,
            batch_size=50,
            process_concurrency=8,
            timeout_secs=self.sdk_timeout,
            update_count=self.update_count,
            transaction_timeout=self.transaction_timeout,
            commit=True,
            durability=self.durability_level,
            sync=self.sync,
            retries=0)
        self.task_manager.get_task_result(trans_task)

    def test_cbcollect_info(self):
        self.parse = self.input.param("parse", False)
        self.metric_name = self.input.param("metric_name", "kv_curr_items")
        log_path = self.input.param("logs_folder")
        self.log.info("Starting update tasks")
        update_tasks = list()
        update_tasks.append(self.task.async_continuous_doc_ops(
            self.cluster, self.bucket, self.gen_load,
            op_type=DocLoading.Bucket.DocOps.UPDATE,
            persist_to=1, replicate_to=1,
            process_concurrency=1,
            batch_size=10,
            timeout_secs=30))
        update_tasks.append(self.task.async_continuous_doc_ops(
            self.cluster, self.bucket, self.gen_load,
            op_type=DocLoading.Bucket.DocOps.UPDATE,
            replicate_to=1,
            process_concurrency=1,
            batch_size=10,
            timeout_secs=30))
        update_tasks.append(self.task.async_continuous_doc_ops(
            self.cluster, self.bucket, self.gen_load,
            op_type=DocLoading.Bucket.DocOps.UPDATE,
            persist_to=1,
            process_concurrency=1,
            batch_size=10,
            timeout_secs=30))

        node_to_upgrade = self.fetch_node_to_upgrade()
        while node_to_upgrade is not None:
            # Cbcollect with mixed mode cluster
            status = self.__trigger_cbcollect(log_path)
            if status is False:
                break

            self.log.info("Selected node for upgrade: %s"
                          % node_to_upgrade.ip)
            self.upgrade_function[self.upgrade_type](node_to_upgrade,
                                                     self.upgrade_version)
            self.cluster_util.print_cluster_stats(self.cluster)

            try:
                self.cluster.update_master_using_diag_eval(
                    self.cluster.servers[0])
            except Exception:
                self.cluster.update_master_using_diag_eval(
                    self.cluster.servers[self.nodes_init-1])

            # TODO: Do some validations here
            try:
                self.get_all_metrics(self.parse, self.metric_name)
            except Exception:
                pass

            node_to_upgrade = self.fetch_node_to_upgrade()

            # Halt further upgrade if test has failed during current upgrade
            if self.test_failure is True:
                break

        # Metrics should work in fully upgraded cluster
        self.get_all_metrics(self.parse, self.metric_name)
        # Cbcollect with fully upgraded cluster
        self.__trigger_cbcollect(log_path)

        for update_task in update_tasks:
            # Wait for update_task to complete
            update_task.end_task()
            self.task_manager.get_task_result(update_task)

        self.validate_test_failure()

    def get_low_cardinality_metrics(self, parse):
        content = None
        for server in self.cluster_util.get_kv_nodes(self.cluster):
            content = StatsHelper(server).get_prometheus_metrics(parse=parse)
            if not parse:
                StatsHelper(server)._validate_metrics(content)
        for line in content:
            self.log.info(line.strip("\n"))

    def get_high_cardinality_metrics(self,parse):
        content = None
        try:
            for server in self.cluster_util.get_kv_nodes(self.cluster):
                content = StatsHelper(server).get_prometheus_metrics_high(
                    parse=parse)
                if not parse:
                    StatsHelper(server)._validate_metrics(content)
            for line in content:
                self.log.info(line.strip("\n"))
        except:
            pass

    def get_range_api_metrics(self, metric_name):
        label_values = {"bucket": self.cluster.buckets[0].name,
                        "nodes": self.cluster.master.ip}
        content = StatsHelper(self.cluster.master).get_range_api_metrics(
            metric_name, label_values=label_values)
        self.log.info(content)

    def get_instant_api(self, metric_name):
        pass

    def get_all_metrics(self, parse, metrics):
        self.get_low_cardinality_metrics(parse)
        self.get_high_cardinality_metrics(parse)
        self.get_range_api_metrics(metrics)
        self.get_instant_api(metrics)

    def upsert_docs_to_increase_frag_val(self):
        ### Fetching fragmentation value after initial load ###
        frag_dict = dict()
        server_frag = dict()
        field_to_grep = "rw_0:magma"

        for server in self.cluster.kv_nodes:
            cb_obj = Cbstats(server)
            frag_res = cb_obj.magma_stats(self.cluster.buckets[0].name, field_to_grep,
                                            "kvstore")
            frag_dict[server.ip] = frag_res
            cb_obj.disconnect()
            #server_frag[server.ip] = float(frag_dict[server.ip][field_to_grep]["Fragmentation"])

        self.log.info(
            "Fragmentation after initial load {0}".format(server_frag))

        ### Upserting all data to increase fragmentation value ###
        upsert_spec = self.bucket_util.get_crud_template_from_package(
            self.upsert_data_spec)
        CollectionBase.over_ride_doc_loading_template_params(self, upsert_spec)
        CollectionBase.set_retry_exceptions(upsert_spec, self.durability_level)

        if self.alternate_load is True:
            upsert_spec["doc_crud"][
                MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 20

        upsert_task = self.bucket_util.run_scenario_from_spec(
            self.task,
            self.cluster,
            self.cluster.buckets,
            upsert_spec,
            mutation_num=0,
            batch_size=self.batch_size,
            process_concurrency=self.process_concurrency,
            load_using=self.load_docs_using,
            ops_rate=self.ops_rate)

        ### Fetching fragmentation value after upserting data ###
        if upsert_task.result is True:
            self.log.info("Upsert task finished.")

            frag_dict = dict()
            server_frag = dict()

            for server in self.cluster.kv_nodes:
                cb_obj = Cbstats(server)
                frag_res = cb_obj.magma_stats(self.cluster.buckets[0].name,
                                              field_to_grep, "kvstore")
                frag_dict[server.ip] = frag_res
                cb_obj.disconnect()

            self.log.info("Fragmentation after upsert {0}".format(server_frag))

            if int(self.cluster.version[0]) >= 7:
                self.__wait_for_persistence_and_validate()
        else:
            self.log_failure("Upsert task failed.")

    def loading_large_documents(self, large_docs_start_num):
        self.log.info("Loading large docs...")
        gen_create = doc_generator("large_docs", large_docs_start_num,
                                   large_docs_start_num + 1000,
                                   doc_size=1024000,
                                   randomize_value=True)

        task = self.task.async_load_gen_docs(
            self.cluster, self.cluster.buckets[0],
            gen_create, DocLoading.Bucket.DocOps.CREATE, exp=0,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            process_concurrency=4,
            skip_read_on_error=True,
            suppress_error_table=True,
            load_using=self.load_docs_using)
        self.task_manager.get_task_result(task)

    def update_item_count_after_loading_large_docs(self):
        self.sleep(30, "Wait for items to get reflected")
        prev_count = \
        self.cluster.buckets[0].scopes[CbServer.default_scope].collections[
            CbServer.default_collection].num_items
        total_count = 0
        for server in self.cluster.nodes_in_cluster:
            cbstat_obj = Cbstats(server)
            default_count_dict = cbstat_obj.magma_stats(self.cluster.buckets[0].name,
                                                        field_to_grep="items",
                                                        stat_name="collections _default._default")
            cbstat_obj.disconnect()
            for key in default_count_dict:
                total_count += default_count_dict[key]
        large_doc_count = total_count - prev_count
        self.log.info("Large doc count = {0}".format(large_doc_count))
        self.cluster.buckets[0].scopes[CbServer.default_scope].collections[
            CbServer.default_collection].num_items += large_doc_count

    def enable_cdc_and_get_vb_details(self):
        for bucket in self.cluster.buckets:
            if bucket.storageBackend == Bucket.StorageBackend.magma:
                self.PrintStep("Enabling CDC")
                self.bucket_util.update_bucket_property(self.cluster.master,
                                                        bucket,
                                                        history_retention_seconds=86400,
                                                        history_retention_bytes=96000000000)
                self.log.info("CDC Enabled for bucket: {}".format(bucket.name))
        self.sleep(60, "Wait for History params to get reflected")

        for bucket in self.cluster.buckets:
            if bucket.storageBackend == Bucket.StorageBackend.magma:
                self.vb_dict = self.bucket_util.get_vb_details_for_bucket(bucket,
                                                            self.cluster.nodes_in_cluster)
                self.vb_dict_list1[bucket.name] = self.vb_dict

    def verify_history_start_sequence_numbers(self, bucket):
        self.log.info("Verifying history start sequence numbers")
        vb_dict1 = self.bucket_util.get_vb_details_for_bucket(bucket,
                                                            self.cluster.nodes_in_cluster)
        vb_dict = self.vb_dict_list1[bucket.name]

        mismatch_count = 0
        for vb_no in range(1024):
            mismatch = 0
            # Verifying active vbuckets
            vb_active_seq = vb_dict[vb_no]['active']['high_seqno']
            vb_active_hist = vb_dict1[vb_no]['active'][
                'history_start_seqno']
            if vb_active_hist < vb_active_seq:
                mismatch = 1

            # Verifying replica vbuckets
            replica_list1 = vb_dict[vb_no]['replica']
            replica_list2 = vb_dict1[vb_no]['replica']
            for j in range(len(replica_list1)):
                vb_replica_seq = replica_list1[j]['high_seqno']
                vb_replica_hist = replica_list2[j]['history_start_seqno']
                if vb_replica_hist < vb_replica_seq:
                    mismatch = 1

            if mismatch == 1:
                mismatch_count += 1

        if mismatch_count != 0:
            self.log.info("Start sequence mismatch in {0} vbuckets".format(
                mismatch_count))
        else:
            self.log.info(
                "History start sequence numbers verified for all 1024 vbuckets")

    def tasks_post_upgrade(self, data_load=True):
        rebalance_tasks = []

        if(self.rebalance_op == "all"):
            rebalance_tasks = ["rebalance_in", "rebalance_out", "swap_rebalance",
                               "failover_delta", "failover_full", "replica_update"]
        else:
            rebalance_tasks = self.rebalance_op.split(":")

        for reb_task in rebalance_tasks:
            self.sleep(120, "Sleeping before starting the next rebalance task")
            rebalance_data_load = None
            if data_load:
                rebalance_data_load = self.load_during_rebalance(self.sub_data_spec,
                                                                 async_load=True)

            if reb_task == "rebalance_in":
                self.upgrade_helper.new_install_version_on_all_nodes(
                    [self.spare_node], self.upgrade_version)

                rest = ClusterRestAPI(self.cluster.master)
                services = self.cluster_util.get_nodes_services(self.cluster.master)
                self.log.info("Services on orchestrator node : {}".format(services))
                services_on_master = services[(self.cluster.master.ip + ":"
                                               + str(self.cluster.master.port))]

                rest.add_node(self.spare_node.ip,
                            self.creds.rest_username,
                            self.creds.rest_password,
                            services=services_on_master)
                otp_nodes = [node.id for node in \
                             self.cluster_util.get_nodes(self.cluster.master)]

                # Validate orchestrator selection
                self.cluster_util.validate_orchestrator_selection(self.cluster)

                self.log.info("Rebalance starting...")
                self.log.info("Rebalancing-in the node {0}".format(self.spare_node.ip))
                rest.rebalance(known_nodes=otp_nodes, eject_nodes=[])
                self.sleep(10, "Wait before fetching rebalance stats")
                rebalance_result = RebalanceUtil(self.cluster).monitor_rebalance()

                if(rebalance_result):
                    self.log.info("Rebalance-in passed successfully")

                # Validate orchestrator selection
                self.cluster_util.validate_orchestrator_selection(self.cluster)

                self.cluster_util.print_cluster_stats(self.cluster)

            elif(reb_task == "rebalance_out"):

                rest = ClusterRestAPI(self.cluster.master)

                nodes = self.cluster_util.get_nodes(self.cluster.master)
                for node in nodes:
                    if node.ip == self.spare_node.ip:
                        eject_node = node
                        break
                otp_nodes = [node.id for node in \
                             self.cluster_util.get_nodes(self.cluster.master)]
                self.log.info("Rebalance-out starting...")
                self.log.info("Rebalancing out the node {0}".format(eject_node.ip))
                rest.rebalance(known_nodes=otp_nodes, eject_nodes=[eject_node.id])
                self.sleep(10, "Wait before fetching rebalance stats")
                rebalance_passed = RebalanceUtil(self.cluster).monitor_rebalance()

                if rebalance_passed:
                    self.log.info("Rebalance-out of the node successful")
                    self.cluster_util.print_cluster_stats(self.cluster)

                # Validate orchestrator selection
                self.cluster_util.validate_orchestrator_selection(self.cluster)

            elif reb_task == "swap_rebalance":
                self.upgrade_helper.new_install_version_on_all_nodes(
                        nodes=[self.spare_node], version=self.upgrade_version,
                        cluster_profile=self.cluster_profile)

                rest = ClusterRestAPI(self.cluster.master)
                cluster_nodes = self.cluster_util.get_nodes(self.cluster.master)
                services = self.cluster_util.get_nodes_services(self.cluster.master)
                services_on_target_node = services[
                    (self.cluster.master.ip + ":"
                     + str(self.cluster.master.port))]

                self.node_to_remove = self.cluster.master

                rebalance_passed = self.task.rebalance(
                        self.cluster,
                        to_add=[self.spare_node],
                        to_remove=[self.cluster.master],
                        check_vbucket_shuffling=False,
                        services=[",".join(services_on_target_node)])

                if(rebalance_passed):
                    self.log.info("Swap Rebalance successful")
                    # self.cluster.master = self.spare_node
                    self.spare_node = self.node_to_remove
                    self.cluster_util.print_cluster_stats(self.cluster)
                else:
                    self.log.info("Swap Rebalance failed")

            elif(reb_task == "failover_delta"):
                rest = ClusterRestAPI(self.cluster.master)
                nodes = self.cluster_util.get_nodes(self.cluster.master)
                for node in nodes:
                    if node.ip != self.cluster.master.ip:
                        otp_node = node
                        break

                self.log.info("Failing over the node {0}".format(otp_node.ip))
                failover_task, _ = rest.perform_graceful_failover(otp_node.id)

                if(failover_task):
                    self.log.info("Graceful Failover of the node successful")
                else:
                    self.log.info("Failover failed")

                self.sleep(10, "Wait before fetching rebalance stats")
                rebalance_passed = RebalanceUtil(self.cluster).monitor_rebalance()

                if(rebalance_passed):
                    self.log.info("Failover rebalance passed")
                    self.cluster_util.print_cluster_stats(self.cluster)

                # Validate orchestrator selection
                self.cluster_util.validate_orchestrator_selection(self.cluster)

                rest.set_failover_recovery_type(otp_node.id, "delta")

                # Validate orchestrator selection
                self.cluster_util.validate_orchestrator_selection(self.cluster)

                delta_recovery_buckets = [bucket.name for bucket in self.cluster.buckets]
                self.log.info("Rebalance starting...")
                rest.rebalance(known_nodes=[node.id for node in \
                                    self.cluster_util.get_nodes(self.cluster.master)],
                               delta_recovery_buckets=delta_recovery_buckets)
                self.sleep(10, "Wait before fetching rebalance stats")
                rebalance_passed = RebalanceUtil(self.cluster).monitor_rebalance()

                if(rebalance_passed):
                    self.log.info("Rebalance after recovery completed")
                    self.cluster_util.print_cluster_stats(self.cluster)

                # Validate orchestrator selection
                self.cluster_util.validate_orchestrator_selection(self.cluster)

            elif(reb_task == "failover_full"):
                rest = ClusterRestAPI(self.cluster.master)
                nodes = self.cluster_util.get_nodes(self.cluster.master)
                for node in nodes:
                    if node.ip != self.cluster.master.ip:
                        otp_node = node
                        break

                self.log.info("Failing over the node {0}".format(otp_node.ip))
                failover_task, _ = rest.perform_hard_failover(otp_node.id)

                if(failover_task):
                    self.log.info("Hard Failover of the node successful")
                else:
                    self.log.info("Failover failed")

                self.sleep(10, "Wait before fetching rebalance stats")
                rebalance_passed = RebalanceUtil(self.cluster).monitor_rebalance()

                if(rebalance_passed):
                    self.log.info("Failover rebalance passed")
                    self.cluster_util.print_cluster_stats(self.cluster)

                # Validate orchestrator selection
                self.cluster_util.validate_orchestrator_selection(self.cluster)

                rest.set_failover_recovery_type(otp_node.id, "full")

                # Validate orchestrator selection
                self.cluster_util.validate_orchestrator_selection(self.cluster)

                self.log.info("Rebalance starting...")
                rest.rebalance(known_nodes=[node.id for node in \
                                self.cluster_util.get_nodes(self.cluster.master)])
                self.sleep(10, "Wait before fetching rebalance stats")
                rebalance_passed = RebalanceUtil(self.cluster).monitor_rebalance()

                if(rebalance_passed):
                    self.log.info("Rebalance after recovery completed")
                    self.cluster_util.print_cluster_stats(self.cluster)

                # Validate orchestrator selection
                self.cluster_util.validate_orchestrator_selection(self.cluster)

            elif(reb_task == "replica_update"):
                self.log.info("Rebalancing-in the node {0}".format(self.spare_node.ip))
                rebalance_task = self.task.async_rebalance(self.cluster, to_add=[self.spare_node],
                                            to_remove=[], services=[CbServer.Services.KV])
                self.task_manager.get_task_result(rebalance_task)
                self.assertTrue(rebalance_task.result, "Rebalance-in failed")
                self.cluster_util.print_cluster_stats(self.cluster)

                for bucket in self.cluster.buckets:
                    if(bucket.name == "bucket-0"):
                        bucket_update = bucket
                        curr_replicas = bucket_update.replicaNumber
                        self.log.info("Updating replica count to {0} for {1}".format(curr_replicas + 1,
                                                                                     bucket_update.name))
                        self.bucket_util.update_bucket_property(self.cluster.master,
                                                                bucket_update,
                                                                replica_number=curr_replicas+1)
                self.bucket_util.print_bucket_stats(self.cluster)

                reb_task = self.task.async_rebalance(self.cluster, [], [])
                self.task_manager.get_task_result(reb_task)
                self.assertTrue(reb_task.result, "Rebalance after replica update failed")

            if rebalance_data_load is not None:
                self.task_manager.get_task_result(rebalance_data_load)

    def create_new_bucket_post_upgrade_and_load_data(self):

        self.PrintStep("Creating new bucket post upgrade")
        new_bucket_spec = self.bucket_util.get_bucket_template_from_package(self.spec_name)
        new_bucket_spec["buckets"] = {}

        new_bucket_spec[MetaConstants.USE_SIMPLE_NAMES] = True
        new_bucket_spec[MetaConstants.NUM_BUCKETS] = 1
        new_bucket_spec["buckets"]["new-bucket"] = {
            MetaConstants.NUM_SCOPES_PER_BUCKET: 2,
            MetaConstants.NUM_COLLECTIONS_PER_SCOPE: 5,
            MetaConstants.NUM_ITEMS_PER_COLLECTION: 1000,
            Bucket.ramQuotaMB: 512,
            Bucket.storageBackend: Bucket.StorageBackend.magma,
            Bucket.evictionPolicy: self.spec_bucket[Bucket.evictionPolicy]
        }

        CollectionBase.over_ride_bucket_template_params(self, self.bucket_storage,
                                                        new_bucket_spec)
        self.bucket_util.create_buckets_using_json_data(self.cluster, new_bucket_spec)
        self.bucket_util.wait_for_collection_creation_to_complete(self.cluster)

        self.bucket_util.print_bucket_stats(self.cluster)

        new_bucket = self.cluster.buckets[0]
        for bucket in self.cluster.buckets:
            if bucket.name == "new-bucket":
                new_bucket = bucket

        self.log.info("Creating required SDK clients for the new bucket")
        clients_per_bucket = int(self.thread_to_use / len(self.cluster.buckets))
        if self.load_docs_using == "default_loader":
            self.cluster.sdk_client_pool.shutdown()
            self.cluster.sdk_client_pool = SDKClientPool()
            for bucket in self.cluster.buckets:
                self.cluster.sdk_client_pool.create_clients(
                    self.cluster, bucket,
                    req_clients=clients_per_bucket,
                    compression_settings=self.sdk_compression)
        elif self.load_docs_using == "sirius_java_sdk":
            SiriusCouchbaseLoader.create_clients_in_pool(
                    self.cluster.master, self.cluster.master.rest_username,
                    self.cluster.master.rest_password,
                    bucket.name, req_clients=clients_per_bucket)

        self.log.info("Loading data into the new bucket...")
        new_bucket_load = self.bucket_util.get_crud_template_from_package(
            self.initial_data_spec)
        CollectionBase.over_ride_doc_loading_template_params(self, new_bucket_load)
        CollectionBase.set_retry_exceptions(new_bucket_load, self.durability_level)

        data_load_task = self.bucket_util.run_scenario_from_spec(
                                self.task,
                                self.cluster,
                                [new_bucket],
                                new_bucket_load,
                                mutation_num=0,
                                async_load=False,
                                batch_size=self.batch_size,
                                process_concurrency=self.process_concurrency,
                                load_using=self.load_docs_using,
                                ops_rate=self.ops_rate)

        if data_load_task.result is True:
            self.log.info("Data load for the new bucket complete")
            time.sleep(10)
            self.bucket_util.print_bucket_stats(self.cluster)

        ### Creating a few collections with history = false in the new bucket ###
        if self.magma_upgrade:
            self.log.info("Creating a few collections in the new bucket with history = false")
            for i in range(5):
                coll_obj = {"name":"new_col", "history":"false"}
                coll_obj["name"] += str(i)
                self.bucket_util.create_collection(self.cluster.master,
                                                    new_bucket,
                                                    scope_name=CbServer.default_scope,
                                                    collection_spec=coll_obj)

    def edit_history_for_collections_existing_bucket(self):

        ### Setting history = true for 50% of the collections ###
        self.PrintStep("Updating history value to true for 50 percent of the collections")
        scope_count = 0
        for scope in self.cluster.buckets[0].scopes:
            scope_name = scope
            for col in self.cluster.buckets[0].scopes[scope].collections:
                coll_name = col
                self.bucket_util.set_history_retention_for_collection(self.cluster.master,
                                                                    self.cluster.buckets[0],
                                                                    scope_name,
                                                                    coll_name,
                                                                    history="true")
            scope_count += 1
            if scope_count == 5:
                break

        ### Creating new collections in the existing bucket ###
        self.PrintStep("Creating a few collections in the existing bucket")
        for i in range(10):
            coll_obj = {"name":"new_col", "history":"true"}
            coll_obj["name"] += str(i)
            if i >= 5:
                coll_obj["history"] = "false"
            self.bucket_util.create_collection(self.cluster.master,
                                            self.cluster.buckets[0],
                                            scope_name=CbServer.default_scope,
                                            collection_spec=coll_obj)

    def swap_rebalance_all_nodes(self, data_load=True):
        self.PrintStep("Starting Swap Rebalance of all nodes post upgrade")
        self.spare_nodes = self.cluster.servers[self.nodes_init+1:]
        self.spare_nodes.append(self.spare_node)

        self.log.info("Installing the version {0} on the spare node".format(
                                                        self.upgrade_version))
        self.upgrade_helper.new_install_version_on_all_nodes(
            [self.spare_node], self.upgrade_version)

        nodes_to_replace = self.cluster.nodes_in_cluster

        services = self.cluster_util.get_nodes_services(self.cluster.master)
        services_on_target_node = services[
                (self.cluster.master.ip + ":" + str(self.cluster.master.port))]

        rebalance_passed = self.task.async_rebalance(
                    self.cluster,
                    to_add=self.spare_nodes,
                    to_remove=self.cluster.nodes_in_cluster,
                    check_vbucket_shuffling=False,
                    services=[",".join(services_on_target_node)])

        if data_load:
            self.log.info("Starting data load during rebalance...")
            self.load_during_rebalance(self.sub_data_spec)

        self.perform_collection_ops_load(self.collection_spec)
        self.task_manager.get_task_result(rebalance_passed)
        if rebalance_passed.result is True:
            self.log.info("Swap Rebalance successful")
        else:
            self.log.info("Swap Rebalance failed")

        self.cluster.nodes_in_cluster = self.spare_nodes
        self.cluster.master = self.cluster.nodes_in_cluster[0]
        self.spare_nodes = nodes_to_replace
        self.spare_node = self.spare_nodes[0]

    def swap_rebalance_all_nodes_iteratively(self):
        self.PrintStep("Swap rebalance of all nodes iteratively")
        self.log.info("Installing the version {0} on the spare node".format(
                                                    self.upgrade_version))
        self.upgrade_helper.new_install_version_on_all_nodes(
            [self.spare_node], self.upgrade_version)

        cluster_nodes = self.cluster.nodes_in_cluster

        for node in cluster_nodes:
            services = self.cluster_util.get_nodes_services(self.cluster.master)
            services_on_node = services[(node.ip + ":" + str(node.port))]

            self.log.info("Swap rebalance starting...")
            swap_reb_task = self.task.async_rebalance(
                self.cluster,
                to_add=[self.spare_node],
                to_remove=[node],
                check_vbucket_shuffling=False,
                services=[",".join(services_on_node)])

            self.log.info("Starting data load during rebalance...")
            self.load_during_rebalance(self.sub_data_spec)

            self.task_manager.get_task_result(swap_reb_task)
            if swap_reb_task.result is True:
                self.log.info("Swap rebalance of node {0} passed".format(node.ip))
                migrated_node_in = self.spare_node
                self.spare_node = node
                self.validate_mixed_storage_during_migration(migrated_node_in)
            else:
                self.log.info("Swap rebalance of node {0} failed".format(node.ip))
                return

        self.log.info("Iterative swap rebalance of all nodes done.")

    def failover_recovery_all_nodes(self):
        self.PrintStep("Starting Failover/Recovery of all nodes post upgrade")
        rest = ClusterRestAPI(self.cluster.master)
        nodes = self.cluster_util.get_nodes(self.cluster.master)

        for otp_node in nodes:

            self.log.info("Failing over the node {0}".format(otp_node.ip))
            failover_task, _ = rest.perform_graceful_failover(otp_node.id)
            self.assertTrue(failover_task, "Graceful Failover of node failed")

            self.sleep(10, "Wait before fetching rebalance stats")
            rebalance_passed = RebalanceUtil(self.cluster).monitor_rebalance()

            if rebalance_passed:
                self.cluster_util.print_cluster_stats(self.cluster)

            rest.set_failover_recovery_type(otp_node.id, "full")

            self.log.info("Rebalance starting...")
            rest.rebalance(known_nodes=[node.id for node in \
                            self.cluster_util.get_nodes(self.cluster.master)])
            self.sleep(10, "Wait before fetching rebalance stats")
            rebalance_passed = RebalanceUtil(self.cluster).monitor_rebalance()
            self.log.info("Performing data load during failover rebalance")
            self.load_during_rebalance(self.sub_data_spec)

            if rebalance_passed:
                self.log.info("Failover/recovery completed for node {0}".format(otp_node.ip))
                self.cluster_util.print_cluster_stats(self.cluster)
                self.validate_mixed_storage_during_migration(otp_node)

    def verify_storage_key_for_migration(self, bucket, storage_backend):
        bucket_helper = BucketHelper(self.cluster.master)
        bucket_stats = bucket_helper.get_buckets_json(bucket.name)
        for node in bucket_stats['nodes']:
            if 'storageBackend' not in node or \
                ('storageBackend' in node and node['storageBackend'] != storage_backend):
                self.log.info("storageBackend key mismatch/not present in node {0}".format(
                                                                        node['hostname']))
            else:
                self.log.info("storageBackend key introduced correctly for node {0}".format(
                                                                        node['hostname']))

    def validate_mixed_storage_during_migration(self, migrated_node):
        bucket_helper = BucketHelper(self.cluster.master)
        for bucket in self.cluster.buckets:
            bucket_stats = bucket_helper.get_buckets_json(bucket.name)
            for node in bucket_stats['nodes']:
                if migrated_node.ip == node['hostname'][:-5]:
                    if 'storageBackend' not in node:
                        self.log.info("Storage mode of node {0} is updated correctly"
                            " and now set to the global storage mode".format(migrated_node.ip))
                    else:
                        self.log.info("Storage mode wasn't updated correctly for node {0}".format(
                                                                                migrated_node.ip))

        sync_load_spec = self.bucket_util.get_crud_template_from_package(self.sync_write_spec)
        CollectionBase.over_ride_doc_loading_template_params(self, sync_load_spec)
        CollectionBase.set_retry_exceptions(sync_load_spec, self.durability_level)

        sync_load_spec[MetaCrudParams.DURABILITY_LEVEL] = SDKConstants.DurabilityLevel.MAJORITY

        ### Collections are dropped and re-created while the cluster is mixed mode ###
        self.log.info("Performing collection ops in mixed mode cluster setting...")
        sync_load_spec[MetaCrudParams.COLLECTIONS_TO_DROP] = 2
        sync_load_spec[MetaCrudParams.COLLECTIONS_TO_RECREATE] = 1
        sync_load_spec["doc_crud"][
            MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS] = self.items_per_col

        # Validate sync_write results after upgrade
        self.PrintStep("Sync Write and Collection Ops task starting...")
        sync_write_task = self.bucket_util.run_scenario_from_spec(
            self.task,
            self.cluster,
            self.cluster.buckets,
            sync_load_spec,
            mutation_num=0,
            batch_size=self.batch_size,
            process_concurrency=self.process_concurrency,
            validate_task=True,
            load_using=self.load_docs_using)

        if sync_write_task.result is True:
            self.log.info("SyncWrite and collectionOps task succeeded during migration")
        else:
            self.log_failure("SyncWrite/collectionOps failed during migration")

    def migrate_node_random(self, node, method):
        self.log.info("Procedure selected for migration of {0} : {1}".format(node.ip, method))
        if method == "swap_rebalance":
            rest = ClusterRestAPI(node)
            services = self.cluster_util.get_nodes_services(self.cluster.master)
            services_on_node = services[(node.ip + ":" + str(node.port))]

            self.log.info("Swap rebalance starting...")
            swap_reb_task = self.task.async_rebalance(
                self.cluster,
                to_add=[self.spare_node],
                to_remove=[node],
                check_vbucket_shuffling=False,
                services=[",".join(services_on_node)])

            self.log.info("Starting data load during rebalance...")
            self.load_during_rebalance(self.sub_data_spec)

            self.task_manager.get_task_result(swap_reb_task)
            if swap_reb_task.result is True:
                self.log.info("Swap rebalance of node {0} passed".format(node.ip))
                migrated_node_in = self.spare_node
                self.spare_node = node
                self.validate_mixed_storage_during_migration(migrated_node_in)
            else:
                self.log.info("Swap rebalance of node {0} failed".format(node.ip))

        else:
            rest = ClusterRestAPI(node)
            otp_node = None
            nodes = self.cluster_util.get_nodes(self.cluster.master)
            for n in nodes:
                if n.ip == node.ip:
                    otp_node = n
            self.log.info("Failing over the node {0}".format(otp_node.ip))
            failover_task, _ = rest.perform_graceful_failover(otp_node.id)
            self.assertTrue(failover_task, "Graceful Failover of node failed")

            self.sleep(10, "Wait before fetching rebalance stats")
            rebalance_passed = RebalanceUtil(self.cluster).monitor_rebalance()

            if rebalance_passed:
                self.cluster_util.print_cluster_stats(self.cluster)

            rest.set_failover_recovery_type(otp_node.id, "full")

            self.log.info("Rebalance starting...")
            rest.rebalance(known_nodes=[node.id for node in \
                           self.cluster_util.get_nodes(self.cluster.master)])
            self.sleep(10, "Wait before fetching rebalance stats")
            rebalance_passed = RebalanceUtil(self.cluster).monitor_rebalance()

            self.log.info("Performing data load during failover rebalance")
            self.load_during_rebalance(self.sub_data_spec)

            if rebalance_passed:
                self.log.info("Failover/recovery completed for node {0}".format(otp_node.ip))
                self.cluster_util.print_cluster_stats(self.cluster)
                self.validate_mixed_storage_during_migration(otp_node)

    def swap_rebalance_batch_migrate(self, nodes):
        spare_node_list = self.spare_nodes[:2]
        print("Spare node list", spare_node_list)

        self.log.info("Swap rebalance starting...")
        swap_reb_task = self.task.async_rebalance(
            self.cluster,
            to_add=spare_node_list,
            to_remove=nodes,
            check_vbucket_shuffling=False)

        self.load_during_rebalance(self.sub_data_spec)

        self.task_manager.get_task_result(swap_reb_task)
        if swap_reb_task.result is True:
            self.log.info("Batch Swap rebalance passed")
            self.spare_nodes = nodes
            for node in spare_node_list:
                self.validate_mixed_storage_during_migration(node)
        else:
            self.log.info("Batch Swap rebalance failed")

    def post_upgrade_migration_guardrail_validation(self):
        self.log.info("Performing post upgrade/migration guardrail validation")
        self.bucket = self.cluster.buckets[0]

        if self.test_guardrail_migration:
            if self.guardrail_type == "resident_ratio":
                self.log.info("RR guardrail limits = {}".format(self.rr_guardrail_limits))
                val = self.rr_guardrail_limits[self.bucket.storageBackend]
                rr_val = self.check_resident_ratio(self.cluster)
                rr_val_bucket = rr_val[self.bucket.name]
                if min(rr_val_bucket) < val:
                    self.breach_guardrail = True
                else:
                    self.breach_guardrail = False

            elif self.guardrail_type == "bucket_size":
                self.log.info("Bucket size guardrail limits = {}".format(self.bucket_size_guardrail_limits))
                val = self.bucket_size_guardrail_limits[self.bucket.storageBackend]
                bucket_sizes = self.check_bucket_data_size_per_node(self.cluster)
                bucket_val = bucket_sizes[self.bucket.name]
                if max(bucket_val) > val:
                    self.breach_guardrail = True
                else:
                    self.breach_guardrail = False

        if self.guardrail_type == "resident_ratio":
            error_code = 'BUCKET_RESIDENT_RATIO_TOO_LOW'
        elif self.guardrail_type == "bucket_size":
            error_code = 'BUCKET_DATA_SIZE_TOO_BIG'

        elif self.guardrail_type == "collection_guardrail":
            self.log.info("Trying to create a new collection which exceeds max collection limit")
            try:
                coll_obj = {"name":"new_col"}
                self.bucket_util.create_collection(self.cluster.master,
                                                   self.bucket,
                                                   scope_name=CbServer.default_scope,
                                                   collection_spec=coll_obj)
            except Exception as e:
                self.log.info("Exception = {}. Creation of collection failed as expected".format(e))
            else:
                self.fail("Collection creation did not fail even though it has breached guardrail limits")

        elif self.guardrail_type == "bucket_guardrail":
            self.log.info("Current bucket count = {}".format(len(self.cluster.buckets)))
            self.log.info("Trying to create a new bucket which would exceed the bucket count")
            try:
                self.bucket_util.create_default_bucket(
                    self.cluster, bucket_type=self.bucket_type,
                    ram_quota=256, replica=1, storage=Bucket.StorageBackend.magma,
                    eviction_policy=self.bucket_eviction_policy,
                    bucket_name="bucket_new")
            except Exception as e:
                self.log.info("Exception = {}. Creation of bucket failed as expected".format(e))
            else:
                self.fail("Bucket guardrail didn't prevent the creation of a new bucket")

        for bucket in self.cluster.buckets:
            result = self.insert_new_docs_sdk(num_docs=10, bucket=bucket)
            for res in result:
                if self.breach_guardrail:
                    exp = res["status"] == False and error_code in res["error"]
                    self.assertTrue(exp, "Mutations were not blocked")
                else:
                    exp = res["status"] == True
                    self.assertTrue(exp, "Mutations were blocked")
                    bucket.scopes[CbServer.default_scope].collections[
                        CbServer.default_collection].num_items += 1

    def check_and_set_guardrail_limits(self):
        if self.guardrail_type == "resident_ratio":
            self.bucket = self.cluster.buckets[0]
            current_rr = self.check_resident_ratio(self.cluster)
            bucket_rr = current_rr[self.bucket.name]
            self.log.info("Current resident ratio of bucket: {0} = {1}".format(self.bucket.name,
                                                                               bucket_rr))
            if self.breach_guardrail:
                guardrail_val = max(bucket_rr) + 5
            else:
                guardrail_val = max(min(bucket_rr) - 10, 1)

            if self.test_guardrail_migration:
                if self.preferred_storage_mode == Bucket.StorageBackend.couchstore:
                    status, content = BucketHelper(self.cluster.master).\
                                        set_bucket_rr_guardrails(couch_min_rr=guardrail_val)
                else:
                    status, content = BucketHelper(self.cluster.master).\
                                        set_bucket_rr_guardrails(magma_min_rr=guardrail_val)
                self.rr_guardrail_limits[self.preferred_storage_mode] = guardrail_val
            elif self.test_guardrail_upgrade:
                if self.bucket_storage == Bucket.StorageBackend.couchstore:
                    status, content = BucketHelper(self.cluster.master).\
                                        set_bucket_rr_guardrails(couch_min_rr=guardrail_val)
                else:
                    status, content = BucketHelper(self.cluster.master).\
                                        set_bucket_rr_guardrails(magma_min_rr=guardrail_val)
                self.rr_guardrail_limits[self.bucket_storage] = guardrail_val
            if status:
                self.log.info("Bucket RR Guardrails set {}".format(content))
                self.sleep(30, "Wait for 30 seconds after setting guardrail")

        elif self.guardrail_type == "bucket_size":
            current_bucket_sizes = self.check_bucket_data_size_per_node(self.cluster)
            bucket_size = current_bucket_sizes[self.bucket.name]
            self.log.info("Current size of bucket: {0} = {1}".format(self.bucket.name,
                                                                     bucket_size))
            if self.breach_guardrail:
                guardrail_val = min(bucket_size) / float(2)
            else:
                guardrail_val = max(bucket_size) + 0.01
            if self.test_guardrail_migration:
                if self.preferred_storage_mode == Bucket.StorageBackend.couchstore:
                    status, content = BucketHelper(self.cluster.master).\
                                    set_max_data_per_bucket_guardrails(couch_max_data=guardrail_val)
                else:
                    status, content = BucketHelper(self.cluster.master).\
                                    set_max_data_per_bucket_guardrails(magma_max_data=guardrail_val)
                self.bucket_size_guardrail_limits[self.preferred_storage_mode] = guardrail_val
            elif self.test_guardrail_upgrade:
                if self.bucket_storage == Bucket.StorageBackend.couchstore:
                    status, content = BucketHelper(self.cluster.master).\
                                    set_max_data_per_bucket_guardrails(couch_max_data=guardrail_val)
                else:
                    status, content = BucketHelper(self.cluster.master).\
                                    set_max_data_per_bucket_guardrails(magma_max_data=guardrail_val)
                self.bucket_size_guardrail_limits[self.bucket_storage] = guardrail_val
            if status:
                self.log.info("Bucket max data Guardrails set {}".format(content))
                self.sleep(30, "Wait for 30 seconds after setting guardrail")

    def disable_history_on_buckets(self):
        self.log.info("Disabling history before migrating to CouchStore")
        for bucket in self.cluster.buckets:
            if bucket.storageBackend == Bucket.StorageBackend.magma:
                self.bucket_util.update_bucket_property(self.cluster.master, bucket,
                                            history_retention_collection_default="false")
                for scope in bucket.scopes:
                    for coll in bucket.scopes[scope].collections:
                        self.bucket_util.set_history_retention_for_collection(self.cluster.master,
                                                                    bucket, scope, coll, "false")

    def create_primary_indexes(self):
        bucket = self.cluster.buckets[0]
        self.query_client = RestConnection(self.cluster.query_nodes[0])
        self.log.info("Creating primary indexes..")
        for scope in self.bucket_util.get_active_scopes(bucket):
            for col in self.bucket_util.get_active_collections(bucket, scope.name):
                primary_index_query = "CREATE PRIMARY INDEX ON `{0}`.`{1}`.`{2}`".format(
                    bucket.name, scope.name, col.name)
                result = self.query_client.query_tool(primary_index_query)
                self.log.info("Result for creation of index = {0}".format(result['status']))
                if result['status'] == 'success':
                    self.index_count += 1

    def create_primary_partitioned_indexes(self):
        bucket = self.cluster.buckets[0]
        self.query_client = RestConnection(self.cluster.query_nodes[0])
        self.log.info("Creating primary partitioned indexes..")
        for col in self.bucket_util.get_active_collections(bucket, "_default"):
            part_idx_name = '{0}_{1}_{2}_part_index'.format(bucket.name, "_default",
                                                                col.name)
            self.log.info("Creating partitioned index {}".format(part_idx_name))
            primary_index_query = 'CREATE PRIMARY INDEX `{0}` ON `{1}`.`{2}`.`{3}`'.format(
                part_idx_name, bucket.name, "_default", col.name)
            primary_index_query += ' PARTITION BY HASH(META().id) WITH {"num_partition": 8};'
            result = self.query_client.query_tool(primary_index_query)
            self.log.info("Result for creation of index {0} = {1}".format(part_idx_name,
                                                                    result['status']))
            if result['status'] == 'success':
                self.index_count += 1

    def create_secondary_indexes(self, partitioned=False, replica=True,
                                index_suffix='sec_index', field='name'):
        bucket = self.cluster.buckets[0]
        self.query_client = RestConnection(self.cluster.query_nodes[0])
        self.log.info("Creating secondary indexes..")
        for scope in self.bucket_util.get_active_scopes(bucket):
            if scope.name == "_system":
                continue
            for col in self.bucket_util.get_active_collections(bucket, scope.name):
                idx_name = '{0}_{1}_{2}_'.format(bucket.name, scope.name, col.name)
                idx_name += index_suffix
                self.log.info("Creating secondary index {}".format(idx_name))
                query = 'CREATE INDEX `{0}` ON `{1}`.`{2}`.`{3}`(`{4}` include missing)'.format(
                            idx_name, bucket.name, scope.name, col.name, field)
                if partitioned:
                    query += ' PARTITION BY HASH(META().id) WITH {"num_partition": 8};'
                if replica:
                    query += ' WITH { "num_replica":1 };'
                result = self.query_client.query_tool(query)
                self.log.info("Result for creation of index {0} = {1}".format(idx_name,
                                                                    result['status']))
                if result['status'] == 'success':
                    self.index_count += 1
                break

    def test_index_file_based_rebalance(self):

        self.PrintStep("Testing index file based rebalance")

        extra_node = self.cluster.servers[-1]
        nodes_to_add = [self.spare_node] + [extra_node]

        self.log.info("Installing latest version on the spare node")
        self.upgrade_helper.install_version_on_nodes([self.spare_node],
                                            self.upgrade_version,
                                            cluster_profile=self.cluster_profile)
        index_nodes = self.cluster.index_nodes

        self.log.info("Swap rebalance of index nodes")
        swap_rebalance_result = self.task.rebalance(
                self.cluster,
                to_add=nodes_to_add,
                to_remove=index_nodes,
                check_vbucket_shuffling=False,
                services=["index,n1ql"]*len(index_nodes))
        self.assertTrue(swap_rebalance_result, "Swap rebalance failed")
        self.log.info("Swap rebalance of index nodes was successful")
        self.spare_node = index_nodes[0]

        self.log.info("Rebalancing-in index node {}".format(self.spare_node.ip))
        rebalance_in_result = self.task.rebalance(
                self.cluster,
                to_add=[self.spare_node],
                to_remove=[],
                check_vbucket_shuffling=False,
                services=["index,n1ql"])
        self.assertTrue(rebalance_in_result, "Rebalance-in failed")
        self.log.info("Rebalance-in of index node successful")

    def validate_index_count_and_status(self):

        self.indexer_rest = GsiHelper(self.cluster.index_nodes[0], self.log)

        self.log.info("Validating index count")
        sdk_client = SDKClient(self.cluster, None,
                               servers=[self.cluster.index_nodes[0]])
        query = 'SELECT name FROM system:indexes;'
        result = sdk_client.cluster.query(query).rowsAsObject()
        idx_count = 0
        for idx in result:
            idx_count += 1
        sdk_client.close()

        self.log.info("Actual index count = {}".format(idx_count))
        self.log.info("Expected index count = {}".format(self.index_count))

        self.assertTrue(self.index_count == idx_count,
                        "Index count does not match")
        self.log.info("Indexes count verified")

        index_host_dict = dict()
        self.log.info("Validating that indexes are present on all nodes")
        result = self.indexer_rest.get_index_status()

        for res in result["status"]:
            nodes = res["hosts"]
            for node in nodes:
                if node not in index_host_dict:
                    index_host_dict[node] = 1
                else:
                    index_host_dict[node] += 1

        self.log.info("Indexes hosted on each indexer node = {}".format(index_host_dict))

        err_msg = 'Indexes are not present on all nodes'
        self.assertTrue(len(index_host_dict) == len(self.cluster.index_nodes), err_msg)
        self.log.info("Indexes are hosted on all nodes")

        self.log.info("Verifying that alternate shard IDs have been created for all indexes")
        for res in result["status"]:
            idx_name = res["name"]
            exp = len(res["alternateShardIds"]) >= 1
            error_msg = "Alternate shard ID not present for {}".format(idx_name)
            self.assertTrue(exp, error_msg)
        self.log.info("Alternate shard IDs present for all indexes")

    def insert_json_docs_sdk(self, num_items):

        bucket = self.cluster.buckets[0]
        global success_insert_count
        success_insert_count = 0
        data_load_threads = []
        self.log.info("Creating a new scope and a new collection")
        self.bucket_util.create_scope(self.cluster.master, bucket,
                                      {"name": "myscope"})
        self.bucket_util.create_collection(self.cluster.master, bucket,
                                        "myscope", {"name": "mycoll"})

        def insert_docs(server, bucket, num_docs, key_prefix):
            sdk_client_for_load = SDKClient(
                self.cluster, bucket, servers=[server],
                scope="myscope", collection="mycoll")
            hair = ['Burgundy', 'English vermillion', 'Battleship grey', 'Blizzard blue']
            local_item_count = 0
            for j in range(num_docs):
                age_random = random.randint(1,100)
                height_random = random.randint(1,100)
                weight_random = random.randint(1,100)
                hair_random = random.choice(hair)
                doc = {
                    "name": "XJohn",
                    "age": age_random,
                    "animals": ["Electric lime", "Electric purple"],
                    "attributes": {
                        "hair": hair_random,
                        "dimensions": {
                        "height": height_random,
                        "weight": weight_random
                        },
                        "hobbies": [{
                        "type": random.choice(["Sports", "Music"]),
                        "name": "Jewelry",
                        "details": {
                            "location": {
                            "lat": 37.51093566196537,
                            "lon": 46.31837025664398
                            }
                        }
                        }]
                    },
                    "gender": random.choice(["M", "F"]),
                    "marital": random.choice(["W", "D", "M"]),
                    "mutated": 0,
                    "body": "efibuhebfljqkwndhbilhdeijhdlihdeilhiuwedfiewbfilbeifleifiwb" \
                            "iewufbhuiebfikbehiuwsibgiuqbvdhjwvbdjhwvdbjhwvqbhsjbqjsbsjhb"
                    }
                key = key_prefix + "_" + str(j)
                res = sdk_client_for_load.insert(key, doc)
                if res["status"] == True:
                    local_item_count += 1
            global success_insert_count
            success_insert_count += local_item_count
            sdk_client_for_load.close()

        thread_count = 10
        items_per_thread = int(num_items // thread_count)
        self.log.info("Inserting {} items into the new collection".format(num_items))
        for i in range(thread_count):
            key = "json_doc" + str(i+1)
            t = Thread(target=insert_docs, args=[self.cluster.master, bucket,
                                                 items_per_thread, key])
            t.start()
            data_load_threads.append(t)

        for th in data_load_threads:
            th.join()
        return success_insert_count

    def create_new_indexes_for_new_docs(self):

        indexes = ['create index `new_index1` on `bucket-0`.`myscope`.`mycoll`(age) where age between 30 and 50;',
           'create index `new_index2` on`bucket-0`.`myscope`.`mycoll`(marital,age);',
           'create index `new_index3` on `bucket-0`.`myscope`.`mycoll`(ALL `animals`,`attributes`.`hair`,`name`) where attributes.hair = "Burgundy";',
           'CREATE INDEX `new_index4` ON `bucket-0`.`myscope`.`mycoll`(`gender`,`attributes`.`hair`, DISTINCT ARRAY `hobby`.`type` FOR hobby in `attributes`.`hobbies` END) where gender="F" and attributes.hair = "Burgundy";',
           'create index `new_index5` on `bucket-0`.`myscope`.`mycoll`(`gender`,`attributes`.`dimensions`.`weight`, `attributes`.`dimensions`.`height`,`name`);']

        idx_prefix = "new_index"
        count = 0
        self.log.info("Creating indexes for the new collection")
        for idx_query in indexes:
            idx_name = idx_prefix + str(count)
            self.log.info("Index query = {}".format(idx_query))
            result = self.query_client.query_tool(idx_query)
            self.log.info("Result for creation of {0} = {1}".format(idx_name, result))
            if result['status'] == 'success':
                self.index_count += 1
            count += 1

    def create_indexes_pre_upgrade(self):
        self.PrintStep("Performing indexing specific steps before upgrade")
        self.index_count = 0
        total_coll_in_bucket = \
            self.spec_bucket[MetaConstants.NUM_SCOPES_PER_BUCKET] * \
                self.spec_bucket[MetaConstants.NUM_COLLECTIONS_PER_SCOPE]
        index_query_node = self.cluster.index_nodes[0]
        rest = RestConnection(index_query_node)
        self.log.info("Setting indexer params")
        if self.redistribute_indexes:
            rest.set_indexer_params(redistributeIndexes='true')
        sdk_client = SDKClient(self.cluster, None,
                               servers=[index_query_node])
        self.create_primary_indexes()
        if self.create_partitioned_indexes:
            self.create_primary_partitioned_indexes()
            self.log.info("Creating partitioned indexes...")
            self.create_secondary_indexes(partitioned=True, replica=False,
                                            index_suffix='sec_part_index',
                                            field='name')
            self.log.info("Creating secondary indexes on the field 'body' ")
            self.create_secondary_indexes(index_suffix='sec_body_index',
                                            field='body')
            self.log.info("Creating secondary indexes on the field 'mutation_type' ")
            self.create_secondary_indexes(index_suffix='sec_mutation_index',
                                            field='mutation_type')
        CollectionBase.create_indexes_for_all_collections(self, sdk_client)
        self.index_count += total_coll_in_bucket
        new_collection_docs = 1000000
        self.items_inserted = self.insert_json_docs_sdk(new_collection_docs)
        self.log.info("{} items were successfully inserted".format(self.items_inserted))
        self.create_new_indexes_for_new_docs()
        self.log.info("Initial index count = {}".format(self.index_count))
        sdk_client.close()
