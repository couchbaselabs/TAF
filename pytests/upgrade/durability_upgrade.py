import time
from random import choice
from threading import Thread

from BucketLib.bucket import Bucket
from Cb_constants import DocLoading, CbServer
import testconstants
from bucket_utils.bucket_ready_functions import DocLoaderUtils
from cb_tools.cbstats import Cbstats
from collections_helper.collections_spec_constants import MetaCrudParams, MetaConstants
from couchbase_helper.documentgenerator import doc_generator
from couchbase_helper.durability_helper import DurabilityHelper, \
    BucketDurability
from membase.api.rest_client import RestConnection
from platform_utils.remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient, SDKClientPool
from sdk_exceptions import SDKException
from StatsLib.StatsOperations import StatsHelper
from upgrade.upgrade_base import UpgradeBase
from bucket_collections.collections_base import CollectionBase


class UpgradeTests(UpgradeBase):
    def setUp(self):
        super(UpgradeTests, self).setUp()
        self.durability_helper = DurabilityHelper(
            self.log,
            len(self.cluster.nodes_in_cluster))
        self.verification_dict = dict()
        self.verification_dict["ops_create"] = self.num_items
        self.verification_dict["ops_delete"] = 0

    def tearDown(self):
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
        DocLoaderUtils.sdk_client_pool = SDKClientPool()
        self.log.info("Creating required SDK clients for client_pool")
        clients_per_bucket = \
            int(self.thread_to_use / len(self.cluster.buckets))
        for bucket in self.cluster.buckets:
            DocLoaderUtils.sdk_client_pool.create_clients(
                bucket, [self.cluster.master], clients_per_bucket,
                compression_settings=self.sdk_compression)

        # Client based scope/collection crud tests
        client = \
            DocLoaderUtils.sdk_client_pool.get_client_for_bucket(self.bucket)
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
        DocLoaderUtils.sdk_client_pool.release_client(client)

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
        CollectionBase.set_retry_exceptions(self, collection_load_spec)
        collection_task = \
            self.bucket_util.run_scenario_from_spec(self.task,
                                                    self.cluster,
                                                    self.cluster.buckets,
                                                    collection_load_spec,
                                                    mutation_num=1,
                                                    batch_size=500)
        if collection_task.result is False:
            self.log_failure("Collection task failed")
            return

        if collection_task.result is True:
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
                                                    batch_size=500)
        if collection_task.result is False:
            self.log_failure("Drop scope/collection failed")
            return

        if collection_task.result is True:
            self.log.info("Task 2 completed")
            if not self.upgrade_with_data_load and self.upgrade_type == "offline":
                self.__wait_for_persistence_and_validate()

        DocLoaderUtils.sdk_client_pool.shutdown()

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
        self.install_version_on_node(
            [self.cluster.servers[self.nodes_init]],
            self.upgrade_version)
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
        iter = 0

        ### Considering buckets with less than 100% DGM and durability=none for data load###
        for bucket in self.cluster.buckets:
            bucket_items = self.bucket_util.get_expected_total_num_items(bucket)
            bucket_ram = bucket.ramQuotaMB
            ram_in_gb = bucket_ram / 1024

            if ram_in_gb > 0 and (bucket_items / ram_in_gb > 1000000 and bucket.durability_level == "none"):
                self.buckets_to_load.append(bucket)

        if len(self.buckets_to_load) == 0:
            self.buckets_to_load.append(self.cluster.buckets[0])

        ### Upserting all docs to increase fragmentation value ###
        ### Compaction operation is called once frag val hits 50% ###
        if self.magma_upgrade:
            self.PrintStep("Upserting docs to increase fragmentation value")
            self.upsert_docs_to_increase_frag_val()

        ### Fetching the first node to upgrade ###
        node_to_upgrade = self.fetch_node_to_upgrade()

        self.PrintStep("Upgrade begins...")
        ### Each node in the cluster is upgraded iteratively ###
        while node_to_upgrade is not None:

            if self.upgrade_with_data_load:
                ### Loading docs with size > 1MB ###
                if self.load_large_docs:
                    self.loading_large_documents(large_docs_start_num)

                ### Starting async subsequent data load just before the upgrade starts ###
                ''' Skipping this step for online swap because data load for swap rebalance 
                    is started after the rebalance function is called '''
                if self.upgrade_type != "online_swap":
                    sub_load_spec = self.bucket_util.get_crud_template_from_package(
                        self.sub_data_spec)
                    CollectionBase.over_ride_doc_loading_template_params(self,
                                                                         sub_load_spec)
                    CollectionBase.set_retry_exceptions_for_initial_data_load(
                        self, sub_load_spec)

                    if self.alternate_load is True:
                        sub_load_spec["doc_crud"][
                            MetaCrudParams.DocCrud.READ_PERCENTAGE_PER_COLLECTION] = 10
                        sub_load_spec["doc_crud"][
                            MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 10

                    update_task = self.bucket_util.run_scenario_from_spec(
                        self.task,
                        self.cluster,
                        self.buckets_to_load,
                        sub_load_spec,
                        mutation_num=0,
                        async_load=True,
                        batch_size=500,
                        process_concurrency=4)

            ### The upgrade procedure starts ###
            self.log.info("Selected node for upgrade: %s" % node_to_upgrade.ip)

            ### Based on the type of upgrade, the upgrade function is called ###
            if self.upgrade_type in ["failover_delta_recovery",
                                     "failover_full_recovery"]:
                self.upgrade_function[self.upgrade_type](node_to_upgrade)
            elif self.upgrade_type == "full_offline":
                self.upgrade_function[self.upgrade_type](
                    self.cluster.nodes_in_cluster, self.upgrade_version)
            else:
                self.upgrade_function[self.upgrade_type](node_to_upgrade,
                                                         self.upgrade_version)

            ### Upgrade of one node complete ###

            self.cluster_util.print_cluster_stats(self.cluster)

            ### Stopping the data load ###
            if self.upgrade_with_data_load and self.upgrade_type != "online_swap":
                update_task.stop_indefinite_doc_loading_tasks()
                self.task_manager.get_task_result(update_task)

            if self.load_large_docs:
                large_docs_start_num += 1000

            self.sleep(10, "Wait for items to get reflected")

            ### Performing sync write while the cluster is in mixed mode ###
            ### Sync write is performed after the upgrade of each node ###
            sync_load_spec = self.bucket_util.get_crud_template_from_package(
                self.sync_write_spec)
            CollectionBase.over_ride_doc_loading_template_params(self, sync_load_spec)
            CollectionBase.set_retry_exceptions_for_initial_data_load(self, sync_load_spec)

            sync_load_spec[MetaCrudParams.DURABILITY_LEVEL] = Bucket.DurabilityLevel.MAJORITY

            ### Collections are dropped and re-created while the cluster is mixed mode ###
            if self.cluster_supports_collections and self.perform_collection_ops:
                self.log.info("Performing collection ops in mixed mode cluster setting...")
                sync_load_spec[MetaCrudParams.COLLECTIONS_TO_DROP] = 4
                sync_load_spec[MetaCrudParams.COLLECTIONS_TO_RECREATE] = 2
                sync_load_spec["doc_crud"][
                    MetaCrudParams.DocCrud.NUM_ITEMS_FOR_NEW_COLLECTIONS] = self.items_per_col

            # Validate sync_write results after upgrade
            self.PrintStep("Sync Write task starting...")
            sync_write_task = self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.buckets_to_load,
                sync_load_spec,
                mutation_num=0,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                validate_task=True)

            if sync_write_task.result is True:
                self.log.info("SyncWrite task succeeded")
            else:
                self.log_failure("SyncWrite failed during upgrade")

            ### Fetching the next node to upgrade ###
            node_to_upgrade = self.fetch_node_to_upgrade()
            iter += 1

            self.PrintStep("Upgrade of node {0} done".format(iter))
            if iter < self.nodes_init:
                self.PrintStep("Starting the upgrade of the next node")

            # Halt further upgrade if test has failed during current upgrade
            if self.test_failure is not None:
                break

            ''' Break out of the while loop, because full_offline upgrade procedure
                upgrades all of the nodes at once '''
            if self.upgrade_type == "full_offline":
                break

        ### Printing cluster stats after the upgrade of the whole cluster ###
        self.cluster_util.print_cluster_stats(self.cluster)
        self.PrintStep("Upgrade of the whole cluster complete")

        if self.disk_location_data != testconstants.COUCHBASE_DATA_PATH or \
                self.disk_location_index != testconstants.COUCHBASE_DATA_PATH:
            self.verify_custom_path_post_upgrade()

        ### Swap Rebalance/Failover recovery of all nodes post upgrade ###
        if self.complete_cluster_swap:
            if self.upgrade_type in ["failover_delta_recovery", "failover_full_recovery"]:
                self.failover_recovery_all_nodes()
            else:
                self.swap_rebalance_all_nodes()

        self.ver = float(self.upgrade_version[:3])

        ### Enabling CDC ###
        if self.magma_upgrade and self.ver >= 7.2:
            self.enable_cdc_and_get_vb_details()

        if self.load_large_docs and self.upgrade_with_data_load:
            self.update_item_count_after_loading_large_docs()

        self.log.info("starting doc verification")
        self.__wait_for_persistence_and_validate()
        self.log.info("Final doc count verified")

        # Play with collection if upgrade was successful
        if not self.test_failure:
            self.__play_with_collection()

        ### Verifying start sequence numbers ###
        if self.magma_upgrade and self.ver >= 7.2:
            self.verify_history_start_sequence_numbers()

        self.cluster_util.print_cluster_stats(self.cluster)

        ### Editing history for a few collections after enabling CDC ###
        if self.magma_upgrade and self.ver >= 7.2:
            self.edit_history_for_collections_existing_bucket()

        ### Creation of a new bucket post upgrade ###
        if self.magma_upgrade:
            self.create_new_bucket_post_upgrade_and_load_data()

        ### Rebalance/failover tasks after the whole cluster is upgraded ###
        if self.rebalance_op != "None":
            self.PrintStep("Starting rebalance/failover tasks post upgrade...")
            self.tasks_post_upgrade()

        # Perform final collection/doc_count validation
        self.validate_test_failure()

    def test_bucket_durability_upgrade(self):
        update_task = None
        self.sdk_timeout = 60
        create_batch_size = 10000
        if self.atomicity:
            create_batch_size = 10

        # To make sure sync_write can we supported by initial cluster version
        sync_write_support = True
        if float(self.initial_version[0:3]) < 6.5:
            sync_write_support = False

        if sync_write_support:
            self.verification_dict["rollback_item_count"] = 0
            self.verification_dict["sync_write_aborted_count"] = 0

        if self.upgrade_with_data_load:
            self.log.info("Starting async doc updates")
            update_task = self.task.async_continuous_doc_ops(
                self.cluster, self.bucket, self.gen_load,
                op_type=DocLoading.Bucket.DocOps.UPDATE,
                process_concurrency=1,
                persist_to=1,
                replicate_to=1,
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
                    self.cluster.servers[self.nodes_init - 1])

            create_gen = doc_generator(self.key, self.num_items,
                                       self.num_items + create_batch_size)
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
                    sdk_client_pool=self.sdk_client_pool,
                    skip_read_on_error=True,
                    suppress_error_table=True)
            self.task_manager.get_task_result(sync_write_task)
            self.num_items += create_batch_size

            retry_index = 0
            while retry_index < 5:
                self.sleep(3, "Wait for num_items to match")
                current_items = self.bucket_util.get_bucket_current_item_count(
                    self.cluster, self.bucket)
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
        possible_d_levels[Bucket.Type.MEMBASE] = \
            self.bucket_util.get_supported_durability_levels()
        possible_d_levels[Bucket.Type.EPHEMERAL] = [
            Bucket.DurabilityLevel.NONE,
            Bucket.DurabilityLevel.MAJORITY]
        len_possible_d_levels = len(possible_d_levels[self.bucket_type]) - 1

        if not sync_write_support:
            self.verification_dict["rollback_item_count"] = 0
            self.verification_dict["sync_write_aborted_count"] = 0

        # Perform bucket_durability update
        key, value = doc_generator("b_durability_doc", 0, 1).next()
        client = SDKClient([self.cluster.master], self.cluster.buckets[0])
        for index, d_level in enumerate(possible_d_levels[self.bucket_type]):
            self.log.info("Updating bucket_durability=%s" % d_level)
            self.bucket_util.update_bucket_property(
                self.cluster.master,
                self.cluster.buckets[0],
                bucket_durability=BucketDurability[d_level])
            self.bucket_util.print_bucket_stats(self.cluster)

            buckets = self.bucket_util.get_all_buckets(self.cluster)
            if buckets[0].durability_level != BucketDurability[d_level]:
                self.log_failure("New bucket_durability not taken")

            self.summary.add_step("Update bucket_durability=%s" % d_level)

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
                    sync=self.sync, defer=self.defer,
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
                    self.cluster.servers[self.nodes_init - 1])

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
                                   self.num_items * 2)
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
            sync=self.sync, defer=self.defer,
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
            persist_to=1,
            replicate_to=1,
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
                    self.cluster.servers[self.nodes_init - 1])

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

    def get_high_cardinality_metrics(self, parse):
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

        for server in self.cluster.nodes_in_cluster:
            cb_shell = RemoteMachineShellConnection(server)
            cb_obj = Cbstats(cb_shell)
            frag_res = cb_obj.magma_stats(self.buckets_to_load[0], field_to_grep,
                                            "kvstore")
            frag_dict[server.ip] = frag_res
            server_frag[server.ip] = float(
                frag_dict[server.ip][field_to_grep]["Fragmentation"])
            cb_shell.disconnect()

        self.log.info(
            "Fragmentation after initial load {0}".format(server_frag))

        ### Upserting all data to increase fragmentation value ###
        upsert_spec = self.bucket_util.get_crud_template_from_package(
            self.upsert_data_spec)
        CollectionBase.over_ride_doc_loading_template_params(self, upsert_spec)
        CollectionBase.set_retry_exceptions_for_initial_data_load(self,
                                                                  upsert_spec)

        if self.alternate_load is True:
            upsert_spec["doc_crud"][
                MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 20

        upsert_task = self.bucket_util.run_scenario_from_spec(
            self.task,
            self.cluster,
            self.buckets_to_load,
            upsert_spec,
            mutation_num=0,
            batch_size=self.batch_size,
            process_concurrency=self.process_concurrency)

        ### Fetching fragmentation value after upserting data ###
        if upsert_task.result is True:
            self.log.info("Upsert task finished.")

            frag_dict = dict()
            server_frag = dict()

            for server in self.cluster.nodes_in_cluster:
                cb_shell = RemoteMachineShellConnection(server)
                cb_obj = Cbstats(cb_shell)
                frag_res = cb_obj.magma_stats(self.buckets_to_load[0], field_to_grep,
                                                "kvstore")
                frag_dict[server.ip] = frag_res
                server_frag[server.ip] = float(
                    frag_dict[server.ip][field_to_grep]["Fragmentation"])
                cb_shell.disconnect()

            self.log.info(
                "Fragmentation after upsert {0}".format(server_frag))

            if int(self.initial_version[0]) >= 7:
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
            self.cluster, self.buckets_to_load[0],
            gen_create, DocLoading.Bucket.DocOps.CREATE, exp=0,
            durability=self.durability_level,
            timeout_secs=self.sdk_timeout,
            sdk_client_pool=self.sdk_client_pool,
            process_concurrency=4,
            skip_read_on_error=True,
            suppress_error_table=True)

    def update_item_count_after_loading_large_docs(self):
        self.sleep(30, "Wait for items to get reflected")
        prev_count = \
        self.buckets_to_load[0].scopes[CbServer.default_scope].collections[
            CbServer.default_collection].num_items
        total_count = 0
        for server in self.cluster.nodes_in_cluster:
            shell = RemoteMachineShellConnection(server)
            cbstat_obj = Cbstats(shell)
            default_count_dict = cbstat_obj.magma_stats(self.buckets_to_load[0],
                                                        field_to_grep="items",
                                                        stat_name="collections _default._default")
            for key in default_count_dict:
                total_count += default_count_dict[key]
            shell.disconnect()
        large_doc_count = total_count - prev_count
        self.log.info("Large doc count = {0}".format(large_doc_count))
        self.buckets_to_load[0].scopes[CbServer.default_scope].collections[
            CbServer.default_collection].num_items += large_doc_count

    def enable_cdc_and_get_vb_details(self):
        self.PrintStep("Enabling CDC")
        shell = RemoteMachineShellConnection(self.cluster.master)
        cbstat_obj = Cbstats(shell)

        self.bucket_util.update_bucket_property(self.cluster.master,
                                                self.buckets_to_load[0],
                                                history_retention_seconds=86400,
                                                history_retention_bytes=96000000000)
        self.log.info("CDC Enabled - History parameters set")
        self.sleep(60, "Wait for History params to get reflected")

        history_check = cbstat_obj.magma_stats(self.buckets_to_load[0],
                                                field_to_grep="history_retention",
                                                stat_name="all")
        self.log.info(history_check)

        self.vb_dict = self.bucket_util.get_vb_details_for_bucket(self.buckets_to_load[0],
                                                             self.cluster.nodes_in_cluster)

        shell.disconnect()

    def verify_history_start_sequence_numbers(self):
        self.log.info("Verifying history start sequence numbers")
        vb_dict1 = self.bucket_util.get_vb_details_for_bucket(self.buckets_to_load[0],
                                                                self.cluster.nodes_in_cluster)

        mismatch_count = 0
        for vb_no in range(1024):
            mismatch = 0
            # Verifying active vbuckets
            vb_active_seq = self.vb_dict[vb_no]['active']['high_seqno']
            vb_active_hist = vb_dict1[vb_no]['active'][
                'history_start_seqno']
            if vb_active_hist < vb_active_seq:
                mismatch = 1

            # Verifying replica vbuckets
            replica_list1 = self.vb_dict[vb_no]['replica']
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

        if self.rebalance_op == "all":
            rebalance_tasks = ["rebalance_in", "rebalance_out",
                               "swap_rebalance",
                               "failover_delta", "failover_full",
                               "replica_update"]
        else:
            rebalance_tasks.append(self.rebalance_op)

        for reb_task in rebalance_tasks:

            rebalance_data_load = None
            if data_load:
                rebalance_data_load = self.load_during_rebalance(self.sub_data_spec,
                                                                 async_load=True)

            if reb_task == "rebalance_in":
                self.install_version_on_node([self.spare_node],
                                             self.upgrade_version)

                rest = RestConnection(self.cluster.master)
                services = rest.get_nodes_services()
                print(services)
                self.cluster.master.port = CbServer.port
                services_on_master = services[(self.cluster.master.ip + ":"
                                               + str(self.cluster.master.port))]
                if self.enable_tls and self.tls_level == "strict":
                    self.cluster.master.port = CbServer.ssl_port

                rest.add_node(self.creds.rest_username,
                              self.creds.rest_password,
                              self.spare_node.ip,
                              self.spare_node.port,
                              services=services_on_master)
                otp_nodes = [node.id for node in rest.node_statuses()]
                self.log.info("Rebalance starting...")
                self.log.info(
                    "Rebalancing-in the node {0}".format(self.spare_node.ip))
                rest.rebalance(otpNodes=otp_nodes, ejectedNodes=[])
                rebalance_result = rest.monitorRebalance()

                if rebalance_result:
                    self.log.info("Rebalance-in passed successfully")

                self.cluster_util.print_cluster_stats(self.cluster)

            elif reb_task == "rebalance_out":

                rest = RestConnection(self.cluster.master)

                nodes = rest.node_statuses()
                for node in nodes:
                    if node.ip == self.spare_node.ip:
                        eject_node = node
                        break
                otp_nodes = [node.id for node in rest.node_statuses()]
                self.log.info("Rebalance-out starting...")
                self.log.info(
                    "Rebalancing out the node {0}".format(eject_node.ip))
                rest.rebalance(otpNodes=otp_nodes,
                               ejectedNodes=[eject_node.id])
                rebalance_passed = rest.monitorRebalance()

                if rebalance_passed:
                    self.log.info("Rebalance-out of the node successful")
                    self.cluster_util.print_cluster_stats(self.cluster)

            elif reb_task == "swap_rebalance":
                self.install_version_on_node([self.spare_node],
                                             self.upgrade_version)

                rest = RestConnection(self.cluster.master)
                services = rest.get_nodes_services()
                self.cluster.master.port = CbServer.port
                services_on_target_node = services[
                    (self.cluster.master.ip + ":"
                     + str(self.cluster.master.port))]
                if self.enable_tls and self.tls_level == "strict":
                    self.cluster.master.port = CbServer.ssl_port

                self.node_to_remove = self.cluster.master

                rebalance_passed = self.task.rebalance(
                    self.cluster_util.get_nodes(self.cluster.master),
                    to_add=[self.spare_node],
                    to_remove=[self.cluster.master],
                    check_vbucket_shuffling=False,
                    services=[",".join(services_on_target_node)])

                if rebalance_passed:
                    self.log.info("Swap Rebalance successful")
                    self.cluster.master = self.spare_node
                    self.spare_node = self.node_to_remove
                    self.cluster_util.print_cluster_stats(self.cluster)
                else:
                    self.log.info("Swap Rebalance failed")

            elif reb_task == "failover_delta":
                rest = RestConnection(self.cluster.master)
                nodes = rest.node_statuses()
                for node in nodes:
                    if node.ip != self.cluster.master.ip:
                        otp_node = node
                        break

                self.log.info("Failing over the node {0}".format(otp_node.ip))
                failover_task = rest.fail_over(otp_node.id, graceful=True)

                if failover_task:
                    self.log.info("Graceful Failover of the node successful")
                else:
                    self.log.info("Failover failed")

                rebalance_passed = rest.monitorRebalance()

                if rebalance_passed:
                    self.log.info("Failover rebalance passed")
                    self.cluster_util.print_cluster_stats(self.cluster)

                rest.set_recovery_type(otp_node.id,
                                       recoveryType="delta")

                delta_recovery_buckets = [bucket.name for bucket in
                                          self.cluster.buckets]
                self.log.info("Rebalance starting...")
                rest.rebalance(
                    otpNodes=[node.id for node in rest.node_statuses()],
                    deltaRecoveryBuckets=delta_recovery_buckets)
                rebalance_passed = rest.monitorRebalance()

                if rebalance_passed:
                    self.log.info("Rebalance after recovery completed")
                    self.cluster_util.print_cluster_stats(self.cluster)

            elif reb_task == "failover_full":
                rest = RestConnection(self.cluster.master)
                nodes = rest.node_statuses()
                for node in nodes:
                    if node.ip != self.cluster.master.ip:
                        otp_node = node
                        break

                self.log.info("Failing over the node {0}".format(otp_node.ip))
                failover_task = rest.fail_over(otp_node.id, graceful=False)

                if failover_task:
                    self.log.info("Hard Failover of the node successful")
                else:
                    self.log.info("Failover failed")

                rebalance_passed = rest.monitorRebalance()

                if rebalance_passed:
                    self.log.info("Failover rebalance passed")
                    self.cluster_util.print_cluster_stats(self.cluster)

                rest.set_recovery_type(otp_node.id, recoveryType="full")

                self.log.info("Rebalance starting...")
                rest.rebalance(
                    otpNodes=[node.id for node in rest.node_statuses()])
                rebalance_passed = rest.monitorRebalance()

                if rebalance_passed:
                    self.log.info("Rebalance after recovery completed")
                    self.cluster_util.print_cluster_stats(self.cluster)

            elif reb_task == "replica_update":
                rest = RestConnection(self.cluster.master)
                services = rest.get_nodes_services()
                self.cluster.master.port = CbServer.port
                services_on_master = services[(self.cluster.master.ip + ":"
                                               + str(self.cluster.master.port))]
                if self.enable_tls and self.tls_level == "strict":
                    self.cluster.master.port = CbServer.ssl_port

                rest.add_node(self.creds.rest_username,
                              self.creds.rest_password,
                              self.spare_node.ip,
                              self.spare_node.port,
                              services=services_on_master)
                otp_nodes = [node.id for node in rest.node_statuses()]
                self.log.info("Rebalance starting...")
                self.log.info(
                    "Rebalancing-in the node {0}".format(self.spare_node.ip))
                rest.rebalance(otpNodes=otp_nodes, ejectedNodes=[])
                rebalance_result = rest.monitorRebalance()

                if rebalance_result:
                    self.log.info("Rebalance-in of the node successful")
                else:
                    self.log.info("Rebalance-in failed")

                self.cluster_util.print_cluster_stats(self.cluster)

                for bucket in self.cluster.buckets:
                    if bucket.name == "bucket-0":
                        bucket_update = bucket

                self.log.info(
                    "Updating replica count from 2 to 3 for {0}".format(
                        bucket_update.name))

                self.bucket_util.update_bucket_property(self.cluster.master,
                                                        bucket_update,
                                                        replica_number=3)

                self.bucket_util.print_bucket_stats(self.cluster)
                otp_nodes = [node.id for node in rest.node_statuses()]

                rest.rebalance(otpNodes=otp_nodes, ejectedNodes=[])
                reb_result = rest.monitorRebalance()

                if reb_result:
                    self.log.info("Rebalance after replica update successful")
                else:
                    self.log.info("Rebalance after replica update failed")

                self.cluster_util.print_cluster_stats(self.cluster)

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
            Bucket.storageBackend: self.spec_bucket[Bucket.storageBackend],
            Bucket.evictionPolicy: self.spec_bucket[Bucket.evictionPolicy]
        }

        CollectionBase.over_ride_bucket_template_params(self, new_bucket_spec)
        self.bucket_util.create_buckets_using_json_data(self.cluster, new_bucket_spec)
        self.bucket_util.wait_for_collection_creation_to_complete(self.cluster)

        self.bucket_util.print_bucket_stats(self.cluster)

        for bucket in self.cluster.buckets:
            if bucket.name == "new-bucket":
                new_bucket = bucket

        self.sdk_client_pool.shutdown()
        DocLoaderUtils.sdk_client_pool = SDKClientPool()
        self.log.info("Creating required SDK clients for the new bucket")
        clients_per_bucket = int(self.thread_to_use / len(self.cluster.buckets))
        for bucket in self.cluster.buckets:
            DocLoaderUtils.sdk_client_pool.create_clients(
                bucket, [self.cluster.master], clients_per_bucket,
                compression_settings=self.sdk_compression)

        self.log.info("Loading data into the new bucket...")
        new_bucket_load = self.bucket_util.get_crud_template_from_package(
            self.initial_data_spec)
        CollectionBase.over_ride_doc_loading_template_params(self, new_bucket_load)
        CollectionBase.set_retry_exceptions_for_initial_data_load(self, new_bucket_load)

        data_load_task = self.bucket_util.run_scenario_from_spec(
                                self.task,
                                self.cluster,
                                [new_bucket],
                                new_bucket_load,
                                mutation_num=0,
                                async_load=False,
                                batch_size=self.batch_size,
                                process_concurrency=self.process_concurrency)

        if data_load_task.result is True:
            self.log.info("Data load for the new bucket complete")
            time.sleep(10)
            self.bucket_util.print_bucket_stats(self.cluster)

        ### Creating a few collections with history = false in the new bucket ###
        if self.magma_upgrade and self.ver >= 7.2:
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
        for scope in self.buckets_to_load[0].scopes:
            scope_name = scope
            for col in self.buckets_to_load[0].scopes[scope].collections:
                coll_name = col
                self.bucket_util.set_history_retention_for_collection(self.cluster.master,
                                                                    self.buckets_to_load[0],
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
                                            self.buckets_to_load[0],
                                            scope_name=CbServer.default_scope,
                                            collection_spec=coll_obj)

    def swap_rebalance_all_nodes(self, data_load=True):
        self.PrintStep("Starting Swap Rebalance of all nodes post upgrade")
        self.spare_nodes.append(self.spare_node)

        self.log.info("Installing the version {0} on all spare nodes".format(
                                                        self.upgrade_version))
        self.install_version_on_node(self.spare_nodes, self.upgrade_version)

        nodes_to_replace = self.cluster.nodes_in_cluster

        i = 0

        for node in nodes_to_replace:
            rest = RestConnection(node)
            services = rest.get_nodes_services()
            self.cluster.master.port = CbServer.port
            services_on_target_node = services[
                    (node.ip + ":" + str(self.cluster.master.port))]
            if self.enable_tls and self.tls_level == "strict":
                self.cluster.master.port = CbServer.ssl_port

            node_to_add = self.spare_nodes[i]

            rebalance_data_load = None
            if data_load:
                rebalance_data_load = self.load_during_rebalance(self.sub_data_spec,
                                                                 async_load=True)

            rebalance_passed = self.task.rebalance(
                        self.cluster_util.get_nodes(self.cluster.master),
                        to_add=[node_to_add],
                        to_remove=[node],
                        check_vbucket_shuffling=False,
                        services=[",".join(services_on_target_node)])

            if rebalance_passed:
                self.log.info("Swap Rebalance successful for node {0}".format(i+1))
                i += 1
                if node.ip == self.cluster.master.ip:
                    self.cluster.master = node_to_add
            else:
                self.log.info("Swap Rebalance failed for node {0}".format(i+1))

            if rebalance_data_load is not None:
                self.task_manager.get_task_result(rebalance_data_load)

        self.cluster.nodes_in_cluster = self.spare_nodes
        self.spare_nodes = nodes_to_replace
        self.spare_node = self.spare_nodes[0]

    def failover_recovery_all_nodes(self):
        self.PrintStep("Starting Failover/Recovery of all nodes post upgrade")
        rest = RestConnection(self.cluster.master)
        nodes = rest.node_statuses()

        for otp_node in nodes:

            self.log.info("Failing over the node {0}".format(otp_node.ip))
            failover_task = rest.fail_over(otp_node.id, graceful=True)

            rebalance_passed = rest.monitorRebalance()

            if rebalance_passed:
                self.cluster_util.print_cluster_stats(self.cluster)

            rest.set_recovery_type(otp_node.id,
                                    recoveryType="full")

            self.log.info("Rebalance starting...")
            rest.rebalance(
                otpNodes=[node.id for node in rest.node_statuses()])
            rebalance_passed = rest.monitorRebalance()

            if rebalance_passed:
                self.log.info("Failover/recovery completed for node {0}".format(otp_node.ip))
                self.cluster_util.print_cluster_stats(self.cluster)
