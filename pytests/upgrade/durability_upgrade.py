import time
from random import choice
from threading import Thread

from BucketLib.bucket import Bucket
from Cb_constants import DocLoading, CbServer
from bucket_utils.bucket_ready_functions import DocLoaderUtils
from cb_tools.cbstats import Cbstats
from collections_helper.collections_spec_constants import MetaCrudParams
from constants.sdk_constants.java_client import SDKConstants
from couchbase_helper.documentgenerator import doc_generator
from couchbase_helper.durability_helper import DurabilityHelper, \
    BucketDurability
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from sdk_client3 import SDKClient, SDKClientPool
from sdk_exceptions import SDKException
from StatsLib.StatsOperations import StatsHelper
from upgrade.upgrade_base import UpgradeBase
from storage.magma.magma_base import MagmaBaseTest
from bucket_collections.collections_base import CollectionBase
from bucket_collections.collections_rebalance import CollectionsRebalance
from collections_helper.collections_spec_constants import MetaConstants

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
            cbstat_cmd="checkpoint", stat_name="persistence:num_items_for_cursor",
            timeout=300)
        self.bucket_util._wait_for_stats_all_buckets(
            self.cluster, self.cluster.buckets,
            cbstat_cmd="all", stat_name="ep_queue_size",
            timeout=60)
        self.bucket_util.validate_docs_per_collections_all_buckets(
            self.cluster,timeout=1200)

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
        
        if(collection_task.result is True):
            self.log.info("Collection task 1 completed")
            if not self.upgrade_with_data_load:
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
        
        if(collection_task.result is True):
            self.log.info("Task 2 completed")
            if not self.upgrade_with_data_load:
                self.__wait_for_persistence_and_validate()

    def test_upgrade(self):
        self.thread_keeper = dict()
        thread_list = []

        def check_res(target_node):
            self.log.info("verification for storage upgrade")
            timeout = time.time() + 7
            while time.time() < timeout:
                try:
                    rest = RestConnection(target_node)
                    node = rest.get_nodes_self()
                    if node.status == 'healthy':
                        return True
                except Exception:
                    continue
                finally:
                    self.sleep(5)
            return False

        def parallel_api_calls(target_node):
            rest = RestConnection(target_node)
            rest.username = self.test_user[0]["name"]
            rest.password = self.test_user[0]["password"]
            while self.thread_keeper[target_node] is not None:
                try:
                    for iter1 in range(4):
                        rest.cluster_status(parse=False)
                        rest.get_pools_info(parse=False)
                    for bucket in self.cluster.buckets:
                        rest.get_bucket_details(bucket.name, False)
                except Exception:
                    continue
        self.test_user = None
        iter = 0
        if self.test_storage_upgrade:
            user_name = "Random"
            user_pass = "Random"
            permissions = 'data_writer[*],data_reader[*]'
            self.role_list = [{"id": user_name,
                         "name": user_name,
                         "roles": "%s" % permissions}]
            self.test_user = [{'id': user_name, 'name': user_name,
                         'password': user_pass}]
            self.bucket_util.add_rbac_user(self.cluster.master,
                                           testuser=self.test_user,
                                           rolelist=self.role_list)
        create_batch_size = 10000
        update_task = None

        ### Fetching fragmentation value after initial load ###
        if(self.spec_bucket[Bucket.storageBackend]==Bucket.StorageBackend.magma):
            frag_dict = dict()
            server_frag = dict()
            field_to_grep = "rw_0:magma"

            for server in self.cluster.nodes_in_cluster:
                cb_shell = RemoteMachineShellConnection(server)
                cb_obj = Cbstats(cb_shell)
                frag_res = cb_obj.magma_stats(self.cluster.buckets[0], field_to_grep, "kvstore")
                frag_dict[server.ip] = frag_res
                server_frag[server.ip] = float(frag_dict[server.ip][field_to_grep]["Fragmentation"])
                cb_shell.disconnect()

            self.log.info("Fragmentation after initial load {0}".format(server_frag))

        ### Upserting all data to increase fragmentation value ###
        upsert_spec = self.bucket_util.get_crud_template_from_package(self.upsert_data_spec)
        CollectionBase.over_ride_doc_loading_template_params(self, upsert_spec)
        CollectionBase.set_retry_exceptions_for_initial_data_load(self, upsert_spec)

        upsert_task = self.bucket_util.run_scenario_from_spec(
            self.task,
            self.cluster,
            self.cluster.buckets,
            upsert_spec,
            mutation_num=0,
            batch_size=self.batch_size,
            process_concurrency=self.process_concurrency)

        ### Fetching fragmentation value after upserting data ###
        if(upsert_task.result is True):
            self.log.info("Upsert task finished.")

            if(self.spec_bucket[Bucket.storageBackend]==Bucket.StorageBackend.magma):
                frag_dict = dict()
                server_frag = dict()

                for server in self.cluster.nodes_in_cluster:
                    cb_shell = RemoteMachineShellConnection(server)
                    cb_obj = Cbstats(cb_shell)
                    frag_res = cb_obj.magma_stats(self.cluster.buckets[0], field_to_grep, "kvstore")
                    frag_dict[server.ip] = frag_res
                    server_frag[server.ip] = float(frag_dict[server.ip][field_to_grep]["Fragmentation"])
                    cb_shell.disconnect()

                self.log.info("Fragmentation after upsert {0}".format(server_frag))

            self.__wait_for_persistence_and_validate()

        else:
            self.log_failure("Upsert task failed.")

        large_doc_count = 0
        self.log.info("Upgrading cluster nodes to target version")
        node_to_upgrade = self.fetch_node_to_upgrade()
        while node_to_upgrade is not None:
            iter += 1
            self.log.info("Selected node for upgrade: %s" % node_to_upgrade.ip)
            ### Subsequent data load with spec file ###
            if self.upgrade_with_data_load:
                self.log.info("Starting async doc updates")

                if(self.load_large_docs):
                    self.log.info("Loading large docs...")
                    self.sdk_for_load = SDKClient([self.cluster.master],
                                            self.cluster.buckets[0],
                                            scope=CbServer.default_scope,
                                            collection=CbServer.default_collection)

                    for i in range(10):
                        large_doc = doc_generator("large_docs", 0, 1,
                                                    doc_size=10000000,
                                                    doc_type=self.doc_type,
                                                    vbuckets=self.cluster.vbuckets,
                                                    key_size=self.key_size,
                                                    randomize_value=True)
                        key_obj, val_obj = large_doc.next()
                        key_obj = key_obj[:-1]
                        key_obj += str(large_doc_count)
                        self.sdk_for_load.insert(key_obj, val_obj)
                        self.log.info("Item {0} inserted".format(key_obj))
                        time.sleep(10)
                        read_val = self.sdk_for_load.read(key_obj, timeout=10)

                        #Upserting the doc twice
                        for j in range(2):
                            read_val["mutated"] = j
                            self.sdk_for_load.upsert(key_obj, read_val)
                            time.sleep(2)
                        large_doc_count += 1

                    self.log.info("Loading of large docs complete.")
                    self.bucket_util.print_bucket_stats(self.cluster)

                sub_load_spec = self.bucket_util.get_crud_template_from_package(self.sub_data_spec)
                CollectionBase.over_ride_doc_loading_template_params(self, sub_load_spec)
                CollectionBase.set_retry_exceptions_for_initial_data_load(self, sub_load_spec)
                update_task = self.bucket_util.run_scenario_from_spec(
                self.task,
                self.cluster,
                self.cluster.buckets,
                sub_load_spec,
                mutation_num=0,
                async_load=True,
                batch_size=self.batch_size,
                process_concurrency=self.process_concurrency,
                validate_task=True)

            if self.test_storage_upgrade:
                gen_loader = doc_generator("create", 0, 100000,
                                           randomize_doc_size=True,
                                           randomize_value=True,
                                           randomize=True)
                for bucket in self.cluster.buckets:
                    if "testBucket" in bucket.name:
                        self.task.async_continuous_doc_ops(
                            self.cluster, bucket, gen_loader,
                            op_type=DocLoading.Bucket.DocOps.CREATE,
                            process_concurrency=1,
                            persist_to=1,
                            replicate_to=1,
                            durability=Bucket.DurabilityLevel.MAJORITY,
                            timeout_secs=30)

                for node in self.cluster.nodes_in_cluster:
                    new_threads = []
                    if node.ip != node_to_upgrade.ip:
                        if node not in self.thread_keeper:
                            self.thread_keeper[node] = True
                        for i in range(7):
                            new_threads.append(Thread(target=parallel_api_calls,
                                                      args=[node]))
                        for threads in new_threads:
                            threads.start()
                            thread_list.append(threads)

            if(self.upgrade_type in ["failover_delta_recovery","failover_full_recovery"]):
                self.upgrade_function[self.upgrade_type](node_to_upgrade)
            else:
                self.upgrade_function[self.upgrade_type](node_to_upgrade,
                                                     self.upgrade_version)
            self.cluster_util.print_cluster_stats(self.cluster)
            for node in self.thread_keeper.keys():
                if node not in self.cluster.nodes_in_cluster:
                    self.thread_keeper[node] = None

            if self.upgrade_with_data_load:
                # Wait for update_task to complete
                update_task.stop_indefinite_doc_loading_tasks()
                self.task_manager.get_task_result(update_task)

            sync_load_spec = self.bucket_util.get_crud_template_from_package(self.sync_write_spec)
            CollectionBase.over_ride_doc_loading_template_params(self, sync_load_spec)
            CollectionBase.set_retry_exceptions_for_initial_data_load(self, sync_load_spec)

            if self.cluster_supports_sync_write:
                sync_load_spec[MetaCrudParams.DURABILITY_LEVEL] = Bucket.DurabilityLevel.MAJORITY

            # Validate sync_write results after upgrade
            if self.atomicity:
                create_batch_size = 10
                create_gen = doc_generator(
                    self.key,
                    self.num_items,
                    self.num_items+create_batch_size)
                sync_write_task = self.task.async_load_gen_docs_atomicity(
                    self.cluster, self.cluster.buckets,
                    create_gen, DocLoading.Bucket.DocOps.CREATE,
                    process_concurrency=1,
                    transaction_timeout=self.transaction_timeout,
                    record_fail=True)
            else:
                sync_write_task = self.bucket_util.run_scenario_from_spec(
                    self.task,
                    self.cluster,
                    self.cluster.buckets,
                    sync_load_spec,
                    mutation_num=0,
                    batch_size=self.batch_size,
                    process_concurrency=self.process_concurrency,
                    validate_task=True)

            if self.test_storage_upgrade and iter >= self.nodes_init:
                node_to_upgrade = None
            else:
                node_to_upgrade = self.fetch_node_to_upgrade()

            if(sync_write_task.result is True):
                self.log.info("Sync Write Result : {0}".format(sync_write_task.result))
                self.log.info("Sync write task finished")
            
            if self.atomicity:
                self.sleep(10)
                current_items = self.bucket_util.get_bucket_current_item_count(
                    self.cluster, self.bucket)
                if node_to_upgrade is None:
                    if current_items < self.num_items+create_batch_size:
                        self.log_failure(
                            "Failures after cluster upgrade {} {}"
                            .format(current_items,
                                    self.num_items+create_batch_size))
                elif current_items > self.num_items:
                    self.log_failure(
                        "SyncWrite succeeded with mixed mode cluster")
            else:
                if node_to_upgrade is None:
                    if sync_write_task.fail.keys():
                        self.log_failure("Failures after cluster upgrade")
                    else:
                        self.num_items += create_batch_size
                        self.bucket.scopes[
                            CbServer.default_scope].collections[
                            CbServer.default_collection] \
                            .num_items += create_batch_size
                elif self.cluster_supports_sync_write:
                    if sync_write_task.fail:
                        self.log.error("SyncWrite failed: %s"
                                       % sync_write_task.fail)
                        self.log_failure("SyncWrite failed during upgrade")
                    else:
                        self.num_items += create_batch_size
                        self.bucket.scopes[
                            CbServer.default_scope].collections[
                            CbServer.default_collection] \
                            .num_items += create_batch_size
                        create_gen = doc_generator(
                            self.key,
                            self.num_items,
                            self.num_items + create_batch_size)
                elif len(sync_write_task.fail.keys()) != create_batch_size:
                    self.log_failure(
                        "SyncWrite succeeded with mixed mode cluster")
                else:
                    for doc_id, doc_result in sync_write_task.fail.items():
                        if SDKException.FeatureNotAvailableException \
                                not in str(doc_result["error"]):
                            self.log_failure("Invalid exception for %s: %s"
                                             % (doc_id, doc_result))

            # Halt further upgrade if test has failed during current upgrade
            if self.test_failure is not None:
                break
        if self.test_storage_upgrade:
            self.sleep(30, "waiting to try cause storage upgrade failure")
            node_to_check = None
            for node in self.thread_keeper.keys():
                if self.thread_keeper[node] is not None:
                    node_to_check = node
                    break
            status = check_res(node_to_check)
            self.assertTrue(status, "expected nodes to be warmed up")
            for node in self.thread_keeper.keys():
                self.thread_keeper[node] = None
            for threads in thread_list:
                threads.join()
            retry = 2
            for i in range(retry):
                bucket_found = False
                for bucket in self.cluster.buckets:
                    if "testBucket" in bucket.name:
                        bucket_found = True
                        self.bucket_util.delete_bucket(self.cluster, bucket,
                                                       wait_for_bucket_deletion=True)
                if not bucket_found:
                    break

        ### Enabling CDC ###
        if(self.spec_bucket[Bucket.storageBackend]==Bucket.StorageBackend.magma):
            shell = RemoteMachineShellConnection(self.cluster.master)
            cbstat_obj = Cbstats(shell)

            self.bucket_util.update_bucket_property(self.cluster.master,
                                                    self.cluster.buckets[0],
                                                    history_retention_seconds=86400,
                                                    history_retention_bytes=96000000000)
            self.log.info("CDC Enabled - History parameters set")

            self.sleep(60, "Wait for History params to get reflected")

            history_check = cbstat_obj.magma_stats(self.cluster.buckets[0],
                                                field_to_grep="history_retention",
                                                stat_name="all")
            self.log.info(history_check)

            vb_dict = self.bucket_util.get_vb_details_for_bucket(self.cluster.buckets[0],
                                                                self.cluster.nodes_in_cluster)

            shell.disconnect()

        if(self.load_large_docs):
            self.cluster.buckets[0].scopes[CbServer.default_scope].collections[
                                    CbServer.default_collection].num_items += 20

        self.log.info("starting doc verification")
        self.__wait_for_persistence_and_validate()
        self.log.info("Final doc count verified")

        # Play with collection if upgrade was successful
        if not self.test_failure:
            self.__play_with_collection()

        ### Verifying start sequence numbers ###
        if(self.spec_bucket[Bucket.storageBackend]==Bucket.StorageBackend.magma):
            self.log.info("Verifying history start sequence numbers")
            vb_dict1 = self.bucket_util.get_vb_details_for_bucket(self.cluster.buckets[0],
                                                                self.cluster.nodes_in_cluster)

            mismatch_count = 0
            for vb_no in range(1024):
                mismatch = 0
                # Verifying active vbuckets
                vb_active_seq = vb_dict[vb_no]['active']['high_seqno']
                vb_active_hist = vb_dict1[vb_no]['active']['history_start_seqno']
                if(vb_active_hist<vb_active_seq):
                    mismatch = 1

                # Verifying replica vbuckets
                replica_list1 = vb_dict[vb_no]['replica']
                replica_list2 = vb_dict1[vb_no]['replica']
                for j in range(len(replica_list1)):
                    vb_replica_seq = replica_list1[j]['high_seqno']
                    vb_replica_hist = replica_list2[j]['history_start_seqno']
                    if(vb_replica_hist<vb_replica_seq):
                        mismatch = 1

                if mismatch==1:
                    mismatch_count += 1

            if(mismatch_count!=0):
                self.log.info("Start sequence mismatch in {0} vbuckets".format(mismatch_count))
            else:
                self.log.info("History start sequence numbers verified for all 1024 vbuckets")

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
