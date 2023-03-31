import time
from random import choice
from threading import Thread

from BucketLib.bucket import Bucket
from Cb_constants import DocLoading, CbServer
import testconstants
from bucket_utils.bucket_ready_functions import DocLoaderUtils
from BucketLib.BucketOperations import BucketHelper
from cb_tools.cbstats import Cbstats
from collections_helper.collections_spec_constants import MetaCrudParams
from constants.sdk_constants.java_client import SDKConstants
from couchbase_helper.documentgenerator import doc_generator
from couchbase_helper.durability_helper import DurabilityHelper, \
    BucketDurability
from membase.api.rest_client import RestConnection
from platform_utils.remote.remote_util import RemoteMachineShellConnection
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
            cbstat_cmd="checkpoint", stat_name="num_items_for_persistence",
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

        if(collection_task.result is True):
            self.log.info("Task 2 completed")
            if not self.upgrade_with_data_load and self.upgrade_type == "offline":
                self.__wait_for_persistence_and_validate()

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
                         "roles": "{0}".format(permissions)}]
            self.test_user = [{'id': user_name, 'name': user_name,
                         'password': user_pass}]
            self.bucket_util.add_rbac_user(self.cluster.master,
                                           testuser=self.test_user,
                                           rolelist=self.role_list)
        create_batch_size = 10000
        update_task = None
        large_bucket = None

        for bucket in self.cluster.buckets:
            if(bucket.name == "bucket-0"):
                large_bucket = bucket

        if(large_bucket == None):
            large_bucket = self.cluster.buckets[0]

        ### Fetching fragmentation value after initial load ###
        if(self.spec_bucket[Bucket.storageBackend]==Bucket.StorageBackend.magma):
            frag_dict = dict()
            server_frag = dict()
            field_to_grep = "rw_0:magma"

            for server in self.cluster.nodes_in_cluster:
                cb_shell = RemoteMachineShellConnection(server)
                cb_obj = Cbstats(cb_shell)
                frag_res = cb_obj.magma_stats(large_bucket, field_to_grep, "kvstore")
                frag_dict[server.ip] = frag_res
                server_frag[server.ip] = float(frag_dict[server.ip][field_to_grep]["Fragmentation"])
                cb_shell.disconnect()

            self.log.info("Fragmentation after initial load {0}".format(server_frag))

        ### Upserting all data to increase fragmentation value ###
        upsert_spec = self.bucket_util.get_crud_template_from_package(self.upsert_data_spec)
        CollectionBase.over_ride_doc_loading_template_params(self, upsert_spec)
        CollectionBase.set_retry_exceptions_for_initial_data_load(self, upsert_spec)

        if(self.alternate_load == True):
            upsert_spec["doc_crud"][MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 20

        upsert_task = self.bucket_util.run_scenario_from_spec(
            self.task,
            self.cluster,
            [large_bucket],
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
                    frag_res = cb_obj.magma_stats(large_bucket, field_to_grep, "kvstore")
                    frag_dict[server.ip] = frag_res
                    server_frag[server.ip] = float(frag_dict[server.ip][field_to_grep]["Fragmentation"])
                    cb_shell.disconnect()

                self.log.info("Fragmentation after upsert {0}".format(server_frag))

            self.__wait_for_persistence_and_validate()

        else:
            self.log_failure("Upsert task failed.")

        large_doc_count = 0
        start_num = 0
        self.log.info("Upgrading cluster nodes to target version")
        node_to_upgrade = self.fetch_node_to_upgrade()
        while node_to_upgrade is not None:
            iter += 1
            self.log.info("Selected node for upgrade: %s" % node_to_upgrade.ip)
            ### Subsequent data load with spec file ###
            task = None
            if self.upgrade_with_data_load:
                self.log.info("Starting async doc updates")

                if(self.collection_operations):
                    collection_load = self.bucket_util.get_crud_template_from_package(self.collection_spec)
                    CollectionBase.over_ride_doc_loading_template_params(self, collection_load)
                    CollectionBase.set_retry_exceptions_for_initial_data_load(self, collection_load)

                    collection_task = self.bucket_util.run_scenario_from_spec(
                        self.task,
                        self.cluster,
                        [large_bucket],
                        collection_load,
                        mutation_num=0,
                        batch_size=self.batch_size,
                        process_concurrency=self.process_concurrency)
                    
                    if(collection_task.result is True):
                        self.log.info("Collection task completed")

                self.sleep(10, "Wait for items to get reflected")

                if(self.load_large_docs):
                    
                    self.log.info("Loading large docs...")
                    gen_create = doc_generator("large_docs", start_num, start_num+1000,
                                               doc_size=1024000,
                                               randomize_value=True)

                    task = self.task.async_load_gen_docs(
                        self.cluster, large_bucket,
                        gen_create, DocLoading.Bucket.DocOps.CREATE, exp=0,
                        durability=self.durability_level,
                        timeout_secs=self.sdk_timeout,
                        sdk_client_pool=self.sdk_client_pool,
                        process_concurrency=4,
                        skip_read_on_error=True,
                        suppress_error_table=True)

                    self.log.info("Loading of large docs starting...")

                    self.bucket_util.print_bucket_stats(self.cluster)

                if(self.upgrade_type != "online_swap"):
                    sub_load_spec = self.bucket_util.get_crud_template_from_package(self.sub_data_spec)
                    CollectionBase.over_ride_doc_loading_template_params(self, sub_load_spec)
                    CollectionBase.set_retry_exceptions_for_initial_data_load(self, sub_load_spec)

                    if(self.alternate_load == True):
                        sub_load_spec["doc_crud"][MetaCrudParams.DocCrud.READ_PERCENTAGE_PER_COLLECTION] = 10
                        sub_load_spec["doc_crud"][MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 10

                    update_task = self.bucket_util.run_scenario_from_spec(
                    self.task,
                    self.cluster,
                    [large_bucket],
                    sub_load_spec,
                    mutation_num=0,
                    async_load=True,
                    batch_size=500,
                    process_concurrency=1)

            if self.test_storage_upgrade:
                gen_loader = doc_generator("create", 0, 100000,
                                           randomize_doc_size=True,
                                           randomize_value=True,
                                           randomize=True)
                for bucket in self.cluster.buckets:
                    if "testBucket" in bucket.name:
                        self.log.info("Starting async doc updates inside!!!")
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
            elif(self.upgrade_type == "full_offline"):
                self.upgrade_function[self.upgrade_type](self.cluster.nodes_in_cluster, self.upgrade_version)
            else:
                self.upgrade_function[self.upgrade_type](node_to_upgrade,
                                                     self.upgrade_version)
            self.cluster_util.print_cluster_stats(self.cluster)

            if task:
                self.task.jython_task_manager.get_task_result(task)
                start_num += 1000
            for node in self.thread_keeper.keys():
                if node not in self.cluster.nodes_in_cluster:
                    self.thread_keeper[node] = None

            if self.upgrade_with_data_load and self.upgrade_type != "online_swap":
                # Wait for update_task to complete
                update_task.stop_indefinite_doc_loading_tasks()
                self.task_manager.get_task_result(update_task)

            self.sleep(10, "Wait for items to get reflected")

            sync_load_spec = self.bucket_util.get_crud_template_from_package(self.sync_write_spec)
            CollectionBase.over_ride_doc_loading_template_params(self, sync_load_spec)
            CollectionBase.set_retry_exceptions_for_initial_data_load(self, sync_load_spec)

            if self.cluster_supports_sync_write:
                sync_load_spec[MetaCrudParams.DURABILITY_LEVEL] = Bucket.DurabilityLevel.MAJORITY

            # Validate sync_write results after upgrade
            self.log.info("Sync Write task starting...")
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
                    [large_bucket],
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
                if self.cluster_supports_sync_write:
                    if(sync_write_task.result is False):
                        self.log_failure("SyncWrite failed during upgrade")

                else:
                    for task_bucket in sync_write_task.loader_spec:
                        for scope in task_bucket["scopes"]:
                            for collections in scope["collections"]:
                                for op_type in collections:
                                    for doc_id, doc_result in op_type["fail"].items():
                                        if SDKException.FeatureNotAvailableException not in str(doc_result["error"]):
                                            self.log_failure("Invalid exception for {0}:{1}".format(doc_id, doc_result))

            # Halt further upgrade if test has failed during current upgrade
            if self.test_failure is not None:
                break

            if(self.upgrade_type == "full_offline"):
                break

        self.cluster_util.print_cluster_stats(self.cluster)
        if self.disk_location_data != testconstants.COUCHBASE_DATA_PATH or \
                self.disk_location_index != testconstants.COUCHBASE_DATA_PATH:
            self.verify_custom_path_post_upgrade()
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
                                                    large_bucket,
                                                    history_retention_seconds=86400,
                                                    history_retention_bytes=96000000000)
            self.log.info("CDC Enabled - History parameters set")

            self.sleep(60, "Wait for History params to get reflected")

            history_check = cbstat_obj.magma_stats(large_bucket,
                                                field_to_grep="history_retention",
                                                stat_name="all")
            self.log.info(history_check)

            vb_dict = self.bucket_util.get_vb_details_for_bucket(large_bucket,
                                                                self.cluster.nodes_in_cluster)

            shell.disconnect()

        if(self.load_large_docs and self.upgrade_with_data_load):
            self.sleep(30, "Wait for items to get reflected")
            prev_count = large_bucket.scopes[CbServer.default_scope].collections[
                                    CbServer.default_collection].num_items
            total_count = 0
            for server in self.cluster.nodes_in_cluster:
                shell = RemoteMachineShellConnection(server)
                cbstat_obj = Cbstats(shell)
                default_count_dict = cbstat_obj.magma_stats(large_bucket,
                                                    field_to_grep="items",
                                                    stat_name="collections _default._default")
                for key in default_count_dict:
                    total_count += default_count_dict[key]
                shell.disconnect()
            count_diff = total_count - prev_count
            self.log.info("Count diff = {0}".format(count_diff))
            large_bucket.scopes[CbServer.default_scope].collections[
                                    CbServer.default_collection].num_items += count_diff
            
        if(len(self.cluster.buckets)>1):
            dur_bucket = None
            for bucket in self.cluster.buckets:
                if(bucket.name == "bucket-1"):
                    dur_bucket = bucket

            dur_bucket.scopes[CbServer.default_scope].collections[
                                    CbServer.default_collection].num_items = 0

        self.log.info("starting doc verification")
        self.__wait_for_persistence_and_validate()
        self.log.info("Final doc count verified")

        # Play with collection if upgrade was successful
        if not self.test_failure:
            self.__play_with_collection()

        ### Verifying start sequence numbers ###
        if(self.spec_bucket[Bucket.storageBackend]==Bucket.StorageBackend.magma):
            self.log.info("Verifying history start sequence numbers")
            vb_dict1 = self.bucket_util.get_vb_details_for_bucket(large_bucket,
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

        self.cluster_util.print_cluster_stats(self.cluster)

        ### Rebalance/failover tasks after the whole cluster is upgraded ###
        if(self.rebalance_op != "None"):
            self.log.info("Starting rebalance/failover tasks post upgrade...")
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

    def tasks_post_upgrade(self):
        rebalance_tasks = []

        if(self.rebalance_op == "all"):
            rebalance_tasks = ["rebalance_in", "rebalance_out", "swap_rebalance",
                               "failover_delta", "failover_full", "replica_update"]
        else:
            rebalance_tasks.append(self.rebalance_op)

        self.log.info("Starting data load...")
        rebalance_data_spec = self.bucket_util.get_crud_template_from_package(self.sub_data_spec)
        CollectionBase.over_ride_doc_loading_template_params(self, rebalance_data_spec)
        CollectionBase.set_retry_exceptions_for_initial_data_load(self, rebalance_data_spec)

        rebalance_data_spec["doc_crud"][MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 10
        rebalance_data_spec["doc_crud"][MetaCrudParams.DocCrud.READ_PERCENTAGE_PER_COLLECTION] = 10

        if(self.alternate_load == True):
            rebalance_data_spec["doc_crud"][MetaCrudParams.DocCrud.UPDATE_PERCENTAGE_PER_COLLECTION] = 5
            rebalance_data_spec["doc_crud"][MetaCrudParams.DocCrud.READ_PERCENTAGE_PER_COLLECTION] = 5

        rebalance_data_load = self.bucket_util.run_scenario_from_spec(
                            self.task,
                            self.cluster,
                            self.cluster.buckets,
                            rebalance_data_spec,
                            mutation_num=0,
                            async_load=True,
                            batch_size=self.batch_size,
                            process_concurrency=self.process_concurrency,
                            validate_task=True)

        for reb_task in rebalance_tasks:
            if(reb_task == "rebalance_in"):
                self.install_version_on_node([self.spare_node], self.upgrade_version)

                rest = RestConnection(self.cluster.master)
                services = rest.get_nodes_services()
                services_on_master = services[(self.cluster.master.ip + ":"
                                                + str(self.cluster.master.port))]

                rest.add_node(self.creds.rest_username,
                        self.creds.rest_password,
                        self.spare_node.ip,
                        self.spare_node.port,
                        services=services_on_master)
                otp_nodes = [node.id for node in rest.node_statuses()]
                self.log.info("Rebalance starting...")
                self.log.info("Rebalancing-in the node {0}".format(self.spare_node.ip))
                rest.rebalance(otpNodes=otp_nodes, ejectedNodes=[])
                rebalance_result = rest.monitorRebalance()

                if(rebalance_result):
                    self.log.info("Rebalance-in passed successfully")

                self.cluster_util.print_cluster_stats(self.cluster)

            elif(reb_task == "rebalance_out"):

                rest = RestConnection(self.cluster.master)

                nodes = rest.node_statuses()
                for node in nodes:
                    if node.ip == self.spare_node.ip:
                        eject_node = node
                        break
                otp_nodes = [node.id for node in rest.node_statuses()]
                self.log.info("Rebalance-out starting...")
                self.log.info("Rebalancing out the node {0}".format(eject_node.ip))
                rest.rebalance(otpNodes=otp_nodes, ejectedNodes=[eject_node.id])
                rebalance_passed = rest.monitorRebalance()

                if(rebalance_passed):
                    self.log.info("Rebalance-out of the node successful")
                    self.cluster_util.print_cluster_stats(self.cluster)

            elif(reb_task == "swap_rebalance"):
                self.install_version_on_node([self.spare_node], self.upgrade_version)

                rest = RestConnection(self.cluster.master)
                cluster_nodes = rest.node_statuses()
                services = rest.get_nodes_services()
                self.cluster.master.port = CbServer.port
                services_on_target_node = services[(self.cluster.master.ip + ":"
                                                    + str(self.cluster.master.port))]

                self.node_to_remove = self.cluster.master

                rebalance_passed = self.task.rebalance(
                        self.cluster_util.get_nodes(self.cluster.master),
                        to_add=[self.spare_node],
                        to_remove=[self.cluster.master],
                        check_vbucket_shuffling=False,
                        services=[",".join(services_on_target_node)])

                if(rebalance_passed):
                    self.log.info("Swap Rebalance successful")
                    self.cluster.master = self.spare_node
                    self.spare_node = self.node_to_remove
                    self.cluster_util.print_cluster_stats(self.cluster)
                else:
                    self.log.info("Swap Rebalance failed")

            elif(reb_task == "failover_delta"):
                rest = RestConnection(self.cluster.master)
                nodes = rest.node_statuses()
                for node in nodes:
                    if node.ip != self.cluster.master.ip:
                        otp_node = node
                        break

                self.log.info("Failing over the node {0}".format(otp_node.ip))
                failover_task = rest.fail_over(otp_node.id, graceful=True)

                if(failover_task):
                    self.log.info("Graceful Failover of the node successful")
                else:
                    self.log.info("Failover failed")

                rebalance_passed = rest.monitorRebalance()

                if(rebalance_passed):
                    self.log.info("Failover rebalance passed")
                    self.cluster_util.print_cluster_stats(self.cluster)

                rest.set_recovery_type(otp_node.id,
                                recoveryType="delta")

                delta_recovery_buckets = [bucket.name for bucket in self.cluster.buckets]
                self.log.info("Rebalance starting...")
                rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()],
                        deltaRecoveryBuckets=delta_recovery_buckets)
                rebalance_passed = rest.monitorRebalance()

                if(rebalance_passed):
                    self.log.info("Rebalance after recovery completed")
                    self.cluster_util.print_cluster_stats(self.cluster)

            elif(reb_task == "failover_full"):
                rest = RestConnection(self.cluster.master)
                nodes = rest.node_statuses()
                for node in nodes:
                    if node.ip != self.cluster.master.ip:
                        otp_node = node
                        break

                self.log.info("Failing over the node {0}".format(otp_node.ip))
                failover_task = rest.fail_over(otp_node.id, graceful=False)

                if(failover_task):
                    self.log.info("Hard Failover of the node successful")
                else:
                    self.log.info("Failover failed")

                rebalance_passed = rest.monitorRebalance()

                if(rebalance_passed):
                    self.log.info("Failover rebalance passed")
                    self.cluster_util.print_cluster_stats(self.cluster)

                rest.set_recovery_type(otp_node.id, recoveryType="full")

                self.log.info("Rebalance starting...")
                rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()])
                rebalance_passed = rest.monitorRebalance()

                if(rebalance_passed):
                    self.log.info("Rebalance after recovery completed")
                    self.cluster_util.print_cluster_stats(self.cluster)

            elif(reb_task == "replica_update"):
                rest = RestConnection(self.cluster.master)
                services = rest.get_nodes_services()
                services_on_master = services[(self.cluster.master.ip + ":"
                                                + str(self.cluster.master.port))]

                rest.add_node(self.creds.rest_username,
                        self.creds.rest_password,
                        self.spare_node.ip,
                        self.spare_node.port,
                        services=services_on_master)
                otp_nodes = [node.id for node in rest.node_statuses()]
                self.log.info("Rebalance starting...")
                self.log.info("Rebalancing-in the node {0}".format(self.spare_node.ip))
                rest.rebalance(otpNodes=otp_nodes, ejectedNodes=[])
                rebalance_result = rest.monitorRebalance()

                if(rebalance_result):
                    self.log.info("Rebalance-in of the node successful")
                else:
                    self.log.info("Rebalance-in failed")

                self.cluster_util.print_cluster_stats(self.cluster)

                for bucket in self.cluster.buckets:
                    if(bucket.name == "bucket-0"):
                        bucket_update = bucket

                self.log.info("Updating replica count from 2 to 3 for {0}".format(bucket_update.name))

                self.bucket_util.update_bucket_property(self.cluster.master,
                                                        bucket_update,
                                                        replica_number=3)

                self.bucket_util.print_bucket_stats(self.cluster)
                otp_nodes = [node.id for node in rest.node_statuses()]

                rest.rebalance(otpNodes=otp_nodes, ejectedNodes= [])
                reb_result = rest.monitorRebalance()

                if(reb_result):
                    self.log.info("Rebalance after replica update successful")
                else:
                    self.log.info("Rebalance after replica update failed")

                self.cluster_util.print_cluster_stats(self.cluster)

        rebalance_data_load.stop_indefinite_doc_loading_tasks()
        self.task_manager.get_task_result(rebalance_data_load)
