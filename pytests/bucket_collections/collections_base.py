import json

from math import ceil
from random import sample

from basetestcase import ClusterSetup
from cb_constants import DocLoading, CbServer
from cb_tools.cbepctl import Cbepctl
from collections_helper.collections_spec_constants import \
    MetaConstants, MetaCrudParams
from couchbase_helper.durability_helper import DurabilityHelper
from membase.api.rest_client import RestConnection
from BucketLib.BucketOperations import BucketHelper
from BucketLib.bucket import Bucket
from remote.remote_util import RemoteMachineShellConnection
from bucket_utils.bucket_ready_functions import CollectionUtils
from cb_tools.cbstats import Cbstats
from sdk_client3 import SDKClient, SDKClientPool
from sdk_exceptions import SDKException

from com.couchbase.client.core.error import \
    IndexFailureException,\
    IndexNotFoundException, \
    InternalServerFailureException

from java.lang import Exception as Java_base_exception


class CollectionBase(ClusterSetup):
    def setUp(self):
        super(CollectionBase, self).setUp()
        self.log_setup_status("CollectionBase", "started")

        self.key = 'test_collection'.rjust(self.key_size, '0')
        self.skip_collections_during_data_load = self.input.param("skip_col_dict", None)
        self.simulate_error = self.input.param("simulate_error", None)
        self.doc_ops = self.input.param("doc_ops", None)
        self.spec_name = self.input.param("bucket_spec",
                                          "single_bucket.default")
        self.fragmentation = self.input.param("fragmentation", None)
        self.key_size = self.input.param("key_size", 8)
        self.range_scan_timeout = self.input.param("range_scan_timeout",
                                                      None)
        self.expect_range_scan_exceptions = self.input.param(
            "expect_range_scan_exceptions",
            ["com.couchbase.client.core.error.CouchbaseException: "
             "The range scan internal partition UUID could not be found on the server "])
        self.data_spec_name = self.input.param("data_spec_name",
                                               "initial_load")
        self.range_scan_collections = self.input.param("range_scan_collections", None)
        self.skip_range_scan_collection_mutation = self.input.param(
            "skip_range_scan_collection_mutation", True)
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
        self.range_scan_task = None
        if self.num_replicas > 1:
            self.num_nodes_affected = 2

        if self.doc_ops:
            self.doc_ops = self.doc_ops.split(';')

        self.durability_helper = DurabilityHelper(
            self.log, len(self.cluster.kv_nodes), self.durability_level)

        # Disable auto-failover to avoid failover of nodes
        status = RestConnection(self.cluster.master) \
            .update_autofailover_settings(False, 120)
        self.assertTrue(status, msg="Failure during disabling auto-failover")
        self.bucket_helper_obj = BucketHelper(self.cluster.master)
        self.disk_optimized_thread_settings = self.input.param("disk_optimized_thread_settings", False)
        if self.disk_optimized_thread_settings:
            self.bucket_util.update_memcached_num_threads_settings(
                self.cluster.master,
                num_writer_threads="disk_io_optimized",
                num_reader_threads="disk_io_optimized")
        try:
            self.collection_setup()
            CollectionBase.setup_collection_history_settings(self)
            CollectionBase.setup_indexing_for_dcp_oso_backfill(self)
        except Java_base_exception as exception:
            self.handle_setup_exception(exception)
        except Exception as exception:
            self.handle_setup_exception(exception)
        self.supported_d_levels = \
            self.bucket_util.get_supported_durability_levels()
        self.log_setup_status("CollectionBase", "complete")

    def tearDown(self):
        if self.range_scan_task is not None:
            self.range_scan_task.stop_task = True
            self.task.jython_task_manager.get_task_result(self.range_scan_task)
            result = CollectionUtils.get_range_scan_results(
                self.range_scan_task.fail_map, self.range_scan_task.expect_range_scan_failure, self.log)
            self.assertTrue(result, "unexpected failures in range scans")

        cbstat_obj = Cbstats(self.cluster.kv_nodes[0])
        for bucket in self.cluster.buckets:
            if bucket.bucketType != Bucket.Type.MEMCACHED:
                retry = 0
                while True:
                    try:
                        result = cbstat_obj.all_stats(bucket.name)
                        self.log.info("Bucket: %s, Active Resident ratio(DGM): %s%%"
                                      % (bucket.name,
                                         result["vb_active_perc_mem_resident"]))
                        self.log.info("Bucket: %s, Replica Resident ratio(DGM): %s%%"
                                      % (bucket.name,
                                         result["vb_replica_perc_mem_resident"]))
                        break
                    except KeyError as e:
                        if retry == 5:
                            raise e
                        retry += 1
                        self.sleep(5, "Retrying due to %s" % e)

            if not self.skip_collections_cleanup \
                    and bucket.bucketType != Bucket.Type.MEMCACHED:
                self.bucket_util.remove_scope_collections_for_bucket(
                    self.cluster, bucket)

        cbstat_obj.disconnect()

        if self.validate_docs_count_during_teardown:
            self.bucket_util.validate_docs_per_collections_all_buckets(
                self.cluster)
        if self.disk_optimized_thread_settings:
            self.bucket_util.update_memcached_num_threads_settings(
                self.cluster.master,
                num_writer_threads="default",
                num_reader_threads="default",
                num_storage_threads="default")
        super(CollectionBase, self).tearDown()

    def collection_setup(self):
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
            "magma_dgm.1_percent_dgm.5_node_3_replica_magma_ttl_768_single_bucket",
            "magma_dgm.1_percent_dgm.5_node_3_replica_magma_ttl_512_single_bucket"
        ]

        self.bucket_util.add_rbac_user(self.cluster.master)
        CollectionBase.deploy_buckets_from_spec_file(self)

        # Change magma quota only for bloom filter testing
        if self.change_magma_quota:
            bucket_helper = BucketHelper(self.cluster.master)
            bucket_helper.set_magma_quota_percentage()
            self.sleep(30, "Wait for magma storage setting to apply")

        validate_docs = False if self.spec_name in ttl_buckets else True

        CollectionBase.enable_dcp_oso_backfill(self)
        CollectionBase.create_clients_for_sdk_pool(self)
        CollectionBase.load_data_from_spec_file(self, self.data_spec_name,
                                                validate_docs)
        if self.range_scan_collections > 0:
            CollectionBase.range_scan_load_setup(self)

    @staticmethod
    def create_sdk_clients(cluster, num_threads, servers_to_connect,
                           buckets, sdk_compression):
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
            cluster.sdk_client_pool.create_clients(
                cluster, bucket=bucket, servers=[servers_to_connect],
                req_clients=req_clients,
                compression_settings=sdk_compression)

    @staticmethod
    def setup_collection_history_settings(test_obj):
        if test_obj.bucket_dedup_retention_bytes is None \
                and test_obj.bucket_dedup_retention_seconds is None:
            return
        update_itr = test_obj.input.param("dedupe_update_itrs", 1)
        test_obj.log.info("Loading initial dedupe data. Total itr:: {0}"
                          .format(update_itr))
        for bucket in test_obj.cluster.buckets:
            if bucket.storageBackend != Bucket.StorageBackend.magma:
                continue
            num_cols = 0
            cols = list()
            for s_name, scope in bucket.scopes.items():
                for c_name, col in scope.collections.items():
                    if c_name == CbServer.default_collection:
                        continue
                    cols.append([s_name, c_name])
                    num_cols += 1

            num_cols = int(num_cols * .9)
            cols = sample(cols, num_cols)
            test_obj.log.debug("{0} - Enabling history on collections: {1}"
                               .format(bucket.name, cols))
            for col in cols:
                test_obj.bucket_util.set_history_retention_for_collection(
                    test_obj.cluster.master, bucket, col[0], col[1], "true")

            # Setting default_collection_history_policy=true
            # This will make new collections to be created with history=on
            test_obj.bucket_util.update_bucket_property(
                test_obj.cluster.master, bucket,
                history_retention_collection_default="true")
        # Validate bucket/collection's history retention settings
        result = test_obj.bucket_util.validate_history_retention_settings(
            test_obj.cluster.master, test_obj.cluster.buckets)
        test_obj.assertTrue(result, "History setting validation failed")
        # Create history docs on disks
        CollectionBase.mutate_history_retention_data(
            test_obj, doc_key="test_collections-", update_percent=1,
            update_itrs=update_itr)
    @staticmethod
    def create_indexes_for_all_collections(test_obj, sdk_client,
                                           validate_item_count=False):
        b_util = test_obj.bucket_util
        test_obj.log.critical("Testing oso_backfill by creating indexes")
        for bucket in test_obj.cluster.buckets:
            test_obj.log.info("Creating indexes for collections in bucket {}"
                              .format(bucket.name))
            for scope in b_util.get_active_scopes(bucket):
                for col in b_util.get_active_collections(bucket, scope.name):
                    query = 'create index `{0}_{1}_{2}_index` on ' \
                            '`{0}`.`{1}`.`{2}`(`name` include missing) WITH ' \
                            '{{ "defer_build": true, "num_replica": 1 }};' \
                        .format(bucket.name, scope.name, col.name)
                    test_obj.log.debug("Creating index {}".format(query))
                    sdk_client.cluster.query(query)

            test_obj.log.info("Building deferred indexes")
            index_names = list()
            for scope in b_util.get_active_scopes(bucket):
                for col in b_util.get_active_collections(bucket, scope.name):
                    i_name = "{0}_{1}_{2}_index"\
                        .format(bucket.name, scope.name, col.name)
                    query = "BUILD INDEX on `{0}`.`{1}`.`{2}`" \
                            "(`{0}_{1}_{2}_index`) USING GSI" \
                        .format(bucket.name, scope.name, col.name)
                    try:
                        sdk_client.cluster.query(query)
                    except InternalServerFailureException as e:
                        if "Build Already In Progress" in str(e):
                            pass
                    index_names.append(i_name)

            test_obj.log.info("Waiting for index build to complete")
            for i_name in index_names:
                query = "SELECT state FROM system:indexes WHERE name='{}'" \
                    .format(i_name)
                retry = 90
                while retry > 0:
                    state = sdk_client.cluster.query(query) \
                        .rowsAsObject()[0].get("state")
                    if state == "online":
                        test_obj.log.debug("Index {} online".format(i_name))
                        break
                    test_obj.sleep(30)
                else:
                    test_obj.fail("{} - creation timed out".format(i_name))

            if validate_item_count:
                test_obj.log.info("Validating num_items using created indexes")
                for scope in b_util.get_active_scopes(bucket):
                    for col in b_util.get_active_collections(bucket,
                                                             scope.name):
                        query = "SELECT count(*) as num_items from " \
                                "`{0}`.`{1}`.`{2}`" \
                            .format(bucket.name, scope.name, col.name)
                        doc_count = sdk_client.cluster.query(query) \
                            .rowsAsObject()[0].getNumber("num_items")
                        if doc_count != col.num_items:
                            test_obj.fail(
                                "Doc count mismatch. Exp {} != {} actual"
                                .format(col.num_items, doc_count))

    @staticmethod
    def enable_dcp_oso_backfill(test_obj):
        set_oso_config_using = test_obj.input.param("set_oso_config_using",
                                                    "diag_eval")
        if test_obj.oso_dcp_backfill is not None:
            test_obj.cluster_util.update_cluster_nodes_service_list(
                test_obj.cluster)
            test_obj.log.critical("Setting oso_dcp_backfill={}"
                                  .format(test_obj.oso_dcp_backfill))
            if set_oso_config_using == "cbepctl":
                for node in test_obj.cluster.kv_nodes:
                    shell = RemoteMachineShellConnection(node)
                    cbepctl = Cbepctl(shell)
                    for bucket in test_obj.cluster.buckets:
                        test_obj.log.debug(
                            "{} - Setting oso_dcp_backfill={}"
                            .format(node.ip, test_obj.oso_dcp_backfill))
                        cbepctl.set(bucket.name, "dcp_param",
                                    "dcp_oso_backfill",
                                    test_obj.oso_dcp_backfill)
            elif set_oso_config_using == "diag_eval":
                test_obj.log.info("Setting oso_dcp_backfill={} using diag_eval"
                                  .format(test_obj.oso_dcp_backfill))
                master_shell = RemoteMachineShellConnection(
                    test_obj.cluster.master)
                master_shell.enable_diag_eval_on_non_local_hosts()
                master_shell.disconnect()
                for bucket in test_obj.cluster.buckets:
                    code = 'ns_bucket:update_bucket_props("%s", ' \
                           '[{extra_config_string, "dcp_oso_backfill=%s"}])' \
                           % (bucket.name, test_obj.oso_dcp_backfill)
                    RestConnection(test_obj.cluster.master).diag_eval(code)
                for node in test_obj.cluster.kv_nodes:
                    shell = RemoteMachineShellConnection(node)
                    shell.restart_couchbase()
                for node in test_obj.cluster.kv_nodes:
                    RestConnection(node).is_ns_server_running(300)
            else:
                test_obj.fail("Invalid value '{}'" % set_oso_config_using)

        test_obj.assertTrue(
            test_obj.bucket_util.validate_oso_dcp_backfill_value(
                test_obj.cluster.kv_nodes, test_obj.cluster.buckets,
                test_obj.oso_dcp_backfill),
            "oso_dcp_backfill value mismatch")

    @staticmethod
    def setup_indexing_for_dcp_oso_backfill(test_obj):
        if test_obj.input.param("test_oso_backfill", False):
            sdk_client = SDKClient(test_obj.cluster, None)
            CollectionBase.create_indexes_for_all_collections(
                test_obj, sdk_client, validate_item_count=True)
            sdk_client.close()

    @staticmethod
    def recreate_indexes_on_each_collection(test_obj, iterations):
        """
        This code is used by oso_dcp_backfill tests
        Refer: CBQE-7927
        """
        b_util = test_obj.bucket_util
        sdk_client = SDKClient(test_obj.cluster, None)
        for itr in range(1, iterations+1):
            test_obj.log.info("Itr::{}. Dropping existing indexes")
            for bucket in test_obj.cluster.buckets:
                for scope in b_util.get_active_scopes(bucket):
                    for col in b_util.get_active_collections(bucket,
                                                             scope.name):
                        query = "DROP INDEX `{0}_{1}_{2}_index` " \
                                "on `{0}`.`{1}`.`{2}` USING GSI" \
                            .format(bucket.name, scope.name, col.name)
                        try:
                            sdk_client.cluster.query(query)
                        except (IndexFailureException,
                                IndexNotFoundException) as e:
                            test_obj.log.warning(e)
            CollectionBase.create_indexes_for_all_collections(test_obj,
                                                              sdk_client)
        sdk_client.close()

    @staticmethod
    def range_scan_load_setup(test_obj):
        key_size = 8
        rc_conn_list = []
        include_prefix_scan = True
        include_range_scan = True
        timeout = 60
        expect_exceptions = []
        range_scan_runs_per_collection = 1
        if hasattr(test_obj, 'include_prefix_scan'):
            include_prefix_scan = test_obj.include_prefix_scan
        if hasattr(test_obj, 'include_range_scan'):
            include_range_scan = test_obj.include_range_scan
        if hasattr(test_obj, 'range_scan_timeout') and \
                test_obj.range_scan_timeout is not None:
            timeout = test_obj.range_scan_timeout
        if hasattr(test_obj, 'expect_range_scan_exceptions') and \
                test_obj.expect_range_scan_exceptions is not None:
            expect_exceptions = test_obj.expect_range_scan_exceptions
        if hasattr(test_obj, 'range_scan_runs_per_collection') and \
                test_obj.range_scan_runs_per_collection is not None:
            range_scan_runs_per_collection = test_obj.range_scan_runs_per_collection
        if hasattr(test_obj, 'expect_range_scan_failure') and \
                test_obj.expect_range_scan_failure is not None:
            expect_range_scan_failure = test_obj.range_scan_runs_per_collection
        if test_obj.key_size is not None:
            key_size = test_obj.key_size

        # selecting random collection to be use for parallel range scan
        # avoiding selection of system once for the above
        if test_obj.range_scan_collections > 0:
            buckets_to_consider = []
            dict_to_skip = dict()
            for bucket in test_obj.cluster.buckets:
                dict_to_skip = {
                    bucket.name: {
                        "scopes": {
                            CbServer.system_scope: {
                                "collections": {
                                    CbServer.eventing_collection: {},
                                    CbServer.query_collection: {},
                                    CbServer.mobile_collection: {},
                                    CbServer.regulator_collection: {}
                                }
                            }
                        }
                    }
                }
                if bucket.bucketType != Bucket.Type.EPHEMERAL:
                    buckets_to_consider.append(bucket)
            collections_selected_for_range_scan = \
                test_obj.bucket_util.get_random_collections(
                    buckets_to_consider, test_obj.range_scan_collections, 1000,
                    1000, exclude_from=dict_to_skip)
            collections_created = list()

            # if skip_range_scan_collection_mutation is True in the test
            # subsequent data load will skip range scan collection
            # test must also reuse self.skip_collections_during_data_load for
            # any subsequent data load
            test_obj.skip_collections_during_data_load = {}
            for bucket in collections_selected_for_range_scan:
                for scope in collections_selected_for_range_scan[bucket]['scopes']:
                    for collection in collections_selected_for_range_scan[
                        bucket]['scopes'][scope]['collections']:
                        for t_bucket in test_obj.cluster.buckets:
                            if t_bucket.name == bucket:
                                rc_conn_list.append(SDKClient(
                                    test_obj.cluster,
                                    t_bucket, scope=scope, collection=collection))
                                collections_created.append(collection)
            if test_obj.skip_range_scan_collection_mutation:
                test_obj.skip_collections_during_data_load = collections_selected_for_range_scan
            doc_loading_spec = \
                test_obj.bucket_util.get_bucket_template_from_package(
                    test_obj.spec_name)
            items_per_collection = doc_loading_spec[
                MetaConstants.NUM_ITEMS_PER_COLLECTION]
            # starting parallel range scans task
            # teardown is handled in teardown method of this class
            test_obj.range_scan_task = test_obj.task.async_range_scan(
                test_obj.cluster, test_obj.task.jython_task_manager,
                rc_conn_list,
                collections_created, items_per_collection,
                key_size=key_size, include_range_scan=include_range_scan,
                include_prefix_scan=include_prefix_scan, timeout=timeout,
                expect_exceptions=expect_exceptions,
                runs_per_collection=range_scan_runs_per_collection)

    @staticmethod
    def get_history_retention_load_spec(test_obj, doc_key="test_collections-",
                                        update_percent=1, update_itrs=1000,
                                        doc_ttl=0):
        return {
            "doc_crud": {
                MetaCrudParams.DocCrud.DOC_SIZE: test_obj.doc_size,
                MetaCrudParams.DocCrud.RANDOMIZE_VALUE: True,
                MetaCrudParams.DocCrud.COMMON_DOC_KEY: doc_key,

                MetaCrudParams.DocCrud.CONT_UPDATE_PERCENT_PER_COLLECTION:
                    (update_percent, update_itrs),
            },
            MetaCrudParams.BUCKETS_CONSIDERED_FOR_CRUD: "all",
            MetaCrudParams.SCOPES_CONSIDERED_FOR_CRUD: 5,
            MetaCrudParams.COLLECTIONS_CONSIDERED_FOR_CRUD: 5,
            MetaCrudParams.DOC_TTL: doc_ttl,
            MetaCrudParams.SKIP_READ_ON_ERROR: True,
            MetaCrudParams.SUPPRESS_ERROR_TABLE: True,
        }

    @staticmethod
    def start_history_retention_data_load(test_obj, async_load=True):
        cont_doc_load = None
        if test_obj.bucket_dedup_retention_seconds is not None \
                or test_obj.bucket_dedup_retention_bytes is not None:
            update_percent = 2
            update_itr = test_obj.input.param("dedupe_update_itrs", 3)
            if async_load:
                update_itr = -1
            test_obj.log.info("Starting dedupe updates load ({0}, {1})"
                              .format(update_percent, update_itr))
            load_spec = CollectionBase.get_history_retention_load_spec(
                test_obj, doc_key="hist_retention_docs-",
                update_percent=update_percent, update_itrs=update_itr)
            CollectionBase.over_ride_doc_loading_template_params(test_obj,
                                                                 load_spec)
            CollectionBase.set_retry_exceptions(load_spec,
                                                test_obj.durability_level)

            cont_doc_load = test_obj.bucket_util.run_scenario_from_spec(
                test_obj.task, test_obj.cluster, test_obj.cluster.buckets,
                load_spec, mutation_num=0, async_load=async_load,
                print_ops_rate=False, batch_size=500, process_concurrency=1,
                validate_task=(not test_obj.skip_validations))
        return cont_doc_load

    @staticmethod
    def wait_for_cont_doc_load_to_complete(test_obj, load_task):
        if load_task is not None:
            load_task.stop_indefinite_doc_loading_tasks()
            test_obj.task_manager.get_task_result(load_task)
            CollectionBase.remove_docs_created_for_dedupe_load(
                test_obj, load_task)

    @staticmethod
    def remove_docs_created_for_dedupe_load(test_obj, spec_load_task):
        # Remove all docs created by dedupe data loader
        for bucket, scope_dict in spec_load_task.loader_spec.items():
            for s_name, collection_dict in scope_dict["scopes"].items():
                for c_name, crud_spec in collection_dict["collections"].items():
                    for crud_name, crud_info in crud_spec.items():
                        crud_spec[DocLoading.Bucket.DocOps.DELETE] = crud_info
                        crud_info["iterations"] = 1
                        crud_spec.pop(crud_name)
        task = test_obj.task.async_load_gen_docs_from_spec(
            test_obj.cluster, test_obj.task_manager,
            spec_load_task.loader_spec,
            batch_size=1000, process_concurrency=1, track_failures=False,
            print_ops_rate=False, start_task=True)
        test_obj.task_manager.get_task_result(task)
        test_obj.bucket_util._wait_for_stats_all_buckets(
            test_obj.cluster, test_obj.cluster.buckets)

    @staticmethod
    def mutate_history_retention_data(test_obj, doc_key="test_collections-",
                                      update_percent=1, update_itrs=1000,
                                      doc_ttl=0):
        test_obj.log.info("Loading docs for history_retention testing")
        load_spec = CollectionBase.get_history_retention_load_spec(
            test_obj, doc_key=doc_key, update_percent=update_percent,
            update_itrs=update_itrs, doc_ttl=doc_ttl)
        CollectionBase.over_ride_doc_loading_template_params(test_obj, load_spec)

        cont_doc_load = test_obj.bucket_util.run_scenario_from_spec(
            test_obj.task, test_obj.cluster, test_obj.cluster.buckets,
            load_spec, mutation_num=0, async_load=False,
            batch_size=500, process_concurrency=1, validate_task=True)
        test_obj.assertTrue(cont_doc_load.result, "Hist retention load failed")

    @staticmethod
    def deploy_buckets_from_spec_file(test_obj):
        # If True, creates bucket/scope/collections with simpler names
        use_simple_names = test_obj.input.param("use_simple_names", True)

        # enable encryption at rest
        if test_obj.enable_encryption_at_rest:
            params = BucketHelper(test_obj.cluster.master).create_secret_params(
                usage=["bucket-encryption-*"],
                rotationIntervalInSeconds=test_obj.rotationIntervalInSeconds,
            )
            status, response = RestConnection(
                test_obj.cluster.master).create_secret(params)
            test_obj.assertTrue(status, "failed to create encryption at rest secret key")
            response_dict = json.loads(response)
            test_obj.secret_id = response_dict.get('id')

        # Create bucket(s) and add rbac user
        buckets_spec = \
            test_obj.bucket_util.get_bucket_template_from_package(
                test_obj.spec_name)
        buckets_spec[MetaConstants.USE_SIMPLE_NAMES] = use_simple_names

        if hasattr(test_obj, "fragmentation") and test_obj.fragmentation is not None:
            buckets_spec[Bucket.autoCompactionDefined] = "true"
            buckets_spec[Bucket.fragmentationPercentage] = int(test_obj.fragmentation)

        test_obj.log.info("Creating bucket from spec: %s" % test_obj.spec_name)
        # Process params to over_ride values if required
        CollectionBase.over_ride_bucket_template_params(
            test_obj, test_obj.bucket_storage, buckets_spec)
        if test_obj.enable_encryption_at_rest:
            buckets_spec[Bucket.encryptionAtRestSecretId] = test_obj.secret_id
            buckets_spec[Bucket.encryptionAtRestDekRotationInterval] = \
                test_obj.encryptionAtRestDekRotationInterval
            test_obj.log.info("Encryption at rest enabled")
        test_obj.bucket_util.create_buckets_using_json_data(
            test_obj.cluster, buckets_spec)

        if hasattr(test_obj, "upgrade_chain") and \
                    float(test_obj.upgrade_chain[0][:3]) < 7.6:
            for bucket in test_obj.cluster.buckets:
                for coll in bucket.scopes[CbServer.system_scope].collections:
                    bucket.scopes[CbServer.system_scope].collections.pop(coll)
                bucket.scopes.pop(CbServer.system_scope)

        if not (hasattr(test_obj, "upgrade_chain")
                and int(test_obj.upgrade_chain[0][0]) < 7):
            test_obj.bucket_util.wait_for_collection_creation_to_complete(
                test_obj.cluster)

        # Prints bucket stats before doc_ops
        test_obj.cluster_util.print_cluster_stats(test_obj.cluster)
        test_obj.bucket_util.print_bucket_stats(test_obj.cluster)

    @staticmethod
    def create_clients_for_sdk_pool(test_obj, cluster=None):
        if not cluster:
            cluster = test_obj.cluster

        # Init sdk_client_pool if not initialized before
        if cluster.sdk_client_pool is None:
            cluster.sdk_client_pool = SDKClientPool()

        test_obj.log.info("Creating required SDK clients for client_pool")
        CollectionBase.create_sdk_clients(
            cluster,
            test_obj.task_manager.number_of_threads,
            cluster.master,
            cluster.buckets,
            test_obj.sdk_compression)

    @staticmethod
    def load_data_from_spec_file(test_obj, data_spec_name, validate_docs=True):
        test_obj.log.info("Load docs using spec file %s" % data_spec_name)
        doc_loading_spec = \
            test_obj.bucket_util.get_crud_template_from_package(data_spec_name)
        # Process params to over_ride values if required
        CollectionBase.over_ride_doc_loading_template_params(
            test_obj, doc_loading_spec)
        CollectionBase.set_retry_exceptions(
            doc_loading_spec, test_obj.durability_level)
        doc_loading_task = \
            test_obj.bucket_util.run_scenario_from_spec(
                test_obj.task,
                test_obj.cluster,
                test_obj.cluster.buckets,
                doc_loading_spec,
                mutation_num=0,
                batch_size=test_obj.batch_size,
                process_concurrency=test_obj.process_concurrency)
        if doc_loading_task.result is False:
            test_obj.fail("Initial doc_loading failed")

        test_obj.cluster_util.print_cluster_stats(test_obj.cluster)

        # Code to handle collection specific validation during upgrade test
        collection_supported = True
        if hasattr(test_obj, "upgrade_chain") \
                and int(test_obj.upgrade_chain[0][0]) < 7:
            collection_supported = False

        # Verify initial doc load count
        test_obj.log.info("Wait for ep_queue_size to drain")
        test_obj.bucket_util._wait_for_stats_all_buckets(
            test_obj.cluster, test_obj.cluster.buckets, timeout=1200)
        if validate_docs:
            if collection_supported:
                test_obj.bucket_util.validate_docs_per_collections_all_buckets(
                    test_obj.cluster, timeout=2400)
            else:
                test_obj.bucket_util.verify_stats_all_buckets(test_obj.cluster, test_obj.num_items)

        # Prints bucket stats after doc_ops
        test_obj.cluster_util.print_cluster_stats(test_obj.cluster)
        test_obj.bucket_util.print_bucket_stats(test_obj.cluster)

    @staticmethod
    def over_ride_bucket_template_params(test_obj, bucket_storage,
                                         bucket_spec):
        # Set default values for serverless deployment
        if CbServer.cluster_profile == "serverless":
            bucket_spec[Bucket.ramQuotaMB] = 256
            bucket_spec[Bucket.replicaNumber] = Bucket.ReplicaNum.TWO
            bucket_spec[Bucket.evictionPolicy] = \
                Bucket.EvictionPolicy.FULL_EVICTION

        for key, val in test_obj.input.test_params.items():
            if key == "replicas":
                bucket_spec[Bucket.replicaNumber] = test_obj.num_replicas
            elif key == "bucket_size":
                bucket_spec[Bucket.ramQuotaMB] = test_obj.bucket_size
            elif key == "num_items":
                bucket_spec[MetaConstants.NUM_ITEMS_PER_COLLECTION] = \
                    test_obj.num_items
            elif key == "remove_default_collection":
                bucket_spec[MetaConstants.REMOVE_DEFAULT_COLLECTION] = \
                    test_obj.input.param(key)
            elif key == "bucket_storage":
                bucket_spec[Bucket.storageBackend] = \
                    test_obj.bucket_storage
            elif key == "bucket_eviction_policy":
                bucket_spec[Bucket.evictionPolicy] \
                    = test_obj.bucket_eviction_policy
            elif key == "compression_mode":
                bucket_spec[Bucket.compressionMode] = \
                    test_obj.compression_mode
            elif key == "flushEnabled":
                bucket_spec[Bucket.flushEnabled] = \
                    int(test_obj.flush_enabled)
            elif key == "bucket_type":
                bucket_spec[Bucket.bucketType] = test_obj.bucket_type
            elif key == "default_history_retention_for_collections":
                bucket_spec[Bucket.historyRetentionCollectionDefault] \
                    = str(test_obj.bucket_collection_history_retention_default).lower()
            elif key == "bucket_history_retention_seconds":
                bucket_spec[Bucket.historyRetentionSeconds] \
                    = int(test_obj.bucket_dedup_retention_seconds)
            elif key == "bucket_history_retention_bytes":
                bucket_spec[Bucket.historyRetentionBytes] \
                    = int(test_obj.bucket_dedup_retention_bytes)
            elif key == "magma_key_tree_data_block_size":
                bucket_spec[Bucket.magmaKeyTreeDataBlockSize] \
                    = int(test_obj.magma_key_tree_data_block_size)
            elif key == "magma_seq_tree_data_block_size":
                bucket_spec[Bucket.magmaSeqTreeDataBlockSize] \
                    = int(test_obj.magma_seq_tree_data_block_size)
            elif key == "load_collections_exponentially":
                bucket_spec[MetaConstants.LOAD_COLLECTIONS_EXPONENTIALLY] \
                    = test_obj.load_collections_exponentially

    @staticmethod
    def over_ride_doc_loading_template_params(test_obj, target_spec):
        for key, value in test_obj.input.test_params.items():
            if key == "durability":
                target_spec[MetaCrudParams.DURABILITY_LEVEL] = \
                    test_obj.durability_level
            elif key == "sdk_timeout":
                target_spec[MetaCrudParams.SDK_TIMEOUT] = test_obj.sdk_timeout
            elif key == "doc_size":
                target_spec["doc_crud"][MetaCrudParams.DocCrud.DOC_SIZE] \
                    = test_obj.doc_size
            elif key == "randomize_value":
                target_spec["doc_crud"][
                    MetaCrudParams.DocCrud.RANDOMIZE_VALUE] \
                    = test_obj.randomize_value
        if test_obj.key_size is not None:
            target_spec["doc_crud"][MetaCrudParams.DocCrud.DOC_KEY_SIZE] \
                = test_obj.key_size

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
            client = self.cluster.sdk_client_pool.get_client_for_bucket(bucket)
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
                            is_sub_doc=is_sub_doc)
                        self.task_manager.get_task_result(task)

    @staticmethod
    def set_retry_exceptions(doc_loading_spec, d_level):
        retry_exceptions = list()
        retry_exceptions.append(SDKException.AmbiguousTimeoutException)
        retry_exceptions.append(SDKException.TimeoutException)
        retry_exceptions.append(SDKException.RequestCanceledException)
        retry_exceptions.append(SDKException.DocumentNotFoundException)
        retry_exceptions.append(SDKException.ServerOutOfMemoryException)
        if d_level:
            retry_exceptions.append(SDKException.DurabilityAmbiguousException)
            retry_exceptions.append(SDKException.DurabilityImpossibleException)
        doc_loading_spec[MetaCrudParams.RETRY_EXCEPTIONS] = retry_exceptions

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
