from random import choice
from threading import Thread

from BucketLib.bucket import Bucket
from Cb_constants import DocLoading, CbServer
from bucket_utils.bucket_ready_functions import DocLoaderUtils
from collections_helper.collections_spec_constants import MetaCrudParams
from couchbase_helper.documentgenerator import doc_generator
from couchbase_helper.durability_helper import DurabilityHelper, \
    BucketDurability
from membase.api.rest_client import RestConnection
from sdk_client3 import SDKClient, SDKClientPool
from sdk_exceptions import SDKException
from StatsLib.StatsOperations import StatsHelper
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

    def tearDown(self):
        super(UpgradeTests, self).tearDown()

    def __wait_for_persistence_and_validate(self):
        self.bucket_util._wait_for_stats_all_buckets(
            cbstat_cmd="checkpoint", stat_name="num_items_for_persistence",
            timeout=300)
        self.bucket_util._wait_for_stats_all_buckets(
            cbstat_cmd="all", stat_name="ep_queue_size",
            timeout=60)
        self.bucket_util.validate_docs_per_collections_all_buckets()

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
            int(self.thread_to_use / len(self.bucket_util.buckets))
        for bucket in self.bucket_util.buckets:
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
                                                    self.bucket_util.buckets,
                                                    collection_load_spec,
                                                    mutation_num=1,
                                                    batch_size=500)
        if collection_task.result is False:
            self.log_failure("Collection task failed")
            return

        # Perform collection/doc_count validation
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
                                                    self.bucket_util.buckets,
                                                    collection_load_spec,
                                                    mutation_num=1,
                                                    batch_size=500)
        if collection_task.result is False:
            self.log_failure("Drop scope/collection failed")
            return

        # Perform collection/doc_count validation
        if not self.upgrade_with_data_load:
            self.__wait_for_persistence_and_validate()

        # MB-44092 - Close client_pool after collection ops
        DocLoaderUtils.sdk_client_pool.shutdown()

    def test_upgrade(self):
        create_batch_size = 10000
        update_task = None

        t_durability_level = ""
        if self.cluster_supports_sync_write:
            t_durability_level = Bucket.DurabilityLevel.MAJORITY

        if self.upgrade_with_data_load:
            self.log.info("Starting async doc updates")
            update_task = self.task.async_continuous_doc_ops(
                self.cluster, self.bucket, self.gen_load,
                op_type=DocLoading.Bucket.DocOps.UPDATE,
                process_concurrency=1,
                persist_to=1,
                replicate_to=1,
                durability=t_durability_level,
                timeout_secs=30)

        create_gen = doc_generator(self.key, self.num_items,
                                   self.num_items+create_batch_size)
        self.log.info("Upgrading cluster nodes to target version")
        node_to_upgrade = self.fetch_node_to_upgrade()
        while node_to_upgrade is not None:
            self.log.info("Selected node for upgrade: %s"
                          % node_to_upgrade.ip)
            self.upgrade_function[self.upgrade_type](node_to_upgrade,
                                                     self.upgrade_version)
            self.cluster_util.print_cluster_stats()

            # Validate sync_write results after upgrade
            if self.atomicity:
                create_batch_size = 10
                create_gen = doc_generator(
                    self.key,
                    self.num_items,
                    self.num_items+create_batch_size)
                sync_write_task = self.task.async_load_gen_docs_atomicity(
                    self.cluster, self.bucket_util.buckets,
                    create_gen, DocLoading.Bucket.DocOps.CREATE,
                    process_concurrency=1,
                    transaction_timeout=self.transaction_timeout,
                    record_fail=True)
            else:
                sync_write_task = self.task.async_load_gen_docs(
                    self.cluster, self.bucket, create_gen,
                    DocLoading.Bucket.DocOps.CREATE,
                    durability=self.durability_level,
                    timeout_secs=self.sdk_timeout,
                    sdk_client_pool=self.sdk_client_pool,
                    process_concurrency=4,
                    skip_read_on_error=True,
                    suppress_error_table=True)
            self.task_manager.get_task_result(sync_write_task)

            node_to_upgrade = self.fetch_node_to_upgrade()
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

        # Validate default num_items before collection tests
        self.bucket_util.validate_docs_per_collections_all_buckets()

        # Play with collection if upgrade was successful
        if not self.test_failure:
            self.__play_with_collection()

        if self.upgrade_with_data_load:
            # Wait for update_task to complete
            update_task.end_task()
            self.task_manager.get_task_result(update_task)

        # Perform final collection/doc_count validation
        self.__wait_for_persistence_and_validate()
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
                    self.cluster, self.bucket_util.buckets,
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
            self.cluster_util.print_cluster_stats()

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
            self.bucket_util.buckets[0],
            self.cluster_util.get_kv_nodes(),
            vbuckets=self.cluster_util.vbuckets,
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
        client = SDKClient([self.cluster.master], self.bucket_util.buckets[0])
        for index, d_level in enumerate(possible_d_levels[self.bucket_type]):
            self.log.info("Updating bucket_durability=%s" % d_level)
            self.bucket_util.update_bucket_property(
                self.bucket_util.buckets[0],
                bucket_durability=BucketDurability[d_level])
            self.bucket_util.print_bucket_stats()

            buckets = self.bucket_util.get_all_buckets()
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
            self.bucket_util.buckets[0],
            self.cluster_util.get_kv_nodes(),
            vbuckets=self.cluster_util.vbuckets,
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
                    self.cluster, self.bucket_util.buckets, self.gen_load,
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

            self.cluster_util.print_cluster_stats()
            self.bucket_util.print_bucket_stats()

            self.summary.add_step("Upgrade %s" % node_to_upgrade.ip)

            # Halt further upgrade if test has failed during current upgrade
            if self.test_failure:
                break

            node_to_upgrade = self.fetch_node_to_upgrade()
            for bucket in self.bucket_util.get_all_buckets():
                tombstone_doc_supported = \
                    "tombstonedUserXAttrs" in bucket.bucketCapabilities
                if node_to_upgrade is None and not tombstone_doc_supported:
                    self.log_failure("Tombstone docs not added to %s "
                                     "capabilities" % bucket.name)
                elif node_to_upgrade is not None and tombstone_doc_supported:
                    self.log_failure("Tombstone docs supported for %s before "
                                     "cluster upgrade" % bucket.name)

        self.validate_test_failure()

        create_gen = doc_generator(self.key, self.num_items,
                                   self.num_items*2)
        # Start transaction load after node upgrade
        trans_task = self.task.async_load_gen_docs_atomicity(
            self.cluster, self.bucket_util.buckets,
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
            self.cluster_util.print_cluster_stats()

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
        for server in self.cluster_util.get_kv_nodes():
            content = StatsHelper(server).get_prometheus_metrics(parse=parse)
            if not parse:
                StatsHelper(server)._validate_metrics(content)
        for line in content:
            self.log.info(line.strip("\n"))

    def get_high_cardinality_metrics(self,parse):
        content = None
        try:
            for server in self.cluster_util.get_kv_nodes():
                content = StatsHelper(server).get_prometheus_metrics_high(
                    parse=parse)
                if not parse:
                    StatsHelper(server)._validate_metrics(content)
            for line in content:
                self.log.info(line.strip("\n"))
        except:
            pass

    def get_range_api_metrics(self, metric_name):
        label_values = {"bucket": self.bucket_util.buckets[0].name,
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
