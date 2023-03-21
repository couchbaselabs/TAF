'''
Created on 04-Mar-2021

@author: Umang

Very important note -
Number of datasets in <= 6.5.0 should not be more than 8.
'''

from math import ceil
from Cb_constants import DocLoading, CbServer
from collections_helper.collections_spec_constants import MetaConstants, \
    MetaCrudParams
from couchbase_helper.documentgenerator import doc_generator
from sdk_exceptions import SDKException
from upgrade.upgrade_base import UpgradeBase
from cbas_utils.cbas_utils import CbasUtil, CBASRebalanceUtil
from membase.api.rest_client import RestConnection
from BucketLib.BucketOperations import BucketHelper
from security_utils.security_utils import SecurityUtils
from security_config import trust_all_certs
import random


class UpgradeTests(UpgradeBase):

    def setUp(self):
        super(UpgradeTests, self).setUp()
        self.cbas_util = CbasUtil(self.task)
        self.cbas_spec_name = self.input.param("cbas_spec", "local_datasets")
        self.rebalance_util = CBASRebalanceUtil(
            self.cluster_util, self.bucket_util, self.task,
            vbucket_check=True, cbas_util=self.cbas_util)

        if self.input.param("n2n_encryption", False):
            CbServer.use_https = True
            trust_all_certs()

            self.security_util = SecurityUtils(self.log)

            rest = RestConnection(self.cluster.master)
            self.log.info("Disabling Auto-Failover")
            if not rest.update_autofailover_settings(False, 120):
                self.fail("Disabling Auto-Failover failed")

            self.log.info("Setting node to node encryption level to all")
            self.security_util.set_n2n_encryption_level_on_nodes(
                self.cluster.nodes_in_cluster, level="all")

            CbServer.use_https = True
            self.log.info("Enabling Auto-Failover")
            if not rest.update_autofailover_settings(True, 300):
                self.fail("Enabling Auto-Failover failed")

        cbas_cc_node_ip = None
        retry = 0
        self.cluster.cbas_nodes = \
            self.cluster_util.get_nodes_from_services_map(
                self.cluster, service_type="cbas", get_all_nodes=True,
                servers=self.cluster.nodes_in_cluster)
        while True and retry < 60:
            cbas_cc_node_ip = self.cbas_util.retrieve_cc_ip_from_master(
                self.cluster)
            if cbas_cc_node_ip:
                break
            else:
                self.sleep(10, "Waiting for CBAS service to come up")
                retry += 1

        if not cbas_cc_node_ip:
            self.fail("CBAS service did not come up even after 10 "
                      "mins.")

        for server in self.cluster.cbas_nodes:
            if server.ip == cbas_cc_node_ip:
                self.cluster.cbas_cc_node = server
                break

        if not self.cbas_util.wait_for_cbas_to_recover(
                self.cluster, timeout=300):
            self.fail("Analytics service failed to start post adding cbas "
                      "nodes to cluster")

        self.pre_upgrade_setup()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        self.cluster.master = self.cluster_util.get_kv_nodes(self.cluster)[0]
        self.cluster_util.cluster_cleanup(self.cluster, self.bucket_util)
        super(UpgradeTests, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.tearDown.__name__)

    def pre_upgrade_setup(self):
        """
        Number of datasets is fixed here, as pre 6.6 default max number of
        datasets that can be created was 8.
        """
        major_version = float(self.initial_version[:3])
        if major_version >= 7.0:
            update_spec = {
                "no_of_dataverses": self.input.param('pre_update_no_of_dv', 2),
                "no_of_datasets_per_dataverse": self.input.param('pre_update_ds_per_dv', 4),
                "no_of_synonyms": self.input.param('pre_update_no_of_synonym', 0),
                "no_of_indexes": self.input.param('pre_update_no_of_index', 3),
                "max_thread_count": self.input.param('no_of_threads', 10),
            }
        else:
            update_spec = {
                "no_of_dataverses": self.input.param('pre_update_no_of_dv', 2),
                "no_of_datasets_per_dataverse": self.input.param('pre_update_ds_per_dv', 4),
                "no_of_synonyms": 0,
                "no_of_indexes": self.input.param('pre_update_no_of_index', 3),
                "max_thread_count": self.input.param('no_of_threads', 10),
                "dataverse": {
                    "cardinality": 1,
                    "creation_method": "dataverse"
                },
                "dataset": {
                    "creation_methods": ["cbas_dataset"],
                    "bucket_cardinality": 1
                },
                "index": {
                    "creation_method": "index"
                }
            }
            if update_spec["no_of_dataverses"] * update_spec[
                "no_of_datasets_per_dataverse"] > 8:
                self.fail("Total number of datasets across all dataverses "
                          "cannot be more than 8 for pre 7.0 builds")
        if not self.cbas_setup(update_spec):
            self.fail("Pre Upgrade CBAS setup failed")

        if major_version >= 7.1:
            self.replica_num = self.input.param('replica_num', 0)
            set_result = self.cbas_util.set_replica_number_from_settings(
                self.cluster.master, replica_num=self.replica_num)
            if set_result != self.replica_num:
                self.fail("Error while setting replica for CBAS")

            self.log.info(
                "Rebalancing for CBAS replica setting change to take "
                "effect.")
            rebalance_task, _ = self.rebalance_util.rebalance(
                self.cluster, kv_nodes_in=0, kv_nodes_out=0, cbas_nodes_in=0,
                cbas_nodes_out=0, available_servers=[], exclude_nodes=[])
            if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                    rebalance_task, self.cluster):
                self.fail("Rebalance failed")

    def cbas_setup(self, update_spec, connect_local_link=True):
        if self.cbas_spec_name:
            self.cbas_spec = self.cbas_util.get_cbas_spec(
                self.cbas_spec_name)
            self.cbas_util.update_cbas_spec(self.cbas_spec, update_spec)
            cbas_infra_result = self.cbas_util.create_cbas_infra_from_spec(
                self.cluster, self.cbas_spec, self.bucket_util,
                wait_for_ingestion=False)
            if not cbas_infra_result[0]:
                self.log.error(
                    "Error while creating infra from CBAS spec -- {0}".format(
                        cbas_infra_result[1]))
                return False

        if connect_local_link:
            for dataverse in self.cbas_util.dataverses:
                if not self.cbas_util.connect_link(
                        self.cluster, ".".join([dataverse, "Local"])):
                    self.log.error(
                        "Failed to connect Local link for dataverse - {0}".format(
                            dataverse))
                    return False
        if not self.cbas_util.wait_for_ingestion_all_datasets(
                self.cluster, self.bucket_util):
            self.log.error("Data ingestion did not happen in the datasets")
            return False
        return True

    def post_upgrade_validation(self):
        major_version = float(self.upgrade_version[:3])
        # rebalance once again to activate CBAS service
        self.sleep(180, "Sleep before rebalancing to activate CBAS service")
        rebalance_task, _ = self.rebalance_util.rebalance(
            self.cluster, kv_nodes_in=0, kv_nodes_out=0, cbas_nodes_in=0,
            cbas_nodes_out=0, available_servers=[], exclude_nodes=[])
        if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                rebalance_task, self.cluster):
            self.log_failure("Rebalance failed")
            return False

        rest = RestConnection(self.cluster.master)
        # Update RAM quota allocated to buckets created before upgrade
        cluster_info = rest.get_nodes_self()
        kv_quota = \
            cluster_info.__getattribute__(CbServer.Settings.KV_MEM_QUOTA)
        bucket_size = kv_quota // (self.input.param("num_buckets", 1) + 1)
        for bucket in self.cluster.buckets:
            self.bucket_util.update_bucket_property(
                self.cluster.master, bucket, bucket_size)

        validation_results = {}
        self.log.info("Validating pre upgrade cbas infra")
        results = list()
        for dataverse in self.cbas_util.dataverses:
            results.append(
                self.cbas_util.validate_dataverse_in_metadata(
                    self.cluster, dataverse))
        for dataset in self.cbas_util.list_all_dataset_objs(
                dataset_source="internal"):
            results.append(
                self.cbas_util.validate_dataset_in_metadata(
                    self.cluster, dataset_name=dataset.name,
                    dataverse_name=dataset.dataverse_name))
            results.append(
                self.cbas_util.validate_cbas_dataset_items_count(
                    self.cluster, dataset_name=dataset.full_name,
                    expected_count=dataset.num_of_items))
        for index in self.cbas_util.list_all_index_objs():
            result, _ = self.cbas_util.verify_index_created(
                self.cluster, index_name=index.name,
                dataset_name=index.dataset_name,
                indexed_fields=index.indexed_fields)
            results.append(result)
            results.append(
                self.cbas_util.verify_index_used(
                    self.cluster,
                    statement="SELECT VALUE v FROM {0} v WHERE age > 2".format(
                        index.full_dataset_name),
                    index_used=True, index_name=None))
        validation_results["pre_upgrade"] = all(results)

        if major_version >= 7.1:
            self.log.info("Enabling replica for analytics")
            self.replica_num = self.input.param('replica_num', 0)
            set_result = self.cbas_util.set_replica_number_from_settings(
                self.cluster.master, replica_num=self.replica_num)
            if set_result != self.replica_num:
                self.fail("Error while setting replica for CBAS")

            self.log.info(
                "Rebalancing for CBAS replica setting change to take "
                "effect.")
            rebalance_task, _ = self.rebalance_util.rebalance(
                self.cluster, kv_nodes_in=0, kv_nodes_out=0, cbas_nodes_in=0,
                cbas_nodes_out=0, available_servers=[], exclude_nodes=[])
            if not self.rebalance_util.wait_for_rebalance_task_to_complete(
                    rebalance_task, self.cluster):
                self.fail("Rebalance failed")

            if not self.cbas_util.wait_for_replication_to_finish(self.cluster):
                self.fail("Replication could not complete before timeout")

            if not self.cbas_util.verify_actual_number_of_replicas(
                    self.cluster, len(self.cluster.cbas_nodes) - 1):
                self.fail("Actual number of replicas is different from what "
                          "was set")

        self.log.info("Loading docs in default collection of existing buckets")
        for bucket in self.cluster.buckets:
            gen_load = doc_generator(
                self.key, self.num_items, self.num_items*2,
                randomize_doc_size=True, randomize_value=True, randomize=True)
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
                CbServer.default_collection].num_items = self.num_items * 2

            # Verify doc load count
            self.bucket_util._wait_for_stats_all_buckets(
                self.cluster, self.cluster.buckets)
            self.sleep(30, "Wait for num_items to get reflected")
            current_items = self.bucket_util.get_bucket_current_item_count(
                self.cluster, bucket)
            if current_items == self.num_items * 2:
                validation_results["post_upgrade_data_load"] = True
            else:
                self.log.error(
                    "Mismatch in doc_count. Actual: %s, Expected: %s"
                    % (current_items, self.num_items*2))
                validation_results["post_upgrade_data_load"] = False
        self.bucket_util.print_bucket_stats(self.cluster)
        if not self.cbas_util.wait_for_ingestion_all_datasets(
                self.cluster, self.bucket_util):
            validation_results["post_upgrade_data_load"] = False
            self.log.error("Data ingestion did not happen in the datasets")
        else:
            validation_results["post_upgrade_data_load"] = True

        self.log.info("Deleting all the data from default collection of buckets created before upgrade")
        for bucket in self.cluster.buckets:
            gen_load = doc_generator(
                self.key, 0, self.num_items*2,
                randomize_doc_size=True, randomize_value=True, randomize=True)
            async_load_task = self.task.async_load_gen_docs(
                self.cluster, bucket, gen_load,
                DocLoading.Bucket.DocOps.DELETE,
                active_resident_threshold=self.active_resident_threshold,
                timeout_secs=self.sdk_timeout,
                process_concurrency=8,
                batch_size=500,
                sdk_client_pool=self.sdk_client_pool)
            self.task_manager.get_task_result(async_load_task)

            # Verify doc load count
            self.bucket_util._wait_for_stats_all_buckets(
                self.cluster, self.cluster.buckets)
            while True:
                current_items = self.bucket_util.get_bucket_current_item_count(
                    self.cluster, bucket)
                if current_items == 0:
                    break
                else:
                    self.sleep(30, "Wait for num_items to get reflected")

            bucket.scopes[CbServer.default_scope].collections[
                    CbServer.default_collection].num_items = 0

        if major_version >= 7.0:
            self.log.info("Creating scopes and collections in existing bucket")
            scope_spec = {"name": self.cbas_util.generate_name()}
            self.bucket_util.create_scope_object(
                self.cluster.buckets[0], scope_spec)
            collection_spec = {"name": self.cbas_util.generate_name(),
                               "num_items": self.num_items}
            self.bucket_util.create_collection_object(
                self.cluster.buckets[0], scope_spec["name"], collection_spec)

            bucket_helper = BucketHelper(self.cluster.master)
            status, content = bucket_helper.create_scope(
                self.cluster.buckets[0].name, scope_spec["name"])
            if status is False:
                self.fail(
                    "Create scope failed for %s:%s, Reason - %s" % (
                        self.cluster.buckets[0].name, scope_spec["name"], content))
            self.bucket.stats.increment_manifest_uid()
            status, content = bucket_helper.create_collection(
                self.cluster.buckets[0].name, scope_spec["name"], collection_spec)
            if status is False:
                self.fail("Create collection failed for %s:%s:%s, Reason - %s"
                          % (self.cluster.buckets[0].name, scope_spec["name"],
                             collection_spec["name"], content))
            self.bucket.stats.increment_manifest_uid()

        self.log.info("Creating new buckets with scopes and collections")
        for i in range(1, self.input.param("num_buckets", 1)+1):
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
                if current_items == self.num_items:
                    validation_results["post_upgrade_KV_infra"] = True
                else:
                    self.log.error(
                        "Mismatch in doc_count. Actual: %s, Expected: %s"
                        % (current_items, self.num_items))
                    validation_results["post_upgrade_KV_infra"] = False

        self.log.info("Create CBAS infra post upgrade and check for data "
                      "ingestion")
        if major_version >= 7.0:
            update_spec = {
                "no_of_dataverses": self.input.param('no_of_dv', 2),
                "no_of_datasets_per_dataverse": self.input.param('ds_per_dv', 4),
                "no_of_synonyms": self.input.param('no_of_synonym', 2),
                "no_of_indexes": self.input.param('no_of_index', 3),
                "max_thread_count": self.input.param('no_of_threads', 10),
            }
        else:
            update_spec = {
                "no_of_dataverses": self.input.param('no_of_dv', 2),
                "no_of_datasets_per_dataverse": self.input.param('ds_per_dv', 4),
                "no_of_synonyms": 0,
                "no_of_indexes": self.input.param('no_of_index', 3),
                "max_thread_count": self.input.param('no_of_threads', 10),
                "dataverse": {
                    "cardinality": 1,
                    "creation_method": "dataverse"
                },
                "dataset": {
                    "creation_methods": ["cbas_dataset"],
                    "bucket_cardinality": 1
                },
                "index": {
                    "creation_method": "index"
                }
            }
            if update_spec["no_of_dataverses"] * update_spec[
                "no_of_datasets_per_dataverse"] > 8:
                self.log_failure("Total number of datasets across all "
                                 "dataverses cannot be more than 8 for pre "
                                 "7.0 builds")
                return False

        if self.cbas_setup(update_spec, False):
            validation_results["post_upgrade_cbas_infra"] = True
        else:
            validation_results["post_upgrade_cbas_infra"] = False

        if major_version >= 7.1:
            self.cluster.rest = RestConnection(self.cluster.master)

            def post_replica_activation_verification():
                self.log.info("Verifying doc count accross all datasets")
                if not self.cbas_util.validate_docs_in_all_datasets(
                        self.cluster, self.bucket_util, timeout=600):
                    self.log_failure(
                        "Docs are missing after replicas become active")
                    validation_results["post_upgrade_replica_verification"] = \
                        False

                if update_spec["no_of_indexes"]:
                    self.log.info("Verifying CBAS indexes are working")
                    for idx in self.cbas_util.list_all_index_objs():
                        statement = "Select * from {0} where age > 5 limit 10".format(
                            idx.full_dataset_name)
                        if not self.cbas_util.verify_index_used(
                                self.cluster, statement, index_used=True,
                                index_name=idx.name):
                            self.log.info(
                                "Index {0} on dataset {1} was not used while "
                                "executing query".format(
                                    idx.name, idx.full_dataset_name))

            self.log.info("Marking one of the CBAS nodes as failed over.")
            self.available_servers, kv_failover_nodes, cbas_failover_nodes =\
                self.rebalance_util.failover(
                    self.cluster, kv_nodes=0, cbas_nodes=1,
                    failover_type="Hard", action=None, timeout=7200,
                    available_servers=[], exclude_nodes=[self.cluster.cbas_cc_node],
                    kv_failover_nodes=None, cbas_failover_nodes=None,
                    all_at_once=False)
            post_replica_activation_verification()

            self.available_servers, kv_failover_nodes, cbas_failover_nodes = \
                self.rebalance_util.perform_action_on_failed_over_nodes(
                    self.cluster,
                    action=self.input.param('action_on_failover', "FullRecovery"),
                    available_servers=self.available_servers,
                    kv_failover_nodes=kv_failover_nodes,
                    cbas_failover_nodes=cbas_failover_nodes)

            post_replica_activation_verification()
            validation_results["post_upgrade_replica_verification"] = True

        if major_version >= 7.2:
            datasets = self.cbas_util.list_all_dataset_objs(
                dataset_source="internal")

            # create CBO samples for all datasets
            for dataset in datasets:
                result = self.cbas_util.create_sample_for_analytics_collections(
                    self.cluster, dataset.full_name, random.choice(["low","medium","high"]))
                if not result:
                    validation_results["create_cbo_samples"] = False
            validation_results["create_cbo_samples"] = True

            # Validate whether CBO estimates are being generated
            query = "Explain select count(*) from {0} x,{1} y where x.age=y.age;".format(
                datasets[0].full_name, datasets[1].full_name)
            status, metrics, errors, results, _ = self.cbas_util.execute_statement_on_cbas_util(self.cluster, query)
            if status != "success":
                self.fail("Error while running analytics query")
            elif 'optimizer-estimates' in results[0]:
                validation_results["cbo_estimates"] = True
            else:
                validation_results["cbo_estimates"] = True

            # Drop all CBO samples created above
            for dataset in datasets:
                result = self.cbas_util.drop_sample_for_analytics_collections(
                    self.cluster, dataset.full_name)
                if not result:
                    validation_results["drop_cbo_samples"] = False
            validation_results["drop_cbo_samples"] = True

        self.log.info("Delete the bucket created before upgrade")
        if self.bucket_util.delete_bucket(
                self.cluster, self.cluster.buckets[0], wait_for_bucket_deletion=True):
            validation_results["bucket_delete"] = True
        else:
            validation_results["bucket_delete"] = False

        if validation_results["bucket_delete"]:
            self.log.info("Check all datasets created on the deleted bucket "
                          "are empty")
            results = []
            for dataset in self.cbas_util.list_all_dataset_objs(
                    dataset_source="internal"):
                if dataset.kv_bucket.name == "default":
                    if self.cbas_util.wait_for_ingestion_complete(
                            self.cluster, dataset.full_name, 0, timeout=300):
                        results.append(True)
                    else:
                        results.append(False)
            validation_results["bucket_delete"] = all(results)

        for scenario in validation_results:
            if validation_results[scenario]:
                self.log.info("{0} : Passed".format(scenario))
            else:
                self.log.info("{0} : Failed".format(scenario))

        return validation_results

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

    def test_upgrade(self):
        self.log.info("Upgrading cluster nodes to target version")
        node_to_upgrade = self.fetch_node_to_upgrade()
        while node_to_upgrade is not None:
            self.log.info("Selected node for upgrade: %s"
                          % node_to_upgrade.ip)
            if self.upgrade_type == "offline":
                self.upgrade_function[self.upgrade_type](
                    node_to_upgrade, self.upgrade_version, True)
            else:
                self.upgrade_function[self.upgrade_type](node_to_upgrade,
                                                         self.upgrade_version)
            self.cluster_util.print_cluster_stats(self.cluster)
            node_to_upgrade = self.fetch_node_to_upgrade()
        if not all(self.post_upgrade_validation().values()):
            self.fail("Post upgrade scenarios failed")

    def test_upgrade_with_failover(self):
        self.log.info("Upgrading cluster nodes to target version")
        node_to_upgrade = self.fetch_node_to_upgrade()
        while node_to_upgrade is not None:
            self.log.info("Selected node for upgrade: %s"
                          % node_to_upgrade.ip)
            rest = RestConnection(node_to_upgrade)
            services = rest.get_nodes_services()
            services_on_target_node = services[(node_to_upgrade.ip + ":"
                                                + node_to_upgrade.port)]
            self.log.info("Selected node services {0}".format(
                services_on_target_node))
            if "cbas" in services_on_target_node:
                self.upgrade_function["failover_full_recovery"](
                    node_to_upgrade, False)
            else:
                self.upgrade_function[self.upgrade_type](node_to_upgrade)
            self.cluster_util.print_cluster_stats(self.cluster)
            node_to_upgrade = self.fetch_node_to_upgrade()
        if not all(self.post_upgrade_validation().values()):
            self.fail("Post upgrade scenarios failed")
