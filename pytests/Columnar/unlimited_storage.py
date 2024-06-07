"""
Created on 22-MAY-2024

@author: umang.agrawal

Assumptions -
1. All the datasets should have equal amount of data.

Default Configurations -
1. Total data to be ingested into columnar instance will be 10 times the value
set for cloud_storage_debug_sweep_threshold_size_in_GB.
    cloud_storage_debug_sweep_threshold_size_in_GB - Default 1
2. A bucket with 10 collections in _default scope will be created in remote
cluster.
3. 10 remote collections will be created, 1 on each of the remote collection.
"""
import random
import time

from Columnar.columnar_base import ColumnarBaseTest
from Jython_tasks.task import RunQueriesTask
from threading import Thread

# Imports for Sirius data loaders
from sirius_client_framework.multiple_database_config import CouchbaseLoader
from sirius_client_framework.operation_config import WorkloadOperationConfig
from Jython_tasks.sirius_task import WorkLoadTask
from sirius_client_framework.sirius_constants import SiriusCodes


class UnlimitedStorage(ColumnarBaseTest):

    def setUp(self):
        super(UnlimitedStorage, self).setUp()

        # Since all the test cases are being run on 1 cluster only
        self.instance = self.tenant.columnar_instances[0]
        # remove this
        self.cbas_util.cleanup_cbas(self.instance)

        # Setting debug flags for unlimited storage
        self.log.info("Setting debug flags for Unlimited Storage")
        self.nvme_threshold = self.input.param(
            "cloud_storage_debug_sweep_threshold_size_in_GB", 1) * 1073741824
        unlimited_storage_debug_flags = {
            "cloudStorageCachePolicy": self.input.param(
                "cloud_storage_cache_policy", "selective"),
            "cloudStorageDiskMonitorInterval": self.input.param(
                "cloud_storage_disk_monitor_interval", 60),
            "cloudStorageIndexInactiveDurationThreshold": self.input.param(
                "cloud_storage_index_inactive_duration_threshold", 1),
            "cloudStorageDebugModeEnabled": self.input.param(
                "cloud_storage_debug_mode_enabled", True),
            "cloudStorageDebugSweepThresholdSize": self.nvme_threshold
        }
        status, content, response = self.cbas_util.update_service_parameter_configuration_on_cbas(
            self.instance, unlimited_storage_debug_flags,
            self.instance.username, self.instance.password)
        if not status:
            self.fail("Failed to set debug flags for Unlimited Storage")

        self.log.info("Restarting columnar instance")
        status, content, response = self.cbas_util.restart_analytics_cluster_uri(
            self.instance, self.instance.username, self.instance.password)
        if not status:
            self.fail("Failed to restart Columnar instance")

        if not self.cbas_util.is_analytics_running(self.instance):
            self.fail("Analytics service did not come up post instance "
                      "restart")

        if not self.columnar_spec_name:
            self.columnar_spec_name = "sanity.on-prem.bvf_e2e"

        self.columnar_spec = self.cbas_util.get_columnar_spec(
            self.columnar_spec_name)

        self.remote_cluster = self.tenant.clusters[0]
        self.use_existing_bucket = self.input.param("use_existing_bucket", False)

        self.doc_template_info = dict()

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.instance, self.columnar_spec):
            self.fail("Error while deleting cbas entities")
        if not self.use_existing_bucket:
            self.delete_all_buckets_from_capella_cluster(
                self.tenant, self.remote_cluster)
        super(UnlimitedStorage, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage="Teardown")

    def build_cbas_columnar_infra(self):

        # Updating Database spec
        self.columnar_spec["database"]["no_of_databases"] = 2

        # Updating Dataverse/Scope spec
        self.columnar_spec["dataverse"]["no_of_dataverses"] = 2

        # Updating Remote Links Spec
        resp = self.capellaAPI.cluster_ops_apis.add_CIDR_to_allowed_CIDRs_list(
            self.tenant.id, self.tenant.project_id, self.remote_cluster.id,
            "0.0.0.0/0")
        if resp.status_code == 201 or resp.status_code == 422:
            self.log.info("Added allowed IP 0.0.0.0/0")
        else:
            self.fail("Failed to add allowed IP")
        resp = self.capellaAPI.cluster_ops_apis.get_cluster_certificate(
            self.tenant.id, self.tenant.project_id, self.remote_cluster.id)
        if resp.status_code == 200:
            certificate = resp.json()["certificate"]
        else:
            self.fail("Failed to get cluster certificate")

        self.columnar_spec["remote_link"][
            "no_of_remote_links"] = 1
        self.columnar_spec["remote_link"]["properties"] = [{
            "type": "couchbase",
            "hostname": self.remote_cluster.master.ip,
            "username": self.remote_cluster.master.rest_username,
            "password": self.remote_cluster.master.rest_password,
            "encryption": "full",
            "certificate": certificate}]

        self.columnar_spec["remote_dataset"]["num_of_remote_datasets"] = 10

        if not self.cbas_util.create_cbas_infra_from_spec(
                cluster=self.instance, cbas_spec=self.columnar_spec,
                bucket_util=self.bucket_util, wait_for_ingestion=False,
                remote_clusters=[self.remote_cluster]):
            self.fail("Error while creating analytics entities.")

    def load_initial_data(self):
        total_docs_per_collection = ((self.nvme_threshold // self.doc_size) *
                                     len(self.instance.nodes_in_cluster))

        for remote_bucket in self.remote_cluster.buckets:
            for scope_name, scope in remote_bucket.scopes.items():
                if scope_name != "_system":
                    for collection_name, collection in (
                            scope.collections.items()):

                        database_information = CouchbaseLoader(
                            username=self.remote_cluster.master.rest_username,
                            password=self.remote_cluster.master.rest_password,
                            connection_string="couchbases://{0}".format(
                                self.remote_cluster.master.ip),
                            bucket=remote_bucket.name, scope=scope_name,
                            collection=collection_name, sdk_batch_size=200)

                        # load docs in batches of 1 GB
                        initial_operation_config = WorkloadOperationConfig(
                            start=0, end=0,
                            template=SiriusCodes.Templates.PRODUCT,
                            doc_size=self.doc_size,
                        )
                        if total_docs_per_collection * self.doc_size > 1073741824:
                            increment_by = 1073741824 // self.doc_size
                            initial_operation_config.end = increment_by
                        else:
                            increment_by = 1
                            initial_operation_config.end = total_docs_per_collection

                        while (initial_operation_config.end <=
                               total_docs_per_collection):
                            task_insert = WorkLoadTask(
                                task_manager=self.task_manager,
                                op_type=SiriusCodes.DocOps.BULK_CREATE,
                                database_information=database_information,
                                operation_config=initial_operation_config)
                            self.task_manager.add_new_task(task_insert)
                            self.task_manager.get_task_result(task_insert)
                            if task_insert.fail_count == total_docs_per_collection:
                                self.fail(
                                    "Doc loading failed for {0}.{1}.{2}".format(
                                        remote_bucket.name, scope_name,
                                        collection_name))
                            else:
                                if task_insert.fail_count > 0:
                                    self.log.error(
                                        "{0} Docs failed to load for {1}.{2}.{3}"
                                        .format(task_insert.fail_count,
                                                remote_bucket.name, scope_name,
                                                collection_name))
                                    collection.num_items = (
                                            total_docs_per_collection -
                                            task_insert.fail_count)
                                else:
                                    collection.num_items = total_docs_per_collection
                            initial_operation_config.start = (
                                initial_operation_config.end)
                            initial_operation_config.end += increment_by

    def validate_query_execution_metrics(self, query_results):
        for result in query_results:
            if result["errors"]:
                self.fail(f"Error observed while running query - "
                          f"{result['errors']}")

    def generate_doc_template_from_doc(self):
        if self.use_existing_bucket:
            time.sleep(60)
        collection = self.cbas_util.get_all_dataset_objs()[0]
        status, _, _, result, _ = self.cbas_util.execute_statement_on_cbas_util(
            self.instance, f"select value b from {collection.full_name} as b limit 1")
        if status == "success":
            result = result[0]
            for key, value in result.items():
                if isinstance(value, str):
                    self.doc_template_info[key] = "string"
                elif isinstance(value, int):
                    self.doc_template_info[key] = "bigint"
                elif isinstance(value, float):
                    self.doc_template_info[key] = "double"
                elif isinstance(value, list):
                    self.doc_template_info[key] = "array"
                elif isinstance(value, dict):
                    self.doc_template_info[key] = "object"
        else:
            self.fail(f"Unable to fetch document from {collection.full_name}")

    def test_unlimited_storage(self):

        if self.use_existing_bucket:
            self.generate_bucket_object_for_existing_buckets(
                self.tenant, self.remote_cluster)
        else:
            self.log.info("Creating Buckets, Scopes and Collections on remote "
                          "cluster.")
            self.create_bucket_scopes_collections_in_capella_cluster(
                self.tenant, self.remote_cluster, num_buckets=1,
                bucket_ram_quota=3000, num_scopes_per_bucket=1,
                num_collections_per_scope=10)

        self.build_cbas_columnar_infra()

        if not self.use_existing_bucket:
            self.load_initial_data()

        self.cbas_util.wait_for_ingestion_all_datasets(self.instance)

        self.generate_doc_template_from_doc()

        all_collections = self.cbas_util.get_all_dataset_objs()

        self.log.debug("Running query on 8 collections, so that the data for "
                       "the other 2 collections might get evicted from NVMe")
        queries = list()
        for collection in all_collections[:-2]:
            queries.append(f"SET `compiler.external.field.pushdown` "
                           f"\"false\"; SELECT COUNT(*) from "
                           f"{collection.full_name}")

        query_task = RunQueriesTask(
            cluster=self.instance, queries=queries,
            task_manager=self.task_manager, helper=self.cbas_util,
            query_type="cbas", parallelism=3, is_prepared=True,
            record_results=True, return_only_results=False)

        for i in range(0, 5):
            self.task_manager.add_new_task(query_task)
            self.task_manager.get_task_result(query_task)

        query_results = query_task.result
        self.validate_query_execution_metrics(query_results)

        self.log.debug("Running query on the 2 collections which were not "
                       "used above, so verify cache miss")
        query_task.queries = list()
        for collection in all_collections[-2:]:
            query_task.queries.append(
                f"SET `compiler.external.field.pushdown` \"false\"; SELECT "
                f"COUNT(*) from {collection.full_name}")
        query_task.result = list()
        query_task.query_tasks = list()

        for i in range(0, 5):
            self.task_manager.add_new_task(query_task)
            self.task_manager.get_task_result(query_task)

        query_results = query_task.result
        self.validate_query_execution_metrics(query_results)

        self.log.debug("Creating secondary index on one of the collection")
        collection = all_collections[0]
        indexed_field = "id:string"
        if not self.cbas_util.create_cbas_index(
                self.instance, "idx1", [indexed_field], collection.full_name):
            self.fail(f"Error while creating index on {collection.full_name}")

        self.log.debug("Running query which access some columns on all "
                       "collections expect on the collection on which the "
                       "index was created")
        columns_to_skip = random.sample(list(self.doc_template_info.keys()), 2)
        query = "SET `compiler.external.field.pushdown` \"false\"; SELECT "
        for column in list(self.doc_template_info.keys()):
            if column not in columns_to_skip:
                query += f"count({column}) as {column}_count, "
        query = query.rstrip(", ") + " from {0};"
        query_task.queries = list()
        for collection in all_collections[1:]:
            query_task.queries.append(query.format(collection.full_name))
        query_task.result = list()
        query_task.query_tasks = list()

        for i in range(0, 5):
            self.task_manager.add_new_task(query_task)
            self.task_manager.get_task_result(query_task)

        query_results = query_task.result
        self.validate_query_execution_metrics(query_results)

        self.log.debug("Running query which access the columns which were "
                       "not accessed in last step on all collections expect "
                       "on the collection on which the index was created")
        query = "SET `compiler.external.field.pushdown` \"false\"; SELECT "
        for column in columns_to_skip:
            query += f"count({column}) as {column}_count, "
        query = query.rstrip(", ") + " from {0};"
        query_task.queries = list()
        for collection in all_collections[1:]:
            query_task.queries.append(query.format(collection.full_name))
        query_task.result = list()
        query_task.query_tasks = list()

        for i in range(0, 5):
            self.task_manager.add_new_task(query_task)
            self.task_manager.get_task_result(query_task)

        query_results = query_task.result
        self.validate_query_execution_metrics(query_results)

        self.log.debug("Running query on collection on which the index was "
                       "created")
        query_task.queries = list()
        query_task.queries.append(
            f"SET `compiler.external.field.pushdown` \"false\"; SELECT "
            f"COUNT(*) from {all_collections[0].full_name}")
        query_task.result = list()
        query_task.query_tasks = list()

        for i in range(0, 5):
            self.task_manager.add_new_task(query_task)
            self.task_manager.get_task_result(query_task)

        query_results = query_task.result
        self.validate_query_execution_metrics(query_results)

        self.log.debug("Creating a new collection in existing remote KV "
                       "bucket")
        remote_bucket, kv_scope, kv_collection = None, None, None
        for bucket in self.remote_cluster.buckets:
            for scope_name, scope in bucket.scopes.items():
                if scope_name != "_system":
                    collection_name = self.cbas_util.generate_name()
                    resp = self.capellaAPI.cluster_ops_apis.create_collection(
                        self.tenant.id, self.tenant.project_id,
                        clusterId=self.remote_cluster.id,
                        bucketId=bucket.uuid, scopeName=scope_name,
                        name=collection_name)
                    if resp.status_code == 201:
                        self.log.info(
                            "Create collection {} in scope {}".format(
                                collection_name, scope_name))
                        self.bucket_util.create_collection_object(
                            bucket, scope_name,
                            collection_spec={"name": collection_name})
                        collection = scope.collections[collection_name]
                    else:
                        self.fail(
                            "Failed creating collection {} in scope {}".format(
                                collection_name, scope_name))
                    remote_bucket = bucket
                    kv_scope = scope
                    kv_collection = collection

        self.stop_data_loading = False

        def start_parallel_data_load_into_kv_collection(
                bucket, scope, collection):
            increment = 1073741824 // self.doc_size

            initial_operation_config = WorkloadOperationConfig(
                start=0, end=increment, template=SiriusCodes.Templates.PRODUCT,
                doc_size=self.doc_size,
            )

            database_information = CouchbaseLoader(
                username=self.remote_cluster.master.rest_username,
                password=self.remote_cluster.master.rest_password,
                connection_string="couchbases://{0}".format(
                    self.remote_cluster.master.ip),
                bucket=bucket.name, scope=scope.name,
                collection=collection.name, sdk_batch_size=200)

            while not self.stop_data_loading:
                task_insert = WorkLoadTask(
                    task_manager=self.task_manager,
                    op_type=SiriusCodes.DocOps.BULK_CREATE,
                    database_information=database_information,
                    operation_config=initial_operation_config,
                )
                self.task_manager.add_new_task(task_insert)
                self.task_manager.get_task_result(task_insert)
                if task_insert.fail_count == initial_operation_config.end:
                    self.fail(
                        "Doc loading failed for {0}.{1}.{2}".format(
                            bucket.name, scope.name, collection.name))
                else:
                    if task_insert.fail_count > 0:
                        self.log.error(
                            "{0} Docs failed to load for {1}.{2}.{3}"
                            .format(task_insert.fail_count,
                                    bucket.name, scope.name, collection.name))
                        collection.num_items = (
                                initial_operation_config.end -
                                task_insert.fail_count)
                    else:
                        collection.num_items = initial_operation_config.end
                initial_operation_config.start = initial_operation_config.end
                initial_operation_config.end += increment

        data_load_task = Thread(
            target=start_parallel_data_load_into_kv_collection,
            name="parallel_data_load",
            args=(remote_bucket, kv_scope, kv_collection))
        data_load_task.start()

        collection_objs = self.cbas_util.create_remote_dataset_obj(
            self.instance, remote_bucket, kv_scope, kv_collection, None,
            self.bucket_util, True, True, "column", capella_as_source=True)
        for collection in collection_objs:
            if not self.cbas_util.create_remote_dataset(
                    self.instance, collection.name,
                    collection.full_kv_entity_name, collection.link_name,
                    collection.dataverse_name, collection.database_name,
                    storage_format=collection.storage_format
            ):
                self.fail(f"Error while creating remote collection "
                          f"{collection.full_name}")
        all_collections.extend(collection_objs)

        self.log.debug("Creating secondary index on one of the collection")
        indexed_field = "id:string"
        if not self.cbas_util.create_cbas_index(
                self.instance, "idx1", [indexed_field],
                collection_objs[0].full_name):
            self.fail(
                f"Error while creating index on {collection_objs[0].full_name}")

        self.log.debug("Running query on all the collections and verify "
                       "there is no cache miss for the new collection.")
        query_task.queries = list()
        for collection in all_collections:
            query_task.queries.append(
                f"SET `compiler.external.field.pushdown` \"false\"; SELECT "
                f"COUNT(*) from {collection.full_name}")
        query_task.result = list()
        query_task.query_tasks = list()

        for i in range(0, 5):
            self.task_manager.add_new_task(query_task)
            self.task_manager.get_task_result(query_task)

        query_results = query_task.result
        self.validate_query_execution_metrics(query_results)

        self.log.debug("Stopping data load task.")
        self.stop_data_loading = True
        data_load_task.join()

        self.log.info("Scaling out instance")
        num_nodes = len(self.instance.nodes_in_cluster) * 2
        if not self.columnar_utils.scale_instance(
                self.pod, self.tenant, self.tenant.project_id, self.instance,
                num_nodes):
            self.fail(
                "Scale API failed while scaling instance from {0} --> "
                "{1}".format(
                    len(self.instance.nodes_in_cluster), num_nodes))

        if not self.columnar_utils.wait_for_instance_scaling_operation(
                self.pod, self.tenant, self.tenant.project_id, self.instance):
            self.fail("Failed to scale OUT instance even after 3600 seconds")

        self.update_columnar_instance_obj(self.pod, self.tenant, self.instance)

        self.log.debug("Running query post scale OUT, approx 50% cache miss "
                       "should be observed")
        query_task.queries = list()
        query_task.queries.append(
            f"SET `compiler.external.field.pushdown` \"false\"; SELECT "
            f"COUNT(*) from {all_collections[0].full_name}")
        query_task.result = list()
        query_task.query_tasks = list()

        for i in range(0, 5):
            self.task_manager.add_new_task(query_task)
            self.task_manager.get_task_result(query_task)

        query_results = query_task.result
        self.validate_query_execution_metrics(query_results)

        self.log.info("Scaling in instance")
        num_nodes = len(self.instance.nodes_in_cluster) // 2
        # fix this code in columnar util
        if not self.columnar_utils.scale_instance(
                self.pod, self.tenant, self.tenant.project_id, self.instance,
                num_nodes):
            self.fail(
                "Scale API failed while scaling instance from {0} --> "
                "{1}".format(
                    len(self.instance.nodes_in_cluster), num_nodes))

        if not self.columnar_utils.wait_for_instance_scaling_operation(
                self.pod, self.tenant, self.tenant.project_id, self.instance):
            self.fail("Failed to scale IN instance even after 3600 seconds")

        self.update_columnar_instance_obj(self.pod, self.tenant, self.instance)

        self.log.debug("Running query post scale IN, approx 50% cache miss "
                       "should be observed")
        query_task.queries = list()
        query_task.queries.append(
            f"SET `compiler.external.field.pushdown` \"false\"; SELECT "
            f"COUNT(*) from {all_collections[0].full_name}")
        query_task.result = list()
        query_task.query_tasks = list()

        for i in range(0, 5):
            self.task_manager.add_new_task(query_task)
            self.task_manager.get_task_result(query_task)

        query_results = query_task.result
        self.validate_query_execution_metrics(query_results)

        self.log.info("Turning instance off and then on.")
        if not self.columnar_utils.turn_off_instance(
                self.pod, self.tenant, self.tenant.project_id, self.instance):
            self.fail("Turning off instance failed.")

        if not self.columnar_utils.turn_on_instance(
                self.pod, self.tenant, self.tenant.project_id, self.instance):
            self.fail("Turning off instance failed.")

        self.update_columnar_instance_obj(self.pod, self.tenant, self.instance)

        self.log.debug("Running query post instance off/on, 100% cache miss "
                       "is expected")
        query_task.queries = list()
        query_task.queries.append(
            f"SET `compiler.external.field.pushdown` \"false\"; SELECT "
            f"COUNT(*) from {all_collections[0].full_name}")
        query_task.result = list()
        query_task.query_tasks = list()

        for i in range(0, 5):
            self.task_manager.add_new_task(query_task)
            self.task_manager.get_task_result(query_task)

        query_results = query_task.result
        self.validate_query_execution_metrics(query_results)

        if self.use_existing_bucket:
            resp = self.capellaAPI.cluster_ops_apis.delete_collection(
                self.tenant.id, self.tenant.project_id,
                clusterId=self.remote_cluster.id,
                bucketId=remote_bucket.uuid, scopeName=kv_scope.name,
                collectionName=kv_collection.name)
            if resp.status_code != 204:
                self.fail(f"Error while deleting collection "
                          f"{remote_bucket.name}.{kv_scope.name}"
                          f".{kv_collection.name}")
