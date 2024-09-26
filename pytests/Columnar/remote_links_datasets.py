from queue import Queue

from Columnar.columnar_base import ColumnarBaseTest

# External Database loader related imports
from Jython_tasks.sirius_task import CouchbaseUtil
from sirius_client_framework.sirius_constants import SiriusCodes


class RemoteLinksDatasets(ColumnarBaseTest):
    def setUp(self):
        super(RemoteLinksDatasets, self).setUp()
        self.columnar_cluster = self.tenant.columnar_instances[0]

        self.remote_cluster = self.tenant.clusters[0]
        self.couchbase_doc_loader = CouchbaseUtil(
            task_manager=self.task_manager,
            hostname=self.remote_cluster.master.ip,
            username=self.remote_cluster.master.rest_username,
            password=self.remote_cluster.master.rest_password,
        )

        self.initial_doc_count = self.input.param("initial_doc_count", 100)
        self.doc_size = self.input.param("doc_size", 1024)

        if not self.columnar_spec_name:
            self.columnar_spec_name = "full_template"

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        """
        Delete all the analytics link and columnar instance
        """
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.columnar_cluster, self.columnar_spec):
            self.fail("Error while deleting cbas entities")

        self.delete_all_buckets_from_capella_cluster(
            self.tenant, self.remote_cluster)
        super(ColumnarBaseTest, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished", stage="Teardown")

    def load_doc_to_remote_collections(self, start, end):
        for remote_bucket in self.remote_cluster.buckets:
            for scope_name, scope in remote_bucket.scopes.items():
                if scope_name != "_system" and scope != "_mobile":
                    for collection_name, collection in (
                            scope.collections.items()):
                        self.log.info(
                            f"Loading docs in {remote_bucket.name}."
                            f"{scope_name}.{collection_name}")
                        cb_doc_loading_task = self.couchbase_doc_loader.load_docs_in_couchbase_collection(
                            bucket=remote_bucket.name, scope=scope_name,
                            collection=collection_name, start=start,
                            end=end, 
                            doc_template=SiriusCodes.Templates.PRODUCT,
                            doc_size=self.doc_size, sdk_batch_size=1000
                        )
                        if not cb_doc_loading_task.result:
                            self.fail(
                                f"Failed to load docs in couchbase collection "
                                f"{remote_bucket.name}.{scope_name}.{collection_name}")
                        else:
                            collection.num_items = cb_doc_loading_task.success_count

    def test_create_connect_disconnect_query_drop_remote_links_and_datasets(self):
        # creating bucket scope and collections for remote collection
        self.create_bucket_scopes_collections_in_capella_cluster(
            self.tenant, self.remote_cluster,
            self.input.param("num_buckets", 1))

        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=self.remote_cluster)

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
        if not result:
            self.fail(msg)

        self.load_doc_to_remote_collections(0, self.initial_doc_count)

        remote_links = self.cbas_util.get_all_link_objs("couchbase")
        remote_datasets = self.cbas_util.get_all_dataset_objs("remote")

        for link in remote_links:
            if not self.cbas_util.connect_link(self.columnar_cluster, link.full_name):
                self.fail("Failed to connect link")

        jobs = Queue()
        results = []
        for dataset in remote_datasets:
            jobs.put((
                self.cbas_util.get_num_items_in_cbas_dataset,
                {"cluster": self.columnar_cluster, "dataset_name": dataset.full_name,
                 "timeout": 3600, "analytics_timeout": 3600}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        for result in results:
            if result != self.initial_doc_count:
                self.fail("Doc count mismatch. Expected - {0}, Actual - {"
                          "1}".format(self.initial_doc_count, result))

        for link in remote_links:
            if not self.cbas_util.disconnect_link(self.columnar_cluster, link.full_name):
                self.fail("Failed to disconnect link")

    def test_alter_remote_links(self):
        # creating bucket scope and collections for remote collection
        self.create_bucket_scopes_collections_in_capella_cluster(
            self.tenant, self.remote_cluster,
            self.input.param("num_buckets", 1))

        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=self.remote_cluster)

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
        if not result:
            self.fail(msg)

        remote_links = self.cbas_util.get_all_link_objs("couchbase")
        remote_datasets = self.cbas_util.get_all_dataset_objs("remote")

        for link in remote_links:
            if not self.cbas_util.connect_link(self.columnar_cluster, link.full_name):
                self.fail("Failed to connect link")

        for link in remote_links:
            if not self.cbas_util.disconnect_link(self.columnar_cluster, link.full_name):
                self.fail("Failed to disconnect link")

        remote_link_properties = self.columnar_spec["remote_link"]["properties"]
        remote_link_properties[0]["encryption"] = "half"
        self.cbas_util.alter_link_properties(self.columnar_cluster, remote_link_properties[0])

        self.load_doc_to_remote_collections(self.bucket_name, self.scope_name,
                                            self.collection_name, 0, self.initial_doc_count)

        for link in remote_links:
            if not self.cbas_util.connect_link(self.columnar_cluster, link.full_name):
                self.fail("Failed to connect link")

        jobs = Queue()
        results = []
        for dataset in remote_datasets:
            jobs.put((
                self.cbas_util.wait_for_ingestion_complete,
                {"cluster": self.columnar_cluster, "dataset_name": dataset.full_name,
                 "num_items": self.initial_doc_count, "timeout": 3600}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.fail("Data ingestion did not complete for all datasets")

        for link in remote_links:
            if not self.cbas_util.disconnect_link(self.columnar_cluster, link.full_name):
                self.fail("Failed to disconnect link")

    def test_remote_links_operations_during_remote_cluster_scaling_operation(self):
        # creating bucket scope and collections for remote collection
        self.create_bucket_scopes_collections_in_capella_cluster(
            self.tenant, self.remote_cluster,
            self.input.param("num_buckets", 1))

        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=self.remote_cluster)

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
        if not result:
            self.fail(msg)

        self.load_doc_to_remote_collections(0, self.initial_doc_count)

        remote_links = self.cbas_util.get_all_link_objs("couchbase")
        remote_datasets = self.cbas_util.get_all_dataset_objs("remote")

        for link in remote_links:
            if not self.cbas_util.connect_link(self.columnar_cluster, link.full_name):
                self.fail("Failed to connect link")

        capella_cluster_config = self.capella_cluster_config
        capella_cluster_config["specs"][0]["count"] += 2
        capella_cluster_config["specs"][0]["services"] = [{"type": "kv"}]
        capella_cluster_config["specs"][0]["compute"] = {"type": self.compute["data"]}
        rebalance_task = self.task.async_rebalance_capella(self.pod, self.tenant,
                                                           self.remote_cluster,
                                                           capella_cluster_config["specs"])
        self.task_manager.get_task_result(rebalance_task)

        capella_cluster_config["specs"][0]["count"] -= 2
        rebalance_task = self.task.async_rebalance_capella(self.pod, self.tenant,
                                                           self.remote_cluster,
                                                           capella_cluster_config["specs"])
        self.task_manager.get_task_result(rebalance_task)

        jobs = Queue()
        results = []
        for dataset in remote_datasets:
            jobs.put((
                self.cbas_util.get_num_items_in_cbas_dataset,
                {"cluster": self.columnar_cluster, "dataset_name": dataset.full_name,
                 "timeout": 3600, "analytics_timeout": 3600}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        for result in results:
            if result != self.initial_doc_count:
                self.fail("Doc count mismatch. Expected - {0}, Actual - {"
                          "1}".format(self.initial_doc_count, result))

        for link in remote_links:
            if not self.cbas_util.disconnect_link(self.columnar_cluster, link.full_name):
                self.fail("Failed to disconnect link")

    def test_remote_link_operations_before_and_after_pause_resume_of_remote_cluster(self):
        # creating bucket scope and collections for remote collection
        self.create_bucket_scopes_collections_in_capella_cluster(
            self.tenant, self.remote_cluster,
            self.input.param("num_buckets", 1))

        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=self.remote_cluster)

        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
        if not result:
            self.fail(msg)

        self.load_doc_to_remote_collections(0, self.initial_doc_count)

        remote_links = self.cbas_util.get_all_link_objs("couchbase")
        remote_datasets = self.cbas_util.get_all_dataset_objs("remote")

        for link in remote_links:
            if not self.cbas_util.connect_link(self.columnar_cluster, link.full_name):
                self.fail("Failed to connect link")

        jobs = Queue()
        results = []
        for dataset in remote_datasets:
            jobs.put((
                self.cbas_util.get_num_items_in_cbas_dataset,
                {"cluster": self.columnar_cluster, "dataset_name": dataset.full_name,
                 "timeout": 3600, "analytics_timeout": 3600}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        for result in results:
            if result != self.initial_doc_count:
                self.fail("Doc count mismatch. Expected - {0}, Actual - {"
                          "1}".format(self.initial_doc_count, result))

        if not self.columnar_utils.turn_off_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_off=True):
            self.fail("Failed to Turn-Off the cluster")

        # resume the instance
        if not self.columnar_utils.turn_on_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster, wait_to_turn_on=True):
            self.fail("Failed to Turn-On the cluster")

        self.load_doc_to_remote_collections(
            self.initial_doc_count, self.initial_doc_count * 2)

        for dataset in remote_datasets:
            if not self.cbas_util.wait_for_ingestion_complete(
                    self.columnar_cluster, dataset.full_name,
                    self.initial_doc_count * 2):
                self.fail("Doc count mismatch.")

        for link in remote_links:
            if not self.cbas_util.disconnect_link(self.columnar_cluster, link.full_name):
                self.fail("Failed to disconnect link")
