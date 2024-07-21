from queue import Queue

from Columnar.columnar_base import ColumnarBaseTest
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI
from BucketLib.bucket import Bucket
from pytests.aGoodDoctor.hostedOnOff import DoctorHostedOnOff
from sirius_client_framework.multiple_database_config import CouchbaseLoader
from sirius_client_framework.operation_config import WorkloadOperationConfig
from Jython_tasks.sirius_task import WorkLoadTask
from sirius_client_framework.sirius_constants import SiriusCodes


class RemoteLinksDatasets(ColumnarBaseTest):
    def setUp(self):
        super(RemoteLinksDatasets, self).setUp()
        self.cluster = self.tenant.columnar_instances[0]
        self.remote_cluster = list(self.cb_clusters.values())[0]

        self.initial_doc_count = self.input.param("initial_doc_count", 100)
        self.doc_size = self.input.param("doc_size", 1024)
        self.capellaAPI = CapellaAPI(self.pod.url_public, '', '', self.tenant.user,
                                     self.tenant.pwd, '')

        if not self.columnar_spec_name:
            self.columnar_spec_name = "sanity.remote_links_datasets"
        self.columnar_spec = self.cbas_util.get_columnar_spec(self.columnar_spec_name)

        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        """
        Delete all the analytics link and columnar instance
        """
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.cluster, self.columnar_spec):
            self.fail("Error while deleting cbas entities")

        if hasattr(self, 'bucket_id'):
            self.delete_capella_bucket(self.bucket_id)
        super(ColumnarBaseTest, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished", stage="Teardown")

    def base_infra_setup(self):
        self.columnar_spec["dataverse"]["no_of_dataverses"] = self.input.param(
            "no_of_scopes", 1)
        self.columnar_spec["remote_link"]["no_of_remote_links"] = self.input.param(
            "no_of_remote_links", 1)

        resp = (self.capellaAPI.create_control_plane_api_key(self.tenant.id, 'init api keys')).json()
        self.capellaAPI.cluster_ops_apis.SECRET = resp['secretKey']
        self.capellaAPI.cluster_ops_apis.ACCESS = resp['id']
        self.capellaAPI.cluster_ops_apis.bearer_token = resp['token']
        self.capellaAPI.org_ops_apis.SECRET = resp['secretKey']
        self.capellaAPI.org_ops_apis.ACCESS = resp['id']
        self.capellaAPI.org_ops_apis.bearer_token = resp['token']

        resp = self.capellaAPI.org_ops_apis.create_api_key(
            organizationId=self.tenant.id,
            name=self.cbas_util.generate_name(),
            organizationRoles=["organizationOwner"],
            description=self.cbas_util.generate_name())
        if resp.status_code == 201:
            self.capella_cluster_keys = resp.json()
        else:
            self.fail("Error while creating API key for organization owner")

        resp = self.capellaAPI.cluster_ops_apis.add_CIDR_to_allowed_CIDRs_list(
            self.tenant.id, self.tenant.project_id, self.remote_cluster.id, "0.0.0.0/0")
        if resp.status_code == 201 or resp.status_code == 422:
            self.log.info("Added allowed IP 0.0.0.0/0")
        else:
            self.fail("Failed to add allowed IP")
        remote_cluster_certificate_request = (
            self.capellaAPI.cluster_ops_apis.get_cluster_certificate(self.tenant.id,
                                                                     self.tenant.project_id,
                                                                     self.remote_cluster.id))
        if remote_cluster_certificate_request.status_code == 200:
            remote_cluster_certificate = (remote_cluster_certificate_request.json()["certificate"])
        else:
            self.fail("Failed to get cluster certificate")
        if self.input.param("no_of_remote_links", 1):
            remote_link_properties = list()
            remote_link_properties.append(
                {"type": "couchbase", "hostname": self.remote_cluster.srv,
                    "username": self.rest_username,
                    "password": self.rest_password,
                    "encryption": "full",
                    "certificate": remote_cluster_certificate})
            self.columnar_spec["remote_link"]["properties"] = remote_link_properties
            self.columnar_spec["remote_dataset"]["num_of_remote_datasets"] = \
            self.input.param("num_of_remote_coll", 1)

        self.bucket_id, self.bucket_name = self.create_capella_bucket()
        self.scope_name = self.create_capella_scope(self.bucket_id)
        self.collection_name = self.create_capella_collection(self.bucket_id,
                                                             self.scope_name)
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            self.cluster, self.columnar_spec, self.bucket_util, False, [self.remote_cluster])
        if not result:
            self.fail(msg)

    def create_capella_bucket(self):
        bucket = Bucket(
            {Bucket.name: self.cbas_util.generate_name(),
             Bucket.ramQuotaMB: 2000,
             Bucket.maxTTL: self.bucket_ttl,
             Bucket.replicaNumber: self.num_replicas,
             Bucket.storageBackend: self.bucket_storage,
             Bucket.evictionPolicy: self.bucket_eviction_policy,
             Bucket.durabilityMinLevel: self.bucket_durability_level,
             Bucket.flushEnabled: True})
        response = (self.capellaAPI.cluster_ops_apis.create_bucket(
            self.tenant.id, self.tenant.project_id,
            self.remote_cluster.id, bucket.name, "couchbase",
            bucket.storageBackend, bucket.ramQuotaMB, "seqno",
            bucket.durabilityMinLevel, bucket.replicaNumber,
            bucket.flushEnabled, bucket.maxTTL))

        if response.status_code == 201:
            self.remote_cluster.buckets.append(bucket)
            return response.json()["id"], bucket.name
        else:
            self.fail("Error creating bucket on remote cluster")

    def create_capella_scope(self, bucket_id, scope_name=None):
        if not scope_name:
            scope_name = self.cbas_util.generate_name()

        resp = self.capellaAPI.cluster_ops_apis.create_scope(self.tenant.id, self.tenant.project_id,
                                                             self.remote_cluster.id,
                                                             bucket_id, scope_name)

        if resp.status_code == 201:
            self.log.info("Created scope {} on bucket {}".format(scope_name, bucket_id))
            self.bucket_util.create_scope_object(self.remote_cluster.buckets[0],
                                                 scope_spec={"name": scope_name})
            return scope_name
        else:
            self.fail("Scope creation failed")

    def create_capella_collection(self, bucket_id, scope_id, collection_name=None):
        if not collection_name:
            collection_name = self.cbas_util.generate_name()
        resp = self.capellaAPI.cluster_ops_apis.create_collection(
            self.tenant.id, self.tenant.project_id, clusterId=self.remote_cluster.id,
            bucketId=bucket_id, scopeName=scope_id, name=collection_name)

        if resp.status_code == 201:
            self.log.info("Create collection {} in scope {}".format(collection_name, scope_id))
            self.bucket_util.create_collection_object(self.remote_cluster.buckets[0], self.scope_name,
                                                      collection_spec={"name": collection_name})
            return collection_name

    def delete_capella_bucket(self, bucket_id):
        resp = self.capellaAPI.cluster_ops_apis.delete_bucket(self.tenant.id, self.tenant.project_id,
                                                              self.remote_cluster.id, bucket_id)
        if resp.status_code == 204:
            self.log.info("Bucket deleted successfully")
        else:
            self.log.error("Bucket deletion failed")

    def load_doc_to_remote_collections(self, bucket, scope, collection, start, end):
        database_information = CouchbaseLoader(username= self.remote_cluster.username,
                                               password=self.remote_cluster.password,
                                               connection_string= "couchbases://" + self.remote_cluster.srv,
                                               bucket=bucket, scope=scope,
                                               collection=collection)
        operation_config = WorkloadOperationConfig(start=start, end=end,
                                                   template="hotel", doc_size=self.doc_size)
        task_insert = WorkLoadTask(bucket=bucket, task_manager=self.task,
                                   op_type=SiriusCodes.DocOps.CREATE,
                                   database_information=database_information,
                                   operation_config=operation_config)
        self.task_manager.add_new_task(task_insert)
        self.task_manager.get_task_result(task_insert)
        return task_insert

    def test_create_connect_disconnect_query_drop_remote_links_and_datasets(self):
        self.base_infra_setup()

        self.load_doc_to_remote_collections(self.bucket_name, self.scope_name,
                                            self.collection_name, 0, self.initial_doc_count)

        remote_links = self.cbas_util.get_all_link_objs("couchbase")
        remote_datasets = self.cbas_util.get_all_dataset_objs("remote")

        for link in remote_links:
            if not self.cbas_util.connect_link(self.cluster, link.full_name):
                self.fail("Failed to connect link")

        jobs = Queue()
        results = []
        for dataset in remote_datasets:
            jobs.put((
                self.cbas_util.get_num_items_in_cbas_dataset,
                {"cluster": self.cluster, "dataset_name": dataset.full_name,
                 "timeout": 3600, "analytics_timeout": 3600}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        for result in results:
            if result != self.initial_doc_count:
                self.fail("Doc count mismatch. Expected - {0}, Actual - {"
                          "1}".format(self.initial_doc_count, result))

        for link in remote_links:
            if not self.cbas_util.disconnect_link(self.cluster, link.full_name):
                self.fail("Failed to disconnect link")

    def test_alter_remote_links(self):
        self.base_infra_setup()

        remote_links = self.cbas_util.get_all_link_objs("couchbase")
        remote_datasets = self.cbas_util.get_all_dataset_objs("remote")

        for link in remote_links:
            if not self.cbas_util.connect_link(self.cluster, link.full_name):
                self.fail("Failed to connect link")

        for link in remote_links:
            if not self.cbas_util.disconnect_link(self.cluster, link.full_name):
                self.fail("Failed to disconnect link")

        remote_link_properties = self.columnar_spec["remote_link"]["properties"]
        remote_link_properties[0]["encryption"] = "half"
        self.cbas_util.alter_link_properties(self.cluster, remote_link_properties[0])

        self.load_doc_to_remote_collections(self.bucket_name, self.scope_name,
                                            self.collection_name, 0, self.initial_doc_count)

        for link in remote_links:
            if not self.cbas_util.connect_link(self.cluster, link.full_name):
                self.fail("Failed to connect link")

        jobs = Queue()
        results = []
        for dataset in remote_datasets:
            jobs.put((
                self.cbas_util.wait_for_ingestion_complete,
                {"cluster": self.cluster, "dataset_name": dataset.full_name,
                 "num_items": self.initial_doc_count, "timeout": 3600}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.fail("Data ingestion did not complete for all datasets")

        for link in remote_links:
            if not self.cbas_util.disconnect_link(self.cluster, link.full_name):
                self.fail("Failed to disconnect link")

    def test_remote_links_operations_during_remote_cluster_scaling_operation(self):
        self.base_infra_setup()

        self.load_doc_to_remote_collections(self.bucket_name, self.scope_name,
                                            self.collection_name, 0, self.initial_doc_count)

        remote_links = self.cbas_util.get_all_link_objs("couchbase")
        remote_datasets = self.cbas_util.get_all_dataset_objs("remote")

        for link in remote_links:
            if not self.cbas_util.connect_link(self.cluster, link.full_name):
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
                {"cluster": self.cluster, "dataset_name": dataset.full_name,
                 "timeout": 3600, "analytics_timeout": 3600}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        for result in results:
            if result != self.initial_doc_count:
                self.fail("Doc count mismatch. Expected - {0}, Actual - {"
                          "1}".format(self.initial_doc_count, result))

        for link in remote_links:
            if not self.cbas_util.disconnect_link(self.cluster, link.full_name):
                self.fail("Failed to disconnect link")

    def test_remote_link_operations_before_and_after_pause_resume_of_remote_cluster(self):
        self.base_infra_setup()

        self.load_doc_to_remote_collections(self.bucket_name, self.scope_name,
                                            self.collection_name, 0, self.initial_doc_count)

        remote_links = self.cbas_util.get_all_link_objs("couchbase")
        remote_datasets = self.cbas_util.get_all_dataset_objs("remote")

        for link in remote_links:
            if not self.cbas_util.connect_link(self.cluster, link.full_name):
                self.fail("Failed to connect link")

        jobs = Queue()
        results = []
        for dataset in remote_datasets:
            jobs.put((
                self.cbas_util.get_num_items_in_cbas_dataset,
                {"cluster": self.cluster, "dataset_name": dataset.full_name,
                 "timeout": 3600, "analytics_timeout": 3600}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        for result in results:
            if result != self.initial_doc_count:
                self.fail("Doc count mismatch. Expected - {0}, Actual - {"
                          "1}".format(self.initial_doc_count, result))

        cluster_on_off = DoctorHostedOnOff(self.pod, self.tenant, self.remote_cluster)
        cluster_off_result = cluster_on_off.turn_off_cluster(timeout=1200)
        self.assertTrue(cluster_off_result, "Failed to turn off cluster")
        self.sleep(60, "Wait before turning cluster on")
        cluster_on_result = cluster_on_off.turn_on_cluster(timeout=1200)
        self.assertTrue(cluster_on_result, "Failed to turn on cluster")
        self.sleep(60, "Wait after cluster is turned on")

        self.load_doc_to_remote_collections(self.bucket_name, self.scope_name,
                                            self.collection_name, self.initial_doc_count,
                                            self.initial_doc_count * 2)

        results = []
        for dataset in remote_datasets:
            jobs.put((
                self.cbas_util.get_num_items_in_cbas_dataset,
                {"cluster": self.cluster, "dataset_name": dataset.full_name,
                 "timeout": 3600, "analytics_timeout": 3600}))
        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        for result in results:
            if result != self.initial_doc_count * 2:
                self.fail("Doc count mismatch. Expected - {0}, Actual - {"
                          "1}".format(self.initial_doc_count, result))

        for link in remote_links:
            if not self.cbas_util.disconnect_link(self.cluster, link.full_name):
                self.fail("Failed to disconnect link")
