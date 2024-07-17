"""
Created on 17-Oct-2023
@author: Umang Agrawal
"""

from basetestcase import BaseTestCase
from TestInput import TestInputSingleton
from cbas_utils.cbas_utils import CbasUtil
from BucketLib.bucket import Bucket
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI
from BucketLib.BucketOperations import BucketHelper
import json


class ColumnarBaseTest(BaseTestCase):

    def setUp(self):
        """
        Since BaseTestCase will initialize at least one cluster, we pass service
        for the master node of that cluster
        """
        if not hasattr(self, "input"):
            self.input = TestInputSingleton.input

        super(ColumnarBaseTest, self).setUp()

        if self._testMethodDoc:
            self.log.info("Starting Test: %s - %s"
                          % (self._testMethodName, self._testMethodDoc))
        else:
            self.log.info("Starting Test: %s" % self._testMethodName)

        self.use_sdk_for_cbas = self.input.param("use_sdk_for_cbas", False)
        self.sdk_clients_per_user = self.input.param("sdk_clients_per_user", 1)

        if self.use_sdk_for_cbas:
            for instance in self.tenant.columnar_instances:
                self.init_sdk_pool_object(
                    instance, self.sdk_clients_per_user,
                    instance.master.rest_username,
                    instance.master.rest_password)

        # This is to support static remote clusters. Multiple remote cluster
        # IPs can be passed in format ip1:ip2
        """remote_cluster_ips = self.input.param("remote_cluster_ips", None)
        if remote_cluster_ips:
            remote_cluster_ips = remote_cluster_ips.split("|")
            self.remote_clusters = list()
            for remote_ip in remote_cluster_ips:
                remote_server = copy.deepcopy(self.servers[0])
                remote_server.ip = remote_cluster_ips[i - 1]
                cluster = CBCluster(
                    name=cluster_name, servers=[remote_server])"""

        # Common properties
        self.num_concurrent_queries = self.input.param('num_queries', 5000)
        self.concurrent_batch_size = self.input.param('concurrent_batch_size',
                                                      100)
        self.retry_time = self.input.param("retry_time", 300)
        self.num_retries = self.input.param("num_retries", 1)

        self.columnar_spec_name = self.input.param("columnar_spec_name", None)

        self.cbas_util = CbasUtil(self.task, self.use_sdk_for_cbas)

        self.perform_gf_instance_cleanup = self.input.param(
            "perform_gf_instance_cleanup", True)

        # AWS credentials and other info
        self.aws_access_key = self.input.param("aws_access_key", "")
        self.aws_secret_key = self.input.param("aws_secret_key", "")
        self.aws_session_token = self.input.param("aws_session_token", "")
        self.aws_region = self.input.param("aws_region", "us-west-1")

        # For sanity tests we are hard coding the bucket from which the data
        # will be read. This will ensure stable and consistent test runs.
        # Override this variable in your test setup if you want to use a
        # different bucket
        self.s3_source_bucket = self.input.param(
            "s3_source_bucket", "columnar-functional-sanity-test-data")

        # Initialising capella V4 API object, which can used to make capella
        # V4 API calls.
        self.capellaAPI = CapellaAPI(
            self.pod.url_public, '', '', self.tenant.user,
            self.tenant.pwd, '')
        response = self.capellaAPI.create_control_plane_api_key(
            self.tenant.id, 'init api keys')
        if response.status_code == 201:
            response = response.json()
        else:
            self.log.error("Error while creating V2 control plane API key")
            self.fail("{}".format(response.content))
        self.capellaAPI.cluster_ops_apis.SECRET = response['secretKey']
        self.capellaAPI.cluster_ops_apis.ACCESS = response['id']
        self.capellaAPI.cluster_ops_apis.bearer_token = response['token']
        self.capellaAPI.org_ops_apis.SECRET = response['secretKey']
        self.capellaAPI.org_ops_apis.ACCESS = response['id']
        self.capellaAPI.org_ops_apis.bearer_token = response['token']

        # create the first V4 API KEY WITH organizationOwner role, which will
        # be used to perform further V4 api operations
        resp = self.capellaAPI.org_ops_apis.create_api_key(
            organizationId=self.tenant.id,
            name=self.cbas_util.generate_name(),
            organizationRoles=["organizationOwner"],
            description=self.cbas_util.generate_name())
        if resp.status_code == 201:
            org_owner_key = resp.json()
        else:
            self.log.error("Error while creating V4 API key for organization "
                           "owner")

        self.capellaAPI.org_ops_apis.bearer_token = \
            self.capellaAPI.cluster_ops_apis.bearer_token = \
            org_owner_key["token"]

        for instance in self.tenant.columnar_instances:
            if not self.cbas_util.wait_for_cbas_to_recover(instance):
                self.fail("Analytics service failed to start")

        self.log.info("=== CBAS_BASE setup was finished for test #{0} {1} ==="
                      .format(self.case_number, self._testMethodName))

    def tearDown(self):
        if self.perform_gf_instance_cleanup:
            for instance in self.tenant.columnar_instances:
                self.cbas_util.cleanup_cbas(instance)

        super(ColumnarBaseTest, self).tearDown()

    def create_bucket_scopes_collections_in_capella_cluster(
            self, tenant, cluster, num_buckets=1, bucket_ram_quota=1024,
            num_scopes_per_bucket=1, num_collections_per_scope=1):
        for i in range(0, num_buckets):
            bucket = Bucket(
                {Bucket.name: self.cbas_util.generate_name(),
                 Bucket.ramQuotaMB: bucket_ram_quota,
                 Bucket.maxTTL: self.bucket_ttl,
                 Bucket.replicaNumber: self.num_replicas,
                 Bucket.storageBackend: self.bucket_storage,
                 Bucket.evictionPolicy: self.bucket_eviction_policy,
                 Bucket.durabilityMinLevel: self.bucket_durability_level,
                 Bucket.flushEnabled: True})
            response = self.capellaAPI.cluster_ops_apis.create_bucket(
                tenant.id, tenant.project_id, cluster.id, bucket.name,
                "couchbase", bucket.storageBackend, bucket.ramQuotaMB, "seqno",
                bucket.durabilityMinLevel, bucket.replicaNumber,
                bucket.flushEnabled, bucket.maxTTL)
            if response.status_code == 201:
                self.log.info("Created bucket {}".format(bucket.name))
                bucket.uuid = response.json()["id"]
                cluster.buckets.append(bucket)
            else:
                self.fail("Error creating bucket {0} on cluster {1}".format(
                    bucket.name, cluster.name))

            # since default scope is already present in the bucket.
            for j in range(1, num_scopes_per_bucket):
                scope_name = self.cbas_util.generate_name()
                resp = self.capellaAPI.cluster_ops_apis.create_scope(
                    tenant.id, tenant.project_id, cluster.id,
                    bucket.uuid, scope_name)
                if resp.status_code == 201:
                    self.log.info("Created scope {} on bucket {}".format(
                        scope_name, bucket.name))
                    self.bucket_util.create_scope_object(
                        bucket, scope_spec={"name": scope_name})
                else:
                    self.fail("Failed while creating scope {} on bucket {"
                              "}".format(scope_name, bucket.name))

            for scope_name, scope in bucket.scopes.items():
                if scope_name != "_system":
                    collections_to_create = num_collections_per_scope
                    if "_default" in scope.collections:
                        collections_to_create -= 1
                    for k in range(0, collections_to_create):
                        collection_name = self.cbas_util.generate_name()
                        resp = self.capellaAPI.cluster_ops_apis.create_collection(
                            tenant.id, tenant.project_id, clusterId=cluster.id,
                            bucketId=bucket.uuid, scopeName=scope_name,
                            name=collection_name)

                        if resp.status_code == 201:
                            self.log.info(
                                "Create collection {} in scope {}".format(
                                    collection_name, scope_name))
                            self.bucket_util.create_collection_object(
                                bucket, scope_name,
                                collection_spec={"name": collection_name})
                        else:
                            self.fail(
                                "Failed creating collection {} in scope {}".format(
                                    collection_name, scope_name))

    def delete_all_buckets_from_capella_cluster(self, tenant, cluster):
        resp = self.capellaAPI.cluster_ops_apis.list_buckets(
            tenant.id, tenant.project_id, cluster.id)
        if resp.status_code == 200:
            failed_to_delete_buckets = []
            data = resp.json()["data"]
            for bucket in data:
                resp = self.capellaAPI.cluster_ops_apis.delete_bucket(
                    tenant.id, tenant.project_id, cluster.id, bucket["id"])
                if resp.status_code == 204:
                    self.log.info("Bucket {0} deleted successfully".format(
                        bucket["name"]))
                else:
                    self.log.error(
                        "Bucket {0} deletion failed".format(bucket["name"]))
                    failed_to_delete_buckets.append(bucket["name"])
            if failed_to_delete_buckets:
                self.fail("Following buckets were not deleted {0}".format(
                    failed_to_delete_buckets))
        else:
            self.fail("Error while fetching bucket list for cluster {"
                      "0}".format(cluster.id))

    def generate_bucket_object_for_existing_buckets(self, tenant, cluster):
        for bucket in cluster.buckets:
            resp = self.capellaAPI.cluster_ops_apis.list_scopes(
                tenant.id, tenant.project_id, cluster.id, bucket.uuid)
            if resp.status_code == 200:
                scope_data = resp.json()["scopes"]
                for scope_info in scope_data:
                    if scope_info["name"] not in ["_default", "_system"]:
                        self.bucket_util.create_scope_object(
                            bucket, scope_spec={"name": scope_info["name"]})

                    for collection_info in scope_info["collections"]:
                        if collection_info["name"] != "_default":
                            self.bucket_util.create_collection_object(
                                bucket, scope_info["name"],
                                collection_spec={
                                    "name": collection_info["name"],
                                    "maxTTL": collection_info["maxTTL"]})
            else:
                self.fail(
                    f"Error while fetching scope list for bucket {bucket.name}")

        bucket_helper_obj = BucketHelper(cluster.master)
        stats_api = bucket_helper_obj.base_url + "/pools/default/stats/range"
        def get_stats_param(bucket_name, scope_name, collection_id):
            stats_param = [
                {"applyFunctions": ["sum"],
                 "metric":[
                     {"label": "name", "value": "kv_collection_item_count"},
                     {"label": "bucket", "value": bucket_name},
                     {"label": "scope", "value": scope_name},
                     {"label": "collection_id",
                      "value": hex(int(collection_id, 16))}
                 ],
                 "nodesAggregation": "sum",
                 "start": -3,
                 "step": 3,
                 "timeWindow": 360}]
            return json.dumps(stats_param)

        for bucket in cluster.buckets:
            for scope_name, scope in bucket.scopes.items():
                if scope_name != "_system":
                    for collection_name, collection in (
                            scope.collections.items()):
                        coll_id = bucket_helper_obj.get_collection_id(
                            bucket, scope_name, collection_name)
                        param = get_stats_param(bucket.name, scope_name, coll_id)
                        status, content, _ = bucket_helper_obj.request(
                            stats_api, "POST", params=param)
                        if status:
                            collection.num_items = int(content[0]["data"][0][
                                "values"][-1][1])
