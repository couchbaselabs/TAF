from cb_constants import CbServer
from TestInput import TestInputSingleton, TestInputServer
from bucket_utils.bucket_ready_functions import BucketUtils
from capella_utils.common_utils import Pod, Tenant
from cluster_utils.cluster_ready_functions import ClusterUtils, CBCluster, GoldfishNebula
from security_config import trust_all_certs

import global_vars
from capella_utils.columnar import GoldfishUtils
from capella_utils.dedicated import CapellaUtils
import copy
from pytests.dedicatedbasetestcase import CapellaBaseTest


class ColumnarBaseTest(CapellaBaseTest):
    def setUp(self):
        super(ColumnarBaseTest, self).setUp()

        self.goldfish_utils = GoldfishUtils(self.log)
        cluster_ids = TestInputSingleton.input.capella \
            .get("clusters", "")
        if cluster_ids:
            cluster_ids = cluster_ids.split(",")
            for cluster_id in cluster_ids:
                cluster = CBCluster(username=None,
                                    password=None,
                                    servers=[None])
                cluster.id = cluster_id
                self.tenants[0].cluster.append(cluster)
        else:
            for tenant in self.tenants:
                tenant.project_id = tenant.projects[0]
                for _ in range(self.num_clusters):
                    cluster = CBCluster(username=None,
                                        password=None,
                                        servers=[None])
                    self.log.info("Deploying columnar instance for tenant...")
                    cluster_config = self.goldfish_utils.generate_cluster_configuration(nodes=1)
                    cluster.id = self.goldfish_utils.create_cluster(
                        self.pod, tenant, cluster_config)
                    self.log.info("Columnar instance deployment started: %s" % cluster.id)
                    tenant.clusters.append(cluster)
        self.sleep(30, "Sleep after initializing deployment: %s" % cluster.id)
        self.cluster_util = ClusterUtils(self.task_manager)
        self.bucket_util = BucketUtils(self.cluster_util, self.task)
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                cluster.type = "columnar"
                self.log.info("Checking columnar instance deployment status...")
                resp = self.goldfish_utils.wait_for_cluster_deploy(
                    self.pod, tenant, cluster)
                self.assertTrue(resp, "cluster deployment failed")
                resp = self.goldfish_utils.get_cluster_info(self.pod, tenant,
                                                            cluster)
        
                server = TestInputServer()
                server.ip = resp["config"]["endpoint"]
        
                creds = self.goldfish_utils.create_db_user_api_keys(self.pod, tenant, cluster)
                server.rest_username = creds["apikeyId"]
                server.rest_password = creds["secret"]
                server.port = "18001"
                server.memcached_port = CbServer.ssl_memcached_port
                server.cbas_port = "18001"
                server.type = "columnar"
                self.log.critical("Access/Secret for cluster {} is {}:{}".format(
                    cluster.id, server.rest_username, server.rest_password))
        
                cluster.nebula = GoldfishNebula(resp["config"]["endpoint"], server)
                server = copy.deepcopy(server)
                server.ip = resp["config"]["endpoint"].replace("r-stmgmpftyc", "r-rvfzrfywdw")
                server.port = 8091
                nodes = self.cluster_util.get_nodes(server)
                cluster.refresh_object(nodes)


        # Setting global_vars for future reference
        global_vars.cluster_util = self.cluster_util
        global_vars.bucket_util = self.bucket_util

    def tearDown(self):
        if self.skip_teardown_cleanup:
            return

        for tenant in self.tenants:
            for cluster in tenant.clusters:
                self.delete_cluster(tenant, cluster)
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                self.wait_for_cluster_deletion(tenant, cluster)
        project_in_ini = TestInputSingleton.input.capella.get("project", None)
        if not project_in_ini:
            for tenant in self.tenants:
                for project_id in tenant.projects:
                    CapellaUtils.delete_project(self.pod, tenant, project_id)

        self.shutdown_task_manager()
        if self.cluster.sdk_client_pool:
            self.cluster.sdk_client_pool.shutdown()

        if self.is_test_failed() and self.get_cbcollect_info:
            # Add code to get goldfish logs.
            pass

    def delete_cluster(self, tenant, cluster):
        result = self.goldfish_utils.delete_cluster(
            self.pod, tenant, cluster)
        self.assertTrue(result, "Deleting cluster {0} failed".format(cluster.name))

    def wait_for_cluster_deletion(self, tenant, cluster):
        result = self.goldfish_utils.wait_for_cluster_destroy(
            self.pod, tenant, cluster)
        self.assertTrue(result, "Cluster {0} failed to be deleted".format(cluster.name))


class ClusterSetup(ColumnarBaseTest):
    def setUp(self):
        super(ClusterSetup, self).setUp()

        self.log_setup_status("ClusterSetup", "started", "setup")

        # Print cluster stats
        for tenant in self.tenants:
            for cluster in tenant.clusters:
                self.cluster_util.print_cluster_stats(cluster)
        self.log_setup_status("ClusterSetup", "complete", "setup")

    def tearDown(self):
        super(ClusterSetup, self).tearDown()
