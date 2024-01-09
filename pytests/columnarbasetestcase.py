from Cb_constants import CbServer
from TestInput import TestInputSingleton, TestInputServer
from bucket_utils.bucket_ready_functions import BucketUtils
from capella_utils.common_utils import Pod, Tenant
from cb_basetest import CouchbaseBaseTest
from cluster_utils.cluster_ready_functions import ClusterUtils, CBCluster,\
    Nebula, GoldfishNebula
from security_config import trust_all_certs

import global_vars
from capella_utils.columnar import GoldfishUtils
from capella_utils.dedicated import CapellaUtils
from connections.Rest_Connection import RestConnection
import copy


class ColumnarBaseTest(CouchbaseBaseTest):
    def setUp(self):
        super(ColumnarBaseTest, self).setUp()

        # Cluster level info settings
        self.capella = self.input.capella

        self.wait_timeout = self.input.param("wait_timeout", 1800)
        CbServer.use_https = False
        trust_all_certs()

        # initialize pod object
        url = self.capella.get("pod")
        self.pod = Pod(
            "https://%s" % url, self.capella.get("token", None))

        self.log_setup_status(self.__class__.__name__, "started")

        # Create control plane users
        self.tenant = Tenant(self.input.capella.get("tenant_id"),
                             self.input.capella.get("capella_user"),
                             self.input.capella.get("capella_pwd"),
                             self.input.capella.get("secret_key"),
                             self.input.capella.get("access_key"))

        if not (self.input.capella.get("access_key") and\
                self.input.capella.get("secret_key")):
            self.log.info("Creating API keys for tenant...")
            resp = CapellaUtils.create_access_secret_key(self.pod, self.tenant, "key")
            self.tenant.api_secret_key = resp["secret"]
            self.tenant.api_access_key = resp["access"]

        self.tenant.project_id = \
            TestInputSingleton.input.capella.get("project", None)
        if not self.tenant.project_id:
            self.log.info("Creating project for tenant...")
            CapellaUtils.create_project(self.pod, self.tenant, "a_taf_run")

        self.goldfish_utils = GoldfishUtils(self.log)
        self.cluster = CBCluster(username=None,
                                 password=None,
                                 servers=[None])
        self.cluster.type = "columnar"
        cluster_ids = TestInputSingleton.input.capella \
            .get("clusters", "")
        if cluster_ids:
            cluster_ids = cluster_ids.split(",")
            self.cluster.cluster_id = cluster_ids[0]
        else:
            self.log.info("Deploying columnar instance for tenant...")
            cluster_config = self.goldfish_utils.generate_cluster_configuration(nodes=1)
            self.cluster.cluster_id = self.goldfish_utils.create_cluster(
                self.pod, self.tenant, cluster_config)
            self.log.info("Columnar instance deployment started: %s" % self.cluster.cluster_id)
            self.sleep(30, "Sleep after initializing deployment: %s" % self.cluster.cluster_id)
            self.log.info("Checking columnar instance deployment status...")
            resp = self.goldfish_utils.wait_for_cluster_deploy(
                self.pod, self.tenant, self.cluster)
            self.assertTrue(resp, "cluster deployment failed")
        resp = self.goldfish_utils.get_cluster_info(self.pod, self.tenant,
                                                    self.cluster)

        server = TestInputServer()
        server.ip = resp["config"]["endpoint"]

        creds = self.goldfish_utils.create_db_user_api_keys(self.pod, self.tenant, self.cluster)
        server.rest_username = creds["apikeyId"]
        server.rest_password = creds["secret"]
        server.port = "18001"
        server.memcached_port = CbServer.ssl_memcached_port
        server.cbas_port = "18001"
        server.type = "columnar"
        self.log.critical("Access/Secret for cluster {} is {}:{}".format(
            self.cluster.cluster_id, server.rest_username, server.rest_password))

        self.cluster.nebula = GoldfishNebula(resp["config"]["endpoint"], server)
        self.cluster_util = ClusterUtils(self.task_manager)
        self.bucket_util = BucketUtils(self.cluster_util, self.task)

        # Setting global_vars for future reference
        global_vars.cluster_util = self.cluster_util
        global_vars.bucket_util = self.bucket_util

        server = copy.deepcopy(server)
        server.ip = resp["config"]["endpoint"].replace("r-stmgmpftyc", "r-rvfzrfywdw")
        server.port = 8091
        nodes = self.cluster_util.get_nodes(server)
        self.cluster.refresh_object(nodes)

    def tearDown(self):
        # Deleting the project created in setUp -> "a_taf_run"
        project_in_ini = TestInputSingleton.input.capella.get("project", None)
        if not project_in_ini:
            CapellaUtils.delete_project(self.pod, self.tenant)

        self.shutdown_task_manager()
        if self.sdk_client_pool:
            self.sdk_client_pool.shutdown()

        if self.is_test_failed() and self.get_cbcollect_info:
            # Add code to get goldfish logs.
            return

        if self.skip_teardown_cleanup:
            return

        def delete_cluster(user, cluster, result):
            if not self.goldfish_utils.delete_goldfish_cluster(
                    self.pod, user, cluster):
                result.append("Deleting cluster {0} failed".format(cluster.name))

        def wait_for_cluster_deletion(user, cluster, results):
            result = self.goldfish_utils.wait_for_cluster_to_be_destroyed(
                self.pod, user, cluster)
            if not result:
                results.append("Cluster {0} failed to be deleted".format(
                    cluster.name))


class ClusterSetup(ColumnarBaseTest):
    def setUp(self):
        super(ClusterSetup, self).setUp()

        self.log_setup_status("ClusterSetup", "started", "setup")

        # Print cluster stats
        self.cluster_util.print_cluster_stats(self.cluster)
        self.log_setup_status("ClusterSetup", "complete", "setup")

    def tearDown(self):
        super(ClusterSetup, self).tearDown()
