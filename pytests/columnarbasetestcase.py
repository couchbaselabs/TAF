
from TestInput import TestInputSingleton, TestInputServer
from bucket_utils.bucket_ready_functions import BucketUtils
from cluster_utils.cluster_ready_functions import ClusterUtils, CBCluster

import global_vars
from capella_utils.columnar import GoldfishUtils
from capella_utils.dedicated import CapellaUtils
from pytests.dedicatedbasetestcase import CapellaBaseTest
import threading
from Jython_tasks.task import DeployColumnarInstance


class ColumnarBaseTest(CapellaBaseTest):
    def setUp(self):
        super(ColumnarBaseTest, self).setUp()
        self.rest_username = \
            TestInputSingleton.input.membase_settings.rest_username
        self.rest_password = \
            TestInputSingleton.input.membase_settings.rest_password

        self.goldfish_utils = GoldfishUtils(self.log)
        cluster_ids = TestInputSingleton.input.capella \
            .get("clusters", "")
        if cluster_ids:
            cluster_ids = cluster_ids.split(",")
            self.__get_existing_instance_details(self.tenants, cluster_ids)
        else:
            tasks = list()
            for tenant in self.tenants:
                tenant.project_id = tenant.projects[0]
                for _ in range(self.num_clusters):
                    self.log.info("Deploying columnar instance for tenant...")
                    self.cluster_config = self.goldfish_utils.generate_cluster_configuration(nodes=1)
                    deploy_task = DeployColumnarInstance(self.pod, tenant, self.cluster_config["name"],
                                                         self.cluster_config,
                                                         timeout=self.wait_timeout)
                    self.task_manager.add_new_task(deploy_task)
                    tasks.append(deploy_task)
            for task in tasks:
                self.task_manager.get_task_result(task)
                self.assertTrue(task.result, "Cluster deployment failed!")
                CapellaUtils.create_db_user(
                        self.pod, task.tenant, task.cluster_id,
                        self.rest_username, self.rest_password)
                self.__populate_cluster_info(task.tenant, task.instance_id, task.cluster_id, task.servers,
                                             task.srv, task.name,
                                             self.cluster_config)
        self.cluster_util = ClusterUtils(self.task_manager)
        self.bucket_util = BucketUtils(self.cluster_util, self.task)

        # Setting global_vars for future reference
        global_vars.cluster_util = self.cluster_util
        global_vars.bucket_util = self.bucket_util

    def __populate_cluster_info(self, tenant, instance_id, cluster_id, servers, srv, name, config):
        nodes = list()
        for server in servers:
            temp_server = TestInputServer()
            temp_server.ip = server.get("hostname")
            temp_server.hostname = server.get("hostname")
            temp_server.services = server.get("services")
            temp_server.port = "18091"
            temp_server.cbas_port = 18095
            temp_server.rest_username = self.rest_username
            temp_server.rest_password = self.rest_password
            temp_server.type = "columnar"
            temp_server.memcached_port = "11207"
            nodes.append(temp_server)
        cluster = CBCluster(username=self.rest_username,
                            password=self.rest_password,
                            servers=[None] * 40)
        cluster.name = name
        cluster.id = instance_id
        cluster.cluster_id = cluster_id
        cluster.srv = srv
        cluster.cluster_config = config
        cluster.pod = self.pod
        cluster.type = "columnar"

        for temp_server in nodes:
            cluster.nodes_in_cluster.append(temp_server)
            if "Data" in temp_server.services:
                cluster.kv_nodes.append(temp_server)
            if "Query" in temp_server.services:
                cluster.query_nodes.append(temp_server)
            if "Index" in temp_server.services:
                cluster.index_nodes.append(temp_server)
            if "Eventing" in temp_server.services:
                cluster.eventing_nodes.append(temp_server)
            if "Analytics" in temp_server.services:
                cluster.cbas_nodes.append(temp_server)
            if "Search" in temp_server.services:
                cluster.fts_nodes.append(temp_server)

        cluster.master = cluster.kv_nodes[0]
        tenant.columnar_instances.append(cluster)
        self.cb_clusters[name] = cluster
        self.log.info("Cluster deployed! InstanceID:{} , ClusterID:{}".format(
            instance_id, cluster_id))

    def __get_existing_instance_details(self, tenants, instance_ids):
        for i, instance_id in enumerate(instance_ids):
            self.log.info("Fetching columnar instance details for: %s" % instance_id)
            resp = GoldfishUtils.get_cluster_info(self.pod, tenants[i], instance_id)
            cluster_srv = resp["data"]["config"]["endpoint"]
            cluster_id = resp["data"]["config"]["clusterId"]
            cluster_name = resp["data"]["name"]
            creds = self.goldfish_utils.create_db_user_api_keys(self.pod, tenants[i], instance_id)
            self.log.critical("Access/Secret for cluster {} is {}:{}".format(
                instance_id, creds["apikeyId"], creds["secret"]))
            CapellaUtils.create_db_user(
                    self.pod, tenants[i], cluster_id,
                    self.rest_username, self.rest_password)
            servers = CapellaUtils.get_nodes(self.pod, tenants[i], cluster_id)
            self.__populate_cluster_info(tenants[i], instance_id, cluster_id, servers, cluster_srv,
                                         cluster_name, None)

    def tearDown(self):
        if self.skip_teardown_cleanup:
            return

        self.shutdown_task_manager()
        for tenant in self.tenants:
            for cluster in tenant.clusters + tenant.columnar_instances:
                if cluster.sdk_client_pool:
                    cluster.sdk_client_pool.shutdown()

        if self.is_test_failed() and self.get_cbcollect_info:
            # Add code to get goldfish logs.
            pass

        if not TestInputSingleton.input.capella.get("clusters", None):
            th = list()
            for tenant in self.tenants:
                for cluster in tenant.columnar_instances:
                    self.log.info("Deleting columnar cluster: {}".format(cluster.id))
                    delete_th = threading.Thread(
                        target=self.delete_cluster, name=cluster.id,
                        args=(tenant, cluster))
                    delete_th.start()
                    th.append(delete_th)
            for delete_th in th:
                delete_th.join()

        project_in_ini = TestInputSingleton.input.capella.get("project", None)
        if not project_in_ini:
            for tenant in self.tenants:
                for project_id in tenant.projects:
                    CapellaUtils.delete_project(self.pod, tenant, project_id)

    def delete_cluster(self, tenant, cluster):
        result = self.goldfish_utils.delete_cluster(
            self.pod, tenant, cluster)
        self.assertTrue(result, "Deleting cluster {0} failed".format(cluster.name))
        self.wait_for_cluster_deletion(tenant, cluster)

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
