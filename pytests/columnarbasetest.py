"""
Created on Oct 14, 2023

@author: umang.agrawal
"""
import global_vars
import random
import subprocess
import shlex
from threading import Thread

from Jython_tasks.task import DeployColumnarInstance
from TestInput import TestInputSingleton, TestInputServer
from bucket_utils.bucket_ready_functions import BucketUtils
from capella_utils.columnar_final import (ColumnarInstance, ColumnarUtils,
                                          ColumnarRBACUtil)
from capella_utils.dedicated import CapellaUtils
from cluster_utils.cluster_ready_functions import ClusterUtils
from pytests.dedicatedbasetestcase import ProvisionedBaseTestCase
from sdk_client3 import SDKClientPool


class ColumnarBaseTest(ProvisionedBaseTestCase):
    def setUp(self):
        super(ColumnarBaseTest, self).setUp()
        self.log_setup_status(self.__class__.__name__, "started")

        # Columnar instance configuration
        self.num_nodes_in_columnar_instance = self.input.param(
            "num_nodes_in_columnar_instance", 2)
        self.columnar_utils = ColumnarUtils(self.log)
        self.columnar_image = self.input.capella.get("columnar_image")
        self.columnar_rbac_util = ColumnarRBACUtil(self.log)

        # Utility objects
        self.columnar_utils = ColumnarUtils(self.log)

        def populate_columnar_instance_obj(
                tenant, instance_id, instance_name=None, instance_config=None):

            resp = self.columnar_utils.get_instance_info(
                pod=self.pod, tenant=tenant, project_id=tenant.project_id,
                instance_id=instance_id)

            srv = resp["data"]["config"]["endpoint"]
            cluster_id = resp["data"]["config"]["clusterId"]
            name = instance_name or str(resp["data"]["name"])
            cmd = "dig @8.8.8.8  _couchbases._tcp.{} srv".format(srv)
            proc = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE)
            out, _ = proc.communicate()
            servers = list()
            for line in out.split("\n"):
                if "11207" in line:
                    servers.append(line.split("11207")[-1].rstrip(".").lstrip(" "))

            instance_obj = ColumnarInstance(
                            tenant_id=tenant.id,
                            project_id=tenant.project_id,
                            instance_name=name,
                            instance_id=instance_id,
                            cluster_id=cluster_id,
                            instance_endpoint=srv,
                            db_users=list())

            instance_obj.username, instance_obj.password = \
                self.columnar_utils.create_couchbase_cloud_qe_user(
                    self.pod, tenant, instance_obj)

            for server in servers:
                temp_server = TestInputServer()
                temp_server.ip = server
                temp_server.hostname = server
                temp_server.services = None
                temp_server.port = "18091"
                temp_server.type = "columnar"
                temp_server.memcached_port = "11207"
                temp_server.rest_username = instance_obj.username
                temp_server.rest_password = instance_obj.password
                instance_obj.servers.append(temp_server)
            instance_obj.nodes_in_cluster = instance_obj.servers
            instance_obj.master = instance_obj.servers[0]
            instance_obj.cbas_cc_node = instance_obj.servers[0]
            instance_obj.instance_config = instance_config
            tenant.columnar_instances.append(instance_obj)

            self.log.info("Instance Ready! InstanceID:{} , ClusterID:{}"
                          .format(instance_id, instance_obj.cluster_id))

        def allow_access_from_everywhere_on_instance(
                tenant, project_id, instance_obj):
            response = self.columnar_utils.allow_ip_on_instance(
                self.pod, tenant, project_id, instance_obj)
            if not response:
                return False
            return True

        # Columnar clusters can be reused within a test suite, only when they
        # are deployed on single tenant under single project.
        # Set skip_redeploy to True if you want to reuse the columnar instance.
        self.skip_redeploy = self.input.param("skip_redeploy", True)

        tasks = list()

        # Single tenant, single project and multiple instances
        self.tenant = self.tenants[0]
        self.tenant.project_id = self.tenant.projects[0]
        if not self.capella.get("project", None):
            self.capella["project"] = self.tenant.project_id

        self.capella["clusters"] = ",".join([
            cluster.id for cluster in self.tenant.clusters])

        instance_ids = self.capella.get("instance_id", "")
        if instance_ids:
            instance_ids = instance_ids.split(',')

        self.instance_type = {
                "vcpus":"%svCPUs" % self.input.param("cpu", 4),
                "memory":"%sGB" % self.input.param("ram", 16)
            }

        for i in range(0, self.input.param("num_columnar_instances", 1)):
            if instance_ids and i < len(instance_ids):
                populate_columnar_instance_obj(self.tenant, instance_ids[i])
            else:
                instance_config = (
                    self.columnar_utils.generate_instance_configuration(
                        name=self.prefix + "Columnar_{0}".format(
                            random.randint(1, 100000)),
                        region=self.region,
                        nodes=self.num_nodes_in_columnar_instance,
                        instance_types={
                            "vcpus": self.instance_type[0],
                            "memory": self.instance_type[1]
                        },
                        token=self.pod.override_key))

                self.log.info("Deploying Columnar Instance {}".format(
                    instance_config["name"]))

                deploy_task = DeployColumnarInstance(
                    self.pod, self.tenant, instance_config["name"],
                    instance_config, timeout=self.wait_timeout)
                self.task_manager.add_new_task(deploy_task)
                tasks.append(deploy_task)

        for task in tasks:
            self.task_manager.get_task_result(task)
            self.assertTrue(task.result, "Cluster deployment failed!")
            populate_columnar_instance_obj(self.tenant, task.instance_id,
                                           task.name, instance_config)

        # Adding db user to each instance.
        for instance in self.tenant.columnar_instances:
            count = 0
            analytics_admin_user = None
            while not analytics_admin_user and count < 5:
                analytics_admin_user = self.columnar_rbac_util.create_custom_analytics_admin_user(
                    self.pod, self.tenant, self.tenant.project_id, instance,
                    self.rest_username, self.rest_password
                )
                count += 1

            self.cluster_util.print_cluster_stats(instance)

        if self.skip_redeploy:
            self.capella["instance_id"] = ",".join([
                instance.instance_id for instance in
                self.tenant.columnar_instances])

        self.cluster_util = ClusterUtils(self.task_manager)
        self.bucket_util = BucketUtils(self.cluster_util, self.task)

        # Setting global_vars for future reference
        global_vars.cluster_util = self.cluster_util
        global_vars.bucket_util = self.bucket_util

    def tearDown(self):
        self.shutdown_task_manager()

        for tenant in self.tenants:
            for instance in tenant.columnar_instances:
                if instance.sdk_client_pool:
                    instance.sdk_client_pool.shutdown()

            for cluster in tenant.clusters:
                if cluster.sdk_client_pool:
                    cluster.sdk_client_pool.shutdown()

        if self.is_test_failed() and self.get_cbcollect_info:
            # Add code to get columnar logs.
            return

        if self.skip_teardown_cleanup:
            return

        delete_instance_threads = list()
        wait_for_instance_delete_threads = list()

        delete_instance_results = list()
        wait_for_instance_delete_results = list()

        def delete_instance(tenant, project_id, instance, result):
            self.log.info(f"Attempt deleting instance: {instance.instance_id}")
            if not self.columnar_utils.delete_instance(
                    self.pod, tenant, project_id, instance):
                result.append(f"Deleting Columnar Instance - {instance.name}, "
                              f"Instance ID - {instance.instance_id} failed")

        def wait_for_instance_deletion(tenant, project_id, instance, results):
            result = self.columnar_utils.wait_for_instance_to_be_destroyed(
                self.pod, tenant, project_id, instance)
            if not result:
                results.append(f"Columnar Instance {instance.instance_id} "
                               "failed to be deleted")

        def delete_cloud_infra():
            for tenant in self.tenants:
                for instance in tenant.columnar_instances:
                    delete_instance_threads.append(Thread(
                        target=delete_instance,
                        name="delete_instance_thread",
                        args=(tenant, instance.project_id, instance,
                              delete_instance_results,)
                    ))
                    wait_for_instance_delete_threads.append(Thread(
                        target=wait_for_instance_deletion,
                        name="waiter_thread",
                        args=(tenant, instance.project_id, instance,
                              wait_for_instance_delete_results,)
                    ))

            self.start_threads(delete_instance_threads)
            if delete_instance_results:
                raise Exception("Following Columnar Instance deletion APIs "
                                f"failed - {delete_instance_results}")

            self.start_threads(wait_for_instance_delete_threads)
            if wait_for_instance_delete_results:
                raise Exception(
                    f"Failure while waiting for Columnar Instance "
                    f"to be deleted - {wait_for_instance_delete_results}")

            # So that the capella cluster get's destroyed in
            # ProvisionedBaseTestCase teardown
            self.capella["clusters"] = None

            # On Prod env don't delete project, as the project is used
            # across pipeline.
            if "cloud.couchbase.com" not in self.pod.url_public:
                self.capella["project"] = None
            super(ColumnarBaseTest, self).tearDown()

            for tenant in self.tenants:
                CapellaUtils.revoke_access_secret_key(
                    self.pod, tenant, tenant.api_key_id)

        if self.skip_redeploy:
            if (TestInputSingleton.input.test_params["case_number"] ==
                    TestInputSingleton.input.test_params["no_of_test_identified"]):
                delete_cloud_infra()
                # This is required to delete project in
                # dedicatedbasetestcase.py teardown method.
                self.skip_redeploy = False
            else:
                for tenant in self.tenants:
                    CapellaUtils.revoke_access_secret_key(
                        self.pod, tenant, tenant.api_key_id)
        else:
            self.capella["project"] = self.capella["instance_id"] = ""
            delete_cloud_infra()

    def start_threads(self, thread_list, async_run=False):
        for thread in thread_list:
            thread.start()
        if not async_run:
            for thread in thread_list:
                thread.join()

    @staticmethod
    def init_sdk_pool_object(instance, num_clients=1,
                             username="Administrator", password="password"):
        """
        Overriding the method from CouchbaseBaseTest class
        :return:
        """
        instance.sdk_client_pool = SDKClientPool()
        instance.sdk_client_pool.create_cluster_clients(
            cluster=instance, servers=[instance.master],
            req_clients=num_clients, username=username, password=password)


class ClusterSetup(ColumnarBaseTest):
    def setUp(self):
        self.log_setup_status("ClusterSetup", "started", "setup")
        super(ClusterSetup, self).setUp()

        self.log_setup_status("ClusterSetup", "complete", "setup")

    def tearDown(self):
        super(ClusterSetup, self).tearDown()
