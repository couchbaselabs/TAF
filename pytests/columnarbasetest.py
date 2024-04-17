"""
Created on Oct 14, 2023

@author: umang.agrawal
"""
import time

from bucket_utils.bucket_ready_functions import BucketUtils
from cluster_utils.cluster_ready_functions import ClusterUtils
from pytests.dedicatedbasetestcase import ProvisionedBaseTestCase

import global_vars
from capella_utils.columnar_final import DBUser, ColumnarInstance, ColumnarUtils
from threading import Thread
from sdk_client3 import SDKClientPool
from TestInput import TestInputSingleton, TestInputServer

from capella_utils.dedicated import CapellaUtils


class ColumnarBaseTest(ProvisionedBaseTestCase):

    def setUp(self):
        super(ColumnarBaseTest, self).setUp()

        self.log_setup_status(self.__class__.__name__, "started")

        self.num_nodes_in_columnar_instance = self.input.param(
            "num_nodes_in_columnar_instance", 0)

        self.columnar_utils = ColumnarUtils(self.log)

        def create_columnar_instance(tenant, project_id, result):
            instance_obj = ColumnarInstance(
                tenant_id=tenant.id, project_id=project_id,
                instance_name=None, instance_id=None, instance_endpoint=None,
                db_users=list())
            instance_config = (
                self.columnar_utils.generate_instance_configuration(
                    instance_obj.name,
                    nodes=self.num_nodes_in_columnar_instance))
            self.log.info("Creating Columnar Instance {}".format(
                instance_obj.name))
            instance_id = self.columnar_utils.create_instance(
                self.pod, tenant, project_id, instance_config)
            if not instance_id:
                result.append(
                    "Unable to create Columnar Instance {0} in "
                    "project {1}".format(instance_obj.name, project_id))
            instance_obj.instance_id = instance_id
            tenant.columnar_instances.append(instance_obj)

        def purge_lists(*args):
            for l in args:
                del l[:]

        def wait_for_instance_deployment(
                tenant, project_id, instance_obj, result):
            response = self.columnar_utils.wait_for_instance_to_be_deployed(
                self.pod, tenant, project_id, instance_obj)
            if not response:
                result.append("Columnar Instance {0} failed to deploy".format(
                    instance_obj.instance_id))

        def populate_columnar_instance_obj(
                tenant, project_id, instance_obj, result):
            response = self.columnar_utils.get_instance_info(
                self.pod, tenant, project_id, instance_obj
            )
            if not response:
                result.append("Fetching Instance details for {0} "
                              "failed".format(instance_obj.instance_id))
            instance_obj.srv = str(response["data"]["config"]["endpoint"])

            instance_obj.cluster_id = response["data"]["config"]["clusterId"]

            servers = CapellaUtils.get_nodes(self.pod, tenant,
                                             instance_obj.cluster_id)
            for server in servers:
                temp_server = TestInputServer()
                temp_server.ip = server.get("hostname")
                temp_server.hostname = server.get("hostname")
                temp_server.services = server.get("services")
                temp_server.port = "18091"
                temp_server.type = "columnar"
                temp_server.memcached_port = "11207"
                instance_obj.servers.append(temp_server)
            instance_obj.master = instance_obj.servers[0]
            instance_obj.cbas_cc_node = instance_obj.servers[0]

        def allow_access_from_everywhere_on_instance(
                tenant, project_id, instance_obj, result):
            response = self.columnar_utils.allow_ip_on_instance(
                self.pod, tenant, project_id, instance_obj)
            if not response:
                result.append("Setting Allow IP as 0.0.0.0/0  for instance {0}"
                              "failed".format(instance_obj.instance_id))

        # Columnar clusters can be reused within a test suite, only when they
        # are deployed on single tenant under single project.
        # Set skip_redeploy to True if you want to reuse the columnar instance.
        self.skip_redeploy = self.input.param("skip_redeploy", True)

        threads = list()
        thread_results = list()

        if self.skip_redeploy:
            # Single tenant, single project and multiple instances
            self.tenant = self.tenants[0]
            self.tenant.project_id = self.tenant.projects[0]
            if not self.capella.get("project", None):
                self.capella["project"] = self.tenant.project_id

            self.capella["clusters"] = ",".join([
                cluster.id for cluster in self.tenant.clusters])

            if isinstance(self.capella.get("instance_id"), list):
                instance_ids = self.capella.get("instance_id").split(',')
            else:
                instance_ids = self.capella.get("instance_id")

            for i in range(0, self.input.param("num_columnar_instances", 1)):
                if instance_ids and i < len(instance_ids):
                    instance = ColumnarInstance(
                        tenant_id=self.tenant.id,
                        project_id=self.tenant.project_id,
                        instance_name=None, instance_id=instance_ids[i],
                        instance_endpoint=None, db_users=list())
                    resp = self.columnar_utils.get_instance_info(
                        self.pod, self.tenant, self.tenant.project_id,
                        instance)
                    if not resp:
                        thread_results.append(
                            "Fetching instance details for {0} failed".format(
                                instance.instance_id))
                    else:
                        instance.name = str(resp["data"]["name"])
                        self.tenant.columnar_instances.append(instance)
                else:
                    threads.append(Thread(
                        target=create_columnar_instance,
                        name="create_columnar_instance_thread",
                        args=(self.tenant, self.tenant.project_id,
                              thread_results,)))

            if threads:
                self.start_threads(threads)
            if thread_results:
                raise Exception(
                    "Following Columnar instance creation failed - {0}".format(
                        thread_results))
            else:
                self.capella["instance_id"] = [
                    instance.instance_id for instance in
                    self.tenant.columnar_instances]

            purge_lists(threads, thread_results)

            populate_instance_obj_threads = list()
            populate_instance_obj_thread_results = list()
            allow_access_from_everywhere_threads = list()
            allow_access_from_everywhere_thread_results = list()
            for instance in self.tenant.columnar_instances:
                threads.append(Thread(
                    target=wait_for_instance_deployment,
                    name="waiter_thread",
                    args=(self.tenant, self.tenant.project_id, instance,
                          thread_results,)))
                populate_instance_obj_threads.append(Thread(
                    target=populate_columnar_instance_obj,
                    name="set_instance_ip_thread",
                    args=(self.tenant, self.tenant.project_id, instance,
                          populate_instance_obj_thread_results,)))
                allow_access_from_everywhere_threads.append(Thread(
                    target=allow_access_from_everywhere_on_instance,
                    name="allow_ip_thread",
                    args=(self.tenant, self.tenant.project_id, instance,
                          allow_access_from_everywhere_thread_results,)
                ))
            self.start_threads(threads)
            if thread_results:
                raise Exception("Failed while waiting for following instance "
                                "to be deployed - {0}".format(thread_results))
            purge_lists(threads, thread_results)

            self.start_threads(populate_instance_obj_threads)
            if populate_instance_obj_thread_results:
                raise Exception("Failed fetching connection string for "
                                "following instances- {0}".format(
                    populate_instance_obj_thread_results))

            self.start_threads(allow_access_from_everywhere_threads)
            if allow_access_from_everywhere_thread_results:
                raise Exception("Failed setting allow access from everywhere "
                                "on instances- {0}".format(
                    allow_access_from_everywhere_thread_results))

            # Adding db user to each instance.
            for instance in self.tenant.columnar_instances:
                resp = None
                count = 0
                while not resp and count < 5:
                    resp = self.columnar_utils.create_api_keys(
                        self.pod, self.tenant, self.tenant.project_id,
                        instance)
                    count += 1
                    time.sleep(10)
                for server in instance.servers:
                    server.rest_username = str(resp["apikeyId"])
                    server.rest_password = str(resp["secret"])
        else:
            # Multiple tenants, projects and instances
            instance_count = self.input.param("num_columnar_instances", 1)
            for tenant in self.tenants:
                if instance_count:
                    for project_id in tenant.projects:
                        if instance_count:
                            threads.append(Thread(
                                target=create_columnar_instance,
                                name="create_columnar_instance_thread",
                                args=(tenant, project_id, thread_results,)))
                            instance_count -= 1
                        else:
                            break
                else:
                    break

            if threads:
                self.start_threads(threads)
            if thread_results:
                raise Exception(
                    "Following Columnar instance creation failed - {0}".format(
                        thread_results))

            purge_lists(threads, thread_results)

            populate_instance_obj_threads = list()
            populate_instance_obj_thread_results = list()
            allow_access_from_everywhere_threads = list()
            allow_access_from_everywhere_thread_results = list()
            for tenant in self.tenants:
                for instance in tenant.columnar_instances:
                    threads.append(Thread(
                        target=wait_for_instance_deployment,
                        name="waiter_thread",
                        args=(tenant, instance.project_id, instance,
                              thread_results,)))
                    populate_instance_obj_threads.append(Thread(
                        target=populate_columnar_instance_obj,
                        name="set_instance_ip_thread",
                        args=(tenant, instance.project_id, instance,
                              populate_instance_obj_thread_results,)))
                    allow_access_from_everywhere_threads.append(Thread(
                        target=allow_access_from_everywhere_on_instance,
                        name="allow_ip_thread",
                        args=(self.tenant, self.tenant.project_id, instance,
                              allow_access_from_everywhere_thread_results,)
                    ))
            self.start_threads(threads)
            if thread_results:
                raise Exception("Failed while waiting for following instance "
                                "to be deployed - {0}".format(thread_results))
            purge_lists(threads, thread_results)

            self.start_threads(populate_instance_obj_threads)
            if populate_instance_obj_thread_results:
                raise Exception("Failed fetching connection string for "
                                "following instances- {0}".format(
                    populate_instance_obj_thread_results))

            self.start_threads(allow_access_from_everywhere_threads)
            if allow_access_from_everywhere_thread_results:
                raise Exception("Failed setting allow access from everywhere "
                                "on instances- {0}".format(
                    allow_access_from_everywhere_thread_results))

            # Adding db user to each instance.
            for tenant in self.tenants:
                for instance in tenant.columnar_instances:
                    resp = None
                    count = 0
                    while not resp and count < 5:
                        resp = self.columnar_utils.create_api_keys(
                            self.pod, tenant, instance.project_id,
                            instance)
                        count += 1
                        time.sleep(10)
                    for server in instance.servers:
                        server.rest_username = str(resp["apikeyId"])
                        server.rest_password = str(resp["secret"])

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
            if not self.columnar_utils.delete_instance(
                    self.pod, tenant, project_id, instance):
                result.append("Deleting Columnar Instance - {0}, "
                              "Instance ID - {1} failed".format(
                    instance.name, instance.instance_id))

        def wait_for_instance_deletion(tenant, project_id, instance, results):
            result = self.columnar_utils.wait_for_instance_to_be_destroyed(
                self.pod, tenant, project_id, instance)
            if not result:
                results.append("Columnar Instance {0} failed to be "
                               "deleted".format(instance.instance_id))

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
                                "failed - {0}".format(delete_instance_results))

            self.start_threads(wait_for_instance_delete_threads)
            if wait_for_instance_delete_results:
                raise Exception("Failure while waiting for Columnar Instance  "
                                "to be deleted - {0}".format(
                    wait_for_instance_delete_results))

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

        if self.input.param("skip_redeploy", True):
            if (TestInputSingleton.input.test_params["case_number"] ==
                    TestInputSingleton.input.test_params["no_of_test_identified"]):
                delete_cloud_infra()
            else:
                for tenant in self.tenants:
                    CapellaUtils.revoke_access_secret_key(
                        self.pod, tenant, tenant.api_key_id)
        else:
            self.capella["project_id"] = self.capella["instance_id"] = ""
            delete_cloud_infra()

    def start_threads(self, thread_list, async=False):
        for thread in thread_list:
            thread.start()
        if not async:
            for thread in thread_list:
                thread.join()

    def init_sdk_pool_object(self, instance, num_clients=1,
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
