"""
Created on Oct 14, 2023

@author: umang.agrawal
"""
import time

from TestInput import TestInputSingleton
from bucket_utils.bucket_ready_functions import BucketUtils
from cluster_utils.cluster_ready_functions import ClusterUtils
from pytests.dedicatedbasetestcase import CapellaBaseTest

import global_vars
from capella_utils.columnar_final import DBUser, ColumnarInstance, ColumnarUtils
from threading import Thread
from sdk_client3 import SDKClientPool

from capella_utils.dedicated import CapellaUtils


class ColumnarBaseTest(CapellaBaseTest):

    def setUp(self):
        super(ColumnarBaseTest, self).setUp()

        self.log_setup_status(self.__class__.__name__, "started")

        # Nebula ports
        self.nebula_sdk_proxy_port = self.capella.get(
            "nebula_sdk_proxy_port", 16001)
        self.nebula_rest_proxy_port = self.capella.get(
            "nebula_rest_proxy_port", 18001)

        self.num_nodes_in_columnar_instance = self.input.param(
            "num_nodes_in_columnar_instance", 0)

        self.columnar_utils = ColumnarUtils(self.log)

        def create_columnar_instance(tenant, project_id, result):
            instance_obj = ColumnarInstance(
                org_id=tenant.id, project_id=project_id,
                instance_name=None, instance_id=None, instance_endpoint=None,
                nebula_sdk_port=self.nebula_sdk_proxy_port,
                nebula_rest_port=self.nebula_rest_proxy_port, db_users=list())
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

        def get_instance_endpoint(
                tenant, project_id, instance_obj, result):
            response = self.columnar_utils.get_instance_info(
                self.pod, tenant, project_id, instance_obj
            )
            if not response:
                result.append("Fetching Instance details for {0} "
                              "failed".format(instance_obj.instance_id))
            instance_obj.endpoint = str(response["config"]["endpoint"])
            instance_obj.master.ip = instance_obj.endpoint

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

            if isinstance(self.capella.get("instance_id"), list):
                instance_ids = self.capella.get("instance_id")
            else:
                instance_ids = self.capella.get("instance_id").split(
                    ',') if not (
                        self.capella.get("instance_id") == "") else None

            for i in range(0, self.input.param("num_columnar_instances", 1)):
                if instance_ids and i < len(instance_ids):
                    instance = ColumnarInstance(
                        org_id=self.tenant.id,
                        project_id=self.tenant.project_id,
                        instance_name=None, instance_id=instance_ids[i],
                        instance_endpoint=None,
                        nebula_sdk_port=self.nebula_sdk_proxy_port,
                        nebula_rest_port=self.nebula_rest_proxy_port, db_users=list())
                    resp = self.columnar_utils.get_instance_info(
                        self.pod, self.tenant, self.tenant.project_id,
                        instance)
                    if not resp:
                        thread_results.append(
                            "Fetching instance details for {0} failed".format(
                                instance.instance_id))
                    else:
                        instance.name = str(resp["name"])
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

            fetch_instance_conn_str_threads = list()
            fetch_instance_conn_str_thread_results = list()
            for instance in self.tenant.columnar_instances:
                threads.append(Thread(
                    target=wait_for_instance_deployment,
                    name="waiter_thread",
                    args=(self.tenant, self.tenant.project_id, instance,
                          thread_results,)))
                fetch_instance_conn_str_threads.append(Thread(
                    target=get_instance_endpoint,
                    name="set_instance_ip_thread",
                    args=(self.tenant, self.tenant.project_id, instance,
                          fetch_instance_conn_str_thread_results,)))
            self.start_threads(threads)
            if thread_results:
                raise Exception("Failed while waiting for following instance "
                                "to be deployed - {0}".format(thread_results))
            purge_lists(threads, thread_results)

            self.start_threads(fetch_instance_conn_str_threads)
            if fetch_instance_conn_str_thread_results:
                raise Exception("Failed fetching connection string for "
                                "following instances- {0}".format(
                    fetch_instance_conn_str_thread_results))

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
                instance.api_access_key = str(resp["apikeyId"])
                instance.api_secret_key = str(resp["secret"])
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

            fetch_instance_conn_str_threads = list()
            fetch_instance_conn_str_thread_results = list()
            for tenant in self.tenants:
                for instance in tenant.columnar_instances:
                    threads.append(Thread(
                        target=wait_for_instance_deployment,
                        name="waiter_thread",
                        args=(tenant, instance.project_id, instance,
                              thread_results,)))
                    fetch_instance_conn_str_threads.append(Thread(
                        target=get_instance_endpoint,
                        name="set_instance_ip_thread",
                        args=(tenant, instance.project_id, instance,
                              fetch_instance_conn_str_thread_results,)))
            self.start_threads(threads)
            if thread_results:
                raise Exception("Failed while waiting for following instance "
                                "to be deployed - {0}".format(thread_results))
            purge_lists(threads, thread_results)

            self.start_threads(fetch_instance_conn_str_threads)
            if fetch_instance_conn_str_thread_results:
                raise Exception("Failed fetching connection string for "
                                "following instances- {0}".format(
                    fetch_instance_conn_str_thread_results))

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
                    instance.api_access_key = str(resp["apikeyId"])
                    instance.api_secret_key = str(resp["secret"])

        self.cluster_util = ClusterUtils(self.task_manager)
        self.bucket_util = BucketUtils(self.cluster_util, self.task)

        # Setting global_vars for future reference
        global_vars.cluster_util = self.cluster_util
        global_vars.bucket_util = self.bucket_util

    def tearDown(self):
        self.shutdown_task_manager()
        if self.sdk_client_pool:
            self.sdk_client_pool.shutdown()

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

            if "cloud.couchbase.com" in self.pod.url_public:
                return

            for tenant in self.tenants:
                for project_id in self.tenants.projects:
                    result = CapellaUtils.delete_project(
                        self.pod, tenant, project_id)
                    if not result:
                        raise Exception(
                            "Project {0} failed to be deleted".format(
                                project_id))

        if self.input.param("skip_redeploy", False):
            if (TestInputSingleton.input.test_params["case_number"] ==
                    TestInputSingleton.input.test_params["no_of_test_identified"]):
                delete_cloud_infra()
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
