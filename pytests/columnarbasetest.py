"""
Created on Oct 14, 2023

@author: umang.agrawal
"""
import time

from Cb_constants import CbServer
from TestInput import TestInputSingleton
from bucket_utils.bucket_ready_functions import BucketUtils
from capella_utils.common_utils import Pod
from cb_basetest import CouchbaseBaseTest
from cluster_utils.cluster_ready_functions import ClusterUtils
from security_config import trust_all_certs

import global_vars
from capella_utils.columnar import (
    Users, Project, DBUser, ColumnarInstance, ColumnarUtils)
from threading import Thread
from sdk_client3 import SDKClientPool

from capellaAPI.capella.common.CapellaAPI import CommonCapellaAPI


class OnCloudBaseTest(CouchbaseBaseTest):

    def setUp(self):
        super(OnCloudBaseTest, self).setUp()

        # Instance level info settings
        self.capella = self.input.capella

        self.wait_timeout = self.input.param("wait_timeout", 1800)
        CbServer.use_https = True
        trust_all_certs()

        # initialize pod object
        url = self.capella.get("pod")
        project_id = self.capella.get("project_id") if not self.capella.get("project_id") == "" else None
        if isinstance(self.capella.get("instance_id"), list):
            instance_ids = self.capella.get("instance_id")
        else:
            instance_ids = self.capella.get("instance_id").split(',') if not (
                    self.capella.get("instance_id") == "") else None

        self.pod = Pod(
            "https://%s" % url, self.capella.get("token", None))

        self.log_setup_status(self.__class__.__name__, "started")

        self.rest_username = \
            TestInputSingleton.input.membase_settings.rest_username
        self.rest_password = \
            TestInputSingleton.input.membase_settings.rest_password

        # Nebula ports
        self.nebula_sdk_proxy_port = self.capella.get(
            "nebula_sdk_proxy_port", 16001)
        self.nebula_rest_proxy_port = self.capella.get(
            "nebula_rest_proxy_port", 18001)

        self.num_nodes_in_columnar_instance = self.input.param(
            "num_nodes_in_columnar_instance", 0)

        # Create control plane users
        self.user = Users(
            self.capella.get("tenant_id"),
            self.capella.get("capella_user").split("@")[0],
            self.capella.get("capella_user"),
            self.capella.get("capella_pwd")
        )

        self.columnar_utils = ColumnarUtils(self.log, self.pod, self.user)

        # Create project.
        self.project = Project(self.capella.get("tenant_id"))

        if not project_id:
            self.log.info("Creating project {}".format(self.project.name))
            resp = self.columnar_utils.create_project(self.project.name)
            if not resp:
                raise Exception("Error while creating project {0}".format(
                    self.project.name))
            self.project.project_id = resp
            self.capella["project_id"] = self.project.project_id
        else:
            self.project.project_id = project_id
            capella_api = CommonCapellaAPI(
                self.pod.url_public, self.user.api_secret_key,
                self.user.api_access_key, self.user.email, self.user.password)
            resp = self.columnar_utils.capella_api.access_project(
                self.user.org_id, self.project.project_id)
            if resp.status_code != 200:
                self.fail("Unable to fetch project info for {}".format(
                    self.project.project_id))
            self.project.name = resp.json()["data"]["name"]

        threads = list()
        thread_results = list()

        def purge_lists(*args):
            for l in args:
                del l[:]

        def create_columnar_instance(project, result):
            instance = ColumnarInstance(
                org_id=project.org_id, project_id=project.project_id,
                instance_name=None, instance_id=None, instance_endpoint=None,
                nebula_sdk_port=self.nebula_sdk_proxy_port,
                nebula_rest_port=self.nebula_rest_proxy_port,
                db_users=list(), instance_type="columnar")
            instance_config = (
                self.columnar_utils.generate_cloumnar_instance_configuration(
                    instance.name, nodes=self.num_nodes_in_columnar_instance))
            self.log.info("Creating Columnar Instance {}".format(instance.name))
            instance_id = self.columnar_utils.create_columnar_instance(
                project, instance_config)
            if not instance_id:
                result.append(
                    "Unable to create Columnar Instance {0} in "
                    "project {1}".format(instance.name, project.name))
            instance.instance_id = instance_id
            project.instances.append(instance)

        for i in range(0, self.input.param("num_columnar_instances", 1)):
            if instance_ids and i < len(instance_ids):
                instance = ColumnarInstance(
                    org_id=self.project.org_id,
                    project_id=self.project.project_id,
                    instance_name=None, instance_id=instance_ids[i],
                    instance_endpoint=None,
                    nebula_sdk_port=self.nebula_sdk_proxy_port,
                    nebula_rest_port=self.nebula_rest_proxy_port,
                    db_users=list(), instance_type="columnar")
                resp = self.columnar_utils.get_instance_info(instance)
                if not resp:
                    thread_results.append(
                        "Fetching instance details for {0} failed".format(
                            instance.instance_id))
                instance.name = str(resp["name"])
                self.project.instances.append(instance)
            else:
                threads.append(Thread(
                    target=create_columnar_instance,
                    name="create_columnar_instance_thread",
                    args=(self.project, thread_results,)
                ))

        if threads:
            self.start_threads(threads)
        if thread_results:
            raise Exception("Following Columnar instance creation failed - {"
                            "0}".format(thread_results))
        else:
            self.capella["instance_id"] = [instance.instance_id for instance in
                                           self.project.instances]

        purge_lists(threads, thread_results)

        def wait_for_instance_deployment(instance, result):
            resp = self.columnar_utils.wait_for_instance_to_be_deployed(
                instance)
            if not resp:
                result.append("Columnar Instance {0} failed to deploy".format(
                    instance.instance_id))

        def get_instance_endpoint(instance, result):
            resp = self.columnar_utils.get_instance_info(instance)
            if not resp:
                result.append("Fetching Instance details for {0} "
                              "failed".format(instance.instance_id))
            instance.endpoint = resp["config"]["endpoint"]
            instance.master.ip = instance.endpoint

        fetch_instance_conn_str_threads = list()
        fetch_instance_conn_str_thread_results = list()
        for instance in self.project.instances:
            threads.append(Thread(
                target=wait_for_instance_deployment,
                name="waiter_thread",
                args=(instance, thread_results,)
            ))
            fetch_instance_conn_str_threads.append(Thread(
                target=get_instance_endpoint,
                name="set_instance_ip_thread",
                args=(instance, fetch_instance_conn_str_thread_results,)
            ))
        self.start_threads(threads)
        if thread_results:
            raise Exception("Failed while waiting for following instance "
                            "to be deployed - {0}".format(thread_results))
        purge_lists(threads, thread_results)

        self.start_threads(fetch_instance_conn_str_threads)
        if fetch_instance_conn_str_thread_results:
            raise Exception("Failed fetching connection string for "
                            "following instances- {0}".format(thread_results))

        # Adding db user to each instance.
        for instance in self.project.instances:
            resp = None
            count = 0
            while not resp and count < 5:
                resp = self.columnar_utils.create_columnar_instance_api_keys(
                    instance)
                count += 1
                time.sleep(10)
            instance.api_access_key = resp["apikeyId"]
            instance.api_secret_key = resp["secret"]

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

        def delete_instace(instance, result):
            if not self.columnar_utils.delete_columnar_instance(instance):
                result.append("Deleting Columnar Instance - {0}, "
                              "Instance ID - {1} failed".format(
                    instance.name, instance.instance_id))

        def wait_for_instance_deletion(instance, results):
            result = self.columnar_utils.wait_for_instance_to_be_destroyed(
                instance)
            if not result:
                results.append("Columnar Instance {0} failed to be "
                               "deleted".format(instance.instance_id))

        def delete_cloud_infra():
            for instance in self.project.instances:
                delete_instance_threads.append(Thread(
                    target=delete_instace,
                    name="delete_instance_thread",
                    args=(instance, delete_instance_results,)
                ))
                wait_for_instance_delete_threads.append(Thread(
                    target=wait_for_instance_deletion,
                    name="waiter_thread",
                    args=(instance, wait_for_instance_delete_results,)
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
            result = self.columnar_utils.delete_project(self.project)
            if not result:
                raise Exception("Project {0} failed to be deleted".format(
                    self.project.name))

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


class ClusterSetup(OnCloudBaseTest):
    def setUp(self):
        self.log_setup_status("ClusterSetup", "started", "setup")
        super(ClusterSetup, self).setUp()

        self.log_setup_status("ClusterSetup", "complete", "setup")

    def tearDown(self):
        super(ClusterSetup, self).tearDown()
