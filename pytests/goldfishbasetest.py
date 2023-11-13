"""
Created on Oct 14, 2023

@author: umang.agrawal
"""
from Cb_constants import CbServer
from TestInput import TestInputSingleton
from bucket_utils.bucket_ready_functions import BucketUtils
from capella_utils.common_utils import Pod
from cb_basetest import CouchbaseBaseTest
from cluster_utils.cluster_ready_functions import ClusterUtils
from security_config import trust_all_certs

import global_vars
from goldfish_utils.common_utils import (
    Users, Project, GoldfishCluster, GoldfishUtils, DBUser)
from threading import Thread
from sdk_client3 import SDKClientPool


class OnCloudBaseTest(CouchbaseBaseTest):
    def setUp(self):
        super(OnCloudBaseTest, self).setUp()

        # Cluster level info settings
        self.capella = self.input.capella

        self.wait_timeout = self.input.param("wait_timeout", 1800)
        CbServer.use_https = True
        trust_all_certs()

        # initialize pod object
        url = self.capella.get("pod")
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

        # Create control plane users
        self.users = list()
        # Since first user is passed in ini file.
        self.users.append(Users(
            self.capella.get("tenant_id"),
            self.capella.get("capella_user").split("@")[0],
            self.capella.get("capella_user"),
            self.capella.get("capella_pwd")
        ))
        self.super_user = self.users[0]

        self.goldfish_utils = GoldfishUtils(self.log)

        for i in range(1, self.input.param("num_users", 1)):
            user = Users(self.capella.get("tenant_id"))
            result = (
                self.goldfish_utils.create_org_user_without_email_verification(
                    self.pod, self.super_user, user))
            if not result:
                raise Exception("Error while creating User {0}".format(
                    user.email))
            user.user_id = result
            self.users.append(user)

        threads = list()
        thread_results = list()

        def purge_lists(*args):
            for l in args:
                del l[:]

        # Create project using each of the users above.
        if self.input.param("num_projects_per_user", 1):
            def create_projects_per_user(user, result):
                for i in range(self.input.param("num_projects_per_user", 1)):
                    project = Project(user.org_id)
                    resp = self.goldfish_utils.create_project(
                        self.pod, user, project.name)
                    if not resp:
                        result.append("Error while creating project {0}".format(
                            project.name))
                    project.project_id = resp
                    user.projects.append(project)

            for user in self.users:
                threads.append(Thread(
                    target=create_projects_per_user,
                    name="create_project_thread",
                    args=(user, thread_results,)
                ))

            self.start_threads(threads)

            if thread_results:
                raise Exception("Following project creation failed - {0}".format(
                    thread_results))

            purge_lists(threads, thread_results)

        if self.input.param("num_goldfish_clusters_per_project", 1):
            def create_clusters_per_project(user, result):
                for project in user.projects:
                    for i in range(self.input.param(
                            "num_goldfish_clusters_per_project", 1)):
                        cluster = GoldfishCluster(
                            org_id=project.org_id, project_id=project.project_id,
                            cluster_name=None, cluster_id=None,
                            cluster_endpoint=None,
                            nebula_sdk_port=self.nebula_sdk_proxy_port,
                            nebula_rest_port=self.nebula_rest_proxy_port,
                            db_users=list(), type="goldfish")
                        cluster_config = (
                            self.goldfish_utils.generate_goldfish_cluster_configuration(
                            cluster.name))
                        cluster_id = self.goldfish_utils.create_goldfish_cluster(
                            self.pod, user, project, cluster_config)
                        if not cluster_id:
                            result.append(
                                "Unable to create goldfish cluster {0} in "
                                "project {1}".format(cluster.name, project.name))
                        cluster.cluster_id = cluster_id
                        project.clusters.append(cluster)

            for user in self.users:
                threads.append(Thread(
                    target=create_clusters_per_project,
                    name="create_cluster_thread",
                    args=(user, thread_results,)
                ))

            self.start_threads(threads)
            if thread_results:
                raise Exception("Following cluster creation API failed - {"
                                "0}".format(thread_results))

            purge_lists(threads, thread_results)

            def wait_for_cluster_deployment(user, cluster, result):
                resp = self.goldfish_utils.wait_for_cluster_to_be_deployed(
                    self.pod, user, cluster)
                if not resp:
                    result.append("Cluster {0} failed to deploy".format(
                        cluster.name))

            def get_cluster_endpoint(user, cluster, result):
                resp = self.goldfish_utils.get_cluster_info(self.pod, user,
                                                            cluster)
                if not resp:
                    result.append("Fetching cluster details for {0} "
                                  "failed".format(cluster.name))
                cluster.endpoint = resp["config"]["endpoint"]
                cluster.master.ip = cluster.endpoint

            fetch_cluster_conn_str_threads = list()
            fetch_cluster_conn_str_thread_results = list()
            for user in self.users:
                for project in user.projects:
                    for cluster in project.clusters:
                        threads.append(Thread(
                            target=wait_for_cluster_deployment,
                            name="waiter_thread",
                            args=(user, cluster, thread_results,)
                        ))
                        fetch_cluster_conn_str_threads.append(Thread(
                            target=get_cluster_endpoint,
                            name="set_cluster_ip_thread",
                            args=(user, cluster, fetch_cluster_conn_str_thread_results,)
                        ))
            self.start_threads(threads)
            if thread_results:
                raise Exception("Failed while waiting for following clusters "
                                "to be deployed - {0}".format(thread_results))
            purge_lists(threads, thread_results)

            self.start_threads(fetch_cluster_conn_str_threads)
            if fetch_cluster_conn_str_thread_results:
                raise Exception("Failed fetching connection string for "
                                "following clusters- {0}".format(thread_results))

            # Adding db user to each cluster.
            for user in self.users:
                for project in user.projects:
                    for cluster in project.clusters:
                        resp = self.goldfish_utils.create_db_user_api_keys(
                            self.pod, user, cluster)
                        db_user = DBUser(resp["apikeyId"], resp["secret"])
                        cluster.db_users.append(db_user)

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
            # Add code to get goldfish logs.
            return

        if self.skip_teardown_cleanup:
            return

        delete_cluster_threads = list()
        wait_for_cluster_delete_threads = list()
        delete_project_threads = list()
        delete_users_threads = list()

        delete_cluster_results = list()
        wait_for_cluster_delete_results = list()
        delete_project_results = list()
        delete_users_results = list()

        def delete_cluster(user, clutser, result):
            if not self.goldfish_utils.delete_goldfish_cluster(
                    self.pod, user, clutser):
                result.append("Deleting cluster {0} failed".format(cluster.name))

        def wait_for_cluster_deletion(user, cluster, results):
            result = self.goldfish_utils.wait_for_cluster_to_be_destroyed(
                self.pod, user, cluster)
            if not result:
                results.append("Cluster {0} failed to be deleted".format(
                    cluster.name))

        def delete_project(user, project, results):
            result = self.goldfish_utils.delete_project(self.pod, user, project)
            if not result:
                results.append("Project {0} failed to be deleted".format(
                    project.name))

        def delete_user(user, results):
            result = self.goldfish_utils.delete_org_user(self.pod, user)
            if not result:
                results.append("User {0} failed to be deleted".format(
                    user.email))

        for user in self.users:
            for project in user.projects:
                for cluster in project.clusters:
                    delete_cluster_threads.append(Thread(
                        target=delete_cluster,
                        name="delete_cluster_thread",
                        args=(user, cluster, delete_cluster_results,)
                    ))
                    wait_for_cluster_delete_threads.append(Thread(
                        target=wait_for_cluster_deletion,
                        name="waiter_thread",
                        args=(user, cluster, wait_for_cluster_delete_results,)
                    ))
                delete_project_threads.append(Thread(
                    target=delete_project,
                    name="delete_project_thread",
                    args=(user, project, delete_project_results,)
                ))
            if user.email != self.capella.get("capella_user"):
                delete_users_threads.append(Thread(
                    target=delete_user,
                    name="delete_user_thread",
                    args=(user, delete_users_results,)
                ))

        self.start_threads(delete_cluster_threads)
        if delete_cluster_results:
            raise Exception("Following cluster deletion APIs failed - {"
                            "0}".format(delete_cluster_results))

        self.start_threads(wait_for_cluster_delete_threads)
        if wait_for_cluster_delete_results:
            raise Exception("Failure while waiting for cluster to be deleted "
                            "- {0}".format(wait_for_cluster_delete_results))

        self.start_threads(delete_project_threads)
        if delete_project_results:
            raise Exception("Following project deletion failed - {0}".format(
                delete_project_results))

        self.start_threads(delete_users_threads)
        if delete_users_results:
            raise Exception("Following user deletion failed - {0}".format(
                delete_users_results))

    def start_threads(self, thread_list, async=False):
        for thread in thread_list:
            thread.start()
        if not async:
            for thread in thread_list:
                thread.join()

    def init_sdk_pool_object(self, cluster, num_clients=1,
                             username="Administrator", password="password"):
        """
        Overriding the method from CouchbaseBaseTest class
        :return:
        """
        cluster.sdk_client_pool = SDKClientPool()
        cluster.sdk_client_pool.create_cluster_clients(
            cluster=cluster, servers=[cluster.master], req_clients=num_clients,
            username=username, password=password)

    def list_all_projects(self):
        projects = list()
        for user in self.users:
            projects.extend(user.projects)
        return projects

    def list_all_clusters(self):
        clusters = list()
        for project in self.list_all_projects():
            clusters.extend(project.clusters)
        return clusters

class ClusterSetup(OnCloudBaseTest):
    def setUp(self):
        super(ClusterSetup, self).setUp()

        self.log_setup_status("ClusterSetup", "started", "setup")
        self.log_setup_status("ClusterSetup", "complete", "setup")

    def tearDown(self):
        super(ClusterSetup, self).tearDown()
