"""
Created on Oct 14, 2023

@author: umang.agrawal
"""
import re
import cluster_utils.cluster_ready_functions
import global_vars
import random
from datetime import datetime, timezone

from bucket_utils.bucket_ready_functions import BucketUtils
from cluster_utils.cluster_ready_functions import ClusterUtils
from capella_utils.columnar import ColumnarInstance, ColumnarUtils
from Jython_tasks.task import DeployColumnarInstance
from pytests.dedicatedbasetestcase import ProvisionedBaseTestCase
from sdk_client3 import SDKClientPool
from threading import Thread
from TestInput import TestInputSingleton
from capella_utils.dedicated import CapellaUtils


def transform_couchbase_version(aws_version):
    """
    Changes the build name from aws to gcp
    Example: couchbase-columnar-1.1.0-XXXX-arm64-v1.1.0 to couchbase-columnar-1-1-0-XXXX-arm64-v1-1-0
    """
    return re.sub(r'(\d+)\.(\d+)\.(\d+)', r'\1-\2-\3', aws_version)

class ColumnarBaseTest(ProvisionedBaseTestCase):
    def setUp(self):
        super(ColumnarBaseTest, self).setUp()
        self.log_setup_status(self.__class__.__name__, "started")

        # Columnar instance configuration
        self.num_nodes_in_columnar_instance = self.input.param(
            "num_nodes_in_columnar_instance", 2)
        """ 
        Instance type for columnar instance. Valid values are 
        4vCPUs:16GB, 4vCPUs:32GB, 8vCPUs:32GB, 8vCPUs:64GB, 16vCPUs:64GB, 
        16vCPUs:128GB,
        """
        self.instance_type = self.input.param("instance_type",
                                              "4vCPUs:32GB").split(":")
        provider = self.input.param("columnar_provider", "aws")
        self.columnar_image = self.capella.get("columnar_image", None)
        if provider == "gcp":
            self.columnar_image = transform_couchbase_version(self.columnar_image)

        # Utility objects
        self.columnar_utils = ColumnarUtils(self.log)

        def populate_columnar_instance_obj(
                tenant, instance_id, instance_name=None, instance_config=None):

            instance_obj = ColumnarInstance(
                tenant_id=tenant.id,
                project_id=tenant.project_id,
                instance_name=instance_name,
                instance_id=instance_id)

            instance_obj.username, instance_obj.password = \
                self.columnar_utils.create_couchbase_cloud_qe_user(
                    self.pod, tenant, instance_obj)

            if not allow_access_from_everywhere_on_instance(
                    tenant, tenant.project_id, instance_obj):
                raise Exception(
                    "Setting Allow IP as 0.0.0.0/0 for instance {0} "
                    "failed".format(instance_obj.instance_id))

            self.columnar_utils.update_columnar_instance_obj(
                self.pod, tenant, instance_obj)
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

        provider = self.input.param("columnar_provider", "aws")
        for i in range(0, self.input.param("num_columnar_instances", 1)):
            if instance_ids and i < len(instance_ids):
                populate_columnar_instance_obj(self.tenant, instance_ids[i])
            else:
                instance_config = (
                    self.columnar_utils.generate_instance_configuration(
                        name=self.prefix + "Columnar_{0}".format(
                            random.randint(1, 100000)),
                        provider = provider,
                        region=self.region if provider == "aws" else self.gcp_region,
                        nodes=self.num_nodes_in_columnar_instance,
                        instance_types={
                            "vcpus": self.instance_type[0],
                            "memory": self.instance_type[1]
                        },
                        token=self.pod.override_key,
                        image=self.columnar_image))

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
            # So that columnar cluster isn't deployed for current test in case
            # previous test failed in setup and a cluster was already deployed.
            if self.capella["instance_id"]:
                self.capella["instance_id"] += f",{task.instance_id}"
            else:
                self.capella["instance_id"] = task.instance_id
            populate_columnar_instance_obj(self.tenant, task.instance_id,
                                           task.name, instance_config)

        # Adding db user to each instance.
        for instance in self.tenant.columnar_instances:
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

    def has_test_failed(self):
        if hasattr(self._outcome, 'errors'):
            result = self.defaultTestResult()
            self._feedErrorsToResult(result, self._outcome.errors)
        else:
            result = self._outcome.result
        ok = all(test != self for test, text in result.errors + result.failures)
        if ok:
            return False
        else:
            self.log.info('Errors/failures seen during test execution of {}. Errors {} and Failures {}'.format(
                self._testMethodName,
                result.errors,
                result.failures))
            return True

    def collect_logs(self):
        log_links = []
        for instance in self.tenant.columnar_instances + self.tenant.clusters:
            current_date_time = datetime.now(timezone.utc)
            formatted_date_time = current_date_time.strftime("%Y-%m-%d_%H:%M:%S")
            log_id = self._testMethodName + '_' + str(formatted_date_time)
            # internal error for call with ticket id
            if isinstance(instance, cluster_utils.cluster_ready_functions.CBCluster):
                CapellaUtils.trigger_log_collection(self.pod, self.tenant, instance.id, "")
                tasks = CapellaUtils.check_logs_collect_status(self.pod, self.tenant, instance.id)
            else:
                CapellaUtils.trigger_log_collection(self.pod, self.tenant, instance.cluster_id, "")
                tasks = CapellaUtils.check_logs_collect_status(self.pod, self.tenant, instance.cluster_id)
            for key in tasks['perNode']:
                log_links.append(tasks['perNode'][key]['url'])

        self.log.info("Logs collection timezone is UTC, Collect logs at:")
        for link in log_links:
            self.log.info(link)

    def tearDown(self):
        if self.has_test_failed():
            self.collect_logs()

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
            else:
                self.log.info(f"Columnar Instance {instance.name} "
                              f"successfully deleted")

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
