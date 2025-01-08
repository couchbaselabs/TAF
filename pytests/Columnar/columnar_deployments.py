from Columnar.columnar_base import ColumnarBaseTest
from capella_utils.columnar import ColumnarInstance
from Jython_tasks.task import DeployColumnarInstance
from cbas_utils.cbas_utils_columnar import CbasUtil as columnarCBASUtil
import itertools

class ColumnarDeployments(ColumnarBaseTest):

    def setUp(self):
        super(ColumnarDeployments, self).setUp()
        self.instance = None
        if len(self.tenant.columnar_instances) > 0:
            self.instance = self.tenant.columnar_instances[0]
        self.columnar_cbas_utils = columnarCBASUtil(
            self.task, self.use_sdk_for_cbas)

        self.deployment_regions = ['us-east-1', 'us-east-2', 'us-west-2',
                                   'eu-west-1', 'eu-central-1', 'ap-south-1',
                                   'ap-southeast-1', 'ap-southeast-2']
        self.deployment_regions_gcp = ['us-east1', 'us-east4', 'us-central1', 'europe-west1',
                                       'europe-west4', 'europe-west3', 'asia-southeast1']
        if self.input.param("columnar_provider", "aws") == "gcp":
            self.deployment_regions = self.deployment_regions_gcp
        self.deployment_nodes = [1, 2, 4, 8, 16, 32]
        self.instance_types = [
            {'vcpus': '4vCPUs', 'memory': '32GB'},
            {'vcpus': '8vCPUs', 'memory': '32GB'},
            {'vcpus': '8vCPUs', 'memory': '64GB'},
            {'vcpus': '16vCPUs', 'memory': '64GB'},
            {'vcpus': '16vCPUs', 'memory': '128GB'}
        ]
        self.deployment_plans = ["developerPro", "enterprise"]
        self.deployment_azs = ["single", "multi"]

    def tearDown(self):
        super(ColumnarDeployments, self).tearDown()

    def test_cluster_deployments(self):
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

            self.log.info("Instance Ready! InstanceID:{} , ClusterID:{}"
                          .format(instance_id, instance_obj.cluster_id))

            return instance_obj

        def allow_access_from_everywhere_on_instance(
                tenant, project_id, instance_obj):
            response = self.columnar_utils.allow_ip_on_instance(
                self.pod, tenant, project_id, instance_obj)
            if not response:
                return False
            return True

        combinations = list(itertools.product(self.deployment_regions,
                                              self.deployment_nodes,
                                              self.instance_types,
                                              self.deployment_plans,
                                              self.deployment_azs))

        deployment_tasks = []
        deployment_failures = []
        query_failures = []
        for comb in combinations:
            region = comb[0]
            nodes = comb[1]
            instance_type = comb[2]
            plan = comb[3]
            az = comb[4]

            if nodes == 1 and (az == "multi" or plan == "enterprise"):
                continue

            instance_config = (
                self.columnar_utils.generate_instance_configuration())
            instance_config['provider'] = "gcp" if self.input.param("columnar_provider", "aws") == "gcp" else "aws"
            instance_config['region'] = region
            instance_config['nodes'] = nodes
            instance_config['instanceTypes'] = instance_type
            instance_config['package']['key'] = plan
            instance_config['availabilityZone'] = az

            deploy_task = DeployColumnarInstance(
                self.pod, self.tenant, instance_config["name"],
                instance_config, timeout=self.wait_timeout, retries=10)
            self.task_manager.add_new_task(deploy_task)
            deploy_task.instance_config = instance_config
            deployment_tasks.append(deploy_task)

            if len(deployment_tasks) == 2:
                instances_created = []
                for task in deployment_tasks:
                    self.task_manager.get_task_result(task)
                    if not task.result:
                        deployment_failures.append(task.config)

                    if hasattr(task, 'instance_id'):
                        if task.instance_id:
                            instance_obj = populate_columnar_instance_obj(self.tenant, task.instance_id,
                                                                          task.name, task.config)
                            instances_created.append(instance_obj)

                self.sleep(30, "Wait for sometime before executing query")
                for instance in instances_created:
                    query_result = self.columnar_cbas_utils.execute_statement_on_cbas_util(
                        instance,
                        "select 1;")
                    if not query_result:
                        query_failures.append(instance.instance_config)
                    if not self.columnar_utils.delete_instance(
                            self.pod, self.tenant, self.tenant.project_id, instance):
                        self.log.error(f"Deleting Columnar Instance - {instance.name}, "
                                       f"Instance ID - {instance.instance_id} failed")

                deployment_tasks = []

        if len(deployment_failures) > 0:
            self.fail("Cluster deployment failed for following configs: {}." \
                      "Query failed for the following configs: {}".
                      format(deployment_failures, query_failures))