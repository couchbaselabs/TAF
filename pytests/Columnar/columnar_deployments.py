from Columnar.columnar_base import ColumnarBaseTest
from capella_utils.columnar import ColumnarInstance
from cbas_utils.cbas_utils_columnar import CbasUtil as columnarCBASUtil
import itertools
from beautifultable import BeautifulTable

"""
Deploying -> Deployed/Failed -> Querying -> Query Success/Query Failed -> Deleting -> Deleted/Delete Failed
"""


class ColumnarDeployments(ColumnarBaseTest):

    def __init__(self, methodName: str = "runTest"):
        super().__init__(methodName)
        self.deployment_regions = ['us-east-1', 'us-east-2', 'us-west-2',
                                   'eu-west-1', 'eu-central-1', 'ap-south-1',
                                   'ap-southeast-1', 'ap-southeast-2']
        self.deployment_regions_gcp = ['us-east1', 'us-east4', 'us-central1', 'europe-west1',
                                       'europe-west4', 'europe-west3', 'asia-southeast1']
        self.deployment_nodes = [1, 16]  # [1, 2, 4, 8, 16, 32]
        self.instance_types = [
            {'vcpus': '4vCPUs', 'memory': '32GB'},
            {'vcpus': '8vCPUs', 'memory': '64GB'},
            {'vcpus': '16vCPUs', 'memory': '128GB'}
        ]
        # {'vcpus': '8vCPUs', 'memory': '32GB'},
        # {'vcpus': '16vCPUs', 'memory': '64GB'},
        self.deployment_plans = ["developerPro", "enterprise"]
        self.deployment_azs = ["single", "multi"]

    def setUp(self):
        super(ColumnarDeployments, self).setUp()
        self.instance = None
        if len(self.tenant.columnar_instances) > 0:
            self.instance = self.tenant.columnar_instances[0]
        self.columnar_cbas_utils = columnarCBASUtil(
            self.task, self.use_sdk_for_cbas)
        self.columnar_image = self.input.capella.get("columnar_image", None)
        if self.input.param("columnar_provider", "aws") == "gcp":
            self.deployment_regions = self.deployment_regions_gcp
            self.columnar_image = self.columnar_image.replace(".", "-")
        self.override_key = self.input.capella.get("override_key")
        self.wait_timeout = 1800  # 30 minutes

        # Initialize deployment tracking
        self.deployment_tracking = []

    def tearDown(self):
        super(ColumnarDeployments, self).tearDown()

    def log_deployment_state(self, comb_id, combination, state, instance_id=None, error_msg=None):
        region, nodes, instance_type, plan, az = combination

        # Update or add tracking entry
        existing_entry = None
        for entry in self.deployment_tracking:
            if entry['combination_id'] == comb_id:
                existing_entry = entry
                break

        if existing_entry:
            existing_entry['state'] = state
            existing_entry['instance_id'] = instance_id
            existing_entry['error_msg'] = error_msg
        else:
            self.deployment_tracking.append({
                'combination_id': comb_id,
                'combination_tuple': combination,
                'state': state,
                'instance_id': instance_id,
                'error_msg': error_msg
            })

        # Print current deployment status table for the current combination only
        table = BeautifulTable()
        table.columns.header = ['Combo ID', 'Region', 'Nodes',
                                'Instance Type', 'Plan', 'AZ', 'State', 'Instance ID', 'Error']

        # Set column widths for better readability
        table.columns.width = [15, 20, 10, 20, 20, 20, 20, 40, 50]

        # Find the current combination entry
        current_entry = None
        for entry in self.deployment_tracking:
            if entry['combination_id'] == comb_id:
                current_entry = entry
                break

        if current_entry:
            region, nodes, instance_type, plan, az = current_entry['combination_tuple']
            # Format instance type to show only CPU and memory
            instance_type_str = f"{instance_type['vcpus']}, {instance_type['memory']}"
            table.rows.append([
                current_entry['combination_id'],
                region,
                str(nodes),
                instance_type_str,
                plan,
                az,
                current_entry['state'],
                current_entry['instance_id'] or 'N/A',
                current_entry['error_msg'] or 'N/A'
            ])

        self.log.info(f"\n{table}\n")

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
            self.sleep(60, "Wait for sometime before updating instance object")
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

        self.log.info(f"Total combinations: {len(combinations)}")
        deployment_failures = []
        query_failures = []
        delete_failures = []

        # Process each combination sequentially
        for idx, comb in enumerate(combinations):
            region = comb[0]
            nodes = comb[1]
            instance_type = comb[2]
            plan = comb[3]
            az = comb[4]

            # Generate unique combination ID
            comb_id = f"combo_{idx}"

            # invalid combinations
            if nodes == 1 and (az == "multi" or plan == "enterprise"):
                self.log.info(f"Invalid combination: {comb}")
                continue
            if nodes != 1 and (plan == "developerPro" or az == "single"):
                self.log.info(f"Invalid combination: {comb}")
                continue

            instance_config = (
                self.columnar_utils.generate_instance_configuration())
            instance_config['provider'] = "gcp" if self.input.param(
                "columnar_provider", "aws") == "gcp" else "aws"
            instance_config['region'] = region
            instance_config['nodes'] = nodes
            instance_config['instanceTypes'] = instance_type
            instance_config['package']['key'] = plan
            instance_config['availabilityZone'] = az
            if self.columnar_image:
                instance_config['overRide'] = dict()
                instance_config['overRide']['image'] = self.columnar_image
                instance_config['overRide']['token'] = self.override_key

            # Deploy instance
            retry_count = 0
            max_retries = 3
            status = False
            instance_obj = None
            while not status and retry_count < max_retries:
                try:
                    # Columnar deployment
                    self.log_deployment_state(
                        comb_id, comb, f'Deploying({retry_count + 1})')

                    instance_id = self.columnar_utils.create_instance(
                        self.pod, self.tenant, instance_config, timeout=self.wait_timeout, retries=10)

                    if instance_id:
                        instance_info = self.columnar_utils.get_instance_info(
                            self.pod, self.tenant, self.tenant.project_id, instance_id)
                        current_state = instance_info.get("data", {}).get(
                            "state", "unknown") if instance_info else "unknown"

                        if current_state != "healthy":
                            raise Exception(
                                f"Deployment failed - instance state: {current_state}")

                        self.log_deployment_state(
                            comb_id, comb, 'Deployed', instance_id=instance_id)

                        instance_obj = populate_columnar_instance_obj(
                            self.tenant, instance_id, instance_config["name"], instance_config)

                        self.sleep(
                            30, "Wait for sometime before executing query")

                        # Query execution
                        self.log_deployment_state(
                            comb_id, comb, 'Querying', instance_id=instance_id)

                        query_result = self.columnar_cbas_utils.execute_statement_on_cbas_util(
                            instance_obj, "select 1;")
                        if query_result:
                            self.log_deployment_state(
                                comb_id, comb, 'Query Success', instance_id=instance_id, error_msg=None)
                        else:
                            self.log_deployment_state(
                                comb_id, comb, 'Query Failed', instance_id=instance_id, error_msg="Query execution failed")
                            query_failures.append(comb_id)
                            break

                        # Deletion
                        self.log_deployment_state(
                            comb_id, comb, 'Deleting', instance_id=instance_id)

                        delete_success = self.columnar_utils.delete_instance(
                            self.pod, self.tenant, self.tenant.project_id, instance_obj)
                        if delete_success:
                            self.log_deployment_state(
                                comb_id, comb, 'Deleted', instance_id=instance_id, error_msg=None)
                            status = True
                        else:
                            self.log_deployment_state(
                                comb_id, comb, 'Delete Failed', instance_id=instance_id, error_msg=f"Failed to delete instance {instance_id}")
                            delete_failures.append(comb_id)
                            break

                    else:
                        self.log_deployment_state(
                            comb_id, comb, 'Failed', error_msg="Deployment failed - no instance ID returned")
                        if instance_id:
                            # Create a minimal instance object for deletion
                            temp_instance_obj = ColumnarInstance(
                                tenant_id=self.tenant.id,
                                project_id=self.tenant.project_id,
                                instance_id=instance_id)
                            self.columnar_utils.delete_instance(
                                self.pod, self.tenant, self.tenant.project_id, temp_instance_obj)
                        retry_count += 1

                except Exception as e:
                    self.log_deployment_state(
                        comb_id, comb, 'Failed', error_msg=f"Deployment exception: {str(e)}")
                    if instance_id:
                        # Create a minimal instance object for deletion
                        temp_instance_obj = ColumnarInstance(
                            tenant_id=self.tenant.id,
                            project_id=self.tenant.project_id,
                            instance_id=instance_id)
                        self.columnar_utils.delete_instance(
                            self.pod, self.tenant, self.tenant.project_id, temp_instance_obj)
                    retry_count += 1

                if retry_count >= max_retries:
                    self.log.info(
                        f"Deployment failed - max retries exceeded for combination id: {comb_id}")
                    deployment_failures.append(comb_id)
                    break

        # Failure summary
        self.log.info(f"\n{'='*50}")
        self.log.info("FAILURE SUMMARY")
        self.log.info(f"{'='*50}")

        self.log.info(f"Deployment failures: {deployment_failures}")
        self.log.info(f"Query failures: {query_failures}")
        self.log.info(f"Delete failures: {delete_failures}")
        self.log.info(f"{'='*50}")

        if len(deployment_failures) > 0 or len(query_failures) > 0 or len(delete_failures) > 0:
            self.fail("Test cluster deployment failed")

        self.log.info("Test cluster deployment passed")
