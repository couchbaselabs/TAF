import json
import random
import string
from queue import Queue
import time

from Columnar.columnar_base import ColumnarBaseTest
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI
from capella_utils.columnar import ColumnarUtils, ColumnarRBACUtil
from cbas_utils.cbas_utils_columnar import CbasUtil as columnarCBASUtil
from capellaAPI.capella.columnar.CapellaAPI import CapellaAPI as ColumnarAPI
from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI as CapellaAPIv2

from awsLib.s3_data_helper import perform_S3_operation

class ColumnarRBAC(ColumnarBaseTest):
    def setUp(self):
        super(ColumnarRBAC, self).setUp()
        self.cluster = self.tenant.columnar_instances[0]
        self.remote_cluster = self.cluster
        if self.num_clusters > 0:
            self.remote_cluster = self.tenant.clusters[0]
        self.doc_loader_url = self.input.param("sirius_url", None)
        self.doc_loader_port = self.input.param("sirius_port", None)
        self.no_of_docs = self.input.param("no_of_docs", 1000)
        self.sink_s3_bucket_name = None
        self.capellaAPIv4 = CapellaAPI(self.pod.url_public, '', '', self.tenant.user, self.tenant.pwd, '')
        self.capellaAPIv2 = CapellaAPIv2(self.pod.url_public, self.tenant.api_secret_key,
                                         self.tenant.api_access_key, self.tenant.user,
                                         self.tenant.pwd)
        self.columnarAPI = ColumnarAPI(self.pod.url_public, '', '', self.tenant.user,
                                       self.tenant.pwd, '')
        self.columnar_utils = ColumnarUtils(self.log)
        self.columnar_rbac_util = ColumnarRBACUtil(self.log)
        self.columnar_cbas_utils = columnarCBASUtil(
            self.task, self.use_sdk_for_cbas)
        self.project_roles = ["projectOwner", "projectDataWriter", "projectClusterViewer",
                              "projectClusterManager", "projectDataViewer"]
        self.ACCESS_DENIED_ERR = "Insufficient permissions or the requested object does not exist"
        self.database_privileges = ["database_create", "database_drop"]
        self.scope_privileges = ["scope_create", "scope_drop"]
        self.collection_ddl_privileges = ["collection_create", "collection_drop"]
        self.collection_dml_privileges = ["collection_insert", "collection_upsert",
                                          "collection_delete", "collection_analyze",
                                          "collection_select"]
        self.link_ddl_privileges = ["link_create", "link_drop", "link_alter"]
        self.link_connection_privileges = ["link_connect", "link_disconnect"]
        self.link_dml_privileges = ["link_copy_to", "link_copy_from"]
        self.synonym_privileges = ["synonym_create", "synonym_drop"]
        self.index_privileges = ["index_create", "index_drop"]
        self.udf_ddl_privileges = ["function_create", "function_drop"]
        self.udf_execute_privileges = ["function_execute"]
        self.view_ddl_privileges = ["view_create", "view_drop"]
        self.view_select_privileges = ["view_select"]

        # if none provided use 1 Kb doc size
        self.doc_size = self.input.param("doc_size", 1000)
        self.cluster_on_off = self.input.param("cluster_on_off", False)
        self.cluster_backup_restore = self.input.param("cluster_backup_restore", False)
        self.scale_cluster = self.input.param("scale_cluster", False)

        if not self.columnar_spec_name:
            self.columnar_spec_name = "regressions.copy_to_s3"

        self.columnar_spec = self.cbas_util.get_columnar_spec(
            self.columnar_spec_name)

        self.aws_region = "us-west-1"
        self.s3_source_bucket = "columnar-sanity-test-data-mohsin"
        self.sink_s3_bucket_name = None

        self.create_different_organization_roles()

    @staticmethod
    def generate_random_string(length=10, special_characters=True,
                               prefix=""):
        """
        Generates random name of specified length
        """
        if special_characters:
            special_characters = "!@#$%^&*()-_=+{[]}\|;:'\",.<>/?" + " " + "\t"
        else:
            special_characters = ""

        characters = string.ascii_letters + string.digits + special_characters
        name = ""
        for i in range(length):
            name += random.choice(characters)

        if prefix:
            name = prefix + name

        return name

    def create_different_organization_roles(self):
        self.log.info("Creating Different Organization Roles")
        self.test_users = {}
        roles = ["organizationOwner", "projectCreator", "organizationMember"]
        setup_capella_api = CapellaAPIv2(self.pod.url_public, self.tenant.api_secret_key,
                                         self.tenant.api_access_key, self.tenant.user,
                                         self.tenant.pwd)

        num = 1
        for role in roles:
            usrname = self.tenant.user.split('@')
            username = usrname[0] + "+" + self.generate_random_string(9, False) + "@" + usrname[1]
            name = "Test_User_" + str(num)
            self.log.info("Creating user {} with role {}".format(username, role))
            create_user_resp = setup_capella_api.create_user(self.tenant.id,
                                                             name,
                                                             username,
                                                             "Password@123",
                                                             [role])
            self.log.info("User creation response - {}".format(create_user_resp.content))
            if create_user_resp.status_code == 200:
                self.log.info("User {} created successfully".format(username))
                self.test_users["User" + str(num)] = {
                    "name": create_user_resp.json()["data"]["name"],
                    "mailid": create_user_resp.json()["data"]["email"],
                    "role": role,
                    "password": "Password@123",
                    "userid": create_user_resp.json()["data"]["id"]
                }

            elif create_user_resp.status_code == 422:
                msg = "is already in use. Please sign-in."
                if msg in create_user_resp.json()["message"]:
                    self.log.info("User is already in use. Please sign-in")
                    num = num + 1
                    continue
                else:
                    self.fail("Not able to create user. Reason -".format(create_user_resp.content))

            else:
                self.fail("Not able to create user. Reason -".format(create_user_resp.content))

            num = num + 1

    def delete_different_organization_roles(self):
        self.log.info("Deleting different Organization Roles")
        for user in self.test_users:
            user_id = self.test_users[user]["userid"]
            self.log.info("Deleting user from organization. User Id: {}".format(user_id))

            resp = self.capellaAPIv2.delete_user(self.tenant.id,
                                                 user_id)

            if resp.status_code != 204:
                self.log.info("Failed to delete user with user id: {}. Reason: {} {}".format(
                    user_id, resp.json()["message"], resp.json()["hint"]))
                raise Exception("Failed to delete user")

        self.log.info("Deleted all the Organization Roles successfully")

    def get_remote_cluster_certificate(self, remote_cluster):
        remote_cluster_certificate_request = (
            self.capellaAPIv4.cluster_ops_apis.get_cluster_certificate(self.tenant.id, self.tenant.project_id,
                                                                       remote_cluster.id))
        if remote_cluster_certificate_request.status_code == 200:
            remote_cluster_certificate = (remote_cluster_certificate_request.json()["certificate"])
        else:
            self.fail("Failed to get cluster certificate")

        return remote_cluster_certificate

    def create_s3_sink_bucket(self):
        for i in range(5):
            try:
                self.sink_s3_bucket_name = "copy-to-s3-mohsin-" + str(random.randint(1, 100000))
                self.log.info("Creating S3 bucket for : {}".format(self.sink_s3_bucket_name))
                self.sink_bucket_created = perform_S3_operation(
                    aws_access_key=self.aws_access_key,
                    aws_secret_key=self.aws_secret_key,
                    aws_session_token=self.aws_session_token,
                    create_bucket=True, bucket_name=self.sink_s3_bucket_name,
                    region=self.aws_region)
                break
            except Exception as e:
                self.log.error("Creating S3 bucket - {0} in region {1} failed".format(self.sink_s3_bucket_name,
                                                                                      self.aws_region))
                self.log.error(str(e))
            finally:
                if i == 4:
                    self.sink_s3_bucket_name = None
                    self.fail("Unable to create S3 bucket even after 5 retries")

    def delete_s3_bucket(self):
        """
            Delete S3 bucket created for copying files
            """
        for i in range(5):
            try:
                if perform_S3_operation(
                        aws_access_key=self.aws_access_key,
                        aws_secret_key=self.aws_secret_key,
                        aws_session_token=self.aws_session_token,
                        empty_bucket=True,
                        bucket_name=self.sink_s3_bucket_name,
                        region=self.aws_region):
                    break
            except Exception as e:
                self.log.error("Unable to empty S3 bucket - {0}".format(
                    self.sink_s3_bucket_name))
                self.log.error(str(e))
            finally:
                if i == 4:
                    self.fail("Unable to empty S3 bucket even after 5 "
                              "retries")

        # Delete the created S3 bucket
        self.log.info("Deleting AWS S3 bucket - {0}".format(
            self.sink_s3_bucket_name))

        for i in range(5):
            try:
                if perform_S3_operation(
                        aws_access_key=self.aws_access_key,
                        aws_secret_key=self.aws_secret_key,
                        aws_session_token=self.aws_session_token,
                        delete_bucket=True,
                        bucket_name=self.sink_s3_bucket_name,
                        region=self.aws_region):
                    break
            except Exception as e:
                self.log.error("Unable to delete S3 bucket - {0}".format(
                    self.sink_s3_bucket_name))
                self.log.error(str(e))
            finally:
                if i == 4:
                    self.fail("Unable to delete S3 bucket even after 5 "
                              "retries")

    def build_cbas_columnar_infra(self):

        self.columnar_spec["database"]["no_of_databases"] = self.input.param(
            "no_of_databases", 1)
        self.columnar_spec["dataverse"]["no_of_dataverses"] = self.input.param(
            "no_of_scopes", 0)

        self.columnar_spec["remote_link"]["no_of_remote_links"] = self.input.param(
            "no_of_remote_links", 0)

        resp = (self.capellaAPIv4.create_control_plane_api_key(self.tenant.id, 'init api keys')).json()
        self.capellaAPIv4.cluster_ops_apis.SECRET = resp['secretKey']
        self.capellaAPIv4.cluster_ops_apis.ACCESS = resp['id']
        self.capellaAPIv4.cluster_ops_apis.bearer_token = resp['token']
        self.capellaAPIv4.org_ops_apis.SECRET = resp['secretKey']
        self.capellaAPIv4.org_ops_apis.ACCESS = resp['id']
        self.capellaAPIv4.org_ops_apis.bearer_token = resp['token']
        resp = self.capellaAPIv4.cluster_ops_apis.add_CIDR_to_allowed_CIDRs_list(self.tenant.id,
                                                                                 self.tenant.project_id,
                                                                                 self.remote_cluster.id, "0.0.0.0/0")
        if resp.status_code == 201 or resp.status_code == 422:
            self.log.info("Added allowed IP 0.0.0.0/0")
        else:
            self.fail("Failed to add allowed IP")

        self.create_bucket_scopes_collections_in_capella_cluster(self.tenant, self.remote_cluster)

        for bucket in self.remote_cluster.buckets:
            for scope_name, scope in bucket.scopes.items():
                if scope_name != "_system":
                    for collection_name, collection in scope.collections.items():
                        self.remote_bucket = bucket
                        self.remote_scope = scope
                        self.remote_collection = collection
                        break

        remote_cluster_certificate = self.get_remote_cluster_certificate(self.remote_cluster)
        if self.input.param("no_of_remote_links", 0):
            remote_link_properties = list()
            remote_link_properties.append(
                {"type": "couchbase", "hostname": str(self.remote_cluster.srv),
                 "username": self.rest_username,
                 "password": self.rest_password,
                 "encryption": "full",
                 "certificate": remote_cluster_certificate})
            self.columnar_spec["remote_link"]["properties"] = remote_link_properties
            self.columnar_spec["remote_dataset"]["num_of_remote_datasets"] = self.input.param("num_of_remote_coll", 0)

        # Update External Links Spec here
        self.columnar_spec["external_link"][
            "no_of_external_links"] = self.input.param(
            "no_of_external_links", 0)
        self.columnar_spec["external_link"]["properties"] = [{
            "type": "s3",
            "region": self.aws_region,
            "accessKeyId": self.aws_access_key,
            "secretAccessKey": self.aws_secret_key,
            "serviceEndpoint": None
        }]

        self.columnar_spec["standalone_dataset"][
            "num_of_standalone_coll"] = self.input.param(
            "num_of_standalone_coll", 0)
        self.columnar_spec["standalone_dataset"]["primary_key"] = [{"name": "string", "email": "string"}]

        # Update Kafka Links Spec here

        # Update External Datasets Spec here
        self.columnar_spec["external_dataset"]["num_of_external_datasets"] = self.input.param("num_of_external_coll", 0)
        if self.input.param("num_of_external_coll", 0):
            external_dataset_properties = [{
                "external_container_name": self.s3_source_bucket,
                "path_on_external_container": None,
                "file_format": self.input.param("file_format", "json"),
                "include": ["*.{0}".format(self.input.param("file_format", "json"))],
                "exclude": None,
                "region": self.aws_region,
                "object_construction_def": None,
                "redact_warning": None,
                "header": None,
                "null_string": None,
                "parse_json_string": 0,
                "convert_decimal_to_double": 0,
                "timezone": ""
            }]
            self.columnar_spec["external_dataset"][
                "external_dataset_properties"] = external_dataset_properties

        # Update Kafka Datasets Spec here.

        # Update Synonym Spec here
        self.columnar_spec["synonym"]["no_of_synonyms"] = 0

        # Update Index Spec here
        self.columnar_spec["index"]["no_of_indexes"] = 0
        self.columnar_spec["index"]["indexed_fields"] = []

        result, error = self.columnar_cbas_utils.create_cbas_infra_from_spec(
            self.cluster, self.columnar_spec, self.bucket_util, False)

        if not result:
            self.fail("Error while creating analytics entities: {}".format(error))

    def wait_for_off(self, timeout=900):
        status = None
        start_time = time.time()
        while status != 'turning_off' and time.time() < start_time + 300:
            resp = self.columnar_utils.get_instance_info(self.pod, self.tenant, self.tenant.project_id,
                                                         self.cluster.instance_id)
            status = resp["data"]["state"]
        while status == 'turning_off' and time.time() < start_time + timeout:
            try:
                resp = self.columnar_utils.get_instance_info(self.pod, self.tenant, self.tenant.project_id,
                                                             self.cluster.instance_id)
                status = resp["data"]["state"]
                self.log.info("Instance is still turning off")
                time.sleep(20)
            except Exception as e:
                self.log.error(str(e))
        if status == "turned_off":
            self.log.info("Instance off successful")
            return True
        else:
            self.log.error("Failed to turn off the instance")
            return False

    def wait_for_on(self, timeout=900):
        status = None
        start_time = time.time()
        while status != 'turning_on' and time.time() < start_time + 300:
            resp = self.columnar_utils.get_instance_info(self.pod, self.tenant, self.tenant.project_id,
                                                         self.cluster.instance_id)
            status = resp["data"]["state"]
        while status == 'turning_on' and time.time() < start_time + timeout:
            try:
                resp = self.columnar_utils.get_instance_info(self.pod, self.tenant, self.tenant.project_id,
                                                             self.cluster.instance_id)
                status = resp["data"]["state"]
                self.log.info("Instance is still turning on")
                time.sleep(20)
            except Exception as e:
                self.log.error(str(e))
        if status == "healthy":
            self.log.info("Instance on successful")
            return True
        else:
            self.log.error("Failed to turn on the instance")
            return False

    def scale_columnar_cluster(self, nodes, timeout=900):
        start_time = time.time()
        status = None
        resp = self.columnarAPI.update_columnar_instance(self.tenant.id,
                                                         self.tenant.project_id,
                                                         self.cluster.instance_id,
                                                         self.cluster.name, '', nodes)
        if resp.status_code != 202:
            self.fail("Failed to scale cluster")
            # check for nodes in the cluster
        while status != "healthy" and start_time + timeout > time.time():
            resp = self.columnarAPI.get_specific_columnar_instance(self.tenant.id,
                                                                   self.tenant.project_id,
                                                                   self.cluster.instance_id)
            resp = resp.json()
            status = resp["data"]["state"]
        if time.time() > start_time + timeout:
            self.fail("Cluster state is {} after 15 minutes".format(status))

    def get_backup_from_backup_lists(self, backup_id=None):
        """
        Returns backup info for a backup id
        Returns all backups for instance if backup_id is None
        Parameters:
            backup_id: Optional, fetch the backup info for backup_id
        """
        resp = self.columnarAPI.list_backups(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        backup_list = resp.json()["data"]
        if not backup_id:
            return backup_list
        else:
            for backup in backup_list:
                if backup["data"]["id"] == backup_id:
                    return backup["data"]
        return -1

    def get_restore_from_restore_list(self, backup_id, restore_id=None):
        """
        Returns restore info for a restore id
        Returns all restore for instance if restore is None
        Parameters:
            backup_id: fetch the backup info for backup_id
            restore_id: Optional, only provide when to fetch specific restore
        """
        resp = self.columnarAPI.list_restores(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        restore_list = resp.json()["data"]
        if not restore_id:
            return restore_list
        else:
            for restore in restore_list:
                if restore["data"]["id"] == restore_id:
                    return restore["data"]
        return -1

    def wait_for_backup_complete(self, backup_id, timeout=3600):
        start_time = time.time()
        backup_state = None
        while backup_state != "complete" and time.time() < start_time + timeout:
            backup_state = self.get_backup_from_backup_lists(backup_id)
            if backup_state == -1:
                self.fail("Backup with backup id: {0}, Not found".format(backup_id))
            backup_state = self.get_backup_from_backup_lists(backup_id)["progress"]["status"]
            self.log.info("Waiting for backup to be completed, current state: {}".format(backup_state))
            time.sleep(60)
        if backup_state != "complete":
            self.fail("Failed to create backup with timeout of {}".format(timeout))
        else:
            self.log.info("Successfully created backup in {} seconds".format(time.time() - start_time))
            return True

    def create_backup_wait_for_complete(self, retention=None, timeout=3600):
        if retention:
            resp = self.columnarAPI.create_backup(self.tenant.id, self.tenant.project_id, self.cluster.instance_id,
                                                  retention)
        else:
            resp = self.columnarAPI.create_backup(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
        backup_id = resp.json()["id"]
        self.log.info("Backup Id: {}".format(backup_id))

        # wait for backup to complete
        self.wait_for_backup_complete(backup_id, timeout)
        return backup_id

    def restore_wait_for_complete(self, backup_id, timeout=3600):
        resp = self.columnarAPI.create_restore(self.tenant.id, self.tenant.project_id, self.cluster.instance_id,
                                               backup_id)
        restore_id = resp.json()["id"]
        start_time = time.time()
        self.log.info("Restore Id: {}".format(restore_id))
        restore_state = None
        while restore_state != "complete" and time.time() < start_time + timeout:
            restore_state = self.get_restore_from_restore_list(backup_id, restore_id)["status"]
            if restore_state == -1:
                self.fail("Restore id: {0} not found for backup id: {1}".format(restore_id, backup_id))
            self.log.info("Waiting for restore to complete, current status {0}".format(restore_state))
            time.sleep(60)
        if restore_state != "complete":
            self.fail("Fail to restore backup with timeout of {}".format(timeout))
        else:
            self.log.info("Successfully restored backup in {} seconds".format(time.time() - start_time))

    def wait_for_instance_to_be_healthy(self, timeout=600):
        status = None
        start_time = time.time()
        while status != "healthy" and time.time() < start_time + timeout:
            resp = self.columnarAPI.get_specific_columnar_instance(self.tenant.id,
                                                                   self.tenant.project_id,
                                                                   self.cluster.instance_id)
            resp = resp.json()
            status = resp["data"]["state"]
            self.log.info("Instance state: {}".format(status))
            time.sleep(30)
        if status != "healthy":
            self.fail("Instance failed to be healthy")
        else:
            self.log.info("Instance is in healthy state")

    def generate_random_password(self, length=12):
        """Generate a random password."""
        password_characters = string.ascii_letters + string.digits
        password = ''.join(random.choice(password_characters) for i in range(length))
        password += "!123Aa"
        return password

    def generate_random_entity_name(self, length=5, type="database"):
        """Generate random database name."""
        base_name = "TAF-" + type
        entity_id = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(length))
        entity_name = base_name + "-" + entity_id
        return entity_name

    def generate_test_case(self, user, privileges, resources, validate_err_msg=False,
                           denied_resources=[]):

        test_case = {}

        test_case["user"] = user
        test_case["privileges"] = privileges
        test_case["description"] = "Test {} on {} for user {}".format(','.join(privileges),
                                                                      ','.join(resources),
                                                                      user)
        test_case["resources"] = resources
        test_case["validate_err_msg"] = validate_err_msg
        if validate_err_msg:
            test_case["expected_err_msg"] = self.ACCESS_DENIED_ERR
        else:
            test_case["expected_err_msg"] = None

        return test_case

    def create_rbac_testcases(self, privileges=[], resources=[],
                              resource_type="instance", extended_res_priv_map=[]):

        def find_child_resources(parent_resources, child_resources):
            result = []

            for parent in parent_resources:
                matching_children = [child for child in child_resources if child.startswith(parent)]
                result.extend(matching_children)

            return result

        testcases = []
        if len(resources) == 0:
            resources.append("")

        for idx, res in enumerate(resources):
            resources[idx] = self.columnar_cbas_utils.unformat_name(res)

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        resource_priv_map = []
        for res in resources:
            res_priv_obj = {
                "name": res,
                "type": resource_type,
                "privileges": privileges
            }
            resource_priv_map.append(res_priv_obj)
        resource_priv_map.extend(extended_res_priv_map)
        privileges_payload = self.columnar_rbac_util.create_privileges_payload(resource_priv_map)
        user1 = self.columnar_rbac_util.create_api_keys(self.pod, self.tenant,
                                                        self.tenant.project_id, self.cluster,
                                                        username=username,
                                                        password=password,
                                                        privileges_payload=privileges_payload)
        test_case = self.generate_test_case(user1, privileges, resources)
        testcases.append(test_case)

        role_name = self.generate_random_entity_name(type="role")
        role1 = self.columnar_rbac_util.create_columnar_role(self.pod, self.tenant,
                                                             self.tenant.project_id, self.cluster,
                                                             role_name=role_name,
                                                             privileges_payload=privileges_payload)
        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        user2 = self.columnar_rbac_util.create_api_keys(self.pod, self.tenant,
                                                        self.tenant.project_id, self.cluster,
                                                        username=username,
                                                        password=password,
                                                        role_ids=[role1.id])
        test_case = self.generate_test_case(user2, privileges, resources)
        testcases.append(test_case)

        resource_priv_map = []
        test_resource = resources[0]
        if len(privileges) > 1:

            test_resource = resources[-1]
            for res in resources[:-1]:
                res_priv_obj = {
                    "name": res,
                    "type": resource_type,
                    "privileges": privileges
                }
                resource_priv_map.append(res_priv_obj)
        resource_priv_map.extend(extended_res_priv_map)
        privileges_payload = self.columnar_rbac_util.create_privileges_payload(resource_priv_map)

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        user3 = self.columnar_rbac_util.create_api_keys(self.pod, self.tenant,
                                                        self.tenant.project_id, self.cluster,
                                                        username=username,
                                                        password=password,
                                                        privileges_payload=privileges_payload)
        negative_test_case = self.generate_test_case(user3, privileges, [test_resource],
                                                     validate_err_msg=True)
        testcases.append(negative_test_case)

        role_name = self.generate_random_entity_name(type="role")
        role2 = self.columnar_rbac_util.create_columnar_role(self.pod, self.tenant,
                                                             self.tenant.project_id, self.cluster,
                                                             role_name=role_name,
                                                             privileges_payload=privileges_payload)
        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        user4 = self.columnar_rbac_util.create_api_keys(self.pod, self.tenant,
                                                        self.tenant.project_id, self.cluster,
                                                        username=username,
                                                        password=password,
                                                        role_ids=[role2.id])
        negative_test_case = self.generate_test_case(user4, privileges, [test_resource],
                                                     validate_err_msg=True)
        testcases.append(negative_test_case)

        additional_test_cases = []
        if resource_type in ["database", "link"]:
            additional_test_cases = self.create_rbac_testcases(privileges, [], "instance",
                                                               extended_res_priv_map=extended_res_priv_map)

        elif resource_type == "scope":
            databases = []
            for res in resources:
                db_name = res.split(".")[0]
                if db_name not in databases:
                    databases.append(db_name)
            additional_test_cases = self.create_rbac_testcases(privileges, databases, "database",
                                                               extended_res_priv_map=extended_res_priv_map)

        elif resource_type in ["collection", "function", "view"]:
            scopes = []
            for res in resources:
                scope_name = ".".join(res.split('.')[:2])
                if scope_name not in scopes:
                    scopes.append(scope_name)
            additional_test_cases = self.create_rbac_testcases(privileges, scopes, "scope",
                                                               extended_res_priv_map=extended_res_priv_map)

        for test_case in additional_test_cases:
            parent_resources = test_case['resources']
            child_resources = find_child_resources(parent_resources, resources)
            test_case['resources'] = child_resources

        testcases.extend(additional_test_cases)

        return testcases

    def generate_external_dataset_cmd(self, dataset_obj):

        cmd = self.columnar_cbas_utils.generate_create_external_dataset_cmd(
                dataset_obj.name,
                dataset_obj.dataset_properties[
                    "external_container_name"],
                dataset_obj.link_name, False,
                dataset_obj.dataverse_name,
                dataset_obj.database_name,
                dataset_obj.dataset_properties[
                    "object_construction_def"],
                None,
                dataset_obj.dataset_properties[
                    "file_format"],
                dataset_obj.dataset_properties[
                    "redact_warning"],
                dataset_obj.dataset_properties[
                    "header"],
                dataset_obj.dataset_properties[
                    "null_string"],
                dataset_obj.dataset_properties[
                    "include"],
                dataset_obj.dataset_properties[
                    "exclude"],
                dataset_obj.dataset_properties[
                    "parse_json_string"],
                dataset_obj.dataset_properties[
                    "convert_decimal_to_double"],
                dataset_obj.dataset_properties[
                    "timezone"]
        )

        return cmd

    def create_external_dataset(self, dataset_obj, validate_error_msg=False,
                                expected_error=None, username=None, password=None):

        if self.columnar_cbas_utils.create_dataset_on_external_resource(
                self.cluster, dataset_obj.name,
                dataset_obj.dataset_properties[
                    "external_container_name"],
                dataset_obj.link_name, False,
                dataset_obj.dataverse_name,
                dataset_obj.database_name,
                dataset_obj.dataset_properties[
                    "object_construction_def"],
                None,
                dataset_obj.dataset_properties[
                    "file_format"],
                dataset_obj.dataset_properties[
                    "redact_warning"],
                dataset_obj.dataset_properties[
                    "header"],
                dataset_obj.dataset_properties[
                    "null_string"],
                dataset_obj.dataset_properties[
                    "include"],
                dataset_obj.dataset_properties[
                    "exclude"],
                dataset_obj.dataset_properties[
                    "parse_json_string"],
                dataset_obj.dataset_properties[
                    "convert_decimal_to_double"],
                dataset_obj.dataset_properties[
                    "timezone"],
                validate_error_msg=validate_error_msg,
                username=username,
                password=password,
                expected_error=expected_error,
                timeout=300,
                analytics_timeout=300):
            return True
        return False

    def test_rbac_database(self):
        self.log.info("RBAC test for database started")

        testcases = self.create_rbac_testcases(self.database_privileges, [],
                                               "instance")

        if self.cluster_on_off:
            resp = self.columnarAPI.turn_off_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
            if resp.status_code == 202:
                self.log.info("Started turning off instance")
            else:
                self.fail("API failed to turn off the instance with status code: {}".format(resp.status_code))
            if not self.wait_for_off():
                self.fail("Failed to turn off instance")

            # resume the instance
            resp = self.columnarAPI.turn_on_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
            if resp.status_code == 202:
                self.log.info("Started turning on instance")
            else:
                self.fail("API Failed to turn on instance with status code : {}".format(resp.status_code))
            if not self.wait_for_on():
                self.fail("Failed to turn on the instance")

        if self.cluster_backup_restore:
            backup_id = self.create_backup_wait_for_complete()
            self.restore_wait_for_complete(backup_id)
            self.wait_for_instance_to_be_healthy()
            self.columnar_utils.allow_ip_on_instance(self.pod, self.tenant, self.tenant.project_id, self.cluster)

        if self.scale_cluster:
            self.scale_columnar_cluster(4)

        for idx, test_case in enumerate(testcases):
            self.log.info("========== TEST CASE {} ===========".format(idx))
            self.log.info(test_case['description'])
            if test_case['validate_err_msg']:
                self.log.info("EXPECTED: ERROR")
            else:
                self.log.info("EXPECTED: SUCCESS")

            privileges = test_case['privileges']

            for priv in privileges:
                if priv == "database_create":
                    database_name = self.generate_random_entity_name(type="database")
                    expected_error = self.ACCESS_DENIED_ERR
                    result = self.columnar_cbas_utils.create_database(
                        self.cluster,
                        database_name,
                        username=test_case['user'].username,
                        password=test_case['user'].password,
                        validate_error_msg=test_case['validate_err_msg'],
                        expected_error=expected_error)
                    if not result:
                        self.fail("Test case failed while attempting to create database")

                elif priv == "DROP":
                    database_name = self.generate_random_entity_name(type="database")
                    expected_error = self.ACCESS_DENIED_ERR
                    result = self.columnar_cbas_utils.create_database(self.cluster,
                                                                      database_name)
                    if not result:
                        self.fail("Failed to create database")
                    result = self.columnar_cbas_utils.drop_database(
                        self.cluster,
                        database_name,
                        username=test_case['user'].id,
                        password=test_case['user'].password,
                        validate_error_msg=test_case['validate_err_msg'],
                        expected_error=expected_error)
                    if not result:
                        self.fail("Test case failed while attempting to drop database")

        self.log.info("Testing for cloud roles")
        create_database_cmd = "CREATE DATABASE {};"
        drop_database_cmd = "DROP DATABASE {};"
        for user in self.test_users:
            self.log.info("========== CLOUD USER TEST CASE: {} ===========".
                          format(self.test_users[user]["role"]))
            self.columnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                               self.test_users[user]["mailid"],
                                               self.test_users[user]["password"])

            for priv in self.database_privileges:
                if priv == "database_create":
                    database_name = self.generate_random_entity_name(type="database")
                    database_name = self.columnar_cbas_utils.format_name(database_name)
                    execute_cmd = create_database_cmd.format(database_name)
                elif priv == "database_drop":
                    database_name = self.generate_random_entity_name(type="database")
                    database_name = self.columnar_cbas_utils.format_name(database_name)
                    result = self.columnar_cbas_utils.create_database(self.cluster,
                                                                      database_name)
                    if not result:
                        self.fail("Failed to create database")
                    execute_cmd = drop_database_cmd.format(database_name)
                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.cluster.instance_id,
                                                              execute_cmd)
                if self.test_users[user]["role"] == "organizationOwner":
                    self.assertEqual(200, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}'\
                                         'For role: {}'.format(resp.status_code, 200,
                                                               self.test_users[user]["role"]))
                else:
                    self.assertEqual(403, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 403,
                                                               self.test_users[user]["role"]))

        user = self.test_users['User3']
        for role in self.project_roles:
            self.log.info("========== CLOUD USER TEST CASE: {} ===========".
                          format(role))
            self.log.info(
                "Adding user to project {} with role as {}".format(self.tenant.project_id,
                                                                   role))
            payload = {
                "resourceId": self.tenant.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }

            resp = self.capellaAPIv2.add_user_to_project(self.tenant.id,
                                                         json.dumps(payload))
            self.assertEqual(200, resp.status_code,
                             msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code,
                                                                         200))
            self.columnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                               user["mailid"],
                                               user["password"])
            for priv in self.database_privileges:
                if priv == "database_create":
                    database_name = self.generate_random_entity_name(type="database")
                    database_name = self.columnar_cbas_utils.format_name(database_name)
                    execute_cmd = create_database_cmd.format(database_name)
                elif priv == "database_drop":
                    database_name = self.generate_random_entity_name(type="database")
                    database_name = self.columnar_cbas_utils.format_name(database_name)
                    result = self.columnar_cbas_utils.create_database(self.cluster,
                                                                      database_name)
                    if not result:
                        self.fail("Failed to create database")
                    execute_cmd = drop_database_cmd.format(database_name)
                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.cluster.instance_id,
                                                              execute_cmd)
                if role == "projectOwner" or role == "projectDataWriter":
                    self.assertEqual(200, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 200, role))
                else:
                    self.assertEqual(403, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 403, role))

    def test_rbac_deleted_api_keys(self):
        testcases = self.create_rbac_testcases(self.database_privileges[:1], [],
                                               "instance")

        for idx, test_case in enumerate(testcases):
            user = test_case['user']
            self.log.info("Deleting API keys with id: {}, username: {}".
                          format(user.id, user.username))
            result = self.columnar_rbac_util.delete_api_keys(self.pod, self.tenant,
                                                             self.tenant.project_id,
                                                             self.cluster,
                                                             user.id)
            if not result:
                self.log.error("Failed to delete user {}".format(user.username))

            database_name = self.generate_random_entity_name(type="database")
            result = self.columnar_cbas_utils.create_database(
                self.cluster,
                database_name,
                username=user.username,
                password=user.password,
                validate_error_msg=True,
                expected_error="Unauthorized user.")
            if not result:
                self.fail("Test case failed while attempting to create database")

    def test_rbac_scope(self):
        self.log.info("RBAC test for scope started")
        self.build_cbas_columnar_infra()

        databases = self.columnar_cbas_utils.get_all_databases_from_metadata(self.cluster)

        testcases = self.create_rbac_testcases(self.scope_privileges, databases,
                                               "database")

        if self.cluster_on_off:
            resp = self.columnarAPI.turn_off_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
            if resp.status_code == 202:
                self.log.info("Started turning off instance")
            else:
                self.fail("API failed to turn off the instance with status code: {}".format(resp.status_code))
            if not self.wait_for_off():
                self.fail("Failed to turn off instance")

            # resume the instance
            resp = self.columnarAPI.turn_on_instance(self.tenant.id, self.tenant.project_id, self.cluster.instance_id)
            if resp.status_code == 202:
                self.log.info("Started turning on instance")
            else:
                self.fail("API Failed to turn on instance with status code : {}".format(resp.status_code))
            if not self.wait_for_on():
                self.fail("Failed to turn on the instance")

        for idx, test_case in enumerate(testcases):
            self.log.info("========== TEST CASE {} ===========".format(idx))
            self.log.info(test_case['description'])
            if test_case['validate_err_msg']:
                self.log.info("EXPECTED: ERROR")
            else:
                self.log.info("EXPECTED: SUCCESS")

            privileges = test_case['privileges']
            resources = test_case['resources']

            for priv in privileges:
                for res in resources:
                    scope_name = self.generate_random_entity_name(type="scope")

                    if priv == "scope_create":
                        expected_error = self.ACCESS_DENIED_ERR
                        result = self.columnar_cbas_utils.create_scope(
                            self.cluster, scope_name,
                            res,
                            username=test_case['user'].username,
                            password=test_case['user'].password,
                            validate_error_msg=test_case['validate_err_msg'],
                            expected_error=expected_error)
                        if not result:
                            self.fail("Test case failed while attempting to create scope {}".format(
                                scope_name))
                    elif priv == "scope_drop":
                        expected_error = self.ACCESS_DENIED_ERR
                        result = self.columnar_cbas_utils.create_dataverse(self.cluster,
                                                                           scope_name,
                                                                           res)
                        if not result:
                            self.fail("Failed to create scope {}".format(scope_name))
                        result = self.columnar_cbas_utils.drop_dataverse(
                            self.cluster,
                            scope_name,
                            res,
                            username=test_case['user'].username,
                            password=test_case['user'].password,
                            validate_error_msg=test_case['validate_err_msg'],
                            expected_error=expected_error)
                        if not result:
                            self.fail("Test case failed while attempting to drop scope {}".format(
                                scope_name))

        self.log.info("Testing for cloud roles")
        create_scope_cmd = "CREATE SCOPE {};"
        drop_scope_cmd = "DROP SCOPE {};"
        for user in self.test_users:
            self.log.info("========== CLOUD USER TEST CASE: {} ===========".
                          format(self.test_users[user]["role"]))
            self.columnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                               self.test_users[user]["mailid"],
                                               self.test_users[user]["password"])
            for priv in self.scope_privileges:
                if priv == "scope_create":
                    database_name = random.choice(databases)
                    scope_name = self.generate_random_entity_name(type="scope")
                    scope_name = "{}.{}".format(database_name, scope_name)
                    scope_name = self.columnar_cbas_utils.format_name(scope_name)
                    execute_cmd = create_scope_cmd.format(scope_name)
                elif priv == "scope_drop":
                    database_name = random.choice(databases)
                    scope_name = self.generate_random_entity_name(type="scope")
                    result = self.columnar_cbas_utils.create_dataverse(self.cluster,
                                                                       scope_name,
                                                                       database_name)
                    if not result:
                        self.fail("Failed to create scope {}".format(scope_name))
                    scope_name = "{}.{}".format(database_name, scope_name)
                    scope_name = self.columnar_cbas_utils.format_name(scope_name)
                    execute_cmd = drop_scope_cmd.format(scope_name)

                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.cluster.instance_id,
                                                              execute_cmd)
                if self.test_users[user]["role"] == "organizationOwner":
                    self.assertEqual(200, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code,
                                                                                 200))
                else:
                    self.assertEqual(403, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 403,
                                                               self.test_users[user]["role"]))

        user = self.test_users['User3']
        for role in self.project_roles:
            self.log.info("========== CLOUD USER TEST CASE: {} ===========".
                          format(role))
            self.log.info(
                "Adding user to project {} with role as {}".format(self.tenant.project_id,
                                                                   role))
            payload = {
                "resourceId": self.tenant.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }

            resp = self.capellaAPIv2.add_user_to_project(self.tenant.id,
                                                         json.dumps(payload))
            self.assertEqual(200, resp.status_code,
                             msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code,
                                                                         200))

            self.columnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                               user["mailid"],
                                               user["password"])
            for priv in self.scope_privileges:
                if priv == "scope_create":
                    database_name = random.choice(databases)
                    scope_name = self.generate_random_entity_name(type="scope")
                    scope_name = "{}.{}".format(database_name, scope_name)
                    scope_name = self.columnar_cbas_utils.format_name(scope_name)
                    execute_cmd = create_scope_cmd.format(scope_name)
                elif priv == "scope_drop":
                    database_name = random.choice(databases)
                    scope_name = self.generate_random_entity_name(type="scope")
                    result = self.columnar_cbas_utils.create_dataverse(self.cluster,
                                                                       scope_name,
                                                                       database_name)
                    if not result:
                        self.fail("Failed to create scope {}".format(scope_name))
                    scope_name = "{}.{}".format(database_name, scope_name)
                    scope_name = self.columnar_cbas_utils.format_name(scope_name)
                    execute_cmd = drop_scope_cmd.format(scope_name)
                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.cluster.instance_id,
                                                              execute_cmd)
                if role == "projectOwner" or role == "projectDataWriter":
                    self.assertEqual(200, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 200, role))
                else:
                    self.assertEqual(403, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 403, role))

    def test_rbac_collection_ddl(self):
        self.log.info("RBAC collection ddl test started")
        self.build_cbas_columnar_infra()

        scopes = self.columnar_cbas_utils.get_all_dataverses_from_metadata(self.cluster)

        extended_res_priv_map = []
        create_collection_priv = {
            "name": "",
            "type": "instance",
            "privileges": ["link_create_collection"]
        }
        extended_res_priv_map.append(create_collection_priv)
        testcases = self.create_rbac_testcases(self.collection_ddl_privileges, scopes,
                                               resource_type="scope",
                                               extended_res_priv_map=extended_res_priv_map)

        for idx, test_case in enumerate(testcases):
            self.log.info("========== TEST CASE {} ===========".format(idx))
            self.log.info(test_case['description'])
            if test_case['validate_err_msg']:
                self.log.info("EXPECTED: ERROR")
            else:
                self.log.info("EXPECTED: SUCCESS")

            privileges = test_case['privileges']
            resources = test_case['resources']

            for priv in privileges:
                for res in resources:
                    remote_links = self.columnar_cbas_utils.get_all_link_objs("couchbase")
                    remote_link = None
                    for rm_link in remote_links:
                        if res in rm_link.name:
                            remote_link = rm_link
                            break

                    database_name = res.split(".")[0]
                    scope_name = res.split(".")[1]

                    if priv == "collection_create":

                        # create remote collection.
                        self.log.info("Test creating remote collection")
                        remote_coll_obj = self.columnar_cbas_utils.create_remote_dataset_obj(
                            self.cluster,
                            self.remote_bucket,
                            self.remote_scope,
                            self.remote_collection,
                            remote_link,
                            use_only_existing_db=True,
                            use_only_existing_dv=True,
                            database=database_name,
                            dataverse=scope_name)[0]
                        expected_error = self.ACCESS_DENIED_ERR

                        result = self.columnar_cbas_utils.create_remote_dataset(
                            self.cluster,
                            remote_coll_obj.name,
                            remote_coll_obj.full_kv_entity_name,
                            remote_coll_obj.link_name,
                            remote_coll_obj.dataverse_name,
                            remote_coll_obj.database_name,
                            username=test_case['user'].username,
                            password=test_case['user'].password,
                            validate_error_msg=test_case['validate_err_msg'],
                            expected_error=expected_error)
                        if not result:
                            self.fail("Test case failed while attempting to create remote coll {}". \
                                      format(remote_coll_obj.name))

                        # create external collection.
                        self.log.info("Test creating external collection")
                        external_coll_obj = self.columnar_cbas_utils.create_external_dataset_obj(
                            self.cluster,
                            external_container_names={
                                self.s3_source_bucket: self.aws_region},
                            file_format="json",
                            use_only_existing_db=True,
                            use_only_existing_dv=True,
                            paths_on_external_container=None,
                            database=database_name,
                            dataverse=scope_name)[0]

                        expected_error = self.ACCESS_DENIED_ERR
                        result = self.create_external_dataset(
                            external_coll_obj,
                            validate_error_msg=test_case['validate_err_msg'],
                            expected_error=expected_error,
                            username=test_case['user'].username,
                            password=test_case['user'].password)
                        if not result:
                            self.fail("Test case failed while attempting to create external coll {}". \
                                      format(external_coll_obj.name))

                        self.log.info("Test creating standlone collection")
                        standalone_coll_obj = self.columnar_cbas_utils.create_standalone_dataset_obj(
                            self.cluster,
                            primary_key={"name": "string", "email": "string"},
                            database_name=database_name,
                            dataverse_name=scope_name)[0]

                        expected_error = self.ACCESS_DENIED_ERR

                        result = self.columnar_cbas_utils.create_standalone_collection(
                            self.cluster,
                            standalone_coll_obj.name,
                            dataverse_name=standalone_coll_obj.dataverse_name,
                            database_name=standalone_coll_obj.database_name,
                            primary_key=standalone_coll_obj.primary_key,
                            validate_error_msg=test_case['validate_err_msg'],
                            expected_error=expected_error,
                            username=test_case['user'].username,
                            password=test_case['user'].password)
                        if not result:
                            self.fail("Test case failed while attempting to create standalone " \
                                      "collection {}".format(standalone_coll_obj.name))

                        if not test_case['validate_err_msg']:
                            result = self.columnar_cbas_utils.drop_dataset(
                                self.cluster, remote_coll_obj.full_name)
                            if not result:
                                self.log.error("Failed to drop coll {}".format(remote_coll_obj.full_name))

                            result = self.columnar_cbas_utils.drop_dataset(
                                self.cluster, external_coll_obj.full_name)
                            if not result:
                                self.log.error("Failed to drop coll {}".format(external_coll_obj.full_name))

                            result = self.columnar_cbas_utils.drop_dataset(
                                self.cluster, standalone_coll_obj.full_name)
                            if not result:
                                self.log.error("Failed to drop coll {}".format(standalone_coll_obj.full_name))

                    elif priv == "collection_drop":
                        # create and drop standalone collection
                        self.log.info("Test dropping remote collection")
                        remote_coll_obj = self.columnar_cbas_utils.create_remote_dataset_obj(
                            self.cluster,
                            self.remote_bucket,
                            self.remote_scope,
                            self.remote_collection,
                            remote_link,
                            use_only_existing_db=True,
                            use_only_existing_dv=True)[0]

                        expected_error = self.ACCESS_DENIED_ERR
                        result = self.columnar_cbas_utils.create_remote_dataset(
                            self.cluster,
                            remote_coll_obj.name,
                            remote_coll_obj.full_kv_entity_name,
                            remote_coll_obj.link_name,
                            remote_coll_obj.dataverse_name,
                            remote_coll_obj.database_name)
                        if not result:
                            self.fail("Failed to create remote collection {}".format(
                                remote_coll_obj.name))

                        result = self.columnar_cbas_utils.drop_dataset(
                            self.cluster, remote_coll_obj.full_name,
                            username=test_case['user'].username,
                            password=test_case['user'].password,
                            validate_error_msg=test_case['validate_err_msg'],
                            expected_error=expected_error)
                        if not result:
                            self.fail("Test case failed while attempting to drop remote dataset {}"
                                      .format(remote_coll_obj.name))

                        # create and drop external collection
                        self.log.info("Test dropping external collection")
                        external_coll_obj = self.columnar_cbas_utils.create_external_dataset_obj(
                            self.cluster,
                            external_container_names={
                                self.s3_source_bucket: self.aws_region},
                            file_format="json",
                            use_only_existing_db=True,
                            use_only_existing_dv=True,
                            paths_on_external_container=None,
                            database=database_name,
                            dataverse=scope_name)[0]

                        expected_error = self.ACCESS_DENIED_ERR
                        result = self.create_external_dataset(external_coll_obj)
                        if not result:
                            self.fail("Failed to create external collection {}".
                                      format(external_coll_obj.name))

                        result = self.columnar_cbas_utils.drop_dataset(
                            self.cluster, external_coll_obj.full_name,
                            username=test_case['user'].username,
                            password=test_case['user'].password,
                            validate_error_msg=test_case['validate_err_msg'],
                            expected_error=expected_error)
                        if not result:
                            self.fail("Test case failed while attempting to drop external collection {}".
                                      format(external_coll_obj.name))

                        # create and drop standalone collection
                        self.log.info("Test dropping standalone collection")
                        standalone_coll_obj = self.columnar_cbas_utils.create_standalone_dataset_obj(
                            self.cluster,
                            primary_key={"name": "string", "email": "string"},
                            database_name=database_name,
                            dataverse_name=scope_name)[0]

                        expected_error = self.ACCESS_DENIED_ERR
                        result = self.columnar_cbas_utils.create_standalone_collection(
                            self.cluster,
                            standalone_coll_obj.name,
                            dataverse_name=standalone_coll_obj.dataverse_name,
                            database_name=standalone_coll_obj.database_name,
                            primary_key=standalone_coll_obj.primary_key)
                        if not result:
                            self.fail("Failed to create standalone collection")

                        result = self.columnar_cbas_utils.drop_dataset(
                            self.cluster, standalone_coll_obj.full_name,
                            username=test_case['user'].username,
                            password=test_case['user'].password,
                            validate_error_msg=test_case['validate_err_msg'],
                            expected_error=expected_error)
                        if not result:
                            self.fail("Test case failed while attempting to drop external collection {}".
                                      format(standalone_coll_obj.name))

                        if test_case['validate_err_msg']:
                            result = self.columnar_cbas_utils.drop_dataset(
                                self.cluster, remote_coll_obj.full_name)
                            if not result:
                                self.log.error("Failed to drop coll {}".format(remote_coll_obj.full_name))

                            result = self.columnar_cbas_utils.drop_dataset(
                                self.cluster, external_coll_obj.full_name)
                            if not result:
                                self.log.error("Failed to drop coll {}".format(external_coll_obj.full_name))

                            result = self.columnar_cbas_utils.drop_dataset(
                                self.cluster, standalone_coll_obj.full_name)
                            if not result:
                                self.log.error("Failed to drop coll {}".format(standalone_coll_obj.full_name))

        ######### TEST FOR CLOUD ROLES #########
        def generate_commands(priv):
            if priv == "collection_create":
                execute_commands = []
                #REMOTE COLLECTION
                remote_coll_obj = self.columnar_cbas_utils.create_remote_dataset_obj(
                        self.cluster,
                        self.remote_bucket,
                        self.remote_scope,
                        self.remote_collection,
                        remote_link,
                        use_only_existing_db=True,
                        use_only_existing_dv=True,
                        database="Default",
                        dataverse="Default")[0]
                cmd = self.columnar_cbas_utils.generate_create_dataset_cmd(
                        remote_coll_obj.name,
                        remote_coll_obj.full_kv_entity_name,
                        remote_coll_obj.dataverse_name,
                        remote_coll_obj.database_name,
                        link_name=remote_coll_obj.link_name)
                execute_commands.append(cmd)

                #EXTERNAL COLLECTION
                external_coll_obj = self.columnar_cbas_utils.create_external_dataset_obj(
                        self.cluster,
                        external_container_names={
                            self.s3_source_bucket: self.aws_region},
                        file_format="json",
                        use_only_existing_db=True,
                        use_only_existing_dv=True,
                        paths_on_external_container=None,
                        database="Default",
                        dataverse="Default")[0]
                cmd = self.generate_external_dataset_cmd(external_coll_obj)
                execute_commands.append(cmd)

                #STANDALONE COLLECTION
                standalone_coll_obj = self.columnar_cbas_utils.create_standalone_dataset_obj(
                        self.cluster,
                        primary_key={"name": "string", "email": "string"},
                        database_name="Default",
                        dataverse_name="Default")[0]
                cmd = self.columnar_cbas_utils.generate_standalone_create_DDL(
                        standalone_coll_obj.name,
                        dataverse_name=standalone_coll_obj.dataverse_name,
                        database_name=standalone_coll_obj.database_name,
                        primary_key=standalone_coll_obj.primary_key)
                execute_commands.append(cmd)

            elif priv == "collection_drop":
                execute_commands = []
                drop_cmd = "DROP DATASET {}"

                #REMOTE COLLECTION
                remote_coll_obj = self.columnar_cbas_utils.create_remote_dataset_obj(
                    self.cluster,
                    self.remote_bucket,
                    self.remote_scope,
                    self.remote_collection,
                    remote_link,
                    use_only_existing_db=True,
                    use_only_existing_dv=True,
                    database="Default",
                    dataverse="Default")[0]
                result = self.columnar_cbas_utils.create_remote_dataset(
                        self.cluster,
                        remote_coll_obj.name,
                        remote_coll_obj.full_kv_entity_name,
                        remote_coll_obj.link_name,
                        remote_coll_obj.dataverse_name,
                        remote_coll_obj.database_name)
                if not result:
                    self.fail("Failed to create remote collection {}".format(
                        remote_coll_obj.name))
                cmd = drop_cmd.format(self.columnar_cbas_utils.format_name(
                    remote_coll_obj.name))
                execute_commands.append(cmd)

                #EXTERNAL COLLECTION
                external_coll_obj = self.columnar_cbas_utils.create_external_dataset_obj(
                        self.cluster,
                        external_container_names={
                            self.s3_source_bucket: self.aws_region},
                        file_format="json",
                        use_only_existing_db=True,
                        use_only_existing_dv=True,
                        paths_on_external_container=None,
                        database="Default",
                        dataverse="Default")[0]
                result = self.create_external_dataset(external_coll_obj)
                if not result:
                    self.fail("Failed to create external collection {}".
                                format(external_coll_obj.name))
                cmd = drop_cmd.format(self.columnar_cbas_utils.format_name(
                    external_coll_obj.name))
                execute_commands.append(cmd)

                #STANDALONE COLLECTION
                standalone_coll_obj = self.columnar_cbas_utils.create_standalone_dataset_obj(
                        self.cluster,
                        primary_key={"name": "string", "email": "string"},
                        database_name="Default",
                        dataverse_name="Default")[0]
                result = self.columnar_cbas_utils.create_standalone_collection(
                    self.cluster,
                    standalone_coll_obj.name,
                    dataverse_name=standalone_coll_obj.dataverse_name,
                    database_name=standalone_coll_obj.database_name,
                    primary_key=standalone_coll_obj.primary_key)
                if not result:
                    self.fail("Failed to create standalone collection")
                cmd = drop_cmd.format(self.columnar_cbas_utils.format_name(
                    standalone_coll_obj.name))
                execute_commands.append(cmd)

            return execute_commands


        self.log.info("Testing for cloud roles")
        for user in self.test_users:
            self.log.info("========== CLOUD USER TEST CASE: {} ===========".
                          format(self.test_users[user]["role"]))
            self.columnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                               self.test_users[user]["mailid"],
                                               self.test_users[user]["password"])

            for priv in self.collection_ddl_privileges:
                execute_commands = generate_commands(priv)

                for execute_cmd in execute_commands:
                    resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                                  self.tenant.project_id,
                                                                  self.cluster.instance_id,
                                                                  execute_cmd)
                    if self.test_users[user]["role"] == "organizationOwner":
                        self.assertEqual(200, resp.status_code,
                                        msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code,
                                                                                    200))
                    else:
                        self.assertEqual(403, resp.status_code,
                                        msg='FAIL, Outcome:{}, Expected: {}.' \
                                            'For role: {}'.format(resp.status_code, 403,
                                                                self.test_users[user]["role"]))

        user = self.test_users['User3']
        for role in self.project_roles:
            self.log.info("========== CLOUD USER TEST CASE: {} ===========".
                          format(role))
            self.log.info(
                "Adding user to project {} with role as {}".format(self.tenant.project_id,
                                                                   role))
            payload = {
                "resourceId": self.tenant.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }

            resp = self.capellaAPIv2.add_user_to_project(self.tenant.id,
                                                         json.dumps(payload))
            self.assertEqual(200, resp.status_code,
                             msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code,
                                                                         200))

            self.columnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                               user["mailid"],
                                               user["password"])
            for priv in self.collection_ddl_privileges:
                execute_commands = generate_commands(priv)

                for execute_cmd in execute_commands:
                    resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                                  self.tenant.project_id,
                                                                  self.cluster.instance_id,
                                                                  execute_cmd)

                    if role == "projectOwner" or role == "projectDataWriter":
                        self.assertEqual(200, resp.status_code,
                                         msg='FAIL, Outcome:{}, Expected: {}.' \
                                             'For role: {}'.format(resp.status_code, 200, role))
                    else:
                        self.assertEqual(403, resp.status_code,
                                         msg='FAIL, Outcome:{}, Expected: {}.' \
                                             'For role: {}'.format(resp.status_code, 403, role))


    def test_rbac_collection_dml(self):
        self.log.info("RBAC collection dml test started")
        self.build_cbas_columnar_infra()

        collections = self.columnar_cbas_utils.get_all_datasets_from_metadata(self.cluster)

        testcases = self.create_rbac_testcases(self.collection_dml_privileges, collections,
                                               "collection")

        for idx, test_case in enumerate(testcases):
            self.log.info("========== TEST CASE {} ===========".format(idx))
            self.log.info(test_case['description'])
            if test_case['validate_err_msg']:
                self.log.info("EXPECTED: ERROR")
            else:
                self.log.info("EXPECTED: SUCCESS")

            privileges = test_case['privileges']
            resources = test_case['resources']

            for priv in privileges:
                for res in resources:

                    if priv == "collection_insert":
                        sample_doc = self.columnar_cbas_utils.generate_docs(1024)
                        expected_error = self.ACCESS_DENIED_ERR
                        result = self.columnar_cbas_utils.insert_into_standalone_collection(
                            self.cluster,
                            res,
                            [sample_doc],
                            validate_error_msg=test_case['validate_err_msg'],
                            expected_error=expected_error,
                            username=test_case['user'].username,
                            password=test_case['user'].password)
                        if not result:
                            self.fail("Test case failed while attempting to insert doc " \
                                      "into standalone coll {}".format(res))

                    elif priv == "collection_upsert":
                        sample_doc = self.columnar_cbas_utils.generate_docs(1024)
                        expected_error = self.ACCESS_DENIED_ERR
                        result = self.columnar_cbas_utils.upsert_into_standalone_collection(
                            self.cluster,
                            res,
                            [sample_doc],
                            validate_error_msg=test_case['validate_err_msg'],
                            expected_error=expected_error,
                            username=test_case['user'].username,
                            password=test_case['user'].password)
                        if not result:
                            self.fail("Test case failed while attempting to upsert doc " \
                                      "into standalone coll {}".format(res))

                    elif priv == "collection_select":
                        cmd = "SELECT * from " + self.columnar_cbas_utils.format_name(res) + ";"
                        expected_error = self.ACCESS_DENIED_ERR
                        status, metrics, errors, results, _, warnings = (
                            self.columnar_cbas_utils.execute_statement_on_cbas_util(
                                self.cluster, cmd,
                                username=test_case['user'].username,
                                password=test_case['user'].password,
                                timeout=300, analytics_timeout=300))

                        if test_case['validate_err_msg']:
                            result = self.columnar_cbas_utils.validate_error_and_warning_in_response(
                                status, errors, expected_error, None)
                            if not result:
                                self.fail("Test case failed while attempting to get docs from standalone coll {}".
                                          format(res))
                        else:
                            if status != "success":
                                self.fail("Test case failed while attempting to get docs from standalone coll {}." \
                                          "Error: {}".format(res, errors))
                    elif priv == "collection_delete":
                        expected_error = self.ACCESS_DENIED_ERR
                        result = (self.columnar_cbas_utils.delete_from_standalone_collection(
                            self.cluster,
                            res,
                            validate_error_msg=test_case['validate_err_msg'],
                            expected_error=expected_error,
                            username=test_case['user'].username,
                            password=test_case['user'].password))
                        if not result:
                            self.fail("Test case failed while attempting to delete documents from standalone coll {}".
                                      format(res))

        self.log.info("Testing for cloud roles")
        standalone_coll = collections[0]
        for user in self.test_users:
            self.log.info("========== CLOUD USER TEST CASE: {} ===========".
                          format(self.test_users[user]["role"]))
            self.columnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                               self.test_users[user]["mailid"],
                                               self.test_users[user]["password"])
            for priv in self.collection_dml_privileges:
                if priv == "collection_insert":
                    sample_doc = self.columnar_cbas_utils.generate_docs(1024)
                    execute_cmd = self.columnar_cbas_utils.generate_insert_into_cmd(
                        [sample_doc],
                        collection_name=standalone_coll)
                elif priv == "collection_upsert":
                    sample_doc = self.columnar_cbas_utils.generate_docs(1024)
                    execute_cmd = self.columnar_cbas_utils.generate_upsert_into_cmd(
                        standalone_coll,
                        [sample_doc])
                elif priv == "collection_select":
                    execute_cmd = "SELECT * from " + \
                                  self.columnar_cbas_utils.format_name(standalone_coll) + ";"
                elif priv == "collection_delete":
                    execute_cmd = self.columnar_cbas_utils.generate_delete_from_cmd(standalone_coll)

                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.cluster.instance_id,
                                                              execute_cmd)
                if self.test_users[user]["role"] == "organizationOwner":
                    self.assertEqual(200, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code,
                                                                                 200))
                else:
                    self.assertEqual(403, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 403,
                                                               self.test_users[user]["role"]))

        user = self.test_users['User3']
        for role in self.project_roles:
            self.log.info("========== CLOUD USER TEST CASE: {} ===========".
                          format(role))
            self.log.info(
                "Adding user to project {} with role as {}".format(self.tenant.project_id,
                                                                   role))
            payload = {
                "resourceId": self.tenant.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }

            resp = self.capellaAPIv2.add_user_to_project(self.tenant.id,
                                                         json.dumps(payload))
            self.assertEqual(200, resp.status_code,
                             msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code,
                                                                         200))

            self.columnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                               user["mailid"],
                                               user["password"])
            for priv in self.collection_dml_privileges:
                if priv == "collection_insert":
                    sample_doc = self.columnar_cbas_utils.generate_docs(1024)
                    execute_cmd = self.columnar_cbas_utils.generate_insert_into_cmd(
                        [sample_doc],
                        collection_name=standalone_coll)
                    resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                                  self.tenant.project_id,
                                                                  self.cluster.instance_id,
                                                                  execute_cmd)
                    if role == "projectOwner" or role == "projectDataWriter":
                        self.assertEqual(200, resp.status_code,
                                         msg='FAIL, Outcome:{}, Expected: {}.' \
                                             'For role: {}'.format(resp.status_code, 200, role))
                    else:
                        self.assertEqual(403, resp.status_code,
                                         msg='FAIL, Outcome:{}, Expected: {}.' \
                                             'For role: {}'.format(resp.status_code, 403, role))
                elif priv == "collection_upsert":
                    sample_doc = self.columnar_cbas_utils.generate_docs(1024)
                    execute_cmd = self.columnar_cbas_utils.generate_upsert_into_cmd(
                        standalone_coll,
                        [sample_doc])
                    resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                                  self.tenant.project_id,
                                                                  self.cluster.instance_id,
                                                                  execute_cmd)
                    if role == "projectOwner" or role == "projectDataWriter":
                        self.assertEqual(200, resp.status_code,
                                         msg='FAIL, Outcome:{}, Expected: {}.' \
                                             'For role: {}'.format(resp.status_code, 200, role))
                    else:
                        self.assertEqual(403, resp.status_code,
                                         msg='FAIL, Outcome:{}, Expected: {}.' \
                                             'For role: {}'.format(resp.status_code, 403, role))
                elif priv == "collection_select":
                    execute_cmd = "SELECT * from " + \
                                  self.columnar_cbas_utils.format_name(standalone_coll) + ";"
                    resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                                  self.tenant.project_id,
                                                                  self.cluster.instance_id,
                                                                  execute_cmd)
                    if role == "projectOwner" or role == "projectDataWriter" or \
                            role == "projectDataViewer":
                        self.assertEqual(200, resp.status_code,
                                         msg='FAIL, Outcome:{}, Expected: {}.' \
                                             'For role: {}'.format(resp.status_code, 200, role))
                    else:
                        self.assertEqual(403, resp.status_code,
                                         msg='FAIL, Outcome:{}, Expected: {}.' \
                                             'For role: {}'.format(resp.status_code, 403, role))
                elif priv == "collection_delete":
                    execute_cmd = self.columnar_cbas_utils.generate_delete_from_cmd(standalone_coll)
                    if role == "projectOwner" or role == "projectDataWriter":
                        self.assertEqual(200, resp.status_code,
                                         msg='FAIL, Outcome:{}, Expected: {}.' \
                                             'For role: {}'.format(resp.status_code, 200, role))
                    else:
                        self.assertEqual(403, resp.status_code,
                                         msg='FAIL, Outcome:{}, Expected: {}.' \
                                             'For role: {}'.format(resp.status_code, 403, role))

    def test_rbac_link_ddl(self):
        self.log.info("RBAC link ddl test started")
        self.build_cbas_columnar_infra()
        remote_links_created = []
        external_links_created = []

        testcases = self.create_rbac_testcases(self.link_ddl_privileges, [],
                                               resource_type="instance")

        for idx, test_case in enumerate(testcases):
            self.log.info("========== TEST CASE {} ===========".format(idx))
            self.log.info(test_case['description'])
            if test_case['validate_err_msg']:
                self.log.info("EXPECTED: ERROR")
            else:
                self.log.info("EXPECTED: SUCCESS")

            privileges = test_case['privileges']

            for priv in privileges:
                if priv == "link_create":
                    self.log.info("Do nothing")
                    # create remote link
                    self.columnar_cbas_utils.create_remote_link_obj(
                        self.cluster,
                        hostname=self.remote_cluster.master.ip,
                        username=self.remote_cluster.master.rest_username,
                        password=self.remote_cluster.master.rest_password,
                        certificate=self.get_remote_cluster_certificate(self.remote_cluster),
                        encryption="full")
                    remote_link_obj = None
                    remote_links = self.columnar_cbas_utils.get_all_link_objs("couchbase")
                    for rm_link in remote_links:
                        if rm_link.name not in remote_links_created:
                            remote_link_obj = rm_link
                            break
                    expected_error = self.ACCESS_DENIED_ERR

                    result = self.columnar_cbas_utils.create_remote_link(
                        self.cluster,
                        remote_link_obj.properties,
                        username=test_case['user'].username,
                        password=test_case['user'].password,
                        validate_error_msg=test_case['validate_err_msg'],
                        expected_error=expected_error)
                    if not result:
                        self.fail("Test case failed while attempting to create remote link {}".
                                  format(remote_link_obj.name))
                    remote_links_created.append(remote_link_obj.name)

                    # create external link
                    self.columnar_cbas_utils.create_external_link_obj(self.cluster,
                                                                      accessKeyId=self.aws_access_key,
                                                                      secretAccessKey=self.aws_secret_key,
                                                                      regions=[self.aws_region])
                    external_links = self.columnar_cbas_utils.get_all_link_objs("s3")
                    s3_link_obj = None
                    for ex_link in external_links:
                        if ex_link.name not in external_links_created:
                            s3_link_obj = ex_link
                            break
                    expected_error = self.ACCESS_DENIED_ERR

                    result = self.columnar_cbas_utils.create_external_link(
                        self.cluster,
                        s3_link_obj.properties,
                        username=test_case['user'].username,
                        password=test_case['user'].password,
                        validate_error_msg=test_case['validate_err_msg'],
                        expected_error=expected_error)
                    if not result:
                        self.fail("Test case failed while attempting to create external link {}".
                                  format(s3_link_obj.name))
                    external_links_created.append(s3_link_obj.name)
                elif priv == "link_drop":
                    # create and drop remote link
                    self.columnar_cbas_utils.create_remote_link_obj(
                        self.cluster,
                        hostname=self.remote_cluster.master.ip,
                        username=self.remote_cluster.master.rest_username,
                        password=self.remote_cluster.master.rest_password,
                        certificate=self.get_remote_cluster_certificate(self.remote_cluster),
                        encryption="full")
                    remote_link_obj = None
                    remote_links = self.columnar_cbas_utils.get_all_link_objs("couchbase")
                    for rm_link in remote_links:
                        if rm_link.name not in remote_links_created:
                            remote_link_obj = rm_link
                            break
                    expected_error = self.ACCESS_DENIED_ERR

                    result = self.columnar_cbas_utils.create_remote_link(self.cluster,
                                                                         remote_link_obj.properties)
                    if not result:
                        self.fail("Failed to create remote link {}".format(remote_link_obj.name))
                    remote_links_created.append(remote_link_obj.name)
                    result = self.columnar_cbas_utils.drop_link(
                        self.cluster,
                        remote_link_obj.name,
                        username=test_case['user'].username,
                        password=test_case['user'].password,
                        validate_error_msg=test_case['validate_err_msg'],
                        expected_error=expected_error)
                    if not result:
                        self.fail("Test case failed to while attemptint to drop remote link {}".
                                  format(remote_link_obj.name))

                    # create and drop external link
                    self.columnar_cbas_utils.create_external_link_obj(
                        self.cluster,
                        accessKeyId=self.aws_access_key,
                        secretAccessKey=self.aws_secret_key,
                        regions=[self.aws_region])
                    external_links = self.columnar_cbas_utils.get_all_link_objs("s3")
                    s3_link_obj = None
                    for ex_link in external_links:
                        if ex_link.name not in external_links_created:
                            s3_link_obj = ex_link
                            break
                    expected_error = self.ACCESS_DENIED_ERR

                    result = self.columnar_cbas_utils.create_external_link(
                        self.cluster,
                        s3_link_obj.properties)
                    if not result:
                        self.fail("Failed to create external link {}". \
                                  format(s3_link_obj.name))
                    external_links_created.append(s3_link_obj.name)

                    result = self.columnar_cbas_utils.drop_link(
                        self.cluster,
                        s3_link_obj.name,
                        username=test_case['user'].username,
                        password=test_case['user'].password,
                        validate_error_msg=test_case['validate_err_msg'],
                        expected_error=expected_error)
                    if not result:
                        self.fail("Test case failed while attempting to drop external link {}".
                                  format(s3_link_obj.name))

    def test_rbac_link_connection(self):
        self.log.info("RBAC link connection test started")
        self.build_cbas_columnar_infra()

        link_objs = self.columnar_cbas_utils.get_all_link_objs("couchbase")

        remote_links = []
        for rm_link in link_objs:
            result = self.columnar_cbas_utils.disconnect_link(self.cluster,
                                                              rm_link.name)
            remote_links.append(rm_link.name)
            if not result:
                self.fail("Failed to disconnect link {}".format(rm_link.name))

        testcases = self.create_rbac_testcases(self.link_connection_privileges,
                                               remote_links,
                                               resource_type="link")
        for idx, test_case in enumerate(testcases):
            self.log.info("========== TEST CASE {} ===========".format(idx))
            self.log.info(test_case['description'])
            if test_case['validate_err_msg']:
                self.log.info("EXPECTED: ERROR")
            else:
                self.log.info("EXPECTED: SUCCESS")

            privileges = test_case['privileges']
            resources = test_case['resources']

            for priv in privileges:

                if priv == "link_connect":
                    for res in resources:
                        link_name = res.split(".")[0]
                        expected_error = self.ACCESS_DENIED_ERR
                        is_link_active = self.columnar_cbas_utils.is_link_active(
                            self.cluster,
                            link_name,
                            "couchbase")
                        if is_link_active:
                            result = self.columnar_cbas_utils.disconnect_link(self.cluster,
                                                                              res)
                            if not result:
                                self.fail("Failed to disconnect link {}".format(res))

                        result = self.columnar_cbas_utils.connect_link(
                            self.cluster,
                            res,
                            validate_error_msg=test_case['validate_err_msg'],
                            expected_error=expected_error,
                            username=test_case['user'].username,
                            password=test_case['user'].password)
                        if not result:
                            self.fail("Test case failed while attempting to connect link {}".
                                      format(res))

                elif res == "link_disconnect":
                    for res in resources:
                        link_name = res.split(".")[0]
                        expected_error = self.ACCESS_DENIED_ERR
                        is_link_active = self.columnar_cbas_utils.is_link_active(
                            self.cluster,
                            link_name,
                            "couchbase")
                        if not is_link_active:
                            result = self.columnar_cbas_utils.connect_link(self.cluster,
                                                                           res)
                            if not result:
                                self.fail("Failed to connect link {}".format(res))

                        result = self.columnar_cbas_utils.disconnect_link(
                            self.cluster,
                            res,
                            validate_error_msg=test_case['validate_err_msg'],
                            expected_error=expected_error,
                            username=test_case['user'].username,
                            password=test_case['user'].password)
                        if not result:
                            self.fail("Test case failed while attempting to disconnect link {}".
                                      format(res))

        rm_link = remote_links[0]
        self.log.info("Testing for cloud roles")
        connect_link_cmd = "CONNECT LINK {};"
        disconnect_link_cmd = "DISCONNECT LINK {};"
        for user in self.test_users:
            self.log.info("========== CLOUD USER TEST CASE: {} ===========".
                          format(self.test_users[user]["role"]))
            self.columnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                               self.test_users[user]["mailid"],
                                               self.test_users[user]["password"])
            for priv in self.link_connection_privileges:
                if priv == "link_connect":
                    link_name = rm_link.split(".")[0]
                    expected_error = self.ACCESS_DENIED_ERR
                    is_link_active = self.columnar_cbas_utils.is_link_active(
                        self.cluster,
                        link_name,
                        "couchbase")
                    if is_link_active:
                        result = self.columnar_cbas_utils.disconnect_link(self.cluster,
                                                                          rm_link)
                        if not result:
                            self.fail("Failed to disconnect link {}".format(rm_link))

                    execute_cmd = connect_link_cmd.format(
                        self.columnar_cbas_utils.format_name(rm_link))
                elif priv == "link_disconnect":
                    link_name = rm_link.split(".")[0]
                    expected_error = self.ACCESS_DENIED_ERR
                    is_link_active = self.columnar_cbas_utils.is_link_active(
                        self.cluster,
                        link_name,
                        "couchbase")
                    if not is_link_active:
                        result = self.columnar_cbas_utils.connect_link(self.cluster,
                                                                       rm_link)
                        if not result:
                            self.fail("Failed to connect link {}".format(rm_link))
                    execute_cmd = disconnect_link_cmd.format(
                        self.columnar_cbas_utils.format_name(rm_link))

                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.cluster.instance_id,
                                                              execute_cmd)
                if self.test_users[user]["role"] == "organizationOwner":
                    self.assertEqual(200, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code,
                                                                                 200))
                else:
                    self.assertEqual(403, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 403,
                                                               self.test_users[user]["role"]))

        user = self.test_users['User3']
        for role in self.project_roles:
            self.log.info("========== CLOUD USER TEST CASE: {} ===========".
                          format(role))
            self.log.info(
                "Adding user to project {} with role as {}".format(self.tenant.project_id,
                                                                   role))
            payload = {
                "resourceId": self.tenant.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }

            resp = self.capellaAPIv2.add_user_to_project(self.tenant.id,
                                                         json.dumps(payload))
            self.assertEqual(200, resp.status_code,
                             msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code,
                                                                         200))

            self.columnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                               user["mailid"],
                                               user["password"])
            for priv in self.link_connection_privileges:
                if priv == "link_connect":
                    link_name = rm_link.split(".")[0]
                    expected_error = self.ACCESS_DENIED_ERR
                    is_link_active = self.columnar_cbas_utils.is_link_active(
                        self.cluster,
                        link_name,
                        "couchbase")
                    if is_link_active:
                        result = self.columnar_cbas_utils.disconnect_link(self.cluster,
                                                                          rm_link)
                        if not result:
                            self.fail("Failed to disconnect link {}".format(rm_link))

                    execute_cmd = connect_link_cmd.format(
                        self.columnar_cbas_utils.format_name(rm_link))
                elif priv == "link_disconnect":
                    link_name = rm_link.split(".")[0]
                    expected_error = self.ACCESS_DENIED_ERR
                    is_link_active = self.columnar_cbas_utils.is_link_active(
                        self.cluster,
                        link_name,
                        "couchbase")
                    if not is_link_active:
                        result = self.columnar_cbas_utils.connect_link(self.cluster,
                                                                       rm_link)
                        if not result:
                            self.fail("Failed to connect link {}".format(rm_link))
                    execute_cmd = disconnect_link_cmd.format(
                        self.columnar_cbas_utils.format_name(rm_link))

                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.cluster.instance_id,
                                                              execute_cmd)
                if role == "projectOwner" or role == "projectDataWriter":
                    self.assertEqual(200, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 200, role))
                else:
                    self.assertEqual(403, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 403, role))

    def test_rbac_link_dml(self):
        self.log.info("RBAC link dml test started")
        self.build_cbas_columnar_infra()
        external_links = self.columnar_cbas_utils.get_all_link_objs("s3")
        s3_links = []
        for ex_link in external_links:
            s3_links.append(ex_link.name)
        remote_links = self.columnar_cbas_utils.get_all_link_objs("couchbase")
        rm_links = []
        for rm_link in remote_links:
            rm_links.append(rm_link.name)

        self.create_s3_sink_bucket()

        dataset = self.columnar_cbas_utils.get_all_dataset_objs("standalone")[0]
        standalone_coll_obj = self.columnar_cbas_utils.create_standalone_dataset_obj(
            self.cluster,
            primary_key={"id": "string"},
            database_name="Default",
            dataverse_name="Default")[0]
        result = self.columnar_cbas_utils.create_standalone_collection(
            self.cluster,
            standalone_coll_obj.name,
            dataverse_name=standalone_coll_obj.dataverse_name,
            database_name=standalone_coll_obj.database_name,
            primary_key=standalone_coll_obj.primary_key)
        if not result:
            self.fail("Failed to create copy from s3 datastaset")
        standalone_datasets = self.columnar_cbas_utils.get_all_dataset_objs("standalone")
        for ds in standalone_datasets:
            if ds.name != dataset.name:
                dataset_copy_from = ds
                break
        no_of_docs = self.input.param("no_of_docs", 100)
        jobs = Queue()
        results = []
        self.log.info("Adding {} documents in standalone dataset. Default doc size is 1KB".format(no_of_docs))
        jobs.put((self.columnar_cbas_utils.load_doc_to_standalone_collection,
                  {"cluster": self.cluster, "collection_name": dataset.name,
                   "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name
                      , "no_of_docs": no_of_docs}))

        self.columnar_cbas_utils.run_jobs_in_parallel(
            jobs, results, 1, async_run=False)

        if not all(results):
            self.log.error("Some documents were not inserted")

        extended_res_priv_map = []
        res_priv_obj = {
            "name": "",
            "type": "instance",
            "privileges": ["collection_select", "collection_insert"]
        }
        extended_res_priv_map.append(res_priv_obj)
        testcases = self.create_rbac_testcases(self.link_dml_privileges, s3_links + rm_links,
                                               resource_type="link",
                                               extended_res_priv_map=extended_res_priv_map)

        path_idx = 0
        for idx, test_case in enumerate(testcases):
            self.log.info("========== TEST CASE {} ===========".format(idx))
            self.log.info(test_case['description'])
            if test_case['validate_err_msg']:
                self.log.info("EXPECTED: ERROR")
            else:
                self.log.info("EXPECTED: SUCCESS")

            privileges = test_case['privileges']
            resources = test_case['resources']

            for priv in privileges:
                if priv == "link_copy_to":
                    self.log.info("Testing COPY TO")
                    for res in resources:
                        expected_error = self.ACCESS_DENIED_ERR
                        path = "copy_dataset_" + str(path_idx)
                        path_idx += 1
                        collection = "{}.{}.{}".format(self.remote_bucket.name,
                                                       self.remote_scope.name,
                                                       self.remote_collection.name)
                        # copy to s3
                        if res in s3_links:
                            result = self.columnar_cbas_utils.copy_to_s3(
                                self.cluster,
                                dataset.name,
                                dataset.dataverse_name,
                                dataset.database_name,
                                destination_bucket=self.sink_s3_bucket_name,
                                destination_link_name=res,
                                path=path,
                                validate_error_msg=test_case['validate_err_msg'],
                                expected_error=expected_error,
                                username=test_case['user'].username,
                                password=test_case['user'].password)
                            if not result:
                                self.fail("Test case failed while attempting to copy data to s3 from link {}".
                                        format(res))

                        #copy to kv
                        if res in rm_links:
                            result = self.columnar_cbas_utils.copy_to_kv(
                                self.cluster,
                                dataset.name,
                                dataset.database_name,
                                dataset.dataverse_name,
                                dest_bucket=collection,
                                link_name=res,
                                validate_error_msg=test_case['validate_err_msg'],
                                expected_error=expected_error,
                                username=test_case['user'].username,
                                password=test_case['user'].password
                            )
                            if not result:
                                self.fail("Test case failed while attempting to copy data to kv from link {}".
                                        format(res))

                elif priv == "link_copy_from":
                    self.log.info("Testing COPY FROM")
                    for res in resources:
                        expected_error = self.ACCESS_DENIED_ERR
                        if res in s3_links:
                            result = self.columnar_cbas_utils.copy_from_external_resource_into_standalone_collection(
                                self.cluster,
                                dataset_copy_from.name,
                                self.s3_source_bucket,
                                res,
                                dataset_copy_from.dataverse_name,
                                dataset_copy_from.database_name,
                                "*.json",
                                validate_error_msg=test_case['validate_err_msg'],
                                expected_error=expected_error,
                                username=test_case['user'].username,
                                password=test_case['user'].password)
                            if not result:
                                self.fail("Test case failed while attempting to copy data from s3 " \
                                        "on link {} from coll {}".format(res, dataset_copy_from.full_name))

        self.log.info("Testing for cloud roles")
        res = s3_links[0]
        for user in self.test_users:
            self.log.info("========== CLOUD USER TEST CASE: {} ===========".
                          format(self.test_users[user]["role"]))
            self.columnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                               self.test_users[user]["mailid"],
                                               self.test_users[user]["password"])
            for priv in privileges:
                if priv == "link_copy_to":
                    execute_commands = []
                    for res in resources:
                        expected_error = self.ACCESS_DENIED_ERR
                        path = "copy_dataset_" + str(path_idx)
                        path_idx += 1
                        # copy to s3
                        if res in s3_links:
                            execute_cmd = self.columnar_cbas_utils.generate_copy_to_s3_cmd(
                                dataset.name,
                                dataset.dataverse_name,
                                dataset.database_name,
                                destination_bucket=self.sink_s3_bucket_name,
                                destination_link_name=res,
                                path=path
                            )
                            execute_commands.append(execute_cmd)

                        if res in rm_links:
                            execute_cmd = self.columnar_cbas_utils.generate_copy_to_kv_cmd(
                                dataset.name,
                                dataset.database_name,
                                dataset.dataverse_name,
                                dest_bucket=collection,
                                link_name=res)
                            execute_commands.append(execute_cmd)

                elif priv == "link_copy_from":
                    execute_commands = []
                    for res in resources:
                        if res in s3_links:
                            execute_cmd = self.columnar_cbas_utils.generate_copy_from_cmd(
                                dataset_copy_from.name,
                                self.s3_source_bucket,
                                res,
                                dataset_copy_from.dataverse_name,
                                dataset_copy_from.database_name,
                                "*.json"
                            )
                            execute_commands.append(execute_cmd)

                for execute_cmd in execute_commands:
                    resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                                  self.tenant.project_id,
                                                                  self.cluster.instance_id,
                                                                  execute_cmd)
                    if self.test_users[user]["role"] == "organizationOwner":
                        self.assertEqual(200, resp.status_code,
                                        msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code,
                                                                                    200))
                    else:
                        self.assertEqual(403, resp.status_code,
                                        msg='FAIL, Outcome:{}, Expected: {}.' \
                                            'For role: {}'.format(resp.status_code, 403,
                                                                self.test_users[user]["role"]))

        user = self.test_users['User3']
        for role in self.project_roles:
            self.log.info("========== CLOUD USER TEST CASE: {} ===========".
                          format(role))
            self.log.info(
                "Adding user to project {} with role as {}".format(self.tenant.project_id,
                                                                   role))
            payload = {
                "resourceId": self.tenant.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }

            resp = self.capellaAPIv2.add_user_to_project(self.tenant.id,
                                                         json.dumps(payload))
            self.assertEqual(200, resp.status_code,
                             msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code,
                                                                         200))

            self.columnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                               user["mailid"],
                                               user["password"])
            for priv in privileges:
                if priv == "link_copy_to":
                    execute_commands = []
                    for res in resources:
                        expected_error = self.ACCESS_DENIED_ERR
                        path = "copy_dataset_" + str(path_idx)
                        path_idx += 1
                        # copy to s3
                        if res in s3_links:
                            execute_cmd = self.columnar_cbas_utils.generate_copy_to_s3_cmd(
                                dataset.name,
                                dataset.dataverse_name,
                                dataset.database_name,
                                destination_bucket=self.sink_s3_bucket_name,
                                destination_link_name=res,
                                path=path
                            )
                            execute_commands.append(execute_cmd)
                        #copy to kv
                        if res in rm_links:
                            execute_cmd = self.columnar_cbas_utils.generate_copy_to_kv_cmd(
                                dataset.name,
                                dataset.database_name,
                                dataset.dataverse_name,
                                dest_bucket=collection,
                                link_name=res)
                            execute_commands.append(execute_cmd)

                elif priv == "link_copy_from":
                    execute_commands = []
                    for res in resources:
                        execute_cmd = self.columnar_cbas_utils.generate_copy_from_cmd(
                            dataset_copy_from.name,
                            self.s3_source_bucket,
                            res,
                            dataset_copy_from.dataverse_name,
                            dataset_copy_from.database_name,
                            "*.json"
                        )

                for execute_cmd in execute_commands:
                    resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                                self.tenant.project_id,
                                                                self.cluster.instance_id,
                                                                execute_cmd)
                    if role == "projectOwner" or role == "projectDataWriter":
                        self.assertEqual(200, resp.status_code,
                                        msg='FAIL, Outcome:{}, Expected: {}.' \
                                            'For role: {}'.format(resp.status_code, 200, role))
                    else:
                        self.assertEqual(403, resp.status_code,
                                        msg='FAIL, Outcome:{}, Expected: {}.' \
                                            'For role: {}'.format(resp.status_code, 403, role))

    def test_rbac_synonym(self):
        self.log.info("RBAC synonym test started")
        self.build_cbas_columnar_infra()
        scopes = self.columnar_cbas_utils.get_all_dataverses_from_metadata(self.cluster)
        collection = self.columnar_cbas_utils.get_all_dataset_objs("standalone")[0]

        testcases = self.create_rbac_testcases(self.synonym_privileges, scopes,
                                               resource_type="scope")

        for idx, test_case in enumerate(testcases):
            self.log.info("========== TEST CASE {} ===========".format(idx))
            self.log.info(test_case['description'])
            if test_case['validate_err_msg']:
                self.log.info("EXPECTED: ERROR")
            else:
                self.log.info("EXPECTED: SUCCESS")

            privileges = test_case['privileges']
            resources = test_case['resources']

            for priv in privileges:
                if priv == "synonym_create":
                    for res in resources:
                        synonym_name = self.generate_random_entity_name(type="synonym")
                        synonym_full_name = "{}.{}".format(res, synonym_name)
                        synonym_full_name = self.columnar_cbas_utils.format_name(synonym_full_name)
                        expected_error = self.ACCESS_DENIED_ERR
                        result = self.columnar_cbas_utils.create_analytics_synonym(
                            self.cluster,
                            synonym_full_name,
                            collection.full_name,
                            validate_error_msg=test_case['validate_err_msg'],
                            expected_error=expected_error,
                            username=test_case['user'].username,
                            password=test_case['user'].password)
                        if not result:
                            self.fail("Test case failed while attempting to create synonym {}".
                                      format(synonym_full_name))

                elif priv == "synonym_drop":
                    for res in resources:
                        synonym_name = self.generate_random_entity_name(type="synonym")
                        synonym_full_name = "{}.{}".format(res, synonym_name)
                        synonym_full_name = self.columnar_cbas_utils.format_name(synonym_full_name)
                        expected_error = self.ACCESS_DENIED_ERR
                        result = self.columnar_cbas_utils.create_analytics_synonym(self.cluster,
                                                                                   synonym_full_name,
                                                                                   collection.full_name)
                        if not result:
                            self.fail("Failed to create synonym {}".format(synonym_full_name))

                        result = self.columnar_cbas_utils.drop_analytics_synonym(
                            self.cluster,
                            synonym_full_name,
                            validate_error_msg=test_case['validate_err_msg'],
                            expected_error=expected_error,
                            username=test_case['user'].username,
                            password=test_case['user'].password)
                        if not result:
                            self.fail("Test case failed while attempting to create synonym {}".
                                      format(synonym_full_name))

        self.log.info("Testing for cloud roles")
        for user in self.test_users:
            self.log.info("========== CLOUD USER TEST CASE: {} ===========".
                          format(self.test_users[user]["role"]))
            self.columnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                               self.test_users[user]["mailid"],
                                               self.test_users[user]["password"])
            for priv in self.synonym_privileges:
                if priv == "synonym_create":
                    synonym_name = self.generate_random_entity_name(type="synonym")
                    synonym_full_name = "{}.{}".format(res, synonym_name)
                    synonym_full_name = self.columnar_cbas_utils.format_name(synonym_full_name)
                    execute_cmd = self.columnar_cbas_utils.generate_create_analytics_synonym_cmd(
                        synonym_full_name, collection.full_name)
                elif priv == "synonym_drop":
                    synonym_name = self.generate_random_entity_name(type="synonym")
                    synonym_full_name = "{}.{}".format(res, synonym_name)
                    synonym_full_name = self.columnar_cbas_utils.format_name(synonym_full_name)
                    result = self.columnar_cbas_utils.create_analytics_synonym(self.cluster,
                                                                               synonym_full_name,
                                                                               collection.full_name)
                    if not result:
                        self.fail("Failed to create synonym {}".format(synonym_full_name))
                    execute_cmd = self.columnar_cbas_utils.generate_drop_analytics_synonym_cmd(
                        synonym_full_name)
                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.cluster.instance_id,
                                                              execute_cmd)
                if self.test_users[user]["role"] == "organizationOwner":
                    self.assertEqual(200, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code,
                                                                                 200))
                else:
                    self.assertEqual(403, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 403,
                                                               self.test_users[user]["role"]))

        user = self.test_users['User3']
        for role in self.project_roles:
            self.log.info("========== CLOUD USER TEST CASE: {} ===========".
                          format(role))
            self.log.info(
                "Adding user to project {} with role as {}".format(self.tenant.project_id,
                                                                   role))
            payload = {
                "resourceId": self.tenant.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }

            resp = self.capellaAPIv2.add_user_to_project(self.tenant.id,
                                                         json.dumps(payload))
            self.assertEqual(200, resp.status_code,
                             msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code,
                                                                         200))

            self.columnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                               user["mailid"],
                                               user["password"])
            for priv in self.synonym_privileges:
                if priv == "synonym_create":
                    synonym_name = self.generate_random_entity_name(type="synonym")
                    synonym_full_name = "{}.{}".format(res, synonym_name)
                    synonym_full_name = self.columnar_cbas_utils.format_name(synonym_full_name)
                    execute_cmd = self.columnar_cbas_utils.generate_create_analytics_synonym_cmd(
                        synonym_full_name, collection.full_name)
                elif priv == "synonym_drop":
                    synonym_name = self.generate_random_entity_name(type="synonym")
                    synonym_full_name = "{}.{}".format(res, synonym_name)
                    synonym_full_name = self.columnar_cbas_utils.format_name(synonym_full_name)
                    result = self.columnar_cbas_utils.create_analytics_synonym(self.cluster,
                                                                               synonym_full_name,
                                                                               collection.full_name)
                    if not result:
                        self.fail("Failed to create synonym {}".format(synonym_full_name))
                    execute_cmd = self.columnar_cbas_utils.generate_drop_analytics_synonym_cmd(
                        synonym_full_name)
                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.cluster.instance_id,
                                                              execute_cmd)
                if role == "projectOwner" or role == "projectDataWriter":
                    self.assertEqual(200, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 200, role))
                else:
                    self.assertEqual(403, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 403, role))

    def generate_create_view_cmd(self, view_full_name, view_defn, if_not_exists=False):
        cmd = "create analytics view {0}".format(view_full_name)

        if if_not_exists:
            cmd += " If Not Exists"

        cmd += " as {0};".format(view_defn)

        return cmd

    def test_rbac_views(self):
        self.log.info("RBAC views test started")
        self.build_cbas_columnar_infra()

        datasets = self.columnar_cbas_utils.get_all_dataset_objs("standalone")

        no_of_docs = self.input.param("no_of_docs", 1000)
        jobs = Queue()
        results = []
        for dataset in datasets:
            self.log.info("Adding {} documents in standalone dataset {}. Default doc size is 1KB".
                          format(no_of_docs, dataset.full_name))
            jobs.put((self.columnar_cbas_utils.load_doc_to_standalone_collection,
                      {"cluster": self.cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name
                          , "no_of_docs": no_of_docs}))

        self.columnar_cbas_utils.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Some documents were not inserted")

        scopes = self.columnar_cbas_utils.get_all_dataverses_from_metadata(self.cluster)
        view_collection = datasets[0]

        view_defn = "SELECT name, email from {0}".format(
            self.columnar_cbas_utils.format_name(view_collection.full_name))

        extended_res_priv_map = []
        create_collection_priv = {
            "name": "",
            "type": "instance",
            "privileges": ["collection_select"]
        }
        extended_res_priv_map.append(create_collection_priv)
        testcases = self.create_rbac_testcases(self.view_ddl_privileges, scopes,
                                               resource_type="scope",
                                               extended_res_priv_map=extended_res_priv_map)

        for idx, test_case in enumerate(testcases):
            self.log.info("========== TEST CASE {} ===========".format(idx))
            self.log.info(test_case['description'])
            if test_case['validate_err_msg']:
                self.log.info("EXPECTED: ERROR")
            else:
                self.log.info("EXPECTED: SUCCESS")

            privileges = test_case['privileges']
            resources = test_case['resources']

            self.log.critical(test_case)

            for priv in privileges:
                for res in resources:
                    if priv == "view_create":
                        view_name = self.generate_random_entity_name(type="view")
                        view_full_name = "{}.{}".format(res, view_name)
                        view_full_name = self.columnar_cbas_utils.format_name(view_full_name)
                        expected_error = self.ACCESS_DENIED_ERR
                        result = self.columnar_cbas_utils.create_analytics_view(
                            self.cluster,
                            view_full_name,
                            view_defn,
                            validate_error_msg=test_case['validate_err_msg'],
                            expected_error=expected_error,
                            username=test_case['user'].username,
                            password=test_case['user'].password)
                        if not result:
                            self.log.error("Test failed")
                            self.fail("Test case failed while attempting to create view {}".
                                      format(view_full_name))
                    elif priv == "view_drop":
                        view_name = self.generate_random_entity_name(type="view")
                        view_full_name = "{}.{}".format(res, view_name)
                        view_full_name = self.columnar_cbas_utils.format_name(view_full_name)
                        expected_error = self.ACCESS_DENIED_ERR

                        result = self.columnar_cbas_utils.create_analytics_view(
                            self.cluster,
                            view_full_name,
                            view_defn)
                        if not result:
                            self.log.error("Failed to create view")
                            self.fail("Failed to create analytics view {}".format(view_full_name))

                        result = self.columnar_cbas_utils.drop_analytics_view(
                            self.cluster,
                            view_full_name,
                            validate_error_msg=test_case['validate_err_msg'],
                            expected_error=expected_error,
                            username=test_case['user'].username,
                            password=test_case['user'].password)
                        if not result:
                            self.log.error("Test failed")
                            self.fail("Test case failed while attempting to drop analytics view".
                                      format(view_full_name))

        self.log.info("Testing for cloud roles")
        for user in self.test_users:
            self.log.info("========== CLOUD USER TEST CASE: {} ===========".
                          format(self.test_users[user]["role"]))
            self.columnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                               self.test_users[user]["mailid"],
                                               self.test_users[user]["password"])
            for priv in self.view_ddl_privileges:
                if priv == "view_create":
                    view_name = self.generate_random_entity_name(type="view")
                    view_full_name = "{}.{}".format("Default", view_name)
                    view_full_name = self.columnar_cbas_utils.format_name(view_full_name)
                    execute_cmd = self.generate_create_view_cmd(view_full_name, view_defn)
                elif priv == "view_drop":
                    view_name = self.generate_random_entity_name(type="view")
                    view_full_name = "{}.{}".format("Default", view_name)
                    view_full_name = self.columnar_cbas_utils.format_name(view_full_name)
                    result = self.columnar_cbas_utils.create_analytics_view(
                        self.cluster,
                        view_full_name,
                        view_defn)
                    if not result:
                        self.log.error("Failed to create view")
                        self.fail("Failed to create analytics view {}".format(view_full_name))
                    execute_cmd = "drop analytics view {0}".format(view_full_name)

                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.cluster.instance_id,
                                                              execute_cmd)
                if self.test_users[user]["role"] == "organizationOwner":
                    self.assertEqual(200, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code,
                                                                                 200))
                else:
                    self.assertEqual(403, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 403,
                                                               self.test_users[user]["role"]))

        user = self.test_users['User3']
        for role in self.project_roles:
            self.log.info("========== CLOUD USER TEST CASE: {} ===========".
                          format(role))
            self.log.info(
                "Adding user to project {} with role as {}".format(self.tenant.project_id,
                                                                   role))
            payload = {
                "resourceId": self.tenant.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }

            resp = self.capellaAPIv2.add_user_to_project(self.tenant.id,
                                                         json.dumps(payload))
            self.assertEqual(200, resp.status_code,
                             msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code,
                                                                         200))

            self.columnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                               user["mailid"],
                                               user["password"])
            for priv in self.view_ddl_privileges:
                if priv == "view_create":
                    view_name = self.generate_random_entity_name(type="view")
                    view_full_name = "{}.{}".format("Default", view_name)
                    view_full_name = self.columnar_cbas_utils.format_name(view_full_name)
                    execute_cmd = self.generate_create_view_cmd(view_full_name, view_defn)
                elif priv == "view_drop":
                    view_name = self.generate_random_entity_name(type="view")
                    view_full_name = "{}.{}".format("Default", view_name)
                    view_full_name = self.columnar_cbas_utils.format_name(view_full_name)
                    result = self.columnar_cbas_utils.create_analytics_view(
                        self.cluster,
                        view_full_name,
                        view_defn)
                    if not result:
                        self.log.error("Failed to create view")
                        self.fail("Failed to create analytics view {}".format(view_full_name))
                    execute_cmd = "drop analytics view {0}".format(view_full_name)

                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.cluster.instance_id,
                                                              execute_cmd)
                if role == "projectOwner" or role == "projectDataWriter":
                    self.assertEqual(200, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 200, role))
                else:
                    self.assertEqual(403, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 403, role))

    def test_rbac_view_select(self):
        self.log.info("RBAC views test started")
        self.build_cbas_columnar_infra()

        datasets = self.columnar_cbas_utils.get_all_dataset_objs("standalone")

        no_of_docs = self.input.param("no_of_docs", 1000)
        jobs = Queue()
        results = []
        for dataset in datasets:
            self.log.info("Adding {} documents in standalone dataset {}. Default doc size is 1KB".
                          format(no_of_docs, dataset.full_name))
            jobs.put((self.columnar_cbas_utils.load_doc_to_standalone_collection,
                      {"cluster": self.cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name
                          , "no_of_docs": no_of_docs}))

        self.columnar_cbas_utils.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Some documents were not inserted")

        scopes = self.columnar_cbas_utils.get_all_dataverses_from_metadata(self.cluster)
        view_collection = datasets[0]

        num_views = self.input.param("num_of_views", 1)
        view_defn = "SELECT name, email from {0}".format(
            self.columnar_cbas_utils.format_name(view_collection.full_name))

        for i in range(num_views):
            view_name = self.generate_random_entity_name(type="view")
            view_name = self.columnar_cbas_utils.format_name(view_name)
            result = self.columnar_cbas_utils.create_analytics_view(
                            self.cluster,
                            view_name,
                            view_defn)
            if not result:
                self.fail("Failed to create analytics view {}".format(view_name))

        views = self.columnar_cbas_utils.get_all_views_from_metadata(self.cluster)

        self.log.info("Views: {}".format(views))
        testcases = self.create_rbac_testcases(self.view_select_privileges,
                                               views,
                                               "view")

        for idx, test_case in enumerate(testcases):
            self.log.info("========== TEST CASE {} ===========".format(idx))
            self.log.info(test_case['description'])
            if test_case['validate_err_msg']:
                self.log.info("EXPECTED: ERROR")
            else:
                self.log.info("EXPECTED: SUCCESS")

            privileges = test_case['privileges']
            resources = test_case['resources']

            for res in resources:
                select_cmd = "SELECT * FROM {0}". \
                            format(self.columnar_cbas_utils.format_name(res))
                expected_error = self.ACCESS_DENIED_ERR
                status, metrics, errors, results, _, warnings = (
                            self.columnar_cbas_utils.execute_statement_on_cbas_util(
                                self.cluster, select_cmd,
                                username=test_case['user'].username,
                                password=test_case['user'].password,
                                timeout=300, analytics_timeout=300))

                if test_case['validate_err_msg']:
                    result = self.columnar_cbas_utils.validate_error_and_warning_in_response(
                                status, errors, expected_error, None)
                    if not result:
                        self.fail("Test case failed while attempting to get docs from view {}".
                                format(res))
                else:
                    if status != "success":
                        self.fail("Test case failed while attempting to get docs from view {}".
                                format(res))

        self.log.info("Testing for cloud roles")
        select_cmd = "SELECT * FROM {0}". \
            format(self.columnar_cbas_utils.format_name(views[0]))
        for user in self.test_users:
            self.log.info("========== CLOUD USER TEST CASE: {} ===========".
                          format(self.test_users[user]["role"]))
            self.columnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                               self.test_users[user]["mailid"],
                                               self.test_users[user]["password"])
            for priv in self.view_select_privileges:
                execute_cmd = select_cmd

                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.cluster.instance_id,
                                                              execute_cmd)
                if self.test_users[user]["role"] == "organizationOwner":
                    self.assertEqual(200, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code,
                                                                                 200))
                else:
                    self.assertEqual(403, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 403,
                                                               self.test_users[user]["role"]))

        user = self.test_users['User3']
        for role in self.project_roles:
            self.log.info("========== CLOUD USER TEST CASE: {} ===========".
                          format(role))
            self.log.info(
                "Adding user to project {} with role as {}".format(self.tenant.project_id,
                                                                   role))
            payload = {
                "resourceId": self.tenant.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }

            resp = self.capellaAPIv2.add_user_to_project(self.tenant.id,
                                                         json.dumps(payload))
            self.assertEqual(200, resp.status_code,
                             msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code,
                                                                         200))

            self.columnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                               user["mailid"],
                                               user["password"])
            for priv in self.view_select_privileges:
                execute_cmd = select_cmd

                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.cluster.instance_id,
                                                              execute_cmd)
                if role == "projectOwner" or role == "projectDataWriter" or \
                        role == "projectDataViewer":
                    self.assertEqual(200, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 200, role))
                else:
                    self.assertEqual(403, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 403, role))

    def test_rbac_index(self):
        self.log.info("RBAC index test started")
        self.build_cbas_columnar_infra()
        datasets = self.columnar_cbas_utils.get_all_dataset_objs("standalone")

        no_of_docs = self.input.param("no_of_docs", 1000)
        jobs = Queue()
        results = []
        self.log.info("Adding {} documents in standalone dataset. Default doc size is 1KB". \
                      format(no_of_docs))
        for dataset in datasets:
            jobs.put((self.columnar_cbas_utils.load_doc_to_standalone_collection,
                      {"cluster": self.cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name
                          , "no_of_docs": no_of_docs}))
        self.columnar_cbas_utils.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)
        if not all(results):
            self.log.error("Some documents were not inserted")

        collections = []
        for coll in datasets:
            collections.append(coll.full_name)

        testcases = self.create_rbac_testcases(self.index_privileges, collections,
                                               resource_type="collection")
        index_fields = ["name:string"]
        for idx, test_case in enumerate(testcases):
            self.log.info("========== TEST CASE {} ===========".format(idx))
            self.log.info(test_case['description'])
            if test_case['validate_err_msg']:
                self.log.info("EXPECTED: ERROR")
            else:
                self.log.info("EXPECTED: SUCCESS")

            privileges = test_case['privileges']
            resources = test_case['resources']

            for priv in privileges:
                for res in resources:

                    if priv == "index_create":
                        index_name = self.generate_random_entity_name(type="index")
                        index_name = self.columnar_cbas_utils.format_name(index_name)
                        expected_error = self.ACCESS_DENIED_ERR
                        result = self.columnar_cbas_utils.create_cbas_index(
                            self.cluster,
                            index_name,
                            index_fields,
                            res,
                            validate_error_msg=test_case['validate_err_msg'],
                            expected_error=expected_error,
                            username=test_case['user'].username,
                            password=test_case['user'].password)
                        if not result:
                            self.fail("Test case failed while attempting to create index {} on coll {}".
                                      format(index_name, res))

                    elif priv == "index_drop":
                        index_name = self.generate_random_entity_name(type="index")
                        index_name = self.columnar_cbas_utils.format_name(index_name)
                        expected_error = self.ACCESS_DENIED_ERR
                        result = self.columnar_cbas_utils.create_cbas_index(self.cluster,
                                                                            index_name,
                                                                            index_fields,
                                                                            res)
                        if not result:
                            self.fail("Failed to create index {} on coll {}".
                                      format(index_name, res))
                        result = self.columnar_cbas_utils.drop_cbas_index(
                            self.cluster,
                            index_name,
                            res,
                            validate_error_msg=test_case['validate_err_msg'],
                            expected_error=expected_error,
                            username=test_case['user'].username,
                            password=test_case['user'].password)
                        if not result:
                            self.fail("Test case failed while attempting to drop index {} on coll {}".
                                      format(index_name, res))

        self.log.info("Testing for cloud roles")
        coll = collections[0]
        for user in self.test_users:
            self.log.info("========== CLOUD USER TEST CASE: {} ===========".
                          format(self.test_users[user]["role"]))
            self.columnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                               self.test_users[user]["mailid"],
                                               self.test_users[user]["password"])
            for priv in self.index_privileges:
                if priv == "index_create":
                    index_name = self.generate_random_entity_name(type="index")
                    index_name = self.columnar_cbas_utils.format_name(index_name)
                    execute_cmd = self.columnar_cbas_utils.generate_create_index_cmd(
                        index_name, index_fields, coll)
                elif priv == "index_drop":
                    index_name = self.generate_random_entity_name(type="index")
                    index_name = self.columnar_cbas_utils.format_name(index_name)
                    result = self.columnar_cbas_utils.create_cbas_index(self.cluster,
                                                                        index_name,
                                                                        index_fields,
                                                                        coll)
                    if not result:
                        self.fail("Failed to create index {} on coll {}".
                                  format(index_name, coll))
                    execute_cmd = self.columnar_cbas_utils.generate_drop_index_cmd(
                        index_name, coll)

                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.cluster.instance_id,
                                                              execute_cmd)
                if self.test_users[user]["role"] == "organizationOwner":
                    self.assertEqual(200, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code,
                                                                                 200))
                else:
                    self.assertEqual(403, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 403,
                                                               self.test_users[user]["role"]))

        user = self.test_users['User3']
        for role in self.project_roles:
            self.log.info("========== CLOUD USER TEST CASE: {} ===========".
                          format(role))
            self.log.info(
                "Adding user to project {} with role as {}".format(self.tenant.project_id,
                                                                   role))
            payload = {
                "resourceId": self.tenant.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }

            resp = self.capellaAPIv2.add_user_to_project(self.tenant.id,
                                                         json.dumps(payload))
            self.assertEqual(200, resp.status_code,
                             msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code,
                                                                         200))

            self.columnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                               user["mailid"],
                                               user["password"])
            for priv in self.index_privileges:
                if priv == "index_create":
                    index_name = self.generate_random_entity_name(type="index")
                    index_name = self.columnar_cbas_utils.format_name(index_name)
                    execute_cmd = self.columnar_cbas_utils.generate_create_index_cmd(
                        index_name, index_fields, coll)
                elif priv == "index_drop":
                    index_name = self.generate_random_entity_name(type="index")
                    index_name = self.columnar_cbas_utils.format_name(index_name)
                    result = self.columnar_cbas_utils.create_cbas_index(self.cluster,
                                                                        index_name,
                                                                        index_fields,
                                                                        coll)
                    if not result:
                        self.fail("Failed to create index {} on coll {}".
                                  format(index_name, coll))
                    execute_cmd = self.columnar_cbas_utils.generate_drop_index_cmd(
                        index_name, coll)
                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.cluster.instance_id,
                                                              execute_cmd)
                if role == "projectOwner" or role == "projectDataWriter":
                    self.assertEqual(200, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 200, role))
                else:
                    self.assertEqual(403, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 403, role))

    def test_rbac_udfs(self):
        self.log.info("RBAC test for UDFs started")
        self.build_cbas_columnar_infra()

        scopes = self.columnar_cbas_utils.get_all_dataverses_from_metadata(self.cluster)

        testcases = self.create_rbac_testcases(self.udf_ddl_privileges, scopes,
                                               resource_type="scope")
        function_body = "select 1"

        for idx, test_case in enumerate(testcases):
            self.log.info("========== TEST CASE {} ===========".format(idx))
            self.log.info(test_case['description'])
            if test_case['validate_err_msg']:
                self.log.info("EXPECTED: ERROR")
            else:
                self.log.info("EXPECTED: SUCCESS")

            privileges = test_case['privileges']
            resources = test_case['resources']

            for priv in privileges:
                for res in resources:
                    if priv == "function_create":
                        udf_name = self.generate_random_entity_name(type="udf")
                        udf_name = self.columnar_cbas_utils.format_name(udf_name)
                        expected_error = self.ACCESS_DENIED_ERR
                        result = self.columnar_cbas_utils.create_udf(
                            self.cluster,
                            udf_name,
                            res,
                            None,
                            body=function_body,
                            validate_error_msg=test_case['validate_err_msg'],
                            expected_error=expected_error,
                            username=test_case['user'].username,
                            password=test_case['user'].password)
                        if not result:
                            self.fail("Test case failed while attempting to create function " \
                                      " {} in {}".format(udf_name, res))
                    elif priv == "function_drop":
                        udf_name = self.generate_random_entity_name(type="udf")
                        udf_name = self.columnar_cbas_utils.format_name(udf_name)
                        expected_error = self.ACCESS_DENIED_ERR
                        result = self.columnar_cbas_utils.create_udf(
                            self.cluster,
                            udf_name,
                            res,
                            None,
                            body=function_body)
                        if not result:
                            self.fail("Failed to create udf function {} in {}".format(udf_name,
                                                                                      res))
                        result = self.columnar_cbas_utils.drop_udf(
                            self.cluster,
                            udf_name,
                            res,
                            None,
                            validate_error_msg=test_case['validate_err_msg'],
                            expected_error=expected_error,
                            username=test_case['user'].username,
                            password=test_case['user'].password)
                        if not result:
                            self.fail("Test case failed while attempting to drop function "
                                      " {} in {} ".format(udf_name, res))

        self.log.info("Testing for cloud roles")
        for user in self.test_users:
            self.log.info("========== CLOUD USER TEST CASE: {} ===========".
                          format(self.test_users[user]["role"]))
            self.columnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                               self.test_users[user]["mailid"],
                                               self.test_users[user]["password"])
            for priv in self.udf_ddl_privileges:
                if priv == "function_create":
                    udf_name = self.generate_random_entity_name(type="udf")
                    udf_name = self.columnar_cbas_utils.format_name(udf_name)
                    execute_cmd = self.columnar_cbas_utils.generate_create_udf_cmd(
                        udf_name, "Default.Default", body=function_body)
                elif priv == "function_drop":
                    udf_name = self.generate_random_entity_name(type="udf")
                    udf_name = self.columnar_cbas_utils.format_name(udf_name)
                    result = self.columnar_cbas_utils.create_udf(
                        self.cluster,
                        udf_name,
                        "Default.Default",
                        None,
                        body=function_body)
                    if not result:
                        self.fail("Failed to create udf function {} in {}".format(udf_name,
                                                                                  res))
                    execute_cmd = self.columnar_cbas_utils.generate_drop_udf_cmd(
                        udf_name, "Default.Default")

                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.cluster.instance_id,
                                                              execute_cmd)
                if self.test_users[user]["role"] == "organizationOwner":
                    self.assertEqual(200, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code,
                                                                                 200))
                else:
                    self.assertEqual(403, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 403,
                                                               self.test_users[user]["role"]))

        user = self.test_users['User3']
        for role in self.project_roles:
            self.log.info("========== CLOUD USER TEST CASE: {} ===========".
                          format(role))
            self.log.info(
                "Adding user to project {} with role as {}".format(self.tenant.project_id,
                                                                   role))
            payload = {
                "resourceId": self.tenant.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }

            resp = self.capellaAPIv2.add_user_to_project(self.tenant.id,
                                                         json.dumps(payload))
            self.assertEqual(200, resp.status_code,
                             msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code,
                                                                         200))

            self.columnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                               user["mailid"],
                                               user["password"])
            for priv in self.udf_ddl_privileges:
                if priv == "function_create":
                    udf_name = self.generate_random_entity_name(type="udf")
                    udf_name = self.columnar_cbas_utils.format_name(udf_name)
                    execute_cmd = self.columnar_cbas_utils.generate_create_udf_cmd(
                        udf_name, "Default.Default", body=function_body)
                elif priv == "function_drop":
                    udf_name = self.generate_random_entity_name(type="udf")
                    udf_name = self.columnar_cbas_utils.format_name(udf_name)
                    result = self.columnar_cbas_utils.create_udf(
                        self.cluster,
                        udf_name,
                        "Default.Default",
                        None,
                        body=function_body)
                    if not result:
                        self.fail("Failed to create udf function {} in {}".format(udf_name,
                                                                                  res))
                    execute_cmd = self.columnar_cbas_utils.generate_drop_udf_cmd(
                        udf_name, "Default.Default")

                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.cluster.instance_id,
                                                              execute_cmd)
                if role == "projectOwner" or role == "projectDataWriter":
                    self.assertEqual(200, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 200, role))
                else:
                    self.assertEqual(403, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 403, role))

    def test_rbac_udf_execute(self):
        self.log.info("RBAC udf execute test started")

        analytics_udfs = []
        num_of_udfs = self.input.param('num_of_udfs', 2)
        for i in range(num_of_udfs):
            udf_name = self.generate_random_entity_name(type="udf")
            udf_name = self.columnar_cbas_utils.format_name(udf_name)
            function_body = "select 1"
            result = self.columnar_cbas_utils.create_udf(
                self.cluster,
                udf_name,
                "Default.Default",
                None,
                body=function_body)
            if not result:
                self.fail("Failed to create UDF {}".format(udf_name))
            udf_name_full_name = "Default.Default." + udf_name
            analytics_udfs.append(udf_name_full_name)

        testcases = self.create_rbac_testcases(self.udf_execute_privileges,
                                               analytics_udfs,
                                               resource_type="function")

        for idx, test_case in enumerate(testcases):
            self.log.info("========== TEST CASE {} ===========".format(idx))
            self.log.info(test_case['description'])
            if test_case['validate_err_msg']:
                self.log.info("EXPECTED: ERROR")
            else:
                self.log.info("EXPECTED: SUCCESS")

            privileges = test_case['privileges']
            resources = test_case['resources']

            for res in resources:
                res = self.columnar_cbas_utils.format_name(res)
                execute_cmd = "{}()".format(res)
                full_resource_name = self.columnar_cbas_utils.unformat_name(res). \
                                         replace(".", ":") + ":0"
                expected_error = self.ACCESS_DENIED_ERR
                status, metrics, errors, results, _, _ = (
                    self.columnar_cbas_utils.execute_statement_on_cbas_util(
                        self.cluster, execute_cmd,
                        username=test_case['user'].username,
                        password=test_case['user'].password,
                        timeout=300, analytics_timeout=300))

                if test_case['validate_err_msg']:
                    result = self.columnar_cbas_utils.validate_error_and_warning_in_response(
                        status, errors, expected_error, None)
                    if not result:
                        self.fail("Test case failed while attempting to execute function {}".
                                  format(res))
                else:
                    if status != "success":
                        self.fail("Test case failed while attempting to execute function {}".
                                  format(res))

        self.log.info("Testing for cloud roles")
        func_name = analytics_udfs[0]
        func_name = self.columnar_cbas_utils.format_name(func_name)
        for user in self.test_users:
            self.log.info("========== CLOUD USER TEST CASE: {} ===========".
                          format(self.test_users[user]["role"]))
            self.columnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                               self.test_users[user]["mailid"],
                                               self.test_users[user]["password"])
            for priv in self.udf_execute_privileges:
                execute_cmd = "{}()".format(func_name)

                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.cluster.instance_id,
                                                              execute_cmd)
                if self.test_users[user]["role"] == "organizationOwner":
                    self.assertEqual(200, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code,
                                                                                 200))
                else:
                    self.assertEqual(403, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 403,
                                                               self.test_users[user]["role"]))

        user = self.test_users['User3']
        for role in self.project_roles:
            self.log.info("========== CLOUD USER TEST CASE: {} ===========".
                          format(role))
            self.log.info(
                "Adding user to project {} with role as {}".format(self.tenant.project_id,
                                                                   role))
            payload = {
                "resourceId": self.tenant.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }

            resp = self.capellaAPIv2.add_user_to_project(self.tenant.id,
                                                         json.dumps(payload))
            self.assertEqual(200, resp.status_code,
                             msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code,
                                                                         200))

            self.columnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                               user["mailid"],
                                               user["password"])
            for priv in self.udf_execute_privileges:
                execute_cmd = "{}()".format(func_name)

                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.cluster.instance_id,
                                                              execute_cmd)
                if role == "projectOwner" or role == "projectDataWriter":
                    self.assertEqual(200, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 200, role))
                else:
                    self.assertEqual(403, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 403, role))

    def tearDown(self):
        if self.sink_s3_bucket_name:
            self.log.info("Deleting s3 bucket")
            self.delete_s3_bucket()

        self.columnar_cbas_utils.cleanup_cbas(self.cluster)

        self.delete_different_organization_roles()
        if not self.cbas_util.delete_cbas_infra_created_from_spec(
                self.cluster, self.columnar_spec):
            self.fail("Error while deleting cbas entities")

        for role in self.cluster.columnar_roles:
            result = self.columnar_rbac_util.delete_columnar_role(self.pod, self.tenant,
                                                                  self.tenant.project_id,
                                                                  self.cluster,
                                                                  role.id)
            if not result:
                self.log.error("Failed to delete columnar role {}".format(role.id))

        for db_user in self.cluster.db_users:
            result = self.columnar_rbac_util.delete_api_keys(self.pod, self.tenant,
                                                             self.tenant.project_id,
                                                             self.cluster,
                                                             db_user.id)
            if not result:
                self.log.error("Failed to delete user {}".format(db_user.id))

        super(ColumnarBaseTest, self).tearDown()