import json
import random
import string
from queue import Queue
import time

from Columnar.columnar_base import ColumnarBaseTest
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI
from capella_utils.columnar import ColumnarUtils, ColumnarRBACUtil
from capellaAPI.capella.columnar.CapellaAPI import CapellaAPI as ColumnarAPI
from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI as CapellaAPIv2
from Jython_tasks.sirius_task import CouchbaseUtil

from awsLib.s3_data_helper import perform_S3_operation


def generate_random_password(length=12):
    """Generate a random password."""
    password_characters = string.ascii_letters + string.digits
    password = ''.join(random.choice(password_characters) for i in range(length))
    password += "!123Aa"
    return password


def generate_random_entity_name(length=5, type="database"):
    """Generate random database name."""
    base_name = "TAF-" + type
    entity_id = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(length))
    entity_name = base_name + "-" + entity_id
    return entity_name


def generate_create_view_cmd(view_full_name, view_defn, if_not_exists=False):
    cmd = "create analytics view {0}".format(view_full_name)

    if if_not_exists:
        cmd += " If Not Exists"

    cmd += " as {0};".format(view_defn)

    return cmd


class ColumnarRBAC(ColumnarBaseTest):
    def __init__(self, methodName: str = "runTest"):
        super().__init__(methodName)
        self.pod = None
        self.tenant = None

    def setUp(self):
        super(ColumnarRBAC, self).setUp()
        self.columnar_cluster = self.tenant.columnar_instances[0]
        self.remote_cluster = None
        if len(self.tenant.clusters) > 0:
            self.remote_cluster = self.tenant.clusters[0]
            self.couchbase_doc_loader = CouchbaseUtil(
                task_manager=self.task_manager,
                hostname=self.remote_cluster.master.ip,
                username=self.remote_cluster.master.rest_username,
                password=self.remote_cluster.master.rest_password,
            )
        self.no_of_docs = self.input.param("no_of_docs", 1000)
        self.sink_s3_bucket_name = None
        self.capellaAPIv2 = CapellaAPIv2(self.pod.url_public, self.tenant.api_secret_key,
                                         self.tenant.api_access_key, self.tenant.user,
                                         self.tenant.pwd)
        self.columnarAPI = ColumnarAPI(self.pod.url_public, '', '', self.tenant.user,
                                       self.tenant.pwd, '')
        self.columnar_utils = ColumnarUtils(self.log)
        self.columnar_rbac_util = ColumnarRBACUtil(self.log)
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
            self.columnar_spec_name = "full_template"

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
            self.capellaAPI.cluster_ops_apis.get_cluster_certificate(self.tenant.id, self.tenant.project_id,
                                                                     remote_cluster.id))
        if remote_cluster_certificate_request.status_code == 200:
            remote_cluster_certificate = (remote_cluster_certificate_request.json()["certificate"])
        else:
            self.fail("Failed to get cluster certificate")

        return remote_cluster_certificate

    def create_s3_sink_bucket(self):
        for i in range(5):
            try:
                self.sink_s3_bucket_name = "copy-to-s3-rbac-" + str(random.randint(1, 100000))
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

        # populate spec file for the entities to be created
        self.columnar_spec = self.populate_columnar_infra_spec(
            columnar_spec=self.cbas_util.get_columnar_spec(
                self.columnar_spec_name),
            remote_cluster=self.remote_cluster,
            external_collection_file_formats=["json"])

        if self.input.param("primary_key", None) is not None:
            self.columnar_spec["standalone_dataset"]["primary_key"] = json.loads(self.input.param("primary_key"))
        else:
            self.columnar_spec["standalone_dataset"]["primary_key"] = [{"name": "string", "email": "string"}]

        # create entities on columnar cluster based on spec file
        result, msg = self.cbas_util.create_cbas_infra_from_spec(
            cluster=self.columnar_cluster, cbas_spec=self.columnar_spec,
            bucket_util=self.bucket_util, wait_for_ingestion=False,
            remote_clusters=[self.remote_cluster])
        if not result:
            self.fail(str(msg))

    def generate_test_case(self, user, privileges, resources, validate_err_msg=False,
                           denied_resources=None):

        if denied_resources is None:
            denied_resources = []
        test_case = {"user": user, "privileges": privileges,
                     "description": "Test {} on {} for user {}".format(','.join(privileges),
                                                                       ','.join(resources),
                                                                       user), "resources": resources,
                     "validate_err_msg": validate_err_msg}

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
            resources[idx] = self.cbas_util.unformat_name(res)

        username = generate_random_entity_name(type="user")
        password = generate_random_password()
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
                                                        self.tenant.project_id, self.columnar_cluster,
                                                        username=username,
                                                        password=password,
                                                        privileges_payload=privileges_payload)
        test_case = self.generate_test_case(user1, privileges, resources)
        testcases.append(test_case)

        role_name = generate_random_entity_name(type="role")
        role1 = self.columnar_rbac_util.create_columnar_role(self.pod, self.tenant,
                                                             self.tenant.project_id, self.columnar_cluster,
                                                             role_name=role_name,
                                                             privileges_payload=privileges_payload)
        username = generate_random_entity_name(type="user")
        password = generate_random_password()
        user2 = self.columnar_rbac_util.create_api_keys(self.pod, self.tenant,
                                                        self.tenant.project_id, self.columnar_cluster,
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

        username = generate_random_entity_name(type="user")
        password = generate_random_password()
        user3 = self.columnar_rbac_util.create_api_keys(self.pod, self.tenant,
                                                        self.tenant.project_id, self.columnar_cluster,
                                                        username=username,
                                                        password=password,
                                                        privileges_payload=privileges_payload)
        negative_test_case = self.generate_test_case(user3, privileges, [test_resource],
                                                     validate_err_msg=True)
        testcases.append(negative_test_case)

        role_name = generate_random_entity_name(type="role")
        role2 = self.columnar_rbac_util.create_columnar_role(self.pod, self.tenant,
                                                             self.tenant.project_id, self.columnar_cluster,
                                                             role_name=role_name,
                                                             privileges_payload=privileges_payload)
        username = generate_random_entity_name(type="user")
        password = generate_random_password()
        user4 = self.columnar_rbac_util.create_api_keys(self.pod, self.tenant,
                                                        self.tenant.project_id, self.columnar_cluster,
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

        cmd = self.cbas_util.generate_create_external_dataset_cmd(
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

        if self.cbas_util.create_dataset_on_external_resource(
                self.columnar_cluster, dataset_obj.name,
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

    def create_backup_wait_for_complete(self, retention=0, timeout=3600):
        self.log.info("Starting backup")
        resp = self.columnar_utils.create_backup(
            pod=self.pod, tenant=self.tenant,
            project_id=self.tenant.project_id,
            instance=self.columnar_cluster, retention_time=retention)
        if resp is None:
            self.fail("Unable to schedule backup")
        else:
            backup_id = resp["id"]

        self.log.info("Backup Id: {}".format(backup_id))
        if not self.columnar_utils.wait_for_backup_to_complete(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster,
                backup_id=backup_id, timeout=timeout):
            self.fail("Backup failed.")
        return backup_id

    def restore_wait_for_complete(self, backup_id, timeout=3600):
        self.log.info("Restoring backup")
        resp = self.columnar_utils.restore_backup(
            pod=self.pod, tenant=self.tenant,
            project_id=self.tenant.project_id, instance=self.columnar_cluster,
            backup_id=backup_id)

        if resp is None:
            self.fail("Unable to start restore")
        else:
            restore_id = resp["id"]

        if not self.columnar_utils.wait_for_restore_to_complete(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster,
                restore_id=restore_id, timeout=timeout):
            self.fail("Unable to restore backup taken before the upgrade.")

    def scale_columnar_cluster(self, nodes, timeout=3600):
        self.log.info("Scaling columnar cluster")
        if not self.columnar_utils.scale_instance(
                pod=self.pod, tenant=self.tenant,
                project_id=self.tenant.project_id,
                instance=self.columnar_cluster,
                nodes=nodes):
            self.fail("Unable to initiate cluster scale operation")

    def test_rbac_database(self):
        self.log.info("RBAC test for database started")

        testcases = self.create_rbac_testcases(self.database_privileges, [],
                                               "instance")

        if self.cluster_on_off:
            if not self.columnar_utils.turn_off_instance(
                    pod=self.pod, tenant=self.tenant,
                    project_id=self.tenant.project_id,
                    instance=self.columnar_cluster, wait_to_turn_off=True):
                self.fail("Failed to Turn-Off the cluster")

            if not self.columnar_utils.turn_on_instance(
                    pod=self.pod, tenant=self.tenant,
                    project_id=self.tenant.project_id,
                    instance=self.columnar_cluster, wait_to_turn_on=True):
                self.fail("Failed to Turn-On the cluster")

        if self.cluster_backup_restore:
            backup_id = self.create_backup_wait_for_complete()
            self.restore_wait_for_complete(backup_id)
            if not self.columnar_utils.wait_for_instance_to_be_healthy(
                    pod=self.pod, tenant=self.tenant,
                    instance=self.columnar_cluster):
                self.fail("Cluster is not is healthy state")
            if not self.columnar_utils.allow_ip_on_instance(
                    pod=self.pod, tenant=self.tenant,
                    project_id=self.tenant.project_id,
                    instance=self.columnar_cluster):
                self.fail("Unable to set Allowed IP post restoring backup")

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
                    database_name = generate_random_entity_name(type="database")
                    expected_error = self.ACCESS_DENIED_ERR
                    result = self.cbas_util.create_database(
                        self.columnar_cluster,
                        database_name,
                        username=test_case['user'].username,
                        password=test_case['user'].password,
                        validate_error_msg=test_case['validate_err_msg'],
                        expected_error=expected_error)
                    if not result:
                        self.fail("Test case failed while attempting to create database")

                elif priv == "DROP":
                    database_name = generate_random_entity_name(type="database")
                    expected_error = self.ACCESS_DENIED_ERR
                    result = self.cbas_util.create_database(self.columnar_cluster,
                                                                      database_name)
                    if not result:
                        self.fail("Failed to create database")
                    result = self.cbas_util.drop_database(
                        self.columnar_cluster,
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
                execute_cmd = ""
                if priv == "database_create":
                    database_name = generate_random_entity_name(type="database")
                    database_name = self.cbas_util.format_name(database_name)
                    execute_cmd = create_database_cmd.format(database_name)
                elif priv == "database_drop":
                    database_name = generate_random_entity_name(type="database")
                    database_name = self.cbas_util.format_name(database_name)
                    result = self.cbas_util.create_database(self.columnar_cluster,
                                                                      database_name)
                    if not result:
                        self.fail("Failed to create database")
                    execute_cmd = drop_database_cmd.format(database_name)
                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.columnar_cluster.instance_id,
                                                              execute_cmd)
                if self.test_users[user]["role"] == "organizationOwner":
                    self.assertEqual(200, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}' \
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
                execute_cmd = ""
                if priv == "database_create":
                    database_name = generate_random_entity_name(type="database")
                    database_name = self.cbas_util.format_name(database_name)
                    execute_cmd = create_database_cmd.format(database_name)
                elif priv == "database_drop":
                    database_name = generate_random_entity_name(type="database")
                    database_name = self.cbas_util.format_name(database_name)
                    result = self.cbas_util.create_database(self.columnar_cluster,
                                                                      database_name)
                    if not result:
                        self.fail("Failed to create database")
                    execute_cmd = drop_database_cmd.format(database_name)
                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.columnar_cluster.instance_id,
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
                                                             self.columnar_cluster,
                                                             user.id)
            if not result:
                self.log.error("Failed to delete user {}".format(user.username))

            database_name = generate_random_entity_name(type="database")
            result = self.cbas_util.create_database(
                self.columnar_cluster,
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

        databases = self.cbas_util.get_all_databases_from_metadata(self.columnar_cluster)

        testcases = self.create_rbac_testcases(self.scope_privileges, databases,
                                               "database")

        if self.cluster_on_off:
            if not self.columnar_utils.turn_off_instance(
                    pod=self.pod, tenant=self.tenant,
                    project_id=self.tenant.project_id,
                    instance=self.columnar_cluster, wait_to_turn_off=True):
                self.fail("Failed to Turn-Off the cluster")

            if not self.columnar_utils.turn_on_instance(
                    pod=self.pod, tenant=self.tenant,
                    project_id=self.tenant.project_id,
                    instance=self.columnar_cluster, wait_to_turn_on=True):
                self.fail("Failed to Turn-On the cluster")

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
                    scope_name = generate_random_entity_name(type="scope")

                    if priv == "scope_create":
                        expected_error = self.ACCESS_DENIED_ERR
                        result = self.cbas_util.create_scope(
                            self.columnar_cluster, scope_name,
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
                        result = self.cbas_util.create_dataverse(self.columnar_cluster,
                                                                           scope_name,
                                                                           res)
                        if not result:
                            self.fail("Failed to create scope {}".format(scope_name))
                        result = self.cbas_util.drop_dataverse(
                            self.columnar_cluster,
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
                execute_cmd = ""
                if priv == "scope_create":
                    database_name = random.choice(databases)
                    scope_name = generate_random_entity_name(type="scope")
                    scope_name = "{}.{}".format(database_name, scope_name)
                    scope_name = self.cbas_util.format_name(scope_name)
                    execute_cmd = create_scope_cmd.format(scope_name)
                elif priv == "scope_drop":
                    database_name = random.choice(databases)
                    scope_name = generate_random_entity_name(type="scope")
                    result = self.cbas_util.create_dataverse(self.columnar_cluster,
                                                                       scope_name,
                                                                       database_name)
                    if not result:
                        self.fail("Failed to create scope {}".format(scope_name))
                    scope_name = "{}.{}".format(database_name, scope_name)
                    scope_name = self.cbas_util.format_name(scope_name)
                    execute_cmd = drop_scope_cmd.format(scope_name)

                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.columnar_cluster.instance_id,
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
                execute_cmd = ""
                if priv == "scope_create":
                    database_name = random.choice(databases)
                    scope_name = generate_random_entity_name(type="scope")
                    scope_name = "{}.{}".format(database_name, scope_name)
                    scope_name = self.cbas_util.format_name(scope_name)
                    execute_cmd = create_scope_cmd.format(scope_name)
                elif priv == "scope_drop":
                    database_name = random.choice(databases)
                    scope_name = generate_random_entity_name(type="scope")
                    result = self.cbas_util.create_dataverse(self.columnar_cluster,
                                                                       scope_name,
                                                                       database_name)
                    if not result:
                        self.fail("Failed to create scope {}".format(scope_name))
                    scope_name = "{}.{}".format(database_name, scope_name)
                    scope_name = self.cbas_util.format_name(scope_name)
                    execute_cmd = drop_scope_cmd.format(scope_name)
                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.columnar_cluster.instance_id,
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
        self.create_bucket_scopes_collections_in_capella_cluster(
            self.tenant, self.remote_cluster,
            self.input.param("num_buckets", 1))
        self.build_cbas_columnar_infra()

        scopes = self.cbas_util.get_all_dataverses_from_metadata(self.columnar_cluster)

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
                    remote_links = self.cbas_util.get_all_link_objs("couchbase")
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
                        remote_coll_obj = self.cbas_util.create_remote_dataset_obj(
                            cluster=self.columnar_cluster,
                            bucket=self.remote_cluster.buckets[0].name,
                            scope="_default", collection="_default",
                            link=remote_link, use_only_existing_db=True,
                            use_only_existing_dv=True, database=database_name,
                            dataverse=scope_name, capella_as_source=True)[0]
                        expected_error = self.ACCESS_DENIED_ERR

                        result = self.cbas_util.create_remote_dataset(
                            self.columnar_cluster,
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
                        external_coll_obj = self.cbas_util.create_external_dataset_obj(
                            self.columnar_cluster,
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
                        standalone_coll_obj = self.cbas_util.create_standalone_dataset_obj(
                            self.columnar_cluster,
                            primary_key={"name": "string", "email": "string"},
                            database_name=database_name,
                            dataverse_name=scope_name)[0]

                        expected_error = self.ACCESS_DENIED_ERR

                        result = self.cbas_util.create_standalone_collection(
                            self.columnar_cluster,
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
                            result = self.cbas_util.drop_dataset(
                                self.columnar_cluster, remote_coll_obj.full_name)
                            if not result:
                                self.log.error("Failed to drop coll {}".format(remote_coll_obj.full_name))

                            result = self.cbas_util.drop_dataset(
                                self.columnar_cluster, external_coll_obj.full_name)
                            if not result:
                                self.log.error("Failed to drop coll {}".format(external_coll_obj.full_name))

                            result = self.cbas_util.drop_dataset(
                                self.columnar_cluster, standalone_coll_obj.full_name)
                            if not result:
                                self.log.error("Failed to drop coll {}".format(standalone_coll_obj.full_name))

                    elif priv == "collection_drop":
                        # create and drop standalone collection
                        self.log.info("Test dropping remote collection")
                        remote_coll_obj = self.cbas_util.create_remote_dataset_obj(
                            self.columnar_cluster,
                            self.remote_cluster.buckets[0].name,
                            "_default",
                            "_default",
                            remote_link,
                            use_only_existing_db=True,
                            use_only_existing_dv=True,
                            database=database_name,
                            dataverse=scope_name,
                            capella_as_source=True)[0]

                        expected_error = self.ACCESS_DENIED_ERR
                        result = self.cbas_util.create_remote_dataset(
                            self.columnar_cluster,
                            remote_coll_obj.name,
                            remote_coll_obj.full_kv_entity_name,
                            remote_coll_obj.link_name,
                            remote_coll_obj.dataverse_name,
                            remote_coll_obj.database_name)
                        if not result:
                            self.fail("Failed to create remote collection {}".format(
                                remote_coll_obj.name))

                        result = self.cbas_util.drop_dataset(
                            self.columnar_cluster, remote_coll_obj.full_name,
                            username=test_case['user'].username,
                            password=test_case['user'].password,
                            validate_error_msg=test_case['validate_err_msg'],
                            expected_error=expected_error)
                        if not result:
                            self.fail("Test case failed while attempting to drop remote dataset {}"
                                      .format(remote_coll_obj.name))

                        # create and drop external collection
                        self.log.info("Test dropping external collection")
                        external_coll_obj = self.cbas_util.create_external_dataset_obj(
                            self.columnar_cluster,
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

                        result = self.cbas_util.drop_dataset(
                            self.columnar_cluster, external_coll_obj.full_name,
                            username=test_case['user'].username,
                            password=test_case['user'].password,
                            validate_error_msg=test_case['validate_err_msg'],
                            expected_error=expected_error)
                        if not result:
                            self.fail("Test case failed while attempting to drop external collection {}".
                                      format(external_coll_obj.name))

                        # create and drop standalone collection
                        self.log.info("Test dropping standalone collection")
                        standalone_coll_obj = self.cbas_util.create_standalone_dataset_obj(
                            self.columnar_cluster,
                            primary_key={"name": "string", "email": "string"},
                            database_name=database_name,
                            dataverse_name=scope_name)[0]

                        expected_error = self.ACCESS_DENIED_ERR
                        result = self.cbas_util.create_standalone_collection(
                            self.columnar_cluster,
                            standalone_coll_obj.name,
                            dataverse_name=standalone_coll_obj.dataverse_name,
                            database_name=standalone_coll_obj.database_name,
                            primary_key=standalone_coll_obj.primary_key)
                        if not result:
                            self.fail("Failed to create standalone collection")

                        result = self.cbas_util.drop_dataset(
                            self.columnar_cluster, standalone_coll_obj.full_name,
                            username=test_case['user'].username,
                            password=test_case['user'].password,
                            validate_error_msg=test_case['validate_err_msg'],
                            expected_error=expected_error)
                        if not result:
                            self.fail("Test case failed while attempting to drop external collection {}".
                                      format(standalone_coll_obj.name))

                        if test_case['validate_err_msg']:
                            result = self.cbas_util.drop_dataset(
                                self.columnar_cluster, remote_coll_obj.full_name)
                            if not result:
                                self.log.error("Failed to drop coll {}".format(remote_coll_obj.full_name))

                            result = self.cbas_util.drop_dataset(
                                self.columnar_cluster, external_coll_obj.full_name)
                            if not result:
                                self.log.error("Failed to drop coll {}".format(external_coll_obj.full_name))

                            result = self.cbas_util.drop_dataset(
                                self.columnar_cluster, standalone_coll_obj.full_name)
                            if not result:
                                self.log.error("Failed to drop coll {}".format(standalone_coll_obj.full_name))

        ######### TEST FOR CLOUD ROLES #########
        def generate_commands(priv):
            if priv == "collection_create":
                execute_commands = []
                # REMOTE COLLECTION
                remote_coll_obj = self.cbas_util.create_remote_dataset_obj(
                    self.columnar_cluster,
                    self.remote_cluster.buckets[0].name,
                    "_default",
                    "_default",
                    remote_link,
                    use_only_existing_db=True,
                    use_only_existing_dv=True,
                    database="Default",
                    dataverse="Default",
                    capella_as_source=True)[0]

                cmd = self.cbas_util.generate_create_dataset_cmd(
                    remote_coll_obj.name,
                    remote_coll_obj.full_kv_entity_name,
                    remote_coll_obj.dataverse_name,
                    remote_coll_obj.database_name,
                    link_name=remote_coll_obj.link_name)
                execute_commands.append(cmd)

                # EXTERNAL COLLECTION
                external_coll_obj = self.cbas_util.create_external_dataset_obj(
                    self.columnar_cluster,
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

                # STANDALONE COLLECTION
                standalone_coll_obj = self.cbas_util.create_standalone_dataset_obj(
                    self.columnar_cluster,
                    primary_key={"name": "string", "email": "string"},
                    database_name="Default",
                    dataverse_name="Default")[0]
                cmd = self.cbas_util.generate_standalone_create_DDL(
                    standalone_coll_obj.name,
                    dataverse_name=standalone_coll_obj.dataverse_name,
                    database_name=standalone_coll_obj.database_name,
                    primary_key=standalone_coll_obj.primary_key)
                execute_commands.append(cmd)

            elif priv == "collection_drop":
                execute_commands = []
                drop_cmd = "DROP DATASET {}"

                # REMOTE COLLECTION
                remote_coll_obj = self.cbas_util.create_remote_dataset_obj(
                    self.columnar_cluster,
                    self.remote_cluster.buckets[0].name,
                    "_default",
                    "_default",
                    remote_link,
                    use_only_existing_db=True,
                    use_only_existing_dv=True,
                    database="Default",
                    dataverse="Default",
                    capella_as_source=True)[0]
                result = self.cbas_util.create_remote_dataset(
                    self.columnar_cluster,
                    remote_coll_obj.name,
                    remote_coll_obj.full_kv_entity_name,
                    remote_coll_obj.link_name,
                    remote_coll_obj.dataverse_name,
                    remote_coll_obj.database_name)
                if not result:
                    self.fail("Failed to create remote collection {}".format(
                        remote_coll_obj.name))
                cmd = drop_cmd.format(self.cbas_util.format_name(
                    remote_coll_obj.name))
                execute_commands.append(cmd)

                # EXTERNAL COLLECTION
                external_coll_obj = self.cbas_util.create_external_dataset_obj(
                    self.columnar_cluster,
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
                cmd = drop_cmd.format(self.cbas_util.format_name(
                    external_coll_obj.name))
                execute_commands.append(cmd)

                # STANDALONE COLLECTION
                standalone_coll_obj = self.cbas_util.create_standalone_dataset_obj(
                    self.columnar_cluster,
                    primary_key={"name": "string", "email": "string"},
                    database_name="Default",
                    dataverse_name="Default")[0]
                result = self.cbas_util.create_standalone_collection(
                    self.columnar_cluster,
                    standalone_coll_obj.name,
                    dataverse_name=standalone_coll_obj.dataverse_name,
                    database_name=standalone_coll_obj.database_name,
                    primary_key=standalone_coll_obj.primary_key)
                if not result:
                    self.fail("Failed to create standalone collection")
                cmd = drop_cmd.format(self.cbas_util.format_name(
                    standalone_coll_obj.name))
                execute_commands.append(cmd)

            return execute_commands

        remote_link = self.cbas_util.get_all_link_objs("couchbase")[0]

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
                                                                  self.columnar_cluster.instance_id,
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
                                                                  self.columnar_cluster.instance_id,
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

        collections = self.cbas_util.get_all_datasets_from_metadata(self.columnar_cluster)

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
                        sample_doc = self.cbas_util.generate_docs(1024)
                        expected_error = self.ACCESS_DENIED_ERR
                        result = self.cbas_util.insert_into_standalone_collection(
                            self.columnar_cluster,
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
                        sample_doc = self.cbas_util.generate_docs(1024)
                        expected_error = self.ACCESS_DENIED_ERR
                        result = self.cbas_util.upsert_into_standalone_collection(
                            self.columnar_cluster,
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
                        cmd = "SELECT * from " + self.cbas_util.format_name(res) + ";"
                        expected_error = self.ACCESS_DENIED_ERR
                        status, metrics, errors, results, _, warnings = (
                            self.cbas_util.execute_statement_on_cbas_util(
                                self.columnar_cluster, cmd,
                                username=test_case['user'].username,
                                password=test_case['user'].password,
                                timeout=300, analytics_timeout=300))

                        if test_case['validate_err_msg']:
                            result = self.cbas_util.validate_error_and_warning_in_response(
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
                        result = (self.cbas_util.delete_from_standalone_collection(
                            self.columnar_cluster,
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
                execute_cmd = ""
                if priv == "collection_insert":
                    sample_doc = self.cbas_util.generate_docs(1024)
                    execute_cmd = self.cbas_util.generate_insert_into_cmd(
                        [sample_doc],
                        collection_name=standalone_coll)
                elif priv == "collection_upsert":
                    sample_doc = self.cbas_util.generate_docs(1024)
                    execute_cmd = self.cbas_util.generate_upsert_into_cmd(
                        standalone_coll,
                        [sample_doc])
                elif priv == "collection_select":
                    execute_cmd = "SELECT * from " + \
                                  self.cbas_util.format_name(standalone_coll) + ";"
                elif priv == "collection_delete":
                    execute_cmd = self.cbas_util.generate_delete_from_cmd(standalone_coll)

                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.columnar_cluster.instance_id,
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
                    sample_doc = self.cbas_util.generate_docs(1024)
                    execute_cmd = self.cbas_util.generate_insert_into_cmd(
                        [sample_doc],
                        collection_name=standalone_coll)
                    resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                                  self.tenant.project_id,
                                                                  self.columnar_cluster.instance_id,
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
                    sample_doc = self.cbas_util.generate_docs(1024)
                    execute_cmd = self.cbas_util.generate_upsert_into_cmd(
                        standalone_coll,
                        [sample_doc])
                    resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                                  self.tenant.project_id,
                                                                  self.columnar_cluster.instance_id,
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
                                  self.cbas_util.format_name(standalone_coll) + ";"
                    resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                                  self.tenant.project_id,
                                                                  self.columnar_cluster.instance_id,
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
                    execute_cmd = self.cbas_util.generate_delete_from_cmd(standalone_coll)
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
                    self.cbas_util.create_remote_link_obj(
                        self.columnar_cluster,
                        hostname=self.remote_cluster.master.ip,
                        username=self.remote_cluster.master.rest_username,
                        password=self.remote_cluster.master.rest_password,
                        certificate=self.get_remote_cluster_certificate(self.remote_cluster),
                        encryption="full")
                    remote_link_obj = None
                    remote_links = self.cbas_util.get_all_link_objs("couchbase")
                    for rm_link in remote_links:
                        if rm_link.name not in remote_links_created:
                            remote_link_obj = rm_link
                            break
                    expected_error = self.ACCESS_DENIED_ERR

                    result = self.cbas_util.create_remote_link(
                        self.columnar_cluster,
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
                    self.cbas_util.create_external_link_obj(self.columnar_cluster,
                                                                      accessKeyId=self.aws_access_key,
                                                                      secretAccessKey=self.aws_secret_key,
                                                                      regions=[self.aws_region])
                    external_links = self.cbas_util.get_all_link_objs("s3")
                    s3_link_obj = None
                    for ex_link in external_links:
                        if ex_link.name not in external_links_created:
                            s3_link_obj = ex_link
                            break
                    expected_error = self.ACCESS_DENIED_ERR

                    result = self.cbas_util.create_external_link(
                        self.columnar_cluster,
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
                    self.cbas_util.create_remote_link_obj(
                        self.columnar_cluster,
                        hostname=self.remote_cluster.master.ip,
                        username=self.remote_cluster.master.rest_username,
                        password=self.remote_cluster.master.rest_password,
                        certificate=self.get_remote_cluster_certificate(self.remote_cluster),
                        encryption="full")
                    remote_link_obj = None
                    remote_links = self.cbas_util.get_all_link_objs("couchbase")
                    for rm_link in remote_links:
                        if rm_link.name not in remote_links_created:
                            remote_link_obj = rm_link
                            break
                    expected_error = self.ACCESS_DENIED_ERR

                    result = self.cbas_util.create_remote_link(self.columnar_cluster,
                                                                         remote_link_obj.properties)
                    if not result:
                        self.fail("Failed to create remote link {}".format(remote_link_obj.name))
                    remote_links_created.append(remote_link_obj.name)
                    result = self.cbas_util.drop_link(
                        self.columnar_cluster,
                        remote_link_obj.name,
                        username=test_case['user'].username,
                        password=test_case['user'].password,
                        validate_error_msg=test_case['validate_err_msg'],
                        expected_error=expected_error)
                    if not result:
                        self.fail("Test case failed to while attemptint to drop remote link {}".
                                  format(remote_link_obj.name))

                    # create and drop external link
                    self.cbas_util.create_external_link_obj(
                        self.columnar_cluster,
                        accessKeyId=self.aws_access_key,
                        secretAccessKey=self.aws_secret_key,
                        regions=[self.aws_region])
                    external_links = self.cbas_util.get_all_link_objs("s3")
                    s3_link_obj = None
                    for ex_link in external_links:
                        if ex_link.name not in external_links_created:
                            s3_link_obj = ex_link
                            break
                    expected_error = self.ACCESS_DENIED_ERR

                    result = self.cbas_util.create_external_link(
                        self.columnar_cluster,
                        s3_link_obj.properties)
                    if not result:
                        self.fail("Failed to create external link {}". \
                                  format(s3_link_obj.name))
                    external_links_created.append(s3_link_obj.name)

                    result = self.cbas_util.drop_link(
                        self.columnar_cluster,
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
        self.create_bucket_scopes_collections_in_capella_cluster(
            self.tenant, self.remote_cluster,
            self.input.param("num_buckets", 1))
        self.build_cbas_columnar_infra()

        link_objs = self.cbas_util.get_all_link_objs("couchbase")

        remote_links = []
        for rm_link in link_objs:
            result = self.cbas_util.disconnect_link(self.columnar_cluster,
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
                        is_link_active = self.cbas_util.is_link_active(
                            self.columnar_cluster,
                            link_name,
                            "couchbase")
                        if is_link_active:
                            result = self.cbas_util.disconnect_link(self.columnar_cluster,
                                                                              res)
                            if not result:
                                self.fail("Failed to disconnect link {}".format(res))

                        result = self.cbas_util.connect_link(
                            self.columnar_cluster,
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
                        is_link_active = self.cbas_util.is_link_active(
                            self.columnar_cluster,
                            link_name,
                            "couchbase")
                        if not is_link_active:
                            result = self.cbas_util.connect_link(self.columnar_cluster,
                                                                           res)
                            if not result:
                                self.fail("Failed to connect link {}".format(res))

                        result = self.cbas_util.disconnect_link(
                            self.columnar_cluster,
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
                execute_cmd = ""
                if priv == "link_connect":
                    link_name = rm_link.split(".")[0]
                    expected_error = self.ACCESS_DENIED_ERR
                    is_link_active = self.cbas_util.is_link_active(
                        self.columnar_cluster,
                        link_name,
                        "couchbase")
                    if is_link_active:
                        result = self.cbas_util.disconnect_link(self.columnar_cluster,
                                                                          rm_link)
                        if not result:
                            self.fail("Failed to disconnect link {}".format(rm_link))

                    execute_cmd = connect_link_cmd.format(
                        self.cbas_util.format_name(rm_link))
                elif priv == "link_disconnect":
                    link_name = rm_link.split(".")[0]
                    expected_error = self.ACCESS_DENIED_ERR
                    is_link_active = self.cbas_util.is_link_active(
                        self.columnar_cluster,
                        link_name,
                        "couchbase")
                    if not is_link_active:
                        result = self.cbas_util.connect_link(self.columnar_cluster,
                                                                       rm_link)
                        if not result:
                            self.fail("Failed to connect link {}".format(rm_link))
                    execute_cmd = disconnect_link_cmd.format(
                        self.cbas_util.format_name(rm_link))

                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.columnar_cluster.instance_id,
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
                execute_cmd = ""
                if priv == "link_connect":
                    link_name = rm_link.split(".")[0]
                    expected_error = self.ACCESS_DENIED_ERR
                    is_link_active = self.cbas_util.is_link_active(
                        self.columnar_cluster,
                        link_name,
                        "couchbase")
                    if is_link_active:
                        result = self.cbas_util.disconnect_link(self.columnar_cluster,
                                                                          rm_link)
                        if not result:
                            self.fail("Failed to disconnect link {}".format(rm_link))

                    execute_cmd = connect_link_cmd.format(
                        self.cbas_util.format_name(rm_link))
                elif priv == "link_disconnect":
                    link_name = rm_link.split(".")[0]
                    expected_error = self.ACCESS_DENIED_ERR
                    is_link_active = self.cbas_util.is_link_active(
                        self.columnar_cluster,
                        link_name,
                        "couchbase")
                    if not is_link_active:
                        result = self.cbas_util.connect_link(self.columnar_cluster,
                                                                       rm_link)
                        if not result:
                            self.fail("Failed to connect link {}".format(rm_link))
                    execute_cmd = disconnect_link_cmd.format(
                        self.cbas_util.format_name(rm_link))

                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.columnar_cluster.instance_id,
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
        self.create_bucket_scopes_collections_in_capella_cluster(
            self.tenant, self.remote_cluster,
            self.input.param("num_buckets", 1))
        self.build_cbas_columnar_infra()
        external_links = self.cbas_util.get_all_link_objs("s3")
        s3_links = []
        for ex_link in external_links:
            s3_links.append(ex_link.name)
        remote_links = self.cbas_util.get_all_link_objs("couchbase")
        rm_links = []
        for rm_link in remote_links:
            rm_links.append(rm_link.name)

        self.create_s3_sink_bucket()

        dataset = self.cbas_util.get_all_dataset_objs("standalone")[0]
        standalone_coll_obj = self.cbas_util.create_standalone_dataset_obj(
            self.columnar_cluster,
            primary_key={"id": "string"},
            database_name="Default",
            dataverse_name="Default")[0]
        result = self.cbas_util.create_standalone_collection(
            self.columnar_cluster,
            standalone_coll_obj.name,
            dataverse_name=standalone_coll_obj.dataverse_name,
            database_name=standalone_coll_obj.database_name,
            primary_key=standalone_coll_obj.primary_key)
        if not result:
            self.fail("Failed to create copy from s3 datastaset")
        standalone_datasets = self.cbas_util.get_all_dataset_objs("standalone")
        for ds in standalone_datasets:
            if ds.name != dataset.name:
                dataset_copy_from = ds
                break
        no_of_docs = self.input.param("no_of_docs", 100)
        jobs = Queue()
        results = []
        self.log.info("Adding {} documents in standalone dataset. Default doc size is 1KB".format(no_of_docs))
        jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                  {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                   "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name
                      , "no_of_docs": no_of_docs}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, 1, async_run=False)

        if not all(results):
            self.log.error("Some documents were not inserted")

        extended_res_priv_map = []
        res_priv_obj = {
            "name": "",
            "type": "instance",
            "privileges": ["collection_select", "collection_insert", "collection_upsert"]
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
                        collection = "{}.{}.{}".format(self.remote_cluster.buckets[0].name,
                                                       "_default","_default")
                        # copy to s3
                        if res in s3_links:
                            result = self.cbas_util.copy_to_s3(
                                self.columnar_cluster,
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

                        # copy to kv
                        if res in rm_links:
                            result = self.cbas_util.copy_to_kv(
                                self.columnar_cluster,
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
                            result = self.cbas_util.copy_from_external_resource_into_standalone_collection(
                                self.columnar_cluster,
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
        resources = s3_links + rm_links
        for user in self.test_users:
            self.log.info("========== CLOUD USER TEST CASE: {} ===========".
                          format(self.test_users[user]["role"]))
            self.columnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                               self.test_users[user]["mailid"],
                                               self.test_users[user]["password"])
            for priv in self.link_dml_privileges:
                if priv == "link_copy_to":
                    execute_commands = []
                    for res in resources:
                        collection = "{}.{}.{}".format(self.remote_cluster.buckets[0].name,
                                                       "_default","_default")
                        expected_error = self.ACCESS_DENIED_ERR
                        path = "copy_dataset_" + str(path_idx)
                        path_idx += 1
                        # copy to s3
                        if res in s3_links:
                            execute_cmd = self.cbas_util.generate_copy_to_s3_cmd(
                                dataset.name,
                                dataset.dataverse_name,
                                dataset.database_name,
                                destination_bucket=self.sink_s3_bucket_name,
                                destination_link_name=res,
                                path=path
                            )
                            execute_commands.append(execute_cmd)

                        if res in rm_links:
                            execute_cmd = self.cbas_util.generate_copy_to_kv_cmd(
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
                            execute_cmd = self.cbas_util.generate_copy_from_cmd(
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
                                                                  self.columnar_cluster.instance_id,
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
            for priv in self.link_dml_privileges:
                if priv == "link_copy_to":
                    execute_commands = []
                    for res in resources:
                        collection = "{}.{}.{}".format(self.remote_cluster.buckets[0].name,
                                                       "_default", "_default")
                        expected_error = self.ACCESS_DENIED_ERR
                        path = "copy_dataset_" + str(path_idx)
                        path_idx += 1
                        # copy to s3
                        if res in s3_links:
                            execute_cmd = self.cbas_util.generate_copy_to_s3_cmd(
                                dataset.name,
                                dataset.dataverse_name,
                                dataset.database_name,
                                destination_bucket=self.sink_s3_bucket_name,
                                destination_link_name=res,
                                path=path
                            )
                            execute_commands.append(execute_cmd)
                        # copy to kv
                        if res in rm_links:
                            execute_cmd = self.cbas_util.generate_copy_to_kv_cmd(
                                dataset.name,
                                dataset.database_name,
                                dataset.dataverse_name,
                                dest_bucket=collection,
                                link_name=res)
                            execute_commands.append(execute_cmd)

                elif priv == "link_copy_from":
                    execute_commands = []
                    for res in resources:
                        execute_cmd = self.cbas_util.generate_copy_from_cmd(
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
                                                                  self.columnar_cluster.instance_id,
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
        scopes = self.cbas_util.get_all_dataverses_from_metadata(self.columnar_cluster)
        collection = self.cbas_util.get_all_dataset_objs("standalone")[0]

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
                        synonym_name = generate_random_entity_name(type="synonym")
                        synonym_full_name = "{}.{}".format(res, synonym_name)
                        synonym_full_name = self.cbas_util.format_name(synonym_full_name)
                        expected_error = self.ACCESS_DENIED_ERR
                        result = self.cbas_util.create_analytics_synonym(
                            self.columnar_cluster,
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
                        synonym_name = generate_random_entity_name(type="synonym")
                        synonym_full_name = "{}.{}".format(res, synonym_name)
                        synonym_full_name = self.cbas_util.format_name(synonym_full_name)
                        expected_error = self.ACCESS_DENIED_ERR
                        result = self.cbas_util.create_analytics_synonym(self.columnar_cluster,
                                                                                   synonym_full_name,
                                                                                   collection.full_name)
                        if not result:
                            self.fail("Failed to create synonym {}".format(synonym_full_name))

                        result = self.cbas_util.drop_analytics_synonym(
                            self.columnar_cluster,
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
                    synonym_name = generate_random_entity_name(type="synonym")
                    synonym_full_name = "{}.{}".format(res, synonym_name)
                    synonym_full_name = self.cbas_util.format_name(synonym_full_name)
                    execute_cmd = self.cbas_util.generate_create_analytics_synonym_cmd(
                        synonym_full_name, collection.full_name)
                elif priv == "synonym_drop":
                    synonym_name = generate_random_entity_name(type="synonym")
                    synonym_full_name = "{}.{}".format(res, synonym_name)
                    synonym_full_name = self.cbas_util.format_name(synonym_full_name)
                    result = self.cbas_util.create_analytics_synonym(self.columnar_cluster,
                                                                               synonym_full_name,
                                                                               collection.full_name)
                    if not result:
                        self.fail("Failed to create synonym {}".format(synonym_full_name))
                    execute_cmd = self.cbas_util.generate_drop_analytics_synonym_cmd(
                        synonym_full_name)
                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.columnar_cluster.instance_id,
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
                    synonym_name = generate_random_entity_name(type="synonym")
                    synonym_full_name = "{}.{}".format(res, synonym_name)
                    synonym_full_name = self.cbas_util.format_name(synonym_full_name)
                    execute_cmd = self.cbas_util.generate_create_analytics_synonym_cmd(
                        synonym_full_name, collection.full_name)
                elif priv == "synonym_drop":
                    synonym_name = generate_random_entity_name(type="synonym")
                    synonym_full_name = "{}.{}".format(res, synonym_name)
                    synonym_full_name = self.cbas_util.format_name(synonym_full_name)
                    result = self.cbas_util.create_analytics_synonym(self.columnar_cluster,
                                                                               synonym_full_name,
                                                                               collection.full_name)
                    if not result:
                        self.fail("Failed to create synonym {}".format(synonym_full_name))
                    execute_cmd = self.cbas_util.generate_drop_analytics_synonym_cmd(
                        synonym_full_name)
                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.columnar_cluster.instance_id,
                                                              execute_cmd)
                if role == "projectOwner" or role == "projectDataWriter":
                    self.assertEqual(200, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 200, role))
                else:
                    self.assertEqual(403, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 403, role))

    def test_rbac_views(self):
        self.log.info("RBAC views test started")
        self.build_cbas_columnar_infra()

        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        no_of_docs = self.input.param("no_of_docs", 1000)
        jobs = Queue()
        results = []
        for dataset in datasets:
            self.log.info("Adding {} documents in standalone dataset {}. Default doc size is 1KB".
                          format(no_of_docs, dataset.full_name))
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name
                          , "no_of_docs": no_of_docs}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Some documents were not inserted")

        scopes = self.cbas_util.get_all_dataverses_from_metadata(self.columnar_cluster)
        view_collection = datasets[0]

        view_defn = "SELECT name, email from {0}".format(
            self.cbas_util.format_name(view_collection.full_name))

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
                        view_name = generate_random_entity_name(type="view")
                        view_full_name = "{}.{}".format(res, view_name)
                        view_full_name = self.cbas_util.format_name(view_full_name)
                        expected_error = self.ACCESS_DENIED_ERR
                        result = self.cbas_util.create_analytics_view(
                            self.columnar_cluster,
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
                        view_name = generate_random_entity_name(type="view")
                        view_full_name = "{}.{}".format(res, view_name)
                        view_full_name = self.cbas_util.format_name(view_full_name)
                        expected_error = self.ACCESS_DENIED_ERR

                        result = self.cbas_util.create_analytics_view(
                            self.columnar_cluster,
                            view_full_name,
                            view_defn)
                        if not result:
                            self.log.error("Failed to create view")
                            self.fail("Failed to create analytics view {}".format(view_full_name))

                        result = self.cbas_util.drop_analytics_view(
                            self.columnar_cluster,
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
                    view_name = generate_random_entity_name(type="view")
                    view_full_name = "{}.{}".format("Default", view_name)
                    view_full_name = self.cbas_util.format_name(view_full_name)
                    execute_cmd = generate_create_view_cmd(view_full_name, view_defn)
                elif priv == "view_drop":
                    view_name = generate_random_entity_name(type="view")
                    view_full_name = "{}.{}".format("Default", view_name)
                    view_full_name = self.cbas_util.format_name(view_full_name)
                    result = self.cbas_util.create_analytics_view(
                        self.columnar_cluster,
                        view_full_name,
                        view_defn)
                    if not result:
                        self.log.error("Failed to create view")
                        self.fail("Failed to create analytics view {}".format(view_full_name))
                    execute_cmd = "drop analytics view {0}".format(view_full_name)

                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.columnar_cluster.instance_id,
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
                    view_name = generate_random_entity_name(type="view")
                    view_full_name = "{}.{}".format("Default", view_name)
                    view_full_name = self.cbas_util.format_name(view_full_name)
                    execute_cmd = generate_create_view_cmd(view_full_name, view_defn)
                elif priv == "view_drop":
                    view_name = generate_random_entity_name(type="view")
                    view_full_name = "{}.{}".format("Default", view_name)
                    view_full_name = self.cbas_util.format_name(view_full_name)
                    result = self.cbas_util.create_analytics_view(
                        self.columnar_cluster,
                        view_full_name,
                        view_defn)
                    if not result:
                        self.log.error("Failed to create view")
                        self.fail("Failed to create analytics view {}".format(view_full_name))
                    execute_cmd = "drop analytics view {0}".format(view_full_name)

                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.columnar_cluster.instance_id,
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

        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        no_of_docs = self.input.param("no_of_docs", 1000)
        jobs = Queue()
        results = []
        for dataset in datasets:
            self.log.info("Adding {} documents in standalone dataset {}. Default doc size is 1KB".
                          format(no_of_docs, dataset.full_name))
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name
                          , "no_of_docs": no_of_docs}))

        self.cbas_util.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Some documents were not inserted")

        scopes = self.cbas_util.get_all_dataverses_from_metadata(self.columnar_cluster)
        view_collection = datasets[0]

        num_views = self.input.param("num_of_views", 1)
        view_defn = "SELECT name, email from {0}".format(
            self.cbas_util.format_name(view_collection.full_name))

        for i in range(num_views):
            view_name = generate_random_entity_name(type="view")
            view_name = self.cbas_util.format_name(view_name)
            result = self.cbas_util.create_analytics_view(
                self.columnar_cluster,
                view_name,
                view_defn)
            if not result:
                self.fail("Failed to create analytics view {}".format(view_name))

        views = self.cbas_util.get_all_views_from_metadata(self.columnar_cluster)

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
                    format(self.cbas_util.format_name(res))
                expected_error = self.ACCESS_DENIED_ERR
                status, metrics, errors, results, _, warnings = (
                    self.cbas_util.execute_statement_on_cbas_util(
                        self.columnar_cluster, select_cmd,
                        username=test_case['user'].username,
                        password=test_case['user'].password,
                        timeout=300, analytics_timeout=300))

                if test_case['validate_err_msg']:
                    result = self.cbas_util.validate_error_and_warning_in_response(
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
            format(self.cbas_util.format_name(views[0]))
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
                                                              self.columnar_cluster.instance_id,
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
                                                              self.columnar_cluster.instance_id,
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
        datasets = self.cbas_util.get_all_dataset_objs("standalone")

        no_of_docs = self.input.param("no_of_docs", 1000)
        jobs = Queue()
        results = []
        self.log.info("Adding {} documents in standalone dataset. Default doc size is 1KB". \
                      format(no_of_docs))
        for dataset in datasets:
            jobs.put((self.cbas_util.load_doc_to_standalone_collection,
                      {"cluster": self.columnar_cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name
                          , "no_of_docs": no_of_docs}))
        self.cbas_util.run_jobs_in_parallel(
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
                        index_name = generate_random_entity_name(type="index")
                        index_name = self.cbas_util.format_name(index_name)
                        expected_error = self.ACCESS_DENIED_ERR
                        result = self.cbas_util.create_cbas_index(
                            self.columnar_cluster,
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
                        index_name = generate_random_entity_name(type="index")
                        index_name = self.cbas_util.format_name(index_name)
                        expected_error = self.ACCESS_DENIED_ERR
                        result = self.cbas_util.create_cbas_index(self.columnar_cluster,
                                                                            index_name,
                                                                            index_fields,
                                                                            res)
                        if not result:
                            self.fail("Failed to create index {} on coll {}".
                                      format(index_name, res))
                        result = self.cbas_util.drop_cbas_index(
                            self.columnar_cluster,
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
                    index_name = generate_random_entity_name(type="index")
                    index_name = self.cbas_util.format_name(index_name)
                    execute_cmd = self.cbas_util.generate_create_index_cmd(
                        index_name, index_fields, coll)
                elif priv == "index_drop":
                    index_name = generate_random_entity_name(type="index")
                    index_name = self.cbas_util.format_name(index_name)
                    result = self.cbas_util.create_cbas_index(self.columnar_cluster,
                                                                        index_name,
                                                                        index_fields,
                                                                        coll)
                    if not result:
                        self.fail("Failed to create index {} on coll {}".
                                  format(index_name, coll))
                    execute_cmd = self.cbas_util.generate_drop_index_cmd(
                        index_name, coll)

                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.columnar_cluster.instance_id,
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
                    index_name = generate_random_entity_name(type="index")
                    index_name = self.cbas_util.format_name(index_name)
                    execute_cmd = self.cbas_util.generate_create_index_cmd(
                        index_name, index_fields, coll)
                elif priv == "index_drop":
                    index_name = generate_random_entity_name(type="index")
                    index_name = self.cbas_util.format_name(index_name)
                    result = self.cbas_util.create_cbas_index(self.columnar_cluster,
                                                                        index_name,
                                                                        index_fields,
                                                                        coll)
                    if not result:
                        self.fail("Failed to create index {} on coll {}".
                                  format(index_name, coll))
                    execute_cmd = self.cbas_util.generate_drop_index_cmd(
                        index_name, coll)
                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.columnar_cluster.instance_id,
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

        scopes = self.cbas_util.get_all_dataverses_from_metadata(self.columnar_cluster)

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
                        udf_name = generate_random_entity_name(type="udf")
                        udf_name = self.cbas_util.format_name(udf_name)
                        expected_error = self.ACCESS_DENIED_ERR
                        result = self.cbas_util.create_udf(
                            self.columnar_cluster,
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
                        udf_name = generate_random_entity_name(type="udf")
                        udf_name = self.cbas_util.format_name(udf_name)
                        expected_error = self.ACCESS_DENIED_ERR
                        result = self.cbas_util.create_udf(
                            self.columnar_cluster,
                            udf_name,
                            res,
                            None,
                            body=function_body)
                        if not result:
                            self.fail("Failed to create udf function {} in {}".format(udf_name,
                                                                                      res))
                        result = self.cbas_util.drop_udf(
                            self.columnar_cluster,
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
                    udf_name = generate_random_entity_name(type="udf")
                    udf_name = self.cbas_util.format_name(udf_name)
                    execute_cmd = self.cbas_util.generate_create_udf_cmd(
                        udf_name, "Default.Default", body=function_body)
                elif priv == "function_drop":
                    udf_name = generate_random_entity_name(type="udf")
                    udf_name = self.cbas_util.format_name(udf_name)
                    result = self.cbas_util.create_udf(
                        self.columnar_cluster,
                        udf_name,
                        "Default.Default",
                        None,
                        body=function_body)
                    if not result:
                        self.fail("Failed to create udf function {} in {}".format(udf_name,
                                                                                  res))
                    execute_cmd = self.cbas_util.generate_drop_udf_cmd(
                        udf_name, "Default.Default")

                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.columnar_cluster.instance_id,
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
                    udf_name = generate_random_entity_name(type="udf")
                    udf_name = self.cbas_util.format_name(udf_name)
                    execute_cmd = self.cbas_util.generate_create_udf_cmd(
                        udf_name, "Default.Default", body=function_body)
                elif priv == "function_drop":
                    udf_name = generate_random_entity_name(type="udf")
                    udf_name = self.cbas_util.format_name(udf_name)
                    result = self.cbas_util.create_udf(
                        self.columnar_cluster,
                        udf_name,
                        "Default.Default",
                        None,
                        body=function_body)
                    if not result:
                        self.fail("Failed to create udf function {} in {}".format(udf_name,
                                                                                  res))
                    execute_cmd = self.cbas_util.generate_drop_udf_cmd(
                        udf_name, "Default.Default")

                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.columnar_cluster.instance_id,
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
            udf_name = generate_random_entity_name(type="udf")
            udf_name = self.cbas_util.format_name(udf_name)
            function_body = "select 1"
            result = self.cbas_util.create_udf(
                self.columnar_cluster,
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
                res = self.cbas_util.format_name(res)
                execute_cmd = "{}()".format(res)
                full_resource_name = self.cbas_util.unformat_name(res). \
                                         replace(".", ":") + ":0"
                expected_error = self.ACCESS_DENIED_ERR
                status, metrics, errors, results, _, _ = (
                    self.cbas_util.execute_statement_on_cbas_util(
                        self.columnar_cluster, execute_cmd,
                        username=test_case['user'].username,
                        password=test_case['user'].password,
                        timeout=300, analytics_timeout=300))

                if test_case['validate_err_msg']:
                    result = self.cbas_util.validate_error_and_warning_in_response(
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
        func_name = self.cbas_util.format_name(func_name)
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
                                                              self.columnar_cluster.instance_id,
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
                                                              self.columnar_cluster.instance_id,
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

        self.cbas_util.cleanup_cbas(self.columnar_cluster)

        self.delete_different_organization_roles()

        for role in self.columnar_cluster.columnar_roles:
            result = self.columnar_rbac_util.delete_columnar_role(self.pod, self.tenant,
                                                                  self.tenant.project_id,
                                                                  self.columnar_cluster,
                                                                  role.id)
            if not result:
                self.log.error("Failed to delete columnar role {}".format(role.id))

        for db_user in self.columnar_cluster.db_users:
            result = self.columnar_rbac_util.delete_api_keys(self.pod, self.tenant,
                                                             self.tenant.project_id,
                                                             self.columnar_cluster,
                                                             db_user.id)
            if not result:
                self.log.error("Failed to delete user {}".format(db_user.id))

        if hasattr(self, "remote_cluster") and self.remote_cluster:
            self.delete_all_buckets_from_capella_cluster(
                self.tenant, self.remote_cluster)
        #super(ColumnarBaseTest, self).tearDown()
