import json
import random
import string
from queue import Queue

from Columnar.columnar_base import ColumnarBaseTest
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI
from capella_utils.columnar_final import (ColumnarUtils, ColumnarRBACUtil)
from cbas_utils.cbas_utils_columnar import CbasUtil as columnarCBASUtil
from capellaAPI.capella.columnar.CapellaAPI import CapellaAPI as ColumnarAPI
from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI as CapellaAPIv2


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
        self.capellaAPIv4 = CapellaAPI(self.pod.url_public, '', '', self.tenant.user, self.tenant.pwd, '')
        self.capellaAPIv2 = CapellaAPIv2(self.pod.url_public, self.tenant.api_secret_key,
                                         self.tenant.api_access_key, self.tenant.user,
                                         self.tenant.pwd)
        self.columnar_utils = ColumnarUtils(self.log)
        self.columnar_rbac_util = ColumnarRBACUtil(self.log)
        self.columnar_cbas_utils = columnarCBASUtil(
            self.task, self.use_sdk_for_cbas)
        self.project_roles = ["projectOwner", "projectClusterViewer", "projectClusterManager",
                              "projectDataWriter", "projectDataViewer"]

        self.ACCESS_DENIED_ERR = "User must have permission (cluster.analytics.grant.{}[{}]!{})"
        self.database_privileges = ["database_create", "database_drop"]
        self.scope_privileges = ["scope_create", "scope_drop"]
        self.collection_ddl_privileges = ["collection_create", "collection_drop"]
        self.collection_dml_privileges = ["collection_insert", "collection_upsert",
                                          "collection_delete", "collection_analyze"]
        self.link_ddl_privileges = ["link_create", "link_create", "link_alter"]
        self.link_connection_privileges = ["link_connect", "link_disconnect"]
        self.link_dml_privileges = ["link_copy_to", "link_copy_from"]
        self.synonym_privileges = ["synonym_create", "synonym_drop"]
        self.index_privileges = ["index_create", "index_drop"]
        self.udf_ddl_privileges = ["function_create", "function_drop"]
        self.udf_execute_privileges = ["function_execute"]

        # if none provided use 1 Kb doc size
        self.doc_size = self.input.param("doc_size", 1000)

        if not self.columnar_spec_name:
            self.columnar_spec_name = "regressions.copy_to_s3"

        self.columnar_spec = self.cbas_util.get_columnar_spec(
            self.columnar_spec_name)

        self.aws_region = "us-west-1"
        self.s3_source_bucket = "columnar-sanity-test-data-mohsin"

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

        resp = self.capellaAPIv4.load_sample_bucket(self.tenant.id, self.tenant.project_id,
                                                    self.remote_cluster.id, "travel-sample")
        if resp.status_code != 201:
            self.fail("Failed to load sample bucket")
        self.log.info("Loaded travel-sample bucket")
        self.remote_bucket_name = "travel-sample"
        self.remote_scope_name = "inventory"
        self.remote_coll_name = "airline"

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

    def generate_random_password(self, length=12):
        """Generate a random password."""
        password_characters = string.ascii_letters + string.digits
        password = ''.join(random.choice(password_characters) for i in range(length))
        password += "!123"
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

        testcases = []
        if len(resources) == 0:
            resources.append("")

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

        privileges_payload = self.columnar_rbac_util.create_privileges_payload(extended_res_priv_map)

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        user3 = self.columnar_rbac_util.create_api_keys(self.pod, self.tenant,
                                                        self.tenant.project_id, self.cluster,
                                                        username=username,
                                                        password=password,
                                                        privileges_payload=privileges_payload)
        negative_test_case = self.generate_test_case(user3, privileges, [resources[0]],
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
        negative_test_case = self.generate_test_case(user4, privileges, [resources[0]],
                                                     validate_err_msg=True)
        testcases.append(negative_test_case)

        return testcases

    def create_external_dataset(self, dataset_obj, validate_error_msg=False,
                                expected_error=None, username=None, password=None):
        if self.columnar_cbas_utils.create_dataset_on_external_resource(
                self.analytics_cluster, dataset_obj.name,
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
                    expected_error = self.ACCESS_DENIED_ERR.format("DATABASE", database_name, "CREATE")
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
                    expected_error = self.ACCESS_DENIED_ERR.format("DATABASE", database_name, "DROP")
                    result = self.columnar_cbas_utils.create_database(self.analytics_cluster,
                                                                      database_name)
                    if not result:
                        self.fail("Failed to create database")
                    result = self.columnar_cbas_utils.drop_database(
                        self.analytics_cluster,
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
                                     msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 200))
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
                if role == "projectOwner" or role == "projectClusterManager" or \
                        role == "projectClusterViewer":  # Bug
                    self.assertEqual(200, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 200, role))
                else:
                    self.assertEqual(403, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 403, role))

    def test_rbac_scope(self):
        self.log.info("RBAC test for scope started")
        self.build_cbas_columnar_infra()

        databases = self.columnar_cbas_utils.get_all_databases_from_metadata(self.cluster)

        testcases = self.create_rbac_testcases(self.scope_privileges, databases,
                                               "database")

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
                    full_resource_name = self.columnar_cbas_utils.unformat_name(res) + ":" + \
                                         scope_name
                    if priv == "scope_create":
                        expected_error = self.ACCESS_DENIED_ERR.format("SCOPE", full_resource_name,
                                                                       "CREATE")
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
                        expected_error = self.ACCESS_DENIED_ERR.format("SCOPE", full_resource_name,
                                                                       "DROP")
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
                    scope_name = self.generate_random_entity_name(type="scope")
                    scope_name = self.columnar_cbas_utils.format_name(scope_name)
                    execute_cmd = create_scope_cmd.format(scope_name)
                elif priv == "scope_drop":
                    scope_name = self.generate_random_entity_name(type="scope")
                    scope_name = self.columnar_cbas_utils.format_name(scope_name)
                    result = self.columnar_cbas_utils.create_dataverse(self.cluster,
                                                                       scope_name,
                                                                       res)
                    if not result:
                        self.fail("Failed to create scope {}".format(scope_name))
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
                    scope_name = self.generate_random_entity_name(type="scope")
                    scope_name = self.columnar_cbas_utils.format_name(scope_name)
                    execute_cmd = create_scope_cmd.format(scope_name)
                elif priv == "scope_drop":
                    scope_name = self.generate_random_entity_name(type="scope")
                    scope_name = self.columnar_cbas_utils.format_name(scope_name)
                    result = self.columnar_cbas_utils.create_dataverse(self.cluster,
                                                                       scope_name,
                                                                       res)
                    if not result:
                        self.fail("Failed to create scope {}".format(scope_name))
                    execute_cmd = drop_scope_cmd.format(scope_name)
                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.cluster.instance_id,
                                                              execute_cmd)
                if role == "projectOwner" or role == "projectClusterManager" or \
                        role == "projectClusterViewer":  # Bug
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
            "privileges": ["create_collection"]
        }
        testcases = self.create_rbac_testcases(self.collection_ddl_privileges, scopes,
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
                for res in resources:
                    remote_links = self.columnar_cbas_utils.get_all_link_objs("couchbase")
                    remote_link = None
                    for rm_link in remote_links:
                        if res in rm_link.full_name:
                            remote_link = rm_link
                            break

                    s3_links = self.columnar_cbas_utils.get_all_link_objs("s3")
                    external_link = None
                    for ex_link in s3_links:
                        if res in ex_link.full_name:
                            external_link = ex_link
                            break

                    if priv == "collection_create":

                        # Commented because of https://issues.couchbase.com/browse/MB-61596

                        # create remote collection.
                        self.log.info("Test creating remote collection")
                        remote_coll_obj = self.columnar_cbas_utils.create_remote_dataset_obj(
                            self.cluster,
                            self.remote_bucket_name,
                            self.remote_scope_name,
                            self.remote_coll_name,
                            remote_link,
                            same_db_same_dv_for_link_and_dataset=True)[0]

                        full_resource_name = self.columnar_cbas_utils.unformat_name(res). \
                                                 replace(".", ":") + ":" + remote_coll_obj.name
                        expected_error = self.ACCESS_DENIED_ERR.format("COLLECTION",
                                                                       full_resource_name, priv)

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
                            same_db_same_dv_for_link_and_dataset=True,
                            paths_on_external_container=None)[0]
                        full_resource_name = self.columnar_cbas_utils.unformat_name(res). \
                                                 replace(".", ":") + ":" + external_coll_obj.name
                        expected_error = self.ACCESS_DENIED_ERR.format("COLLECTION",
                                                                       full_resource_name, priv)
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
                        database_name = res.split(".")[0]
                        scope_name = res.split(".")[1]
                        standalone_coll_obj = self.columnar_cbas_utils.create_standalone_dataset_obj(
                            self.cluster,
                            primary_key={"name": "string", "email": "string"},
                            database_name=database_name,
                            dataverse_name=scope_name)[0]
                        full_resource_name = self.columnar_cbas_utils.unformat_name(res). \
                                                 replace(".", ":") + ":" + standalone_coll_obj.name
                        expected_error = self.ACCESS_DENIED_ERR.format("COLLECTION",
                                                                       full_resource_name, priv)

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

                    elif priv == "collection_drop":
                        # create and drop standalone collection
                        self.log.info("Test dropping remote collection")
                        remote_coll_obj = self.columnar_cbas_utils.create_remote_dataset_obj(
                            self.cluster,
                            self.remote_bucket_name,
                            self.remote_scope_name,
                            self.remote_coll_name,
                            remote_link,
                            same_db_same_dv_for_link_and_dataset=True)[0]

                        full_resource_name = self.columnar_cbas_utils.unformat_name(res). \
                                                 replace(".", ":") + ":" + remote_coll_obj.name
                        expected_error = self.ACCESS_DENIED_ERR.format("COLLECTION",
                                                                       full_resource_name, priv)

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
                            self.cluster, remote_coll_obj.name,
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
                            same_db_same_dv_for_link_and_dataset=True,
                            paths_on_external_container=None)[0]
                        full_resource_name = self.columnar_cbas_utils.unformat_name(res). \
                                                 replace(".", ":") + ":" + external_coll_obj.name
                        expected_error = self.ACCESS_DENIED_ERR.format("COLLECTION",
                                                                       full_resource_name, priv)
                        result = self.create_external_dataset(external_coll_obj)
                        if not result:
                            self.fail("Failed to create external collection {}".
                                      format(external_coll_obj.name))

                        result = self.columnar_cbas_utils.drop_dataset(
                            self.cluster, external_coll_obj.name,
                            username=test_case['user'].username,
                            password=test_case['user'].password,
                            validate_error_msg=test_case['validate_err_msg'],
                            expected_error=expected_error)
                        if not result:
                            self.fail("Test case failed while attempting to drop external collection {}".
                                      format(external_coll_obj.name))

                        # create and drop external collection
                        self.log.info("Test dropping external collection")
                        external_coll_obj = self.columnar_cbas_utils.create_external_dataset_obj(
                            self.cluster,
                            external_container_names={
                                self.s3_source_bucket: self.aws_region},
                            file_format="json",
                            same_db_same_dv_for_link_and_dataset=True,
                            paths_on_external_container=None)[0]
                        full_resource_name = self.columnar_cbas_utils.unformat_name(res). \
                                                 replace(".", ":") + ":" + external_coll_obj.name
                        expected_error = self.ACCESS_DENIED_ERR.format("COLLECTION",
                                                                       full_resource_name, priv)
                        result = self.create_external_dataset(external_coll_obj)
                        if not result:
                            self.fail("Failed to create external collection {}".
                                      format(external_coll_obj.name))

                        result = self.columnar_cbas_utils.drop_dataset(
                            self.cluster, external_coll_obj.name,
                            username=test_case['user'].username,
                            password=test_case['user'].password,
                            validate_error_msg=test_case['validate_err_msg'],
                            expected_error=expected_error)
                        if not result:
                            self.fail("Test case failed while attempting to drop external collection {}".
                                      format(external_coll_obj.name))

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
                    full_resource_name = self.columnar_cbas_utils.unformat_name(res). \
                        replace(".", ":")

                    if priv == "collection_insert":
                        sample_doc = self.columnar_cbas_utils.generate_docs(1024)
                        expected_error = self.ACCESS_DENIED_ERR.format("COLLECTION", full_resource_name, "INSERT")
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
                        expected_error = self.ACCESS_DENIED_ERR.format("COLLECTION",
                                                                       full_resource_name, "UPSERT")
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
                        expected_error = self.ACCESS_DENIED_ERR.format("COLLECTION",
                                                                       full_resource_name, "SELECT")
                        status, metrics, errors, results, _ = (
                            self.columnar_cbas_utils.execute_statement_on_cbas_util(
                                self.cluster, cmd,
                                username=test_case['user'].id,
                                password=test_case['user'].password,
                                timeout=300, analytics_timeout=300))

                        if test_case['validate_err_msg']:
                            result = self.columnar_cbas_utils.validate_error_in_response(
                                status, errors, expected_error, None)
                            if not result:
                                self.fail("Test case failed while attempting to get docs from standalone coll {}".
                                          format(res))
                        else:
                            if status != "success":
                                self.fail("Test case failed while attempting to get docs from standalone coll {}".
                                          format(res))
                    elif priv == "collection_delete":
                        expected_error = self.ACCESS_DENIED_ERR.format("COLLECTION",
                                                                       full_resource_name, "DELETE")
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
                    if role == "projectOwner" or role == "projectDataWriter" or \
                            role == "projectClusterViewer":  # Bug
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
                    if role == "projectOwner" or role == "projectDataWriter" or \
                            role == "projectClusterViewer":  # Bug
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
                            role == "projectDataViewer":  # Bug
                        self.assertEqual(200, resp.status_code,
                                         msg='FAIL, Outcome:{}, Expected: {}.' \
                                             'For role: {}'.format(resp.status_code, 200, role))
                    else:
                        self.assertEqual(403, resp.status_code,
                                         msg='FAIL, Outcome:{}, Expected: {}.' \
                                             'For role: {}'.format(resp.status_code, 403, role))
                elif priv == "collection_delete":
                    execute_cmd = self.columnar_cbas_utils.generate_delete_from_cmd(standalone_coll)
                    if role == "projectOwner" or role == "projectDataWriter" or \
                            role == "projectDataViewer":  # Bug
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
                    # Commented due to https://issues.couchbase.com/browse/MB-61610

                    # create remote link
                    self.columnar_cbas_utils.create_remote_link_obj(
                        self.cluster,
                        hostname=self.remote_cluster.master.ip,
                        username=self.remote_cluster.master.rest_username,
                        password=self.remote_cluster.master.rest_password,
                        certificate=self.get_remote_cluster_certificate(self.remote_cluster),
                        encryption="full")
                    remote_links = self.columnar_cbas_utils.get_all_link_objs("couchbase")
                    remote_link_obj = remote_links[0]
                    expected_error = self.ACCESS_DENIED_ERR.format("LINK", remote_link_obj.full_name, priv)

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

                    # create external link
                    self.columnar_cbas_utils.create_external_link_obj(self.cluster,
                                                                      accessKeyId=self.aws_access_key,
                                                                      secretAccessKey=self.aws_secret_key,
                                                                      regions=[self.aws_region])
                    external_links = self.columnar_cbas_utils.get_all_link_objs("s3")
                    s3_link_obj = external_links[-1]
                    expected_error = self.ACCESS_DENIED_ERR.format("LINK", s3_link_obj.full_name, priv)

                    result = self.columnar_cbas_utils.create_external_link(
                        self.cluster,
                        s3_link_obj.properties,
                        username=test_case['user'].username,
                        password=test_case['user'].password,
                        validate_error_msg=test_case['validate_err_msg'],
                        expected_error=expected_error)
                    if not result:
                        self.fail("Test case failed while attempting to create external link {}".
                                  format(s3_link_obj.full_name))
                elif priv == "link_drop":
                    # create and drop remote link
                    self.columnar_cbas_utils.create_remote_link_obj(
                        self.cluster,
                        hostname=self.remote_cluster.master.ip,
                        username=self.remote_cluster.master.rest_username,
                        password=self.remote_cluster.master.rest_password,
                        certificate=self.get_remote_cluster_certificate(self.remote_cluster),
                        encryption="full")
                    remote_links = self.columnar_cbas_utils.get_all_link_objs("couchbase")
                    remote_link_obj = remote_links[-1]
                    expected_error = self.ACCESS_DENIED_ERR.format("LINK", "", priv)

                    result = self.columnar_cbas_utils.create_remote_link(self.cluster,
                                                                         remote_link_obj.properties)
                    if not result:
                        self.fail("Failed to create remote link {}".format(remote_link_obj.full_name))
                    result = self.columnar_cbas_utils.drop_link(
                        self.cluster,
                        remote_link_obj.full_name,
                        username=test_case['user'].username,
                        password=test_case['user'].password,
                        validate_error_msg=test_case['validate_err_msg'],
                        expected_error=expected_error)
                    if not result:
                        self.fail("Test case failed to while attemptint to drop remote link {}".
                                  format(remote_link_obj.full_name))

                    # create and drop external link
                    self.columnar_cbas_utils.create_external_link_obj(
                        self.cluster,
                        accessKeyId=self.aws_access_key,
                        secretAccessKey=self.aws_secret_key,
                        regions=[self.aws_region])
                    external_links = self.columnar_cbas_utils.get_all_link_objs("s3")
                    s3_link_obj = external_links[-1]
                    expected_error = self.ACCESS_DENIED_ERR.format("LINK", "", priv)

                    result = self.columnar_cbas_utils.create_external_link(
                        self.cluster,
                        s3_link_obj.properties)
                    if not result:
                        self.fail("Failed to create external link {}". \
                                  format(s3_link_obj.full_name))

                    result = self.columnar_cbas_utils.drop_link(
                        self.cluster,
                        s3_link_obj.full_name,
                        username=test_case['user'].username,
                        password=test_case['user'].password,
                        validate_error_msg=test_case['validate_err_msg'],
                        expected_error=expected_error)
                    if not result:
                        self.fail("Test case failed while attempting to drop external link {}".
                                  format(s3_link_obj.full_name))

    def test_rbac_link_connection(self):
        self.log.info("RBAC link connection test started")
        self.build_cbas_columnar_infra()

        link_objs = self.columnar_cbas_utils.get_all_link_objs("couchbase")

        remote_links = []
        for rm_link in link_objs:
            result = self.columnar_cbas_utils.disconnect_link(self.cluster,
                                                              rm_link.full_name)
            remote_links.append(rm_link.full_name)
            if not result:
                self.fail("Failed to disconnect link {}".format(rm_link.full_name))

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
                        db_name = res.split(".")[0]
                        scope_name = res.split(".")[1]
                        link_name = res.split(".")[2]
                        expected_error = self.ACCESS_DENIED_ERR.format("LINK", link_name, priv)
                        is_link_active = self.columnar_cbas_utils.is_link_active(
                            self.cluster,
                            link_name,
                            scope_name,
                            db_name,
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
                        db_name = res.split(".")[0]
                        scope_name = res.split(".")[1]
                        link_name = res.split(".")[2]
                        expected_error = self.ACCESS_DENIED_ERR.format("LINK", link_name, priv)
                        is_link_active = self.columnar_cbas_utils.is_link_active(
                            self.cluster,
                            link_name,
                            scope_name,
                            db_name,
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
                    db_name = rm_link.split(".")[0]
                    scope_name = rm_link.split(".")[1]
                    link_name = rm_link.split(".")[2]
                    expected_error = self.ACCESS_DENIED_ERR.format("LINK", link_name, priv)
                    is_link_active = self.columnar_cbas_utils.is_link_active(
                        self.cluster,
                        link_name,
                        scope_name,
                        db_name,
                        "couchbase")
                    if is_link_active:
                        result = self.columnar_cbas_utils.disconnect_link(self.cluster,
                                                                          rm_link)
                        if not result:
                            self.fail("Failed to disconnect link {}".format(rm_link))

                    execute_cmd = connect_link_cmd.format(
                        self.columnar_cbas_utils.format_name(rm_link))
                elif priv == "link_disconnect":
                    db_name = rm_link.split(".")[0]
                    scope_name = rm_link.split(".")[1]
                    link_name = rm_link.split(".")[2]
                    expected_error = self.ACCESS_DENIED_ERR.format("LINK", link_name, priv)
                    is_link_active = self.columnar_cbas_utils.is_link_active(
                        self.cluster,
                        link_name,
                        scope_name,
                        db_name,
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
                    db_name = rm_link.split(".")[0]
                    scope_name = rm_link.split(".")[1]
                    link_name = rm_link.split(".")[2]
                    expected_error = self.ACCESS_DENIED_ERR.format("LINK", link_name, priv)
                    is_link_active = self.columnar_cbas_utils.is_link_active(
                        self.cluster,
                        link_name,
                        scope_name,
                        db_name,
                        "couchbase")
                    if is_link_active:
                        result = self.columnar_cbas_utils.disconnect_link(self.cluster,
                                                                          rm_link)
                        if not result:
                            self.fail("Failed to disconnect link {}".format(rm_link))

                    execute_cmd = connect_link_cmd.format(
                        self.columnar_cbas_utils.format_name(rm_link))
                elif priv == "link_disconnect":
                    db_name = rm_link.split(".")[0]
                    scope_name = rm_link.split(".")[1]
                    link_name = rm_link.split(".")[2]
                    expected_error = self.ACCESS_DENIED_ERR.format("LINK", link_name, priv)
                    is_link_active = self.columnar_cbas_utils.is_link_active(
                        self.cluster,
                        link_name,
                        scope_name,
                        db_name,
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
                if role == "projectOwner" or role == "projectClusterManager" or \
                        role == "projectClusterViewer":  # Bug
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
        testcases = self.create_rbac_testcases(self.link_dml_privileges, s3_links,
                                               resource_type="link",
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
                if priv == "link_copy_to":
                    self.log.info("Testing COPY TO")
                    for res in resources:
                        link_name = res
                        expected_error = self.ACCESS_DENIED_ERR.format("LINK", link_name, "COPY_TO")
                        path = "copy_dataset_" + str(idx)
                        # copy to s3
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
                elif priv == "link_copy_from":
                    self.log.info("Testing COPY FROM")
                    for res in resources:
                        link_name = res
                        expected_error = self.ACCESS_DENIED_ERR.format("LINK", link_name,
                                                                       "COPY_FROM")
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
                    for res in resources:
                        link_name = res
                        expected_error = self.ACCESS_DENIED_ERR.format("LINK", link_name, "COPY_TO")
                        path = "copy_dataset_" + str(idx)
                        # copy to s3
                        execute_cmd = self.columnar_cbas_utils.generate_copy_to_s3_cmd(
                            dataset.name,
                            dataset.dataverse_name,
                            dataset.database_name,
                            destination_bucket=self.sink_s3_bucket_name,
                            destination_link_name=res,
                            path=path
                        )
                elif priv == "link_copy_from":
                    for res in resources:
                        link_name = res
                        execute_cmd = self.columnar_cbas_utils.generate_copy_from_cmd(
                            dataset_copy_from.name,
                            self.s3_source_bucket,
                            res,
                            dataset_copy_from.dataverse_name,
                            dataset_copy_from.database_name,
                            "*.json"
                        )
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
                    for res in resources:
                        link_name = res
                        expected_error = self.ACCESS_DENIED_ERR.format("LINK", link_name, "COPY_TO")
                        path = "copy_dataset_" + str(idx)
                        # copy to s3
                        execute_cmd = self.columnar_cbas_utils.generate_copy_to_s3_cmd(
                            dataset.name,
                            dataset.dataverse_name,
                            dataset.database_name,
                            destination_bucket=self.sink_s3_bucket_name,
                            destination_link_name=res,
                            path=path
                        )
                elif priv == "link_copy_from":
                    for res in resources:
                        db_name = res.split(".")[0]
                        scope_name = res.split(".")[1]
                        link_name = res.split(".")[2]
                        execute_cmd = self.columnar_cbas_utils.generate_copy_from_cmd(
                            dataset_copy_from.name,
                            self.s3_source_bucket,
                            res,
                            dataset_copy_from.dataverse_name,
                            dataset_copy_from.database_name,
                            "*.json"
                        )

                resp = self.columnarAPIrole.execute_statement(self.tenant.id,
                                                              self.tenant.project_id,
                                                              self.cluster.instance_id,
                                                              execute_cmd)
                if role == "projectOwner" or role == "projectClusterManager" or \
                        role == "projectClusterViewer":  # Bug
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
                        full_resource_name = self.columnar_cbas_utils.unformat_name(res).replace(".", ":")
                        expected_error = self.ACCESS_DENIED_ERR.format("SYNONYM", full_resource_name,
                                                                       "CREATE")
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
                        full_resource_name = self.columnar_cbas_utils.unformat_name(res). \
                            replace(".", ":")
                        expected_error = self.ACCESS_DENIED_ERR.format("SYNONYM", full_resource_name,
                                                                       "DROP")
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
                if role == "projectOwner" or role == "projectClusterManager" or \
                        role == "projectClusterViewer":  # Bug
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
                        full_resource_name = self.columnar_cbas_utils.unformat_name(res).replace(".", ":")
                        expected_error = self.ACCESS_DENIED_ERR.format("INDEX", full_resource_name, priv)
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
                        full_resource_name = self.columnar_cbas_utils.unformat_name(res). \
                            replace(".", ":")
                        expected_error = self.ACCESS_DENIED_ERR.format("INDEX", full_resource_name, priv)
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
                if role == "projectOwner" or role == "projectClusterManager" or \
                        role == "projectClusterViewer":  # Bug
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
                        full_resource_name = self.columnar_cbas_utils.unformat_name(res). \
                            replace(".", ":")
                        udf_name = self.columnar_cbas_utils.format_name(udf_name)
                        expected_error = self.ACCESS_DENIED_ERR.format("FUNCTION",
                                                                       full_resource_name,
                                                                       "CREATE")
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
                        full_resource_name = self.columnar_cbas_utils.unformat_name(res). \
                            replace(".", ":")
                        udf_name = self.columnar_cbas_utils.format_name(udf_name)
                        expected_error = self.ACCESS_DENIED_ERR.format("FUNCTION",
                                                                       full_resource_name,
                                                                       "DROP")
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
                if role == "projectOwner" or role == "projectClusterManager" or \
                        role == "projectClusterViewer":  # Bug
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
                execute_cmd = "{}()".format(res)
                full_resource_name = self.columnar_cbas_utils.unformat_name(res). \
                                         replace(".", ":") + ":0"
                expected_error = self.ACCESS_DENIED_ERR.format("FUNCTION", full_resource_name,
                                                               "EXECUTE")
                status, metrics, errors, results, _ = (
                    self.columnar_cbas_utils.execute_statement_on_cbas_util(
                        self.cluster, execute_cmd,
                        username=test_case['user'].username,
                        password=test_case['user'].password,
                        timeout=300, analytics_timeout=300))

                if test_case['validate_err_msg']:
                    result = self.columnar_cbas_utils.validate_error_in_response(
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
                if role == "projectOwner" or role == "projectClusterManager" or \
                        role == "projectClusterViewer":  # Bug
                    self.assertEqual(200, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 200, role))
                else:
                    self.assertEqual(403, resp.status_code,
                                     msg='FAIL, Outcome:{}, Expected: {}.' \
                                         'For role: {}'.format(resp.status_code, 403, role))

    def tearDown(self):
        if self.sink_s3_bucket_name:
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
