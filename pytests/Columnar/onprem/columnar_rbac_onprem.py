import math
import json
import os.path
import random
import string
import time
from queue import Queue

from cbas.cbas_base import CBASBaseTest
from couchbase_utils.security_utils.x509main import x509main
from CbasLib.CBASOperations import CBASHelper
from cbas_utils.cbas_utils_columnar import CbasUtil as columnarCBASUtil
from cbas_utils.cbas_utils_columnar import RBAC_Util as ColumnarRBACUtil
from awsLib.s3_data_helper import perform_S3_operation


class ColumnarRBAC(CBASBaseTest):

    def setUp(self):
        super(ColumnarRBAC, self).setUp()
        self.use_sdk_for_cbas = self.input.param("use_sdk_for_cbas", False)
        self.columnar_cbas_utils = columnarCBASUtil(
            self.task, self.use_sdk_for_cbas)
        self.columnar_rbac_util = ColumnarRBACUtil(
            self.task, self.use_sdk_for_cbas)
        self.sdk_clients_per_user = self.input.param("sdk_clients_per_user", 1)

        self.columnar_spec_name = self.input.param(
            "columnar_spec_name", "regressions.copy_to_s3")
        self.columnar_spec = self.columnar_cbas_utils.get_columnar_spec(
            self.columnar_spec_name)

        for cluster_name, cluster in self.cb_clusters.items():
            if hasattr(cluster, "cbas_cc_node"):
                self.analytics_cluster = cluster
            else:
                self.remote_cluster = cluster

        self.ACCESS_DENIED_ERR = "User must have permission (cluster.analytics.grant.{}[{}]!{})"
        self.database_privileges = ["CREATE", "DROP"]
        self.scope_privileges = ["CREATE", "DROP"]
        self.collection_ddl_privileges = ["CREATE", "DROP"]
        self.collection_dml_privileges = ["SELECT", "INSERT", "UPSERT",
                                          "DELETE", "ANALYZE"]
        self.link_ddl_privileges = ["CREATE", "DROP", "ALTER"]
        self.link_connection_privileges = ["CONNECT", "DISCONNECT"]
        self.link_dml_privileges = ["COPY TO", "COPY FROM"]
        self.role_privieleges = ["CREATE", "DROP"]
        self.synonym_privileges = ["CREATE", "DROP"]
        self.index_privileges = ["CREATE", "DROP"]
        self.udf_ddl_privileges = ["CREATE", "DROP"]
        self.udf_execute_privileges = ["EXECUTE"]
        self.view_ddl_privileges = ["CREATE", "DROP"]
        self.view_select_privileges = ["SELECT"]

        self.aws_region = "us-west-1"
        self.s3_source_bucket = "columnar-sanity-test-data-mohsin"
        self.sink_s3_bucket_name = None
        self.aws_access_key = self.input.param("aws_access_key")
        self.aws_secret_key = self.input.param("aws_secret_key")
        self.aws_session_token = self.input.param("aws_session_token", "")


    def generate_random_password(self, length=12):
        """Generate a random password."""
        password_characters = string.ascii_letters + string.digits
        password = ''.join(random.choice(password_characters) for i in range(length))
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
                              denied_resources=[], privilege_resource_type="database"):
        """
        1. Create a user for each privilege and assign the corresponding
           privilege to the user
        2. Create a role for each privilege and assign the corresponding
           privilege to the role. And assign the role to a user
        3. Create a negative test case to check other privileges return errors
           for both user and role.
        3. Create a user and assign it all the privileges
        4. Create a role and assing it all the privileges. Assing the role
           to a user.
        5. Create a negative test case wherer no privilege is assinged to the user
           and role.
        """
        testcases = []
        for priv in privileges:
            username = self.generate_random_entity_name(type="user")
            password = self.generate_random_password()
            result = self.columnar_rbac_util.create_user(self.analytics_cluster,
                                                         username,
                                                         username,
                                                         password)
            if not result:
                self.fail("Failed to create user1")
            user1 = self.columnar_rbac_util.get_user_obj(username)
            role_name = self.generate_random_entity_name(type="role")
            result = self.columnar_rbac_util.create_columnar_role(self.analytics_cluster,
                                                                  role_name)
            if not result:
                self.fail("Failed to create role.")
            role1 = self.columnar_rbac_util.get_columnar_role_object(role_name)

            #Grant privilege to role and user
            result = self.columnar_rbac_util.grant_privileges_to_subjects(self.analytics_cluster,
                                                                          [priv],
                                                                          [user1, role1],
                                                                          resources,
                                                                          privilege_resource_type)
            if not result:
                self.fail("Failed to assign {} privilege to {}, {}".format(
                    priv, str(user1), str(role1)))

            test_case = self.generate_test_case(user1, [priv], resources)
            testcases.append(test_case)

            if len(privileges) > 1:
                exluded_privs = [ex_priv for ex_priv in privileges if ex_priv != priv]
                exluded_privs = random.sample(exluded_privs, min(2, len(exluded_privs)))
                negative_test_case = self.generate_test_case(user1, exluded_privs,
                                                             resources, True)
                testcases.append(negative_test_case)

            #Assign role to new user
            username = self.generate_random_entity_name(type="user")
            password = self.generate_random_password()
            result = self.columnar_rbac_util.create_user(self.analytics_cluster,
                                                         username,
                                                         username,
                                                         password)
            if not result:
                self.fail("Failed to create user2")
            user2 = self.columnar_rbac_util.get_user_obj(username)
            # Grant role to user
            self.columnar_rbac_util.assign_role_to_user(self.analytics_cluster,
                                                        roles=[role1],
                                                        users=[user2])
            test_case = self.generate_test_case(user2, [priv], resources)
            testcases.append(test_case)

            if len(privileges) > 1:
                exluded_privs = [ex_priv for ex_priv in privileges if ex_priv != priv]
                exluded_privs = random.sample(exluded_privs, min(2, len(exluded_privs)))
                negative_test_case = self.generate_test_case(user2, exluded_privs,
                                                             resources, True)
                testcases.append(negative_test_case)

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        result = self.columnar_rbac_util.create_user(self.analytics_cluster,
                                                     username,
                                                     username,
                                                     password)
        if not result:
            self.fail("Failed to create user3")

        user3 = self.columnar_rbac_util.get_user_obj(username)

        role_name = self.generate_random_entity_name(type="role")
        result = self.columnar_rbac_util.create_columnar_role(self.analytics_cluster,
                                                              role_name)
        if not result:
            self.fail("Failed to create role2")
        role2 = self.columnar_rbac_util.get_columnar_role_object(role_name)

        result = self.columnar_rbac_util.grant_privileges_to_subjects(self.analytics_cluster,
                                                                      privileges,
                                                                      [user3, role2],
                                                                      resources,
                                                                      privilege_resource_type)
        if not result:
                self.fail("Failed to assign {} privileges to {}, {}".format(
                    privileges, str(user3), str(role2)))
        test_case = self.generate_test_case(user3, privileges, resources)
        testcases.append(test_case)

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        result = self.columnar_rbac_util.create_user(self.analytics_cluster,
                                                     username,
                                                     username,
                                                     password)
        if not result:
            self.fail("Failed to create user4")
        user4 = self.columnar_rbac_util.get_user_obj(username)

        result = self.columnar_rbac_util.assign_role_to_user(self.analytics_cluster,
                                                             roles=[role2],
                                                             users=[user4])
        if not result:
            self.fail("Failed to asssign role2 {} to user4 {}".
                      format(str(role2), str(user4)))
        test_case = self.generate_test_case(user4, privileges, resources)
        testcases.append(test_case)

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        result = self.columnar_rbac_util.create_user(self.analytics_cluster,
                                                     username,
                                                     username,
                                                     password)

        if not result:
            self.fail("Failed to create user5 {}".format(username))
        user5 = self.columnar_rbac_util.get_user_obj(username)

        role_name = self.generate_random_entity_name(type="role")
        result = self.columnar_rbac_util.create_columnar_role(self.analytics_cluster,
                                                                role_name)
        if not result:
            self.fail("Failed to create role3 {}".format(role_name))
        role3 = self.columnar_rbac_util.get_columnar_role_object(role_name)

        negative_test_case = self.generate_test_case(user5, privileges, resources, True)
        testcases.append(negative_test_case)

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        result = self.columnar_rbac_util.create_user(self.analytics_cluster,
                                                     username,
                                                     username,
                                                     password)
        if not result:
            self.fail("Failed to create user5 {}".format(username))
        user6 = self.columnar_rbac_util.get_user_obj(username)

        result = self.columnar_rbac_util.assign_role_to_user(self.analytics_cluster,
                                                             [role3],
                                                             [user6])
        if not result:
            self.fail("Failed to assign role3 {} to user6 {}".
                      format(str(role3), str(user6)))

        negative_test_case = self.generate_test_case(user6, privileges, resources, True)
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
        # Update Remote Links Spec here
        status, certificate, header = x509main(
            self.remote_cluster.master)._get_cluster_ca_cert()
        self.columnar_spec["remote_link"]["properties"] = [{
            "type": "couchbase",
            "hostname": self.remote_cluster.master.ip,
            "username": self.remote_cluster.master.rest_username,
            "password": self.remote_cluster.master.rest_password,
            "encryption": "none"}]

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
                "external_container_name": self.input.param("s3_source_bucket", None),
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
                            cluster=self.analytics_cluster, cbas_spec=self.columnar_spec,
                            bucket_util=self.bucket_util, wait_for_ingestion=False,
                            remote_clusters=[self.remote_cluster])

        if not result:
            self.fail("Error while creating analytics entities: {}".format(error))


    def test_rbac_database(self):
        self.log.info("RBAC database test started")

        testcases = self.create_rbac_testcases(self.database_privileges, [], [],
                                               "database")

        for idx, test_case in enumerate(testcases):
            self.log.info("========== TEST CASE {} ===========".format(idx))
            self.log.info(test_case['description'])
            if test_case['validate_err_msg']:
                self.log.info("EXPECTED: ERROR")
            else:
                self.log.info("EXPECTED: SUCCESS")

            privileges = test_case['privileges']

            for priv in privileges:
                if priv == "CREATE":
                    database_name = self.generate_random_entity_name(type="database")
                    expected_error = self.ACCESS_DENIED_ERR.format("DATABASE", database_name, priv)
                    result = self.columnar_cbas_utils.create_database(self.analytics_cluster,
                                                                      database_name,
                                                                      username=test_case['user'].id,
                                                                      password=test_case['user'].password,
                                                                      validate_error_msg=test_case['validate_err_msg'],
                                                                      expected_error=expected_error)
                    if not result:
                        self.fail("Test case failed while attempting to create database")
                elif priv == "DROP":
                    database_name = self.generate_random_entity_name(type="database")
                    expected_error = self.ACCESS_DENIED_ERR.format("DATABASE", database_name, priv)
                    result = self.columnar_cbas_utils.create_database(self.analytics_cluster,
                                                                      database_name)
                    if not result:
                        self.fail("Failed to create database")
                    result = self.columnar_cbas_utils.drop_database(self.analytics_cluster,
                                                                    database_name,
                                                                    username=test_case['user'].id,
                                                                    password=test_case['user'].password,
                                                                    validate_error_msg=test_case['validate_err_msg'],
                                                                    expected_error=expected_error)
                    if not result:
                        self.fail("Test case failed while attempting to drop database")

    def test_rbac_scope(self):
        self.log.info("RBAC scope test started")
        self.build_cbas_columnar_infra()

        databases = self.columnar_cbas_utils.get_all_databases_from_metadata(self.analytics_cluster)

        testcases = self.create_rbac_testcases(self.scope_privileges, databases,
                                               privilege_resource_type="scope")

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
                    expected_error = self.ACCESS_DENIED_ERR.format("SCOPE", full_resource_name, priv)
                    if priv == "CREATE":
                        result = self.columnar_cbas_utils.create_dataverse(self.analytics_cluster,
                                                                        scope_name,
                                                                        res,
                                                                        username=test_case['user'].id,
                                                                        password=test_case['user'].password,
                                                                        validate_error_msg=test_case['validate_err_msg'],
                                                                        expected_error=expected_error)
                        if not result:
                            self.fail("Test case failed while attempting to create scope {}".format(scope_name))
                    elif priv == "DROP":
                        result = self.columnar_cbas_utils.create_dataverse(self.analytics_cluster,
                                                                           scope_name,
                                                                           res)
                        if not result:
                            self.fail("Failed to create scope {}".format(scope_name))
                        result = self.columnar_cbas_utils.drop_dataverse(self.analytics_cluster,
                                                                         scope_name,
                                                                         res,
                                                                         username=test_case['user'].id,
                                                                         password=test_case['user'].password,
                                                                         validate_error_msg=test_case['validate_err_msg'],
                                                                         expected_error=expected_error)
                        if not result:
                            self.fail("Test case failed while attempting to drop scope {}".format(scope_name))

    def test_rbac_collection_ddl(self):
        self.log.info("RBAC collection ddl test started")
        self.build_cbas_columnar_infra()
        self.collectionSetUp(cluster=self.remote_cluster, load_data=False)

        remote_cluster_bucket = None
        remote_cluster_scope = None
        remote_cluster_coll = None
        for bucket in self.remote_cluster.buckets:
            for scope_name, scope in bucket.scopes.iteritems():
                if scope_name != "_system":
                    for collection_name, collection in (
                            scope.collections.iteritems()):
                        remote_cluster_bucket = bucket
                        remote_cluster_scope = scope
                        remote_cluster_coll = collection
                        break

        scopes = self.columnar_cbas_utils.get_all_dataverses_from_metadata(self.analytics_cluster)

        testcases = self.create_rbac_testcases(self.collection_ddl_privileges,
                                               scopes, privilege_resource_type="collection_ddl")
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

                    if priv == "CREATE":

                        # Commented because of https://issues.couchbase.com/browse/MB-61596
                        """
                        # create remote collection.
                        remote_coll_obj = self.columnar_cbas_utils.create_remote_dataset_obj(self.analytics_cluster,
                                                                                            remote_cluster_bucket,
                                                                                            remote_cluster_scope,
                                                                                            remote_cluster_coll,
                                                                                            remote_link,
                                                                                            same_db_same_dv_for_link_and_dataset=True)[0]

                        full_resource_name = self.columnar_cbas_utils.unformat_name(res).replace(".", ":") + ":" + \
                                             remote_coll_obj.name
                        expected_error = self.ACCESS_DENIED_ERR.format("COLLECTION", full_resource_name, priv)

                        result = self.columnar_cbas_utils.create_remote_dataset(self.analytics_cluster,
                                                                                remote_coll_obj.name,
                                                                                remote_coll_obj.full_kv_entity_name,
                                                                                remote_coll_obj.link_name,
                                                                                remote_coll_obj.dataverse_name,
                                                                                remote_coll_obj.database_name,
                                                                                username=test_case['user'].id,
                                                                                password=test_case['user'].password,
                                                                                validate_error_msg=test_case['validate_err_msg'],
                                                                                expected_error=expected_error)
                        if not result:
                            self.fail("Test case failed while attempting to create remote coll {}".format(remote_coll_obj.name))
                        """

                        # create external collection.
                        external_coll_obj = self.columnar_cbas_utils.create_external_dataset_obj(self.analytics_cluster,
                                                                                                 external_container_names={
                                                                                                     self.s3_source_bucket: self.aws_region},
                                                                                                 file_format="json",
                                                                                                 same_db_same_dv_for_link_and_dataset=True,
                                                                                                 paths_on_external_container=None)[0]
                        full_resource_name = self.columnar_cbas_utils.unformat_name(res).replace(".", ":") + ":" + \
                                             external_coll_obj.name
                        expected_error = self.ACCESS_DENIED_ERR.format("COLLECTION", full_resource_name, priv)
                        result = self.create_external_dataset(external_coll_obj,
                                                              validate_error_msg=test_case['validate_err_msg'],
                                                              expected_error=expected_error,
                                                              username=test_case['user'].id,
                                                              password=test_case['user'].password)
                        if not result:
                            self.fail("Test case failed while attempting to create external coll {}".format(external_coll_obj.name))

                        # create standalone collection
                        database_name = res.split(".")[0]
                        scope_name = res.split(".")[1]
                        standalone_coll_obj = self.columnar_cbas_utils.create_standalone_dataset_obj(self.analytics_cluster,
                                                                                                     primary_key={"name": "string", "email": "string"},
                                                                                                     database_name=database_name,
                                                                                                     dataverse_name=scope_name)[0]
                        full_resource_name = self.columnar_cbas_utils.unformat_name(res).replace(".", ":") + ":" + \
                                             standalone_coll_obj.name
                        expected_error = self.ACCESS_DENIED_ERR.format("COLLECTION", full_resource_name, priv)

                        result = self.columnar_cbas_utils.create_standalone_collection(self.analytics_cluster,
                                                                                       standalone_coll_obj.name,
                                                                                       dataverse_name=standalone_coll_obj.dataverse_name,
                                                                                       database_name=standalone_coll_obj.database_name,
                                                                                       primary_key=standalone_coll_obj.primary_key,
                                                                                       validate_error_msg=test_case['validate_err_msg'],
                                                                                       expected_error=expected_error,
                                                                                       username=test_case['user'].id,
                                                                                       password=test_case['user'].password)
                        if not result:
                            self.fail("Test case failed while attempting to create standalone collection {}".format(standalone_coll_obj.name))

                    elif priv == "DROP":
                        # create and drop standalone collection
                        remote_coll_obj = self.columnar_cbas_utils.create_remote_dataset_obj(self.analytics_cluster,
                                                                                            remote_cluster_bucket,
                                                                                            remote_cluster_scope,
                                                                                            remote_cluster_coll,
                                                                                            remote_link,
                                                                                            same_db_same_dv_for_link_and_dataset=True)[0]

                        full_resource_name = self.columnar_cbas_utils.unformat_name(res).replace(".", ":") + ":" + \
                                             remote_coll_obj.name
                        expected_error = self.ACCESS_DENIED_ERR.format("COLLECTION", full_resource_name, priv)

                        result = self.columnar_cbas_utils.create_remote_dataset(self.analytics_cluster,
                                                                                remote_coll_obj.name,
                                                                                remote_coll_obj.full_kv_entity_name,
                                                                                remote_coll_obj.link_name,
                                                                                remote_coll_obj.dataverse_name,
                                                                                remote_coll_obj.database_name)
                        if not result:
                            self.fail("Failed to create remote collection {}".format(remote_coll_obj.name))

                        result = self.columnar_cbas_utils.drop_dataset(self.analytics_cluster, remote_coll_obj.name,
                                                                       username=test_case['user'].id,
                                                                       password=test_case['user'].password,
                                                                       validate_error_msg=test_case['validate_err_msg'],
                                                                       expected_error=expected_error)
                        if not result:
                            self.fail("Test case failed while attempting to drop remote dataset {}"
                                      .format(remote_coll_obj.name))

                        # create and drop external collection
                        external_coll_obj = self.columnar_cbas_utils.create_external_dataset_obj(self.analytics_cluster,
                                                                                                 external_container_names={
                                                                                                     self.s3_source_bucket: self.aws_region},
                                                                                                 file_format="json",
                                                                                                 same_db_same_dv_for_link_and_dataset=True,
                                                                                                 paths_on_external_container=None)[0]
                        full_resource_name = self.columnar_cbas_utils.unformat_name(res).replace(".", ":") + ":" + \
                                             external_coll_obj.name
                        expected_error = self.ACCESS_DENIED_ERR.format("COLLECTION", full_resource_name, priv)
                        result = self.create_external_dataset(external_coll_obj)
                        if not result:
                            self.fail("Failed to create external collection {}".
                                      format(external_coll_obj.name))

                        result = self.columnar_cbas_utils.drop_dataset(self.analytics_cluster, external_coll_obj.name,
                                                                       username=test_case['user'].id,
                                                                       password=test_case['user'].password,
                                                                       validate_error_msg=test_case['validate_err_msg'],
                                                                       expected_error=expected_error)
                        if not result:
                            self.fail("Test case failed while attempting to drop external collection {}".
                                      format(external_coll_obj.name))

                        # create and drop standalone collection
                        database_name = res.split(".")[0]
                        scope_name = res.split(".")[1]
                        standalone_coll_obj = self.columnar_cbas_utils.create_standalone_dataset_obj(self.analytics_cluster,
                                                                                                     primary_key={"name": "string", "email": "string"},
                                                                                                     database_name=database_name,
                                                                                                     dataverse_name=scope_name)[0]
                        full_resource_name = self.columnar_cbas_utils.unformat_name(res).replace(".", ":") + ":" + \
                                             standalone_coll_obj.name
                        expected_error = self.ACCESS_DENIED_ERR.format("COLLECTION", full_resource_name, priv)

                        result = self.columnar_cbas_utils.create_standalone_collection(self.analytics_cluster,
                                                                                       standalone_coll_obj.name,
                                                                                       dataverse_name=standalone_coll_obj.dataverse_name,
                                                                                       database_name=standalone_coll_obj.database_name,
                                                                                       primary_key=standalone_coll_obj.primary_key)
                        if not result:
                            self.fail("Failed to create standalone collection {}".format(standalone_coll_obj.name))

                        result = self.columnar_cbas_utils.drop_dataset(self.analytics_cluster,
                                                                       standalone_coll_obj.name,
                                                                       username=test_case['user'].id,
                                                                       password=test_case['user'].password,
                                                                       validate_error_msg=test_case['validate_err_msg'],
                                                                       expected_error=expected_error)
                        if not result:
                            self.fail("Test case failed while attempting to drop standalone collection {}".
                                      format(standalone_coll_obj.name))

    def test_rbac_collection_dml(self):
        self.log.info("RBAC collection dml test started")
        self.build_cbas_columnar_infra()

        collections = self.columnar_cbas_utils.get_all_datasets_from_metadata(self.analytics_cluster)

        testcases = self.create_rbac_testcases(self.collection_dml_privileges, collections,
                                               privilege_resource_type="collection_dml")

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
                    full_resource_name = self.columnar_cbas_utils.unformat_name(res).replace(".", ":")
                    expected_error = self.ACCESS_DENIED_ERR.format("COLLECTION", full_resource_name, priv)

                    if priv == "INSERT":
                        sample_doc = self.columnar_cbas_utils.generate_docs(1024)
                        result = self.columnar_cbas_utils.insert_into_standalone_collection(self.analytics_cluster,
                                                                                            res,
                                                                                            [sample_doc],
                                                                                            validate_error_msg=test_case['validate_err_msg'],
                                                                                            expected_error=expected_error,
                                                                                            username=test_case['user'].id,
                                                                                            password=test_case['user'].password)
                        if not result:
                            self.fail("Test case failed while attempting to insert doc " \
                                      "into standalone coll {}".format(res))

                    elif priv == "UPSERT":
                        sample_doc = self.columnar_cbas_utils.generate_docs(1024)
                        result = self.columnar_cbas_utils.upsert_into_standalone_collection(self.analytics_cluster,
                                                                                            res,
                                                                                            [sample_doc],
                                                                                            validate_error_msg=test_case['validate_err_msg'],
                                                                                            expected_error=expected_error,
                                                                                            username=test_case['user'].id,
                                                                                            password=test_case['user'].password)
                        if not result:
                            self.fail("Test case failed while attempting to upsert doc "\
                                      "into standalone coll {}".format(res))

                    elif priv == "SELECT":
                        cmd = "SELECT * from " + self.columnar_cbas_utils.format_name(res) + ";"
                        status, metrics, errors, results, _ = (
                            self.columnar_cbas_utils.execute_statement_on_cbas_util(
                                self.analytics_cluster, cmd,
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

                    elif priv == "DELETE":
                        result = self.columnar_cbas_utils.delete_from_standalone_collection(self.analytics_cluster,
                                                                                            res,
                                                                                            validate_error_msg=test_case['validate_err_msg'],
                                                                                            expected_error=expected_error,
                                                                                            username=test_case['user'].id,
                                                                                            password=test_case['user'].password)
                        if not result:
                            self.fail("Test case failed while attempting to delete documents from standalone coll {}".
                                      format(res))

    def test_rbac_link_ddl(self):
        self.log.info("RBAC link ddl test started")

        testcases = self.create_rbac_testcases(self.link_ddl_privileges, [],
                                               privilege_resource_type="link_ddl")

        for idx, test_case in enumerate(testcases):
            self.log.info("========== TEST CASE {} ===========".format(idx))
            self.log.info(test_case['description'])
            if test_case['validate_err_msg']:
                self.log.info("EXPECTED: ERROR")
            else:
                self.log.info("EXPECTED: SUCCESS")

            privileges = test_case['privileges']

            for priv in privileges:
                if priv == "CREATE":
                    self.log.info("Do nothing")
                    #Commented due to https://issues.couchbase.com/browse/MB-61610
                    """
                    # create remote link
                    self.columnar_cbas_utils.create_remote_link_obj(self.analytics_cluster,
                                                                    hostname=self.remote_cluster.master.ip,
                                                                    username=self.remote_cluster.master.rest_username,
                                                                    password=self.remote_cluster.master.rest_password,
                                                                    encryption="none")
                    remote_links = self.columnar_cbas_utils.get_all_link_objs("couchbase")
                    remote_link_obj = remote_links[-1]
                    expected_error = self.ACCESS_DENIED_ERR.format("LINK", remote_link_obj.full_name, priv)

                    result = self.columnar_cbas_utils.create_remote_link(self.analytics_cluster,
                                                                         remote_link_obj.properties,
                                                                         username=test_case['user'].id,
                                                                         password=test_case['user'].password,
                                                                         validate_error_msg=test_case['validate_err_msg'],
                                                                         expected_error=expected_error)
                    if not result:
                        self.fail("Test case failed while attempting to create remote link {}".
                                  format(remote_link_obj.name))

                    # create external link
                    self.columnar_cbas_utils.create_external_link_obj(self.analytics_cluster,
                                                                      accessKeyId=self.aws_access_key,
                                                                      secretAccessKey=self.aws_secret_key,
                                                                      regions=[self.aws_region])
                    external_links = self.columnar_cbas_utils.get_all_link_objs("s3")
                    s3_link_obj = external_links[-1]
                    expected_error = self.ACCESS_DENIED_ERR.format("LINK", s3_link_obj.full_name, priv)

                    result = self.columnar_cbas_utils.create_external_link(self.analytics_cluster,
                                                                           s3_link_obj.properties,
                                                                           username=test_case['user'].id,
                                                                           password=test_case['user'].password,
                                                                           validate_error_msg=test_case['validate_err_msg'],
                                                                           expected_error=expected_error)
                    if not result:
                        self.fail("Test case failed while attempting to create external link {}".
                                  format(s3_link_obj.full_name))
                    """

                elif priv == "DROP":
                    #create and drop remote link
                    self.columnar_cbas_utils.create_remote_link_obj(self.analytics_cluster,
                                                                    hostname=self.remote_cluster.master.ip,
                                                                    username=self.remote_cluster.master.rest_username,
                                                                    password=self.remote_cluster.master.rest_password,
                                                                    encryption="none")
                    remote_links = self.columnar_cbas_utils.get_all_link_objs("couchbase")
                    remote_link_obj = remote_links[-1]
                    expected_error = self.ACCESS_DENIED_ERR.format("LINK", "", priv)

                    result = self.columnar_cbas_utils.create_remote_link(self.analytics_cluster,
                                                                         remote_link_obj.properties)
                    if not result:
                        self.fail("Failed to create remote link {}".format(remote_link_obj.full_name))
                    result = self.columnar_cbas_utils.drop_link(self.analytics_cluster,
                                                                remote_link_obj.full_name,
                                                                username=test_case['user'].id,
                                                                password=test_case['user'].password,
                                                                validate_error_msg=test_case['validate_err_msg'],
                                                                expected_error=expected_error)
                    if not result:
                        self.fail("Test case failed to while attemptint to drop remote link {}".
                                  format(remote_link_obj.full_name))

                    # create and drop external link
                    self.columnar_cbas_utils.create_external_link_obj(self.analytics_cluster,
                                                                      accessKeyId=self.aws_access_key,
                                                                      secretAccessKey=self.aws_secret_key,
                                                                      regions=[self.aws_region])
                    external_links = self.columnar_cbas_utils.get_all_link_objs("s3")
                    s3_link_obj = external_links[-1]
                    expected_error = self.ACCESS_DENIED_ERR.format("LINK", "", priv)

                    result = self.columnar_cbas_utils.create_external_link(self.analytics_cluster,
                                                                           s3_link_obj.properties)
                    if not result:
                        self.fail("Failed to create external link {}".format(s3_link_obj.full_name))

                    result = self.columnar_cbas_utils.drop_link(self.analytics_cluster,
                                                                s3_link_obj.full_name,
                                                                username=test_case['user'].id,
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
            result = self.columnar_cbas_utils.disconnect_link(self.analytics_cluster,
                                                              rm_link.full_name)
            remote_links.append(rm_link.full_name)
            if not result:
                self.fail("Failed to disconnect link {}".format(rm_link.full_name))

        testcases = self.create_rbac_testcases(self.link_connection_privileges, remote_links,
                                               privilege_resource_type="link_connection")

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

                if priv == "CONNECT":
                    for res in resources:
                        db_name = res.split(".")[0]
                        scope_name = res.split(".")[1]
                        link_name = res.split(".")[2]
                        expected_error = self.ACCESS_DENIED_ERR.format("LINK", link_name, priv)
                        is_link_active = self.columnar_cbas_utils.is_link_active(self.analytics_cluster,
                                                                                 link_name,
                                                                                 scope_name,
                                                                                 db_name,
                                                                                 "couchbase")
                        if is_link_active:
                            result = self.columnar_cbas_utils.disconnect_link(self.analytics_cluster,
                                                                              res)
                            if not result:
                                self.fail("Failed to disconnect link {}".format(res))

                        result = self.columnar_cbas_utils.connect_link(self.analytics_cluster,
                                                                       res,
                                                                       validate_error_msg=test_case['validate_err_msg'],
                                                                       expected_error=expected_error,
                                                                       username=test_case['user'].id,
                                                                       password=test_case['user'].password)
                        if not result:
                            self.fail("Test case failed while attempting to connect link {}".
                                      format(res))
                elif res == "DISCONNECT":
                    for res in resources:
                        db_name = res.split(".")[0]
                        scope_name = res.split(".")[1]
                        link_name = res.split(".")[2]
                        expected_error = self.ACCESS_DENIED_ERR.format("LINK", link_name, priv)
                        is_link_active = self.columnar_cbas_utils.is_link_active(self.analytics_cluster,
                                                                                 link_name,
                                                                                 scope_name,
                                                                                 db_name,
                                                                                 "couchbase")
                        if not is_link_active:
                            result = self.columnar_cbas_utils.connect_link(self.analytics_cluster,
                                                                           res)
                            if not result:
                                self.fail("Failed to connect link {}".format(res))

                        result = self.columnar_cbas_utils.disconnect_link(self.analytics_cluster,
                                                                          res,
                                                                          validate_error_msg=test_case['validate_err_msg'],
                                                                          expected_error=expected_error,
                                                                          username=test_case['user'].id,
                                                                          password=test_case['user'].password)
                        if not result:
                            self.fail("Test case failed while attempting to disconnect link {}".
                                      format(res))

    def test_rbac_link_dml(self):
        self.build_cbas_columnar_infra()
        external_links = self.columnar_cbas_utils.get_all_link_objs("s3")
        s3_links = []
        for ex_link in external_links:
            s3_links.append(ex_link.full_name)

        self.create_s3_sink_bucket()

        dataset = self.columnar_cbas_utils.get_all_dataset_objs("standalone")[0]
        standalone_coll_obj = self.columnar_cbas_utils.create_standalone_dataset_obj(
                                self.analytics_cluster,
                                primary_key={"id": "string"},
                                database_name="Default",
                                dataverse_name="Default")[0]
        result = self.columnar_cbas_utils.create_standalone_collection(
                        self.analytics_cluster,
                        standalone_coll_obj.name,
                        dataverse_name=standalone_coll_obj.dataverse_name,
                        database_name=standalone_coll_obj.database_name,
                        primary_key=standalone_coll_obj.primary_key)
        if not result:
            self.fail("Failed to create copy from s3 datastaset")
        dataset_copy_from = self.columnar_cbas_utils.get_all_dataset_objs("standalone")[1]
        no_of_docs = self.input.param("no_of_docs", 1000)
        jobs = Queue()
        results = []
        self.log.info("Adding {} documents in standalone dataset. Default doc size is 1KB".format(no_of_docs))
        jobs.put((self.columnar_cbas_utils.load_doc_to_standalone_collection,
                    {"cluster": self.analytics_cluster, "collection_name": dataset.name,
                    "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name
                        , "no_of_docs": no_of_docs}))

        self.columnar_cbas_utils.run_jobs_in_parallel(
            jobs, results, 1, async_run=False)

        if not all(results):
            self.log.error("Some documents were not inserted")

        testcases = self.create_rbac_testcases(self.link_dml_privileges, s3_links,
                                               privilege_resource_type="link_dml")

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
                if priv == "COPY TO":
                    result = self.columnar_rbac_util.grant_privileges_to_subjects(self.analytics_cluster,
                                                                          ["SELECT"], [test_case['user']],
                                                                          [dataset.full_name],
                                                                          privilege_resource_type="collection_dml")
                    if not result:
                        self.fail("Failed to assing SELECT privilege to user {} on collection {}".
                                format(str(test_case['user']), dataset.full_name))
                    for res in resources:
                        db_name = res.split(".")[0]
                        scope_name = res.split(".")[1]
                        link_name = res.split(".")[2]
                        expected_error = self.ACCESS_DENIED_ERR.format("LINK", link_name, "COPY_TO")
                        path = "copy_dataset_" + str(idx)
                        # copy to s3
                        result = self.columnar_cbas_utils.copy_to_s3(self.analytics_cluster,
                                                                     dataset.name,
                                                                     dataset.dataverse_name,
                                                                     dataset.database_name,
                                                                     destination_bucket=self.sink_s3_bucket_name,
                                                                     destination_link_name=res,
                                                                     path=path,
                                                                     validate_error_msg=test_case['validate_err_msg'],
                                                                     expected_error=expected_error,
                                                                     username=test_case['user'].id,
                                                                     password=test_case['user'].password)
                        if not result:
                            self.fail("Test case failed while attempting to copy data to s3 from link {}".
                                      format(res))
                if priv == "COPY FROM":
                    result = self.columnar_rbac_util.grant_privileges_to_subjects(self.analytics_cluster,
                                                                          ["INSERT"], [test_case['user']],
                                                                          [dataset_copy_from.full_name],
                                                                          privilege_resource_type="collection_dml")
                    if not result:
                        self.fail("Failed to assing INSERT privilege to user {} on collection {}".
                                format(str(test_case['user']), dataset_copy_from.full_name))
                    for res in resources:
                        db_name = res.split(".")[0]
                        scope_name = res.split(".")[1]
                        link_name = res.split(".")[2]
                        expected_error = self.ACCESS_DENIED_ERR.format("LINK", link_name,
                                                                       "COPY_FROM")
                        result = self.columnar_cbas_utils.copy_from_external_resource_into_standalone_collection(
                                    self.analytics_cluster,
                                    dataset_copy_from.name,
                                    self.s3_source_bucket,
                                    res,
                                    dataset_copy_from.dataverse_name,
                                    dataset_copy_from.database_name,
                                    "*.json",
                                    validate_error_msg=test_case['validate_err_msg'],
                                    expected_error=expected_error,
                                    username=test_case['user'].id,
                                    password=test_case['user'].password)
                        if not result:
                            self.fail("Test case failed while attempting to copy data from s3 " \
                                      "on link {} from coll {}".format(res, dataset_copy_from.full_name))


        self.collectionSetUp(cluster=self.remote_cluster, load_data=False)

        remote_cluster_bucket = None
        remote_cluster_scope = None
        remote_cluster_coll = None
        for bucket in self.remote_cluster.buckets:
            for scope_name, scope in bucket.scopes.iteritems():
                if scope_name != "_system":
                    for collection_name, collection in (
                            scope.collections.iteritems()):
                        remote_cluster_bucket = bucket.name
                        remote_cluster_scope = scope_name
                        remote_cluster_coll = collection_name
                        break

        couchbase_links = self.columnar_cbas_utils.get_all_link_objs("couchbase")
        remote_links = []
        for rm_link in couchbase_links:
            remote_links.append(rm_link.full_name)

        testcases = self.create_rbac_testcases(self.link_dml_privileges, remote_links,
                                               privilege_resource_type="link_dml")

        for idx, test_case in enumerate(testcases):
            self.log.info("========== TEST CASE {} ===========".format(idx))
            self.log.info(test_case['description'])
            if test_case['validate_err_msg']:
                self.log.info("EXPECTED: ERROR")
            else:
                self.log.info("EXPECTED: SUCCESS")

            privileges = test_case['privileges']
            resources = test_case['resources']

            result = self.columnar_rbac_util.grant_privileges_to_subjects(self.analytics_cluster,
                                                                          ["SELECT"], [test_case['user']],
                                                                          [dataset.full_name],
                                                                          privilege_resource_type="collection_dml")
            if not result:
                self.fail("Failed to assing SELECT privilege to user {} on collection {}".
                          format(str(test_case['user']), dataset.full_name))

            for priv in privileges:
                if priv == "COPY TO":
                    for res in resources:
                        db_name = res.split(".")[0]
                        scope_name = res.split(".")[1]
                        link_name = res.split(".")[2]
                        expected_error = self.ACCESS_DENIED_ERR.format("LINK", link_name, "COPY_TO")
                        # copy to kv
                        dest_bucket = "{}.{}.{}".format(remote_cluster_bucket, remote_cluster_scope,
                                                        remote_cluster_coll)
                        dest_bucket = self.columnar_cbas_utils.format_name(dest_bucket)
                        result = self.columnar_cbas_utils.copy_to_kv(self.analytics_cluster,
                                                                     dataset.name,
                                                                     dataset.database_name,
                                                                     dataset.dataverse_name,
                                                                     dest_bucket=dest_bucket,
                                                                     link_name=res,
                                                                     validate_error_msg=test_case['validate_err_msg'],
                                                                     expected_error=expected_error,
                                                                     username=test_case['user'].id,
                                                                     password=test_case['user'].password)
                        if not result:
                            self.fail("Test case failed while attempting to copy data to kv from link {}".
                                      format(res))


    def test_rbac_role(self):
        self.log.info("RBAC role test started")

        testcases = self.create_rbac_testcases(self.role_privieleges, [],
                                               privilege_resource_type="role")

        for idx, test_case in enumerate(testcases):
            self.log.info("========== TEST CASE {} ===========".format(idx))
            self.log.info(test_case['description'])
            if test_case['validate_err_msg']:
                self.log.info("EXPECTED: ERROR")
            else:
                self.log.info("EXPECTED: SUCCESS")

            privileges = test_case['privileges']

            for priv in privileges:
                expected_error = self.ACCESS_DENIED_ERR.format("ROLE", "", priv)

                if priv == "CREATE":
                    role_name = self.generate_random_entity_name(type="role")
                    result = self.columnar_rbac_util.create_columnar_role(self.analytics_cluster,
                                                                        role_name,
                                                                        validate_error_msg=test_case['validate_err_msg'],
                                                                        expected_error=expected_error,
                                                                        username=test_case['user'].id,
                                                                        password=test_case['user'].password)
                    if not result:
                        self.fail("Test case failed whiel attempting to create role {}".format(role_name))

                elif priv == "DROP":
                    role_name = self.generate_random_entity_name(type="role")
                    result = self.columnar_rbac_util.create_columnar_role(self.analytics_cluster,
                                                                          role_name)
                    if not result:
                        self.fail("Failed to create columnar role {}".format(role_name))
                    result = self.columnar_rbac_util.drop_columnar_role(self.analytics_cluster,
                                                                        role_name,
                                                                        validate_error_msg=test_case['validate_err_msg'],
                                                                        expected_error=expected_error,
                                                                        username=test_case['user'].id,
                                                                        password=test_case['user'].password)
                    if not result:
                        self.fail("Test case failed while attempting to drop role {}".format(role_name))

    def test_rbac_synonym(self):
        self.log.info("RBAC synonym test started")
        self.build_cbas_columnar_infra()
        scopes = self.columnar_cbas_utils.get_all_dataverses_from_metadata(self.analytics_cluster)
        collection = self.columnar_cbas_utils.get_all_dataset_objs("standalone")[0]

        testcases = self.create_rbac_testcases(self.synonym_privileges, scopes,
                                               privilege_resource_type="synonym")

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
                if priv == "CREATE":
                    for res in resources:
                        synonym_name = self.generate_random_entity_name(type="synonym")
                        synonym_full_name = "{}.{}".format(res, synonym_name)
                        synonym_full_name = self.columnar_cbas_utils.format_name(synonym_full_name)
                        full_resource_name = self.columnar_cbas_utils.unformat_name(res).replace(".", ":")
                        expected_error = self.ACCESS_DENIED_ERR.format("SYNONYM", full_resource_name,
                                                                       priv)
                        result = self.columnar_cbas_utils.create_analytics_synonym(self.analytics_cluster,
                                                                                   synonym_full_name,
                                                                                   collection.full_name,
                                                                                   validate_error_msg=test_case['validate_err_msg'],
                                                                                   expected_error=expected_error,
                                                                                   username=test_case['user'].id,
                                                                                   password=test_case['user'].password)
                        if not result:
                            self.fail("Test case failed while attempting to create synonym {}".
                                    format(synonym_full_name))

                elif priv == "DROP":
                    for res in resources:
                        synonym_name = self.generate_random_entity_name(type="synonym")
                        synonym_full_name = "{}.{}".format(res, synonym_name)
                        synonym_full_name = self.columnar_cbas_utils.format_name(synonym_full_name)
                        full_resource_name = self.columnar_cbas_utils.unformat_name(res).replace(".", ":")
                        expected_error = self.ACCESS_DENIED_ERR.format("SYNONYM", full_resource_name,
                                                                        priv)
                        result = self.columnar_cbas_utils.create_analytics_synonym(self.analytics_cluster,
                                                                                   synonym_full_name,
                                                                                   collection.full_name)
                        if not result:
                            self.fail("Failed to create synonym {}".format(synonym_full_name))

                        result = self.columnar_cbas_utils.drop_analytics_synonym(self.analytics_cluster,
                                                                                 synonym_full_name,
                                                                                 validate_error_msg=test_case['validate_err_msg'],
                                                                                 expected_error=expected_error,
                                                                                 username=test_case['user'].id,
                                                                                 password=test_case['user'].password)
                        if not result:
                            self.fail("Test case failed while attempting to create synonym {}".
                                      format(synonym_full_name))

    def test_rbac_index(self):
        self.log.info("RBAC index test started")
        self.build_cbas_columnar_infra()
        datasets = self.columnar_cbas_utils.get_all_dataset_objs("standalone")

        no_of_docs = self.input.param("no_of_docs", 1000)
        jobs = Queue()
        results = []
        self.log.info("Adding {} documents in standalone dataset. Default doc size is 1KB".format(no_of_docs))
        for dataset in datasets:
            jobs.put((self.columnar_cbas_utils.load_doc_to_standalone_collection,
                      {"cluster": self.analytics_cluster, "collection_name": dataset.name,
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
                                               privilege_resource_type="index")

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

                    if priv == "CREATE":
                        index_name = self.generate_random_entity_name(type="index")
                        index_name = self.columnar_cbas_utils.format_name(index_name)
                        full_resource_name = self.columnar_cbas_utils.unformat_name(res).replace(".", ":")
                        expected_error = self.ACCESS_DENIED_ERR.format("INDEX", full_resource_name, priv)
                        result = self.columnar_cbas_utils.create_cbas_index(self.analytics_cluster,
                                                                            index_name,
                                                                            index_fields,
                                                                            res,
                                                                            validate_error_msg=test_case['validate_err_msg'],
                                                                            expected_error=expected_error,
                                                                            username=test_case['user'].id,
                                                                            password=test_case['user'].password)
                        if not result:
                            self.fail("Test case failed while attempting to create index {} on coll {}".
                                      format(index_name, res))

                    elif priv == "DROP":
                        index_name = self.generate_random_entity_name(type="index")
                        index_name = self.columnar_cbas_utils.format_name(index_name)
                        full_resource_name = self.columnar_cbas_utils.unformat_name(res).replace(".", ":")
                        expected_error = self.ACCESS_DENIED_ERR.format("INDEX", full_resource_name, priv)
                        result = self.columnar_cbas_utils.create_cbas_index(self.analytics_cluster,
                                                                            index_name,
                                                                            index_fields,
                                                                            res)
                        if not result:
                            self.fail("Failed to create index {} on coll {}".
                                      format(index_name, res))
                        result = self.columnar_cbas_utils.drop_cbas_index(self.analytics_cluster,
                                                                          index_name,
                                                                          res,
                                                                          validate_error_msg=test_case['validate_err_msg'],
                                                                          expected_error=expected_error,
                                                                          username=test_case['user'].id,
                                                                          password=test_case['user'].password)
                        if not result:
                            self.fail("Test case failed while attempting to drop index {} on coll {}".
                                      format(index_name, res))

    def test_rbac_udfs(self):
        self.log.info("RBAC test for UDFs")
        self.build_cbas_columnar_infra()

        datasets = self.columnar_cbas_utils.get_all_dataset_objs("standalone")

        no_of_docs = self.input.param("no_of_docs", 1000)
        jobs = Queue()
        results = []
        self.log.info("Adding {} documents in standalone dataset. Default doc size is 1KB".format(no_of_docs))
        for dataset in datasets:
            jobs.put((self.columnar_cbas_utils.load_doc_to_standalone_collection,
                      {"cluster": self.analytics_cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name
                          , "no_of_docs": no_of_docs}))

        self.columnar_cbas_utils.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Some documents were not inserted")

        scopes = self.columnar_cbas_utils.get_all_dataverses_from_metadata(self.analytics_cluster)

        testcases = self.create_rbac_testcases(self.udf_ddl_privileges, scopes,
                                               privilege_resource_type="udf_ddl")
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
                    if priv == "CREATE":
                        udf_name = self.generate_random_entity_name(type="udf")
                        full_resource_name = self.columnar_cbas_utils.unformat_name(res). \
                                             replace(".", ":")
                        udf_name = self.columnar_cbas_utils.format_name(udf_name)
                        expected_error = self.ACCESS_DENIED_ERR.format("FUNCTION",
                                                                       full_resource_name,
                                                                       priv)
                        result = self.columnar_cbas_utils.create_udf(
                                    self.analytics_cluster,
                                    udf_name,
                                    res,
                                    None,
                                    body=function_body,
                                    validate_error_msg=test_case['validate_err_msg'],
                                    expected_error=expected_error,
                                    username=test_case['user'].id,
                                    password=test_case['user'].password)
                        if not result:
                            self.fail("Test case failed while attempting to create function "\
                                      " {} in {}".format(udf_name, res))
                    elif priv == "DROP":
                        udf_name = self.generate_random_entity_name(type="udf")
                        full_resource_name = self.columnar_cbas_utils.unformat_name(res). \
                                             replace(".", ":")
                        udf_name = self.columnar_cbas_utils.format_name(udf_name)
                        expected_error = self.ACCESS_DENIED_ERR.format("FUNCTION",
                                                                       full_resource_name,
                                                                       priv)
                        result = self.columnar_cbas_utils.create_udf(
                                    self.analytics_cluster,
                                    udf_name,
                                    res,
                                    None,
                                    body=function_body)
                        if not result:
                            self.fail("Failed to create udf function {} in {}".format(udf_name,
                                                                                      res))
                        result = self.columnar_cbas_utils.drop_udf(
                                    self.analytics_cluster,
                                    udf_name,
                                    res,
                                    None,
                                    validate_error_msg=test_case['validate_err_msg'],
                                    expected_error=expected_error,
                                    username=test_case['user'].id,
                                    password=test_case['user'].password)
                        if not result:
                            self.fail("Test case failed while attempting to drop function "
                                      " {} in {} ".format(udf_name, res))

    def test_rbac_udf_execute(self):
        self.log.info("RBAC udf execute test started")

        analytics_udfs = []
        num_of_udfs = self.input.param('num_of_udfs', 2)
        for i in range(num_of_udfs):
            udf_name = self.generate_random_entity_name(type="udf")
            udf_name = self.columnar_cbas_utils.format_name(udf_name)
            function_body = "select 1"
            result = self.columnar_cbas_utils.create_udf(
                                    self.analytics_cluster,
                                    udf_name,
                                    "Default.Default",
                                    None,
                                    body=function_body)
            if not result:
                self.fail("Failed to create UDF {}".format(udf_name))
            udf_name_full_name = "Default.Default" + udf_name
            analytics_udfs.append(udf_name_full_name)

        testcases = self.create_rbac_testcases(self.udf_execute_privileges,
                                               analytics_udfs,
                                               privilege_resource_type="udf_execute")

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
                expected_error = self.ACCESS_DENIED_ERR.format("FUNCTION", res,
                                                               "EXECUTE")
                status, metrics, errors, results, _ = (
                            self.columnar_cbas_utils.execute_statement_on_cbas_util(
                                self.analytics_cluster, execute_cmd,
                                username=test_case['user'].id,
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

    def test_rbac_views(self):
        self.log.info("RBAC views ddl test started")
        self.build_cbas_columnar_infra()

        datasets = self.columnar_cbas_utils.get_all_dataset_objs("standalone")

        no_of_docs = self.input.param("no_of_docs", 1000)
        jobs = Queue()
        results = []
        for dataset in datasets:
            self.log.info("Adding {} documents in standalone dataset {}. Default doc size is 1KB".
                          format(no_of_docs, dataset.full_name))
            jobs.put((self.columnar_cbas_utils.load_doc_to_standalone_collection,
                      {"cluster": self.analytics_cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name
                          , "no_of_docs": no_of_docs}))

        self.columnar_cbas_utils.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Some documents were not inserted")

        scopes = self.columnar_cbas_utils.get_all_dataverses_from_metadata(self.analytics_cluster)
        view_collection = datasets[0]

        view_defn = "SELECT name, email from {0};".format(
            self.columnar_cbas_utils.format_name(view_collection.full_name))

        testcases = self.create_rbac_testcases(self.view_ddl_privileges,
                                               scopes,
                                               privilege_resource_type="view_ddl")

        for idx, test_case in enumerate(testcases):
            self.log.info("========== TEST CASE {} ===========".format(idx))
            self.log.info(test_case['description'])
            if test_case['validate_err_msg']:
                self.log.info("EXPECTED: ERROR")
            else:
                self.log.info("EXPECTED: SUCCESS")

            privileges = test_case['privileges']
            resources = test_case['resources']

            result = self.columnar_rbac_util.grant_privileges_to_subjects(self.analytics_cluster,
                                                                          ["SELECT"], [test_case['user']],
                                                                          [view_collection.full_name],
                                                                          privilege_resource_type="collection_dml")
            if not result:
                self.fail("Failed to assing SELECT privilege to user {} on collection {}".
                        format(str(test_case['user']), view_collection.full_name))

            for priv in privileges:
                for res in resources:
                    if priv == "CREATE":
                        view_name = self.generate_random_entity_name(type="view")
                        view_full_name = "{}.{}".format(res, view_name)
                        view_full_name = self.columnar_cbas_utils.format_name(view_full_name)
                        full_resource_name = self.columnar_cbas_utils.unformat_name(res). \
                                                replace(".", ":")
                        expected_error = self.ACCESS_DENIED_ERR.format("VIEW", full_resource_name,
                                                                    priv)
                        result = self.columnar_cbas_utils.create_analytics_view(
                                        self.analytics_cluster,
                                        view_full_name,
                                        view_defn,
                                        validate_error_msg=test_case['validate_err_msg'],
                                        expected_error=expected_error,
                                        username=test_case['user'].id,
                                        password=test_case['user'].password)
                        if not result:
                            self.fail("Test case failed while attempting to create view {}".
                                    format(view_full_name))

                    elif priv == "DROP":
                        view_name = self.generate_random_entity_name(type="view")
                        view_full_name = "{}.{}".format(res, view_name)
                        view_full_name = self.columnar_cbas_utils.format_name(view_full_name)
                        full_resource_name = self.columnar_cbas_utils.unformat_name(res). \
                                                replace(".", ":")
                        expected_error = self.ACCESS_DENIED_ERR.format("VIEW", full_resource_name,
                                                                    priv)
                        result = self.columnar_cbas_utils.create_analytics_view(
                                        self.analytics_cluster,
                                        view_full_name,
                                        view_defn)
                        if not result:
                            self.fail("Failed to create analytics view {}".format(view_full_name))

                        result = self.columnar_cbas_utils.drop_analytics_view(
                                        self.analytics_cluster,
                                        view_full_name,
                                        validate_error_msg=test_case['validate_err_msg'],
                                        expected_error=expected_error,
                                        username=test_case['user'].id,
                                        password=test_case['user'].password)
                        if not result:
                            self.fail("Test case failed while attempting to drop analytics view {}".
                                      format(view_full_name))

    def test_rbac_view_select(self):
        self.log.info("RBAC views select test started")

        self.build_cbas_columnar_infra()

        datasets = self.columnar_cbas_utils.get_all_dataset_objs("standalone")

        no_of_docs = self.input.param("no_of_docs", 1000)
        jobs = Queue()
        results = []
        for dataset in datasets:
            self.log.info("Adding {} documents in standalone dataset {}. Default doc size is 1KB".
                          format(no_of_docs, dataset.full_name))
            jobs.put((self.columnar_cbas_utils.load_doc_to_standalone_collection,
                      {"cluster": self.analytics_cluster, "collection_name": dataset.name,
                       "dataverse_name": dataset.dataverse_name, "database_name": dataset.database_name
                          , "no_of_docs": no_of_docs}))

        self.columnar_cbas_utils.run_jobs_in_parallel(
            jobs, results, self.sdk_clients_per_user, async_run=False)

        if not all(results):
            self.log.error("Some documents were not inserted")

        view_collection = datasets[0]

        num_views = self.input.param("num_of_views", 2)
        view_defn = "SELECT name, email from {0};".format(
            self.columnar_cbas_utils.format_name(view_collection.full_name))
        for i in range(num_views):
            view_name = self.generate_random_entity_name(type="view")
            view_name = self.columnar_cbas_utils.format_name(view_name)
            result = self.columnar_cbas_utils.create_analytics_view(
                            self.analytics_cluster,
                            view_name,
                            view_defn)
            if not result:
                self.fail("Failed to create analytics view {}".format(view_name))

        views = self.columnar_cbas_utils.get_all_views_from_metadata(self.analytics_cluster)

        testcases = self.create_rbac_testcases(self.view_select_privileges,
                                               views,
                                               privilege_resource_type="view_select")
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
                full_resource_name = self.columnar_cbas_utils.unformat_name(res). \
                                                replace(".", ":")
                expected_error = self.ACCESS_DENIED_ERR.format("VIEW", full_resource_name,
                                                               "SELECT")
                status, metrics, errors, results, _ = (
                            self.columnar_cbas_utils.execute_statement_on_cbas_util(
                                self.analytics_cluster, select_cmd,
                                username=test_case['user'].id,
                                password=test_case['user'].password,
                                timeout=300, analytics_timeout=300))

                if test_case['validate_err_msg']:
                    result = self.columnar_cbas_utils.validate_error_in_response(
                                status, errors, expected_error, None)
                    if not result:
                        self.fail("Test case failed while attempting to get docs from view {}".
                                format(res))
                else:
                    if status != "success":
                        self.fail("Test case failed while attempting to get docs from view {}".
                                format(res))

    def tearDown(self):

        if self.sink_s3_bucket_name:
            self.delete_s3_bucket()

        self.columnar_cbas_utils.cleanup_cbas(self.cluster)
        super(ColumnarRBAC, self).tearDown()
