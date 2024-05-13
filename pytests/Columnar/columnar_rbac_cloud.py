import random
import string
from Columnar.columnar_base import ColumnarBaseTest
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI
from capella_utils.columnar_final import (ColumnarUtils, ColumnarRBACUtil)
from cbas_utils.cbas_utils_columnar import CbasUtil as columnarCBASUtil

from Queue import Queue

from awsLib.s3_data_helper import perform_S3_operation


class ColumnarRBAC(ColumnarBaseTest):
    def setUp(self):
        super(ColumnarRBAC, self).setUp()
        self.cluster = self.tenant.columnar_instances[0]
        self.remote_cluster = self.cluster
        if self.num_clusters > 0:
            self.remote_cluster = self.tenant.clusters[0]
        self.aws_access_key = self.input.param("aws_access_key")
        self.aws_secret_key = self.input.param("aws_secret_key")
        self.aws_session_token = self.input.param("aws_session_token", "")
        self.doc_loader_url = self.input.param("sirius_url", None)
        self.doc_loader_port = self.input.param("sirius_port", None)
        self.no_of_docs = self.input.param("no_of_docs", 1000)
        self.capellaAPI = CapellaAPI(self.pod.url_public, '', '', self.tenant.user, self.tenant.pwd, '')
        self.columnar_utils = ColumnarUtils(self.log)
        self.columnar_rbac_util = ColumnarRBACUtil(self.log)
        self.columnar_cbas_utils = columnarCBASUtil(
            self.task, self.use_sdk_for_cbas)

        self.ACCESS_DENIED_ERR = "User must have permission (cluster.analytics.grant.{}[{}]!{})"
        self.database_privileges = ["database_create", "database_drop"]
        self.scope_privileges = ["scope_create", "scope_drop"]
        self.collection_ddl_privileges = ["collection_create", "collection_drop"]
        self.collection_dml_privileges = ["collection_insert", "collection_upsert",
                                          "collection_delete", "collection_analyze"]
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
        self.aws_bucket_name = "columnar-sanity-test-data-mohsin"

    def get_remote_cluster_certificate(self, remote_cluster):
        remote_cluster_certificate_request = (
            self.capellaAPI.cluster_ops_apis.get_cluster_certificate(self.tenant.id, self.tenant.project_id,
                                                                     remote_cluster.id))
        if remote_cluster_certificate_request.status_code == 200:
            remote_cluster_certificate = (remote_cluster_certificate_request.json()["certificate"])
        else:
            self.fail("Failed to get cluster certificate")

        return remote_cluster_certificate

    def build_cbas_columnar_infra(self):

        self.columnar_spec["database"]["no_of_databases"] = self.input.param(
            "no_of_databases", 1)
        self.columnar_spec["dataverse"]["no_of_dataverses"] = self.input.param(
            "no_of_scopes", 0)

        self.columnar_spec["remote_link"]["no_of_remote_links"] = self.input.param(
            "no_of_remote_links", 0)

        resp = (self.capellaAPI.create_control_plane_api_key(self.tenant.id, 'init api keys')).json()
        self.capellaAPI.cluster_ops_apis.SECRET = resp['secretKey']
        self.capellaAPI.cluster_ops_apis.ACCESS = resp['id']
        self.capellaAPI.cluster_ops_apis.bearer_token = resp['token']
        self.capellaAPI.org_ops_apis.SECRET = resp['secretKey']
        self.capellaAPI.org_ops_apis.ACCESS = resp['id']
        self.capellaAPI.org_ops_apis.bearer_token = resp['token']
        resp = self.capellaAPI.cluster_ops_apis.add_CIDR_to_allowed_CIDRs_list(self.tenant.id,
                                                                               self.tenant.project_id,
                                                                               self.remote_cluster.id, "0.0.0.0/0")
        if resp.status_code == 201 or resp.status_code == 422:
            self.log.info("Added allowed IP 0.0.0.0/0")
        else:
            self.fail("Failed to add allowed IP")

        resp = self.capellaAPI.load_sample_bucket(self.tenant.id, self.tenant.project_id,
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
                              resource_type="instance"):

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

        username = self.generate_random_entity_name(type="user")
        password = self.generate_random_password()
        user3 = self.columnar_rbac_util.create_api_keys(self.pod, self.tenant,
                                                        self.tenant.project_id, self.cluster,
                                                        username=username,
                                                        password=password)
        negative_test_case = self.generate_test_case(user3, privileges, [resources[0]],
                                                     validate_err_msg=True)
        testcases.append(negative_test_case)

        role_name = self.generate_random_entity_name(type="role")
        role2 = self.columnar_rbac_util.create_columnar_role(self.pod, self.tenant,
                                                             self.tenant.project_id, self.cluster,
                                                             role_name=role_name)
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

    def test_rbac_collection_ddl(self):
        self.log.info("RBAC collection ddl test started")
        self.build_cbas_columnar_infra()


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
                            self.fail("Test case failed while attempting to upsert doc "\
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
                            self.fail("Test case failed while attempting to create function "\
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

    def tearDown(self):

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