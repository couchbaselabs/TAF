"""
Created on July 2026

@author: Created using cbRAT cbModule by Vipul Bhardwaj
"""

import copy
from pytests.Capella.RestAPIv4.DatabaseRoles.database_roles_base import \
    DatabaseRoleBase


class GetDatabaseRole(DatabaseRoleBase):

    def setUp(self, nomenclature="DatabaseRoles_GET"):
        DatabaseRoleBase.setUp(self, nomenclature)

    def tearDown(self):
        super(GetDatabaseRole, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Send call with valid path params"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}/roles",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title>"
                                  "</head><body><center><h1>404NotFound"
                                  "</h1></center><hr><center>nginx</center>"
                                  "</body></html>"
            }, {
                "description": "Replace the last path param name in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/role",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}"
                       "/roles/extra",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Call API with non-hex organizationId",
                "invalid_organizationId": self.replace_last_character(
                    self.organisation_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if you have provided a valid URL and all "
                            "the required params are present in the request "
                            "body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived to "
                               "be a client error."
                }
            }, {
                "description": "Call API with non-hex projectId",
                "invalid_projectId": self.replace_last_character(
                    self.project_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if you have provided a valid URL and all "
                            "the required params are present in the request "
                            "body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived to "
                               "be a client error."
                }
            }, {
                "description": "Call API with non-hex clusterId",
                "invalid_clusterId": self.replace_last_character(
                    self.cluster_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if you have provided a valid URL and all "
                            "the required params are present in the request "
                            "body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived to "
                               "be a client error."
                }
            }, {
                "description": "Call API with non-hex roleId",
                "invalid_roleId": self.replace_last_character(
                    self.role_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if you have provided a valid URL and all "
                            "the required params are present in the request "
                            "body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived to "
                               "be a client error."
                }
            }
        ]
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            organization = self.organisation_id
            project = self.project_id
            cluster = self.cluster_id
            role = self.role_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.db_role_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]
            elif "invalid_clusterId" in testcase:
                cluster = testcase["invalid_clusterId"]
            elif "invalid_roleId" in testcase:
                role = testcase["invalid_roleId"]

            result = self.capellaAPI.cluster_ops_apis.fetch_database_role_info(
                organization, project, cluster, role)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = \
                    self.capellaAPI.cluster_ops_apis.fetch_database_role_info(
                        organization, project, cluster, role)
            self.capellaAPI.cluster_ops_apis.db_role_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters/{}/roles"
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner", "projectViewer"
        ]):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = self.capellaAPI.cluster_ops_apis.fetch_database_role_info(
                self.organisation_id, self.project_id, self.cluster_id,
                self.role_id, header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = \
                    self.capellaAPI.cluster_ops_apis.fetch_database_role_info(
                        self.organisation_id, self.project_id, self.cluster_id,
                        self.role_id, header)
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_query_parameters(self):
        self.log.debug(
            "Correct Params - OrgID: {}, ProjID: {}, ClusID: {}, "
            "RoleID: {}".format(
                self.organisation_id, self.project_id, self.cluster_id,
                self.role_id))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id, self.cluster_id,
                self.role_id):
            testcases += 1
            testcase = {
                "description": "OrganizationID: {}, ProjectID: {}, "
                               "ClusterID: {}, RoleID: {}".format(
                                   str(combination[0]), str(combination[1]),
                                   str(combination[2]), str(combination[3])),
                "organizationID": combination[0],
                "projectID": combination[1],
                "clusterID": combination[2],
                "roleID": combination[3]
            }
            if not (combination[0] == self.organisation_id and
                    combination[1] == self.project_id and
                    combination[2] == self.cluster_id and
                    combination[3] == self.role_id):
                if (combination[0] == "" or combination[1] == "" or
                        combination[2] == "" or combination[3] == ""):
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = "404 page not found"
                elif any(variable in [
                    int, bool, float, list, tuple, set, type(None)] for
                         variable in [
                             type(combination[0]), type(combination[1]),
                             type(combination[2]), type(combination[3])]):
                    testcase["expected_status_code"] = 400
                    testcase["expected_error"] = {
                        "code": 1000,
                        "hint": "Check if all the required params are "
                                "present in the request body.",
                        "httpStatusCode": 400,
                        "message": "The server cannot or will not process "
                                   "the request due to something that is "
                                   "perceived to be a client error."
                    }
                elif combination[0] != self.organisation_id:
                    testcase["expected_status_code"] = 403
                    testcase["expected_error"] = {
                        "code": 1002,
                        "hint": "Your access to the requested resource is "
                                "denied. Please make sure you have the "
                                "necessary permissions to access the "
                                "resource.",
                        "httpStatusCode": 403,
                        "message": "Access Denied."
                    }
                elif combination[2] != self.cluster_id:
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = {
                        "code": 4025,
                        "hint": "The requested cluster details could not be "
                                "found or fetched. Please ensure that the "
                                "correct cluster ID is provided.",
                        "message": "Unable to fetch the cluster details.",
                        "httpStatusCode": 404
                    }
                elif combination[1] != self.project_id:
                    testcase["expected_status_code"] = 422
                    testcase["expected_error"] = {
                        "code": 4031,
                        "hint": "Please provide a valid projectId.",
                        "httpStatusCode": 422,
                        "message": "Unable to process the request. The "
                                   "provided projectId {} is not valid for "
                                   "the cluster {}.".format(
                                       combination[1], combination[2])
                    }
                else:
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = {
                        "code": 404,
                        "hint": "The requested database role details could "
                                "not be found or fetched. Please ensure that "
                                "the correct role ID is provided.",
                        "message": "Role not found.",
                        "httpStatusCode": 404
                    }
            self.log.info("Executing test: {}".format(testcase["description"]))
            if "param" in testcase:
                kwarg = {testcase["param"]: testcase["paramValue"]}
            else:
                kwarg = dict()

            result = self.capellaAPI.cluster_ops_apis.fetch_database_role_info(
                testcase["organizationID"], testcase["projectID"],
                testcase["clusterID"], testcase["roleID"], **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = \
                    self.capellaAPI.cluster_ops_apis.fetch_database_role_info(
                        testcase["organizationID"], testcase["projectID"],
                        testcase["clusterID"], testcase["roleID"], **kwarg)
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.fetch_database_role_info, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.role_id
            )
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.fetch_database_role_info, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.role_id
            )
        ]]
        self.throttle_test(api_func_list, True, self.project_id)


class UpdateDatabaseRole(DatabaseRoleBase):

    def setUp(self, nomenclature="DatabaseRoles_PUT"):
        DatabaseRoleBase.setUp(self, nomenclature)
        self.update_access = [
            {
            "privileges": [
                "dataRead"
            ]
        }
        ]
        self.update_description = "Updated test role description"

    def tearDown(self):
        super(UpdateDatabaseRole, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Send call with valid path params"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}/roles",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title>"
                                  "</head><body><center><h1>404NotFound"
                                  "</h1></center><hr><center>nginx</center>"
                                  "</body></html>"
            }, {
                "description": "Replace the last path param name in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/role",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}"
                       "/roles/extra",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Call API with non-hex organizationId",
                "invalid_organizationId": self.replace_last_character(
                    self.organisation_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if you have provided a valid URL and all "
                            "the required params are present in the request "
                            "body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived to "
                               "be a client error."
                }
            }, {
                "description": "Call API with non-hex projectId",
                "invalid_projectId": self.replace_last_character(
                    self.project_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if you have provided a valid URL and all "
                            "the required params are present in the request "
                            "body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived to "
                               "be a client error."
                }
            }, {
                "description": "Call API with non-hex clusterId",
                "invalid_clusterId": self.replace_last_character(
                    self.cluster_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if you have provided a valid URL and all "
                            "the required params are present in the request "
                            "body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived to "
                               "be a client error."
                }
            }, {
                "description": "Call API with non-hex roleId",
                "invalid_roleId": self.replace_last_character(
                    self.role_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if you have provided a valid URL and all "
                            "the required params are present in the request "
                            "body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived to "
                               "be a client error."
                }
            }
        ]
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            organization = self.organisation_id
            project = self.project_id
            cluster = self.cluster_id
            role = self.role_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.db_role_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]
            elif "invalid_clusterId" in testcase:
                cluster = testcase["invalid_clusterId"]
            elif "invalid_roleId" in testcase:
                role = testcase["invalid_roleId"]

            result = self.capellaAPI.cluster_ops_apis.update_database_role(
                organization, project, cluster, role,
                self.update_access, self.update_description)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.update_database_role(
                    organization, project, cluster, role,
                    self.update_access, self.update_description)
            self.capellaAPI.cluster_ops_apis.db_role_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters/{}/roles"
            self.validate_testcase(result, [204], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner"
        ]):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = self.capellaAPI.cluster_ops_apis.update_database_role(
                self.organisation_id, self.project_id, self.cluster_id,
                self.role_id, self.update_access, self.update_description,
                header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.update_database_role(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.role_id, self.update_access, self.update_description,
                    header)
            self.validate_testcase(result, [204], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_payload(self):
        testcases = list()
        for key in ["description"]:
            values = [
                "", 1, 0, 100000, -1, 123.123, None,
                self.generate_random_string(special_characters=False),
                self.generate_random_string(5000, special_characters=False),
            ]
            for value in values:
                testcase = {
                    "access": self.update_access,
                    "description": self.update_description,
                    "desc": "Testing `{}` with val: {} of {}".format(
                        key, value, type(value))
                }
                testcase[key] = value
                if value and not isinstance(value, str):
                    testcase["expected_status_code"] = 400
                    testcase["expected_error"] = {
                        "code": 1000,
                        "hint": "The request was malformed or invalid.",
                        "httpStatusCode": 400,
                        "message": "Bad Request. Error: body contains "
                                   "incorrect JSON type for field "
                                   "\"description\"."
                    }
                testcases.append(testcase)

        testcases.append({
            "access": [],
            "description": self.update_description,
            "desc": "Testing `access` with empty list",
            "expected_status_code": 400,
            "expected_error": {
                "code": 1000,
                "hint": "The request was malformed or invalid.",
                "httpStatusCode": 400,
                "message": "Bad Request. Error: access cannot be empty."
            }
        })
        testcases.append({
            "access": "invalid",
            "description": self.update_description,
            "desc": "Testing `access` with string value",
            "expected_status_code": 400,
            "expected_error": {
                "code": 1000,
                "hint": "The request was malformed or invalid.",
                "httpStatusCode": 400,
                "message": "Bad Request. Error: body contains incorrect JSON "
                           "type for field \"access\"."
            }
        })

        failures = list()
        for testcase in testcases:
            self.log.info(testcase["desc"])
            result = self.capellaAPI.cluster_ops_apis.update_database_role(
                self.organisation_id, self.project_id, self.cluster_id,
                self.role_id, testcase["access"],
                testcase.get("description", ""))
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.update_database_role(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.role_id, testcase["access"],
                    testcase.get("description", ""))
            self.validate_testcase(result, [204], testcase, failures,
                                   payloadTest=True)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_query_parameters(self):
        self.log.debug(
            "Correct Params - OrgID: {}, ProjID: {}, ClusID: {}, "
            "RoleID: {}".format(
                self.organisation_id, self.project_id, self.cluster_id,
                self.role_id))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id, self.cluster_id,
                self.role_id):
            testcases += 1
            testcase = {
                "description": "OrganizationID: {}, ProjectID: {}, "
                               "ClusterID: {}, RoleID: {}".format(
                                   str(combination[0]), str(combination[1]),
                                   str(combination[2]), str(combination[3])),
                "organizationID": combination[0],
                "projectID": combination[1],
                "clusterID": combination[2],
                "roleID": combination[3]
            }
            if not (combination[0] == self.organisation_id and
                    combination[1] == self.project_id and
                    combination[2] == self.cluster_id and
                    combination[3] == self.role_id):
                if (combination[0] == "" or combination[1] == "" or
                        combination[2] == "" or combination[3] == ""):
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = "404 page not found"
                elif any(variable in [
                    int, bool, float, list, tuple, set, type(None)] for
                         variable in [
                             type(combination[0]), type(combination[1]),
                             type(combination[2]), type(combination[3])]):
                    testcase["expected_status_code"] = 400
                    testcase["expected_error"] = {
                        "code": 1000,
                        "hint": "Check if all the required params are "
                                "present in the request body.",
                        "httpStatusCode": 400,
                        "message": "The server cannot or will not process "
                                   "the request due to something that is "
                                   "perceived to be a client error."
                    }
                elif combination[0] != self.organisation_id:
                    testcase["expected_status_code"] = 403
                    testcase["expected_error"] = {
                        "code": 1002,
                        "hint": "Your access to the requested resource is "
                                "denied. Please make sure you have the "
                                "necessary permissions to access the "
                                "resource.",
                        "httpStatusCode": 403,
                        "message": "Access Denied."
                    }
                elif combination[2] != self.cluster_id:
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = {
                        "code": 4025,
                        "hint": "The requested cluster details could not be "
                                "found or fetched. Please ensure that the "
                                "correct cluster ID is provided.",
                        "message": "Unable to fetch the cluster details.",
                        "httpStatusCode": 404
                    }
                elif combination[1] != self.project_id:
                    testcase["expected_status_code"] = 422
                    testcase["expected_error"] = {
                        "code": 4031,
                        "hint": "Please provide a valid projectId.",
                        "httpStatusCode": 422,
                        "message": "Unable to process the request. The "
                                   "provided projectId {} is not valid for "
                                   "the cluster {}.".format(
                                       combination[1], combination[2])
                    }
                else:
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = {
                        "code": 404,
                        "hint": "The requested database role details could "
                                "not be found or fetched. Please ensure that "
                                "the correct role ID is provided.",
                        "message": "Role not found.",
                        "httpStatusCode": 404
                    }
            self.log.info("Executing test: {}".format(testcase["description"]))
            if "param" in testcase:
                kwarg = {testcase["param"]: testcase["paramValue"]}
            else:
                kwarg = dict()

            result = self.capellaAPI.cluster_ops_apis.update_database_role(
                testcase["organizationID"], testcase["projectID"],
                testcase["clusterID"], testcase["roleID"],
                self.update_access, self.update_description, **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.update_database_role(
                    testcase["organizationID"], testcase["projectID"],
                    testcase["clusterID"], testcase["roleID"],
                    self.update_access, self.update_description, **kwarg)
            self.validate_testcase(result, [204], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.update_database_role, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.role_id, self.update_access, self.update_description
            )
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.update_database_role, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.role_id, self.update_access, self.update_description
            )
        ]]
        self.throttle_test(api_func_list, True, self.project_id)


class DeleteDatabaseRole(DatabaseRoleBase):

    def setUp(self, nomenclature="DatabaseRoles_DELETE"):
        DatabaseRoleBase.setUp(self, nomenclature)

    def tearDown(self):
        super(DeleteDatabaseRole, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Send call with valid path params"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}/roles",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title>"
                                  "</head><body><center><h1>404NotFound"
                                  "</h1></center><hr><center>nginx</center>"
                                  "</body></html>"
            }, {
                "description": "Replace the last path param name in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/role",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}"
                       "/roles/extra",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Call API with non-hex organizationId",
                "invalid_organizationId": self.replace_last_character(
                    self.organisation_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if you have provided a valid URL and all "
                            "the required params are present in the request "
                            "body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived to "
                               "be a client error."
                }
            }, {
                "description": "Call API with non-hex projectId",
                "invalid_projectId": self.replace_last_character(
                    self.project_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if you have provided a valid URL and all "
                            "the required params are present in the request "
                            "body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived to "
                               "be a client error."
                }
            }, {
                "description": "Call API with non-hex clusterId",
                "invalid_clusterId": self.replace_last_character(
                    self.cluster_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if you have provided a valid URL and all "
                            "the required params are present in the request "
                            "body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived to "
                               "be a client error."
                }
            }, {
                "description": "Call API with non-hex roleId",
                "invalid_roleId": self.replace_last_character(
                    self.role_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if you have provided a valid URL and all "
                            "the required params are present in the request "
                            "body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived to "
                               "be a client error."
                }
            }
        ]
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            organization = self.organisation_id
            project = self.project_id
            cluster = self.cluster_id
            role = self.role_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.db_role_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]
            elif "invalid_clusterId" in testcase:
                cluster = testcase["invalid_clusterId"]
            elif "invalid_roleId" in testcase:
                role = testcase["invalid_roleId"]

            result = self.capellaAPI.cluster_ops_apis.delete_database_role(
                organization, project, cluster, role)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.delete_database_role(
                    organization, project, cluster, role)
            self.capellaAPI.cluster_ops_apis.db_role_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters/{}/roles"
            if self.validate_testcase(result, [204], testcase, failures):
                self.log.debug("Deletion Successful, recreating role.")
                res = self.capellaAPI.cluster_ops_apis.create_database_role(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.expected_res["name"], self.expected_res["access"],
                    self.expected_res["description"])
                if res.status_code == 429:
                    self.handle_rate_limit(int(res.headers["Retry-After"]))
                    res = self.capellaAPI.cluster_ops_apis.create_database_role(
                        self.organisation_id, self.project_id, self.cluster_id,
                        self.expected_res["name"], self.expected_res["access"],
                        self.expected_res["description"])
                if res.status_code == 201:
                    self.role_id = res.json()["id"]

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner"
        ]):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = self.capellaAPI.cluster_ops_apis.delete_database_role(
                self.organisation_id, self.project_id, self.cluster_id,
                self.role_id, header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.delete_database_role(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.role_id, header)
            if self.validate_testcase(result, [204], testcase, failures):
                self.log.debug("Deletion Successful, recreating role.")
                res = self.capellaAPI.cluster_ops_apis.create_database_role(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.expected_res["name"], self.expected_res["access"],
                    self.expected_res["description"])
                if res.status_code == 429:
                    self.handle_rate_limit(int(res.headers["Retry-After"]))
                    res = self.capellaAPI.cluster_ops_apis.create_database_role(
                        self.organisation_id, self.project_id, self.cluster_id,
                        self.expected_res["name"], self.expected_res["access"],
                        self.expected_res["description"])
                if res.status_code == 201:
                    self.role_id = res.json()["id"]

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_query_parameters(self):
        self.log.debug(
            "Correct Params - OrgID: {}, ProjID: {}, ClusID: {}, "
            "RoleID: {}".format(
                self.organisation_id, self.project_id, self.cluster_id,
                self.role_id))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id, self.cluster_id,
                self.role_id):
            testcases += 1
            testcase = {
                "description": "OrganizationID: {}, ProjectID: {}, "
                               "ClusterID: {}, RoleID: {}".format(
                                   str(combination[0]), str(combination[1]),
                                   str(combination[2]), str(combination[3])),
                "organizationID": combination[0],
                "projectID": combination[1],
                "clusterID": combination[2],
                "roleID": combination[3]
            }
            if not (combination[0] == self.organisation_id and
                    combination[1] == self.project_id and
                    combination[2] == self.cluster_id and
                    combination[3] == self.role_id):
                if (combination[0] == "" or combination[1] == "" or
                        combination[2] == "" or combination[3] == ""):
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = "404 page not found"
                elif any(variable in [
                    int, bool, float, list, tuple, set, type(None)] for
                         variable in [
                             type(combination[0]), type(combination[1]),
                             type(combination[2]), type(combination[3])]):
                    testcase["expected_status_code"] = 400
                    testcase["expected_error"] = {
                        "code": 1000,
                        "hint": "Check if all the required params are "
                                "present in the request body.",
                        "httpStatusCode": 400,
                        "message": "The server cannot or will not process "
                                   "the request due to something that is "
                                   "perceived to be a client error."
                    }
                elif combination[0] != self.organisation_id:
                    testcase["expected_status_code"] = 403
                    testcase["expected_error"] = {
                        "code": 1002,
                        "hint": "Your access to the requested resource is "
                                "denied. Please make sure you have the "
                                "necessary permissions to access the "
                                "resource.",
                        "httpStatusCode": 403,
                        "message": "Access Denied."
                    }
                elif combination[2] != self.cluster_id:
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = {
                        "code": 4025,
                        "hint": "The requested cluster details could not be "
                                "found or fetched. Please ensure that the "
                                "correct cluster ID is provided.",
                        "message": "Unable to fetch the cluster details.",
                        "httpStatusCode": 404
                    }
                elif combination[1] != self.project_id:
                    testcase["expected_status_code"] = 422
                    testcase["expected_error"] = {
                        "code": 4031,
                        "hint": "Please provide a valid projectId.",
                        "httpStatusCode": 422,
                        "message": "Unable to process the request. The "
                                   "provided projectId {} is not valid for "
                                   "the cluster {}.".format(
                                       combination[1], combination[2])
                    }
                else:
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = {
                        "code": 404,
                        "hint": "The requested database role details could "
                                "not be found or fetched. Please ensure that "
                                "the correct role ID is provided.",
                        "message": "Role not found.",
                        "httpStatusCode": 404
                    }
            self.log.info("Executing test: {}".format(testcase["description"]))
            if "param" in testcase:
                kwarg = {testcase["param"]: testcase["paramValue"]}
            else:
                kwarg = dict()

            result = self.capellaAPI.cluster_ops_apis.delete_database_role(
                testcase["organizationID"], testcase["projectID"],
                testcase["clusterID"], testcase["roleID"], **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.delete_database_role(
                    testcase["organizationID"], testcase["projectID"],
                    testcase["clusterID"], testcase["roleID"], **kwarg)
            if self.validate_testcase(result, [204], testcase, failures):
                self.log.debug("Deletion Successful, recreating role.")
                res = self.capellaAPI.cluster_ops_apis.create_database_role(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.expected_res["name"], self.expected_res["access"],
                    self.expected_res["description"])
                if res.status_code == 429:
                    self.handle_rate_limit(int(res.headers["Retry-After"]))
                    res = self.capellaAPI.cluster_ops_apis.create_database_role(
                        self.organisation_id, self.project_id, self.cluster_id,
                        self.expected_res["name"], self.expected_res["access"],
                        self.expected_res["description"])
                if res.status_code == 201:
                    self.role_id = res.json()["id"]

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.delete_database_role, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.role_id
            )
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.delete_database_role, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.role_id
            )
        ]]
        self.throttle_test(api_func_list, True, self.project_id)
