"""
Created on July 2026

@author: Created using cbRAT cbModule by Vipul Bhardwaj
"""

import copy
from pytests.Capella.RestAPIv4.DatabaseCredentials.database_credentials_base \
    import DatabaseCredentialBase


class CreateDatabaseCredential(DatabaseCredentialBase):

    def setUp(self, nomenclature="DatabaseCredentials_POST"):
        DatabaseCredentialBase.setUp(self, nomenclature)

    def tearDown(self):
        super(CreateDatabaseCredential, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Send call with valid path params"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}/users",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title>"
                                  "</head><body><center><h1>404NotFound"
                                  "</h1></center><hr><center>nginx</center>"
                                  "</body></html>"
            }, {
                "description": "Replace the last path param name in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/user",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}"
                       "/users/user",
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
            }
        ]
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            organization = self.organisation_id
            project = self.project_id
            cluster = self.cluster_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.db_user_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]
            elif "invalid_clusterId" in testcase:
                cluster = testcase["invalid_clusterId"]

            result = self.capellaAPI.cluster_ops_apis.create_database_user(
                organization, project, cluster,
                self.expected_res["name"], self.expected_res["password"], self.expected_res["credentialType"], self.expected_res["userRoles"])
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.create_database_user(
                    organization, project, cluster,
                    self.expected_res["name"], self.expected_res["access"])
            self.capellaAPI.cluster_ops_apis.db_user_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters/{}/users"
            self.validate_testcase(result, [201], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner", "projectManager"
        ]):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = self.capellaAPI.cluster_ops_apis.create_database_user(
                self.organisation_id, self.project_id, self.cluster_id,
                self.expected_res["name"], self.expected_res["password"], self.expected_res["credentialType"], self.expected_res["userRoles"],
                headers=header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.create_database_user(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.expected_res["name"], self.expected_res["password"], self.expected_res["credentialType"], self.expected_res["userRoles"],
                    headers=header)
            self.validate_testcase(result, [201], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_payload(self):
        testcases = list()

        for key in ["name"]:
            values = [
                "", 1, 0, 100000, -1, 123.123, None
            ]
            for value in values:
                testcase = {
                    "name": self.expected_res["name"],
                    "password": self.expected_res["password"],
                    "credentialType": self.expected_res["credentialType"],
                    "userRoles": self.expected_res["userRoles"],
                    "desc": "Testing `{}` with val: {} of {}".format(
                        key, value, type(value))
                }
                testcase[key] = value
                if key == "name":
                    if value is None or value == "":
                        testcase["expected_status_code"] = 400
                        testcase["expected_error"] = {
                            "code": 1000,
                            "hint": "The request was malformed or invalid.",
                            "httpStatusCode": 400,
                            "message": "Bad Request. Error: name is required."
                        }
                    elif not isinstance(value, str):
                        testcase["expected_status_code"] = 400
                        testcase["expected_error"] = {
                            "code": 1000,
                            "hint": "The request was malformed or invalid.",
                            "httpStatusCode": 400,
                            "message": "Bad Request. Error: body contains "
                                       "incorrect JSON type for field "
                                       "\"{}\".".format(key)
                        }
                testcases.append(testcase)

        testcases.append({
            "name": self.expected_res["name"],
            "password": self.expected_res["password"],
            "credentialType": "advanced",
            "userRoles": ["access"],
            "desc": "Creating advanced credential with access - should fail "
                    "(mutually exclusive)",
            "expected_status_code": 422,
            "expected_error": {
                "code": 8024,
                "hint": "Create the application user roles on the cluster, or remove them from the request, before assigning them to a database credential.",
                "httpStatusCode": 422,
                "message": "Can not manage the database credential as the following application user roles do not exist on this cluster: access. Please create the user roles or revise the request and try again."
            }
        })
        testcases.append({
            "name": self.expected_res["name"],
            "password": self.expected_res["password"],
            "credentialType": "basic",
            "userRoles": [self.role_name],
            "desc": "Creating basic credential with userRoles - should fail "
                    "(mutually exclusive)",
            "expected_status_code": 422,
            "expected_error": {
                "code": 422,
                "hint": "Please review your request and ensure that all required parameters are correctly provided.",
                "httpStatusCode": 422,
                "message": "Can not create new dataplane user without at least (1) valid permission being provided. Please provide at least (1) valid permission for the new user."
            }
        })
        #1
        testcases.append({
            "name": self.expected_res["name"],
            "password": self.expected_res["password"],
            "credentialType": "advanced",
            "userRoles": None,
            "desc": "Creating advanced credential without userRoles - should "
                    "fail",
            "expected_status_code": 422,
            "expected_error": {
                "code": 422,
                "hint": "Please review your request and ensure that all required parameters are correctly provided.",
                "httpStatusCode": 422,
                "message": "Can not create new dataplane user without at least (1) valid permission being provided. Please provide at least (1) valid permission for the new user."
            }
        })
        testcases.append({
            "name": self.expected_res["name"],
            "password": self.expected_res["password"],
            "credentialType": "invalid",
            "userRoles": None,
            "desc": "Creating credential with invalid credentialType",
            "expected_status_code": 400,
            "expected_error": {
                "code": 1000,
                "hint": "Please review your request and ensure that all required parameters are correctly provided.",
                "httpStatusCode": 400,
                "message": "Can not create application user. The credential type provided invalid is invalid. Please check the request and try again."
            }
        })

        failures = list()
        for testcase in testcases:
            self.log.info(testcase["desc"])
            result = self.capellaAPI.cluster_ops_apis.create_database_user(
                self.organisation_id, self.project_id, self.cluster_id,
                testcase["name"], testcase.get("password"),
                credentialType=testcase.get("credentialType", "basic"),
                userRoles=testcase.get("userRoles"))
            print("Result: {}".format(result.json()))
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.create_database_user(
                    self.organisation_id, self.project_id, self.cluster_id,
                    testcase["name"], testcase.get("password"),
                    credentialType=testcase.get("credentialType", "basic"),
                    userRoles=testcase.get("userRoles"))
            self.validate_testcase(result, [201], testcase, failures,
                                   payloadTest=True)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_query_parameters(self):
        self.log.debug(
            "Correct Params - OrgID: {}, ProjID: {}, ClusID: {}".format(
                self.organisation_id, self.project_id, self.cluster_id))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id, self.cluster_id):
            testcases += 1
            testcase = {
                "description": "OrganizationID: {}, ProjectID: {}, "
                               "ClusterID: {}".format(
                                   str(combination[0]), str(combination[1]),
                                   str(combination[2])),
                "organizationID": combination[0],
                "projectID": combination[1],
                "clusterID": combination[2]
            }
            if not (combination[0] == self.organisation_id and
                    combination[1] == self.project_id and
                    combination[2] == self.cluster_id):
                if (combination[0] == "" or combination[1] == "" or
                        combination[2] == ""):
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = "404 page not found"
                elif any(variable in [
                    int, bool, float, list, tuple, set, type(None)] for
                         variable in [
                             type(combination[0]), type(combination[1]),
                             type(combination[2])]):
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
                else:
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
            self.log.info("Executing test: {}".format(testcase["description"]))
            if "param" in testcase:
                kwarg = {testcase["param"]: testcase["paramValue"]}
            else:
                kwarg = dict()

            result = self.capellaAPI.cluster_ops_apis.create_database_user(
                testcase["organizationID"], testcase["projectID"],
                testcase["clusterID"],
                self.expected_res["name"], self.expected_res["access"],
                **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.create_database_user(
                    testcase["organizationID"], testcase["projectID"],
                    testcase["clusterID"],
                    self.expected_res["name"], self.expected_res["access"],
                    **kwarg)
            self.validate_testcase(result, [201], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.create_database_user, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.expected_res["name"], self.expected_res["access"]
            )
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.create_database_user, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.expected_res["name"], self.expected_res["access"]
            )
        ]]
        self.throttle_test(api_func_list, True, self.project_id)


class ListDatabaseCredentials(DatabaseCredentialBase):

    def setUp(self, nomenclature="DatabaseCredentials_LIST"):
        DatabaseCredentialBase.setUp(self, nomenclature)
        self.list_expected_res = {
            "cursor": {
                "hrefs": {
                    "first": None,
                    "last": None,
                    "next": None,
                    "previous": None
                },
                "pages": {
                    "last": None,
                    "next": None,
                    "page": None,
                    "perPage": None,
                    "previous": None,
                    "totalItems": None
                }
            },
            "data": []
        }

    def tearDown(self):
        super(ListDatabaseCredentials, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Send call with valid path params"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}/users",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title>"
                                  "</head><body><center><h1>404NotFound"
                                  "</h1></center><hr><center>nginx</center>"
                                  "</body></html>"
            }, {
                "description": "Replace the last path param name in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/user",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}"
                       "/users/user",
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
            }
        ]
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            organization = self.organisation_id
            project = self.project_id
            cluster = self.cluster_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.db_user_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]
            elif "invalid_clusterId" in testcase:
                cluster = testcase["invalid_clusterId"]

            result = self.capellaAPI.cluster_ops_apis.list_database_users(
                organization, project, cluster)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.list_database_users(
                    organization, project, cluster)
            self.capellaAPI.cluster_ops_apis.db_user_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters/{}/users"
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner", "projectManager",
            "projectViewer", "projectDataReaderWriter", "projectDataReader"
        ]):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = self.capellaAPI.cluster_ops_apis.list_database_users(
                self.organisation_id, self.project_id, self.cluster_id,
                headers=header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.list_database_users(
                    self.organisation_id, self.project_id, self.cluster_id,
                    headers=header)
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_query_parameters(self):
        self.log.debug(
            "Correct Params - OrgID: {}, ProjID: {}, ClusID: {}".format(
                self.organisation_id, self.project_id, self.cluster_id))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id, self.cluster_id):
            testcases += 1
            testcase = {
                "description": "OrganizationID: {}, ProjectID: {}, "
                               "ClusterID: {}".format(
                                   str(combination[0]), str(combination[1]),
                                   str(combination[2])),
                "organizationID": combination[0],
                "projectID": combination[1],
                "clusterID": combination[2]
            }
            if not (combination[0] == self.organisation_id and
                    combination[1] == self.project_id and
                    combination[2] == self.cluster_id):
                if (combination[0] == "" or combination[1] == "" or
                        combination[2] == ""):
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = "404 page not found"
                elif any(variable in [
                    int, bool, float, list, tuple, set, type(None)] for
                         variable in [
                             type(combination[0]), type(combination[1]),
                             type(combination[2])]):
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
                else:
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
            self.log.info("Executing test: {}".format(testcase["description"]))
            if "param" in testcase:
                kwarg = {testcase["param"]: testcase["paramValue"]}
            else:
                kwarg = dict()

            result = self.capellaAPI.cluster_ops_apis.list_database_users(
                testcase["organizationID"], testcase["projectID"],
                testcase["clusterID"], **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.list_database_users(
                    testcase["organizationID"], testcase["projectID"],
                    testcase["clusterID"], **kwarg)
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.list_database_users, (
                self.organisation_id, self.project_id, self.cluster_id
            )
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.list_database_users, (
                self.organisation_id, self.project_id, self.cluster_id
            )
        ]]
        self.throttle_test(api_func_list, True, self.project_id)
