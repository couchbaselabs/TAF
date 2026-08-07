"""
Created on July 2026

@author: Created using cbRAT cbModule by Vipul Bhardwaj
"""

import copy
from pytests.Capella.RestAPIv4.DatabaseRoles.database_roles_base import \
    DatabaseRoleBase


class CreateDatabaseRole(DatabaseRoleBase):

    def setUp(self, nomenclature="DatabaseRoles_POST"):
        DatabaseRoleBase.setUp(self, nomenclature)

    def tearDown(self):
        super(CreateDatabaseRole, self).tearDown()

    def test_api_path(self):
        testcases = [
            # {
            #     "description": "Send call with valid path params"
            # },
              {
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
                "description": "Add an invalo the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}"
                       "/roles/role",
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
                self.capellaAPI.cluster_ops_apis.db_role_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]
            elif "invalid_clusterId" in testcase:
                cluster = testcase["invalid_clusterId"]

            result = self.capellaAPI.cluster_ops_apis.create_database_role(
                organization, project, cluster,
                self.expected_res["name"], self.expected_res["access"],
                self.expected_res["description"])
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.create_database_role(
                    organization, project, cluster,
                    self.expected_res["name"], self.expected_res["access"],
                    self.expected_res["description"])
            self.capellaAPI.cluster_ops_apis.db_role_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters/{}/roles"
            self.validate_testcase(result, [201], testcase, failures)

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
            result = self.capellaAPI.cluster_ops_apis.create_database_role(
                self.organisation_id, self.project_id, self.cluster_id,
                self.expected_res["name"], self.expected_res["access"],
                self.expected_res["description"], header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.create_database_role(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.expected_res["name"], self.expected_res["access"],
                    self.expected_res["description"], header)
            self.validate_testcase(result, [201], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_payload(self):
        testcases = list()
        for key in ["name", "description"]:
            values = [
                "", 1, 0, 100000, -1, 123.123, None,
                self.generate_random_string(special_characters=False),
                self.generate_random_string(5000, special_characters=False),
            ]
            for value in values:
                testcase = {
                    "name": self.expected_res["name"],
                    "description": self.expected_res["description"],
                    "access": self.expected_res["access"],
                    "desc": "Testing `{}` with val: {} of {}".format(
                        key, value, type(value))
                }
                testcase[key] = value
                if key == "name":
                    if value is None or value == "":
                        testcase["expected_status_code"] = 400
                        testcase["expected_error"] = {
                            "code": 400,
                            "hint": "Please review your request and ensure "
                                    "that all required parameters are "
                                    "correctly provided.",
                            "httpStatusCode": 400,
                            "message": "Name must be at least 2 characters long"
                        }
                    elif not isinstance(value, str):
                        testcase["expected_status_code"] = 400
                        testcase["expected_error"] = {
                            "code": 1000,
                            "hint": "The request was malformed or invalid.",
                            "httpStatusCode": 400,
                            "message": "Bad Request. Error: body contains "
                                       "incorrect JSON type for field \"name\"."
                        }
                    elif len(value) > 32:
                        testcase["expected_status_code"] = 422
                        testcase["expected_error"] = {
                            "code": 422,
                            "hint": "Please review your request and ensure "
                                    "that all required parameters are "
                                    "correctly provided.",
                            "httpStatusCode": 422,
                            "message": "Can not create application user role."
                                       " The name provided is too long. Can"
                                       " not exceed 32 characters. Please"
                                       " revise the user role name and try"
                                       " again."
                        }
                elif key == "description":
                    testcase["name"] = self.generate_random_string(
                        10, special_characters=False)
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
            "name": self.expected_res["name"],
            "description": self.expected_res["description"],
            "access": None,
            "desc": "Testing `access` with val: None of NoneType",
            "expected_status_code": 400,
            "expected_error": {
                "code": 1000,
                "hint": "The request was malformed or invalid.",
                "httpStatusCode": 400,
                "message": "Bad Request. Error: access is required."
            }
        })
        testcases.append({
            "name": self.expected_res["name"],
            "description": self.expected_res["description"],
            "access": [],
            "desc": "Testing `access` with empty list",
            "expected_status_code": 400,
            "expected_error": {
                "code": 1000,
                "hint": "The request was malformed or invalid.",
                "httpStatusCode": 400,
                "message": "Bad Request. Error: access is required."
            }
        })

        failures = list()
        for testcase in testcases:
            self.log.info(testcase["desc"])
            result = self.capellaAPI.cluster_ops_apis.create_database_role(
                self.organisation_id, self.project_id, self.cluster_id,
                testcase["name"], testcase["access"],
                testcase.get("description", ""))
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.create_database_role(
                    self.organisation_id, self.project_id, self.cluster_id,
                    testcase["name"], testcase["access"],
                    testcase.get("description", ""))
            self.validate_testcase(result, [201], testcase, failures,
                                   payloadTest=True)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))


class ListDatabaseRoles(DatabaseRoleBase):

    def setUp(self, nomenclature="DatabaseRoles_LIST"):
        DatabaseRoleBase.setUp(self, nomenclature)

    def tearDown(self):
        super(ListDatabaseRoles, self).tearDown()

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
                       "/roles/role",
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
                self.capellaAPI.cluster_ops_apis.db_role_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]
            elif "invalid_clusterId" in testcase:
                cluster = testcase["invalid_clusterId"]

            result = self.capellaAPI.cluster_ops_apis.list_database_roles(
                organization, project, cluster)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.list_database_roles(
                    organization, project, cluster)
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
            "organizationOwner", "projectOwner", "projectViewer",
            "projectDataReaderWriter", "projectDataReader"
        ]):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = self.capellaAPI.cluster_ops_apis.list_database_roles(
                self.organisation_id, self.project_id, self.cluster_id,
                headers=header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.list_database_roles(
                    self.organisation_id, self.project_id, self.cluster_id,
                    headers=header)
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))
