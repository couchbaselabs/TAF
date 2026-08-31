"""
Created on July 2026

@author: Created using cbRAT cbModule by Vipul Bhardwaj
"""

from random import random
import string
import copy

from pytests.Capella.RestAPIv4.DatabaseCredentials.database_credentials_base \
    import DatabaseCredentialBase


class GetDatabaseCredential(DatabaseCredentialBase):

    def setUp(self, nomenclature="DatabaseCredentials_GET"):
        DatabaseCredentialBase.setUp(self, nomenclature)

    def tearDown(self):
        super(GetDatabaseCredential, self).tearDown()

    def test_api_path(self):
        self.log.info("Creating Database Credential for the test")
        res = self.capellaAPI.cluster_ops_apis.create_database_user(
            self.organisation_id, self.project_id, self.cluster_id,
            self.expected_res["name"], self.expected_res["password"], self.expected_res["credentialType"], self.expected_res["userRoles"])
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.create_database_user(
                self.organisation_id, self.project_id, self.cluster_id,
                self.expected_res["name"], self.expected_res["access"])
        if res.status_code != 201:
            self.log.error("Result: {}".format(res.content))
            self.tearDown()
            self.fail("Error while creating Database Credential for the test.")
        self.log.info("Database Credential created successfully.")
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
                       "/users/extra",
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
                "description": "Call API with non-hex userId",
                "invalid_userId": self.replace_last_character(
                    res.json()["id"], non_hex=True),
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
            user = res.json()["id"]

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.db_user_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]
            elif "invalid_clusterId" in testcase:
                cluster = testcase["invalid_clusterId"]
            elif "invalid_userId" in testcase:
                user = testcase["invalid_userId"]

            result = self.capellaAPI.cluster_ops_apis.fetch_database_user_info(
                organization, project, cluster, user)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = \
                    self.capellaAPI.cluster_ops_apis.fetch_database_user_info(
                        organization, project, cluster, user)
            self.capellaAPI.cluster_ops_apis.db_user_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters/{}/users"
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        self.log.info("Creating Database Credential for the test")
        res = self.capellaAPI.cluster_ops_apis.create_database_user(
            self.organisation_id, self.project_id, self.cluster_id,
            self.expected_res["name"], self.expected_res["password"],
            self.expected_res["credentialType"], self.expected_res["userRoles"])
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.create_database_user(
                self.organisation_id, self.project_id, self.cluster_id,
                self.expected_res["name"], self.expected_res["password"],
                self.expected_res["credentialType"],
                self.expected_res["userRoles"])
        if res.status_code != 201:
            self.log.error("Result: {}".format(res.content))
            self.tearDown()
            self.fail("Error while creating Database Credential for the test.")
        self.log.info("Database Credential created successfully.")

        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner", "projectViewer"
        ]):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = self.capellaAPI.cluster_ops_apis.fetch_database_user_info(
                self.organisation_id, self.project_id, self.cluster_id,
                res.json()["id"], header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = \
                    self.capellaAPI.cluster_ops_apis.fetch_database_user_info(
                        self.organisation_id, self.project_id,
                        self.cluster_id, res.json()["id"], header)
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))


class UpdateDatabaseCredential(DatabaseCredentialBase):

    def setUp(self, nomenclature="DatabaseCredentials_PUT"):
        DatabaseCredentialBase.setUp(self, nomenclature)
        self.update_access = {
            "password": "Mathematics12!",
            "userRoles": [
                self.prefix + "cred-role"
            ],
            "credentialType": "advanced"
        }

    def tearDown(self):
        super(UpdateDatabaseCredential, self).tearDown()

    def test_api_path(self):
        self.log.info("Creating Database Credential for the test")
        res = self.capellaAPI.cluster_ops_apis.create_database_user(
            self.organisation_id, self.project_id, self.cluster_id,
            self.expected_res["name"], self.expected_res["password"], self.expected_res["credentialType"], self.expected_res["userRoles"])
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.create_database_user(
                self.organisation_id, self.project_id, self.cluster_id,
                self.expected_res["name"], self.expected_res["access"])
        if res.status_code != 201:
            self.log.error("Result: {}".format(res.content))
            self.tearDown()
            self.fail("Error while creating Database Credential for the test.")
        self.log.info("Database Credential created successfully.")
        testcases = [
            # {
            #     "description": "Send call with valid path params"
            # }, 
            {
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
                       "/users/extra",
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
                "description": "Call API with non-hex userId",
                "invalid_userId": self.replace_last_character(
                    res.json()["id"], non_hex=True),
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
            user = res.json()["id"]

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.db_user_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]
            elif "invalid_clusterId" in testcase:
                cluster = testcase["invalid_clusterId"]
            elif "invalid_userId" in testcase:
                user = testcase["invalid_userId"]

            result = self.capellaAPI.cluster_ops_apis.update_database_user(
                organization, project, cluster, user, self.update_access)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.update_database_user(
                    organization, project, cluster, user, self.update_access)
            self.capellaAPI.cluster_ops_apis.db_user_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters/{}/users"
            self.validate_testcase(result, [204], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        self.log.info("Creating Database Credential for the test")
        res = self.capellaAPI.cluster_ops_apis.create_database_user(
            self.organisation_id, self.project_id, self.cluster_id,
            self.expected_res["name"], self.expected_res["password"], self.expected_res["credentialType"], self.expected_res["userRoles"])
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.create_database_user(
                self.organisation_id, self.project_id, self.cluster_id,
                self.expected_res["name"], self.expected_res["access"])
        if res.status_code != 201:
            self.log.error("Result: {}".format(res.content))
            self.tearDown()
            self.fail("Error while creating Database Credential for the test.")
        self.log.info("Database Credential created successfully.")
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner"
        ]):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = self.capellaAPI.cluster_ops_apis.update_database_user(
                self.organisation_id, self.project_id, self.cluster_id,
                res.json()["id"], copy.deepcopy(self.update_access),
                userRoles=self.update_access["userRoles"], headers=header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.update_database_user(
                    self.organisation_id, self.project_id, self.cluster_id,
                    res.json()["id"], copy.deepcopy(self.update_access),
                    userRoles=self.update_access["userRoles"], headers=header)
            self.validate_testcase(result, [204], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_payload(self):
        self.log.info("Creating Database Credential for the test")
        res = self.capellaAPI.cluster_ops_apis.create_database_user(
            self.organisation_id, self.project_id, self.cluster_id,
            self.expected_res["name"], self.expected_res["password"], self.expected_res["credentialType"], self.expected_res["userRoles"])
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.create_database_user(
                self.organisation_id, self.project_id, self.cluster_id,
                self.expected_res["name"], self.expected_res["access"])
        if res.status_code != 201:
            self.log.error("Result: {}".format(res.content))
            self.tearDown()
            self.fail("Error while creating Database Credential for the test.")
        self.log.info("Database Credential created successfully.")
        testcases = [
            {
                "desc": "Update basic credential with access - should succeed",
                "password": "Mathematics12!",
                "userRoles": None,
                "expected_status_code": 422,
                "expected_error": {
                    "code": 422,
                    "hint": "Please review your request and ensure that all required parameters are correctly provided.",
                    "httpStatusCode": 422,
                    "message": "Can not update existing dataplane user permissions unless at least (1) valid permission is supplied on the request."
                }
            }, {
                "desc": "Update basic credential with userRoles - should fail "
                        "(credential is basic type)",
                "password": None,
                "userRoles": [self.role_name],
                "expected_status_code": 422,
                "expected_error": {
                    "code": 422,
                    "hint": "Please review your request and ensure that all required parameters are correctly provided.",
                    "httpStatusCode": 422,
                    "message": "Can not update existing dataplane user permissions unless at least (1) valid permission is supplied on the request."
                }
            }, {
                "desc": "Update with both access and userRoles - should fail "
                        "(mutually exclusive)",
                "password": "Mathematics12!",
                "userRoles": [self.role_name],
                "expected_status_code": 422,
                "expected_error": {
                    "code": 422,
                    "hint": "Please review your request and ensure that all required parameters are correctly provided.",
                    "httpStatusCode": 422,
                    "message": "Can not update existing dataplane user permissions unless at least (1) valid permission is supplied on the request."
                }
            }, {
                "desc": "Update with neither access nor userRoles",
                "password": None,
                "userRoles": None,
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Please review your request and ensure that all required parameters are correctly provided.",
                    "httpStatusCode": 400,
                    "message": "Can not update existing dataplane user permissions unless at least (1) valid permission is supplied on the request."
                }
            }, {
                "desc": "Update with non-existent role in userRoles",
                "password": None,
                "userRoles": ["nonexistent-role-name"],
                "expected_status_code": 422,
                "expected_error": {
                    "code": 422,
                    "hint": "Please review your request and ensure that all required parameters are correctly provided.",
                    "httpStatusCode": 422,
                    "message": "Can not update existing dataplane user permissions unless at least (1) valid permission is supplied on the request."
                }
            }
        ]
        failures = list()
        for testcase in testcases:
            self.log.info(testcase["desc"])
            result = self.capellaAPI.cluster_ops_apis.update_database_user(
                self.organisation_id, self.project_id, self.cluster_id,
                res.json()['id'], self.update_access,
                userRoles=testcase["userRoles"])
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.update_database_user(
                    self.organisation_id, self.project_id, self.cluster_id,
                    res.json()['id'], self.update_access,
                    userRoles=testcase["userRoles"])
            self.validate_testcase(result, [204], testcase, failures,
                                   payloadTest=True)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))



class DeleteDatabaseCredential(DatabaseCredentialBase):

    def setUp(self, nomenclature="DatabaseCredentials_DELETE"):
        DatabaseCredentialBase.setUp(self, nomenclature)

    def tearDown(self):
        super(DeleteDatabaseCredential, self).tearDown()

    def test_api_path(self):
        self.log.info("Creating Database Credential for the test")
        res = self.capellaAPI.cluster_ops_apis.create_database_user(
            self.organisation_id, self.project_id, self.cluster_id,
            self.expected_res["name"], self.expected_res["password"], self.expected_res["credentialType"], self.expected_res["userRoles"])
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.create_database_user(
                self.organisation_id, self.project_id, self.cluster_id,
                self.expected_res["name"], self.expected_res["access"])
        if res.status_code != 201:
            self.log.error("Result: {}".format(res.content))
            self.tearDown()
            self.fail("Error while creating Database Credential for the test.")
        self.log.info("Database Credential created successfully.")
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
                       "/users/extra",
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
                "description": "Call API with non-hex userId",
                "invalid_userId": self.replace_last_character(
                    res.json()["id"], non_hex=True),
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
            user = res.json()["id"]

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.db_user_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]
            elif "invalid_clusterId" in testcase:
                cluster = testcase["invalid_clusterId"]
            elif "invalid_userId" in testcase:
                user = testcase["invalid_userId"]

            result = self.capellaAPI.cluster_ops_apis.delete_database_user(
                organization, project, cluster, user)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.delete_database_user(
                    organization, project, cluster, user)
            self.capellaAPI.cluster_ops_apis.db_user_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters/{}/users"
            if self.validate_testcase(result, [204], testcase, failures):
                self.log.debug("Deletion Successful, recreating credential.")
                res = self.capellaAPI.cluster_ops_apis.create_database_user(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.expected_res["name"], self.expected_res["password"], self.expected_res["credentialType"], self.expected_res["userRoles"])
                if res.status_code == 429:
                    self.handle_rate_limit(int(res.headers["Retry-After"]))
                    res = \
                        self.capellaAPI.cluster_ops_apis.create_database_user(
                            self.organisation_id, self.project_id,
                            self.cluster_id,
                            self.expected_res["name"], self.expected_res["password"], self.expected_res["credentialType"], self.expected_res["userRoles"])
                if res.status_code == 201:
                    self.user_id = res.json()["id"]

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        self.log.info("Creating Database Credential for the test")
        res = self.capellaAPI.cluster_ops_apis.create_database_user(
            self.organisation_id, self.project_id, self.cluster_id,
            self.expected_res["name"], self.expected_res["password"], self.expected_res["credentialType"], self.expected_res["userRoles"])
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.create_database_user(
                self.organisation_id, self.project_id, self.cluster_id,
                self.expected_res["name"], self.expected_res["access"])
        if res.status_code != 201:
            self.log.error("Result: {}".format(res.content))
            self.tearDown()
            self.fail("Error while creating Database Credential for the test.")
        self.log.info("Database Credential created successfully.")
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner"
        ]):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = self.capellaAPI.cluster_ops_apis.delete_database_user(
                self.organisation_id, self.project_id, self.cluster_id,
                res.json()["id"], header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.delete_database_user(
                    self.organisation_id, self.project_id, self.cluster_id,
                    res.json()["id"], header)
            if self.validate_testcase(result, [204], testcase, failures):
                self.log.debug("Deletion Successful, recreating credential.")
                res = self.capellaAPI.cluster_ops_apis.create_database_user(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.expected_res["name"], self.expected_res["password"], self.expected_res["credentialType"], self.expected_res["userRoles"])
                if res.status_code == 429:
                    self.handle_rate_limit(int(res.headers["Retry-After"]))
                    res = \
                        self.capellaAPI.cluster_ops_apis.create_database_user(
                            self.organisation_id, self.project_id,
                            self.cluster_id,
                            self.expected_res["name"], self.expected_res["password"], self.expected_res["credentialType"], self.expected_res["userRoles"])
                if res.status_code == 201:
                    self.user_id = res.json()["id"]

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))
