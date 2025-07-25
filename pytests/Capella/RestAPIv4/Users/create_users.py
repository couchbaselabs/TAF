"""
Created on July 24, 2025

@author: Created using cbRAT cbFile by Vipul Bhardwaj
"""

from pytests.Capella.RestAPIv4.api_base import APIBase


class PostUser(APIBase):

    def setUp(self, nomenclature="Users_Post"):
        APIBase.setUp(self, nomenclature)

        self.expected_res = {
            "audit": {
                "createdAt": "2025-07-24T09:07:01.331133631Z",
                "createdBy": "c60b8780-1cc1-4faa-a5dd-27b95fd68083",
                "modifiedAt": "2025-07-24T09:07:01.331133631Z",
                "modifiedBy": "c60b8780-1cc1-4faa-a5dd-27b95fd68083",
                "version": 1
            },
            "email": "john.doe@couchbase.com",
            "expiresAt": "2025-10-22T09:07:01.331134613Z",
            "id": "",
            "inactive": True,
            "name": "John",
            "organizationId": self.organisation_id,
            "organizationRoles": [
                "organizationOwner"
            ],
            "resources": [],
            "status": "not-verified"
        }
        
        self.log.info("Creating a User for future use in other tests like GET, DELETE, etc.")
        res = self.capellaAPI.cluster_ops_apis.create_user(
            self.organisation_id, self.expected_res["resources"],
            self.expected_res["email"], self.expected_res["name"], 
            self.expected_res["organizationRoles"])
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.create_user(
                self.organisation_id, self.expected_res["resources"],
                self.expected_res["email"], self.expected_res["name"], 
                self.expected_res["organizationRoles"])
        if res.status_code != 201:
            self.log.error("Result: {}".format(res.content))
            self.tearDown()
            self.fail("Error while creating User.")

        self.log.info("User created successfully.")
        self.user_id = res.json()["id"]
        self.expected_res["id"] = self.user_id

    def tearDown(self):
        self.update_auth_with_api_token(self.org_owner_key["token"])
        super(PostUser, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Send call with valid path params"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/users",
                "expected_status_code": 404,
                "expected_error": {
                    "errorType": "RouteNotFound",
                    "message": "Not found"
                }
            }, {
                "description": "Replace the last path param name in URI",
                "url": "/v4/organizations/{}/user",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/users/user",
                "expected_status_code": 405,
                "expected_error": ""
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
            }
        ]
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            organization = self.organisation_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.user_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]

            result = self.capellaAPI.cluster_ops_apis.create_user(
                organization, self.expected_res["resources"],
                self.expected_res["email"], self.expected_res["name"], 
                self.expected_res["organizationRoles"])
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.create_user(
                    organization, self.expected_res["resources"],
                    self.expected_res["email"], self.expected_res["name"], 
                    self.expected_res["organizationRoles"])
            self.capellaAPI.cluster_ops_apis.user_endpoint = \
                "/v4/organizations/{}/users"
            if self.validate_testcase(result, [201], testcase, failures):
                self.log.debug("Creation Successful")
                self.delete_user(result.json()["id"])

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
             "organizationOwner"
        ]):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = self.capellaAPI.cluster_ops_apis.create_user(
                self.organisation_id, self.expected_res["resources"],
                self.expected_res["email"], self.expected_res["name"],
                self.expected_res["organizationRoles"], header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.create_user(
                    self.organisation_id, self.expected_res["resources"],
                    self.expected_res["email"], self.expected_res["name"],
                    self.expected_res["organizationRoles"], header)
            if self.validate_testcase(result, [201], testcase, failures):
                self.log.debug("Creation Successful")
                self.delete_user(result.json()["id"])
        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))
