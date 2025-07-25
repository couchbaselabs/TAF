"""
Created on July 25, 2025

@author: Created using cbRAT cbFile by Vipul Bhardwaj
"""

from pytests.Capella.RestAPIv4.Users.create_users import PostUser


class DeleteUsers(PostUser):

    def setUp(self, nomenclature="Users_Delete"):
        PostUser.setUp(self, nomenclature)

    def tearDown(self):
        super(DeleteUsers, self).tearDown()

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
                "description": "Call API with non-hex userId",
                "invalid_userId": self.replace_last_character(
                    self.user_id, non_hex=True),
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
            user = self.user_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.user_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_userId" in testcase:
                user = testcase["invalid_userId"]

            result = self.capellaAPI.cluster_ops_apis.delete_user(
                organization, user)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.delete_user(
                    organization, user)
            self.capellaAPI.cluster_ops_apis.user_endpoint = \
                "/v4/organizations/{}/users"
            if self.validate_testcase(result, [204], testcase, failures):
                self.log.debug("Deletion Successful")
                self.user_id = self.create_user_to_be_tested(
                    self.expected_res["resources"],
                    self.expected_res["email"], self.expected_res["name"], 
                    self.expected_res["organizationRoles"])

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
            result = self.capellaAPI.cluster_ops_apis.delete_user(
                self.organisation_id, self.user_id,
                header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.delete_user(
                    self.organisation_id, self.user_id,
                    header)
            if self.validate_testcase(result, [204], testcase, failures):
                self.log.debug("Deletion Successful")
                self.user_id = self.create_user_to_be_tested(
                    self.expected_res["resources"],
                    self.expected_res["email"], self.expected_res["name"], 
                    self.expected_res["organizationRoles"])
        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))
