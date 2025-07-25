"""
Created on July 25, 2025

@author: Created using cbRAT cbFile by Vipul Bhardwaj
"""

from pytests.Capella.RestAPIv4.Users.create_users import PostUser


class ListUsers(PostUser):

    def setUp(self, nomenclature="Users_List"):
        PostUser.setUp(self, nomenclature)

        self.expected_res = {
            "cursor": {
                "hrefs": {
                    "first": "https://cloud.couchbase.com/v4/users?page=1\u0026perPage=10",
                    "last": "https://cloud.couchbase.com/v4/users?page=1\u0026perPage=10",
                    "next": "https://cloud.couchbase.com/v4/users?page=1\u0026perPage=10",
                    "previous": "https://cloud.couchbase.com/v4/users?page=1\u0026perPage=10"
                },
                "pages": {
                    "last": 10,
                    "next": 3,
                    "page": 2,
                    "perPage": 10,
                    "previous": 1,
                    "totalItems": 10
                }
            },
            "data": [
                {
                    "audit": {
                        "createdAt": "2021-09-01T12:34:56.000Z",
                        "createdBy": "ffffffff-aaaa-1414-eeee-000000000000",
                        "modifiedAt": "2021-09-01T12:34:56.000Z",
                        "modifiedBy": "ffffffff-aaaa-1414-eeee-000000000000",
                        "version": 1
                    },
                    "email": "john.doe@example.com",
                    "enableNotifications": False,
                    "expiresAt": "2023-07-17T07:05:39.116Z",
                    "id": self.user_id,
                    "inactive": False,
                    "lastLogin": "2023-07-17T07:05:39.116Z",
                    "name": "John",
                    "organizationId": self.organisation_id,
                    "organizationRoles": [
                        "organizationOwner"
                    ],
                    "region": "North America",
                    "status": "verified",
                    "timeZone": "(UTC +5:30) India Standard Time"
                }
            ]
        }

    def tearDown(self):
        self.update_auth_with_api_token(self.org_owner_key["token"])
        super(ListUsers, self).tearDown()

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

            result = self.capellaAPI.cluster_ops_apis.list_users(
                organization)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.list_users(
                    organization)
            self.capellaAPI.cluster_ops_apis.user_endpoint = \
                "/v4/organizations/{}/users"
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
             "organizationOwner", "organizationMember", "projectCreator"
        ]):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = self.capellaAPI.cluster_ops_apis.list_users(
                self.organisation_id,
                header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.list_users(
                    self.organisation_id,
                    header)
            self.validate_testcase(result, [200], testcase, failures)
        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))
