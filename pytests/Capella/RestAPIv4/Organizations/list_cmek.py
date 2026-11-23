"""
Created on May 05, 2026

@author: Automation
"""

from pytests.Capella.RestAPIv4.api_base import APIBase


class ListCMEK(APIBase):

    def setUp(self):
        APIBase.setUp(self)

    def tearDown(self):
        self.update_auth_with_api_token(self.curr_owner_key)
        super(ListCMEK, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Fetch CMEK metadata with valid organization ID"
            }, {
                "description": "Replace API version in URI",
                "url": "/v3/organizations/{}/cmek",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }, {
                "description": "Replace cmek with cmeks in URI",
                "url": "/v4/organizations/{}/cmeks",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/cmek/key",
                # The extra segment is taken as a resource ID, which isn't a UUID (19750).
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if you have provided a valid URL and all the required params are present in the request body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the request due to something that is perceived to be a client error."
                }
            }, {
                "description": "Call API with non-hex organizationId",
                "invalid_organizationId": self.replace_last_character(
                    self.organisation_id, non_hex=True),
                # Non-UUID IDs are rejected with 400 before the route is looked up (19750).
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if you have provided a valid URL and all the required params are present in the request body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the request due to something that is perceived to be a client error."
                }
            }
        ]

        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            organization = self.organisation_id

            if "url" in testcase:
                self.capellaAPI.org_ops_apis.cmek_endpoint = testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]

            result = self.capellaAPI.org_ops_apis.list_cmek_metadata(organization)
            self.capellaAPI.org_ops_apis.cmek_endpoint = \
                "/v4/organizations/{}/cmek"
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        # Every key in the org can list CMEKs, projectCreator included
        # (19683: all 36 role keys got 200).
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "organizationMember", "projectCreator"
        ], None):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header, self.project_id)
            result = self.capellaAPI.org_ops_apis.list_cmek_metadata(
                self.organisation_id, headers=header)
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_query_parameters(self):
        organization_id_values = [
            self.organisation_id,
            self.replace_last_character(self.organisation_id),
            True,
            123456789,
            123456789.123456789,
            "",
            (self.organisation_id,),
            [self.organisation_id],
            {self.organisation_id},
            None
        ]

        testcases = list()
        for val in organization_id_values:
            testcase = {
                "description": "Organization ID : {}".format(str(val)),
                "organizationID": val
            }
            if val == "":
                testcase["expected_status_code"] = 404
            elif type(val) in [int, bool, float, list, tuple, set, type(None)]:
                testcase["expected_status_code"] = 400
            elif val != self.organisation_id:
                testcase["expected_status_code"] = 403
            testcases.append(testcase)

        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            result = self.capellaAPI.org_ops_apis.list_cmek_metadata(
                testcase["organizationID"])
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[self.capellaAPI.org_ops_apis.list_cmek_metadata,
                          (self.organisation_id,)]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[self.capellaAPI.org_ops_apis.list_cmek_metadata,
                          (self.organisation_id,)]]
        self.throttle_test(api_func_list, True, self.project_id)
