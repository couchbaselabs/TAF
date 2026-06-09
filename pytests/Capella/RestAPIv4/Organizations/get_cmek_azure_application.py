"""
Created on May 05, 2026

@author: Automation
"""

from pytests.Capella.RestAPIv4.api_base import APIBase


class GetCMEKAzureApplication(APIBase):

    def setUp(self):
        super(GetCMEKAzureApplication, self).setUp()
        # The GET cmekAzureApplication endpoint 404s until Azure CMEK is
        # enabled for the tenant (registers the Azure Entra ID application).
        self.enable_azure_cmek()

    def tearDown(self):
        self.update_auth_with_api_token(self.curr_owner_key)
        super(GetCMEKAzureApplication, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Fetch Azure CMEK application with valid path"
            }, {
                "description": "Replace API version in URI",
                "url": "/v3/organizations/{}/cmekAzureApplication",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }, {
                "description": "Replace cmekAzureApplication with cmekApplication in URI",
                "url": "/v4/organizations/{}/cmekApplication",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/cmekAzureApplication/app",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }
        ]

        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            organization = self.organisation_id

            if "url" in testcase:
                self.capellaAPI.org_ops_apis.cmek_azure_application_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]

            result = self.capellaAPI.org_ops_apis.fetch_cmek_azure_application(
                organization)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.fetch_cmek_azure_application(
                    organization)
            self.capellaAPI.org_ops_apis.cmek_azure_application_endpoint = \
                "/v4/organizations/{}/cmekAzureApplication"
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "organizationMember"
        ], None):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header, self.project_id)
            result = self.capellaAPI.org_ops_apis.fetch_cmek_azure_application(
                self.organisation_id, headers=header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.fetch_cmek_azure_application(
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
            result = self.capellaAPI.org_ops_apis.fetch_cmek_azure_application(
                testcase["organizationID"])
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.fetch_cmek_azure_application(
                    testcase["organizationID"])
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))
