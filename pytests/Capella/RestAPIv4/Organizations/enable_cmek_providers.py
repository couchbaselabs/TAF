"""
Created on May 05, 2026

@author: Automation
"""

from pytests.Capella.RestAPIv4.api_base import APIBase


class EnableCMEKProvider(APIBase):

    def setUp(self):
        APIBase.setUp(self)

    def tearDown(self):
        self.update_auth_with_api_token(self.curr_owner_key)
        super(EnableCMEKProvider, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Enable CMEK provider with valid path",
                "cloudProvider": "aws",
                "expected_status_code": [204, 422]
            }, {
                "description": "Replace API version in URI",
                "url": "/v3/organizations/{}/cmek/providers",
                "cloudProvider": "aws",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }, {
                "description": "Replace providers with provider in URI",
                "url": "/v4/organizations/{}/cmek/provider",
                "cloudProvider": "aws",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/cmek/providers/provider",
                "cloudProvider": "aws",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Call API with non-hex organizationId",
                "invalid_organizationId": self.replace_last_character(
                    self.organisation_id, non_hex=True),
                "cloudProvider": "aws",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }
        ]

        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            organization = self.organisation_id

            if "url" in testcase:
                self.capellaAPI.org_ops_apis.cmek_providers_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]

            result = self.capellaAPI.org_ops_apis.enable_cmek_provider(
                organization, testcase["cloudProvider"])
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.enable_cmek_provider(
                    organization, testcase["cloudProvider"])
            self.capellaAPI.org_ops_apis.cmek_providers_endpoint = \
                "/v4/organizations/{}/cmek/providers"
            self.validate_testcase(result, [204], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init(["organizationOwner"], None):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header, self.project_id)
            result = self.capellaAPI.org_ops_apis.enable_cmek_provider(
                self.organisation_id, "aws", headers=header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.enable_cmek_provider(
                    self.organisation_id, "aws", headers=header)
            self.validate_testcase(result, [204], testcase, failures)

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
                "organizationID": val,
                "cloudProvider": "aws"
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
            result = self.capellaAPI.org_ops_apis.enable_cmek_provider(
                testcase["organizationID"], testcase["cloudProvider"])
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.enable_cmek_provider(
                    testcase["organizationID"], testcase["cloudProvider"])
            self.validate_testcase(result, [204], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_payload(self):
        provider_values = [
            "aws",
            "gcp",
            "azure",
            "",
            "invalid_provider",
            True,
            123,
            123.45,
            [],
            {},
            None
        ]

        testcases = list()
        for value in provider_values:
            testcase = {
                "desc": "cloudProvider: {}".format(str(value)),
                "cloudProvider": value
            }
            if value in ["aws", "gcp", "azure"]:
                testcase["expected_status_code"] = [204, 422]
            else:
                testcase["expected_status_code"] = [422],
                testcase["expected_error"] = {
                        "code": 1000,
                        "hint": "Please review your request and ensure that all required parameters are correctly provided.",
                        "httpStatusCode": 422,
                        "message": "Unable to process request. The specified provider is not supported for CMEK. Please ensure the provider is either AWS, GCP, or Azure and try again."
                    }
            testcases.append(testcase)

        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["desc"]))
            result = self.capellaAPI.org_ops_apis.enable_cmek_provider(
                self.organisation_id, testcase["cloudProvider"])
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.enable_cmek_provider(
                    self.organisation_id, testcase["cloudProvider"])
            self.validate_testcase(result, [204], testcase, failures,
                                   payloadTest=True)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))
