"""
Created on May 05, 2026

@author: Automation
"""

from pytests.Capella.RestAPIv4.api_base import APIBase


class EnableProjectCMEKProvider(APIBase):

    def setUp(self):
        super(EnableProjectCMEKProvider, self).setUp()

    def tearDown(self):
        self.update_auth_with_api_token(self.curr_owner_key)
        super(EnableProjectCMEKProvider, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Enable project CMEK provider with valid path",
                "cloudProvider": "azure",
                "expected_status_code": [204, 422]
            }, {
                "description": "Replace API version in URI",
                "url": "/v3/organizations/{}/projects/{}/cmek/providers",
                "cloudProvider": "azure",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }, {
                "description": "Replace providers with provider in URI",
                "url": "/v4/organizations/{}/projects/{}/cmek/provider",
                "cloudProvider": "azure",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/cmek/providers/provider",
                "cloudProvider": "azure",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Call API with non-hex organizationId",
                "invalid_organizationId": self.replace_last_character(
                    self.organisation_id, non_hex=True),
                "cloudProvider": "azure",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }, {
                "description": "Call API with non-hex projectId",
                "invalid_projectId": self.replace_last_character(
                    self.project_id, non_hex=True),
                "cloudProvider": "azure",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }
        ]

        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            organization = self.organisation_id
            project = self.project_id

            if "url" in testcase:
                self.capellaAPI.org_ops_apis.project_cmek_providers_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]

            result = self.capellaAPI.org_ops_apis.enable_project_cmek_provider(
                organization, project, testcase["cloudProvider"])
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.enable_project_cmek_provider(
                    organization, project, testcase["cloudProvider"])
            self.capellaAPI.org_ops_apis.project_cmek_providers_endpoint = \
                "/v4/organizations/{}/projects/{}/cmek/providers"
            self.validate_testcase(result, [204], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner", "projectManager",
            "projectViewer", "projectDataReader", "projectDataReaderWriter"
        ], None):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header, self.project_id)
            result = self.capellaAPI.org_ops_apis.enable_project_cmek_provider(
                self.organisation_id, self.project_id, "azure", headers=header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.enable_project_cmek_provider(
                    self.organisation_id, self.project_id, "azure",
                    headers=header)
            self.validate_testcase(result, [204], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_payload(self):
        provider_values = [
            # "azure",
            "aws",
            "gcp",
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
            if value == "azure":
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
            result = self.capellaAPI.org_ops_apis.enable_project_cmek_provider(
                self.organisation_id, self.project_id,
                testcase["cloudProvider"])
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.enable_project_cmek_provider(
                    self.organisation_id, self.project_id,
                    testcase["cloudProvider"])
            self.validate_testcase(result, [204], testcase, failures,
                                   payloadTest=True)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))
