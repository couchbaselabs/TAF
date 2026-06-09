"""
Created on May 05, 2026

@author: Automation
"""

from pytests.Capella.RestAPIv4.api_base import APIBase


class GetProjectCMEKAzureApplication(APIBase):

    def setUp(self):
        APIBase.setUp(self)

    def tearDown(self):
        self.update_auth_with_api_token(self.curr_owner_key)
        super(GetProjectCMEKAzureApplication, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Fetch project Azure CMEK application with valid path"
            }, {
                "description": "Replace API version in URI",
                "url": "/v3/organizations/{}/projects/{}/cmekAzureApplication",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }, {
                "description": "Replace cmekAzureApplication with cmekApplication in URI",
                "url": "/v4/organizations/{}/projects/{}/cmekApplication",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/cmekAzureApplication/app",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Call API with non-hex organizationId",
                "invalid_organizationId": self.replace_last_character(
                    self.organisation_id, non_hex=True),
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }, {
                "description": "Call API with non-hex projectId",
                "invalid_projectId": self.replace_last_character(
                    self.project_id, non_hex=True),
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
                self.capellaAPI.org_ops_apis.project_cmek_azure_application_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]

            result = self.capellaAPI.org_ops_apis.fetch_project_cmek_azure_application(
                organization, project)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.fetch_project_cmek_azure_application(
                    organization, project)
            self.capellaAPI.org_ops_apis.project_cmek_azure_application_endpoint = \
                "/v4/organizations/{}/projects/{}/cmekAzureApplication"
            # 200 -> Azure CMEK enabled; 404 -> Azure Entra ID application not
            # provisioned (CMEK not enabled for this project). Both are valid
            # post-authorization responses; an unauthorized caller gets 403.
            self.validate_testcase(result, [200, 404], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner", "projectManager"
        ], None):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header, self.project_id)
            result = self.capellaAPI.org_ops_apis.fetch_project_cmek_azure_application(
                self.organisation_id, self.project_id, headers=header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.fetch_project_cmek_azure_application(
                    self.organisation_id, self.project_id, headers=header)
            # 200 -> Azure CMEK enabled; 404 -> Azure Entra ID application not
            # provisioned (CMEK not enabled for this project). Both are valid
            # post-authorization responses; an unauthorized caller gets 403.
            self.validate_testcase(result, [200, 404], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))
