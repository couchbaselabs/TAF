"""
Created on May 05, 2026

@author: Automation
"""

from pytests.Capella.RestAPIv4.api_base import APIBase


class ProjectCMEK(APIBase):

    def setUp(self):
        APIBase.setUp(self)

    def tearDown(self):
        self.update_auth_with_api_token(self.curr_owner_key)
        super(ProjectCMEK, self).tearDown()

    def test_get_api_path(self):
        testcases = [
            {
                "description": "List project CMEK metadata with valid path"
            }, {
                "description": "Replace API version in URI",
                "url": "/v3/organizations/{}/projects/{}/cmek",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }, {
                "description": "Replace cmek with cmeks in URI",
                "url": "/v4/organizations/{}/projects/{}/cmeks",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/cmek/key",
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
                self.capellaAPI.org_ops_apis.project_cmek_endpoint = testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]

            result = self.capellaAPI.org_ops_apis.list_project_cmek_metadata(
                organization, project)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.list_project_cmek_metadata(
                    organization, project)
            self.capellaAPI.org_ops_apis.project_cmek_endpoint = \
                "/v4/organizations/{}/projects/{}/cmek"
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_post_api_path(self):
        key_location = self.input.param("project_cmek_key_location", "")
        region = self.input.param("project_cmek_region", "")
        payload = {
            "name": self.prefix + "project-cmek",
            "description": "Project CMEK metadata create path test",
            "config": {
                "keyLocation": key_location,
                "region": region
            }
        }
        testcases = [
            {
                "description": "Create project CMEK metadata using valid path",
                "expected_status_code": [200, 400, 404, 422]
            }, {
                "description": "Replace API version in URI",
                "url": "/v3/organizations/{}/projects/{}/cmek",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }, {
                "description": "Replace cmek with cmeks in URI",
                "url": "/v4/organizations/{}/projects/{}/cmeks",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/cmek/key",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Create with non-hex organizationId",
                "invalid_organizationId": self.replace_last_character(
                    self.organisation_id, non_hex=True),
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }, {
                "description": "Create with non-hex projectId",
                "invalid_projectId": self.replace_last_character(
                    self.project_id, non_hex=True),
                "expected_status_code": 400
            }
        ]

        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            organization = self.organisation_id
            project = self.project_id

            if "url" in testcase:
                self.capellaAPI.org_ops_apis.project_cmek_endpoint = testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]

            result = self.capellaAPI.org_ops_apis.create_project_cmek_metadata(
                organization, project, payload["name"], payload["config"],
                payload["description"])
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.create_project_cmek_metadata(
                    organization, project, payload["name"], payload["config"],
                    payload["description"])
            self.capellaAPI.org_ops_apis.project_cmek_endpoint = \
                "/v4/organizations/{}/projects/{}/cmek"
            self.validate_testcase(result, [200, 400, 404, 422], testcase,
                                   failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))
