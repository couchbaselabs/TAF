"""
Created on May 05, 2026

@author: Automation
"""

from pytests.Capella.RestAPIv4.api_base import APIBase


class ProjectCMEKById(APIBase):

    def setUp(self):
        APIBase.setUp(self)
        self.cmek_id = self.invalid_UUID
        self.cmek_created = False
        self.key_location = self.input.param("project_cmek_key_location", "")
        self.region = self.input.param("project_cmek_region", "")
        self.rotate_key_location = self.input.param(
            "project_cmek_rotate_key_location", self.key_location)
        self.rotate_region = self.input.param(
            "project_cmek_rotate_region", self.region)
        if self.key_location and self.region:
            resp = self.capellaAPI.org_ops_apis.create_project_cmek_metadata(
                self.organisation_id, self.project_id,
                self.prefix + "project-cmek-by-id",
                {
                    "keyLocation": self.key_location,
                    "region": self.region
                },
                "Project CMEK metadata for by-id tests")
            if resp.status_code == 429:
                self.handle_rate_limit(int(resp.headers["Retry-After"]))
                resp = self.capellaAPI.org_ops_apis.create_project_cmek_metadata(
                    self.organisation_id, self.project_id,
                    self.prefix + "project-cmek-by-id",
                    {
                        "keyLocation": self.key_location,
                        "region": self.region
                    },
                    "Project CMEK metadata for by-id tests")
            if resp.status_code == 200:
                self.cmek_id = resp.json()["id"]
                self.cmek_created = True

    def tearDown(self):
        if self.cmek_created:
            resp = self.capellaAPI.org_ops_apis.delete_project_cmek_metadata(
                self.organisation_id, self.project_id, self.cmek_id)
            if resp.status_code == 429:
                self.handle_rate_limit(int(resp.headers["Retry-After"]))
                resp = self.capellaAPI.org_ops_apis.delete_project_cmek_metadata(
                    self.organisation_id, self.project_id, self.cmek_id)
            if resp.status_code not in [204, 404]:
                self.log.error("Project CMEK cleanup failed: {}"
                               .format(resp.content))
        self.update_auth_with_api_token(self.curr_owner_key)
        super(ProjectCMEKById, self).tearDown()

    def test_get_api_path(self):
        testcases = [
            # {
            #     "description": "Fetch project CMEK metadata with valid path",
            #     "expected_status_code": 200 if self.cmek_created else 404
            # }, 
            {
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

            result = self.capellaAPI.org_ops_apis.fetch_project_cmek_metadata(
                organization, project, self.cmek_id)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.fetch_project_cmek_metadata(
                    organization, project, self.cmek_id)
            self.capellaAPI.org_ops_apis.project_cmek_endpoint = \
                "/v4/organizations/{}/projects/{}/cmek"
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_put_api_path(self):
        testcases = [
            # {
            #     "description": "Update project CMEK metadata with valid path",
            #     "expected_status_code": [204, 412, 422] if self.cmek_created
            #     else 404
            # },
            {
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

            result = self.capellaAPI.org_ops_apis.update_project_cmek_metadata(
                organization, project, self.cmek_id,
                {
                    "keyLocation": self.rotate_key_location,
                    "region": self.rotate_region
                })
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.update_project_cmek_metadata(
                    organization, project, self.cmek_id,
                    {
                        "keyLocation": self.rotate_key_location,
                        "region": self.rotate_region
                    })
            self.capellaAPI.org_ops_apis.project_cmek_endpoint = \
                "/v4/organizations/{}/projects/{}/cmek"
            self.validate_testcase(result, [204], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_delete_api_path(self):
        testcases = [
            # {
            #     "description": "Delete project CMEK metadata with valid path",
            #     "expected_status_code": 204 if self.cmek_created else 404
            # }, 
            {
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

            result = self.capellaAPI.org_ops_apis.delete_project_cmek_metadata(
                organization, project, self.cmek_id)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.delete_project_cmek_metadata(
                    organization, project, self.cmek_id)
            self.capellaAPI.org_ops_apis.project_cmek_endpoint = \
                "/v4/organizations/{}/projects/{}/cmek"
            self.validate_testcase(result, [204], testcase, failures)
            if result.status_code == 204 and \
                    organization == self.organisation_id and \
                    project == self.project_id:
                self.cmek_created = False

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))
