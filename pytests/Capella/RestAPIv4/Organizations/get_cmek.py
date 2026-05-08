"""
Created on May 05, 2026

@author: Automation
"""

from pytests.Capella.RestAPIv4.api_base import APIBase


class GetCMEK(APIBase):

    def setUp(self):
        super(GetCMEK, self).setUp()
        self.cmek_id = self.invalid_UUID
        self.cmek_created = False
        self.cmek_arn = self.input.param("cmek_arn", "")
        if self.cmek_arn:
            resp = self.capellaAPI.org_ops_apis.create_cmek_metadata(
                self.organisation_id, self.prefix + "cmek-get",
                {"arn": self.cmek_arn},
                "CMEK metadata for get tests")
            if resp.status_code == 429:
                self.handle_rate_limit(int(resp.headers["Retry-After"]))
                resp = self.capellaAPI.org_ops_apis.create_cmek_metadata(
                    self.organisation_id, self.prefix + "cmek-get",
                    {"arn": self.cmek_arn},
                    "CMEK metadata for get tests")
            if resp.status_code == 200:
                self.cmek_id = resp.json()["id"]
                self.cmek_created = True

    def tearDown(self):
        if self.cmek_created:
            resp = self.capellaAPI.org_ops_apis.delete_cmek_metadata(
                self.organisation_id, self.cmek_id)
            if resp.status_code == 429:
                self.handle_rate_limit(int(resp.headers["Retry-After"]))
                resp = self.capellaAPI.org_ops_apis.delete_cmek_metadata(
                    self.organisation_id, self.cmek_id)
            if resp.status_code not in [204, 404]:
                self.log.error("CMEK cleanup failed: {}".format(resp.content))
        self.update_auth_with_api_token(self.curr_owner_key)
        super(GetCMEK, self).tearDown()

    def test_api_path(self):
        testcases = [
            # {
            #     "description": "Fetch CMEK metadata with valid path"
            # },
             {
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
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Call API with non-hex organizationId",
                "invalid_organizationId": self.replace_last_character(
                    self.organisation_id, non_hex=True),
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }
        ]

        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            organization = self.organisation_id
            cmek_id = self.cmek_id

            if "url" in testcase:
                self.capellaAPI.org_ops_apis.cmek_endpoint = testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]

            result = self.capellaAPI.org_ops_apis.fetch_cmek_metadata(
                organization, cmek_id)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.fetch_cmek_metadata(
                    organization, cmek_id)
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
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "organizationMember"
        ], None):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header, self.project_id)
            result = self.capellaAPI.org_ops_apis.fetch_cmek_metadata(
                self.organisation_id, self.cmek_id, headers=header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.fetch_cmek_metadata(
                    self.organisation_id, self.cmek_id, headers=header)
            self.validate_testcase(result, [200 if self.cmek_created else 404],
                                   testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_query_parameters(self):
        cmek_id_values = [
            self.cmek_id,
            self.replace_last_character(self.cmek_id),
            True,
            123456789,
            123456789.123456789,
            "",
            (self.cmek_id,),
            [self.cmek_id],
            {self.cmek_id},
            None
        ]

        testcases = list()
        for val in cmek_id_values:
            testcase = {
                "description": "CMEK ID : {}".format(str(val)),
                "cmekID": val
            }
            if val == "":
                testcase["expected_status_code"] = 404
            elif type(val) in [int, bool, float, list, tuple, set, type(None)]:
                testcase["expected_status_code"] = 400
            elif val != self.cmek_id:
                testcase["expected_status_code"] = 404
            elif not self.cmek_created:
                testcase["expected_status_code"] = 404
            testcases.append(testcase)

        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            result = self.capellaAPI.org_ops_apis.fetch_cmek_metadata(
                self.organisation_id, testcase["cmekID"])
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.fetch_cmek_metadata(
                    self.organisation_id, testcase["cmekID"])
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))
