"""
Created on May 05, 2026

@author: Automation
"""

from pytests.Capella.RestAPIv4.Organizations.get_cmek import GetCMEK


class ListCMEKHistory(GetCMEK):

    def test_api_path(self):
        testcases = [
            # {
            #     "description": "List CMEK rotation history with valid path",
            #     "expected_status_code": 200 if self.cmek_created else 404
            # }, 
            {
                "description": "Replace API version in URI",
                "url": "/v3/organizations/{}/cmek/{}/history",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }, {
                "description": "Replace history with histories in URI",
                "url": "/v4/organizations/{}/cmek/{}/histories",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/cmek/{}/history/key",
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

            if "url" in testcase:
                self.capellaAPI.org_ops_apis.cmek_history_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]

            result = self.capellaAPI.org_ops_apis.list_cmek_history(
                organization, self.cmek_id)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.list_cmek_history(
                    organization, self.cmek_id)
            self.capellaAPI.org_ops_apis.cmek_history_endpoint = \
                "/v4/organizations/{}/cmek/{}/history"
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
            result = self.capellaAPI.org_ops_apis.list_cmek_history(
                self.organisation_id, self.cmek_id, headers=header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.list_cmek_history(
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
            result = self.capellaAPI.org_ops_apis.list_cmek_history(
                self.organisation_id, testcase["cmekID"])
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.list_cmek_history(
                    self.organisation_id, testcase["cmekID"])
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))
