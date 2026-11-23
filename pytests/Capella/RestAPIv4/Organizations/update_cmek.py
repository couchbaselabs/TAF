"""
Created on May 05, 2026

@author: Automation
"""

from pytests.Capella.RestAPIv4.Organizations.get_cmek import GetCMEK


class UpdateCMEK(GetCMEK):

    def setUp(self):
        GetCMEK.setUp(self)
        self.rotate_arn = self.input.param("cmek_rotate_arn", self.cmek_arn)

    def test_api_path(self):
        testcases = [
            # {
            #     "description": "Rotate CMEK metadata with valid path",
            #     "expected_status_code": [204, 412, 422] if self.cmek_created
            #     else 404
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

            result = self.capellaAPI.org_ops_apis.rotate_cmek_metadata(
                organization, self.cmek_id, {"arn": self.rotate_arn})
            self.capellaAPI.org_ops_apis.cmek_endpoint = \
                "/v4/organizations/{}/cmek"
            self.validate_testcase(result, [204], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        # Rotating an org-level CMEK is organizationOwner only, like create
        # and delete; every other role (project roles included) gets 403
        # (19839).
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner"
        ], None):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            if not self.cmek_created:
                # No CMEK to look up: the backend returns 404 to every valid
                # key before checking roles (19683), so RBAC can't be
                # verified - only the 401 cases keep their expectation.
                self.expect_same_response_for_all_keys([testcase])
            self.auth_test_setup(testcase, failures, header, self.project_id)
            result = self.capellaAPI.org_ops_apis.rotate_cmek_metadata(
                self.organisation_id, self.cmek_id, {"arn": self.rotate_arn},
                headers=header)
            # When a real cmek_arn is supplied the CMEK exists and rotate
            # returns 204/412/422; otherwise there is no key to rotate and the
            # authorized roles get 404 (mirrors GetCMEK.test_authorization and
            # this class's commented test_api_path/test_payload valid cases).
            # Unauthorized roles are still denied with 403.
            self.validate_testcase(
                result,
                [204, 412, 422] if self.cmek_created else [404],
                testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_payload(self):
        payloads = [
            # {
            #     "desc": "Valid payload",
            #     "config": {"arn": self.rotate_arn},
            #     "expected_status_code": [204, 412, 422] if self.cmek_created
            #     else 404
            # },
             {
                "desc": "Missing config field",
                "config": {},
                "expected_status_code": 422,
                "expected_error":{
                        "code": 12003,
                        "hint": "Please ensure that the configuration for the key includes metadata for either the AWS, GCP, or Azure key, and then try again.",
                        "httpStatusCode": 422,
                        "message": "Unable to process request for key. The configuration for the key must provide the metadata for the AWS, GCP, or Azure key. None was provided."
                    }
            }, {
                # A config that is not an object cannot be decoded into any
                # provider config (ErrParsingCMEKConfig in
                # cp-open-api/server/v4/cmek.go).
                "desc": "Invalid config type",
                "config": "invalid",
                "expected_status_code": 400,
                "expected_error": {
                    "code": 400,
                    "hint": "Please review your request and ensure that all "
                            "required parameters are correctly provided.",
                    "httpStatusCode": 400,
                    "message": "Unable to process request for key. The "
                               "config provided is invalid"
                }
            }
        ]

        failures = list()
        for testcase in payloads:
            self.log.info("Executing test: {}".format(testcase["desc"]))
            if not self.cmek_created:
                # No CMEK to rotate: the key lookup fails before the payload
                # is validated (19750).
                testcase["expected_status_code"] = 404
                testcase["expected_error"] = {
                    "code": 404,
                    "hint": "Please review your request and ensure that all "
                            "required parameters are correctly provided.",
                    "httpStatusCode": 404,
                    "message": "Unable to process request for key. The key "
                               "requested does not exist. Please ensure the "
                               "ID of the CMEK is correct and try again."
                }
            result = self.capellaAPI.org_ops_apis.rotate_cmek_metadata(
                self.organisation_id, self.cmek_id, testcase["config"])
            self.validate_testcase(result, [204], testcase, failures,
                                   payloadTest=True)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(payloads)))
