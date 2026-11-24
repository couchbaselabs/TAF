"""
Created on May 05, 2026

@author: Automation
"""

from pytests.Capella.RestAPIv4.Organizations.list_cmek import ListCMEK


class CreateCMEK(ListCMEK):

    def setUp(self):
        ListCMEK.setUp(self)
        self.created_cmek_id = None

    def tearDown(self):
        self.delete_created_cmek()
        super(CreateCMEK, self).tearDown()

    def delete_created_cmek(self):
        """Delete the CMEK metadata created by the last call, if any.

        test_authorization leaves the API client authenticated with whichever
        role key it tried last (possibly a revoked/expired key), so switch
        back to the owner key first; otherwise the delete gets 401 and the
        key leaks (19839).
        """
        if not self.created_cmek_id:
            return
        self.update_auth_with_api_token(self.curr_owner_key)
        result = self.capellaAPI.org_ops_apis.delete_cmek_metadata(
            self.organisation_id, self.created_cmek_id)
        if result.status_code == 429:
            self.handle_rate_limit(int(result.headers["Retry-After"]))
            result = self.capellaAPI.org_ops_apis.delete_cmek_metadata(
                self.organisation_id, self.created_cmek_id)
        if result.status_code not in [204, 404]:
            self.log.error("Cleanup failed for CMEK key {}: {}".format(
                self.created_cmek_id, result.content))
        self.created_cmek_id = None

    def test_api_path(self):
        cmek_arn = self._get_cmek_arn()
        payload = {
            "name": self.prefix + "cmek",
            "description": "CMEK metadata create path test",
            "config": {
                "arn": cmek_arn
            }
        }
        testcases = [
            # {
            #     "description": "Create CMEK metadata using valid path",
            #     "expected_status_code": [200, 422]
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
                # The extra segment matches the get-by-ID route, which doesn't allow POST (19750).
                "expected_status_code": 405,
                "expected_error": ""
            }, {
                "description": "Create with non-hex organizationId",
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

            result = self.capellaAPI.org_ops_apis.create_cmek_metadata(
                organization, payload["name"], payload["config"],
                payload["description"])
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.create_cmek_metadata(
                    organization, payload["name"], payload["config"],
                    payload["description"])
            self.capellaAPI.org_ops_apis.cmek_endpoint = \
                "/v4/organizations/{}/cmek"

            self.validate_testcase(result, [200], testcase, failures)
            if result.status_code == 200 and organization == self.organisation_id:
                self.created_cmek_id = result.json()["id"]
                result = self.capellaAPI.org_ops_apis.delete_cmek_metadata(
                    self.organisation_id, self.created_cmek_id)
                if result.status_code == 429:
                    self.handle_rate_limit(int(result.headers["Retry-After"]))
                    result = self.capellaAPI.org_ops_apis.delete_cmek_metadata(
                        self.organisation_id, self.created_cmek_id)
                if result.status_code != 204:
                    self.fail("CMEK cleanup failed with {}".format(result.content))
                self.created_cmek_id = None

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        cmek_arn = self._get_cmek_arn()
        payload = {
            "name": self.prefix + "cmek",
            "description": "CMEK metadata create path test",
            "config": {
                "arn": cmek_arn
            }
        }
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner"
        ], None):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header, self.project_id)
            result = self.capellaAPI.org_ops_apis.create_cmek_metadata(
                self.organisation_id, payload["name"], payload["config"],
                payload["description"])

            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.create_cmek_metadata(
                    self.organisation_id, payload["name"], payload["config"],
                    payload["description"])
            # 200 + {"id": ...} -> created (a valid cmek_arn is supplied,
            # 19839); 422 -> key-metadata validation error when no real CMEK
            # ARN is configured. Both prove the caller passed authorization
            # (an unauthorized role is denied with 403 before validation),
            # which is what this RBAC test verifies.
            self.validate_testcase(result, [200, 422], testcase, failures)
            if result.status_code == 200:
                # Every allowed role creates a new key; delete it right away
                # so the org doesn't collect one CMEK per role per run.
                self.created_cmek_id = result.json()["id"]
                self.delete_created_cmek()

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_payload(self):
        cmek_arn = self._get_cmek_arn()
        testcases = [
            {
                "desc": "Missing required name and config fields",
                "payload": {},
                "expected_status_code": 422,
                "expected_error":{
                        "code": 12003,
                        "hint": "Please ensure that the configuration for the key includes metadata for either the AWS, GCP, or Azure key, and then try again.",
                        "httpStatusCode": 422,
                        "message": "Unable to process request for key. The configuration for the key must provide the metadata for the AWS, GCP, or Azure key. None was provided."
                    }
            }, {
                "desc": "Missing required config field",
                "payload": {
                    "name": self.prefix + "cmek-missing-config"
                },
                "expected_status_code": 422,
                "expected_error":{
                        "code": 12003,
                        "hint": "Please ensure that the configuration for the key includes metadata for either the AWS, GCP, or Azure key, and then try again.",
                        "httpStatusCode": 422,
                        "message": "Unable to process request for key. The configuration for the key must provide the metadata for the AWS, GCP, or Azure key. None was provided."
                    }
            }, {
                "desc": "Wrong data type for name",
                "payload": {
                    "name": 12345,
                    "config": {"arn": cmek_arn}
                },
                "expected_status_code": [400, 422]
            }
        ]

        if cmek_arn:
            testcases.append({
                "desc": "Create CMEK metadata with valid payload",
                "payload": {
                    "name": self.prefix + "cmek-valid",
                    "description": "Valid CMEK metadata payload",
                    "config": {
                        "arn": cmek_arn
                    }
                },
                "expected_status_code": 200
            })

        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["desc"]))
            payload = testcase["payload"]
            result = self.capellaAPI.org_ops_apis.create_cmek_metadata(
                self.organisation_id,
                payload.get("name", ""),
                payload.get("config", {}),
                payload.get("description", ""))
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.create_cmek_metadata(
                    self.organisation_id,
                    payload.get("name", ""),
                    payload.get("config", {}),
                    payload.get("description", ""))
            self.validate_testcase(result, [200], testcase, failures,
                                   payloadTest=True)
            if result.status_code == 200:
                self.created_cmek_id = result.json()["id"]
                result = self.capellaAPI.org_ops_apis.delete_cmek_metadata(
                    self.organisation_id, self.created_cmek_id)
                if result.status_code == 429:
                    self.handle_rate_limit(int(result.headers["Retry-After"]))
                    result = self.capellaAPI.org_ops_apis.delete_cmek_metadata(
                        self.organisation_id, self.created_cmek_id)
                if result.status_code != 204:
                    self.fail("CMEK cleanup failed with {}".format(result.content))
                self.created_cmek_id = None

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        cmek_arn = self._get_cmek_arn()
        api_func_list = [[self.capellaAPI.org_ops_apis.create_cmek_metadata,
                          (self.organisation_id, self.prefix + "cmek",
                           {"arn": cmek_arn}, "throttle test")]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        cmek_arn = self._get_cmek_arn()
        api_func_list = [[self.capellaAPI.org_ops_apis.create_cmek_metadata,
                          (self.organisation_id, self.prefix + "cmek",
                           {"arn": cmek_arn}, "throttle test")]]
        self.throttle_test(api_func_list, True, self.project_id)
