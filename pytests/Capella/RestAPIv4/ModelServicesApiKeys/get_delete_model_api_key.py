"""
Created on June 09, 2026
"""

from pytests.Capella.RestAPIv4.ModelServicesApiKeys.model_api_keys_base \
    import ModelApiKeysBase


class GetModelApiKey(ModelApiKeysBase):

    def setUp(self, nomenclature="ModelApiKeys_Get"):
        ModelApiKeysBase.setUp(self, nomenclature)
        self.existing_key_id = self.ensure_model_api_key_exists()

    def test_api_path(self):
        testcases = [
            {
                "description": "Get model API key with valid path params"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/aiServices/models/apiKeys",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title>"
                                  "</head><body><center><h1>404NotFound"
                                  "</h1></center><hr><center>nginx"
                                  "</center></body></html>"
            }, {
                "description": "Replace models with model in URI",
                "url": "/v4/organizations/{}/aiServices/model/apiKeys",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Replace apiKeys with apiKey in URI",
                "url": "/v4/organizations/{}/aiServices/models/apiKey",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/aiServices/models/apiKeys/{}/info",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Get model API key with non-hex organizationID",
                "invalid_organizationID": self.replace_last_character(
                    self.organisation_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": self.expected_invalid_uuid_error()
            }, {
                "description": "Get model API key with non-hex apiKeyId",
                "invalid_keyID": self.replace_last_character(
                    self.existing_key_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": self.expected_invalid_uuid_error()
            }, {
                "description": "Get model API key with non-existing apiKeyId",
                "non_existing_key": True,
                "expected_status_code": 404,
                "expected_error": ""
            }
        ]
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(
                testcase["description"]))
            if "url" in testcase:
                self.capellaAPI.org_ops_apis.model_api_key_endpoint = \
                    testcase["url"]
            organization = self.organisation_id
            key_id = self.existing_key_id
            if "invalid_organizationID" in testcase:
                organization = testcase["invalid_organizationID"]
            elif "invalid_keyID" in testcase:
                key_id = testcase["invalid_keyID"]
            elif "non_existing_key" in testcase:
                key_id = "00000000-0000-0000-0000-000000000000"
            result = self.api_call_with_retry(
                self.capellaAPI.org_ops_apis.get_model_api_key,
                organization, key_id)
            self.capellaAPI.org_ops_apis.model_api_key_endpoint = \
                self.model_api_key_endpoint_default
            self.validate_testcase(result, [200], testcase, failures)
        if failures:
            self.fail("{} tests FAILED".format(len(failures)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init(
                ["organizationOwner"], other_proj=False):
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = self.api_call_with_retry(
                self.capellaAPI.org_ops_apis.get_model_api_key,
                self.organisation_id, self.existing_key_id, headers=header)
            self.validate_testcase(result, [200], testcase, failures)
        if failures:
            self.fail("{} tests FAILED".format(len(failures)))

    def test_query_parameters(self):
        key_id_values = [
            self.existing_key_id,
            self.replace_last_character(self.existing_key_id),
            self.replace_last_character(self.existing_key_id, non_hex=True),
            True,
            123456789,
            "",
            None
        ]
        testcases = list()
        for val in key_id_values:
            testcase = {
                "description": "API key ID: {}".format(str(val)),
                "keyID": val
            }
            if val == "":
                testcase["expected_status_code"] = 404
                testcase["expected_error"] = "404 page not found"
            elif type(val) in [int, bool, float, type(None)]:
                testcase["expected_status_code"] = 400
                testcase["expected_error"] = self.expected_invalid_uuid_error()
            elif val != self.existing_key_id:
                testcase["expected_status_code"] = 404
                testcase["expected_error"] = ""
            testcases.append(testcase)

        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(
                testcase["description"]))
            result = self.api_call_with_retry(
                self.capellaAPI.org_ops_apis.get_model_api_key,
                self.organisation_id, testcase["keyID"])
            self.validate_testcase(result, [200], testcase, failures)
        if failures:
            self.fail("{} tests FAILED".format(len(failures)))


class DeleteModelApiKey(ModelApiKeysBase):

    def setUp(self, nomenclature="ModelApiKeys_Delete"):
        ModelApiKeysBase.setUp(self, nomenclature)
        self.existing_key_id = self.ensure_model_api_key_exists()

    def test_api_path(self):
        testcases = [
            {
                "description": "Delete model API key with valid path params"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/aiServices/models/apiKeys",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title>"
                                  "</head><body><center><h1>404NotFound"
                                  "</h1></center><hr><center>nginx"
                                  "</center></body></html>"
            }, {
                "description": "Replace models with model in URI",
                "url": "/v4/organizations/{}/aiServices/model/apiKeys",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Replace apiKeys with apiKey in URI",
                "url": "/v4/organizations/{}/aiServices/models/apiKey",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/aiServices/models/apiKeys/{}/delete",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Delete model API key with non-hex "
                               "organizationID",
                "invalid_organizationID": self.replace_last_character(
                    self.organisation_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": self.expected_invalid_uuid_error()
            }, {
                "description": "Delete model API key with non-hex apiKeyId",
                "invalid_keyID": self.replace_last_character(
                    self.existing_key_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": self.expected_invalid_uuid_error()
            }, {
                "description": "Delete non-existing model API key",
                "non_existing_key": True,
                "expected_status_code": 404,
                "expected_error": ""
            }
        ]
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(
                testcase["description"]))
            if "url" in testcase:
                self.capellaAPI.org_ops_apis.model_api_key_endpoint = \
                    testcase["url"]
            organization = self.organisation_id
            key_id = self.existing_key_id
            if "invalid_organizationID" in testcase:
                organization = testcase["invalid_organizationID"]
            elif "invalid_keyID" in testcase:
                key_id = testcase["invalid_keyID"]
            elif "non_existing_key" in testcase:
                key_id = "00000000-0000-0000-0000-000000000000"
            result = self.api_call_with_retry(
                self.capellaAPI.org_ops_apis.delete_model_api_key,
                organization, key_id)
            self.capellaAPI.org_ops_apis.model_api_key_endpoint = \
                self.model_api_key_endpoint_default
            # Remove from tracking if delete succeeds
            if result.status_code == 204 and \
                    key_id == self.existing_key_id:
                self.created_key_ids.discard(key_id)
            self.validate_testcase(result, [204], testcase, failures)
        if failures:
            self.fail("{} tests FAILED".format(len(failures)))

    def test_authorization(self):
        failures = list()
        # Create a fresh key for each auth test since we delete during tests
        key_id = self.ensure_model_api_key_exists(
            name="{}_{}".format(self.key_prefix,
                                self.generate_random_string(4).lower()))
        for testcase in self.v4_RBAC_injection_init(
                ["organizationOwner"], other_proj=False):
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = self.api_call_with_retry(
                self.capellaAPI.org_ops_apis.delete_model_api_key,
                self.organisation_id, key_id, headers=header)
            if result.status_code == 204:
                self.created_key_ids.discard(key_id)
                # Create a new key for next auth iteration
                key_id = self.ensure_model_api_key_exists(
                    name="{}_{}".format(
                        self.key_prefix,
                        self.generate_random_string(4).lower()))
            self.validate_testcase(result, [204], testcase, failures)
        if failures:
            self.fail("{} tests FAILED".format(len(failures)))

    def test_query_parameters(self):
        key_id_values = [
            self.existing_key_id,
            self.replace_last_character(self.existing_key_id),
            self.replace_last_character(self.existing_key_id, non_hex=True),
            True,
            123456789,
            "",
            None
        ]
        testcases = list()
        for val in key_id_values:
            testcase = {
                "description": "API key ID for delete: {}".format(str(val)),
                "keyID": val
            }
            if val == "":
                testcase["expected_status_code"] = 404
                testcase["expected_error"] = "404 page not found"
            elif type(val) in [int, bool, float, type(None)]:
                testcase["expected_status_code"] = 400
                testcase["expected_error"] = self.expected_invalid_uuid_error()
            elif val != self.existing_key_id:
                testcase["expected_status_code"] = 404
                testcase["expected_error"] = ""
            testcases.append(testcase)

        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(
                testcase["description"]))
            result = self.api_call_with_retry(
                self.capellaAPI.org_ops_apis.delete_model_api_key,
                self.organisation_id, testcase["keyID"])
            if result.status_code == 204 and \
                    testcase["keyID"] == self.existing_key_id:
                self.created_key_ids.discard(self.existing_key_id)
                # Create a new key to continue tests
                self.existing_key_id = self.ensure_model_api_key_exists(
                    name="{}_{}".format(
                        self.key_prefix,
                        self.generate_random_string(4).lower()))
                testcases.append({
                    "description": "API key ID for delete: {}".format(
                        self.existing_key_id),
                    "keyID": self.existing_key_id
                })
            self.validate_testcase(result, [204], testcase, failures)
        if failures:
            self.fail("{} tests FAILED".format(len(failures)))
