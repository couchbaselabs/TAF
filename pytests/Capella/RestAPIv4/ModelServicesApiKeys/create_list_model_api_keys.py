"""
Created on June 09, 2026
"""

from pytests.Capella.RestAPIv4.ModelServicesApiKeys.model_api_keys_base \
    import ModelApiKeysBase


class CreateModelApiKey(ModelApiKeysBase):

    def setUp(self, nomenclature="ModelApiKeys_Create"):
        ModelApiKeysBase.setUp(self, nomenclature)

    def test_api_path(self):
        testcases = [
            {
                "description": "Create model API key with valid path params"
            }
            # {
            #     "description": "Replace api version in URI",
            #     "url": "/v3/organizations/{}/aiServices/models/apiKeys",
            #     "expected_status_code": 404,
            #     "expected_error": "<html><head><title>404NotFound</title>"
            #                       "</head><body><center><h1>404NotFound"
            #                       "</h1></center><hr><center>nginx"
            #                       "</center></body></html>"
            # }, {
            #     "description": "Replace models with model in URI",
            #     "url": "/v4/organizations/{}/aiServices/model/apiKeys",
            #     "expected_status_code": 404,
            #     "expected_error": "404 page not found"
            # }, {
            #     "description": "Replace apiKeys with apiKey in URI",
            #     "url": "/v4/organizations/{}/aiServices/models/apiKey",
            #     "expected_status_code": 404,
            #     "expected_error": "404 page not found"
            # }, {
            #     "description": "Add an invalid segment to the URI",
            #     "url": "/v4/organizations/{}/aiServices/models/apiKeys/key",
            #     "expected_status_code": 404,
            #     "expected_error": "404 page not found"
            # }, {
            #     "description": "Create model API key with non-hex "
            #                    "organizationID",
            #     "invalid_organizationID": self.replace_last_character(
            #         self.organisation_id, non_hex=True),
            #     "expected_status_code": 400,
            #     "expected_error": self.expected_invalid_uuid_error()
            # }
        ]
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(
                testcase["description"]))
            if "url" in testcase:
                self.capellaAPI.org_ops_apis.model_api_keys_endpoint = \
                    testcase["url"]
            organization = self.organisation_id
            if "invalid_organizationID" in testcase:
                organization = testcase["invalid_organizationID"]
            result = self.create_model_api_key(
                name="{}_{}".format(self.key_prefix,
                                    self.generate_random_string(4).lower()))
            self.capellaAPI.org_ops_apis.model_api_keys_endpoint = \
                self.model_api_keys_endpoint_default
            self.validate_testcase(result, [201], testcase, failures)
        if failures:
            self.fail("{} tests FAILED".format(len(failures)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init(
                ["organizationOwner"], other_proj=False):
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = self.create_model_api_key(headers=header)
            self.validate_testcase(result, [201], testcase, failures)
        if failures:
            self.fail("{} tests FAILED".format(len(failures)))

    def test_payload(self):
        testcases = [
            {
                "desc": "Create model API key with valid payload"
            }, {
                "desc": "Create model API key with description",
                "description": "My test model API key"
            }, {
                "desc": "Create model API key with allowedModels specified",
                "allowedModels": ["*"]
            }, {
                "desc": "Create model API key without expiry",
                "remove_expiry": True,
                "expected_status_code": 422,
                "expected_error": {
                    "code": 422,
                    "httpStatusCode": 422,
                    "message": "Request validation failed."
                }
            }, {
                "desc": "Create model API key without region",
                "remove_region": True,
                "expected_status_code": 422,
                "expected_error": {
                    "code": 422,
                    "httpStatusCode": 422,
                    "message": "Request validation failed."
                }
            }, {
                "desc": "Create model API key without allowedCIDRs",
                "remove_allowedCIDRs": True,
                "expected_status_code": 422,
                "expected_error": {
                    "code": 422,
                    "httpStatusCode": 422,
                    "message": "Request validation failed."
                }
            }, {
                "desc": "Create model API key without name",
                "remove_name": True,
                "expected_status_code": 422,
                "expected_error": {
                    "code": 422,
                    "httpStatusCode": 422,
                    "message": "Request validation failed."
                }
            }, {
                "desc": "Create model API key with empty name",
                "name": "",
                "expected_status_code": 422,
                "expected_error": {
                    "code": 422,
                    "httpStatusCode": 422,
                    "message": "Request validation failed."
                }
            }, {
                "desc": "Create model API key with invalid expiry (string)",
                "expiry": "invalid",
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if all the required params are present in "
                            "the request body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived "
                               "to be a client error."
                }
            }, {
                "desc": "Create model API key with invalid region",
                "region": "invalid-region-1",
                "expected_status_code": 422,
                "expected_error": ""
            }, {
                "desc": "Create model API key with expiry 0",
                "expiry": 0,
                "expected_status_code": 422,
                "expected_error": ""
            }, {
                "desc": "Create model API key with expiry > 365",
                "expiry": 400,
                "expected_status_code": 422,
                "expected_error": ""
            }
        ]
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["desc"]))
            payload = self.build_payload(
                name=testcase.get("name", "{}_{}".format(
                    self.key_prefix,
                    self.generate_random_string(4).lower())),
                description=testcase.get("description"),
                expiry=testcase.get("expiry", 180),
                allowed_cidrs=testcase.get("allowedCIDRs"),
                allowed_models=testcase.get("allowedModels"),
                region=testcase.get("region"))
            if testcase.get("remove_expiry"):
                del payload["expiry"]
            if testcase.get("remove_region"):
                del payload["region"]
            if testcase.get("remove_allowedCIDRs"):
                del payload["allowedCIDRs"]
            if testcase.get("remove_name"):
                del payload["name"]
            result = self.create_model_api_key(payload=payload)
            self.validate_testcase(result, [201], testcase, failures,
                                   payloadTest=True)
        if failures:
            self.fail("{} tests FAILED".format(len(failures)))

    def test_query_parameters(self):
        organization_id_values = [
            self.organisation_id,
            self.replace_last_character(self.organisation_id),
            self.replace_last_character(self.organisation_id, non_hex=True),
            True,
            123456789,
            "",
            None
        ]
        testcases = list()
        for val in organization_id_values:
            testcase = {
                "description": "Organization ID for create: {}".format(
                    str(val)),
                "organizationID": val
            }
            if val == "":
                testcase["expected_status_code"] = 405
                testcase["expected_error"] = ""
            elif type(val) in [int, bool, float, type(None)]:
                testcase["expected_status_code"] = 400
                testcase["expected_error"] = self.expected_invalid_uuid_error()
            elif val != self.organisation_id:
                testcase["expected_status_code"] = 403
                testcase["expected_error"] = {
                    "code": 1002,
                    "hint": "Your access to the requested resource is denied."
                            " Please make sure you have the necessary "
                            "permissions to access the resource.",
                    "httpStatusCode": 403,
                    "message": "Access Denied."
                }
            testcases.append(testcase)

        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(
                testcase["description"]))
            result = self.api_call_with_retry(
                self.capellaAPI.org_ops_apis.create_model_api_key,
                testcase["organizationID"],
                self.build_payload(
                    name="{}_{}".format(
                        self.key_prefix,
                        self.generate_random_string(4).lower())))
            self.validate_testcase(result, [201], testcase, failures)
        if failures:
            self.fail("{} tests FAILED".format(len(failures)))


class ListModelApiKeys(ModelApiKeysBase):

    def setUp(self, nomenclature="ModelApiKeys_List"):
        ModelApiKeysBase.setUp(self, nomenclature)
        self.ensure_model_api_key_exists()

    def test_api_path(self):
        testcases = [
            {
                "description": "List model API keys with valid path params"
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
                "url": "/v4/organizations/{}/aiServices/models/apiKeys/key",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "List model API keys with non-hex "
                               "organizationID",
                "invalid_organizationID": self.replace_last_character(
                    self.organisation_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": self.expected_invalid_uuid_error()
            }
        ]
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(
                testcase["description"]))
            if "url" in testcase:
                self.capellaAPI.org_ops_apis.model_api_keys_endpoint = \
                    testcase["url"]
            organization = self.organisation_id
            if "invalid_organizationID" in testcase:
                organization = testcase["invalid_organizationID"]
            result = self.api_call_with_retry(
                self.capellaAPI.org_ops_apis.list_model_api_keys,
                organization)
            self.capellaAPI.org_ops_apis.model_api_keys_endpoint = \
                self.model_api_keys_endpoint_default
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
                self.capellaAPI.org_ops_apis.list_model_api_keys,
                self.organisation_id, headers=header)
            self.validate_testcase(result, [200], testcase, failures)
        if failures:
            self.fail("{} tests FAILED".format(len(failures)))

    def test_query_parameters(self):
        testcases = [
            {
                "description": "List model API keys with valid pagination",
                "page": 1,
                "perPage": 10
            }, {
                "description": "List model API keys with sortBy name",
                "sortBy": "name",
                "sortDirection": "asc"
            }, {
                "description": "List model API keys with sortBy createdAt",
                "sortBy": "createdAt",
                "sortDirection": "desc"
            }, {
                "description": "List model API keys with filterBy region",
                "filterBy": "region:eq:{}".format(self.region)
            }, {
                "description": "List model API keys with invalid filterBy",
                "filterBy": "name:eq:test",
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if all the required params are present "
                            "in the request body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived "
                               "to be a client error."
                }
            }, {
                "description": "List model API keys with invalid page",
                "page": "invalid",
                "expected_status_code": 400,
                "expected_error": ""
            }
        ]
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(
                testcase["description"]))
            result = self.api_call_with_retry(
                self.capellaAPI.org_ops_apis.list_model_api_keys,
                self.organisation_id,
                page=testcase.get("page"),
                perPage=testcase.get("perPage"),
                sortBy=testcase.get("sortBy"),
                sortDirection=testcase.get("sortDirection"),
                filterBy=testcase.get("filterBy"))
            self.validate_testcase(result, [200], testcase, failures)
        if failures:
            self.fail("{} tests FAILED".format(len(failures)))
