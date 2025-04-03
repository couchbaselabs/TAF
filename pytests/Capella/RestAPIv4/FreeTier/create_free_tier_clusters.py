"""
Created on February 11, 2025

@author: Created using cbRAT cbModule by Vipul Bhardwaj
"""

import copy
from pytests.Capella.RestAPIv4.FreeTier.get_free_tier_clusters import \
    GetFreeTier


class PostFreeTier(GetFreeTier):

    def setUp(self, nomenclature="FreeTier_POST"):
        GetFreeTier.setUp(self, nomenclature)
        self.dummy_cloud_provider = ("Wrong Value for creation to through a "
                                     "valid/expected error")

    def tearDown(self):
        super(PostFreeTier, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Send call with valid path params"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/freeTier",
                "expected_status_code": 404,
                "expected_error": {
                    "errorType": "RouteNotFound",
                    "message": "Not found"
                }
            }, {
                "description": "Replace the last path param name in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/freeTie",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/freeTier/freeTie",
                "expected_status_code": 405,
                "expected_error": ""
            }, {
                "description": "Call API with non-hex organizationId",
                "invalid_organizationId": self.replace_last_character(
                    self.organisation_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if you have provided a valid URL and all "
                            "the required params are present in the request "
                            "body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived to "
                               "be a client error."
                }
            }, {
                "description": "Call API with non-hex projectId",
                "invalid_projectId": self.replace_last_character(
                    self.project_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if you have provided a valid URL and all "
                            "the required params are present in the request "
                            "body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived to "
                               "be a client error."
                }
            }
        ]
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            organization = self.organisation_id
            project = self.project_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.free_tier_cluster_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]

            result = self.capellaAPI.cluster_ops_apis.create_free_tier_cluster(
                organization, project, self.expected_res["name"],
                self.dummy_cloud_provider)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = (self.capellaAPI.cluster_ops_apis.
                          create_free_tier_cluster(
                            organization, project,
                            self.expected_res["name"],
                            self.dummy_cloud_provider))
            self.capellaAPI.cluster_ops_apis.free_tier_cluster_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters/freeTier"
            if self.validate_testcase(result, [400], testcase, failures):
                self.log.debug("Dummy based error Successful")
        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
             "organizationOwner", "projectOwner", "projectManager"
        ]):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = self.capellaAPI.cluster_ops_apis.create_free_tier_cluster(
                self.organisation_id, self.project_id,
                self.expected_res["name"], self.dummy_cloud_provider,
                header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.create_free_tier_cluster(
                    self.organisation_id, self.project_id,
                    self.expected_res["name"],
                    self.dummy_cloud_provider,
                    header)
            if self.validate_testcase(result, [400], testcase, failures):
                self.log.debug("Dummy based error Successful")
        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_query_parameters(self):
        self.log.debug(
                "Correct Params - organization ID: {}, project ID: {}".format(
                    self.organisation_id, self.project_id))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id):
            testcases += 1
            testcase = {
                "description": "organization ID: {}, project ID: {}"
                .format(str(combination[0]), str(combination[1])),
                "organizationID": combination[0],
                "projectID": combination[1]
            }
            if not (combination[0] == self.organisation_id and
                    combination[1] == self.project_id):
                if combination[0] == "":
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = "404 page not found"
                elif combination[1] == "":
                    testcase["expected_status_code"] = 405
                    testcase["expected_error"] = ""
                elif any(variable in [
                    int, bool, float, list, tuple, set, type(None)] for
                         variable in [
                             type(combination[0]), type(combination[1])]):
                    testcase["expected_status_code"] = 400
                    testcase["expected_error"] = {
                        "code": 1000,
                        "hint": "Check if you have provided a valid URL and "
                                "all the required params are present in the "
                                "request body.",
                        "httpStatusCode": 400,
                        "message": "The server cannot or will not process the "
                                   "request due to something that is "
                                   "perceived to be a client error."
                    }
                elif combination[0] != self.organisation_id:
                    testcase["expected_status_code"] = 403
                    testcase["expected_error"] = {
                        "code": 1002,
                        "hint": "Your access to the requested resource is "
                                "denied. Please make sure you have the "
                                "necessary permissions to access the "
                                "resource.",
                        "httpStatusCode": 403,
                        "message": "Access Denied."
                    }
                else:
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = {
                        "code": 2000,
                        "hint": "Check if the project ID is valid.",
                        "httpStatusCode": 404,
                        "message": "The server cannot find a project by its "
                                   "ID."
                    }
            self.log.info("Executing test: {}".format(testcase["description"]))
            if "param" in testcase:
                kwarg = {testcase["param"]: testcase["paramValue"]}
            else:
                kwarg = dict()

            result = self.capellaAPI.cluster_ops_apis.create_free_tier_cluster(self.organisation_id, self.project_id,
                self.expected_res["name"],
                self.dummy_cloud_provider,
                **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = (self.capellaAPI.cluster_ops_apis.
                          create_free_tier_cluster(
                            self.organisation_id, self.project_id,
                            self.expected_res["name"],
                            self.dummy_cloud_provider, **kwarg))
            if self.validate_testcase(result, [202], testcase, failures):
                self.log.debug("Creation Successful")
        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_payload(self):
        testcases = list()
        for k in self.expected_res:
            if k not in ["cloudProvider", "name", "description"]:
                continue
            for v in [
                "", 1, 0, 100000, -1, 123.123, None, [], {},
                self.generate_random_string(special_characters=False),
                self.generate_random_string(500, special_characters=False),
            ]:
                testcase = copy.deepcopy(self.expected_res)
                if k in ["cloudProvider", "name", "description"]:
                    testcase[k] = v
                testcase["desc"] = "Testing `{}` with val: `{}` of {}"\
                    .format(k, v, type(v))
                # Add expected failure codes for malformed payload values...
                if k == "cloudProvider" and (isinstance(v, dict) or v is None):
                    testcase["expected_error"] = {
                        "code": 422,
                        "hint": "Please review your request and ensure that "
                                "all required parameters are correctly "
                                "provided.",
                        "httpStatusCode": 422,
                        "message": "Unable to process request to create trial "
                                   "cluster. Self service trials are limited to "
                                   "provisioning one cluster."
                    }
                    testcase["expected_status_code"] = 422
                elif k == "cloudProvider":
                    testcase["expected_error"] = {
                        "code": 1000,
                        "hint": "The request was malformed or invalid.",
                        "httpStatusCode": 400,
                        "message": "Bad Request. Error: body contains "
                                   "incorrect JSON type for field "
                                   "\"cloudProvider\"."
                    }
                    testcase["expected_status_code"] = 400
                elif k == "description" and (isinstance(v, str) or v is None):
                    testcase["expected_error"] = {
                        "code": 422,
                        "hint": "Please review your request and ensure that "
                                "all required parameters are correctly "
                                "provided.",
                        "httpStatusCode": 422,
                        "message": "Unable to process request to create trial "
                                   "cluster. Self service trials are limited to "
                                   "provisioning one cluster."
                    }
                    testcase["expected_status_code"] = 422
                elif k == "description":
                    testcase["expected_error"] = {
                        "code": 1000,
                        "hint": "The request was malformed or invalid.",
                        "httpStatusCode": 400,
                        "message": "Bad Request. Error: body contains "
                                   "incorrect JSON type for field "
                                   "\"description\"."
                    }
                    testcase["expected_status_code"] = 400
                elif k == "name" and not isinstance(v, str):
                    testcase["expected_error"] = {
                        "code": 1000,
                        "hint": "The request was malformed or invalid.",
                        "httpStatusCode": 400,
                        "message": "Bad Request. Error: body contains "
                                   "incorrect JSON type for field \"name\"."
                    }
                    testcase["expected_status_code"] = 400
                elif k == "name" and (len(v) < 2 or v is None):
                    testcase["expected_error"] = {
                        "code": 422,
                        "hint": "Please review your request and ensure that "
                                "all required parameters are correctly "
                                "provided.",
                        "httpStatusCode": 422,
                        "message": "Unable to process request to create trial "
                                   "cluster. Self service trials are limited to "
                                   "provisioning one cluster."
                    }
                    testcase["expected_status_code"] = 422
                elif k == "name" and len(v) > 128:
                    testcase["expected_error"] = {
                        "code": 422,
                        "hint": "Please review your request and ensure that "
                                "all required parameters are correctly "
                                "provided.",
                        "httpStatusCode": 422,
                        "message": "Unable to process request to create trial "
                                   "cluster. Self service trials are limited to "
                                   "provisioning one cluster."
                    }
                    testcase["expected_status_code"] = 422
                else:
                    testcase["expected_error"] = {
                        "code": 422,
                        "hint": "Please review your request and ensure that "
                                "all required parameters are correctly "
                                "provided.",
                        "httpStatusCode": 422,
                        "message": "Unable to process request to create trial "
                                   "cluster. Self service trials are limited to "
                                   "provisioning one cluster."
                    }
                    testcase["expected_status_code"] = 422
                testcases.append(testcase)
        failures = list()
        for testcase in testcases:
            self.log.info(testcase['desc'])
            result = self.capellaAPI.cluster_ops_apis.create_free_tier_cluster(
                self.organisation_id, self.project_id,
                testcase["name"], testcase["cloudProvider"],
                testcase["description"])
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = (self.capellaAPI.cluster_ops_apis.
                          create_free_tier_cluster(
                            self.organisation_id, self.project_id,
                            testcase["name"], testcase["cloudProvider"],
                            testcase["description"]))
            if self.validate_testcase(result, [202], testcase, failures,
                                      payloadTest=True):
                self.log.debug("Dummy based error Successful")
        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.create_free_tier_cluster, (
                self.organisation_id, self.project_id,
                self.expected_res["name"], self.dummy_cloud_provider
            )
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.create_free_tier_cluster, (
                self.organisation_id, self.project_id,
                self.expected_res["name"], self.dummy_cloud_provider
            )
        ]]
        self.throttle_test(api_func_list, True, self.project_id)
