"""
Created on February 11, 2025

@author: Created using cbRAT cbModule by Vipul Bhardwaj
"""

import copy
from pytests.Capella.RestAPIv4.FreeTier.get_free_tier_app_services import \
    GetFreeTierApp

class PutFreeTier(GetFreeTierApp):

    def setUp(self, nomenclature="FreeTier_PUT"):
        GetFreeTierApp.setUp(self, nomenclature)

    def tearDown(self):
        super(PutFreeTier, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Send call with valid path params"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}/appservices/freeTier",
                "expected_status_code": 404,
                "expected_error": {
                    "errorType": "RouteNotFound",
                    "message": "Not found"
                }
            }, {
                "description": "Replace the last path param name in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/appservices/freeTie",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/appservices/freeTier/freeTie",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
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
            }, {
                "description": "Call API with non-hex clusterId",
                "invalid_clusterId": self.replace_last_character(
                    self.free_tier_cluster_id, non_hex=True),
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
                "description": "Call API with non-hex appServiceId",
                "invalid_appServiceId": self.replace_last_character(
                    self.free_tier_app_id, non_hex=True),
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
            cluster = self.free_tier_cluster_id
            appService = self.free_tier_app_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.free_tier_app_svc_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]
            elif "invalid_clusterId" in testcase:
                cluster = testcase["invalid_clusterId"]
            elif "invalid_appServiceId" in testcase:
                appService = testcase["invalid_appServiceId"]

            result = (self.capellaAPI.cluster_ops_apis.
                      update_free_tier_app_service(
                        organization, project, cluster, appService,
                        self.expected_res["name"]))
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = (self.capellaAPI.cluster_ops_apis.
                          update_free_tier_app_service(
                            organization, project, cluster, appService,
                            self.expected_res["name"]))
            self.capellaAPI.cluster_ops_apis.free_tier_app_svc_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters/{}/appservices/"\
                "freeTier"
            self.validate_testcase(result, [204], testcase, failures)
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
            result = (self.capellaAPI.cluster_ops_apis.
                      update_free_tier_app_service(
                        self.organisation_id, self.project_id,
                        self.free_tier_cluster_id, self.free_tier_app_id,
                        self.expected_res["name"], headers=header))
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = (self.capellaAPI.cluster_ops_apis.
                          update_free_tier_app_service(
                            self.organisation_id, self.project_id,
                            self.free_tier_cluster_id, self.free_tier_app_id,
                            self.expected_res["name"], headers=header))
            self.validate_testcase(result, [204], testcase, failures)
        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_query_parameters(self):
        self.log.debug(
                "Correct Params - organization ID: {}, project ID: {}, "
                "cluster ID: {}, appService ID: {}".format(
                    self.organisation_id, self.project_id, self.free_tier_cluster_id,
                    self.free_tier_app_id))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id, self.free_tier_cluster_id,
                self.free_tier_app_id):
            testcases += 1
            testcase = {
                "description": "organization ID: {}, project ID: {}, "
                "cluster ID: {}, appService ID: {}"
                .format(str(combination[0]), str(combination[1]),
                        str(combination[2]), str(combination[3])),
                "organizationID": combination[0],
                "projectID": combination[1],
                "clusterID": combination[2],
                "appServiceID": combination[3]
            }
            if not (combination[0] == self.organisation_id and
                    combination[1] == self.project_id and
                    combination[2] == self.free_tier_cluster_id and
                    combination[3] == self.free_tier_app_id):
                if (combination[0] == "" or combination[1] == "" or
                        combination[2] == "" or combination[3] == ""):
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = "404 page not found"
                elif any(variable in [
                    int, bool, float, list, tuple, set, type(None)] for
                         variable in [
                             type(combination[0]), type(combination[1]), 
                             type(combination[2]), type(combination[3])]):
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
                elif combination[3] != self.bucket_id and not \
                        isinstance(combination[3], type(None)):
                    testcase["expected_status_code"] = 400
                    testcase["expected_error"] = {
                        "code": 400,
                        "hint": "Please review your request and ensure "
                                "that all required parameters are "
                                "correctly provided.",
                        "message": "BucketID is invalid.",
                        "httpStatusCode": 400
                    }
                elif combination[2] != self.free_tier_cluster_id:
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = {
                        "code": 4025,
                        "hint": "The requested cluster details could not be "
                                "found or fetched. Please ensure that the "
                                "correct cluster ID is provided.",
                        "message": "Unable to fetch the cluster details.",
                        "httpStatusCode": 404
                    }
                elif combination[1] != self.project_id:
                    testcase["expected_status_code"] = 422
                    testcase["expected_error"] = {
                        "code": 4031,
                        "hint": "Please provide a valid projectId.",
                        "httpStatusCode": 422,
                        "message": "Unable to process the request. The "
                                   "provided projectId {} is not valid for "
                                   "the cluster {}."
                        .format(combination[1], combination[2])
                    }
                elif isinstance(combination[3], type(None)):
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = {
                        "code": 6008,
                        "hint": "The requested bucket does not exist. Please "
                                "ensure that the correct bucket ID is "
                                "provided.",
                        "httpStatusCode": 404,
                        "message": "Unable to find the specified bucket."
                    }
                else:
                    testcase["expected_status_code"] = 400
                    testcase["expected_error"] = {
                        "code": 400,
                        "hint": "Please review your request and ensure that "
                                "all required parameters are correctly "
                                "provided.",
                        "message": "BucketID is invalid.",
                        "httpStatusCode": 400
                    }
            self.log.info("Executing test: {}".format(testcase["description"]))
            if "param" in testcase:
                kwarg = {testcase["param"]: testcase["paramValue"]}
            else:
                kwarg = dict()

            result = self.capellaAPI.cluster_ops_apis.update_free_tier_app_service(self.organisation_id, self.project_id, self.free_tier_cluster_id, 
                self.free_tier_app_id,
                **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.update_free_tier_app_service(self.organisation_id, self.project_id, self.free_tier_cluster_id, 
                    self.free_tier_app_id,
                    **kwarg)
            self.validate_testcase(result, [204], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_payload(self):
        testcases = list()
        for k in self.expected_res:
            for v in [
                "", 1, 0, 100000, -1, 123.123, None, [], {},
                self.generate_random_string(special_characters=False),
                self.generate_random_string(500, special_characters=False),
            ]:
                testcase = copy.deepcopy(self.expected_res)
                testcase[k] = v
                testcase["desc"] = "Testing `{}` with val: `{}` of {}"\
                    .format(k, v, type(v))
                if k == 'description' and v is None:
                    testcases.append(testcase)
                    continue
                if k in ['description', 'name'] and not isinstance(v, str):
                    testcase["expected_error"] = {
                        "code": 1000,
                        "hint": "The request was malformed or invalid.",
                        "httpStatusCode": 400,
                        "message": "Bad Request. Error: body contains "
                                   "incorrect JSON type for field \"{}\"."
                                   .format(k)
                    }
                    testcase["expected_status_code"] = 400
                elif k == 'name':
                    testcase["expected_error"] = {
                        "httpStatusCode": 422,
                    }
                    if len(v) < 2:
                        testcase["expected_error"] = {
                            "code": 422,
                            "hint": "Please review your request and ensure "
                                    "that all required parameters are "
                                    "correctly provided.",
                            "message": "App service name is too short. It must "
                                       "be at least 2 characters long."}
                    elif len(v) > 128:
                        testcase["expected_error"] = {
                            "code": 422,
                            "hint": "Please review your request and ensure "
                                    "that all required parameters are "
                                    "correctly provided.",
                            "message": "App service name must be 128 "
                                       "characters or less."
                        }
                    testcase["expected_status_code"] = 422
                elif k == 'description' and len(v) > 280:
                    testcase["expected_error"] = {
                        "code": 11053,
                        "hint": "The description provided is too long. The "
                                "maximum length of the description can be 256 "
                                "characters.",
                        "httpStatusCode": 422,
                        "message": "App service description must be 256 "
                                   "characters or less."
                    }
                    testcase["expected_status_code"] = 422
                testcases.append(testcase)
        failures = list()
        for testcase in testcases:
            self.log.info(testcase['desc'])
            result = (self.capellaAPI.cluster_ops_apis.
                      update_free_tier_app_service(
                        self.organisation_id, self.project_id,
                        self.free_tier_cluster_id, self.free_tier_app_id,
                        testcase["name"], testcase["description"]))
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = (self.capellaAPI.cluster_ops_apis.
                          update_free_tier_app_service(
                            self.organisation_id, self.project_id,
                            self.free_tier_cluster_id, self.free_tier_app_id,
                            testcase["name"], testcase["description"]))
            self.validate_testcase(result, [204], testcase, failures)
        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.update_free_tier_app_service, (
                self.organisation_id, self.project_id,
                self.free_tier_cluster_id, self.free_tier_app_id
            )
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.update_free_tier_app_service, (
                self.organisation_id, self.project_id,
                self.free_tier_cluster_id, self.free_tier_app_id
            )
        ]]
        self.throttle_test(api_func_list, True, self.project_id)
