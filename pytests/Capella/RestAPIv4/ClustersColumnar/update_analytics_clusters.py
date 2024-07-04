"""
Created on May 29, 2024

@author: Created using cbRAT cbModule by Vipul Bhardwaj
"""

import copy
from pytests.Capella.RestAPIv4.ClustersColumnar.get_analytics_clusters import \
    GetAnalyticsClusters


class PutAnalyticsClusters(GetAnalyticsClusters):

    def setUp(self, nomenclature="ClustersColumnar_PUT"):
        GetAnalyticsClusters.setUp(self, nomenclature)

    def tearDown(self):
        super(PutAnalyticsClusters, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Send call with valid path params"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/analyticsClusters",
                "expected_status_code": 404,
                "expected_error": {
                    "errorType": "RouteNotFound",
                    "message": "Not found"
                }
            }, {
                "description": "Replace the last path param name in URI",
                "url": "/v4/organizations/{}/projects/{}/analyticsCluster",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/analyticsClusters/analyticsCluster",
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
                "description": "Call API with non-hex analyticsClusterId",
                "invalid_analyticsClusterId": self.replace_last_character(
                    self.analyticsCluster_id, non_hex=True),
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
            analyticsCluster = self.analyticsCluster_id

            if "url" in testcase:
                self.columnarAPI.analytics_clusters_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]
            elif "invalid_analyticsClusterId" in testcase:
                analyticsCluster = testcase["invalid_analyticsClusterId"]

            result = self.columnarAPI.update_analytics_cluster(
                organization, project, analyticsCluster,
                self.expected_res["name"], self.expected_res["nodes"],
                self.expected_res["support"])
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.columnarAPI.update_analytics_cluster(
                    organization, project, analyticsCluster,
                    self.expected_res["name"], self.expected_res["nodes"],
                    self.expected_res["support"])

            self.columnarAPI.analytics_clusters_endpoint = \
                "/v4/organizations/{}/projects/{}/analyticsClusters"

            self.validate_testcase(result, [204], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        self.api_keys.update(
            self.create_api_keys_for_all_combinations_of_roles(
                [self.project_id]))

        resp = self.capellaAPI.org_ops_apis.create_project(
            self.organisation_id, "Auth_Project")
        if resp.status_code == 201:
            other_project_id = resp.json()["id"]
        else:
            self.fail("Error while creating project")

        testcases = []
        for role in self.api_keys:
            testcase = {
                "description": "Calling API with {} role".format(role),
                "token": self.api_keys[role]["token"],
            }
            if not any(element in [
                 "organizationOwner", "projectOwner",
                 "projectManager"
            ] for element in self.api_keys[role]["roles"]):
                testcase["expected_error"] = {
                    "code": 1002,
                    "hint": "Your access to the requested resource is denied. "
                            "Please make sure you have the necessary "
                            "permissions to access the resource.",
                    "httpStatusCode": 403,
                    "message": "Access Denied."
                }
                testcase["expected_status_code"] = 403
            testcases.append(testcase)
        self.auth_test_extension(testcases, other_project_id)

        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, other_project_id)
            result = self.columnarAPI.update_analytics_cluster(
                self.organisation_id, self.project_id,
                self.analyticsCluster_id, self.expected_res["name"],
                self.expected_res["nodes"], self.expected_res["support"],
                headers=header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.columnarAPI.update_analytics_cluster(
                    self.organisation_id, self.project_id,
                    self.analyticsCluster_id, self.expected_res["name"],
                    self.expected_res["nodes"], self.expected_res["support"],
                    headers=header)

            self.validate_testcase(result, [204], testcase, failures)

        self.update_auth_with_api_token(self.org_owner_key["token"])
        resp = self.capellaAPI.org_ops_apis.delete_project(
            self.organisation_id, other_project_id)
        if resp.status_code != 204:
            self.log.error("Error while deleting project {}"
                           .format(other_project_id))

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_query_parameters(self):
        self.log.debug(
                "Correct Params - organization ID: {}, project ID: {}, "
                "analyticsCluster ID: {}".format(self.organisation_id,
                                                 self.project_id,
                                                 self.analyticsCluster_id))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id,
                self.analyticsCluster_id):
            testcases += 1
            testcase = {
                "description": "organization ID: {}, project ID: {}, "
                "analyticsCluster ID: {}"
                .format(str(combination[0]), str(combination[1]),
                        str(combination[2])),
                "organizationID": combination[0],
                "projectID": combination[1],
                "analyticsClusterID": combination[2]
            }
            if not (combination[0] == self.organisation_id and
                    combination[1] == self.project_id and
                    combination[2] == self.analyticsCluster_id):
                if (combination[0] == "" or combination[1] == "" or
                        combination[2] == ""):
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = "404 page not found"
                elif any(variable in [
                    int, bool, float, list, tuple, set, type(None)] for
                         variable in [
                             type(combination[0]), type(combination[1]), 
                             type(combination[2])]):
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
                elif combination[2] != self.analyticsCluster_id:
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = {
                        'code': 404,
                        'message': 'Unable to fetch the instance details.',
                        'hint': 'Please review your request and ensure that '
                                'all required parameters are correctly '
                                'provided.',
                        'httpStatusCode': 404
                    }
                else:
                    testcase["expected_status_code"] = 422
                    testcase["expected_error"] = {
                        "code": 422,
                        "hint": "Please review your request and ensure that "
                                "all required parameters are correctly "
                                "provided.",
                        "httpStatusCode": 422,
                        "message": "Unable to process the request. The "
                                   "provided projectId {} is not valid for "
                                   "the analytics cluster {}."
                        .format(combination[1], combination[2])
                    }
            self.log.info("Executing test: {}".format(testcase["description"]))
            if "param" in testcase:
                kwarg = {testcase["param"]: testcase["paramValue"]}
            else:
                kwarg = dict()

            result = self.columnarAPI.update_analytics_cluster(
                testcase["organizationID"], testcase["projectID"],
                testcase["analyticsClusterID"], self.expected_res["name"],
                self.expected_res["nodes"], self.expected_res["support"],
                **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.columnarAPI.update_analytics_cluster(
                    testcase["organizationID"], testcase["projectID"],
                    testcase["analyticsClusterID"], self.expected_res["name"],
                    self.expected_res["nodes"], self.expected_res["support"],
                    **kwarg)

            self.validate_testcase(result, [204], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_payload(self):
        testcases = list()

        for key in self.expected_res:
            if key in ["region", "cloudProvider", "availability", "compute", "id"]:
                continue

            values = [
                "", 1, 0, 100000, -1, 123.123,
                self.generate_random_string(special_characters=False),
                self.generate_random_string(2500, special_characters=False),
            ]
            for value in values:
                testcase = copy.deepcopy(self.expected_res)
                testcase[key] = value
                for param in ["region", "cloudProvider"]:
                    del testcase[param]

                testcase["desc"] = "Testing '{}' with val: `{}` of type: `{}`"\
                                   .format(key, value, type(value))
                if (
                        (key in ["name", "description"] and not isinstance(
                            value, str)) or
                        (key == "nodes" and not isinstance(value, int)) or
                        (key == "support" and not isinstance(value, dict))
                ):
                    testcase["expected_status_code"] = 400
                    testcase["expected_error"] = {
                        "code": 1000,
                        "hint": "The request was malformed or invalid.",
                        "httpStatusCode": 400,
                        "message": 'Bad Request. Error: body contains '
                                   'incorrect JSON type for '
                                   'field "{}".'.format(key)
                    }
                elif key == "nodes" and value not in [1, 2, 4, 8, 16, 32]:
                    testcase["expected_status_code"] = 422
                    testcase["expected_error"] = {
                        "code": 422,
                        "hint": "Please review your request and ensure "
                                "that all required parameters are "
                                "correctly provided.",
                        "httpStatusCode": 422,
                        "message": "Unable to process request for columnar"
                                   " instance. The node count provided of "
                                   "{} is not valid. Must be one of 1, 2, "
                                   "4, 8, 16, or 32.".format(value)
                    }
                elif key == "name":
                    if len(value) > 128:
                        testcase["expected_status_code"] = 422
                        testcase["expected_error"] = {
                            "code": 4012,
                            "hint": "Please ensure that the provided cluster "
                                    "name does not exceed the maximum length "
                                    "limit.",
                            "httpStatusCode": 422,
                            "message": "The name provided is not valid. The "
                                       "name must be 128 characters or less."
                        }
                    elif len(value) < 2:
                        testcase["expected_status_code"] = 422
                        testcase["expected_error"] = {
                            "code": 4011,
                            "hint": "Please ensure that the provided name "
                                    "has at least the minimum length.",
                            "httpStatusCode": 422,
                            "message": "The name provided is not valid. The "
                                       "name is too short. It must be at "
                                       "least 2 characters long."
                        }
                    elif any(specialChar in value for specialChar in [
                            "#", "+"]):
                        testcase["expected_status_code"] = 422
                        testcase["expected_error"] = {
                            "code": 4013,
                            "hint": "Please ensure that the cluster name "
                                    "only contains valid characters when "
                                    "creating or modifying the cluster name.",
                            "httpStatusCode": 422,
                            "message": "Unable to process request. The name "
                                       "provided for the cluster is not "
                                       "valid. It contains an invalid "
                                       "character '#'."
                        }
                elif key == "description" and len(value) > 280:
                    testcase["expected_status_code"] = 422
                    testcase["expected_error"] = {
                        "code": 4014,
                        "hint": "Please ensure that the provided cluster "
                                "description does not exceed the maximum "
                                "length limit when creating or modifying the "
                                "cluster.",
                        "httpStatusCode": 422,
                        "message": "Unable to process request. The description"
                                   " provided is not valid. The description "
                                   "can be a maximum of 280 characters."
                    }
                testcases.append(testcase)

        failures = list()
        for testcase in testcases:
            self.log.info(testcase['desc'])
            res = self.columnarAPI.update_analytics_cluster(
                self.organisation_id, self.project_id,
                self.analyticsCluster_id, testcase["name"],
                testcase["nodes"], testcase["support"],
                testcase["description"])
            if res.status_code == 429:
                self.handle_rate_limit(int(res.header['Retry-After']))
                res = self.columnarAPI.update_analytics_cluster(
                    self.organisation_id, self.project_id,
                    self.analyticsCluster_id, testcase["name"],
                    testcase["nodes"], testcase["support"],
                    testcase["description"])

            self.validate_testcase(res, [204], testcase, failures,
                                   payloadTest=True)
        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.columnarAPI.update_analytics_cluster, (
                self.organisation_id, self.project_id,
                self.analyticsCluster_id,
                self.expected_res["name"], self.expected_res["nodes"]
            )
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.columnarAPI.update_analytics_cluster, (
                self.organisation_id, self.project_id,
                self.analyticsCluster_id,
                self.expected_res["name"], self.expected_res["nodes"]
            )
        ]]
        self.throttle_test(api_func_list, True, self.project_id)
