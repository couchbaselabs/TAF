"""
Created on June 04, 2024

@author: Created using cbRAT cbModule by Vipul Bhardwaj
"""

import copy
import re
from pytests.Capella.RestAPIv4.Clusters.get_clusters import GetCluster

class PostEndpointCommand(GetCluster):

    def setUp(self, nomenclature="PrivateEndpointCommands_POST"):
        GetCluster.setUp(self, nomenclature)
        self.expected_res = {
            "vpcID": self.input.param("vpcId", "mockValue"),
            "subnetIDs": ["mockValue"],
        }
        if (self.input.param("cluster_template", "AWS_r5_xlarge") ==
                "Azure_E4s_v5"):
            self.expected_res = {
                "resourceGroupName": "test-rg",
                "virtualNetwork": "vnet-1/subnet-1"
            }

    def tearDown(self):
        super(PostEndpointCommand, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Send call with valid path params"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}/privateEndpointService/endpointCommand",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }, {
                "description": "Replace the last path param name in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/privateEndpointService/endpointComman",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/privateEndpointService/endpointCommand/endpointComman",
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
                    self.cluster_id, non_hex=True),
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
            cluster = self.cluster_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.private_network_command_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]
            elif "invalid_clusterId" in testcase:
                cluster = testcase["invalid_clusterId"]

            result = (self.capellaAPI.cluster_ops_apis
                      .post_private_endpoint_command(
                        organization, project, cluster, self.expected_res))
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = (self.capellaAPI.cluster_ops_apis
                          .post_private_endpoint_command(
                            organization, project, cluster, self.expected_res))
            self.capellaAPI.cluster_ops_apis\
                .private_network_command_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters/{}/"\
                "privateEndpointService/endpointCommand"
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectViewer", "projectOwner",
            "projectManager"
        ]):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = (self.capellaAPI.cluster_ops_apis
                      .post_private_endpoint_command(
                        self.organisation_id, self.project_id, self.cluster_id,
                        self.expected_res, header))
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = (self.capellaAPI.cluster_ops_apis
                          .post_private_endpoint_command(
                            self.organisation_id, self.project_id,
                            self.cluster_id, self.expected_res, header))
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_query_parameters(self):
        self.log.debug(
                "Correct Params - organization ID: {}, project ID: {}, "
                "cluster ID: {}".format(
                    self.organisation_id, self.project_id, self.cluster_id))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id, self.cluster_id):
            testcases += 1
            testcase = {
                "description": "organization ID: {}, project ID: {}, "
                "cluster ID: {}"
                .format(str(combination[0]), str(combination[1]),
                        str(combination[2])),
                "organizationID": combination[0],
                "projectID": combination[1],
                "clusterID": combination[2]
            }
            if not (combination[0] == self.organisation_id and
                    combination[1] == self.project_id and
                    combination[2] == self.cluster_id):
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
                elif combination[2] != self.cluster_id:
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = {
                        "code": 4025,
                        "hint": "The requested cluster details could not be "
                                "found or fetched. Please ensure that the "
                                "correct cluster ID is provided.",
                        "message": "Unable to fetch the cluster details.",
                        "httpStatusCode": 404
                    }
                else:
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
            self.log.info("Executing test: {}".format(testcase["description"]))
            if "param" in testcase:
                kwarg = {testcase["param"]: testcase["paramValue"]}
            else:
                kwarg = dict()

            result = (self.capellaAPI.cluster_ops_apis
                      .post_private_endpoint_command(
                        testcase["organizationID"], testcase["projectID"],
                        testcase["clusterID"], self.expected_res, **kwarg))
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = (self.capellaAPI.cluster_ops_apis
                          .post_private_endpoint_command(
                            testcase["organizationID"], testcase["projectID"],
                            testcase["clusterID"], self.expected_res, **kwarg))
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_payload(self):
        testcases = list()
        for key in self.expected_res:
            if key in ["command"]:
                continue
            values = [
                "", 1, 0, 100000, -1, 123.123, None,
                self.generate_random_string(special_characters=False),
                self.generate_random_string(5000, special_characters=False),
            ]
            for value in values:
                testcase = copy.deepcopy(self.expected_res)
                testcase[key] = value
                testcase["desc"] = "Testing `{}` with val: {} of {}" \
                    .format(key, value, type(value))
                if key == "vpcID" and (value == "" or value is None):
                    testcase["expected_status_code"] = 400
                    testcase["expected_error"] = {
                        "code": 1000,
                        "hint": "The request was malformed or invalid.",
                        "httpStatusCode": 400,
                        "message": "Bad Request. Error: The VPC ID provided "
                                   "is invalid.."
                    }
                elif key == "subnetIDs" and value is None :
                    testcase["expected_status_code"] = 400
                    testcase["expected_error"] = {
                        "code": 1000,
                        "hint": "The request was malformed or invalid.",
                        "httpStatusCode": 400,
                        "message": "Bad Request. Error: The Subnet ID provided "
                                   "is invalid.."
                    }
                elif (
                        key == "vpcID" and not isinstance(value, str) or
                        key == "subnetIDs" and not isinstance(value, list) or
                        key == "resourceGroupName" and not isinstance(
                        value, str) or key == "virtualNetwork" and not
                        isinstance(value, str)
                ):
                    testcase["expected_status_code"] = 400
                    testcase["expected_error"] = {
                        "code": 1000,
                        "hint": "The request was malformed or invalid.",
                        "httpStatusCode": 400,
                        "message": "Bad Request. Error: body contains "
                                   "incorrect JSON type for field "
                                   "\"{}\".".format(key)
                    }
                elif key == "virtualNetwork" and not re.match(
                        value, r'^[A-Za-z0-9_-]+/[A-Za-z0-9_-]+$'):
                    testcase["expected_status_code"] = 422
                    testcase["expected_error"] = {
                        "code": 422,
                        "hint": "Please review your request and ensure that "
                                "all required parameters are correctly "
                                "provided.",
                        "httpStatusCode": 422,
                        "message": "Malformed subnet input format, expected "
                                   "'VirtualNetworkName/SubnetName' but got "
                                   "{}.".format(value)
                    }
                testcases.append(testcase)
        failures = list()
        for testcase in testcases:
            payload = {key: testcase[key] for key in self.expected_res.keys()}
            self.log.info(testcase['desc'])
            result = (self.capellaAPI.cluster_ops_apis
                      .post_private_endpoint_command(
                        self.organisation_id, self.project_id, self.cluster_id,
                        payload))
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = (self.capellaAPI.cluster_ops_apis
                          .post_private_endpoint_command(
                            self.organisation_id, self.project_id,
                            self.cluster_id, payload))
            self.validate_testcase(result, [200], testcase, failures,
                                   payloadTest=True)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.post_private_endpoint_command, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.expected_res
            )
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.post_private_endpoint_command, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.expected_res
            )
        ]]
        self.throttle_test(api_func_list, True, self.project_id)
