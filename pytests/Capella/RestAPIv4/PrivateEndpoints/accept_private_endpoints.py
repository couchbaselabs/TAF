"""
Created on June 04, 2024

@author: Created using cbRAT cbModule by Vipul Bhardwaj
"""

from pytests.Capella.RestAPIv4.Clusters.get_clusters import GetCluster


class PostAssociate(GetCluster):

    def setUp(self, nomenclature="PrivateEndpoints_POST"):
        GetCluster.setUp(self, nomenclature)
        self.endpoint_id = "vpce-079a0245094731925"
        if (self.input.param("cluster_template", "AWS_r5_xlarge") ==
                "Azure_E4s_v5"):
            self.expected_code = 404
            self.expected_err = {
                "code": 404,
                "hint": "Please review your request and ensure that all "
                        "required parameters are correctly provided.",
                "httpStatusCode": 404,
                "message": "The VpcEndpointService Id '{}' does not exist"
                           .format(self.endpoint_id)
            }
        else:
            self.expected_code = 400
            self.expected_err = {
                "code": 400,
                "hint": "Please review your request and ensure that all "
                        "required parameters are correctly provided.",
                "httpStatusCode": 400,
                "message": "Invalid endpoint ID. Did you run the connection "
                           "command?"
            }

    def tearDown(self):
        super(PostAssociate, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Send call with valid path params",
                "expected_error": self.expected_err,
                "expected_status_code": self.expected_code
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}/privateEndpointService/endpoints/{}/associate",
                "expected_status_code": 404,
                "expected_error": {
                    "errorType": "RouteNotFound",
                    "message": "Not found"
                }
            }, {
                "description": "Replace the last path param name in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/privateEndpointService/endpoints/{}/associat",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/privateEndpointService/endpoints/{}/associate/associat",
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
            }, {
                "description": "Call API with non-hex endpointId",
                "invalid_endpointId": self.replace_last_character(
                    self.endpoint_id, non_hex=True),
                "expected_error": self.expected_err,
                "expected_status_code": self.expected_code
            }
        ]
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            organization = self.organisation_id
            project = self.project_id
            cluster = self.cluster_id
            endpoint = self.endpoint_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.associate_private_network_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]
            elif "invalid_clusterId" in testcase:
                cluster = testcase["invalid_clusterId"]
            elif "invalid_endpointId" in testcase:
                endpoint = testcase["invalid_endpointId"]

            result = self.capellaAPI.cluster_ops_apis.accept_private_endpoint(
                organization, project, cluster, endpoint)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.accept_private_endpoint(
                    organization, project, cluster, endpoint)
            self.capellaAPI.cluster_ops_apis.associate_private_network_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters/{}/privateEndpointService/endpoints/{}/associate"
            if self.validate_testcase(result, [204], testcase, failures):
                self.log.debug("Association Successful")

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner", "projectManager"
        ], self.expected_code, self.expected_err):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = self.capellaAPI.cluster_ops_apis.accept_private_endpoint(
                self.organisation_id, self.project_id, self.cluster_id,
                self.endpoint_id,
                header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.accept_private_endpoint(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.endpoint_id,
                    header)
            if self.validate_testcase(result, [204], testcase, failures):
                self.log.debug("Association Successful")

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_query_parameters(self):
        self.log.debug(
                "Correct Params - organization ID: {}, project ID: {}, "
                "cluster ID: {}, endpoint ID: {}".format(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.endpoint_id))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id, self.cluster_id,
                self.endpoint_id):
            testcases += 1
            testcase = {
                "description": "organization ID: {}, project ID: {}, "
                "cluster ID: {}, endpoint ID: {}"
                .format(str(combination[0]), str(combination[1]),
                        str(combination[2]), str(combination[3])),
                "organizationID": combination[0],
                "projectID": combination[1],
                "clusterID": combination[2],
                "endpointID": combination[3],
                "expected_error": self.expected_err,
                "expected_status_code": self.expected_code
            }
            if not (combination[0] == self.organisation_id and
                    combination[1] == self.project_id and
                    combination[2] == self.cluster_id and
                    combination[3] == self.endpoint_id):
                if (combination[0] == "" or combination[1] == "" or
                        combination[2] == "" or combination[3] == ""):
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
                else:
                    testcase["expected_status_code"] = self.expected_code
                    testcase["expected_error"] = self.expected_err
            self.log.info("Executing test: {}".format(testcase["description"]))
            if "param" in testcase:
                kwarg = {testcase["param"]: testcase["paramValue"]}
            else:
                kwarg = dict()

            result = self.capellaAPI.cluster_ops_apis.accept_private_endpoint(
                testcase["organizationID"], testcase["projectID"],
                testcase["clusterID"], testcase["endpointID"],
                **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.accept_private_endpoint(
                    testcase["organizationID"], testcase["projectID"],
                    testcase["clusterID"], testcase["endpointID"],
                    **kwarg)
            if self.validate_testcase(result, [204], testcase, failures):
                self.log.debug("Mock Association Successful")

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.accept_private_endpoint, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.endpoint_id
            )
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.accept_private_endpoint, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.endpoint_id
            )
        ]]
        self.throttle_test(api_func_list, True, self.project_id)
