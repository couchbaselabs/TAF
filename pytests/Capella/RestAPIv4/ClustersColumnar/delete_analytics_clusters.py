"""
Created on May 29, 2024

@author: Created using cbRAT cbModule by Vipul Bhardwaj
"""

from pytests.Capella.RestAPIv4.ClustersColumnar.\
    get_analytics_clusters import GetAnalyticsClusters


class DeleteAnalyticsClusters(GetAnalyticsClusters):

    def setUp(self, nomenclature="ClustersColumnar_DELETE"):
        GetAnalyticsClusters.setUp(self, nomenclature)

    def tearDown(self):
        super(DeleteAnalyticsClusters, self).tearDown()

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

            result = self.columnarAPI.delete_analytics_cluster(
                organization, project, analyticsCluster)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.columnarAPI.delete_analytics_cluster(
                    organization, project, analyticsCluster)
            self.columnarAPI.analytics_clusters_endpoint = \
                "/v4/organizations/{}/projects/{}/analyticsClusters"
            if self.validate_testcase(result, [202], testcase, failures):
                self.log.debug("Deletion Successful")
            if result.status_code == 202:
                self.instances.remove(self.analyticsCluster_id)
                self.analyticsCluster_id = self.create_columnar_instance_to_be_tested()

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
            result = self.columnarAPI.delete_analytics_cluster(
                self.organisation_id, self.project_id,
                self.analyticsCluster_id, header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.columnarAPI.delete_analytics_cluster(
                    self.organisation_id, self.project_id,
                    self.analyticsCluster_id, header)
            if self.validate_testcase(result, [202], testcase, failures):
                self.log.debug("Deletion Successful")
            if result.status_code == 202:
                self.instances.remove(self.analyticsCluster_id)
                self.analyticsCluster_id = self.create_columnar_instance_to_be_tested()

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

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

            result = self.columnarAPI.delete_analytics_cluster(
                testcase["organizationID"], testcase["projectID"],
                testcase["analyticsClusterID"],
                **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.columnarAPI.delete_analytics_cluster(
                    testcase["organizationID"], testcase["projectID"],
                    testcase["analyticsClusterID"],
                    **kwarg)

            if self.validate_testcase(result, [202], testcase, failures):
                self.log.debug("Deletion Successful")
            if result.status_code == 202:
                self.instances.remove(self.analyticsCluster_id)
                self.analyticsCluster_id = self.create_columnar_instance_to_be_tested()

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.columnarAPI.delete_analytics_cluster, (
                self.organisation_id, self.project_id, self.analyticsCluster_id
            )
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.columnarAPI.delete_analytics_cluster, (
                self.organisation_id, self.project_id, self.analyticsCluster_id
            )
        ]]
        self.throttle_test(api_func_list, True, self.project_id)
