"""
Created on May 29, 2024

@author: Created using cbRAT cbModule by Vipul Bhardwaj
"""

from pytests.Capella.RestAPIv4.ClustersColumnar.get_analytics_clusters import \
    GetAnalyticsClusters


class PostAnalyticsClusters(GetAnalyticsClusters):

    def setUp(self, nomenclature="ClustersColumnar_POST"):
        GetAnalyticsClusters.setUp(self, nomenclature)

    def tearDown(self):
        super(PostAnalyticsClusters, self).tearDown()

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
                self.columnarAPI.analytics_clusters_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]

            result = self.columnarAPI.create_analytics_cluster(
                organization, project,
                self.expected_res["name"], self.expected_res["cloudProvider"],
                self.expected_res["compute"], self.expected_res["region"],
                self.expected_res["nodes"], self.expected_res["support"],
                self.expected_res["availability"])
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.columnarAPI.create_analytics_cluster(
                    organization, project, self.expected_res["name"],
                    self.expected_res["cloudProvider"],
                    self.expected_res["compute"], self.expected_res["region"],
                    self.expected_res["nodes"], self.expected_res["support"],
                    self.expected_res["availability"])

            self.columnarAPI.analytics_clusters_endpoint = \
                "/v4/organizations/{}/projects/{}/analyticsClusters"

            if self.validate_testcase(result, [202], testcase, failures):
                self.log.debug("Creation Successful")
            if result.status_code == 202:
                self.instances.append(result.json()["id"])

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
            result = self.columnarAPI.create_analytics_cluster(
                self.organisation_id, self.project_id,
                self.expected_res["name"], self.expected_res["cloudProvider"],
                self.expected_res["compute"], self.expected_res["region"],
                self.expected_res["nodes"], self.expected_res["support"],
                self.expected_res["availability"],
                headers=header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.columnarAPI.create_analytics_cluster(
                    self.organisation_id, self.project_id,
                    self.expected_res["name"],
                    self.expected_res["cloudProvider"],
                    self.expected_res["compute"], self.expected_res["region"],
                    self.expected_res["nodes"], self.expected_res["support"],
                    self.expected_res["availability"],
                    headers=header)

            if self.validate_testcase(result, [202], testcase, failures):
                self.log.debug("Creation Successful")
            if result.status_code == 202:
                self.instances.append(result.json()["id"])

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
                if combination[1] == "":
                    testcase["expected_status_code"] = 405
                    testcase["expected_error"] = ""
                elif combination[0] == "":
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = "404 page not found"
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

            result = self.columnarAPI.create_analytics_cluster(
                testcase["organizationID"], testcase["projectID"],
                self.expected_res["name"], self.expected_res["cloudProvider"],
                self.expected_res["compute"], self.expected_res["region"],
                self.expected_res["nodes"], self.expected_res["support"],
                self.expected_res["availability"],
                **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.columnarAPI.create_analytics_cluster(
                    testcase["organizationID"], testcase["projectID"],
                    self.expected_res["name"],
                    self.expected_res["cloudProvider"],
                    self.expected_res["compute"], self.expected_res["region"],
                    self.expected_res["nodes"], self.expected_res["support"],
                    self.expected_res["availability"],
                    **kwarg)

            if self.validate_testcase(result, [202], testcase, failures):
                self.log.debug("Creation Successful")
            if result.status_code == 202:
                self.instances.append(result.json()["id"])

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.columnarAPI.create_analytics_cluster, (
                self.organisation_id, self.project_id,
                self.expected_res["name"], self.expected_res["cloudProvider"],
                self.expected_res["compute"], self.expected_res["region"],
                self.expected_res["nodes"], self.expected_res["support"],
                self.expected_res["availability"]
            )
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.columnarAPI.create_analytics_cluster, (
                self.organisation_id, self.project_id,
                self.expected_res["name"], self.expected_res["cloudProvider"],
                self.expected_res["compute"], self.expected_res["region"],
                self.expected_res["nodes"], self.expected_res["support"],
                self.expected_res["availability"]
            )
        ]]
        self.throttle_test(api_func_list, True, self.project_id)
