"""
Created on February 6, 2024

@author: Vipul Bhardwaj
"""

from pytests.Capella.RestAPIv4.Clusters.get_clusters import GetCluster

class ClusterOn(GetCluster):

    def setUp(self, nomenclature="Switch_Cluster_On"):
        GetCluster.setUp(self, nomenclature)

    def tearDown(self):
        self.update_auth_with_api_token(self.org_owner_key["token"])
        super(ClusterOn, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Turn on a valid cluster",
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}/on",
                "expected_status_code": 404,
                "expected_error": {
                    "errorType": "RouteNotFound",
                    "message": "Not found"
                }
            }, {
                "description": "Replace clusters with cluster in URI",
                "url": "/v4/organizations/{}/projects/{}/cluster/{}/on",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/cluster/{}/on",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Switch cluster on but with non-hex "
                               "organizationID",
                "invalid_organizationID": self.replace_last_character(
                    self.organisation_id, non_hex=True),
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Switch cluster on but with non-hex projectID",
                "invalid_projectID": self.replace_last_character(
                    self.project_id, non_hex=True),
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Switch cluster on but with non-hex clusterID",
                "invalid_clusterID": self.replace_last_character(
                    self.cluster_id, non_hex=True),
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }
        ]
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            org = self.organisation_id
            proj = self.project_id
            clus = self.cluster_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.cluster_on_off_endpoint = \
                    testcase["url"]
            if "invalid_organizationID" in testcase:
                org = testcase["invalid_organizationID"]
            elif "invalid_projectID" in testcase:
                proj = testcase["invalid_projectID"]
            elif "invalid_clusterID" in testcase:
                clus = testcase["invalid_clusterID"]

            result = self.capellaAPI.cluster_ops_apis.switch_cluster_on(
                org, proj, clus, False)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.switch_cluster_on(
                    org, proj, clus, False)
            if result.status_code in [409, 202]:
                if not self.validate_onoff_state(
                        ["turningOn", "healthy"],
                        self.project_id, self.cluster_id):
                    self.log.error("Status == 409, Key validation Failure "
                                   ": {}".format(testcase["description"]))
                    self.log.warning("Result : {}".format(result.json()))
                    failures.append(testcase["description"])
            else:
                self.validate_testcase(result, 409, testcase, failures)

            self.capellaAPI.cluster_ops_apis.cluster_on_off_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters/{}/on"

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
            if not any(element in ["organizationOwner",
                                   "projectOwner", "projectManager"] for
                       element in self.api_keys[role]["roles"]):
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
            result = self.capellaAPI.cluster_ops_apis.switch_cluster_on(
                self.organisation_id, self.project_id, self.cluster_id,
                False, headers=header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.switch_cluster_on(
                    self.organisation_id, self.project_id, self.cluster_id,
                    False, headers=header)
            if result.status_code in [409, 202]:
                if not self.validate_onoff_state(
                        ["turningOn", "healthy"],
                        self.project_id, self.cluster_id):
                    self.log.error("Status == 409, Key validation Failure "
                                   ": {}".format(testcase["description"]))
                    self.log.warning("Result : {}".format(result.json()))
                    failures.append(testcase["description"])
            else:
                self.validate_testcase(result, 409, testcase, failures)

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
        self.log.debug("Correct Params - OrgID: {}, ProjID: {}, ClusID: {}"
                       .format(self.organisation_id, self.project_id,
                               self.cluster_id))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id, self.cluster_id):
            testcases += 1
            testcase = {
                "description": "OrganizationID: {}, ProjectID: {}, "
                               "ClusterID: {}".format(str(combination[0]),
                                                      str(combination[1]),
                                                      str(combination[2])),
                "organizationID": combination[0],
                "projectID": combination[1],
                "clusterID": combination[2]
            }
            if not (combination[0] == self.organisation_id and
                    combination[1] == self.project_id and
                    combination[2] == self.cluster_id):
                if combination[1] == "" or combination[0] == "" or \
                        combination[2] == "":
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
                        "hint": "Check if all the required params are "
                                "present in the request body.",
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

            result = self.capellaAPI.cluster_ops_apis.switch_cluster_on(
                testcase["organizationID"], testcase["projectID"],
                testcase["clusterID"], False, **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.switch_cluster_on(
                    testcase["organizationID"], testcase["projectID"],
                    testcase["clusterID"], False, **kwarg)
            if result.status_code in [409, 202]:
                if not self.validate_onoff_state(
                        ["turningOn", "healthy"],
                        self.project_id, self.cluster_id):
                    self.log.error("Status == 409, Key validation Failure "
                                   ": {}".format(testcase["description"]))
                    self.log.warning("Result : {}".format(result.json()))
                    failures.append(testcase["description"])
            else:
                self.validate_testcase(result, 409, testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_payload(self):
        failures = list()
        testcases = 0
        for val in [True, 100, None, "abc", 1.2, [True], -1]:
            testcases += 1
            testcase = {
                "description": "Testing payload with value `{}` of {}"
                .format(val, type(val))
            }
            if type(val) is not bool:
                testcase["expected_status_code"] = 400
                testcase["expected_error"] = {
                    "code": 1000,
                    "hint": "The request was malformed or invalid.",
                    "httpStatusCode": 400,
                    "message": "Bad Request. Error: body contains incorrect "
                               "JSON type for field "
                               "\"turnOnLinkedAppService\"."
                }
            self.log.info("Executing test: {}".format(testcase["description"]))
            result = self.capellaAPI.cluster_ops_apis.switch_cluster_on(
                self.organisation_id, self.project_id, self.cluster_id, val)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.switch_cluster_on(
                    self.organisation_id, self.project_id, self.cluster_id, val)
            if result.status_code in [409, 202]:
                if not self.validate_onoff_state(
                        ["turningOn", "healthy"],
                        self.project_id, self.cluster_id):
                    self.log.error("Status == {}, Key validation Failure : {}"
                                   .format(result.status_code,
                                           testcase["description"]))
                    self.log.warning("Result : {}".format(result.json()))
                    failures.append(testcase["description"])
            else:
                self.validate_testcase(result, 409, testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[self.capellaAPI.cluster_ops_apis.switch_cluster_on,
                          (self.organisation_id, self.project_id,
                           self.cluster_id, False)]]

        for i in range(self.input.param("num_api_keys", 1)):
            resp = self.capellaAPI.org_ops_apis.create_api_key(
                self.organisation_id,
                self.generate_random_string(prefix=self.prefix),
                ["organizationOwner"], self.generate_random_string(50))
            if resp.status_code == 429:
                self.handle_rate_limit(int(resp.headers["Retry-After"]))
                resp = self.capellaAPI.org_ops_apis.create_api_key(
                    self.organisation_id,
                    self.generate_random_string(prefix=self.prefix),
                    ["organizationOwner"], self.generate_random_string(50))
            if resp.status_code == 201:
                self.api_keys["organizationOwner_{}".format(i)] = resp.json()
            else:
                self.fail("Error while creating API key for "
                          "organizationOwner_{}".format(i))

        if self.input.param("rate_limit", False):
            results = self.make_parallel_api_calls(
                310, api_func_list, self.api_keys)
            for result in results:
                if ((not results[result]["rate_limit_hit"])
                        or results[result][
                            "total_api_calls_made_to_hit_rate_limit"] > 300):
                    self.fail(
                        "Rate limit was hit after {0} API calls. "
                        "This is definitely an issue.".format(
                            results[result][
                                "total_api_calls_made_to_hit_rate_limit"]
                        ))

        results = self.make_parallel_api_calls(
            99, api_func_list, self.api_keys)
        for result in results:
            # Removing failure for tests which are intentionally ran
            # for :
            #   # unauthorized roles, ie, which give a 403 response.
            if "403" in results[result]["4xx_errors"]:
                del results[result]["4xx_errors"]["403"]
            #   # conflicting state of the resource, ie, which give a
            #   409 response.
            if "409" in results[result]["4xx_errors"]:
                del results[result]["4xx_errors"]["409"]

            if len(results[result]["4xx_errors"]) > 0 or len(
                    results[result]["5xx_errors"]) > 0:
                self.fail("Some API calls failed")

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[self.capellaAPI.cluster_ops_apis.switch_cluster_on,
                          (self.organisation_id, self.project_id,
                           self.cluster_id, False)]]

        org_roles = self.input.param("org_roles", "organizationOwner")
        proj_roles = self.input.param("proj_roles", "projectDataReader")
        org_roles = org_roles.split(":")
        proj_roles = proj_roles.split(":")

        api_key_dict = self.create_api_keys_for_all_combinations_of_roles(
            [self.project_id], proj_roles, org_roles)
        for i, api_key in enumerate(api_key_dict):
            if api_key in self.api_keys:
                self.api_keys["{}_{}".format(api_key_dict[api_key], i)] = \
                    api_key_dict[api_key]
            else:
                self.api_keys[api_key] = api_key_dict[api_key]

        if self.input.param("rate_limit", False):
            results = self.make_parallel_api_calls(
                310, api_func_list, self.api_keys)
            for result in results:
                if ((not results[result]["rate_limit_hit"])
                        or results[result][
                            "total_api_calls_made_to_hit_rate_limit"] > 300):
                    self.fail(
                        "Rate limit was hit after {0} API calls. "
                        "This is definitely an issue.".format(
                            results[result][
                                "total_api_calls_made_to_hit_rate_limit"]
                        ))

        results = self.make_parallel_api_calls(
            99, api_func_list, self.api_keys)
        for result in results:
            # Removing failure for tests which are intentionally ran
            # for :
            #   # unauthorized roles, ie, which give a 403 response.
            if "403" in results[result]["4xx_errors"]:
                del results[result]["4xx_errors"]["403"]
            #   # conflicting state of the resource, ie, which give a
            #   409 response.
            if "409" in results[result]["4xx_errors"]:
                del results[result]["4xx_errors"]["409"]

            if len(results[result]["4xx_errors"]) > 0 or len(
                    results[result]["5xx_errors"]) > 0:
                self.fail("Some API calls failed")
