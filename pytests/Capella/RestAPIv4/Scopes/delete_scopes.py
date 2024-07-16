"""
Created on December 6, 2023

@author: Vipul Bhardwaj
"""

from pytests.Capella.RestAPIv4.Scopes.get_scopes import GetScope


class DeleteScope(GetScope):

    def setUp(self, nomenclature="Scopes_Delete"):
        GetScope.setUp(self, nomenclature)

    def tearDown(self):
        super(DeleteScope, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Delete a valid scope"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}/buckets/"
                       "{}/scopes",
                "expected_status_code": 404,
                "expected_error": {
                    "errorType": "RouteNotFound",
                    "message": "Not found"
                }
            }, {
                "description": "Replace scopes with scope in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/bucket/"
                       "{}/scope",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/buckets/"
                       "{}/scopes/scope",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Delete scope but with non-hex organizationID",
                "invalid_organizationID": self.replace_last_character(
                    self.organisation_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if you have provided a valid URL and all "
                            "the required params are present in the request "
                            "body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived"
                               " to be a client error."
                }
            }, {
                "description": "Delete scope but with non-hex projectID",
                "invalid_projectID": self.replace_last_character(
                    self.project_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if you have provided a valid URL and all "
                            "the required params are present in the request "
                            "body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived"
                               " to be a client error."
                }
            }, {
                "description": "Delete scope but with non-hex clusterID",
                "invalid_clusterID": self.replace_last_character(
                    self.cluster_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if you have provided a valid URL and all "
                            "the required params are present in the request "
                            "body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived"
                               " to be a client error."
                }
            }, {
                "description": "Delete scope but with invalid bucketID",
                "invalid_bucketID": self.replace_last_character(
                    self.bucket_id),
                "expected_status_code": 404,
                "expected_error": {
                    "code": 6008,
                    "hint": "The requested bucket does not exist. Please "
                            "ensure that the correct bucket ID is provided.",
                    "httpStatusCode": 404,
                    "message": "Unable to find the specified bucket."
                }
            }, {
                "description": "Delete scope but with invalid scopeName",
                "invalid_scopeName": self.replace_last_character(
                    self.scope_name),
                "expected_status_code": 404,
                "expected_error": {
                    "code": 11002,
                    "hint": "The requested scope details could not be found "
                            "or fetched. Please ensure that the correct scope "
                            "name is provided.",
                    "httpStatusCode": 404,
                    "message": "Scope Not Found"
                }
            }
        ]
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            org = self.organisation_id
            proj = self.project_id
            clus = self.cluster_id
            buck = self.bucket_id
            scope = self.scope_name

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.scope_endpoint = \
                    testcase["url"]
            if "invalid_organizationID" in testcase:
                org = testcase["invalid_organizationID"]
            elif "invalid_projectID" in testcase:
                proj = testcase["invalid_projectID"]
            elif "invalid_clusterID" in testcase:
                clus = testcase["invalid_clusterID"]
            elif "invalid_bucketID" in testcase:
                buck = testcase["invalid_bucketID"]
            elif "invalid_scopeName" in testcase:
                scope = testcase["invalid_scopeName"]

            result = self.capellaAPI.cluster_ops_apis.delete_scope(
                org, proj, clus, buck, scope)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.delete_scope(
                    org, proj, clus, buck, scope)

            self.capellaAPI.cluster_ops_apis.scope_endpoint = "/v4/" \
                "organizations/{}/projects/{}/clusters/{}/buckets/{}/scopes"

            if self.validate_testcase(result, [204], testcase, failures):
                self.log.debug("Deletion Successful.")
                self.scope_name = self.create_scope_to_be_tested(
                    self.organisation_id, self.project_id,
                    self.cluster_id, self.bucket_id)
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
                    "code": 1003,
                    "hint": "Make sure you have adequate access to the "
                            "resource.",
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
            result = self.capellaAPI.cluster_ops_apis.delete_scope(
                self.organisation_id, self.project_id, self.cluster_id,
                self.bucket_id, self.scope_name, header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.delete_scope(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.scope_name, header)

            if self.validate_testcase(result, [204], testcase, failures):
                self.log.debug("Deletion Successful.")
                self.scope_name = self.create_scope_to_be_tested(
                    self.organisation_id, self.project_id,
                    self.cluster_id, self.bucket_id)

        self.update_auth_with_api_token(self.curr_owner_key)
        resp = self.capellaAPI.org_ops_apis.delete_project(
            self.organisation_id, other_project_id)
        if resp.status_code != 204:
            failures.append("Error while deleting project {}"
                            .format(other_project_id))

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_query_parameters(self):
        self.log.debug("Correct Params - OrgID: {}, ProjID: {}, ClusID: {}, Bu"
                       "ckID: {}, ScopeName: {}".format(
                        self.organisation_id, self.project_id, self.cluster_id,
                        self.bucket_id, self.scope_name))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id, self.cluster_id,
                self.bucket_id, self.scope_name):
            testcases += 1
            testcase = {
                "description": "OrganizationID: {}, ProjectID: {}, "
                               "ClusterID: {}, BucketID: {}, ScopeName: {}"
                .format(str(combination[0]), str(combination[1]),
                        str(combination[2]), str(combination[3]),
                        str(combination[4])),
                "organizationID": combination[0],
                "projectID": combination[1],
                "clusterID": combination[2],
                "bucketID": combination[3],
                "scopeName": combination[4]
            }
            if not (combination[0] == self.organisation_id and
                    combination[1] == self.project_id and
                    combination[2] == self.cluster_id and
                    combination[3] == self.bucket_id and
                    combination[4] == self.scope_name):
                if (combination[1] == "" or combination[0] == "" or
                        combination[2] == "" or combination[3] == "" or
                        combination[4] == ""):
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = "404 page not found"
                elif any(variable in [
                        int, bool, float, list, tuple, set, type(None)] for
                             variable in [type(combination[0]),
                                          type(combination[1]),
                                          type(combination[2])]):
                    testcase["expected_status_code"] = 400
                    testcase["expected_error"] = {
                        "code": 1000,
                        "hint": "Check if all the required params are present "
                                "in the request body.",
                        "message": "The server cannot or will not process the "
                                   "request due to something that is perceived"
                                   " to be a client error.",
                        "httpStatusCode": 400
                    }
                elif combination[0] != self.organisation_id:
                    testcase["expected_status_code"] = 403
                    testcase["expected_error"] = {
                        "code": 1002,
                        "hint": "Your access to the requested resource is "
                                "denied. Please make sure you have the "
                                "necessary permissions to access the "
                                "resource.",
                        "message": "Access Denied.",
                        "httpStatusCode": 403
                    }
                elif combination[3] != self.bucket_id and not \
                        isinstance(combination[3], type(None)):
                    testcase["expected_status_code"] = 400
                    testcase["expected_error"] = {
                        "code": 400,
                        "hint": "Please review your request and ensure that "
                                "all required parameters are correctly "
                                "provided.",
                        "message": "BucketID is invalid.",
                        "httpStatusCode": 400
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
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = {
                        "code": 11002,
                        "hint": "The requested scope details could not be "
                                "found or fetched. Please ensure that the "
                                "correct scope name is provided.",
                        "httpStatusCode": 404,
                        "message": "Scope Not Found"
                    }
            self.log.info("Executing test: {}".format(testcase["description"]))
            if "param" in testcase:
                kwarg = {testcase["param"]: testcase["paramValue"]}
            else:
                kwarg = dict()

            result = self.capellaAPI.cluster_ops_apis.delete_scope(
                testcase["organizationID"], testcase["projectID"],
                testcase["clusterID"], testcase["bucketID"],
                testcase["scopeName"], **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.delete_scope(
                    testcase["organizationID"], testcase["projectID"],
                    testcase["clusterID"], testcase["bucketID"],
                    self.scope_name, **kwarg)

            if self.validate_testcase(result, [204], testcase, failures):
                self.log.debug("Deletion Successful.")
                self.scope_name = self.create_scope_to_be_tested(
                    self.organisation_id, self.project_id,
                    self.cluster_id, self.bucket_id)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        """
        Scope deletion requests here have an empty name on purpose.
        And we want to see the erroneous response and handle it as a
        success to the API calls sent.
        """
        api_func_list = [[self.capellaAPI.cluster_ops_apis.delete_scope,
                          (self.organisation_id, self.project_id,
                           self.cluster_id, self.bucket_id, "")]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        """
        Scope deletion requests here have an empty name on purpose.
        And we want to see the erroneous response and handle it as a
        success to the API calls sent.
        """
        api_func_list = [[self.capellaAPI.cluster_ops_apis.delete_scope,
                          (self.organisation_id, self.project_id,
                           self.cluster_id, self.bucket_id, "")]]
        self.throttle_test(api_func_list, True, self.project_id)
