"""
Created on December 7, 2023

@author: Vipul Bhardwaj
"""

from pytests.Capella.RestAPIv4.Scopes.get_scopes import GetScope


class CreateCollection(GetScope):

    def setUp(self, nomenclature="Collections_Create"):
        GetScope.setUp(self, nomenclature)

        # Initialize collection params.
        self.collections = list()

    def tearDown(self):
        self.update_auth_with_api_token(self.curr_owner_key)

        # Delete the Collections that were created.
        if self.flush_collections(self.organisation_id, self.project_id,
                                  self.cluster_id, self.bucket_id,
                                  self.scope_name, self.collections):
            self.log.error("Collections flushing operation could not be "
                           "completed successfully.")

        super(CreateCollection, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Create a valid collection"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}/buckets/"
                       "{}/scopes/{}/collections/",
                "expected_status_code": 404,
                "expected_error": {
                    "errorType": "RouteNotFound",
                    "message": "Not found"
                }
            }, {
                "description": "Replace collections with collection in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/bucket/"
                       "{}/scopes/{}/collection/",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/buckets/"
                       "{}/scopes/{}/collections/collection/",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Create collection but with non-hex "
                               "organizationID",
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
                "description": "Create collection but with non-hex projectID",
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
                "description": "Create collection but with non-hex clusterID",
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
                "description": "Create collection but with invalid bucketID",
                "invalid_bucketID": self.replace_last_character(
                    self.bucket_id),
                "expected_status_code": [400, 404],
                "expected_error": self.expected_bucketID_errors
            }, {
                "description": "Create collection but with invalid scopeName",
                "invalid_scopeName": self.replace_last_character(
                    self.scope_name),
                "expected_status_code": 404,
                "expected_error": {
                    "code": 11002,
                    "hint": "The requested scope details could not be "
                            "found or fetched. Please ensure that the "
                            "correct scope name is provided.",
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
                self.capellaAPI.cluster_ops_apis.collection_endpoint = \
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

            collection_name = self.generate_random_string(
                5, False, self.prefix)
            result = self.capellaAPI.cluster_ops_apis.create_collection(
                org, proj, clus, buck, scope, collection_name)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.create_collection(
                    org, proj, clus, buck, scope, collection_name)
            self.capellaAPI.cluster_ops_apis.collection_endpoint = "/v4/" \
                "organizations/{}/projects/{}/clusters/{}/buckets/{}/scopes" \
                "/{}/collections"
            if self.validate_testcase(result, [201], testcase, failures):
                self.collections.append(collection_name)

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
            collection_name = self.generate_random_string(5, False)
            result = self.capellaAPI.cluster_ops_apis.create_collection(
                self.organisation_id, self.project_id, self.cluster_id,
                self.bucket_id, self.scope_name, collection_name,
                headers=header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.create_collection(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.bucket_id, self.scope_name, collection_name,
                    headers=header)
            if self.validate_testcase(result, [201], testcase, failures):
                self.collections.append(collection_name)
            if len(self.collections) == 1000:
                self.log.warning("Reached 1000 Collections, flushing all.")
                self.update_auth_with_api_token(self.curr_owner_key)
                if self.flush_collections(self.organisation_id,
                                          self.project_id, self.cluster_id,
                                          self.bucket_id, self.scope_name,
                                          self.collections):
                    self.fail("Collections flushing operation could not be "
                              "completed successfully.")

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

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
                        combination[2] == "" or combination[3] == ""):
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = "404 page not found"
                elif combination[4] == "":
                    testcase["expected_status_code"] = 405
                    testcase["expected_error"] = ""
                elif any(variable in [
                    int, bool, float, list, tuple, set, type(None)] for
                     variable in [type(combination[0]), type(combination[1]),
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
                        "hint": "Please review your request and ensure "
                                "that all required parameters are "
                                "correctly provided.",
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

            collection_name = self.generate_random_string(5, False)
            result = self.capellaAPI.cluster_ops_apis.create_collection(
                testcase["organizationID"], testcase["projectID"],
                testcase["clusterID"], testcase["bucketID"],
                testcase["scopeName"], collection_name, **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.create_collection(
                    testcase["organizationID"], testcase["projectID"],
                    testcase["clusterID"], testcase["bucketID"],
                    testcase["scopeName"], collection_name, **kwarg)
            if self.validate_testcase(result, [201], testcase, failures):
                self.collections.append(collection_name)
            if len(self.collections) == 1000:
                self.log.warning("Reached 1000 Collections, flushing all.")
                self.update_auth_with_api_token(self.curr_owner_key)
                if self.flush_collections(
                        self.organisation_id, self.project_id,
                        self.cluster_id, self.bucket_id, self.scope_name,
                        self.collections):
                    self.fail("Collections flushing operation could not "
                              "be completed successfully.")

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_payload(self):
        testcases = list()
        testcases.append({
            "description": "Passing negative maxTTL value.",
            "name": "negative_maxTTL",
            "maxTTL": -1
        })
        for length in [0, 1, 100, 251, 1000]:
            for pre in ["", "_", "%"]:
                name = self.generate_random_string(length, False, pre)
                testcase = {
                    "description": "Testing scope with name {}".format(name),
                    "name": name
                }
                if pre != "" or len(name) == 0 or len(name) > 251:
                    testcase["expected_status_code"] = 400
                    testcase["expected_error"] = {
                        "code": 400,
                        "hint": "Please review your request and ensure that "
                                "all required parameters are correctly "
                                "provided.",
                        "httpStatusCode": 400,
                        "message": "Bad Request"
                    }
                elif testcase["maxTTL"] < 0:
                    testcase["expected_status_code"] = 422
                    testcase["expected_error"] = {
                        "code": 11003,
                        "hint": "Returned when a time-to-live unit given "
                                "during collection creation is not supported. "
                                "This should be a non-negative value.",
                        "httpStatusCode": 422,
                        "message": "The time-to-live value provided is not "
                                   "supported. It should be a non-negative "
                                   "integer."
                    }
                testcases.append(testcase)

        failures = list()
        for testcase in testcases:
            self.log.info(testcase['description'])

            res = self.capellaAPI.cluster_ops_apis.create_collection(
                self.organisation_id, self.project_id, self.cluster_id,
                self.bucket_id, self.scope_name, testcase["name"])
            if res.status_code == 429:
                res = self.capellaAPI.cluster_ops_apis.create_collection(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.bucket_id, self.scope_name, testcase["name"])
            if self.validate_testcase(res, [201], testcase, failures):
                self.collections.append(testcase["name"])
            if len(self.collections) == 1000:
                self.log.warning("Reached 1000 Collections, flushing all.")
                self.update_auth_with_api_token(self.curr_owner_key)
                if self.flush_collections(self.organisation_id,
                                          self.project_id, self.cluster_id,
                                          self.bucket_id, self.scope_name,
                                          self.collections):
                    self.fail("Collections flushing operation could not be "
                              "completed successfully.")

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        """
        Collection creation requests here have an empty name on purpose.
        And we want to see the erroneous response and handle it as a
        success to the API calls sent.
        """
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.create_collection,
            (self.organisation_id, self.project_id, self.cluster_id,
             self.bucket_id, self.scope_name, "")
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        """
        Collection creation requests here have an empty name on purpose.
        And we want to see the erroneous response and handle it as a
        success to the API calls sent.
        """
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.create_collection,
            (self.organisation_id, self.project_id, self.cluster_id,
             self.bucket_id, self.scope_name, "")
        ]]
        self.throttle_test(api_func_list, True, self.project_id)
