"""
Created on August 02, 2023

@author: Vipul Bhardwaj
"""

from pytests.Capella.RestAPIv4.Clusters.get_clusters import GetCluster


class ListCluster(GetCluster):

    def setUp(self, nomenclature="Clusters_List"):
        GetCluster.setUp(self, nomenclature)
        self.expected_res = {
            "cursor": {
                "hrefs": {
                    "first": None,
                    "last": None,
                    "next": None,
                    "previous": None
                },
                "pages": {
                    "last": None,
                    "next": None,
                    "page": None,
                    "perPage": None,
                    "previous": None,
                    "totalItems": None
                }
            },
            "data": [
                self.expected_res
            ]
        }

    def tearDown(self):
        super(ListCluster, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "List clusters with valid path, params."
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }, {
                "description": "Replace clusters with cluster in URI",
                "url": "/v4/organizations/{}/projects/{}/cluster",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/cluster",
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
                "description": "List clusters with non-hex organizationID",
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
                               "request due to something that is perceived to "
                               "be a client error."
                }
            }, {
                "description": "List clusters with non-hex projectID",
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
                               "request due to something that is perceived to "
                               "be a client error."
                }
            }
        ]
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            org = self.organisation_id
            proj = self.project_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.cluster_endpoint = \
                    testcase["url"]
            if "invalid_organizationID" in testcase:
                org = testcase["invalid_organizationID"]
            elif "invalid_projectID" in testcase:
                proj = testcase["invalid_projectID"]

            result = self.capellaAPI.cluster_ops_apis.list_clusters(org, proj)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.list_clusters(
                    org, proj)
            self.capellaAPI.cluster_ops_apis.cluster_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters"
            self.validate_testcase(result, [200], testcase, failures, True,
                                   self.expected_res, self.cluster_id)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectDataReader", "projectOwner",
            "projectDataReaderWriter", "projectViewer", "projectManager"
        ]):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = self.capellaAPI.cluster_ops_apis.list_clusters(
                self.organisation_id, self.project_id, headers=header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.list_clusters(
                    self.organisation_id, self.project_id, headers=header)
            self.validate_testcase(result, [200], testcase, failures, True,
                                   self.expected_res, self.cluster_id)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_query_parameters(self):
        self.log.debug("Correct Params - OrgID: {}, ProjID: {}"
                       .format(self.organisation_id, self.project_id))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id):
            testcases += 1
            testcase = {
                "description": "OrganizationID: {}, ProjectID: {}".format(
                    str(combination[0]), str(combination[1])),
                "organizationID": combination[0],
                "projectID": combination[1]
            }
            if not (combination[0] == self.organisation_id and
                    combination[1] == self.project_id):
                if combination[0] == "":
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = "404 page not found"
                elif combination[1] == "" or any(variable in [
                    int, bool, float, list, tuple, set, type(None)] for
                         variable in [type(combination[0]),
                                      type(combination[1])]):
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
                        "message": "Access Denied.",
                        "httpStatusCode": 403
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

            result = self.capellaAPI.cluster_ops_apis.list_clusters(
                testcase["organizationID"], testcase["projectID"], **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.list_clusters(
                    testcase["organizationID"], testcase["projectID"], **kwarg)

            self.validate_testcase(result, [200], testcase, failures, True,
                                   self.expected_res, self.cluster_id)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[self.capellaAPI.cluster_ops_apis.list_clusters,
                          (self.organisation_id, self.project_id)]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[self.capellaAPI.cluster_ops_apis.list_clusters,
                          (self.organisation_id, self.project_id)]]
        self.throttle_test(api_func_list, True, self.project_id)
