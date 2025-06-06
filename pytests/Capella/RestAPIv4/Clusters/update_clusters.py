"""
Created on August 31, 2023

@author: Vipul Bhardwaj
"""

from couchbase_utils.capella_utils.dedicated import CapellaUtils
from pytests.Capella.RestAPIv4.Projects.get_projects import GetProject


class UpdateCluster(GetProject):

    def setUp(self, nomenclature="Clusters_Update"):
        GetProject.setUp(self, nomenclature)

        self.dummy_cluster_id = "aaaaaaaa-ffff-ffff-ffff-cccccccccccc"
        cluster_name = self.prefix + nomenclature
        """
        Supplying fake/non-existent cluster-ID Parameter in PUT tests, 
        and tests will be verified based on a correct request being sent 
        where the expected response would be : 
        {
            "code": 4025,
            "hint": "The requested cluster details could not be found 
                    or fetched. Please ensure that the correct cluster 
                    ID is provided.",
            "httpStatusCode": 404,
            "message": "Unable to fetch the cluster details."
        }
        """
        self.expected_result = {
            "name": cluster_name,
            "description": None,
            "cloudProvider": {
                "type": "aws",
                "region": "us-east-1",
                "cidr": CapellaUtils.get_next_cidr() + "/20"
            },
            "couchbaseServer": {
                "version": str(self.input.param("server_version", 7.6))
            },
            "serviceGroups": [
                {
                    "node": {
                        "compute": {
                            "cpu": 4,
                            "ram": 16
                        },
                        "disk": {
                            "storage": 50,
                            "type": "gp3",
                            "iops": 3000,
                            "autoExpansion": "on"
                        }
                    },
                    "numOfNodes": 3,
                    "services": [
                        "data"
                    ]
                }
            ],
            "availability": {
                "type": "single"
            },
            "support": {
                "plan": "basic",
                "timezone": "GMT"
            },
            "currentState": None,
            "audit": {
                "createdBy": None,
                "createdAt": None,
                "modifiedBy": None,
                "modifiedAt": None,
                "version": None
            }
        }

    def tearDown(self):
        self.update_auth_with_api_token(self.curr_owner_key)
        super(UpdateCluster, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
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
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Fetch cluster but with non-hex organizationID",
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
                "description": "Fetch cluster but with non-hex projectID",
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
            clus = self.dummy_cluster_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.cluster_endpoint = \
                    testcase["url"]
            if "invalid_organizationID" in testcase:
                org = testcase["invalid_organizationID"]
            elif "invalid_projectID" in testcase:
                proj = testcase["invalid_projectID"]

            result = self.capellaAPI.cluster_ops_apis.update_cluster(
                org, proj, clus, self.expected_result["name"],
                self.expected_result['description'],
                self.expected_result['support'],
                self.expected_result['serviceGroups'], False)
            self.capellaAPI.cluster_ops_apis.cluster_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters"
            self.validate_testcase(result, [404], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner", "projectManager"
        ], 403, {
            "code": 4025,
            "hint": "The requested cluster details could not be found "
                    "or fetched. Please ensure that the correct "
                    "cluster ID is provided.",
            "httpStatusCode": 404,
            "message": "Unable to fetch the cluster details."
        }):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = self.capellaAPI.cluster_ops_apis.update_cluster(
                self.organisation_id, self.project_id, self.dummy_cluster_id,
                self.expected_result["name"],
                self.expected_result['description'],
                self.expected_result['support'],
                self.expected_result['serviceGroups'], False, header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.update_cluster(
                    self.organisation_id, self.project_id,
                    self.dummy_cluster_id, self.expected_result["name"],
                    self.expected_result['description'],
                    self.expected_result['support'],
                    self.expected_result['serviceGroups'], False, header)
            self.validate_testcase(result, [404], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_query_parameters(self):
        self.log.debug("Correct Params - OrgID: {}, ProjID: {}, "
                       "dummy ClusID: {}"
                       .format(self.organisation_id, self.project_id,
                               self.dummy_cluster_id))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id, self.dummy_cluster_id):
            testcases += 1
            testcase = {
                "expected_status_code": 404,
                "expected_error": {
                    "code": 4025,
                    "hint": "The requested cluster details could not be found "
                            "or fetched. Please ensure that the correct "
                            "cluster ID is provided.",
                    "httpStatusCode": 404,
                    "message": "Unable to fetch the cluster details."
                },
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
                    combination[2] == self.dummy_cluster_id):
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
                elif combination[2] != self.dummy_cluster_id:
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

            result = self.capellaAPI.cluster_ops_apis.update_cluster(
                testcase["organizationID"], testcase["projectID"],
                testcase["clusterID"], self.expected_result["name"],
                self.expected_result['description'],
                self.expected_result['support'],
                self.expected_result['serviceGroups'], False, **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.update_cluster(
                    testcase["organizationID"], testcase["projectID"],
                    testcase["clusterID"], self.expected_result["name"],
                    self.expected_result['description'],
                    self.expected_result['support'],
                    self.expected_result['serviceGroups'], False, **kwarg)

            self.validate_testcase(result, [404], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[self.capellaAPI.cluster_ops_apis.update_cluster,
                          (self.organisation_id, self.project_id,
                           self.dummy_cluster_id, self.expected_result["name"],
                           self.expected_result['description'],
                           self.expected_result['support'],
                           self.expected_result['serviceGroups'], False)]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[self.capellaAPI.cluster_ops_apis.update_cluster,
                          (self.organisation_id, self.project_id,
                           self.dummy_cluster_id, self.expected_result["name"],
                           self.expected_result['description'],
                           self.expected_result['support'],
                           self.expected_result['serviceGroups'], False)]]
        self.throttle_test(api_func_list, True, self.project_id)
