"""
Created on February 11, 2025

@author: Created using cbRAT cbModule by Vipul Bhardwaj
"""

import time
from pytests.Capella.RestAPIv4.Projects.get_projects import GetProject


class GetFreeTier(GetProject):

    def setUp(self, nomenclature="FreeTier_GET"):
        GetProject.setUp(self, nomenclature)
        cluster_template = self.input.param("free_tier_template",
                                            "AWS_free_tier")
        self.expected_res = {
            "name": self.prefix + cluster_template,
            "description": "",
            "cloudProvider": {
                "cidr": "10.1.0.0/20",
                "region": self.input.param("region", "us-east-2"),
                "type": "aws"
            },
            "configurationType": "singleNode",
            "connectionString": None,
            "couchbaseServer": {
                "version": None
            },
            "serviceGroups": None,
            "availability": {
                "type": "single"
            },
            "support": {
                "plan": "",
                "timezone": "PT"
            },
            "currentState": None,
            "audit": {
                "createdBy": None,
                "createdAt": None,
                "modifiedBy": None,
                "modifiedAt": None,
                "version": None
            },
            "cmekId": None,
            "enablePrivateDNSResolution": None
        }

        res = self.select_CIDR(
            self.organisation_id, self.project_id,
            self.expected_res["name"], self.expected_res["cloudProvider"])
        if res.status_code == 422:
            self.log.warning("A Free Tier cluster already exists")
            self.expected_res['name'], self.expected_res["cloudProvider"][
                "cidr"] = self.fetch_free_tier_cluster()
            self.expected_res["id"] = self.free_tier_cluster_id
            res, _ = self.validate_onoff_state(["turnedOff","turningOff"],
                                              free_tier=self.free_tier_cluster_id,)
            while res :
                self.log.debug("turning on the Cluster from it's turned "
                               "off state")
                self.capellaAPI.cluster_ops_apis.turn_free_tier_cluster_on(
                    self.organisation_id, self.project_id,
                    self.free_tier_cluster_id,False)
                res, _ = self.validate_onoff_state(["turnedOff", "turningOff"],
                                                   free_tier=self.free_tier_cluster_id, sleep=10)
            self.wait_for_deployment(clus_id=self.free_tier_cluster_id)
            return
        if res.status_code != 202:
            self.log.error("Err: {}".format(res.content))
            self.tearDown()
            self.fail("!!!...Error while creating FreeTier...!!!")
        self.log.info("FreeTier created successfully.")
        self.free_tier_cluster_id = res.json()["id"]
        self.expected_res["id"] = self.free_tier_cluster_id

        # Wait for the deployment request to complete.
        self.log.info("Checking for FREE TIER CLUSTER {} to be stable."
                      .format(self.free_tier_cluster_id))
        start_time = time.time()
        res = False, None
        while not res:
            res, self.expected_res["cloudProvider"]["cidr"] = \
                self.validate_onoff_state(
                ["healthy"], free_tier=self.free_tier_cluster_id)
            if time.time() > 1800 + start_time:
                self.tearDown()
                self.fail("!!!...Free Tier Cluster didn't deploy within "
                          "30mins...!!!")
        self.log.info("Successfully deployed Free Tier Cluster.")

    def tearDown(self):
        self.update_auth_with_api_token(self.curr_owner_key)
        super(GetFreeTier, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Send call with valid path params"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/freeTier",
                "expected_status_code": 404,
                "expected_error": {
                    "errorType": "RouteNotFound",
                    "message": "Not found"
                }
            }, {
                "description": "Replace the last path param name in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/freeTie",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/freeTier/freeTie",
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
                    self.free_tier_cluster_id, non_hex=True),
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
            cluster = self.free_tier_cluster_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.free_tier_cluster_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]
            elif "invalid_clusterId" in testcase:
                cluster = testcase["invalid_clusterId"]

            result = (self.capellaAPI.cluster_ops_apis.
                      fetch_free_tier_cluster_info(
                        organization, project, cluster))
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = (self.capellaAPI.cluster_ops_apis.
                          fetch_free_tier_cluster_info(
                            organization, project, cluster))
            self.capellaAPI.cluster_ops_apis.free_tier_cluster_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters/freeTier"
            self.validate_testcase(result, [200], testcase, failures, True,
                                   self.expected_res, self.free_tier_cluster_id)
        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
             "organizationOwner", "projectOwner", "projectManager",
             "projectViewer", "projectDataReaderWriter", "projectDataReader"
        ]):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = (self.capellaAPI.cluster_ops_apis.
                      fetch_free_tier_cluster_info(
                        self.organisation_id, self.project_id,
                        self.free_tier_cluster_id, header))
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = (self.capellaAPI.cluster_ops_apis.
                          fetch_free_tier_cluster_info(
                            self.organisation_id, self.project_id,
                            self.free_tier_cluster_id, header))
            self.validate_testcase(result, [200], testcase, failures, True,
                                   self.expected_res,
                                   self.free_tier_cluster_id)
        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_query_parameters(self):
        self.log.debug(
                "Correct Params - organization ID: {}, project ID: {}, "
                "cluster ID: {}".format(
                    self.organisation_id, self.project_id, self.free_tier_cluster_id))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id, self.free_tier_cluster_id):
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
                    combination[2] == self.free_tier_cluster_id):
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
                elif combination[2] != self.free_tier_cluster_id:
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

            result = self.capellaAPI.cluster_ops_apis.fetch_free_tier_cluster_info(self.organisation_id, self.project_id, self.free_tier_cluster_id,
                **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.fetch_free_tier_cluster_info(self.organisation_id, self.project_id, self.free_tier_cluster_id,
                    **kwarg)
            self.validate_testcase(result, [200], testcase, failures, True,
                                   self.expected_res, self.free_tier_cluster_id)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.fetch_free_tier_cluster_info, (
                self.organisation_id, self.project_id,
                self.free_tier_cluster_id
            )
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.fetch_free_tier_cluster_info, (
                self.organisation_id, self.project_id,
                self.free_tier_cluster_id
            )
        ]]
        self.throttle_test(api_func_list, True, self.project_id)
