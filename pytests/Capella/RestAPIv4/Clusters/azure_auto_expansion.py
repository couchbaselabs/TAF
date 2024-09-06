"""
Created on January 23, 2024

@author: Vipul Bhardwaj
"""

from pytests.Capella.RestAPIv4.Clusters.get_clusters import GetCluster

class ToggleAzureAutoExpansion(GetCluster):

    def setUp(self, nomenclature="Auto_Expansion_Update"):
        GetCluster.setUp(self, nomenclature)
        self.update_service_group = [
            {
                "node": {
                    "compute": {
                        "cpu": 4,
                        "ram": 16
                    },
                    "disk": {
                        "type": "P6",
                        "autoExpansion": False
                    }
                },
                "numOfNodes": 3,
                "services": [
                    "data",
                    "index",
                    "query"
                ]
            }
        ]
        # self.expected_result = {
        #     "name": self.prefix + nomenclature,
        #     "description": None,
        #     "cloudProvider": {
        #         "type": "azure",
        #         "region": "eastus",
        #         "cidr": "10.0.0.0/20"
        #     },
        #     "couchbaseServer": {
        #         "version": str(self.input.param("server_version", 7.6))
        #     },
        #     "serviceGroups": [
        #         {
        #             "node": {
        #                 "compute": {
        #                     "cpu": 4,
        #                     "ram": 16
        #                 },
        #                 "disk": {
        #                     "type": "P6",
        #                     "autoExpansion": False
        #                 }
        #             },
        #             "numOfNodes": 3,
        #             "services": [
        #                 "data"
        #             ]
        #         }
        #     ],
        #     "availability": {
        #         "type": "single"
        #     },
        #     "support": {
        #         "plan": "basic",
        #         "timezone": "GMT"
        #     }
        # }
        # result = self.select_CIDR(self.organisation_id, self.project_id,
        #                           self.expected_res["name"],
        #                           self.expected_res['cloudProvider'],
        #                           self.expected_res['serviceGroups'],
        #                           self.expected_res['availability'],
        #                           self.expected_result['support'])
        # if result.status_code != 202:
        #     self.tearDown()
        #     self.fail("!!!...Azure cluster creation failed...!!!")
        #
        # self.cluster_id
        if not self.wait_for_deployment(self.cluster_id):
            self.tearDown()
            self.fail("!!!...Azure Cluster deployment failed...!!!")

    def tearDown(self):
        super(ToggleAzureAutoExpansion, self).tearDown()

    def validate_auto_expansion(self, cluster_id):
        self.cluster_id = cluster_id
        self.update_auth_with_api_token(self.curr_owner_key)
        autoExpansion = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
            self.organisation_id, self.project_id, cluster_id
        ).json()["serviceGroups"][0]["node"]["disk"]["autoExpansion"]

        # Wait for cluster scaling to finish.
        if not self.wait_for_deployment(self.cluster_id):
            self.fail("!!!...Cluster Scaling failed...!!!")

        # Set the Auto Expansion back to ON.
        self.capellaAPI.cluster_ops_apis.update_cluster(
            self.organisation_id, self.project_id, cluster_id,
            self.expected_res["name"], "", self.expected_res['support'],
            self.expected_res["serviceGroups"], False)

        # Wait for cluster scaling to finish.
        if not self.wait_for_deployment(self.cluster_id):
            self.fail("!!!...Cluster Scaling failed...!!!")

        if not autoExpansion:
            self.log.error("Auto Expansion for cluster: {} is = {}"
                           .format(cluster_id, autoExpansion))
            return False
        self.log.info("Cluster was updated successfully with Auto Expansion "
                      "as On")
        return True

    def test_api_path(self):
        testcases = [
            {
                "description": "Update a valid Azure cluster"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters",
                "expected_status_code": 404,
                "expected_error": {
                    "errorType": "RouteNotFound",
                    "message": "Not found"
                }
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
                "description": "Update Azure cluster but with non-hex "
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
                "description": "Update Azure cluster but with non-hex "
                               "projectID",
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
                "description": "Update Azure cluster but with non-hex "
                               "clusterID",
                "invalid_projectID": self.replace_last_character(
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
            }
        ]
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            org = self.organisation_id
            proj = self.project_id
            clus = self.cluster_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.cluster_endpoint = \
                    testcase["url"]
            if "invalid_organizationID" in testcase:
                org = testcase["invalid_organizationID"]
            elif "invalid_projectID" in testcase:
                proj = testcase["invalid_projectID"]
            elif "invalid_clusterID" in testcase:
                clus = testcase["invalid_clusterID"]

            result = self.capellaAPI.cluster_ops_apis.update_cluster(
                org, proj, clus, self.expected_res["name"], "",
                self.expected_res['support'],
                self.update_service_group, False)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.update_cluster(
                    org, proj, clus, self.expected_res["name"], "",
                    self.expected_res['support'],
                    self.update_service_group, True)
            self.capellaAPI.cluster_ops_apis.cluster_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters"
            if self.validate_testcase(result, [204], testcase, failures):
                if not self.validate_auto_expansion(self.cluster_id):
                    self.log.warning("Result : {}".format(result.content))
                    failures.append(testcase["description"])

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
            result = self.capellaAPI.cluster_ops_apis.update_cluster(
                self.organisation_id, self.project_id, self.cluster_id,
                self.expected_res["name"], "",
                self.expected_res['support'], self.update_service_group,
                False, header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.update_cluster(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.expected_res["name"], "",
                    self.expected_res['support'],
                    self.update_service_group, False, header)
            if self.validate_testcase(result, [204], testcase, failures):
                if not self.validate_auto_expansion(self.cluster_id):
                    self.log.warning("Result : {}".format(result.content))
                    failures.append(testcase["description"])

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_query_parameters(self):
        self.log.debug("Correct Params - OrgID: {}, ProjID: {}, "
                       "dummy ClusID: {}"
                       .format(self.organisation_id, self.project_id,
                               self.cluster_id))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id, self.cluster_id):
            testcases += 1
            testcase = {
                "description": "OrganizationID: {}, ProjectID: {}, ClusterID: "
                "{}".format(str(combination[0]), str(combination[1]),
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

            result = self.capellaAPI.cluster_ops_apis.update_cluster(
                testcase["organizationID"], testcase["projectID"],
                testcase["clusterID"], self.expected_res["name"], "",
                self.expected_res['support'], self.update_service_group,
                False, **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.update_cluster(
                    testcase["organizationID"], testcase["projectID"],
                    testcase["clusterID"], self.expected_res["name"], "",
                    self.expected_res['support'], self.update_service_group,
                    False, **kwarg)

            if self.validate_testcase(result, [204], testcase, failures):
                if not self.validate_auto_expansion(self.cluster_id):
                    self.log.warning("Result : {}".format(result.content))
                    failures.append(testcase["description"])

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))
