"""
Created on June 10, 2024

@author: Created using cbRAT cbModule by Vipul Bhardwaj
"""

from pytests.Capella.RestAPIv4.Clusters.get_clusters import GetCluster


class GetNetworkPeers(GetCluster):

    def setUp(self, nomenclature="VPCs_GET"):
        GetCluster.setUp(self, nomenclature)
        self.provider_type = str(self.input.param("providerType", "aws"))
        if self.provider_type == "azure":
            self.skipTest("!!!SKIPPING AZURE VNET PEERING TESTS!!!")

        vpc_config = {
            "providerId": None
        }
        if self.provider_type == "aws":
            config = {
                "accountId": str(self.input.param("accountId", None)),
                "cidr": str(self.input.param("vpcCidr", "10.222.0.0/23")),
                "region": "us-east-1",
                "vpcId": str(self.input.param("vpcId", None))
            }
            vpc_config["AWSConfig"] = config
        elif self.provider_type == "azure":
            config = {
                "azureTenantId": str(self.input.param("azureTenantId", None)),
                "cidr": str(self.input.param("vpcCidr", "10.1.0.0/16")),
                "resourceGroup": str(self.input.param("azureResourceGroup", None)),
                "subscriptionId": str(self.input.param("azureSubscriptionId", None)),
                "vnetId": str(self.input.param("vpcId", None)),
            }
            vpc_config["AzureConfig"] = config
        elif self.provider_type == "gcp":
            config = {
                "networkName": str(self.input.param("vpcId", None)),
                "projectId": str(self.input.param("gcpProjectId", None)),
                "serviceAccount": str(self.input.param("gcpServiceAccount",
                                                       None)),
                "cidr": str(self.input.param("vpcCidr", "10.1.0.0/24"))
            }
            vpc_config["GCPConfig"] = config

        self.expected_res = {
            "audit": {
                "createdAt": None,
                "createdBy": None,
                "modifiedAt": None,
                "modifiedBy": None,
                "version": None
            },
            "commands": None,
            "name": self.prefix + self.provider_type + nomenclature,
            "providerType": self.provider_type,
            "providerConfig": vpc_config,
            "status": {
                "reasoning": "",
                "state": "complete"
            }
        }

        if self.capella["clusters"]["vpc_id"]:
            self.peer_id = self.capella["clusters"]["vpc_id"]
        else:
            self.log.info("Creating NetworkPeer")
            res = self.capellaAPI.cluster_ops_apis.create_network_peer(
                self.organisation_id, self.project_id, self.cluster_id,
                self.expected_res["name"],
                config,
                self.expected_res["providerType"]
            )
            if res.status_code != 201:
                self.log.error("Result: {}".format(res.content))
                self.tearDown()
                self.fail("!!!...Error while creating NetworkPeer...!!!")
            self.log.info("NetworkPeer created successfully.")

            self.peer_id = res.json()["id"]
            self.capella["clusters"]["vpc_id"] = self.peer_id

            # Wait for cluster to be healthy.
            self.wait_for_deployment()
        self.expected_res["id"] = self.peer_id

    def tearDown(self):
        self.update_auth_with_api_token(self.curr_owner_key)
        super(GetNetworkPeers, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Send call with valid path params"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}/networkPeers",
                "expected_status_code": 404,
                "expected_error": {
                    "errorType": "RouteNotFound",
                    "message": "Not found"
                }
            }, {
                "description": "Replace the last path param name in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/networkPeer",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/networkPeers/networkPeer",
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
                "description": "Call API with non-hex peerId",
                "invalid_peerId": self.replace_last_character(
                    self.peer_id, non_hex=True),
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
            cluster = self.cluster_id
            peer = self.peer_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.vpc_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]
            elif "invalid_clusterId" in testcase:
                cluster = testcase["invalid_clusterId"]
            elif "invalid_peerId" in testcase:
                peer = testcase["invalid_peerId"]

            result = self.capellaAPI.cluster_ops_apis.fetch_network_peer_record_info(
                organization, project, cluster, peer)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.fetch_network_peer_record_info(
                    organization, project, cluster, peer)

            self.capellaAPI.cluster_ops_apis.vpc_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters/{}/networkPeers"

            self.validate_testcase(result, [200], testcase, failures, True,
                                   self.expected_res, self.peer_id)

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
                 "organizationOwner", "projectOwner"
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
            result = self.capellaAPI.cluster_ops_apis.fetch_network_peer_record_info(
                self.organisation_id, self.project_id, self.cluster_id,
                self.peer_id,
                header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.fetch_network_peer_record_info(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.peer_id,
                    header)

            self.validate_testcase(result, [200], testcase, failures, True,
                                   self.expected_res, self.peer_id)

        self.update_auth_with_api_token(self.curr_owner_key)
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
                "Correct Params - organization ID: {}, project ID: {}, "
                "cluster ID: {}, peer ID: {}".format(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.peer_id))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id, self.cluster_id,
                self.peer_id):
            testcases += 1
            testcase = {
                "description": "organization ID: {}, project ID: {}, "
                "cluster ID: {}, peer ID: {}"
                .format(str(combination[0]), str(combination[1]),
                        str(combination[2]), str(combination[3])),
                "organizationID": combination[0],
                "projectID": combination[1],
                "clusterID": combination[2],
                "peerID": combination[3]
            }
            if not (combination[0] == self.organisation_id and
                    combination[1] == self.project_id and
                    combination[2] == self.cluster_id and
                    combination[3] == self.peer_id):
                if (combination[0] == "" or combination[1] == "" or
                        combination[2] == "" or combination[3] == ""):
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = "404 page not found"
                elif any(variable in [
                    int, bool, float, list, tuple, set, type(None)] for
                         variable in [
                             type(combination[0]), type(combination[1]), 
                             type(combination[2]), type(combination[3])]):
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
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = {
                        "code": 3000,
                        "hint": "Resource not Found.",
                        "httpStatusCode": 404,
                        "message": "Not Found."
                    }
            self.log.info("Executing test: {}".format(testcase["description"]))
            if "param" in testcase:
                kwarg = {testcase["param"]: testcase["paramValue"]}
            else:
                kwarg = dict()

            result = self.capellaAPI.cluster_ops_apis.fetch_network_peer_record_info(
                testcase["organizationID"], testcase["projectID"],
                testcase["clusterID"], testcase["peerID"],
                **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.fetch_network_peer_record_info(
                    testcase["organizationID"], testcase["projectID"],
                    testcase["clusterID"], testcase["peerID"],
                    **kwarg)

            self.validate_testcase(result, [200], testcase, failures, True,
                                   self.expected_res, self.peer_id)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.fetch_network_peer_record_info, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.peer_id
            )
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.fetch_network_peer_record_info, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.peer_id
            )
        ]]
        self.throttle_test(api_func_list, True, self.project_id)