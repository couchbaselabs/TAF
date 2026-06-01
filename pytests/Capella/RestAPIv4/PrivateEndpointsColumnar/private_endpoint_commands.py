import copy

from pytests.Capella.RestAPIv4.ClustersColumnar.get_analytics_clusters import \
    GetAnalyticsClusters


class PostEndpointCommand(GetAnalyticsClusters):

    def setUp(self, nomenclature="PrivateEndpointsColumnar_POST"):
        GetAnalyticsClusters.setUp(self, nomenclature)
        self.expected_res = {
            "vpcNetworkID": self.input.param("vpcId", "mockValue"),
            "subnetIDs": ["mockValue"],
        }
        instance_template = self.input.param("instance_template", "AWS_4v16_4node")
        cloud_provider = self.instance_templates.get(
            instance_template, {}).get("cloudProvider", "aws")
        if cloud_provider == "azure":
            self.expected_res = {
                "resourceGroupName": "test-rg",
                "virtualNetwork": "vnet-1/subnet-1"
            }

    def tearDown(self):
        super(PostEndpointCommand, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Send call with valid path params",
                "expected_status_code": 400,
                 "expected_error": {
                    "code": 400,
                    "hint": "Please review your request and ensure that all required parameters are correctly provided.",
                    "httpStatusCode": 400,
                    "message": "Private endpoints aren't enabled for this cluster. Please select another cluster."
                }
            },
            {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/analyticsClusters/{}/privateEndpointService/endpointCommand",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            },
            {
                "description": "Replace the last path param name in URI",
                "url": "/v4/organizations/{}/projects/{}/analyticsClusters/{}/privateEndpointService/endpointComman",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            },
            {
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
            organization = self.organisation_id
            project = self.project_id
            analytics_cluster = self.analyticsCluster_id

            if "url" in testcase:
                self.columnarAPI.private_network_command_endpoint = testcase["url"]
            if "invalid_analyticsClusterId" in testcase:
                analytics_cluster = testcase["invalid_analyticsClusterId"]

            result = self.columnarAPI.post_private_endpoint_command(
                organization, project, analytics_cluster, self.expected_res)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.columnarAPI.post_private_endpoint_command(
                    organization, project, analytics_cluster, self.expected_res)

            self.columnarAPI.private_network_command_endpoint = (
                "/v4/organizations/{}/projects/{}/analyticsClusters/{}/privateEndpointService/endpointCommand")
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_payload(self):
        testcases = list()
        for key in self.expected_res:
            testcase = copy.deepcopy(self.expected_res)
            testcase[key] = None
            testcase["desc"] = "Testing `{}` with None".format(key)
            testcase["expected_status_code"] = 400
            testcases.append(testcase)

        failures = list()
        for testcase in testcases:
            payload = {key: testcase[key] for key in self.expected_res.keys()}
            result = self.columnarAPI.post_private_endpoint_command(
                self.organisation_id, self.project_id,
                self.analyticsCluster_id, payload)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.columnarAPI.post_private_endpoint_command(
                    self.organisation_id, self.project_id,
                    self.analyticsCluster_id, payload)
            self.validate_testcase(result, [200], testcase, failures,
                                   payloadTest=True)

        if failures:
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))
