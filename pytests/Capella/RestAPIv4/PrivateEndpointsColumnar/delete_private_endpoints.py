from pytests.Capella.RestAPIv4.PrivateEndpointsColumnar.accept_private_endpoints \
    import PostAssociate


class PostUnassociate(PostAssociate):

    def setUp(self, nomenclature="PrivateEndpointsColumnar_POST"):
        PostAssociate.setUp(self, nomenclature)

    def tearDown(self):
        super(PostUnassociate, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/analyticsClusters/{}/privateEndpointService/endpoints/{}/unassociate",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            },
            {
                "description": "Replace the last path param name in URI",
                "url": "/v4/organizations/{}/projects/{}/analyticsClusters/{}/privateEndpointService/endpoints/{}/unassociat",
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
            endpoint = self.endpoint_id

            if "url" in testcase:
                self.columnarAPI.unassociate_private_network_endpoint = testcase["url"]
            if "invalid_analyticsClusterId" in testcase:
                analytics_cluster = testcase["invalid_analyticsClusterId"]

            result = self.columnarAPI.delete_private_endpoint(
                organization, project, analytics_cluster, endpoint)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.columnarAPI.delete_private_endpoint(
                    organization, project, analytics_cluster, endpoint)

            self.columnarAPI.unassociate_private_network_endpoint = (
                "/v4/organizations/{}/projects/{}/analyticsClusters/{}/privateEndpointService/endpoints/{}/unassociate")
            self.validate_testcase(result, [204], testcase, failures)

        if failures:
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))
