"""
Created on January 9, 2026

@author: Thuan Nguyen
"""

import time
from pytests.Capella.RestAPIv4.Clusters.get_clusters import GetCluster

class EnableClusterExpressScaling(GetCluster):
    def setUp(self, nomenclature="EnableClusterExpressScaling_PUT"):
        GetCluster.setUp(self, nomenclature)

    def tearDown(self):
        super(EnableClusterExpressScaling, self).tearDown()
 

    def enable_express_scaling(self):
        cluster_express_scaling_status = self.get_cluster_express_scaling_status()
        if cluster_express_scaling_status=="enabled":
            self.capellaAPI.cluster_ops_apis.disable_express_scaling(
                self.organisation_id, self.project_id, self.cluster_id)
            time.sleep(15)
            self.wait_for_cluster_healthy()

        cluster_express_scaling_status = self.get_cluster_express_scaling_status()
        if cluster_express_scaling_status=="disabled":
            result = self.capellaAPI.cluster_ops_apis.enable_express_scaling(
            self.organisation_id, self.project_id, self.cluster_id)
            time.sleep(20)
        else:
            self.fail("Cluster express scaling status is not disabled")
        print("************* Enable express scaling result: {} *************".format(result))
        if result.status_code != 204:
            self.fail("Failed to enable express scaling. Reason: {}".format(result.content))

        cluster_express_scaling_status = self.get_cluster_express_scaling_status()
        if cluster_express_scaling_status!="enabled":
            self.wait_for_cluster_healthy()

        return result

    def test_enable_express_scaling_api_path(self):
        testcases = [
            {
                "description": "Send call with valid path params",
                "expected_status_code": 204
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}/expressScaling",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404 Not Found</title></head><body><center><h1>404 Not Found</h1></center><hr><center>nginx</center></body></html>"
            }, {
                "description": "Replace the last path param name in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/expressScaling",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                    "url": "/v4/organizations/{}/projects/{}/clusters/{}/expressScaling/expressScaling",
                "expected_status_code": 400,
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
            }
        ]
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            organization = self.organisation_id
            project = self.project_id
            cluster = self.cluster_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.express_scaling_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]
            elif "invalid_clusterId" in testcase:
                cluster = testcase["invalid_clusterId"]

            result = self.capellaAPI.cluster_ops_apis.enable_express_scaling(
                                self.organisation_id, self.project_id, self.cluster_id)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.enable_express_scaling(
                                self.organisation_id, self.project_id, self.cluster_id)
            self.capellaAPI.cluster_ops_apis.express_scaling_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters/{}/expressScaling"
            self.validate_testcase(result, [testcase["expected_status_code"]], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.enable_express_scaling, (
                self.organisation_id, self.project_id, self.cluster_id,
            )
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.enable_express_scaling, (
                self.organisation_id, self.project_id, self.cluster_id
            )
        ]]
        self.throttle_test(api_func_list, True, self.project_id)

    def wait_for_cluster_healthy(self):
        res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
            self.organisation_id, self.project_id, self.cluster_id)
        start_time = time.time()
        timeout = 300  # 5 minutes in seconds
        cluster_healthy = False
        while not cluster_healthy:
            if res.json()["currentState"] and res.json()["currentState"] == "healthy":
                cluster_healthy = True
                break
            if time.time() > start_time + timeout:
                self.fail("Timeout after 5 minutes waiting for cluster to be healthy. "
                         "Current state: {}".format(res.json()["currentState"]))
            self.log.warning("Waiting for cluster to rebalance. Current state: {}"
                           .format(res.json()["currentState"]))
            self.sleep(10, "Waiting for cluster to be healthy")
            res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                self.organisation_id, self.project_id, self.cluster_id)
        return cluster_healthy

    def get_cluster_express_scaling_status(self):
        status = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(self.organisation_id,
                                                                     self.project_id,
                                                                     self.cluster_id)
        status = status.json()["currentState"]
        while status != 'healthy':
            self.sleep(15, "Waiting for cluster to be in healthy state. Current status - {}"
                       .format(status))
            status = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                                                                                self.organisation_id,
                                                                                self.project_id,
                                                                                self.cluster_id)
            status = status.json()["currentState"]
        if status != "healthy":
            self.fail("Cluster express scaling status is not healthy")

        # check express scaling status
        express_scaling_status = self.capellaAPI.cluster_ops_apis.get_express_scaling_status(self.organisation_id,
                                                                     self.project_id,
                                                                     self.cluster_id)
        print("************* Cluster express scaling status: {} *************".format(express_scaling_status))
        return express_scaling_status
