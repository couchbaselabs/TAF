"""
Created on May 05, 2026

@author: Automation
"""

from pytests.Capella.RestAPIv4.Clusters.get_clusters import GetCluster


class UnassociateCMEK(GetCluster):

    def setUp(self, nomenclature="Unassociate_CMEK_POST"):
        GetCluster.setUp(self, nomenclature)
        self.cmek_id = self.input.param("cmek_id", "00000000-0000-0000-0000-000000000000")

    def tearDown(self):
        self.update_auth_with_api_token(self.curr_owner_key)
        super(UnassociateCMEK, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Unassociate CMEK using valid path",
                "expected_status_code": [204, 400, 404, 422]
            }, {
                "description": "Replace API version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}/cmek/{}/unassociate",
                "expected_status_code": 404
            }, {
                "description": "Replace cmek with cmeks in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/cmeks/{}/unassociate",
                "expected_status_code": 404
            }, {
                "description": "Replace unassociate with unassociates in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/cmek/{}/unassociates",
                "expected_status_code": 404
            }, {
                "description": "Call API with non-hex organizationId",
                "invalid_organizationId": self.replace_last_character(
                    self.organisation_id, non_hex=True),
                "expected_status_code": 400
            }, {
                "description": "Call API with non-hex projectId",
                "invalid_projectId": self.replace_last_character(
                    self.project_id, non_hex=True),
                "expected_status_code": 400
            }, {
                "description": "Call API with non-hex clusterId",
                "invalid_clusterId": self.replace_last_character(
                    self.cluster_id, non_hex=True),
                "expected_status_code": 400
            }, {
                "description": "Call API with non-hex cmekId",
                "invalid_cmekId": self.replace_last_character(
                    self.cmek_id, non_hex=True),
                "expected_status_code": 400
            }
        ]

        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            organization = self.organisation_id
            project = self.project_id
            cluster = self.cluster_id
            cmek_id = self.cmek_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.unassociate_cmek_endpoint = testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]
            elif "invalid_clusterId" in testcase:
                cluster = testcase["invalid_clusterId"]
            elif "invalid_cmekId" in testcase:
                cmek_id = testcase["invalid_cmekId"]

            result = self.capellaAPI.cluster_ops_apis.unassociate_cmek(
                organization, project, cluster, cmek_id)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.unassociate_cmek(
                    organization, project, cluster, cmek_id)
            self.capellaAPI.cluster_ops_apis.unassociate_cmek_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters/{}/cmek/{}/unassociate"
            self.validate_testcase(result, [204, 400, 404, 422], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner", "projectManager"
        ], None):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = self.capellaAPI.cluster_ops_apis.unassociate_cmek(
                self.organisation_id, self.project_id, self.cluster_id,
                self.cmek_id, headers=header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.unassociate_cmek(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.cmek_id, headers=header)
            self.validate_testcase(result, [204, 400, 404, 422], testcase,
                                   failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))
