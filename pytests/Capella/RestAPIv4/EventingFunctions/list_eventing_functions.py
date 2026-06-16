"""
Created on June 04, 2026
"""

from pytests.Capella.RestAPIv4.EventingFunctions.eventing_base import \
    EventingFunctionBase


class ListEventingFunctions(EventingFunctionBase):

    def setUp(self, nomenclature="EventingFunctions_List"):
        EventingFunctionBase.setUp(self, nomenclature, create_function=True)

    def test_api_path(self):
        testcases = [
            {
                "description": "List eventing functions with valid path params"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}/eventingFunctions",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }, {
                "description": "Replace eventingFunctions with eventingFunction in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/eventingFunction",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/eventingFunctions/function",
                "expected_status_code": 400,
                "expected_error": self.expected_invalid_uuid_error()
            }, {
                "description": "List eventing functions with non-hex organizationID",
                "invalid_organizationID": self.replace_last_character(
                    self.organisation_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": self.expected_invalid_uuid_error()
            }, {
                "description": "List eventing functions with non-hex projectID",
                "invalid_projectID": self.replace_last_character(
                    self.project_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": self.expected_invalid_uuid_error()
            }, {
                "description": "List eventing functions with non-hex clusterID",
                "invalid_clusterID": self.replace_last_character(
                    self.cluster_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": self.expected_invalid_uuid_error()
            }
        ]
        failures = list()
        for testcase in testcases:
            organization = self.organisation_id
            project = self.project_id
            cluster = self.cluster_id
            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.eventing_functions_endpoint = \
                    testcase["url"]
            if "invalid_organizationID" in testcase:
                organization = testcase["invalid_organizationID"]
            elif "invalid_projectID" in testcase:
                project = testcase["invalid_projectID"]
            elif "invalid_clusterID" in testcase:
                cluster = testcase["invalid_clusterID"]
            result = self.api_call_with_retry(
                self.capellaAPI.cluster_ops_apis.list_eventing_functions,
                organization, project, cluster)
            self.capellaAPI.cluster_ops_apis.eventing_functions_endpoint = \
                self.eventing_functions_endpoint_default
            self.validate_testcase(result, [200], testcase, failures)
        if failures:
            self.fail("{} tests FAILED".format(len(failures)))

    def test_authorization(self):
        failures = list()
        allowed_roles = [
            "organizationOwner", "projectOwner", "projectManager",
            "projectViewer", "projectDataReader", "projectDataReaderWriter"
        ]
        for testcase in self.v4_RBAC_injection_init(allowed_roles):
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = self.api_call_with_retry(
                self.capellaAPI.cluster_ops_apis.list_eventing_functions,
                self.organisation_id, self.project_id, self.cluster_id,
                headers=header)
            self.validate_testcase(result, [200], testcase, failures)
        if failures:
            self.fail("{} tests FAILED".format(len(failures)))

    def test_query_parameters(self):
        testcases = [
            {
                "description": "List eventing functions with valid status filter",
                "status": "undeployed"
            }, {
                "description": "List eventing functions with multi status filter",
                "status": "deployed,undeployed"
            }, {
                "description": "List eventing functions with invalid status filter",
                "status": "invalid-state",
                "expected_status_code": [400, 422],
                "expected_error": ""
            }
        ]
        failures = list()
        for testcase in testcases:
            result = self.api_call_with_retry(
                self.capellaAPI.cluster_ops_apis.list_eventing_functions,
                self.organisation_id, self.project_id, self.cluster_id,
                status=testcase["status"])
            self.validate_testcase(result, [200], testcase, failures)
        if failures:
            self.fail("{} tests FAILED out of {} TOTAL tests".format(
                len(failures), len(testcases)))
