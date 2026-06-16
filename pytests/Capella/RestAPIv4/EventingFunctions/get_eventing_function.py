"""
Created on June 04, 2026
"""

from pytests.Capella.RestAPIv4.EventingFunctions.eventing_base import \
    EventingFunctionBase


class GetEventingFunction(EventingFunctionBase):

    def setUp(self, nomenclature="EventingFunctions_Get"):
        EventingFunctionBase.setUp(self, nomenclature, create_function=True)

    def test_api_path(self):
        testcases = [
            {
                "description": "Get eventing function with valid path params"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}/eventingFunctions/{}",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }, {
                "description": "Replace eventingFunctions with eventingFunction in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/eventingFunction/{}",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/eventingFunctions/{}/function",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Get eventing function with non-hex organizationID",
                "invalid_organizationID": self.replace_last_character(
                    self.organisation_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": self.expected_invalid_uuid_error()
            }, {
                "description": "Get eventing function with non-hex projectID",
                "invalid_projectID": self.replace_last_character(
                    self.project_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": self.expected_invalid_uuid_error()
            }, {
                "description": "Get eventing function with non-hex clusterID",
                "invalid_clusterID": self.replace_last_character(
                    self.cluster_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": self.expected_invalid_uuid_error()
            }, {
                "description": "Get missing eventing function",
                "function_name": "evf_missing_{}".format(
                    self.generate_random_string(6, special_characters=False).lower()),
                "expected_status_code": [404, 422],
                "expected_error": ""
            }
        ]
        failures = list()
        for testcase in testcases:
            organization = self.organisation_id
            project = self.project_id
            cluster = self.cluster_id
            function_name = testcase.get("function_name", self.function_name)
            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.eventing_function_endpoint = \
                    testcase["url"]
            if "invalid_organizationID" in testcase:
                organization = testcase["invalid_organizationID"]
            elif "invalid_projectID" in testcase:
                project = testcase["invalid_projectID"]
            elif "invalid_clusterID" in testcase:
                cluster = testcase["invalid_clusterID"]
            result = self.api_call_with_retry(
                self.capellaAPI.cluster_ops_apis.get_eventing_function,
                organization, project, cluster, function_name)
            self.capellaAPI.cluster_ops_apis.eventing_function_endpoint = \
                self.eventing_function_endpoint_default
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
                self.capellaAPI.cluster_ops_apis.get_eventing_function,
                self.organisation_id, self.project_id, self.cluster_id,
                self.function_name, headers=header)
            self.validate_testcase(result, [200], testcase, failures)
        if failures:
            self.fail("{} tests FAILED".format(len(failures)))

    def test_query_parameters(self):
        testcases = [
            {
                "description": "Get eventing function with export=true",
                "export": True
            }, {
                "description": "Get eventing function with export=false",
                "export": False
            }, {
                "description": "Get eventing function with invalid export value",
                "export": "abc",
                "expected_status_code": [400, 422],
                "expected_error": ""
            }
        ]
        failures = list()
        for testcase in testcases:
            result = self.api_call_with_retry(
                self.capellaAPI.cluster_ops_apis.get_eventing_function,
                self.organisation_id, self.project_id, self.cluster_id,
                self.function_name, export=testcase["export"])
            self.validate_testcase(result, [200], testcase, failures)
        if failures:
            self.fail("{} tests FAILED out of {} TOTAL tests".format(
                len(failures), len(testcases)))
