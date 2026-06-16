"""
Created on June 08, 2026
"""

from pytests.Capella.RestAPIv4.EventingFunctions.eventing_base import \
    EventingFunctionBase


class GetEventingFunctionLogs(EventingFunctionBase):

    def setUp(self, nomenclature="EventingFunctions_GetLogs"):
        EventingFunctionBase.setUp(self, nomenclature, create_function=True)

    def test_api_path(self):
        testcases = [
            {
                "description": "Get eventing function logs with valid path params"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}/eventingFunctions/{}/logs",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }, {
                "description": "Replace logs with log in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/eventingFunctions/{}/log",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add invalid segment to URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/eventingFunctions/{}/logs/log",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Get logs with non-hex organizationID",
                "invalid_organizationID": self.replace_last_character(
                    self.organisation_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": self.expected_invalid_uuid_error()
            }, {
                "description": "Get logs with non-hex projectID",
                "invalid_projectID": self.replace_last_character(
                    self.project_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": self.expected_invalid_uuid_error()
            }, {
                "description": "Get logs with non-hex clusterID",
                "invalid_clusterID": self.replace_last_character(
                    self.cluster_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": self.expected_invalid_uuid_error()
            }, {
                "description": "Get logs for non-existent function",
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
                self.capellaAPI.cluster_ops_apis.eventing_function_logs_endpoint = \
                    testcase["url"]
            if "invalid_organizationID" in testcase:
                organization = testcase["invalid_organizationID"]
            elif "invalid_projectID" in testcase:
                project = testcase["invalid_projectID"]
            elif "invalid_clusterID" in testcase:
                cluster = testcase["invalid_clusterID"]
            result = self.api_call_with_retry(
                self.capellaAPI.cluster_ops_apis.get_eventing_function_logs,
                organization, project, cluster, function_name)
            self.capellaAPI.cluster_ops_apis.eventing_function_logs_endpoint = \
                self.eventing_function_endpoint_default + "/logs"
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
                self.capellaAPI.cluster_ops_apis.get_eventing_function_logs,
                self.organisation_id, self.project_id, self.cluster_id,
                self.function_name, headers=header)
            self.validate_testcase(result, [200], testcase, failures)
        if failures:
            self.fail("{} tests FAILED".format(len(failures)))

    def test_query_parameters(self):
        testcases = [
            {
                "description": "Get logs with valid path params",
                "organizationID": self.organisation_id,
                "projectID": self.project_id,
                "clusterID": self.cluster_id,
                "functionName": self.function_name
            }, {
                "description": "Get logs with empty organizationID",
                "organizationID": "",
                "projectID": self.project_id,
                "clusterID": self.cluster_id,
                "functionName": self.function_name,
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Get logs with empty functionName",
                "organizationID": self.organisation_id,
                "projectID": self.project_id,
                "clusterID": self.cluster_id,
                "functionName": "",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Get logs with wrong organizationID",
                "organizationID": self.replace_last_character(
                    self.organisation_id),
                "projectID": self.project_id,
                "clusterID": self.cluster_id,
                "functionName": self.function_name,
                "expected_status_code": 403,
                "expected_error": {
                    "code": 1002,
                    "hint": "Your access to the requested resource is "
                            "denied. Please make sure you have the necessary "
                            "permissions to access the resource.",
                    "message": "Access Denied.",
                    "httpStatusCode": 403
                }
            }
        ]
        failures = list()
        for testcase in testcases:
            result = self.api_call_with_retry(
                self.capellaAPI.cluster_ops_apis.get_eventing_function_logs,
                testcase["organizationID"], testcase["projectID"],
                testcase["clusterID"], testcase["functionName"])
            self.validate_testcase(result, [200], testcase, failures)
        if failures:
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))
