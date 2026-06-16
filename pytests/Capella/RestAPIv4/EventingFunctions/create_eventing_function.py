"""
Created on June 04, 2026
"""

from pytests.Capella.RestAPIv4.EventingFunctions.eventing_base import \
    EventingFunctionBase


class CreateEventingFunction(EventingFunctionBase):

    def setUp(self, nomenclature="EventingFunctions_Create"):
        EventingFunctionBase.setUp(self, nomenclature, create_function=False)

    def test_api_path(self):
        testcases = [
            {
                "description": "Create eventing function with valid path params"
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
                "expected_status_code": 405,
                "expected_error": ""
            }, {
                "description": "Create eventing function with non-hex organizationID",
                "invalid_organizationID": self.replace_last_character(
                    self.organisation_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": self.expected_invalid_uuid_error()
            }, {
                "description": "Create eventing function with non-hex projectID",
                "invalid_projectID": self.replace_last_character(
                    self.project_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": self.expected_invalid_uuid_error()
            }, {
                "description": "Create eventing function with non-hex clusterID",
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
            function_name = "evf_{}".format(
                self.generate_random_string(10, special_characters=False).lower())
            payload = self.build_payload(function_name=function_name)
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
                self.capellaAPI.cluster_ops_apis.create_eventing_function_v4,
                organization, project, cluster, payload)
            self.capellaAPI.cluster_ops_apis.eventing_functions_endpoint = \
                self.eventing_functions_endpoint_default
            self.validate_testcase(result, [201, 409], testcase, failures)
            if result.status_code in [201, 409]:
                self.created_function_names.add(function_name)
        if failures:
            self.fail("{} tests FAILED".format(len(failures)))

    def test_authorization(self):
        failures = list()
        allowed_roles = [
            "organizationOwner", "projectOwner", "projectManager",
            "projectDataReaderWriter"
        ]
        for testcase in self.v4_RBAC_injection_init(allowed_roles):
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            function_name = "evf_{}".format(
                self.generate_random_string(10, special_characters=False).lower())
            payload = self.build_payload(function_name=function_name)
            result = self.create_function(function_name, payload, header)
            self.validate_testcase(result, [201, 409], testcase, failures)
        if failures:
            self.fail("{} tests FAILED".format(len(failures)))

    def test_payload(self):
        testcases = [
            {
                "desc": "Create eventing function with valid payload",
                "payload": self.build_payload(function_name="evf_{}".format(
                    self.generate_random_string(10, special_characters=False).lower()))
            }, {
                "desc": "Create eventing function with invalid workerCount type",
                "payload": dict(self.build_payload(function_name="evf_{}".format(
                    self.generate_random_string(10, special_characters=False).lower()))),
                "expected_status_code": 400,
                "expected_error": ""
            }, {
                "desc": "Create eventing function with empty name",
                "payload": dict(self.build_payload(function_name="")),
                "expected_status_code": [400, 422],
                "expected_error": ""
            }, {
                "desc": "Create eventing function with missing event source bucket",
                "payload": dict(self.build_payload(function_name="evf_{}".format(
                    self.generate_random_string(10, special_characters=False).lower()))),
                "expected_status_code": [400, 422],
                "expected_error": ""
            }
        ]
        testcases[1]["payload"]["settings"]["workerCount"] = "invalid"
        testcases[3]["payload"]["eventSource"]["bucket"] = None

        failures = list()
        for testcase in testcases:
            payload = testcase["payload"]
            result = self.create_function(payload=payload)
            self.validate_testcase(result, [201, 409], testcase, failures,
                                   payloadTest=True)
        if failures:
            self.fail("{} tests FAILED out of {} TOTAL tests".format(
                len(failures), len(testcases)))

    def test_query_parameters(self):
        testcases = [
            {
                "description": "Create eventing function with valid path params",
                "organizationID": self.organisation_id,
                "projectID": self.project_id,
                "clusterID": self.cluster_id
            }, {
                "description": "Create eventing function with empty organizationID",
                "organizationID": "",
                "projectID": self.project_id,
                "clusterID": self.cluster_id,
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Create eventing function with empty projectID",
                "organizationID": self.organisation_id,
                "projectID": "",
                "clusterID": self.cluster_id,
                "expected_status_code": 405,
                "expected_error": ""
            }, {
                "description": "Create eventing function with wrong organizationID",
                "organizationID": self.replace_last_character(
                    self.organisation_id),
                "projectID": self.project_id,
                "clusterID": self.cluster_id,
                "expected_status_code": 403,
                "expected_error": {
                    "code": 1002,
                    "hint": "Your access to the requested resource is "
                            "denied. Please make sure you have the necessary "
                            "permissions to access the resource.",
                    "message": "Access Denied.",
                    "httpStatusCode": 403
                }
            }, {
                "description": "Create eventing function with integer clusterID",
                "organizationID": self.organisation_id,
                "projectID": self.project_id,
                "clusterID": 123456789,
                "expected_status_code": 400,
                "expected_error": self.expected_invalid_uuid_error()
            }
        ]
        failures = list()
        for testcase in testcases:
            payload = self.build_payload(function_name="evf_{}".format(
                self.generate_random_string(10, special_characters=False).lower()))
            result = self.api_call_with_retry(
                self.capellaAPI.cluster_ops_apis.create_eventing_function_v4,
                testcase["organizationID"], testcase["projectID"],
                testcase["clusterID"], payload)
            self.validate_testcase(result, [201, 409], testcase, failures)
            if result.status_code in [201, 409]:
                self.created_function_names.add(payload["name"])
        if failures:
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))
