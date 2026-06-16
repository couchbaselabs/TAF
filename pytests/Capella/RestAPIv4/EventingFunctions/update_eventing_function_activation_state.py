"""
Created on June 04, 2026
"""

from pytests.Capella.RestAPIv4.EventingFunctions.eventing_base import \
    EventingFunctionBase


class UpdateEventingFunctionActivationState(EventingFunctionBase):

    def setUp(self, nomenclature="EventingFunctions_UpdateActivationState"):
        EventingFunctionBase.setUp(self, nomenclature, create_function=True)

    def test_api_path(self):
        testcases = [
            {
                "description": "Update eventing function activation state with valid path params"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}/eventingFunctions/{}/activationState",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }, {
                "description": "Replace activationState with activation",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/eventingFunctions/{}/activation",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add invalid segment to activationState path",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/eventingFunctions/{}/activationState/state",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Update activation state with non-hex organizationID",
                "invalid_organizationID": self.replace_last_character(
                    self.organisation_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": self.expected_invalid_uuid_error()
            }, {
                "description": "Update activation state with non-hex projectID",
                "invalid_projectID": self.replace_last_character(
                    self.project_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": self.expected_invalid_uuid_error()
            }, {
                "description": "Update activation state with non-hex clusterID",
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
                self.capellaAPI.cluster_ops_apis.eventing_function_activation_state_endpoint = \
                    testcase["url"]
            if "invalid_organizationID" in testcase:
                organization = testcase["invalid_organizationID"]
            elif "invalid_projectID" in testcase:
                project = testcase["invalid_projectID"]
            elif "invalid_clusterID" in testcase:
                cluster = testcase["invalid_clusterID"]
            result = self.api_call_with_retry(
                self.capellaAPI.cluster_ops_apis.update_eventing_function_activation_state_v4,
                organization, project, cluster, self.function_name, "deploy")
            self.capellaAPI.cluster_ops_apis.eventing_function_activation_state_endpoint = \
                self.eventing_function_activation_state_endpoint_default
            self.validate_testcase(result, [204], testcase, failures)
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
            self.update_auth_with_api_token(self.curr_owner_key)
            self.api_call_with_retry(
                self.capellaAPI.cluster_ops_apis.update_eventing_function_activation_state_v4,
                self.organisation_id, self.project_id, self.cluster_id,
                self.function_name, "undeploy")
            result = self.api_call_with_retry(
                self.capellaAPI.cluster_ops_apis.update_eventing_function_activation_state_v4,
                self.organisation_id, self.project_id, self.cluster_id,
                self.function_name, "deploy", headers=header)
            self.validate_testcase(result, [204, 400, 422], testcase, failures)
        if failures:
            self.fail("{} tests FAILED".format(len(failures)))

    def test_payload(self):
        testcases = [
            {
                "desc": "Update activation state with deploy",
                "state": "deploy",
                "expected_status_code": [204, 400, 422],
                "expected_error": ""
            }, {
                "desc": "Update activation state with undeploy",
                "state": "undeploy",
                "expected_status_code": [204, 400, 422],
                "expected_error": ""
            }, {
                "desc": "Update activation state with pause",
                "state": "pause",
                "expected_status_code": [204, 400, 422],
                "expected_error": ""
            }, {
                "desc": "Update activation state with resume",
                "state": "resume",
                "expected_status_code": [204, 400, 422],
                "expected_error": ""
            }, {
                "desc": "Update activation state with invalid state",
                "state": "invalid",
                "expected_status_code": [400, 422],
                "expected_error": ""
            }
        ]
        failures = list()
        for testcase in testcases:
            result = self.api_call_with_retry(
                self.capellaAPI.cluster_ops_apis.update_eventing_function_activation_state_v4,
                self.organisation_id, self.project_id, self.cluster_id,
                self.function_name, testcase["state"])
            self.validate_testcase(result, [204], testcase, failures,
                                   payloadTest=True)
        if failures:
            self.fail("{} tests FAILED out of {} TOTAL tests".format(
                len(failures), len(testcases)))
