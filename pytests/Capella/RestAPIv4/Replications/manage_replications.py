"""
Created on May 25, 2026

@author: Automation
"""

from pytests.Capella.RestAPIv4.Replications.replication_base import \
    ReplicationBase


class ManageReplication(ReplicationBase):

    def setUp(self, nomenclature="Replications_Manage"):
        ReplicationBase.setUp(self, nomenclature)

    def tearDown(self):
        super(ManageReplication, self).tearDown()

    def _call_by_operation(
            self, operation, organization, project, cluster, replication_id,
            headers=None, payload=None):
        if operation == "get":
            return self.api_call_with_retry(
                self.capellaAPI.cluster_ops_apis.fetch_replication_info,
                organization, project, cluster, replication_id, headers=headers)
        if operation == "put":
            return self.api_call_with_retry(
                self.capellaAPI.cluster_ops_apis.update_replication,
                organization, project, cluster, replication_id,
                payload if payload else self.payload_copy(), headers=headers)
        return self.api_call_with_retry(
            self.capellaAPI.cluster_ops_apis.delete_replication,
            organization, project, cluster, replication_id, headers=headers)

    def test_api_path(self):
        operations = [ "put", "get","delete"]
        testcases = [
            # {
            #     "description": "Manage replication with valid path"
            # }, 
            {
                "description": "Manage replication with non-hex organizationID",
                "invalid_organizationID": self.replace_last_character(
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
                "description": "Manage replication with non-hex projectID",
                "invalid_projectID": self.replace_last_character(
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
                "description": "Manage replication with non-hex clusterID",
                "invalid_clusterID": self.replace_last_character(
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
                "description": "Manage replication with non-hex replicationID",
                "invalid_replicationID": self.replace_last_character(
                    self.replication_id, non_hex=True),
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
        for operation in operations:
            for testcase in testcases:
                organization = self.organisation_id
                project = self.project_id
                cluster = self.cluster_id
                replication_id = self.replication_id

                if "invalid_organizationID" in testcase:
                    organization = testcase["invalid_organizationID"]
                elif "invalid_projectID" in testcase:
                    project = testcase["invalid_projectID"]
                elif "invalid_clusterID" in testcase:
                    cluster = testcase["invalid_clusterID"]
                elif "invalid_replicationID" in testcase:
                    replication_id = testcase["invalid_replicationID"]

                result = self._call_by_operation(
                    operation, organization, project, cluster, replication_id)
                self.validate_testcase(
                    result, [200, 202, 204, 400, 404, 409, 422], testcase,
                    failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner", "projectManager",
            "projectViewer", "projectDataReader", "projectDataReaderWriter"
        ], None):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header, self.project_id)
            get_result = self._call_by_operation(
                "get", self.organisation_id, self.project_id, self.cluster_id,
                self.replication_id, headers=header)
            self.validate_testcase(
                get_result, [200, 404], testcase, failures)

        mutate_testcases = self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner", "projectManager"
        ])
        for testcase in mutate_testcases:
            header = dict()
            self.auth_test_setup(testcase, failures, header, self.project_id)
            put_result = self._call_by_operation(
                "put", self.organisation_id, self.project_id, self.cluster_id,
                self.replication_id, headers=header)
            self.validate_testcase(
                put_result, [200, 202, 400, 404, 409, 422], testcase, failures)

            del_result = self._call_by_operation(
                "delete", self.organisation_id, self.project_id,
                self.cluster_id, self.replication_id, headers=header)
            self.validate_testcase(
                del_result, [204, 400, 404, 409, 422], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_payload(self):
        testcases = [
            {
                "desc": "Valid update payload",
                "payload": self.payload_copy()
            }, {
                "desc": "Invalid direction type",
                "payload": dict(self.payload_copy(), direction=False),
                "expected_status_code": 400
            }, {
                "desc": "Invalid target type",
                "payload": dict(self.payload_copy(), target="target"),
                "expected_status_code": 400
            }
        ]

        failures = list()
        for testcase in testcases:
            result = self._call_by_operation(
                "put", self.organisation_id, self.project_id, self.cluster_id,
                self.replication_id, payload=testcase["payload"])
            self.validate_testcase(
                result, [200, 202, 400, 404, 409, 422], testcase, failures,
                payloadTest=True)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests".format(
                len(failures), len(testcases)))
