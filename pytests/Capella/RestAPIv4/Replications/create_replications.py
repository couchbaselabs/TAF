"""
Created on May 25, 2026

@author: Automation
"""

from pytests.Capella.RestAPIv4.Replications.replication_base import \
    ReplicationBase


class CreateReplication(ReplicationBase):

    def setUp(self, nomenclature="Replications_Create"):
        ReplicationBase.setUp(self, nomenclature)

    def tearDown(self):
        super(CreateReplication, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Create replication with valid path"
            }, {
                "description": "Create replication with non-hex organizationID",
                "invalid_organizationID": self.replace_last_character(
                    self.organisation_id, non_hex=True),
                "expected_status_code": 400
            }, {
                "description": "Create replication with non-hex projectID",
                "invalid_projectID": self.replace_last_character(
                    self.project_id, non_hex=True),
                "expected_status_code": 400
            }, {
                "description": "Create replication with non-hex clusterID",
                "invalid_clusterID": self.replace_last_character(
                    self.cluster_id, non_hex=True),
                "expected_status_code": 400
            }
        ]

        failures = list()
        for testcase in testcases:
            organization = self.organisation_id
            project = self.project_id
            cluster = self.cluster_id

            if "invalid_organizationID" in testcase:
                organization = testcase["invalid_organizationID"]
            elif "invalid_projectID" in testcase:
                project = testcase["invalid_projectID"]
            elif "invalid_clusterID" in testcase:
                cluster = testcase["invalid_clusterID"]

            result = self.api_call_with_retry(
                self.capellaAPI.cluster_ops_apis.create_replication,
                organization, project, cluster, self.payload_copy())
            self.validate_testcase(
                result, [201, 202, 400, 404, 409, 422], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests".format(
                len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner"
        ], None):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header, self.project_id)
            result = self.api_call_with_retry(
                self.capellaAPI.cluster_ops_apis.create_replication,
                self.organisation_id, self.project_id, self.cluster_id,
                self.payload_copy(), headers=header)
            self.validate_testcase(
                result, [201, 202, 400, 404, 409, 422], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_payload(self):
        testcases = [
            {
                "desc": "Valid replication payload",
                "payload": self.payload_copy()
            }, {
                "desc": "Invalid direction type",
                "payload": dict(self.payload_copy(), direction=1),
                "expected_status_code": 400
            }, {
                "desc": "Missing target block",
                "payload": dict(self.payload_copy(), target=None),
                "expected_status_code": 400
            }, {
                "desc": "Invalid settings type",
                "payload": dict(self.payload_copy(), settings="invalid"),
                "expected_status_code": 400
            }
        ]

        failures = list()
        for testcase in testcases:
            result = self.api_call_with_retry(
                self.capellaAPI.cluster_ops_apis.create_replication,
                self.organisation_id, self.project_id, self.cluster_id,
                testcase["payload"])
            self.validate_testcase(
                result, [201, 202, 400, 404, 409, 422], testcase, failures,
                payloadTest=True)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests".format(
                len(failures), len(testcases)))
