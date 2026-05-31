"""
Created on May 25, 2026

@author: Automation
"""

from pytests.Capella.RestAPIv4.Replications.replication_base import \
    ReplicationBase


class ListProjectReplication(ReplicationBase):

    def setUp(self, nomenclature="Project_Replications_List"):
        ReplicationBase.setUp(self, nomenclature)

    def tearDown(self):
        super(ListProjectReplication, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "List project-level replications with valid path"
            }, {
                "description": "List project-level replications with non-hex organizationID",
                "invalid_organizationID": self.replace_last_character(
                    self.organisation_id, non_hex=True),
                "expected_status_code": 400
            }, {
                "description": "List project-level replications with non-hex projectID",
                "invalid_projectID": self.replace_last_character(
                    self.project_id, non_hex=True),
                "expected_status_code": 400
            }
        ]

        failures = list()
        for testcase in testcases:
            organization = self.organisation_id
            project = self.project_id

            if "invalid_organizationID" in testcase:
                organization = testcase["invalid_organizationID"]
            elif "invalid_projectID" in testcase:
                project = testcase["invalid_projectID"]

            result = self.api_call_with_retry(
                self.capellaAPI.cluster_ops_apis.list_project_replications,
                organization, project)
            self.validate_testcase(result, [200, 404], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests".format(
                len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner", "projectManager",
            "projectViewer", "projectDataReader", "projectDataReaderWriter"
        ]):
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = self.api_call_with_retry(
                self.capellaAPI.cluster_ops_apis.list_project_replications,
                self.organisation_id, self.project_id, headers=header)
            self.validate_testcase(result, [200, 404], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))
