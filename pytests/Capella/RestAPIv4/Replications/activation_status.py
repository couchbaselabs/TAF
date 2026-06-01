"""
Created on May 25, 2026

@author: Automation
"""

from pytests.Capella.RestAPIv4.Replications.replication_base import \
    ReplicationBase


class ReplicationActivationStatus(ReplicationBase):

    def setUp(self, nomenclature="Replications_Activation_Status"):
        ReplicationBase.setUp(self, nomenclature)

    def tearDown(self):
        super(ReplicationActivationStatus, self).tearDown()

    def _activate(self, organization, project, cluster, replication_id,
                  headers=None):
        return self.api_call_with_retry(
            self.capellaAPI.cluster_ops_apis.activate_replication,
            organization, project, cluster, replication_id, headers=headers)

    def _deactivate(self, organization, project, cluster, replication_id,
                    headers=None):
        return self.api_call_with_retry(
            self.capellaAPI.cluster_ops_apis.deactivate_replication,
            organization, project, cluster, replication_id, headers=headers)

    def test_api_path(self):
        testcases = [
            {
                "description": "Activate/deactivate replication with valid path"
            }, {
                "description": "ActivationStatus with non-hex organizationID",
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
                "description": "ActivationStatus with non-hex projectID",
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
                "description": "ActivationStatus with non-hex clusterID",
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
                "description": "ActivationStatus with non-hex replicationID",
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

            activate_result = self._activate(
                organization, project, cluster, replication_id)
            self.validate_testcase(
                activate_result, [200, 202, 204, 400, 404, 409, 422],
                testcase, failures)
            deactivate_result = self._deactivate(
                organization, project, cluster, replication_id)
            self.validate_testcase(
                deactivate_result, [200, 202, 204, 400, 404, 409, 422],
                testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner", "projectManager"
        ]):
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            activate_result = self._activate(
                self.organisation_id, self.project_id, self.cluster_id,
                self.replication_id, headers=header)
            self.validate_testcase(
                activate_result, [200, 202, 204, 400, 404, 409, 422],
                testcase, failures)
            deactivate_result = self._deactivate(
                self.organisation_id, self.project_id, self.cluster_id,
                self.replication_id, headers=header)
            self.validate_testcase(
                deactivate_result, [200, 202, 204, 400, 404, 409, 422],
                testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))
