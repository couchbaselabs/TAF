from pytests.Capella.RestAPIv4.CloudSnapshotBackups.cloud_snapshot_backup_base import \
    CloudSnapshotBackupBase


class ListProjectLevelCloudSnapshotBackups(CloudSnapshotBackupBase):

    def setUp(self, nomenclature="Project_Level_Cloud_Snapshot_Backups_List"):
        CloudSnapshotBackupBase.setUp(self, nomenclature)

    def tearDown(self):
        super(ListProjectLevelCloudSnapshotBackups, self).tearDown()

    def test_api_path(self):
        testcases = [
            {"description": "List project level cloud snapshot backups with valid path"},
            {
                "description": "List project level cloud snapshot backups with non-hex organizationID",
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
                "description": "List project level cloud snapshot backups with non-hex projectID",
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
                self.capellaAPI.cluster_ops_apis.list_project_level_cloud_snapshot_backups,
                organization, project)
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests".format(
                len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner", "projectManager"
        ]):
            header = dict()
            self.auth_test_setup(
                testcase, failures, header, self.project_id, self.other_project_id)
            result = self.api_call_with_retry(
                self.capellaAPI.cluster_ops_apis.list_project_level_cloud_snapshot_backups,
                self.organisation_id, self.project_id, headers=header)
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))
