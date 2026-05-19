from pytests.Capella.RestAPIv4.CloudSnapshotBackups.cloud_snapshot_backup_base import \
    CloudSnapshotBackupBase


class UpdateCloudSnapshotBackupRetention(CloudSnapshotBackupBase):

    def setUp(self, nomenclature="Cloud_Snapshot_Backup_Retention_Update"):
        CloudSnapshotBackupBase.setUp(self, nomenclature)
        self.backup_id = self.ensure_backup()

    def tearDown(self):
        super(UpdateCloudSnapshotBackupRetention, self).tearDown()

    def test_api_path(self):
        testcases = [
            {"description": "Update cloud snapshot retention with valid path"},
            {
                "description": "Update cloud snapshot retention with non-hex organizationID",
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
                "description": "Update cloud snapshot retention with non-hex projectID",
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
                "description": "Update cloud snapshot retention with non-hex clusterID",
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
                "description": "Update cloud snapshot retention with non-hex backupID",
                "invalid_backupID": self.replace_last_character(
                    self.backup_id, non_hex=True),
                "expected_status_code": 400,"expected_error": {
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
            backup = self.backup_id

            if "invalid_organizationID" in testcase:
                organization = testcase["invalid_organizationID"]
            elif "invalid_projectID" in testcase:
                project = testcase["invalid_projectID"]
            elif "invalid_clusterID" in testcase:
                cluster = testcase["invalid_clusterID"]
            elif "invalid_backupID" in testcase:
                backup = testcase["invalid_backupID"]

            result = self.api_call_with_retry(
                self.capellaAPI.cluster_ops_apis.edit_cloud_snapshot_backup_retention,
                organization, project, cluster, backup, 336)
            self.validate_testcase(result, [204], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests".format(
                len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner"
        ]):
            header = dict()
            self.auth_test_setup(
                testcase, failures, header, self.project_id, self.other_project_id)
            result = self.api_call_with_retry(
                self.capellaAPI.cluster_ops_apis.edit_cloud_snapshot_backup_retention,
                self.organisation_id, self.project_id, self.cluster_id,
                self.backup_id, 336, headers=header)
            self.validate_testcase(result, [204], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_payload(self):
        testcases = [
            {
                "desc": "Valid cloud snapshot retention payload",
                "retention": 240
            }, {
                "desc": "Invalid retention type",
                "retention": "240",
                "expected_status_code": 422
            }
        ]
        failures = list()
        for testcase in testcases:
            result = self.api_call_with_retry(
                self.capellaAPI.cluster_ops_apis.edit_cloud_snapshot_backup_retention,
                self.organisation_id, self.project_id, self.cluster_id,
                self.backup_id, testcase["retention"])
            self.validate_testcase(result, [204], testcase, failures,
                                   payloadTest=True)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests".format(
                len(failures), len(testcases)))
