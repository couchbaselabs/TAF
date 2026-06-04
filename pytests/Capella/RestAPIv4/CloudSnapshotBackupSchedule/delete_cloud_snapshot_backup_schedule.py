"""
Created on May 15, 2026
"""

from pytests.Capella.RestAPIv4.CloudSnapshotBackupSchedule. \
    get_cloud_snapshot_backup_schedule import GetCloudSnapshotBackupSchedule


class DeleteCloudSnapshotBackupSchedule(GetCloudSnapshotBackupSchedule):

    def setUp(self, nomenclature="Cloud_Snapshot_Backup_Schedule_Delete"):
        GetCloudSnapshotBackupSchedule.setUp(self, nomenclature)

    def tearDown(self):
        super(DeleteCloudSnapshotBackupSchedule, self).tearDown()

    def recreate_schedule(self):
        res = self.capellaAPI.cluster_ops_apis. \
            upsert_cloud_snapshot_backup_schedule(
                self.organisation_id, self.project_id, self.cluster_id,
                self.schedule_payload["interval"],
                self.schedule_payload["retention"],
                self.schedule_payload["startTime"])
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis. \
                upsert_cloud_snapshot_backup_schedule(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.schedule_payload["interval"],
                    self.schedule_payload["retention"],
                    self.schedule_payload["startTime"])
        if res.status_code != 204:
            self.log.error(res.content)
            self.fail("Cloud snapshot backup schedule recreation failed")

    def test_api_path(self):
        testcases = [
            {
                "description": "Send call with valid path params"
            }, {
                "description": "Send call with non-hex organizationID",
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
                "description": "Send call with non-hex projectID",
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
                "description": "Send call with non-hex clusterID",
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
            }
        ]
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            organization = self.organisation_id
            project = self.project_id
            cluster = self.cluster_id

            if "invalid_organizationID" in testcase:
                organization = testcase["invalid_organizationID"]
            elif "invalid_projectID" in testcase:
                project = testcase["invalid_projectID"]
            elif "invalid_clusterID" in testcase:
                cluster = testcase["invalid_clusterID"]

            result = self.capellaAPI.cluster_ops_apis. \
                delete_cloud_snapshot_backup_schedule(
                    organization, project, cluster)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis. \
                    delete_cloud_snapshot_backup_schedule(
                        organization, project, cluster)
            if self.validate_testcase(result, [204], testcase, failures):
                self.recreate_schedule()

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner", "projectManager",
            "projectViewer", "projectDataReader", "projectDataReaderWriter"
        ], None):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header, self.project_id)
            result = self.capellaAPI.cluster_ops_apis. \
                delete_cloud_snapshot_backup_schedule(
                    self.organisation_id, self.project_id, self.cluster_id,
                    headers=header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis. \
                    delete_cloud_snapshot_backup_schedule(
                        self.organisation_id, self.project_id, self.cluster_id,
                        headers=header)
            if self.validate_testcase(result, [204], testcase, failures):
                self.recreate_schedule()

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))
