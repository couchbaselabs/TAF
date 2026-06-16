"""
Created on June 09, 2026
"""

from pytests.Capella.RestAPIv4.AnalyticsCloudSnapshotBackups.update_analytics_cloud_snapshot_backup_retention import \
    UpdateAnalyticsCloudSnapshotBackupRetention


class RestoreAnalyticsCloudSnapshotBackup(
        UpdateAnalyticsCloudSnapshotBackupRetention):

    def setUp(self, nomenclature="Analytics_Cloud_Snapshot_Backup_Restore"):
        UpdateAnalyticsCloudSnapshotBackupRetention.setUp(
            self, nomenclature)
        if not self.wait_for_backup_complete(self.backup_id, timeout=1800):
            self.fail("Backup {} did not complete within timeout; cannot "
                      "run restore tests".format(self.backup_id))

    def tearDown(self):
        super(RestoreAnalyticsCloudSnapshotBackup, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Restore cloud snapshot backup with valid path"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/analyticsClusters/{}/cloudSnapshotBackups",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }, {
                "description": "Call API with non-hex organizationId",
                "invalid_organizationId": self.replace_last_character(
                    self.organisation_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": self.expected_invalid_uuid_error()
            }, {
                "description": "Call API with non-hex projectId",
                "invalid_projectId": self.replace_last_character(
                    self.project_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": self.expected_invalid_uuid_error()
            }, {
                "description": "Call API with non-hex analyticsClusterId",
                "invalid_analyticsClusterId": self.replace_last_character(
                    self.analyticsCluster_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": self.expected_invalid_uuid_error()
            }, {
                "description": "Call API with non-hex backupId",
                "invalid_backupId": self.replace_last_character(
                    self.backup_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": self.expected_invalid_uuid_error()
            }
        ]
        failures = list()
        for testcase in testcases:
            organization = self.organisation_id
            project = self.project_id
            analytics_cluster = self.analyticsCluster_id
            backup = self.backup_id
            if "url" in testcase:
                self.columnarAPI.cloud_snapshot_backups_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]
            elif "invalid_analyticsClusterId" in testcase:
                analytics_cluster = testcase["invalid_analyticsClusterId"]
            elif "invalid_backupId" in testcase:
                backup = testcase["invalid_backupId"]
            result = self.api_call_with_retry(
                self.columnarAPI.restore_cloud_snapshot_backup,
                organization, project, analytics_cluster, backup)
            self.columnarAPI.cloud_snapshot_backups_endpoint = \
                self.backups_endpoint_default
            self.validate_testcase(result, [202, 409], testcase, failures)
        if failures:
            self.fail("{} tests FAILED out of {} TOTAL tests".format(
                len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner"
        ]):
            header = dict()
            self.auth_test_setup(
                testcase, failures, header,
                self.project_id, self.other_project_id)
            result = self.api_call_with_retry(
                self.columnarAPI.restore_cloud_snapshot_backup,
                self.organisation_id, self.project_id,
                self.analyticsCluster_id, self.backup_id, headers=header)
            self.validate_testcase(result, [202, 409], testcase, failures)
        if failures:
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_payload(self):
        testcases = [
            {
                "desc": "Restore with no extra payload",
                "expected_status_code": [202, 409],
                "expected_error": ""
            }, {
                "desc": "Restore with invalid extra parameter",
                "sourceRegion": "invalid-region",
                "expected_status_code": [400, 422]
            }
        ]
        failures = list()
        for testcase in testcases:
            extra = {}
            if "sourceRegion" in testcase:
                extra["sourceRegion"] = testcase["sourceRegion"]
            result = self.api_call_with_retry(
                self.columnarAPI.restore_cloud_snapshot_backup,
                self.organisation_id, self.project_id,
                self.analyticsCluster_id, self.backup_id, **extra)
            self.validate_testcase(result, [202, 409], testcase, failures,
                                   payloadTest=True)
        if failures:
            self.fail("{} tests FAILED out of {} TOTAL tests".format(
                len(failures), len(testcases)))
