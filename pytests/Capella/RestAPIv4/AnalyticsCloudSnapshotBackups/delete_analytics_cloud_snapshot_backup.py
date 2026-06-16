"""
Created on June 09, 2026
"""

from pytests.Capella.RestAPIv4.AnalyticsCloudSnapshotBackups.update_analytics_cloud_snapshot_backup_retention import \
    UpdateAnalyticsCloudSnapshotBackupRetention


class DeleteAnalyticsCloudSnapshotBackup(
        UpdateAnalyticsCloudSnapshotBackupRetention):

    def setUp(self, nomenclature="Analytics_Cloud_Snapshot_Backup_Delete"):
        UpdateAnalyticsCloudSnapshotBackupRetention.setUp(
            self, nomenclature)

    def tearDown(self):
        super(DeleteAnalyticsCloudSnapshotBackup, self).tearDown()

    def recreate_backup(self):
        self.backup_id = self.ensure_backup()

    def test_api_path(self):
        testcases = [
            {
                "description": "Delete cloud snapshot backup with valid path"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/analyticsClusters/{}/cloudSnapshotBackups",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }, {
                "description": "Replace cloudSnapshotBackups with cloudSnapshotBackup",
                "url": "/v4/organizations/{}/projects/{}/analyticsClusters/{}/cloudSnapshotBackup",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
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
                self.columnarAPI.delete_cloud_snapshot_backup,
                organization, project, analytics_cluster, backup)
            if self.validate_testcase(result, [202], testcase, failures):
                self.recreate_backup()
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
                self.columnarAPI.delete_cloud_snapshot_backup,
                self.organisation_id, self.project_id,
                self.analyticsCluster_id, self.backup_id, headers=header)
            if self.validate_testcase(result, [202], testcase, failures):
                self.recreate_backup()
        if failures:
            self.fail("{} tests FAILED.".format(len(failures)))
