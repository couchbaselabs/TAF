"""
Created on June 09, 2026
"""

from pytests.Capella.RestAPIv4.AnalyticsCloudSnapshotBackups.analytics_cloud_snapshot_backup_base import \
    AnalyticsCloudSnapshotBackupBase


class UpdateAnalyticsCloudSnapshotBackupRetention(
        AnalyticsCloudSnapshotBackupBase):

    def setUp(self, nomenclature="Analytics_Cloud_Snapshot_Backup_Retention_Update"):
        AnalyticsCloudSnapshotBackupBase.setUp(self, nomenclature)
        self.backup_id = self.ensure_backup()

    def tearDown(self):
        super(UpdateAnalyticsCloudSnapshotBackupRetention, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Update retention with valid path"
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
                self.columnarAPI.update_cloud_snapshot_backup_retention,
                organization, project, analytics_cluster, backup, 144)
            self.columnarAPI.cloud_snapshot_backups_endpoint = \
                self.backups_endpoint_default
            self.validate_testcase(result, [204], testcase, failures)
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
                self.columnarAPI.update_cloud_snapshot_backup_retention,
                self.organisation_id, self.project_id,
                self.analyticsCluster_id, self.backup_id, 144,
                headers=header)
            self.validate_testcase(result, [204], testcase, failures)
        if failures:
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_payload(self):
        testcases = [
            {
                "desc": "Update with valid retention",
                "retention": 144
            }, {
                "desc": "Update with invalid retention type (string)",
                "retention": "240",
                "expected_status_code": 400
            }, {
                "desc": "Update with zero retention",
                "retention": 0,
                "expected_status_code": [400, 422]
            }, {
                "desc": "Update with negative retention",
                "retention": -1,
                "expected_status_code": 400
            }
        ]
        failures = list()
        for testcase in testcases:
            result = self.api_call_with_retry(
                self.columnarAPI.update_cloud_snapshot_backup_retention,
                self.organisation_id, self.project_id,
                self.analyticsCluster_id, self.backup_id,
                testcase["retention"])
            self.validate_testcase(result, [204], testcase, failures,
                                   payloadTest=True)
        if failures:
            self.fail("{} tests FAILED out of {} TOTAL tests".format(
                len(failures), len(testcases)))
