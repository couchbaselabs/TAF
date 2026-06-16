"""
Created on June 09, 2026
"""

from pytests.Capella.RestAPIv4.AnalyticsCloudSnapshotBackups.analytics_cloud_snapshot_backup_base import \
    AnalyticsCloudSnapshotBackupBase


class ListAnalyticsCloudSnapshotBackups(AnalyticsCloudSnapshotBackupBase):

    def setUp(self, nomenclature="Analytics_Cloud_Snapshot_Backups_List"):
        AnalyticsCloudSnapshotBackupBase.setUp(self, nomenclature)

    def tearDown(self):
        super(ListAnalyticsCloudSnapshotBackups, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "List cloud snapshot backups with valid path"
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
                "description": "Add invalid segment to URI",
                "url": "/v4/organizations/{}/projects/{}/analyticsClusters/{}/cloudSnapshotBackups/invalid",
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
            }
        ]
        failures = list()
        for testcase in testcases:
            organization = self.organisation_id
            project = self.project_id
            analytics_cluster = self.analyticsCluster_id
            if "url" in testcase:
                self.columnarAPI.cloud_snapshot_backups_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]
            elif "invalid_analyticsClusterId" in testcase:
                analytics_cluster = testcase["invalid_analyticsClusterId"]
            result = self.api_call_with_retry(
                self.columnarAPI.list_cloud_snapshot_backups,
                organization, project, analytics_cluster)
            self.columnarAPI.cloud_snapshot_backups_endpoint = \
                self.backups_endpoint_default
            self.validate_testcase(result, [200], testcase, failures)
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
                self.columnarAPI.list_cloud_snapshot_backups,
                self.organisation_id, self.project_id,
                self.analyticsCluster_id, headers=header)
            self.validate_testcase(result, [200], testcase, failures)
        if failures:
            self.fail("{} tests FAILED.".format(len(failures)))
