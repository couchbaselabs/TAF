"""
Created on June 09, 2026
"""

from pytests.Capella.RestAPIv4.AnalyticsCloudSnapshotBackups.get_analytics_cloud_snapshot_backup_schedule import \
    GetAnalyticsCloudSnapshotBackupSchedule


class DeleteAnalyticsCloudSnapshotBackupSchedule(
        GetAnalyticsCloudSnapshotBackupSchedule):

    def setUp(self,
              nomenclature="Analytics_Cloud_Snapshot_Backup_Schedule_Delete"):
        GetAnalyticsCloudSnapshotBackupSchedule.setUp(self, nomenclature)

    def tearDown(self):
        super(DeleteAnalyticsCloudSnapshotBackupSchedule, self).tearDown()

    def recreate_schedule(self):
        res = self.columnarAPI.upsert_cloud_snapshot_backup_schedule(
            self.organisation_id, self.project_id,
            self.analyticsCluster_id,
            self.schedule_payload["interval"],
            self.schedule_payload["retention"],
            self.schedule_payload["startTime"])
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.columnarAPI.upsert_cloud_snapshot_backup_schedule(
                self.organisation_id, self.project_id,
                self.analyticsCluster_id,
                self.schedule_payload["interval"],
                self.schedule_payload["retention"],
                self.schedule_payload["startTime"])
        if res.status_code != 204:
            self.log.error(res.content)
            self.fail(
                "Analytics cloud snapshot backup schedule recreation failed")

    def test_api_path(self):
        testcases = [
            {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/analyticsClusters/{}"
                       "/cloudSnapshotBackupSchedule",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }, {
                "description": "Replace cloudSnapshotBackupSchedule with "
                               "cloudSnapshotBackupSched",
                "url": "/v4/organizations/{}/projects/{}/analyticsClusters/{}"
                       "/cloudSnapshotBackupSched",
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
                self.columnarAPI.cloud_snapshot_backup_schedule_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]
            elif "invalid_analyticsClusterId" in testcase:
                analytics_cluster = testcase["invalid_analyticsClusterId"]
            result = self.api_call_with_retry(
                self.columnarAPI.delete_cloud_snapshot_backup_schedule,
                organization, project, analytics_cluster)
            self.columnarAPI.cloud_snapshot_backup_schedule_endpoint = \
                "/v4/organizations/{}/projects/{}/analyticsClusters/{}/" \
                "cloudSnapshotBackupSchedule"
            if self.validate_testcase(result, [204], testcase, failures):
                self.recreate_schedule()
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
                self.columnarAPI.delete_cloud_snapshot_backup_schedule,
                self.organisation_id, self.project_id,
                self.analyticsCluster_id, headers=header)
            if self.validate_testcase(result, [204], testcase, failures):
                self.recreate_schedule()
        if failures:
            self.fail("{} tests FAILED.".format(len(failures)))
