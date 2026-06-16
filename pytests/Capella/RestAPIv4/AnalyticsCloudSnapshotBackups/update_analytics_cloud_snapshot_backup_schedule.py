"""
Created on June 09, 2026
"""

import copy

from pytests.Capella.RestAPIv4.AnalyticsCloudSnapshotBackups.get_analytics_cloud_snapshot_backup_schedule import \
    GetAnalyticsCloudSnapshotBackupSchedule


class UpdateAnalyticsCloudSnapshotBackupSchedule(
        GetAnalyticsCloudSnapshotBackupSchedule):

    def setUp(self,
              nomenclature="Analytics_Cloud_Snapshot_Backup_Schedule_Update"):
        GetAnalyticsCloudSnapshotBackupSchedule.setUp(self, nomenclature)

    def tearDown(self):
        super(UpdateAnalyticsCloudSnapshotBackupSchedule, self).tearDown()

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
                self.columnarAPI.upsert_cloud_snapshot_backup_schedule,
                organization, project, analytics_cluster,
                12, 144, self.build_start_time(3))
            self.columnarAPI.cloud_snapshot_backup_schedule_endpoint = \
                "/v4/organizations/{}/projects/{}/analyticsClusters/{}/" \
                "cloudSnapshotBackupSchedule"
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
                self.columnarAPI.upsert_cloud_snapshot_backup_schedule,
                self.organisation_id, self.project_id,
                self.analyticsCluster_id,
                12, 144, self.build_start_time(3), headers=header)
            self.validate_testcase(result, [204], testcase, failures)
        if failures:
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_payload(self):
        payload = {
            "interval": 24,
            "retention": 168,
            "startTime": self.build_start_time(4),
        }
        testcases = [
            {
                "desc": "Invalid payload type for interval (string)",
                "payload": dict(payload, interval="24"),
                "expected_status_code": 400
            }, {
                "desc": "Invalid payload type for retention (string)",
                "payload": dict(payload, retention="168"),
                "expected_status_code": 400
            }, {
                "desc": "Invalid payload type for startTime (integer)",
                "payload": dict(payload, startTime=20260601),
                "expected_status_code": 400,
                "expected_error": self.expected_invalid_uuid_error()
            }, {
                "desc": "Invalid startTime format (not ISO 8601)",
                "payload": dict(payload,
                                startTime="2026/06/01 16:00:00"),
                "expected_status_code": 400
            }
        ]
        failures = list()
        for testcase in testcases:
            pld = testcase["payload"]
            result = self.api_call_with_retry(
                self.columnarAPI.upsert_cloud_snapshot_backup_schedule,
                self.organisation_id, self.project_id,
                self.analyticsCluster_id,
                pld["interval"], pld["retention"], pld["startTime"])
            self.validate_testcase(result, [204], testcase, failures,
                                   payloadTest=True)
        if failures:
            self.fail("{} tests FAILED out of {} TOTAL tests".format(
                len(failures), len(testcases)))
