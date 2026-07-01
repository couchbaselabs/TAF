"""
Created on June 09, 2026
"""

import time

from pytests.Capella.RestAPIv4.ClustersColumnar.get_analytics_clusters import \
    GetAnalyticsClusters


class AnalyticsCloudSnapshotBackupBase(GetAnalyticsClusters):

    def setUp(self, nomenclature="Analytics_Cloud_Snapshot_Backup_Base"):
        GetAnalyticsClusters.setUp(self, nomenclature)
        self.default_retention = 168
        self.backup_id = None
        self.backups_endpoint_default = \
            "/v4/organizations/{}/projects/{}/analyticsClusters/{}/cloudSnapshotBackups"

    def tearDown(self):
        self.update_auth_with_api_token(self.curr_owner_key)
        super(AnalyticsCloudSnapshotBackupBase, self).tearDown()

    def api_call_with_retry(self, method, *args, **kwargs):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = method(*args, **kwargs)
            except SystemExit as e:
                if attempt < max_retries - 1:
                    self.log.warning(
                        "Connection error on attempt {}/{}, retrying in 5s:"
                        " {}".format(attempt + 1, max_retries, str(e)))
                    time.sleep(5)
                    continue
                self.fail("API call failed after {} attempts: {}".format(
                    max_retries, str(e)))
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                continue
            if result.status_code >= 500 and attempt < max_retries - 1:
                self.log.warning(
                    "Server error {} on attempt {}/{}, retrying in 5s..."
                    .format(result.status_code, attempt + 1, max_retries))
                time.sleep(5)
                continue
            return result
        return result

    @staticmethod
    def _extract_backup_id(result):
        try:
            content = result.json()
        except Exception:
            return None
        if isinstance(content, dict):
            if "backupId" in content:
                return content["backupId"]
            if "data" in content and isinstance(content["data"], list):
                for item in content["data"]:
                    if isinstance(item, dict):
                        data = item.get("data", item)
                        if isinstance(data, dict) and data.get("id"):
                            return data["id"]
        return None

    def get_existing_backup_id(self):
        res = self.api_call_with_retry(
            self.columnarAPI.list_cloud_snapshot_backups,
            self.organisation_id, self.project_id,
            self.analyticsCluster_id)
        if res.status_code != 200:
            return None
        return self._extract_backup_id(res)

    def ensure_backup(self, wait_for_complete=False):
        res = self.api_call_with_retry(
            self.columnarAPI.create_cloud_snapshot_backup,
            self.organisation_id, self.project_id,
            self.analyticsCluster_id, retention=self.default_retention)
        if res.status_code not in [202, 409]:
            self.log.error(res.content)
            self.fail("Unable to create analytics cloud snapshot backup")

        backup_id = self._extract_backup_id(res) or self.get_existing_backup_id()
        if not backup_id:
            self.fail("Backup ID not found from create/list response")

        if wait_for_complete:
            self.wait_for_backup_complete(backup_id)
        self.backup_id = backup_id
        return backup_id

    def wait_for_backup_complete(self, backup_id, timeout=900):
        end_time = time.time() + timeout
        while time.time() <= end_time:
            res = self.api_call_with_retry(
                self.columnarAPI.list_cloud_snapshot_backups,
                self.organisation_id, self.project_id,
                self.analyticsCluster_id)
            if res.status_code != 200:
                time.sleep(20)
                continue
            try:
                items = res.json().get("data", [])
            except Exception:
                items = []
            for item in items:
                data = item.get("data", item) if isinstance(item, dict) else {}
                if data.get("id") == backup_id:
                    status = data.get("progress", {}).get("status")
                    if status == "complete":
                        return True
            time.sleep(20)
        return False

    @staticmethod
    def expected_invalid_uuid_error():
        return {
            "code": 1000,
            "hint": "Check if you have provided a valid URL and all "
                    "the required params are present in the request body.",
            "httpStatusCode": 400,
            "message": "The server cannot or will not process the request "
                       "due to something that is perceived to be a client "
                       "error."
        }
