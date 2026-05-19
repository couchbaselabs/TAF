import copy
import time

from pytests.Capella.RestAPIv4.Clusters.get_clusters import GetCluster


class CloudSnapshotBackupBase(GetCluster):

    def setUp(self, nomenclature="Cloud_Snapshot_Backup_Base"):
        GetCluster.setUp(self, nomenclature)
        self.default_retention = 168
        self.backup_id = None

    def tearDown(self):
        super(CloudSnapshotBackupBase, self).tearDown()

    def api_call_with_retry(self, method, *args, **kwargs):
        result = method(*args, **kwargs)
        if result.status_code == 429:
            self.handle_rate_limit(int(result.headers["Retry-After"]))
            result = method(*args, **kwargs)
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
            self.capellaAPI.cluster_ops_apis.list_cloud_snapshot_backups,
            self.organisation_id, self.project_id, self.cluster_id)
        if res.status_code != 200:
            return None
        return self._extract_backup_id(res)

    def ensure_backup(self, wait_for_complete=False):
        res = self.api_call_with_retry(
            self.capellaAPI.cluster_ops_apis.create_cloud_snapshot_backup,
            self.organisation_id, self.project_id, self.cluster_id,
            retention=self.default_retention)
        if res.status_code not in [202, 409]:
            self.log.error(res.content)
            self.fail("Unable to create cloud snapshot backup")

        backup_id = self._extract_backup_id(res) or self.get_existing_backup_id()
        if not backup_id:
            self.fail("Backup ID not found from create/list cloud snapshot backup response")

        if wait_for_complete:
            self.wait_for_backup_complete(backup_id)
        self.backup_id = backup_id
        return backup_id

    def wait_for_backup_complete(self, backup_id, timeout=900):
        end_time = time.time() + timeout
        while time.time() <= end_time:
            res = self.api_call_with_retry(
                self.capellaAPI.cluster_ops_apis.list_cloud_snapshot_backups,
                self.organisation_id, self.project_id, self.cluster_id)
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

    def clone_payload(self):
        res = self.api_call_with_retry(
            self.capellaAPI.cluster_ops_apis.fetch_cluster_info,
            self.organisation_id, self.project_id, self.cluster_id)
        if res.status_code != 200:
            self.log.error(res.content)
            self.fail("Unable to fetch cluster details for clone payload")

        cluster_data = res.json()
        cloud_provider = copy.deepcopy(cluster_data.get("cloudProvider", {}))
        cloud_provider["cidr"] = self.next_cidr(cloud_provider.get("cidr"))

        payload = {
            "name": self.generate_random_string(special_characters=False),
            "description": "Cloud snapshot clone automation",
            "cloudProvider": cloud_provider,
            "availability": copy.deepcopy(cluster_data.get("availability", {"type": "single"})),
            "support": copy.deepcopy(cluster_data.get("support", {"plan": "developer pro", "timezone": "PT"}))
        }

        zones = cluster_data.get("zones")
        if zones:
            payload["zones"] = zones
        return payload

    @staticmethod
    def next_cidr(cidr):
        if not cidr or "/" not in cidr:
            return "10.200.0.0/23"
        ip, mask = cidr.split("/")
        octets = ip.split(".")
        if len(octets) != 4:
            return "10.200.0.0/23"
        try:
            octets[1] = str((int(octets[1]) + 7) % 254 or 1)
            return "{}.{}.{}.{}".format(
                octets[0], octets[1], octets[2], octets[3]) + "/" + mask
        except Exception:
            return "10.200.0.0/23"
