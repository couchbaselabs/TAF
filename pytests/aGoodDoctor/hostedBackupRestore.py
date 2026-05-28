import json
import time

from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI as dedicatedCapellaAPI
from capella_utils.dedicated import CapellaUtils as CapellaAPI
from global_vars import logger


class DoctorHostedBackupRestore:

    def __init__(self, pod):
        self.pod = pod
        self.log = logger.get("test")

    def backup_now(self, tenant, cluster, bucket, timeout=30, wait_for_backup=True):
        CapellaAPI.backup_now(pod=self.pod, tenant=tenant, cluster_id=cluster.id,
                              bucket_name=bucket.name)
        time.sleep(180)
        
        time_now = time.time()
        capella_api = dedicatedCapellaAPI(self.pod.url_public, tenant.api_secret_key,
                                          tenant.api_access_key, tenant.user,
                                          tenant.pwd)
        while time.time() - time_now < timeout * 60:
            jobs_response = CapellaAPI.jobs(capella_api, self.pod, tenant, cluster.id)
            backup_job = False
            for item in jobs_response['data']:
                if item['data']['jobType'] == 'backup' and item['data']['completionPercentage'] < 100:
                    self.log.info("Backup is running. Backup completion percentage:{}".format(
                        item['data']['completionPercentage']))
                    backup_job = True
                    if not wait_for_backup:
                        return
                    time.sleep(60)
            if backup_job is False:
                return

    def restore_from_backup(self, tenant, cluster, bucket, timeout=300):
        CapellaAPI.restore_from_backup(pod=self.pod, tenant=tenant, cluster_id=cluster.id,
                                       bucket_name=bucket.name)
        time.sleep(180)
        time_now = time.time()
        capella_api = dedicatedCapellaAPI(self.pod.url_public, tenant.api_secret_key,
                                          tenant.api_access_key, tenant.user,
                                          tenant.pwd)
        while time.time() - time_now < timeout:
            jobs_response = CapellaAPI.jobs(capella_api, self.pod, tenant, cluster.id)
            if not jobs_response['data']:
                break
            else:
                for item in jobs_response['data']:
                    if item['data']['jobType'] == 'restore' and item['data']['completionPercentage'] < 100:
                        self.log.info("Restore is running. Restore completion percentage:{}".format(
                            item['data']['completionPercentage']))
                        time.sleep(60)

    def list_all_backups(self, tenant, cluster, bucket):
        resp = CapellaAPI.list_all_backups(pod=self.pod, tenant=tenant, cluster=cluster,
                                           bucket_name=bucket.name)
        self.log.info("List all backups response is {}".format(resp))
        return resp

    def backup_now_and_get_id(self, tenant, cluster, bucket, timeout=60):
        """
        Trigger a backup, wait for it to complete, and return the backup record ID.

        :param tenant: Tenant object
        :param cluster: Cluster object
        :param bucket: Bucket object
        :param timeout: Max minutes to wait for backup completion
        :return: Backup record ID string, or None on failure
        """
        capella_api = dedicatedCapellaAPI(self.pod.url_public, tenant.api_secret_key,
                                          tenant.api_access_key, tenant.user,
                                          tenant.pwd)
        before_trigger = time.time()
        CapellaAPI.backup_now(pod=self.pod, tenant=tenant, cluster_id=cluster.id,
                              bucket_name=bucket.name)
        time.sleep(60)

        time_now = time.time()
        while time.time() - time_now < timeout * 60:
            jobs_response = CapellaAPI.jobs(capella_api, self.pod, tenant, cluster.id)
            backup_running = False
            for item in jobs_response.get("data", []):
                if (item["data"]["jobType"] == "backup"
                        and item["data"]["completionPercentage"] < 100):
                    self.log.info("Backup completion: {}%".format(
                        item["data"]["completionPercentage"]))
                    backup_running = True
                    time.sleep(60)
                    break
            if not backup_running:
                break

        resp = self.list_all_backups(tenant, cluster, bucket)
        try:
            backups = resp.json().get("backups", {}).get("data", [])
        except Exception:
            self.log.error("Failed to parse list_all_backups response")
            return None

        for item in backups:
            backup_data = item.get("data", {})
            created_at = backup_data.get("createdAt", "")
            backup_id = backup_data.get("id")
            if backup_id and created_at:
                try:
                    from datetime import datetime, timezone
                    ts = datetime.fromisoformat(created_at.rstrip("Z")).replace(
                        tzinfo=timezone.utc).timestamp()
                    if ts >= before_trigger:
                        self.log.info(f"Found backup ID {backup_id} created after trigger")
                        return backup_id
                except Exception:
                    pass

        if backups:
            backup_id = backups[0].get("data", {}).get("id")
            self.log.info(f"Returning most recent backup ID: {backup_id}")
            return backup_id

        self.log.error("No backup found after backup_now_and_get_id")
        return None

    def restore_to_cluster(self, tenant, source_cluster, target_cluster, bucket, timeout=300):
        """
        Restore the latest backup from source_cluster's bucket onto target_cluster.

        Supports cross-cluster restore: source and target may differ.

        :param tenant: Tenant object
        :param source_cluster: Cluster whose backup will be restored
        :param target_cluster: Cluster that will receive the restored data
        :param bucket: Bucket object (from source_cluster)
        :param timeout: Max seconds to wait for restore completion
        """
        capella_api = dedicatedCapellaAPI(self.pod.url_public, tenant.api_secret_key,
                                          tenant.api_access_key, tenant.user,
                                          tenant.pwd)
        bucket_id = capella_api.get_backups_bucket_id(
            tenant_id=tenant.id,
            project_id=tenant.projects[0],
            cluster_id=source_cluster.id,
            bucket_name=bucket.name
        )
        payload = {
            "sourceClusterId": source_cluster.id,
            "targetClusterId": target_cluster.id,
            "options": {
                "services": ["data", "query", "index", "search"],
                "filterKeys": "",
                "filterValues": "",
                "mapData": "",
                "includeData": "",
                "excludeData": "",
                "autoCreateBuckets": True,
                "autoRemoveCollections": True,
                "forceUpdates": True,
            },
        }
        url = (
            "{}/v2/organizations/{}/projects/{}/clusters/{}/buckets/{}/restore"
            .format(capella_api.internal_url, tenant.id, tenant.projects[0],
                    source_cluster.id, bucket_id)
        )
        resp = capella_api.do_internal_request(url, method="POST", params=json.dumps(payload))
        self.log.info(
            f"restore_to_cluster {source_cluster.id} -> {target_cluster.id}: "
            f"status={resp.status_code}"
        )
        time.sleep(120)

        time_now = time.time()
        while time.time() - time_now < timeout:
            jobs_response = CapellaAPI.jobs(capella_api, self.pod, tenant, target_cluster.id)
            restore_running = False
            for item in jobs_response.get("data", []):
                if (item["data"]["jobType"] == "restore"
                        and item["data"]["completionPercentage"] < 100):
                    self.log.info("Cross-cluster restore completion: {}%".format(
                        item["data"]["completionPercentage"]))
                    restore_running = True
                    time.sleep(60)
                    break
            if not restore_running:
                break
