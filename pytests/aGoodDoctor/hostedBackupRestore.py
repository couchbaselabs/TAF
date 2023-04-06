import time
import json

from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI as dedicatedCapellaAPI
from capella_utils.dedicated import CapellaUtils as CapellaAPI
from global_vars import logger


class DoctorHostedBackupRestore:

    def __init__(self, cluster, bucket_name, pod, tenant):
        self.cluster = cluster
        self.bucket_name = bucket_name
        self.pod = pod
        self.tenant = tenant
        self.capella_api = dedicatedCapellaAPI(self.cluster.pod.url_public, self.cluster.tenant.api_secret_key,
                                               self.cluster.tenant.api_access_key, self.cluster.tenant.user,
                                               self.cluster.tenant.pwd)
        self.log = logger.get("test")

    def backup_now(self, timeout=30, wait_for_backup=True):
        CapellaAPI.backup_now(pod=self.pod, tenant=self.tenant, cluster_id=self.cluster.id,
                              bucket_name=self.bucket_name)
        time.sleep(180)
        time_now = time.time()
        while time.time() - time_now < timeout * 60:
            jobs_response = CapellaAPI.jobs(self.capella_api, self.pod, self.tenant, self.cluster.id)
            if not jobs_response['data']:
                break
            else:
                for item in jobs_response['data']:
                    if item['data']['jobType'] == 'backup' and item['data']['completionPercentage'] < 100:
                        self.log.info("Backup is still running. Backup completion percentage:{}".format(
                            item['data']['completionPercentage']))
                        time.sleep(60)
                        if not wait_for_backup:
                            return

    def restore_from_backup(self, timeout=30):
        CapellaAPI.restore_from_backup(pod=self.pod, tenant=self.tenant, cluster_id=self.cluster.id,
                                       bucket_name=self.bucket_name)
        time.sleep(180)
        time_now = time.time()
        while time.time() - time_now < timeout * 60:
            jobs_response = CapellaAPI.jobs(self.capella_api, self.pod, self.tenant, self.cluster.id)
            if not jobs_response['data']:
                break
            else:
                for item in jobs_response['data']:
                    if item['data']['jobType'] == 'restore' and item['data']['completionPercentage'] < 100:
                        self.log.info("Restore is still running. Restore completion percentage:{}".format(
                            item['data']['completionPercentage']))
                        time.sleep(60)

    def list_all_backups(self):
        resp = CapellaAPI.list_all_backups(pod=self.pod, tenant=self.tenant, cluster=self.cluster,
                                           bucket_name=self.bucket_name)
        self.log.info("List all backups response is {}".format(resp))
        return resp
