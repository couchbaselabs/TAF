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
