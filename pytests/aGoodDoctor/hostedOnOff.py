from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI as dedicatedCapellaAPI
from capella_utils.dedicated import CapellaUtils as CapellaAPI
from global_vars import logger
from common_lib import sleep
import time

class DoctorHostedOnOff:

    def __init__(self, cluster, pod, tenant):
        self.cluster = cluster
        self.pod = pod
        self.tenant = tenant
        self.capella_api = dedicatedCapellaAPI(self.cluster.pod.url_public, self.cluster.tenant.api_secret_key,
                                               self.cluster.tenant.api_access_key, self.cluster.tenant.user,
                                               self.cluster.tenant.pwd)
        self.log = logger.get("test")
        self.sleep = sleep

    def turn_off_cluster(self, timeout=300):
        resp = self.capella_api.turn_off_cluster(self.tenant.id, self.tenant.project_id, self.cluster.id)
        if resp.status_code != 202:
            self.log.critical("Failed to turn off cluster: {}".format(resp.content))
            return False

        end_time = time.time() + timeout
        cluster_state = None
        while time.time() < end_time:
            cluster_state = CapellaAPI.get_cluster_state(self.pod, self.tenant, self.cluster.id)
            if cluster_state == "turnedOff":
                return True
            elif cluster_state == "turningOffFailed":
                self.log.critical("Failed to turn on cluster: Turning off failed")
                return False
            else:
                self.sleep(5, "Waiting for cluster to turn off")

        self.log.critical("Failed to turn off cluster: Timeout, Cluster state: {}".format(cluster_state))
        return False

    def turn_on_cluster(self, timeout=300):
        resp = self.capella_api.turn_on_cluster(self.tenant.id, self.tenant.project_id, self.cluster.id)
        if resp.status_code != 202:
            self.log.critical("Failed to turn on cluster: {}".format(resp.content))
            return False

        end_time = time.time() + timeout
        cluster_state = None
        while time.time() < end_time:
            cluster_state = CapellaAPI.get_cluster_state(self.pod, self.tenant, self.cluster.id)
            if cluster_state == "healthy":
                return True
            elif cluster_state == "turningOnFailed":
                self.log.critical("Failed to turn on cluster: Turning on failed")
                return False
            else:
                self.sleep(5, "Waiting for cluster to turn on")

        self.log.critical("Failed to turn on cluster: Timeout, Cluster state: {}".format(cluster_state))
        return False

