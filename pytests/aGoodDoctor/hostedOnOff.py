from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI as dedicatedCapellaAPI
from capella_utils.dedicated import CapellaUtils as CapellaAPI
from global_vars import logger
from common_lib import sleep
import time

class DoctorHostedOnOff:

    def __init__(self, pod, tenant, cluster):
        self.pod = pod
        self.tenant = tenant
        self.cluster = cluster
        self.capella_api = dedicatedCapellaAPI(self.pod.url_public, self.tenant.api_secret_key,
                                               self.tenant.api_access_key, self.tenant.user,
                                               self.tenant.pwd)
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
            if cluster_state == "turned_off":
                return True
            elif cluster_state == "turning_off_failed":
                self.log.critical("Failed to turn off cluster: Turning off failed")
                return False
            else:
                self.sleep(5, "Waiting for cluster to turn off, current state: {}".format(cluster_state))

        self.log.critical("Failed to turn off cluster: Timeout, Cluster state: {}".format(cluster_state))
        return False

    def turn_on_cluster(self, timeout=300, poll_callback=None,
                        callback_delay=60, callback_interval=30):
        """Turn on the cluster and wait for it to reach 'healthy' state.

        poll_callback: optional callable invoked periodically during the wait
            loop — first call after callback_delay seconds, then every
            callback_interval seconds. Useful for in-loop health checks
            (e.g. dp-agent crash detection) without a separate thread.
        """
        resp = self.capella_api.turn_on_cluster(self.tenant.id, self.tenant.project_id, self.cluster.id)
        if resp.status_code != 202:
            self.log.critical("Failed to turn on cluster: {}".format(resp.content))
            return False

        end_time = time.time() + timeout
        start_time = time.time()
        last_callback_time = 0.0
        cluster_state = None
        while time.time() < end_time:
            cluster_state = CapellaAPI.get_cluster_state(self.pod, self.tenant, self.cluster.id)
            if cluster_state == "healthy":
                return True
            elif cluster_state == "turning_on_failed":
                self.log.critical("Failed to turn on cluster: Turning on failed")
                return False

            if poll_callback is not None:
                now = time.time()
                elapsed = now - start_time
                if elapsed >= callback_delay and (now - last_callback_time) >= callback_interval:
                    poll_callback()
                    last_callback_time = time.time()

            self.sleep(5, "Waiting for cluster to turn on, current state: {}".format(cluster_state))

        self.log.critical("Failed to turn on cluster: Timeout, Cluster state: {}".format(cluster_state))
        return False

