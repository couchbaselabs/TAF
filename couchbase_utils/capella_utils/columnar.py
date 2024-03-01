'''
Created on Nov 3, 2023

@author: ritesh.agarwal
'''
import json
import random
import time
from capellaAPI.capella.columnar.CapellaAPI import CapellaAPI


class GoldfishUtils:

    def __init__(self, log):
        self.log = log

    """
    Method generates config for creating goldfish clusters.
    """
    def generate_cluster_configuration(
            self, name=None, description=None, provider=None, region=None,
            nodes=0):
        if not name:
            name = "Goldfish_{0}".format(random.randint(1, 100000))

        if not description:
            description = name

        if not provider:
            provider = random.choice(["aws"])

        if not region:
            region = random.choice(["us-east-2"])

        if not nodes:
            nodes = 2

        config = {
            "name": name,
            "description": description,
            "provider": provider,
            "region": region,
            "nodes": nodes
        }
        return config

    def create_cluster(self, pod, tenant, cluster_config=None):
        gf_api = CapellaAPI(pod.url_public, tenant.api_secret_key,
                            tenant.api_access_key, tenant.user, tenant.pwd)
        if not cluster_config:
            cluster_config = self.generate_goldfish_cluster_configuration()
        resp = gf_api.create_columnar_instance(
            tenant.id, tenant.project_id, cluster_config["name"],
            cluster_config["description"], cluster_config["provider"],
            cluster_config["region"], cluster_config["nodes"]
        )
        if resp.status_code != 201:
            self.log.error("Unable to create goldfish cluster {0} in project "
                           "{1}".format(cluster_config["name"], tenant.project_id))
            return None
        resp = json.loads(resp.content)
        return resp["id"]

    def delete_cluster(self, pod, tenant, cluster):
        gf_api = CapellaAPI(pod.url_public, tenant.api_secret_key,
                            tenant.api_access_key, tenant.user, tenant.pwd)
        resp = gf_api.delete_columnar_instance(
            tenant.id, tenant.project_id, cluster.id,)
        if resp.status_code != 202:
            self.log.error("Unable to delete goldfish cluster {0}: {1}".format(
                cluster.id, resp.content))
            return False
        return True

    def scale_cluster(self, pod, tenant, cluster, nodes):
        gf_api = CapellaAPI(pod.url_public, tenant.api_secret_key,
                            tenant.api_access_key, tenant.user, tenant.pwd)
        gf_instance_info = self.get_cluster_info(pod, tenant, cluster)
        resp = gf_api.update_columnar_instance(
            tenant.id, tenant.project_id, cluster.id,
            gf_instance_info["name"], gf_instance_info["description"],
            nodes)
        self.log.info(resp)
        self.log.info(resp.status_code)
        if resp.status_code != 202:
            self.log.error("Unable to scale goldfish cluster {0}".format(
                cluster.name))
            return None
        return resp

    def get_cluster_info(self, pod, tenant, cluster):
        gf_api = CapellaAPI(pod.url_public, tenant.api_secret_key,
                            tenant.api_access_key, tenant.user, tenant.pwd)
        resp = gf_api.get_specific_columnar_instance(
            tenant.id, tenant.project_id, cluster.id)
        if resp.status_code != 200:
            self.log.error(
                "Unable to fetch details for goldfish cluster {0} with ID "
                "{1}".format(cluster.name, cluster.id))
            return None
        return json.loads(resp.content)

    def wait_for_cluster_deploy(
            self, pod, tenant, cluster, timeout=3600):
        gf_api = CapellaAPI(pod.url_public, tenant.api_secret_key,
                            tenant.api_access_key, tenant.user, tenant.pwd)
        start_time = time.time()
        while time.time() < start_time + timeout:
            resp = gf_api.get_specific_columnar_instance(
                tenant.id, tenant.project_id, cluster.id)
            if resp.status_code != 200:
                self.log.error(
                    "Unable to fetch details for goldfish cluster {0} with ID "
                    "{1}".format(cluster.name, cluster.id))
                continue
            state = json.loads(resp.content)["state"]
            self.log.info("Cluster %s state: %s" % (cluster.id, state))
            if state == "deploying":
                time.sleep(10)
            else:
                break
        if state == "healthy":
            self.log.info("Columnar instance is deployed successfully in %s s" % str(time.time() - start_time))
            return True
        else:
            self.log.error("Cluster {0} failed to deploy even after {"
                           "1} seconds. Current cluster state - {2}".format(
                               cluster.name, str(time.time() - start_time), state))
            return False

    def wait_for_cluster_destroy(
            self, pod, tenant, cluster, timeout=3600):
        gf_api = CapellaAPI(pod.url_public, tenant.api_secret_key,
                            tenant.api_access_key, tenant.user, tenant.pwd)
        start_time = time.time()
        while time.time() < start_time + timeout:
            resp = gf_api.get_specific_columnar_instance(
                tenant.id, tenant.project_id, cluster.id)
            if resp.status_code != 200:
                self.log.info("Columnar instance is deleted successfully in %s s" % str((time.time() - start_time)))
                return True
            content = json.loads(resp.content)
            state = content["state"]
            self.log.info("Cluster %s current state: %s" % (cluster.id, state))
            if state == "destroying" or state == "healthy":
                time.sleep(10)
            else:
                raise Exception("Incorrect cluster state found during cluster deletion: {}".format(
                    state))

        self.log.error("Cluster {0} deletion failed even after {1} "
                       "seconds".format(cluster.name, timeout))
        return False

    def wait_for_cluster_scaling(
            self, pod, tenant, cluster, timeout=3600):
        gf_api = CapellaAPI(pod.url_public, tenant.api_secret_key,
                            tenant.api_access_key, tenant.user, tenant.pwd)
        start_time = time.time()
        while time.time() < start_time + timeout:
            resp = gf_api.get_specific_columnar_instance(
                tenant.id, tenant.project_id, cluster.id)
            if resp.status_code != 200:
                self.log.error(
                    "Unable to fetch details for goldfish cluster {0} with ID "
                    "{1}".format(cluster.name, cluster.id))
                continue
            state = json.loads(resp.content)["state"]
            self.log.info("Cluster %s state: %s" % (cluster.id, state))
            if state == "scaling":
                self.log.info("Cluster is still scaling. Waiting for 10s.")
                time.sleep(10)
            else:
                break
        if state == "healthy":
            self.log.info("Columnar instance is scaled successfully in %s s" % str((time.time() - start_time)))
            return True
        else:
            self.log.error("Cluster {0} failed to scale even after {1} seconds.\
                            Current cluster state - {2}".format(
                                cluster.name, timeout, state))
            return False

    def create_db_user_api_keys(self, pod, tenant, cluster):
        gf_api = CapellaAPI(pod.url_public, tenant.api_secret_key,
                            tenant.api_access_key, tenant.user, tenant.pwd)
        resp = gf_api.create_api_keys(
            tenant.id, tenant.project_id, cluster.id)
        if resp.status_code != 201:
            self.log.error(
                "Unable to create API keys for goldfish cluster {0} with ID "
                "{1}".format(cluster.name, cluster.id))
            return None
        return json.loads(resp.content)
