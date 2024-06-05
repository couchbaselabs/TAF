'''
Created on Nov 3, 2023

@author: ritesh.agarwal
'''
import json
import random
import time
from capellaAPI.capella.columnar.CapellaAPI import CapellaAPI
from capella_utils.dedicated import CapellaUtils
from global_vars import logger


class GoldfishUtils:
    log = logger.get("infra")

    def __init__(self, log):
        log = log

    """
    Method generates config for creating goldfish clusters.
    """
    @staticmethod
    def generate_cluster_configuration(
            name=None, description=None, provider=None, region=None,
            nodes=0, instance_types=None, support_package=None, availability_zone="single"):
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

        if not instance_types:
            instance_types = {
                "vcpus":"4vCPUs",
                "memory":"16GB"
            }

        if not support_package:
            support_package = {
                "key":"Developer Pro",
                "timezone":"PT"
            }

        config = {
            "name": name,
            "description": description,
            "provider": provider,
            "region": region,
            "nodes": nodes,
            "instance_types": instance_types,
            "support_package": support_package,
            "availability_zone": availability_zone
        }
        return config

    @staticmethod
    def create_cluster(pod, tenant, cluster_config=None, timeout=7200):
        gf_api = CapellaAPI(pod.url_public, tenant.api_secret_key,
                            tenant.api_access_key, tenant.user, tenant.pwd)
        if not cluster_config:
            cluster_config = GoldfishUtils.generate_goldfish_cluster_configuration()
        resp = gf_api.create_columnar_instance(
            tenant.id, tenant.project_id, cluster_config["name"],
            cluster_config["description"], cluster_config["provider"],
            cluster_config["region"], cluster_config["nodes"],
            cluster_config["instance_types"], cluster_config["support_package"],
            cluster_config["availability_zone"]
        )
        instance_id = None
        if resp.status_code == 201:
            instance_id = json.loads(resp.content).get("id")
        elif resp.status_code == 500:
            GoldfishUtils.log.critical(str(resp.content))
            raise Exception(str(resp.content))
        elif resp.status_code == 422:
            if resp.content.find("not allowed based on your activation status") != -1:
                GoldfishUtils.log.critical("Tenant is not activated yet...retrying")
            else:
                GoldfishUtils.log.critical(resp.content)
                raise Exception("Cluster deployment failed.")
        else:
            GoldfishUtils.log.error("Unable to create goldfish cluster {0} in project "
                           "{1}".format(cluster_config["name"], tenant.project_id))
            GoldfishUtils.log.critical("Capella API returned " + str(
                resp.status_code))
            GoldfishUtils.log.critical(resp.json()["message"])
        time.sleep(5)

        GoldfishUtils.log.info("Cluster created with cluster ID: {}"\
                              .format(instance_id))
        resp = GoldfishUtils.get_cluster_info(pod, tenant, instance_id)
        srv = resp["data"]["config"]["endpoint"]
        cluster_id = resp["data"]["config"]["clusterId"]

        start_time = time.time()
        while time.time() < start_time + timeout:
            resp = gf_api.get_specific_columnar_instance(
                tenant.id, tenant.project_id, instance_id)
            if resp.status_code != 200:
                GoldfishUtils.log.error(
                    "Unable to fetch details for goldfish cluster {0} with ID "
                    "{1}".format(cluster_config["name"], instance_id))
                continue
            state = json.loads(resp.content)["data"]["state"]
            GoldfishUtils.log.info("Cluster %s state: %s" % (instance_id, state))
            if state == "deploying":
                time.sleep(10)
            else:
                break
        if state == "healthy":
            GoldfishUtils.log.info("Columnar instance is deployed successfully in %s s" % str(time.time() - start_time))
        else:
            GoldfishUtils.log.error("Cluster {0} failed to deploy even after {"
                           "1} seconds. Current cluster state - {2}".format(
                               cluster_config["name"], str(time.time() - start_time), state))

        # CapellaUtils.allow_my_ip(pod, tenant, cluster_id)
        servers = CapellaUtils.get_nodes(pod, tenant, cluster_id)
        return instance_id, cluster_id, srv, servers

    @staticmethod
    def delete_cluster(pod, tenant, cluster):
        gf_api = CapellaAPI(pod.url_public, tenant.api_secret_key,
                            tenant.api_access_key, tenant.user, tenant.pwd)
        resp = gf_api.delete_columnar_instance(
            tenant.id, tenant.project_id, cluster.instance_id,)
        if resp.status_code != 202:
            GoldfishUtils.log.error("Unable to delete goldfish cluster {0}: {1}".format(
                cluster.instance_id, resp.content))
            return False
        return True

    @staticmethod
    def scale_cluster(pod, tenant, cluster, nodes):
        gf_api = CapellaAPI(pod.url_public, tenant.api_secret_key,
                            tenant.api_access_key, tenant.user, tenant.pwd)
        gf_instance_info = GoldfishUtils.get_cluster_info(pod, tenant, cluster.instance_id)
        resp = gf_api.update_columnar_instance(
            tenant.id, tenant.project_id, cluster.instance_id,
            gf_instance_info["data"]["name"], gf_instance_info["data"]["description"],
            nodes)
        GoldfishUtils.log.info(resp)
        GoldfishUtils.log.info(resp.status_code)
        if resp.status_code != 202:
            GoldfishUtils.log.error("Unable to scale goldfish cluster {0}".format(
                cluster.name))
            return None
        return resp

    @staticmethod
    def get_cluster_info(pod, tenant, cluster_id):
        gf_api = CapellaAPI(pod.url_public, tenant.api_secret_key,
                            tenant.api_access_key, tenant.user, tenant.pwd)
        resp = gf_api.get_specific_columnar_instance(
            tenant.id, tenant.project_id, cluster_id)
        if resp.status_code != 200:
            GoldfishUtils.log.error(
                "Unable to fetch details for goldfish cluster with ID "
                "{0}".format(cluster_id))
            return None
        return json.loads(resp.content)

    @staticmethod
    def wait_for_cluster_deploy(pod, tenant, cluster, timeout=3600):
        gf_api = CapellaAPI(pod.url_public, tenant.api_secret_key,
                            tenant.api_access_key, tenant.user, tenant.pwd)
        start_time = time.time()
        while time.time() < start_time + timeout:
            resp = gf_api.get_specific_columnar_instance(
                tenant.id, tenant.project_id, cluster.instance_id)
            if resp.status_code != 200:
                GoldfishUtils.log.error(
                    "Unable to fetch details for goldfish cluster {0} with ID "
                    "{1}".format(cluster.name, cluster.instance_id))
                continue
            state = json.loads(resp.content)["data"]["state"]
            GoldfishUtils.log.info("Cluster %s state: %s" % (cluster.instance_id, state))
            if state == "deploying":
                time.sleep(10)
            else:
                break
        if state == "healthy":
            GoldfishUtils.log.info("Columnar instance is deployed successfully in %s s" % str(time.time() - start_time))
            return True
        else:
            GoldfishUtils.log.error("Cluster {0} failed to deploy even after {"
                           "1} seconds. Current cluster state - {2}".format(
                               cluster.name, str(time.time() - start_time), state))
            return False

    @staticmethod
    def wait_for_cluster_destroy(pod, tenant, cluster, timeout=3600):
        gf_api = CapellaAPI(pod.url_public, tenant.api_secret_key,
                            tenant.api_access_key, tenant.user, tenant.pwd)
        start_time = time.time()
        while time.time() < start_time + timeout:
            resp = gf_api.get_specific_columnar_instance(
                tenant.id, tenant.project_id, cluster.instance_id)
            if resp.status_code != 200:
                GoldfishUtils.log.info("Columnar instance is deleted successfully in %s s" % str((time.time() - start_time)))
                return True
            content = json.loads(resp.content)
            state = content["state"]
            GoldfishUtils.log.info("Cluster %s current state: %s" % (cluster.instance_id, state))
            if state == "destroying" or state == "healthy":
                time.sleep(10)
            else:
                raise Exception("Incorrect cluster state found during cluster deletion: {}".format(
                    state))

        GoldfishUtils.log.error("Cluster {0} deletion failed even after {1} "
                       "seconds".format(cluster.name, timeout))
        return False

    @staticmethod
    def wait_for_cluster_scaling(pod, tenant, cluster, timeout=3600):
        gf_api = CapellaAPI(pod.url_public, tenant.api_secret_key,
                            tenant.api_access_key, tenant.user, tenant.pwd)
        start_time = time.time()
        while time.time() < start_time + timeout:
            resp = gf_api.get_specific_columnar_instance(
                tenant.id, tenant.project_id, cluster.instance_id)
            if resp.status_code != 200:
                GoldfishUtils.log.error(
                    "Unable to fetch details for goldfish cluster {0} with ID "
                    "{1}".format(cluster.name, cluster.instance_id))
                continue
            state = json.loads(resp.content)["data"]["state"]
            GoldfishUtils.log.info("Cluster %s state: %s" % (cluster.instance_id, state))
            if state == "scaling":
                GoldfishUtils.log.info("Cluster is still scaling. Waiting for 10s.")
                time.sleep(10)
            else:
                break
        if state == "healthy":
            GoldfishUtils.log.info("Columnar instance is scaled successfully in %s s" % str((time.time() - start_time)))
            return True
        else:
            GoldfishUtils.log.error("Cluster {0} failed to scale even after {1} seconds.\
                            Current cluster state - {2}".format(
                                cluster.name, timeout, state))
            return False

    @staticmethod
    def create_db_user_api_keys(pod, tenant, instance_id):
        gf_api = CapellaAPI(pod.url_public, tenant.api_secret_key,
                            tenant.api_access_key, tenant.user, tenant.pwd)
        resp = gf_api.create_api_keys(
            tenant.id, tenant.project_id, instance_id)
        if resp.status_code != 201:
            GoldfishUtils.log.error(
                "Unable to create API keys for goldfish cluster {0}: "
                "{1}".format(instance_id, resp.content))
            return None
        return json.loads(resp.content)
