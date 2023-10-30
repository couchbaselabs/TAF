'''
Created on Oct 13, 2023

@author: umang.agrawal
'''
import json
import random
import string
import time
from global_vars import logger
from goldfishAPI.GoldfishAPIs.ServerlessAnalytics.ServerlessAnalytics import GoldfishAPI
from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI
from TestInput import TestInputServer


class Users:

    def __init__(self, org_id, name=None, email=None, password=None,
                 roles=[], access=None, secret=None, user_id=None):
        self.user_id = user_id
        self.org_id = org_id
        self.api_secret_key = secret
        self.api_access_key = access

        if name:
            self.name = name
        else:
            self.name = "test.user{0}".format(random.randint(1, 100000))

        if email:
            self.email = email
        else:
            self.email = "{0}@couchbase.com".format(self.name)

        if password:
            self.password = password
        else:
            self.password = ""
            self.password += "".join(random.choice(
                string.ascii_lowercase) for _ in range(3))
            self.password += "".join(random.choice(
                string.ascii_uppercase) for _ in range(3))
            self.password += "".join(random.choice(
                string.digits) for _ in range(3))
            self.password += "@"

        if roles:
            self.roles = roles
        else:
            self.roles = ["organizationOwner"]

        self.projects = list()


class Project:

    def __init__(self, org_id, name=None, project_id=None):
        self.org_id = org_id
        self.project_id = project_id
        if name:
            self.name = name
        else:
            self.name = "project{0}".format(random.randint(1, 100000))

        # List of goldfish cluster objects
        self.clusters = list()


class GoldfishCluster:

    def __init__(self, org_id, project_id, cluster_name=None, cluster_id=None,
                 cluster_endpoint=None, nebula_sdk_port=16001,
                 nebula_rest_port=18001, db_users=list(), type="goldfish"):
        self.org_id = org_id
        self.project_id = project_id

        if cluster_name:
            self.name = cluster_name
        else:
            self.name = "GFcluster{0}".format(random.randint(1, 100000))

        self.cluster_id = cluster_id
        self.endpoint = cluster_endpoint

        self.master = TestInputServer()
        self.master.ip = self.endpoint
        self.master.port = nebula_rest_port
        self.master.nebula_sdk_port = nebula_sdk_port
        self.master.nebula_rest_port = nebula_rest_port
        self.master.type = type

        self.cbas_cc_node = self.master

        self.db_users = db_users
        self.type = type


class DBUser:

    def __init__(self, username="Administrator", password="password"):
        self.username = username
        self.password = password
        self.permission = list()


class GoldfishUtils:

    def __init__(self, log):
        self.log = log

    def create_org_user_without_email_verification(self, pod, user, new_user):
        capella_api = CapellaAPI(
            pod.url_public, user.api_secret_key, user.api_access_key,
            user.email, user.password)
        resp = capella_api.create_user(
            new_user.org_id, new_user.name, new_user.email,
            new_user.password, new_user.roles)
        if resp.status_code != 200:
            self.log.error("Creating capella User {0} failed: {1}".format(
                new_user.name, resp.content))
            return None
        result = json.loads(resp.content).get("data")
        # Verify whether the user is marked as verified.
        if not result.get("addresses")[0].get("status") == "verified":
            self.log.error(
                "User {0} was not verified. Verification bypass flag "
                "might not be active.".format(new_user.user))
            return None
        return result["id"]

    def delete_org_user(self, pod, user):
        capella_api = CapellaAPI(
            pod.url_public, user.api_secret_key, user.api_access_key,
            user.email, user.password)
        resp = capella_api.remove_user(user.org_id, user.user_id)
        if resp.status_code != 204:
            self.log.error("Deleting capella User {0} failed: {1}".format(
                user.name, resp.content))
            return False
        return True

    def create_project(self, pod, user, project_name):
        capella_api = CapellaAPI(
            pod.url_public, user.api_secret_key, user.api_access_key,
            user.email, user.password)
        resp = capella_api.create_project(user.org_id, project_name)
        if resp.status_code != 201:
            self.log.error("Creating project {0} failed: {1}".
                            format(project_name, resp.content))
            return None
        return json.loads(resp.content).get("id")

    def delete_project(self, pod, user, project):
        capella_api = CapellaAPI(
            pod.url_public, user.api_secret_key, user.api_access_key,
            user.email, user.password)
        resp = capella_api.delete_project(user.org_id, project.project_id)
        if resp.status_code != 204:
            self.log.error("Deleting project {0} failed: {1}".
                            format(project.name, resp.content))
            return False
        return True

    """
    Method generates config for creating goldfish clusters.
    """
    def generate_goldfish_cluster_configuration(
            self, name=None, description=None, provider=None, region=None,
            nodes=0):
        if not name:
            name = "GFcluster{0}".format(random.randint(1, 100000))

        if not description:
            description = str(''.join(random.choice(
                string.ascii_letters + string.digits) for _ in range(
                random.randint(1, 256))))

        if not provider:
            provider = random.choice(["aws"])

        if not region:
            region = random.choice(["us-east-2"])

        if not nodes:
            nodes = 2
            #nodes = random.randint(2, 8)

        config = {
            "name": name,
            "description": description,
            "provider": provider,
            "region": region,
            "nodes": nodes
        }
        return config

    def create_goldfish_cluster(self, pod, user, project, cluster_config=None):
        gf_api = GoldfishAPI(pod.url_public, user.api_secret_key,
                             user.api_access_key, user.email, user.password)
        if not cluster_config:
            cluster_config = self.generate_goldfish_cluster_configuration()
        resp = gf_api.create_goldfish_instance(
            user.org_id, project.project_id, cluster_config["name"],
            cluster_config["description"], cluster_config["provider"],
            cluster_config["region"], cluster_config["nodes"]
        )
        if resp.status_code != 201:
            self.log.error("Unable to create goldfish cluster {0} in project "
                           "{1}".format(cluster_config["name"], project.name))
            return None
        resp = json.loads(resp.content)
        return resp["id"]

    def delete_goldfish_cluster(self, pod, user, cluster):
        gf_api = GoldfishAPI(pod.url_public, user.api_secret_key,
                             user.api_access_key, user.email, user.password)
        resp = gf_api.delete_goldfish_instance(
            cluster.org_id, cluster.project_id, cluster.cluster_id)
        if resp.status_code != 204:
            self.log.error("Unable to delete goldfish cluster {0}".format(
                cluster.name))
            return None
        resp = json.loads(resp.content)
        return resp["id"]

    def scale_goldfish_cluster(self, pod, user, cluster, nodes):
        gf_api = GoldfishAPI(pod.url_public, user.api_secret_key,
                             user.api_access_key, user.email, user.password)
        gf_instance_info = self.get_cluster_info(pod, user, cluster)
        resp = gf_api.update_goldfish_instance(
            cluster.org_id, cluster.project_id, cluster.cluster_id,
            gf_instance_info["name"], gf_instance_info["description"],
            nodes)
        self.log.info(resp)
        self.log.info(resp.status_code)
        if resp.status_code != 202:
            self.log.error("Unable to scale goldfish cluster {0}".format(
                cluster.name))
            return None
        return resp

    def get_cluster_info(self, pod, user, cluster):
        gf_api = GoldfishAPI(pod.url_public, user.api_secret_key,
            user.api_access_key, user.email, user.password)
        resp = gf_api.get_specific_goldfish_instance(
            cluster.org_id, cluster.project_id, cluster.cluster_id)
        if resp.status_code != 200:
            self.log.error(
                "Unable to fetch details for goldfish cluster {0} with ID "
                "{1}".format(cluster.name, cluster.cluster_id))
            return None
        return json.loads(resp.content)

    def wait_for_cluster_to_be_deployed(
            self, pod, user, cluster, timeout=3600):
        end_time = time.time() + timeout
        while time.time() < end_time:
            state = self.get_cluster_info(pod, user, cluster)["state"]
            if state == "deploying":
                self.log.info("Cluster is still deploying. Waiting for 10s.")
                time.sleep(10)
            else:
                break
        if state == "healthy":
            return True
        else:
            self.log.error("Cluster {0} failed to deploy even after {"
                           "1} seconds. Current cluster state - {2}".format(
                cluster.name, timeout, state))
            return False

    def wait_for_cluster_to_be_destroyed(
            self, pod, user, cluster, timeout=3600):
        end_time = time.time() + timeout
        while time.time() < end_time:
            resp = self.get_cluster_info(pod, user, cluster)
            if not resp:
                state = None
                break
            state = resp["state"]
            if state == "destroying":
                self.log.info("Cluster is still deleting. Waiting for 10s.")
                time.sleep(10)
            else:
                break
        if state == "destroying":
            self.log.error("Cluster {0} deletion failed even after {1} "
                           "seconds".format(cluster.name, timeout))
            return False
        elif not state:
            return True

    def wait_for_cluster_scaling_operation_to_complete(
            self, pod, user, cluster, timeout=3600):
        end_time = time.time() + timeout
        while time.time() < end_time:
            state = self.get_cluster_info(pod, user, cluster)["state"]
            if state == "scaling":
                self.log.info("Cluster is still scaling. Waiting for 10s.")
                time.sleep(10)
            else:
                break
        if state == "healthy":
            return True
        else:
            self.log.error("Cluster {0} failed to scale even after {"
                            "1} seconds. Current cluster state - {2}".format(
                cluster.name, timeout, state))
            return False
