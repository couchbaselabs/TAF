'''
Created on Oct 13, 2023

@author: umang.agrawal
This file is temporary and will be merged with columnar.py in the same
folder.
'''
import json
import random
import string
import time
from capellaAPI.capella.columnar.CapellaAPI import CapellaAPI as ColumnarAPI
from sdk_client3 import SDKClient


class ColumnarInstance:

    def __init__(self, tenant_id, project_id, instance_name=None,
                 instance_id=None, cluster_id=None, instance_endpoint=None,
                 db_users=list()):
        self.tenant_id = tenant_id
        self.project_id = project_id

        if instance_name:
            self.name = instance_name
        else:
            self.name = "Columnar_instance{0}".format(random.randint(1, 100000))

        self.instance_id = instance_id
        self.cluster_id = cluster_id
        self.srv = instance_endpoint

        self.servers = list()
        self.master = None
        self.cbas_cc_node = self.master

        self.db_users = db_users
        self.type = "columnar"

        # SDK related objects
        self.sdk_client_pool = None
        # Note: Referenced only for sdk_client3.py SDKClient
        self.sdk_cluster_env = SDKClient.create_cluster_env()
        self.sdk_env_built = self.sdk_cluster_env.build()


class DBUser:

    def __init__(self, userId="", username="Administrator",
                 password="password"):
        self.username = username
        self.password = password
        self.roles = list()
        self.privileges = list()
        self.id = userId

class ColumnarRole:
    def __init__(self, roleId=""):
        self.id = roleId
        self.privileges = list()

class ColumnarUtils:

    def __init__(self, log):
        self.log = log

    """
    Method generates config for creating columnar instance.
    """
    def generate_instance_configuration(
            self, name=None, description=None, provider=None, region=None,
            nodes=0):
        if not name:
            name = "Columnar_{0}".format(random.randint(1, 100000))

        if not description:
            description = str(''.join(random.choice(
                string.ascii_letters + string.digits) for _ in range(
                random.randint(1, 256))))

        if not provider:
            provider = random.choice(["aws"])

        if not region:
            region = random.choice(["us-east-2"])

        if not nodes:
            nodes = random.choice([1, 2, 4, 8])

        config = {
            "name": name,
            "description": description,
            "provider": provider,
            "region": region,
            "nodes": nodes
        }
        return config

    def create_instance(self, pod, tenant, instance_config=None, timeout=7200):
        columnar_api = ColumnarAPI(
            pod.url_public, tenant.api_secret_key, tenant.api_access_key,
            tenant.user, tenant.pwd)
        if not instance_config:
            instance_config = self.generate_instance_configuration()
        resp = columnar_api.create_columnar_instance(
            tenant.id, tenant.project_id, instance_config["name"],
            instance_config["description"], instance_config["provider"],
            instance_config["region"], instance_config["nodes"]
        )
        instance_id = None
        if resp.status_code == 201:
            instance_id = json.loads(resp.content).get("id")
        elif resp.status_code == 500:
            self.log.critical(str(resp.content))
            raise Exception(str(resp.content))
        elif resp.status_code == 422:
            if resp.content.find("not allowed based on your activation status") != -1:
                self.log.critical("Tenant is not activated yet...retrying")
            else:
                self.log.critical(resp.content)
                raise Exception("Cluster deployment failed.")
        else:
            self.log.error("Unable to create goldfish cluster {0} in project "
                           "{1}".format(instance_config["name"], tenant.project_id))
            self.log.critical("Capella API returned " + str(
                resp.status_code))
            self.log.critical(resp.json()["message"])
        time.sleep(5)
        self.log.info("Cluster created with cluster ID: {}"\
                              .format(instance_id))
        
        start_time = time.time()
        while time.time() < start_time + timeout:
            resp = columnar_api.get_specific_columnar_instance(
                tenant.id, tenant.project_id, instance_id)
            if resp.status_code != 200:
                self.log.error(
                    "Unable to fetch details for goldfish cluster {0} with ID "
                    "{1}".format(instance_config["name"], instance_id))
                continue
            state = json.loads(resp.content)["data"]["state"]
            self.log.info("Cluster %s state: %s" % (instance_id, state))
            if state == "deploying":
                time.sleep(10)
            else:
                break
        if state == "healthy":
            self.log.info("Columnar instance is deployed successfully in %s s" % str(time.time() - start_time))
        else:
            self.log.error("Cluster {0} failed to deploy even after {"
                           "1} seconds. Current cluster state - {2}".format(
                               instance_config["name"], str(time.time() - start_time), state))

        return instance_id


    def delete_instance(self, pod, tenant, project_id, instance):
        columnar_api = ColumnarAPI(
            pod.url_public, tenant.api_secret_key, tenant.api_access_key,
            tenant.user, tenant.pwd)
        resp = columnar_api.delete_columnar_instance(
            tenant.id, project_id, instance.instance_id)
        if resp.status_code != 202:
            self.log.error("Unable to delete columnar instance {0}".format(
                instance.name))
            return False
        return True

    def get_instance_info(self, pod, tenant, project_id, instance_id,
                          columnar_api=None):
        if not columnar_api:
            columnar_api = ColumnarAPI(
                pod.url_public, tenant.api_secret_key, tenant.api_access_key,
                tenant.user, tenant.pwd)
        resp = columnar_api.get_specific_columnar_instance(
            tenant.id, project_id, instance_id)
        if resp.status_code != 200:
            self.log.error(
                "Unable to fetch details for Columnar instance with ID "
                "{0}".format(instance_id))
            return None
        return json.loads(resp.content)

    def scale_instance(
            self, pod, tenant, project_id, instance, nodes):
        columnar_api = ColumnarAPI(
            pod.url_public, tenant.api_secret_key, tenant.api_access_key,
            tenant.user, tenant.pwd)
        columnar_instance_info = self.get_instance_info(
            pod, tenant, project_id, instance.instance_id, columnar_api)
        resp = columnar_api.update_columnar_instance(
            tenant.id, project_id, instance.instance_id,
            columnar_instance_info["name"],
            columnar_instance_info["description"], nodes)
        self.log.info(resp)
        self.log.info(resp.status_code)
        if resp.status_code != 202:
            self.log.error("Unable to scale columnar instance {0}".format(
                instance.name))
            return False
        return resp

    def wait_for_instance_to_be_destroyed(
            self, pod, tenant, project_id, instance, timeout=3600):
        columnar_api = ColumnarAPI(
            pod.url_public, tenant.api_secret_key, tenant.api_access_key,
            tenant.user, tenant.pwd)
        end_time = time.time() + timeout
        while time.time() < end_time:
            resp = self.get_instance_info(
                pod, tenant, project_id, instance.instance_id, columnar_api)
            if not resp:
                state = None
                break
            state = resp["state"]
            if state == "destroying":
                self.log.info("instance is still deleting. Waiting for 10s.")
                time.sleep(10)
            elif state == "healthy":
                self.log.info("instance is queued for deletion. Waiting for "
                              "10s.")
                time.sleep(10)
            else:
                break
        if state == "destroying":
            self.log.error("instance {0} deletion failed even after {1} "
                           "seconds".format(instance.name, timeout))
            return False
        elif state == "healthy":
            self.log.error("instance {0} still in deletion queue even after "
                           "{1} seconds".format(instance.name, timeout))
            return False
        elif not state:
            return True

    def wait_for_instance_scaling_operation(
            self, pod, tenant, project_id, instance, timeout=3600):
        columnar_api = ColumnarAPI(
            pod.url_public, tenant.api_secret_key, tenant.api_access_key,
            tenant.user, tenant.pwd)
        end_time = time.time() + timeout
        while time.time() < end_time:
            state = self.get_instance_info(
                pod, tenant, project_id, instance.instance_id, columnar_api)["state"]
            if state == "scaling":
                self.log.info("Instance is still scaling. Waiting for 10s.")
                time.sleep(10)
            else:
                break
        if state == "healthy":
            return True
        else:
            self.log.error("Instance {0} failed to scale even after {1} "
                           "seconds. Current instance state - {2}".format(
                instance.name, timeout, state))
            return False

    def create_api_keys(self, pod, tenant, project_id, instance):
        columnar_api = ColumnarAPI(
            pod.url_public, tenant.api_secret_key, tenant.api_access_key,
            tenant.user, tenant.pwd)
        resp = columnar_api.create_api_keys(
            tenant.id, project_id, instance.instance_id)
        if resp.status_code != 201:
            self.log.error(
                "Unable to create API keys for Columnar instance {0} with ID "
                "{1}".format(instance.name, instance.instance_id))
            if resp.text:
                self.log.error("Following error recieved {}".format(resp.text))
            return None
        return json.loads(resp.content)

    def delete_api_keys(self, pod, tenant, project_id, instance, api_key):
        columnar_api = ColumnarAPI(
            pod.url_public, tenant.api_secret_key, tenant.api_access_key,
            tenant.user, tenant.pwd)
        resp = columnar_api.delete_api_keys(
            tenant.id, project_id, instance.instance_id, api_key)
        if resp.status_code != 201:
            self.log.error(
                "Unable to delete API keys for Columnar instance {0} with ID "
                "{1}".format(instance.name, instance.instance_id))
            if resp.text:
                self.log.error("Following error recieved {}".format(resp.text))
            return None
        return json.loads(resp.content)

    def allow_ip_on_instance(self, pod, tenant, project_id, instance,
                             ip="0.0.0.0/0", description=""):
        columnar_api = ColumnarAPI(
            pod.url_public, tenant.api_secret_key, tenant.api_access_key,
            tenant.user, tenant.pwd)
        resp = columnar_api.allow_ip(
            tenant.id, project_id, instance.instance_id, ip, description)
        if resp.status_code != 201:
            if (resp.status_code == 422 and resp.json()["errorType"] ==
                    "ErrAllowListsCreateDuplicateCIDR"):
                return True
            else:
                self.log.error(
                    "Unable to add IP {0} to Columnar instance {1} with ID "
                    "{2}".format(ip, instance.name, instance.instance_id))
                if resp.text:
                    self.log.error("Following error recieved {}".format(resp.text))
                return False
        return True
