'''
Created on Oct 13, 2023

@author: umang.agrawal
'''
import json
import random
import string
import time
from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI
from capellaAPI.capella.columnar.CapellaAPI import CapellaAPI as ColumnarAPI
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


class Project:

    def __init__(self, org_id, name=None, project_id=None):
        self.org_id = org_id
        self.project_id = project_id
        if name:
            self.name = name
        else:
            self.name = "columnar_project{0}".format(random.randint(1, 100000))

        # List of Columnar instance objects
        self.instances = list()


class ColumnarInstance:

    def __init__(self, org_id, project_id, instance_name=None, instance_id=None,
                 instance_endpoint=None, nebula_sdk_port=16001,
                 nebula_rest_port=18001, api_access_key=None,
                 api_secret_key=None, db_users=list(),
                 instance_type="columnar"):
        self.org_id = org_id
        self.project_id = project_id

        if instance_name:
            self.name = instance_name
        else:
            self.name = "Columnar_instance{0}".format(random.randint(1, 100000))

        self.instance_id = instance_id
        self.endpoint = instance_endpoint

        self.master = TestInputServer()
        self.master.ip = self.endpoint
        self.master.port = nebula_rest_port
        self.master.nebula_sdk_port = nebula_sdk_port
        self.master.nebula_rest_port = nebula_rest_port
        self.master.type = instance_type

        self.cbas_cc_node = self.master

        self.api_access_key = api_access_key
        self.api_secret_key = api_secret_key

        self.db_users = db_users
        self.type = instance_type


class DBUser:

    def __init__(self, username="Administrator", password="password"):
        self.username = username
        self.password = password
        self.permission = list()


class ColumnarUtils:

    def __init__(self, log, pod=None, user=None):
        self.log = log
        if pod and user:
            self.capella_api = CapellaAPI(
                pod.url_public, user.api_secret_key, user.api_access_key,
                user.email, user.password)
            self.columnar_api = ColumnarAPI(
                pod.url_public, user.api_secret_key,
                user.api_access_key, user.email, user.password)

    def create_org_user_without_email_verification(
            self, new_user, pod=None, user=None):
        if pod and user:
            capella_api = CapellaAPI(
                pod.url_public, user.api_secret_key, user.api_access_key,
                user.email, user.password)
        else:
            capella_api = self.capella_api
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

    def delete_org_user(self, user_to_deleted, pod=None, user=None):
        if pod and user:
            capella_api = CapellaAPI(
                pod.url_public, user.api_secret_key, user.api_access_key,
                user.email, user.password)
        else:
            capella_api = self.capella_api
        resp = capella_api.remove_user(
            user_to_deleted.org_id, user_to_deleted.user_id)
        if resp.status_code != 204:
            self.log.error("Deleting capella User {0} failed: {1}".format(
                user_to_deleted.name, resp.content))
            return False
        return True

    def create_project(self, project, pod=None, user=None):
        if pod and user:
            capella_api = CapellaAPI(
                pod.url_public, user.api_secret_key, user.api_access_key,
                user.email, user.password)
        else:
            capella_api = self.capella_api
        resp = capella_api.create_project(project.org_id, project.name)
        if resp.status_code != 201:
            self.log.error("Creating project {0} failed: {1}".
                            format(project.name, resp.content))
            return None
        return json.loads(resp.content).get("id")

    def delete_project(self, project, pod=None, user=None):
        if pod and user:
            capella_api = CapellaAPI(
                pod.url_public, user.api_secret_key, user.api_access_key,
                user.email, user.password)
        else:
            capella_api = self.capella_api
        resp = capella_api.delete_project(project.org_id, project.project_id)
        if resp.status_code != 202:
            self.log.error("Deleting project {0} failed: {1}".
                           format(project.name, resp.content))
            return False
        return True

    """
    Method generates config for creating columnar instance.
    """
    def generate_cloumnar_instance_configuration(
            self, name=None, description=None, provider=None, region=None,
            nodes=0):
        if not name:
            name = "Columnar_instance{0}".format(random.randint(1, 100000))

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

    def create_columnar_instance(
            self, project, instance_config=None, pod=None, user=None):
        if pod and user:
            columnar_api = ColumnarAPI(
                pod.url_public, user.api_secret_key, user.api_access_key,
                user.email, user.password)
        else:
            columnar_api = self.columnar_api
        if not instance_config:
            instance_config = self.generate_cloumnar_instance_configuration()
        resp = columnar_api.create_columnar_instance(
            user.org_id, project.project_id, instance_config["name"],
            instance_config["description"], instance_config["provider"],
            instance_config["region"], instance_config["nodes"]
        )
        if resp.status_code != 201:
            self.log.error("Unable to create columnar instance {0} in project "
                           "{1}".format(instance_config["name"], project.name))
            return None
        resp = json.loads(resp.content)
        return resp["id"]

    def delete_columnar_instance(self, instance, pod=None, user=None):
        if pod and user:
            columnar_api = ColumnarAPI(
                pod.url_public, user.api_secret_key, user.api_access_key,
                user.email, user.password)
        else:
            columnar_api = self.columnar_api
        resp = columnar_api.delete_columnar_instance(
            instance.org_id, instance.project_id, instance.instance_id)
        if resp.status_code != 202:
            self.log.error("Unable to delete columnar instance {0}".format(
                instance.name))
            return None
        resp = json.loads(resp.content)
        return resp["id"]

    def scale_columnar_instance(self, instance, nodes, pod=None, user=None):
        if pod and user:
            columnar_api = ColumnarAPI(
                pod.url_public, user.api_secret_key, user.api_access_key,
                user.email, user.password)
        else:
            columnar_api = self.columnar_api
        columnar_instance_info = self.get_instance_info(pod, user, instance)
        resp = columnar_api.update_columnar_instance(
            instance.org_id, instance.project_id, instance.instance_id,
            columnar_instance_info["name"],
            columnar_instance_info["description"],nodes)
        self.log.info(resp)
        self.log.info(resp.status_code)
        if resp.status_code != 202:
            self.log.error("Unable to scale columnar instance {0}".format(
                instance.name))
            return False
        return resp

    def get_instance_info(self, instance, pod=None, user=None):
        if pod and user:
            columnar_api = ColumnarAPI(
                pod.url_public, user.api_secret_key, user.api_access_key,
                user.email, user.password)
        else:
            columnar_api = self.columnar_api
        resp = columnar_api.get_specific_columnar_instance(
            instance.org_id, instance.project_id, instance.instance_id)
        if resp.status_code != 200:
            self.log.error(
                "Unable to fetch details for Columnar instance {0} with ID "
                "{1}".format(instance.name, instance.instance_id))
            return None
        return json.loads(resp.content)

    def wait_for_instance_to_be_deployed(
            self, instance, timeout=3600, pod=None, user=None):
        end_time = time.time() + timeout
        while time.time() < end_time:
            resp = self.get_instance_info(instance, pod, user)
            if not resp:
                state = None
                continue
            state = resp["state"]
            if state == "deploying":
                self.log.info("instance is still deploying. Waiting for 10s.")
                time.sleep(10)
            else:
                break
        if state == "healthy":
            return True
        else:
            self.log.error("instance {0} failed to deploy even after {"
                           "1} seconds. Current instance state - {2}".format(
                instance.name, timeout, state))
            return False

    def wait_for_instance_to_be_destroyed(
            self, instance, timeout=3600, pod=None, user=None):
        end_time = time.time() + timeout
        while time.time() < end_time:
            resp = self.get_instance_info(instance, pod, user)
            if not resp:
                state = None
                break
            state = resp["state"]
            if state == "destroying":
                self.log.info("instance is still deleting. Waiting for 10s.")
                time.sleep(10)
            else:
                break
        if state == "destroying":
            self.log.error("instance {0} deletion failed even after {1} "
                           "seconds".format(instance.name, timeout))
            return False
        elif not state:
            return True

    def wait_for_instance_scaling_operation_to_complete(
            self, instance, timeout=3600, pod=None, user=None):
        end_time = time.time() + timeout
        while time.time() < end_time:
            state = self.get_instance_info(instance, pod, user)["state"]
            if state == "scaling":
                self.log.info("instance is still scaling. Waiting for 10s.")
                time.sleep(10)
            else:
                break
        if state == "healthy":
            return True
        else:
            self.log.error("instance {0} failed to scale even after {"
                            "1} seconds. Current instance state - {2}".format(
                instance.name, timeout, state))
            return False

    def create_columnar_instance_api_keys(self, instance, pod=None, user=None):
        if pod and user:
            columnar_api = ColumnarAPI(
                pod.url_public, user.api_secret_key, user.api_access_key,
                user.email, user.password)
        else:
            columnar_api = self.columnar_api
        resp = columnar_api.create_api_keys(
            instance.org_id, instance.project_id, instance.instance_id)
        if resp.status_code != 201:
            self.log.error(
                "Unable to create API keys for Columnar instance {0} with ID "
                "{1}".format(instance.name, instance.instance_id))
            if resp.text:
                self.log.error("Following error recieved {}".format(resp.text))
            return None
        return json.loads(resp.content)
    
    def delete_columnar_instance_api_keys(
            self, instance, api_key, pod=None, user=None):
        if pod and user:
            columnar_api = ColumnarAPI(
                pod.url_public, user.api_secret_key, user.api_access_key,
                user.email, user.password)
        else:
            columnar_api = self.columnar_api
        resp = columnar_api.delete_api_keys(
            instance.org_id, instance.project_id, instance.instance_id, api_key)
        if resp.status_code != 201:
            self.log.error(
                "Unable to dekete API keys for Columnar instance {0} with ID "
                "{1}".format(instance.name, instance.instance_id))
            if resp.text:
                self.log.error("Following error recieved {}".format(resp.text))
            return None
        return json.loads(resp.content)
