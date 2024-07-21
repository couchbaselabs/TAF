"""
Created on Oct 13, 2023

@author: umang.agrawal
This file is temporary and will be merged with columnar.py in the same
folder.
"""
import json
import random
import string
import time
from capellaAPI.capella.columnar.CapellaAPI import CapellaAPI as ColumnarAPI
from sdk_client3 import SDKClient
from cb_server_rest_util.cluster_nodes.cluster_nodes_api import ClusterRestAPI
from TestInput import TestInputServer


class ColumnarInstance:

    def __init__(self, tenant_id, project_id, instance_name=None,
                 instance_id=None, cluster_id=None, instance_endpoint=None,
                 db_users=list(), roles=list()):
        self.tenant_id = tenant_id
        self.project_id = project_id

        self.name = instance_name
        if not instance_name:
            self.name = "Columnar_instance{0}".format(random.randint(1, 100000))

        self.instance_id = instance_id
        self.cluster_id = cluster_id
        self.srv = instance_endpoint

        self.servers = list()
        self.nodes_in_cluster = list()
        self.master = None
        self.cbas_cc_node = self.master

        self.db_users = db_users
        self.columnar_roles = roles
        self.type = "columnar"

        # SDK related objects
        self.sdk_client_pool = None

    def refresh_object(self, servers):
        self.kv_nodes = list()
        self.fts_nodes = list()
        self.cbas_nodes = list()
        self.index_nodes = list()
        self.query_nodes = list()
        self.eventing_nodes = list()
        self.backup_nodes = list()
        self.nodes_in_cluster = list()

        for server in servers:
            server.type = self.type
            if self.type != "default":
                server.memcached_port = "11207"
            if "Data" in server.services or "kv" in server.services:
                self.kv_nodes.append(server)
            if "Query" in server.services or "n1ql" in server.services:
                self.query_nodes.append(server)
            if "Index" in server.services or "index" in server.services:
                self.index_nodes.append(server)
            if "Eventing" in server.services or "eventing" in server.services:
                self.eventing_nodes.append(server)
            if "Analytics" in server.services or "cbas" in server.services:
                self.cbas_nodes.append(server)
            if "Search" in server.services or "fts" in server.services:
                self.fts_nodes.append(server)
            self.nodes_in_cluster.append(server)
        self.master = self.kv_nodes[0]


class DBUser:
    def __init__(self, userId="", username="Administrator",
                 password="password"):
        self.username = username
        self.password = password
        self.roles = list()
        self.privileges = list()
        self.id = userId

    def __str__(self):
        return self.username


class ColumnarRole:
    def __init__(self, roleId="", role_name=""):
        self.id = roleId
        self.name = role_name
        self.privileges = list()

    def __str__(self):
        return self.name


class ColumnarRBACUtil:
    def __init__(self, log):
        self.log = log

    def create_custom_analytics_admin_user(
            self, pod, tenant, project_id, instance,
            username, password):
        privileges_list = [
            "database_create", "database_drop", "scope_create", "scope_drop",
            "collection_create", "collection_drop", "collection_select",
            "collection_insert", "collection_upsert", "collection_delete",
            "collection_analyze", "view_create", "view_drop", "view_select",
            "index_create", "index_drop", "function_create", "function_drop",
            "function_execute", "link_create", "link_drop", "link_alter",
            "link_connect", "link_disconnect", "link_copy_to",
            "link_copy_from", "link_create_collection", "link_describe",
            "synonym_create", "synonym_drop"
        ]

        resources_privileges_map = {
            "name": "",
            "privileges": privileges_list,
            "type": "instance"
        }

        privileges_payload = self.create_privileges_payload([resources_privileges_map])
        analytics_admin_role = self.create_columnar_role(
            pod, tenant, project_id, instance, "analytics_admin",
            privileges_payload)
        if not analytics_admin_role:
            self.log.error("Failed to create analytics admin role")
            return None
        instance.columnar_roles.append(analytics_admin_role)
        analytics_admin_user = self.create_api_keys(pod, tenant, project_id, instance,
                                                    username, password,
                                                    role_ids=[analytics_admin_role.id])
        instance.db_users.append(analytics_admin_user)
        return analytics_admin_user

    def create_privileges_payload(self, resources_privileges_map=[]):
        def get_entity_obj(entity_obj_map={}, entity_name=""):
            if entity_name in entity_obj_map:
                return entity_obj_map[entity_name]
            else:
                entity_obj = {
                    "privileges": []
                }
                entity_obj_map[entity_name] = entity_obj
                return entity_obj

        privileges_payload = {
            "databases": {},
            "links": {},
            "privileges": []
        }
        for res_priv_map in resources_privileges_map:
            res_name = res_priv_map["name"]
            privs = res_priv_map["privileges"]
            res_type = res_priv_map["type"]
            res_entities = res_name.split(".") if res_name else []

            if res_type == "instance":
                privileges_payload["privileges"].extend(privs)
            elif res_type == "link":
                links_payload = privileges_payload["links"]
                if res_name not in links_payload:
                    links_payload[res_name] = []
                link_obj_privs = links_payload[res_name]
                link_obj_privs.extend(privs)
            else:
                db_name = res_entities[0]
                db_obj = get_entity_obj(privileges_payload["databases"], db_name)
                if res_type == "database":
                    db_obj["privileges"].extend(privs)
                else:
                    if "scopes" not in db_obj:
                        db_obj["scopes"] = {}
                    scope_name = res_entities[1]
                    scope_obj = get_entity_obj(db_obj["scopes"], scope_name)
                    if res_type == "scope":
                        scope_obj["privileges"].extend(privs)
                    else:
                        res_field_name = res_type + "s"
                        entity_name = res_entities[2]
                        if res_field_name not in scope_obj:
                            scope_obj[res_field_name] = {}
                        scope_object_payload = scope_obj[res_field_name]
                        if entity_name not in scope_object_payload:
                            scope_object_payload[entity_name] = []
                        entity_privileges = scope_object_payload[entity_name]
                        entity_privileges.extend(privs)

        return privileges_payload

    def create_api_keys(
            self, pod, tenant, project_id, instance, username, password,
            privileges_payload=None, role_ids=[]):

        columnar_api = ColumnarAPI(
            pod.url_public, tenant.api_secret_key, tenant.api_access_key,
            tenant.user, tenant.pwd)

        if not privileges_payload:
            privileges_payload = self.create_privileges_payload()

        api_key_payload = {
            "name": username,
            "password": password,
            "privileges": privileges_payload,
            "roles": role_ids
        }

        resp = columnar_api.create_api_keys(
            tenant.id, project_id, instance.instance_id,
            api_key_payload
        )

        if resp.status_code == 201:
            self.log.info("API keys created successfully")
            user_id = json.loads(resp.content).get("id")
            db_user = DBUser(user_id, username, password)
            db_user.roles.extend(role_ids)
            return db_user
        elif resp.status_code == 500:
            self.log.critical(str(resp.content))
            return None
        else:
            self.log.critical("Unable to create API keys")
            self.log.critical("Capella API returned " + str(
                resp.status_code))
            self.log.critical(resp.json()["message"])
            return None

    def delete_api_keys(
            self, pod, tenant, project_id, instance,
            api_key_id):

        columnar_api = ColumnarAPI(
            pod.url_public, tenant.api_secret_key, tenant.api_access_key,
            tenant.user, tenant.pwd)

        resp = columnar_api.delete_api_keys(tenant.id, project_id,
                                            instance.instance_id,
                                            api_key_id)

        if resp.status_code == 202:
            self.log.info("Successfully deleted API key {}".format(api_key_id))
            return True
        elif resp.status_code == 500:
            self.log.critical(str(resp.content))
            return False
        else:
            self.log.critical("Unable to delete API keys")
            self.log.critical("Capella API returned " + str(
                resp.status_code))
            self.log.critical(resp.json()["message"])
            return False

    def create_columnar_role(
            self, pod, tenant, project_id, instance,
            role_name, privileges_payload = None):

        columnar_api = ColumnarAPI(
            pod.url_public, tenant.api_secret_key, tenant.api_access_key,
            tenant.user, tenant.pwd)

        if not privileges_payload:
            privileges_payload = self.create_privileges_payload()

        role_payload = {
            "name": role_name,
            "privileges": privileges_payload
        }

        resp = columnar_api.create_columnar_role(
            tenant.id, project_id, instance.instance_id,
            role_payload
        )

        if resp.status_code == 201:
            self.log.info("Columnar role created successfully")
            role_id = json.loads(resp.content).get("id")
            columnar_role = ColumnarRole(role_id, role_name)
            return columnar_role
        elif resp.status_code == 500:
            self.log.critical(str(resp.content))
            return None
        else:
            self.log.critical("Unable to create columnar role")
            self.log.critical("Capella API returned " + str(
                resp.status_code))
            self.log.critical(resp.json()["message"])
            return None

    def delete_columnar_role(
            self, pod, tenant, project_id, instance,
            role_id):

        columnar_api = ColumnarAPI(
            pod.url_public, tenant.api_secret_key, tenant.api_access_key,
            tenant.user, tenant.pwd)

        resp = columnar_api.delete_columnar_role(tenant.id, project_id,
                                                 instance.instance_id,
                                                 role_id)

        if resp.status_code == 204:
            self.log.info(f"Successfully deleted columnar role {role_id}")
            return True
        elif resp.status_code == 500:
            self.log.critical(str(resp.content))
            return False
        else:
            self.log.critical("Unable to delete API keys")
            self.log.critical("Capella API returned " + str(
                resp.status_code))
            self.log.critical(resp.json()["message"])
            return False


class ColumnarUtils:
    def __init__(self, log):
        self.log = log

    """
    Method generates config for creating columnar instance.
    """
    def generate_instance_configuration(
            self, name=None, description=None, provider=None, region=None,
            nodes=0, instance_types=None, support_package=None,
            availability_zone="single", token=None, image=None):
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
            nodes = random.choice([1, 2, 4, 8, 16, 32])

        if not instance_types:
            instance_types = {
                "vcpus": "4vCPUs",
                "memory": "16GB"
            }

        if not support_package:
            support_package = {
                "key": "Developer Pro",
                "timezone": "PT"
            }

        config = {
            "name": name,
            "description": description,
            "provider": provider,
            "region": region,
            "nodes": nodes,
            "instanceTypes": instance_types,
            "package": support_package,
            "availabilityZone": availability_zone
        }
        if image and token:
            config.update({
                "overRide": {
                    "token": token,
                    "image": image
                }
            })
        self.log.debug(f"Columnar Instance deployment config - {str(config)}")
        return config

    def create_instance(self, pod, tenant, instance_config=None, timeout=7200):
        columnar_api = ColumnarAPI(
            pod.url_public, tenant.api_secret_key, tenant.api_access_key,
            tenant.user, tenant.pwd)
        if not instance_config:
            instance_config = self.generate_instance_configuration()
        resp = columnar_api.create_columnar_instance(
            tenant.id, tenant.project_id, instance_config)
        instance_id = None
        if resp.status_code == 201:
            instance_id = json.loads(resp.content).get("id")
        elif resp.status_code == 500:
            self.log.critical(str(resp.content))
            raise Exception(str(resp.content))
        elif resp.status_code == 422:
            if resp.content.decode("utf-8").find(
                    "not allowed based on your activation status") != -1:
                self.log.critical("Tenant is not activated yet...retrying")
            else:
                self.log.critical(resp.content)
                raise Exception("Cluster deployment failed.")
        else:
            self.log.error("Unable to create goldfish cluster {0} in project "
                           "{1}".format(instance_config["name"],
                                        tenant.project_id))
            self.log.critical("Capella API returned " + str(
                resp.status_code))
            self.log.critical(resp.json()["message"])
        time.sleep(5)
        self.log.info("Cluster created with cluster ID: {}" \
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
            self.log.info(
                "Columnar instance is deployed successfully in %s s" % str(
                    time.time() - start_time))
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
            self.log.error("Unable to delete columnar instance {0}/{1}: {2}"
                           .format(instance.name, instance.instance_id,
                                   resp.content))
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
            columnar_instance_info["data"]["name"],
            columnar_instance_info["data"]["description"], nodes)
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
        state = None
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
            self, pod, tenant, project_id, instance, timeout=3600,
            verify_with_backend_cluster=False, expected_num_of_nodes=0):
        columnar_api = ColumnarAPI(
            pod.url_public, tenant.api_secret_key, tenant.api_access_key,
            tenant.user, tenant.pwd)
        end_time = time.time() + timeout
        state = None
        while time.time() < end_time:
            resp = self.get_instance_info(
                pod, tenant, project_id, instance.instance_id, columnar_api)
            if resp:
                state = resp["data"]["state"]
                if state == "scaling":
                    self.log.info("Instance is still scaling. Waiting for 10s.")
                    time.sleep(10)
                else:
                    break
        if state == "healthy":
            if verify_with_backend_cluster:
                self.log.info("Verifying on backend cluster")
                rest = ClusterRestAPI(instance.master)
                end_time = time.time() + timeout
                while time.time() < end_time:
                    status, content = rest.cluster_details()
                    if status:
                        if ("nodes" in content) and (len(content["nodes"]) ==
                                                     expected_num_of_nodes):
                            self.update_columnar_instance_obj(
                                pod, tenant, instance)
                            return True
                    time.sleep(10)
                self.log.error(
                    "Instance was not rebalanced. Expected Nodes - {0}, "
                    "Actual Nodes - {1}.".format(
                        expected_num_of_nodes, len(content["nodes"])))
                self.log.error("This can be due to rebalance job still "
                               "pending on control plane or can be a issue. "
                               "Please verify manually")
                return False
            self.update_columnar_instance_obj(pod, tenant, instance)
            return True
        else:
            self.log.error("Instance {0} failed to scale even after {1} "
                           "seconds. Current instance state - {2}".format(
                instance.name, timeout, state))
            return False

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
                    self.log.error(f"Following error recieved {resp.text}")
                return False
        return True

    def turn_off_instance(self, pod, tenant, project_id, instance,
                          wait_to_turn_off=True, timeout=900):
        columnar_api = ColumnarAPI(
            pod.url_public, tenant.api_secret_key, tenant.api_access_key,
            tenant.user, tenant.pwd)
        resp = columnar_api.turn_off_instance(tenant.id, project_id,
                                              instance.instance_id)
        if resp.status_code == 202:
            self.log.info("Started turning off instance")
            if wait_to_turn_off:
                return self.wait_for_instance_to_turn_off(
                    pod, tenant, project_id, instance, timeout)
        else:
            self.log.error(
                "Instance turn off API failed with status code: {}".format(
                    resp.status_code))
            return False

    def wait_for_instance_to_turn_off(self, pod, tenant, project_id,
                                      instance, timeout=900):
        status = None
        end_time = time.time() + timeout
        # First wait for turn-off job to get picked up by control plane
        while status != 'turning_off' and time.time() < end_time:
            resp = self.get_instance_info(pod, tenant, project_id,
                                          instance.instance_id)
            status = resp["data"]["state"]
            self.log.info("Waiting for instance to be in turning off state")
            time.sleep(30)
        if status != 'turning_off':
            self.log.error("Instance turn-off was not initiated by control "
                           "place even after {} seconds".format(timeout))
            return False

        end_time = time.time() + timeout
        while (status == 'turning_off' or not status) and (
                time.time() < end_time):
            resp = self.get_instance_info(
                pod, tenant, project_id, instance.instance_id)
            status = resp["data"]["state"]
        if status == "turned_off":
            self.log.info("Instance turned off successful")
            return True
        else:
            self.log.error("Failed to turn off the instance")
            return False

    def turn_on_instance(self, pod, tenant, project_id, instance,
                         wait_to_turn_on=True, timeout=900):
        columnar_api = ColumnarAPI(
            pod.url_public, tenant.api_secret_key, tenant.api_access_key,
            tenant.user, tenant.pwd)
        resp = columnar_api.turn_on_instance(tenant.id, project_id,
                                             instance.instance_id)
        if resp.status_code == 202:
            self.log.info("Started turning on instance")
            if wait_to_turn_on:
                return self.wait_for_instance_to_turn_on(
                    pod, tenant, project_id, instance, timeout)
        else:
            self.log.error(
                "Instance turn on API failed with status code: {}".format(
                    resp.status_code))
            return False

    def wait_for_instance_to_turn_on(self, pod, tenant, project_id,
                                     instance, timeout=900):
        status = None
        end_time = time.time() + timeout
        # First wait for turn-on job to get picked up by control plane
        while status != 'turning_on' and time.time() < end_time:
            resp = self.get_instance_info(pod, tenant, project_id,
                                          instance.instance_id)
            status = resp["data"]["state"]
            self.log.info("Waiting for instance to be in turning on state")
            time.sleep(30)
        if status != 'turning_on':
            self.log.error("Instance turn-on was not initiated by control "
                           "place even after {} seconds".format(timeout))
            return False

        end_time = time.time() + timeout
        while (status == 'turning_on' or not status) and (
                time.time() < end_time):
            resp = self.get_instance_info(
                pod, tenant, project_id, instance.instance_id)
            status = resp["data"]["state"]
            self.log.info("Instance is still turning on")
            time.sleep(20)
        if status == "healthy":
            self.log.info("Instance turned on successful")
            self.update_columnar_instance_obj(pod, tenant, instance)
            return True
        else:
            self.log.error("Failed to turn on the instance")
            return False

    def create_couchbase_cloud_qe_user(self, pod, tenant, instance):
        columnar_api = ColumnarAPI(
            pod.url_public, tenant.api_secret_key, tenant.api_access_key,
            tenant.user, tenant.pwd, TOKEN_FOR_INTERNAL_SUPPORT=pod.TOKEN)
        resp = columnar_api.create_analytics_admin_user(instance.instance_id)
        if resp.status_code == 200:
            self.log.info("Created user couchbase-cloud-qe")
            return resp.json()["username"], resp.json()["password"]
        elif resp.status_code == 422 and resp.json()[
            "errorType"] == "ErrDataplaneUserNameExists":
            if self.delete_couchbase_cloud_qe_user(pod, tenant, instance):
                return self.create_couchbase_cloud_qe_user(
                    pod, tenant, instance)
            else:
                return None, None
        else:
            self.log.error("Unable to create user couchbase-cloud-qe")
            return None, None

    def delete_couchbase_cloud_qe_user(self, pod, tenant, instance):
        columnar_api = ColumnarAPI(
            pod.url_public, tenant.api_secret_key, tenant.api_access_key,
            tenant.user, tenant.pwd, TOKEN_FOR_INTERNAL_SUPPORT=pod.TOKEN)
        resp = columnar_api.delete_analytics_admin_user(instance.instance_id)
        if resp.status_code == 204:
            self.log.info("Deleted user couchbase-cloud-qe")
            return True
        else:
            self.log.error("Unable to delete user couchbase-cloud-qe")
            return False

    def update_columnar_instance_obj(self, pod, tenant, instance):
        info_resp = self.get_instance_info(
            pod=pod, tenant=tenant, project_id=tenant.project_id,
            instance_id=instance.instance_id)

        if not info_resp:
            raise Exception(
                "Failed fetching connection string for following instance - "
                "{0}".format(instance.instance_id))

        instance.name = str(info_resp["data"]["name"])
        instance.srv = info_resp["data"]["config"]["endpoint"]
        instance.cluster_id = info_resp["data"]["config"]["clusterId"]

        if not instance.master:
            # Fixing the instance master node, such that master node ip and
            # hostname is instance's connection string, port is 18091 and
            # username and password are couchbase-cloud-qe and it's
            # password. This is done so that when the cluster scales or
            # turns on/off or is restored the connection string does not
            # change even if the nodes in the backend cluster changes.
            instance.master = TestInputServer()
            instance.master.ip = instance.srv
            instance.master.hostname = instance.srv
            instance.master.port = "18091"
            instance.master.type = "columnar"
            instance.master.memcached_port = "11207"
            instance.master.rest_username = instance.username
            instance.master.rest_password = instance.password
        else:
            instance.master.ip = instance.srv
            instance.master.hostname = instance.srv

        for i in range(0, 10):
            try:
                rest = ClusterRestAPI(instance.master)
                break
            except Exception as err:
                if i == 9:
                    raise Exception(str(err))
                else:
                    self.log.info(
                        "DNS entry for the cluster might not have propogated, "
                        "hence waiting for 1 minutes to retry.")
                    time.sleep(60)

        status, content = rest.cluster_details()
        if not status:
            raise Exception("Error while fetching pools/default using "
                            "connection string")

        instance.servers = list()

        for t_server in content["nodes"]:
            temp_server = TestInputServer()
            temp_server.ip = t_server.get("hostname").replace(":8091", "")
            temp_server.hostname = t_server.get("hostname")
            temp_server.services = t_server.get("services")
            temp_server.port = "18091"
            temp_server.type = "columnar"
            temp_server.memcached_port = "11207"
            temp_server.rest_username = instance.username
            temp_server.rest_password = instance.password
            instance.servers.append(temp_server)
        instance.nodes_in_cluster = instance.servers
        instance.cbas_cc_node = instance.servers[0]
