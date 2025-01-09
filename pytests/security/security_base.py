import json
import shlex
import subprocess
import time
import string
import random

import requests
# import itertools
# import base64
# from datetime import datetime
from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI as CapellaAPIv2
from capellaAPI.capella.columnar.CapellaAPI import CapellaAPI as ColumnarAPI
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI
from constants.cloud_constants.capella_constants import AWS
from TestInput import TestInputSingleton
# from couchbase_utils.capella_utils.dedicated import CapellaUtils
from pytests.cb_basetest import CouchbaseBaseTest


class SecurityBase(CouchbaseBaseTest):
    cidr = "10.0.0.0"

    def setUp(self):
        CouchbaseBaseTest.setUp(self)

        self.log.info("-------Setup started for SecurityBase-------")

        self.url = self.input.capella.get("pod")
        self.user = self.input.capella.get("capella_user")
        self.passwd = self.input.capella.get("capella_pwd")
        self.access_key = self.input.capella.get("access_key")
        self.secret_key = self.input.capella.get("secret_key")
        self.tenant_id = self.input.capella.get("tenant_id")
        self.prefix = "Security_API_Test_"
        self.invalid_id = "00000000-0000-0000-0000-000000000000"
        self.count = 0
        self.server_version = self.input.capella.get("server_version", "7.6")
        self.internal_token = self.input.capella.get("internal_token")
        self.provider = self.input.param("provider", AWS.__str__).lower()
        self.num_nodes = {
            "data": self.input.param("kv_nodes", 3),
            "query": self.input.param("n1ql_nodes", 2),
            "index": self.input.param("gsi_nodes", 2),
            "search": self.input.param("fts_nodes", 2),
            "analytics": self.input.param("cbas_nodes", 2),
            "eventing": self.input.param("eventing_nodes", 2)
        }
        self.compute_cpu = {
            "data": self.input.param("kv_cpu", 4),
            "query": self.input.param("n1ql_cpu", 4),
            "index": self.input.param("gsi_cpu", 4),
            "search": self.input.param("fts_cpu", 4),
            "analytics": self.input.param("cbas_cpu", 4),
            "eventing": self.input.param("eventing_cpu", 4)
        }
        self.compute_ram = {
            "data": self.input.param("kv_ram", 16),
            "query": self.input.param("n1ql_ram", 16),
            "index": self.input.param("gsi_ram", 16),
            "search": self.input.param("fts_ram", 16),
            "analytics": self.input.param("cbas_ram", 16),
            "eventing": self.input.param("eventing_ram", 16)
        }
        self.disk = {
            "data": self.input.param("kv_disk", 64),
            "query": self.input.param("n1ql_disk", 64),
            "index": self.input.param("gsi_disk", 64),
            "search": self.input.param("fts_disk", 64),
            "analytics": self.input.param("cbas_disk", 64),
            "eventing": self.input.param("eventing_disk", 64)
        }
        self.disk_type = {
            "aws": "gp3",
            "azure": "P6",
            "gcp": "pd-ssd"
        }
        self.iops = {
            "aws": 3000,
            "azure": 240
        }
        self.capellaAPI = CapellaAPI("https://" + self.url, '', '', self.user, self.passwd, '')
        self.capellaAPIv2 = CapellaAPIv2("https://" + self.url, self.secret_key, self.access_key,
                                         self.user, self.passwd)
        self.capellaAPI_internal = CapellaAPIv2("https://" + self.url, self.secret_key, self.access_key,
                                                self.user, self.passwd, TOKEN_FOR_INTERNAL_SUPPORT=self.internal_token)
        self.columnarAPI = ColumnarAPI("https://" + self.url, '', '', self.user, self.passwd, '')
        self.create_initial_v4_api_keys()
        self.create_different_organization_roles()
        self.create_api_keys_for_different_roles()
        self.project_id = self.input.capella.get("project_id", None)
        self.instance_id = None
        self.cmek_id = None
        if self.project_id is None:
            self.create_project(self.prefix + "Project")

        self.cluster_id = self.input.capella.get("cluster_id", None)
        if self.cluster_id is None:
            self.create_cluster(self.prefix + "Cluster", self.server_version, self.provider)
        self.instance_id = self.input.capella.get("instance_id", None)
        if self.instance_id is None:
            self.create_columnar_cluster(self.prefix + "Columnar-Cluster")
        if self.cluster_id is not None:
            self.sleep(10, "Waiting for cluster to be responsive")
            self.allow_ip(self.cluster_id, self.project_id)

        self.log.info("-------Setup finished for CouchbaseBaseTest-------")

    def tearDown(self):
        self.log.info("-------Teardown started for SecurityBase-------")

        if self.input.capella.get("cluster_id") is None:
            self.delete_cluster()

        if self.input.capella.get("instance_id") is None:
            self.delete_columnar_cluster()

        if self.input.capella.get("project_id") is None:
            self.delete_project()

        self.delete_api_keys_for_different_roles()
        self.delete_different_organization_roles()

        self.log.info("-------Teardown finished for SecurityBase-------")

    def connect_node_port(self, node, ports, expect_to_connect=True):
        session = requests.Session()
        for port in ports:
            connect = "https://" + node + ":" + port
            self.log.info("Trying to connect to {0}".format(connect))
            try:
                session.get(connect, params='', headers=None, timeout=60, verify=False)
            except requests.exceptions.ConnectionError as e:
                if expect_to_connect:
                    self.fail(
                        msg="Connection to the node should have passed. Failed with error: {0} on "
                            "port: {1}".format(e, port))
            else:
                if not expect_to_connect:
                    self.fail(
                        msg="Connection to the node should have failed on port: {0}".format(
                            port))

    def allow_ip(self, cluster_id, project_id):
        resp = self.capellaAPI.allow_my_ip(self.tenant_id, project_id,
                                           cluster_id)
        if resp.status_code != 202:
            result = json.loads(resp.content)
            if result["errorType"] == "ErrAllowListsCreateDuplicateCIDR":
                self.log.warn("IP is already added: %s" % result["message"])
                return
            self.log.critical(resp.content)
            raise Exception("Adding allowed IP failed.")
        self.log.info("IP added successfully")

    def create_project(self, project_name):
        self.log.info("Creating Project for Security test")
        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id, project_name)
        if resp.status_code != 201:
            self.fail("Project Creation failed in SecurityBase Setup. Reason: {}".format(
                resp.content))
        else:
            self.project_id = resp.json()["id"]
            self.log.info("Project Created Successfully. Project ID: {}".format(self.project_id))

    def delete_project(self):
        self.log.info("Deleting project with id: {}".format(self.project_id))

        self.log.info("Fetching all the clusters under the project")
        resp = self.capellaAPI.cluster_ops_apis.list_clusters(self.tenant_id,
                                                              self.project_id)

        resp = resp.json()

        cluster_ids = list()
        for cluster in resp["data"]:
            cluster_ids.append(cluster["id"])
            if cluster["currentState"] == "healthy":
                self.log.info("Deleting cluster from project with cluster id: {}".format(
                    cluster["id"]))
                delete_resp = self.capellaAPI.cluster_ops_apis.delete_cluster(self.tenant_id,
                                                                              self.project_id,
                                                                              cluster["id"])
                self.assertEqual(202, delete_resp.status_code,
                                 msg='FAIL. Outcome: {}, Expected: {}. Reason: {}'.format(
                                     delete_resp.status_code, 202, delete_resp.content))

        for id in cluster_ids:
            resp = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(self.tenant_id,
                                                                       self.project_id,
                                                                       id)

            end_time = time.time() + 1800
            while resp.status_code != 404 and time.time() < end_time:
                resp = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(self.tenant_id,
                                                                           self.project_id,
                                                                           id)
                self.sleep(15, "Waiting for clusters to be deleted in the project")

        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id, self.project_id)
        if resp.status_code != 204:
            self.fail("Project Deletion failed in SecurityBase Teardown. Reason: {}".format(
                resp.content))
        else:
            self.log.info("Project Deleted Successfully")

    def wait_for_columnar_instance_to_turn_off(self, instance_id, timeout=1800):
        start_time = time.time()
        while time.time() < start_time + timeout:
            resp = self.columnarAPI.get_specific_columnar_instance(self.tenant_id,
                                                                   self.project_id,
                                                                   instance_id)
            if resp.status_code != 200:
                self.log.error("Unable to fetch details for columnar cluster {}." \
                               " Returned status code : {}".
                               format(instance_id, resp.status_code))
                self.sleep(10)
            else:
                state = json.loads(resp.content)["data"]["state"]
                self.log.info("Cluster %s state: %s" % (instance_id, state))
                if state == "turning_off":
                    self.sleep(10)
                else:
                    break
        if state == "turned_off":
            self.log.info("Columnar cluster turned off successfully")
        else:
            self.fail("Failed to deploy columnar cluster even after {} seconds." \
                      " Cluster state: {}".format(timeout, state))

    def wait_for_columnar_instance_to_turn_on(self, instance_id, timeout=1800):
        start_time = time.time()
        while time.time() < start_time + timeout:
            resp = self.columnarAPI.get_specific_columnar_instance(self.tenant_id,
                                                                   self.project_id,
                                                                   instance_id)
            if resp.status_code != 200:
                self.log.error("Unable to fetch details for columnar cluster {}." \
                               " Returned status code : {}".
                               format(instance_id, resp.status_code))
                self.sleep(10)
            else:
                state = json.loads(resp.content)["data"]["state"]
                self.log.info("Cluster %s state: %s" % (instance_id, state))
                if state == "turning_on":
                    self.sleep(10)
                else:
                    break
        if state == "healthy":
            self.log.info("Columnar cluster turned on successfully")
        else:
            self.fail("Failed to deploy columnar cluster even after {} seconds." \
                      " Cluster state: {}".format(timeout, state))

    def wait_for_columnar_instance_to_deploy(self, instance_id, timeout=1800):
        start_time = time.time()
        while time.time() < start_time + timeout:
            resp = self.columnarAPI.get_specific_columnar_instance(self.tenant_id,
                                                                   self.project_id,
                                                                   instance_id)
            if resp.status_code != 200:
                self.log.error("Unable to fetch details for columnar cluster {}." \
                               " Returned status code : {}".
                               format(instance_id, resp.status_code))
                self.sleep(10)
            else:
                state = json.loads(resp.content)["data"]["state"]
                self.log.info("Cluster %s state: %s" % (instance_id, state))
                if state == "deploying":
                    self.sleep(10)
                else:
                    break
        if state == "healthy":
            self.log.info("Columnar cluster deployed successfully")
        else:
            self.fail("Failed to deploy columnar cluster even after {} seconds." \
                      " Cluster state: {}".format(timeout, state))

    def get_columnar_cluster_nodes(self, instance_id):
        resp = self.columnarAPI.get_specific_columnar_instance(self.tenant_id,
                                                               self.project_id,
                                                               instance_id)
        if resp.status_code != 200:
            self.log.error("Unable to fetch details for columnar cluster {}." \
                           " Returned status code : {}. Error: {}".
                           format(instance_id, resp.status_code,
                                  resp.content))
        srv = json.loads(resp.content)["data"]["config"]["endpoint"]
        cmd = "dig @8.8.8.8  _couchbases._tcp.{} srv".format(srv)
        proc = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE)
        out, _ = proc.communicate()
        servers = list()
        for line in out.split("\n"):
            if "11207" in line:
                servers.append(line.split("11207")[-1].rstrip(".").lstrip(" "))

        return servers

    def wait_for_columnar_instance_to_scale(self, instance_id, timeout=1800):
        start_time = time.time()
        while time.time() < start_time + timeout:
            resp = self.columnarAPI.get_specific_columnar_instance(self.tenant_id,
                                                                   self.project_id,
                                                                   instance_id)
            if resp.status_code != 200:
                self.log.error("Unable to fetch details for columnar cluster {}." \
                               " Returned status code : {}. Error: {}".
                               format(instance_id, resp.status_code,
                                      resp.content))
                self.sleep(10)
            else:
                state = json.loads(resp.content)["data"]["state"]
                self.log.info("Cluster %s state: %s" % (instance_id, state))
                if state == "scaling":
                    self.sleep(10)
                else:
                    break
        if state == "healthy":
            self.log.info("Columnar cluster deployed successfully")
        else:
            self.fail("Failed to deploy columnar cluster even after {} seconds." \
                      " Cluster state: {}".format(timeout, state))

    def create_columnar_cluster(self, cluster_name):
        num_clusters = TestInputSingleton.input.param("num_columnar_clusters", 0)
        for i in range(0, num_clusters):
            self.log.info("Creating columnar cluster")
            payload = self.get_columnar_cluster_payload(cluster_name + i)
            resp = self.columnarAPI.create_columnar_instance(self.tenant_id,
                                                             self.project_id,
                                                             payload)
            if resp.status_code != 201:
                self.fail("Failed to create columnar instance: {}".format(resp.content))
            else:
                self.instance_id = resp.json()["id"]
                self.wait_for_columnar_instance_to_deploy(self.instance_id)

    def create_cluster(self, cluster_name, server_version, provider="AWS", deploy_payload={}):
        num_clusters = TestInputSingleton.input.param("num_clusters", 1)
        single_node = TestInputSingleton.input.param("single_node", False)
        if single_node:
            payload = self.get_single_node_cluster_payload(provider)
            payload["serverVersion"] = TestInputSingleton.input.param("server_version", "7.6")

            while (self.cluster_id is None):
                resp = self.capellaAPIv2.get_unique_cidr(self.tenant_id)
                subnet = resp.json()["cidr"]["suggestedBlock"]
                payload["cidr"] = subnet
                resp = self.capellaAPIv2.deploy_v2_cluster(self.tenant_id, payload)
                if resp.status_code == 202:
                    self.cluster_id = resp.json()['id']
                    break
                elif "CIDR" in resp.json()["message"]:
                    continue
                else:
                    self.assertFalse(resp.status_code, "Failed to create a cluster with error "
                                                       "as {}".format(resp.content))

            status = self.get_cluster_status(self.cluster_id)
            self.assertEqual(status, "healthy",
                             msg="FAIL, Outcome: {}, Expected: {}".format(status, "healthy"))

            self.log.info("Cluster creation is successful. Cluster ID: {}".format(self.cluster_id))
            return

        for _ in range(0, num_clusters):
            self.log.info("Creating Cluster for Security Test")
            payload = self.get_cluster_payload(provider)
            payload["name"] = cluster_name
            if len(server_version) > 3 and not self.input.capella.get("image", None):
                server_version = server_version[:3]

            payload["couchbaseServer"]["version"] = server_version

            end_time = time.time() + 1800
            while time.time() < end_time:
                resp = self.capellaAPIv2.get_unique_cidr(self.tenant_id)
                subnet = resp.json()["cidr"]["suggestedBlock"]
                payload["cloudProvider"]["cidr"] = subnet
                self.log.info("Trying out with cidr {}".format(subnet))

                if self.input.capella.get("image"):
                    ami_payload = self.get_custom_ami_payload()
                    image = self.input.capella["image"]
                    token = self.input.capella["override_token"]
                    server_version = self.input.capella["server_version"]
                    release_id = self.input.capella.get("release_id", None)
                    ami_payload["overRide"]["token"] = token
                    ami_payload["overRide"]["image"] = image
                    ami_payload["overRide"]["server"] = server_version
                    ami_payload["projectId"] = self.project_id
                    resp = self.capellaAPIv2.get_unique_cidr(self.tenant_id)
                    subnet = resp.json()["cidr"]["suggestedBlock"]
                    ami_payload["cidr"] = subnet
                    self.log.info("Trying out with cidr {}".format(subnet))
                    if release_id:
                        ami_payload["overRide"]["releaseId"] = release_id

                    resp = self.capellaAPI.create_cluster_customAMI(self.tenant_id,
                                                                    ami_payload)
                else:

                    payload.update(deploy_payload)
                    if self.cmek_id:
                        cmek = {"cmekId": self.cmek_id}
                        resp = self.capellaAPI.cluster_ops_apis.create_cluster(self.tenant_id,
                                                                               self.project_id,
                                                                               payload["name"],
                                                                               payload["cloudProvider"],
                                                                               couchbaseServer=payload["couchbaseServer"],
                                                                               serviceGroups=payload["serviceGroups"],
                                                                               availability=payload["availability"],
                                                                               support=payload["support"],
                                                                               description="",
                                                                               headers=None,
                                                                               **cmek)
                    else:
                        resp = self.capellaAPI.cluster_ops_apis.create_cluster(self.tenant_id,
                                                                               self.project_id,
                                                                               payload["name"],
                                                                               payload["cloudProvider"],
                                                                               payload["couchbaseServer"],
                                                                               payload["serviceGroups"],
                                                                               payload["availability"],
                                                                               payload["support"])

                if resp.status_code == 202:
                    self.cluster_id = resp.json()['id']
                    break

                elif "CIDR" in resp.json()["message"]:
                    continue

                else:
                    self.assertFalse(resp.status_code, "Failed to create a cluster with error "
                                                       "as {}".format(resp.content))

            self.sleep(5, "Waiting for 5 seconds for cluster to be responsive")
            status = self.get_cluster_status(self.cluster_id)
            self.assertEqual(status, "healthy",
                             msg="FAIL, Outcome: {}, Expected: {}".format(status, "healthy"))

            self.log.info("Cluster creation is successful. Cluster ID: {}".format(self.cluster_id))

    def delete_columnar_cluster(self):
        if self.instance_id is None:
            self.log.info("No columnar clusters to delete")
            return

        self.log.info("Deleting cluster with id: {}".format(self.instance_id))
        resp = self.columnarAPI.delete_columnar_instance(self.instance_id, self.project_id,
                                                         self.instance_id)
        if resp.status_code != 202:
            self.fail("Columnar cluster deletion failed in SecurityBase TearDown." \
                      "Reason: {}".format(resp.content))

        self.log.info("Columnar cluster deletion successful")

    def delete_cluster(self):
        if self.cluster_id is None:
            self.log.info("No clusters to delete")
            return

        self.log.info("Deleting cluster with id: {}".format(self.cluster_id))
        resp = self.capellaAPI.cluster_ops_apis.delete_cluster(self.tenant_id,
                                                               self.project_id,
                                                               self.cluster_id)

        if resp.status_code != 202:
            self.fail("Cluster deletion failed in SecurityBase TearDown. Reason; {}".format(
                resp.content))
        else:
            self.log.info("Waiting for cluster to be deleted")
            status_code = 200
            end_time = time.time() + 1800
            while status_code != 404 and time.time() < end_time:
                self.sleep(15, "Waiting for cluster to be deleted")
                resp = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(self.tenant_id,
                                                                           self.project_id,
                                                                           self.cluster_id)
                status_code = resp.status_code
            if status_code != 404:
                self.fail("Cluster Deletion failed in SecurityBase TearDown")

            self.log.info("Cluster Deletion Successful")

    @staticmethod
    def get_next_cidr():
        addr = SecurityBase.cidr.split(".")
        if int(addr[1]) < 255:
            addr[1] = str(int(addr[1]) + 1)
        elif int(addr[2]) < 255:
            addr[2] = str(int(addr[2]) + 1)
        SecurityBase.cidr = ".".join(addr)
        return SecurityBase.cidr

    def get_service_groups(self, provider="aws"):
        service_groups = self.input.param("services", "data:query:index:search")

        service_groups_payload = []
        for service_group in service_groups.split("-"):
            services = sorted(service_group.split(":"))

            service_group_payload = {}
            service_group_payload["node"] = {
                "compute": {
                    "cpu": self.compute_cpu[services[0]],
                    "ram": self.compute_ram[services[0]]
                },
                "disk": {
                    "storage": self.disk[services[0]],
                    "type": self.disk_type[provider],
                    "iops": self.iops[provider] if provider != "gcp" else None
                }
            }
            service_group_payload["services"] = services
            service_group_payload["numOfNodes"] = self.num_nodes[services[0]]

            service_groups_payload.append(service_group_payload)

        return service_groups_payload

    def get_columnar_cluster_payload(self, name):
        config = {
            "name": name,
            "description": "",
            "provider": "aws",
            "region": "us-east-1",
            "nodes": 1,
            "instanceTypes": {
                "vcpus": "4vCPUs",
                "memory": "16GB"
            },
            "package": {
                "key": "Developer Pro",
                "timezone": "PT"
            },
            "availabilityZone": "single"
        }

        return config

    def get_cluster_payload(self, cloud_provider):
        cluster_payloads = {
            "AWS": {
                "name": self.prefix + "Cluster",
                "description": "Security Cluster v4",
                "cloudProvider": {
                    "type": self.provider,
                    "region": "us-east-1",
                    "cidr": "10.7.22.0/23"
                },
                "couchbaseServer": {
                    "version": self.server_version
                },
                "serviceGroups": self.get_service_groups("aws"),
                "availability": {
                    "type": "multi"
                },
                "support": {
                    "plan": "enterprise",
                    "timezone": "PT"
                }
            },
            "Azure": {
                "name": "Azure-Test-Cluster-V4-Koushal",
                "description": "My first test azure cluster.",
                "cloudProvider": {
                    "type": "azure",
                    "region": "eastus",
                    "cidr": "10.1.35.0/23"
                },
                "couchbaseServer": {
                    "version": self.server_version
                },
                "serviceGroups": self.get_service_groups("azure"),
                "availability": {
                    "type": "single"
                },
                "support": {
                    "plan": "basic",
                    "timezone": "ET"
                }
            },
            "GCP": {
                "name": "GCP-Test-Cluster-V4-Koushal",
                "description": "My first test gcp cluster.",
                "cloudProvider": {
                    "type": "gcp",
                    "region": "us-east1",
                    "cidr": "10.9.82.0/23"
                },
                "couchbaseServer": {
                    "version": self.server_version
                },
                "serviceGroups": self.get_service_groups("gcp"),
                "availability": {
                    "type": "single"
                },
                "support": {
                    "plan": "basic",
                    "timezone": "ET"
                }
            }
        }

        if cloud_provider.lower() == "aws":
            return cluster_payloads["AWS"]
        elif cloud_provider.lower() == "azure":
            return cluster_payloads["Azure"]
        elif cloud_provider.lower() == "gcp":
            return cluster_payloads["GCP"]

    def get_single_node_cluster_payload(self, provider):
        payload_AWS = {
            "name": self.prefix + "Cluster",
            "description": "",
            "projectId": self.project_id,
            "provider": "aws",
            "region": "us-east-1",
            "plan": "Developer Pro",
            "supportTimezone": "PT",
            "serverVersion": "7.6",
            "cidr": "",
            "deploymentType": "single",
            "serviceGroupsTemplate": "singleA",
            "serviceGroups": [
                {
                    "key": "dataWithAny2",
                    "compute": "m6g.xlarge",
                    "storage": {
                        "key": "gp3",
                        "sizeInGb": 50,
                        "iops": 3000,
                        "autoScalingEnabled": True
                    },
                    "services": ["kv", "index", "n1ql"],
                    "nodes": 1
                }
            ],
            "availabilityZones": {"key": "single"},
            "enablePrivateDNSResolution": False
        }

        payload_Azure = {
            "name": self.prefix + "Cluster",
            "description": "",
            "projectId": self.project_id,
            "provider": "azure",
            "region": "eastus",
            "plan": "Developer Pro",
            "supportTimezone": "PT",
            "serverVersion": "7.6",
            "cidr": "",
            "deploymentType": "single",
            "serviceGroupsTemplate": "singleA",
            "serviceGroups": [
                {
                    "key": "dataWithAny2",
                    "compute": "Standard_D4s_v5",
                    "storage": {
                        "key": "P6",
                        "sizeInGb": 64,
                        "iops": 240,
                        "autoScalingEnabled": False
                    },
                    "services": ["kv", "index", "n1ql", "fts", "eventing", "cbas"],
                    "nodes": 1
                }
            ],
            "availabilityZones": {
                "key": "single"
            },
            "enablePrivateDNSResolution": False
        }

        payload_GCP = {
            "name": self.prefix + "Cluster",
            "description": "",
            "projectId": self.project_id,
            "provider": "gcp",
            "region": "us-east1",
            "plan": "Developer Pro",
            "supportTimezone": "PT",
            "serverVersion": "7.6",
            "cidr": "",
            "deploymentType": "single",
            "serviceGroupsTemplate": "singleA",
            "serviceGroups": [
                {
                    "key": "dataWithAny2",
                    "compute": "n2-standard-4",
                    "storage": {
                        "key": "pd-ssd",
                        "sizeInGb": 50,
                        "autoScalingEnabled": True
                    },
                    "services": ["kv", "index", "n1ql", "fts", "eventing", "cbas"],
                    "nodes": 1
                }
            ],
            "availabilityZones": {"key": "single"}, "enablePrivateDNSResolution": False
        }

        if provider.lower() == "aws":
            return payload_AWS
        elif provider.lower() == "azure":
            return payload_Azure
        elif provider.lower() == "gcp":
            return payload_GCP

        return payload_AWS

    def get_custom_ami_payload(self):
        payload = {
            'package': 'developerPro',
            'overRide': {
                'image': '',
                'server': '7.6.0',
                'token': ''
            },
            'singleAZ': False,
            'description': '',
            'specs': [
                {
                    'count': 3,
                    'services': [
                        {'type': 'kv'}
                    ],
                    'compute': {
                        'type': 'm5.xlarge'
                    },
                    'disk': {
                        'type': 'gp3',
                        'sizeInGb': 50,
                        'iops': 5740
                    },
                    'diskAutoScaling': {
                        'enabled': True
                    }
                }
            ],
            'provider': 'hostedAWS',
            'name': 'Security_API_Test_',
            'cidr': '10.1.0.0/20',
            'region': 'us-west-2',
            'projectId': ''
        }

        return payload

    def get_cluster_status(self, cluster_id):
        status = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(self.tenant_id,
                                                                     self.project_id,
                                                                     cluster_id)
        status = status.json()["currentState"]
        while status != 'healthy':
            self.sleep(15, "Waiting for cluster to be in healthy state. Current status - {}"
                       .format(status))
            if status in ["deploymentFailed", "scaleFailed", "upgradeFailed", "rebalanceFailed",
                          "peeringFailed", "destroyFailed", "offline", "turningOffFailed",
                          "turningOnFailed"]:
                self.fail("FAIL. Cluster status is -{}".format(status))
            cluster_ready_resp = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                self.tenant_id,
                self.project_id,
                cluster_id)
            cluster_ready_resp = cluster_ready_resp.json()
            status = cluster_ready_resp["currentState"]

        return status

    def create_initial_v4_api_keys(self):
        self.capellaAPI = CapellaAPI("https://" + self.url, '', '', self.user, self.passwd, '')
        resp = self.capellaAPI.create_control_plane_api_key(self.tenant_id, 'Security Base APIs')
        resp = resp.json()
        self.capellaAPI.cluster_ops_apis.SECRET = resp['secretKey']
        self.capellaAPI.cluster_ops_apis.ACCESS = resp['id']
        self.capellaAPI.cluster_ops_apis.bearer_token = resp['token']
        self.capellaAPI.org_ops_apis.SECRET = resp['secretKey']
        self.capellaAPI.org_ops_apis.ACCESS = resp['id']
        self.capellaAPI.org_ops_apis.bearer_token = resp['token']

        self.capellaAPI.cluster_ops_apis.SECRETINI = resp['secretKey']
        self.capellaAPI.cluster_ops_apis.ACCESSINI = resp['id']
        self.capellaAPI.cluster_ops_apis.TOKENINI = resp['token']
        self.capellaAPI.org_ops_apis.SECRETINI = resp['secretKey']
        self.capellaAPI.org_ops_apis.ACCESSINI = resp['id']
        self.capellaAPI.org_ops_apis.TOKENINI = resp['token']

    def create_api_keys_for_different_roles(self):
        self.api_keys = []
        for user in self.test_users:
            if self.test_users[user]["role"] != "cloudManager":
                resp = self.capellaAPI.org_ops_apis.create_api_key(
                    self.tenant_id,
                    self.test_users[user]["role"],
                    organizationRoles=[self.test_users[user]["role"]],
                    expiry=1)

                if resp.status_code == 201:
                    self.log.info("Created API Keys for role {}".format(self.test_users[user]["role"]))
                    resp = resp.json()
                    self.test_users[user]["token"] = resp['token']
                    self.test_users[user]["id"] = resp["id"]

                    self.api_keys.append(resp["id"])

                else:
                    self.fail("Creating API Keys failed for role {}. Reason: {}".format(
                        self.test_users[user]["role"], resp.content))

    def delete_api_keys_for_different_roles(self):
        self.log.info("Deleting the api keys of different roles")
        for api_key in self.api_keys:
            self.log.info("Deleting api key: {}".format(api_key))
            resp = self.capellaAPI.org_ops_apis.delete_api_key(self.tenant_id,
                                                               api_key)
            if resp.status_code != 204:
                self.fail("Deleting API key failed. Reason: {} {}".format(
                    resp.json()["message"], resp.json()["hint"]))

        self.log.info("API Keys Deleted Successfully")

    def reset_api_keys(self):
        self.capellaAPI.cluster_ops_apis.SECRET = self.capellaAPI.cluster_ops_apis.SECRETINI
        self.capellaAPI.cluster_ops_apis.ACCESS = self.capellaAPI.cluster_ops_apis.ACCESSINI
        self.capellaAPI.cluster_ops_apis.bearer_token = self.capellaAPI.cluster_ops_apis.TOKENINI
        self.capellaAPI.org_ops_apis.SECRET = self.capellaAPI.org_ops_apis.SECRETINI
        self.capellaAPI.org_ops_apis.ACCESS = self.capellaAPI.org_ops_apis.ACCESSINI
        self.capellaAPI.org_ops_apis.bearer_token = self.capellaAPI.org_ops_apis.TOKENINI

    def create_different_organization_roles(self):
        self.log.info("Creating Different Organization Roles")
        self.test_users = {}
        roles = ["organizationOwner", "projectCreator", "organizationMember"]
        setup_capella_api = CapellaAPIv2("https://" + self.url, self.secret_key, self.access_key,
                                         self.user, self.passwd)

        num = 1
        for role in roles:
            _, domain = self.user.split('@')
            username = "couchbase-security+" + self.generate_random_string(9, False) \
                       + "@" + domain
            name = "Test_User_" + str(num)
            self.log.info("Creating user {} with role {}".format(username, role))
            create_user_resp = setup_capella_api.create_user(self.tenant_id,
                                                             name,
                                                             username,
                                                             "Password@123",
                                                             [role])
            self.log.info("User creation response - {}".format(create_user_resp.content))
            if create_user_resp.status_code == 200:
                self.log.info("User {} created successfully".format(username))
                self.test_users["User" + str(num)] = {
                    "name": create_user_resp.json()["data"]["name"],
                    "mailid": create_user_resp.json()["data"]["email"],
                    "role": role,
                    "password": "Password@123",
                    "userid": create_user_resp.json()["data"]["id"]
                }

            elif create_user_resp.status_code == 422:
                msg = "is already in use. Please sign-in."
                if msg in create_user_resp.json()["message"]:
                    self.log.info("User is already in use. Please sign-in")
                    num = num + 1
                    continue
                else:
                    self.fail("Not able to create user. Reason -".format(create_user_resp.content))

            else:
                self.fail("Not able to create user. Reason -".format(create_user_resp.content))

            num = num + 1

    def delete_different_organization_roles(self):
        self.log.info("Deleting different Organization Roles")
        for user in self.test_users:
            user_id = self.test_users[user]["userid"]
            self.log.info("Deleting user from organization. User Id: {}".format(user_id))

            resp = self.capellaAPI.org_ops_apis.delete_user(self.tenant_id,
                                                            user_id)

            if resp.status_code != 204:
                self.log.info("Failed to delete use with user id :{}.Reason {}".format(user_id, resp.text))
                raise Exception("Failed to delete user")

        self.log.info("Deleted all the Organization Roles successfully")

    @staticmethod
    def generate_random_string(length=9, special_characters=True,
                               prefix=""):
        """
        Generates random name of specified length
        """
        if special_characters:
            special_characters = "!@#$%^&*()-_=+{[]}\|;:'\",.<>/?" + " " + "\t"
        else:
            special_characters = ""

        characters = string.ascii_letters + string.digits + special_characters
        name = ""
        for i in range(length):
            name += random.choice(characters)

        if prefix:
            name = prefix + name

        return name

    def test_authentication(self, url, method='GET', payload=None, expected_status_codes=[401]):

        headers = {
            'invalid_header': 'abcded',
            'empty_header': ''
        }

        for header in headers:
            cbc_api_request_headers = {
                'Authorization': 'Bearer {}'.format(headers[header]),
                'Content-Type': 'application/json'
            }

            resp = self.capellaAPI._urllib_request(url, method=method, headers=cbc_api_request_headers,
                                                   params=json.dumps(payload))
            if resp.status_code not in expected_status_codes:
                error = "Test failed for invalid auth value: {}. " \
                        "Expected status code: {}, Returned status code: {}". \
                    format(cbc_api_request_headers["Authorization"],
                           expected_status_codes, resp.status_code)
                self.log.error(error)
                return False, error

        return True, None

    def test_tenant_ids(self, test_method=None, test_method_args=None, tenant_id_arg=None,
                        expected_success_code=None, on_success_callback=None, expected_failure_code=None):
        tenant_ids = {
            "valid_tenant_id": self.tenant_id,
            "invalid_tenant_id": self.invalid_id
        }

        if expected_success_code is None:
            expected_success_code = []
        elif isinstance(expected_success_code, int):
            expected_success_code = [expected_success_code]

        if expected_failure_code is None:
            expected_failure_code = [404]
        elif isinstance(expected_failure_code, int):
            expected_failure_code = [expected_failure_code]

        for tenant_id in tenant_ids:
            test_method_args[tenant_id_arg] = tenant_ids[tenant_id]
            resp = test_method(**test_method_args)

            if tenant_id == "invalid_tenant_id":
                if resp.status_code not in expected_failure_code:
                    error = "Test failed for invalid tenant id: {}. " \
                            "Expected status code: {}, Returned status code: {}". \
                        format(tenant_ids[tenant_id], 404, resp.status_code)
                    self.log.error(error)
                    return False, error
            else:
                if resp.status_code not in expected_success_code:
                    self.log.error(resp.content)
                    error = "Test failed for valid tenant id: {}. " \
                            "Expected status code: {}, Returned status code: {}". \
                        format(tenant_ids[tenant_id], expected_success_code,
                               resp.status_code)
                    self.log.error(error)
                    return False, error

                if on_success_callback:
                    on_success_callback(resp)

        return True, None

    def test_project_ids(self, test_method=None, test_method_args=None, project_id_arg=None,
                         expected_success_code=None, on_success_callback=None,
                         include_different_project=True):

        project_ids = {
            'valid_project_id': self.project_id,
            'invalid_project_id': self.invalid_id
        }

        if expected_success_code is None:
            expected_success_code = []
        elif isinstance(expected_success_code, int):
            expected_success_code = [expected_success_code]

        if include_different_project:
            resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id,
                                                               "security-test-project{}"
                                                               .format(random.randint(1, 100000)))
            if resp.status_code != 201:
                self.fail("Failed to create new project. Error: {}. Status code: {}".
                          format(resp.content, resp.status_code))
            project_id = resp.json()["id"]
            project_ids['different_project_id'] = project_id

        for project_id in project_ids:
            test_method_args[project_id_arg] = project_ids[project_id]
            resp = test_method(**test_method_args)

            if project_id == "invalid_project_id" or \
                    project_id == "different_project_id":

                if resp.status_code != 404:
                    error = "Test failed for invalid project id: {}. " \
                            "Expected status code: {}, Returned status code: {}". \
                        format(project_ids[project_id], 404, resp.status_code)
                    self.log.error(error)
                    return False, error
            else:
                if resp.status_code not in expected_success_code:
                    error = "Test failed for valid project id: {}. " \
                            "Expected status code: {}, Returned status code: {}". \
                        format(project_ids[project_id], expected_success_code,
                               resp.status_code)
                    self.log.error(error)
                    return False, error

                if on_success_callback:
                    on_success_callback(resp, test_method_args)

        if include_different_project:
            self.log.info("Deleting project")
            resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                               project_ids["different_project_id"])
            if resp.status_code != 204:
                self.fail("Failed to delete project. Error: {}. Status code: {}".
                          format(resp.content, resp.status_code))

        return True, None

    def test_with_org_roles(self, test_method_name=None, test_method_args=None,
                            expected_success_code=None, on_success_callback=None,
                            api_type="columnar"):

        if expected_success_code is None:
            expected_success_code = []
        elif isinstance(expected_success_code, int):
            expected_success_code = [expected_success_code]

        for user in self.test_users:
            self.log.info("Verifying status code for Role: {0}"
                          .format(self.test_users[user]["role"]))

            if api_type == "provisioned":
                capellaAPIrole = CapellaAPIv2("https://" + self.url, self.secret_key, self.access_key,
                                              self.test_users[user]["mailid"],
                                              self.test_users[user]["password"])
            elif api_type == "columnar":
                capellaAPIrole = ColumnarAPI("https://" + self.url, self.secret_key, self.access_key,
                                             self.test_users[user]["mailid"],
                                             self.test_users[user]["password"])

            if hasattr(capellaAPIrole, test_method_name):
                test_method = getattr(capellaAPIrole, test_method_name)
                resp = test_method(**test_method_args)

                if self.test_users[user]["role"] == "organizationOwner":
                    if resp.status_code not in expected_success_code:
                        error = "Test failed for role: {}. " \
                                "Expected status code: {}, Returned status code: {}". \
                            format(self.test_users[user]["role"], expected_success_code,
                                   resp.status_code)
                        self.log.error(error)
                        return False, error

                    if on_success_callback:
                        on_success_callback(resp, test_method_args)

                else:
                    if resp.status_code != 403:
                        error = "Test failed for role: {}. " \
                                "Expected status code: {}, Returned status code: {}". \
                            format(self.test_users[user]["role"], 403,
                                   resp.status_code)
                        self.log.error(error)
                        return False, error
            else:
                return False, "No method named: {}".format(test_method_name)

        return True, None

    def test_with_project_roles(self, test_method_name=None, test_method_args=None,
                                valid_project_roles=[], expected_success_code=None,
                                on_success_callback=None, api_type="columnar"):
        self.log.info("Verifying endpoint for different roles under project - RBAC")
        self.capellaAPIv2 = CapellaAPIv2("https://" + self.url, self.secret_key, self.access_key,
                                         self.user, self.passwd)
        project_roles = ["projectOwner", "projectClusterViewer", "projectClusterManager",
                         "projectDataWriter", "projectDataViewer"]
        user = self.test_users["User3"]

        for role in project_roles:
            self.log.info(
                "Adding user to project {} with role as {}".format(self.project_id, role))
            payload = {
                "resourceId": self.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }
            resp = self.capellaAPIv2.add_user_to_project(self.tenant_id, json.dumps(payload))
            if resp.status_code != 200:
                return False, "Failed to add user {} to project {}. Error: {}". \
                    format(user["name"], self.project_id, resp.content)

            if api_type == "provisioned":
                capellaAPIrole = CapellaAPIv2("https://" + self.url, self.secret_key, self.access_key,
                                              user["mailid"],
                                              user["password"])
            elif api_type == "columnar":
                capellaAPIrole = ColumnarAPI("https://" + self.url, self.secret_key, self.access_key,
                                             self.test_users[user]["mailid"],
                                             self.test_users[user]["password"])
            if hasattr(capellaAPIrole, test_method_name):
                test_method = getattr(capellaAPIrole, test_method_name)
                resp = test_method(**test_method_args)

                if role in valid_project_roles:
                    if resp.status_code != expected_success_code:
                        error = "Test failed for role: {}. " \
                                "Expected status code: {}, Returned status code: {}". \
                            format(role, expected_success_code,
                                   resp.status_code)
                        self.log.error(error)
                        return False, error

                    if on_success_callback:
                        on_success_callback(resp, test_method_args)

                else:
                    if resp.status_code != 403:
                        error = "Test failed for role: {}. " \
                                "Expected status code: {}, Returned status code: {}". \
                            format(role, 403,
                                   resp.status_code)
                        self.log.error(error)
                        return False, error
            else:
                return False, "No method named: {}".format(test_method_name)

            resp = self.capellaAPIv2.remove_user_from_project(self.tenant_id, user["userid"],
                                                              self.project_id)
            if resp.status_code != 204:
                return False, "Failed to remove user {} from project {}. Error: {}". \
                    format(user["name"], self.project_id, resp.content)

        return True, None
