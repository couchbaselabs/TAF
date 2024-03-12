import json
import time
import string
import random
# import itertools
# import base64
# from datetime import datetime
from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI as CapellaAPIv2
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
        self.server_version = self.input.capella.get("server_version", "7.2")
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
        self.create_initial_v4_api_keys()
        self.create_different_organization_roles()
        self.create_api_keys_for_different_roles()
        self.project_id = self.input.capella.get("project_id", None)
        if self.project_id is None:
            self.create_project(self.prefix + "Project")

        self.cluster_id = self.input.capella.get("cluster_id", None)
        if self.cluster_id is None:
            self.create_cluster(self.prefix + "Cluster", self.server_version)
        if self.cluster_id is not None:
            self.allow_ip(self.cluster_id, self.project_id)

        self.log.info("-------Setup finished for CouchbaseBaseTest-------")

    def tearDown(self):
        self.log.info("-------Teardown started for SecurityBase-------")

        if self.input.capella.get("cluster_id") is None:
            self.delete_cluster()

        if self.input.capella.get("project_id") is None:
            self.delete_project()

        self.delete_api_keys_for_different_roles()
        self.delete_different_organization_roles()

        self.log.info("-------Teardown finished for SecurityBase-------")

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

    def create_cluster(self, cluster_name, server_version, provider="AWS"):
        num_clusters = TestInputSingleton.input.param("num_clusters", 1)
        for _ in range(0, num_clusters):
            self.log.info("Creating Cluster for Security Test")
            payload = self.get_cluster_payload(provider)
            payload["name"] = cluster_name
            payload["couchbaseServer"]["version"] = server_version

            end_time = time.time() + 1800
            while time.time() < end_time:
                subnet = self.get_next_cidr() + "/20"
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
                    if release_id:
                        ami_payload["overRide"]["releaseId"] = release_id

                    resp = self.capellaAPI.create_cluster_customAMI(self.tenant_id,
                                                                    ami_payload)
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

                elif "Please ensure you are passing a unique CIDR block and try again" \
                        in resp.json()["message"]:
                    continue

                else:
                    self.assertFalse(resp.status_code, "Failed to create a cluster with error "
                                                       "as {}".format(resp.content))

            status = self.get_cluster_status(self.cluster_id)
            self.assertEqual(status, "healthy",
                             msg="FAIL, Outcome: {}, Expected: {}".format(status, "healthy"))

            self.log.info("Cluster creation is successful. Cluster ID: {}".format(self.cluster_id))

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
                        "plan": "developer pro",
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

        if cloud_provider == "AWS":
            return cluster_payloads["AWS"]
        elif cloud_provider == "Azure":
            return cluster_payloads["Azure"]
        elif cloud_provider == "GCP":
            return cluster_payloads["GCP"]

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
            if self.test_users[user]["role"] == "cloudManager":
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
            user_len = len(self.user)
            gen_len = 64 - user_len
            gen_len = min(gen_len, 9)
            usrname = self.user.split('@')
            username = usrname[0] + "+" + self.generate_random_string(gen_len, False) + "@" + \
                       usrname[1]
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
    def generate_random_string(length=10, special_characters=True,
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