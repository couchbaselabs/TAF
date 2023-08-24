import time
import json
import random
from pytests.basetestcase import BaseTestCase
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI

class SecurityTest(BaseTestCase):
    cidr = "10.0.0.0"

    def setUp(self):
        BaseTestCase.setUp(self)
        self.url = self.input.capella.get("pod")
        self.user = self.input.capella.get("capella_user")
        self.passwd = self.input.capella.get("capella_pwd")
        self.tenant_id = self.input.capella.get("tenant_id")
        self.project_id = self.tenant.project_id
        self.cluster_id = self.cluster.id
        self.invalid_id = "00000000-0000-0000-0000-000000000000"
        self.capellaAPI = CapellaAPI("https://" + self.url, '', '', self.user, self.passwd, '')
        resp = self.capellaAPI.create_control_plane_api_key(self.tenant_id, 'init api keys')
        resp = resp.json()
        self.capellaAPI.cluster_ops_apis.SECRET = resp['secretKey']
        self.capellaAPI.cluster_ops_apis.ACCESS = resp['accessKey']
        self.capellaAPI.cluster_ops_apis.bearer_token = resp['token']
        self.capellaAPI.org_ops_apis.SECRET = resp['secretKey']
        self.capellaAPI.org_ops_apis.ACCESS = resp['accessKey']
        self.capellaAPI.org_ops_apis.bearer_token = resp['token']

        self.capellaAPI.cluster_ops_apis.SECRETINI = resp['secretKey']
        self.capellaAPI.cluster_ops_apis.ACCESSINI = resp['accessKey']
        self.capellaAPI.cluster_ops_apis.TOKENINI = resp['token']
        self.capellaAPI.org_ops_apis.SECRETINI = resp['secretKey']
        self.capellaAPI.org_ops_apis.ACCESSINI = resp['accessKey']
        self.capellaAPI.org_ops_apis.TOKENINI = resp['token']

        if self.input.capella.get("test_users"):
            self.test_users = json.loads(json.loads(self.input.capella.get("test_users")))
        else:
            self.test_users = {"User1": {"password": self.passwd, "mailid": self.user,
                                         "role": "organizationOwner"}}

        for user in self.test_users:
            resp = self.capellaAPI.org_ops_apis.create_api_key(
                self.tenant_id, 'API Key for role {}'.format(
                self.test_users[user]["role"]), organizationRoles=[self.test_users[user]["role"]],
                expiry=1)
            resp = resp.json()
            self.test_users[user]["token"] = resp['token']

    def tearDown(self):
        super(SecurityTest, self).tearDown()

    def reset_api_keys(self):
        self.capellaAPI.cluster_ops_apis.SECRET = self.capellaAPI.cluster_ops_apis.SECRETINI
        self.capellaAPI.cluster_ops_apis.ACCESS = self.capellaAPI.cluster_ops_apis.ACCESSINI
        self.capellaAPI.cluster_ops_apis.bearer_token = self.capellaAPI.cluster_ops_apis.TOKENINI
        self.capellaAPI.org_ops_apis.SECRET = self.capellaAPI.org_ops_apis.SECRETINI
        self.capellaAPI.org_ops_apis.ACCESS = self.capellaAPI.org_ops_apis.ACCESSINI
        self.capellaAPI.org_ops_apis.bearer_token = self.capellaAPI.org_ops_apis.TOKENINI

    def generate_random_cidr(self):
        return '.'.join(
            str(random.randint(0, 255)) for _ in range(4)
        ) + '/23'

    @staticmethod
    def get_next_cidr():
        addr = SecurityTest.cidr.split(".")
        if int(addr[1]) < 255:
            addr[1] = str(int(addr[1]) + 1)
        elif int(addr[2]) < 255:
            addr[2] = str(int(addr[2]) + 1)
        SecurityTest.cidr = ".".join(addr)
        return SecurityTest.cidr

    def get_cluster_payload(self, cloud_provider):
        cluster_payloads = {
                "AWS": {
                    "name": "AWS-Test-Cluster-V4-Koushal-",
                    "description": "My first test aws cluster for multiple services.",
                    "cloudProvider": {
                        "type": "aws",
                        "region": "us-east-1",
                        "cidr": "10.7.22.0/23"
                    },
                    "couchbaseServer": {
                        "version": "7.1"
                    },
                    "serviceGroups": [
                        {
                            "node": {
                                "compute": {
                                    "cpu": 4,
                                    "ram": 16
                                },
                                "disk": {
                                    "storage": 50,
                                    "type": "gp3",
                                    "iops": 3000
                                }
                            },
                            "numOfNodes": 3,
                            "services": [
                                "data",
                                "query",
                                "index",
                                "search"
                            ]
                        },
                        {
                            "node": {
                                "compute": {
                                    "cpu": 4,
                                    "ram": 32
                                },
                                "disk": {
                                    "storage": 50,
                                    "type": "io2",
                                    "iops": 3005
                                }
                            },
                            "numOfNodes": 2,
                            "services": [
                                "analytics"
                            ]
                        }
                    ],
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
                        "version": "7.1"
                    },
                    "serviceGroups": [
                        {
                            "node": {
                                "compute": {
                                    "cpu": 4,
                                    "ram": 16
                                },
                                "disk": {
                                    "storage": 64,
                                    "type": "P6",
                                    "iops": 240
                                }
                            },
                            "numOfNodes": 3,
                            "services": [
                                "data",
                                "query",
                                "index",
                                "search"
                            ]
                        },
                        {
                            "node": {
                                "compute": {
                                    "cpu": 4,
                                    "ram": 32
                                },
                                "disk": {
                                    "storage": 64,
                                    "type": "P10",
                                    "iops": 240
                                }
                            },
                            "numOfNodes": 4,
                            "services": [
                                "analytics"
                            ]
                        }
                    ],
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
                        "version": "7.1"
                    },
                    "serviceGroups": [
                        {
                            "node": {
                                "compute": {
                                    "cpu": 4,
                                    "ram": 16
                                },
                                "disk": {
                                    "storage": 64,
                                    "type": "pd-ssd"
                                }
                            },
                            "numOfNodes": 3,
                            "services": [
                                "data",
                                "query",
                                "index",
                                "search"
                            ]
                        }
                    ],
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

    def deploy_clusters(self, num_clusters=1):
        self.log.info("Deploying clusters for the test")
        cluster_ids = []

        payload = self.get_cluster_payload("AWS")
        for num in range(num_clusters):
            self.log.info("Deploying cluster no. {} for the test".format(num))
            payload["name"] = payload["name"] + str(num)

            end_time = time.time() + 1800
            while time.time() < end_time:
                subnet = SecurityTest.get_next_cidr() + "/20"
                payload["cloudProvider"]["cidr"] = subnet
                self.log.info("Trying out with cidr {}".format(subnet))
                resp = self.capellaAPI.cluster_ops_apis.create_cluster(
                    self.tenant_id, self.project_id, payload["name"],
                    payload["cloudProvider"], payload["couchbaseServer"],
                    payload["serviceGroups"], payload["availability"], payload["support"])
                temp_resp = resp.json()

                if resp.status_code == 202:
                    cluster_ids.append(temp_resp["id"])
                    self.assertEqual(202, resp.status_code,
                                     msg='FAIL, Outcome: {}, Expected: {} Failed to delete the '
                                         'cluster'.format(resp.status_code, 202))
                    break

                elif "Please ensure you are passing a unique CIDR block and try again" \
                        in temp_resp["message"]:
                    continue
                else:
                    self.assertFalse(resp.status_code, "Failed to create a cluster with error "
                                                       "as {}".format(resp.content))

        for cluster_id in cluster_ids:
            status = self.get_cluster_status(cluster_id)
            self.assertEqual(status, "healthy",
                             msg="FAIL, Outcome: {}, Expected: {}".format(status, "healthy"))

        return cluster_ids

    def get_cluster_status(self, cluster_id):
        status = "Unknown"
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

    def test_create_cluster(self):
        self.log.info("Verify creating clusters for v4 APIs")

        self.log.info("Verifying the create cluster endpoint authentication with different test "
                      "cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        payload = self.get_cluster_payload("AWS")

        resp = self.capellaAPI.cluster_ops_apis.create_cluster(self.tenant_id,
                                                               self.project_id,
                                                               payload["name"],
                                                               payload["cloudProvider"],
                                                               payload["couchbaseServer"],
                                                               payload["serviceGroups"],
                                                               payload["availability"],
                                                               payload["support"])
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.create_cluster(self.tenant_id,
                                                               self.project_id,
                                                               payload["name"],
                                                               payload["cloudProvider"],
                                                               payload["couchbaseServer"],
                                                               payload["serviceGroups"],
                                                               payload["availability"],
                                                               payload["support"])
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.create_cluster(self.tenant_id,
                                                               self.project_id,
                                                               payload["name"],
                                                               payload["cloudProvider"],
                                                               payload["couchbaseServer"],
                                                               payload["serviceGroups"],
                                                               payload["availability"],
                                                               payload["support"])
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.create_cluster(self.tenant_id,
                                                               self.project_id,
                                                               payload["name"],
                                                               payload["cloudProvider"],
                                                               payload["couchbaseServer"],
                                                               payload["serviceGroups"],
                                                               payload["availability"],
                                                               payload["support"])
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        # Trying out with different organization ids
        self.log.info("Verify the endpoints with different organizations id")
        tenant_ids = {
            "valid_tenant_id": self.tenant_id,
            "invalid_tenant_id": self.invalid_id
        }
        for tenant_id in tenant_ids:
            payload = self.get_cluster_payload("AWS")

            if tenant_id == "valid_tenant_id":
                end_time = time.time() + 1800
                while time.time() < end_time:
                    subnet = SecurityTest.get_next_cidr() + "/20"
                    payload["cloudProvider"]["cidr"] = subnet
                    self.log.info("Trying out with cidr {}".format(subnet))
                    resp = self.capellaAPI.cluster_ops_apis.create_cluster(
                                                                    tenant_ids[tenant_id],
                                                                    self.project_id,
                                                                    payload["name"],
                                                                    payload["cloudProvider"],
                                                                    payload["couchbaseServer"],
                                                                    payload["serviceGroups"],
                                                                    payload["availability"],
                                                                    payload["support"])
                    temp_resp = resp.json()

                    if resp.status_code == 202:
                        cluster_id = temp_resp["id"]
                        resp = self.capellaAPI.cluster_ops_apis.delete_cluster(
                                                                            tenant_ids[tenant_id],
                                                                            self.project_id,
                                                                            cluster_id)
                        self.assertEqual(202, resp.status_code,
                                         msg='FAIL, Outcome: {}, Expected: {} Failed to delete the '
                                             'cluster'.format(resp.status_code, 202))
                        break

                    elif "Please ensure you are passing a unique CIDR block and try again" \
                    in temp_resp["message"]:
                        continue
                    else:
                        self.assertFalse(resp.status_code, "Failed to create a cluster with error "
                                                           "as {}".format(resp.content))

            else:
                resp = self.capellaAPI.cluster_ops_apis.create_cluster(
                                                                        tenant_ids[tenant_id],
                                                                        self.project_id,
                                                                        payload["name"],
                                                                        payload["cloudProvider"],
                                                                        payload["couchbaseServer"],
                                                                        payload["serviceGroups"],
                                                                        payload["availability"],
                                                                        payload["support"])
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Trying out with different project ids
        self.log.info("Verifying the endpoint access with different projects")
        self.log.info("Creating a project")
        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id,
                                                           "Clusters Project Koushal+1")
        self.assertEqual(201, resp.status_code,
                         msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                      201))
        resp = resp.json()

        project_ids = {
            'valid_project_id': self.project_id,
            'different_project_id': resp['id'],
            'invalid_project_id': self.invalid_id
        }

        payload = self.get_cluster_payload("GCP")

        for project_id in project_ids:

            if project_id == 'valid_project_id':
                end_time = time.time() + 1800
                while time.time() < end_time:
                    subnet = SecurityTest.get_next_cidr() + "/20"
                    payload["cloudProvider"]["cidr"] = subnet
                    self.log.info("Trying out with cidr {}".format(subnet))
                    resp = self.capellaAPI.cluster_ops_apis.create_cluster(
                                                                        self.tenant_id,
                                                                        project_ids[project_id],
                                                                        payload["name"],
                                                                        payload["cloudProvider"],
                                                                        payload["couchbaseServer"],
                                                                        payload["serviceGroups"],
                                                                        payload["availability"],
                                                                        payload["support"])
                    temp_resp = resp.json()

                    if resp.status_code == 202:
                        cluster_id = temp_resp["id"]
                        resp = self.capellaAPI.cluster_ops_apis.delete_cluster(
                                                                            self.tenant_id,
                                                                            project_ids[project_id],
                                                                            cluster_id)
                        self.assertEqual(202, resp.status_code,
                                         msg='FAIL, Outcome: {}, Expected: {} Failed to delete the '
                                             'cluster'.format(resp.status_code, 202))
                        break

                    elif "Please ensure you are passing a unique CIDR block and try again" \
                            in temp_resp["message"]:
                        continue
                    else:
                        self.assertFalse(resp.status_code, "Failed to create a cluster with error "
                                                           "as {}".format(resp.content))

            elif project_id == "invalid_project_id":
                resp = self.capellaAPI.cluster_ops_apis.create_cluster(self.tenant_id,
                                                                        project_ids[project_id],
                                                                        payload["name"],
                                                                        payload["cloudProvider"],
                                                                        payload["couchbaseServer"],
                                                                        payload["serviceGroups"],
                                                                        payload["availability"],
                                                                        payload["support"])
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        self.log.info("Deleting project")
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                           project_ids["different_project_id"])
        self.assertEqual(204, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 204))

        # Testing for Organizations RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"], self.test_users[user]['token'])

            if self.test_users[user]["role"] == "organizationOwner":
                end_time = time.time() + 1800
                while time.time() < end_time:
                    subnet = SecurityTest.get_next_cidr() + "/20"
                    payload["cloudProvider"]["cidr"] = subnet
                    self.log.info("Trying out with cidr {}".format(subnet))
                    role_response = self.capellaAPIRole.cluster_ops_apis.create_cluster(
                                                                        self.tenant_id,
                                                                        self.project_id,
                                                                        payload["name"],
                                                                        payload["cloudProvider"],
                                                                        payload["couchbaseServer"],
                                                                        payload["serviceGroups"],
                                                                        payload["availability"],
                                                                        payload["support"])
                    temp_resp = role_response.json()

                    if role_response.status_code == 202:
                        cluster_id = temp_resp["id"]
                        role_response = self.capellaAPIRole.cluster_ops_apis.delete_cluster(
                                                                                self.tenant_id,
                                                                                self.project_id,
                                                                                cluster_id)
                        self.assertEqual(202, role_response.status_code,
                                         msg='FAIL, Outcome: {}, Expected: {} Failed to delete the '
                                             'cluster'.format(role_response.status_code, 202))
                        break

                    elif "Please ensure you are passing a unique CIDR block and try again" \
                            in temp_resp["message"]:
                        continue

                    else:
                        self.assertFalse(role_response.status_code,
                            "Failed to create a cluster with error as {}".format(
                                role_response.content))
            else:
                role_response = self.capellaAPIRole.cluster_ops_apis.create_cluster(
                                                                        self.tenant_id,
                                                                        self.project_id,
                                                                        payload["name"],
                                                                        payload["cloudProvider"],
                                                                        payload["couchbaseServer"],
                                                                        payload["serviceGroups"],
                                                                        payload["availability"],
                                                                        payload["support"])
                self.assertEqual(role_response.status_code, 403,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 403))

        # Testing for Project Level RBAC roles
        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectOwner", "projectViewer", "projectManager",
                         "projectDataReaderWriter", "projectDataReader"]
        user = self.test_users["User3"]

        for role in project_roles:
            self.log.info("Creating apiKeys for role {}".format(role))
            resources = [
                {
                    "id": self.project_id,
                    "roles": [role]
                }
            ]
            resp = self.capellaAPI.org_ops_apis.create_api_key(
                                                           self.tenant_id,
                                                           'API Key for role {}'.format(
                                                           user["role"]),
                                                           organizationRoles=["organizationMember"],
                                                           expiry=1,
                                                           resources=resources)
            resp = resp.json()
            api_key_id = resp['id']
            user['token'] = resp['token']

            self.log.info(
                "Adding user to project {} with role as {}".format(self.project_id, role))
            dic = {"update_info": [{
                "op": "add",
                "path": "/resources/{}".format(self.project_id),
                "value": {
                    "id": self.project_id,
                    "type": "project",
                    "roles": [role]
                }
            }]
            }
            self.capellaAPI.org_ops_apis.update_user(self.tenant_id,
                                                     user['userid'],
                                                     dic["update_info"])

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', user["mailid"],
                                             user["password"], user['token'])

            if role == "projectOwner" or role == "projectManager":
                end_time = time.time() + 1800
                while time.time() < end_time:
                    subnet = SecurityTest.get_next_cidr() + "/20"
                    payload["cloudProvider"]["cidr"] = subnet
                    self.log.info("Trying out with cidr {}".format(subnet))
                    role_response = self.capellaAPIRole.cluster_ops_apis.create_cluster(
                                                                        self.tenant_id,
                                                                        self.project_id,
                                                                        payload["name"],
                                                                        payload["cloudProvider"],
                                                                        payload["couchbaseServer"],
                                                                        payload["serviceGroups"],
                                                                        payload["availability"],
                                                                        payload["support"])
                    temp_resp = role_response.json()

                    if role_response.status_code == 202:
                        cluster_id = temp_resp["id"]
                        response = self.capellaAPI.cluster_ops_apis.delete_cluster(
                                                                        self.tenant_id,
                                                                        self.project_id,
                                                                        cluster_id)
                        self.assertEqual(202, response.status_code,
                                         msg='FAIL, Outcome: {}, Expected: {} Failed to delete the '
                                             'cluster'.format(response.status_code, 202))
                        break

                    elif "Please ensure you are passing a unique CIDR block and try again" \
                            in temp_resp["message"]:
                        continue
                    else:
                        self.assertFalse(role_response.status_code,
                                         "Failed to create a cluster with error as {}".format(
                                             role_response.content))

            else:
                role_response = self.capellaAPIRole.cluster_ops_apis.create_cluster(
                                                                        self.tenant_id,
                                                                        self.project_id,
                                                                        payload["name"],
                                                                        payload["cloudProvider"],
                                                                        payload["couchbaseServer"],
                                                                        payload["serviceGroups"],
                                                                        payload["availability"],
                                                                        payload["support"])
                self.assertEqual(403, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(
                                     role_response.status_code, 403))
            self.log.info(
                "Removing user from project {} with role as {}".format(self.project_id,
                                                                       role))
            update_info = [{
                "op": "remove",
                "path": "/resources/{}".format(self.project_id)
            }]

            remove_proj_resp = self.capellaAPI.org_ops_apis.update_user(
                self.tenant_id, user['userid'], update_info)

            self.assertEqual(200, remove_proj_resp.status_code,
                             msg="FAIL: Outcome:{}, Expected: {}".format(
                                 remove_proj_resp.status_code, 200))

            resp = self.capellaAPI.org_ops_apis.delete_api_key(self.tenant_id, api_key_id)
            self.assertEqual(204, resp.status_code,
                             msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                          204))

    def test_list_clusters(self):
        self.log.info("Verify listing cluster for v4 APIs")

        self.log.info("Verifying the list cluster endpoint authentication with different test "
                      "cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.list_clusters(self.tenant_id, self.project_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.list_clusters(self.tenant_id, self.project_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.list_clusters(self.tenant_id, self.project_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.list_clusters(self.tenant_id, self.project_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        # Verify the endpoint with different organization ids
        self.log.info("Verify the endpoints with different organizations id")
        tenant_ids = {
            "valid_tenant_id": self.tenant_id,
            "invalid_tenant_id": self.invalid_id
        }
        for tenant_id in tenant_ids:
            resp = self.capellaAPI.cluster_ops_apis.list_clusters(tenant_ids[tenant_id],
                                                                  self.project_id)

            if tenant_id == "valid_tenant_id":
                self.assertEqual(200, resp.status_code,
                                 msg="FAIL, Outcome: {}, Expected: {}".format(resp.status_code,
                                                                              200))

            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Verify the endpoint with different project ids
        self.log.info("Verifying the endpoint access with different projects")

        self.log.info("Creating a project")
        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id,
                                                           "List Clusters Project "
                                                           "Koushal+1")
        self.assertEqual(201, resp.status_code,
                         msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                      201))

        resp = resp.json()
        project_ids = {
            'valid_project_id': self.project_id,
            'different_project_id': resp['id'],
            'invalid_project_id': self.invalid_id
        }
        for project_id in project_ids:
            resp = self.capellaAPI.cluster_ops_apis.list_clusters(self.tenant_id,
                                                                  project_ids[project_id])

            if project_id == "valid_project_id":
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(
                                     resp.status_code,
                                     200))
            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(
                                     resp.status_code,
                                     404))

        self.log.info("Deleting project")
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                           project_ids[
                                                               "different_project_id"])
        self.assertEqual(204, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code,
                                                                      204))

        # Testing for organization RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))
            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '',
                                             self.test_users[user]["mailid"],
                                             self.test_users[user]["password"],
                                             self.test_users[user]['token'])

            role_response = self.capellaAPIRole.cluster_ops_apis.list_clusters(self.tenant_id,
                                                                               self.project_id)

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(role_response.status_code, 200,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 200))
            else:
                self.assertEqual(role_response.status_code, 403,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 403))

        # Testing for Project Level RBAC roles
        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectOwner", "projectViewer", "projectManager",
                         "projectDataReaderWriter", "projectDataReader"]
        user = self.test_users["User3"]

        for role in project_roles:
            resources = [
                {
                    "id": self.project_id,
                    "roles": [role]
                }
            ]
            resp = self.capellaAPI.org_ops_apis.create_api_key(
                self.tenant_id, 'API Key for role {}'.format(
                    user["role"]), organizationRoles=["organizationMember"], expiry=1,
                resources=resources)
            resp = resp.json()
            api_key_id = resp['id']
            user['token'] = resp['token']

            self.log.info(
                "Adding user to project {} with role as {}".format(self.project_id, role))
            dic = {"update_info": [{
                "op": "add",
                "path": "/resources/{}".format(self.project_id),
                "value": {
                    "id": self.project_id,
                    "type": "project",
                    "roles": [role]
                }
            }]
            }
            self.capellaAPI.org_ops_apis.update_user(self.tenant_id,
                                                     user['userid'],
                                                     dic["update_info"])

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '',
                                             user["mailid"], user["password"], user['token'])

            role_response = self.capellaAPIRole.cluster_ops_apis.list_clusters(self.tenant_id,
                                                                               self.project_id)


            self.assertEqual(200, role_response.status_code,
                             msg="FAIL: Outcome:{}, Expected: {}".format(
                                 role_response.status_code, 200))

            self.log.info(
                "Removing user from project {} with role as {}".format(self.project_id,
                                                                       role))
            update_info = [{
                "op": "remove",
                "path": "/resources/{}".format(self.project_id)
            }]
            remove_proj_resp = self.capellaAPI.org_ops_apis.update_user(
                self.tenant_id, user['userid'], update_info)
            self.assertEqual(200, remove_proj_resp.status_code,
                             msg="FAIL: Outcome:{}, Expected: {}".format(
                                 remove_proj_resp.status_code, 200))

            resp = self.capellaAPI.org_ops_apis.delete_api_key(self.tenant_id, api_key_id)
            self.assertEqual(204, resp.status_code,
                             msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                          204))

    def test_fetch_cluster_info(self):
        self.log.info("Verify fetching a specific cluster details")

        self.log.info("Verifying the endpoint authentication with different test cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(self.tenant_id,
                                                                   self.project_id,
                                                                   self.cluster_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(self.tenant_id,
                                                                   self.project_id,
                                                                   self.cluster_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(self.tenant_id,
                                                                   self.project_id,
                                                                   self.cluster_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(self.tenant_id,
                                                                   self.project_id,
                                                                   self.cluster_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        # Verify the certificate endpoint with different organization ids
        self.log.info("Verifying with different tenant ids")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            'invalid_tenant_id': self.invalid_id
        }
        for tenant_id in tenant_ids:
            resp = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(tenant_ids[tenant_id],
                                                                            self.project_id,
                                                                            self.cluster_id)
            if tenant_id == 'valid_tenant_id':
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))
            else:
                # For now the response is 403. Later change it to 404.
                self.assertEqual(resp.status_code, 403,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Verify the endpoint with different projects
        self.log.info("Verifying the endpoint access with different projects")
        self.log.info("Creating a project")

        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id, "Allowed CIDR Project "
                                                                           "Koushal+1")

        self.assertEqual(201, resp.status_code,
                         msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 201))

        resp = resp.json()
        project_ids = {
            'valid_project_id': self.project_id,
            'different_project_id': resp['id'],
            'invalid_project_id': self.invalid_id
        }

        for project_id in project_ids:
            resp = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(self.tenant_id,
                                                                        project_ids[project_id],
                                                                        self.cluster_id)

            if project_id == 'valid_project_id':
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))
            else:
                self.assertEqual(resp.status_code, 422,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              422))
        self.log.info("Deleting project")
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                           project_ids["different_project_id"])

        self.assertEqual(204, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 204))

        # Testing for organization RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"], self.test_users[user]['token'])

            role_response = self.capellaAPIRole.cluster_ops_apis.fetch_cluster_info(
                                                                                self.tenant_id,
                                                                                self.project_id,
                                                                                self.cluster_id)
            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(role_response.status_code, 200,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 200))
            else:
                self.assertEqual(role_response.status_code, 403,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 403))

        # Testing for Project Level RBAC roles
        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectOwner", "projectViewer", "projectManager",
                         "projectDataReaderWriter", "projectDataReader"]
        user = self.test_users["User3"]

        for role in project_roles:
            resources = [
                {
                    "id": self.project_id,
                    "roles": [role]
                }
            ]
            resp = self.capellaAPI.org_ops_apis.create_api_key(
                self.tenant_id, 'API Key for role {}'.format(
                    user["role"]), organizationRoles=["organizationMember"], expiry=1,
                resources=resources)
            resp = resp.json()
            api_key_id = resp['id']
            user['token'] = resp['token']

            self.log.info("Adding user to project {} with role as {}".format(self.project_id, role))
            dic = {"update_info": [{
                "op": "add",
                "path": "/resources/{}".format(self.project_id),
                "value": {
                    "id": self.project_id,
                    "type": "project",
                    "roles": [role]
                }
            }]
            }
            self.capellaAPI.org_ops_apis.update_user(self.tenant_id,
                                                     user['userid'],
                                                     dic["update_info"])

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', user["mailid"],
                                             user["password"], user['token'])

            role_response = self.capellaAPIRole.cluster_ops_apis.fetch_cluster_info(
                                                                                self.tenant_id,
                                                                                self.project_id,
                                                                                self.cluster_id)

            self.assertEqual(200, role_response.status_code,
                             msg="FAIL: Outcome:{}, Expected: {}".format(
                                 role_response.status_code, 200))

            self.log.info("Removing user from project {} with role as {}".format(self.project_id,
                                                                                 role))
            update_info = [{
                "op": "remove",
                "path": "/resources/{}".format(self.project_id)
            }]
            remove_proj_resp = self.capellaAPI.org_ops_apis.update_user(self.tenant_id,
                                                                        user['userid'],
                                                                        update_info)
            self.assertEqual(200, remove_proj_resp.status_code,
                             msg="FAIL: Outcome:{}, Expected: {}".format(
                                 remove_proj_resp.status_code, 200))

            resp = self.capellaAPI.org_ops_apis.delete_api_key(self.tenant_id, api_key_id)
            self.assertEqual(204, resp.status_code,
                             msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 204))

    def test_update_cluster(self):
        self.log.info("Verify updating a particular cluster")

        self.test_cluster_ids = self.deploy_clusters(5)

        update_payload = {
          "name": "AWS-Test-Cluster-V4-Koushal",
          "description": "Testing update cluster with v4 apis",
          "availability": {
            "type": "multi"
          },
          "support": {
            "plan": "developer pro",
            "timezone": "PT"
          },
          "serviceGroups": [
            {
              "node": {
                "compute": {
                  "cpu": 4,
                  "ram": 32
                },
                "disk": {
                  "type": "gp3",
                  "storage": 55,
                  "iops": 3000
                }
              },
              "numOfNodes": 3,
              "services": [
                  "data",
                  "query",
                  "index",
                  "search"
              ]
            }
          ]
        }

        self.log.info("Verifying the endpoint authentication with different test cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.update_cluster(self.tenant_id,
                                                               self.project_id,
                                                               self.test_cluster_ids[0],
                                                               update_payload["name"],
                                                               update_payload["description"],
                                                               update_payload["support"],
                                                               update_payload["serviceGroups"],
                                                               False)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.update_cluster(self.tenant_id,
                                                               self.project_id,
                                                               self.test_cluster_ids[0],
                                                               update_payload["name"],
                                                               update_payload["description"],
                                                               update_payload["support"],
                                                               update_payload["serviceGroups"],
                                                               False)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.update_cluster(self.tenant_id,
                                                               self.project_id,
                                                               self.test_cluster_ids[0],
                                                               update_payload["name"],
                                                               update_payload["description"],
                                                               update_payload["support"],
                                                               update_payload["serviceGroups"],
                                                               False)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.update_cluster(self.tenant_id,
                                                               self.project_id,
                                                               self.test_cluster_ids[0],
                                                               update_payload["name"],
                                                               update_payload["description"],
                                                               update_payload["support"],
                                                               update_payload["serviceGroups"],
                                                               False)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        # Verify endpoint for different organizations id
        self.log.info("Verify the update cluster v4 api for different organization ids")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            'invalid_tenant_id': self.invalid_id
        }
        for tenant_id in tenant_ids:
            resp = self.capellaAPI.cluster_ops_apis.update_cluster(tenant_ids[tenant_id],
                                                                   self.project_id,
                                                                   self.test_cluster_ids[0],
                                                                   update_payload["name"],
                                                                   update_payload["description"],
                                                                   update_payload["support"],
                                                                   update_payload["serviceGroups"],
                                                                   False)
            if tenant_id == 'valid_tenant_id':
                self.assertEqual(resp.status_code, 204,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              204))

            else:
                # For now the response is 403. Later change it to 404.
                self.assertEqual(resp.status_code, 403,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Verify the endpoint with different projects
        self.log.info("Verifying the endpoint access with different projects")
        self.log.info("Creating a project")

        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id,
                                                           "Allowed CIDR Project "
                                                           "Koushal+1")

        self.assertEqual(201, resp.status_code,
                         msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 201))

        resp = resp.json()
        project_ids = {
            'valid_project_id': self.project_id,
            'different_project_id': resp['id'],
            'invalid_project_id': self.invalid_id
        }

        for project_id in project_ids:
            resp = self.capellaAPI.cluster_ops_apis.update_cluster(self.tenant_id,
                                                                   project_ids[project_id],
                                                                   self.test_cluster_ids[1],
                                                                   update_payload["name"],
                                                                   update_payload["description"],
                                                                   update_payload["support"],
                                                                   update_payload["serviceGroups"],
                                                                   False)

            if project_id == 'valid_project_id':
                self.assertEqual(resp.status_code, 204,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(
                                     resp.status_code, 204))
            else:
                self.assertEqual(resp.status_code, 422,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(
                                     resp.status_code, 422))

        self.log.info("Deleting project")
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                           project_ids["different_project_id"])

        self.assertEqual(204, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 204))

        # Testing for organization RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"], self.test_users[user]['token'])

            role_response = self.capellaAPIRole.cluster_ops_apis.update_cluster(self.tenant_id,
                                                                   self.project_id,
                                                                   self.test_cluster_ids[2],
                                                                   update_payload["name"],
                                                                   update_payload["description"],
                                                                   update_payload["support"],
                                                                   update_payload["serviceGroups"],
                                                                   False)

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(role_response.status_code, 204,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 204))
            else:
                self.assertEqual(role_response.status_code, 403,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 403))

        # Testing for Project Level RBAC roles
        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectOwner", "projectViewer", "projectManager",
                         "projectDataReaderWriter", "projectDataReader"]
        user = self.test_users["User3"]

        for role in project_roles:
            resources = [
                {
                    "id": self.project_id,
                    "roles": [role]
                }
            ]
            resp = self.capellaAPI.org_ops_apis.create_api_key(
                self.tenant_id, 'API Key for role {}'.format(
                    user["role"]), organizationRoles=["organizationMember"], expiry=1,
                resources=resources)
            resp = resp.json()
            api_key_id = resp['id']
            user['token'] = resp['token']

            self.log.info(
                "Adding user to project {} with role as {}".format(self.project_id, role))
            dic = {"update_info": [{
                "op": "add",
                "path": "/resources/{}".format(self.project_id),
                "value": {
                    "id": self.project_id,
                    "type": "project",
                    "roles": [role]
                }
            }]
            }
            self.capellaAPI.org_ops_apis.update_user(self.tenant_id,
                                                     user['userid'],
                                                     dic["update_info"])

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', user["mailid"],
                                             user["password"], user['token'])

            if role == "projectOwner":
                role_response = self.capellaAPIRole.cluster_ops_apis.update_cluster(
                                                                    self.tenant_id,
                                                                    self.project_id,
                                                                    self.test_cluster_ids[3],
                                                                    update_payload["name"],
                                                                    update_payload["description"],
                                                                    update_payload["support"],
                                                                    update_payload["serviceGroups"],
                                                                    False)
                self.assertEqual(204, role_response.status_code,
                             msg="FAIL: Outcome:{}, Expected: {}".format(
                                 role_response.status_code, 204))

            elif role == "projectManager":
                role_response = self.capellaAPIRole.cluster_ops_apis.update_cluster(
                                                                    self.tenant_id,
                                                                    self.project_id,
                                                                    self.test_cluster_ids[4],
                                                                    update_payload["name"],
                                                                    update_payload["description"],
                                                                    update_payload["support"],
                                                                    update_payload["serviceGroups"],
                                                                    False)
                self.assertEqual(204, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(
                                     role_response.status_code, 204))

            else:
                role_response = self.capellaAPIRole.cluster_ops_apis.update_cluster(
                                                                    self.tenant_id,
                                                                    self.project_id,
                                                                    self.test_cluster_ids[0],
                                                                    update_payload["name"],
                                                                    update_payload["description"],
                                                                    update_payload["support"],
                                                                    update_payload["serviceGroups"],
                                                                    False)
                self.assertEqual(403, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(
                                     role_response.status_code,403))

            self.log.info(
                "Removing user from project {} with role as {}".format(self.project_id,
                                                                       role))
            update_info = [{
                "op": "remove",
                "path": "/resources/{}".format(self.project_id)
            }]
            remove_proj_resp = self.capellaAPI.org_ops_apis.update_user(self.tenant_id,
                                                                        user['userid'],
                                                                        update_info)
            self.assertEqual(200, remove_proj_resp.status_code,
                             msg="FAIL: Outcome:{}, Expected: {}".format(
                                 remove_proj_resp.status_code, 200))

            resp = self.capellaAPI.org_ops_apis.delete_api_key(self.tenant_id, api_key_id)
            self.assertEqual(204, resp.status_code,
                             msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                          204))

        # Deleting the cluster created for this test
        self.log.info("Deleting the clusters created for the test")
        for cluster_id in self.test_cluster_ids:
            resp = self.capellaAPI.cluster_ops_apis.delete_cluster(self.tenant_id,
                                                                   self.project_id,
                                                                   cluster_id)
            self.assertEqual(202, resp.status_code,
                             msg='FAIL, Outcome: {}, Expected: {} Failed to delete the '
                                 'cluster'.format(resp.status_code, 202))

    def test_delete_cluster(self):
        self.log.info("Verify delete cluster v4 API")

        # Deploying a cluster
        self.log.info("Deploying a cluster")
        payload = self.get_cluster_payload("AWS")
        self.test_cluster_id = ""

        end_time = time.time() + 1800
        while time.time() < end_time:
            subnet = SecurityTest.get_next_cidr() + "/20"
            payload["cloudProvider"]["cidr"] = subnet
            self.log.info("Trying out with cidr {}".format(subnet))
            resp = self.capellaAPI.cluster_ops_apis.create_cluster(self.tenant_id,
                                                                   self.project_id,
                                                                   payload["name"],
                                                                   payload["cloudProvider"],
                                                                   payload["couchbaseServer"],
                                                                   payload["serviceGroups"],
                                                                   payload["availability"],
                                                                   payload["support"])
            temp_resp = resp.json()

            if resp.status_code == 202:
                self.test_cluster_id = temp_resp["id"]
                break

            elif "Please ensure you are passing a unique CIDR block and try again" \
                    in temp_resp["message"]:
                continue

            else:
                self.assertFalse(resp.status_code, "Failed to create a cluster with error "
                                                   "as {}".format(resp.content))
        status = "deploying"
        while status != 'healthy':
            self.sleep(15, "Waiting for cluster to be in healthy state. Current status - {}"
                       .format(status))
            cluster_ready_resp = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                self.tenant_id,
                self.project_id,
                self.test_cluster_id)
            cluster_ready_resp = cluster_ready_resp.json()
            status = cluster_ready_resp["currentState"]

        self.log.info("Verifying the endpoint authentication with different test cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.delete_cluster(self.tenant_id,
                                                               self.project_id,
                                                               self.test_cluster_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.delete_cluster(self.tenant_id,
                                                               self.project_id,
                                                               self.test_cluster_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.delete_cluster(self.tenant_id,
                                                               self.project_id,
                                                               self.test_cluster_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.delete_cluster(self.tenant_id,
                                                               self.project_id,
                                                               self.test_cluster_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        # Verify endpoint for different organizations id
        self.log.info("Verify the update cluster v4 api for different organization ids")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            'invalid_tenant_id': self.invalid_id
        }
        for tenant_id in tenant_ids:
            resp = self.capellaAPI.cluster_ops_apis.delete_cluster(tenant_ids[tenant_id],
                                                                   self.project_id,
                                                                   self.test_cluster_id)
            if tenant_id == 'valid_tenant_id':
                self.assertEqual(resp.status_code, 202,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              202))

                end_time = time.time() + 1800
                while time.time() < end_time:
                    subnet = SecurityTest.get_next_cidr() + "/20"
                    payload["cloudProvider"]["cidr"] = subnet
                    self.log.info("Trying out with cidr {}".format(subnet))
                    resp = self.capellaAPI.cluster_ops_apis.create_cluster(
                                                                   self.tenant_id,
                                                                   self.project_id,
                                                                   payload["name"],
                                                                   payload["cloudProvider"],
                                                                   payload["couchbaseServer"],
                                                                   payload["serviceGroups"],
                                                                   payload["availability"],
                                                                   payload["support"])
                    temp_resp = resp.json()

                    if resp.status_code == 202:
                        self.test_cluster_id = temp_resp["id"]
                        break

                    elif "Please ensure you are passing a unique CIDR block and try again" \
                            in temp_resp["message"]:
                        continue

                    else:
                        self.assertFalse(resp.status_code, "Failed to create a cluster with error "
                                                           "as {}".format(resp.content))

            else:
                # For now the response is 403. Later change it to 404.
                self.assertEqual(resp.status_code, 403,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        status = "deploying"
        while status != 'healthy':
            self.sleep(15, "Waiting for cluster to be in healthy state. Current status - {}"
                       .format(status))
            cluster_ready_resp = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                                                                            self.tenant_id,
                                                                            self.project_id,
                                                                            self.test_cluster_id)
            cluster_ready_resp = cluster_ready_resp.json()
            status = cluster_ready_resp["currentState"]

        # Verify the endpoint with different projects
        self.log.info("Verifying the endpoint access with different projects")
        self.log.info("Creating a project")

        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id,
                                                           "Delete Cluster Koushal+1")

        self.assertEqual(201, resp.status_code,
                         msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 201))

        resp = resp.json()
        project_ids = {
            'valid_project_id': self.project_id,
            'different_project_id': resp['id'],
            'invalid_project_id': self.invalid_id
        }

        for project_id in project_ids:
            resp = self.capellaAPI.cluster_ops_apis.delete_cluster(self.tenant_id,
                                                                   project_ids[project_id],
                                                                   self.test_cluster_id)

            if project_id == 'valid_project_id':
                self.assertEqual(resp.status_code, 202,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              202))

                end_time = time.time() + 1800
                while time.time() < end_time:
                    subnet = SecurityTest.get_next_cidr() + "/20"
                    payload["cloudProvider"]["cidr"] = subnet
                    self.log.info("Trying out with cidr {}".format(subnet))
                    resp = self.capellaAPI.cluster_ops_apis.create_cluster(
                                                                        self.tenant_id,
                                                                        self.project_id,
                                                                        payload["name"],
                                                                        payload["cloudProvider"],
                                                                        payload["couchbaseServer"],
                                                                        payload["serviceGroups"],
                                                                        payload["availability"],
                                                                        payload["support"])
                    temp_resp = resp.json()

                    if resp.status_code == 202:
                        self.test_cluster_id = temp_resp["id"]
                        break

                    elif "Please ensure you are passing a unique CIDR block and try again" \
                            in temp_resp["message"]:
                        continue

                    else:
                        self.assertFalse(resp.status_code, "Failed to create a cluster with error "
                                                           "as {}".format(resp.content))

            else:
                self.assertEqual(resp.status_code, 422,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              422))
        self.log.info("Deleting project")
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                           project_ids["different_project_id"])

        self.assertEqual(204, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 204))

        # Testing for organization RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"], self.test_users[user]['token'])

            role_response = self.capellaAPIRole.cluster_ops_apis.delete_cluster(
                                                                            self.tenant_id,
                                                                            self.project_id,
                                                                            self.test_cluster_id)

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(role_response.status_code, 202,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 202))

                end_time = time.time() + 1800
                while time.time() < end_time:
                    subnet = SecurityTest.get_next_cidr() + "/20"
                    payload["cloudProvider"]["cidr"] = subnet
                    self.log.info("Trying out with cidr {}".format(subnet))
                    resp = self.capellaAPI.cluster_ops_apis.create_cluster(
                                                                        self.tenant_id,
                                                                        self.project_id,
                                                                        payload["name"],
                                                                        payload["cloudProvider"],
                                                                        payload["couchbaseServer"],
                                                                        payload["serviceGroups"],
                                                                        payload["availability"],
                                                                        payload["support"])
                    temp_resp = resp.json()

                    if resp.status_code == 202:
                        self.test_cluster_id = temp_resp["id"]
                        break

                    elif "Please ensure you are passing a unique CIDR block and try again" \
                            in temp_resp["message"]:
                        continue

                    else:
                        self.assertFalse(resp.status_code, "Failed to create a cluster with error "
                                                           "as {}".format(resp.content))

            else:
                self.assertEqual(role_response.status_code, 403,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 403))

        # Testing for Project Level RBAC roles
        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectOwner", "projectViewer", "projectManager",
                         "projectDataReaderWriter", "projectDataReader"]
        user = self.test_users["User3"]

        for role in project_roles:
            resources = [
                {
                    "id": self.project_id,
                    "roles": [role]
                }
            ]
            resp = self.capellaAPI.org_ops_apis.create_api_key(
                                                        self.tenant_id,
                                                        'API Key for role {}'.format(user["role"]),
                                                        organizationRoles=["organizationMember"],
                                                        expiry=1,
                                                        resources=resources)
            resp = resp.json()
            api_key_id = resp['id']
            user['token'] = resp['token']

            self.log.info("Adding user to project {} with role as {}".format(self.project_id, role))
            dic = {"update_info": [{
                "op": "add",
                "path": "/resources/{}".format(self.project_id),
                "value": {
                    "id": self.project_id,
                    "type": "project",
                    "roles": [role]
                }
            }]
            }
            self.capellaAPI.org_ops_apis.update_user(self.tenant_id,
                                                     user['userid'],
                                                     dic["update_info"])

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', user["mailid"],
                                             user["password"], user['token'])

            role_response = self.capellaAPIRole.cluster_ops_apis.delete_cluster(
                                                                            self.tenant_id,
                                                                            self.project_id,
                                                                            self.test_cluster_id)

            if role in ["projectOwner", "projectManager"]:
                self.assertEqual(202, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(
                                 role_response.status_code, 202))
            else:
                self.assertEqual(403, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(
                                     role_response.status_code,403))

            self.log.info("Removing user from project {} with role as {}".format(self.project_id,
                                                                                 role))
            update_info = [{
                "op": "remove",
                "path": "/resources/{}".format(self.project_id)
            }]
            remove_proj_resp = self.capellaAPI.org_ops_apis.update_user(self.tenant_id,
                                                                        user['userid'],
                                                                        update_info)
            self.assertEqual(200, remove_proj_resp.status_code,
                             msg="FAIL: Outcome:{}, Expected: {}".format(
                                 remove_proj_resp.status_code, 200))

            resp = self.capellaAPI.org_ops_apis.delete_api_key(self.tenant_id, api_key_id)
            self.assertEqual(204, resp.status_code,
                             msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 204))