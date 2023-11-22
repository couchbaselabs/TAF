import base64
import json
import random
import socket
import time

import requests
from goldfish_base import GoldFishBaseTest
from goldfishAPI.GoldfishAPIs.ServerlessAnalytics.ServerlessAnalytics import GoldfishAPI
from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI
from uuid import uuid4

class SecurityTest(GoldFishBaseTest):

    def setUp(self):
        GoldFishBaseTest.setUp(self)
        self.url = self.input.capella.get("pod")
        self.user = self.input.capella.get("capella_user")
        self.passwd = self.input.capella.get("capella_pwd")
        self.tenant_id = self.input.capella.get("tenant_id")
        self.project_id = self.users[0].projects[0].project_id
        #self.cluster_id = self.cluster.id
        self.invalid_id = "00000000-0000-0000-0000-000000000000"
        self.CBAS_PORT = 18001
        self.KV_PORT = 16001
        self.SVC_MGR_PORT = 18500

        self.goldfishAPI = GoldfishAPI("https://" + self.url, "", "", self.user, self.passwd, "")
        self.capellaAPI = CapellaAPI("https://" + self.url, self.input.capella.get("secret_key"),
                                    self.input.capella.get("access_key"), self.user,
                                    self.passwd)

        #list of clusters created during tests
        self.cluster_list = []

        self.test_users = []
        self.create_users_for_tests(self.user)

    def tearDown(self):

        #delete the users created
        self.log.info("Removing the users created")
        for user in self.test_users:
            user_id = user["userid"]
            resp = self.capellaAPI.remove_user(self.tenant_id, user_id)
            self.log.info("Status code, remove user {}".format(resp.status_code))

        #delete clusters that failed to get deleted in the test
        for cluster_id in self.cluster_list:
            resp = self.goldfishAPI.delete_goldfish_instance(self.tenant_id,
                                                             self.project_id,
                                                             cluster_id)
            self.assertEqual(204, resp.status_code,
                             "Failed to delete instance: {}".format(cluster_id))
            self.wait_for_cluster_to_be_deleted(cluster_id)


    def create_users_for_tests(self, email):
        organization_roles = ["organizationOwner", "organizationMember", "projectCreator"]

        for org_role in organization_roles:
            uuid = uuid4()
            a,b = email.split("@")
            self.capellaAPI.create_user
            seed_email = "{}+{}@{}".format(a, uuid, b)
            name = "{}".format(uuid)
            password = str(uuid4()) + "!1Aa"

            resp = self.capellaAPI.create_user(self.tenant_id, name, seed_email, password,
                                               [org_role])
            self.log.info("Status code, add user {}".format(resp.status_code))
            if resp.status_code != 200:
                self.fail("Failed to create user")
            resp = json.loads(resp.content)
            user_id = resp["data"]["id"]
            user = {
                "name": name,
                "role": org_role,
                "password": password,
                "mailid": seed_email,
                "userid": user_id
            }
            self.test_users.append(user)

        self.log.info("Test users: {}".format(self.test_users))


    def wait_for_cluster_to_be_healty(self, cluster_id):
        end_time = time.time() + 1800

        state = None
        while time.time() < end_time:
            resp = self.goldfishAPI.get_specific_goldfish_instance(self.tenant_id,
                                                                   self.project_id,
                                                                   cluster_id)
            self.assertEqual(200, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 202))

            resp = resp.json()
            state = resp["state"]

            if state == "healthy":
                break
            elif state == "deploying":
                self.sleep(10, "Wait for cluster to be healthy")
            else:
                self.fail("Goldfish cluster deployment failed")

        if state == "deploying":
            self.fail("Cluster is still deploying after timeout")

    def wait_for_cluster_to_be_deleted(self, cluster_id):
        end_time = time.time() + 1800

        state = None
        while time.time() < end_time:
            resp = self.goldfishAPI.get_specific_goldfish_instance(self.tenant_id,
                                                                   self.project_id,
                                                                   cluster_id)
            if resp.status_code == 404:
                state = "desotryed"
                break

            resp = resp.json()
            state = resp["state"]

            if state == "destroying":
                self.sleep(10, "Wait for cluster to be destroyed")
            else:
                self.fail("Goldfish cluster deployment failed")

        if state == "destroying":
            self.fail("Cluster is still destroying after timeout")

    def is_port_open(self, host, port):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)  # Set a timeout for the connection attempt
            s.connect((host, port))
            s.close()  # Close the socket after the connection
            return True
        except (socket.timeout, socket.error):
            return False

    def generate_random_port(self):
        port = random.randint(1024, 65535)
        while port == self.CBAS_PORT or port == self.KV_PORT or port == self.SVC_MGR_PORT:
            port = random.randint(1024, 65535)

        return port

    def generate_database_credentials(self, cluster):
        resp = self.goldfishAPI.create_api_keys(self.tenant_id, self.project_id,
                                                cluster.cluster_id)
        resp = resp.json()

        return resp["apikeyId"], resp["secret"]

    def _create_headers(self, username=None, password=None, connection='close'):
        authorization = base64.b64encode('{}:{}'.format(username, password).encode()).decode()
        return {'Authorization': 'Basic %s' % authorization,
                'Connection': connection,
                'Accept': '*/*'}

    def send_request_to_nebula(self, host, port, endpoint, username, password):

        url = "https://" + host + ":" +  str(port) + endpoint
        print("URL: {}".format(url))

        session = requests.Session()
        headers = self._create_headers(username, password)
        print("headers: {}".format(headers))
        resp = session.get(url, headers=headers,
                            timeout=60, verify=False)
        return resp

    def validate_error_message(self, expected_err_msg, actual_err_msg):

        if expected_err_msg == actual_err_msg:
            return True

        return False

    def test_create_goldfish_instance_security(self):

        headers = {
            "invalid_header": 'abcdefgh',
            "empty_header": ''
        }
        for header in headers:
            cbc_api_request_headers = {
                    'Authorization': 'Bearer %s' % headers[header],
                    'Content-Type': 'application/json'
            }
            resp = self.goldfishAPI.create_goldfish_instance(self.tenant_id, self.project_id,
                                                             "test-db", "desc", "aws", "us-east-1", 3,
                                                             headers=cbc_api_request_headers,
                                                             skip_jwt_retry=True)
            self.log.info("Response: {}".format(resp))
            self.assertEqual(401, resp.status_code,
                            msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))

        tenant_ids = {
            "valid_tenant_id": self.tenant_id,
            "invalid_tenant_id": self.invalid_id
        }

        for tenant_id in tenant_ids:
            resp = self.goldfishAPI.create_goldfish_instance(tenant_ids[tenant_id], self.project_id,
                                                             "test-db", "desc", "aws", "us-east-1", 3)
            if tenant_id == "invalid_tenant_id":
                self.assertEqual(404, resp.status_code,
                                msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 404))
            else:
                self.assertEqual(201, resp.status_code,
                                msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 201))
                #wait for cluster to be healty
                resp = json.loads(resp.content)
                cluster_id = resp["id"]
                self.wait_for_cluster_to_be_healty(cluster_id)
                self.cluster_list.append(cluster_id)
                #delete the cluster
                resp = self.goldfishAPI.delete_goldfish_instance(self.tenant_id, self.project_id,
                                                                cluster_id)
                self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 204))
                self.wait_for_cluster_to_be_deleted(cluster_id)
                self.cluster_list.remove(cluster_id)

        project_ids = {
            'valid_project_id': self.project_id,
            'invalid_project_id': self.invalid_id
        }

        for project_id in project_ids:
            resp = self.goldfishAPI.create_goldfish_instance(self.tenant_id, project_ids[project_id],
                                                             "test-db", "desc", "aws", "us-east-1", 3)
            if project_id == "valid_project_id":
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 201))
                #wait for cluster to be healty
                resp = json.loads(resp.content)
                cluster_id = resp["id"]
                self.wait_for_cluster_to_be_healty(cluster_id)
                self.cluster_list.append(cluster_id)
                #delete the cluster
                resp = self.goldfishAPI.delete_goldfish_instance(self.tenant_id, self.project_id,
                                                                cluster_id)
                self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 204))
                self.wait_for_cluster_to_be_deleted(cluster_id)
                self.cluster_list.remove(cluster_id)
            else:
                self.assertEqual(404, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 404))


        for user in self.test_users:
            self.goldfishAPIrole = GoldfishAPI("https://" + self.url, "", "",
                                               user["mailid"],
                                               user["password"])

            resp = self.goldfishAPIrole.create_goldfish_instance(self.tenant_id, self.project_id,
                                                                 "test-db", "desc", "aws", "us-east-1", 3)

            if user["role"] == "organizationOwner":
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 201))
                #wait for cluster to be healty
                resp = json.loads(resp.content)
                cluster_id = resp["id"]
                self.wait_for_cluster_to_be_healty(cluster_id)
                self.cluster_list.append(cluster_id)
                #delete the cluster
                resp = self.goldfishAPI.delete_goldfish_instance(self.tenant_id, self.project_id,
                                                                cluster_id)
                self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 204,
                                                           user["role"]))
                self.wait_for_cluster_to_be_deleted(cluster_id)
                self.cluster_list.remove(cluster_id)
            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 403,
                                                           user["role"]))

        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectOwner", "projectClusterViewer", "projectClusterManager",
                         "projectDataWriter", "projectDataViewer"]
        user = self.test_users[2]

        for role in project_roles:

            self.log.info(
                "Adding user to project {} with role as {}".format(self.project_id, role))

            payload = {
                "resourceId": self.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }

            resp = self.capellaAPI.add_user_to_project(self.tenant_id, json.dumps(payload))
            self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 200))

            self.goldfishAPIrole = GoldfishAPI("https://" + self.url, "", "",
                                               user["mailid"],
                                               user["password"])

            resp = self.goldfishAPIrole.create_goldfish_instance(self.tenant_id, self.project_id,
                                                                 "test-db", "desc", "aws", "us-east-1", 3)

            if role == "projectOwner" or role == "projectClusterManager":
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 201, role))
                #wait for cluster to be healty
                resp = json.loads(resp.content)
                cluster_id = resp["id"]
                self.wait_for_cluster_to_be_healty(cluster_id)
                self.cluster_list.append(cluster_id)
                #delete the cluster
                resp = self.goldfishAPI.delete_goldfish_instance(self.tenant_id, self.project_id,
                                                                cluster_id)
                self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 204))
                self.wait_for_cluster_to_be_deleted(cluster_id)
                self.cluster_list.remove(cluster_id)
            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 403, role))

            resp = self.capellaAPI.remove_user_from_project(self.tenant_id, user["userid"],
                                                            self.project_id)
            self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 204))


    def test_get_goldfish_instances_security(self):

        headers = {
            "invalid_header": 'abcdefgh',
            "empty_header": ''
        }
        for header in headers:
            print("Header: {}".format(headers[header]))
            cbc_api_request_headers = {
                    'Authorization': 'Bearer %s' % headers[header],
                    'Content-Type': 'application/json'
            }
            resp = self.goldfishAPI.get_goldfish_instances(self.tenant_id, self.project_id,
                                                           headers=cbc_api_request_headers,
                                                           skip_jwt_retry=True)
            self.log.info("Response: {}".format(resp))
            self.assertEqual(401, resp.status_code,
                            msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))

        #trying with different tenant ids
        tenant_ids = {
            "valid_tenant_id": self.tenant_id,
            "invalid_tenant_id": self.invalid_id
        }

        for tenant_id in tenant_ids:
            print("Tenant id: {}".format(tenant_ids[tenant_id]))
            resp = self.goldfishAPI.get_goldfish_instances(tenant_ids[tenant_id], self.project_id)
            if tenant_id == "invalid_tenant_id":
                self.assertEqual(404, resp.status_code,
                                msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 403))
            else:
                self.assertEqual(200, resp.status_code,
                                msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 201))

         # Trying out with different project ids
        self.log.info("Verifying the endpoint access with different projects")
        self.log.info("Creating a project")
        resp = self.capellaAPI.create_project(self.tenant_id, "security-test-project")
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
            resp = self.goldfishAPI.get_goldfish_instances(self.tenant_id, project_ids[project_id])
            if project_id == "valid_project_id":
                self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 200))
            else:
                self.assertEqual(404, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 404))

        self.log.info("Deleting project")
        resp = self.capellaAPI.delete_project(self.tenant_id, project_ids["different_project_id"])
        self.assertEqual(204, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 204))

        for user in self.test_users:
            self.goldfishAPIrole = GoldfishAPI("https://" + self.url, "", "",
                                               user["mailid"],
                                               user["password"])

            resp = self.goldfishAPIrole.get_goldfish_instances(self.tenant_id)

            if user["role"] == "organizationOwner":
                self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 200,
                                                           user["role"]))
            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 403,
                                                           user["role"]))

        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectOwner", "projectClusterViewer", "projectClusterManager",
                         "projectDataWriter", "projectDataViewer"]
        user = self.test_users[2]

        for role in project_roles:

            self.log.info(
                "Adding user to project {} with role as {}".format(self.project_id, role))

            payload = {
                "resourceId": self.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }

            resp = self.capellaAPI.add_user_to_project(self.tenant_id, json.dumps(payload))
            self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 200))

            self.goldfishAPIrole = GoldfishAPI("https://" + self.url, "", "",
                                               user["mailid"],
                                               user["password"])

            resp = self.goldfishAPIrole.get_goldfish_instances(self.tenant_id, self.project_id)

            self.assertEqual(200, resp.status_code,
                                msg='FAIL, Outcome:{}, Expected: {}.' \
                                    'For role: {}'.format(resp.status_code, 200, role))

            resp = self.capellaAPI.remove_user_from_project(self.tenant_id, user["userid"],
                                                            self.project_id)
            self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 204))

    def test_fetch_goldfish_instance_details_security(self):

        self.cluster = self.list_all_clusters()[0]
        cluster_id = self.cluster.cluster_id

        headers = {
            "invalid_header": 'abcdefgh',
            "empty_header": ''
        }
        for header in headers:
            cbc_api_request_headers = {
                    'Authorization': 'Bearer %s' % headers[header],
                    'Content-Type': 'application/json'
            }
            resp = self.goldfishAPI.get_specific_goldfish_instance(self.tenant_id, self.project_id,
                                                                   cluster_id, headers=cbc_api_request_headers,
                                                                   skip_jwt_retry=True)
            self.log.info("Response: {}".format(resp))
            self.assertEqual(401, resp.status_code,
                            msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))

        #testing with different tenant ids
        tenant_ids = {
            "valid_tenant_id": self.tenant_id,
            "invalid_tenant_id": self.invalid_id
        }

        for tenant_id in tenant_ids:
            resp = self.goldfishAPI.get_specific_goldfish_instance(tenant_ids[tenant_id], self.project_id,
                                                                   cluster_id)
            if tenant_id == "invalid_tenant_id":
                self.assertEqual(404, resp.status_code,
                                msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 404))
            else:
                self.assertEqual(200, resp.status_code,
                                msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 200))

        # Trying out with different project ids
        self.log.info("Verifying the endpoint access with different projects")
        self.log.info("Creating a project")
        resp = self.capellaAPI.create_project(self.tenant_id, "security-test-project")
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
            resp = self.goldfishAPI.get_specific_goldfish_instance(self.tenant_id, project_ids[project_id],
                                                                   cluster_id)
            if project_id == "valid_project_id":
                self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 200))
            else:
                self.assertEqual(404, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 404))

        self.log.info("Deleting project")
        resp = self.capellaAPI.delete_project(self.tenant_id, project_ids["different_project_id"])
        self.assertEqual(204, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 204))

        for user in self.test_users:
            self.goldfishAPIrole = GoldfishAPI("https://" + self.url, "", "",
                                               user["mailid"],
                                               user["password"])

            resp = self.goldfishAPIrole.get_specific_goldfish_instance(self.tenant_id, self.project_id,
                                                                       cluster_id)

            if user["role"] == "organizationOwner":
                self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 200,
                                                           user["role"]))
            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 403,
                                                           user["role"]))

        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectOwner", "projectClusterViewer", "projectClusterManager",
                         "projectDataWriter", "projectDataViewer"]
        user = self.test_users[2]

        for role in project_roles:

            self.log.info(
                "Adding user to project {} with role as {}".format(self.project_id, role))

            payload = {
                "resourceId": self.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }

            resp = self.capellaAPI.add_user_to_project(self.tenant_id, json.dumps(payload))
            self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 200))

            self.goldfishAPIrole = GoldfishAPI("https://" + self.url, "", "",
                                               user["mailid"],
                                               user["password"])

            resp = self.goldfishAPIrole.get_specific_goldfish_instance(self.tenant_id, self.project_id,
                                                                       cluster_id)

            self.assertEqual(200, resp.status_code,
                                msg='FAIL, Outcome:{}, Expected: {}.' \
                                    'For role: {}'.format(resp.status_code, 200, role))

            resp = self.capellaAPI.remove_user_from_project(self.tenant_id, user["userid"],
                                                            self.project_id)
            self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 204))

    def test_delete_goldfish_instance_secuirty(self):
        resp = self.goldfishAPI.create_goldfish_instance(self.tenant_id, self.project_id, "test-db-2", "desc",
                                                  provider="aws", region="us-east-2", nodes=3)
        if resp.status_code != 201:
            self.fail("Failed to create goldfish isntance")
        resp = resp.json()
        cluster_id = resp["id"]
        self.wait_for_cluster_to_be_healty(cluster_id)
        self.cluster_list.append(cluster_id)

        headers = {
            "invalid_header": 'abcdefgh',
            "empty_header": ''
        }
        for header in headers:
            cbc_api_request_headers = {
                    'Authorization': 'Bearer %s' % headers[header],
                    'Content-Type': 'application/json'
            }
            resp = self.goldfishAPI.delete_goldfish_instance(self.tenant_id, self.project_id,
                                                            cluster_id, headers=cbc_api_request_headers,
                                                            skip_jwt_retry=True)
            self.log.info("Response: {}".format(resp))
            self.assertEqual(401, resp.status_code,
                            msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))

        #testing with different tenant ids
        tenant_ids = {
            "valid_tenant_id": self.tenant_id,
            "invalid_tenant_id": self.invalid_id
        }

        for tenant_id in tenant_ids:
            resp = self.goldfishAPI.delete_goldfish_instance(tenant_ids[tenant_id], self.project_id,
                                                             cluster_id)
            if tenant_id == "invalid_tenant_id":
                self.assertEqual(404, resp.status_code,
                                msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 404))
            else:
                self.assertEqual(204, resp.status_code,
                                msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 204))
                self.wait_for_cluster_to_be_deleted(cluster_id)
                self.cluster_list.remove(cluster_id)
                resp = self.goldfishAPI.create_goldfish_instance(self.tenant_id, self.project_id, "test-db-2", "desc",
                                                  provider="aws", region="us-east-2", nodes=3)
                if resp.status_code != 201:
                    self.fail("Failed to create goldfish isntance")
                resp = resp.json()
                cluster_id = resp["id"]
                self.wait_for_cluster_to_be_healty(cluster_id)
                self.cluster_list.append(cluster_id)

        # Trying out with different project ids
        self.log.info("Verifying the endpoint access with different projects")
        self.log.info("Creating a project")
        resp = self.capellaAPI.create_project(self.tenant_id, "security-test-project")
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
            resp = self.goldfishAPI.delete_goldfish_instance(self.tenant_id, project_ids[project_id],
                                                                   cluster_id)
            if project_id == "valid_project_id":
                self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 204))
                self.wait_for_cluster_to_be_deleted(cluster_id)
                self.cluster_list.remove(cluster_id)
                resp = self.goldfishAPI.create_goldfish_instance(self.tenant_id, self.project_id, "test-db-2", "desc",
                                                  provider="aws", region="us-east-2", nodes=3)
                if resp.status_code != 201:
                    self.fail("Failed to create goldfish isntance")
                resp = resp.json()
                cluster_id = resp["id"]
                self.wait_for_cluster_to_be_healty(cluster_id)
                self.cluster_list.append(cluster_id)
            else:
                self.assertEqual(404, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 404))

        self.log.info("Deleting project")
        resp = self.capellaAPI.delete_project(self.tenant_id, project_ids["different_project_id"])
        self.assertEqual(204, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 204))

        for user in self.test_users:
            self.goldfishAPIrole = GoldfishAPI("https://" + self.url, "", "",
                                               user["mailid"],
                                               user["password"])

            resp = self.goldfishAPIrole.delete_goldfish_instance(self.tenant_id, self.project_id,
                                                                       cluster_id)

            if user["role"] == "organizationOwner":
                self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 204,
                                                           user["role"]))
                self.wait_for_cluster_to_be_deleted(cluster_id)
                self.cluster_list.remove(cluster_id)
                resp = self.goldfishAPI.create_goldfish_instance(self.tenant_id, self.project_id, "test-db-2", "desc",
                                                  provider="aws", region="us-east-2", nodes=3)
                if resp.status_code != 201:
                    self.fail("Failed to create goldfish isntance")
                resp = resp.json()
                cluster_id = resp["id"]
                self.wait_for_cluster_to_be_healty(cluster_id)
                self.cluster_list.append(cluster_id)
            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 403,
                                                           user["role"]))

        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectOwner", "projectClusterViewer", "projectClusterManager",
                         "projectDataWriter", "projectDataViewer"]
        user = self.test_users[2]

        for role in project_roles:

            self.log.info(
                "Adding user to project {} with role as {}".format(self.project_id, role))

            payload = {
                "resourceId": self.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }

            resp = self.capellaAPI.add_user_to_project(self.tenant_id, json.dumps(payload))
            self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 200))

            self.goldfishAPIrole = GoldfishAPI("https://" + self.url, "", "",
                                               user["mailid"],
                                               user["password"])

            resp = self.goldfishAPIrole.delete_goldfish_instance(self.tenant_id, self.project_id,
                                                                 cluster_id)

            if role == "projectOwner" or role == "projectClusterManager":
                self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 200, role))
                self.wait_for_cluster_to_be_deleted(cluster_id)
                self.cluster_list.remove(cluster_id)
                resp = self.goldfishAPI.create_goldfish_instance(self.tenant_id, self.project_id, "test-db-2", "desc",
                                                  provider="aws", region="us-east-2", nodes=3)
                if resp.status_code != 201:
                    self.fail("Failed to create goldfish isntance")
                resp = resp.json()
                cluster_id = resp["id"]
                self.wait_for_cluster_to_be_healty(cluster_id)
                self.cluster_list.append(cluster_id)
            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 200, role))

            resp = self.capellaAPI.remove_user_from_project(self.tenant_id, user["userid"],
                                                            self.project_id)
            self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 204))

        resp = self.goldfishAPI.delete_goldfish_instance(self.tenant_id, self.project_id,
                                                         cluster_id)
        self.assertEqual(204, resp.status_code,
                        msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 204))
        self.wait_for_cluster_to_be_deleted(cluster_id)
        self.cluster_list.remove(cluster_id)

    def test_cluster_scaling(self):

        self.cluster = self.list_all_clusters()[0]
        cluster_id = self.cluster.cluster_id


        #test empty jwt and invalid jwt
        headers = {
            "invalid_header": 'abcdefgh',
            "empty_header": ''
        }
        for header in headers:
            cbc_api_request_headers = {
                    'Authorization': 'Bearer %s' % headers[header],
                    'Content-Type': 'application/json'
            }
            resp = self.goldfishAPI.update_goldfish_instance(self.tenant_id, self.project_id,
                                                            cluster_id, self.cluster.name, "",
                                                            3, headers=cbc_api_request_headers,
                                                            skip_jwt_retry=True)
            self.log.info("Response: {}".format(resp))
            self.assertEqual(401, resp.status_code,
                            msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))

        #testing with different tenant ids
        tenant_ids = {
            "valid_tenant_id": self.tenant_id,
            "invalid_tenant_id": self.invalid_id
        }

        for tenant_id in tenant_ids:
            resp = self.goldfishAPI.update_goldfish_instance(tenant_ids[tenant_id], self.project_id,
                                                             cluster_id, self.cluster.name, "",
                                                             3)
            #wait for cluster scaling
            #scaled down cluster
            if tenant_id == "invalid_tenant_id":
                self.assertEqual(404, resp.status_code,
                                msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 404))
            else:
                self.assertEqual(202, resp.status_code,
                                msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 202))
                self.goldfish_utils.wait_for_cluster_scaling_operation_to_complete(
                            self.pod, self.users[0], self.cluster)

         # Trying out with different project ids
        self.log.info("Verifying the endpoint access with different projects")
        self.log.info("Creating a project")
        resp = self.capellaAPI.create_project(self.tenant_id, "security-test-project")
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
            resp = self.goldfishAPI.update_goldfish_instance(self.tenant_id, self.project_id,
                                                            cluster_id, self.cluster.name, "",
                                                            3)
            if project_id == "valid_project_id":
                self.assertEqual(202, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 202))
                self.goldfish_utils.wait_for_cluster_scaling_operation_to_complete(
                            self.pod, self.users[0], self.cluster)
            else:
                self.assertEqual(404, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 404))

        self.log.info("Deleting project")
        resp = self.capellaAPI.delete_project(self.tenant_id, project_ids["different_project_id"])
        self.assertEqual(204, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 204))

        for user in self.test_users:
            self.goldfishAPIrole = GoldfishAPI("https://" + self.url, "", "",
                                               user["mailid"],
                                               user["password"])

            resp = self.goldfishAPIrole.update_goldfish_instance(self.tenant_id, self.project_id,
                                                             cluster_id, self.cluster.name, "",
                                                             3)

            if user["role"] == "organizationOwner":
                self.assertEqual(202, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 202,
                                                           user["role"]))
                self.goldfish_utils.wait_for_cluster_scaling_operation_to_complete(
                            self.pod, self.users[0], self.cluster)
            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 403,
                                                           user["role"]))

        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectOwner", "projectClusterViewer", "projectClusterManager",
                         "projectDataWriter", "projectDataViewer"]
        user = self.test_users[2]

        for role in project_roles:

            self.log.info(
                "Adding user to project {} with role as {}".format(self.project_id, role))

            payload = {
                "resourceId": self.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }

            resp = self.capellaAPI.add_user_to_project(self.tenant_id, json.dumps(payload))
            self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 200))

            self.goldfishAPIrole = GoldfishAPI("https://" + self.url, "", "",
                                               user["mailid"],
                                               user["password"])

            resp = self.goldfishAPIrole.update_goldfish_instance(self.tenant_id, self.project_id,
                                                             cluster_id, self.cluster.name, "",
                                                             3)

            if role == "projectOwner" or role == "projectClusterManager":
                self.assertEqual(202, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 202, role))
            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 403, role))

            resp = self.capellaAPI.remove_user_from_project(self.tenant_id, user["userid"],
                                                            self.project_id)
            self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 204))


    def get_api_key_payload(self):
        pass

    def test_create_api_keys_security(self):

        self.cluster = self.list_all_clusters()[0]
        cluster_id = self.cluster.cluster_id
        #create bucket and collections
        payload = self.get_api_key_payload()

        headers = {
            "invalid_header": 'abcdefgh',
            "empty_header": ''
        }
        for header in headers:
            cbc_api_request_headers = {
                    'Authorization': 'Bearer %s' % headers[header],
                    'Content-Type': 'application/json'
            }
            resp = self.goldfishAPI.create_api_keys(self.tenant_id, self.project_id,
                                                    cluster_id,
                                                    headers=cbc_api_request_headers,
                                                    skip_jwt_retry=True)
            self.log.info("Response: {}".format(resp))
            self.assertEqual(401, resp.status_code,
                            msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))

        #testing with different tenant ids
        tenant_ids = {
            "valid_tenant_id": self.tenant_id,
            "invalid_tenant_id": self.invalid_id
        }

        for tenant_id in tenant_ids:
            print("Tenant id: {}".format(tenant_ids[tenant_id]))
            resp = self.goldfishAPI.create_api_keys(tenant_ids[tenant_id], self.project_id,
                                                    cluster_id)
            if tenant_id == "invalid_tenant_id":
                self.assertEqual(404, resp.status_code,
                                msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 404))
            else:
                self.assertEqual(201, resp.status_code,
                                msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 201))
                #delte api keys

        # Trying out with different project ids
        self.log.info("Verifying the endpoint access with different projects")
        self.log.info("Creating a project")
        resp = self.capellaAPI.create_project(self.tenant_id, "security-test-project")
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
            resp = self.goldfishAPI.create_api_keys(self.tenant_id, project_ids[project_id],
                                                    cluster_id)
            if project_id == "valid_project_id":
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 201))
                #delete api keys
            else:
                self.assertEqual(404, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 404))

        self.log.info("Deleting project")
        resp = self.capellaAPI.delete_project(self.tenant_id, project_ids["different_project_id"])
        self.assertEqual(204, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 204))

        for user in self.test_users:
            self.goldfishAPIrole = GoldfishAPI("https://" + self.url, "", "",
                                               user["mailid"],
                                               user["password"])

            resp = self.goldfishAPIrole.create_api_keys(self.tenant_id, self.project_id,
                                                    cluster_id)

            if user["role"] == "organizationOwner":
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 201, user["role"]))
                #delte api keys
            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 403, user["role"]))

        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectOwner", "projectClusterViewer", "projectClusterManager",
                         "projectDataWriter", "projectDataViewer"]
        user = self.test_users[2]

        for role in project_roles:

            self.log.info(
                "Adding user to project {} with role as {}".format(self.project_id, role))

            payload = {
                "resourceId": self.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }

            resp = self.capellaAPI.add_user_to_project(self.tenant_id, json.dumps(payload))
            self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 200))

            self.goldfishAPIrole = GoldfishAPI("https://" + self.url, "", "",
                                               user["mailid"],
                                               user["password"])

            resp = self.goldfishAPIrole.create_api_keys(self.tenant_id, self.project_id,
                                                    cluster_id)

            if role == "projectOwner" or role == "projectClusterManager":
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(role, resp.status_code, 201))
                #delete api key
            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(role, resp.status_code, 403))

            resp = self.capellaAPI.remove_user_from_project(self.tenant_id, user["userid"],
                                                            self.project_id)
            self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 204))

    def goldfish_test_data_isolation(self):

        self.cluster1 = self.list_all_clusters()[0]
        cluster_username1, cluster_password1 = self.generate_database_credentials(self.cluster1)
        dataverse_name = "securityScope"
        collection_name = "securityColl"
        print("C1 name,  endpoint: {}, {}".format(self.cluster1.name, self.cluster1.endpoint))

        #create scope and collections in one intance
        res = self.cbas_util.create_dataverse(self.cluster1, dataverse_name, cluster_username1,
                                              cluster_password1, if_not_exists=True)
        if not res:
            self.fail("Failed to create dataverse")

        res = self.cbas_util.create_standalone_collection(self.cluster1, collection_name,
                                                          dataverse_name=dataverse_name,
                                                          username=cluster_username1,
                                                          password=cluster_password1)
        if not res:
            self.fail("Failed to create standalone collection")

        self.cluster2 = self.list_all_clusters()[1]
        cluster_username2, cluster_password2 = self.generate_database_credentials(self.cluster2)
        print("C2 name,  endpoint: {}, {}".format(self.cluster2.name, self.cluster2.endpoint))

        execute_statement = "Select * from " + dataverse_name + "." + collection_name
        response, metrics, errors, results, handle = self.cbas_util.execute_statement_on_cbas_util(self.cluster2,
                                                                                              execute_statement,
                                                                                              username=cluster_username2,
                                                                                              password=cluster_password2)


        expected_err_msg = "Cannot find analytics collection securityColl in analytics scope securityScope" \
                           " nor an alias with name securityColl"
        actual_err_msg = errors[0]["msg"]
        if expected_err_msg not in actual_err_msg:
            self.fail("Data isolation failed")

    def goldfish_nebula_test_invalid_credentials(self):

        statement_to_execute = "Select 1;"

        self.cluster = self.list_all_clusters()[0]
        cluster_username, cluster_password = self.generate_database_credentials(self.cluster)

        usernames = [cluster_username, "", "%!xyz", "Administrator"]
        passwords = [cluster_password, "", "78!awy", "password"]

        for i in range(len(usernames)):
            username = usernames[i]
            for j in range(len(passwords)):
                password = passwords[j]

                response, metrics, errors, results, handle = self.cbas_util.execute_statement_on_cbas_util(self.cluster,
                                                                                              statement_to_execute,
                                                                                              username=username,
                                                                                              password=password)

                if i == 0 and j == 0:
                    if errors:
                        self.fail("Got error message for valid credentials, Err: {}".format(errors[0]["msg"]))
                else:
                    actual_err_msg = errors[0]["msg"]
                    expected_err_msg = "Unauthorized user."
                    if not actual_err_msg == expected_err_msg:
                        self.fail("Received unexpected error message for invalid credentials, {}:{}." \
                                  "Expected: {}, Actual: {}".format(username, password,
                                                                    expected_err_msg, actual_err_msg))

    def goldfish_nebula_test_open_ports(self):

        self.cluster = self.list_all_clusters()[0]
        valid_ports = [self.CBAS_PORT, self.KV_PORT, self.SVC_MGR_PORT]
        invalid_ports = []

        for i in range(10):
            random_port = self.generate_random_port()
            invalid_ports.append(random_port)

        for port in valid_ports:
            port_status = self.is_port_open(self.cluster.endpoint, port)
            if not port_status:
                self.fail("Expected port {} to be open but connection to port failed ".
                          format(port))

        for port in invalid_ports:
            port_status = self.is_port_open(self.cluster.endpoint, port)
            if port_status:
                self.fail("Expected port {} to be closed but port is open".
                          format(port))

    def goldfish_security_test_invalid_endpoints(self):

        self.cluster = self.list_all_clusters()[0]

        cluster_username, cluster_password = self.generate_database_credentials(self.cluster)

        test_endpoints = ["/analytics/service",
                          "/analytics/cluster", "/analytics/status/ingestion",
                          "/analytics/node/agg/stats/remaining",
                          "/analytics/config/service", "/analytics/config/node",
                          "/settings/analytics", "/analytics/link/"]

        for endpoint in test_endpoints:
            response = self.send_request_to_nebula(self.cluster.endpoint, self.CBAS_PORT,
                                                   endpoint, cluster_username, cluster_password)
            if endpoint == "/analytics/service":
                if response.status_code != 400:
                    self.fail("Failed to send request to service endpoint. " \
                              "Expected status code: {}, Returned status code: {}".
                              format(400, response.status_code))
            elif endpoint == "/analytics/link/":
                if response.status_code != 200:
                    self.fail("Failed to send request to link endpoint." \
                              "Expected status code: {}, Returned status code: {}".
                              format(200, response.status_code))
            else:
                if response.status_code != 404:
                    self.fail("Found unexpected endpoint: {}" \
                              " Expected status code: {}, Returned status code: {}".
                              format(endpoint, 404, response.status_code))

        return












