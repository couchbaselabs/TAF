import base64
import json
import random
import socket
import string
import time

import requests
from Columnar.columnar_base import ColumnarBaseTest
from capella_utils.columnar import (
    Users, Project, DBUser, ColumnarInstance, ColumnarUtils)
from capellaAPI.capella.columnar.CapellaAPI import CapellaAPI as ColumnarAPI
from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI as CapellaAPIv2

from uuid import uuid4

class SecurityTest(ColumnarBaseTest):

    def setUp(self):
        ColumnarBaseTest.setUp(self)
        self.tenant_id = self.capella.get("tenant_id")
        self.project_id = self.project.project_id
        self.invalid_id = "00000000-0000-0000-0000-000000000000"
        self.instance_list = []

        self.columnar_api = ColumnarAPI(self.pod.url_public, self.user.api_secret_key,
                                        self.user.api_access_key, self.user.email,
                                        self.user.password)
        self.capella_api = CapellaAPIv2(self.pod.url_public, self.user.api_secret_key,
                                        self.user.api_access_key, self.user.email,
                                        self.user.password)

        self.create_different_organization_roles()

    def tearDown(self):
        self.delete_different_organization_roles()
        for instance_id in self.instance_list:
            self.log.info("Deleting instance: {}".format(instance_id))
            resp = self.columnar_api.delete_columnar_instance(self.tenant_id, self.project_id,
                                                              instance_id)
            if resp.status_code != 202:
                self.log.error("Failed to delete instance: {}".format(instance_id))

    def generate_random_email():
        domain = 'couchbase.com'
        username = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        return "{}@{}".format(username, domain)

    def generate_random_password():
        chars = string.ascii_letters + string.digits + string.punctuation
        return ''.join(random.choices(chars, k=12))

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

    def delete_different_organization_roles(self):
        self.log.info("Deleting different Organization Roles")
        for user in self.test_users:
            user_id = self.test_users[user]["userid"]
            self.log.info("Deleting user from organization. User Id: {}".format(user_id))

            resp = self.capella_api.delete_user(self.tenant_id,
                                                user_id)

            if resp.status_code != 204:
                self.log.info("Failed to delete user with user id: {}. Reason: {} {}".format(
                    user_id, resp.json()["message"], resp.json()["hint"]))
                raise Exception("Failed to delete user")

        self.log.info("Deleted all the Organization Roles successfully")

    def create_different_organization_roles(self):
        self.log.info("Creating Different Organization Roles")
        self.test_users = {}
        roles = ["organizationOwner", "projectCreator", "organizationMember"]
        setup_capella_api = CapellaAPIv2(self.pod.url_public, self.user.api_secret_key,
                                         self.user.api_access_key, self.user.email,
                                         self.user.password)

        num = 1
        for role in roles:
            usrname = self.user.email.split('@')
            username = usrname[0] + "+" + self.generate_random_string(9, False) + "@" + usrname[1]
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


    def wait_for_instance_to_be_healty(self, instance_id):
        start_time = time.time()
        instance_deployment_end_time = start_time + 1800
        fetch_instance_details_end_time = start_time + 300

        state = None
        while time.time() < instance_deployment_end_time:
            resp = self.columnar_api.get_specific_columnar_instance(self.tenant_id,
                                                                    self.project.project_id,
                                                                    instance_id)
            if resp.status_code != 200:
                if time.time() > fetch_instance_details_end_time:
                    self.fail("Failed to fetch instance details within 5 minutes")
                self.sleep(10, "Could not fetch instance details, Status code: {}".format(resp.status_code))
                continue

            resp = resp.json()
            state = resp["state"]

            if state == "healthy":
                break
            elif state == "deploying":
                self.sleep(10, "Wait for test instance to be healthy")
            else:
                self.fail("Columnar instance deployment failed, Instance state: {}".format(state))

        if state == "deploying":
            self.fail("Instance is still deploying after timeout")

    def wait_for_instance_to_be_deleted(self, instance_id):
        start_time = time.time()
        end_time_healthy_to_destroying = start_time + 300  # 5 minutes
        fetch_instance_details_end_time = start_time + 300 # 5 minutes

        while True:
            resp = self.columnar_api.get_specific_columnar_instance(self.tenant_id,
                                                                    self.project.project_id,
                                                                    instance_id)
            if resp.status_code == 404:
                self.log.info("Instance is already deleted")
                return

            if resp.status_code != 200:
                if time.time() > fetch_instance_details_end_time:
                    self.fail("Failed to fetch instance details within 5 minutes")
                self.sleep(10, "Could not fetch instance details, Status code: {}".format(resp.status_code))
                continue

            resp_json = resp.json()
            state = resp_json["state"]

            if state == "healthy":
                if time.time() > end_time_healthy_to_destroying:
                    self.fail("Instance didn't transition to destroying within 5 minutes")
                self.sleep(10, "Wait for test instance to transition to destroying")
            elif state == "destroying":
                break
            else:
                self.fail("Columnar instance deletion failed, Instance state: {}".format(state))

        start_time = time.time()
        end_time_destroying_to_destroyed = start_time + 1800  # 30 minutes
        fetch_instance_details_end_time = start_time + 300 # 5 minutes

        while time.time() < end_time_destroying_to_destroyed:
            resp = self.columnar_api.get_specific_columnar_instance(self.tenant_id,
                                                                    self.project.project_id,
                                                                    instance_id)
            if resp.status_code == 404:
                return  # Instance is deleted

            if resp.status_code != 200:
                if time.time() > fetch_instance_details_end_time:
                    self.fail("Failed to fetch instance details within 5 minutes")
                self.sleep(10, "Could not fetch instance details, Status code: {}".format(resp.status_code))
                continue

            resp_json = resp.json()
            state = resp_json["state"]

            if state == "destroying":
                self.sleep(10, "Wait for test instance to be destroyed")
            else:
                self.fail("Columnar instance deletion failed, Instance state: {}".format(state))

        self.fail("Instance is still destroying after timeout")

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

    def test_create_columnar_instance_security(self):

        self.log.info("Test started")
        # temp_user = Users(self.tenant_id, "", self.generate_random_email(),
        #                   self.generate_random_password())
        # self.columnar_utils_temp = ColumnarUtils(self.log, self.pod, temp_user)
        # headers = {
        #     "invalid_header": 'abcdefgh',
        #     "empty_header": ''
        # }
        # for header in headers:
        #     cbc_api_request_headers = {
        #             'Authorization': 'Bearer %s' % headers[header],
        #             'Content-Type': 'application/json'
        #     }
        #     resp = self.columnar_utils.create
        #     self.log.info("Response: {}".format(resp))
        #     self.assertEqual(401, resp.status_code,
        #                     msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))

        tenant_ids = {
            "valid_tenant_id": self.tenant_id,
            "invalid_tenant_id": self.invalid_id
        }

        for tenant_id in tenant_ids:

            instance_config = self.columnar_utils.generate_cloumnar_instance_configuration()
            resp = self.columnar_api.create_columnar_instance(
                tenant_ids[tenant_id], self.project.project_id, instance_config["name"],
                instance_config["description"], instance_config["provider"],
                instance_config["region"], instance_config["nodes"]
            )
            if tenant_id == "invalid_tenant_id":
                self.assertEqual(404, resp.status_code,
                                msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 404))
            else:
                self.assertEqual(201, resp.status_code,
                                msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 201))
                #wait for cluster to be healty
                resp = json.loads(resp.content)
                instance_id = resp["id"]
                self.wait_for_instance_to_be_healty(instance_id)
                self.instance_list.append(instance_id)

                #delete the instance
                resp = self.columnar_api.delete_columnar_instance(self.tenant_id, self.project_id,
                                                                  instance_id)
                self.assertEqual(202, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 202))
                self.wait_for_instance_to_be_deleted(instance_id)
                self.instance_list.remove(instance_id)

        project_ids = {
            'valid_project_id': self.project_id,
            'invalid_project_id': self.invalid_id
        }

        for project_id in project_ids:
            instance_config = self.columnar_utils.generate_cloumnar_instance_configuration()
            resp = self.columnar_api.create_columnar_instance(
                self.project.org_id, project_ids[project_id], instance_config["name"],
                instance_config["description"], instance_config["provider"],
                instance_config["region"], instance_config["nodes"]
            )
            if project_id == "valid_project_id":
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 201))
                #wait for cluster to be healty
                resp = json.loads(resp.content)
                instance_id = resp["id"]
                self.wait_for_instance_to_be_healty(instance_id)
                self.instance_list.append(instance_id)
                #delete the cluster
                resp = self.columnar_api.delete_columnar_instance(self.tenant_id, self.project_id,
                                                                  instance_id)
                self.assertEqual(202, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 202))
                self.wait_for_instance_to_be_deleted(instance_id)
                self.instance_list.remove(instance_id)
            else:
                self.assertEqual(404, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 404))


        for user in self.test_users:

            self.colunmnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                               self.test_users[user]["mailid"],
                                               self.test_users[user]["password"])

            instance_config = self.columnar_utils.generate_cloumnar_instance_configuration()
            resp = self.colunmnarAPIrole.create_columnar_instance(
                self.project.org_id, self.project.project_id, instance_config["name"],
                instance_config["description"], instance_config["provider"],
                instance_config["region"], instance_config["nodes"]
            )

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 201))
                #wait for cluster to be healty
                resp = json.loads(resp.content)
                instance_id = resp["id"]
                self.wait_for_instance_to_be_healty(instance_id)
                self.instance_list.append(instance_id)
                #delete the cluster
                resp = self.columnar_api.delete_columnar_instance(self.tenant_id, self.project_id,
                                                                  instance_id)
                self.assertEqual(202, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 202,
                                                           self.test_users[user]["role"]))
                self.wait_for_instance_to_be_deleted(instance_id)
                self.instance_list.remove(instance_id)
            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 403,
                                                           self.test_users[user]["role"]))

        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectOwner", "projectClusterViewer", "projectClusterManager",
                         "projectDataWriter", "projectDataViewer"]
        user = self.test_users['User3']

        for role in project_roles:

            self.log.info(
                "Adding user to project {} with role as {}".format(self.project_id, role))

            payload = {
                "resourceId": self.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }

            resp = self.capella_api.add_user_to_project(self.tenant_id, json.dumps(payload))
            self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 200))

            self.colunmnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                                user["mailid"],
                                                user["password"])

            instance_config = self.columnar_utils.generate_cloumnar_instance_configuration()
            resp = self.colunmnarAPIrole.create_columnar_instance(
                self.project.org_id, self.project.project_id, instance_config["name"],
                instance_config["description"], instance_config["provider"],
                instance_config["region"], instance_config["nodes"]
            )

            if role == "projectOwner" or role == "projectClusterManager":
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 201, role))
                #wait for cluster to be healty
                resp = json.loads(resp.content)
                instance_id = resp["id"]
                self.wait_for_instance_to_be_healty(instance_id)
                self.instance_list.append(instance_id)
                #delete the cluster
                resp = self.columnar_api.delete_columnar_instance(self.tenant_id, self.project_id,
                                                                  instance_id)
                self.assertEqual(202, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 202))
                self.wait_for_instance_to_be_deleted(instance_id)
                self.instance_list.remove(instance_id)
            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 403, role))

            resp = self.capella_api.remove_user_from_project(self.tenant_id, user["userid"],
                                                            self.project_id)
            self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 204))


    def test_get_columnar_instances_security(self):

        # headers = {
        #     "invalid_header": 'abcdefgh',
        #     "empty_header": ''
        # }
        # for header in headers:
        #     print("Header: {}".format(headers[header]))
        #     cbc_api_request_headers = {
        #             'Authorization': 'Bearer %s' % headers[header],
        #             'Content-Type': 'application/json'
        #     }
        #     resp = self.columnarAPI.get_columnar_instances(self.tenant_id, self.project_id,
        #                                                    headers=cbc_api_request_headers,
        #                                                    skip_jwt_retry=True)
        #     self.log.info("Response: {}".format(resp))
        #     self.assertEqual(401, resp.status_code,
        #                     msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))

        #trying with different tenant ids
        tenant_ids = {
            "valid_tenant_id": self.tenant_id,
            "invalid_tenant_id": self.invalid_id
        }

        for tenant_id in tenant_ids:
            resp = self.columnar_api.get_columnar_instances(tenant_ids[tenant_id],
                                                            self.project.project_id)
            if tenant_id == "invalid_tenant_id":
                self.assertEqual(404, resp.status_code,
                                msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 403))
            else:
                self.assertEqual(200, resp.status_code,
                                msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 201))

         # Trying out with different project ids

        project_ids = {
            'valid_project_id': self.project_id,
            'invalid_project_id': self.invalid_id
        }

        for project_id in project_ids:
            resp = self.columnar_api.get_columnar_instances(self.tenant_id, project_ids[project_id])
            if project_id == "valid_project_id":
                self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 200))
            else:
                self.assertEqual(404, resp.status_code,
                                 msg='FAIL for {}:{}, Outcome:{}, Expected: {}'
                                 .format(project_id, project_ids[project_id],
                                         resp.status_code, 404))

        for user in self.test_users:
            self.colunmnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                                self.test_users[user]["mailid"],
                                                self.test_users[user]["password"])

            resp = self.colunmnarAPIrole.get_columnar_instances(self.tenant_id,
                                                                self.project_id)

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 200,
                                                           self.test_users[user]["role"]))
            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 403,
                                                           self.test_users[user]["role"]))

        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectOwner", "projectClusterViewer", "projectClusterManager",
                         "projectDataWriter", "projectDataViewer"]
        user = self.test_users['User3']

        for role in project_roles:

            self.log.info(
                "Adding user to project {} with role as {}".format(self.project_id, role))

            payload = {
                "resourceId": self.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }

            resp = self.capella_api.add_user_to_project(self.tenant_id, json.dumps(payload))
            self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 200))

            self.colunmnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                                user["mailid"],
                                                user["password"])

            resp = self.colunmnarAPIrole.get_columnar_instances(self.tenant_id, self.project_id)

            self.assertEqual(200, resp.status_code,
                                msg='FAIL, Outcome:{}, Expected: {}.' \
                                    'For role: {}'.format(resp.status_code, 200, role))

            resp = self.capella_api.remove_user_from_project(self.tenant_id, user["userid"],
                                                            self.project_id)
            self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 204))

    def test_fetch_columnar_instance_details_security(self):

        instance = self.project.instances[0]
        instance_id = instance.instance_id

        # headers = {
        #     "invalid_header": 'abcdefgh',
        #     "empty_header": ''
        # }
        # for header in headers:
        #     cbc_api_request_headers = {
        #             'Authorization': 'Bearer %s' % headers[header],
        #             'Content-Type': 'application/json'
        #     }
        #     resp = self.columnarAPI.get_specific_columnar_instance(self.tenant_id, self.project_id,
        #                                                            cluster_id, headers=cbc_api_request_headers,
        #                                                            skip_jwt_retry=True)
        #     self.log.info("Response: {}".format(resp))
        #     self.assertEqual(401, resp.status_code,
        #                     msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))

        #testing with different tenant ids
        tenant_ids = {
            "valid_tenant_id": self.tenant_id,
            "invalid_tenant_id": self.invalid_id
        }

        for tenant_id in tenant_ids:
            resp = self.columnar_api.get_specific_columnar_instance(tenant_ids[tenant_id], self.project_id,
                                                                    instance_id)
            if tenant_id == "invalid_tenant_id":
                self.assertEqual(404, resp.status_code,
                                msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 404))
            else:
                self.assertEqual(200, resp.status_code,
                                msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 200))

        # Trying out with different project ids
        self.log.info("Verifying the endpoint access with different projects")
        self.log.info("Creating a project")
        resp = self.capella_api.create_project(self.tenant_id,
                                               "security-test-project{}"
                                               .format(random.randint(1, 100000)))
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
            resp = self.columnar_api.get_specific_columnar_instance(self.tenant_id, project_ids[project_id],
                                                                    instance_id)
            if project_id == "valid_project_id":
                self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 200))
            else:
                self.assertEqual(404, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 404))

        self.log.info("Deleting project")
        resp = self.capella_api.delete_project(self.tenant_id, project_ids["different_project_id"])
        self.assertEqual(204, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 204))

        for user in self.test_users:
            self.colunmnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                                self.test_users[user]["mailid"],
                                                self.test_users[user]["password"])

            resp = self.colunmnarAPIrole.get_specific_columnar_instance(self.tenant_id, self.project_id,
                                                                        instance_id)

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 200,
                                                           self.test_users[user]["role"]))
            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 403,
                                                           self.test_users[user]["role"]))

        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectOwner", "projectClusterViewer", "projectClusterManager",
                         "projectDataWriter", "projectDataViewer"]
        user = self.test_users['User3']

        for role in project_roles:

            self.log.info(
                "Adding user to project {} with role as {}".format(self.project_id, role))

            payload = {
                "resourceId": self.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }

            resp = self.capella_api.add_user_to_project(self.tenant_id, json.dumps(payload))
            self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 200))

            self.colunmnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                                user["mailid"],
                                                user["password"])

            resp = self.colunmnarAPIrole.get_specific_columnar_instance(self.tenant_id, self.project_id,
                                                                        instance_id)

            self.assertEqual(200, resp.status_code,
                                msg='FAIL, Outcome:{}, Expected: {}.' \
                                    'For role: {}'.format(resp.status_code, 200, role))

            resp = self.capella_api.remove_user_from_project(self.tenant_id, user["userid"],
                                                             self.project_id)
            self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 204))

    def test_delete_columnar_instance_secuirty(self):
        instance = self.project.instances[0]
        instance_id = instance.instance_id
        self.instance_list.append(instance_id)

        # headers = {
        #     "invalid_header": 'abcdefgh',
        #     "empty_header": ''
        # }
        # for header in headers:
        #     cbc_api_request_headers = {
        #             'Authorization': 'Bearer %s' % headers[header],
        #             'Content-Type': 'application/json'
        #     }
        #     resp = self.columnarAPI.delete_columnar_instance(self.tenant_id, self.project_id,
        #                                                     cluster_id, headers=cbc_api_request_headers,
        #                                                     skip_jwt_retry=True)
        #     self.log.info("Response: {}".format(resp))
        #     self.assertEqual(401, resp.status_code,
        #                     msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))

        #testing with different tenant ids
        tenant_ids = {
            "valid_tenant_id": self.tenant_id,
            "invalid_tenant_id": self.invalid_id
        }

        for tenant_id in tenant_ids:
            resp = self.columnar_api.delete_columnar_instance(tenant_ids[tenant_id], self.project_id,
                                                              instance_id)
            if tenant_id == "invalid_tenant_id":
                self.assertEqual(404, resp.status_code,
                                msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 404))
            else:
                self.assertEqual(202, resp.status_code,
                                msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 202))
                self.wait_for_instance_to_be_deleted(instance_id)
                self.instance_list.remove(instance_id)
                instance_config = self.columnar_utils.generate_cloumnar_instance_configuration()
                resp = self.columnar_api.create_columnar_instance(
                    self.tenant_id, self.project.project_id, instance_config["name"],
                    instance_config["description"], instance_config["provider"],
                    instance_config["region"], instance_config["nodes"]
                )
                if resp.status_code != 201:
                    self.fail("Failed to create columnar isntance")
                resp = resp.json()
                instance_id = resp["id"]
                self.wait_for_instance_to_be_healty(instance_id)
                self.instance_list.append(instance_id)

        # Trying out with different project ids
        self.log.info("Verifying the endpoint access with different projects")
        self.log.info("Creating a project")
        resp = self.capella_api.create_project(self.tenant_id,
                                               "security-test-project"
                                               .format(random.randint(1, 100000)))
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
            resp = self.columnar_api.delete_columnar_instance(self.tenant_id, project_ids[project_id],
                                                              instance_id)
            if project_id == "valid_project_id":
                self.assertEqual(202, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 202))
                self.wait_for_instance_to_be_deleted(instance_id)
                self.instance_list.remove(instance_id)
                instance_config = self.columnar_utils.generate_cloumnar_instance_configuration()
                resp = self.columnar_api.create_columnar_instance(
                    self.tenant_id, self.project.project_id, instance_config["name"],
                    instance_config["description"], instance_config["provider"],
                    instance_config["region"], instance_config["nodes"]
                )
                if resp.status_code != 201:
                    self.fail("Failed to create columnar isntance")
                resp = resp.json()
                instance_id = resp["id"]
                self.wait_for_instance_to_be_healty(instance_id)
                self.instance_list.append(instance_id)
            else:
                self.assertEqual(404, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 404))

        self.log.info("Deleting project")
        resp = self.capella_api.delete_project(self.tenant_id, project_ids["different_project_id"])
        self.assertEqual(204, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 204))

        for user in self.test_users:
            self.colunmnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                                self.test_users[user]["mailid"],
                                                self.test_users[user]["password"])

            resp = self.colunmnarAPIrole.delete_columnar_instance(self.tenant_id, self.project_id,
                                                                  instance_id)

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(202, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 202,
                                                           self.test_users[user]["role"]))
                self.wait_for_instance_to_be_deleted(instance_id)
                self.instance_list.remove(instance_id)
                instance_config = self.columnar_utils.generate_cloumnar_instance_configuration()
                resp = self.columnar_api.create_columnar_instance(
                    self.tenant_id, self.project.project_id, instance_config["name"],
                    instance_config["description"], instance_config["provider"],
                    instance_config["region"], instance_config["nodes"]
                )
                if resp.status_code != 201:
                    self.fail("Failed to create columnar isntance")
                resp = resp.json()
                instance_id = resp["id"]
                self.wait_for_instance_to_be_healty(instance_id)
                self.instance_list.append(instance_id)
            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 403,
                                                           self.test_users[user]["role"]))

        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectOwner", "projectClusterViewer", "projectClusterManager",
                         "projectDataWriter", "projectDataViewer"]
        user = self.test_users['User3']

        for role in project_roles:

            self.log.info(
                "Adding user to project {} with role as {}".format(self.project_id, role))

            payload = {
                "resourceId": self.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }

            resp = self.capella_api.add_user_to_project(self.tenant_id, json.dumps(payload))
            self.assertEqual(200, resp.status_code,
                             msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 200))

            self.columnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                                user["mailid"],
                                                user["password"])

            resp = self.columnarAPIrole.delete_columnar_instance(self.tenant_id, self.project_id,
                                                                  instance_id)

            if role == "projectOwner" or role == "projectClusterManager":
                self.assertEqual(202, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 202, role))
                self.wait_for_instance_to_be_deleted(instance_id)
                self.instance_list.remove(instance_id)
                instance_config = self.columnar_utils.generate_cloumnar_instance_configuration()
                resp = self.columnar_api.create_columnar_instance(
                    self.tenant_id, self.project.project_id, instance_config["name"],
                    instance_config["description"], instance_config["provider"],
                    instance_config["region"], instance_config["nodes"]
                )
                if resp.status_code != 201:
                    self.fail("Failed to create columnar isntance")
                resp = resp.json()
                instance_id = resp["id"]
                self.wait_for_cluster_to_be_healty(instance_id)
                self.cluster_list.append(instance_id)
            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 200, role))

            resp = self.capella_api.remove_user_from_project(self.tenant_id, user["userid"],
                                                             self.project_id)
            self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 204))

        resp = self.columnar_api.delete_columnar_instance(self.tenant_id, self.project_id,
                                                          instance_id)
        self.assertEqual(202, resp.status_code,
                        msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 202))
        self.wait_for_instance_to_be_deleted(instance_id)
        self.instance_list.remove(instance_id)

    def test_cluster_scaling(self):

        self.cluster = self.user.project.clusters[0]
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
            resp = self.columnarAPI.update_columnar_instance(self.tenant_id, self.project_id,
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
            resp = self.columnarAPI.update_columnar_instance(tenant_ids[tenant_id], self.project_id,
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
                self.columnar_utils.wait_for_cluster_scaling_operation_to_complete(
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
            resp = self.columnarAPI.update_columnar_instance(self.tenant_id, self.project_id,
                                                            cluster_id, self.cluster.name, "",
                                                            3)
            if project_id == "valid_project_id":
                self.assertEqual(202, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 202))
                self.columnar_utils.wait_for_cluster_scaling_operation_to_complete(
                            self.pod, self.users[0], self.cluster)
            else:
                self.assertEqual(404, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 404))

        self.log.info("Deleting project")
        resp = self.capellaAPI.delete_project(self.tenant_id, project_ids["different_project_id"])
        self.assertEqual(204, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 204))

        for user in self.test_users:
            self.columnarAPIrole = columnarAPI("https://" + self.url, "", "",
                                               user["mailid"],
                                               user["password"])

            resp = self.columnarAPIrole.update_columnar_instance(self.tenant_id, self.project_id,
                                                             cluster_id, self.cluster.name, "",
                                                             3)

            if user["role"] == "organizationOwner":
                self.assertEqual(202, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 202,
                                                           user["role"]))
                self.columnar_utils.wait_for_cluster_scaling_operation_to_complete(
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

            self.columnarAPIrole = columnarAPI("https://" + self.url, "", "",
                                               user["mailid"],
                                               user["password"])

            resp = self.columnarAPIrole.update_columnar_instance(self.tenant_id, self.project_id,
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

        instance = self.project.instances[0]
        instance_id = instance.instance_id
        #create bucket and collections
        # payload = self.get_api_key_payload()

        # headers = {
        #     "invalid_header": 'abcdefgh',
        #     "empty_header": ''
        # }
        # for header in headers:
        #     cbc_api_request_headers = {
        #             'Authorization': 'Bearer %s' % headers[header],
        #             'Content-Type': 'application/json'
        #     }
        #     resp = self.columnarAPI.create_api_keys(self.tenant_id, self.project_id,
        #                                             cluster_id,
        #                                             headers=cbc_api_request_headers,
        #                                             skip_jwt_retry=True)
        #     self.log.info("Response: {}".format(resp))
        #     self.assertEqual(401, resp.status_code,
        #                     msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))

        #testing with different tenant ids
        tenant_ids = {
            "valid_tenant_id": self.tenant_id,
            "invalid_tenant_id": self.invalid_id
        }

        for tenant_id in tenant_ids:
            resp = self.columnar_api.create_api_keys(tenant_ids[tenant_id], self.project_id,
                                                     instance_id)
            if tenant_id == "invalid_tenant_id":
                self.assertEqual(404, resp.status_code,
                                msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 404))
            else:
                self.assertEqual(201, resp.status_code,
                                msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 201))
                #delte api keys

        # Trying out with different project ids
        self.log.info("Verifying the endpoint access with different projects")

        resp = resp.json()

        project_ids = {
            'valid_project_id': self.project_id,
            'invalid_project_id': self.invalid_id
        }

        for project_id in project_ids:
            resp = self.columnar_api.create_api_keys(self.tenant_id, project_ids[project_id],
                                                     instance_id)
            if project_id == "valid_project_id":
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 201))
                #delete api keys
            else:
                self.assertEqual(404, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 404))


        for user in self.test_users:
            self.colunmnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                                self.test_users[user]["mailid"],
                                                self.test_users[user]["password"])

            resp = self.colunmnarAPIrole.create_api_keys(self.tenant_id, self.project_id,
                                                         instance_id)

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 201, self.test_users[user]["role"]))
                #delte api keys
            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 403, self.test_users[user]["role"]))

        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectOwner", "projectClusterViewer", "projectClusterManager",
                         "projectDataWriter", "projectDataViewer"]
        user = self.test_users['User3']

        for role in project_roles:

            self.log.info(
                "Adding user to project {} with role as {}".format(self.project_id, role))

            payload = {
                "resourceId": self.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }

            resp = self.capella_api.add_user_to_project(self.tenant_id, json.dumps(payload))
            self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 200))

            self.columnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                               user["mailid"],
                                               user["password"])

            resp = self.columnarAPIrole.create_api_keys(self.tenant_id, self.project_id,
                                                        instance_id)

            if role == "projectOwner" or role == "projectClusterManager":
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(role, resp.status_code, 201))
                #delete api key
            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(role, resp.status_code, 403))

            resp = self.capella_api.remove_user_from_project(self.tenant_id, user["userid"],
                                                             self.project_id)
            self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 204))

    def test_get_columnar_api_keys_security(self):
        instance = self.project.instances[0]
        instance_id = instance.instance_id

        # headers = {
        #     "invalid_header": 'abcdefgh',
        #     "empty_header": ''
        # }
        # for header in headers:
        #     cbc_api_request_headers = {
        #             'Authorization': 'Bearer %s' % headers[header],
        #             'Content-Type': 'application/json'
        #     }
        #     resp = self.columnarAPI.create_api_keys(self.tenant_id, self.project_id,
        #                                             cluster_id,
        #                                             headers=cbc_api_request_headers,
        #                                             skip_jwt_retry=True)
        #     self.log.info("Response: {}".format(resp))
        #     self.assertEqual(401, resp.status_code,
        #                     msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))

        #testing with different tenant ids
        tenant_ids = {
            "valid_tenant_id": self.tenant_id,
            "invalid_tenant_id": self.invalid_id
        }

        for tenant_id in tenant_ids:
            resp = self.columnar_api.get_api_keys(tenant_ids[tenant_id], self.project_id,
                                                  instance_id)
            if tenant_id == "invalid_tenant_id":
                self.assertEqual(404, resp.status_code,
                                msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 404))
            else:
                self.assertEqual(200, resp.status_code,
                                msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 200))

        # Trying out with different project ids
        self.log.info("Verifying the endpoint access with different projects")

        resp = resp.json()

        project_ids = {
            'valid_project_id': self.project_id,
            'invalid_project_id': self.invalid_id
        }

        for project_id in project_ids:
            resp = self.columnar_api.get_api_keys(self.tenant_id, project_ids[project_id],
                                                  instance_id)
            if project_id == "valid_project_id":
                self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 200))
                #delete api keys
            else:
                self.assertEqual(404, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 404))

        for user in self.test_users:
            self.colunmnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                                self.test_users[user]["mailid"],
                                                self.test_users[user]["password"])

            resp = self.colunmnarAPIrole.get_api_keys(self.tenant_id, self.project_id,
                                                      instance_id)

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 200, self.test_users[user]["role"]))
            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 403, self.test_users[user]["role"]))

        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectOwner", "projectClusterViewer", "projectClusterManager",
                         "projectDataWriter", "projectDataViewer"]
        user = self.test_users['User3']

        for role in project_roles:

            self.log.info(
                "Adding user to project {} with role as {}".format(self.project_id, role))

            payload = {
                "resourceId": self.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }

            resp = self.capella_api.add_user_to_project(self.tenant_id, json.dumps(payload))
            self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 200))

            self.columnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                               user["mailid"],
                                               user["password"])

            resp = self.columnarAPIrole.get_api_keys(self.tenant_id, self.project_id,
                                                     instance_id)

            if role == "projectOwner" or role == "projectClusterManager" \
                                      or role == "projectClusterViewer":
                self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(role, resp.status_code, 200))
                #delete api key
            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(role, resp.status_code, 403))

            resp = self.capella_api.remove_user_from_project(self.tenant_id, user["userid"],
                                                             self.project_id)
            self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 204))


    def test_delete_columnar_api_keys(self):
        instance = self.project.instances[0]
        instance_id = instance.instance_id

        # headers = {
        #     "invalid_header": 'abcdefgh',
        #     "empty_header": ''
        # }
        # for header in headers:
        #     cbc_api_request_headers = {
        #             'Authorization': 'Bearer %s' % headers[header],
        #             'Content-Type': 'application/json'
        #     }
        #     resp = self.columnarAPI.create_api_keys(self.tenant_id, self.project_id,
        #                                             cluster_id,
        #                                             headers=cbc_api_request_headers,
        #                                             skip_jwt_retry=True)
        #     self.log.info("Response: {}".format(resp))
        #     self.assertEqual(401, resp.status_code,
        #                     msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))

        resp = self.columnar_api.create_api_keys(self.tenant_id, self.project_id,
                                                 instance_id)
        if resp.status_code != 201:
            self.fail("Failed to create API keys")
        resp = resp.json()
        api_key_id = resp["apikeyId"]

        #testing with different tenant ids
        tenant_ids = {
            "valid_tenant_id": self.tenant_id,
            "invalid_tenant_id": self.invalid_id
        }

        for tenant_id in tenant_ids:
            resp = self.columnar_api.delete_api_keys(tenant_ids[tenant_id], self.project_id,
                                                     instance_id, api_key_id)
            if tenant_id == "invalid_tenant_id":
                self.assertEqual(404, resp.status_code,
                                msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 404))
            else:
                self.assertEqual(202, resp.status_code,
                                msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 202))
                resp = self.columnar_api.create_api_keys(self.tenant_id, self.project_id,
                                                 instance_id)
                if resp.status_code != 201:
                    self.fail("Failed to create API keys")
                resp = resp.json()
                api_key_id = resp["apikeyId"]

        for user in self.test_users:
            self.colunmnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                                self.test_users[user]["mailid"],
                                                self.test_users[user]["password"])

            resp = self.colunmnarAPIrole.delete_api_keys(self.tenant_id, self.project_id,
                                                         instance_id, api_key_id)

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(202, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 202, self.test_users[user]["role"]))

                resp = self.columnar_api.create_api_keys(self.tenant_id, self.project_id,
                                                 instance_id)
                if resp.status_code != 201:
                    self.fail("Failed to create API keys")
                resp = resp.json()
                api_key_id = resp["apikeyId"]
            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(resp.status_code, 403, self.test_users[user]["role"]))

        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectOwner", "projectClusterViewer", "projectClusterManager",
                         "projectDataWriter", "projectDataViewer"]
        user = self.test_users['User3']

        for role in project_roles:

            self.log.info(
                "Adding user to project {} with role as {}".format(self.project_id, role))

            payload = {
                "resourceId": self.project_id,
                "resourceType": "project",
                "roles": [role], "users": [user["userid"]]
            }

            resp = self.capella_api.add_user_to_project(self.tenant_id, json.dumps(payload))
            self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 200))

            self.columnarAPIrole = ColumnarAPI(self.pod.url_public, "", "",
                                               user["mailid"],
                                               user["password"])

            resp = self.columnarAPIrole.delete_api_keys(self.tenant_id, self.project_id,
                                                        instance_id, api_key_id)

            if role == "projectOwner" or role == "projectClusterManager":
                self.assertEqual(202, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(role, resp.status_code, 202))

                resp = self.columnar_api.create_api_keys(self.tenant_id, self.project_id,
                                                 instance_id)
                if resp.status_code != 201:
                    self.fail("Failed to create API keys")
                resp = resp.json()
                api_key_id = resp["apikeyId"]
            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}.' \
                                     'For role: {}'.format(role, resp.status_code, 403))

            resp = self.capella_api.remove_user_from_project(self.tenant_id, user["userid"],
                                                             self.project_id)
            self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 204))

    def columnar_test_data_isolation(self):

        self.cluster1 = self.user.project.clusters[0]
        cluster_username1, cluster_password1 = self.generate_database_credentials(self.cluster1)
        dataverse_name = "securityScope"
        collection_name = "securityColl"

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

        self.cluster2 = self.user.project.clusters[1]
        cluster_username2, cluster_password2 = self.generate_database_credentials(self.cluster2)

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

    def columnar_security_test_invalid_endpoints(self):

        self.cluster = self.user.project.clusters[0]

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












