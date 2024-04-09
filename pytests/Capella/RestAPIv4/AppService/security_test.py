import json
import time
import random
from pytests.Capella.RestAPIv4.security_base import SecurityBase
from pytests.basetestcase import BaseTestCase
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI

class SecurityTest(SecurityBase):
    cidr = "10.0.0.0"
    def setUp(self):
        SecurityBase.setUp(self)
        self.app_services_payload = {
            "name": "App_service_security",
            "description": "Testing App Service",
            "nodes": 2,
            "compute":
            {
                "cpu": 2,
                "ram": 4
            },
            "version": "3.0"
        }
        self.test_cluster_ids = []
        self.app_service_ids = []
        # self.test_cluster_ids = []

    def tearDown(self):
        self.tear_app_services()
        self.delete_clusters()
        super(SecurityTest, self).tearDown()

    def tear_app_services(self):
        #Deleting the app services created
        num = 0
        for item in self.app_service_ids:
            status = self.get_app_service_status(self.tenant_id,
                                                 self.project_id,
                                                 item[0],
                                                 item[1])
            if status == "healthy":
                resp = self.capellaAPI.cluster_ops_apis.delete_appservice(self.tenant_id,
                                                                          self.project_id,
                                                                          item[0],
                                                                          item[1])
                self.assertEqual(202, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {} Failed to delete the '
                                     'cluster'.format(resp.status_code, 202))
                num = num + 1
            else:
                self.fail("Failed to delete the app service")
        self.sleep(300, "Waiting for the app services to be deleted")

    def delete_clusters(self):
        self.log.info("Deleting the clusters created for the test")
        for cluster_id in self.test_cluster_ids:
            resp = self.capellaAPI.cluster_ops_apis.delete_cluster(self.tenant_id,
                                                                   self.project_id,
                                                                   cluster_id)
            self.assertEqual(202, resp.status_code,
                             msg='FAIL, Outcome: {}, Expected: {} Failed to delete the '
                                 'cluster'.format(resp.status_code, 202))

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

    def get_app_service_status(self, tenant_id, project_id, cluster_id, app_service_id):
        status = 'pending'
        end_time = time.time() + 1800
        while status != 'healthy' and time.time() < end_time:
            self.sleep(15, "Waiting for app services to be in ready state")
            resp = self.capellaAPI.cluster_ops_apis.get_appservice(tenant_id, project_id,
                                                                   cluster_id, app_service_id)
            status = resp.json()['currentState']
        return status

    def wait_until_app_service_turned_on_off(self, cluster_id, app_service_id,
                                            expected_cluster_status, failed_status):
        status = self.capellaAPI.cluster_ops_apis.get_appservice(self.tenant_id,
                                                                 self.project_id,
                                                                 self.cluster_id,
                                                                 app_service_id)
        status = status.json()["currentState"]
        end_time = time.time() + 1800
        while status != expected_cluster_status and time.time() < end_time:
            self.sleep(15, "Waiting for app service to be in {} state. Current status - {}"
                       .format(expected_cluster_status, status))
            cluster_ready_resp = self.capellaAPI.cluster_ops_apis.get_appservice(self.tenant_id,
                                                                                 self.project_id,
                                                                                 self.cluster_id,
                                                                                 app_service_id)
            cluster_ready_resp = cluster_ready_resp.json()
            status = cluster_ready_resp["currentState"]

            if status == failed_status:
                self.fail("App Service turn on/off failed with status {}. Reason: {}".format(
                                                failed_status, cluster_ready_resp.content))

        return status

    def deploy_clusters(self, num_clusters=1):
        self.log.info("Deploying clusters for the test")
        cluster_ids = []
        payload = {
            "name": "AWS-Test-Cluster-V4-Koushal-",
            "description": "My first test aws cluster for multiple services.",
            "cloudProvider": {
                "type": "aws",
                "region": "us-east-1",
                "cidr": "10.7.22.0/23"
            },
            "couchbaseServer": {
                "version": "7.2"
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
        }
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

    def test_post_create_appservice(self):
        self.log.info("     I. POST Create App Service")
        """
        /v4/organizations/{organizationId}/projects/{projectId}/clusters/{clusterId}/appservices
        {
            "name": "MyAppSyncService",
            "description": "My app sync service.",
            "nodes": 2,
            "compute":
            {
                "cpu": 2,
                "ram": 4
            },
            "version": "3.0"
        }
        """
        # Deploying cluster for the test
        self.test_cluster_ids = self.deploy_clusters(6)

        self.log.info("Verifying status code for creating an app service")
        self.log.info("Verifying the endpoint authentication with different test cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.create_appservice(self.tenant_id,
                                                                  self.project_id,
                                                                  self.cluster_id,
                                                                  appservice_name=self.app_services_payload["name"],
                                                                  compute=self.app_services_payload["compute"],
                                                                  nodes=self.app_services_payload["nodes"],
                                                                  version=self.app_services_payload["version"])
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        # Verifying the create app service endpoint with different organization ids
        self.log.info("Verifying with different tenant ids")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            'invalid_tenant_id': self.invalid_id
        }
        for tenant_id in tenant_ids:
            resp = self.capellaAPI.cluster_ops_apis.create_appservice(tenant_ids[tenant_id],
                                                      self.project_id,
                                                      self.test_cluster_ids[0],
                                                      appservice_name=self.app_services_payload["name"],
                                                      compute=self.app_services_payload["compute"],
                                                      nodes=self.app_services_payload["nodes"],
                                                      version=self.app_services_payload["version"])
            if tenant_id == 'valid_tenant_id':
                self.assertEqual(resp.status_code, 201,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,201))

                temp_app_service_id = resp.json()["id"]
                self.app_service_ids.append([self.test_cluster_ids[0], temp_app_service_id])

            else:
                self.assertEqual(resp.status_code, 403,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Verifying the create app service endpoint with different project ids
        self.log.info("Creating a project")
        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id, "Project_security")
        self.assertEqual(201, resp.status_code,
                         msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code, 201))
        resp = resp.json()
        project_ids = {
            'valid_project_id': self.project_id,
            'different_project_id': resp['id'],
            'invalid_project_id': self.invalid_id
        }
        for project_id in project_ids:
            resp = self.capellaAPI.cluster_ops_apis.create_appservice(self.tenant_id,
                                                                     project_ids[project_id],
                                                                     self.test_cluster_ids[1],
                                                                     appservice_name=self.app_services_payload["name"],
                                                                     compute=self.app_services_payload["compute"],
                                                                     nodes=self.app_services_payload["nodes"],
                                                                     version=self.app_services_payload["version"])
            if project_id == 'valid_project_id':
                self.assertEqual(resp.status_code, 201,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              201))

                temp_app_service_id = resp.json()["id"]
                self.app_service_ids.append([self.test_cluster_ids[1], temp_app_service_id])

            else:
                self.assertEqual(resp.status_code, 422,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              422))
        self.log.info("Deleting project")
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                           project_ids["different_project_id"])
        self.assertEqual(204, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 204))

        # Verifying the create app service endpoint with different cluster ids
        self.log.info("Verifying with different cluster ids")
        cluster_ids = {
            'valid_cluster_id': self.test_cluster_ids[2],
            'invalid_cluster_id': self.invalid_id
        }
        for cluster_id in cluster_ids:
            resp = self.capellaAPI.cluster_ops_apis.create_appservice(self.tenant_id,
                                                                     self.project_id,
                                                                     cluster_ids[cluster_id],
                                                                     appservice_name=self.app_services_payload["name"],
                                                                     compute=self.app_services_payload["compute"],
                                                                     nodes=self.app_services_payload["nodes"],
                                                                     version=self.app_services_payload["version"])
            if cluster_id == 'valid_cluster_id':
                self.assertEqual(resp.status_code, 201,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              201))

                temp_app_service_id = resp.json()["id"]
                self.app_service_ids.append([cluster_ids[cluster_id], temp_app_service_id])

            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))
        # Testing for organization RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"], self.test_users[user]["token"])

            role_response = self.capellaAPIRole.cluster_ops_apis.create_appservice(self.tenant_id,
                                                                 self.project_id,
                                                                 self.test_cluster_ids[3],
                                                                 appservice_name=self.app_services_payload["name"],
                                                                 compute=self.app_services_payload["compute"],
                                                                 nodes=self.app_services_payload["nodes"],
                                                                 version=self.app_services_payload["version"])

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(role_response.status_code, 201,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 201))

                temp_app_service_id = role_response.json()["id"]
                self.app_service_ids.append([self.test_cluster_ids[3], temp_app_service_id])

            else:
                self.assertEqual(role_response.status_code, 403,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 403))

        # Testing for Project Level RBAC roles
        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectOwner", "projectViewer", "projectManager",
                         "projectDataReaderWriter", "projectDataReader"]
        user = self.test_users["User3"]
        num = 4
        for role in project_roles:
            self.log.info("Creating apiKeys for role {}".format(role))
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
                                             user["password"], user["token"])

            role_response = self.capellaAPIRole.cluster_ops_apis.create_appservice(
                                                                 self.tenant_id,
                                                                 self.project_id,
                                                                 self.test_cluster_ids[num],
                                                                 appservice_name=self.app_services_payload["name"],
                                                                 compute=self.app_services_payload["compute"],
                                                                 nodes=self.app_services_payload["nodes"],
                                                                 version=self.app_services_payload["version"])
            if role in ['projectOwner', 'projectManager']:
                self.assertEqual(201, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(
                                     role_response.status_code, 201))
                num = num + 1
                num = num % 6
                temp_app_service_id = role_response.json()["id"]
                self.app_service_ids.append([self.test_cluster_ids[num], temp_app_service_id])

            else:
                self.assertEqual(403, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(
                                     role_response.status_code, 403))

            self.log.info("Removing user from project {} with role as {}".format(self.project_id,
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
                             msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 204))

        # self.tear_app_services()
        # self.delete_clusters()

    def test_get_list_appservices(self):
        self.log.info("     II. GET List App Services")
        """
        /v4/organizations/{organizationId}/appservices
        """
        self.log.info("Verifying status code for listing all app services")

        self.log.info("Verifying the endpoint authentication with different test cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.list_appservices(self.tenant_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.list_appservices(self.tenant_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.list_appservices(self.tenant_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.list_appservices(self.tenant_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        # Verifying the list app service endpoint with different organization ids
        self.log.info("Verifying with different tenant ids")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            'invalid_tenant_id': self.invalid_id
        }
        for tenant_id in tenant_ids:
            resp = self.capellaAPI.cluster_ops_apis.list_appservices(tenant_ids[tenant_id])

            if tenant_id == 'valid_tenant_id':
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))
            else:
                self.assertEqual(resp.status_code, 403,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Testing for organization RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"], self.test_users[user]["token"])

            role_response = self.capellaAPIRole.cluster_ops_apis.list_appservices(self.tenant_id)

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
            self.log.info("Creating apiKeys for role {}".format(role))
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

            self.capellaAPI.org_ops_apis.update_user(self.tenant_id, user['userid'],
                                                     dic["update_info"])

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', user["mailid"],
                                             user["password"], user['token'])

            role_response = self.capellaAPIRole.cluster_ops_apis.list_appservices(self.tenant_id)
            self.assertEqual(200, role_response.status_code,
                             msg="FAIL: Outcome:{}, Expected: {}".format(
                             role_response.status_code, 200))
            self.log.info("Removing user from project {} with role as {}".format(self.project_id,
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
                             msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 204))

    def test_get_get_appservice(self):
        self.log.info("     III. GET Get App Service")
        """
        /v4/organizations/{organizationId}/projects/{projectId}/clusters/{clusterId}/appservices/{appServiceId}
        """
        self.log.info("Verifying status code for retrieving information of a particular app "
                      "service")

        self.log.info("Creating an app service")
        resp = self.capellaAPI.cluster_ops_apis.create_appservice(
                                                                 self.tenant_id,
                                                                 self.project_id,
                                                                 self.cluster_id,
                                                                 appservice_name=self.app_services_payload["name"],
                                                                 compute=self.app_services_payload["compute"],
                                                                 nodes=self.app_services_payload["nodes"],
                                                                 version=self.app_services_payload["version"])
        self.assertEqual(resp.status_code, 201,
                         msg='FAIL: Outcome:{}, Expected:{}'.format(resp.status_code, 201))
        app_service_id = resp.json()["id"]

        self.log.info("Verifying the endpoint authentication with different test cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.get_appservice(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                app_service_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.get_appservice(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                app_service_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.get_appservice(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                app_service_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.get_appservice(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                app_service_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        # Verifying the get app service endpoint with different organization ids
        self.log.info("Verifying with different tenant ids")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            'invalid_tenant_id': self.invalid_id
        }
        for tenant_id in tenant_ids:
            resp = self.capellaAPI.cluster_ops_apis.get_appservice(tenant_ids[tenant_id],
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    app_service_id)
            if tenant_id == 'valid_tenant_id':
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))
            else:
                self.assertEqual(resp.status_code, 403,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))
        # Verify the endpoint with different projects
        self.log.info("Verifying the endpoint access with different projects")

        self.log.info("Creating a project")

        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id, "Project_security")
        self.assertEqual(201, resp.status_code,
                         msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code, 201))
        resp = resp.json()
        project_ids = {
            'valid_project_id': self.project_id,
            'different_project_id': resp['id'],
            'invalid_project_id': self.invalid_id
        }
        for project_id in project_ids:
            resp = self.capellaAPI.cluster_ops_apis.get_appservice(self.tenant_id,
                                                                    project_ids[project_id],
                                                                    self.cluster_id,
                                                                    app_service_id)
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

        # Verifying the get app service endpoint with different cluster ids
        self.log.info("Verifying with different cluster ids")
        cluster_ids = {
            'valid_cluster_id': self.cluster_id,
            'invalid_cluster_id': self.invalid_id
        }
        for cluster_id in cluster_ids:
            resp = self.capellaAPI.cluster_ops_apis.get_appservice(self.tenant_id,
                                                                    self.project_id,
                                                                    cluster_ids[cluster_id],
                                                                    app_service_id)
            if cluster_id == 'valid_cluster_id':
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))
            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        # Verifying the get app service endpoint with different app service ids
        self.log.info("Verifying with different app service ids")
        app_service_ids = {
            'valid_app_service_id': app_service_id,
            'invalid_app_service_id': self.invalid_id
        }
        for a_s_id in app_service_ids:
            resp = self.capellaAPI.cluster_ops_apis.get_appservice(self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    app_service_ids[a_s_id])
            if a_s_id == 'valid_app_service_id':
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))
            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        # Testing for organization RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"], self.test_users[user]["token"])

            role_response = self.capellaAPIRole.cluster_ops_apis.get_appservice(self.tenant_id,
                                                                                 self.project_id,
                                                                                 self.cluster_id,
                                                                                 app_service_id)
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
            self.log.info("Creating apiKeys for role {}".format(role))
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
            self.capellaAPI.org_ops_apis.update_user(self.tenant_id, user['userid'],
                                                     dic["update_info"])

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', user["mailid"],
                                             user["password"], user["token"])
            role_response = self.capellaAPIRole.cluster_ops_apis.get_appservice(self.tenant_id,
                                                                                 self.project_id,
                                                                                 self.cluster_id,
                                                                                 app_service_id)
            self.assertEqual(200, role_response.status_code,
                             msg="FAIL: Outcome:{}, Expected: {}".format(role_response.status_code,
                                                                         200))
            self.log.info("Removing user from project {} with role as {}".format(self.project_id,
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
                             msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 204))

        status = self.get_app_service_status(self.tenant_id, self.project_id, self.cluster_id,
                                             app_service_id)
        if status == "healthy":
            self.log.info("Deleting app service")
            resp = self.capellaAPI.cluster_ops_apis.delete_appservice(self.tenant_id,
                                                                   self.project_id,
                                                                   self.cluster_id,
                                                                   app_service_id)
            self.assertEqual(202, resp.status_code,
                             msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code,
                                                                          202))
    def test_put_update_appservices(self):
        self.log.info("     IV. PUT Update App Service")
        """
        /v4/organizations/{organizationId}/projects/{projectId}/clusters/{clusterId}/appservices/{appServiceId}
        {
            "nodes": 2,
            "compute":
            {
                "cpu": 2,
                "ram": 4
            }
        }
        """
        self.log.info("Verifying status code for updating specs of a particular app service")

        # Deploying cluster for the test
        self.test_cluster_ids = self.deploy_clusters(7)
        for test_cluster_id in self.test_cluster_ids:
            self.log.info("Creating an app service")
            resp = self.capellaAPI.cluster_ops_apis.create_appservice(self.tenant_id,
                                                                      self.project_id,
                                                                      test_cluster_id,
                                                                      appservice_name=self.app_services_payload["name"],
                                                                      compute=self.app_services_payload["compute"],
                                                                      nodes=self.app_services_payload["nodes"],
                                                                      version=self.app_services_payload["version"])
            self.assertEqual(resp.status_code, 201,
                             msg='FAIL: Outcome:{}, Expected:{}'.format(
                                 resp.status_code, 201))
            app_service_id = resp.json()["id"]
            self.app_service_ids.append([test_cluster_id, app_service_id])
        for item in self.app_service_ids:
            status = self.get_app_service_status(self.tenant_id,
                                                 self.project_id,
                                                 item[0],
                                                 item[1])
            if status != "healthy":
                self.fail("Failed to create app service for a cluster")

        num = 0

        self.log.info("Verifying the endpoint authentication with different test cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.update_appservices(self.tenant_id,
                                                                   self.project_id,
                                                                   self.app_service_ids[num][0],
                                                                   self.app_service_ids[num][1],
                                                                   3, 4, 8)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.update_appservices(self.tenant_id,
                                                                   self.project_id,
                                                                   self.app_service_ids[num][0],
                                                                   self.app_service_ids[num][1],
                                                                   3, 4, 8)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.update_appservices(self.tenant_id,
                                                                   self.project_id,
                                                                   self.app_service_ids[num][0],
                                                                   self.app_service_ids[num][1],
                                                                   3, 4, 8)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.update_appservices(self.tenant_id,
                                                                   self.project_id,
                                                                   self.app_service_ids[num][0],
                                                                   self.app_service_ids[num][1],
                                                                   3, 4, 8)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        # Verifying the update app service endpoint with different organization ids
        self.log.info("Verifying with different tenant ids")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            'invalid_tenant_id': self.invalid_id
        }
        for tenant_id in tenant_ids:
            resp = self.capellaAPI.cluster_ops_apis.update_appservices(tenant_ids[tenant_id],
                                                                       self.project_id,
                                                                       self.app_service_ids[num][0],
                                                                       self.app_service_ids[num][1],
                                                                       3, 4, 8)
            if tenant_id == 'valid_tenant_id':
                self.assertEqual(resp.status_code, 204,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              204))
                num = num + 1
            else:
                self.assertEqual(resp.status_code, 403,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Verify the endpoint with different projects
        self.log.info("Verifying the endpoint access with different projects")

        self.log.info("Creating a project")
        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id, "Project_security")
        self.assertEqual(201, resp.status_code,
                         msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code, 201))
        resp = resp.json()
        project_ids = {
            'valid_project_id': self.project_id,
            'different_project_id': resp['id'],
            'invalid_project_id': self.invalid_id
        }
        for project_id in project_ids:
            resp = self.capellaAPI.cluster_ops_apis.update_appservices(self.tenant_id,
                                                                       project_ids[project_id],
                                                                       self.app_service_ids[num][0],
                                                                       self.app_service_ids[num][1],
                                                                       3, 4, 8)
            if project_id == 'valid_project_id':
                self.assertEqual(resp.status_code, 204,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              204))
                num = num + 1
            else:
                self.assertEqual(resp.status_code, 422,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              422))

        self.log.info("Deleting project")
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                           project_ids["different_project_id"])
        self.assertEqual(204, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 204))

        # Verifying the update app service endpoint with different cluster ids
        self.log.info("Verifying with different cluster ids")
        cluster_ids = {
            'valid_cluster_id': self.app_service_ids[num][0],
            'invalid_cluster_id': self.invalid_id
        }
        for cluster_id in cluster_ids:
            resp = self.capellaAPI.cluster_ops_apis.update_appservices(self.tenant_id,
                                                                       self.project_id,
                                                                       cluster_ids[cluster_id],
                                                                       self.app_service_ids[num][1],
                                                                       3, 4, 8)
            if cluster_id == 'valid_cluster_id':
                self.assertEqual(resp.status_code, 204,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              204))
            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))
        num = num + 1

        # Verifying the update project endpoint with different app service ids
        self.log.info("Verifying with different app service ids")
        app_service_ids = {
            'valid_app_service_id': self.app_service_ids[num][1],
            'invalid_app_service_id': self.invalid_id
        }
        for a_s_id in app_service_ids:
            resp = self.capellaAPI.cluster_ops_apis.update_appservices(self.tenant_id,
                                                                       self.project_id,
                                                                       self.app_service_ids[num][0],
                                                                       app_service_ids[a_s_id],
                                                                       3, 4, 8)
            if a_s_id == 'valid_app_service_id':
                self.assertEqual(resp.status_code, 204,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              204))
            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        num = num + 1

        # Testing for organization RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"], self.test_users[user]["token"])

            role_response = self.capellaAPIRole.cluster_ops_apis.update_appservices(
                                                                    self.tenant_id,
                                                                    self.project_id,
                                                                    self.app_service_ids[num][0],
                                                                    self.app_service_ids[num][1],
                                                                    3, 4, 8)

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(role_response.status_code, 204,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 204))
                num = num + 1
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
            self.log.info("Creating apiKeys for role {}".format(role))
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
            self.capellaAPI.org_ops_apis.update_user(self.tenant_id, user['userid'],
                                                     dic["update_info"])

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', user["mailid"],
                                             user["password"], user["token"])

            role_response = self.capellaAPIRole.cluster_ops_apis.update_appservices(
                                                                    self.tenant_id,
                                                                    self.project_id,
                                                                    self.app_service_ids[num][0],
                                                                    self.app_service_ids[num][1],
                                                                    3, 4, 8)

            if role in ["projectOwner", "projectManager"]:
                self.assertEqual(204, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(
                                     role_response.status_code, 204))
                num = num + 1
            else:
                self.assertEqual(403, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(role_response, 403))

            self.log.info("Removing user from project {} with role as {}".format(self.project_id,
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
                             msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 204))

        self.tear_app_services()
        self.delete_clusters()

    def test_del_delete_appservice(self):
        self.log.info("     V. DEL Delete App Service")
        """
        /v4/organizations/{organizationId}/projects/{projectId}/clusters/{clusterId}/appservices/{appServiceId}
        """
        self.log.info("Verifying status code for deleting an app service")

        # Deploying cluster for the test
        self.test_cluster_ids = self.deploy_clusters(7)
        for test_cluster_id in self.test_cluster_ids:
            self.log.info("Creating an app service")
            resp = self.capellaAPI.cluster_ops_apis.create_appservice(self.tenant_id,
                                                                      self.project_id,
                                                                      test_cluster_id,
                                                                      "App_service_security",
                                                                      "Testing App Service",
                                                                      2, 2, 4, "3.0")
            self.assertEqual(resp.status_code, 201,
                             msg='FAIL: Outcome:{}, Expected:{}'.format(
                                 resp.status_code, 201))
            app_service_id = resp.json()["id"]
            self.app_service_ids.append([test_cluster_id, app_service_id])
        for item in self.app_service_ids:
            status = self.get_app_service_status(self.tenant_id,
                                                 self.project_id,
                                                 item[0],
                                                 item[1])
            if status != "healthy":
                self.fail("Failed to create app service for a cluster")

        num = 0

        self.log.info("Verifying the endpoint authentication with different test cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.delete_appservice(self.tenant_id,
                                                                  self.project_id,
                                                                  self.app_service_ids[num][0],
                                                                  self.app_service_ids[num][1])
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.delete_appservice(self.tenant_id,
                                                                  self.project_id,
                                                                  self.app_service_ids[num][0],
                                                                  self.app_service_ids[num][1]
                                                                  )
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.delete_appservice(self.tenant_id,
                                                                  self.project_id,
                                                                  self.app_service_ids[num][0],
                                                                  self.app_service_ids[num][1]
                                                                  )
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.delete_appservice(self.tenant_id,
                                                                  self.project_id,
                                                                  self.app_service_ids[num][0],
                                                                  self.app_service_ids[num][1])
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        # Verifying delete app service endpoint with different organization ids
        self.log.info("Verifying with different tenant ids")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            'invalid_tenant_id': self.invalid_id
        }
        for tenant_id in tenant_ids:
            resp = self.capellaAPI.cluster_ops_apis.delete_appservice(tenant_ids[tenant_id],
                                                                      self.project_id,
                                                                      self.app_service_ids[num][0],
                                                                      self.app_service_ids[num][1])
            if tenant_id == 'valid_tenant_id':
                self.assertEqual(resp.status_code, 202,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              202))
                num = num + 1
            else:
                self.assertEqual(resp.status_code, 403,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Verify the endpoint with different projects
        self.log.info("Verifying the endpoint access with different projects")

        self.log.info("Creating a project")
        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id, "Project_security")
        self.assertEqual(201, resp.status_code,
                         msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code, 201))
        resp = resp.json()
        project_ids = {
            'valid_project_id': self.project_id,
            'different_project_id': resp['id'],
            'invalid_project_id': self.invalid_id
        }
        for project_id in project_ids:
            resp = self.capellaAPI.cluster_ops_apis.delete_appservice(self.tenant_id,
                                                                      project_ids[project_id],
                                                                      self.app_service_ids[num][0],
                                                                      self.app_service_ids[num][1])
            if project_id == 'valid_project_id':
                self.assertEqual(resp.status_code, 202,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              202))
                num = num + 1
            else:
                self.assertEqual(resp.status_code, 422,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              422))

        self.log.info("Deleting project")
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                           project_ids["different_project_id"])
        self.assertEqual(204, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 204))

        # Verifying the app service endpoint with different cluster ids
        self.log.info("Verifying with different cluster ids")
        cluster_ids = {
            'valid_cluster_id': self.app_service_ids[num][0],
            'invalid_cluster_id': self.invalid_id
        }
        for cluster_id in cluster_ids:
            resp = self.capellaAPI.cluster_ops_apis.delete_appservice(self.tenant_id,
                                                                      self.project_id,
                                                                      cluster_ids[cluster_id],
                                                                      self.app_service_ids[num][1])
            if cluster_id == 'valid_cluster_id':
                self.assertEqual(resp.status_code, 202,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              202))
                num = num + 1
            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        # Verifying delete endpoint with different app service ids
        self.log.info("Verifying with different app service ids")
        app_service_ids = {
            'valid_app_service_id': self.app_service_ids[num][1],
            'invalid_app_service_id': self.invalid_id
        }
        for a_s_id in app_service_ids:
            resp = self.capellaAPI.cluster_ops_apis.delete_appservice(self.tenant_id,
                                                                      self.project_id,
                                                                      self.app_service_ids[num][0],
                                                                      app_service_ids[a_s_id])
            if a_s_id == 'valid_app_service_id':
                self.assertEqual(resp.status_code, 202,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              202))
                num = num + 1
            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        # Testing for organization RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"], self.test_users[user]["token"])

            role_response = self.capellaAPIRole.cluster_ops_apis.delete_appservice(
                                                                    self.tenant_id,
                                                                    self.project_id,
                                                                    self.app_service_ids[num][0],
                                                                    self.app_service_ids[num][1])
            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(role_response.status_code, 202,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 202))
                num = num + 1
            else:
                self.assertEqual(role_response.status_code, 403,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 403))

        # Testing for Project Level RBAC roles
        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectViewer", "projectDataReaderWriter",
                          "projectDataReader", "projectOwner", "projectManager"]
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
            self.capellaAPI.org_ops_apis.update_user(self.tenant_id, user['userid'],
                                                     dic["update_info"])

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', user["mailid"],
                                             user["password"], user["token"])

            role_response = self.capellaAPIRole.cluster_ops_apis.delete_appservice(
                                                                    self.tenant_id,
                                                                    self.project_id,
                                                                    self.app_service_ids[num][0],
                                                                    self.app_service_ids[num][1])

            if role in ["projectOwner", "projectManager"]:
                self.assertEqual(202, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(
                                     role_response.status_code, 202))
                num = (num + 1) %  7
            else:
                self.assertEqual(403, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(
                                     role_response.status_code, 403))

            self.log.info("Removing user from project {} with role as {}".format(self.project_id,
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
                             msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 204))

        self.delete_clusters()

    def turn_on_app_service(self):
        self.log.info("Test turn on app service v4 api")

        self.log.info("Creating an app service for the cluster")
        resp = self.capellaAPI.cluster_ops_apis.create_appservice(
                                                            self.tenant_id,
                                                            self.project_id,
                                                            self.cluster_id,
                                                            self.app_services_payload["name"],
                                                            self.app_services_payload["compute"],
                                                            self.app_services_payload["nodes"],
                                                            self.app_services_payload["version"])

        self.assertEqual(201, resp.status_code,
                         msg='FAIL, Outcome: {}, Expected: {}, Reason: {}'.format(
                             resp.status_code, 201, resp.content))
        app_service_id = resp.json()["id"]
        status = self.get_app_service_status(self.tenant_id,
                                             self.project_id,
                                             self.cluster_id,
                                             app_service_id)
        self.assertEqual(status, "healthy",
                         msg='FAIL, Outcome: {}, Expected: {}'.format(status, "healthy"))

        self.log.info("Turning Off App Service")
        resp = self.capellaAPI.cluster_ops_apis.switch_app_service_off(
                                                            self.tenant_id,
                                                            self.project_id,
                                                            self.cluster_id,
                                                            app_service_id)
        self.assertEqual(202, resp.status_code,
                         msg='FAIL, Outcome: {}, Expected: {}, Reason: {}'.format(
                             resp.status_code, 202, resp.content))

        self.wait_until_app_service_turned_on_off(self.cluster_id, app_service_id,
                                                  "turnedOff", "turnOffFailed")
        self.log.info("App Service Successfully Turned Off")

        self.log.info("Verifying status code for turning on an app service")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.switch_app_service_on(
                                                              self.tenant_id,
                                                              self.project_id,
                                                              self.cluster_id,
                                                              app_service_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.switch_app_service_on(
                                                              self.tenant_id,
                                                              self.project_id,
                                                              self.cluster_id,
                                                              app_service_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.switch_app_service_on(
                                                            self.tenant_id,
                                                            self.project_id,
                                                            self.cluster_id,
                                                            app_service_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.switch_app_service_on(
                                                            self.tenant_id,
                                                            self.project_id,
                                                            self.cluster_id,
                                                            app_service_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        # Verifying the create app service endpoint with different organization ids
        self.log.info("Verifying with different tenant ids")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            'invalid_tenant_id': self.invalid_id
        }
        for tenant_id in tenant_ids:
            resp = self.capellaAPI.cluster_ops_apis.switch_app_service_on(
                                                            tenant_ids[tenant_id],
                                                            self.project_id,
                                                            self.cluster_id,
                                                            app_service_id)
            if tenant_id == 'valid_tenant_id':
                self.assertEqual(resp.status_code, 201,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              201))

                resp = self.capellaAPI.cluster_ops_apis.switch_app_service_off(
                                                            tenant_ids[tenant_id],
                                                            self.project_id,
                                                            self.cluster_id,
                                                            app_service_id)
                self.assertEqual(202, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}, Reason: {}'.format(
                                     resp.status_code, 202, resp.content))

                self.wait_until_app_service_turned_on_off(self.cluster_id, app_service_id,
                                                          "turnedOff", "turnOffFailed")
                self.log.info("App Service Successfully Turned Off")

            else:
                self.assertEqual(resp.status_code, 403,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Verifying the create app service endpoint with different project ids
        self.log.info("Creating a project")
        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id, "Project_security")
        self.assertEqual(201, resp.status_code,
                         msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code, 201))
        resp = resp.json()
        project_ids = {
            'valid_project_id': self.project_id,
            'different_project_id': resp['id'],
            'invalid_project_id': self.invalid_id
        }
        for project_id in project_ids:
            resp = self.capellaAPI.cluster_ops_apis.switch_app_service_on(
                                                            self.tenant_id,
                                                            project_ids[project_id],
                                                            self.cluster_id,
                                                            app_service_id)
            if project_id == 'valid_project_id':
                self.assertEqual(resp.status_code, 201,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              201))

                resp = self.capellaAPI.cluster_ops_apis.switch_app_service_off(
                                                            self.tenant_id,
                                                            project_ids[project_id],
                                                            self.cluster_id,
                                                            app_service_id)
                self.assertEqual(202, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}, Reason: {}'.format(
                                     resp.status_code, 202, resp.content))

                self.wait_until_app_service_turned_on_off(self.cluster_id, app_service_id,
                                                          "turnedOff", "turnOffFailed")
                self.log.info("App Service Successfully Turned Off")

            else:
                self.assertEqual(resp.status_code, 422,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              422))
        self.log.info("Deleting project")
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                           project_ids["different_project_id"])
        self.assertEqual(204, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 204))

        # Verifying the create app service endpoint with different cluster ids
        self.log.info("Verifying with different cluster ids")
        cluster_ids = {
            'valid_cluster_id': self.test_cluster_ids[2],
            'invalid_cluster_id': self.invalid_id
        }
        for cluster_id in cluster_ids:
            resp = self.capellaAPI.cluster_ops_apis.switch_app_service_on(
                                                            self.tenant_id,
                                                            self.project_id,
                                                            cluster_ids[cluster_id],
                                                            app_service_id)
            if cluster_id == 'valid_cluster_id':
                self.assertEqual(resp.status_code, 201,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              201))

                resp = self.capellaAPI.cluster_ops_apis.switch_app_service_off(
                                                            self.tenant_id,
                                                            self.project_id,
                                                            cluster_ids[cluster_id],
                                                            app_service_id)
                self.assertEqual(202, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}, Reason: {}'.format(
                                     resp.status_code, 202, resp.content))

                self.wait_until_app_service_turned_on_off(self.cluster_id, app_service_id,
                                                          "turnedOff", "turnOffFailed")
                self.log.info("App Service Successfully Turned Off")

            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))
        # Testing for organization RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"], self.test_users[user]["token"])

            role_response = self.capellaAPIRole.cluster_ops_apis.switch_app_service_on(
                                                            self.tenant_id,
                                                            self.project_id,
                                                            self.cluster_id,
                                                            app_service_id)

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(role_response.status_code, 201,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 201))

                resp = self.capellaAPI.cluster_ops_apis.switch_app_service_off(
                                                            self.tenant_id,
                                                            self.project_id,
                                                            self.cluster_id,
                                                            app_service_id)
                self.assertEqual(202, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}, Reason: {}'.format(
                                     resp.status_code, 202, resp.content))

                self.wait_until_app_service_turned_on_off(self.cluster_id, app_service_id,
                                                          "turnedOff", "turnOffFailed")
                self.log.info("App Service Successfully Turned Off")

            else:
                self.assertEqual(role_response.status_code, 403,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 403))

        # Testing for Project Level RBAC roles
        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectOwner", "projectViewer", "projectManager",
                         "projectDataReaderWriter", "projectDataReader"]
        user = self.test_users["User3"]
        num = 4
        for role in project_roles:
            self.log.info("Creating apiKeys for role {}".format(role))
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
                                             user["password"], user["token"])

            role_response = self.capellaAPIRole.cluster_ops_apis.switch_app_service_on(
                                                            self.tenant_id,
                                                            self.project_id,
                                                            self.cluster_id,
                                                            app_service_id)
            if role in ['projectOwner', 'projectManager']:
                self.assertEqual(201, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(
                                     role_response.status_code, 201))
                resp = self.capellaAPI.cluster_ops_apis.switch_app_service_off(
                                                            self.tenant_id,
                                                            self.project_id,
                                                            self.cluster_id,
                                                            app_service_id)
                self.assertEqual(202, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}, Reason: {}'.format(
                                     resp.status_code, 202, resp.content))

                self.wait_until_app_service_turned_on_off(self.cluster_id, app_service_id,
                                                          "turnedOff", "turnOffFailed")
                self.log.info("App Service Successfully Turned Off")

            else:
                self.assertEqual(403, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(
                                     role_response.status_code, 403))

            self.log.info("Removing user from project {} with role as {}".format(self.project_id,
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
                             msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 204))

    def turn_off_app_service(self):
        self.log.info("Test turn off app service v4 api")

        self.log.info("Creating an app service for the cluster")
        resp = self.capellaAPI.cluster_ops_apis.create_appservice(
                                                            self.tenant_id,
                                                            self.project_id,
                                                            self.cluster_id,
                                                            self.app_services_payload["name"],
                                                            self.app_services_payload["compute"],
                                                            self.app_services_payload["nodes"],
                                                            self.app_services_payload["version"])

        self.assertEqual(201, resp.status_code,
                         msg='FAIL, Outcome: {}, Expected: {}, Reason: {}'.format(
                             resp.status_code, 201, resp.content))
        app_service_id = resp.json()["id"]
        self.wait_until_app_service_turned_on_off(self.cluster_id, app_service_id, "deploying",
                                                  "healthy")

        self.log.info("Verifying status code for turning on an app service")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.switch_app_service_off(
                                                              self.tenant_id,
                                                              self.project_id,
                                                              self.cluster_id,
                                                              app_service_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.switch_app_service_off(
                                                              self.tenant_id,
                                                              self.project_id,
                                                              self.cluster_id,
                                                              app_service_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.switch_app_service_off(
                                                            self.tenant_id,
                                                            self.project_id,
                                                            self.cluster_id,
                                                            app_service_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.switch_app_service_off(
                                                            self.tenant_id,
                                                            self.project_id,
                                                            self.cluster_id,
                                                            app_service_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        # Verifying the create app service endpoint with different organization ids
        self.log.info("Verifying with different tenant ids")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            'invalid_tenant_id': self.invalid_id
        }
        for tenant_id in tenant_ids:
            resp = self.capellaAPI.cluster_ops_apis.switch_app_service_off(
                                                            tenant_ids[tenant_id],
                                                            self.project_id,
                                                            self.cluster_id,
                                                            app_service_id)
            if tenant_id == 'valid_tenant_id':
                self.assertEqual(resp.status_code, 201,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              201))

                self.wait_until_app_service_turned_on_off(self.cluster_id, app_service_id,
                                                          "turnedOff", "turnedOffFailed")

                resp = self.capellaAPI.cluster_ops_apis.switch_app_service_on(
                                                            tenant_ids[tenant_id],
                                                            self.project_id,
                                                            self.cluster_id,
                                                            app_service_id)
                self.assertEqual(202, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}, Reason: {}'.format(
                                     resp.status_code, 202, resp.content))

                self.wait_until_app_service_turned_on_off(self.cluster_id, app_service_id,
                                                          "healthy", "turnOnFailed")
                self.log.info("App Service Successfully Turned Off")

            else:
                self.assertEqual(resp.status_code, 403,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Verifying the create app service endpoint with different project ids
        self.log.info("Creating a project")
        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id, "Project_security")
        self.assertEqual(201, resp.status_code,
                         msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code, 201))
        resp = resp.json()
        project_ids = {
            'valid_project_id': self.project_id,
            'different_project_id': resp['id'],
            'invalid_project_id': self.invalid_id
        }
        for project_id in project_ids:
            resp = self.capellaAPI.cluster_ops_apis.switch_app_service_on(
                                                            self.tenant_id,
                                                            project_ids[project_id],
                                                            self.cluster_id,
                                                            app_service_id)
            if project_id == 'valid_project_id':
                self.assertEqual(resp.status_code, 201,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              201))

                resp = self.capellaAPI.cluster_ops_apis.switch_app_service_off(
                                                            self.tenant_id,
                                                            project_ids[project_id],
                                                            self.cluster_id,
                                                            app_service_id)
                self.assertEqual(202, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}, Reason: {}'.format(
                                     resp.status_code, 202, resp.content))

                self.wait_until_app_service_turned_on_off(self.cluster_id, app_service_id,
                                                          "turnedOff", "turnOffFailed")
                self.log.info("App Service Successfully Turned Off")

            else:
                self.assertEqual(resp.status_code, 422,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              422))
        self.log.info("Deleting project")
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                           project_ids["different_project_id"])
        self.assertEqual(204, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 204))

        # Verifying the create app service endpoint with different cluster ids
        self.log.info("Verifying with different cluster ids")
        cluster_ids = {
            'valid_cluster_id': self.test_cluster_ids[2],
            'invalid_cluster_id': self.invalid_id
        }
        for cluster_id in cluster_ids:
            resp = self.capellaAPI.cluster_ops_apis.switch_app_service_on(
                                                            self.tenant_id,
                                                            self.project_id,
                                                            cluster_ids[cluster_id],
                                                            app_service_id)
            if cluster_id == 'valid_cluster_id':
                self.assertEqual(resp.status_code, 201,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              201))

                resp = self.capellaAPI.cluster_ops_apis.switch_app_service_off(
                                                            self.tenant_id,
                                                            self.project_id,
                                                            cluster_ids[cluster_id],
                                                            app_service_id)
                self.assertEqual(202, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}, Reason: {}'.format(
                                     resp.status_code, 202, resp.content))

                self.wait_until_app_service_turned_on_off(self.cluster_id, app_service_id,
                                                          "turnedOff", "turnOffFailed")
                self.log.info("App Service Successfully Turned Off")

            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))
        # Testing for organization RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"], self.test_users[user]["token"])

            role_response = self.capellaAPIRole.cluster_ops_apis.switch_app_service_on(
                                                            self.tenant_id,
                                                            self.project_id,
                                                            self.cluster_id,
                                                            app_service_id)

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(role_response.status_code, 201,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 201))

                resp = self.capellaAPI.cluster_ops_apis.switch_app_service_off(
                                                            self.tenant_id,
                                                            self.project_id,
                                                            self.cluster_id,
                                                            app_service_id)
                self.assertEqual(202, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}, Reason: {}'.format(
                                     resp.status_code, 202, resp.content))

                self.wait_until_app_service_turned_on_off(self.cluster_id, app_service_id,
                                                          "turnedOff", "turnOffFailed")
                self.log.info("App Service Successfully Turned Off")

            else:
                self.assertEqual(role_response.status_code, 403,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 403))

        # Testing for Project Level RBAC roles
        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectOwner", "projectViewer", "projectManager",
                         "projectDataReaderWriter", "projectDataReader"]
        user = self.test_users["User3"]
        num = 4
        for role in project_roles:
            self.log.info("Creating apiKeys for role {}".format(role))
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
                                             user["password"], user["token"])

            role_response = self.capellaAPIRole.cluster_ops_apis.switch_app_service_on(
                                                            self.tenant_id,
                                                            self.project_id,
                                                            self.cluster_id,
                                                            app_service_id)

            if role in ['projectOwner', 'projectManager']:
                self.assertEqual(201, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(
                                     role_response.status_code, 201))
                resp = self.capellaAPI.cluster_ops_apis.switch_app_service_off(
                                                            self.tenant_id,
                                                            self.project_id,
                                                            self.cluster_id,
                                                            app_service_id)
                self.assertEqual(202, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}, Reason: {}'.format(
                                     resp.status_code, 202, resp.content))

                self.wait_until_app_service_turned_on_off(self.cluster_id, app_service_id,
                                                          "turnedOff", "turnOffFailed")
                self.log.info("App Service Successfully Turned Off")

            else:
                self.assertEqual(403, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(
                                     role_response.status_code, 403))

            self.log.info("Removing user from project {} with role as {}".format(self.project_id,
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
                             msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 204))