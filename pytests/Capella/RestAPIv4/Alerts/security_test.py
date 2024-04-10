import time
import json
import random
from pytests.Capella.RestAPIv4.security_base import SecurityBase
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI
from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI as CapellaAPIv2

class SecurityTest(SecurityBase):
    cidr = "10.0.0.0"

    def setUp(self):
        SecurityBase.setUp(self)

    def tearDown(self):
        super(SecurityTest, self).tearDown()

    def create_alert_payload(self):
        payload = {
            "kind": "webhook",
            "name": "Slack Webhook",
            "config": {
                "webhook": {
                    "method": "POST",
                    "url": "https://dev214215.service-now.com/api/1326135/capella_webhook_api_dev",
                    "headers": {},
                    "exclude": {
                        "clusters": [],
                        "appservices": []
                    },
                    "basicAuth": {
                        "user": "capella-dev-webhooks-user",
                        "password": "1LA7bZ4_DfD;0s)w}EK>PKqzGR3#kJZ,w}"
                    }
                }
            }
        }

        return payload

    def test_create_alert(self):
        self.log.info("Verify creating webhooks alert for v4 APIs")
        payload = self.create_alert_payload()
        self.log.info("Verifying the create webhook alert endpoint authentication with different "
                      "test cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""

        resp = self.capellaAPI.cluster_ops_apis.create_alert(self.tenant_id,
                                                             self.project_id,
                                                             payload["kind"],
                                                             payload["config"],
                                                             payload["name"])
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.create_alert(self.tenant_id,
                                                             self.project_id,
                                                             payload["kind"],
                                                             payload["config"],
                                                             payload["name"])
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.create_alert(self.tenant_id,
                                                             self.project_id,
                                                             payload["kind"],
                                                             payload["config"],
                                                             payload["name"])
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.create_alert(self.tenant_id,
                                                             self.project_id,
                                                             payload["kind"],
                                                             payload["config"],
                                                             payload["name"])
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
            resp = self.capellaAPI.cluster_ops_apis.create_alert(tenant_ids[tenant_id],
                                                                 self.project_id,
                                                                 payload["kind"],
                                                                 payload["config"],
                                                                 payload["name"])

            if tenant_id == "valid_tenant_id":
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              201))

                alert_id = resp.json()["id"]
                resp = self.capellaAPI.cluster_ops_apis.delete_alert(tenant_ids[tenant_id],
                                                                     self.project_id,
                                                                     alert_id)
                self.assertEqual(resp.status_code, 204,
                                 msg="FAIL, Outcome: {}, Expected: {}".format(resp.status_code,
                                                                              200))

            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Trying out with different project ids
        self.log.info("Verifying the endpoint access with different projects")
        project_ids = {
            'valid_project_id': self.project_id,
            'invalid_project_id': self.invalid_id
        }

        for project_id in project_ids:
            resp = self.capellaAPI.cluster_ops_apis.create_alert(self.tenant_id,
                                                                 project_ids[project_id],
                                                                 payload["kind"],
                                                                 payload["config"],
                                                                 payload["name"])

            if project_id == 'valid_project_id':
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              201))
                alert_id = resp.json()["id"]
                resp = self.capellaAPI.cluster_ops_apis.delete_alert(self.tenant_id,
                                                                     project_ids[project_id],
                                                                     alert_id)
                self.assertEqual(resp.status_code, 204,
                                 msg="FAIL, Outcome: {}, Expected: {}".format(resp.status_code,
                                                                              200))

            else:
                self.assertEqual(404, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        # Testing for Organizations RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"], self.test_users[user]['token'])

            resp = self.capellaAPIRole.cluster_ops_apis.create_alert(self.tenant_id,
                                                                     self.project_id,
                                                                     payload["kind"],
                                                                     payload["config"],
                                                                     payload["name"])

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              201))

                alert_id = resp.json()["id"]
                resp = self.capellaAPIRole.cluster_ops_apis.delete_alert(self.tenant_id,
                                                                         self.project_id,
                                                                         alert_id)
                self.assertEqual(resp.status_code, 204,
                                 msg="FAIL, Outcome: {}, Expected: {}".format(resp.status_code,
                                                                              200))
            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Testing for Project Level RBAC roles
        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectOwner", "projectViewer", "projectManager",
                         "projectDataReaderWriter", "projectDataReader"]
        user = self.test_users["User3"]
        collection_num = 6
        for role in project_roles:
            collection_name = "collection_" + str(collection_num)
            collection_num = collection_num + 1
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

            resp = self.capellaAPIRole.cluster_ops_apis.create_alert(self.tenant_id,
                                                                     self.project_id,
                                                                     payload["kind"],
                                                                     payload["config"],
                                                                     payload["name"])

            if role in ["projectOwner", "projectManager"]:
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              201))

                alert_id = resp.json()["id"]
                resp = self.capellaAPIRole.cluster_ops_apis.delete_alert(self.tenant_id,
                                                                         self.project_id,
                                                                         alert_id)
                self.assertEqual(resp.status_code, 204,
                                 msg="FAIL, Outcome: {}, Expected: {}".format(resp.status_code,
                                                                              200))

            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

            self.log.info(
                "Removing user from project {} with role as {}".format(self.project_id, role))
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

    def test_list_alerts(self):
        self.log.info("Verify list alerts endpoints for v4 APIs")

        self.log.info("Verifying the list alerts endpoint authentication with different test "
                      "cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""

        resp = self.capellaAPI.cluster_ops_apis.list_alerts(self.tenant_id,
                                                            self.project_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.list_alerts(self.tenant_id,
                                                            self.project_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.list_alerts(self.tenant_id,
                                                            self.project_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.list_alerts(self.tenant_id,
                                                            self.project_id)

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
            resp = self.capellaAPI.cluster_ops_apis.list_alerts(tenant_ids[tenant_id],
                                                                self.project_id)

            if tenant_id == "valid_tenant_id":
                self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))

            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Trying out with different project ids
        self.log.info("Verifying the endpoint access with different projects")
        project_ids = {
            'valid_project_id': self.project_id,
            'invalid_project_id': self.invalid_id
        }

        for project_id in project_ids:
            resp = self.capellaAPI.cluster_ops_apis.list_alerts(self.tenant_id,
                                                                project_ids[project_id])

            if project_id == 'valid_project_id':
                self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))

            else:
                self.assertEqual(404, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        # Testing for Organizations RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"], self.test_users[user]['token'])

            resp = self.capellaAPIRole.cluster_ops_apis.list_alerts(self.tenant_id,
                                                                    self.project_id)

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))
            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

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

            resp = self.capellaAPIRole.cluster_ops_apis.list_alerts(self.tenant_id,
                                                                    self.project_id)

            if role in ["projectOwner", "projectViewer", "projectManager"]:
                self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))

            else:
                self.assertEqual(403, resp.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(
                                     resp.status_code, 403))

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

    def test_get_alert(self):
        self.log.info("Verify fetch alert info endpoints for v4 APIs")
        payload = self.create_alert_payload()
        resp = self.capellaAPI.cluster_ops_apis.create_alert(self.tenant_id,
                                                             self.project_id,
                                                             payload["kind"],
                                                             payload["config"],
                                                             payload["name"])

        alert_id = resp.json()["id"]

        self.log.info("Verifying the fetch alert info endpoint authentication with different test "
                      "cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""

        resp = self.capellaAPI.cluster_ops_apis.fetch_alert_info(self.tenant_id,
                                                                 self.project_id,
                                                                 alert_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.fetch_alert_info(self.tenant_id,
                                                                 self.project_id,
                                                                 alert_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.fetch_alert_info(self.tenant_id,
                                                                 self.project_id,
                                                                 alert_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.fetch_alert_info(self.tenant_id,
                                                                 self.project_id,
                                                                 alert_id)

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
            resp = self.capellaAPI.cluster_ops_apis.fetch_alert_info(tenant_ids[tenant_id],
                                                                     self.project_id,
                                                                     alert_id)

            if tenant_id == "valid_tenant_id":
                self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))

            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Trying out with different project ids
        self.log.info("Verifying the endpoint access with different projects")
        project_ids = {
            'valid_project_id': self.project_id,
            'invalid_project_id': self.invalid_id
        }

        for project_id in project_ids:
            resp = self.capellaAPI.cluster_ops_apis.fetch_alert_info(self.tenant_id,
                                                                     project_ids[project_id],
                                                                     alert_id)

            if project_id == 'valid_project_id':
                self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))

            else:
                self.assertEqual(404, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        # Testing for Organizations RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"], self.test_users[user]['token'])

            resp = self.capellaAPIRole.cluster_ops_apis.fetch_alert_info(self.tenant_id,
                                                                         self.project_id,
                                                                         alert_id)

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))
            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

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

            resp = self.capellaAPIRole.cluster_ops_apis.fetch_alert_info(self.tenant_id,
                                                                         self.project_id,
                                                                         alert_id)

            if role in ["projectOwner", "projectManager", "projectViewer"]:
                self.assertEqual(200, resp.status_code,
                                msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                            200))

            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

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

    def test_update_alert(self):
        self.log.info("Verify delete alert endpoint for v4 APIs")
        payload = self.create_alert_payload()
        resp = self.capellaAPI.cluster_ops_apis.create_alert(self.tenant_id,
                                                             self.project_id,
                                                             payload["kind"],
                                                             payload["config"],
                                                             payload["name"])

        alert_id = resp.json()["id"]

        self.log.info("Verifying the delete alert endpoint authentication with different test "
                      "cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""

        resp = self.capellaAPI.cluster_ops_apis.update_alert(self.tenant_id,
                                                             self.project_id,
                                                             alert_id,
                                                             payload["kind"],
                                                             payload["config"],
                                                             payload["name"] + "_1")
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.update_alert(self.tenant_id,
                                                             self.project_id,
                                                             alert_id,
                                                             payload["kind"],
                                                             payload["config"],
                                                             payload["name"] + "_1")
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.update_alert(self.tenant_id,
                                                             self.project_id,
                                                             alert_id,
                                                             payload["kind"],
                                                             payload["config"],
                                                             payload["name"] + "_1")
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.update_alert(self.tenant_id,
                                                             self.project_id,
                                                             alert_id,
                                                             payload["kind"],
                                                             payload["config"],
                                                             payload["name"] + "_1")

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
            resp = self.capellaAPI.cluster_ops_apis.update_alert(tenant_ids[tenant_id],
                                                                 self.project_id,
                                                                 alert_id,
                                                                 payload["kind"],
                                                                 payload["config"],
                                                                 payload["name"] + "_1")

            if tenant_id == "valid_tenant_id":
                self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              204))

            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Trying out with different project ids
        self.log.info("Verifying the endpoint access with different projects")
        project_ids = {
            'valid_project_id': self.project_id,
            'invalid_project_id': self.invalid_id
        }

        for project_id in project_ids:
            resp = self.capellaAPI.cluster_ops_apis.update_alert(self.tenant_id,
                                                                 project_ids[project_id],
                                                                 alert_id,
                                                                 payload["kind"],
                                                                 payload["config"],
                                                                 payload["name"] + "_2")

            if project_id == 'valid_project_id':
                self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              204))

            else:
                self.assertEqual(404, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        # Testing for Organizations RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"], self.test_users[user]['token'])

            resp = self.capellaAPIRole.cluster_ops_apis.update_alert(self.tenant_id,
                                                                     self.project_id,
                                                                     alert_id,
                                                                     payload["kind"],
                                                                     payload["config"],
                                                                     payload["name"] + "_3")

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              204))

            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Testing for Project Level RBAC roles
        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectOwner", "projectViewer", "projectManager",
                         "projectDataReaderWriter", "projectDataReader"]
        user = self.test_users["User3"]

        for role in project_roles:
            num = "_4"
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

            resp = self.capellaAPIRole.cluster_ops_apis.update_alert(self.tenant_id,
                                                                     self.project_id,
                                                                     alert_id,
                                                                     payload["kind"],
                                                                     payload["config"],
                                                                     payload["name"] + num)

            if role in ["projectOwner", "projectManager"]:
                self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              204))
                num = "_6"

            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

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

    def test_delete_alert(self):
        self.log.info("Verify delete alert endpoint for v4 APIs")
        payload = self.create_alert_payload()
        resp = self.capellaAPI.cluster_ops_apis.create_alert(self.tenant_id,
                                                             self.project_id,
                                                             payload["kind"],
                                                             payload["config"],
                                                             payload["name"])

        alert_id = resp.json()["id"]

        self.log.info("Verifying the delete alert endpoint authentication with different test "
                      "cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""

        resp = self.capellaAPI.cluster_ops_apis.delete_alert(self.tenant_id,
                                                             self.project_id,
                                                             alert_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.delete_alert(self.tenant_id,
                                                             self.project_id,
                                                             alert_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.delete_alert(self.tenant_id,
                                                             self.project_id,
                                                             alert_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.delete_alert(self.tenant_id,
                                                             self.project_id,
                                                             alert_id)

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
            resp = self.capellaAPI.cluster_ops_apis.delete_alert(tenant_ids[tenant_id],
                                                                 self.project_id,
                                                                 alert_id)

            if tenant_id == "valid_tenant_id":
                self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              204))

                resp = self.capellaAPI.cluster_ops_apis.create_alert(self.tenant_id,
                                                                     self.project_id,
                                                                     payload["kind"],
                                                                     payload["config"],
                                                                     payload["name"])

                alert_id = resp.json()["id"]

            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Trying out with different project ids
        self.log.info("Verifying the endpoint access with different projects")
        project_ids = {
            'valid_project_id': self.project_id,
            'invalid_project_id': self.invalid_id
        }

        for project_id in project_ids:
            resp = self.capellaAPI.cluster_ops_apis.delete_alert(self.tenant_id,
                                                                 project_ids[project_id],
                                                                 alert_id)

            if project_id == 'valid_project_id':
                self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              204))

                resp = self.capellaAPI.cluster_ops_apis.create_alert(self.tenant_id,
                                                                     project_ids[project_id],
                                                                     payload["kind"],
                                                                     payload["config"],
                                                                     payload["name"])

                alert_id = resp.json()["id"]

            else:
                self.assertEqual(404, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        # Testing for Organizations RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"], self.test_users[user]['token'])

            resp = self.capellaAPIRole.cluster_ops_apis.delete_alert(self.tenant_id,
                                                                     self.project_id,
                                                                     alert_id)

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              204))

                resp = self.capellaAPI.cluster_ops_apis.create_alert(self.tenant_id,
                                                                     self.project_id,
                                                                     payload["kind"],
                                                                     payload["config"],
                                                                     payload["name"])

                alert_id = resp.json()["id"]

            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

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

            resp = self.capellaAPIRole.cluster_ops_apis.delete_alert(self.tenant_id,
                                                                     self.project_id,
                                                                     alert_id)

            if role in ["projectOwner", "projectManager"]:
                self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              204))

                resp = self.capellaAPI.cluster_ops_apis.create_alert(self.tenant_id,
                                                                     self.project_id,
                                                                     payload["kind"],
                                                                     payload["config"],
                                                                     payload["name"])

                alert_id = resp.json()["id"]

            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

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