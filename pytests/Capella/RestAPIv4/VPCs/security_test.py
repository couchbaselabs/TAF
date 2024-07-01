import time
import json
import random
from pytests.Capella.RestAPIv4.security_base import SecurityBase
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI

class SecurityTest(SecurityBase):
    cidr = "10.0.0.0"

    def setUp(self):
        SecurityBase.setUp(self)

    def tearDown(self):
        super(SecurityTest, self).tearDown()

    def test_create_network_peer(self):
        self.log.info("Verify creating vpc peering for v4 APIs")

        payload = {
            "name": "VPCPeerTestAWS",
            "providerType": "aws",
            "providerConfig": {
                "accountID": "123456789110",
                "vpcId": "vpc-00ff00ff00ff0f",
                "region": "us-east-1",
                "cidr": "10.1.0.0/23"
            }
        }

        self.log.info("Verifying the create cluster endpoint authentication with different test "
                      "cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""

        resp = self.capellaAPI.cluster_ops_apis.create_network_peer(self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    payload["name"],
                                                                    payload["providerConfig"],
                                                                    payload["providerType"])
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.create_network_peer(self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    payload["name"],
                                                                    payload["providerConfig"],
                                                                    payload["providerType"])
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.create_network_peer(self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    payload["name"],
                                                                    payload["providerConfig"],
                                                                    payload["providerType"])
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.create_network_peer(self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    payload["name"],
                                                                    payload["providerConfig"],
                                                                    payload["providerType"])
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
            resp = self.capellaAPI.cluster_ops_apis.create_network_peer(tenant_ids[tenant_id],
                                                                        self.project_id,
                                                                        self.cluster_id,
                                                                        payload["name"],
                                                                        payload["providerConfig"],
                                                                        payload["providerType"])

            if tenant_id == "valid_tenant_id":
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {} Failed to create '
                                     'network peer'.format(resp.status_code, 201))

            else:
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

        for project_id in project_ids:
            resp = self.capellaAPI.cluster_ops_apis.create_network_peer(self.tenant_id,
                                                                        project_ids[project_id],
                                                                        self.cluster_id,
                                                                        payload["name"],
                                                                        payload["providerConfig"],
                                                                        payload["providerType"])

            if project_id == 'valid_project_id':
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {} Failed to delete the '
                                     'cluster'.format(resp.status_code, 201))

            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        self.log.info("Deleting project")
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                           project_ids["different_project_id"])
        self.assertEqual(204, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 204))

        cluster_ids = {
            'valid_cluster_id': self.project_id,
            'invalid_cluster_id': self.invalid_id
        }

        for cluster_id in cluster_ids:
            resp = self.capellaAPI.cluster_ops_apis.create_network_peer(self.tenant_id,
                                                                        self.project_id,
                                                                        cluster_ids[cluster_id],
                                                                        payload["name"],
                                                                        payload["providerConfig"],
                                                                        payload["providerType"])

            if cluster_ids == 'valid_cluster_id':
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {} Failed to delete the '
                                     'cluster'.format(resp.status_code, 201))

            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        # Testing for Organizations RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"], self.test_users[user]['token'])

            role_response = self.capellaAPIRole.cluster_ops_apis.create_network_peer(
                                                                self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                payload["name"],
                                                                payload["providerConfig"],
                                                                payload["providerType"])

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(201, role_response.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {} Failed to delete the '
                                     'cluster'.format(role_response.status_code, 201))

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

            role_response = self.capellaAPIRole.cluster_ops_apis.create_network_peer(
                                                                    self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    payload["name"],
                                                                    payload["providerConfig"],
                                                                    payload["providerType"])

            if role == "projectOwner":
                self.assertEqual(201, role_response.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {} Failed to delete the '
                                     'cluster'.format(role_response.status_code, 201))

            else:
                self.assertEqual(role_response.status_code, 403,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
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

    def test_list_network_peers(self):
        self.log.info("Verify listing vpc peering for v4 APIs")

        self.log.info("Verifying the list vpc endpoint authentication with different test "
                      "cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""

        resp = self.capellaAPI.cluster_ops_apis.list_network_peer_records(self.tenant_id,
                                                                          self.project_id,
                                                                          self.cluster_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.list_network_peer_records(self.tenant_id,
                                                                          self.project_id,
                                                                          self.cluster_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.list_network_peer_records(self.tenant_id,
                                                                          self.project_id,
                                                                          self.cluster_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.list_network_peer_records(self.tenant_id,
                                                                          self.project_id,
                                                                          self.cluster_id)
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
            resp = self.capellaAPI.cluster_ops_apis.list_network_peer_records(
                                                                            tenant_ids[tenant_id],
                                                                            self.project_id,
                                                                            self.cluster_id)

            if tenant_id == "valid_tenant_id":
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {} Failed to create '
                                     'network peer'.format(resp.status_code, 201))

            else:
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

        for project_id in project_ids:
            resp = self.capellaAPI.cluster_ops_apis.list_network_peer_records(
                                                                            self.tenant_id,
                                                                            project_ids[project_id],
                                                                            self.cluster_id)

            if project_id == 'valid_project_id':
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {} Failed to delete the '
                                     'cluster'.format(resp.status_code, 201))

            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        self.log.info("Deleting project")
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                           project_ids["different_project_id"])
        self.assertEqual(204, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 204))

        cluster_ids = {
            'valid_cluster_id': self.project_id,
            'invalid_cluster_id': self.invalid_id
        }

        for cluster_id in cluster_ids:
            resp = self.capellaAPI.cluster_ops_apis.list_network_peer_records(
                                                                            self.tenant_id,
                                                                            self.project_id,
                                                                            cluster_ids[cluster_id])

            if cluster_ids == 'valid_cluster_id':
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {} Failed to delete the '
                                     'cluster'.format(resp.status_code, 201))

            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        # Testing for Organizations RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"], self.test_users[user]['token'])

            role_response = self.capellaAPIRole.cluster_ops_apis.list_network_peer_records(
                                                                            self.tenant_id,
                                                                            self.project_id,
                                                                            self.cluster_id)

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(201, role_response.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {} Failed to delete the '
                                     'cluster'.format(role_response.status_code, 201))

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

            role_response = self.capellaAPIRole.cluster_ops_apis.list_network_peer_records(
                                                                            self.tenant_id,
                                                                            self.project_id,
                                                                            self.cluster_id)

            if role == "projectOwner":
                self.assertEqual(201, role_response.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {} Failed to delete the '
                                     'cluster'.format(role_response.status_code, 201))

            else:
                self.assertEqual(role_response.status_code, 403,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
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

    def test_fetch_network_peer_record_info(self):
        self.log.info("Verify listing vpc peering for v4 APIs")

        payload = {
            "name": "VPCPeerTestAWS",
            "providerType": "aws",
            "providerConfig": {
                "accountID": "123456789110",
                "vpcId": "vpc-00ff00ff00ff0f",
                "region": "us-east-1",
                "cidr": "10.1.0.0/23"
            }
        }

        resp = self.capellaAPI.cluster_ops_apis.create_network_peer(self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    payload["name"],
                                                                    payload["providerConfig"],
                                                                    payload["providerType"])
        vpcId = resp.content.json()['id']

        self.log.info("Verifying the list vpc endpoint authentication with different test "
                      "cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""

        resp = self.capellaAPI.cluster_ops_apis.fetch_network_peer_record_info(self.tenant_id,
                                                                               self.project_id,
                                                                               self.cluster_id,
                                                                               vpcId)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.fetch_network_peer_record_info(self.tenant_id,
                                                                               self.project_id,
                                                                               self.cluster_id,
                                                                               vpcId)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.fetch_network_peer_record_info(self.tenant_id,
                                                                               self.project_id,
                                                                               self.cluster_id,
                                                                               vpcId)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.fetch_network_peer_record_info(self.tenant_id,
                                                                               self.project_id,
                                                                               self.cluster_id,
                                                                               vpcId)
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
            resp = self.capellaAPI.cluster_ops_apis.fetch_network_peer_record_info(
                                                                            tenant_ids[tenant_id],
                                                                            self.project_id,
                                                                            self.cluster_id,
                                                                            vpcId)

            if tenant_id == "valid_tenant_id":
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {} Failed to create '
                                     'network peer'.format(resp.status_code, 201))

            else:
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

        for project_id in project_ids:
            resp = self.capellaAPI.cluster_ops_apis.fetch_network_peer_record_info(
                                                                            self.tenant_id,
                                                                            project_ids[project_id],
                                                                            self.cluster_id,
                                                                            vpcId)

            if project_id == 'valid_project_id':
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {} Failed to delete the '
                                     'cluster'.format(resp.status_code, 201))

            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        self.log.info("Deleting project")
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                           project_ids["different_project_id"])
        self.assertEqual(204, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 204))

        cluster_ids = {
            'valid_cluster_id': self.project_id,
            'invalid_cluster_id': self.invalid_id
        }

        for cluster_id in cluster_ids:
            resp = self.capellaAPI.cluster_ops_apis.fetch_network_peer_record_info(
                                                                            self.tenant_id,
                                                                            self.project_id,
                                                                            cluster_ids[cluster_id],
                                                                            vpcId)

            if cluster_ids == 'valid_cluster_id':
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {} Failed to delete the '
                                     'cluster'.format(resp.status_code, 201))

            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        # Testing for Organizations RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"], self.test_users[user]['token'])

            role_response = self.capellaAPIRole.cluster_ops_apis.fetch_network_peer_record_info(
                                                                            self.tenant_id,
                                                                            self.project_id,
                                                                            self.cluster_id,
                                                                            vpcId)

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(201, role_response.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {} Failed to delete the '
                                     'cluster'.format(role_response.status_code, 201))

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

            role_response = self.capellaAPIRole.cluster_ops_apis.fetch_network_peer_record_info(
                                                                            self.tenant_id,
                                                                            self.project_id,
                                                                            self.cluster_id,
                                                                            vpcId)

            if role == "projectOwner":
                self.assertEqual(201, role_response.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {} Failed to delete the '
                                     'cluster'.format(role_response.status_code, 201))

            else:
                self.assertEqual(role_response.status_code, 403,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
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

    def test_delete_network_peer(self):
        self.log.info("Verify listing vpc peering for v4 APIs")

        payload = {
            "name": "VPCPeerTestAWS",
            "providerType": "aws",
            "providerConfig": {
                "accountID": "123456789110",
                "vpcId": "vpc-00ff00ff00ff0f",
                "region": "us-east-1",
                "cidr": "10.1.0.0/23"
            }
        }

        resp = self.capellaAPI.cluster_ops_apis.create_network_peer(self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    payload["name"],
                                                                    payload["providerConfig"],
                                                                    payload["providerType"])
        vpcId = resp.content.json()['id']

        self.log.info("Verifying the list vpc endpoint authentication with different test "
                      "cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""

        resp = self.capellaAPI.cluster_ops_apis.delete_network_peer(self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    vpcId)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.delete_network_peer(self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    vpcId)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.delete_network_peer(self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    vpcId)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.delete_network_peer(self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    vpcId)
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
            resp = self.capellaAPI.cluster_ops_apis.delete_network_peer(
                                                                    tenant_ids[tenant_id],
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    vpcId)

            if tenant_id == "valid_tenant_id":
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {} Failed to delete '
                                     'network peer'.format(resp.status_code, 201))

            else:
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

        for project_id in project_ids:
            resp = self.capellaAPI.cluster_ops_apis.delete_network_peer(
                                                                    self.tenant_id,
                                                                    project_ids[project_id],
                                                                    self.cluster_id,
                                                                    vpcId)

            if project_id == 'valid_project_id':
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {} Failed to delete the '
                                     'vpc peering'.format(resp.status_code, 201))

            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        self.log.info("Deleting project")
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                           project_ids["different_project_id"])
        self.assertEqual(204, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 204))

        cluster_ids = {
            'valid_cluster_id': self.project_id,
            'invalid_cluster_id': self.invalid_id
        }

        for cluster_id in cluster_ids:
            resp = self.capellaAPI.cluster_ops_apis.delete_network_peer(
                                                                    self.tenant_id,
                                                                    self.project_id,
                                                                    cluster_ids[cluster_id],
                                                                    vpcId)

            if cluster_ids == 'valid_cluster_id':
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {} Failed to delete the '
                                     'vpc peering'.format(resp.status_code, 201))

            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        # Testing for Organizations RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"], self.test_users[user]['token'])

            role_response = self.capellaAPIRole.cluster_ops_apis.delete_network_peer(
                                                                    self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    vpcId)

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(201, role_response.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {} Failed to delete the '
                                     'vpc peering'.format(role_response.status_code, 201))

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

            role_response = self.capellaAPIRole.cluster_ops_apis.delete_network_peer(
                                                                    self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    vpcId)

            if role == "projectOwner":
                self.assertEqual(201, role_response.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {} Failed to delete the '
                                     'vpc peering'.format(role_response.status_code, 201))

            else:
                self.assertEqual(role_response.status_code, 403,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
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