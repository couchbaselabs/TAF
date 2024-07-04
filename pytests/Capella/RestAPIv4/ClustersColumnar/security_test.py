from pytests.Capella.RestAPIv4.security_base import SecurityBase
from capellaAPI.capella.columnar.ColumnarAPI_v4 import ColumnarAPIs

class SecurityTest(SecurityBase):

    def setUp(self):
        SecurityBase.setUp(self)

    def tearDown(self):
        super(SecurityTest, self).tearDown()

    def get_create_cluster_payload(self):
        payload = {
            "name": self.prefix + "Columnar-Cluster",
            "cloudProvider": "aws",
            "compute": {
                "cpu": 4,
                "ram": 16
            },
            "region": "us-east-1",
            "nodes": 1,
            "support": {
                "plan": "enterprise",
                "timezone": "ET"
            },
            "availability": {
                "type": "single"
            }
        }

        return payload

    def test_create_columnar_cluster(self):
        self.log.info("Verifying the create schedule cluster on/off authentication with different "
                      "test cases")
        self.log.info("     1. Empty Credentials")
        self.columnarAPI.bearer_token = ""

        payload = self.get_create_cluster_payload()
        resp = self.columnarAPI.create_analytics_cluster(self.tenant_id, self.project_id,
                                                         payload["name"],
                                                         payload["cloudProvider"],
                                                         payload["compute"],
                                                         payload["region"],
                                                         payload["nodes"],
                                                         payload["support"],
                                                         payload["availability"])
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Invalid Token")
        self.columnarAPI.bearer_token = self.invalid_id
        resp = self.columnarAPI.create_analytics_cluster(self.tenant_id, self.project_id,
                                                         payload["name"],
                                                         payload["cloudProvider"],
                                                         payload["compute"],
                                                         payload["region"],
                                                         payload["nodes"],
                                                         payload["support"],
                                                         payload["availability"])
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("Verify the endpoints with different organizations id")
        tenant_ids = {
            "valid_tenant_id": self.tenant_id,
            "invalid_tenant_id": self.invalid_id
        }

        for tenant_id in tenant_ids:
            resp = self.columnarAPI.create_analytics_cluster(tenant_ids[tenant_id], self.project_id,
                                                             payload["name"],
                                                             payload["cloudProvider"],
                                                             payload["compute"],
                                                             payload["region"],
                                                             payload["nodes"],
                                                             payload["support"],
                                                             payload["availability"])
            if tenant_id == "valid_tenant_id":
                self.assertEqual(202, resp.status_code,
                                 msg="FAIL: Outcome: {}, Expected: {}, Reason: {}".format(
                                     resp.status_code, 202, resp.content))
                instance_id = resp.json()["id"]
                delete_resp = self.columnarAPI.delete_analytics_cluster(self.tenant_id,
                                                                        self.project_id,
                                                                        instance_id)
                self.assertEqual(202, delete_resp.status_code,
                                 msg="FAIL: Outcome: {}, Expected: {}, Reason: {}".format(
                                     delete_resp.status_code, 202, delete_resp.content))
                self.wait_for_columnar_cluster_to_be_deleted(instance_id, 1800)
            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}, Reason: {}'.format(
                                     resp.status_code, 403, resp.status_code))

        # Trying out with different project ids
        self.log.info("Verifying the endpoint access with different projects")
        project_ids = {
            'valid_project_id': self.project_id,
            'invalid_project_id': self.invalid_id
        }

        for project_id in project_ids:
            resp = self.columnarAPI.create_analytics_cluster(self.tenant_id, project_ids[project_id],
                                                             payload["name"],
                                                             payload["cloudProvider"],
                                                             payload["compute"],
                                                             payload["region"],
                                                             payload["nodes"],
                                                             payload["support"],
                                                             payload["availability"])
            if project_id == "valid_project_id":
                self.assertEqual(202, resp.status_code,
                                 msg="FAIL: Outcome: {}, Expected: {}, Reason: {}".format(
                                     resp.status_code, 202, resp.content))
                instance_id = resp.json()["id"]
                delete_resp = self.columnarAPI.delete_analytics_cluster(self.tenant_id,
                                                                        self.project_id,
                                                                        instance_id)
                self.assertEqual(202, delete_resp.status_code,
                                 msg="FAIL: Outcome: {}, Expected: {}, Reason: {}".format(
                                     delete_resp.status_code, 202, delete_resp.content))
                self.wait_for_columnar_cluster_to_be_deleted(instance_id, 1800)
            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}, Reason: {}'.format(
                                     resp.status_code, 404, resp.content))

        # Testing for Organizations RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))

            self.columnarAPIrole = ColumnarAPIs("https://" + self.url, "", "",
                                                self.test_users[user]['token'])
            role_response = self.columnarAPIrole.create_analytics_cluster(self.tenant_id, self.project_id,
                                                                          payload["name"],
                                                                          payload["cloudProvider"],
                                                                          payload["compute"],
                                                                          payload["region"],
                                                                          payload["nodes"],
                                                                          payload["support"],
                                                                          payload["availability"])
            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(202, role_response.status_code,
                                 msg="FAIL: Outcome: {}, Expected: {}, Reason: {}".format(
                                     role_response.status_code, 202, resp.content))
                instance_id = role_response.json()["id"]
                delete_resp = self.columnarAPI.delete_analytics_cluster(self.tenant_id,
                                                                        self.project_id,
                                                                        instance_id)
                self.assertEqual(202, delete_resp.status_code,
                                 msg="FAIL: Outcome: {}, Expected: {}, Reason: {}".format(
                                     delete_resp.status_code, 202, delete_resp.content))
                self.wait_for_columnar_cluster_to_be_deleted(instance_id, 1800)
            else:
                self.assertEqual(role_response.status_code, 403,
                                 msg='FAIL: Outcome:{}, Expected:{}, Role: {}'.format(
                                     role_response.status_code, 403, self.test_users[user]["role"]))

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

            self.columnarAPIrole = ColumnarAPIs("https://" + self.url, "", "",
                                                user['token'])
            role_response = self.columnarAPIrole.create_analytics_cluster(self.tenant_id, self.project_id,
                                                                          payload["name"],
                                                                          payload["cloudProvider"],
                                                                          payload["compute"],
                                                                          payload["region"],
                                                                          payload["nodes"],
                                                                          payload["support"],
                                                                          payload["availability"])
            if role in ["projectOwner", "projectManager"]:
                self.assertEqual(202, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}, Reason: {}".format(
                                     role_response.status_code, 202, role_response.content))
                instance_id = role_response.json()["id"]
                delete_resp = self.columnarAPI.delete_analytics_cluster(self.tenant_id,
                                                                        self.project_id,
                                                                        instance_id)
                self.assertEqual(202, delete_resp.status_code,
                                 msg="FAIL: Outcome: {}, Expected: {}, Reason: {}".format(
                                     delete_resp.status_code, 202, delete_resp.content))
                self.wait_for_columnar_cluster_to_be_deleted(instance_id, 1800)
            else:
                self.assertEqual(403, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}, Reason: {}".format(
                                     role_response.status_code, 403, role_response.content))

            resp = self.capellaAPI.org_ops_apis.delete_api_key(self.tenant_id, api_key_id)
            self.assertEqual(204, resp.status_code,
                             msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                          204))

    def test_list_org_level_columnar_clusters(self):
        self.log.info("Verifying the create schedule cluster on/off authentication with different "
                      "test cases")
        self.log.info("     1. Empty Credentials")
        self.columnarAPI.bearer_token = ""

        payload = self.get_create_cluster_payload()
        resp = self.columnarAPI.list_organization_level_analytics_clusters(self.tenant_id,
                                                                           1, 25,
                                                                           "name", "asc")
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Invalid Token")
        self.columnarAPI.bearer_token = self.invalid_id
        resp = self.columnarAPI.list_organization_level_analytics_clusters(self.tenant_id,
                                                                           1, 25,
                                                                           "name", "asc")
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("Verify the endpoints with different organizations id")
        tenant_ids = {
            "valid_tenant_id": self.tenant_id,
            "invalid_tenant_id": self.invalid_id
        }

        for tenant_id in tenant_ids:
            resp = self.columnarAPI.list_organization_level_analytics_clusters(tenant_ids[tenant_id],
                                                                               1, 25,
                                                                               "name", "asc")
            if tenant_id == "valid_tenant_id":
                self.assertEqual(200, resp.status_code,
                                 msg="FAIL: Outcome: {}, Expected: {}, Reason: {}".format(
                                     resp.status_code, 200, resp.content))
            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}, Reason: {}'.format(
                                     resp.status_code, 403, resp.status_code))

        # Testing for Organizations RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))

            self.columnarAPIrole = ColumnarAPIs("https://" + self.url, "", "",
                                                self.test_users[user]['token'])
            role_response = self.columnarAPIrole.list_organization_level_analytics_clusters(self.tenant_id,
                                                                                            1, 25,
                                                                                            "name", "asc")
            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(200, role_response.status_code,
                                 msg="FAIL: Outcome: {}, Expected: {}, Reason: {}".format(
                                     role_response.status_code, 200, resp.content))
            else:
                self.assertEqual(role_response.status_code, 403,
                                 msg='FAIL: Outcome:{}, Expected:{}, Role: {}'.format(
                                     role_response.status_code, 403, self.test_users[user]["role"]))

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

            self.columnarAPIrole = ColumnarAPIs("https://" + self.url, "", "",
                                                user['token'])
            role_response = self.columnarAPIrole.list_organization_level_analytics_clusters(self.tenant_id,
                                                                                            1, 25,
                                                                                            "name", "asc")
            if role in ["projectOwner", "projectViewer", "projectManager",
                         "projectDataReaderWriter", "projectDataReader"]:
                self.assertEqual(200, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}, Reason: {}".format(
                                     role_response.status_code, 200, role_response.content))
            else:
                self.assertEqual(403, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}, Reason: {}".format(
                                     role_response.status_code, 403, role_response.content))

            resp = self.capellaAPI.org_ops_apis.delete_api_key(self.tenant_id, api_key_id)
            self.assertEqual(204, resp.status_code,
                             msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                          204))

    def test_list_project_level_columnar_clusters(self):
        self.log.info("Verifying the create schedule cluster on/off authentication with different "
                      "test cases")
        self.log.info("     1. Empty Credentials")
        self.columnarAPI.bearer_token = ""

        payload = self.get_create_cluster_payload()
        resp = self.columnarAPI.list_project_level_analytics_clusters(self.tenant_id,
                                                                      self.project_id,
                                                                      1, 25,
                                                                      "name", "asc")
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Invalid Token")
        self.columnarAPI.bearer_token = self.invalid_id
        resp = self.columnarAPI.list_project_level_analytics_clusters(self.tenant_id,
                                                                      self.project_id,
                                                                      1, 25,
                                                                      "name", "asc")
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("Verify the endpoints with different organizations id")
        tenant_ids = {
            "valid_tenant_id": self.tenant_id,
            "invalid_tenant_id": self.invalid_id
        }

        for tenant_id in tenant_ids:
            resp = self.columnarAPI.list_project_level_analytics_clusters(tenant_ids[tenant_id],
                                                                          self.project_id,
                                                                          1, 25,
                                                                          "name", "asc")
            if tenant_id == "valid_tenant_id":
                self.assertEqual(200, resp.status_code,
                                 msg="FAIL: Outcome: {}, Expected: {}, Reason: {}".format(
                                     resp.status_code, 200, resp.content))
            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}, Reason: {}'.format(
                                     resp.status_code, 403, resp.status_code))

        # Trying out with different project ids
        self.log.info("Verifying the endpoint access with different projects")
        project_ids = {
            'valid_project_id': self.project_id,
            'invalid_project_id': self.invalid_id
        }

        for project_id in project_ids:
            resp = self.columnarAPI.list_project_level_analytics_clusters(self.tenant_id,
                                                                          project_ids[project_id],
                                                                          1, 25,
                                                                          "name", "asc")
            if project_id == "valid_project_id":
                self.assertEqual(200, resp.status_code,
                                 msg="FAIL: Outcome: {}, Expected: {}, Reason: {}".format(
                                     resp.status_code, 200, resp.content))
            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}, Reason: {}'.format(
                                     resp.status_code, 404, resp.content))

        # Testing for Organizations RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))

            self.columnarAPIrole = ColumnarAPIs("https://" + self.url, "", "",
                                                self.test_users[user]['token'])
            role_response = self.columnarAPIrole.list_project_level_analytics_clusters(self.tenant_id,
                                                                                       self.project_id,
                                                                                       1, 25,
                                                                                       "name", "asc")
            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(200, role_response.status_code,
                                 msg="FAIL: Outcome: {}, Expected: {}, Reason: {}".format(
                                     role_response.status_code, 200, resp.content))
            else:
                self.assertEqual(role_response.status_code, 403,
                                 msg='FAIL: Outcome:{}, Expected:{}, Role: {}'.format(
                                     role_response.status_code, 403, self.test_users[user]["role"]))

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

            self.columnarAPIrole = ColumnarAPIs("https://" + self.url, "", "",
                                                user['token'])
            role_response = self.columnarAPIrole.list_project_level_analytics_clusters(self.tenant_id,
                                                                                       self.project_id,
                                                                                       1, 25,
                                                                                       "name", "asc")
            if role in ["projectOwner", "projectViewer", "projectManager",
                         "projectDataReaderWriter", "projectDataReader"]:
                self.assertEqual(200, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}, Reason: {}".format(
                                     role_response.status_code, 200, role_response.content))
            else:
                self.assertEqual(403, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}, Reason: {}".format(
                                     role_response.status_code, 403, role_response.content))

            resp = self.capellaAPI.org_ops_apis.delete_api_key(self.tenant_id, api_key_id)
            self.assertEqual(204, resp.status_code,
                             msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                          204))

    def test_update_columnar_cluster(self):
        self.log.info("Verifying the create schedule cluster on/off authentication with different "
                      "test cases")
        self.log.info("     1. Empty Credentials")
        self.columnarAPI.bearer_token = ""

        payload = self.get_create_cluster_payload()
        resp = self.columnarAPI.update_analytics_cluster(self.tenant_id, self.project_id,
                                                         self.instance_id,
                                                         payload["name"],
                                                         payload["nodes"])
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Invalid Token")
        self.columnarAPI.bearer_token = self.invalid_id
        resp = self.columnarAPI.update_analytics_cluster(self.tenant_id, self.project_id,
                                                         self.instance_id,
                                                         payload["name"],
                                                         payload["nodes"])
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("Verify the endpoints with different organizations id")
        tenant_ids = {
            "valid_tenant_id": self.tenant_id,
            "invalid_tenant_id": self.invalid_id
        }

        for tenant_id in tenant_ids:
            resp = self.columnarAPI.update_analytics_cluster(tenant_ids[tenant_id], self.project_id,
                                                             self.instance_id,
                                                             payload["name"],
                                                             payload["nodes"])
            if tenant_id == "valid_tenant_id":
                self.assertEqual(204, resp.status_code,
                                 msg="FAIL: Outcome: {}, Expected: {}, Reason: {}".format(
                                     resp.status_code, 204, resp.content))
            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}, Reason: {}'.format(
                                     resp.status_code, 403, resp.status_code))

        # Trying out with different project ids
        self.log.info("Verifying the endpoint access with different projects")
        self.log.info("Creating a project")
        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id,
                                                           "Columnar On-Off schedule project")
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
            resp = self.columnarAPI.update_analytics_cluster(self.tenant_id,
                                                             project_ids[project_id],
                                                             self.instance_id,
                                                             payload["name"],
                                                             payload["nodes"])
            if project_id == "valid_project_id":
                self.assertEqual(204, resp.status_code,
                                 msg="FAIL: Outcome: {}, Expected: {}, Reason: {}".format(
                                     resp.status_code, 204, resp.content))
            else:
                self.assertEqual(resp.status_code, 422,
                                 msg='FAIL: Outcome: {}, Expected: {}, Reason: {}'.format(
                                     resp.status_code, 422, resp.content))

        self.log.info("Deleting project")
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                           project_ids["different_project_id"])
        self.assertEqual(204, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 204))

        # Testing for Organizations RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))

            self.columnarAPIrole = ColumnarAPIs("https://" + self.url, "", "",
                                                self.test_users[user]['token'])
            role_response = self.columnarAPIrole.update_analytics_cluster(self.tenant_id,
                                                             self.project_id,
                                                             self.instance_id,
                                                             payload["name"],
                                                             payload["nodes"])
            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(204, role_response.status_code,
                                 msg="FAIL: Outcome: {}, Expected: {}, Reason: {}".format(
                                     role_response.status_code, 204, resp.content))
            else:
                self.assertEqual(role_response.status_code, 403,
                                 msg='FAIL: Outcome:{}, Expected:{}, Role: {}'.format(
                                     role_response.status_code, 403, self.test_users[user]["role"]))

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

            self.columnarAPIrole = ColumnarAPIs("https://" + self.url, "", "",
                                                user['token'])
            role_response = self.columnarAPIrole.update_analytics_cluster(self.tenant_id,
                                                             self.project_id,
                                                             self.instance_id,
                                                             payload["name"],
                                                             payload["nodes"])
            if role in ["projectOwner", "projectManager"]:
                self.assertEqual(204, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}, Reason: {}".format(
                                     role_response.status_code, 204, role_response.content))
            else:
                self.assertEqual(403, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}, Reason: {}".format(
                                     role_response.status_code, 403, role_response.content))

            resp = self.capellaAPI.org_ops_apis.delete_api_key(self.tenant_id, api_key_id)
            self.assertEqual(204, resp.status_code,
                             msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                          204))

    def test_delete_columnar_cluster(self):
        payload = self.get_create_cluster_payload()
        resp = self.columnarAPI.create_analytics_cluster(self.tenant_id, self.project_id,
                                                         payload["name"],
                                                         payload["cloudProvider"],
                                                         payload["compute"],
                                                         payload["region"],
                                                         payload["nodes"],
                                                         payload["support"],
                                                         payload["availability"])
        self.assertEqual(202, resp.status_code,
                                 msg="FAIL: Outcome: {}, Expected: {}, Reason: {}".format(
                                     resp.status_code, 202, resp.content))
        instance_id = resp.json()["id"]
        self.wait_for_columnar_cluster_to_be_healthy(instance_id, 1800)

        self.log.info("Verifying the create schedule cluster on/off authentication with different "
                      "test cases")
        self.log.info("     1. Empty Credentials")
        self.columnarAPI.bearer_token = ""
        resp = self.columnarAPI.delete_analytics_cluster(self.tenant_id,
                                                         self.project_id,
                                                         instance_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Invalid Token")
        self.columnarAPI.bearer_token = self.invalid_id
        resp = self.columnarAPI.delete_analytics_cluster(self.tenant_id,
                                                         self.project_id,
                                                         instance_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("Verify the endpoints with different organizations id")
        tenant_ids = {
            "valid_tenant_id": self.tenant_id,
            "invalid_tenant_id": self.invalid_id
        }

        for tenant_id in tenant_ids:
            resp = self.columnarAPI.delete_analytics_cluster(tenant_ids[tenant_id],
                                                             self.project_id,
                                                             instance_id)
            if tenant_id == "valid_tenant_id":
                self.assertEqual(202, resp.status_code,
                                 msg="FAIL: Outcome: {}, Expected: {}, Reason: {}".format(
                                     resp.status_code, 202, resp.content))
                self.wait_for_columnar_cluster_to_be_deleted(instance_id, 1800)
                resp = self.columnarAPI.create_analytics_cluster(self.tenant_id, self.project_id,
                                                         payload["name"],
                                                         payload["cloudProvider"],
                                                         payload["compute"],
                                                         payload["region"],
                                                         payload["nodes"],
                                                         payload["support"],
                                                         payload["availability"])
                self.assertEqual(202, resp.status_code,
                                        msg="FAIL: Outcome: {}, Expected: {}, Reason: {}".format(
                                            resp.status_code, 202, resp.content))
                instance_id = resp.json()["id"]
                self.wait_for_columnar_cluster_to_be_healthy(instance_id, 1800)
            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}, Reason: {}'.format(
                                     resp.status_code, 403, resp.status_code))

        # Trying out with different project ids
        self.log.info("Verifying the endpoint access with different projects")
        self.log.info("Creating a project")
        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id,
                                                           "Columnar On-Off schedule project")
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
            resp = self.columnarAPI.delete_analytics_cluster(self.tenant_id,
                                                             project_ids[project_id],
                                                             instance_id)
            if project_id == "valid_project_id":
                self.assertEqual(202, resp.status_code,
                                 msg="FAIL: Outcome: {}, Expected: {}, Reason: {}".format(
                                     resp.status_code, 202, resp.content))
                self.wait_for_columnar_cluster_to_be_deleted(instance_id, 1800)
                resp = self.columnarAPI.create_analytics_cluster(self.tenant_id, self.project_id,
                                                         payload["name"],
                                                         payload["cloudProvider"],
                                                         payload["compute"],
                                                         payload["region"],
                                                         payload["nodes"],
                                                         payload["support"],
                                                         payload["availability"])
                self.assertEqual(202, resp.status_code,
                                        msg="FAIL: Outcome: {}, Expected: {}, Reason: {}".format(
                                            resp.status_code, 202, resp.content))
                instance_id = resp.json()["id"]
                self.wait_for_columnar_cluster_to_be_healthy(instance_id, 1800)
            else:
                self.assertEqual(resp.status_code, 422,
                                 msg='FAIL: Outcome: {}, Expected: {}, Reason: {}'.format(
                                     resp.status_code, 422, resp.content))

        self.log.info("Deleting project")
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                           project_ids["different_project_id"])
        self.assertEqual(204, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 204))

        # Testing for Organizations RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))

            self.columnarAPIrole = ColumnarAPIs("https://" + self.url, "", "",
                                                self.test_users[user]['token'])
            role_response = self.columnarAPIrole.delete_analytics_cluster(self.tenant_id,
                                                                          self.project_id,
                                                                          instance_id)
            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(202, role_response.status_code,
                                 msg="FAIL: Outcome: {}, Expected: {}, Reason: {}".format(
                                     role_response.status_code, 202, resp.content))
                self.wait_for_columnar_cluster_to_be_deleted(instance_id, 1800)
                resp = self.columnarAPI.create_analytics_cluster(self.tenant_id, self.project_id,
                                                         payload["name"],
                                                         payload["cloudProvider"],
                                                         payload["compute"],
                                                         payload["region"],
                                                         payload["nodes"],
                                                         payload["support"],
                                                         payload["availability"])
                self.assertEqual(202, resp.status_code,
                                        msg="FAIL: Outcome: {}, Expected: {}, Reason: {}".format(
                                            resp.status_code, 202, resp.content))
                instance_id = resp.json()["id"]
                self.wait_for_columnar_cluster_to_be_healthy(instance_id, 1800)
            else:
                self.assertEqual(role_response.status_code, 403,
                                 msg='FAIL: Outcome:{}, Expected:{}, Role: {}'.format(
                                     role_response.status_code, 403, self.test_users[user]["role"]))

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

            self.columnarAPIrole = ColumnarAPIs("https://" + self.url, "", "",
                                                user['token'])
            role_response = self.columnarAPIrole.delete_analytics_cluster(self.tenant_id,
                                                                          self.project_id,
                                                                          instance_id)
            if role in ["projectOwner", "projectManager"]:
                self.assertEqual(202, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}, Reason: {}".format(
                                     role_response.status_code, 202, role_response.content))
                self.wait_for_columnar_cluster_to_be_deleted(instance_id, 1800)
                resp = self.columnarAPI.create_analytics_cluster(self.tenant_id, self.project_id,
                                                         payload["name"],
                                                         payload["cloudProvider"],
                                                         payload["compute"],
                                                         payload["region"],
                                                         payload["nodes"],
                                                         payload["support"],
                                                         payload["availability"])
                self.assertEqual(202, resp.status_code,
                                        msg="FAIL: Outcome: {}, Expected: {}, Reason: {}".format(
                                            resp.status_code, 202, resp.content))
                instance_id = resp.json()["id"]
                self.wait_for_columnar_cluster_to_be_healthy(instance_id, 1800)
            else:
                self.assertEqual(403, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}, Reason: {}".format(
                                     role_response.status_code, 403, role_response.content))

            resp = self.capellaAPI.org_ops_apis.delete_api_key(self.tenant_id, api_key_id)
            self.assertEqual(204, resp.status_code,
                             msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                          204))