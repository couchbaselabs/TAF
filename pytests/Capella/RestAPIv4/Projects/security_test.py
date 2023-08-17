import json
from pytests.basetestcase import BaseTestCase
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI


class SecurityTest(BaseTestCase):

    def setUp(self):
        BaseTestCase.setUp(self)
        self.url = self.input.capella.get("pod")
        self.user = self.input.capella.get("capella_user")
        self.passwd = self.input.capella.get("capella_pwd")
        self.tenant_id = self.input.capella.get("tenant_id")
        self.project_id = self.tenant.project_id
        self.cluster_id = self.cluster.id
        self.invalid_id = "00000000-0000-0000-0000-000000000000"
        self.capellaAPI = CapellaAPI("https://" + self.url, '', '', self.user, self.passwd)
        self.commonCapellaAPI = self.capellaAPI.cluster_ops_apis
        resp = self.capellaAPI.create_control_plane_api_key(self.tenant_id, 'init api keys')
        resp = resp.json()
        self.capellaAPI.cluster_ops_apis.SECRET = resp['secretKey']
        self.capellaAPI.cluster_ops_apis.ACCESS = resp['accessKey']
        self.capellaAPI.org_ops_apis.SECRET = resp['secretKey']
        self.capellaAPI.org_ops_apis.ACCESS = resp['accessKey']
        self.capellaAPI.cluster_ops_apis.SECRETINI = resp['secretKey']
        self.capellaAPI.cluster_ops_apis.ACCESSINI = resp['accessKey']
        self.capellaAPI.org_ops_apis.SECRETINI = resp['secretKey']
        self.capellaAPI.org_ops_apis.ACCESSINI = resp['accessKey']
        if self.input.capella.get("test_users"):
            self.test_users = json.loads(self.input.capella.get("test_users"))
        else:
            self.test_users = {"User1": {"password": self.passwd, "mailid": self.user,
                                         "role": "organizationOwner"}}

        for user in self.test_users:
            resp = self.capellaAPI.org_ops_apis.create_api_key(
                self.tenant_id, 'API Key for role {}'.format(self.test_users[user]["role"]),
                organizationRoles=[self.test_users[user]["role"]],
                expiry=1)
            resp = resp.json()
            self.test_users[user]["accessKey"] = resp['accessKey']
            self.test_users[user]["secretKey"] = resp['secretKey']

    def tearDown(self):
        super(SecurityTest, self).tearDown()

    def reset_api_keys(self):
        self.capellaAPI.cluster_ops_apis.SECRET = self.capellaAPI.cluster_ops_apis.SECRETINI
        self.capellaAPI.cluster_ops_apis.ACCESS = self.capellaAPI.cluster_ops_apis.ACCESSINI
        self.capellaAPI.org_ops_apis.SECRET = self.capellaAPI.org_ops_apis.SECRETINI
        self.capellaAPI.org_ops_apis.ACCESS = self.capellaAPI.org_ops_apis.ACCESSINI

    def test_post_create_project(self):
        self.log.info("     I. POST Create Project")
        """
        /v4/organizations/{organizationId}/projects
        {
          "name": "My Project",
          "description": "My awesome project"
        }
        """
        self.log.info("Verifying status code for creating a project")

        self.log.info("Verifying the endpoint authentication with different test cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        resp = self.capellaAPI.cluster_ops_apis.create_project(self.tenant_id, "Project_security")
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        resp = self.capellaAPI.cluster_ops_apis.create_project(self.tenant_id, "Project_security")
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        resp = self.capellaAPI.cluster_ops_apis.create_project(self.tenant_id, "Project_security")
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        resp = self.capellaAPI.cluster_ops_apis.create_project(self.tenant_id, "Project_security")
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("    5. Cross Combination of Valid AccessKey and SecretKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.test_users["User1"]["accessKey"]
        self.capellaAPI.cluster_ops_apis.SECRET = self.test_users["User2"]["secretKey"]
        resp = self.capellaAPI.cluster_ops_apis.create_project(self.tenant_id, "Project_security")
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        # Verifying the create project endpoint with different organization ids
        self.log.info("Verifying with different tenant ids")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            'invalid_tenant_id': self.invalid_id
        }
        for tenant_id in tenant_ids:
            resp = self.capellaAPI.cluster_ops_apis.create_project(tenant_ids[tenant_id],
                                                                   "Project_security")

            if tenant_id == 'valid_tenant_id':
                self.assertEqual(resp.status_code, 201,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              201))
                temp_proj_id = resp.json()["id"]
                self.log.info("Deleting project")
                resp = self.capellaAPI.org_ops_apis.delete_project(tenant_ids[tenant_id],
                                                                   temp_proj_id)
                self.assertEqual(204, resp.status_code,
                                 msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code,
                                                                              204))
            else:
                self.assertEqual(resp.status_code, 403,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Testing for organization RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))
            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"])

            self.capellaAPIRole.cluster_ops_apis.SECRET = self.test_users[user]["secretKey"]
            self.capellaAPIRole.cluster_ops_apis.ACCESS = self.test_users[user]["accessKey"]

            role_response = self.capellaAPIRole.cluster_ops_apis.create_project(self.tenant_id,
                                                                                "Project_security")
            if self.test_users[user]["role"] == "organizationOwner" or \
                    self.test_users[user]["role"] == "projectCreator":
                self.assertEqual(role_response.status_code, 201,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 201))
                temp_proj_id = role_response.json()["id"]
                self.log.info("Deleting project")
                resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                                   temp_proj_id)
                self.assertEqual(204, resp.status_code,
                                 msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code,
                                                                              204))

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
                                             user["password"])
            self.capellaAPIRole.cluster_ops_apis.SECRET = user["secretKey"]
            self.capellaAPIRole.cluster_ops_apis.ACCESS = user["accessKey"]

            role_response = self.capellaAPIRole.cluster_ops_apis.create_project(self.tenant_id,
                                                                                "Project_security")

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

    def test_get_list_project(self):
        self.log.info("     II. GET List Project")
        """
        /v4/organizations/{organizationId}/projects
        """
        self.log.info("Verifying status code for retrieving information of a particular project")

        self.log.info("Verifying the endpoint authentication with different test cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        resp = self.capellaAPI.cluster_ops_apis.list_projects(self.tenant_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        resp = self.capellaAPI.cluster_ops_apis.list_projects(self.tenant_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        resp = self.capellaAPI.cluster_ops_apis.list_projects(self.tenant_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        resp = self.capellaAPI.cluster_ops_apis.list_projects(self.tenant_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("    5. Cross Combination of Valid AccessKey and SecretKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.test_users["User1"]["accessKey"]
        self.capellaAPI.cluster_ops_apis.SECRET = self.test_users["User2"]["secretKey"]
        resp = self.capellaAPI.cluster_ops_apis.list_projects(self.tenant_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        # Verifying the list project endpoint with different organization ids
        self.log.info("Verifying with different tenant ids")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            'invalid_tenant_id': self.invalid_id
        }
        for tenant_id in tenant_ids:
            resp = self.capellaAPI.cluster_ops_apis.list_projects(self.tenant_id)

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
                user]["mailid"], self.test_users[user]["password"])

            self.capellaAPIRole.cluster_ops_apis.SECRET = self.test_users[user]["secretKey"]
            self.capellaAPIRole.cluster_ops_apis.ACCESS = self.test_users[user]["accessKey"]

            role_response = self.capellaAPIRole.cluster_ops_apis.list_projects(self.tenant_id)
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
                                             user["password"])
            self.capellaAPIRole.cluster_ops_apis.SECRET = user["secretKey"]
            self.capellaAPIRole.cluster_ops_apis.ACCESS = user["accessKey"]

            role_response = self.capellaAPIRole.cluster_ops_apis.list_projects(self.tenant_id)

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

    def test_get_get_project(self):
        self.log.info("     III. GET Get Project")
        """
        /v4/organizations/{organizationId}/projects/{projectId}
        """
        self.log.info("Verifying status code for retrieving information of a particular project")

        self.log.info("Verifying the endpoint authentication with different test cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        resp = self.capellaAPI.cluster_ops_apis.get_project(self.tenant_id,
                                                            self.project_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        resp = self.capellaAPI.cluster_ops_apis.get_project(self.tenant_id,
                                                            self.project_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        resp = self.capellaAPI.cluster_ops_apis.get_project(self.tenant_id,
                                                            self.project_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        resp = self.capellaAPI.cluster_ops_apis.get_project(self.tenant_id,
                                                            self.project_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("    5. Cross Combination of Valid AccessKey and SecretKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.test_users["User1"]["accessKey"]
        self.capellaAPI.cluster_ops_apis.SECRET = self.test_users["User2"]["secretKey"]
        resp = self.capellaAPI.cluster_ops_apis.get_project(self.tenant_id,
                                                            self.project_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        # Verifying the get project endpoint with different organization ids
        self.log.info("Verifying with different tenant ids")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            'invalid_tenant_id': self.invalid_id
        }
        for tenant_id in tenant_ids:
            resp = self.capellaAPI.cluster_ops_apis.get_project(tenant_ids[tenant_id],
                                                                self.project_id)

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
            resp = self.capellaAPI.cluster_ops_apis.get_project(self.tenant_id,
                                                                project_ids[project_id])

            if project_id == 'valid_project_id' or project_id == 'different_project_id':
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))
            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

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
                user]["mailid"], self.test_users[user]["password"])

            self.capellaAPIRole.cluster_ops_apis.SECRET = self.test_users[user]["secretKey"]
            self.capellaAPIRole.cluster_ops_apis.ACCESS = self.test_users[user]["accessKey"]

            role_response = self.capellaAPIRole.cluster_ops_apis.get_project(self.tenant_id,
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
                                             user["password"])
            self.capellaAPIRole.cluster_ops_apis.SECRET = user["secretKey"]
            self.capellaAPIRole.cluster_ops_apis.ACCESS = user["accessKey"]

            role_response = self.capellaAPIRole.cluster_ops_apis.get_project(self.tenant_id,
                                                                             self.project_id)

            if role == "projectOwner":
                self.assertEqual(200, role_response,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(role_response, 200))
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

    def test_put_update_project(self):
        self.log.info("     IV. PUT Update Project")
        """
        /v4/organizations/{organizationId}/projects/{projectId}
        {
            "name": "My-New-Project",
            "description": "The extended description of my awesome project."
        }
        """
        self.log.info("Verifying status code for updating information of a particular project")

        self.log.info("Verifying the endpoint authentication with different test cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        resp = self.capellaAPI.cluster_ops_apis.update_project(self.tenant_id,
                                                               self.project_id, "Project_security1")
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        resp = self.capellaAPI.cluster_ops_apis.update_project(self.tenant_id,
                                                               self.project_id, "Project_security1")
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        resp = self.capellaAPI.cluster_ops_apis.update_project(self.tenant_id,
                                                               self.project_id, "Project_security1")
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        resp = self.capellaAPI.cluster_ops_apis.update_project(self.tenant_id,
                                                               self.project_id, "Project_security1")
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("    5. Cross Combination of Valid AccessKey and SecretKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.test_users["User1"]["accessKey"]
        self.capellaAPI.cluster_ops_apis.SECRET = self.test_users["User2"]["secretKey"]
        resp = self.capellaAPI.cluster_ops_apis.update_project(self.tenant_id,
                                                               self.project_id, "Project_security1")
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        # Verifying the update project endpoint with different organization ids
        self.log.info("Verifying with different tenant ids")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            'invalid_tenant_id': self.invalid_id
        }
        for tenant_id in tenant_ids:
            resp = self.capellaAPI.cluster_ops_apis.update_project(tenant_ids[tenant_id],
                                                                   self.project_id,
                                                                   "Project_security1")

            if tenant_id == 'valid_tenant_id':
                self.assertEqual(resp.status_code, 204,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              204))
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
            resp = self.capellaAPI.cluster_ops_apis.update_project(self.tenant_id,
                                                                   project_ids[project_id],
                                                                   "Project_security1")

            if project_id == 'valid_project_id' or project_id == 'different_project_id':
                self.assertEqual(resp.status_code, 204,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              204))
            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

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
                user]["mailid"], self.test_users[user]["password"])

            self.capellaAPIRole.cluster_ops_apis.SECRET = self.test_users[user]["secretKey"]
            self.capellaAPIRole.cluster_ops_apis.ACCESS = self.test_users[user]["accessKey"]

            role_response = self.capellaAPIRole.cluster_ops_apis.update_project(self.tenant_id,
                                                                                self.project_id,
                                                                                "Project_security1")
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
                                             user["password"])
            self.capellaAPIRole.cluster_ops_apis.SECRET = user["secretKey"]
            self.capellaAPIRole.cluster_ops_apis.ACCESS = user["accessKey"]

            role_response = self.capellaAPIRole.cluster_ops_apis.update_project(self.tenant_id,
                                                                                self.project_id,
                                                                                "Project_security1")

            if role == "projectOwner":
                self.assertEqual(204, role_response,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(role_response, 204))
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

    def test_del_delete_project(self):
        self.log.info("     V. DEL Delete Project")
        """
        /v4/organizations/{organizationId}/projects/{projectId}
        """
        self.log.info("Verifying status code for deleting a particular project")

        self.log.info("Verifying the endpoint authentication with different test cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        resp = self.capellaAPI.cluster_ops_apis.delete_project(self.tenant_id,
                                                               self.project_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        resp = self.capellaAPI.cluster_ops_apis.delete_project(self.tenant_id,
                                                               self.project_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        resp = self.capellaAPI.cluster_ops_apis.delete_project(self.tenant_id,
                                                               self.project_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        resp = self.capellaAPI.cluster_ops_apis.delete_project(self.tenant_id,
                                                               self.project_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("    5. Cross Combination of Valid AccessKey and SecretKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.test_users["User1"]["accessKey"]
        self.capellaAPI.cluster_ops_apis.SECRET = self.test_users["User2"]["secretKey"]
        resp = self.capellaAPI.cluster_ops_apis.delete_project(self.tenant_id,
                                                               self.project_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        # Verifying delete project endpoint with different organization ids
        self.log.info("Verifying with different tenant ids")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            'invalid_tenant_id': self.invalid_id
        }
        for tenant_id in tenant_ids:
            if tenant_id == 'valid_tenant_id':
                self.log.info("Creating a project")
                resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id,
                                                                   "Project_security")
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              201))
                temp_proj_id = resp.json()["id"]
                resp = self.capellaAPI.cluster_ops_apis.delete_project(tenant_ids[tenant_id],
                                                                       temp_proj_id)
            else:
                resp = self.capellaAPI.cluster_ops_apis.delete_project(tenant_ids[tenant_id],
                                                                       self.project_id)

            if tenant_id == 'valid_tenant_id':
                self.assertEqual(resp.status_code, 204,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              204))
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
            'different_project_id': resp['id'],
            'invalid_project_id': self.invalid_id
        }
        for project_id in project_ids:
            resp = self.capellaAPI.cluster_ops_apis.delete_project(self.tenant_id,
                                                                   project_ids[project_id])

            if project_id == 'different_project_id':
                self.assertEqual(resp.status_code, 204,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              204))
            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        # Testing for organization RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))
            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"])

            self.capellaAPIRole.cluster_ops_apis.SECRET = self.test_users[user]["secretKey"]
            self.capellaAPIRole.cluster_ops_apis.ACCESS = self.test_users[user]["accessKey"]

            self.log.info("Creating a project")
            resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id, "Project_security")
            self.assertEqual(201, resp.status_code,
                             msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code, 201))
            temp_proj_id = resp.json()["id"]

            role_response = self.capellaAPIRole.cluster_ops_apis.delete_project(self.tenant_id,
                                                                                temp_proj_id)
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
                                             user["password"])
            self.capellaAPIRole.cluster_ops_apis.SECRET = user["secretKey"]
            self.capellaAPIRole.cluster_ops_apis.ACCESS = user["accessKey"]

            self.log.info("Creating a project")
            resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id, "Project_security")
            self.assertEqual(201, resp.status_code,
                             msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code, 201))
            temp_proj_id = resp.json()["id"]

            role_response = self.capellaAPIRole.cluster_ops_apis.delete_project(self.tenant_id,
                                                                                temp_proj_id)

            if role == "projectOwner":
                self.assertEqual(204, role_response,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(role_response, 204))
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
