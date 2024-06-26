import json
from pytests.Capella.RestAPIv4.security_base import SecurityBase
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI


class SecurityTest(SecurityBase):

    def setUp(self):
        SecurityBase.setUp(self)

    def tearDown(self):
        super(SecurityTest, self).tearDown()

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
        self.capellaAPI.org_ops_apis.ACCESS = ""
        self.capellaAPI.org_ops_apis.bearer_token = ""
        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id, "Project_security")
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.org_ops_apis.SECRET = ""
        self.capellaAPI.org_ops_apis.bearer_token = ""
        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id, "Project_security")
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.org_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.org_ops_apis.bearer_token = ""
        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id, "Project_security")
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.org_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.org_ops_apis.bearer_token = ""
        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id, "Project_security")
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
            resp = self.capellaAPI.org_ops_apis.create_project(tenant_ids[tenant_id],
                                                               self.prefix + "Project")

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
                user]["mailid"], self.test_users[user]["password"], self.test_users[user]['token'])

            role_response = self.capellaAPIRole.org_ops_apis.create_project(self.tenant_id,
                                                                                "Project_security")
            if self.test_users[user]["role"] in ["organizationOwner", "projectCreator"]:
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

    def test_get_list_project(self):
        self.log.info("     II. GET List Project")
        """
        /v4/organizations/{organizationId}/projects
        """
        self.log.info("Verifying status code for retrieving information of a particular project")

        self.log.info("Verifying the endpoint authentication with different test cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.org_ops_apis.ACCESS = ""
        self.capellaAPI.org_ops_apis.bearer_token = ""
        resp = self.capellaAPI.org_ops_apis.list_projects(self.tenant_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.org_ops_apis.ACCESS = ""
        self.capellaAPI.org_ops_apis.bearer_token = ""
        resp = self.capellaAPI.org_ops_apis.list_projects(self.tenant_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.org_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.org_ops_apis.bearer_token = ""
        resp = self.capellaAPI.org_ops_apis.list_projects(self.tenant_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.org_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.org_ops_apis.bearer_token = ""
        resp = self.capellaAPI.org_ops_apis.list_projects(self.tenant_id)
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
            resp = self.capellaAPI.org_ops_apis.list_projects(tenant_ids[tenant_id])

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

            role_response = self.capellaAPIRole.org_ops_apis.list_projects(self.tenant_id)
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
            self.capellaAPI.org_ops_apis.update_user(self.tenant_id, user['userid'],
                                                     dic["update_info"])

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', user["mailid"],
                                             user["password"], user["token"])

            role_response = self.capellaAPIRole.org_ops_apis.list_projects(self.tenant_id)

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

    def test_get_get_project(self):
        self.log.info("     III. GET Get Project")
        """
        /v4/organizations/{organizationId}/projects/{projectId}
        """
        self.log.info("Verifying status code for retrieving information of a particular project")

        self.log.info("Verifying the endpoint authentication with different test cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.org_ops_apis.ACCESS = ""
        self.capellaAPI.org_ops_apis.bearer_token = ""
        resp = self.capellaAPI.org_ops_apis.fetch_project_info(self.tenant_id,
                                                            self.project_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.org_ops_apis.SECRET = ""
        self.capellaAPI.org_ops_apis.bearer_token = ""
        resp = self.capellaAPI.org_ops_apis.fetch_project_info(self.tenant_id,
                                                            self.project_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.org_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.org_ops_apis.bearer_token = ""
        resp = self.capellaAPI.org_ops_apis.fetch_project_info(self.tenant_id,
                                                            self.project_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.org_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.org_ops_apis.bearer_token = ""
        resp = self.capellaAPI.org_ops_apis.fetch_project_info(self.tenant_id,
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
            resp = self.capellaAPI.org_ops_apis.fetch_project_info(tenant_ids[tenant_id],
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
            resp = self.capellaAPI.org_ops_apis.fetch_project_info(self.tenant_id,
                                                                project_ids[project_id])

            if project_id in ['valid_project_id', 'different_project_id']:
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
                user]["mailid"], self.test_users[user]["password"], self.test_users[user]["token"])

            role_response = self.capellaAPIRole.org_ops_apis.fetch_project_info(self.tenant_id,
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

            role_response = self.capellaAPIRole.org_ops_apis.fetch_project_info(self.tenant_id,
                                                                             self.project_id)

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

        self.log.info("Creating a new project for the test")
        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id,
                                                           "Test_Update_Project")
        parent_project_id = resp.json()['id']
        self.assertEqual(201, resp.status_code,
                         msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code, 201))

        self.log.info("Verifying the endpoint authentication with different test cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.org_ops_apis.ACCESS = ""
        self.capellaAPI.org_ops_apis.bearer_token = ""
        resp = self.capellaAPI.org_ops_apis.update_project(self.tenant_id,
                                                           parent_project_id,
                                                           "Test_Update_Project1",
                                                           "Adding 1 at the end",
                                                           False)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.org_ops_apis.SECRET = ""
        self.capellaAPI.org_ops_apis.bearer_token = ""
        resp = self.capellaAPI.org_ops_apis.update_project(self.tenant_id,
                                                           parent_project_id,
                                                           "Test_Update_Project1",
                                                           "Adding 1 at the end",
                                                           False)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.org_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.org_ops_apis.bearer_token = ""
        resp = self.capellaAPI.org_ops_apis.update_project(self.tenant_id,
                                                           parent_project_id,
                                                           "Test_Update_Project1",
                                                           "Adding 1 at the end",
                                                           False)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.org_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.org_ops_apis.bearer_token = ""
        resp = self.capellaAPI.org_ops_apis.update_project(self.tenant_id,
                                                           parent_project_id,
                                                           "Test_Update_Project1",
                                                           "Adding 1 at the end",
                                                           False)
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
            resp = self.capellaAPI.org_ops_apis.update_project(tenant_ids[tenant_id],
                                                               parent_project_id,
                                                               "Test_Update_Project1",
                                                               "Adding 1 at the end",
                                                               False)

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
            resp = self.capellaAPI.org_ops_apis.update_project(self.tenant_id,
                                                               project_ids[project_id],
                                                               "Test_Update_Project2",
                                                               "Adding 2 at the end",
                                                               False)

            if project_id in ['valid_project_id', 'different_project_id']:
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
                user]["mailid"], self.test_users[user]["password"], self.test_users[user]["token"])

            role_response = self.capellaAPIRole.org_ops_apis.update_project(self.tenant_id,
                                                               parent_project_id,
                                                               "Test_Update_Project3",
                                                               "Adding 3 at the end",
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
                    "id": parent_project_id,
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

            role_response = self.capellaAPIRole.org_ops_apis.update_project(self.tenant_id,
                                                                            parent_project_id,
                                                                            "Test_Update_Project4",
                                                                            "Adding 4 at the end",
                                                                            False)

            if role == "projectOwner":
                self.assertEqual(204, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(
                                     role_response.status_code, 204))
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

        self.log.info("Deleting project created for the test")
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                           parent_project_id)
        self.assertEqual(204, resp.status_code,
                         msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code, 204))

    def test_del_delete_project(self):
        self.log.info("     V. DEL Delete Project")
        """
        /v4/organizations/{organizationId}/projects/{projectId}
        """
        self.log.info("Verifying status code for deleting a particular project")

        self.log.info("Creating a new project for the test")
        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id,
                                                           "Test_Update_Project")
        parent_project_id = resp.json()['id']
        self.assertEqual(201, resp.status_code,
                         msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code, 201))

        self.log.info("Verifying the endpoint authentication with different test cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.org_ops_apis.ACCESS = ""
        self.capellaAPI.org_ops_apis.bearer_token = ""
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                           parent_project_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.org_ops_apis.SECRET = ""
        self.capellaAPI.org_ops_apis.bearer_token = ""
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                           parent_project_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.org_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.org_ops_apis.bearer_token = ""
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                           parent_project_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.org_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.org_ops_apis.bearer_token = ""
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                           parent_project_id)
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
                resp = self.capellaAPI.org_ops_apis.delete_project(tenant_ids[tenant_id],
                                                                       temp_proj_id)
            else:
                resp = self.capellaAPI.org_ops_apis.delete_project(tenant_ids[tenant_id],
                                                                       self.project_id)

            if tenant_id == 'valid_tenant_id':
                self.assertEqual(resp.status_code, 204,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              204))
            else:
                self.assertEqual(resp.status_code, 403,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Testing for organization RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")

        self.log.info("Creating a project")
        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id, "Project_security")
        self.assertEqual(201, resp.status_code,
                         msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code, 201))
        temp_proj_id = resp.json()["id"]

        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))
            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"], self.test_users[user]["token"])

            role_response = self.capellaAPIRole.org_ops_apis.delete_project(self.tenant_id,
                                                                            temp_proj_id)

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(role_response.status_code, 204,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 204))

                self.log.info("Creating a project")
                resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id,
                                                                   "Project_security")
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              201))
                temp_proj_id = resp.json()["id"]
            else:
                self.assertEqual(role_response.status_code, 403,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 403))

        self.log.info("Deleting a project")
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id, temp_proj_id)
        self.assertEqual(204, resp.status_code,
                         msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code, 204))

        # Testing for Project Level RBAC roles
        self.log.info("Creating a project")
        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id, "Project_security")
        self.assertEqual(201, resp.status_code,
                         msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code, 201))
        temp_proj_id = resp.json()["id"]

        self.log.info("Verifying endpoint for different roles under project - RBAC")
        project_roles = ["projectOwner", "projectViewer", "projectManager",
                         "projectDataReaderWriter", "projectDataReader"]
        user = self.test_users["User3"]
        for role in project_roles:
            resources = [
                {
                    "id": temp_proj_id,
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

            role_response = self.capellaAPIRole.org_ops_apis.delete_project(self.tenant_id,
                                                                            temp_proj_id)

            if role == "projectOwner":
                self.assertEqual(204, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(
                                     role_response.status_code, 204))
                self.log.info("Creating a project")
                resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id,
                                                                   "Project_security")
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              201))
                temp_proj_id = resp.json()["id"]

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

        self.log.info("Deleting a project")
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id, temp_proj_id)
        self.assertEqual(204, resp.status_code,
                         msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code, 204))
