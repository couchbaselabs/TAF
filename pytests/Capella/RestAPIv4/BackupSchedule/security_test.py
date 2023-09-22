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

        self.capellaAPI = CapellaAPI(
            "https://" + self.url, "", "", self.user, self.passwd, "")
        self.create_v2_control_plane_api_key()

        # create the first V4 API KEY WITH organizationOwner role, which will
        # be used to perform further V4 api operations
        resp = self.capellaAPI.org_ops_apis.create_api_key(
            organizationId=self.tenant_id,
            name="Admin keys",
            organizationRoles=["organizationOwner"])
        if resp.status_code == 201:
            self.org_owner_key = resp.json()
        else:
            self.fail("Error while creating API key for organization owner")

        # update the token for capellaAPI object, so that is it being used
        # for api auth.
        self.update_auth_with_api_token(self.org_owner_key["token"])

        self.capellaAPI.cluster_ops_apis.bearer_token_temp = self.capellaAPI.cluster_ops_apis.bearer_token
        self.capellaAPI.org_ops_apis.bearer_token_temp = self.capellaAPI.org_ops_apis.bearer_token
        self.bucket_name = "test-bucket"
        if self.input.capella.get("test_users"):
            self.test_users = json.loads(self.input.capella.get("test_users"))
        else:
            self.test_users = {"User1": {"password": self.passwd, "mailid": self.user,
                                         "role": "organizationOwner"}}
        self.log.info("Test users: {}".format(self.test_users))
        for user in self.test_users:
            self.log.info("User: {}".format(user))
            resp = self.capellaAPI.org_ops_apis.create_api_key(
                self.tenant_id, 'API Key for role {}'.format(
                self.test_users[user]["role"]), organizationRoles=[self.test_users[user]["role"]],
                expiry=1)
            resp = resp.json()
            self.test_users[user]["token"] = resp['token']

        self.log.info("Creating a bucket with name: {}".format(self.bucket_name))
        resp = self.capellaAPI.cluster_ops_apis.create_bucket(self.tenant_id,
                                                              self.project_id,
                                                              self.cluster_id,
                                                              self.bucket_name,
                                                              "couchbase",
                                                              "magma",
                                                              1024,
                                                              "seqno",
                                                              "majority",
                                                              2,
                                                              False,
                                                              0)
        if resp.status_code != 201:
            self.fail("Failed to create bucket: {}".format(resp.content))
        resp = resp.json()
        self.bucket_id = resp["id"]
        self.log.info("Created bucket with bucket id: {}".format(self.bucket_id))

    def update_auth_with_api_token(self, token):
        self.capellaAPI.org_ops_apis.bearer_token = token
        self.capellaAPI.cluster_ops_apis.bearer_token = token

    def create_v2_control_plane_api_key(self):
        # Generate the first set of API access and secret access keys
        # Currently v2 API is being used for this.
        response = self.capellaAPI.create_control_plane_api_key(
            self.tenant_id, "initial_api"
        )
        if response.status_code == 201:
            response = response.json()
            self.v2_control_plane_api_access_key = response["accessKey"]
            self.update_auth_with_api_token(response["token"])
        else:
            self.log.error("Error while creating V2 control plane API key")
            self.fail("{}".format(response.content))

    def reset_bearer_token(self):
        self.capellaAPI.cluster_ops_apis.bearer_token = self.capellaAPI.cluster_ops_apis.bearer_token_temp
        self.capellaAPI.org_ops_apis.bearer_token = self.capellaAPI.org_ops_apis.bearer_token_temp

    def test_create_backup_schedule(self):
        self.log.info("Verifying status code for creating backup schedule")

        self.log.info("Verifying the endpoint authentication for different test cases")

        self.log.info("     1.Empty token:")
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.create_backup_schedule(self.tenant_id, self.project_id,
                                                                       self.cluster_id, self.bucket_id,
                                                                       "weekly", "sunday", 10,
                                                                       4, "90days", False)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_bearer_token()

        self.log.info("     2.Invalid token:")
        self.capellaAPI.cluster_ops_apis.bearer_token = self.invalid_id
        resp = self.capellaAPI.cluster_ops_apis.create_backup_schedule(self.tenant_id, self.project_id,
                                                                       self.cluster_id, self.bucket_id,
                                                                       "weekly", "sunday", 10,
                                                                       4, "90days", False)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_bearer_token()

        # Verifying the certificate endpoint with different organization ids
        self.log.info("Verifying with different tenant ids")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            'invalid_tenant_id': self.invalid_id
        }

        for tenant_id in tenant_ids:
            resp = self.capellaAPI.cluster_ops_apis.create_backup_schedule(tenant_ids[tenant_id], self.project_id,
                                                                       self.cluster_id, self.bucket_id,
                                                                       "weekly", "sunday", 10,
                                                                       4, "90days", False)
            if tenant_id == "valid_tenant_id":
                self.assertEqual(resp.status_code, 201,
                            msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 201))
            else:
                self.assertEqual(resp.status_code, 404,
                            msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 404))

        # Verify the endpoint with different projects
        self.log.info("Verifying the endpoint access with different projects")
        self.log.info("Creating a project")
        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id, "Test Project")
        self.assertEqual(201, resp.status_code,
                         msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code, 201))
        resp = resp.json()
        project_ids = {
            'valid_project_id': self.project_id,
            'different_project_id': resp['id'],
            'invalid_project_id': self.invalid_id
        }

        for project_id in project_ids:
            resp = self.capellaAPI.cluster_ops_apis.create_backup_schedule(self.tenant_id, project_ids[project_id],
                                                                       self.cluster_id, self.bucket_id,
                                                                       "weekly", "sunday", 10,
                                                                       4, "90days", False)

            if project_id == "valid_project_id":
                self.assertEqual(resp.status_code, 201,
                            msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 201))
                resp = self.capellaAPI.cluster_ops_apis.delete_backup_schedule(self.tenant_id, project_ids[project_id],
                                                                               self.cluster_id, self.bucket_id)
            elif project_id == "different_project_id":
                self.assertEqual(resp.status_code, 404,
                            msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 404))
            else:
                self.assertEqual(resp.status_code, 404,
                            msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 404))

        self.log.info("Deleting project")
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                           project_ids["different_project_id"])

        # Testing for organization RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))
            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"], "")

            self.capellaAPIRole.cluster_ops_apis.bearer_token = self.test_users[user]["token"]

            role_response = self.capellaAPIRole.cluster_ops_apis.create_backup_schedule(self.tenant_id, self.project_id,
                                                                                        self.cluster_id, self.bucket_id,
                                                                                        "weekly", "sunday", 10,
                                                                                        4, "90days", False)
            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(role_response.status_code, 201,
                             msg='FAIL: Outcome:{}, Expected:{}'.format(
                                 role_response.status_code, 201))
                resp = self.capellaAPI.cluster_ops_apis.delete_backup_schedule(self.tenant_id, self.project_id,
                                                                               self.cluster_id, self.bucket_id)
                self.assertEqual(resp.status_code, 202,
                                 msg='Failed to delete schedule')
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
                    role), organizationRoles=["organizationMember"], expiry=1,
                    resources=resources)
            resp = resp.json()
            api_key_id = resp['Id']
            user['token'] = resp['token']

            self.log.info("Adding user to project {} with role as {}".format(self.project_id, role))
            dic = {"update_info" : [{
                    "op": "add",
                    "path": "/resources/{}".format(self.project_id),
                    "value": {
                      "id": self.project_id,
                      "type": "project",
                      "roles": [role]
                    }
                }]
            }
            add_proj_resp = self.capellaAPI.org_ops_apis.update_user(
                self.tenant_id, user['userid'], dic["update_info"])

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', user["mailid"],
                                             user["password"], "")
            self.capellaAPI.org_ops_apis.update_user(self.tenant_id,
                                                     user['userid'],
                                                     dic["update_info"])

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', user["mailid"],
                                             user["password"], user['token'])

            role_response = self.capellaAPIRole.cluster_ops_apis.create_backup_schedule(self.tenant_id, self.project_id,
                                                                                        self.cluster_id, self.bucket_id,
                                                                                        "weekly", "sunday", 10,
                                                                                        4, "90days", False)


            if role == "projectOwner":
                self.assertEqual(201, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(role_response.status_code, 201))
                resp = self.capellaAPI.cluster_ops_apis.delete_backup_schedule(self.tenant_id, self.project_id,
                                                                               self.cluster_id, self.bucket_id)
                self.assertEqual(resp.status_code, 202,
                                 msg='Failed to delete schedule')
            else:
                self.assertEqual(403, role_response.status_code,
                                    msg="FAIL: Outcome:{}, Expected: {}, Role: {}".format(role_response.status_code, 403,
                                                                                          role))

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

    def test_update_backup_schedule(self):

        self.log.info("Creating a backup schedule")
        resp = self.capellaAPI.cluster_ops_apis.create_backup_schedule(self.tenant_id, self.project_id,
                                                                       self.cluster_id, self.bucket_id,
                                                                       "weekly", "sunday", 10,
                                                                       4, "90days", False)
        if resp.status_code != 201:
            self.fail("Failed to create backup schedule: {}".format(resp.content))

        self.log.info("Verifying status code for updating backup schedule")

        self.log.info("Verifying the endpoint authentication for different test cases")

        self.log.info("     1.Empty token:")
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.update_backup_schedule(self.tenant_id, self.project_id,
                                                                       self.cluster_id, self.bucket_id,
                                                                       "weekly", "monday", 11,
                                                                       4, "90days", False)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_bearer_token()

        self.log.info("     2.Invalid token:")
        self.capellaAPI.cluster_ops_apis.bearer_token = self.invalid_id
        resp = self.capellaAPI.cluster_ops_apis.create_backup_schedule(self.tenant_id, self.project_id,
                                                                       self.cluster_id, self.bucket_id,
                                                                       "weekly", "monday", 11,
                                                                       4, "90days", False)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_bearer_token()

        # Verifying the certificate endpoint with different organization ids
        self.log.info("Verifying with different tenant ids")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            'invalid_tenant_id': self.invalid_id
        }

        for tenant_id in tenant_ids:
            resp = self.capellaAPI.cluster_ops_apis.update_backup_schedule(tenant_ids[tenant_id], self.project_id,
                                                                       self.cluster_id, self.bucket_id,
                                                                       "weekly", "monday", 11,
                                                                       4, "90days", False)
            if tenant_id == "valid_tenant_id":
                self.assertEqual(resp.status_code, 200,
                            msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 201))
            else:
                self.assertEqual(resp.status_code, 404,
                            msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 404))

        # Verify the endpoint with different projects
        self.log.info("Verifying the endpoint access with different projects")
        self.log.info("Creating a project")
        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id, "Test Project")
        self.assertEqual(201, resp.status_code,
                         msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code, 201))
        resp = resp.json()
        project_ids = {
            'valid_project_id': self.project_id,
            'different_project_id': resp['id'],
            'invalid_project_id': self.invalid_id
        }
        # Verify the endpoint with different projects
        self.log.info("Verifying the endpoint access with different projects")
        self.log.info("Creating a project")
        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id, "Test Project")
        self.assertEqual(201, resp.status_code,
                         msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code, 201))
        resp = resp.json()
        project_ids = {
            'valid_project_id': self.project_id,
            'different_project_id': resp['id'],
            'invalid_project_id': self.invalid_id
        }

        for project_id in project_ids:
            resp = self.capellaAPI.cluster_ops_apis.update_backup_schedule(self.tenant_id, project_ids[project_id],
                                                                       self.cluster_id, self.bucket_id,
                                                                       "weekly", "tuesday", 9,
                                                                       4, "90days", False)

            if project_id == "valid_project_id":
                self.assertEqual(resp.status_code, 200,
                            msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 201))
            elif project_id == "different_project_id":
                self.assertEqual(resp.status_code, 404,
                            msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 404))
            else:
                self.assertEqual(resp.status_code, 404,
                            msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 404))

        self.log.info("Deleting project")
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                           project_ids["different_project_id"])

                # Testing for organization RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))
            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"], "")

            self.capellaAPIRole.cluster_ops_apis.bearer_token = self.test_users[user]["token"]

            role_response = self.capellaAPIRole.cluster_ops_apis.update_backup_schedule(self.tenant_id, self.project_id,
                                                                                        self.cluster_id, self.bucket_id,
                                                                                        "weekly", "monday", 11,
                                                                                        4, "90days", False)
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
            resources = [
                {
                    "id": self.project_id,
                    "roles": [role]
                }
            ]
            resp = self.capellaAPI.org_ops_apis.create_api_key(
                self.tenant_id, 'API Key for role {}'.format(
                    role), organizationRoles=["organizationMember"], expiry=1,
                    resources=resources)
            resp = resp.json()
            api_key_id = resp['Id']
            user['token'] = resp['token']

            dic = {"update_info" : [{
                    "op": "add",
                    "path": "/resources/{}".format(self.project_id),
                    "value": {
                      "id": self.project_id,
                      "type": "project",
                      "roles": [role]
                    }
                }]
            }
            add_proj_resp = self.capellaAPI.org_ops_apis.update_user(
                self.tenant_id, user['userid'], dic["update_info"])

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', user["mailid"],
                                             user["password"], user['token'])

            role_response = self.capellaAPIRole.cluster_ops_apis.update_backup_schedule(self.tenant_id, self.project_id,
                                                                                        self.cluster_id, self.bucket_id,
                                                                                        "weekly", "monday", 11,
                                                                                        4, "90days", False)


            if role == "projectOwner":
                self.assertEqual(200, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(role_response.status_code, 200))
            else:
                self.assertEqual(403, role_response.status_code,
                                    msg="FAIL: Outcome:{}, Expected: {}, Role: {}".format(role_response.status_code, 403,
                                                                                          role))

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


    def test_get_backup_schedule(self):
        self.log.info("Creating a backup schedule")
        resp = self.capellaAPI.cluster_ops_apis.create_backup_schedule(self.tenant_id, self.project_id,
                                                                       self.cluster_id, self.bucket_id,
                                                                       "weekly", "sunday", 10,
                                                                       4, "90days", False)
        if resp.status_code != 201:
            self.fail("Failed to create backup schedule: {}".format(resp.content))

        self.log.info("Verifying the endpoint authentication for different test cases")

        self.log.info("     1.Empty token:")
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.get_backup_schedule(self.tenant_id, self.project_id,
                                                                    self.cluster_id, self.bucket_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_bearer_token()

        self.log.info("     2.Invalid token:")
        self.capellaAPI.cluster_ops_apis.bearer_token = self.invalid_id
        resp = self.capellaAPI.cluster_ops_apis.get_backup_schedule(self.tenant_id, self.project_id,
                                                                    self.cluster_id, self.bucket_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_bearer_token()

        # Verifying the certificate endpoint with different organization ids
        self.log.info("Verifying with different tenant ids")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            'invalid_tenant_id': self.invalid_id
        }

        for tenant_id in tenant_ids:
            resp = self.capellaAPI.cluster_ops_apis.get_backup_schedule(tenant_ids[tenant_id], self.project_id,
                                                                    self.cluster_id, self.bucket_id)
            if tenant_id == "valid_tenant_id":
                self.assertEqual(resp.status_code, 200,
                            msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 201))
            else:
                self.assertEqual(resp.status_code, 404,
                            msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 404))

        # Verify the endpoint with different projects
        self.log.info("Verifying the endpoint access with different projects")
        self.log.info("Creating a project")
        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id, "Test Project")
        self.assertEqual(201, resp.status_code,
                         msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code, 201))
        resp = resp.json()
        project_ids = {
            'valid_project_id': self.project_id,
            'different_project_id': resp['id'],
            'invalid_project_id': self.invalid_id
        }

        for project_id in project_ids:
            resp = self.capellaAPI.cluster_ops_apis.get_backup_schedule(self.tenant_id, project_ids[project_id],
                                                                    self.cluster_id, self.bucket_id)

            if project_id == "valid_project_id":
                self.assertEqual(resp.status_code, 200,
                            msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 201))
            elif project_id == "different_project_id":
                self.assertEqual(resp.status_code, 404,
                            msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 404))
            else:
                self.assertEqual(resp.status_code, 404,
                            msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 404))

        self.log.info("Deleting project")
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                           project_ids["different_project_id"])

                        # Testing for organization RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))
            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"], "")

            self.capellaAPIRole.cluster_ops_apis.bearer_token = self.test_users[user]["token"]

            role_response = self.capellaAPIRole.cluster_ops_apis.get_backup_schedule(self.tenant_id,
                                                                                     self.project_id,
                                                                                     self.cluster_id,
                                                                                     self.bucket_id)
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
            resources = [
                {
                    "id": self.project_id,
                    "roles": [role]
                }
            ]
            resp = self.capellaAPI.org_ops_apis.create_api_key(
                self.tenant_id, 'API Key for role {}'.format(
                    role), organizationRoles=["organizationMember"], expiry=1,
                    resources=resources)
            resp = resp.json()
            api_key_id = resp['Id']
            user['token'] = resp['token']

            dic = {"update_info" : [{
                    "op": "add",
                    "path": "/resources/{}".format(self.project_id),
                    "value": {
                      "id": self.project_id,
                      "type": "project",
                      "roles": [role]
                    }
                }]
            }
            add_proj_resp = self.capellaAPI.org_ops_apis.update_user(
                self.tenant_id, user['userid'], dic["update_info"])

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', user["mailid"],
                                             user["password"], user['token'])

            role_response = self.capellaAPIRole.cluster_ops_apis.get_backup_schedule(self.tenant_id,
                                                                                     self.project_id,
                                                                                     self.cluster_id,
                                                                                     self.bucket_id)


            if role == "projectOwner":
                self.assertEqual(200, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(role_response.status_code, 200))
            else:
                self.assertEqual(403, role_response.status_code,
                                    msg="FAIL: Outcome:{}, Expected: {}, Role: {}".format(role_response.status_code, 403,
                                                                                          role))

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


    def test_delete_backup_schedule(self):
        self.log.info("Creating a backup schedule")
        resp = self.capellaAPI.cluster_ops_apis.create_backup_schedule(self.tenant_id, self.project_id,
                                                                       self.cluster_id, self.bucket_id,
                                                                       "weekly", "sunday", 10,
                                                                       4, "90days", False)
        if resp.status_code != 201:
            self.fail("Failed to create backup schedule: {}".format(resp.content))

        self.log.info("Verifying status code for updating backup schedule")

        self.log.info("Verifying the endpoint authentication for different test cases")

        self.log.info("     1.Empty token:")
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.delete_backup_schedule(self.tenant_id, self.project_id,
                                                                    self.cluster_id, self.bucket_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_bearer_token()

        self.log.info("     2.Invalid token:")
        self.capellaAPI.cluster_ops_apis.bearer_token = self.invalid_id
        resp = self.capellaAPI.cluster_ops_apis.delete_backup_schedule(self.tenant_id, self.project_id,
                                                                    self.cluster_id, self.bucket_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_bearer_token()

        # Verifying the certificate endpoint with different organization ids
        self.log.info("Verifying with different tenant ids")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            'invalid_tenant_id': self.invalid_id
        }

        for tenant_id in tenant_ids:
            resp = self.capellaAPI.cluster_ops_apis.delete_backup_schedule(tenant_ids[tenant_id], self.project_id,
                                                                    self.cluster_id, self.bucket_id)
            if tenant_id == "valid_tenant_id":
                self.assertEqual(resp.status_code, 202,
                            msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 201))
            else:
                self.assertEqual(resp.status_code, 404,
                            msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 404))

        # Verify the endpoint with different projects
        self.log.info("Verifying the endpoint access with different projects")
        self.log.info("Creating a project")
        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id, "Test Project")
        self.assertEqual(201, resp.status_code,
                         msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code, 201))
        resp = resp.json()
        project_ids = {
            'valid_project_id': self.project_id,
            'different_project_id': resp['id'],
            'invalid_project_id': self.invalid_id
        }

        for project_id in project_ids:
            resp = self.capellaAPI.cluster_ops_apis.delete_backup_schedule(self.tenant_id, project_ids[project_id],
                                                                    self.cluster_id, self.bucket_id)

            if project_id == "valid_project_id":
                self.assertEqual(resp.status_code, 202,
                            msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 201))
            elif project_id == "different_project_id":
                self.assertEqual(resp.status_code, 404,
                            msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 404))
            else:
                self.assertEqual(resp.status_code, 404,
                            msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 404))

        self.log.info("Deleting project")
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                           project_ids["different_project_id"])

                                # Testing for organization RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))
            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"], "")

            self.capellaAPIRole.cluster_ops_apis.bearer_token = self.test_users[user]["token"]

            role_response = self.capellaAPIRole.cluster_ops_apis.delete_backup_schedule(self.tenant_id,
                                                                                     self.project_id,
                                                                                     self.cluster_id,
                                                                                     self.bucket_id)
            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(role_response.status_code, 202,
                             msg='FAIL: Outcome:{}, Expected:{}'.format(
                                 role_response.status_code, 202))
                resp = self.capellaAPI.cluster_ops_apis.create_backup_schedule(self.tenant_id, self.project_id,
                                                                       self.cluster_id, self.bucket_id,
                                                                       "weekly", "sunday", 10,
                                                                       4, "90days", False)
                if resp.status_code != 201:
                    self.fail("Failed to create backup schedule: {}".format(resp.content))
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
            resources = [
                {
                    "id": self.project_id,
                    "roles": [role]
                }
            ]
            resp = self.capellaAPI.org_ops_apis.create_api_key(
                self.tenant_id, 'API Key for role {}'.format(
                    role), organizationRoles=["organizationMember"], expiry=1,
                    resources=resources)
            resp = resp.json()
            api_key_id = resp['Id']
            user['token'] = resp['token']

            dic = {"update_info" : [{
                    "op": "add",
                    "path": "/resources/{}".format(self.project_id),
                    "value": {
                      "id": self.project_id,
                      "type": "project",
                      "roles": [role]
                    }
                }]
            }
            add_proj_resp = self.capellaAPI.org_ops_apis.update_user(
                self.tenant_id, user['userid'], dic["update_info"])

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', user["mailid"],
                                             user["password"], user['token'])

            role_response = self.capellaAPIRole.cluster_ops_apis.delete_backup_schedule(self.tenant_id,
                                                                                     self.project_id,
                                                                                     self.cluster_id,
                                                                                     self.bucket_id)


            if role == "projectOwner":
                self.assertEqual(202, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(role_response.status_code, 202))
                resp = self.capellaAPI.cluster_ops_apis.create_backup_schedule(self.tenant_id, self.project_id,
                                                                       self.cluster_id, self.bucket_id,
                                                                       "weekly", "sunday", 10,
                                                                       4, "90days", False)
                if resp.status_code != 201:
                    self.fail("Failed to create backup schedule: {}".format(resp.content))
            else:
                self.assertEqual(403, role_response.status_code,
                                    msg="FAIL: Outcome:{}, Expected: {}, Role: {}".format(role_response.status_code, 403,
                                                                                          role))

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

    def tearDown(self):
        resp = self.capellaAPI.cluster_ops_apis.delete_bucket(self.tenant_id, self.project_id,
                                                              self.cluster_id, self.bucket_id)
        super(SecurityTest, self).tearDown()
