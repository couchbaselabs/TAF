# import time
# import json
import random
# from pytests.basetestcase import BaseTestCase
from pytests.Capella.RestAPIv4.security_base import SecurityBase
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI

class SecurityTest(SecurityBase):

    def setUp(self):
        SecurityBase.setUp(self)

    def tearDown(self):
        super(SecurityTest, self).tearDown()

    def generate_random_cidr(self):
        return '.'.join(
            str(random.randint(0, 255)) for _ in range(4)
        ) + '/32'

    def test_create_allowed_cidr(self):
        self.log.info("Verify adding a cidr to the cluster's list of allowed CIDRs")

        self.log.info("Verifying the endpoint authentication with different test cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        cidr = self.generate_random_cidr()
        self.log.info("The cidr to be added is {}".format(cidr))
        resp = self.capellaAPI.cluster_ops_apis.add_CIDR_to_allowed_CIDRs_list(
                self.tenant_id, self.project_id, self.cluster_id, cidr, "Temp cidr"
                )
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        cidr = self.generate_random_cidr()
        self.log.info("The cidr to be added is {}".format(cidr))
        resp = self.capellaAPI.cluster_ops_apis.add_CIDR_to_allowed_CIDRs_list(self.tenant_id,
                                                                               self.project_id,
                                                                               self.cluster_id,
                                                                               cidr,
                                                                               "Temp cidr")
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        cidr = self.generate_random_cidr()
        self.log.info("The cidr to be added is {}".format(cidr))
        resp = self.capellaAPI.cluster_ops_apis.add_CIDR_to_allowed_CIDRs_list(
                self.tenant_id, self.project_id, self.cluster_id, cidr, "Temp cidr"
                )
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        cidr = self.generate_random_cidr()
        self.log.info("The cidr to be added is {}".format(cidr))
        resp = self.capellaAPI.cluster_ops_apis.add_CIDR_to_allowed_CIDRs_list(
                self.tenant_id, self.project_id, self.cluster_id, cidr, "Temp cidr")
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        # Trying with different organization ids
        self.log.info("Accessing the endpoint with different tenant ids")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            'invalid_tenant_id': self.invalid_id
        }
        for tenant_id in tenant_ids:
            cidr = self.generate_random_cidr()
            self.log.info("The cidr to be added is {}".format(cidr))
            resp = self.capellaAPI.cluster_ops_apis.add_CIDR_to_allowed_CIDRs_list(
                tenant_ids[tenant_id], self.project_id, self.cluster_id, cidr
            )
            if tenant_id == 'valid_tenant_id':
                self.cidr = cidr
                self.assertEqual(resp.status_code, 201,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              201))
            else:
                self.assertEqual(resp.status_code, 403,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Verify the endpoint with different projects
        self.log.info("Verifying the endpoint access with different projects")
        self.log.info("Creating a project")
        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id,
                                                           "Allowed CIDR Project Koushal+1")
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
            cidr = self.generate_random_cidr()
            self.log.info("The cidr to be added is {}".format(cidr))
            resp = self.capellaAPI.cluster_ops_apis.add_CIDR_to_allowed_CIDRs_list(
                                                                self.tenant_id,
                                                                project_ids[project_id],
                                                                self.cluster_id,
                                                                cidr)

            if project_id == 'valid_project_id':
                self.assertEqual(resp.status_code, 201,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              201))

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


            cidr = self.generate_random_cidr()
            self.log.info("The cidr to be added is {}".format(cidr))
            role_response = self.capellaAPIRole.cluster_ops_apis.add_CIDR_to_allowed_CIDRs_list(
                self.tenant_id, self.project_id, self.cluster_id, cidr)

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(role_response.status_code, 201,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 201))
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
            self.capellaAPI.org_ops_apis.update_user(self.tenant_id,
                                                     user['userid'],
                                                     dic["update_info"])

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', user["mailid"],
                                        user["password"], user['token'])

            cidr = self.generate_random_cidr()
            self.log.info("The cidr to be added is {}".format(cidr))
            role_response = self.capellaAPIRole.cluster_ops_apis.add_CIDR_to_allowed_CIDRs_list(
                self.tenant_id, self.project_id, self.cluster_id, cidr)

            if role == "projectOwner" or role == "projectManager":
                self.assertEqual(201, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(
                                     role_response.status_code, 201))
            else:
                self.assertEqual(403, role_response.status_code,
                                msg="FAIL: Outcome:{}, Expected: {}".format(
                                    role_response.status_code, 403))
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
                             msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 204))

        # Verify the endpoints with different payloads
        self.log.info("Verify the endpoint for different payloads")
        payloads = [
            {
                "params": {},
                "http_response": 400
            },
            {
                "params":
                    {
                        'cidr': ''
                    },
                "http_response": 400
            },
            {
                "params":
                    {
                        'cidr': 'abcde'
                    },
                "http_response": 422
            },
            {
                "params":
                    {
                        'cidr': None
                    },
                "http_response": 400
            },
            {
                "params":
                    {
                        'cidr': '1234.10.10.10/32'
                    },
                "http_response": 422
            },
            {
                "params":
                    {
                        'cidr': True
                    },
                "http_response": 400
            },
            {
                "params":
                    {
                        'cidr': '10.1.4.10/32',
                        'comment': True,
                        'expiresAt': '2022-05-14T21:49:58.465Z'
                    },
                "http_response": 400
            },
            {
                "params":
                    {
                        'cidr': '10.1.4.10/32',
                        'comment': 'Allow my local machine IP',
                        'expiresAt': '2022-05-14T21:49:58.465Z'
                    },
                "http_response": 422
            },
            {
                "params":
                    {
                        "cidr": "10.1.4.10/32",
                        "comment": "Allow my local machine IP Allow my local machine IP Allow my "
                                   "local machine IP Allow my local machine IP Allow my local "
                                   "machine IP Allow my local machine IP",
                        "expiresAt": "2022-05-14T21:49:58.465Z"
                    },
                "http_response": 422
            },
            {
                "params":
                    {
                        "cidr": "22.1.7.10/32",
                        "comment": "Allow my local machine IP Allow my local machine IP Allow "
                                   "my local machine IP Allow my local machine IP Allow my local "
                                   "machine IP Allow my local machine IP",
                        "expiresAt": "2022-05-14T21:49:58.465Z",
                        "username": "admin",
                        "password": "password"
                    },
                "http_response": 422
            }
        ]
        for payload in payloads:
            if payload["params"].has_key("cidr"):
                cidr = payload["params"]["cidr"]
            else:
                cidr = None
            if payload["params"].has_key("comment"):
                comment = payload["params"]["comment"]
            else:
                comment = ""
            if payload["params"].has_key("expiresAt"):
                expiresAt = payload["params"]["expiresAt"]
            else:
                expiresAt = ""
            self.log.info("The cidr to be added is {}".format(cidr))
            resp = self.capellaAPI.cluster_ops_apis.add_CIDR_to_allowed_CIDRs_list(
                self.tenant_id, self.project_id, self.cluster_id, cidr, comment, expiresAt)

            self.assertEqual(resp.status_code, payload["http_response"],
                             msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                          payload["http_response"]))

    def test_list_allowed_CIDRs(self):
        self.log.info("Verify listing the cidr added to the cluster's ip allowlist")

        self.log.info("Verifying the endpoint authentication with different test cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.list_allowed_CIDRs(self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.list_allowed_CIDRs(self.tenant_id,
                                                                        self.project_id,
                                                                        self.cluster_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.list_allowed_CIDRs(self.tenant_id,
                                                                        self.project_id,
                                                                        self.cluster_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.list_allowed_CIDRs(self.tenant_id,
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
            resp = self.capellaAPI.cluster_ops_apis.list_allowed_CIDRs(tenant_ids[tenant_id],
                                                                       self.project_id,
                                                                       self.cluster_id)
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
        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id, "Allowed CIDR Project "
                                                                           "Koushal+1")
        self.assertEqual(201, resp.status_code,
                         msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code, 201))

        resp = resp.json()
        project_ids = {
            'valid_project_id': self.project_id,
            'different_project_id': resp['id'],
            'invalid_project_id': self.invalid_id
        }
        for project_id in project_ids:
            resp = self.capellaAPI.cluster_ops_apis.list_allowed_CIDRs(self.tenant_id,
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
            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '',
                            self.test_users[user]["mailid"], self.test_users[user]["password"],
                                             self.test_users[user]['token'])

            role_response = self.capellaAPIRole.cluster_ops_apis.list_allowed_CIDRs(
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

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '',
                                             user["mailid"], user["password"], user['token'])

            role_response = self.capellaAPIRole.cluster_ops_apis.list_allowed_CIDRs(
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
            remove_proj_resp = self.capellaAPI.org_ops_apis.update_user(
                self.tenant_id, user['userid'], update_info)
            self.assertEqual(200, remove_proj_resp.status_code,
                             msg="FAIL: Outcome:{}, Expected: {}".format(
                                 remove_proj_resp.status_code, 200))

            resp = self.capellaAPI.org_ops_apis.delete_api_key(self.tenant_id, api_key_id)
            self.assertEqual(204, resp.status_code,
                             msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 204))

    def test_fetch_allowed_CIDR_info(self):
        self.log.info("Verify fetching the a specific cidr")

        self.log.info("Verifying the endpoint authentication with different test cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        cidr = self.generate_random_cidr()
        resp = self.capellaAPI.cluster_ops_apis.fetch_allowed_CIDR_info(self.tenant_id,
                                                                        self.project_id,
                                                                        self.cluster_id,
                                                                        cidr)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.fetch_allowed_CIDR_info(self.tenant_id,
                                                                        self.project_id,
                                                                        self.cluster_id,
                                                                        cidr)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.fetch_allowed_CIDR_info(self.tenant_id,
                                                                        self.project_id,
                                                                        self.cluster_id,
                                                                        cidr)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.fetch_allowed_CIDR_info(self.tenant_id,
                                                                        self.project_id,
                                                                        self.cluster_id,
                                                                        cidr)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        # Adding a cidr to the cluster allow-list
        parent_cidr = self.generate_random_cidr()
        self.log.info("The cidr to be added is {}".format(cidr))
        resp = self.capellaAPI.cluster_ops_apis.add_CIDR_to_allowed_CIDRs_list(self.tenant_id,
                                                                               self.project_id,
                                                                               self.cluster_id,
                                                                               parent_cidr)
        parent_cidr = resp.json()['id']
        self.assertEqual(201, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 201))

        # Verify the certificate endpoint with different organization ids
        self.log.info("Verifying with different tenant ids")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            'invalid_tenant_id': self.invalid_id
        }
        for tenant_id in tenant_ids:
            resp = self.capellaAPI.cluster_ops_apis.fetch_allowed_CIDR_info(tenant_ids[tenant_id],
                                                                            self.project_id,
                                                                            self.cluster_id,
                                                                            parent_cidr)
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
            resp = self.capellaAPI.cluster_ops_apis.fetch_allowed_CIDR_info(self.tenant_id,
                                                                       project_ids[project_id],
                                                                       self.cluster_id,
                                                                       parent_cidr)

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

            role_response = self.capellaAPIRole.cluster_ops_apis.fetch_allowed_CIDR_info(
                                                                    self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    parent_cidr)
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

            role_response = self.capellaAPIRole.cluster_ops_apis.fetch_allowed_CIDR_info(
                                                                    self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    parent_cidr)

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

    def test_delete_allowed_CIDR(self):
        self.log.info("Verify deleting allowed cidr from the IP allowlist")

        self.log.info("Verifying the endpoint authentication with different test cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        cidr = self.generate_random_cidr()
        resp = self.capellaAPI.cluster_ops_apis.delete_allowed_CIDR(self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    cidr)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.delete_allowed_CIDR(self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    cidr)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.delete_allowed_CIDR(self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    cidr)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.delete_allowed_CIDR(self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    cidr)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     5. Deleting a random cidr which does not exist in allowlist")
        cidr = self.generate_random_cidr()
        resp = self.capellaAPI.cluster_ops_apis.delete_allowed_CIDR(self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    cidr)
        self.assertEqual(404, resp.status_code,
                        msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 404))

        # Adding a cidr to the cluster allow-list ---------------------------------
        parent_cidr = self.generate_random_cidr()
        self.log.info("The cidr to be added is {}".format(cidr))
        resp = self.capellaAPI.cluster_ops_apis.add_CIDR_to_allowed_CIDRs_list(self.tenant_id,
                                                                               self.project_id,
                                                                               self.cluster_id,
                                                                               parent_cidr)
        parent_cidr = resp.json()['id']
        self.assertEqual(201, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 201))

        # Verify the certificate endpoint with different organization ids
        self.log.info("Verifying with different tenant ids")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            'invalid_tenant_id': self.invalid_id
        }
        for tenant_id in tenant_ids:
            resp = self.capellaAPI.cluster_ops_apis.delete_allowed_CIDR(tenant_ids[tenant_id],
                                                                            self.project_id,
                                                                            self.cluster_id,
                                                                            parent_cidr)
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

        # Adding a cidr to the cluster allow-list ---------------------------------
        parent_cidr = self.generate_random_cidr()
        self.log.info("The cidr to be added is {}".format(cidr))
        resp = self.capellaAPI.cluster_ops_apis.add_CIDR_to_allowed_CIDRs_list(self.tenant_id,
                                                                               self.project_id,
                                                                               self.cluster_id,
                                                                               parent_cidr)
        parent_cidr = resp.json()['id']
        self.assertEqual(201, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 201))

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
            resp = self.capellaAPI.cluster_ops_apis.delete_allowed_CIDR(self.tenant_id,
                                                                        project_ids[project_id],
                                                                        self.cluster_id,
                                                                        parent_cidr)

            if project_id == 'valid_project_id':
                self.assertEqual(resp.status_code, 204,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              204))
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

        # Adding a cidr to the cluster allow-list ---------------------------------
        parent_cidr = self.generate_random_cidr()
        self.log.info("The cidr to be added is {}".format(cidr))

        resp = self.capellaAPI.cluster_ops_apis.add_CIDR_to_allowed_CIDRs_list(
            self.tenant_id, self.project_id, self.cluster_id, parent_cidr)
        parent_cidr = resp.json()['id']
        self.assertEqual(201, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 201))

        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))
            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '',
                            self.test_users[user]["mailid"], self.test_users[user]["password"],
                                             self.test_users[user]["token"])

            role_response = self.capellaAPIRole.cluster_ops_apis.delete_allowed_CIDR(
                                                                self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                parent_cidr)
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
            # Adding a cidr to the cluster allow-list ---------------------------------
            parent_cidr = self.generate_random_cidr()
            self.log.info("The cidr to be added is {}".format(cidr))
            resp = self.capellaAPI.cluster_ops_apis.add_CIDR_to_allowed_CIDRs_list(
                self.tenant_id, self.project_id, self.cluster_id, parent_cidr)
            parent_cidr = resp.json()['id']
            self.assertEqual(201, resp.status_code,
                             msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 201))
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
            add_proj_resp = self.capellaAPI.org_ops_apis.update_user(
                self.tenant_id, user['userid'], dic["update_info"])

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', user["mailid"],
                                             user["password"], user['token'])

            role_response = self.capellaAPIRole.cluster_ops_apis.delete_allowed_CIDR(
                                                                self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                parent_cidr)

            if role in ["projectOwner", "projectManager"]:
                self.assertEqual(204, role_response.status_code,
                    msg="FAIL: Outcome:{}, Expected: {}".format(role_response.status_code, 204))
            else:
                self.assertEqual(403, role_response.status_code,
                    msg="FAIL: Outcome:{}, Expected: {}".format(role_response.status_code, 403))

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
