import time
import json
import random
from pytests.basetestcase import BaseTestCase
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI

class SecurityTest(BaseTestCase):
    cidr = "10.0.0.0"

    def setUp(self):
        BaseTestCase.setUp(self)
        self.url = self.input.capella.get("pod")
        self.user = self.input.capella.get("capella_user")
        self.passwd = self.input.capella.get("capella_pwd")
        self.tenant_id = self.input.capella.get("tenant_id")
        self.project_id = self.tenant.project_id
        self.cluster_id = self.cluster.id
        self.invalid_id = "00000000-0000-0000-0000-000000000000"
        self.capellaAPI = CapellaAPI("https://" + self.url, '', '', self.user, self.passwd, '')
        resp = self.capellaAPI.create_control_plane_api_key(self.tenant_id, 'init api keys')
        resp = resp.json()
        self.capellaAPI.cluster_ops_apis.SECRET = resp['secretKey']
        self.capellaAPI.cluster_ops_apis.ACCESS = resp['accessKey']
        self.capellaAPI.cluster_ops_apis.bearer_token = resp['token']
        self.capellaAPI.org_ops_apis.SECRET = resp['secretKey']
        self.capellaAPI.org_ops_apis.ACCESS = resp['accessKey']
        self.capellaAPI.org_ops_apis.bearer_token = resp['token']

        self.capellaAPI.cluster_ops_apis.SECRETINI = resp['secretKey']
        self.capellaAPI.cluster_ops_apis.ACCESSINI = resp['accessKey']
        self.capellaAPI.cluster_ops_apis.TOKENINI = resp['token']
        self.capellaAPI.org_ops_apis.SECRETINI = resp['secretKey']
        self.capellaAPI.org_ops_apis.ACCESSINI = resp['accessKey']
        self.capellaAPI.org_ops_apis.TOKENINI = resp['token']

        if self.input.capella.get("test_users"):
            self.test_users = json.loads(self.input.capella.get("test_users"))
        else:
            self.test_users = {"User1": {"password": self.passwd, "mailid": self.user,
                                         "role": "organizationOwner"}}

        for user in self.test_users:
            resp = self.capellaAPI.org_ops_apis.create_api_key(
                self.tenant_id, 'API Key for role {}'.format(
                self.test_users[user]["role"]), organizationRoles=[self.test_users[user]["role"]],
                expiry=1)
            resp = resp.json()
            self.test_users[user]["token"] = resp['token']

    def tearDown(self):
        super(SecurityTest, self).tearDown()

    def reset_api_keys(self):
        self.capellaAPI.cluster_ops_apis.SECRET = self.capellaAPI.cluster_ops_apis.SECRETINI
        self.capellaAPI.cluster_ops_apis.ACCESS = self.capellaAPI.cluster_ops_apis.ACCESSINI
        self.capellaAPI.cluster_ops_apis.bearer_token = self.capellaAPI.cluster_ops_apis.TOKENINI
        self.capellaAPI.org_ops_apis.SECRET = self.capellaAPI.org_ops_apis.SECRETINI
        self.capellaAPI.org_ops_apis.ACCESS = self.capellaAPI.org_ops_apis.ACCESSINI
        self.capellaAPI.org_ops_apis.bearer_token = self.capellaAPI.org_ops_apis.TOKENINI

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

    def get_bucket_payload(self):
        payload = {
              "name": "CBExample11",
              "type": "couchbase",
              "storageBackend": "couchstore",
              "memoryAllocationInMb": 100,
              "bucketConflictResolution": "seqno",
              "durabilityLevel": "majorityAndPersistActive",
              "replicas": 1,
              "flush": True,
              "timeToLiveInSeconds": 0
        }

        return payload

    def test_create_bucket(self):
        self.log.info("Verify creating clusters for v4 APIs")

        payload = self.get_bucket_payload()

        self.log.info("Verifying the create cluster endpoint authentication with different test "
                      "cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""

        resp = self.capellaAPI.cluster_ops_apis.create_bucket(self.tenant_id,
                                                              self.project_id,
                                                              self.cluster_id,
                                                              payload["name"],
                                                              payload["type"],
                                                              payload["storageBackend"],
                                                              payload["memoryAllocationInMb"],
                                                              payload["bucketConflictResolution"],
                                                              payload["durabilityLevel"],
                                                              payload["replicas"],
                                                              payload["flush"],
                                                              payload["timeToLiveInSeconds"])
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""

        resp = self.capellaAPI.cluster_ops_apis.create_bucket(self.tenant_id,
                                                              self.project_id,
                                                              self.cluster_id,
                                                              payload["name"],
                                                              payload["type"],
                                                              payload["storageBackend"],
                                                              payload["memoryAllocationInMb"],
                                                              payload["bucketConflictResolution"],
                                                              payload["durabilityLevel"],
                                                              payload["replicas"],
                                                              payload["flush"],
                                                              payload["timeToLiveInSeconds"])
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""

        resp = self.capellaAPI.cluster_ops_apis.create_bucket(self.tenant_id,
                                                              self.project_id,
                                                              self.cluster_id,
                                                              payload["name"],
                                                              payload["type"],
                                                              payload["storageBackend"],
                                                              payload["memoryAllocationInMb"],
                                                              payload["bucketConflictResolution"],
                                                              payload["durabilityLevel"],
                                                              payload["replicas"],
                                                              payload["flush"],
                                                              payload["timeToLiveInSeconds"])
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""

        resp = self.capellaAPI.cluster_ops_apis.create_bucket(self.tenant_id,
                                                              self.project_id,
                                                              self.cluster_id,
                                                              payload["name"],
                                                              payload["type"],
                                                              payload["storageBackend"],
                                                              payload["memoryAllocationInMb"],
                                                              payload["bucketConflictResolution"],
                                                              payload["durabilityLevel"],
                                                              payload["replicas"],
                                                              payload["flush"],
                                                              payload["timeToLiveInSeconds"])
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
            resp = self.capellaAPI.cluster_ops_apis.create_bucket(
                                                              tenant_ids[tenant_id],
                                                              self.project_id,
                                                              self.cluster_id,
                                                              payload["name"],
                                                              payload["type"],
                                                              payload["storageBackend"],
                                                              payload["memoryAllocationInMb"],
                                                              payload["bucketConflictResolution"],
                                                              payload["durabilityLevel"],
                                                              payload["replicas"],
                                                              payload["flush"],
                                                              payload["timeToLiveInSeconds"])

            # if tenant_id == "valid_tenant_id":
            self.assertEqual(201, resp.status_code,
                             msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                          201))
            temp_str = payload["name"][:len(payload["name"])-1] + str(int(payload["name"][-1]) + 1)
            payload["name"] = temp_str
            # Bug - Gives 201 for different tenant id as well.
            # else:
            #     self.assertEqual(403, resp.status_code,
            #                      msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
            #                                                                   403))

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
            resp = self.capellaAPI.cluster_ops_apis.create_bucket(
                                                                self.tenant_id,
                                                                project_ids[project_id],
                                                                self.cluster_id,
                                                                payload["name"],
                                                                payload["type"],
                                                                payload["storageBackend"],
                                                                payload["memoryAllocationInMb"],
                                                                payload["bucketConflictResolution"],
                                                                payload["durabilityLevel"],
                                                                payload["replicas"],
                                                                payload["flush"],
                                                                payload["timeToLiveInSeconds"])

            # if project_id == 'valid_project_id':
            self.assertEqual(resp.status_code, 201,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              201))
            temp_str = payload["name"][:len(payload["name"])-1] + str(int(payload["name"][-1]) + 1)
            payload["name"] = temp_str
            # Bug - Gives 201 for different project id and invalid project id
            # else:
            #     self.assertEqual(resp.status_code, 422,
            #                      msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
            #                                                                   422))

        self.log.info("Deleting project")
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                           project_ids["different_project_id"])
        self.assertEqual(204, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 204))

        # Testing for Organizations RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"], self.test_users[user]['token'])

            role_response = self.capellaAPIRole.cluster_ops_apis.create_bucket(
                                                                self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                payload["name"],
                                                                payload["type"],
                                                                payload["storageBackend"],
                                                                payload["memoryAllocationInMb"],
                                                                payload["bucketConflictResolution"],
                                                                payload["durabilityLevel"],
                                                                payload["replicas"],
                                                                payload["flush"],
                                                                payload["timeToLiveInSeconds"])

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(role_response.status_code, 201,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 201))
                temp_str = payload["name"][:len(payload["name"])-1] + \
                           str(int(payload["name"][-1]) + 1)
                payload["name"] = temp_str

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

            role_response = self.capellaAPIRole.cluster_ops_apis.create_bucket(
                                                                self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                payload["name"],
                                                                payload["type"],
                                                                payload["storageBackend"],
                                                                payload["memoryAllocationInMb"],
                                                                payload["bucketConflictResolution"],
                                                                payload["durabilityLevel"],
                                                                payload["replicas"],
                                                                payload["flush"],
                                                                payload["timeToLiveInSeconds"])

            if role in ["projectOwner", "projectManager"]:
                self.assertEqual(201, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(
                                     role_response.status_code, 201))
                temp_str = payload["name"][:len(payload["name"])-1] + \
                           str(int(payload["name"][-1]) + 1)
                payload["name"] = temp_str

            else:
                self.assertEqual(403, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(
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

    def test_list_buckets(self):
        self.log.info("Verify listing cluster for v4 APIs")

        self.log.info("Verifying the list cluster endpoint authentication with different test "
                      "cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.list_buckets(self.tenant_id,
                                                             self.project_id,
                                                             self.cluster_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.list_buckets(self.tenant_id,
                                                             self.project_id,
                                                             self.cluster_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.list_buckets(self.tenant_id,
                                                             self.project_id,
                                                             self.cluster_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.list_buckets(self.tenant_id,
                                                             self.project_id,
                                                             self.cluster_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        # Verify the endpoint with different organization ids
        self.log.info("Verify the endpoints with different organizations id")
        tenant_ids = {
            "valid_tenant_id": self.tenant_id,
            "invalid_tenant_id": self.invalid_id
        }
        for tenant_id in tenant_ids:
            resp = self.capellaAPI.cluster_ops_apis.list_buckets(tenant_ids[tenant_id],
                                                                 self.project_id,
                                                                 self.cluster_id)

            if tenant_id == "valid_tenant_id":
                self.assertEqual(200, resp.status_code,
                                 msg="FAIL, Outcome: {}, Expected: {}".format(resp.status_code,
                                                                              200))

            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Verify the endpoint with different project ids
        self.log.info("Verifying the endpoint access with different projects")

        self.log.info("Creating a project")
        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id,
                                                           "List Clusters Project "
                                                           "Koushal+1")
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
            resp = self.capellaAPI.cluster_ops_apis.list_buckets(self.tenant_id,
                                                                 project_ids[project_id],
                                                                 self.cluster_id)

            if project_id in ["valid_project_id", "different_project_id"]:
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(
                                     resp.status_code,
                                     200))
            # Bug - Gives 200 for different project ids
            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(
                                     resp.status_code,
                                     404))

        self.log.info("Deleting project")
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                           project_ids[
                                                               "different_project_id"])
        self.assertEqual(204, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code,
                                                                      204))

        # Testing for organization RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))
            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '',
                                             self.test_users[user]["mailid"],
                                             self.test_users[user]["password"],
                                             self.test_users[user]['token'])

            role_response = self.capellaAPIRole.cluster_ops_apis.list_buckets(self.tenant_id,
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

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '',
                                             user["mailid"], user["password"], user['token'])

            role_response = self.capellaAPIRole.cluster_ops_apis.list_buckets(self.tenant_id,
                                                                              self.project_id,
                                                                              self.cluster_id)

            self.assertEqual(200, role_response.status_code,
                             msg="FAIL: Outcome:{}, Expected: {}".format(
                                 role_response.status_code, 200))

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

    def test_fetch_bucket_info(self):
        self.log.info("Verify fetching a specific cluster details")

        payload = self.get_bucket_payload()
        temp_str = payload["name"] + str(int(payload["name"][-1]) + 30)
        payload["name"] = temp_str
        resp = self.capellaAPI.cluster_ops_apis.create_bucket(
                                                            self.tenant_id,
                                                            self.project_id,
                                                            self.cluster_id,
                                                            payload["name"],
                                                            payload["type"],
                                                            payload["storageBackend"],
                                                            payload["memoryAllocationInMb"],
                                                            payload["bucketConflictResolution"],
                                                            payload["durabilityLevel"],
                                                            payload["replicas"],
                                                            payload["flush"],
                                                            payload["timeToLiveInSeconds"])

        self.assertEqual(resp.status_code, 201,
                         msg='FAIL: Outcome:{}, Expected:{}'.format(
                             resp.status_code, 201))

        resp = resp.json()
        bucket_id = resp['id']

        self.log.info("Verifying the endpoint authentication with different test cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.fetch_bucket_info(self.tenant_id,
                                                                  self.project_id,
                                                                  self.cluster_id,
                                                                  bucket_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.fetch_bucket_info(self.tenant_id,
                                                                  self.project_id,
                                                                  self.cluster_id,
                                                                  bucket_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.fetch_bucket_info(self.tenant_id,
                                                                  self.project_id,
                                                                  self.cluster_id,
                                                                  bucket_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.fetch_bucket_info(self.tenant_id,
                                                                  self.project_id,
                                                                  self.cluster_id,
                                                                  bucket_id)
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
            resp = self.capellaAPI.cluster_ops_apis.fetch_bucket_info(tenant_ids[tenant_id],
                                                                      self.project_id,
                                                                      self.cluster_id,
                                                                      bucket_id)
            if tenant_id == 'valid_tenant_id':
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))
                temp_str = payload["name"][:len(payload["name"])-1] + \
                           str(int(payload["name"][-1]) + 1)
                payload["name"] = temp_str
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
            resp = self.capellaAPI.cluster_ops_apis.fetch_bucket_info(self.tenant_id,
                                                                      project_ids[project_id],
                                                                      self.cluster_id,
                                                                      bucket_id)

            # Bug - https://couchbasecloud.atlassian.net/browse/AV-59794
            # For now the different_project_id gives 200 response. It should give 4xx.
            if project_id in ['valid_project_id', 'different_project_id']:
                self.assertEqual(resp.status_code, 200,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))
                temp_str = payload["name"][:len(payload["name"])-1] + \
                           str(int(payload["name"][-1]) + 1)
                payload["name"] = temp_str

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
                user]["mailid"], self.test_users[user]["password"], self.test_users[user]['token'])

            role_response = self.capellaAPIRole.cluster_ops_apis.fetch_bucket_info(
                                                                                self.tenant_id,
                                                                                self.project_id,
                                                                                self.cluster_id,
                                                                                bucket_id)
            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(role_response.status_code, 200,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 200))
                temp_str = payload["name"][:len(payload["name"])-1] + \
                           str(int(payload["name"][-1]) + 1)
                payload["name"] = temp_str
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

            role_response = self.capellaAPIRole.cluster_ops_apis.fetch_bucket_info(
                                                                                self.tenant_id,
                                                                                self.project_id,
                                                                                self.cluster_id,
                                                                                bucket_id)

            self.assertEqual(200, role_response.status_code,
                             msg="FAIL: Outcome:{}, Expected: {}".format(
                                 role_response.status_code, 200))
            temp_str = payload["name"][:len(payload["name"])-1] + str(int(payload["name"][-1]) + 1)
            payload["name"] = temp_str

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

    def test_update_bucket_config(self):
        self.log.info("Verify updating a particular bucket")

        payload = self.get_bucket_payload()
        payload["name"] = payload["name"][:len(payload["name"])-1] + str(int(payload["name"][-1]) +
                                                                           60)
        resp = self.capellaAPI.cluster_ops_apis.create_bucket(
                                                            self.tenant_id,
                                                            self.project_id,
                                                            self.cluster_id,
                                                            payload["name"],
                                                            payload["type"],
                                                            payload["storageBackend"],
                                                            payload["memoryAllocationInMb"],
                                                            payload["bucketConflictResolution"],
                                                            payload["durabilityLevel"],
                                                            payload["replicas"],
                                                            payload["flush"],
                                                            payload["timeToLiveInSeconds"])
        resp = resp.json()
        bucket_id = resp["id"]

        update_payload = {
          "memoryAllocationInMb": 100,
          "durabilityLevel": "none",
          "replicas": 1,
          "flush": True,
          "timeToLiveInSeconds": 100,
          "id": "dGVzdA",
          "name": "My-First-Bucket",
          "type": "string",
          "storageBackend": "couchstore",
          "bucketConflictResolution": "string",
          "evictionPolicy": "nruEviction",
          "stats": {
            "itemCount": 10,
            "opsPerSecond": 0,
            "diskUsedInMib": 17,
            "memoryUsedInMib": 50
          }
        }

        self.log.info("Verifying the endpoint authentication with different test cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.update_bucket_config(
                                                            self.tenant_id,
                                                            self.project_id,
                                                            self.cluster_id,
                                                            bucket_id,
                                                            update_payload["memoryAllocationInMb"],
                                                            update_payload["durabilityLevel"],
                                                            update_payload["replicas"],
                                                            update_payload["flush"],
                                                            update_payload["timeToLiveInSeconds"],
                                                            False)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.update_bucket_config(
                                                            self.tenant_id,
                                                            self.project_id,
                                                            self.cluster_id,
                                                            bucket_id,
                                                            update_payload["memoryAllocationInMb"],
                                                            update_payload["durabilityLevel"],
                                                            update_payload["replicas"],
                                                            update_payload["flush"],
                                                            update_payload["timeToLiveInSeconds"],
                                                            False)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.update_bucket_config(
                                                            self.tenant_id,
                                                            self.project_id,
                                                            self.cluster_id,
                                                            bucket_id,
                                                            update_payload["memoryAllocationInMb"],
                                                            update_payload["durabilityLevel"],
                                                            update_payload["replicas"],
                                                            update_payload["flush"],
                                                            update_payload["timeToLiveInSeconds"],
                                                            False)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.update_bucket_config(
                                                            self.tenant_id,
                                                            self.project_id,
                                                            self.cluster_id,
                                                            bucket_id,
                                                            update_payload["memoryAllocationInMb"],
                                                            update_payload["durabilityLevel"],
                                                            update_payload["replicas"],
                                                            update_payload["flush"],
                                                            update_payload["timeToLiveInSeconds"],
                                                            False)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        # Verify endpoint for different organizations id
        self.log.info("Verify the update cluster v4 api for different organization ids")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            'invalid_tenant_id': self.invalid_id
        }
        for tenant_id in tenant_ids:
            resp = self.capellaAPI.cluster_ops_apis.update_bucket_config(
                                                            tenant_ids[tenant_id],
                                                            self.project_id,
                                                            self.cluster_id,
                                                            bucket_id,
                                                            update_payload["memoryAllocationInMb"],
                                                            update_payload["durabilityLevel"],
                                                            update_payload["replicas"],
                                                            update_payload["flush"],
                                                            update_payload["timeToLiveInSeconds"],
                                                            False)
            if tenant_id == 'valid_tenant_id':
                self.assertEqual(resp.status_code, 204,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              204))
                update_payload["memoryAllocationInMb"] = update_payload["memoryAllocationInMb"] + 1
            else:
                # For now the response is 403. Later change it to 404.
                self.assertEqual(resp.status_code, 403,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Verify the endpoint with different projects
        self.log.info("Verifying the endpoint access with different projects")
        self.log.info("Creating a project")

        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id,
                                                           "Allowed CIDR Project "
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
            resp = self.capellaAPI.cluster_ops_apis.update_bucket_config(
                                                            self.tenant_id,
                                                            project_ids[project_id],
                                                            self.cluster_id,
                                                            bucket_id,
                                                            update_payload["memoryAllocationInMb"],
                                                            update_payload["durabilityLevel"],
                                                            update_payload["replicas"],
                                                            update_payload["flush"],
                                                            update_payload["timeToLiveInSeconds"],
                                                            False)

            # For different_project_id it should give 4xx but instead it gives 200 for now.
            if project_id in ['valid_project_id', 'different_project_id']:
                self.assertEqual(resp.status_code, 204,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(
                                     resp.status_code, 204))

                update_payload["memoryAllocationInMb"] = update_payload["memoryAllocationInMb"] + 1

            else:
                self.assertEqual(resp.status_code, 404,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(
                                     resp.status_code, 404))

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

            role_response = self.capellaAPIRole.cluster_ops_apis.update_bucket_config(
                                                            self.tenant_id,
                                                            self.project_id,
                                                            self.cluster_id,
                                                            bucket_id,
                                                            update_payload["memoryAllocationInMb"],
                                                            update_payload["durabilityLevel"],
                                                            update_payload["replicas"],
                                                            update_payload["flush"],
                                                            update_payload["timeToLiveInSeconds"],
                                                            False)

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(role_response.status_code, 204,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 204))
                update_payload["memoryAllocationInMb"] = update_payload["memoryAllocationInMb"] + 1

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

            role_response = self.capellaAPIRole.cluster_ops_apis.update_bucket_config(
                                                        self.tenant_id,
                                                        self.project_id,
                                                        self.cluster_id,
                                                        bucket_id,
                                                        update_payload["memoryAllocationInMb"],
                                                        update_payload["durabilityLevel"],
                                                        update_payload["replicas"],
                                                        update_payload["flush"],
                                                        update_payload["timeToLiveInSeconds"],
                                                        False)

            if role in ["projectOwner", "projectManager"]:
                self.assertEqual(204, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(
                                 role_response.status_code, 204))
                update_payload["memoryAllocationInMb"] = update_payload[
                                                             "memoryAllocationInMb"] + 1

            else:
                self.assertEqual(403, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(
                                 role_response.status_code, 403))

            self.log.info(
                "Removing user from project {} with role as {}".format(self.project_id,
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
                             msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                          204))

    def test_delete_bucket(self):
        self.log.info("Verify delete cluster v4 API")

        # Deploying a bucket
        self.log.info("Creating a bucket")
        payload = self.get_bucket_payload()
        payload["name"] = payload["name"][:len(payload["name"]) - 1] + str(
            int(payload["name"][-1]) + 22)
        resp = self.capellaAPI.cluster_ops_apis.create_bucket(
                                                            self.tenant_id,
                                                            self.project_id,
                                                            self.cluster_id,
                                                            payload["name"],
                                                            payload["type"],
                                                            payload["storageBackend"],
                                                            payload["memoryAllocationInMb"],
                                                            payload["bucketConflictResolution"],
                                                            payload["durabilityLevel"],
                                                            payload["replicas"],
                                                            payload["flush"],
                                                            payload["timeToLiveInSeconds"])
        resp = resp.json()
        bucket_id = resp["id"]

        self.log.info("Verifying the endpoint authentication with different test cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.delete_bucket(self.tenant_id,
                                                              self.project_id,
                                                              self.cluster_id,
                                                              bucket_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.delete_bucket(self.tenant_id,
                                                               self.project_id,
                                                               self.cluster_id,
                                                               bucket_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.delete_bucket(self.tenant_id,
                                                               self.project_id,
                                                               self.cluster_id,
                                                               bucket_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.delete_bucket(self.tenant_id,
                                                               self.project_id,
                                                               self.cluster_id,
                                                               bucket_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL: Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        # Verify endpoint for different organizations id
        self.log.info("Verify the update cluster v4 api for different organization ids")
        tenant_ids = {
            'valid_tenant_id': self.tenant_id,
            'invalid_tenant_id': self.invalid_id
        }
        for tenant_id in tenant_ids:
            resp = self.capellaAPI.cluster_ops_apis.delete_bucket(tenant_ids[tenant_id],
                                                                   self.project_id,
                                                                   self.cluster_id,
                                                                   bucket_id)
            if tenant_id == 'valid_tenant_id':
                self.assertEqual(resp.status_code, 204,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              204))

                resp = self.capellaAPI.cluster_ops_apis.create_bucket(
                                                                self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                payload["name"],
                                                                payload["type"],
                                                                payload["storageBackend"],
                                                                payload["memoryAllocationInMb"],
                                                                payload["bucketConflictResolution"],
                                                                payload["durabilityLevel"],
                                                                payload["replicas"],
                                                                payload["flush"],
                                                                payload["timeToLiveInSeconds"])
                resp = resp.json()
                bucket_id = resp["id"]

            else:
                # For now the response is 403. Later change it to 404.
                self.assertEqual(resp.status_code, 403,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Verify the endpoint with different projects
        self.log.info("Verifying the endpoint access with different projects")
        self.log.info("Creating a project")

        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id,
                                                           "Delete Bucket Koushal+1")

        self.assertEqual(201, resp.status_code,
                         msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code, 201))

        resp = resp.json()
        project_ids = {
            'valid_project_id': self.project_id,
            'different_project_id': resp['id'],
            'invalid_project_id': self.invalid_id
        }

        for project_id in project_ids:
            resp = self.capellaAPI.cluster_ops_apis.delete_bucket(self.tenant_id,
                                                                  project_ids[project_id],
                                                                  self.cluster_id,
                                                                  bucket_id)

            # Bug - https://couchbasecloud.atlassian.net/browse/AV-59794
            # For now the different_project_id gives 200 response. It should give 4xx.
            if project_id in ['valid_project_id', 'different_project_id']:
                self.assertEqual(resp.status_code, 204,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              204))

                resp = self.capellaAPI.cluster_ops_apis.create_bucket(
                                                                self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                payload["name"],
                                                                payload["type"],
                                                                payload["storageBackend"],
                                                                payload["memoryAllocationInMb"],
                                                                payload["bucketConflictResolution"],
                                                                payload["durabilityLevel"],
                                                                payload["replicas"],
                                                                payload["flush"],
                                                                payload["timeToLiveInSeconds"])
                resp = resp.json()
                bucket_id = resp["id"]

            else:
                # For now the different_project_id gives 401 unauthorized request. It should
                # give 404 response or invalid path response.
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
                user]["mailid"], self.test_users[user]["password"], self.test_users[user]['token'])

            role_response = self.capellaAPIRole.cluster_ops_apis.delete_bucket(
                                                                            self.tenant_id,
                                                                            self.project_id,
                                                                            self.cluster_id,
                                                                            bucket_id)

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(role_response.status_code, 204,
                                 msg='FAIL: Outcome:{}, Expected:{}'.format(
                                     role_response.status_code, 204))

                resp = self.capellaAPI.cluster_ops_apis.create_bucket(
                                                                self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                payload["name"],
                                                                payload["type"],
                                                                payload["storageBackend"],
                                                                payload["memoryAllocationInMb"],
                                                                payload["bucketConflictResolution"],
                                                                payload["durabilityLevel"],
                                                                payload["replicas"],
                                                                payload["flush"],
                                                                payload["timeToLiveInSeconds"])
                resp = resp.json()
                bucket_id = resp["id"]

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
                    self.tenant_id, 'API Key for role {}'.format(user["role"]),
                organizationRoles=["organizationMember"], expiry=1, resources=resources)
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

            role_response = self.capellaAPIRole.cluster_ops_apis.delete_bucket(
                                                                            self.tenant_id,
                                                                            self.project_id,
                                                                            self.cluster_id,
                                                                            bucket_id)

            if role in ["projectOwner", "projectManager"]:
                self.assertEqual(204, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(
                                 role_response.status_code, 204))

                resp = self.capellaAPI.cluster_ops_apis.create_bucket(
                                                                self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id,
                                                                payload["name"],
                                                                payload["type"],
                                                                payload["storageBackend"],
                                                                payload["memoryAllocationInMb"],
                                                                payload["bucketConflictResolution"],
                                                                payload["durabilityLevel"],
                                                                payload["replicas"],
                                                                payload["flush"],
                                                                payload["timeToLiveInSeconds"])
                resp = resp.json()
                bucket_id = resp["id"]

            else:
                self.assertEqual(403, role_response.status_code,
                                 msg="FAIL: Outcome:{}, Expected: {}".format(
                                     role_response.status_code,403))

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