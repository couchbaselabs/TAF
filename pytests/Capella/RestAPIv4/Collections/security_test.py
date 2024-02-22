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

        self.create_test_bucket()
        self.create_scope()

    def tearDown(self):
        super(SecurityTest, self).tearDown()

    def get_bucket_payload(self):
        payload = {
              "name": self.prefix + "Bucket_" + self.generate_random_string(3, False),
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

    def create_test_bucket(self):
        payload = self.get_bucket_payload()
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
        if resp.status_code == 201:
            self.bucket_id = resp.json()['id']

        else:
            self.fail("Unable to create bucket due to error -".format(resp.content))

        self.sleep(10, "Waiting for bucket to be loaded")

    def create_scope(self):
        self.scope_name = "Test_Scope"
        resp = self.capellaAPI.cluster_ops_apis.create_scope(self.tenant_id,
                                                             self.project_id,
                                                             self.cluster_id,
                                                             self.bucket_id,
                                                             self.scope_name)

        self.assertEqual(201, resp.status_code,
                         msg='FAIL. Outcome: {}, Expected: {}'.format(resp.status_code, 201))

    def test_create_collection(self):
        self.log.info("Verify creating collection for v4 APIs")

        self.log.info("Verifying the create collection endpoint authentication with different test "
                      "cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""

        resp = self.capellaAPI.cluster_ops_apis.create_collection(self.tenant_id,
                                                                  self.project_id,
                                                                  self.cluster_id,
                                                                  self.bucket_id,
                                                                  self.scope_name,
                                                                  "collection_1")
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.create_collection(self.tenant_id,
                                                                  self.project_id,
                                                                  self.cluster_id,
                                                                  self.bucket_id,
                                                                  self.scope_name,
                                                                  "collection_1")
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.create_collection(self.tenant_id,
                                                                  self.project_id,
                                                                  self.cluster_id,
                                                                  self.bucket_id,
                                                                  self.scope_name,
                                                                  "collection_1")
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.create_collection(self.tenant_id,
                                                                  self.project_id,
                                                                  self.cluster_id,
                                                                  self.bucket_id,
                                                                  self.scope_name,
                                                                  "collection_1")
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
            resp = self.capellaAPI.cluster_ops_apis.create_collection(tenant_ids[tenant_id],
                                                                      self.project_id,
                                                                      self.cluster_id,
                                                                      self.bucket_id,
                                                                      self.scope_name,
                                                                      "collection_1")

            if tenant_id == "valid_tenant_id":
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              201))

            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Trying out with different project ids
        self.log.info("Verifying the endpoint access with different projects")
        self.log.info("Creating a project")
        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id,
                                                           "Scopes_Project_Koushal")
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
            resp = self.capellaAPI.cluster_ops_apis.create_collection(self.tenant_id,
                                                                      project_ids[project_id],
                                                                      self.cluster_id,
                                                                      self.bucket_id,
                                                                      self.scope_name,
                                                                      "collection_2")

            if project_id == 'valid_project_id':
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              201))

            else:
                self.assertEqual(422, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              422))

        self.log.info("Deleting project")
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                           project_ids["different_project_id"])
        self.assertEqual(204, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 204))

        # Testing for different cluster ids
        self.log.info("Verifying endpoint for different cluster ids")
        cluster_ids = {
            'valid_cluster_id': self.cluster_id,
            'invalid_cluster_id': self.invalid_id
        }

        for cluster_id in cluster_ids:
            resp = self.capellaAPI.cluster_ops_apis.create_collection(self.tenant_id,
                                                                      self.project_id,
                                                                      cluster_ids[cluster_id],
                                                                      self.bucket_id,
                                                                      self.scope_name,
                                                                      "collection_3")

            if cluster_id == 'valid_cluster_id':
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              201))

            else:
                self.assertEqual(404, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        # Testing for different bucket ids
        self.log.info("Verifying endpoint for different bucket ids")
        bucket_ids = {
            'valid_bucket_id': self.bucket_id,
            'invalid_bucket_id': self.invalid_id
        }

        for bucket_id in bucket_ids:
            resp = self.capellaAPI.cluster_ops_apis.create_collection(self.tenant_id,
                                                                      self.project_id,
                                                                      self.cluster_id,
                                                                      bucket_ids[bucket_id],
                                                                      self.scope_name,
                                                                      "collection_4")

            if bucket_id == 'valid_bucket_id':
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(
                                     resp.status_code, 201))

            else:
                self.assertEqual(404, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(
                                     resp.status_code, 404))

        # Testing for Organizations RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"], self.test_users[user]['token'])

            resp = self.capellaAPIRole.cluster_ops_apis.create_collection(self.tenant_id,
                                                                          self.project_id,
                                                                          self.cluster_id,
                                                                          self.bucket_id,
                                                                          self.scope_name,
                                                                          "collection_5")

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              201))
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

            resp = self.capellaAPIRole.cluster_ops_apis.create_collection(self.tenant_id,
                                                                          self.project_id,
                                                                          self.cluster_id,
                                                                          self.bucket_id,
                                                                          self.scope_name,
                                                                          collection_name)

            if role in ["projectOwner", "projectManager"]:
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              201))

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

    def test_list_collection(self):
        self.log.info("Verify list collection endpoints for v4 APIs")

        self.log.info("Creating collections in the bucket")
        num = '1'
        self.scopes = list()
        for i in range(0, 5):
            collection_name = "Test_Collection_" + num
            resp = self.capellaAPI.cluster_ops_apis.create_collection(self.tenant_id,
                                                                      self.project_id,
                                                                      self.cluster_id,
                                                                      self.bucket_id,
                                                                      self.scope_name,
                                                                      collection_name)
            self.assertEqual(201, resp.status_code,
                             msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                          201))

            self.scopes.append(collection_name)
            num = num + '1'

        self.log.info("Verifying the list collection endpoint authentication with different test "
                      "cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""

        resp = self.capellaAPI.cluster_ops_apis.list_collections(self.tenant_id,
                                                                 self.project_id,
                                                                 self.cluster_id,
                                                                 self.bucket_id,
                                                                 self.scope_name)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.list_collections(self.tenant_id,
                                                                 self.project_id,
                                                                 self.cluster_id,
                                                                 self.bucket_id,
                                                                 self.scope_name)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.list_collections(self.tenant_id,
                                                                 self.project_id,
                                                                 self.cluster_id,
                                                                 self.bucket_id,
                                                                 self.scope_name)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.list_collections(self.tenant_id,
                                                                 self.project_id,
                                                                 self.cluster_id,
                                                                 self.bucket_id,
                                                                 self.scope_name)

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
            resp = self.capellaAPI.cluster_ops_apis.list_collections(tenant_ids[tenant_id],
                                                                     self.project_id,
                                                                     self.cluster_id,
                                                                     self.bucket_id,
                                                                     self.scope_name)

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
        self.log.info("Creating a project")
        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id,
                                                           "Scopes_Project_Koushal")
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
            resp = self.capellaAPI.cluster_ops_apis.list_collections(self.tenant_id,
                                                                     project_ids[project_id],
                                                                     self.cluster_id,
                                                                     self.bucket_id,
                                                                     self.scope_name)

            if project_id == 'valid_project_id':
                self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))

            else:
                self.assertEqual(422, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              422))

        self.log.info("Deleting project")
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                           project_ids["different_project_id"])
        self.assertEqual(204, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 204))

        # Testing for different cluster ids
        self.log.info("Verifying endpoint for different cluster ids")
        cluster_ids = {
            'valid_cluster_id': self.cluster_id,
            'invalid_cluster_id': self.invalid_id
        }

        for cluster_id in cluster_ids:
            resp = self.capellaAPI.cluster_ops_apis.list_collections(self.tenant_id,
                                                                     self.project_id,
                                                                     cluster_ids[cluster_id],
                                                                     self.bucket_id,
                                                                     self.scope_name)

            if cluster_id == 'valid_cluster_id':
                self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))

            else:
                self.assertEqual(404, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        # Testing for different bucket ids
        self.log.info("Verifying endpoint for different bucket ids")
        bucket_ids = {
            'valid_bucket_id': self.bucket_id,
            'invalid_bucket_id': self.invalid_id
        }

        for bucket_id in bucket_ids:
            resp = self.capellaAPI.cluster_ops_apis.list_collections(self.tenant_id,
                                                                     self.project_id,
                                                                     self.cluster_id,
                                                                     bucket_ids[bucket_id],
                                                                     self.scope_name)

            if bucket_id == 'valid_bucket_id':
                self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(
                                     resp.status_code, 200))

            else:
                self.assertEqual(404, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(
                                     resp.status_code, 404))

        # Testing for Organizations RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"], self.test_users[user]['token'])

            resp = self.capellaAPIRole.cluster_ops_apis.list_collections(self.tenant_id,
                                                                         self.project_id,
                                                                         self.cluster_id,
                                                                         self.bucket_id,
                                                                         self.scope_name)

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

            resp = self.capellaAPIRole.cluster_ops_apis.list_collections(self.tenant_id,
                                                                         self.project_id,
                                                                         self.cluster_id,
                                                                         self.bucket_id,
                                                                         self.scope_name)

            self.assertEqual(200, resp.status_code,
                             msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                          200))

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

    def test_get_collection(self):
        self.log.info("Verify get collection endpoints for v4 APIs")

        self.log.info("Creating a collection in the bucket")
        resp = self.capellaAPI.cluster_ops_apis.create_collection(self.tenant_id,
                                                                  self.project_id,
                                                                  self.cluster_id,
                                                                  self.bucket_id,
                                                                  self.scope_name,
                                                                  "collection_1")
        self.assertEqual(201, resp.status_code,
                         msg='FAIL, Outcome: {}, Expected: {} Fail to create a bucket'.format(
                             resp.status_code, 201))

        self.log.info("Verifying the get collection endpoint authentication with different test "
                      "cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""

        resp = self.capellaAPI.cluster_ops_apis.fetch_collection_info(self.tenant_id,
                                                                      self.project_id,
                                                                      self.cluster_id,
                                                                      self.bucket_id,
                                                                      self.scope_name,
                                                                      "collection_1")
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.fetch_collection_info(self.tenant_id,
                                                                      self.project_id,
                                                                      self.cluster_id,
                                                                      self.bucket_id,
                                                                      self.scope_name,
                                                                      "collection_1")
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.fetch_collection_info(self.tenant_id,
                                                                      self.project_id,
                                                                      self.cluster_id,
                                                                      self.bucket_id,
                                                                      self.scope_name,
                                                                      "collection_1")
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.fetch_collection_info(self.tenant_id,
                                                                      self.project_id,
                                                                      self.cluster_id,
                                                                      self.bucket_id,
                                                                      self.scope_name,
                                                                      "collection_1")

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
            resp = self.capellaAPI.cluster_ops_apis.fetch_collection_info(tenant_ids[tenant_id],
                                                                          self.project_id,
                                                                          self.cluster_id,
                                                                          self.bucket_id,
                                                                          self.scope_name,
                                                                          "collection_1")

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
        self.log.info("Creating a project")
        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id,
                                                           "Scopes_Project_Koushal")
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
            resp = self.capellaAPI.cluster_ops_apis.fetch_collection_info(self.tenant_id,
                                                                          project_ids[project_id],
                                                                          self.cluster_id,
                                                                          self.bucket_id,
                                                                          self.scope_name,
                                                                          "collection_1")

            if project_id == 'valid_project_id':
                self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))

            else:
                self.assertEqual(422, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              422))

        self.log.info("Deleting project")
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                           project_ids["different_project_id"])
        self.assertEqual(204, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 204))

        # Testing for different cluster ids
        self.log.info("Verifying endpoint for different cluster ids")
        cluster_ids = {
            'valid_cluster_id': self.cluster_id,
            'invalid_cluster_id': self.invalid_id
        }

        for cluster_id in cluster_ids:
            resp = self.capellaAPI.cluster_ops_apis.fetch_collection_info(self.tenant_id,
                                                                          self.project_id,
                                                                          cluster_ids[cluster_id],
                                                                          self.bucket_id,
                                                                          self.scope_name,
                                                                          "collection_1")

            if cluster_id == 'valid_cluster_id':
                self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))

            else:
                self.assertEqual(404, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        # Testing for different bucket ids
        self.log.info("Verifying endpoint for different bucket ids")
        bucket_ids = {
            'valid_bucket_id': self.bucket_id,
            'invalid_bucket_id': self.invalid_id
        }

        for bucket_id in bucket_ids:
            resp = self.capellaAPI.cluster_ops_apis.fetch_collection_info(self.tenant_id,
                                                                          self.project_id,
                                                                          self.cluster_id,
                                                                          bucket_ids[bucket_id],
                                                                          self.scope_name,
                                                                          "collection_1")

            if bucket_id == 'valid_bucket_id':
                self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(
                                     resp.status_code, 200))

            else:
                self.assertEqual(404, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(
                                     resp.status_code, 404))

        # Testing for Organizations RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"], self.test_users[user]['token'])

            resp = self.capellaAPIRole.cluster_ops_apis.fetch_collection_info(self.tenant_id,
                                                                              self.project_id,
                                                                              self.cluster_id,
                                                                              self.bucket_id,
                                                                              self.scope_name,
                                                                              "collection_1")

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

            resp = self.capellaAPIRole.cluster_ops_apis.fetch_collection_info(self.tenant_id,
                                                                              self.project_id,
                                                                              self.cluster_id,
                                                                              self.bucket_id,
                                                                              self.scope_name,
                                                                              "collection_1")

            self.assertEqual(200, resp.status_code,
                             msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                          200))

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

    def test_delete_collection(self):
        self.log.info("Verify delete collection endpoints for v4 APIs")

        self.log.info("Creating collection in the bucket")
        num = 1
        self.collections = list()
        for i in range(0, 9):       # Creating 7 collections as total 7 cases will return HTTP OK
            collection_name = "Test_Collection_" + str(num)
            resp = self.capellaAPI.cluster_ops_apis.create_collection(self.tenant_id,
                                                                      self.project_id,
                                                                      self.cluster_id,
                                                                      self.bucket_id,
                                                                      self.scope_name,
                                                                      collection_name)
            self.assertEqual(201, resp.status_code,
                             msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                          201))

            self.collections.append(collection_name)
            num = num + 1

        num = 0
        self.log.info("Verifying the delete scope endpoint authentication with different test "
                      "cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""

        resp = self.capellaAPI.cluster_ops_apis.delete_collection(self.tenant_id,
                                                                  self.project_id,
                                                                  self.cluster_id,
                                                                  self.bucket_id,
                                                                  self.scope_name,
                                                                  self.collections[num])
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.delete_collection(self.tenant_id,
                                                                  self.project_id,
                                                                  self.cluster_id,
                                                                  self.bucket_id,
                                                                  self.scope_name,
                                                                  self.collections[num])
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.delete_collection(self.tenant_id,
                                                                  self.project_id,
                                                                  self.cluster_id,
                                                                  self.bucket_id,
                                                                  self.scope_name,
                                                                  self.collections[num])
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.delete_collection(self.tenant_id,
                                                                  self.project_id,
                                                                  self.cluster_id,
                                                                  self.bucket_id,
                                                                  self.scope_name,
                                                                  self.collections[num])

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
            resp = self.capellaAPI.cluster_ops_apis.delete_collection(tenant_ids[tenant_id],
                                                                      self.project_id,
                                                                      self.cluster_id,
                                                                      self.bucket_id,
                                                                      self.scope_name,
                                                                      self.collections[num])

            if tenant_id == "valid_tenant_id":
                self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))
                num = num + 1

            else:
                self.assertEqual(403, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              403))

        # Trying out with different project ids
        self.log.info("Verifying the endpoint access with different projects")
        self.log.info("Creating a project")
        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id,
                                                           "Scopes_Project_Koushal")
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
            resp = self.capellaAPI.cluster_ops_apis.delete_collection(self.tenant_id,
                                                                      project_ids[project_id],
                                                                      self.cluster_id,
                                                                      self.bucket_id,
                                                                      self.scope_name,
                                                                      self.collections[num])

            if project_id == 'valid_project_id':
                self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))
                num = num + 1

            else:
                self.assertEqual(422, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              422))

        self.log.info("Deleting project")
        resp = self.capellaAPI.org_ops_apis.delete_project(self.tenant_id,
                                                           project_ids["different_project_id"])
        self.assertEqual(204, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 204))

        # Testing for different cluster ids
        self.log.info("Verifying endpoint for different cluster ids")
        cluster_ids = {
            'valid_cluster_id': self.cluster_id,
            'invalid_cluster_id': self.invalid_id
        }

        for cluster_id in cluster_ids:
            resp = self.capellaAPI.cluster_ops_apis.delete_collection(self.tenant_id,
                                                                      self.project_id,
                                                                      cluster_ids[cluster_id],
                                                                      self.bucket_id,
                                                                      self.scope_name,
                                                                      self.collections[num])

            if cluster_id == 'valid_cluster_id':
                self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))
                num = num + 1

            else:
                self.assertEqual(404, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              404))

        # Testing for different bucket ids
        self.log.info("Verifying endpoint for different bucket ids")
        bucket_ids = {
            'valid_bucket_id': self.bucket_id,
            'invalid_bucket_id': self.invalid_id
        }

        for bucket_id in bucket_ids:
            resp = self.capellaAPI.cluster_ops_apis.delete_collection(self.tenant_id,
                                                                      self.project_id,
                                                                      self.cluster_id,
                                                                      bucket_ids[bucket_id],
                                                                      self.scope_name,
                                                                      self.collections[num])

            if bucket_id == 'valid_bucket_id':
                self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(
                                     resp.status_code, 200))
                num = num + 1

            else:
                self.assertEqual(404, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(
                                     resp.status_code, 404))

        # Testing for different scope ids
        self.log.info("Verifying endpoint for different bucket ids")
        scopes = {
            'valid_scope_name': self.scope_name,
            'invalid_scope_name': "abcdefg"
        }

        for scope in scopes:
            resp = self.capellaAPI.cluster_ops_apis.delete_collection(self.tenant_id,
                                                                      self.project_id,
                                                                      self.cluster_id,
                                                                      self.bucket_id,
                                                                      scopes[scope],
                                                                      self.collections[num])

            if scope == 'valid_scope_name':
                self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(
                                     resp.status_code, 200))
                num = num + 1

            else:
                self.assertEqual(404, resp.status_code,
                                 msg='FAIL: Outcome: {}, Expected: {}'.format(
                                     resp.status_code, 404))

        # Testing for Organizations RBAC roles
        self.log.info("Verifying endpoint for different roles under organization - RBAC")
        for user in self.test_users:
            self.log.info("Checking with role - {}".format(self.test_users[user]["role"]))

            self.capellaAPIRole = CapellaAPI("https://" + self.url, '', '', self.test_users[
                user]["mailid"], self.test_users[user]["password"], self.test_users[user]['token'])

            resp = self.capellaAPIRole.cluster_ops_apis.delete_collection(self.tenant_id,
                                                                          self.project_id,
                                                                          self.cluster_id,
                                                                          self.bucket_id,
                                                                          self.scope_name,
                                                                          self.collections[num])

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))
                num = num + 1
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

            resp = self.capellaAPIRole.cluster_ops_apis.delete_collection(self.tenant_id,
                                                                          self.project_id,
                                                                          self.cluster_id,
                                                                          self.bucket_id,
                                                                          self.scope_name,
                                                                          self.collections[num])

            if role in ["projectOwner", "projectManager"]:
                self.assertEqual(200, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              200))
                num = (num + 1) % 8

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