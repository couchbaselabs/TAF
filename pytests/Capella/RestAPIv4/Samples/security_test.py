import time
# import json
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

    def test_create_sample_bucket(self):
        self.log.info("Verify sample bucket imports for v4 APIs")

        self.log.info("Verifying the sample buckets import endpoint authentication with different "
                      "test cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.create_sample_bucket(self.tenant_id,
                                                                     self.project_id,
                                                                     self.cluster_id,
                                                                     "travel-sample")
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.create_sample_bucket(self.tenant_id,
                                                                     self.project_id,
                                                                     self.cluster_id,
                                                                     "travel-sample")
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.create_sample_bucket(self.tenant_id,
                                                                     self.project_id,
                                                                     self.cluster_id,
                                                                     "travel-sample")
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.create_sample_bucket(self.tenant_id,
                                                                     self.project_id,
                                                                     self.cluster_id,
                                                                     "travel-sample")
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
            resp = self.capellaAPI.cluster_ops_apis.create_sample_bucket(tenant_ids[tenant_id],
                                                                         self.project_id,
                                                                         self.cluster_id,
                                                                         "travel-sample")

            if tenant_id == "valid_tenant_id":
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              201))
                bucket_id = resp.json()['bucketId']
                self.sleep(20, "Waiting for bucket to be loaded")
                resp = self.capellaAPI.cluster_ops_apis.delete_sample_bucket(tenant_ids[tenant_id],
                                                                             self.project_id,
                                                                             self.cluster_id,
                                                                             bucket_id)
                self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              204))

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
            resp = self.capellaAPI.cluster_ops_apis.create_sample_bucket(self.tenant_id,
                                                                         project_ids[project_id],
                                                                         self.cluster_id,
                                                                         "beer-sample")

            if project_id == 'valid_project_id':
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              201))
                bucket_id = resp.json()['bucketId']
                self.sleep(20, "Waiting for bucket to be loaded")
                resp = self.capellaAPI.cluster_ops_apis.delete_sample_bucket(self.tenant_id,
                                                                             project_ids[project_id],
                                                                             self.cluster_id,
                                                                             bucket_id)
                self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              204))

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
            resp = self.capellaAPI.cluster_ops_apis.create_sample_bucket(self.tenant_id,
                                                                         self.project_id,
                                                                         cluster_ids[cluster_id],
                                                                         "gamesim-sample")

            if cluster_id == 'valid_cluster_id':
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              201))
                bucket_id = resp.json()['bucketId']
                self.sleep(20, "Waiting for bucket to be loaded")
                resp = self.capellaAPI.cluster_ops_apis.delete_sample_bucket(self.tenant_id,
                                                                             self.project_id,
                                                                             cluster_ids[cluster_id],
                                                                             bucket_id)
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

            resp = self.capellaAPIRole.cluster_ops_apis.create_sample_bucket(self.tenant_id,
                                                                             self.project_id,
                                                                             self.cluster_id,
                                                                             "travel-sample")

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              201))
                bucket_id = resp.json()['bucketId']
                self.sleep(20, "Waiting for bucket to be loaded")
                resp = self.capellaAPI.cluster_ops_apis.delete_sample_bucket(self.tenant_id,
                                                                             self.project_id,
                                                                             self.cluster_id,
                                                                             bucket_id)
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
        user = self.test_users["User2"]

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

            resp = self.capellaAPIRole.cluster_ops_apis.create_sample_bucket(self.tenant_id,
                                                                             self.project_id,
                                                                             self.cluster_id,
                                                                             "travel-sample")

            if role in ["projectOwner", "projectManager"]:
                self.assertEqual(201, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              201))
                bucket_id = resp.json()['bucketId']
                self.sleep(20, "Waiting for bucket to be loaded")
                resp = self.capellaAPI.cluster_ops_apis.delete_sample_bucket(self.tenant_id,
                                                                             self.project_id,
                                                                             self.cluster_id,
                                                                             bucket_id)
                self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              204))

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

    def test_list_sample_buckets(self):
        self.log.info("Verify list sample buckets endpoints for v4 APIs")

        self.log.info("Creating sample buckets")
        sample_buckets = ["travel-sample", "beer-sample", "gamesim-sample"]
        for bucket in sample_buckets:
            resp = self.capellaAPI.cluster_ops_apis.create_sample_bucket(self.tenant_id,
                                                                        self.project_id,
                                                                        self.cluster_id,
                                                                        bucket)
            self.assertEqual(201, resp.status_code,
                             msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                          201))

        self.log.info("Verifying the list scope endpoint authentication with different test "
                      "cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""

        resp = self.capellaAPI.cluster_ops_apis.list_sample_buckets(self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.list_sample_buckets(self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.list_sample_buckets(self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.list_sample_buckets(self.tenant_id,
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
            resp = self.capellaAPI.cluster_ops_apis.list_sample_buckets(tenant_ids[tenant_id],
                                                                        self.project_id,
                                                                        self.cluster_id)

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
                                                           "Samples_Koushal_Fetch")
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
            resp = self.capellaAPI.cluster_ops_apis.list_sample_buckets(self.tenant_id,
                                                                        project_ids[project_id],
                                                                        self.cluster_id)

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
            resp = self.capellaAPI.cluster_ops_apis.list_sample_buckets(self.tenant_id,
                                                                        self.project_id,
                                                                        cluster_ids[cluster_id])

            if cluster_id == 'valid_cluster_id':
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

            resp = self.capellaAPIRole.cluster_ops_apis.list_sample_buckets(self.tenant_id,
                                                                            self.project_id,
                                                                            self.cluster_id)

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

            resp = self.capellaAPIRole.cluster_ops_apis.list_sample_buckets(self.tenant_id,
                                                                            self.project_id,
                                                                            self.cluster_id)

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

    def test_fetch_sample_bucket(self):
        self.log.info("Verify fetch sample bucket endpoints for v4 APIs")

        self.log.info("Creating a sample bucket in the bucket")
        resp = self.capellaAPI.cluster_ops_apis.create_sample_bucket(self.tenant_id,
                                                                     self.project_id,
                                                                     self.cluster_id,
                                                                     "travel-sample")
        self.assertEqual(201, resp.status_code,
                         msg='FAIL, Outcome: {}, Expected: {} Fail to create a sample '
                             'bucket'.format(resp.status_code, 201))
        bucketId = resp.json()['bucketId']

        self.log.info("Verifying the fetch sample bucket endpoint authentication with different "
                      "test cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.fetch_sample_bucket(self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    bucketId)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.fetch_sample_bucket(self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    bucketId)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.fetch_sample_bucket(self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    bucketId)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.fetch_sample_bucket(self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    bucketId)

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
            resp = self.capellaAPI.cluster_ops_apis.fetch_sample_bucket(tenant_ids[tenant_id],
                                                                         self.project_id,
                                                                         self.cluster_id,
                                                                         bucketId)

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
                                                           "Samples_Project")
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
            resp = self.capellaAPI.cluster_ops_apis.fetch_sample_bucket(self.tenant_id,
                                                                        project_ids[project_id],
                                                                        self.cluster_id,
                                                                        bucketId)

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
            resp = self.capellaAPI.cluster_ops_apis.fetch_sample_bucket(self.tenant_id,
                                                                        self.project_id,
                                                                        cluster_ids[cluster_id],
                                                                        bucketId)

            if cluster_id == 'valid_cluster_id':
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

            resp = self.capellaAPIRole.cluster_ops_apis.fetch_sample_bucket(self.tenant_id,
                                                                            self.project_id,
                                                                            self.cluster_id,
                                                                            bucketId)

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

            resp = self.capellaAPIRole.cluster_ops_apis.fetch_sample_bucket(self.tenant_id,
                                                                            self.project_id,
                                                                            self.cluster_id,
                                                                            bucketId)

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

    def test_delete_sample_bucket(self):
        self.log.info("Verify delete scopes endpoints for v4 APIs")

        self.log.info("Creating sample buckets in the cluster")
        resp = self.capellaAPI.cluster_ops_apis.create_sample_bucket(self.tenant_id,
                                                                     self.project_id,
                                                                     self.cluster_id,
                                                                     "travel-sample")
        self.assertEqual(201, resp.status_code,
                         msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code, 201))
        bucket_id = resp.json()["bucketId"]

        num = 0
        self.log.info("Verifying the delete scope endpoint authentication with different test "
                      "cases")
        self.log.info("     1. Empty AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""

        resp = self.capellaAPI.cluster_ops_apis.delete_sample_bucket(self.tenant_id,
                                                                     self.project_id,
                                                                     self.cluster_id,
                                                                     bucket_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     2. Empty SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = ""
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.delete_sample_bucket(self.tenant_id,
                                                                     self.project_id,
                                                                     self.cluster_id,
                                                                     bucket_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     3. Invalid AccessKey")
        self.capellaAPI.cluster_ops_apis.ACCESS = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.delete_sample_bucket(self.tenant_id,
                                                                     self.project_id,
                                                                     self.cluster_id,
                                                                     bucket_id)
        self.assertEqual(401, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 401))
        self.reset_api_keys()

        self.log.info("     4. Invalid SecretKey")
        self.capellaAPI.cluster_ops_apis.SECRET = self.invalid_id
        self.capellaAPI.cluster_ops_apis.bearer_token = ""
        resp = self.capellaAPI.cluster_ops_apis.delete_sample_bucket(self.tenant_id,
                                                                     self.project_id,
                                                                     self.cluster_id,
                                                                     bucket_id)

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
            resp = self.capellaAPI.cluster_ops_apis.delete_sample_bucket(tenant_ids[tenant_id],
                                                                         self.project_id,
                                                                         self.cluster_id,
                                                                         bucket_id)

            if tenant_id == "valid_tenant_id":
                self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              204))
                resp = self.capellaAPI.cluster_ops_apis.create_sample_bucket(self.tenant_id,
                                                                             self.project_id,
                                                                             self.cluster_id,
                                                                             "beer-sample")
                self.assertEqual(201, resp.status_code,
                                 msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code,
                                                                              201))
                bucket_id = resp.json()["bucketId"]
                self.sleep(20, "Waiting for bucket to be loaded")

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
            resp = self.capellaAPI.cluster_ops_apis.delete_sample_bucket(self.tenant_id,
                                                                         project_ids[project_id],
                                                                         self.cluster_id,
                                                                         bucket_id)

            if project_id == 'valid_project_id':
                self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              204))
                resp = self.capellaAPI.cluster_ops_apis.create_sample_bucket(self.tenant_id,
                                                                             self.project_id,
                                                                             self.cluster_id,
                                                                             "gamesim-sample")
                self.assertEqual(201, resp.status_code,
                                 msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code,
                                                                              201))
                bucket_id = resp.json()["bucketId"]
                self.sleep(20, "Waiting for bucket to be loaded")

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
            resp = self.capellaAPI.cluster_ops_apis.delete_sample_bucket(self.tenant_id,
                                                                         self.project_id,
                                                                         cluster_ids[cluster_id],
                                                                         bucket_id)

            if cluster_id == 'valid_cluster_id':
                self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              204))
                resp = self.capellaAPI.cluster_ops_apis.create_sample_bucket(self.tenant_id,
                                                                             self.project_id,
                                                                             self.cluster_id,
                                                                             "travel-sample")
                self.assertEqual(201, resp.status_code,
                                 msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code,
                                                                              201))
                bucket_id = resp.json()["bucketId"]
                self.sleep(20, "Waiting for bucket to be loaded")

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

            resp = self.capellaAPIRole.cluster_ops_apis.delete_sample_bucket(self.tenant_id,
                                                                             self.project_id,
                                                                             self.cluster_id,
                                                                             bucket_id)

            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              204))
                resp = self.capellaAPI.cluster_ops_apis.create_sample_bucket(self.tenant_id,
                                                                             self.project_id,
                                                                             self.cluster_id,
                                                                             "beer-sample")
                self.assertEqual(201, resp.status_code,
                                 msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code,
                                                                              201))
                bucket_id = resp.json()["bucketId"]
                self.sleep(20, "Waiting for bucket to be loaded")
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

            resp = self.capellaAPIRole.cluster_ops_apis.delete_sample_bucket(self.tenant_id,
                                                                             self.project_id,
                                                                             self.cluster_id,
                                                                             bucket_id)

            if role in ["projectOwner", "projectManager"]:
                self.assertEqual(204, resp.status_code,
                                 msg='FAIL, Outcome: {}, Expected: {}'.format(resp.status_code,
                                                                              204))
                resp = self.capellaAPI.cluster_ops_apis.create_sample_bucket(self.tenant_id,
                                                                             self.project_id,
                                                                             self.cluster_id,
                                                                             "beer-sample")
                self.assertEqual(201, resp.status_code,
                                 msg="FAIL: Outcome: {}, Expected: {}".format(resp.status_code,
                                                                              201))
                bucket_id = resp.json()["bucketId"]
                self.sleep(20, "Waiting for bucket to be loaded")

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