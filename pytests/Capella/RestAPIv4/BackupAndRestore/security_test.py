import json
import threading
import time
import random
import string
import uuid

from pytests.basetestcase import BaseTestCase
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI


def get_random_dummy_email():
    return 'dummy.user+' + str(random.random()) + '@couchbase.com'


def get_random_string_of_given_length(uid=False, letters=True, digits=True, special_chars=False, length=64):
    """
    Can be used to generate random string of any length.
    ie. name, description, secret keys, etc.
    default config is for random secret keys and access keys.
    """
    if uid:
        return str(uuid.uuid4())

    characters = ""
    if letters:
        characters += string.ascii_letters
    if digits:
        characters += string.digits
    if special_chars:
        characters += string.punctuation

    return ''.join(random.choice(characters) for _ in range(length))


class SecurityTest(BaseTestCase):
    cidr = "10.0.0.0"

    def setUp(self):
        BaseTestCase.setUp(self)
        self.url = self.input.capella.get("pod")
        self.user = self.input.capella.get("capella_user")
        self.passwd = self.input.capella.get("capella_pwd")
        self.tenant_id = self.input.capella.get("tenant_id")
        self.cluster_id = self.cluster.id

        if self.input.capella.get("diff_tenant_id"):
            self.diff_tenant_id = self.input.capella.get("diff_tenant_id")
        else:
            for tenant in self.tenants:
                self.diff_tenant_id = tenant.id.encode('utf-8')
                # self.diff_tenant_project_ids = tenant.project_id.encode('utf-8')

        self.capellaAPI = CapellaAPI("https://" + self.url, '',
                                     '', self.user, self.passwd, '')

        if self.input.capella.get("timeout"):
            self.timeout = self.input.capella.get("timeout")
        else:
            self.timeout = 1000

        resp = self.capellaAPI.create_control_plane_api_key(self.tenant_id, 'init api keys')
        resp = resp.json()

        self.capellaAPI.org_ops_apis.ACCESS = resp['accessKey']
        self.capellaAPI.org_ops_apis.bearer_token = resp['token']

        self.capellaAPI.cluster_ops_apis.bearer_token = resp['token']

        self.access_key_ini = resp['accessKey']
        self.bearer_token_ini = resp['token']

        self.project_id = self.tenant.project_id

        resp = self.capellaAPI.org_ops_apis.create_project(self.tenant_id, 'Secondary project')
        self.log.info(resp.json())
        self.secondary_project_id = resp.json()['id']

        self.cluster_id = self.cluster.id
        self.invalid_id = "00000000-0000-0000-0000-000000000000"
        self.api_keys = []
        self.users = []
        self.db_creds = []
        self.clusters_list = []

        # load_sample_bucket
        self.bucket_name = 'travel-sample'
        resp = self.capellaAPI.load_sample_bucket(tenant_id=self.tenant_id, project_id=self.project_id,
                                                  cluster_id=self.cluster_id, bucket_name=self.bucket_name)
        self.assertEqual(resp.status_code, 201)

        resp = self.capellaAPI.cluster_ops_apis.list_buckets(organizationId=self.tenant_id, projectId=self.project_id,
                                                             clusterId=self.cluster_id)

        beer_sample = filter(lambda x: x['name'] == self.bucket_name, resp.json()['data'])

        self.api_keys_list = self.rbac_roles_generator_wrapper()

        self.bucket_id = beer_sample[0]['id']

        if self.input.capella.get("test_users"):
            self.test_users = json.loads(self.input.capella.get("test_users"))
        else:
            self.test_users = {"User1": {"password": self.passwd, "mailid": self.user,
                                         "role": "organizationOwner"}}

    def wait_till_cluster_is_healthy(self, organizationId, projectId, clusterId):
        end_time = time.time() + self.timeout
        while end_time > time.time():

            self.log.info("Polling cluster status, and waiting for it's status become healthy.")

            resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.fetch_cluster_info,
                                                  organizationId=organizationId, projectId=projectId,
                                                  clusterId=clusterId)
            if resp.status_code != 200:
                return False
            elif resp.json()['currentState'] == 'healthy':
                return True
            else:
                time.sleep(10)

        return False

    def wait_till_cluster_is_deleted(self, organizationId, projectId, clusterId):
        end_time = time.time() + self.timeout
        while end_time > time.time():

            self.log.info("Polling cluster status, and waiting for it to be destroyed.")

            resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.fetch_cluster_info,
                                                  organizationId=organizationId, projectId=projectId,
                                                  clusterId=clusterId)
            if resp.status_code == 404:
                return True
            else:
                time.sleep(10)

        return False

    def tearDown(self):
        failures = []
        self.reset_access_keys_to_default()

        for cluster in self.clusters_list:

            resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.delete_cluster,
                                                  organizationId=cluster['tenant'],
                                                  projectId=cluster['project'],
                                                  clusterId=cluster['cluster'])

            if resp.status_code != 202:
                failures.append("Error while deleting Cluster {}"
                                .format(cluster['cluster']))

        for cluster in self.clusters_list:
            self.wait_till_cluster_is_deleted(organizationId=cluster['tenant'],
                                              projectId=cluster['project'],
                                              clusterId=cluster['cluster'])

        for user in self.users:

            resp = self.make_call_to_given_method(method=self.capellaAPI.org_ops_apis.delete_user,
                                                  organizationId=user['tenant'],
                                                  userId=user["user"])

            if resp.status_code != 204:
                failures.append("Error while deleting User {}"
                                .format(user['user']))

        for api_key in self.api_keys:
            resp = self.make_call_to_given_method(method=self.capellaAPI.org_ops_apis.delete_api_key,
                                                  organizationId=api_key['tenant'],
                                                  accessKey=api_key["access_key"])

            if resp.status_code != 204:
                failures.append("Error while deleting api key {}"
                                .format(api_key['access_key']))

        for db_cred in self.db_creds:
            resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.delete_database_user,
                                                  organizationId=db_cred['tenant'],
                                                  projectId=db_cred['project'],
                                                  clusterId=db_cred['cluster'],
                                                  userId=db_cred['db_id'])

            if resp.status_code != 204:
                failures.append("Error while deleting db credential key {}"
                                .format(db_cred['db_id']))

        resp = self.make_call_to_given_method(method=self.capellaAPI.org_ops_apis.delete_project,
                                              organizationId=self.tenant_id,
                                              projectId=self.secondary_project_id)
        if resp.status_code != 204:
            failures.append("Error while deleting project with id {}"
                            .format(self.secondary_project_id))

        if failures:
            self.fail("Following error occurred in teardown - {}".format(
                failures))
        super(SecurityTest, self).tearDown()

    def reset_access_keys_to_default(self):
        # resetting keys to default
        self.capellaAPI.org_ops_apis.ACCESS = self.access_key_ini
        self.capellaAPI.org_ops_apis.bearer_token = self.bearer_token_ini

        self.capellaAPI.cluster_ops_apis.ACCESS = self.access_key_ini
        self.capellaAPI.cluster_ops_apis.bearer_token = self.bearer_token_ini

    def set_access_keys(self, access_key, token):
        self.capellaAPI.org_ops_apis.ACCESS = access_key
        self.capellaAPI.org_ops_apis.bearer_token = token

        self.capellaAPI.cluster_ops_apis.ACCESS = access_key
        self.capellaAPI.cluster_ops_apis.bearer_token = token

    def append_to_api_keys(self, acc, tenant):
        self.api_keys.append({
            'access_key': acc,
            'tenant': tenant
        })

    def append_to_users(self, user, tenant):
        self.users.append({
            'user': user,
            'tenant': tenant
        })

    def append_to_db_creds(self, tenant, project, cluster, db_id):
        self.db_creds.append({
            "tenant": tenant,
            'project': project,
            'cluster': cluster,
            'db_id': db_id
        })

    def get_backup_id_for_given_bucket(self, bucketId, organizationId, projectId, clusterId):

        end_time = time.time() + self.timeout
        while end_time > time.time():

            self.log.info("Polling backup, and waiting for it to be available.")
            resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.list_backups,
                                                  organizationId=organizationId, projectId=projectId,
                                                  clusterId=clusterId)

            if resp.status_code == 200:
                data = resp.json()['data']
                target_backup = list(filter(lambda x: (x['bucketID'] == bucketId), data))[0]

                return target_backup['id']
            else:
                time.sleep(10)

        self.log.info("Backup id could not be found.")

    def wait_till_backup_in_ready_state(self, backupId, organizationId, projectId, clusterId):
        end_time = time.time() + self.timeout
        while end_time > time.time():

            self.log.info("Polling backup status, and waiting for it to be in ready state.")

            resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.get_backup,
                                                  organizationId=organizationId, projectId=projectId,
                                                  clusterId=clusterId, backupId=backupId)
            if resp.json()['status'] == 'ready':
                return True
            else:
                time.sleep(10)

        return False

    def create_cluster(self, tenant_id, project_id):

        payload = {
            "name": "AWS-Test-Cluster-V4-Security",
            "description": "My first test aws cluster for multiple services.",
            "cloudProvider": {
                "type": "aws",
                "region": "us-east-1",
                "cidr": "10.7.22.0/23"
            },
            "couchbaseServer": {
                "version": "7.1"
            },
            "serviceGroups": [
                {
                    "node": {
                        "compute": {
                            "cpu": 4,
                            "ram": 16
                        },
                        "disk": {
                            "storage": 50,
                            "type": "gp3",
                            "iops": 3000
                        }
                    },
                    "numOfNodes": 3,
                    "services": [
                        "data",
                        "query",
                        "index",
                        "search"
                    ]
                },
                {
                    "node": {
                        "compute": {
                            "cpu": 4,
                            "ram": 32
                        },
                        "disk": {
                            "storage": 50,
                            "type": "io2",
                            "iops": 3005
                        }
                    },
                    "numOfNodes": 2,
                    "services": [
                        "analytics"
                    ]
                }
            ],
            "availability": {
                "type": "multi"
            },
            "support": {
                "plan": "developer pro",
                "timezone": "PT"
            }
        }

        end_time = time.time() + self.timeout
        while time.time() < end_time:
            subnet = self.get_next_cidr() + "/20"
            payload["cloudProvider"]["cidr"] = subnet
            self.log.info("Trying out with cidr {}".format(subnet))

            resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.create_cluster,
                                                  organizationId=tenant_id, projectId=project_id,
                                                  name=payload["name"], cloudProvider=payload["cloudProvider"],
                                                  couchbaseServer=payload["couchbaseServer"],
                                                  serviceGroups=payload["serviceGroups"],
                                                  availability=payload["availability"],
                                                  support=payload["support"])
            temp_resp = resp.json()

            if resp.status_code == 202:
                self.clusters_list.append({'tenant': self.tenant_id, 'project': project_id,
                                           'cluster': resp.json()['id']})
                return resp.json()['id']

            elif "Please ensure you are passing a unique CIDR block and try again" \
                    in temp_resp["message"]:
                continue
            else:
                self.assertFalse(resp.status_code, "Failed to create a cluster with error "
                                                   "as {}".format(resp.content))

    @staticmethod
    def get_next_cidr():
        addr = SecurityTest.cidr.split(".")
        if int(addr[1]) < 255:
            addr[1] = str(int(addr[1]) + 1)
        elif int(addr[2]) < 255:
            addr[2] = str(int(addr[2]) + 1)
        SecurityTest.cidr = ".".join(addr)
        return SecurityTest.cidr

    def make_call_to_given_method(self, method, **kwargs):
        """
        to avoid unnecessary rate limit issues during test.
        """
        resp = method(**kwargs)

        if resp.status_code == 429:
            self.log.info("Rate limit exceeded, sleep for 1 min.")
            time.sleep(60)
            resp = method(**kwargs)

        return resp

    def rate_limit_wrapper(self, method=None, **kwargs):
        """
        make 100 request to the given endpoint using method and kwargs.
        using multiple threads just to ensure all the api call are completed in the window of
        1 minute.
        if it still exceeds 1 minute then we are passing the test.
        """

        self.log.info("Rate limit test for method {} \nkwargs :{}".format(method.__name__, kwargs))

        self.reset_access_keys_to_default()

        resp_list = [None] * 101

        def call_endpoint(idx):

            for i in range(5):
                resp = method(**kwargs)
                resp_list[idx * 5 + i] = resp

        self.log.info("sleep for 1 min, to avoid any effect of the previous test.")
        time.sleep(60)

        st_time = time.time()

        threads = []
        for i in range(20):
            t1 = threading.Thread(target=call_endpoint, args=[i])
            threads.append(t1)

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        end_time = time.time()

        result = {"time taken": end_time - st_time}

        if end_time - st_time <= 60:
            resp = method(**kwargs)
            resp_list[100] = resp
            if resp.status_code == 429:
                result["pass"] = True
            else:
                result["pass"] = False
        else:
            resp = method(**kwargs)
            resp_list[100] = resp
            if resp.status_code == 429:
                result["pass"] = False
            else:
                result["pass"] = True

        self.log.info("sleep for 1 min, to avoid any effect of the current rate limit on other tests.")
        time.sleep(60)

        result['response list'] = resp_list
        return result

    def authentication_token_test_wrapper(self, method=None, **kwargs):
        """
        Contains test for empty ,invalid token authentication, accessing from a different ip (allowed cidr),
        expired token.
        """
        self.log.info("Authentication test for method {} \nkwargs :{}".format(method.__name__, kwargs))

        self.log.info("Passing empty bearer token")
        self.set_access_keys(self.capellaAPI.org_ops_apis.ACCESS, '')

        resp = self.make_call_to_given_method(method=method, **kwargs)
        self.assertEqual(resp.status_code, 401)
        self.reset_access_keys_to_default()

        self.log.info("Creating access key using invalid auth")
        self.set_access_keys(self.capellaAPI.org_ops_apis.ACCESS,
                             get_random_string_of_given_length(
                                 length=len(self.capellaAPI.org_ops_apis.bearer_token)))

        resp = self.make_call_to_given_method(method=method, **kwargs)
        self.assertEqual(resp.status_code, 401)
        self.reset_access_keys_to_default()

        self.log.info("Accessing using different cidr")

        resp = self.make_call_to_given_method(method=self.capellaAPI.org_ops_apis.create_api_key,
                                              organizationId=self.tenant_id, name="name", expiry=1,
                                              organizationRoles=["organizationOwner"],
                                              description="description", allowedCIDRs=['10.254.254.254/20'])

        content = resp.json()

        self.append_to_api_keys(content['id'], self.tenant_id)
        self.set_access_keys(content['id'], content['token'])

        resp = self.make_call_to_given_method(method=method, **kwargs)
        # self.assertEqual(resp.status_code, 403)       # getting 404 instead, what should be the status code for this?
        self.reset_access_keys_to_default()

        self.log.info("Token Expiry Test.")
        resp = self.make_call_to_given_method(method=self.capellaAPI.org_ops_apis.create_api_key,
                                              organizationId=self.tenant_id, name="name",
                                              organizationRoles=["organizationOwner"],
                                              description="description", expiry=0.001)

        content = resp.json()
        self.set_access_keys(content['id'], content['token'])

        self.log.info("Sleep for 90 seconds for api-key to get expired.")
        time.sleep(90)

        resp = self.make_call_to_given_method(method=method, **kwargs)
        self.assertEqual(resp.status_code, 401)
        self.reset_access_keys_to_default()

    def rbac_roles_generator_wrapper(self):
        """
        generates all the possible combinations of roles using org level and project roles, combining with project
        level access, access to a secondary project and no project access.
        """

        # RBAC
        org_roles = ["organizationOwner", "projectCreator", "organizationMember"]
        project_roles = ["projectOwner", "projectManager", "projectViewer",
                         "projectDataReaderWriter", "projectDataReader"]

        api_keys_list = []
        for user in org_roles:
            for role in project_roles:
                self.log.info("Rbac test for {}, {} role".format(user, role))
                self.log.info("Using Project level access")

                resp = self.make_call_to_given_method(method=self.capellaAPI.org_ops_apis.create_api_key,
                                                      organizationId=self.tenant_id, name="name", expiry=1,
                                                      organizationRoles=[user], description="description",
                                                      resources=[{"id": self.project_id,
                                                                  "roles": [role]}])

                content = resp.json()
                self.assertEqual(resp.status_code, 201)
                self.append_to_api_keys(content['id'], self.tenant_id)

                api_keys_list.append({
                    'accessKey': content["id"],
                    'token': content['token'],
                    'organizationRole': user,
                    'projectId': self.project_id,
                    'projectRole': role,
                    'description': 'same project'
                })

                resp = self.make_call_to_given_method(method=self.capellaAPI.org_ops_apis.create_api_key,
                                                      organizationId=self.tenant_id, name="name", expiry=1,
                                                      organizationRoles=[user], description="description",
                                                      resources=[{"id": self.secondary_project_id,
                                                                  "roles": [role]}])

                content = resp.json()
                self.assertEqual(resp.status_code, 201)
                self.append_to_api_keys(content['id'], self.tenant_id)

                api_keys_list.append({
                    'accessKey': content["id"],
                    'token': content['token'],
                    'organizationRole': user,
                    'projectId': self.secondary_project_id,
                    'projectRole': role,
                    'description': 'secondary project'
                })

                self.log.info("Using Organization level access")

                resp = self.make_call_to_given_method(method=self.capellaAPI.org_ops_apis.create_api_key,
                                                      organizationId=self.tenant_id, name="name", expiry=1,
                                                      organizationRoles=[user], description="description")

                content = resp.json()
                self.assertEqual(resp.status_code, 201)
                self.append_to_api_keys(content['id'], self.tenant_id)

                api_keys_list.append({
                    'accessKey': content["id"],
                    'token': content['token'],
                    'organizationRole': user,
                    'projectId': self.project_id,
                    'projectRole': '',
                    'description': 'no project'
                })

        return api_keys_list

    def test_create_backup(self):
        self.log.info("Create backup test")

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.create_backup,
                                              organizationId=self.tenant_id, projectId=self.project_id,
                                              clusterId=self.cluster_id, bucketId=self.bucket_id)
        # there can be a backup already exist for the bucket.
        self.assertIn(resp.status_code, [202, 409])

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.create_backup,
                                              organizationId=self.invalid_id, projectId=self.project_id,
                                              clusterId=self.cluster_id, bucketId=self.bucket_id)
        self.assertEqual(resp.status_code, 403)

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.create_backup,
                                              organizationId=self.diff_tenant_id, projectId=self.project_id,
                                              clusterId=self.cluster_id, bucketId=self.bucket_id)
        self.assertEqual(resp.status_code, 403)

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.create_backup,
                                              organizationId=self.tenant_id,
                                              projectId=get_random_string_of_given_length(uid=True),
                                              clusterId=self.cluster_id, bucketId=self.bucket_id)
        self.assertEqual(resp.status_code, 404)

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.create_backup,
                                              organizationId=self.tenant_id,
                                              projectId=self.project_id,
                                              clusterId=get_random_string_of_given_length(uid=True),
                                              bucketId=self.bucket_id)
        self.assertEqual(resp.status_code, 404)

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.create_backup,
                                              organizationId=self.tenant_id,
                                              projectId=self.project_id,
                                              clusterId=self.cluster_id,
                                              bucketId=get_random_string_of_given_length(length=20))
        self.assertEqual(resp.status_code, 404)

        self.authentication_token_test_wrapper(method=self.capellaAPI.cluster_ops_apis.create_backup,
                                               organizationId=self.tenant_id, projectId=self.project_id,
                                               clusterId=self.cluster_id, bucketId=self.bucket_id)

        # RBAC
        for api_key in self.api_keys_list:
            self.log.info("Rbac test for organizationRole: {}, projectRole: {}, project: {}"
                          .format(api_key['organizationRole'],
                                  api_key['projectRole'],
                                  api_key['description']))
            self.set_access_keys(api_key['accessKey'], api_key['token'])

            resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.create_backup,
                                                  organizationId=self.tenant_id, projectId=self.project_id,
                                                  clusterId=self.cluster_id, bucketId=self.bucket_id)

            if api_key['organizationRole'] == 'organizationOwner':
                self.assertIn(resp.status_code, [202, 409])
            elif api_key['projectRole'] == 'projectOwner':
                if api_key['description'] == 'same project':
                    self.assertIn(resp.status_code, [202, 409])
                else:
                    self.assertEqual(resp.status_code, 403)
            else:
                self.assertEqual(resp.status_code, 403)

            self.reset_access_keys_to_default()

        result = self.rate_limit_wrapper(method=self.capellaAPI.cluster_ops_apis.create_backup,
                                         organizationId=self.tenant_id, projectId=self.project_id,
                                         clusterId=self.cluster_id, bucketId=self.bucket_id)

        self.log.info("create backup rate limit response : {}".format(result))
        self.assertTrue(result["pass"])

    def test_list_backups(self):

        self.log.info("list backup test")

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.list_backups,
                                              organizationId=self.tenant_id, projectId=self.project_id,
                                              clusterId=self.cluster_id)
        self.assertEqual(resp.status_code, 200)

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.list_backups,
                                              organizationId=self.invalid_id, projectId=self.project_id,
                                              clusterId=self.cluster_id)
        self.assertEqual(resp.status_code, 403)

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.list_backups,
                                              organizationId=self.diff_tenant_id, projectId=self.project_id,
                                              clusterId=self.cluster_id)
        self.assertEqual(resp.status_code, 403)

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.list_backups,
                                              organizationId=self.tenant_id,
                                              projectId=get_random_string_of_given_length(uid=True),
                                              clusterId=self.cluster_id)
        self.assertEqual(resp.status_code, 404)

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.list_backups,
                                              organizationId=self.tenant_id,
                                              projectId=self.project_id,
                                              clusterId=get_random_string_of_given_length(uid=True))
        self.assertEqual(resp.status_code, 404)

        self.authentication_token_test_wrapper(method=self.capellaAPI.cluster_ops_apis.list_backups,
                                               organizationId=self.tenant_id, projectId=self.project_id,
                                               clusterId=self.cluster_id)

        # RBAC
        for api_key in self.api_keys_list:
            self.log.info("Rbac test for organizationRole: {}, projectRole: {}, project: {}"
                          .format(api_key['organizationRole'],
                                  api_key['projectRole'],
                                  api_key['description']))
            self.set_access_keys(api_key['accessKey'], api_key['token'])

            resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.list_backups,
                                                  organizationId=self.tenant_id, projectId=self.project_id,
                                                  clusterId=self.cluster_id)

            if api_key['organizationRole'] == 'organizationOwner':
                self.assertEqual(resp.status_code, 200)
            elif api_key['projectRole'] == 'projectOwner':
                if api_key['description'] == 'same project':
                    self.assertEqual(resp.status_code, 200)
                else:
                    self.assertEqual(resp.status_code, 403)
            else:
                self.assertEqual(resp.status_code, 403)

            self.reset_access_keys_to_default()

        result = self.rate_limit_wrapper(method=self.capellaAPI.cluster_ops_apis.list_backups,
                                         organizationId=self.tenant_id, projectId=self.project_id,
                                         clusterId=self.cluster_id)

        self.log.info("list backup rate limit response : {}".format(result))
        self.assertTrue(result["pass"])

    def test_get_backup(self):
        self.log.info("get backup test")

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.list_backups,
                                              organizationId=self.tenant_id, projectId=self.project_id,
                                              clusterId=self.cluster_id)
        backupId = resp.json()['data'][0]['id']

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.get_backup,
                                              organizationId=self.tenant_id, projectId=self.project_id,
                                              clusterId=self.cluster_id, backupId=backupId)
        self.assertEqual(resp.status_code, 200)

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.get_backup,
                                              organizationId=self.invalid_id, projectId=self.project_id,
                                              clusterId=self.cluster_id, backupId=backupId)
        self.assertEqual(resp.status_code, 403)

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.get_backup,
                                              organizationId=self.diff_tenant_id, projectId=self.project_id,
                                              clusterId=self.cluster_id, backupId=backupId)
        self.assertEqual(resp.status_code, 403)

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.get_backup,
                                              organizationId=self.tenant_id,
                                              projectId=get_random_string_of_given_length(uid=True),
                                              clusterId=self.cluster_id, backupId=backupId)
        self.assertEqual(resp.status_code, 404)

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.get_backup,
                                              organizationId=self.tenant_id,
                                              projectId=self.project_id,
                                              clusterId=get_random_string_of_given_length(uid=True),
                                              backupId=backupId)
        self.assertEqual(resp.status_code, 404)

        self.authentication_token_test_wrapper(method=self.capellaAPI.cluster_ops_apis.get_backup,
                                               organizationId=self.tenant_id, projectId=self.project_id,
                                               clusterId=self.cluster_id, backupId=backupId)

        # RBAC
        for api_key in self.api_keys_list:
            self.log.info("Rbac test for organizationRole: {}, projectRole: {}, project: {}"
                          .format(api_key['organizationRole'],
                                  api_key['projectRole'],
                                  api_key['description']))
            self.set_access_keys(api_key['accessKey'], api_key['token'])

            resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.get_backup,
                                                  organizationId=self.tenant_id, projectId=self.project_id,
                                                  clusterId=self.cluster_id, backupId=backupId)

            if api_key['organizationRole'] == 'organizationOwner':
                self.assertEqual(resp.status_code, 200)
            elif api_key['projectRole'] == 'projectOwner':
                if api_key['description'] == 'same project':
                    self.assertEqual(resp.status_code, 200)
                else:
                    self.assertEqual(resp.status_code, 403)
            else:
                self.assertEqual(resp.status_code, 403)

            self.reset_access_keys_to_default()

        result = self.rate_limit_wrapper(method=self.capellaAPI.cluster_ops_apis.get_backup,
                                         organizationId=self.tenant_id, projectId=self.project_id,
                                         clusterId=self.cluster_id, backupId=backupId)

        self.log.info("get backup rate limit response : {}".format(result))
        self.assertTrue(result["pass"])

    def test_delete_backup(self):
        self.log.info("Delete backup test")

        _ = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.create_backup,
                                           organizationId=self.tenant_id, projectId=self.project_id,
                                           clusterId=self.cluster_id, bucketId=self.bucket_id)

        backupId = self.get_backup_id_for_given_bucket(organizationId=self.tenant_id, projectId=self.project_id,
                                                       clusterId=self.cluster_id, bucketId=self.bucket_id)

        self.wait_till_backup_in_ready_state(organizationId=self.tenant_id, projectId=self.project_id,
                                             clusterId=self.cluster_id, backupId=backupId)

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.delete_backup,
                                              organizationId=self.tenant_id, projectId=self.project_id,
                                              clusterId=self.cluster_id, backupId=backupId)
        self.assertEqual(resp.status_code, 202)

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.delete_backup,
                                              organizationId=self.invalid_id, projectId=self.project_id,
                                              clusterId=self.cluster_id, backupId=backupId)
        self.assertEqual(resp.status_code, 403)

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.delete_backup,
                                              organizationId=self.diff_tenant_id, projectId=self.project_id,
                                              clusterId=self.cluster_id, backupId=backupId)
        self.assertEqual(resp.status_code, 403)

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.delete_backup,
                                              organizationId=self.tenant_id,
                                              projectId=get_random_string_of_given_length(uid=True),
                                              clusterId=self.cluster_id, backupId=backupId)
        self.assertEqual(resp.status_code, 404)

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.delete_backup,
                                              organizationId=self.tenant_id,
                                              projectId=self.project_id,
                                              clusterId=get_random_string_of_given_length(uid=True),
                                              backupId=backupId)
        self.assertEqual(resp.status_code, 404)

        self.authentication_token_test_wrapper(method=self.capellaAPI.cluster_ops_apis.delete_backup,
                                               organizationId=self.tenant_id, projectId=self.project_id,
                                               clusterId=self.cluster_id, backupId=backupId)

        # RBAC
        for api_key in self.api_keys_list:
            self.log.info("Rbac test for organizationRole: {}, projectRole: {}, project: {}"
                          .format(api_key['organizationRole'],
                                  api_key['projectRole'],
                                  api_key['description']))
            self.set_access_keys(api_key['accessKey'], api_key['token'])

            # since backupId is not returned while creating backup, we have to filter it out from list backup api-call.
            # but it's taking too much time, so commenting this logic,
            # can be uncommented if response time of list backup is acceptable.
            # _ = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.create_backup,
            #                                    organizationId=self.tenant_id, projectId=self.project_id,
            #                                    clusterId=self.cluster_id, bucketId=self.bucket_id)
            #
            # backupId = self.get_backup_id_for_given_bucket(organizationId=self.tenant_id, projectId=self.project_id,
            #                                                clusterId=self.cluster_id, bucketId=self.bucket_id)

            # self.wait_till_backup_in_ready_state(organizationId=self.tenant_id, projectId=self.project_id,
            #                                      clusterId=self.cluster_id, backupId=backupId)

            # since backupId is already deleted, if api-key having access to the resource should give 404.
            resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.delete_backup,
                                                  organizationId=self.tenant_id, projectId=self.project_id,
                                                  clusterId=self.cluster_id, backupId=backupId)

            if api_key['organizationRole'] == 'organizationOwner':
                # self.assertEqual(resp.status_code, 202)  bug https://couchbasecloud.atlassian.net/browse/AV-60636
                self.assertEqual(resp.status_code, 404)
            elif api_key['projectRole'] == 'projectOwner':
                if api_key['description'] == 'same project':
                    # self.assertEqual(resp.status_code, 202)  bug https://couchbasecloud.atlassian.net/browse/AV-60636
                    self.assertEqual(resp.status_code, 404)
                else:
                    self.assertEqual(resp.status_code, 403)  # user don't have access to the resource.
            else:
                self.assertEqual(resp.status_code, 403)

            self.reset_access_keys_to_default()

        result = self.rate_limit_wrapper(method=self.capellaAPI.cluster_ops_apis.delete_backup,
                                         organizationId=self.tenant_id, projectId=self.project_id,
                                         clusterId=self.cluster_id, backupId=backupId)

        self.log.info("delete backup rate limit response : {}".format(result))
        self.assertTrue(result["pass"])

    def test_restore_backup(self):

        _ = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.create_backup,
                                           organizationId=self.tenant_id, projectId=self.project_id,
                                           clusterId=self.cluster_id, bucketId=self.bucket_id)

        backupId = self.get_backup_id_for_given_bucket(organizationId=self.tenant_id, projectId=self.project_id,
                                                       clusterId=self.cluster_id, bucketId=self.bucket_id)

        sec_proj_cluster_id = self.create_cluster(self.tenant_id, self.secondary_project_id)

        self.wait_till_backup_in_ready_state(backupId=backupId, organizationId=self.tenant_id,
                                             projectId=self.project_id, clusterId=self.cluster_id)

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.restore_backup,
                                              organizationId=self.tenant_id,
                                              projectId=self.project_id,
                                              targetClusterID=self.cluster_id,
                                              sourceClusterID=self.cluster_id,
                                              backupID=backupId,
                                              services=["data", "query"])
        self.assertEqual(resp.status_code, 202)

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.restore_backup,
                                              organizationId=self.invalid_id,
                                              projectId=self.project_id,
                                              targetClusterID=self.cluster_id,
                                              sourceClusterID=self.cluster_id,
                                              backupID=backupId,
                                              services=["data", "query"])
        # self.assertEqual(resp.status_code, 403)  # bug https://couchbasecloud.atlassian.net/browse/AV-60638

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.restore_backup,
                                              organizationId=self.diff_tenant_id,
                                              projectId=self.project_id,
                                              targetClusterID=self.cluster_id,
                                              sourceClusterID=self.cluster_id,
                                              backupID=backupId,
                                              services=["data", "query"])
        # self.assertEqual(resp.status_code, 403)  # bug https://couchbasecloud.atlassian.net/browse/AV-60638

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.restore_backup,
                                              organizationId=self.tenant_id,
                                              projectId=get_random_string_of_given_length(uid=True),
                                              targetClusterID=self.cluster_id,
                                              sourceClusterID=self.cluster_id,
                                              backupID=backupId,
                                              services=["data", "query"])
        self.assertEqual(resp.status_code, 404)

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.restore_backup,
                                              organizationId=self.tenant_id,
                                              projectId=self.secondary_project_id,
                                              targetClusterID=self.cluster_id,
                                              sourceClusterID=self.cluster_id,
                                              backupID=backupId,
                                              services=["data", "query"])

        # self.assertEqual(resp.status_code, 404)   #  bug https://couchbasecloud.atlassian.net/browse/AV-60638

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.restore_backup,
                                              organizationId=self.tenant_id,
                                              projectId=self.project_id,
                                              targetClusterID=self.invalid_id,
                                              sourceClusterID=self.cluster_id,
                                              backupID=backupId,
                                              services=["data", "query"])
        self.assertEqual(resp.status_code, 404)

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.restore_backup,
                                              organizationId=self.tenant_id,
                                              projectId=self.project_id,
                                              targetClusterID=self.cluster_id,
                                              sourceClusterID=self.invalid_id,
                                              backupID=backupId,
                                              services=["data", "query"])
        self.assertEqual(resp.status_code, 404)

        resp = self.make_call_to_given_method(method=self.capellaAPI.cluster_ops_apis.restore_backup,
                                              organizationId=self.tenant_id,
                                              projectId=self.project_id,
                                              targetClusterID=self.cluster_id,
                                              sourceClusterID=self.cluster_id,
                                              backupID=get_random_string_of_given_length(uid=True),
                                              services=["data", "query"])
        # self.assertEqual(resp.status_code, 404)   bug https://couchbasecloud.atlassian.net/browse/AV-60637

        self.authentication_token_test_wrapper(method=self.capellaAPI.cluster_ops_apis.restore_backup,
                                               organizationId=self.tenant_id,
                                               projectId=self.project_id,
                                               targetClusterID=self.cluster_id,
                                               sourceClusterID=self.cluster_id,
                                               backupID=backupId,
                                               services=["data", "query"])

        # RBAC
        for api_key in self.api_keys_list:
            self.log.info("Rbac test for organizationRole: {}, projectRole: {}, project: {}"
                          .format(api_key['organizationRole'],
                                  api_key['projectRole'],
                                  api_key['description']))
            self.set_access_keys(api_key['accessKey'], api_key['token'])

            self.wait_till_cluster_is_healthy(self.tenant_id, self.project_id, self.cluster_id)
            self.wait_till_cluster_is_healthy(self.tenant_id, self.secondary_project_id, sec_proj_cluster_id)

            prim_proj_restore_resp = self.make_call_to_given_method(
                method=self.capellaAPI.cluster_ops_apis.restore_backup,
                organizationId=self.tenant_id,
                projectId=self.project_id,
                targetClusterID=self.cluster_id,
                sourceClusterID=self.cluster_id,
                backupID=backupId,
                services=["data", "query"])

            sec_proj_restore_resp = self.make_call_to_given_method(
                method=self.capellaAPI.cluster_ops_apis.restore_backup,
                organizationId=self.tenant_id,
                projectId=self.secondary_project_id,
                targetClusterID=self.cluster_id,
                sourceClusterID=sec_proj_cluster_id,
                backupID=backupId,
                services=["data", "query"])

            if api_key['organizationRole'] == 'organizationOwner':
                self.assertEqual(prim_proj_restore_resp.status_code, 202)
                self.assertEqual(sec_proj_restore_resp.status_code, 202)

            elif api_key['projectRole'] == "projectOwner":
                if api_key['description'] == 'same project':
                    self.assertEqual(prim_proj_restore_resp.status_code, 202)
                    self.assertEqual(sec_proj_restore_resp.status_code, 403)
                else:
                    self.assertEqual(prim_proj_restore_resp.status_code, 403)
                    self.assertEqual(sec_proj_restore_resp.status_code, 403)
            else:
                self.assertEqual(prim_proj_restore_resp.status_code, 403)
                self.assertEqual(sec_proj_restore_resp.status_code, 403)

            self.reset_access_keys_to_default()

        result = self.rate_limit_wrapper(method=self.capellaAPI.cluster_ops_apis.restore_backup,
                                         organizationId=self.tenant_id,
                                         projectId=self.project_id,
                                         targetClusterID=self.cluster_id,
                                         sourceClusterID=self.cluster_id,
                                         backupID=backupId,
                                         services=["data", "query"])

        self.log.info("Restore backup rate limit response : {}".format(result))
        self.assertTrue(result["pass"])
