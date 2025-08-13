import json
import base64
import requests
from pytests.security.security_base import SecurityBase
from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI
from TestInput import TestInputSingleton

class DB_CREDS_TESTS(SecurityBase):

    def setUp(self):
        SecurityBase.setUp(self)

        self.rest_username = TestInputSingleton.input.membase_settings.rest_username
        self.rest_password = TestInputSingleton.input.membase_settings.rest_password
        self.data_api_url = self.input.capella.get("data_api_url", None)
        if self.data_api_url is None:
            self.data_api_url = self.enable_data_api()

        self.scopes = ["all", "scope1", "scope2"]
        self.collections = ["collection1", "collection2"]
        self.scopes_and_collections = [["scope1", "collection1"], ["scope2", "collection2"]]

    def tearDown(self):
        self.test_delete_db_users()
        self.test_delete_buckets()
        super(DB_CREDS_TESTS, self).tearDown()

    def enable_data_api(self):
        self.log.info("Enabling Data API")

        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key, self.user,
                                 self.passwd)
        url = "https://{}/v2/organizations/{}/projects/{}/clusters/{}/data-api".format(self.url.replace("cloud", "", 1),
                                                                                       self.tenant_id,
                                                                                       self.project_id,
                                                                                       self.cluster_id)
        payload = {
            "enabled": True
        }
        resp = capella_api.do_internal_request(url, "PUT", payload=payload)
        if resp.status_code != 202:
            self.fail("Failed to enable Data API for the cluster. Reason: {}".format(resp.content))

        data_api_status = "enabling"
        while data_api_status == "enabling":
            self.sleep(60, "Waiting for Data API to be enabled. Current status: {}".format(data_api_status))
            resp = self.capellaAPI.cluster_ops_apis.get_cluster(self.tenant_id,
                                                                self.project_id,
                                                                self.cluster_id)
            if resp.status_code != 200:
                self.fail("Failed to get cluster data. Reason {}".format(resp.content))
            else:
                resp = resp.json()
                data_api_status = resp["data"]["dataApiState"]

        if data_api_status == "enabled":
            self.log.info("Data API enabled")
            return resp["data"]["dataApiHostname"]
        else:
            self.fail("Failed to enable Data API. Reason: {}".format(data_api_status))

    def create_db_payload(self, username, password, bucket_name="", scope="", access=""):
        payload = {
            "name": username,
            "password": password,
            "permissions": {},
            "credentialType": "basic"
        }

        if scope == "all":
            scope = "*"

        if bucket_name != "":
            if 'read' in access:
                payload["permissions"]["data_reader"] = {
                    "buckets": [
                        {
                            "name" : bucket_name,
                            "scopes": [{"name": "{}".format(scope)}]
                        }
                    ]
                }
            if 'write' in access:
                payload["permissions"]["data_writer"] = {
                    "buckets": [
                        {
                            "name": bucket_name,
                            "scopes": [{"name": "{}".format(scope)}]
                        }
                    ]
                }

        if access == 'full':
            payload = {
                "name": username,
                "password": password,
                "permissions": {
                    "data_reader": {},
                    "data_writer": {}
                },
                "credentialType": "basic"
            }

        return payload
    """
    {
        "name":"Administrator",
        "password":"Password@123",
        "permissions":{
            "data_reader":{
                "buckets":[
                    {
                        "name":"bucket1",
                        "scopes":[
                            {
                                "name":"scope1"
                            }
                        ]
                    }
                ]
            }
        },
        "credentialType":"basic"
    }
    {
        "name": "Administrator",
        "password": "Password@123",
        "permissions": {
            "data_reader": {},
            "data_writer": {}
        },
        "credentialType": "basic"
    }
    """

    def create_db_creds(self, body, access, bucket):
        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key, self.user,
                                 self.passwd)
        self.log.info("Creating Cluster Access Creds. User - {}, Bucket - {}, Access - {}".format(body["name"],
                                                     bucket, access))
        # {"name":"Administrator","password":"Password@123","permissions":{"data_reader":{},"data_writer":{}},"credentialType":"basic"}
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/users' \
            .format("https://" + self.url.replace("cloud", "", 1), self.tenant_id,
                    self.project_id, self.cluster_id)
        create_db_cred_resp = capella_api.do_internal_request(url, method="POST",
                                        params=json.dumps(body))

        self.log.info("create_db_cred_resp: {}, {}".format(create_db_cred_resp.status_code,
                                                       create_db_cred_resp.content))

        return create_db_cred_resp

    def get_db_creds(self):
        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key, self.user,
                                 self.passwd)
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}' \
            .format("https://" + self.url.replace("cloud", "", 1), self.tenant_id,
                    self.project_id, self.cluster_id)
        url = url + '/users?page=%s&perPage=%s' % (1, 100)

        get_db_user_resp = capella_api.do_internal_request(url, method="GET")

        self.assertEqual(get_db_user_resp.status_code, 200,
                         msg='FAIL, Outcome:{}, Expected:{}, Reason: {}'
                         .format(get_db_user_resp.status_code, 200, get_db_user_resp.content))

    def create_scopes_and_collections(self, buckets):
        for bucket in buckets:
            for item in self.scopes_and_collections:
                resp = self.capellaAPI.cluster_ops_apis.create_scope(self.tenant_id, self.project_id, self.cluster_id, bucket, item[0])
                if resp.status_code != 201:
                    self.fail("Failed to create scope {} for bucket '{}'. Reason - {}".format(item[0], bucket,
                                                                                              resp.content))

                resp = self.capellaAPI.cluster_ops_apis.create_collection(self.tenant_id, self.project_id, self.cluster_id, bucket,
                                                     item[0], item[1])
                if resp.status_code != 201:
                    self.fail("Failed to create collection for bucket '{}'. Reason - {}".format(bucket,
                                                                                                resp.content))

        self.log.info("Successfully created scopes and collections")


    def write_doc_into_buckets(self, buckets, doc_id):
        self.log.info("Writing documents into the buckets")

        capella_api = CapellaAPI("https://" + self.url, self.secret_key,
                                 self.access_key, self.user, self.passwd)
        payload = {
            "doc_id": doc_id,
            "name": self.generate_random_string(3, False, "name"),
            "last": self.generate_random_string(3, False, "last"),
        }

        for bucket in buckets:
            for scope in self.scopes_and_collections:
                url = 'https://{}/v1/buckets/{}/scopes/{}/collections/{}/documents/{}'.format(
                    self.data_api_url, bucket, scope[0], scope[1], doc_id)
                header = {
                    'Authorization': 'Basic {}'.format(base64.b64encode('{}:{}'.format(self.rest_username,
                                                                                 self.rest_password).encode()).decode()),
                    'Content-Type': 'application/json'
                }
                resp = capella_api.do_internal_request(url, "POST", params=payload, headers=header)
                self.assertEqual(resp.status_code, 200,
                             msg='FAIL, Outcome:{}, Expected:{}, Reason: {}'
                             .format(resp.status_code, 200, resp.content))

    def read_db_creds_test(self, read_db_creds, buckets, doc_id):
        for idx, dictionary in enumerate(read_db_creds):
            for key, value in dictionary.items():
                for bucket in buckets:
                    for scope in self.scopes_and_collections:
                        self.log.info(
                            "bucket - {}, value[0] - {}, scope[0] - {}, value[1] - {},".format(bucket, value[0],
                                                                                               scope[0], value[1]))
                        if bucket == value[0] and (scope[0] == value[1] or value[1] == "all"):
                            continue
                        else:
                            self.log.info("Trying to read a document")
                            self.log.info("User - {}, Bucket - {}, Scope - {} ".format(key, bucket, value[1]))

                            url = 'https://{}/v1/buckets/{}/scopes/{}/collections/{}/documents/{}'.format(
                                self.data_api_url, bucket, scope[0], scope[1], doc_id)
                            authorization = base64.b64encode(
                                '{}:{}'.format(key, self.rest_password).encode()).decode()
                            headers = {
                                'Authorization': 'Basic %s' % authorization,
                                'Accept': '*/*'
                            }
                            resp = requests.request("GET", url, headers=headers, verify=False)
                            self.assertEqual(resp.status_code, 403,
                                             msg='FAIL, Outcome:{}, Expected: {}, Reason: {}'.format(
                                                 resp.status_code, 403, resp.content))

    def write_db_creds_test(self, write_db_creds, buckets, doc_id):
        for idx, dictionary in enumerate(write_db_creds):
            for key, value in dictionary.items():
                for bucket in buckets:
                    for scope in self.scopes_and_collections:
                        self.log.info(
                            "bucket - {}, value[0] - {}, scope[0] - {}, value[1] - {},".format(bucket, value[0],
                                                                                               scope[0], value[1]))
                        if bucket == value[0] and (scope[0] == value[1] or value[1] == "all"):
                            continue
                        else:
                            self.log.info("Trying to write a document into bucket - {}".format(bucket))
                            self.log.info("User - {}, Bucket - {}, Scope - {}".format(key, bucket, scope))

                            url = 'https://{}/v1/buckets/{}/scopes/{}/collections/{}/documents/{}'.format(
                                                                    self.data_api_url, bucket,
                                                                          scope[0], scope[1], doc_id)
                            authorization = base64.b64encode(
                                '{}:{}'.format(key, self.rest_password).encode()).decode()
                            headers = {
                                'Authorization': 'Basic %s' % authorization,
                                'Accept': '*/*'
                            }
                            resp = requests.request("POST", url, headers=headers, verify=False)
                            self.assertEqual(resp.status_code, 403,
                                             msg='FAIL, Outcome:{}, Expected: {}, Reason: {}'.format(
                                                 resp.status_code, 403, resp.content))

    def read_write_db_creds_test(self, read_write_db_creds, buckets, doc_id):
        for idx, dictionary in enumerate(read_write_db_creds):
            for key, value in dictionary.items():
                for bucket in buckets:
                    for scope in self.scopes_and_collections:
                        self.log.info("bucket - {}, value[0] - {}, scope[0] - {}, value[1] - {},".format(bucket, value[0], scope[0], value[1]))
                        if bucket == value[0] and (scope[0] == value[1] or value[1] == "all"):
                            continue
                        else:
                            self.log.info("Trying to read a document from bucket - {}".format(bucket))
                            self.log.info("User - {}, Bucket - {}, Scope - {}".format(key, bucket, scope))

                            url = 'https://{}/v1/buckets/{}/scopes/{}/collections/{}/documents/{}'.format(self.data_api_url,
                                                                                                          bucket,
                                                                                                          scope[0],
                                                                                                          scope[1],
                                                                                                          doc_id)
                            authorization = base64.b64encode(
                                '{}:{}'.format(key, self.rest_password).encode()).decode()
                            headers = {
                                'Authorization': 'Basic %s' % authorization,
                                'Accept': '*/*'
                            }
                            resp = requests.request("GET", url, headers=headers, verify=False)
                            self.assertEqual(resp.status_code, 403,
                                             msg='FAIL, Outcome:{}, Expected: {}, Reason: {}'.format(
                                                 resp.status_code, 403, resp.content))

                            self.log.info("Trying to write a document into bucket - {}".format(bucket))
                            self.log.info("User - {}, Bucket - {}, Scope - {}".format(key, bucket, scope))

                            url = 'https://{}/v1/buckets/{}/scopes/{}/collections/{}/documents/{}'.format(self.data_api_url,
                                                                                                          bucket,
                                                                                                          scope[0],
                                                                                                          scope[1],
                                                                                                          doc_id)
                            authorization = base64.b64encode(
                                '{}:{}'.format(key, self.rest_password).encode()).decode()
                            headers = {
                                'Authorization': 'Basic %s' % authorization,
                                'Accept': '*/*'
                            }
                            resp = requests.request("POST", url, headers=headers, verify=False)
                            self.assertEqual(resp.status_code, 403,
                                             msg='FAIL, Outcome:{}, Expected: {}, Reason: {}'.format(
                                                 resp.status_code, 403, resp.content))

    def test_db_creds(self):
        self.log.info("Running a test to validate the data api with cluster access")

        self.log.info("Creating Admin user for the test")
        body = self.create_db_payload(self.rest_username, self.rest_password, "", access="full")
        admin_response = self.create_db_creds(body, "", "")
        if admin_response.status_code != 200:
            self.log.info("Failed to create admin users. Reason - {}".format(admin_response.content))
        else:
            self.log.info("Successfully created admin user")

        buckets = []
        self.bucket_ids = []
        for i in range(10):
            bucket_params = {
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
            resp = self.capellaAPI.cluster_ops_apis.create_bucket(self.tenant_id,
                                                                  self.project_id,
                                                                  self.cluster_id,
                                                                  bucket_params["name"],
                                                                  bucket_params["type"],
                                                                  bucket_params["storageBackend"],
                                                                  bucket_params["memoryAllocationInMb"],
                                                                  bucket_params["bucketConflictResolution"],
                                                                  bucket_params["durabilityLevel"],
                                                                  bucket_params["replicas"],
                                                                  bucket_params["flush"],
                                                                  bucket_params["timeToLiveInSeconds"])
            if resp.status_code == 201:
                buckets.append(bucket_params["name"])
                self.log.info("Created bucket - {}".format(resp.content))
                self.bucket_ids.append(resp.json()["id"])
            else:
                self.fail("Failed to create a bucket. Reason: {}".format(resp.content))

        self.log.info("Created all the buckets")

        access_roles = ["read", "write", "readwrite"]
        reads_db_creds = []
        writes_db_creds = []
        read_writes_db_creds = []

        doc_id = "doc_1"
        self.create_scopes_and_collections(self.bucket_ids)
        self.write_doc_into_buckets(buckets, doc_id)

        num = 0
        for bucket in buckets:
            for scope in self.scopes:
                for access in access_roles:
                    username = "Administrator_" + str(num)
                    body = self.create_db_payload(username, self.rest_password, bucket, scope, access)
                    resp = self.create_db_creds(body, access, bucket)

                    if resp.status_code == 200:
                        if access == "read":
                            reads_db_creds.append({username: [bucket, scope]})
                        elif access == "write":
                            writes_db_creds.append({username: [bucket, scope]})
                        elif access == "readwrite":
                            read_writes_db_creds.append({username: [bucket, scope]})

                    num = num + 1

        self.log.info("Created all the db credentials")
        self.log.info("The Read DB Creds are - {}".format(reads_db_creds))
        self.log.info("The Write DB Creds are - {}".format(writes_db_creds))
        self.log.info("The Read/Write DB Creds are - {}".format(read_writes_db_creds))

        self.read_db_creds_test(reads_db_creds, buckets, doc_id)
        self.log.info("Done with Read tests")
        self.write_db_creds_test(writes_db_creds, buckets, doc_id)
        self.log.info("Done with write tests")
        self.read_write_db_creds_test(read_writes_db_creds, buckets, doc_id)
        self.log.info("Done with Read/write tests")

        self.log.info("The test is completed successfully")

    def restore_db_creds(self, cluster_id, payload=None, header=None):
        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                 self.user, self.passwd)
        url = '{}/internal/support/clusters/{}/rbac/restore'.format(capella_api.internal_url,
                                                                    cluster_id)
        if header is None:
            header = {
                'Authorization': 'Bearer %s' % self.support_token,
                'Content-Type': 'application/json'
            }
        if payload is None:
            payload = {
                "clusterId": ""
            }
        resp = capella_api._urllib_request(url, method="POST", params=json.dumps(payload),
                                           headers=header)
        return resp

    def test_restore_db_creds(self):
        self.log.info("Testing Authentication and Authorization for Restoring DB Creds")
        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                 self.user, self.passwd)
        self.log.info("1. Authentication tests")
        valid_bearer_resp = capella_api.get_authorization_internal()

        # cbc_api_headers = {
        #     "empty_token": {
        #         'Authorization': '',
        #         'Content-Type': 'application/json'
        #     },
        #     "invalid_token": {
        #         'Authorization': 'Bearer abcdfekjui',
        #         'Content-Type': 'application/json'
        #     },
        #     "incorrect_bearer_token": {
        #         'Authorization': valid_bearer_resp['Authorization'],
        #         'Content-Type': 'application/json'
        #     }
        # }
        # for key, header in cbc_api_headers.items():
        #     resp = self.restore_db_creds(self.cluster_id, header=header)
        #     self.assertEqual(401, resp.status_code,
        #                      msg='FAIL. Outcome: {}, Expected: {}, Reason: {}'.format(
        #                          resp.status_code, 401, resp.content))

        self.log.info("2. Test with valid credentials")
        payload = {
            "clusterId": ""
        }
        valid_token_resp = self.restore_db_creds(self.cluster_id, payload)
        self.assertEqual(204, valid_token_resp.status_code,
                         msg='FAIL. Outcome: {}, Expected: {}, Reason: {}'.format(
                             valid_token_resp.status_code, 204, valid_token_resp.content))

        # self.log.info("2. Test with cluster id of different tenant")
        # valid_token_resp = self.restore_db_creds(self.cluster_id)
        # self.assertEqual(404, valid_token_resp.status_code,
        #                  msg='FAIL. Outcome: {}, Expected: {}, Reason: {}'.format(
        #                      valid_token_resp.status_code, 404, valid_token_resp.content))

    def test_delete_buckets(self):
        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                     self.user, self.passwd)
        resp = capella_api.get_buckets(self.tenant_id, self.project_id, self.cluster_id)
        resp = resp.json()
        bucket_ids = []
        for buckets in resp["buckets"]['data']:
            bucket_ids.append(buckets['data']['id'])

        for bucket_id in bucket_ids:
            capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key, self.user,
                                 self.passwd)
            resp = capella_api.delete_bucket(self.tenant_id, self.project_id, self.cluster_id,
                                             bucket_id)

    def test_delete_db_users(self):
        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                 self.user, self.passwd)
        url = "https://{}/v2/organizations/{}/projects/{}/clusters/{}/users?page=1&perPage=100&sortBy" \
              "=name&sortDirection=asc".format(self.url.replace("cloud", ""), self.tenant_id,
                                               self.project_id, self.cluster_id)
        resp = capella_api.do_internal_request(url, method="GET")
        resp = resp.json()
        db_ids = []
        for db_id in resp['data']:
            db_ids.append(db_id["data"]["id"])
        for db_id in db_ids:
            resp = capella_api.delete_db_user(self.tenant_id, self.project_id, self.cluster_id,
                                              db_id)

    def test_url(self):
        # capella_api = CapellaAPI("https://" + self.url, self.secret_key,
        #                          self.access_key, self.user, self.passwd)
        # resp = capella_api.get_nodes(self.tenant_id, self.project_id,
        #                              self.cluster_id)
        # ip = resp.json()['data'][0]['data']['hostname']
        # doc_id = 'airline_8091'
        # url = 'https://{}:18091/pools/default/buckets/{}/docs/{}'.format(ip, bucket,
        #                                                                  doc_id)
        # url = 'https://svc-dqis-node-001.zyavpocecnnmdf8k.sandbox.nonprod-project-avengers.com:18091/pools/default/buckets/bucket_0/docs/airline_8091'
        # authorization = base64.b64encode(
        #     '{}:{}'.format("Administrator_1", self.rest_password).encode()).decode()
        # headers = {
        #     'Authorization': 'Basic %s' % authorization,
        #     'Accept': '*/*'
        # }
        # resp = requests.request("GET", url, headers=headers, verify=False)
        # if resp.status_code == 403:
        #     print("Passed")
        # else:
        #     print("Fail")
        # self.assertEqual(resp.status_code, 403,
        #                  msg='FAIL, Outcome:{}, Expected:{}, Reason: {}'.format(
        #                      resp.status_code, 403, resp.content))
        buckets = ["bucket_4", "bucket_5"]
        self.write_doc_into_buckets(buckets)
