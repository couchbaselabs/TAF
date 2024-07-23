import time
import json
import base64
import random
import string
import requests
from pytests.security.security_base import SecurityBase
from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI
from TestInput import TestInputSingleton
import threading
from Queue import Queue
import subprocess
from datetime import timedelta

class DB_CREDS_TESTS(SecurityBase):

    def setUp(self):
        SecurityBase.setUp(self)

        self.rest_username = TestInputSingleton.input.membase_settings.rest_username
        self.rest_password = TestInputSingleton.input.membase_settings.rest_password
        self.support_token = self.input.capella.get("support_token")

    def tearDown(self):
        self.test_delete_db_users()
        self.test_delete_buckets()
        super(DB_CREDS_TESTS, self).tearDown()

    def create_db_payload(self, username, password, bucket_name="", access=""):
        payload = {
            "name": username,
            "password": password,
            "permissions": {}
        }

        if bucket_name != "":
            if 'read' in access:
                payload["permissions"]["data_reader"] = {
                    "buckets": [
                        {
                            "name" : bucket_name,
                            "scopes": [{"name": "*"}]
                        }
                    ]
                }
            if 'write' in access:
                payload["permissions"]["data_writer"] = {
                    "buckets": [
                        {
                            "name": bucket_name,
                            "scopes": [{"name": "*"}]
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
                }
            }

        return payload

    def create_db_creds(self, body, access, bucket, result_queue=None):
        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key, self.user,
                                 self.passwd)
        self.log.info("Creating DB Creds. User - {}, Bucket - {}, Access - {}".format(body["name"],
                                                     bucket, access))
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/users' \
            .format("https://" + self.url.replace("cloud", "", 1), self.tenant_id,
                    self.project_id, self.cluster_id)
        create_db_cred_resp = capella_api.do_internal_request(url, method="POST",
                                        params=json.dumps(body))

        # result_queue.put((create_db_cred_resp, access, bucket, body["name"]))
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

    def write_doc_into_buckets(self, buckets):
        self.log.info("Writing documents into the buckets")

        capella_api = CapellaAPI("https://" + self.url, self.secret_key,
                                 self.access_key, self.user, self.passwd)
        payload = {
                "id": "8091",
                "name": "Koushal-test-doc"
            }
        doc_id = "airline_8091"
        for bucket in buckets:
            url = 'https://{}/v2/databases/{}/proxy/pools/default' \
                  '/buckets/{}/scopes/_default/collections/_default/docs/{}'.format(
                self.url.replace(
                    "cloud", ""), self.cluster_id, bucket, doc_id)
            header = {
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            resp = capella_api.do_internal_request(url, "POST", params=payload, headers=header)
            self.assertEqual(resp.status_code, 200,
                             msg='FAIL, Outcome:{}, Expected:{}, Reason: {}'
                             .format(resp.status_code, 200, resp.content))

    def read_db_creds_test(self, read_db_creds, buckets):
        for idx, dictionary in enumerate(read_db_creds):
            for key, value in dictionary.items():
                for bucket in buckets:
                    if bucket == value:
                        continue
                    else:
                        self.log.info("Trying to read a document")
                        self.log.info("Bucket - {}, User - {}".format(bucket, key))
                        capella_api = CapellaAPI("https://" + self.url, self.secret_key,
                                                 self.access_key, self.user, self.passwd)
                        resp = capella_api.get_nodes(self.tenant_id, self.project_id,
                                                     self.cluster_id)
                        ip = resp.json()['data'][0]['data']['hostname']
                        doc_id = 'airline_8091'
                        url = 'https://{}:18091/pools/default/buckets/{}/docs/{}'.format(ip, bucket,
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

    def write_db_creds_test(self, write_db_creds, buckets):
        for idx, dictionary in enumerate(write_db_creds):
            for key, value in dictionary.items():
                for bucket in buckets:
                    if bucket == value:
                        continue
                    else:
                        self.log.info("Trying to write a document into bucket - {}".format(bucket))
                        self.log.info("Bucket - {}, User - {}".format(bucket, key))
                        capella_api = CapellaAPI("https://" + self.url, self.secret_key,
                                                 self.access_key, self.user, self.passwd)
                        resp = capella_api.get_nodes(self.tenant_id, self.project_id,
                                                     self.cluster_id)
                        ip = resp.json()['data'][0]['data']['hostname']
                        doc_id = 'airline_8092'
                        url = 'https://{}:18091/pools/default/buckets/{}/docs/{}'.format(ip, bucket,
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

    def read_write_db_creds_test(self, read_write_db_creds, buckets):
        for idx, dictionary in enumerate(read_write_db_creds):
            for key, value in dictionary.items():
                for bucket in buckets:
                    if bucket == value:
                        continue
                    else:
                        self.log.info("Trying to read a document from bucket - {}".format(bucket))
                        self.log.info("Bucket - {}, User - {}".format(bucket, key))
                        capella_api = CapellaAPI("https://" + self.url, self.secret_key,
                                                 self.access_key, self.user, self.passwd)
                        resp = capella_api.get_nodes(self.tenant_id, self.project_id,
                                                     self.cluster_id)
                        ip = resp.json()['data'][0]['data']['hostname']
                        doc_id = 'airline_8091'
                        url = 'https://{}:18091/pools/default/buckets/{}/docs/{}'.format(ip, bucket,
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
                        self.log.info("Bucket - {}, User - {}".format(bucket, key))
                        doc_id = 'airline_8092'
                        url = 'https://{}:18091/pools/default/buckets/{}/docs/{}'.format(ip, bucket,
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

    def test_db_creds_parallely(self):
        self.log.info("Running a test to validate the database credentials")


        # buckets = []
        # threads = []
        # for i in range(10):
        #     capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
        #                              self.user, self.passwd)
        #     t = threading.Thread(target=capella_api.create_bucket, args=(self.tenant_id,
        #                                                                  self.project_id,
        #                                                                  self.cluster_id,
        #                                                                  {"name": "bucket_" +
        #                                                                   str(i)}))
        #     buckets.append("bucket_" + str(i))
        #     threads.append(t)
        #     t.start()
        #
        # for t in threads:
        #     t.join()
        #
        # self.log.info("Created all the buckets")
        #
        # threads = []
        # access_roles = ["read", "write", "readwrite", "readwriteexpiry"]
        # reads_db_creds = []
        # writes_db_creds = []
        # read_writes_db_creds = []
        # read_writes_db_creds_expiry = []
        # result_queue = Queue()
        # num = 0
        # for bucket in buckets:
        #     for access in access_roles:
        #         body = self.create_db_payload("Administrator_" + str(num), self.rest_password,
        #                                       bucket, access)
        #         t = threading.Thread(target=self.create_db_creds, args=(body, access, result_queue,
        #                                                                 bucket))
        #         threads.append(t)
        #         t.start()
        #         num = num + 1
        #
        # for t in threads:
        #     t.join()
        #
        # while not result_queue.empty():
        #     resp, access, bucket, username = result_queue.get()
        #     self.assertEqual(resp.status_code, 200,
        #                      msg='FAIL, Outcome:{}, Expected:{}, Reason: {}'
        #                      .format(resp.status_code, 200, resp.content))
        #     if resp.status_code == 200:
        #         if access == "read":
        #             reads_db_creds.append({username: bucket})
        #         elif access == "write":
        #             writes_db_creds.append({username: bucket})
        #         elif access == "readwrite":
        #             read_writes_db_creds.append({username: bucket})
        #         elif access == "readwriteexpiry":
        #             read_writes_db_creds_expiry.append({username: bucket})
        #
        # threads = []
        # t1 = threading.Thread(target=self.read_db_creds_test, args=(reads_db_creds, buckets))
        # t2 = threading.Thread(target=self.write_db_creds_test, args=(writes_db_creds, buckets))
        # t3 = threading.Thread(target=self.read_write_db_creds_test, args=(read_writes_db_creds, buckets))
        #
        # threads.append(t1)
        # threads.append(t2)
        # threads.append(t3)
        #
        # t1.start()
        # t2.start()
        # t3.start()
        #
        # for t in threads:
        #     t.join()
        #
        # self.log.info("The test is completed successfully")

    def test_db_creds(self):
        self.log.info("Running a test to validate the database credentials")
        buckets = []
        self.bucket_ids = []
        for i in range(10):
            capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                     self.user, self.passwd)
            bucket_params = {
                "name": "bucket_" + str(i)
            }
            resp = capella_api.create_bucket(self.tenant_id, self.project_id, self.cluster_id,
                                             bucket_params)
            if resp.status_code == 201:
                buckets.append("bucket_" + str(i))
            else:
                self.fail("Failed to create a bucket. Reason: {}".format(resp.content))

        self.log.info("Created all the buckets")

        access_roles = ["read", "write", "readwrite", "readwriteexpiry"]
        reads_db_creds = []
        writes_db_creds = []
        read_writes_db_creds = []
        read_writes_db_creds_expiry = []

        num = 0
        for bucket in buckets:
            for access in access_roles:
                username = "Administrator_" + str(num)
                body = self.create_db_payload(username, self.rest_password, bucket, access)
                resp = self.create_db_creds(body, access, bucket)

                if resp.status_code == 200:
                    if access == "read":
                        reads_db_creds.append({username: bucket})
                    elif access == "write":
                        writes_db_creds.append({username: bucket})
                    elif access == "readwrite":
                        read_writes_db_creds.append({username: bucket})
                    elif access == "readwriteexpiry":
                        read_writes_db_creds_expiry.append({username: bucket})

                num = num + 1

        self.log.info("Created all the db credentials")
        self.log.info("The Read DB Creds are - {}".format(reads_db_creds))
        self.log.info("The Write DB Creds are - {}".format(writes_db_creds))
        self.log.info("The Read/Write DB Creds are - {}".format(read_writes_db_creds))
        self.log.info("The Read/Write Expiry DB Creds are - {}".format(read_writes_db_creds_expiry))

        self.write_doc_into_buckets(buckets)

        self.read_db_creds_test(reads_db_creds, buckets)
        self.log.info("Done with Read tests")
        self.write_db_creds_test(writes_db_creds, buckets)
        self.log.info("Done with write tests")
        self.read_write_db_creds_test(read_writes_db_creds, buckets)
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
