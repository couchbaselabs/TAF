import time
import json
import base64
import random
import string
import requests
from pytests.basetestcase import BaseTestCase
from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI
from couchbase_utils.capella_utils.dedicated import CapellaUtils


class SecurityTest(BaseTestCase):
    def setUp(self):
        BaseTestCase.setUp(self)
        self.url = self.input.capella.get("pod")
        self.user = self.input.capella.get("capella_user")
        self.passwd = self.input.capella.get("capella_pwd")
        self.tenant_id = self.input.capella.get("tenant_id")
        self.secret_key = self.input.capella.get("secret_key")
        self.access_key = self.input.capella.get("access_key")
        self.project_id = self.input.capella.get("project")
        self.cluster_id = self.input.capella.get("clusters")
        if self.input.capella.get("test_users"):
            self.test_users = json.loads(self.input.capella.get("test_users"))
        else:
            self.test_users = {"User": {"password": self.passwd, "mailid": self.user,
                                        "role": "organizationOwner"}}

    def tearDown(self):
        super(SecurityTest, self).tearDown()

    @staticmethod
    def create_cluster(base_url, tenant_id, capella_api, cluster_details, timeout=1800):
        end_time = time.time() + timeout
        while time.time() < end_time:
            subnet = CapellaUtils.get_next_cidr() + "/20"
            print("Trying with cidr: {}".format(subnet))
            cluster_details["cidr"] = subnet
            url = '{0}/v2/organizations/{1}/clusters'.format("https://" + base_url, tenant_id)
            capella_api_resp = capella_api.do_internal_request(url, method="POST",
                                                               params=json.dumps(
                                                                   cluster_details))
            if capella_api_resp.status_code == 202:
                cluster_id = json.loads(capella_api_resp.content).get("id")
                print("Creating capella cluster with id: {0}".format(cluster_id))
                break
            elif capella_api_resp.status_code == 403:
                return capella_api_resp
            print("Create capella_utils cluster failed.")
            print("Capella API returned " + str(
                capella_api_resp.status_code))
            print(capella_api_resp.json()["message"])
        return capella_api_resp

    def connect_node_port(self, node, ports, expect_to_connect=True):
        session = requests.Session()
        for port in ports:
            connect = "https://" + node + ":" + port
            self.log.info("Trying to connect to {0}".format(connect))
            try:
                session.get(connect, params='', headers=None, timeout=60, verify=False)
            except requests.exceptions.ConnectionError as e:
                if expect_to_connect:
                    self.fail(msg="Connection to the node should have passed. Failed with "
                                  "error: {0} on port: {1}".format(e, port))
            else:
                if not expect_to_connect:
                    self.fail(msg="Connection to the node should have failed on port: {0}"
                              .format(port))

    def test_create_project(self):
        self.log.info("Verifying status code for creating project")
        expected_response_code = {"organizationOwner": 201, "projectCreator": 201,
                                  "cloudManager": 403, "organizationMember": 403}
        for user in self.test_users:
            self.log.info("Verifying status code for Role: {0}"
                          .format(self.test_users[user]["role"]))
            capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                     self.test_users[user]["mailid"],
                                     self.test_users[user]["password"])
            resp = capella_api.create_project(self.tenant_id, user + "_Project")
            if resp.status_code == 201:
                project_id = json.loads(resp.content).get("id")
                self.log.info("Creating capella project with id: {0}".format(project_id))
            else:
                self.log.info("Creating capella project failed: {}".format(resp.content))
            self.assertEqual(expected_response_code[self.test_users[user]["role"]],
                             resp.status_code, msg="FAIL, Outcome: {0}, Expected: {1}".format(
                    resp.status_code, expected_response_code[self.test_users[user]["role"]]))

    def test_delete_project(self):
        self.log.info("creating a project to test deletion")
        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                 self.user, self.passwd)
        resp = capella_api.create_project(self.tenant_id, "Project_to_check_deletion")
        project_id = json.loads(resp.content).get("id")
        self.log.info("Verifying status code for deleting project")
        expected_response_code = {"organizationOwner": 2, "projectCreator": 4,
                                  "cloudManager": 4, "organizationMember": 4}
        for user in self.test_users:
            self.log.info("Verifying status code for Role: {0}"
                          .format(self.test_users[user]["role"]))
            capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                     self.test_users[user]["mailid"],
                                     self.test_users[user]["password"])
            resp = capella_api.delete_project(self.tenant_id, project_id)
            self.assertEqual(expected_response_code[self.test_users[user]["role"]],
                             resp.status_code // 100,
                             msg="FAIL, Outcome: {0}, Expected: {1}".format(
                                 resp.status_code // 100,
                                 expected_response_code[self.test_users[user]["role"]]))

    def test_retrieve_cluster_details(self):
        self.log.info("Verifying status code for retrieving cluster details")
        expected_response_code = {"organizationOwner": 200, "projectCreator": 403,
                                  "cloudManager": 403, "organizationMember": 403}
        for user in self.test_users:
            self.log.info("Verifying status code for Role: {0}"
                          .format(self.test_users[user]["role"]))
            capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                     self.test_users[user]["mailid"],
                                     self.test_users[user]["password"])
            resp = capella_api.get_cluster_internal(self.tenant_id, self.project_id,
                                                    self.cluster_id)
            self.assertEqual(expected_response_code[self.test_users[user]["role"]],
                             resp.status_code,
                             msg="FAIL, Outcome: {0}, Expected: {1}"
                             .format(resp.status_code,
                                     expected_response_code[self.test_users[user]["role"]]))
        self.log.info("Retrieve  details of a cluster that does not exist")
        non_exist_cluster_id = "00000000-0000-0000-0000-000000000000"
        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                 self.user, self.passwd)
        resp = capella_api.get_cluster_internal(self.tenant_id, self.project_id,
                                                non_exist_cluster_id)
        self.assertEqual(404, resp.status_code, msg="FAIL, Outcome: {0}, Expected: {1}"
                         .format(resp.status_code, 404))

    def test_deploy_cluster(self):
        self.log.info("Verifying status code for deploying cluster")
        expected_response_code = {"organizationOwner": 202, "projectCreator": 403,
                                  "cloudManager": 403, "organizationMember": 403}
        for user in self.test_users:
            self.log.info("Verifying status code for Role: {0}"
                          .format(self.test_users[user]["role"]))
            capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                     self.test_users[user]["mailid"],
                                     self.test_users[user]["password"])
            capella_cluster_config = {"region": "us-west-2", "name": user + "_Cluster",
                                      "cidr": None, "singleAZ": False,
                                      "specs": [{"services": ["kv"], "count": 3,
                                                 "compute": "m5.xlarge",
                                                 "disk": {"type": "gp3", "sizeInGb": 50,
                                                          "iops": 3000}}],
                                      "plan": "Developer Pro",
                                      "projectId": self.project_id, "timezone": "PT",
                                      "description": "", "provider": "aws"}
            resp = self.create_cluster(self.url.replace("cloud", "", 1), self.tenant_id,
                                       capella_api,
                                       capella_cluster_config)
            self.assertEqual(expected_response_code[self.test_users[user]["role"]],
                             resp.status_code, msg="FAIL, Outcome: {0}, Expected: {1}"
                             .format(resp.status_code,
                                     expected_response_code[self.test_users[user]["role"]]))

    def test_create_bucket(self):
        self.log.info("Verifying status code for creating bucket")
        expected_response_code = {"organizationOwner": 201, "projectCreator": 403,
                                  "cloudManager": 403, "organizationMember": 403}
        for user in self.test_users:
            self.log.info("Verifying status code for Role: {0}"
                          .format(self.test_users[user]["role"]))
            capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                     self.test_users[user]["mailid"],
                                     self.test_users[user]["password"])
            resp = capella_api.load_sample_bucket(self.tenant_id, self.project_id,
                                                  self.cluster_id, "travel-sample")
            self.assertEqual(expected_response_code[self.test_users[user]["role"]],
                             resp.status_code,
                             msg="FAIL, Outcome: {0}, Expected: {1}"
                             .format(resp.status_code,
                                     expected_response_code[self.test_users[user]["role"]]))

    def test_retrieve_bucket_details(self):
        self.log.info("Verifying status code for retrieving bucket details")
        expected_response_code = {"organizationOwner": 200, "projectCreator": 403,
                                  "cloudManager": 403, "organizationMember": 403}
        for user in self.test_users:
            self.log.info("Verifying status code for Role: {0}"
                          .format(self.test_users[user]["role"]))
            capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                     self.test_users[user]["mailid"],
                                     self.test_users[user]["password"])
            resp = capella_api.get_buckets(self.tenant_id, self.project_id, self.cluster_id)
            self.assertEqual(expected_response_code[self.test_users[user]["role"]],
                             resp.status_code,
                             msg="FAIL, Outcome: {0}, Expected: {1}"
                             .format(resp.status_code,
                                     expected_response_code[self.test_users[user]["role"]]))

    def test_connect_node_ip_allowlist(self):
        self.log.info("Verifying user can connect to the node only when the ip is added to the "
                      "allowlist and to valid ports")
        expected_response_code = {"organizationOwner": 200, "projectCreator": 403,
                                  "cloudManager": 403, "organizationMember": 403}
        self.log.info("Verifying status code for adding a database user:")
        for user in self.test_users:
            self.log.info("Verifying status code for Role: {0}"
                          .format(self.test_users[user]["role"]))
            capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                     self.test_users[user]["mailid"],
                                     self.test_users[user]["password"])
            resp = capella_api.create_db_user(self.tenant_id, self.project_id, self.cluster_id,
                                              user, self.rest_password)
            self.assertEqual(expected_response_code[self.test_users[user]["role"]],
                             resp.status_code,
                             msg="FAIL, Outcome: {0}, Expected: {1}"
                             .format(resp.status_code,
                                     expected_response_code[self.test_users[user]["role"]]))
            if resp.status_code == 200:
                self.log.info("Trying to connect to the node...")
                self.log.info("Ip is already added in the base test case, so should be able to "
                              "connect to the node")
                resp = capella_api.get_nodes(self.tenant_id, self.project_id,
                                             self.cluster_id)
                node = json.loads(resp.content)["data"][0]["data"]["hostname"]
                valid_ports = ["18091", "18092", "18093", "18094", "18095", "18096"]
                self.connect_node_port(node, valid_ports, expect_to_connect=True)

                invalid_ports = ["22", "3389"]
                random_ports = random.sample(range(0, 65536), 5)
                s = set(valid_ports)
                invalid_ports.extend([str(x) for x in random_ports if str(x) not in s])
                self.connect_node_port(node, invalid_ports, expect_to_connect=False)

                self.log.info("Deleting the ip")
                # get ip id
                url = '{}/v2/organizations/{}/projects/{}/clusters/{}' \
                    .format("https://" + self.url.replace("cloud", "", 1), self.tenant_id,
                            self.project_id,
                            self.cluster_id)
                url = url + '/allowlists?page={0}&perPage={1}'.format(1, 100)
                resp = capella_api.do_internal_request(url, method="GET")
                ip_id = json.loads(resp.content)["data"][0]["data"]["id"]
                # delete ip
                del_ip_body = {"delete": ["{0}".format(ip_id)]}
                url = '{}/v2/organizations/{}/projects/{}/clusters/{}' \
                    .format("https://" + self.url.replace("cloud", "", 1), self.tenant_id,
                            self.project_id,
                            self.cluster_id)
                url = url + '/allowlists-bulk'
                capella_api.do_internal_request(url, method="POST",
                                                params=json.dumps(del_ip_body))
                self.sleep(1000, message="Waiting for ips to get deleted")

                ports = valid_ports + invalid_ports
                self.connect_node_port(node, ports, expect_to_connect=False)

    def test_login_to_cb(self):
        self.log.info("Verifying user can login with valid credentials")
        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                 self.user, self.passwd)
        resp = capella_api.get_nodes(self.tenant_id, self.project_id, self.cluster_id)
        node = json.loads(resp.content)["data"][3]["data"]["hostname"]
        api = "https://" + node + ':18091/pools/default'
        self.log.info("Connecting to {0}".format(api))
        usernames = [self.rest_username, self.rest_username + random.choice(string.ascii_letters)]
        passwords = [self.rest_password, self.rest_password + random.choice(string.ascii_letters)]
        for username in usernames:
            for password in passwords:
                authorization = base64.encodestring('%s:%s'
                                                    % (username, password)).strip("\n")
                headers = {'Content-Type': 'application/x-www-form-urlencoded',
                           'Authorization': 'Basic %s' % authorization,
                           'Connection': 'close',
                           'Accept': '*/*'}
                session = requests.Session()
                response = session.get(api, params='', headers=headers, timeout=60, verify=False)
                if username == self.rest_username and password == self.rest_password:
                    self.assertEqual(200, response.status_code, "User should be able to login")
                else:
                    self.assertEqual(401, response.status_code, "User should not be able to login")

    def test_delete_and_restore_backup_data(self):
        expected_response_code = {"organizationOwner": 200, "projectCreator": 403,
                                  "cloudManager": 403, "organizationMember": 403}
        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                 self.user, self.passwd)
        capella_api.load_sample_bucket(self.tenant_id, self.project_id,
                                       self.cluster_id, "travel-sample")
        self.sleep(50, message="Waiting for buckets to load")
        capella_api.backup_now(self.tenant_id, self.project_id, self.cluster_id,
                               "travel-sample")
        self.sleep(300, message="Waiting for backup")
        self.log.info("Retrieving backup details")
        url = "{0}/v2/organizations/{1}/projects/{2}/clusters/{3}/backups".format("https://" +
                                                                                  self.url.replace(
                                                                                      "cloud",
                                                                                      "", 1),
                                                                                  self.tenant_id,
                                                                                  self.project_id,
                                                                                  self.cluster_id)
        resp = capella_api.do_internal_request(url, method="GET", params='')

        bucket_id = json.loads(resp.content)["data"][0]["data"]["bucketId"]
        backup_id = json.loads(resp.content)["data"][0]["data"]["id"]
        for user in self.test_users:
            self.log.info("Verifying status code for role : {0}"
                          .format(self.test_users[user]["role"]))
            capella_api_user = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                          self.test_users[user]["mailid"],
                                          self.test_users[user]["password"])
            self.log.info("Verifying status code for restoring backup data")
            payload = {"sourceClusterId": self.cluster_id,
                       "targetClusterId": self.cluster_id,
                       "options": {"services": ["data", "query", "index", "search"],
                                   "filterKeys": "",
                                   "filterValues": "",
                                   "mapData": "", "includeData": "", "excludeData": "",
                                   "autoCreateBuckets": True,
                                   "autoRemoveCollections": True, "forceUpdates": True}}
            url = r"{}/v2/organizations/{}/projects/{}/clusters/{}/buckets/{}/restore" \
                .format("https://" + self.url.replace("cloud", "", 1), self.tenant_id,
                        self.project_id, self.cluster_id, bucket_id)
            resp = capella_api_user.do_internal_request(url, method="POST",
                                                        params=json.dumps(payload))
            self.sleep(100, message="Waiting for backups to restore")
            self.assertEqual(expected_response_code[self.test_users[user]["role"]] // 100,
                             resp.status_code // 100,
                             msg="FAIL, Outcome: {0}, Expected: {1}"
                             .format(resp.status_code,
                                     expected_response_code[self.test_users[user]["role"]]))

            self.log.info("Verifying status code for deleting backup data")
            url = "{}/v2/organizations/{}/projects/{}/clusters/{}/backups/{}/cycle".format(
                "https://" + self.url.replace("cloud", "", 1), self.tenant_id, self.project_id,
                self.cluster_id, backup_id)
            result = capella_api_user.do_internal_request(url, method="DELETE", params='')
            self.assertEqual(expected_response_code[self.test_users[user]["role"]] // 100,
                             result.status_code // 100,
                             msg="FAIL, Outcome: {0}, Expected: {1}"
                             .format(result.status_code,
                                     expected_response_code[self.test_users[user]["role"]]))

    def test_jump_tenant_boundary(self):
        self.log.info("Jumping tenant boundaries")
        self.log.info("Create a cluster in a different tenant where the user does not exist")
        diff_tenant_id = self.input.param("diff_tenant_id", "00000000-0000-0000-0000-000000000000")
        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                 self.user, self.passwd)
        capella_cluster_config = {"region": "us-west-2", "name": self.user + "_Cluster",
                                  "cidr": None, "singleAZ": False,
                                  "specs": [{"services": ["kv"], "count": 3,
                                             "compute": "m5.xlarge",
                                             "disk": {"type": "gp3", "sizeInGb": 50,
                                                      "iops": 3000}}],
                                  "plan": "Developer Pro",
                                  "projectId": self.project_id, "timezone": "PT",
                                  "description": "", "provider": "aws"}
        resp = self.create_cluster(self.url.replace("cloud", "", 1), diff_tenant_id, capella_api,
                                   capella_cluster_config, timeout=30)
        self.assertEqual(404, resp.status_code,
                         msg="FAIL, Outcome: {0}, Expected: {1}".format(resp.status_code, 404))

        self.log.info("Expose details of a user who is part of the tenant")
        url = "{0}/v2/organizations/{1}/users/{2}".format("https://" +
                                                          self.url.replace("cloud", "", 1),
                                                          self.tenant_id,
                                                          self.test_users["User1"]["userid"])
        resp = capella_api.do_internal_request(url, method="GET", params='')
        self.assertEqual(200, resp.status_code,
                         msg="FAIL, Outcome: {0}, Expected: {1}".format(resp.status_code, 200))
        self.log.info("Expose details of a user who is not part of the tenant")
        url = "{0}/v2/organizations/{1}/users/{2}".format("https://" +
                                                          self.url.replace("cloud", "", 1),
                                                          diff_tenant_id,
                                                          self.test_users["User1"]["userid"])
        resp = capella_api.do_internal_request(url, method="GET", params='')
        self.assertEqual(500, resp.status_code,
                         msg="FAIL, Outcome: {0}, Expected: {1}".format(resp.status_code, 500))
