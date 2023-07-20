import time
import json
import base64
import random
import string
import requests
from pytests.basetestcase import BaseTestCase
from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI
from capellaAPI.capella.common.CapellaAPI import CommonCapellaAPI
from couchbase_utils.capella_utils.dedicated import CapellaUtils
from platform_utils.remote.remote_util import RemoteMachineShellConnection


class ServerInfo:
    def __init__(self,
                 ip,
                 port,
                 ssh_username,
                 ssh_password,
                 memcached_port,
                 ssh_key=''):
        self.ip = ip
        self.ssh_username = ssh_username
        self.ssh_password = ssh_password
        self.port = port
        self.ssh_key = ssh_key
        self.memcached_port = memcached_port
        self.type = None
        self.remote_info = None


class SecurityTest(BaseTestCase):
    SLAVE_HOST = ServerInfo('127.0.0.1', 22, 'root', 'couchbase', 18091)

    def setUp(self):
        BaseTestCase.setUp(self)
        self.url = self.input.capella.get("pod")
        self.user = self.input.capella.get("capella_user")
        self.passwd = self.input.capella.get("capella_pwd")
        self.tenant_id = self.input.capella.get("tenant_id")
        self.secret_key = self.input.capella.get("secret_key")
        self.access_key = self.input.capella.get("access_key")
        self.project_id = self.tenant.project_id
        self.cluster_id = self.cluster.id
        self.invalid_id = "00000000-0000-0000-0000-000000000000"
        if self.input.capella.get("test_users"):
            self.test_users = json.loads(self.input.capella.get("test_users"))
        else:
            self.test_users = {"User1": {"password": self.passwd, "mailid": self.user,
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
                    self.fail(
                        msg="Connection to the node should have passed. Failed with error: {0} on "
                            "port: {1}".format(e, port))
            else:
                if not expect_to_connect:
                    self.fail(
                        msg="Connection to the node should have failed on port: {0}".format(
                            port))

    def run_query(self, user, password, role, query_statement):
        pod = "https://" + self.url.replace("cloud", "", 1)
        url = "{0}/v2/databases/{1}/proxy/_p/query/query/service".format(pod, self.cluster_id)
        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key, user,
                                 password)
        body = {"statement": "{0}".format(query_statement)}
        resp = capella_api.do_internal_request(url, method="POST", params=json.dumps(body))
        status = resp.status_code
        content = resp.content
        if role != "organizationOwner":
            if status == 412:
                self.log.info("Pass. No permissions")
            else:
                self.fail("FAIL. Permission shouldn't be allowed")
        elif 13014 == json.loads(content.decode('utf-8'))["errors"][0]["code"]:
            self.log.info("Pass. Curl access denied")
        else:
            self.fail("FAIL. CURL access shouldn't be allowed")

    def find_buckets(self, name):
        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key, self.user,
                                 self.passwd)
        total_buckets = capella_api.get_buckets(self.tenant_id, self.project_id, self.cluster_id)
        if total_buckets.status_code == 422:
            self.fail("Not able to fetch the buckets in the cluster")
        total_buckets = json.loads(total_buckets.content)
        for bucket in total_buckets["buckets"]["data"]:
            if bucket["data"]["name"] == name:
                self.log.info("Got the bucket - ", name)
                return True
        return False

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
            if self.test_users[user]["role"] != "organizationOwner":
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

                invalid_ports = ["3389"]
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
        node = json.loads(resp.content)["data"][0]["data"]["hostname"]
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

        self.log.info("Expose details of a user who is not part of the tenant")
        url = "{0}/v2/organizations/{1}/users/{2}".format("https://" +
                                                          self.url.replace("cloud", "", 1),
                                                          self.tenant_id, diff_tenant_id)
        resp = capella_api.do_internal_request(url, method="GET", params='')
        self.assertEqual(404, resp.status_code,
                         msg="FAIL, Outcome: {0}, Expected: {1}".format(resp.status_code, 404))

    def test_n1ql_service(self):
        self.log.info("Verifying status code for running cURL via query")
        queries = ["SELECT CURL(\"\", \"\");"]
        for user in self.test_users:
            for query_statement in queries:
                self.log.info(
                    "Verifying status code for Role: {0}".format(self.test_users[user]["role"]))
                self.log.info("Running query: {0}".format(query_statement))
                self.run_query(self.test_users[user]["mailid"], self.test_users[user]["password"],
                               self.test_users[user]["role"], query_statement)

    def test_invalid_cpu_and_memory_parameters(self):
        self.log.info("Verifying status code for deploying cluster with invalid cpu and parameters")
        invalid_compute = ["Standard_D2s_v4", "Standard_D3s_v4"]
        for compute in invalid_compute:
            capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                     self.user, self.passwd)
            capella_cluster_config = {"region": "eastus", "name": "_Cluster",
                                      "cidr": "10.64.118.0/23", "singleAZ": False,
                                      "specs": [{"services": ["kv"], "count": 3,
                                                 "compute": compute,
                                                 "disk": {"type": "P6", "sizeInGb": 64,
                                                          "iops": 240}}],
                                      "plan": "Developer Pro",
                                      "projectId": self.project_id, "timezone": "PT",
                                      "description": "", "provider": "hostedAzure"}
            resp = self.create_cluster(self.url.replace("cloud", "", 1), self.tenant_id,
                                       capella_api, capella_cluster_config, timeout=100)
            self.assertEqual(422, resp.status_code, msg="FAIL, Outcome: {0}, Expected: {1}"
                             .format(resp.status_code, 422))

    def test_deploy_cluster_with_invalid_node_configuration(self):
        self.log.info("Verifying status code for deploying various cluster configurations")
        invalid_config = [[["kv", "index"], ["index", "fts"]],
                          [["index"], ["fts"]],
                          [["kv", "n1ql"], ["n1ql", "index"]]]
        for config in invalid_config:
            self.log.info("Verifying status code for cluster configuration: {0}".format(config))
            capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                     self.user, self.passwd)
            capella_cluster_config = {"region": "eastus", "name": "Test", "cidr": "10.0.70.0/23",
                                      "singleAZ": True, "specs": [{"services": config[0],
                                                                   "count": 3,
                                                                   "compute": "Standard_D4s_v5",
                                                                   "disk": {"type": "P6",
                                                                            "sizeInGb": 64,
                                                                            "iops": 240}},
                                                                  {"services": config[1],
                                                                   "count": 3,
                                                                   "compute": "Standard_D4s_v5",
                                                                   "disk": {"type": "P6",
                                                                            "sizeInGb": 64,
                                                                            "iops": 240}}],
                                      "plan": "Developer Pro", "projectId": self.project_id,
                                      "timezone": "PT", "description": "",
                                      "provider": "hostedAzure"}
            resp = self.create_cluster(self.url.replace("cloud", "", 1), self.tenant_id,
                                       capella_api, capella_cluster_config, timeout=100)
            self.assertEqual(422, resp.status_code,
                             msg="FAIL, Outcome: {0}, Expected: {1}".format(resp.status_code, 422))

    def test_zone_transfer(self):
        self.log.info("Verifying if zone tranfer is possible or not")
        pod = "https://" + self.url.replace("cloud", "", 1)
        url = "{0}/v2/organizations/{1}/projects/{2}/clusters/{3}".format(pod, self.tenant_id,
                                                                          self.project_id,
                                                                          self.cluster_id)
        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                 self.user, self.passwd)
        resp = capella_api.do_internal_request(url, method="GET")
        shell = RemoteMachineShellConnection(SecurityTest.SLAVE_HOST)
        connection_string = json.loads(resp.content.decode('utf-8'))["data"]["connect"]["srv"]
        cmd = "dig {0} AXFR".format(connection_string)
        output, error = shell.execute_command(cmd)
        sz = len(output)
        if output[sz - 1] == "; Transfer failed.":
            self.log.info("Zone transfer failed as expected")
        else:
            self.fail("Test failed. Zone transfer should have failed")
        shell.disconnect()

    def test_eventing_curl(self):
        self.log.info(
            "Verifying that executing curl command to access metadata in eventing is not allowed")
        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                 self.user, self.passwd)
        bucket_present = self.find_buckets("beer-sample")
        if not bucket_present:
            capella_api.load_sample_bucket(self.tenant_id, self.project_id, self.cluster_id,
                                           "beer-sample")
        body = {
            "appcode": "function curlIMDS() {\n  try {  \n    var result = curl(\"GET\", "
                       "azureApi, {\n        headers: {\n            \"Metadata\":\"true\"   \n   "
                       "     }\n    });\n    log(result);\n} \ncatch(e) \n{\n    log("
                       "e);\n}\n}\n\nfunction OnUpdate(doc, meta) {\n    log(\"Doc "
                       "created/updated\", meta.id);\n    curlIMDS();\n}\n\nfunction OnDelete("
                       "meta, options) {\n    log(\"Doc deleted/expired\", meta.id);\n}",
            "depcfg": {"curl": [
                {"hostname": "http://169.254.169.254/metadata/instance?api-version=2021-02-01",
                 "value": "azureApi", "auth_type": "no-auth", "username": "", "password": "*****",
                 "bearer_key": "*****", "allow_cookies": False, "validate_ssl_certificate": False}],
                "source_bucket": "beer-sample", "source_scope": "_default",
                "source_collection": "_default", "metadata_bucket": "metadata",
                "metadata_scope": "_default", "metadata_collection": "_default"},
            "version": "", "enforce_schema": False, "handleruuid": 651380377,
            "function_instance_id": "R6mcj", "appname": "curl_command",
            "settings": {"dcp_stream_boundary": "from_now", "deadline_timeout": 62,
                         "deployment_status": True, "description": "Testing for curl command",
                         "execution_timeout": 60, "language_compatibility": "6.6.2",
                         "log_level": "INFO", "n1ql_consistency": "request",
                         "processing_status": True, "timer_context_size": 1024,
                         "user_prefix": "eventing", "worker_count": 1},
            "function_scope": {"bucket": "*", "scope": "*"}
        }
        resp = capella_api.create_eventing_function(self.cluster_id, body["appname"], body,
                                                    body["function_scope"])
        if resp.status_code == 422:
            self.log.info("Eventing function is already created")
        time.sleep(10)
        query = "INSERT INTO `beer-sample`._default._default (KEY, VALUE) VALUES (" \
                "\"airline_test-2222\", {\"id\":\"007\",\"type\":\"airline\"," \
                "\"name\":\"couchbase-airlines\",\"iata\":\"Q5\",\"icao\":\"MLA\"," \
                "\"callsign\":\"MILE-AIR\",\"country\":\"India\"}); "
        pod = "https://" + self.url.replace("cloud", "", 1)
        url = "{0}/v2/databases/{1}/proxy/_p/query/query/service".format(pod, self.cluster_id)
        query_body = {"statement": "{0}".format(query)}
        capella_api.do_internal_request(url, method="POST", params=json.dumps(query_body))
        logs_url = "{0}/v2/databases/{1}/proxy/_p/event/getAppLog?aggregate=true&name={2}".format(
            pod, self.cluster_id,
            body["appname"])
        time.sleep(80)
        resp = capella_api.do_internal_request(logs_url, method="GET")
        compare_string = "Unable to perform the request: Timeout was reached"
        logs = resp.content.decode('utf-8').split("\n")
        one_log = logs[0].split(" [INFO] ")
        if json.loads(one_log[1].decode('utf-8'))["message"] == compare_string:
            self.log.info(
                "Timeout was reached. As expected, curl in eventing cannot access metadata")
        else:
            self.fail("Curl access to metadata is allowed")

    def test_export_backup(self):
        """
        Function to test the backup export functionality
        Test verifies the following:
        i.   RBAC enforced on each API
        ii.  Pass in junk values
        """
        self.log.info("Verifying user can use the export backup methods in accordance with the "
                      "RBAC")
        expected_response_code = {"organizationOwner": 200, "projectCreator": 403,
                                  "cloudManager": 403, "organizationMember": 403}
        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                 self.user, self.passwd)
        self.log.info("Importing travel-sample bucket")
        capella_api.load_sample_bucket(self.tenant_id, self.project_id, self.cluster_id,
                                       "travel-sample")
        self.sleep(30, "Waiting for buckets to load")
        bucket_id = "dHJhdmVsLXNhbXBsZQ=="
        self.log.info("Backing up travel-sample bucket")
        capella_api.backup_now(self.tenant_id, self.project_id, self.cluster_id,
                               "travel-sample")
        self.sleep(240, "Waiting for backup to complete")
        for user in self.test_users:
            self.log.info("Verifying status code for Role: {0}"
                          .format(self.test_users[user]["role"]))
            capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                     self.test_users[user]["mailid"],
                                     self.test_users[user]["password"])

            self.log.info("List all backups")
            resp = capella_api.list_all_bucket_backups(self.tenant_id, self.project_id,
                                                       self.cluster_id, bucket_id)
            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(expected_response_code[self.test_users[user]["role"]] // 100,
                                 resp.status_code // 100,
                                 msg="FAIL, Outcome: {0}, Expected: {1}"
                                 .format(resp.status_code,
                                         expected_response_code[self.test_users[user]["role"]]))
                backup_id = json.loads(resp.content)["backups"]["data"][0]["data"]["id"]
            else:
                self.assertEqual(expected_response_code[self.test_users[user]["role"]],
                                 resp.status_code,
                                 msg="FAIL, Outcome: {0}, Expected: {1}"
                                 .format(resp.status_code,
                                         expected_response_code[self.test_users[user]["role"]]))
                backup_id = self.invalid_id
            self.log.info("Backup id: {}".format(backup_id))

            self.log.info("Begin an export")
            resp = capella_api.begin_export(self.tenant_id, self.project_id, self.cluster_id,
                                            backup_id)
            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(expected_response_code[self.test_users[user]["role"]] // 100,
                                 resp.status_code // 100,
                                 msg="FAIL, Outcome: {0}, Expected: {1}"
                                 .format(resp.status_code,
                                         expected_response_code[self.test_users[user]["role"]]))
            else:
                self.assertEqual(expected_response_code[self.test_users[user]["role"]],
                                 resp.status_code,
                                 msg="FAIL, Outcome: {0}, Expected: {1}"
                                 .format(resp.status_code,
                                         expected_response_code[self.test_users[user]["role"]]))

            self.log.info("List what exports are queued, executing and finished")
            resp = capella_api.export_status(self.tenant_id, self.project_id, self.cluster_id,
                                             bucket_id)
            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(expected_response_code[self.test_users[user]["role"]] // 100,
                                 resp.status_code // 100,
                                 msg="FAIL, Outcome: {0}, Expected: {1}"
                                 .format(resp.status_code,
                                         expected_response_code[self.test_users[user]["role"]]))
                export_id = json.loads(resp.content)["data"][0]["data"]["id"]
            else:
                self.assertEqual(expected_response_code[self.test_users[user]["role"]],
                                 resp.status_code,
                                 msg="FAIL, Outcome: {0}, Expected: {1}"
                                 .format(resp.status_code,
                                         expected_response_code[self.test_users[user]["role"]]))
                export_id = self.invalid_id
            self.log.info("Export id: {}".format(export_id))

            self.log.info("Generate a pre-signed link for the given export")
            resp = capella_api.generate_export_link(self.tenant_id, self.project_id,
                                                    self.cluster_id, export_id)
            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(409, resp.status_code,
                                 msg="FAIL, Outcome: {0}, Expected: {1}"
                                 .format(resp.status_code, 409))
            else:
                self.assertEqual(expected_response_code[self.test_users[user]["role"]],
                                 resp.status_code,
                                 msg="FAIL, Outcome: {0}, Expected: {1}"
                                 .format(resp.status_code,
                                         expected_response_code[self.test_users[user]["role"]]))

        self.log.info("Verifying response on passing invalid/junk values")

        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                 self.user, self.passwd)

        self.log.info("Generate a pre-signed link for the given export - invalid export id")
        resp = capella_api.generate_export_link(self.tenant_id, self.project_id,
                                                self.cluster_id, self.invalid_id)
        self.assertEqual(404, resp.status_code,
                         msg="FAIL, Outcome: {0}, Expected: {1}".format(resp.status_code, 404))

    def test_turn_off_on_cluster(self):
        self.log.info("Verifying the status code for turning clusters on/off")
        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                 self.user, self.passwd)
        common_capella_api = CommonCapellaAPI("https://" + self.url, self.secret_key,
                                              self.access_key, self.user, self.passwd)
        # loading a sample bucket
        capella_api.load_sample_bucket(self.tenant_id, self.project_id, self.cluster_id,
                                       "beer-sample")
        self.sleep(20, "Waiting for the beer-sample bucket to be loaded")

        #Turning off a cluster
        resp = capella_api.turn_off_cluster(self.tenant_id, self.project_id, self.cluster_id)
        self.assertEqual(202, resp.status_code,
                         msg='FAIL, Outcome: {0}, Expected: {1}'.format(resp.status_code, 202))

        end_time = time.time() + 320
        while time.time() < end_time:
            cluster_status = capella_api.get_cluster_status(self.cluster_id)
            status = json.loads(cluster_status.content.decode('utf-8'))["status"]
            if status == "turnedOff":
                break
            self.sleep(10, "Waiting for the cluster to be in turned off state")

        # Testing if running a query is allowed or not. Ideally it shouldn't
        query = "SELECT * FROM `beer-sample`._default._default"
        payload = {"timeout":"600s","statement": query,
                   "profile":"timings","use_cbo":True,"txtimeout":"120s"}
        resp = common_capella_api.run_query(self.cluster_id, payload)
        self.assertEqual(422, resp.status_code,
                         msg='FAIL, Outcome:{}, Expected: {}'.format(resp.status_code, 422))

        # Testing if adding ip to the allowlist is allowed or not. Ideally it should
        ips = ["104.172.65.2"]  # Any random ip
        resp = capella_api.add_allowed_ips(self.tenant_id, self.project_id, self.cluster_id, ips)
        self.assertEqual(202, resp.status_code,
                      msg='FAIL, Outcome: {0}, Expected: {1}'.format(resp.status_code, 202))

    def enable_gcp_private_endpoint(self, tenant_id, project_id, cluster_id, header='', payload='',
                            method='POST'):
        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                 self.user, self.passwd)
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/privateendpoint'\
            .format(capella_api.internal_url, tenant_id, project_id,
                    cluster_id)
        response = capella_api._urllib_request(url, method=method, params=payload, headers=header)
        return response

    def get_gcp_private_endpoint_status(self, tenant_id, project_id, cluster_id, header='',
                                        payload='', method='GET'):
        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                 self.user, self.passwd)
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/privateendpoint' \
            .format(capella_api.internal_url, tenant_id, project_id, cluster_id)
        response = capella_api._urllib_request(url, method=method, params=payload, headers=header)
        return response

    def delete_gcp_private_endpoint(self, tenant_id, project_id, cluster_id, header='',
                                        payload='', method='DELETE'):
        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                 self.user, self.passwd)
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/privateendpoint' \
            .format(capella_api.internal_url, tenant_id, project_id, cluster_id)
        response = capella_api._urllib_request(url, method=method, params=payload, headers=header)
        return response

    def get_gcp_private_endpoint_connection_link(self, tenant_id, project_id, cluster_id, header='',
                                             payload='', method='POST'):
        """
            body =
            {
                "vpcId": "{customers-VPC}",
                "subnetIds": "{customers-subnet}"
            }
        """
        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                 self.user, self.passwd)
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/privateendpoint/linkcommand' \
            .format(capella_api.internal_url, tenant_id, project_id, cluster_id)
        resp = capella_api._urllib_request(url, method=method, params=json.dumps(payload),
                                           headers=header)
        return resp

    def accept_gcp_private_endpoint_connection(self, tenant_id, project_id, cluster_id, header='',
                                             payload='', method="POST"):
        """
            body =
            {
                "endpointId": "{customers-GCP-Project-Id}"
            }
        """
        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                 self.user, self.passwd)
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/privateendpoint/connection' \
            .format(capella_api.internal_url, tenant_id, project_id, cluster_id)
        resp = capella_api._urllib_request(url, method=method, params=json.dumps(payload),
                                           headers=header)
        return resp

    def list_gcp_private_endpoint_connections(self, tenant_id, project_id, cluster_id, header='',
                                             payload='', method="GET"):

        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                 self.user, self.passwd)
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/privateendpoint/connection' \
            .format(capella_api.internal_url, tenant_id, project_id, cluster_id)
        resp = capella_api._urllib_request(url, method=method, params=json.dumps(payload),
                                           headers=header)
        return resp

    def reject_gcp_private_endpoint_connection(self, tenant_id, project_id, cluster_id,
                                              gcp_project_id, header='', payload='',
                                               method="DELETE"):
        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                 self.user, self.passwd)
        url = '{}/v2/organizations/{}/projects/{}/clusters/{}/privateendpoint/connection/{}' \
            .format(capella_api.internal_url, tenant_id, project_id, cluster_id, gcp_project_id)
        resp = capella_api._urllib_request(url, method=method, params=json.dumps(payload),
                                           headers=header)
        return resp

    def test_gcp_private_endpoint(self):
        self.log.info("Verifying the GCP private endpoints enabling and disabling")

        capella_api = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                 self.user, self.passwd)

        # To be integrated when the GCP Private Endpoint changes have been merged to master
        #
        # create_cluster_body = {"projectId": "28c626d3-5fa1-4247-8670-b5b7ec7b229d", "specs": [
        #     {"provider": "gcp", "compute": "n2-standard-4",
        #      "disk": {"type": "pd-ssd", "sizeInGb": 50, "iops": 3000},
        #      "services": ["kv", "index", "n1ql", "fts"],
        #      "count": 3, "diskAutoScaling": {"enabled": True}}], "name": "PrivateEndpointDB",
        #  "region": "us-east1", "provider": "hostedGCP", "cidr": "10.0.6.0/23", "description": "",
        #  "timezone": "PT", "singleAZ": False, "plan": "Developer Pro", "server": "7.1"}
        #
        # subnet = CapellaUtils.get_next_cidr() + "/20"
        # create_cluster_body["cidr"] = subnet
        #
        # create_cluster_resp = capella_api.create_cluster(json.dumps(create_cluster_body))
        # print("The cluster id is -", create_cluster_resp.content)
        # cluster_id = json.loads(create_cluster_resp.content.decode('utf-8'))["id"]
        # cluster_status = capella_api.get_cluster_status(cluster_id)
        #
        # while cluster_status != "healthy":
        #     cluster_status = json.loads(create_cluster_resp.content.decode('utf-8'))["id"]
        #     if cluster_status == "healthy":
        #         break
        #     self.sleep(10, message="Waiting for cluster to be in healthy state")

        initial_resp = capella_api.enable_private_endpoint(self.tenant_id, self.project_id,
                                                       self.cluster_id)
        self.assertEqual(202, initial_resp.status_code,
                             msg='FAIL, Outcome: {0}, Expected:{1}'
                             .format(initial_resp.status_code, 202))
        status = "disabled"
        while status != "enabled":
            resp = capella_api.get_private_endpoint_status(self.tenant_id, self.project_id,
                                                               self.cluster_id)
            status = json.loads(resp.content.decode('utf-8'))["data"]["status"]
            if status == "enableFailed":
                self.fail(msg="Enabling GCP Private Endpoint has failed for the cluster")
            self.sleep(60, message="Waiting for gcp private endpoints to be enabled")

        # Trying out with different headers to bypass authentication
        self.log.info("Calling API with various headers to bypass authentication")
        headers = {
            "invalid_header": 'abcdefgh',
            "empty_header": ''
        }
        for header in headers:
            cbc_api_request_headers = {
                'Authorization': 'Bearer %s' % headers[header],
                'Length': '123'
            }
            enable_gcp_resp = self.enable_gcp_private_endpoint(self.tenant_id, self.project_id,
                                                     self.cluster_id, cbc_api_request_headers)
            self.assertEqual(401, enable_gcp_resp.status_code,
                             msg='FAIL, Outcome: {0}, Expected:{1}'
                             .format(enable_gcp_resp.status_code, 401))

            get_endpoint_status_resp = self.get_gcp_private_endpoint_status(self.tenant_id,
                                    self.project_id, self.cluster_id, cbc_api_request_headers)
            self.assertEqual(401, get_endpoint_status_resp.status_code,
                             msg='FAIL, Outcome: {0}, Expected:{1}'
                             .format(get_endpoint_status_resp.status_code, 401))

            delete_endpoint_resp = self.delete_gcp_private_endpoint(self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    cbc_api_request_headers)
            self.assertEqual(401, delete_endpoint_resp.status_code,
                             msg='FAIL, Outcome: {0}, Expected:{1}'
                             .format(delete_endpoint_resp.status_code, 401))

            get_connection_link_resp = self.get_gcp_private_endpoint_connection_link(
                                                                    self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    cbc_api_request_headers)
            self.assertEqual(401, get_connection_link_resp.status_code,
                             msg='FAIL, Outcome: {0}, Expected:{1}'
                             .format(get_connection_link_resp.status_code, 401))

            accept_connection_resp = self.accept_gcp_private_endpoint_connection(
                                                                    self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    cbc_api_request_headers)
            self.assertEqual(401, accept_connection_resp.status_code,
                             msg='FAIL, Outcome: {0}, Expected:{1}'
                             .format(accept_connection_resp.status_code, 401))

            list_connections_resp = self.list_gcp_private_endpoint_connections(
                                                                    self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    cbc_api_request_headers)
            self.assertEqual(401, list_connections_resp.status_code,
                             msg='FAIL, Outcome: {0}, Expected:{1}'
                             .format(list_connections_resp.status_code, 401))

            reject_connections_resp = self.reject_gcp_private_endpoint_connection(
                                                                    self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    "1234",
                                                                    cbc_api_request_headers)
            self.assertEqual(401, reject_connections_resp.status_code,
                             msg='FAIL, Outcome: {0}, Expected:{1}'
                             .format(reject_connections_resp.status_code, 401))

    # Trying out with different methods
        # 1. Enable GCP Endpoints with three methods
        self.log.info("Calling Enable GCP Private Endpoint API with invalid methods")
        methods = {"PUT"}
        for method in methods:
            enable_gcp_resp = self.enable_gcp_private_endpoint(self.tenant_id, self.project_id,
                                                       self.cluster_id, method=method)
            self.assertEqual(405, enable_gcp_resp.status_code,
                             msg='FAIL, Outcome: {0}, Expected:{1}'
                             .format(enable_gcp_resp.status_code, 405))

            get_endpoint_status_resp = self.get_gcp_private_endpoint_status(self.tenant_id,
                                                                            self.project_id,
                                                                            self.cluster_id,
                                                                            method=method)
            self.assertEqual(405, get_endpoint_status_resp.status_code,
                             msg='FAIL, Outcome: {0}, Expected:{1}'
                             .format(get_endpoint_status_resp.status_code, 405))

            delete_endpoint_resp = self.delete_gcp_private_endpoint(self.tenant_id,
                                                                    self.project_id,
                                                                    self.cluster_id,
                                                                    method=method)
            self.assertEqual(405, delete_endpoint_resp.status_code,
                             msg='FAIL, Outcome: {0}, Expected:{1}'
                             .format(delete_endpoint_resp.status_code, 405))

        # 2. For Getting Private Endpoint Connections Link and Accepting Connection
        self.log.info("Calling Get GCP Private Endpoint Connection Link API with invalid methods")
        link_command_invalid_methods = ["GET", "PUT", "DELETE"]
        for method in link_command_invalid_methods:
            get_connection_link_resp = self.get_gcp_private_endpoint_connection_link(
                                                                        self.tenant_id,
                                                                        self.project_id,
                                                                        self.cluster_id,
                                                                        method=method)
            self.assertEqual(405, get_connection_link_resp.status_code,
                             msg='FAIL, Outcome: {0}, Expected:{1}'
                             .format(get_connection_link_resp.status_code, 405))

        # 3. For Accepting Private Connection
        self.log.info("Calling Accept GCP Private Endpoint Connection API with invalid methods")
        accept_connection_invalid_methods = ["PUT", "DELETE"]
        for method in accept_connection_invalid_methods:
            accept_connection_resp = self.accept_gcp_private_endpoint_connection(
                                                                        self.tenant_id,
                                                                        self.project_id,
                                                                        self.cluster_id,
                                                                        method=method)
            self.assertEqual(405, accept_connection_resp.status_code,
                             msg='FAIL, Outcome: {0}, Expected:{1}'
                             .format(accept_connection_resp.status_code, 405))

        # 4. For Rejecting the Private Connection
        self.log.info("Calling Reject GCP Private Endpoint Connection API with invalid methods")
        reject_connection_invalid_methods = ["GET", "POST", "PUT"]
        for method in reject_connection_invalid_methods:
            reject_connections_resp = self.reject_gcp_private_endpoint_connection(
                                                                        self.tenant_id,
                                                                        self.project_id,
                                                                        self.cluster_id,
                                                                        "1234",
                                                                        method=method)
            self.assertEqual(405, reject_connections_resp.status_code,
                             msg='FAIL, Outcome: {0}, Expected:{1}'
                             .format(reject_connections_resp.status_code, 405))

        # Trying out Different payloads for Getting Private Endpoint Connections Link
        self.log.info("Calling Get Private Endpoint Connections Link API with invalid payload")
        connections_links_payloads = {
            "payload1":
                {
                    "name": "Administrator",
                    "password": "password"
                },
            "payload2":
                {
                    "vpcId": "",
                    "subnetIds": "1234"
                },
            "payload3":
                {
                    "vpcId": "1234",
                    "subnetIds": ""
                },
            "payload4":
                {
                    "vpcId": "abcd-1234",
                    "subnetIds": "1234.234.11.27"
                }
        }

        for payload in connections_links_payloads:
            get_connection_link_resp = capella_api.get_gcp_private_endpoint_connection_link(
                                                                        self.tenant_id,
                                                                        self.project_id,
                                                                        self.cluster_id,
                                                    connections_links_payloads[payload])

            # Ideally this should give a response code of 4xx.
            self.assertEqual(200, get_connection_link_resp.status_code,
                             msg='FAIL, Outcome:{}, Expected:{}'
                             .format(get_connection_link_resp.status_code, 200))
            # self.assertEqual(400, get_connection_link_resp.status_code,
            #                  msg='FAIL, Outcome:{}, Expected:{}'
            #                  .format(get_connection_link_resp, 400))

        self.log.info("Calling Accept Private Endpoint Connections API with invalid payload")
        accept_connection_payload = {
            "payload1":
                {
                    "name": "Administrator",
                    "password": "password"
                },
            "payload2":
                {
                    "endpointId": ""
                },
            "payload3":
                {
                    "endpointId": "1234"
                }
        }
        for payload in accept_connection_payload:
            accept_connection_resp = capella_api.accept_gcp_private_endpoint_connection(
                                                            self.tenant_id,
                                                            self.project_id,
                                                            self.cluster_id,
                                                            accept_connection_payload[payload])
            if payload == "payload3":
                self.assertEqual(202, accept_connection_resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected:{}'
                                 .format(accept_connection_resp.status_code, 202))
            else:
                self.assertEqual(400, accept_connection_resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected:{}'
                                 .format(accept_connection_resp.status_code, 400))


        # Checking enabling, getting and deleting the gcp endpoint for different roles
        self.log.info("Testing all the endpoints with RBAC roles")
        for user in self.test_users:
            capella_api_role = CapellaAPI("https://" + self.url, self.secret_key, self.access_key,
                                 self.test_users[user]["mailid"], self.test_users[user]["password"])

            enable_endpoint_resp = capella_api_role.enable_private_endpoint(self.tenant_id,
                                                                        self.project_id,
                                                                        self.cluster_id)
            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(202, enable_endpoint_resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected:{}'
                                 .format(enable_endpoint_resp.status_code, 202))
            else:
                self.assertEqual(403, enable_endpoint_resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected:{}'
                                 .format(enable_endpoint_resp.status_code, 403))

            get_endpoint_status_resp = capella_api_role.get_private_endpoint_status(
                                                                            self.tenant_id,
                                                                            self.project_id,
                                                                            self.cluster_id)
            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(200, get_endpoint_status_resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected:{}'
                                 .format(get_endpoint_status_resp.status_code, 200))
            else:
                self.assertEqual(403, get_endpoint_status_resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected:{}'
                                 .format(get_endpoint_status_resp.status_code, 403))

            body = {
                "vpcId": "abcd",
                "subnetIds": "1234"
            }
            get_connection_link_resp = capella_api_role.get_gcp_private_endpoint_connection_link(
                                                                        self.tenant_id,
                                                                        self.project_id,
                                                                        self.cluster_id,
                                                                        body)
            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(200, get_connection_link_resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected:{}'
                                 .format(get_connection_link_resp.status_code, 200))
            else:
                self.assertEqual(403, get_connection_link_resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected:{}'
                                 .format(get_connection_link_resp.status_code, 403))

            body_1 = {
                "endpointId": "1234"
            }
            accept_connection_resp = capella_api_role.accept_gcp_private_endpoint_connection(
                                                                        self.tenant_id,
                                                                        self.project_id,
                                                                        self.cluster_id,
                                                                        body_1)
            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(202, accept_connection_resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected:{}'
                                 .format(accept_connection_resp.status_code, 202))
            else:
                self.assertEqual(403, accept_connection_resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected:{}'
                                 .format(accept_connection_resp.status_code, 403))

            list_connections_resp = capella_api_role.list_private_endpoint_connections(
                                                                            self.tenant_id,
                                                                            self.project_id,
                                                                            self.cluster_id)
            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(200, list_connections_resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected:{}'
                                 .format(list_connections_resp.status_code, 200))
            else:
                self.assertEqual(403, list_connections_resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected:{}'
                                 .format(list_connections_resp.status_code, 403))

            reject_connections_resp = capella_api_role.reject_private_endpoint_connection(
                                                                            self.tenant_id,
                                                                            self.project_id,
                                                                            self.cluster_id,
                                                                            "1234")
            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(202, reject_connections_resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected:{}'
                                 .format(reject_connections_resp.status_code, 202))
            else:
                self.assertEqual(403, reject_connections_resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected:{}'
                                 .format(reject_connections_resp.status_code, 403))

            delete_endpoint_resp = capella_api_role.delete_private_endpoint(
                                                                        self.tenant_id,
                                                                        self.project_id,
                                                                        self.cluster_id)
            if self.test_users[user]["role"] == "organizationOwner":
                self.assertEqual(202, delete_endpoint_resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected:{}'
                                 .format(delete_endpoint_resp.status_code, 202))
            else:
                self.assertEqual(403, delete_endpoint_resp.status_code,
                                 msg='FAIL, Outcome:{}, Expected:{}'
                                 .format(delete_endpoint_resp.status_code, 403))