import time
import json
from capellaAPI.CapellaAPI import CapellaAPI
from pytests.basetestcase import BaseTestCase
from couchbase_utils.capella_utils.capella_utils import CapellaUtils


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
        CapellaUtils.log.info("Cluster created with cluster ID: {}".format(cluster_id))
        return capella_api_resp

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
            resp = self.create_cluster(self.url.replace("cloud", ""), self.tenant_id, capella_api,
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
