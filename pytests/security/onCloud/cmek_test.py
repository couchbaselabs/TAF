import time
import json
import requests
from couchbase_utils.capella_utils.dedicated import CapellaUtils
from pytests.Capella.RestAPIv4.security_base import SecurityBase


class CMEKTest(SecurityBase):
    def setUp(self):
        SecurityBase.setUp(self)
        self.base_url = "https://" + self.url
        self.log.info("Base URL: {0}".format(self.base_url))
        self.url = self.input.capella.get("pod")
        self.user = self.input.capella.get("capella_user")
        self.passwd = self.input.capella.get("capella_pwd")
        self.tenant_id = self.input.capella.get("tenant_id")
        self.access_key = self.input.capella.get("access_key")
        self.invalid_id = "00000000-0000-0000-0000-000000000000"
        self.bearer_token_key = self.input.capella.get("bearer_token_key")
        self.cmek_base_url = "{0}/v4/organizations/{1}".format(self.base_url, self.tenant_id)
        self.log.info("CMEK Base URL: {0}".format(self.cmek_base_url))

    def tearDown(self):
        super(CMEKTest, self).tearDown()

    # GET LIST KEYS
    def get_list_cmek_keys(self):
        self.log.info("Listing CMEK keys in the tenant...")
        headers = {
            'Authorization': 'Bearer '
                             '{0}'.format(self.capellaAPI.cluster_ops_apis.bearer_token),
        }

        self.log.info("CMEK Base URL: {0}".format(self.cmek_base_url))
        # verify=False
        response = requests.get("{0}/cmek".format(self.cmek_base_url), headers=headers, verify=False)

        data = json.loads(response.content.decode())

        # Convert dictionary to JSON with indentation for pretty self.log.infoing
        pretty_json = json.dumps(data, indent=4)

        # self.log.info the pretty JSON
        self.log.info(pretty_json)

        cmek_id_list = []
        for ids in data["data"]:
            cmek_id = ids["id"]
            cmek_id_list.append(cmek_id)

        return cmek_id_list

    # POST CREATE A KEY
    def post_create_a_key(self, arn, resourceName):
        self.log.info("Creating a AWS key...")
        self.log.info("ARN: {0}".format(arn))
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer '
                             '{0}'.format(self.capellaAPI.cluster_ops_apis.bearer_token),
        }

        json_data = {
            'name': 'test_key_cmek_AWS',
            'description': 'AWS',
            'config': {
                'arn': arn,
            },
        }

        response = requests.post("{0}/cmek".format(self.cmek_base_url),
                                 headers=headers,
                                 json=json_data,
                                 )

        data = json.loads(response.content.decode())

        # Convert dictionary to JSON with indentation for pretty self.log.infoing
        pretty_json = json.dumps(data, indent=4)

        # self.log.info the pretty JSON
        self.log.info(pretty_json)
        aws_key_id = data["id"]

        self.log.info("Creating a GCP key...")
        self.log.info("resourceName: {0}".format(resourceName))
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer '
                             '{0}'.format(self.capellaAPI.cluster_ops_apis.bearer_token),
        }

        json_data = {
            'name': 'test_key_cmek_GCP',
            'description': 'GCP',
            'config': {
                'resourceName': resourceName,
            },
        }

        # response = requests.post("{0}/cmek".format(self.cmek_base_url),
        #                          headers=headers,
        #                          json=json_data,
        #                          )
        # self.log.info("Response Status Code of creating a key: {0}".format(response.status_code))
        # self.log.info("Response Status Content of creating a key: {0}".format(response.content))
        #
        # data = json.loads(response.content.decode())
        #
        # # Convert dictionary to JSON with indentation for pretty self.log.infoing
        # pretty_json = json.dumps(data, indent=4)
        #
        # # self.log.info the pretty JSON
        # self.log.info(pretty_json)
        gcp_key_id = data["id"]
        gcp_key_id = ""

        return aws_key_id, gcp_key_id

    # GET GET KEY DETAIL
    def get_get_key_detail(self, aws_key_id, gcp_key_id):
        headers = {
            'Authorization': 'Bearer '
                             '{0}'.format(self.capellaAPI.cluster_ops_apis.bearer_token),
        }

        self.log.info("AWS key id detail: ")
        response = requests.get("{0}/cmek/{1}".format(self.cmek_base_url, aws_key_id), headers=headers,
                                verify=False)

        data = json.loads(response.content.decode())

        # Convert dictionary to JSON with indentation for pretty self.log.infoing
        pretty_json = json.dumps(data, indent=4)

        # self.log.info the pretty JSON
        self.log.info(pretty_json)

        self.log.info("GCP key id detail: ")
        response = requests.get("{0}/cmek/{1}".format(self.cmek_base_url, gcp_key_id), headers=headers,
                                verify=False)

        data = json.loads(response.content.decode())

        # Convert dictionary to JSON with indentation for pretty self.log.infoing
        pretty_json = json.dumps(data, indent=4)

        # self.log.info the pretty JSON
        self.log.info(pretty_json)

    # PUT UPDATE KEY
    def put_update_key(self, cmek_key_id, arn):
        self.log.info("Rotating key...")
        self.log.info("New key: {0}".format(arn))
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer '
                             '{0}'.format(self.capellaAPI.cluster_ops_apis.bearer_token),
        }

        json_data = {
            'config': {
                'arn': arn,
            },
        }
        response = requests.put("{0}/cmek/{1}".format(self.cmek_base_url, cmek_key_id),
                                headers=headers,
                                json=json_data,
                                )
        return response

    # DEL DELETE KEY
    def del_delete_key(self):
        cmek_id_list = self.get_list_cmek_keys()
        self.log.info("Deleting keys...")
        headers = {
            'Authorization': 'Bearer '
                             '{0}'.format(self.capellaAPI.cluster_ops_apis.bearer_token),
        }

        for cmek_key_id in cmek_id_list:
            self.log.info("Deleting key with id: {0}".format(cmek_key_id))
            response = requests.delete("{0}/cmek/{1}".format(self.cmek_base_url, cmek_key_id), headers=headers)
            self.log.info(response.status_code)
            self.log.info(response.content)

    def post_deploy_cluster_aws(self, aws_key_id):
        self.log.info("Deploying AWS cluster")
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer '
                             '{0}'.format(self.capellaAPI.cluster_ops_apis.bearer_token),
        }

        json_data = {
            'name': '0000000-Test-cmek-AWS',
            'description': 'Deploy CMEK cluster',
            'cloudProvider': {
                'type': 'aws',
                'region': 'us-east-1',
                'cidr': '10.1.14.0/23',
            },
            'couchbaseServer': {
                'version': '7.2',
            },
            'serviceGroups': [
                {
                    'node': {
                        'compute': {
                            'cpu': 4,
                            'ram': 16,
                        },
                        'disk': {
                            'storage': 50,
                            'type': 'gp3',
                            'iops': 3000,
                        },
                    },
                    'numOfNodes': 3,
                    'services': [
                        'data',
                        'query',
                        'index',
                        'search',
                    ],
                },
                {
                    'node': {
                        'compute': {
                            'cpu': 4,
                            'ram': 32,
                        },
                        'disk': {
                            'storage': 50,
                            'type': 'io2',
                            'iops': 3005,
                        },
                    },
                    'numOfNodes': 2,
                    'services': [
                        'analytics',
                    ],
                },
            ],
            'availability': {
                'type': 'multi',
            },
            'support': {
                'plan': 'enterprise',
                'timezone': 'PT',
            },
            'cmekId': aws_key_id,
        }
        self.log.info("{0}/projects/{1}/clusters',".format(self.cmek_base_url, self.project_id))
        end_time = time.time() + 1800
        response = aws_cluster_id = ""
        while time.time() < end_time:
            subnet = CapellaUtils.get_next_cidr() + "/20"
            self.log.info("Trying with cidr: {}".format(subnet))
            json_data["cloudProvider"]["cidr"] = subnet
            response = requests.post("{0}/projects/{1}/clusters".format(self.cmek_base_url, self.project_id),
                                     headers=headers,
                                     json=json_data,
                                     )
            if response.status_code == 202:
                aws_cluster_id = json.loads(response.content).get("id")
                self.log.info("Creating capella cluster with id: {0}".format(aws_cluster_id))
                break
        self.log.info(response.content)
        data = json.loads(response.content.decode())

        # Convert dictionary to JSON with indentation for pretty self.log.infoing
        pretty_json = json.dumps(data, indent=4)

        # self.log.info the pretty JSON
        self.log.info(pretty_json)
        return aws_cluster_id

    def post_deploy_cluster_gcp(self, gcp_key_id):
        self.log.info("Deploying GCP cluster")
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer '
                             '{0}'.format(self.capellaAPI.cluster_ops_apis.bearer_token),
        }

        json_data = {
            "name": "0000000-Test-cmek-GCP",
            "description": "My first test GCP cluster.",
            "cloudProvider": {
                "type": "gcp",
                "region": "asia-south2",
                "cidr": "10.1.16.0/23"
            },
            "couchbaseServer": {
                "version": "7.2"
            },
            "serviceGroups": [
                {
                    "node": {
                        "compute": {
                            "cpu": 4,
                            "ram": 16
                        },
                        "disk": {
                            "storage": 64,
                            "type": "pd-ssd"
                        }
                    },
                    "numOfNodes": 3,
                    "services": [
                        "data",
                        "query",
                        "index",
                        "search"
                    ]
                }
            ],
            "availability": {
                "type": "single"
            },
            "support": {
                "plan": "enterprise",
                "timezone": "ET"
            },
            'cmekId': gcp_key_id,
        }
        self.log.info("{0}/projects/{1}/clusters',".format(self.cmek_base_url, self.project_id))
        end_time = time.time() + 1800
        response = gcp_cluster_id = ""
        while time.time() < end_time:
            subnet = CapellaUtils.get_next_cidr() + "/20"
            self.log.info("Trying with cidr: {}".format(subnet))
            json_data["cloudProvider"]["cidr"] = subnet
            response = requests.post("{0}/projects/{1}/clusters".format(self.cmek_base_url, self.project_id),
                                     headers=headers,
                                     json=json_data,
                                     )
            if response.status_code == 202:
                gcp_cluster_id = json.loads(response.content).get("id")
                self.log.info("Creating capella cluster with id: {0}".format(gcp_cluster_id))
                break
        self.log.info(response.content)
        data = json.loads(response.content.decode())

        # Convert dictionary to JSON with indentation for pretty self.log.infoing
        pretty_json = json.dumps(data, indent=4)

        # self.log.info the pretty JSON
        self.log.info(pretty_json)
        return gcp_cluster_id

    # POST UNASSOCIATE A KEY
    def post_unassociate_a_key(self, cmek_id, cluster_id):
        self.log.info("Unssociating a key to the cluster...")
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer '
                             '{0}'.format(self.capellaAPI.cluster_ops_apis.bearer_token),
        }

        response = requests.post(
            "{0}/projects/{1}/clusters/{2}/cmek/{3}/unassociate".format(self.cmek_base_url, self.project_id,
                                                                        cluster_id, cmek_id),
            headers=headers
            )
        self.log.info(response.status_code)
        return response

    # POST ASSOCIATE A KEY
    def post_associate_a_key(self, cmek_id, cluster_id):
        self.log.info("Associating a key to the cluster...")
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer '
                             '{0}'.format(self.capellaAPI.cluster_ops_apis.bearer_token),
        }

        response = requests.post(
            "{0}/projects/{1}/clusters/{2}/cmek/{3}/associate".format(self.cmek_base_url, self.project_id,
                                                                      cluster_id, cmek_id),
            headers=headers
            )
        self.log.info(response.status_code)
        return response

    # GET GET IAM ROLE
    def get_get_iam_role(self):
        self.log.info("Fetching cloud account details...")
        headers = {
            'Authorization': 'Bearer '
                             '{0}'.format(self.capellaAPI.cluster_ops_apis.bearer_token),
        }

        response = requests.get("{0}/cloudAccounts".format(self.cmek_base_url), headers=headers, verify=False)

        data = json.loads(response.content.decode())

        # Convert dictionary to JSON with indentation for pretty self.log.infoing
        pretty_json = json.dumps(data, indent=4)

        # self.log.info the pretty JSON
        self.log.info(pretty_json)

        return data["aws-capella-account"], data["gcp-capella-project"]

    # #### TEST CASES #### #

    # #### TEST CASE 1 #### #

    def test_cmek(self):
        """
        Test e2e
        1) Get IAM roles
            i) AWS
            ii) GCP
        2) Create Key and get arn and resourceName respectively
            i) AWS -> arn
                a) https://us-east-1.console.aws.amazon.com/kms/home?region=us-east-1#/kms/keys/0e055dc1-f585-4754-878b-a18df3fefef8
            ii) GCP -> resourceName
                a) projects/cbc-capella-test/locations/global/keyRings/shaazin_test_cmek/cryptoKeys/shaazin_test_cmek_key2
        3) Create key
            i) AWS
            ii) GCP
        """
        self.log.info("---Test CMEK start1---")
        self.log.info("Details:")
        self.log.info("URL: {0}".format(self.url))
        self.log.info("Tenant id: {0}".format(self.tenant_id))
        self.log.info("Project id: {0}".format(self.project_id))
        self.log.info("Cluster id: {0}".format(self.cluster_id))
        self.log.info("Token: {0}".format(self.capellaAPI.cluster_ops_apis.bearer_token))

        self.del_delete_key()
        cmek_id_list = self.get_list_cmek_keys()
        self.log.info(cmek_id_list)

        # 1
        aws_account, gcp_account = self.get_get_iam_role()
        self.log.info("aws-capella-account: {0}".format(aws_account))
        self.log.info("gcp-capella-project: {0}".format(gcp_account))

        # 2
        arn = "arn:aws:kms:us-east-1:264138468394:key/0e055dc1-f585-4754-878b-a18df3fefef8"
        resourceName = "projects/cbc-capella-test/locations/global/keyRings/cmek-ga/cryptoKeys/cmek-ga-key"

        # 3
        self.get_list_cmek_keys()
        aws_key_id, gcp_key_id = self.post_create_a_key(arn, resourceName)
        self.log.info("aws_key_id: ".format(aws_key_id))
        self.log.info("gcp_key_id: ".format(gcp_key_id))

        # 4
        self.get_get_key_detail(aws_key_id, gcp_key_id)

        # 5
        aws_cluster_id = self.post_deploy_cluster_aws(aws_key_id)
        # gcp_cluster_id = self.post_deploy_cluster_gcp(gcp_key_id)

        # 6
        self.del_delete_key()

        # 7
        # Delete clusters
        self.log.info("Deleting cluster with id: {}".format(aws_cluster_id))
        resp = self.capellaAPI.cluster_ops_apis.delete_cluster(self.tenant_id,
                                                               self.project_id,
                                                               aws_cluster_id)
        self.log.info(resp)

        # self.log.info("Deleting cluster with id: {}".format(gcp_cluster_id))
        # resp = self.capellaAPI.cluster_ops_apis.delete_cluster(self.tenant_id,
        #                                                        self.project_id,
        #                                                        gcp_cluster_id)
        # self.log.info(resp)

        # 8
        self.del_delete_key()

        self.log.info("---Test CMEK end1---")

    # #### TEST CASE 2 #### #

    def test_cmek_asst_unasst(self):
        """
        Associate and unassociate a key to the cluster
        """
        self.log.info("---Test CMEK start2---")
        self.log.info("Details:")
        self.log.info("URL: {0}".format(self.url))
        self.log.info("Tenant id: {0}".format(self.tenant_id))
        self.log.info("Project id: {0}".format(self.project_id))
        self.log.info("Cluster id: {0}".format(self.cluster_id))
        self.log.info("Token: {0}".format(self.capellaAPI.cluster_ops_apis.bearer_token))

        # check flag to ignore
        self.del_delete_key()
        cmek_id_list = self.get_list_cmek_keys()
        self.log.info(cmek_id_list)

        # 1
        aws_account, gcp_account = self.get_get_iam_role()
        self.log.info("aws-capella-account: {0}".format(aws_account))
        self.log.info("gcp-capella-project: {0}".format(gcp_account))

        # 2
        arn = "arn:aws:kms:us-east-1:264138468394:key/0e055dc1-f585-4754-878b-a18df3fefef8"
        resourceName = "projects/cbc-capella-test/locations/global/keyRings/cmek-ga/cryptoKeys/cmek-ga-key"

        # 3
        self.get_list_cmek_keys()
        aws_key_id, gcp_key_id = self.post_create_a_key(arn, resourceName)
        self.log.info("aws_key_id: ".format(aws_key_id))
        self.log.info("gcp_key_id: ".format(gcp_key_id))

        # 4
        self.get_get_key_detail(aws_key_id, gcp_key_id)

        # 5
        aws_response = self.post_associate_a_key(aws_key_id, self.cluster_id)
        if aws_response.status_code == 422:
            self.log.info("This is as expected as CMEK is only supported on the Enterprise plan")
        else:
            self.fail("CMEK should only be supported on the Enterprise plan")

        aws_cluster_id = self.post_deploy_cluster_aws(aws_key_id)

        aws_response = self.post_unassociate_a_key(aws_key_id, aws_cluster_id)
        self.log.info(aws_response)
        aws_response = self.post_associate_a_key(aws_key_id, aws_cluster_id)
        self.log.info(aws_response)

        # 6
        self.del_delete_key()

        # 7
        # Delete clusters
        self.log.info("Deleting cluster with id: {}".format(aws_cluster_id))
        resp = self.capellaAPI.cluster_ops_apis.delete_cluster(self.tenant_id,
                                                               self.project_id,
                                                               aws_cluster_id)
        self.log.info(resp)

        # 8
        self.del_delete_key()

        self.log.info("---Test CMEK end2---")

    # #### TEST CASE 3 #### #

    def test_cmek_rbac(self):
        """
        Verify access control
        """
        self.log.info("---Test CMEK start3---")
        self.log.info("Details:")
        self.log.info("URL: {0}".format(self.url))
        self.log.info("Tenant id: {0}".format(self.tenant_id))
        self.log.info("Project id: {0}".format(self.project_id))
        self.log.info("Cluster id: {0}".format(self.cluster_id))
        self.log.info("Token: {0}".format(self.capellaAPI.cluster_ops_apis.bearer_token))

        self.del_delete_key()
        cmek_id_list = self.get_list_cmek_keys()
        self.log.info(cmek_id_list)

        # 1
        aws_account, gcp_account = self.get_get_iam_role()
        self.log.info("aws-capella-account: {0}".format(aws_account))
        self.log.info("gcp-capella-project: {0}".format(gcp_account))

        # 2
        arn = "arn:aws:kms:us-east-1:264138468394:key/0e055dc1-f585-4754-878b-a18df3fefef8"

        # 3
        self.log.info("Create key as an Org Member")
        self.log.info("Creating a AWS key...")
        self.log.info("ARN: {0}".format(arn))
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer '
                             '{0}'.format(self.test_users["User3"]["token"]),
        }

        json_data = {
            'name': 'test_key_cmek_AWS',
            'description': 'AWS',
            'config': {
                'arn': arn,
            },
        }

        response = requests.post("{0}/cmek".format(self.cmek_base_url),
                                 headers=headers,
                                 json=json_data,
                                 )

        if response.status_code != 403:
            self.fail("User should not have permissions to create akey")

        # 4
        self.del_delete_key()

        self.log.info("---Test CMEK end3---")

    # #### TEST CASE 4 #### #

    def test_cmek_update_key(self):
        self.log.info("---Test CMEK start4---")
        self.log.info("Details:")
        self.log.info("URL: {0}".format(self.url))
        self.log.info("Tenant id: {0}".format(self.tenant_id))
        self.log.info("Project id: {0}".format(self.project_id))
        self.log.info("Cluster id: {0}".format(self.cluster_id))
        self.log.info("Token: {0}".format(self.capellaAPI.cluster_ops_apis.bearer_token))

        self.del_delete_key()
        cmek_id_list = self.get_list_cmek_keys()
        self.log.info(cmek_id_list)

        # 1
        aws_account, gcp_account = self.get_get_iam_role()
        self.log.info("aws-capella-account: {0}".format(aws_account))
        self.log.info("gcp-capella-project: {0}".format(gcp_account))

        # 2
        arn = "arn:aws:kms:us-east-1:264138468394:key/0e055dc1-f585-4754-878b-a18df3fefef8"
        resourceName = "projects/cbc-capella-test/locations/global/keyRings/cmek-ga/cryptoKeys/cmek-ga-key"

        # 3
        self.get_list_cmek_keys()
        aws_key_id, gcp_key_id = self.post_create_a_key(arn, resourceName)
        self.log.info("aws_key_id: ".format(aws_key_id))
        self.log.info("gcp_key_id: ".format(gcp_key_id))

        # 4
        self.get_get_key_detail(aws_key_id, gcp_key_id)

        # 5
        aws_cluster_id = self.post_deploy_cluster_aws(aws_key_id)
        new_resourceName = "arn:aws:kms:us-east-1:264138468394:key/0e055dc1-f585-4754-878b-a18df3fefef8"
        resp = self.put_update_key(aws_cluster_id, new_resourceName)
        if resp.status_code != 204:
            self.fail("Key rotation failed")
        resp = self.put_update_key(aws_cluster_id, new_resourceName)
        if resp.status_code != 422:
            self.fail("Key rotation should have failed as its less than 30 days")

        # 6
        self.del_delete_key()

        # 7
        # Delete clusters

        self.log.info("Deleting cluster with id: {}".format(aws_cluster_id))
        resp = self.capellaAPI.cluster_ops_apis.delete_cluster(self.tenant_id,
                                                               self.project_id,
                                                               aws_cluster_id)
        self.log.info(resp)

        # 8
        self.del_delete_key()

        self.log.info("---Test CMEK end4---")

    # #### TEST CASE 6 #### #

    def test_cmek_key_perms(self):
        """
        Verify key permissions
        """
        self.log.info("---Test CMEK start6---")
        self.log.info("Details:")
        self.log.info("URL: {0}".format(self.url))
        self.log.info("Tenant id: {0}".format(self.tenant_id))
        self.log.info("Project id: {0}".format(self.project_id))
        self.log.info("Cluster id: {0}".format(self.cluster_id))
        self.log.info("Token: {0}".format(self.capellaAPI.cluster_ops_apis.bearer_token))

        self.del_delete_key()
        cmek_id_list = self.get_list_cmek_keys()
        self.log.info(cmek_id_list)

        # 1
        aws_account, gcp_account = self.get_get_iam_role()
        self.log.info("aws-capella-account: {0}".format(aws_account))
        self.log.info("gcp-capella-project: {0}".format(gcp_account))

        # 2
        # key = does not have sufficient permissions
        resourceName = "projects/cbc-capella-test/locations/global/keyRings/shaazin_test_cmek/cryptoKeys/shaazin_test_cmek_key"
        # key = unavailable
        arn = "arn:aws:kms:us-east-1:264138468394:key/8e055dc1-f585-4754-878b-a18df3fefef0"

        # 3
        self.get_list_cmek_keys()
        try:
            aws_key_id, gcp_key_id = self.post_create_a_key(arn, resourceName)
            self.log.info("aws_key_id: ".format(aws_key_id))
            self.log.info("gcp_key_id: ".format(gcp_key_id))
        except Exception:
            self.log.info("Failed as expected")
        else:
            self.fail("Key creation should have failed")

        # 5
        self.del_delete_key()

        self.log.info("---Test CMEK end5---")

    # #### TEST CASE 7 #### #

    def test_cmek_key_regions(self):
        """
        Verify key regions
        """
        self.log.info("---Test CMEK start7---")
        self.log.info("Details:")
        self.log.info("URL: {0}".format(self.url))
        self.log.info("Tenant id: {0}".format(self.tenant_id))
        self.log.info("Project id: {0}".format(self.project_id))
        self.log.info("Cluster id: {0}".format(self.cluster_id))
        self.log.info("Token: {0}".format(self.capellaAPI.cluster_ops_apis.bearer_token))

        self.del_delete_key()
        cmek_id_list = self.get_list_cmek_keys()
        self.log.info(cmek_id_list)

        # 1
        aws_account, gcp_account = self.get_get_iam_role()
        self.log.info("aws-capella-account: {0}".format(aws_account))
        self.log.info("gcp-capella-project: {0}".format(gcp_account))

        # 2
        arn = "arn:aws:kms:us-east-1:264138468394:key/0e055dc1-f585-4754-878b-a18df3fefef8"
        resourceName = "projects/cbc-capella-test/locations/nam3/keyRings/diff-region/cryptoKeys/test-reg"

        # 3
        self.get_list_cmek_keys()
        aws_key_id, gcp_key_id = self.post_create_a_key(arn, resourceName)
        self.log.info("aws_key_id: ".format(aws_key_id))
        self.log.info("gcp_key_id: ".format(gcp_key_id))

        # 4
        self.get_get_key_detail(aws_key_id, gcp_key_id)

        # 5
        try:
            self.post_deploy_cluster_gcp(gcp_key_id)
        except Exception as e:
            self.log.info("Ran into an Exception: {0}".format(e))
            self.log.info("Failed as expected as different region")
        else:
            self.fail("Should have failed for different region")

        # 6
        self.del_delete_key()

        self.log.info("---Test CMEK end6---")
