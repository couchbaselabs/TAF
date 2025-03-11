import time
from pytests.Capella.RestAPIv4.api_base import APIBase
from couchbase_utils.capella_utils.dedicated import CapellaUtils
class ClusterBase(APIBase):

    def setUp(self):
        APIBase.setUp(self)
        resp = self.capellaAPI.org_ops_apis.create_api_key(
            organizationId=self.organisation_id,
            name=self.generate_random_string(),
            organizationRoles=["organizationOwner"],
            description=self.generate_random_string(50))
        if resp.status_code == 201:
            self.org_owner_key = resp.json()
        else:
            self.fail("Error while creating API key for organization owner")

        # update the access key and secret key for capellaAPI object,
        # so that is it being used for api auth.
        self.update_auth_with_api_token(self.curr_owner_key)

        # Create project.
        # The project ID will be used to create API keys for roles that
        # require project ID
        project_name = "TAF_api_test_project_" + self.generate_random_string(5,False)
        project_description = self.generate_random_string(100)
        self.project_id = self.capellaAPI.org_ops_apis.create_project(
            self.organisation_id, project_name,
            project_description).json()["id"]


        self.cluster_name = "TAF_api_test_cluster_" + self.generate_random_string(5,False)
        cidr=CapellaUtils.get_next_cidr() + "/20"
        self.expected_result = {
            "cursor": {
                "hrefs": {
                    "first": None,
                    "last": None,
                    "next": None,
                    "previous": None
                },
                "pages": {
                    "last": None,
                    "next": None,
                    "page": None,
                    "perPage": None,
                    "previous": None,
                    "totalItems": None
                }
            },
            "data": [
                {
                    "name": self.cluster_name,
                    "description": None,
                    "cloudProvider": {
                        "type": "aws",
                        "region": "us-east-1",
                        "cidr": "10.3.36.0/23"
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
                                    "iops": 3000,
                                    "autoExpansion": "on"
                                }
                            },
                            "numOfNodes": 3,
                            "services": [
                                "data"
                            ]
                        }
                    ],
                    "availability": {
                        "type": "single"
                    },
                    "support": {
                        "plan": "basic",
                        "timezone": "GMT"
                    },
                    "currentState": None,
                    "audit": {
                        "createdBy": None,
                        "createdAt": None,
                        "modifiedBy": None,
                        "modifiedAt": None,
                        "version": None

                    }
                }
            ]
        }
        self.cluster_id = self.capellaAPI.cluster_ops_apis.create_cluster(
            self.organisation_id, self.project_id, self.cluster_name,
            self.expected_result["data"][0]['cloudProvider'],
            self.expected_result["data"][0]['couchbaseServer'],
            self.expected_result["data"][0]['serviceGroups'],
            self.expected_result["data"][0]['availability'],
            self.expected_result["data"][0]['support']).json()['id']
        self.expected_result["data"][0]['id'] = self.cluster_id


        self.api_keys = dict()

        # Wait for the cluster to be deployed.
        self.log.info("Waiting for cluster to be deployed.")
        while self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                self.organisation_id, self.project_id,
                self.cluster_id).json()["currentState"] == "deploying":
            time.sleep(10)
        self.log.info("Cluster deployed successfully.")

    def tearDown(self):
        failures=list()
        if self.capellaAPI.cluster_ops_apis.delete_cluster(
                self.organisation_id, self.project_id,
                self.cluster_id).status_code != 202:
            failures.append("Error while deleting cluster {}".format(
                self.cluster_id))

            # Wait for the cluster to be destroyed.
        self.log.info("Waiting for cluster to be destroyed.")
        while not self.capellaAPI.cluster_ops_apis.list_clusters(
                self.organisation_id, self.project_id).status_code == 404:
            time.sleep(10)
        self.log.info("Cluster destroyed, destroying Project now.")

        # Delete the project that was created.
        if self.delete_projects(
                self.organisation_id, [self.project_id],
                self.curr_owner_key
        ):
            failures.append("Error while deleting project {}".format(
                self.project_id))

        # Delete organizationOwner API key
        self.log.info("Deleting API key for role organization Owner")
        resp = self.capellaAPI.org_ops_apis.delete_api_key(
            organizationId=self.organisation_id,
            accessKey=self.org_owner_key["Id"]
        )
        if resp.status_code != 204:
            failures.append("Error while deleting api key for role "
                            "organization Owner")

        if failures:
            self.fail("Following error occurred in teardown: {}".format(
                failures))
        super(ClusterBase, self).tearDown()