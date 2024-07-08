"""
Created on February 8, 2024

@author: Vipul Bhardwaj
"""

from pytests.Capella.RestAPIv4.Clusters.get_clusters import GetCluster

class GetAppService(GetCluster):

    def setUp(self, nomenclature="App_Service_Get"):
        GetCluster.setUp(self, nomenclature)
        self.expected_res = {
            "name": self.prefix + "WRAPPER",
            "description": "App service made by the v4 APIs Automation script",
            "clusterId": self.cluster_id,
            "currentState": None,
            "version": None,
            "audit": {
                "createdBy": None,
                "createdAt": None,
                "modifiedBy": None,
                "modifiedAt": None,
                "version": None
            }
        }
        self.expected_res.update(self.app_svc_templates[self.input.param(
            "app_svc_template", "AWS_2v4_2node")])

        # Wait for the deployment request in GetCluster to complete.
        self.log.info("Waiting for AppService {} to be deployed."
                      .format(self.app_service_id))
        if not self.wait_for_deployment(self.cluster_id, self.app_service_id):
            self.tearDown()
            self.fail("!!!...App Svc deployment failed...!!!")
        self.log.info("Successfully deployed App Svc.")

    def tearDown(self):
        self.update_auth_with_api_token(self.org_owner_key["token"])
        super(GetAppService, self).tearDown()
