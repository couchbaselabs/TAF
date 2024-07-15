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

    def tearDown(self):
        self.update_auth_with_api_token(self.curr_owner_key)
        super(GetAppService, self).tearDown()
