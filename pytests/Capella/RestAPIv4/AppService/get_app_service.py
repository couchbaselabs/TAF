"""
Created on February 8, 2024

@author: Vipul Bhardwaj
"""

import time
from pytests.Capella.RestAPIv4.Clusters.get_clusters import GetCluster

class GetAppService(GetCluster):

    def setUp(self, nomenclature="App_Service_Get"):
        GetCluster.setUp(self, nomenclature)
        app_svc_template = self.input.param("app_svc_template", "2v4_2node")
        self.expected_res = {
            "name": self.prefix + app_svc_template,
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
        self.expected_res.update(self.app_svc_templates[app_svc_template])

        # Wait for the APP SVC to be stable.
        self.log.info("Checking for APP SVC {} to be stable."
                      .format(self.app_service_id))
        start_time = time.time()
        while not self.validate_onoff_state(["healthy", "turnedOff"],
                                            app=self.app_service_id):
            if time.time() > 1800 + start_time:
                self.tearDown()
                self.fail("!!!...App Svc didn't deploy within 30mins...!!!")
        self.log.info("Successfully deployed App Svc.")

    def tearDown(self):
        self.update_auth_with_api_token(self.curr_owner_key)
        super(GetAppService, self).tearDown()
