"""
Created on February 8, 2024

@author: Vipul Bhardwaj
"""

from pytests.Capella.RestAPIv4.Clusters.get_clusters import GetCluster

class GetAppService(GetCluster):

    def setUp(self, nomenclature="App_Service_Get"):
        GetCluster.setUp(self, nomenclature, ["index", "query"])

        self.expected_result = {
            "name": self.prefix + nomenclature,
            "description": "Description of the App Service.",
            "cloudProvider": "aws",
            "nodes": 2,
            "compute": {
                "cpu": 2,
                "ram": 4
            },
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

        # Create app service
        self.log.info("Creating App Service...")
        res = self.capellaAPI.cluster_ops_apis.create_appservice(
            self.organisation_id, self.project_id, self.cluster_id,
            self.expected_result["name"], self.expected_result["compute"])
        if res.status_code != 201:
            self.log.error("Error while deploying the app service: {}"
                           .format(res.content))
            self.tearDown()
            self.fail("!!!..AppService creation failed...!!!")

        self.app_service_id = res.json()["id"]
        self.expected_result["id"] = self.app_service_id
        self.log.info("Waiting for appservice {} to be deployed."
                      .format(self.app_service_id))
        if not self.wait_for_deployment(self.project_id, self.cluster_id,
                                        self.app_service_id):
            self.tearDown()
            self.fail("!!!..AppService deployment failed...!!!")
        self.log.info("Successfully deployed app service.")

    def tearDown(self):
        self.update_auth_with_api_token(self.org_owner_key["token"])

        # Wait for app_service to be turned off.
        self.log.info("Waiting for AppService to be in a stable state.")
        while not self.validate_onoff_state(["healthy", "turnedOff"],
                                            self.project_id, self.cluster_id,
                                            self.app_service_id):
            self.log.info("...Waiting further...")

        # Delete App Service
        self.log.info("Deleting App Service...")
        res = self.capellaAPI.cluster_ops_apis.delete_appservice(
            self.organisation_id, self.project_id, self.cluster_id,
            self.app_service_id)
        if res.status_code != 202:
            self.fail("Error while deleting the app service: {}"
                      .format(res.content))

        self.log.info("Waiting for app service to be deleted...")
        self.verify_app_services_empty(self.project_id)
        super(GetAppService, self).tearDown()
