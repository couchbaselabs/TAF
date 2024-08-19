"""
Created on February 8, 2024

@author: Vipul Bhardwaj
"""

import time
from pytests.Capella.RestAPIv4.Clusters.get_clusters import GetCluster
from capellaAPI.capella.dedicated.CapellaAPI import CapellaAPI

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

        # Create a bucket for the App endpoint to reside in
        res = self.capellaAPI.cluster_ops_apis.create_bucket(
            self.organisation_id, self.project_id, self.cluster_id,
            "bucketForAppEndpoint", "magma", 1024, "seqno", "none", 1, True)
        if res.status_code == 429:
            self.handle_rate_limit(res.headers["Retry-After"])
            res = self.capellaAPI.cluster_ops_apis.create_bucket(
                self.organisation_id, self.project_id, self.cluster_id,
                "bucketForAppEndpoint", "magma", 1024, "seqno", "none", 1,
                True)
        if res.status_code != 201:
            self.log.error("Error : {}".format(res.content))
            self.tearDown()
            self.fail("!!!..Bucket creation failed...!!!")
        self.app_endpoint_bucket_id = res.json()['id']
        self.app_endpoint_bucket_name = "bucketForAppEndpoint"
        # self.buckets.append(self.bucket_id)

        # Create an App Endpoint
        self.capellaAPI_v2 = CapellaAPI(
            "https://" + self.url, "", "", self.user, self.passwd)
        self.appEndpointName = "test_vipul"
        res = self.capellaAPI_v2.create_sgw_database(
            self.organisation_id, self.project_id, self.cluster_id,
            self.app_service_id, {
                "name": "test_vipul",
                "bucket": "bucketForAppEndpoint",
                "delta_sync": False,
                "scopes": {
                    "_default": {
                        "collections": {
                            "_default": {
                                "sync": "",
                                "import_filter": ""
                            }
                        }
                    }
                }
            }
        )
        if res.status_code != 200:
            self.log.error("Error: {}".format(res.content))
            self.tearDown()
            self.fail("!!!...App Endpoint creation Failed...!!!")
        self.log.info("App Endpoint created successfully.")

        # Resume App Endpoint
        res = self.capellaAPI_v2.resume_sgw_database(
            self.organisation_id, self.project_id, self.cluster_id,
            self.app_service_id, self.appEndpointName)
        if res.status_code != 200:
            self.log.error("Error: {}".format(res.content))
            self.tearDown()
            self.fail("!!!...App Endpoint resume resume request failed...!!!")
        self.log.info("Successfully resumed App Endpoint.")

    def tearDown(self):
        self.update_auth_with_api_token(self.curr_owner_key)
        super(GetAppService, self).tearDown()
