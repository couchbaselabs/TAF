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

        self.capellaAPI_v2 = CapellaAPI(
            "https://" + self.url, "", "", self.user, self.passwd)

        # If an app endpoint already exists, don't bother creating another one
        self.log.debug("...Checking if an App Endpoint already exists...")
        res = self.capellaAPI_v2.get_sgw_databases(
            self.organisation_id, self.project_id, self.cluster_id,
            self.app_service_id)
        self.log.debug("Res: {}".format(res.content))
        try:
            retry = 1
            while res.json()["errorType"] == "AppServiceConnectionRefused" \
                    and retry < 6:
                self.log.warning(res.json()["message"])
                retry += 1
                self.log.debug("...Waiting for 5 seconds before retrying...")
                time.sleep(5)
                res = self.capellaAPI_v2.get_sgw_databases(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.app_service_id)
            if res.json()["errorType"] == "EntityStateInvalid":
                self.log.warning("App service is off, so skipping the App "
                                 "Endpoint gibberish")
                return
        except Exception as e:
            self.log.warning("Exception: {}".format(e))
            self.log.error("Exception encountered in conversion of res to "
                           "JSON / or fetching the key `errorType` from "
                           "JSONified res.")
        if res.status_code != 200:
            self.log.error("Error: {}".format(res.content))
            self.tearDown()
            self.fail("!!!...Listing App Endpoints Failed...!!!")
        appEndpoints = res.json()["data"]
        if len(appEndpoints):
            self.appEndpointName = appEndpoints[0]["data"]["name"]
            self.log.info("The App Endpoint: {}, is already present inside "
                          "the App Service.".format(self.appEndpointName))
            return

        # Create a bucket for the App endpoint to reside in
        self.log.debug("...Creating a bucket for the App Endpoint to be "
                       "linked to...")
        res = self.capellaAPI.cluster_ops_apis.create_bucket(
            self.organisation_id, self.project_id, self.cluster_id,
            "bucketForAppEndpoint", "couchbase", "magma", 1024, "seqno",
            "none", 1, False, 0)
        if res.status_code == 429:
            self.handle_rate_limit(res.headers["Retry-After"])
            res = self.capellaAPI.cluster_ops_apis.create_bucket(
                self.organisation_id, self.project_id, self.cluster_id,
                "bucketForAppEndpoint", "couchbase", "magma", 1024, "seqno",
                "none", 1, False, 0)
        if res.status_code != 201:
            try:
                if res.json()["code"] == 6001 and res.json()["hint"] == \
                        ("The bucket name provided already exists. Please "
                         "choose a different name for the bucket."):
                    self.log.warning("...Bucket already exists...")
            except (Exception,):
                self.log.error("Error : {}".format(res.content))
                self.tearDown()
                self.fail("!!!..Bucket creation failed...!!!")
        self.app_endpoint_bucket_id = res.json()['id']
        self.app_endpoint_bucket_name = "bucketForAppEndpoint"

        # Allow my IP for this App Service.
        self.log.debug("Adding current IP in the allow list for the APP SVC")
        res = self.capellaAPI_v2.allow_my_ip_sgw(
            self.organisation_id, self.project_id, self.cluster_id,
            self.app_service_id)
        if res.status_code != 200:
            self.log.error(res.content)
            self.tearDown()
            self.fail("!!!...Failed to allow my IP...!!!")

        # Create an App Endpoint.
        self.log.debug("...Creating a App Endpoint inside the App Service: "
                       "{}...".format(self.app_service_id))
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
        self.appEndpointName = "test_vipul"

        # Resume App Endpoint
        self.log.debug("...Starting the App Endpoint: {}..."
                       .format(self.appEndpointName))
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
