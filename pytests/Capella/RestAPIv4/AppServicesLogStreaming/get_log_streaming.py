from pytests.Capella.RestAPIv4.AppService.get_app_service import GetAppService

class GetLogStreaming(GetAppService):
    
    def setUp(self, nomenclature="App_Service_Log_Streaming_GET"):
        GetAppService.setUp(self, nomenclature)
        self.expected_res = {
            # outputType can be one of: datadog, generic_http, sumologic, loki, elastic, splunk, dynatrace
            # credentials will vary based on outputType
            # datadog: {"apiKey": "<api_key>", "url": "<url>"}
            # generic_http: {"url": "<url>", "user": "<user>", "password": "<password>"}
            # sumologic: {"url": "<url>"}
            # loki : {"url": "<url>", "user": "<user>", "password": "<password>"}
            # elastic: {"url": "<url>", "user": "<user>", "password": "<password>"}
            # splunk: {"url": "<url>", "splunkToken": "<splunk_token>"}
            # dynatrace: {"url": "<url>", "apiToken": "<api_token>"}
            "outputType": "datadog",
            "credentials": {
                "apiKey": "your_datadog_api_key_here",
                "url": "datadog_api_endpoint_here"
            }
            
        }
        self.log.info("Creating Log Streaming for App Service ID: {}".format(self.app_service_id))
        result = self.capellaAPI.cluster_ops_apis.create_app_service_log_streaming(
            self.organisation_id, self.project_id, self.cluster_id, self.app_service_id,
            self.expected_res["outputType"], self.expected_res["credentials"])
        if result.status_code == 429:
            self.handle_rate_limit(int(result.headers["Retry-After"]))
            result = self.capellaAPI.cluster_ops_apis.create_app_service_log_streaming(
                self.organisation_id, self.project_id, self.cluster_id,
                self.app_service_id, self.expected_res["outputType"],
                self.expected_res["credentials"])
        if result.status_code != 202:
            self.log.error("Result: {}".format(result.content))
            self.tearDown()
            self.fail("Error while creating App Service Log Streaming.")
        self.log.info("App Service Log Streaming created successfully.")

    def tearDown(self):
        self.update_auth_with_api_token(self.org_owner_key["token"])
        super(GetLogStreaming, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Get App Service Log Streaming",
                "expected_status_code": 200
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}/appservices/{}/logStreaming",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }, {
                "description": "Replace the last path param name in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/appservices/{}/logStreamings",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/appservices/{}/logStreaming/invalidSegment",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Call API with non-hex organizationID",
                "invalid_organizationID": self.replace_last_character(
                    self.organisation_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if you have provided a valid URL and all "
                            "the required params are present in the request "
                            "body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived to "
                               "be a client error."
                }
            }, {
                "description": "Call API with non-hex projectID",
                "invalid_projectID": self.replace_last_character(
                    self.project_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if you have provided a valid URL and all "
                            "the required params are present in the request "
                            "body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived to "
                               "be a client error."
                }
            }, {
                "description": "Send call with with non-hex clusterID",
                "invalid_clusterID": self.replace_last_character(
                    self.cluster_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if you have provided a valid URL and all "
                            "the required params are present in the request "
                            "body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived to "
                               "be a client error."
                }
            }, {
                "description": "Call API with non-hex appServiceId",
                "invalid_appServiceId": self.replace_last_character(
                    self.app_service_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if you have provided a valid URL and all "
                            "the required params are present in the request "
                            "body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived to "
                               "be a client error."
                }
            }
        ]

        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            organization = self.organisation_id
            project = self.project_id
            cluster = self.cluster_id
            appService = self.app_service_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.app_svc_log_streaming_endpoint = \
                    testcase["url"]
            if "invalid_organizationID" in testcase:
                organization = testcase["invalid_organizationID"]
            elif "invalid_projectID" in testcase:
                project = testcase["invalid_projectID"]
            elif "invalid_clusterID" in testcase:
                cluster = testcase["invalid_clusterID"]
            elif "invalid_appServiceId" in testcase:
                appService = testcase["invalid_appServiceId"]

            result = self.capellaAPI.cluster_ops_apis.get_app_service_log_streaming(
                organization, project, cluster, appService)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.get_app_service_log_streaming(
                    organization, project, cluster, appService)

            self.capellaAPI.cluster_ops_apis.app_svc_log_streaming_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters/{}/appservices/{}" \
                "/logStreaming"

            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                        .format(len(failures), len(testcases)))

 
    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner", "projectDataReader",
            "projectViewer", "projectDataReaderWriter", "projectManager"
        ]):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = self.capellaAPI.cluster_ops_apis.get_app_service_log_streaming(
                self.organisation_id, self.project_id, self.cluster_id,
                self.app_service_id, header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = (self.capellaAPI.cluster_ops_apis.
                          get_app_service_log_streaming(
                              self.organisation_id, self.project_id,
                              self.cluster_id, self.app_service_id,
                              header))
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))
    
    def test_query_parameters(self):
        self.log.debug("Correct Params - OrgID: {}, ProjID: {}, ClusID: {}, "
                       "AppSvcID: {}".format(
                        self.organisation_id, self.project_id,
                        self.cluster_id, self.app_service_id))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id, self.cluster_id,
                self.app_service_id):
            testcases += 1
            testcase = {
                "description": "OrganizationID: {}, ProjectID: {}, "
                               "ClusterID: {}, AppServiceID: {}"
                .format(str(combination[0]), str(combination[1]),
                        str(combination[2]), str(combination[3])),
                "organizationID": combination[0],
                "projectID": combination[1],
                "clusterID": combination[2],
                "appServiceID": combination[3]
            }
            if not (combination[0] == self.organisation_id and
                    combination[1] == self.project_id and
                    combination[2] == self.cluster_id and
                    combination[3] == self.app_service_id):
                if combination[1] == "" or combination[0] == "" or \
                        combination[2] == "":
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = "404 page not found"
                elif combination[3] == "" or any(variable in [
                    int, bool, float, list, tuple, set, type(None)] for
                         variable in [
                             type(combination[0]), type(combination[1]),
                             type(combination[2]), type(combination[3])]):
                    testcase["expected_status_code"] = 400
                    testcase["expected_error"] = {
                        "code": 1000,
                        "hint": "Check if all the required params are "
                                "present in the request body.",
                        "httpStatusCode": 400,
                        "message": "The server cannot or will not process the "
                                   "request due to something that is "
                                   "perceived to be a client error."
                    }
                elif combination[0] != self.organisation_id:
                    testcase["expected_status_code"] = 403
                    testcase["expected_error"] = {
                        "code": 1002,
                        "hint": "Your access to the requested resource is "
                                "denied. Please make sure you have the "
                                "necessary permissions to access the "
                                "resource.",
                        "httpStatusCode": 403,
                        "message": "Access Denied."
                    }
                elif combination[1] != self.project_id:
                    testcase["expected_status_code"] = 422
                    testcase["expected_error"] = {
                        "code": 4031,
                        "hint": "Please provide a valid projectId.",
                        "httpStatusCode": 422,
                        "message": "Unable to process the request. The "
                                   "provided projectId {} is not valid for "
                                   "the cluster {}."
                        .format(combination[1], combination[2])
                    }
                elif combination[2] != self.cluster_id:
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = {
                        "code": 4025,
                        "hint": "The requested cluster details could not be "
                                "found or fetched. Please ensure that the "
                                "correct cluster ID is provided.",
                        "message": "Unable to fetch the cluster details.",
                        "httpStatusCode": 404
                    }
                else:
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = {
                        "code": 404,
                        "hint": "Please review your request and ensure that "
                                "all required parameters are correctly "
                                "provided.",
                        "httpStatusCode": 404,
                        "message": "Requested App Service was not found"
                    }
            self.log.info("Executing test: {}".format(testcase["description"]))
            if "param" in testcase:
                kwarg = {testcase["param"]: testcase["paramValue"]}
            else:
                kwarg = dict()

            result = self.capellaAPI.cluster_ops_apis.get_app_service_log_streaming(
                testcase["organizationID"], testcase["projectID"],
                testcase["clusterID"], testcase['appServiceID'], **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.get_app_service_log_streaming(
                    testcase["organizationID"], testcase["projectID"],
                    testcase["clusterID"], testcase['appServiceID'], **kwarg)

            if self.validate_testcase(result, [200], testcase, failures):
                res, _ = self.validate_onoff_state(["turningOn", "healthy"],app=self.app_service_id)
                if not res:
                    failures.append(testcase["description"])

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))
    
    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
        self.capellaAPI.cluster_ops_apis.get_app_service_log_streaming,
            (self.organisation_id, self.project_id, self.cluster_id,
             self.app_service_id)
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.get_app_service_log_streaming,
            (self.organisation_id, self.project_id, self.cluster_id,
             self.app_service_id)
        ]]
        self.throttle_test(api_func_list, True, self.project_id)