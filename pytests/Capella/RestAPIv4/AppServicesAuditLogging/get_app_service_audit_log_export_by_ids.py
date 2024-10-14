"""
Created on August 06, 2024

@author: Created using cbRAT cbModule by Vipul Bhardwaj
"""

from pytests.Capella.RestAPIv4.AppService.get_app_service import GetAppService


class GetAuditLogExports(GetAppService):

    def setUp(self, nomenclature="AppServicesAuditLogging_GET"):
        GetAppService.setUp(self, nomenclature)
        self.expected_res = {
            # Creation params.
            "start": self.get_utc_datetime(-60),
            "end": self.get_utc_datetime(-10),

            # Response params.
            "data": {
                "id": "to be changed after creation",
                "start": self.get_utc_datetime(-60),
                "end": self.get_utc_datetime(-10),
                "download_id": None,
                "download_expires": None,
                "status": None,
                "appServiceId": self.app_service_id,
                "tenantId": self.organisation_id,
                "clusterId": self.cluster_id,
                "createdByUserID": None,
                "upsertedByUserID": None,
                "createdAt": None,
                "upsertedAt": None,
                "modifiedByUserID": None,
                "modifiedAt": None,
                "version": None
            },
            "permissions": {
                "create": {
                    "accessible": None
                },
                "read": {
                    "accessible": None
                },
                "update": {
                    "accessible": None
                },
                "delete": {
                    "accessible": None
                }
            }
        }

        self.log.info("...Creating AppSvc Audit Logging Export...")
        res = self.capellaAPI.cluster_ops_apis.create_app_svc_audit_log_export(
            self.organisation_id, self.project_id, self.cluster_id,
            self.app_service_id, self.expected_res["start"],
            self.expected_res["end"])
        if res.status_code == 429:
            self.handle_rate_limit(res.headers["Retry-After"])
            res = (self.capellaAPI.cluster_ops_apis.
                   create_app_svc_audit_log_export(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.app_service_id, self.expected_res["start"],
                    self.expected_res["end"]))
        if res.status_code != 202:
            self.log.error("Error: {}".format(res.content))
            self.tearDown()
            self.fail("!!!...Error while creating AppSvc Audit Logging "
                      "Export...!!!")
        self.log.info("AppSvc Audit Logging Export created successfully.")

        # Set Parameters for the expected res to be validated in GET
        self.auditLogExport_id = res.json()["exportId"]
        self.expected_res["data"]["id"] = self.auditLogExport_id

    def tearDown(self):
        self.update_auth_with_api_token(self.curr_owner_key)
        super(GetAuditLogExports, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Send call with valid path params"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}/appservices/{}/auditLogExports",
                "expected_status_code": 404,
                "expected_error": {
                    "errorType": "RouteNotFound",
                    "message": "Not found"
                }
            }, {
                "description": "Replace the last path param name in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/appservices/{}/auditLogExport",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/appservices/{}/auditLogExports/auditLogExport",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Call API with non-hex organizationId",
                "invalid_organizationId": self.replace_last_character(
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
                "description": "Call API with non-hex projectId",
                "invalid_projectId": self.replace_last_character(
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
                "description": "Call API with non-hex clusterId",
                "invalid_clusterId": self.replace_last_character(
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
            }, {
                "description": "Call API with non-hex auditLogExportId",
                "invalid_auditLogExportId": self.replace_last_character(
                    self.auditLogExport_id, non_hex=True),
                "expected_status_code": 404,
                "expected_error": {
                    "code": 3000,
                    "hint": "Resource not Found.",
                    "httpStatusCode": 404,
                    "message": "Not Found."
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
            auditLogExport = self.auditLogExport_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis\
                    .app_svc_audit_log_exports_endpoint = testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]
            elif "invalid_clusterId" in testcase:
                cluster = testcase["invalid_clusterId"]
            elif "invalid_appServiceId" in testcase:
                appService = testcase["invalid_appServiceId"]
            elif "invalid_auditLogExportId" in testcase:
                auditLogExport = testcase["invalid_auditLogExportId"]

            result = (self.capellaAPI.cluster_ops_apis
                      .fetch_app_svc_audit_log_export_info(
                        organization, project, cluster, appService,
                        auditLogExport))
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = (self.capellaAPI.cluster_ops_apis
                          .fetch_app_svc_audit_log_export_info(
                            organization, project, cluster, appService,
                            auditLogExport))
            self.capellaAPI.cluster_ops_apis\
                .app_svc_audit_log_exports_endpoint = \
                ("/v4/organizations/{}/projects/{}/clusters/{}/appservices/{}/"
                 "auditLogExports")
            self.validate_testcase(result, [200], testcase, failures, True,
                                   self.expected_res, self.auditLogExport_id)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner", "projectManager",
            "projectViewer"
        ]):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = (self.capellaAPI.cluster_ops_apis
                      .fetch_app_svc_audit_log_export_info(
                        self.organisation_id, self.project_id, self.cluster_id,
                        self.app_service_id, self.auditLogExport_id, header))
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = (self.capellaAPI.cluster_ops_apis
                          .fetch_app_svc_audit_log_export_info(
                            self.organisation_id, self.project_id,
                            self.cluster_id, self.app_service_id,
                            self.auditLogExport_id, header))
            self.validate_testcase(result, [200], testcase, failures, True,
                                   self.expected_res, self.auditLogExport_id)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_query_parameters(self):
        self.log.debug(
                "Correct Params - organization ID: {}, project ID: {}, "
                "cluster ID: {}, appService ID: {}, "
                "auditLogExport ID: {}".format(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.app_service_id, self.auditLogExport_id))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id, self.cluster_id,
                self.app_service_id, self.auditLogExport_id):
            testcases += 1
            testcase = {
                "description": "organization ID: {}, project ID: {}, "
                "cluster ID: {}, appService ID: {}, auditLogExport ID: {}"
                .format(str(combination[0]), str(combination[1]),
                        str(combination[2]), str(combination[3]),
                        str(combination[4])),
                "organizationID": combination[0],
                "projectID": combination[1],
                "clusterID": combination[2],
                "appServiceID": combination[3],
                "auditLogExportID": combination[4]
            }
            if not (combination[0] == self.organisation_id and
                    combination[1] == self.project_id and
                    combination[2] == self.cluster_id and
                    combination[3] == self.app_service_id and
                    combination[4] == self.auditLogExport_id):
                if (combination[0] == "" or combination[1] == "" or
                        combination[2] == "" or combination[3] == "" or
                        combination[4] == ""):
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = "404 page not found"
                elif any(variable in [
                    int, bool, float, list, tuple, set, type(None)] for
                         variable in [
                             type(combination[0]), type(combination[1]), 
                             type(combination[2]), type(combination[3]), 
                             type(combination[4])]):
                    testcase["expected_status_code"] = 400
                    testcase["expected_error"] = {
                        "code": 1000,
                        "hint": "Check if you have provided a valid URL and "
                                "all the required params are present in the "
                                "request body.",
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
                elif combination[3] != self.app_service_id:
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = {
                        "code": 6008,
                        "hint": "The requested bucket does not exist. Please "
                                "ensure that the correct bucket ID is "
                                "provided.",
                        "httpStatusCode": 404,
                        "message": "Unable to find the specified bucket."
                    }
                else:
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = {
                        "code": 11002,
                        "hint": "The requested scope details could not be "
                                "found or fetched. Please ensure that the "
                                "correct scope name is provided.",
                        "httpStatusCode": 404,
                        "message": "Scope Not Found"
                    }
            self.log.info("Executing test: {}".format(testcase["description"]))
            if "param" in testcase:
                kwarg = {testcase["param"]: testcase["paramValue"]}
            else:
                kwarg = dict()

            result = (self.capellaAPI.cluster_ops_apis
                      .fetch_app_svc_audit_log_export_info(
                        testcase["organizationID"], testcase["projectID"],
                        testcase["clusterID"], testcase["appServiceID"],
                        testcase["auditLogExportID"], **kwarg))
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = (self.capellaAPI.cluster_ops_apis
                          .fetch_app_svc_audit_log_export_info(
                            testcase["organizationID"], testcase["projectID"],
                            testcase["clusterID"], testcase["appServiceID"],
                            testcase["auditLogExportID"], **kwarg))
            self.validate_testcase(result, [200], testcase, failures, True,
                                   self.expected_res, self.auditLogExport_id)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis
            .fetch_app_svc_audit_log_export_info, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.app_service_id, self.auditLogExport_id
            )
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis
            .fetch_app_svc_audit_log_export_info, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.app_service_id, self.auditLogExport_id
            )
        ]]
        self.throttle_test(api_func_list, True, self.project_id)