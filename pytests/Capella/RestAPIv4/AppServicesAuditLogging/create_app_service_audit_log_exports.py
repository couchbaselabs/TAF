"""
Created on August 06, 2024

@author: Created using cbRAT cbModule by Vipul Bhardwaj
"""

import copy
from pytests.Capella.RestAPIv4.AppServicesAuditLogging\
    .get_app_service_audit_log_export_by_ids import GetAuditLogExports


class PostAuditLogExports(GetAuditLogExports):

    def setUp(self, nomenclature="AppServicesAuditLogging_POST"):
        GetAuditLogExports.setUp(self, nomenclature)

    def tearDown(self):
        super(PostAuditLogExports, self).tearDown()

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
                "expected_status_code": 405,
                "expected_error": ""
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

            result = (self.capellaAPI.cluster_ops_apis
                      .create_app_svc_audit_log_export(
                        organization, project, cluster, appService,
                        self.expected_res["start"],
                        self.expected_res["end"]))
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = (self.capellaAPI.cluster_ops_apis
                          .create_app_svc_audit_log_export(
                            organization, project, cluster, appService,
                            self.expected_res["start"],
                            self.expected_res["end"]))
            self.capellaAPI.cluster_ops_apis\
                .app_svc_audit_log_exports_endpoint = \
                ("/v4/organizations/{}/projects/{}/clusters/{}/appservices/{}/"
                 "auditLogExports")
            if self.validate_testcase(result, [202], testcase, failures):
                self.log.debug("Creation Successful")

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner", "projectManager"
        ]):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = (self.capellaAPI.cluster_ops_apis
                      .create_app_svc_audit_log_export(
                        self.organisation_id, self.project_id, self.cluster_id,
                        self.app_service_id, self.expected_res["start"],
                        self.expected_res["end"], header))
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = (self.capellaAPI.cluster_ops_apis
                          .create_app_svc_audit_log_export(
                            self.organisation_id, self.project_id,
                            self.cluster_id, self.app_service_id,
                            self.expected_res["start"],
                            self.expected_res["end"], header))
            if self.validate_testcase(result, [202], testcase, failures):
                self.log.debug("Creation Successful")

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_query_parameters(self):
        self.log.debug(
                "Correct Params - organization ID: {}, project ID: {}, "
                "cluster ID: {}, appService ID: {}".format(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.app_service_id))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id, self.cluster_id,
                self.app_service_id):
            testcases += 1
            testcase = {
                "description": "organization ID: {}, project ID: {}, "
                "cluster ID: {}, appService ID: {}"
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
                elif combination[3] == "":
                    testcase["expected_status_code"] = 405
                    testcase["expected_error"] = ""
                elif any(variable in [
                    int, bool, float, list, tuple, set, type(None)] for
                     variable in [type(combination[0]), type(combination[1]),
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

            result = (self.capellaAPI.cluster_ops_apis
                      .create_app_svc_audit_log_export(
                        testcase["organizationID"], testcase["projectID"],
                        testcase["clusterID"], testcase["appServiceID"],
                        self.expected_res["start"], self.expected_res["end"],
                        **kwarg))
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = (self.capellaAPI.cluster_ops_apis
                          .create_app_svc_audit_log_export(
                            testcase["organizationID"], testcase["projectID"],
                            testcase["clusterID"], testcase["appServiceID"],
                            self.expected_res["start"],
                            self.expected_res["end"], **kwarg))
            if self.validate_testcase(result, [202], testcase, failures):
                self.log.debug("Creation Successful")

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_payload(self):
        testcases = list()
        for key in self.expected_res:
            if key not in ["start", "end"]:
                continue
            values = [
                "", "abc", 1, 1e1000, -1, 0, None,
                self.generate_random_string(special_characters=False),
                self.generate_random_string(5000, False)
            ]
            for val in values:
                testcase = copy.deepcopy(self.expected_res)
                testcase[key] = val
                testcase["description"] = "Testing `{}` with val: {} of {}" \
                    .format(key, val, type(val))
                # Expected error conditions
                # if isinstance(val, type(int)) or val == "":
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
                if isinstance(val, type(None)):
                    testcase["expected_status_code"] = 422
                    if key == "end":
                        testcase["expected_error"] = {
                            "code": 422,
                            "hint": "Please review your request and ensure "
                                    "that all required parameters are "
                                    "correctly provided.",
                            "httpStatusCode": 422,
                            "message": "Audit Logging Export Request is too "
                                       "old. Cannot request logs older than "
                                       "30 days."
                        }
                    if key == "start":
                        testcase["expected_error"]["message"] = \
                            "Requested start time is after the end time."
                testcases.append(testcase)
        failures = list()
        for testcase in testcases:
            self.log.info(testcase['description'])
            res = (self.capellaAPI.cluster_ops_apis.
                   create_app_svc_audit_log_export(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.app_service_id, testcase["start"], testcase["end"]))
            if res.status_code == 429:
                self.handle_rate_limit(res.headers["Retry-After"])
                res = (self.capellaAPI.cluster_ops_apis.
                       create_app_svc_audit_log_export(
                        self.organisation_id, self.project_id, self.cluster_id,
                        self.app_service_id, testcase["start"], testcase["end"]
                       ))
            self.validate_testcase(res, [202], testcase, failures,
                                   payloadTest=True)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.create_app_svc_audit_log_export, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.app_service_id,
                self.expected_res["start"], self.expected_res["end"]
            )
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.create_app_svc_audit_log_export, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.app_service_id,
                self.expected_res["start"], self.expected_res["end"]
            )
        ]]
        self.throttle_test(api_func_list, True, self.project_id)