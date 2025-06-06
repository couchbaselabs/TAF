"""
Created on March 13, 2024

@author: Created using cbRAT cbFile by Vipul Bhardwaj
"""

from pytests.Capella.RestAPIv4.Clusters.get_clusters import GetCluster


class GetAuditLogExport(GetCluster):

    def setUp(self, nomenclature="AuditLogExports_Get"):
        GetCluster.setUp(self, nomenclature)

        self.expected_res = {
            "auditLogDownloadURL": None,
            "expiration": None,
            "start": self.get_utc_datetime(-30),
            "end": self.get_utc_datetime(50),
            "createdAt": None,
            "status": None
        }
        self.log.info("Creating AuditLogExport")
        res = self.capellaAPI.cluster_ops_apis.create_audit_log_export(
            self.organisation_id, self.project_id, self.cluster_id,
            self.expected_res["start"], self.expected_res["end"])
        if res.status_code != 202:
            self.log.error("Result: {}".format(res.content))
            self.tearDown()
            self.fail("Error while creating AuditLogExport.")
        self.log.info("AuditLogExport created successfully.")

        self.auditlogexport_id = res.json()["exportId"]
        self.expected_res["auditLogExportId"] = self.auditlogexport_id

    def tearDown(self):
        self.update_auth_with_api_token(self.curr_owner_key)
        super(GetAuditLogExport, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Send call with valid path params"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}/auditLogExports",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }, {
                "description": "Replace the last path param name in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/auditLogExport",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/auditLogExports/auditLogExport",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Send call with non-hex organizationID",
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
                "description": "Send call with non-hex projectID",
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
                "description": "Send call with non-hex auditlogexportID",
                "invalid_auditlogexportID": self.replace_last_character(
                    self.auditlogexport_id, non_hex=True),
                "expected_status_code": 404,
                "expected_error": {
                    "code": 404,
                    "hint": "Please review your request and ensure that all "
                            "required parameters are correctly provided.",
                    "httpStatusCode": 404,
                    "message": "The requested export ID does not exist."
                }
            }
        ]

        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            organization = self.organisation_id
            project = self.project_id
            cluster = self.cluster_id
            auditlogexport = self.auditlogexport_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.audit_log_exports_endpoint = \
                    testcase["url"]
            if "invalid_organizationID" in testcase:
                organization = testcase["invalid_organizationID"]
            elif "invalid_projectID" in testcase:
                project = testcase["invalid_projectID"]
            elif "invalid_clusterID" in testcase:
                cluster = testcase["invalid_clusterID"]
            elif "invalid_auditlogexportID" in testcase:
                auditlogexport = testcase["invalid_auditlogexportID"]

            result = self.capellaAPI.cluster_ops_apis.fetch_audit_log_export_info(
                organization, project, cluster, auditlogexport)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.fetch_audit_log_export_info(
                    organization, project, cluster, auditlogexport)
            self.capellaAPI.cluster_ops_apis.audit_log_exports_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters/{}/auditLogExports"
            self.validate_testcase(result, [200, 404], testcase, failures,
                                   True, self.expected_res,
                                   self.auditlogexport_id)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectViewer", "projectOwner",
            "projectDataReaderWriter", "projectDataReader", "projectManager"
        ]):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = self.capellaAPI.cluster_ops_apis.fetch_audit_log_export_info(
                self.organisation_id, self.project_id, self.cluster_id,
                self.auditlogexport_id, header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.fetch_audit_log_export_info(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.auditlogexport_id, header)
            self.validate_testcase(result, [200, 404], testcase, failures,
                                   True, self.expected_res,
                                   self.auditlogexport_id)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_query_parameters(self):
        self.log.debug(
                "Correct Params - organization ID: {}, project ID: {}, "
                "cluster ID: {}, auditlogexport ID: {}".format(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.auditlogexport_id))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id, self.cluster_id,
                self.auditlogexport_id):
            testcases += 1
            testcase = {
                "description": "organization ID: {}, project ID: {}, "
                "cluster ID: {}, auditlogexport ID: {}"
                .format(str(combination[0]), str(combination[1]),
                        str(combination[2]), str(combination[3])),
                "organizationID": combination[0],
                "projectID": combination[1],
                "clusterID": combination[2],
                "auditlogexportID": combination[3]
            }
            if not (combination[0] == self.organisation_id and
                    combination[1] == self.project_id and
                    combination[2] == self.cluster_id and
                    combination[3] == self.auditlogexport_id):
                if (combination[0] == "" or combination[1] == "" or
                        combination[2] == "" or combination[3] == ""):
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = "404 page not found"
                elif any(variable in [
                    int, bool, float, list, tuple, set, type(None)] for
                         variable in [
                             type(combination[0]), type(combination[1]),
                             type(combination[2])]):
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
                elif combination[2] != self.project_id:
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
                        "message": "The requested export ID does not exist."
                    }
            self.log.info("Executing test: {}".format(testcase["description"]))
            if "param" in testcase:
                kwarg = {testcase["param"]: testcase["paramValue"]}
            else:
                kwarg = dict()

            result = self.capellaAPI.cluster_ops_apis.fetch_audit_log_export_info(
                testcase["organizationID"], testcase["projectID"],
                testcase["clusterID"], testcase["auditlogexportID"], **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.fetch_audit_log_export_info(
                    testcase["organizationID"], testcase["projectID"],
                    testcase["clusterID"], testcase["auditlogexportID"],
                    **kwarg)
            self.validate_testcase(result, [200, 404], testcase, failures,
                                   True, self.expected_res,
                                   self.auditlogexport_id)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.fetch_audit_log_export_info, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.auditlogexport_id
            )
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.fetch_audit_log_export_info, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.auditlogexport_id
            )
        ]]
        self.throttle_test(api_func_list, True, self.project_id)
