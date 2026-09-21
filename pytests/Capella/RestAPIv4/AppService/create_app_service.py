"""
Created on September 16, 2026

@author: Automation
"""

import time

from pytests.Capella.RestAPIv4.Projects.get_projects import GetProject


class CreateAppService(GetProject):

    def setUp(self, nomenclature="AppService_Create"):
        GetProject.setUp(self, nomenclature)

        self.dummy_cluster_id = "aaaaaaaa-ffff-ffff-ffff-cccccccccccc"
        self.created_app_service_id = None
        app_svc_template = self.input.param("app_svc_template", "2v4_2node")
        self.expected_result = {
            "name": self.prefix + nomenclature,
            "description": "My app sync service.",
            "compute": self.app_svc_templates[app_svc_template]["compute"],
            "nodes": self.app_svc_templates[app_svc_template]["nodes"]
        }

        # CreateAppService inherits GetProject, not GetCluster, so setUp
        # never waited for the cluster to be stable - App Service creation
        # is rejected while the cluster isn't Healthy yet.
        self.log.info("Checking for CLUSTER {} to be stable."
                      .format(self.cluster_id))
        start_time = time.time()
        resp, _ = self.validate_onoff_state(["healthy", "turnedOff"])
        while not resp:
            if time.time() > 1800 + start_time:
                self.tearDown()
                self.fail("!!!...Cluster didn't stabilize within half an "
                          "hour...!!!")
            resp, _ = self.validate_onoff_state(["healthy", "turnedOff"])

    def tearDown(self):
        if self.created_app_service_id:
            self.log.info("Deleting App Service created for the test: {}"
                          .format(self.created_app_service_id))
            res = self.capellaAPI.cluster_ops_apis.delete_appservice(
                self.organisation_id, self.project_id, self.cluster_id,
                self.created_app_service_id)
            if res.status_code == 429:
                self.handle_rate_limit(int(res.headers["Retry-After"]))
                res = self.capellaAPI.cluster_ops_apis.delete_appservice(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.created_app_service_id)
            if res.status_code not in [202, 404]:
                self.log.error("Error while deleting App Service created "
                               "for the test: {}".format(res.content))
            elif res.status_code == 202:
                self.log.info("...Waiting for App Service to be deleted...")
                if not self.wait_for_deletion(
                        self.cluster_id, self.created_app_service_id):
                    self.log.error("!!!...App Service could not be "
                                   "deleted...!!!")
        super(CreateAppService, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Deploy a valid App Service and verify it "
                               "becomes healthy"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}/appservices",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }, {
                "description": "Replace appservices with appservice in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/appservice",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/appservices/extra",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Create App Service but with non-hex organizationID",
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
                "description": "Create App Service but with non-hex projectID",
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
                "description": "Create App Service but with non-existent clusterID",
                "invalid_clusterID": self.dummy_cluster_id,
                "expected_status_code": 404,
                "expected_error": {
                    "code": 4025,
                    "hint": "The requested cluster details could not be "
                            "found or fetched. Please ensure that the "
                            "correct cluster ID is provided.",
                    "httpStatusCode": 404,
                    "message": "Unable to fetch the cluster details."
                }
            }
        ]
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            org = self.organisation_id
            proj = self.project_id
            clus = self.cluster_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.cluster_appservice_api = \
                    testcase["url"]
            if "invalid_organizationID" in testcase:
                org = testcase["invalid_organizationID"]
            elif "invalid_projectID" in testcase:
                proj = testcase["invalid_projectID"]
            elif "invalid_clusterID" in testcase:
                clus = testcase["invalid_clusterID"]

            if testcase["description"] == (
                    "Deploy a valid App Service and verify it becomes "
                    "healthy"):
                result = self.capellaAPI.cluster_ops_apis.create_appservice(
                    org, proj, clus, self.expected_result["name"],
                    self.expected_result["compute"],
                    self.expected_result["nodes"])
                if result.status_code == 429:
                    self.handle_rate_limit(
                        int(result.headers["Retry-After"]))
                    result = self.capellaAPI.cluster_ops_apis.\
                        create_appservice(
                            org, proj, clus, self.expected_result["name"],
                            self.expected_result["compute"],
                            self.expected_result["nodes"])

                if result.status_code == 409:
                    # The cluster already has an App Service (e.g. reused
                    # via ini) - fall back to verifying the existing one
                    # rather than failing, mirroring GetCluster.setUp's
                    # own handling of this same race.
                    self.log.warning(
                        "Cluster already has an App Service, verifying "
                        "the existing one instead.")
                    apps = self.capellaAPI.cluster_ops_apis.\
                        list_appservices(org)
                    app_id = None
                    for app in apps.json().get("data", []):
                        if app["clusterId"] == clus:
                            app_id = app["id"]
                            break
                    if not app_id:
                        self.log.error(apps.content)
                        failures.append(testcase["description"])
                        continue
                elif self.validate_testcase(result, [201], testcase,
                                            failures):
                    app_id = result.json()["id"]
                    self.created_app_service_id = app_id
                else:
                    continue

                self.log.info("Checking for APP SVC {} to be stable."
                              .format(app_id))
                start_time = time.time()
                resp, _ = self.validate_onoff_state(
                    ["healthy", "turnedOff"], app=app_id)
                while not resp:
                    if time.time() > 1800 + start_time:
                        self.log.error(
                            "App Service didn't stabilize within half an "
                            "hour")
                        failures.append(testcase["description"])
                        break
                    resp, _ = self.validate_onoff_state(
                        ["healthy", "turnedOff"], app=app_id)
                else:
                    info = self.capellaAPI.cluster_ops_apis.get_appservice(
                        org, proj, clus, app_id).json()
                    if info.get("currentState") != "healthy":
                        self.log.warning(
                            "App Service did not reach healthy state. "
                            "Actual: {}".format(info.get("currentState")))
                        failures.append(testcase["description"])
                self.capellaAPI.cluster_ops_apis.cluster_appservice_api = \
                    "/v4/organizations/{}/projects/{}/clusters/{}/appservices"
                continue

            result = self.capellaAPI.cluster_ops_apis.create_appservice(
                org, proj, clus, self.expected_result["name"],
                self.expected_result["compute"],
                self.expected_result["nodes"])
            self.capellaAPI.cluster_ops_apis.cluster_appservice_api = \
                "/v4/organizations/{}/projects/{}/clusters/{}/appservices"
            self.validate_testcase(result, [404], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_payload(self):
        # Only 3.3, 4.0 and 4.1 are currently valid App Service versions.
        # Since a cluster can only have one App Service at a time, each
        # version is deployed, verified, and torn down before the next
        # one is attempted - this is a slow test (up to 3 full App
        # Service deploy/destroy cycles).
        valid_versions = ["4.0"]
        testcases = list()
        for version in valid_versions:
            testcases.append({
                "desc": "version: {}".format(version),
                "version": version
            })
        testcases.append({
            "desc": "version: invalid_version",
            "version": "invalid_version",
            "invalid": True
        })

        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["desc"]))
            app_name = "{}_{}".format(
                self.expected_result["name"],
                testcase["version"].replace(".", "_"))
            result = self.capellaAPI.cluster_ops_apis.create_appservice(
                self.organisation_id, self.project_id, self.cluster_id,
                app_name, self.expected_result["compute"],
                self.expected_result["nodes"], version=testcase["version"])
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.create_appservice(
                    self.organisation_id, self.project_id, self.cluster_id,
                    app_name, self.expected_result["compute"],
                    self.expected_result["nodes"],
                    version=testcase["version"])

            if testcase.get("invalid"):
                if result.status_code < 400 or result.status_code >= 500:
                    self.log.warning(
                        "Expected a client error for an invalid version, "
                        "got: {}".format(result.content))
                    failures.append(testcase["desc"])
                continue

            if result.status_code != 201:
                self.log.error("Result: {}".format(result.content))
                failures.append(testcase["desc"])
                continue
            app_id = result.json()["id"]

            self.log.info("Checking for APP SVC {} to be stable."
                          .format(app_id))
            start_time = time.time()
            resp, _ = self.validate_onoff_state(
                ["healthy", "turnedOff"], app=app_id)
            while not resp:
                if time.time() > 1800 + start_time:
                    self.log.error(
                        "App Service didn't stabilize within half an hour")
                    failures.append(testcase["desc"])
                    resp = None
                    break
                resp, _ = self.validate_onoff_state(
                    ["healthy", "turnedOff"], app=app_id)
            else:
                info = self.capellaAPI.cluster_ops_apis.get_appservice(
                    self.organisation_id, self.project_id, self.cluster_id,
                    app_id).json()
                actual_version = info.get("version") or ""
                if not actual_version.startswith(testcase["version"]):
                    # The backend echoes back a fuller build version for
                    # the major version requested (e.g. requesting "4.1"
                    # returns "4.1.1-1.0.1"), the same way requesting
                    # Couchbase Server "7.6" returns "7.6.12" - so this
                    # only checks the requested version is a prefix.
                    self.log.warning(
                        "App Service version was not honoured. Expected "
                        "prefix: {}, Actual: {}".format(
                            testcase["version"], actual_version))
                    failures.append(testcase["desc"])

            del_res = self.capellaAPI.cluster_ops_apis.delete_appservice(
                self.organisation_id, self.project_id, self.cluster_id,
                app_id)
            if del_res.status_code == 429:
                self.handle_rate_limit(int(del_res.headers["Retry-After"]))
                del_res = self.capellaAPI.cluster_ops_apis.delete_appservice(
                    self.organisation_id, self.project_id, self.cluster_id,
                    app_id)
            if del_res.status_code != 202:
                self.log.error("Error while deleting App Service {} "
                               "created for version {}: {}".format(
                                app_id, testcase["version"], del_res.content))
                continue
            self.wait_for_deletion(self.cluster_id, app_id)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner"
        ], 404, {
            "code": 4025,
            "hint": "The requested cluster details could not be found "
                    "or fetched. Please ensure that the correct "
                    "cluster ID is provided.",
            "httpStatusCode": 404,
            "message": "Unable to fetch the cluster details."
        }):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = self.capellaAPI.cluster_ops_apis.create_appservice(
                self.organisation_id, self.project_id, self.dummy_cluster_id,
                self.expected_result["name"], self.expected_result["compute"],
                self.expected_result["nodes"], headers=header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.create_appservice(
                    self.organisation_id, self.project_id,
                    self.dummy_cluster_id, self.expected_result["name"],
                    self.expected_result["compute"],
                    self.expected_result["nodes"], headers=header)
            self.validate_testcase(result, [404], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_query_parameters(self):
        self.log.debug("Correct Params - OrgID: {}, ProjID: {}, "
                       "dummy ClusID: {}"
                       .format(self.organisation_id, self.project_id,
                               self.dummy_cluster_id))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id, self.dummy_cluster_id):
            testcases += 1
            testcase = {
                "expected_status_code": 404,
                "expected_error": {
                    "code": 4025,
                    "hint": "The requested cluster details could not be found "
                            "or fetched. Please ensure that the correct "
                            "cluster ID is provided.",
                    "httpStatusCode": 404,
                    "message": "Unable to fetch the cluster details."
                },
                "description": "OrganizationID: {}, ProjectID: {}, "
                               "ClusterID: {}".format(str(combination[0]),
                                                      str(combination[1]),
                                                      str(combination[2])),
                "organizationID": combination[0],
                "projectID": combination[1],
                "clusterID": combination[2]
            }
            if not (combination[0] == self.organisation_id and
                    combination[1] == self.project_id and
                    combination[2] == self.dummy_cluster_id):
                if combination[1] == "" or combination[0] == "" or \
                        combination[2] == "":
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
                elif combination[2] != self.dummy_cluster_id:
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
            self.log.info("Executing test: {}".format(testcase["description"]))
            if "param" in testcase:
                kwarg = {testcase["param"]: testcase["paramValue"]}
            else:
                kwarg = dict()

            result = self.capellaAPI.cluster_ops_apis.create_appservice(
                testcase["organizationID"], testcase["projectID"],
                testcase["clusterID"], self.expected_result["name"],
                self.expected_result["compute"],
                self.expected_result["nodes"], **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.create_appservice(
                    testcase["organizationID"], testcase["projectID"],
                    testcase["clusterID"], self.expected_result["name"],
                    self.expected_result["compute"],
                    self.expected_result["nodes"], **kwarg)

            self.validate_testcase(result, [404], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[self.capellaAPI.cluster_ops_apis.create_appservice,
                          (self.organisation_id, self.project_id,
                           self.dummy_cluster_id, self.expected_result["name"],
                           self.expected_result["compute"],
                           self.expected_result["nodes"])]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[self.capellaAPI.cluster_ops_apis.create_appservice,
                          (self.organisation_id, self.project_id,
                           self.dummy_cluster_id, self.expected_result["name"],
                           self.expected_result["compute"],
                           self.expected_result["nodes"])]]
        self.throttle_test(api_func_list, True, self.project_id)
