"""
Created on May 06, 2025

@author: Created by ramesh
"""

from pytests.Capella.RestAPIv4.AppService.get_app_service import GetAppService
# from pytests.Capella.RestAPIv4.AppEndpoints.get_app_endpoints import GetAppEndpoints


class GetAdminUsers(GetAppService):

    def setUp(self, nomenclature="AppService_GET"):
        GetAppService.setUp(self, nomenclature)
        # GetAppEndpoints.setUp(self,nomenclature)

        self.expected_res = {
            "name": "user3",
            "password": "passwordD1,",
            "access": {
                "accessAllEndpoints": True
            }
        }

        self.log.info("Creating Admin User for the test")
        res = self.capellaAPI.cluster_ops_apis.add_app_service_admin_user(
            self.organisation_id, self.project_id, self.cluster_id, 
            self.app_service_id,
            **self.expected_res
        )
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.add_app_service_admin_user(
                self.organisation_id, self.project_id, self.cluster_id, 
                self.app_service_id,
                **self.expected_res
            )
        if res.status_code != 201:
            self.log.error("Result: {}".format(res.content))
            self.fail("Error while creating Admin User for the test.")
        self.log.info("Admin User created successfully.")
        self.user_id = res.json()['id']

    def tearDown(self):
        self.update_auth_with_api_token(self.curr_owner_key)
        result = self.capellaAPI.cluster_ops_apis.list_app_service_admin_user(
            self.organisation_id, self.project_id, self.cluster_id,
            self.app_service_id)

        if result.status_code == 429:
            self.handle_rate_limit(int(result.headers["Retry-After"]))
            result = self.capellaAPI.cluster_ops_apis.list_app_service_admin_user(
                self.organisation_id, self.project_id, self.cluster_id,
                self.app_service_id)

        if result.status_code != 200:
            self.fail("Failed to list admin users: {} - {}".format(result.status_code,result.content))

        users_data = result.json().get("data", [])
        list_of_admin_user_ids = [user["id"] for user in users_data if
                                  user.get("id") != self.user_id]
        if hasattr(self, 'user_id') and self.user_id:
            list_of_admin_user_ids.append(self.user_id)

        for user_id in list_of_admin_user_ids:
            self.log.info("Deleting the Admin User: {}".format(user_id))
            res = self.capellaAPI.cluster_ops_apis.delete_app_service_admin_user(
                self.organisation_id,
                self.project_id,
                self.cluster_id,
                self.app_service_id,
                user_id
            )

            if res.status_code == 429:
                self.handle_rate_limit(int(res.headers["Retry-After"]))
                res = self.capellaAPI.cluster_ops_apis.delete_app_service_admin_user(
                    self.organisation_id,
                    self.project_id,
                    self.cluster_id,
                    self.app_service_id,
                    user_id
                )

            if res.status_code not in [200, 202, 204, 404]:
                self.log.error(
                    "Failed to delete Admin User {}: {}".format(user_id,res.content))
                self.fail(
                    "Failed to delete Admin User {}: {}".format(user_id,res.content))
            else:
                self.log.info("Successfully deleted the Admin User: {}".format(user_id))

        super(GetAdminUsers, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Send call with valid path params"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}/appservices/{}/adminUsers",
                "expected_status_code": 404,
                "expected_error": {
                    "errorType": "RouteNotFound",
                    "message": "Not found"
                }
            }, {
                "description": "Replace the last path param name in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/appservices/{}/adminUser",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/appservices/{}/adminUsers/adminUser",
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
                "description": "Call API with non-hex userId",
                "invalid_userId": self.replace_last_character(
                    self.user_id, non_hex=True),
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
            user = self.user_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.admin_users_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]
            elif "invalid_clusterId" in testcase:
                cluster = testcase["invalid_clusterId"]
            elif "invalid_appServiceId" in testcase:
                appService = testcase["invalid_appServiceId"]
            elif "invalid_userId" in testcase:
                user = testcase["invalid_userId"]

            result = self.capellaAPI.cluster_ops_apis.fetch_app_service_admin_user_info(
                organization, project, cluster, appService, user)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.fetch_app_service_admin_user_info(
                    organization, project, cluster, appService, user)
            self.capellaAPI.cluster_ops_apis.admin_users_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters/{}/appservices/{}/adminUsers"
            self.validate_testcase(result, [200], testcase, failures)

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
            result = self.capellaAPI.cluster_ops_apis.fetch_app_service_admin_user_info(
                self.organisation_id, self.project_id, self.cluster_id,
                self.app_service_id, self.user_id,
                header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.fetch_app_service_admin_user_info(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.app_service_id, self.user_id,
                    header)
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_query_parameters(self):
        self.log.debug(
                "Correct Params - organization ID: {}, project ID: {}, "
                "cluster ID: {}, appService ID: {}, "
                "user ID: {}".format(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.app_service_id, self.user_id))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id, self.cluster_id,
                self.app_service_id, self.user_id):
            testcases += 1
            testcase = {
                "description": "organization ID: {}, project ID: {}, "
                "cluster ID: {}, appService ID: {}, "
                "user ID: {}"
                .format(str(combination[0]), str(combination[1]),
                        str(combination[2]), str(combination[3]),
                        str(combination[4])),
                "organizationID": combination[0],
                "projectID": combination[1],
                "clusterID": combination[2],
                "appServiceID": combination[3],
                "userID": combination[4]
            }
            if not (combination[0] == self.organisation_id and
                    combination[1] == self.project_id and
                    combination[2] == self.cluster_id and
                    combination[3] == self.app_service_id and
                    combination[4] == self.user_id):
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
                elif combination[3] != self.app_service_id and not isinstance(combination[3], type(None)):
                    testcase["expected_status_code"] = 404 
                    testcase["expected_error"] = { 
                        "code": 404, 
                        "hint": "The requested App Service could not be found.", # Or specific app service error
                        "message": "AppService not found.", 
                        "httpStatusCode": 404
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
                elif isinstance(combination[3], type(None)):
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = "404 page not found"
                else:
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = {
                        "code": 2000, # Assuming a specific error code for User Not Found for App Service
                        "hint": "The requested admin user could not be "
                                "found or fetched. Please ensure that the "
                                "correct user ID is provided.",
                        "httpStatusCode": 404,
                        "message": "App Service User not found." # More specific than Scope Not Found
                    }
            self.log.info("Executing test: {}".format(testcase["description"]))
            if "param" in testcase:
                kwarg = {testcase["param"]: testcase["paramValue"]}
            else:
                kwarg = dict()

            result = self.capellaAPI.cluster_ops_apis.fetch_app_service_admin_user_info(
                testcase["organizationID"], testcase["projectID"], 
                testcase["clusterID"], testcase["appServiceID"], 
                testcase["userID"],
                **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.fetch_app_service_admin_user_info(
                    testcase["organizationID"], testcase["projectID"], 
                    testcase["clusterID"], testcase["appServiceID"], 
                    testcase["userID"],
                    **kwarg)
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.fetch_app_service_admin_user_info, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.app_service_id, self.user_id
            )
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.fetch_app_service_admin_user_info, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.app_service_id, self.user_id
            )
        ]]
        self.throttle_test(api_func_list, True, self.project_id)
