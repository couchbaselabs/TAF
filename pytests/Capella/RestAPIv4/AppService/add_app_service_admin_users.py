"""
Created on April 17, 2025

@author: Created using cbRAT cbModule by ramesh
"""

from pytests.Capella.RestAPIv4.AppService.get_app_service_admin_users import GetAdminUsers
from copy import deepcopy


class PostAdminUsers(GetAdminUsers):

    def setUp(self, nomenclature="AppService_POST"):
        GetAdminUsers.setUp(self, nomenclature)

    def tearDown(self):
        super(PostAdminUsers, self).tearDown()

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
        if self.user_id:
            self.flush_appservices_admin_user(
                self.project_id, self.cluster_id, self.app_service_id,
                self.user_id
            )
            self.user_id = None
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            organization = self.organisation_id
            project = self.project_id
            cluster = self.cluster_id
            appService = self.app_service_id

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

            test_params = deepcopy(self.expected_res)
            random_suffix = self.generate_random_string(8, special_characters=False)
            test_params["name"] = "user_{}".format(random_suffix)
            self.log.debug("Creating admin user with unique name: {}".format(test_params['name']))

            result = self.capellaAPI.cluster_ops_apis.add_app_service_admin_user(
                organization, project, cluster, appService,
                **test_params)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.add_app_service_admin_user(
                    organization, project, cluster, appService,
                    **test_params)
            self.capellaAPI.cluster_ops_apis.admin_users_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters/{}/appservices/{}/adminUsers"
            if self.validate_testcase(result, [201], testcase, failures):
                self.log.debug("Creation Successful")
                self.log.debug("Response: {}".format(result.json()))
                response_data = result.json()
                temp_user_id = response_data.get("id")
                if temp_user_id:
                    self.log.debug("Cleaning up admin user with ID: {}".format(temp_user_id))
                    self.flush_appservices_admin_user(
                        self.project_id, self.cluster_id, self.app_service_id,
                        temp_user_id
                    )
                else:
                    self.log.warning(
                        "No 'id' in response; skipping cleanup. Response: {}".format(
                            response_data))
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
            result = self.capellaAPI.cluster_ops_apis.add_app_service_admin_user(
                self.organisation_id, self.project_id, self.cluster_id,
                self.app_service_id,
                header,
                **self.expected_res
            )
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.add_app_service_admin_user(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.app_service_id,
                    header,
                    **self.expected_res
                )
            if self.validate_testcase(result, [200], testcase, failures):
                self.log.debug("Creation Successful")
                self.flush_appservices_admin_user(
                    self.project_id, self.cluster_id, self.app_service_id,
                    [result.json()["id"]])

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
                if (combination[0] == "" or combination[1] == "" or combination[2] == ""
                        ):
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = "404 page not found"
                elif combination[3] == "":
                    testcase["expected_status_code"] = 405
                    testcase["expected_error"] = ""
                elif any(variable in [
                    int, bool, float, list, tuple, set, type(None)] for
                         variable in [
                             type(combination[0]), type(combination[1]),
                             type(combination[2]), type(combination[3])]):
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
                elif combination[3] != self.app_endpoint_bucket_id and not \
                        isinstance(combination[3], type(None)):
                    testcase["expected_status_code"] = 400
                    testcase["expected_error"] = {
                        "code": 400,
                        "hint": "Please review your request and ensure "
                                "that all required parameters are "
                                "correctly provided.",
                        "message": "BucketID is invalid.",
                        "httpStatusCode": 400
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
                    testcase["expected_error"] = {
                        "code": 6008,
                        "hint": "The requested bucket does not exist. Please "
                                "ensure that the correct bucket ID is "
                                "provided.",
                        "httpStatusCode": 404,
                        "message": "Unable to find the specified bucket."
                    }
                else:
                    testcase["expected_status_code"] = 400
                    testcase["expected_error"] = {
                        "code": 400,
                        "hint": "Please review your request and ensure that "
                                "all required parameters are correctly "
                                "provided.",
                        "message": "BucketID is invalid.",
                        "httpStatusCode": 400
                    }
            self.log.info("Executing test: {}".format(testcase["description"]))
            if "param" in testcase:
                kwarg = {testcase["param"]: testcase["paramValue"]}
            else:
                kwarg = dict()
            kwarg.update(self.expected_res)
            result = self.capellaAPI.cluster_ops_apis.add_app_service_admin_user(
                self.organisation_id, self.project_id, self.cluster_id,
                self.app_service_id,
                **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.add_app_service_admin_user(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.app_service_id,
                    **kwarg)
            if self.validate_testcase(result, [200], testcase, failures):
                self.log.debug("Creation Successful")
                self.flush_appservices_admin_user(
                    self.project_id, self.cluster_id, self.app_service_id,
                    [result.json()["id"]])

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_payload(self):
        testcases = list()
        if self.user_id:
            self.flush_appservices_admin_user(
                self.project_id, self.cluster_id, self.app_service_id,
                self.user_id
            )
            self.user_id = None
        for k in self.expected_res:
            if k in []:
                continue

            for v in [
                "", 1, 0, 100000, -1, 123.123, None, [], {},
                self.generate_random_string(special_characters=False),
                self.generate_random_string(500, special_characters=False),
            ]:
                testcase = deepcopy(self.expected_res)
                if k == "access":
                    testcase[k]["accessAllEndpoints"] = v
                else:
                    testcase[k] = v
                for param in []:
                    del testcase[param]
                testcase["description"] = "Testing `{}` with val: `{}` of {}"\
                    .format(k, v, type(v))
 
                # Added expected validation errors based on field and value
                if k == "access":
                    testcase["expected_status_code"] = 422
                    testcase["expected_error"] = {
                        "code": 422,
                        "hint": "Please review your request and ensure that all required parameters are correctly provided.",
                        "httpStatusCode": 422,
                        "message": "Payload for creating or modifying app service admin user contains or lacks both, list of endpoints and all endpoints flag."
                    }
                elif k == "password":
                    if isinstance(v, (int, float, list, dict)):
                        testcase["expected_status_code"] = 400
                        testcase["expected_error"] = {
                            "code": 1000,
                            "hint": "The request was malformed or invalid.",
                            "httpStatusCode": 400,
                            "message": "Bad Request. Error: body contains incorrect JSON type for field \"password\"."
                        }
                    elif v == "" or v is None or (isinstance(v, str) and len(v) < 8):
                        testcase["expected_status_code"] = 422
                        testcase["expected_error"] = {
                            "code": 422,
                            "hint": "Please review your request and ensure that all required parameters are correctly provided.",
                            "httpStatusCode": 422,
                            "message": "Can not create application user. The password provided is too short. Can not be less than 8 characters."
                        }
                    elif isinstance(v, str) and len(v) >= 8 and not any(c in "!@#$%^&*()_+-={}[]|\\:;\"'<>,.?/" for c in v):
                        testcase["expected_status_code"] = 422
                        testcase["expected_error"] = {
                            "code": 422,
                            "hint": "Please review your request and ensure that all required parameters are correctly provided.",
                            "httpStatusCode": 422,
                            "message": "Can not create application user. The password must contain one special character."
                        }
                elif k == "name":
                    if isinstance(v, (int, float, list, dict)):
                        testcase["expected_status_code"] = 400
                        testcase["expected_error"] = {
                            "code": 1000,
                            "hint": "The request was malformed or invalid.",
                            "httpStatusCode": 400,
                            "message": "Bad Request. Error: body contains incorrect JSON type for field \"name\"."
                        }
                    elif v == "" or v is None or (isinstance(v, str) and len(v) < 2):
                        testcase["expected_status_code"] = 422
                        testcase["expected_error"] = {
                            "code": 422,
                            "hint": "Please review your request and ensure that all required parameters are correctly provided.",
                            "httpStatusCode": 422,
                            "message": "Can not create application user. The name provided is too short. Can not be less than 2 characters."
                        }
                    elif isinstance(v, str) and len(v) > 128:
                        testcase["expected_status_code"] = 422
                        testcase["expected_error"] = {
                            "code": 422,
                            "hint": "Please review your request and ensure that all required parameters are correctly provided.",
                            "httpStatusCode": 422,
                            "message": "Can not create application user. The name provided is too long. Can not exceed 128 UTF characters."
                        }
 
                testcases.append(testcase)

        failures = list()
        for testcase in testcases:
            self.log.info(testcase['description'])
 
            api_params = deepcopy(testcase)
            if "description" in api_params:
                del api_params["description"]
            if "expected_status_code" in api_params:
                del api_params["expected_status_code"]
            if "expected_error" in api_params:
                del api_params["expected_error"]
            if "Testing `name`" not in testcase['description']:
                random_suffix = self.generate_random_string(8, special_characters=False)
                api_params["name"] = "user_{}".format(random_suffix)
                self.log.debug("Creating admin user with unique name: {}".format(api_params['name']))
 
            result = self.capellaAPI.cluster_ops_apis.add_app_service_admin_user(
                self.organisation_id, self.project_id, self.cluster_id,
                self.app_service_id,
                **api_params)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.add_app_service_admin_user(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.app_service_id,
                    **api_params)
            expected_codes = [201]
            if "expected_status_code" in testcase:
                expected_codes = [testcase["expected_status_code"]]
            if self.validate_testcase(result, expected_codes, testcase, failures):
                self.log.debug("Creation Successful")
                self.log.debug("Response: {}".format(result.json()))
                response_data = result.json()
                temp_user_id = response_data.get("id")
                if temp_user_id:
                    self.log.debug("Cleaning up admin user with ID: {}".format(temp_user_id))
                    self.flush_appservices_admin_user(
                        self.project_id, self.cluster_id, self.app_service_id,
                        temp_user_id
                    )
                else:
                    self.log.warning(
                        "No 'id' in response; skipping cleanup. Response: {}".format(
                            response_data))

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.add_app_service_admin_user, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.app_service_id,
                self.expected_res
            )
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.add_app_service_admin_user, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.app_service_id,
                self.expected_res
            )
        ]]
        self.throttle_test(api_func_list, True, self.project_id)
