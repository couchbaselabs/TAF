"""
Created on September 4, 2023

@author: Vipul Bhardwaj
"""

import copy
from pytests.Capella.RestAPIv4.Projects.get_projects import GetProject


class CreateProject(GetProject):

    def setUp(self, nomenclature="Project_Create"):
        GetProject.setUp(self)

    def tearDown(self):
        self.update_auth_with_api_token(self.curr_owner_key)
        super(CreateProject, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Create Project via valid path"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }, {
                "description": "Replace projects with project in URI",
                "url": "/v4/organizations/{}/project",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/project",
                "expected_status_code": 405,
                "expected_error": ""
            }, {
                "description": "Create project but with non-hex "
                               "organizationID",
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
            }
        ]
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            org = self.organisation_id

            if "url" in testcase:
                self.capellaAPI.org_ops_apis.project_endpoint = testcase["url"]
            if "invalid_organizationID" in testcase:
                org = testcase["invalid_organizationID"]

            result = self.capellaAPI.org_ops_apis.create_project(
                org, self.prefix + "CREATE")
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.create_project(
                    org, self.prefix + "CREATE")
            self.capellaAPI.org_ops_apis.project_endpoint = \
                "/v4/organizations/{}/projects"
            self.validate_testcase(result, [201], testcase, failures)
            if result.status_code == 201:
                project_id = result.json()["id"]
                self.log.debug("Creation Successful.")
                self.log.info("Deleting Project: {}".format(project_id))
                res = self.capellaAPI.org_ops_apis.delete_project(
                    self.organisation_id, project_id)
                if res.status_code == 429:
                    self.handle_rate_limit(int(res.headers["Retry-After"]))
                    res = self.capellaAPI.org_ops_apis.delete_project(
                        self.organisation_id, project_id)
                if res.status_code != 204:
                    self.fail("Error: {}".format(res.content))
                self.log.debug("Project CleanUp Successful.")

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectCreator"
        ], None):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header, self.project_id)
            result = self.capellaAPI.org_ops_apis.create_project(
                self.organisation_id, self.prefix + "CREATE",
                headers=header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.create_project(
                    self.organisation_id, self.prefix + "CREATE",
                    headers=header)
            self.validate_testcase(result, [201], testcase, failures)
            if result.status_code == 201:
                project_id = result.json()["id"]
                self.log.debug("Creation Successful.")
                self.log.info("Deleting Project: {}".format(project_id))
                res = self.capellaAPI.org_ops_apis.delete_project(
                    self.organisation_id, project_id)
                if res.status_code == 429:
                    self.handle_rate_limit(int(res.headers["Retry-After"]))
                    res = self.capellaAPI.org_ops_apis.delete_project(
                        self.organisation_id, project_id)
                if res.status_code != 204:
                    self.fail("Error: {}".format(res.content))
                self.log.debug("Project CleanUp Successful.")

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_query_parameters(self):
        self.log.debug("Correct Params - OrgID: {}"
                       .format(self.organisation_id))
        organization_id_values = [
            self.organisation_id,
            self.replace_last_character(self.organisation_id),
            True,
            123456789,
            123456789.123456789,
            "",
            (self.organisation_id,),
            [self.organisation_id],
            {self.organisation_id},
            None
        ]

        testcases = list()
        for value in organization_id_values:
            testcase = {
                "description": "OrganizationID: {}".format(str(value)),
                "organizationID": value
            }
            if not (value == self.organisation_id):
                if value == "":
                    testcase["expected_status_code"] = 405
                    testcase["expected_error"] = ""
                elif type(value) in [int, bool, float, list, tuple, set,
                                     type(None)]:
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
                else:
                    testcase["expected_status_code"] = 403
                    testcase["expected_error"] = {
                        "code": 1002,
                        "hint": "Your access to the requested resource is "
                                "denied. Please make sure you have the "
                                "necessary permissions to access the "
                                "resource.",
                        "message": "Access Denied.",
                        "httpStatusCode": 403
                    }
            testcases.append(testcase)

        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            if "param" in testcase:
                kwarg = {testcase["param"]: testcase["paramValue"]}
            else:
                kwarg = dict()

            result = self.capellaAPI.org_ops_apis.create_project(
                testcase["organizationID"], self.prefix + "CREATE",
                **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.create_project(
                    testcase["organizationID"], self.prefix + "CREATE",
                    **kwarg)
            self.validate_testcase(result, [201], testcase, failures)
            if result.status_code == 201:
                project_id = result.json()["id"]
                self.log.debug("Creation Successful.")
                self.log.info("Deleting Project: {}".format(project_id))
                res = self.capellaAPI.org_ops_apis.delete_project(
                    self.organisation_id, project_id)
                if res.status_code == 429:
                    self.handle_rate_limit(int(res.headers["Retry-After"]))
                    res = self.capellaAPI.org_ops_apis.delete_project(
                        self.organisation_id, project_id)
                if res.status_code != 204:
                    self.fail("Error: {}".format(res.content))
                self.log.debug("Project CleanUp Successful.")

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_payload(self):
        testcases = list()
        for key in self.expected_res:
            if key in ["audit", "id"]:
                continue
            values = [
                "", 1, 0, 100000, -1, 123.123, self.generate_random_string(special_characters=False),
                self.generate_random_string(500, special_characters=False),
            ]
            for value in values:
                testcase = copy.deepcopy(self.expected_res)
                testcase[key] = value
                testcase["desc"] = "Testing '{}' with value: {}".format(
                    key, str(value))
                testcase["expected_status_code"] = 201
                if key == "description" and value == 0:
                    testcases.append(testcase)
                    continue
                if not isinstance(value, str):
                    testcase["expected_status_code"] = 400
                    testcase["expected_error"] = {
                        "code": 1000,
                        "hint": "The request was malformed or invalid.",
                        "httpStatusCode": 400,
                        "message": 'Bad Request. Error: body contains '
                                   'incorrect JSON type for field "{}".'
                        .format(key)
                    }
                elif key == "description" and len(value) > 256:
                    testcase["expected_status_code"] = 422
                    testcase["expected_error"] = {
                        "code": 2003,
                        "hint": "Retry with a description with a maximum "
                                "length 256 characters.",
                        "httpStatusCode": 422,
                        "message": "Unable to save project. The description "
                                   "provided exceeds 256 characters."
                    }
                elif key == "name" and len(value) >= 128:
                    testcase["expected_status_code"] = 422
                    testcase["expected_error"] = {
                        "code": 2004,
                        "hint": "Retry with a name with a maximum length 128 "
                                "characters.",
                        "httpStatusCode": 422,
                        "message": "Unable to save project. A project name "
                                   "must be less than 128 characters."
                    }
                elif key == "name" and value == "":
                    testcase["expected_status_code"] = 422
                    testcase["expected_error"] = {
                        "code": 422,
                        "hint": "Please review your request and ensure that "
                                "all required parameters are correctly "
                                "provided.",
                        "httpStatusCode": 422,
                        "message": "Unable to save project. A project name is "
                                   "required. Please provide a project name."
                    }
                testcases.append(testcase)

        failures = list()
        for testcase in testcases:
            self.log.info(testcase['desc'])

            result = self.capellaAPI.org_ops_apis.create_project(
                self.organisation_id, testcase["name"],
                testcase["description"])
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.create_project(
                    self.organisation_id, testcase["name"],
                    testcase["description"])
            self.validate_testcase(result, [201], testcase, failures,
                                   payloadTest=True)
            if result.status_code == 201:
                project_id = result.json()["id"]
                self.log.debug("Creation Successful.")
                self.log.info("Deleting Project: {}".format(project_id))
                res = self.capellaAPI.org_ops_apis.delete_project(
                    self.organisation_id, project_id)
                if res.status_code == 429:
                    self.handle_rate_limit(int(res.headers["Retry-After"]))
                    res = self.capellaAPI.org_ops_apis.delete_project(
                        self.organisation_id, project_id)
                if res.status_code != 204:
                    self.fail("Error: {}".format(res.content))
                self.log.debug("Project CleanUp Successful.")

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[self.capellaAPI.org_ops_apis.create_project,
                          (self.organisation_id, "")]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[self.capellaAPI.org_ops_apis.create_project,
                          (self.organisation_id, "")]]
        self.throttle_test(api_func_list, True, self.project_id)
