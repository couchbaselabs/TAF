"""
Created on September 1, 2023

@author: Vipul Bhardwaj
"""

from pytests.Capella.RestAPIv4.api_base import APIBase


class DeleteProject(APIBase):

    def setUp(self):
        APIBase.setUp(self)

        # Create a test subject resource.
        res = self.capellaAPI.org_ops_apis.create_project(
            self.organisation_id, self.prefix + "WRAPPER")
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
        while res.status_code != 201:
            res = self.capellaAPI.org_ops_apis.create_project(
                self.organisation_id, self.prefix + "WRAPPER")
        self.test_subject_id = res.json()['id']

    def tearDown(self):
        self.update_auth_with_api_token(self.curr_owner_key)

        # Delete the created Test Subject
        self.log.debug("Deleting test subject -> Project: {}"
                       .format(self.project_id))
        res = self.capellaAPI.org_ops_apis.delete_project(
            self.organisation_id, self.test_subject_id)
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.org_ops_apis.delete_project(
                self.organisation_id, self.test_subject_id)
        if res.status_code != 204:
            self.fail("Error: {}".format(res.content))
        self.log.info("Test Subject deleted successfully.")

        super(DeleteProject, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Delete a valid project"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects",
                "expected_status_code": 404,
                "expected_error": {
                    "errorType": "RouteNotFound",
                    "message": "Not found"
                }
            }, {
                "description": "Replace projects with project in URI",
                "url": "/v4/organizations/{}/project",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/project",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Fetch project but with non-hex organizationID",
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
                "description": "Fetch project but with non-hex projectID",
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
            }
        ]
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            org = self.organisation_id
            proj = self.test_subject_id

            if "url" in testcase:
                self.capellaAPI.org_ops_apis.project_endpoint = testcase["url"]
            if "invalid_organizationID" in testcase:
                org = testcase["invalid_organizationID"]
            elif "invalid_projectID" in testcase:
                proj = testcase["invalid_projectID"]

            result = self.capellaAPI.org_ops_apis.delete_project(org, proj)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.delete_project(org, proj)
            self.capellaAPI.org_ops_apis.project_endpoint = \
                "/v4/organizations/{}/projects"
            if self.validate_testcase(result, [204], testcase, failures):
                self.log.debug("Deletion Successful.")
                # Create a test subject resource.
                res = self.capellaAPI.org_ops_apis.create_project(
                    self.organisation_id, self.prefix + "WRAPPER")
                if res.status_code == 429:
                    self.handle_rate_limit(int(res.headers["Retry-After"]))
                    res = self.capellaAPI.org_ops_apis.create_project(
                        self.organisation_id, self.prefix + "WRAPPER")
                if res.status_code != 201:
                    self.fail("Error while creating TEST PROJECT.")
                self.test_subject_id = res.json()['id']

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        """
        Skipping projectOwner even through that can delete projects as the
        initial projectID is used to build all the keys and after a
        successful deletion a new project doesn't have the same access config.
        """
        failures = list()
        for testcase in self.v4_RBAC_injection_init(["organizationOwner"],
                                                    None):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.test_subject_id, self.other_project_id)
            result = self.capellaAPI.org_ops_apis.delete_project(
                self.organisation_id, self.test_subject_id, header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.delete_project(
                    self.organisation_id, self.project_id, header)
            if self.validate_testcase(result, [204], testcase, failures):
                self.log.debug("Deletion Successful.")
                # Create a test subject resource.
                res = self.capellaAPI.org_ops_apis.create_project(
                    self.organisation_id, self.prefix + "WRAPPER")
                if res.status_code == 429:
                    self.handle_rate_limit(int(res.headers["Retry-After"]))
                    res = self.capellaAPI.org_ops_apis.create_project(
                        self.organisation_id, self.prefix + "WRAPPER")
                if res.status_code != 201:
                    self.fail("Error while creating TEST PROJECT.")
                self.test_subject_id = res.json()['id']

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_query_parameters(self):
        self.log.debug("Correct Params - OrgID: {}, ProjID: {}"
                       .format(self.organisation_id, self.test_subject_id))
        og_project_id = self.test_subject_id
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.test_subject_id):
            testcases += 1
            testcase = {
                "description": "OrganizationID: {}, ProjectID: {}"
                .format(str(combination[0]), str(combination[1])),
                "organizationID": combination[0],
                "projectID": combination[1],
            }
            if not (combination[0] == self.organisation_id and
                    combination[1] == self.test_subject_id):
                if combination[1] == "" or combination[0] == "":
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = "404 page not found"
                elif any(variable in [
                    int, bool, float, list, tuple, set, type(None)] for
                         variable in [
                             type(combination[0]), type(combination[1])]):
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
                        "message": "Access Denied.",
                        "httpStatusCode": 403
                    }
                else:
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = {
                        "code": 2000,
                        "hint": "Check if the project ID is valid.",
                        "httpStatusCode": 404,
                        "message": "The server cannot find a project by its "
                                   "ID."
                    }
            # to handle the deleted project ID to be replaced by the
            # new project ID in the first run of second orgID
            # combination
            if testcase["projectID"] == og_project_id:
                testcase["projectID"] = self.test_subject_id

            self.log.info("Executing test: {}".format(testcase["description"]))
            if "param" in testcase:
                kwarg = {testcase["param"]: testcase["paramValue"]}
            else:
                kwarg = dict()

            result = self.capellaAPI.org_ops_apis.delete_project(
                testcase["organizationID"], testcase["projectID"], **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.delete_project(
                    testcase["organizationID"], testcase["projectID"], **kwarg)
            if self.validate_testcase(result, [204], testcase, failures):
                self.log.debug("Deletion Successful.")
                # Create a test subject resource.
                res = self.capellaAPI.org_ops_apis.create_project(
                    self.organisation_id, self.prefix + "WRAPPER")
                if res.status_code == 429:
                    self.handle_rate_limit(int(res.headers["Retry-After"]))
                    res = self.capellaAPI.org_ops_apis.create_project(
                        self.organisation_id, self.prefix + "WRAPPER")
                if res.status_code != 201:
                    self.fail("Error while creating TEST PROJECT.")
                self.test_subject_id = res.json()['id']

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[self.capellaAPI.org_ops_apis.delete_project,
                          (self.organisation_id,
                           self.replace_last_character(self.test_subject_id))]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[self.capellaAPI.org_ops_apis.delete_project,
                          (self.organisation_id,
                           self.replace_last_character(self.test_subject_id))]]
        self.throttle_test(api_func_list, True, self.project_id)
