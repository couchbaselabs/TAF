"""
Created on September 1, 2023

@author: Vipul Bhardwaj
"""

from pytests.Capella.RestAPIv4.Projects.get_projects import GetProject


class ListProject(GetProject):

    def setUp(self, nomenclature="Projects_List"):
        GetProject.setUp(self)
        self.expected_res = {
            "data": [
                self.expected_res
            ],
            "cursor": {
                "pages": {
                    "page": None,
                    "next": None,
                    "previous": None,
                    "last": None,
                    "perPage": None,
                    "totalItems": None
                },
                "hrefs": {
                    "first": None,
                    "last": None,
                    "previous": None,
                    "next": None
                }
            }
        }

    def tearDown(self):
        self.update_auth_with_api_token(self.curr_owner_key)
        super(ListProject, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "List Projects via valid paths"
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

            result = self.capellaAPI.org_ops_apis.list_projects(org)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.list_projects(org)
            self.capellaAPI.org_ops_apis.project_endpoint = \
                "/v4/organizations/{}/projects"
            self.validate_testcase(result, [200], testcase, failures, True,
                                   self.expected_res, self.project_id)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectDataReader", "projectOwner",
            "projectDataReaderWriter", "projectViewer", "projectManager"
        ]):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header, self.project_id)
            result = self.capellaAPI.org_ops_apis.list_projects(
                self.organisation_id, headers=header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.list_projects(
                    self.organisation_id, headers=header)
            self.validate_testcase(result, [200], testcase, failures, True,
                                   self.expected_res, self.project_id)

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
                if type(value) in [int, bool, float, list, tuple, set,
                                   type(None)] or value == "":
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

            result = self.capellaAPI.org_ops_apis.list_projects(
                testcase["organizationID"], **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.list_projects(
                    testcase["organizationID"], **kwarg)
            self.validate_testcase(result, [200], testcase, failures, True,
                                   self.expected_res, self.project_id)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[self.capellaAPI.org_ops_apis.list_projects,
                          (self.organisation_id,)]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[self.capellaAPI.org_ops_apis.list_projects,
                          (self.organisation_id,)]]
        self.throttle_test(api_func_list, True, self.project_id)
