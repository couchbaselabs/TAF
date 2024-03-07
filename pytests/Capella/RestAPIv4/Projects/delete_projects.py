"""
Created on September 1, 2023

@author: Vipul Bhardwaj
"""

from pytests.Capella.RestAPIv4.api_base import APIBase


class DeleteProject(APIBase):

    def setUp(self):
        APIBase.setUp(self)

        # Create project.
        # The project ID will be used to create API keys for roles that
        # require project ID
        self.project_name = self.prefix + 'Projects_Delete'
        self.project_id = self.capellaAPI.org_ops_apis.create_project(
            self.organisation_id, self.project_name,
            self.generate_random_string(0, self.prefix)).json()["id"]
        self.expected_result = {
            "id": self.project_id,
            "description": None,
            "name": self.project_name,
            "audit": {
                "createdBy": None,
                "createdAt": None,
                "modifiedBy": None,
                "modifiedAt": None,
                "version": None
            }
        }

    def tearDown(self):
        self.update_auth_with_api_token(self.org_owner_key["token"])
        self.delete_api_keys(self.api_keys)

        # Delete the project that was created.
        self.log.info("Deleting Project: {}".format(self.project_id))
        if self.delete_projects(self.organisation_id, [self.project_id],
                                self.org_owner_key["token"]):
            self.log.error("Error while deleting project.")
        else:
            self.log.info("Project deleted successfully")

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
            proj = self.project_id

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
                res = self.capellaAPI.org_ops_apis.create_project(
                    self.organisation_id, self.project_name)
                if res.status_code == 429:
                    self.handle_rate_limit(int(res.headers["Retry-After"]))
                while res.status_code != 201:
                    res = self.capellaAPI.org_ops_apis.create_project(
                        self.organisation_id, self.project_name)
                self.project_id = res.json()['id']

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        self.api_keys.update(
            self.create_api_keys_for_all_combinations_of_roles(
                [self.project_id]))

        resp = self.capellaAPI.org_ops_apis.create_project(
            organizationId=self.organisation_id,
            name=self.generate_random_string(prefix=self.prefix),
            description=self.generate_random_string(100))
        if resp.status_code == 201:
            other_project_id = resp.json()["id"]
        else:
            self.fail("Error while creating project")

        testcases = []
        for role in self.api_keys:
            testcase = {
                "description": "Calling API with {} role".format(role),
                "token": self.api_keys[role]["token"]
            }
            # Skipping projectOwner even through that can delete
            # projects as the initial projectID is used to build all the
            # keys and after a successful deletion a new project doesn't
            # have the same access config.
            if not any(element in ["organizationOwner"] for
                       element in self.api_keys[role]["roles"]):
                testcase["expected_error"] = {
                    "code": 1002,
                    "hint": "Your access to the requested resource is denied. "
                            "Please make sure you have the necessary "
                            "permissions to access the resource.",
                    "message": "Access Denied.",
                    "httpStatusCode": 403
                }
                testcase["expected_status_code"] = 403
            testcases.append(testcase)
        self.auth_test_extension(testcases, other_project_id)

        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, other_project_id)
            result = self.capellaAPI.org_ops_apis.delete_project(
                self.organisation_id, self.project_id, header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.delete_project(
                    self.organisation_id, self.project_id, header)

            if self.validate_testcase(result, [204], testcase, failures):
                self.log.debug("Deletion Successful.")
                res = self.capellaAPI.org_ops_apis.create_project(
                    self.organisation_id, self.project_name)
                if res.status_code == 429:
                    self.handle_rate_limit(int(res.headers["Retry-After"]))
                while res.status_code != 201:
                    res = self.capellaAPI.org_ops_apis.create_project(
                        self.organisation_id, self.project_name)
                self.project_id = res.json()['id']

        self.update_auth_with_api_token(self.org_owner_key["token"])
        resp = self.capellaAPI.org_ops_apis.delete_project(
            self.organisation_id, other_project_id)
        if resp.status_code != 204:
            self.log.error("Error while deleting project {}"
                           .format(other_project_id))

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_query_parameters(self):
        self.log.debug("Correct Params - OrgID: {}, ProjID: {}"
                       .format(self.organisation_id, self.project_id))
        og_project_id = self.project_id
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id):
            testcases += 1
            testcase = {
                "description": "OrganizationID: {}, ProjectID: {}"
                .format(str(combination[0]), str(combination[1])),
                "organizationID": combination[0],
                "projectID": combination[1],
            }
            if not (combination[0] == self.organisation_id and
                    combination[1] == self.project_id):
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
                testcase["projectID"] = self.project_id

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
                res = self.capellaAPI.org_ops_apis.create_project(
                    self.organisation_id, self.project_name)
                if res.status_code == 429:
                    self.handle_rate_limit(int(res.headers["Retry-After"]))
                while res.status_code != 201:
                    res = self.capellaAPI.org_ops_apis.create_project(
                        self.organisation_id, self.project_name)
                self.project_id = res.json()['id']

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[self.capellaAPI.org_ops_apis.delete_project,
                          (self.organisation_id,
                           self.replace_last_character(self.project_id))]]

        for i in range(self.input.param("num_api_keys", 1)):
            resp = self.capellaAPI.org_ops_apis.create_api_key(
                self.organisation_id,
                self.generate_random_string(prefix=self.prefix),
                ["organizationOwner"], self.generate_random_string(50))
            if resp.status_code == 429:
                self.handle_rate_limit(int(resp.headers["Retry-After"]))
                resp = self.capellaAPI.org_ops_apis.create_api_key(
                    self.organisation_id,
                    self.generate_random_string(prefix=self.prefix),
                    ["organizationOwner"], self.generate_random_string(50))
            if resp.status_code == 201:
                self.api_keys["organizationOwner_{}".format(i)] = resp.json()
            else:
                self.fail("Error while creating API key for "
                          "organizationOwner_{}".format(i))

        results = self.throttle_test(api_func_list, self.api_keys)
        for result in results:
            # Removing failure for tests which are intentionally ran for
            # unauthorized roles, ie, which give a 403 response.
            if "403" in results[result]["4xx_errors"]:
                del results[result]["4xx_errors"]["403"]
            if "404" in results[result]["4xx_errors"]:
                del results[result]["4xx_errors"]["404"]

            if len(results[result]["4xx_errors"]) > 0 or len(
                    results[result]["5xx_errors"]) > 0:
                self.fail("Some API calls failed")

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[self.capellaAPI.org_ops_apis.delete_project,
                          (self.organisation_id,
                           self.replace_last_character(self.project_id))]]

        org_roles = self.input.param("org_roles", "organizationOwner")
        proj_roles = self.input.param("proj_roles", "projectDataReader")
        org_roles = org_roles.split(":")
        proj_roles = proj_roles.split(":")

        api_key_dict = self.create_api_keys_for_all_combinations_of_roles(
            [self.project_id], proj_roles, org_roles)
        for i, api_key in enumerate(api_key_dict):
            if api_key in self.api_keys:
                self.api_keys["{}_{}".format(api_key_dict[api_key], i)] = \
                    api_key_dict[api_key]
            else:
                self.api_keys[api_key] = api_key_dict[api_key]

        results = self.throttle_test(api_func_list, self.api_keys)
        for result in results:
            # Removing failure for tests which are intentionally ran for
            # unauthorized roles, ie, which give a 403 response.
            if "403" in results[result]["4xx_errors"]:
                del results[result]["4xx_errors"]["403"]
            if "404" in results[result]["4xx_errors"]:
                del results[result]["4xx_errors"]["404"]

            if len(results[result]["4xx_errors"]) > 0 or len(
                    results[result]["5xx_errors"]) > 0:
                self.fail("Some API calls failed")
