"""
Created on September 4, 2023

@author: Vipul Bhardwaj
"""

import copy
from pytests.Capella.RestAPIv4.api_base import APIBase
import time
import base64


class CreateProject(APIBase):

    def setUp(self):
        APIBase.setUp(self)

        # Create project.
        # The project ID will be used to create API keys for roles that
        # require project ID
        self.project_name = self.prefix + 'Post'
        self.project_id = self.capellaAPI.org_ops_apis.create_project(
            self.organisation_id, self.project_name,
            self.generate_random_string(100, prefix=self.prefix)).json()["id"]

    def tearDown(self):
        failures = list()
        self.update_auth_with_api_token(self.org_owner_key["token"])
        self.delete_api_keys(self.api_keys)

        # Delete the project that was created in setUp.
        if self.delete_projects(self.organisation_id, [self.project_id],
                                self.org_owner_key["token"]):
            failures.append("Error while deleting project.")
        else:
            self.log.info("Project deleted successfully")

        if failures:
            self.log.error("Following error occurred in teardown: {}"
                           .format(failures))
        super(CreateProject, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Create Project via valid path"
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
                    "hint": "Check if all the required params are present "
                            "in the request body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived"
                               " to be a client error."
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
                org, self.project_name)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.create_project(
                    org, self.project_name)
            if result.status_code == 201:
                project_id = result.json()['id']
                if "expected_error" in testcase:
                    self.log.error(testcase["description"])
                    failures.append(testcase["description"])
                else:
                    self.log.debug("Creation Successful.")
                    res = self.capellaAPI.org_ops_apis.delete_project(
                        self.organisation_id, project_id)
                    if res.status_code == 429:
                        self.handle_rate_limit(int(res.headers["Retry-After"]))
                    while res.status_code != 204:
                        res = self.capellaAPI.org_ops_apis.delete_project(
                            self.organisation_id, project_id)
            elif result.status_code >= 500:
                self.log.critical(testcase["description"])
                self.log.warning(result.content)
                failures.append(testcase["description"])
                continue
            elif result.status_code == testcase["expected_status_code"]:
                try:
                    result = result.json()
                    for key in result:
                        if result[key] != testcase["expected_error"][key]:
                            self.log.error("Status != 200, Key validation "
                                           "Failure : {}".format(
                                            testcase["description"]))
                            self.log.warning("Result : {}".format(result))
                            failures.append(testcase["description"])
                            break
                except (Exception, ):
                    if str(testcase["expected_error"]) not in result.content:
                        self.log.error("Response type not JSON, Failure : {}"
                                       .format(testcase["description"]))
                        self.log.warning(result.content)
                        failures.append(testcase["description"])
            else:
                self.log.error("Expected HTTP status code {}, Actual "
                               "HTTP status code {}".format(
                                testcase["expected_status_code"],
                                result.status_code))
                self.log.warning("Result : {}".format(result.content))
                failures.append(testcase["description"])
            self.capellaAPI.org_ops_apis.project_endpoint = \
                "/v4/organizations/{}/projects"

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests".format(
                len(failures), len(testcases)))

    def test_authorization(self):
        self.api_keys.update(
            self.create_api_keys_for_all_combinations_of_roles(
                [self.project_id]))

        testcases = []
        for role in self.api_keys:
            testcase = {
                "description": "Calling API with {} role".format(role),
                "token": self.api_keys[role]["token"]
            }
            if not any(element in ["organizationOwner", "projectCreator"] for
                       element in self.api_keys[role]["roles"]):
                testcase["expected_error"] = {
                    "code": 1003,
                    "hint": "Make sure you have adequate access to the "
                            "resource.",
                    "message": "Access Denied.",
                    "httpStatusCode": 403
                }
                testcase["expected_status_code"] = 403
            testcases.append(testcase)
        testcases.extend([
            {
                "description": "Calling API without bearer token",
                "token": "",
                "expected_status_code": 401,
                "expected_error": {
                    "code": 1001,
                    "hint": "The request is unauthorized. Please ensure you "
                            "have provided appropriate credentials in the "
                            "request header. Please make sure the client IP "
                            "that is trying to access the resource using the "
                            "API key is in the API key allowlist.",
                    "httpStatusCode": 401,
                    "message": "Unauthorized"
                }
            }, {
                "description": "calling API with expired API keys",
                "expire_key": True,
                "expected_status_code": 401,
                "expected_error": {
                    "code": 1001,
                    "hint": "The request is unauthorized. Please ensure you "
                            "have provided appropriate credentials in the "
                            "request header. Please make sure the client IP "
                            "that is trying to access the resource using the "
                            "API key is in the API key allowlist.",
                    "httpStatusCode": 401,
                    "message": "Unauthorized"
                }
            }, {
                "description": "calling API with revoked API keys",
                "revoke_key": True,
                "expected_status_code": 401,
                "expected_error": {
                    "code": 1001,
                    "hint": "The request is unauthorized. Please ensure you "
                            "have provided appropriate credentials in the "
                            "request header. Please make sure the client IP "
                            "that is trying to access the resource using the "
                            "API key is in the API key allowlist.",
                    "httpStatusCode": 401,
                    "message": "Unauthorized"
                }
            }, {
                "description": "Calling API with Username and Password",
                "userpwd": True,
                "expected_status_code": 401,
                "expected_error": {
                    "code": 1001,
                    "hint": "The request is unauthorized. Please ensure you "
                            "have provided appropriate credentials in the "
                            "request header. Please make sure the client IP "
                            "that is trying to access the resource using the "
                            "API key is in the API key allowlist.",
                    "httpStatusCode": 401,
                    "message": "Unauthorized"
                }
            }
        ])

        failures = list()
        header = dict()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))

            if "expire_key" in testcase:
                self.update_auth_with_api_token(self.org_owner_key["token"])
                # create a new API key with expiry of approx 2 mins
                resp = self.capellaAPI.org_ops_apis.create_api_key(
                    organizationId=self.organisation_id,
                    name=self.generate_random_string(prefix=self.prefix),
                    description=self.generate_random_string(50),
                    organizationRoles=["organizationOwner"],
                    expiry=0.001
                )
                if resp.status_code == 201:
                    self.api_keys["organizationOwner_new"] = resp.json()
                else:
                    self.fail("Error while creating API key for organization "
                              "owner with expiry of 0.001 days")
                # wait for key to expire
                self.log.debug("Waiting 3 minutes for key expiry")
                time.sleep(180)
                self.update_auth_with_api_token(
                    self.api_keys["organizationOwner_new"]["token"])
                del self.api_keys["organizationOwner_new"]
            elif "revoke_key" in testcase:
                self.update_auth_with_api_token(self.org_owner_key["token"])
                resp = self.capellaAPI.org_ops_apis.delete_api_key(
                    organizationId=self.organisation_id,
                    accessKey=self.api_keys["organizationOwner"]["id"])
                if resp.status_code != 204:
                    failures.append(testcase["description"])
                self.update_auth_with_api_token(
                    self.api_keys["organizationOwner"]["token"])
                del self.api_keys["organizationOwner"]
            elif "userpwd" in testcase:
                basic = base64.b64encode("{}:{}".format(
                    self.user, self.passwd).encode()).decode()
                header["Authorization"] = 'Basic {}'.format(basic)
            else:
                header = {}
                self.update_auth_with_api_token(testcase["token"])

            result = self.capellaAPI.org_ops_apis.create_project(
                self.organisation_id, self.project_name, headers=header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.create_project(
                    self.organisation_id, self.project_name, headers=header)
            if result.status_code == 201:
                project_id = result.json()['id']
                if "expected_error" in testcase:
                    self.log.error(testcase["description"])
                    failures.append(testcase["description"])
                else:
                    self.log.debug("Creation Successful.")
                    res = self.capellaAPI.org_ops_apis.delete_project(
                        self.organisation_id, project_id)
                    if res.status_code == 429:
                        self.handle_rate_limit(int(res.headers["Retry-After"]))
                    while res.status_code != 204:
                        res = self.capellaAPI.org_ops_apis.delete_project(
                            self.organisation_id, project_id)
            elif result.status_code >= 500:
                self.log.critical(testcase["description"])
                self.log.warning(result.content)
                failures.append(testcase["description"])
                continue
            elif result.status_code == testcase["expected_status_code"]:
                try:
                    result = result.json()
                    for key in result:
                        if result[key] != testcase["expected_error"][key]:
                            self.log.error("Status != 200, Key validation "
                                           "Error : {}".format(
                                            testcase["description"]))
                            self.log.warning("Failure : {}".format(result))
                            failures.append(testcase["description"])
                            break
                except (Exception,):
                    if str(testcase["expected_error"]) not in \
                            result.content:
                        self.log.error("Response type not JSON, Failure : {}"
                                       .format(testcase["description"]))
                        self.log.warning(result.content)
                        failures.append(testcase["description"])
            else:
                self.log.error("Expected HTTP status code {}, Actual "
                               "HTTP status code {}".format(
                                testcase["expected_status_code"],
                                result.status_code))
                self.log.warning("Result : {}".format(result.content))
                failures.append(testcase["description"])

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests".format(
                len(failures), len(testcases)))

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
                testcase["organizationID"], self.project_name, **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.create_project(
                    testcase["organizationID"], self.project_name, **kwarg)
            if result.status_code == 201:
                project_id = result.json()['id']
                if "expected_error" in testcase:
                    self.log.error(testcase["description"])
                    failures.append(testcase["description"])
                else:
                    self.log.debug("Creation Successful.")
                    res = self.capellaAPI.org_ops_apis.delete_project(
                        self.organisation_id, project_id)
                    if res.status_code == 429:
                        self.handle_rate_limit(int(res.headers["Retry-After"]))
                    while res.status_code != 204:
                        res = self.capellaAPI.org_ops_apis.delete_project(
                            self.organisation_id, project_id)
            elif result.status_code >= 500:
                self.log.critical(testcase["description"])
                self.log.warning(result.content)
                failures.append(testcase["description"])
                continue
            elif result.status_code == testcase["expected_status_code"]:
                try:
                    result = result.json()
                    for key in result:
                        if result[key] != testcase["expected_error"][key]:
                            self.log.error("Status != 200, Err validation "
                                           "Failure : {}".format(
                                            testcase["description"]))
                            self.log.warning("Result : {}".format(result))
                            failures.append(testcase["description"])
                            break
                except (Exception,):
                    if str(testcase["expected_error"]) not in \
                            result.content:
                        self.log.error(
                            "Response type not JSON, Failure : {}".format(
                                testcase["description"]))
                        self.log.warning(result.content)
                        failures.append(testcase["description"])
            else:
                self.log.error("Expected HTTP status code {}, Actual "
                               "HTTP status code {}".format(
                                testcase["expected_status_code"],
                                result.status_code))
                self.log.warning("Result : {}".format(result.content))
                failures.append(testcase["description"])

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests".format(
                len(failures), len(testcases)))

    def test_payload(self):
        expected_result = {
            "id": self.project_id,
            "description": "",
            "name": self.project_name,
            "audit": {
                "createdBy": None,
                "createdAt": None,
                "modifiedBy": None,
                "modifiedAt": None,
                "version": None
            }
        }
        testcases = list()

        for key in expected_result:
            if key in ["audit", "id"]:
                continue

            values = [
                "", 1, 0, 100000, -1, 123.123, self.generate_random_string(),
                self.generate_random_string(500, special_characters=False),
            ]
            for value in values:
                testcase = copy.deepcopy(expected_result)
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
                elif key == "name" and len(value) >= 128:
                    testcase["expected_status_code"] = 422
                    testcase["expected_error"] = {
                        "code": 422,
                        "hint": "Please review your request and ensure "
                                "that all required parameters are "
                                "correctly provided.",
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
            if result.status_code == 201:
                project_id = result.json()['id']
                if "expected_error" in testcase:
                    self.log.error(testcase["desc"])
                    failures.append(testcase["desc"])
                else:
                    self.log.debug("Creation Successful.")
                    res = self.capellaAPI.org_ops_apis.delete_project(
                        self.organisation_id, project_id)
                    if res.status_code == 429:
                        self.handle_rate_limit(int(res.headers["Retry-After"]))
                    while res.status_code != 204:
                        res = self.capellaAPI.org_ops_apis.delete_project(
                            self.organisation_id, project_id)
            else:
                if result.status_code >= 500:
                    self.log.critical(testcase["desc"])
                    self.log.warning(result.content)
                    failures.append(testcase["desc"])
                    continue
                if result.status_code == testcase["expected_status_code"]:
                    try:
                        result = result.json()
                        for key in result:
                            if result[key] != testcase["expected_error"][key]:
                                self.log.error("Status != 204, Key validation "
                                               "failed for Test : {}".format(
                                                testcase["desc"]))
                                self.log.warning("Failure : {}".format(result))
                                failures.append(testcase["desc"])
                                break
                    except (Exception,):
                        if str(testcase["expected_error"]) \
                                not in result.content:
                            self.log.error(
                                "Response type not JSON, Test : {}".format(
                                    testcase["description"]))
                            self.log.warning("Failure : {}".format(result))
                            failures.append(testcase["desc"])
                else:
                    self.log.error("Expected HTTP status code {}, Actual "
                                   "HTTP status code {}".format(
                                    testcase["expected_status_code"],
                                    result.status_code))
                    self.log.warning("Result : {}".format(result.content))
                    failures.append(testcase["desc"])

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests".format(
                len(failures), len(testcases)))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[self.capellaAPI.org_ops_apis.create_project,
                          (self.organisation_id, "")]]

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

        if self.input.param("rate_limit", False):
            results = self.make_parallel_api_calls(
                310, api_func_list, self.api_keys)
            for result in results:
                if ((not results[result]["rate_limit_hit"])
                        or results[result][
                            "total_api_calls_made_to_hit_rate_limit"] > 300):
                    self.fail(
                        "Rate limit was hit after {0} API calls. "
                        "This is definitely an issue.".format(
                            results[result][
                                "total_api_calls_made_to_hit_rate_limit"]
                        ))

        results = self.make_parallel_api_calls(
            99, api_func_list, self.api_keys)
        for result in results:
            # Removing failure for tests which are intentionally ran
            # for :
            #   # unauthorized roles, ie, which give a 403 response.
            if "403" in results[result]["4xx_errors"]:
                del results[result]["4xx_errors"]["403"]
            #   # invalid name param, ie, which give a 422 response.
            if "422" in results[result]["4xx_errors"]:
                del results[result]["4xx_errors"]["422"]

            if len(results[result]["4xx_errors"]) > 0 or len(
                    results[result]["5xx_errors"]) > 0:
                self.fail("Some API calls failed")

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[self.capellaAPI.org_ops_apis.create_project,
                          (self.organisation_id, "")]]

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

        if self.input.param("rate_limit", False):
            results = self.make_parallel_api_calls(
                310, api_func_list, self.api_keys)
            for result in results:
                if ((not results[result]["rate_limit_hit"])
                        or results[result][
                            "total_api_calls_made_to_hit_rate_limit"] > 300):
                    self.fail(
                        "Rate limit was hit after {0} API calls. "
                        "This is definitely an issue.".format(
                            results[result][
                                "total_api_calls_made_to_hit_rate_limit"]
                        ))

        results = self.make_parallel_api_calls(
            99, api_func_list, self.api_keys)
        for result in results:
            # Removing failure for tests which are intentionally ran
            # for :
            #   # unauthorized roles, ie, which give a 403 response.
            if "403" in results[result]["4xx_errors"]:
                del results[result]["4xx_errors"]["403"]
            #   # invalid name param, ie, which give a 422 response.
            if "422" in results[result]["4xx_errors"]:
                del results[result]["4xx_errors"]["422"]

            if len(results[result]["4xx_errors"]) > 0 or len(
                    results[result]["5xx_errors"]) > 0:
                self.fail("Some API calls failed")
