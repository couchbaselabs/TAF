"""
Created on June 28, 2023

@author: Vipul Bhardwaj
"""

import base64
from pytests.Capella.RestAPIv4.api_base import APIBase
import time


class GetOrganization(APIBase):

    def setUp(self):
        APIBase.setUp(self)

        organisation_name = self.input.capella.get("tenant_name")
        # Create project.
        # The project ID will be used to create API keys for roles that
        # require project ID
        project_name = self.generate_random_string(prefix=self.prefix)
        project_description = self.generate_random_string(
            100, prefix=self.prefix)
        self.project_id = self.capellaAPI.org_ops_apis.create_project(
            organizationId=self.organisation_id,
            name=project_name,
            description=project_description).json()["id"]

        self.expected_result = {
            "id": self.organisation_id,
            "name": organisation_name,
            "description": None,
            "preferences": {
                "sessionDuration": None
            },
            "audit": {
                "createdBy": None,
                "createdAt": None,
                "modifiedBy": None,
                "modifiedAt": None,
                "version": None
            }
        }

    def tearDown(self):
        failures = list()
        self.update_auth_with_api_token(self.org_owner_key["token"])
        self.delete_api_keys(self.api_keys)

        # Delete the project that was created.
        if self.delete_projects(self.organisation_id, [self.project_id],
                                self.org_owner_key["token"]):
            failures.append("Error while deleting project {}".format(
                self.project_id))

        if failures:
            self.fail("Following error occurred in teardown: {}".format(
                failures))
        super(GetOrganization, self).tearDown()

    def validate_org_api_response(self, expected_resp, actual_resp):
        for key in expected_resp:
            if key not in actual_resp:
                return False
            elif isinstance(expected_resp[key], dict):
                self.validate_org_api_response(
                    expected_resp[key], actual_resp[key])
            elif expected_resp[key]:
                if expected_resp[key] != actual_resp[key]:
                    return False
        return True

    def test_api_path(self):
        testcases = [
            {
                "description": "Fetch info for a valid organization"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations",
                "expected_status_code": 404,
                "expected_error": {
                    "errorType": "RouteNotFound",
                    "message": "Not found"
                }
            }, {
                "description": "Replace organizations with organization in "
                               "URI",
                "url": "/v4/organization",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Replace organizations with organisations in "
                               "URI",
                "url": "/v4/organisations",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/organization",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }
        ]

        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            if "url" in testcase:
                self.capellaAPI.org_ops_apis.organization_endpoint = \
                    testcase["url"]

            result = self.capellaAPI.org_ops_apis.fetch_organization_info(
                self.organisation_id)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.fetch_organization_info(
                    self.organisation_id)
            if result.status_code == 200 and "expected_error" not in testcase:
                if not self.validate_org_api_response(
                        self.expected_result, result.json()):
                    self.log.error("Status == 200, Key validation Failure "
                                   ": {}".format(testcase["description"]))
                    failures.append(testcase["description"])
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
                            self.log.warning("Result: {}".format(result))
                            failures.append(testcase["description"])
                            break
                except (Exception, ):
                    if str(testcase["expected_error"]) not in \
                            result.content:
                        self.log.error(
                            "Response type not JSON, Failure : {}".format(
                                testcase["description"]))
                        self.log.warning("Result: {}".format(result))
                        failures.append(testcase["description"])
            else:
                self.log.error("Expected HTTP status code {}, Actual "
                               "HTTP status code {}".format(
                                testcase["expected_status_code"],
                                result.status_code))
                self.log.warning("Result: {}".format(result))
                failures.append(testcase["description"])
            self.capellaAPI.org_ops_apis.organization_endpoint = \
                "/v4/organizations"

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
                "token": self.api_keys[role]["token"],
            }
            if not any(element in ["organizationOwner", "organizationMember",
                                   "projectCreator", "projectViewer",
                                   "projectDataReaderWriter", "projectManager",
                                   "projectOwner", "projectDataReader"] for
                       element in self.api_keys[role]["roles"]):
                testcase["expected_status_code"] = 403,
                testcase["expected_error"] = {
                    "code": 1003,
                    "hint": "Make sure you have adequate access to the "
                            "resource.",
                    "message": "Access Denied.",
                    "httpStatusCode": 403
                }
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
                    description=self.generate_random_string(
                        50, prefix=self.prefix),
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

            result = self.capellaAPI.org_ops_apis.fetch_organization_info(
                self.organisation_id, header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.fetch_organization_info(
                    self.organisation_id, header)
            if result.status_code == 200 and "expected_error" not in testcase:
                if not self.validate_org_api_response(
                        self.expected_result, result.json()):
                    self.log.error("Status == 200, Key validation Failure "
                                   ": {}".format(testcase["description"]))
                    failures.append(testcase["description"])
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
                        self.log.error(
                            "Response type not JSON, Failure : {}".format(
                                testcase["description"]))
                        self.log.warning("Failure : {}".format(result))
                        failures.append(testcase["description"])
            else:
                self.log.error("Expected HTTP status code {}, Actual "
                               "HTTP status code {}".format(
                                testcase["expected_status_code"],
                                result.status_code))
                self.log.warning("Result : {}".format(result))
                failures.append(testcase["description"])

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests".format(
                len(failures), len(testcases)))

    def test_query_parameters(self):
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
        for val in organization_id_values:
            testcase = {
                "description": "Organization ID : {}".format(str(val)),
                "organizationID": val
            }
            if val == "":
                testcase["expected_status_code"] = 404
                testcase["expected_error"] = "404 page not found"
            elif type(val) in [int, bool, float, list, tuple, set,
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
            elif val != self.organisation_id:
                testcase["expected_status_code"] = 403
                testcase["expected_error"] = {
                    "code": 1002,
                    "hint": "Your access to the requested resource is denied. "
                            "Please make sure you have the necessary "
                            "permissions to access the resource.",
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

            result = self.capellaAPI.org_ops_apis.fetch_organization_info(
                testcase["organizationID"], **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.fetch_organization_info(
                    testcase["organizationID"], **kwarg)
            if result.status_code == 200 and "expected_error" not in testcase:
                if not self.validate_org_api_response(self.expected_result,
                                                      result.json()):
                    self.log.error("Status == 200, Key validation Failure "
                                   ": {}".format(testcase["description"]))
                    failures.append(testcase["description"])
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
                        self.log.warning("Failure : {}".format(result))
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

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[self.capellaAPI.org_ops_apis.fetch_organization_info,
                          (self.organisation_id,)]]

        for i in range(self.input.param("num_api_keys", 1)):
            resp = self.capellaAPI.org_ops_apis.create_api_key(
                self.organisation_id, self.generate_random_string(),
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
            # Removing failure for tests which are intentionally ran for
            # unauthorized roles, ie, which give a 403 response.
            if "403" in results[result]["4xx_errors"]:
                del results[result]["4xx_errors"]["403"]

            if len(results[result]["4xx_errors"]) > 0 or len(
                    results[result]["5xx_errors"]) > 0:
                self.fail("Some API calls failed")

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[self.capellaAPI.org_ops_apis.fetch_organization_info,
                          (self.organisation_id,)]]

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
            # Removing failure for tests which are intentionally ran for
            # unauthorized roles, ie, which give a 403 response.
            if "403" in results[result]["4xx_errors"]:
                del results[result]["4xx_errors"]["403"]

            if len(results[result]["4xx_errors"]) > 0 or len(
                    results[result]["5xx_errors"]) > 0:
                self.fail("Some API calls failed")
