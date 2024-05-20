"""
Created on July 28, 2023

@author: Vipul Bhardwaj
"""

from pytests.Capella.RestAPIv4.api_base import APIBase
import time
import base64


class ListOrganization(APIBase):

    def setUp(self):
        APIBase.setUp(self)

        organisation_name = self.input.capella.get("tenant_name")
        # Create project.
        # The project ID will be used to create API keys for roles that
        # require project ID
        self.project_id = self.capellaAPI.org_ops_apis.create_project(
            self.organisation_id, self.prefix + "Organizations_List",
            self.generate_random_string(0, self.prefix)).json()["id"]

        self.expected_result = {
            "data": [
                {
                    "id": self.organisation_id,
                    "name": organisation_name,
                    "description": None,
                    "preferences": {
                        "sessionDuration": None
                    },
                    "subdomain": None,
                    "audit": {
                        "createdBy": None,
                        "createdAt": None,
                        "modifiedBy": None,
                        "modifiedAt": None,
                        "version": None
                    }
                }
            ]
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

        super(ListOrganization, self).tearDown()

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
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/organization",
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
            if "url" in testcase:
                self.capellaAPI.org_ops_apis.organization_endpoint = \
                    testcase["url"]

            result = self.capellaAPI.org_ops_apis.list_organizations()
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.list_organizations()

            self.capellaAPI.org_ops_apis.organization_endpoint = \
                "/v4/organizations"

            self.validate_testcase(result, [200], testcase, failures, True,
                                   self.expected_result, self.organisation_id)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

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
        self.auth_test_extension(testcases, None)

        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header, self.project_id)
            result = self.capellaAPI.org_ops_apis.list_organizations(header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.org_ops_apis.list_organizations(
                    header)

            self.validate_testcase(result, [200], testcase, failures, True,
                                   self.expected_result, self.organisation_id)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[self.capellaAPI.org_ops_apis.list_organizations, ()]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[self.capellaAPI.org_ops_apis.list_organizations, ()]]
        self.throttle_test(api_func_list, True, self.project_id)
