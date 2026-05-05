"""
Created on Mar 25, 2026

@author: Thuan Nguyen
"""

from datetime import datetime, timedelta
from pytests.Capella.RestAPIv4.Clusters.get_clusters import GetCluster


class GetPrepaidCreditConsumption(GetCluster):

    def setUp(self, nomenclature="GetPrepaidCreditConsumption_GET"):
        GetCluster.setUp(self, nomenclature)

    def tearDown(self):
        # delete clusters is handling at the pay as you go tests
        pass

    def test_api_path(self):
        testcases = [
            {
                "description": "Send call with valid path params"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/prePaidCredits",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }, {
                "description": "Replace the last path param name in URI",
                "url": "/v4/organizations/{}/prePaidCredits",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/prePaidCredits/bill",
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
            }
        ]
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            organization = self.organisation_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.billing_get_prepaid_credit_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]

            result = self.capellaAPI.cluster_ops_apis.get_prepaid_credit_consumption(
                organization, page=1, perPage=10)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.get_prepaid_credit_consumption(
                    organization, page=1, perPage=10)
            self.capellaAPI.cluster_ops_apis.billing_get_prepaid_credit_endpoint = \
                "/v4/organizations/{}/prePaidCredits"
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectCreator", "projectOwner",
            "projectManager", "projectViewer", "projectDataReader",
            "projectDataReaderWriter"
        ], None):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header, self.project_id)
            result = self.capellaAPI.cluster_ops_apis.get_prepaid_credit_consumption(
                self.organisation_id, page=1, perPage=10, headers=header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.get_prepaid_credit_consumption(
                    self.organisation_id, page=1, perPage=10, headers=header)
            self.validate_testcase(result, [200, 403], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_query_parameters(self):
        self.log.debug(
                "Correct Params - organization ID: {}".format(
                    self.organisation_id))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id):
            testcases += 1
            testcase = {
                "description": "organization ID: {}"
                .format(str(combination[0])),
                "organizationID": combination[0]
            }
            if not (combination[0] == self.organisation_id):
                if combination[0] == "" or any(variable in [
                    int, bool, float, list, tuple, set, type(None)] for
                         variable in [
                             type(combination[0])]):
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
            self.log.info("Executing test: {}".format(testcase["description"]))
            if "param" in testcase:
                kwarg = {testcase["param"]: testcase["paramValue"]}
            else:
                kwarg = dict()

            result = self.capellaAPI.cluster_ops_apis.get_prepaid_credit_consumption(
                testcase["organizationID"], page=1, perPage=10)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.get_prepaid_credit_consumption(
                    testcase["organizationID"], page=1, perPage=10)
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        json_body = {
            "page": 1,
            "perPage": 10
        }
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.get_prepaid_credit_consumption, (
                self.organisation_id, json_body
            )
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        json_body = {
            "page": 1,
            "perPage": 10
        }
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.get_prepaid_credit_consumption, (
                self.organisation_id, json_body
            )
        ]]
        self.throttle_test(api_func_list, True, self.project_id)

    def test_pagination_valid_params(self):
        testcases = [
            {"description": "page=1, perPage=1", "page": 1, "perPage": 1},
            {"description": "page=1, perPage=10", "page": 1, "perPage": 10},
            {"description": "page=1, perPage=50", "page": 1, "perPage": 50},
            {"description": "page=1, perPage=100", "page": 1, "perPage": 100},
            {"description": "page=5, perPage=20", "page": 5, "perPage": 20},
            {"description": "page=10, perPage=10", "page": 10, "perPage": 10},
            {"description": "page=50, perPage=2", "page": 50, "perPage": 2},
            {"description": "page=100, perPage=1", "page": 100, "perPage": 1},
            {"description": "page=25, perPage=75", "page": 25, "perPage": 75},
            {"description": "page=100, perPage=100", "page": 100, "perPage": 100},
        ]
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            result = self.capellaAPI.cluster_ops_apis.get_prepaid_credit_consumption(
                self.organisation_id, page=testcase["page"],
                perPage=testcase["perPage"])
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.get_prepaid_credit_consumption(
                    self.organisation_id, page=testcase["page"],
                    perPage=testcase["perPage"])
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_pagination_invalid_params(self):
        testcases = [
            {
                "description": "perPage=0 (invalid)",
                "page": 1,
                "perPage": 0,
                "expected_status_code": 400,
                "expected_error": {
                    "code": 400,
                    "hint": "Please review your request and ensure that all required parameters are correctly provided.",
                    "httpStatusCode": 400,
                    "message": "Pagination per page must be greater than 1 and less than or equal to 100"
                }
            },
            {
                "description": "perPage=120 (exceeds max 100)",
                "page": 1,
                "perPage": 120,
                "expected_status_code": 400,
                "expected_error": {
                    "code": 400,
                    "hint": "Please review your request and ensure that all required parameters are correctly provided.",
                    "httpStatusCode": 400,
                    "message": "Pagination per page must be greater than 1 and less than or equal to 100"
                }
            },
            {
                "description": "perPage=-5 (negative)",
                "page": 1,
                "perPage": -5,
                "expected_status_code": 400,
                "expected_error": {
                    "code": 400,
                    "hint": "Please review your request and ensure that all required parameters are correctly provided.",
                    "httpStatusCode": 400,
                    "message": "Pagination per page must be greater than 1 and less than or equal to 100"
                }
            },
            {
                "description": "page=0 (invalid)",
                "page": 0,
                "perPage": 10,
                "expected_status_code": 400,
                "expected_error": {
                    "code": 400,
                    "hint": "Please review your request and ensure that all required parameters are correctly provided.",
                    "httpStatusCode": 400,
                    "message": "Pagination page must be greater than 0."
                }
            },
            {
                "description": "page=-2 (negative)",
                "page": -2,
                "perPage": 10,
                "expected_status_code": 400,
                "expected_error": {
                    "code": 400,
                    "hint": "Please review your request and ensure that all required parameters are correctly provided.",
                    "httpStatusCode": 400,
                    "message": "Pagination page must be greater than 0."
                }
            },
        ]
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            result = self.capellaAPI.cluster_ops_apis.get_prepaid_credit_consumption(
                self.organisation_id, page=testcase["page"],
                perPage=testcase["perPage"])
            print("\n***** Response: {}, {}".format(result.status_code, result.text))
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.get_prepaid_credit_consumption(
                    self.organisation_id, page=testcase["page"],
                    perPage=testcase["perPage"])
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))



