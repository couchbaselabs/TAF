"""
Created on Mar 25, 2026

@author: Thuan Nguyen
"""

from datetime import datetime, timedelta
from pytests.Capella.RestAPIv4.Clusters.get_clusters import GetCluster


class CategorizedBilling(GetCluster):

    def setUp(self, nomenclature="CategorizedBilling_POST"):
        GetCluster.setUp(self, nomenclature)

    def tearDown(self):
        super(CategorizedBilling, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Send call with valid path params"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/billing",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }, {
                "description": "Replace the last path param name in URI",
                "url": "/v4/organizations/{}/billing",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/billing/bill",
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
                self.capellaAPI.cluster_ops_apis.billing_categorized_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]

            result = self.capellaAPI.cluster_ops_apis.get_categorized_billing(
                organization)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.get_categorized_billing(
                    organization)
            self.capellaAPI.cluster_ops_apis.billing_categorized_endpoint = \
                "/v4/organizations/{}/billing"
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
            self.auth_test_setup(testcase, failures, header)
            result = self.capellaAPI.cluster_ops_apis.get_categorized_billing(header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.get_categorized_billing(header)
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

            result = self.capellaAPI.cluster_ops_apis.get_categorized_billing(
                testcase["organizationID"], **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.get_categorized_billing(
                    testcase["organizationID"], **kwarg)
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.get_categorized_billing, (
                self.organisation_id
            )
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.get_categorized_billing, (
                self.organisation_id
            )
        ]]
        self.throttle_test(api_func_list, True)

    def test_categorized_billing_without_filter(self):
        today = datetime.now()
        date_fmt = "%Y-%m-%d"
        testcases = [
            {
                "description": "Last 3 days",
                "json": {
                    "startDate": (today - timedelta(days=3)).strftime(date_fmt),
                    "endDate": today.strftime(date_fmt)
                }
            },
            {
                "description": "Last 10 days",
                "json": {
                    "startDate": (today - timedelta(days=10)).strftime(date_fmt),
                    "endDate": today.strftime(date_fmt)
                }
            },
            {
                "description": "Last 20 days",
                "json": {
                    "startDate": (today - timedelta(days=20)).strftime(date_fmt),
                    "endDate": today.strftime(date_fmt)
                }
            },
            {
                "description": "Last 2 months (60 days)",
                "json": {
                    "startDate": (today - timedelta(days=60)).strftime(date_fmt),
                    "endDate": today.strftime(date_fmt)
                }
            },
            {
                "description": "Last 6 months (180 days)",
                "json": {
                    "startDate": (today - timedelta(days=180)).strftime(date_fmt),
                    "endDate": today.strftime(date_fmt)
                }
            },
            {
                "description": "Last one year (365 days)",
                "json": {
                    "startDate": (today - timedelta(days=365)).strftime(date_fmt),
                    "endDate": today.strftime(date_fmt)
                }
            },
            {
                "description": "Future date in 10 days (invalid)",
                "json": {
                    "startDate": today.strftime(date_fmt),
                    "endDate": (today + timedelta(days=10)).strftime(date_fmt)
                },
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
            },
            {
                "description": "One year in future (invalid)",
                "json": {
                    "startDate": today.strftime(date_fmt),
                    "endDate": (today + timedelta(days=365)).strftime(date_fmt)
                },
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
            },
            {
                "description": "Date range more than one year (invalid)",
                "json": {
                    "startDate": (today - timedelta(days=400)).strftime(date_fmt),
                    "endDate": today.strftime(date_fmt)
                },
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
            },
        ]
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            result = self.capellaAPI.cluster_ops_apis.get_categorized_billing(
                self.organisation_id, json=testcase["json"])
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.get_categorized_billing(
                    self.organisation_id, json=testcase["json"])
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_categorized_billing_with_filter(self):
        today = datetime.now()
        date_fmt = "%Y-%m-%d"

        all_categories = [
            "operationalComputeAndStorage",
            "privateEndpointsStandard",
            "operationalBucketBackup",
            "operationalClusterBackup",
            "appServicesComputeAndStorage",
            "dataTransferStandard",
            "dataApiStandard",
            "aiServicesLLM",
            "aiServicesAiGateway",
            "aiServicesUdsPager",
            "aiServicesSdsPager",
            "analyticsCompute",
            "analyticsClusterBackup",
            "analyticsStorage"
        ]

        date_ranges = [
            {"desc": "Last 3 days", "days": 3},
            {"desc": "Last 10 days", "days": 10},
            {"desc": "Last 20 days", "days": 20},
            {"desc": "Last 2 months", "days": 60},
            {"desc": "Last 6 months", "days": 180},
            {"desc": "Last one year", "days": 365},
        ]

        category_scenarios = [
            {"desc": "All 14 categories", "categories": all_categories},
        ]
        for cat in all_categories:
            category_scenarios.append({"desc": "Single: {}".format(cat), "categories": [cat]})
        category_scenarios.extend([
            {"desc": "Combo: operational + analytics", "categories": ["operationalComputeAndStorage", "analyticsCompute"]},
            {"desc": "Combo: AI services", "categories": ["aiServicesLLM", "aiServicesAiGateway", "aiServicesUdsPager"]},
            {"desc": "Combo: backup categories", "categories": ["operationalBucketBackup", "operationalClusterBackup", "analyticsClusterBackup"]},
        ])

        testcases = []
        for dr in date_ranges:
            for cs in category_scenarios:
                testcases.append({
                    "description": "{} with {}".format(dr["desc"], cs["desc"]),
                    "json": {
                        "startDate": (today - timedelta(days=dr["days"])).strftime(date_fmt),
                        "endDate": today.strftime(date_fmt),
                        "filters": {
                            "categories": cs["categories"],
                            "projectIds": [self.project_id],
                            "instanceIds": [self.cluster_id]
                        }
                    }
                })

        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            result = self.capellaAPI.cluster_ops_apis.get_categorized_billing(
                self.organisation_id, json=testcase["json"])
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.get_categorized_billing(
                    self.organisation_id, json=testcase["json"])
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))
