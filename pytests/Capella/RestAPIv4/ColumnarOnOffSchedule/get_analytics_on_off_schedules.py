"""
Created on May 31, 2024

@author: Created using cbRAT cbModule by Vipul Bhardwaj
"""

import time
from pytests.Capella.RestAPIv4.ClustersColumnar.get_analytics_clusters import \
    GetAnalyticsClusters


class GetOnOffSchedule(GetAnalyticsClusters):

    def setUp(self, nomenclature="ColumnarScheduleOnOff_GET"):
        GetAnalyticsClusters.setUp(self, nomenclature)
        self.expected_res = {
            "timezone": "US/Pacific",
            "days": [
                {
                    "day": "monday",
                    "state": "custom",
                    "from": {
                        "hour": 12,
                        "minute": 30
                    },
                    "to": {
                        "hour": 14,
                        "minute": 30
                    }
                },
                {
                    "day": "tuesday",
                    "state": "custom",
                    "from": {
                        "hour": 12,
                        "minute": 30
                    },
                    "to": {
                        "hour": 14,
                        "minute": 30
                    }
                },
                {
                    "day": "wednesday",
                    "state": "on"
                },
                {
                    "day": "thursday",
                    "state": "on"
                },
                {
                    "day": "friday",
                    "state": "custom",
                    "from": {
                        "hour": 12,
                        "minute": 30
                    },
                    "to": {
                        "hour": 14,
                        "minute": 30
                    }
                },
                {
                    "day": "saturday",
                    "state": "off"
                },
                {
                    "day": "sunday",
                    "state": "off"
                }
            ]
        }
        res = self.columnarAPI.create_on_off_schedule(
            self.organisation_id, self.project_id, self.analyticsCluster_id,
            self.expected_res["timezone"], self.expected_res["days"])
        if res.status_code != 204:
            self.log.warning("Failed to create On/Off schedule")
            self.log.error(res.content)
            self.fail("!!!...On Off Schedule creation failed...!!!")
        else:
            self.log.info("Schedule created successfully.")
            time.sleep(2)

    def tearDown(self):
        # Delete Analytics On/Off Schedule
        result = self.columnarAPI.delete_on_off_schedule(
            self.organisation_id, self.project_id,
            self.analyticsCluster_id)
        if result.status_code == 429:
            result = self.columnarAPI.delete_on_off_schedule(
                self.organisation_id, self.project_id,
                self.analyticsCluster_id)
        if result.status_code != 204:
            self.log.error("Error: {}".format(result.content))
            super(GetOnOffSchedule, self).tearDown()
            self.fail("!!!...Error while Deleting on/off Schedule")
        else:
            time.sleep(2)
            super(GetOnOffSchedule, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Send call with valid path params"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/analyticsClusters/{}/onOffSchedule",
                "expected_status_code": 404,
                "expected_error": {
                    "errorType": "RouteNotFound",
                    "message": "Not found"
                }
            }, {
                "description": "Replace the last path param name in URI",
                "url": "/v4/organizations/{}/projects/{}/analyticsClusters/{}/onOffSchedul",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/analyticsClusters/{}/onOffSchedule/onOffSchedul",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
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
                "description": "Call API with non-hex analyticsClusterId",
                "invalid_analyticsClusterId": self.replace_last_character(
                    self.analyticsCluster_id, non_hex=True),
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
            project = self.project_id
            analyticsCluster = self.analyticsCluster_id

            if "url" in testcase:
                self.columnarAPI.schedule_on_off_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]
            elif "invalid_analyticsClusterId" in testcase:
                analyticsCluster = testcase["invalid_analyticsClusterId"]

            result = self.columnarAPI.fetch_on_off_schedule_info(
                organization, project, analyticsCluster)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.columnarAPI.fetch_on_off_schedule_info(
                    organization, project, analyticsCluster)
            self.columnarAPI.schedule_on_off_endpoint = \
                "/v4/organizations/{}/projects/{}/analyticsClusters/{}/onOffSchedule"
            self.validate_testcase(result, [200], testcase, failures)

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
            result = self.columnarAPI.fetch_on_off_schedule_info(
                self.organisation_id, self.project_id,
                self.analyticsCluster_id, header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.columnarAPI.fetch_on_off_schedule_info(
                    self.organisation_id, self.project_id,
                    self.analyticsCluster_id, header)
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_query_parameters(self):
        self.log.debug(
                "Correct Params - organization ID: {}, project ID: {}, "
                "analyticsCluster ID: {}".format(
                    self.organisation_id, self.project_id,
                    self.analyticsCluster_id))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id,
                self.analyticsCluster_id):
            testcases += 1
            testcase = {
                "description": "organization ID: {}, project ID: {}, "
                "analyticsCluster ID: {}"
                .format(str(combination[0]), str(combination[1]),
                        str(combination[2])),
                "organizationID": combination[0],
                "projectID": combination[1],
                "analyticsClusterID": combination[2]
            }
            if not (combination[0] == self.organisation_id and
                    combination[1] == self.project_id and
                    combination[2] == self.analyticsCluster_id):
                if combination[0] == "" or combination[1] == "":
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = "404 page not found"
                elif combination[2] == "" or any(variable in [
                    int, bool, float, list, tuple, set, type(None)] for
                         variable in [
                             type(combination[0]), type(combination[1]),
                             type(combination[2])]):
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
                elif combination[2] != self.analyticsCluster_id:
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = {
                        'code': 404,
                        'message': 'Unable to fetch the instance details.',
                        'hint': 'Please review your request and ensure that '
                                'all required parameters are correctly '
                                'provided.',
                        'httpStatusCode': 404
                    }
                else:
                    testcase["expected_status_code"] = 422
                    testcase["expected_error"] = {
                        "code": 422,
                        "hint": "Please review your request and ensure that "
                                "all required parameters are correctly "
                                "provided.",
                        "httpStatusCode": 422,
                        "message": "Unable to process the request. The "
                                   "provided projectId {} is not valid for "
                                   "the analytics cluster {}."
                        .format(combination[1], combination[2])
                    }
            self.log.info("Executing test: {}".format(testcase["description"]))
            if "param" in testcase:
                kwarg = {testcase["param"]: testcase["paramValue"]}
            else:
                kwarg = dict()

            result = self.columnarAPI.fetch_on_off_schedule_info(
                testcase["organizationID"], testcase["projectID"],
                testcase["analyticsClusterID"],
                **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.columnarAPI.fetch_on_off_schedule_info(
                    testcase["organizationID"], testcase["projectID"],
                    testcase["analyticsClusterID"],
                    **kwarg)
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.columnarAPI.fetch_on_off_schedule_info, (
                self.organisation_id, self.project_id, self.analyticsCluster_id
            )
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.columnarAPI.fetch_on_off_schedule_info, (
                self.organisation_id, self.project_id, self.analyticsCluster_id
            )
        ]]
        self.throttle_test(api_func_list, True, self.project_id)
