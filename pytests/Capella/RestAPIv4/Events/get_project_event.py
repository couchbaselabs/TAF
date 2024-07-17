"""
Created on July 08, 2024

@author: Created using cbRAT cbModule by Vipul Bhardwaj
"""

from pytests.Capella.RestAPIv4.Projects.get_projects import GetProject


class GetProjectEvent(GetProject):

    def setUp(self, nomenclature="Events_GET"):
        GetProject.setUp(self, nomenclature)

        self.log.debug("Fetching a Project Level Event")
        res = self.capellaAPI.cluster_ops_apis.list_project_events(
            self.organisation_id, self.project_id)
        if res.status_code != 200:
            self.log.error("Result: {}".format(res.content))
            self.tearDown()
            self.fail("!!!...Error while fetching Event...!!!")
        self.log.info("Event fetched : {}".format(res.json()['data'][0]))
        self.event_id = res.json()['data'][-1]['id']

    def tearDown(self):
        super(GetProjectEvent, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Send call with valid path params"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/events",
                "expected_status_code": 404,
                "expected_error": {
                    "errorType": "RouteNotFound",
                    "message": "Not found"
                }
            }, {
                "description": "Replace the last path param name in URI",
                "url": "/v4/organizations/{}/projects/{}/event",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/events/event",
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
                "description": "Call API with non-hex eventId",
                "invalid_eventId": self.replace_last_character(
                    self.event_id, non_hex=True),
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
            event = self.event_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.project_events_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]
            elif "invalid_eventId" in testcase:
                event = testcase["invalid_eventId"]

            result = self.capellaAPI.cluster_ops_apis.fetch_project_event_info(
                organization, project, event)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.fetch_project_event_info(
                    organization, project, event)

            self.capellaAPI.cluster_ops_apis.project_events_endpoint = \
                "/v4/organizations/{}/projects/{}/events"

            self.validate_testcase(result, [200], testcase, failures)

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
            if not any(element in [
                 "organizationOwner", "projectOwner",
                 "projectManager", "projectViewer",
                 "projectDataReader", "projectDataReaderWriter"
            ] for element in self.api_keys[role]["roles"]):
                testcase["expected_error"] = {
                    "code": 1002,
                    "hint": "Your access to the requested resource is denied. "
                            "Please make sure you have the necessary "
                            "permissions to access the resource.",
                    "httpStatusCode": 403,
                    "message": "Access Denied."
                }
                testcase["expected_status_code"] = 403
            testcases.append(testcase)
        self.auth_test_extension(testcases, False)

        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header, self.project_id)
            result = self.capellaAPI.cluster_ops_apis.fetch_project_event_info(
                self.organisation_id, self.project_id, self.event_id, header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.fetch_project_event_info(
                    self.organisation_id, self.project_id, self.event_id,
                    header)

            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_query_parameters(self):
        self.log.debug(
                "Correct Params - organization ID: {}, project ID: {}, "
                "event ID: {}".format(
                    self.organisation_id, self.project_id, self.event_id))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id, self.event_id):
            testcases += 1
            testcase = {
                "description": "organization ID: {}, project ID: {}, "
                "event ID: {}"
                .format(str(combination[0]), str(combination[1]),
                        str(combination[2])),
                "organizationID": combination[0],
                "projectID": combination[1],
                "eventID": combination[2]
            }
            if not (combination[0] == self.organisation_id and
                    combination[1] == self.project_id and
                    combination[2] == self.event_id):
                # if combination[0] == "" and not\
                #         (combination[2] == "" or combination[1] == ""):
                #     testcase["expected_status_code"] = 400
                #     testcase["expected_error"] = {
                #         "code": 1000,
                #         "hint": "Check if you have provided a valid URL and "
                #                 "all the required params are present in the "
                #                 "request body.",
                #         "httpStatusCode": 400,
                #         "message": "The server cannot or will not process the "
                #                    "request due to something that is "
                #                    "perceived to be a client error."
                #     }
                if (combination[1] == "" and combination[0] != "") or \
                        combination[2] == "" or \
                        (combination[0] == "" and combination[1] != ""):
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = "404 page not found"
                elif ((combination[0] == "" and combination[1] == "") or
                      any(variable in [
                        int, bool, float, list, tuple, set, type(None)] for
                      variable in [
                             type(combination[0]), type(combination[1]), 
                             type(combination[2])])):
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
                elif combination[2] != self.event_id:
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = {
                        "code": 4025,
                        "hint": "The requested cluster details could not be "
                                "found or fetched. Please ensure that the "
                                "correct cluster ID is provided.",
                        "message": "Unable to fetch the cluster details.",
                        "httpStatusCode": 404
                    }
                else:
                    testcase["expected_status_code"] = 422
                    testcase["expected_error"] = {
                        "code": 4031,
                        "hint": "Please provide a valid projectId.",
                        "httpStatusCode": 422,
                        "message": "Unable to process the request. The "
                                   "provided projectId {} is not valid for "
                                   "the cluster {}."
                        .format(combination[1], combination[2])
                    }
            self.log.info("Executing test: {}".format(testcase["description"]))
            if "param" in testcase:
                kwarg = {testcase["param"]: testcase["paramValue"]}
            else:
                kwarg = dict()

            result = self.capellaAPI.cluster_ops_apis.fetch_project_event_info(
                testcase["organizationID"], testcase["projectID"],
                testcase["eventID"], **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.fetch_project_event_info(
                    testcase["organizationID"], testcase["projectID"],
                    testcase["eventID"], **kwarg)

            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.fetch_project_event_by_i_d_info, (
                self.organisation_id, self.project_id, self.event_id
            )
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.fetch_project_event_by_i_d_info, (
                self.organisation_id, self.project_id, self.event_id
            )
        ]]
        self.throttle_test(api_func_list, True, self.project_id)
