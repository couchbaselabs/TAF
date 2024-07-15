"""
Created on January 31, 2024

@author: Vipul Bhardwaj
"""

import time
from pytests.Capella.RestAPIv4.Clusters.get_clusters import GetCluster


class CreateClusterSchedule(GetCluster):

    def setUp(self, nomenclature="Clusters_Schedule_Create"):
        GetCluster.setUp(self, nomenclature)
        self.expected_res = {
            "timezone": "Pacific/Midway",
            "days": [
                {
                    "state": "on",
                    "day": "monday",
                }, {
                    "state": "off",
                    "day": "tuesday",
                }, {
                    "state": "on",
                    "day": "wednesday",
                }, {
                    "state": "off",
                    "day": "thursday",
                }, {
                    "state": "on",
                    "day": "friday",
                }, {
                    "state": "off",
                    "day": "saturday",
                }, {
                    "state": "custom",
                    "day": "sunday",
                    "from": {
                        "hour": 0,
                        "minute": 0
                    },
                    "to": {
                        "hour": 1,
                        "minute": 30
                    }
                }
            ]
        }

    def tearDown(self):
        super(CreateClusterSchedule, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Create valid schedule"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}/"
                       "onOffSchedule",
                "expected_status_code": 404,
                "expected_error": {
                    "errorType": "RouteNotFound",
                    "message": "Not found"
                }
            }, {
                "description": "Replace onOffSchedule with onOff in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/onOff",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/"
                       "onOffSchedule/onOff",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Create schedule but with non-hex "
                               "organizationID",
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
                "description": "Create schedule but with non-hex projectID",
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
            }, {
                "description": "Create schedule but with non-hex clusterID",
                "invalid_clusterID": self.replace_last_character(
                    self.cluster_id, non_hex=True),
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
            clus = self.cluster_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.\
                    cluster_on_off_schedule_endpoint = testcase["url"]
            if "invalid_organizationID" in testcase:
                org = testcase["invalid_organizationID"]
            elif "invalid_projectID" in testcase:
                proj = testcase["invalid_projectID"]
            elif "invalid_clusterID" in testcase:
                clus = testcase["invalid_clusterID"]

            result = self.capellaAPI.cluster_ops_apis.\
                create_cluster_on_off_schedule(
                    org, proj, clus, self.expected_res["timezone"],
                    self.expected_res["days"])
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.\
                    create_cluster_on_off_schedule(
                        org, proj, clus, self.expected_res["timezone"],
                        self.expected_res["days"])

            self.capellaAPI.cluster_ops_apis.cluster_on_off_schedule_endpoint \
                = "/v4/organizations/{}/projects/{}/clusters/{}/onOffSchedule"

            if self.validate_testcase(result, [204, 422], testcase, failures):
                self.log.info("Schedule created successfully.")
                time.sleep(2)
                res = self.capellaAPI.cluster_ops_apis.\
                    delete_cluster_on_off_schedule(org, proj, clus)
                if res.status_code == 429:
                    self.handle_rate_limit(int(res.headers['Retry-After']))
                    res = self.capellaAPI.cluster_ops_apis.\
                        delete_cluster_on_off_schedule(org, proj, clus)
                if res.status_code != 204:
                    self.fail("Error while deleting the currently "
                              "created Schedule")
                else:
                    self.log.info("Schedule deleted successfully.")
                    time.sleep(2)

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
            self.organisation_id, "Auth_Project")
        if resp.status_code == 201:
            other_project_id = resp.json()["id"]
        else:
            self.fail("Error while creating project")

        testcases = []
        for role in self.api_keys:
            testcase = {
                "description": "Calling API with {} role".format(role),
                "token": self.api_keys[role]["token"],
            }
            if not any(element in ["organizationOwner", "projectOwner",
                                   "projectManager"] for
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
            result = self.capellaAPI.cluster_ops_apis.\
                create_cluster_on_off_schedule(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.expected_res['timezone'],
                    self.expected_res['days'], headers=header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.\
                    create_cluster_on_off_schedule(
                        self.organisation_id, self.project_id, self.cluster_id,
                        self.expected_res['timezone'],
                        self.expected_res['days'], headers=header)

            if self.validate_testcase(result, [204, 422], testcase, failures):
                self.log.info("Schedule created successfully.")
                time.sleep(2)
                res = self.capellaAPI.cluster_ops_apis.\
                    delete_cluster_on_off_schedule(
                        self.organisation_id, self.project_id,
                        self.cluster_id)
                if res.status_code == 429:
                    self.handle_rate_limit(int(res.headers['Retry-After']))
                    res = self.capellaAPI.cluster_ops_apis.\
                        delete_cluster_on_off_schedule(
                            self.organisation_id, self.project_id,
                            self.cluster_id)
                if res.status_code != 204:
                    self.fail("Error while deleting the currently "
                              "created Schedule")
                else:
                    self.log.info("Schedule deleted successfully.")
                    time.sleep(2)

        self.update_auth_with_api_token(self.curr_owner_key)
        resp = self.capellaAPI.org_ops_apis.delete_project(
            self.organisation_id, other_project_id)
        if resp.status_code != 204:
            failures.append("Error while deleting project {}"
                            .format(other_project_id))

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_query_parameters(self):
        self.log.debug("Correct Params - OrgID: {}, ProjID: {}, ClusID: {}"
                       .format(self.organisation_id, self.project_id,
                               self.cluster_id))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id, self.cluster_id):
            testcases += 1
            testcase = {
                "description": "OrganizationID: {}, ProjectID: {}, "
                               "ClusterID: {}".format(str(combination[0]),
                                                      str(combination[1]),
                                                      str(combination[2])),
                "organizationID": combination[0],
                "projectID": combination[1],
                "clusterID": combination[2],
            }
            if not (combination[0] == self.organisation_id and
                    combination[1] == self.project_id and
                    combination[2] == self.cluster_id):
                if combination[1] == "" or combination[0] == "":
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = "404 page not found"
                elif combination[2] == "":
                    testcase["expected_status_code"] = 405
                    testcase["expected_error"] = ""
                elif any(variable in [int, bool, float, list, tuple, set,
                                      type(None)] for variable in [
                             type(combination[0]), type(combination[1]),
                             type(combination[2])]):
                    testcase["expected_status_code"] = 400
                    testcase["expected_error"] = {
                        "code": 1000,
                        "hint": "Check if all the required params are "
                                "present in the request body.",
                        "httpStatusCode": 400,
                        "message": "The server cannot or will not process "
                                   "the request due to something that is "
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
                elif combination[2] != self.cluster_id:
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
            self.expected_res['name'] = self.generate_random_string(
                special_characters=False)
            if "param" in testcase:
                kwarg = {testcase["param"]: testcase["paramValue"]}
            else:
                kwarg = dict()

            result = self.capellaAPI.cluster_ops_apis.\
                create_cluster_on_off_schedule(
                    testcase["organizationID"], testcase["projectID"],
                    testcase["clusterID"], self.expected_res['timezone'],
                    self.expected_res['days'], **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.\
                    create_cluster_on_off_schedule(
                        testcase["organizationID"], testcase["projectID"],
                        testcase["clusterID"],
                        self.expected_res['timezone'],
                        self.expected_res['days'], **kwarg)

            if self.validate_testcase(result, [204, 422], testcase, failures):
                self.log.info("Schedule created successfully.")
                time.sleep(2)
                res = self.capellaAPI.cluster_ops_apis.\
                    delete_cluster_on_off_schedule(
                        testcase["organizationID"], testcase["projectID"],
                        testcase["clusterID"])
                if res.status_code == 429:
                    self.handle_rate_limit(int(res.headers['Retry-After']))
                    res = self.capellaAPI.cluster_ops_apis.\
                        delete_cluster_on_off_schedule(
                            testcase["organizationID"], testcase["projectID"],
                            testcase["clusterID"])
                if res.status_code != 204:
                    self.fail("Error while deleting the currently "
                              "created Schedule")
                else:
                    self.log.info("Schedule deleted successfully.")
                    time.sleep(2)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.create_cluster_on_off_schedule,
            (self.organisation_id, self.project_id, self.cluster_id,
             "", self.expected_res["days"])
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.create_cluster_on_off_schedule,
            (self.organisation_id, self.project_id, self.cluster_id,
             "", self.expected_res["days"])
        ]]
        self.throttle_test(api_func_list, True, self.project_id)
