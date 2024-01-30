"""
Created on August 30, 2023

@author: Vipul Bhardwaj
"""

import time
import base64
from couchbase_utils.capella_utils.dedicated import CapellaUtils
from pytests.Capella.RestAPIv4.Clusters.get_clusters import GetCluster
from pytests.Capella.RestAPIv4.Projects.get_projects import GetProject

class CreateCluster(GetProject):

    def setUp(self, nomenclature="Clusters_Create"):
        GetProject.setUp(self, nomenclature)

        """
        Supplying empty Support Parameter in POST tests, and tests will 
        be verified based on a correct request being sent where the
        expected response would be : 
        {
            "code": 4002,
            "hint": "Please ensure that payload or body of the "
                    "request is not empty.",
            "httpStatusCode": 422,
            "message": "Unable to process request. Support package is "
                       "not valid. Please provide a valid support "
                       "package and try again."
        }
        """
        self.expected_result = {
            "name": self.prefix + nomenclature,
            "description": None,
            "cloudProvider": {
                "type": "aws",
                "region": "us-east-1",
                "cidr": CapellaUtils.get_next_cidr() + "/20"
            },
            "couchbaseServer": {
                "version": str(self.input.param("server_version", 7.2))
            },
            "serviceGroups": [
                {
                    "node": {
                        "compute": {
                            "cpu": 4,
                            "ram": 16
                        },
                        "disk": {
                            "storage": 50,
                            "type": "gp3",
                            "iops": 3000,
                            "autoExpansion": "on"
                        }
                    },
                    "numOfNodes": 3,
                    "services": [
                        "data"
                    ]
                }
            ],
            "availability": {
                "type": "single"
            },
            "support": {
                # "plan": "basic",
                # "timezone": "GMT"
            },
            "currentState": None,
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
        super(CreateCluster, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters",
                "expected_status_code": 404,
                "expected_error": {
                    "errorType": "RouteNotFound",
                    "message": "Not found"
                }
            }, {
                "description": "Replace clusters with cluster in URI",
                "url": "/v4/organizations/{}/projects/{}/cluster",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/cluster",
                "expected_status_code": 405,
                "expected_error": ""
            }, {
                "description": "Fetch cluster but with non-hex organizationID",
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
                "description": "Fetch cluster but with non-hex projectID",
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
                self.capellaAPI.cluster_ops_apis.cluster_endpoint = \
                    testcase["url"]
            if "invalid_organizationID" in testcase:
                org = testcase["invalid_organizationID"]
            elif "invalid_projectID" in testcase:
                proj = testcase["invalid_projectID"]

            result = self.capellaAPI.cluster_ops_apis.create_cluster(
                org, proj, self.expected_result["name"],
                self.expected_result['cloudProvider'],
                self.expected_result['couchbaseServer'],
                self.expected_result['serviceGroups'],
                self.expected_result['availability'],
                self.expected_result['support'])
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.create_cluster(
                    org, proj, self.expected_result["name"],
                    self.expected_result['cloudProvider'],
                    self.expected_result['couchbaseServer'],
                    self.expected_result['serviceGroups'],
                    self.expected_result['availability'],
                    self.expected_result['support'])
            self.validate_testcase(result, 422, testcase, failures)

            self.capellaAPI.cluster_ops_apis.cluster_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters"

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
                "expected_status_code": 422,
                "expected_error": {
                    "code": 4002,
                    "hint": "Please ensure that payload or body of the "
                            "request is not empty.",
                    "httpStatusCode": 422,
                    "message": "Unable to process request. Support package is "
                               "not valid. Please provide a valid support "
                               "package and try again."
                }
            }
            if not any(element in ["organizationOwner",
                                   "projectOwner", "projectManager"] for
                       element in self.api_keys[role]["roles"]):
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
        self.auth_test_extension(testcases, other_project_id, 422, {
            "code": 4002,
            "hint": "Please ensure that payload or body of the "
                    "request is not empty.",
            "httpStatusCode": 422,
            "message": "Unable to process request. Support package is "
                       "not valid. Please provide a valid support "
                       "package and try again."
        })

        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, other_project_id)
            result = self.capellaAPI.cluster_ops_apis.create_cluster(
                self.organisation_id, self.project_id,
                self.expected_result["name"],
                self.expected_result['cloudProvider'],
                self.expected_result['couchbaseServer'],
                self.expected_result['serviceGroups'],
                self.expected_result['availability'],
                self.expected_result['support'], headers=header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.create_cluster(
                    self.organisation_id, self.project_id,
                    self.expected_result["name"],
                    self.expected_result['cloudProvider'],
                    self.expected_result['couchbaseServer'],
                    self.expected_result['serviceGroups'],
                    self.expected_result['availability'],
                    self.expected_result['support'], headers=header)
            if result.status_code == 422:
                if isinstance(testcase["expected_error"], dict) \
                        and result.json()["code"] != 4002:
                    self.log.error("Dummy error not correct")
                    self.log.warning("Result : {}".format(result.json()))
                    failures.append(testcase)
                else:
                    self.log.debug("This is a handler condition for the dummy "
                                   "cluster creation request, we expect a "
                                   "{}, but as the cluster BODY is dummy, the "
                                   "deletion will return a 422:4002.".format(
                                    testcase["expected_error"]))
            else:
                self.validate_testcase(result, 422, testcase, failures)

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
                      .format(len(failures), testcases))

    def test_query_parameters(self):
        self.log.debug("Correct Params - OrgID: {}, ProjID: {}"
                       .format(self.organisation_id, self.project_id))
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
                "expected_status_code": 422,
                "expected_error": {
                    "code": 4002,
                    "hint": "Please ensure that payload or body of the "
                            "request is not empty.",
                    "httpStatusCode": 422,
                    "message": "Unable to process request. Support package is "
                               "not valid. Please provide a valid support "
                               "package and try again."
                }
            }
            if not (combination[0] == self.organisation_id and
                    combination[1] == self.project_id):
                if combination[0] == "":
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = "404 page not found"
                elif combination[1] == "":
                    testcase["expected_status_code"] = 405
                    testcase["expected_error"] = ""
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
            self.log.info("Executing test: {}".format(testcase["description"]))
            if "param" in testcase:
                kwarg = {testcase["param"]: testcase["paramValue"]}
            else:
                kwarg = dict()

            result = self.capellaAPI.cluster_ops_apis.create_cluster(
                testcase["organizationID"], testcase["projectID"],
                self.expected_result["name"],
                self.expected_result['cloudProvider'],
                self.expected_result['couchbaseServer'],
                self.expected_result['serviceGroups'],
                self.expected_result['availability'],
                self.expected_result['support'], **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.create_cluster(
                    testcase["organizationID"], testcase["projectID"],
                    self.expected_result["name"],
                    self.expected_result['cloudProvider'],
                    self.expected_result['couchbaseServer'],
                    self.expected_result['serviceGroups'],
                    self.expected_result['availability'],
                    self.expected_result['support'], **kwarg)
            if result.status_code == 422:
                if isinstance(testcase["expected_error"], dict) \
                        and result.json()["code"] != 4002:
                    self.log.error("Dummy error not correct")
                    self.log.warning("Result : {}".format(result.json()))
                    failures.append(testcase)
                else:
                    self.log.debug("This is a handler condition for the dummy "
                                   "cluster creation request, we expect a "
                                   "{}, but as the cluster BODY is dummy, the "
                                   "deletion will return a 422:4002.".format(
                                    testcase["expected_error"]))
            else:
                self.validate_testcase(result, 422, testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[self.capellaAPI.cluster_ops_apis.create_cluster,
                          (self.organisation_id, self.project_id,
                           self.expected_result["name"],
                           self.expected_result['cloudProvider'],
                           self.expected_result['couchbaseServer'],
                           self.expected_result['serviceGroups'],
                           self.expected_result['availability'],
                           self.expected_result['support'])]]

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
            #   # invalid body params, ie, which give a 422 response.
            if "422" in results[result]["4xx_errors"]:
                del results[result]["4xx_errors"]["422"]

            if len(results[result]["4xx_errors"]) > 0 or len(
                    results[result]["5xx_errors"]) > 0:
                self.fail("Some API calls failed")

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[self.capellaAPI.cluster_ops_apis.create_cluster,
                          (self.organisation_id, self.project_id,
                           self.expected_result["name"],
                           self.expected_result['cloudProvider'],
                           self.expected_result['couchbaseServer'],
                           self.expected_result['serviceGroups'],
                           self.expected_result['availability'],
                           self.expected_result['support'])]]

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
            #   # invalid body params, ie, which give a 422 response.
            if "422" in results[result]["4xx_errors"]:
                del results[result]["4xx_errors"]["422"]

            if len(results[result]["4xx_errors"]) > 0 or len(
                    results[result]["5xx_errors"]) > 0:
                self.fail("Some API calls failed")
