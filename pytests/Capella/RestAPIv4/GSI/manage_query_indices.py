"""
Created on September 18, 2024

@author: Created using cbRAT cbModule by Vipul Bhardwaj
"""

import time
from pytests.Capella.RestAPIv4.Clusters.get_clusters import GetCluster


class PostIndex(GetCluster):

    def setUp(self, nomenclature="GSI_POST"):
        GetCluster.setUp(self, nomenclature)
        self.i = 0
        self.expected_res = {
            "defs": [
                {
                    "definition": "create index idx on `travel-sample`._default._default(airline, destinationairport, sourceairport) partition by hash(airline) where id in [1000,2000,3000] WITH {\"defer_build\":true}"
                }, {
                    "definition": "build index on `travel-sample`.`_default`.`_default`(def_city, def_faa, def_icao)"
                }, {
                    "definition": "alter index idx on `travel-sample`._default`.`_default with { \"action\": \"replica_count\", \"num_replica\": 2 }"
                }, {
                    "definition": "drop index idx on `travel-sample`._default._default"
                }
            ]
        }
        # Create a sample bucket
        res = self.capellaAPI.cluster_ops_apis.create_sample_bucket(
            self.organisation_id, self.project_id, self.cluster_id,
            "travel-sample")
        if res.status_code == 429:
            self.handle_rate_limit(self.curr_owner_key)
            res = self.capellaAPI.cluster_ops_apis.create_sample_bucket(
                self.organisation_id, self.project_id, self.cluster_id,
                "travel-sample")
        if res.status_code != 201:
            self.log.error(res.content)
            self.tearDown()
            self.fail("!!!...Travel Sample creation failed...!!!")
        self.capella["clusters"]["bucket"] = res.json()["bucketId"]
        self.bucket = "travel-sample"
        self.log.debug("Waiting for data load in sample bucket to complete")
        time.sleep(10)

    def tearDown(self):
        super(PostIndex, self).tearDown()

    def test_api_path(self):
        testcases = list()
        for definition in self.expected_res["defs"]:
            testcases.append({
                "description": "Send valid call for: `{}`"
                .format(definition["definition"]),
                "definition": definition["definition"],
                "expected_status_code": 500,
                "expected_error": {
                    "code": 500,
                    "hint": "Please review your request and ensure that all "
                            "required parameters are correctly provided.",
                    "httpStatusCode": 500,
                    "message": "GSI CreateIndex() - cause:  Encountered "
                               "transient error. Index idx will retry "
                               "building in the background for reason: Build "
                               "Already In Progress. Keyspace travel-sample. "
                               "Encountered transient error. Index idx will "
                               "retry building in the background for reason: "
                               "Build Already In Progress. Keyspace "
                               "travel-sample. Encountered transient error. "
                               "Index idx will retry building in the "
                               "background for reason: Build Already In "
                               "Progress. Keyspace travel-sample. Encountered "
                               "transient error. Index idx will retry "
                               "building in the background for reason: Build "
                               "Already In Progress. Keyspace travel-sample."
                               "  \n"
                }
            })
        testcases.extend([
            {
                "description": "Send multiple valid index statements",
                "definition": "create index idx1 on test.test.test(c1); "
                              "create index idx2 on test.test.test(c2)",
                "expected_status_code": 400,
                "expected_error": {
                    "code": 400,
                    "hint": "Please review your request and ensure that all "
                            "required parameters are correctly provided.",
                    "httpStatusCode": 400,
                    "message": "syntax error - line 1, column 42, near '..."
                               "test.test.test(c1); ', at: create "
                               "(reserved word)"
                }
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}/queryService/indexes",
                "definition": "build index on `travel-sample`.`_default`.`_default`(def_city, def_faa, def_icao)",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }, {
                "description": "Replace the last path param name in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/queryService/indexe",
                "definition": "build index on `travel-sample`.`_default`.`_default`(def_city, def_faa, def_icao)",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/queryService/indexes/indexe",
                "definition": "build index on `travel-sample`.`_default`.`_default`(def_city, def_faa, def_icao)",
                "expected_status_code": 405,
                "expected_error": ""
            }, {
                "description": "Call API with non-hex organizationId",
                "invalid_organizationId": self.replace_last_character(
                    self.organisation_id, non_hex=True),
                "definition": "build index on `travel-sample`.`_default`.`_default`(def_city, def_faa, def_icao)",
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
                "definition": "build index on `travel-sample`.`_default`.`_default`(def_city, def_faa, def_icao)",
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
                "description": "Call API with non-hex clusterId",
                "invalid_clusterId": self.replace_last_character(
                    self.cluster_id, non_hex=True),
                "definition": "build index on `travel-sample`.`_default`.`_default`(def_city, def_faa, def_icao)",
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
        ])
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            organization = self.organisation_id
            project = self.project_id
            cluster = self.cluster_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.index_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]
            elif "invalid_clusterId" in testcase:
                cluster = testcase["invalid_clusterId"]

            result = self.capellaAPI.cluster_ops_apis.manage_query_indices(
                organization, project, cluster, testcase["definition"])
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.manage_query_indices(
                    organization, project, cluster, testcase["definition"])
            self.capellaAPI.cluster_ops_apis.index_endpoint = \
                ("/v4/organizations/{}/projects/{}/clusters/{}/queryService/"
                 "indexes")
            if self.validate_testcase(result, [200], testcase, failures):
                self.log.debug("Index Creation Successful")
                self.i += 1

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner", "projectDataReaderWriter"
        ]):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = self.capellaAPI.cluster_ops_apis.manage_query_indices(
                self.organisation_id, self.project_id, self.cluster_id,
                self.expected_res["defs"][1]["definition"], header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.manage_query_indices(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.expected_res["defs"][1]["definition"], header)
            if self.validate_testcase(result, [200], testcase, failures):
                self.log.debug("Index Creation Successful")
                self.i += 1

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_query_parameters(self):
        self.log.debug(
            "Correct Params - organization ID: {}, project ID: {}, "
            "cluster ID: {}".format(
                self.organisation_id, self.project_id, self.cluster_id))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id, self.cluster_id):
            testcases += 1
            testcase = {
                "description": "organization ID: {}, project ID: {}, "
                               "cluster ID: {}"
                .format(str(combination[0]), str(combination[1]),
                        str(combination[2])),
                "organizationID": combination[0],
                "projectID": combination[1],
                "clusterID": combination[2]
            }
            if not (combination[0] == self.organisation_id and
                    combination[1] == self.project_id and
                    combination[2] == self.cluster_id):
                if combination[0] == "" or combination[1] == "":
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = "404 page not found"
                elif combination[2] == "":
                    testcase["expected_status_code"] = 405
                    testcase["expected_error"] = ""
                elif any(variable in [
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
            if "param" in testcase:
                kwarg = {testcase["param"]: testcase["paramValue"]}
            else:
                kwarg = dict()

            result = self.capellaAPI.cluster_ops_apis.manage_query_indices(
                testcase["organizationID"], testcase["projectID"],
                testcase["clusterID"],
                self.expected_res["defs"][1]["definition"],
                **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.manage_query_indices(
                    testcase["organizationID"], testcase["projectID"],
                    testcase["clusterID"],
                    self.expected_res["defs"][1]["definition"],
                    **kwarg)
            if self.validate_testcase(result, [200], testcase, failures):
                self.log.debug("Index Creation Successful")
                self.i += 1

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_payload(self):
        testcases = list()
        for v in [0, -1, 1, 100000000, self.generate_random_string(1000),
                  None, "SELECT * FROM test WHERE c = 10", "", "abc", ]:
            testcase = {
                "description": "Testing query statement with value: "
                               "`{}`".format(v),
                "payload": v,
                "expected_status_code": 422,
                "expected_error": {
                    "code": 422,
                    "hint": "Please review your request and ensure that all "
                            "required parameters are correctly provided.",
                    "httpStatusCode": 422,
                    "message": "Statement is not index DDL"
                }
            }
            testcases.append(testcase)
        failures = list()
        for testcase in testcases:
            self.log.info(testcase["description"])
            res = self.capellaAPI.cluster_ops_apis.manage_query_indices(
                self.organisation_id, self.project_id, self.cluster_id,
                testcase["payload"])
            if res.status_code == 429:
                self.handle_rate_limit(self.curr_owner_key)
                res = self.capellaAPI.cluster_ops_apis.manage_query_indices(
                    self.organisation_id, self.project_id, self.cluster_id,
                    testcase["payload"])
            self.validate_testcase(res, [200], testcase, failures,
                                   payloadTest=True)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.manage_query_indices, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.expected_res["definition"]
            )
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.manage_query_indices, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.expected_res["definition"]
            )
        ]]
        self.throttle_test(api_func_list, True, self.project_id)
