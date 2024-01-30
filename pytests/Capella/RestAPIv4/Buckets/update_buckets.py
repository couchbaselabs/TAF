"""
Created on August 22, 2023

@author: Vipul Bhardwaj
"""

import copy
import base64
import time
from pytests.Capella.RestAPIv4.Clusters.get_clusters import GetCluster


class UpdateBucket(GetCluster):

    def setUp(self, nomenclature="Buckets_Update"):
        GetCluster.setUp(self, nomenclature)

        # Initialise bucket params and create a bucket.
        self.expected_result = {
            "name": self.generate_random_string(
                5, False, self.prefix + nomenclature),
            "type": "couchbase",
            "storageBackend": "couchstore",
            "memoryAllocationInMb": 100,
            "bucketConflictResolution": "seqno",
            "durabilityLevel": "none",
            "replicas": 2,
            "flush": False,
            "timeToLiveInSeconds": 0,
            "evictionPolicy": "fullEviction",
            "stats": {
                "itemCount": None,
                "opsPerSecond": None,
                "diskUsedInMib": None,
                "memoryUsedInMib": None
            }
        }
        self.bucket_id = self.capellaAPI.cluster_ops_apis.create_bucket(
            self.organisation_id, self.project_id, self.cluster_id,
            self.expected_result['name'], "couchbase", "couchstore", 100,
            "seqno", "none", 1, False, 0).json()['id']

    def tearDown(self):
        self.update_auth_with_api_token(self.org_owner_key["token"])

        # Delete the bucket that was created.
        self.log.info("Deleting bucket: {}".format(self.bucket_id))
        if self.capellaAPI.cluster_ops_apis.delete_bucket(
                self.organisation_id, self.project_id, self.cluster_id,
                self.bucket_id).status_code != 204:
            self.log.error("Error while deleting bucket.")
        else:
            self.log.info("Successfully deleted bucket.")

        super(UpdateBucket, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Valid bucket update."
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}/buckets",
                "expected_status_code": 404,
                "expected_error": {
                    "errorType": "RouteNotFound",
                    "message": "Not found"
                }
            }, {
                "description": "Replace buckets with bucket in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/bucket",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{"
                       "}/buckets/bucket",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Update bucket but with non-hex organizationID",
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
                "description": "Update bucket but with non-hex projectID",
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
                "description": "Update bucket but with non-hex clusterID",
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
            }, {
                "description": "Update bucket but with invalid bucketID",
                "invalid_bucketID": self.replace_last_character(
                    self.bucket_id),
                "expected_status_code": 404,
                "expected_error": {
                    "code": 6008,
                    "hint": "The requested bucket does not exist. Please "
                            "ensure that the correct bucket ID is provided.",
                    "httpStatusCode": 404,
                    "message": "Unable to find the specified bucket."
                }
            }
        ]
        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            org = self.organisation_id
            proj = self.project_id
            clus = self.cluster_id
            buck = self.bucket_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.bucket_endpoint = \
                    testcase["url"]
            if "invalid_organizationID" in testcase:
                org = testcase["invalid_organizationID"]
            elif "invalid_projectID" in testcase:
                proj = testcase["invalid_projectID"]
            elif "invalid_clusterID" in testcase:
                clus = testcase["invalid_clusterID"]
            elif "invalid_bucketID" in testcase:
                buck = testcase["invalid_bucketID"]

            self.log.info("Updating bucket.")
            result = self.capellaAPI.cluster_ops_apis.update_bucket_config(
                org, proj, clus, buck,
                self.expected_result['memoryAllocationInMb'],
                self.expected_result['durabilityLevel'],
                self.expected_result['replicas'], True, 10, False
            )
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.update_bucket_config(
                    org, proj, clus, buck,
                    self.expected_result['memoryAllocationInMb'],
                    self.expected_result['durabilityLevel'],
                    self.expected_result['replicas'], True, 10, False
                )
            if result.status_code == 204:
                if "expected_error" in testcase:
                    self.log.error(testcase["description"])
                    failures.append(testcase["description"])
                else:
                    self.log.debug("Updation Successful.")
            else:
                self.validate_testcase(result, 204, testcase, failures)

            self.capellaAPI.cluster_ops_apis.bucket_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters/{}/buckets"

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

            # Wait for cluster to rebalance (if it is).
            self.update_auth_with_api_token(self.org_owner_key['token'])
            res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                self.organisation_id, self.project_id, self.cluster_id)
            if res.status_code == 429:
                self.handle_rate_limit(int(res.headers["Retry-After"]))
                res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                    self.organisation_id, self.project_id, self.cluster_id)
            while res.json()["currentState"] != "healthy":
                self.log.warning("Waiting for cluster to rebalance.")
                time.sleep(10)
                res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                    self.organisation_id, self.project_id, self.cluster_id)
                if res.status_code == 429:
                    self.handle_rate_limit(int(res.headers["Retry-After"]))
                    res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                        self.organisation_id, self.project_id, self.cluster_id)
            self.log.debug("Cluster state healthy.")

            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, other_project_id)
            result = self.capellaAPI.cluster_ops_apis.update_bucket_config(
                self.organisation_id, self.project_id, self.cluster_id,
                self.bucket_id, self.expected_result['memoryAllocationInMb'],
                self.expected_result['durabilityLevel'],
                self.expected_result['replicas'], True, 10, False, header
            )
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.update_bucket_config(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.bucket_id,
                    self.expected_result['memoryAllocationInMb'],
                    self.expected_result['durabilityLevel'],
                    self.expected_result['replicas'], True, 10, False
                )
            if result.status_code == 204:
                if "expected_error" in testcase:
                    self.log.error(testcase["description"])
                    failures.append(testcase["description"])
                else:
                    self.log.debug("Updation Successful.")
            else:
                self.validate_testcase(result, 204, testcase, failures)

        self.update_auth_with_api_token(self.org_owner_key["token"])
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
        self.log.debug("Correct Params - OrgID: {}, ProjID: {}, ClusID: {}, Bu"
                       "ckID: {}".format(self.organisation_id, self.project_id,
                                         self.cluster_id, self.bucket_id))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id, self.cluster_id,
                self.bucket_id):
            testcases += 1
            testcase = {
                "description": "OrganizationID: {}, ProjectID: {}, "
                               "ClusterID: {}, BucketID: {}".format(
                                str(combination[0]), str(combination[1]),
                                str(combination[2]), str(combination[3])),
                "organizationID": combination[0],
                "projectID": combination[1],
                "clusterID": combination[2],
                "bucketID": combination[3]
            }
            if not (combination[0] == self.organisation_id and
                    combination[1] == self.project_id and
                    combination[2] == self.cluster_id and
                    combination[3] == self.bucket_id):
                if (combination[1] == "" or combination[0] == "" or
                        combination[2] == "" or combination[3] == ""):
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = "404 page not found"
                elif any(variable in [
                     int, bool, float, list, tuple, set, type(None)] for
                              variable in [type(combination[0]),
                                           type(combination[1]),
                                           type(combination[2])]):
                    testcase["expected_status_code"] = 400
                    testcase["expected_error"] = {
                        "code": 1000,
                        "hint": "Check if all the required params are present "
                                "in the request body.",
                        "message": "The server cannot or will not process the "
                                   "request due to something that is perceived"
                                   " to be a client error.",
                        "httpStatusCode": 400
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
                elif combination[3] != self.bucket_id and not \
                        isinstance(combination[3], type(None)):
                    testcase["expected_status_code"] = 400
                    testcase["expected_error"] = {
                        "code": 400,
                        "hint": "Please review your request and ensure "
                                "that all required parameters are "
                                "correctly provided.",
                        "message": "BucketID is invalid.",
                        "httpStatusCode": 400
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
                elif combination[1] != self.project_id:
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
                elif isinstance(combination[3], type(None)):
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = {
                        "code": 6008,
                        "hint": "The requested bucket does not exist. Please "
                                "ensure that the correct bucket ID is "
                                "provided.",
                        "httpStatusCode": 404,
                        "message": "Unable to find the specified bucket."
                    }
                else:
                    testcase["expected_status_code"] = 400
                    testcase["expected_error"] = {
                        "code": 400,
                        "hint": "Please review your request and ensure that "
                                "all required parameters are correctly "
                                "provided.",
                        "message": "BucketID is invalid.",
                        "httpStatusCode": 400
                    }
            self.log.info("Executing test: {}".format(testcase["description"]))
            if "param" in testcase:
                kwarg = {testcase["param"]: testcase["paramValue"]}
            else:
                kwarg = dict()

            # Wait for cluster to rebalance (if it is).
            self.update_auth_with_api_token(self.org_owner_key['token'])
            res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                self.organisation_id, self.project_id, self.cluster_id)
            if res.status_code == 429:
                self.handle_rate_limit(int(res.headers["Retry-After"]))
                res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                    self.organisation_id, self.project_id, self.cluster_id)
            while res.json()["currentState"] != "healthy":
                self.log.warning("Waiting for cluster to rebalance.")
                time.sleep(10)
                res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                    self.organisation_id, self.project_id, self.cluster_id)
                if res.status_code == 429:
                    self.handle_rate_limit(int(res.headers["Retry-After"]))
                    res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                        self.organisation_id, self.project_id,
                        self.cluster_id)
            self.log.debug("Cluster state healthy.")

            result = self.capellaAPI.cluster_ops_apis.update_bucket_config(
                testcase["organizationID"], testcase["projectID"],
                testcase["clusterID"], testcase['bucketID'],
                self.expected_result['memoryAllocationInMb'],
                self.expected_result['durabilityLevel'],
                self.expected_result['replicas'], True, 10, False, **kwarg
            )
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.update_bucket_config(
                    testcase["organizationID"], testcase["projectID"],
                    testcase["clusterID"], testcase['bucketID'],
                    self.expected_result['memoryAllocationInMb'],
                    self.expected_result['durabilityLevel'],
                    self.expected_result['replicas'], True, 10, False, **kwarg
                )
            if result.status_code == 204:
                if "expected_error" in testcase:
                    self.log.error(testcase["description"])
                    failures.append(testcase["description"])
                else:
                    self.log.debug("Updation Successful.")
            else:
                self.validate_testcase(result, 204, testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_payload(self):
        testcases = list()

        for key in self.expected_result:
            if key in ["name", "stats", "evictionPolicy", "type",
                       "storageBackend", "bucketConflictResolution"]:
                continue

            values = [
                "", 1, 0, 100000, -1, 123.123, self.generate_random_string(),
                self.generate_random_string(500, special_characters=False),
            ]
            for value in values:
                testcase = copy.deepcopy(self.expected_result)
                testcase[key] = value

                for param in ["name", "stats", "evictionPolicy", "type",
                              "storageBackend", "bucketConflictResolution"]:
                    del testcase[param]
                testcase["description"] = "Testing '{}' with value: " \
                                          "{}".format(key, value)

                if (
                        (key in ["memoryAllocationInMb",
                                 "timeToLiveInSeconds", "replicas"]
                            and not isinstance(value, int)) or
                        (key == "durabilityLevel"
                         and not isinstance(value, str)) or
                        (key == "flush" and not isinstance(value, bool))
                ):
                    testcase["expected_status_code"] = 400
                    testcase["expected_error"] = {
                        "code": 1000,
                        "hint": "The request was malformed or invalid.",
                        "httpStatusCode": 400,
                        "message": 'Bad Request. Error: body contains '
                                   'incorrect JSON type for '
                                   'field "{}".'.format(key)
                    }
                elif (
                 (key == 'timeToLiveInSeconds' and (value >= 0))
                 or
                 (key == 'replicas' and value in range(0, 4))
                 or
                 (key == 'flush' and isinstance(value, bool))
                 or
                 (key == 'memoryAllocationInMb' and isinstance(
                            value, int)
                         and value >= 100)
                 or
                 (key == 'durabilityLevel' and value in [
                            "none", "majority", "majorityAndPersistActive",
                            "persistToMajority"])
                ):
                    continue
                elif key == "durabilityLevel":
                    testcase["expected_status_code"] = 422
                    testcase["expected_error"] = {
                        "code": 6005,
                        "hint": "The provided durability level is not "
                                "supported. The supported levels are 'none', "
                                "'majority', 'persistToMajority', and "
                                "'majorityAndPersistActive'. Please choose a "
                                "valid durability level for the bucket.",
                        "httpStatusCode": 422,
                        "message": "The durability level {} provided is not "
                                   "supported. The supported level are 'none',"
                                   " 'majority', 'persistToMajority', and "
                                   "'majorityAndPersistActive'.".format(value)
                    }
                elif key == "memoryAllocationInMb" and value < 100:
                    testcase["expected_status_code"] = 422
                    testcase["expected_error"] = {
                        "code": 6023,
                        "hint": "Cannot create bucket. The requested size of "
                                "the bucket is less than the minimum amount "
                                "of 100MB. Please choose a larger size for "
                                "the bucket.",
                        "httpStatusCode": 422,
                        "message": "Cannot create bucket. The requested size "
                                   "of the bucket is less than the minimum "
                                   "amount of 100MB."
                    }
                elif key == "replicas":
                    testcase["expected_status_code"] = 422
                    if value < 1:
                        testcase["expected_error"] = {
                            "code": 6026,
                            "hint": "The replica count provided for the "
                                    "bucket is not valid. The minimum number "
                                    "of replicas is 0. Please increase the "
                                    "number of replicas for the bucket.",
                            "httpStatusCode": 422,
                            "message": "The replica count provided for the "
                                       "buckets is not valid. The minimum "
                                       "number of replicas is (0)."
                        }
                    elif value > 3:
                        testcase["expected_error"] = {
                            "code": 6024,
                            "hint": "The replica count provided for the "
                                    "bucket is not valid. The maximum number "
                                    "of replicas is 3. Please reduce the "
                                    "number of replicas for the bucket.",
                            "httpStatusCode": 422,
                            "message": "The replica count provided for the "
                                       "buckets is not valid. The maximum "
                                       "number of replicas is (3)."
                        }
                elif key == "timeToLiveInSeconds":
                    testcase["expected_status_code"] = 422
                    testcase["expected_error"] = {
                        "code": 8021,
                        "hint": "Returned when a time-to-live unit that is "
                                "not supported is given during bucket "
                                "creation. This should be a non-negative "
                                "value.",
                        "httpStatusCode": 422,
                        "message": "The time-to-live value provided is not "
                                   "supported. It should be a non-negative "
                                   "integer."
                    }
                else:
                    testcase["expected_status_code"] = 400
                    testcase["expected_error"] = {
                        "code": 400,
                        "hint": "Please review your request and ensure that "
                                "all required parameters are correctly "
                                "provided.",
                        "httpStatusCode": 400,
                        "message": "Bad Request"
                    }

                testcases.append(testcase)

        failures = list()
        for testcase in testcases:
            self.log.info(testcase['description'])

            # Wait for cluster to rebalance (if it is).
            self.update_auth_with_api_token(
                self.org_owner_key['token'])
            res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                self.organisation_id, self.project_id, self.cluster_id)
            if res.status_code == 429:
                self.handle_rate_limit(int(res.headers["Retry-After"]))
                res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                    self.organisation_id, self.project_id,
                    self.cluster_id)
            while res.json()["currentState"] != "healthy":
                self.log.warning("Waiting for cluster to rebalance.")
                time.sleep(10)
                res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                    self.organisation_id, self.project_id,
                    self.cluster_id)
                if res.status_code == 429:
                    self.handle_rate_limit(
                        int(res.headers["Retry-After"]))
                    res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                        self.organisation_id, self.project_id,
                        self.cluster_id)
            self.log.debug("Cluster state healthy.")

            result = self.capellaAPI.cluster_ops_apis.update_bucket_config(
                self.organisation_id, self.project_id, self.cluster_id,
                self.bucket_id, testcase["memoryAllocationInMb"],
                testcase["durabilityLevel"], testcase['replicas'],
                testcase['flush'], testcase['timeToLiveInSeconds'], False
            )
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.update_bucket_config(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.bucket_id, testcase["memoryAllocationInMb"],
                    testcase["durabilityLevel"], testcase['replicas'],
                    testcase['flush'], testcase['timeToLiveInSeconds'], False
                )
            if result.status_code == 204:
                if "expected_error" in testcase:
                    self.log.error(testcase["description"])
                    failures.append(testcase["description"])
                else:
                    self.log.debug("Updation Successful.")
            else:
                if result.status_code >= 500:
                    self.log.critical(testcase["description"])
                    self.log.warning(result.content)
                    failures.append(testcase["description"])
                    continue
                if result.status_code == testcase["expected_status_code"]:
                    try:
                        result = result.json()
                        for key in result:
                            if result[key] != testcase["expected_error"][key]:
                                self.log.error("Status != 204, Key validation "
                                               "failed for Test : {}".format(
                                                testcase["description"]))
                                self.log.warning("Failure : {}".format(result))
                                failures.append(testcase["description"])
                                break
                    except (Exception,):
                        if str(testcase["expected_error"]) \
                                not in result.content:
                            self.log.error(
                                "Response type not JSON, Test : {}".format(
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
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.update_bucket_config,
            (self.organisation_id, self.project_id, self.cluster_id,
             self.bucket_id, self.expected_result['memoryAllocationInMb'],
             self.expected_result['durabilityLevel'],
             self.expected_result['replicas'], True, 10, False)
        ]]
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
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.update_bucket_config,
            (self.organisation_id, self.project_id, self.cluster_id,
             self.bucket_id, self.expected_result['memoryAllocationInMb'],
             self.expected_result['durabilityLevel'],
             self.expected_result['replicas'], True, 10, False)
        ]]
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
