"""
Created on July 17, 2023

@author: Vipul Bhardwaj
"""

from pytests.Capella.RestAPIv4.Clusters.get_clusters import GetCluster


class GetBucket(GetCluster):

    def setUp(self, nomenclature="Buckets_Get"):
        GetCluster.setUp(self, nomenclature)

        # Initialise bucket params and create a bucket.
        self.bucket_name = self.prefix + nomenclature
        self.expected_res = {
            "name": self.bucket_name + self.input.param("storageBackend", "magma"),
            "type": "couchbase",
            "storageBackend": self.input.param("storageBackend", "magma"),
            "bucketConflictResolution": "seqno",
            "durabilityLevel": "none",
            "replicas": 1,
            "flush": True,
            "flushEnabled": True,
            "timeToLiveInSeconds": 0,
            "evictionPolicy": "fullEviction",
            "priority": 0,
            "memoryAllocationInMb": 1024,  # Adding missing field
            "vbuckets": 1024,  # Adding missing field
            "stats": {
                "itemCount": None,
                "opsPerSecond": None,
                "diskUsedInMib": None,
                "memoryUsedInMib": None
            }
        }
        self.expected_bucketID_errors = [
            {
                "code": 6008,
                "hint": "The requested bucket does not exist. Please ensure "
                        "that the correct bucket ID is provided.",
                "httpStatusCode": 404,
                "message": "Unable to find the specified bucket."
            }, {
                "code": 400,
                "hint": "Please review your request and ensure that all "
                        "required parameters are correctly provided.",
                "httpStatusCode": 400,
                "message": "BucketID is invalid."
            }
        ]
        if self.input.param("storageBackend", "magma") == "couchstore":
            self.expected_res["memoryAllocationInMb"] = 100
        else:
            self.expected_res["memoryAllocationInMb"] = 1024

        res = self.capellaAPI.cluster_ops_apis.create_bucket(
            self.organisation_id, self.project_id, self.cluster_id,
            self.expected_res['name'], self.expected_res['type'],
            self.expected_res['storageBackend'],
            self.expected_res['memoryAllocationInMb'],
            self.expected_res['bucketConflictResolution'],
            self.expected_res['durabilityLevel'],
            self.expected_res['replicas'], self.expected_res['flush'],
            self.expected_res['timeToLiveInSeconds'])
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.create_bucket(
                self.organisation_id, self.project_id, self.cluster_id,
                self.expected_res['name'], self.expected_res['type'],
                self.expected_res['storageBackend'],
                self.expected_res['memoryAllocationInMb'],
                self.expected_res['bucketConflictResolution'],
                self.expected_res['durabilityLevel'],
                self.expected_res['replicas'], self.expected_res['flush'],
                self.expected_res['timeToLiveInSeconds'])
        if res.status_code != 201:
            self.log.error("Error : {}".format(res.content))
            self.tearDown()
            self.fail("!!!..Bucket creation failed...!!!")
        self.bucket_id = res.json()['id']
        self.expected_res['id'] = self.bucket_id
        self.buckets.append(self.bucket_id)

        # Wait for App Service to be Healthy before deleting the bucket.
        self.log.info("Polling the APP SVC: {}".format(self.app_service_id))
        self.wait_for_deployment(app_svc_id=self.app_service_id)

    def tearDown(self):
        self.update_auth_with_api_token(self.curr_owner_key)

        # Delete the buckets that were created.
        self.delete_buckets(self.buckets)
        self.log.info("Successfully deleted buckets.")

        super(GetBucket, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Fetch info for a valid bucket"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}/buckets",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
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
                "description": "Fetch bucket but with non-hex organizationID",
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
                "description": "Fetch bucket but with non-hex projectID",
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
                "description": "Fetch bucket but with non-hex clusterID",
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
                "description": "Fetch info but with invalid bucketID",
                "invalid_bucketID": self.replace_last_character(
                    self.bucket_id),
                "expected_status_code": [400, 404],
                "expected_error": self.expected_bucketID_errors
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

            result = self.capellaAPI.cluster_ops_apis.fetch_bucket_info(
                org, proj, clus, buck)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.fetch_bucket_info(
                    org, proj, clus, buck)
            self.capellaAPI.cluster_ops_apis.bucket_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters/{}/buckets"
            self.validate_testcase(result, [200], testcase, failures, True,
                                   self.expected_res, self.bucket_id)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectDataReader", "projectOwner",
            "projectDataReaderWriter", "projectViewer", "projectManager"
        ]):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = self.capellaAPI.cluster_ops_apis.fetch_bucket_info(
                self.organisation_id, self.project_id, self.cluster_id,
                self.bucket_id, headers=header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.fetch_bucket_info(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.bucket_id, headers=header)
            self.validate_testcase(result, [200], testcase, failures, True,
                                   self.expected_res, self.bucket_id)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

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

            result = self.capellaAPI.cluster_ops_apis.fetch_bucket_info(
                testcase["organizationID"], testcase["projectID"],
                testcase["clusterID"], testcase["bucketID"], **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.fetch_bucket_info(
                    testcase["organizationID"], testcase["projectID"],
                    testcase["clusterID"], testcase["bucketID"], **kwarg)
            self.validate_testcase(result, [200], testcase, failures, True,
                                   self.expected_res, self.bucket_id)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[self.capellaAPI.cluster_ops_apis.fetch_bucket_info,
                          (self.organisation_id, self.project_id,
                           self.cluster_id, self.bucket_id)]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[self.capellaAPI.cluster_ops_apis.fetch_bucket_info,
                          (self.organisation_id, self.project_id,
                           self.cluster_id, self.bucket_id)]]
        self.throttle_test(api_func_list, True, self.project_id)
