"""
Created on November 30, 2023

@author: Vipul Bhardwaj
"""

from pytests.Capella.RestAPIv4.api_base import APIBase
import base64
import time
import itertools
from couchbase_utils.capella_utils.dedicated import CapellaUtils


class GetScope(APIBase):

    def setUp(self):
        APIBase.setUp(self)

        # Create project.
        # The project ID will be used to create API keys for roles that
        # require project ID
        self.project_id = self.capellaAPI.org_ops_apis.create_project(
            self.organisation_id,
            self.generate_random_string(prefix=self.prefix),
            self.generate_random_string(
                100, prefix=self.prefix)).json()["id"]

        # Initialize params for cluster creation.
        cluster_name = self.prefix + "TestScopesGet"
        cluster = {
            "name": cluster_name,
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
                "plan": "basic",
                "timezone": "GMT"
            }
        }

        # CIDR selection.
        start_time = time.time()
        while time.time() - start_time < 1800:
            res = self.capellaAPI.cluster_ops_apis.create_cluster(
                self.organisation_id, self.project_id, cluster_name,
                cluster['cloudProvider'], cluster['couchbaseServer'],
                cluster['serviceGroups'], cluster['availability'],
                cluster['support'])
            if res.status_code == 202:
                self.cluster_id = res.json()['id']
                cluster['id'] = self.cluster_id
                break
            elif "Please ensure you are passing a unique CIDR block" in \
                    res.json()["message"]:
                newCIDR = CapellaUtils.get_next_cidr() + "/20"
                cluster["cloudProvider"]["cidr"] = newCIDR
        if time.time() - start_time >= 1800:
            self.fail("Couldn't find CIDR within half an hour.")

        # Wait for the cluster to be deployed.
        self.log.info("Waiting for cluster to be deployed.")
        start_time = time.time()
        while (start_time + 1800 > time.time() and
                self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                self.organisation_id, self.project_id,
                self.cluster_id).json()["currentState"] == "deploying"):
            time.sleep(10)
        if self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                self.organisation_id, self.project_id,
                self.cluster_id).json()["currentState"] == "healthy":
            self.log.info("Cluster deployed successfully.")
        else:
            self.fail("Cluster didn't deploy within half an hour.")

        # Initialise params for bucket creation and assign BucketID.
        bucket_name = "TestScopesGet"
        bucket = {
            "name": bucket_name,
            "type": "couchbase",
            "storageBackend": "couchstore",
            "memoryAllocationInMb": 100,
            "bucketConflictResolution": "seqno",
            "durabilityLevel": "none",
            "replicas": 1,
            "flush": False,
            "timeToLiveInSeconds": 0
        }
        self.bucket_id = self.capellaAPI.cluster_ops_apis.create_bucket(
            self.organisation_id, self.project_id, self.cluster_id,
            bucket_name, bucket['type'], bucket['storageBackend'],
            bucket['memoryAllocationInMb'], bucket['bucketConflictResolution'],
            bucket['durabilityLevel'], bucket['replicas'],
            bucket['flush'], bucket['timeToLiveInSeconds']
        ).json()['id']

        # Initialize scope params and create a scope.
        self.scope_name = self.generate_random_string(5, False, self.prefix)
        self.expected_result = {
            "collections": [],
            "name": self.scope_name,
            "uid": None
        }
        res = self.capellaAPI.cluster_ops_apis.create_scope(
            self.organisation_id, self.project_id, self.cluster_id,
            self.bucket_id, self.scope_name)
        if res.status_code != 201:
            self.fail("Scope creation failed!")
        self.log.info("Scope: {} creation successful".format(self.scope_name))

    def tearDown(self):
        failures = list()

        self.update_auth_with_api_token(self.org_owner_key["token"])
        self.delete_api_keys(self.api_keys)

        # Delete scope
        self.log.info("Deleting scope: {}".format(self.scope_name))
        if self.capellaAPI.cluster_ops_apis.delete_scope(
                self.organisation_id, self.project_id, self.cluster_id,
                self.bucket_id, self.scope_name).status_code != 200:
            failures.append("Error while deleting Scope.")
        else:
            self.log.info("Scope deletion successful.")

        # Delete the bucket that was created.
        self.log.info("Deleting bucket: {}".format(self.bucket_id))
        if self.capellaAPI.cluster_ops_apis.delete_bucket(
                self.organisation_id, self.project_id, self.cluster_id,
                self.bucket_id).status_code != 204:
            failures.append("Error while deleting bucket.")
        else:
            self.log.info("Successfully deleted bucket.")

        # Delete the cluster that was created.
        self.log.info("Destroying Cluster: {}".format(self.cluster_id))
        if self.capellaAPI.cluster_ops_apis.delete_cluster(
                self.organisation_id, self.project_id,
                self.cluster_id).status_code != 202:
            failures.append("Error while deleting cluster.")

        # Wait for the cluster to be destroyed.
        self.log.info("Waiting for cluster to be destroyed.")
        while not self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                self.organisation_id, self.project_id,
                self.cluster_id).status_code == 404:
            time.sleep(10)
        self.log.info("Cluster destroyed successfully.")

        # Delete the project that was created.
        self.log.info("Deleting Project: {}".format(self.project_id))
        if self.delete_projects(self.organisation_id, [self.project_id],
                                self.org_owner_key["token"]):
            failures.append("Error while deleting project.")
        else:
            self.log.info("Project deleted successfully")

        if failures:
            self.log.error("Following error occurred in teardown: {}"
                           .format(failures))
        super(GetScope, self).tearDown()

    def validate_scope_api_response(self, expected_res, actual_res):
        for key in actual_res:
            if key not in expected_res:
                return False
            if key == "name" and actual_res[key] != self.scope_name:
                continue
            if isinstance(expected_res[key], dict):
                self.validate_scope_api_response(
                    expected_res[key], actual_res[key])
            elif expected_res[key]:
                if expected_res[key] != actual_res[key]:
                    return False
        return True

    def test_api_path(self):
        testcases = [
            {
                "description": "Fetch info for a valid scope"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}/buckets/"
                       "{}/scopes",
                "expected_status_code": 404,
                "expected_error": {
                    "errorType": "RouteNotFound",
                    "message": "Not found"
                }
            }, {
                "description": "Replace scopes with scope in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/bucket/"
                       "{}/scope",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}/buckets/"
                       "{}/scopes/scope",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Fetch scope but with non-hex organizationID",
                "invalid_organizationID": self.replace_last_character(
                    self.organisation_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if all the required params are present "
                            "in the request body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived"
                               " to be a client error."
                }
            }, {
                "description": "Fetch scope but with non-hex projectID",
                "invalid_projectID": self.replace_last_character(
                    self.project_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if all the required params are present "
                            "in the request body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived"
                               " to be a client error."
                }
            }, {
                "description": "Fetch scope but with non-hex clusterID",
                "invalid_clusterID": self.replace_last_character(
                    self.cluster_id, non_hex=True),
                "expected_status_code": 400,
                "expected_error": {
                    "code": 1000,
                    "hint": "Check if all the required params are present "
                            "in the request body.",
                    "httpStatusCode": 400,
                    "message": "The server cannot or will not process the "
                               "request due to something that is perceived"
                               " to be a client error."
                }
            }, {
                "description": "Fetch scope but with invalid bucketID",
                "invalid_bucketID": self.replace_last_character(
                    self.bucket_id),
                "expected_status_code": 400,
                "expected_error": {
                    "code": 400,
                    "hint": "Please review your request and ensure that "
                            "all required parameters are correctly "
                            "provided.",
                    "message": "BucketID is invalid.",
                    "httpStatusCode": 400
                }
            }, {
                "description": "Fetch scope but with invalid scopeName",
                "invalid_scopeName": self.replace_last_character(
                    self.scope_name),
                "expected_status_code": 404,
                "expected_error": {
                    "code": 11002,
                    "hint": "The requested scope details could not be found "
                            "or fetched. Please ensure that the correct scope "
                            "name is provided.",
                    "httpStatusCode": 404,
                    "message": "Scope Not Found"
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
            scope = self.scope_name

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.scope_endpoint = \
                    testcase["url"]
            if "invalid_organizationID" in testcase:
                org = testcase["invalid_organizationID"]
            elif "invalid_projectID" in testcase:
                proj = testcase["invalid_projectID"]
            elif "invalid_clusterID" in testcase:
                clus = testcase["invalid_clusterID"]
            elif "invalid_bucketID" in testcase:
                buck = testcase["invalid_bucketID"]
            elif "invalid_scopeName" in testcase:
                scope = testcase["invalid_scopeName"]

            result = self.capellaAPI.cluster_ops_apis.fetch_scope_info(
                org, proj, clus, buck, scope)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.fetch_scope_info(
                    org, proj, clus, buck, scope)
            if result.status_code == 200 and "expected_error" not in testcase:
                if not self.validate_scope_api_response(
                        self.expected_result, result.json()):
                    self.log.error("Status == 200, Key validation Failure : {}"
                                   .format(testcase["description"]))
                    self.log.warning("Result : {}".format(result.json()))
                    failures.append(testcase["description"])
            elif result.status_code >= 500:
                self.log.critical(testcase["description"])
                self.log.warning(result.content)
                failures.append(testcase["description"])
                continue
            elif result.status_code == testcase["expected_status_code"]:
                try:
                    result = result.json()
                    for key in result:
                        if result[key] != testcase["expected_error"][key]:
                            self.log.error("Status != 200, Key validation "
                                           "Failure : {}".format(
                                            testcase["description"]))
                            self.log.warning("Result : {}".format(result))
                            failures.append(testcase["description"])
                            break
                except (Exception,):
                    if str(testcase["expected_error"]) not in result.content:
                        self.log.error("Response type not JSON, Failure : {}"
                                       .format(testcase["description"]))
                        self.log.warning(result.content)
                        failures.append(testcase["description"])
            else:
                self.log.error("Expected HTTP status code {}, Actual "
                               "HTTP status code {}".format(
                                testcase["expected_status_code"],
                                result.status_code))
                self.log.warning("Result : {}".format(result.content))
                failures.append(testcase["description"])
            self.capellaAPI.cluster_ops_apis.scope_endpoint = "/v4/" \
                "organizations/{}/projects/{}/clusters/{}/buckets/{}/scopes"

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests".format(
                len(failures), len(testcases)))

    def test_authorization(self):
        self.api_keys.update(
            self.create_api_keys_for_all_combinations_of_roles(
                [self.project_id]))

        resp = self.capellaAPI.org_ops_apis.create_project(
            organizationId=self.organisation_id,
            name=self.generate_random_string(prefix=self.prefix),
            description=self.generate_random_string(100))
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
            if not any(element in ["organizationOwner", "projectDataReader",
                                   "projectOwner", "projectDataReaderWriter",
                                   "projectViewer", "projectManager"] for
                       element in self.api_keys[role]["roles"]):
                testcase["expected_error"] = {
                    "code": 1003,
                    "hint": "Make sure you have adequate access to the "
                            "resource.",
                    "httpStatusCode": 403,
                    "message": "Access Denied."
                }
                testcase["expected_status_code"] = 403
            testcases.append(testcase)
        testcases.extend([
            {
                "description": "Calling API without bearer token",
                "token": "",
                "expected_status_code": 401,
                "expected_error": {
                    "code": 1001,
                    "hint": "The request is unauthorized. Please ensure you "
                            "have provided appropriate credentials in the "
                            "request header. Please make sure the client IP "
                            "that is trying to access the resource using the "
                            "API key is in the API key allowlist.",
                    "httpStatusCode": 401,
                    "message": "Unauthorized"
                }
            }, {
                "description": "calling API with expired API keys",
                "expire_key": True,
                "expected_status_code": 401,
                "expected_error": {
                    "code": 1001,
                    "hint": "The request is unauthorized. Please ensure you "
                            "have provided appropriate credentials in the "
                            "request header. Please make sure the client IP "
                            "that is trying to access the resource using the "
                            "API key is in the API key allowlist.",
                    "httpStatusCode": 401,
                    "message": "Unauthorized"
                }
            }, {
                "description": "calling API with revoked API keys",
                "revoke_key": True,
                "expected_status_code": 401,
                "expected_error": {
                    "code": 1001,
                    "hint": "The request is unauthorized. Please ensure you "
                            "have provided appropriate credentials in the "
                            "request header. Please make sure the client IP "
                            "that is trying to access the resource using the "
                            "API key is in the API key allowlist.",
                    "httpStatusCode": 401,
                    "message": "Unauthorized"
                }
            }, {
                "description": "Calling API with Username and Password",
                "userpwd": True,
                "expected_status_code": 401,
                "expected_error": {
                    "code": 1001,
                    "hint": "The request is unauthorized. Please ensure you "
                            "have provided appropriate credentials in the "
                            "request header. Please make sure the client IP "
                            "that is trying to access the resource using the "
                            "API key is in the API key allowlist.",
                    "httpStatusCode": 401,
                    "message": "Unauthorized"
                }
            }, {
                "description": "Calling API with user having access to get "
                               "multiple projects ",
                "has_multi_project_access": True
            }, {
                "description": "Calling API with user not having access to "
                               "get project specific but has access to get "
                               "other project",
                "has_multi_project_access": False,
                "expected_status_code": 403,
                "expected_error": {
                    "code": 1003,
                    "hint": "Make sure you have adequate access to the "
                            "resource.",
                    "message": "Access Denied.",
                    "httpStatusCode": 403
                }
            }
        ])

        failures = list()
        header = dict()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))

            if "expire_key" in testcase:
                self.update_auth_with_api_token(self.org_owner_key["token"])
                # create a new API key with expiry of approx 2 mins
                resp = self.capellaAPI.org_ops_apis.create_api_key(
                    organizationId=self.organisation_id,
                    name=self.generate_random_string(),
                    description=self.generate_random_string(
                        50, prefix=self.prefix),
                    organizationRoles=["organizationOwner"],
                    expiry=0.001
                )
                if resp.status_code == 201:
                    self.api_keys["organizationOwner_new"] = resp.json()
                else:
                    self.fail("Error while creating API key for organization "
                              "owner with expiry of 0.001 days")
                # wait for key to expire
                self.log.debug("Sleeping 3 minutes for key to expire")
                time.sleep(180)
                self.update_auth_with_api_token(
                    self.api_keys["organizationOwner_new"]["token"])
                del self.api_keys["organizationOwner_new"]
            elif "revoke_key" in testcase:
                self.update_auth_with_api_token(self.org_owner_key["token"])
                resp = self.capellaAPI.org_ops_apis.delete_api_key(
                    organizationId=self.organisation_id,
                    accessKey=self.api_keys["organizationOwner"]["id"])
                if resp.status_code != 204:
                    failures.append(testcase["description"])
                self.update_auth_with_api_token(
                    self.api_keys["organizationOwner"]["token"])
                del self.api_keys["organizationOwner"]
            elif "userpwd" in testcase:
                basic = base64.b64encode("{}:{}".format(
                    self.user, self.passwd).encode()).decode()
                header["Authorization"] = 'Basic {}'.format(basic)
            elif "has_multi_project_access" in testcase:
                header = {}
                org_roles = ["organizationMember"]
                resource = [{
                    "type": "project",
                    "id": other_project_id,
                    "roles": ["projectOwner"]
                }]
                if testcase["has_multi_project_access"]:
                    key = "multi_project_1"
                    resource.append({
                        "type": "project",
                        "id": self.project_id,
                        "roles": ["projectOwner"]
                    })
                else:
                    key = "multi_project_2"
                    org_roles.append("projectCreator")
                self.update_auth_with_api_token(self.org_owner_key["token"])
                # create a new API key with expiry of approx 2 mins
                resp = self.capellaAPI.org_ops_apis.create_api_key(
                    organizationId=self.organisation_id,
                    name=self.generate_random_string(),
                    organizationRoles=org_roles,
                    description=self.generate_random_string(
                        50, prefix=self.prefix),
                    expiry=180,
                    allowedCIDRs=["0.0.0.0/0"],
                    resources=resource)
                if resp.status_code == 201:
                    self.api_keys[key] = resp.json()
                else:
                    self.fail(
                        "Error while creating API key for role having access "
                        "to multiple projects")
                self.update_auth_with_api_token(self.api_keys[key]["token"])
            else:
                header = {}
                self.update_auth_with_api_token(testcase["token"])

            result = self.capellaAPI.cluster_ops_apis.fetch_scope_info(
                self.organisation_id, self.project_id, self.cluster_id,
                self.bucket_id, self.scope_name, header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.fetch_scope_info(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.bucket_id, self.scope_name, header)
            if result.status_code == 200 and "expected_error" not in testcase:
                if not self.validate_scope_api_response(
                        self.expected_result, result.json()):
                    self.log.error("Status == 200, Key validation Failure : {}"
                                   .format(testcase["description"]))
                    failures.append(testcase["description"])
            elif result.status_code >= 500:
                self.log.critical(testcase["description"])
                self.log.warning(result.content)
                failures.append(testcase["description"])
                continue
            elif result.status_code == testcase["expected_status_code"]:
                try:
                    result = result.json()
                    for key in result:
                        if result[key] != testcase["expected_error"][key]:
                            self.log.error("Status != 200, Key validation "
                                           "Failure : {}".format(
                                            testcase["description"]))
                            self.log.warning("Failure : {}".format(result))
                            failures.append(testcase["description"])
                            break
                except (Exception,):
                    if str(testcase["expected_error"]) not in result.content:
                        self.log.error("Response type not JSON, Failure : {}"
                                       .format(testcase["description"]))
                        self.log.warning(result.content)
                        failures.append(testcase["description"])
            else:
                self.log.error("Expected HTTP status code {}, Actual "
                               "HTTP status code {}".format(
                                testcase["expected_status_code"],
                                result.status_code))
                self.log.warning("Result : {}".format(result.content))
                failures.append(testcase["description"])

        resp = self.capellaAPI.org_ops_apis.delete_project(
            self.organisation_id, other_project_id)
        if resp.status_code != 204:
            failures.append("Error while deleting project {}".format(
                other_project_id))

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests".format(
                len(failures), len(testcases)))

    def test_query_parameters(self):
        self.log.debug("Correct Params - OrgID: {}, ProjID: {}, ClusID: {}, Bu"
                       "ckID: {}, ScopeName: {}".format(
                        self.organisation_id, self.project_id, self.cluster_id,
                        self.bucket_id, self.scope_name))
        combinations = self.create_path_combinations(
            org_id=self.organisation_id, proj_id=self.project_id,
            clus_id=self.cluster_id, buck_id=self.bucket_id,
            scope_name=self.scope_name)

        testcases = list()
        for combination in combinations:
            testcase = {
                "description": "OrganizationID: {}, ProjectID: {}, "
                               "ClusterID: {}, BucketID: {}, ScopeName: {}"
                .format(str(combination[0]), str(combination[1]),
                        str(combination[2]), str(combination[3]),
                        str(combination[4])),
                "organizationID": combination[0],
                "projectID": combination[1],
                "clusterID": combination[2],
                "bucketID": combination[3],
                "scopeName": combination[4]
            }
            if not (combination[0] == self.organisation_id and
                    combination[1] == self.project_id and
                    combination[2] == self.cluster_id and
                    combination[3] == self.bucket_id and
                    combination[4] == self.scope_name):
                if (combination[1] == "" or combination[0] == "" or
                        combination[2] == "" or combination[3] == "" or
                        combination[4] == ""):
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
                        "hint": "Please review your request and ensure that "
                                "all required parameters are correctly "
                                "provided.",
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
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = {
                        "code": 11002,
                        "hint": "The requested scope details could not be "
                                "found or fetched. Please ensure that the "
                                "correct scope name is provided.",
                        "httpStatusCode": 404,
                        "message": "Scope Not Found"
                    }
            testcases.append(testcase)

        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            if "param" in testcase:
                kwarg = {testcase["param"]: testcase["paramValue"]}
            else:
                kwarg = dict()

            result = self.capellaAPI.cluster_ops_apis.fetch_scope_info(
                testcase["organizationID"], testcase["projectID"],
                testcase["clusterID"], testcase["bucketID"],
                testcase["scopeName"], **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.fetch_scope_info(
                    testcase["organizationID"], testcase["projectID"],
                    testcase["clusterID"], testcase["bucketID"],
                    testcase["scopeName"], **kwarg)
            if result.status_code == 200 and "expected_error" not in testcase:
                if not self.validate_scope_api_response(
                        self.expected_result, result.json()):
                    self.log.error("Status == 200, Key validation Failure : {}"
                                   .format(testcase["description"]))
                    failures.append(testcase["description"])
            elif result.status_code >= 500:
                self.log.critical(testcase["description"])
                self.log.warning(result.content)
                failures.append(testcase["description"])
                continue
            elif result.status_code == testcase["expected_status_code"]:
                try:
                    result = result.json()
                    for key in result:
                        if result[key] != testcase["expected_error"][key]:
                            self.log.error("Status != 200, Key validation "
                                           "failed for Test : {}".format(
                                            testcase["description"]))
                            self.log.warning("Failure : {}".format(result))
                            failures.append(testcase["description"])
                            break
                except (Exception,):
                    if str(testcase["expected_error"]) not in result.content:
                        self.log.error("Response type not JSON, Test : {}"
                                       .format(testcase["description"]))
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
            self.fail("{} tests FAILED out of {} TOTAL tests".format(
                len(failures), len(testcases)))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[self.capellaAPI.cluster_ops_apis.fetch_scope_info,
                          (self.organisation_id, self.project_id,
                           self.cluster_id, self.bucket_id, self.scope_name)]]

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
                    self.fail("Rate limit was hit after {0} API calls. This is"
                              " definitely an issue.".format(results[result][
                                "total_api_calls_made_to_hit_rate_limit"]))

        results = self.make_parallel_api_calls(
            99, api_func_list, self.api_keys)
        for result in results:
            # Removing failure for tests which are intentionally ran for
            # unauthorized roles, ie, which give a 403 response.
            if "403" in results[result]["4xx_errors"]:
                del results[result]["4xx_errors"]["403"]

            if len(results[result]["4xx_errors"]) > 0 or len(
                    results[result]["5xx_errors"]) > 0:
                self.fail("Some API calls failed")

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[self.capellaAPI.cluster_ops_apis.fetch_scope_info,
                          (self.organisation_id, self.project_id,
                           self.cluster_id, self.bucket_id, self.scope_name)]]

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
                    self.fail("Rate limit was hit after {0} API calls. This is"
                              " definitely an issue.".format(results[result][
                                "total_api_calls_made_to_hit_rate_limit"]))

        results = self.make_parallel_api_calls(
            99, api_func_list, self.api_keys)
        for result in results:
            # Removing failure for tests which are intentionally ran for
            # unauthorized roles, ie, which give a 403 response.
            if "403" in results[result]["4xx_errors"]:
                del results[result]["4xx_errors"]["403"]

            if len(results[result]["4xx_errors"]) > 0 or len(
                    results[result]["5xx_errors"]) > 0:
                self.fail("Some API calls failed")
