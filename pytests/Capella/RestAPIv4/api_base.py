"""
Created on June 28, 2023

@author: umang.agrawal
"""

import time
import string
import random
import itertools
import base64
from datetime import datetime
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI
from couchbase_utils.capella_utils.dedicated import CapellaUtils
from pytests.basetestcase import BaseTestCase
import threading


class APIBase(BaseTestCase):

    def setUp(self):
        BaseTestCase.setUp(self)

        self.url = self.input.capella.get("pod")
        self.user = self.input.capella.get("capella_user")
        self.passwd = self.input.capella.get("capella_pwd")
        self.organisation_id = self.input.capella.get("tenant_id")
        self.invalid_UUID = "00000000-0000-0000-0000-000000000000"
        self.prefix = "Automated_API_test_"
        self.count = 0

        self.capellaAPI = CapellaAPI(
            "https://" + self.url, "", "", self.user, self.passwd, "")
        self.create_v2_control_plane_api_key()

        # create the first V4 API KEY WITH organizationOwner role, which will
        # be used to perform further V4 api operations
        resp = self.capellaAPI.org_ops_apis.create_api_key(
            organizationId=self.organisation_id,
            name=self.generate_random_string(prefix=self.prefix),
            organizationRoles=["organizationOwner"],
            description=self.generate_random_string(
                length=50, prefix=self.prefix))
        if resp.status_code == 201:
            self.org_owner_key = resp.json()
        else:
            self.fail("Error while creating API key for organization owner")

        # update the token for capellaAPI object, so that is it being used
        # for api auth.
        self.update_auth_with_api_token(self.org_owner_key["token"])
        self.api_keys = dict()

    def tearDown(self):
        # Delete organizationOwner API key
        self.log.info("Deleting API key for role organization Owner")
        resp = self.capellaAPI.org_ops_apis.delete_api_key(
            organizationId=self.organisation_id,
            accessKey=self.org_owner_key["id"]
        )
        if resp.status_code != 204:
            self.fail("Error while deleting api key for role organization "
                      "Owner")

        if hasattr(self, "v2_control_plane_api_access_key"):
            response = self.capellaAPI.delete_control_plane_api_key(
                self.organisation_id, self.v2_control_plane_api_access_key
            )
            if response.status_code != 204:
                self.log.error("Error while deleting V2 control plane API key")
                self.fail("{}".format(response.content))
        super(APIBase, self).tearDown()

    def create_v2_control_plane_api_key(self):
        # Generate the first set of API access and secret access keys
        # Currently v2 API is being used for this.
        response = self.capellaAPI.create_control_plane_api_key(
            self.organisation_id, "initial_api"
        )
        if response.status_code == 201:
            response = response.json()
            self.v2_control_plane_api_access_key = response["id"]
            self.update_auth_with_api_token(response["token"])
        else:
            self.log.error("Error while creating V2 control plane API key")
            self.fail("{}".format(response.content))

    @staticmethod
    def generate_random_string(length=10, special_characters=True,
                               prefix=""):
        """
        Generates random name of specified length
        """
        if special_characters:
            special_characters = "!@#$%^&*()-_=+{[]}\|;:'\",.<>/?" + " " + "\t"
        else:
            special_characters = ""

        characters = string.ascii_letters + string.digits + special_characters
        name = ""
        for i in range(length):
            name += random.choice(characters)

        if prefix:
            name = prefix + name

        return name

    def handle_rate_limit(self, retry_after):
        self.log.warning("Rate Limit hit.")
        self.log.info("Sleeping for {0} for rate limit to "
                      "expire".format(retry_after))
        time.sleep(retry_after)

    def create_api_keys_for_all_combinations_of_roles(
            self, project_ids, project_roles=[], organization_roles=[]):
        if not project_roles:
            project_roles = ["projectOwner", "projectManager", "projectViewer",
                             "projectDataReaderWriter", "projectDataReader"]
        if not organization_roles:
            organization_roles = ["organizationOwner", "organizationMember",
                                  "projectCreator"]
        role_combinations = list()
        for r in range(1, len(organization_roles+project_roles) + 1):
            combinations = itertools.combinations(
                organization_roles + project_roles, r)
            role_combinations.extend([list(c) for c in combinations])

        api_key_dict = dict()
        for role_combination in role_combinations:
            o_roles = []
            p_roles = []
            resource = []
            for role in role_combination:
                if role in organization_roles:
                    o_roles.append(role)
                elif role in project_roles:
                    p_roles.append(role)
            if p_roles:
                for project_id in project_ids:
                    resource.append({
                        "type": "project",
                        "id": project_id,
                        "roles": p_roles
                    })
                # In case of project roles, organization role type of
                # organizationMember is to be added if not present.
                if "organizationMember" not in o_roles:
                    o_roles.append("organizationMember")

            resp = self.capellaAPI.org_ops_apis.create_api_key(
                organizationId=self.organisation_id,
                name=self.generate_random_string(prefix=self.prefix),
                organizationRoles=o_roles,
                description=self.generate_random_string(
                    50, prefix=self.prefix),
                expiry=180,
                allowedCIDRs=["0.0.0.0/0"],
                resources=resource)
            if resp.status_code == 429:
                self.handle_rate_limit(int(resp.headers["Retry-After"]))
                resp = self.capellaAPI.org_ops_apis.create_api_key(
                    organizationId=self.organisation_id,
                    name=self.generate_random_string(prefix=self.prefix),
                    organizationRoles=o_roles,
                    description=self.generate_random_string(
                        50, prefix=self.prefix),
                    expiry=180,
                    allowedCIDRs=["0.0.0.0/0"],
                    resources=resource)

            if resp.status_code == 201:
                api_key_dict["-".join(role_combination)] = {
                    "id": resp.json()["id"],
                    "token": resp.json()["token"],
                    "roles": role_combination
                }
            else:
                try:
                    resp = resp.json()
                    if 'errorType' in resp.json():
                        self.log.error("Error received - \n Message - {} \n "
                                       "Error Type - {}".format(
                                        resp.json()["message"],
                                        resp.json()["errorType"]))
                    else:
                        self.log.error(
                            "Error received - \n Message - {}".format(
                                resp.json()["message"]))
                except (Exception,):
                    self.log.error("Error received - {}".format(resp.content))
                # In order to delete the created keys.
                self.api_keys = api_key_dict
                self.fail("Error while generating API keys for role {}".format(
                    "-".join(role_combination)))
        self.log.info("API keys created for all combination of roles")
        return api_key_dict

    def delete_api_keys(self, api_key_dict):
        """
        Delete API keys specified.
        """
        failed_deletion = list()
        for role in api_key_dict:
            self.log.debug("Deleting API key for role {}".format(role))
            api_key_dict[role]["retry"] = 0
            while api_key_dict[role]["retry"] < 5:
                resp = self.capellaAPI.org_ops_apis.delete_api_key(
                    organizationId=self.organisation_id,
                    accessKey=api_key_dict[role]["id"]
                )
                if resp.status_code == 429:
                    self.handle_rate_limit(int(resp.headers["Retry-After"]))
                    resp = self.capellaAPI.org_ops_apis.delete_api_key(
                        organizationId=self.organisation_id,
                        accessKey=api_key_dict[role]["id"]
                    )

                if resp.status_code != 204:
                    try:
                        resp = resp.json()
                        if 'errorType' in resp.json():
                            self.log.error(
                                "Error received - \n Message - {} \n "
                                "Error Type - {}".format(
                                    resp.json()["message"],
                                    resp.json()["errorType"]))
                        else:
                            self.log.error(
                                "Error received - \n Message - {}".format(
                                    resp.json()["message"]))
                    except (Exception,):
                        self.log.error(
                            "Error received - {}".format(resp))
                    api_key_dict[role]["retry"] += 1
                    if api_key_dict[role]["retry"] == 5:
                        failed_deletion.append(role)
                else:
                    break
        if failed_deletion:
            self.fail("Error while deleting API key for roles {}".format(
                api_key_dict.keys()))
        self.log.info("All API keys were deleted")
        return failed_deletion

    def update_auth_with_api_token(self, token):
        self.capellaAPI.org_ops_apis.bearer_token = token
        self.capellaAPI.cluster_ops_apis.bearer_token = token

    """
    Method makes parallel api calls.
    param num_of_calls_per_api (int) Number of API calls per API to be made.
    param apis_to_call (list(list)) List of lists, where inner list is of
    format [api_function_call, function_args]
    param api_key_dict dict API keys to be used while making API calls
    """
    def make_parallel_api_calls(
            self, num_of_calls_per_api=100, apis_to_call=[],
            api_key_dict={}, wait_time=0):
        results = dict()
        for role in api_key_dict:
            api_key_dict[role].update({"role": role})
        api_key_list = [api_key_dict[role] for role in api_key_dict]

        threads = list()

        def call_api_with_api_key(api_role, api_func, api_args, results):
            header = {
                'Authorization': 'Bearer ' + api_role["token"],
                'Content-Type': 'application/json'
            }
            results[api_role["id"]] = {
                "role": api_role["role"],
                "rate_limit_hit": False,
                "total_api_calls_made_to_hit_rate_limit": 0,
                "2xx_status_code": {},
                "4xx_errors": {},
                "5xx_errors": {}
            }
            for i in range(num_of_calls_per_api):
                resp = api_func(*api_args, headers=header)
                results[api_role["id"]][
                    "total_api_calls_made_to_hit_rate_limit"] += 1
                if resp.status_code == 429:
                    results[api_role["id"]]["rate_limit_hit"] = True
                    self.handle_rate_limit(int(resp.headers["Retry-After"]))
                    break
                elif str(resp.status_code).startswith("2"):
                    if str(resp.status_code) in results[
                        api_role["id"]]["2xx_status_code"]:
                        results[api_role["id"]]["2xx_status_code"][
                            str(resp.status_code)] += 1
                    else:
                        results[api_role["id"]]["2xx_status_code"][
                            str(resp.status_code)] = 1
                elif str(resp.status_code).startswith("4"):
                    if str(resp.status_code) in results[
                        api_role["id"]]["4xx_errors"]:
                        results[api_role["id"]]["4xx_errors"][
                            str(resp.status_code)] += 1
                    else:
                        results[api_role["id"]]["4xx_errors"][
                            str(resp.status_code)] = 1
                elif str(resp.status_code).startswith("5"):
                    if str(resp.status_code) in results[
                        api_role["id"]]["5xx_errors"]:
                        results[api_role["id"]]["5xx_errors"][
                            str(resp.status_code)] += 1
                    else:
                        results[api_role["id"]]["5xx_errors"][
                            str(resp.status_code)] = 1

        # Submit API call tasks to the executor
        for i in range(len(api_key_list) * len(apis_to_call)):
            threads.append(threading.Thread(
                target=call_api_with_api_key,
                name="thread_{0}".format(i),
                args=(
                    api_key_list[i % len(api_key_list)],
                    apis_to_call[i % len(apis_to_call)][0],
                    apis_to_call[i % len(apis_to_call)][1],
                    results,)))

        for thread in threads:
            thread.start()
            if wait_time:
                time.sleep(wait_time)
        for thread in threads:
            thread.join()

        for result in results:
            self.log.info("API call result for API ID {0} with role {1}"
                          .format(result, results[result]["role"]))

            if results[result]["rate_limit_hit"]:
                self.log.info("Rate limit was hit after {0} API calls".format(
                    results[result]["total_api_calls_made_to_hit_rate_limit"]))

            def print_status_code_wise_results(status_code_dict):
                for status_code in status_code_dict:
                    self.log.info("Total API calls which returned {0} : {1}"
                                  .format(status_code,
                                          status_code_dict[status_code]))

            print_status_code_wise_results(results[result]["2xx_status_code"])
            print_status_code_wise_results(results[result]["4xx_errors"])
            print_status_code_wise_results(results[result]["5xx_errors"])

        return results

    @staticmethod
    def replace_last_character(id, non_hex=False):
        if non_hex:
            replaced_id = id[:-1] + 'g'
            return replaced_id

        last_char = id[-1]
        if last_char.isdigit():
            if int(last_char) == 9:
                next_char = str(int(last_char) - 1)
            else:
                next_char = str(int(last_char) + 1)
        elif last_char.isalpha():
            if last_char.lower() == 'f':
                next_char = 'a' if last_char.islower() else 'A'
            else:
                next_char = chr(ord(last_char) + 1)
        else:
            # If the last character is a special character
            next_char = chr(ord(last_char) + 1)
        replaced_id = id[:-1] + next_char
        return replaced_id

    @staticmethod
    def auth_test_extension(testcases):
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
                "has_multi_project_access": True,
            }, {
                "description": "Calling API with user not having access to "
                               "get project specific but has access to get "
                               "other project",
                "has_multi_project_access": False,
                "expected_status_code": 403,
                "expected_error": {
                    "code": 1002,
                    "hint": "Your access to the requested resource is denied. "
                            "Please make sure you have the necessary "
                            "permissions to access the resource.",
                    "httpStatusCode": 403,
                    "message": "Access Denied."
                }
            }
        ])

    def auth_test_setup(self, testcase, failures, header,
                        project_id, other_project_id=None):
        if "expire_key" in testcase:
            self.update_auth_with_api_token(self.org_owner_key["token"])
            # create a new API key with expiry of approx 2 mins
            resp = self.capellaAPI.org_ops_apis.create_api_key(
                self.organisation_id, "Expiry_Key", ["organizationOwner"],
                expiry=0.001)
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
                    "id": project_id,
                    "roles": ["projectOwner"]
                })
            else:
                key = "multi_project_2"
                org_roles.append("projectCreator")

            self.update_auth_with_api_token(self.org_owner_key["token"])

            # create a new API key with expiry of approx 2 mins
            resp = self.capellaAPI.org_ops_apis.create_api_key(
                self.organisation_id, "MultiProj_Key", org_roles,
                expiry=180, allowedCIDRs=["0.0.0.0/0"], resources=resource)
            if resp.status_code == 201:
                self.api_keys[key] = resp.json()
            else:
                self.fail("Error while creating API key for role having "
                          "access to multiple projects")
            self.update_auth_with_api_token(self.api_keys[key]["token"])
        else:
            self.update_auth_with_api_token(testcase["token"])

    def validate_testcase(self, result, success_code, testcase, failures):
        if result.status_code >= 500:
            self.log.critical(testcase["description"])
            self.log.warning(result.content)
            failures.append(testcase["description"])
            return
        elif result.status_code == testcase["expected_status_code"]:
            try:
                result = result.json()
                for key in result:
                    if result[key] != testcase["expected_error"][key]:
                        self.log.error("Status != {}, Key validation "
                                       "Failure : {}".format(
                                        success_code, testcase["description"]))
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
            self.log.error("Expected HTTP status code {}, Actual HTTP status "
                           "code {}".format(testcase["expected_status_code"],
                                            result.status_code))
            self.log.warning("Result : {}".format(result.content))
            failures.append(testcase["description"])

    def select_CIDR(self, org, proj, name, cp, cs, sg, av, sp,
                    header=None, **kwargs):
        self.log.info("Selecting CIDR for cluster deployment.")

        start_time = time.time()
        while time.time() - start_time < 1800:
            result = self.capellaAPI.cluster_ops_apis.create_cluster(
                org, proj, name, cp, cs, sg, av, sp, header, **kwargs)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.create_cluster(
                    org, proj, name, cp, cs, sg, av, sp, header, **kwargs)
            if result.status_code != 422:
                return result
            elif "Please ensure you are passing a unique CIDR block" in \
                    result.json()["message"]:
                cp["cidr"] = CapellaUtils.get_next_cidr() + "/20"
            if time.time() - start_time >= 1800:
                self.fail("Couldn't find CIDR within half an hour.")

    def wait_for_cluster_deployment(self, org_id, proj_id, clus_id, st=None):
        if not st:
            st = time.time()

        self.log.info("Waiting for cluster {} to be deployed.".format(clus_id))
        time.sleep(20)
        if st + 1800 > time.time() and \
            self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                org_id, proj_id, clus_id).json()["currentState"] != "healthy":
            self.wait_for_cluster_deployment(org_id, proj_id, clus_id, st)

        if st + 1800 <= time.time():
            self.fail("Cluster didn't deploy within half an hour.")

    def verify_project_empty(self, proj_id):
        res = self.capellaAPI.cluster_ops_apis.list_clusters(
            self.organisation_id, proj_id)
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.list_clusters(
                self.organisation_id, proj_id)
        if len(res.json()["data"]) != 0:
            time.sleep(30)
            self.verify_project_empty(proj_id)
        return

    def create_path_combinations(self, *args):
        combination_list = []
        for val in args:
            values = [val, self.replace_last_character(val), True, None,
                      123456788, 123456789.123456789, "", [val], (val,), {val}]
            combination_list.append(values)

        for combination in list(itertools.product(*combination_list)):
            yield combination

    def create_projects(self, org_id, num_projects, access_key, token,
                        prefix=""):
        projects = dict()
        self.update_auth_with_api_token(token)
        for i in range(num_projects):
            project_name = self.generate_random_string(
                special_characters=False, prefix=prefix)
            projects[project_name] = {
                "description": self.generate_random_string(
                    100, special_characters=False)
            }
            resp = self.capellaAPI.org_ops_apis.create_project(
                organizationId=org_id, name=project_name,
                description=projects[project_name]["description"])
            if resp.status_code == 429:
                self.handle_rate_limit(int(resp.headers["Retry-After"]))
                resp = self.capellaAPI.org_ops_apis.create_project(
                    organizationId=org_id, name=project_name,
                    description=projects[project_name]["description"])
            if resp.status_code == 201:
                projects[project_name]["id"] = resp.json()["id"]
            else:
                self.fail("Error while creating project {}"
                          .format(project_name))

            projects[project_name]["expected_result"] = {
                "id": projects[project_name]["id"],
                "description": projects[project_name]["description"],
                "name": project_name,
                "audit": {
                    "createdBy": access_key,
                    "createdAt": datetime.now().strftime("%Y-%m-%d"),
                    "modifiedBy": access_key,
                    "modifiedAt": datetime.now().strftime("%Y-%m-%d"),
                    "version": 1
                }
            }
        return projects

    def delete_projects(self, org_id, project_ids, token):
        project_deletion_failed = False
        self.update_auth_with_api_token(token)
        for project_id in project_ids:
            resp = self.capellaAPI.org_ops_apis.delete_project(
                organizationId=org_id, projectId=project_id)
            if resp.status_code == 429:
                self.handle_rate_limit(int(resp.headers["Retry-After"]))
                resp = self.capellaAPI.org_ops_apis.delete_project(
                    organizationId=org_id, projectId=project_id)
            if resp.status_code != 204:
                self.log.error("Error while deleting project {}".format(
                    project_id))
                project_deletion_failed = project_deletion_failed or True
        return project_deletion_failed

    def create_bucket_to_be_tested(self, org_id, proj_id, clus_id, buck_name):
        # Wait for cluster to rebalance (if it is).
        self.update_auth_with_api_token(self.org_owner_key['token'])
        res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
            self.organisation_id, proj_id, clus_id)
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                self.organisation_id, proj_id, clus_id)
        while res.json()["currentState"] != "healthy":
            self.log.warning("Waiting for cluster to rebalance.")
            time.sleep(10)
            res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                self.organisation_id, proj_id, clus_id)
            if res.status_code == 429:
                self.handle_rate_limit(int(res.headers["Retry-After"]))
                res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                    self.organisation_id, proj_id, clus_id)
        self.log.debug("Cluster state healthy.")

        resp = self.capellaAPI.cluster_ops_apis.create_bucket(
            org_id, proj_id, clus_id, buck_name, "couchbase", "couchstore",
            100, "seqno", "none", 1, False, 0)
        if resp.status_code == 429:
            self.handle_rate_limit(int(resp.headers["Retry-After"]))
            resp = self.capellaAPI.cluster_ops_apis.create_bucket(
                org_id, proj_id, clus_id, buck_name, "couchbase", "couchstore",
                100, "seqno", "none", 1, False, 0)
        if resp.status_code == 201:
            buck_id = resp.json()['id']
            self.log.debug("New bucket created, ID: {}".format(buck_id))
            return buck_id
        self.log.error(resp)
        self.fail("New bucket creation failed.")

    def delete_buckets(self, org_id, proj_id, clus_id, bucket_ids):
        bucket_deletion_failed = False
        self.update_auth_with_api_token(self.org_owner_key['token'])
        for bucket_id in bucket_ids:
            resp = self.capellaAPI.cluster_ops_apis.delete_bucket(
                org_id, proj_id, clus_id, bucket_id)
            if resp.status_code == 429:
                self.handle_rate_limit(int(resp.headers["Retry-After"]))
                resp = self.capellaAPI.cluster_ops_apis.delete_bucket(
                    org_id, proj_id, clus_id, bucket_id)
            if resp.status_code != 204:
                self.log.error("Error while deleting bucket {}".format(
                    bucket_id))
                bucket_deletion_failed = bucket_deletion_failed or True
            else:
                bucket_ids.remove(bucket_id)

            # Wait for cluster to rebalance (if it is).
            res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                self.organisation_id, proj_id, clus_id)
            if res.status_code == 429:
                self.handle_rate_limit(int(res.headers["Retry-After"]))
                res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                    self.organisation_id, proj_id, clus_id)
            while res.json()["currentState"] != "healthy":
                self.log.warning("Waiting for cluster to rebalance.")
                time.sleep(10)
                res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                    self.organisation_id, proj_id, clus_id)
                if res.status_code == 429:
                    self.handle_rate_limit(int(res.headers["Retry-After"]))
                    res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                        self.organisation_id, proj_id, clus_id)
            self.log.debug("Cluster state healthy.")

        return bucket_deletion_failed

    def create_scope_to_be_tested(self, org_id, proj_id, clus_id, buck_id):
        self.update_auth_with_api_token(self.org_owner_key["token"])

        new_scope_name = self.generate_random_string(5, False, self.prefix)
        res = self.capellaAPI.cluster_ops_apis.create_scope(
            org_id, proj_id, clus_id, buck_id, new_scope_name)
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.create_scope(
                org_id, proj_id, clus_id, buck_id, new_scope_name)
        if res.status_code == 201:
            return new_scope_name
        self.fail("Scope creation unsuccessful.")

    def create_collection_to_be_tested(self, org_id, proj_id, clus_id,
                                       buck_id, scope_name):
        self.update_auth_with_api_token(self.org_owner_key["token"])

        new_coll_name = self.generate_random_string(5, False, self.prefix)
        res = self.capellaAPI.cluster_ops_apis.create_collection(
            org_id, proj_id, clus_id, buck_id, scope_name, new_coll_name)
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.create_collection(
                org_id, proj_id, clus_id, buck_id, scope_name, new_coll_name)
        if res.status_code == 201:
            return new_coll_name
        self.fail("Collection creation unsuccessful.")

    def flush_scopes(self, org_id, proj_id, clus_id, buck_id, scopes):
        self.update_auth_with_api_token(self.org_owner_key['token'])

        scopes_deletion_failed = False
        for scope in scopes:
            res = self.capellaAPI.cluster_ops_apis.delete_scope(
                org_id, proj_id, clus_id, buck_id, scope)
            if res.status_code == 429:
                self.handle_rate_limit(int(res.headers["Retry-After"]))
                res = self.capellaAPI.cluster_ops_apis.delete_scope(
                    org_id, proj_id, clus_id, buck_id, scope)
            if res.status_code != 200:
                self.log.error("Error while deleting scope {}".format(scope))
                scopes_deletion_failed = True
            else:
                scopes.remove(scope)

        return scopes_deletion_failed

    def flush_collections(self, org_id, proj_id, clus_id, buck_id, scope,
                          collections):
        self.update_auth_with_api_token(self.org_owner_key['token'])

        collections_deletion_failed = False
        for collection in collections:
            res = self.capellaAPI.cluster_ops_apis.delete_collection(
                org_id, proj_id, clus_id, buck_id, scope, collection)
            if res.status_code == 429:
                self.handle_rate_limit(int(res.headers['Retry-After']))
                res = self.capellaAPI.cluster_ops_apis.delete_collection(
                    org_id, proj_id, clus_id, buck_id, scope, collection)
            if res.status_code != 200:
                self.log.error("Error while deleting collection {}"
                               .format(collection))
                collections_deletion_failed = True
            else:
                collections.remove(collection)

        return collections_deletion_failed
