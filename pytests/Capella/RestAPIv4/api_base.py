"""
Created on June 28, 2023

@author: umang.agrawal
"""
import copy
import time
import string
import random
import base64
import itertools
import threading
from datetime import datetime
from pytests.cb_basetest import CouchbaseBaseTest
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI
from capellaAPI.capella.columnar.ColumnarAPI_v4 import ColumnarAPIs
from couchbase_utils.capella_utils.dedicated import CapellaUtils
from TestInput import TestInputSingleton

class APIBase(CouchbaseBaseTest):

    def setUp(self, nomenclature="WRAPPER", services=[]):
        CouchbaseBaseTest.setUp(self)

        self.capella = self.input.capella
        self.url = self.input.capella.get("pod")
        self.user = self.input.capella.get("capella_user")
        self.passwd = self.input.capella.get("capella_pwd")
        self.organisation_id = self.input.capella.get("tenant_id")
        self.invalid_UUID = "00000000-0000-0000-0000-000000000000"
        self.prefix = "Automated_v4-API_test_"
        self.project_id = None
        self.count = 0

        self.capellaAPI = CapellaAPI(
            "https://" + self.url, "", "", self.user, self.passwd, "")
        self.columnarAPI = ColumnarAPIs("https://" + self.url, "", "", "")
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
            self.log.error("Error while creating API key for organization "
                           "owner")

        # update the token for capellaAPI object, so that is it being used
        # for api auth.
        self.update_auth_with_api_token(self.org_owner_key["token"])
        self.api_keys = dict()

        # Create a wrapper project to be used for all the projects :
        # IF not already present.
        if TestInputSingleton.input.capella.get("project", None):
            self.project_id = TestInputSingleton.input.capella.get("project")
        else:
            res = self.capellaAPI.org_ops_apis.create_project(
                self.organisation_id, self.prefix + "WRAPPER")
            if res.status_code != 201:
                self.log.error(res.content)
                self.tearDown()
                self.fail("!!!..Project creation failed...!!!")
            else:
                self.log.info("Project Creation Successful")
                self.project_id = res.json()["id"]
                self.capella["project"] = self.project_id

        # Templates for cluster configurations across computes and CSPs.
        self.cluster_templates = {
            # Fixed parameter initialization
            "name": self.prefix + "WRAPPER",
            "currentState": None,
            "audit": {
                "createdBy": None,
                "createdAt": None,
                "modifiedBy": None,
                "modifiedAt": None,
                "version": None
            },
            "connectionString": None,
            "configurationType": None,

            # TEMPLATES :
            "AWS_template_m7_xlarge": {
                "cloudProvider": {
                    "type": "aws",
                    "region": self.input.param("region", "us-east-1"),
                    "cidr": "10.0.0.0/20"
                },
                "couchbaseServer": {
                    "version": str(self.input.param("server_version", 7.6))
                },
                "serviceGroups": [
                    {
                        "node": {
                            "compute": {
                                "cpu": self.input.param("cpu", 4),
                                "ram": self.input.param("ram", 16)
                            },
                            "disk": {
                                "storage": 50,
                                "type": "gp3",
                                "iops": 3000
                            }
                        },
                        "numOfNodes": self.input.param("numOfNodes", 3),
                        "services": [
                            "data",
                            "index",
                            "query"
                        ]
                    }
                ],
                "availability": {
                    "type": self.input.param("availabilityType", "multi")
                },
                "support": {
                    "plan": self.input.param("supportPlan", "enterprise"),
                    "timezone": "GMT"
                }
            },
        }
        if TestInputSingleton.input.capella.get("clusters", None):
            self.cluster_id = TestInputSingleton.input.capella.get(
                "clusters")["cluster_id"]
        else:
            cluster_template = self.input.param("cluster_template",
                                                "AWS_template_m7_xlarge")
            self.cluster_templates[cluster_template]['serviceGroups'][0][
                "services"].extend(services)
            res = self.select_CIDR(
                self.organisation_id, self.project_id,
                self.cluster_templates["name"],
                self.cluster_templates[cluster_template]['cloudProvider'],
                self.cluster_templates[cluster_template]['serviceGroups'],
                self.cluster_templates[cluster_template]['availability'],
                self.cluster_templates[cluster_template]['support'],
                self.cluster_templates[cluster_template]['couchbaseServer'])
            try:
                if res.status_code != 202:
                    self.log.error("Failed while creating cluster")
                    self.tearDown()
                    self.fail("!!!...Cluster creation Failed...!!!")
                else:
                    self.cluster_id = res.json()["id"]
                    self.capella["clusters"] = {
                        "cluster_id": self.cluster_id,
                        "vpc_id": None,
                        "app_id": None
                    }
            except (Exception,):
                self.log.error(res.status_code)
                self.log.error(res.content)
                self.tearDown()
                self.fail("!!!...Couldn't decipher result...!!!")

        # Templates for instance creation per CSPs and computes
        self.instance_templates = {
            "name": self.prefix + "WRAPPER",
            "support": {
                "plan": "enterprise",
                "timezone": "ET"
            },

            # TEMPLATES :
            "4v16_AWS_singleNode_ue1": {
                "nodes": self.input.param("nodes", 1),
                "region": self.input.param("region", "us-east-1"),
                "cloudProvider": "aws",
                "compute": {
                    "cpu": self.input.param("cpu", 4),
                    "ram": self.input.param("ram", 16)
                },
                "availability": {
                    "type": self.input.param("availabilityType",
                                             "single")
                }
            }
        }
        if TestInputSingleton.input.capella.get("instance_id", None):
            self.analyticsCluster_id = TestInputSingleton.input.capella.get(
                "instance_id")
        else:
            instance_template = self.input.param("instance_template",
                                                 "4v16_AWS_singleNode_ue1")
            res = self.columnarAPI.create_analytics_cluster(
                self.organisation_id, self.project_id,
                self.instance_templates["name"],
                self.instance_templates[instance_template]["cloudProvider"],
                self.instance_templates[instance_template]["compute"],
                self.instance_templates[instance_template]["region"],
                self.instance_templates[instance_template]["nodes"],
                self.instance_templates["support"],
                self.instance_templates[instance_template]["availability"])
            if res.status_code != 202:
                self.log.error(res.content)
                self.tearDown()
                self.fail("!!!...Instance creation Failed...!!!")
            self.analyticsCluster_id = res.json()["id"]
            self.capella["instance_id"] = self.analyticsCluster_id
        self.instances = list()

    def tearDown(self):
        # Delete the WRAPPER resources, IF, the current test is the last
        # testcase being run.
        if (TestInputSingleton.input.test_params["case_number"] ==
                TestInputSingleton.input.test_params["no_of_test_identified"]):
            # Delete the created instance.
            if self.flush_columnar_instances(self.instances):
                super(APIBase, self).tearDown()
                self.fail("!!!...Instance(s) deletion failed...!!!")
            self.wait_for_deletion(instances=self.instances)

            # Delete the cluster that was created.
            self.log.info("Destroying Cluster: {}".format(self.cluster_id))
            if self.capellaAPI.cluster_ops_apis.delete_cluster(
                    self.organisation_id, self.project_id,
                    self.cluster_id).status_code != 202:
                self.log.error("Error while deleting cluster.")

            # Wait for the cluster to be destroyed.
            self.log.info("Waiting for cluster to be destroyed.")
            if not self.wait_for_deletion(self.cluster_id):
                self.fail("Cluster could not be destroyed")
            self.log.info("Cluster destroyed successfully.")
            self.cluster_id = None

            # Delete the project that was created.
            self.log.info("Deleting Project: {}".format(self.project_id))
            if self.delete_projects(self.organisation_id, [self.project_id],
                                    self.org_owner_key["token"]):
                self.log.error("Error while deleting project.")
            else:
                self.log.info("Project deleted successfully")
                self.project_id = None

        # Delete organizationOwner API key
        self.log.info("Deleting API key for role organization Owner")
        resp = self.capellaAPI.org_ops_apis.delete_api_key(
            organizationId=self.organisation_id,
            accessKey=self.org_owner_key["id"])
        if resp.status_code != 204:
            self.log.error("Error while deleting api key for role "
                           "organization Owner")

        if hasattr(self, "v2_control_plane_api_access_key"):
            response = self.capellaAPI.delete_control_plane_api_key(
                self.organisation_id, self.v2_control_plane_api_access_key)
            if response.status_code != 204:
                self.log.error("Error while deleting V2 control plane API key")
                self.log.error("{}".format(response.content))
        super(APIBase, self).tearDown()

    def create_v2_control_plane_api_key(self):
        # Generate the first set of API access and secret access keys
        # Currently v2 API is being used for this.
        response = self.capellaAPI.create_control_plane_api_key(
            self.organisation_id, "initial_api")
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

    @staticmethod
    def get_utc_datetime(minutes_delta=0):
        import datetime
        now = datetime.datetime.utcnow()

        if minutes_delta:
            delta = datetime.timedelta(minutes=minutes_delta)
            now = now + delta

        return now.strftime('%Y-%m-%dT%H:%M:%S') + "Z"

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
        self.columnarAPI.bearer_token = token

    """
    Method makes parallel api calls.
    param num_of_calls_per_api (int) Number of API calls per API to be made.
    param apis_to_call (list(list)) List of lists, where inner list is of
    format [api_function_call, function_args]
    param api_key_dict dict API keys to be used while making API calls
    """
    def make_parallel_api_calls(
            self, num_of_calls_per_api=100, apis_to_call=[],
            api_key_dict={}, batch_size=10, wait_time=0):
        results = dict()
        for role in api_key_dict:
            api_key_dict[role].update({"role": role})
        api_key_list = [api_key_dict[role] for role in api_key_dict]

        threads = list()

        def call_api_with_api_key(call_batch_per_api, api_role, api_func,
                                  api_args):
            header = {
                'Authorization': 'Bearer ' + api_role["token"],
                'Content-Type': 'application/json'
            }
            if api_role["id"] not in results:
                results[api_role["id"]] = {
                    "role": api_role["role"],
                    "rate_limit_hit": False,
                    "start": datetime.now(),
                    "end": None,
                    "total_api_calls_made_to_hit_rate_limit": 0,
                    "2xx_status_code": {},
                    "4xx_errors": {},
                    "5xx_errors": {}
                }
            for i in range(call_batch_per_api):
                resp = api_func(*api_args, headers=header)
                if resp.status_code == 429:
                    results[api_role["id"]]["rate_limit_hit"] = True
                if not results[api_role["id"]]["rate_limit_hit"]:
                    results[api_role["id"]][
                        "total_api_calls_made_to_hit_rate_limit"] += 1
                results[api_role["id"]]["end"] = datetime.now()
                if str(resp.status_code).startswith("2"):
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
            batches = num_of_calls_per_api / batch_size
            last_batch = num_of_calls_per_api % batch_size
            for batch in range(batches):
                threads.append(threading.Thread(
                    target=call_api_with_api_key,
                    name="thread_{0}_{1}".format(batch, i),
                    args=(batch_size, api_key_list[i % len(api_key_list)],
                          apis_to_call[i % len(apis_to_call)][0],
                          apis_to_call[i % len(apis_to_call)][1],)))
            if last_batch > 0:
                threads.append(threading.Thread(
                    target=call_api_with_api_key,
                    name="thread_for_last_batch_{}".format(i),
                    args=(last_batch, api_key_list[i % len(api_key_list)],
                          apis_to_call[i % len(apis_to_call)][0],
                          apis_to_call[i % len(apis_to_call)][1],)))

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

    def throttle_test(self, api_func_list, multi_key=False, proj_id=None):
        """
        api_func_list: (list(list)) List of lists, where inner list is of format :
            [api_function_call, function_args]
        multi_key: a boolean value which is used to get to know whether the  test is being run with a single key (which has proper access to the resource),  or multiple keys (which might or might not have access to the resource) being tested via throttling.
        proj_id: The UUID of the project that will be used in case of multiple keys throttle testing with different roles.
        """
        exclude_codes = self.input.param("exclude_codes", "")
        if exclude_codes:
            exclude_codes = str(exclude_codes)
            exclude_codes = exclude_codes.split('-')
        else:
            exclude_codes = []
        exclude_codes.append("403")

        if (not multi_key and proj_id) or (multi_key and not proj_id):
            self.fail("Please provide the project ID in the testcase while "
                      "calling the throttling function in case of multiple "
                      "keys scenario (which may or may not have access to the "
                      "resource")

        if multi_key:
            org_roles = self.input.param("org_roles", "organizationOwner")
            proj_roles = self.input.param("proj_roles", "projectDataReader")
            org_roles = org_roles.split(":")
            proj_roles = proj_roles.split(":")

            api_key_dict = self.create_api_keys_for_all_combinations_of_roles(
                [proj_id], proj_roles, org_roles)
            for i, api_key in enumerate(api_key_dict):
                if api_key in self.api_keys:
                    self.api_keys["{}_{}".format(api_key_dict[api_key], i)] = \
                        api_key_dict[api_key]
                else:
                    self.api_keys[api_key] = api_key_dict[api_key]
        else:
            self.log.info("Rate Limit test is using {} API keys"
                          .format(self.input.param("num_api_keys", 1)))
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
                    self.api_keys[
                        "organizationOwner_{}".format(i)] = resp.json()
                else:
                    self.fail("Error while creating API key for "
                              "organizationOwner_{}".format(i))

        if self.input.param("rate_limit", False):
            results = self.make_parallel_api_calls(
                150, api_func_list, self.api_keys)
            for r in results:
                self.log.info("**********************************************")
                self.log.info("Parallel API calls for role {} took {} seconds"
                              .format(results[r]["role"], (results[r]["end"]
                                      - results[r]["start"]).total_seconds()))
                self.log.info("**********************************************")
            for result in results:
                if ((not results[result]["rate_limit_hit"])
                        or results[result][
                            "total_api_calls_made_to_hit_rate_limit"] > 100):
                    self.fail(
                        "Rate limit was hit after {0} API calls. "
                        "This is definitely an issue.".format(
                            results[result][
                                "total_api_calls_made_to_hit_rate_limit"]
                        ))
            self.log.info("Sleeping 1 min to let previous rate limit expire")
            time.sleep(60)

        results = self.make_parallel_api_calls(
            99, api_func_list, self.api_keys)
        for r in results:
            self.log.info("**********************************************")
            self.log.info("Parallel API calls for role {} took {} seconds"
                          .format(results[r]["role"], (results[r]["end"]
                                  - results[r]["start"]).total_seconds()))
            self.log.info("**********************************************")

        for r in results:
            if "429" in results[r]["4xx_errors"]:
                del results[r]["4xx_errors"]["429"]
            for status_code in exclude_codes:
                if status_code in results[r]["4xx_errors"]:
                    del results[r]["4xx_errors"][status_code]

            if (len(results[r]["4xx_errors"]) > 0 or
                    len(results[r]["5xx_errors"]) > 0):
                self.fail("Some API calls failed")

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
    def auth_test_extension(testcases, other_project_id,
                            failure_expected_code=None,
                            failure_expected_error=None):
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
            }
        ])
        if other_project_id:
            testcases.extend([
                {
                    "description": "Calling API with user having access to "
                                   "get multiple projects ",
                    "has_multi_project_access": True,
                }, {
                    "description": "Calling API with user not having access "
                                   "to get project specific but has access to "
                                   "get other project",
                    "has_multi_project_access": False,
                    "expected_status_code": 403,
                    "expected_error": {
                        "code": 1002,
                        "hint": "Your access to the requested resource is "
                                "denied. Please make sure you have the "
                                "necessary permissions to access the "
                                "resource.",
                        "httpStatusCode": 403,
                        "message": "Access Denied."
                    }
                }
            ])
        if failure_expected_code:
            testcases[-2]["expected_status_code"] = failure_expected_code
            testcases[-2]["expected_error"] = failure_expected_error

    def auth_test_setup(self, testcase, failures, header,
                        project_id, other_project_id=None):
        if "expire_key" in testcase:
            self.update_auth_with_api_token(self.org_owner_key["token"])
            # create a new API key with expiry of approx 2 mins
            resp = self.capellaAPI.org_ops_apis.create_api_key(
                self.organisation_id, "Expiry_Key", ["organizationOwner"],
                expiry=0.0001)
            if resp.status_code == 201:
                self.api_keys["organizationOwner_new"] = resp.json()
            else:
                self.fail("Error while creating API key for organization "
                          "owner with expiry of 0.001 days")
            # wait for key to expire
            self.log.debug("Sleeping 10 seconds for key to expire")
            time.sleep(10)
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
        elif "has_multi_project_access" in testcase and other_project_id:
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

    def validate_testcase(self, result, success_codes, testcase, failures,
                          validate_response=False, expected_res=None,
                          resource_id=None, payloadTest=False):
        # Parser for payload tests.
        testDescriptionKey = "description"
        if payloadTest:
            testDescriptionKey = "desc"

        # Condition is for Sample Buckets delete testcases.
        if ("content" in result and "code" in result.content and
                result.json()["code"] == 600 and 6008 in success_codes):
            return True

        # Acceptor for expected error codes.
        if ("expected_status_code" in testcase and
                testcase["expected_status_code"] == result.status_code):
            self.log.debug("This test expected the code: {}, with error: {}"
                           .format(testcase["expected_status_code"],
                                   testcase["expected_error"]))
            return True

        if result.status_code in success_codes:
            if ("expected_error" in testcase and
                    testcase["expected_status_code"] != 404):
                self.log.error("NO ERRORS in Response, But Test expected "
                               "error: {}".format(testcase["expected_error"]))
                try:
                    self.log.warning("Result : {}".format(result.json()))
                except (Exception,):
                    self.log.warning("Result : {}".format(result.content))
                failures.append(testcase[testDescriptionKey])
            if validate_response:
                if not self.validate_api_response(
                        expected_res, result.json(), resource_id):
                    self.log.error("Status == {}, Key validation Failure : {}"
                                   .format(result.status_code,
                                           testcase[testDescriptionKey]))
                    self.log.warning("Result : {}".format(result.content))
                    failures.append(testcase[testDescriptionKey])
                else:
                    return True
            else:
                return True
        elif result.status_code >= 500:
            self.log.critical(testcase[testDescriptionKey])
            self.log.warning(result.content)
            failures.append(testcase[testDescriptionKey])
        elif "expected_status_code" not in testcase:
            self.log.error("Expected NO ERRORS but got {}".format(result))
            self.log.error(result.content)
            failures.append(testcase[testDescriptionKey])
        elif result.status_code == testcase["expected_status_code"]:
            try:
                result = result.json()
                for key in result:
                    if result[key] != testcase["expected_error"][key]:
                        self.log.error("Status != {}, Error validation Failure"
                                       " : {}".format(
                                        success_codes,
                                        testcase[testDescriptionKey]))
                        self.log.warning("Failure : {}".format(result))
                        failures.append(testcase[testDescriptionKey])
                        break
            except (Exception,):
                if str(testcase["expected_error"]) not in result.content:
                    self.log.error("Response type not JSON, Failure : {}"
                                   .format(testcase[testDescriptionKey]))
                    self.log.warning(result.content)
                    failures.append(testcase[testDescriptionKey])
        else:
            self.log.error("Expected HTTP status code {}, Actual HTTP status "
                           "code {}".format(testcase["expected_status_code"],
                                            result.status_code))
            self.log.warning("Result : {}".format(result.content))
            failures.append(testcase[testDescriptionKey])
        return False

    def validate_onoff_state(self, states, inst=None, app=None, sleep=2):
        if sleep:
            time.sleep(sleep)
        if app:
            res = self.capellaAPI.cluster_ops_apis.get_appservice(
                self.organisation_id, self.project_id, self.cluster_id, app)
            if res.status_code == 429:
                self.handle_rate_limit(int(res.headers['Retry-After']))
                res = self.capellaAPI.cluster_ops_apis.get_appservice(
                    self.organisation_id, self.project_id, self.cluster_id, app)
        elif inst:
            res = self.columnarAPI.fetch_analytics_cluster_info(
                self.organisation_id, self.project_id, inst)
            if res.status_code == 429:
                self.handle_rate_limit(int(res.headers['Retry-After']))
                res = self.columnarAPI.fetch_analytics_cluster_info(
                    self.organisation_id, self.project_id, inst)
        else:
            res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                self.organisation_id, self.project_id, self.cluster_id)
            if res.status_code == 429:
                self.handle_rate_limit(int(res.headers['Retry-After']))
                res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                    self.organisation_id, self.project_id, self.cluster_id)

        if res.status_code != 200:
            self.log.error("Could not fetch on/off state info : {}"
                           .format(res.content))

        if res.json()['currentState'] in states:
            return True

        self.log.warning("Current State: '{}', Expected States: '{}'"
                         .format(res.json()["currentState"], states))
        return False

    def validate_api_response(self, expected_res, actual_res, id):
        for key in actual_res:
            if key == "version" or not expected_res[key]:
                continue
            if key not in expected_res:
                return False
            if isinstance(expected_res[key], dict):
                if not self.validate_api_response(
                        expected_res[key], actual_res[key], id):
                    return False
            elif isinstance(expected_res[key], list):
                j = 0
                if key == "services":
                    for service in expected_res[key]:
                        if service not in actual_res[key]:
                            return False
                    continue
                for i in range(len(actual_res[key])):
                    if key == "data":
                        if "id" in actual_res[key][i] and \
                                actual_res[key][i]["id"] != id:
                            continue
                        elif "name" in actual_res[key][i] and \
                                actual_res[key][i]["name"] != id:
                            continue
                    if len(expected_res[key]) > 1:
                        j = i
                    if not self.validate_api_response(
                            expected_res[key][j], actual_res[key][i], id):
                        return False
            elif expected_res[key] != actual_res[key]:
                return False
        return True

    def select_CIDR(self, org, proj, name, cloudProvider, serviceGroups,
                    availability, support, couchbaseServer=None, header=None,
                    **kwargs):
        self.log.info("Selecting CIDR for cluster deployment.")

        start_time = time.time()
        while time.time() - start_time < 1800:
            result = self.capellaAPI.cluster_ops_apis.create_cluster(
                org, proj, name, cloudProvider, couchbaseServer,
                serviceGroups, availability, support, header, **kwargs)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.create_cluster(
                    org, proj, name, cloudProvider, couchbaseServer,
                    serviceGroups, availability, support, header, **kwargs)
            if result.status_code == 202:
                return result
            if ("Please ensure that the CIDR range is unique within this "
                    "organisation") in result.json()["message"]:
                cloudProvider["cidr"] = CapellaUtils.get_next_cidr() + "/20"
                self.log.info("Trying CIDR: {}".format(cloudProvider["cidr"]))
            if time.time() - start_time >= 1800:
                self.log.error("Couldn't find CIDR within half an hour.")

    def wait_for_deployment(self, clus_id=None, app_svc_id=None,
                            inst_id=None, pes=False):
        start_time = time.time()
        while start_time + 1800 > time.time():
            self.log.info("...Waiting further...")
            time.sleep(5)

            if app_svc_id:
                state = self.capellaAPI.cluster_ops_apis.get_appservice(
                    self.organisation_id, self.project_id, clus_id, app_svc_id)
                if state.status_code == 429:
                    self.handle_rate_limit(int(state.headers['Retry-After']))
                    state = self.capellaAPI.cluster_ops_apis.get_appservice(
                        self.organisation_id, self.project_id, clus_id,
                        app_svc_id)
            elif inst_id:
                state = self.columnarAPI.fetch_analytics_cluster_info(
                    self.organisation_id, self.project_id, inst_id)
                if state.status_code == 429:
                    self.handle_rate_limit(int(state.headers["Retry-After"]))
                    state = self.columnarAPI.fetch_analytics_cluster_info(
                        self.organisation_id, self.project_id, inst_id)
            elif pes:
                state = self.capellaAPI.cluster_ops_apis \
                    .fetch_private_endpoint_service_status_info(
                        self.organisation_id, self.project_id, self.cluster_id)
                if state.status_code == 429:
                    self.handle_rate_limit(int(state.headers["Retry-After"]))
                    state = self.capellaAPI.cluster_ops_apis \
                        .fetch_private_endpoint_service_status_info(
                            self.organisation_id, self.project_id,
                            self.cluster_id)
            elif clus_id:
                state = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                    self.organisation_id, self.project_id, clus_id)
                if state.status_code == 429:
                    self.handle_rate_limit(int(state.headers['Retry-After']))
                    state = self.capellaAPI.cluster_ops_apis.\
                        fetch_cluster_info(
                            self.organisation_id, self.project_id, clus_id)
            else:
                state = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                    self.organisation_id, self.project_id, self.cluster_id)
                if state.status_code == 429:
                    self.handle_rate_limit(int(state.headers['Retry-After']))
                    state = self.capellaAPI.cluster_ops_apis.\
                        fetch_cluster_info(self.organisation_id,
                                           self.project_id, self.cluster_id)

            if state.status_code >= 400:
                self.log.error("Something went wrong while fetching details."
                               "\nResult: {}".format(state.content))
                return False
            if pes:
                if state.json()["enabled"]:
                    return True
                continue

            self.log.info("Current state: {}"
                          .format(state.json()["currentState"]))

            if state.json()["currentState"] == "deploymentFailed":
                self.log.error("!!!Deployment Failed!!!")
                self.log.error(state.content)
                return False
            if app_svc_id and state.json()["currentState"] == "turnedOff":
                self.log.warning("App Service is turned off")
                return True
            if inst_id and state.json()["currentState"] in [
                    "turningOff", "turnedOff"]:
                self.log.warning("Instance is turning off")
                return True
            elif state.json()["currentState"] == "healthy":
                return True
        self.log.error("Resource didn't deploy within half an hour.")
        return False

    def wait_for_deletion(self, clus_id=None, app_svc_id=None, instances=None):
        start_time = time.time()
        while start_time + 1800 > time.time():
            time.sleep(15)

            if instances:
                temp_instances = copy.deepcopy(instances)
                for instance in instances:
                    while self.columnarAPI.fetch_analytics_cluster_info(
                            self.organisation_id, self.project_id,
                            instance).status_code != 404:
                        self.log.info("...Waiting further...")
                        time.sleep(2)
                    self.log.info("Instance {} deleted".format(instance))
                    temp_instances.remove(instance)
                if len(temp_instances) == 0:
                    self.log.info("All instances deleted successfully.")
                    del instances[:]
                    return
                self.log.error("!!!...All instances did not delete...!!!")
                return
            if app_svc_id:
                res = self.capellaAPI.cluster_ops_apis.get_appservice(
                    self.organisation_id, self.project_id, clus_id, app_svc_id)
                if res.status_code == 429:
                    self.handle_rate_limit(int(res.headers["Retry-After"]))
                    res = self.capellaAPI.cluster_ops_apis.get_appservice(
                        self.organisation_id, self.project_id, clus_id,
                        app_svc_id)
            else:
                res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                    self.organisation_id, self.project_id, clus_id)
                if res.status_code == 429:
                    self.handle_rate_limit(int(res.headers["Retry-After"]))
                    res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                        self.organisation_id, self.project_id, clus_id)

            if res.status_code == 404:
                try:
                    res = res.json()
                    if "code" in res:
                        if app_svc_id and res["code"] == 404:
                            return True
                        elif not app_svc_id and res["code"] == 4025:
                            return True
                        else:
                            self.log.error("Error while retrieving resource "
                                           "information: {}"
                                           .format(res.content))
                            return False
                except (Exception, ):
                    self.log.error("Error while retrieving resource "
                                   "information: {}".format(res.content))
                    return False
            elif res.status_code == 200:
                self.log.info("...Waiting further...")
            else:
                self.log.error("Error while retrieving resource "
                               "information: {}".format(res.content))
                return False
        self.log.error("Resource didn't delete within half an hour.")
        return False

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
                self.log.error("Error while deleting project {}\nError:"
                               .format(project_id, resp.content))
                project_deletion_failed = project_deletion_failed or True
        return project_deletion_failed

    def create_alert_to_be_tested(self, proj_id, kind, name, config):
        self.update_auth_with_api_token(self.org_owner_key['token'])
        res = self.capellaAPI.cluster_ops_apis.create_alert(
            self.organisation_id, proj_id, kind, name, config)
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.create_alert(
                self.organisation_id, proj_id, kind, name, config)
        if res.status_code == 201:
            alert_id = res.json()['id']
            self.log.info("New alert ID: {}".format(alert_id))
            return alert_id
        self.log.error(res.content)
        self.fail("!!!...New alert creation failed...!!!")

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
            self.log.debug("New bucket ID: {}".format(buck_id))
            return buck_id
        self.log.error(resp.content)
        self.fail("!!!...New bucket creation failed...!!!")

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

        name = self.prefix + "Scope_Delete"
        res = self.capellaAPI.cluster_ops_apis.create_scope(
            org_id, proj_id, clus_id, buck_id, name)
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.create_scope(
                org_id, proj_id, clus_id, buck_id, name)
        if res.status_code == 201:
            self.log.debug("New scope Name: {}".format(name))
            return name
        self.log.error(res.content)
        self.fail("!!!...Scope creation unsuccessful...!!!")

    def create_collection_to_be_tested(self, org_id, proj_id, clus_id,
                                       buck_id, scope_name):
        self.update_auth_with_api_token(self.org_owner_key["token"])

        name = self.prefix + "Collections_Delete"
        res = self.capellaAPI.cluster_ops_apis.create_collection(
            org_id, proj_id, clus_id, buck_id, scope_name, name)
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.create_collection(
                org_id, proj_id, clus_id, buck_id, scope_name, name)
        if res.status_code == 201:
            self.log.debug("New collection Name: {}".format(name))
            return name
        self.log.error(res.content)
        self.fail("!!!...Collection creation unsuccessful...!!!")

    def flush_alerts(self, project_id, alerts):
        self.update_auth_with_api_token(self.org_owner_key['token'])

        alert_deletion_failed = False
        for alert in alerts:
            res = self.capellaAPI.cluster_ops_apis.delete_alert(
                self.organisation_id, project_id, alert)
            if res.status_code == 429:
                self.handle_rate_limit(int(res.headers["Retry-After"]))
                res = self.capellaAPI.cluster_ops_apis.delete_alert(
                    self.organisation_id, project_id, alert)
            if res.status_code != 204:
                self.log.warning("Error while deleting alert {}".format(alert))
                self.log.error("Response: {}".format(res.content))
                alert_deletion_failed = True
            else:
                alerts.remove(alert)

        return alert_deletion_failed

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
            if res.status_code != 204:
                self.log.warning("Error while deleting scope {}".format(scope))
                self.log.error("Response: {}".format(res.content))
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
            if res.status_code != 204:
                self.log.warning("Error while deleting collection {}"
                                 .format(collection))
                self.log.error("Response: {}".format(res.content))
                collections_deletion_failed = True
            else:
                collections.remove(collection)

        return collections_deletion_failed

    def create_columnar_instance_to_be_tested(self):
        self.update_auth_with_api_token(self.org_owner_key["token"])

        name = self.prefix + "ColumnarDelete_New"
        res = self.columnarAPI.create_analytics_cluster(
            self.organisation_id, self.project_id, name, "aws",
            {"cpu": 4, "ram": 16}, "us-east-1", 1,
            {"plan": "enterprise", "timezone": "ET"}, {"type": "single"})
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers['Retry-After']))
            res = self.columnarAPI.create_analytics_cluster(
                self.organisation_id, self.project_id, name, "aws",
                {"cpu": 4, "ram": 16}, "us-east-1", 1,
                {"plan": "enterprise", "timezone": "ET"}, {"type": "single"})
        if res.status_code == 202:
            self.log.debug("New Instance ID: {}".format(res.json()["id"]))
            self.capella["instance_id"] = res.json()["id"]
            self.instances.append(res.json()["id"])
            return res.json()["id"]
        self.log.error(res.content)
        self.fail("!!!...Instance Creation unsuccessful...!!!")

    def flush_columnar_instances(self, instances):
        self.update_auth_with_api_token(self.org_owner_key['token'])

        instance_deletion_failed = False
        for instance in instances:
            self.log.info("Deleting instance {}".format(instance))
            res = self.columnarAPI.delete_analytics_cluster(
                self.organisation_id, self.project_id, instance)
            if res.status_code == 429:
                self.handle_rate_limit(int(res.headers['Retry-After']))
                res = self.columnarAPI.delete_analytics_cluster(
                    self.organisation_id, self.project_id, instance)
            if res.status_code != 202:
                self.log.error("Error while deleting instance: {}\nError: {}"
                               .format(instance, res.content))
                instance_deletion_failed = True
            else:
                self.log.info("Instance delete request successful.")

        return instance_deletion_failed
