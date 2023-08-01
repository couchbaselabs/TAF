'''
Created on June 28, 2023

@author: umang.agrawal
'''

import time
import string
import random
import itertools
from datetime import datetime
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI
from pytests.basetestcase import BaseTestCase
import concurrent.futures


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
            self.url, "", "", self.user, self.passwd, "")
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
            self.v2_control_plane_api_access_key = response["accessKey"]
            self.update_auth_with_api_token(response["token"])
        else:
            self.log.error("Error while creating V2 control plane API key")
            self.fail("{}".format(response.content))

    def generate_random_string(self, length=10, special_characters=True,
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

    def rate_limit_failsafe(self):
        self.count += 1
        if self.count % 99:
            return
        self.log.debug("Sleeping 60 seconds to avoid hitting rate limit")
        time.sleep(60)

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
                organization_roles+project_roles, r)
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
            self.rate_limit_failsafe()

            if resp.status_code == 201:
                api_key_dict["-".join(role_combination)] = {
                    "Id": resp.json()["Id"],
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
                    accessKey=api_key_dict[role]["Id"]
                )
                self.rate_limit_failsafe()
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
    Method make parallel api calls.
    param num_of_parallel_calls (int) Number of parallel API calls to be made.
    param apis_to_call (list(list)) List of lists, where inner list is of
    format [api_function_call, function_args]
    """
    def make_parallel_api_calls(self, num_of_parallel_calls=100,
                                apis_to_call=[], api_key_dict={}, wait_time=0):
        results = list()
        for role in api_key_dict:
            api_key_dict[role].update({"role": role})
        api_key_list = [api_key_dict[role] for role in api_key_dict]
        with concurrent.futures.ThreadPoolExecutor() as executor:
            api_call_futures = list()
            num_api_calls = len(apis_to_call)

            def call_api_with_api_key(api_role, api_func, api_args):
                self.update_auth_with_api_token(api_role["token"])
                resp = api_func(*api_args)
                resp.api_role = api_role["role"]
                return resp

            # Submit API call tasks to the executor
            for i in range(num_of_parallel_calls):

                api_call_futures.append(executor.submit(
                    call_api_with_api_key,
                    api_key_list[i % len(api_key_list)],
                    apis_to_call[i % num_api_calls][0],
                    apis_to_call[i % num_api_calls][1]
                ))
                if wait_time:
                    time.sleep(wait_time)

            # Retrieve the results as they become available
            for future in concurrent.futures.as_completed(api_call_futures):
                results.append(future.result())

        failed_api_calls = list()
        successfull_api_calls = list()

        for result in results:
            if result.status_code is not 429:
                successfull_api_calls.append(result)
                if result.status_code == 403:
                    self.log.debug("Access denied for {}"
                                   .format(result.api_role))
                if result.status_code == 401:
                    self.log.debug("Authorization failed error {} for {}"
                                   .format(result.content, result.api_role))
            else:
                failed_api_calls.append(result)

        self.log.info("Total API calls - {}".format(len(results)))
        self.log.info("Total Successfull API calls - {}".format(len(
            successfull_api_calls)))
        self.log.info("Total Failed API calls - {}".format(len(
            failed_api_calls)))

        return results, successfull_api_calls, failed_api_calls

    def validate_api_response(self, expected_resp, actual_resp):
        for key in expected_resp:
            if key not in actual_resp:
                return False
            else:
                if key in ["createdAt", "modifiedAt"]:
                    if expected_resp[key] not in actual_resp[key]:
                        return False
                elif isinstance(expected_resp[key], dict):
                    self.validate_api_response(expected_resp[key],
                                               actual_resp[key])
                elif expected_resp[key] != actual_resp[key]:
                    return False
        return True

    def validate_list_api_response(self, expected_resp, actual_resp):
        match_counter = 0
        for data in actual_resp:
            if data["name"] in expected_resp:
                if self.validate_api_response(
                        expected_resp[data["name"]]["expected_result"], data):
                    match_counter += 1
                else:
                    self.log.error(
                        "Expected Response - {}. Actual Response - {}".format(
                            expected_resp[data["name"]]["expected_result"],
                            data))
        if len(expected_resp) == match_counter:
            return True
        else:
            return False

    def replace_last_character(self, id):
        last_char = id[-1]
        if last_char.isdigit():
            if int(last_char) == 9:
                next_char = str(int(last_char) - 1)
            else:
                next_char = str(int(last_char) + 1)
        elif last_char.isalpha():
            if last_char.lower() == 'z':
                next_char = 'a' if last_char.islower() else 'A'
            else:
                next_char = chr(ord(last_char) + 1)
        else:
            # If the last character is a special character
            next_char = chr(ord(last_char) + 1)
        replaced_id = id[:-1] + next_char
        return replaced_id

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
                organizationId=org_id,
                name=project_name,
                description=projects[project_name]["description"])
            if resp.status_code == 201:
                projects[project_name]["id"] = resp.json()["id"]
            else:
                self.fail(
                    "Error while creating project {}".format(project_name))

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
                organizationId=org_id,
                projectId=project_id
            )
            if resp.status_code != 204:
                self.log.error("Error while deleting project {}".format(
                    project_id))
                project_deletion_failed = project_deletion_failed or True
        return project_deletion_failed
