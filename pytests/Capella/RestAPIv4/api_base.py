"""
Created on June 28, 2023

@author: umang.agrawal
"""

from cgi import test
import re
import copy
import time
import string
import random
import base64
import itertools
import threading
from datetime import datetime
from logging import exception

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
        self.prefix = "Automated_v4API_test_"
        self.project_id = self.input.capella.get("project", None)
        self.free_tier_cluster_id = None
        self.count = 0
        self.capellaAPI = CapellaAPI(
            "https://" + self.url, "", "", self.user, self.passwd, "")
        self.columnarAPI = ColumnarAPIs("https://" + self.url, "", "", "")
        if isinstance(self.organisation_id, dict):
            self.organisation_id = self.organisation_id["id"]
            self.v2_control_plane_api_access_key = self.capella[
                "tenant_id"]["v2key"]
            self.org_owner_key1 = self.capella["tenant_id"]["key1"]
            self.org_owner_key2 = self.capella["tenant_id"]["key2"]
            self.curr_owner_key = self.org_owner_key1
            self.other_project_id = self.capella["tenant_id"]["otherProj"]
            self.api_keys = self.capella["tenant_id"]["apiKeys"]
            self.update_auth_with_api_token(self.curr_owner_key)
        else:
            self.capella["tenant_id"] = {
                "id": self.organisation_id,
                "otherProj": None
            }
            self.create_v2_control_plane_api_key()

            # create the first V4 API KEY WITH organizationOwner role, which
            # will be used to perform further V4 api operations
            resp = self.capellaAPI.org_ops_apis.create_api_key(
                self.organisation_id, self.prefix + "OrgOwnerKey-1",
                ["organizationOwner"], self.prefix, 1)
            if resp.status_code == 201:
                self.org_owner_key1 = resp.json()
                self.org_owner_key1["blockTime"] = time.time()
                self.org_owner_key1["retryAfter"] = 0
                self.capella["tenant_id"]["key1"] = self.org_owner_key1
            else:
                self.fail("!!!...Error while creating first OrgOwner v4 "
                          "API key...!!!")

            # Create another API key with Org Owner Privileges to tap out for
            # one another at time of Rate Limiting.
            resp = self.capellaAPI.org_ops_apis.create_api_key(
                self.organisation_id, self.prefix + "OrgOwnerKey-2",
                ["organizationOwner"], self.prefix, 1)
            if resp.status_code == 201:
                self.org_owner_key2 = resp.json()
                self.org_owner_key2["blockTime"] = time.time()
                self.org_owner_key2["retryAfter"] = 0
                self.capella["tenant_id"]["key2"] = self.org_owner_key2
            else:
                self.fail("!!!...Error while creating second OrgOwner v4 "
                          "API key...!!!")

            # update the token for capellaAPI object, so that is it being used
            # for api auth.
            self.curr_owner_key = self.org_owner_key1
            self.update_auth_with_api_token(self.curr_owner_key)

            # Create a wrapper project to be used for all the projects,
            # IF not already present.
            if TestInputSingleton.input.capella.get("project", None):
                self.project_id = TestInputSingleton.input.capella.get(
                    "project")
            else:
                self.log.info("Creating the functional-test required project")
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

            self.api_keys = dict()
            if self.input.param("GROUP", "functional") == "security":
                # Create a residual project used for auth verification tests.
                self.log.info("Creating the security-test required project")
                resp = self.capellaAPI.org_ops_apis.create_project(
                    self.organisation_id, "Auth_Project")
                if resp.status_code == 201:
                    self.other_project_id = resp.json()["id"]
                    self.capella["tenant_id"][
                        "otherProj"] = self.other_project_id
                else:
                    self.fail("Error while creating project: {}"
                              .format(resp.content))

                if "apiKeys" not in self.capella["tenant_id"] or not \
                        self.capella["tenant_id"]["apiKeys"]:
                    # Create security related API keys, 255 to be exact.
                    self.api_keys.update(
                        self.create_api_keys_for_all_combinations_of_roles(
                            [self.project_id]))
            self.capella["tenant_id"]["apiKeys"] = self.api_keys

        # Templates for cluster configurations across Computes and CSPs.
        self.cluster_templates = {
            # TEMPLATES :
            "AWS_r5_xlarge": {
                "cloudProvider": {
                    "type": "aws",
                    "region": self.input.param("region", "us-east-1"),
                    "cidr": "10.1.0.0/20"
                },
                "couchbaseServer": {
                    "version": str(self.input.param("server_version", 7.6))
                },
                "serviceGroups": [
                    {
                        "node": {
                            "compute": {
                                "cpu": self.input.param("cpu", 4),
                                "ram": self.input.param("ram", 32)
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
            "AWS_singleNode": {
                "cloudProvider": {
                    "type": "aws",
                    "region": self.input.param("region", "us-east-1"),
                    "cidr": "10.1.0.0/20"
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
                        "numOfNodes": self.input.param("numOfNodes", 1),
                        "services": [
                            "data",
                            "index",
                            "query"
                        ]
                    }
                ],
                "availability": {
                    "type": self.input.param("availabilityType", "single")
                },
                "support": {
                    "plan": self.input.param("supportPlan", "basic"),
                    "timezone": "GMT"
                }
            },
            "Azure_E4s_v5": {
                "cloudProvider": {
                    "type": "azure",
                    "region": self.input.param("region", "eastus"),
                    "cidr": "10.1.0.0/20"
                },
                "couchbaseServer": {
                    "version": str(self.input.param("server_version", 7.6))
                },
                "serviceGroups": [
                    {
                        "node": {
                            "compute": {
                                "cpu": self.input.param("cpu", 4),
                                "ram": self.input.param("ram", 32)
                            },
                            "disk": {
                                "autoExpansion": True,
                                "type": "P6",
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
            "Azure_singleNode": {
                "cloudProvider": {
                    "type": "azure",
                    "region": self.input.param("region", "eastus"),
                    "cidr": "10.1.0.0/20"
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
                                "autoExpansion": True,
                                "type": "P15",
                            }
                        },
                        "numOfNodes": self.input.param("numOfNodes", 1),
                        "services": [
                            "data",
                            "index",
                            "query"
                        ]
                    }
                ],
                "availability": {
                    "type": self.input.param("availabilityType", "single")
                },
                "support": {
                    "plan": self.input.param("supportPlan", "basic"),
                    "timezone": "GMT"
                }
            },
            "GCP_n2_highmem_4": {
                "cloudProvider": {
                    "type": "gcp",
                    "region": self.input.param("region", "us-east1"),
                    "cidr": "10.1.0.0/20"
                },
                "couchbaseServer": {
                    "version": str(self.input.param("server_version", 7.6))
                },
                "serviceGroups": [
                    {
                        "node": {
                            "compute": {
                                "cpu": self.input.param("cpu", 4),
                                "ram": self.input.param("ram", 32)
                            },
                            "disk": {
                                "storage": 50,
                                "type": "pd-ssd",
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
            "GCP_singleNode": {
                "cloudProvider": {
                    "type": "gcp",
                    "region": self.input.param("region", "us-east1"),
                    "cidr": "10.1.0.0/20"
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
                                "storage": 100,
                                "type": "pd-ssd",
                            }
                        },
                        "numOfNodes": self.input.param("numOfNodes", 1),
                        "services": [
                            "data",
                            "index",
                            "query"
                        ]
                    }
                ],
                "availability": {
                    "type": self.input.param("availabilityType", "single")
                },
                "support": {
                    "plan": self.input.param("supportPlan", "basic"),
                    "timezone": "GMT"
                }
            },
            "AWS_Free_Tier": {
                "cloudProvider": {
                    "cidr": "10.1.0.0/20",
                    "region": self.input.param("region", "us-east-1"),
                    "type": "aws"
                },
                "description": "Cluster for AWS free tier v4 API testing."
            }
        }
        cluster_template = self.input.param("cluster_template",
                                            "AWS_r5_xlarge")
        if TestInputSingleton.input.capella.get("clusters", None):
            self.cluster_id = TestInputSingleton.input.capella.get("clusters")
            if isinstance(self.cluster_id, dict):
                self.capella["clusters"] = self.cluster_id
                self.cluster_id = self.cluster_id["cluster_id"]
            else:
                self.capella["clusters"] = {
                    "cluster_id": self.cluster_id,
                    "cidr": None,
                    "vpc_id": None,
                    "app_id": None
                }
        else:
            self.capella["clusters"] = {
                "cluster_id": None,
                "cidr": None,
                "vpc_id": None,
                "app_id": None
            }
            self.cluster_templates[cluster_template]['serviceGroups'][0][
                "services"].extend(services)
            res = self.select_CIDR(
                self.organisation_id, self.project_id,
                self.prefix + cluster_template,
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
            except (Exception,):
                self.log.error(res.status_code)
                self.log.error(res.content)
                self.tearDown()
                self.fail("!!!...Couldn't decipher result...!!!")
            self.capella["clusters"]["cluster_id"] = self.cluster_id
        self.cluster_templates[cluster_template]["cloudProvider"][
            "cidr"] = self.capella["clusters"]["cidr"]

        # Templates for instance configurations across CSPs and Computes
        self.instance_templates = {
            # TEMPLATES :
            "AWS_singleNode": {
                "nodes": self.input.param("nodes", 1),
                "region": self.input.param("region", "us-east-1"),
                "cloudProvider": "aws",
                "compute": {
                    "cpu": self.input.param("cpu", 4),
                    "ram": self.input.param("ram", 32)
                },  # https://jira.issues.couchbase.com/browse/AV-92705
                "support": {
                    "plan": "developer pro",
                    "timezone": "IST"
                },
                "availability": {
                    "type": "single"
                }
            },
            "AWS_4v16_4node": {
                "nodes": self.input.param("nodes", 4),
                "region": self.input.param("region", "us-east-1"),
                "cloudProvider": "aws",
                "compute": {
                    "cpu": self.input.param("cpu", 4),
                    "ram": self.input.param("ram", 32)
                },  # https://jira.issues.couchbase.com/browse/AV-92705
                "support": {
                    "plan": "enterprise",
                    "timezone": "IST"
                },
                "availability": {
                    "type": "multi"
                }
            }
        }
        # Error for future use, to handle CIDR clash for columnar instances :
        # "Unable to process request. The CIDR provided"
        if TestInputSingleton.input.capella.get("instance_id", None):
            self.analyticsCluster_id = TestInputSingleton.input.capella.get(
                "instance_id")
        else:
            instance_template = self.input.param("instance_template",
                                                 "AWS_singleNode")
            res = self.columnarAPI.create_analytics_cluster(
                self.organisation_id, self.project_id,
                self.prefix + instance_template,
                self.instance_templates[instance_template]["cloudProvider"],
                self.instance_templates[instance_template]["compute"],
                self.instance_templates[instance_template]["region"],
                self.instance_templates[instance_template]["nodes"],
                self.instance_templates[instance_template]["support"],
                self.instance_templates[instance_template]["availability"])
            if res.status_code != 202:
                self.log.error(res.content)
                self.tearDown()
                self.fail("!!!...Instance creation Failed...!!!")
            self.analyticsCluster_id = res.json()["id"]
            self.capella["instance_id"] = self.analyticsCluster_id
        self.instances = {self.analyticsCluster_id}

        # Holder for buckets which would be created in the future.
        self.buckets = list()

        # Templates for app service configurations across CSPs and Computes.
        self.app_svc_templates = {
            # TEMPLATES :
            "2v4_2node": {
                "nodes": 2,
                "compute": {
                    "cpu": 2,
                    "ram": 4
                },
            }
        }

    def tearDown(self):
        # Delete the WRAPPER resources, IF, the current test is the last
        # testcase being run.
        if (TestInputSingleton.input.test_params["case_number"] ==
                TestInputSingleton.input.test_params["no_of_test_identified"]):
            # Wait of cluster to be in a stable state
            self.log.info("Polling CLUSTER: {}".format(
                self.capella["clusters"]["cluster_id"]))
            start_time = time.time()
            res,_ = self.validate_onoff_state(["healthy"])
            while not res:
                if time.time() > 1800 + start_time:
                    self.fail("!!!...Cluster didn't stabilize within half an "
                              "hour...!!!")
                res,_ = self.validate_onoff_state(["healthy"])

            # Delete the app service if it was a part of the tests.
            if self.capella["clusters"]["app_id"] and not isinstance(
                    self.capella["clusters"]["app_id"], bool):
                # Wait for app_service to be in a stable state.
                self.log.info("Polling APP SERVICE: {}".format(
                    self.capella["clusters"]["app_id"]))
                start_time = time.time()

                res,_ = self.validate_onoff_state(
                        ["healthy", "turnedOff"],
                        app=self.capella["clusters"]["app_id"])
                while not res:
                    if time.time() > 1800 + start_time:
                        self.fail("!!!...App Service didn't stabilize within "
                                  "half an hour...!!!")
                    self.log.info("...Waiting further...")
                    res, _ = self.validate_onoff_state(
                        ["healthy", "turnedOff"],
                        app=self.capella["clusters"]["app_id"])

                # Delete App Service
                self.log.info("Deleting App Service: {}".format(
                    self.capella["clusters"]["app_id"]))
                res = self.capellaAPI.cluster_ops_apis.delete_appservice(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.capella["clusters"]["app_id"])
                if res.status_code != 202:
                    self.fail("Error while deleting the app service: {}"
                              .format(res.content))

                self.log.info("...Waiting for App Svc to be deleted...")
                if not self.wait_for_deletion(
                        self.cluster_id, self.capella["clusters"]["app_id"]):
                    self.log.error("!!!...App Svc could not be deleted...!!!")
                self.log.info("App Svc Deleted Successfully")

            # Delete the cluster that was created.
            self.log.info("Destroying Cluster: {}".format(self.cluster_id))
            res = self.capellaAPI.cluster_ops_apis.delete_cluster(
                self.organisation_id, self.project_id, self.cluster_id)
            if res.status_code != 202:
                self.log.error("Error while deleting cluster: {}."
                               .format(res.content))

            # Delete the created instances.
            self.log.info("Deleting INSTANCES: {}".format(
                self.instances))
            if self.flush_columnar_instances(self.instances):
                self.log.error("!!!...Instance(s) deletion failed...!!!")
            self.wait_for_deletion(instances=self.instances)

            # Wait for the cluster to be destroyed.
            self.log.info("Waiting for cluster to be destroyed.")
            if not self.wait_for_deletion(self.cluster_id):
                self.log.error("!!!...Cluster could not be destroyed...!!!")
            self.log.info("Cluster destroyed successfully.")
            self.cluster_id = None

            # Delete all the security related API Keys created and stored in
            # the tenant DICT during initial SETUP run
            self.log.info("Deleting {} API keys".format(len(self.api_keys)))
            self.delete_api_keys(self.api_keys)

            # Delete the projects that were created.
            projects = [self.project_id]
            if self.capella["tenant_id"]["otherProj"]:
                projects.append(self.other_project_id)
            if self.delete_projects(self.organisation_id, projects,
                                    self.curr_owner_key):
                self.log.error("Error while deleting projects.")
            else:
                self.log.info("Projects deleted successfully")
                self.project_id = None

            # Delete organizationOwner API keys
            self.log.info("Deleting the first OrgOwner API key")
            resp = self.capellaAPI.org_ops_apis.delete_api_key(
                self.organisation_id, self.org_owner_key1["id"])
            if resp.status_code != 204:
                self.log.error("Error while deleting the first OrgOwner key")
                self.log.error(resp.content)

            self.update_auth_with_api_token(self.org_owner_key2)
            self.log.info("Deleting the second OrgOwner API key")
            resp = self.capellaAPI.org_ops_apis.delete_api_key(
                self.organisation_id, self.org_owner_key2["id"])
            if resp.status_code != 204:
                self.log.error("Error while deleting the second OrgOwner key")
                self.log.error(resp.content)

            if hasattr(self, "v2_control_plane_api_access_key"):
                response = self.capellaAPI.delete_control_plane_api_key(
                    self.organisation_id, self.v2_control_plane_api_access_key)
                if response.status_code != 204:
                    self.log.error("Error while deleting V2 control plane key")
                    self.log.error(response.content)
        if "multi_project_1" in self.api_keys:
            del self.api_keys["multi_project_1"]
        if "multi_project_2" in self.api_keys:
            del self.api_keys["multi_project_2"]
        super(APIBase, self).tearDown()

    def create_v2_control_plane_api_key(self):
        # Generate the first set of API access and secret access keys
        # Currently v2 API is being used for this.
        response = self.capellaAPI.create_control_plane_api_key(
            self.organisation_id, "initial_api")
        if response.status_code == 201:
            response = response.json()
            self.v2_control_plane_api_access_key = response["id"]
            self.capella["tenant_id"]["v2key"] = \
                self.v2_control_plane_api_access_key
            self.update_auth_with_api_token(response)
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
        self.log.warning("Rate Limit hit by the key: {}."
                         .format(self.curr_owner_key))
        if self.curr_owner_key == self.org_owner_key1:
            self.org_owner_key1["blockTime"] = time.time()
            self.org_owner_key1["retryAfter"] = retry_after

            # verify if the other key is available to be used
            timeDiff = time.time() - self.org_owner_key2["blockTime"]
            if timeDiff < self.org_owner_key2["retryAfter"]:
                sleepTime = self.org_owner_key2["retryAfter"] - timeDiff
                self.log.debug("Key needs to wait for {} more seconds"
                               .format(sleepTime))
                time.sleep(sleepTime)
            self.curr_owner_key = self.org_owner_key2
        else:
            self.org_owner_key2["blockTime"] = time.time()
            self.org_owner_key2["retryAfter"] = retry_after

            # verify if the other key is available to be used
            timeDiff = time.time() - self.org_owner_key1["blockTime"]
            if timeDiff < self.org_owner_key1["retryAfter"]:
                sleepTime = self.org_owner_key1["retryAfter"] - timeDiff
                self.log.debug("Key needs to wait for {} more seconds"
                               .format(sleepTime))
                time.sleep(sleepTime)
            self.curr_owner_key = self.org_owner_key1

        self.log.debug("Tapping out, switching to the key: {}"
                       .format(self.curr_owner_key))
        self.update_auth_with_api_token(self.curr_owner_key)

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
                self.organisation_id, self.prefix + "security", o_roles,
                "", 0.5, ["0.0.0.0/0"], resource)
            if resp.status_code == 429:
                self.handle_rate_limit(int(resp.headers["Retry-After"]))
                resp = self.capellaAPI.org_ops_apis.create_api_key(
                    self.organisation_id, self.prefix + "security", o_roles,
                    "", 0.5, ["0.0.0.0/0"], resource)
            if resp.status_code == 201:
                api_key_dict["-".join(role_combination)] = {
                    "id": resp.json()["id"],
                    "token": resp.json()["token"],
                    "roles": role_combination
                }
            else:
                try:
                    resp = resp.json()
                    if 'errorType' in resp:
                        self.log.error("Error Message - {}, Error Type - {}"
                                       .format(resp["message"], resp["errorType"]))
                    else:
                        self.log.error("Error Message - {}".format(resp["message"]))
                except (Exception,):
                    self.log.error("Error received - {}".format(resp.content))
                # In order to delete the created keys.
                self.api_keys = api_key_dict
                self.fail("Error while generating API keys for role {}".format(
                    "-".join(role_combination)))
        self.log.info("{} API keys created, for all combination of roles"
                      .format(len(api_key_dict)))
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

    def update_auth_with_api_token(self, keyObj):
        self.capellaAPI.org_ops_apis.bearer_token = keyObj["token"]
        self.capellaAPI.cluster_ops_apis.bearer_token = keyObj["token"]
        self.columnarAPI.bearer_token = keyObj["token"]

    def v4_RBAC_injection_init(self, allowed_roles, other_proj=True,
                               expected_failure_code=None,
                               expected_failure_error=None):
        testcases = []
        for role in self.api_keys:
            testcase = {
                "description": "Calling API with {} role".format(role),
                "token": self.api_keys[role]["token"]
            }
            if expected_failure_code:
                testcase["expected_status_code"] = expected_failure_code
                testcase["expected_error"] = expected_failure_error
            if not any(element in allowed_roles for
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
        if other_proj:
            self.auth_test_extension(testcases, self.other_project_id,
                                     expected_failure_code,
                                     expected_failure_error)
        else:
            self.auth_test_extension(testcases, None, expected_failure_code,
                                     expected_failure_error)
        return testcases

    def make_parallel_api_calls(
            self, num_of_calls_per_api=100, apis_to_call=[],
            api_key_dict={}, batch_size=10, wait_time=0):
        """
        Method makes parallel api calls.
        param num_of_calls_per_api (int) Number of API calls per API to be made.
        param apis_to_call (list(list)) List of lists, where inner list is of
        format [api_function_call, function_args]
        param api_key_dict dict API keys to be used while making API calls
        """
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

    def auth_test_extension(self, testcases, other_project_id,
                            failure_expected_code=None,
                            failure_expected_error=None):
        testcases.extend([
            {
                "description": "Calling API with invalid bearer token",
                "token": self.replace_last_character(self.curr_owner_key["token"]),
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
            self.update_auth_with_api_token(self.curr_owner_key)
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
            self.update_auth_with_api_token(self.api_keys["organizationOwner_new"])
            del self.api_keys["organizationOwner_new"]
        elif "revoke_key" in testcase:
            self.update_auth_with_api_token(self.curr_owner_key)
            resp = self.capellaAPI.org_ops_apis.create_api_key(
                self.organisation_id, "Revoked_Key", ["organizationOwner"])
            if resp.status_code == 201:
                self.api_keys["revoked_key"] = resp.json()
            else:
                self.fail("Error while creating API key for organization "
                          "owner for revoking later")
            resp = self.capellaAPI.org_ops_apis.delete_api_key(
                self.organisation_id, self.api_keys["revoked_key"]["id"])
            if resp.status_code != 204:
                failures.append(testcase["description"])
            self.update_auth_with_api_token(self.api_keys["revoked_key"])
            del self.api_keys["revoked_key"]
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

            self.update_auth_with_api_token(self.curr_owner_key)

            # create a new API key with expiry of approx 2 mins
            resp = self.capellaAPI.org_ops_apis.create_api_key(
                self.organisation_id, "MultiProj_Key", org_roles,
                expiry=0.5, allowedCIDRs=["0.0.0.0/0"], resources=resource)
            if resp.status_code == 201:
                self.api_keys[key] = resp.json()
            else:
                self.fail("Error while creating API key for role having "
                          "access to multiple projects")
            self.update_auth_with_api_token(self.api_keys[key])
        else:
            self.update_auth_with_api_token(testcase)

    def validate_testcase(self, result, success_codes, testcase, failures,
                          validate_response=False, expected_res=None,
                          resource_id=None, payloadTest=False):
        # Parser for payload tests.
        testDescriptionKey = "description"
        if payloadTest:
            testDescriptionKey = "desc"

        try:
            # Condition for Sample Buckets delete testcases.
            if (6008 in success_codes and "code" in result.json() and
                    result.json()["code"] == 6008):
                self.log.info("This is an expected error handler for Sample "
                              "Buckets Test Cases. The test has passed!")
                return True
            # Condition for Create Buckets dupe name cases.
            if (6001 in success_codes and "code" in result.json() and
                    result.json()["code"] == 6001):
                self.log.info("This is an expected error handler for Create "
                              "Buckets Test Cases. The test has passed!")
                return True
        except Exception as e:
            if 6001 in success_codes or 6008 in success_codes:
                self.log.error(e)

        # Acceptor for expected error codes.
        if ("expected_status_code" in testcase and
                "expected_error" in testcase):
            if isinstance(testcase["expected_status_code"], list):
                for code in testcase["expected_status_code"]:
                    if code == result.status_code:
                        self.log.debug("The test expected one of the "
                                       "following error responses to the "
                                       "user: {}".format(
                                        testcase["expected_error"]))
            elif result.status_code == testcase["expected_status_code"]:
                try:
                    if result.content and result.content.strip(): # checking if the response is not empty
                        try:
                            result_json = result.json()
                            expected_error = testcase["expected_error"]
                            if isinstance(expected_error, dict):
                                if not self._compare_error_dict(result_json, expected_error):
                                    self.log.error("Correct Satus Code: {}, BUT wrong "
                                                   "error goof: {}".format(
                                                    testcase["expected_status_code"],
                                                    expected_error))
                                    self.log.warning("Failure : {}".format(result_json))
                                    failures.append(testcase[testDescriptionKey])
                            else:
                                if str(expected_error) not in str(result_json):
                                    self.log.error("Correct Satus Code: {}, BUT wrong "
                                                   "error goof: {}".format(
                                                    testcase["expected_status_code"],
                                                    expected_error))
                                    self.log.warning("Failure : {}".format(result_json))
                                    failures.append(testcase[testDescriptionKey])

                        except ValueError:
                            result_text = result.content.decode('utf-8', errors='replace')
                            expected_error = re.sub(r'\s+', '', str(testcase["expected_error"]))
                            result_text = re.sub(r'\s+', '', result_text)
                            if expected_error not in result_text:
                                self.log.error("Correct Status Code: {}, BUT expected error '{}' "
                                      "not found in non-JSON response".format(
                                    testcase["expected_status_code"],
                                    testcase["expected_error"]))
                                self.log.warning("Failure : {}".format(result_text))
                                failures.append(testcase[testDescriptionKey])
                    else:
                        # handle the empty response case
                        if testcase["expected_error"]:
                            self.log.error("Correct Satus Code: {}, BUT got empty response "
                                           "instead of expected error: {}".format(
                                            testcase["expected_status_code"],
                                            testcase["expected_error"]))
                            self.log.warning("Failure : {}".format(result))
                            failures.append(testcase[testDescriptionKey])
                except Exception as e:
                    self.log.debug("Error while conversion of response: {}"
                                   .format(e))
                    return True
                self.log.debug("This test expected the code: {}, with error: "
                               "{}".format(testcase["expected_status_code"],
                                           testcase["expected_error"]))
                self.log.debug("Response : {}".format(result))
            return False

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
        else:
            self.log.error("Expected HTTP status code {}, Actual HTTP status "
                           "code {}".format(testcase["expected_status_code"],
                                            result.status_code))
            self.log.warning("Result : {}".format(result.content))
            failures.append(testcase[testDescriptionKey])
        return False

    def _compare_error_dict(self, result_json, expected_error):
        """
        Compare two error dictionaries.
        """
        for key in expected_error:
            if key not in expected_error:
                return False

            expected_value = expected_error[key]
            actual_value = result_json[key]

            # String comparison
            if isinstance(expected_value, str) or isinstance(actual_value, str):
                expected_str = str(expected_value).strip().replace("'", "")
                actual_str = str(actual_value).strip().replace("'", "")
                if expected_str not in actual_str and actual_str not in expected_str:
                    return False
            # Direct comparison
            elif expected_value != actual_value:
                return False

        return True

    def validate_onoff_state(self, states, inst=None, app=None, free_tier=None,
                             sleep=10):
        self.update_auth_with_api_token(self.curr_owner_key)
        if sleep:
            time.sleep(sleep)
        if not free_tier and app:
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
        elif free_tier and not app:
            res = (self.capellaAPI.cluster_ops_apis.
                   fetch_free_tier_cluster_info(
                    self.organisation_id, self.project_id, free_tier))
            if res.status_code == 429:
                self.handle_rate_limit(int(res.headers['Retry-After']))
                res = (self.capellaAPI.cluster_ops_apis.
                       fetch_free_tier_cluster_info(
                        self.organisation_id, self.project_id, free_tier))
        elif free_tier and app:
            res = (self.capellaAPI.cluster_ops_apis.
                   fetch_free_tier_app_service_info(
                    self.organisation_id, self.project_id, free_tier, app))
            if res.status_code == 429:
                self.handle_rate_limit(int(res.headers['Retry-After']))
                res = (self.capellaAPI.cluster_ops_apis.
                       fetch_free_tier_app_service_info(
                        self.organisation_id, self.project_id, free_tier, app))
        else:
            res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                self.organisation_id, self.project_id, self.cluster_id)
            if res.status_code == 429:
                self.handle_rate_limit(int(res.headers['Retry-After']))
                res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                    self.organisation_id, self.project_id, self.cluster_id)
            elif res.status_code == 404:
                self.log.warning("The cluster: {} does not exist".format(
                    self.cluster_id))
                return True, None

        if res.status_code != 200:
            self.log.error("Could not fetch on/off state info : {}"
                           .format(res.content))

        if res.json()['currentState'] in states:
            self.log.info("Resource state: {}, Expected States: {}"
                          .format(res.json()["currentState"], states))
            if app:
                return True, None
            return True, res.json()['cloudProvider']['cidr']

        self.log.warning("Current State: '{}', Expected States: '{}'"
                         .format(res.json()["currentState"], states))
        return False, None

    def validate_api_response(self, expected_res, actual_res, id):
        for key in actual_res:
            if key not in expected_res:
                self.log.error("Key: {} not found in expRes: {}"
                               .format(key, expected_res))
                return False
            if key == "version" or not expected_res[key]:
                continue
            if key not in expected_res:
                self.log.error("Key: {} not present in expRes: {}"
                               .format(key, expected_res))
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
                            self.log.error("Service: {} not in RES"
                                           .format(service))
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
                self.log.error("Value mismatch for key: {}, expRes: {} VS RES:"
                               " {}".format(key, expected_res[key],
                                            actual_res[key]))
                return False
        return True

    def select_CIDR(self, org, proj, name, cloudProvider,
                    serviceGroups=None, availability=None, support=None,
                    couchbaseServer=None, header=None, **kwargs):
        self.log.info("Selecting CIDR for cluster deployment.")
        duplicate_CIDR_messages = [
            "Please ensure that the CIDR range is unique within this organisation",
            "Please ensure you are passing a unique CIDR block and try again.",
            "Please choose a different CIDR and try again",
            "overlaps with existing resource with CIDR"
        ]

        start_time = time.time()
        while time.time() - start_time < 1800:
            if serviceGroups is None:
                result = (self.capellaAPI.cluster_ops_apis.
                          create_free_tier_cluster(
                            org, proj, name, cloudProvider))
                if result.status_code == 429:
                    self.handle_rate_limit(int(result.headers["Retry-After"]))
                    result = (self.capellaAPI.cluster_ops_apis.
                              create_free_tier_cluster(
                                org, proj, name, cloudProvider))
            else:
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
            try:
                r = result.json()
                if any(i in r["message"] for i in duplicate_CIDR_messages):
                    cloudProvider["cidr"] = CapellaUtils.get_next_cidr() + "/20"
                    self.capella["clusters"]["cidr"] = cloudProvider["cidr"]
                    self.log.info("Trying CIDR: {}".format(cloudProvider["cidr"]))
                else:
                    return result
            except Exception as e:
                self.log.error("Ran into exception: {}".format(e))
                return result
        self.log.error("Couldn't find CIDR within half an hour.")

    def wait_for_deployment(self, clus_id=None, app_svc_id=None,
                            inst_id=None, pes=False):
        start_time = time.time()
        while start_time + 1800 > time.time():
            self.log.info("...Waiting further...")
            time.sleep(5)

            if app_svc_id and clus_id:
                state = self.capellaAPI.cluster_ops_apis.get_appservice(
                    self.organisation_id, self.project_id, clus_id,
                    app_svc_id)
                if state.status_code == 429:
                    self.handle_rate_limit(int(state.headers['Retry-After']))
                    state = self.capellaAPI.cluster_ops_apis.get_appservice(
                        self.organisation_id, self.project_id, clus_id,
                        app_svc_id)
            elif app_svc_id:
                state = self.capellaAPI.cluster_ops_apis.get_appservice(
                    self.organisation_id, self.project_id, self.cluster_id,
                    app_svc_id)
                if state.status_code == 429:
                    self.handle_rate_limit(int(state.headers['Retry-After']))
                    state = self.capellaAPI.cluster_ops_apis.get_appservice(
                        self.organisation_id, self.project_id, self.cluster_id,
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
                        self.log.debug("...Waiting further...")
                        time.sleep(2)
                    self.log.info("Instance {} deleted".format(instance))
                    instances.remove(instance)
                    temp_instances.remove(instance)
                if len(temp_instances) == 0:
                    self.log.info("All instances deleted successfully.")
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
            self.log.debug("Deleting Project: {}".format(project_id))
            resp = self.capellaAPI.org_ops_apis.delete_project(
                org_id, project_id)
            if resp.status_code == 429:
                self.handle_rate_limit(int(resp.headers["Retry-After"]))
                resp = self.capellaAPI.org_ops_apis.delete_project(
                    org_id, project_id)
            if resp.status_code != 204:
                self.log.error("Error: {}".format(resp.content))
                project_deletion_failed = project_deletion_failed or True
        return project_deletion_failed

    def create_alert_to_be_tested(self, proj_id, kind, name, config):
        self.update_auth_with_api_token(self.curr_owner_key)
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

    def create_bucket_to_be_tested(self, org_id, proj_id, clus_id,
                                   buck_name, buckets):
        # Wait for cluster to rebalance (if it is).
        self.update_auth_with_api_token(self.curr_owner_key)
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
            buckets.append(buck_id)
            return buck_id
        self.log.error(resp.content)
        self.fail("!!!...New bucket creation failed...!!!")

    def delete_buckets(self, buckets):
        self.update_auth_with_api_token(self.curr_owner_key)
        for bucket in buckets:
            self.log.debug("Deleting the bucket: {}".format(bucket))
            resp = self.capellaAPI.cluster_ops_apis.delete_bucket(
                self.organisation_id, self.project_id, self.cluster_id, bucket)
            if resp.status_code == 429:
                self.handle_rate_limit(int(resp.headers["Retry-After"]))
                resp = self.capellaAPI.cluster_ops_apis.delete_bucket(
                    self.organisation_id, self.project_id, self.cluster_id,
                    bucket)
            if resp.status_code != 204:
                self.log.error("Error: {}".format(resp.content))

            # Wait for cluster to rebalance (if it is).
            self.wait_for_deployment(self.cluster_id)
            self.log.debug("Cluster state healthy.")

    def create_scope_to_be_tested(self, org_id, proj_id, clus_id, buck_id):
        self.update_auth_with_api_token(self.curr_owner_key)

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
        self.update_auth_with_api_token(self.curr_owner_key)

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
        self.update_auth_with_api_token(self.curr_owner_key)

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
        self.update_auth_with_api_token(self.curr_owner_key)

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
        self.update_auth_with_api_token(self.curr_owner_key)

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

    def flush_appservices_admin_user(self, project_id, cluster_id,
                                   app_service_id, admin_id):
        self.update_auth_with_api_token(self.curr_owner_key)
        self.log.debug("flushing admin user :{}".format(admin_id))

        res = self.capellaAPI.cluster_ops_apis.delete_app_service_admin_user(
            self.organisation_id, project_id, cluster_id, app_service_id, admin_id)
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers['Retry-After']))
            res = self.capellaAPI.cluster_ops_apis.delete_app_service_admin_user(
                self.organisation_id, project_id, cluster_id, app_service_id,
                admin_id)
        if res.status_code != 202 :
            self.log.warning("Failure to delete Admin user: {}".format(admin_id))
            self.log.error("Response: {}".format(res.content))

    def create_columnar_instance_to_be_tested(self):
        self.update_auth_with_api_token(self.curr_owner_key)

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
            self.instances.add(res.json()["id"])
            return res.json()["id"]
        self.log.error(res.content)
        self.fail("!!!...Instance Creation unsuccessful...!!!")

    def flush_columnar_instances(self, instances):
        self.update_auth_with_api_token(self.curr_owner_key)

        instance_deletion_failed = False
        for instance in instances:
            self.log.debug("Deleting instance {}".format(instance))
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
                self.log.debug("Instance delete request successful.")

        return instance_deletion_failed

    def create_app_endpoint_oidc_Provider_to_be_tested(self, app_svc_id,
                                                       app_endpoint_name, Admin_user):
        oidc_overrides = {
            "issuer": Admin_user["issuer"],
            "clientId": Admin_user["clientId"]
        }

        res = self.capellaAPI.cluster_ops_apis.create_app_endpoint_o_i_d_c_provider(
            self.organisation_id, self.project_id, self.cluster_id,
            app_svc_id, app_endpoint_name, **oidc_overrides)
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.create_app_endpoint_o_i_d_c_provider(
                self.organisation_id, self.project_id, self.cluster_id,
                app_svc_id, app_endpoint_name, **oidc_overrides)
        if res.status_code != 201:
            self.log.error(res.content)
            self.tearDown()
            self.fail("!!!...Failed to create a replacement App "
                      "Endpoint...!!!")
        self.log.debug("...Replacement OIDC provider created successfully...")
        try:
            oidcProviderId = res.json().get("providerId")
            if not oidcProviderId:
                raise KeyError("providerId not found in response JSON.")
        except (ValueError, KeyError) as e:
            self.log.error(
                "Failed to extract providerId from response: {}".format(
                    res.content))
            self.tearDown()
            self.fail("!!!...Failed to retrieve providerId...!!!")
        return oidcProviderId

    def create_app_endpoint_to_be_tested(self, app_svc_id,app_endpoint_name,
                                         delta_sync, bucket, scopes, userXattrKey, cors):

        res = self.capellaAPI.cluster_ops_apis.create_app_endpoint(
            self.organisation_id, self.project_id, self.cluster_id,
            app_svc_id,
            app_endpoint_name, delta_sync,
            bucket, scopes,
            userXattrKey, cors)
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.create_app_endpoint(
                self.organisation_id, self.project_id, self.cluster_id,
                app_svc_id,
                app_endpoint_name, delta_sync,
                bucket, scopes,
                userXattrKey, cors)
        if res.status_code == 412:
            self.log.debug("App endpoint {} already exists".format(
                app_endpoint_name))
        elif res.status_code != 201:
            self.log.error(res.content)
            self.fail("!!!...Creating App Endpoint failed...!!!")
        self.log.info("Created App Endpoint: {} successfully".format(
            app_endpoint_name))

    def create_app_service_admin_user_to_be_tested(self,project_id, cluster_id,
                                         app_svc_id, **kwargs):
        res = self.capellaAPI.cluster_ops_apis.add_app_service_admin_user(
            self.organisation_id,
            project_id,
            cluster_id,
            app_svc_id,
            **kwargs
        )

        if res.status_code == 429:
            self.handle_rate_limit(res.headers.get("Retry-After"))
            res = self.capellaAPI.cluster_ops_apis.add_app_service_admin_user(
                self.organisation_id,
                project_id,
                cluster_id,
                app_svc_id,
                **kwargs
            )
        if res.status_code != 201:
            self.log.error("App Service admin user creation failed: {}".format(res.content))
            self.tearDown()
            self.fail("!!!...Failed to create App Service admin user to be "
                      "tested...!!!")

        self.log.debug("...App Service admin user created successfully...")

        try: 
            return res.json().get("id")
        except Exception as e:
            self.log.error("Error occured: {}".format(e))
            self.log.error("App Service admin user creation failed: {}".format(res.content))
        return None

    def fetch_free_tier_cluster(self):
        self.log.debug(
            "Fetching the free tier cluster in Org: {}, "
            "in Project: {}".format(self.organisation_id, self.project_id))

        res = self.capellaAPI.cluster_ops_apis.list_clusters(
            self.organisation_id, self.project_id)
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.list_clusters(
                self.organisation_id, self.project_id)
        if res.status_code != 200:
            self.log.error(res.content)
            self.tearDown()
            self.fail("!!!...Filed while listing clusters...!!!")
        try:
            res = res.json()
            if not len(res["data"]):
                self.log.warning("No clusters exist...!!!\nCreating one...")
                return None
            for clus in res["data"]:
                if clus["support"]["plan"] == "free":
                    self.log.info("Found free tier cluster, id : {}".format(
                        clus["id"]))
                    self.free_tier_cluster_id = clus["id"]
                    return clus['name'], clus["cloudProvider"]["cidr"]
            self.log.warning("No free tier cluster found in the list "
                             "of clusters :(\nCreating one...")
        except Exception as e:
            self.log.error(e)
            self.tearDown()
            self.fail()
        return None

    def fetch_free_tier_app(self, clus):
        self.log.debug("Fetching the id for the PFT App...")
        res = self.capellaAPI.cluster_ops_apis.list_appservices(
            self.organisation_id)
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.list_appservices(
                self.organisation_id)
        if res.status_code != 200:
            self.log.error(res.content)
            self.tearDown()
            self.fail("!!!...Filed while listing Apps...!!!")
        try:
            res = res.json()
            if not len(res["data"]):
                self.log.warning("No Apps exist...!!!\nCreating one...")
                return None
            for app in res["data"]:
                if clus == app["clusterId"]:
                    self.log.info("Found free tier App, id: {}"
                                  .format(app["id"]))
                    return app["id"]
            self.log.warning("No free tier App found in the list of Apps :("
                             "\nCreating one...")
        except Exception as e:
            self.log.error(e)
            self.tearDown()
            self.fail()
        return None

    def flush_freetier_buckets(self, clus):
        res = self.capellaAPI.cluster_ops_apis.list_free_tier_bucket(
            self.organisation_id, self.project_id, clus)
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.list_free_tier_bucket(
                self.organisation_id, self.project_id, clus)
        if res.status_code != 200:
            self.log.error("Err: {}".format(res.content))
            self.tearDown()
            self.fail("!!!...Error while listing buckets...!!!")
        for buck in res.json()["data"]:
            bkt = buck["id"]
            res = self.capellaAPI.cluster_ops_apis.delete_free_tier_bucket(
                self.organisation_id, self.project_id, clus, bkt)
            if res.status_code == 429:
                self.handle_rate_limit(int(res.headers["Retry-After"]))
                res = self.capellaAPI.cluster_ops_apis.delete_free_tier_bucket(
                    self.organisation_id, self.project_id, clus, buck)
            if res.status_code != 204:
                self.log.error(res.content)
                self.log.error("Failed while deleting bucket: {}".format(bkt))
            self.log.debug("Bucket: {} deleted successfully".format(bkt))

    def create_freetier_bucket_to_be_tested(self, clus, name, mem):
        res = self.capellaAPI.cluster_ops_apis.create_free_tier_bucket(
            self.organisation_id, self.project_id, clus, name, mem)
        if res.status_code == 429:
            self.handle_rate_limit(int(res.header["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.create_free_tier_bucket(
                self.organisation_id, self.project_id, clus, name, mem)
        if res.status_code != 201:
            self.log.error("Err: {}".format(res.content))
            self.tearDown()
            self.fail("!!!...Error while creating free tier bucket...!!!")

    def flush_oidcProviders(self, project_id, cluster_id, app_service_id,
                           appEndpointName, oidcProviderId):
        self.update_auth_with_api_token(self.curr_owner_key)

        oidProvider_Deletion_Failed = False
        res = self.capellaAPI.cluster_ops_apis.delete_app_endpoint_o_i_d_c_provider(
            self.organisation_id, project_id, cluster_id, app_service_id,
            appEndpointName, oidcProviderId)
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.delete_app_endpoint_o_i_d_c_provider(
                self.organisation_id, project_id, cluster_id, app_service_id,
                appEndpointName, oidcProviderId)
        if res.status_code != 202:
            self.log.warning("Error while deleting OidcProvider {}".format(
                oidcProviderId))
            self.log.error("Response: {}".format(res.content))
            oidProvider_Deletion_Failed = True

        return oidProvider_Deletion_Failed

    def create_user_to_be_tested(self, resources, email, name, organizationRoles):
        self.log.info("Creating a User for next test in the queue.")

        res = self.capellaAPI.cluster_ops_apis.create_user(
            self.organisation_id, resources, email, name, organizationRoles)
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.create_user(
                self.organisation_id, resources, email, name, organizationRoles)
        if res.status_code != 201:
            self.log.error("Result: {}".format(res.content))
            self.tearDown()
            self.fail("Error while creating User.")
        return res.json()["id"]

    def delete_user(self, user_id):
        self.log.info("Deleting a User for next test in the queue.")
        res = self.capellaAPI.cluster_ops_apis.delete_user(
            self.organisation_id, user_id)
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.delete_user(
                self.organisation_id, user_id)
        if res.status_code != 204:
            self.log.error("Result: {}".format(res.content))
            self.tearDown()
            self.fail("Error while deleting User.")
        self.log.info("User deleted successfully.")
