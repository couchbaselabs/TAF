"""
Created on June 28, 2023

@author: umang.agrawal
"""

from cgi import test
import os
import re
import json
import copy
import time
import string
import random
import base64
import calendar
import itertools
import threading
from datetime import datetime
from logging import exception

from pytests.cb_basetest import CouchbaseBaseTest
from capellaAPI.capella.dedicated.CapellaAPI_v4 import CapellaAPI
from capellaAPI.capella.columnar.ColumnarAPI_v4 import ColumnarAPIs
from couchbase_utils.capella_utils.dedicated import CapellaUtils
from couchbase_utils.capella_utils import capella_http
from TestInput import TestInputSingleton

# Serialises writes to the collected-logs list from the parallel dp-agent and server log collectors.
_collected_logs_lock = threading.Lock()


class APIBase(CouchbaseBaseTest):

    @staticmethod
    def normalize_server_version(raw_version):
        # "8.0.3-0000" -> "8.0", "7.6.12-0000" -> "7.6", "8.5" -> "8.5"
        parts = str(raw_version).split("-")[0].split(".")
        return ".".join(parts[:2])

    def _get_server_version(self):
        # -p param wins; otherwise use the [capella] section from the ini
        return self.input.param(
            "server_version",
            self.input.capella.get("server_version", "7.6"))

    # Some regions aren't available on every cloud provider - requesting
    # one of these maps to the equivalent region on azure/gcp instead,
    # while aws (and any provider not listed here) uses it as-is.
    REGION_EQUIVALENTS = {
        "eu-west-1": {"azure": "westus3", "gcp": "europe-west2"}
    }

    # Errors a create returns when its CIDR clashes with an existing one.
    DUPLICATE_CIDR_MESSAGES = [
        "Please ensure that the CIDR range is unique within this organisation",
        "Please ensure you are passing a unique CIDR block and try again.",
        "Please choose a different CIDR and try again",
        "overlaps with existing resource with CIDR"
    ]

    def _get_region(self, default, cloud_provider="aws"):
        # -p param wins; otherwise use the [capella] section from the ini
        region = self.input.param(
            "region", self.input.capella.get("region", default))
        return self.REGION_EQUIVALENTS.get(region, {}).get(
            cloud_provider, region)

    def _get_cmek_arn(self, default=""):
        # -p param wins; otherwise use the [capella] section from the ini
        return self.input.param(
            "cmek_arn", self.input.capella.get("cmek_arn", default))

    def create_cmek_metadata(self, org_id, name, config, description="",
                             attempts=3, wait=20):
        """org_ops_apis.create_cmek_metadata, retried while Capella's own
        kms:DescribeKey check of a valid key fails transiently. The same
        ARN passes on the runs before and after (19875), so a 422 naming
        kms:DescribeKey is retried; any other response is returned as is."""
        for attempt in range(1, attempts + 1):
            result = self.capellaAPI.org_ops_apis.create_cmek_metadata(
                org_id, name, config, description)
            if result.status_code != 422 or attempt == attempts or \
                    "kms:DescribeKey" not in str(result.content):
                return result
            self.log.warning(
                "CMEK create got 422 from kms:DescribeKey (attempt {}/{}, "
                "request_id={}), retrying in {}s".format(
                    attempt, attempts,
                    result.headers.get("Vnd-Cloud-Couchbase-Com-Request-Id"),
                    wait))
            time.sleep(wait)

    def setUp(self, nomenclature="WRAPPER", services=[]):
        CouchbaseBaseTest.setUp(self)

        # Timeouts, retries and a single 429 policy for every Capella client
        # this test creates (couchbase_utils/capella_utils/capella_http.py).
        # Tune with capella_http_<option>=<value> test params.
        capella_http.install(capella_http.Config.from_params(self.input),
                             logger=self.log)
        capella_http.set_rate_limit_handler(
            self._owner_key_rate_limit_handler)

        # An earlier test of this run lost the shared setup (API keys,
        # WRAPPER project or the shared cluster) and already cleaned up.
        # Every later test would only fail slowly against resources that
        # don't exist, so fail now, in seconds, with the original reason.
        broken = self.input.capella.get("shared_setup_broken")
        if broken:
            capella_http.set_rate_limit_handler(None)
            self.fail("NOT RUN - shared setup failed earlier in this run: {}"
                      .format(broken))

        self.capella = self.input.capella
        self.url = self.input.capella.get("pod")
        self.user = self.input.capella.get("capella_user")
        self.passwd = self.input.capella.get("capella_pwd")
        self.organisation_id = self.input.capella.get("tenant_id")
        self.invalid_UUID = "00000000-0000-0000-0000-000000000000"
        self.prefix = "Automated_v4API_test_"
        # Unique per shard/run, so security-key cleanup never touches keys
        # belonging to another shard concurrently using the same org.
        self.security_key_prefix = self.prefix + "security_" + \
            self.input.test_params.get("cluster_name", "") + "_"
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
            # A run that didn't start with a security TC (e.g. GROUP=backup)
            # never set up the other project needed by auth tests.
            if self.is_security_test() and not self.other_project_id:
                self.other_project_id = self.get_or_create_auth_project()
                self.capella["tenant_id"]["otherProj"] = self.other_project_id
            # Recreate security keys if cleared by previous TC's tearDown.
            if self.is_security_test() and not self.api_keys:
                self.api_keys.update(
                    self.create_api_keys_for_all_combinations_of_roles(
                        [self.capella["project"]]))
                self.capella["tenant_id"]["apiKeys"] = self.api_keys
        else:
            self.capella["tenant_id"] = {
                "id": self.organisation_id,
                "otherProj": None
            }
            self.other_project_id = None
            self.create_v2_control_plane_api_key()

            # Free up API key quota held by keys leaked from earlier
            # aborted/crashed runs (any shard) before creating our own.
            self.cleanup_stale_automation_api_keys("start of run")

            # create the first V4 API KEY WITH organizationOwner role, which
            # will be used to perform further V4 api operations
            resp = self.create_api_key_with_quota_retry(
                self.organisation_id, self.prefix + "OrgOwnerKey-1",
                ["organizationOwner"], self.prefix, 1)
            if resp.status_code == 201:
                self.org_owner_key1 = resp.json()
                self.org_owner_key1["blockTime"] = time.time()
                self.org_owner_key1["retryAfter"] = 0
                self.capella["tenant_id"]["key1"] = self.org_owner_key1
            else:
                reason = ("Error while creating first OrgOwner v4 API key: "
                          "status={} body={}".format(resp.status_code,
                                                     resp.content))
                self._abort_shared_setup(reason)
                self.fail("!!!...{}...!!!".format(reason))

            # Create another API key with Org Owner Privileges to tap out for
            # one another at time of Rate Limiting.
            resp = self.create_api_key_with_quota_retry(
                self.organisation_id, self.prefix + "OrgOwnerKey-2",
                ["organizationOwner"], self.prefix, 1)
            if resp.status_code == 201:
                self.org_owner_key2 = resp.json()
                self.org_owner_key2["blockTime"] = time.time()
                self.org_owner_key2["retryAfter"] = 0
                self.capella["tenant_id"]["key2"] = self.org_owner_key2
            else:
                reason = ("Error while creating second OrgOwner v4 API key: "
                          "status={} body={}".format(resp.status_code,
                                                     resp.content))
                self._abort_shared_setup(reason)
                self.fail("!!!...{}...!!!".format(reason))

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
                self.project_id = self.find_project_by_name(
                    self.organisation_id, self.prefix + "WRAPPER")
                if self.project_id:
                    self.log.info("Reusing existing project: {}"
                                  .format(self.project_id))
                else:
                    self.log.info(
                        "Creating the functional-test required project")
                    res = self.capellaAPI.org_ops_apis.create_project(
                        self.organisation_id, self.prefix + "WRAPPER")
                    if res.status_code != 201:
                        reason = "Project creation failed: HTTP {} {}".format(
                            res.status_code, res.content)
                        self._abort_shared_setup(reason)
                        self.fail("!!!..{}...!!!".format(reason))
                    else:
                        self.log.info("Project Creation Successful")
                        self.project_id = res.json()["id"]
                self.capella["project"] = self.project_id

            self.api_keys = dict()
            if self.is_security_test():
                self.other_project_id = self.get_or_create_auth_project()
                self.capella["tenant_id"]["otherProj"] = self.other_project_id

                if "apiKeys" not in self.capella["tenant_id"] or not \
                        self.capella["tenant_id"]["apiKeys"]:
                    # (Leftover keys from aborted runs were already cleaned
                    # up by cleanup_stale_automation_api_keys above.)
                    # Create security API keys (size-1 + size-2 combos = 36).
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
                    "region": self._get_region("us-east-1"),
                    "cidr": "10.1.0.0/20"
                },
                "couchbaseServer": {
                    "version": self.normalize_server_version(self._get_server_version()),
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
            "AWS_r5_xlarge_eventing": {
                    "cloudProvider": {
                        "type": "aws",
                        "region": self._get_region("us-east-1"),
                        "cidr": "10.1.0.0/20"
                    },
                    "couchbaseServer": {
                        "version": self.normalize_server_version(self._get_server_version()),
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
                            "numOfNodes": self.input.param("numOfNodes", 4),
                            "services": [
                                "data",
                                "index",
                                "query",
                                "eventing"
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
                    "region": self._get_region("us-east-1"),
                    "cidr": "10.1.0.0/20"
                },
                "couchbaseServer": {
                    "version": self.normalize_server_version(self._get_server_version()),
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
                    "region": self._get_region("eastus", "azure"),
                    "cidr": "10.1.0.0/20"
                },
                "couchbaseServer": {
                    "version": self.normalize_server_version(self._get_server_version()),
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
                    "region": self._get_region("eastus", "azure"),
                    "cidr": "10.1.0.0/20"
                },
                "couchbaseServer": {
                    "version": self.normalize_server_version(self._get_server_version()),
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
                    "region": self._get_region("us-east1", "gcp"),
                    "cidr": "10.1.0.0/20"
                },
                "couchbaseServer": {
                    "version": self.normalize_server_version(self._get_server_version()),
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
                    "region": self._get_region("us-east1", "gcp"),
                    "cidr": "10.1.0.0/20"
                },
                "couchbaseServer": {
                    "version": self.normalize_server_version(self._get_server_version()),
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
                    "region": self._get_region("us-east-1"),
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
            # select_CIDR returns None when no free CIDR was found in 30 min.
            if res is None or res.status_code != 202:
                reason = "Cluster creation Failed: {}".format(
                    "no free CIDR within 30 min" if res is None else
                    "HTTP {} {}".format(res.status_code, res.content))
                self._abort_shared_setup(reason)
                self.fail("!!!...{}...!!!".format(reason))
            try:
                self.cluster_id = res.json()["id"]
            except (Exception,):
                reason = "Couldn't decipher cluster creation result: " \
                         "{}".format(res.content)
                self._abort_shared_setup(reason)
                self.fail("!!!...{}...!!!".format(reason))
            self.capella["clusters"]["cluster_id"] = self.cluster_id
        self.cluster_templates[cluster_template]["cloudProvider"][
            "cidr"] = self.capella["clusters"]["cidr"]

        # Templates for instance configurations across CSPs and Computes
        self.instance_templates = {
            # TEMPLATES :
            "AWS_singleNode": {
                "nodes": self.input.param("nodes", 1),
                "region": self._get_region("us-east-1"),
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
                "region": self._get_region("us-east-1"),
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
        elif not self.needs_columnar_instance():
            # Most suites never touch it; creating one for every run cost
            # an instance per shard and failed whole suites when columnar
            # capacity was short.
            self.analyticsCluster_id = None
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
                self.fail("!!!...Instance creation Failed: HTTP {} {}...!!!"
                          .format(res.status_code, res.content))
            self.analyticsCluster_id = res.json()["id"]
            self.capella["instance_id"] = self.analyticsCluster_id
            # It is now created by the first suite that needs it rather
            # than at the start of the run, and not every columnar suite
            # waits for it, so wait here once.
            self.log.info("Waiting for columnar instance {} to deploy"
                          .format(self.analyticsCluster_id))
            if not self.wait_for_deployment(
                    inst_id=self.analyticsCluster_id):
                self.tearDown()
                self.fail("!!!...Columnar instance {} did not deploy (see "
                          "states above)...!!!".format(
                              self.analyticsCluster_id))
        self.instances = {self.analyticsCluster_id} \
            if self.analyticsCluster_id else set()

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

    # Suites that use self.analyticsCluster_id / self.instances.
    COLUMNAR_SUITES = ("ClustersColumnar", "ColumnarOnOffSchedule",
                       "PrivateEndpointsColumnar",
                       "AnalyticsCloudSnapshotBackups")

    def needs_columnar_instance(self):
        """
        columnar_instance=auto (default): only for the COLUMNAR_SUITES
        modules; always / never force it.
        """
        mode = str(self.input.param("columnar_instance", "auto")).lower()
        if mode in ("always", "true"):
            return True
        if mode in ("never", "false"):
            return False
        module = type(self).__module__ or ""
        return any(name in module for name in self.COLUMNAR_SUITES)

    ROUTING_REJECTION_CODES = (400, 404, 405)
    MALFORMED_PATH_CASES = ("add an invalid segment to the uri",
                            "add invalid segment to uri",
                            "add invalid segment to activationstate path",
                            "replace the last path param name in uri")

    def _is_malformed_path_case(self, description, testcase):
        """
        True for cases that only check a malformed URL is rejected: a bad
        route segment, or a non-hex ID in the path (400 from validation or
        404 from the router are both correct rejections).
        """
        if self.input.param("strict_routing_codes", False):
            return False
        expected = testcase.get("expected_status_code")
        if expected not in self.ROUTING_REJECTION_CODES:
            return False
        desc = str(description).strip().lower()
        if "url" in testcase and desc in self.MALFORMED_PATH_CASES:
            return True
        return "non-hex" in desc and expected in (400, 404) and any(
            k.startswith("invalid_") for k in testcase)

    def _targets_shared_cluster(self, result):
        """True if the request behind `result` was for this run's shared
        cluster (capella["clusters"]["cluster_id"])."""
        clusters = self.capella.get("clusters")
        shared_id = clusters.get("cluster_id") \
            if isinstance(clusters, dict) else None
        request = getattr(result, "request", None)
        url = str(getattr(request, "url", "") or "")
        return bool(shared_id) and "/clusters/{}".format(shared_id) in url

    def _abort_shared_setup(self, reason, collect_logs=True):
        """
        The run-wide setup (OrgOwner keys, WRAPPER project, shared cluster)
        failed. Release what this run created now - the shared cluster is
        the expensive part - and flag the run so every later test fails in
        seconds (see the top of setUp) instead of running for hours against
        resources that don't exist. The last-TC tearDown can't be relied on
        for this: unittest never calls tearDown when setUp fails.
        """
        capella = self.input.capella
        if capella.get("shared_setup_broken"):
            return
        capella["shared_setup_broken"] = reason
        self.log.critical("SHARED SETUP FAILED: {}. Cleaning up now; every "
                          "remaining test in this run will be reported as "
                          "NOT RUN.".format(reason))

        def best_effort(what, fn, *args):
            try:
                resp = fn(*args)
                self.log.info("cleanup: {} -> HTTP {}".format(
                    what, getattr(resp, "status_code", None)))
            except Exception as e:
                self.log.error("cleanup: {} failed: {}".format(what, e))

        tenant = capella.get("tenant_id")
        tenant = tenant if isinstance(tenant, dict) else {}
        owner_keys = [k for k in (tenant.get("key1"), tenant.get("key2"))
                      if isinstance(k, dict) and k.get("token")]
        org = self.organisation_id
        proj = getattr(self, "project_id", None)
        clusters = capella.get("clusters")
        clusters = clusters if isinstance(clusters, dict) else {}
        cluster_id = clusters.get("cluster_id")
        app_id = clusters.get("app_id")

        if owner_keys:
            self.update_auth_with_api_token(owner_keys[0])
            if proj and cluster_id:
                if app_id and not isinstance(app_id, bool):
                    best_effort("delete app service {}".format(app_id),
                                self.capellaAPI.cluster_ops_apis
                                .delete_appservice, org, proj, cluster_id,
                                app_id)
                if collect_logs:
                    self.collect_cluster_logs(cluster_id)
                best_effort("delete cluster {}".format(cluster_id),
                            self.capellaAPI.cluster_ops_apis.delete_cluster,
                            org, proj, cluster_id)
            if proj and capella.get("instance_id"):
                best_effort("delete columnar instance {}".format(
                    capella["instance_id"]),
                    self.columnarAPI.delete_analytics_cluster, org, proj,
                    capella["instance_id"])
            if getattr(self, "api_keys", None):
                try:
                    self.delete_api_keys(self.api_keys)
                except Exception as e:
                    self.log.error("cleanup: security keys failed: {}"
                                   .format(e))
                tenant["apiKeys"] = {}
            for key in owner_keys:
                self.update_auth_with_api_token(key)
                best_effort("delete OrgOwner key {}".format(key.get("id")),
                            self.capellaAPI.org_ops_apis.delete_api_key,
                            org, key.get("id"))
        v2key = tenant.get("v2key") or getattr(
            self, "v2_control_plane_api_access_key", None)
        if v2key:
            best_effort("delete v2 control plane key",
                        self.capellaAPI.delete_control_plane_api_key,
                        org, v2key)

    def is_security_test(self):
        """
        True when this TC needs the security setup (Auth_Project + API keys
        for every role combination). The runtime GROUP can hold several
        ';'-separated groups (e.g. "security;backup") or not name security
        at all (e.g. "backup", "P2") while still running test_authorization,
        so don't rely on GROUP == "security" alone.
        """
        groups = str(self.input.param("GROUP", "functional")).split(";")
        return ("security" in groups or
                self._testMethodName == "test_authorization")

    def get_or_create_auth_project(self):
        # Reuse a residual project used for auth verification tests, IF
        # already present.
        project_id = self.find_project_by_name(
            self.organisation_id, "Auth_Project")
        if project_id:
            self.log.info("Reusing existing project: {}".format(project_id))
            return project_id
        self.log.info("Creating the security-test required project")
        resp = self.capellaAPI.org_ops_apis.create_project(
            self.organisation_id, "Auth_Project")
        if resp.status_code == 201:
            return resp.json()["id"]
        self.fail("Error while creating project: {}".format(resp.content))

    def tearDown(self):
        # Delete security keys after every TC to prevent accumulation
        # against the org-level API key limit. This must run before the
        # organizationOwner keys are deleted below (on the last TC) -
        # deleting the key currently authenticating this session
        # immediately invalidates it, so any cleanup attempted afterwards
        # (like this one) would fail with 401 on every call.
        if self.is_security_test() and self.api_keys:
            self.log.info("Deleting {} security API keys after TC".format(
                len(self.api_keys)))
            self.delete_api_keys(self.api_keys)
            self.capella["tenant_id"]["apiKeys"] = {}
            self.api_keys = {}

        # Delete the WRAPPER resources, IF, the current test is the last
        # testcase being run.
        if (TestInputSingleton.input.test_params["case_number"] ==
                TestInputSingleton.input.test_params["no_of_test_identified"]):
            # Wait of cluster to be in a stable state
            self.log.info("Polling CLUSTER: {}".format(
                self.capella["clusters"]["cluster_id"]))
            start_time = time.time()
            # deploymentFailed is final too: still delete the cluster, there is just nothing to collect.
            res,_ = self.validate_onoff_state(["healthy", "deploymentFailed"])
            while not res:
                if time.time() > 1800 + start_time:
                    self.collect_cluster_logs(self.cluster_id)
                    self.fail("!!!...Cluster didn't stabilize within half an "
                              "hour...!!!")
                res,_ = self.validate_onoff_state(["healthy", "deploymentFailed"])
            info = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                self.organisation_id, self.project_id, self.cluster_id)
            deployment_failed = info.status_code == 200 and \
                info.json().get("currentState") == "deploymentFailed"

            # Collect server and dp-agent logs before anything below can fail the teardown (collect_logs=True).
            if deployment_failed:
                self.skip_cluster_logs(
                    self.cluster_id, "cluster never finished deploying "
                    "(state deploymentFailed), so no node can answer")
            else:
                self.collect_cluster_logs(self.cluster_id)

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

            # Delete the created instances (none unless a columnar suite ran).
            if self.instances:
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

            # The WRAPPER project and Auth_Project are found by name and
            # shared by every suite running in parallel on this org, so
            # the first suite to finish used to delete them from under the
            # others (stale Auth_Project / "project not found" failures).
            # Keep them as long-lived fixtures; delete_shared_projects=True
            # restores the old clean-up for a one-off run on its own org.
            if self.input.param("delete_shared_projects", False):
                projects = [self.project_id]
                if self.capella["tenant_id"]["otherProj"]:
                    projects.append(self.other_project_id)
                if self.delete_projects(self.organisation_id, projects,
                                        self.curr_owner_key):
                    self.log.error("Error while deleting projects.")
                else:
                    self.log.info("Projects deleted successfully")
                    self.project_id = None
            else:
                self.log.info("Keeping shared projects {} for other suites "
                              "on this org (delete_shared_projects=False)"
                              .format([p for p in [self.project_id,
                                                   self.other_project_id]
                                       if p]))

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
        capella_http.set_rate_limit_handler(None)
        super(APIBase, self).tearDown()

    def create_v2_control_plane_api_key(self):
        # Generate the first set of API access and secret access keys
        # Currently v2 API is being used for this.
        # lib/capellaAPI logs in through _urllib_request, which returns None
        # (and then raises AttributeError inside the lib) when the pod
        # can't be reached. Either way the run has no credentials, so stop
        # it here instead of letting every test trip over the half-built
        # tenant record.
        try:
            response = self.capellaAPI.create_control_plane_api_key(
                self.organisation_id, "initial_api")
        except Exception as e:
            response = None
            error = "{}: {}".format(type(e).__name__, e)
        else:
            error = "no response" if response is None else \
                "HTTP {} {}".format(response.status_code, response.content)
        if response is None or response.status_code != 201:
            reason = "could not log in / create the V2 control plane API " \
                     "key on {} as {}: {}".format(self.url, self.user, error)
            self._abort_shared_setup(reason)
            self.fail(reason)
        response = response.json()
        self.v2_control_plane_api_access_key = response["id"]
        self.capella["tenant_id"]["v2key"] = \
            self.v2_control_plane_api_access_key
        self.update_auth_with_api_token(response)

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
    
    @staticmethod
    def generate_random_string_with_one_digit(length=10, special_characters=True, prefix=""):
        if special_characters:
            special_chars = "!@#$%^&*()-_=+{[]}\|;:'\",.<>/?" + " " + "\t"
        else:
            special_chars = ""

        characters = string.ascii_letters + string.digits + special_chars

        # Ensure at least one digit
        result = [random.choice(string.digits)]

        # Fill remaining
        result += [random.choice(characters) for _ in range(length - 1)]

        # Shuffle to avoid digit always first
        random.shuffle(result)

        name = "".join(result)

        if prefix:
            name = prefix + name

        return name

    def _owner_key_rate_limit_handler(self, api_obj, wait):
        """
        capella_http calls this on a 429. When the throttled call was made
        with the current org-owner key through one of this test's own
        clients, switch to the other owner key (handle_rate_limit) so the
        retry doesn't have to sleep. Anything else (role keys, inline
        clients) returns False and capella_http waits Retry-After.
        """
        owner = getattr(self, "curr_owner_key", None)
        if not isinstance(owner, dict) or \
                not getattr(self, "org_owner_key2", None):
            return False
        own_clients = [getattr(self, "capellaAPI", None),
                       getattr(self, "columnarAPI", None)]
        capella_api = own_clients[0]
        if capella_api is not None:
            own_clients.extend([getattr(capella_api, "org_ops_apis", None),
                                getattr(capella_api, "cluster_ops_apis",
                                        None)])
        if not any(api_obj is c for c in own_clients if c is not None):
            return False
        if getattr(api_obj, "bearer_token", None) != owner.get("token"):
            return False
        self.handle_rate_limit(wait)
        return True

    def handle_rate_limit(self, retry_after):
        self.log.warning("Rate Limit hit by the key: {} (retry after {}s)."
                         .format(self.curr_owner_key.get("id"), retry_after))
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
                       .format(self.curr_owner_key.get("id")))
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
        for r in range(1, 3):
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

            resp = self.create_api_key_with_quota_retry(
                self.organisation_id, self.security_key_prefix, o_roles,
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
                # This runs from setUp, and unittest skips tearDown when
                # setUp fails - delete the keys created so far here, or
                # they leak and eat into the org-wide API key quota.
                if api_key_dict:
                    self.log.warning(
                        "Deleting {} API keys created before the failure"
                        .format(len(api_key_dict)))
                    self.delete_api_keys(api_key_dict)
                self.api_keys = dict()
                self.fail("Error while generating API keys for role {}".format(
                    "-".join(role_combination)))
        self.log.info("{} API keys created, for all combination of roles"
                      .format(len(api_key_dict)))
        return api_key_dict

    # Names of short-lived keys auth_test_setup creates without the
    # automation prefix.
    AUTH_TEST_KEY_NAMES = ("MultiProj_Key", "Expiry_Key", "Revoked_Key")

    @staticmethod
    def is_api_key_limit_error(resp):
        try:
            return "limit has been reached" in resp.content
        except Exception:
            return False

    @staticmethod
    def _api_key_created_epoch(key):
        """createdAt (e.g. 2026-11-19T07:09:12.123456789Z) as UTC epoch."""
        created = (key.get("audit") or {}).get("createdAt")
        if not created:
            return None
        try:
            return calendar.timegm(time.strptime(
                created[:19], "%Y-%m-%dT%H:%M:%S"))
        except (ValueError, TypeError):
            return None

    def cleanup_stale_automation_api_keys(self, reason=""):
        """
        Delete API keys leaked by earlier automation runs that never got
        to their tearDown (aborted/killed builds, setUp crashes), from
        ANY shard sharing this org, so they stop counting against the
        org-wide API key quota ("Unable to create an API key - the limit
        has been reached").

        Only automation-named keys are touched, and only once they are
        older than anything a live run could still be using:
          - per-TC security role keys (<prefix>security_*) and the
            auth-test keys: older than stale_role_key_age_hours (def. 1h;
            a run deletes them in each TC's tearDown, and the longest
            auth TC observed holds them ~16 min)
          - org owner keys (<prefix>OrgOwnerKey-*), which live for a
            whole conf run: older than stale_owner_key_age_hours (def.
            25h; they're created with a 1-day expiry anyway)
        Never raises; returns the number of keys deleted.
        """
        role_age = float(self.input.param("stale_role_key_age_hours", 1)) \
            * 3600
        owner_age = float(self.input.param("stale_owner_key_age_hours",
                                           25)) * 3600
        keep_ids = set()
        for key in [getattr(self, "org_owner_key1", None),
                    getattr(self, "org_owner_key2", None),
                    getattr(self, "curr_owner_key", None)]:
            if isinstance(key, dict) and key.get("id"):
                keep_ids.add(key["id"])
        for key in (getattr(self, "api_keys", None) or {}).values():
            if isinstance(key, dict) and key.get("id"):
                keep_ids.add(key["id"])

        now = time.time()
        stale, by_name, total = [], {}, 0
        page = 1
        throttled = 0
        try:
            while True:
                resp = self.capellaAPI.org_ops_apis.list_api_keys(
                    self.organisation_id, page=page, perPage=100)
                if resp.status_code == 429 and throttled < 3:
                    throttled += 1
                    time.sleep(int(resp.headers.get("Retry-After", 60)))
                    continue
                if resp.status_code != 200:
                    self.log.warning(
                        "API key cleanup ({}): could not list keys: "
                        "status={} body={}".format(
                            reason, resp.status_code, resp.content))
                    break
                body = resp.json()
                for key in body.get("data", []) or []:
                    total += 1
                    name = key.get("name", "") or ""
                    group = name.split("_security_")[0] + "_security_*" \
                        if "_security_" in name else name
                    by_name[group] = by_name.get(group, 0) + 1
                    if key.get("id") in keep_ids:
                        continue
                    if name.startswith(self.prefix + "security_") or \
                            name in self.AUTH_TEST_KEY_NAMES:
                        max_age = role_age
                    elif name.startswith(self.prefix + "OrgOwnerKey-"):
                        max_age = owner_age
                    else:
                        continue
                    created = self._api_key_created_epoch(key)
                    if created is not None and now - created > max_age:
                        stale.append((key["id"], name, now - created))
                pages = (body.get("cursor") or {}).get("pages") or {}
                if not pages.get("next") or page >= 100:
                    break
                page += 1
        except Exception as e:
            self.log.warning("API key cleanup ({}): error while listing "
                             "keys: {}".format(reason, e))

        self.log.info("API key cleanup ({}): {} key(s) in org, by name: {}"
                      .format(reason, total, sorted(
                          by_name.items(), key=lambda x: -x[1])[:10]))
        deleted = 0
        for key_id, name, age in stale:
            try:
                resp = self.capellaAPI.org_ops_apis.delete_api_key(
                    self.organisation_id, key_id)
                if resp.status_code == 429:
                    time.sleep(int(resp.headers.get("Retry-After", 60)))
                    resp = self.capellaAPI.org_ops_apis.delete_api_key(
                        self.organisation_id, key_id)
                if resp.status_code in [204, 404]:
                    deleted += 1
                else:
                    self.log.warning(
                        "API key cleanup: could not delete {} ({}): "
                        "status={}".format(name, key_id, resp.status_code))
            except Exception as e:
                self.log.warning("API key cleanup: error deleting {}: {}"
                                 .format(key_id, e))
        self.log.info("API key cleanup ({}): deleted {} of {} stale "
                      "automation key(s)".format(reason, deleted, len(stale)))
        return deleted

    def create_api_key_with_quota_retry(self, *args, **kwargs):
        """
        create_api_key, plus:
        - 429: retried up to 3 times (owner-key swap when the owner keys
          exist, else a plain wait for Retry-After).
        - 5xx: retried up to 3 times with a short backoff. The control
          plane occasionally returns a one-off 500 (code 10000) on key
          creation; failing the TC for it hides an otherwise passing
          run (V4 #29, FlushBucket.test_authorization revoked-key case).
        - org-wide API key limit reached: clean up stale automation keys
          and retry at once; if still full (other builds sharing the org
          are holding keys for their running TCs), keep cleaning up and
          retrying every 60s for up to api_key_quota_wait_mins (def. 20)
          before giving up with the last response.
        """
        wait_secs = float(self.input.param("api_key_quota_wait_mins",
                                           20)) * 60
        deadline = None
        throttled = 0
        server_errors = 0
        while True:
            resp = self.capellaAPI.org_ops_apis.create_api_key(
                *args, **kwargs)
            if resp.status_code >= 500 and server_errors < 3:
                server_errors += 1
                self.log.warning(
                    "API key creation returned {} (attempt {}/3), "
                    "retrying in {}s. request_id={} body={}".format(
                        resp.status_code, server_errors, 10 * server_errors,
                        resp.headers.get(
                            "Vnd-Cloud-Couchbase-Com-Request-Id"),
                        resp.content))
                time.sleep(10 * server_errors)
                continue
            if resp.status_code == 429 and throttled < 3:
                throttled += 1
                retry_after = int(resp.headers.get("Retry-After", 60))
                if getattr(self, "org_owner_key2", None) and \
                        getattr(self, "curr_owner_key", None):
                    self.handle_rate_limit(retry_after)
                else:
                    time.sleep(retry_after)
                continue
            if not self.is_api_key_limit_error(resp):
                return resp
            if deadline is None:
                deadline = time.time() + wait_secs
                self.log.warning("Org API key limit reached, cleaning up "
                                 "stale automation keys and retrying")
                self.cleanup_stale_automation_api_keys("key limit reached")
                continue
            if time.time() >= deadline:
                self.log.error("Org API key limit still reached after "
                               "waiting {:.0f} min".format(wait_secs / 60))
                return resp
            self.log.warning("Org API key limit still reached, waiting 60s "
                             "for keys to be freed ({:.0f}s left)".format(
                                 deadline - time.time()))
            time.sleep(60)
            self.cleanup_stale_automation_api_keys("waiting for key quota")

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

                if resp.status_code == 404:
                    # Already gone (e.g. removed by the stale-key sweep).
                    break
                if resp.status_code != 204:
                    try:
                        body = resp.json()
                        self.log.error(
                            "Error deleting key {}: HTTP {} {} {}".format(
                                api_key_dict[role]["id"], resp.status_code,
                                body.get("errorType", ""),
                                body.get("message", "")))
                    except (Exception,):
                        self.log.error(
                            "Error deleting key {}: HTTP {} {}".format(
                                api_key_dict[role]["id"], resp.status_code,
                                resp.content))
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
        # Passing the code/error positionally used to land them in
        # other_proj/expected_failure_code, turning every role case into a
        # mismatch; catch that mistake here, once, with a clear message.
        if (other_proj is not None and not isinstance(other_proj, bool)) \
                or isinstance(expected_failure_code, dict):
            self.fail("v4_RBAC_injection_init: other_proj must be True/False/"
                      "None and expected_failure_code a status code (got "
                      "{!r}, {!r}); pass expected_failure_code=/"
                      "expected_failure_error= as keywords".format(
                          other_proj, expected_failure_code))
        testcases = []
        for role in self.api_keys:
            if "roles" not in self.api_keys[role]:
                continue
            testcase = {
                "description": "Calling API with {} role".format(role),
                "token": self.api_keys[role]["token"]
            }
            if expected_failure_code:
                testcase["expected_status_code"] = expected_failure_code
                testcase["expected_error"] = expected_failure_error
            # Keys with project roles are created with organizationMember
            # added (see create_api_keys_for_all_combinations_of_roles), so
            # the backend lets them through wherever organizationMember is
            # allowed, even though the key name doesn't say so.
            effective_roles = list(self.api_keys[role]["roles"])
            if (any(r.startswith("project") and r != "projectCreator"
                    for r in effective_roles) and
                    "organizationMember" not in effective_roles):
                effective_roles.append("organizationMember")
            if not any(element in allowed_roles for
                       element in effective_roles):
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
                    "5xx_errors": {},
                    "exceptions": 0
                }
            for i in range(call_batch_per_api):
                try:
                    resp = api_func(*api_args, headers=header)
                except Exception as e:
                    self.log.warning(
                        "API call raised an exception for role {} "
                        "(attempt {}): {}".format(
                            api_role["role"], i, e))
                    results[api_role["id"]]["exceptions"] += 1
                    results[api_role["id"]]["end"] = datetime.now()
                    continue
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
            batches = int(num_of_calls_per_api // batch_size)
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

        # These calls count 429s, so they must see them instead of having
        # capella_http wait them out and retry.
        with capella_http.suspend_429_retries():
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

    def throttle_test(self, api_func_list, multi_key=False, proj_id=None,
                      extra_exclude_codes=None):
        """
        api_func_list: (list(list)) List of lists, where inner list is of format :
            [api_function_call, function_args]
        multi_key: a boolean value which is used to get to know whether the  test is being run with a single key (which has proper access to the resource),  or multiple keys (which might or might not have access to the resource) being tested via throttling.
        proj_id: The UUID of the project that will be used in case of multiple keys throttle testing with different roles.
        extra_exclude_codes: (list of str) Additional HTTP status codes to exclude from failure checking, e.g. ["400", "422"] for create endpoints with external validation constraints.
        """
        exclude_codes = self.input.param("exclude_codes", "")
        if exclude_codes:
            exclude_codes = str(exclude_codes)
            exclude_codes = exclude_codes.split('-')
        else:
            exclude_codes = []
        exclude_codes.append("403")
        if extra_exclude_codes:
            exclude_codes.extend(extra_exclude_codes)

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
                if results[r]["end"] and results[r]["start"]:
                    duration = (results[r]["end"]
                               - results[r]["start"]).total_seconds()
                else:
                    duration = "N/A"
                self.log.info("Parallel API calls for role {} took {} seconds"
                              .format(results[r]["role"], duration))
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
                # No "expired API key" case: the backend no longer accepts
                # sub-minute expiry values (see 5f21eea51), so the key
                # created for it was a regular organizationOwner key valid
                # for 180 days. Its calls succeeded and the 401 mismatch
                # was silently ignored by validate_testcase - and for
                # delete tests it deleted the resource under test as the
                # owner (DeleteAdminUsers/DeleteScope then got 404).
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

    @staticmethod
    def expect_same_response_for_all_keys(testcases, status_code=None,
                                          error=None):
        """
        The backend looks the target resource up before it checks roles, so
        when the resource doesn't exist (dummy cluster ID, CMEK that was
        never created, Azure CMEK app not provisioned) every valid key -
        authorized or not - gets the same 404. Make every case except the
        401 ones expect that response. With status_code=None the
        expectations are dropped, so the caller's success codes apply.
        RBAC can't be verified in this situation.
        """
        for testcase in testcases:
            if testcase.get("expected_status_code") == 401:
                continue
            if status_code is None:
                testcase.pop("expected_status_code", None)
                testcase.pop("expected_error", None)
            else:
                testcase["expected_status_code"] = status_code
                testcase["expected_error"] = error
        return testcases

    def auth_test_setup(self, testcase, failures, header,
                        project_id, other_project_id=None,
                        resource_ready_check=None):
        if "expire_key" in testcase:
            self.update_auth_with_api_token(self.curr_owner_key)
            # create a new API key with expiry of approx 2 mins. Goes
            # through the retry helper so a 429/5xx/quota hit on key
            # creation doesn't fail the whole authorization test.
            resp = self.create_api_key_with_quota_retry(
                self.organisation_id, "Expiry_Key", ["organizationOwner"])
            if resp.status_code == 201:
                self.api_keys["organizationOwner_new"] = resp.json()
            else:
                self.fail(resp.json())
            # wait for key to expire
            self.log.debug("Sleeping 10 seconds for key to expire")
            time.sleep(10)
            self.update_auth_with_api_token(self.api_keys["organizationOwner_new"])
            del self.api_keys["organizationOwner_new"]
        elif "revoke_key" in testcase:
            self.update_auth_with_api_token(self.curr_owner_key)
            # A one-off 500 here failed FlushBucket.test_authorization after
            # every role case had passed (V4 #29), so retry 429/5xx/quota.
            resp = self.create_api_key_with_quota_retry(
                self.organisation_id, "Revoked_Key", ["organizationOwner"])
            if resp.status_code == 201:
                self.api_keys["revoked_key"] = resp.json()
            else:
                self.fail("Error while creating API key for organization "
                          "owner for revoking later: {} request_id={} {}"
                          .format(resp.status_code,
                                  resp.headers.get(
                                      "Vnd-Cloud-Couchbase-Com-Request-Id"),
                                  resp.content))
            resp = self.capellaAPI.org_ops_apis.delete_api_key(
                self.organisation_id, self.api_keys["revoked_key"]["id"])
            if resp.status_code != 204:
                failures.append(testcase["description"])
            self.update_auth_with_api_token(self.api_keys["revoked_key"])
            del self.api_keys["revoked_key"]
            self.wait_for_key_revocation(project_id)
        elif "userpwd" in testcase:
            basic = base64.b64encode("{}:{}".format(
                self.user, self.passwd).encode()).decode()
            header["Authorization"] = 'Basic {}'.format(basic)
        elif "has_multi_project_access" in testcase and other_project_id:
            def build_resources(other_proj):
                res = [{
                    "type": "project",
                    "id": other_proj,
                    "roles": ["projectOwner"]
                }]
                if testcase["has_multi_project_access"]:
                    res.append({
                        "type": "project",
                        "id": project_id,
                        "roles": ["projectOwner"]
                    })
                return res

            org_roles = ["organizationMember"]
            if testcase["has_multi_project_access"]:
                key = "multi_project_1"
            else:
                key = "multi_project_2"
                org_roles.append("projectCreator")

            self.update_auth_with_api_token(self.curr_owner_key)

            # create a new API key with expiry of approx 2 mins. This runs
            # right after the expired/revoked key cases, which also create
            # keys, so it can hit the key-creation rate limit (429); the
            # shared Auth_Project can also have been deleted since setUp.
            refreshed_project = False
            resp = None
            for attempt in range(1, 4):
                resp = self.create_api_key_with_quota_retry(
                    self.organisation_id, "MultiProj_Key", org_roles,
                    expiry=0.5, allowedCIDRs=["0.0.0.0/0"],
                    resources=build_resources(other_project_id))
                if resp.status_code == 201:
                    break
                self.log.warning(
                    "Multi-project API key creation attempt {}/3 failed: "
                    "status={} request_id={} body={}".format(
                        attempt, resp.status_code,
                        resp.headers.get("Vnd-Cloud-Couchbase-Com-Request-Id"),
                        resp.content))
                if resp.status_code == 429:
                    self.handle_rate_limit(
                        int(resp.headers.get("Retry-After", 60)))
                elif (resp.status_code in [400, 404, 422] and
                        not self.is_api_key_limit_error(resp) and
                        not refreshed_project):
                    # other_project_id may be stale - re-resolve it
                    refreshed_project = True
                    other_project_id = self.get_or_create_auth_project()
                    self.other_project_id = other_project_id
                    try:
                        self.capella["tenant_id"]["otherProj"] = \
                            other_project_id
                    except (KeyError, TypeError):
                        pass
                else:
                    break
            if resp.status_code == 201:
                self.api_keys[key] = resp.json()
            else:
                self.fail("Error while creating API key for role having "
                          "access to multiple projects: status={} body={}"
                          .format(resp.status_code, resp.content))
            self.update_auth_with_api_token(self.api_keys[key])
            if testcase["has_multi_project_access"]:
                # This key actually has access to project_id, so the
                # caller will read/modify a resource in it next. Newly
                # created keys can take 15-20+ seconds for their resource
                # scope to propagate; poll a lightweight, read-only call
                # instead of guessing a fixed sleep duration, so we wait
                # exactly as long as needed. Callers that know exactly
                # which resource they're about to act on can also pass
                # resource_ready_check to wait for that resource's own
                # visibility, which can lag independently of project-level
                # authorization (see DeleteAdminUsers.test_authorization).
                self.wait_for_new_key_authorization(
                    project_id, resource_ready_check)
            else:
                # This key intentionally has NO access to project_id and
                # is only used to assert a 403 there, which the
                # propagation lag above doesn't affect - keep the
                # original short wait unchanged.
                self.log.debug("Sleeping 5 seconds for new API key's "
                               "resource scope to propagate")
                time.sleep(5)
        else:
            self.update_auth_with_api_token(testcase)

    def wait_for_key_revocation(self, project_id, timeout=60,
                                poll_interval=3):
        """Poll a read-only call with the currently active (just deleted)
        API key until the API rejects it. The control plane keeps
        accepting a deleted key for a few seconds (200 right after the 204
        delete, 19870), so without this the revoked-key case is a race.
        Returns after `timeout` regardless, so a key that is never revoked
        still fails the test."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            resp = self.capellaAPI.org_ops_apis.fetch_project_info(
                self.organisation_id, project_id)
            if resp.status_code == 401:
                self.log.debug("Revoked API key rejected after {:.1f}s"
                               .format(time.time() - start_time))
                return True
            time.sleep(poll_interval)
        self.log.warning("Revoked API key still accepted after {}s"
                         .format(timeout))
        return False

    def wait_for_new_key_authorization(self, project_id, extra_check=None,
                                       timeout=90, poll_interval=3):
        """
        Poll a read-only project-info call (and, if given, an additional
        caller-supplied resource_ready_check) using the currently active
        API key (set via update_auth_with_api_token) until both succeed,
        to wait out the backend's resource-scope propagation lag observed
        for freshly-created, resource-scoped API keys. `extra_check` must
        be a zero-arg callable returning True once the specific resource
        the caller is about to act on is visible to this key - project-
        level authorization and a specific resource's own visibility have
        been observed to propagate on independent timelines. Falls back
        to returning after `timeout` seconds regardless, so callers
        relying on this to avoid false failures should still expect a
        genuine failure to surface if the backend never becomes ready.
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            resp = self.capellaAPI.org_ops_apis.fetch_project_info(
                self.organisation_id, project_id)
            if resp.status_code == 200:
                ready = True
                if extra_check is not None:
                    try:
                        ready = extra_check()
                    except Exception as e:
                        self.log.debug(
                            "resource_ready_check raised while polling "
                            "for resource visibility: {}".format(e))
                        ready = False
                if ready:
                    self.log.debug(
                        "New API key's resource scope propagated after "
                        "{:.1f}s".format(time.time() - start_time))
                    return True
            time.sleep(poll_interval)
        self.log.warning(
            "New API key's resource scope did not propagate within {}s"
            .format(timeout))
        return False

    def validate_testcase(self, result, success_codes, testcase, failures,
                          validate_response=False, expected_res=None,
                          resource_id=None, payloadTest=False):
        # Parser for payload tests.
        testDescriptionKey = "description"
        if payloadTest:
            testDescriptionKey = "desc"
        description = testcase.get(testDescriptionKey,
                                   testcase.get("description",
                                                testcase.get("desc")))
        # One line per testcase, so a failure can always be tied back to
        # the role/case that caused it (the failure lines below don't
        # otherwise say which case they belong to).
        self.log.info("[validate] '{}' -> HTTP {} (expected {}) "
                      "request_id={}".format(
                          description, result.status_code,
                          testcase.get("expected_status_code", success_codes),
                          result.headers.get(
                              "Vnd-Cloud-Couchbase-Com-Request-Id")))

        # The shared cluster died under the run: every later call would get
        # this same 422, so release it and stop the run (NOT RUN) now. Only
        # for the shared cluster - a test's own cluster (FreeTier, security)
        # failing to deploy must not end the run.
        if result.status_code == 422 and "Deployment Failed state" in str(
                getattr(result, "content", "")) and \
                self._targets_shared_cluster(result):
            self._abort_shared_setup(
                "shared cluster is in the Deployment Failed state ('{}')"
                .format(description), collect_logs=False)

        # Malformed-path cases (a bad URL segment, or a non-hex ID in the
        # path) check that the API rejects the URL. Which rejection code the
        # router picks (400 bad id / 404 no route / 405) moves when routes
        # are added - the same GET got 404 and 400 minutes apart in one run.
        # When the code differs from the test's but is another rejection
        # code, accept it with a warning (the error body can't match a
        # different code). When it matches, the normal checks below run,
        # error body included. A 2xx/5xx always fails.
        # strict_routing_codes=True turns this off.
        if self._is_malformed_path_case(description, testcase) and \
                result.status_code != testcase["expected_status_code"] and \
                result.status_code in (
                    (400, 404) if "non-hex" in str(description).lower()
                    else self.ROUTING_REJECTION_CODES):
            self.log.warning("[validate] '{}': HTTP {} accepted as a "
                             "rejection of the malformed path (test lists "
                             "{})".format(description, result.status_code,
                                          testcase["expected_status_code"]))
            return False

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
            expected_codes = testcase["expected_status_code"]
            if not isinstance(expected_codes, list):
                expected_codes = [expected_codes]
            if result.status_code not in expected_codes:
                # Fall through to the checks below, which record the
                # mismatch as a failure. (This used to return False
                # silently, so e.g. a role that should get 403 but got a
                # 2xx/404 passed without any log line.) The old lenient
                # behaviour can be restored with strict_status_check=False.
                if not self.input.param("strict_status_check", True):
                    self.log.warning(
                        "'{}': expected HTTP {}, got {} - ignored "
                        "(strict_status_check=False)".format(
                            description, expected_codes,
                            result.status_code))
                    return False
            elif isinstance(testcase["expected_status_code"], list):
                self.log.debug("The test expected one of the following "
                               "error responses to the user: {}".format(
                                   testcase["expected_error"]))
                return False
            else:
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

        # Error bodies aren't always JSON (e.g. an empty 401/502).
        try:
            resp_message = result.json().get("message", "") or ""
        except Exception:
            resp_message = ""

        if result.status_code in success_codes:
            if ("expected_error" in testcase and
                    testcase["expected_status_code"] != 404):
                self.log.error("'{}': NO ERRORS in Response (HTTP {}), But "
                               "Test expected HTTP {} with error: {}".format(
                                   description, result.status_code,
                                   testcase["expected_status_code"],
                                   testcase["expected_error"]))
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
        elif result.status_code >= 500 and resp_message != "Private endpoints aren't enabled for this cluster. Please select another cluster.":
            self.log.critical(testcase[testDescriptionKey])
            self.log.warning(result.content)
            self.log.warning("Request ID (for backend log lookup): {}".format(
                result.headers.get("Vnd-Cloud-Couchbase-Com-Request-Id")))
            failures.append(testcase[testDescriptionKey])
        elif "expected_status_code" not in testcase:
            self.log.error("'{}': Expected NO ERRORS but got {}".format(
                description, result))
            self.log.error(result.content)
            failures.append(testcase[testDescriptionKey])
        elif resp_message == "Private endpoints aren't enabled for this cluster. Please select another cluster.":
            return True
        elif result.status_code == 400 and "Bad Request" in resp_message:
            return True
        else:
            self.log.error("'{}': Expected HTTP status code {}, Actual HTTP "
                           "status code {}".format(
                               description, testcase["expected_status_code"],
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
        elif inst:
            res = self.columnarAPI.fetch_analytics_cluster_info(
                self.organisation_id, self.project_id, inst)
        elif free_tier and not app:
            res = (self.capellaAPI.cluster_ops_apis.
                   fetch_free_tier_cluster_info(
                    self.organisation_id, self.project_id, free_tier))
        elif free_tier and app:
            res = (self.capellaAPI.cluster_ops_apis.
                   fetch_free_tier_app_service_info(
                    self.organisation_id, self.project_id, free_tier, app))
        else:
            res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                self.organisation_id, self.project_id, self.cluster_id)
            if res.status_code == 429:
                self.handle_rate_limit(int(res.headers.get("Retry-After", 60)))
                res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                    self.organisation_id, self.project_id, self.cluster_id)
            elif res.status_code == 404:
                self.log.warning("The cluster: {} does not exist".format(
                    self.cluster_id))
                return True, None

        if res.status_code != 200:
            # Every caller polls in a time-bounded loop, so report "not yet"
            # instead of crashing on res.json()['currentState'] below.
            self.log.warning("Could not fetch on/off state (HTTP {}), will "
                             "poll again: {}".format(res.status_code,
                                                     res.content))
            return False, None

        if res.json()['currentState'] in states:
            self.log.info("Resource state: {}, Expected States: {}"
                          .format(res.json()["currentState"], states))
            if app:
                return True, None
            return True, res.json()['cloudProvider']['cidr']

        # deploymentFailed never recovers, so stop now rather than poll every test until the build times out.
        if res.json()['currentState'] == "deploymentFailed":
            self.fail("!!!...Resource is in deploymentFailed and will not "
                      "reach {}...!!!".format(states))

        self.log.warning("Current State: '{}', Expected States: '{}'"
                         .format(res.json()["currentState"], states))
        return False, None

    def validate_api_response(self, expected_res, actual_res, id):
        for key in actual_res:
            if key == "clusterStats":
                continue
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
                        elif "auditLogExportId" in actual_res[key][i] and \
                                actual_res[key][i]["auditLogExportId"] != id:
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

        start_time = time.time()
        while time.time() - start_time < 1800:
            if serviceGroups is None:
                result = (self.capellaAPI.cluster_ops_apis.
                          create_free_tier_cluster(
                            org, proj, name, cloudProvider))
            else:
                result = self.capellaAPI.cluster_ops_apis.create_cluster(
                    org, proj, name, cloudProvider, couchbaseServer,
                    serviceGroups, availability, support, header, **kwargs)
            if result.status_code == 202:
                return result
            try:
                r = result.json()
                if any(i in r["message"] for i in self.DUPLICATE_CIDR_MESSAGES):
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
                            inst_id=None, pes=False, pes_enabled=True):
        start_time = time.time()
        while start_time + 1800 > time.time():
            self.log.info("...Waiting further...")
            time.sleep(5)

            if app_svc_id and clus_id:
                state = self.capellaAPI.cluster_ops_apis.get_appservice(
                    self.organisation_id, self.project_id, clus_id,
                    app_svc_id)
            elif app_svc_id:
                state = self.capellaAPI.cluster_ops_apis.get_appservice(
                    self.organisation_id, self.project_id, self.cluster_id,
                    app_svc_id)
            elif inst_id:
                state = self.columnarAPI.fetch_analytics_cluster_info(
                    self.organisation_id, self.project_id, inst_id)
            elif pes:
                state = self.capellaAPI.cluster_ops_apis \
                    .fetch_private_endpoint_service_status_info(
                        self.organisation_id, self.project_id, self.cluster_id)
            elif clus_id:
                state = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                    self.organisation_id, self.project_id, clus_id)
            else:
                state = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                    self.organisation_id, self.project_id, self.cluster_id)

            if self.is_transient_status(state.status_code):
                # 429 left over after capella_http's retries, or a 5xx:
                # keep polling inside the 30 min budget instead of
                # declaring the deployment failed on one bad poll.
                self.log.warning("Transient HTTP {} while polling, will retry."
                                 " Result: {}".format(state.status_code,
                                                      state.content))
                continue
            if state.status_code >= 400:
                self.log.error("Something went wrong while fetching details."
                               "\nResult: {}".format(state.content))
                return False
            if pes:
                if state.json()["enabled"] == pes_enabled:
                    return True
                continue

            self.log.info("Current state: {}"
                          .format(state.json()["currentState"]))

            if state.json()["currentState"] == "deploymentFailed":
                self.log.error("!!!Deployment Failed!!!")
                self.log.error(state.content)
                # The run's shared cluster can't recover from this: release
                # it and stop the rest of the run from using it.
                shared = (self.capella.get("clusters") or {}) \
                    if isinstance(self.capella.get("clusters"), dict) else {}
                polled = clus_id or self.cluster_id
                if not (app_svc_id or inst_id or pes) and polled and \
                        polled == shared.get("cluster_id"):
                    self._abort_shared_setup(
                        "shared cluster {} is deploymentFailed".format(
                            polled), collect_logs=False)
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

    def collect_cluster_logs(self, cluster_id, timeout=1200, poll_interval=10,
                             max_poll_errors=6):
        """Collects dp-agent and Couchbase Server logs for a cluster when collect_logs=True; best effort, never fails the test."""
        if not self.input.param("collect_logs", False) or not cluster_id:
            return
        token = self.internal_support_token()
        if not token:
            for log_type in ("dp-agent", "server"):
                self._record_collected_log(cluster_id, "before destroy", log_type, "", "FAILED", "",
                                           "no internal support token")
            self.log.warning("collect_logs is set but no internal support "
                             "token was found, skipping log collection")
            return

        base = self.internal_support_url(
            "/logcollections/clusters/{}".format(cluster_id))
        headers = {"Authorization": "Bearer {}".format(token),
                   "Content-Type": "application/json"}
        step = "before destroy"

        def describe(resp):
            return "no response" if resp is None else "HTTP {}: {}".format(resp.status_code, resp.content)

        # dp-agent logs: one zip per cluster on Supportal, collected in parallel with server logs as cp-cli does.
        def collect_agent_logs():
            resp = self.capellaAPI._urllib_request(
                base + "/agents", "POST", params=json.dumps({}), headers=headers,
                timeout=timeout)
            if resp is None or resp.status_code not in (200, 201):
                self._record_collected_log(cluster_id, step, "dp-agent", "", "FAILED", "", describe(resp))
                self.log.warning("Failed to collect dp-agent logs for cluster {}: {}".format(cluster_id, describe(resp)))
                return
            body = resp.json()
            location = "s3://{}/{}".format(body.get("bucket"), body.get("key"))
            for n in body.get("collected", []):
                self._record_collected_log(cluster_id, step, "dp-agent", n.get("nodeId"), "OK", location)
            for n in body.get("failed", []):
                self._record_collected_log(cluster_id, step, "dp-agent", n.get("nodeId"), "FAILED", "", n.get("error"))
            if not body.get("collected") and not body.get("failed"):
                self._record_collected_log(cluster_id, step, "dp-agent", "", "SKIPPED", "", "cluster has no nodes")
            self.log.info("dp-agent logs for cluster {}: {} (collected: {}, failed: {})".format(
                cluster_id, location, [n.get("nodeId") for n in body.get("collected", [])], body.get("failed", [])))

        def get_tasks():
            return self.capellaAPI._urllib_request(base + "/tasks", "GET", headers=headers)

        # Couchbase Server logs: trigger cbcollect, then poll until every node has uploaded.
        def collect_server_logs():
            resp = self.capellaAPI._urllib_request(
                base, "POST", params=json.dumps({}), headers=headers)
            if resp is None or resp.status_code != 201:
                # Server runs one collection at a time, so a failed trigger may mean one is already running; attach to it.
                existing = get_tasks()
                running = existing is not None and existing.status_code == 200 and existing.json()
                if not running:
                    self._record_collected_log(cluster_id, step, "server", "", "FAILED", "",
                                               "trigger failed: " + describe(resp))
                    self.log.warning("Failed to trigger server log collection for cluster {}: {}".format(
                        cluster_id, describe(resp)))
                    return
            # Each node is judged on its own: uploaded is OK with its link whatever happened overall, anything else FAILED with its last status.
            per_node = {}

            def node_rows(reason):
                for node, info in per_node.items():
                    uploaded = info.get("status") == "uploaded" and info.get("url")
                    self._record_collected_log(cluster_id, step, "server", node, "OK" if uploaded else "FAILED",
                                               info.get("url") if uploaded else "",
                                               "" if uploaded else 'node status "{}": {}'.format(info.get("status"), reason))
                    self.log.info("Server logs for cluster {} node {}: {} ({})".format(
                        cluster_id, node, info.get("url"), info.get("status")))
                if not per_node:
                    self._record_collected_log(cluster_id, step, "server", "", "FAILED", "", reason)

            end_time = time.time() + timeout
            poll_errors = 0
            while True:
                resp = get_tasks()
                ok = resp is not None and resp.status_code == 200
                poll_errors = 0 if ok else poll_errors + 1
                tasks = resp.json() if ok else []
                task = tasks[0] if tasks else {}
                # The API returns "PerNode"; accept either casing.
                per_node = task.get("PerNode") or task.get("perNode") or per_node
                if task.get("status") in ("completed", "failed", "cancelled"):
                    node_rows("collection {}".format(task.get("status")))
                    return
                if poll_errors >= max_poll_errors:
                    node_rows("giving up after {} failed status checks: {}".format(poll_errors, describe(resp)))
                    self.log.warning("Giving up on server log collection for cluster {}".format(cluster_id))
                    return
                if time.time() >= end_time:
                    break
                time.sleep(poll_interval)
            node_rows("timed out after {} minutes".format(timeout // 60))
            self.log.warning("Timed out waiting for server log collection on cluster {}".format(cluster_id))

        def safe(fn, log_type):
            try:
                fn()
            except Exception as e:
                self._record_collected_log(cluster_id, step, log_type, "", "FAILED", "", "unexpected error: {}".format(e))

        agent_thread = threading.Thread(target=safe, args=(collect_agent_logs, "dp-agent"))
        agent_thread.start()
        safe(collect_server_logs, "server")
        agent_thread.join(timeout + 60)

    def skip_cluster_logs(self, cluster_id, reason):
        """Lists a cluster with nothing to collect as SKIPPED with the reason when collect_logs=True."""
        if not self.input.param("collect_logs", False) or not cluster_id:
            return
        for log_type in ("dp-agent", "server"):
            self._record_collected_log(cluster_id, "before destroy", log_type, "", "SKIPPED", "", reason)
        self.log.info("Skipping log collection for cluster {}: {}".format(cluster_id, reason))

    def internal_support_token(self):
        """Token for the /internal/support API: test param, [capella] section, then the executor's env vars."""
        # Not [capella] override_token: pipelines pass the test override key there.
        return self.input.param("internal_support_token", None) or \
            self.input.capella.get("internal_support_token") or \
            os.environ.get("TOKEN_FOR_INTERNAL_SUPPORT") or \
            os.environ.get(self._internal_support_token_env())

    def internal_support_url(self, path):
        """The pod's /internal/support URL for path; the support API lives on api.<env>, not cloudapi.<env>."""
        return ("https://" + self.url).replace("https://cloud", "https://", 1) + \
            "/internal/support" + path

    def _internal_support_token_env(self):
        """Name of the per-environment token variable the TAF Jenkins executors export, chosen from the pod URL."""
        pod = (self.url or "").lower()
        if "sandbox" in pod or ".sbx-" in pod or ".qe-" in pod:
            return "sbx_token_for_internal_support"
        if ".dev." in pod:
            return "dev_token_for_internal_support"
        if "stage" in pod:
            return "stage_token_for_internal_support"
        return "token_for_internal_support"

    def _collected_logs_file_name(self):
        """Names the log list after the v4 suite (the executor's subcomponent), else the conf file, e.g. collected_logs_v4api_gcp.log."""
        suite = os.environ.get("subcomponent") or \
            os.path.splitext(os.path.basename(self.input.param("conf_file", "") or ""))[0]
        name = "collected_logs_" + suite if suite else "collected_logs"
        return re.sub(r"[^A-Za-z0-9._-]+", "_", name) + ".log"

    def _record_collected_log(self, cluster_id, step, log_type, node, status, location, error=""):
        """Appends one row to logs/collected_logs_<suite>.log (top of the build artifacts), the same columns as cp-cli's list."""
        logs_folder = self.input.param("logs_folder", None)
        # logs_folder is logs/testrunner-<run>/test_<n>; the list sits in logs/ itself.
        logs_dir = os.path.dirname(os.path.dirname(logs_folder)) if logs_folder else "logs"
        path = os.path.join(logs_dir, self._collected_logs_file_name())
        test_name = "{}.{}".format(self.__class__.__name__, getattr(self, "_testMethodName", ""))
        row = [time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), test_name, "", cluster_id, step,
               log_type, node or "", status, location or "", error or ""]
        try:
            with _collected_logs_lock:
                new = not os.path.exists(path)
                with open(path, "a") as f:
                    if new:
                        f.write("Time,Scenario,Cluster Name,Cluster ID,Step,Log Type,Node,Status,Location,Error\n")
                    f.write(",".join('"{}"'.format(str(v).replace('"', '""')) for v in row) + "\n")
        except Exception as e:
            self.log.warning("Could not write {}: {}".format(path, e))

    @staticmethod
    def is_transient_status(status_code):
        """429 or 5xx: worth polling/retrying again rather than failing."""
        return status_code == 429 or status_code >= 500

    def _wait_for_instances_deletion(self, instances, timeout=1800):
        """
        Poll until every columnar instance in `instances` returns 404,
        removing each from the collection as it goes. Bounded by `timeout`
        (the old loop could spin forever on one instance).
        """
        end_time = time.time() + timeout
        while instances and time.time() < end_time:
            for instance in list(instances):
                res = self.columnarAPI.fetch_analytics_cluster_info(
                    self.organisation_id, self.project_id, instance)
                if res.status_code == 404:
                    self.log.info("Instance {} deleted".format(instance))
                    instances.remove(instance)
                elif self.is_transient_status(res.status_code) or \
                        res.status_code == 200:
                    self.log.debug("Instance {} still present (HTTP {})"
                                   .format(instance, res.status_code))
                else:
                    self.log.warning("Unexpected HTTP {} while waiting for "
                                     "instance {} deletion: {}".format(
                                         res.status_code, instance,
                                         res.content))
            if instances:
                time.sleep(15)
        if instances:
            self.log.error("!!!...Instances not deleted within {}s: {}...!!!"
                           .format(timeout, list(instances)))
            return False
        self.log.info("All instances deleted successfully.")
        return True

    def wait_for_deletion(self, clus_id=None, app_svc_id=None, instances=None):
        if instances:
            return self._wait_for_instances_deletion(instances)
        start_time = time.time()
        while start_time + 1800 > time.time():
            time.sleep(15)

            if app_svc_id:
                res = self.capellaAPI.cluster_ops_apis.get_appservice(
                    self.organisation_id, self.project_id, clus_id, app_svc_id)
            else:
                res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                    self.organisation_id, self.project_id, clus_id)

            if res.status_code == 404:
                try:
                    body = res.json()
                    if "code" in body:
                        if app_svc_id and body["code"] == 404:
                            return True
                        elif not app_svc_id and body["code"] == 4025:
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
            elif self.is_transient_status(res.status_code):
                self.log.warning("Transient HTTP {} while waiting for "
                                 "deletion, will retry: {}".format(
                                     res.status_code, res.content))
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

    def find_project_by_name(self, org_id, name):
        """
        Returns the ID of the oldest project named `name` in the given
        organization, or None if there is none. Suites running in parallel
        can each create one before seeing the other's, so always picking
        the oldest makes them all converge on the same project.
        """
        page = 1
        matches = []
        while True:
            resp = self.capellaAPI.org_ops_apis.list_projects(
                org_id, page=page, perPage=100)
            if resp.status_code != 200:
                self.log.error("Error while listing projects: {}"
                              .format(resp.content))
                return None
            body = resp.json()
            for project in body.get("data", []):
                if project.get("name") == name:
                    matches.append(project)
            if not body.get("cursor", {}).get("pages", {}).get("next"):
                break
            page += 1
        if not matches:
            return None
        if len(matches) > 1:
            self.log.warning("{} projects named {}; using the oldest".format(
                len(matches), name))
        matches.sort(key=lambda p: (p.get("audit") or {}).get(
            "createdAt") or "")
        return matches[0].get("id")

    def delete_projects(self, org_id, project_ids, token):
        project_deletion_failed = False

        self.update_auth_with_api_token(token)
        for project_id in project_ids:
            self.log.debug("Deleting Project: {}".format(project_id))
            # Right after a cluster is destroyed the control plane still
            # counts it for a few minutes and answers 400 "Unable to delete
            # a Project with Cluster resources ... retry in a few minutes".
            for attempt in range(1, 11):
                resp = self.capellaAPI.org_ops_apis.delete_project(
                    org_id, project_id)
                if resp.status_code in (204, 404):
                    break
                if resp.status_code == 400 and \
                        "retry in a few minutes" in str(resp.content) and \
                        attempt < 10:
                    self.log.info("Project {} still has cluster resources, "
                                  "retrying delete in 30s (attempt {}/10)"
                                  .format(project_id, attempt))
                    time.sleep(30)
                    continue
                self.log.error("Error: {}".format(resp.content))
                project_deletion_failed = True
                break
        return project_deletion_failed

    def create_alert_to_be_tested(self, proj_id, kind, name, config):
        self.update_auth_with_api_token(self.curr_owner_key)
        _rate_limit_markers = ("rate_limited", "rate limit exceeded")
        _max_retries = 5
        _retry_wait = 60
        for attempt in range(1, _max_retries + 1):
            res = self.capellaAPI.cluster_ops_apis.create_alert(
                self.organisation_id, proj_id, kind, name, config)
            if res.status_code == 429:
                self.handle_rate_limit(int(res.headers.get("Retry-After", 60)))
                continue
            if res.status_code == 201:
                alert_id = res.json()['id']
                self.log.info("New alert ID: {}".format(alert_id))
                return alert_id
            content = res.content if isinstance(res.content, str) \
                else res.content.decode("utf-8", errors="replace")
            if attempt < _max_retries:
                if res.status_code == 500:
                    self.log.warning(
                        "create_alert_to_be_tested got 500 "
                        "(attempt {}/{}); retrying in 30s".format(
                            attempt, _max_retries))
                    time.sleep(30)
                    continue
                if res.status_code == 400 and any(
                        m in content for m in _rate_limit_markers):
                    self.log.warning(
                        "create_alert_to_be_tested rate-limited "
                        "(attempt {}/{}); retrying in {}s".format(
                            attempt, _max_retries, _retry_wait))
                    time.sleep(_retry_wait)
                    continue
            self.log.error(res.content)
            self.fail("!!!...New alert creation failed...!!!")

    def create_bucket_to_be_tested(self, org_id, proj_id, clus_id,
                                   buck_name, buckets):
        # Wait for cluster to rebalance (if it is).
        self.update_auth_with_api_token(self.curr_owner_key)
        res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
            self.organisation_id, proj_id, clus_id)
        while res.json()["currentState"] != "healthy":
            self.log.warning("Waiting for cluster to rebalance.")
            time.sleep(10)
            res = self.capellaAPI.cluster_ops_apis.fetch_cluster_info(
                self.organisation_id, proj_id, clus_id)
        self.log.debug("Cluster state healthy.")

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
            if resp.status_code != 204:
                self.log.error("Error: {}".format(resp.content))

            # Wait for cluster to rebalance (if it is).
            self.wait_for_deployment(self.cluster_id)
            self.log.debug("Cluster state healthy.")

    def create_scope_to_be_tested(self, org_id, proj_id, clus_id, buck_id):
        self.update_auth_with_api_token(self.curr_owner_key)

        # Use a fresh name on every call. Recreating a scope with the same
        # name right after deleting it races with the backend's async drop
        # of the old scope, which can silently remove the new one too
        # (create returns 201, later fetch/delete return 404 Scope Not Found).
        name = self.prefix + "Scope_Delete_" + self.generate_random_string(
            length=6, special_characters=False)
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
        if res.status_code == 201:
            self.log.debug("New collection Name: {}".format(name))
            return name
        self.log.error(res.content)
        self.fail("!!!...Collection creation unsuccessful...!!!")

    def flush_alerts(self, project_id, alerts):
        self.update_auth_with_api_token(self.curr_owner_key)

        alert_deletion_failed = False
        for alert in list(alerts):
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
        # Iterate over a copy: removing from the list being iterated
        # skipped every other scope, leaking them into later tests.
        for scope in list(scopes):
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
        # Iterate over a copy (see flush_scopes).
        for collection in list(collections):
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
        if res.status_code != 200:
            self.log.error("Err: {}".format(res.content))
            self.tearDown()
            self.fail("!!!...Error while listing buckets...!!!")
        for buck in res.json()["data"]:
            bkt = buck["id"]
            res = self.capellaAPI.cluster_ops_apis.delete_free_tier_bucket(
                self.organisation_id, self.project_id, clus, bkt)
            if res.status_code != 204:
                self.log.error(res.content)
                self.log.error("Failed while deleting bucket: {}".format(bkt))
            self.log.debug("Bucket: {} deleted successfully".format(bkt))

    def create_freetier_bucket_to_be_tested(self, clus, name, mem):
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
        if res.status_code != 201:
            self.log.error("Result: {}".format(res.content))
            self.tearDown()
            self.fail("Error while creating User.")
        return res.json()["id"]

    def delete_user(self, user_id):
        self.log.info("Deleting a User for next test in the queue.")
        res = self.capellaAPI.cluster_ops_apis.delete_user(
            self.organisation_id, user_id)
        if res.status_code != 204:
            self.log.error("Result: {}".format(res.content))
            self.tearDown()
            self.fail("Error while deleting User.")
        self.log.info("User deleted successfully.")
