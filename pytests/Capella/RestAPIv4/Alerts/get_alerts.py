"""
Created on February 23, 2024

@author: Vipul Bhardwaj
"""

import time

from pytests.Capella.RestAPIv4.Projects.get_projects import GetProject

class GetAlert(GetProject):

    # Shared QE creds bucket: webhook/Slack/Teams secrets aren't allowed in
    # the .ini file, so they live in a doc on the internal server pool instead.
    _CREDS_POOL_HOST = "172.23.217.21"
    _CREDS_BUCKET = "creds_for_tests"
    _CREDS_DOC_ID = "slack and teams integration links"
    _alert_creds_cache = None

    @classmethod
    def _fetch_alert_creds(cls):
        if GetAlert._alert_creds_cache is not None:
            return GetAlert._alert_creds_cache

        from java.time import Duration
        from com.couchbase.client.java import Cluster, ClusterOptions
        from com.couchbase.client.java.env import ClusterEnvironment
        from com.couchbase.client.core.env import TimeoutConfig, IoConfig

        creds = {}
        cluster = None
        try:
            cluster_env = ClusterEnvironment.builder().ioConfig(
                IoConfig.numKvConnections(1)).timeoutConfig(
                TimeoutConfig.builder()
                .connectTimeout(Duration.ofSeconds(20))
                .kvTimeout(Duration.ofSeconds(10)))
            cluster_options = ClusterOptions.clusterOptions(
                "Administrator", "esabhcuoc").environment(
                cluster_env.build())
            cluster = Cluster.connect(cls._CREDS_POOL_HOST, cluster_options)
            bucket = cluster.bucket(cls._CREDS_BUCKET)
            bucket.waitUntilReady(Duration.ofSeconds(10))
            content = bucket.defaultCollection().get(
                cls._CREDS_DOC_ID).contentAsObject()
            for key in ("slack_bot_token", "slack_webhook_url",
                        "teams_webhook_url", "serviceNowUrl",
                        "serviceNowPassword"):
                creds[key] = content.getString(key)
        except Exception:
            creds = {}
        finally:
            if cluster is not None:
                cluster.disconnect()

        GetAlert._alert_creds_cache = creds
        return creds

    # Config templates keyed by alert_kind test param.
    # Each entry has "api_kind" (sent to Capella) and "config" (request body).
    # Pass alert_kind=<key> as a test param to select which one is used.
    # webhook/Slack/Teams secrets all come from _fetch_alert_creds.
    @property
    def _KIND_CONFIGS(self):
        alert_creds = self._fetch_alert_creds()
        return {
            "webhook": {
                "api_kind": "webhook",
                "config": {
                    "webhook": {
                        "basicAuth": {
                            "user": "capella-webhook-uitest-user",
                            "password": alert_creds.get(
                                "serviceNowPassword",
                                "<PLACEHOLDER_PASSWORD>")
                        },
                        "method": "POST",
                        "url": alert_creds.get(
                            "serviceNowUrl",
                            "https://placeholder.service-now.com/api/"
                            "1325623/capella_webhook_ui_test")
                    }
                }
            },
            "slack": {
                "api_kind": "slack",
                "config": {
                    "slack": {
                        "botToken": alert_creds.get(
                            "slack_bot_token", "xoxb-PLACEHOLDER-TOKEN"),
                        "channel": "#capella-alerts-pipeline",
                        "clusterChannelMappings": {}
                    }
                }
            },
            "slack_webhook": {
                "api_kind": "slack",
                "config": {
                    "slack": {
                        "channelWebhookUrlMappings": {
                            "#capella-alerts-pipeline": alert_creds.get(
                                "slack_webhook_url",
                                "https://hooks.slack.com/services/"
                                "PLACEHOLDER/PLACEHOLDER/PLACEHOLDER")
                        },
                        "clusterWebhookChannelMappings": {}
                    }
                }
            },
            "teams": {
                "api_kind": "teams",
                "config": {
                    "teams": {
                        "webhookUrlMappings": {
                            "default": alert_creds.get(
                                "teams_webhook_url",
                                "https://placeholder.environment.api."
                                "powerplatform.com/powerautomate/"
                                "automations/direct/workflows/PLACEHOLDER/"
                                "triggers/manual/paths/invoke?api-version=1"
                                "&sig=PLACEHOLDER")
                        },
                        "clusterWebhookMappings": {}
                    }
                }
            }
        }

    def _init_expected_res(self, alert_kind, nomenclature):
        kind_cfg = self._KIND_CONFIGS[alert_kind]
        self.expected_res = {
            "enabled": False,
            "status": "healthy",
            "audit": {
                "createdAt": None,
                "createdBy": None,
                "modifiedAt": None,
                "modifiedBy": None,
                "version": None
            },
            "config": kind_cfg["config"],
            "kind": kind_cfg["api_kind"],
            "name": self.prefix + nomenclature,
            "projectId": self.project_id,
            "tenantId": self.organisation_id
        }

    def setUp(self, nomenclature="Alerts_Get"):
        self.alerts = []
        GetProject.setUp(self, nomenclature)

        alert_kind = self.input.param("alert_kind", "webhook")
        if alert_kind not in self._KIND_CONFIGS:
            self.fail("Unsupported alert_kind '{}'. Choose from: {}".format(
                alert_kind, list(self._KIND_CONFIGS.keys())))

        self._init_expected_res(alert_kind, nomenclature)

        # Teams alert requires at least one cluster mapping. Use the cluster
        # deployed by setUp (self.cluster_id); fail clearly if none exists.
        if (self.expected_res["kind"] == "teams" and
                not self.expected_res["config"]["teams"].get(
                    "clusterWebhookMappings")):
            if self.cluster_id:
                self.expected_res["config"]["teams"][
                    "clusterWebhookMappings"] = {
                    self.cluster_id: "default"
                }
            else:
                self.log.error(
                    "Teams alert kind requires a cluster. Re-run without "
                    "skip_cluster=True, or use alert_kind=slack for "
                    "cluster-less testing.")
                self.tearDown()
                self.fail("Error while creating Alert.")

        # Clean up stale alerts from previous runs (exact name and
        # unique-suffix variants) before creating the fixture alert.
        stale = self.capellaAPI.cluster_ops_apis.list_alerts(
            self.organisation_id, self.project_id)
        if stale.status_code == 200:
            for alert in stale.json().get("data", []):
                if alert.get("name", "").startswith(self.expected_res["name"]):
                    self.capellaAPI.cluster_ops_apis.delete_alert(
                        self.organisation_id, self.project_id, alert["id"])
                    self.log.info("Deleted stale alert: {}".format(
                        alert["id"]))

        self.log.info("Creating Alert of Kind: {}"
                      .format(self.expected_res["kind"]))
        _rate_limit_markers = ("rate_limited", "rate limit exceeded")
        _max_retries = 5
        _retry_wait = 60
        res = None
        for attempt in range(1, _max_retries + 1):
            res = self.capellaAPI.cluster_ops_apis.create_alert(
                self.organisation_id, self.project_id,
                self.expected_res["kind"], self.expected_res["config"],
                self.expected_res["name"])
            if res.status_code == 201:
                break
            content = res.content if isinstance(res.content, str) \
                else res.content.decode("utf-8", errors="replace")
            if attempt < _max_retries:
                if res.status_code == 500:
                    self.log.warning(
                        "create_alert got 500 (attempt {}/{}); "
                        "retrying in 30s".format(attempt, _max_retries))
                    time.sleep(30)
                    continue
                if res.status_code == 400 and any(
                        m in content for m in _rate_limit_markers):
                    self.log.warning(
                        "create_alert rate-limited (attempt {}/{}); "
                        "retrying in {}s".format(
                            attempt, _max_retries, _retry_wait))
                    time.sleep(_retry_wait)
                    continue
            self.log.error("Result: {}".format(res.content))
            self.tearDown()
            self.fail("Error while creating Alert.")
        self.log.info("Alert created successfully.")
        self.alert_id = res.json()["id"]
        self.alerts = [self.alert_id]

        # Set Parameters for the alert to be validated in the get req
        self.expected_res["id"] = self.alert_id
        self.expected_res["configKey"] = self.alert_id + "-alert-integration"

    def tearDown(self):
        self.update_auth_with_api_token(self.curr_owner_key)

        # Delete the Alert.
        self.log.info("Deleting the Alert")
        if self.flush_alerts(self.project_id, self.alerts):
            self.log.error("Alert deletion failed")
        else:
            self.log.info("Successfully deleted the Alert.")

        super(GetAlert, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Send call with valid path params"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/alertIntegrations",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title></head><body><center><h1>404NotFound</h1></center><hr><center>nginx</center></body></html>"
            }, {
                "description": "Replace the last path param name in URI",
                "url": "/v4/organizations/{}/projects/{}/alertIntegration",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/alertIntegrations"
                       "/alert",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Send call with non-hex organizationID",
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
                "description": "Send call with non-hex projectID",
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
                "description": "Send call with non-hex alertID",
                "invalid_alertID": self.replace_last_character(
                    self.alert_id, non_hex=True),
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
            alert = self.alert_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.alerts_endpoint = \
                    testcase["url"]
            if "invalid_organizationID" in testcase:
                org = testcase["invalid_organizationID"]
            elif "invalid_projectID" in testcase:
                proj = testcase["invalid_projectID"]
            elif "invalid_alertID" in testcase:
                alert = testcase["invalid_alertID"]

            result = self.capellaAPI.cluster_ops_apis.fetch_alert_info(
                org, proj, alert)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.fetch_alert_info(
                    org, proj, alert)

            self.capellaAPI.cluster_ops_apis.alerts_endpoint = \
                "/v4/organizations/{}/projects/{}/alertIntegrations"

            self.validate_testcase(result, [200], testcase, failures, True,
                                   self.expected_res, self.alert_id)

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
                                   "projectManager", "projectViewer"] for
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
        self.auth_test_extension(testcases, other_project_id)

        failures = list()
        for testcase in testcases:
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, other_project_id)
            result = self.capellaAPI.cluster_ops_apis.fetch_alert_info(
                self.organisation_id, self.project_id, self.alert_id, header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.fetch_alert_info(
                    self.organisation_id, self.project_id, self.alert_id,
                    header)

            self.validate_testcase(result, [200], testcase, failures, True,
                                   self.expected_res, self.alert_id)

        self.update_auth_with_api_token(self.curr_owner_key)
        resp = self.capellaAPI.org_ops_apis.delete_project(
            self.organisation_id, other_project_id)
        if resp.status_code != 204:
            self.log.error("Error while deleting project {}"
                           .format(other_project_id))

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_query_parameters(self):
        self.log.debug("Correct Params - OrgID: {}, ProjID: {}, AlertID: {}, "
                       .format(self.organisation_id, self.project_id,
                               self.alert_id))
        testcases = 0
        failures = list()
        for combination in self.create_path_combinations(
                self.organisation_id, self.project_id, self.alert_id):
            testcases += 1
            testcase = {
                "description": "OrganizationID: {}, ProjectID: {}, "
                               "AlertID: {}".format(str(combination[0]),
                                                    str(combination[1]),
                                                    str(combination[2])),
                "organizationID": combination[0],
                "projectID": combination[1],
                "alertID": combination[2],
            }
            if not (combination[0] == self.organisation_id and
                    combination[1] == self.project_id and
                    combination[2] == self.alert_id):
                if combination[1] == "" or combination[0] == "" or \
                        combination[2] == "":
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = "404 page not found"
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
                elif combination[1] != self.project_id:
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = {
                        "code": 2000,
                        "hint": "Check if the project ID is valid.",
                        "httpStatusCode": 404,
                        "message": "The server cannot find a project by its "
                                   "ID."
                    }
                else:
                    testcase["expected_status_code"] = 404
                    testcase["expected_error"] = {
                        "code": 3000,
                        "hint": "Please ensure that the organization ID is "
                                "correct.",
                        "httpStatusCode": 404,
                        "message": "Not Found."
                    }
            self.log.info("Executing test: {}".format(testcase["description"]))
            if "param" in testcase:
                kwarg = {testcase["param"]: testcase["paramValue"]}
            else:
                kwarg = dict()

            result = self.capellaAPI.cluster_ops_apis.fetch_alert_info(
                testcase["organizationID"], testcase["projectID"],
                testcase["alertID"], **kwarg)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.fetch_alert_info(
                    testcase["organizationID"], testcase["projectID"],
                    testcase["alertID"], **kwarg)

            self.validate_testcase(result, [200], testcase, failures, True,
                                   self.expected_res, self.alert_id)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), testcases))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[self.capellaAPI.cluster_ops_apis.fetch_alert_info,
                          (self.organisation_id, self.project_id,
                           self.alert_id)]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[self.capellaAPI.cluster_ops_apis.fetch_alert_info,
                          (self.organisation_id, self.project_id,
                           self.alert_id)]]
        self.throttle_test(api_func_list, True, self.project_id)
