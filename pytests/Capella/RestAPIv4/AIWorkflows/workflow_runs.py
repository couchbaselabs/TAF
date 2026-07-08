"""
Created on July 03, 2026

@author: Created using cbRAT cbModule by Vipul Bhardwaj
"""

from pytests.Capella.RestAPIv4.AIWorkflows.ai_workflows_base import \
    AIWorkflowBase


class AIWorkflowRunBase(AIWorkflowBase):

    def setUp(self, nomenclature="AIWorkflows_Runs_Base"):
        AIWorkflowBase.setUp(self, nomenclature)

        self.log.info("Creating AI Workflow Run for the test")
        res = self.capellaAPI.cluster_ops_apis.create_ai_workflow_run(
            self.organisation_id, self.project_id, self.cluster_id,
            self.workflow_id)
        if res.status_code == 429:
            self.handle_rate_limit(int(res.headers["Retry-After"]))
            res = self.capellaAPI.cluster_ops_apis.create_ai_workflow_run(
                self.organisation_id, self.project_id, self.cluster_id,
                self.workflow_id)
        if res.status_code != 201:
            self.log.error("Result: {}".format(res.content))
            self.tearDown()
            self.fail("Error while creating AI Workflow Run for the test.")
        self.log.info("AI Workflow Run created successfully.")
        self.run_id = res.json()["id"]

    def tearDown(self):
        self.update_auth_with_api_token(self.curr_owner_key)
        if hasattr(self, "workflow_id") and self.workflow_id:
            self.log.info("Deleting all runs for workflow: {}".format(
                self.workflow_id))
            res = self.capellaAPI.cluster_ops_apis.delete_ai_workflow_runs(
                self.organisation_id, self.project_id, self.cluster_id,
                self.workflow_id)
            if res.status_code == 429:
                self.handle_rate_limit(int(res.headers["Retry-After"]))
                res = self.capellaAPI.cluster_ops_apis.delete_ai_workflow_runs(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.workflow_id)
            if res.status_code not in [200, 202, 204, 404]:
                self.log.error("Failed to delete runs for workflow {}: {}"
                               .format(self.workflow_id, res.content))
        super(AIWorkflowRunBase, self).tearDown()


class CreateAIWorkflowRun(AIWorkflowBase):

    def setUp(self, nomenclature="AIWorkflows_Runs_POST"):
        AIWorkflowBase.setUp(self, nomenclature)

    def tearDown(self):
        super(CreateAIWorkflowRun, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Send call with valid path params"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}"
                       "/aiServices/workflows/{}/runs",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title>"
                                  "</head><body><center><h1>404NotFound"
                                  "</h1></center><hr><center>nginx</center>"
                                  "</body></html>"
            }, {
                "description": "Replace the last path param name in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}"
                       "/aiServices/workflows/{}/run",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}"
                       "/aiServices/workflows/{}/runs/run",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Call API with non-hex organizationId",
                "invalid_organizationId": self.replace_last_character(
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
                "description": "Call API with non-hex projectId",
                "invalid_projectId": self.replace_last_character(
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
                "description": "Call API with non-hex clusterId",
                "invalid_clusterId": self.replace_last_character(
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
                "description": "Call API with non-hex workflowId",
                "invalid_workflowId": self.replace_last_character(
                    self.workflow_id, non_hex=True),
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
            organization = self.organisation_id
            project = self.project_id
            cluster = self.cluster_id
            workflow = self.workflow_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.ai_workflow_runs_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]
            elif "invalid_clusterId" in testcase:
                cluster = testcase["invalid_clusterId"]
            elif "invalid_workflowId" in testcase:
                workflow = testcase["invalid_workflowId"]

            result = self.capellaAPI.cluster_ops_apis.create_ai_workflow_run(
                organization, project, cluster, workflow)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = (self.capellaAPI.cluster_ops_apis
                          .create_ai_workflow_run(
                              organization, project, cluster, workflow))
            self.capellaAPI.cluster_ops_apis.ai_workflow_runs_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters/{}" \
                "/aiServices/workflows/{}/runs"
            self.validate_testcase(result, [201], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner", "projectManager"
        ]):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = self.capellaAPI.cluster_ops_apis.create_ai_workflow_run(
                self.organisation_id, self.project_id, self.cluster_id,
                self.workflow_id, headers=header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = (self.capellaAPI.cluster_ops_apis
                          .create_ai_workflow_run(
                              self.organisation_id, self.project_id,
                              self.cluster_id, self.workflow_id,
                              headers=header))
            self.validate_testcase(result, [201], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.create_ai_workflow_run, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.workflow_id
            )
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.create_ai_workflow_run, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.workflow_id
            )
        ]]
        self.throttle_test(api_func_list, True, self.project_id)


class ListAIWorkflowRuns(AIWorkflowBase):

    def setUp(self, nomenclature="AIWorkflows_Runs_LIST"):
        AIWorkflowBase.setUp(self, nomenclature)
        self.expected_res = {
            "cursor": {
                "hrefs": {
                    "first": None,
                    "last": None,
                    "next": None,
                    "previous": None
                },
                "pages": {
                    "last": None,
                    "next": None,
                    "page": None,
                    "perPage": None,
                    "previous": None,
                    "totalItems": None
                }
            },
            "data": []
        }

    def tearDown(self):
        super(ListAIWorkflowRuns, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Send call with valid path params"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}"
                       "/aiServices/workflows/{}/runs",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title>"
                                  "</head><body><center><h1>404NotFound"
                                  "</h1></center><hr><center>nginx</center>"
                                  "</body></html>"
            }, {
                "description": "Replace the last path param name in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}"
                       "/aiServices/workflows/{}/run",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}"
                       "/aiServices/workflows/{}/runs/run",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Call API with non-hex organizationId",
                "invalid_organizationId": self.replace_last_character(
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
                "description": "Call API with non-hex projectId",
                "invalid_projectId": self.replace_last_character(
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
                "description": "Call API with non-hex clusterId",
                "invalid_clusterId": self.replace_last_character(
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
                "description": "Call API with non-hex workflowId",
                "invalid_workflowId": self.replace_last_character(
                    self.workflow_id, non_hex=True),
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
            organization = self.organisation_id
            project = self.project_id
            cluster = self.cluster_id
            workflow = self.workflow_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.ai_workflow_runs_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]
            elif "invalid_clusterId" in testcase:
                cluster = testcase["invalid_clusterId"]
            elif "invalid_workflowId" in testcase:
                workflow = testcase["invalid_workflowId"]

            result = self.capellaAPI.cluster_ops_apis.list_ai_workflow_runs(
                organization, project, cluster, workflow)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = (self.capellaAPI.cluster_ops_apis
                          .list_ai_workflow_runs(
                              organization, project, cluster, workflow))
            self.capellaAPI.cluster_ops_apis.ai_workflow_runs_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters/{}" \
                "/aiServices/workflows/{}/runs"
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner", "projectManager",
            "projectViewer", "projectDataReaderWriter", "projectDataReader"
        ]):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = self.capellaAPI.cluster_ops_apis.list_ai_workflow_runs(
                self.organisation_id, self.project_id, self.cluster_id,
                self.workflow_id, headers=header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = (self.capellaAPI.cluster_ops_apis
                          .list_ai_workflow_runs(
                              self.organisation_id, self.project_id,
                              self.cluster_id, self.workflow_id,
                              headers=header))
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.list_ai_workflow_runs, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.workflow_id
            )
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.list_ai_workflow_runs, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.workflow_id
            )
        ]]
        self.throttle_test(api_func_list, True, self.project_id)


class DeleteAIWorkflowRuns(AIWorkflowBase):

    def setUp(self, nomenclature="AIWorkflows_Runs_DELETE"):
        AIWorkflowBase.setUp(self, nomenclature)

    def tearDown(self):
        super(DeleteAIWorkflowRuns, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Send call with valid path params"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}"
                       "/aiServices/workflows/{}/runs",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title>"
                                  "</head><body><center><h1>404NotFound"
                                  "</h1></center><hr><center>nginx</center>"
                                  "</body></html>"
            }, {
                "description": "Replace the last path param name in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}"
                       "/aiServices/workflows/{}/run",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}"
                       "/aiServices/workflows/{}/runs/run",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Call API with non-hex organizationId",
                "invalid_organizationId": self.replace_last_character(
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
                "description": "Call API with non-hex projectId",
                "invalid_projectId": self.replace_last_character(
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
                "description": "Call API with non-hex clusterId",
                "invalid_clusterId": self.replace_last_character(
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
                "description": "Call API with non-hex workflowId",
                "invalid_workflowId": self.replace_last_character(
                    self.workflow_id, non_hex=True),
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
            organization = self.organisation_id
            project = self.project_id
            cluster = self.cluster_id
            workflow = self.workflow_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.ai_workflow_runs_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]
            elif "invalid_clusterId" in testcase:
                cluster = testcase["invalid_clusterId"]
            elif "invalid_workflowId" in testcase:
                workflow = testcase["invalid_workflowId"]

            result = (self.capellaAPI.cluster_ops_apis
                      .delete_ai_workflow_runs(
                          organization, project, cluster, workflow))
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = (self.capellaAPI.cluster_ops_apis
                          .delete_ai_workflow_runs(
                              organization, project, cluster, workflow))
            self.capellaAPI.cluster_ops_apis.ai_workflow_runs_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters/{}" \
                "/aiServices/workflows/{}/runs"
            self.validate_testcase(result, [204], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner", "projectManager"
        ]):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = (self.capellaAPI.cluster_ops_apis
                      .delete_ai_workflow_runs(
                          self.organisation_id, self.project_id,
                          self.cluster_id, self.workflow_id, header))
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = (self.capellaAPI.cluster_ops_apis
                          .delete_ai_workflow_runs(
                              self.organisation_id, self.project_id,
                              self.cluster_id, self.workflow_id, header))
            self.validate_testcase(result, [204], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.delete_ai_workflow_runs, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.workflow_id
            )
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.delete_ai_workflow_runs, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.workflow_id
            )
        ]]
        self.throttle_test(api_func_list, True, self.project_id)


class GetAIWorkflowRun(AIWorkflowRunBase):

    def setUp(self, nomenclature="AIWorkflows_Runs_GET"):
        AIWorkflowRunBase.setUp(self, nomenclature)

    def tearDown(self):
        super(GetAIWorkflowRun, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Send call with valid path params"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}"
                       "/aiServices/workflows/{}/runs/{}",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title>"
                                  "</head><body><center><h1>404NotFound"
                                  "</h1></center><hr><center>nginx</center>"
                                  "</body></html>"
            }, {
                "description": "Replace the last path param name in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}"
                       "/aiServices/workflows/{}/run/{}",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}"
                       "/aiServices/workflows/{}/runs/{}/run",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Call API with non-hex organizationId",
                "invalid_organizationId": self.replace_last_character(
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
                "description": "Call API with non-hex projectId",
                "invalid_projectId": self.replace_last_character(
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
                "description": "Call API with non-hex clusterId",
                "invalid_clusterId": self.replace_last_character(
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
                "description": "Call API with non-hex workflowId",
                "invalid_workflowId": self.replace_last_character(
                    self.workflow_id, non_hex=True),
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
                "description": "Call API with non-hex runId",
                "invalid_runId": self.replace_last_character(
                    self.run_id, non_hex=True),
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
            organization = self.organisation_id
            project = self.project_id
            cluster = self.cluster_id
            workflow = self.workflow_id
            run = self.run_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis.ai_workflow_run_endpoint = \
                    testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]
            elif "invalid_clusterId" in testcase:
                cluster = testcase["invalid_clusterId"]
            elif "invalid_workflowId" in testcase:
                workflow = testcase["invalid_workflowId"]
            elif "invalid_runId" in testcase:
                run = testcase["invalid_runId"]

            result = self.capellaAPI.cluster_ops_apis.get_ai_workflow_run(
                organization, project, cluster, workflow, run)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.get_ai_workflow_run(
                    organization, project, cluster, workflow, run)
            self.capellaAPI.cluster_ops_apis.ai_workflow_run_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters/{}" \
                "/aiServices/workflows/{}/runs/{}"
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner", "projectManager",
            "projectViewer", "projectDataReaderWriter", "projectDataReader"
        ]):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = self.capellaAPI.cluster_ops_apis.get_ai_workflow_run(
                self.organisation_id, self.project_id, self.cluster_id,
                self.workflow_id, self.run_id, header)
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = self.capellaAPI.cluster_ops_apis.get_ai_workflow_run(
                    self.organisation_id, self.project_id, self.cluster_id,
                    self.workflow_id, self.run_id, header)
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.get_ai_workflow_run, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.workflow_id, self.run_id
            )
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.get_ai_workflow_run, (
                self.organisation_id, self.project_id, self.cluster_id,
                self.workflow_id, self.run_id
            )
        ]]
        self.throttle_test(api_func_list, True, self.project_id)


class ListProcessedFiles(AIWorkflowRunBase):

    def setUp(self, nomenclature="AIWorkflows_ProcessedFiles_LIST"):
        AIWorkflowRunBase.setUp(self, nomenclature)
        self.expected_res = {
            "cursor": {
                "hrefs": {
                    "first": None,
                    "last": None,
                    "next": None,
                    "previous": None
                },
                "pages": {
                    "last": None,
                    "next": None,
                    "page": None,
                    "perPage": None,
                    "previous": None,
                    "totalItems": None
                }
            },
            "data": []
        }

    def tearDown(self):
        super(ListProcessedFiles, self).tearDown()

    def test_api_path(self):
        testcases = [
            {
                "description": "Send call with valid path params"
            }, {
                "description": "Replace api version in URI",
                "url": "/v3/organizations/{}/projects/{}/clusters/{}"
                       "/aiServices/workflows/{}/runs/{}/processedFiles",
                "expected_status_code": 404,
                "expected_error": "<html><head><title>404NotFound</title>"
                                  "</head><body><center><h1>404NotFound"
                                  "</h1></center><hr><center>nginx</center>"
                                  "</body></html>"
            }, {
                "description": "Replace the last path param name in URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}"
                       "/aiServices/workflows/{}/runs/{}/processedFile",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Add an invalid segment to the URI",
                "url": "/v4/organizations/{}/projects/{}/clusters/{}"
                       "/aiServices/workflows/{}/runs/{}/processedFiles/file",
                "expected_status_code": 404,
                "expected_error": "404 page not found"
            }, {
                "description": "Call API with non-hex organizationId",
                "invalid_organizationId": self.replace_last_character(
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
                "description": "Call API with non-hex projectId",
                "invalid_projectId": self.replace_last_character(
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
                "description": "Call API with non-hex clusterId",
                "invalid_clusterId": self.replace_last_character(
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
                "description": "Call API with non-hex workflowId",
                "invalid_workflowId": self.replace_last_character(
                    self.workflow_id, non_hex=True),
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
                "description": "Call API with non-hex runId",
                "invalid_runId": self.replace_last_character(
                    self.run_id, non_hex=True),
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
            organization = self.organisation_id
            project = self.project_id
            cluster = self.cluster_id
            workflow = self.workflow_id
            run = self.run_id

            if "url" in testcase:
                self.capellaAPI.cluster_ops_apis\
                    .ai_workflow_processed_files_endpoint = testcase["url"]
            if "invalid_organizationId" in testcase:
                organization = testcase["invalid_organizationId"]
            elif "invalid_projectId" in testcase:
                project = testcase["invalid_projectId"]
            elif "invalid_clusterId" in testcase:
                cluster = testcase["invalid_clusterId"]
            elif "invalid_workflowId" in testcase:
                workflow = testcase["invalid_workflowId"]
            elif "invalid_runId" in testcase:
                run = testcase["invalid_runId"]

            result = (self.capellaAPI.cluster_ops_apis
                      .list_ai_workflow_processed_files(
                          organization, project, cluster, workflow, run))
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = (self.capellaAPI.cluster_ops_apis
                          .list_ai_workflow_processed_files(
                              organization, project, cluster, workflow, run))
            self.capellaAPI.cluster_ops_apis\
                .ai_workflow_processed_files_endpoint = \
                "/v4/organizations/{}/projects/{}/clusters/{}" \
                "/aiServices/workflows/{}/runs/{}/processedFiles"
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED out of {} TOTAL tests"
                      .format(len(failures), len(testcases)))

    def test_authorization(self):
        failures = list()
        for testcase in self.v4_RBAC_injection_init([
            "organizationOwner", "projectOwner", "projectManager",
            "projectViewer", "projectDataReaderWriter", "projectDataReader"
        ]):
            self.log.info("Executing test: {}".format(testcase["description"]))
            header = dict()
            self.auth_test_setup(testcase, failures, header,
                                 self.project_id, self.other_project_id)
            result = (self.capellaAPI.cluster_ops_apis
                      .list_ai_workflow_processed_files(
                          self.organisation_id, self.project_id,
                          self.cluster_id, self.workflow_id, self.run_id,
                          headers=header))
            if result.status_code == 429:
                self.handle_rate_limit(int(result.headers["Retry-After"]))
                result = (self.capellaAPI.cluster_ops_apis
                          .list_ai_workflow_processed_files(
                              self.organisation_id, self.project_id,
                              self.cluster_id, self.workflow_id, self.run_id,
                              headers=header))
            self.validate_testcase(result, [200], testcase, failures)

        if failures:
            for fail in failures:
                self.log.warning(fail)
            self.fail("{} tests FAILED.".format(len(failures)))

    def test_multiple_requests_using_API_keys_with_same_role_which_has_access(
            self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.list_ai_workflow_processed_files,
            (
                self.organisation_id, self.project_id, self.cluster_id,
                self.workflow_id, self.run_id
            )
        ]]
        self.throttle_test(api_func_list)

    def test_multiple_requests_using_API_keys_with_diff_role(self):
        api_func_list = [[
            self.capellaAPI.cluster_ops_apis.list_ai_workflow_processed_files,
            (
                self.organisation_id, self.project_id, self.cluster_id,
                self.workflow_id, self.run_id
            )
        ]]
        self.throttle_test(api_func_list, True, self.project_id)
