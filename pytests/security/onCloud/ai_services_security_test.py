import json
import random
import string
import time

from pytests.security.security_base import SecurityBase


class SecurityTest(SecurityBase):

    def setUp(self):
        try:
            SecurityBase.setUp(self)
            self.integration_id = None
            self.workflow_id = None
            #load travel sample bucket
            res = self.capellaAPIv2.load_sample_bucket(self.tenant_id, self.project_id,
                                                       self.cluster_id,
                                                       "travel-sample")
            if res.status_code != 201:
                self.fail("Failed to load travel sample bucket." \
                          "Status code: {}, Error: {}".format(res.status_code, res.content))

        except Exception as e:
            # self.tearDown()
            self.fail("Base Setup Failed with error as - {}".format(e))

    def tearDown(self):

        resp = self.capellaAPIv2.list_autovec_workflows(self.tenant_id)
        workflow_ids = []
        if resp.status_code == 200:
            data = json.loads(resp.content)["data"]
            for workflow in data:
                workflow_ids.append(workflow["data"]["id"])

        for workflow_id in workflow_ids:
            self.log.info("Deleting workflow: {}".format(workflow_id))
            resp = self.capellaAPIv2.delete_autovec_workflow(self.tenant_id, self.project_id, self.cluster_id,
                                                             workflow_id)
            if resp.status_code == 202:
                result = self.wait_for_workflow_deletion(workflow_id)
                if not result:
                    self.log.error("Timed out while waiting for workflow deletion. Workflow id: {}".format(workflow_id))
            else:
                self.log.error("Failed to delete workflow: {}. Reason: {}".format(workflow_id,
                                                                                  resp.content))

        super(SecurityTest, self).tearDown()

    def generate_random_name(self, base_name="integration"):

        # Generate a random string of lowercase letters and digits
        random_suffix = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(10))

        # Combine the base name with the random suffix
        random_name = "{}{}".format(base_name, random_suffix)

        return random_name

    def get_sample_model_deployment_payload(self):
        payload = {
          "compute": "g6.xlarge",
          "configuration": {
            "name": "intfloat/e5-mistral-7b-instruct",
            "kind": "embedding-generation",
            "parameters": {}
          }
        }

        return payload

    def get_sample_autovec_workflow_payload(self):
        payload = {
            "type": "structured",
            "schemaFields": [
                "country"
            ],
            "embeddingModel": {
                "external": {
                    "name": self.generate_random_name("OpenAIIntegration"),
                    "modelName": "text-embedding-3-small",
                    "provider": "openAI",
                    "apiKey": "asfasd"
                }
            },
            "cbKeyspace": {
                "bucket": "travel-sample",
                "scope": "inventory",
                "collection": "airline"
            },
            "vectorIndexName": "vector-index-name",
            "embeddingFieldName": "embedding-field",
            "name": "flow2"
        }

        return payload

    def get_sample_integrations_payload(self, integration_type="s3"):

        if integration_type == "s3":
            payload = {
                "integrationType": "s3",
                "name": self.generate_random_name(),
                "data": {
                    "accessKeyId": "sample_access_key_id",
                    "secretAccessKey": "sample_secret_access_key",
                    "awsRegion": "us-east-1",
                    "bucket": "bucket",
                    "folderPath": "path"
                }
            }
        elif integration_type == "openAI":
            payload = {
                "integrationType": "openAI",
                "name": self.generate_random_name(),
                "data": {
                    "key": "sample_secret_key"
                }
            }

        return payload

    def wait_for_model_deletion(self, model_id, timeout=1800):
        start_time = time.time()
        while time.time() < start_time + timeout:
            resp = self.capellaAPIv2.get_model_details(self.tenant_id, self.project_id,
                                                       self.cluster_id, model_id)
            if resp.status_code == 404:
                return True

            self.sleep(10, "Wait for model deletion")

        return False

    def wait_for_workflow_deletion(self, workflow_id, timeout=1800):
        start_time = time.time()
        while time.time() < start_time + timeout:
            resp = self.capellaAPIv2.get_autovec_workflow(self.tenant_id, self.project_id,
                                                          self.cluster_id, workflow_id)
            if resp.status_code == 404:
                return True

            self.sleep(10, "Wait for workflow deletion")

        return False

    def create_model(self):
        resp = self.capellaAPIv2.deploy_model(self.tenant_id, self.project_id,
                                              self.cluster_id, self.get_sample_model_deployment_payload())

        error_type = ""
        if resp.status_code == 409:
            error_type = resp.json()["errorType"]

        while resp.status_code == 409 and error_type == "LLMCleanUpInProgress":
            error = resp.json()
            error_type = error["errorType"]
            resp = self.capellaAPIv2.deploy_model(self.tenant_id, self.project_id,
                                                  self.cluster_id, self.get_sample_model_deployment_payload())
            self.sleep(30, "Wait for model resource cleanup")

        if resp.status_code != 202:
            self.fail("Failed to create model. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))

        return resp.json()["id"]

    def create_autovec_workflow(self):
        resp = self.capellaAPIv2.create_autovec_workflow(self.tenant_id, self.project_id,
                                                         self.cluster_id, self.get_sample_autovec_workflow_payload())
        if resp.status_code != 202:
            self.fail("Failed to create autovec workflow. Status code: {}, Error: {}".
                      format(resp.status_code, resp.content))

        return resp.json()["id"]

    def create_autovec_integration(self):
        resp = self.capellaAPIv2.create_autovec_integration(self.tenant_id,
                                                            self.get_sample_integrations_payload())
        if resp.status_code != 202:
            return None

        return resp.json()["id"]

    def create_model_callback(self, resp, test_method_args=None):
        model_id = resp.json()["id"]
        res = self.capellaAPIv2.delete_model(self.tenant_id, self.project_id,
                                             self.cluster_id, model_id)
        if res.status_code != 204:
            self.fail("Failed to delete model: {}. Status code: {}. Error: {}".
                      format(model_id, res.status_code, res.content))

        res = self.wait_for_model_deletion(model_id)
        if not res:
            self.fail("Failed to delete model even after timeout")
        self.log.info("Model deleted")

    def create_autovec_workflow_callback(self, resp, test_method_args=None):
        workflow_id = resp.json()["id"]
        res = self.capellaAPIv2.delete_autovec_workflow(self.tenant_id, self.project_id,
                                                        self.cluster_id, workflow_id)
        if res.status_code != 202:
            self.fail("Failed to delete autovec workflow: {}. Status code: {}, Error: {}".
                      format(workflow_id, res.status_code, res.content))

        res = self.wait_for_workflow_deletion(workflow_id)
        if not res:
            self.fail("Failed to delete autovec workflow even after timeout")
        self.log.info("Workflow deleted")

    def delete_model_callback(self, resp, test_method_args=None):
        if resp.status_code != 204:
            self.fail("Failed to delete model")

        res = self.wait_for_model_deletion(self.model_id)
        if not res:
            self.fail("Failed to delete model even after timeout")

        self.model_id = self.create_model()
        self.log.info("Created model: {}".format(self.model_id))

    def delete_autovec_workflow_callback(self, resp, test_method_args=None):
        if resp.status_code != 202:
            self.fail("Failed to delete workflow")

        res = self.wait_for_workflow_deletion(self.workflow_id)
        if not res:
            self.fail("Failed to delete workflow even after timeout")
        self.workflow_id = self.create_autovec_workflow()
        self.log.info("Created workflow: {}".format(self.workflow_id))

    def delete_autovec_integration_callback(self, resp, test_method_args=None):
        if resp.status_code != 202:
            self.fail("Failed to delete integration")

        self.integration_id = self.create_autovec_integration()

    def test_autovec_integrations_api_auth(self):
        #Test for Authentication
        integration_url = "{}/v2/organizations/{}/integrations". \
                format(self.capellaAPIv2.internal_url, self.tenant_id)
        test_methods = ["GET", "POST"]
        self.log.info("Testing auth for url: {}".format(integration_url))
        for method in test_methods:
            result, error = self.test_authentication(integration_url, method=method)
            if not result:
                self.fail("Auth test failed for method: {}, url: {}, error: {}".
                          format(method, integration_url, error))

        specific_integration_url = "{}/v2/organizations/{}/integrations/{}". \
                format(self.capellaAPIv2.internal_url, self.tenant_id, self.invalid_id)

        test_methods = ["GET", "PUT", "DELETE"]
        self.log.info("Testing auth for url: {}".format(specific_integration_url))
        for method in test_methods:
            result, error = self.test_authentication(specific_integration_url, method=method)
            if not result:
                self.fail("Auth test failed for method: {}, url: {}, error: {}".
                          format(method, specific_integration_url, error))

    def test_autovec_integration_api_authz_tenant_ids(self):
        #Test with different tenant ids
        test_method_args = {
            'payload': self.get_sample_integrations_payload()
        }
        self.log.info("Testing tenant ids authorization for create autovec integration")
        result, error = self.test_tenant_ids(self.capellaAPIv2.create_autovec_integration,
                                             test_method_args, 'tenant_id', 202,
                                             expected_failure_code=403)
        if not result:
            self.fail("Authorization tenant ids test failed for create integration. Error: {}".
                      format(error))

        self.integration_id = self.create_autovec_integration()
        test_method_args = {
            'integration_id': self.integration_id
        }
        self.log.info("Testing tenant ids authorization for get autovec integration")
        result, error = self.test_tenant_ids(self.capellaAPIv2.get_autovec_integration,
                                             test_method_args, 'tenant_id', 200,
                                             expected_failure_code=403)
        if not result:
            self.fail("Authorization tenant ids test failed for get integration. Error: {}".
                      format(error))

        self.log.info("Testing tenant ids authorization for delete autovec integration")
        result, error = self.test_tenant_ids(self.capellaAPIv2.delete_autovec_integration,
                                             test_method_args, 'tenant_id', 202,
                                             self.delete_autovec_integration_callback, expected_failure_code=403)
        if not result:
            self.fail("Authorization tenant ids test failed for delete integration. Error: {}".
                      format(error))

        test_method_args = {
            'integration_id': self.integration_id,
            'payload': self.get_sample_integrations_payload()
        }
        self.log.info("Testing tenant ids Authorization for update autovec integration")
        result, error = self.test_tenant_ids(self.capellaAPIv2.update_autovec_integration,
                                             test_method_args, 'tenant_id', 204,
                                             expected_failure_code=403)
        if not result:
            self.fail("Authorization tenant ids test failed for update integration. Error: {}".format(error))

        test_method_args = {}
        self.log.info("Testing tenant ids Authorization for list autovec integration")
        result, error = self.test_tenant_ids(self.capellaAPIv2.list_autovec_integrations,
                                             test_method_args, 'tenant_id', 200,
                                             expected_failure_code=403)
        if not result:
            self.fail("Authorization tenant ids test failed for list integrations. Error: {}".
                      format(error))

    def test_autovec_integration_api_authz_roles(self):
        # Test with org roles
        test_method_args = {
            'tenant_id': self.tenant_id,
            'payload': self.get_sample_integrations_payload()
        }
        result, error = self.test_with_org_roles("create_autovec_integration", test_method_args,
                                                 202, None, "provisioned")
        if not result:
            self.fail("Authorization org roles test failed for create autovec integration. Error: {}".
                      format(error))

        self.integration_id = self.create_autovec_integration()
        test_method_args = {
            'tenant_id': self.tenant_id,
            'integration_id': self.integration_id,
            'payload': self.get_sample_integrations_payload()
        }
        result, error = self.test_with_org_roles("update_autovec_integration", test_method_args,
                                                 204, None, "provisioned")
        if not result:
            self.fail("Authorization org roles test failed for update autovec integration. Error: {}".
                      format(error))

        test_method_args = {
            'tenant_id': self.tenant_id,
            'integration_id': self.integration_id
        }
        result, error = self.test_with_org_roles("get_autovec_integration", test_method_args,
                                                 200, None, "provisioned")
        if not result:
            self.fail("Authorization org roles test failed for get autovec integration. Error: {}".
                      format(error))

        result, error = self.test_with_org_roles("delete_autovec_integration", test_method_args,
                                                 202, self.delete_autovec_integration_callback,
                                                 "provisioned")
        if not result:
            self.fail("Authorization org roles test failed for delete autovec integration. Error: {}".
                      format(error))

        test_method_args = {
            'tenant_id': self.tenant_id
        }
        result, error = self.test_with_org_roles("list_autovec_integrations", test_method_args,
                                                 200, None, "provisioned")
        if not result:
            self.fail("Authorization org roles test failed for list autovec integrations. Error: {}".
                      format(error))

    def test_autovec_workflow_api_auth(self):
        url_method_map = {}
        workflow_url = "{}/v2/organizations/{}/projects/{}/clusters/{}/ai/workflows". \
            format(self.capellaAPIv2.internal_url, self.tenant_id, self.project_id, self.cluster_id)
        url_method_map[workflow_url] = ["POST"]

        self.workflow_id = self.create_autovec_workflow()
        specific_workflow_url = "{}/v2/organizations/{}/projects/{}/clusters/{}/ai/workflows/{}". \
            format(self.capellaAPIv2.internal_url, self.tenant_id, self.project_id, self.cluster_id, self.workflow_id)
        url_method_map[specific_workflow_url] = ["GET", "PUT", "DELETE"]

        list_workflow_url = "{}/v2/organizations/{}/ai/workflows?page={}&perPage={}". \
            format(self.capellaAPIv2.internal_url, self.tenant_id, 1, 10)
        url_method_map[list_workflow_url] = ["GET"]

        for url, test_methods in url_method_map.items():
            self.log.info("Testing auth for url: {}".format(url))
            for method in test_methods:
                result, error = self.test_authentication(url, method=method,
                                                         expected_status_codes=[401, 404])
                if not result:
                    self.fail("Auth test failed for method: {}, url: {}, error: {}".
                              format(method, url, error))

    def test_autovec_workflow_api_authz_tenant_ids(self):
        #Test with tenant ids
        test_method_args = {
            'project_id': self.project_id,
            'cluster_id': self.cluster_id,
            'payload': self.get_sample_autovec_workflow_payload()
        }
        self.log.info("Testing tenant ids authorization for create autovec workflow api")
        result, error = self.test_tenant_ids(self.capellaAPIv2.create_autovec_workflow, test_method_args,
                                             'tenant_id', 202,
                                             self.create_autovec_workflow_callback)
        if not result:
            self.fail("Authorization tenant ids test failed for create autovec workflow. Error: {}".
                      format(error))

        self.workflow_id = self.create_autovec_workflow()
        test_method_args = {
            'project_id': self.project_id,
            'cluster_id': self.cluster_id,
            'workflow_id': self.workflow_id
        }
        self.log.info("Testing tenant ids authorization for get autovec workflow")
        result, error = self.test_tenant_ids(self.capellaAPIv2.get_autovec_workflow, test_method_args,
                                             'tenant_id', 200)
        if not result:
            self.fail("Authorization tenant ids test failed for get autovec workflow. Error: {}".
                      format(error))

        self.log.info("Testing tenant ids authorization for delete autovec workflow")
        result, error = self.test_tenant_ids(self.capellaAPIv2.delete_autovec_workflow, test_method_args,
                                             'tenant_id', 202,
                                             self.delete_autovec_workflow_callback)
        if not result:
            self.fail("Authorization tenant ids test failed for delete autovec workflow. Error: {}".
                      format(error))

        test_method_args = {}
        self.log.info("Test tenant ids authorization for list autovec workflow")
        result, error = self.test_tenant_ids(self.capellaAPIv2.list_autovec_workflows, test_method_args,
                                             'tenant_id', 200)
        if not result:
            self.fail("Authorization tenant ids test failed for list autovec workflow. Error: {}".
                      format(error))

    def test_autovec_workflow_api_authz_project_ids(self):
        test_method_args = {
            'tenant_id': self.tenant_id,
            'cluster_id': self.cluster_id,
            'payload': self.get_sample_autovec_workflow_payload()
        }
        self.log.info("Testing project ids authorization for create autovec workflow")
        result, error = self.test_project_ids(self.capellaAPIv2.create_autovec_workflow, test_method_args,
                                              'project_id', 202,
                                              self.create_autovec_workflow_callback, False)
        if not result:
            self.fail("Authorization project ids test failed for create autovec workflow. Error: {}".
                      format(error))

        self.workflow_id = self.create_autovec_workflow()
        test_method_args = {
            'tenant_id': self.tenant_id,
            'cluster_id': self.cluster_id,
            'workflow_id': self.workflow_id
        }
        self.log.info("Testing project ids authorization for get autovec workflow")
        result, error = self.test_project_ids(self.capellaAPIv2.get_autovec_workflow, test_method_args,
                                              'project_id', 200)
        if not result:
            self.fail("Authorization project ids test failed for get autovec workflow. Error: {}".
                      format(error))

        self.log.info("Testing project ids authorization for delete autovec workflow")
        result, error = self.test_project_ids(self.capellaAPIv2.delete_autovec_workflow, test_method_args,
                                              'project_id', 202,
                                              self.delete_autovec_workflow_callback)
        if not result:
            self.fail("Authorization project ids test failed for delete autovec workflow. Error: {}".
                      format(error))

    def test_autovec_workflow_api_authz_org_roles(self):
        test_method_args = {
            'tenant_id': self.tenant_id,
            'project_id': self.project_id,
            'cluster_id': self.cluster_id,
            'payload': self.get_sample_autovec_workflow_payload()
        }
        self.log.info("Testing org roles authorization for create autovec workflow")
        result, error = self.test_with_org_roles("create_autovec_workflow", test_method_args,
                                                 202, self.create_autovec_workflow_callback,
                                                 "provisioned")
        if not result:
            self.fail("Authorization org roles test failed for create autovec workflow. Error: {}".
                      format(error))

        self.workflow_id = self.create_autovec_workflow()
        test_method_args = {
            'tenant_id': self.tenant_id,
            'project_id': self.project_id,
            'cluster_id': self.cluster_id,
            'workflow_id': self.workflow_id
        }
        self.log.info("Testing org roles authorization for get autovec workflow")
        result, error = self.test_with_org_roles("get_autovec_workflow", test_method_args,
                                                 200, None, "provisioned")
        if not result:
            self.fail("Authorization org roles test failed for get autovec workflow. Error: {}".
                      format(error))

        self.log.info("Testing org roles authorization for delete autovec workflow")
        result, error = self.test_with_org_roles("delete_autovec_workflow", test_method_args,
                                                  202, self.delete_autovec_workflow_callback,
                                                  "provisioned")
        if not result:
            self.fail("Authorization org roles test failed for delete autovec workflow. Error: {}".
                      format(error))

        test_method_args = {
            'tenant_id': self.tenant_id
        }
        self.log.info("Testing org roles authorization for list autovec workflow")
        result, error = self.test_with_org_roles("list_autovec_workflow", test_method_args,
                                                 200, None, "provisioned")
        if not result:
            self.fail("Authorization org roles test failed for list autovec workflow. Error: {}".
                      format(error))

    def test_autovec_workflow_api_authz_project_roles(self):
        test_method_args = {
            'tenant_id': self.tenant_id,
            'project_id': self.project_id,
            'cluster_id': self.cluster_id,
            'payload': self.get_sample_autovec_workflow_payload()
        }
        self.log.info("Testing project roles authorization for create autovec workflow")
        result, error = self.test_with_project_roles("create_autovec_workflow", test_method_args,
                                                     ["projectOwner", "projectClusterManager"], 202,
                                                     self.create_autovec_workflow_callback, "provisioned")
        if not result:
            self.fail("Authorization project roles test failed for create autovec workflow. Error: {}".
                      format(error))

        self.workflow_id = self.create_autovec_workflow()
        test_method_args = {
            'tenant_id': self.tenant_id,
            'project_id': self.project_id,
            'cluster_id': self.cluster_id,
            'workflow_id': self.workflow_id
        }
        self.log.info("Testing project roles authorization for get autovec workflow")
        result, error = self.test_with_project_roles("get_autovec_workflow", test_method_args,
                                                     ["projectOwner", "projectClusterManager", "projectClusterViewer"], 200,
                                                     None, "provionsed")
        if not result:
            self.fail("Authorization project roles test failed for get autovec workflow. Error: {}".
                      format(error))

        self.log.info("Testing project roles authorization for delete autovec workflow")
        result, error = self.test_with_project_roles("delete_autovec_workflow", test_method_args,
                                                     ["projectOwner", "projectClusterManager"], 202,
                                                     self.delete_autovec_workflow_callback, "provisioned")
        if not result:
            self.fail("Authorization project roles test failed for delete autovec workflow. Error: {}".
                      format(error))

        test_method_args = {
            'tenant_id': self.tenant_id
        }
        self.log.info("Testing project roles authorization for list autovec workflow")
        result, error = self.test_with_project_roles("list_autovec_workflow", test_method_args,
                                                     [], 200,
                                                     None, "provionsed")
        if not result:
            self.fail("Authorization project roles test failed for list autovec workflow")

    def test_model_api_auth(self):
        url_method_map = {}
        model_url = "{}/v2/organizations/{}/projects/{}/clusters/{}/languagemodels". \
            format(self.capellaAPIv2.internal_url, self.tenant_id, self.project_id, self.cluster_id)
        url_method_map[model_url] = ["POST"]

        specific_model_url = "{}/v2/organizations/{}/projects/{}/clusters/{}/languagemodels/{}". \
            format(self.capellaAPIv2.internal_url, self.tenant_id, self.project_id, self.cluster_id, self.invalid_id)
        url_method_map[specific_model_url] = ["GET", "DELETE"]

        list_model_url = "{}/v2/organizations/{}/languagemodels?page={}&perPage={}". \
            format(self.capellaAPIv2.internal_url, self.tenant_id, 1, 10)
        url_method_map[list_model_url] = ["GET"]

        for url, test_methods in url_method_map.items():
            self.log.info("Testing auth for url: {}".format(url))
            for method in test_methods:
                result, error = self.test_authentication(url, method=method,
                                                         expected_status_codes=[401])
                if not result:
                    self.fail("Auth test failed for method: {}, url: {}, error: {}".
                              format(method, url, error))

    def test_model_api_tenant_ids(self):
        test_method_args = {
            'project_id': self.project_id,
            'cluster_id': self.cluster_id,
            'payload': self.get_sample_model_deployment_payload()
        }
        self.log.info("Testing tenant ids authorization for create model api")
        result, error = self.test_tenant_ids(self.capellaAPIv2.deploy_model, test_method_args,
                                             'tenant_id', [202, 409],
                                             self.create_model_callback, 403)
        if not result:
            self.fail("Authorization tenant ids test failed for create model api. Error: {}".
                      format(error))

        self.model_id = self.create_model()
        test_method_args = {
            'project_id': self.project_id,
            'cluster_id': self.cluster_id,
            'model_id': self.model_id
        }
        self.log.info("Testing tenant ids authorization for get model api")
        result, error = self.test_tenant_ids(self.capellaAPIv2.get_model_details, test_method_args,
                                             'tenant_id', 200, None,
                                             [404, 403])
        if not result:
            self.fail("Authorization tenant ids test failed for get model api. Error: {}".
                      format(error))

        test_method_args = {
            'project_id': self.project_id,
            'cluster_id': self.cluster_id,
            'model_id': self.model_id
        }
        self.log.info("Testing tenant ids authorization for delete model api")
        result, error = self.test_tenant_ids(self.capellaAPIv2.delete_model, test_method_args,
                                             'tenant_id', 204,
                                             self.delete_model_callback, [404, 403])
        if not result:
            self.fail("Authorization tenant ids failed for delete model api. Error: {}".
                      format(error))

        test_method_args = {}
        self.log.info("Testing tenant ids authorization for list model api")
        result, error = self.test_tenant_ids(self.capellaAPIv2.list_models, test_method_args,
                                             'tenant_id', 200,
                                             None, [404, 403])
        if not result:
            self.fail("Authorization tenant ids test failed for list model api. Error: {}".
                      format(error))


    def test_model_api_project_ids(self):
        test_method_args = {
            'tenant_id': self.tenant_id,
            'cluster_id': self.cluster_id,
            'payload': self.get_sample_model_deployment_payload()
        }
        result, error = self.test_project_ids(self.capellaAPIv2.deploy_model, test_method_args,
                                              'project_id', [202, 409],
                                              self.create_model_callback, False)
        if not result:
            self.fail("Authorization project ids test failed for create model api. Error: {}".
                      format(error))

        self.model_id = self.create_model()
        test_method_args = {
            'tenant_id': self.tenant_id,
            'cluster_id': self.cluster_id,
            'model_id': self.model_id
        }
        result, error = self.test_project_ids(self.capellaAPIv2.get_model_details, test_method_args,
                                              'project_id', [200])
        if not result:
            self.fail("Authorization project ids test failed for get model api. Error: {}".
                      format(error))

        test_method_args = {
            'tenant_id': self.tenant_id,
            'cluster_id': self.cluster_id,
            'model_id': self.model_id
        }
        result, error = self.test_project_ids(self.capellaAPIv2.delete_model, test_method_args,
                                              'project_id', [204, 404],
                                              self.delete_model_callback, True)
        if not result:
            self.fail("Authorization project ids test failed for delete model api. Error: {}".
                      format(error))