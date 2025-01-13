import base64
import json
import random
import string
import time

import requests

from pytests.security.security_base import SecurityBase
from scripts.old_install import params


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

            self.deploy_second_cluster = self.input.param('deploy_second_cluster', False)
            if self.deploy_second_cluster:
                self.first_cluster_id = self.cluster_id
                self.create_cluster("Security_AI_second_cluster", self.server_version, self.provider)
                self.second_cluster_id = self.cluster_id
                self.cluster_id = self.first_cluster_id

        except Exception as e:
            self.fail("Base Setup Failed with error as - {}".format(e))

    def tearDown(self):

        if self.deploy_second_cluster:
            self.cluster_id = self.second_cluster_id
            self.delete_cluster()
            self.cluster_id = self.first_cluster_id

        resp = self.capellaAPIv2.list_autovec_workflows(self.tenant_id)
        workflow_ids = []
        if resp.status_code == 200:
            data = json.loads(resp.content)["data"]
            for workflow in data:
                cluster_id = workflow["data"]["clusterId"]
                if cluster_id == self.cluster_id:
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
        random_suffix = ''.join(random.choice(string.ascii_lowercase + string.ascii_uppercase + string.digits) for _ in range(10))

        # Combine the base name with the random suffix
        random_name = "{}{}".format(base_name, random_suffix)

        return random_name

    def get_sample_model_deployment_payload(self, type="embedding"):

        if type == "embedding":
            payload = {
              "compute": "g6.xlarge",
              "configuration": {
                "name": "intfloat/e5-mistral-7b-instruct",
                "kind": "embedding-generation",
                "parameters": {}
              }
            }
        elif type == "text":
            payload = {
                "compute": "g6.xlarge",
                "configuration": {
                    "name": "meta-llama/Llama-3.1-8B-Instruct",
                    "kind": "text-generation",
                    "parameters": {}
                }
            }

        return payload

    def get_sample_autovec_workflow_payload(self, integration_id=None, openai_key=None):

        if integration_id:
            openai_payload = {
                "id": integration_id,
                "modelName": "text-embedding-3-small"
            }
        elif openai_key:
            openai_payload = {
                "name": self.generate_random_name("OpenAIIntegration"),
                "modelName": "text-embedding-3-small",
                "provider": "openAI",
                "apiKey": openai_key
            }
        else:
            openai_payload = {
                "name": self.generate_random_name("OpenAIIntegration"),
                "modelName": "text-embedding-3-small",
                "provider": "openAI",
                "apiKey": "afdasd"
            }

        payload = {
            "type": "structured",
            "schemaFields": [
                "country"
            ],
            "embeddingModel": {
                "external": openai_payload
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

    def get_sample_vulcan_workflow_payload(self, s3_access_key=None, s3_secret_key=None,
                                           s3_region="us-east-1", s3_path="pdf", s3_bucket="davinci-tests",
                                           s3_integration=None, openai_integration=None, openai_key=None):

        if s3_access_key and s3_secret_key:
            data_source = {
                "accessKey": s3_access_key,
                "secretKey": s3_secret_key,
                "bucket": s3_bucket,
                "path": s3_path,
                "region": s3_region,
                "name": self.generate_random_name("s3Integration")
            }
        elif s3_integration:
            data_source = {
                "id": s3_integration
            }
        else:
            data_source = {
                "accessKey": "sample_acess_key",
                "secretKey": "sample_secret_key",
                "bucket": "test-bucket",
                "path": "pdf",
                "region": "us-east-1",
                "name": self.generate_random_name("s3Integration")
            }

        if openai_integration:
            openai_payload = {
                "id": openai_integration,
                "modelName": "text-embedding-3-small"
            }
        elif openai_key:
            openai_payload = {
                "name": self.generate_random_name("OpenAIIntegration"),
                "modelName": "text-embedding-3-small",
                "provider": "openAI",
                "apiKey": openai_key
            }
        else:
            openai_payload = {
                "name": self.generate_random_name("OpenAIIntegration"),
                "modelName": "text-embedding-3-small",
                "provider": "openAI",
                "apiKey": "afdasd"
            }

        payload = {
          "name": "testworkflow1",
          "type": "unstructured",
          "cbKeyspace": {
            "bucket": "travel-sample",
            "scope": "_default",
            "collection": "_default"
          },
          "embeddingModel": {
            "external": openai_payload
          },
          "vectorIndexName": "test-index-1",
          "embeddingFieldName": "embedding-text",
          "dataSource": data_source,
          "chunkingStrategy": {
            "strategyType": "PARAGRAPH_SPLITTER",
            "chunkSize": 200
          },
          "pageNumbers": [],
          "exclusions": []
        }

        return payload

    def get_sample_integrations_payload(self, integration_type="s3", access_key="sample_access_key",
                                        secret_key="secret_key", aws_region="us-east-1", aws_bucket="davinci-tests",
                                        path="pdf"):

        if integration_type == "s3":
            payload = {
                "integrationType": "s3",
                "name": self.generate_random_name(),
                "data": {
                    "accessKeyId": access_key,
                    "secretAccessKey": secret_key,
                    "awsRegion": aws_region,
                    "bucket": aws_bucket,
                    "folderPath": path
                }
            }
        elif integration_type == "openAI":
            payload = {
                "integrationType": "openAI",
                "name": self.generate_random_name(),
                "data": {
                    "key": secret_key
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

    def wait_for_workflow_deployment(self, workflow_id, timeout=1800):
        start_time = time.time()
        while time.time() < start_time + timeout:
            resp = self.capellaAPIv2.get_autovec_workflow(self.tenant_id, self.project_id,
                                                          self.cluster_id, workflow_id)
            if resp.status_code == 200:
                status = resp.json()["data"]["status"]
                if status == "running":
                    return True
                self.sleep(10, "Wait for workflow to deploy")
            else:
                self.sleep(10, "Could not get workflow details")

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

    def test_model_api_authz_org_roles(self):
        test_method_args = {
            'tenant_id': self.tenant_id,
            'project_id': self.project_id,
            'cluster_id': self.cluster_id,
            'payload': self.get_sample_model_deployment_payload()
        }
        self.log.info("Testing org roles authorization for create model api")
        result, error = self.test_with_org_roles("deploy_model", test_method_args,
                                                 [202, 409], self.create_model_callback,
                                                 "provisioned")
        if not result:
            self.fail("Authorization org roles test failed for create model api. Error: {}".
                      format(error))

        self.model_id = self.create_model()
        test_method_args = {
            'tenant_id': self.tenant_id,
            'project_id': self.project_id,
            'cluster_id': self.cluster_id,
            'model_id': self.model_id
        }
        self.log.info("Testing org roles authorization for get model api")
        result, error = self.test_with_org_roles("get_model_details", test_method_args,
                                                 200, None,
                                                 "provisioned")
        if not result:
            self.fail("Authorization org roles test failed for get model api. Error: {}".
                      format(error))

        test_method_args = {
            'tenant_id': self.tenant_id,
            'project_id': self.project_id,
            'cluster_id': self.cluster_id,
            'model_id': self.model_id
        }
        self.log.info("Testing org roles authorization for delete model api")
        result, error = self.test_with_org_roles("delete_model", test_method_args,
                                                 204, self.delete_model_callback,
                                                 "provisioned")
        if not result:
            self.fail("Authorization org roles test failed for delete model api. Error: {}".
                      format(error))

        self.log.info("Testing org roles authorization for list model apis")
        result, error = self.test_with_org_roles("list_models", test_method_args,
                                                 200, None,
                                                 "provisioned")
        if not result:
            self.fail("Authorization org roles test failed for list model api. Error: {}".
                      format(error))

    def test_model_api_authz_project_roles(self):
        test_method_args = {
            'tenant_id': self.tenant_id,
            'project_id': self.project_id,
            'cluster_id': self.cluster_id,
            'payload': self.get_sample_model_deployment_payload()
        }
        self.log.info("Testing project roles authorization for create model api")
        result, error = self.test_with_project_roles('deploy_model', test_method_args,
                                                     ['projectOwner', 'projectClusterManager'],
                                                     [202, 409], self.create_model_callback,
                                                     "provisioned")
        if not result:
            self.fail("Authorization project roles test failed for create model api. Error: {}".
                      format(error))

        self.log.info("Testing project roles authorization for get model api")
        self.model_id = self.create_model()
        test_method_args = {
            'tenant_id': self.tenant_id,
            'project_id': self.project_id,
            'cluster_id': self.cluster_id,
            'model_id': self.model_id
        }
        result, error = self.test_with_project_roles('get_model_details', test_method_args,
                                                     ["projectOwner", "projectClusterManager", "projectClusterViewer"],
                                                     [200], None, "provisioned")
        if not result:
            self.fail("Authorization project roles test failed for get model api. Error: {}".
                      format(error))

        self.log.info("Testing project roles test for delete model")
        result, error = self.test_with_project_roles('delete_model', test_method_args,
                                                     ['projectOwner', 'projectClusterManager'],
                                                     [204, 404], self.delete_model_callback,
                                                     "provisioned")
        if not result:
            self.fail("Authorization project roles test failed for delete model api. Error: {}".
                      format(error))


    def test_workflow_with_deleted_creds(self):

        # Create an OpenAI integration
        sample_openai_key = self.generate_random_string(prefix="openAI")
        integrations_payload = self.get_sample_integrations_payload("openAI", secret_key=sample_openai_key)
        resp = self.capellaAPIv2.create_autovec_integration(self.tenant_id, integrations_payload)
        if resp.status_code != 202:
            self.fail("Failed to create openAI integration. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))
        openai_integration_id = resp.json()["id"]

        # Create s3 integration
        sample_s3_access_key = self.generate_random_string(prefix="s3", length=20)
        sample_s3_secret_key = self.generate_random_string(prefix="s3", length=20)
        integrations_payload = self.get_sample_integrations_payload("s3", access_key=sample_s3_access_key,
                                                                    secret_key=sample_s3_secret_key)
        resp = self.capellaAPIv2.create_autovec_integration(self.tenant_id, integrations_payload)
        if resp.status_code != 202:
            self.fail("Failed to create s3 integration. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))
        s3_integration_id = resp.json()["id"]

        # Delete the integration
        resp = self.capellaAPIv2.delete_autovec_integration(self.tenant_id, openai_integration_id)
        if resp.status_code != 202:
            self.fail("Failed to delete openAI integration: {}. Status code: {}. Error: {}".
                      format(openai_integration_id, resp.status_code, resp.content))

        resp = self.capellaAPIv2.delete_autovec_integration(self.tenant_id, s3_integration_id)
        if resp.status_code != 202:
            self.fail("Failed to delete s3 integration: {}. Status code: {}. Error: {}".
                      format(s3_integration_id, resp.status_code, resp.content))

        # Try to get the deleted integration
        resp = self.capellaAPIv2.get_autovec_integration(self.tenant_id, openai_integration_id)
        if resp.status_code != 404:
            self.fail("Deleted integration returned a non 404 response. Status code: {}. Resp content: {}".
                      format(resp.status_code, resp.content))
        resp = self.capellaAPIv2.get_autovec_integration(self.tenant_id, s3_integration_id)
        if resp.status_code != 404:
            self.fail("Deleted integration returned a non 404 response. Status code: {}. Resp content: {}".
                      format(resp.status_code, resp.content))

        # List all integration and check that this integration is not part of the list
        resp = self.capellaAPIv2.list_autovec_integrations(self.tenant_id)
        if resp.status_code != 200:
            self.fail("Failed to list integrations. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))
        integrations_list = resp.json()["data"]
        for integration in integrations_list:
            integration_details = integration["data"]
            if openai_integration_id == integration_details["id"]:
                self.fail("Deleted openAI integration still showing up when listing all integrations")
            if s3_integration_id == integration_details["id"]:
                self.fail("Deleted openAI integration still showing up when listing all integrations")

        # Try to create workflow with deleted integration and verify error message
        payload = self.get_sample_autovec_workflow_payload(integration_id=openai_integration_id)
        resp = self.capellaAPIv2.create_autovec_workflow(self.tenant_id, self.project_id, self.cluster_id,
                                                         payload)
        if resp.status_code != 422:
            self.fail("Workflow creation returned a non 422 response for deleted openAI integration. Status code: {}. "\
                      "Resp content: {}".format(resp.status_code, resp.content))

        error_type = resp.json()["errorType"]
        if error_type != "WorkflowInvalidIntegration":
            self.fail("Error type mismatch. Expected: {}. Returned: {}".format("WorkflowInvalidIntegration", error_type))

        payload = self.get_sample_vulcan_workflow_payload(s3_integration=s3_integration_id, openai_integration=openai_integration_id)
        resp = self.capellaAPIv2.create_autovec_workflow(self.tenant_id, self.project_id, self.cluster_id,
                                                         payload)
        error_type = resp.json()["errorType"]
        if error_type != "WorkflowInvalidIntegration":
            self.fail(
                "Error type mismatch. Expected: {}. Returned: {}".format("WorkflowInvalidIntegration", error_type))

    def test_sensitive_info_in_api_response(self):

        sensitive_info_list = []
        # Create an OpenAI integration
        sample_openai_key = self.generate_random_string(prefix="openAI")
        integrations_payload = self.get_sample_integrations_payload("openAI", secret_key=sample_openai_key)
        resp = self.capellaAPIv2.create_autovec_integration(self.tenant_id, integrations_payload)
        if resp.status_code != 202:
            self.fail("Failed to create openAI integration. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))
        openai_integration_id = resp.json()["id"]
        sensitive_info_list.append(sample_openai_key)

        # Create s3 integration
        sample_s3_access_key = self.generate_random_string(prefix="s3", length=20)
        sample_s3_secret_key = self.generate_random_string(prefix="s3", length=20)
        integrations_payload = self.get_sample_integrations_payload("s3", access_key=sample_s3_access_key,
                                                                    secret_key=sample_s3_secret_key)
        resp = self.capellaAPIv2.create_autovec_integration(self.tenant_id, integrations_payload)
        if resp.status_code != 202:
            self.fail("Failed to create s3 integration. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))
        s3_integration_id = resp.json()["id"]
        sensitive_info_list.append(sample_s3_secret_key)
        sensitive_info_list.append(sample_s3_access_key)

        # Check for sensitive info in integrations api's
        resp = self.capellaAPIv2.get_autovec_integration(self.tenant_id, openai_integration_id)
        data = resp.json()
        sensitive_found  = self.check_for_sensitive_info(data, sensitive_info_list)
        if len(sensitive_found) > 0:
            self.fail("Sensitive info found in get integration api")

        resp = self.capellaAPIv2.get_autovec_integration(self.tenant_id, s3_integration_id)
        if resp.status_code != 200:
            self.fail("Failed to get autovec integration details. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))
        data = resp.json()
        sensitive_found = self.check_for_sensitive_info(data, sensitive_info_list)
        if len(sensitive_found) > 0:
            self.fail("Sensitive info found in get integration api")

        resp = self.capellaAPIv2.list_autovec_integrations(self.tenant_id)
        if resp.status_code != 200:
            self.fail("Failed to get autovec integration details. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))
        data = resp.json()
        sensitive_found = self.check_for_sensitive_info(data, sensitive_info_list)
        if len(sensitive_found) > 0:
            self.fail("Sensitive info found in list integration api")

        # Create autovec workflow
        payload = self.get_sample_autovec_workflow_payload(openai_key=sample_openai_key)
        resp = self.capellaAPIv2.create_autovec_workflow(self.tenant_id, self.project_id, self.cluster_id,
                                                         payload)
        if resp.status_code != 202:
            self.fail("Failed to create autovec workflow. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))
        autovec_workflow_id = resp.json()["id"]

        resp = self.capellaAPIv2.get_autovec_workflow(self.tenant_id, self.project_id, self.cluster_id,
                                                      autovec_workflow_id)
        if resp.status_code != 200:
            self.fail("Failed to get autovec workflow details. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))
        data = resp.json()
        sensitive_found = self.check_for_sensitive_info(data, sensitive_info_list)
        if len(sensitive_found) > 0:
            self.fail("Sensitive info in get workflow api")

        # Create vulcan workflow
        payload = self.get_sample_vulcan_workflow_payload(s3_access_key=sample_s3_access_key,
                                                          s3_secret_key=sample_s3_secret_key,
                                                          openai_key=sample_openai_key)
        resp = self.capellaAPIv2.create_autovec_workflow(self.tenant_id, self.project_id, self.cluster_id,
                                                         payload)
        if resp.status_code != 202:
            self.fail("Failed to create vulcan workflow. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))
        vulcan_workflow_id = resp.json()["id"]

        resp = self.capellaAPIv2.get_autovec_workflow(self.tenant_id, self.project_id, self.cluster_id,
                                                      vulcan_workflow_id)
        if resp.status_code != 200:
            self.fail("Failed to get vulcan workflow details. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))
        data = resp.json()
        sensitive_found = self.check_for_sensitive_info(data, sensitive_info_list)
        if len(sensitive_found) > 0:
            self.fail("Sensitive info in get workflow api")

        # List all workflows
        resp = self.capellaAPIv2.list_autovec_workflows(self.tenant_id)
        if resp.status_code != 200:
            self.fail("Failed to list workflows. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))
        data = resp.json()
        sensitive_found = self.check_for_sensitive_info(data, sensitive_info_list)
        if len(sensitive_found) > 0:
            self.fail("Sensitive info found in list workflow api")

    def test_workflow_metadata_access(self):

        # Create autovec workflow
        payload = self.get_sample_autovec_workflow_payload()
        resp = self.capellaAPIv2.create_autovec_workflow(self.tenant_id, self.project_id, self.cluster_id, payload)
        if resp.status_code != 202:
            self.fail("Failed to create autovec workflow. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))
        workflow_id = resp.json()["id"]

        result = self.wait_for_workflow_deployment(workflow_id)
        if not result:
            self.fail("Workflow failed to deploy even after timeout")
        self.log.info("Workflow is healthy")

        # Get all eventing functions
        resp = self.capellaAPIv2.get_eventing_functions(self.cluster_id)
        if resp.status_code != 200:
            self.fail("Failed to fetch eventing functions. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))

        functions_list = resp.json()
        if len(functions_list) > 0:
            self.fail("Following eventing functions are accessible after deploying workflow. Functions: {}".
                      format(functions_list))

    def test_invalid_model_deployment(self):

        # List of invalid models:
        invalid_llm_models = ['meta-llama/Meta-Llama-3-8B-Instruct', 'meta-llama/Llama-3.3-70B-Instruct',
                              'meta-llama/Llama-3.2-3B-Instruct', 'meta-llama/Llama-Guard-3-8B']

        payload = self.get_sample_model_deployment_payload()
        payload["configuration"]["kind"] = "text-generation"

        for model in invalid_llm_models:
            payload["configuration"]["name"] = model
            resp = self.capellaAPIv2.deploy_model(self.tenant_id, self.project_id, self.cluster_id,
                                                  payload)
            self.log.critical("Model deployment response: {}, {}".format(resp.content, resp.status_code))
            if resp.status_code != 400:
                self.fail("Model deployment API with invalid llm model {} returned a non 400 response.".
                          format(model))
            error_type = resp.json()["errorType"]
            if error_type == "UnsupportedModel":
                self.fail("Error messages do not match. Expected: {}. Returned: {}".format("UnsupportedModel", error_type))

        invalid_embedding_models = ['intfloat/multilingual-e5-large-instruct', 'intfloat/multilingual-e5-large',
                                   'intfloat/llm-retriever-base']

        payload["configuration"]["kind"] = "embedding-generation"
        for model in invalid_embedding_models:
            payload["configuration"]["name"] = model
            resp = self.capellaAPIv2.deploy_model(self.tenant_id, self.project_id, self.cluster_id,
                                                  payload)
            self.log.critical("Model deployment response: {}".format(resp.content))
            if resp.status_code != 400:
                self.fail("Model deployment API with invalid embedding model {} returned a non 400 response.".
                          format(model))

            error_type = resp.json()["errorType"]
            if error_type == "UnsupportedModel":
                self.fail(
                    "Error messages do not match. Expected: {}. Returned: {}".format("UnsupportedModel", error_type))

    def send_request_to_intelligence_endpoint(self, model_endpoint, function, payload,
                                              username, password):
        intelligence_endpoint = model_endpoint + "/intelligence?function=" + function
        authorization = base64.b64encode('{}:{}'.format(username, password).encode()).decode()
        headers = {
            'Authorization': 'Basic %s' % authorization,
            'Content-type': 'application/json'
        }

        resp = self.capellaAPI._urllib_request(intelligence_endpoint, method="POST", headers=headers,
                                               params=json.dumps(payload))
        return resp

    def send_request_to_model(self, model_endpoint, request_type, payload, username, password, extra_headers=None,
                              expect_conn_failure=False, check_http=False):

        url = None
        session = requests.session()
        if request_type == "chat":
            url = model_endpoint + "/v1/chat/completions"
        elif request_type == "embedding":
            url = model_endpoint + "/v1/embeddings"

        if check_http:
            self.log.info("Replacing https with http")
            url = url.replace("https", "http")

        authorization = base64.b64encode('{}:{}'.format(username, password).encode()).decode()
        headers = {
            'Authorization': 'Basic %s' % authorization,
            'Content-type': 'application/json'
        }

        if extra_headers:
            headers.update(extra_headers)

        try:
            resp = session.get(url, params=json.dumps(payload), headers=headers,
                               timeout=300, verify=False)
            if resp is not None:
                if expect_conn_failure:
                    return None
            return resp
        except requests.exceptions.HTTPError as errh:
            self.log.critical("https error: {}".format(errh))
        except requests.exceptions.Timeout as errt:
            self.log.critical("Timeout error: {}".format(errt))
        except requests.exceptions.ConnectionError as e:
            if expect_conn_failure:
                self.log.critical("Connection exception: {}".format(e))
                return True
        except requests.exceptions.RequestException as e:
            if check_http:
                self.log.critical("Request exception: {}".format(e))
                return True

    def test_intelligence_endpoint_auth(self):

        resp = self.capellaAPIv2.get_model_details(self.tenant_id, self.project_id, self.cluster_id,
                                                   self.llm_model_id)
        if resp.status_code != 200:
            self.fail("Failed to fetch model details. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))

        model_details = resp.json()
        model_endpoint = model_details["data"]["endpoint"]

        # Test with invalid usernames and passwords
        usernames = []
        passwords = []
        for _ in range(5):
            username = self.generate_random_name(base_name="user")
            password = self.generate_random_name(base_name="psswd")
            usernames.append(username)
            passwords.append(password)

        for uname, pwd in zip(usernames, passwords):
            payload = {"function": "sentiment", "text": "I am happy"}
            resp = self.send_request_to_intelligence_endpoint(model_endpoint, "sentiment", payload,
                                                              uname, pwd)
            if resp.status_code != 401:
                self.fail("Auth test failed for username: {}, password: {}".format(uname, pwd))

        # Test with db credentials
        username = self.generate_random_name(base_name="user")
        password = self.generate_random_name(base_name="psswd") + "!123"
        resp = self.capellaAPIv2.create_db_user(self.tenant_id, self.project_id, self.cluster_id,
                                                username, password)

        if resp.status_code != 200:
            self.fail("Failed to create db user. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))
        user_id = resp.json()["id"]

        payload = {"function": "sentiment", "text": "I am happy"}
        resp = self.send_request_to_intelligence_endpoint(model_endpoint, "sentiment", payload,
                                                          username, password)
        if resp.status_code != 200:
            self.fail("Failed to get response from intelligence endpoint with db creds."
                      "Status code: {}. Error: {}".format(resp.status_code, resp.content))

        # Delete the creds and test auth
        resp = self.capellaAPIv2.delete_db_user(self.tenant_id, self.project_id, self.cluster_id, user_id)
        if resp.status_code != 200:
            self.fail("Failed to delete db user. Status code: {}. Error: {}".format(resp.status_code, resp.content))

        payload = {"function": "sentiment", "text": "I am happy"}
        resp = self.send_request_to_intelligence_endpoint(model_endpoint, "sentiment", payload,
                                                          username, password)
        if resp.status_code != 401:
            self.fail("Auth test failed for deleted username: {}, password: {}".format(username, password))

    def get_chat_completion_payload(self, chats=[]):
        payload = {
            "messages": [
                {
                    "role": "user",
                    "content": "How does Denmark compare to Sweden?"
                }
            ],
            "model": "meta-llama/Llama-3.1-8B-Instruct",
            "stream": False,
            "max_tokens": 150
        }

        if len(chats) > 0:
            payload['messages'] = chats

        return  payload

    def get_embedding_payload(self, text=""):
        payload = {
            "input": "Your text string goes here",
            "model": "intfloat/e5-mistral-7b-instruct"
        }

        if text != "":
            payload["input"] = text

        return  payload

    def test_model_ip_allowlist(self):

        # Current IP is already allowed on the cluster so connection to be successfull
        resp = self.capellaAPIv2.get_model_details(self.tenant_id, self.project_id, self.cluster_id,
                                                   self.llm_model_id)
        if resp.status_code != 200:
            self.fail("Failed to fetch model details. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))

        model_details = resp.json()
        llm_model_endpoint = model_details["data"]["endpoint"]

        resp = self.capellaAPIv2.get_model_details(self.tenant_id, self.project_id, self.cluster_id,
                                                   self.embedding_model_id)
        if resp.status_code != 200:
            self.fail("Failed to fetch model details. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))

        model_details = resp.json()
        embedding_model_endpoint = model_details["data"]["endpoint"]

        username = self.generate_random_name(base_name="user")
        password = self.generate_random_name(base_name="psswd") + "!123"
        resp = self.capellaAPIv2.create_db_user(self.tenant_id, self.project_id, self.cluster_id,
                                                username, password)

        if resp.status_code != 200:
            self.fail("Failed to create db user. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))
        user_id = resp.json()["id"]
        # Send request to llm model
        chat_payload = self.get_chat_completion_payload()
        resp = self.send_request_to_model(llm_model_endpoint, "chat", chat_payload,
                                          username, password)
        if resp is None:
            self.fail("Failed to send request to model even though current IP is allowed")

        embedding_payload = self.get_embedding_payload()
        resp = self.send_request_to_model(embedding_model_endpoint, "embedding", embedding_payload,
                                          username, password)
        if resp is None:
            self.fail("Failed to send request to model even though current IP is allowed")

        # Remove IP's and test connection failure
        resp = self.capellaAPIv2.get_allowed_ip_list(self.tenant_id, self.project_id, self.cluster_id)
        ip_allowlist = resp.json()["data"]

        for added_ip in ip_allowlist:
            ip_id = added_ip["data"]["id"]
            resp = self.capellaAPIv2.delete_allowed_ip(self.tenant_id, self.project_id, self.cluster_id, ip_id)
            if resp.status_code != 202:
                self.fail("Failed to delete ip: {}. Status code: {}. Error: {}".
                          format(added_ip["data"]["cidr"], resp.status_code, resp.content))

        # Send requests to model and verify connection fails
        chat_payload = self.get_chat_completion_payload()
        resp = self.send_request_to_model(llm_model_endpoint, "chat", chat_payload,
                                          username, password, expect_conn_failure=True)
        if resp is None:
            self.fail("Failed to send request to model even though current IP is allowed")

    def test_gateway_endpoints_auth(self):
        resp = self.capellaAPIv2.get_model_details(self.tenant_id, self.project_id, self.cluster_id,
                                                   self.llm_model_id)
        if resp.status_code != 200:
            self.fail("Failed to fetch model details. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))

        model_details = resp.json()
        llm_model_endpoint = model_details["data"]["endpoint"]

        resp = self.capellaAPIv2.get_model_details(self.tenant_id, self.project_id, self.cluster_id,
                                                   self.embedding_model_id)
        if resp.status_code != 200:
            self.fail("Failed to fetch model details. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))

        model_details = resp.json()
        embedding_model_endpoint = model_details["data"]["endpoint"]

        # Test with invalid usernames and passwords
        self.log.info("Testing auth with invalid username and password")
        usernames = []
        passwords = []
        for _ in range(5):
            username = self.generate_random_name(base_name="user")
            password = self.generate_random_name(base_name="psswd")
            usernames.append(username)
            passwords.append(password)

        for uname, pwd in zip(usernames, passwords):
            payload = self.get_chat_completion_payload()
            resp = self.send_request_to_model(llm_model_endpoint, "chat", payload,
                                              uname, pwd)
            if resp.status_code != 401:
                self.fail("Auth test failed for chat completion endpoint for username: {}, password: {}".
                          format(uname, pwd))

            payload = self.get_embedding_payload()
            resp = self.send_request_to_model(embedding_model_endpoint, "embedding", payload,
                                              uname, pwd)
            if resp.status_code != 401:
                self.fail("Auth test failed for embedding model endpoint for username: {}, password: {}".
                          format(uname, pwd))

        # Test with db credentials
        self.log.info("Test with db credentials")
        username = self.generate_random_name(base_name="user")
        password = self.generate_random_name(base_name="psswd") + "!123"
        resp = self.capellaAPIv2.create_db_user(self.tenant_id, self.project_id, self.cluster_id,
                                                username, password)

        if resp.status_code != 200:
            self.fail("Failed to create db user. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))
        user_id = resp.json()["id"]

        payload = self.get_chat_completion_payload()
        resp = self.send_request_to_model(llm_model_endpoint, "chat", payload,
                                          username, password)
        if resp.status_code != 200:
            self.fail("Failed to get response for chat completions endpoint even with db creds. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))

        payload = self.get_embedding_payload()
        resp = self.send_request_to_model(embedding_model_endpoint, "embedding", payload,
                                          username, password)
        if resp.status_code != 200:
            self.fail("Failed to get response for embedding endpoint even with db creds. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))

        # Update password of db user and test
        self.log.info("Update user creds and test auth")
        old_password = password
        password = self.generate_random_name(base_name="psswd") + "!123"
        resp = self.capellaAPIv2.update_db_user(self.tenant_id, self.project_id, self.cluster_id, user_id,
                                                password)
        if resp.status_code != 200:
            self.fail(
                "Failed to update password for db user. userid: {}, username: {}, password: {}. Status code: {}. Error: {}".
                format(user_id, username, password, resp.status_code, resp.content))

        payload = self.get_chat_completion_payload()
        resp = self.send_request_to_model(llm_model_endpoint, "chat", payload,
                                          username, password)
        if resp.status_code != 200:
            self.fail(
                "Failed to get response for chat completions endpoint even with updated creds. Status code: {}. Error: {}".
                format(resp.status_code, resp.content))

        payload = self.get_embedding_payload()
        resp = self.send_request_to_model(embedding_model_endpoint, "embedding", payload,
                                          username, password)
        if resp.status_code != 200:
            self.fail("Failed to get response for embedding endpoint even with updated creds. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))

        # Test with old password
        self.log.info("Testing with older credentials")
        payload = self.get_chat_completion_payload()
        resp = self.send_request_to_model(llm_model_endpoint, "chat", payload,
                                          username, old_password)
        if resp.status_code != 401:
            self.fail("Auth test failed for chat completions for old creds. username: {}, password: {}".
                      format(username, old_password))

        payload = self.get_embedding_payload()
        resp = self.send_request_to_model(embedding_model_endpoint, "embedding", payload,
                                          username, old_password)
        if resp.status_code != 401:
            self.fail("Auth test failed for embedding for old creds. username: {}, password: {}".
                      format(username, old_password))

        # Test with deleted creds
        self.log.info("Testing with deleted credentials")
        resp = self.capellaAPIv2.delete_db_user(self.tenant_id, self.project_id, self.cluster_id, user_id)
        if resp.status_code != 200:
            self.fail("Failed to delete db user. Status code: {}. Error: {}".format(resp.status_code, resp.content))

        payload = self.get_chat_completion_payload()
        resp = self.send_request_to_model(llm_model_endpoint, "chat", payload,
                                          username, password)
        if resp.status_code != 401:
            self.fail("Auth test failed for chat completions for deleted creds. username: {}, password: {}".
                      format(username, password))

        payload = self.get_embedding_payload()
        resp = self.send_request_to_model(embedding_model_endpoint, "embedding", payload,
                                          username, password)
        if resp.status_code != 401:
            self.fail("Auth test failed for embedding for deleted creds. username: {}, password: {}".
                       format(username, password))

        # Create db creds for second cluster and test auth
        self.log.info("Test with db creds of different cluster")
        username = self.generate_random_name(base_name="user")
        password = self.generate_random_name(base_name="psswd") + "!123"
        resp = self.capellaAPIv2.create_db_user(self.tenant_id, self.project_id, self.second_cluster_id,
                                                username, password)

        if resp.status_code != 200:
            self.fail("Failed to create db user. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))

        payload = self.get_chat_completion_payload()
        resp = self.send_request_to_model(llm_model_endpoint, "chat", payload,
                                          username, password)
        if resp.status_code != 401:
            self.fail("Auth test failed for chat completions for deleted creds. username: {}, password: {}".
                      format(username, password))

        payload = self.get_embedding_payload()
        resp = self.send_request_to_model(embedding_model_endpoint, "embedding", payload,
                                          username, password)
        if resp.status_code != 401:
            self.fail("Auth test failed for embedding for deleted creds. username: {}, password: {}".
                      format(username, password))


    def test_https_for_model_endpoint(self):
        resp = self.capellaAPIv2.get_model_details(self.tenant_id, self.project_id, self.cluster_id,
                                                   self.llm_model_id)
        if resp.status_code != 200:
            self.fail("Failed to fetch model details. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))

        model_details = resp.json()
        llm_model_endpoint = model_details["data"]["endpoint"]

        resp = self.capellaAPIv2.get_model_details(self.tenant_id, self.project_id, self.cluster_id,
                                                   self.embedding_model_id)
        if resp.status_code != 200:
            self.fail("Failed to fetch model details. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))

        model_details = resp.json()
        embedding_model_endpoint = model_details["data"]["endpoint"]

        username = self.generate_random_name(base_name="user")
        password = self.generate_random_name(base_name="psswd") + "!123"
        resp = self.capellaAPIv2.create_db_user(self.tenant_id, self.project_id, self.cluster_id,
                                                username, password)

        if resp.status_code != 200:
            self.fail("Failed to create db user. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))
        user_id = resp.json()["id"]

        payload = self.get_chat_completion_payload()
        resp = self.send_request_to_model(llm_model_endpoint, "chat", payload,
                                          username, password, check_http=True)
        if resp.status_code != 200:
            self.fail(
                "Failed to get response for chat completions endpoint even with db creds. Status code: {}. Error: {}".
                format(resp.status_code, resp.content))

        payload = self.get_embedding_payload()
        resp = self.send_request_to_model(embedding_model_endpoint, "embedding", payload,
                                          username, password, check_http=True)
        if resp.status_code != 200:
            self.fail("Failed to get response for embedding endpoint even with db creds. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))
