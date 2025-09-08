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
            self.cidr = "0.0.0.0/0"
            SecurityBase.setUp(self)
            self.integration_id = None
            self.workflow_id = None
            self.openai_api_key = self.input.capella.get("openai_api_key", "sk-it-operations-jay-goutham")
            self.llm_model_id = self.input.capella.get("llm_model_id", None)
            self.embedding_model_id = self.input.capella.get("embedding_model_id", None)
            self.clean_model_ids = []

            self.deployable_llm_model_catalog_id = self.get_deployable_model_catalog_id("meta/llama-3.1-8b-instruct")
            self.deployable_embedding_model_catalog_id = self.get_deployable_model_catalog_id("snowflake/arctic-embed-l")

            if self.llm_model_id is None:
                self.llm_model_id = self.create_model(model_type="text")
                self.clean_model_ids.append(self.llm_model_id)
                self.wait_for_model_deployment(model_id=self.llm_model_id)
            if self.embedding_model_id is None:
                self.embedding_model_id = self.create_model(model_type="embedding")
                self.clean_model_ids.append(self.embedding_model_id)
                self.wait_for_model_deployment(model_id=self.embedding_model_id)

            # load travel sample bucket
            res = self.capellaAPIv2.load_sample_bucket(self.tenant_id, self.project_id,
                                                       self.cluster_id,
                                                       "travel-sample")
            if res.status_code != 201:
                self.fail("Failed to load travel sample bucket." \
                          "Status code: {}, Error: {}".format(res.status_code, res.content))

        except Exception as e:
            self.fail("Base Setup Failed with error as - {}".format(e))


    def tearDown(self):

        resp = self.capellaAPIv2.list_autovec_workflows(self.tenant_id)
        workflow_ids = []

        for model_id in self.clean_model_ids:
            self.log.info("Deleting model: {}".format(model_id))
            resp = self.capellaAPIv2.delete_model(self.tenant_id, model_id)
            if resp.status_code != 202:
                self.log.error("Failed to delete model: {}. Reason: {}".format(model_id,
                                                                              resp.content))

        if resp.status_code == 200:
            data = json.loads(resp.content)["data"]
            for workflow in data:
                cluster_id = workflow["data"]["clusterId"]
                if cluster_id == self.cluster_id:
                    workflow_ids.append(workflow["data"]["id"])

        if not workflow_ids:
            self.log.info("No workflows found for deletion in cluster: {}".format(self.cluster_id))
        else:
            self.log.info("Found {} workflow(s) to delete for cluster: {}".format(len(workflow_ids), self.cluster_id))

        for workflow_id in workflow_ids:
            self.log.info("Deleting workflow: {}".format(workflow_id))
            
            # First check if workflow is still deploying, wait before cleanup
            self.wait_for_workflow_deployment(workflow_id, acceptable_statuses=["completed", "failed"])  
            
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


    def get_sample_model_deployment_payload(self, model_catalog_id = None, type="embedding"):

        if type == "embedding":
            payload = {
                "name": self.generate_random_name("embeddingModel"),
                "modelCatalogId": model_catalog_id if model_catalog_id else self.deployable_embedding_model_catalog_id,
                "config": {
                    "provider": "hostedAWS",
                    "region": "us-east-1",
                    "multiAZ": False,
                    "compute": {
                        "instanceType": "g6.xlarge",
                        "instanceCount": 1
                    }
                },
                "parameters": {
                    "tuning": {
                        "quantization": "full-precision",
                        "optimization": "throughput",
                        "dimensions": 4096
                    }
                }
            }
        elif type == "text":
            payload = {
                "name": self.generate_random_name("textModel"),
                "modelCatalogId": model_catalog_id if model_catalog_id else self.deployable_llm_model_catalog_id,
                "config": {
                    "provider": "hostedAWS",
                    "region": "us-east-1",
                    "multiAZ": False,
                    "compute": {
                        "instanceType": "g6.xlarge",
                        "instanceCount": 1
                    }
                }
            }

        return payload


    def get_sample_autovec_workflow_payload(self, integration_id=None):

        if not integration_id:
            openai_integration_payload = self.get_sample_integrations_payload(integration_type="openAI", secret_key=self.openai_api_key)
            integration_id = self.create_autovec_integration(payload=openai_integration_payload)

        openai_payload = {
            "id": integration_id,
            "modelName": "text-embedding-3-small",
            "provider": "openAI"
        }

        payload = {
            "type": "structured",
            "createIndexes": True,
            "embeddingFieldMappings": {
                "vectorEmbeddingField1": {
                    "sourceFields": ["country"],
                    "encodingFormat": "float"
                }
            },
            "embeddingModel": {
                "external": openai_payload
            },
            "cbKeyspace": {
                "bucket": "travel-sample",
                "scope": "inventory",
                "collection": "airline"
            },
            "name": self.generate_random_name("flow")
        }

        return payload


    def get_sample_vulcan_workflow_payload(self, s3_access_key=None, s3_secret_key=None,
                                           s3_region="us-east-1", s3_path="pdf", s3_bucket="davinci-tests",
                                           s3_integration=None, openai_integration=None, openai_key=None): 
        
        # S3 integration for the data source
        if s3_integration is None:
            s3_integration_paylod = self.get_sample_integrations_payload(integration_type="s3", access_key=s3_access_key, secret_key=s3_secret_key, aws_region=s3_region, aws_bucket=s3_bucket, path=s3_path)
            s3_integration_id = self.create_autovec_integration(payload=s3_integration_paylod)
        else:
            s3_integration_id = s3_integration
        
        data_source = {
            "id": s3_integration_id
        }

        # OpenAI integration for the embedding model
        if openai_integration is None:
            secret_key = openai_key if openai_key else self.openai_api_key
            openai_integration_payload = self.get_sample_integrations_payload(integration_type="openAI", secret_key=secret_key)
            openai_integration_id = self.create_autovec_integration(payload=openai_integration_payload)
        else:
            openai_integration_id = openai_integration

        openai_payload = {
            "id": openai_integration_id,
            "modelName": "text-embedding-3-large",
            "provider": "openAI"
        }

        payload = {
          "name": self.generate_random_name("vulcan"),
          "type": "unstructured",
          "createIndexes": True,
          "cbKeyspace": {
            "bucket": "travel-sample",
            "scope": "_default",
            "collection": "_default"
          },
          "embeddingModel": {
            "external": openai_payload
          },
          "dataSource": data_source,
          "chunkingStrategy": {
            "strategyType": "RECURSIVE_SPLITTER",
            "chunkSize": 300,
            "chunkOverlap": 50
          },
          "pageNumbers": [],
          "exclusions": []
        }
        
        return payload


    def get_sample_integrations_payload(self, integration_type="s3", access_key="sample_access_key",
                                        secret_key="secret_key", aws_region="us-east-1", aws_bucket="davinci-tests",
                                        path="pdf", session_token=None):

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
            if session_token:
                payload["data"]["sessionToken"] = session_token
        elif integration_type == "openAI":
            payload = {
                "integrationType": "openAI",
                "name": self.generate_random_name(),
                "data": {
                    "key": secret_key
                }
            }
        elif integration_type == "bedrock":
            payload = {
                "integrationType": "bedrock",
                "name": self.generate_random_name(),
                "data": {
                    "accessKeyId": access_key,
                    "secretAccessKey": secret_key
                }
            }

        return payload

    def get_deployable_model_catalog_id(self, model_name):
        model_deployment_options = self.capellaAPIv2.get_model_deployment_options(tenant_id=self.tenant_id)

        if model_deployment_options.status_code != 200:
            self.fail("Failed to get model deployment options for model {}, resp: {}".format(model_name, model_deployment_options.text))

        for model_deployment_option in model_deployment_options.json()["data"]:
            self.log.info("Getting model deployment options for model {}".format(model_deployment_option))
            if model_deployment_option["modelName"] == model_name:
                return model_deployment_option["id"]

        return None

    def wait_for_model_deletion(self, model_id, timeout=1800):
        start_time = time.time()
        
        while time.time() < start_time + timeout:
            resp = self.capellaAPIv2.get_model_details(self.tenant_id, model_id)
            if resp.status_code == 404:
                return True
            self.sleep(10, "Wait for model deletion")
            
        return False


    def wait_for_model_deployment(self, model_id, timeout=60 * 60):
        start_time = time.time()
        
        while time.time() < start_time + timeout:
            resp = self.capellaAPIv2.get_model_details(self.tenant_id, model_id)
            if resp.status_code == 200 and resp.json()["data"]["modelConfig"]["status"] == "healthy":
                return True
            self.sleep(10, "Wait for model deployment")

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


    def wait_for_workflow_deployment(self, workflow_id, acceptable_statuses=["completed"], timeout=1800):
        
        start_time = time.time()
        while time.time() < start_time + timeout:
            resp = self.capellaAPIv2.get_autovec_workflow(self.tenant_id, self.project_id,
                                                          self.cluster_id, workflow_id)
            if resp.status_code == 200:
                workflow_runs = resp.json()["data"]["workflowRuns"]
                if workflow_runs and len(workflow_runs) > 0:
                    latest_run = workflow_runs[0]  
                    status = latest_run["status"]
                    if status in acceptable_statuses:
                        return True
                self.sleep(10, "Waiting for workflow to deploy")
            else:
                self.sleep(10, "Could not get workflow details")

        return False


    def create_model(self, model_type="embedding"):
        resp = self.capellaAPIv2.deploy_model(self.tenant_id, self.get_sample_model_deployment_payload(type=model_type))
        self.log.info("Model deployment response: {}".format(resp.text))

        if resp.status_code != 202:
            self.fail("Failed to create model. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))

        return resp.json()["id"]


    def create_model_apikey(self, allowed_ips):
        resp = self.capellaAPIv2.create_model_api_key(tenant_id=self.tenant_id,
                                                      payload=self.get_model_apikey_create_payload(allowed_ips=allowed_ips))

        if resp.status_code != 201:
            self.fail("Failed to create model api key. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))

        model_apikey_response = resp.json()
        self.log.info("Created model api key. Model api key: {}".format(model_apikey_response))
        api_key = model_apikey_response["apiKey"]
        api_key_id = model_apikey_response["keyId"]
        return api_key_id, api_key


    def create_autovec_workflow(self):

        resp = self.capellaAPIv2.create_autovec_workflow(self.tenant_id, self.project_id,
                                                         self.cluster_id, self.get_sample_autovec_workflow_payload())
        if resp.status_code != 202:
            self.fail("Failed to create autovec workflow. Status code: {}, Error: {}".
                      format(resp.status_code, resp.content))

        return resp.json()["id"]


    def create_autovec_integration(self, payload=None):

        resp = self.capellaAPIv2.create_autovec_integration(self.tenant_id, payload=payload if payload is not None else self.get_sample_integrations_payload())
        if resp.status_code != 202:
            return None

        return resp.json()["id"]


    def create_model_callback(self, resp, test_method_args=None):
        model_id = resp.json()["id"]
        res = self.capellaAPIv2.delete_model(self.tenant_id, model_id)
        if res.status_code != 202:
            self.fail("Failed to delete model: {}. Status code: {}. Error: {}".
                      format(model_id, res.status_code, res.content))
        self.log.info("Model deleted")


    def create_autovec_workflow_callback(self, resp, test_method_args=None):

        workflow_id = resp.json()["id"]
        # Wait till the workflow is healthy/completed/failed
        deployment_result = self.wait_for_workflow_deployment(workflow_id, acceptable_statuses=["completed"])
        if not deployment_result:
            self.fail("Workflow {} failed to deploy even after timeout".format(workflow_id))
        self.log.info("Workflow {} is healthy, proceeding with deletion".format(workflow_id))
        res = self.capellaAPIv2.delete_autovec_workflow(self.tenant_id, self.project_id,
                                                        self.cluster_id, workflow_id)
        if res.status_code != 202:
            self.fail("Failed to delete autovec workflow: {}. Status code: {}, Error: {}".
                      format(workflow_id, res.status_code, res.content))
        # Wait for the workflow deletion to complete.
        deletion_result = self.wait_for_workflow_deletion(workflow_id)
        if not deletion_result:
            self.fail("Failed to delete autovec workflow {} even after timeout".format(workflow_id))
        self.log.info("Workflow {} deleted".format(workflow_id))

        # Wait for secrets manager cleanup to complete after deletion before the next test creates a new workflow
        self.sleep(10, "Allow secrets manager cleanup to complete")


    def delete_model_callback(self, resp, test_method_args=None):
        if resp.status_code != 202:
            self.fail("Failed to delete model, resp: {}, status_code: {}".format(resp.text, resp.status_code))

        self.model_id = self.create_model()
        self.log.info("Created model: {}".format(self.model_id))


    def delete_autovec_workflow_callback(self, resp, test_method_args=None):

        # Include workflow_id from test_method_args if provided.
        workflow_id_to_delete = test_method_args.get('workflow_id') if test_method_args else self.workflow_id
        if resp.status_code != 202:
            self.fail("Failed to delete workflow {}".format(workflow_id_to_delete))
        res = self.wait_for_workflow_deletion(workflow_id_to_delete)
        if res:
            self.log.info("Workflow {} deleted".format(workflow_id_to_delete))
        else:
            self.fail("Failed to delete workflow {} even after timeout".format(workflow_id_to_delete))

        # Wait for secrets manager cleanup to complete before creating new workflow
        self.sleep(10, "Allow secrets manager cleanup to complete")

        self.workflow_id = self.create_autovec_workflow()
        self.log.info("Created workflow: {}".format(self.workflow_id))

        # Wait for the new workflow to be ready
        self.log.info("Waiting for newly created workflow {} to be ready".format(self.workflow_id))
        if not self.wait_for_workflow_deployment(self.workflow_id, acceptable_statuses=["completed"]):
            self.fail("Newly created workflow {} failed to deploy even after timeout".format(self.workflow_id))

        # Update test_method_args with the new workflow_id for subsequent tests.
        if test_method_args:
            test_method_args['workflow_id'] = self.workflow_id


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

        # Wait for workflow to be ready before running authorization tests
        self.log.info("Waiting for workflow {} to be ready for authorization tests".format(self.workflow_id))
        if not self.wait_for_workflow_deployment(self.workflow_id, acceptable_statuses=["completed"]):
            self.fail("Workflow {} failed to deploy even after timeout".format(self.workflow_id))

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

        test_method_args = {}
        self.log.info("Test tenant ids authorization for list autovec workflow")
        result, error = self.test_tenant_ids(self.capellaAPIv2.list_autovec_workflows, test_method_args,
                                             'tenant_id', 200)
        if not result:
            self.fail("Authorization tenant ids test failed for list autovec workflow. Error: {}".
                      format(error))

        test_method_args = {
            'project_id': self.project_id,
            'cluster_id': self.cluster_id,
            'workflow_id': self.workflow_id
        }
        self.log.info("Testing tenant ids authorization for delete autovec workflow")
        result, error = self.test_tenant_ids(self.capellaAPIv2.delete_autovec_workflow, test_method_args,
                                             'tenant_id', 202,
                                             self.delete_autovec_workflow_callback)
        if not result:
            self.fail("Authorization tenant ids test failed for delete autovec workflow. Error: {}".
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

        # Wait for workflow to be ready before running authorization tests
        self.log.info("Waiting for workflow {} to be ready for authorization tests".format(self.workflow_id))
        if not self.wait_for_workflow_deployment(self.workflow_id, acceptable_statuses=["completed"]):
            self.fail("Workflow {} failed to deploy even after timeout".format(self.workflow_id))

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

        # Wait for workflow to be ready before running authorization tests
        self.log.info("Waiting for workflow {} to be ready for authorization tests".format(self.workflow_id))
        if not self.wait_for_workflow_deployment(self.workflow_id, acceptable_statuses=["completed"]):
            self.fail("Workflow {} failed to deploy even after timeout".format(self.workflow_id))

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

        test_method_args = {
            'tenant_id': self.tenant_id
        }
        self.log.info("Testing org roles authorization for list autovec workflow")
        result, error = self.test_with_org_roles("list_autovec_workflows", test_method_args,
                                                 200, None, "provisioned")
        if not result:
            self.fail("Authorization org roles test failed for list autovec workflow. Error: {}".
                      format(error))

        test_method_args = {
            'tenant_id': self.tenant_id,
            'project_id': self.project_id,
            'cluster_id': self.cluster_id,
            'workflow_id': self.workflow_id
        }

        self.log.info("Testing org roles authorization for delete autovec workflow")
        result, error = self.test_with_org_roles("delete_autovec_workflow", test_method_args,
                                                  202, self.delete_autovec_workflow_callback, "provisioned")
        if not result:
            self.fail("Authorization org roles test failed for delete autovec workflow. Error: {}".
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

        # Wait for workflow to be ready before running authorization tests
        self.log.info("Waiting for workflow {} to be ready for authorization tests".format(self.workflow_id))
        if not self.wait_for_workflow_deployment(self.workflow_id, acceptable_statuses=["completed"]):
            self.fail("Workflow {} failed to deploy even after timeout".format(self.workflow_id))

        test_method_args = {
            'tenant_id': self.tenant_id,
            'project_id': self.project_id,
            'cluster_id': self.cluster_id,
            'workflow_id': self.workflow_id
        }
        self.log.info("Testing project roles authorization for get autovec workflow")
        result, error = self.test_with_project_roles("get_autovec_workflow", test_method_args,
                                                     ["projectOwner", "projectClusterManager", "projectClusterViewer",
                                                      "projectDataWriter", "projectDataViewer"], 200,
                                                     None, "provisioned")
        if not result:
            self.fail("Authorization project roles test failed for get autovec workflow. Error: {}".
                      format(error))

        test_method_args = {
            'tenant_id': self.tenant_id
        }
        self.log.info("Testing project roles authorization for list autovec workflow")
        result, error = self.test_with_project_roles("list_autovec_workflows", test_method_args,
                                                     ["projectOwner", "projectClusterManager", "projectClusterViewer", "projectDataWriter", "projectDataViewer"], 200,
                                                     None, "provisioned")
        if not result:
            self.fail("Authorization project roles test failed for list autovec workflow")

        test_method_args = {
            'tenant_id': self.tenant_id,
            'project_id': self.project_id,
            'cluster_id': self.cluster_id,
            'workflow_id': self.workflow_id
        }

        self.log.info("Testing project roles authorization for delete autovec workflow")
        result, error = self.test_with_project_roles("delete_autovec_workflow", test_method_args,
                                                     ["projectOwner", "projectClusterManager"], 202,
                                                     self.delete_autovec_workflow_callback, "provisioned")
        if not result:
            self.fail("Authorization project roles test failed for delete autovec workflow. Error: {}".
                      format(error))
    

    def test_model_api_auth(self):
        url_method_map = {}
        model_url = "{}/v2/organizations/{}/languagemodels". \
            format(self.capellaAPIv2.internal_url, self.tenant_id)
        url_method_map[model_url] = ["POST"]

        specific_model_url = "{}/v2/organizations/{}/languagemodels/{}". \
            format(self.capellaAPIv2.internal_url, self.tenant_id, self.invalid_id)
        url_method_map[specific_model_url] = ["GET", "DELETE"]

        list_model_url = "{}/v2/organizations/{}/languagemodels?page={}&perPage={}". \
            format(self.capellaAPIv2.internal_url, self.tenant_id, 1, 10)
        url_method_map[list_model_url] = ["GET"]

        for url, test_methods in url_method_map.items():
            self.log.info("Testing auth for url: {}".format(url))
            for method in test_methods:
                result, error = self.test_authentication(url, method)
                if not result:
                    self.fail("Auth test failed for method: {}, url: {}, error: {}".
                              format(method, url, error))


    def test_model_api_tenant_ids(self):
        test_method_args = {
            'payload': self.get_sample_model_deployment_payload()
        }
        self.log.info("Testing tenant ids authorization for create model api")
        result, error = self.test_tenant_ids(self.capellaAPIv2.deploy_model, test_method_args,
                                             'tenant_id', [202, 409],
                                             self.create_model_callback, [404])
        if not result:
            self.fail("Authorization tenant ids test failed for create model api. Error: {}".
                      format(error))

        self.model_id = self.create_model()
        test_method_args = {
            'model_id': self.model_id
        }
        self.log.info("Testing tenant ids authorization for get model api")
        result, error = self.test_tenant_ids(self.capellaAPIv2.get_model_details, test_method_args,
                                             'tenant_id', 200, None,
                                             [404])
        if not result:
            self.fail("Authorization tenant ids test failed for get model api. Error: {}".
                      format(error))

        test_method_args = {
            'model_id': self.model_id
        }
        self.log.info("Testing tenant ids authorization for delete model api")
        result, error = self.test_tenant_ids(self.capellaAPIv2.delete_model, test_method_args,
                                             'tenant_id', 202,
                                             self.delete_model_callback, [404])
        if not result:
            self.fail("Authorization tenant ids failed for delete model api. Error: {}".
                      format(error))

        test_method_args = {}
        self.log.info("Testing tenant ids authorization for list model api")
        result, error = self.test_tenant_ids(self.capellaAPIv2.list_models, test_method_args,
                                             'tenant_id', 200,
                                             None, [404])

        # cleanup
        self.clean_model_ids.append(self.model_id)


    def test_model_api_authz_org_roles(self):
        test_method_args = {
            'tenant_id': self.tenant_id,
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
            'model_id': self.model_id
        }
        self.log.info("Testing org roles authorization for delete model api")
        result, error = self.test_with_org_roles("delete_model", test_method_args,
                                                 202, self.delete_model_callback,
                                                 "provisioned")
        if not result:
            self.fail("Authorization org roles test failed for delete model api. Error: {}".
                      format(error))

        test_method_args = {
            'tenant_id': self.tenant_id
        }

        self.log.info("Testing org roles authorization for list model apis")
        result, error = self.test_with_org_roles("list_models", test_method_args,
                                                 200, None,
                                                 "provisioned")
        # Cleanup
        self.clean_model_ids.append(self.model_id)


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

        # Create bedrock integration
        sample_bedrock_access_key = self.generate_random_string(prefix="bedrock", length=20)
        sample_bedrock_secret_key = self.generate_random_string(prefix="bedrock", length=20)
        integrations_payload = self.get_sample_integrations_payload("bedrock", access_key=sample_bedrock_access_key,
                                                                    secret_key=sample_bedrock_secret_key)
        resp = self.capellaAPIv2.create_autovec_integration(self.tenant_id, integrations_payload)
        if resp.status_code != 202:
            self.fail("Failed to create bedrock integration. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))
        bedrock_integration_id = resp.json()["id"]
        sensitive_info_list.append(sample_bedrock_secret_key)
        sensitive_info_list.append(sample_bedrock_access_key)

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

        resp = self.capellaAPIv2.get_autovec_integration(self.tenant_id, bedrock_integration_id)
        if resp.status_code != 200:
            self.fail("Failed to get bedrock integration details. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))
        data = resp.json()
        sensitive_found = self.check_for_sensitive_info(data, sensitive_info_list)
        if len(sensitive_found) > 0:
            self.fail("Sensitive info found in get bedrock integration api")

        resp = self.capellaAPIv2.list_autovec_integrations(self.tenant_id)
        if resp.status_code != 200:
            self.fail("Failed to get autovec integration details. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))
        data = resp.json()
        sensitive_found = self.check_for_sensitive_info(data, sensitive_info_list)
        if len(sensitive_found) > 0:
            self.fail("Sensitive info found in list integration api")

        # Create autovec workflow
        payload = self.get_sample_autovec_workflow_payload()
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
        # The API response 'data' is likely a dict, not a list. Get the list of workflows from the correct key.

        sensitive_found = self.check_for_sensitive_info(data, sensitive_info_list)
        if len(sensitive_found) > 0:
            self.fail("Sensitive info found in list workflow api")

        workflows = [workflow["data"] for workflow in data["data"]]
        for workflow in workflows:
            # Only proceed with deletion if workflow is in a successful state
            final_status = self.wait_for_workflow_deployment(workflow["id"], acceptable_statuses=["completed", "failed"])
            if final_status:
                self.log.info("Workflow {} is healthy, proceeding with deletion".format(workflow["id"]))
                resp = self.capellaAPIv2.delete_autovec_workflow(self.tenant_id, self.project_id, self.cluster_id, workflow["id"])
                if resp.status_code == 202:
                    result = self.wait_for_workflow_deletion(workflow["id"])
                    if not result:
                        self.log.error("Timed out while waiting for workflow deletion. Workflow id: {}".format(workflow["id"]))
                else:
                    self.log.error("Failed to delete workflow: {}. Reason: {}".format(workflow["id"], resp.content))
            else:
                self.log.warning("Workflow {} is not yet healthy, skipping deletion".format(workflow["id"]))

    def test_workflow_metadata_access(self):

        # Create autovec workflow
        payload = self.get_sample_autovec_workflow_payload()
        resp = self.capellaAPIv2.create_autovec_workflow(self.tenant_id, self.project_id, self.cluster_id, payload)
        if resp.status_code != 202:
            self.fail("Failed to create autovec workflow. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))
        workflow_id = resp.json()["id"]

        result = self.wait_for_workflow_deployment(workflow_id, acceptable_statuses=["completed", "running"])
        if not result:
            self.fail("Workflow failed to deploy even after timeout")
        self.log.info("Workflow is healthy")


    def test_invalid_model_deployment(self):

        # List of invalid models:
        invalid_llm_models = ['00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002',
                              '00000000-0000-0000-0000-000000000003', '00000000-0000-0000-0000-000000000004']


        for model in invalid_llm_models:
            payload = self.get_sample_model_deployment_payload(model_catalog_id=model, type="text")
            resp = self.capellaAPIv2.deploy_model(self.tenant_id,  payload)
            self.log.critical("Model deployment response: {}, {}".format(resp.content, resp.status_code))
            if resp.status_code != 422:
                self.fail("Model deployment API with invalid llm model {} returned a non 400 response. response: {}, status_code: {}".
                          format(model, resp.text, resp.status_code))
            error_type = resp.json()["errorType"]
            if error_type != "InvalidModelMetadataID":
                self.fail(
                    "Error messages do not match. Expected: {}. Returned: {}".format("InvalidModelMetadataID", error_type))

        invalid_embedding_models = ['00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002',
                              '00000000-0000-0000-0000-000000000003', '00000000-0000-0000-0000-000000000004']


        for model in invalid_embedding_models:
            payload = self.get_sample_model_deployment_payload(model_catalog_id=model, type="embedding")
            resp = self.capellaAPIv2.deploy_model(self.tenant_id, payload)
            self.log.critical("Model deployment response: {}".format(resp.content))
            if resp.status_code != 422:
                self.fail("Model deployment API with invalid embedding model {} returned a non 400 response.".
                          format(model))

            error_type = resp.json()["errorType"]
            if error_type != "InvalidModelMetadataID":
                self.fail(
                    "Error messages do not match. Expected: {}. Returned: {}".format("InvalidModelMetadataID", error_type))

    def send_request_to_intelligence_endpoint(self, model_endpoint, function, payload, api_key):
        intelligence_endpoint = model_endpoint + "/intelligence?function=" + function
        headers = {
            'Bearer': 'Bearer %s' % api_key,
            'Content-type': 'application/json'
        }

        resp = self.capellaAPI._urllib_request(intelligence_endpoint, method="POST", headers=headers,
                                               params=json.dumps(payload))
        return resp

    def send_request_to_model(self, model_endpoint, request_type, payload, apikey, extra_headers=None,
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

        headers = {
            'Authorization': 'Bearer %s' % apikey,
            'Content-type': 'application/json'
        }

        if extra_headers:
            headers.update(extra_headers)

        try:
            #print the payload
            self.log.info("Sending request to model endpoint: {}, payload: {}, headers: {}".format(url, payload, headers))
            resp = session.post(url, json=payload, headers=headers,
                               timeout=300, verify=False)
            # self.log.info("Response from model: {}".format(resp.text))
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

    def get_chat_completion_payload(self, model_id=None, chats=[]):
        payload = {
            "messages": [
                {
                    "role": "user",
                    "content": "How does Denmark compare to Sweden?"
                }
            ],
            "model": self.llm_model_id if model_id is None else model_id,
            "stream": False,
            "max_tokens": 150
        }

        if len(chats) > 0:
            payload['messages'] = chats

        return payload

    def get_embedding_payload(self, model_id=None, text=""):
        payload = {
            "input": "Your text string goes here",
            "model": self.embedding_model_id if model_id is None else model_id,
            "input_type": "passage"
        }

        if text != "":
            payload["input"] = text

        return payload

    def get_model_apikey_update_payload(self, allowed_ips):
        return self.get_model_apikey_create_payload(allowed_ips)

    def test_model_ip_allowlist(self):

        # Current IP is already allowed on the cluster so connection to be successful
        resp = self.capellaAPIv2.get_model_details(self.tenant_id, self.llm_model_id)
        if resp.status_code != 200:
            self.fail("Failed to fetch model details. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))

        model_details = resp.json()
        llm_model_endpoint = model_details["data"]["network"]["endpoint"]

        resp = self.capellaAPIv2.get_model_details(self.tenant_id, self.embedding_model_id)
        if resp.status_code != 200:
            self.fail("Failed to fetch model details. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))

        model_details = resp.json()
        embedding_model_endpoint = model_details["data"]["network"]["endpoint"]

        api_key_id, api_key = self.create_model_apikey(allowed_ips=[self.cidr])

        if resp.status_code != 200:
            self.fail("Failed to create db user. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))

        # Send request to llm model
        chat_payload = self.get_chat_completion_payload()
        resp = self.send_request_to_model(llm_model_endpoint, "chat", chat_payload, api_key)
        if resp is None:
            self.fail("Failed to send request to model even though current IP is allowed")

        embedding_payload = self.get_embedding_payload()
        resp = self.send_request_to_model(embedding_model_endpoint, "embedding", embedding_payload, api_key)
        if resp is None:
            self.fail("Failed to send request to model even though current IP is allowed")

        # Remove IP's and test connection failure
        updated_payload = self.get_model_apikey_update_payload(allowed_ips=['10.254.254.254/20'])

        resp = self.capellaAPIv2.update_model_api_key(self.tenant_id, api_key_id, updated_payload)
        if resp.status_code != 204:
            self.fail("Failed to update api_key: {}. Status code: {}. Error: {}".
                      format(api_key, resp.status_code, resp.text))

        # Send requests to model and verify connection fails
        resp = self.send_request_to_model(llm_model_endpoint, "chat", chat_payload, api_key)
        if resp is None:
            self.fail("request sent successfully even though current IP is disallowed")

        resp = self.send_request_to_model(embedding_model_endpoint, "embedding", embedding_payload, api_key)
        if resp is None:
            self.fail("request sent successfully even though current IP is disallowed")

    def test_gateway_endpoints_auth(self):
        resp = self.capellaAPIv2.get_model_details(self.tenant_id, self.llm_model_id)
        if resp.status_code != 200:
            self.fail("Failed to fetch model details. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))

        model_details = resp.json()
        llm_model_endpoint = model_details["data"]["network"]["endpoint"]
        llm_model_id = model_details["data"]["id"]

        resp = self.capellaAPIv2.get_model_details(self.tenant_id, self.embedding_model_id)
        if resp.status_code != 200:
            self.fail("Failed to fetch model details. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))

        model_details = resp.json()
        embedding_model_endpoint = model_details["data"]["network"]["endpoint"]
        embedding_model_id = model_details["data"]["id"]

        # Test with invalid api_keys
        self.log.info("Testing auth with invalid api_keys")

        for _ in range(5):
            rand_api_key = self.generate_random_name(base_name="apikey")
            payload = self.get_chat_completion_payload()
            resp = self.send_request_to_model(llm_model_endpoint, "chat", payload,
                                              rand_api_key)
            if resp.status_code != 401:
                self.fail("Auth test failed for chat completion endpoint for api key: {}".format(rand_api_key))

            payload = self.get_embedding_payload()
            resp = self.send_request_to_model(embedding_model_endpoint, "embedding", payload, rand_api_key)
            if resp.status_code != 401:
                self.fail("Auth test failed for embedding model endpoint for api key: {}".
                          format(rand_api_key))

        # Test with valid api key
        self.log.info("Test with generated api key")
        api_key_id, api_key = self.create_model_apikey(allowed_ips=[self.cidr])

        payload = self.get_chat_completion_payload(model_id=llm_model_id)
        resp = self.send_request_to_model(llm_model_endpoint, "chat", payload, api_key)
        if resp.status_code != 200:
            self.fail(
                "Failed to get response for chat completions endpoint even with db creds. Status code: {}. Error: {}".
                format(resp.status_code, resp.content))

        payload = self.get_embedding_payload(model_id=embedding_model_id)
        resp = self.send_request_to_model(embedding_model_endpoint, "embedding", payload, api_key)
        if resp.status_code != 200:
            self.fail("Failed to get response for embedding endpoint even with db creds. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))

        # Revoke the api_key and test
        self.log.info("Revoke the api key and test auth")
        resp = self.capellaAPIv2.delete_model_api_key(self.tenant_id, api_key_id=api_key_id)
        if resp.status_code != 204:
            self.fail(
                "Failed to delete api_key: {}. Status code: {}. Error: {}".
                format(api_key, resp.status_code, resp.content))

        self.log.info("Testing with revoked api key")
        payload = self.get_chat_completion_payload(model_id=llm_model_id)
        resp = self.send_request_to_model(llm_model_endpoint, "chat", payload, api_key)
        if resp.status_code != 401:
            self.fail("Auth test failed for chat completions for revoked api key. api_key: {}".
                      format(api_key))

        payload = self.get_embedding_payload(model_id=embedding_model_id)
        resp = self.send_request_to_model(embedding_model_endpoint, "embedding", payload,
                                          api_key)
        if resp.status_code != 401:
            self.fail("Auth test failed for embedding for revoked api key. api_key: {}".
                      format(api_key))

    def get_model_apikey_create_payload(self, allowed_ips):
        payload = {
            "name": "TestAPIKey",
            "description": "Test API Key",
            "expiryDuration": 15552000,
            'region': 'us-east-1',
            "accessPolicy": {
                "allowedIPs": allowed_ips,
            }
        }
        return payload

    def test_https_for_model_endpoint(self):
        resp = self.capellaAPIv2.get_model_details(self.tenant_id, self.llm_model_id)
        if resp.status_code != 200:
            self.fail("Failed to fetch model details. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))

        model_details = resp.json()
        llm_model_endpoint = model_details["data"]["network"]["endpoint"]
        llm_model_id = model_details["data"]["id"]

        resp = self.capellaAPIv2.get_model_details(self.tenant_id, self.embedding_model_id)
        if resp.status_code != 200:
            self.fail("Failed to fetch model details. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))

        model_details = resp.json()
        embedding_model_endpoint = model_details["data"]["network"]["endpoint"]
        embedding_model_id = model_details["data"]["id"]

        # create an api key
        _, api_key = self.create_model_apikey(allowed_ips=[self.cidr])

        payload = self.get_chat_completion_payload(model_id=llm_model_id)
        resp = self.send_request_to_model(llm_model_endpoint,"chat", payload, api_key)
        if resp.status_code != 200:
            self.fail(
                "Failed to get response for chat completions endpoint even with db creds. Status code: {}. Error: {}".
                format(resp.status_code, resp.content))

        payload = self.get_embedding_payload(model_id=embedding_model_id)
        resp = self.send_request_to_model(embedding_model_endpoint, "embedding", payload, api_key)
        if resp.status_code != 200:
            self.fail("Failed to get response for embedding endpoint even with db creds. Status code: {}. Error: {}".
                      format(resp.status_code, resp.content))