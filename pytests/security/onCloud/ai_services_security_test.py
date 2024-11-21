import random
import string

from pytests.security.security_base import SecurityBase


class SecurityTest(SecurityBase):

    def setUp(self):
        try:
            SecurityBase.setUp(self)
            self.integration_id = None
        except Exception as e:
            self.tearDown()
            self.fail("Base Setup Failed with error as - {}".format(e))

    def tearDown(self):
        super(SecurityTest, self).tearDown()

    def generate_random_name(self, base_name="integration"):

        # Generate a random string of lowercase letters and digits
        random_suffix = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(10))

        # Combine the base name with the random suffix
        random_name = "{}{}".format(base_name, random_suffix)

        return random_name

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

    def create_autovec_integration(self):
        resp = self.capellaAPIv2.create_autovec_integration(self.tenant_id,
                                                            self.get_sample_integrations_payload())
        if resp.status_code != 202:
            return None

        return resp.json()["id"]

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
