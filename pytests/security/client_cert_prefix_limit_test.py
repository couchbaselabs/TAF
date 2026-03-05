from couchbase_utils.cb_server_rest_util.security.security_api import SecurityRestAPI
from pytests.onPrem_basetestcase import OnPremBaseTest


class ClientCertPrefixLimitTest(OnPremBaseTest):
    """
    Test validation of client certificate prefix configuration limits.

    This test uses REST API to configure and validate client certificate
    authentication prefix limits without requiring full X.509 certificate setup.
    """

    def setUp(self):
        super(ClientCertPrefixLimitTest, self).setUp()

        self.security_api = SecurityRestAPI(self.cluster.master)

        self.max_prefixes_limit = 50
        self.num_prefixes = self.input.param('num_prefixes', None)

        self.expected_error_msg = f"Maximum number of prefixes supported is {self.max_prefixes_limit}"

        self.log.info(f"Max prefix limit: {self.max_prefixes_limit}")

    def tearDown(self):
        try:
            status, content, _ = self.security_api.cleanup_client_cert_auth()
            self.log.info(f"Cleanup status: {status}, content: {content}")
        finally:
            super(ClientCertPrefixLimitTest, self).tearDown()

    def test_client_cert_prefix_max_allowed(self):
        """
        Test client certificate configuration with maximum allowed prefixes (50).

        Steps:
        1. Generate client cert auth configuration with 50 prefixes (max for 8.1.x and later)
        2. Upload configuration via /settings/clientCertAuth REST endpoint
        3. Verify configuration is accepted (success status, no errors)
        4. Verify GET /settings/clientCertAuth returns configuration with correct number of prefixes
        """
        self.log.info("Starting test: client certificate prefix maximum allowed")
        self.log.info(f"Max prefix limit: {self.max_prefixes_limit}")

        num_prefixes = self.num_prefixes or self.max_prefixes_limit
        self.log.info(f"Generating client cert configuration with {num_prefixes} prefixes (max allowed)")

        prefixes_json = self.security_api.generate_client_cert_prefixes_json(num_prefixes)
        self.log.info(f"Generated {len(prefixes_json)} prefix entries")

        self.log.info("Uploading client certificate configuration via REST API")

        status, content, _ = self.security_api.set_client_cert_auth_config(
            state='enable', prefixes=prefixes_json)

        self.log.info(f"PUT status: {status}")
        self.log.info(f"PUT content: {content}")

        self.sleep(2, "Waiting for client certificate configuration to be applied")

        if status and status >= 400:
            content_str = str(content) if content else ""
            if 'error' in content_str.lower() or 'fail' in content_str.lower():
                self.fail(f"Client cert configuration failed with status {status}: {content_str}")

        self.log.info("Verifying client cert configuration was accepted")

        config = self.security_api.get_client_cert_auth_config()

        self.log.info(f"Retrieved configuration: {config}")

        self.assertIsNotNone(config, "Failed to retrieve client cert configuration")
        self.assertEqual(config.get('state'), 'enable',
                        f"Expected state to be 'enable', got {config.get('state')}")
        self.assertIn('prefixes', config,
                      "Configuration should contain 'prefixes' field")

        prefix_count = len(config.get('prefixes', []))
        self.log.info(f"Number of prefixes in configuration: {prefix_count}")
        self.assertEqual(prefix_count, num_prefixes,
                        f"Expected {num_prefixes} prefixes, got {prefix_count}")

        self.log.info("Test passed: Client certificate configuration with "
                     f"{num_prefixes} prefixes was successfully configured")

    def test_client_cert_prefix_within_limit(self):
        """
        Test client certificate configuration with 12 prefixes (within limit but not minimal).

        Steps:
        1. Generate client cert auth configuration with 12 prefixes
        2. Upload configuration via /settings/clientCertAuth REST endpoint
        3. Verify configuration is accepted (success status, no errors)
        4. Verify GET /settings/clientCertAuth returns configuration with correct number of prefixes
        """
        self.log.info("Starting test: client certificate configuration with 12 prefixes")

        num_prefixes = 12
        self.log.info(f"Generating client cert configuration with {num_prefixes} prefixes (within limit)")

        prefixes_json = self.security_api.generate_client_cert_prefixes_json(num_prefixes)
        self.log.info(f"Generated {len(prefixes_json)} prefix entries")

        self.log.info("Uploading client certificate configuration via REST API")

        status, content, _ = self.security_api.set_client_cert_auth_config(
            state='enable', prefixes=prefixes_json)

        self.log.info(f"PUT status: {status}")
        self.log.info(f"PUT content: {content}")

        self.sleep(2, "Waiting for client certificate configuration to be applied")

        if status and status >= 400:
            content_str = str(content) if content else ""
            if 'error' in content_str.lower() or 'fail' in content_str.lower():
                self.fail(f"Client cert configuration failed with status {status}: {content_str}")

        self.log.info("Verifying client cert configuration was accepted")

        config = self.security_api.get_client_cert_auth_config()

        self.log.info(f"Retrieved configuration: {config}")

        self.assertIsNotNone(config, "Failed to retrieve client cert configuration")
        self.assertEqual(config.get('state'), 'enable',
                        f"Expected state to be 'enable', got {config.get('state')}")
        self.assertIn('prefixes', config,
                      "Configuration should contain 'prefixes' field")

        prefix_count = len(config.get('prefixes', []))
        self.log.info(f"Number of prefixes in configuration: {prefix_count}")
        self.assertEqual(prefix_count, num_prefixes,
                        f"Expected {num_prefixes} prefixes, got {prefix_count}")

        self.log.info("Test passed: Client certificate configuration with "
                     f"{num_prefixes} prefixes was successfully configured")

    def test_client_cert_prefix_exceeds_limit(self):
        """
        Test client certificate configuration with 51 prefixes (exceeds max limit of 50).

        Steps:
        1. Generate client cert auth configuration with 51 prefixes (exceeds max of 50)
        2. Attempt to upload configuration via /settings/clientCertAuth REST endpoint
        3. Verify configuration is REJECTED with appropriate error message:
           "Maximum number of prefixes supported is 50"
        4. Verify GET /settings/clientCertAuth shows configuration was NOT applied
        """
        self.log.info("Starting test: client certificate prefix exceeds limit")
        self.log.info(f"Max prefix limit: {self.max_prefixes_limit}")
        self.log.info(f"Expected error message: {self.expected_error_msg}")

        num_prefixes = self.max_prefixes_limit + 1
        self.log.info(f"Generating client cert configuration with {num_prefixes} prefixes (exceeds limit)")

        prefixes_json = self.security_api.generate_client_cert_prefixes_json(num_prefixes)
        self.log.info(f"Generated {len(prefixes_json)} prefix entries")

        self.log.info("Attempting to upload client certificate configuration via REST API")

        status, content, _ = self.security_api.set_client_cert_auth_config(
            state='enable', prefixes=prefixes_json)

        self.sleep(2, "Waiting for server to process the configuration request")

        self.log.info(f"PUT status: {status}")
        self.log.info(f"PUT content: {content}")

        content_str = str(content) if content else ""

        self.assertIsNotNone(content, "Expected error response content")
        self.assertIn(self.expected_error_msg, content_str,
                     f"Expected error message '{self.expected_error_msg}', "
                     f"got: {content_str}")

        self.log.info("Verifying configuration was NOT applied")

        config = self.security_api.get_client_cert_auth_config()

        if config:
            self.log.info(f"Retrieved configuration: {config}")
            self.assertNotEqual(config.get('state'), 'enable',
                            f"Configuration should not be applied - state should not be 'enable'")
        else:
            self.log.info("Configuration does not exist (as expected)")

        self.log.info("Test passed: Client certificate configuration with "
                     f"{num_prefixes} prefixes was correctly rejected")
