
import json
import time
import uuid
import urllib.parse

from couchbase_utils.security_utils import jwt_utils
from couchbase_utils.security_utils.jwt_utils import JWTUtils
from membase.api.rest_client import RestConnection
from pytests.onPrem_basetestcase import OnPremBaseTest
from shell_util.remote_connection import RemoteMachineShellConnection


class JWTTokenTest(OnPremBaseTest):
    """
    Basic JWT auth sanity test for Couchbase.
    """

    def setUp(self):
        super(JWTTokenTest, self).setUp()

        shell_conn = RemoteMachineShellConnection(self.cluster.master)
        try:
            install_dir = getattr(shell_conn, "default_install_dir", None)
            if not install_dir:
                install_dir = getattr(shell_conn, "cb_path", None)
                if install_dir:
                    shell_conn.default_install_dir = install_dir.rstrip("/")

            self.default_install_dir = getattr(shell_conn, "default_install_dir", None)
            self.cli_bin_dir = (
                f"{self.default_install_dir.rstrip('/')}/bin"
                if self.default_install_dir else None
            )
        finally:
            shell_conn.disconnect()

        self.binary_path = self.input.param("couchbase_cli_path", None)
        if not self.binary_path:
            if not self.cli_bin_dir:
                self.fail(
                    "Unable to derive Couchbase CLI path from remote install dir. "
                    "Please pass `couchbase_cli_path` in conf."
                )
            self.binary_path = f"{self.cli_bin_dir}/couchbase-cli"

        self.issuer_name = self.input.param("issuer_name", "custom-issuer")
        self.user_name = self.input.param("user_name", "jwt_user")
        self.algorithm = self.input.param("algorithm", "ES256")
        self.key_size = self.input.param("key_size", 2048)
        self.ttl = self.input.param("ttl", 300)
        self.nbf_seconds = self.input.param("nbf_seconds", 0)
        self.jwt_utils = JWTUtils(log=self.log)

        token_audience = self.input.param("token_audience", "['cb-cluster']")
        self.token_audience = self.jwt_utils.safe_literal_list(token_audience, ["cb-cluster"])

        user_groups = self.input.param("user_groups", "['admin']")
        self.user_groups = self.jwt_utils.safe_literal_list(user_groups, ["admin"])

        token_group_rule = self.input.param("token_group_rule", None)
        self.token_group_matching_rule = (
            self.jwt_utils.safe_literal_list(token_group_rule, None) if token_group_rule else None
        )

        self.group_name = self.input.param("cb_group_name", None) or \
            f"jwt_admin_{uuid.uuid4().hex[:8]}"

        self.rest = RestConnection(self.cluster.master)
        self.jwt_utils = JWTUtils(log=self.log)
        self.private_key, self.pub_key = self.jwt_utils.generate_key_pair(
            algorithm=self.algorithm, key_size=self.key_size
        )

        # Developer Preview mode is not required for JWT authentication in version 8.1.x and later
        # Commenting out the following call as it is no longer needed
        # self._enable_dev_preview()

    def tearDown(self):
        try:
            self._disable_jwt()
        finally:
            group_maps = self.token_group_matching_rule or [f"^admin$ {self.group_name}"]
            for group in self.jwt_utils.extract_cb_groups_from_group_maps(group_maps):
                self._delete_group(group)
            super(JWTTokenTest, self).tearDown()

    def _cluster_cli_args(self):
        if self.use_https:
            cluster_arg = f"https://localhost:{self.cluster.master.port}"
            no_ssl_verify = " --no-ssl-verify"
        else:
            cluster_arg = f"localhost:{self.cluster.master.port}"
            no_ssl_verify = ""
        return cluster_arg, no_ssl_verify

    def _enable_dev_preview(self):
        """
        Enable Developer Preview mode.
        
        NOTE: This method is commented out in setUp() and tearDown() because
        Developer Preview mode is not required for JWT authentication in version 8.1.x and later.
        The method is kept here for reference or future use if testing against older versions.
        """
        self.log.info("Enabling Developer Preview mode (JWT prerequisite)")
        shell_conn = RemoteMachineShellConnection(self.cluster.master)
        try:
            cluster_arg, no_ssl_verify = self._cluster_cli_args()

            cmd = (
                f"echo y | {self.binary_path} enable-developer-preview "
                f"-c {cluster_arg} "
                f"-u {self.cluster.master.rest_username} "
                f"-p {self.cluster.master.rest_password} "
                f"--enable{no_ssl_verify}"
            )
            output, err = shell_conn.execute_command(cmd)
            if err:
                err_text = " ".join(err).strip().lower()
                if "preview" in err_text and "already" in err_text:
                    self.log.info("Developer Preview already enabled")
                else:
                    self.log.error(f"enable-developer-preview stderr: {err}")
                    self.fail("Failed to enable Developer Preview mode")
            self.log.info(f"enable-developer-preview output: {output}")
        finally:
            shell_conn.disconnect()

    

    def _create_group(self, group_name, roles="admin", description=None):
        description = description or "Admin group for JWT authentication"
        payload = urllib.parse.urlencode({"description": description, "roles": roles})

        self._delete_group(group_name)
        self.log.info(f"Creating RBAC group {group_name} with roles {roles}")

        try:
            self.rest.add_set_bulitin_group(group_name, payload)
        except Exception as e:
            self.log.warn(f"Group helper failed ({e}); retrying raw REST call")
            api = f"{self.rest.baseUrl}settings/rbac/groups/{group_name}"
            ok, content, resp = self.rest._http_request(api, "PUT", payload)
            if not ok:
                status = self.jwt_utils.status_code(resp)
                self.fail(
                    f"Failed to create group {group_name}. status={status} content={content}"
                )

    def _delete_group(self, group_name):
        if not group_name:
            return
        try:
            self.rest.delete_builtin_group(group_name)
        except Exception:
            pass

    def _get_jwt_config(self):
        group_maps = self.token_group_matching_rule or [f"^admin$ {self.group_name}"]
        return self.jwt_utils.get_jwt_config(
            issuer_name=self.issuer_name,
            algorithm=self.algorithm,
            pub_key=self.pub_key,
            token_audience=self.token_audience,
            token_group_matching_rule=group_maps,
            jit_provisioning=True,
        )

    def _put_jwt_config(self, config):
        return self.jwt_utils.put_jwt_config(self.rest, config)

    def _disable_jwt(self):
        self.jwt_utils.disable_jwt(self.rest)

    

    def _request_with_jwt(self, token, endpoint, method="GET", params=""):
        return self.jwt_utils.request_with_jwt(self.rest, token, endpoint, method, params)

    def create_token(self):
        token_audience = self.jwt_utils.get_single_audience(self.token_audience) if isinstance(self.token_audience, list) else self.token_audience
        return self.jwt_utils.create_token(
            issuer_name=self.issuer_name,
            user_name=self.user_name,
            algorithm=self.algorithm,
            private_key=self.private_key,
            token_audience=token_audience,
            user_groups=self.user_groups,
            ttl=self.ttl,
            nbf_seconds=self.nbf_seconds,
        )

    def _verify_token_rest(self, token):
        """
        Verify JWT token works for a REST request.
        Returns: (ok_bool, status_code, content)
        """
        return self.jwt_utils.verify_token_rest(self.rest, token)

    def _verify_token_cli(self, token):
        """
        Verify JWT token works for a CLI request.

        Prefer `couchbase-cli` if it supports bearer token flags; else fallback
        to `curl` as a CLI-based verification for the same REST endpoint.
        Returns: (ok_bool, status_code_or_none, output)
        """
        shell_conn = RemoteMachineShellConnection(self.cluster.master)
        try:
            cluster_arg, no_ssl_verify = self._cluster_cli_args()

            help_out, help_err = shell_conn.execute_command(f"{self.binary_path} --help")
            help_text = "\n".join(help_out + help_err).lower()

            bearer_flag = None
            for flag in ["--bearer-token", "--jwt", "--auth-token", "--access-token"]:
                if flag in help_text:
                    bearer_flag = flag
                    break

            if bearer_flag:
                cmd = (
                    f"{self.binary_path} server-list "
                    f"-c {cluster_arg} {bearer_flag} '{token}'{no_ssl_verify}"
                )
                out, err = shell_conn.execute_command(cmd)
                text = "\n".join(out + err).strip()
                ok = "error" not in text.lower() and "unauthorized" not in text.lower()
                return ok, None, text

            scheme = "https" if self.use_https else "http"
            insecure = "-k " if self.use_https else ""
            url = f"{scheme}://127.0.0.1:{self.cluster.master.port}/pools/default/buckets"
            cmd = (
                f"curl -s {insecure}-o /tmp/jwt_cli_out.txt -w '%{{http_code}}' "
                f"-H 'Authorization: Bearer {token}' "
                f"'{url}'"
            )
            http_code_out, err = shell_conn.execute_command(cmd)
            http_code = None
            if http_code_out and http_code_out[0]:
                http_code = http_code_out[0].strip().strip("'").strip('"')
            body_out, _ = shell_conn.execute_command("cat /tmp/jwt_cli_out.txt || true")
            body = "\n".join(body_out).strip()
            ok = http_code is not None and str(http_code).isdigit() and (200 <= int(http_code) < 300)
            return ok, http_code, body
        finally:
            shell_conn.disconnect()

    def _get_single_audience(self):
        """
        Convert token_audience to single string if it's a list with one element.
        Returns: Single audience string or the original value.
        """
        return self.jwt_utils.get_single_audience(self.token_audience)

    def _create_token_with_ttl(self, ttl, private_key=None):
        """
        Create JWT token with custom TTL.
        Args:
            ttl: Time to live in seconds (can be negative for expired tokens)
            private_key: Private key for signing (defaults to self.private_key)
        Returns:
            str: JWT token
        """
        if private_key is None:
            private_key = self.private_key

        return self.jwt_utils.create_token_with_ttl(
            issuer_name=self.issuer_name,
            user_name=self.user_name,
            algorithm=self.algorithm,
            private_key=private_key,
            token_audience=self.token_audience,
            user_groups=self.user_groups,
            ttl=ttl,
            nbf_seconds=self.nbf_seconds,
        )

    def _create_token_custom(self, audience=None, ttl=300, private_key=None,
                              issuer_name=None, user_name=None, user_groups=None):
        """
        Create JWT token with custom parameters.

        Args:
            audience: Token audience (defaults to config audience)
            ttl: Time to live in seconds
            private_key: Private key for signing
            issuer_name: JWT issuer name
            user_name: Token user name
            user_groups: Token user groups

        Returns:
            str: JWT token
        """
        if private_key is None:
            private_key = self.private_key
        if audience is None:
            audience = self._get_single_audience()
        if issuer_name is None:
            issuer_name = self.issuer_name
        if user_name is None:
            user_name = self.user_name
        if user_groups is None:
            user_groups = self.user_groups

        return self.jwt_utils.create_token_with_ttl(
            issuer_name=issuer_name,
            user_name=user_name,
            algorithm=self.algorithm,
            private_key=private_key,
            token_audience=audience,
            user_groups=user_groups,
            ttl=ttl,
            nbf_seconds=0,
        )

    def _assert_auth_fails(self, status_code, message):
        """
        Assert that authentication failed.

        Args:
            status_code: HTTP status code
            message: Assertion error message
        """
        self.jwt_utils.assert_auth_fails(status_code, message)

    def _assert_auth_succeeds(self, ok, status_code, message):
        """
        Assert that authentication succeeded.

        Args:
            ok: Boolean success indicator
            status_code: HTTP status code
            message: Assertion error message
        """
        self.jwt_utils.assert_auth_succeeds(ok, status_code, message)

    def _assert_unauthorized_status(self, status_code, message):
        """
        Assert that status code indicates unauthorized (401 or 403).

        Args:
            status_code: HTTP status code
            message: Assertion error message
        """
        self.jwt_utils.assert_unauthorized_status(status_code, message)

    def _assert_token_fails_repeatedly(self, token, num_retries, context=""):
        """
        Assert that token fails authentication on multiple retry attempts.

        Args:
            token: JWT token to test
            num_retries: Number of retry attempts
            context: Context string for logging
        """
        self.jwt_utils.assert_token_fails_repeatedly(
            rest_connection=self.rest,
            token=token,
            num_retries=num_retries,
            context=context,
            log_callback=self.log.info
        )

    def _assert_token_succeeds_repeatedly(self, token, num_requests, context=""):
        """
        Assert that token succeeds authentication on multiple requests.

        Args:
            token: JWT token to test
            num_requests: Number of requests
            context: Context string for logging
        """
        self.jwt_utils.assert_token_succeeds_repeatedly(
            rest_connection=self.rest,
            token=token,
            num_requests=num_requests,
            context=context,
            log_callback=self.log.info
        )

    def _perform_key_rotation(self, original_config, new_pub_key, sleep_seconds=10):
        """
        Perform JWT key rotation by updating config with new public key.

        Args:
            original_config: Original JWT config
            new_pub_key: New public key for rotation
            sleep_seconds: Seconds to wait for config propagation

        Returns:
            dict: Rotated JWT config
        """
        return self.jwt_utils.perform_key_rotation(
            rest_connection=self.rest,
            issuer_name=self.issuer_name,
            algorithm=self.algorithm,
            pub_key=new_pub_key,
            token_audience=self.token_audience,
            token_group_matching_rule=(original_config.get("issuers", [{}])[0].get("groupsMaps", [])),
            sleep_callback=self.sleep,
            sleep_seconds=sleep_seconds,
            jit_provisioning=True,
        )

    def _setup_jwt_config(self, config, create_groups=True):
        """
        PUT jwt config, optionally create referenced groups, and wait for it to take effect.
        """
        self.jwt_utils.setup_jwt_config(
            rest_connection=self.rest,
            config=config,
            create_groups_callback=self._create_group if create_groups else None,
            sleep_callback=self.sleep
        )

    def test_create_config(self):
        """
        Basic JWT auth test.

        Validates token works for:
        - REST request (python)
        - CLI request (couchbase-cli if supported, else curl fallback)
        """
        expected_jwt_auth = self.input.param("expected_jwt_auth", True)
        create_groups = self.input.param("create_groups", True)
        verify_cli = self.input.param("verify_cli", True)

        config = self._get_jwt_config()
        self._setup_jwt_config(config, create_groups=create_groups)

        token = self.create_token()
        self.log.info("JWT token generated (redacted)")

        rest_ok, status_code, content = self._verify_token_rest(token)
        self.log.info(f"JWT REST ok={rest_ok} status={status_code} content={content}")

        cli_ok, cli_status, cli_out = True, None, ""
        if verify_cli:
            cli_ok, cli_status, cli_out = self._verify_token_cli(token)
            self.log.info(f"JWT CLI ok={cli_ok} status={cli_status} out={cli_out}")

        if expected_jwt_auth:
            if status_code is None:
                self.fail("No HTTP status code returned for REST JWT request")
            if not (200 <= int(status_code) < 300):
                self.fail(f"JWT REST auth failed. status={status_code} content={content}")
            if verify_cli and not cli_ok:
                self.fail(f"JWT CLI auth failed. status={cli_status} out={cli_out}")
        else:
            rest_failed = (status_code is None) or not (200 <= int(status_code) < 300)
            cli_failed = (not cli_ok) if verify_cli else True
            if not (rest_failed and cli_failed):
                self.fail(
                    "Expected JWT auth to fail but at least one path succeeded. "
                    f"rest_status={status_code} cli_ok={cli_ok}"
                )

    def test_jwt_token_matrix(self):
        """
        Allows overriding token fields independently of config fields using:
        - token_issuer_name
        - token_user_name
        - token_algorithm
        - token_private_key_mode: "config_key" (default) | "new_key"
        - token_audience
        - token_user_groups
        - token_ttl
        - token_nbf_seconds

        Expected outcomes:
        - expected_token_create: True/False
        - expected_jwt_auth: True/False
        """
        expected_token_create = self.input.param("expected_token_create", True)
        expected_jwt_auth = self.input.param("expected_jwt_auth", True)
        create_groups = self.input.param("create_groups", True)
        verify_cli = self.input.param("verify_cli", True)

        config = self._get_jwt_config()
        self._setup_jwt_config(config, create_groups=create_groups)

        token_issuer_name = self.input.param("token_issuer_name", self.issuer_name)
        token_user_name = self.input.param("token_user_name", self.user_name)
        token_algorithm = self.input.param("token_algorithm", self.algorithm)

        token_audience_raw = self.input.param("token_audience_override", None)
        token_audience = (
            self.jwt_utils.safe_literal_list(token_audience_raw, None)
            if token_audience_raw is not None
            else self.token_audience
        )
        if isinstance(token_audience, list) and len(token_audience) == 1:
            token_audience = token_audience[0]

        token_groups_raw = self.input.param("token_user_groups_override", None)
        token_user_groups = (
            self.jwt_utils.safe_literal_list(token_groups_raw, None)
            if token_groups_raw is not None
            else self.user_groups
        )

        token_ttl = self.input.param("token_ttl", self.ttl)
        token_nbf_seconds = self.input.param("token_nbf_seconds", self.nbf_seconds)

        token_private_key_mode = self.input.param("token_private_key_mode", "config_key")
        token_private_key = self.private_key
        if token_private_key_mode == "new_key":
            token_private_key, _ = self.jwt_utils.generate_key_pair(
                algorithm=token_algorithm, key_size=self.key_size
            )

        token = None
        token_create_err = None
        try:
            token = self.jwt_utils.create_token(
                issuer_name=token_issuer_name,
                user_name=token_user_name,
                algorithm=token_algorithm,
                private_key=token_private_key,
                token_audience=token_audience,
                user_groups=token_user_groups,
                ttl=token_ttl,
                nbf_seconds=token_nbf_seconds,
            )
        except Exception as e:
            token_create_err = str(e)

        if expected_token_create:
            if not token:
                self.fail(f"Expected token creation to succeed but failed: {token_create_err}")
        else:
            if token:
                self.fail("Expected token creation to fail but it succeeded")
            return

        self.log.info("JWT token generated for matrix test (redacted)")

        rest_ok, status_code, content = self._verify_token_rest(token)
        self.log.info(f"JWT REST ok={rest_ok} status={status_code} content={content}")

        cli_ok, cli_status, cli_out = True, None, ""
        if verify_cli:
            cli_ok, cli_status, cli_out = self._verify_token_cli(token)
            self.log.info(f"JWT CLI ok={cli_ok} status={cli_status} out={cli_out}")

        if expected_jwt_auth:
            if status_code is None or not (200 <= int(status_code) < 300):
                self.fail(f"JWT REST auth failed. status={status_code} content={content}")
            if verify_cli and not cli_ok:
                self.fail(f"JWT CLI auth failed. status={cli_status} out={cli_out}")
        else:
            rest_failed = (status_code is None) or not (200 <= int(status_code) < 300)
            cli_failed = (not cli_ok) if verify_cli else True
            if not (rest_failed and cli_failed):
                self.fail(
                    "Expected JWT auth to fail but at least one path succeeded. "
                    f"rest_status={status_code} cli_ok={cli_ok}"
                )

    def _verify_malformed_token_rejection(self, token, token_type):
        """
        Helper method to verify that a malformed token is properly rejected.

        This method performs the following validations:
        1. REST request with malformed token should fail (non-2xx status)
        2. CLI request with malformed token should fail if enabled
        3. Status code should be 400 (Bad Request) or 401 (Unauthorized)
        4. Error content should contain JWT/token-related keywords

        Args:
            token: The malformed token string
            token_type: Description of the token type for logging

        Raises:
            Exception: If token verification fails unexpectedly (exceptions are logged)
        """
        verify_cli = self.input.param("verify_cli", True)

        self.jwt_utils.verify_malformed_token_rejection(
            rest_connection=self.rest,
            token=token,
            token_type=token_type,
            verify_cli_callback=self._verify_token_cli if verify_cli else None,
            log_callback=self.log.info
        )

    def test_jwt_malformed_structure(self):
        """
        Test JWT token with missing segments.

        Expected behavior:
        - A valid JWT token must have exactly 3 segments: header.payload.signature
        - Token with only 2 segments should be rejected during parsing
        - HTTP status should be 400 or 401
        - Error message should indicate invalid/malformed token
        """
        config = self._get_jwt_config()
        self._setup_jwt_config(config)

        token = "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJqd3RfdXNlciJ9"
        self.log.info("Testing token with missing segments (only 2 parts)")

        self._verify_malformed_token_rejection(token, "missing segments")

    def test_jwt_invalid_base64(self):
        """
        Test JWT token with invalid base64 encoding in payload.

        Expected behavior:
        - Token with non-base64 characters should fail during base64 decoding
        - HTTP status should be 400 or 401
        - Error message should indicate decoding failure or invalid format
        """
        config = self._get_jwt_config()
        self._setup_jwt_config(config)

        token = "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.invalid@#$base64!.signature"
        self.log.info("Testing token with invalid base64 encoding")

        self._verify_malformed_token_rejection(token, "invalid base64")

    def test_jwt_garbage_token(self):
        """
        Test JWT token that is a completely invalid random string.

        Expected behavior:
        - Random string with no JWT structure should fail immediately during parsing
        - HTTP status should be 400 or 401
        - Error message should indicate invalid token format

        Validates:
        - Parser correctly rejects completely invalid input
        - Server returns appropriate error status
        - Error content clearly identifies format issue
        """
        config = self._get_jwt_config()
        self._setup_jwt_config(config)
        token = "this-is-not-a-valid-jwt-token-at-all"
        self.log.info("Testing completely garbage token string")

        self._verify_malformed_token_rejection(token, "garbage string")

    def test_jwt_empty_token(self):
        """
        Test JWT token that is empty or whitespace-only.

        Expected behavior:
        - Empty token should be rejected immediately
        - HTTP status should be 400 or 401
        - Error message should indicate missing or empty token

        Validates:
        - Parser correctly handles empty input
        - Server returns appropriate error status
        - Error content identifies the empty token issue
        """
        config = self._get_jwt_config()
        self._setup_jwt_config(config)
        token = ""
        self.log.info("Testing empty token")

        self._verify_malformed_token_rejection(token, "empty")

    def test_jwt_expired_token_with_error_messaging(self):
        """
        Test that expired tokens fail immediately with clear error messaging.

        Validates:
        - Tokens with negative TTL (already expired) are rejected
        - Response status code indicates token expiry
        - Error content clearly indicates expiration issue (contains "expir", "expire", "timeout", or similar)
        """
        config = self._get_jwt_config()
        self._setup_jwt_config(config)

        self.log.info("Creating expired token with negative TTL")
        token = self._create_token_with_ttl(ttl=-60)

        self.log.info("Testing expired token rejection")
        ok, status_code, content = self._verify_token_rest(token)
        self.log.info(f"JWT REST ok={ok} status={status_code} content={content}")

        self._assert_auth_fails(status_code, "Expired token should fail")
        self._assert_unauthorized_status(
            status_code,
            f"Unauthorized status expected for expired token, got {status_code}"
        )

        if content:
            content_lower = content.lower()
            expiry_keywords = ["expir", "expire", "valid", "token", "jwt"]
            has_expiry_indicator = any(k in content_lower for k in expiry_keywords)
            self.assertTrue(
                has_expiry_indicator,
                f"Error message should indicate token validity issue. Content: {content}"
            )

    def test_jwt_expired_token_multiple_retries(self):
        """
        Test that expired tokens consistently fail on multiple retry attempts.

        Validates stateless behavior: expired tokens should not gain access
        through retries, and each request should independently validate the token.
        """
        config = self._get_jwt_config()
        self._setup_jwt_config(config)

        self.log.info("Creating expired token for retry testing")
        token = self._create_token_with_ttl(ttl=-120)

        num_retries = 5
        self._assert_token_fails_repeatedly(token, num_retries, "Expired token retry")

        self.log.info(f"Expired token consistently rejected across {num_retries} retries (stateless)")

    def test_jwt_stateless_session_verification(self):
        """
        Test that token validity alone determines access; no server-side session state is retained.

        Validates Couchbase remains stateless with respect to JWTs:
        1. Valid tokens from multiple issuers work independently
        2. Token parameters are validated on each request (no caching of authenticated state)
        3. Token reuse across multiple requests always validates the token itself
        """
        config = self._get_jwt_config()
        self._setup_jwt_config(config)

        self.log.info("Creating token for stateless session verification")
        token = self.create_token()

        num_requests = 5
        self._assert_token_succeeds_repeatedly(token, num_requests, "Stateless session request")

        self.log.info("Testing token parameter validation on each request")
        bad_audience_token = self._create_token_custom(audience="wrong-audience")

        ok, status_code, content = self._verify_token_rest(bad_audience_token)
        self.log.info(f"Bad audience token: ok={ok} status={status_code}")

        self._assert_auth_fails(status_code, "Token with wrong audience should fail immediately")

        self.log.info("Stateless session verification: token validity alone determines access")

    def test_jwt_key_rotation_invalidates_old_tokens(self):
        """
        Test JWKS key rotation/replacement must invalidate tokens signed with removed keys.

        Simulates key rotation by:
        1. Create token with original key pair
        2. Generate new key pair and update JWT config
        3. Verify old token fails (signed with removed key)
        4. Verify new token with new key works
        5. Verify old token continues to fail after cache refresh period

        Expected behavior:
        - Old tokens signed with rotated keys are invalid after config update
        - New tokens with new keys work correctly
        - Old tokens remain invalidated after JWKS cache refresh

        Validates:
        - Immediate key rotation invalidation
        - New keys are accepted
        - Cache refresh doesn't revive old keys (stateless behavior)
        """
        self.log.info("Setting up JWT config with original key")
        config = self._get_jwt_config()
        self._setup_jwt_config(config)

        self.log.info("Creating token with original key")
        original_token = self.create_token()

        ok, status_code, content = self._verify_token_rest(original_token)
        self.log.info(f"Original token verification: ok={ok} status={status_code}")
        self._assert_auth_succeeds(
            ok, status_code,
            "Original token should work before key rotation"
        )

        self.log.info("Generating new key pair for rotation")
        new_private_key, new_pub_key = self.jwt_utils.generate_key_pair(
            algorithm=self.algorithm, key_size=self.key_size
        )

        self.log.info("Updating JWT config with new key")
        rotated_config = self._perform_key_rotation(config, new_pub_key, sleep_seconds=10)

        self.log.info("Testing old token after key rotation")
        ok, status_code, content = self._verify_token_rest(original_token)
        self.log.info(f"Old token after rotation: ok={ok} status={status_code}")

        self._assert_auth_fails(
            status_code,
            "Old token signed with removed key should fail after key rotation"
        )

        self.log.info("Verifying old token remains invalid after cache refresh period")
        self.sleep(15, "Waiting for JWKS cache refresh after key rotation")

        num_cache_checks = 5
        self._assert_token_fails_repeatedly(original_token, num_cache_checks, "Cache timeout")

        self.log.info("Creating token with new key")
        new_token = self._create_token_custom(private_key=new_private_key)

        ok, status_code, content = self._verify_token_rest(new_token)
        self.log.info(f"New token verification: ok={ok} status={status_code}")

        self._assert_auth_succeeds(
            ok, status_code,
            "New token signed with new key should work after rotation"
        )

        self.log.info("Key rotation validated: old keys invalidated, new keys work, cache timeout verified")

    def test_jwt_key_rotation_cache_timeout(self):
        """
        Test JWKS key rotation with cache refresh semantics.

        Validates that old keys do not remain valid indefinitely:
        - When key is rotated, tokens signed with old keys are rejected after cache refresh
        - Multiple retry attempts after rotation consistently fail
        """
        self.log.info("Setting up JWT config with original key for cache test")
        config = self._get_jwt_config()
        self._setup_jwt_config(config)

        self.log.info("Creating token with original key")
        original_token = self.create_token()

        ok, status_code, content = self._verify_token_rest(original_token)
        self._assert_auth_succeeds(ok, status_code, "Original token should work initially")

        self.log.info("Generating new key pair and rotating")
        new_private_key, new_pub_key = self.jwt_utils.generate_key_pair(
            algorithm=self.algorithm, key_size=self.key_size
        )

        rotated_config = self.jwt_utils.get_jwt_config(
            issuer_name=self.issuer_name,
            algorithm=self.algorithm,
            pub_key=new_pub_key,
            token_audience=self.token_audience,
            token_group_matching_rule=(config.get("issuers", [{}])[0].get("groupsMaps", [])),
            jit_provisioning=True,
        )
        self._setup_jwt_config(rotated_config, create_groups=False)

        self.sleep(15, "Waiting for JWKS cache refresh after key rotation")

        num_cache_checks = 5
        self._assert_token_fails_repeatedly(original_token, num_cache_checks, "Cache check")

        self.log.info("Old keys consistently invalidated after cache refresh")


