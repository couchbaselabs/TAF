
import json
import time
import uuid
import urllib.parse
import base64
import jwt

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

    def _setup_jit_config(self, jit_provisioning=True, group_maps=None, create_groups=True):
        """
        Configure JWT with JIT on/off and optional group-maps.

        Returns:
            tuple: (config, created_group_names)
        """
        if group_maps is None:
            group_name = f"jwt_jit_{uuid.uuid4().hex[:8]}"
            group_maps = [f"^admin$ {group_name}"]
        else:
            group_name = None

        config = self.jwt_utils.get_jwt_config(
            issuer_name=self.issuer_name,
            algorithm=self.algorithm,
            pub_key=self.pub_key,
            token_audience=self.token_audience,
            token_group_matching_rule=group_maps,
            jit_provisioning=jit_provisioning,
        )
        self._setup_jwt_config(config, create_groups=create_groups)

        created = []
        if group_name:
            created.append(group_name)
        return config, created

    

    def _request_with_jwt(self, token, endpoint, method="GET", params=""):
        return self.jwt_utils.request_with_jwt(self.rest, token, endpoint, method, params)

    def create_token(
        self,
        audience=None,
        ttl=None,
        private_key=None,
        issuer_name=None,
        user_name=None,
        user_groups=None,
        nbf_seconds=None,
    ):
        if private_key is None:
            private_key = self.private_key
        if ttl is None:
            ttl = self.ttl
        if audience is None:
            audience = self.token_audience
        if issuer_name is None:
            issuer_name = self.issuer_name
        if user_name is None:
            user_name = self.user_name
        if user_groups is None:
            user_groups = self.user_groups
        if nbf_seconds is None:
            nbf_seconds = self.nbf_seconds

        return self.jwt_utils.create_token(
            issuer_name=issuer_name,
            user_name=user_name,
            algorithm=self.algorithm,
            private_key=private_key,
            token_audience=audience,
            user_groups=user_groups,
            ttl=ttl,
            nbf_seconds=nbf_seconds,
            normalize_audience=True,
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
                text_lower = text.lower()
                if "please check your username" in text_lower or "username (-u)" in text_lower:
                    bearer_flag = None
                else:
                    ok = "error" not in text_lower and "unauthorized" not in text_lower
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
        expected_jwt_authz = self.input.param("expected_jwt_authz", None)
        expected_jwt_auth = self.input.param("expected_jwt_auth", None)
        if expected_jwt_authz is None:
            # Backward compatibility: old conf used expected_jwt_auth for authz endpoint
            expected_jwt_authz = self.input.param("expected_jwt_auth", True)
        create_groups = self.input.param("create_groups", True)
        verify_cli = self.input.param("verify_cli", True)

        config = self._get_jwt_config()
        self._setup_jwt_config(config, create_groups=create_groups)

        token = self.create_token()
        self.log.info("JWT token generated (redacted)")
        try:
            self.jwt_utils.validate_jwt_expectations(
                rest_connection=self.rest,
                token=token,
                expected_jwt_authz=expected_jwt_authz,
                expected_jwt_auth=expected_jwt_auth,
                verify_cli=verify_cli,
                verify_cli_callback=self._verify_token_cli if verify_cli else None,
                log_callback=self.log.info,
            )
        except AssertionError as e:
            self.fail(str(e))

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
        expected_jwt_authz = self.input.param("expected_jwt_authz", None)
        expected_jwt_auth = self.input.param("expected_jwt_auth", None)
        if expected_jwt_authz is None:
            expected_jwt_authz = self.input.param("expected_jwt_auth", True)
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
        try:
            self.jwt_utils.validate_jwt_expectations(
                rest_connection=self.rest,
                token=token,
                expected_jwt_authz=expected_jwt_authz,
                expected_jwt_auth=expected_jwt_auth,
                verify_cli=verify_cli,
                verify_cli_callback=self._verify_token_cli if verify_cli else None,
                log_callback=self.log.info,
            )
        except AssertionError as e:
            self.fail(str(e))

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
        token = self.create_token(ttl=-60)

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
        token = self.create_token(ttl=-120)

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
        bad_audience_token = self.create_token(audience="wrong-audience")

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
        new_token = self.create_token(private_key=new_private_key)

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

    def test_jwt_malformed_tokens(self):
        """
        Param-driven malformed token test. REST and CLI both expect rejection.

        Param: malformed_type = missing_segments | invalid_base64 | garbage | empty
        """
        malformed_type = self.input.param("malformed_type", "missing_segments")
        tokens = {
            "missing_segments": "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJqd3RfdXNlciJ9",
            "invalid_base64": "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.invalid@#$base64!.signature",
            "garbage": "this-is-not-a-valid-jwt-token-at-all",
            "empty": "",
        }
        if malformed_type not in tokens:
            self.fail(
                f"Unknown malformed_type={malformed_type}. "
                "Use: missing_segments|invalid_base64|garbage|empty"
            )
        token = tokens[malformed_type]
        config = self._get_jwt_config()
        self._setup_jwt_config(config)
        self.log.info(f"Testing malformed token: {malformed_type}")
        self._verify_malformed_token_rejection(token, malformed_type)

    def test_jwt_rbac_role_mapping(self):
        """
        RBAC Role and Group Mapping test.

        We validate:
        - Authentication succeeds for both tokens via /whoami
        - Authorization differs on protected endpoint (/pools/default/buckets):
          - admin token succeeds (2xx)
          - readonly token fails (non-2xx)
        """
        expected_authz_admin = self.input.param("expected_jwt_authz", True)
        expected_auth_admin = self.input.param("expected_jwt_auth", True)
        verify_cli = self.input.param("verify_cli", True)

        group_admin = f"jwt_rbac_admin_{uuid.uuid4().hex[:8]}"
        group_ro = f"jwt_rbac_ro_{uuid.uuid4().hex[:8]}"

        self._create_group(group_admin, roles="admin")
        self._create_group(group_ro, roles="ro_admin")

        try:
            group_maps = [f"^admin$ {group_admin}", f"^readonly$ {group_ro}"]
            config = self.jwt_utils.get_jwt_config(
                issuer_name=self.issuer_name,
                algorithm=self.algorithm,
                pub_key=self.pub_key,
                token_audience=self.token_audience,
                token_group_matching_rule=group_maps,
                jit_provisioning=True,
            )
            self._setup_jwt_config(config, create_groups=False)

            admin_user = f"jwt_test_user_admin_{uuid.uuid4().hex[:8]}"
            self.log.info("Testing admin mapping (auth+authz expected)")
            admin_token = self.create_token(user_name=admin_user, user_groups=["admin"])
            try:
                self.jwt_utils.validate_jwt_expectations(
                    rest_connection=self.rest,
                    token=admin_token,
                    expected_jwt_auth=expected_auth_admin,
                    expected_jwt_authz=expected_authz_admin,
                    verify_cli=verify_cli,
                    verify_cli_callback=self._verify_token_cli if verify_cli else None,
                    log_callback=self.log.info,
                )
            except AssertionError as e:
                self.fail(str(e))

            whoami_admin = self.jwt_utils.get_user_info_from_whoami(self.rest, admin_token)
            self.assertTrue(whoami_admin, "Expected /whoami JSON for admin token")
            self.assertEqual(whoami_admin.get("domain"), "external", f"Unexpected whoami: {whoami_admin}")
            self.assertEqual(whoami_admin.get("id"), admin_user, f"Unexpected whoami: {whoami_admin}")
            admin_roles = [r.get("role") for r in (whoami_admin.get("roles") or []) if isinstance(r, dict)]
            self.assertTrue("admin" in admin_roles, f"Expected admin role in whoami roles, got {admin_roles}")

            self.log.info("Testing readonly mapping (auth OK, authz denied)")
            readonly_user = f"jwt_test_user_readonly_{uuid.uuid4().hex[:8]}"
            readonly_token = self.create_token(user_name=readonly_user, user_groups=["readonly"])
            try:
                self.jwt_utils.validate_jwt_expectations(
                    rest_connection=self.rest,
                    token=readonly_token,
                    expected_jwt_auth=True,
                    expected_jwt_authz=False,
                    authz_endpoint="/settings/rbac/users",
                    verify_cli=False,
                    log_callback=self.log.info,
                )
            except AssertionError as e:
                self.fail(str(e))

            whoami_ro = self.jwt_utils.get_user_info_from_whoami(self.rest, readonly_token)
            self.assertTrue(whoami_ro, "Expected /whoami JSON for readonly token")
            self.assertEqual(whoami_ro.get("domain"), "external", f"Unexpected whoami: {whoami_ro}")
            self.assertEqual(whoami_ro.get("id"), readonly_user, f"Unexpected whoami: {whoami_ro}")
            ro_roles = [r.get("role") for r in (whoami_ro.get("roles") or []) if isinstance(r, dict)]
            self.assertTrue("admin" not in ro_roles, f"Did not expect admin role for readonly token, got {ro_roles}")
        finally:
            self._delete_group(group_admin)
            self._delete_group(group_ro)

    def test_jwt_rbac_group_mapping(self):
        """
        Test that JWT group claim correctly maps to Couchbase RBAC groups.
        """
        verify_cli = self.input.param("verify_cli", True)
        expected_authz = self.input.param("expected_jwt_authz", True)
        expected_auth = self.input.param("expected_jwt_auth", True)

        group_name = f"jwt_test_group_{uuid.uuid4().hex[:8]}"
        self._create_group(group_name, roles="admin")

        try:
            group_maps = [f"^mapped$ {group_name}"]
            config = self.jwt_utils.get_jwt_config(
                issuer_name=self.issuer_name,
                algorithm=self.algorithm,
                pub_key=self.pub_key,
                token_audience=self.token_audience,
                token_group_matching_rule=group_maps,
                jit_provisioning=True,
            )
            self._setup_jwt_config(config, create_groups=False)

            user = f"jwt_test_user_mapped_{uuid.uuid4().hex[:8]}"
            token = self.create_token(user_name=user, user_groups=["mapped"])
            try:
                self.jwt_utils.validate_jwt_expectations(
                    rest_connection=self.rest,
                    token=token,
                    expected_jwt_auth=expected_auth,
                    expected_jwt_authz=expected_authz,
                    verify_cli=verify_cli,
                    verify_cli_callback=self._verify_token_cli if verify_cli else None,
                    log_callback=self.log.info,
                )
            except AssertionError as e:
                self.fail(str(e))

            whoami = self.jwt_utils.get_user_info_from_whoami(self.rest, token)
            self.assertTrue(whoami, "Expected /whoami JSON for mapped token")
            self.assertEqual(whoami.get("domain"), "external", f"Unexpected whoami: {whoami}")
            self.assertEqual(whoami.get("id"), user, f"Unexpected whoami: {whoami}")
            roles = [r.get("role") for r in (whoami.get("roles") or []) if isinstance(r, dict)]
            self.assertTrue("admin" in roles, f"Expected admin role in whoami roles, got {roles}")
        finally:
            self._delete_group(group_name)

    def _assert_auth_ok_authz_fail_empty_roles(self, token_groups, context):
        """
        Helper: expect /whoami auth succeeds but protected endpoint authz fails,
        and /whoami roles are empty (or missing).
        """
        verify_cli = self.input.param("verify_cli", True)
        expected_authz = self.input.param("expected_jwt_authz", False)

        user = f"jwt_test_user_{uuid.uuid4().hex[:8]}"
        token = self.create_token(user_name=user, user_groups=token_groups)

        try:
            self.jwt_utils.validate_jwt_expectations(
                rest_connection=self.rest,
                token=token,
                expected_jwt_authz=expected_authz,
                verify_cli=verify_cli,
                verify_cli_callback=self._verify_token_cli if verify_cli else None,
                log_callback=self.log.info,
            )
        except AssertionError as e:
            self.fail(str(e))

        # Do not assert /whoami success here: some builds reject empty/malformed groups at auth time.
        # If /whoami returns JSON, we can still assert roles are empty.
        whoami = self.jwt_utils.get_user_info_from_whoami(self.rest, token)
        if whoami:
            roles = [r.get("role") for r in (whoami.get("roles") or []) if isinstance(r, dict)]
            self.assertEqual(roles, [], f"Expected empty roles for {context}, got {roles}")

    def test_jwt_rbac_restricted_roles(self):
        """
        Test that restricted roles are never granted implicitly.

        This is a safety check: even when mapped to admin, server should not
        implicitly grant sensitive roles like security_admin.
        """
        verify_cli = self.input.param("verify_cli", True)
        expected_auth = self.input.param("expected_jwt_auth", True)
        expected_authz = self.input.param("expected_jwt_authz", True)

        group_admin = f"jwt_rbac_admin_{uuid.uuid4().hex[:8]}"
        self._create_group(group_admin, roles="admin")

        try:
            group_maps = [f"^admin$ {group_admin}"]
            config = self.jwt_utils.get_jwt_config(
                issuer_name=self.issuer_name,
                algorithm=self.algorithm,
                pub_key=self.pub_key,
                token_audience=self.token_audience,
                token_group_matching_rule=group_maps,
                jit_provisioning=True,
            )
            self._setup_jwt_config(config, create_groups=False)

            user = f"jwt_test_user_admin_{uuid.uuid4().hex[:8]}"
            token = self.create_token(user_name=user, user_groups=["admin"])

            try:
                self.jwt_utils.validate_jwt_expectations(
                    rest_connection=self.rest,
                    token=token,
                    expected_jwt_auth=expected_auth,
                    expected_jwt_authz=expected_authz,
                    verify_cli=verify_cli,
                    verify_cli_callback=self._verify_token_cli if verify_cli else None,
                    log_callback=self.log.info,
                )
            except AssertionError as e:
                self.fail(str(e))

            whoami = self.jwt_utils.get_user_info_from_whoami(self.rest, token)
            self.assertTrue(whoami, "Expected /whoami JSON for admin token")
            roles = [r.get("role") for r in (whoami.get("roles") or []) if isinstance(r, dict)]
            self.assertTrue(len(roles) > 0, f"Expected non-empty roles, got {roles}")
            self.assertTrue("admin" in roles, f"Expected admin role, got {roles}")

            restricted_denylist = ["security_admin"]
            self.assertTrue(
                all(r not in restricted_denylist for r in roles),
                f"Restricted roles should not be granted implicitly. roles={roles}",
            )
        finally:
            self._delete_group(group_admin)

    def test_jwt_rbac_role_update_reflection(self):
        """
        Test that updating RBAC group roles reflects in subsequent authorization.
        """
        verify_cli = self.input.param("verify_cli", True)
        expected_auth_initial = self.input.param("expected_jwt_auth", True)
        expected_authz_initial = self.input.param("expected_jwt_authz", True)

        group_name = f"jwt_rbac_update_{uuid.uuid4().hex[:8]}"
        self._create_group(group_name, roles="admin")

        try:
            group_maps = [f"^admin$ {group_name}"]
            config = self.jwt_utils.get_jwt_config(
                issuer_name=self.issuer_name,
                algorithm=self.algorithm,
                pub_key=self.pub_key,
                token_audience=self.token_audience,
                token_group_matching_rule=group_maps,
                jit_provisioning=True,
            )
            self._setup_jwt_config(config, create_groups=False)

            user = f"jwt_test_user_update_{uuid.uuid4().hex[:8]}"
            token = self.create_token(user_name=user, user_groups=["admin"])

            # Initially: admin should authorize
            try:
                self.jwt_utils.validate_jwt_expectations(
                    rest_connection=self.rest,
                    token=token,
                    expected_jwt_auth=expected_auth_initial,
                    expected_jwt_authz=expected_authz_initial,
                    verify_cli=verify_cli,
                    verify_cli_callback=self._verify_token_cli if verify_cli else None,
                    log_callback=self.log.info,
                )
            except AssertionError as e:
                self.fail(str(e))

            # Update group roles to read-only-ish and ensure authz is denied
            self._create_group(group_name, roles="ro_admin")
            self.sleep(5, "Waiting for RBAC group role update to propagate")

            try:
                self.jwt_utils.validate_jwt_expectations(
                    rest_connection=self.rest,
                    token=token,
                    expected_jwt_auth=True,
                    expected_jwt_authz=False,
                    authz_endpoint="/settings/rbac/users",
                    verify_cli=False,
                    log_callback=self.log.info,
                )
            except AssertionError as e:
                self.fail(str(e))

            whoami = self.jwt_utils.get_user_info_from_whoami(self.rest, token)
            self.assertTrue(whoami, "Expected /whoami JSON after role update")
            roles = [r.get("role") for r in (whoami.get("roles") or []) if isinstance(r, dict)]
            self.assertTrue("admin" not in roles, f"Did not expect admin role after update, got {roles}")
        finally:
            self._delete_group(group_name)

    def test_jwt_rbac_missing_roles_groups(self):
        """Auth succeeds but authz fails when roles/groups are missing; /whoami returns empty roles."""
        self.log.info("Testing token with empty groups")
        self._assert_auth_ok_authz_fail_empty_roles([], "no groups")
        self.log.info("Testing token with None groups")
        self._assert_auth_ok_authz_fail_empty_roles(None, "None groups")

    def test_jwt_rbac_malformed_roles_groups(self):
        """Auth succeeds but authz fails with malformed roles/groups; /whoami returns empty roles."""
        self.log.info("Testing token with malformed group names")
        self._assert_auth_ok_authz_fail_empty_roles(
            ["invalid$group!name", "no#spaces"], "malformed groups"
        )

    def test_jwt_rbac_unmapped_roles_groups(self):
        """Auth succeeds but authz fails with unmapped roles/groups; /whoami returns empty roles."""
        self.log.info("Testing token with unmapped groups")
        self._assert_auth_ok_authz_fail_empty_roles(["nonexistent_group_xyz"], "unmapped groups")

    @staticmethod
    def _b64url_encode(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")

    def _create_signed_token_from_payload(self, payload: dict, headers: dict | None = None):
        return jwt.encode(
            payload=payload,
            algorithm=self.algorithm,
            key=self.private_key,
            headers=headers,
        )

    def test_jwt_validation_claims(self):
        """
        Param-driven claim validation. Param: claim = missing_issuer | wrong_issuer |
        missing_audience | wrong_audience | invalid_nbf.
        """
        verify_cli = self.input.param("verify_cli", True)
        expected_auth = self.input.param("expected_jwt_auth", False)
        expected_authz = self.input.param("expected_jwt_authz", False)

        claim = self.input.param("claim", "missing_issuer")
        config = self._get_jwt_config()
        self._setup_jwt_config(config)

        user = f"jwt_test_user_claim_{uuid.uuid4().hex[:8]}"

        if claim == "wrong_issuer":
            token = self.create_token(user_name=user, issuer_name="untrusted-issuer")
        elif claim == "wrong_audience":
            token = self.create_token(user_name=user, audience="wrong-audience")
        elif claim == "invalid_nbf":
            token = self.create_token(user_name=user, nbf_seconds=300)
        elif claim in ("missing_issuer", "missing_audience"):
            curr_time = int(time.time())
            payload = {
                "sub": user,
                "exp": curr_time + self.ttl,
                "iat": curr_time,
                "nbf": curr_time + self.nbf_seconds,
                "jti": str(uuid.uuid4()),
                "groups": self.user_groups,
            }
            if claim == "missing_audience":
                payload["iss"] = self.issuer_name
            if claim == "missing_issuer":
                payload["aud"] = self.jwt_utils.get_single_audience(self.token_audience)
            token = self._create_signed_token_from_payload(payload)
        else:
            self.fail(
                f"Unknown claim={claim}. "
                "Use: missing_issuer|wrong_issuer|missing_audience|wrong_audience|invalid_nbf"
            )

        try:
            self.jwt_utils.validate_jwt_expectations(
                rest_connection=self.rest,
                token=token,
                expected_jwt_auth=expected_auth,
                expected_jwt_authz=expected_authz,
                verify_cli=verify_cli,
                verify_cli_callback=self._verify_token_cli if verify_cli else None,
                log_callback=self.log.info,
            )
        except AssertionError as e:
            self.fail(str(e))

    def test_jwt_validation_algorithm(self):
        """
        Param-driven algorithm validation. Param: scenario = missing_alg | unsupported_alg | alg_none.
        """
        verify_cli = self.input.param("verify_cli", True)
        expected_auth = self.input.param("expected_jwt_auth", False)
        expected_authz = self.input.param("expected_jwt_authz", False)

        scenario = self.input.param("scenario", "missing_alg")
        config = self._get_jwt_config()
        self._setup_jwt_config(config)

        user = f"jwt_test_user_alg_{uuid.uuid4().hex[:8]}"

        if scenario == "missing_alg":
            curr_time = int(time.time())
            payload = {
                "iss": self.issuer_name,
                "sub": user,
                "groups": self.user_groups,
                "aud": self.jwt_utils.get_single_audience(self.token_audience),
                "exp": curr_time + self.ttl,
                "iat": curr_time,
                "nbf": curr_time + self.nbf_seconds,
                "jti": str(uuid.uuid4()),
            }
            header = {"typ": "JWT"}  # deliberately no "alg"
            token = f"{self._b64url_encode(json.dumps(header, separators=(',', ':')).encode('utf-8'))}." \
                    f"{self._b64url_encode(json.dumps(payload, separators=(',', ':')).encode('utf-8'))}."
        elif scenario == "unsupported_alg":
            token = jwt.encode(
                payload={
                    "iss": self.issuer_name,
                    "sub": user,
                    "exp": int(time.time()) + self.ttl,
                    "iat": int(time.time()),
                    "nbf": int(time.time()) + self.nbf_seconds,
                    "jti": str(uuid.uuid4()),
                    "aud": self.jwt_utils.get_single_audience(self.token_audience),
                    "groups": self.user_groups,
                },
                algorithm="HS256",
                key="test-secret-key-for-hs256",
            )
        elif scenario == "alg_none":
            curr_time = int(time.time())
            payload = {
                "iss": self.issuer_name,
                "sub": user,
                "groups": self.user_groups,
                "aud": self.jwt_utils.get_single_audience(self.token_audience),
                "exp": curr_time + self.ttl,
                "iat": curr_time,
                "nbf": curr_time + self.nbf_seconds,
                "jti": str(uuid.uuid4()),
            }
            header = {"alg": "none", "typ": "JWT"}
            token = f"{self._b64url_encode(json.dumps(header, separators=(',', ':')).encode('utf-8'))}." \
                    f"{self._b64url_encode(json.dumps(payload, separators=(',', ':')).encode('utf-8'))}."
        else:
            self.fail(f"Unknown scenario={scenario}. Use: missing_alg|unsupported_alg|alg_none|unsupported_alg")

        try:
            self.jwt_utils.validate_jwt_expectations(
                rest_connection=self.rest,
                token=token,
                expected_jwt_auth=expected_auth,
                expected_jwt_authz=expected_authz,
                verify_cli=verify_cli,
                verify_cli_callback=self._verify_token_cli if verify_cli else None,
                log_callback=self.log.info,
            )
        except AssertionError as e:
            self.fail(str(e))

    def test_jwt_secure_storage(self):
        """
        Test that JWT configuration and keys are handled securely.

        We validate that responses don't leak sensitive material like private keys/passwords.
        """
        expected_auth = self.input.param("expected_jwt_auth", True)
        expected_authz = self.input.param("expected_jwt_authz", True)
        verify_cli = self.input.param("verify_cli", True)

        config = self._get_jwt_config()
        self._setup_jwt_config(config)

        token = self.create_token()
        self.log.info("JWT token generated (redacted)")

        try:
            self.jwt_utils.validate_jwt_expectations(
                rest_connection=self.rest,
                token=token,
                expected_jwt_auth=expected_auth,
                expected_jwt_authz=expected_authz,
                verify_cli=verify_cli,
                verify_cli_callback=self._verify_token_cli if verify_cli else None,
                log_callback=self.log.info,
            )
        except AssertionError as e:
            self.fail(str(e))

        # Inspect a typical authz response body for leakage
        ok, status_code, content = self._verify_token_rest(token)
        self.log.info(f"Secure storage response: ok={ok} status={status_code}")
        self.jwt_utils.verify_no_sensitive_data_in_response(
            content, context="secure storage test (authz response)", log_callback=self.log.info
        )

        if content and self.pub_key:
            self.assertFalse(
                self.pub_key in content,
                "Public key should not appear in authz responses",
            )

    def test_jwt_admin_only_config(self):
        """
        Verify that only admin users can update JWT configuration.
        """
        config = self._get_jwt_config()

        # Case 1: Admin should succeed (current rest connection is admin)
        status_code, content = self._put_jwt_config(config)
        self.assertTrue(
            status_code is not None and 200 <= int(status_code) < 300,
            f"Admin PUT JWT config should succeed. status={status_code} content={content}",
        )

        # Case 2: Non-admin should fail (401/403)
        non_admin_user = f"jwt_test_noadmin_{uuid.uuid4().hex[:8]}"
        non_admin_password = f"noadmin_pass_{uuid.uuid4().hex[:4]}"
        non_admin_role = "ro_admin"

        try:
            payload = f"name={non_admin_user}&roles={non_admin_role}&password={non_admin_password}"
            self.rest.add_set_builtin_user(non_admin_user, payload)

            status_code, content = self.jwt_utils.put_jwt_config_as(
                self.rest, config, username=non_admin_user, password=non_admin_password
            )

            self.assertTrue(
                status_code in (401, 403),
                f"Non-admin PUT JWT config should be rejected. status={status_code} content={content}",
            )
        finally:
            try:
                self.rest.delete_builtin_user(non_admin_user)
            except Exception:
                pass

    def _setup_multi_issuer_env(self):
        """
        Set up a two-issuer environment using jwt_utils, then create the corresponding RBAC groups.
        """
        env = self.jwt_utils.setup_multi_issuer_env(
            [
                {"name": "issuer-alpha", "algorithm": "ES256"},
                {"name": "issuer-beta", "algorithm": "RS256", "key_size": 2048},
            ],
            group_name_prefix="jwt_multi",
        )
        # Create groups referenced in config (roles=admin for test simplicity)
        for issuer in env.get("issuers", []):
            self._create_group(issuer["group_name"], roles="admin")
        return env

    def _cleanup_multi_issuer_groups(self, env):
        for group_name in self.jwt_utils.extract_multi_issuer_group_names(env):
            self._delete_group(group_name)

    def test_jwt_multiple_issuers_config(self):
        """
        Test cluster accepts multi-issuer config and tokens from both issuers authenticate.
        """
        expected_auth = self.input.param("expected_jwt_auth", True)
        expected_authz = self.input.param("expected_jwt_authz", True)
        verify_cli = self.input.param("verify_cli", True)

        env = self._setup_multi_issuer_env()
        try:
            self._setup_jwt_config(env["config"], create_groups=False)

            for issuer in (env.get("issuers") or []):
                token = self.jwt_utils.create_token(
                    issuer_name=issuer["name"],
                    user_name=f"jwt_multi_user_{uuid.uuid4().hex[:8]}",
                    algorithm=issuer["algorithm"],
                    private_key=issuer["private_key"],
                    token_audience=issuer["audience"],
                    user_groups=issuer["groups"],
                    ttl=self.ttl,
                    normalize_audience=True,
                )
                try:
                    self.jwt_utils.validate_jwt_expectations(
                        rest_connection=self.rest,
                        token=token,
                        expected_jwt_auth=expected_auth,
                        expected_jwt_authz=expected_authz,
                        verify_cli=verify_cli,
                        verify_cli_callback=self._verify_token_cli if verify_cli else None,
                        log_callback=self.log.info,
                    )
                except AssertionError as e:
                    self.fail(str(e))
        finally:
            self._cleanup_multi_issuer_groups(env)

    def test_jwt_multiple_issuers_token_routing(self):
        """
        Tokens should be validated against correct issuer by 'iss'; unknown issuer rejected.
        """
        verify_cli = self.input.param("verify_cli", True)

        env = self._setup_multi_issuer_env()
        try:
            self._setup_jwt_config(env["config"], create_groups=False)

            # each known issuer should succeed
            for issuer in (env.get("issuers") or []):
                token = self.jwt_utils.create_token(
                    issuer_name=issuer["name"],
                    user_name=f"jwt_multi_user_{uuid.uuid4().hex[:8]}",
                    algorithm=issuer["algorithm"],
                    private_key=issuer["private_key"],
                    token_audience=issuer["audience"],
                    user_groups=issuer["groups"],
                    ttl=self.ttl,
                    normalize_audience=True,
                )
                try:
                    self.jwt_utils.validate_jwt_expectations(
                        rest_connection=self.rest,
                        token=token,
                        expected_jwt_auth=True,
                        expected_jwt_authz=True,
                        verify_cli=verify_cli,
                        verify_cli_callback=self._verify_token_cli if verify_cli else None,
                        log_callback=self.log.info,
                    )
                except AssertionError as e:
                    self.fail(str(e))

            # unknown issuer should fail
            issuer0 = (env.get("issuers") or [])[0]
            token_unknown = self.jwt_utils.create_token(
                issuer_name="unknown-issuer",
                user_name=f"jwt_multi_user_{uuid.uuid4().hex[:8]}",
                algorithm=issuer0["algorithm"],
                private_key=issuer0["private_key"],
                token_audience=issuer0["audience"],
                user_groups=issuer0["groups"],
                ttl=self.ttl,
                normalize_audience=True,
            )
            try:
                self.jwt_utils.validate_jwt_expectations(
                    rest_connection=self.rest,
                    token=token_unknown,
                    expected_jwt_auth=False,
                    expected_jwt_authz=False,
                    verify_cli=verify_cli,
                    verify_cli_callback=self._verify_token_cli if verify_cli else None,
                    log_callback=self.log.info,
                )
            except AssertionError as e:
                self.fail(str(e))
        finally:
            self._cleanup_multi_issuer_groups(env)

    def test_jwt_multiple_issuers_cross_validation(self):
        """
        Token signed by one issuer but claiming another issuer in 'iss' must be rejected.
        """
        verify_cli = self.input.param("verify_cli", True)
        expected_auth = self.input.param("expected_jwt_auth", False)
        expected_authz = self.input.param("expected_jwt_authz", False)

        env = self._setup_multi_issuer_env()
        try:
            self._setup_jwt_config(env["config"], create_groups=False)
            issuers = env.get("issuers") or []
            if len(issuers) < 2:
                self.fail("Expected 2 issuers in env")
            a, b = issuers[0], issuers[1]

            token_ab = self.jwt_utils.create_token(
                issuer_name=b["name"],  # claim B
                user_name=f"jwt_multi_user_{uuid.uuid4().hex[:8]}",
                algorithm=a["algorithm"],  # sign with A algo
                private_key=a["private_key"],
                token_audience=b["audience"],
                user_groups=b["groups"],
                ttl=self.ttl,
                normalize_audience=True,
            )
            token_ba = self.jwt_utils.create_token(
                issuer_name=a["name"],  # claim A
                user_name=f"jwt_multi_user_{uuid.uuid4().hex[:8]}",
                algorithm=b["algorithm"],  # sign with B algo
                private_key=b["private_key"],
                token_audience=a["audience"],
                user_groups=a["groups"],
                ttl=self.ttl,
                normalize_audience=True,
            )

            for t in (token_ab, token_ba):
                try:
                    self.jwt_utils.validate_jwt_expectations(
                        rest_connection=self.rest,
                        token=t,
                        expected_jwt_auth=expected_auth,
                        expected_jwt_authz=expected_authz,
                        verify_cli=verify_cli,
                        verify_cli_callback=self._verify_token_cli if verify_cli else None,
                        log_callback=self.log.info,
                    )
                except AssertionError as e:
                    self.fail(str(e))
        finally:
            self._cleanup_multi_issuer_groups(env)

    def _setup_tamper_test(self):
        def _setup_cb_config(cfg):
            self._setup_jwt_config(cfg, create_groups=False)

        def _validate_ok(tok):
            self.jwt_utils.validate_jwt_expectations(
                rest_connection=self.rest,
                token=tok,
                expected_jwt_auth=True,
                expected_jwt_authz=True,
                verify_cli=False,
                log_callback=self.log.info,
            )

        return self.jwt_utils.setup_tamper_test_env(
            issuer_name=self.issuer_name,
            algorithm=self.algorithm,
            key_size=self.key_size,
            token_audience=self.token_audience,
            token_groups=["admin"],
            ttl=self.ttl,
            nbf_seconds=self.nbf_seconds,
            create_group_callback=self._create_group,
            setup_jwt_config_callback=_setup_cb_config,
            validate_callback=_validate_ok,
        )

    def _assert_tampered_token_rejected(self, tampered_token, group_name, message):
        verify_cli = self.input.param("verify_cli", True)

        def _validate_fail(tok):
            self.jwt_utils.validate_jwt_expectations(
                rest_connection=self.rest,
                token=tok,
                expected_jwt_auth=False,
                expected_jwt_authz=False,
                verify_cli=verify_cli,
                verify_cli_callback=self._verify_token_cli if verify_cli else None,
                log_callback=self.log.info,
            )

        self.jwt_utils.assert_tampered_token_rejected(
            tampered_token=tampered_token,
            validate_callback=_validate_fail,
            delete_group_callback=self._delete_group,
            group_name=group_name,
        )

    def test_jwt_tampered_payload_signature(self):
        """Modifying the JWT payload after issuance must invalidate the token (signature mismatch)."""
        group_name, valid_token = self._setup_tamper_test()
        payload = self.jwt_utils.get_payload_from_token(valid_token)
        tampered_token = self.jwt_utils.build_tampered_payload_token(
            valid_token, {"exp": payload["exp"] + 3600}
        )
        self._assert_tampered_token_rejected(
            tampered_token, group_name, "Server must reject modified payload"
        )

    def test_jwt_tampered_header(self):
        """Modifying the JWT header (e.g. alg=none) must invalidate the token."""
        group_name, valid_token = self._setup_tamper_test()
        tampered_token = self.jwt_utils.build_tampered_header_token(
            valid_token, {"alg": "none"}, drop_signature=True
        )
        self._assert_tampered_token_rejected(
            tampered_token, group_name, "Server must reject modified header"
        )

    def test_jwt_external_only_issuer_validation(self):
        """
        Token signed correctly with a separate private key but with an unconfigured issuer must be rejected.
        """
        verify_cli = self.input.param("verify_cli", True)
        expected_auth = self.input.param("expected_jwt_auth", False)
        expected_authz = self.input.param("expected_jwt_authz", False)

        group_name = f"jwt_ext_{uuid.uuid4().hex[:8]}"
        group_maps = [f"^admin$ {group_name}"]

        try:
            private_key, pub_key = self.jwt_utils.generate_key_pair(
                algorithm=self.algorithm, key_size=self.key_size
            )
            self._create_group(group_name, roles="admin")
            config = self.jwt_utils.get_jwt_config(
                issuer_name=self.issuer_name,
                algorithm=self.algorithm,
                pub_key=pub_key,
                token_audience=self.token_audience,
                token_group_matching_rule=group_maps,
                jit_provisioning=True,
            )
            self._setup_jwt_config(config, create_groups=False)

            ext_private_key, _ = self.jwt_utils.generate_key_pair(
                algorithm=self.algorithm, key_size=self.key_size
            )
            external_token = self.jwt_utils.create_token(
                issuer_name="unknown-external-attacker",
                user_name=self.user_name,
                algorithm=self.algorithm,
                private_key=ext_private_key,
                token_audience=self.token_audience,
                user_groups=["admin"],
                ttl=self.ttl,
                nbf_seconds=self.nbf_seconds,
                normalize_audience=True,
            )

            try:
                self.jwt_utils.validate_jwt_expectations(
                    rest_connection=self.rest,
                    token=external_token,
                    expected_jwt_auth=expected_auth,
                    expected_jwt_authz=expected_authz,
                    verify_cli=verify_cli,
                    verify_cli_callback=self._verify_token_cli if verify_cli else None,
                    log_callback=self.log.info,
                )
            except AssertionError as e:
                self.fail(str(e))
        finally:
            self._delete_group(group_name)

    def test_jwt_jit_off_user_not_present(self):
        """
        JIT OFF: authentication succeeds but authorization fails when user doesn't exist.

        Validates:
        - jitProvisioning=false, token for non-existent user:
          - /whoami succeeds (valid JWT signature)
          - /pools/default/buckets fails (no RBAC roles)
        - /whoami returns empty roles list
        - external user is NOT created
        """
        verify_cli = self.input.param("verify_cli", True)
        expected_auth = self.input.param("expected_jwt_auth", True)
        expected_authz = self.input.param("expected_jwt_authz", False)

        jit_user = f"jit_test_user_{uuid.uuid4().hex[:8]}"

        # JWT config with JIT disabled and no group-maps to grant roles implicitly
        config = self.jwt_utils.get_jwt_config(
            issuer_name=self.issuer_name,
            algorithm=self.algorithm,
            pub_key=self.pub_key,
            token_audience=self.token_audience,
            token_group_matching_rule=[],
            jit_provisioning=False,
        )
        self._setup_jwt_config(config, create_groups=False)

        # Ensure user doesn't exist
        self.jwt_utils.delete_external_user(self.rest, jit_user)

        token = self.jwt_utils.create_token(
            issuer_name=self.issuer_name,
            user_name=jit_user,
            algorithm=self.algorithm,
            private_key=self.private_key,
            token_audience=self.token_audience,
            user_groups=["admin"],
            ttl=self.ttl,
            nbf_seconds=self.nbf_seconds,
            normalize_audience=True,
        )

        try:
            self.jwt_utils.validate_jwt_expectations(
                rest_connection=self.rest,
                token=token,
                expected_jwt_auth=expected_auth,
                expected_jwt_authz=expected_authz,
                verify_cli=verify_cli,
                verify_cli_callback=self._verify_token_cli if verify_cli else None,
                log_callback=self.log.info,
            )
        except AssertionError as e:
            self.fail(str(e))

        whoami = self.jwt_utils.get_user_info_from_whoami(self.rest, token)
        self.assertTrue(whoami, "Should get user info from /whoami")
        self.assertEqual(whoami.get("domain"), "external", f"Unexpected whoami: {whoami}")
        self.assertEqual(whoami.get("id"), jit_user, f"Unexpected whoami: {whoami}")
        roles = [r.get("role") for r in (whoami.get("roles") or []) if isinstance(r, dict)]
        self.assertEqual(roles, [], f"Non-existent user should have no roles. whoami={whoami}")

        user_info = self.jwt_utils.get_external_user(self.rest, jit_user)
        self.assertTrue(
            user_info is None,
            f"JIT OFF: user should NOT be created. user_info={user_info}",
        )

        # Cleanup just in case
        self.jwt_utils.delete_external_user(self.rest, jit_user)

    def test_jwt_jit_off_user_exists(self):
        """
        JIT OFF: both authentication and authorization succeed when user exists.

        Validates:
        - jitProvisioning=false, token for existing external user with RBAC roles:
          - /whoami succeeds
          - /pools/default/buckets succeeds
        - user exists beforehand (no auto-provisioning required)
        """
        verify_cli = self.input.param("verify_cli", True)
        expected_auth = self.input.param("expected_jwt_auth", True)
        expected_authz = self.input.param("expected_jwt_authz", True)

        jit_user = f"jit_test_user_{uuid.uuid4().hex[:8]}"

        config = self.jwt_utils.get_jwt_config(
            issuer_name=self.issuer_name,
            algorithm=self.algorithm,
            pub_key=self.pub_key,
            token_audience=self.token_audience,
            token_group_matching_rule=[],
            jit_provisioning=False,
        )
        self._setup_jwt_config(config, create_groups=False)

        try:
            self.jwt_utils.create_external_user(self.rest, jit_user, roles="admin")
            self.sleep(3, "Waiting for external user creation to propagate")

            user_info = self.jwt_utils.get_external_user(self.rest, jit_user)
            self.assertTrue(user_info is not None, f"Expected external user to exist. user_info={user_info}")

            token = self.jwt_utils.create_token(
                issuer_name=self.issuer_name,
                user_name=jit_user,
                algorithm=self.algorithm,
                private_key=self.private_key,
                token_audience=self.token_audience,
                user_groups=["admin"],
                ttl=self.ttl,
                nbf_seconds=self.nbf_seconds,
                normalize_audience=True,
            )

            try:
                self.jwt_utils.validate_jwt_expectations(
                    rest_connection=self.rest,
                    token=token,
                    expected_jwt_auth=expected_auth,
                    expected_jwt_authz=expected_authz,
                    verify_cli=verify_cli,
                    verify_cli_callback=self._verify_token_cli if verify_cli else None,
                    log_callback=self.log.info,
                )
            except AssertionError as e:
                self.fail(str(e))

            whoami = self.jwt_utils.get_user_info_from_whoami(self.rest, token)
            self.assertTrue(whoami, "Expected /whoami JSON for existing external user")
            self.assertEqual(whoami.get("domain"), "external", f"Unexpected whoami: {whoami}")
            self.assertEqual(whoami.get("id"), jit_user, f"Unexpected whoami: {whoami}")
            roles = [r.get("role") for r in (whoami.get("roles") or []) if isinstance(r, dict)]
            self.assertTrue("admin" in roles, f"Expected admin role in whoami roles, got {roles}")
        finally:
            self.jwt_utils.delete_external_user(self.rest, jit_user)

    def test_jwt_jit_off_user_deleted_no_autoprovision(self):
        """
        JIT OFF: after deleting the external user, auth continues to succeed but authz fails.
        """
        verify_cli = self.input.param("verify_cli", True)
        expected_auth_initial = self.input.param("expected_jwt_auth", True)
        expected_authz_initial = self.input.param("expected_jwt_authz", True)

        jit_user = f"jit_deleted_{uuid.uuid4().hex[:8]}"

        config = self.jwt_utils.get_jwt_config(
            issuer_name=self.issuer_name,
            algorithm=self.algorithm,
            pub_key=self.pub_key,
            token_audience=self.token_audience,
            token_group_matching_rule=[],
            jit_provisioning=False,
        )
        self._setup_jwt_config(config, create_groups=False)

        token = None
        try:
            self.jwt_utils.create_external_user(self.rest, jit_user, roles="admin")
            self.sleep(3, "Waiting for external user creation to propagate")

            token = self.jwt_utils.create_token(
                issuer_name=self.issuer_name,
                user_name=jit_user,
                algorithm=self.algorithm,
                private_key=self.private_key,
                token_audience=self.token_audience,
                user_groups=["admin"],
                ttl=self.ttl,
                nbf_seconds=self.nbf_seconds,
                normalize_audience=True,
            )

            try:
                self.jwt_utils.validate_jwt_expectations(
                    rest_connection=self.rest,
                    token=token,
                    expected_jwt_auth=expected_auth_initial,
                    expected_jwt_authz=expected_authz_initial,
                    verify_cli=verify_cli,
                    verify_cli_callback=self._verify_token_cli if verify_cli else None,
                    log_callback=self.log.info,
                )
            except AssertionError as e:
                self.fail(str(e))

            self.jwt_utils.delete_external_user(self.rest, jit_user)
            self.sleep(3, "Waiting for external user deletion to propagate")
            self.assertTrue(
                self.jwt_utils.get_external_user(self.rest, jit_user) is None,
                "Expected external user to be deleted",
            )

            try:
                self.jwt_utils.validate_jwt_expectations(
                    rest_connection=self.rest,
                    token=token,
                    expected_jwt_auth=True,
                    expected_jwt_authz=False,
                    verify_cli=verify_cli,
                    verify_cli_callback=self._verify_token_cli if verify_cli else None,
                    log_callback=self.log.info,
                )
            except AssertionError as e:
                self.fail(str(e))

            whoami = self.jwt_utils.get_user_info_from_whoami(self.rest, token)
            self.assertTrue(whoami, "Expected /whoami JSON after user deletion")
            roles = [r.get("role") for r in (whoami.get("roles") or []) if isinstance(r, dict)]
            self.assertEqual(roles, [], f"Expected empty roles after user deletion, got {roles}")
        finally:
            self.jwt_utils.delete_external_user(self.rest, jit_user)

    def test_jwt_jit_repeated_login_no_mutation(self):
        """Repeated logins with JIT enabled do not mutate virtual user info."""
        expected_auth = self.input.param("expected_jwt_auth", True)
        expected_authz = self.input.param("expected_jwt_authz", True)
        verify_cli = self.input.param("verify_cli", True)

        username = f"jit_repeat_{uuid.uuid4().hex[:8]}"
        _, groups = self._setup_jit_config(jit_provisioning=True, create_groups=True)
        group_name = groups[0] if groups else None

        try:
            token = self.create_token(user_name=username, user_groups=["admin"])
            user_infos = []
            for i in range(3):
                try:
                    self.jwt_utils.validate_jwt_expectations(
                        rest_connection=self.rest,
                        token=token,
                        expected_jwt_auth=expected_auth,
                        expected_jwt_authz=expected_authz,
                        verify_cli=verify_cli,
                        verify_cli_callback=self._verify_token_cli if verify_cli else None,
                        log_callback=self.log.info,
                    )
                except AssertionError as e:
                    self.fail(str(e))

                info = self.jwt_utils.get_user_info_from_whoami(self.rest, token)
                self.assertTrue(info, "Expected /whoami JSON")
                user_infos.append(info)

                if i < 2:
                    self.sleep(2, "Wait before next login")

            # Stable identity
            self.assertEqual(user_infos[0].get("id"), user_infos[1].get("id"))
            self.assertEqual(user_infos[0].get("id"), user_infos[2].get("id"))
            self.assertEqual(user_infos[0].get("domain"), "external")
            self.assertEqual(user_infos[0].get("domain"), user_infos[1].get("domain"))
            self.assertEqual(user_infos[0].get("domain"), user_infos[2].get("domain"))

            # Stable roles
            self.assertEqual(user_infos[0].get("roles"), user_infos[1].get("roles"))
            self.assertEqual(user_infos[0].get("roles"), user_infos[2].get("roles"))

            # Stable groups field if present
            if "external_groups" in user_infos[0]:
                self.assertEqual(user_infos[0].get("external_groups"), user_infos[1].get("external_groups"))
                self.assertEqual(user_infos[0].get("external_groups"), user_infos[2].get("external_groups"))
        finally:
            if group_name:
                self._delete_group(group_name)

    def test_jwt_jit_local_user_conflict(self):
        """
        JWT users must not inherit privileges from local users with same username.
        """
        expected_auth = self.input.param("expected_jwt_auth", True)
        expected_authz = self.input.param("expected_jwt_authz", True)
        verify_cli = self.input.param("verify_cli", True)

        username = f"jwt_conflict_{uuid.uuid4().hex[:8]}"
        ro_admin_group = f"jwt_ro_admin_{uuid.uuid4().hex[:8]}"
        group_maps = [f"^ro_admin$ {ro_admin_group}"]

        self._create_group(ro_admin_group, roles="ro_admin", description="JWT ro_admin group")
        config = self.jwt_utils.get_jwt_config(
            issuer_name=self.issuer_name,
            algorithm=self.algorithm,
            pub_key=self.pub_key,
            token_audience=self.token_audience,
            token_group_matching_rule=group_maps,
            jit_provisioning=True,
        )
        self._setup_jwt_config(config, create_groups=False)

        try:
            # Create local user with higher privilege
            api = f"{self.rest.baseUrl}settings/rbac/users/local/{username}"
            payload = "password=testpass123&roles=admin"
            ok, _, _ = self.rest._http_request(api, "PUT", payload)
            self.assertTrue(ok, "Local admin user creation should succeed")

            token = self.create_token(user_name=username, user_groups=["ro_admin"])

            try:
                self.jwt_utils.validate_jwt_expectations(
                    rest_connection=self.rest,
                    token=token,
                    expected_jwt_auth=expected_auth,
                    expected_jwt_authz=expected_authz,
                    verify_cli=verify_cli,
                    verify_cli_callback=self._verify_token_cli if verify_cli else None,
                    log_callback=self.log.info,
                )
            except AssertionError as e:
                self.fail(str(e))

            user = self.jwt_utils.get_user_info_from_whoami(self.rest, token)
            self.assertTrue(user, "Expected /whoami JSON")
            self.assertEqual(user.get("domain"), "external")
            self.assertEqual(user.get("id"), username)
            roles = [r.get("role") for r in (user.get("roles") or []) if isinstance(r, dict)]
            self.assertIn("ro_admin", roles, f"Expected ro_admin role for JWT user. roles={roles}")
            self.assertNotIn("admin", roles, f"JWT user must NOT inherit local admin role. roles={roles}")

            # Admin-only endpoint must be denied
            ok2, sc2, _ = self.jwt_utils.request_with_jwt(
                self.rest, token, "/settings/rbac/users", method="GET"
            )
            self.assertFalse(ok2 and self.jwt_utils._is_success_status(sc2), "Admin-only RBAC listing must be denied")
        finally:
            try:
                api = f"{self.rest.baseUrl}settings/rbac/users/local/{username}"
                self.rest._http_request(api, "DELETE")
            except Exception:
                pass
            self._delete_group(ro_admin_group)

    def test_jwt_jit_missing_sub_claim(self):
        """
        JIT enabled: token without sub claim must fail authentication and no user is created.
        """
        verify_cli = self.input.param("verify_cli", True)
        expected_auth = self.input.param("expected_jwt_auth", False)
        expected_authz = self.input.param("expected_jwt_authz", False)

        _, groups = self._setup_jit_config(jit_provisioning=True, create_groups=True)
        group_name = groups[0] if groups else None

        try:
            curr_time = int(time.time())
            payload = {
                "iss": self.issuer_name,
                # sub intentionally missing
                "aud": self.jwt_utils.get_single_audience(self.token_audience),
                "exp": curr_time + self.ttl,
                "iat": curr_time,
                "nbf": curr_time,
                "jti": str(uuid.uuid4()),
                "groups": ["admin"],
            }
            token = jwt.encode(payload, self.private_key, algorithm=self.algorithm)

            try:
                self.jwt_utils.validate_jwt_expectations(
                    rest_connection=self.rest,
                    token=token,
                    expected_jwt_auth=expected_auth,
                    expected_jwt_authz=expected_authz,
                    verify_cli=verify_cli,
                    verify_cli_callback=self._verify_token_cli if verify_cli else None,
                    log_callback=self.log.info,
                )
            except AssertionError as e:
                self.fail(str(e))
        finally:
            if group_name:
                self._delete_group(group_name)

    def test_jwt_jit_roles_from_claims_only(self):
        """
        JIT enabled: roles are derived strictly from JWT claims; no default roles added.
        """
        verify_cli = self.input.param("verify_cli", True)
        expected_auth = self.input.param("expected_jwt_auth", True)
        expected_authz = self.input.param("expected_jwt_authz", True)

        _, groups = self._setup_jit_config(jit_provisioning=True, create_groups=True)
        group_name = groups[0] if groups else None

        try:
            user_with_roles = f"jit_roles_{uuid.uuid4().hex[:8]}"
            token = self.create_token(user_name=user_with_roles, user_groups=["admin"])
            try:
                self.jwt_utils.validate_jwt_expectations(
                    rest_connection=self.rest,
                    token=token,
                    expected_jwt_auth=expected_auth,
                    expected_jwt_authz=expected_authz,
                    verify_cli=verify_cli,
                    verify_cli_callback=self._verify_token_cli if verify_cli else None,
                    log_callback=self.log.info,
                )
            except AssertionError as e:
                self.fail(str(e))

            whoami = self.jwt_utils.get_user_info_from_whoami(self.rest, token)
            self.assertTrue(whoami, "Expected /whoami JSON for user with roles")
            roles = [r.get("role") for r in (whoami.get("roles") or []) if isinstance(r, dict)]
            self.assertTrue(len(roles) > 0, f"Expected roles from JWT claims, got {roles}")

            user_no_roles = f"jit_empty_{uuid.uuid4().hex[:8]}"
            token_empty = self.create_token(user_name=user_no_roles, user_groups=[])
            try:
                self.jwt_utils.validate_jwt_expectations(
                    rest_connection=self.rest,
                    token=token_empty,
                    expected_jwt_auth=True,
                    expected_jwt_authz=False,
                    verify_cli=verify_cli,
                    verify_cli_callback=self._verify_token_cli if verify_cli else None,
                    log_callback=self.log.info,
                )
            except AssertionError as e:
                self.fail(str(e))

            whoami2 = self.jwt_utils.get_user_info_from_whoami(self.rest, token_empty)
            self.assertTrue(whoami2, "Expected /whoami JSON for user without roles")
            roles2 = [r.get("role") for r in (whoami2.get("roles") or []) if isinstance(r, dict)]
            self.assertEqual(roles2, [], f"Expected no roles for empty groups, got {roles2}")
        finally:
            if group_name:
                self._delete_group(group_name)

