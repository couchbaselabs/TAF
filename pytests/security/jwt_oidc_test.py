import time
import urllib.parse

from membase.api.rest_client import RestConnection
from shell_util.remote_connection import RemoteMachineShellConnection

from couchbase_utils.security_utils import jwt_utils
from pytests.security.jwt_oidc_base import JWTOIDCBase


class JWTOIDCTest(JWTOIDCBase):
    """
    JWT/OIDC authentication tests for Couchbase Server with Keycloak IdP.

    All setUp/tearDown/helpers live in JWTOIDCBase.
    This class contains only test_* methods.
    """

    def test_oidc_disable_preserves_local_admin_auth(self):
        """
        Test disabling OIDC preserves local admin authentication.

        Steps:
        1. Enable OIDC config
        2. Disable JWT/OIDC
        3. Verify config shows disabled
        4. Verify local admin auth still works
        5. Verify OIDC auth endpoint is blocked

        Expected:
        - OIDC disabled successfully
        - Local admin auth unaffected
        - OIDC login endpoint blocked/unavailable
        """
        self.log.info("Testing OIDC disable preserves local admin auth")

        self._enable_oidc_config(sleep_seconds=5)

        self.jwt_utils.disable_jwt(self.rest)
        self.sleep(5, "Waiting for disable to take effect")

        get_content = self._get_parsed_jwt_config()
        self.assertFalse(
            get_content.get("enabled", True),
            "JWT should be disabled after disable call",
        )

        # Verify local admin auth still works (no JWT bearer, uses basic auth)
        api = f"{self.rest.baseUrl}{jwt_utils.ENDPOINT_POOLS_DEFAULT.lstrip('/')}"
        ok_local, _, _ = self.rest._http_request(api, "GET")
        self.assertTrue(ok_local, "Local admin auth should work after OIDC disable")

        # Verify OIDC login flow does NOT redirect to IdP when disabled
        encoded_issuer = urllib.parse.quote(self.issuer_url, safe="")
        oidc_auth_url = (
            f"{self.rest.baseUrl}{jwt_utils.ENDPOINT_OIDC_AUTH.lstrip('/')}"
            f"?issuer={encoded_issuer}"
        )
        _, _, resp_oidc = self.rest._http_request(oidc_auth_url, "GET")

        location = ""
        if resp_oidc and hasattr(resp_oidc, "headers"):
            location = resp_oidc.headers.get("Location", "")
        elif isinstance(resp_oidc, dict):
            location = resp_oidc.get("Location", "")

        self.log.info(f"OIDC auth endpoint response: location={location}")
        self.assertFalse(
            "openid-connect/auth" in location,
            f"OIDC redirect to IdP should not happen when JWT disabled, got location={location}",
        )

        self.log.info("OIDC disabled, local auth preserved, no IdP redirect")


    def test_oidc_config_persists_after_restart(self):
        """
        Test that OIDC config is retained after ns_server restart.

        Steps:
        1. Enable OIDC config
        2. Restart ns_server on master node
        3. Verify config is retained

        Expected:
        - OIDC config retained after restart
        """
        self.log.info("Testing OIDC config persistence after restart")

        self._enable_oidc_config(sleep_seconds=5)

        self.log.info("Restarting ns_server on master node")
        shell_conn = RemoteMachineShellConnection(self.cluster.master)
        try:
            shell_conn.restart_couchbase()
            self.sleep(30, "Waiting for Couchbase to restart")
        finally:
            shell_conn.disconnect()

        self.sleep(15, "Waiting for cluster to stabilize")

        get_content = self._get_parsed_jwt_config()
        self.assertTrue(
            get_content.get("enabled"),
            "JWT should still be enabled after restart",
        )

        issuers = get_content.get("issuers", [])
        issuer_names = [i.get("name") for i in issuers]
        self.assertIn(
            self.issuer_url, issuer_names,
            f"Issuer should persist after restart. Found: {issuer_names}",
        )

        self.log.info("OIDC config persisted after restart")


    def test_oidc_admin_login_with_keycloak_token(self):
        """
        Test successful admin login using JWT from Keycloak.
        Also validates OIDC config enable/persistence.

        Steps:
        1. Verify Keycloak connectivity
        2. Enable OIDC config and verify it's persisted
        3. Get JWT token from Keycloak via password grant
        4. Verify authentication via /whoami with identity assertions
        5. Verify authorization via /pools/default (admin operation)

        Expected:
        - Config enabled and persisted correctly
        - Token obtained from Keycloak
        - Authentication succeeds with correct user identity
        - Authorization succeeds for admin operations
        """
        self.log.info("Testing admin login with Keycloak token")

        self._ensure_external_user(self.keycloak_username, roles="admin")

        keycloak_check = self._verify_keycloak_connectivity()
        self.log.info(f"Keycloak reachable with {keycloak_check['jwks_keys_count']} JWKS keys")

        config = self._enable_oidc_config()
        self.log.info("OIDC config being sent to Couchbase (secrets redacted):")
        self.jwt_utils.log_oidc_config_safe(config)
        self._get_parsed_jwt_config()
        self.log.info("OIDC config verified as enabled")

        self.log.info("Getting JWT token from Keycloak")
        token = self._get_token_from_keycloak()
        self.assertIsNotNone(token, "Failed to get token from Keycloak")

        token_claims = self.jwt_utils.get_payload_from_token(token)
        self.log.info(f"JWT payload iss={token_claims.get('iss')} sub={token_claims.get('sub')}")

        self.assertEqual(
            token_claims.get("iss"),
            self.issuer_url,
            f"Token issuer should match configured issuer. "
            f"Expected '{self.issuer_url}', got '{token_claims.get('iss')}'",
        )

        self._assert_admin_authz(token)
        self.log.info("Admin login with Keycloak token successful")


    def test_oidc_redirect_flow(self):
        """
        Test OIDC redirect flow to Keycloak.

        Steps:
        1. Enable OIDC config
        2. Hit /oidc/auth endpoint
        3. Verify 302 redirect to Keycloak login page

        Expected:
        - 302 redirect from /oidc/auth
        - Redirect location contains Keycloak URL
        """
        self.log.info("Testing OIDC redirect flow")

        self._enable_oidc_config()

        result = self.jwt_utils.validate_oidc_redirect_flow(
            cluster_master_ip=self.cluster_master_ip,
            cluster_port=self.cluster.master.port,
            issuer_url=self.issuer_url,
            keycloak_ip=self.keycloak_ip,
            couchbase_use_https=self.cluster_use_https,
            keycloak_use_https=self.keycloak_use_https,
        )

        self.log.info(f"OIDC redirect result: {result}")

        self.assertEqual(
            result["redirect_status"],
            302,
            f"Expected 302 redirect, got {result['redirect_status']}",
        )
        self.assertTrue(
            result["redirects_to_keycloak"],
            f"Should redirect to Keycloak. Location: {result['redirect_location']}",
        )
        self.assertTrue(
            result.get("redirect_scheme_valid"),
            f"Redirect scheme should match keycloak_use_https={self.keycloak_use_https}. "
            f"Location: {result['redirect_location']}",
        )

        self._validate_redirect_location(result["redirect_location"])
        if "request=" in result["redirect_location"]:
            self.log.info("Redirect uses JAR (JWT Secured Authorization Request) format")
        self.log.info("OIDC redirect flow validated with required parameters")


    def test_oidc_external_readonly_user(self):
        """
        Test external RBAC user with read-only role.
        Validates both authentication (identity) and authorization (permissions).

        Steps:
        1. Create external readonly user in Couchbase
        2. Enable OIDC config (JIT disabled - explicit RBAC mapping)
        3. Get token for readonly user from Keycloak
        4. Verify /whoami returns correct identity and external domain
        5. Verify readonly operations succeed (GET /pools/default)
        6. Verify admin write operations are denied (POST /settings/web)

        Expected:
        - Authentication succeeds with correct identity
        - Readonly operations allowed
        - Admin modifications blocked (401/403)
        """
        readonly_username = self.input.param(
            "readonly_username", "combined_user@localhost.com"
        )
        readonly_password = self._user_password(
            "readonly_password", "KEYCLOAK_READONLY_USER_PASSWORD"
        )

        self.log.info(f"Testing external readonly user: {readonly_username}")

        self._ensure_external_user(readonly_username, roles="ro_admin")
        self._enable_oidc_config(jit_provisioning=False)

        token = self._get_token_from_keycloak(
            username=readonly_username, password=readonly_password
        )

        whoami = self.jwt_utils.get_user_info_from_whoami(self.rest, token)
        self.jwt_utils.assert_external_identity(whoami, readonly_username)
        role_names = [r.get("role") for r in whoami.get("roles", []) if isinstance(r, dict)]
        self.log.info(f"Identity verified: id={whoami.get('id')} roles={role_names}")

        # Verify readonly operations succeed
        ok_read, status_read, _ = self.jwt_utils.request_with_jwt(
            self.rest, token, jwt_utils.ENDPOINT_POOLS_DEFAULT, method="GET"
        )
        self.assertTrue(ok_read, "pools request failed")
        self.jwt_utils.assert_success_status(
            status_read, f"Readonly user should access /pools/default. status={status_read}"
        )
        self.log.info("Readonly user can read cluster info")

        # Verify admin write operations are denied
        _, status_write, _ = self.jwt_utils.request_with_jwt(
            self.rest, token, jwt_utils.ENDPOINT_SETTINGS_WEB, method="POST",
            params="username=testuser&password=testpass&port=8091"
        )
        self.assertIn(
            int(status_write) if status_write else 0, [401, 403],
            f"Readonly user should NOT modify admin settings. status={status_write}",
        )
        self.log.info(f"Admin write correctly denied: status={status_write}")

        self.log.info("External readonly user RBAC enforcement validated")


    def test_oidc_no_rbac_mapping(self):
        """
        Test that valid JWT from IdP is denied when no RBAC mapping exists.

        This is a critical security test that validates:
        - JWT signature validation succeeds
        - Issuer/audience validation succeeds
        - Identity extraction succeeds
        - BUT Couchbase authorization fails due to missing RBAC mapping

        Without this, any valid IdP user could become cluster admin.

        Steps:
        1. Enable OIDC config with JIT disabled
        2. Verify JIT provisioning is disabled
        3. Get token for unmapped user (exists in Keycloak, NOT in Couchbase)
        4. Verify token is structurally valid
        5. Verify authorization is denied on protected endpoints

        Expected:
        - Token obtained from Keycloak
        - Authorization denied by Couchbase (401/403)
        """
        unmapped_username = self.input.param(
            "unmapped_username", "unmapped_user@localhost.com"
        )
        unmapped_password = self._user_password(
            "unmapped_password", "KEYCLOAK_UNMAPPED_USER_PASSWORD"
        )

        self.log.info(f"Testing unmapped user: {unmapped_username}")

        self._enable_oidc_and_verify(jit_provisioning=False)
        self.log.info("JIT provisioning verified as disabled")

        token = self._get_token_from_keycloak(
            username=unmapped_username,
            password=unmapped_password
        )

        self.assertIsNotNone(token, "Failed to get token from Keycloak")
        self.log.info("Token obtained from Keycloak for unmapped user")

        try:
            token_claims = self.jwt_utils.get_payload_from_token(token)
            self.log.info(f"Token subject: {token_claims.get('preferred_username')}")
            self.assertEqual(
                token_claims.get("preferred_username"), unmapped_username,
                "Token should contain correct username"
            )
        except Exception as e:
            self.log.warning(f"Could not decode token: {e}")

        _, status_code, content = self.jwt_utils.request_with_jwt(
            self.rest, token, jwt_utils.ENDPOINT_POOLS_DEFAULT, method="GET"
        )

        self.log.info(f"Unmapped user access attempt: status={status_code}")
        self.assertIn(
            int(status_code) if status_code else 0, [401, 403],
            f"Unmapped external user should be denied access. status={status_code} content={content}"
        )

        self.log.info("Unmapped user correctly denied - RBAC enforcement validated")


    def test_oidc_get_settings_masked(self):
        """
        Test that GET /settings/jwt masks sensitive fields and
        PUT with masked value preserves the original secret.

        Steps:
        1. Enable OIDC config with client secret
        2. GET /settings/jwt
        3. Verify client secret is masked with "********"
        4. Verify raw secret is NOT exposed
        5. PUT the masked config back
        6. Verify OIDC login still works (secret preserved)

        Expected:
        - Sensitive fields masked with "********" in GET response
        - Raw secret NOT visible
        - PUT with masked payload preserves original secret
        """
        self.log.info("Testing GET /settings/jwt sensitive field masking")

        self._ensure_external_user(self.keycloak_username, roles="admin")
        self._enable_oidc_config(sleep_seconds=5)

        get_content = self._get_parsed_jwt_config()

        masked_secret = (
            get_content.get("issuers", [{}])[0]
            .get("oidcSettings", {})
            .get("clientSecret")
        )
        self.assertEqual(
            masked_secret, "********",
            f"Expected masked clientSecret '********', got: {masked_secret}"
        )
        self.log.info("Client secret correctly masked with '********'")

        content_str = str(get_content)
        self.assertNotIn(
            self.keycloak_client_secret, content_str,
            "Raw client secret should not appear in GET response",
        )

        # PUT the masked config back — should NOT overwrite the real secret with "********"
        self.log.info("Testing PUT with masked payload preserves original secret")
        status_reput, _ = self.jwt_utils.put_jwt_config(self.rest, get_content)
        self.jwt_utils.assert_success_status(
            status_reput, f"PUT with masked config should succeed. status={status_reput}"
        )
        self.sleep(5, "Waiting for config to take effect")

        self.log.info("Verifying OIDC login still works after PUT with masked payload")
        token = self._get_token_from_keycloak()
        self.assertIsNotNone(token, "Should still get token - secret preserved")

        whoami = self.jwt_utils.get_user_info_from_whoami(self.rest, token)
        self.assertIsNotNone(
            whoami,
            "OIDC login should work - PUT with masked secret preserved original"
        )
        self.log.info(f"Login verified: {whoami.get('id')}")

        self.log.info("Sensitive fields properly masked and PUT preserves secret")


    def test_oidc_invalid_config_rejected(self):
        """
        Test that invalid OIDC configs are rejected with proper 400 validation errors.
        Also verifies invalid config does NOT partially persist.

        Steps:
        1. Get current config state
        2. Try multiple invalid configs
        3. Verify each returns 400 (not 500/503)
        4. Verify config unchanged after failed PUTs

        Expected:
        - 400 validation error for each invalid config
        - No partial persistence of invalid config
        """
        self.log.info("Testing invalid config rejection")

        config_before = self._get_parsed_jwt_config()
        enabled_before = config_before.get("enabled", False)

        invalid_cases = [
            {
                "name": "missing_issuer_name",
                "config": {
                    "enabled": True,
                    "issuers": [{}]
                },
            },
            {
                "name": "invalid_public_key_source",
                "config": {
                    "enabled": True,
                    "issuers": [{
                        "name": self.issuer_url,
                        "publicKeySource": "invalid_source",
                    }]
                },
            },
            {
                "name": "empty_audiences",
                "config": {
                    "enabled": True,
                    "issuers": [{
                        "name": self.issuer_url,
                        "publicKeySource": "jwks_uri",
                        "audiences": [],
                        "oidcSettings": {
                            "clientId": self.keycloak_client_id,
                            "clientSecret": self.keycloak_client_secret,
                        }
                    }]
                },
            },
            {
                "name": "missing_oidc_discovery_and_jwks",
                "config": {
                    "enabled": True,
                    "issuers": [{
                        "name": self.issuer_url,
                        "publicKeySource": "jwks_uri",
                        "oidcSettings": {
                            "clientId": self.keycloak_client_id,
                            "clientSecret": self.keycloak_client_secret,
                        }
                    }]
                },
            },
            {
                "name": "invalid_redirect_uri_type",
                "config": {
                    "enabled": True,
                    "issuers": [{
                        "name": self.issuer_url,
                        "publicKeySource": "jwks_uri",
                        "oidcSettings": {
                            "clientId": self.keycloak_client_id,
                            "clientSecret": self.keycloak_client_secret,
                            "baseRedirectUris": "not-a-list",
                        }
                    }]
                },
            },
        ]

        for case in invalid_cases:
            self.log.info(f"Testing invalid case: {case['name']}")
            status, content = self.jwt_utils.put_jwt_config(self.rest, case["config"])
            self.log.info(f"  {case['name']}: status={status}")
            self.assertEqual(
                int(status) if status else 0, 400,
                f"Invalid config '{case['name']}' should return 400, got {status}. "
                f"Content: {content}"
            )

        self.log.info("Verifying no partial persistence of invalid config")
        config_after = self._get_parsed_jwt_config()
        enabled_after = config_after.get("enabled", False)
        self.assertEqual(
            enabled_before, enabled_after,
            f"Config 'enabled' state should be unchanged. Before: {enabled_before}, After: {enabled_after}"
        )

        self.log.info("Invalid configs properly rejected with 400, no partial persistence")


    def test_oidc_expired_token_rejected(self):
        """
        Verify expired JWT tokens are rejected.
        This is a critical security test to ensure token expiry is enforced.

        Uses a separate Keycloak client (test-client_ttl) with short token lifespan (10s).

        Steps:
        1. Create external user for TTL test
        2. Enable OIDC config with TTL client
        3. Obtain short-lived JWT token from Keycloak
        4. Verify token initially works
        5. Wait until token expires
        6. Verify authentication fails with 401

        Expected:
        - Valid token authenticates successfully (200)
        - Expired token returns 401 Unauthorized

        Keycloak Setup Required:
        - Client: test-client_ttl with Access Token Lifespan = 10 seconds
        - User: adminttl@localhost.com with password
        """
        ttl = self._load_realm_params("ttl")
        self.log.info(
            f"Testing expired token rejection: realm={ttl['realm']}, "
            f"client={ttl['client_id']}, user={ttl['username']}"
        )

        self._ensure_external_user(ttl["username"], roles="admin")
        self._enable_oidc_config(
            sleep_seconds=5,
            algorithm=ttl["algorithm"],
            jit_provisioning=False,
            expiry_leeway_s=0,
            **ttl["kc_overrides"],
        )

        self.log.info("Obtaining short-lived JWT token from Keycloak")
        token = self._get_token_from_keycloak(**ttl["token_kwargs"])
        self.assertIsNotNone(token, "Failed to get token from Keycloak TTL client")

        token_claims = self.jwt_utils.get_payload_from_token(token)
        exp_time = token_claims.get("exp", 0)
        iat_time = token_claims.get("iat", 0)
        current_time = int(time.time())
        token_lifetime = exp_time - iat_time
        time_until_expiry = exp_time - current_time

        self.log.info(f"Token lifetime: {token_lifetime}s")
        self.log.info(f"Time until expiry: {time_until_expiry}s")

        self.assertLessEqual(
            token_lifetime, 60,
            f"Expected short-lived token (<=60s), got {token_lifetime}s. "
            f"Ensure Keycloak client {ttl['client_id']} has Access Token Lifespan = 10 seconds."
        )

        self.log.info("Verifying token works before expiry")
        whoami = self.jwt_utils.get_user_info_from_whoami(self.rest, token)
        self.assertIsNotNone(whoami, "Valid token should authenticate successfully")
        self.log.info(f"Token valid - authenticated as: {whoami.get('id')}")

        wait_time = max(0, time_until_expiry + 5)
        if wait_time:
            self.log.info(f"Waiting {wait_time}s for token to expire...")
            self.sleep(wait_time, "Waiting for token expiry")
        else:
            self.log.info("Token already expired (clock skew or very short TTL), proceeding")

        self.log.info("Verifying expired token is rejected")
        _, status_code, _ = self.jwt_utils.verify_token_whoami_rest(self.rest, token)

        self.log.info(f"Expired token response: status={status_code}")
        self.assertEqual(
            int(status_code) if status_code else 0, 401,
            f"Expired token should return 401 Unauthorized, got {status_code}"
        )

        self.log.info("Expired token correctly rejected with 401 - security validated")


    def test_oidc_jwks_key_rotation(self):
        """
        Test JWKS key rotation - validates Couchbase handles IdP key rotation correctly.

        This test performs ACTUAL key rotation via Keycloak Admin API:
        1. Get token with current active key (kid1)
        2. Verify authentication works
        3. Rotate signing keys in Keycloak (swap priorities)
        4. Get new token (should have different kid2)
        5. Verify JWKS endpoint exposes new key
        6. Verify NEW token authenticates (Couchbase refreshed JWKS)
        7. Verify NEW token has correct issuer and authz works
        8. Verify OLD token still works (old key still in JWKS)
        9. Rotate back to original key (cleanup in finally block)

        Uses cb-rotation realm with TWO RS256 signing keys.

        Keycloak setup required:
        - Realm: cb-rotation
        - Client: rotation-client
        - User: rotation_user@localhost.com
        - Two RS256 signing keys (rsa-generated + rotation-key-2)
        - Admin credentials: admin/password
        """
        self.log.info("Testing JWKS key rotation with actual key swap")

        rotation = self._load_realm_params("rotation")
        kc_overrides = rotation["kc_overrides"]
        token_kwargs = rotation["token_kwargs"]
        expected_issuer = rotation["expected_issuer"]
        rotation_realm = rotation["realm"]

        keycloak_admin_user = self.input.param("keycloak_admin_user", "admin")
        keycloak_admin_pass = self._secret(
            "keycloak_admin_pass", "KEYCLOAK_ADMIN_PASSWORD"
        )

        rotation_performed = False
        admin_token = None

        try:
            self.log.info("Getting Keycloak admin token")
            admin_token = self.jwt_utils.get_keycloak_admin_token(
                keycloak_ip=self.keycloak_ip,
                keycloak_port=self.keycloak_port,
                admin_username=keycloak_admin_user,
                admin_password=keycloak_admin_pass,
                tls_verify=self.keycloak_tls_verify,
                use_https=self.keycloak_use_https,
            )
            self.assertIsNotNone(admin_token, "Failed to get Keycloak admin token")

            self.log.info(f"Fetching JWKS from {rotation_realm} realm")
            jwks_before = self.jwt_utils.get_keycloak_jwks(
                keycloak_ip=self.keycloak_ip,
                keycloak_port=self.keycloak_port,
                keycloak_realm=rotation_realm,
                tls_verify=self.keycloak_tls_verify,
                use_https=self.keycloak_use_https,
            )
            self.assertIsNone(jwks_before.get("error"), f"JWKS fetch failed: {jwks_before.get('error')}")

            signing_keys = [k for k in jwks_before["keys"] if k.get("use") == "sig"]
            signing_kids = [k.get("kid") for k in signing_keys]
            self.log.info(f"Found {len(signing_keys)} signing keys: {signing_kids}")

            self.assertGreaterEqual(
                len(signing_keys), 2,
                f"Need at least 2 signing keys for rotation testing. Found: {len(signing_keys)}"
            )
            self.sleep(2, "Waiting after JWKS fetch")

            self._ensure_external_user(rotation["username"], roles="admin")
            self.sleep(3, "Waiting for external user to propagate")

            self.log.info("Enabling OIDC config for cb-rotation realm")
            config = self._enable_oidc_config(
                algorithm="RS256",
                jit_provisioning=False,
                cluster_master_ip=self.cluster.master.ip,
                **kc_overrides,
            )

            self.log.info("Getting token BEFORE key rotation")
            token_before = self._get_token_from_keycloak(**token_kwargs)
            self.assertIsNotNone(token_before, "Failed to get token before rotation")

            kid_before = self.jwt_utils.get_kid_from_token(token_before)
            self.log.info(f"Token BEFORE rotation: kid={kid_before}")
            self.assertIsNotNone(kid_before, "Token should have kid")

            whoami_before = self.jwt_utils.get_user_info_from_whoami(self.rest, token_before)
            self.assertIsNotNone(whoami_before, "Token should authenticate before rotation")
            self.log.info(f"Auth BEFORE rotation: user={whoami_before.get('id')}")

            self.log.info("*** ROTATING SIGNING KEYS ***")
            rotation_result = self.jwt_utils.rotate_keycloak_signing_key(
                keycloak_ip=self.keycloak_ip,
                keycloak_port=self.keycloak_port,
                keycloak_realm=rotation_realm,
                admin_token=admin_token,
                tls_verify=self.keycloak_tls_verify,
                use_https=self.keycloak_use_https,
            )
            self.assertTrue(
                rotation_result.get("success"),
                f"Key rotation failed: {rotation_result.get('error')}"
            )
            rotation_performed = True
            self.log.info(
                f"Key rotation complete: {rotation_result.get('old_active_key')} -> "
                f"{rotation_result.get('new_active_key')}"
            )

            self.sleep(5, "Waiting for Keycloak to apply key rotation")

            self.log.info("Getting token AFTER key rotation")
            token_after = self._get_token_from_keycloak(**token_kwargs)
            self.assertIsNotNone(token_after, "Failed to get token after rotation")

            kid_after = self.jwt_utils.get_kid_from_token(token_after)
            self.log.info(f"Token AFTER rotation: kid={kid_after}")

            self.assertNotEqual(
                kid_before, kid_after,
                f"Token kid should change after rotation! Before={kid_before}, After={kid_after}"
            )
            self.log.info(f"*** KID CHANGED: {kid_before} -> {kid_after} ***")

            self.log.info("Verifying JWKS endpoint exposes new key")
            jwks_after = self.jwt_utils.get_keycloak_jwks(
                keycloak_ip=self.keycloak_ip,
                keycloak_port=self.keycloak_port,
                keycloak_realm=rotation_realm,
                tls_verify=self.keycloak_tls_verify,
                use_https=self.keycloak_use_https,
            )
            self.assertIsNone(jwks_after.get("error"), "JWKS fetch failed after rotation")

            signing_kids_after = [
                k.get("kid") for k in jwks_after["keys"] if k.get("use") == "sig"
            ]
            self.assertIn(
                kid_after, signing_kids_after,
                f"New kid={kid_after} should be in JWKS after rotation. Found: {signing_kids_after}"
            )
            self.log.info(f"JWKS contains new kid: {kid_after}")

            self.log.info("Validating issuer claim in new token")
            claims_after = self.jwt_utils.get_payload_from_token(token_after)
            actual_issuer = claims_after.get("iss")
            self.assertEqual(
                actual_issuer, expected_issuer,
                f"Token issuer mismatch. Expected={expected_issuer}, Got={actual_issuer}"
            )
            self.log.info(f"Issuer validated: {actual_issuer}")

            self.log.info("Re-applying config to refresh JWKS cache")
            status, _ = self.jwt_utils.put_jwt_config(self.rest, config)
            self.jwt_utils.assert_success_status(
                status, "Failed to re-apply config for JWKS refresh"
            )
            self.sleep(10, "Waiting for JWKS cache to refresh")

            self.log.info("Verifying NEW token authenticates after rotation")
            whoami_after = self.jwt_utils.get_user_info_from_whoami(self.rest, token_after)
            self.assertIsNotNone(
                whoami_after,
                f"NEW token (kid={kid_after}) should authenticate after JWKS refresh"
            )
            self.jwt_utils.assert_external_identity(whoami_after, rotation["username"])
            self.log.info(f"Auth AFTER rotation: user={whoami_after.get('id')}")

            self.log.info("Verifying authorization (RBAC) with new token")
            ok_authz, status_authz, _ = self.jwt_utils.request_with_jwt(
                self.rest, token_after, jwt_utils.ENDPOINT_POOLS_DEFAULT, method="GET"
            )
            self.assertTrue(ok_authz, "authz request failed")
            self.jwt_utils.assert_success_status(
                status_authz, f"Authz should work after rotation. status={status_authz}"
            )
            self.log.info(f"Authz validated: /pools/default returned {status_authz}")

            self.log.info("Verifying OLD token still works (old key in JWKS)")
            whoami_old = self.jwt_utils.get_user_info_from_whoami(self.rest, token_before)
            self.assertIsNotNone(
                whoami_old,
                f"OLD token (kid={kid_before}) should still work (key still in JWKS)"
            )
            self.log.info(f"OLD token still valid: user={whoami_old.get('id')}")

            self.log.info(
                f"*** JWKS KEY ROTATION TEST PASSED ***\n"
                f"  - kid BEFORE: {kid_before}\n"
                f"  - kid AFTER:  {kid_after}\n"
                f"  - issuer: {actual_issuer}\n"
                f"  - NEW token auth: OK\n"
                f"  - NEW token authz: OK\n"
                f"  - OLD token auth: OK (key retained in JWKS)"
            )

        finally:
            if rotation_performed and admin_token:
                self.log.info("Cleanup: Rotating back to original key")
                try:
                    cleanup_result = self.jwt_utils.rotate_keycloak_signing_key(
                        keycloak_ip=self.keycloak_ip,
                        keycloak_port=self.keycloak_port,
                        keycloak_realm=rotation_realm,
                        admin_token=admin_token,
                        tls_verify=self.keycloak_tls_verify,
                        use_https=self.keycloak_use_https,
                    )
                    if cleanup_result.get("success"):
                        self.log.info("Cleanup: Keys rotated back successfully")
                    else:
                        self.log.warning(f"Cleanup: Key rotation failed: {cleanup_result.get('error')}")
                except Exception as cleanup_err:
                    self.log.warning(f"Cleanup failed: {cleanup_err}")


    def test_oidc_cluster_wide_consistency(self):
        """
        Test that OIDC config is consistent across all cluster nodes.

        This is critical for production Couchbase clusters:
        - Config on node A should propagate to nodes B/C
        - JWT authentication should work on any node
        - Config should survive rebalance

        Steps:
        1. Create external user
        2. Enable OIDC config on master node
        3. Get token from Keycloak
        4. Verify config consistency across all nodes (issuer, clientId, etc.)
        5. Verify authentication on all nodes
        6. Verify authorization (RBAC) on all nodes
        7. Perform rebalance swap
        8. Verify JWT still works after rebalance

        Expected:
        - All nodes accept the same JWT token
        - Config is identical on all nodes
        - Auth and authz work after rebalance
        """
        self.log.info("Testing cluster-wide OIDC consistency")

        cluster_nodes = self.cluster.nodes_in_cluster
        if not cluster_nodes or len(cluster_nodes) < 2:
            self.log.warning(f"nodes_in_cluster={[n.ip for n in cluster_nodes] if cluster_nodes else []}")
            self.log.warning(f"servers[:nodes_init]={[n.ip for n in self.cluster.servers[:self.nodes_init]]}")
            self.skipTest("Cluster-wide test requires at least 2 nodes in cluster")

        self.log.info(f"Testing across {len(cluster_nodes)} actual cluster nodes: {[n.ip for n in cluster_nodes]}")

        self._ensure_external_user(self.keycloak_username, roles="admin")
        self._enable_oidc_config(sleep_seconds=0)

        master_config = self._get_parsed_jwt_config()
        master_issuer = master_config.get("issuers", [{}])[0]

        self._wait_jwt_config_on_all_nodes(cluster_nodes, master_config, timeout=120)

        self.log.info("Getting JWT token from Keycloak")
        token = self._get_token_from_keycloak()
        self.assertIsNotNone(token, "Failed to get token from Keycloak")

        for i, node in enumerate(cluster_nodes):
            node_rest = RestConnection(node)
            node_ip = node.ip
            self.log.info(f"Testing node {i+1}/{len(cluster_nodes)}: {node_ip}")

            whoami = self._assert_node_jwt_ok(
                node_rest, token, master_issuer, node_ip, self.keycloak_username
            )
            roles = whoami.get("roles", [])
            role_names = [r.get("role") for r in roles if isinstance(r, dict)]
            self.log.info(
                f"Node {node_ip}: JWT enabled=True, user={whoami.get('id')}, "
                f"domain={whoami.get('domain')}, roles={role_names}, authz=OK"
            )

        self.log.info(f"Pre-rebalance: Cluster-wide consistency validated across {len(cluster_nodes)} nodes")

        if len(cluster_nodes) >= 3:
            self.log.info("Performing rebalance to test config persistence")

            node_to_rebalance = cluster_nodes[-1]
            self.log.info(f"Rebalancing out node: {node_to_rebalance.ip}")

            rebalance_task = self.task.async_rebalance(
                self.cluster,
                to_add=[],
                to_remove=[node_to_rebalance],
                check_vbucket_shuffling=False
            )
            self.task_manager.get_task_result(rebalance_task)
            self.assertTrue(
                rebalance_task.result,
                f"Rebalance out of {node_to_rebalance.ip} failed"
            )
            self.sleep(10, "Waiting after rebalance out")

            self.log.info(f"Rebalancing in node: {node_to_rebalance.ip}")
            rebalance_task = self.task.async_rebalance(
                self.cluster,
                to_add=[node_to_rebalance],
                to_remove=[],
                check_vbucket_shuffling=False
            )
            self.task_manager.get_task_result(rebalance_task)
            self.assertTrue(
                rebalance_task.result,
                f"Rebalance in of {node_to_rebalance.ip} failed"
            )

            self._wait_jwt_config_on_all_nodes(cluster_nodes, master_config, timeout=120)

            self.log.info("Getting fresh token after rebalance")
            token_after = self._get_token_from_keycloak()
            self.assertIsNotNone(token_after, "Should get token after rebalance")

            self.log.info("Validating JWT on ALL nodes after rebalance")
            for i, node in enumerate(cluster_nodes):
                node_rest = RestConnection(node)
                node_ip = node.ip
                self.log.info(f"Post-rebalance testing node {i+1}/{len(cluster_nodes)}: {node_ip}")

                whoami_after = self._assert_node_jwt_ok(
                    node_rest,
                    token_after,
                    master_issuer,
                    node_ip,
                    self.keycloak_username,
                    label="Post-rebalance",
                    config_fields=("name", "audiences"),
                )
                roles_after = whoami_after.get("roles", [])
                role_names_after = [r.get("role") for r in roles_after if isinstance(r, dict)]
                self.log.info(
                    f"Post-rebalance node {node_ip}: config=OK, user={whoami_after.get('id')}, "
                    f"roles={role_names_after}, authz=OK"
                )

            self.log.info("Post-rebalance: All nodes validated successfully")
        else:
            self.log.info("Skipping rebalance test (requires 3+ nodes)")

        self.log.info(f"Cluster-wide consistency validated across {len(cluster_nodes)} nodes")
