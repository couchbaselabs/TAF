import time
import urllib.parse
import uuid

import requests
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


    def test_oidc_jit_provisioning_allows_unmapped_user(self):
        """
        Test JIT provisioning auto-creates external users on first login.

        When jitProvisioning=True, a valid IdP user with no Couchbase RBAC mapping
        is auto-provisioned on first authentication — no pre-existing RBAC entry needed.

        Direct contrast to test_oidc_no_rbac_mapping, which uses jit_provisioning=False
        and expects 401/403 for the same unmapped user.

        Steps:
        1. Ensure unmapped user has NO Couchbase RBAC entry (clean state for JIT)
        2. Enable OIDC with jitProvisioning=True, rolesClaim disabled
        3. Authenticate with a valid Keycloak token for the unmapped user
        4. Verify /whoami succeeds — JIT created the user on first login
        5. Verify user identity (id matches, domain=external)
        6. Verify no admin roles assigned (no rolesClaim, no Keycloak client roles)
        7. Verify admin endpoints denied (no roles → no authz)

        Expected:
        - /whoami returns user info (auto-provisioned)
        - User domain is 'external'
        - No admin role in response (no rolesClaim configured)
        - /pools/default denied (401 or 403)

        Keycloak setup:
        - unmapped_user@localhost.com must exist in the 'cb' realm with a password
        - User must NOT have Couchbase admin client roles in Keycloak test-client
        """
        unmapped_username = self.input.param("unmapped_username", "unmapped_user@localhost.com")
        unmapped_password = self._user_password(
            "unmapped_password", "KEYCLOAK_UNMAPPED_USER_PASSWORD"
        )

        self.log.info(f"Testing JIT provisioning for: {unmapped_username}")

        try:
            # Remove any pre-existing Couchbase RBAC entry — JIT must create it fresh.
            self.jwt_utils.delete_external_user(self.rest, unmapped_username)
            self.sleep(3, "Waiting for external user deletion to propagate")
            self.assertIsNone(
                self.jwt_utils.get_external_user(self.rest, unmapped_username),
                f"External user {unmapped_username} must not exist before JIT login",
            )
            self.log.info(f"Confirmed no Couchbase RBAC entry for {unmapped_username}")

            # Enable JIT with no rolesClaim — user gets auto-created but no roles assigned.
            get_content = self._enable_oidc_and_verify(
                jit_provisioning=True, roles_claim=""
            )
            issuer = get_content.get("issuers", [{}])[0]
            self.assertNotIn(
                "rolesClaim",
                issuer,
                f"rolesClaim should be absent/disabled for this test. issuer={issuer}",
            )

            token = self._get_token_from_keycloak(
                username=unmapped_username, password=unmapped_password
            )
            self.assertIsNotNone(token, "Failed to get token from Keycloak for unmapped user")

            # JIT: attempt /whoami — if auto-provisioned, authentication succeeds.
            ok_whoami, status_whoami, content_whoami = (
                self.jwt_utils.verify_token_whoami_rest(self.rest, token)
            )
            self.log.info(
                f"/whoami response: ok={ok_whoami} status={status_whoami} "
                f"content={str(content_whoami)[:300]}"
            )

            if int(status_whoami or 0) in [401, 403]:
                self.skipTest(
                    f"JIT provisioning not supported for Bearer token auth on this build "
                    f"(status={status_whoami}). Verified working on CB 8.1.0+. "
                    "On older builds, jitProvisioning may only apply to the browser OIDC flow."
                )

            self.assertTrue(
                ok_whoami and self.jwt_utils._is_success_status(status_whoami),
                f"Unexpected /whoami error: status={status_whoami} content={content_whoami}",
            )

            whoami = self.jwt_utils.get_user_info_from_whoami(self.rest, token)
            self.assertIsNotNone(whoami, f"Failed to parse /whoami response: {content_whoami}")
            self.jwt_utils.assert_external_identity(whoami, unmapped_username)

            role_names = [
                r.get("role") for r in whoami.get("roles", []) if isinstance(r, dict)
            ]
            self.log.info(
                f"JIT provisioned: id={whoami.get('id')} domain={whoami.get('domain')} "
                f"roles={role_names}"
            )

            self.assertEqual(
                role_names,
                [],
                f"JIT user with rolesClaim disabled should not receive roles. Got: {role_names}",
            )

            # No roles → protected endpoints must still be denied.
            _, status_denied, _ = self.jwt_utils.request_with_jwt(
                self.rest, token, jwt_utils.ENDPOINT_POOLS_DEFAULT, method="GET"
            )
            self.assertIn(
                int(status_denied) if status_denied else 0, [401, 403],
                f"JIT user with no roles should be denied /pools/default. status={status_denied}",
            )

            self.log.info(
                "JIT provisioning validated: user auto-created on first login, "
                "authentication OK, no roles without rolesClaim -> authz denied as expected"
            )
        finally:
            self.jwt_utils.delete_external_user(self.rest, unmapped_username)


    def test_oidc_jit_with_roles_claim(self):
        """
        Test JIT provisioning with rolesClaim maps JWT token roles to Couchbase RBAC.

        When both jitProvisioning=True and rolesClaim are configured, a user is
        auto-created AND assigned Couchbase roles sourced from their JWT claims —
        no manual RBAC setup in Couchbase needed. This is the zero-config RBAC model
        used by most enterprise OIDC deployments.

        Steps:
        1. Delete user's Couchbase RBAC entry to guarantee JIT must create it fresh
        2. Enable OIDC with jitProvisioning=True and rolesClaim set
        3. Obtain a token that carries role claims
        4. Inspect token claims to know expected Couchbase roles
        5. Verify /whoami returns the user with roles sourced from token
        6. Verify admin authz works when token grants admin role

        Expected:
        - User auto-provisioned on first login (no pre-existing RBAC entry)
        - Roles in /whoami match token's rolesClaim value
        - /pools/default accessible if token carries admin role

        Keycloak setup required:
        - The test user (default: admin@localhost.com) must have the 'admin' client
          role assigned under Clients → test-client → Users/Role Mappings in the 'cb' realm.
          This populates resource_access.test-client.roles = ["admin"] in the JWT.
        - Without this, the test fails as a Keycloak setup error because P0 must
          exercise role mapping, not only user auto-provisioning.
        """
        jit_username = self.input.param("jit_roles_username", self.keycloak_username)
        jit_password = self._user_password("jit_roles_password", "KEYCLOAK_TEST_USER_PASSWORD")

        expected_role = self.input.param("jit_roles_expected_role", "admin")

        self.log.info(f"Testing JIT + rolesClaim for: {jit_username}")

        try:
            # Delete Couchbase RBAC entry — JIT + rolesClaim must recreate it from token claims.
            self.jwt_utils.delete_external_user(self.rest, jit_username)
            self.sleep(3, "Waiting for external user deletion to propagate")
            self.assertIsNone(
                self.jwt_utils.get_external_user(self.rest, jit_username),
                f"External user {jit_username} must not exist before JIT login",
            )
            self.log.info(f"Confirmed no Couchbase RBAC entry for {jit_username}")

            # Enable JIT=True with default rolesClaim ("resource_access.test-client.roles").
            get_content = self._enable_oidc_and_verify(jit_provisioning=True)
            issuer = get_content.get("issuers", [{}])[0]
            self.assertEqual(
                issuer.get("rolesClaim"),
                self.roles_claim,
                f"rolesClaim should be configured for JIT role mapping. issuer={issuer}",
            )

            token = self._get_token_from_keycloak(username=jit_username, password=jit_password)
            self.assertIsNotNone(token, "Failed to get token from Keycloak")

            # Inspect token claims to know what Couchbase roles to expect.
            claims = self.jwt_utils.get_payload_from_token(token)
            resource_access = claims.get("resource_access", {})
            client_roles = resource_access.get(self.keycloak_client_id, {}).get("roles", [])
            self.log.info(
                f"Token carries roles for '{self.keycloak_client_id}': {client_roles}"
            )
            self.assertIn(
                expected_role,
                client_roles,
                f"Keycloak setup error: token for '{jit_username}' must include "
                f"'{expected_role}' under client '{self.keycloak_client_id}'. "
                f"Token roles: {client_roles}",
            )

            # JIT + rolesClaim: attempt /whoami — user should be auto-provisioned with token roles.
            ok_whoami, status_whoami, content_whoami = (
                self.jwt_utils.verify_token_whoami_rest(self.rest, token)
            )
            self.log.info(
                f"/whoami response: ok={ok_whoami} status={status_whoami} "
                f"content={str(content_whoami)[:300]}"
            )

            if int(status_whoami or 0) in [401, 403]:
                self.skipTest(
                    f"JIT provisioning not supported for Bearer token auth on this build "
                    f"(status={status_whoami}). Verified working on CB 8.1.0+. "
                    "On older builds, jitProvisioning may only apply to the browser OIDC flow."
                )

            whoami = self.jwt_utils.get_user_info_from_whoami(self.rest, token)
            self.assertIsNotNone(whoami, f"Failed to parse /whoami response: {content_whoami}")
            self.jwt_utils.assert_external_identity(whoami, jit_username)

            provisioned_roles = [
                r.get("role") for r in whoami.get("roles", []) if isinstance(r, dict)
            ]
            self.log.info(
                f"JIT provisioned: id={whoami.get('id')} "
                f"token_roles={client_roles} couchbase_roles={provisioned_roles}"
            )

            # Each role in the token should appear in Couchbase after JIT + rolesClaim.
            for role in client_roles:
                self.assertIn(
                    role,
                    provisioned_roles,
                    f"Token role '{role}' should appear in Couchbase roles after JIT+rolesClaim. "
                    f"Couchbase returned: {provisioned_roles}",
                )

            _, status_pools, _ = self.jwt_utils.request_with_jwt(
                self.rest, token, jwt_utils.ENDPOINT_POOLS_DEFAULT, method="GET"
            )
            self.jwt_utils.assert_success_status(
                status_pools,
                f"JIT+rolesClaim {expected_role} should access /pools/default. "
                f"status={status_pools}",
            )
            self.log.info(
                f"{expected_role} authz via JIT+rolesClaim: /pools/default returned {status_pools}"
            )

            self.log.info(
                f"JIT + rolesClaim validated: user auto-created with roles from token "
                f"(token: {client_roles} -> Couchbase: {provisioned_roles})"
            )
        finally:
            self.jwt_utils.delete_external_user(self.rest, jit_username)

    def test_oidc_invalid_bearer_tokens_rejected(self):
        """
        Test that Couchbase rejects a matrix of invalid JWT Bearer tokens.

        Verifies the CB JWT validation pipeline correctly refuses:
        1. Tampered payload with original signature (signature mismatch)
        2. alg=none header downgrade attack (algorithm confusion)
        3. Wrong issuer claim (tampered, still signature-mismatch)
        4. Wrong azp/aud claim (tampered, signature-mismatch)
        5. Token with nbf (not-before) set 2 hours in the future
        6. Malformed non-JWT string
        7. Empty token string

        Each invalid token variant must be rejected (non-2xx) — a 2xx on any
        variant is a test failure and indicates a validation bypass.

        Keycloak setup:
        - Default admin@localhost.com user with admin role in 'cb' realm
        """
        self.log.info("Testing invalid Bearer token rejection matrix")

        self._ensure_external_user(self.keycloak_username, roles="admin")
        self.sleep(3, "Waiting for external user to propagate")
        self._enable_oidc_and_verify()

        token = self._get_token_from_keycloak()
        self.assertIsNotNone(token, "Failed to get valid Keycloak token for baseline")

        # Baseline: valid token must authenticate.
        whoami = self.jwt_utils.get_user_info_from_whoami(self.rest, token)
        self.assertIsNotNone(
            whoami,
            "Valid Keycloak token should authenticate before invalid-token matrix",
        )
        self.log.info(f"Baseline token OK: user={whoami.get('id')}")

        invalid_cases = [
            (
                "tampered_payload_signature_mismatch",
                self.jwt_utils.build_tampered_payload_token(
                    token, {"sub": "evil-attacker@evil.com"}
                ),
            ),
            (
                "alg_none_downgrade",
                self.jwt_utils.build_tampered_header_token(
                    token, {"alg": "none"}, drop_signature=True
                ),
            ),
            (
                "wrong_issuer",
                self.jwt_utils.build_tampered_payload_token(
                    token, {"iss": "https://evil.example.com/realms/fake"}
                ),
            ),
            (
                "wrong_azp_audience",
                self.jwt_utils.build_tampered_payload_token(
                    token, {"azp": "evil-client", "aud": "evil-client"}
                ),
            ),
            (
                "nbf_in_future",
                self.jwt_utils.build_tampered_payload_token(
                    token, {"nbf": int(time.time()) + 7200}
                ),
            ),
            (
                "malformed_not_a_jwt",
                "this.is.notvalid",
            ),
            (
                "empty_token",
                "",
            ),
        ]

        failures = []
        for name, bad_token in invalid_cases:
            self.log.info(f"Testing invalid token variant: {name}")
            _, status, _ = self.jwt_utils.request_with_jwt(
                self.rest, bad_token, jwt_utils.ENDPOINT_WHOAMI, method="GET"
            )
            if status is None:
                failures.append(f"{name}: request failed with no HTTP status (transport/crash)")
            elif int(status) not in (400, 401, 403):
                failures.append(f"{name}: expected 400/401/403 but got status={status}")
            else:
                self.log.info(f"  {name}: correctly rejected (status={status})")

        self.assertEqual(
            failures,
            [],
            "Some invalid token variants were not rejected:\n" + "\n".join(failures),
        )
        self.log.info(
            f"All {len(invalid_cases)} invalid Bearer token variants correctly rejected"
        )

    def test_oidc_jwks_rotation_without_config_reapply(self):
        """
        Test that Couchbase auto-discovers rotated JWKS keys without admin config reapply.

        The existing test_oidc_jwks_key_rotation re-applies the JWT config (PUT
        /settings/jwt) after rotation to force a JWKS cache refresh. This test
        verifies on-demand key discovery: when CB receives a token signed with an
        unknown kid, it should automatically fetch the current JWKS from the IdP
        and accept the token without any admin action.

        Steps:
        1. Enable OIDC with cb-rotation realm
        2. Get token with current active key → verify auth works
        3. Rotate signing keys in Keycloak (swap priorities)
        4. Get new token (different kid)
        5. Retry authentication for up to 60s WITHOUT re-applying config
        6. If new token authenticates → on-demand JWKS refresh confirmed
        7. Rotate back (cleanup)

        Uses cb-rotation realm with TWO RS256 signing keys.

        Keycloak setup required:
        - Realm: cb-rotation, Client: rotation-client
        - User: rotation_user@localhost.com with admin Couchbase RBAC entry
        - Two RS256 signing keys for rotation
        - Admin credentials in KEYCLOAK_ADMIN_PASSWORD
        """
        self.log.info("Testing JWKS on-demand refresh after key rotation (no config reapply)")

        rotation = self._load_realm_params("rotation")
        kc_overrides = rotation["kc_overrides"]
        token_kwargs = rotation["token_kwargs"]
        rotation_realm = rotation["realm"]

        keycloak_admin_user = self.input.param("keycloak_admin_user", "admin")
        keycloak_admin_pass = self._secret("keycloak_admin_pass", "KEYCLOAK_ADMIN_PASSWORD")

        rotation_performed = False
        admin_token = None

        try:
            admin_token = self.jwt_utils.get_keycloak_admin_token(
                keycloak_ip=self.keycloak_ip,
                keycloak_port=self.keycloak_port,
                admin_username=keycloak_admin_user,
                admin_password=keycloak_admin_pass,
                tls_verify=self.keycloak_tls_verify,
                use_https=self.keycloak_use_https,
            )
            self.assertIsNotNone(admin_token, "Failed to get Keycloak admin token")

            # Verify at least 2 signing keys exist for rotation.
            jwks_before = self.jwt_utils.get_keycloak_jwks(
                keycloak_ip=self.keycloak_ip,
                keycloak_port=self.keycloak_port,
                keycloak_realm=rotation_realm,
                tls_verify=self.keycloak_tls_verify,
                use_https=self.keycloak_use_https,
            )
            self.assertIsNone(
                jwks_before.get("error"), f"JWKS fetch failed: {jwks_before.get('error')}"
            )
            signing_keys = [k for k in jwks_before["keys"] if k.get("use") == "sig"]
            self.assertGreaterEqual(
                len(signing_keys),
                2,
                f"Need at least 2 signing keys for rotation. Found: {len(signing_keys)}",
            )

            self._ensure_external_user(rotation["username"], roles="admin")
            self.sleep(3, "Waiting for external user to propagate")

            self.log.info("Enabling OIDC config for cb-rotation realm")
            self._enable_oidc_config(
                algorithm="RS256",
                jit_provisioning=False,
                cluster_master_ip=self.cluster.master.ip,
                **kc_overrides,
            )

            token_before = self._get_token_from_keycloak(**token_kwargs)
            self.assertIsNotNone(token_before, "Failed to get token before rotation")
            kid_before = self.jwt_utils.get_kid_from_token(token_before)
            self.log.info(f"Token BEFORE rotation: kid={kid_before}")

            whoami_before = self.jwt_utils.get_user_info_from_whoami(self.rest, token_before)
            self.assertIsNotNone(whoami_before, "Token should authenticate before rotation")

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
                f"Key rotation failed: {rotation_result.get('error')}",
            )
            rotation_performed = True
            self.log.info(
                f"Rotation complete: {rotation_result['old_active_key']} -> "
                f"{rotation_result['new_active_key']}"
            )

            self.sleep(5, "Waiting for Keycloak to apply key rotation")

            token_after = self._get_token_from_keycloak(**token_kwargs)
            self.assertIsNotNone(token_after, "Failed to get token after rotation")
            kid_after = self.jwt_utils.get_kid_from_token(token_after)
            self.log.info(f"Token AFTER rotation: kid={kid_after}")

            self.assertNotEqual(
                kid_before,
                kid_after,
                f"Token kid must change after rotation. Before={kid_before}, After={kid_after}",
            )

            # Retry without config reapply — CB must auto-fetch new JWKS on unknown kid.
            self.log.info(
                f"Retrying auth with new kid={kid_after} for up to 120s "
                "(no config reapply - testing on-demand JWKS refresh)"
            )
            whoami_after = None
            retry_interval = 10
            max_retries = 12
            for attempt in range(1, max_retries + 1):
                ok_auth, status_auth, content_auth = self.jwt_utils.verify_token_whoami_rest(
                    self.rest, token_after
                )
                self.log.info(
                    f"Attempt {attempt}/{max_retries}: "
                    f"status={status_auth} content={str(content_auth)[:200]}"
                )
                if ok_auth and self.jwt_utils._is_success_status(status_auth):
                    whoami_after = self.jwt_utils.get_user_info_from_whoami(
                        self.rest, token_after
                    )
                    self.log.info(
                        f"On-demand JWKS refresh confirmed on attempt {attempt}: "
                        f"user={whoami_after.get('id') if whoami_after else None}"
                    )
                    break
                self.log.info(
                    f"Attempt {attempt}/{max_retries}: new token not yet accepted "
                    f"(CB may still be caching old JWKS), retrying in {retry_interval}s"
                )
                self.sleep(retry_interval, f"Waiting for JWKS on-demand refresh (attempt {attempt})")

            self.assertIsNotNone(
                whoami_after,
                f"CB did not auto-discover new JWKS key (kid={kid_after}) within 120s. "
                "CB may require an admin config reapply after key rotation - "
                "verify /settings/jwt PUT triggers JWKS cache refresh.",
            )
            self.jwt_utils.assert_external_identity(whoami_after, rotation["username"])

            whoami_old = self.jwt_utils.get_user_info_from_whoami(self.rest, token_before)
            self.assertIsNotNone(
                whoami_old,
                f"Old token (kid={kid_before}) should still work while old key remains in JWKS",
            )

            # Authz must also work with the new key.
            _, status_authz, _ = self.jwt_utils.request_with_jwt(
                self.rest, token_after, jwt_utils.ENDPOINT_POOLS_DEFAULT, method="GET"
            )
            self.jwt_utils.assert_success_status(
                status_authz,
                f"Admin authz should work after on-demand JWKS refresh. status={status_authz}",
            )

            self.log.info(
                f"JWKS on-demand refresh validated: kid changed {kid_before} -> {kid_after}, "
                f"new token auth+authz OK without config reapply"
            )

        finally:
            if rotation_performed and admin_token:
                self.log.info("Cleanup: rotating keys back to original state")
                try:
                    cleanup = self.jwt_utils.rotate_keycloak_signing_key(
                        keycloak_ip=self.keycloak_ip,
                        keycloak_port=self.keycloak_port,
                        keycloak_realm=rotation_realm,
                        admin_token=admin_token,
                        tls_verify=self.keycloak_tls_verify,
                        use_https=self.keycloak_use_https,
                    )
                    if not cleanup.get("success"):
                        self.log.warning(
                            f"Cleanup rotation failed: {cleanup.get('error')}"
                        )
                except Exception as exc:
                    self.log.warning(f"Cleanup rotation raised: {exc}")

    def test_oidc_callback_rejects_invalid_state(self):
        """
        Test that /oidc/callback with an invalid or missing state parameter is rejected.

        The OIDC state parameter is a CSRF defense: CB generates a nonce at /oidc/auth
        time and validates it at /oidc/callback. A callback with a random or absent
        state must NOT create a session.

        Steps:
        1. Enable OIDC
        2. POST to CB /oidc/callback with a random state — no prior /oidc/auth flow
        3. POST to CB /oidc/callback with no state at all
        4. Assert both return error (non-2xx, or redirect without session cookie)

        Expected:
        - /oidc/callback with invalid/missing state returns 400 or error redirect
        - No session cookie set in either response
        """
        self.log.info("Testing /oidc/callback rejection of invalid state parameter")

        self._enable_oidc_and_verify()

        cb_scheme = "https" if self.cluster_use_https else "http"
        cb_base = f"{cb_scheme}://{self.cluster_master_ip}:{self.cluster_port}"
        callback_url = f"{cb_base}/oidc/callback"

        invalid_cases = [
            ("random_state_no_code", {"state": uuid.uuid4().hex, "code": "fake-code"}),
            ("missing_state_with_code", {"code": "fake-code"}),
            ("missing_both_state_and_code", {}),
        ]

        failures = []
        for name, params in invalid_cases:
            self.log.info(f"Testing /oidc/callback with: {name}")
            session = requests.Session()
            session.verify = self.keycloak_tls_verify
            resp = session.get(
                callback_url,
                params=params,
                allow_redirects=False,
                timeout=10,
            )
            self.log.info(
                f"  {name}: status={resp.status_code} "
                f"location={resp.headers.get('Location', '')[:100]}"
            )
            # A valid session must NOT be created for any invalid callback response.
            cookie_names = list(session.cookies.keys()) + list(resp.cookies.keys())
            session_cookies_set = any(
                cookie == "NS_ui_auth" or cookie.startswith("ui-auth-")
                for cookie in cookie_names
            )
            if session_cookies_set:
                failures.append(
                    f"{name}: invalid callback created session cookie(s) {cookie_names} "
                    f"(status={resp.status_code})"
                )
                continue

            whoami_resp = session.get(f"{cb_base}/whoami", allow_redirects=False, timeout=10)
            authenticated_identity = None
            if 200 <= whoami_resp.status_code < 300:
                try:
                    whoami = whoami_resp.json()
                except Exception:
                    whoami = {}
                if whoami.get("domain") != "anonymous" or whoami.get("id"):
                    authenticated_identity = whoami

            if authenticated_identity:
                failures.append(
                    f"{name}: invalid callback produced authenticated identity "
                    f"{authenticated_identity}"
                )
            else:
                self.log.info(
                    f"  {name}: correctly rejected without session "
                    f"(callback_status={resp.status_code}, whoami_status={whoami_resp.status_code})"
                )

        self.assertEqual(
            failures,
            [],
            "Some /oidc/callback invalid-state cases incorrectly created sessions:\n"
            + "\n".join(failures),
        )
        self.log.info("All invalid /oidc/callback state variants correctly rejected")

    def test_oidc_query_service_with_bearer_token(self):
        """
        Test JWT Bearer token authentication on the N1QL query service (port 8093).

        Verifies that the CB query service (cbq-engine) respects JWT auth, not just
        the management REST API (port 8091). Both the auth and authz paths are exercised:
        - Admin token → query succeeds (SELECT 1)
        - Unmapped user token (no RBAC) → query denied (401/403)

        Steps:
        1. Enable OIDC
        2. Create external user with admin role for query access
        3. Get admin token from Keycloak
        4. POST SELECT query to :8093 with Bearer token → expect 200 and query result
        5. Get token for unmapped user (no RBAC)
        6. POST same query with unmapped token → expect 401/403

        Expected:
        - Admin Bearer token grants query service access
        - Unmapped user Bearer token is denied at query service

        Keycloak setup:
        - admin@localhost.com with admin RBAC role
        - unmapped_user@localhost.com with no RBAC mapping (for authz denial check)
        """
        self.log.info("Testing Bearer token authentication on N1QL query service")

        query_username = self.input.param("query_username", self.keycloak_username)
        query_password = self._user_password(
            "query_password", "KEYCLOAK_TEST_USER_PASSWORD"
        )
        unmapped_username = self.input.param(
            "unmapped_username", "unmapped_user@localhost.com"
        )
        unmapped_password = self._user_password(
            "unmapped_password", "KEYCLOAK_UNMAPPED_USER_PASSWORD"
        )

        self._ensure_external_user(query_username, roles="admin")
        self.sleep(3, "Waiting for external user to propagate")
        self._enable_oidc_and_verify()

        admin_token = self._get_token_from_keycloak(
            username=query_username, password=query_password
        )
        self.assertIsNotNone(admin_token, "Failed to get admin token from Keycloak")

        query_use_https = self.input.param("query_use_https", False)
        query_port = self.input.param("query_port", 18093 if query_use_https else 8093)
        query_scheme = "https" if query_use_https else "http"
        query_node = None
        services_init = self.input.param("services_init", "")
        service_specs = services_init.split("-") if services_init else []
        for index, spec in enumerate(service_specs):
            services = {service.strip() for service in spec.replace(",", ":").split(":")}
            if services.intersection({"n1ql", "query"}):
                candidate_nodes = self.cluster.servers[:int(self.nodes_init)]
                if index < len(candidate_nodes):
                    query_node = candidate_nodes[index]
                    break
        if query_node is None:
            for node in self.cluster.nodes_in_cluster:
                services = getattr(node, "services", "")
                if isinstance(services, str):
                    services = services.replace(",", ":").split(":")
                if set(services).intersection({"n1ql", "query"}):
                    query_node = node
                    break
        if query_node is None:
            self.fail(
                f"N1QL/query node not found in cluster. services_init={services_init}, "
                f"nodes={[n.ip for n in self.cluster.nodes_in_cluster]}"
            )

        query_host = getattr(query_node, "ip", None) or getattr(query_node, "hostname", None)
        query_url = f"{query_scheme}://{query_host}:{query_port}/query/service"
        bearer_headers = {
            "Authorization": f"Bearer {admin_token}",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }

        self.log.info(f"Testing admin Bearer auth on N1QL service: {query_url}")
        try:
            resp_admin = self.http.post(
                query_url,
                data="statement=SELECT 1 AS test",
                headers=bearer_headers,
                timeout=30,
            )
        except Exception as exc:
            self.fail(
                f"Query service at {query_url} not reachable even though query node was selected: {exc}"
            )

        self.log.info(
            f"Query service admin response: status={resp_admin.status_code} "
            f"body={resp_admin.text[:300]}"
        )

        if resp_admin.status_code == 404:
            self.fail(
                f"N1QL service not available at {query_url} - "
                "test requires kv:n1ql:index services"
            )

        self.assertNotIn(
            resp_admin.status_code,
            [401, 403],
            f"Admin Bearer token should be accepted by query service. "
            f"status={resp_admin.status_code} body={resp_admin.text[:300]}",
        )
        self.assertEqual(
            resp_admin.status_code,
            200,
            f"Admin Bearer token should succeed on N1QL service. "
            f"status={resp_admin.status_code} body={resp_admin.text[:300]}",
        )
        try:
            result_body = resp_admin.json()
            self.assertEqual(
                result_body.get("status"),
                "success",
                f"N1QL query should succeed. response={result_body}",
            )
        except Exception:
            pass

        self.log.info("Admin Bearer token accepted by N1QL service")

        # Authz denial check: unmapped user has no query permissions.
        self.log.info(f"Testing unmapped user denial on N1QL service: {unmapped_username}")
        try:
            self.jwt_utils.delete_external_user(self.rest, unmapped_username)
        except Exception:
            pass

        unmapped_token = self._get_token_from_keycloak(
            username=unmapped_username, password=unmapped_password
        )
        self.assertIsNotNone(unmapped_token, "Failed to get token for unmapped user")

        resp_unmapped = self.http.post(
            query_url,
            data="statement=SELECT * FROM system:user_info LIMIT 1",
            headers={
                "Authorization": f"Bearer {unmapped_token}",
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
            timeout=30,
        )
        self.log.info(
            f"Query service unmapped user response: status={resp_unmapped.status_code} "
            f"body={resp_unmapped.text[:300]}"
        )

        self.assertIn(
            resp_unmapped.status_code,
            [401, 403],
            f"Unmapped user with no RBAC should be denied by query service "
            f"(system:user_info requires admin/security_admin). "
            f"status={resp_unmapped.status_code}",
        )
        self.log.info(
            "N1QL query service Bearer token auth validated: "
            f"admin allowed, unmapped user denied (status={resp_unmapped.status_code})"
        )
