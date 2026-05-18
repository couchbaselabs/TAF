import os
import shlex
import types

import requests
import urllib3
from membase.api.rest_client import RestConnection

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from shell_util.remote_connection import RemoteMachineShellConnection

from couchbase_utils.security_utils import jwt_utils
from pytests.onPrem_basetestcase import ClusterSetup


class JWTOIDCBase(ClusterSetup):
    """
    Base class for JWT/OIDC tests against Couchbase Server with Keycloak IdP.

    Provides setUp/tearDown, Keycloak connectivity, secret resolution,
    and shared helper methods. Test classes extend this and implement test_* methods.

    Secrets (Jenkins credentials -> environment variables):
    - KEYCLOAK_IDP_CLIENT_SECRET, KEYCLOAK_TTL_CLIENT_SECRET,
      KEYCLOAK_ROTATION_CLIENT_SECRET, KEYCLOAK_ADMIN_PASSWORD,
      KEYCLOAK_TEST_USER_PASSWORD (and optional per-user overrides)
    """

    # Class-level flag to avoid repeated Keycloak checks across tests
    _keycloak_verified = False

    _REALM_SECRET_ENV = {
        "ttl": "KEYCLOAK_TTL_CLIENT_SECRET",
        "rotation": "KEYCLOAK_ROTATION_CLIENT_SECRET",
    }

    def setUp(self):
        super().setUp()

        self.keycloak_ip = self.input.param(
            "keycloak_ip", os.environ.get("KEYCLOAK_IDP_IP", "172.23.106.226")
        )
        self.keycloak_port = self.input.param("keycloak_port", 8444)
        self.keycloak_realm = self.input.param("keycloak_realm", "cb")
        self.keycloak_client_id = self.input.param("keycloak_client_id", "test-client")
        self.keycloak_client_secret = self._secret(
            "keycloak_client_secret", "KEYCLOAK_IDP_CLIENT_SECRET"
        )
        self.keycloak_username = self.input.param("keycloak_username", "admin@localhost.com")
        self.keycloak_password = self._secret(
            "keycloak_password", "KEYCLOAK_TEST_USER_PASSWORD"
        )
        # Keycloak default signing algorithm is RS384
        self.keycloak_algorithm = self.input.param("keycloak_algorithm", "RS384")
        self.keycloak_use_https = self.input.param("keycloak_use_https", True)
        # Set to False for self-signed certificates on Keycloak
        self.keycloak_tls_verify = self.input.param("keycloak_tls_verify", False)

        # OIDC specific settings
        self.roles_claim = self.input.param(
            "roles_claim", "resource_access.test-client.roles"
        )
        self.sub_claim = self.input.param("sub_claim", "preferred_username")
        # Token has azp claim, not aud - use azp for audience validation
        self.aud_claim = self.input.param("aud_claim", "azp")
        self.jit_provisioning = self.input.param("jit_provisioning", False)
        self.pkce_enabled = self.input.param("pkce_enabled", True)
        self.nonce_validation = self.input.param("nonce_validation", True)

        # Initialize utilities
        self.jwt_utils = jwt_utils.JWTUtils(log=self.log)
        self.rest = RestConnection(self.cluster.master)

        # Reusable HTTP session (avoids TCP/TLS reconnects)
        self.http = requests.Session()
        self.http.verify = self.keycloak_tls_verify

        # Get cluster master IP and port for redirect URIs
        self.cluster_master_ip = getattr(
            self.cluster.master, "ip", None
        ) or getattr(self.cluster.master, "hostname", None)
        self.cluster_port = getattr(self.cluster.master, "port", 8091)
        # Detect if cluster uses HTTPS (port 18091 = HTTPS)
        self.cluster_use_https = self.input.param(
            "cluster_use_https", self.cluster_port == 18091
        )

        # Build issuer URL
        scheme = "https" if self.keycloak_use_https else "http"
        self.issuer_url = (
            f"{scheme}://{self.keycloak_ip}:{self.keycloak_port}"
            f"/realms/{self.keycloak_realm}"
        )

        # Ensure Keycloak is running (only once per test class)
        cls = self.__class__
        if not cls._keycloak_verified:
            self._ensure_keycloak_running()
            cls._keycloak_verified = True

    def tearDown(self):
        try:
            self.jwt_utils.disable_jwt(self.rest)
        except Exception as e:
            self.log.warning(f"Failed to disable JWT in tearDown: {e}")
        finally:
            super().tearDown()


    def _secret(self, param_name, env_name, *, default=None, required=True):
        """
        Resolve a secret from TAF param or environment variable (Jenkins injects env).

        Prefer Jenkins Credentials bound to env_name; local runs may use export or -p.
        """
        value = self.input.param(param_name, os.environ.get(env_name) or default)
        if required and not value:
            self.fail(
                f"Missing secret '{param_name}': set environment variable {env_name} "
                f"(Jenkins credential) or pass -p {param_name}=<value> for local runs"
            )
        return value

    def _user_password(self, param_name, env_name):
        """User password from param, per-user env, or KEYCLOAK_TEST_USER_PASSWORD."""
        param_val = self.input.param(param_name, None)
        if param_val:
            return param_val
        env_val = os.environ.get(env_name) or os.environ.get("KEYCLOAK_TEST_USER_PASSWORD")
        if not env_val:
            self.fail(
                f"Missing password: set {env_name} or KEYCLOAK_TEST_USER_PASSWORD "
                f"(Jenkins credential), or -p {param_name}=<value>"
            )
        return env_val

    def _load_realm_params(self, prefix):
        """
        Load realm-specific Keycloak params (ttl, rotation, etc.).

        Returns dict with kc_overrides, token_kwargs, and expected_issuer.
        """
        defaults = {
            "ttl": {
                "realm": "cb-shortttl",
                "client_id": "test-client_ttl",
                "username": "adminttl@localhost.com",
                "algorithm": "RS256",
            },
            "rotation": {
                "realm": "cb-rotation",
                "client_id": "rotation-client",
                "username": "rotation_user@localhost.com",
                "algorithm": "RS256",
            },
        }.get(prefix, {})

        realm = self.input.param(f"{prefix}_realm", defaults.get("realm"))
        client_id = self.input.param(f"{prefix}_client_id", defaults.get("client_id"))
        secret_env = self._REALM_SECRET_ENV[prefix]
        client_secret = self._secret(f"{prefix}_client_secret", secret_env)
        username = self.input.param(f"{prefix}_username", defaults.get("username"))
        password = self._user_password(
            f"{prefix}_password",
            f"KEYCLOAK_{prefix.upper()}_USER_PASSWORD",
        )
        algorithm = self.input.param(
            f"{prefix}_algorithm", defaults.get("algorithm", self.keycloak_algorithm)
        )

        kc_overrides = {
            "keycloak_realm": realm,
            "client_id": client_id,
            "client_secret": client_secret,
        }
        token_kwargs = {
            "username": username,
            "password": password,
            **kc_overrides,
        }
        scheme = "https" if self.keycloak_use_https else "http"
        expected_issuer = (
            f"{scheme}://{self.keycloak_ip}:{self.keycloak_port}/realms/{realm}"
        )
        return {
            "realm": realm,
            "client_id": client_id,
            "client_secret": client_secret,
            "username": username,
            "password": password,
            "algorithm": algorithm,
            "kc_overrides": kc_overrides,
            "token_kwargs": token_kwargs,
            "expected_issuer": expected_issuer,
        }


    def _ensure_keycloak_running(self):
        """
        Check if Keycloak is reachable, start it via SSH if not.
        Keycloak container is left running (no cleanup needed).
        Uses class-level flag to avoid repeated checks across tests.
        Uses flock to prevent race conditions in parallel CI jobs.
        """
        scheme = "https" if self.keycloak_use_https else "http"
        check_url = (
            f"{scheme}://{self.keycloak_ip}:{self.keycloak_port}"
            f"/realms/{self.keycloak_realm}/.well-known/openid-configuration"
        )

        try:
            resp = self.http.get(check_url, timeout=5)
            if resp.status_code == 200:
                self.log.info(f"Keycloak already running at {self.keycloak_ip}:{self.keycloak_port}")
                return
        except Exception:
            pass  # Not reachable, try to start

        self.log.warning(
            f"Keycloak not reachable at {self.keycloak_ip}:{self.keycloak_port}, "
            "attempting to start..."
        )

        keycloak_ssh_user = self.input.param("keycloak_ssh_user", "root")
        keycloak_ssh_pass = self._secret("keycloak_ssh_pass", "KEYCLOAK_SSH_PASSWORD")
        keycloak_compose_dir = self.input.param("keycloak_compose_dir", "/data/keycloak")
        keycloak_container = self.input.param("keycloak_container", "keycloak-persistent")

        shell = None
        try:
            keycloak_server = types.SimpleNamespace(
                ip=self.keycloak_ip,
                port=22,
                ssh_username=keycloak_ssh_user,
                ssh_password=keycloak_ssh_pass,
                ssh_key="",
                memcached_port=11210,
            )
            shell = RemoteMachineShellConnection(keycloak_server)

            # Use flock to prevent race conditions in parallel CI jobs.
            # Fast path: try docker start first, fallback to compose.
            container = shlex.quote(keycloak_container)
            compose_dir = shlex.quote(keycloak_compose_dir)
            cmd = (
                "flock /tmp/keycloak.lock -c "
                f"'docker start {container} 2>/dev/null "
                f"|| (cd {compose_dir} && docker compose up -d)'"
            )
            output, error = shell.execute_command(cmd)
            self.log.info(f"Keycloak start command output: {output}")

            if error:
                error_str = "".join(error) if isinstance(error, list) else str(error)
                error_lower = error_str.lower()
                if any(x in error_lower for x in ["no such file", "command not found", "permission denied"]):
                    self.fail(f"Keycloak start failed: {error_str}")

            self.log.info("Waiting for Keycloak to start...")
            for attempt in range(12):
                self.sleep(5, f"Waiting for Keycloak (attempt {attempt + 1}/12)")
                try:
                    resp = self.http.get(check_url, timeout=5)
                    if resp.status_code == 200:
                        self.log.info("Keycloak started successfully")
                        return
                except Exception:
                    continue

            # Startup failed — dump compose logs for debugging
            self.log.error("Keycloak startup timeout - fetching container logs...")
            logs, _ = shell.execute_command(
                f"cd {keycloak_compose_dir} && docker compose logs --tail=200"
            )
            log_str = "".join(logs) if isinstance(logs, list) else str(logs)
            self.log.error(f"Keycloak logs:\n{log_str}")
            self.fail(
                f"Keycloak failed to start after 60s at "
                f"{self.keycloak_ip}:{self.keycloak_port}"
            )
        except Exception as e:
            self.fail(f"Failed to start Keycloak via SSH: {e}")
        finally:
            if shell:
                shell.disconnect()


    def _get_oidc_jwt_config(self, **overrides):
        """Build OIDC JWT config using test instance defaults."""
        return self.jwt_utils.get_oidc_jwt_config(
            keycloak_ip=overrides.get("keycloak_ip", self.keycloak_ip),
            keycloak_port=overrides.get("keycloak_port", self.keycloak_port),
            keycloak_realm=overrides.get("keycloak_realm", self.keycloak_realm),
            client_id=overrides.get("client_id", self.keycloak_client_id),
            client_secret=overrides.get("client_secret", self.keycloak_client_secret),
            cluster_master_ip=overrides.get("cluster_master_ip", self.cluster_master_ip),
            cluster_port=overrides.get("cluster_port", self.cluster_port),
            cluster_use_https=overrides.get("cluster_use_https", self.cluster_use_https),
            algorithm=overrides.get("algorithm", self.keycloak_algorithm),
            sub_claim=overrides.get("sub_claim", self.sub_claim),
            aud_claim=overrides.get("aud_claim", self.aud_claim),
            roles_claim=overrides.get("roles_claim", self.roles_claim),
            jit_provisioning=overrides.get("jit_provisioning", self.jit_provisioning),
            tls_verify_peer=overrides.get("tls_verify_peer", self.keycloak_tls_verify),
            pkce_enabled=overrides.get("pkce_enabled", self.pkce_enabled),
            nonce_validation=overrides.get("nonce_validation", self.nonce_validation),
            use_https=overrides.get("use_https", self.keycloak_use_https),
            expiry_leeway_s=overrides.get("expiry_leeway_s"),
        )

    def _enable_oidc_config(self, sleep_seconds=10, **overrides):
        """PUT OIDC JWT config and wait for it to take effect."""
        config = self._get_oidc_jwt_config(**overrides)
        status, content = self.jwt_utils.put_jwt_config(self.rest, config)
        self.jwt_utils.assert_success_status(
            status, f"Failed to enable OIDC config: {content}"
        )
        if sleep_seconds:
            self.sleep(sleep_seconds, "Waiting for config to take effect")
        return config

    def _get_parsed_jwt_config(self, rest=None):
        """GET /settings/jwt and return parsed config dict."""
        rest = rest or self.rest
        ok, _, content = self.jwt_utils.get_jwt_config_settings(
            rest, expected_status_code=200
        )
        self.assertTrue(ok, f"Failed to GET JWT config: {content}")
        return self.jwt_utils.parse_jwt_config_content(content)

    def _enable_oidc_and_verify(self, *, jit_provisioning=None, sleep_seconds=10, **overrides):
        """Enable OIDC and verify enabled (and optional JIT) in GET config."""
        if jit_provisioning is not None:
            overrides = {**overrides, "jit_provisioning": jit_provisioning}
        self._enable_oidc_config(sleep_seconds=sleep_seconds, **overrides)
        get_content = self._get_parsed_jwt_config()
        self.assertTrue(get_content.get("enabled"), "JWT should be enabled")
        if jit_provisioning is not None:
            issuers = get_content.get("issuers", [])
            self.assertTrue(len(issuers) > 0, "Should have at least one issuer configured")
            self.assertEqual(
                issuers[0].get("jitProvisioning", True),
                jit_provisioning,
                f"JIT provisioning should be {jit_provisioning}",
            )
        return get_content


    def _get_token_from_keycloak(self, username=None, password=None, **overrides):
        """Get JWT token from Keycloak using test instance defaults."""
        return self.jwt_utils.get_token_from_keycloak(
            keycloak_ip=overrides.get("keycloak_ip", self.keycloak_ip),
            keycloak_port=overrides.get("keycloak_port", self.keycloak_port),
            keycloak_realm=overrides.get("keycloak_realm", self.keycloak_realm),
            client_id=overrides.get("client_id", self.keycloak_client_id),
            client_secret=overrides.get("client_secret", self.keycloak_client_secret),
            username=username or self.keycloak_username,
            password=password or self.keycloak_password,
            tls_verify=overrides.get("tls_verify", self.keycloak_tls_verify),
            use_https=overrides.get("use_https", self.keycloak_use_https),
        )

    def _verify_keycloak_connectivity(self):
        """Verify Keycloak is reachable, fail test if not."""
        result = self.jwt_utils.verify_keycloak_connectivity(
            keycloak_ip=self.keycloak_ip,
            keycloak_port=self.keycloak_port,
            keycloak_realm=self.keycloak_realm,
            tls_verify=self.keycloak_tls_verify,
            use_https=self.keycloak_use_https,
        )
        if not result["reachable"]:
            self.fail(f"Keycloak not reachable: {result['error']}")
        return result


    def _ensure_external_user(self, username, roles):
        """Create external RBAC user if not already present."""
        try:
            self.jwt_utils.create_external_user(
                self.rest, user_name=username, roles=roles
            )
        except Exception as e:
            self.log.warning(f"Could not create external user {username}: {e}")


    def _wait_jwt_config_on_all_nodes(self, cluster_nodes, master_config, timeout=120):
        """Poll until JWT config has propagated to all cluster nodes."""
        node_rests = [RestConnection(node) for node in cluster_nodes]
        self.jwt_utils.wait_jwt_config_propagation(
            node_rests,
            master_config,
            timeout=timeout,
            log_callback=self.log.info,
        )


    def _assert_admin_authz(self, token, *, check_write=True):
        """Verify admin external identity, role, read authz, and optional write probe."""
        who_ok, who_status, who_content = self.jwt_utils.verify_token_whoami_rest(
            self.rest, token
        )
        self.log.info(f"whoami response: ok={who_ok} status={who_status}")
        self.jwt_utils.assert_success_status(
            who_status,
            f"Token should authenticate via /whoami. status={who_status} content={who_content}",
        )
        self.assertTrue(who_ok, "whoami request failed")

        whoami = self.jwt_utils.get_user_info_from_whoami(self.rest, token)
        self.jwt_utils.assert_external_identity(whoami, self.keycloak_username)
        role_names = self.jwt_utils.assert_role_present(whoami, "admin")
        self.log.info(f"Identity verified: id={whoami.get('id')} roles={role_names}")

        ok_pools, status_pools, _ = self.jwt_utils.request_with_jwt(
            self.rest, token, jwt_utils.ENDPOINT_POOLS_DEFAULT, method="GET"
        )
        self.assertTrue(ok_pools, "pools request failed")
        self.jwt_utils.assert_success_status(
            status_pools, f"Admin should access /pools/default. status={status_pools}"
        )

        if check_write:
            _, status_write, _ = self.jwt_utils.request_with_jwt(
                self.rest, token, jwt_utils.ENDPOINT_SETTINGS_STATS,
                method="POST", params="sendStats=false"
            )
            self.log.info(f"Admin write test: status={status_write}")
            self.assertNotIn(
                int(status_write) if status_write else 0,
                [401, 403],
                f"Admin should pass authz for write operation. status={status_write}",
            )

        return whoami

    def _assert_node_jwt_ok(
        self,
        node_rest,
        token,
        master_issuer,
        node_ip,
        expected_username,
        *,
        label="",
        config_fields=("name", "audiences", "subClaim", "publicKeySource"),
    ):
        """Verify JWT config, authentication, and authorization on a cluster node."""
        prefix = f"{label}: " if label else ""
        node_config = self._get_parsed_jwt_config(node_rest)
        self.assertTrue(
            node_config.get("enabled"),
            f"{prefix}JWT should be enabled on node {node_ip}",
        )
        node_issuer = node_config.get("issuers", [{}])[0]
        for field in config_fields:
            self.assertEqual(
                node_issuer.get(field),
                master_issuer.get(field),
                f"{prefix}{field} mismatch on node {node_ip}",
            )

        ok_auth, status_auth, _ = self.jwt_utils.verify_token_whoami_rest(node_rest, token)
        self.jwt_utils.assert_success_status(
            status_auth,
            f"{prefix}Token should authenticate on node {node_ip}. status={status_auth}",
        )
        self.assertTrue(ok_auth, f"{prefix}whoami request failed on node {node_ip}")

        whoami = self.jwt_utils.get_user_info_from_whoami(node_rest, token)
        self.jwt_utils.assert_external_identity(
            whoami, expected_username, prefix=f"{prefix}node {node_ip}".strip()
        )

        ok_pools, status_pools, _ = self.jwt_utils.request_with_jwt(
            node_rest, token, jwt_utils.ENDPOINT_POOLS_DEFAULT, method="GET"
        )
        self.assertTrue(
            ok_pools and self.jwt_utils._is_success_status(status_pools),
            f"{prefix}Admin should access /pools/default on node {node_ip}. status={status_pools}",
        )
        return whoami

    def _validate_redirect_location(self, redirect_location):
        """Validate redirect Location scheme, host, and OIDC parameters."""
        self.jwt_utils.validate_oidc_redirect_location(
            redirect_location,
            client_id=self.keycloak_client_id,
            keycloak_ip=self.keycloak_ip,
            keycloak_use_https=self.keycloak_use_https,
        )
