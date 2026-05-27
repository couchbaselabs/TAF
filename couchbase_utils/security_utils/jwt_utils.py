import ast
import base64
import json
import re
import secrets
import time
import urllib.parse
import uuid

import jwt
import requests
from cb_server_rest_util.security.jwt import JWTAPI
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from cryptography.hazmat.primitives.serialization import load_pem_public_key
from shell_util.remote_connection import RemoteMachineShellConnection

# ── Couchbase REST endpoint constants ───────────────────────────────────────
# Single source of truth for all JWT/OIDC-related Couchbase paths.
# Use with jwt_utils.request_with_jwt() (strips leading slash automatically) or
# strip manually when building URLs directly: endpoint.lstrip("/")
ENDPOINT_WHOAMI = "/whoami"
ENDPOINT_POOLS_DEFAULT = "/pools/default"
ENDPOINT_POOLS_BUCKETS = "/pools/default/buckets"
ENDPOINT_SETTINGS_JWT = "/settings/jwt"
ENDPOINT_OIDC_AUTH = "/oidc/auth"
ENDPOINT_SETTINGS_STATS = "/settings/stats"
ENDPOINT_SETTINGS_WEB = "/settings/web"

# ── Keycloak API path templates ───────────────────────────────────────────────
# Call .format(realm=...) or .format(realm=..., component_id=...) before use.
# Build full URL as: f"{scheme}://{ip}:{port}{KC_PATH_TOKEN.format(realm=realm)}"
KC_PATH_REALM = "/realms/{realm}"
KC_PATH_TOKEN = "/realms/{realm}/protocol/openid-connect/token"
KC_PATH_JWKS = "/realms/{realm}/protocol/openid-connect/certs"
KC_PATH_DISCOVERY = "/realms/{realm}/.well-known/openid-configuration"
KC_PATH_ADMIN_TOKEN = "/realms/master/protocol/openid-connect/token"
KC_PATH_ADMIN_KEYS = "/admin/realms/{realm}/keys"
KC_PATH_ADMIN_COMPONENTS = "/admin/realms/{realm}/components"
KC_PATH_ADMIN_COMPONENT = "/admin/realms/{realm}/components/{component_id}"

class JWTUtils:
    """Utility class for JWT token generation and configuration"""

    EC_CURVE_MAP = {
        "ES256": ec.SECP256R1(),
        "ES384": ec.SECP384R1(),
        "ES512": ec.SECP521R1(),
        "ES256K": ec.SECP256K1(),
    }

    def __init__(self, log=None):
        """Initialize JWTUtils
        Args:
            log: Logger instance for logging messages (optional)
        """
        self.log = log

    def generate_key_pair(self, algorithm: str, key_size: int = 2048):
        """Generate a key pair for JWT signing.
        Args:
            algorithm: JWT signing algorithm (RS256, ES256, etc.)
            key_size: Key size in bits for RSA algorithms (minimum 2048)
        Returns:
            tuple: (private_key_pem, public_key_pem) as strings
        """
        algorithm = algorithm.upper()
        if self.log:
            self.log.info(f"Generating key pair for algorithm: {algorithm}")

        if algorithm.startswith("RS") or algorithm.startswith("PS"):
            if key_size < 2048:
                raise ValueError(f"RSA key size must be 2048 or greater. Got {key_size}")
            private_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=key_size,
            )
        elif algorithm in self.EC_CURVE_MAP:
            curve = self.EC_CURVE_MAP[algorithm]
            private_key = ec.generate_private_key(curve)
        else:
            raise ValueError(f"Unsupported algorithm: {algorithm}. Supported: RS*, PS*, ES*")

        if not private_key:
            raise RuntimeError("Error while creating key pair")

        private_key_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        public_key = private_key.public_key()
        public_key_pem = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        if self.log:
            self.log.info("Key pair generated successfully.")
        return private_key_pem.decode(), public_key_pem.decode()

    def generate_hmac_secret(self, byte_length: int = 32) -> str:
        """Generate a cryptographically random HMAC shared secret as a hex string."""
        return secrets.token_hex(byte_length)

    def get_jwt_config(
        self,
        issuer_name,
        algorithm,
        pub_key,
        token_audience,
        token_group_matching_rule,
        jit_provisioning=True,
        public_key_source="pem",
        jwks_uri=None,
        jwks_json=None,
    ):
        """Get JWT configuration with configurable JIT provisioning
        Args:
            issuer_name: Name of the JWT issuer
            algorithm: JWT signing algorithm
            pub_key: Public key in PEM format
            token_audience: List of audiences for the token
            token_group_matching_rule: List of group matching rules
            jit_provisioning (bool): Enable/disable JIT user provisioning. Default: True
        Returns:
            dict: JWT configuration dictionary
        """
        issuer = self._build_jwt_issuer_entry(
            name=issuer_name,
            algorithm=algorithm,
            pub_key=pub_key,
            audiences=token_audience,
            group_maps=token_group_matching_rule,
            jit_provisioning=jit_provisioning,
            public_key_source=public_key_source,
            jwks_uri=jwks_uri,
            jwks_json=jwks_json,
        )
        return self.get_jwt_config_from_issuers([issuer])


    def get_multi_issuer_jwt_config(self, issuers_list):
        """
        Get JWT configuration with multiple issuers.

        Args:
            issuers_list: List of dicts, each containing:
                - name (str): Issuer name
                - algorithm (str): JWT signing algorithm
                - pub_key (str): Public key in PEM format
                - audiences (list): List of audiences
                - group_maps (list): List of group matching rules
                - jit_provisioning (bool, optional): Enable JIT provisioning. Default: True

        Returns:
            dict: JWT configuration dictionary with multiple issuers
        """
        issuers = []
        for issuer in issuers_list or []:
            issuers.append(
                self._build_jwt_issuer_entry(
                    name=issuer["name"],
                    algorithm=issuer["algorithm"],
                    pub_key=issuer["pub_key"],
                    audiences=issuer["audiences"],
                    group_maps=issuer["group_maps"],
                    jit_provisioning=issuer.get("jit_provisioning", True),
                )
            )
        return self.get_jwt_config_from_issuers(issuers)

    def setup_multi_issuer_env(
        self,
        issuers,
        *,
        default_audience=None,
        default_groups=None,
        group_name_prefix="jwt_multi",
    ):
        """
        Build a multi-issuer JWT test environment (keys + group maps + config).

        This helper does NOT create RBAC groups on the cluster. Callers should
        create the returned `group_name` values themselves.

        Args:
            issuers: list[dict] where each dict may contain:
                - name (str, required)
                - algorithm (str, required)
                - key_size (int, optional; RSA only, default 2048)
                - audiences (list, optional; default `default_audience` or ["cb-cluster"])
                - groups (list, optional; default `default_groups` or ["admin"])
                - group_name (str, optional; autogenerated if missing)
                - jit_provisioning (bool, optional; default True)
            default_audience: default audiences list for issuers missing `audiences`
            default_groups: default groups list for issuers missing `groups`
            group_name_prefix: prefix for autogenerated group names

        Returns:
            dict: {
                "config": <jwt config>,
                "issuers": [ {name, algorithm, private_key, pub_key, audience, groups, group_name, group_maps}, ... ],
                ... plus convenience keys issuer_a/issuer_b when exactly 2 issuers
            }
        """
        if default_audience is None:
            default_audience = ["cb-cluster"]
        if default_groups is None:
            default_groups = ["admin"]

        issuer_envs = []
        config_issuers = []

        for idx, spec in enumerate(issuers or []):
            name = spec["name"]
            algorithm = spec["algorithm"]
            key_size = spec.get("key_size", 2048)
            audiences = spec.get("audiences") or list(default_audience)
            groups = spec.get("groups") or list(default_groups)
            group_name = spec.get("group_name") or f"{group_name_prefix}_{idx}_{uuid.uuid4().hex[:8]}"
            jit_prov = spec.get("jit_provisioning", True)

            if algorithm.upper().startswith("RS") or algorithm.upper().startswith("PS"):
                priv, pub = self.generate_key_pair(algorithm=algorithm, key_size=key_size)
            else:
                priv, pub = self.generate_key_pair(algorithm=algorithm)

            group_maps = spec.get("group_maps")
            if not group_maps:
                group_maps = [f"^{g}$ {group_name}" for g in groups]

            config_issuers.append(
                {
                    "name": name,
                    "algorithm": algorithm,
                    "pub_key": pub,
                    "audiences": audiences,
                    "group_maps": group_maps,
                    "jit_provisioning": jit_prov,
                }
            )

            issuer_envs.append(
                {
                    "name": name,
                    "algorithm": algorithm,
                    "private_key": priv,
                    "pub_key": pub,
                    "audience": audiences,
                    "groups": groups,
                    "group_name": group_name,
                    "group_maps": group_maps,
                }
            )

        config = self.get_multi_issuer_jwt_config(config_issuers)

        env = {"config": config, "issuers": issuer_envs}
        if len(issuer_envs) == 2:
            env["issuer_a"] = issuer_envs[0]
            env["issuer_b"] = issuer_envs[1]

        if self.log:
            names = ", ".join([f"{i['name']} ({i['algorithm']})" for i in issuer_envs])
            self.log.info(f"Multi-issuer env created: {names}")

        return env

    def create_token(
        self,
        issuer_name,
        user_name,
        algorithm,
        private_key,
        token_audience=None,
        user_groups=None,
        ttl=300,
        nbf_seconds=0,
        normalize_audience=False,
        headers=None,
    ):
        """Create a JWT token with the specified parameters
        Args:
            issuer_name: Name of the JWT issuer
            user_name: Username for the token subject
            algorithm: JWT signing algorithm
            private_key: Private key for signing the token
            token_audience: List of audiences for the token (optional)
            user_groups: List of user groups (optional)
            ttl: Time to live in seconds (default: 300)
            nbf_seconds: Not before offset in seconds (default: 0)
            normalize_audience: If True, convert single-item list audience to string
        Returns:
            str: Encoded JWT token
        """
        curr_time = int(time.time())
        if normalize_audience:
            token_audience = self.get_single_audience(token_audience)
        payload = {
            "iss": issuer_name,
            "sub": user_name,
            "exp": curr_time + ttl,
            "iat": curr_time,
            "nbf": curr_time + nbf_seconds,
            "jti": str(uuid.uuid4()),
        }
        if token_audience:
            payload['aud'] = token_audience
        if user_groups:
            payload['groups'] = user_groups

        if self.log:
            self.log.info(f"Creating JWT token with payload: {json.dumps(payload, indent=2)}")

        jwt_token = jwt.encode(
            payload=payload,
            algorithm=algorithm,
            key=private_key,
            headers=headers,
        )
        return jwt_token


    def request_with_jwt(self, rest_connection, token, endpoint, method="GET", params=""):
        """
        Make an HTTP request with JWT bearer token authorization.

        Tries JWTAPI (CBRestConnection path) first; falls back to the legacy
        membase RestConnection._http_request path if JWTAPI cannot be initialized.

        Args:
            rest_connection: RestConnection instance for making requests
            token: JWT bearer token
            endpoint: REST endpoint path (e.g., "/pools/default/buckets")
            method: HTTP method (default: "GET")
            params: Request parameters (default: "")

        Returns:
            tuple: (ok_bool, status_code, content)
        """
        try:
            jwt_api = JWTAPI()
            shim = self._cbrest_server_shim(rest_connection)
            jwt_api.set_server_values(shim)
            jwt_api.set_endpoint_urls(shim)
            status, content, response = jwt_api.request_with_bearer(
                token, endpoint, method, params
            )
            if self.log:
                self.log.debug(
                    f"request_with_jwt [JWTAPI] {method} {endpoint}: "
                    f"status={status} http={self.status_code(response)} "
                    f"base_url={jwt_api.base_url}"
                )
            return status, self.status_code(response), content
        except Exception as _jwt_ex:
            if method.upper() not in ("GET", "HEAD", "OPTIONS"):
                raise
            if self.log:
                self.log.warning(
                    f"request_with_jwt [JWTAPI] failed for {endpoint}: {_jwt_ex!r} — using legacy fallback"
                )

        # Legacy fallback: use membase RestConnection directly
        if endpoint.startswith("/"):
            endpoint = endpoint[1:]
        api = f"{rest_connection.baseUrl}{endpoint}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "*/*",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        if self.log:
            self.log.debug(f"request_with_jwt [legacy] {method} {api}")
        ok, content, resp = rest_connection._http_request(api, method, params, headers=headers)
        return ok, self.status_code(resp), content

    def verify_token_rest(self, rest_connection, token, endpoint=None):
        """
        Verify JWT token works for a REST request.

        Args:
            rest_connection: RestConnection instance for making requests
            token: JWT bearer token to verify
            endpoint: REST endpoint path to validate (default: ENDPOINT_POOLS_BUCKETS)

        Returns:
            tuple: (ok_bool, status_code, content)
        """
        if endpoint is None:
            endpoint = ENDPOINT_POOLS_BUCKETS
        return self.request_with_jwt(rest_connection, token, endpoint, method="GET")

    def verify_token_whoami_rest(self, rest_connection, token):
        """
        Verify JWT token against the /whoami endpoint.

        /whoami is typically accessible to all authenticated users and can be used
        to validate authentication independent of RBAC roles.

        Args:
            rest_connection: RestConnection instance for making requests
            token: JWT bearer token to verify

        Returns:
            tuple: (ok_bool, status_code, content)
        """
        return self.verify_token_rest(rest_connection, token, endpoint=ENDPOINT_WHOAMI)


    def validate_jwt_expectations(
        self,
        *,
        rest_connection,
        token,
        expected_jwt_authz=None,
        expected_jwt_auth=None,
        authz_endpoint=None,
        verify_cli=False,
        verify_cli_callback=None,
        log_callback=None,
    ):
        """
        Centralized JWT validation helper.

        Args:
            rest_connection: RestConnection instance
            token: JWT token
            expected_jwt_authz: If set, validate authorization endpoint (pools/buckets).
                               True expects 2xx; False expects non-2xx.
            expected_jwt_auth: If set, validate authentication-only endpoint (/whoami).
                               True expects 2xx; False expects non-2xx.
            verify_cli: Whether to run CLI validation (authz only)
            verify_cli_callback: callable(token) -> (cli_ok, cli_status, cli_out)
            log_callback: Optional callable(str) for logging

        Returns:
            dict: results for debugging/logging
        """
        results = {}

        def _format_results_snapshot():
            def _short(val, limit=500):
                if val is None:
                    return None
                text = str(val)
                return text if len(text) <= limit else text[:limit] + "...<truncated>"

            authz_rest = results.get("authz_rest", {})
            authz_cli = results.get("authz_cli", {})
            whoami = results.get("auth_whoami_rest", {})
            return (
                "Debug snapshot: "
                f"authz_rest(status={authz_rest.get('status')}, ok={authz_rest.get('ok')}, content={_short(authz_rest.get('content'))}), "
                f"authz_cli(status={authz_cli.get('status')}, ok={authz_cli.get('ok')}, out={_short(authz_cli.get('out'))}), "
                f"whoami_rest(status={whoami.get('status')}, ok={whoami.get('ok')}, content={_short(whoami.get('content'))})"
            )

        if expected_jwt_authz is not None:
            if authz_endpoint is None:
                authz_endpoint = ENDPOINT_POOLS_BUCKETS
            rest_ok, rest_status, rest_content = self.verify_token_rest(
                rest_connection, token, endpoint=authz_endpoint
            )
            results["authz_rest"] = {
                "ok": rest_ok,
                "status": rest_status,
                "content": rest_content,
            }
            if log_callback:
                log_callback(f"JWT authz REST ok={rest_ok} status={rest_status} endpoint={authz_endpoint}")

            cli_ok, cli_status, cli_out = True, None, ""
            if verify_cli:
                if authz_endpoint != ENDPOINT_POOLS_BUCKETS:
                    raise ValueError(
                        "CLI validation currently supports only ENDPOINT_POOLS_BUCKETS. "
                        f"Got authz_endpoint={authz_endpoint}"
                    )
                if not verify_cli_callback:
                    raise ValueError("verify_cli_callback is required when verify_cli=True")
                cli_ok, cli_status, cli_out = verify_cli_callback(token)
                results["authz_cli"] = {"ok": cli_ok, "status": cli_status, "out": cli_out}
                if log_callback:
                    log_callback(f"JWT authz CLI ok={cli_ok} status={cli_status}")

            rest_success = self._is_success_status(rest_status)
            if expected_jwt_authz:
                if not rest_success:
                    raise AssertionError(
                        f"JWT authz REST expected success but failed. status={rest_status} content={rest_content}. "
                        f"{_format_results_snapshot()}"
                    )
                if verify_cli and not cli_ok:
                    raise AssertionError(
                        f"JWT authz CLI expected success but failed. status={cli_status} out={cli_out}. "
                        f"{_format_results_snapshot()}"
                    )
            else:
                rest_failed = not rest_success
                cli_failed = (not cli_ok) if verify_cli else True
                if not (rest_failed and cli_failed):
                    raise AssertionError(
                        "JWT authz expected failure but at least one path succeeded. "
                        f"rest_status={rest_status} cli_ok={cli_ok}. {_format_results_snapshot()}"
                    )

        if expected_jwt_auth is not None:
            who_ok, who_status, who_content = self.verify_token_whoami_rest(rest_connection, token)
            results["auth_whoami_rest"] = {
                "ok": who_ok,
                "status": who_status,
                "content": who_content,
            }
            if log_callback:
                log_callback(f"JWT whoami REST ok={who_ok} status={who_status}")

            who_success = self._is_success_status(who_status)
            if expected_jwt_auth:
                if not who_success:
                    raise AssertionError(
                        f"JWT whoami expected success but failed. status={who_status} content={who_content}. "
                        f"{_format_results_snapshot()}"
                    )
            else:
                if who_success:
                    raise AssertionError(
                        f"JWT whoami expected failure but succeeded. status={who_status} content={who_content}. "
                        f"{_format_results_snapshot()}"
                    )

        return results

    def verify_token_cli(
        self,
        server,
        token,
        binary_path,
        use_https,
        run_command_callback,
        endpoint=None,
        log_callback=None,
    ):
        """
        Verify JWT token via CLI: prefer couchbase-cli with bearer token if supported,
        else fallback to curl against a REST endpoint.

        Args:
            server: Server object with .port and .ip (e.g. cluster.master)
            token: JWT bearer token string
            binary_path: Path to couchbase-cli binary
            use_https: Whether cluster uses HTTPS
            run_command_callback: callable(cmd: str) -> (stdout_lines, stderr_lines)
            endpoint: REST path for curl fallback (default: ENDPOINT_POOLS_BUCKETS)
            log_callback: Optional callback for logging (receives message str)

        Returns:
            tuple: (ok_bool, status_code_or_none, output_text)
        """
        if endpoint is None:
            endpoint = ENDPOINT_POOLS_BUCKETS

        cluster_arg = f"https://localhost:{server.port}" if use_https else f"localhost:{server.port}"
        no_ssl_verify = " --no-ssl-verify" if use_https else ""

        help_out, help_err = run_command_callback(f"{binary_path} --help")
        help_text = "\n".join(help_out + help_err).lower()
        bearer_flag = None
        for flag in ["--bearer-token", "--jwt", "--auth-token", "--access-token"]:
            if flag in help_text:
                bearer_flag = flag
                break

        if bearer_flag:
            cmd = (
                f"{binary_path} server-list "
                f"-c {cluster_arg} {bearer_flag} '{token}'{no_ssl_verify}"
            )
            out, err = run_command_callback(cmd)
            text = "\n".join(out + err).strip()
            ok = "error" not in text.lower() and "unauthorized" not in text.lower()
            return ok, None, text

        scheme = "https" if use_https else "http"
        insecure = "-k " if use_https else ""
        url = f"{scheme}://127.0.0.1:{server.port}{endpoint}"
        cmd = (
            f"curl -s {insecure}-o /tmp/jwt_cli_out.txt -w '%{{http_code}}' "
            f"-H 'Authorization: Bearer {token}' '{url}'"
        )

        if log_callback:
            log_callback(f"JWT CLI fallback using curl: {url}")

        http_code_out, err = run_command_callback(cmd)
        http_code = None
        if http_code_out and http_code_out[0]:
            http_code = http_code_out[0].strip().strip("'").strip('"')
        body_out, _ = run_command_callback("cat /tmp/jwt_cli_out.txt || true")
        body = "\n".join(body_out).strip()
        ok = http_code is not None and str(http_code).isdigit() and (200 <= int(http_code) < 300)
        return ok, http_code, body

    def put_jwt_config(self, rest_connection, config):
        """
        PUT JWT configuration to the cluster.

        Args:
            rest_connection: RestConnection instance for making requests
            config: JWT configuration dictionary

        Returns:
            tuple: (status_code, content)
        """
        return self.put_jwt_config_as(rest_connection, config)

    @staticmethod
    def _cbrest_server_shim(rest_connection, username=None, password=None):
        """
        Build a minimal server-like object for CBRestConnection.set_server_values().
        """
        class _Shim:
            pass

        shim = _Shim()
        shim.ip = getattr(rest_connection, "ip", None)
        shim.port = getattr(rest_connection, "port", None)
        shim.rest_username = username or getattr(rest_connection, "username", None) or getattr(rest_connection, "rest_username", None)
        shim.rest_password = password or getattr(rest_connection, "password", None) or getattr(rest_connection, "rest_password", None)
        shim.type = getattr(rest_connection, "type", "default")
        shim.services = getattr(rest_connection, "services", None)
        shim.hostname = getattr(rest_connection, "hostname", None)
        return shim

    def put_jwt_config_as(self, rest_connection, config, username=None, password=None):
        """
        PUT JWT config using JWTAPI (CBRestConnection style) when available.
        Falls back to membase RestConnection `_http_request` otherwise.

        Returns:
            tuple: (status_code, content)
        """
        try:
            jwt_api = JWTAPI()
            shim = self._cbrest_server_shim(rest_connection, username=username, password=password)
            jwt_api.set_server_values(shim)
            jwt_api.set_endpoint_urls(shim)
            status, content, response = jwt_api.put_jwt_config(config)
            return self.status_code(response), content
        except Exception:
            pass

        # Fall back to direct REST connection using only the current endpoint
        headers = rest_connection._create_capi_headers(
            username=username, password=password, contentType="application/json"
        )
        body = json.dumps(config)

        path = "settings/jwt"
        api = f"{rest_connection.baseUrl}{path}"
        ok, content, resp = rest_connection._http_request(api, "PUT", body, headers=headers)
        status = self.status_code(resp)

        if not ok:
            raise Exception(
                f"Failed to PUT /{path}. status={status} content={content}"
            )

        return status, content

    def get_jwt_config_settings(self, rest_connection, expected_status_code=200, log_callback=None):
        """
        GET JWT configuration from cluster and return status/content.

        Args:
            rest_connection: RestConnection instance for making requests
            expected_status_code: Expected HTTP status code (default: 200). If None, no check.
            log_callback: Optional callback function for logging (receives message)

        Returns:
            tuple: (ok_bool, status_code, content)
        """
        try:
            jwt_api = JWTAPI()
            shim = self._cbrest_server_shim(rest_connection)
            jwt_api.set_server_values(shim)
            jwt_api.set_endpoint_urls(shim)
            status, content, response = jwt_api.get_jwt_config(expected_status_code=expected_status_code)
            return status, self.status_code(response), content
        except Exception:
            pass

        # Fall back to direct REST connection using only the current endpoint
        path = "settings/jwt"
        api = f"{rest_connection.baseUrl}{path}"
        ok, content, resp = rest_connection._http_request(api, "GET")
        status_code = self.status_code(resp)

        if log_callback:
            log_callback(f"JWT config GET /{path}: ok={ok} status_code={status_code}")

        if expected_status_code is not None and status_code is not None:
            assert int(status_code) == int(expected_status_code), (
                f"Expected GET /{path} status {expected_status_code}, got {status_code}"
            )

        return ok, status_code, content

    def disable_jwt(self, rest_connection):
        """
        Disable JWT authentication on the cluster.
        Uses GET-then-PUT so the payload includes required fields (e.g. issuers).

        Args:
            rest_connection: RestConnection instance for making requests
        """
        try:
            try:
                jwt_api = JWTAPI()
                shim = self._cbrest_server_shim(rest_connection)
                jwt_api.set_server_values(shim)
                jwt_api.set_endpoint_urls(shim)
                status, _content, _response = jwt_api.disable_jwt()
                if status:
                    return
            except Exception:
                pass
            ok, _status_code, content = self.get_jwt_config_settings(
                rest_connection, expected_status_code=None, log_callback=None
            )
            if ok and content:
                try:
                    config = json.loads(content) if isinstance(content, str) else content
                    config["enabled"] = False
                    if "issuers" not in config:
                        config["issuers"] = []
                    self.put_jwt_config(rest_connection, config)
                    return
                except Exception:
                    pass
            self.put_jwt_config(rest_connection, {"enabled": False, "issuers": []})
        except Exception:
            pass


    def verify_no_sensitive_data_in_response(
        self,
        content,
        additional_sensitive_keywords=None,
        context="",
        log_callback=None,
    ):
        """
        Verify response doesn't contain sensitive data.

        Args:
            content: Response content to check
            additional_sensitive_keywords: Additional keywords to check beyond defaults
            context: Description for assertion message
            log_callback: Optional callback function for logging (receives message)

        Returns:
            bool: True if no sensitive data found, False if content is empty
        """
        if not content:
            return False

        if log_callback:
            log_callback(f"Checking for sensitive data in response. Content length: {len(str(content))}")

        content_lower = str(content).lower()
        sensitive_keywords = [
            "private_key",
            "private-key",
            "secret_key",
            "secret-key",
            "decryption_key",
            "decryption-key",
            "password",
            "rsa_private",
            "ec_private",
        ]
        if additional_sensitive_keywords:
            sensitive_keywords.extend(additional_sensitive_keywords)

        found_sensitive = [kw for kw in sensitive_keywords if kw in content_lower]
        assert not found_sensitive, (
            f"Security violation in {context}: Sensitive keywords found in response: {found_sensitive}"
        )
        if log_callback:
            log_callback(f"No sensitive data detected in response. {context}")
        return True

    def get_user_info_from_whoami(self, rest_connection, token):
        """
        Get user information from /whoami endpoint.

        Returns:
            dict or None
        """
        ok, status_code, content = self.request_with_jwt(
            rest_connection, token, ENDPOINT_WHOAMI, method="GET"
        )
        if self.log:
            content_preview = str(content)[:200] if content is not None else "None"
            self.log.info(
                f"get_user_info_from_whoami: ok={ok} status={status_code} "
                f"content_type={type(content).__name__} preview={content_preview}"
            )
        if not ok or not self._is_success_status(status_code):
            if self.log:
                self.log.warning(
                    f"get_user_info_from_whoami: returning None — ok={ok} status={status_code}"
                )
            return None
        try:
            parsed = content if isinstance(content, dict) else json.loads(content)
            user_id = parsed.get("id")
            domain = parsed.get("domain")
            if user_id and domain and domain != "anonymous":
                return parsed
            if self.log:
                self.log.warning(
                    f"get_user_info_from_whoami: unauthenticated response — "
                    f"id={user_id!r} domain={domain!r}"
                )
            return None
        except Exception as e:
            if self.log:
                self.log.warning(
                    f"get_user_info_from_whoami: parse failed — {e} — content={str(content)[:200]}"
                )
            return None

    def verify_token_authentication(self, rest_connection, token, expected_status_code=200):
        """
        Verify JWT token authentication using /whoami endpoint.

        expected_status_code can be:
        - int: exact match
        - True: expect 2xx
        - False: expect non-2xx
        - None: skip validation
        """
        ok, status_code, content = self.verify_token_whoami_rest(rest_connection, token)
        if expected_status_code is not None and status_code is not None:
            if isinstance(expected_status_code, bool):
                assert expected_status_code == self._is_success_status(status_code), (
                    f"Expected authentication to {'succeed' if expected_status_code else 'fail'}, "
                    f"got status {status_code}"
                )
            else:
                assert int(status_code) == int(expected_status_code), (
                    f"Expected authentication status {expected_status_code}, got {status_code}"
                )
        return ok, status_code, content

    def verify_token_authorization(
        self,
        rest_connection,
        token,
        endpoint=None,
        expected_status_code=None,
        method="GET",
        params="",
    ):
        """
        Verify JWT token authorization using a protected endpoint.

        expected_status_code can be int/bool/None similar to verify_token_authentication.
        """
        if endpoint is None:
            endpoint = ENDPOINT_POOLS_BUCKETS
        ok, status_code, content = self.request_with_jwt(rest_connection, token, endpoint, method, params)
        if expected_status_code is not None and status_code is not None:
            if isinstance(expected_status_code, bool):
                assert expected_status_code == self._is_success_status(status_code), (
                    f"Expected authorization to {'succeed' if expected_status_code else 'fail'}, "
                    f"got status {status_code}"
                )
            else:
                assert int(status_code) == int(expected_status_code), (
                    f"Expected authorization status {expected_status_code}, got {status_code}"
                )
        return ok, status_code, content

    def verify_token_complete(
        self,
        rest_connection,
        token,
        expected_auth_status=200,
        expected_authz_status=None,
        authz_endpoint=None,
    ):
        """
        Verify JWT token authentication (/whoami) and authorization (protected endpoint).

        Returns:
            dict: {'auth': (ok,status,content), 'authz': (ok,status,content)}
        """
        result = {
            "auth": self.verify_token_authentication(rest_connection, token, expected_auth_status),
        }
        if expected_authz_status is not None:
            ep = authz_endpoint or ENDPOINT_POOLS_BUCKETS
            result["authz"] = self.verify_token_authorization(
                rest_connection, token, endpoint=ep, expected_status_code=expected_authz_status
            )
        else:
            result["authz"] = (None, None, None)
        return result

    def create_external_user(self, rest_connection, user_name, roles="admin"):
        """
        Create external RBAC user in Couchbase.
        """
        api = f"{rest_connection.baseUrl}settings/rbac/users/external/{user_name}"
        payload = urllib.parse.urlencode({"name": user_name, "roles": roles})
        ok, content, resp = rest_connection._http_request(api, "PUT", payload)
        status_code = self.status_code(resp)
        if not ok:
            raise Exception(
                f"Failed to create external user {user_name}. "
                f"status={status_code} content={content}"
            )
        if self.log:
            self.log.info(f"Created external user {user_name} with roles: {roles}")
        return ok, status_code, content

    def get_external_user(self, rest_connection, user_name):
        """
        Get external user info from RBAC.
        """
        try:
            api = f"{rest_connection.baseUrl}settings/rbac/users/external/{user_name}"
            ok, content, resp = rest_connection._http_request(api, "GET")
            if ok and content:
                return json.loads(content) if isinstance(content, str) else content
        except Exception:
            pass
        return None

    def delete_external_user(self, rest_connection, user_name):
        """
        Delete external user from RBAC.
        """
        if not user_name:
            return True, None, None
        try:
            api = f"{rest_connection.baseUrl}settings/rbac/users/external/{user_name}"
            ok, content, resp = rest_connection._http_request(api, "DELETE")
            status_code = self.status_code(resp)
            if self.log:
                self.log.info(f"Deleted external user {user_name}")
            return ok, status_code, content
        except Exception as e:
            if self.log:
                self.log.warn(f"Error deleting external user {user_name}: {e}")
            return False, None, str(e)


    def setup_jwt_config(self, rest_connection, config, create_groups_callback=None, sleep_callback=None):
        """
        PUT jwt config, optionally create referenced groups, and wait for it to take effect.

        Args:
            rest_connection: RestConnection instance for making requests
            config: JWT configuration dictionary
            create_groups_callback: Optional callback function to create groups (receives group_name)
            sleep_callback: Optional callback function to wait (receives seconds and message)
        """
        if create_groups_callback:
            for issuer in (config or {}).get("issuers", []) or []:
                group_maps = issuer.get("groupsMaps", []) or []
                for group in self.extract_cb_groups_from_group_maps(group_maps):
                    create_groups_callback(group, roles="admin")

        if self.log:
            self.log.info(f"JWT config payload: {json.dumps(config, indent=2)}")
        status, content = self.put_jwt_config(rest_connection, config)
        if self.log:
            self.log.info(f"JWT config PUT status={status} content={content}")
        if sleep_callback:
            sleep_callback(10, "Waiting for JWT config to take effect")

    def perform_key_rotation(self, rest_connection, issuer_name, algorithm, pub_key,
                            token_audience, token_group_matching_rule, sleep_callback=None,
                            sleep_seconds=10, jit_provisioning=True):
        """
        Perform JWT key rotation by updating config with new public key.

        Args:
            rest_connection: RestConnection instance for making requests
            issuer_name: JWT issuer name
            algorithm: JWT signing algorithm
            pub_key: New public key for rotation
            token_audience: Token audience configuration
            token_group_matching_rule: Group matching rules
            sleep_callback: Optional callback function to wait (receives seconds and message)
            sleep_seconds: Seconds to wait for config propagation (default: 10)
            jit_provisioning: Enable/disable JIT user provisioning (default: True)

        Returns:
            dict: Rotated JWT configuration
        """
        rotated_config = self.get_jwt_config(
            issuer_name=issuer_name,
            algorithm=algorithm,
            pub_key=pub_key,
            token_audience=token_audience,
            token_group_matching_rule=token_group_matching_rule,
            jit_provisioning=jit_provisioning,
        )
        self.setup_jwt_config(
            rest_connection=rest_connection,
            config=rotated_config,
            create_groups_callback=None,
            sleep_callback=sleep_callback
        )
        if sleep_callback:
            sleep_callback(sleep_seconds, "Waiting for key rotation to take effect")
        return rotated_config


    def assert_token_fails_repeatedly(self, rest_connection, token, num_retries, context="", log_callback=None):
        """
        Assert that token fails authentication on multiple retry attempts.

        Args:
            rest_connection: RestConnection instance for making requests
            token: JWT token to test
            num_retries: Number of retry attempts
            context: Context string for logging
            log_callback: Optional callback function for logging (receives message)

        Raises:
            AssertionError: If token succeeds on any attempt
        """
        for attempt in range(num_retries):
            if log_callback:
                log_callback(f"{context} - Attempt {attempt + 1}/{num_retries}")

            ok, status_code, content = self.verify_token_rest(rest_connection, token)
            if log_callback:
                log_callback(f"Attempt {attempt + 1}: ok={ok} status={status_code}")

            self.assert_auth_fails(
                status_code,
                f"{context} - Token should fail on attempt {attempt + 1} but succeeded"
            )

            self.assert_unauthorized_status(
                status_code,
                f"{context} - Expected unauthorized status on attempt {attempt + 1}, got {status_code}"
            )

    def assert_token_succeeds_repeatedly(self, rest_connection, token, num_requests, context="", log_callback=None):
        """
        Assert that token succeeds authentication on multiple requests.

        Args:
            rest_connection: RestConnection instance for making requests
            token: JWT token to test
            num_requests: Number of requests
            context: Context string for logging
            log_callback: Optional callback function for logging (receives message)

        Raises:
            AssertionError: If token fails on any request
        """
        for request_num in range(num_requests):
            if log_callback:
                log_callback(f"{context} - Request {request_num + 1}/{num_requests}")

            ok, status_code, content = self.verify_token_rest(rest_connection, token)
            if log_callback:
                log_callback(f"Request {request_num + 1}: ok={ok} status={status_code}")

            self.assert_auth_succeeds(
                ok, status_code,
                f"{context} - Token should work on request {request_num + 1}"
            )

    def verify_malformed_token_rejection(self, rest_connection, token, token_type,
                                       verify_cli_callback=None, log_callback=None):
        """
        Verify that a malformed token is properly rejected.

        This method performs the following validations:
        1. REST request with malformed token should fail (non-2xx status)
        2. CLI request with malformed token should fail if enabled
        3. Status code should be 400 (Bad Request) or 401 (Unauthorized)
        4. Error content should contain JWT/token-related keywords

        Args:
            rest_connection: RestConnection instance for making requests
            token: The malformed token string
            token_type: Description of the token type for logging
            verify_cli_callback: Optional callback for CLI verification (receives token, returns cli_ok, cli_status, cli_out)
            log_callback: Optional callback function for logging (receives message)

        Raises:
            AssertionError: If token verification fails unexpectedly
        """
        rest_ok, status_code, content = None, None, None

        try:
            rest_ok, status_code, content = self.verify_token_rest(rest_connection, token)
            if log_callback:
                log_callback(f"JWT REST ok={rest_ok} status={status_code} content={content}")
        except Exception as e:
            if log_callback:
                log_callback(f"Exception during REST token verification for {token_type}: {str(e)}")
            rest_ok = False
            status_code = None

        cli_ok, cli_status, cli_out = True, None, ""

        if verify_cli_callback:
            try:
                cli_ok, cli_status, cli_out = verify_cli_callback(token)
                if log_callback:
                    log_callback(f"JWT CLI ok={cli_ok} status={cli_status} out={cli_out}")
            except Exception as e:
                if log_callback:
                    log_callback(f"Exception during CLI token verification for {token_type}: {str(e)}")
                cli_ok = False
                cli_status = None

        rest_failed = (status_code is None) or not (200 <= int(status_code) < 300)
        cli_failed = (not cli_ok) if verify_cli_callback else True

        assert rest_failed and cli_failed, (
            "Expected malformed token to fail but at least one path succeeded. "
            f"rest_status={status_code} cli_ok={cli_ok}"
        )

        if status_code is not None:
            assert int(status_code) in (400, 401), (
                f"Unexpected status for malformed token: {status_code}"
            )

        if content:
            content_lower = content.lower()
            assert any(k in content_lower for k in ["jwt", "token", "invalid", "malformed"]), (
                f"Unexpected error content: {content}"
            )

        if log_callback:
            log_callback(f"Malformed token ({token_type}) correctly rejected")

    def setup_tamper_test_env(
        self,
        *,
        issuer_name,
        algorithm,
        key_size,
        token_audience,
        token_groups,
        ttl,
        nbf_seconds=0,
        group_name_prefix="jwt_tamper",
        create_group_callback,
        setup_jwt_config_callback,
        validate_callback,
    ):
        """
        Helper for tamper tests: create keypair + RBAC group + JWT config + valid token.

        Returns:
            tuple: (group_name, valid_token)
        """
        group_name = f"{group_name_prefix}_{uuid.uuid4().hex[:8]}"
        group_maps = [f"^admin$ {group_name}"]

        private_key, pub_key = self.generate_key_pair(algorithm=algorithm, key_size=key_size)
        create_group_callback(group_name, roles="admin")

        config = self.get_jwt_config(
            issuer_name=issuer_name,
            algorithm=algorithm,
            pub_key=pub_key,
            token_audience=token_audience,
            token_group_matching_rule=group_maps,
            jit_provisioning=True,
        )
        setup_jwt_config_callback(config)

        valid_token = self.create_token(
            issuer_name=issuer_name,
            user_name="jwt_tamper_user",
            algorithm=algorithm,
            private_key=private_key,
            token_audience=token_audience,
            user_groups=token_groups,
            ttl=ttl,
            nbf_seconds=nbf_seconds,
            normalize_audience=True,
        )
        validate_callback(valid_token)
        return group_name, valid_token

    def assert_tampered_token_rejected(
        self,
        *,
        tampered_token,
        validate_callback,
        delete_group_callback,
        group_name,
    ):
        """
        Helper for tamper tests: validate token is rejected and cleanup group.
        """
        try:
            validate_callback(tampered_token)
        finally:
            delete_group_callback(group_name)

    @staticmethod
    def _build_jwt_issuer_entry(
        name,
        algorithm,
        audiences,
        group_maps,
        pub_key=None,
        jit_provisioning=True,
        sub_claim="sub",
        aud_claim="aud",
        groups_claim="groups",
        audience_handling="any",
        public_key_source="pem",
        jwks_uri=None,
        jwks_json=None,
    ):
        """
        Build a single issuer entry for the JWT config payload.
        """
        issuer = {
            "name": name,
            "signingAlgorithm": algorithm,
            "publicKeySource": public_key_source,
            "jitProvisioning": jit_provisioning,
            "subClaim": sub_claim,
            "audClaim": aud_claim,
            "audienceHandling": audience_handling,
            "audiences": audiences,
            "groupsClaim": groups_claim,
            "groupsMaps": group_maps,
        }

        if public_key_source == "jwks_uri":
            if not jwks_uri:
                raise ValueError("jwks_uri is required when public_key_source='jwks_uri'")
            issuer["jwksUri"] = jwks_uri
        elif public_key_source in ("jwks_inline", "jwks"):
            if not jwks_json:
                raise ValueError("jwks_json is required when public_key_source is 'jwks'/'jwks_inline'")
            if isinstance(jwks_json, str):
                try:
                    issuer["jwks"] = json.loads(jwks_json)
                except Exception:
                    issuer["jwks"] = jwks_json
            else:
                issuer["jwks"] = jwks_json
            issuer["publicKeySource"] = "jwks"
        else:
            if not pub_key:
                raise ValueError("pub_key is required when public_key_source='pem'")
            issuer["publicKey"] = pub_key

        return issuer

    @staticmethod
    def _b64url(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")

    def build_ec_jwk_from_public_key_pem(self, public_key_pem: str, *, kid: str, algorithm: str):
        """
        Build an EC JWK entry from a PEM public key for ES* algorithms.

        Returns: dict representing one JWK (not wrapped in {"keys": [...]})
        """
        alg = (algorithm or "").upper()
        if alg not in ("ES256", "ES384", "ES512"):
            raise ValueError(f"Unsupported algorithm for EC JWK: {algorithm}")

        pub = load_pem_public_key(public_key_pem.encode("utf-8"))
        if not isinstance(pub, ec.EllipticCurvePublicKey):
            raise ValueError("Provided public key is not an EC public key")

        curve_name = pub.curve.name
        crv_map = {
            "secp256r1": "P-256",
            "secp384r1": "P-384",
            "secp521r1": "P-521",
        }
        if curve_name not in crv_map:
            raise ValueError(f"Unsupported EC curve for JWKS: {curve_name}")

        nums = pub.public_numbers()
        x_int, y_int = nums.x, nums.y
        field_size_bytes = (pub.curve.key_size + 7) // 8
        x = self._b64url(x_int.to_bytes(field_size_bytes, "big"))
        y = self._b64url(y_int.to_bytes(field_size_bytes, "big"))

        return {
            "kty": "EC",
            "crv": crv_map[curve_name],
            "x": x,
            "y": y,
            "use": "sig",
            "alg": alg,
            "kid": kid,
        }

    def build_jwks_for_public_key_pem(self, public_key_pem: str, *, kid: str, algorithm: str):
        """
        Build a JWKS document for the given public key.
        """
        alg = (algorithm or "").upper()
        if alg.startswith("ES"):
            jwk = self.build_ec_jwk_from_public_key_pem(public_key_pem, kid=kid, algorithm=alg)
            return {"keys": [jwk]}
        raise ValueError(f"JWKS building currently supports only ES* algorithms. Got {algorithm}")

    @staticmethod
    def build_jwks_uri(jwks_host: str, jwks_port: int, jwks_path: str = "/jwks.json"):
        """
        Build a http(s) URL string for a JWKS URI.
        """
        if not jwks_path.startswith("/"):
            jwks_path = "/" + jwks_path
        netloc = f"{jwks_host}:{int(jwks_port)}"
        return urllib.parse.urlunparse(("http", netloc, jwks_path, "", "", ""))

    @staticmethod
    def get_jwt_config_from_issuers(issuers):
        """
        Build the top-level JWT config payload from a list of issuer entries.
        """
        return {"enabled": True, "issuers": issuers or []}

    @staticmethod
    def extract_multi_issuer_group_names(env):
        """Extract RBAC group names from a multi-issuer environment dict."""
        group_names = []
        for issuer in (env or {}).get("issuers", []) or []:
            group_name = issuer.get("group_name")
            if group_name:
                group_names.append(group_name)
        for key in ("issuer_a", "issuer_b"):
            if key in (env or {}) and "group_name" in env[key]:
                group_names.append(env[key]["group_name"])
        return group_names

    @staticmethod
    def safe_literal_list(val, default):
        """
        Parse a python-literal list from conf safely.
        `val` is usually a string like "['a','b']" from the .conf.
        """
        if val is None:
            return default
        if isinstance(val, (list, tuple)):
            return list(val)
        if not isinstance(val, str) or not val.strip():
            return default
        try:
            parsed = ast.literal_eval(val)
            if parsed is None:
                return default
            if isinstance(parsed, (list, tuple)):
                return list(parsed)
            if isinstance(parsed, str):
                inner = parsed.strip()
                if (inner.startswith("[") and inner.endswith("]")) or \
                        (inner.startswith("(") and inner.endswith(")")):
                    try:
                        inner_parsed = ast.literal_eval(inner)
                        if isinstance(inner_parsed, (list, tuple)):
                            return list(inner_parsed)
                    except Exception:
                        pass
                return [parsed]
        except Exception:
            try:
                parsed = eval(val)  # noqa: S307
                if isinstance(parsed, (list, tuple)):
                    return list(parsed)
            except Exception:
                return default
        return default

    @staticmethod
    def extract_cb_groups_from_group_maps(group_maps):
        """Extract Couchbase group names from group mapping rule strings."""
        groups = set()
        if not group_maps:
            return groups
        for entry in group_maps:
            try:
                group = entry.strip().split()[-1]
                if group:
                    groups.add(group)
            except Exception:
                continue
        return groups

    @staticmethod
    def status_code(response):
        """Extract HTTP status code from response object."""
        if response is None:
            return None
        return getattr(response, "status", None) or getattr(response, "status_code", None)

    @staticmethod
    def _is_success_status(status_code):
        if status_code is None:
            return False
        try:
            return 200 <= int(status_code) < 300
        except Exception:
            return False

    @staticmethod
    def assert_error_contains_keywords(content, keywords, content_description=""):
        """Assert that error content contains expected keywords."""
        if not content:
            return False
        content_lower = str(content).lower()
        has_indicator = any(str(k).lower() in content_lower for k in (keywords or []))
        assert has_indicator, (
            f"Error message should indicate {content_description}. Content: {content}"
        )
        return True

    @staticmethod
    def _pad_b64(s):
        """Pad base64 string for decoding."""
        return s + "=" * ((4 - len(s) % 4) % 4)

    @staticmethod
    def get_payload_from_token(token):
        """Decode JWT payload (no signature verification). Returns dict of claims."""
        parts = token.split(".")
        if len(parts) < 2:
            raise ValueError("Token must have at least 2 parts")
        payload_b64 = parts[1]
        payload_bytes = base64.urlsafe_b64decode(JWTUtils._pad_b64(payload_b64))
        return json.loads(payload_bytes.decode("utf-8"))

    @staticmethod
    def get_header_from_token(token):
        """Decode JWT header (contains alg, typ, kid). Returns dict."""
        parts = token.split(".")
        if len(parts) < 1:
            raise ValueError("Token must have at least 1 part")
        header_b64 = parts[0]
        header_bytes = base64.urlsafe_b64decode(JWTUtils._pad_b64(header_b64))
        return json.loads(header_bytes.decode("utf-8"))

    @staticmethod
    def get_kid_from_token(token):
        """Extract Key ID (kid) from JWT header. Returns kid string or None."""
        header = JWTUtils.get_header_from_token(token)
        return header.get("kid")

    @staticmethod
    def build_tampered_payload_token(valid_token, payload_overrides):
        """Build a token with modified payload and same signature."""
        parts = valid_token.split(".")
        if len(parts) != 3:
            raise ValueError("Token must have 3 parts")
        payload_bytes = base64.urlsafe_b64decode(JWTUtils._pad_b64(parts[1]))
        payload_dict = json.loads(payload_bytes.decode("utf-8"))
        payload_dict.update(payload_overrides or {})
        tampered_b64 = base64.urlsafe_b64encode(
            json.dumps(payload_dict, separators=(",", ":")).encode("utf-8")
        ).decode("utf-8").rstrip("=")
        return f"{parts[0]}.{tampered_b64}.{parts[2]}"

    @staticmethod
    def build_tampered_header_token(valid_token, header_overrides, drop_signature=False):
        """Build a token with modified header (e.g. alg=none)."""
        parts = valid_token.split(".")
        if len(parts) != 3:
            raise ValueError("Token must have 3 parts")
        header_bytes = base64.urlsafe_b64decode(JWTUtils._pad_b64(parts[0]))
        header_dict = json.loads(header_bytes.decode("utf-8"))
        header_dict.update(header_overrides or {})
        tampered_b64 = base64.urlsafe_b64encode(
            json.dumps(header_dict, separators=(",", ":")).encode("utf-8")
        ).decode("utf-8").rstrip("=")
        if drop_signature:
            return f"{tampered_b64}.{parts[1]}."
        return f"{tampered_b64}.{parts[1]}.{parts[2]}"

    @staticmethod
    def get_single_audience(token_audience):
        """Convert token_audience to single string if it's a list with one element."""
        audience = token_audience
        if isinstance(audience, list) and len(audience) == 1:
            audience = audience[0]
        return audience

    @staticmethod
    def assert_auth_fails(status_code, message):
        """Assert that authentication failed."""
        assert status_code is None or not (200 <= int(status_code) < 300), message

    @staticmethod
    def assert_auth_succeeds(ok, status_code, message):
        """Assert that authentication succeeded."""
        assert ok and status_code and (200 <= int(status_code) < 300), message

    @staticmethod
    def assert_unauthorized_status(status_code, message):
        """Assert that status code indicates unauthorized (401 or 403)."""
        if status_code is not None:
            assert int(status_code) in (401, 403), message

    @staticmethod
    def assert_success_status(status_code, message):
        """Assert that status code indicates HTTP success (2xx)."""
        assert JWTUtils._is_success_status(status_code), message

    def log_oidc_config_safe(self, config):
        """Log OIDC config with client secrets redacted."""
        safe = json.loads(json.dumps(config))
        for issuer in safe.get("issuers", []) or []:
            oidc = issuer.get("oidcSettings") or {}
            if "clientSecret" in oidc:
                oidc["clientSecret"] = "********"
        if self.log:
            self.log.info(json.dumps(safe, indent=2))

    @staticmethod
    def assert_external_identity(whoami, username, domain="external", *, prefix=""):
        """Assert /whoami identity fields."""
        p = f"{prefix}: " if prefix else ""
        assert whoami is not None, f"{p}Should get user info from /whoami"
        assert whoami.get("id") == username, (
            f"{p}Expected user id '{username}', got '{whoami.get('id')}'"
        )
        assert whoami.get("domain") == domain, (
            f"{p}Expected domain '{domain}', got '{whoami.get('domain')}'"
        )

    @staticmethod
    def assert_role_present(whoami, role_name):
        """Assert that role_name is present in /whoami roles. Returns full role list."""
        roles = whoami.get("roles", [])
        role_names = [r.get("role") for r in roles if isinstance(r, dict)]
        assert role_name in role_names, f"Expected '{role_name}' role in {role_names}"
        return role_names

    @staticmethod
    def parse_jwt_config_content(content):
        """Parse JWT config GET response (JSON string or dict) into a dict."""
        if isinstance(content, str):
            return json.loads(content)
        return content

    def wait_jwt_config_propagation(
        self,
        node_rest_connections,
        master_config,
        *,
        timeout=120,
        sleep_interval=2,
        log_callback=None,
        raise_on_timeout=True,
    ):
        """
        Poll until JWT config has propagated to all cluster nodes.

        Args:
            node_rest_connections: List of RestConnection instances (one per node)
            master_config: Expected JWT config dict (from master node)
            timeout: Maximum seconds to wait
            sleep_interval: Seconds between poll attempts
            log_callback: Optional callable(str) for info/debug logging
            raise_on_timeout: If True, raise AssertionError on timeout; else return False

        Returns:
            bool: True if propagated, False if timed out and raise_on_timeout=False
        """
        master_enabled = master_config.get("enabled")
        master_issuer = master_config.get("issuers", [{}])[0]
        master_issuer_name = master_issuer.get("name")

        start_time = time.time()

        while time.time() - start_time < timeout:
            all_ready = True
            failed_node = None
            failed_reason = None

            for node_rest in node_rest_connections:
                node_ip = getattr(node_rest, "ip", None) or getattr(node_rest, "hostname", "unknown")
                try:
                    ok, _, cfg = self.get_jwt_config_settings(
                        node_rest, expected_status_code=200
                    )
                    if not ok:
                        all_ready = False
                        failed_node = node_ip
                        failed_reason = "GET /settings/jwt failed"
                        break

                    cfg = self.parse_jwt_config_content(cfg)

                    if cfg.get("enabled") != master_enabled:
                        all_ready = False
                        failed_node = node_ip
                        failed_reason = f"enabled={cfg.get('enabled')}, expected={master_enabled}"
                        break

                    node_issuer = cfg.get("issuers", [{}])[0]
                    if node_issuer.get("name") != master_issuer_name:
                        all_ready = False
                        failed_node = node_ip
                        failed_reason = (
                            f"issuer={node_issuer.get('name')}, expected={master_issuer_name}"
                        )
                        break

                except Exception as e:
                    all_ready = False
                    failed_node = node_ip
                    failed_reason = str(e)
                    break

            if all_ready:
                elapsed = time.time() - start_time
                msg = (
                    f"JWT config propagated to all {len(node_rest_connections)} nodes "
                    f"in {elapsed:.1f}s"
                )
                if log_callback:
                    log_callback(msg)
                elif self.log:
                    self.log.info(msg)
                return True

            if log_callback:
                log_callback(f"Waiting for JWT config on {failed_node}: {failed_reason}")
            time.sleep(sleep_interval)

        err_msg = f"JWT config did not propagate to all nodes within {timeout}s"
        if self.log:
            self.log.error(err_msg)
        for node_rest in node_rest_connections:
            node_ip = getattr(node_rest, "ip", None) or getattr(node_rest, "hostname", "unknown")
            try:
                ok, _, cfg = self.get_jwt_config_settings(
                    node_rest, expected_status_code=200
                )
                cfg = self.parse_jwt_config_content(cfg)
                detail = (
                    f"Node {node_ip}: enabled={cfg.get('enabled')}, "
                    f"issuers={len(cfg.get('issuers', []))}"
                )
                if self.log:
                    self.log.error(detail)
                elif log_callback:
                    log_callback(detail)
            except Exception as e:
                detail = f"Node {node_ip}: error getting config - {e}"
                if self.log:
                    self.log.error(detail)
                elif log_callback:
                    log_callback(detail)

        if raise_on_timeout:
            raise AssertionError(err_msg)
        return False

    def get_oidc_jwt_config(
        self,
        keycloak_ip,
        keycloak_port,
        keycloak_realm,
        client_id,
        client_secret,
        cluster_master_ip,
        cluster_port=8091,
        cluster_use_https=False,
        algorithm="RS384",  # Keycloak default signing algorithm
        sub_claim="preferred_username",
        aud_claim="azp",  # Keycloak tokens use 'azp' not 'aud' for client ID
        roles_claim=None,
        jit_provisioning=False,
        tls_verify_peer=False,
        pkce_enabled=True,
        nonce_validation=True,
        disable_par=True,
        scopes=None,
        use_https=True,
        expiry_leeway_s=None,
    ):
        """
        Build JWT configuration with OIDC settings for Keycloak integration.

        Note: When using oidcDiscoveryUri, do NOT set jwksUri - the discovery
        endpoint provides it automatically. Setting both causes a 400 error.

        Args:
            keycloak_ip: Keycloak server IP/hostname
            keycloak_port: Keycloak server port
            keycloak_realm: Keycloak realm name (e.g., 'cb')
            client_id: OIDC client ID
            client_secret: OIDC client secret
            cluster_master_ip: Couchbase cluster master IP for redirect URI
            cluster_port: Couchbase cluster port (default: 8091, use 18091 for HTTPS)
            cluster_use_https: Use HTTPS for Couchbase redirect URI (default: False)
            algorithm: JWT signing algorithm (default: RS256)
            sub_claim: Subject claim name (default: preferred_username)
            aud_claim: Audience claim name (default: aud)
            roles_claim: Roles claim path (e.g., 'resource_access.test-client.roles')
            jit_provisioning: Enable JIT user provisioning (default: False)
            tls_verify_peer: Verify TLS peer (default: True)
            pkce_enabled: Enable PKCE for OIDC flow (default: True)
            nonce_validation: Enable nonce validation (default: True)
            disable_par: Disable Pushed Authorization Requests (default: True)
            scopes: List of OIDC scopes (default: ["openid", "profile", "email"])
            use_https: Use HTTPS for Keycloak (default: True)
            expiry_leeway_s: Token expiry leeway in seconds (default: None uses server default 15s)

        Returns:
            dict: JWT configuration with OIDC settings
        """
        scheme = "https" if use_https else "http"
        keycloak_base = f"{scheme}://{keycloak_ip}:{keycloak_port}"
        issuer_name = f"{keycloak_base}{KC_PATH_REALM.format(realm=keycloak_realm)}"
        discovery_uri = f"{keycloak_base}{KC_PATH_DISCOVERY.format(realm=keycloak_realm)}"

        if scopes is None:
            scopes = ["openid", "profile", "email"]

        # Note: Do NOT set jwksUri when using oidcDiscoveryUri
        # OIDC discovery provides the JWKS URI automatically
        # Disable TLS verification for self-signed certs in test automation
        issuer = {
            "name": issuer_name,
            "signingAlgorithm": algorithm,
            "publicKeySource": "jwks_uri",
            "jwksUriTlsVerifyPeer": tls_verify_peer,
            "subClaim": sub_claim,
            "audClaim": aud_claim,
            "audienceHandling": "any",
            "audiences": [client_id],
            "jitProvisioning": jit_provisioning,
            "oidcSettings": {
                "clientId": client_id,
                "clientSecret": client_secret,
                "baseRedirectUris": [
                    f"{'https' if cluster_use_https else 'http'}://{cluster_master_ip}:{cluster_port}/"
                ],
                "oidcDiscoveryUri": discovery_uri,
                "scopes": scopes,
                "pkceEnabled": pkce_enabled,
                "nonceValidation": nonce_validation,
                "disablePushedAuthorizationRequests": disable_par,
                "tlsVerifyPeer": tls_verify_peer,
            },
        }

        if roles_claim:
            issuer["rolesClaim"] = roles_claim

        if expiry_leeway_s is not None:
            issuer["expiryLeewayS"] = expiry_leeway_s

        if self.log:
            self.log.info(f"Building OIDC JWT config for issuer: {issuer_name}")

        return {"enabled": True, "issuers": [issuer]}

    def get_token_from_keycloak(
        self,
        keycloak_ip,
        keycloak_port,
        keycloak_realm,
        client_id,
        client_secret,
        username,
        password,
        tls_verify=False,
        use_https=True,
        timeout=30,
    ):
        """
        Obtain a JWT access token from Keycloak via OAuth2 password grant.

        Args:
            keycloak_ip: Keycloak server IP/hostname
            keycloak_port: Keycloak server port
            keycloak_realm: Keycloak realm name
            client_id: OIDC client ID
            client_secret: OIDC client secret
            username: Keycloak user's username
            password: Keycloak user's password
            tls_verify: Verify TLS certificate (default: False)
            use_https: Use HTTPS for Keycloak (default: True)
            timeout: Request timeout in seconds (default: 30)

        Returns:
            str: JWT access token

        Raises:
            Exception: If token retrieval fails
        """
        scheme = "https" if use_https else "http"
        token_endpoint = (
            f"{scheme}://{keycloak_ip}:{keycloak_port}"
            f"{KC_PATH_TOKEN.format(realm=keycloak_realm)}"
        )

        if self.log:
            self.log.info(f"Requesting JWT token from Keycloak: {token_endpoint}")

        data = {
            "grant_type": "password",
            "scope": "openid",
            "client_id": client_id,
            "client_secret": client_secret,
            "username": username,
            "password": password,
        }

        response = requests.post(
            token_endpoint,
            data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=timeout,
            verify=tls_verify,
        )

        if response.status_code != 200:
            raise Exception(
                f"Failed to obtain JWT token from Keycloak: "
                f"{response.status_code} {response.text}"
            )

        access_token = response.json().get("access_token")
        if not access_token:
            raise Exception(f"No access_token in Keycloak response: {response.text}")

        if self.log:
            self.log.info("JWT token obtained from Keycloak successfully")

        return access_token

    def get_oidc_discovery_config(
        self,
        keycloak_ip,
        keycloak_port,
        keycloak_realm,
        tls_verify=False,
        use_https=True,
        timeout=30,
    ):
        """
        Fetch OIDC discovery configuration from Keycloak.

        Args:
            keycloak_ip: Keycloak server IP/hostname
            keycloak_port: Keycloak server port
            keycloak_realm: Keycloak realm name
            tls_verify: Verify TLS certificate (default: False)
            use_https: Use HTTPS for Keycloak (default: True)
            timeout: Request timeout in seconds (default: 30)

        Returns:
            dict: OIDC discovery configuration
        """
        scheme = "https" if use_https else "http"
        discovery_url = (
            f"{scheme}://{keycloak_ip}:{keycloak_port}"
            f"{KC_PATH_DISCOVERY.format(realm=keycloak_realm)}"
        )

        if self.log:
            self.log.info(f"Fetching OIDC discovery config from: {discovery_url}")

        response = requests.get(discovery_url, timeout=timeout, verify=tls_verify)

        if response.status_code != 200:
            raise Exception(
                f"Failed to fetch OIDC discovery config: "
                f"{response.status_code} {response.text}"
            )

        return response.json()

    @staticmethod
    def validate_oidc_redirect_location(
        redirect_location,
        *,
        client_id,
        keycloak_ip,
        keycloak_use_https=True,
    ):
        """
        Validate OIDC redirect Location header (scheme, host, and OAuth parameters).

        Raises:
            AssertionError: If redirect URL is malformed or missing required parameters
        """
        if not redirect_location:
            raise AssertionError("Redirect Location header is empty")

        expected_scheme = "https://" if keycloak_use_https else "http://"
        if not redirect_location.startswith(expected_scheme):
            raise AssertionError(
                f"Redirect should use {expected_scheme} (keycloak_use_https={keycloak_use_https}). "
                f"Got: {redirect_location[:120]}"
            )

        import urllib.parse as _urlparse
        parsed_redirect = _urlparse.urlparse(redirect_location)
        if parsed_redirect.hostname != keycloak_ip:
            raise AssertionError(
                f"Redirect should target Keycloak host {keycloak_ip}. "
                f"Got: {redirect_location[:120]}"
            )

        if f"client_id={client_id}" not in redirect_location:
            raise AssertionError(
                f"Redirect URL should contain client_id={client_id}"
            )

        if "response_type=code" not in redirect_location:
            raise AssertionError("Redirect URL should contain response_type=code")

        if "scope=" not in redirect_location or "openid" not in redirect_location:
            raise AssertionError("Redirect URL scope should contain 'openid'")

        if "request=" in redirect_location:
            return

        for param in ("code_challenge=", "state=", "nonce="):
            if param not in redirect_location:
                raise AssertionError(
                    f"Redirect URL should contain {param} (PKCE/plain OIDC flow)"
                )

    def validate_oidc_redirect_flow(
        self,
        cluster_master_ip,
        cluster_port,
        issuer_url,
        keycloak_ip,
        timeout=30,
        *,
        couchbase_use_https=False,
        keycloak_use_https=True,
        use_https=None,
    ):
        """
        Validate OIDC redirect flow by checking that /oidc/auth redirects to Keycloak.

        Args:
            cluster_master_ip: Couchbase cluster master IP
            cluster_port: Couchbase REST port (usually 8091)
            issuer_url: Full issuer URL (e.g., https://keycloak:8444/realms/cb)
            keycloak_ip: Keycloak IP to verify in redirect location
            timeout: Request timeout in seconds (default: 30)
            couchbase_use_https: Use HTTPS for Couchbase /oidc/auth URL
            keycloak_use_https: Expected scheme for Keycloak redirect Location header
            use_https: Deprecated alias for couchbase_use_https

        Returns:
            dict: {
                'redirect_status': HTTP status code (302 expected),
                'redirect_location': Location header value,
                'redirects_to_keycloak': bool,
                'redirect_scheme_valid': bool,
                'error': error message if any
            }
        """
        if use_https is not None:
            couchbase_use_https = use_https

        cb_scheme = "https" if couchbase_use_https else "http"
        encoded_issuer = urllib.parse.quote(issuer_url, safe="")
        oidc_auth_url = (
            f"{cb_scheme}://{cluster_master_ip}:{cluster_port}"
            f"{ENDPOINT_OIDC_AUTH}?issuer={encoded_issuer}"
        )

        if self.log:
            self.log.info(
                f"Validating OIDC redirect flow (cb_https={couchbase_use_https}, "
                f"kc_https={keycloak_use_https}): {oidc_auth_url}"
            )

        result = {
            "redirect_status": None,
            "redirect_location": None,
            "redirects_to_keycloak": False,
            "redirect_scheme_valid": False,
            "error": None,
        }

        try:
            response = requests.get(
                oidc_auth_url,
                allow_redirects=False,
                timeout=timeout,
                verify=False,
            )

            result["redirect_status"] = response.status_code
            result["redirect_location"] = response.headers.get("Location", "")

            location = result["redirect_location"]
            expected_kc_scheme = "https://" if keycloak_use_https else "http://"
            if location.startswith(expected_kc_scheme) and keycloak_ip in location:
                result["redirect_scheme_valid"] = True

            if response.status_code == 302 and keycloak_ip in location:
                result["redirects_to_keycloak"] = True

            if self.log:
                self.log.info(
                    f"OIDC redirect: status={result['redirect_status']}, "
                    f"scheme_ok={result['redirect_scheme_valid']}, "
                    f"location={location[:100]}..."
                )

        except Exception as e:
            result["error"] = str(e)
            if self.log:
                self.log.error(f"OIDC redirect validation failed: {e}")

        return result

    def complete_oidc_login_flow(
        self,
        cluster_master_ip,
        cluster_port,
        issuer_url,
        keycloak_ip,
        keycloak_port,
        keycloak_realm,
        username,
        password,
        tls_verify=False,
        timeout=30,
        couchbase_use_https=False,
    ):
        """
        Complete the full OIDC login flow:
        1. Get redirect to Keycloak from /oidc/auth
        2. Submit credentials to Keycloak login form
        3. Get redirect back to Couchbase with auth code

        Args:
            cluster_master_ip: Couchbase cluster master IP
            cluster_port: Couchbase REST port
            issuer_url: Full issuer URL
            keycloak_ip: Keycloak IP
            keycloak_port: Keycloak port
            keycloak_realm: Keycloak realm
            username: Keycloak username
            password: Keycloak password
            tls_verify: Verify TLS certificates (default: False)
            timeout: Request timeout (default: 30)
            couchbase_use_https: Use HTTPS for Couchbase /oidc/auth URL (default: False)

        Returns:
            dict: {
                'success': bool,
                'auth_code': authorization code if successful,
                'session_cookie': session cookie if any,
                'error': error message if any,
                'steps': list of step results for debugging
            }
        """
        result = {
            "success": False,
            "auth_code": None,
            "session_cookie": None,
            "error": None,
            "steps": [],
        }

        session = requests.Session()
        session.verify = tls_verify

        try:
            # Step 1: Hit Couchbase /oidc/auth to get redirect to Keycloak
            encoded_issuer = urllib.parse.quote(issuer_url, safe="")
            cb_scheme = "https" if couchbase_use_https else "http"
            oidc_auth_url = (
                f"{cb_scheme}://{cluster_master_ip}:{cluster_port}"
                f"{ENDPOINT_OIDC_AUTH}?issuer={encoded_issuer}"
            )

            if self.log:
                self.log.info(f"Step 1: Initiating OIDC auth: {oidc_auth_url}")

            resp1 = session.get(oidc_auth_url, allow_redirects=False, timeout=timeout)
            result["steps"].append({
                "step": "oidc_auth",
                "status": resp1.status_code,
                "location": resp1.headers.get("Location", ""),
            })

            if resp1.status_code != 302:
                result["error"] = f"Expected 302 from /oidc/auth, got {resp1.status_code}"
                return result

            keycloak_login_url = resp1.headers.get("Location")
            if not keycloak_login_url or keycloak_ip not in keycloak_login_url:
                result["error"] = f"Redirect not to Keycloak: {keycloak_login_url}"
                return result

            # Step 2: Follow redirect to Keycloak login page
            if self.log:
                self.log.info("Step 2: Following redirect to Keycloak login")

            resp2 = session.get(keycloak_login_url, timeout=timeout)
            result["steps"].append({
                "step": "keycloak_login_page",
                "status": resp2.status_code,
            })

            if resp2.status_code != 200:
                result["error"] = f"Failed to load Keycloak login page: {resp2.status_code}"
                return result

            # Step 3: Extract login form action URL and submit credentials
            # Keycloak login form typically has action URL in the HTML
            action_match = re.search(r'action="([^"]+)"', resp2.text)
            if not action_match:
                result["error"] = "Could not find login form action URL"
                return result

            login_action_url = action_match.group(1).replace("&amp;", "&")

            if self.log:
                self.log.info("Step 3: Submitting credentials to Keycloak")

            login_data = {"username": username, "password": password}
            resp3 = session.post(
                login_action_url,
                data=login_data,
                allow_redirects=False,
                timeout=timeout,
            )
            result["steps"].append({
                "step": "keycloak_login_submit",
                "status": resp3.status_code,
                "location": resp3.headers.get("Location", ""),
            })

            # Step 4: Check for redirect back to Couchbase with auth code
            if resp3.status_code == 302:
                callback_url = resp3.headers.get("Location", "")
                if "code=" in callback_url:
                    # Extract auth code
                    parsed = urllib.parse.urlparse(callback_url)
                    params = urllib.parse.parse_qs(parsed.query)
                    result["auth_code"] = params.get("code", [None])[0]
                    result["success"] = True
                    result["session_cookie"] = session.cookies.get_dict()

                    if self.log:
                        self.log.info("OIDC login flow completed successfully")
                else:
                    result["error"] = f"No auth code in callback: {callback_url}"
            else:
                result["error"] = f"Login submit returned {resp3.status_code}, expected 302"

        except Exception as e:
            result["error"] = str(e)
            if self.log:
                self.log.error(f"OIDC login flow failed: {e}")

        return result

    def verify_keycloak_connectivity(
        self,
        keycloak_ip,
        keycloak_port,
        keycloak_realm,
        tls_verify=False,
        use_https=True,
        timeout=10,
    ):
        """
        Verify connectivity to Keycloak by fetching JWKS endpoint.

        Args:
            keycloak_ip: Keycloak server IP/hostname
            keycloak_port: Keycloak server port
            keycloak_realm: Keycloak realm name
            tls_verify: Verify TLS certificate (default: False)
            use_https: Use HTTPS (default: True)
            timeout: Request timeout in seconds (default: 10)

        Returns:
            dict: {
                'reachable': bool,
                'jwks_keys_count': number of keys in JWKS,
                'error': error message if any
            }
        """
        scheme = "https" if use_https else "http"
        jwks_url = (
            f"{scheme}://{keycloak_ip}:{keycloak_port}"
            f"{KC_PATH_JWKS.format(realm=keycloak_realm)}"
        )

        result = {"reachable": False, "jwks_keys_count": 0, "error": None}

        try:
            response = requests.get(jwks_url, timeout=timeout, verify=tls_verify)
            if response.status_code == 200:
                jwks = response.json()
                result["reachable"] = True
                result["jwks_keys_count"] = len(jwks.get("keys", []))
            else:
                result["error"] = f"JWKS endpoint returned {response.status_code}"
        except Exception as e:
            result["error"] = str(e)

        if self.log:
            self.log.info(f"Keycloak connectivity check: {result}")

        return result

    def get_keycloak_jwks(
        self,
        keycloak_ip,
        keycloak_port,
        keycloak_realm,
        tls_verify=False,
        use_https=True,
        timeout=10,
    ):
        """
        Fetch JWKS (JSON Web Key Set) from Keycloak.

        Args:
            keycloak_ip: Keycloak server IP/hostname
            keycloak_port: Keycloak server port
            keycloak_realm: Keycloak realm name
            tls_verify: Verify TLS certificate (default: False)
            use_https: Use HTTPS (default: True)
            timeout: Request timeout in seconds (default: 10)

        Returns:
            dict: {
                'keys': list of JWK dicts,
                'kids': list of key IDs (kid) from all keys,
                'error': error message if any
            }
        """
        scheme = "https" if use_https else "http"
        jwks_url = (
            f"{scheme}://{keycloak_ip}:{keycloak_port}"
            f"{KC_PATH_JWKS.format(realm=keycloak_realm)}"
        )
        result = {"keys": [], "kids": [], "error": None}
        try:
            response = requests.get(jwks_url, timeout=timeout, verify=tls_verify)
            if response.status_code == 200:
                jwks = response.json()
                result["keys"] = jwks.get("keys", [])
                result["kids"] = [k.get("kid") for k in result["keys"] if k.get("kid")]
                if self.log:
                    self.log.info(f"Fetched JWKS with {len(result['keys'])} keys, kids={result['kids']}")
            else:
                result["error"] = f"JWKS endpoint returned {response.status_code}"
        except Exception as e:
            result["error"] = str(e)
        return result

    def get_keycloak_admin_token(
        self,
        keycloak_ip,
        keycloak_port,
        admin_username="admin",
        admin_password=None,
        tls_verify=False,
        use_https=True,
        timeout=30,
    ):
        """
        Get Keycloak admin access token for Admin API operations.

        Args:
            keycloak_ip: Keycloak server IP/hostname
            keycloak_port: Keycloak server port
            admin_username: Keycloak admin username (default: admin)
            admin_password: Keycloak admin password (required, no default)
            tls_verify: Verify TLS certificate (default: False)
            use_https: Use HTTPS (default: True)
            timeout: Request timeout in seconds (default: 30)

        Returns:
            str: Admin access token, or None on failure
        """
        scheme = "https" if use_https else "http"
        token_url = f"{scheme}://{keycloak_ip}:{keycloak_port}{KC_PATH_ADMIN_TOKEN}"

        try:
            response = requests.post(
                token_url,
                data={
                    "grant_type": "password",
                    "client_id": "admin-cli",
                    "username": admin_username,
                    "password": admin_password,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=timeout,
                verify=tls_verify,
            )
            if response.status_code == 200:
                token = response.json().get("access_token")
                if self.log:
                    self.log.info("Obtained Keycloak admin token")
                return token
            else:
                if self.log:
                    self.log.error(f"Failed to get admin token: {response.status_code} {response.text}")
                return None
        except Exception as e:
            if self.log:
                self.log.error(f"Failed to get admin token: {e}")
            return None

    def get_keycloak_realm_keys(
        self,
        keycloak_ip,
        keycloak_port,
        keycloak_realm,
        admin_token,
        tls_verify=False,
        use_https=True,
        timeout=30,
    ):
        """
        Get realm key providers from Keycloak Admin API.

        Args:
            keycloak_ip: Keycloak server IP/hostname
            keycloak_port: Keycloak server port
            keycloak_realm: Realm name
            admin_token: Keycloak admin access token
            tls_verify: Verify TLS certificate (default: False)
            use_https: Use HTTPS (default: True)
            timeout: Request timeout in seconds (default: 30)

        Returns:
            dict: {
                'keys': list of key info dicts,
                'active_kid': kid of active signing key,
                'error': error message if any
            }
        """
        scheme = "https" if use_https else "http"
        keys_url = f"{scheme}://{keycloak_ip}:{keycloak_port}{KC_PATH_ADMIN_KEYS.format(realm=keycloak_realm)}"

        result = {"keys": [], "active_kid": None, "error": None}
        try:
            response = requests.get(
                keys_url,
                headers={"Authorization": f"Bearer {admin_token}"},
                timeout=timeout,
                verify=tls_verify,
            )
            if response.status_code == 200:
                data = response.json()
                result["keys"] = data.get("keys", [])
                # Find active signing key
                for key in result["keys"]:
                    if key.get("use") == "SIG" and key.get("status") == "ACTIVE":
                        result["active_kid"] = key.get("kid")
                        break
                if self.log:
                    self.log.info(f"Realm keys: {len(result['keys'])}, active_kid={result['active_kid']}")
            else:
                result["error"] = f"Keys endpoint returned {response.status_code}: {response.text}"
        except Exception as e:
            result["error"] = str(e)
        return result

    def get_keycloak_key_components(
        self,
        keycloak_ip,
        keycloak_port,
        keycloak_realm,
        admin_token,
        tls_verify=False,
        use_https=True,
        timeout=30,
    ):
        """
        Get realm key provider components (for modifying priorities).

        Args:
            keycloak_ip: Keycloak server IP/hostname
            keycloak_port: Keycloak server port
            keycloak_realm: Realm name
            admin_token: Keycloak admin access token
            tls_verify: Verify TLS certificate (default: False)
            use_https: Use HTTPS (default: True)
            timeout: Request timeout in seconds (default: 30)

        Returns:
            list: List of key component dicts with id, name, providerId, config
        """
        scheme = "https" if use_https else "http"
        # Get components of type "org.keycloak.keys.KeyProvider"
        components_url = (
            f"{scheme}://{keycloak_ip}:{keycloak_port}"
            f"{KC_PATH_ADMIN_COMPONENTS.format(realm=keycloak_realm)}"
            f"?type=org.keycloak.keys.KeyProvider"
        )

        try:
            response = requests.get(
                components_url,
                headers={"Authorization": f"Bearer {admin_token}"},
                timeout=timeout,
                verify=tls_verify,
            )
            if response.status_code == 200:
                components = response.json()
                if self.log:
                    names = [c.get("name") for c in components]
                    self.log.info(f"Found {len(components)} key components: {names}")
                return components
            else:
                if self.log:
                    self.log.error(f"Failed to get components: {response.status_code}")
                return []
        except Exception as e:
            if self.log:
                self.log.error(f"Failed to get components: {e}")
            return []

    def update_keycloak_key_priority(
        self,
        keycloak_ip,
        keycloak_port,
        keycloak_realm,
        admin_token,
        component_id,
        new_priority,
        tls_verify=False,
        use_https=True,
        timeout=30,
    ):
        """
        Update a key provider's priority in Keycloak.

        Args:
            keycloak_ip: Keycloak server IP/hostname
            keycloak_port: Keycloak server port
            keycloak_realm: Realm name
            admin_token: Keycloak admin access token
            component_id: ID of the key provider component
            new_priority: New priority value (higher = more preferred)
            tls_verify: Verify TLS certificate (default: False)
            use_https: Use HTTPS (default: True)
            timeout: Request timeout in seconds (default: 30)

        Returns:
            bool: True if successful, False otherwise
        """
        scheme = "https" if use_https else "http"
        component_url = (
            f"{scheme}://{keycloak_ip}:{keycloak_port}"
            f"{KC_PATH_ADMIN_COMPONENT.format(realm=keycloak_realm, component_id=component_id)}"
        )

        try:
            # First GET the current component
            response = requests.get(
                component_url,
                headers={"Authorization": f"Bearer {admin_token}"},
                timeout=timeout,
                verify=tls_verify,
            )
            if response.status_code != 200:
                if self.log:
                    self.log.error(f"Failed to get component: {response.status_code}")
                return False

            component = response.json()
            old_priority = component.get("config", {}).get("priority", ["100"])[0]

            # Update priority
            if "config" not in component:
                component["config"] = {}
            component["config"]["priority"] = [str(new_priority)]

            # PUT the updated component
            response = requests.put(
                component_url,
                headers={
                    "Authorization": f"Bearer {admin_token}",
                    "Content-Type": "application/json",
                },
                json=component,
                timeout=timeout,
                verify=tls_verify,
            )
            if response.status_code in [200, 204]:
                if self.log:
                    self.log.info(
                        f"Updated key {component.get('name')} priority: {old_priority} -> {new_priority}"
                    )
                return True
            else:
                if self.log:
                    self.log.error(f"Failed to update component: {response.status_code} {response.text}")
                return False
        except Exception as e:
            if self.log:
                self.log.error(f"Failed to update component: {e}")
            return False

    def rotate_keycloak_signing_key(
        self,
        keycloak_ip,
        keycloak_port,
        keycloak_realm,
        admin_token,
        tls_verify=False,
        use_https=True,
        timeout=30,
    ):
        """
        Rotate the active signing key by swapping priorities of RSA signing keys.

        Finds all RSA signing key providers and swaps priorities so the
        currently lower-priority key becomes active.

        Args:
            keycloak_ip: Keycloak server IP/hostname
            keycloak_port: Keycloak server port
            keycloak_realm: Realm name
            admin_token: Keycloak admin access token
            tls_verify: Verify TLS certificate (default: False)
            use_https: Use HTTPS (default: True)
            timeout: Request timeout in seconds (default: 30)

        Returns:
            dict: {
                'success': bool,
                'old_active_key': name of previously active key,
                'new_active_key': name of newly active key,
                'error': error message if any
            }
        """
        result = {"success": False, "old_active_key": None, "new_active_key": None, "error": None}

        # Get all key components
        components = self.get_keycloak_key_components(
            keycloak_ip, keycloak_port, keycloak_realm, admin_token,
            tls_verify=tls_verify, use_https=use_https, timeout=timeout
        )

        # Filter for RSA signing keys (rsa-generated or rsa providers)
        rsa_signing_keys = []
        for comp in components:
            provider_id = comp.get("providerId", "")
            if "rsa" in provider_id.lower() and "enc" not in provider_id.lower():
                priority = int(comp.get("config", {}).get("priority", ["100"])[0])
                rsa_signing_keys.append({
                    "id": comp.get("id"),
                    "name": comp.get("name"),
                    "priority": priority,
                })

        if len(rsa_signing_keys) < 2:
            result["error"] = f"Need at least 2 RSA signing keys for rotation, found {len(rsa_signing_keys)}"
            return result

        # Sort by priority (highest first = currently active)
        rsa_signing_keys.sort(key=lambda x: x["priority"], reverse=True)
        active_key = rsa_signing_keys[0]
        inactive_key = rsa_signing_keys[1]

        if self.log:
            self.log.info(
                f"Rotating keys: {active_key['name']} (pri={active_key['priority']}) <-> "
                f"{inactive_key['name']} (pri={inactive_key['priority']})"
            )

        # Swap priorities directly — avoids negative values when priorities are small
        new_active_priority = active_key["priority"]
        new_inactive_priority = inactive_key["priority"]

        # Give inactive key the higher (currently active) priority
        success1 = self.update_keycloak_key_priority(
            keycloak_ip, keycloak_port, keycloak_realm, admin_token,
            inactive_key["id"], new_active_priority,
            tls_verify=tls_verify, use_https=use_https, timeout=timeout
        )

        # Give active key the lower (currently inactive) priority
        success2 = self.update_keycloak_key_priority(
            keycloak_ip, keycloak_port, keycloak_realm, admin_token,
            active_key["id"], new_inactive_priority,
            tls_verify=tls_verify, use_https=use_https, timeout=timeout
        )

        if success1 and success2:
            result["success"] = True
            result["old_active_key"] = active_key["name"]
            result["new_active_key"] = inactive_key["name"]
            if self.log:
                self.log.info(f"Key rotation complete: {inactive_key['name']} is now active")
        else:
            result["error"] = f"Failed to update priorities: key1={success1}, key2={success2}"

        return result


# Remote JWKS helpers (used by JWT tests)
def remote_write_file_b64(shell_conn, remote_path: str, content: str):
    """
    Write a UTF-8 string to a remote path using base64 (SSH-safe).
    """
    b64 = base64.b64encode(content.encode("utf-8")).decode("utf-8")
    cmd = (
        "python3 -c \"import base64,os; "
        f"os.makedirs(os.path.dirname('{remote_path}'), exist_ok=True); "
        f"open('{remote_path}','wb').write(base64.b64decode('{b64}'))\""
    )
    return shell_conn.execute_command(cmd)


def remote_curl(shell_conn, url: str, *, timeout_seconds: int = 5):
    """
    Run curl on remote node and return (http_code, body, stderr_text).
    """
    cmd = (
        "sh -c "
        f"\"rm -f /tmp/taf_jwks_curl_body.txt; "
        f"curl -sS -m {int(timeout_seconds)} -o /tmp/taf_jwks_curl_body.txt "
        f"-w '%{{http_code}}' '{url}'\""
    )
    out, err = shell_conn.execute_command(cmd)
    http_code = out[0].strip() if out else ""
    body_out, body_err = shell_conn.execute_command(
        "sh -c \"cat /tmp/taf_jwks_curl_body.txt 2>/dev/null || true\""
    )
    body = "\n".join(body_out).strip()
    stderr_text = "\n".join((err or []) + (body_err or [])).strip()
    return http_code, body, stderr_text


def assert_remote_port_listening(shell_conn, port: int):
    """
    Return diagnostic string if port is listening, else empty string.
    """
    ps_check_cmd = (
        f"sh -c \"lsof -nP -iTCP:{int(port)} -sTCP:LISTEN || "
        f"netstat -an 2>/dev/null | grep '\\\\.{int(port)} ' || true\""
    )
    out, err = shell_conn.execute_command(ps_check_cmd)
    return "\n".join((out or []) + (err or [])).strip()


def stop_remote_process(shell_conn, pid: str | None):
    if not pid:
        return
    shell_conn.execute_command(f"sh -c \"kill {pid} >/dev/null 2>&1 || true\"")


def start_remote_http_server(
    shell_conn,
    *,
    port: int,
    directory: str,
    bind: str,
    log_path="/tmp/taf_jwks_httpserver.log",
):
    """
    Start python http.server on remote node and return (pid, listen_diag_text, start_cmd).
    """
    bind_arg = f"--bind {bind}" if bind else ""
    cmd = (
        "sh -c "
        f"\"cd '{directory}' && "
        f"nohup python3 -m http.server {int(port)} {bind_arg} "
        f"> {log_path} 2>&1 & "
        "pid=$!; sleep 1; "
        f"(lsof -nP -iTCP:{int(port)} -sTCP:LISTEN || netstat -an 2>/dev/null | grep '\\\\.{int(port)} ' || true); "
        "echo PID:$pid\""
    )
    out, err = shell_conn.execute_command(cmd)
    out_text = "\n".join((out or []) + (err or [])).strip()
    pid = None
    for line in (out_text.splitlines() if out_text else []):
        if line.startswith("PID:"):
            pid = line.split("PID:", 1)[1].strip() or None
            break
    listen_diag = assert_remote_port_listening(shell_conn, port)
    return pid, listen_diag, cmd


def setup_jwks_uri_issuer_env(
    *,
    jwt_utils_obj,
    cluster_master,
    issuer_name: str,
    algorithm: str,
    pub_key: str,
    token_audience,
    kid: str,
    jwks_port: int,
    jwks_bind: str,
    jwks_uri_host_mode: str,
    group_prefix: str,
    create_group_callback,
    delete_group_callback=None,
    log_callback=None,
    start_attempts: int = 10,
):
    """
    Shared setup for JWKS-URI based tests.
    Returns env dict with shell_conn/pid/tmp_dir/group_name/group_maps/jwks_uri/config.
    """

    group_name = f"{group_prefix}_{uuid.uuid4().hex[:8]}"
    tmp_dir = f"/tmp/taf_jwks_{uuid.uuid4().hex[:8]}"
    pid = None

    shell_conn = RemoteMachineShellConnection(cluster_master)
    shell_conn.execute_command("sh -c \"pkill -f 'http.server' >/dev/null 2>&1 || true\"")

    create_group_callback(group_name, roles="admin")
    group_maps = [f"^admin$ {group_name}"]

    jwks = jwt_utils_obj.build_jwks_for_public_key_pem(pub_key, kid=kid, algorithm=algorithm)
    jwks_json = json.dumps(jwks, separators=(",", ":"))
    remote_write_file_b64(shell_conn, f"{tmp_dir}/jwks.json", jwks_json)

    if jwks_uri_host_mode == "node_ip":
        jwks_host = getattr(cluster_master, "ip", None) or getattr(cluster_master, "hostname", None)
    else:
        jwks_host = "127.0.0.1"

    chosen_port = None
    jwks_uri = None
    last_code, last_body = None, None

    for attempt in range(1, max(1, int(start_attempts)) + 1):
        candidate_port = int(jwks_port) + (attempt - 1)
        pid, _listen_diag, start_cmd = start_remote_http_server(
            shell_conn, port=candidate_port, directory=tmp_dir, bind=jwks_bind
        )
        if log_callback:
            log_callback(f"Starting remote JWKS server: {start_cmd}")

        jwks_uri = jwt_utils_obj.build_jwks_uri(
            jwks_host=jwks_host, jwks_port=candidate_port, jwks_path="/jwks.json"
        )
        if log_callback:
            log_callback(f"Using JWKS URI (attempt {attempt}): {jwks_uri}")

        http_code, body, _curl_err = remote_curl(shell_conn, jwks_uri, timeout_seconds=5)
        last_code, last_body = http_code, body
        if str(http_code) == "200" and body and "\"keys\"" in body:
            chosen_port = candidate_port
            break

        stop_remote_process(shell_conn, pid)
        pid = None

    if chosen_port is None:
        try:
            shell_conn.execute_command(f"sh -c \"rm -rf '{tmp_dir}' || true\"")
        except Exception:
            pass
        shell_conn.disconnect()
        if delete_group_callback:
            try:
                delete_group_callback(group_name)
            except Exception:
                pass
        raise AssertionError(
            "JWKS server never started correctly.\n"
            f"Last HTTP code={last_code}\n"
            f"Last body={last_body}\n"
            "Likely port conflict or server start failure."
        )

    config = jwt_utils_obj.get_jwt_config(
        issuer_name=issuer_name,
        algorithm=algorithm,
        pub_key=None,
        token_audience=token_audience,
        token_group_matching_rule=group_maps,
        jit_provisioning=True,
        public_key_source="jwks_uri",
        jwks_uri=jwks_uri,
    )

    return {
        "shell_conn": shell_conn,
        "pid": pid,
        "tmp_dir": tmp_dir,
        "group_name": group_name,
        "group_maps": group_maps,
        "jwks_uri": jwks_uri,
        "config": config,
    }


def cleanup_jwks_uri_issuer_env(env: dict | None, delete_group_callback):
    if not env:
        return
    shell_conn = env.get("shell_conn")
    pid = env.get("pid")
    tmp_dir = env.get("tmp_dir")
    group_name = env.get("group_name")

    try:
        if shell_conn:
            stop_remote_process(shell_conn, pid)
            if tmp_dir:
                shell_conn.execute_command(f"sh -c \"rm -rf '{tmp_dir}' || true\"")
    except Exception:
        pass
    try:
        if group_name:
            delete_group_callback(group_name)
    except Exception:
        pass
    try:
        if shell_conn:
            shell_conn.disconnect()
    except Exception:
        pass
