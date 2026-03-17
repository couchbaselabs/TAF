import time
import uuid
import ast
import jwt
import json
import base64
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa, ec

from cb_server_rest_util.security.jwt import JWTAPI


# Endpoints used for JWT auth (whoami) and authz (pools/buckets) validation
ENDPOINT_WHOAMI = "/whoami"
ENDPOINT_POOLS_BUCKETS = "/pools/default/buckets"

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

    def get_jwt_config(self, issuer_name, algorithm, pub_key, token_audience, 
                       token_group_matching_rule, jit_provisioning=True):
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
        
        jwt_token = jwt.encode(payload=payload,
                               algorithm=algorithm,
                               key=private_key)
        return jwt_token


    def request_with_jwt(self, rest_connection, token, endpoint, method="GET", params=""):
        """
        Make an HTTP request with JWT bearer token authorization.

        Args:
            rest_connection: RestConnection instance for making requests
            token: JWT bearer token
            endpoint: REST endpoint path (e.g., "/pools/default/buckets")
            method: HTTP method (default: "GET")
            params: Request parameters (default: "")

        Returns:
            tuple: (ok_bool, status_code, content)
        """
        if endpoint.startswith("/"):
            endpoint = endpoint[1:]
        api = f"{rest_connection.baseUrl}{endpoint}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "*/*",
            "Content-Type": "application/x-www-form-urlencoded",
        }
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
                        f"JWT authz REST expected success but failed. status={rest_status} content={rest_content}"
                    )
                if verify_cli and not cli_ok:
                    raise AssertionError(
                        f"JWT authz CLI expected success but failed. status={cli_status} out={cli_out}"
                    )
            else:
                rest_failed = not rest_success
                cli_failed = (not cli_ok) if verify_cli else True
                if not (rest_failed and cli_failed):
                    raise AssertionError(
                        "JWT authz expected failure but at least one path succeeded. "
                        f"rest_status={rest_status} cli_ok={cli_ok}"
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
                        f"JWT whoami expected success but failed. status={who_status} content={who_content}"
                    )
            else:
                if who_success:
                    raise AssertionError(
                        f"JWT whoami expected failure but succeeded. status={who_status} content={who_content}"
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
        if not ok or not self._is_success_status(status_code):
            return None
        try:
            return json.loads(content)
        except Exception:
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
        import urllib.parse

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
        pub_key,
        audiences,
        group_maps,
        jit_provisioning=True,
        sub_claim="sub",
        aud_claim="aud",
        groups_claim="groups",
        audience_handling="any",
    ):
        """
        Build a single issuer entry for the JWT config payload.
        """
        return {
            "name": name,
            "signingAlgorithm": algorithm,
            "publicKeySource": "pem",
            "publicKey": pub_key,
            "jitProvisioning": jit_provisioning,
            "subClaim": sub_claim,
            "audClaim": aud_claim,
            "audienceHandling": audience_handling,
            "audiences": audiences,
            "groupsClaim": groups_claim,
            "groupsMaps": group_maps,
        }

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
