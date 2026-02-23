import time
import uuid
import ast
import jwt
import json
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa, ec


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
        return {
            "enabled": True,
            "issuers": [
                {
                    "name": issuer_name,
                    "signingAlgorithm": algorithm,
                    "publicKeySource": "pem",
                    "publicKey": pub_key,
                    "jitProvisioning": jit_provisioning,
                    "subClaim": "sub",
                    "audClaim": "aud",
                    "audienceHandling": "any",
                    "audiences": token_audience,
                    "groupsClaim": "groups",
                    "groupsMaps": token_group_matching_rule
                }
            ]
        }

    def create_token(self, issuer_name, user_name, algorithm, private_key, 
                     token_audience=None, user_groups=None, ttl=300, 
                     nbf_seconds=0):
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
        Returns:
            str: Encoded JWT token
        """
        curr_time = int(time.time())
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

    @staticmethod
    def safe_literal_list(val, default):
        """
        Parse a python-literal list from conf safely.
        `val` is usually a string like "['a','b']" from the .conf.

        Args:
            val: String or list/parseable value from config
            default: Default value to return if parsing fails

        Returns:
            list: Parsed list or default value
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
        """
        Extract Couchbase group names from group mapping rule strings.

        Args:
            group_maps: List of group mapping strings like ["^admin$ group_name"]

        Returns:
            set: Set of Couchbase group names
        """
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
        """
        Extract HTTP status code from response object.

        Args:
            response: Response object with .status or .status_code attribute

        Returns:
            int or None: HTTP status code
        """
        if response is None:
            return None
        return getattr(response, "status", None) or getattr(response, "status_code", None)

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

    def verify_token_rest(self, rest_connection, token):
        """
        Verify JWT token works for a REST request.

        Args:
            rest_connection: RestConnection instance for making requests
            token: JWT bearer token to verify

        Returns:
            tuple: (ok_bool, status_code, content)
        """
        return self.request_with_jwt(rest_connection, token, "/pools/default/buckets", method="GET")

    def put_jwt_config(self, rest_connection, config):
        """
        PUT JWT configuration to the cluster.

        Args:
            rest_connection: RestConnection instance for making requests
            config: JWT configuration dictionary

        Returns:
            tuple: (status_code, content)
        """
        headers = rest_connection._create_capi_headers(contentType="application/json")
        body = json.dumps(config)

        paths = ["settings/security/jwt", "settings/jwt"]
        last_status, last_content = None, None
        for path in paths:
            api = f"{rest_connection.baseUrl}{path}"
            ok, content, resp = rest_connection._http_request(api, "PUT", body, headers=headers)
            status = self.status_code(resp)
            last_status, last_content = status, content
            if ok:
                return status, content
            if status == 404:
                continue
            raise Exception(
                f"Failed to PUT /{path}. status={status} content={content}"
            )

        raise Exception(
            "Failed to PUT JWT config using all known endpoints. "
            f"last_status={last_status} last_content={last_content}"
        )

    def disable_jwt(self, rest_connection):
        """
        Disable JWT authentication on the cluster.

        Args:
            rest_connection: RestConnection instance for making requests
        """
        try:
            self.put_jwt_config(rest_connection, {"enabled": False})
        except Exception:
            pass

    def setup_jwt_config(self, rest_connection, config, create_groups_callback=None, sleep_callback=None):
        """
        PUT jwt config, optionally create referenced groups, and wait for it to take effect.

        Args:
            rest_connection: RestConnection instance for making requests
            config: JWT configuration dictionary
            create_groups_callback: Optional callback function to create groups (receives group_name)
            sleep_callback: Optional callback function to wait (receives seconds and message)
        """
        group_maps = config.get("issuers", [{}])[0].get("groupsMaps", [])
        if create_groups_callback:
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

    @staticmethod
    def get_single_audience(token_audience):
        """
        Convert token_audience to single string if it's a list with one element.

        Args:
            token_audience: Audience value (list, string, or None)

        Returns:
            Single audience string or the original value
        """
        audience = token_audience
        if isinstance(audience, list) and len(audience) == 1:
            audience = audience[0]
        return audience

    @staticmethod
    def create_token_with_ttl(issuer_name, user_name, algorithm, private_key,
                             token_audience, user_groups, ttl, nbf_seconds=0):
        """
        Create JWT token with custom TTL.

        Args:
            issuer_name: JWT issuer name
            user_name: Username for the token
            algorithm: JWT signing algorithm
            private_key: Private key for signing
            token_audience: Token audience (list or string)
            user_groups: User groups list
            ttl: Time to live in seconds (can be negative for expired tokens)
            nbf_seconds: Not before offset in seconds (default: 0)

        Returns:
            str: JWT token
        """
        token_audience = JWTUtils.get_single_audience(token_audience)

        return JWTUtils.create_token(
            issuer_name=issuer_name,
            user_name=user_name,
            algorithm=algorithm,
            private_key=private_key,
            token_audience=token_audience,
            user_groups=user_groups,
            ttl=ttl,
            nbf_seconds=nbf_seconds,
        )

    @staticmethod
    def assert_auth_fails(status_code, message):
        """
        Assert that authentication failed.

        Args:
            status_code: HTTP status code
            message: Assertion error message

        Raises:
            AssertionError: If authentication appears to succeed
        """
        assert status_code is None or not (200 <= int(status_code) < 300), message

    @staticmethod
    def assert_auth_succeeds(ok, status_code, message):
        """
        Assert that authentication succeeded.

        Args:
            ok: Boolean success indicator
            status_code: HTTP status code
            message: Assertion error message

        Raises:
            AssertionError: If authentication fails
        """
        assert ok and status_code and (200 <= int(status_code) < 300), message

    @staticmethod
    def assert_unauthorized_status(status_code, message):
        """
        Assert that status code indicates unauthorized (401 or 403).

        Args:
            status_code: HTTP status code
            message: Assertion error message

        Raises:
            AssertionError: If status code is not 401 or 403
        """
        if status_code is not None:
            assert int(status_code) in (401, 403), message

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
