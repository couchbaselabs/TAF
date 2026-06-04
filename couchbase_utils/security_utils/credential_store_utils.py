import json
import time
import urllib.parse

import requests

from cb_server_rest_util.security.credential_store import (
    ALLOWED_SERVICES,
    ENDPOINT_CBAUTH_CREDENTIAL,
    ENDPOINT_CREDENTIAL_STORE,
    ENDPOINT_CREDENTIALS,
    ENDPOINT_RBAC_SERVICES,
    ENDPOINT_RBAC_USERS,
    ERROR_CREDENTIAL_EXPIRED,
    ERROR_INSUFFICIENT_PERMISSIONS,
    ERROR_SERVICE_GUARDRAIL_BLOCKED,
    ERROR_UNSUPPORTED_SCHEMA_VERSION,
    CredentialStoreAPI,
)

# ── Re-export endpoint + error constants for test use ──────────────────────
__all__ = [
    "CredentialStoreUtils",
    "SENSITIVE_FIELDS_BY_TYPE",
    "ENDPOINT_CREDENTIALS",
    "ENDPOINT_CREDENTIAL_STORE",
    "ENDPOINT_RBAC_SERVICES",
    "ENDPOINT_CBAUTH_CREDENTIAL",
    "ENDPOINT_RBAC_USERS",
    "ALLOWED_SERVICES",
    "ERROR_INSUFFICIENT_PERMISSIONS",
    "ERROR_SERVICE_GUARDRAIL_BLOCKED",
    "ERROR_CREDENTIAL_EXPIRED",
    "ERROR_UNSUPPORTED_SCHEMA_VERSION",
]

# ── Sensitive fields per credential type ────────────────────────────────────
# Fields listed here must NEVER appear as plaintext in admin (non-consume) API responses.
SENSITIVE_FIELDS_BY_TYPE = {
    "aws": {"secretAccessKey", "sessionToken"},
    "azureShared": {"accountKey"},
    "azureAd": {"clientSecret", "certPassword"},
    "azureSas": {"sharedAccessSignature"},
    "azureManaged": set(),
    "gcp": {"jsonCredentials", "secretAccessKey"},
    "http": {"password", "token", "privateKey", "passphrase"},
    "couchbase": {"password", "privateKey", "passphrase"},
}


class CredentialStoreUtils:
    """
    High-level utilities for Credential Store tests.

    Mirrors the JWTUtils pattern: every public method takes a `rest_connection`
    as first argument and falls back from CredentialStoreAPI (CBRestConnection)
    to raw _http_request when the API class cannot be initialised.

    Secrets are never stored on this object. Pass them explicitly per call.
    """

    def __init__(self, log=None):
        self.log = log

    # ── Internal helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _connection_use_https(rest_connection):
        """True when the connection targets the cluster over HTTPS."""
        port = getattr(rest_connection, "port", None)
        try:
            port = int(port)
        except (TypeError, ValueError):
            port = None

        def _as_bool(value):
            if isinstance(value, str):
                return value.strip().lower() in ("1", "true", "yes", "on")
            return bool(value)

        use_https = _as_bool(getattr(rest_connection, "use_https", False))
        ssl = _as_bool(getattr(rest_connection, "ssl", False))
        return use_https or ssl or port == 18091

    @staticmethod
    def _url_host(rest_connection):
        """Return host for URL construction; prefer hostname when ip is absent."""
        ip = getattr(rest_connection, "ip", None)
        hostname = getattr(rest_connection, "hostname", None) or ""
        if hostname and (not ip or hostname.find(ip) == -1):
            return hostname
        return ip

    @staticmethod
    def _encode_path_segment(value, *, allow_empty=False):
        """
        URL-encode a user-supplied path segment.

        Args:
            value: Segment value (str, bytes, or bytearray).
            allow_empty: When False (default), raise on None or empty so
                callers fail fast before sending a malformed request.
                Set True only for explicitly optional segments.
        """
        if value is None or (value == "" and not allow_empty):
            raise ValueError("path segment must be a non-empty string")
        if value == "" and allow_empty:
            return ""
        if isinstance(value, (bytes, bytearray)):
            return urllib.parse.quote_from_bytes(bytes(value), safe="")
        return urllib.parse.quote(str(value), safe="")

    @staticmethod
    def _cs_api(rest_connection, username=None, password=None):
        """Build and configure a CredentialStoreAPI from a RestConnection shim."""

        class _Shim:
            pass

        shim = _Shim()
        shim.ip = getattr(rest_connection, "ip", None)
        shim.port = getattr(rest_connection, "port", None)
        shim.rest_username = (
            username
            or getattr(rest_connection, "username", None)
            or getattr(rest_connection, "rest_username", None)
        )
        shim.rest_password = (
            password
            or getattr(rest_connection, "password", None)
            or getattr(rest_connection, "rest_password", None)
        )
        shim.type = getattr(rest_connection, "type", "default")
        shim.services = getattr(rest_connection, "services", None)
        shim.hostname = getattr(rest_connection, "hostname", None)
        api = CredentialStoreAPI()
        api.set_server_values(shim)
        api.set_endpoint_urls(shim)
        # Preserve caller's baseUrl (handles HTTPS / load-balancer hostnames).
        # Check both attribute names used by different RestConnection variants.
        caller_base = (
            getattr(rest_connection, "baseUrl", None)
            or getattr(rest_connection, "base_url", None)
        )
        if caller_base:
            api.base_url = caller_base.rstrip("/")
        elif rest_connection.ip and rest_connection.port:
            scheme = (
                "https"
                if CredentialStoreUtils._connection_use_https(rest_connection)
                else "http"
            )
            host = CredentialStoreUtils._url_host(rest_connection)
            if host and ":" in host and not host.startswith("["):
                host = f"[{host}]"
            api.base_url = f"{scheme}://{host}:{rest_connection.port}"
        return api

    @staticmethod
    def _base_url(rest_connection):
        """Return a URL string ending without trailing slash (e.g. http://ip:port)."""
        base = (
            getattr(rest_connection, "baseUrl", None)
            or getattr(rest_connection, "base_url", None)
        )
        if base:
            return base.rstrip("/")
        scheme = (
            "https"
            if CredentialStoreUtils._connection_use_https(rest_connection)
            else "http"
        )
        host = CredentialStoreUtils._url_host(rest_connection)
        if host and ":" in host and not host.startswith("["):
            host = f"[{host}]"
        return f"{scheme}://{host}:{rest_connection.port}"

    @staticmethod
    def status_code(response):
        """Extract integer HTTP status code from a response object or dict."""
        if response is None:
            return None
        if isinstance(response, dict):
            for key in ("status_code", "status", "statusCode", "code"):
                if key in response:
                    try:
                        return int(response[key])
                    except (TypeError, ValueError):
                        pass
        for attr in ("status_code", "status"):
            val = getattr(response, attr, None)
            if val is not None:
                try:
                    return int(val)
                except (TypeError, ValueError):
                    pass
        return None

    @staticmethod
    def _is_success_status(status):
        """True for 2xx status codes."""
        try:
            return 200 <= int(status) < 300
        except (TypeError, ValueError):
            return False

    def parse_content(self, content):
        """
        Return parsed JSON dict.

        On parse failure, logs a warning and returns raw content instead of
        None — callers must not treat a non-None return as 'valid JSON'.
        """
        if content is None:
            return None
        try:
            if isinstance(content, (bytes, bytearray)):
                content = content.decode("utf-8", "replace")
            if isinstance(content, str):
                return json.loads(content)
            return content
        except (ValueError, TypeError) as exc:
            if self.log:
                self.log.warning(f"Failed to parse JSON response: {exc}")
            return None

    # ── Credential payload builders ───────────────────────────────────────────

    @staticmethod
    def build_aws_payload(
        access_key_id,
        secret_access_key,
        region,
        *,
        description=None,
        allowed_services=None,
        expires_at_ms=None,
        session_token=None,
        endpoint=None,
    ):
        """
        Build an 'aws' credential POST/PUT payload.

        Args:
            access_key_id: AWS access key ID (non-sensitive)
            secret_access_key: AWS secret access key (sensitive)
            region: AWS region (e.g. 'us-east-1')
            description: Optional free-text description
            allowed_services: Optional list of service names for guardrails
            expires_at_ms: Optional int, ms since epoch (must be >= now + 5 minutes)
            session_token: Optional temporary session token (sensitive)
            endpoint: Optional custom endpoint URL

        Returns:
            dict suitable for POST /settings/credentials/:id
        """
        fields = {
            "accessKeyId": access_key_id,
            "secretAccessKey": secret_access_key,
            "region": region,
        }
        if session_token:
            fields["sessionToken"] = session_token
        if endpoint:
            fields["endpoint"] = endpoint

        payload = {"type": "aws", "fields": fields}

        # description, expiresAt, and guardrails are top-level request fields,
        # not nested under meta (meta is only present in GET responses).
        if description:
            payload["description"] = description
        if expires_at_ms is not None:
            payload["expiresAt"] = expires_at_ms
        if allowed_services is not None:
            payload["guardrails"] = {"allowedServices": allowed_services}

        return payload

    @staticmethod
    def expires_at_ms(seconds_from_now):
        """Return an expiresAt value (ms epoch) at least `seconds_from_now` in the future."""
        return int((time.time() + seconds_from_now) * 1000)

    # ── Admin credential CRUD ─────────────────────────────────────────────────

    def create_credential(self, rest, cred_id, payload, username=None, password=None):
        """
        POST /settings/credentials/:id.

        Returns:
            tuple: (status_code, content)
        """
        try:
            api = self._cs_api(rest, username=username, password=password)
            _, content, response = api.create_credential(cred_id, payload)
            return self.status_code(response), content
        except Exception as _cs_err:
            if self.log:
                self.log.debug(f"CredentialStoreAPI path unavailable, using fallback: {_cs_err}")
        encoded_id = self._encode_path_segment(cred_id)
        url = f"{self._base_url(rest)}/{ENDPOINT_CREDENTIALS.lstrip('/')}/{encoded_id}"
        headers = rest._create_capi_headers(
            username=username, password=password, contentType="application/json"
        )
        _, content, resp = rest._http_request(
            url, "POST", json.dumps(payload), headers=headers
        )
        return self.status_code(resp), content

    def get_credential(self, rest, cred_id, username=None, password=None):
        """
        GET /settings/credentials/:id.

        Returns:
            tuple: (status_code, content)
        """
        try:
            api = self._cs_api(rest, username=username, password=password)
            _, content, response = api.get_credential(cred_id)
            return self.status_code(response), content
        except Exception as _cs_err:
            if self.log:
                self.log.debug(f"CredentialStoreAPI path unavailable, using fallback: {_cs_err}")
        encoded_id = self._encode_path_segment(cred_id)
        url = f"{self._base_url(rest)}/{ENDPOINT_CREDENTIALS.lstrip('/')}/{encoded_id}"
        headers = rest._create_capi_headers(username=username, password=password)
        _, content, resp = rest._http_request(url, "GET", headers=headers)
        return self.status_code(resp), content

    def list_credentials(self, rest, prefix=None, username=None, password=None):
        """
        GET /settings/credentials[?prefix=...].

        Returns:
            tuple: (status_code, content)
        """
        try:
            api = self._cs_api(rest, username=username, password=password)
            _, content, response = api.list_credentials(prefix=prefix)
            return self.status_code(response), content
        except Exception as _cs_err:
            if self.log:
                self.log.debug(f"CredentialStoreAPI path unavailable, using fallback: {_cs_err}")
        path = ENDPOINT_CREDENTIALS.lstrip("/")
        if prefix:
            path = f"{path}?prefix={urllib.parse.quote(prefix, safe='')}"
        url = f"{self._base_url(rest)}/{path}"
        headers = rest._create_capi_headers(username=username, password=password)
        _, content, resp = rest._http_request(url, "GET", headers=headers)
        return self.status_code(resp), content

    def update_credential(self, rest, cred_id, payload, username=None, password=None):
        """
        PUT /settings/credentials/:id (full replace; type is immutable).

        Returns:
            tuple: (status_code, content)
        """
        try:
            api = self._cs_api(rest, username=username, password=password)
            _, content, response = api.update_credential(cred_id, payload)
            return self.status_code(response), content
        except Exception as _cs_err:
            if self.log:
                self.log.debug(f"CredentialStoreAPI path unavailable, using fallback: {_cs_err}")
        encoded_id = self._encode_path_segment(cred_id)
        url = f"{self._base_url(rest)}/{ENDPOINT_CREDENTIALS.lstrip('/')}/{encoded_id}"
        headers = rest._create_capi_headers(
            username=username, password=password, contentType="application/json"
        )
        _, content, resp = rest._http_request(
            url, "PUT", json.dumps(payload), headers=headers
        )
        return self.status_code(resp), content

    def delete_credential(self, rest, cred_id, username=None, password=None):
        """
        DELETE /settings/credentials/:id.

        Returns:
            tuple: (status_code, content)
        """
        try:
            api = self._cs_api(rest, username=username, password=password)
            _, content, response = api.delete_credential(cred_id)
            return self.status_code(response), content
        except Exception as _cs_err:
            if self.log:
                self.log.debug(f"CredentialStoreAPI path unavailable, using fallback: {_cs_err}")
        encoded_id = self._encode_path_segment(cred_id)
        url = f"{self._base_url(rest)}/{ENDPOINT_CREDENTIALS.lstrip('/')}/{encoded_id}"
        headers = rest._create_capi_headers(username=username, password=password)
        _, content, resp = rest._http_request(url, "DELETE", headers=headers)
        return self.status_code(resp), content

    # ── Store settings ────────────────────────────────────────────────────────

    def get_store_settings(self, rest, username=None, password=None):
        """
        GET /settings/credentialStore.

        Returns:
            tuple: (status_code, content)
        """
        try:
            api = self._cs_api(rest, username=username, password=password)
            _, content, response = api.get_credential_store_settings()
            return self.status_code(response), content
        except Exception as _cs_err:
            if self.log:
                self.log.debug(f"CredentialStoreAPI path unavailable, using fallback: {_cs_err}")
        url = f"{self._base_url(rest)}/{ENDPOINT_CREDENTIAL_STORE.lstrip('/')}"
        headers = rest._create_capi_headers(username=username, password=password)
        _, content, resp = rest._http_request(url, "GET", headers=headers)
        return self.status_code(resp), content

    def put_store_settings(
        self,
        rest,
        config_encryption_override,
        n2n_encryption_override,
        username=None,
        password=None,
    ):
        """
        PUT /settings/credentialStore — both fields are always required.

        Returns:
            tuple: (status_code, content)
        """
        settings = {
            "configEncryptionOverride": config_encryption_override,
            "n2nEncryptionOverride": n2n_encryption_override,
        }
        try:
            api = self._cs_api(rest, username=username, password=password)
            _, content, response = api.put_credential_store_settings(settings)
            return self.status_code(response), content
        except Exception as _cs_err:
            if self.log:
                self.log.debug(f"CredentialStoreAPI path unavailable, using fallback: {_cs_err}")
        url = f"{self._base_url(rest)}/{ENDPOINT_CREDENTIAL_STORE.lstrip('/')}"
        headers = rest._create_capi_headers(
            username=username, password=password, contentType="application/json"
        )
        _, content, resp = rest._http_request(
            url, "PUT", json.dumps(settings), headers=headers
        )
        return self.status_code(resp), content

    def delete_store_settings(self, rest, username=None, password=None):
        """
        DELETE /settings/credentialStore — reset to defaults.

        Returns:
            tuple: (status_code, content)
        """
        try:
            api = self._cs_api(rest, username=username, password=password)
            _, content, response = api.delete_credential_store_settings()
            return self.status_code(response), content
        except Exception as _cs_err:
            if self.log:
                self.log.debug(f"CredentialStoreAPI path unavailable, using fallback: {_cs_err}")
        url = f"{self._base_url(rest)}/{ENDPOINT_CREDENTIAL_STORE.lstrip('/')}"
        headers = rest._create_capi_headers(username=username, password=password)
        _, content, resp = rest._http_request(url, "DELETE", headers=headers)
        return self.status_code(resp), content

    # ── Service role management ───────────────────────────────────────────────

    def get_service_roles(self, rest, service_name, username=None, password=None):
        """
        GET /settings/rbac/services/:name/roles — Full Admin only.

        Returns:
            tuple: (status_code, content)
        """
        try:
            api = self._cs_api(rest, username=username, password=password)
            _, content, response = api.get_service_roles(service_name)
            return self.status_code(response), content
        except Exception as _cs_err:
            if self.log:
                self.log.debug(f"CredentialStoreAPI path unavailable, using fallback: {_cs_err}")
        encoded_service = self._encode_path_segment(service_name)
        url = (
            f"{self._base_url(rest)}/{ENDPOINT_RBAC_SERVICES.lstrip('/')}/"
            f"{encoded_service}/roles"
        )
        headers = rest._create_capi_headers(username=username, password=password)
        _, content, resp = rest._http_request(url, "GET", headers=headers)
        return self.status_code(resp), content

    def put_service_roles(self, rest, service_name, roles, username=None, password=None):
        """
        PUT /settings/rbac/services/:name/roles — Full Admin only.

        Args:
            roles: str or list (e.g. ["credential_consumer[cred-id]"])

        Returns:
            tuple: (status_code, content)
        """
        try:
            api = self._cs_api(rest, username=username, password=password)
            _, content, response = api.put_service_roles(service_name, roles)
            return self.status_code(response), content
        except Exception as _cs_err:
            if self.log:
                self.log.debug(f"CredentialStoreAPI path unavailable, using fallback: {_cs_err}")
        encoded_service = self._encode_path_segment(service_name)
        url = (
            f"{self._base_url(rest)}/{ENDPOINT_RBAC_SERVICES.lstrip('/')}/"
            f"{encoded_service}/roles"
        )
        roles_str = ",".join(roles) if isinstance(roles, list) else roles
        body = urllib.parse.urlencode({"roles": roles_str})
        headers = rest._create_capi_headers(
            username=username,
            password=password,
            contentType="application/x-www-form-urlencoded",
        )
        _, content, resp = rest._http_request(url, "PUT", body, headers=headers)
        return self.status_code(resp), content

    def delete_service_roles(self, rest, service_name, username=None, password=None):
        """
        DELETE /settings/rbac/services/:name/roles — Full Admin only.

        Returns:
            tuple: (status_code, content)
        """
        try:
            api = self._cs_api(rest, username=username, password=password)
            _, content, response = api.delete_service_roles(service_name)
            return self.status_code(response), content
        except Exception as _cs_err:
            if self.log:
                self.log.debug(f"CredentialStoreAPI path unavailable, using fallback: {_cs_err}")
        encoded_service = self._encode_path_segment(service_name)
        url = (
            f"{self._base_url(rest)}/{ENDPOINT_RBAC_SERVICES.lstrip('/')}/"
            f"{encoded_service}/roles"
        )
        headers = rest._create_capi_headers(username=username, password=password)
        _, content, resp = rest._http_request(url, "DELETE", headers=headers)
        return self.status_code(resp), content

    # ── Local user management ─────────────────────────────────────────────────

    def create_local_user(self, rest, username, password, roles):
        """
        PUT /settings/rbac/users/local/:username — create or replace a local user.

        Args:
            roles: str or list of role strings

        Returns:
            tuple: (status_code, content)
        """
        encoded_user = self._encode_path_segment(username)
        url = f"{self._base_url(rest)}/{ENDPOINT_RBAC_USERS.lstrip('/')}/local/{encoded_user}"
        roles_str = ",".join(roles) if isinstance(roles, list) else roles
        body = urllib.parse.urlencode({"password": password, "roles": roles_str})
        _, content, resp = rest._http_request(url, "PUT", body)
        return self.status_code(resp), content

    def delete_local_user(self, rest, username):
        """
        DELETE /settings/rbac/users/local/:username.

        Returns:
            tuple: (status_code, content)
        """
        encoded_user = self._encode_path_segment(username)
        url = f"{self._base_url(rest)}/{ENDPOINT_RBAC_USERS.lstrip('/')}/local/{encoded_user}"
        _, content, resp = rest._http_request(url, "DELETE")
        return self.status_code(resp), content

    def grant_consume_to_local_user(self, rest, username, cred_id_pattern):
        """
        Append credential_consumer[<cred_id_pattern>] to a local user's roles.

        Performs GET then PUT to preserve existing roles.

        Returns:
            tuple: (status_code, content)
        """
        encoded_user = self._encode_path_segment(username)
        url = f"{self._base_url(rest)}/{ENDPOINT_RBAC_USERS.lstrip('/')}/local/{encoded_user}"
        ok, existing_content, _ = rest._http_request(url, "GET")
        existing_roles = []
        if ok and existing_content:
            try:
                user_data = (
                    json.loads(existing_content)
                    if isinstance(existing_content, str)
                    else existing_content
                )
                existing_roles = [
                    r.get("role", "")
                    for r in user_data.get("roles", [])
                    if isinstance(r, dict) and r.get("role")
                ]
            except Exception:
                pass
        new_role = f"credential_consumer[{cred_id_pattern}]"
        if new_role not in existing_roles:
            existing_roles.append(new_role)
        roles_str = ",".join(existing_roles)
        body = urllib.parse.urlencode({"roles": roles_str})
        _, content, resp = rest._http_request(url, "PUT", body)
        return self.status_code(resp), content

    # ── Consume path (cbauth) ─────────────────────────────────────────────────

    def consume_credential(
        self,
        base_url,
        cred_id,
        service_name,
        service_password,
        on_behalf_user=None,
        on_behalf_domain=None,
        tls_verify=False,
        timeout=10,
    ):
        """
        Simulate a cbauth credential consume request.

        Pattern A (end-user path):
            on_behalf_user = "alice", on_behalf_domain = "local"

        Pattern B (service-identity path):
            on_behalf_user = "@cbq-engine", on_behalf_domain = "admin"

        The service_name is used to build the Basic Auth username: @<service_name>.
        Pass the service's cbauth password via service_password — available as a
        test parameter (cbq_engine_password) or environment variable
        (CBAUTH_CBQ_ENGINE_PASSWORD).

        Args:
            base_url: Cluster management URL, e.g. "http://192.168.1.1:8091"
            cred_id: Credential ID to consume
            service_name: Service name without @-prefix (e.g. "cbq-engine")
            service_password: cbauth password for the service identity
            on_behalf_user: User to consume on behalf of (Pattern A) or service itself (Pattern B)
            on_behalf_domain: Domain of on_behalf_user
            tls_verify: Whether to verify TLS certificates
            timeout: Request timeout in seconds

        Returns:
            tuple: (status_code, body)
                body is parsed JSON dict on 200, raw string otherwise
        """
        service_user = (
            service_name
            if service_name.startswith("@")
            else f"@{service_name}"
        )
        encoded_id = self._encode_path_segment(cred_id)
        url = f"{base_url.rstrip('/')}{ENDPOINT_CBAUTH_CREDENTIAL}/{encoded_id}"
        params = {}
        if on_behalf_user:
            params["user"] = on_behalf_user
        if on_behalf_domain:
            params["domain"] = on_behalf_domain

        resp = requests.get(
            url,
            params=params,
            auth=(service_user, service_password),
            verify=tls_verify,
            timeout=timeout,
        )
        try:
            body = resp.json()
        except Exception:
            body = resp.text
        if self.log:
            self.log.info(
                f"consume_credential: cred_id={cred_id} service={service_user} "
                f"user={on_behalf_user}/{on_behalf_domain} → status={resp.status_code}"
            )
        return resp.status_code, body

    # ── Redaction / secret-safety assertions ─────────────────────────────────

    def assert_secrets_redacted(self, content, cred_type, known_secret_values=None):
        """
        Assert that sensitive fields are NOT present as plaintext in an admin response.

        Two checks:
        1. If known_secret_values are given, none may appear verbatim in the stringified
           content.
        2. For each sensitive field of cred_type, the field value (if present) must not
           look like a real secret (i.e. it should be empty, masked, or absent).

        Args:
            content: Raw response content (str, bytes, or dict)
            cred_type: One of SENSITIVE_FIELDS_BY_TYPE keys
            known_secret_values: Optional iterable of known plaintext secret strings

        Returns:
            dict: Parsed content (for further inspection)
        """
        content_str = (
            content.decode("utf-8", "replace")
            if isinstance(content, (bytes, bytearray))
            else str(content)
        )

        if known_secret_values:
            for secret in known_secret_values:
                if secret and secret in content_str:
                    raise AssertionError(
                        f"Raw secret value unexpectedly visible in admin response "
                        f"(type={cred_type}). Secret must be redacted. "
                        f"content_preview={content_str[:400]}"
                    )

        parsed = self.parse_content(content)
        if isinstance(parsed, dict):
            fields = parsed.get("fields", {})
            for field in SENSITIVE_FIELDS_BY_TYPE.get(cred_type, set()):
                value = fields.get(field)
                if value not in (None, "", "********"):
                    raise AssertionError(
                        f"Sensitive field '{field}' not redacted: {value!r}"
                    )
        return parsed

    def assert_no_raw_secret(self, content, secret_value, context=""):
        """Assert that secret_value does not appear verbatim anywhere in content."""
        if not secret_value:
            return
        content_str = (
            content.decode("utf-8", "replace")
            if isinstance(content, (bytes, bytearray))
            else str(content)
        )
        assert secret_value not in content_str, (
            f"Raw secret value unexpectedly found in response. "
            f"context={context} content_preview={content_str[:400]}"
        )

    def get_warnings(self, content):
        """Extract the 'warnings' list from a parsed credential store response."""
        parsed = self.parse_content(content)
        if isinstance(parsed, dict):
            return parsed.get("warnings", [])
        if isinstance(parsed, list):
            for item in parsed:
                if isinstance(item, dict) and "warnings" in item:
                    return item["warnings"]
        return []

    def get_credential_ids_from_list(self, content):
        """Extract credential IDs from a list-credentials response body."""
        parsed = self.parse_content(content)
        if isinstance(parsed, list):
            return [item.get("id") for item in parsed if isinstance(item, dict)]
        if isinstance(parsed, dict):
            items = parsed.get("credentials", parsed.get("items", []))
            if isinstance(items, list):
                return [item.get("id") for item in items if isinstance(item, dict)]
        return []

    def get_error_code(self, content):
        """Extract error code string from a response body."""
        parsed = self.parse_content(content)
        if isinstance(parsed, dict):
            return parsed.get("error") or parsed.get("errorCode") or parsed.get("code")
        return None

    def get_cbauth_error_code(self, body):
        """
        Extract the cbauth error code from a consume-path 403 response body.

        Handles both flat and nested error formats:
          {"error": "INSUFFICIENT_PERMISSIONS"}
          {"error": {"code": "INSUFFICIENT_PERMISSIONS", "reason": "..."}}
          {"errorCode": "SERVICE_GUARDRAIL_BLOCKED"}

        Returns:
            str: error code string, or None if not parseable
        """
        if isinstance(body, dict):
            error = body.get("error")
            if isinstance(error, dict):
                return error.get("code")
            if isinstance(error, str):
                return error
            return body.get("errorCode") or body.get("code")
        return self.get_error_code(body)

    # ── Status-code assertion helpers ─────────────────────────────────────────

    def assert_success_status(self, status, message):
        """Assert 2xx status code, raising AssertionError with message otherwise."""
        try:
            code = int(status)
        except (TypeError, ValueError):
            raise AssertionError(
                f"{message} — got non-numeric status: {status!r}"
            )
        assert 200 <= code < 300, f"{message} — got status={status}"

    def assert_error_status(self, status, message, expected_codes=None):
        """
        Assert error status code.

        Args:
            expected_codes: Optional list of acceptable status codes. Defaults to any 4xx/5xx.
        """
        try:
            code = int(status)
        except (TypeError, ValueError):
            raise AssertionError(
                f"{message} — got non-numeric status: {status!r}"
            )
        if expected_codes:
            assert code in expected_codes, (
                f"{message} — expected one of {expected_codes}, got {status}"
            )
        else:
            assert 400 <= code < 600, (
                f"{message} — expected 4xx/5xx error, got {status}"
            )

    @staticmethod
    def _redact_for_log(obj):
        """Recursively mask common secret keys before logging."""
        secret_keys = {
            "password", "secret", "token", "privatekey", "passphrase",
            "secretaccesskey", "sessiontoken", "accountkey", "clientsecret",
            "sharedaccesssignature", "jsoncredentials",
        }
        if isinstance(obj, dict):
            return {
                key: (
                    "********"
                    if key.lower() in secret_keys
                    else CredentialStoreUtils._redact_for_log(value)
                )
                for key, value in obj.items()
            }
        if isinstance(obj, list):
            return [CredentialStoreUtils._redact_for_log(item) for item in obj]
        return obj

    def log_safe(self, label, content):
        """Log a response label + safe preview (never logs known secret values)."""
        if self.log:
            preview = self._redact_for_log(self.parse_content(content))
            preview_str = str(preview)[:300] if preview is not None else "(empty)"
            self.log.info(f"{label}: {preview_str}")