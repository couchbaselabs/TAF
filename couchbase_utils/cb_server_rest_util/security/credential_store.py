import json
from urllib.parse import quote, quote_from_bytes, urlencode

from cb_server_rest_util.connection import CBRestConnection

# ── Endpoint constants ──────────────────────────────────────────────────────
ENDPOINT_CREDENTIALS = "/settings/credentials"
ENDPOINT_CREDENTIAL_STORE = "/settings/credentialStore"
ENDPOINT_RBAC_SERVICES = "/settings/rbac/services"
ENDPOINT_CBAUTH_CREDENTIAL = "/_cbauth/getCredential"
ENDPOINT_RBAC_USERS = "/settings/rbac/users"

# ── cbauth consume error codes (returned in response body) ──────────────────
ERROR_INSUFFICIENT_PERMISSIONS = "INSUFFICIENT_PERMISSIONS"
ERROR_SERVICE_GUARDRAIL_BLOCKED = "SERVICE_GUARDRAIL_BLOCKED"
ERROR_CREDENTIAL_EXPIRED = "CREDENTIAL_EXPIRED"
ERROR_UNSUPPORTED_SCHEMA_VERSION = "UNSUPPORTED_SCHEMA_VERSION"

# ── Allowed service names for service-role management ──────────────────────
ALLOWED_SERVICES = ("n1ql", "backup", "index", "xdcr", "fts", "eventing", "cbas")


class CredentialStoreAPI(CBRestConnection):
    """
    Credential Store admin REST API for Couchbase Server 8.1+ Enterprise.

    Wraps /settings/credentials, /settings/credentialStore, and
    /settings/rbac/services endpoints.

    Used directly by CredentialStoreUtils (credential_store_utils.py) via the
    _cs_api() shim — the same pattern as JWTAPI used by JWTUtils.
    The cbauth consume path (/_cbauth/getCredential) is exercised by
    CredentialStoreUtils.consume_credential() using requests directly because it
    requires service-identity Basic Auth rather than admin credentials.
    """

    def __init__(self):
        super(CredentialStoreAPI, self).__init__()

    # ── Admin credential CRUD ─────────────────────────────────────────────────

    @staticmethod
    def _encode_id(cred_id):
        """URL-encode a credential ID for safe embedding in path segments."""
        if cred_id is None or (isinstance(cred_id, str) and cred_id == ""):
            raise ValueError("cred_id must be a non-empty string")
        if isinstance(cred_id, (bytes, bytearray)):
            if not cred_id:
                raise ValueError("cred_id must be non-empty bytes")
            return quote_from_bytes(bytes(cred_id), safe="")
        return quote(str(cred_id), safe="")

    def create_credential(self, cred_id, payload):
        """
        POST /settings/credentials/:id — create a new credential.

        Args:
            cred_id: Credential ID string (max 128 ASCII chars, no spaces)
            payload: dict with keys: type, fields, and optional meta/guardrails

        Returns:
            tuple: (status_bool, content, response)
        """
        api = f"{self.base_url}{ENDPOINT_CREDENTIALS}/{self._encode_id(cred_id)}"
        headers = self.get_headers_for_content_type_json()
        body = json.dumps(payload)
        return self.request(api, self.POST, body, headers=headers)

    def get_credential(self, cred_id):
        """
        GET /settings/credentials/:id — read one credential (secrets redacted).

        Returns:
            tuple: (status_bool, content, response)
        """
        api = f"{self.base_url}{ENDPOINT_CREDENTIALS}/{self._encode_id(cred_id)}"
        return self.request(api, self.GET)

    def list_credentials(self, prefix=None):
        """
        GET /settings/credentials[?prefix=...] — list all or prefix-filtered credentials.

        Args:
            prefix: Optional string to filter by credential ID prefix

        Returns:
            tuple: (status_bool, content, response)
        """
        api = f"{self.base_url}{ENDPOINT_CREDENTIALS}"
        if prefix:
            api = f"{api}?prefix={quote(prefix, safe='')}"
        return self.request(api, self.GET)

    def update_credential(self, cred_id, payload):
        """
        PUT /settings/credentials/:id — full replace (type field is immutable).

        Returns:
            tuple: (status_bool, content, response)
        """
        api = f"{self.base_url}{ENDPOINT_CREDENTIALS}/{self._encode_id(cred_id)}"
        headers = self.get_headers_for_content_type_json()
        body = json.dumps(payload)
        return self.request(api, self.PUT, body, headers=headers)

    def delete_credential(self, cred_id):
        """
        DELETE /settings/credentials/:id.

        Returns:
            tuple: (status_bool, content, response)
        """
        api = f"{self.base_url}{ENDPOINT_CREDENTIALS}/{self._encode_id(cred_id)}"
        return self.request(api, self.DELETE)

    # ── Store settings ────────────────────────────────────────────────────────

    def get_credential_store_settings(self):
        """
        GET /settings/credentialStore — read override settings and warnings.

        Returns:
            tuple: (status_bool, content, response)
        """
        api = f"{self.base_url}{ENDPOINT_CREDENTIAL_STORE}"
        return self.request(api, self.GET)

    def put_credential_store_settings(self, settings):
        """
        PUT /settings/credentialStore — update override settings.

        Both configEncryptionOverride and n2nEncryptionOverride are required.

        Args:
            settings: dict with keys configEncryptionOverride, n2nEncryptionOverride

        Returns:
            tuple: (status_bool, content, response)
        """
        api = f"{self.base_url}{ENDPOINT_CREDENTIAL_STORE}"
        headers = self.get_headers_for_content_type_json()
        body = json.dumps(settings)
        return self.request(api, self.PUT, body, headers=headers)

    def delete_credential_store_settings(self):
        """
        DELETE /settings/credentialStore — reset to defaults.

        Returns:
            tuple: (status_bool, content, response)
        """
        api = f"{self.base_url}{ENDPOINT_CREDENTIAL_STORE}"
        return self.request(api, self.DELETE)

    # ── Service role management ───────────────────────────────────────────────

    def get_service_roles(self, service_name):
        """
        GET /settings/rbac/services/:name/roles — Full Admin only.

        Args:
            service_name: One of ALLOWED_SERVICES

        Returns:
            tuple: (status_bool, content, response)
        """
        api = f"{self.base_url}{ENDPOINT_RBAC_SERVICES}/{quote(service_name, safe='')}/roles"
        return self.request(api, self.GET)

    def put_service_roles(self, service_name, roles):
        """
        PUT /settings/rbac/services/:name/roles — Full Admin only.

        Args:
            service_name: One of ALLOWED_SERVICES
            roles: str or list of role strings (e.g. ["credential_consumer[cred-id]"])

        Returns:
            tuple: (status_bool, content, response)
        """
        api = f"{self.base_url}{ENDPOINT_RBAC_SERVICES}/{quote(service_name, safe='')}/roles"
        roles_str = ",".join(roles) if isinstance(roles, list) else roles
        body = urlencode({"roles": roles_str})
        return self.request(api, self.PUT, body)

    def delete_service_roles(self, service_name):
        """
        DELETE /settings/rbac/services/:name/roles — Full Admin only.

        Args:
            service_name: One of ALLOWED_SERVICES

        Returns:
            tuple: (status_bool, content, response)
        """
        api = f"{self.base_url}{ENDPOINT_RBAC_SERVICES}/{quote(service_name, safe='')}/roles"
        return self.request(api, self.DELETE)
