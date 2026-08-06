import contextlib
import json
import time
from urllib.parse import quote

import requests
from cb_server_rest_util.connection import CBRestConnection

# ── Endpoint constants ──────────────────────────────────────────────────────
ENDPOINT_CRL_SETTINGS = "/settings/crl"
ENDPOINT_CRL_FILES = "/settings/crl/files"
ENDPOINT_CRL_DIAGNOSTICS_STATUS = "/settings/crl/diagnostics/status"
ENDPOINT_CRL_DIAGNOSTICS_VALIDATE = "/settings/crl/diagnostics/validate"
ENDPOINT_RELOAD_CRL = "/node/controller/reloadCrl"
ENDPOINT_CBAUTH_CRLS_VALIDATE = "/_cbauth/crlsValidate"


class CRLAPI(CBRestConnection):
    """
    CRL (Certificate Revocation List) admin/diagnostic REST API for
    Couchbase Server Enterprise, Totoro+ compat (MB-32989/MB-72045).

    Wraps /settings/crl, /settings/crl/files, /settings/crl/diagnostics/*,
    /node/controller/reloadCrl and the internal /_cbauth/crlsValidate endpoint.

    Used by CRLUtils (crl_utils.py) via the _crl_api() shim — same pattern as
    JWTAPI/CredentialStoreAPI. upload_crl_file() does not go through
    CBRestConnection.request() since that method only sends a raw body, not
    multipart/form-data — it builds its own requests call instead.
    """

    def __init__(self):
        super().__init__()

    # ── Settings ─────────────────────────────────────────────────────────────

    def get_crl_settings(self):
        """
        GET /settings/crl — current cluster-wide CRL config (defaults merged in).

        Returns:
            tuple: (status_bool, content, response)
        """
        api = f"{self.base_url}{ENDPOINT_CRL_SETTINGS}"
        return self.request(api, self.GET)

    def post_crl_settings(self, payload):
        """
        POST /settings/crl — partial update (SET semantics, omitted fields
        keep their existing value). Returns the full merged config on success.

        Args:
            payload: dict, any subset of policyPerScope/directory/
                dirPollIntervalMs/checkIntermediateCerts/urls/urlPollIntervalMs

        Returns:
            tuple: (status_bool, content, response)
        """
        api = f"{self.base_url}{ENDPOINT_CRL_SETTINGS}"
        headers = self.get_headers_for_content_type_json()
        body = json.dumps(payload)
        return self.request(api, self.POST, body, headers=headers)

    # ── File lifecycle ──────────────────────────────────────────────────────

    def get_crl_files(self):
        """
        GET /settings/crl/files — list uploaded CRL file metadata.

        Returns:
            tuple: (status_bool, content, response)
        """
        api = f"{self.base_url}{ENDPOINT_CRL_FILES}"
        return self.request(api, self.GET)

    def upload_crl_file(self, filename, content_bytes, timeout=300):
        """
        POST /settings/crl/files — upload one CRL file, multipart/form-data.

        Args:
            filename: str, 1-255 chars of [a-zA-Z0-9._-], not "." or ".."
            content_bytes: bytes, PEM or DER CRL content
            timeout: seconds; also the retry deadline for transient
                connection errors (mirrors CBRestConnection.request()).

        Returns:
            tuple: (status_bool, content, response)
        """
        api = f"{self.base_url}{ENDPOINT_CRL_FILES}"
        files = {"file": (filename, content_bytes, "application/pkix-crl")}
        end_time = time.time() + timeout
        last_err = None
        while time.time() <= end_time:
            try:
                response = requests.post(
                    api, files=files, auth=(self.username, self.password),
                    verify=False, timeout=timeout,
                )
                status = response.ok
                content = response.content
                with contextlib.suppress(ValueError):
                    content = response.json()
                return status, content, response
            except requests.exceptions.RequestException as err:
                self.log.error(f"Error uploading CRL file {filename}: {err}")
                last_err = err
                time.sleep(3)
        raise Exception(f"ServerUnavailableException - {self.ip}") from last_err

    def delete_crl_file(self, filename):
        """
        DELETE /settings/crl/files/:filename.

        Returns:
            tuple: (status_bool, content, response)
        """
        api = f"{self.base_url}{ENDPOINT_CRL_FILES}/{quote(filename, safe='')}"
        return self.request(api, self.DELETE)

    # ── Diagnostics ──────────────────────────────────────────────────────────

    def get_diagnostics_status(self, nodes=None):
        """
        GET /settings/crl/diagnostics/status[?nodes=host1,host2] — per-node
        CRL cache/file status. Omit nodes for all active nodes.

        Args:
            nodes: optional list of "host:port" strings

        Returns:
            tuple: (status_bool, content, response)
        """
        api = f"{self.base_url}{ENDPOINT_CRL_DIAGNOSTICS_STATUS}"
        if nodes:
            api = f"{api}?nodes={quote(','.join(nodes), safe='')}"
        return self.request(api, self.GET)

    def post_diagnostics_status(self, nodes=None):
        """
        POST /settings/crl/diagnostics/status — same as GET, node list as a
        JSON array in the body (use when the list is too long for a query string).

        Args:
            nodes: optional list of "host:port" strings

        Returns:
            tuple: (status_bool, content, response)
        """
        api = f"{self.base_url}{ENDPOINT_CRL_DIAGNOSTICS_STATUS}"
        headers = self.get_headers_for_content_type_json()
        body = json.dumps({"nodes": nodes} if nodes else {})
        return self.request(api, self.POST, body, headers=headers)

    def post_diagnostics_validate(self, policy=None, certs=None):
        """
        POST /settings/crl/diagnostics/validate — admin diagnostic endpoint,
        bypasses the configured policy in favor of a caller-supplied test policy.

        Args:
            policy: "Permissive" or "Require" (defaults server-side to "Require";
                "Disabled" is rejected — nothing to test)
            certs: optional list of PEM strings or base64-encoded DER strings;
                omit for cluster-cert mode (checks every node's own cert chains)

        Returns:
            tuple: (status_bool, content, response)
        """
        api = f"{self.base_url}{ENDPOINT_CRL_DIAGNOSTICS_VALIDATE}"
        headers = self.get_headers_for_content_type_json()
        payload = {}
        if policy is not None:
            payload["policy"] = policy
        if certs is not None:
            payload["certs"] = certs
        body = json.dumps(payload)
        return self.request(api, self.POST, body, headers=headers)

    # ── Reload ───────────────────────────────────────────────────────────────

    def reload_crl(self):
        """
        POST /node/controller/reloadCrl — force immediate reload on the local
        node only (not cluster-wide). No body.

        Returns:
            tuple: (status_bool, content, response)
        """
        api = f"{self.base_url}{ENDPOINT_RELOAD_CRL}"
        return self.request(api, self.POST)

    # ── Internal cbauth integration ─────────────────────────────────────────

    def cbauth_crls_validate(self, certs, scope):
        """
        POST /_cbauth/crlsValidate — internal endpoint used by cbauth-registered
        GO services (query, analytics, indexer, goxdcr). Validates against the
        currently configured policy (unlike diagnostics_validate, which bypasses
        it). Requires {[admin, internal], all} — Administrator creds satisfy it.

        Args:
            certs: list of base64-encoded DER strings, leaf first, chain after
            scope: "clientAuth" or "nodeToNode"

        Returns:
            tuple: (status_bool, content, response)
        """
        api = f"{self.base_url}{ENDPOINT_CBAUTH_CRLS_VALIDATE}"
        headers = self.get_headers_for_content_type_json()
        body = json.dumps({"certs": certs, "scope": scope})
        return self.request(api, self.POST, body, headers=headers)
