# -*- coding: utf-8 -*-
"""
Unified Control Plane API Client
Raw HTTP client for hitting UCP endpoints.
Inherits from TAF's RestConnection to reuse _http_request().
Returns raw responses without wrapper logic.
"""
import json
import urllib
from Rest_Connection import RestConnection as BaseRestConnection
from global_vars import logger
class UnifiedControlPlaneClient(BaseRestConnection):
    """
    Raw API client for Unified Control Plane endpoints.
    Inherits from TAF's RestConnection to reuse _http_request().
    All methods hit endpoints and return raw (status, content, response) tuples.
    """

    def __new__(cls, *args, **kwargs):
        # Bypass RestConnection.__new__ which uses xrange (Python 2 only)
        return object.__new__(cls)

    def __init__(self, portal, timeout=300):
        """
        Initialize UCP Client.
        Args:
            portal: LighthousePortal object with ip, port, username,
                    password for the UCP service.
            timeout: Request timeout in seconds
        """
        self.log = logger.get("infra")
        self.test_log = logger.get("test")
        # Store connection details from portal object
        self.ip = portal.ip
        self.port = portal.port
        self.username = portal.username
        self.password = portal.password
        self.type = "columnar"
        # Build baseUrl directly - do NOT let RestConnection
        # mangle the port with CB TLS logic.
        # UCP has its own port that is independent of Couchbase.
        scheme = "https"
        self.baseUrl = "{0}://{1}:{2}/".format(scheme, self.ip, self.port)
        # Session cookie storage - UCP uses cookie-based sessions
        self._session_cookie = None

    def _http_request(self, api, method='GET', params='', headers=None,
                      timeout=300):
        """
        Override parent to always use requests-based _urllib_request.
        UCP always runs over HTTPS, so we bypass the httplib2 path
        which depends on CbServer.use_https being True.
        """
        if not headers:
            headers = self._json_headers()
        status, content, response = self._urllib_request(
            api, method=method, params=params, headers=headers,
            timeout=timeout, verify=False)
        return status, content, response

    def _json_headers(self):
        """
        Return headers for UCP API requests.
        - Always: Content-Type: application/json
        - If session cookie exists: Cookie header
        - NO Basic auth - UCP uses session-based auth only.
        """
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Connection': 'close'
        }
        if self._session_cookie:
            headers['Cookie'] = self._session_cookie
        return headers
    # ==================== Session APIs ====================
    def session_login(self, username, password):
        """POST /api/v1/session/login"""
        api = self.baseUrl + 'api/v1/session/login'
        body = json.dumps({'username': username, 'password': password})
        status, content, header = self._http_request(
            api, 'POST', body, headers=self._json_headers())
        # Capture session cookie from response headers
        if status and header is not None:
            cookie = header.headers.get('Set-Cookie', '')
            if cookie:
                self._session_cookie = cookie.split(';')[0]
        return status, content, header
    def session_logout(self):
        """POST /api/v1/session/logout"""
        api = self.baseUrl + 'api/v1/session/logout'
        status, content, header = self._http_request(
            api, 'POST', headers=self._json_headers())
        if status:
            self._session_cookie = None
        return status, content, header
    def session_me(self):
        """GET /api/v1/session/me"""
        api = self.baseUrl + 'api/v1/session/me'
        status, content, header = self._http_request(
            api, 'GET', headers=self._json_headers())
        return status, content, header
    def change_password(self, username, current_password, new_password):
        """POST /api/v1/session/change-password

        Session-less call (identifies the user by username in the body),
        used to swap an admin-set temporary password for a usable one.
        An admin-set password (create or reset) is temporary and logging
        in with it returns 401 password_expired -- this must be called
        before the first real login.
        """
        api = self.baseUrl + 'api/v1/session/change-password'
        body = json.dumps({
            'username': username,
            'currentPassword': current_password,
            'newPassword': new_password
        })
        status, content, header = self._http_request(
            api, 'POST', body, headers=self._json_headers())
        return status, content, header
    # ==================== User APIs ====================
    def list_users(self, offset=None, limit=None, enabled=None):
        """GET /api/v1/users"""
        api = self.baseUrl + 'api/v1/users'
        params = {}
        if offset is not None:
            params['offset'] = offset
        if limit is not None:
            params['limit'] = limit
        if enabled is not None:
            params['enabled'] = 'true' if enabled else 'false'
        if params:
            api += '?' + urllib.urlencode(params)
        status, content, header = self._http_request(
            api, 'GET', headers=self._json_headers())
        return status, content, header
    def create_user(self, user_id, roles, enabled=None,
                    auth_type=None, password=None):
        """POST /api/v1/users

        For a local user pass auth_type='local' plus a (temporary) password;
        the portal auto-provisions the backing CBS local user. The password
        set here is TEMPORARY -- a login with it returns 401 password_expired,
        so it must be swapped via change_password() before first login.
        """
        api = self.baseUrl + 'api/v1/users'
        body_dict = {'userId': user_id, 'roles': roles}
        if auth_type is not None:
            body_dict['authType'] = auth_type
        if password is not None:
            body_dict['password'] = password
        if enabled is not None:
            body_dict['enabled'] = enabled
        body = json.dumps(body_dict)
        status, content, header = self._http_request(
            api, 'POST', body, headers=self._json_headers())
        return status, content, header
    def get_user(self, user_id):
        """GET /api/v1/users/{userId}"""
        api = self.baseUrl + 'api/v1/users/%s' % urllib.quote(user_id)
        status, content, header = self._http_request(
            api, 'GET', headers=self._json_headers())
        return status, content, header
    def update_user(self, user_id, etag, enabled=None, roles=None):
        """PUT /api/v1/users/{userId}"""
        api = self.baseUrl + 'api/v1/users/%s' % urllib.quote(user_id)
        body_dict = {}
        if enabled is not None:
            body_dict['enabled'] = enabled
        if roles is not None:
            body_dict['roles'] = roles
        body = json.dumps(body_dict)
        headers = self._json_headers()
        headers['If-Match'] = etag
        status, content, header = self._http_request(api, 'PUT', body,
                                                     headers=headers)
        return status, content, header
    def delete_user(self, user_id):
        """DELETE /api/v1/users/{userId}"""
        api = self.baseUrl + 'api/v1/users/%s' % urllib.quote(user_id)
        status, content, header = self._http_request(
            api, 'DELETE', headers=self._json_headers())
        return status, content, header
    # ==================== Ingest APIs ====================
    def ingest_telemetry(self, telemetry_data):
        """POST /api/v1/ingest/telemetry"""
        api = self.baseUrl + 'api/v1/ingest/telemetry'
        body = json.dumps(telemetry_data)
        status, content, header = self._http_request(
            api, 'POST', body, headers=self._json_headers())
        return status, content, header
    def ingest_health(self):
        """GET /api/v1/ingest/health"""
        api = self.baseUrl + 'api/v1/ingest/health'
        status, content, header = self._http_request(
            api, 'GET', headers=self._json_headers())
        return status, content, header
    # ==================== Cluster APIs ====================
    def list_clusters(self, name=None, offset=None, limit=None):
        """GET /api/v1/clusters"""
        api = self.baseUrl + 'api/v1/clusters'
        params = {}
        if name is not None:
            params['name'] = name
        if offset is not None:
            params['offset'] = offset
        if limit is not None:
            params['limit'] = limit
        if params:
            api += '?' + urllib.urlencode(params)
        status, content, header = self._http_request(
            api, 'GET', headers=self._json_headers())
        return status, content, header
    def get_cluster(self, cluster_uuid):
        """GET /api/v1/clusters/{clusterUuid}"""
        api = self.baseUrl + 'api/v1/clusters/%s' % urllib.quote(cluster_uuid)
        status, content, header = self._http_request(
            api, 'GET', headers=self._json_headers())
        return status, content, header
    def get_cluster_history(self, cluster_uuid, from_timestamp=None,
                           to_timestamp=None, offset=None, limit=None):
        """GET /api/v1/clusters/{clusterUuid}/history"""
        api = self.baseUrl + 'api/v1/clusters/%s/history' % urllib.quote(cluster_uuid)
        params = {}
        if from_timestamp is not None:
            params['from'] = from_timestamp
        if to_timestamp is not None:
            params['to'] = to_timestamp
        if offset is not None:
            params['offset'] = offset
        if limit is not None:
            params['limit'] = limit
        if params:
            api += '?' + urllib.urlencode(params)
        status, content, header = self._http_request(
            api, 'GET', headers=self._json_headers())
        return status, content, header
    def update_cluster(self, cluster_uuid, etag, classification=None,
                      business_unit=None, name=None, description=None):
        """PUT /api/v1/clusters/{clusterUuid}"""
        api = self.baseUrl + 'api/v1/clusters/%s' % urllib.quote(cluster_uuid)
        body_dict = {}
        if classification is not None:
            body_dict['classification'] = classification
        if business_unit is not None:
            body_dict['businessUnit'] = business_unit
        if name is not None:
            body_dict['name'] = name
        if description is not None:
            body_dict['description'] = description
        body = json.dumps(body_dict)
        headers = self._json_headers()
        headers['If-Match'] = etag
        status, content, header = self._http_request(api, 'PUT', body,
                                                     headers=headers)
        return status, content, header
    # ==================== Entitlement APIs ====================
    def get_entitlements(self):
        """GET /api/v1/entitlements"""
        api = self.baseUrl + 'api/v1/entitlements'
        status, content, header = self._http_request(
            api, 'GET', headers=self._json_headers())
        return status, content, header
    def update_entitlements(self, etag, subscriptions=None):
        """PUT /api/v1/entitlements"""
        api = self.baseUrl + 'api/v1/entitlements'
        body_dict = {}
        if subscriptions is not None:
            body_dict['subscriptions'] = subscriptions
        body = json.dumps(body_dict)
        headers = self._json_headers()
        headers['If-Match'] = etag
        status, content, header = self._http_request(api, 'PUT', body,
                                                     headers=headers)
        return status, content, header
    def upload_entitlements(self, document, etag,
                           filename='entitlements.bson',
                           part_content_type='application/octet-stream'):
        """PUT /api/v1/entitlements as multipart/form-data

        Live-probed 2026-08-18 against the QE lab portal:
          - PUT with a JSON body            -> 422 "cannot read multipart
            form: request Content-Type isn't multipart/form-data". This is
            why update_entitlements() below no longer works on this build.
          - PUT multipart, file part 'file' -> reaches the file decoder, so
            'file' is the field name the server wants.
          - POST /api/v1/entitlements/import and .../import/preview -> 404.
            Those paths do not exist here; do not reach for them.

        The accepted file ENCODING is still unknown: plain JSON, a '.bson'
        filename, application/octet-stream, a bare JSON array, gzipped JSON,
        real BSON and base64 JSON all return 400 "Failed to decode uploaded
        file.". Until the product tells us the format, this method reaches the
        decoder and stops there -- so it cannot yet be used to seed a SKU.

        Args:
            document: dict to serialize as the uploaded document, or an
                      already-encoded byte/str payload. Pass a pre-encoded
                      value once the real format is known.
            etag:     value for If-Match, required by this endpoint.
            filename: name advertised for the file part.
            part_content_type: Content-Type of the file part.

        Returns:
            Tuple (status, content, header)
        """
        api = self.baseUrl + 'api/v1/entitlements'
        if isinstance(document, (dict, list)):
            file_content = json.dumps(document)
        else:
            file_content = document
        boundary, body = self._build_multipart_body(
            'file', filename, file_content,
            part_content_type=part_content_type)
        headers = self._json_headers()
        headers['Content-Type'] = ('multipart/form-data; boundary=%s'
                                   % boundary)
        headers['If-Match'] = etag
        status, content, header = self._http_request(api, 'PUT', body,
                                                    headers=headers)
        return status, content, header

    @staticmethod
    def _build_multipart_body(field_name, filename, file_content,
                             part_content_type='application/json'):
        """
        Hand-build a single-part multipart/form-data body.

        RestConnection's _urllib_request passes the body straight through as
        requests' `data=`, so there is no files= hook to use; the body bytes
        and the boundary in the Content-Type header have to be produced here.
        The boundary is fixed rather than random: it keeps requests
        reproducible in logs, and a JSON document can never contain it.

        Returns:
            Tuple (boundary, body_string)
        """
        boundary = '----UCPFormBoundaryEntitlementImport'
        lines = [
            '--' + boundary,
            'Content-Disposition: form-data; name="%s"; filename="%s"'
            % (field_name, filename),
            'Content-Type: %s' % part_content_type,
            '',
            file_content,
            '--' + boundary + '--',
            '',
        ]
        return boundary, '\r\n'.join(lines)

    def get_entitlement_usage(self):
        """GET /api/v1/entitlements/usage"""
        api = self.baseUrl + 'api/v1/entitlements/usage'
        status, content, header = self._http_request(
            api, 'GET', headers=self._json_headers())
        return status, content, header
    # ==================== Reports APIs ====================
    def generate_usage_report(self, from_timestamp=None, to_timestamp=None,
                             format_type='pdf'):
        """GET /api/v1/reports/usage

        NOTE: confirmed live 2026-07-24 -- the portal currently rejects
        'from'/'to' as unknown query parameters (422) and only accepts
        'format' (required, must be 'pdf'). from_timestamp/to_timestamp
        are kept as optional args for forward-compatibility but are only
        sent if explicitly provided.
        """
        api = self.baseUrl + 'api/v1/reports/usage'
        params = {'format': format_type}
        if from_timestamp is not None:
            params['from'] = from_timestamp
        if to_timestamp is not None:
            params['to'] = to_timestamp
        api += '?' + urllib.urlencode(params)
        status, content, header = self._http_request(
            api, 'GET', headers=self._json_headers())
        return status, content, header
    # ==================== Audit APIs ====================
    def list_audit_events(self, offset=None, limit=None,
                         from_timestamp=None, to_timestamp=None,
                         actor=None, action=None):
        """GET /api/v1/audit"""
        api = self.baseUrl + 'api/v1/audit'
        params = {}
        if offset is not None:
            params['offset'] = offset
        if limit is not None:
            params['limit'] = limit
        if from_timestamp is not None:
            params['from'] = from_timestamp
        if to_timestamp is not None:
            params['to'] = to_timestamp
        if actor is not None:
            params['actor'] = actor
        if action is not None:
            params['action'] = action
        if params:
            api += '?' + urllib.urlencode(params)
        status, content, header = self._http_request(
            api, 'GET', headers=self._json_headers())
        return status, content, header
    def get_audit_event(self, audit_event_id):
        """GET /api/v1/audit/{auditEventId}"""
        api = self.baseUrl + 'api/v1/audit/%s' % urllib.quote(audit_event_id)
        status, content, header = self._http_request(
            api, 'GET', headers=self._json_headers())
        return status, content, header
    # ==================== Config APIs ====================
    def get_config(self):
        """GET /api/v1/config"""
        api = self.baseUrl + 'api/v1/config'
        status, content, header = self._http_request(
            api, 'GET', headers=self._json_headers())
        return status, content, header
    def update_config(self, etag, telemetry_retention_days=None,
                     session_idle_timeout_minutes=None,
                     session_absolute_timeout_minutes=None,
                     global_rate_limit_per_sec=None,
                     expensive_rate_limit_per_sec=None):
        """PUT /api/v1/config

        NOTE: the portal validates this as a full replacement, not a
        partial update -- telemetryRetentionDays, sessionIdleTimeoutMinutes,
        sessionAbsoluteTimeoutMinutes, globalRateLimitPerSec AND
        expensiveRateLimitPerSec must ALL be present in the body or the
        portal rejects it with 422 ("expected required property ... to be
        present"). Confirmed live 2026-07-24. Callers must fetch the
        current config first and pass every field (changed or not).
        """
        api = self.baseUrl + 'api/v1/config'
        body_dict = {}
        if telemetry_retention_days is not None:
            body_dict['telemetryRetentionDays'] = telemetry_retention_days
        if session_idle_timeout_minutes is not None:
            body_dict['sessionIdleTimeoutMinutes'] = session_idle_timeout_minutes
        if session_absolute_timeout_minutes is not None:
            body_dict['sessionAbsoluteTimeoutMinutes'] = session_absolute_timeout_minutes
        if global_rate_limit_per_sec is not None:
            body_dict['globalRateLimitPerSec'] = global_rate_limit_per_sec
        if expensive_rate_limit_per_sec is not None:
            body_dict['expensiveRateLimitPerSec'] = expensive_rate_limit_per_sec
        body = json.dumps(body_dict)
        headers = self._json_headers()
        headers['If-Match'] = etag
        status, content, header = self._http_request(api, 'PUT', body,
                                                     headers=headers)
        return status, content, header
    # ==================== Health APIs ====================
    def health(self):
        """GET /api/v1/health"""
        api = self.baseUrl + 'api/v1/health'
        status, content, header = self._http_request(
            api, 'GET', headers=self._json_headers())
        return status, content, header
