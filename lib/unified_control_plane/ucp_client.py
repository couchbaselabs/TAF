
"""
Unified Control Plane API Client
Raw HTTP client for hitting UCP endpoints.
Inherits from TAF's RestConnection to reuse _http_request().
Returns raw responses without wrapper logic.
"""
import json
import urllib.parse
from typing import Optional, Dict, Any, Tuple
from Rest_Connection import RestConnection as BaseRestConnection
from global_vars import logger
class UnifiedControlPlaneClient(BaseRestConnection):
    """
    Raw API client for Unified Control Plane endpoints.
    Inherits from TAF's RestConnection to reuse _http_request().
    All methods hit endpoints and return raw (status, content, response) tuples.
    """
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
        # Build baseUrl directly — do NOT let RestConnection
        # mangle the port with CB TLS logic.
        # UCP has its own port that is independent of Couchbase.
        scheme = "https"
        self.baseUrl = "{0}://{1}:{2}/".format(scheme, self.ip, self.port)
        # Session cookie storage — UCP uses cookie-based sessions
        self._session_cookie = None
    def _json_headers(self):
        """
        Return headers for UCP API requests.
        - Always: Content-Type: application/json
        - If session cookie exists: Cookie header
        - NO Basic auth — UCP uses session-based auth only.
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
            api += '?' + urllib.parse.urlencode(params)
        status, content, header = self._http_request(
            api, 'GET', headers=self._json_headers())
        return status, content, header
    def create_user(self, user_id, roles, enabled=True):
        """POST /api/v1/users"""
        api = self.baseUrl + 'api/v1/users'
        body = json.dumps({
            'userId': user_id,
            'enabled': enabled,
            'roles': roles
        })
        status, content, header = self._http_request(
            api, 'POST', body, headers=self._json_headers())
        return status, content, header
    def get_user(self, user_id):
        """GET /api/v1/users/{userId}"""
        api = self.baseUrl + 'api/v1/users/%s' % urllib.parse.quote(user_id)
        status, content, header = self._http_request(
            api, 'GET', headers=self._json_headers())
        return status, content, header
    def update_user(self, user_id, etag, enabled=None, roles=None):
        """PUT /api/v1/users/{userId}"""
        api = self.baseUrl + 'api/v1/users/%s' % urllib.parse.quote(user_id)
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
        api = self.baseUrl + 'api/v1/users/%s' % urllib.parse.quote(user_id)
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
            api += '?' + urllib.parse.urlencode(params)
        status, content, header = self._http_request(
            api, 'GET', headers=self._json_headers())
        return status, content, header
    def get_cluster(self, cluster_uuid):
        """GET /api/v1/clusters/{clusterUuid}"""
        api = self.baseUrl + 'api/v1/clusters/%s' % urllib.parse.quote(cluster_uuid)
        status, content, header = self._http_request(
            api, 'GET', headers=self._json_headers())
        return status, content, header
    def get_cluster_history(self, cluster_uuid, from_timestamp=None,
                           to_timestamp=None, offset=None, limit=None):
        """GET /api/v1/clusters/{clusterUuid}/history"""
        api = self.baseUrl + 'api/v1/clusters/%s/history' % urllib.parse.quote(cluster_uuid)
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
            api += '?' + urllib.parse.urlencode(params)
        status, content, header = self._http_request(
            api, 'GET', headers=self._json_headers())
        return status, content, header
    def update_cluster(self, cluster_uuid, etag, classification=None,
                      business_unit=None, name=None, description=None):
        """PUT /api/v1/clusters/{clusterUuid}"""
        api = self.baseUrl + 'api/v1/clusters/%s' % urllib.parse.quote(cluster_uuid)
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
    def get_entitlement_usage(self):
        """GET /api/v1/entitlements/usage"""
        api = self.baseUrl + 'api/v1/entitlements/usage'
        status, content, header = self._http_request(
            api, 'GET', headers=self._json_headers())
        return status, content, header
    # ==================== Reports APIs ====================
    def generate_usage_report(self, from_timestamp, to_timestamp,
                             format_type='pdf'):
        """GET /api/v1/reports/usage"""
        api = self.baseUrl + 'api/v1/reports/usage'
        params = {
            'from': from_timestamp,
            'to': to_timestamp,
            'format': format_type
        }
        api += '?' + urllib.parse.urlencode(params)
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
            api += '?' + urllib.parse.urlencode(params)
        status, content, header = self._http_request(
            api, 'GET', headers=self._json_headers())
        return status, content, header
    def get_audit_event(self, audit_event_id):
        """GET /api/v1/audit/{auditEventId}"""
        api = self.baseUrl + 'api/v1/audit/%s' % urllib.parse.quote(audit_event_id)
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
                     session_absolute_timeout_minutes=None):
        """PUT /api/v1/config"""
        api = self.baseUrl + 'api/v1/config'
        body_dict = {}
        if telemetry_retention_days is not None:
            body_dict['telemetryRetentionDays'] = telemetry_retention_days
        if session_idle_timeout_minutes is not None:
            body_dict['sessionIdleTimeoutMinutes'] = session_idle_timeout_minutes
        if session_absolute_timeout_minutes is not None:
            body_dict['sessionAbsoluteTimeoutMinutes'] = session_absolute_timeout_minutes
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
