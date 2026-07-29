
"""
UCP Helper Methods
Helper functions for building API payloads, parsing timestamps,
and constructing request bodies for Unified Control Plane tests.
"""
import json
from datetime import datetime, timedelta

from unified_control_plane import UnifiedControlPlaneClient
from lighthouse.response import UCPResponse
# ==================== Session Helper Methods ====================
def create_session(client, username, password):
    """
    Create a new authenticated session via login.
    Args:
        client: UnifiedControlPlaneClient instance
        username: User email/ID
        password: User password
    Returns:
        Tuple (status, content, header) from login call.
    """
    status, content, header = client.session_login(username, password)
    return status, content, header

def verify_session_active(client):
    """
    Verify the current session is still active by hitting /session/me.
    Returns:
        True if session is valid, False otherwise
    """
    status, content, header = client.session_me()
    return status

def verify_session_expired(client):
    """
    Verify the current session has expired (401 on /session/me).
    Returns:
        True if session is expired (got 401), False if still active
    """
    status, content, header = client.session_me()
    return not status

def get_session_idle_timeout(client):
    """
    Retrieve the current sessionIdleTimeoutMinutes from config.
    Returns:
        int value of sessionIdleTimeoutMinutes, or None on failure
    """
    status, content, header = client.get_config()
    if not status:
        return None
    config = json.loads(content)
    return config.get('sessionIdleTimeoutMinutes')

def set_session_idle_timeout(client, timeout_minutes):
    """
    Set sessionIdleTimeoutMinutes in config. Fetches ETag automatically.
    Args:
        client: UnifiedControlPlaneClient instance (authenticated as admin)
        timeout_minutes: New idle timeout value in minutes (5-480)
    Returns:
        Tuple (status, content, header) from config update
    """
    # Get current config ETag
    status, content, header = client.get_config()
    if not status:
        return status, content, header
    config = json.loads(content)
    etag = header.headers.get('ETag') if header else None
    # Update with new idle timeout. The portal validates this PUT as a
    # full replacement, not a partial update, so every current field must
    # be resent even though only the idle timeout is actually changing.
    status, content, header = client.update_config(
        etag=etag,
        telemetry_retention_days=config.get('telemetryRetentionDays'),
        session_idle_timeout_minutes=timeout_minutes,
        session_absolute_timeout_minutes=config.get(
            'sessionAbsoluteTimeoutMinutes'),
        global_rate_limit_per_sec=config.get('globalRateLimitPerSec'),
        expensive_rate_limit_per_sec=config.get('expensiveRateLimitPerSec')
    )
    return status, content, header

def keep_session_alive(client):
    """
    Touch the session to reset idle timer (any authenticated request).
    Returns:
        True if session is still alive after touch, False otherwise
    """
    status, content, header = client.session_me()
    return status

def get_session_cookie(client):
    """
    Return the session cookie string ("name=value") the client is
    currently holding, or None if no session cookie is held.
    Args:
        client: UnifiedControlPlaneClient instance
    Returns:
        Cookie string or None
    """
    return client._session_cookie

def set_session_cookie(client, cookie):
    """
    Inject a session cookie into the client (pass None to clear it).
    Used to replay a previously saved cookie, e.g. to prove the
    portal rejects a logged-out session's cookie.
    Args:
        client: UnifiedControlPlaneClient instance
        cookie: Cookie string ("name=value") or None
    """
    client._session_cookie = cookie

def extract_session_id(cookie):
    """
    Extract the session ID (the cookie value) from a "name=value"
    session cookie string.
    Args:
        cookie: Cookie string as stored by the client
    Returns:
        Session ID string, or None if the cookie is empty/malformed
    """
    if not cookie or '=' not in cookie:
        return None
    return cookie.split('=', 1)[1]

# ==================== User Provisioning Helpers ====================
def open_local_user_session(portal, admin_client, user_id, temp_password,
                            new_password, roles):
    """
    Provision a local UCP user and return a NEW client already logged in
    as that user.

    An admin-set password is temporary (a login with it returns 401
    password_expired), so a usable session requires three steps:
      1. admin POST /users  {authType:local, password:temp}   -> 201
      2. POST /session/change-password (currentPassword=temp)  -> 204
      3. POST /session/login with the new password             -> 204
    authType:local auto-provisions the backing CBS local user, so no
    separate CBS user creation is needed.

    Args:
        portal:        LighthousePortal config (used to build the new client)
        admin_client:  an already-logged-in admin UnifiedControlPlaneClient
        user_id:       userId to create (keep to clean chars: spaces and
                       ,;<>{} currently return 500 -- CT-BUG-026)
        temp_password: temporary password set at creation
        new_password:  final password (must satisfy CBS password policy;
                       a weak password currently returns 500)
        roles:         list of UCP roles, e.g. [ROLE_SYSTEM_VIEWER]

    Returns:
        Tuple (user_client, error). On success error is None and
        user_client is a logged-in UnifiedControlPlaneClient. On failure
        user_client is None and error is a human-readable string.
    """
    status, content, _ = admin_client.create_user(
        user_id, roles=roles, auth_type='local', password=temp_password)
    if not status:
        return None, "create_user failed: %s" % content
    user_client = UnifiedControlPlaneClient(portal)
    status, content, _ = user_client.change_password(
        user_id, temp_password, new_password)
    if not status:
        return None, "change_password failed: %s" % content
    status, content, _ = user_client.session_login(user_id, new_password)
    if not status:
        return None, "login as new user failed: %s" % content
    return user_client, None

def safe_delete_user(client, user_id):
    """
    Best-effort DELETE of a user; never raises (for setUp/tearDown cleanup).
    Returns True if the delete call succeeded, False otherwise.
    """
    try:
        status, _, _ = client.delete_user(user_id)
        return status
    except Exception:
        return False
# ==================== Raw Request Helpers ====================
def get_raw(client, path, query=None):
    """
    Issue a GET against an arbitrary UCP path (optionally with a raw query
    string), reusing the client's session cookie. For exercising endpoints
    with parameters the typed client methods do not model (e.g. an unknown
    query parameter).
    Args:
        client: UnifiedControlPlaneClient instance (authenticated)
        path:   path below baseUrl, e.g. "api/v1/users"
        query:  raw query string without the leading '?', or None
    Returns:
        Tuple (status, content, header) from the request.
    """
    api = client.baseUrl + path
    if query:
        api += '?' + query
    return client._http_request(api, 'GET')

def raw_request(client, method, path, body=None):
    """
    Issue an arbitrary-method raw request against a UCP path, reusing the
    client's session cookie. For exercising HTTP verbs the typed client
    methods do not expose (e.g. PUT/DELETE against a resource that has no
    corresponding client method because it is meant to be immutable, such
    as an audit event).
    Args:
        client: UnifiedControlPlaneClient instance (authenticated)
        method: HTTP method string, e.g. 'PUT', 'DELETE'
        path:   path below baseUrl, e.g. "api/v1/audit/<id>"
        body:   dict to JSON-encode as the request body, or None
    Returns:
        Tuple (status, content, header) from the request.
    """
    api = client.baseUrl + path
    payload = json.dumps(body) if body is not None else ''
    return client._http_request(api, method, payload,
                                headers=client._json_headers())

def post_raw_body(client, path, raw_body):
    """
    POST a literal, pre-serialized string body against a UCP path, bypassing
    JSON encoding entirely -- for exercising malformed/non-JSON request
    bodies that a dict-based helper could never produce (json.dumps always
    emits valid JSON).
    Args:
        client:   UnifiedControlPlaneClient instance (authenticated)
        path:     path below baseUrl, e.g. "api/v1/users"
        raw_body: literal string sent as the request body, as-is
    Returns:
        Tuple (status, content, header) from the request.
    """
    api = client.baseUrl + path
    return client._http_request(api, 'POST', raw_body,
                                headers=client._json_headers())
# ==================== User Helper Methods ====================
def get_user_with_etag(client, user_id):
    """
    Fetch a user record together with its ETag (needed for any
    PUT /users/{userId} call).
    Args:
        client: UnifiedControlPlaneClient instance (authenticated)
        user_id: userId of the user to fetch
    Returns:
        Tuple (user_dict, etag) -- (None, None) on failure
    """
    status, content, header = client.get_user(user_id)
    if not status:
        return None, None
    user = json.loads(content)
    etag = header.headers.get('ETag') if header is not None else None
    return user, etag
# ==================== Audit Helper Methods ====================
def get_latest_audit_event(client, action=None, actor=None, resource_id=None,
                           since=None, limit=10):
    """
    Fetch the most recent audit event matching action/actor, optionally
    narrowed to a specific resourceId and/or a time window.

    On a shared portal (this lab is used by other engineers/test runs
    concurrently), trusting item 0 of an actor+action-only query is a
    race: someone else's matching event can land in between your action
    and your lookup and get mistaken for yours, or push yours off the
    page. Passing `resource_id` (a userId, cluster UUID, etc -- whatever
    the mutated resource's own id is) and/or `since` (a timestamp
    captured right before performing the action, via
    get_current_iso8601_timestamp()) scopes the match to events that can
    only be the one you just caused.

    Args:
        client:      UnifiedControlPlaneClient instance (authenticated)
        action:      audit action string to filter on (e.g. ACTION_LOGIN),
                     or None for no action filter
        actor:       actor string to filter on, or None for no actor filter
        resource_id: if given, only consider events whose resourceId
                     matches exactly; None to skip this check (e.g. for
                     session/report actions, which carry no resourceId)
        since:       if given, used as a lower bound so only events at/after
                     roughly this instant are considered (see padding note
                     below)
        limit:       how many of the most recent matching events to fetch
                     when resource_id is given (ignored -- forced to 1 --
                     when resource_id is None, since item 0 is then taken
                     directly)
    Returns:
        Tuple (event_dict, response) -- event_dict is None if the call
        failed, or no (optionally resource-matched) event was found.
    """
    from_timestamp = since
    if since is not None:
        from_timestamp = format_iso8601_timestamp(
            parse_iso8601_timestamp(since) - timedelta(seconds=2))
    status, content, header = client.list_audit_events(
        action=action, actor=actor, from_timestamp=from_timestamp,
        limit=(limit if resource_id is not None else 1))
    response = UCPResponse(status, content, header)
    if not status:
        return None, response
    items = response.items
    if not items:
        return None, response
    if resource_id is None:
        return items[0], response
    for item in items:
        if item.get('resourceId') == resource_id:
            return item, response
    return None, response

# ==================== Timestamp Helpers ====================

def parse_iso8601_timestamp(timestamp_str):
    """Parse ISO 8601 UTC timestamp to datetime."""
    if timestamp_str.endswith('Z'):
        timestamp_str = timestamp_str[:-1]
    # The portal returns nanosecond-precision fractional seconds (9 digits);
    # %f only accepts up to 6 (microseconds), so truncate before matching.
    if '.' in timestamp_str:
        whole, frac = timestamp_str.split('.', 1)
        timestamp_str = '%s.%s' % (whole, frac[:6])
    # Handle both with and without fractional seconds
    for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f'):
        try:
            return datetime.strptime(timestamp_str, fmt)
        except ValueError:
            continue
    raise ValueError("Unable to parse timestamp: %s" % timestamp_str)

def format_iso8601_timestamp(dt):
    """Format datetime to ISO 8601 UTC string."""
    return dt.strftime('%Y-%m-%dT%H:%M:%S') + 'Z'

def get_current_iso8601_timestamp():
    """Get current time as ISO 8601 UTC timestamp."""
    return format_iso8601_timestamp(datetime.utcnow())

# ==================== Payload Builders ====================
def build_telemetry_payload(collected_at, cluster_uuid, product, nodes):
    """Build telemetry payload for ingest."""
    return {
        'collectedAt': collected_at,
        'clusterUuid': cluster_uuid,
        'product': product,
        'nodes': nodes
    }

def build_node_telemetry(hostname, cpu_physical_cores, cpu_logical_cores,
                        ram_bytes_total, ram_bytes_used,
                        storage_bytes_total, storage_bytes_used,
                        services, os=None, uptime_seconds=None):
    """Build node telemetry object."""
    node = {
        'cpuPhysicalCores': cpu_physical_cores,
        'cpuLogicalCores': cpu_logical_cores,
        'ramBytesTotal': ram_bytes_total,
        'ramBytesUsed': ram_bytes_used,
        'storageBytesTotal': storage_bytes_total,
        'storageBytesUsed': storage_bytes_used,
        'services': services,
        'hostname': hostname
    }
    if os is not None:
        node['os'] = os
    if uptime_seconds is not None:
        node['uptimeSeconds'] = uptime_seconds
    return node

def build_minimal_ingest_payload(cluster_uuid, product=None):
    """
    Build a well-formed single-node telemetry ingest payload.

    Used as the valid baseline for ingest input-validation / security tests:
    the test takes this dict and mutates one thing (injects an unknown field,
    bloats it, or swaps the clusterUuid) so that the mutation is the only
    reason the request could be rejected.

    The portal validates the payload shape: the top-level 'product' must be
    an object {name, edition, version} and every node must carry an
    'edition'.  Hardware values are arbitrary but valid positive integers --
    the ingest endpoint is being exercised for input handling here, not for
    telemetry accuracy.
    """
    if product is None:
        product = {
            'name': 'Couchbase Server',
            'edition': 'enterprise',
            'version': '7.6.12-8944-enterprise',
        }
    node = build_node_telemetry(
        hostname='node-0.test.local',
        cpu_physical_cores=4, cpu_logical_cores=8,
        ram_bytes_total=17179869184, ram_bytes_used=4294967296,
        storage_bytes_total=107374182400, storage_bytes_used=10737418240,
        services=['data'], os='linux', uptime_seconds=3600)
    node['edition'] = 'enterprise'
    return build_telemetry_payload(
        collected_at=get_current_iso8601_timestamp(),
        cluster_uuid=cluster_uuid,
        product=product,
        nodes=[node])

def build_subscription_payload(start_at, end_at, nodes,
                              logical_cores, ram_bytes,
                              classification='production',
                              support_level='platinum'):
    """
    Build an entitlement subscription payload matching the `entitlements`
    document shape (Architecture Spec 10.4): startAt, endAt, classification,
    supportLevel, limits{nodes, logicalCores, ramBytes}.
    """
    return {
        'startAt': start_at,
        'endAt': end_at,
        'classification': classification,
        'supportLevel': support_level,
        'limits': {
            'nodes': nodes,
            'logicalCores': logical_cores,
            'ramBytes': ram_bytes
        }
    }

def parse_response_json(content):
    """Parse JSON response content, returns None on failure."""
    if not content:
        return None
    try:
        return json.loads(content)
    except ValueError:
        return None
