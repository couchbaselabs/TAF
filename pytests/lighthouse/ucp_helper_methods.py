
"""
UCP Helper Methods
Helper functions for building API payloads, parsing timestamps,
and constructing request bodies for Unified Control Plane tests.
"""
import json
from datetime import datetime
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
    # Update with new idle timeout
    status, content, header = client.update_config(
        etag=etag,
        telemetry_retention_days=config.get('telemetryRetentionDays'),
        session_idle_timeout_minutes=timeout_minutes,
        session_absolute_timeout_minutes=config.get(
            'sessionAbsoluteTimeoutMinutes')
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
# ==================== Timestamp Helpers ====================

def parse_iso8601_timestamp(timestamp_str):
    """Parse ISO 8601 UTC timestamp to datetime."""
    if timestamp_str.endswith('Z'):
        timestamp_str = timestamp_str[:-1]
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

def build_subscription_payload(start_at, end_at, nodes,
                              logical_cores, ram_bytes):
    """Build entitlement subscription payload."""
    return {
        'startAt': start_at,
        'endAt': end_at,
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
