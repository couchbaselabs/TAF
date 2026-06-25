# -*- coding: utf-8 -*-
"""
Collector Helper Methods
Helper functions for interacting with the ns_server Lighthouse Collector APIs.

These helpers call the raw LighthouseCollectorClient methods and handle
JSON parsing, payload building, and common validation patterns — exactly
mirroring the role that ucp_helper_methods.py plays for UnifiedControlPlaneClient.

Design reference: Lighthouse Collector Design Revision v1.4 (17 Jun 2026)
"""
import json

from membase.api.rest_client import RestConnection
from platform_utils.remote.remote_util import RemoteMachineShellConnection

# ==================== Settings Helper Methods ====================

def get_collector_settings(client):
    """
    Retrieve the current Lighthouse Collector configuration.

    Args:
        client: LighthouseCollectorClient instance

    Returns:
        dict of current settings, or None on failure.
        Example:
            {
                "enabled": true,
                "endpoint": "lighthouse.couchbase.internal",
                "reportIntervalHours": 2,
                "reportTimeoutSeconds": 1,
                "externalNodesMaxPayloadBytes": 10240,
                "externalNodesMaxCount": 100
            }
    """
    status, content, header = client.get_lighthouse_settings()
    if not status:
        return None
    return json.loads(content)


def is_collector_enabled(client):
    """
    Check whether the Lighthouse Collector is currently enabled.

    Args:
        client: LighthouseCollectorClient instance

    Returns:
        True if enabled, False if disabled, None on failure.
    """
    settings = get_collector_settings(client)
    if settings is None:
        return None
    return settings.get('enabled')


def set_collector_enabled(client, enabled):
    """
    Enable or disable the Lighthouse Collector.

    Args:
        client:  LighthouseCollectorClient instance
        enabled: bool — True to enable, False to disable

    Returns:
        Tuple (status, content, header)
    """
    return client.update_lighthouse_settings(enabled=enabled)


def set_collector_endpoint(client, endpoint):
    """
    Update the lighthouse portal hostname/IP the collector reports to.

    Note: Any successful POST to the settings endpoint immediately
    triggers a report attempt, so this also acts as a connectivity test.

    Args:
        client:   LighthouseCollectorClient instance
        endpoint: str — e.g. "lighthouse.couchbase.internal"

    Returns:
        Tuple (status, content, header)
    """
    return client.update_lighthouse_settings(endpoint=endpoint)


def set_collector_report_interval(client, hours):
    """
    Update how often (in hours) the collector sends telemetry.

    Args:
        client: LighthouseCollectorClient instance
        hours:  int — reporting interval in hours (default 2)

    Returns:
        Tuple (status, content, header)
    """
    return client.update_lighthouse_settings(report_interval_hours=hours)


def set_collector_timeout(client, seconds):
    """
    Update the per-attempt connection timeout for the collector.

    Args:
        client:  LighthouseCollectorClient instance
        seconds: int — connection timeout in seconds (default 1)

    Returns:
        Tuple (status, content, header)
    """
    return client.update_lighthouse_settings(report_timeout_seconds=seconds)


def set_collector_external_node_limits(client, max_payload_bytes=None,
                                       max_count=None):
    """
    Update limits for external node (e.g. SGW) payload ingestion.

    Args:
        client:            LighthouseCollectorClient instance
        max_payload_bytes: int — max bytes for a single external payload
                           (default 10240)
        max_count:         int — max number of tracked external nodes
                           (default 100)

    Returns:
        Tuple (status, content, header)
    """
    return client.update_lighthouse_settings(
        external_nodes_max_payload_bytes=max_payload_bytes,
        external_nodes_max_count=max_count)


def force_report(client):
    """
    Force an immediate telemetry report attempt to the lighthouse portal.

    Any POST to /internal/settings/lighthouse — even a no-op — triggers
    an immediate report attempt.  This is the designed way to verify
    connectivity without waiting for the report interval.

    Args:
        client: LighthouseCollectorClient instance

    Returns:
        Tuple (status, content, header)
    """
    # Fetch current settings so we can POST them back unchanged (no-op).
    settings = get_collector_settings(client)
    if settings is None:
        # If GET failed, send an empty POST body — server will use defaults.
        return client.update_lighthouse_settings()
    return client.update_lighthouse_settings(
        enabled=settings.get('enabled'),
        endpoint=settings.get('endpoint'),
        report_interval_hours=settings.get('reportIntervalHours'),
        report_timeout_seconds=settings.get('reportTimeoutSeconds'),
        external_nodes_max_payload_bytes=settings.get(
            'externalNodesMaxPayloadBytes'),
        external_nodes_max_count=settings.get('externalNodesMaxCount'))


def restore_collector_settings(client, saved_settings):
    """
    Restore a previously saved settings dict (from get_collector_settings).
    Useful in tearDown to undo config changes made during a test.

    Args:
        client:          LighthouseCollectorClient instance
        saved_settings:  dict as returned by get_collector_settings()

    Returns:
        Tuple (status, content, header)
    """
    return client.update_lighthouse_settings(
        enabled=saved_settings.get('enabled'),
        endpoint=saved_settings.get('endpoint'),
        report_interval_hours=saved_settings.get('reportIntervalHours'),
        report_timeout_seconds=saved_settings.get('reportTimeoutSeconds'),
        external_nodes_max_payload_bytes=saved_settings.get(
            'externalNodesMaxPayloadBytes'),
        external_nodes_max_count=saved_settings.get('externalNodesMaxCount'))


# ==================== SGW / External Node Ingest Helpers ====================

def ingest_sgw_telemetry(client, product_name, instance_id, payload):
    """
    Submit Sync Gateway (or any external component) telemetry to ns_server
    for inclusion in the next lighthouse report.

    ns_server stores the latest payload per (product_name, instance_id)
    pair; after each report the stored payloads are wiped.

    Args:
        client:       LighthouseCollectorClient instance
        product_name: str — stable opaque product identifier
                      (e.g. "sync_gateway").  All nodes sharing the same
                      name are grouped under the same key in
                      externalNodes.<product_name>.
        instance_id:  str — stable unique identifier for this specific
                      external node instance.  Used for deduplication.
        payload:      dict — telemetry body (must be JSON-serialisable).

    Returns:
        Tuple (status, content, header)
    """
    return client.ingest_external_telemetry(product_name, instance_id,
                                            payload)


# ==================== Payload Builders ====================

def build_sgw_payload(instance_id, cpu_cores, ram_bytes_total,
                      ram_bytes_used, edition, version, name,
                      os_version, hostname, uptime_seconds):
    """
    Build a Sync Gateway external-node telemetry payload.

    This matches the reference SGW payload shape from the design doc.
    ns_server passes the payload through to the lighthouse without
    inspection beyond confirming it is valid JSON.

    Args:
        instance_id:     str — stable unique SGW instance identifier
        cpu_cores:       int — number of CPU cores
        ram_bytes_total: int — total RAM in bytes
        ram_bytes_used:  int — used RAM in bytes
        edition:         str — e.g. "enterprise" or "community"
        version:         str — e.g. "3.2.0"
        name:            str — product name, e.g. "Sync Gateway"
        os_version:      str — OS description string
        hostname:        str — SGW node hostname
        uptime_seconds:  int — node uptime in seconds

    Returns:
        dict ready to pass to ingest_sgw_telemetry()
    """
    return {
        'instanceId': instance_id,
        'cpuCores': cpu_cores,
        'ramBytesTotal': ram_bytes_total,
        'ramBytesUsed': ram_bytes_used,
        'product': {
            'edition': edition,
            'version': version,
            'name': name
        },
        'osVersion': os_version,
        'hostname': hostname,
        'uptimeSeconds': uptime_seconds
    }


def build_collector_settings_payload(enabled=True,
                                     endpoint='lighthouse.couchbase.internal',
                                     report_interval_hours=2,
                                     report_timeout_seconds=1,
                                     external_nodes_max_payload_bytes=10240,
                                     external_nodes_max_count=100):
    """
    Build a full Lighthouse Collector settings payload dict.

    Defaults match the design-doc v1.4 defaults.

    Returns:
        dict with camelCase keys as expected by the API.
    """
    return {
        'enabled': enabled,
        'endpoint': endpoint,
        'reportIntervalHours': report_interval_hours,
        'reportTimeoutSeconds': report_timeout_seconds,
        'externalNodesMaxPayloadBytes': external_nodes_max_payload_bytes,
        'externalNodesMaxCount': external_nodes_max_count
    }


def parse_response_json(content):
    """Parse JSON response content, returns None on failure."""
    if not content:
        return None
    try:
        return json.loads(content)
    except ValueError:
        return None


# ==================== Diag/Eval Helpers ====================

def set_lighthouse_interval_via_diag_eval(server, interval_hours):
    """
    Bypass the REST API to set the lighthouse reporting interval directly in
    ns_config via /diag/eval.  Useful in tests that need a sub-1-hour interval
    (e.g. 1/3600 == ~1 second) so the collector fires quickly without waiting
    for the 2-hour default.

    Follows the same pattern used in StatsLib/StatsOperations_Rest.py:
      1. Enable diag/eval on non-local hosts via RemoteMachineShellConnection.
      2. Send the Erlang expression via RestConnection.diag_eval().

    Design-doc example:
      ns_config:set(lighthouse,
          #{reporting_endpoint => <<"127.0.0.1">>,
            reporting_interval_hours => 1/3600}).

    Args:
        server:         TestInputServer pointing at the orchestrator node.
        interval_hours: float — reporting interval in hours.
                        Pass 1/3600.0 for a ~1-second interval.

    Returns:
        Tuple (status, content)
    """
    shell = RemoteMachineShellConnection(server)
    shell.enable_diag_eval_on_non_local_hosts()
    shell.disconnect()
    rest = RestConnection(server)
    code = ('ns_config:set(lighthouse, '
            '#{reporting_interval_hours => %s}).' % interval_hours)
    return rest.diag_eval(code)

