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
import time

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

def _py_to_erlang(value):
    """Convert a Python value to its Erlang literal string representation."""
    if isinstance(value, bool):
        return 'true' if value else 'false'
    elif isinstance(value, str):
        return '<<"%s">>' % value
    elif isinstance(value, float):
        return repr(value)
    else:
        return str(value)


def set_lighthouse_ns_config_via_diag_eval(server, **kwargs):
    """
    Merge one or more keys into the lighthouse ns_config map via diag/eval.

    Uses maps:merge so only the specified keys change — all other settings
    are preserved.  Calling ns_config:set(lighthouse, #{k => v}) without
    maps:merge replaces the entire map, wiping unspecified keys.

    Args:
        server:   TestInputServer pointing at the orchestrator node.
        **kwargs: Erlang atom key names -> Python values.
                  e.g. reporting_port=8080, reporting_interval_hours=0.000277

    Returns:
        Tuple (status, content)
    """
    shell = RemoteMachineShellConnection(server)
    shell.enable_diag_eval_on_non_local_hosts()
    shell.disconnect()
    pairs = ', '.join('%s => %s' % (k, _py_to_erlang(v))
                      for k, v in kwargs.items())
    # ns_config:search returns false on a fresh cluster before any REST POST
    # has written the lighthouse key.  Fall back to #{} so maps:merge works.
    code = ('C = case ns_config:search(lighthouse) of '
            '{value, V} -> V; false -> #{} end, '
            'ns_config:set(lighthouse, maps:merge(C, #{%s})).' % pairs)
    return RestConnection(server).diag_eval(code)


def set_lighthouse_interval_via_diag_eval(server, interval_hours):
    """Convenience wrapper — set only reporting_interval_hours via diag/eval."""
    return set_lighthouse_ns_config_via_diag_eval(
        server, reporting_interval_hours=interval_hours)


# ==================== Constants ====================

LIGHTHOUSE_DEFAULT_PORTAL_PORT = 8080

# Maps Couchbase internal service names to the names used in portal telemetry.
CB_TO_PORTAL_SERVICE_MAP = {
    'kv': 'data',
    'n1ql': 'query',
    'index': 'index',
    'fts': 'search',
    'cbas': 'analytics',
    'eventing': 'eventing',
    'backup': 'backup',
}


# ==================== Telemetry Accuracy Helpers ====================

def get_cb_cluster_uuid(server):
    """
    Return the cluster UUID from /pools.

    The cluster UUID is exposed as the top-level "uuid" field in /pools,
    not in /pools/default (where the uuid query param in bucket URIs is
    the bucket UUID, not the cluster UUID).

    Args:
        server: TestInputServer pointing at the master node.

    Returns:
        str cluster UUID, or None if not found.
    """
    raw = RestConnection(server).get_pools_info().get('uuid')
    if not raw:
        return None
    # /pools returns UUID without hyphens; portal stores in hyphenated format.
    u = raw.replace('-', '')
    return '%s-%s-%s-%s-%s' % (u[0:8], u[8:12], u[12:16], u[16:20], u[20:32])


def get_cb_cluster_node_count(server):
    """
    Return the count of active nodes in the cluster.

    Uses RestConnection.get_cluster_size() which queries /pools/default.

    Args:
        server: TestInputServer pointing at the master node.

    Returns:
        int node count.
    """
    rest = RestConnection(server)
    return rest.get_cluster_size()


def get_cb_cluster_nodes_services(server):
    """
    Return a mapping of hostname -> sorted services list for every
    active node in the cluster.

    Uses RestConnection.get_nodes() which queries /pools/default.
    Services are sorted so callers can do direct equality comparisons
    without worrying about order.

    Args:
        server: TestInputServer pointing at the master node.

    Returns:
        dict {hostname (str): sorted services list (list[str])}
        e.g. {"10.0.0.1:8091": ["index", "kv", "n1ql"]}
    """
    rest = RestConnection(server)
    nodes = rest.get_nodes()
    return {node.hostname: sorted(node.services) for node in nodes}


def get_portal_cluster(ucp_client, cluster_uuid):
    """
    Fetch a single cluster record from the portal by UUID.

    Args:
        ucp_client:   Authenticated UnifiedControlPlaneClient instance.
        cluster_uuid: str — the cluster UUID (from get_cb_cluster_uuid).

    Returns:
        dict of the parsed cluster object, or None on failure.
    """
    status, content, _ = ucp_client.get_cluster(cluster_uuid)
    if not status:
        return None
    return json.loads(content)


def get_portal_cluster_nodes(ucp_client, cluster_uuid):
    """
    Return the nodes[] list from the portal's cluster record.

    Args:
        ucp_client:   Authenticated UnifiedControlPlaneClient instance.
        cluster_uuid: str cluster UUID.

    Returns:
        list of node dicts, or None on failure.
    """
    cluster = get_portal_cluster(ucp_client, cluster_uuid)
    if cluster is None:
        return None
    return cluster.get('nodes', [])


def _uuid_in_clusters_response(data, cluster_uuid):
    """
    Check whether a cluster UUID appears in a parsed list_clusters() response.

    Handles both list responses and paginated object responses that wrap
    items under 'items' or 'clusters'. Accepts both 'uuid' and 'clusterUuid'
    key names to cover API variations.

    Args:
        data:         Parsed JSON from list_clusters() (list or dict).
        cluster_uuid: str — UUID to search for.

    Returns:
        True if found, False otherwise.
    """
    items = data if isinstance(data, list) else data.get(
        'items', data.get('clusters', []))
    for cluster in items:
        if (cluster.get('uuid') == cluster_uuid
                or cluster.get('clusterUuid') == cluster_uuid):
            return True
    return False


def wait_for_cluster_on_portal(ucp_client, cluster_uuid,
                                timeout=60, poll_interval=5):
    """
    Poll GET /api/v1/clusters until the given cluster UUID appears,
    or until timeout is exceeded.

    A POST to /internal/settings/lighthouse triggers an immediate report,
    so 60 s is normally more than enough for the portal to receive it.

    Args:
        ucp_client:    Authenticated UnifiedControlPlaneClient instance.
        cluster_uuid:  str — UUID to wait for.
        timeout:       int — max seconds to wait (default 60).
        poll_interval: int — seconds between polls (default 5).

    Returns:
        True if the cluster appeared within timeout, False otherwise.
    """
    elapsed = 0
    while elapsed < timeout:
        status, content, _ = ucp_client.list_clusters()
        if status and content:
            if _uuid_in_clusters_response(json.loads(content), cluster_uuid):
                return True
        time.sleep(poll_interval)
        elapsed += poll_interval
    return False


def wait_for_portal_node_count(ucp_client, cluster_uuid, expected_count,
                               timeout=120, poll_interval=5):
    """
    Poll the portal until telemetry.nodes for the given cluster UUID
    reaches expected_count, or until timeout is exceeded.

    Use after triggering a collector report so the portal receives the
    updated topology before asserting on node count.

    Args:
        ucp_client:     Authenticated UnifiedControlPlaneClient instance.
        cluster_uuid:   str — the cluster UUID to poll.
        expected_count: int — expected number of nodes in telemetry.nodes.
        timeout:        int — max seconds to wait (default 120).
        poll_interval:  int — seconds between polls (default 5).

    Returns:
        True if the count matched within timeout, False otherwise.
    """
    elapsed = 0
    while elapsed < timeout:
        cluster = get_portal_cluster(ucp_client, cluster_uuid)
        if cluster:
            nodes = cluster.get('telemetry', {}).get('nodes', [])
            if len(nodes) == expected_count:
                return True
        time.sleep(poll_interval)
        elapsed += poll_interval
    return False


# ==================== Hardware Aggregate Helpers ====================

def _parse_cb_version(version_str):
    """
    Parse a raw CB version string into (major_minor, edition).

    Args:
        version_str: str — e.g. "8.0.0-1234-enterprise"

    Returns:
        Tuple (version: str, edition: str) — e.g. ("8.0", "enterprise").
    """
    dash_parts = version_str.split('-')
    version = '.'.join(dash_parts[0].split('.')[:2])
    edition = dash_parts[-1] if len(dash_parts) >= 2 else 'enterprise'
    return version, edition


def _read_physical_cores(shell):
    """
    Read physical CPU core count from an open shell connection.

    Counts unique (physical_id, core_id) pairs in /proc/cpuinfo.
    Falls back to nproc on VMs/containers where physical topology is not
    exposed (e.g. all processors share the same physical id).

    Args:
        shell: Open RemoteMachineShellConnection instance.

    Returns:
        int — physical core count (>= 1).
    """
    out, _ = shell.execute_command(
        "awk '/^physical id/{p=$NF} /^core id/{k=p\",\"$NF;"
        " if(!a[k]++) c++} END{print c+0}' /proc/cpuinfo")
    count_str = out[0].strip() if out else '0'
    count = int(count_str) if count_str.isdigit() else 0
    if count > 0:
        return count
    # Fallback: nproc when physical topology is not visible
    out, _ = shell.execute_command('nproc')
    nproc_str = out[0].strip() if out else '0'
    return int(nproc_str) if nproc_str.isdigit() else 0


def _read_ram_kb(shell):
    """
    Read RAM total and available from an open shell connection via
    /proc/meminfo.

    Uses MemAvailable (not MemFree) to match ns_server's definition of
    free memory, which excludes reclaimable page cache/buffers and gives
    the same ramBytesUsed the collector reports to the portal.

    Args:
        shell: Open RemoteMachineShellConnection instance.

    Returns:
        Tuple (mem_total_kb: int, mem_avail_kb: int).
        Both are 0 on parse failure.
    """
    out, _ = shell.execute_command(
        "awk '/^MemTotal/{t=$2} /^MemAvailable/{a=$2}"
        " END{print t, a}' /proc/meminfo")
    parts = out[0].strip().split() if out else []
    try:
        return int(parts[0]), int(parts[1])
    except (IndexError, ValueError):
        return 0, 0


def get_cb_node_shell_metrics(server):
    """
    Get CPU physical cores and RAM metrics for a node via a single shell
    connection.

    Delegates to _read_physical_cores() and _read_ram_kb() to keep each
    concern separate while reusing one connection.

    Args:
        server: TestInputServer pointing at the node.

    Returns:
        dict with keys:
            cpu_physical_cores: int
            ram_bytes_total:    int  (MemTotal in bytes)
            ram_bytes_used:     int  (MemTotal - MemAvailable in bytes; volatile)
        or None on failure.
    """
    shell = RemoteMachineShellConnection(server)
    try:
        physical = _read_physical_cores(shell)
        mem_total_kb, mem_avail_kb = _read_ram_kb(shell)
        return {
            'cpu_physical_cores': physical,
            'ram_bytes_total': mem_total_kb * 1024,
            'ram_bytes_used': (mem_total_kb - mem_avail_kb) * 1024,
        }
    except Exception:
        return None
    finally:
        shell.disconnect()


def get_cb_node_hardware_via_rest(server):
    """
    Get per-node storage metrics and version info from the CB REST API
    (/nodes/self).

    Storage comes from storageTotals.hdd which is the same source
    ns_server uses when building the collector report.
    RAM and CPU physical cores are obtained via shell (get_cb_node_shell_metrics).

    Args:
        server: TestInputServer pointing at the node.

    Returns:
        dict with keys:
            cpu_logical_cores:   int  (/nodes/self cpuCount)
            storage_bytes_total: int  (storageTotals.hdd.total)
            storage_bytes_used:  int  (storageTotals.hdd.used; volatile)
            version:             str  (raw version, e.g. "8.0.0-1234-enterprise")
        or None on failure.
    """
    rest = RestConnection(server)
    try:
        raw = rest.get_nodes_self_unparsed()
    except Exception:
        return None

    hdd = raw.get('storageTotals', {}).get('hdd', {})

    return {
        'cpu_logical_cores': raw.get('cpuCount', 0),
        'storage_bytes_total': hdd.get('total', 0),
        'storage_bytes_used': hdd.get('used', 0),
        'version': raw.get('version', ''),
    }


def get_cb_cluster_aggregate_hardware(cluster):
    """
    Aggregate hardware metrics across all active nodes in a cluster.

    CPU physical cores and RAM come from shell (/proc/cpuinfo, /proc/meminfo)
    via a single connection per node.  CPU logical cores, storage, and version
    come from /nodes/self (CB REST API).

    Args:
        cluster: CBCluster instance (must have .nodes_in_cluster populated).

    Returns:
        dict:
            cpu_physical_cores:  int (sum across nodes)
            cpu_logical_cores:   int (sum across nodes)
            ram_bytes_total:     int (sum across nodes; ~2% tolerance vs portal)
            ram_bytes_used:      int (sum across nodes; volatile)
            storage_bytes_total: int (sum across nodes)
            storage_bytes_used:  int (sum across nodes; volatile)
            version:             str (major.minor from first node, e.g. "8.0")
            edition:             str (e.g. "enterprise")
        or None if any per-node call fails.
    """
    totals = {
        'cpu_physical_cores': 0,
        'cpu_logical_cores': 0,
        'ram_bytes_total': 0,
        'ram_bytes_used': 0,
        'storage_bytes_total': 0,
        'storage_bytes_used': 0,
        'version': '',
        'edition': '',
    }
    for server in cluster.nodes_in_cluster:
        shell_hw = get_cb_node_shell_metrics(server)
        if shell_hw is None:
            return None
        rest_hw = get_cb_node_hardware_via_rest(server)
        if rest_hw is None:
            return None

        totals['cpu_physical_cores'] += shell_hw['cpu_physical_cores']
        totals['ram_bytes_total'] += shell_hw['ram_bytes_total']
        totals['ram_bytes_used'] += shell_hw['ram_bytes_used']
        totals['cpu_logical_cores'] += rest_hw['cpu_logical_cores']
        totals['storage_bytes_total'] += rest_hw['storage_bytes_total']
        totals['storage_bytes_used'] += rest_hw['storage_bytes_used']
        if not totals['version'] and rest_hw['version']:
            totals['version'], totals['edition'] = \
                _parse_cb_version(rest_hw['version'])
    return totals


def assert_within_tolerance(test_case, actual, expected, tolerance_pct, label=''):
    """
    Assert that actual is within tolerance_pct% of expected.

    Args:
        test_case:     unittest.TestCase instance (provides assert methods).
        actual:        int/float — value from the portal.
        expected:      int/float — ground truth value.
        tolerance_pct: numeric — allowed percentage deviation.
        label:         str — prefix for the failure message.
    """
    if expected == 0:
        test_case.assertEqual(
            actual, 0,
            "%s: expected 0, got %d" % (label, actual))
        return
    diff_pct = abs(actual - expected) / float(expected) * 100
    test_case.assertLessEqual(
        diff_pct, tolerance_pct,
        "%s: actual=%d, expected~%d, diff=%.2f%% (tolerance %d%%)"
        % (label, actual, expected, diff_pct, tolerance_pct))


def get_cb_cluster_services_union(cluster):
    """
    Return the sorted union of portal-mapped service names across all nodes.

    Mirrors the 'services' field at the cluster level in portal telemetry,
    which aggregates and deduplicates per-node services.

    Args:
        cluster: CBCluster instance (has .master attribute).

    Returns:
        list[str] — sorted unique portal service names (e.g. ["data", "query"]).
    """
    all_services = set()
    for services_list in get_cb_cluster_nodes_services(cluster.master).values():
        for svc in services_list:
            all_services.add(CB_TO_PORTAL_SERVICE_MAP.get(svc, svc))
    return sorted(all_services)

