# -*- coding: utf-8 -*-
"""
Entitlement / SKU and usage helper methods for the UCP portal.

Split out from ucp_helper_methods.py because the SKU surface has its own
plumbing: the profile is mutated through a multipart file import rather than
a JSON PUT, and every usage assertion has to be scoped to one cluster (the
lab portal is a shared fleet -- old test clusters can never be deleted, and
a real cluster may be reporting while a test runs, so fleet-wide totals are
never safe to assert on).

Usage response shape, CONFIRMED live 2026-08-18 against the QE lab portal:

    {"items": [{"classification": "production", "supportLevel": "platinum",
                "unlimited": false,
                "instanceSize": {"logicalCores": 32, "ramBytes": 274877906944},
                "limit": 20, "used": 83,
                "clusters": [{"name": "", "uuid": "...", "logicalCores": 4,
                              "ramBytes": 4287483904, "nodes": 2,
                              "businessUnit": ""}]}],
     "summary": {"total": 5, "withinContract": 3, "exceedContract": 2}}

So: rows under `items`, consumed under `used`, entitled under `limit`,
contributors under `clusters` as objects keyed by `uuid`, and the per-node
ceiling under `instanceSize` (NOT `limits` -- that spelling is only on the
subscription that produced the row). Unmatched nodes land in a row with
`classification: null` and `limit: 0`. A cluster can appear MORE THAN ONCE in
one `clusters` array, with its nodes split across entries, so count by summing
`nodes` rather than by counting entries.

The readers below still accept alternative spellings: the confirmed name is
tried first, so a rename shows up as a changed value rather than a crash.

Must stay Jython/Python-2 compatible -- no f-strings, no type annotations.
"""
import json
from datetime import datetime, timedelta

from lighthouse.response import UCPResponse


# ==================== Date Window Helpers ====================

def active_window(days_before=1, days_after=365):
    """
    Return (start_at, end_at) ISO 8601 strings for a window that is active
    now.

    Dates are always computed relative to now, never hardcoded: a literal
    year in a subscription silently stops meaning "active" once that year
    passes, which turns a real matching failure into a green test.

    Args:
        days_before: how far before now the window opens
        days_after:  how far after now the window closes

    Returns:
        Tuple (start_at, end_at) of ISO 8601 UTC strings.
    """
    now = datetime.utcnow()
    return (_iso(now - timedelta(days=days_before)),
            _iso(now + timedelta(days=days_after)))


def _iso(dt):
    """Format a datetime as an ISO 8601 UTC string the portal accepts."""
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')


# ==================== Profile Document Helpers ====================

def build_entitlement_document(subscriptions, schema_version='v1'):
    """
    Build the entitlement import document.

    schemaVersion is gated by the importer -- a document whose version it
    does not accept is rejected before anything else is looked at -- so it is
    always included.

    Args:
        subscriptions:  list of subscription dicts (may be empty)
        schema_version: value for the schemaVersion field

    Returns:
        dict ready to be uploaded.
    """
    return {
        'schemaVersion': schema_version,
        'subscriptions': subscriptions,
    }


def get_entitlement_profile(client):
    """
    Return the current entitlement profile as a dict, or None if the read
    failed.

    Args:
        client: authenticated UnifiedControlPlaneClient

    Returns:
        Parsed profile dict, or None.
    """
    status, content, header = client.get_entitlements()
    response = UCPResponse(status, content, header)
    if not status:
        return None
    return response.json


def get_profile_subscriptions(profile):
    """
    Return the subscriptions list from a profile dict.

    An unconfigured profile omits the key entirely rather than carrying an
    empty list, so a missing key and an empty list both mean "no SKUs".

    Args:
        profile: profile dict as returned by get_entitlement_profile

    Returns:
        List of subscription dicts (empty if unconfigured).
    """
    if not isinstance(profile, dict):
        return []
    subscriptions = profile.get('subscriptions')
    if not isinstance(subscriptions, list):
        return []
    return subscriptions


# ==================== Profile Mutation Helpers ====================

def set_entitlement_subscriptions(client, subscriptions,
                                  filename='entitlements.json',
                                  verify=True):
    """
    Replace the entitlement profile with the given subscriptions.

    The upload is a FULL REPLACE of the profile (confirmed live 2026-08-18: a
    one-subscription upload replaced a four-subscription profile), so there is
    no need to clear first -- and no way to, see
    clear_entitlement_subscriptions.

    Uploads the document to PUT /entitlements as multipart, then reads the
    profile back and confirms the subscription count round-tripped. A usage
    assertion made against an entitlement that silently failed to upload is
    the single biggest source of false results here, so the read-back is on
    by default.

    Args:
        client:        authenticated UnifiedControlPlaneClient (system_admin)
        subscriptions: list of subscription dicts to store
        filename:      name advertised for the uploaded file part
        verify:        when True, read the profile back and check the count

    Returns:
        Tuple (ok, detail) -- ok is a bool, detail is None on success or a
        message naming the HTTP status and body on failure.
    """
    status, content, header = client.get_entitlements()
    current = UCPResponse(status, content, header)
    if not status:
        return False, ("could not read the profile for its ETag before "
                       "writing (HTTP %s): %s"
                       % (current.status_code, content))
    # A never-configured singleton returns no usable ETag; the sentinel "0"
    # is what the portal accepts for that first conditional write.
    etag = current.etag or '"0"'
    if not subscriptions:
        return False, ("the portal rejects a document with an empty/absent "
                       "subscriptions array (400 'Failed to decode uploaded "
                       "file.', confirmed live 2026-08-18), so the profile "
                       "cannot be emptied through this path")
    document = build_entitlement_document(subscriptions)
    status, content, header = client.upload_entitlements(
        document, etag=etag, filename=filename)
    response = UCPResponse(status, content, header)
    if not status:
        return False, ("upload of %d subscription(s) failed (HTTP %s): %s"
                       % (len(subscriptions), response.status_code, content))
    if not verify:
        return True, None
    profile = get_entitlement_profile(client)
    if profile is None:
        return False, "import returned success but the profile could not be read back"
    stored = get_profile_subscriptions(profile)
    if len(stored) != len(subscriptions):
        return False, ("import reported success but the profile holds %d "
                       "subscription(s), expected %d: %s"
                       % (len(stored), len(subscriptions),
                          json.dumps(profile)))
    return True, None


def clear_entitlement_subscriptions(client):
    """
    Attempt to return the profile to the no-SKU-configured state.

    NOT SUPPORTED on current builds and kept only so the intent is recorded
    in one place: every document that would empty the profile -- an empty
    subscriptions array, a null one, or the key omitted -- is rejected with
    400 "Failed to decode uploaded file." (confirmed live 2026-08-18). There
    is therefore no API route back to "no SKU configured" once a profile has
    been written.

    Tests that need a cluster matched by no SKU must instead upload a profile
    whose SKUs deliberately do not cover the cluster (a classification with no
    SKU of its own); such nodes land in the row with classification null and
    limit 0. See SkuUsageTests.test_usage_without_matching_sku.

    Args:
        client: authenticated UnifiedControlPlaneClient (system_admin)

    Returns:
        Tuple (ok, detail) as per set_entitlement_subscriptions -- currently
        always (False, <reason>) against a real portal.
    """
    return set_entitlement_subscriptions(client, [])


# ==================== Cluster Classification Helpers ====================

def tag_portal_cluster(client, cluster_uuid, classification=None,
                       business_unit=None, name=None):
    """
    Set editable metadata on a portal cluster, handling the ETag internally.

    Classification lives here rather than in a collector helper because its
    only purpose in these tests is to drive SKU matching: usage matches a
    node only when its cluster's classification equals the SKU's.

    PUT is a full replacement of the editable fields, so the current values
    are read first and only the requested fields are overridden -- otherwise
    tagging classification would blank out businessUnit and name.

    Args:
        client:         authenticated UnifiedControlPlaneClient (system_admin)
        cluster_uuid:   portal cluster UUID
        classification: new classification, or None to leave as-is
        business_unit:  new businessUnit, or None to leave as-is
        name:           new name, or None to leave as-is

    Returns:
        Tuple (ok, detail) -- detail is None on success, else a message with
        the HTTP status and body.
    """
    status, content, header = client.get_cluster(cluster_uuid)
    current = UCPResponse(status, content, header)
    if not status:
        return False, ("could not read cluster %s before tagging (HTTP %s): %s"
                       % (cluster_uuid, current.status_code, content))
    if current.etag is None:
        return False, ("cluster %s read back with no ETag; a conditional PUT "
                       "is not possible. Headers were: %s"
                       % (cluster_uuid, current.headers))
    existing = current.json if isinstance(current.json, dict) else {}
    status, content, header = client.update_cluster(
        cluster_uuid,
        etag=current.etag,
        classification=(classification if classification is not None
                        else existing.get('classification')),
        business_unit=(business_unit if business_unit is not None
                       else existing.get('businessUnit')),
        name=name if name is not None else existing.get('name'))
    response = UCPResponse(status, content, header)
    if not status:
        return False, ("tagging cluster %s failed (HTTP %s): %s"
                       % (cluster_uuid, response.status_code, content))
    return True, None


# ==================== Usage Readers ====================

def get_usage(client):
    """
    Return the parsed GET /entitlements/usage body, or None if the read
    failed.

    Args:
        client: authenticated UnifiedControlPlaneClient

    Returns:
        Parsed usage dict/list, or None.
    """
    status, content, header = client.get_entitlement_usage()
    response = UCPResponse(status, content, header)
    if not status:
        return None
    return response.json


def iter_usage_rows(usage):
    """
    Return the per-SKU rows from a usage body.

    Accepts a bare list or an envelope under any of the plausible keys; see
    the module docstring on why this is tolerant.

    Args:
        usage: parsed usage body

    Returns:
        List of row dicts (empty when none can be found).
    """
    if usage is None:
        return []
    if isinstance(usage, list):
        return [row for row in usage if isinstance(row, dict)]
    if not isinstance(usage, dict):
        return []
    for key in ('items', 'rows', 'subscriptions', 'entitlements', 'usage',
                'data', 'results'):
        value = usage.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]
    return []


def _first_int(source, names):
    """Return the first of `names` present in `source` as an int, else None."""
    if not isinstance(source, dict):
        return None
    for name in names:
        if name in source:
            value = source[name]
            if isinstance(value, bool):
                continue
            try:
                return int(value)
            except (TypeError, ValueError):
                continue
    return None


def usage_row_used(row):
    """Return the consumed node count on a usage row, or None."""
    return _first_int(row, ('used', 'usedNodes', 'nodesUsed', 'consumed',
                            'nodeCount'))


def usage_row_entitled(row):
    """Return the entitled node count on a usage row, or None."""
    entitled = _first_int(row, ('entitled', 'entitledNodes', 'nodesEntitled',
                                'limit', 'nodeLimit'))
    if entitled is not None:
        return entitled
    limits = row.get('limits') if isinstance(row, dict) else None
    return _first_int(limits, ('nodes',))


def usage_row_cluster_uuids(row):
    """
    Return the cluster UUIDs contributing to a usage row.

    Contributors may be plain UUID strings or objects carrying the UUID under
    one of several field names.

    Args:
        row: one usage row dict

    Returns:
        List of UUID strings (empty when the row lists none).
    """
    if not isinstance(row, dict):
        return []
    uuids = []
    for key in ('clusters', 'clusterUuids', 'contributingClusters',
                'contributors'):
        value = row.get(key)
        if not isinstance(value, list):
            continue
        for entry in value:
            if isinstance(entry, dict):
                for field in ('clusterUuid', 'uuid', 'clusterId', 'id'):
                    candidate = entry.get(field)
                    if candidate:
                        uuids.append(candidate)
                        break
            elif entry:
                # A bare UUID. Deliberately not type-checked against str:
                # json.loads yields unicode under Jython, so isinstance(x,
                # str) would be False and silently drop every contributor.
                uuids.append(entry)
    return uuids


def find_usage_row(usage, classification=None, support_level=None,
                   logical_cores=None, ram_bytes=None):
    """
    Return the first usage row matching the given SKU identity, or None.

    Every criterion left as None is ignored, so a caller can match on as
    little as the classification when only one SKU of that environment
    exists.

    Args:
        usage:          parsed usage body
        classification: SKU classification to match, or None
        support_level:  SKU support level to match, or None
        logical_cores:  SKU per-node core ceiling to match, or None
        ram_bytes:      SKU per-node RAM ceiling to match, or None

    Returns:
        The matching row dict, or None.
    """
    for row in iter_usage_rows(usage):
        if (classification is not None
                and row.get('classification') != classification):
            continue
        if (support_level is not None
                and row.get('supportLevel') != support_level):
            continue
        # Per-node ceiling lives under instanceSize on the usage row
        # (confirmed live 2026-08-18), not under limits the way it does on the
        # subscription that produced the row.
        size = row.get('instanceSize')
        if not isinstance(size, dict):
            size = row.get('limits') if isinstance(row.get('limits'),
                                                   dict) else row
        if logical_cores is not None:
            if _first_int(size, ('logicalCores',)) != logical_cores:
                continue
        if ram_bytes is not None:
            if _first_int(size, ('ramBytes',)) != ram_bytes:
                continue
        return row
    return None


def rows_containing_cluster(usage, cluster_uuid):
    """
    Return every usage row that lists the given cluster as a contributor.

    This is the per-cluster scoping the shared lab fleet forces: a row's
    absolute `used` also counts nodes from clusters this test never touched,
    so assertions key off whether OUR cluster is in the row.

    Args:
        usage:        parsed usage body
        cluster_uuid: the cluster UUID under test

    Returns:
        List of row dicts.
    """
    return [row for row in iter_usage_rows(usage)
            if cluster_uuid in usage_row_cluster_uuids(row)]


def usage_nodes_for_cluster(usage, cluster_uuid, row=None):
    """
    Return how many nodes ONE cluster contributes, summed over the whole usage
    body or over a single row.

    This is the assertion target for anything that changes with cluster
    topology. A row's own `used` counts every cluster in the fleet -- on the
    lab portal that is dozens of clusters this test does not own, any of which
    may report mid-test -- so asserting on `used`, even as a before/after
    delta, fails for reasons unrelated to the node under test. This number
    moves only when THIS cluster's node count moves.

    A cluster can appear more than once in one `clusters` array with its nodes
    split across entries (confirmed live 2026-08-18), so the entries are
    summed rather than counted.

    Args:
        usage:        parsed usage body
        cluster_uuid: the cluster UUID under test
        row:          restrict to this single row, or None for every row

    Returns:
        Integer node count (0 when the cluster contributes nothing).
    """
    rows = [row] if row is not None else iter_usage_rows(usage)
    total = 0
    for candidate in rows:
        if not isinstance(candidate, dict):
            continue
        for key in ('clusters', 'clusterUuids', 'contributingClusters',
                    'contributors'):
            entries = candidate.get(key)
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                for field in ('clusterUuid', 'uuid', 'clusterId', 'id'):
                    if entry.get(field) == cluster_uuid:
                        nodes = _first_int(entry, ('nodes', 'nodeCount'))
                        total += nodes if nodes is not None else 0
                        break
    return total


def total_usage_used(usage):
    """
    Return the summed `used` across every usage row, ignoring rows that
    carry no readable count.

    Only meaningful as a before/after delta -- never as an absolute
    assertion, because the fleet holds clusters this test does not own.

    Args:
        usage: parsed usage body

    Returns:
        Integer sum (0 when nothing is readable).
    """
    total = 0
    for row in iter_usage_rows(usage):
        used = usage_row_used(row)
        if used is not None:
            total += used
    return total


def describe_usage(usage):
    """
    Return a compact single-line rendering of a usage body for log output.

    Every test logs this: the response shape is not pinned down, so the run
    log is what records the real shape for the next reader.

    Args:
        usage: parsed usage body

    Returns:
        JSON string, or a placeholder when the body was unparseable.
    """
    if usage is None:
        return "<no usage body>"
    try:
        return json.dumps(usage, sort_keys=True)
    except (TypeError, ValueError):
        return repr(usage)
