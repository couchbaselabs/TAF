# Lighthouse UCP — Developer Context

**Design reference**: Lighthouse Collector Design Revision v1.4 (17 Jun 2026)  
**Ticket prefix**: AV-135xxx series

---

## Coding Principles

These are the design rules we follow in this test suite. Read these before
writing or modifying any lighthouse test.

### Single Responsibility Principle (SRP)
Every method does exactly one thing.  Break logic into small, named helpers
rather than writing one large test body.

```
_induce_failure()                → only induces failure on a node
_revert_failure()                → only reverts an induced failure
_trigger_report_and_restore_interval()  → only triggers a report + restores interval
get_cb_cluster_uuid()            → only resolves the cluster UUID
wait_for_portal_node_count()     → only polls until count matches
```

Do not add portal assertions inside a failure-induction helper or vice versa.

### Open/Closed Principle (OCP)
Test behaviour is driven by conf parameters (`self.input.param()`).  New
scenarios are added as new conf entries — existing code is not modified.

```
# New scenario = one new line in failover.conf, zero code changes:
lighthouse...test_autofailover_reflected_on_portal,...,failure_method=stop_memcached,master_node=true
```

The `_NEEDS_REVERT` frozenset follows the same rule: adding a new failure
method that requires revert means adding its name to the set, not changing
any conditional logic.

### Dependency Inversion Principle (DIP)
Tests depend on task abstractions, not raw shell commands.

- Use `NodeFailureTask` / `ConcurrentFailoverTask` for all failure induction
  and revert — the same task classes used by `ConcurrentFailoverTests`.
- Use helper functions (`get_cb_cluster_uuid`, `wait_for_portal_node_count`,
  etc.) not inline REST calls in the test body.
- Never issue `RemoteMachineShellConnection` commands directly in a test
  method — wrap them in a helper or task.

### Reuse Over Re-implementation
If the framework already has a tested abstraction, use it.

- `ConcurrentFailoverTask` induces failure AND monitors until autofailover
  fires AND waits for rebalance to complete — do not replicate this logic.
- `cluster_util.rebalance()` handles rebalance monitoring — do not call
  `rest.monitorRebalance()` directly where a cluster_util wrapper exists.

### Snapshot-Restore Pattern
Every resource mutated in a test must be snapshotted in `setUp` and restored
in `tearDown`.

```python
# setUp: always save maxCount, not count (MB-7282 note below)
self._original_afo_max_count = getattr(afo, 'maxCount', 1)

# tearDown: restore unconditionally, try each node, never crash tearDown
for node in self.cluster.nodes_in_cluster:
    try:
        ...restore...
        break
    except Exception:
        continue
```

### Fail-Safe Cleanup (finally blocks)
If a test fails mid-way through, the cluster must be left in a clean state
for the next test.  Use a `failure_reverted` flag + `finally` block to
guarantee revert happens even on assertion failure.

```python
failure_reverted = False
try:
    ...induce failure...
    ...assertions...
    ...revert failure...
    failure_reverted = True
    ...more assertions...
finally:
    if not failure_reverted:
        ...emergency revert via task...
    self.ucp_client.session_logout()
```

### Data-Driven Testing
Keep test logic separate from test parameters.  Parameters come from conf;
logic lives in the test method.  One test method can cover many scenarios
purely through conf variation.

### Robustness in tearDown
tearDown must never raise.  Wrap every restore call in `try/except`.  Log
warnings on failure — do not fail the test in tearDown for a restore error.

---

## Architecture Overview

Two-tier system:

| Tier | Host | Port | Auth | Purpose |
|------|------|------|------|---------|
| **Collector** | CB Server (ns_server) | 8091 | Basic | Collects node telemetry + external component data, reports to Portal |
| **Portal (UCP)** | Standalone UCP service | 8080 | Cookie/session | Receives telemetry, manages clusters, sessions, audit |

**Data flow**: Collector reads `/pools` → maps service names → collects external payloads from ingest endpoint → POSTs report to Portal → Portal stores at `/api/v1/clusters/{uuid}`.

---

## File Map

### Client Library (`lib/unified_control_plane/`)
| File | Purpose |
|------|---------|
| `__init__.py` | Exports `LighthouseCollectorClient`, `UnifiedControlPlaneClient` |
| `constants.py` | All API endpoints, defaults |
| `collector_client.py` | `LighthouseCollectorClient` — targets port 8091 |
| `ucp_client.py` | `UnifiedControlPlaneClient` — targets port 8080 |

### Test Infrastructure (`pytests/lighthouse/`)
| File | Purpose |
|------|---------|
| `lighthouse_base.py` | `LighthouseBase` — multi-cluster base class |
| `lighthouse_portal.py` | `LighthousePortal` — portal config object |
| `response.py` | `UCPResponse` wrapper + exception classes |
| `ucp_helper_methods.py` | UCP portal helper functions |
| `collector_helper_methods.py` | Collector helper functions |

### Test Files
| File | Class | Description |
|------|-------|-------------|
| `collector/collector_tests.py` | `CollectorTests` | Settings, telemetry accuracy, aggregate telemetry, node rebalance |
| `failover/lighthouse_failover_tests.py` | `LighthouseFailoverTests` | Node failure, manual failover, autofailover reflected on portal |
| `sessions/session_timeout_tests.py` | `CollectorConfigTests` | Session timeout tests |

### Config Files (`conf/lighthouse/`)
| File | Purpose |
|------|---------|
| `collector.conf` | Collector test parameters |
| `failover.conf` | Failover test parameters |
| `session.conf` | Session test parameters |

---

## Client APIs

### `LighthouseCollectorClient` (port 8091, Basic auth)
All methods return `(status: bool, content: str, header)`.

```python
client = LighthouseCollectorClient(server)  # server = cluster.master

client.get_lighthouse_settings()
# GET /internal/settings/lighthouse

client.update_lighthouse_settings(
    enabled=None, endpoint=None,
    report_interval_hours=None, report_timeout_seconds=None,
    external_nodes_max_payload_bytes=None, external_nodes_max_count=None
)
# POST /internal/settings/lighthouse
# NOTE: Any successful POST immediately triggers a report to the portal!

client.ingest_external_telemetry(product_name, instance_id, payload)
# POST /_lighthouseCollector/ingest
```

**Default settings**:
```python
{
    'enabled': True,
    'endpoint': 'lighthouse.couchbase.internal',
    'reportIntervalHours': 2,
    'reportTimeoutSeconds': 1,
    'externalNodesMaxPayloadBytes': 10240,
    'externalNodesMaxCount': 100,
}
```

### `UnifiedControlPlaneClient` (port 8080, cookie-based)

```python
client = UnifiedControlPlaneClient(ucp_portal)

client.session_login(username, password)
client.session_logout()
client.list_clusters()
client.get_cluster(cluster_uuid)
client.get_config()
client.update_config(etag, telemetry_retention_days, ...)
client.list_audit_events(...)
client.create_user(), client.get_user(), client.update_user(), ...
```

---

## Helper Functions

### `collector_helper_methods.py`

```python
# Settings
get_collector_settings(client)
restore_collector_settings(client, saved)

# Cluster source-of-truth
get_cb_cluster_uuid(server)               # /pools → hyphenated UUID
get_cb_cluster_node_count(server)
get_cb_cluster_nodes_services(server)     # → {hostname: sorted_services_list}
get_cb_cluster_aggregate_hardware(cluster)
get_cb_cluster_services_union(cluster)

# Portal
get_portal_cluster(ucp_client, uuid)
get_portal_cluster_nodes(ucp_client, uuid)
wait_for_cluster_on_portal(ucp_client, uuid, timeout=60, poll_interval=5)
wait_for_portal_node_count(ucp_client, uuid, expected_count, timeout=120, poll_interval=5)

# Report triggering
set_lighthouse_ns_config_via_diag_eval(server, reporting_endpoint, reporting_port, reporting_interval_hours)
set_lighthouse_interval_via_diag_eval(server, interval_hours)

# Constants
LIGHTHOUSE_DEFAULT_PORTAL_PORT = 8080
CB_TO_PORTAL_SERVICE_MAP = {
    'kv': 'data', 'n1ql': 'query', 'index': 'index',
    'fts': 'search', 'cbas': 'analytics', 'eventing': 'eventing', 'backup': 'backup',
}
```

### `ucp_helper_methods.py`

```python
create_session(client, username, password)
verify_session_active(client)
get_session_idle_timeout(client)
set_session_idle_timeout(client, minutes)
parse_iso8601_timestamp(timestamp_str)
build_telemetry_payload(collected_at, cluster_uuid, product, nodes)
build_node_telemetry(hostname, cpu_physical_cores, ...)
```

---

## Test Base Class (`LighthouseBase`)

**Inheritance**: `LighthouseBase → CollectionBase → ClusterSetup → OnPremBaseTest`

**After `setUp()` provides**:
- `self.cluster` — primary cluster
- `self.clusters` — list of all clusters (primary + secondary)
- `self.ucp_portal` — `LighthousePortal` config (`.ip`, `.port`, `.username`, `.password`)
- `self.ucp_client` — `UnifiedControlPlaneClient` instance (NOT yet logged in)
- `self.task`, `self.task_manager`, `self.cluster_util`

**Input params** are read via `self.input.param("param_name", default)`.

---

## Writing New Tests

### Template

```python
from lighthouse.lighthouse_base import LighthouseBase
from lighthouse.collector_helper_methods import (
    get_collector_settings, restore_collector_settings,
    get_cb_cluster_uuid, get_portal_cluster,
    wait_for_cluster_on_portal,
    set_lighthouse_ns_config_via_diag_eval,
    set_lighthouse_interval_via_diag_eval,
    LIGHTHOUSE_DEFAULT_PORTAL_PORT,
)
from unified_control_plane import LighthouseCollectorClient


class MyNewTests(LighthouseBase):

    def setUp(self):
        super(MyNewTests, self).setUp()
        self.collector_clients = [
            LighthouseCollectorClient(cluster.master)
            for cluster in self.clusters
        ]
        self._original_settings = []
        for i, client in enumerate(self.collector_clients):
            settings = get_collector_settings(client)
            if settings is None:
                self.fail("Could not fetch collector settings from cluster %d" % i)
            self._original_settings.append(settings)

    def tearDown(self):
        for i, (client, saved) in enumerate(
                zip(self.collector_clients, self._original_settings)):
            try:
                restore_collector_settings(client, saved)
            except Exception as e:
                self.log.warning("Cluster %d: failed to restore: %s" % (i, e))
        super(MyNewTests, self).tearDown()

    def test_something(self):
        portal_domain = 'lighthouse.couchbase.internal'
        status, content, _ = self.ucp_client.session_login(
            self.ucp_portal.username, self.ucp_portal.password)
        self.assertTrue(status, "Portal login failed: %s" % content)
        try:
            # Trigger an immediate report (1-min interval, sleep 60s, restore 2h)
            diag_status, diag_content = set_lighthouse_ns_config_via_diag_eval(
                self.cluster.master,
                reporting_endpoint=portal_domain,
                reporting_port=LIGHTHOUSE_DEFAULT_PORTAL_PORT,
                reporting_interval_hours=1 / 60.0)
            self.assertTrue(diag_status, diag_content)
            self.sleep(60, "waiting for report to fire")
            set_lighthouse_interval_via_diag_eval(self.cluster.master, 2)

            uuid = get_cb_cluster_uuid(self.cluster.master)
            portal_cluster = get_portal_cluster(self.ucp_client, uuid)
            self.assertIsNotNone(portal_cluster)
            # assertions ...
        finally:
            self.ucp_client.session_logout()
```

### Conf entry format
```
package.module.ClassName.test_name,nodes_init=2|2,num_of_clusters=2,ucp_port=8080[,extra_params]
```

---

## Failover Test Design

| Scenario | Pre-failure | How |
|----------|------------|-----|
| No failover (`trigger_failover=False`) | Yes — `stop_couchbase`, `stop_memcached`, `restart_couchbase` | Verify portal stays at N |
| Graceful failover | None — node must be healthy | `rest.fail_over(otp_id, graceful=True)` |
| Forceful failover | None | `rest.fail_over(otp_id, graceful=False)` |
| Autofailover | Induced by `ConcurrentFailoverTask` internally | Enable AFO with short timeout, `ConcurrentFailoverTask(task_type="induce_failure")` monitors until AFO fires |

Recovery after failover: `rest.add_back_node()` + `rest.set_recovery_type("delta")` for KV nodes + `cluster_util.rebalance()`.

---

## Key Gotchas

1. **Any POST to `/internal/settings/lighthouse` triggers an immediate report** — use `set_lighthouse_ns_config_via_diag_eval` with `reporting_interval_hours=1/60.0` (1 minute) + 60s sleep to trigger reliably.
2. **Portal stores nodes under `telemetry.nodes`**, not top-level.
3. **Portal uses IP-only hostnames**; CB uses `ip:8091` — strip `:8091` when matching.
4. **Service name mapping**: `kv→data, n1ql→query, fts→search, cbas→analytics`.
5. **UUID format**: `/pools` returns UUID without hyphens; portal uses hyphenated — `get_cb_cluster_uuid()` handles this.
6. **`ucp_client` is NOT logged in after setUp** — always call `session_login()` in the test and wrap in `try/finally` with `session_logout()`.
7. **ETag required for PUT/PATCH** — fetch current config first to get ETag from response headers.
8. **Graceful and forceful failover are pure API calls** — no pre-failure induction needed (`rest.fail_over(otp_id, graceful=True/False)`).
9. **After autofailover**, use a surviving active node as the `master` for `ConcurrentFailoverTask` — the failed node's ns_server may be down (`stop_couchbase`) or transitional (`stop_memcached` as mb_master).
10. **`stop_memcached` uses SIGSTOP/SIGCONT** — the process is frozen, not killed. Revert: `kill -18 $(pgrep memcached)`.
11. **MB-7282**: `update_autofailover_settings` fails when the autofailover event count is non-zero. Always POST `/settings/autoFailover/resetCount` before calling `update_autofailover_settings` in tearDown.
12. **Save `afo.maxCount`, not `afo.count`**: `afo.count` is the current event count (0 at setUp). `afo.maxCount` is the max allowed (typically 1). tearDown must restore the latter.
